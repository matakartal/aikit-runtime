//! Optional Firecracker microVM lifecycle primitives.
//!
//! This module deliberately does not plug Firecracker into the Bash [`PreparedCommand`] path.
//! That path has no guest command/workspace transport seam: treating a successfully booted VM as
//! if it had executed the requested host-workspace command would be a false containment claim.
//! Callers can use the low-level lifecycle here with a separately attested guest transport.

use super::{ActiveContainmentBackend, BackendCapability, ContainmentGuarantees};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::ffi::OsString;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::{Child, Command};

const API_SOCKET_IN_JAIL: &str = "/run/firecracker.socket";
const KERNEL_IN_JAIL: &str = "/assets/vmlinux";
const ROOTFS_IN_JAIL: &str = "/assets/rootfs.ext4";
const MAX_API_RESPONSE_BYTES: usize = 16 * 1024;
const MAX_VERSION_OUTPUT_BYTES: usize = 4 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum FirecrackerError {
    #[error("invalid Firecracker configuration: {0}")]
    InvalidConfig(String),
    #[error("Firecracker host prerequisite failed: {0}")]
    Unavailable(String),
    #[error("Firecracker staging failed: {0}")]
    Staging(String),
    #[error("Firecracker process failed: {0}")]
    Process(String),
    #[error("Firecracker API failed: {0}")]
    Api(String),
}

pub type FirecrackerResult<T> = std::result::Result<T, FirecrackerError>;

/// A host file pinned by canonical path and SHA-256 content identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ImmutableHostFile {
    path: PathBuf,
    sha256: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ImmutableHostFileWire {
    path: PathBuf,
    sha256: String,
}

impl<'de> Deserialize<'de> for ImmutableHostFile {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::Error as _;

        let wire = ImmutableHostFileWire::deserialize(deserializer)?;
        Self::new(wire.path, wire.sha256).map_err(D::Error::custom)
    }
}

impl ImmutableHostFile {
    pub fn new(path: impl Into<PathBuf>, sha256: impl Into<String>) -> FirecrackerResult<Self> {
        let file = Self {
            path: path.into(),
            sha256: sha256.into(),
        };
        file.validate_static("artifact")?;
        Ok(file)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn sha256(&self) -> &str {
        &self.sha256
    }

    fn validate_static(&self, label: &str) -> FirecrackerResult<()> {
        if !self.path.is_absolute() {
            return Err(FirecrackerError::InvalidConfig(format!(
                "{label} path must be absolute"
            )));
        }
        validate_sha256(&self.sha256)
            .map_err(|detail| FirecrackerError::InvalidConfig(format!("{label} sha256 {detail}")))
    }
}

/// Network is disabled by default. A TAP device is accepted only with an explicit network
/// namespace path, so the host device cannot silently expose unrestricted host networking.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum FirecrackerNetwork {
    #[default]
    Disabled,
    Tap {
        device: String,
        netns: PathBuf,
        guest_mac: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FirecrackerConfig {
    jailer: ImmutableHostFile,
    firecracker: ImmutableHostFile,
    kernel: ImmutableHostFile,
    rootfs: ImmutableHostFile,
    chroot_base: PathBuf,
    uid: u32,
    gid: u32,
    vcpu_count: u8,
    memory_mib: u32,
    network: FirecrackerNetwork,
    api_timeout_ms: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FirecrackerConfigWire {
    jailer: ImmutableHostFile,
    firecracker: ImmutableHostFile,
    kernel: ImmutableHostFile,
    rootfs: ImmutableHostFile,
    chroot_base: PathBuf,
    uid: u32,
    gid: u32,
    vcpu_count: u8,
    memory_mib: u32,
    #[serde(default)]
    network: FirecrackerNetwork,
    api_timeout_ms: u64,
}

impl<'de> Deserialize<'de> for FirecrackerConfig {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::Error as _;

        let wire = FirecrackerConfigWire::deserialize(deserializer)?;
        let config = Self {
            jailer: wire.jailer,
            firecracker: wire.firecracker,
            kernel: wire.kernel,
            rootfs: wire.rootfs,
            chroot_base: wire.chroot_base,
            uid: wire.uid,
            gid: wire.gid,
            vcpu_count: wire.vcpu_count,
            memory_mib: wire.memory_mib,
            network: wire.network,
            api_timeout_ms: wire.api_timeout_ms,
        };
        config.validate_static().map_err(D::Error::custom)?;
        Ok(config)
    }
}

impl FirecrackerConfig {
    pub fn new(
        jailer: ImmutableHostFile,
        firecracker: ImmutableHostFile,
        kernel: ImmutableHostFile,
        rootfs: ImmutableHostFile,
        chroot_base: impl Into<PathBuf>,
        uid: u32,
        gid: u32,
    ) -> FirecrackerResult<Self> {
        let config = Self {
            jailer,
            firecracker,
            kernel,
            rootfs,
            chroot_base: chroot_base.into(),
            uid,
            gid,
            vcpu_count: 1,
            memory_mib: 512,
            network: FirecrackerNetwork::Disabled,
            api_timeout_ms: 5_000,
        };
        config.validate_static()?;
        Ok(config)
    }

    pub fn with_resources(mut self, vcpu_count: u8, memory_mib: u32) -> FirecrackerResult<Self> {
        self.vcpu_count = vcpu_count;
        self.memory_mib = memory_mib;
        self.validate_static()?;
        Ok(self)
    }

    pub fn with_network(mut self, network: FirecrackerNetwork) -> FirecrackerResult<Self> {
        self.network = network;
        self.validate_static()?;
        Ok(self)
    }

    pub fn with_api_timeout(mut self, timeout: Duration) -> FirecrackerResult<Self> {
        self.api_timeout_ms = u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX);
        self.validate_static()?;
        Ok(self)
    }

    pub fn jailer(&self) -> &ImmutableHostFile {
        &self.jailer
    }

    pub fn firecracker(&self) -> &ImmutableHostFile {
        &self.firecracker
    }

    pub fn kernel(&self) -> &ImmutableHostFile {
        &self.kernel
    }

    pub fn rootfs(&self) -> &ImmutableHostFile {
        &self.rootfs
    }

    pub fn chroot_base(&self) -> &Path {
        &self.chroot_base
    }

    pub fn uid(&self) -> u32 {
        self.uid
    }

    pub fn gid(&self) -> u32 {
        self.gid
    }

    pub fn vcpu_count(&self) -> u8 {
        self.vcpu_count
    }

    pub fn memory_mib(&self) -> u32 {
        self.memory_mib
    }

    pub fn network(&self) -> &FirecrackerNetwork {
        &self.network
    }

    pub fn api_timeout(&self) -> Duration {
        Duration::from_millis(self.api_timeout_ms)
    }

    fn validate_static(&self) -> FirecrackerResult<()> {
        self.jailer.validate_static("jailer")?;
        self.firecracker.validate_static("firecracker")?;
        self.kernel.validate_static("kernel")?;
        self.rootfs.validate_static("rootfs")?;
        if !self.chroot_base.is_absolute() {
            return Err(FirecrackerError::InvalidConfig(
                "chroot_base must be absolute".into(),
            ));
        }
        if self.uid == 0 || self.gid == 0 {
            return Err(FirecrackerError::InvalidConfig(
                "jailer uid and gid must be non-zero".into(),
            ));
        }
        if !(1..=32).contains(&self.vcpu_count)
            || (self.vcpu_count != 1 && self.vcpu_count & 1 == 1)
        {
            return Err(FirecrackerError::InvalidConfig(
                "vcpu_count must be 1 or an even number between 2 and 32".into(),
            ));
        }
        if !(64..=1_048_576).contains(&self.memory_mib) {
            return Err(FirecrackerError::InvalidConfig(
                "memory_mib must be between 64 and 1048576".into(),
            ));
        }
        if !(100..=60_000).contains(&self.api_timeout_ms) {
            return Err(FirecrackerError::InvalidConfig(
                "api timeout must be between 100ms and 60s".into(),
            ));
        }
        if let FirecrackerNetwork::Tap {
            device,
            netns,
            guest_mac,
        } = &self.network
        {
            if device.is_empty()
                || device.len() > 15
                || !device
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
            {
                return Err(FirecrackerError::InvalidConfig(
                    "TAP device name must be 1-15 safe ASCII characters".into(),
                ));
            }
            if !netns.is_absolute() {
                return Err(FirecrackerError::InvalidConfig(
                    "network namespace path must be absolute".into(),
                ));
            }
            validate_mac(guest_mac)?;
        }
        Ok(())
    }
}

fn validate_sha256(value: &str) -> std::result::Result<(), &'static str> {
    let valid = value.strip_prefix("sha256:").is_some_and(|hex| {
        hex.len() == 64
            && hex
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    });
    if valid {
        Ok(())
    } else {
        Err("must be sha256:<64 lowercase hex>")
    }
}

fn validate_mac(value: &str) -> FirecrackerResult<()> {
    let parts = value.split(':').collect::<Vec<_>>();
    if parts.len() != 6 || parts.iter().any(|part| part.len() != 2) {
        return Err(FirecrackerError::InvalidConfig(
            "guest_mac must be a six-octet unicast MAC".into(),
        ));
    }
    let octets = parts
        .iter()
        .map(|part| u8::from_str_radix(part, 16))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|_| FirecrackerError::InvalidConfig("guest_mac is malformed".into()))?;
    if octets.iter().all(|octet| *octet == 0) || octets[0] & 1 != 0 {
        return Err(FirecrackerError::InvalidConfig(
            "guest_mac must be a six-octet unicast MAC".into(),
        ));
    }
    Ok(())
}

/// Shell-free jailer launch description. It contains no guest command or host environment.
#[derive(Debug, Clone)]
pub struct FirecrackerLaunchPlan {
    config: FirecrackerConfig,
    instance_id: String,
    jailer: PathBuf,
    firecracker: PathBuf,
    chroot_base: PathBuf,
    instance_dir: PathBuf,
    jail_root: PathBuf,
    api_socket: PathBuf,
    args: Vec<OsString>,
}

impl FirecrackerLaunchPlan {
    pub fn build(
        config: &FirecrackerConfig,
        instance_id: &str,
        writable_workspace: &Path,
    ) -> FirecrackerResult<Self> {
        validate_instance_id(instance_id)?;
        config.validate_static()?;
        let workspace = canonical_directory(writable_workspace, "workspace")?;
        let jailer = validate_immutable_executable(&config.jailer, "jailer", Some(&workspace))?;
        let firecracker =
            validate_immutable_executable(&config.firecracker, "firecracker", Some(&workspace))?;
        let kernel = validate_immutable_file(&config.kernel, "kernel", Some(&workspace))?;
        let rootfs = validate_immutable_file(&config.rootfs, "rootfs", Some(&workspace))?;
        let chroot_base =
            canonical_secure_directory(&config.chroot_base, "chroot_base", &workspace)?;
        let network = validate_network_runtime(&config.network, &workspace)?;

        // Freeze every runtime path to the canonical object that was hash/permission validated.
        // Later staging and command construction never return to a caller-supplied alias.
        let mut resolved_config = config.clone();
        resolved_config.jailer.path = jailer.clone();
        resolved_config.firecracker.path = firecracker.clone();
        resolved_config.kernel.path = kernel;
        resolved_config.rootfs.path = rootfs;
        resolved_config.chroot_base = chroot_base.clone();
        resolved_config.network = network;

        let executable_name = firecracker
            .file_name()
            .ok_or_else(|| FirecrackerError::InvalidConfig("firecracker has no filename".into()))?;
        let instance_dir = chroot_base.join(executable_name).join(instance_id);
        let jail_root = instance_dir.join("root");
        let api_socket = jail_root.join(API_SOCKET_IN_JAIL.trim_start_matches('/'));

        let mut args = vec![
            OsString::from("--id"),
            OsString::from(instance_id),
            OsString::from("--exec-file"),
            firecracker.as_os_str().to_owned(),
            OsString::from("--uid"),
            OsString::from(config.uid.to_string()),
            OsString::from("--gid"),
            OsString::from(config.gid.to_string()),
            OsString::from("--chroot-base-dir"),
            chroot_base.as_os_str().to_owned(),
        ];
        // Do not add `--new-pid-ns` until the lifecycle owns Firecracker through a pidfd. In that
        // mode jailer clones the VMM and records a host PID; treating the initially spawned jailer
        // handle as the VMM would make cancellation and cleanup racy. The jailer's mount/chroot,
        // privilege-drop and Firecracker seccomp boundaries remain active without this flag.
        if let FirecrackerNetwork::Tap { netns, .. } = &resolved_config.network {
            args.push(OsString::from("--netns"));
            args.push(netns.as_os_str().to_owned());
        }
        args.extend([
            OsString::from("--"),
            OsString::from("--api-sock"),
            OsString::from(API_SOCKET_IN_JAIL),
        ]);
        Ok(Self {
            config: resolved_config,
            instance_id: instance_id.into(),
            jailer,
            firecracker,
            chroot_base,
            instance_dir,
            jail_root,
            api_socket,
            args,
        })
    }

    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    pub fn api_socket(&self) -> &Path {
        &self.api_socket
    }

    pub fn jail_root(&self) -> &Path {
        &self.jail_root
    }

    pub fn jailer_executable(&self) -> &Path {
        &self.jailer
    }

    pub fn firecracker_executable(&self) -> &Path {
        &self.firecracker
    }

    pub fn args(&self) -> &[OsString] {
        &self.args
    }

    pub fn command(&self) -> Command {
        let mut command = Command::new(&self.jailer);
        command.args(&self.args);
        command.env_clear();
        command.stdin(Stdio::null());
        // Guest serial output is unbounded. Unconsumed pipes would eventually fill and stall the
        // VMM, so the low-level lifecycle discards output until a bounded log sink is modeled.
        command.stdout(Stdio::null());
        command.stderr(Stdio::null());
        command.kill_on_drop(true);
        command
    }

    /// Create a fresh jail root and copy only hash-verified immutable boot artifacts into it.
    /// Existing instance directories are rejected rather than reused or deleted.
    pub fn stage(&self) -> FirecrackerResult<FirecrackerStaging> {
        let executable_parent = self
            .instance_dir
            .parent()
            .ok_or_else(|| FirecrackerError::Staging("instance path has no parent".into()))?;
        std::fs::create_dir_all(executable_parent).map_err(|error| {
            FirecrackerError::Staging(format!(
                "cannot create jailer executable directory {}: {error}",
                executable_parent.display()
            ))
        })?;
        std::fs::create_dir(&self.instance_dir).map_err(|error| {
            FirecrackerError::Staging(format!(
                "cannot create fresh instance directory {}: {error}",
                self.instance_dir.display()
            ))
        })?;
        let result = (|| {
            let assets = self.jail_root.join("assets");
            let run = self.jail_root.join("run");
            std::fs::create_dir_all(&assets)
                .and_then(|_| std::fs::create_dir_all(&run))
                .map_err(|error| FirecrackerError::Staging(error.to_string()))?;
            set_owner_only_directory(&self.instance_dir)?;
            set_owner_only_directory(&self.jail_root)?;
            copy_verified(&self.config.kernel, &assets.join("vmlinux"), "kernel")?;
            copy_verified(&self.config.rootfs, &assets.join("rootfs.ext4"), "rootfs")?;
            Ok(())
        })();
        if let Err(error) = result {
            remove_instance_dir(&self.instance_dir, &self.chroot_base);
            return Err(error);
        }
        Ok(FirecrackerStaging {
            instance_dir: self.instance_dir.clone(),
            chroot_base: self.chroot_base.clone(),
        })
    }

    async fn configure_and_start(&self) -> FirecrackerResult<()> {
        wait_for_api_socket(&self.api_socket, self.config.api_timeout()).await?;
        api_put(
            &self.api_socket,
            "/machine-config",
            &json!({
                "vcpu_count": self.config.vcpu_count,
                "mem_size_mib": self.config.memory_mib,
                "smt": false,
                "track_dirty_pages": false,
            }),
        )
        .await?;
        api_put(
            &self.api_socket,
            "/boot-source",
            &json!({
                "kernel_image_path": KERNEL_IN_JAIL,
                "boot_args": "console=ttyS0 reboot=k panic=1 pci=off",
            }),
        )
        .await?;
        api_put(
            &self.api_socket,
            "/drives/rootfs",
            &json!({
                "drive_id": "rootfs",
                "path_on_host": ROOTFS_IN_JAIL,
                "is_root_device": true,
                "is_read_only": true,
            }),
        )
        .await?;
        if let FirecrackerNetwork::Tap {
            device, guest_mac, ..
        } = &self.config.network
        {
            api_put(
                &self.api_socket,
                "/network-interfaces/eth0",
                &json!({
                    "iface_id": "eth0",
                    "host_dev_name": device,
                    "guest_mac": guest_mac,
                }),
            )
            .await?;
        }
        api_put(
            &self.api_socket,
            "/actions",
            &json!({"action_type": "InstanceStart"}),
        )
        .await
    }
}

/// Staged jail resources. Dropping removes only the exact fresh instance directory.
#[derive(Debug)]
pub struct FirecrackerStaging {
    instance_dir: PathBuf,
    chroot_base: PathBuf,
}

trait FirecrackerReaper: Send + Sync {
    fn reap(&self, child: Child, staging: FirecrackerStaging);
}

#[derive(Debug, Default)]
struct ThreadFirecrackerReaper;

impl FirecrackerReaper for ThreadFirecrackerReaper {
    fn reap(&self, mut child: Child, staging: FirecrackerStaging) {
        // Kill is initiated synchronously, but cleanup remains owned by the reaper until the OS
        // confirms that the child was collected. A failed wait deliberately retains the jail.
        let _ = child.start_kill();
        let job = Arc::new(StdMutex::new(Some((child, staging))));
        let thread_job = job.clone();
        let spawned = std::thread::Builder::new()
            .name("aikit-firecracker-reaper".into())
            .spawn(move || {
                let Some((mut child, staging)) =
                    thread_job.lock().ok().and_then(|mut job| job.take())
                else {
                    return;
                };
                reap_blocking(&mut child, staging);
            });
        if spawned.is_err() {
            // Thread exhaustion is not permission to race cleanup. The rare fallback blocks this
            // drop until the already-killed process is collected.
            if let Some((mut child, staging)) =
                job.lock().ok().and_then(|mut pending| pending.take())
            {
                reap_blocking(&mut child, staging);
            } else {
                std::mem::forget(job);
            }
        }
    }
}

fn reap_blocking(child: &mut Child, staging: FirecrackerStaging) {
    let reaped = loop {
        match child.try_wait() {
            Ok(Some(_)) => break true,
            Ok(None) => std::thread::sleep(Duration::from_millis(10)),
            Err(_) => break false,
        }
    };
    finish_reap(staging, reaped);
}

fn finish_reap(staging: FirecrackerStaging, reaped: bool) {
    if reaped {
        drop(staging);
    } else {
        // Retaining stale jail data is safer than deleting files while process liveness is unknown.
        std::mem::forget(staging);
    }
}

struct FirecrackerProcessSupervisor {
    child: Option<Child>,
    staging: Option<FirecrackerStaging>,
    reaper: Arc<dyn FirecrackerReaper>,
}

impl FirecrackerProcessSupervisor {
    fn new(child: Child, staging: FirecrackerStaging) -> Self {
        Self::new_with_reaper(child, staging, Arc::new(ThreadFirecrackerReaper))
    }

    fn new_with_reaper(
        child: Child,
        staging: FirecrackerStaging,
        reaper: Arc<dyn FirecrackerReaper>,
    ) -> Self {
        Self {
            child: Some(child),
            staging: Some(staging),
            reaper,
        }
    }

    fn child_mut(&mut self) -> &mut Child {
        self.child
            .as_mut()
            .expect("Firecracker supervisor owns a child until it is reaped")
    }

    fn mark_reaped(&mut self) {
        self.child.take();
        self.staging.take();
    }

    async fn kill_and_wait(&mut self) -> FirecrackerResult<()> {
        let child = self.child_mut();
        let _ = child.start_kill();
        match child.wait().await {
            Ok(_) => {
                self.mark_reaped();
                Ok(())
            }
            Err(error) => Err(FirecrackerError::Process(format!(
                "cannot wait for killed jailer/Firecracker: {error}"
            ))),
        }
    }
}

impl Drop for FirecrackerProcessSupervisor {
    fn drop(&mut self) {
        if let (Some(child), Some(staging)) = (self.child.take(), self.staging.take()) {
            self.reaper.reap(child, staging);
        }
    }
}

impl FirecrackerStaging {
    pub fn instance_dir(&self) -> &Path {
        &self.instance_dir
    }
}

impl Drop for FirecrackerStaging {
    fn drop(&mut self) {
        remove_instance_dir(&self.instance_dir, &self.chroot_base);
    }
}

/// A booted microVM process. Guest command transport remains the caller's separately attested
/// responsibility; this handle only owns VM lifecycle and cleanup.
pub struct FirecrackerVm {
    supervisor: FirecrackerProcessSupervisor,
    plan: FirecrackerLaunchPlan,
}

impl FirecrackerVm {
    pub async fn launch(plan: FirecrackerLaunchPlan) -> FirecrackerResult<Self> {
        if !cfg!(target_os = "linux") {
            return Err(FirecrackerError::Unavailable(
                "Firecracker launch requires Linux with KVM; this host is validation-only".into(),
            ));
        }
        validate_live_linux_prerequisites(&plan).await?;
        let stage_plan = plan.clone();
        let staging = tokio::task::spawn_blocking(move || stage_plan.stage())
            .await
            .map_err(|error| FirecrackerError::Staging(error.to_string()))??;
        prepare_runtime_permissions(&plan)?;
        let child = plan
            .command()
            .spawn()
            .map_err(|error| FirecrackerError::Process(error.to_string()))?;
        let mut supervisor = FirecrackerProcessSupervisor::new(child, staging);
        enum LaunchOutcome {
            Configured(FirecrackerResult<()>),
            Exited(std::io::Result<std::process::ExitStatus>),
        }
        let configured = tokio::select! {
            result = tokio::time::timeout(plan.config.api_timeout(), plan.configure_and_start()) => {
                LaunchOutcome::Configured(result.unwrap_or_else(|_| {
                    Err(FirecrackerError::Api(
                        "Firecracker API configuration exceeded its deadline".into(),
                    ))
                }))
            },
            status = supervisor.child_mut().wait() => LaunchOutcome::Exited(status),
        };
        match configured {
            LaunchOutcome::Configured(Ok(())) => {}
            LaunchOutcome::Configured(Err(error)) => {
                let _ = supervisor.kill_and_wait().await;
                return Err(error);
            }
            LaunchOutcome::Exited(Ok(status)) => {
                supervisor.mark_reaped();
                return Err(FirecrackerError::Process(format!(
                    "jailer/Firecracker exited before API configuration completed: {status}"
                )));
            }
            LaunchOutcome::Exited(Err(error)) => {
                return Err(FirecrackerError::Process(format!(
                    "cannot wait for jailer/Firecracker: {error}"
                )));
            }
        }
        match supervisor.child_mut().try_wait() {
            Ok(Some(status)) => {
                supervisor.mark_reaped();
                return Err(FirecrackerError::Process(format!(
                    "Firecracker exited immediately after InstanceStart: {status}"
                )));
            }
            Ok(None) => {}
            Err(error) => {
                return Err(FirecrackerError::Process(format!(
                    "cannot inspect jailer/Firecracker after start: {error}"
                )));
            }
        }
        Ok(Self { supervisor, plan })
    }

    pub fn instance_id(&self) -> &str {
        self.plan.instance_id()
    }

    pub async fn shutdown(mut self) -> FirecrackerResult<()> {
        let _ = api_put(
            self.plan.api_socket(),
            "/actions",
            &json!({"action_type": "SendCtrlAltDel"}),
        )
        .await;
        match tokio::time::timeout(Duration::from_secs(5), self.supervisor.child_mut().wait()).await
        {
            Ok(Ok(_)) => {
                self.supervisor.mark_reaped();
                Ok(())
            }
            Ok(Err(error)) => Err(FirecrackerError::Process(error.to_string())),
            Err(_) => {
                self.supervisor.kill_and_wait().await?;
                Err(FirecrackerError::Process(
                    "microVM did not stop within cleanup deadline".into(),
                ))
            }
        }
    }
}

/// Probe host prerequisites. On non-Linux hosts this always reports unavailable and never claims
/// that unit-tested command construction proves a VM isolation boundary.
pub async fn firecracker_capability(
    config: &FirecrackerConfig,
    writable_workspace: &Path,
) -> BackendCapability {
    let guarantees = ContainmentGuarantees::firecracker(&config.network);
    if !cfg!(target_os = "linux") {
        return BackendCapability::unavailable(
            ActiveContainmentBackend::Firecracker,
            guarantees,
            "Firecracker requires a live Linux KVM+jailer acceptance gate; this host is validation-only",
        );
    }
    let probe_config = config.clone();
    let probe_workspace = writable_workspace.to_owned();
    let plan = match tokio::task::spawn_blocking(move || {
        FirecrackerLaunchPlan::build(&probe_config, "aikit-capability-probe", &probe_workspace)
    })
    .await
    {
        Ok(result) => match result {
            Ok(plan) => plan,
            Err(error) => {
                return BackendCapability::unavailable(
                    ActiveContainmentBackend::Firecracker,
                    guarantees,
                    error.to_string(),
                )
            }
        },
        Err(error) => {
            return BackendCapability::unavailable(
                ActiveContainmentBackend::Firecracker,
                guarantees,
                format!("Firecracker capability validation task failed: {error}"),
            )
        }
    };
    if !is_effective_root() {
        return BackendCapability::unavailable(
            ActiveContainmentBackend::Firecracker,
            guarantees,
            "the Firecracker jailer must be launched as root",
        );
    }
    if OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/kvm")
        .is_err()
    {
        return BackendCapability::unavailable(
            ActiveContainmentBackend::Firecracker,
            guarantees,
            "/dev/kvm is not available for read/write",
        );
    }
    for (path, label) in [
        (plan.jailer.as_path(), "jailer"),
        (plan.firecracker.as_path(), "firecracker"),
        (plan.config.kernel.path(), "kernel"),
        (plan.config.rootfs.path(), "rootfs"),
    ] {
        if let Err(error) = validate_root_owned_ancestry(path, label) {
            return BackendCapability::unavailable(
                ActiveContainmentBackend::Firecracker,
                guarantees,
                error.to_string(),
            );
        }
    }
    if let Err(error) = validate_root_owned_ancestry(&plan.chroot_base, "chroot_base") {
        return BackendCapability::unavailable(
            ActiveContainmentBackend::Firecracker,
            guarantees,
            error.to_string(),
        );
    }
    if let Err(error) = revalidate_plan_artifacts_async(&plan).await {
        return BackendCapability::unavailable(
            ActiveContainmentBackend::Firecracker,
            guarantees,
            error.to_string(),
        );
    }
    for (path, label) in [
        (plan.jailer.as_path(), "jailer"),
        (plan.firecracker.as_path(), "firecracker"),
    ] {
        if let Err(error) = validate_static_elf(path, label) {
            return BackendCapability::unavailable(
                ActiveContainmentBackend::Firecracker,
                guarantees,
                error.to_string(),
            );
        }
    }
    if let Err(error) = compatible_versions(&plan.config).await {
        return BackendCapability::unavailable(
            ActiveContainmentBackend::Firecracker,
            guarantees,
            error,
        );
    }
    BackendCapability::available(
        ActiveContainmentBackend::Firecracker,
        guarantees,
        "Linux KVM, pinned jailer/firecracker, boot artifacts and jail paths validated; guest command transport must be attested separately",
    )
}

fn validate_instance_id(value: &str) -> FirecrackerResult<()> {
    if value.is_empty()
        || value.len() > 64
        || value.starts_with('-')
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    {
        return Err(FirecrackerError::InvalidConfig(
            "instance id must be 1-64 ASCII alphanumeric/hyphen characters and cannot start with '-'"
                .into(),
        ));
    }
    Ok(())
}

fn canonical_directory(path: &Path, label: &str) -> FirecrackerResult<PathBuf> {
    let metadata = std::fs::symlink_metadata(path).map_err(|error| {
        FirecrackerError::InvalidConfig(format!(
            "cannot inspect {label} {}: {error}",
            path.display()
        ))
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(FirecrackerError::InvalidConfig(format!(
            "{label} must be a real directory, not a symlink"
        )));
    }
    std::fs::canonicalize(path).map_err(|error| {
        FirecrackerError::InvalidConfig(format!(
            "cannot canonicalize {label} {}: {error}",
            path.display()
        ))
    })
}

fn canonical_secure_directory(
    path: &Path,
    label: &str,
    workspace: &Path,
) -> FirecrackerResult<PathBuf> {
    let canonical = canonical_directory(path, label)?;
    if canonical.starts_with(workspace) || workspace.starts_with(&canonical) {
        return Err(FirecrackerError::InvalidConfig(format!(
            "{label} and writable workspace must not contain one another"
        )));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let mode = std::fs::metadata(&canonical)
            .map_err(|error| FirecrackerError::InvalidConfig(error.to_string()))?
            .mode();
        if mode & 0o022 != 0 {
            return Err(FirecrackerError::InvalidConfig(format!(
                "{label} must not be group/world writable"
            )));
        }
    }
    Ok(canonical)
}

fn validate_immutable_file(
    file: &ImmutableHostFile,
    label: &str,
    workspace: Option<&Path>,
) -> FirecrackerResult<PathBuf> {
    file.validate_static(label)?;
    let metadata = std::fs::symlink_metadata(&file.path).map_err(|error| {
        FirecrackerError::InvalidConfig(format!(
            "cannot inspect {label} {}: {error}",
            file.path.display()
        ))
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(FirecrackerError::InvalidConfig(format!(
            "{label} must be a regular non-symlink file"
        )));
    }
    let canonical = std::fs::canonicalize(&file.path).map_err(|error| {
        FirecrackerError::InvalidConfig(format!(
            "cannot canonicalize {label} {}: {error}",
            file.path.display()
        ))
    })?;
    if workspace.is_some_and(|workspace| canonical.starts_with(workspace)) {
        return Err(FirecrackerError::InvalidConfig(format!(
            "{label} cannot come from the writable workspace"
        )));
    }
    let actual = hash_file(&canonical)?;
    if actual != file.sha256 {
        return Err(FirecrackerError::InvalidConfig(format!(
            "{label} hash mismatch: expected {}, found {actual}",
            file.sha256
        )));
    }
    Ok(canonical)
}

fn validate_immutable_executable(
    file: &ImmutableHostFile,
    label: &str,
    workspace: Option<&Path>,
) -> FirecrackerResult<PathBuf> {
    let canonical = validate_immutable_file(file, label, workspace)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if std::fs::metadata(&canonical)
            .map_err(|error| FirecrackerError::InvalidConfig(error.to_string()))?
            .permissions()
            .mode()
            & 0o111
            == 0
        {
            return Err(FirecrackerError::InvalidConfig(format!(
                "{label} must be executable"
            )));
        }
    }
    Ok(canonical)
}

fn revalidate_plan_artifacts(plan: &FirecrackerLaunchPlan) -> FirecrackerResult<()> {
    let jailer = validate_immutable_executable(&plan.config.jailer, "jailer", None)?;
    let firecracker = validate_immutable_executable(&plan.config.firecracker, "firecracker", None)?;
    let kernel = validate_immutable_file(&plan.config.kernel, "kernel", None)?;
    let rootfs = validate_immutable_file(&plan.config.rootfs, "rootfs", None)?;
    for (actual, expected, label) in [
        (jailer.as_path(), plan.jailer.as_path(), "jailer"),
        (
            firecracker.as_path(),
            plan.firecracker.as_path(),
            "firecracker",
        ),
        (kernel.as_path(), plan.config.kernel.path(), "kernel"),
        (rootfs.as_path(), plan.config.rootfs.path(), "rootfs"),
    ] {
        if actual != expected {
            return Err(FirecrackerError::Unavailable(format!(
                "{label} resolved to a different object after launch planning"
            )));
        }
    }
    Ok(())
}

async fn revalidate_plan_artifacts_async(plan: &FirecrackerLaunchPlan) -> FirecrackerResult<()> {
    let plan = plan.clone();
    tokio::task::spawn_blocking(move || revalidate_plan_artifacts(&plan))
        .await
        .map_err(|error| FirecrackerError::Unavailable(error.to_string()))?
}

fn hash_file(path: &Path) -> FirecrackerResult<String> {
    let mut file = File::open(path).map_err(|error| {
        FirecrackerError::InvalidConfig(format!("cannot open {}: {error}", path.display()))
    })?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let count = file.read(&mut buffer).map_err(|error| {
            FirecrackerError::InvalidConfig(format!("cannot hash {}: {error}", path.display()))
        })?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    Ok(format!("sha256:{}", lowercase_hex(&hasher.finalize())))
}

fn lowercase_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

fn validate_network_runtime(
    network: &FirecrackerNetwork,
    workspace: &Path,
) -> FirecrackerResult<FirecrackerNetwork> {
    if let FirecrackerNetwork::Tap {
        device,
        netns,
        guest_mac,
    } = network
    {
        let netns = canonical_immutable_runtime_path(netns, "network namespace", workspace)?;
        if !cfg!(target_os = "linux") {
            return Err(FirecrackerError::Unavailable(
                "TAP networking requires Linux".into(),
            ));
        }
        let network_device = Path::new("/sys/class/net").join(device);
        if !network_device.exists() {
            return Err(FirecrackerError::Unavailable(format!(
                "TAP device `{device}` does not exist"
            )));
        }
        if !network_device.join("tun_flags").is_file() {
            return Err(FirecrackerError::Unavailable(format!(
                "network device `{device}` is not a TUN/TAP interface"
            )));
        }
        if !netns.starts_with("/run") && !netns.starts_with("/var/run") {
            return Err(FirecrackerError::InvalidConfig(
                "network namespace must resolve under /run or /var/run".into(),
            ));
        }
        return Ok(FirecrackerNetwork::Tap {
            device: device.clone(),
            netns,
            guest_mac: guest_mac.clone(),
        });
    }
    Ok(FirecrackerNetwork::Disabled)
}

fn canonical_immutable_runtime_path(
    path: &Path,
    label: &str,
    workspace: &Path,
) -> FirecrackerResult<PathBuf> {
    let metadata = std::fs::symlink_metadata(path).map_err(|error| {
        FirecrackerError::InvalidConfig(format!("cannot inspect {label}: {error}"))
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(FirecrackerError::InvalidConfig(format!(
            "{label} must be a regular non-symlink file"
        )));
    }
    let canonical = std::fs::canonicalize(path)
        .map_err(|error| FirecrackerError::InvalidConfig(error.to_string()))?;
    if canonical.starts_with(workspace) {
        return Err(FirecrackerError::InvalidConfig(format!(
            "{label} cannot come from the writable workspace"
        )));
    }
    Ok(canonical)
}

fn copy_verified(source: &ImmutableHostFile, target: &Path, label: &str) -> FirecrackerResult<()> {
    std::fs::copy(&source.path, target)
        .map_err(|error| FirecrackerError::Staging(format!("cannot stage {label}: {error}")))?;
    let actual = hash_file(target)?;
    if actual != source.sha256 {
        let _ = std::fs::remove_file(target);
        return Err(FirecrackerError::Staging(format!(
            "staged {label} hash mismatch"
        )));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(target, std::fs::Permissions::from_mode(0o444))
            .map_err(|error| FirecrackerError::Staging(error.to_string()))?;
    }
    #[cfg(not(unix))]
    {
        let mut permissions = std::fs::metadata(target)
            .map_err(|error| FirecrackerError::Staging(error.to_string()))?
            .permissions();
        permissions.set_readonly(true);
        std::fs::set_permissions(target, permissions)
            .map_err(|error| FirecrackerError::Staging(error.to_string()))?;
    }
    Ok(())
}

fn set_owner_only_directory(path: &Path) -> FirecrackerResult<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
            .map_err(|error| FirecrackerError::Staging(error.to_string()))?;
    }
    Ok(())
}

fn remove_instance_dir(instance_dir: &Path, chroot_base: &Path) {
    if instance_dir.starts_with(chroot_base)
        && instance_dir != chroot_base
        && instance_dir.components().count() >= chroot_base.components().count() + 2
    {
        let _ = std::fs::remove_dir_all(instance_dir);
    }
}

async fn checked_version(executable: &Path) -> std::result::Result<String, String> {
    let mut command = Command::new(executable);
    command.arg("--version");
    command.env_clear();
    command.stdin(Stdio::null());
    command.stdout(Stdio::piped());
    command.stderr(Stdio::piped());
    command.kill_on_drop(true);
    let mut child = command
        .spawn()
        .map_err(|error| format!("{} --version failed: {error}", executable.display()))?;
    let mut stdout = child
        .stdout
        .take()
        .ok_or_else(|| "version stdout was not captured".to_string())?;
    let mut stderr = child
        .stderr
        .take()
        .ok_or_else(|| "version stderr was not captured".to_string())?;
    let capture = async {
        let (status, stdout, stderr) = tokio::join!(
            child.wait(),
            read_bounded_output(&mut stdout),
            read_bounded_output(&mut stderr),
        );
        Ok::<_, String>((status.map_err(|error| error.to_string())?, stdout?, stderr?))
    };
    let (status, stdout, stderr) = match tokio::time::timeout(Duration::from_secs(5), capture).await
    {
        Ok(result) => result?,
        Err(_) => {
            let _ = child.start_kill();
            let _ = child.wait().await;
            return Err(format!("{} --version timed out", executable.display()));
        }
    };
    if status.success() {
        let stdout = String::from_utf8_lossy(&stdout);
        let stderr = String::from_utf8_lossy(&stderr);
        let combined = format!("{stdout} {stderr}");
        combined
            .split_ascii_whitespace()
            .find(|part| {
                part.strip_prefix('v')
                    .and_then(|rest| rest.bytes().next())
                    .is_some_and(|byte| byte.is_ascii_digit())
            })
            .map(|version| {
                version
                    .trim_matches(|ch: char| ch == ',' || ch == ';')
                    .to_owned()
            })
            .ok_or_else(|| {
                format!(
                    "{} --version returned no parseable version",
                    executable.display()
                )
            })
    } else {
        Err(format!(
            "{} --version failed: {}",
            executable.display(),
            String::from_utf8_lossy(&stderr).trim()
        ))
    }
}

async fn read_bounded_output<R>(reader: &mut R) -> std::result::Result<Vec<u8>, String>
where
    R: AsyncRead + Unpin,
{
    let mut output = Vec::new();
    reader
        .take(MAX_VERSION_OUTPUT_BYTES as u64 + 1)
        .read_to_end(&mut output)
        .await
        .map_err(|error| error.to_string())?;
    if output.len() > MAX_VERSION_OUTPUT_BYTES {
        Err("version output exceeded limit".into())
    } else {
        Ok(output)
    }
}

async fn compatible_versions(config: &FirecrackerConfig) -> std::result::Result<(), String> {
    let jailer = checked_version(config.jailer.path()).await?;
    let firecracker = checked_version(config.firecracker.path()).await?;
    if jailer == firecracker {
        Ok(())
    } else {
        Err(format!(
            "jailer ({jailer}) and Firecracker ({firecracker}) versions do not match"
        ))
    }
}

async fn validate_live_linux_prerequisites(plan: &FirecrackerLaunchPlan) -> FirecrackerResult<()> {
    if !is_effective_root() {
        return Err(FirecrackerError::Unavailable(
            "the Firecracker jailer must be launched as root".into(),
        ));
    }
    OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/kvm")
        .map_err(|error| FirecrackerError::Unavailable(format!("cannot open /dev/kvm: {error}")))?;
    for (path, label) in [
        (plan.jailer.as_path(), "jailer"),
        (plan.firecracker.as_path(), "firecracker"),
        (plan.config.kernel.path(), "kernel"),
        (plan.config.rootfs.path(), "rootfs"),
        (plan.chroot_base.as_path(), "chroot_base"),
    ] {
        validate_root_owned_ancestry(path, label)?;
    }
    if let FirecrackerNetwork::Tap { netns, .. } = &plan.config.network {
        validate_root_owned_ancestry(netns, "network namespace")?;
    }
    revalidate_plan_artifacts_async(plan).await?;
    validate_static_elf(&plan.jailer, "jailer")?;
    validate_static_elf(&plan.firecracker, "firecracker")?;
    compatible_versions(&plan.config)
        .await
        .map_err(FirecrackerError::Unavailable)
}

#[cfg(unix)]
fn is_effective_root() -> bool {
    // SAFETY: `geteuid` has no preconditions and does not dereference memory.
    unsafe { libc::geteuid() == 0 }
}

#[cfg(not(unix))]
fn is_effective_root() -> bool {
    false
}

fn prepare_runtime_permissions(plan: &FirecrackerLaunchPlan) -> FirecrackerResult<()> {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::ffi::OsStrExt;
        use std::os::unix::fs::PermissionsExt;

        let runtime = plan.jail_root.join("run");
        let path = std::ffi::CString::new(runtime.as_os_str().as_bytes()).map_err(|_| {
            FirecrackerError::Staging("runtime directory contains an interior NUL".into())
        })?;
        // SAFETY: the CString is NUL-terminated and references a directory we just created.
        if unsafe { libc::chown(path.as_ptr(), plan.config.uid, plan.config.gid) } != 0 {
            return Err(FirecrackerError::Staging(format!(
                "cannot assign runtime directory to jail identity: {}",
                std::io::Error::last_os_error()
            )));
        }
        std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700))
            .map_err(|error| FirecrackerError::Staging(error.to_string()))?;
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = plan;
    }
    Ok(())
}

fn validate_root_owned_ancestry(path: &Path, label: &str) -> FirecrackerResult<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let mut current = Some(path);
        while let Some(component) = current {
            let metadata = std::fs::metadata(component).map_err(|error| {
                FirecrackerError::Unavailable(format!(
                    "cannot inspect trusted {label} path {}: {error}",
                    component.display()
                ))
            })?;
            if metadata.uid() != 0 || metadata.mode() & 0o022 != 0 {
                return Err(FirecrackerError::Unavailable(format!(
                    "trusted {label} path {} must be root-owned and not group/world writable",
                    component.display()
                )));
            }
            current = component.parent();
        }
    }
    #[cfg(not(unix))]
    {
        let _ = (path, label);
    }
    Ok(())
}

fn validate_static_elf(path: &Path, label: &str) -> FirecrackerResult<()> {
    const ELF_MAGIC: &[u8; 4] = b"\x7fELF";
    const PT_INTERP: u32 = 3;

    let mut file = File::open(path).map_err(|error| {
        FirecrackerError::Unavailable(format!(
            "cannot inspect {label} ELF {}: {error}",
            path.display()
        ))
    })?;
    let file_len = file
        .metadata()
        .map_err(|error| FirecrackerError::Unavailable(error.to_string()))?
        .len();
    let mut header = [0_u8; 64];
    file.read_exact(&mut header).map_err(|_| {
        FirecrackerError::Unavailable(format!("{label} is not a complete ELF executable"))
    })?;
    if &header[..4] != ELF_MAGIC {
        return Err(FirecrackerError::Unavailable(format!(
            "{label} must be a Linux ELF executable"
        )));
    }
    let little_endian = match header[5] {
        1 => true,
        2 => false,
        _ => {
            return Err(FirecrackerError::Unavailable(format!(
                "{label} ELF has an unsupported byte order"
            )))
        }
    };
    let (program_offset, entry_size, entry_count) = match header[4] {
        1 => (
            u64::from(decode_u32(&header[28..32], little_endian)),
            u64::from(decode_u16(&header[42..44], little_endian)),
            u64::from(decode_u16(&header[44..46], little_endian)),
        ),
        2 => (
            decode_u64(&header[32..40], little_endian),
            u64::from(decode_u16(&header[54..56], little_endian)),
            u64::from(decode_u16(&header[56..58], little_endian)),
        ),
        _ => {
            return Err(FirecrackerError::Unavailable(format!(
                "{label} ELF has an unsupported class"
            )))
        }
    };
    if entry_count == 0 || entry_count > 4_096 || !(4..=1_024).contains(&entry_size) {
        return Err(FirecrackerError::Unavailable(format!(
            "{label} ELF has an invalid program header table"
        )));
    }
    let table_len = entry_size.checked_mul(entry_count).ok_or_else(|| {
        FirecrackerError::Unavailable(format!("{label} ELF program header overflow"))
    })?;
    let table_end = program_offset.checked_add(table_len).ok_or_else(|| {
        FirecrackerError::Unavailable(format!("{label} ELF program header overflow"))
    })?;
    if table_end > file_len {
        return Err(FirecrackerError::Unavailable(format!(
            "{label} ELF program headers exceed the file"
        )));
    }
    for index in 0..entry_count {
        let offset = program_offset + index * entry_size;
        file.seek(SeekFrom::Start(offset))
            .and_then(|_| file.read_exact(&mut header[..4]))
            .map_err(|error| FirecrackerError::Unavailable(error.to_string()))?;
        if decode_u32(&header[..4], little_endian) == PT_INTERP {
            return Err(FirecrackerError::Unavailable(format!(
                "{label} must be statically linked for the Firecracker jailer"
            )));
        }
    }
    Ok(())
}

fn decode_u16(bytes: &[u8], little_endian: bool) -> u16 {
    let bytes = [bytes[0], bytes[1]];
    if little_endian {
        u16::from_le_bytes(bytes)
    } else {
        u16::from_be_bytes(bytes)
    }
}

fn decode_u32(bytes: &[u8], little_endian: bool) -> u32 {
    let bytes = [bytes[0], bytes[1], bytes[2], bytes[3]];
    if little_endian {
        u32::from_le_bytes(bytes)
    } else {
        u32::from_be_bytes(bytes)
    }
}

fn decode_u64(bytes: &[u8], little_endian: bool) -> u64 {
    let bytes = [
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ];
    if little_endian {
        u64::from_le_bytes(bytes)
    } else {
        u64::from_be_bytes(bytes)
    }
}

async fn wait_for_api_socket(path: &Path, timeout: Duration) -> FirecrackerResult<()> {
    #[cfg(unix)]
    {
        let deadline = Instant::now() + timeout;
        loop {
            let socket = path.to_owned();
            let ready = tokio::task::spawn_blocking(move || api_get_ready(&socket))
                .await
                .map_err(|error| FirecrackerError::Api(error.to_string()))?;
            if ready {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(FirecrackerError::Api(format!(
                    "API socket {} was not ready before timeout",
                    path.display()
                )));
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    #[cfg(not(unix))]
    {
        let _ = (path, timeout);
        Err(FirecrackerError::Unavailable(
            "Firecracker API sockets require Unix".into(),
        ))
    }
}

#[cfg(unix)]
fn api_get_ready(path: &Path) -> bool {
    use std::os::unix::net::UnixStream;
    let Ok(mut stream) = UnixStream::connect(path) else {
        return false;
    };
    let _ = stream.set_read_timeout(Some(Duration::from_millis(200)));
    let _ = stream.set_write_timeout(Some(Duration::from_millis(200)));
    if stream
        .write_all(b"GET /machine-config HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .is_err()
    {
        return false;
    }
    let mut prefix = [0_u8; 12];
    stream
        .read(&mut prefix)
        .is_ok_and(|count| count >= 9 && prefix.starts_with(b"HTTP/1.1 "))
}

async fn api_put(socket: &Path, route: &str, body: &Value) -> FirecrackerResult<()> {
    let socket = socket.to_owned();
    let route = route.to_owned();
    let body = serde_json::to_vec(body)
        .map_err(|error| FirecrackerError::Api(format!("cannot encode request: {error}")))?;
    tokio::task::spawn_blocking(move || api_put_blocking(&socket, &route, &body))
        .await
        .map_err(|error| FirecrackerError::Api(error.to_string()))?
}

#[cfg(unix)]
fn api_put_blocking(socket: &Path, route: &str, body: &[u8]) -> FirecrackerResult<()> {
    use std::os::unix::net::UnixStream;
    if !route.starts_with('/') || route.contains(['\r', '\n']) {
        return Err(FirecrackerError::Api("invalid API route".into()));
    }
    let mut stream = UnixStream::connect(socket).map_err(|error| {
        FirecrackerError::Api(format!("cannot connect to {}: {error}", socket.display()))
    })?;
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .and_then(|_| stream.set_write_timeout(Some(Duration::from_secs(2))))
        .map_err(|error| FirecrackerError::Api(error.to_string()))?;
    write!(
        stream,
        "PUT {route} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .and_then(|_| stream.write_all(body))
    .map_err(|error| FirecrackerError::Api(error.to_string()))?;
    let mut response = Vec::new();
    stream
        .take(MAX_API_RESPONSE_BYTES as u64 + 1)
        .read_to_end(&mut response)
        .map_err(|error| FirecrackerError::Api(error.to_string()))?;
    if response.len() > MAX_API_RESPONSE_BYTES {
        return Err(FirecrackerError::Api("API response exceeded limit".into()));
    }
    let status_line = response
        .split(|byte| *byte == b'\n')
        .next()
        .and_then(|line| std::str::from_utf8(line).ok())
        .unwrap_or_default();
    let status = status_line
        .split_ascii_whitespace()
        .nth(1)
        .and_then(|status| status.parse::<u16>().ok())
        .ok_or_else(|| FirecrackerError::Api("API returned malformed HTTP status".into()))?;
    if !(200..300).contains(&status) {
        return Err(FirecrackerError::Api(format!(
            "API route {route} returned HTTP {status}"
        )));
    }
    Ok(())
}

#[cfg(not(unix))]
fn api_put_blocking(_socket: &Path, _route: &str, _body: &[u8]) -> FirecrackerResult<()> {
    Err(FirecrackerError::Unavailable(
        "Firecracker API sockets require Unix".into(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[cfg(unix)]
    fn read_http_request(stream: &mut std::os::unix::net::UnixStream) -> String {
        stream
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();
        let mut request = Vec::new();
        let mut chunk = [0_u8; 1024];
        loop {
            let count = stream.read(&mut chunk).unwrap();
            assert!(count > 0, "client closed before completing HTTP request");
            request.extend_from_slice(&chunk[..count]);
            let Some(headers_end) = request.windows(4).position(|part| part == b"\r\n\r\n") else {
                continue;
            };
            let headers_end = headers_end + 4;
            let headers = String::from_utf8_lossy(&request[..headers_end]);
            let content_length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().unwrap())
                })
                .unwrap_or(0);
            if request.len() >= headers_end + content_length {
                return String::from_utf8_lossy(&request).into_owned();
            }
        }
    }

    fn write_artifact(dir: &Path, name: &str, body: &[u8]) -> ImmutableHostFile {
        let path = dir.join(name);
        std::fs::write(&path, body).unwrap();
        ImmutableHostFile::new(
            path,
            format!("sha256:{}", lowercase_hex(&Sha256::digest(body))),
        )
        .unwrap()
    }

    fn write_executable(dir: &Path, name: &str, body: &[u8]) -> ImmutableHostFile {
        let file = write_artifact(dir, name, body);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(file.path(), std::fs::Permissions::from_mode(0o500)).unwrap();
        }
        file
    }

    fn fixture() -> (tempfile::TempDir, tempfile::TempDir, FirecrackerConfig) {
        let immutable = tempfile::tempdir().unwrap();
        let chroot = tempfile::tempdir().unwrap();
        let jailer = write_executable(immutable.path(), "jailer", b"jailer-v1");
        let firecracker = write_executable(immutable.path(), "firecracker", b"firecracker-v1");
        let kernel = write_artifact(immutable.path(), "vmlinux", b"kernel-v1");
        let rootfs = write_artifact(immutable.path(), "rootfs.ext4", b"rootfs-v1");
        let config = FirecrackerConfig::new(
            jailer,
            firecracker,
            kernel,
            rootfs,
            chroot.path(),
            1000,
            1000,
        )
        .unwrap();
        (immutable, chroot, config)
    }

    #[test]
    fn config_rejects_mutable_paths_hashes_root_identity_and_network_injection() {
        assert!(ImmutableHostFile::new("relative", format!("sha256:{}", "a".repeat(64))).is_err());
        assert!(ImmutableHostFile::new("/tmp/file", "sha256:short").is_err());
        assert!(serde_json::from_value::<ImmutableHostFile>(json!({
            "path": "relative",
            "sha256": format!("sha256:{}", "a".repeat(64)),
        }))
        .is_err());
        assert!(serde_json::from_value::<ImmutableHostFile>(json!({
            "path": "/tmp/file",
            "sha256": format!("sha256:{}", "a".repeat(64)),
            "unexpected": true,
        }))
        .is_err());
        let (_immutable, _chroot, config) = fixture();
        let encoded = serde_json::to_value(&config).unwrap();
        for (field, value) in [
            ("uid", json!(0)),
            ("vcpu_count", json!(0)),
            ("memory_mib", json!(32)),
            ("api_timeout_ms", json!(0)),
        ] {
            let mut tampered = encoded.clone();
            tampered[field] = value;
            assert!(serde_json::from_value::<FirecrackerConfig>(tampered).is_err());
        }
        assert!(config.clone().with_resources(3, 512).is_err());
        assert!(config
            .clone()
            .with_network(FirecrackerNetwork::Tap {
                device: "tap0;reboot".into(),
                netns: PathBuf::from("/run/netns/a"),
                guest_mac: "06:00:00:00:00:01".into(),
            })
            .is_err());
        assert!(config
            .clone()
            .with_network(FirecrackerNetwork::Tap {
                device: "tap0".into(),
                netns: PathBuf::from("/run/netns/a"),
                guest_mac: "00:00:00:00:00:00".into(),
            })
            .is_err());
        assert!(config
            .clone()
            .with_network(FirecrackerNetwork::Tap {
                device: "tap0".into(),
                netns: PathBuf::from("/run/netns/a"),
                guest_mac: "6:00:00:00:00:01".into(),
            })
            .is_err());
    }

    #[test]
    fn launch_plan_is_jailer_based_argv_safe_and_not_a_shell_command() {
        let (immutable, _chroot, config) = fixture();
        let workspace = tempfile::tempdir().unwrap();
        let plan = FirecrackerLaunchPlan::build(&config, "vm-safe-1", workspace.path()).unwrap();
        assert_eq!(
            plan.jailer_executable(),
            std::fs::canonicalize(config.jailer().path()).unwrap()
        );
        assert_eq!(
            plan.firecracker_executable(),
            std::fs::canonicalize(config.firecracker().path()).unwrap()
        );
        let args = plan
            .args()
            .iter()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert!(args.windows(2).any(|pair| pair == ["--id", "vm-safe-1"]));
        assert!(args
            .windows(2)
            .any(|pair| pair == ["--api-sock", API_SOCKET_IN_JAIL]));
        assert!(!args.iter().any(|arg| arg == "-c" || arg.contains("$(")));
        assert!(!args.iter().any(|arg| arg == "--new-pid-ns"));
        assert!(FirecrackerLaunchPlan::build(&config, "vm_bad", workspace.path()).is_err());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(
                config.jailer().path(),
                std::fs::Permissions::from_mode(0o700),
            )
            .unwrap();
        }
        std::fs::write(config.jailer().path(), b"changed after planning").unwrap();
        assert!(revalidate_plan_artifacts(&plan).is_err());
        drop(immutable);
    }

    #[test]
    fn artifacts_from_writable_workspace_and_hash_drift_fail_closed() {
        let (_immutable, _chroot, mut config) = fixture();
        let workspace = tempfile::tempdir().unwrap();
        config.kernel = write_artifact(workspace.path(), "kernel", b"workspace-kernel");
        assert!(matches!(
            FirecrackerLaunchPlan::build(&config, "vm1", workspace.path()),
            Err(FirecrackerError::InvalidConfig(_))
        ));

        let (_immutable, _chroot, config) = fixture();
        std::fs::write(config.rootfs().path(), b"drifted").unwrap();
        assert!(matches!(
            FirecrackerLaunchPlan::build(&config, "vm2", workspace.path()),
            Err(FirecrackerError::InvalidConfig(_))
        ));
    }

    #[cfg(unix)]
    #[test]
    fn symlinks_non_executables_and_mutable_jail_paths_fail_closed() {
        use std::os::unix::fs::{symlink, PermissionsExt};

        let (immutable, _chroot, mut config) = fixture();
        let workspace = tempfile::tempdir().unwrap();
        let kernel_link = immutable.path().join("kernel-link");
        symlink(config.kernel().path(), &kernel_link).unwrap();
        config.kernel = ImmutableHostFile::new(kernel_link, config.kernel().sha256()).unwrap();
        assert!(FirecrackerLaunchPlan::build(&config, "vm1", workspace.path()).is_err());

        let (_immutable, _chroot, config) = fixture();
        std::fs::set_permissions(
            config.jailer().path(),
            std::fs::Permissions::from_mode(0o600),
        )
        .unwrap();
        assert!(FirecrackerLaunchPlan::build(&config, "vm2", workspace.path()).is_err());

        let (_immutable, _chroot, config) = fixture();
        std::fs::set_permissions(config.chroot_base(), std::fs::Permissions::from_mode(0o777))
            .unwrap();
        assert!(FirecrackerLaunchPlan::build(&config, "vm3", workspace.path()).is_err());
    }

    #[test]
    fn live_binary_validation_rejects_scripts_dynamic_elf_and_truncated_headers() {
        let temp = tempfile::tempdir().unwrap();
        let script = temp.path().join("script");
        std::fs::write(&script, b"#!/bin/sh\n").unwrap();
        assert!(validate_static_elf(&script, "jailer").is_err());

        let truncated = temp.path().join("truncated");
        std::fs::write(&truncated, b"\x7fELF").unwrap();
        assert!(validate_static_elf(&truncated, "jailer").is_err());

        let mut elf = vec![0_u8; 120];
        elf[..4].copy_from_slice(b"\x7fELF");
        elf[4] = 2;
        elf[5] = 1;
        elf[32..40].copy_from_slice(&64_u64.to_le_bytes());
        elf[54..56].copy_from_slice(&56_u16.to_le_bytes());
        elf[56..58].copy_from_slice(&1_u16.to_le_bytes());
        elf[64..68].copy_from_slice(&3_u32.to_le_bytes());
        let dynamic = temp.path().join("dynamic");
        std::fs::write(&dynamic, &elf).unwrap();
        assert!(validate_static_elf(&dynamic, "firecracker").is_err());

        elf[64..68].copy_from_slice(&1_u32.to_le_bytes());
        let static_binary = temp.path().join("static");
        std::fs::write(&static_binary, elf).unwrap();
        validate_static_elf(&static_binary, "firecracker").unwrap();
    }

    #[test]
    fn staging_is_fresh_hash_verified_and_drop_cleans_only_instance() {
        let (_immutable, _chroot, config) = fixture();
        let workspace = tempfile::tempdir().unwrap();
        let plan = FirecrackerLaunchPlan::build(&config, "cleanup-vm", workspace.path()).unwrap();
        let instance_dir = plan.instance_dir.clone();
        let sibling = plan.chroot_base.join("keep-me");
        std::fs::create_dir(&sibling).unwrap();
        let staging = plan.stage().unwrap();
        assert!(staging.instance_dir().join("root/assets/vmlinux").is_file());
        assert!(staging
            .instance_dir()
            .join("root/assets/rootfs.ext4")
            .is_file());
        assert!(
            plan.stage().is_err(),
            "existing instance must never be reused"
        );
        drop(staging);
        assert!(!instance_dir.exists());
        assert!(sibling.exists());
    }

    #[derive(Default)]
    struct CapturingReaper {
        job: Mutex<Option<(Child, FirecrackerStaging)>>,
    }

    impl FirecrackerReaper for CapturingReaper {
        fn reap(&self, child: Child, staging: FirecrackerStaging) {
            *self.job.lock().unwrap() = Some((child, staging));
        }
    }

    #[tokio::test]
    async fn cancelled_owner_transfers_child_and_staging_before_cleanup() {
        let temp = tempfile::tempdir().unwrap();
        let instance_dir = temp.path().join("firecracker/vm-cancelled");
        std::fs::create_dir_all(&instance_dir).unwrap();
        let staging = FirecrackerStaging {
            instance_dir: instance_dir.clone(),
            chroot_base: temp.path().to_owned(),
        };
        let child = Command::new(std::env::current_exe().unwrap())
            .arg("--help")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let reaper = Arc::new(CapturingReaper::default());
        let owned_reaper = reaper.clone();
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let launch = tokio::spawn(async move {
            let _supervisor =
                FirecrackerProcessSupervisor::new_with_reaper(child, staging, owned_reaper);
            let _ = ready_tx.send(());
            std::future::pending::<()>().await;
        });
        ready_rx.await.unwrap();
        launch.abort();
        let _ = launch.await;

        let (mut child, staging) = reaper.job.lock().unwrap().take().unwrap();
        assert!(
            instance_dir.exists(),
            "jail must remain owned until child wait"
        );
        let _ = child.start_kill();
        child.wait().await.unwrap();
        drop(staging);
        assert!(!instance_dir.exists());
    }

    #[test]
    fn failed_reap_retains_jail_instead_of_racing_unknown_process_liveness() {
        let temp = tempfile::tempdir().unwrap();
        let instance_dir = temp.path().join("firecracker/vm-wait-error");
        std::fs::create_dir_all(&instance_dir).unwrap();
        let staging = FirecrackerStaging {
            instance_dir: instance_dir.clone(),
            chroot_base: temp.path().to_owned(),
        };
        finish_reap(staging, false);
        assert!(instance_dir.exists());
        std::fs::remove_dir_all(instance_dir).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn api_readiness_requires_live_http_and_puts_are_bounded() {
        use std::os::unix::net::UnixListener;

        let temp = tempfile::tempdir().unwrap();
        let socket = temp.path().join("api.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        listener.set_nonblocking(false).unwrap();
        let routes = Arc::new(Mutex::new(Vec::new()));
        let seen = routes.clone();
        let server = std::thread::spawn(move || {
            for _ in 0..2 {
                let (mut stream, _) = listener.accept().unwrap();
                let request = read_http_request(&mut stream);
                seen.lock()
                    .unwrap()
                    .push(request.lines().next().unwrap_or_default().to_string());
                stream
                    .write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n")
                    .unwrap();
            }
        });
        wait_for_api_socket(&socket, Duration::from_secs(1))
            .await
            .unwrap();
        api_put(&socket, "/machine-config", &json!({"vcpu_count": 1}))
            .await
            .unwrap();
        server.join().unwrap();
        assert_eq!(
            *routes.lock().unwrap(),
            vec![
                "GET /machine-config HTTP/1.1",
                "PUT /machine-config HTTP/1.1"
            ]
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn api_rejects_oversized_responses_and_route_injection() {
        use std::os::unix::net::UnixListener;

        let temp = tempfile::tempdir().unwrap();
        let socket = temp.path().join("api.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let _ = read_http_request(&mut stream);
            let mut response = b"HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n".to_vec();
            response.extend(std::iter::repeat_n(b'x', MAX_API_RESPONSE_BYTES));
            let _ = stream.write_all(&response);
        });
        let error = api_put(&socket, "/machine-config", &json!({}))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("exceeded limit"));
        server.join().unwrap();

        let injected =
            api_put_blocking(Path::new("/unreachable"), "/actions\r\nX-Evil: yes", b"{}")
                .unwrap_err();
        assert!(injected.to_string().contains("invalid API route"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn mismatched_jailer_and_firecracker_versions_fail_closed() {
        let immutable = tempfile::tempdir().unwrap();
        let chroot = tempfile::tempdir().unwrap();
        let jailer = write_executable(
            immutable.path(),
            "jailer",
            b"#!/bin/sh\nprintf 'jailer v1.2.3\\n'\n",
        );
        let firecracker = write_executable(
            immutable.path(),
            "firecracker",
            b"#!/bin/sh\nprintf 'Firecracker v1.2.4\\n'\n",
        );
        let kernel = write_artifact(immutable.path(), "vmlinux", b"kernel-v1");
        let rootfs = write_artifact(immutable.path(), "rootfs.ext4", b"rootfs-v1");
        let config = FirecrackerConfig::new(
            jailer,
            firecracker,
            kernel,
            rootfs,
            chroot.path(),
            1000,
            1000,
        )
        .unwrap();
        let error = compatible_versions(&config).await.unwrap_err();
        assert!(error.contains("versions do not match"));

        let mut noisy_script = b"#!/bin/sh\nprintf 'v1.2.3 ".to_vec();
        noisy_script.extend(std::iter::repeat_n(b'x', MAX_VERSION_OUTPUT_BYTES + 1));
        noisy_script.extend_from_slice(b"'\n");
        let noisy = write_executable(immutable.path(), "noisy", &noisy_script);
        let error = checked_version(noisy.path()).await.unwrap_err();
        assert!(error.contains("version output exceeded limit"));
    }

    #[tokio::test]
    async fn non_linux_capability_never_claims_escape_proof() {
        let (_immutable, _chroot, config) = fixture();
        let workspace = tempfile::tempdir().unwrap();
        let capability = firecracker_capability(&config, workspace.path()).await;
        if !cfg!(target_os = "linux") {
            assert!(!capability.available);
            assert!(capability.detail.contains("validation-only"));
        }
    }
}
