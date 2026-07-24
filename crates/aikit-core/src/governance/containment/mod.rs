//! OS-backed containment for the built-in Bash tool.
//!
//! Containment is deliberately separate from [`BashPolicy`](super::process::BashPolicy)'s
//! portable process hardening. A required policy never falls back to a host shell: if no selected
//! backend can be proved available, command preparation fails before untrusted code starts.

mod container;
mod firecracker;
mod linux;
mod macos;
mod windows;

pub use firecracker::{
    firecracker_capability, FirecrackerConfig, FirecrackerError, FirecrackerLaunchPlan,
    FirecrackerNetwork, FirecrackerResult, FirecrackerStaging, FirecrackerVm, ImmutableHostFile,
};

use crate::error::{AikitError, Result};
use serde::{Deserialize, Serialize};
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::process::Command;

const DOCKER_OWNERSHIP_WAIT: std::time::Duration = std::time::Duration::from_secs(5);

/// Which backend a required containment policy should use.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BackendSelector {
    /// Prefer the native backend for the host OS, otherwise use configured Docker.
    Auto,
    Native,
    Seatbelt,
    Docker,
}

/// Required containment is fail-closed. Uncontained execution must be an explicit choice.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "mode", content = "backend")]
pub enum ContainmentRequirement {
    Required(BackendSelector),
    Uncontained,
}

/// Docker runtime settings. The image must be immutable (`name@sha256:...` or `sha256:...`) and
/// already present locally; command execution never performs an implicit image pull.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DockerConfig {
    pub executable: PathBuf,
    pub image: String,
    pub pids_limit: u32,
    pub memory_bytes: u64,
    pub cpus: u32,
    pub tmpfs_bytes: u64,
}

impl DockerConfig {
    pub fn new(image: impl Into<String>) -> Self {
        DockerConfig {
            executable: PathBuf::from("docker"),
            image: image.into(),
            pids_limit: 64,
            memory_bytes: 512 << 20,
            cpus: 1,
            tmpfs_bytes: 64 << 20,
        }
    }

    pub fn with_executable(mut self, executable: impl Into<PathBuf>) -> Self {
        self.executable = executable.into();
        self
    }
}

/// Containment configuration for Bash. `Default` is the safe `Required(Auto)` posture.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContainmentPolicy {
    pub requirement: ContainmentRequirement,
    /// Auto may fall back to Docker only when an immutable image is explicitly configured.
    pub docker: Option<DockerConfig>,
}

impl Default for ContainmentPolicy {
    fn default() -> Self {
        Self::required_auto()
    }
}

impl ContainmentPolicy {
    pub fn required_auto() -> Self {
        ContainmentPolicy {
            requirement: ContainmentRequirement::Required(BackendSelector::Auto),
            docker: None,
        }
    }

    pub fn required_seatbelt() -> Self {
        ContainmentPolicy {
            requirement: ContainmentRequirement::Required(BackendSelector::Seatbelt),
            docker: None,
        }
    }

    pub fn required_native() -> Self {
        ContainmentPolicy {
            requirement: ContainmentRequirement::Required(BackendSelector::Native),
            docker: None,
        }
    }

    pub fn required_docker(config: DockerConfig) -> Self {
        ContainmentPolicy {
            requirement: ContainmentRequirement::Required(BackendSelector::Docker),
            docker: Some(config),
        }
    }

    pub fn with_docker_fallback(mut self, config: DockerConfig) -> Self {
        self.docker = Some(config);
        self
    }

    pub fn uncontained() -> Self {
        ContainmentPolicy {
            requirement: ContainmentRequirement::Uncontained,
            docker: None,
        }
    }

    pub fn is_required(&self) -> bool {
        matches!(self.requirement, ContainmentRequirement::Required(_))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActiveContainmentBackend {
    Seatbelt,
    LinuxNamespace,
    WindowsJob,
    Docker,
    /// Low-level lifecycle capability only; the Bash path does not select this backend until a
    /// guest command/workspace transport is available.
    Firecracker,
    Uncontained,
}

/// Mechanism-level guarantees reported honestly instead of flattening every backend to a single
/// "sandboxed" boolean.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContainmentGuarantees {
    pub filesystem_write_boundary: bool,
    pub sensitive_home_read_boundary: bool,
    pub network_boundary: bool,
    pub descendant_inheritance: bool,
    pub syscall_filter: bool,
    pub resource_limits: bool,
}

impl ContainmentGuarantees {
    const fn seatbelt() -> Self {
        ContainmentGuarantees {
            filesystem_write_boundary: true,
            sensitive_home_read_boundary: true,
            network_boundary: true,
            descendant_inheritance: true,
            syscall_filter: false,
            resource_limits: true,
        }
    }

    const fn docker() -> Self {
        ContainmentGuarantees {
            filesystem_write_boundary: true,
            sensitive_home_read_boundary: true,
            network_boundary: true,
            descendant_inheritance: true,
            syscall_filter: true,
            resource_limits: true,
        }
    }

    const fn linux_namespace() -> Self {
        ContainmentGuarantees {
            filesystem_write_boundary: true,
            // The current bwrap profile makes the host root read-only, but it does not mask the
            // user's home. Read-only is a write boundary, not a sensitive-data read boundary.
            sensitive_home_read_boundary: false,
            network_boundary: true,
            descendant_inheritance: true,
            syscall_filter: true,
            resource_limits: true,
        }
    }

    const fn firecracker(_network: &FirecrackerNetwork) -> Self {
        ContainmentGuarantees {
            filesystem_write_boundary: true,
            sensitive_home_read_boundary: true,
            // Disabled mode exposes no NIC; TAP mode always joins an explicitly pinned network
            // namespace. What that namespace may reach remains the operator's egress policy.
            network_boundary: true,
            descendant_inheritance: true,
            // Firecracker enables its production seccomp filters by default and this backend
            // intentionally exposes no switch that disables them.
            syscall_filter: true,
            // Guest RAM/vCPU sizing is enforced, but the host VMM is not yet placed into a
            // caller-supplied cgroup. Do not overstate this as complete host resource limiting.
            resource_limits: false,
        }
    }

    const fn windows_job() -> Self {
        ContainmentGuarantees {
            filesystem_write_boundary: false,
            sensitive_home_read_boundary: false,
            network_boundary: false,
            descendant_inheritance: true,
            syscall_filter: false,
            resource_limits: true,
        }
    }

    const fn none() -> Self {
        ContainmentGuarantees {
            filesystem_write_boundary: false,
            sensitive_home_read_boundary: false,
            network_boundary: false,
            descendant_inheritance: false,
            syscall_filter: false,
            resource_limits: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackendCapability {
    pub backend: ActiveContainmentBackend,
    pub available: bool,
    pub guarantees: ContainmentGuarantees,
    pub detail: String,
}

impl BackendCapability {
    pub(crate) fn unavailable(
        backend: ActiveContainmentBackend,
        guarantees: ContainmentGuarantees,
        detail: impl Into<String>,
    ) -> Self {
        BackendCapability {
            backend,
            available: false,
            guarantees,
            detail: detail.into(),
        }
    }

    pub(crate) fn available(
        backend: ActiveContainmentBackend,
        guarantees: ContainmentGuarantees,
        detail: impl Into<String>,
    ) -> Self {
        BackendCapability {
            backend,
            available: true,
            guarantees,
            detail: detail.into(),
        }
    }
}

/// Result of actively probing the configured backends.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContainmentCapabilityReport {
    pub requirement: ContainmentRequirement,
    pub selected_backend: Option<ActiveContainmentBackend>,
    pub fail_closed: bool,
    pub backends: Vec<BackendCapability>,
}

/// Resource settings translated by backends that launch through a service (Docker), where host
/// `setrlimit` calls on the client process would not constrain the actual sandbox process.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct ContainmentLimits {
    pub max_cpu_seconds: Option<u64>,
    pub max_file_size_bytes: Option<u64>,
    pub max_open_files: Option<u64>,
    pub max_processes: Option<u64>,
}

#[derive(Debug, Clone)]
pub(crate) enum CleanupAction {
    Docker {
        executable: PathBuf,
        cidfile: PathBuf,
        ownership_label: String,
        environment: Vec<(OsString, OsString)>,
        artifact: Arc<tempfile::TempDir>,
    },
}

impl CleanupAction {
    pub(crate) async fn force(&self) -> bool {
        let deadline = std::time::Instant::now() + DOCKER_OWNERSHIP_WAIT;
        let ownership = if let Some(container_id) = self.owned_container_id() {
            DockerOwnership::Owned(container_id)
        } else {
            self.label_ownership(deadline).await
        };
        let DockerOwnership::Owned(container_id) = ownership else {
            // Before an owned cidfile exists, an empty label query can race a daemon-side create.
            // Keep the Drop guard armed so it polls through the shared ownership deadline.
            return false;
        };
        if self.cleanup_owned(&container_id, deadline).await == Some(true) {
            return true;
        }
        matches!(
            self.label_ownership(deadline).await,
            DockerOwnership::Absent
        )
    }

    /// Cancellation cannot await cleanup. Start an argv-safe cleanup process synchronously from
    /// `Drop`, then reap it on a detached thread. Spawning before `Drop` returns avoids depending
    /// on the cancelled Tokio task/runtime to schedule the cleanup; the normal timeout path above
    /// still awaits cleanup deterministically.
    pub(crate) fn spawn_best_effort(&self) {
        let action = self.clone();
        // The detached worker owns the private artifact while Docker finishes writing its cidfile.
        // This closes cancellation between client spawn and ownership proof without ever falling
        // back to a predictable name.
        let _ = std::thread::Builder::new()
            .name("aikit-containment-cleanup".into())
            .spawn(move || {
                let deadline = std::time::Instant::now() + DOCKER_OWNERSHIP_WAIT;
                while std::time::Instant::now() < deadline {
                    let ownership = action
                        .owned_container_id()
                        .map(DockerOwnership::Owned)
                        .unwrap_or_else(|| action.label_ownership_blocking(deadline));
                    match ownership {
                        // On cancellation the daemon may not have completed create yet. An empty
                        // label query is therefore retried until the shared deadline.
                        DockerOwnership::Absent | DockerOwnership::Unknown => {}
                        DockerOwnership::Owned(container_id) => {
                            if action.cleanup_owned_blocking(&container_id, deadline) == Some(true)
                                || matches!(
                                    action.label_ownership_blocking(deadline),
                                    DockerOwnership::Absent
                                )
                            {
                                return;
                            }
                        }
                    }
                    if std::time::Instant::now() < deadline {
                        std::thread::sleep(std::time::Duration::from_millis(50));
                    }
                }
            });
    }

    #[cfg(test)]
    fn best_effort_command(&self) -> Option<std::process::Command> {
        self.owned_container_id()
            .map(|container_id| self.best_effort_cleanup_command(&container_id))
    }

    fn owned_container_id(&self) -> Option<String> {
        match self {
            CleanupAction::Docker {
                cidfile, artifact, ..
            } => {
                if cidfile.parent() != Some(artifact.path()) {
                    return None;
                }
                owned_container_id(cidfile)
            }
        }
    }

    async fn label_ownership(&self, deadline: std::time::Instant) -> DockerOwnership {
        use tokio::io::AsyncReadExt;

        let Some(remaining) = deadline.checked_duration_since(std::time::Instant::now()) else {
            return DockerOwnership::Unknown;
        };
        let mut cmd = self.tokio_label_query_command();
        let Ok(mut child) = cmd.spawn() else {
            return DockerOwnership::Unknown;
        };
        let stdout = child.stdout.take().expect("piped Docker label stdout");
        let operation = async move {
            let mut output = Vec::new();
            let mut limited = stdout.take(130);
            let (read, status) = tokio::join!(limited.read_to_end(&mut output), child.wait());
            (read, status, output)
        };
        match tokio::time::timeout(remaining, operation).await {
            Ok((Ok(_), Ok(status), output)) if status.success() => {
                parse_container_ownership(&output)
            }
            _ => DockerOwnership::Unknown,
        }
    }

    fn label_ownership_blocking(&self, deadline: std::time::Instant) -> DockerOwnership {
        use std::io::Read;

        let mut cmd = self.best_effort_label_query_command();
        let Ok(mut child) = cmd.spawn() else {
            return DockerOwnership::Unknown;
        };
        loop {
            match child.try_wait() {
                Ok(Some(status)) => {
                    let mut stdout = Vec::new();
                    if let Some(pipe) = child.stdout.take() {
                        let _ = pipe.take(130).read_to_end(&mut stdout);
                    }
                    return if status.success() {
                        parse_container_ownership(&stdout)
                    } else {
                        DockerOwnership::Unknown
                    };
                }
                Ok(None) if std::time::Instant::now() < deadline => {
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }
                _ => {
                    let _ = child.kill();
                    let _ = child.wait();
                    return DockerOwnership::Unknown;
                }
            }
        }
    }

    async fn cleanup_owned(
        &self,
        container_id: &str,
        deadline: std::time::Instant,
    ) -> Option<bool> {
        let remaining = deadline.checked_duration_since(std::time::Instant::now())?;
        let mut cmd = self.tokio_cleanup_command(container_id);
        match tokio::time::timeout(remaining, cmd.status()).await {
            Ok(Ok(status)) => Some(status.success()),
            _ => None,
        }
    }

    fn cleanup_owned_blocking(
        &self,
        container_id: &str,
        deadline: std::time::Instant,
    ) -> Option<bool> {
        let mut cmd = self.best_effort_cleanup_command(container_id);
        let mut child = cmd.spawn().ok()?;
        loop {
            match child.try_wait() {
                Ok(Some(status)) => return Some(status.success()),
                Ok(None) if std::time::Instant::now() < deadline => {
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }
                _ => {
                    let _ = child.kill();
                    let _ = child.wait();
                    return None;
                }
            }
        }
    }

    fn tokio_cleanup_command(&self, container_id: &str) -> Command {
        let CleanupAction::Docker {
            executable,
            environment,
            ..
        } = self;
        let mut cmd = Command::new(executable);
        cmd.arg("rm").arg("-f").arg(container_id);
        cmd.stdin(std::process::Stdio::null());
        cmd.stdout(std::process::Stdio::null());
        cmd.stderr(std::process::Stdio::null());
        cmd.kill_on_drop(true);
        cmd.env_clear();
        cmd.envs(environment.clone());
        cmd
    }

    fn best_effort_cleanup_command(&self, container_id: &str) -> std::process::Command {
        let CleanupAction::Docker {
            executable,
            environment,
            ..
        } = self;
        let mut cmd = std::process::Command::new(executable);
        cmd.arg("rm").arg("-f").arg(container_id);
        cmd.stdin(std::process::Stdio::null());
        cmd.stdout(std::process::Stdio::null());
        cmd.stderr(std::process::Stdio::null());
        cmd.env_clear();
        cmd.envs(environment.clone());
        cmd
    }

    fn tokio_label_query_command(&self) -> Command {
        let CleanupAction::Docker {
            executable,
            ownership_label,
            environment,
            ..
        } = self;
        let mut cmd = Command::new(executable);
        cmd.args(["ps", "-aq", "--no-trunc", "--filter"])
            .arg(format!("label={ownership_label}"));
        cmd.stdin(std::process::Stdio::null());
        cmd.stdout(std::process::Stdio::piped());
        cmd.stderr(std::process::Stdio::null());
        cmd.kill_on_drop(true);
        cmd.env_clear();
        cmd.envs(environment.clone());
        cmd
    }

    fn best_effort_label_query_command(&self) -> std::process::Command {
        let CleanupAction::Docker {
            executable,
            ownership_label,
            environment,
            ..
        } = self;
        let mut cmd = std::process::Command::new(executable);
        cmd.args(["ps", "-aq", "--no-trunc", "--filter"])
            .arg(format!("label={ownership_label}"));
        cmd.stdin(std::process::Stdio::null());
        cmd.stdout(std::process::Stdio::piped());
        cmd.stderr(std::process::Stdio::null());
        cmd.env_clear();
        cmd.envs(environment.clone());
        cmd
    }
}

enum DockerOwnership {
    Owned(String),
    Absent,
    Unknown,
}

fn parse_container_ownership(output: &[u8]) -> DockerOwnership {
    if output.len() >= 130 {
        return DockerOwnership::Unknown;
    }
    let decoded = String::from_utf8_lossy(output);
    let mut lines = decoded.lines().filter(|line| !line.trim().is_empty());
    let Some(first) = lines.next() else {
        return DockerOwnership::Absent;
    };
    let Some(container_id) = validated_container_id(first.trim()) else {
        return DockerOwnership::Unknown;
    };
    if lines.next().is_some() {
        DockerOwnership::Unknown
    } else {
        DockerOwnership::Owned(container_id)
    }
}

/// A private cidfile is Docker's proof that this launch created a container. Refuse names, short
/// IDs, options, partial writes, and attacker-controlled file contents: cleanup is authorized only
/// for the full immutable ID written by the successful `docker run --cidfile` operation.
fn owned_container_id(cidfile: &Path) -> Option<String> {
    let metadata = std::fs::metadata(cidfile).ok()?;
    if !metadata.is_file() || metadata.len() > 129 {
        return None;
    }
    let container_id = std::fs::read_to_string(cidfile).ok()?;
    validated_container_id(container_id.trim())
}

fn validated_container_id(container_id: &str) -> Option<String> {
    (container_id.len() == 64 && container_id.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .then(|| container_id.to_ascii_lowercase())
}

pub(crate) struct PreparedCommand {
    pub command: Command,
    pub backend: ActiveContainmentBackend,
    pub environment_overrides: Vec<(OsString, OsString)>,
    pub cleanup: Option<CleanupAction>,
    /// Keeps private profile/home/temp directories alive until the child finishes.
    pub artifacts: Vec<tempfile::TempDir>,
}

/// The host operating system, resolved once so the backend-selection decision is a pure function
/// of policy plus probe results rather than of compile-time `cfg!`. This lets the selection logic
/// (including every fail-closed path) be exhaustively unit-tested on any single platform.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HostOs {
    MacOs,
    Linux,
    Windows,
    Other,
}

const fn current_host_os() -> HostOs {
    if cfg!(target_os = "macos") {
        HostOs::MacOs
    } else if cfg!(target_os = "linux") {
        HostOs::Linux
    } else if cfg!(target_os = "windows") {
        HostOs::Windows
    } else {
        HostOs::Other
    }
}

/// The native backend for a host, gated by whether that backend actually probed available. Returns
/// `None` on a host with no native backend, or when the native backend is unavailable — never a
/// different host's backend.
fn native_backend(
    host: HostOs,
    seatbelt: bool,
    linux: bool,
    windows: bool,
) -> Option<ActiveContainmentBackend> {
    match host {
        HostOs::MacOs if seatbelt => Some(ActiveContainmentBackend::Seatbelt),
        HostOs::Linux if linux => Some(ActiveContainmentBackend::LinuxNamespace),
        HostOs::Windows if windows => Some(ActiveContainmentBackend::WindowsJob),
        _ => None,
    }
}

/// Select the containment backend for a required policy from the host and per-backend availability.
/// Pure and fail-closed: an explicit selector never silently substitutes another backend, `Native`
/// never falls back to Docker, and any unsatisfiable request yields `None`. `Uncontained` is handled
/// by the caller before this point, so it maps to `None` here and is never reached in practice.
fn select_backend(
    requirement: ContainmentRequirement,
    host: HostOs,
    seatbelt: bool,
    linux: bool,
    windows: bool,
    docker: bool,
) -> Option<ActiveContainmentBackend> {
    match requirement {
        ContainmentRequirement::Required(BackendSelector::Seatbelt) if seatbelt => {
            Some(ActiveContainmentBackend::Seatbelt)
        }
        ContainmentRequirement::Required(BackendSelector::Docker) if docker => {
            Some(ActiveContainmentBackend::Docker)
        }
        ContainmentRequirement::Required(BackendSelector::Native) => {
            native_backend(host, seatbelt, linux, windows)
        }
        ContainmentRequirement::Required(BackendSelector::Auto) => {
            native_backend(host, seatbelt, linux, windows)
                .or(docker.then_some(ActiveContainmentBackend::Docker))
        }
        _ => None,
    }
}

/// Probe every configured backend and select one according to policy. This is public so services
/// can fail during startup instead of waiting for the first tool invocation.
pub async fn containment_capabilities(
    policy: &ContainmentPolicy,
    workdir: Option<&Path>,
) -> ContainmentCapabilityReport {
    let host_environment = std::env::vars_os().collect::<Vec<_>>();
    containment_capabilities_with_environment(policy, workdir, &host_environment).await
}

pub(crate) async fn containment_capabilities_with_environment(
    policy: &ContainmentPolicy,
    workdir: Option<&Path>,
    environment: &[(OsString, OsString)],
) -> ContainmentCapabilityReport {
    let docker_control_environment = container::docker_control_environment(environment);
    containment_capabilities_with_docker_control_environment(
        policy,
        workdir,
        &docker_control_environment,
    )
    .await
}

async fn containment_capabilities_with_docker_control_environment(
    policy: &ContainmentPolicy,
    workdir: Option<&Path>,
    docker_control_environment: &[(OsString, OsString)],
) -> ContainmentCapabilityReport {
    if matches!(policy.requirement, ContainmentRequirement::Uncontained) {
        return ContainmentCapabilityReport {
            requirement: policy.requirement,
            selected_backend: Some(ActiveContainmentBackend::Uncontained),
            fail_closed: false,
            backends: vec![BackendCapability::available(
                ActiveContainmentBackend::Uncontained,
                ContainmentGuarantees::none(),
                "explicit uncontained opt-out",
            )],
        };
    }

    let seatbelt = macos::capability(workdir).await;
    let linux = linux::capability(workdir).await;
    let windows = windows::capability(workdir).await;
    let docker = match (&policy.docker, workdir) {
        (Some(config), Some(workdir)) => {
            container::capability(config, workdir, docker_control_environment).await
        }
        (None, _) => BackendCapability::unavailable(
            ActiveContainmentBackend::Docker,
            ContainmentGuarantees::docker(),
            "Docker backend has no immutable image configuration",
        ),
        (Some(_), None) => BackendCapability::unavailable(
            ActiveContainmentBackend::Docker,
            ContainmentGuarantees::docker(),
            "Docker containment requires a workspace root",
        ),
    };

    let selected_backend = select_backend(
        policy.requirement,
        current_host_os(),
        seatbelt.available,
        linux.available,
        windows.available,
        docker.available,
    );

    ContainmentCapabilityReport {
        requirement: policy.requirement,
        selected_backend,
        fail_closed: true,
        backends: vec![seatbelt, linux, windows, docker],
    }
}

pub(crate) async fn prepare_command(
    command: &str,
    workdir: Option<&Path>,
    policy: &ContainmentPolicy,
    environment: &[(OsString, OsString)],
    limits: ContainmentLimits,
) -> Result<PreparedCommand> {
    if matches!(policy.requirement, ContainmentRequirement::Uncontained) {
        #[cfg(windows)]
        let cmd = {
            let mut cmd = Command::new("cmd.exe");
            cmd.args(["/d", "/s", "/c"]).arg(command);
            cmd
        };
        #[cfg(not(windows))]
        let cmd = {
            let mut cmd = Command::new("sh");
            cmd.arg("-c").arg(command);
            cmd
        };
        return Ok(PreparedCommand {
            command: cmd,
            backend: ActiveContainmentBackend::Uncontained,
            environment_overrides: Vec::new(),
            cleanup: None,
            artifacts: Vec::new(),
        });
    }

    let workdir = workdir.ok_or_else(|| {
        AikitError::Sandbox("OS containment is required but Bash has no workspace root".into())
    })?;
    // Snapshot the strictly filtered Docker control environment once. Probe, launch, and cleanup
    // must address the same policy-selected daemon without exposing loader vars or secrets.
    let docker_control_environment = container::docker_control_environment(environment);
    let report = containment_capabilities_with_docker_control_environment(
        policy,
        Some(workdir),
        &docker_control_environment,
    )
    .await;
    let selected = report.selected_backend.ok_or_else(|| {
        let details = report
            .backends
            .iter()
            .map(|b| format!("{:?}: {}", b.backend, b.detail))
            .collect::<Vec<_>>()
            .join("; ");
        AikitError::Sandbox(format!(
            "OS containment is required but no backend is available ({details})"
        ))
    })?;

    match selected {
        ActiveContainmentBackend::Seatbelt => macos::prepare(command, workdir),
        ActiveContainmentBackend::LinuxNamespace => linux::prepare(command, workdir, environment),
        ActiveContainmentBackend::Docker => {
            let config = policy.docker.as_ref().ok_or_else(|| {
                AikitError::Sandbox(
                    "Docker was selected without an immutable image configuration".into(),
                )
            })?;
            container::prepare(
                command,
                workdir,
                config,
                environment,
                docker_control_environment,
                limits,
            )
        }
        ActiveContainmentBackend::WindowsJob => {
            windows::prepare(command, workdir, environment, limits)
        }
        ActiveContainmentBackend::Firecracker => {
            unreachable!("Firecracker has no Bash guest transport and is never selected here")
        }
        ActiveContainmentBackend::Uncontained => unreachable!("required policy selected opt-out"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_required_auto() {
        assert_eq!(
            ContainmentPolicy::default().requirement,
            ContainmentRequirement::Required(BackendSelector::Auto)
        );
    }

    // Backend-selection decision table. These exercise the security boundary's fail-closed logic
    // on every host from any single platform, because `select_backend` takes the host as a value.
    const HOSTS: [HostOs; 4] = [HostOs::MacOs, HostOs::Linux, HostOs::Windows, HostOs::Other];

    fn required(selector: BackendSelector) -> ContainmentRequirement {
        ContainmentRequirement::Required(selector)
    }

    #[test]
    fn auto_prefers_native_backend_for_each_host() {
        // Every native backend available; Auto must pick the host's native one, not Docker.
        assert_eq!(
            select_backend(
                required(BackendSelector::Auto),
                HostOs::MacOs,
                true,
                true,
                true,
                true
            ),
            Some(ActiveContainmentBackend::Seatbelt)
        );
        assert_eq!(
            select_backend(
                required(BackendSelector::Auto),
                HostOs::Linux,
                true,
                true,
                true,
                true
            ),
            Some(ActiveContainmentBackend::LinuxNamespace)
        );
        assert_eq!(
            select_backend(
                required(BackendSelector::Auto),
                HostOs::Windows,
                true,
                true,
                true,
                true
            ),
            Some(ActiveContainmentBackend::WindowsJob)
        );
    }

    #[test]
    fn auto_falls_back_to_docker_only_without_a_native_backend() {
        // Native unavailable but Docker present -> Docker. Nothing available -> None.
        for host in HOSTS {
            assert_eq!(
                select_backend(
                    required(BackendSelector::Auto),
                    host,
                    false,
                    false,
                    false,
                    true
                ),
                Some(ActiveContainmentBackend::Docker),
                "auto+docker should select Docker on {host:?}"
            );
            assert_eq!(
                select_backend(
                    required(BackendSelector::Auto),
                    host,
                    false,
                    false,
                    false,
                    false
                ),
                None,
                "auto with nothing available must fail closed on {host:?}"
            );
        }
    }

    #[test]
    fn explicit_seatbelt_selector_fails_closed_when_seatbelt_is_unavailable() {
        // Everything else available must NOT satisfy an explicit Seatbelt request.
        for host in HOSTS {
            assert_eq!(
                select_backend(
                    required(BackendSelector::Seatbelt),
                    host,
                    false,
                    true,
                    true,
                    true
                ),
                None,
                "explicit Seatbelt must not substitute another backend on {host:?}"
            );
        }
    }

    #[test]
    fn explicit_selectors_are_host_agnostic_when_available() {
        // Seatbelt/Docker are explicit mechanisms, not tied to the host OS.
        for host in HOSTS {
            assert_eq!(
                select_backend(
                    required(BackendSelector::Seatbelt),
                    host,
                    true,
                    false,
                    false,
                    false
                ),
                Some(ActiveContainmentBackend::Seatbelt)
            );
            assert_eq!(
                select_backend(
                    required(BackendSelector::Docker),
                    host,
                    false,
                    false,
                    false,
                    true
                ),
                Some(ActiveContainmentBackend::Docker)
            );
        }
    }

    #[test]
    fn native_selector_never_silently_uses_docker() {
        // Native requested, native unavailable, Docker available -> must be None (not Docker).
        for host in HOSTS {
            assert_eq!(
                select_backend(
                    required(BackendSelector::Native),
                    host,
                    false,
                    false,
                    false,
                    true
                ),
                None,
                "Native must never fall back to Docker on {host:?}"
            );
        }
    }

    #[test]
    fn other_host_has_no_native_backend() {
        // An unrecognized host can only ever be contained by Docker, and only under Auto/Docker.
        assert_eq!(
            select_backend(
                required(BackendSelector::Native),
                HostOs::Other,
                true,
                true,
                true,
                true
            ),
            None
        );
        assert_eq!(
            select_backend(
                required(BackendSelector::Auto),
                HostOs::Other,
                true,
                true,
                true,
                true
            ),
            Some(ActiveContainmentBackend::Docker)
        );
    }

    #[test]
    fn docker_selector_ignores_available_native_backends() {
        // Explicit Docker requested but Docker unavailable, natives available -> fail closed.
        for host in HOSTS {
            assert_eq!(
                select_backend(
                    required(BackendSelector::Docker),
                    host,
                    true,
                    true,
                    true,
                    false
                ),
                None,
                "explicit Docker must not substitute a native backend on {host:?}"
            );
        }
    }

    #[test]
    fn no_availability_always_fails_closed() {
        // Sweep every selector on every host with no backend available: always None.
        for host in HOSTS {
            for selector in [
                BackendSelector::Auto,
                BackendSelector::Native,
                BackendSelector::Seatbelt,
                BackendSelector::Docker,
            ] {
                assert_eq!(
                    select_backend(required(selector), host, false, false, false, false),
                    None,
                    "{selector:?} on {host:?} with nothing available must fail closed"
                );
            }
        }
    }

    #[test]
    fn linux_namespace_does_not_overstate_sensitive_home_isolation() {
        let guarantees = ContainmentGuarantees::linux_namespace();
        assert!(guarantees.filesystem_write_boundary);
        assert!(!guarantees.sensitive_home_read_boundary);
    }

    #[tokio::test]
    async fn explicit_uncontained_is_reported_honestly() {
        let report = containment_capabilities(&ContainmentPolicy::uncontained(), None).await;
        assert_eq!(
            report.selected_backend,
            Some(ActiveContainmentBackend::Uncontained)
        );
        assert!(!report.fail_closed);
        assert!(!report.backends[0].guarantees.network_boundary);
    }

    #[tokio::test]
    async fn required_docker_without_configuration_fails_closed() {
        let policy = ContainmentPolicy {
            requirement: ContainmentRequirement::Required(BackendSelector::Docker),
            docker: None,
        };
        let dir = tempfile::tempdir().unwrap();
        let err = prepare_command(
            "echo must-not-run",
            Some(dir.path()),
            &policy,
            &[],
            ContainmentLimits::default(),
        )
        .await
        .err()
        .expect("required containment must fail");
        assert!(matches!(err, AikitError::Sandbox(_)));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn docker_drop_cleanup_is_immediate_repeatable_and_argv_safe() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let artifact = Arc::new(tempfile::tempdir().unwrap());
        let executable = dir.path().join("fake-docker");
        let log = dir.path().join("fake-docker.log");
        let cidfile = artifact.path().join("container.cid");
        std::fs::write(
            &executable,
            "#!/bin/sh\n{ printf 'CALL\\n'; for arg in \"$@\"; do printf '%s\\n' \"$arg\"; done; } >> \"${0}.log\"\n",
        )
        .unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();

        let container_id = "a".repeat(64);
        std::fs::write(&cidfile, &container_id).unwrap();
        let action = CleanupAction::Docker {
            executable: executable.clone(),
            cidfile: cidfile.clone(),
            ownership_label: "com.aikit.invocation=test-owned".into(),
            environment: vec![(OsString::from("DOCKER_HOST"), OsString::from("safe-daemon"))],
            artifact,
        };

        let command_args = |command: &std::process::Command| {
            command
                .get_args()
                .map(|arg| arg.to_os_string())
                .collect::<Vec<_>>()
        };
        let first = action.best_effort_command().unwrap();
        let second = action.best_effort_command().unwrap();

        assert_eq!(first.get_program(), executable.as_os_str());
        assert_eq!(
            command_args(&first),
            ["rm", "-f", container_id.as_str()]
                .into_iter()
                .map(OsString::from)
                .collect::<Vec<_>>()
        );
        assert_eq!(command_args(&first), command_args(&second));
        assert_eq!(
            first
                .get_envs()
                .filter_map(|(key, value)| value.map(|value| (key, value)))
                .collect::<Vec<_>>(),
            [(
                std::ffi::OsStr::new("DOCKER_HOST"),
                std::ffi::OsStr::new("safe-daemon")
            )]
        );

        action.spawn_best_effort();
        wait_for_file_lines(&log, 4).await;
        action.spawn_best_effort();
        wait_for_file_lines(&log, 8).await;

        let lines = std::fs::read_to_string(&log).unwrap();
        assert_eq!(
            lines.lines().collect::<Vec<_>>(),
            [
                "CALL",
                "rm",
                "-f",
                container_id.as_str(),
                "CALL",
                "rm",
                "-f",
                container_id.as_str()
            ]
        );

        std::fs::write(&cidfile, "unowned;container").unwrap();
        assert!(action.best_effort_command().is_none());
        assert_eq!(std::fs::read_to_string(&log).unwrap().lines().count(), 8);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn docker_drop_cleanup_waits_for_delayed_cidfile_ownership_proof() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let artifact = Arc::new(tempfile::tempdir().unwrap());
        let cidfile = artifact.path().join("container.cid");
        let executable = dir.path().join("fake-docker");
        let log = dir.path().join("fake-docker.log");
        std::fs::write(
            &executable,
            "#!/bin/sh\ncase \"$1\" in ps) exit 0 ;; rm) printf '%s\\n' \"$3\" >> \"${0}.log\" ;; esac\n",
        )
        .unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
        let action = CleanupAction::Docker {
            executable,
            cidfile: cidfile.clone(),
            ownership_label: "com.aikit.invocation=test-delayed".into(),
            environment: Vec::new(),
            artifact,
        };

        assert!(
            !action.force().await,
            "initial empty label result must keep Drop cleanup armed"
        );
        action.spawn_best_effort();
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        std::fs::write(&cidfile, "c".repeat(64)).unwrap();
        wait_for_file_lines(&log, 1).await;

        assert_eq!(std::fs::read_to_string(log).unwrap().trim(), "c".repeat(64));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn docker_drop_cleanup_falls_back_to_unforgeable_invocation_label() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let artifact = Arc::new(tempfile::tempdir().unwrap());
        let executable = dir.path().join("fake-docker");
        let log = dir.path().join("fake-docker.log");
        let container_id = "d".repeat(64);
        std::fs::write(
            &executable,
            format!(
                "#!/bin/sh\ncase \"$1\" in ps) printf '%s\\n' {container_id} ;; rm) printf '%s\\n' \"$3\" >> \"${{0}}.log\" ;; esac\n"
            ),
        )
        .unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
        let action = CleanupAction::Docker {
            executable,
            cidfile: artifact.path().join("container.cid"),
            ownership_label: "com.aikit.invocation=test-label-fallback".into(),
            environment: Vec::new(),
            artifact,
        };

        action.spawn_best_effort();
        wait_for_file_lines(&log, 1).await;

        assert_eq!(std::fs::read_to_string(log).unwrap().trim(), container_id);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn docker_force_cleanup_accepts_verified_absence_after_rm_nonzero() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let artifact = Arc::new(tempfile::tempdir().unwrap());
        let cidfile = artifact.path().join("container.cid");
        std::fs::write(&cidfile, "e".repeat(64)).unwrap();
        let executable = dir.path().join("fake-docker");
        std::fs::write(
            &executable,
            "#!/bin/sh\ncase \"$1\" in rm) exit 1 ;; ps) exit 0 ;; esac\n",
        )
        .unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
        let action = CleanupAction::Docker {
            executable,
            cidfile,
            ownership_label: "com.aikit.invocation=test-already-removed".into(),
            environment: Vec::new(),
            artifact,
        };

        assert!(action.force().await);
    }

    #[cfg(unix)]
    async fn wait_for_file_lines(path: &Path, expected: usize) {
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let lines = std::fs::read_to_string(path)
                    .map(|contents| contents.lines().count())
                    .unwrap_or(0);
                if lines >= expected {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("best-effort cleanup process did not start before the deadline");
    }
}
