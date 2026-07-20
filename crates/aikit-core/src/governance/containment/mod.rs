//! OS-backed containment for the built-in Bash tool.
//!
//! Containment is deliberately separate from [`BashPolicy`](super::process::BashPolicy)'s
//! portable process hardening. A required policy never falls back to a host shell: if no selected
//! backend can be proved available, command preparation fails before untrusted code starts.

mod container;
mod linux;
mod macos;
mod windows;

use crate::error::{AikitError, Result};
use serde::{Deserialize, Serialize};
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use tokio::process::Command;

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
    Docker { executable: PathBuf, name: String },
}

impl CleanupAction {
    pub(crate) async fn force(&self) {
        match self {
            CleanupAction::Docker { executable, name } => {
                let mut cmd = Command::new(executable);
                cmd.arg("rm").arg("-f").arg(name);
                cmd.stdin(std::process::Stdio::null());
                cmd.stdout(std::process::Stdio::null());
                cmd.stderr(std::process::Stdio::null());
                cmd.kill_on_drop(true);
                let _ = tokio::time::timeout(std::time::Duration::from_secs(5), cmd.status()).await;
            }
        }
    }

    /// Cancellation cannot await cleanup. Start an argv-safe cleanup process synchronously from
    /// `Drop`, then reap it on a detached thread. Spawning before `Drop` returns avoids depending
    /// on the cancelled Tokio task/runtime to schedule the cleanup; the normal timeout path above
    /// still awaits cleanup deterministically.
    pub(crate) fn spawn_best_effort(&self) {
        let mut cmd = self.best_effort_command();
        if let Ok(mut child) = cmd.spawn() {
            // Reap the cleanup process without blocking `Drop` or leaving a zombie behind.
            let _ = std::thread::Builder::new()
                .name("aikit-containment-cleanup".into())
                .spawn(move || {
                    let _ = child.wait();
                });
        }
    }

    fn best_effort_command(&self) -> std::process::Command {
        match self {
            CleanupAction::Docker { executable, name } => {
                let mut cmd = std::process::Command::new(executable);
                cmd.arg("rm").arg("-f").arg(name);
                cmd.stdin(std::process::Stdio::null());
                cmd.stdout(std::process::Stdio::null());
                cmd.stderr(std::process::Stdio::null());
                cmd
            }
        }
    }
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
        (Some(config), Some(workdir)) => container::capability(config, workdir).await,
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
    let report = containment_capabilities(policy, Some(workdir)).await;
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
        ActiveContainmentBackend::LinuxNamespace => linux::prepare(command, workdir),
        ActiveContainmentBackend::Docker => {
            let config = policy.docker.as_ref().ok_or_else(|| {
                AikitError::Sandbox(
                    "Docker was selected without an immutable image configuration".into(),
                )
            })?;
            container::prepare(command, workdir, config, environment, limits)
        }
        ActiveContainmentBackend::WindowsJob => windows::prepare(command, workdir, limits),
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
        let executable = dir.path().join("fake-docker");
        let log = dir.path().join("fake-docker.log");
        let sentinel = dir.path().join("must-not-run");
        std::fs::write(
            &executable,
            "#!/bin/sh\n{ printf 'CALL\\n'; for arg in \"$@\"; do printf '%s\\n' \"$arg\"; done; } >> \"${0}.log\"\n",
        )
        .unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();

        let name = format!("aikit-7; touch {}", sentinel.display());
        let action = CleanupAction::Docker {
            executable: executable.clone(),
            name: name.clone(),
        };

        let command_args = |command: &std::process::Command| {
            command
                .get_args()
                .map(|arg| arg.to_os_string())
                .collect::<Vec<_>>()
        };
        let first = action.best_effort_command();
        let second = action.best_effort_command();

        assert_eq!(first.get_program(), executable.as_os_str());
        assert_eq!(
            command_args(&first),
            ["rm", "-f", name.as_str()]
                .into_iter()
                .map(OsString::from)
                .collect::<Vec<_>>()
        );
        assert_eq!(command_args(&first), command_args(&second));

        action.spawn_best_effort();
        wait_for_file_lines(&log, 4).await;
        action.spawn_best_effort();
        wait_for_file_lines(&log, 8).await;

        let lines = std::fs::read_to_string(log).unwrap();
        assert_eq!(
            lines.lines().collect::<Vec<_>>(),
            [
                "CALL",
                "rm",
                "-f",
                name.as_str(),
                "CALL",
                "rm",
                "-f",
                name.as_str()
            ]
        );
        assert!(
            !sentinel.exists(),
            "container name was interpreted by a shell"
        );
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
