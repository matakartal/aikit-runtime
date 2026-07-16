//! OS-backed containment for the built-in Bash tool.
//!
//! Containment is deliberately separate from [`BashPolicy`](super::process::BashPolicy)'s
//! portable process hardening. A required policy never falls back to a host shell: if no selected
//! backend can be proved available, command preparation fails before untrusted code starts.

mod container;
mod macos;

use crate::error::{AikitError, Result};
use serde::{Deserialize, Serialize};
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use tokio::process::Command;

/// Which backend a required containment policy should use.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BackendSelector {
    /// Prefer the native backend on macOS, otherwise use a configured Docker backend.
    Auto,
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

    let selected_backend = match policy.requirement {
        ContainmentRequirement::Required(BackendSelector::Seatbelt) if seatbelt.available => {
            Some(ActiveContainmentBackend::Seatbelt)
        }
        ContainmentRequirement::Required(BackendSelector::Docker) if docker.available => {
            Some(ActiveContainmentBackend::Docker)
        }
        ContainmentRequirement::Required(BackendSelector::Auto) => {
            if cfg!(target_os = "macos") && seatbelt.available {
                Some(ActiveContainmentBackend::Seatbelt)
            } else if docker.available {
                Some(ActiveContainmentBackend::Docker)
            } else {
                None
            }
        }
        _ => None,
    };

    ContainmentCapabilityReport {
        requirement: policy.requirement,
        selected_backend,
        fail_closed: true,
        backends: vec![seatbelt, docker],
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
        let mut cmd = Command::new("sh");
        cmd.arg("-c").arg(command);
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
        ActiveContainmentBackend::Docker => {
            let config = policy.docker.as_ref().ok_or_else(|| {
                AikitError::Sandbox(
                    "Docker was selected without an immutable image configuration".into(),
                )
            })?;
            container::prepare(command, workdir, config, environment, limits)
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
