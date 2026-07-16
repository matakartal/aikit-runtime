use super::{
    ActiveContainmentBackend, BackendCapability, CleanupAction, ContainmentGuarantees,
    ContainmentLimits, DockerConfig, PreparedCommand,
};
use crate::error::{AikitError, Result};
use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::process::Command;

static NEXT_CONTAINER_ID: AtomicU64 = AtomicU64::new(1);

pub(super) async fn capability(config: &DockerConfig, workdir: &Path) -> BackendCapability {
    let guarantees = ContainmentGuarantees::docker();
    if let Err(detail) = validate_config(config) {
        return BackendCapability::unavailable(
            ActiveContainmentBackend::Docker,
            guarantees,
            detail,
        );
    }
    let executable = match resolve_executable(&config.executable, workdir) {
        Ok(path) => path,
        Err(detail) => {
            return BackendCapability::unavailable(
                ActiveContainmentBackend::Docker,
                guarantees,
                detail,
            )
        }
    };
    let security = match checked_output(
        &executable,
        ["info", "--format", "{{json .SecurityOptions}}"],
    )
    .await
    {
        Ok(output) => output,
        Err(detail) => {
            return BackendCapability::unavailable(
                ActiveContainmentBackend::Docker,
                guarantees,
                detail,
            )
        }
    };
    if !security.contains("seccomp") {
        return BackendCapability::unavailable(
            ActiveContainmentBackend::Docker,
            guarantees,
            "Docker daemon does not report seccomp support",
        );
    }
    if let Err(detail) = checked_output(
        &executable,
        ["image", "inspect", "--format", "{{.Id}}", &config.image],
    )
    .await
    {
        return BackendCapability::unavailable(
            ActiveContainmentBackend::Docker,
            guarantees,
            format!("immutable Docker image is not ready locally: {detail}"),
        );
    }
    BackendCapability::available(
        ActiveContainmentBackend::Docker,
        guarantees,
        format!(
            "Docker backend ready at {} with builtin seccomp",
            executable.display()
        ),
    )
}

pub(super) fn prepare(
    command: &str,
    workdir: &Path,
    config: &DockerConfig,
    environment: &[(OsString, OsString)],
    limits: ContainmentLimits,
) -> Result<PreparedCommand> {
    validate_config(config).map_err(AikitError::Sandbox)?;
    let workspace = std::fs::canonicalize(workdir).map_err(|error| {
        AikitError::Sandbox(format!(
            "cannot canonicalize Docker workspace {}: {error}",
            workdir.display()
        ))
    })?;
    let workspace_text = workspace
        .to_str()
        .ok_or_else(|| AikitError::Sandbox("Docker workspace path must be valid UTF-8".into()))?;
    if workspace_text.contains(',') {
        return Err(AikitError::Sandbox(
            "Docker workspace path cannot contain ',' because Docker --mount uses CSV syntax"
                .into(),
        ));
    }
    let executable =
        resolve_executable(&config.executable, &workspace).map_err(AikitError::Sandbox)?;
    let name = format!(
        "aikit-{}-{}",
        std::process::id(),
        NEXT_CONTAINER_ID.fetch_add(1, Ordering::Relaxed)
    );

    let mut cmd = Command::new(&executable);
    cmd.arg("run")
        .arg("--rm")
        .arg("--init")
        .arg("--pull=never")
        .arg("--name")
        .arg(&name)
        .arg("--network=none")
        .arg("--ipc=private")
        .arg("--read-only")
        .arg("--cap-drop=ALL")
        .arg("--security-opt=no-new-privileges=true")
        .arg("--security-opt=seccomp=builtin")
        .arg(format!("--pids-limit={}", effective_pids(config, limits)))
        .arg(format!("--memory={}", config.memory_bytes))
        .arg(format!("--memory-swap={}", config.memory_bytes))
        .arg(format!("--cpus={}", config.cpus))
        .arg("--tmpfs")
        .arg(format!(
            "/tmp:rw,nosuid,nodev,noexec,size={}",
            config.tmpfs_bytes
        ))
        .arg("--mount")
        .arg(format!(
            "type=bind,source={workspace_text},target=/workspace"
        ))
        .arg("--workdir")
        .arg("/workspace");

    cmd.arg("--user").arg(container_user());

    if let Some(cpu) = limits.max_cpu_seconds {
        cmd.arg("--ulimit").arg(format!("cpu={cpu}:{cpu}"));
    }
    if let Some(size) = limits.max_file_size_bytes {
        cmd.arg("--ulimit").arg(format!("fsize={size}:{size}"));
    }
    if let Some(files) = limits.max_open_files {
        cmd.arg("--ulimit").arg(format!("nofile={files}:{files}"));
    }

    append_container_environment(&mut cmd, environment);
    cmd.arg("--env=HOME=/tmp")
        .arg("--env=TMPDIR=/tmp")
        .arg("--env=PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin")
        .arg("--entrypoint=/bin/sh")
        .arg(&config.image)
        .arg("-c")
        .arg(command);

    Ok(PreparedCommand {
        command: cmd,
        backend: ActiveContainmentBackend::Docker,
        environment_overrides: Vec::new(),
        cleanup: Some(CleanupAction::Docker { executable, name }),
        artifacts: Vec::new(),
    })
}

fn effective_pids(config: &DockerConfig, limits: ContainmentLimits) -> u64 {
    limits
        .max_processes
        .map(|limit| limit.min(config.pids_limit as u64))
        .unwrap_or(config.pids_limit as u64)
}

fn append_container_environment(cmd: &mut Command, environment: &[(OsString, OsString)]) {
    for (key, _) in environment {
        if key == OsStr::new("HOME") || key == OsStr::new("PATH") || key == OsStr::new("TMPDIR") {
            continue;
        }
        // Docker copies the value from its own scrubbed environment. Keeping the value out of
        // argv avoids exposing an explicitly supplied secret through process listings.
        cmd.arg("--env").arg(key);
    }
}

fn validate_config(config: &DockerConfig) -> std::result::Result<(), String> {
    let valid_digest =
        |digest: &str| digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit());
    let immutable = config
        .image
        .strip_prefix("sha256:")
        .is_some_and(valid_digest)
        || config
            .image
            .rsplit_once("@sha256:")
            .is_some_and(|(name, digest)| {
                !name.is_empty()
                    && !name.starts_with('-')
                    && name.bytes().all(|byte| {
                        byte.is_ascii_alphanumeric()
                            || matches!(byte, b'.' | b'_' | b'/' | b'-' | b':')
                    })
                    && valid_digest(digest)
            });
    if !immutable {
        return Err(
            "Docker image must be pinned as name@sha256:<64 hex> or a local sha256:<id>".into(),
        );
    }
    if config.pids_limit == 0
        || config.memory_bytes == 0
        || config.cpus == 0
        || config.tmpfs_bytes == 0
    {
        return Err("Docker resource limits must all be greater than zero".into());
    }
    Ok(())
}

fn resolve_executable(configured: &Path, workdir: &Path) -> std::result::Result<PathBuf, String> {
    let candidate = if configured.components().count() > 1 || configured.is_absolute() {
        configured.to_path_buf()
    } else {
        std::env::var_os("PATH")
            .into_iter()
            .flat_map(|path| std::env::split_paths(&path).collect::<Vec<_>>())
            .flat_map(|dir| executable_candidates(&dir, configured))
            .find(|path| path.is_file())
            .ok_or_else(|| format!("Docker executable '{}' was not found", configured.display()))?
    };
    let canonical = std::fs::canonicalize(&candidate).map_err(|error| {
        format!(
            "cannot canonicalize Docker executable {}: {error}",
            candidate.display()
        )
    })?;
    if !canonical.is_file() {
        return Err(format!(
            "Docker executable {} is not a regular file",
            canonical.display()
        ));
    }
    let canonical_workdir = std::fs::canonicalize(workdir).map_err(|error| {
        format!(
            "cannot canonicalize Docker workspace {}: {error}",
            workdir.display()
        )
    })?;
    if canonical.starts_with(&canonical_workdir) {
        return Err("refusing to use a Docker executable from the writable workspace".into());
    }
    Ok(canonical)
}

fn executable_candidates(dir: &Path, configured: &Path) -> Vec<PathBuf> {
    let base = dir.join(configured);
    #[cfg(windows)]
    {
        if base.extension().is_none() {
            return vec![base.clone(), base.with_extension("exe")];
        }
    }
    vec![base]
}

#[cfg(unix)]
fn container_user() -> String {
    // SAFETY: `getuid`/`getgid` have no preconditions and return process credentials.
    let (uid, gid) = unsafe { (libc::getuid(), libc::getgid()) };
    if uid == 0 {
        "65534:65534".into()
    } else {
        format!("{uid}:{gid}")
    }
}

#[cfg(not(unix))]
fn container_user() -> String {
    "65534:65534".into()
}

async fn checked_output<I, S>(executable: &Path, args: I) -> std::result::Result<String, String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let mut cmd = Command::new(executable);
    cmd.args(args);
    cmd.stdin(std::process::Stdio::null());
    cmd.kill_on_drop(true);
    let output = tokio::time::timeout(std::time::Duration::from_secs(5), cmd.output())
        .await
        .map_err(|_| format!("{} probe timed out", executable.display()))?
        .map_err(|error| format!("{} probe failed: {error}", executable.display()))?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).trim().to_string());
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pinned_config(executable: &Path) -> DockerConfig {
        DockerConfig::new(format!("example/aikit@sha256:{}", "a".repeat(64)))
            .with_executable(executable)
    }

    #[test]
    fn rejects_mutable_image_tags() {
        let config = DockerConfig::new("alpine:latest");
        assert!(validate_config(&config).is_err());
        assert!(validate_config(&DockerConfig::new("sha256:too-short")).is_err());
        assert!(validate_config(&DockerConfig::new(format!(
            "--network=host@sha256:{}",
            "a".repeat(64)
        )))
        .is_err());
    }

    #[cfg(unix)]
    #[test]
    fn command_builder_is_argv_safe_and_hardened() {
        let workspace = tempfile::tempdir().unwrap();
        let executable = Path::new("/bin/echo");
        let prepared = prepare(
            "printf '%s' \"$UNTRUSTED\"; touch -- /tmp/nope",
            workspace.path(),
            &pinned_config(executable),
            &[(OsString::from("UNTRUSTED"), OsString::from("a;$(id)"))],
            ContainmentLimits::default(),
        )
        .unwrap();
        let args: Vec<String> = prepared
            .command
            .as_std()
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect();
        for required in [
            "--network=none",
            "--read-only",
            "--cap-drop=ALL",
            "--security-opt=no-new-privileges=true",
            "--security-opt=seccomp=builtin",
            "--pull=never",
            "--memory-swap=536870912",
            "--entrypoint=/bin/sh",
        ] {
            assert!(args.iter().any(|arg| arg == required), "missing {required}");
        }
        assert!(args.iter().any(|arg| arg == "UNTRUSTED"));
        assert!(!args.iter().any(|arg| arg.contains("a;$(id)")));
        assert_eq!(
            args.last().unwrap(),
            "printf '%s' \"$UNTRUSTED\"; touch -- /tmp/nope"
        );
    }

    #[cfg(unix)]
    #[test]
    fn every_container_invocation_gets_a_unique_safe_name() {
        let workspace = tempfile::tempdir().unwrap();
        let prepared_one = prepare(
            "true",
            workspace.path(),
            &pinned_config(Path::new("/bin/echo")),
            &[],
            ContainmentLimits::default(),
        )
        .unwrap();
        let prepared_two = prepare(
            "true",
            workspace.path(),
            &pinned_config(Path::new("/bin/echo")),
            &[],
            ContainmentLimits::default(),
        )
        .unwrap();

        let container_name = |prepared: &PreparedCommand| {
            let args = prepared.command.as_std().get_args().collect::<Vec<_>>();
            let index = args
                .iter()
                .position(|arg| *arg == OsStr::new("--name"))
                .expect("Docker command has --name");
            args[index + 1].to_string_lossy().into_owned()
        };
        let first = container_name(&prepared_one);
        let second = container_name(&prepared_two);

        assert_ne!(first, second);
        for name in [first, second] {
            assert!(name.starts_with(&format!("aikit-{}-", std::process::id())));
            assert!(name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-'));
        }
    }
}
