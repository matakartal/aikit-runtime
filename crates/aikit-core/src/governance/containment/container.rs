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

pub(super) async fn capability(
    config: &DockerConfig,
    workdir: &Path,
    control_environment: &[(OsString, OsString)],
) -> BackendCapability {
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
        control_environment,
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
        control_environment,
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
    control_environment: Vec<(OsString, OsString)>,
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
    let artifacts = std::sync::Arc::new(
        tempfile::Builder::new()
            .prefix("aikit-docker-")
            .tempdir()
            .map_err(|error| AikitError::Sandbox(error.to_string()))?,
    );
    let cidfile = artifacts.path().join("container.cid");
    let environment_file = artifacts.path().join("workload.env");
    let ownership_token = artifacts
        .path()
        .file_name()
        .and_then(OsStr::to_str)
        .filter(|name| {
            !name.is_empty()
                && name
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        })
        .ok_or_else(|| AikitError::Sandbox("invalid Docker ownership token".into()))?;
    let ownership_label = format!("com.aikit.invocation={ownership_token}");
    write_guest_environment_file(&environment_file, environment)?;

    let mut cmd = Command::new(&executable);
    cmd.arg("run")
        .arg("--rm")
        .arg("--init")
        .arg("--pull=never")
        .arg("--cidfile")
        .arg(&cidfile)
        .arg("--label")
        .arg(&ownership_label)
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

    cmd.arg("--env-file")
        .arg(&environment_file)
        .arg("--env=HOME=/tmp")
        .arg("--env=TMPDIR=/tmp")
        .arg("--env=PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin")
        .arg("--entrypoint=/bin/sh")
        .arg(&config.image)
        .arg("-c")
        .arg(command);

    Ok(PreparedCommand {
        command: cmd,
        backend: ActiveContainmentBackend::Docker,
        environment_overrides: control_environment.clone(),
        cleanup: Some(CleanupAction::Docker {
            executable,
            cidfile,
            ownership_label,
            environment: control_environment,
            artifact: artifacts,
        }),
        artifacts: Vec::new(),
    })
}

fn effective_pids(config: &DockerConfig, limits: ContainmentLimits) -> u64 {
    limits
        .max_processes
        .map(|limit| limit.min(config.pids_limit as u64))
        .unwrap_or(config.pids_limit as u64)
}

fn write_guest_environment_file(path: &Path, environment: &[(OsString, OsString)]) -> Result<()> {
    use std::io::Write;

    let mut entries = Vec::<(String, String)>::new();
    for (key, value) in environment {
        if key == OsStr::new("HOME") || key == OsStr::new("PATH") || key == OsStr::new("TMPDIR") {
            continue;
        }
        let key = key.to_str().ok_or_else(|| {
            AikitError::Sandbox("Docker environment names must be valid UTF-8".into())
        })?;
        let value = value.to_str().ok_or_else(|| {
            AikitError::Sandbox(format!(
                "Docker environment value for {key} must be valid UTF-8"
            ))
        })?;
        let valid_key = key.bytes().enumerate().all(|(index, byte)| {
            byte == b'_' || byte.is_ascii_alphabetic() || (index > 0 && byte.is_ascii_digit())
        });
        let edge_whitespace = value
            .as_bytes()
            .first()
            .into_iter()
            .chain(value.as_bytes().last())
            .any(|byte| byte.is_ascii_whitespace());
        if key.is_empty()
            || !valid_key
            || value.contains('\r')
            || value.contains('\n')
            || edge_whitespace
        {
            return Err(AikitError::Sandbox(format!(
                "Docker environment entry {key:?} cannot be represented safely"
            )));
        }
        entries.retain(|(existing, _)| existing != key);
        entries.push((key.to_owned(), value.to_owned()));
    }

    let mut file = private_file(path).map_err(|error| AikitError::Sandbox(error.to_string()))?;
    for (key, value) in entries {
        writeln!(file, "{key}={value}").map_err(|error| AikitError::Sandbox(error.to_string()))?;
    }
    file.sync_all()
        .map_err(|error| AikitError::Sandbox(error.to_string()))
}

fn private_file(path: &Path) -> std::io::Result<std::fs::File> {
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path)
}

pub(super) fn docker_control_environment(
    environment: &[(OsString, OsString)],
) -> Vec<(OsString, OsString)> {
    environment
        .iter()
        .filter(|(key, _)| {
            let Some(key) = key.to_str() else {
                return false;
            };
            let upper = key.to_ascii_uppercase();
            matches!(
                upper.as_str(),
                "HOME"
                    | "PATH"
                    | "USERPROFILE"
                    | "SYSTEMROOT"
                    | "HTTP_PROXY"
                    | "HTTPS_PROXY"
                    | "ALL_PROXY"
                    | "NO_PROXY"
                    | "DOCKER_HOST"
                    | "DOCKER_CONTEXT"
                    | "DOCKER_TLS_VERIFY"
                    | "DOCKER_CERT_PATH"
                    | "DOCKER_API_VERSION"
            )
        })
        .cloned()
        .collect()
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

async fn checked_output<I, S>(
    executable: &Path,
    args: I,
    environment: &[(OsString, OsString)],
) -> std::result::Result<String, String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let mut cmd = Command::new(executable);
    cmd.args(args);
    cmd.stdin(std::process::Stdio::null());
    cmd.kill_on_drop(true);
    cmd.env_clear();
    cmd.envs(environment.iter().cloned());
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

    #[test]
    fn docker_control_environment_drops_unrelated_secrets() {
        let environment = vec![
            (OsString::from("DOCKER_HOST"), OsString::from("tcp://safe")),
            (
                OsString::from("https_proxy"),
                OsString::from("http://proxy"),
            ),
            (
                OsString::from("AIKIT_SECRET"),
                OsString::from("must-not-leak"),
            ),
            (
                OsString::from("LD_PRELOAD"),
                OsString::from("must-not-load"),
            ),
            (
                OsString::from("DOCKER_CLI_PLUGIN_EXTRA_DIRS"),
                OsString::from("/untrusted/plugins"),
            ),
            (
                OsString::from("DOCKER_CONFIG"),
                OsString::from("/untrusted/config"),
            ),
        ];
        assert_eq!(
            docker_control_environment(&environment),
            vec![
                (OsString::from("DOCKER_HOST"), OsString::from("tcp://safe")),
                (
                    OsString::from("https_proxy"),
                    OsString::from("http://proxy")
                ),
            ]
        );
    }

    #[test]
    fn guest_environment_file_uses_exact_docker_run_env_syntax() {
        let holder = tempfile::tempdir().unwrap();
        let path = holder.path().join("workload.env");
        write_guest_environment_file(
            &path,
            &[(
                OsString::from("AIKIT_TOKEN"),
                OsString::from("a=b#c interior space"),
            )],
        )
        .unwrap();
        assert_eq!(
            std::fs::read_to_string(path).unwrap(),
            "AIKIT_TOKEN=a=b#c interior space\n"
        );

        assert!(write_guest_environment_file(
            &holder.path().join("unsafe-value.env"),
            &[(OsString::from("AIKIT_TOKEN"), OsString::from(" leading"))],
        )
        .is_err());
        assert!(write_guest_environment_file(
            &holder.path().join("unsafe-key.env"),
            &[(OsString::from("BAD-NAME"), OsString::from("value"))],
        )
        .is_err());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn capability_probe_uses_only_the_scrubbed_docker_control_environment() {
        use std::os::unix::fs::PermissionsExt;

        let holder = tempfile::tempdir().unwrap();
        let workspace = holder.path().join("workspace");
        std::fs::create_dir(&workspace).unwrap();
        let executable = holder.path().join("fake-docker");
        std::fs::write(
            &executable,
            "#!/bin/sh\n[ -z \"${AIKIT_SECRET:-}\" ] || exit 90\n[ \"${DOCKER_HOST:-}\" = tcp://safe ] || exit 91\ncase \"$1\" in info) printf '%s\\n' seccomp ;; image) printf '%s\\n' sha256:ready ;; *) exit 92 ;; esac\n",
        )
        .unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
        let environment = vec![
            (OsString::from("DOCKER_HOST"), OsString::from("tcp://safe")),
            (
                OsString::from("AIKIT_SECRET"),
                OsString::from("must-not-reach-probe"),
            ),
        ];

        let control_environment = docker_control_environment(&environment);
        let result = capability(
            &pinned_config(&executable),
            &workspace,
            &control_environment,
        )
        .await;
        assert!(result.available, "{}", result.detail);
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
            Vec::new(),
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
        assert!(args.iter().any(|arg| arg == "--env-file"));
        assert!(!args.iter().any(|arg| arg.contains("a;$(id)")));
        let environment_file = args
            .iter()
            .position(|arg| arg == "--env-file")
            .map(|index| &args[index + 1])
            .expect("Docker command has --env-file");
        assert_eq!(
            std::fs::read_to_string(environment_file).unwrap(),
            "UNTRUSTED=a;$(id)\n"
        );
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
            Vec::new(),
            ContainmentLimits::default(),
        )
        .unwrap();
        let prepared_two = prepare(
            "true",
            workspace.path(),
            &pinned_config(Path::new("/bin/echo")),
            &[],
            Vec::new(),
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
