use super::{ActiveContainmentBackend, BackendCapability, ContainmentGuarantees, PreparedCommand};
use crate::error::{AikitError, Result};
#[cfg(target_os = "macos")]
use std::ffi::OsString;
use std::path::Path;
#[cfg(target_os = "macos")]
use std::path::PathBuf;
#[cfg(target_os = "macos")]
use tokio::process::Command;

#[cfg(target_os = "macos")]
const SEATBELT_EXECUTABLE: &str = "/usr/bin/sandbox-exec";

// `allow default` keeps ordinary developer toolchains usable, while explicit deny rules create
// the security boundary this first production slice promises: no network, no writes outside the
// workspace/private temp directory, no reads from the user's home outside the workspace, and no
// Apple Events/LaunchServices escape hatch. See docs/THREAT-MODEL.md for the intentionally honest
// limitations of this host-policy backend.
#[cfg(any(target_os = "macos", test))]
const PROFILE: &str = r#"(version 1)
(allow default)
(deny network*)
(deny appleevent-send)
(deny lsopen)
(deny file-read*
  (require-all
    (subpath (param "USERS_ROOT"))
    (require-not (subpath (param "WORKSPACE")))))
(deny file-read*
  (require-all
    (subpath (param "HOME_ROOT"))
    (require-not (subpath (param "WORKSPACE")))
    (require-not (subpath (param "TMPDIR")))))
(deny file-write*
  (require-all
    (require-not (subpath (param "WORKSPACE")))
    (require-not (subpath (param "TMPDIR")))
    (require-not (literal "/dev/null"))))"#;

pub(super) async fn capability(workdir: Option<&Path>) -> BackendCapability {
    #[cfg(not(target_os = "macos"))]
    {
        let _ = workdir;
        BackendCapability::unavailable(
            ActiveContainmentBackend::Seatbelt,
            ContainmentGuarantees::seatbelt(),
            "Seatbelt is available only on macOS",
        )
    }

    #[cfg(target_os = "macos")]
    {
        if !Path::new(SEATBELT_EXECUTABLE).is_file() {
            return BackendCapability::unavailable(
                ActiveContainmentBackend::Seatbelt,
                ContainmentGuarantees::seatbelt(),
                format!("{SEATBELT_EXECUTABLE} is missing"),
            );
        }
        let Some(workdir) = workdir else {
            return BackendCapability::unavailable(
                ActiveContainmentBackend::Seatbelt,
                ContainmentGuarantees::seatbelt(),
                "Seatbelt containment requires a workspace root",
            );
        };
        let outside = match tempfile::Builder::new()
            .prefix("aikit-seatbelt-probe-")
            .tempdir()
        {
            Ok(outside) => outside,
            Err(error) => {
                return BackendCapability::unavailable(
                    ActiveContainmentBackend::Seatbelt,
                    ContainmentGuarantees::seatbelt(),
                    format!("Seatbelt probe temp directory failed: {error}"),
                )
            }
        };
        let blocked = outside.path().join("must-not-write");
        let mut prepared = match prepare(
            "if printf blocked > \"$AIKIT_SEATBELT_PROBE\"; then exit 91; else exit 0; fi",
            workdir,
        ) {
            Ok(prepared) => prepared,
            Err(error) => {
                return BackendCapability::unavailable(
                    ActiveContainmentBackend::Seatbelt,
                    ContainmentGuarantees::seatbelt(),
                    format!("Seatbelt profile could not be prepared: {error}"),
                )
            }
        };
        prepared.command.stdin(std::process::Stdio::null());
        prepared.command.stdout(std::process::Stdio::null());
        prepared.command.stderr(std::process::Stdio::piped());
        prepared.command.kill_on_drop(true);
        prepared.command.env_clear();
        prepared
            .command
            .envs(prepared.environment_overrides.clone());
        prepared.command.env("AIKIT_SEATBELT_PROBE", &blocked);
        match tokio::time::timeout(
            std::time::Duration::from_secs(3),
            prepared.command.output(),
        )
        .await
        {
            Ok(Ok(output)) if output.status.success() && !blocked.exists() => {
                BackendCapability::available(
                    ActiveContainmentBackend::Seatbelt,
                    ContainmentGuarantees::seatbelt(),
                    "Seatbelt profile enforcement probe succeeded (sandbox-exec is deprecated by Apple)",
                )
            }
            Ok(Ok(output)) => BackendCapability::unavailable(
                ActiveContainmentBackend::Seatbelt,
                ContainmentGuarantees::seatbelt(),
                format!(
                    "Seatbelt enforcement probe failed (outside_write={}): {}",
                    blocked.exists(),
                    String::from_utf8_lossy(&output.stderr).trim()
                ),
            ),
            Ok(Err(error)) => BackendCapability::unavailable(
                ActiveContainmentBackend::Seatbelt,
                ContainmentGuarantees::seatbelt(),
                format!("Seatbelt probe could not start: {error}"),
            ),
            Err(_) => BackendCapability::unavailable(
                ActiveContainmentBackend::Seatbelt,
                ContainmentGuarantees::seatbelt(),
                "Seatbelt probe timed out",
            ),
        }
    }
}

pub(super) fn prepare(command: &str, workdir: &Path) -> Result<PreparedCommand> {
    #[cfg(not(target_os = "macos"))]
    {
        let _ = (command, workdir);
        Err(AikitError::Sandbox(
            "Seatbelt containment is unavailable on this platform".into(),
        ))
    }

    #[cfg(target_os = "macos")]
    {
        let workspace = std::fs::canonicalize(workdir).map_err(|error| {
            AikitError::Sandbox(format!(
                "cannot canonicalize Seatbelt workspace {}: {error}",
                workdir.display()
            ))
        })?;
        let temp = tempfile::Builder::new()
            .prefix("aikit-seatbelt-")
            .tempdir()
            .map_err(|error| {
                AikitError::Sandbox(format!(
                    "cannot create private Seatbelt temp directory: {error}"
                ))
            })?;
        let temp_path = std::fs::canonicalize(temp.path()).map_err(|error| {
            AikitError::Sandbox(format!(
                "cannot canonicalize private Seatbelt temp directory: {error}"
            ))
        })?;
        let private_home = temp_path.join("home");
        std::fs::create_dir(&private_home).map_err(|error| {
            AikitError::Sandbox(format!("cannot create private Seatbelt HOME: {error}"))
        })?;

        let home_root = safe_home_root();
        let mut cmd = Command::new(SEATBELT_EXECUTABLE);
        cmd.arg("-p").arg(PROFILE);
        push_definition(&mut cmd, "USERS_ROOT", Path::new("/Users"));
        push_definition(&mut cmd, "HOME_ROOT", &home_root);
        push_definition(&mut cmd, "WORKSPACE", &workspace);
        push_definition(&mut cmd, "TMPDIR", &temp_path);
        cmd.arg("--").arg("/bin/sh").arg("-c").arg(command);

        Ok(PreparedCommand {
            command: cmd,
            backend: ActiveContainmentBackend::Seatbelt,
            environment_overrides: vec![
                (OsString::from("HOME"), private_home.into_os_string()),
                (OsString::from("TMPDIR"), temp_path.into_os_string()),
            ],
            cleanup: None,
            artifacts: vec![temp],
        })
    }
}

#[cfg(target_os = "macos")]
fn safe_home_root() -> PathBuf {
    let candidate = std::env::var_os("HOME")
        .map(PathBuf::from)
        .filter(|path| path.is_absolute())
        .and_then(|path| std::fs::canonicalize(path).ok())
        .filter(|path| path != Path::new("/"));
    candidate.unwrap_or_else(|| PathBuf::from("/var/empty"))
}

#[cfg(target_os = "macos")]
fn push_definition(cmd: &mut Command, name: &str, value: &Path) {
    let mut definition = OsString::from(name);
    definition.push("=");
    definition.push(value.as_os_str());
    cmd.arg("-D").arg(definition);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profile_blocks_network_and_non_workspace_writes() {
        assert!(PROFILE.contains("(deny network*)"));
        assert!(PROFILE.contains("(deny file-write*"));
        assert!(PROFILE.contains("(deny appleevent-send)"));
        assert!(PROFILE.contains("(deny lsopen)"));
    }
}
