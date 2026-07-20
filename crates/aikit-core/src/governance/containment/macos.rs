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
        cmd.args(seatbelt_args(command, &workspace, &home_root, &temp_path));

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
    resolve_home_root(std::env::var_os("HOME").map(PathBuf::from))
}

/// Resolve the HOME root the profile hides: an absolute, canonicalizable, non-`/` candidate, or
/// the conservative `/var/empty` fallback. Pure over its input so the fallback matrix is testable
/// on any platform.
#[cfg(any(target_os = "macos", test))]
fn resolve_home_root(candidate: Option<std::path::PathBuf>) -> std::path::PathBuf {
    candidate
        .filter(|path| path.is_absolute())
        .and_then(|path| std::fs::canonicalize(path).ok())
        .filter(|path| path != Path::new("/"))
        .unwrap_or_else(|| std::path::PathBuf::from("/var/empty"))
}

/// The exact `sandbox-exec` argv (everything after the program itself): the inline profile, the
/// four `-D` parameter definitions the profile references, then the shell invocation with the
/// untrusted command as the single final argument. Pure so profile wiring and argv-injection
/// safety are unit-testable on any platform.
#[cfg(any(target_os = "macos", test))]
fn seatbelt_args(
    command: &str,
    workspace: &Path,
    home_root: &Path,
    temp_path: &Path,
) -> Vec<std::ffi::OsString> {
    use std::ffi::OsString;

    let definition = |name: &str, value: &Path| {
        let mut definition = OsString::from(name);
        definition.push("=");
        definition.push(value.as_os_str());
        definition
    };
    let mut args: Vec<OsString> = vec!["-p".into(), PROFILE.into()];
    for (name, value) in [
        ("USERS_ROOT", Path::new("/Users")),
        ("HOME_ROOT", home_root),
        ("WORKSPACE", workspace),
        ("TMPDIR", temp_path),
    ] {
        args.push("-D".into());
        args.push(definition(name, value));
    }
    args.push("--".into());
    args.push("/bin/sh".into());
    args.push("-c".into());
    args.push(command.into());
    args
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

    #[test]
    fn profile_denies_home_and_users_reads_outside_workspace() {
        // Both read-deny rules must reference their roots AND carve the workspace back out —
        // a profile that hides the workspace itself would break the tool it contains.
        for root in ["USERS_ROOT", "HOME_ROOT"] {
            let rule_start = PROFILE
                .find(&format!("(subpath (param \"{root}\"))"))
                .unwrap_or_else(|| panic!("profile must reference {root}"));
            let rule_region = &PROFILE[rule_start.saturating_sub(64)..];
            assert!(
                rule_region.contains("(require-not (subpath (param \"WORKSPACE\")))"),
                "{root} deny rule must exempt the workspace"
            );
        }
        assert!(PROFILE.contains("(deny file-read*"));
    }

    #[test]
    fn seatbelt_argv_defines_all_four_parameters_and_shell() {
        let args = seatbelt_args(
            "echo merhaba",
            Path::new("/work/space"),
            Path::new("/Users/someone"),
            Path::new("/private/tmp/aikit-x"),
        );
        let as_str: Vec<&str> = args.iter().filter_map(|a| a.to_str()).collect();

        assert_eq!(as_str[0], "-p");
        assert_eq!(as_str[1], PROFILE);
        // Every profile parameter is defined exactly once, as a -D NAME=value pair.
        for expected in [
            "USERS_ROOT=/Users",
            "HOME_ROOT=/Users/someone",
            "WORKSPACE=/work/space",
            "TMPDIR=/private/tmp/aikit-x",
        ] {
            let position = as_str
                .iter()
                .position(|a| *a == expected)
                .unwrap_or_else(|| panic!("missing definition {expected}"));
            assert_eq!(
                as_str[position - 1],
                "-D",
                "{expected} must follow a -D flag"
            );
        }
        let tail: Vec<&str> = as_str[as_str.len() - 4..].to_vec();
        assert_eq!(tail, ["--", "/bin/sh", "-c", "echo merhaba"]);
    }

    #[test]
    fn seatbelt_argv_is_argv_safe() {
        let hostile = "true; touch /tmp/pwned; $(id) `id` && echo owned";
        let args = seatbelt_args(
            hostile,
            Path::new("/w"),
            Path::new("/Users/x"),
            Path::new("/tmp/t"),
        );
        assert_eq!(args.last(), Some(&std::ffi::OsString::from(hostile)));
        let occurrences = args
            .iter()
            .filter(|a| a.to_str().is_some_and(|s| s.contains("pwned")))
            .count();
        assert_eq!(
            occurrences, 1,
            "hostile command leaked into extra argv entries"
        );
    }

    #[test]
    fn resolve_home_root_falls_back_to_var_empty() {
        let fallback = std::path::PathBuf::from("/var/empty");
        // No candidate, a relative candidate, a non-existent candidate, and `/` itself must all
        // fall back rather than exposing (or hiding) the wrong tree.
        assert_eq!(resolve_home_root(None), fallback);
        assert_eq!(
            resolve_home_root(Some(std::path::PathBuf::from("relative/home"))),
            fallback
        );
        assert_eq!(
            resolve_home_root(Some(std::path::PathBuf::from(
                "/nonexistent-aikit-test-home-4711"
            ))),
            fallback
        );
        assert_eq!(
            resolve_home_root(Some(std::path::PathBuf::from("/"))),
            fallback
        );
        // A real directory resolves to its canonical path.
        let dir = tempfile::tempdir().unwrap();
        let canonical = std::fs::canonicalize(dir.path()).unwrap();
        assert_eq!(resolve_home_root(Some(dir.path().to_path_buf())), canonical);
    }
}
