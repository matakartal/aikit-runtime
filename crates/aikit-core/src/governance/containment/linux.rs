use super::{ActiveContainmentBackend, BackendCapability, ContainmentGuarantees, PreparedCommand};
use crate::error::{AikitError, Result};
use std::path::Path;

#[cfg(target_os = "linux")]
const BWRAP: &str = "/usr/bin/bwrap";

pub(super) async fn capability(workdir: Option<&Path>) -> BackendCapability {
    #[cfg(not(target_os = "linux"))]
    {
        let _ = workdir;
        BackendCapability::unavailable(
            ActiveContainmentBackend::LinuxNamespace,
            ContainmentGuarantees::linux_namespace(),
            "Linux namespace containment is available only on Linux",
        )
    }
    #[cfg(target_os = "linux")]
    {
        let Some(workdir) = workdir else {
            return BackendCapability::unavailable(
                ActiveContainmentBackend::LinuxNamespace,
                ContainmentGuarantees::linux_namespace(),
                "Linux containment requires a workspace root",
            );
        };
        if !Path::new(BWRAP).is_file() {
            return BackendCapability::unavailable(
                ActiveContainmentBackend::LinuxNamespace,
                ContainmentGuarantees::linux_namespace(),
                format!("{BWRAP} is missing"),
            );
        }
        let mut prepared = match prepare("true", workdir) {
            Ok(prepared) => prepared,
            Err(error) => {
                return BackendCapability::unavailable(
                    ActiveContainmentBackend::LinuxNamespace,
                    ContainmentGuarantees::linux_namespace(),
                    error.to_string(),
                )
            }
        };
        prepared.command.stdin(std::process::Stdio::null());
        prepared.command.stdout(std::process::Stdio::null());
        prepared.command.stderr(std::process::Stdio::piped());
        prepared.command.kill_on_drop(true);
        match tokio::time::timeout(std::time::Duration::from_secs(3), prepared.command.output())
            .await
        {
            Ok(Ok(output)) if output.status.success() => BackendCapability::available(
                ActiveContainmentBackend::LinuxNamespace,
                ContainmentGuarantees::linux_namespace(),
                "user/mount/pid/network namespaces and seccomp probe succeeded; the host home is read-only but not hidden",
            ),
            Ok(Ok(output)) => BackendCapability::unavailable(
                ActiveContainmentBackend::LinuxNamespace,
                ContainmentGuarantees::linux_namespace(),
                format!(
                    "namespace/seccomp probe failed: {}",
                    String::from_utf8_lossy(&output.stderr).trim()
                ),
            ),
            Ok(Err(error)) => BackendCapability::unavailable(
                ActiveContainmentBackend::LinuxNamespace,
                ContainmentGuarantees::linux_namespace(),
                format!("namespace/seccomp probe could not start: {error}"),
            ),
            Err(_) => BackendCapability::unavailable(
                ActiveContainmentBackend::LinuxNamespace,
                ContainmentGuarantees::linux_namespace(),
                "namespace/seccomp probe timed out",
            ),
        }
    }
}

pub(super) fn prepare(command: &str, workdir: &Path) -> Result<PreparedCommand> {
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (command, workdir);
        Err(AikitError::Sandbox(
            "Linux namespace containment is unavailable on this platform".into(),
        ))
    }
    #[cfg(target_os = "linux")]
    {
        use std::ffi::OsString;
        use std::io::Write;
        use std::os::fd::AsRawFd;
        use std::os::unix::process::CommandExt;
        use tokio::process::Command;

        let workspace = std::fs::canonicalize(workdir).map_err(|error| {
            AikitError::Sandbox(format!("cannot canonicalize Linux workspace: {error}"))
        })?;
        let artifacts = tempfile::Builder::new()
            .prefix("aikit-linux-sandbox-")
            .tempdir()
            .map_err(|error| AikitError::Sandbox(error.to_string()))?;
        let filter_path = artifacts.path().join("seccomp.bpf");
        let mut filter = std::fs::File::create(&filter_path)
            .map_err(|error| AikitError::Sandbox(error.to_string()))?;
        filter
            .write_all(&seccomp_filter()?)
            .map_err(|error| AikitError::Sandbox(error.to_string()))?;
        filter
            .sync_all()
            .map_err(|error| AikitError::Sandbox(error.to_string()))?;
        drop(filter);
        let filter = std::fs::File::open(&filter_path)
            .map_err(|error| AikitError::Sandbox(error.to_string()))?;

        let mut cmd = Command::new(BWRAP);
        cmd.args(bwrap_args(&workspace, command));
        let fd = filter.as_raw_fd();
        // SAFETY: after fork and before exec, only async-signal-safe libc calls are used. The
        // owned File stays captured until the hook finishes, and fd 3 is deliberately inherited
        // by bubblewrap as its seccomp program.
        unsafe {
            cmd.as_std_mut().pre_exec(move || {
                if libc::dup2(fd, 3) < 0 {
                    return Err(std::io::Error::last_os_error());
                }
                if libc::fcntl(3, libc::F_SETFD, 0) < 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        Ok(PreparedCommand {
            command: cmd,
            backend: ActiveContainmentBackend::LinuxNamespace,
            environment_overrides: vec![
                (OsString::from("HOME"), workspace.clone().into_os_string()),
                (OsString::from("TMPDIR"), OsString::from("/tmp")),
            ],
            cleanup: None,
            artifacts: vec![artifacts],
        })
    }
}

/// The exact bubblewrap argv (everything after the `bwrap` program itself). Pure so the isolation
/// profile — and its argv-injection safety — can be unit-tested on any platform.
#[cfg(any(target_os = "linux", test))]
fn bwrap_args(workspace: &Path, command: &str) -> Vec<std::ffi::OsString> {
    use std::ffi::OsString;

    let ws = workspace.as_os_str();
    let mut args: Vec<OsString> = Vec::with_capacity(28);
    for fixed in ["--unshare-all", "--die-with-parent", "--new-session"] {
        args.push(fixed.into());
    }
    for fixed in ["--ro-bind", "/", "/"] {
        args.push(fixed.into());
    }
    args.push("--bind".into());
    args.push(ws.into());
    args.push(ws.into());
    for fixed in ["--tmpfs", "/tmp", "--proc", "/proc", "--dev", "/dev"] {
        args.push(fixed.into());
    }
    args.push("--chdir".into());
    args.push(ws.into());
    args.push("--setenv".into());
    args.push("HOME".into());
    args.push(ws.into());
    for fixed in [
        "--setenv",
        "TMPDIR",
        "/tmp",
        "--seccomp",
        "3",
        "--",
        "/bin/sh",
        "-c",
    ] {
        args.push(fixed.into());
    }
    args.push(command.into());
    args
}

#[cfg(any(target_os = "linux", all(test, unix)))]
fn seccomp_filter() -> Result<Vec<u8>> {
    #[cfg(target_arch = "x86_64")]
    let denied: &[u32] = &[
        101, 155, 165, 166, 175, 176, 246, 250, 298, 304, 313, 321, 323,
    ];
    #[cfg(target_arch = "aarch64")]
    let denied: &[u32] = &[39, 40, 41, 104, 105, 106, 117, 219, 241, 265, 273, 280, 282];
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    return Err(AikitError::Sandbox(
        "seccomp filter is unsupported on this Linux architecture".into(),
    ));

    let mut out = Vec::new();
    push_bpf(&mut out, 0x20, 0, 0, 0); // load seccomp_data.nr
    for syscall in denied {
        push_bpf(&mut out, 0x15, 0, 1, *syscall); // if equal, return EPERM
        push_bpf(&mut out, 0x06, 0, 0, 0x0005_0000 | libc::EPERM as u32);
    }
    push_bpf(&mut out, 0x06, 0, 0, 0x7fff_0000); // SECCOMP_RET_ALLOW
    Ok(out)
}

#[cfg(any(target_os = "linux", all(test, unix)))]
fn push_bpf(out: &mut Vec<u8>, code: u16, jt: u8, jf: u8, k: u32) {
    out.extend_from_slice(&code.to_ne_bytes());
    out.push(jt);
    out.push(jf);
    out.extend_from_slice(&k.to_ne_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsString;

    #[test]
    fn bwrap_argv_pins_isolation_flags() {
        let ws = Path::new("/work/space");
        let args = bwrap_args(ws, "echo merhaba");
        let as_str: Vec<&str> = args.iter().filter_map(|a| a.to_str()).collect();

        for flag in ["--unshare-all", "--die-with-parent", "--new-session"] {
            assert!(as_str.contains(&flag), "missing isolation flag {flag}");
        }
        // Read-only root, then the workspace bound read-write at its own path.
        let ro = as_str
            .iter()
            .position(|a| *a == "--ro-bind")
            .expect("--ro-bind");
        assert_eq!(&as_str[ro..ro + 3], ["--ro-bind", "/", "/"]);
        let bind = as_str.iter().position(|a| *a == "--bind").expect("--bind");
        assert_eq!(
            &as_str[bind..bind + 3],
            ["--bind", "/work/space", "/work/space"]
        );
        // Private /tmp, /proc, /dev; workspace as HOME and cwd; seccomp program on fd 3.
        for window in [
            &["--tmpfs", "/tmp"][..],
            &["--proc", "/proc"][..],
            &["--dev", "/dev"][..],
            &["--chdir", "/work/space"][..],
            &["--setenv", "HOME", "/work/space"][..],
            &["--setenv", "TMPDIR", "/tmp"][..],
            &["--seccomp", "3"][..],
        ] {
            assert!(
                as_str.windows(window.len()).any(|w| *w == *window),
                "missing argv window {window:?}"
            );
        }
        // The shell invocation terminates the argv: -- /bin/sh -c <command last>.
        let tail: Vec<&str> = as_str[as_str.len() - 4..].to_vec();
        assert_eq!(tail, ["--", "/bin/sh", "-c", "echo merhaba"]);
    }

    #[test]
    fn bwrap_argv_is_argv_safe() {
        // Shell metacharacters must stay one un-interpolated trailing argument — never split into
        // additional argv entries and never merged into any other argument.
        let hostile = "true; touch /tmp/pwned; $(id) `id` && echo owned";
        let args = bwrap_args(Path::new("/w"), hostile);
        assert_eq!(args.last(), Some(&OsString::from(hostile)));
        let occurrences = args
            .iter()
            .filter(|a| a.to_str().is_some_and(|s| s.contains("pwned")))
            .count();
        assert_eq!(
            occurrences, 1,
            "hostile command leaked into extra argv entries"
        );
    }

    #[cfg(unix)]
    #[test]
    fn seccomp_filter_encodes_deny_list_and_allow_terminator() {
        let bytes = seccomp_filter().expect("supported test architectures have a filter");
        assert_eq!(bytes.len() % 8, 0, "BPF programs are 8-byte instructions");
        let insns: Vec<(u16, u8, u8, u32)> = bytes
            .chunks_exact(8)
            .map(|c| {
                (
                    u16::from_ne_bytes([c[0], c[1]]),
                    c[2],
                    c[3],
                    u32::from_ne_bytes([c[4], c[5], c[6], c[7]]),
                )
            })
            .collect();

        // Layout: 1 load of seccomp_data.nr, then (jump-if-equal, return-EPERM) per denied
        // syscall, then the SECCOMP_RET_ALLOW terminator.
        assert_eq!(
            insns[0],
            (0x20, 0, 0, 0),
            "first insn must load the syscall nr"
        );
        assert_eq!(
            insns.last().unwrap(),
            &(0x06, 0, 0, 0x7fff_0000),
            "filter must terminate with SECCOMP_RET_ALLOW"
        );
        let denied_pairs = &insns[1..insns.len() - 1];
        assert_eq!(denied_pairs.len() % 2, 0);
        assert_eq!(
            denied_pairs.len() / 2,
            13,
            "each supported architecture denies exactly 13 syscalls"
        );
        let errno_return = 0x0005_0000 | libc::EPERM as u32;
        for pair in denied_pairs.chunks_exact(2) {
            assert_eq!(pair[0].0, 0x15, "deny entries start with a jump-if-equal");
            assert!(pair[0].3 > 0, "denied syscall numbers are non-zero");
            assert_eq!(
                pair[1],
                (0x06, 0, 0, errno_return),
                "denied syscalls must return EPERM, not kill"
            );
        }
    }
}
