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
        cmd.args(["--unshare-all", "--die-with-parent", "--new-session"])
            .args(["--ro-bind", "/", "/"])
            .arg("--bind")
            .arg(&workspace)
            .arg(&workspace)
            .args(["--tmpfs", "/tmp", "--proc", "/proc", "--dev", "/dev"])
            .arg("--chdir")
            .arg(&workspace)
            .args(["--setenv", "HOME"])
            .arg(&workspace)
            .args([
                "--setenv",
                "TMPDIR",
                "/tmp",
                "--seccomp",
                "3",
                "--",
                "/bin/sh",
                "-c",
            ])
            .arg(command);
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

#[cfg(target_os = "linux")]
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

#[cfg(target_os = "linux")]
fn push_bpf(out: &mut Vec<u8>, code: u16, jt: u8, jf: u8, k: u32) {
    out.extend_from_slice(&code.to_ne_bytes());
    out.push(jt);
    out.push(jf);
    out.extend_from_slice(&k.to_ne_bytes());
}
