//! Process hardening and OS-containment orchestration for the `Bash` tool.
//!
//! # What this is (and is NOT)
//!
//! [`BashPolicy`] is portable hardening, not a containment boundary by itself. The built-in Bash
//! tool combines it with fail-closed OS containment; the legacy [`run_bash`] function remains an
//! explicitly uncontained compatibility entry point. New callers should use
//! [`run_bash_with_containment`].
//!
//! Layers, in order of portability:
//!   1. **Environment scrubbing** (all platforms) — `env_clear()` + a small pass-through allow-list.
//!   2. **Wall-clock timeout** (all platforms) — the command is killed if it overruns.
//!   3. **Bounded output** (all platforms) — stdout/stderr capture is capped, so a runaway `yes`
//!      cannot exhaust host memory.
//!   4. **rlimits** (Unix) — CPU seconds, max file size, open files, and (opt-in) process count,
//!      applied in the child between `fork` and `exec` via `pre_exec`.

use crate::error::{AikitError, Result};
use crate::governance::containment::{
    prepare_command, ActiveContainmentBackend, CleanupAction, ContainmentLimits, ContainmentPolicy,
};
use std::ffi::OsString;
use std::path::Path;
use std::time::Duration;
use tokio::io::AsyncReadExt;

/// Resource + environment isolation for a single `Bash` invocation.
#[derive(Debug, Clone)]
pub struct BashPolicy {
    /// If true, skip environment scrubbing and inherit the parent environment verbatim (secrets
    /// included). Off by default — scrubbing is the point.
    pub inherit_env: bool,
    /// Environment variables passed through from the parent when scrubbing. Everything else is
    /// cleared, so secrets never reach the shell unless named here.
    pub env_passthrough: Vec<String>,
    /// Extra variables to set explicitly in the child (name, value).
    pub env_extra: Vec<(String, String)>,
    /// Kill the command if it runs longer than this (wall clock).
    pub timeout: Duration,
    /// Cap on captured stdout and stderr (each), in bytes — bounds host memory.
    pub max_output_bytes: usize,
    /// Max CPU seconds (`RLIMIT_CPU`). Unix only; `None` disables.
    pub max_cpu_seconds: Option<u64>,
    /// Max bytes any single file the process writes may reach (`RLIMIT_FSIZE`). Unix only.
    pub max_file_size_bytes: Option<u64>,
    /// Max open file descriptors (`RLIMIT_NOFILE`). Unix only.
    pub max_open_files: Option<u64>,
    /// Max processes for the UID (`RLIMIT_NPROC`), a fork-bomb backstop. Unix only. **Off by
    /// default**: the limit is per-user, not per-process, so on a shared login it can wedge the
    /// user's other processes — opt in only when aikit runs as a dedicated user.
    pub max_processes: Option<u64>,
}

impl Default for BashPolicy {
    /// Safe-by-default hardening: scrubbed env, 30s timeout, 1 MiB output cap, 10 CPU-seconds,
    /// 64 MiB file-size cap, 256 open files. `max_processes` is off (see its docs).
    fn default() -> Self {
        BashPolicy {
            inherit_env: false,
            env_passthrough: ["PATH", "HOME", "LANG", "TERM", "TZ"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            env_extra: Vec::new(),
            timeout: Duration::from_secs(30),
            max_output_bytes: 1 << 20,
            max_cpu_seconds: Some(10),
            max_file_size_bytes: Some(64 << 20),
            max_open_files: Some(256),
            max_processes: None,
        }
    }
}

impl BashPolicy {
    /// A fully permissive policy: inherit the environment, no rlimits, a long timeout. For trusted
    /// local use where the hardening gets in the way. Prefer [`Default`] otherwise.
    pub fn permissive() -> Self {
        BashPolicy {
            inherit_env: true,
            env_passthrough: Vec::new(),
            env_extra: Vec::new(),
            timeout: Duration::from_secs(600),
            max_output_bytes: 16 << 20,
            max_cpu_seconds: None,
            max_file_size_bytes: None,
            max_open_files: None,
            max_processes: None,
        }
    }
}

/// Compatibility entry point: run under process hardening but **without OS containment**.
/// Prefer [`run_bash_with_containment`] for untrusted commands.
pub async fn run_bash(
    command: &str,
    workdir: Option<&Path>,
    policy: &BashPolicy,
) -> Result<String> {
    run_bash_with_containment(command, workdir, policy, &ContainmentPolicy::uncontained()).await
}

/// Run Bash with an explicit containment policy. Required containment is fail-closed: command
/// preparation returns an error before spawning a shell when no requested backend is available.
pub async fn run_bash_with_containment(
    command: &str,
    workdir: Option<&Path>,
    policy: &BashPolicy,
    containment: &ContainmentPolicy,
) -> Result<String> {
    let environment = child_environment(policy);
    let limits = ContainmentLimits {
        max_cpu_seconds: policy.max_cpu_seconds,
        max_file_size_bytes: policy.max_file_size_bytes,
        max_open_files: policy.max_open_files,
        max_processes: policy.max_processes,
    };
    let prepared = prepare_command(command, workdir, containment, &environment, limits).await?;
    let mut cmd = prepared.command;
    let backend = prepared.backend;
    if backend == ActiveContainmentBackend::Seatbelt {
        if let Some((key, _)) = environment
            .iter()
            .find(|(key, _)| is_loader_control_environment_key(key))
        {
            return Err(AikitError::Sandbox(format!(
                "Seatbelt cannot safely preserve loader-control environment variable {}",
                key.to_string_lossy()
            )));
        }
    }
    let pending_cleanup = prepared.cleanup;
    // Keep private cidfiles/environment artifacts alive until after the cancellation guard drops.
    let _artifacts = prepared.artifacts;

    cmd.stdin(std::process::Stdio::null());
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());
    cmd.kill_on_drop(true);
    if let Some(dir) = workdir {
        cmd.current_dir(dir);
    }

    // Host containment wrappers are control-plane processes. Docker and PowerShell receive only
    // their explicit minimal environment; their workload environment travels through a private
    // backend artifact. Native wrappers that directly exec the workload preserve prior semantics.
    let isolated_wrapper = matches!(
        backend,
        ActiveContainmentBackend::Docker
            | ActiveContainmentBackend::LinuxNamespace
            | ActiveContainmentBackend::WindowsJob
    );
    if isolated_wrapper || !policy.inherit_env {
        cmd.env_clear();
    }
    let launch_environment = if isolated_wrapper {
        prepared.environment_overrides
    } else {
        merge_environment(environment, prepared.environment_overrides)
    };
    cmd.envs(launch_environment);

    // 4. A separate process group plus rlimits, applied between fork and exec (Unix only).
    #[cfg(unix)]
    {
        let rlimits = unix_rlimits(policy);
        // SAFETY: `setsid` and `setrlimit` are async-signal-safe, and the closure touches only its
        // moved-in data, so it is safe to run in the forked child before exec.
        unsafe {
            cmd.pre_exec(move || {
                // A separate session gives timeout cleanup a process-group target. OS
                // containment remains the security boundary if a malicious child detaches.
                if libc::setsid() == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                for (resource, value) in &rlimits {
                    apply_rlimit(*resource, *value)?;
                }
                Ok(())
            });
        }
    }

    #[cfg(not(unix))]
    let _ = &limits;

    let mut child = cmd
        .spawn()
        .map_err(|e| AikitError::ToolExecution(format!("spawn failed: {e}")))?;
    // A failed spawn cannot own a Docker container. Once the launcher exists, cleanup remains
    // guarded by its private cidfile and can never target a merely predictable container name.
    let cleanup_action = pending_cleanup.clone();
    let mut cleanup_guard = CleanupGuard::new(pending_cleanup);
    // `Child::kill_on_drop` only targets the direct child. Keep a separate synchronous guard for
    // the whole Unix session so cancellation (which simply drops this future) cannot leave shell
    // descendants running. This also covers the Seatbelt wrapper and the Docker client process.
    let mut process_group_guard = ProcessGroupGuard::new(child.id());

    let stdout = child.stdout.take().expect("piped stdout");
    let stderr = child.stderr.take().expect("piped stderr");
    let cap = policy.max_output_bytes as u64;

    // 2 + 3. Bounded, concurrent capture of both pipes, under the wall-clock timeout.
    let collected = tokio::time::timeout(policy.timeout, async move {
        let (out_res, err_res) = tokio::join!(read_capped(stdout, cap), read_capped(stderr, cap),);
        let status = child.wait().await?;
        Ok::<_, std::io::Error>((status, out_res?, err_res?))
    })
    .await;

    match collected {
        Ok(Ok((status, out_buf, err_buf))) => {
            // A shell can exit successfully after starting a detached-looking background job.
            // Finish the invocation by killing any process that still belongs to its session.
            process_group_guard.terminate();
            // `docker run --rm` normally removes itself, but a non-zero client exit or daemon
            // disconnect must not turn that expectation into a leaked containment container.
            let cleaned = if let Some(cleanup) = &cleanup_action {
                cleanup.force().await
            } else {
                true
            };
            if cleaned {
                cleanup_guard.disarm();
            }
            let mut body = String::new();
            body.push_str(&String::from_utf8_lossy(&out_buf));
            if !err_buf.is_empty() {
                body.push_str(&String::from_utf8_lossy(&err_buf));
            }
            let code = status.code().unwrap_or(-1);
            Ok(format!("[exit {code}]\n{}", body.trim_end()))
        }
        Ok(Err(e)) => {
            process_group_guard.terminate();
            let cleaned = if let Some(cleanup) = &cleanup_action {
                cleanup.force().await
            } else {
                true
            };
            if cleaned {
                cleanup_guard.disarm();
            }
            Err(AikitError::ToolExecution(format!("io error: {e}")))
        }
        // Timed out: kill the process group and force backend cleanup (notably `docker rm -f`).
        Err(_elapsed) => {
            process_group_guard.terminate();
            let cleaned = if let Some(cleanup) = &cleanup_action {
                cleanup.force().await
            } else {
                true
            };
            if cleaned {
                cleanup_guard.disarm();
            }
            Err(AikitError::ToolExecution(format!(
                "command timed out after {:?}",
                policy.timeout
            )))
        }
    }
}

pub(crate) fn child_environment(policy: &BashPolicy) -> Vec<(OsString, OsString)> {
    let mut environment = Vec::new();
    if policy.inherit_env {
        environment.extend(std::env::vars_os());
    } else {
        for name in &policy.env_passthrough {
            if let Some(value) = std::env::var_os(name) {
                environment.push((OsString::from(name), value));
            }
        }
    }
    environment.extend(
        policy
            .env_extra
            .iter()
            .map(|(key, value)| (OsString::from(key), OsString::from(value))),
    );
    environment
}

fn merge_environment(
    mut environment: Vec<(OsString, OsString)>,
    overrides: Vec<(OsString, OsString)>,
) -> Vec<(OsString, OsString)> {
    for (key, value) in overrides {
        environment.retain(|(existing, _)| existing != &key);
        environment.push((key, value));
    }
    environment
}

fn is_loader_control_environment_key(key: &std::ffi::OsStr) -> bool {
    let key = key.to_string_lossy().to_ascii_uppercase();
    key.starts_with("DYLD_")
        || matches!(key.as_str(), "LD_PRELOAD" | "LD_LIBRARY_PATH" | "LD_AUDIT")
}

struct CleanupGuard {
    action: Option<CleanupAction>,
}

impl CleanupGuard {
    fn new(action: Option<CleanupAction>) -> Self {
        CleanupGuard { action }
    }

    fn disarm(&mut self) {
        self.action = None;
    }
}

impl Drop for CleanupGuard {
    fn drop(&mut self) {
        if let Some(action) = &self.action {
            action.spawn_best_effort();
        }
    }
}

/// Synchronous RAII cleanup for the Unix session created in `pre_exec`.
///
/// Tokio cancellation drops futures; it does not give them an async cleanup phase. Keeping the
/// process-group id outside the child-wait future means every drop path still sends `SIGKILL` to
/// the whole group before returning control to the runtime.
struct ProcessGroupGuard {
    child_id: Option<u32>,
}

impl ProcessGroupGuard {
    fn new(child_id: Option<u32>) -> Self {
        ProcessGroupGuard { child_id }
    }

    fn terminate(&mut self) {
        kill_process_group(self.child_id.take());
    }
}

impl Drop for ProcessGroupGuard {
    fn drop(&mut self) {
        self.terminate();
    }
}

#[cfg(unix)]
fn kill_process_group(child_id: Option<u32>) {
    if let Some(child_id) = child_id {
        // SAFETY: negative pid addresses the process group created by `setsid`; SIGKILL has no
        // user-space handler. The id belongs to the dedicated session created just before exec.
        unsafe {
            libc::kill(-(child_id as libc::pid_t), libc::SIGKILL);
        }
    }
}

#[cfg(not(unix))]
fn kill_process_group(_child_id: Option<u32>) {}

/// Read at most `cap` bytes from `reader` (draining to EOF or the cap). Bounds memory: a process
/// that keeps writing past the cap blocks on a full pipe and is reaped by the timeout.
async fn read_capped<R>(reader: R, cap: u64) -> std::io::Result<Vec<u8>>
where
    R: AsyncReadExt + Unpin,
{
    let mut buf = Vec::new();
    reader.take(cap).read_to_end(&mut buf).await?;
    Ok(buf)
}

/// The platform type of a `setrlimit` resource id (Linux widened it to `__rlimit_resource_t`).
#[cfg(all(unix, target_os = "linux"))]
type RlimitResource = libc::__rlimit_resource_t;
#[cfg(all(unix, not(target_os = "linux")))]
type RlimitResource = libc::c_int;

/// The (resource, value) rlimits this policy asks for, in application order.
#[cfg(unix)]
fn unix_rlimits(policy: &BashPolicy) -> Vec<(RlimitResource, u64)> {
    let mut v = Vec::new();
    if let Some(s) = policy.max_cpu_seconds {
        v.push((libc::RLIMIT_CPU, s));
    }
    if let Some(b) = policy.max_file_size_bytes {
        v.push((libc::RLIMIT_FSIZE, b));
    }
    if let Some(n) = policy.max_open_files {
        v.push((libc::RLIMIT_NOFILE, n));
    }
    if let Some(p) = policy.max_processes {
        v.push((libc::RLIMIT_NPROC, p));
    }
    v
}

/// Set both the soft and hard limit for `resource` to `value`. Runs in the forked child.
#[cfg(unix)]
fn apply_rlimit(resource: RlimitResource, value: u64) -> std::io::Result<()> {
    let rlim = libc::rlimit {
        rlim_cur: value as libc::rlim_t,
        rlim_max: value as libc::rlim_t,
    };
    // SAFETY: `rlim` is a valid, fully-initialized struct; setrlimit reads it and returns a status.
    let rc = unsafe { libc::setrlimit(resource, &rlim) };
    if rc != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    static FAKE_DOCKER_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    fn fast() -> BashPolicy {
        // A quick policy for tests: short timeout, tiny caps stay off unless a test sets them.
        BashPolicy {
            timeout: Duration::from_secs(5),
            ..BashPolicy::default()
        }
    }

    #[cfg(unix)]
    async fn wait_for_pid(path: &Path) -> libc::pid_t {
        tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                if let Ok(pid) = std::fs::read_to_string(path) {
                    if let Ok(pid) = pid.trim().parse::<libc::pid_t>() {
                        break pid;
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("background descendant never started")
    }

    #[cfg(unix)]
    async fn assert_process_exits(pid: libc::pid_t, context: &str) {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                // SAFETY: signal 0 only tests whether this process id is still live.
                let rc = unsafe { libc::kill(pid, 0) };
                if rc == -1 && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("background descendant survived {context}"));
    }

    #[tokio::test]
    async fn runs_and_reports_exit_code() {
        let out = run_bash("echo governed", None, &fast()).await.unwrap();
        assert!(out.contains("governed"));
        assert!(out.contains("[exit 0]"));
    }

    #[tokio::test]
    async fn scrubs_secrets_from_the_environment() {
        // A secret in the PARENT env must NOT reach the shell (it isn't in the pass-through list).
        std::env::set_var("AIKIT_FAKE_SECRET", "sk-super-secret");
        let out = run_bash("echo [${AIKIT_FAKE_SECRET:-CLEARED}]", None, &fast())
            .await
            .unwrap();
        std::env::remove_var("AIKIT_FAKE_SECRET");
        assert!(
            out.contains("[CLEARED]"),
            "secret leaked into the shell: {out}"
        );
        assert!(!out.contains("super-secret"));
    }

    #[tokio::test]
    async fn passes_through_whitelisted_and_extra_vars() {
        let mut policy = fast();
        policy
            .env_extra
            .push(("AIKIT_GREETING".into(), "merhaba".into()));
        let out = run_bash("echo $AIKIT_GREETING", None, &policy)
            .await
            .unwrap();
        assert!(out.contains("merhaba"));
    }

    #[test]
    fn identifies_loader_control_environment_keys_for_seatbelt_fail_closed() {
        for key in [
            "DYLD_INSERT_LIBRARIES",
            "LD_PRELOAD",
            "LD_LIBRARY_PATH",
            "LD_AUDIT",
        ] {
            assert!(is_loader_control_environment_key(std::ffi::OsStr::new(key)));
        }
        assert!(!is_loader_control_environment_key(std::ffi::OsStr::new(
            "PATH"
        )));
    }

    #[tokio::test]
    async fn enforces_the_wall_clock_timeout() {
        let mut policy = fast();
        policy.timeout = Duration::from_millis(200);
        let started = std::time::Instant::now();
        let err = run_bash("sleep 5", None, &policy).await.unwrap_err();
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "did not kill promptly"
        );
        assert!(matches!(err, AikitError::ToolExecution(m) if m.contains("timed out")));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn timeout_kills_background_descendants() {
        let dir = tempfile::tempdir().unwrap();
        let descendant_pid_file = dir.path().join("timeout-descendant.pid");
        let mut policy = fast();
        policy.timeout = Duration::from_millis(200);
        policy.env_extra.push((
            "AIKIT_DESCENDANT_PID_FILE".into(),
            descendant_pid_file.to_string_lossy().into_owned(),
        ));

        let error = run_bash(
            "sleep 30 & echo $! > \"$AIKIT_DESCENDANT_PID_FILE\"; wait",
            Some(dir.path()),
            &policy,
        )
        .await
        .unwrap_err();

        assert!(
            matches!(error, AikitError::ToolExecution(message) if message.contains("timed out"))
        );
        let descendant_pid = wait_for_pid(&descendant_pid_file).await;
        assert_process_exits(descendant_pid, "the Bash wall-clock timeout").await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn dropping_pending_execution_kills_background_descendants() {
        let dir = tempfile::tempdir().unwrap();
        let descendant_pid_file = dir.path().join("descendant.pid");
        let mut policy = fast();
        policy.timeout = Duration::from_secs(30);
        policy.env_extra.push((
            "AIKIT_DESCENDANT_PID_FILE".into(),
            descendant_pid_file.to_string_lossy().into_owned(),
        ));
        let workdir = dir.path().to_path_buf();

        let task = tokio::spawn(async move {
            run_bash(
                "sleep 30 & echo $! > \"$AIKIT_DESCENDANT_PID_FILE\"; wait",
                Some(&workdir),
                &policy,
            )
            .await
        });

        let descendant_pid = wait_for_pid(&descendant_pid_file).await;

        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_process_exits(descendant_pid, "Bash future cancellation").await;
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn dropping_pending_seatbelt_execution_kills_background_descendants() {
        let workspace = tempfile::tempdir().unwrap();
        let descendant_pid_file = workspace.path().join("seatbelt-descendant.pid");
        let mut policy = fast();
        policy.timeout = Duration::from_secs(30);
        policy.env_extra.push((
            "AIKIT_DESCENDANT_PID_FILE".into(),
            descendant_pid_file.to_string_lossy().into_owned(),
        ));
        let workdir = workspace.path().to_path_buf();

        let task = tokio::spawn(async move {
            run_bash_with_containment(
                "sleep 30 & echo $! > \"$AIKIT_DESCENDANT_PID_FILE\"; wait",
                Some(&workdir),
                &policy,
                &ContainmentPolicy::required_seatbelt(),
            )
            .await
        });

        let descendant_pid = wait_for_pid(&descendant_pid_file).await;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_process_exits(descendant_pid, "Seatbelt Bash future cancellation").await;
    }

    #[tokio::test]
    async fn caps_captured_output() {
        let mut policy = fast();
        policy.max_output_bytes = 50;
        // The command prints 200 'a's and exits; we must capture no more than the cap.
        let out = run_bash("printf 'a%.0s' $(seq 1 200)", None, &policy)
            .await
            .unwrap();
        let body = out.strip_prefix("[exit 0]\n").unwrap_or(&out);
        assert!(body.len() <= 50, "output not capped: {} bytes", body.len());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn rlimit_fsize_stops_a_runaway_write() {
        let dir = tempfile::tempdir().unwrap();
        let mut policy = fast();
        policy.max_file_size_bytes = Some(4096); // 4 KiB ceiling
                                                 // Try to write ~1 MiB — RLIMIT_FSIZE trips SIGXFSZ, so the write fails (non-zero exit).
        let out = run_bash(
            "head -c 1048576 /dev/zero > big.dat",
            Some(dir.path()),
            &policy,
        )
        .await
        .unwrap();
        assert!(
            !out.contains("[exit 0]"),
            "runaway write was not stopped: {out}"
        );
        // And the file on disk never exceeded the limit.
        let sz = std::fs::metadata(dir.path().join("big.dat"))
            .map(|m| m.len())
            .unwrap_or(0);
        assert!(sz <= 4096, "file exceeded the FSIZE limit: {sz} bytes");
    }

    #[tokio::test]
    async fn required_containment_without_a_workspace_fails_before_shell_spawn() {
        let outside = tempfile::tempdir().unwrap();
        let sentinel = outside.path().join("must-not-exist");
        let mut policy = fast();
        policy.env_extra.push((
            "AIKIT_SENTINEL".into(),
            sentinel.to_string_lossy().into_owned(),
        ));

        let error = run_bash_with_containment(
            "touch \"$AIKIT_SENTINEL\"",
            None,
            &policy,
            &ContainmentPolicy::default(),
        )
        .await
        .unwrap_err();

        assert!(matches!(error, AikitError::Sandbox(_)));
        assert_eq!(error.info().code, crate::error::ErrorCode::Sandbox);
        assert!(
            !sentinel.exists(),
            "the host shell ran despite fail-closed mode"
        );
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn seatbelt_really_denies_a_write_outside_the_workspace() {
        let workspace = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let blocked = outside.path().join("blocked.txt");
        let mut policy = fast();
        policy.env_extra.push((
            "AIKIT_OUTSIDE".into(),
            blocked.to_string_lossy().into_owned(),
        ));

        let output = run_bash_with_containment(
            "printf inside > inside.txt; if printf blocked > \"$AIKIT_OUTSIDE\"; then exit 91; else exit 0; fi",
            Some(workspace.path()),
            &policy,
            &ContainmentPolicy::required_seatbelt(),
        )
        .await
        .unwrap();

        assert!(
            output.contains("[exit 0]"),
            "Seatbelt denial probe failed: {output}"
        );
        assert_eq!(
            std::fs::read_to_string(workspace.path().join("inside.txt")).unwrap(),
            "inside"
        );
        assert!(
            !blocked.exists(),
            "Seatbelt allowed an out-of-workspace write"
        );
    }

    #[cfg(unix)]
    fn fake_docker(holder: &Path) -> std::path::PathBuf {
        use std::os::unix::fs::PermissionsExt;

        let executable = holder.join("fake-docker");
        std::fs::write(
            &executable,
            r#"#!/bin/sh
log="${0}.log"
if [ -n "${AIKIT_WORKLOAD_SECRET:-}" ] || [ -n "${LD_PRELOAD:-}" ] || [ -n "${DOCKER_CONFIG:-}" ]; then
  printf '%s\n' HOST_ENV_LEAK >> "$log"
  exit 97
fi
case "$1" in
  info)
    printf '%s\n' INFO >> "$log"
    printf '%s\n' seccomp
    ;;
  image)
    printf '%s\n' IMAGE >> "$log"
    printf '%s\n' sha256:ready
    ;;
  run)
    printf '%s\n' RUN >> "$log"
    cidfile=
    envfile=
    while [ "$#" -gt 0 ]; do
      printf 'ARG:%s\n' "$1" >> "$log"
      case "$1" in
        --cidfile)
          shift
          cidfile=$1
          printf 'ARG:%s\n' "$1" >> "$log"
          ;;
        --env-file)
          shift
          envfile=$1
          printf 'ARG:%s\n' "$1" >> "$log"
          ;;
      esac
      shift
    done
    if [ -f "${0}.collision" ]; then
      printf '%s\n' COLLISION >> "$log"
      exit 125
    fi
    printf '%s\n' bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb > "$cidfile"
    while IFS= read -r line; do
      printf 'GUEST_ENV:%s\n' "$line" >> "$log"
    done < "$envfile"
    printf '%s\n' 'fake container ran'
    ;;
  rm)
    if [ -f "${0}.fail-first-rm" ] && [ ! -f "${0}.rm-attempted" ]; then
      : > "${0}.rm-attempted"
      printf 'REMOVE_FAILED:%s\n' "$3" >> "$log"
      exit 86
    fi
    printf 'REMOVE:%s\n' "$3" >> "$log"
    ;;
  *)
    exit 99
    ;;
esac
"#,
        )
        .unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
        executable
    }

    #[cfg(unix)]
    fn fake_docker_policy(executable: &Path) -> (BashPolicy, ContainmentPolicy) {
        use crate::governance::containment::DockerConfig;

        let mut bash = fast();
        bash.env_extra.extend([
            ("DOCKER_HOST".into(), "tcp://trusted-daemon".into()),
            ("AIKIT_WORKLOAD_SECRET".into(), "guest-only-secret".into()),
            ("LD_PRELOAD".into(), "guest-preload.so".into()),
            ("DOCKER_CONFIG".into(), "/workload-untrusted-config".into()),
        ]);
        let docker = DockerConfig::new(format!("example/aikit@sha256:{}", "a".repeat(64)))
            .with_executable(executable);
        (bash, ContainmentPolicy::required_docker(docker))
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn docker_launch_separates_control_and_workload_environment_and_cleans_owned_id() {
        let _serial = FAKE_DOCKER_TEST_LOCK.lock().await;
        let holder = tempfile::tempdir().unwrap();
        let workspace = holder.path().join("workspace");
        std::fs::create_dir(&workspace).unwrap();
        let executable = fake_docker(holder.path());
        let (bash, containment) = fake_docker_policy(&executable);

        let output = run_bash_with_containment(
            "printf '%s' \"$AIKIT_WORKLOAD_SECRET\"",
            Some(&workspace),
            &bash,
            &containment,
        )
        .await
        .unwrap();

        assert!(output.contains("[exit 0]"));
        let log = std::fs::read_to_string(executable.with_extension("log")).unwrap();
        assert!(!log.contains("HOST_ENV_LEAK"));
        assert!(log.contains("GUEST_ENV:AIKIT_WORKLOAD_SECRET=guest-only-secret"));
        assert!(log.contains("GUEST_ENV:LD_PRELOAD=guest-preload.so"));
        assert!(log.contains("GUEST_ENV:DOCKER_CONFIG=/workload-untrusted-config"));
        assert!(log.contains(&format!("REMOVE:{}", "b".repeat(64))));
        for line in log.lines().filter(|line| line.starts_with("ARG:")) {
            assert!(!line.contains("guest-only-secret"));
            assert!(!line.contains("guest-preload.so"));
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn docker_name_collision_without_cidfile_never_deletes_unowned_container() {
        let _serial = FAKE_DOCKER_TEST_LOCK.lock().await;
        let holder = tempfile::tempdir().unwrap();
        let workspace = holder.path().join("workspace");
        std::fs::create_dir(&workspace).unwrap();
        let executable = fake_docker(holder.path());
        std::fs::write(executable.with_extension("collision"), "occupied").unwrap();
        let (bash, containment) = fake_docker_policy(&executable);

        let output = run_bash_with_containment("true", Some(&workspace), &bash, &containment)
            .await
            .unwrap();

        assert!(output.contains("[exit 125]"));
        let log = std::fs::read_to_string(executable.with_extension("log")).unwrap();
        assert!(log.contains("COLLISION"));
        assert!(!log.contains("REMOVE:"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn docker_cleanup_guard_retries_after_synchronous_rm_failure() {
        let _serial = FAKE_DOCKER_TEST_LOCK.lock().await;
        let holder = tempfile::tempdir().unwrap();
        let workspace = holder.path().join("workspace");
        std::fs::create_dir(&workspace).unwrap();
        let executable = fake_docker(holder.path());
        std::fs::write(executable.with_extension("fail-first-rm"), "retry").unwrap();
        let log_path = executable.with_extension("log");
        let (bash, containment) = fake_docker_policy(&executable);

        let output = run_bash_with_containment("true", Some(&workspace), &bash, &containment)
            .await
            .unwrap();
        assert!(output.contains("[exit 0]"));
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if std::fs::read_to_string(&log_path).is_ok_and(|log| log.contains("REMOVE:")) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("Drop cleanup did not retry the owned container ID");

        let log = std::fs::read_to_string(log_path).unwrap();
        assert!(log.contains(&format!("REMOVE_FAILED:{}", "b".repeat(64))));
        assert!(log.contains(&format!("REMOVE:{}", "b".repeat(64))));
    }

    #[tokio::test]
    #[ignore = "requires a running Docker daemon and AIKIT_TEST_DOCKER_IMAGE pinned by digest"]
    async fn docker_really_has_a_read_only_root_and_writable_workspace() {
        use crate::governance::containment::DockerConfig;

        let image = std::env::var("AIKIT_TEST_DOCKER_IMAGE")
            .expect("set AIKIT_TEST_DOCKER_IMAGE=name@sha256:<64 hex>");
        let workspace = tempfile::tempdir().unwrap();
        let output = run_bash_with_containment(
            "printf inside > inside.txt; if printf blocked > /aikit-root-write; then exit 91; else exit 0; fi",
            Some(workspace.path()),
            &fast(),
            &ContainmentPolicy::required_docker(DockerConfig::new(image)),
        )
        .await
        .unwrap();

        assert!(
            output.contains("[exit 0]"),
            "Docker denial probe failed: {output}"
        );
        assert_eq!(
            std::fs::read_to_string(workspace.path().join("inside.txt")).unwrap(),
            "inside"
        );
    }
}
