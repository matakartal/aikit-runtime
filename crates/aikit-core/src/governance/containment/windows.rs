use super::{
    ActiveContainmentBackend, BackendCapability, ContainmentGuarantees, ContainmentLimits,
    PreparedCommand,
};
use crate::error::{AikitError, Result};
use std::path::Path;

// Windows PowerShell 5.1 performs a cold C# compilation for Add-Type on fresh CI hosts. That
// control-plane startup gets its own realistic outer budget; once native Run starts, the probe's
// child remains independently bounded to five seconds and cleanup remains bounded in the launcher.
#[cfg(any(target_os = "windows", test))]
const WINDOWS_JOB_PROBE_LAUNCHER_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);
#[cfg(any(target_os = "windows", test))]
const WINDOWS_JOB_PROBE_CHILD_WAIT_MS: u32 = 5_000;

pub(super) async fn capability(workdir: Option<&Path>) -> BackendCapability {
    #[cfg(not(target_os = "windows"))]
    {
        let _ = workdir;
        BackendCapability::unavailable(
            ActiveContainmentBackend::WindowsJob,
            ContainmentGuarantees::windows_job(),
            "Windows Job containment is available only on Windows",
        )
    }
    #[cfg(target_os = "windows")]
    {
        let Some(workdir) = workdir else {
            return BackendCapability::unavailable(
                ActiveContainmentBackend::WindowsJob,
                ContainmentGuarantees::windows_job(),
                "Windows Job containment requires a workspace root",
            );
        };
        let mut prepared = match prepare("exit /b 0", workdir, &[], ContainmentLimits::default()) {
            Ok(prepared) => prepared,
            Err(error) => {
                return BackendCapability::unavailable(
                    ActiveContainmentBackend::WindowsJob,
                    ContainmentGuarantees::windows_job(),
                    error.to_string(),
                )
            }
        };
        let Some(artifacts) = prepared.artifacts.first() else {
            return BackendCapability::unavailable(
                ActiveContainmentBackend::WindowsJob,
                ContainmentGuarantees::windows_job(),
                "Windows Job probe has no private diagnostics directory",
            );
        };
        let phase_file = artifacts.path().join("launcher.phase");
        if let Err(error) = std::fs::write(&phase_file, b"prepared") {
            return BackendCapability::unavailable(
                ActiveContainmentBackend::WindowsJob,
                ContainmentGuarantees::windows_job(),
                format!("Windows Job probe diagnostics could not initialize: {error}"),
            );
        }
        prepared.environment_overrides.extend([
            (
                std::ffi::OsString::from("AIKIT_JOB_PHASE_FILE"),
                phase_file.clone().into_os_string(),
            ),
            (
                std::ffi::OsString::from("AIKIT_JOB_WAIT_MS"),
                std::ffi::OsString::from(WINDOWS_JOB_PROBE_CHILD_WAIT_MS.to_string()),
            ),
        ]);
        prepared.command.stdin(std::process::Stdio::null());
        prepared.command.stdout(std::process::Stdio::null());
        // The probe has no output contract. Keep its streams non-piped and judge only the launcher
        // status; native lifecycle diagnostics travel through the private phase file above.
        prepared.command.stderr(std::process::Stdio::null());
        prepared.command.kill_on_drop(true);
        prepared.command.env_clear();
        prepared
            .command
            .envs(prepared.environment_overrides.clone());
        let outcome = tokio::time::timeout(
            WINDOWS_JOB_PROBE_LAUNCHER_TIMEOUT,
            prepared.command.status(),
        )
        .await;
        let phase = read_launcher_phase(&phase_file);
        match outcome {
            Ok(Ok(status)) if status.success() => BackendCapability::available(
                ActiveContainmentBackend::WindowsJob,
                ContainmentGuarantees::windows_job(),
                "suspended child assignment to kill-on-close Windows Job succeeded; process limit enforced, job-memory limit is host-dependent, filesystem/network are not isolated",
            ),
            Ok(Ok(status)) => BackendCapability::unavailable(
                ActiveContainmentBackend::WindowsJob,
                ContainmentGuarantees::windows_job(),
                format!("Windows Job probe failed with status {status}; phase={phase}"),
            ),
            Ok(Err(error)) => BackendCapability::unavailable(
                ActiveContainmentBackend::WindowsJob,
                ContainmentGuarantees::windows_job(),
                format!("Windows Job probe could not start: {error}; phase={phase}"),
            ),
            Err(_) => BackendCapability::unavailable(
                ActiveContainmentBackend::WindowsJob,
                ContainmentGuarantees::windows_job(),
                format!("Windows Job probe timed out; phase={phase}"),
            ),
        }
    }
}

#[cfg(target_os = "windows")]
fn read_launcher_phase(path: &Path) -> &'static str {
    let Ok(raw) = std::fs::read_to_string(path) else {
        return "unreadable";
    };
    safe_launcher_phase(&raw)
}

/// Convert the private launcher marker to a fixed vocabulary before it reaches public capability
/// detail. Never echo file contents: command and workload environment values must stay private.
#[cfg(any(target_os = "windows", test))]
fn safe_launcher_phase(raw: &str) -> &'static str {
    match raw.trim() {
        "prepared" => "prepared",
        "powershell_started" => "powershell_started",
        "native_type_loaded" => "native_type_loaded",
        "workload_environment_loaded" => "workload_environment_loaded",
        "job_create" => "job_create",
        "job_configure" => "job_configure",
        "child_create" => "child_create",
        "child_assign" => "child_assign",
        "child_resume" => "child_resume",
        "child_wait" => "child_wait",
        "child_wait_timed_out" => "child_wait_timed_out",
        "child_exited" => "child_exited",
        "cleanup" => "cleanup",
        "complete" => "complete",
        _ => "unrecognized",
    }
}

pub(super) fn prepare(
    command: &str,
    workdir: &Path,
    environment: &[(std::ffi::OsString, std::ffi::OsString)],
    limits: ContainmentLimits,
) -> Result<PreparedCommand> {
    #[cfg(not(target_os = "windows"))]
    {
        let _ = (command, workdir, environment, limits);
        Err(AikitError::Sandbox(
            "Windows Job containment is unavailable on this platform".into(),
        ))
    }
    #[cfg(target_os = "windows")]
    {
        use tokio::process::Command;

        let workspace = std::fs::canonicalize(workdir).map_err(|error| {
            AikitError::Sandbox(format!("cannot canonicalize Windows workspace: {error}"))
        })?;
        let system = resolve_windows_system_paths()?;
        let artifacts = tempfile::Builder::new()
            .prefix("aikit-windows-job-")
            .tempdir()
            .map_err(|error| AikitError::Sandbox(error.to_string()))?;
        let control_temp = artifacts.path().join("control-temp");
        std::fs::create_dir(&control_temp).map_err(|error| {
            AikitError::Sandbox(format!(
                "cannot create private Windows launcher temp directory: {error}"
            ))
        })?;
        let environment_file = artifacts.path().join("workload.env");
        write_workload_environment(&environment_file, environment)?;
        let script = encode_powershell(WINDOWS_JOB_LAUNCHER);
        let mut cmd = Command::new(&system.powershell);
        cmd.args([
            "-NoLogo",
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-EncodedCommand",
            &script,
        ]);
        Ok(PreparedCommand {
            command: cmd,
            backend: ActiveContainmentBackend::WindowsJob,
            environment_overrides: job_environment(
                command,
                workspace,
                &system,
                environment_file,
                control_temp,
                limits,
            ),
            cleanup: None,
            artifacts: vec![artifacts],
        })
    }
}

#[cfg(any(target_os = "windows", test))]
struct WindowsSystemPaths {
    root: std::path::PathBuf,
    powershell: std::path::PathBuf,
    shell: std::path::PathBuf,
    path: std::ffi::OsString,
}

#[cfg(target_os = "windows")]
fn resolve_windows_system_paths() -> Result<WindowsSystemPaths> {
    let configured_root = std::env::var_os("SystemRoot")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from(r"C:\Windows"));
    if !configured_root.is_absolute() {
        return Err(AikitError::Sandbox(
            "Windows SystemRoot must be an absolute path".into(),
        ));
    }
    let root = std::fs::canonicalize(&configured_root).map_err(|error| {
        AikitError::Sandbox(format!(
            "cannot canonicalize Windows SystemRoot {}: {error}",
            configured_root.display()
        ))
    })?;
    let resolve_executable = |relative: &Path| -> Result<std::path::PathBuf> {
        let candidate = root.join(relative);
        let executable = std::fs::canonicalize(&candidate).map_err(|error| {
            AikitError::Sandbox(format!(
                "cannot resolve Windows system executable {}: {error}",
                candidate.display()
            ))
        })?;
        if !executable.is_file() || !executable.starts_with(&root) {
            return Err(AikitError::Sandbox(format!(
                "Windows system executable is not a regular file under SystemRoot: {}",
                executable.display()
            )));
        }
        Ok(executable)
    };
    let powershell =
        resolve_executable(Path::new(r"System32\WindowsPowerShell\v1.0\powershell.exe"))?;
    let shell = resolve_executable(Path::new(r"System32\cmd.exe"))?;
    let path = std::env::join_paths([
        root.join("System32"),
        root.clone(),
        root.join(r"System32\WindowsPowerShell\v1.0"),
    ])
    .map_err(|error| AikitError::Sandbox(format!("invalid Windows control PATH: {error}")))?;
    Ok(WindowsSystemPaths {
        root,
        powershell,
        shell,
        path,
    })
}

#[cfg(target_os = "windows")]
fn write_workload_environment(
    path: &Path,
    environment: &[(std::ffi::OsString, std::ffi::OsString)],
) -> Result<()> {
    use std::io::Write;
    use std::os::windows::ffi::OsStrExt;

    let encode = |value: &std::ffi::OsStr| {
        let bytes = value
            .encode_wide()
            .flat_map(u16::to_le_bytes)
            .collect::<Vec<_>>();
        base64(&bytes)
    };
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|error| AikitError::Sandbox(error.to_string()))?;
    for (key, value) in environment {
        writeln!(file, "{}\t{}", encode(key), encode(value))
            .map_err(|error| AikitError::Sandbox(error.to_string()))?;
    }
    file.sync_all()
        .map_err(|error| AikitError::Sandbox(error.to_string()))
}

/// The environment contract between `prepare` and the PowerShell launcher: the untrusted command,
/// resolved workdir/shell, and clamped limits travel as `AIKIT_JOB_*` variables (which the
/// launcher scrubs before exec). Pure so the contract is unit-testable on any platform.
#[cfg(any(target_os = "windows", test))]
fn job_environment(
    command: &str,
    workspace: std::path::PathBuf,
    system: &WindowsSystemPaths,
    environment_file: std::path::PathBuf,
    control_temp: std::path::PathBuf,
    limits: ContainmentLimits,
) -> Vec<(std::ffi::OsString, std::ffi::OsString)> {
    use std::ffi::OsString;

    let process_limit = limits.max_processes.unwrap_or(64).clamp(1, u32::MAX as u64);
    let memory_limit = 512_u64 << 20;
    vec![
        (
            OsString::from("SystemRoot"),
            system.root.clone().into_os_string(),
        ),
        (
            OsString::from("WINDIR"),
            system.root.clone().into_os_string(),
        ),
        (OsString::from("PATH"), system.path.clone()),
        (
            OsString::from("TEMP"),
            control_temp.clone().into_os_string(),
        ),
        (OsString::from("TMP"), control_temp.into_os_string()),
        (OsString::from("AIKIT_JOB_COMMAND"), OsString::from(command)),
        (
            OsString::from("AIKIT_JOB_WORKDIR"),
            workspace.into_os_string(),
        ),
        (
            OsString::from("AIKIT_JOB_SHELL"),
            system.shell.clone().into_os_string(),
        ),
        (
            OsString::from("AIKIT_JOB_ENV_FILE"),
            environment_file.into_os_string(),
        ),
        (
            OsString::from("AIKIT_JOB_PROCESS_LIMIT"),
            OsString::from(process_limit.to_string()),
        ),
        (
            OsString::from("AIKIT_JOB_MEMORY_LIMIT"),
            OsString::from(memory_limit.to_string()),
        ),
    ]
}

#[cfg(any(target_os = "windows", test))]
fn encode_powershell(script: &str) -> String {
    let bytes: Vec<u8> = script.encode_utf16().flat_map(u16::to_le_bytes).collect();
    base64(&bytes)
}

#[cfg(any(target_os = "windows", test))]
fn base64(bytes: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let n = (u32::from(chunk[0]) << 16)
            | (u32::from(*chunk.get(1).unwrap_or(&0)) << 8)
            | u32::from(*chunk.get(2).unwrap_or(&0));
        out.push(TABLE[((n >> 18) & 63) as usize] as char);
        out.push(TABLE[((n >> 12) & 63) as usize] as char);
        out.push(if chunk.len() > 1 {
            TABLE[((n >> 6) & 63) as usize] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            TABLE[(n & 63) as usize] as char
        } else {
            '='
        });
    }
    out
}

#[cfg(any(target_os = "windows", test))]
const WINDOWS_JOB_LAUNCHER: &str = r#"
$src = @'
using System;
using System.ComponentModel;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
public static class AikitJob {
  const uint WAIT_OBJECT_0 = 0u, WAIT_TIMEOUT = 0x102u, STILL_ACTIVE = 259u, TERMINATION_WAIT_MS = 5000u;
  [StructLayout(LayoutKind.Sequential, CharSet=CharSet.Unicode)] public struct STARTUPINFO { public int cb; public string lpReserved; public string lpDesktop; public string lpTitle; public int dwX; public int dwY; public int dwXSize; public int dwYSize; public int dwXCountChars; public int dwYCountChars; public int dwFillAttribute; public int dwFlags; public short wShowWindow; public short cbReserved2; public IntPtr lpReserved2; public IntPtr hStdInput; public IntPtr hStdOutput; public IntPtr hStdError; }
  [StructLayout(LayoutKind.Sequential)] public struct PROCESS_INFORMATION { public IntPtr hProcess; public IntPtr hThread; public int dwProcessId; public int dwThreadId; }
  [StructLayout(LayoutKind.Sequential)] public struct IO_COUNTERS { public ulong ReadOperationCount, WriteOperationCount, OtherOperationCount, ReadTransferCount, WriteTransferCount, OtherTransferCount; }
  [StructLayout(LayoutKind.Sequential)] public struct BASIC_LIMITS { public long PerProcessUserTimeLimit, PerJobUserTimeLimit; public uint LimitFlags; public UIntPtr MinimumWorkingSetSize, MaximumWorkingSetSize; public uint ActiveProcessLimit; public UIntPtr Affinity; public uint PriorityClass, SchedulingClass; }
  [StructLayout(LayoutKind.Sequential)] public struct EXTENDED_LIMITS { public BASIC_LIMITS BasicLimitInformation; public IO_COUNTERS IoInfo; public UIntPtr ProcessMemoryLimit, JobMemoryLimit, PeakProcessMemoryUsed, PeakJobMemoryUsed; }
  [DllImport("kernel32.dll", CharSet=CharSet.Unicode, SetLastError=true)] static extern bool CreateProcessW(string app, StringBuilder cmd, IntPtr pa, IntPtr ta, bool inherit, uint flags, IntPtr env, string cwd, ref STARTUPINFO si, out PROCESS_INFORMATION pi);
  [DllImport("kernel32.dll", SetLastError=true)] static extern IntPtr CreateJobObjectW(IntPtr attr, string name);
  [DllImport("kernel32.dll", SetLastError=true)] static extern bool SetInformationJobObject(IntPtr job, int info, ref EXTENDED_LIMITS data, uint len);
  [DllImport("kernel32.dll", SetLastError=true)] static extern bool AssignProcessToJobObject(IntPtr job, IntPtr process);
  [DllImport("kernel32.dll", SetLastError=true)] static extern uint ResumeThread(IntPtr thread);
  [DllImport("kernel32.dll", SetLastError=true)] static extern bool TerminateProcess(IntPtr process, uint exitCode);
  [DllImport("kernel32.dll", SetLastError=true)] static extern uint WaitForSingleObject(IntPtr handle, uint ms);
  [DllImport("kernel32.dll", SetLastError=true)] static extern bool GetExitCodeProcess(IntPtr process, out uint code);
  [DllImport("kernel32.dll")] static extern bool CloseHandle(IntPtr handle);
  [DllImport("kernel32.dll")] static extern IntPtr GetStdHandle(int which);
  static void Check(bool ok, string op) { if (!ok) { int code = Marshal.GetLastWin32Error(); throw new Win32Exception(code, op + " (Win32 " + code + ")"); } }
  static void Phase(string path, string phase) { if (!String.IsNullOrWhiteSpace(path)) { try { File.WriteAllText(path, phase, Encoding.ASCII); } catch { } } }
  static void StopChild(IntPtr process) {
    uint code;
    if (GetExitCodeProcess(process, out code) && code != STILL_ACTIVE) return;
    Check(TerminateProcess(process, 1u), "TerminateProcess");
    uint waited = WaitForSingleObject(process, TERMINATION_WAIT_MS);
    Check(waited == WAIT_OBJECT_0, "WaitForSingleObject terminated child");
  }
  public static int Run(string command, string cwd, string shell, uint processes, ulong memory, uint waitMilliseconds, string phaseFile) {
    if (String.IsNullOrWhiteSpace(cwd)) cwd = Environment.CurrentDirectory;
    if (String.IsNullOrWhiteSpace(shell)) shell = @"C:\Windows\System32\cmd.exe";
    if (cwd.StartsWith(@"\\?\UNC\", StringComparison.OrdinalIgnoreCase)) cwd = @"\\" + cwd.Substring(8);
    else if (cwd.StartsWith(@"\\?\", StringComparison.OrdinalIgnoreCase)) cwd = cwd.Substring(4);
    Environment.CurrentDirectory = cwd;
    Phase(phaseFile, "job_create");
    IntPtr job = CreateJobObjectW(IntPtr.Zero, null); if (job == IntPtr.Zero) throw new Win32Exception();
    var limits = new EXTENDED_LIMITS(); limits.BasicLimitInformation.LimitFlags = 0x2000u | 0x8u | 0x200u; limits.BasicLimitInformation.ActiveProcessLimit = processes; limits.JobMemoryLimit = (UIntPtr)memory;
    Phase(phaseFile, "job_configure");
    bool configured = SetInformationJobObject(job, 9, ref limits, (uint)Marshal.SizeOf(limits));
    if (!configured) {
      // Some managed/CI hosts reject a nested job-memory limit even though kill-on-close and
      // active-process limits are available. Preserve the process-tree boundary and fail only if
      // that minimum native contract cannot be installed.
      limits.BasicLimitInformation.LimitFlags = 0x2000u | 0x8u; limits.JobMemoryLimit = UIntPtr.Zero;
      Check(SetInformationJobObject(job, 9, ref limits, (uint)Marshal.SizeOf(limits)), "SetInformationJobObject");
    }
    var si = new STARTUPINFO(); si.cb = Marshal.SizeOf(si); si.dwFlags = 0x100; si.hStdInput = GetStdHandle(-10); si.hStdOutput = GetStdHandle(-11); si.hStdError = GetStdHandle(-12);
    PROCESS_INFORMATION pi; var line = new StringBuilder("\"" + shell + "\" /d /s /c \"" + command.Replace("\"", "\\\"") + "\"");
    Phase(phaseFile, "child_create");
    Check(CreateProcessW(shell, line, IntPtr.Zero, IntPtr.Zero, true, 0x4u | 0x400u, IntPtr.Zero, null, ref si, out pi), "CreateProcessW");
    bool childExited = false;
    try {
      Phase(phaseFile, "child_assign");
      Check(AssignProcessToJobObject(job, pi.hProcess), "AssignProcessToJobObject");
      Phase(phaseFile, "child_resume");
      uint resumed = ResumeThread(pi.hThread);
      if (resumed == 0xffffffffu) throw new Win32Exception(Marshal.GetLastWin32Error(), "ResumeThread");
      if (resumed != 1u) throw new InvalidOperationException("ResumeThread returned an unexpected previous suspend count");
      Phase(phaseFile, "child_wait");
      uint completed = WaitForSingleObject(pi.hProcess, waitMilliseconds);
      if (completed == WAIT_TIMEOUT) {
        Phase(phaseFile, "child_wait_timed_out");
        StopChild(pi.hProcess);
        throw new TimeoutException("Windows Job child exceeded its bounded wait");
      }
      Check(completed == WAIT_OBJECT_0, "WaitForSingleObject child");
      uint code; Check(GetExitCodeProcess(pi.hProcess, out code), "GetExitCodeProcess");
      childExited = true; Phase(phaseFile, "child_exited"); return unchecked((int)code);
    }
    catch {
      if (!childExited) StopChild(pi.hProcess);
      throw;
    }
    finally {
      if (childExited) Phase(phaseFile, "cleanup");
      CloseHandle(pi.hThread); CloseHandle(pi.hProcess); CloseHandle(job);
      if (childExited) Phase(phaseFile, "complete");
    }
  }
}
'@
function Set-AikitPhase([string]$phase) {
  if (![String]::IsNullOrWhiteSpace($phaseFile)) {
    try { [IO.File]::WriteAllText($phaseFile, $phase, [Text.Encoding]::ASCII) } catch { }
  }
}
$phaseFile = $env:AIKIT_JOB_PHASE_FILE
Set-AikitPhase 'powershell_started'
Add-Type -TypeDefinition $src -Language CSharp
Set-AikitPhase 'native_type_loaded'
$command = $env:AIKIT_JOB_COMMAND; $cwd = $env:AIKIT_JOB_WORKDIR; $shell = $env:AIKIT_JOB_SHELL
$processes = [uint32]$env:AIKIT_JOB_PROCESS_LIMIT; $memory = [uint64]$env:AIKIT_JOB_MEMORY_LIMIT
$environmentFile = $env:AIKIT_JOB_ENV_FILE
$waitMilliseconds = [uint32]::MaxValue
$requestedWait = [uint32]0
if ([uint32]::TryParse($env:AIKIT_JOB_WAIT_MS, [ref]$requestedWait) -and $requestedWait -gt 0) {
  $waitMilliseconds = $requestedWait
}
$env:AIKIT_JOB_COMMAND = $null; $env:AIKIT_JOB_WORKDIR = $null; $env:AIKIT_JOB_SHELL = $null
$env:AIKIT_JOB_PROCESS_LIMIT = $null; $env:AIKIT_JOB_MEMORY_LIMIT = $null; $env:AIKIT_JOB_ENV_FILE = $null
$env:AIKIT_JOB_PHASE_FILE = $null; $env:AIKIT_JOB_WAIT_MS = $null
$env:TEMP = $null; $env:TMP = $null
if (![String]::IsNullOrWhiteSpace($environmentFile)) {
  Get-Content -LiteralPath $environmentFile -Encoding ASCII | ForEach-Object {
    $parts = $_ -split "`t", 2
    if ($parts.Length -ne 2) { throw "invalid AIKit workload environment record" }
    $key = [Text.Encoding]::Unicode.GetString([Convert]::FromBase64String($parts[0]))
    $value = [Text.Encoding]::Unicode.GetString([Convert]::FromBase64String($parts[1]))
    [Environment]::SetEnvironmentVariable($key, $value, 'Process')
  }
  Remove-Item -LiteralPath $environmentFile -Force
}
Set-AikitPhase 'workload_environment_loaded'
exit [AikitJob]::Run($command, $cwd, $shell, $processes, $memory, $waitMilliseconds, $phaseFile)
"#;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64_matches_rfc4648_vectors() {
        assert_eq!(base64(b""), "");
        assert_eq!(base64(b"f"), "Zg==");
        assert_eq!(base64(b"fo"), "Zm8=");
        assert_eq!(base64(b"foo"), "Zm9v");
        assert_eq!(base64(b"foob"), "Zm9vYg==");
        assert_eq!(base64(b"fooba"), "Zm9vYmE=");
        assert_eq!(base64(b"foobar"), "Zm9vYmFy");
    }

    #[test]
    fn encode_powershell_is_utf16le_base64() {
        // "exit" in UTF-16LE is 65 00 78 00 69 00 74 00 — the encoding PowerShell's
        // -EncodedCommand requires.
        assert_eq!(encode_powershell("exit"), "ZQB4AGkAdAA=");
    }

    #[test]
    fn launcher_installs_kill_on_close_process_and_memory_limits() {
        // JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE (0x2000) | ACTIVE_PROCESS (0x8) | JOB_MEMORY (0x200),
        // with a documented fallback that keeps the process-tree boundary when a nested host
        // rejects the memory limit.
        assert!(WINDOWS_JOB_LAUNCHER.contains("0x2000u | 0x8u | 0x200u"));
        assert!(WINDOWS_JOB_LAUNCHER.contains("LimitFlags = 0x2000u | 0x8u;"));
        assert!(WINDOWS_JOB_LAUNCHER.contains("AssignProcessToJobObject"));
        assert!(WINDOWS_JOB_LAUNCHER.contains("CreateJobObjectW"));
        // The child starts suspended and is resumed only after job assignment.
        assert!(WINDOWS_JOB_LAUNCHER.contains("ResumeThread"));
    }

    #[test]
    fn assignment_or_resume_failure_terminates_the_suspended_child() {
        assert!(WINDOWS_JOB_LAUNCHER.contains("static extern bool TerminateProcess"));
        assert!(WINDOWS_JOB_LAUNCHER.contains("resumed == 0xffffffffu"));
        assert!(WINDOWS_JOB_LAUNCHER.contains("resumed != 1u"));
        assert!(WINDOWS_JOB_LAUNCHER.contains("TerminateProcess(process, 1u)"));
        assert!(WINDOWS_JOB_LAUNCHER.contains("WaitForSingleObject(process, TERMINATION_WAIT_MS)"));
        assert!(WINDOWS_JOB_LAUNCHER.contains("Check(waited == WAIT_OBJECT_0"));
    }

    #[test]
    fn probe_can_bound_child_wait_without_capping_normal_workloads() {
        assert!(!WINDOWS_JOB_LAUNCHER.contains("WaitForSingleObject(pi.hProcess, 0xffffffff)"));
        assert!(WINDOWS_JOB_LAUNCHER.contains("WaitForSingleObject(pi.hProcess, waitMilliseconds)"));
        assert!(WINDOWS_JOB_LAUNCHER.contains("$waitMilliseconds = [uint32]::MaxValue"));
        assert!(!WINDOWS_JOB_LAUNCHER.contains("86400000"));
        assert!(WINDOWS_JOB_LAUNCHER.contains("completed == WAIT_TIMEOUT"));
        assert!(WINDOWS_JOB_LAUNCHER.contains("child_wait_timed_out"));
        assert!(WINDOWS_JOB_LAUNCHER.contains("StopChild(pi.hProcess)"));
    }

    #[test]
    fn probe_separates_cold_compiler_child_and_cleanup_budgets() {
        assert_eq!(
            WINDOWS_JOB_PROBE_LAUNCHER_TIMEOUT,
            std::time::Duration::from_secs(60)
        );
        assert_eq!(WINDOWS_JOB_PROBE_CHILD_WAIT_MS, 5_000);
        assert!(
            WINDOWS_JOB_PROBE_LAUNCHER_TIMEOUT
                > std::time::Duration::from_millis(WINDOWS_JOB_PROBE_CHILD_WAIT_MS.into())
        );
        assert!(WINDOWS_JOB_LAUNCHER.contains("TERMINATION_WAIT_MS = 5000u"));
    }

    #[test]
    fn launcher_scrubs_job_environment_before_exec() {
        for variable in [
            "AIKIT_JOB_COMMAND",
            "AIKIT_JOB_WORKDIR",
            "AIKIT_JOB_SHELL",
            "AIKIT_JOB_PROCESS_LIMIT",
            "AIKIT_JOB_MEMORY_LIMIT",
            "AIKIT_JOB_ENV_FILE",
            "AIKIT_JOB_PHASE_FILE",
            "AIKIT_JOB_WAIT_MS",
            "TEMP",
            "TMP",
        ] {
            assert!(
                WINDOWS_JOB_LAUNCHER.contains(&format!("$env:{variable} = $null")),
                "launcher must scrub {variable} before exec"
            );
        }
        assert!(WINDOWS_JOB_LAUNCHER.contains("Get-Content -LiteralPath $environmentFile"));
        assert!(WINDOWS_JOB_LAUNCHER.contains("SetEnvironmentVariable($key, $value, 'Process')"));
        let add_type = WINDOWS_JOB_LAUNCHER.find("Add-Type").unwrap();
        let temp_scrub = WINDOWS_JOB_LAUNCHER.find("$env:TEMP = $null").unwrap();
        let workload_environment = WINDOWS_JOB_LAUNCHER
            .find("Get-Content -LiteralPath")
            .unwrap();
        assert!(add_type < temp_scrub);
        assert!(temp_scrub < workload_environment);
    }

    #[test]
    fn launcher_phase_detail_uses_only_fixed_safe_labels() {
        assert_eq!(safe_launcher_phase("child_wait\n"), "child_wait");
        assert_eq!(
            safe_launcher_phase("child_wait_timed_out"),
            "child_wait_timed_out"
        );
        assert_eq!(safe_launcher_phase("secret-command=value"), "unrecognized");
    }

    #[test]
    fn job_environment_carries_command_workdir_and_clamped_limits() {
        let lookup = |environment: &[(std::ffi::OsString, std::ffi::OsString)], key: &str| {
            environment
                .iter()
                .find(|(name, _)| name == key)
                .map(|(_, value)| value.to_string_lossy().into_owned())
                .unwrap_or_else(|| panic!("missing {key}"))
        };

        let system = WindowsSystemPaths {
            root: std::path::PathBuf::from(r"C:\Windows"),
            powershell: std::path::PathBuf::from(
                r"C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe",
            ),
            shell: std::path::PathBuf::from(r"C:\Windows\System32\cmd.exe"),
            path: std::ffi::OsString::from(
                r"C:\Windows\System32;C:\Windows;C:\Windows\System32\WindowsPowerShell\v1.0",
            ),
        };
        let environment = job_environment(
            "echo merhaba",
            std::path::PathBuf::from("/w"),
            &system,
            std::path::PathBuf::from(r"C:\private\workload.env"),
            std::path::PathBuf::from(r"C:\private\control-temp"),
            ContainmentLimits::default(),
        );
        assert_eq!(lookup(&environment, "SystemRoot"), r"C:\Windows");
        assert_eq!(lookup(&environment, "WINDIR"), r"C:\Windows");
        assert!(lookup(&environment, "PATH").contains("WindowsPowerShell"));
        assert_eq!(lookup(&environment, "TEMP"), r"C:\private\control-temp");
        assert_eq!(lookup(&environment, "TMP"), r"C:\private\control-temp");
        assert!(system
            .powershell
            .to_string_lossy()
            .ends_with("powershell.exe"));
        assert_eq!(lookup(&environment, "AIKIT_JOB_COMMAND"), "echo merhaba");
        assert_eq!(lookup(&environment, "AIKIT_JOB_WORKDIR"), "/w");
        assert!(lookup(&environment, "AIKIT_JOB_SHELL").ends_with("cmd.exe"));
        assert!(lookup(&environment, "AIKIT_JOB_ENV_FILE").ends_with("workload.env"));
        assert_eq!(lookup(&environment, "AIKIT_JOB_PROCESS_LIMIT"), "64");
        assert_eq!(lookup(&environment, "AIKIT_JOB_MEMORY_LIMIT"), "536870912");

        // Explicit limits pass through; zero clamps up to one instead of disabling the limit.
        let custom = |max: u64| {
            job_environment(
                "x",
                std::path::PathBuf::from("/w"),
                &system,
                std::path::PathBuf::from(r"C:\private\workload.env"),
                std::path::PathBuf::from(r"C:\private\control-temp"),
                ContainmentLimits {
                    max_processes: Some(max),
                    ..Default::default()
                },
            )
        };
        assert_eq!(lookup(&custom(16), "AIKIT_JOB_PROCESS_LIMIT"), "16");
        assert_eq!(lookup(&custom(0), "AIKIT_JOB_PROCESS_LIMIT"), "1");
    }

    #[cfg(target_os = "windows")]
    #[tokio::test]
    async fn native_job_probe_compiles_launcher_in_private_temp_and_completes() {
        let workspace = tempfile::tempdir().unwrap();
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(70),
            capability(Some(workspace.path())),
        )
        .await
        .expect("native Windows Job probe exceeded its bounded deadline");

        assert!(result.available, "{}", result.detail);
    }
}
