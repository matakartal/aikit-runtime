use super::{
    ActiveContainmentBackend, BackendCapability, ContainmentGuarantees, ContainmentLimits,
    PreparedCommand,
};
use crate::error::{AikitError, Result};
use std::path::Path;

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
        let mut prepared = match prepare("exit /b 0", workdir, ContainmentLimits::default()) {
            Ok(prepared) => prepared,
            Err(error) => {
                return BackendCapability::unavailable(
                    ActiveContainmentBackend::WindowsJob,
                    ContainmentGuarantees::windows_job(),
                    error.to_string(),
                )
            }
        };
        prepared.command.stdin(std::process::Stdio::null());
        prepared.command.stdout(std::process::Stdio::null());
        prepared.command.stderr(std::process::Stdio::piped());
        prepared.command.kill_on_drop(true);
        prepared
            .command
            .envs(prepared.environment_overrides.clone());
        match tokio::time::timeout(std::time::Duration::from_secs(15), prepared.command.output()).await {
            Ok(Ok(output)) if output.status.success() => BackendCapability::available(
                ActiveContainmentBackend::WindowsJob,
                ContainmentGuarantees::windows_job(),
                "suspended child assignment to kill-on-close Windows Job succeeded; process limit enforced, job-memory limit is host-dependent, filesystem/network are not isolated",
            ),
            Ok(Ok(output)) => BackendCapability::unavailable(
                ActiveContainmentBackend::WindowsJob,
                ContainmentGuarantees::windows_job(),
                format!("Windows Job probe failed: {}", String::from_utf8_lossy(&output.stderr).trim()),
            ),
            Ok(Err(error)) => BackendCapability::unavailable(
                ActiveContainmentBackend::WindowsJob,
                ContainmentGuarantees::windows_job(),
                format!("Windows Job probe could not start: {error}"),
            ),
            Err(_) => BackendCapability::unavailable(
                ActiveContainmentBackend::WindowsJob,
                ContainmentGuarantees::windows_job(),
                "Windows Job probe timed out",
            ),
        }
    }
}

pub(super) fn prepare(
    command: &str,
    workdir: &Path,
    limits: ContainmentLimits,
) -> Result<PreparedCommand> {
    #[cfg(not(target_os = "windows"))]
    {
        let _ = (command, workdir, limits);
        Err(AikitError::Sandbox(
            "Windows Job containment is unavailable on this platform".into(),
        ))
    }
    #[cfg(target_os = "windows")]
    {
        use std::ffi::OsString;
        use tokio::process::Command;

        let workspace = std::fs::canonicalize(workdir).map_err(|error| {
            AikitError::Sandbox(format!("cannot canonicalize Windows workspace: {error}"))
        })?;
        let script = encode_powershell(WINDOWS_JOB_LAUNCHER);
        let mut cmd = Command::new("powershell.exe");
        cmd.args([
            "-NoLogo",
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-EncodedCommand",
            &script,
        ]);
        let shell = std::env::var_os("SystemRoot")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| std::path::PathBuf::from(r"C:\Windows"))
            .join("System32")
            .join("cmd.exe");
        Ok(PreparedCommand {
            command: cmd,
            backend: ActiveContainmentBackend::WindowsJob,
            environment_overrides: job_environment(command, workspace, shell, limits),
            cleanup: None,
            artifacts: Vec::new(),
        })
    }
}

/// The environment contract between `prepare` and the PowerShell launcher: the untrusted command,
/// resolved workdir/shell, and clamped limits travel as `AIKIT_JOB_*` variables (which the
/// launcher scrubs before exec). Pure so the contract is unit-testable on any platform.
#[cfg(any(target_os = "windows", test))]
fn job_environment(
    command: &str,
    workspace: std::path::PathBuf,
    shell: std::path::PathBuf,
    limits: ContainmentLimits,
) -> Vec<(std::ffi::OsString, std::ffi::OsString)> {
    use std::ffi::OsString;

    let process_limit = limits.max_processes.unwrap_or(64).clamp(1, u32::MAX as u64);
    let memory_limit = 512_u64 << 20;
    vec![
        (OsString::from("AIKIT_JOB_COMMAND"), OsString::from(command)),
        (
            OsString::from("AIKIT_JOB_WORKDIR"),
            workspace.into_os_string(),
        ),
        (OsString::from("AIKIT_JOB_SHELL"), shell.into_os_string()),
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
using System.Runtime.InteropServices;
using System.Text;
public static class AikitJob {
  [StructLayout(LayoutKind.Sequential, CharSet=CharSet.Unicode)] public struct STARTUPINFO { public int cb; public string lpReserved; public string lpDesktop; public string lpTitle; public int dwX; public int dwY; public int dwXSize; public int dwYSize; public int dwXCountChars; public int dwYCountChars; public int dwFillAttribute; public int dwFlags; public short wShowWindow; public short cbReserved2; public IntPtr lpReserved2; public IntPtr hStdInput; public IntPtr hStdOutput; public IntPtr hStdError; }
  [StructLayout(LayoutKind.Sequential)] public struct PROCESS_INFORMATION { public IntPtr hProcess; public IntPtr hThread; public int dwProcessId; public int dwThreadId; }
  [StructLayout(LayoutKind.Sequential)] public struct IO_COUNTERS { public ulong ReadOperationCount, WriteOperationCount, OtherOperationCount, ReadTransferCount, WriteTransferCount, OtherTransferCount; }
  [StructLayout(LayoutKind.Sequential)] public struct BASIC_LIMITS { public long PerProcessUserTimeLimit, PerJobUserTimeLimit; public uint LimitFlags; public UIntPtr MinimumWorkingSetSize, MaximumWorkingSetSize; public uint ActiveProcessLimit; public UIntPtr Affinity; public uint PriorityClass, SchedulingClass; }
  [StructLayout(LayoutKind.Sequential)] public struct EXTENDED_LIMITS { public BASIC_LIMITS BasicLimitInformation; public IO_COUNTERS IoInfo; public UIntPtr ProcessMemoryLimit, JobMemoryLimit, PeakProcessMemoryUsed, PeakJobMemoryUsed; }
  [DllImport("kernel32.dll", CharSet=CharSet.Unicode, SetLastError=true)] static extern bool CreateProcessW(string app, StringBuilder cmd, IntPtr pa, IntPtr ta, bool inherit, uint flags, IntPtr env, string cwd, ref STARTUPINFO si, out PROCESS_INFORMATION pi);
  [DllImport("kernel32.dll", SetLastError=true)] static extern IntPtr CreateJobObjectW(IntPtr attr, string name);
  [DllImport("kernel32.dll", SetLastError=true)] static extern bool SetInformationJobObject(IntPtr job, int info, ref EXTENDED_LIMITS data, uint len);
  [DllImport("kernel32.dll", SetLastError=true)] static extern bool AssignProcessToJobObject(IntPtr job, IntPtr process);
  [DllImport("kernel32.dll")] static extern uint ResumeThread(IntPtr thread);
  [DllImport("kernel32.dll")] static extern uint WaitForSingleObject(IntPtr handle, uint ms);
  [DllImport("kernel32.dll")] static extern bool GetExitCodeProcess(IntPtr process, out uint code);
  [DllImport("kernel32.dll")] static extern bool CloseHandle(IntPtr handle);
  [DllImport("kernel32.dll")] static extern IntPtr GetStdHandle(int which);
  static void Check(bool ok, string op) { if (!ok) { int code = Marshal.GetLastWin32Error(); throw new Win32Exception(code, op + " (Win32 " + code + ")"); } }
  public static int Run(string command, string cwd, string shell, uint processes, ulong memory) {
    if (String.IsNullOrWhiteSpace(cwd)) cwd = Environment.CurrentDirectory;
    if (String.IsNullOrWhiteSpace(shell)) shell = @"C:\Windows\System32\cmd.exe";
    if (cwd.StartsWith(@"\\?\UNC\", StringComparison.OrdinalIgnoreCase)) cwd = @"\\" + cwd.Substring(8);
    else if (cwd.StartsWith(@"\\?\", StringComparison.OrdinalIgnoreCase)) cwd = cwd.Substring(4);
    Environment.CurrentDirectory = cwd;
    IntPtr job = CreateJobObjectW(IntPtr.Zero, null); if (job == IntPtr.Zero) throw new Win32Exception();
    var limits = new EXTENDED_LIMITS(); limits.BasicLimitInformation.LimitFlags = 0x2000u | 0x8u | 0x200u; limits.BasicLimitInformation.ActiveProcessLimit = processes; limits.JobMemoryLimit = (UIntPtr)memory;
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
    Check(CreateProcessW(shell, line, IntPtr.Zero, IntPtr.Zero, true, 0x4u | 0x400u, IntPtr.Zero, null, ref si, out pi), "CreateProcessW");
    try { Check(AssignProcessToJobObject(job, pi.hProcess), "AssignProcessToJobObject"); ResumeThread(pi.hThread); WaitForSingleObject(pi.hProcess, 0xffffffff); uint code; GetExitCodeProcess(pi.hProcess, out code); return unchecked((int)code); }
    finally { CloseHandle(pi.hThread); CloseHandle(pi.hProcess); CloseHandle(job); }
  }
}
'@
Add-Type -TypeDefinition $src -Language CSharp
$command = $env:AIKIT_JOB_COMMAND; $cwd = $env:AIKIT_JOB_WORKDIR; $shell = $env:AIKIT_JOB_SHELL
$processes = [uint32]$env:AIKIT_JOB_PROCESS_LIMIT; $memory = [uint64]$env:AIKIT_JOB_MEMORY_LIMIT
$env:AIKIT_JOB_COMMAND = $null; $env:AIKIT_JOB_WORKDIR = $null; $env:AIKIT_JOB_SHELL = $null
exit [AikitJob]::Run($command, $cwd, $shell, $processes, $memory)
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
    fn launcher_scrubs_job_environment_before_exec() {
        for variable in ["AIKIT_JOB_COMMAND", "AIKIT_JOB_WORKDIR", "AIKIT_JOB_SHELL"] {
            assert!(
                WINDOWS_JOB_LAUNCHER.contains(&format!("$env:{variable} = $null")),
                "launcher must scrub {variable} before exec"
            );
        }
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

        let environment = job_environment(
            "echo merhaba",
            std::path::PathBuf::from("/w"),
            std::path::PathBuf::from(r"C:\Windows\System32\cmd.exe"),
            ContainmentLimits::default(),
        );
        assert_eq!(lookup(&environment, "AIKIT_JOB_COMMAND"), "echo merhaba");
        assert_eq!(lookup(&environment, "AIKIT_JOB_WORKDIR"), "/w");
        assert!(lookup(&environment, "AIKIT_JOB_SHELL").ends_with("cmd.exe"));
        assert_eq!(lookup(&environment, "AIKIT_JOB_PROCESS_LIMIT"), "64");
        assert_eq!(lookup(&environment, "AIKIT_JOB_MEMORY_LIMIT"), "536870912");

        // Explicit limits pass through; zero clamps up to one instead of disabling the limit.
        let custom = |max: u64| {
            job_environment(
                "x",
                std::path::PathBuf::from("/w"),
                std::path::PathBuf::from("cmd.exe"),
                ContainmentLimits {
                    max_processes: Some(max),
                    ..Default::default()
                },
            )
        };
        assert_eq!(lookup(&custom(16), "AIKIT_JOB_PROCESS_LIMIT"), "16");
        assert_eq!(lookup(&custom(0), "AIKIT_JOB_PROCESS_LIMIT"), "1");
    }
}
