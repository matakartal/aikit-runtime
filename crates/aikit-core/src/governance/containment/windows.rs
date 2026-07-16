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
        match tokio::time::timeout(std::time::Duration::from_secs(5), prepared.command.output()).await {
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
        let process_limit = limits.max_processes.unwrap_or(64).clamp(1, u32::MAX as u64);
        let memory_limit = 512_u64 << 20;
        Ok(PreparedCommand {
            command: cmd,
            backend: ActiveContainmentBackend::WindowsJob,
            environment_overrides: vec![
                (OsString::from("AIKIT_JOB_COMMAND"), OsString::from(command)),
                (
                    OsString::from("AIKIT_JOB_WORKDIR"),
                    workspace.into_os_string(),
                ),
                (
                    OsString::from("AIKIT_JOB_PROCESS_LIMIT"),
                    OsString::from(process_limit.to_string()),
                ),
                (
                    OsString::from("AIKIT_JOB_MEMORY_LIMIT"),
                    OsString::from(memory_limit.to_string()),
                ),
            ],
            cleanup: None,
            artifacts: Vec::new(),
        })
    }
}

#[cfg(target_os = "windows")]
fn encode_powershell(script: &str) -> String {
    let bytes: Vec<u8> = script.encode_utf16().flat_map(u16::to_le_bytes).collect();
    base64(&bytes)
}

#[cfg(target_os = "windows")]
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

#[cfg(target_os = "windows")]
const WINDOWS_JOB_LAUNCHER: &str = r#"
$src = @'
using System;
using System.ComponentModel;
using System.Runtime.InteropServices;
public static class AikitJob {
  [StructLayout(LayoutKind.Sequential, CharSet=CharSet.Unicode)] public struct STARTUPINFO { public int cb; public string lpReserved; public string lpDesktop; public string lpTitle; public int dwX; public int dwY; public int dwXSize; public int dwYSize; public int dwXCountChars; public int dwYCountChars; public int dwFillAttribute; public int dwFlags; public short wShowWindow; public short cbReserved2; public IntPtr lpReserved2; public IntPtr hStdInput; public IntPtr hStdOutput; public IntPtr hStdError; }
  [StructLayout(LayoutKind.Sequential)] public struct PROCESS_INFORMATION { public IntPtr hProcess; public IntPtr hThread; public int dwProcessId; public int dwThreadId; }
  [StructLayout(LayoutKind.Sequential)] public struct IO_COUNTERS { public ulong ReadOperationCount, WriteOperationCount, OtherOperationCount, ReadTransferCount, WriteTransferCount, OtherTransferCount; }
  [StructLayout(LayoutKind.Sequential)] public struct BASIC_LIMITS { public long PerProcessUserTimeLimit, PerJobUserTimeLimit; public uint LimitFlags; public UIntPtr MinimumWorkingSetSize, MaximumWorkingSetSize; public uint ActiveProcessLimit; public UIntPtr Affinity; public uint PriorityClass, SchedulingClass; }
  [StructLayout(LayoutKind.Sequential)] public struct EXTENDED_LIMITS { public BASIC_LIMITS BasicLimitInformation; public IO_COUNTERS IoInfo; public UIntPtr ProcessMemoryLimit, JobMemoryLimit, PeakProcessMemoryUsed, PeakJobMemoryUsed; }
  [DllImport("kernel32.dll", CharSet=CharSet.Unicode, SetLastError=true)] static extern bool CreateProcessW(string app, string cmd, IntPtr pa, IntPtr ta, bool inherit, uint flags, IntPtr env, string cwd, ref STARTUPINFO si, out PROCESS_INFORMATION pi);
  [DllImport("kernel32.dll", SetLastError=true)] static extern IntPtr CreateJobObjectW(IntPtr attr, string name);
  [DllImport("kernel32.dll", SetLastError=true)] static extern bool SetInformationJobObject(IntPtr job, int info, ref EXTENDED_LIMITS data, uint len);
  [DllImport("kernel32.dll", SetLastError=true)] static extern bool AssignProcessToJobObject(IntPtr job, IntPtr process);
  [DllImport("kernel32.dll")] static extern uint ResumeThread(IntPtr thread);
  [DllImport("kernel32.dll")] static extern uint WaitForSingleObject(IntPtr handle, uint ms);
  [DllImport("kernel32.dll")] static extern bool GetExitCodeProcess(IntPtr process, out uint code);
  [DllImport("kernel32.dll")] static extern bool CloseHandle(IntPtr handle);
  [DllImport("kernel32.dll")] static extern IntPtr GetStdHandle(int which);
  static void Check(bool ok, string op) { if (!ok) { int code = Marshal.GetLastWin32Error(); throw new Win32Exception(code, op + " (Win32 " + code + ")"); } }
  public static int Run(string command, string cwd, uint processes, ulong memory) {
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
    PROCESS_INFORMATION pi; string line = Environment.ExpandEnvironmentVariables("%ComSpec%") + " /d /s /c \"" + command.Replace("\"", "\\\"") + "\"";
    Check(CreateProcessW(null, line, IntPtr.Zero, IntPtr.Zero, true, 0x4u | 0x400u, IntPtr.Zero, cwd, ref si, out pi), "CreateProcessW");
    try { Check(AssignProcessToJobObject(job, pi.hProcess), "AssignProcessToJobObject"); ResumeThread(pi.hThread); WaitForSingleObject(pi.hProcess, 0xffffffff); uint code; GetExitCodeProcess(pi.hProcess, out code); return unchecked((int)code); }
    finally { CloseHandle(pi.hThread); CloseHandle(pi.hProcess); CloseHandle(job); }
  }
}
'@
Add-Type -TypeDefinition $src -Language CSharp
$command = $env:AIKIT_JOB_COMMAND; $cwd = $env:AIKIT_JOB_WORKDIR
$processes = [uint32]$env:AIKIT_JOB_PROCESS_LIMIT; $memory = [uint64]$env:AIKIT_JOB_MEMORY_LIMIT
$env:AIKIT_JOB_COMMAND = $null; $env:AIKIT_JOB_WORKDIR = $null
exit [AikitJob]::Run($command, $cwd, $processes, $memory)
"#;
