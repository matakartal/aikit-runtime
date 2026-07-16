# aikit tool containment threat model

This document defines the guarantees and limits of aikit's built-in tool security stack. The
default built-in Bash posture is `Required(Auto)`: if aikit cannot prove that a supported OS
backend is ready, it returns `PermissionDenied` before starting the command.

## Protected assets and attacker

The protected assets are host credentials and user files outside the selected workspace, host
network access, and host availability. The attacker may control a model-produced Bash command,
command arguments, filenames, and repository contents. The host application, aikit configuration,
selected workspace, Docker daemon, and aikit binary are trusted.

This stack reduces the impact of an untrusted command. It does not claim to stop a kernel exploit,
a compromised Docker daemon, a malicious host callback, or a privileged actor on the host.

## Defence-in-depth layers

1. Permissions and lifecycle hooks decide whether a tool call may run and may rewrite its input.
2. The `Sandbox` path jail opens root-directory capabilities once, then resolves every component
   descriptor-relatively with symlink following disabled for Read/Write/Edit/Grep/Glob.
3. `BashPolicy` scrubs the child environment, caps output and time, applies Unix rlimits, and kills
   the process group on timeout.
4. `ContainmentPolicy` places built-in Bash behind Seatbelt, Linux namespaces+seccomp, a Windows
   Job Object, or hardened Docker.

These layers are independent. In particular, the in-process path jail cannot contain a shell.

## Selection and fail-closed behavior

| Policy | Behavior |
|---|---|
| `Required(Auto)` | Select a successfully probed host-native backend (Seatbelt, Linux namespaces+seccomp, or Windows Job), then configured Docker; otherwise deny. |
| `Required(Native)` | Use only the successfully probed native backend for the host OS. |
| `Required(Seatbelt)` | Use only the probed macOS Seatbelt backend; deny elsewhere or on probe failure. |
| `Required(Docker)` | Use only the configured Docker backend; deny if its executable, daemon security, or immutable local image check fails. |
| `Uncontained` | Explicit opt-out. Only `BashPolicy` hardening remains; the capability report says `fail_closed: false`. |

`BuiltinTools::with_bash()` uses `Required(Auto)`. A service can configure Docker and preflight its
posture before accepting work:

```rust,no_run
use aikit_core::{BuiltinTools, ContainmentPolicy, DockerConfig, Sandbox};

# async fn configure() {
let sandbox = Sandbox::jail("/srv/aikit/workspace").unwrap();
let containment = ContainmentPolicy::required_auto().with_docker_fallback(DockerConfig::new(
    "registry.example/aikit-shell@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
));
let tools = BuiltinTools::new(sandbox).with_containment_policy(containment);
let report = tools.containment_capabilities().await;
assert!(report.selected_backend.is_some(), "{report:?}");
# }
```

The Docker image must already exist locally and contain `/bin/sh`; aikit uses `--pull=never`.
Trusted local development can deliberately call `with_uncontained_bash()`, but that is not a
security boundary. The Python and Node built-in APIs do not expose this opt-out.

## Backend guarantees

| Mechanism | Seatbelt | Linux namespace | Windows Job | Docker |
|---|---|---|---|---|
| Workspace writes | Yes | Yes | Host ACLs only | Bind mount |
| Other writes | Denied except private temp | Read-only root, private temp | Not isolated | Read-only root, tmpfs |
| Sensitive home reads | Denied outside workspace | Host root readable; real home hidden by scrubbed `HOME` but not a read boundary | Not isolated | Host home not mounted |
| Network | Denied | New network namespace | Not isolated | None |
| Descendants | Policy inherited | Namespace/filter inherited | Kill-on-close job inherited | Container inherited |
| Syscalls | No custom filter | Deny filter for privilege/escape syscalls | No syscall filter | Runtime seccomp profile |
| Resources | Timeout/output/rlimits | Timeout/output/rlimits | Process count, memory, timeout/output | CPU/memory/pids/ulimits/tmpfs |

Outer command construction is argv-safe: workspace paths, environment keys, image references, and
Docker options are separate arguments. The final model-produced command is intentionally passed as
one argument to `/bin/sh -c`; shell syntax inside that command therefore remains active. Docker
environment values are copied from the scrubbed Docker client environment and are not embedded in
its argv.

## Backend-specific limitations

### Seatbelt

- `/usr/bin/sandbox-exec` is deprecated by Apple. aikit actively compiles and exercises the exact
  profile, including a real rejected out-of-workspace write, before selecting it. A future macOS
  release may remove it; required mode will then fail closed.
- This first profile uses `allow default` plus explicit filesystem, network, Apple Events, and
  LaunchServices denials so normal developer toolchains remain usable. It is not a deny-by-default
  Mach-service or syscall policy and allows ordinary system reads outside user homes.
- Seatbelt shares the host kernel and user account. It is not a VM boundary.
- Process cleanup kills the invocation's Unix process group. A hostile child that successfully
  creates a new session can escape that lifecycle group while still inheriting Seatbelt policy.
  Use required Docker (or an external cgroup/job supervisor) when termination of every descendant
  is a hard requirement.

### Docker

- Docker is a same-kernel boundary on Linux; Docker Desktop adds its own Linux VM on macOS/Windows.
  The daemon and container runtime remain trusted infrastructure.
- The workspace is intentionally writable and is not quota-isolated as a whole. Per-file size,
  memory/swap, pids, CPU, and tmpfs are bounded, but many small workspace files can still consume
  host disk. Apply a workspace quota when that risk matters.
- Do not place privileged Unix sockets, credentials, or other host capabilities inside the mounted
  workspace. `--network=none` does not turn a deliberately mounted Unix socket into an unprivileged
  resource.
- The image is pinned by SHA-256 digest and never pulled implicitly, but image provenance and
  vulnerability management are deployment responsibilities. aikit overrides the image entrypoint
  with `/bin/sh`; approved images should also avoid declared `VOLUME`s that create extra writable
  Docker-managed storage.
- Cancellation starts an argv-safe `docker rm -f` before the dropped future returns. This is
  necessarily best-effort if the trusted Docker executable or daemon itself is unavailable.

### Linux namespace and seccomp

- Selection requires `/usr/bin/bwrap` and an active enforcement probe. User, mount, PID, IPC, UTS,
  cgroup, and network namespaces are created; the host root is read-only and the workspace is
  rebound writable.
- The seccomp filter denies selected privilege/escape syscalls and defaults to allow for developer
  tool compatibility. It is not a complete syscall allowlist or VM boundary.

### Windows Job Object

- PowerShell hosts a small P/Invoke launcher. The command process is created suspended, assigned to
  a kill-on-close Job Object with active-process and memory limits, then resumed.
- Job Objects do not isolate filesystem reads/writes, network access, registry access, or syscalls.
  The capability report marks those guarantees false. Use Docker or an external Windows sandbox
  when those boundaries are required.

## Scope and unsupported surfaces

OS containment currently covers the built-in Bash executor. Read/Write/Edit/Grep/Glob use the
in-process path jail. Arbitrary Rust `ToolExecutor` implementations and Python/Node host callbacks
run in their host process unless the application isolates them separately.

Provider metadata is a data-handling boundary, not sanitized telemetry. It may contain generated
tokens, grounding queries, URLs, and citations. Metadata-only audit sinks omit it, but canonical
run outcomes and session stores preserve it for provider fidelity; applications must protect those
stores and must not forward provider metadata to logs without their own redaction policy.

JSON memory/session files can contain prompts, tool results, explicit remembered values, and raw
provider metadata. JSONL audit is metadata-only by default, but error text, model/tool names, and
timing remain operationally sensitive; explicit `Full` mode also records tool inputs and bounded
output previews. Owner-only file mode and symlink rejection reduce accidental exposure on Unix but
do not provide encryption, tenant authentication, backup policy, or cross-process transactions.
Concurrent JSONL sinks for the same canonical path serialize appends only inside one process;
multi-process writers need external coordination.

The descriptor-relative file-tool jail supports Linux/macOS and fails closed for file operations
on Windows. The Windows Job backend is therefore a Bash process/resource boundary, not a Windows
file-tool sandbox. Docker remains the stronger documented Windows option when filesystem and
network isolation are required.
`run_bash()` remains uncontained for source compatibility; security-sensitive callers must use
`run_bash_with_containment()` or `BuiltinTools::with_bash()`.
