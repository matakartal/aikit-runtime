# aikit tool containment threat model

This document defines the guarantees and limits of aikit's built-in tool security stack. The
default built-in Bash posture is `Required(Auto)`: if aikit cannot prove that a supported OS
backend is ready, it returns `PermissionDenied` before starting the command.

Related: [Security policy](../SECURITY.md) · [Feature reference](FEATURES.md) · [Documentation index](README.md)

## Protected assets and attacker

The protected assets are host credentials and user files outside the selected workspace, host
network access, host availability, stored prompts/tool results/provider metadata, and the integrity
of permission/evaluation decisions. The attacker may control a model-produced Bash command,
command arguments, filenames, repository contents, MCP discovery/results/cursors, provider stream
frames, and structured-output candidates. The host application, aikit configuration, selected
workspace, Docker daemon, and aikit binary are trusted.

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
| Resources | Timeout/output/rlimits | Timeout/output/rlimits | Process count, optional job memory, timeout/output | CPU/memory/pids/ulimits/tmpfs |

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
  a kill-on-close Job Object with an active-process limit, then resumed. A job-memory limit is used
  when the host allows it; nested managed CI jobs may reject that optional flag.
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

Provider option compatibility warnings never contain option values, but parameter names and model
ids remain operational metadata. Strict mode rejects unknown parameters before network I/O;
warn/best-effort forward them unchanged and therefore require the caller to trust the provider's
new wire semantics. Typed provider failures preserve this warning evidence even when transport or
HTTP startup fails. Neither mode can upgrade an unknown capability to supported.

Integrity-bound media is guaranteed only after AIKit has the bytes. Inline bytes/base64 are
size/hash verified. Strict URL and artifact references fail before provider dispatch until the host
resolves them through an enforcing egress/artifact boundary and returns verified bytes. Allowing a
provider to fetch a mutable URL would not preserve the recorded SHA-256 identity.

JSON memory/session files can contain prompts, tool results, explicit remembered values, and raw
provider metadata. JSONL audit is metadata-only by default, but error text, model/tool names, and
timing remain operationally sensitive; explicit `Full` mode also records tool inputs and bounded
output previews. Owner-only file mode and symlink rejection reduce accidental exposure on Unix but
do not provide encryption, tenant authentication, backup policy, or cross-process transactions.
Concurrent JSONL sinks for the same canonical path serialize appends only inside one process;
multi-process writers need external coordination.

Session execution leases prevent concurrent model/tool work but cannot prove whether a crashed
owner completed an external side effect before losing its final session commit. Expiry is therefore
not permission to replay: normal execution rejects active, expired, and malformed lease state.
Only the explicit store recovery primitive can transfer a parseable expired lease, and it never
runs work itself. Operators must verify the former owner is stopped, reconcile external systems,
and preserve provider/tool idempotency before committing or retrying. Owner strings are diagnostic,
not fences: each lease carries a store-generated random token, and commit/release require the exact
current token to prevent same-owner ABA. Binding recovery atomically clears an expired lease only
after an explicit reconciliation assertion, without executing work or creating a second-write
crash window. JSON remains process-local; SQLite provides the transactional cross-process form.
File replacement/open checks compare stable device/inode identity on Unix and volume/file-index
identity on Windows; an identity lookup failure is an error rather than permission to continue.

Durable governance binds policy snapshot hash, tenant, agent, and run id into one sealed value in
the append-only event log and approval records. Restart attachment and every authorization verify
the same binding. Legacy hash-only history remains readable but cannot silently satisfy a new
full-binding authorization. The in-process binding registry is capped at 4,096 entries and fails
closed at capacity. Only terminal runs can retire their exact binding; retirement invalidates stale
clones and reusable grants before releasing the slot, and terminal runs cannot be attached again.

Web fetches validate each HTTPS redirect target against the exact host allowlist before following
it, reject mixed/private/non-routable DNS answers, and pin the checked public addresses into a
proxy-free request. WebDriver revalidates the current URL after navigation, click, type, and
snapshot, but the browser—not aikit—performs its DNS and network requests. A redirect or DNS rebind
may therefore already have sent a request before WebDriver reports the committed URL. Browser tool
construction and Python/Node registration consequently fail closed unless the caller explicitly
asserts that a network proxy, BiDi request interceptor, or equivalent boundary already enforces the
same exact hostname allowlist and denies private/local/non-routable IPs before every request. The
assertion does not configure or verify that boundary; a false assertion restores the SSRF risk.
Postcondition URL checks are retained only as defense in depth. WebDriver responses and browser
inputs are bounded, and protocol failure bodies are not reflected into tool errors.

### MCP servers

MCP stdio and HTTP peers are untrusted protocol endpoints. Aikit caps one response/stdio line at
4 MiB before JSON decoding and bounds each discovery operation to 128 pages, 10,000 incoming items,
8 MiB of serialized items, 4 KiB per cursor, and 64 KiB cumulative cursor data. Repeated cursors,
invalid/duplicate/bidirectional-control tool names, malformed payloads, and stale refresh state fail
closed. These limits reduce memory/loop/log-spoofing risk; they do not make the remote tool safe.

Exact allow/deny filtering happens before discovery-cache retention and again immediately before
execution. Deny wins. Every allowed MCP call still passes the normal schema/governance/tool-result
pipeline. Applications must separately decide whether the remote server, its credentials, and its
side effects are acceptable.

Inbound server state is also attacker-controlled pressure. Tasks, receipts, HTTP sessions, replay
events, result bytes/items/depth, and TTL are bounded and old terminal state is collected. Schema
drift approval is followed by validation against the newly approved schema. Cancellation is not
committed as complete before the host confirms the underlying operation stopped; timeout or an
ambiguous external effect remains fail-closed for reconciliation.

A completed side-effect receipt is replay evidence, not disposable cache. When its retention
window expires, AIKit retires that connection's request-id/dedupe namespace and rejects later
requests on it as reconciliation-required. A caller must establish a fresh connection/session
identity before submitting new work; this bounds retained receipts without turning expiry into an
unsafe duplicate-execution window.

### A2A peers

A2A JSON-RPC/SSE callers are untrusted protocol peers. The listener bounds request bodies,
responses, SSE replay, pagination, artifacts, and task state; authenticated subject and tenant
filtering occurs before totals and cursors. Content-bound idempotency rejects reuse of one message
id with different content instead of silently executing a second effect.

The protected cancellation ingress assumes a private or mutually authenticated network boundary.
Direct exposure to arbitrary internet clients would require caller identity and quota enforcement
before aikit accepts headers/body bytes. Artifact media and base64 remain size/type validated, but
the host still owns malware scanning, retention, and access policy.

Transport persistence currently uses full-snapshot compare-and-swap. The typed delta journal is a
tested contract, not yet the production hot path; it must not be described as deployed distributed
durability or exactly-once execution.

### Firecracker

The optional Firecracker lifecycle is a Linux deployment boundary, not a guarantee established by
macOS unit tests. It requires immutable hash-pinned kernel/rootfs/VMM/jailer inputs, trusted path
ownership, KVM, TAP/netns prerequisites, and a bounded API startup/configuration sequence. The VMM
child and jail staging share one supervisor; cleanup waits for child exit and deliberately retains
the staging directory on irrecoverable reap failure to avoid reusing a live jail path. Guest
command/workspace transport, production resource quotas, Linux root+KVM boot/escape tests, and
network egress enforcement remain deployment gates before it can back the built-in Bash tool.

### Semantic validators and evaluation

Semantic structured-output validators are host callbacks outside Bash containment. The core calls
them only after JSON-Schema validation, caps retries and reasons, catches callback errors/panics,
and does not automatically copy candidate values into audit/error payloads. It does not impose a
callback timeout; the host must bound validators that can block or perform I/O. Validators should
be pure/idempotent because cancellation or a caller retry can present the same value again.

Deterministic evaluation reports omit model output and provider metadata, but dataset/case names,
model ids, usage, gate types, and bounded error categories can still be operationally sensitive.
Dataset files must be treated as code: the CLI rejects symlinks/special files and silently billable
models, but `--allow-live` is an explicit operator authorization for network/cost within the stated
aggregate limits.

The descriptor-relative file-tool jail supports Linux/macOS and fails closed for file operations
on Windows. The Windows Job backend is therefore a Bash process/resource boundary, not a Windows
file-tool sandbox. Docker remains the stronger documented Windows option when filesystem and
network isolation are required.
`run_bash()` remains uncontained for source compatibility; security-sensitive callers must use
`run_bash_with_containment()` or `BuiltinTools::with_bash()`.
