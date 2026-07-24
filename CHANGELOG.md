# Changelog

All notable changes to this project will be documented in this file. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and tagged versions follow
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Breaking

- New provider/runtime entry points default to strict capability validation. An unknown or
  unsupported required capability now returns a typed error instead of silently dropping the
  request option or pretending a downgrade is equivalent.
- The identified `StreamEvent` protocol supersedes `StreamDelta`. `StreamDelta` remains available
  as a deprecated compatibility input during the v0.x migration window.
- Python and Node use factory-created `McpConnection` handles for the MCP client; the old
  `McpServer` name remains only as `legacy.McpServer` through the v0.6 compatibility window.

- Rust `ObjectOptions` now includes `semantic_validator`. Struct-literal callers should add
  `semantic_validator: None` or use `..ObjectOptions::default()` when migrating to 0.2.
- Rust `RunOutcome` now records `invocation_start_message_index`. Struct-literal callers should
  set the boundary explicitly or use `..RunOutcome::default()`; legacy serialized outcomes load
  with no boundary and message-derived eval gates fail closed for them.
- `BrowserTools` and the Python/Node browser registration helpers now require an explicit external
  egress-enforcement assertion; post-navigation URL checks alone are no longer presented as an
  SSRF boundary.
- `OffPromptStore::store` is now fallible and returns `Result<String>` so OS-randomness and
  retention-limit failures cannot be hidden.
- Expired session execution leases are no longer acquired automatically. Recovery now requires an
  explicit `SessionStore::recover_expired_execution_lease` call after side-effect reconciliation.
- `SessionStore` execution-lease methods now exchange the opaque, core-exported
  `SessionExecutionLease` claim: acquire/recovery return it, while commit/release consume it. Custom
  stores that overrode the previous owner-string methods must update their signatures and persist
  the store-generated fencing token. The doc-hidden store-author API
  (`issue_for_store`, `fencing_token`, `expires_at_unix_ms`, and `into_session`) provides the
  complete construction, persistence, validation, and ownership-transfer path needed by those
  overrides.

### Added

- Versioned offline eight-provider model catalog with canonical integrity hash, tri-state model
  capabilities, separate caller override hashes, and no runtime network dependency.
- First-class OpenRouter, Groq, Mistral, and xAI adapters alongside Anthropic, OpenAI, Google, and
  DeepSeek, with strict provider identity/auth/endpoint/stream/error conformance and sanitized
  cassette coverage.
- Identified response/block stream events with monotonic sequence, `start/delta/end`, warning,
  usage, error, raw-event opt-in, and Python/Node async event views.
- Immutable scoped governance contracts, source-to-sink information-flow policy, evidence-rich
  approvals, sandbox/egress profiles, and verified skill packages with source/artifact pinning,
  typosquat/hidden-instruction checks, and separately authorized executable hooks.
- Durable event-sourced runs with checkpoints, activity/idempotency/reconciliation records,
  resume/fork/rewind/cancel, durable approvals, SQLite CAS persistence, and equivalent Python/Node
  `DurableRun` wrappers.
- Sync-only `DurableRunDriver` integration with the real agent loop: provider/tool starts commit
  before I/O, outcomes commit before advancement, completed results are reused, and ambiguous
  provider/tool/audit effects stop for reconciliation under the durable run/audit identity. It is
  CAS-backed in-process coordination with one executor invocation per validated in-process
  attempt, not an exactly-once or distributed-execution guarantee.
- Durability schema v2 prevents newly written failed or cancelled runs from retaining a running
  activity; v1 snapshots and database rows remain readable until their next write. Cooperative
  cancellation is persisted as `Cancelled` only for an unambiguous stop with no running activity.
- Persisted two-phase `RunStopped` delivery: an intent is saved before fail-closed audit delivery
  and acceptance after every sink accepts. If the terminal CAS later fails, restart reuses that
  acceptance and retries only terminal persistence; ambiguous audit effects require reconciliation.
- Exact typed terminal-audit replay binds the original run/invocation/sequence, terminal summary,
  sink and payload policy, and stable host delivery identity. Explicit `SafeToRetry` never reruns
  hooks/provider/tool work; partial replay returns to reconciliation. Legacy v1 zero-attempt
  terminal records require a synthetic attempt and matching typed attestation.
- Durable activity schedules retain input hashes instead of raw request payloads; verbatim replay
  results have a configurable 64 MiB hard ceiling and oversized post-effect results enter
  reconciliation. Repeated attaches keep the logical run id while receiving distinct audit
  invocation ids.
- Policy snapshots pinned into append-only run history, four typed HITL approval states, trusted
  expiry clocks, restart-safe resolution, and a persisted legacy-approver bridge that commits
  through `DurableStore` compare-and-swap before exposing state.
- Integrity-bound `MediaInput` message blocks across Rust, Python, and Node. Inline bytes/base64
  are size-checked and SHA-256 verified before provider dispatch; unresolved artifacts and media
  in unsupported roles fail before network I/O. Strict URL inputs also require governed resolution
  to verified bytes; Google no longer treats ordinary web URLs as Files API URIs.
- Enforced provider `CompatibilityMode`: strict preflight rejects uncataloged parameters before
  network I/O, while warn/best-effort preserve the value and emit a typed stream warning.
- Working, episodic, and semantic memory planes with provenance and compare-and-swap updates.
- Provider-neutral multimodal image/audio/transcript/realtime contracts, persisted realtime
  dedupe, cancellation, typed provider SPIs, and fallback-disabled capability-aware routing.
- Governed MCP 2025-11-25 JSON-RPC server with real stdio and Streamable HTTP listeners, bounded
  SSE replay, Origin/auth/session/version/Host/Accept enforcement, SQLite CAS persistence,
  restart-safe request dedupe, durable Tasks, and schema-drift reapproval. A2A 1.0 now has a
  bounded experimental Rust JSON-RPC/SSE listener; ACP v1 remains mapping-only.
- Canonical A2A `ListTasks` with subject+tenant isolation before filters/counts/cursor pagination,
  bounded pages, stable ordering, snapshot recovery, and shared Rust/Python/Node mapper surfaces.
- A2A artifact and direct-`Message` projection, durable SSE event replay, isolated protected
  cancellation ingress, typed delta-journal/checkpoint contracts, and a pinned official TCK
  workflow that preserves six upstream false negatives while verifying their exact causes.
- Optional fail-closed Firecracker lifecycle: immutable hash-pinned host inputs, shell-free jailer
  argv, trusted-path/version/KVM/TAP/netns checks, bounded API startup and cleanup. It is not a
  Bash backend until guest command/workspace transport and Linux escape proof exist.
- Persistent libFuzzer and chaos workflows for stream/durable/cassette mutation, forced-process
  SQLite recovery, and disposable PostgreSQL cross-connection CAS.
- Deterministic trace assertions and redacted agent/run/model/tool/checkpoint/activity spans,
  exposed through Rust, Python, and Node evaluation helpers.
- Living competitor parity matrix with exact upstream commit/license pins and evidence/status per
  capability.

- Async semantic structured-output validation across Rust, Python, and Node with explicit
  accept/retry/reject decisions, bounded repair attempts, and fail-closed callback handling.
- Exact, case-sensitive MCP tool visibility filters across Rust, Python, and Node; optional allow
  lists are applied before discovery caching, deny entries always win, and hidden tools cannot be
  advertised or executed.
- Deterministic evaluation datasets and gates over canonical outcomes, plus a keyless `aikit eval`
  command with redacted provenance reports, bounded live runs, hardened dataset loading, and
  distinct infrastructure/gate failure codes.
- Declarative permission policy (`PolicySpec`): JSON `mode` / allow / ask / deny rules with
  `Tool(glob)` patterns compile into the enforcing permission engine.
- Plan mode: agents propose a step plan; a host `PlanReviewer` approves, revises, or rejects
  before any tool executes.
- Risk scoring and smart approval: keyless `HeuristicRiskScorer` plus `SmartApprover` that
  auto-allows low-risk calls and escalates the rest to a human approver.
- Reliability rules: declarative tool ordering, prerequisites (`only_after`), use caps, and
  soft forbids — separate from security permissions.
- Off-prompt tool output: large tool results stored by reference with preview; full content
  retrieved through the canonical `retrieve_output` tool schema only when needed.
- Core examples: `policy`, `plan_mode`, `smart_approval`, and `reliability`.
- Strict policy parsing that rejects unknown fields and malformed rules, with multiline-safe glob
  matching to prevent newline-based policy bypasses.
- Isolated OpenAI-compatible endpoints for OpenRouter, Groq, Mistral, and xAI.
- Grok model discovery through `grok-*` and `xai:grok-*`, backed by `XAI_API_KEY` and a keyless
  xAI wire-contract regression test.
- Source-first `aikit` CLI with one-shot runs, canonical multi-turn chat, provider/capability
  discovery, containment doctor, text/JSON/JSONL output, stable exit codes, and shell completions.
- CI security gates: cargo-deny (RustSec advisories, license policy, duplicate/wildcard bans, and
  registry-source pinning via `deny.toml`), an independent cargo-audit run, a full-history Gitleaks
  committed-secret scan, deterministic SBOM/provenance checks, and repository-level CodeQL default
  setup rather than a duplicate workflow file.
- A required deterministic-eval CI job over the keyless `mock-1` datasets, including governed
  tool-trajectory and denied-tool cases.
- Dependabot, community-health files, Discussions, and repository metadata.

### Changed

- MCP discovery and transport now fail closed after bounded page, item, byte, cursor, or response
  limits; stale discovery caches are cleared after refresh failures and repeated cursors cannot
  loop indefinitely.
- Development manifests advance to `0.3.0-alpha.1`; release checks reject reusing an evidenced/tagged
  version for different source bytes.
- Node native bindings use napi-rs 3 with N-API 9 for the declared Node.js 18.17+ compatibility
  floor.
- Linux Python and Node artifacts now build against a digest-pinned glibc 2.28 baseline; workflow
  actions and build tools are immutable or exact-version pinned.
- Run options reject unknown top-level and nested fields, credentials reject blank values, and
  Node AbortSignal cancellation serializes native stream finalization.
- Native provider streams now enforce bounded frames, retained parser state, response lifetime,
  and complete terminal/tool-call invariants; the runtime independently caps retained run output
  and oversized custom-tool results.
- Session execution claims block automatic expired-lease takeover. Recovery is an explicit store
  operation that requires external-side-effect reconciliation before any replay or commit. Random
  per-claim fencing tokens prevent same-owner ABA, and Python/Node expose an assertion-gated atomic
  expired-lease clear that performs no model/tool work.
- Web fetches pin validated public DNS targets with proxy-free, per-hop redirect checks. Browser
  and built-in file-tool inputs, traversal, responses, and outputs have fail-closed size limits.
- SQLite and JSON session files verify descriptor identity on Unix and Windows; unsupported
  platforms fail closed when file identity cannot be proven.
- Browser tool registration now fails closed without an explicit caller assertion of external
  pre-request host/public-IP enforcement. Browser inputs and WebDriver replies are bounded, and
  WebDriver failure payloads are redacted. This changes the Rust, Python, and Node registration
  signatures.
- Updated Tokio, Regex, SQLite, and the Python FFI stack. PyO3, `pyo3-async-runtimes`, and
  `pythonize` now use the patched 0.29 line; SQLite revisions are converted explicitly at the
  signed storage boundary.

### Fixed

- A2A contexts are owner-scoped, so the same wire `context_id` can map to independent sessions in
  different tenants; message receipts are scoped by subject and tenant as well. Versioned restore
  migrates pre-scoping alpha keys and rejects inconsistent task, owner, receipt, revision, or
  sequence relationships before they can overwrite live mapper state.
- A2A foreign and unknown tasks now produce the same not-found result without disclosing runtime
  session/run ids. Unknown message-part fields are rejected, and Rust/Python/Node route semantic
  identity and message validation through the same governed `invalid_request` contract.
- Protocol validation denials now retain `invalid_request`, while missing principal/scope and
  unknown task denials preserve their correct unauthorized/forbidden/not-found error classes.
  Governance envelopes advance to protocol contract version 2 for the new denial value.
- MCP, A2A, and ACP ownership checks now bind both tenant and subject, closing cross-tenant access
  when two tenants use the same subject identifier.
- Raw durable events can no longer schedule work or replace state while a run is paused or awaiting
  reconciliation; SQLite loads validate row identity, schema, revision, and serialized history.
- Durable runtime and approvals now share one state/store/CAS-poison authority. Mismatched policy,
  tenant, agent, run, ordinary approver, stale no-op state, or a post-CAS write attempt fails closed
  before later provider/tool work can run.
- Temporal activity invocations validate queue, timeout, retry, header, identity, and input fields
  against deterministically regenerated state before execution.
- Stream encoding rejects response/block deltas before start, duplicate starts, and events after a
  terminal stop/error while retaining the legacy vector adapter.
- Inline transcription validates the real byte length and SHA-256 under a 25 MiB cap, streams the
  multipart body without cloning, and validates artifact integrity/provenance before commit.
- Timed-out blocking DNS resolutions remain under a shared eight-job concurrency bound instead of
  accumulating in Tokio's blocking pool.
- Provider cassette fixtures now ship inside the Rust core package and the extracted `.crate`
  executes its cassette tests in CI/release validation.
- Python/TypeScript declarations now reflect factory-only MCP handles and optional durable resume
  approvals instead of accepting calls that runtime rejects or rejecting valid commands.

### Documentation

- Reworked the documentation set for the 0.3 alpha parity candidate: architecture and migration
  guides, current status, SDK/CLI contracts, security boundaries, MCP/evaluation limits, release
  operations, historical labels, and repository navigation now share one consistent vocabulary.
- Refreshed the complete documentation map for A2A transport/TCK evidence, eight-module parity,
  durable worker/store boundaries, current remote-workflow status, and the distinction between
  source proof, live-provider acceptance, signing, registry ownership, and publication.

## [0.1.0] - Source preview

### Added

- A single Rust runtime with Rust, Python, and Node.js/TypeScript public surfaces.
- Native Anthropic Messages, OpenAI Responses, Google Gemini, and DeepSeek adapters with
  provider-owned reasoning replay rules.
- Governed tool execution with allow/ask/deny policies, enforcing lifecycle hooks, audit events,
  budgets, routing, subagents, sessions, and explicit memory.
- Sandboxed built-in file tools plus required macOS Seatbelt, Linux namespaces+seccomp, Windows
  Job Object, or hardened digest-pinned Docker containment for Bash.
- Typed text and structured-output APIs with explicit fidelity grades and multimodal input.
- Keyless cross-language conformance, package dry-runs, and an opt-in four-provider live-smoke
  contract.

### Release status

This is a source-first open-source preview. No public package registry or live-provider pass is
claimed. See [`docs/RELEASE.md`](docs/RELEASE.md) for the distribution policy.
