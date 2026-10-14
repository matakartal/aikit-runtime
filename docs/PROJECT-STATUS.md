# Project status

**Snapshot:** 2026-10-14
**Release state:** source-first `v0.3.0-alpha.1` candidate on `main`; not published

The five-phase parity implementation now has a substantially larger local core. This page keeps
local proof separate from evidence that needs provider credentials, external services, signing
identity, registry ownership, or another operating system.

## Implemented and keylessly verifiable

- One Rust core drives Rust, Python/PyO3, Node/napi, and the CLI.
- Eight named provider adapters exist: Anthropic, OpenAI, Google, DeepSeek, OpenRouter, Groq,
  Mistral, and xAI. Authentication, endpoint/model namespace, streaming, tool/error mapping, and
  protected parameters have keyless wire tests.
- `CapabilityState` distinguishes `supported`, `unsupported`, and `unknown`; required unknown
  capabilities fail closed. A versioned eight-provider catalog is embedded offline and user
  overrides form a separate hashed layer.
- The v2 stream protocol has event/response/block identity, monotonic ordering, usage/warning/error
  events, and a compatibility bridge from legacy `StreamDelta`.
- Provider options are preflighted against adapter catalogs: strict mode rejects unknown fields,
  cataloged type mismatches and governed nested-field drift before network I/O. Warn/best-effort
  preserve values and carry typed warnings on successful streams and typed HTTP failures.
  Integrity-bound inline media is hash/size verified; strict URL/artifact media waits for a
  governed host resolver.
- Scoped governance contracts, immutable policy hashes pinned into durable runs, information-flow
  labels, typed durable approval evidence, hardened sandbox/egress profiles, and verified skill
  packages are implemented in the core and projected into Python/Node.
- Durable runs use an append-only event log, replay-validated projections, checkpoints,
  activity/idempotency records, reconciliation, approvals, resume/fork/rewind/cancel, and SQLite
  compare-and-swap persistence. Python and Node expose the same `DurableRun` state machine.
- The real in-process agent loop can attach a Sync-only `DurableRunDriver`: provider/tool activity
  starts commit before I/O, outcomes commit before the loop advances, completed results are reused,
  and the durable run id is also the audit/runtime identity. It makes one executor invocation per
  validated in-process attempt and uses store CAS for local coordination; it is not an exactly-once
  or distributed guarantee. Ambiguous provider, tool, or audit effects require reconciliation.
  `RunStopped` persists delivery intent before audit, then acceptance after every fail-closed sink;
  a later terminal-CAS retry reuses that accepted delivery without rerunning provider/tool/audit
  work. Operator-approved audit replay is exact and typed: it is bound to the original invocation,
  sequence, terminal summary, sink configuration and stable delivery identity, and it cannot rerun
  hooks, provider or tool work. Activity inputs persist as hashes, replay outputs are size-bounded
  verbatim data, and each normal resume has a distinct audit invocation identity. Async/Exit and a
  real distributed worker remain open.
- Durability schema v3 adds `ActivityAttemptCancelled` while retaining v2's rule that a newly
  written terminal run cannot keep a running activity. v1/v2 snapshots and database rows remain
  readable and migrate to v3 on their next write. v2 and older readers cannot safely consume v3,
  so rolling upgrades require new readers before v3 writers and cannot safely downgrade after a
  v3 write. Cooperative cancellation is persisted as `Cancelled` only when it is unambiguous and
  no activity remains running; otherwise the run remains available for reconciliation.
- A feature-gated PostgreSQL store adds transactional row-lock/revision CAS, and the Temporal
  reference adapter deterministically maps activity, retry, idempotency and reconciliation state.
- Working, episodic, and semantic memory planes preserve provenance and use CAS rather than
  last-write-wins. Transcript compaction supports both the deterministic extractive note and a
  host-pluggable model-generated summary (`CompactionSummarizer` /
  `AgentCompactionSummarizer`, exposed as `compaction.summary_model` in Python/Node run options)
  whose failure always falls back to the extractive note.
- Multimodal artifacts, transcription/speech/image/realtime SPIs, persisted realtime dedupe, and
  capability-aware fallback policy are typed. Catalog-gated OpenAI image, transcription, speech,
  and WebRTC-call HTTP transports have keyless wire tests; the shipped catalog still marks
  unproven media models unsupported until live acceptance exists.
- MCP 2025-11-25 has a governed JSON-RPC dispatcher, real stdio and Streamable HTTP listeners,
  bounded SSE replay, restart-safe SQLite CAS, task/dedupe/session persistence and schema-drift
  reapproval. The pinned official `@modelcontextprotocol/sdk` client drives the Streamable HTTP
  server end to end (initialize, tools, resources, prompts, bearer auth, fail-closed negatives)
  through `scripts/mcp-sdk-conformance.sh`, including RFC 9728 protected-resource discovery: the
  optional `.well-known/oauth-protected-resource` document and the 401 `resource_metadata`
  challenge parameter are verified with the SDK's own auth helpers, while a live
  authorization-server token flow stays out of keyless scope. Expired side-effect replay evidence retires its
  connection namespace instead of reopening a duplicate-execution window. The SDK-neutral ACP
  mapper is now served over a real wire transport: `serve_acp_stdio` speaks official ACP v1
  newline-delimited JSON-RPC (initialize negotiation, governed session/new + session/prompt with
  streamed `session/update` notifications, cancel-notification handling, bounded frames and one
  active run per session) with a host prompt seam and duplex-I/O tests covering streaming,
  cancellation, tenant isolation, and fail-closed unknown methods. A2A 1.0 additionally has authenticated subject+tenant
  ListTasks filtering before bounded cursor pagination, the same mapper in Rust/Python/Node, and a
  bounded experimental JSON-RPC/SSE listener with artifact/direct-Message output and protected
  cancellation ingress. Task mutations persist caller-injected wall-clock instants projected as
  official RFC 3339 `TaskStatus.timestamp` values; `statusTimestampAfter` is an enforced inclusive
  ListTasks filter; `Task.history` is projected from durable receipts under official
  `historyLength` semantics; and completed-task artifacts publish durable
  `TaskArtifactUpdateEvent` frames before the terminal frame, surviving restart recovery (mapper
  snapshot schema v5; v4 and older snapshots migrate on read). The typed delta journal is wired
  into the transport persistence boundary: a journal-backed store persists one hash-chained typed
  delta per accepted mutation under the transport's deterministic commit identity, rotates
  checkpoints on a bounded cadence with prefix GC, and restores by replaying checkpoint + tail
  through the canonical validator. Journal backends ship in two forms — the linearizable
  in-memory reference and a durable cross-process SQLite store proven byte-identical to it by a
  differential conformance test, including restart, two-connection linearization, restore-pin GC
  safety, and tampered-row rejection. The default stores remain full-snapshot CAS, and the pinned
  official TCK retains six verified upstream false negatives.
- A distinct external-agent runtime reuses Codex, Gemini, Claude, and Grok CLI accounts without
  extracting OAuth tokens. Codex app-server and ACP transports are bounded and approval-aware;
  Gemini requires explicit vendor-terms acknowledgement. Persisted signed update sources expose
  `configure/status/sync/rollback`; AIKit-managed sync streams and verifies artifacts, stages a
  health check, atomically activates, recovers interrupted activation, and quarantines rejected
  versions. External and vendor-managed installations remain report-only. The deterministic
  discovery/signed-manifest/selection/update-state surface is projected into typed Python and
  Node bindings and byte-compared across all three languages by the `EXTERNAL_AGENTS`
  conformance module. Live sessions are projected too: `spawn_external_agent_session` /
  `ExternalAgentSession` expose spawn (shared per-kind policy and the Gemini vendor-terms gate),
  initialize/auth/models/session/prompt/cancel/approval/user-input/shutdown, and a normalized
  event stream that fails closed on subscriber lag; both bindings drive a scripted ACP agent
  through the real official-SDK client end to end in CI.
- The optional Firecracker lifecycle validates immutable host inputs, jailer/VMM versions,
  root-protected paths, KVM/TAP/netns prerequisites, API readiness and cleanup. It is not selected
  by Bash until guest command/workspace transport exists, and Linux isolation is not claimed from
  macOS tests.
- Persistent libFuzzer targets and CI cover stream lifecycle, durable replay, provider cassettes,
  and A2A journal replay (delta/checkpoint parsing plus delta application over arbitrary bytes).
  A real child process is killed after a committed SQLite checkpoint, then reopened and resumed;
  disposable PostgreSQL proves cross-connection CAS.
- Deterministic outcome and trace evals, redacted span hierarchy, security dependency/license/secret
  checks, CycloneDX SBOM, and provenance validation are present.

## Proof commands

| Proof | Command | Meaning |
|---|---|---|
| Rust workspace | `cargo test --workspace --all-features --locked --exclude aikit-py` | Core, facade, CLI and Node binding-crate behavior on this host; PyO3 is verified through `maturin` below |
| Python binding | `.venv/bin/maturin develop --manifest-path crates/aikit-py/Cargo.toml` plus the Python scenarios | Native extension linkage, runtime behavior and strict typing on this host |
| Node binding | `./scripts/build-node.sh` plus the Node scenarios | Native addon linkage, wrapper behavior and strict typing on this host |
| Strict Rust lint | `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings` | No accepted Rust warnings |
| Three-language parity | `./scripts/parity-check.sh` after both binding builds above | Registered Rust/Python/Node observable contracts are byte-identical |
| Official A2A TCK | Start `a2a_tck_sut`, then run `./scripts/a2a-conformance.sh` | Preserves the raw pinned upstream result; CI separately permits only the exact six verified false negatives |
| Official MCP SDK | `./scripts/mcp-sdk-conformance.sh` | Pinned `@modelcontextprotocol/sdk` client against the deterministic Streamable HTTP fixture |
| Provider cassettes | `cargo test -p aikit-runtime-core --test provider_cassettes --locked` | Sanitized required scenario inventory and envelope validation |
| Process chaos | `cargo test -p aikit-runtime-core --test process_chaos --locked` | A committed SQLite checkpoint survives forced child termination and resumes append-only |
| Persistent fuzz | `cargo +nightly-2026-07-15 fuzz run <target>` for the four targets under `fuzz/` | Coverage-guided mutation of stream, durability, cassette, and A2A journal-replay parsers |
| Keyless eval | `cargo run -p aikit-cli --locked -- eval evals/smoke.json` | Dataset parsing, execution and deterministic gates |
| External agents and updates | `cargo test -p aikit-runtime-core external_agent --lib --locked` plus `cargo test -p aikit-cli --locked` | Keyless process/protocol/update state machines, terms gate and CLI configuration; not a paid vendor acceptance or production signed feed |
| Security/SBOM | `./scripts/security-check.sh --all` | Advisory/license/secret/SBOM/provenance policy on this checkout |
| Source candidate | `./scripts/release-check.sh --candidate` | Version, immutable CI input, history collision and package policy |
| Live providers | `AIKIT_LIVE_SMOKE=1 AIKIT_LIVE_SMOKE_FULL=1 ./scripts/live-smoke.sh` | Fail-closed billable acceptance; requires all eight credential/model pairs |

## Last recorded remote workflow baseline

| Workflow | Result | Evidence |
|---|---|---|
| A2A conformance | Passed | [run 30202557089](https://github.com/matakartal/aikit-runtime/actions/runs/30202557089) |
| Chaos | Passed | [run 30202557088](https://github.com/matakartal/aikit-runtime/actions/runs/30202557088) |
| Security | Passed | [run 30202557113](https://github.com/matakartal/aikit-runtime/actions/runs/30202557113) |
| CodeQL main push | Passed | [run 30202556954](https://github.com/matakartal/aikit-runtime/actions/runs/30202556954) |
| General CI | Passed | [run 30202557096](https://github.com/matakartal/aikit-runtime/actions/runs/30202557096) |
| OpenSSF Scorecard | Passed | [run 30202667158](https://github.com/matakartal/aikit-runtime/actions/runs/30202667158) |

These historical runs passed for code-bearing commit
`d4d463c76d240abb9cdbf5859e482b6b789f5ba3` and include the external-agent/update and GitHub
hardening changes described above. They are not evidence for later code changes; use the repository
Actions page for the current head. The Python and Node A2A mapper scenarios assert snapshot schema
version `5`, and their public type contracts cover the durable outbox and pending-event fields.

## Not yet v1-complete

- Complete Rust-schema-generated declarations across every Python/TypeScript public type. A real
  generator exists and owns a growing region: `scripts/generate-declarations.sh` renders the
  external-agent DTOs plus the canonical `Usage`, `ProviderWarning`, `MediaInput`, and the
  `MediaInputSource` discriminated union for both languages from `schemars` schemas derived from
  the same Rust types `serde` uses on the wire, and its `--check` mode fails CI when the
  checked-in region is stale. It renders structs, string enums, and internally tagged unions, and
  derives requiredness from serde behaviour (a `#[serde(default)]` field without
  `skip_serializing_if` is always written, so it is declared required).
  `scripts/check-binding-declarations.sh` independently enforces that every binding runtime export
  carries a declaration and that the covered field sets match. The gate stays open for one honest
  reason: a schema cannot distinguish a present-as-null `Option` from an absent one, so types
  whose `Option` fields omit `skip_serializing_if` (for example `ErrorInfo` and `ModelPricing`)
  remain hand-written rather than being generated with weaker types.
- Paid live acceptance for every advertised provider/model/capability combination.
- Live-accepted media models, other provider media endpoints, and full realtime reconnect/event
  transports; the OpenAI HTTP contracts exist but are not advertised as live-proven support.
- Transparent egress enforcement for arbitrary child processes and Linux root+KVM Firecracker
  boot/escape/TAP proof; the explicit HTTP/browser broker already pins DNS and revalidates every
  redirect hop.
- PostgreSQL failover/partition proof and a real Temporal SDK worker integration; Sync-only local
  coordination does not satisfy this distributed gate.
- Remove the pinned TCK waivers only when upstream fixes land. The pinned official MCP SDK client
  suite, including RFC 9728 discovery, now runs in CI; only a live authorization-server token
  flow remains outside keyless scope.
  External agents project
  their deterministic discovery/update surface and live sessions into typed Python/Node bindings
  with a three-language conformance module; editor acceptance, vendor-policy monitoring, a
  production signed manifest source and cross-platform managed-update rehearsal remain.
- Longer fuzz/chaos campaigns and multi-platform signing proof.
- crates.io and npm first-release ownership bootstraps, trusted-publisher authority, PyPI
  ownership, published packages and rollback rehearsal.

No missing external gate is converted into a synthetic pass. See
[`PARITY-MATRIX.md`](PARITY-MATRIX.md) for row-level status and exact upstream pins.
