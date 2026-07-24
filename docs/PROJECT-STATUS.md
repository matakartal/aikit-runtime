# Project status

**Snapshot:** 2026-07-24
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
- Durability schema v2 prevents newly written failed or cancelled runs from retaining a running
  activity. Version 1 snapshots and database rows remain readable and are upgraded on their next
  write. Cooperative cancellation is persisted as `Cancelled` only when it is unambiguous and no
  activity remains running; otherwise the run remains available for reconciliation.
- A feature-gated PostgreSQL store adds transactional row-lock/revision CAS, and the Temporal
  reference adapter deterministically maps activity, retry, idempotency and reconciliation state.
- Working, episodic, and semantic memory planes preserve provenance and use CAS rather than
  last-write-wins.
- Multimodal artifacts, transcription/speech/image/realtime SPIs, persisted realtime dedupe, and
  capability-aware fallback policy are typed. Catalog-gated OpenAI image, transcription, speech,
  and WebRTC-call HTTP transports have keyless wire tests; the shipped catalog still marks
  unproven media models unsupported until live acceptance exists.
- MCP 2025-11-25 has a governed JSON-RPC dispatcher, real stdio and Streamable HTTP listeners,
  bounded SSE replay, restart-safe SQLite CAS, task/dedupe/session persistence and schema-drift
  reapproval. Expired side-effect replay evidence retires its connection namespace instead of
  reopening a duplicate-execution window. A2A 1.0 additionally has authenticated subject+tenant
  ListTasks filtering before bounded cursor pagination, the same mapper in Rust/Python/Node, and a
  bounded experimental JSON-RPC/SSE listener with artifact/direct-Message output and protected
  cancellation ingress. Complete timestamp/history and artifact-update coverage remain open. A
  typed delta-journal contract exists but is not wired into the transport hot path, and the pinned
  official TCK retains six verified upstream false negatives; ACP v1 remains mapping-only.
- The optional Firecracker lifecycle validates immutable host inputs, jailer/VMM versions,
  root-protected paths, KVM/TAP/netns prerequisites, API readiness and cleanup. It is not selected
  by Bash until guest command/workspace transport exists, and Linux isolation is not claimed from
  macOS tests.
- Persistent libFuzzer targets and CI cover stream lifecycle, durable replay and provider
  cassettes. A real child process is killed after a committed SQLite checkpoint, then reopened and
  resumed; disposable PostgreSQL proves cross-connection CAS.
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
| Provider cassettes | `cargo test -p aikit-runtime-core --test provider_cassettes --locked` | Sanitized required scenario inventory and envelope validation |
| Process chaos | `cargo test -p aikit-runtime-core --test process_chaos --locked` | A committed SQLite checkpoint survives forced child termination and resumes append-only |
| Persistent fuzz | `cargo +nightly-2026-07-15 fuzz run <target>` for the three targets under `fuzz/` | Coverage-guided mutation of stream, durability and cassette parsers |
| Keyless eval | `cargo run -p aikit-cli --locked -- eval evals/smoke.json` | Dataset parsing, execution and deterministic gates |
| Security/SBOM | `./scripts/security-check.sh --all` | Advisory/license/secret/SBOM/provenance policy on this checkout |
| Source candidate | `./scripts/release-check.sh --candidate` | Version, immutable CI input, history collision and package policy |
| Live providers | `AIKIT_LIVE_SMOKE=1 AIKIT_LIVE_SMOKE_FULL=1 ./scripts/live-smoke.sh` | Fail-closed billable acceptance; requires all eight credential/model pairs |

## Remote workflow status at this snapshot

| Workflow | Result | Evidence |
|---|---|---|
| A2A conformance | Passed | [run 30060414399](https://github.com/matakartal/aikit-runtime/actions/runs/30060414399) |
| Chaos | Passed | [run 30060414351](https://github.com/matakartal/aikit-runtime/actions/runs/30060414351) |
| Security | Passed | [run 30060414388](https://github.com/matakartal/aikit-runtime/actions/runs/30060414388) |
| CodeQL main push | Passed | [run 30060414054](https://github.com/matakartal/aikit-runtime/actions/runs/30060414054) |
| General CI | Passed | [run 30060414507](https://github.com/matakartal/aikit-runtime/actions/runs/30060414507) |

All required workflows passed for commit `ac023c6837d3f235b98f60b51969aa74ebd4a0a3`. The Python and
Node A2A mapper scenarios now assert snapshot schema version `4`, and their public type contracts
cover the current durable outbox and pending-event fields.

## Not yet v1-complete

- Complete Rust-schema-generated declarations across every Python/TypeScript public type.
- Paid live acceptance for every advertised provider/model/capability combination.
- Live-accepted media models, other provider media endpoints, and full realtime reconnect/event
  transports; the OpenAI HTTP contracts exist but are not advertised as live-proven support.
- Transparent egress enforcement for arbitrary child processes and Linux root+KVM Firecracker
  boot/escape/TAP proof; the explicit HTTP/browser broker already pins DNS and revalidates every
  redirect hop.
- PostgreSQL failover/partition proof and a real Temporal SDK worker integration; Sync-only local
  coordination does not satisfy this distributed gate.
- Complete official A2A timestamp/history/artifact-update transport support and wire the typed
  delta journal into production persistence, plus ACP editor/CLI integration; remove the pinned
  TCK waivers only when upstream fixes land, and add MCP SDK/OAuth conformance.
- Longer fuzz/chaos campaigns and multi-platform signing proof.
- crates.io/PyPI/npm ownership, publication authority, published packages and rollback rehearsal.

No missing external gate is converted into a synthetic pass. See
[`PARITY-MATRIX.md`](PARITY-MATRIX.md) for row-level status and exact upstream pins.
