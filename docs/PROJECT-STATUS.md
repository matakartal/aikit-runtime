# Project status

**Snapshot:** 2026-07-20
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
- Scoped governance contracts, immutable policy hashes, information-flow labels, approval evidence,
  hardened sandbox/egress profiles, and verified skill packages are implemented in the core.
- Durable runs use an append-only event log, replay-validated projections, checkpoints,
  activity/idempotency records, reconciliation, approvals, resume/fork/rewind/cancel, and SQLite
  compare-and-swap persistence. Python and Node expose the same `DurableRun` state machine.
- A feature-gated PostgreSQL store adds transactional row-lock/revision CAS, and the Temporal
  reference adapter deterministically maps activity, retry, idempotency and reconciliation state.
- Working, episodic, and semantic memory planes preserve provenance and use CAS rather than
  last-write-wins.
- Multimodal artifacts, transcription/speech/image/realtime SPIs, persisted realtime dedupe, and
  capability-aware fallback policy are typed. Catalog-gated OpenAI image, transcription, speech,
  and WebRTC-call HTTP transports have keyless wire tests; the shipped catalog still marks
  unproven media models unsupported until live acceptance exists.
- Governed MCP task, A2A 1.0, and ACP v1 mapping state machines are present. Network server/editor
  transports and official conformance suites remain open.
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
| Provider cassettes | `cargo test -p aikit-runtime-core --test provider_cassettes --locked` | Sanitized required scenario inventory and envelope validation |
| Keyless eval | `cargo run -p aikit-cli --locked -- eval evals/smoke.json` | Dataset parsing, execution and deterministic gates |
| Security/SBOM | `./scripts/security-check.sh --all` | Advisory/license/secret/SBOM/provenance policy on this checkout |
| Source candidate | `./scripts/release-check.sh --candidate` | Version, immutable CI input, history collision and package policy |
| Live providers | `AIKIT_LIVE_SMOKE=1 AIKIT_LIVE_SMOKE_FULL=1 ./scripts/live-smoke.sh` | Fail-closed billable acceptance; requires all eight credential/model pairs |

## Not yet v1-complete

- Complete Rust-schema-generated declarations across every Python/TypeScript public type.
- Paid live acceptance for every advertised provider/model/capability combination.
- Live-accepted media models, other provider media endpoints, and full realtime reconnect/event
  transports; the OpenAI HTTP contracts exist but are not advertised as live-proven support.
- Transparent egress enforcement for arbitrary child processes and a Firecracker backend; the
  explicit HTTP/browser broker already pins DNS and revalidates every redirect hop.
- Authorized live PostgreSQL failover proof and a real Temporal SDK worker integration.
- MCP server transport, A2A transport and ACP editor/CLI integration against official examples.
- Persistent fuzz targets, process-level chaos jobs and multi-platform signing proof.
- crates.io/PyPI/npm ownership, publication authority, published packages and rollback rehearsal.

No missing external gate is converted into a synthetic pass. See
[`PARITY-MATRIX.md`](PARITY-MATRIX.md) for row-level status and exact upstream pins.
