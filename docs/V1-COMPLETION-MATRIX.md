# v1 completion matrix

This matrix separates implementation proof from live-provider and publication proof. A keyless
test can complete an implementation row, but it cannot prove that a changing provider accepts the
wire request or that an artifact has been published under an authorized registry name.

| Area | v1 requirement | Repository evidence | Status |
|---|---|---|---|
| Phase 0 | Rust stream to a Python async iterator and Python async callback back into Rust | `examples/python/spike.py`, `docs/PHASE-0-SPIKE.md` | Complete |
| Canonical schema | Non-lossy text, reasoning, tool, media, citation, usage, provider options, and raw provider metadata | `types.rs`; adapter/runtime/session tests; three-language `INPUT` conformance | Complete |
| Native providers | Anthropic Messages, OpenAI Responses, Google Gemini, and DeepSeek transports | Four live adapters plus local real-socket wire tests | Implementation complete; live acceptance is a separate gate |
| Reasoning fidelity | Preserve and replay each provider's own opaque/signed reasoning state without cross-provider leakage | `reasoning.rs`; Anthropic/OpenAI Responses/Gemini/DeepSeek replay fixtures | Complete keylessly |
| Agent activation | Environment discovery, runtime `add_key`, `add_tool`, capability growth, and secret-safe introspection | `agent.rs`; Python/Node binding scenarios | Complete |
| Text DX | `generate_text`, `stream_text`, reusable `Client`, canonical string/message/media input, transcript, and cancellation in all three languages | Rust facade; PyO3/napi Agent and Client surfaces; `INPUT` conformance | Complete |
| Structured DX | Schema validation, honest `FidelityGrade`, bounded repair, multimodal `generate_object`/`stream_object`, and serde/Pydantic/Zod materialization | `dx.rs`; Rust/Python/Node typed, real-Zod, streaming, and media tests | Complete |
| Governance | Pre-side-effect schema validation and global authoritative allow/ask/deny, including async approval and safe scoped grants | `governance/*`, `runtime.rs`, host callback tests | Complete |
| Hooks | UserPrompt, PreTool, PostTool, tool-scoped PostToolFailure, general Failure, and Stop with bounded execution and rewrite/block semantics | `governance/hooks.rs`; Rust/Python/Node ordering tests | Complete |
| Audit | Typed lifecycle, provider, permission, hook, tool, usage, budget, structured-output, and subagent events; metadata-only default; fail-closed JSONL; optional Rust-host OTel bridge | `observability.rs`; binding JSONL configuration; audit conformance | Complete; OTel exporter ownership remains with the Rust host |
| Built-in tools | Read/Write/Edit/Glob/Grep and separately enabled Bash, with canonical schemas and host/builtin collision safety | `tools/builtin/*`; Rust/Python/Node public surfaces and runtime tests | Complete on supported jailed platforms |
| Filesystem jail | Descriptor-relative access, no symlink following, multiple roots, regular-file enforcement, and race-resistant writes | `governance/sandbox.rs`, `tools/builtin/fs.rs` tests | Complete on Linux/macOS; unsupported platforms fail closed |
| Bash containment | Environment/resource limits, cancellation cleanup, and required native/Docker isolation | `governance/process.rs`, `governance/containment/*`, threat model and platform probes | Seatbelt, Linux namespace+seccomp, Windows Job, and Docker implemented with distinct guarantees |
| Budget/resilience | Turn/token/USD/wall-time limits, shared reservations, caller pricing, retry/backoff, fallback-before-first-delta, and typed errors | `budget.rs`, `resilience.rs`, `runtime.rs`, `orchestration.rs` | Complete |
| Routing/council | Caller-owned model catalog, deterministic explicit/automatic normal-run routing, bounded fan-out, and quorum synthesis | `routing.rs`, `client.rs`, `orchestration.rs`, `INPUT` conformance | Complete |
| Subagents | Inherited governance/hooks/approver/tools, narrowed scope, shared budget/deadline, audit correlation, and resume | `orchestration.rs`; Python/Node context and resume tests | Complete |
| Memory/session | Explicit namespaced memory, JSON and transactional SQLite stores, canonical recording, revisioned CAS, and resume | `memory.rs`, `session.rs`, `sqlite.rs`; reopen and cross-instance conflict tests | Complete for in-memory, file, and cross-process local SQLite stores |
| Agent extensions | MCP stdio/HTTP tools/resources/prompts, governed Web/Browser, human-approved capability requests, and compaction | `mcp.rs`, `tools/web.rs`, `governance/capability.rs`, `compaction.rs`; three SDK surfaces | Complete keylessly |
| Provider breadth | Four native adapters plus isolated OpenRouter, Groq, Mistral, and xAI compatible endpoints | provider, credential, capability, and routing tests | Implementation complete; live acceptance remains explicit and billable |
| Rust SDK | Ergonomic `aikit` facade over the single core | `crates/aikit`, examples, rustdoc/doctests | Complete locally |
| Python SDK | Typed PyO3 package, async streams/callbacks, canonical media input, typed errors, tool/DX helpers, governance, objects, audit, orchestration, and local stores | `crates/aikit-py`, strict mypy and runtime tests | Complete locally |
| TypeScript SDK | Typed napi package, async iterables/callbacks, canonical media input, typed errors, tool/DX helpers, Zod objects, audit, orchestration, and local stores | `crates/aikit-node`, strict tsc and runtime tests | Complete locally |
| Conformance | Seven canonical Rust/Python/Node modules: governance, objects, options/errors, state/audit, orchestration, built-ins, and multimodal/routed input | `crates/aikit/examples/conformance.rs`, `examples/{python,node}/conformance.*`, `scripts/parity-check.sh` | Complete keylessly |
| Live proof | Text, structured output, governed denial, and two-request replay against all four configured real providers | Ignored harness and fail-closed wrapper in `crates/aikit/tests/live_smoke.rs` / `scripts/live-smoke.sh` | Not run; requires real keys/models and billable network calls |
| OSS readiness | README, feature reference, threat model, security policy, contributing/code of conduct, issue/PR templates, and CI | Root docs and `.github` | Repository materials complete; verified remote/private security contact still required |
| Distribution | Cargo package set, Python ABI3 wheels, npm wrapper/platform packages, licenses, readmes, and types | CI/release workflows plus `stage-node-platform.sh` and packaged-loader tests | Layout complete; remote multi-target run and registry authority remain release evidence gates |

## Deliberate post-v1 boundaries

Remote database services, model-generated compaction summaries, MCP server mode, WASM packaging,
and stronger Windows filesystem/network isolation remain post-v1. Windows Job Objects deliberately
claim process-tree/resource containment only; the descriptor-relative file jail remains
Linux/macOS-only.

## External release blockers

The implementation candidate cannot honestly become a released v1 until a maintainer:

1. runs and records the explicit four-provider live matrix with real keys and current model ids;
2. verifies ownership/publication authority for the coordinated `aikit-runtime` names;
3. configures a real source remote, private security contact, signing, and registry authority; and
4. builds and inspects the final-name native artifacts on every supported release target.

These are authority, credential, and release-environment gates. The keyless suite must not convert
their absence into a synthetic pass.
