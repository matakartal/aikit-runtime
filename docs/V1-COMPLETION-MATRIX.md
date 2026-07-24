# 0.2 implementation matrix

> **Historical inventory:** this file preserves the 0.2 surface audit. The current 0.3 alpha and
> v1 eligibility source of truth is [`PARITY-MATRIX.md`](PARITY-MATRIX.md).

This matrix separates implementation proof from optional live-provider proof. A keyless test can
complete an implementation row, but it cannot prove that a changing provider accepts the wire
request. The historical filename is retained to avoid breaking existing links; this document now
tracks the `v0.2.0` source-preview contract.

| Area | 0.2 source requirement | Repository evidence | Status |
|---|---|---|---|
| Phase 0 | Rust stream to a Python async iterator and Python async callback back into Rust | `examples/python/spike.py`, `docs/PHASE-0-SPIKE.md` | Complete |
| Canonical schema | Non-lossy text, reasoning, tool, media, citation, usage, provider options, and raw provider metadata | `types.rs`; adapter/runtime/session tests; three-language `INPUT` conformance | Complete |
| Native providers | Anthropic Messages, OpenAI Responses, Google Gemini, and DeepSeek transports | Four live adapters plus local real-socket wire tests | Implementation complete; live acceptance is a separate gate |
| Reasoning fidelity | Preserve and replay each provider's own opaque/signed reasoning state without cross-provider leakage | `reasoning.rs`; Anthropic/OpenAI Responses/Gemini/DeepSeek replay fixtures | Complete keylessly |
| Agent activation | Environment discovery, runtime `add_key`, `add_tool`, capability growth, and secret-safe introspection | `agent.rs`; Python/Node binding scenarios | Complete |
| Text DX | `generate_text`, `stream_text`, reusable `Client`, canonical string/message/media input, transcript, and cancellation in all three languages | Rust facade; PyO3/napi Agent and Client surfaces; `INPUT` conformance | Complete |
| Structured DX | Schema validation, honest `FidelityGrade`, bounded repair, async semantic accept/retry/reject validation, multimodal `generate_object`/`stream_object`, and serde/Pydantic/Zod materialization | `dx.rs`; Rust/Python/Node semantic, typed, real-Zod, streaming, and media tests | Complete |
| Governance | Pre-side-effect schema validation and global authoritative allow/ask/deny, including async approval and safe scoped grants | `governance/*`, `runtime.rs`, host callback tests | Complete |
| Declarative policy | Claude-Code-style JSON `PolicySpec` with mode + allow/ask/deny globs compiled into the permission engine | `governance/policy.rs`, `examples/policy.rs` | Complete in Rust core |
| Plan mode | Whole-approach HITL: propose plan, human approve/revise/reject before tools run | `governance/plan.rs`, `examples/plan_mode.rs` | Complete in Rust core |
| Risk / smart approval | Keyless risk scoring and `SmartApprover` that auto-allows low risk and escalates the rest | `governance/risk.rs`, `examples/smart_approval.rs` | Complete in Rust core (LLM judge remains host-pluggable) |
| Reliability rules | Declarative ordering, prerequisites, max uses, soft forbids separate from security | `governance/reliability.rs`, `examples/reliability.rs` | Complete in Rust core |
| Off-prompt output | Store large/sensitive tool results by reference; retrieve on demand | `governance/off_prompt.rs` | Complete in Rust core |
| Hooks | UserPrompt, PreTool, PostTool, tool-scoped PostToolFailure, general Failure, and Stop with bounded execution and rewrite/block semantics | `governance/hooks.rs`; Rust/Python/Node ordering tests | Complete |
| Audit | Typed lifecycle, provider, permission, hook, tool, usage, budget, structured-output, and subagent events; metadata-only default; fail-closed JSONL; optional Rust-host OTel bridge | `observability.rs`; binding JSONL configuration; audit conformance | Complete; OTel exporter ownership remains with the Rust host |
| Built-in tools | Read/Write/Edit/Glob/Grep and separately enabled Bash, with canonical schemas and host/builtin collision safety | `tools/builtin/*`; Rust/Python/Node public surfaces and runtime tests | Complete on supported jailed platforms |
| Filesystem jail | Descriptor-relative access, no symlink following, multiple roots, regular-file enforcement, and race-resistant writes | `governance/sandbox.rs`, `tools/builtin/fs.rs` tests | Complete on Linux/macOS; unsupported platforms fail closed |
| Bash containment | Environment/resource limits, cancellation cleanup, and required native/Docker isolation | `governance/process.rs`, `governance/containment/*`, threat model and platform probes | Seatbelt, Linux namespace+seccomp, Windows Job, and Docker implemented with distinct guarantees |
| Budget/resilience | Turn/token/USD/wall-time limits, shared reservations, caller pricing, retry/backoff, fallback-before-first-delta, and typed errors | `budget.rs`, `resilience.rs`, `runtime.rs`, `orchestration.rs` | Complete |
| Routing/council | Caller-owned model catalog, deterministic explicit/automatic normal-run routing, bounded fan-out, and quorum synthesis | `routing.rs`, `client.rs`, `orchestration.rs`, `INPUT` conformance | Complete |
| Subagents | Inherited governance/hooks/approver/tools, narrowed scope, shared budget/deadline, audit correlation, and resume | `orchestration.rs`; Python/Node context and resume tests | Complete |
| Memory/session | Explicit namespaced memory, JSON and transactional SQLite stores, canonical recording, revisioned CAS, and resume | `memory.rs`, `session.rs`, `sqlite.rs`; reopen and cross-instance conflict tests | Complete for in-memory, file, and cross-process local SQLite stores |
| Agent extensions | Bounded MCP stdio/HTTP tools/resources/prompts with exact tool filters, governed Web/Browser, human-approved capability requests, and compaction | `mcp.rs`, `tools/web.rs`, `governance/capability.rs`, `compaction.rs`; Rust/Python/Node filter and transport tests | Complete keylessly |
| Provider breadth | Four native adapters plus isolated OpenRouter, Groq, Mistral, and xAI compatible endpoints | provider, credential, capability, and routing tests | Implementation complete; live acceptance remains explicit and billable |
| Rust SDK | Ergonomic `aikit` facade over the single core | `crates/aikit`, examples, rustdoc/doctests | Complete locally |
| Python SDK | Typed PyO3 package, async streams/callbacks, canonical media input, typed errors, tool/DX helpers, governance, objects, audit, orchestration, and local stores | `crates/aikit-py`, strict mypy and runtime tests | Complete locally |
| TypeScript SDK | Typed napi package, async iterables/callbacks, canonical media input, typed errors, tool/DX helpers, Zod objects, audit, orchestration, and local stores | `crates/aikit-node`, strict tsc and runtime tests | Complete locally |
| Source CLI | Keyless run, canonical chat, provider/capability discovery, doctor, deterministic eval, structured output modes, exit codes, and completions | `crates/aikit-cli`, unit and binary integration tests | Complete locally |
| Evaluation | Strict versioned datasets; current-invocation text/tool/status/usage/model-attempt gates; redacted provenance; bounded live opt-in | `eval.rs`, `evals/smoke.json`, CLI and Rust/Python/Node outcome-evaluation tests | Complete keylessly |
| Conformance | Eight canonical Rust/Python/Node modules: governance, objects, options/errors, state/audit, orchestration, built-ins, multimodal/routed input, and A2A | `crates/aikit/examples/conformance.rs`, `examples/{python,node}/conformance.*`, `scripts/parity-check.sh` | Complete keylessly |
| Live proof | Text, structured output, governed denial, and two-request replay against all four configured real providers | Ignored harness and fail-closed wrapper in `crates/aikit/tests/live_smoke.rs` / `scripts/live-smoke.sh` | Not run; requires real keys/models and billable network calls |
| OSS readiness | README, feature reference, status, threat model, security/support policies, contributing/code of conduct, issue/PR templates, Discussions, CODEOWNERS, Dependabot, CodeQL, and CI | Root docs, `.github`, repository-level CodeQL default setup, and `security.yml` | Repository materials complete; source remote and private security reporting verified |
| Distribution | Source checkout plus locally assembled Cargo, Python ABI3, and Node artifacts | CI/manual assembly workflow plus `stage-node-platform.sh` and packaged-loader tests | Complete; external registry publication is intentionally out of scope |

## Deliberate boundaries beyond 0.2

Remote database services, model-generated compaction summaries, MCP server mode, WASM packaging,
and stronger Windows filesystem/network isolation remain beyond 0.2. Windows Job Objects deliberately
claim process-tree/resource containment only; the descriptor-relative file jail remains
Linux/macOS-only.

## Optional external validation

The historical 0.2 gate covered four native providers. The current harness expands this to all
eight named adapters and still requires real keys, selected model ids, network
access, and billable calls. It is not required for source distribution and the keyless suite must
not convert its absence into a synthetic pass. The historical artifact snapshot remains in
[`releases/v0.1.0.md`](releases/v0.1.0.md).

## Language-surface note

Declarative policy, plan mode, smart approval, reliability rules, and off-prompt storage ship in
`aikit-runtime-core` with Rust examples. Python and Node already expose permissions, hooks,
approvers, tools, orchestration, and state through the shared binding contract. Projecting every
new governance primitive into typed binding helpers remains ongoing work and must not silently
imply parity before the surface lands in all three languages.

Semantic structured-output validators, MCP tool filters, and outcome evaluation are already
projected across Rust, Python, and Node with strict binding tests. That does not retroactively make
every Rust-only governance convenience a three-language helper.
