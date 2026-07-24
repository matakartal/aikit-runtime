# AIKit competitor parity matrix

**Snapshot:** 2026-07-24
**Source candidate:** `v0.3.0-alpha.1` (not published)
**Rule:** a locally green test is implementation evidence, not live-provider, registry, signing, or
cross-platform release evidence.

This is the living source of truth for v1 parity. `Equivalent` means the scoped behavior is proved
locally. It does not mean the competitor's whole product was copied. A row stays `Partial` when a
transport, language projection, live acceptance run, distributed backend, or release authority is
still missing.

Statuses:

- `Absent`: no usable implementation.
- `Partial`: usable foundation exists, but the behavior or proof is incomplete.
- `Equivalent`: required behavior and deterministic proof exist in the declared scope.
- `Stronger`: equivalent behavior plus an additional enforced guarantee.

## Upstream pins and licenses

The commit is the reviewed upstream `HEAD` on the snapshot date. These pins are for behavioral and
architectural research; AIKit does not copy incompatible code. A future matrix update must advance
the commit and re-review the license together.

| Upstream | Commit | License at pin | Used for |
|---|---|---|---|
| [BAML](https://github.com/BoundaryML/baml) | `fe3304335ff13eb6355233b1f96690b6ede7ae09` | Apache-2.0 | canonical core and generated clients |
| [Pydantic AI](https://github.com/pydantic/pydantic-ai) | `61d751ec55f69804e765509b4e0a35b3cf2b7793` | MIT | model profiles, output validation, durability/evals |
| [Rig](https://github.com/0xPlaygrounds/rig) | `87f3f5b77a3caeffa10d60225c41e386753bf05e` | MIT | Rust provider contracts |
| [LiteLLM](https://github.com/BerriAI/litellm) | `bd44c9e305b89526d4c5d773ee39ca935561b9c8` | MIT outside `enterprise/`; enterprise license inside | capability breadth and provider metadata |
| [Vercel AI SDK](https://github.com/vercel/ai) | `6cd7c74acf0d7ec84dd58a841fc0e20970d6f2e8` | Apache-2.0 | stream lifecycle |
| [Microsoft Agent Governance Toolkit](https://github.com/microsoft/agent-governance-toolkit) | `d00ccdbf31258db917495ca65fa2ecd9e64461b9` | MIT | scoped policy and governance evidence |
| [OpenAI Codex](https://github.com/openai/codex) | `678157acaa819d5510adfe359abb5d0392cfe461` | Apache-2.0 | OS containment and approvals |
| [LangGraph](https://github.com/langchain-ai/langgraph) | `31f90df3e6b0268fa77fd2d118a917d420b84a68` | MIT | checkpoint, resume, fork and interrupt |
| [Microsoft Agent Framework](https://github.com/microsoft/agent-framework) | `0796af0c262df77ca7a8d48f907a5de90b1fca4a` | MIT | workflow checkpoints, A2A and Durable Task worker |
| [Agno](https://github.com/agno-agi/agno) | `1e03b4ef350f7c2706abc553a208e88b3f1e81e1` | Apache-2.0 | AgentOS auth, A2A and HITL |
| [Letta](https://github.com/letta-ai/letta) | `b76da9092518cbaa2d09042e52fdcbde69243e18` | Apache-2.0 | memory planes and tool rules |
| [MCP specification](https://github.com/modelcontextprotocol/modelcontextprotocol) | `26897cc322f356487da89113451bd16b520b9288` | transition mix: Apache-2.0/MIT; docs CC-BY-4.0 | tools/resources/prompts/tasks |
| [A2A](https://github.com/a2aproject/A2A) | `cfc9d34bc41e368827eb6446d31f912e44f795c5` | Apache-2.0 | remote agent task mapping |
| [A2A Python SDK](https://github.com/a2aproject/a2a-python) | `3e6fa6a41d64f0581202df214a0515a0b0194832` | Apache-2.0 | authenticated task-list behavior and wire projection |
| [Zed ACP](https://github.com/agentclientprotocol/agent-client-protocol) | `169194fd4e941c7b1eddee7ca58f5deaf1bcfda0` | Apache-2.0 | editor/CLI adapter |
| [OpenAI Agents Python](https://github.com/openai/openai-agents-python) | `34ab93536750dc3e245a07dfa465c599f1f5697e` | MIT | traces, HITL and resumable state |
| [OpenAI Agents JavaScript](https://github.com/openai/openai-agents-js) | `d601be6dcea96236b8c5aa9a6f5b4196c070cfb3` | MIT | cross-SDK run-state comparison |
| [Google ADK Python](https://github.com/google/adk-python) | `f71d9df9179a4d37a54051ffceb6dda5c821e4c4` | Apache-2.0 | A2A, rewind, eval and telemetry structure |
| [Claude Agent SDK Python](https://github.com/anthropics/claude-agent-sdk-python) | `e6e07f1c9b0542217e1cf4913e96b161a6bf92b2` | MIT | sessions, forks and subprocess trace propagation |
| [Claude Agent SDK TypeScript](https://github.com/anthropics/claude-agent-sdk-typescript) | `dc71e7c4868d6432d883111c425dc6ba7678a614` | Anthropic Commercial Terms; not OSS at pin | API observation only; no source reuse |

## Phase 1 — canonical contract and eight providers

| Required behavior | AIKit implementation | Deterministic evidence | Status | Remaining gate |
|---|---|---|---|---|
| One canonical Rust runtime across Rust/Python/TypeScript | `aikit-runtime-core`, PyO3 and napi wrappers; shared observable conformance | `scripts/parity-check.sh`; binding type checks | `Equivalent` for observable behavior | Rust-schema-generated declarations still cover only part of the public type inventory, so the v1 “all public types generated” gate remains `Partial` |
| Tri-state model capability (`supported/unsupported/unknown`) | `contract.rs`, `routing.rs`; Python/Node profile helpers; unknown requirements fail closed | catalog/routing and strict binding tests | `Equivalent` | live-accepted per-model facts remain a release gate |
| Versioned offline catalog and separate overrides | `catalog.rs`; compiled snapshot in `catalog/model-catalog-v0.3-alpha.json`; canonical hash; immutable Python/Node override layers | catalog integrity/override and byte-parity tests | `Equivalent` locally | scheduled source refresh and live acceptance |
| Structured-output capabilities are orthogonal, not one boolean | `StructuredOutputCapabilities` with six tri-state facts | capability tests | `Equivalent` in core | per-model live acceptance and full SDK helpers |
| Identified, ordered `start/delta/end/error/usage/warning` stream | `StreamEvent` + stateful `StreamEventEncoder`; legacy bridge | streaming tests; real Python/Node async event streams | `Equivalent` | remove `StreamDelta` only at v1 after migration window |
| Honest strict media input with MIME/size/hash | integrity-bound `ContentBlock::MediaInput` plus Python/Node strict message blocks; inline bytes/base64 are re-hashed before provider dispatch | contract, provider and binding negative tests | `Equivalent` for inline bytes/base64; `Partial` overall | strict URL/artifact references fail before network until governed egress/artifact resolvers return verified bytes |
| Eight first-class provider adapters | Anthropic, OpenAI, Google, DeepSeek, OpenRouter, Groq, Mistral, xAI | `provider_conformance.rs`; provider unit and real-local-socket tests | `Equivalent` keylessly for text/tool/auth/error/stream wire contracts | live provider/model acceptance is mandatory and not run |
| Sanitized cassette set for text/stream/tool/schema/error/unsupported | `crates/aikit-core/cassettes/providers/*` + common validator | source-tree and extracted-package `provider_cassettes` tests | `Equivalent` keylessly | live refresh and review for changing APIs |
| No silent unsupported parameter drop | protected option validation plus preflight provider catalogs; `strict` rejects unknown fields, wrong cataloged value types and governed nested-field drift, while `warn`/`best_effort` preserve them and emit typed warnings even when the HTTP call fails | provider, nested-option, HTTP-failure and binding conformance tests | `Equivalent` for shipped adapter parameter catalogs | expand model-specific option tables with live evidence; JSON Schema payloads remain intentionally opaque and a warning never upgrades an unknown model capability |
| Same alpha version in Cargo, Python and npm | release candidate checks | `scripts/release-check.sh --candidate` | `Equivalent` locally | registry packages remain unpublished; full generated-schema inventory and shared schema hash are tracked separately |

## Phase 2 — governance, approvals, skills and containment

| Required behavior | AIKit implementation | Deterministic evidence | Status | Remaining gate |
|---|---|---|---|---|
| `global → tenant → agent → run → tool`, deny-wins, immutable run policy | sealed `PolicySnapshot`, scoped rules, stable hash and append-only `PolicySnapshotPinned` run event; Python/Node governed constructors | governance drift/restart and binding parity tests | `Equivalent` locally | distributed policy-service soak |
| Native YAML plus OPA/Cedar decision adapters | fail-closed YAML loader, replay-bound external evidence and Python/Node normalizers | policy adapter and binding tests | `Partial` | real OPA/Cedar evaluator service conformance |
| Public/internal/confidential/secret flow labels and provenance | `DataLabel`, `Provenance`, source/sink flow policy | secret-to-network and deny precedence tests | `Equivalent` in core | runtime propagation through every third-party tool adapter |
| Durable scoped approval with evidence and timeout-deny | typed approval records, trusted clocks and persisted `DurableToolApprover` CAS bridge | exact-deadline expiry, drift, restart, replay and binding tests | `Equivalent` locally | host transports must keep the clock trusted |
| Pinned skills; prompt/data default; executable only with policy and containment | `SkillPackage`, `SkillLoader`, hash/source pin, typosquat/hidden/executable inspection | skills adversarial tests | `Stronger` locally | host SDK loader helpers and remote-source fetch policy |
| MCP schema/description drift requires re-approval | persisted MCP registry schema identity moves affected tasks to `input_required` and requires a new governed completion | MCP restart/drift tests | `Equivalent` locally | external SDK conformance |
| Seatbelt/Linux namespace+seccomp/Windows Job/Docker under one profile | containment backends + `SandboxProfile` | platform/backend tests and threat model | `Equivalent` for documented backend guarantees | Windows filesystem jail remains unavailable |
| Egress broker: DNS pinning, allowlist, redirect checks, browser proxy | `EgressBroker`, `EgressPolicy`, per-hop DNS pinning and browser assertion | local-socket SSRF/private-IP/rebinding/redirect/size/redaction tests | `Equivalent` for explicit HTTP/browser-broker calls | transparent proxying for arbitrary child processes |
| Optional Firecracker microVM | immutable hash-pinned config, jailer/VMM planning, trusted-path and live-prerequisite checks, bounded API lifecycle and cleanup | 10 fail-closed/adversarial contract tests; Rust 1.88 + strict clippy | `Partial` | Linux root+KVM boot/escape/TAP suite and guest command/workspace transport; never selected by Bash yet |

## Phase 3 — durable execution, HITL and memory

| Required behavior | AIKit implementation | Deterministic evidence | Status | Remaining gate |
|---|---|---|---|---|
| Append-only event log, checkpoint projection, resume/fork/rewind/cancel | `durability.rs`; replay-validating `RunState`; Python/Node `DurableRun` | 20 focused durability tests and real binding round-trips | `Equivalent` locally | distributed soak tests |
| Real agent-loop durable coordination | Sync-only `DurableRunDriver`; durable run id owns audit/runtime identity; provider/tool start and outcome use store CAS; completed results are reused | identity, pre-side-effect CAS failure, completed-provider reuse and ambiguous-tool reconciliation tests | `Equivalent` locally for in-process Sync | Async/Exit, real-loop process-crash matrix, provider idempotency guidance and real Temporal worker |
| `pure/idempotent/reconcile_required`, no false exactly-once claim | stable activity ID/input hash/idempotency; ambiguous effects stop in reconciliation | crash/retry/ambiguous-effect tests | `Stronger` | provider-specific idempotency guidance |
| Preserve successful sibling writes; rerun failed branch only | activity ledger and branch projection | parallel activity recovery test | `Equivalent` | distributed scheduler implementation |
| Durable confirmation/input/review/edit-retry approval states | typed durable events plus Python/Node request/resolve/expiry helpers | invalid response, exact timeout, policy drift and restart parity tests | `Equivalent` locally | distributed UI integration |
| Working/episodic/semantic memory with CAS and provenance | in-memory, JSON and SQLite memory stores | lost-update, plane, reopen and SQLite CAS tests | `Equivalent` locally | distributed semantic index adapter |
| SQLite local durable store | `SqliteDurableStore`, event-sequence CAS | cross-instance CAS and real child-process kill/reopen tests | `Equivalent` | distributed soak |
| PostgreSQL distributed store | feature-gated `PostgresDurableStore`; transaction, row lock, revision CAS and append-only validation | disposable PostgreSQL two-connection CAS passed on 2026-07-20; chaos CI repeats it | `Equivalent` for single-primary CAS | failover/partition soak remains open |
| Temporal reference engine adapter | deterministic SDK-neutral activity/retry/idempotency/reconciliation mapping | eight replay/tamper/reconciliation tests | `Equivalent` for reference mapping | real Temporal SDK worker, replay and cancellation integration tests |

## Phase 4 — multimodal and protocols

| Required behavior | AIKit implementation | Deterministic evidence | Status | Remaining gate |
|---|---|---|---|---|
| Shared image/audio/transcript/artifact/realtime contract | `multimodal.rs`, `media_runtime.rs` | validation, state, persisted dedupe and cancellation tests | `Equivalent` as provider-neutral SPI | language-specific ergonomic wrappers |
| Capability-aware modality routing; no implicit provider fallback | `MediaRouter`, explicit `MediaFallbackPolicy` | unknown/unsupported, all-of and opt-in fallback tests | `Equivalent` | populate every supported model/modality from live acceptance |
| Image generation, transcription, speech and realtime provider transports | catalog-gated OpenAI HTTP image/transcription/speech/WebRTC contracts plus typed SPIs | five keyless local-socket auth/upload/cancel/error/artifact tests | `Partial` | shipped models remain unsupported without live proof; other provider endpoints and realtime event reconnect transport |
| MCP tools/resources/prompts/auth/progress/cancel server | MCP 2025-11-25 JSON-RPC dispatcher with stdio and Streamable HTTP, Origin/auth/session/version/Host/Accept enforcement and bounded SSE replay | 33 protocol tests with real socket/subprocess and official-shaped fixtures | `Equivalent` locally | official external SDK/OAuth discovery conformance |
| Durable MCP Tasks | task/receipt/dedupe/session/SSE state persisted through SQLite CAS; expired side-effect replay evidence retires the old connection namespace fail-closed | restart, duplicate, retention-expiry, cancellation, schema drift and `Last-Event-ID` tests | `Equivalent` locally | PostgreSQL MCP store and official external conformance |
| A2A 1.0 canonical mapping and JSON-RPC/SSE | owner-scoped `context_id → Session`, `task_id → Run`, dedupe/input-required, authenticated ListTasks, artifact/direct-Message projection, protected cancel ingress and Rust/Python/Node mapper surfaces | protocol/binding parity, 75 transport tests, and pinned official TCK raw plus exact-set verification | `Partial` | complete timestamps/history/artifact updates, production journal wiring, authenticated deployment proof, and removal of six pinned upstream TCK waivers |
| Zed ACP v1 adapter | session/prompt/cancel and runtime event mapping | protocol tests | `Equivalent` for mapping | thin CLI/editor transport and Zed acceptance |

## Phase 5 — evals, telemetry, hardening and release

| Required behavior | AIKit implementation | Deterministic evidence | Status | Remaining gate |
|---|---|---|---|---|
| Deterministic trace assertions over streams, policy, durability and approvals | `trace_eval.rs`; Python/Node `evaluate_trace` | core and real binding tests | `Equivalent` | multimedia artifact assertions and larger fixtures |
| Dataset evals for final text/tool/status/usage | `eval.rs`, CLI datasets and governed demo trajectories | `evals/*.json`, CLI tests | `Equivalent` keylessly | optional judge adapter remains explicitly non-authoritative |
| Redacted trace hierarchy `agent → run → model/tool → checkpoint/activity` | `TraceCollector`, `TelemetryPolicy`; OTel bridge | observability tests | `Equivalent` in core | exporter integration tests across SDK hosts |
| Property/fuzz malformed streams, payloads and schema drift | persistent libFuzzer targets for stream lifecycle, durable replay and provider cassettes; curated seed corpus | 3,000 local mutations; pinned scheduled/PR fuzz workflow | `Equivalent` for declared targets | longer sanitizer campaigns and new protocol corpus growth |
| Chaos: kill, disconnect, rate limit, timeout, DB failover | real child-process kill/reopen plus disposable PostgreSQL cross-connection CAS; existing retry/timeout scenarios | process chaos 2/2 and live local PostgreSQL CAS; scheduled chaos workflow | `Partial` | PostgreSQL failover/partition job remains open |
| Security: escape, SSRF, injection, traversal, policy bypass | jail/containment/web/governance adversarial suites plus Firecracker lifecycle validation | core tests + CodeQL/security workflow | `Partial` | Linux live microVM escape/TAP proof and Windows isolation gaps prevent v1 security gate |
| SBOM, dependency/license policy, secret scan and provenance | `deny.toml`, `scripts/security-check.sh`, pinned security workflow | cargo-deny/audit, Gitleaks, CycloneDX, provenance checks | `Equivalent` keylessly | signed release artifacts on every target |
| Rust/Python/npm same release and schema hash | candidate/release workflow | release check | `Partial` | multi-platform signed artifacts and public registry publication |
| Live acceptance for every advertised provider/model capability | fail-closed live smoke harness | `scripts/live-smoke.sh` | `Absent` for this candidate | user-supplied credentials/models and authorized billable run |
| Rollback rehearsal | documented process only | release guide | `Absent` for this candidate | publish authority plus an actual staged rollback rehearsal |

## v1 release decision

`v1.0` is **not eligible** at this snapshot. Mandatory `Partial/Absent` rows remain, principally:
schema-generated full SDK declarations, live provider/modality acceptance, Linux microVM/egress proof,
PostgreSQL failover and a real Temporal worker, production A2A journal persistence, ACP wire transport, unwaived external protocol conformance, cross-platform signed artifacts,
registry publication, and rollback rehearsal.

The local implementation must not turn missing credentials, registry ownership, signing identity,
or billable acceptance into a synthetic pass. Those are explicit external gates.

At this snapshot the dedicated A2A conformance, chaos, security, CodeQL main-push, and general CI
workflows all succeeded for commit `ac023c6837d3f235b98f60b51969aa74ebd4a0a3`. Python and Node
binding tests now assert mapper schema version `4`, matching the Rust runtime contract.
