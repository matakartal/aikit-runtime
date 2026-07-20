# AIKit competitor parity matrix

**Snapshot:** 2026-07-20
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
| [Pydantic AI](https://github.com/pydantic/pydantic-ai) | `7594270096ff92cbe09ce3fe8e80cb9ede591a08` | MIT | model profiles, output validation, durability/evals |
| [Rig](https://github.com/0xPlaygrounds/rig) | `87f3f5b77a3caeffa10d60225c41e386753bf05e` | MIT | Rust provider contracts |
| [LiteLLM](https://github.com/BerriAI/litellm) | `bd44c9e305b89526d4c5d773ee39ca935561b9c8` | MIT outside `enterprise/`; enterprise license inside | capability breadth and provider metadata |
| [Vercel AI SDK](https://github.com/vercel/ai) | `6cd7c74acf0d7ec84dd58a841fc0e20970d6f2e8` | Apache-2.0 | stream lifecycle |
| [Microsoft Agent Governance Toolkit](https://github.com/microsoft/agent-governance-toolkit) | `d00ccdbf31258db917495ca65fa2ecd9e64461b9` | MIT | scoped policy and governance evidence |
| [OpenAI Codex](https://github.com/openai/codex) | `678157acaa819d5510adfe359abb5d0392cfe461` | Apache-2.0 | OS containment and approvals |
| [LangGraph](https://github.com/langchain-ai/langgraph) | `49ae27c2ae983cfb92091b0dea9f7bc37a716479` | MIT | checkpoint, resume, fork and interrupt |
| [Microsoft Agent Framework](https://github.com/microsoft/agent-framework) | `7c6b1e975f75193ace223a05c6535b8556f93ee4` | MIT | workflow checkpoints |
| [Agno](https://github.com/agno-agi/agno) | `24dfe73375f4f708a1314a0bb20e5d2b28d797db` | Apache-2.0 | HITL and memory |
| [Letta](https://github.com/letta-ai/letta) | `b76da9092518cbaa2d09042e52fdcbde69243e18` | Apache-2.0 | memory planes and tool rules |
| [MCP specification](https://github.com/modelcontextprotocol/modelcontextprotocol) | `26897cc322f356487da89113451bd16b520b9288` | transition mix: Apache-2.0/MIT; docs CC-BY-4.0 | tools/resources/prompts/tasks |
| [A2A](https://github.com/a2aproject/A2A) | `af112d9491c1fd4b2a568ac65755af4a62790490` | Apache-2.0 | remote agent task mapping |
| [Zed ACP](https://github.com/agentclientprotocol/agent-client-protocol) | `169194fd4e941c7b1eddee7ca58f5deaf1bcfda0` | Apache-2.0 | editor/CLI adapter |
| [OpenAI Agents Python](https://github.com/openai/openai-agents-python) | `2fa463571e76dae8ff267622f1018eaf06ffeb9f` | MIT | traces, evals and sandbox expectations |
| [Google ADK Python](https://github.com/google/adk-python) | `be5828f317c7430411df29974cd9ccfa875e90de` | Apache-2.0 | eval and telemetry structure |

## Phase 1 — canonical contract and eight providers

| Required behavior | AIKit implementation | Deterministic evidence | Status | Remaining gate |
|---|---|---|---|---|
| One canonical Rust runtime across Rust/Python/TypeScript | `aikit-runtime-core`, PyO3 and napi wrappers; shared observable conformance | `scripts/parity-check.sh`; binding type checks | `Equivalent` for observable behavior | Rust-schema-generated declarations still cover only part of the public type inventory, so the v1 “all public types generated” gate remains `Partial` |
| Tri-state model capability (`supported/unsupported/unknown`) | `contract.rs`, `routing.rs`; unknown requirements fail closed | catalog/routing unit tests | `Equivalent` | project catalog helpers into all SDKs |
| Versioned offline catalog and separate overrides | `catalog.rs`; compiled snapshot in `catalog/model-catalog-v0.3-alpha.json`; canonical hash | catalog integrity/override tests | `Equivalent` in Rust | binding convenience APIs and scheduled source refresh |
| Structured-output capabilities are orthogonal, not one boolean | `StructuredOutputCapabilities` with six tri-state facts | capability tests | `Equivalent` in core | per-model live acceptance and full SDK helpers |
| Identified, ordered `start/delta/end/error/usage/warning` stream | `StreamEvent` + stateful `StreamEventEncoder`; legacy bridge | streaming tests; real Python/Node async event streams | `Equivalent` | remove `StreamDelta` only at v1 after migration window |
| Honest strict media input with MIME/size/hash | `MediaInput` and multimodal validation; legacy `ContentPart` media remains source-based | contract and provider-media tests | `Partial` | project strict media identity and artifact references through every older SDK convenience surface |
| Eight first-class provider adapters | Anthropic, OpenAI, Google, DeepSeek, OpenRouter, Groq, Mistral, xAI | `provider_conformance.rs`; provider unit and real-local-socket tests | `Equivalent` keylessly for text/tool/auth/error/stream wire contracts | live provider/model acceptance is mandatory and not run |
| Sanitized cassette set for text/stream/tool/schema/error/unsupported | `crates/aikit-core/cassettes/providers/*` + common validator | source-tree and extracted-package `provider_cassettes` tests | `Equivalent` keylessly | live refresh and review for changing APIs |
| No silent unsupported parameter drop | protected option validation and typed `ProviderError`; compatibility modes are explicit | provider conformance tests | `Equivalent` for declared adapter parameters | expand model-specific option tables with live evidence |
| Same alpha version in Cargo, Python and npm | release candidate checks | `scripts/release-check.sh --candidate` | `Equivalent` locally | registry packages remain unpublished; full generated-schema inventory and shared schema hash are tracked separately |

## Phase 2 — governance, approvals, skills and containment

| Required behavior | AIKit implementation | Deterministic evidence | Status | Remaining gate |
|---|---|---|---|---|
| `global → tenant → agent → run → tool`, deny-wins, immutable run policy | `PolicySnapshot`, scoped rules and stable hash | governance contract adversarial tests | `Equivalent` in core | wire snapshot pinning into every high-level run constructor |
| Native YAML plus OPA/Cedar decision adapters | fail-closed YAML loader and external-decision adapters | policy adapter tests | `Partial` | external OPA/Cedar conformance service tests and SDK helpers |
| Public/internal/confidential/secret flow labels and provenance | `DataLabel`, `Provenance`, source/sink flow policy | secret-to-network and deny precedence tests | `Equivalent` in core | runtime propagation through every third-party tool adapter |
| Durable scoped approval with evidence and timeout-deny | governance approval evidence + durable approval events | expiry, drift, restart and resume tests | `Partial` | unify the legacy high-level approver with the durable approval record |
| Pinned skills; prompt/data default; executable only with policy and containment | `SkillPackage`, `SkillLoader`, hash/source pin, typosquat/hidden/executable inspection | skills adversarial tests | `Stronger` locally | host SDK loader helpers and remote-source fetch policy |
| MCP schema/description drift requires re-approval | skill/package integrity plus MCP registry schema identity | governance/protocol tests | `Partial` | connect live MCP discovery drift to durable approval invalidation |
| Seatbelt/Linux namespace+seccomp/Windows Job/Docker under one profile | containment backends + `SandboxProfile` | platform/backend tests and threat model | `Equivalent` for documented backend guarantees | Windows filesystem jail remains unavailable |
| Egress broker: DNS pinning, allowlist, redirect checks, browser proxy | `EgressBroker`, `EgressPolicy`, per-hop DNS pinning and browser assertion | local-socket SSRF/private-IP/rebinding/redirect/size/redaction tests | `Equivalent` for explicit HTTP/browser-broker calls | transparent proxying for arbitrary child processes |
| Optional Firecracker microVM | no production backend | none | `Absent` | implement and run Linux escape suite on a supported host |

## Phase 3 — durable execution, HITL and memory

| Required behavior | AIKit implementation | Deterministic evidence | Status | Remaining gate |
|---|---|---|---|---|
| Append-only event log, checkpoint projection, resume/fork/rewind/cancel | `durability.rs`; replay-validating `RunState`; Python/Node `DurableRun` | 13+ core tests and real binding round-trips | `Equivalent` locally | distributed soak tests |
| `pure/idempotent/reconcile_required`, no false exactly-once claim | stable activity ID/input hash/idempotency; ambiguous effects stop in reconciliation | crash/retry/ambiguous-effect tests | `Stronger` | provider-specific idempotency guidance |
| Preserve successful sibling writes; rerun failed branch only | activity ledger and branch projection | parallel activity recovery test | `Equivalent` | distributed scheduler implementation |
| Durable confirmation/input/review/edit-retry approval states | durable approval/events and protocol input-required mapping | approval restart/resume tests | `Partial` | high-level ergonomic review/edit APIs in all SDKs |
| Working/episodic/semantic memory with CAS and provenance | in-memory, JSON and SQLite memory stores | lost-update, plane, reopen and SQLite CAS tests | `Equivalent` locally | distributed semantic index adapter |
| SQLite local durable store | `SqliteDurableStore`, event-sequence CAS | cross-instance CAS tests | `Equivalent` | process-kill integration suite on CI |
| PostgreSQL distributed store | feature-gated `PostgresDurableStore`; transaction, row lock, revision CAS and append-only validation | local validation tests; ignored disposable-DB two-connection CAS test | `Partial` | authorized live PostgreSQL/failover execution |
| Temporal reference engine adapter | deterministic SDK-neutral activity/retry/idempotency/reconciliation mapping | eight replay/tamper/reconciliation tests | `Equivalent` for reference mapping | real Temporal SDK worker, replay and cancellation integration tests |

## Phase 4 — multimodal and protocols

| Required behavior | AIKit implementation | Deterministic evidence | Status | Remaining gate |
|---|---|---|---|---|
| Shared image/audio/transcript/artifact/realtime contract | `multimodal.rs`, `media_runtime.rs` | validation, state, persisted dedupe and cancellation tests | `Equivalent` as provider-neutral SPI | language-specific ergonomic wrappers |
| Capability-aware modality routing; no implicit provider fallback | `MediaRouter`, explicit `MediaFallbackPolicy` | unknown/unsupported, all-of and opt-in fallback tests | `Equivalent` | populate every supported model/modality from live acceptance |
| Image generation, transcription, speech and realtime provider transports | catalog-gated OpenAI HTTP image/transcription/speech/WebRTC contracts plus typed SPIs | five keyless local-socket auth/upload/cancel/error/artifact tests | `Partial` | shipped models remain unsupported without live proof; other provider endpoints and realtime event reconnect transport |
| MCP tools/resources/prompts/auth/progress/cancel server | governed registry/task state machine; existing MCP client transports | protocol and MCP client tests | `Partial` | network server transport and official conformance suite |
| Durable MCP Tasks | task state is durable/governed | protocol state tests | `Partial` | persist through `DurableStore` and test restart over transport |
| A2A 1.0 mapping | `context_id → Session`, `task_id → Run`, dedupe/input-required | protocol tests | `Equivalent` for mapping | HTTP transport, auth and official examples |
| Zed ACP v1 adapter | session/prompt/cancel and runtime event mapping | protocol tests | `Equivalent` for mapping | thin CLI/editor transport and Zed acceptance |

## Phase 5 — evals, telemetry, hardening and release

| Required behavior | AIKit implementation | Deterministic evidence | Status | Remaining gate |
|---|---|---|---|---|
| Deterministic trace assertions over streams, policy, durability and approvals | `trace_eval.rs`; Python/Node `evaluate_trace` | core and real binding tests | `Equivalent` | multimedia artifact assertions and larger fixtures |
| Dataset evals for final text/tool/status/usage | `eval.rs`, CLI datasets and governed demo trajectories | `evals/*.json`, CLI tests | `Equivalent` keylessly | optional judge adapter remains explicitly non-authoritative |
| Redacted trace hierarchy `agent → run → model/tool → checkpoint/activity` | `TraceCollector`, `TelemetryPolicy`; OTel bridge | observability tests | `Equivalent` in core | exporter integration tests across SDK hosts |
| Property/fuzz malformed streams, payloads and schema drift | bounded parser and adversarial unit tests | core provider/runtime tests | `Partial` | persistent fuzz targets and corpus in CI |
| Chaos: kill, disconnect, rate limit, timeout, DB failover | deterministic crash/recovery unit scenarios | durability/resilience tests | `Partial` | process-level chaos and PostgreSQL failover jobs |
| Security: escape, SSRF, injection, traversal, policy bypass | jail/containment/web/governance adversarial suites | core tests + CodeQL/security workflow | `Partial` | microVM and Windows isolation gaps prevent v1 security gate |
| SBOM, dependency/license policy, secret scan and provenance | `deny.toml`, `scripts/security-check.sh`, pinned security workflow | cargo-deny/audit, Gitleaks, CycloneDX, provenance checks | `Equivalent` keylessly | signed release artifacts on every target |
| Rust/Python/npm same release and schema hash | candidate/release workflow | release check | `Partial` | multi-platform signed artifacts and public registry publication |
| Live acceptance for every advertised provider/model capability | fail-closed live smoke harness | `scripts/live-smoke.sh` | `Absent` for this candidate | user-supplied credentials/models and authorized billable run |
| Rollback rehearsal | documented process only | release guide | `Absent` for this candidate | publish authority plus an actual staged rollback rehearsal |

## v1 release decision

`v1.0` is **not eligible** at this snapshot. Mandatory `Partial/Absent` rows remain, principally:
schema-generated full SDK declarations, live provider/modality acceptance, microVM/egress coverage,
live PostgreSQL failover and a real Temporal worker, protocol transports/conformance, cross-platform signed artifacts,
registry publication, and rollback rehearsal.

The local implementation must not turn missing credentials, registry ownership, signing identity,
or billable acceptance into a synthetic pass. Those are explicit external gates.
