# aikit 0.3 alpha feature reference

This document describes the source-first implementation in this repository. Public registry
packages are not distributed, and changing live provider APIs are not claimed as validated. See
the [architecture guide](ARCHITECTURE.md) for component ownership, the
[0.3 migration guide](MIGRATING-0.3.md), and the live [parity matrix](PARITY-MATRIX.md).

## Source-first CLI

The `aikit-cli` workspace crate exposes the same Rust facade through an `aikit` binary. It includes
one-shot runs, canonical multi-turn chat, secret-safe provider discovery, capability inspection,
workspace/containment diagnostics, deterministic eval datasets, and shell completion generation.
`mock-1` is the offline default.

Text is the human output; JSON is the single-document automation format; JSONL is the streaming
chat format. Input errors and runtime errors use distinct stable exit codes. The CLI never prints
credential values and does not silently choose a billable provider.

## One canonical runtime

`aikit-runtime-core` owns messages, usage, provider options/metadata, provider translation, reasoning
replay, the agent loop, governance, budget enforcement, routing, audit, sessions, and memory.
The `aikit` Rust crate re-exports that surface. PyO3 and napi translate host values and callbacks
at the edge; they do not reimplement policy.

The canonical content model preserves text, reasoning, tool calls/results, media, citations,
usage, and provider-specific escape hatches. A run recorder keeps canonical message history;
`final_text` is only a convenience projection.

Rust, Python, and Node text/stream/structured surfaces accept either the string convenience form
or canonical message history. Media blocks retain URL or inline-base64 sources without flattening.
An adapter that does not support media, such as DeepSeek, returns a typed error instead of silently
dropping the block.

Provider metadata is deliberately lossless and therefore potentially sensitive. Logprobs can
contain generated tokens; grounding fields can contain prompt-derived searches, URLs, and
citations. It is not included in metadata-only audit events, but run outcomes and sessions retain
it, so stores and host logs must protect it like prompts and model output.

### 0.3 canonical contracts

- Model features are tri-state: `supported`, `unsupported`, or `unknown`. Routing treats both
  unknown and unsupported required features as ineligible and reports which state caused the
  rejection.
- The embedded offline catalog is versioned and integrity-hashed. Caller overrides resolve into a
  separate hash and never mutate shipped data.
- `StreamEvent` adds event/response/block identity and monotonic `start/delta/end`, usage,
  warning, error, and raw-provider-event envelopes. Legacy `StreamDelta` is bridged during v0.x.
- Strict `MediaInput` values require MIME, size and SHA-256 identity. The existing `ContentPart`
  input union still carries URL/base64 media through its compatibility representation; projecting
  strict media identity through every older convenience surface remains a v1 gate.
- `OutputPart` materializes text, reasoning, media, files, transcripts, tool results, structured
  data, and citations without flattening them.
- Realtime event fingerprints survive serialization so reconnect duplicates remain idempotent;
  reusing an event id with different content fails closed.

Python and Node expose the v2 async event stream and the same durable run/eval state machine. Full
schema-generated declarations for every older convenience surface remain a tracked v1 gate rather
than an implied completion claim.

The provider media layer includes catalog-gated OpenAI HTTP contracts for image generation,
transcription, speech, and realtime WebRTC call setup. It uses typed cancellation/errors, bounded
responses, randomized multipart boundaries, and a host-owned stage/commit/abort artifact store.
The shipped catalog does not mark a media model supported without live acceptance, so these
transports fail closed by default instead of converting keyless wire proof into a support claim.

### Durable execution and protocol interop

`RunState` is reconstructed from an append-only event log. Checkpoints are projections, not the
authority. Activities must be declared `pure`, `idempotent`, or `reconcile_required`; an ambiguous
external effect stops for reconciliation instead of being automatically replayed. Rewind appends a
reverse event, fork creates a new run/branch identity, and approvals survive restart. SQLite uses
event-sequence compare-and-swap. The feature-gated PostgreSQL adapter adds transactional row locks,
revision CAS, and append-only validation; the Temporal reference adapter maps replay-safe IDs,
retry policy, history outcomes, and explicit reconciliation without pretending an SDK worker was
run locally.

MCP Tasks, A2A and ACP mappings share the governance envelope and cannot directly execute tools.
The current implementation proves state/authz/dedupe mappings; network server/editor transports are
still marked `Partial` in the parity matrix.

## Provider capabilities

| Provider | Reasoning | Vision | Citations | Structured-output fidelity |
|---|---:|---:|---:|---|
| Anthropic | Signed replay | Yes | Yes | `native_constrained` via `output_config.format` |
| OpenAI | Opaque Responses item replay | Yes | No | `native_constrained` |
| Google | Gemini 3 signature replay on the exact function-call part | Yes | Yes | `native_constrained` via `responseJsonSchema` |
| DeepSeek | Full reasoning replay for thinking turns with tool calls | No | No | `prompted_and_parsed` |
| OpenRouter | Model-dependent; dropped on replay | Yes | No | `prompted_and_parsed` |
| Groq | No replay claim | No | No | `prompted_and_parsed` |
| Mistral | No replay claim | Yes | No | `prompted_and_parsed` |
| xAI / Grok | Reasoning supported; dropped on replay | Yes | No | `prompted_and_parsed` |

These are capability declarations and tested adapter behavior, not a promise that every model in
a provider's catalog supports every feature. The caller still chooses a compatible model.
Grok models accept both the natural `grok-*` form and the explicit `xai:grok-*` namespace;
credentials load from `XAI_API_KEY` and requests use `https://api.x.ai/v1/chat/completions`.
The exact rules follow the current [DeepSeek Thinking Mode](https://api-docs.deepseek.com/guides/thinking_mode)
and [Gemini thought-signature](https://ai.google.dev/gemini-api/docs/generate-content/thought-signatures)
contracts and remain subject to the live-smoke boundary.
Native structured-output encodings follow Anthropic's
[`output_config.format`](https://platform.claude.com/docs/en/build-with-claude/structured-outputs)
contract and Gemini's
[`GenerationConfig.responseJsonSchema`](https://ai.google.dev/api/generate-content) field.

### Semantic structured-output validation

`ObjectOptions.semantic_validator` runs an async application invariant only after parsing and full
JSON Schema validation. It returns `Accept`, `Retry(reason)`, or `Reject(reason)`: retry consumes
the existing bounded repair loop, while reject terminates immediately with the typed
`structured_output` error. Python exposes `validator=` and Node exposes `options.validator` with
the same contract before Pydantic/Zod materialization. Callbacks must be pure and idempotent;
exceptions, invalid decisions, and Rust panics fail closed. Reasons are size-bounded and the raw
candidate is not automatically included in errors or audit records. Retry reasons are sent to the
provider and recorded in audit, so callbacks should return a safe summary without secrets. Reject
and callback-failure details are returned to the host but recorded only as generic audit failures;
their messages should still avoid secrets because host applications may log errors. The
core rejects more than 32 structured-output retries and bounds each normalized reason to 1,024
bytes. It does not impose a framework timeout on the host callback; applications must wrap
validators whose latency is not already bounded. The
host-validator pattern follows the useful separation in
[PydanticAI output validators](https://pydantic.dev/docs/ai/core-concepts/output/), while aikit
keeps the retry policy and redaction boundary in its shared Rust core.

### Deterministic mock tool fixtures

`MockProvider` remains keyless and keeps its default first-advertised-tool behavior. Tests that
need an exact tool call may explicitly set both `provider_options.mock.tool_name` and
`provider_options.mock.tool_input`. The name must be present in that run's advertised tool list;
missing, malformed, or unadvertised fixture controls fail with the typed `configuration` error.
These fields are a deterministic test-fixture contract only. They are read only by the mock
provider, do not affect live providers, and are not a model-routing or production tool-selection
API.

## Governance and hooks

Tool authorization happens immediately before the side effect:

1. Every advertised tool schema is compiled before the first provider call; an invalid schema is
   a typed configuration failure.
2. Model-produced input is schema-validated before governance or approval callbacks.
3. UserPrompt and PreTool hooks may continue, rewrite, or block.
4. A named allow/ask/deny rule is evaluated; any matching deny is globally authoritative.
5. `ask` calls the host's async `ToolApprover`; no approver means fail closed.
6. Hook/approval-rewritten input is schema-validated again and then executed at most once.
7. PostTool hooks may rewrite the result or mark it as an error.
8. Tool-scoped PostToolFailure hooks run before general Failure hooks; Failure and Stop hooks
   receive typed lifecycle context.

Hooks have bounded timeouts. Permission decisions include their rule/default source in the audit
record. A model-named tool that was not advertised for the run is rejected before the executor.
Child agents receive a narrowed executor and cannot broaden the parent's advertised tool set or
permission scope.

Tool executors can additionally be wrapped in deterministic guardrails. The shared defaults redact
recognized secrets, email/card/SSN data from tool output and can block configured input patterns.
Semantic classifiers integrate through MCP and fail closed instead of embedding a billable model.

### Declarative permission policy

`PolicySpec` is the config front door to the permission engine. Load Claude-Code-style JSON and
compile it into an enforcing `PermissionEngine`:

```json
{
  "mode": "allow",
  "deny": ["Bash(rm -rf *)", "Read(*.env)"],
  "ask": ["Bash(git push *)"],
  "allow": ["Read(*)", "Write(./workspace/**)"]
}
```

Each rule is `Tool` (any input) or `Tool(glob)`. Globs match the whole decoded string leaf of the
tool input (`*` = any run including newlines, `?` = one character). Unknown JSON fields,
malformed rule names, and invalid patterns are rejected rather than ignored. Deny remains
authoritative regardless of rule order.
`PolicySpec::from_json` / `from_file` and `build()` are the Rust entry points; Python and Node
continue to accept structured permission rule lists that compile into the same engine.

Modes: `allow` (default permissive with explicit denials), `deny` (least privilege), `ask`
(escalate every unmatched call).

### Plan mode

Before tools run, the agent can propose a `Plan` (`goal` + ordered `PlanStep`s, optional tool names
per step). A host `PlanReviewer` returns:

- `Approve` — run the plan as proposed;
- `ApproveRevised(plan)` — run a human-edited plan;
- `Reject(reason)` — feed the reason back so the agent replans.

`review_plan` is transport-agnostic and executes nothing; it only decides *what* may run. Use it
when whole-approach HITL is stronger than per-tool approval.

### Risk scoring and smart approval

`RiskScorer` classifies a tool call as `Low`, `Medium`, or `High`. The default
`HeuristicRiskScorer` is deterministic and keyless: read-only tools are Low (Medium if the path
looks sensitive), Bash is High for dangerous verbs / Medium otherwise, writes escalate on
sensitive paths, and unknowns lean cautious.

`SmartApprover` wraps any human `ToolApprover`: calls at or below a risk threshold are
auto-allowed; the rest escalate. `SmartApprover::heuristic(human)` auto-approves only Low-risk
calls. Failure mode is "ask a human needlessly", never "run something dangerous silently".

An LLM-backed risk judge remains optional host code; the core ships the scorer trait and the
heuristic default so approval fatigue can drop without a billable dependency.

### Reliability rules

Permissions answer "is this call *safe*?". `ReliabilityPolicy` answers "does this call make sense
*right now*?". Declarative `ToolRequirement`s support:

- `forbidden` — soft control-flow forbid (distinct from a security deny);
- `only_after` — require prior tools to have run;
- `max_uses` — cap uses in one run;
- `min_step` — block until a minimum tool-call index.

`RunProgress` records tools after they execute; `ReliabilityPolicy::check` consults it before the
next call and returns `Allow` or model-facing `Forbid(reason)`. Rules load from JSON like
`PolicySpec`.

### Off-prompt tool output

`OffPromptExecutor` wraps any `ToolExecutor`. Outputs larger than `max_inline_bytes` are stored in
an `OffPromptStore` and replaced with a compact reference plus preview. The agent retrieves full
content only via the `retrieve_output` tool when needed. Small outputs pass through unchanged.
This protects context budget and reduces replaying bulky or sensitive tool dumps every turn.
Handles contain 128 bits from the OS CSPRNG and are scoped to one executor. The default store keeps
at most 128 outputs / 16 MiB for one hour, evicting oldest entries; oversized outputs fail closed.
The wrapper's canonical `retrieve_output_tool()` specification must be advertised with the wrapped
executor; otherwise references cannot be resolved by the model.

### Capability requests

Agents can request a tool they lack through the governed `request_capability` path. A human
decides; grants are recorded and scoped. Silent escalation is never allowed.

### Examples

```bash
cargo run -p aikit-runtime-core --example policy
cargo run -p aikit-runtime-core --example plan_mode
cargo run -p aikit-runtime-core --example smart_approval
cargo run -p aikit-runtime-core --example reliability
```

## Audit and OpenTelemetry

`AuditTrail` emits typed run, request, route, provider-attempt, permission, hook, tool, usage,
budget, structured-output, subagent, failure, and stop events. Child trails carry a parent run id.

- Payload policy defaults to `MetadataOnly`; arbitrary tool input/output is omitted.
- `Full` payload capture is explicit and bounds output previews.
- Sink failure can be `BestEffort` or `FailClosed`.
- Built-in sinks are in-memory and JSONL.
- On Unix, the JSONL sink rejects symlinks and forces both new and existing files to owner-only
  `0600` permissions.
- Path aliases to the same JSONL file share a process-local append lock, so concurrent in-process
  sinks cannot interleave records. Separate processes still require external coordination.
- Python and Node agents can configure the same JSONL sink; once configured, their defaults are
  metadata-only and fail-closed. Every text, object, client, and orchestration invocation uses a
  fresh run id while retaining configured sinks and subagent correlation.
- The optional `opentelemetry` Cargo feature forwards lifecycle events to a host-configured
  tracer. The library does not install or shut down a global exporter.

Audit files can still contain model names, tool names, errors, and timing. Protect them as
operational data even in metadata-only mode.

## Budgets, routing, and resilience

`BudgetTracker` enforces per-run token/USD limits after reported usage. `BudgetLedger` adds
pre-call reservations for parallel work, so siblings cannot each spend the same remaining budget.
USD enforcement requires caller-supplied `ModelPricing`; unknown pricing fails closed when a cost
limit applies.

The router uses a caller-maintained `ModelCatalog`. Explicit and automatic policies share the same
hard gates: active credentials, context/output limits, required skills/capabilities, and optional
cost caps. Automatic selection supports cost, quality, and balanced objectives with deterministic
tie-breaking. Normal `run` calls can carry `{profiles, request}` routing options; no secret value
enters a route decision.

`ResilientProvider` retries typed transient failures with bounded exponential backoff and can move
to the next configured target. Retry/fallback is allowed only before the first response delta is
released. Once streaming starts, the target is sticky; streamed output and tool side effects are
never replayed.

## Subagents

The core orchestrator supports bounded parallel fan-out and council-style synthesis. Each child
receives:

- an explicit task and model target;
- the intersection of parent, child-requested, and registered tool schemas plus a scoped executor;
- the parent's governance without a child override path;
- a child audit trail correlated to the parent;
- a shared pre-reservation budget ledger.

Results retain per-child status and canonical run evidence. One child failure does not silently
become a successful answer; council synthesis requires an explicit success quorum.
Python and Node expose `subtask` builders and `parallel` aliases over the same canonical machinery;
their `tool` helpers pair schemas with host callbacks without moving execution policy out of Rust.

## Sessions and memory

`RunRecorder` stores canonical messages, the current invocation's start index, usage, terminal
status, stop reason, model attempts, raw provider metadata, and the optional final-text projection. `InMemorySessionStore` and
`JsonFileSessionStore` use explicit revisions and compare-and-swap updates so concurrent resumes
cannot silently overwrite each other. JSON persistence uses same-directory replacement; it is a
process-local store, not a cross-process lock or distributed transaction system. Path aliases that
resolve to the same canonical parent share the in-process lock; final symlinks are rejected and
Unix files are owner-only. Session files can therefore contain sensitive provider output even when
audit payload policy is `MetadataOnly`.

Memory is explicit and namespaced. `InMemoryMemoryStore` and `JsonFileMemoryStore` provide bounded,
deterministic keyword/tag recall. Model output is never written automatically; callers must invoke
`remember`, which avoids turning prompt injection into silent long-term memory poisoning. The file
store canonicalizes path aliases into shared process state, rejects final symlinks, uses private
regular files and atomic same-directory replacement, but does not claim cross-process locking.
Python and Node expose JSON and SQLite memory/session selection; their defaults remain in-memory.
SQLite uses WAL, transactions, optimistic CAS, and a durable execution claim so competing local
processes cannot both perform the same resumed model/tool work. It is not a remote or geographically
distributed database.

Execution claims deliberately fail closed after a crash: ordinary `execute` and `resume` reject
every existing lease, including an expired one, so a tool side effect is never replayed merely
because 24 hours passed. Rust store implementations expose
`SessionStore::recover_expired_execution_lease` as a manual primitive. It only transfers a
parseable, expired claim and performs no model or tool work. The recovery caller must first prove
the old owner stopped, reconcile possibly completed external effects, and use downstream
idempotency keys before committing or explicitly retrying. Every acquire/recovery receives a
store-generated 128-bit fencing token; commit and release compare that token, so reusing an owner
label cannot let an old worker affect a newer lease. Built-in stores keep the session revision
unchanged during acquisition/recovery/release; only the final session commit advances it. The
default third-party-store fallback embeds the claim in session metadata and therefore consumes CAS
revisions. Custom stores that override lease methods use the doc-hidden `SessionExecutionLease`
store-author API to issue a secure claim, persist and compare its owner/token/deadline, then consume
its session. `clear_expired_execution_lease` atomically validates and removes an expired claim after
reconciliation. Python `recover_expired_session(..., side_effects_reconciled=True)` and Node
`recoverExpiredSession(..., true)` expose only that no-execution operation; retry/resume remains a
separate call.

## Deterministic evaluation gates

`EvalDataset` stores strict, bounded JSON cases suitable for version control. `evaluate_outcome`
checks canonical final text, terminal status, ordered or exact tool trajectories, failed tool
results, turns, usage, and model-attempt limits without calling a judge model. `aikit eval` runs
datasets sequentially with `mock-1` by default, emits text/JSON/JSONL reports without copying model
output or provider metadata, and returns exit code `4` when a completed dataset has failed gates.
Non-mock cases require the explicit `--allow-live` acknowledgement before any provider is built.
Live suites additionally enforce aggregate case, input-byte, requested output-token, and wall-time
budgets.
Reports carry schema/runtime versions, the exact dataset SHA-256, and model-attempt provenance;
provider/runtime failures remain distinct from gate regressions at the process exit boundary.
Text, tool, error, and turn gates inspect only messages produced after the recorded invocation
boundary, so resumed conversation history cannot satisfy a current-run gate. Legacy/manual
outcomes without that boundary may still use status/usage gates, but message-derived gates fail
closed.
See [`EVALUATIONS.md`](EVALUATIONS.md).

## MCP, Web, Browser, and compaction

MCP supports stdio and Streamable HTTP, lifecycle initialization, caller-owned bearer auth,
paginated tools/resources/prompts, resource reads, prompt retrieval, and governed tool execution.
Transport responses are capped at 4 MiB before JSON decoding. One discovery operation is limited
to 128 pages, 10,000 incoming items, 8 MiB of serialized items, 4 KiB per cursor, and 64 KiB of
cumulative cursor data; repeated cursors cannot loop forever. Each connection may apply an exact,
case-sensitive tool filter on every page before the
advertised-tool cache is populated. Omitting `allow` preserves allow-all; an explicit empty
`allow` exposes nothing; `deny` always wins. Empty, over-128-character,
control/bidirectional-formatting-character, and duplicate filter names are rejected, with at most
1,024 total entries per filter. Hidden tools are
neither retained as raw discovery values, advertised, nor callable through the MCP executor, and
Python/Node reject unknown filter fields instead of ignoring misspellings. This adopts the
per-server filtering idea documented by
the [OpenAI Agents SDK MCP integration](https://openai.github.io/openai-agents-python/mcp/) and
enforces it again at execution rather than treating discovery filtering as presentation only.
Web requires an exact HTTPS host allowlist, standard port 443, public-only DNS resolution pinned
for each request, validated redirect hops, bounded responses, and no implicit environment proxy.
Browser drives an existing W3C WebDriver session, but registration is denied unless the caller
explicitly asserts that an external proxy, BiDi interceptor, or equivalent pre-request boundary
already enforces the exact host allowlist and public-only IP policy. Rust passes
`BrowserEgressPolicy::ExternallyEnforced`; Python passes the required keyword
`external_egress_enforced=True`; Node passes `{ externalEgressEnforced: true }`. These values are
assertions, not switches that install or verify enforcement. Current-URL checks remain
defense-in-depth postconditions. Browser URL/query/selector/type/session inputs and WebDriver JSON
responses are bounded; failure payloads are redacted. Opt-in deterministic compaction preserves
the task anchor, recent tail, and tool pairs.

## Containment

The containment policy applies to built-in Bash. It combines environment scrubbing, timeout,
output limits, rlimits, process-group termination, and one OS boundary:

- macOS: an actively probed Seatbelt profile;
- Linux: actively probed namespaces, read-only host root, writable workspace, private temp, and a
  seccomp deny filter through bubblewrap;
- Windows: suspended child assignment to a kill-on-close Job Object with a process limit and a
  memory limit where the host permits nested job-memory accounting;
  filesystem and network isolation are explicitly not claimed;
- macOS/Linux/Windows hosts with Docker: a local digest-pinned image, no network, read-only root,
  dropped capabilities, non-root user, bounded resources, and Docker's default seccomp profile;
- unsupported/unready required backend: deny before process launch.

Read/Write/Edit/Glob/Grep use the in-process path jail. Host callbacks and custom Rust executors are
outside this OS boundary. See [`THREAT-MODEL.md`](THREAT-MODEL.md) for guarantees and exclusions.
The Python/Node surfaces expose the jailed file suite separately from Bash and can configure a
digest-pinned Docker fallback, but never expose uncontained Bash. The descriptor-relative jail
remains Linux/macOS-only; Windows file operations fail closed even though Bash can use Job Objects.

## Cross-language conformance

The conformance gate runs canonical modules through Rust, Python, and Node and compares normalized
JSON byte for byte. It covers governance/hooks, structured streaming and repair, run options and
typed terminal errors, state/audit/provider metadata, orchestration/session/shared deadlines, and
the public built-in-tool and multimodal/routed-input contracts. Platform-specific paths,
timestamps, run ids, and selected containment backend names are removed; their security invariants
are asserted instead.

## Remaining v1 parity gates

- Live PostgreSQL failover proof and a deployed Temporal SDK worker beyond the implemented
  transactional PostgreSQL store and deterministic Temporal mapping.
- Model-generated/two-pass summaries beyond deterministic extractive compaction.
- MCP/A2A/ACP network transports and official conformance beyond the governed protocol mappings.
- Stronger Windows filesystem/network isolation beyond Job Objects.
- Built-in LLM risk judge (the trait + heuristic scorer ship now; hosts can plug their own).
- Transparent egress enforcement for arbitrary child processes beyond explicit brokered HTTP and
  browser calls.
- A production Firecracker backend, live-accepted multimodal model profiles, persistent fuzz/chaos
  jobs, signed multi-platform artifacts, public registry publication, and rollback rehearsal.

These are not silently represented as current capabilities. Row-level status and evidence live in
[`PARITY-MATRIX.md`](PARITY-MATRIX.md).
