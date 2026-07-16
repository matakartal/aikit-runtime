# aikit v1 feature reference

This document describes the source-first implementation in this repository. Public registry
packages are not distributed, and changing live provider APIs are not claimed as validated.

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

`RunRecorder` stores canonical messages, usage, terminal status, stop reason, model attempts, raw
provider metadata, and the optional final-text projection. `InMemorySessionStore` and
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
SQLite uses WAL and transactions for cross-process local persistence and optimistic CAS. It is not
a remote or geographically distributed database.

## MCP, Web, Browser, and compaction

MCP supports stdio and Streamable HTTP, lifecycle initialization, caller-owned bearer auth,
paginated tools/resources/prompts, resource reads, prompt retrieval, and governed tool execution.
Web requires an exact HTTPS host allowlist and bounded responses. Browser drives an existing W3C
WebDriver session with the same navigation allowlist. Opt-in deterministic compaction preserves
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

## Deferred after v1

- Remote/distributed database adapters beyond transactional local SQLite.
- Model-generated/two-pass summaries beyond deterministic extractive compaction.
- MCP server mode and WASM/browser-runtime packaging.
- Stronger Windows filesystem/network isolation beyond Job Objects.
- Built-in LLM risk judge (the trait + heuristic scorer ship now; hosts can plug their own).
- Network-egress allowlist proxy for contained Bash beyond Docker `--network=none`.
- microVM containment backends, durable checkpoint resume, and ACP/A2A protocol surfaces.

These are not silently represented as current capabilities.
