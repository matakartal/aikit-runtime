# aikit v1 feature reference

This document describes the implementation candidate in this repository. It is not a claim that
packages have been published or that changing live provider APIs have been validated.

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

These are capability declarations and tested adapter behavior, not a promise that every model in
a provider's catalog supports every feature. The caller still chooses a compatible model.
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
Python and Node expose explicit memory-file/namespace and session-file selection; their defaults
remain in-memory. Reopening the same local files restores explicit memories and resumable subagent
sessions, while namespace and revision checks remain enforced by the Rust core.

## Containment

The containment policy applies to built-in Bash. It combines environment scrubbing, timeout,
output limits, rlimits, process-group termination, and one OS boundary:

- macOS: an actively probed Seatbelt profile;
- macOS/Linux/Windows hosts with Docker: a local digest-pinned image, no network, read-only root,
  dropped capabilities, non-root user, bounded resources, and Docker's default seccomp profile;
- unsupported/unready required backend: deny before process launch.

Read/Write/Edit/Glob/Grep use the in-process path jail. Host callbacks and custom Rust executors are
outside this OS boundary. See [`THREAT-MODEL.md`](THREAT-MODEL.md) for guarantees and exclusions.
The Python/Node surfaces expose the jailed file suite separately from Bash and can configure a
digest-pinned Docker fallback, but never expose uncontained Bash. The descriptor-relative jail is
currently Linux/macOS-only; other hosts fail closed.

## Cross-language conformance

The conformance gate runs canonical modules through Rust, Python, and Node and compares normalized
JSON byte for byte. It covers governance/hooks, structured streaming and repair, run options and
typed terminal errors, state/audit/provider metadata, orchestration/session/shared deadlines, and
the public built-in-tool and multimodal/routed-input contracts. Platform-specific paths,
timestamps, run ids, and selected containment backend names are removed; their security invariants
are asserted instead.

## Deferred after v1

- Full MCP client support and a built-in Web tool.
- Distributed durable session backends and advanced context compaction.
- LiteLLM/long-tail provider adapters.
- WASM/browser support.
- Native Linux namespace/seccomp launch and Windows sandbox/job-object backends beyond the current
  Seatbelt/Docker selection.

These are not silently represented as current capabilities.
