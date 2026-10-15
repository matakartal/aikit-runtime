# aikit Python binding

This package is the PyO3 binding for the local aikit Rust workspace. It exposes the same
canonical agent, streaming, structured-output, routing, memory, governance, and hook behavior as
the Rust core, with type information in `aikit.pyi` / `py.typed`. Permission decisions, hooks,
budgets, and tool execution policy are evaluated in Rust; Python supplies host callbacks and
structured configuration.

> aikit is `v0.3.0-alpha.1` and source-first: build the binding from this checkout. The PyPI
> distribution name is reserved as **`aikit-runtime`** (the existing bare `aikit` package on PyPI
> is unrelated); Python imports remain `import aikit`. No PyPI publication is claimed for this
> candidate; local artifact assembly uses this name only for package-layout verification.

## Build from this checkout

Prerequisites: Rust 1.88+ (`cargo`), a C/C++ toolchain, and Python 3.9+. From the repository
root:

```bash
python3 -m venv .venv
.venv/bin/pip install --require-hashes -r requirements/maturin.txt   # pins maturin 1.14.1
.venv/bin/maturin develop --manifest-path crates/aikit-py/Cargo.toml --locked
```

`maturin develop` compiles the Rust core and installs `aikit` into `.venv`. Re-run it after
pulling or editing Rust code. An `AttributeError` for a name that `aikit.pyi` declares usually
means the installed build is stale.

Smoke test with the deterministic mock provider (no API key, no network):

```bash
.venv/bin/python examples/python/quickstart.py
```

[`examples/python/`](../../examples/python/README.md) lists the other runnable examples.

## Quick start

```python
import asyncio
import aikit

async def main() -> None:
    agent = aikit.Agent.from_env({})   # keyless: only the deterministic mock models
    result = await agent.generate_text("Say hello in one short sentence.")
    print(result["text"])
    print(result["usage"]["input_tokens"], "in /", result["usage"]["output_tokens"], "out")

asyncio.run(main())
```

Without a `model` argument, calls use the keyless `mock-1` model (`mock-structured` for
structured output). The mock answers with fixed Turkish sentences such as
`Araç sonucunu aldım; görevi tamamladım.` and, when tools are registered, calls the first one
with `{"q": "merhaba"}`. That makes every example reproducible offline; it is not a real model.

## Use a real provider

```python
import os

agent = aikit.Agent()                       # reads provider keys from os.environ
# or: agent = aikit.Agent.from_env({"ANTHROPIC_API_KEY": "..."})
# or: agent.add_key(os.environ["OPENAI_API_KEY"], provider="openai")

result = await agent.generate_text("Say hello", model="<a claude-* or gpt-* model id>")
```

The model id picks the provider by prefix: `claude*` (Anthropic), `gpt*` / `o1` / `o3` / `o4`
(OpenAI), `gemini*` (Google), `deepseek*`, `openrouter:`, `groq:`, `mistral:`, and `xai:` /
`grok-`. Recognized key variables are `ANTHROPIC_API_KEY`, `OPENAI_API_KEY`, `DEEPSEEK_API_KEY`,
`GEMINI_API_KEY` / `GOOGLE_API_KEY`, `OPENROUTER_API_KEY`, `GROQ_API_KEY`, `MISTRAL_API_KEY`, and
`XAI_API_KEY`. `add_key` needs an explicit `provider` for `sk-` keys, which could be OpenAI or
DeepSeek. A model whose provider has no key raises `aikit.AikitError` with
`code == "provider_auth"`; aikit never silently falls back to the mock.

## Streaming and run options

```python
async with agent.run(
    "Summarize the release notes",
    {"model": "mock-1", "max_turns": 4, "budget": {"max_total_tokens": 20_000}},
) as stream:
    async for delta in stream:
        if delta["type"] == "text_delta":
            print(delta["text"], end="", flush=True)
        elif delta["type"] == "error":
            print("\nrun error:", delta["info"]["code"])

outcome = stream.outcome()          # terminal RunOutcome
print(outcome["terminal_status"])   # "completed", "budget_exceeded", "max_turns", ...
```

`agent.run` accepts the full `RunOptions` surface (`model`, `fallback_models`, `max_tokens`,
`max_turns`, `provider_options`, `compatibility_mode`, `budget`, `retry`, `routing`,
`compaction`). `agent.stream_text(prompt, model=...)` is the short form without options.
Unknown top-level or nested option fields raise `ValueError`, so a misspelled budget or retry
limit cannot be ignored silently.

Use `async with` (or `await stream.aclose()`) when you might leave the loop early: a plain
`break` does not close a custom async iterator, and closing is what runs Stop hooks, audit
emission, and recorder finalization. Failures after a run has started arrive as an `"error"`
delta and in `outcome()["terminal_status"]`; they are not raised from the loop.

## Structured output

```python
from pydantic import BaseModel

schema = {
    "type": "object",
    "properties": {"city": {"type": "string"}},
    "required": ["city"],
    "additionalProperties": False,
}
obj = await agent.generate_object("Where is the Eiffel Tower?", schema)
print(obj["value"], obj["fidelity"], obj["attempts"])

class Place(BaseModel):
    city: str

typed = await agent.generate_object("Where is the Eiffel Tower?", Place)
assert isinstance(typed["value"], Place)    # Pydantic v2 classes are materialized
```

Pydantic is optional; any class with `model_json_schema()` and `model_validate()` works.
`agent.stream_object(...)` takes the same arguments and yields `attempt_started`, `delta`, and
`validation_failed` events before exactly one `completed` event. Once repair attempts are
exhausted, `generate_object` raises `AikitError` with `code == "structured_output"`.

Add `validator=async_fn` for business rules that JSON Schema cannot express. It receives the raw
schema-valid JSON value and returns `"accept"`, `{"action": "retry", "reason": "..."}`, or
`{"action": "reject", "reason": "..."}`. Retry is bounded by `max_retries`; exceptions fail
closed before Pydantic materialization. Decision objects are exact: aliases, unknown fields,
conflicting keys, and a reason on `accept` are rejected.
The core rejects more than 32 repair retries and truncates normalized reasons to 1,024 bytes. It
does not add a timeout around the Python callback; wrap slow or remote validation in an
application-owned timeout and keep the callback pure/idempotent.

## Tools, permissions, and approval

```python
@aikit.tool(
    "lookup",
    "Look up one market symbol",
    {
        "type": "object",
        "properties": {"symbol": {"type": "string"}},
        "required": ["symbol"],
        "additionalProperties": False,
    },
)
async def lookup(tool_input: dict) -> str:
    return f"price:{tool_input['symbol']}"

agent.add_tool_definition(lookup)   # or agent.add_tool(name, description, schema, callback)

agent.set_permissions([
    {"id": "approve-lookups", "effect": "ask", "tool": "lookup"},
])

async def approve(request: "aikit.ApprovalRequest") -> "aikit.ApprovalResponse":
    tool_input = request["input"]
    allowed = isinstance(tool_input, dict) and tool_input.get("symbol") == "AAPL"
    return "allow" if allowed else "deny"

agent.can_use_tool(approve)
```

Every model-proposed input is validated against the tool's JSON Schema before the callback
runs, and a tool callback must resolve to `str`. `"ask"` rules route through the
`can_use_tool` callback; without one they fail closed. An approver that raises or returns a
malformed decision denies the call, and the model receives an error tool result instead.
Lifecycle hooks (`on_user_prompt`, `on_pre_tool_use`, `on_post_tool_use`,
`on_post_tool_failure`, `on_failure`, `on_stop`) follow the same strict-response rules.

Names such as `aikit.ApprovalRequest`, `aikit.RunOptions`, and `aikit.RunOutcome` are
declared in `aikit.pyi` for type checkers but are not runtime attributes. Quote them in
annotations, as above, or add `from __future__ import annotations`.

## Governed A2A mapper

`aikit.A2aMapper` exposes the shared Rust mapper for owner-scoped contexts/tasks, idempotent
messages, task listing, cancellation decisions, snapshot, and restore. Its snapshots are internal
persistence state, not official A2A response objects, and callers should follow the exported
schema version instead of hard-coding an old number. The Python class does not start an A2A
HTTP or gRPC listener; the bounded experimental wire listener currently lives on the Rust host
side. See the [A2A conformance guide](../../docs/A2A-CONFORMANCE.md) for the exact tested
boundary.

## MCP tool visibility

Filter each MCP connection before registering it with an agent:

```python
server = await aikit.connect_mcp_http(
    "https://mcp.example.com",
    "work",
    tool_filter={
        "allow": ["search", "read_file"],
        "deny": ["read_file"],  # deny is authoritative
    },
)
agent.register_mcp(server)
```

Remote HTTP endpoints must use HTTPS; plain HTTP is accepted only for `localhost` / loopback.
`aikit.connect_mcp_stdio(program, args, name)` spawns a local server without a shell, and the
child does not inherit your environment (or its API keys) unless you pass `inherit_env=True`.
Connection, discovery, and transport failures raise `RuntimeError`.

Names match exactly and case-sensitively. Omit `allow` for the backward-compatible allow-all
default, or pass `"allow": []` to expose no tools. Unknown fields, duplicate/empty names, and names
over 128 characters are rejected; each filter accepts at most 1,024 entries. Filtered tools are not
advertised or executable. Discovery and transport also fail closed on bounded page, item, byte,
cursor, and response limits instead of retaining unbounded server data: 128 pages, 10,000 incoming
items, 8 MiB of serialized items, 4 KiB per cursor, 64 KiB cumulative cursors, and 4 MiB per
transport response/stdio line.

## Multimodal and structured input

Every text and object surface accepts a string or canonical message history:

```python
messages = [{
    "role": "user",
    "content": [
        {"type": "text", "text": "Describe this chart"},
        {
            "type": "media",
            "media_type": "image/png",
            "source": {"kind": "url", "url": "https://example.com/chart.png"},
        },
    ],
}]

result = await agent.generate_text(messages, model="your-model")
obj = await agent.generate_object(messages, schema)
```

Unsupported media is rejected with a typed error instead of being silently dropped.
Credential-free absolute HTTP(S) URLs are valid canonical references and round-trip unchanged, but
provider dispatch rejects unresolved URL/artifact references until a trusted host resolver verifies
their bytes, size, and SHA-256. MIME type matching at provider boundaries is case-insensitive.

Canonical messages also accept `{"type": "media_input", "media": ...}` blocks. This strict form
preserves MIME, size, SHA-256, bytes/URL/artifact identity and is preferred over legacy `media`.

Provider-specific options use `compatibility_mode="strict"` by default. Unknown or unsupported
parameters fail with a typed error. `"warn"` and `"best_effort"` are explicit opt-ins and preserve
every `ProviderWarning` in both stream warning deltas and completed result/outcome `warnings`.

## Deterministic outcome evaluation

Evaluate a completed `RunOutcome` without calling a model, tool, filesystem, or network service:

```python
verdict = aikit.evaluate_outcome(stream.outcome(), [
    {"type": "terminal_status", "status": "completed"},
    {"type": "no_tool_errors"},
    {"type": "max_total_tokens", "value": 2_000},
])
assert verdict["passed"], verdict["checks"]
```

The gate contract is the same versioned JSON contract used by `aikit eval`. Unknown outcome or
gate fields fail closed. Verdict messages report only lengths, counts, and states; they do not
copy raw model output. Text, tool, and turn gates require the runtime-recorded
`invocation_start_message_index`, so earlier conversation history cannot satisfy the current run;
legacy outcomes without that field can still use terminal-status and usage gates.

## Offline catalog, policy evidence, and durable HITL

The reviewed model catalog is compiled into the package and never performs runtime discovery:

```python
catalog = aikit.shipped_model_catalog()
resolved = aikit.resolve_model_catalog([my_profile])
state = aikit.model_capability_state(resolved["profiles"][0], "tool_use")
```

Overrides remain a separate, hashed layer. `validate_model_profile`, `validate_media_input`, and
`validate_media_artifact` fail before provider I/O. Completed OPA and Cedar evaluator responses can
be normalized with `normalize_opa_decision` and `normalize_cedar_decision`; undefined/partial OPA
results and Cedar forbids/diagnostic errors fail closed. aikit does not call OPA or Cedar itself.

`DurableRun` is an append-only, replay-validated state machine. It persists nothing on its own:
store `snapshot()` wherever you like and rebuild with `DurableRun.from_state`.

```python
run = aikit.DurableRun("session-1", "run-1")
approval_id = run.request_confirmation("deploy", "Deploy to production?")   # pauses the run
saved = run.snapshot()                    # persist this, e.g. as JSON
run = aikit.DurableRun.from_state(saved)  # later, possibly in another process
run.resolve_approval("resume-1", approval_id, approved=True)
assert run.status == "running"
```

Invalid transitions raise `AikitError` with `code == "conflict"`. The typed approval helpers
(confirmation, missing input, output review, and edit/retry) create non-expiring approvals for
legacy compatibility. For restart-safe deadlines and canonical approval kinds, call
`request_typed_approval`, then `resolve_approval_at`/`apply_command_at` with an explicit trusted
timestamp. `expire_approvals` appends fail-closed timeout denials idempotently.
`seal_policy_snapshot` plus `DurableRun.with_policy_snapshot` pins a complete run-scoped
governance binding before mutable work. For tenant/agent scoping, use `seal_governance_binding`
and `DurableRun.with_governance_binding`; the sealed binding is replay-validated and propagated
to typed approvals.

## Production state (opt-in)

```python
agent.configure_jsonl_audit("./aikit-audit.jsonl")  # metadata_only + fail_closed
agent.use_memory_file("./aikit-memory.json", namespace="tenant-a")
agent.use_session_file("./aikit-sessions.json")
```

Memory is written only by `remember()` / `remember_cas()`. File-backed sessions and memory can be
reopened later in another process; concurrent coordination is process-local unless you use the
SQLite stores (`use_sqlite_memory`, `use_sqlite_sessions`).

An expired execution lease never replays automatically. After verifying the old worker stopped and
reconciling/idempotently checking its external effects, clear only the expired lease explicitly:

```python
revision = agent.recover_expired_session(
    "session-id",
    side_effects_reconciled=True,
)
# A later run_subagent/resume_subagent call is a separate execution decision.
```

## Built-in tools and containment

```python
agent.register_builtin_tools(["/srv/workspace", "/srv/shared"])

# Separate explicit opt-in. Always Required(Auto) — no uncontained Bash from Python.
agent.enable_bash_with_required_containment({
    # Optional Docker fallback. Image must already exist locally and be digest-pinned.
    "image": "registry.example/aikit-shell@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
})
report = await agent.builtin_containment_capabilities()
assert report.get("selected_backend"), report
```

Jailed file tools reject root escapes and symlinks. Windows currently lacks the descriptor-relative
file jail, so registration fails closed there. Host callbacks run in the Python process and are
**not** covered by built-in Bash OS containment.

## Errors

| Exception | When | What to do |
|---|---|---|
| `aikit.AikitError` (a `RuntimeError`) | Awaited generation fails; a run cannot start (for example `provider_auth`); an `ObjectStream` fails; a durable transition is invalid (`conflict`) | Branch on `error.code`; `error.info` carries `provider`, `model`, `status`, `retryable`, and `retry_after_ms` |
| `ValueError` | Malformed host input: unknown option fields, invalid schemas, messages, permission rules, or keys | Fix the call; nothing was sent to a provider |
| `RuntimeError` | MCP connection or transport failures; memory store failures | Inspect the message; retry if the remote side was at fault |

```python
try:
    result = await agent.generate_text("hello", model="claude-sonnet-4-5")
except aikit.AikitError as error:
    if error.code == "provider_auth":
        ...  # export the provider key or call agent.add_key(...)
    elif error.info["retryable"]:
        ...  # back off for error.info["retry_after_ms"] and retry
    else:
        raise
```

Prefer branching on `code` rather than message text; the stable codes are listed as
`ErrorCode` in `aikit.pyi`. Stream failures after a run has started arrive as `"error"` deltas
instead of exceptions (see [Streaming and run options](#streaming-and-run-options)). The
external-agent helpers put the code in the message as a `"<code>: "` prefix rather than in
`error.code`.

### Troubleshooting

| Symptom | Likely cause and fix |
|---|---|
| `ModuleNotFoundError: No module named 'aikit'` | The binding is not installed in the interpreter you ran. Use `.venv/bin/python`, or re-run `maturin develop` with that venv. |
| `AttributeError` for a function shown in `aikit.pyi` | Stale build. Re-run `maturin develop --manifest-path crates/aikit-py/Cargo.toml --locked`. |
| `AttributeError: module 'aikit' has no attribute 'RunOptions'` (or another TypedDict) | Stub-only type used at runtime. Quote the annotation or add `from __future__ import annotations`. |
| `AikitError` with `code == "provider_auth"` | The model's provider has no key. `Agent.from_env({})` deliberately ignores your shell; use `Agent()` or `add_key`. |
| `ValueError: ... unknown field '...'` | A misspelled `RunOptions`, budget, retry, or decision field. Compare with the TypedDicts in `aikit.pyi`. |
| `ValueError` from `add_key` | Ambiguous `sk-` key or unrecognized format. Pass `provider="openai"` (or another listed provider). |
| A mock run reports a tool error | The mock calls the first tool with `{"q": "merhaba"}`; a schema without a `q` field rejects that input. Expected offline. |

Assembled Linux wheels target glibc 2.28 or newer; musl is unsupported.

## API overview

| Area | Entry points |
|---|---|
| Agent and credentials | `Agent()`, `Agent.from_env`, `add_key`, `active_providers`, `has_provider`, `capabilities` |
| Text and streaming | `generate_text`, `stream_text`, `run` → `QueryStream` (`events`, `cancel`, `aclose`, `outcome`), `client()` / `Client.query`, `aikit.query` |
| Structured output | `generate_object`, `stream_object` → `ObjectStream` |
| Tools | `aikit.tool`, `add_tool`, `add_tool_definition`, `register_builtin_tools`, `enable_bash_with_required_containment`, `builtin_containment_capabilities`, `register_web_tools`, `register_browser_tools` |
| MCP | `connect_mcp_http`, `connect_mcp_stdio`, `register_mcp`, `McpConnection` resources and prompts |
| Governance | `set_permissions`, `can_use_tool`, `enable_capability_requests`, `enable_default_guardrails`, `on_*` hooks, `configure_jsonl_audit` |
| Memory and sessions | `remember`, `remember_cas`, `recall`, `use_memory_file`, `use_sqlite_memory`, `use_session_file`, `use_sqlite_sessions`, `recover_expired_session` |
| Routing and orchestration | `route`, `subtask`, `run_subagent`, `resume_subagent`, `fan_out`, `parallel`, `council` |
| Evaluation | `evaluate_outcome`, `evaluate_trace` |
| Catalog and policy | `shipped_model_catalog`, `resolve_model_catalog`, `validate_model_profile`, `model_capability_state`, `seal_policy_snapshot`, `seal_governance_binding`, `normalize_opa_decision`, `normalize_cedar_decision` |
| Durable runs | `DurableRun` (approvals, checkpoints, commands, snapshots) |
| Protocols | `A2aMapper`; external agents: `discover_external_runtime_candidates`, `spawn_external_agent_session`, `ExternalAgentSession`, `ExternalUpdateState` ([guide](../../docs/EXTERNAL-AGENTS.md)) |

Every public name has a typed declaration and docstring in [`aikit.pyi`](aikit.pyi), which
editors show on hover.

## Conformance

Cross-language parity is enforced by:

```bash
./scripts/parity-check.sh
```

## Documentation map

| Guide | Purpose |
|---|---|
| [Root README](../../README.md) | Project overview and multi-language quick start |
| [Getting started](../../docs/GETTING-STARTED.md) | Step-by-step first run, including Python and Node |
| [Python examples](../../examples/python/README.md) | Runnable keyless examples and what each shows |
| [Architecture](../../docs/ARCHITECTURE.md) | Core ownership, run lifecycle, state, and trust boundaries |
| [Feature reference](../../docs/FEATURES.md) | Full capability and governance reference |
| [Threat model](../../docs/THREAT-MODEL.md) | Containment guarantees and exclusions |
| [Competitor parity](../../docs/PARITY-MATRIX.md) | Current evidence, gaps, and v1 gate |
| [0.3 migration](../../docs/MIGRATING-0.3.md) | Streaming, MCP naming, capability and durability changes |
| [Evaluation guide](../../docs/EVALUATIONS.md) | Dataset, gate, report, and CI contracts |

Licensed under MIT OR Apache-2.0; both license texts are included in the package.
