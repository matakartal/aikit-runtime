# aikit Python binding

This package is the PyO3 binding for the local aikit Rust workspace. It exposes the same
canonical agent, streaming, structured-output, routing, memory, governance, and hook behavior as
the Rust core, with type information in `aikit.pyi` / `py.typed`.

> The PyPI distribution name is **`aikit-runtime`**; the existing bare `aikit` package on PyPI is
> unrelated. Python imports remain `import aikit`. No PyPI publication is currently claimed or
> planned; local artifact assembly uses this name only for package-layout verification.

## Build from this checkout

```bash
# from the repository root
python3 -m venv .venv
.venv/bin/pip install "maturin==1.14.1"
.venv/bin/maturin develop --manifest-path crates/aikit-py/Cargo.toml --locked
```

Smoke test with the deterministic mock provider (no API key, no network):

```bash
.venv/bin/python examples/python/agent_governance.py
.venv/bin/python examples/python/run_options.py
```

## Quick start

```python
import asyncio
import aikit

async def main() -> None:
    agent = aikit.Agent.from_env({})
    answer = await agent.generate_text("Say hello in Turkish")
    print(answer["text"])

asyncio.run(main())
```

## Tools, permissions, and approval

```python
agent = aikit.Agent.from_env({})

async def lookup(payload: dict) -> str:
    return f"price:{payload['symbol']}"

agent.add_tool(
    "lookup",
    "Look up one market symbol",
    {
        "type": "object",
        "properties": {"symbol": {"type": "string"}},
        "required": ["symbol"],
        "additionalProperties": False,
    },
    lookup,
)

agent.set_permissions([
    {"id": "approve-lookups", "effect": "ask", "tool": "lookup"},
])

async def approve(request: dict):
    return "allow" if request["input"]["symbol"] == "AAPL" else "deny"

agent.can_use_tool(approve)
```

Permission decisions, hooks, and tool execution policy live in Rust. Python only supplies host
callbacks and structured configuration.

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

Add `validator=async_fn` for business rules that JSON Schema cannot express. It receives the raw
schema-valid JSON value and returns `"accept"`, `{"action": "retry", "reason": "..."}`, or
`{"action": "reject", "reason": "..."}`. Retry is bounded by `max_retries`; exceptions fail
closed before Pydantic materialization. Decision objects are exact: aliases, unknown fields,
conflicting keys, and a reason on `accept` are rejected.
The core rejects more than 32 repair retries and truncates normalized reasons to 1,024 bytes. It
does not add a timeout around the Python callback; wrap slow or remote validation in an
application-owned timeout and keep the callback pure/idempotent.

Unsupported media is rejected with a typed error instead of being silently dropped.

## Deterministic outcome evaluation

Evaluate a completed `RunOutcome` without calling a model, tool, filesystem, or network service:

```python
verdict = aikit.evaluate_outcome(stream.outcome(), [
    {"type": "terminal_status", "status": "completed"},
    {"type": "no_tool_errors"},
    {"type": "max_total_tokens", "value": 2_000},
])
assert verdict["passed"]
```

The gate contract is the same versioned JSON contract used by `aikit eval`. Unknown outcome or
gate fields fail closed. Verdict messages report only lengths, counts, and states; they do not
copy raw model output. Text, tool, and turn gates require the runtime-recorded
`invocation_start_message_index`, so earlier conversation history cannot satisfy the current run;
legacy outcomes without that field can still use terminal-status and usage gates.

## Production state (opt-in)

```python
agent.configure_jsonl_audit("./aikit-audit.jsonl")  # metadata_only + fail_closed
agent.use_memory_file("./aikit-memory.json", namespace="tenant-a")
agent.use_session_file("./aikit-sessions.json")
```

Memory is written only by `remember()`. File-backed sessions and memory can be reopened later in
another process; concurrent coordination is process-local unless you use the SQLite stores.

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

## Errors and streams

Async generation and terminal structured-stream failures raise `aikit.AikitError` (or surface a
stable `code` / redacted `info` envelope — see type stubs). Prefer branching on `code` rather than
message text.

Unknown top-level or nested run-option fields are rejected, so a misspelled budget or retry limit
cannot be ignored silently. Assembled Linux wheels target glibc 2.28 or newer; musl is unsupported.

## Conformance

Cross-language parity is enforced by:

```bash
./scripts/parity-check.sh
```

See the root [README](../../README.md), [feature reference](../../docs/FEATURES.md), and
[threat model](../../docs/THREAT-MODEL.md). For ownership and upgrade details, see the
[architecture](../../docs/ARCHITECTURE.md), [0.2 migration guide](../../docs/MIGRATING-0.2.md),
and [evaluation guide](../../docs/EVALUATIONS.md).

## Documentation map

| Guide | Purpose |
|---|---|
| [Root README](../../README.md) | Project overview and multi-language quick start |
| [Architecture](../../docs/ARCHITECTURE.md) | Core ownership, run lifecycle, state, and trust boundaries |
| [Feature reference](../../docs/FEATURES.md) | Full capability and governance reference |
| [Threat model](../../docs/THREAT-MODEL.md) | Containment guarantees and exclusions |
| [0.2 migration](../../docs/MIGRATING-0.2.md) | Breaking source-preview changes and upgrade checklist |
| [Evaluation guide](../../docs/EVALUATIONS.md) | Dataset, gate, report, and CI contracts |

Licensed under MIT OR Apache-2.0; both license texts are included in the package.
