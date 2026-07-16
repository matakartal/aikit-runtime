# aikit Python binding

This package is the PyO3 binding for the local aikit Rust workspace. It exposes the same
canonical agent, streaming, structured-output, routing, memory, governance, and hook behavior as
the Rust core, with type information in `aikit.pyi` / `py.typed`.

> The PyPI distribution name is **`aikit-runtime`**; the existing bare `aikit` package on PyPI is
> unrelated. Python imports remain `import aikit`. This package remains unpublished until the
> release evidence gates pass.

## Build from this checkout

```bash
# from the repository root
python3 -m venv .venv
.venv/bin/pip install "maturin>=1.5,<2"
.venv/bin/maturin develop --manifest-path crates/aikit-py/Cargo.toml
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

## Production state (opt-in)

```python
agent.configure_jsonl_audit("./aikit-audit.jsonl")  # metadata_only + fail_closed
agent.use_memory_file("./aikit-memory.json", namespace="tenant-a")
agent.use_session_file("./aikit-sessions.json")
```

Memory is written only by `remember()`. File-backed sessions and memory can be reopened later in
another process; concurrent coordination is process-local unless you use the SQLite stores.

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

## Conformance

Cross-language parity is enforced by:

```bash
./scripts/parity-check.sh
```

See the root [README](../../README.md), [feature reference](../../docs/FEATURES.md), and
[threat model](../../docs/THREAT-MODEL.md).

Licensed under MIT OR Apache-2.0; both license texts are included in the package.
