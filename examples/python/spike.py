"""Phase 0 FFI spike: the two hard cross-language seams, end to end (historical, still in CI).

Run from the repository root after building the binding (see examples/python/README.md):

    .venv/bin/python examples/python/spike.py

Keyless and offline: it uses the deterministic in-memory mock model (`mock-1`), so all it
exercises is the FFI architecture:
  1. streaming OUT    -- Rust tokio loop -> Python `async for`
  2. tool callback IN -- Rust loop awaits a Python `async def` across the boundary

New to aikit? Start with examples/python/quickstart.py instead; this file keeps the original
milestone shape (a hand-written tool class and the low-level `aikit.query` function). See
docs/PHASE-0-SPIKE.md for the background.
"""

import asyncio

import aikit


class Tool:
    """Minimal tool object: metadata attributes + an async __call__(input_dict) -> str.

    The binding now also ships an `aikit.tool(...)` decorator (see quickstart.py); the spike
    keeps the bare shape the Rust binding reads (name / description / input_schema / async
    callable) to show that any object with those attributes works.
    """

    def __init__(self, name, description, input_schema, fn):
        self.name = name
        self.description = description
        self.input_schema = input_schema
        self._fn = fn

    async def __call__(self, tool_input):
        return await self._fn(tool_input)


async def search_db(tool_input):
    # A real async tool: awaits (simulated I/O) and returns a string result.
    await asyncio.sleep(0.01)
    q = tool_input.get("q")
    return f"[db] 3 results for '{q}'"


async def main():
    tool = Tool(
        name="search_db",
        description="search the database",
        input_schema={"type": "object", "properties": {"q": {"type": "string"}}},
        fn=search_db,
    )

    saw_tool_call = False
    saw_tool_result = False
    final_text = ""

    async for ev in aikit.query("search the database for merhaba", tools=[tool], model="mock-1"):
        kind = ev.get("type")
        if kind == "message_start":
            print(f"[message_start] model={ev['model']}")
        elif kind == "text_delta":
            final_text += ev["text"]
            print(ev["text"], end="", flush=True)
        elif kind == "tool_call_start":
            saw_tool_call = True
            print(f"\n[tool_call_start] {ev['name']}")
        elif kind == "tool_call_input":
            print(f"[tool_call_input] {ev['input']}")
        elif kind == "tool_result":
            saw_tool_result = True
            print(f"[tool_result] {ev['content']}")
        elif kind == "usage":
            print(f"[usage] in={ev['input_tokens']} out={ev['output_tokens']}")
        elif kind == "message_stop":
            print(f"[message_stop] {ev['stop_reason']}")
        elif kind == "error":
            raise RuntimeError(f"stream error: {ev['message']}")

    assert saw_tool_call, "seam 1 FAIL: no tool_call was streamed"
    assert saw_tool_result, "seam 2 FAIL: the Python tool did not run or return its result"
    # The mock model's canned final answer is Turkish ("... görevi tamamladım" = "task done").
    assert "tamamladım" in final_text, "the loop produced no final text after the second turn"

    print("\n\nSPIKE OK ✅  — both FFI seams work:")
    print("  1) stream-out:      Rust tokio loop -> Python async for")
    print("  2) tool-callback-in: Rust loop -> Python async def -> result back into Rust")


if __name__ == "__main__":
    asyncio.run(main())
