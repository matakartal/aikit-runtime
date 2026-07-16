"""Phase 0 FFI spike — proves the two hard cross-language seams end to end.

Run (after `maturin develop` in the aikit-py crate's venv):

    python examples/python/spike.py

It uses the in-memory MockProvider (no API key, deterministic), so all it exercises is the
FFI architecture:
  1. streaming OUT   — Rust tokio loop -> Python `async for`
  2. tool callback IN — Rust loop awaits a Python `async def` across the boundary
"""

import asyncio

import aikit


class Tool:
    """Minimal tool object: metadata attributes + an async __call__(input_dict) -> str.

    The real SDK will provide an `@tool` decorator; the spike only needs the shape the
    Rust binding reads (name / description / input_schema / async callable).
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
    return f"[db] '{q}' için 3 sonuç bulundu"


async def main():
    tool = Tool(
        name="search_db",
        description="veritabanında ara",
        input_schema={"type": "object", "properties": {"q": {"type": "string"}}},
        fn=search_db,
    )

    saw_tool_call = False
    saw_tool_result = False
    final_text = ""

    async for ev in aikit.query("veritabanında merhaba ara", tools=[tool], model="mock-1"):
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

    assert saw_tool_call, "seam 1 FAIL: hiç tool_call gelmedi"
    assert saw_tool_result, "seam 2 FAIL: Python tool çalışmadı / sonucu dönmedi"
    assert "tamamladım" in final_text, "loop ikinci turdan sonra final metni üretmedi"

    print("\n\nSPIKE OK ✅  — iki FFI geçişi de çalıştı:")
    print("  1) stream-out:      Rust tokio loop -> Python async for")
    print("  2) tool-callback-in: Rust loop -> Python async def -> sonuç geri Rust'a")


if __name__ == "__main__":
    asyncio.run(main())
