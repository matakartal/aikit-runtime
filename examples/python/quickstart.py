"""Python quickstart: text, streaming, a governed tool, run options, and typed errors.

Run from the repository root after building the binding (see examples/python/README.md):

    .venv/bin/python examples/python/quickstart.py

By default this is keyless and offline: every call goes to the deterministic in-memory mock
model (`mock-1`), so the output is the same on every machine and nothing is billed.

Optional live run (billable, makes network requests): export a provider key and name a model,
for example

    ANTHROPIC_API_KEY=... AIKIT_EXAMPLE_MODEL=<a claude-* model id> \
        .venv/bin/python examples/python/quickstart.py

`aikit.Agent()` discovers provider keys from the process environment (ANTHROPIC_API_KEY,
OPENAI_API_KEY, GEMINI_API_KEY / GOOGLE_API_KEY, DEEPSEEK_API_KEY, ...). The mock-only
assertions below are skipped in live mode, because a real model chooses its own words and tools.
"""

import asyncio
import os
import sys

import aikit

MOCK_MODEL = "mock-1"
# Example-local switch (not an aikit setting): unset means "stay on the keyless mock".
MODEL = os.environ.get("AIKIT_EXAMPLE_MODEL", "").strip() or MOCK_MODEL
LIVE = MODEL != MOCK_MODEL


# A tool is an async callable plus metadata. `aikit.tool` attaches the name, description, and
# JSON Schema; the runtime validates every model-proposed input against that schema before the
# callback runs. (The mock model always proposes `{"q": "merhaba"}` and answers with a fixed
# Turkish sentence, hence the `q` field.)
@aikit.tool(
    "search_notes",
    "Search the user's notes for a phrase",
    {
        "type": "object",
        "properties": {"q": {"type": "string"}},
        "required": ["q"],
        "additionalProperties": False,
    },
)
async def search_notes(tool_input):
    return f"2 notes mention '{tool_input['q']}'"


def section(title):
    print(f"\n== {title}")


async def main():
    # Keyless by default: `from_env({})` ignores the shell, so a stray key cannot change the demo.
    # In live mode `Agent()` reads provider keys from the process environment instead.
    agent = aikit.Agent() if LIVE else aikit.Agent.from_env({})
    print("model:", MODEL, "(live, billable)" if LIVE else "(keyless mock)")
    print("active providers:", agent.active_providers() or "none (mock only)")

    # 1. One-shot text generation. Usage and stop reason come back with the text.
    section("1. generate_text")
    try:
        result = await agent.generate_text("Say hello in one short sentence.", model=MODEL)
    except aikit.AikitError as error:
        # Branch on the stable `code`, never on message text.
        if error.code == "provider_auth":
            sys.exit(
                f"No credential for model {MODEL!r}: {error}.\n"
                "Export the matching provider key (for example ANTHROPIC_API_KEY for claude-*),\n"
                "or unset AIKIT_EXAMPLE_MODEL to run the keyless mock."
            )
        raise
    print("text:", result["text"])
    print("usage:", result["usage"]["input_tokens"], "in /", result["usage"]["output_tokens"], "out")
    print("stop_reason:", result["stop_reason"])

    # 2. Streaming. Each item is a typed delta dict; branch on its "type".
    section("2. stream_text")
    streamed = ""
    async for delta in agent.stream_text("Say hello in one short sentence.", model=MODEL):
        if delta["type"] == "text_delta":
            streamed += delta["text"]
            print(delta["text"], end="", flush=True)
        elif delta["type"] == "error":
            print("\nstream error:", delta["info"]["code"])
    print()

    # 3. A governed tool. The "ask" rule routes every call through the approval callback, which
    #    runs in Rust's tool loop immediately before the side effect. Returning "deny" (or an
    #    exception) means the tool never runs and the model receives an error tool result.
    section("3. governed tool with approval")
    approvals = []

    async def approve(request):
        approvals.append(request["tool"])
        print(f"approval requested for {request['tool']} with input {request['input']}")
        return "allow"

    agent.add_tool_definition(search_notes)
    agent.set_permissions([{"id": "ask-search", "effect": "ask", "tool": "search_notes"}])
    agent.can_use_tool(approve)

    # `run` takes RunOptions. Unknown or misspelled fields raise ValueError instead of being
    #    ignored, so a typo cannot silently disable a budget or turn limit.
    stream = agent.run(
        "Search my notes for 'merhaba'.",
        {"model": MODEL, "max_turns": 4, "budget": {"max_total_tokens": 20_000}},
    )
    async for delta in stream:
        if delta["type"] == "tool_call_start":
            print("model called tool:", delta["name"])
        elif delta["type"] == "tool_result":
            label = "error" if delta["is_error"] else "ok"
            print(f"tool result ({label}):", delta["content"])
    outcome = stream.outcome()
    print("terminal_status:", outcome["terminal_status"])
    print("final_text:", outcome.get("final_text", ""))

    # 4. Deterministic outcome gates: the same contract `aikit eval` uses, no model call needed.
    section("4. evaluate_outcome")
    verdict = aikit.evaluate_outcome(
        outcome,
        [
            {"type": "terminal_status", "status": "completed"},
            {"type": "called_tool", "name": "search_notes"},
            {"type": "no_tool_errors"},
            {"type": "max_total_tokens", "value": 20_000},
        ],
    )
    for check in verdict["checks"]:
        print("PASS" if check["passed"] else "FAIL", check["gate"], "-", check["message"])

    # 5. A live model without its key fails fast with a typed error instead of silently falling
    #    back to the mock. (Demonstrated on a fresh keyless agent so it works in every mode.)
    section("5. typed error for a missing key")
    keyless = aikit.Agent.from_env({})
    try:
        await keyless.generate_text("hello", model="claude-example")
    except aikit.AikitError as error:
        print(f"AikitError code={error.code!r}: {error}")

    if not LIVE:
        assert approvals == ["search_notes"], approvals
        assert verdict["passed"], verdict
        assert streamed == result["text"]

    print("\nQuickstart finished. Next: examples/python/agent_governance.py (hooks, routing,")
    print("structured output, orchestration) and examples/python/run_options.py (RunOptions).")


if __name__ == "__main__":
    asyncio.run(main())
