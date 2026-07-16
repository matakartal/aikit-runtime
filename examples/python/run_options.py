"""Keyless binding conformance for RunOptions, Client, and deterministic cancellation."""

import asyncio
import json

from aikit import Agent, Client


async def drain(stream, expected_error_code=None):
    seen_error_code = None
    async for delta in stream:
        if delta.get("type") == "error":
            seen_error_code = delta["info"]["code"]
    if expected_error_code is not None and seen_error_code != expected_error_code:
        raise RuntimeError("Python StreamDelta ErrorInfo drift")
    return stream.outcome()


async def main() -> None:
    agent = Agent.from_env({})
    client_outcome = await drain(
        Client(agent).query(
            "client parity",
            {
                "model": "mock-1",
                "fallback_models": ["mock-2"],
                "max_tokens": 64,
                "max_turns": 2,
                "provider_options": {"mock": {"tag": "client"}},
                "retry": {"max_attempts_per_model": 1},
            },
        )
    )
    max_turns_outcome = await drain(
        agent.run("turn parity", {"max_turns": 0}), "max_turns"
    )
    budget_outcome = await drain(
        agent.run("budget parity", {"budget": {"max_total_tokens": 0}}),
        "budget_exceeded",
    )
    try:
        agent.run("typed error parity", {"model": "not-a-real-model"})
    except RuntimeError as error:
        error_code = error.code
        if error.info["code"] != error_code:
            raise RuntimeError("Python typed AgentError envelope drift")
    else:
        raise RuntimeError("unknown model unexpectedly started")

    before = agent.run("cancel before first pull")
    before.cancel()
    cancel_before_outcome = await before.aclose()

    blocked = Agent.from_env({})
    entered = asyncio.Event()
    stop_reasons = []
    tool_calls = 0

    async def wait_in_hook(_context):
        entered.set()
        await asyncio.Event().wait()

    async def stopped(context):
        stop_reasons.append(context["reason"])

    async def forbidden_tool(_input):
        nonlocal tool_calls
        tool_calls += 1
        return "should not run"

    blocked.on_user_prompt(wait_in_hook)
    blocked.on_stop(stopped)
    blocked.add_tool(
        "forbidden",
        "must not run after cancellation",
        {"type": "object"},
        forbidden_tool,
    )
    during = blocked.run("cancel while UserPrompt is blocked")
    pending = asyncio.ensure_future(during.__anext__())
    await entered.wait()
    during.cancel()
    try:
        await pending
    except StopAsyncIteration:
        pass
    cancel_during_outcome = await during.aclose()

    # Python custom async iterators do not auto-close on `break`; `async with` does.
    break_stream = Agent.from_env({}).run("break finalization")
    async with break_stream:
        async for _delta in break_stream:
            break
    if break_stream.outcome()["terminal_status"] == "running":
        raise RuntimeError("async-with did not finalize QueryStream")

    result = {
        "budget": budget_outcome["terminal_status"],
        "cancel_before": cancel_before_outcome["terminal_status"],
        "cancel_during": cancel_during_outcome["terminal_status"],
        "client": client_outcome["terminal_status"],
        "error_code": error_code,
        "max_turns": max_turns_outcome["terminal_status"],
        "stop_reasons": stop_reasons,
        "tool_calls": tool_calls,
    }
    print("RUN_OPTIONS_JSON=" + json.dumps(result, sort_keys=True, separators=(",", ":")))


asyncio.run(main())
