"""Runtime contract for binding-owned subagent context and resumable sessions."""

import asyncio
import contextvars
import threading

from aikit import Agent, query


PROFILES = [
    {
        "provider": "mock",
        "model": "mock-1",
        "context_window_tokens": 8192,
        "max_output_tokens": 1024,
        "pricing": None,
        "quality_score": 1,
        "skills": [],
        "capabilities": [],
    }
]


def spec(identifier: str):
    return {
        "id": identifier,
        "prompt": f"run {identifier}",
        "system": None,
        "route": {
            "policy": {"kind": "explicit", "model": "mock-1"},
            "max_cost_usd": None,
            "required_skills": [],
            "required_capabilities": [],
        },
        "allowed_tools": ["search"],
        "max_turns": 3,
        "max_tokens": 64,
        "estimated_input_tokens": 8,
    }


def callback_free_streams():
    """Pure-Rust streams can be constructed before an asyncio loop exists."""
    agent = Agent.from_env({})
    streams = [
        query("callback-free top-level query"),
        agent.stream_text("callback-free agent stream"),
        agent.run("callback-free agent run"),
        agent.client().query("callback-free client query"),
    ]

    async def host_tool(_tool_input):
        return "host"

    callback_agent = Agent.from_env({})
    callback_agent.add_tool("host", "host callback", {"type": "object"}, host_tool)
    try:
        callback_agent.run("callbacks still need an active loop")
    except RuntimeError as error:
        assert "active asyncio loop" in str(error)
    else:
        raise AssertionError("callback-backed stream started without an asyncio loop")
    return streams


async def close_streams(streams) -> None:
    for stream in streams:
        await stream.aclose()


def assert_cross_loop_callback_isolation() -> None:
    """A second loop must never retarget callbacks already bound to the first run."""
    tenant = contextvars.ContextVar("aikit_test_tenant", default="missing")
    hook_entered = threading.Event()
    release_hook = threading.Event()
    second_loop_captured = threading.Event()
    release_second_loop = threading.Event()
    agent = Agent.from_env({})
    results: dict[str, str] = {}
    errors: list[BaseException] = []

    async def gate_first_run(context):
        if context["prompt"] == "run in tenant A":
            hook_entered.set()
            await asyncio.to_thread(release_hook.wait)
        return None

    async def tenant_tool(_tool_input):
        return tenant.get()

    async def semantic_validator(_value):
        return "accept"

    agent.on_user_prompt(gate_first_run)
    agent.add_tool(
        "search_db",
        "return the active tenant context",
        {"type": "object"},
        tenant_tool,
    )

    async def run_first_loop() -> None:
        token = tenant.set("A")
        try:
            async for event in agent.run("run in tenant A"):
                if event["type"] == "tool_result" and not event["is_error"]:
                    results["tool_result"] = event["content"]
        finally:
            tenant.reset(token)

    async def capture_second_loop() -> None:
        token = tenant.set("B")
        try:
            pending = agent.generate_object(
                "structured tenant B",
                {
                    "type": "object",
                    "required": ["currency", "status"],
                    "properties": {
                        "currency": {"type": "string"},
                        "status": {"type": "string"},
                    },
                    "additionalProperties": False,
                },
                validator=semantic_validator,
            )
            second_loop_captured.set()
            await asyncio.to_thread(release_second_loop.wait)
            await pending
        finally:
            tenant.reset(token)

    def run(coroutine) -> None:
        try:
            asyncio.run(coroutine())
        except BaseException as error:  # surfaced in the parent test thread below
            errors.append(error)

    first = threading.Thread(target=run, args=(run_first_loop,), daemon=True)
    second = threading.Thread(target=run, args=(capture_second_loop,), daemon=True)
    first.start()
    assert hook_entered.wait(10), "tenant A hook did not start"
    second.start()
    assert second_loop_captured.wait(10), "tenant B loop did not capture callbacks"
    release_hook.set()
    first.join(10)
    release_second_loop.set()
    second.join(10)
    assert not first.is_alive(), "tenant A run did not finish"
    assert not second.is_alive(), "tenant B run did not finish"
    assert not errors, errors
    assert results.get("tool_result") == "A", results


async def main() -> None:
    cleanup_agent = Agent.from_env({})
    cleanup_stops = 0

    async def cleanup_stop(_context):
        nonlocal cleanup_stops
        cleanup_stops += 1

    cleanup_agent.on_stop(cleanup_stop)
    cleanup_stream = cleanup_agent.run("event view cleanup")
    cleanup_events = cleanup_stream.events("event-view-cleanup")
    async with cleanup_events:
        async for _event in cleanup_events:
            break
    assert cleanup_stops == 1, "event stream context did not run Stop finalization"
    assert cleanup_events.outcome()["terminal_status"] != "running"

    agent = Agent.from_env({})
    calls = 0

    async def search(tool_input):
        nonlocal calls
        calls += 1
        return f"found:{tool_input['q']}"

    agent.add_tool(
        "search",
        "search the host index",
        {
            "type": "object",
            "required": ["q"],
            "properties": {"q": {"type": "string"}},
            "additionalProperties": False,
        },
        search,
    )

    created = await agent.run_subagent(spec("binding-session"), PROFILES)
    assert created["status"] == "succeeded", created
    assert created["session_revision"] == 1, created
    assert calls == 1

    resumed = await agent.resume_subagent(
        "binding-session", spec("binding-session-resumed"), PROFILES
    )
    assert resumed["status"] == "succeeded", resumed
    assert resumed["session_revision"] == 2, resumed

    fan = await agent.fan_out(
        [spec("fan-a"), spec("fan-b")], PROFILES, max_parallelism=2
    )
    assert [result["status"] for result in fan] == ["succeeded", "succeeded"]
    assert calls == 3

    council = await agent.council(
        [spec("member-a"), spec("member-b")],
        spec("synthesis"),
        PROFILES,
        min_successes=2,
        max_parallelism=2,
    )
    assert council["status"] == {"kind": "succeeded"}, council
    assert calls == 6

    approvals = 0

    async def approve(_request):
        nonlocal approvals
        approvals += 1
        return {"decision": "allow", "updated_permissions": ["allow_exact_input"]}

    agent.can_use_tool(approve)
    agent.set_permissions([{"effect": "ask", "tool": "search"}])
    approved = await agent.run_subagent(spec("approved-session"), PROFILES)
    assert approved["status"] == "succeeded", approved
    assert calls == 7
    assert approvals == 1

    # A later deny is authoritative even when an earlier rule allows the same host tool.
    agent.set_permissions(
        [
            {"effect": "allow", "tool": "search"},
            {"effect": "deny", "tool": "search"},
        ]
    )
    denied = await agent.run_subagent(spec("denied-session"), PROFILES)
    assert denied["status"] == "succeeded", denied
    assert calls == 7, "denied subagent reached the host callback"
    assert approvals == 1, "static deny unexpectedly reached the approver"

    # Conflicting aliases must fail closed. Previously `action` silently won over `decision`, so
    # this malformed host response authorized the tool despite carrying an explicit denial.
    malformed = Agent.from_env({})
    malformed_calls = 0

    async def malformed_search(_tool_input):
        nonlocal malformed_calls
        malformed_calls += 1
        return "must not run"

    async def conflicting_approval(_request):
        return {"action": "allow", "decision": "deny"}

    malformed.add_tool(
        "search",
        "must remain denied",
        {"type": "object"},
        malformed_search,
    )
    malformed.can_use_tool(conflicting_approval)
    malformed.set_permissions([{"effect": "ask", "tool": "search"}])
    malformed_result = await malformed.run_subagent(
        spec("malformed-approval"), PROFILES
    )
    assert malformed_result["status"] == "succeeded", malformed_result
    assert malformed_calls == 0, "ambiguous approval reached the host callback"

    # Hooks are action-only. A contradictory, undocumented `decision` alias must not silently
    # turn an explicit block into continue.
    malformed_hook = Agent.from_env({})
    malformed_hook_calls = 0

    async def hook_search(_tool_input):
        nonlocal malformed_hook_calls
        malformed_hook_calls += 1
        return "must not run"

    async def conflicting_hook(_context):
        return {"action": "continue", "decision": "block"}

    malformed_hook.add_tool(
        "search", "must remain blocked", {"type": "object"}, hook_search
    )
    malformed_hook.on_pre_tool_use(conflicting_hook, tool="search")
    malformed_hook_result = await malformed_hook.run_subagent(
        spec("malformed-hook"), PROFILES
    )
    assert malformed_hook_result["status"] == "succeeded", malformed_hook_result
    assert malformed_hook_calls == 0, "ambiguous hook reached the host callback"


if __name__ == "__main__":
    assert_cross_loop_callback_isolation()
    asyncio.run(close_streams(callback_free_streams()))
    asyncio.run(main())
