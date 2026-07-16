"""Runtime contract for binding-owned subagent context and resumable sessions."""

import asyncio

from aikit import Agent


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


async def main() -> None:
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


if __name__ == "__main__":
    asyncio.run(main())
