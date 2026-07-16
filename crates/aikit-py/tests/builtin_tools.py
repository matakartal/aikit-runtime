"""Runtime contract for explicit, jailed built-ins and orchestration inheritance."""

import asyncio
import tempfile
from pathlib import Path

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
FILE_TOOL_NAMES = ["Read", "Write", "Edit", "Grep", "Glob"]


def spec(identifier: str, tool: str):
    return {
        "id": identifier,
        "prompt": f"use {tool}",
        "system": None,
        "route": {
            "policy": {"kind": "explicit", "model": "mock-1"},
            "max_cost_usd": None,
            "required_skills": [],
            "required_capabilities": [],
        },
        "allowed_tools": [tool],
        "max_turns": 3,
        "max_tokens": 64,
        "estimated_input_tokens": 8,
    }


def used_tool(result, name: str) -> bool:
    return any(
        block.get("type") == "tool_use" and block.get("name") == name
        for message in result["outcome"]["messages"]
        for block in message["content"]
    )


async def main() -> None:
    with tempfile.TemporaryDirectory() as primary, tempfile.TemporaryDirectory() as secondary:
        agent = Agent.from_env({})
        host_calls = 0

        async def search(_input):
            nonlocal host_calls
            host_calls += 1
            return "host-result"

        agent.add_tool(
            "search",
            "host search",
            {
                "type": "object",
                "required": ["q"],
                "properties": {"q": {"type": "string"}},
                "additionalProperties": False,
            },
            search,
        )
        agent.register_builtin_tools([primary, secondary])
        assert agent.capabilities()["tools"] == ["search", *FILE_TOOL_NAMES]
        assert "Bash" not in agent.capabilities()["tools"]

        host = await agent.run_subagent(spec("composite-host", "search"), PROFILES)
        assert host["status"] == "succeeded", host
        assert host_calls == 1

        child = await agent.run_subagent(spec("builtin-child", "Read"), PROFILES)
        assert used_tool(child, "Read"), child
        fan = await agent.fan_out(
            [spec("builtin-fan-a", "Read"), spec("builtin-fan-b", "Read")],
            PROFILES,
            max_parallelism=2,
        )
        assert all(used_tool(result, "Read") for result in fan), fan
        council = await agent.council(
            [spec("builtin-member-a", "Read"), spec("builtin-member-b", "Read")],
            spec("builtin-synthesis", "Read"),
            PROFILES,
            min_successes=2,
            max_parallelism=2,
        )
        assert all(used_tool(result, "Read") for result in council["members"]), council
        assert used_tool(council["synthesis"], "Read"), council

        agent.enable_bash_with_required_containment()
        assert agent.capabilities()["tools"] == ["search", *FILE_TOOL_NAMES, "Bash"]
        containment = await agent.builtin_containment_capabilities()
        assert containment["requirement"] == {"mode": "required", "backend": "auto"}
        assert containment["fail_closed"] is True
        assert containment["selected_backend"] != "uncontained"

        agent.enable_bash_with_required_containment({"image": "alpine:latest"})
        invalid_docker = await agent.builtin_containment_capabilities()
        docker = next(
            backend for backend in invalid_docker["backends"] if backend["backend"] == "docker"
        )
        assert docker["available"] is False
        assert "must be pinned" in docker["detail"]

        collision = Agent.from_env({})

        async def colliding(_input):
            return "never"

        collision.add_tool("Read", "collision", {"type": "object"}, colliding)
        try:
            collision.register_builtin_tools([primary])
        except ValueError as error:
            assert "collides with a registered host tool" in str(error)
        else:
            raise AssertionError("host/built-in collision was accepted")

        reverse_collision = Agent.from_env({})
        reverse_collision.register_builtin_tools([str(Path(primary)), str(Path(secondary))])
        try:
            reverse_collision.add_tool("Read", "collision", {"type": "object"}, colliding)
        except ValueError as error:
            assert "already registered" in str(error)
        else:
            raise AssertionError("built-in/host collision was accepted")

        bash_collision = Agent.from_env({})
        bash_collision.add_tool("Bash", "host bash", {"type": "object"}, colliding)
        bash_collision.register_builtin_tools([primary])
        try:
            bash_collision.enable_bash_with_required_containment()
        except ValueError as error:
            assert "collides with a registered host tool" in str(error)
        else:
            raise AssertionError("contained Bash collision was accepted")


if __name__ == "__main__":
    asyncio.run(main())
