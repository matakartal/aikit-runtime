"""Keyless runtime contract for canonical messages, media, routing, and typed failures."""

import asyncio
import json
from typing import Any

from aikit import AikitError, Agent, query, tool


MESSAGES = [
    {
        "role": "system",
        "content": [{"type": "text", "text": "Inspect every supplied input block."}],
    },
    {
        "role": "user",
        "content": [
            {"type": "text", "text": "multimodal"},
            {
                "type": "media",
                "media_type": "image/png",
                "source": {"kind": "url", "url": "https://example.com/chart.png"},
            },
            {
                "type": "media",
                "media_type": "image/jpeg",
                "source": {"kind": "base64", "data": "aGVsbG8="},
            },
            {
                "type": "media_input",
                "media": {
                    "media_type": "application/octet-stream",
                    "source": {"kind": "bytes", "data": [97, 98, 99]},
                    "sha256": "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
                    "size_bytes": 3,
                },
            },
        ],
    },
]

OBJECT_SCHEMA = {
    "type": "object",
    "required": ["status"],
    "properties": {"status": {"type": "string", "const": "ok"}},
    "additionalProperties": False,
}


@tool(
    "decorated",
    "decorator-defined tool",
    {
        "type": "object",
        "required": ["q"],
        "properties": {"q": {"type": "string"}},
        "additionalProperties": False,
    },
)
async def decorated_tool(tool_input: dict[str, Any]) -> str:
    return f"decorated:{tool_input['q']}"


async def drain(stream: Any) -> dict[str, Any]:
    async for _delta in stream:
        pass
    return stream.outcome()


def assert_input_preserved(outcome: dict[str, Any]) -> None:
    assert outcome["messages"][: len(MESSAGES)] == MESSAGES


async def main() -> None:
    agent = Agent.from_env({})

    generated = await agent.generate_text(MESSAGES)
    assert generated["messages"][: len(MESSAGES)] == MESSAGES
    assert_input_preserved(await drain(agent.stream_text(MESSAGES)))
    assert_input_preserved(await drain(agent.run(MESSAGES)))
    assert_input_preserved(await drain(agent.client().query(MESSAGES)))
    assert_input_preserved(await drain(query(MESSAGES)))

    decorated_outcome = await drain(query("use decorated", tools=[decorated_tool]))
    decorated_result = next(
        block["content"]
        for message in decorated_outcome["messages"]
        for block in message["content"]
        if block["type"] == "tool_result"
    )
    assert decorated_result == "decorated:merhaba"

    definition_agent = Agent.from_env({})
    definition_agent.add_tool_definition(decorated_tool)
    definition_outcome = await drain(definition_agent.run("use definition"))
    assert any(
        block.get("type") == "tool_result"
        and block.get("content") == "decorated:merhaba"
        for message in definition_outcome["messages"]
        for block in message["content"]
    )

    compatibility = await agent.generate_text("string compatibility")
    assert compatibility["messages"][0] == {
        "role": "user",
        "content": [{"type": "text", "text": "string compatibility"}],
    }

    structured = await agent.generate_object(MESSAGES, OBJECT_SCHEMA)
    assert structured["value"] == {"status": "ok"}
    completed = None
    async for event in agent.stream_object(MESSAGES, OBJECT_SCHEMA):
        if event["type"] == "completed":
            completed = event["object"]
    assert completed is not None and completed["value"] == {"status": "ok"}

    routed = await drain(
        agent.run(
            MESSAGES,
            {
                "routing": {
                    "profiles": [
                        {
                            "provider": "mock",
                            "model": "mock-routed",
                            "context_window_tokens": 8192,
                            "max_output_tokens": 1024,
                            "pricing": None,
                            "quality_score": 100,
                            "skills": [],
                            "capabilities": ["vision"],
                        }
                    ],
                    "request": {
                        "policy": {"kind": "automatic", "objective": "quality"},
                        "active_providers": [],
                        "estimated_input_tokens": 8,
                        "required_output_tokens": 64,
                        "max_cost_usd": None,
                        "required_skills": [],
                        "required_capabilities": ["vision"],
                    },
                }
            },
        )
    )
    assert routed["model_attempts"] == ["mock-routed"]

    route = {
        "policy": {"kind": "explicit", "model": "mock-routed"},
        "max_cost_usd": None,
        "required_skills": [],
        "required_capabilities": [],
    }
    alias_spec = agent.subtask(
        "alias-child",
        "run alias child",
        route,
        system="Stay concise",
        max_turns=2,
        max_tokens=64,
        estimated_input_tokens=8,
    )
    assert alias_spec == {
        "id": "alias-child",
        "prompt": "run alias child",
        "system": "Stay concise",
        "route": route,
        "allowed_tools": [],
        "max_turns": 2,
        "max_tokens": 64,
        "estimated_input_tokens": 8,
    }
    parallel = await agent.parallel(
        [alias_spec],
        [
            {
                "provider": "mock",
                "model": "mock-routed",
                "context_window_tokens": 8192,
                "max_output_tokens": 1024,
                "pricing": None,
                "quality_score": 100,
                "skills": [],
                "capabilities": [],
            }
        ],
    )
    assert [result["status"] for result in parallel] == ["succeeded"]

    for invalid in ([], [{"role": "user", "content": [{"type": "media"}]}]):
        try:
            agent.run(invalid)
        except ValueError:
            pass
        else:
            raise AssertionError("malformed canonical input unexpectedly reached the provider")

    try:
        await agent.generate_text(MESSAGES, model="not-a-real-model")
    except AikitError as error:
        assert error.code == error.info["code"]
        assert error.info["message"]
    else:
        raise AssertionError("unknown model did not raise typed AikitError")

    object_error = None
    try:
        async for _event in agent.stream_object(
            MESSAGES,
            {
                "type": "object",
                "required": ["value"],
                "properties": {"value": {"type": "string", "minLength": 8}},
            },
            max_retries=0,
        ):
            pass
    except AikitError as error:
        object_error = error
    assert object_error is not None
    assert object_error.code == "structured_output"
    assert object_error.info["code"] == "structured_output"

    failure_contexts: list[dict[str, Any]] = []
    failing = Agent.from_env({})

    async def explode(_tool_input: dict[str, Any]) -> str:
        raise RuntimeError("expected host failure")

    async def after_tool_failure(context: dict[str, Any]) -> None:
        failure_contexts.append(context)

    failing.add_tool(
        "explode",
        "always fails",
        {"type": "object", "properties": {"q": {"type": "string"}}},
        explode,
    )
    failing.on_post_tool_failure(after_tool_failure, tool="explode")
    await drain(failing.run("invoke the failing tool"))
    assert len(failure_contexts) == 1
    assert failure_contexts[0]["stage"] == "tool_execution"
    assert failure_contexts[0]["tool"] == "explode"

    result = {
        "media_sources": ["url", "base64", "strict-bytes"],
        "object_error": object_error.code,
        "post_tool_failure": failure_contexts[0]["stage"],
        "parallel": parallel[0]["status"],
        "routed_model": routed["model_attempts"][0],
        "structured": structured["value"]["status"],
        "text_surfaces": 5,
        "tool_definition": decorated_result,
    }
    print(
        "MULTIMODAL_MESSAGES_JSON="
        + json.dumps(result, sort_keys=True, separators=(",", ":"))
    )


asyncio.run(main())
