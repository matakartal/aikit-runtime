"""Semantic structured-output validator contract for the Python binding."""

import asyncio
from typing import Any, Dict

from aikit import Agent, AikitError


SCHEMA = {
    "type": "object",
    "required": ["currency", "status"],
    "properties": {
        "currency": {"type": "string", "enum": ["EUR"]},
        "status": {"type": "string", "enum": ["ok"]},
    },
    "additionalProperties": False,
}


async def main() -> None:
    agent = Agent.from_env({})
    retry_calls = 0

    async def retry_once(value: Dict[str, Any]) -> Any:
        nonlocal retry_calls
        retry_calls += 1
        assert value == {"currency": "EUR", "status": "ok"}
        if retry_calls == 1:
            return {"action": "retry", "reason": "semantic policy needs repair"}
        return "accept"

    generated = await agent.generate_object(
        "invoice", SCHEMA, max_retries=1, validator=retry_once
    )
    assert generated["attempts"] == 2
    assert retry_calls == 2

    stream_calls = 0

    async def stream_retry(_value: Dict[str, Any]) -> Any:
        nonlocal stream_calls
        stream_calls += 1
        return (
            {"action": "retry", "reason": "stream repair"}
            if stream_calls == 1
            else {"action": "accept"}
        )

    events = [
        event
        async for event in agent.stream_object(
            "invoice", SCHEMA, max_retries=1, validator=stream_retry
        )
    ]
    assert any(
        event["type"] == "validation_failed"
        and event["will_retry"]
        and "stream repair" in event["error"]
        for event in events
    )
    assert events[-1]["type"] == "completed"
    assert events[-1]["object"]["attempts"] == 2

    async def reject(_value: Dict[str, Any]) -> Any:
        return {"action": "reject", "reason": "business policy denied it"}

    try:
        await agent.generate_object("invoice", SCHEMA, validator=reject)
    except AikitError as error:
        assert error.code == "structured_output"
        assert "business policy denied it" in str(error)
    else:
        raise AssertionError("semantic reject must fail with AikitError")

    async def raises(_value: Dict[str, Any]) -> Any:
        raise RuntimeError("validator exploded")

    try:
        await agent.generate_object("invoice", SCHEMA, validator=raises)
    except AikitError as error:
        assert error.code == "structured_output"
        assert "failed closed" in str(error)
    else:
        raise AssertionError("validator exception must fail closed")

    async def invalid_decision(_value: Dict[str, Any]) -> Any:
        return None

    try:
        await agent.generate_object("invoice", SCHEMA, validator=invalid_decision)
    except AikitError as error:
        assert error.code == "structured_output"
        assert "failed closed" in str(error)
    else:
        raise AssertionError("invalid validator decision must fail closed")

    adversarial_decisions = [
        {"decision": "accept"},
        {"action": "accept", "reason": "accept must not carry a reason"},
        {"action": "accept", "unexpected": True},
        {"action": "retry"},
        {"action": "retry", "reason": 7},
        {"action": "retry", "reason": "repair", "unexpected": True},
        {"action": "retry", "decision": "reject", "reason": "conflict"},
        {"action": "reject"},
        {"action": "reject", "reason": "deny", "unexpected": True},
    ]
    for decision in adversarial_decisions:
        async def adversarial(_value: Dict[str, Any]) -> Any:
            return decision

        try:
            await agent.generate_object("invoice", SCHEMA, validator=adversarial)
        except AikitError as error:
            assert error.code == "structured_output"
            assert "failed closed" in str(error)
        else:
            raise AssertionError(f"semantic validator accepted malformed decision: {decision}")

    baseline = await agent.generate_object("invoice", SCHEMA)
    assert baseline["attempts"] == 1
    print("python semantic validator: ok")


if __name__ == "__main__":
    asyncio.run(main())
