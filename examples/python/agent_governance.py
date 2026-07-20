"""Demonstrates the agent-native + governance surface from Python (keyless).

Run (after `maturin develop` in the aikit-py crate's venv):

    python examples/python/agent_governance.py

Shows: (1) the `Agent` primitive — drop in a key, capabilities grow, keys never leak;
(2) governance from Python — a denied tool never runs, the model gets an error result instead.
Uses the in-memory MockProvider, so no API key is needed.
"""

import asyncio
import json
from typing import Literal

import aikit

try:
    from pydantic import BaseModel

    class InvoiceModel(BaseModel):
        currency: Literal["EUR"]
        status: Literal["ok"]

except ImportError:
    # The demo stays keyless and dependency-free, but exercises the exact structural contract
    # used by Pydantic v2 when the optional package is unavailable.
    class InvoiceModel:
        def __init__(self, currency, status):
            self.currency = currency
            self.status = status

        @classmethod
        def model_json_schema(cls):
            return {
                "type": "object",
                "required": ["currency", "status"],
                "properties": {
                    "currency": {"type": "string", "enum": ["EUR"]},
                    "status": {"type": "string", "enum": ["ok"]},
                },
            }

        @classmethod
        def model_validate(cls, value):
            return cls(**value)


class Tool:
    """A minimal tool object (name / description / input_schema + async __call__)."""

    def __init__(self, name, fn):
        self.name = name
        self.description = "demo tool"
        self.input_schema = {"type": "object"}
        self._fn = fn

    async def __call__(self, tool_input):
        return await self._fn(tool_input)


async def main():
    # 1. Agent-native: key gir -> güçlen. Providers are activated by key format.
    # Agent() discovers real provider keys from process env. This deterministic parity demo uses
    # the explicit empty-env constructor so a developer's shell cannot change its transcript.
    agent = aikit.Agent.from_env({})
    fresh = agent.active_providers()
    print("providers (fresh):", fresh)

    # Both complete and streaming Agent methods resolve the requested live provider rather than
    # silently falling back to MockProvider.
    try:
        await agent.generate_text("hello", model="claude-demo")
        raise SystemExit("expected a missing live credential to raise")
    except RuntimeError as error:
        assert "no credential active for provider 'anthropic'" in str(error)
    try:
        agent.stream_text("hello", model="gpt-5")
        raise SystemExit("expected a missing live credential to raise")
    except RuntimeError as error:
        assert "no credential active for provider 'openai'" in str(error)

    agent.add_key("sk-ant-DEMOKEY")  # anthropic (by sk-ant- prefix)
    agent.add_key("AIzaDEMOKEY")     # google    (by AIza prefix)

    caps = agent.capabilities()
    after_keys = agent.active_providers()
    capabilities = [[p["provider"], p["structured_output"]] for p in caps["providers"]]
    print("providers (after keys):", after_keys)
    print("capabilities:", capabilities)
    print("repr (redacted):", repr(agent))
    assert "DEMOKEY" not in repr(agent), "Agent repr leaked a key!"
    assert agent.has_provider("anthropic") and agent.has_provider("google")

    # An sk- key that could be OpenAI or DeepSeek is ambiguous → raises without a hint.
    ambiguous_rejected = False
    try:
        agent.add_key("sk-proj-XXXX")
        raise SystemExit("expected ambiguous key to raise")
    except ValueError as e:
        ambiguous_rejected = True
        print("ambiguous sk- key correctly rejected:", str(e)[:60], "...")
    agent.add_key("sk-proj-XXXX", provider="deepseek")  # disambiguated
    assert agent.has_provider("deepseek")

    # 2. Governance from Python: the SAME tool under two policies. The tool echoes its input, so
    #    its result also proves the tool-callback seam marshalled the input correctly.
    calls = {"n": 0}

    async def run_tool(inp):
        calls["n"] += 1
        return f"rows for {inp.get('q', '?')}"

    tool = Tool("search_db", run_tool)

    # 2a. deny → the tool must NOT run.
    saw_denial = False
    denial_message = ""
    async for ev in aikit.query(
        "veritabanında ara",
        tools=[tool],
        permissions=[{"effect": "deny", "tool": "search_db"}],
    ):
        if ev.get("type") == "tool_result" and ev.get("is_error") and "denied" in ev.get("content", ""):
            saw_denial = True
            denial_message = ev["content"]
            print("deny  → tool_result:", ev["content"])
    assert calls["n"] == 0, "a denied tool must NEVER run"
    assert saw_denial, "expected a denial tool_result to reach the model"

    # 2b. allow → the tool RUNS; its return value flows back to the model (the callback seam).
    tool_echo = ""
    async for ev in aikit.query(
        "veritabanında ara",
        tools=[tool],
        permissions=[{"effect": "allow", "tool": "search_db"}],
    ):
        if ev.get("type") == "tool_result" and not ev.get("is_error"):
            tool_echo = ev["content"]
            print("allow → tool_result:", ev["content"])
    tool_ran = calls["n"] == 1
    assert tool_ran, "an allowed tool must run exactly once"

    # 3. The Agent-native path uses the same governed live-provider loop with registered host
    # tools, Ask approval, and all async lifecycle hooks. Mock keeps the proof deterministic;
    # changing only `model` after add_key uses the same callbacks against a live provider.
    governed = aikit.Agent.from_env({})
    hook_events = []
    tool_inputs = []
    approval_inputs = []

    async def agent_tool(inp):
        hook_events.append("tool")
        tool_inputs.append(inp["q"])
        return f"rows for {inp['q']}"

    async def user_prompt_hook(ctx):
        hook_events.append("prompt")
        return {"action": "rewrite", "prompt": ctx["prompt"] + " [checked]"}

    async def pre_tool_hook(ctx):
        hook_events.append("pre")
        return {"action": "rewrite", "input": {"q": "pre-approved"}}

    async def approve_tool(ctx):
        hook_events.append("approve")
        approval_inputs.append(ctx["input"]["q"])
        return {"decision": "allow", "updated_input": {"q": "approved"}}

    async def post_tool_hook(ctx):
        hook_events.append("post")
        return {"action": "rewrite", "output": "post:" + ctx["output"]}

    async def failure_hook(ctx):
        hook_events.append("failure")
        return {"action": "rewrite", "error": "safe failure"}

    async def stop_hook(_ctx):
        hook_events.append("stop")

    governed.add_tool(
        "agent_search",
        "search from the governed Agent",
        {"type": "object", "properties": {"q": {"type": "string"}}},
        agent_tool,
    )
    governed.on_user_prompt(user_prompt_hook)
    governed.on_pre_tool_use(pre_tool_hook, tool="agent_search")
    governed.on_post_tool_use(post_tool_hook, tool="agent_search")
    governed.on_failure(failure_hook)
    governed.on_stop(stop_hook)
    governed.can_use_tool(approve_tool)
    governed.set_permissions(
        [{"id": "ask-search", "effect": "ask", "tool": "agent_search"}]
    )

    governed_result = await governed.generate_text("governed agent request")
    governed_tool_result = next(
        block["content"]
        for message in governed_result["messages"]
        for block in message["content"]
        if block["type"] == "tool_result" and not block["is_error"]
    )

    # The same Agent.stream_text path is governed too. Denial reaches Failure and never executes
    # the host callback a second time.
    governed.set_permissions(
        [{"id": "deny-search", "effect": "deny", "tool": "agent_search"}]
    )
    governed_denial = ""
    async for ev in governed.stream_text("denied governed request"):
        if ev.get("type") == "tool_result" and ev.get("is_error"):
            governed_denial = ev["content"]

    governed_callbacks = [
        tool_inputs,
        approval_inputs,
        governed_tool_result,
        governed_denial,
        hook_events,
    ]
    assert governed_callbacks == [
        ["pre-approved"],
        ["pre-approved"],
        "post:rows for pre-approved",
        "safe failure",
        [
            "prompt",
            "pre",
            "approve",
            "pre",
            "tool",
            "post",
            "stop",
            "prompt",
            "pre",
            "failure",
            "stop",
        ],
    ]

    # 4. Text generation and streaming use Agent's provider resolver. The mock model proves the
    # same live-capable path without making a network request.
    generated = await agent.generate_text("Say hello")
    assert generated["provider_metadata"] == {}
    generated_text = [
        generated["text"],
        generated["usage"]["input_tokens"],
        generated["usage"]["output_tokens"],
        generated["stop_reason"],
    ]

    streamed = ""
    streamed_output_tokens = 0
    streamed_stop = ""
    async for ev in agent.stream_text("Say hello"):
        if ev.get("type") == "text_delta":
            streamed += ev["text"]
        elif ev.get("type") == "usage":
            streamed_output_tokens += ev["output_tokens"]
        elif ev.get("type") == "message_stop":
            streamed_stop = ev["stop_reason"]
    streamed_text = [streamed, streamed_output_tokens, streamed_stop]
    assert streamed == generated["text"]

    # 5. Memory is explicit: remember writes, recall searches. Timestamps stay out of the parity
    # facts because only semantic content belongs in a cross-process transcript.
    agent.remember("customer_note", "Ada prefers EUR")
    recalled = agent.recall("EUR", limit=3)
    memory_recall = [[entry["key"], entry["value"]] for entry in recalled]
    assert memory_recall == [["customer_note", "Ada prefers EUR"]]

    # 6. Routing accepts typed core model profiles and a typed route request. Fake demo keys only
    # activate capabilities; routing never sees or uses their secret values and makes no request.
    route = agent.route(
        [
            {
                "provider": "anthropic",
                "model": "claude-demo",
                "context_window_tokens": 100_000,
                "max_output_tokens": 4_096,
                "pricing": None,
                "quality_score": 80,
                "skills": ["general"],
                "capabilities": ["tool_use"],
            },
            {
                "provider": "google",
                "model": "gemini-demo",
                "context_window_tokens": 100_000,
                "max_output_tokens": 4_096,
                "pricing": None,
                "quality_score": 90,
                "skills": ["general"],
                "capabilities": ["tool_use"],
            },
        ],
        {
            "policy": {"kind": "automatic", "objective": "quality"},
            "active_providers": [],
            "estimated_input_tokens": 100,
            "required_output_tokens": 64,
            "max_cost_usd": None,
            "required_skills": ["general"],
            "required_capabilities": ["tool_use"],
        },
    )
    route_decision = [route["profile"]["provider"], route["profile"]["model"], route["eligible_models"]]
    assert route_decision == ["google", "gemini-demo", 2]

    # 7. Governed orchestration is keyless with a mock catalog. Every child remains bounded by
    # its own limits plus one shared ledger, and the initial binding grants no host tools.
    mock_profiles = [
        {
            "provider": "mock",
            "model": "mock-1",
            "context_window_tokens": 8_192,
            "max_output_tokens": 1_024,
            "pricing": None,
            "quality_score": 1,
            "skills": [],
            "capabilities": [],
        }
    ]
    orchestration_budget = {
        "max_model_calls": 8,
        "max_input_tokens": 8_192,
        "max_output_tokens": 8_192,
        "max_cost_micro_usd": None,
        "wall_time_ms": 5_000,
    }

    def child_spec(identifier, prompt):
        return {
            "id": identifier,
            "prompt": prompt,
            "system": None,
            "route": {
                "policy": {"kind": "explicit", "model": "mock-1"},
                "max_cost_usd": None,
                "required_skills": [],
                "required_capabilities": [],
            },
            "allowed_tools": [],
            "max_turns": 2,
            "max_tokens": 64,
            "estimated_input_tokens": 8,
        }

    child = await agent.run_subagent(
        child_spec("worker", "Inspect the request"),
        mock_profiles,
        budget=orchestration_budget,
    )
    assert child["status"] == "succeeded"
    fan = await agent.fan_out(
        [child_spec("fan-a", "A"), child_spec("fan-b", "B")],
        mock_profiles,
        budget=orchestration_budget,
        max_parallelism=2,
    )
    assert [result["id"] for result in fan] == ["fan-a", "fan-b"]
    council = await agent.council(
        [child_spec("member-a", "Analyze A"), child_spec("member-b", "Analyze B")],
        child_spec("synthesis", "Reach a conclusion"),
        mock_profiles,
        min_successes=2,
        budget=orchestration_budget,
        max_parallelism=2,
    )
    assert council["status"] == {"kind": "succeeded"}

    # 8. Typed structured output crosses the FFI boundary through the same Rust planner and
    # validator used by live providers. `mock-structured` keeps this parity proof keyless.
    invoice_schema = {
        "type": "object",
        "required": ["currency", "status"],
        "properties": {
            "currency": {"type": "string", "enum": ["EUR"]},
            "status": {"type": "string", "enum": ["ok"]},
        },
    }
    structured = await agent.generate_object("Return the invoice status", invoice_schema)
    structured_output = [
        structured["fidelity"],
        structured["attempts"],
        structured["value"]["currency"],
        structured["value"]["status"],
    ]
    typed_structured = await agent.generate_object(
        "Return the invoice status as a typed model",
        InvoiceModel,
    )
    assert isinstance(typed_structured["value"], InvoiceModel)
    assert typed_structured["value"].currency == "EUR"
    assert typed_structured["value"].status == "ok"

    # 9. `stream_object` is a real pull stream: provider deltas arrive before Completed. A
    # Pydantic model materializes only the final value, leaving every intermediate event visible.
    object_events = []
    object_completed = None
    async for event in agent.stream_object(
        "Stream the invoice status",
        invoice_schema,
        provider_options={"mock": {"temperature": 0}},
        compatibility_mode="warn",
    ):
        object_events.append(event["type"])
        if event["type"] == "completed":
            object_completed = event["object"]
    assert object_completed is not None
    assert object_events.index("delta") < object_events.index("completed")
    assert object_completed["provider_metadata"] == {}

    typed_events = []
    typed_stream_value = None
    async for event in agent.stream_object("Stream a typed invoice", InvoiceModel):
        typed_events.append(event["type"])
        if event["type"] == "completed":
            typed_stream_value = event["object"]["value"]
    assert "delta" in typed_events and typed_events[-1] == "completed"
    assert isinstance(typed_stream_value, InvoiceModel)

    # A deliberately unsatisfied minLength makes the mock's first value fail validation. The
    # binding surfaces both ValidationFailed and the real repair attempt before propagating the
    # terminal validation error.
    repair_sequence = []
    try:
        async for event in agent.stream_object(
            "Exercise repair events",
            {
                "type": "object",
                "required": ["value"],
                "properties": {"value": {"type": "string", "minLength": 8}},
            },
            max_retries=1,
        ):
            if event["type"] == "attempt_started":
                repair_sequence.append(["attempt_started", event["repair"]])
            elif event["type"] == "validation_failed":
                repair_sequence.append(["validation_failed", event["will_retry"]])
    except RuntimeError:
        pass
    assert repair_sequence == [
        ["attempt_started", False],
        ["validation_failed", True],
        ["attempt_started", True],
        ["validation_failed", False],
    ]

    # Interrupting an Ask approval ends the stream after the first model request: no tool result,
    # no callback, and no replay turn are synthesized.
    interrupted = aikit.Agent.from_env({})
    interrupt_counts = {"approval": 0, "tool": 0}
    interrupt_stops = []

    async def interrupted_tool(_input):
        interrupt_counts["tool"] += 1
        return "must not run"

    async def interrupt_approval(_request):
        interrupt_counts["approval"] += 1
        return {"decision": "deny", "message": "operator stopped", "interrupt": True}

    async def interrupt_stop(ctx):
        interrupt_stops.append(ctx["reason"])

    interrupted.add_tool("interrupt_me", "interrupt demo", {"type": "object"}, interrupted_tool)
    interrupted.set_permissions([{"effect": "ask", "tool": "interrupt_me"}])
    interrupted.can_use_tool(interrupt_approval)
    interrupted.on_stop(interrupt_stop)
    interrupt_events = []
    async for event in interrupted.stream_text("stop before tool execution"):
        interrupt_events.append(event)
    interrupt_fact = {
        "approval_calls": interrupt_counts["approval"],
        "errors": sum(event["type"] == "error" for event in interrupt_events),
        "message_starts": sum(event["type"] == "message_start" for event in interrupt_events),
        "stop_reasons": interrupt_stops,
        "tool_calls": interrupt_counts["tool"],
        "tool_results": sum(event["type"] == "tool_result" for event in interrupt_events),
    }
    assert interrupt_fact == {
        "approval_calls": 1,
        "errors": 1,
        "message_starts": 1,
        "stop_reasons": ["approval_interrupted"],
        "tool_calls": 0,
        "tool_results": 0,
    }
    binding_stream_facts = {
        "interrupt": interrupt_fact,
        "repair_sequence": repair_sequence,
        "structured_delta_before_completed": object_events.index("delta")
        < object_events.index("completed"),
        "structured_types": object_events,
        "structured_value": [
            object_completed["value"]["currency"],
            object_completed["value"]["status"],
        ],
        "typed_value": [typed_stream_value.currency, typed_stream_value.status],
    }
    print("structured output:", structured)

    print("\nSPIKE OK ✅  — PyO3 governed, agent-native surface works from Python:")
    print("  1) Agent: key gir -> güçlen (capabilities grow, keys never leak)")
    print("  2) permissions=[deny(...)] denies a tool; [allow(...)] runs it (callback seam)")

    # Canonical facts for the cross-language parity check (scripts/parity-check.sh). The Node
    # demo emits a byte-identical line — same Rust core, so same observable behaviour.
    facts = {
        "ambiguous_rejected": ambiguous_rejected,
        "capabilities": capabilities,
        "denial_message": denial_message,
        "denial_seen": saw_denial,
        "generated_text": generated_text,
        "memory_recall": memory_recall,
        "providers_after_keys": list(after_keys),
        "providers_fresh": list(fresh),
        "route_decision": route_decision,
        "streamed_text": streamed_text,
        "structured_output": structured_output,
        "tool_echo": tool_echo,
        "tool_ran": tool_ran,
    }
    print(
        "GOVERNANCE_JSON="
        + json.dumps(governed_callbacks, separators=(",", ":"), ensure_ascii=False)
    )
    print(
        "BINDING_STREAM_JSON="
        + json.dumps(binding_stream_facts, sort_keys=True, separators=(",", ":"))
    )
    print(
        "PARITY_JSON="
        + json.dumps(facts, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    )


if __name__ == "__main__":
    asyncio.run(main())
