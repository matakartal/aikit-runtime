"""Keyless, canonical public-surface conformance for the Python binding.

Every emitted payload deliberately excludes run IDs, timestamps, durations, and raw error text.
The parity gate compares these compact JSON lines byte-for-byte with Rust and Node.
"""

import asyncio
import json
import tempfile
from contextlib import ExitStack
from pathlib import Path

import aikit


def emit(module, value):
    print(
        f"CONFORMANCE_{module.upper()}_JSON="
        + json.dumps(value, sort_keys=True, separators=(",", ":"))
    )


async def drain(stream):
    error_codes = []
    events = []
    async for event in stream:
        events.append(event)
        if event.get("type") == "error":
            error_codes.append(event["info"]["code"])
    return stream.outcome(), error_codes, events


async def governance_facts():
    agent = aikit.Agent.from_env({})
    events = []
    approval_inputs = []
    tool_inputs = []

    async def tool(value):
        events.append("tool")
        tool_inputs.append(value["q"])
        return f"rows for {value['q']}"

    async def prompt(context):
        events.append("prompt")
        return {"action": "rewrite", "prompt": context["prompt"] + " [checked]"}

    async def pre(_context):
        events.append("pre")
        return {"action": "rewrite", "input": {"q": "pre-approved"}}

    async def approve(context):
        events.append("approve")
        approval_inputs.append(context["input"]["q"])
        return {
            "decision": "allow",
            "updated_input": {"q": "approved"},
            "updated_permissions": ["allow_exact_input"],
        }

    async def post(context):
        events.append("post")
        return {"action": "rewrite", "output": "post:" + context["output"]}

    async def stopped(_context):
        events.append("stop")

    agent.add_tool(
        "search",
        "search",
        {
            "type": "object",
            "required": ["q"],
            "properties": {"q": {"type": "string"}},
        },
        tool,
    )
    agent.on_user_prompt(prompt)
    agent.on_pre_tool_use(pre, tool="search")
    agent.on_post_tool_use(post, tool="search")
    agent.on_stop(stopped)
    agent.can_use_tool(approve)
    agent.set_permissions([{"id": "ask", "effect": "ask", "tool": "search"}])
    generated = await agent.generate_text("governed")
    result = next(
        block["content"]
        for message in generated["messages"]
        for block in message["content"]
        if block["type"] == "tool_result" and not block["is_error"]
    )

    denied = aikit.Agent.from_env({})
    deny_calls = 0
    deny_stages = []

    async def denied_tool(_value):
        nonlocal deny_calls
        deny_calls += 1
        return "must not run"

    async def deny_failure(context):
        deny_stages.append(context["stage"])
        return None

    denied.add_tool("guarded", "guarded", {"type": "object"}, denied_tool)
    denied.on_failure(deny_failure)
    denied.set_permissions(
        [
            {"id": "early-allow", "effect": "allow", "tool": "guarded"},
            {"id": "authoritative-deny", "effect": "deny", "tool": "guarded"},
        ]
    )
    _, _, deny_events = await drain(denied.run("deny wins"))
    deny_results = [event for event in deny_events if event["type"] == "tool_result"]

    invalid = aikit.Agent.from_env({})
    invalid_calls = 0
    invalid_stages = []

    async def invalid_tool(_value):
        nonlocal invalid_calls
        invalid_calls += 1
        return "must not run"

    async def invalid_failure(context):
        invalid_stages.append(context["stage"])
        return None

    invalid.add_tool(
        "typed",
        "typed",
        {
            "type": "object",
            "required": ["count"],
            "properties": {"count": {"type": "integer"}},
        },
        invalid_tool,
    )
    invalid.on_failure(invalid_failure)
    _, _, invalid_events = await drain(invalid.run("invalid tool input"))
    invalid_results = [event for event in invalid_events if event["type"] == "tool_result"]

    interrupted = aikit.Agent.from_env({})
    interrupt_calls = 0
    interrupt_stops = []

    async def interrupt_tool(_value):
        nonlocal interrupt_calls
        interrupt_calls += 1
        return "must not run"

    async def interrupt(_request):
        return {"decision": "deny", "message": "operator stopped", "interrupt": True}

    async def interrupt_stop(context):
        interrupt_stops.append(context["reason"])

    interrupted.add_tool("interrupt", "interrupt", {"type": "object"}, interrupt_tool)
    interrupted.set_permissions([{"effect": "ask", "tool": "interrupt"}])
    interrupted.can_use_tool(interrupt)
    interrupted.on_stop(interrupt_stop)
    _, interrupt_codes, _ = await drain(interrupted.run("interrupt"))

    return {
        "approval": {
            "approval_inputs": approval_inputs,
            "events": events,
            "result": result,
            "tool_inputs": tool_inputs,
        },
        "authoritative_deny": {
            "failure_stages": deny_stages,
            "is_error": len(deny_results) == 1 and deny_results[0]["is_error"],
            "tool_calls": deny_calls,
        },
        "interrupt": {
            "error_codes": interrupt_codes,
            "stop_reasons": interrupt_stops,
            "tool_calls": interrupt_calls,
        },
        "schema_validation": {
            "failure_stages": invalid_stages,
            "is_error": len(invalid_results) == 1 and invalid_results[0]["is_error"],
            "tool_calls": invalid_calls,
        },
    }


async def structured_facts():
    agent = aikit.Agent.from_env({})
    schema = {
        "type": "object",
        "required": ["currency", "status"],
        "properties": {
            "currency": {"type": "string", "enum": ["EUR"]},
            "status": {"type": "string", "enum": ["ok"]},
        },
    }
    event_types = []
    delta_types = []
    completed = None
    async for event in agent.stream_object(
        "structured",
        schema,
        provider_options={"mock": {"temperature": 0, "tag": "parity"}},
    ):
        event_types.append(event["type"])
        if event["type"] == "delta":
            delta_types.append(event["delta"]["type"])
        elif event["type"] == "completed":
            completed = event["object"]
    assert completed is not None

    repair = []
    repair_failed = False
    try:
        async for event in agent.stream_object(
            "repair",
            {
                "type": "object",
                "required": ["value"],
                "properties": {"value": {"type": "string", "minLength": 8}},
            },
            max_retries=1,
        ):
            if event["type"] == "attempt_started":
                repair.append(["attempt_started", event["repair"]])
            elif event["type"] == "validation_failed":
                repair.append(["validation_failed", event["will_retry"]])
    except RuntimeError:
        repair_failed = True

    return {
        "attempts": completed["attempts"],
        "delta_types": delta_types,
        "event_types": event_types,
        "fidelity": completed["fidelity"],
        "provider_metadata_empty": completed["provider_metadata"] == {},
        "repair": repair,
        "repair_failed": repair_failed,
        "value": [completed["value"]["currency"], completed["value"]["status"]],
    }


async def run_options_facts():
    agent = aikit.Agent.from_env({})
    client, client_codes, _ = await drain(
        aikit.Client(agent).query(
            "client",
            {
                "model": "mock-1",
                "fallback_models": ["mock-2"],
                "max_tokens": 64,
                "max_turns": 2,
                "provider_options": {"mock": {"tag": "parity"}},
                "retry": {
                    "max_attempts_per_model": 2,
                    "base_delay_ms": 0,
                    "max_delay_ms": 0,
                    "per_attempt_timeout_ms": 1_000,
                },
            },
        )
    )
    priced, priced_codes, _ = await drain(
        agent.run(
            "priced",
            {
                "budget": {
                    "max_cost_usd": 1.0,
                    "pricing": {
                        "input_per_million_usd": 1.0,
                        "output_per_million_usd": 2.0,
                    },
                }
            },
        )
    )
    limited, limited_codes, _ = await drain(
        agent.run("limited", {"max_turns": 0})
    )
    budget, budget_codes, _ = await drain(
        agent.run("budget", {"budget": {"max_total_tokens": 0}})
    )
    cancelled_stream = agent.run("cancelled")
    cancelled_stream.cancel()
    cancelled, cancel_codes, _ = await drain(cancelled_stream)
    return {
        "budget": [budget["terminal_status"], budget_codes],
        "cancel": [cancelled["terminal_status"], cancel_codes],
        "client": [
            client["terminal_status"],
            client["model_attempts"],
            client_codes,
        ],
        "max_turns": [limited["terminal_status"], limited_codes],
        "priced_budget": [priced["terminal_status"], priced_codes],
    }


async def state_facts():
    agent = aikit.Agent.from_env({})
    stop_reasons = []

    async def stopped(context):
        stop_reasons.append(context["reason"])

    agent.on_stop(stopped)
    generated = await agent.generate_text("state")
    agent.remember("customer_note", "Ada prefers EUR")
    memory = [
        [entry["key"], entry["value"]]
        for entry in agent.recall("EUR", limit=3)
    ]
    return {
        "audit": {
            "advertised": "audit" in agent.capabilities()["runtime_features"],
            "stop_reasons": stop_reasons,
        },
        "memory": memory,
        "provider_metadata_empty": generated["provider_metadata"] == {},
        "session": {
            "roles": [message["role"] for message in generated["messages"]],
            "stop_reason": generated["stop_reason"],
        },
    }


def profiles():
    return [
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


def child_spec(identifier, prompt, allowed_tools=None):
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
        "allowed_tools": allowed_tools or [],
        "max_turns": 2,
        "max_tokens": 64,
        "estimated_input_tokens": 8,
    }


async def orchestration_facts():
    agent = aikit.Agent.from_env({})
    events = []
    approvals = 0
    calls = 0

    async def tool(value):
        nonlocal calls
        calls += 1
        events.append("tool")
        return f"child:{value['q']}"

    async def pre(_context):
        events.append("pre")
        return {"action": "rewrite", "input": {"q": "child-pre"}}

    async def approve(_context):
        nonlocal approvals
        approvals += 1
        events.append("approve")
        return {"decision": "allow", "updated_permissions": ["allow_tool"]}

    async def post(context):
        events.append("post")
        return {"action": "rewrite", "output": "post:" + context["output"]}

    async def stopped(_context):
        events.append("stop")

    agent.add_tool(
        "child_search",
        "child search",
        {
            "type": "object",
            "required": ["q"],
            "properties": {"q": {"type": "string"}},
        },
        tool,
    )
    agent.on_pre_tool_use(pre, tool="child_search")
    agent.on_post_tool_use(post, tool="child_search")
    agent.on_stop(stopped)
    agent.can_use_tool(approve)
    agent.set_permissions([{"effect": "ask", "tool": "child_search"}])
    budget = {
        "max_model_calls": 8,
        "max_input_tokens": 8_192,
        "max_output_tokens": 8_192,
        "wall_time_ms": 5_000,
    }
    first = await agent.run_subagent(
        child_spec("thread", "first", ["child_search"]),
        profiles(),
        budget=budget,
    )
    resumed = await agent.resume_subagent(
        "thread",
        child_spec("thread-resume", "second", ["child_search"]),
        profiles(),
        budget=budget,
    )
    tool_result = next(
        block["content"]
        for message in first["outcome"]["messages"]
        for block in message["content"]
        if block["type"] == "tool_result" and not block["is_error"]
    )

    plain = aikit.Agent.from_env({})
    fan = await plain.fan_out(
        [child_spec("fan-a", "A"), child_spec("fan-b", "B")],
        profiles(),
        budget=budget,
        max_parallelism=2,
    )
    deadline = await plain.run_subagent(
        child_spec("expired", "expired"),
        profiles(),
        budget={"wall_time_ms": 0},
    )
    return {
        "context": {
            "approval_calls": approvals,
            "events": events,
            "status": first["status"],
            "tool_calls": calls,
            "tool_result": tool_result,
        },
        "deadline": {
            "code": deadline.get("error_info", {}).get("code"),
            "status": deadline["status"],
            "terminal": deadline["outcome"]["terminal_status"],
        },
        "fan_out": {
            "ids": [result["id"] for result in fan],
            "statuses": [result["status"] for result in fan],
        },
        "resume": {
            "message_counts": [
                len(first["outcome"]["messages"]),
                len(resumed["outcome"]["messages"]),
            ],
            "revisions": [first["session_revision"], resumed["session_revision"]],
            "statuses": [first["status"], resumed["status"]],
        },
    }


def outcome_tool_result(outcome):
    return next(
        block
        for message in outcome["messages"]
        for block in message["content"]
        if block["type"] == "tool_result"
    )


def outcome_used_tool(outcome, expected):
    return any(
        block.get("type") == "tool_use" and block.get("name") == expected
        for message in outcome["messages"]
        for block in message["content"]
    )


async def force_builtin(agent, name, tool_input):
    outcome, _, _ = await drain(
        agent.run(
            f"deterministic built-in fixture: {name}",
            {
                "provider_options": {
                    "mock": {"tool_name": name, "tool_input": tool_input}
                }
            },
        )
    )
    return outcome


async def input_facts():
    agent = aikit.Agent.from_env({})
    messages = [
        {
            "role": "user",
            "content": [
                {"type": "text", "text": "multimodal"},
                {
                    "type": "media",
                    "media_type": "image/png",
                    "source": {"kind": "base64", "data": "aGVsbG8="},
                },
            ],
        }
    ]

    compatible = await agent.generate_text("string compatibility")
    assert compatible["messages"][0] == {
        "role": "user",
        "content": [{"type": "text", "text": "string compatibility"}],
    }

    generated = await agent.generate_text(messages)
    media = next(
        block
        for message in generated["messages"]
        for block in message["content"]
        if block["type"] == "media"
    )
    text_roles = [
        message["role"]
        for message in generated["messages"]
        if any(
            block.get("type") == "text" and block.get("text") == "multimodal"
            for block in message["content"]
        )
    ]

    structured = await agent.generate_object(
        messages,
        {
            "type": "object",
            "required": ["status"],
            "properties": {"status": {"type": "string", "const": "ok"}},
            "additionalProperties": False,
        },
    )

    routed, _, _ = await drain(
        agent.run(
            messages,
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
                            "capabilities": [],
                        }
                    ],
                    "request": {
                        "policy": {"kind": "automatic", "objective": "quality"},
                        "active_providers": [],
                        "estimated_input_tokens": 8,
                        "required_output_tokens": 64,
                        "max_cost_usd": None,
                        "required_skills": [],
                        "required_capabilities": [],
                    },
                }
            },
        )
    )

    return {
        "media_input": {
            "media_type": media["media_type"],
            "source_kind": media["source"]["kind"],
            "text_roles": text_roles,
        },
        "routing": {"model_attempts": routed["model_attempts"]},
        "structured": {
            "fidelity": structured["fidelity"],
            "status": structured["value"]["status"],
        },
    }


async def builtins_facts():
    file_tool_names = ["Read", "Write", "Edit", "Grep", "Glob"]
    with ExitStack() as stack:
        primary_name = stack.enter_context(tempfile.TemporaryDirectory())
        secondary_name = stack.enter_context(tempfile.TemporaryDirectory())
        outside_name = stack.enter_context(tempfile.TemporaryDirectory())
        primary = Path(primary_name)
        secondary = Path(secondary_name)
        outside = Path(outside_name)
        secondary_file = secondary / "secondary.txt"
        outside_file = outside / "outside.txt"
        secondary_file.write_text("secondary-ok", encoding="utf-8")
        outside_file.write_text("outside-secret", encoding="utf-8")
        (primary / "escape-link.txt").symlink_to(outside_file)

        agent = aikit.Agent.from_env({})
        agent.register_builtin_tools([str(primary), str(secondary)])
        default_names = agent.capabilities()["tools"]

        written = await force_builtin(
            agent,
            "Write",
            {"path": "roundtrip.txt", "content": "before needle"},
        )
        read_before = await force_builtin(agent, "Read", {"path": "roundtrip.txt"})
        edited = await force_builtin(
            agent,
            "Edit",
            {
                "path": "roundtrip.txt",
                "old_string": "before",
                "new_string": "after",
            },
        )
        read_after = await force_builtin(agent, "Read", {"path": "roundtrip.txt"})
        grep = await force_builtin(
            agent, "Grep", {"pattern": "after", "path": "."}
        )
        glob = await force_builtin(agent, "Glob", {"pattern": "*.txt"})
        multi_root = await force_builtin(
            agent, "Read", {"path": str(secondary_file)}
        )
        outside_denial = await force_builtin(
            agent, "Read", {"path": str(outside_file)}
        )
        symlink_denial = await force_builtin(
            agent, "Read", {"path": "escape-link.txt"}
        )
        strict_schema = await force_builtin(
            agent,
            "Read",
            {"path": "roundtrip.txt", "unexpected": True},
        )

        write_result = outcome_tool_result(written)
        before_result = outcome_tool_result(read_before)
        edit_result = outcome_tool_result(edited)
        after_result = outcome_tool_result(read_after)
        grep_result = outcome_tool_result(grep)
        glob_result = outcome_tool_result(glob)
        multi_root_result = outcome_tool_result(multi_root)
        outside_result = outcome_tool_result(outside_denial)
        symlink_result = outcome_tool_result(symlink_denial)
        strict_result = outcome_tool_result(strict_schema)

        coexist = aikit.Agent.from_env({})

        async def search(_value):
            return "host-result"

        coexist.add_tool("search", "search", {"type": "object"}, search)
        coexist.register_builtin_tools([str(primary), str(secondary)])
        host_builtin_coexist = coexist.capabilities()["tools"] == [
            "search",
            *file_tool_names,
        ]

        collision = aikit.Agent.from_env({})

        async def spoofed_read(_value):
            return "spoofed"

        collision.add_tool(
            "Read", "spoofed host Read", {"type": "object"}, spoofed_read
        )
        try:
            collision.register_builtin_tools([str(primary), str(secondary)])
            host_before_builtin_spoof_blocked = False
        except ValueError:
            host_before_builtin_spoof_blocked = True

        agent.enable_bash_with_required_containment()
        bash_names = agent.capabilities()["tools"]
        containment = await agent.builtin_containment_capabilities()
        agent.enable_bash_with_required_containment({"image": "alpine:latest"})
        mutable_docker = await agent.builtin_containment_capabilities()
        mutable_docker_rejected = any(
            backend["backend"] == "docker" and not backend["available"]
            for backend in mutable_docker["backends"]
        )

        child_agent = aikit.Agent.from_env({})
        child_agent.register_builtin_tools([str(primary), str(secondary)])
        child = await child_agent.run_subagent(
            child_spec("builtin-read", "use Read", ["Read"]), profiles()
        )

        return {
            "containment": {
                "fail_closed": containment["fail_closed"],
                "mutable_docker_rejected": mutable_docker_rejected,
                "required_auto": containment["requirement"]
                == {"mode": "required", "backend": "auto"},
                "uncontained": containment.get("selected_backend") == "uncontained",
            },
            "filesystem": {
                "edit": not edit_result["is_error"],
                "glob": not glob_result["is_error"]
                and "roundtrip.txt" in glob_result["content"],
                "grep": not grep_result["is_error"]
                and "after needle" in grep_result["content"],
                "multi_root_read": not multi_root_result["is_error"]
                and multi_root_result["content"] == "secondary-ok",
                "outside_denied": outside_result["is_error"],
                "read_after": not after_result["is_error"]
                and after_result["content"] == "after needle",
                "read_before": not before_result["is_error"]
                and before_result["content"] == "before needle",
                "symlink_denied": symlink_result["is_error"],
                "write": not write_result["is_error"]
                and "wrote" in write_result["content"],
            },
            "registry": {
                "bash_absent_by_default": "Bash" not in default_names,
                "bash_tools": bash_names,
                "canonical_specs_strict": strict_result["is_error"],
                "default_tools": default_names,
                "host_before_builtin_spoof_blocked": host_before_builtin_spoof_blocked,
                "host_builtin_coexist": host_builtin_coexist,
            },
            "subagent": {
                "read_advertised": "Read" in file_tool_names,
                "read_inherited": outcome_used_tool(child["outcome"], "Read"),
                "status": child["status"],
            },
        }
async def main():
    emit("governance", await governance_facts())
    emit("structured", await structured_facts())
    emit("run_options", await run_options_facts())
    emit("state", await state_facts())
    emit("orchestration", await orchestration_facts())
    emit("builtins", await builtins_facts())
    emit("input", await input_facts())


if __name__ == "__main__":
    asyncio.run(main())
