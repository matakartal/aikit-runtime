"""Real Python FFI contract for deterministic outcome evaluation."""

from copy import deepcopy

from aikit import evaluate_outcome


RAW_OUTPUT = "RAW_MODEL_OUTPUT_MUST_NOT_LEAK"


def canonical_outcome(status: str = "completed") -> dict:
    return {
        "messages": [
            {
                "role": "assistant",
                "content": [
                    {"type": "tool_use", "id": "call-1", "name": "search", "input": {}},
                    {"type": "text", "text": RAW_OUTPUT},
                ],
            },
            {
                "role": "tool",
                "content": [
                    {
                        "type": "tool_result",
                        "tool_use_id": "call-1",
                        "content": "ok",
                        "is_error": False,
                    }
                ],
            },
        ],
        "usage": {
            "input_tokens": 7,
            "output_tokens": 11,
            "cache_creation_input_tokens": 0,
            "cache_read_input_tokens": 0,
            "reasoning_tokens": 0,
        },
        "terminal_status": status,
        "stop_reason": "stop",
        "model_attempts": ["mock-1"],
        "final_text": "ignored convenience projection",
        "invocation_start_message_index": 0,
    }


gates = [
    {"type": "output_exact", "value": RAW_OUTPUT},
    {"type": "called_tool", "name": "search"},
    {"type": "tool_sequence", "names": ["search"], "exact": True},
    {"type": "no_tool_errors"},
    {"type": "max_turns", "value": 1},
    {"type": "max_total_tokens", "value": 18},
    {"type": "max_model_attempts", "value": 1},
]

outcome = canonical_outcome()
snapshot = deepcopy(outcome)
first = evaluate_outcome(outcome, gates)
second = evaluate_outcome(outcome, gates)
assert first == second
assert outcome == snapshot
assert first["passed"] is True
assert first["passed_checks"] == first["total_checks"] == len(gates)
assert RAW_OUTPUT not in repr(first), "verdict leaked raw model output"

failed = canonical_outcome("failed")
implicit_terminal_gate = evaluate_outcome(
    failed, [{"type": "max_total_tokens", "value": 18}]
)
assert implicit_terminal_gate["passed"] is False
assert implicit_terminal_gate["checks"][-1]["gate"] == "runtime_completed"
expected_failure = evaluate_outcome(
    failed, [{"type": "terminal_status", "status": "failed"}]
)
assert expected_failure["passed"] is True

legacy = canonical_outcome()
legacy.pop("invocation_start_message_index")
legacy_status_usage = evaluate_outcome(
    legacy,
    [
        {"type": "terminal_status", "status": "completed"},
        {"type": "max_total_tokens", "value": 18},
    ],
)
assert legacy_status_usage["passed"] is True
try:
    evaluate_outcome(legacy, [{"type": "output_exact", "value": RAW_OUTPUT}])
except ValueError as error:
    assert "invocation_start_message_index" in str(error)
    assert RAW_OUTPUT not in str(error)
else:
    raise AssertionError("legacy outcome must not run message-derived gates")

historical_only_match = canonical_outcome()
historical_only_match["messages"].append(
    {
        "role": "user",
        "content": [{"type": "text", "text": "new invocation with no answer"}],
    }
)
historical_only_match["invocation_start_message_index"] = 2
historical_verdict = evaluate_outcome(
    historical_only_match,
    [
        {"type": "output_exact", "value": RAW_OUTPUT},
        {"type": "called_tool", "name": "search"},
    ],
)
assert historical_verdict["passed"] is False
assert [check["passed"] for check in historical_verdict["checks"]] == [False, False]
assert RAW_OUTPUT not in repr(historical_verdict)

unknown_outcome = canonical_outcome()
unknown_outcome["messages"][0]["content"][1]["unexpected"] = RAW_OUTPUT
try:
    evaluate_outcome(unknown_outcome, gates)
except ValueError as error:
    assert RAW_OUTPUT not in str(error)
else:
    raise AssertionError("unknown nested RunOutcome fields must fail closed")

try:
    evaluate_outcome(RAW_OUTPUT, gates)
except ValueError as error:
    assert RAW_OUTPUT not in str(error)
else:
    raise AssertionError("malformed RunOutcome must fail closed")

try:
    evaluate_outcome(outcome, [{"type": "no_tool_errors", "unexpected": True}])
except ValueError:
    pass
else:
    raise AssertionError("unknown eval gate fields must fail closed")

try:
    evaluate_outcome(outcome, [])
except ValueError:
    pass
else:
    raise AssertionError("empty gate sequences must fail closed")

print("EVAL_BINDING_OK=python")
