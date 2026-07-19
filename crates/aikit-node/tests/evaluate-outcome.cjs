"use strict";

// Real Node FFI contract for deterministic outcome evaluation.
const assert = require("node:assert/strict");
const { evaluateOutcome } = require("..");

const rawOutput = "RAW_MODEL_OUTPUT_MUST_NOT_LEAK";

function canonicalOutcome(status = "completed") {
  return {
    messages: [
      {
        role: "assistant",
        content: [
          { type: "tool_use", id: "call-1", name: "search", input: {} },
          { type: "text", text: rawOutput },
        ],
      },
      {
        role: "tool",
        content: [
          {
            type: "tool_result",
            tool_use_id: "call-1",
            content: "ok",
            is_error: false,
          },
        ],
      },
    ],
    usage: {
      input_tokens: 7,
      output_tokens: 11,
      cache_creation_input_tokens: 0,
      cache_read_input_tokens: 0,
      reasoning_tokens: 0,
    },
    terminal_status: status,
    stop_reason: "stop",
    model_attempts: ["mock-1"],
    final_text: "ignored convenience projection",
    invocation_start_message_index: 0,
  };
}

const gates = [
  { type: "output_exact", value: rawOutput },
  { type: "called_tool", name: "search" },
  { type: "tool_sequence", names: ["search"], exact: true },
  { type: "no_tool_errors" },
  { type: "max_turns", value: 1 },
  { type: "max_total_tokens", value: 18 },
  { type: "max_model_attempts", value: 1 },
];

const outcome = canonicalOutcome();
const snapshot = structuredClone(outcome);
const first = evaluateOutcome(outcome, gates);
const second = evaluateOutcome(outcome, gates);
assert.deepEqual(first, second);
assert.deepEqual(outcome, snapshot);
assert.equal(first.passed, true);
assert.equal(first.passed_checks, gates.length);
assert.equal(first.total_checks, gates.length);
assert(!JSON.stringify(first).includes(rawOutput), "verdict leaked raw model output");

const failed = canonicalOutcome("failed");
const implicitTerminalGate = evaluateOutcome(
  failed,
  [{ type: "max_total_tokens", value: 18 }],
);
assert.equal(implicitTerminalGate.passed, false);
assert.equal(implicitTerminalGate.checks.at(-1).gate, "runtime_completed");
assert.equal(
  evaluateOutcome(failed, [{ type: "terminal_status", status: "failed" }]).passed,
  true,
);

const legacy = canonicalOutcome();
delete legacy.invocation_start_message_index;
assert.equal(
  evaluateOutcome(legacy, [
    { type: "terminal_status", status: "completed" },
    { type: "max_total_tokens", value: 18 },
  ]).passed,
  true,
);
assert.throws(
  () => evaluateOutcome(legacy, [{ type: "output_exact", value: rawOutput }]),
  (error) =>
    String(error).includes("invocation_start_message_index") &&
    !String(error).includes(rawOutput),
);

const historicalOnlyMatch = canonicalOutcome();
historicalOnlyMatch.messages.push({
  role: "user",
  content: [{ type: "text", text: "new invocation with no answer" }],
});
historicalOnlyMatch.invocation_start_message_index = 2;
const historicalVerdict = evaluateOutcome(historicalOnlyMatch, [
  { type: "output_exact", value: rawOutput },
  { type: "called_tool", name: "search" },
]);
assert.equal(historicalVerdict.passed, false);
assert.deepEqual(historicalVerdict.checks.map((check) => check.passed), [false, false]);
assert(!JSON.stringify(historicalVerdict).includes(rawOutput));

const unknownOutcome = canonicalOutcome();
unknownOutcome.messages[0].content[1].unexpected = rawOutput;
assert.throws(
  () => evaluateOutcome(unknownOutcome, gates),
  (error) => !String(error).includes(rawOutput),
);
assert.throws(
  () => evaluateOutcome(rawOutput, gates),
  (error) => !String(error).includes(rawOutput),
);
assert.throws(() =>
  evaluateOutcome(outcome, [{ type: "no_tool_errors", unexpected: true }]),
);
assert.throws(() => evaluateOutcome(outcome, []));

console.log("EVAL_BINDING_OK=node");
