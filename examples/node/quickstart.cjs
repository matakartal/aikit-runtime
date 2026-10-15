"use strict";

// aikit Node quickstart — the smallest tour of the binding, keyless and deterministic.
//
// Every call below runs against the built-in MockProvider (model "mock-1"), so it needs no API
// key and makes no network request. The mock's canned reply text is a fixed Turkish sentence;
// that is expected. Swapping in a live provider later changes only the agent's keys and the
// `model` option — see examples/node/live_provider.cjs.
//
// Prerequisite (once, from the repository root):  ./scripts/build-node.sh
// Run:                                            node examples/node/quickstart.cjs
//
// Steps:
//   1. Create a keyless Agent
//   2. generateText — one complete answer
//   3. streamText   — the same answer as typed deltas
//   4. A host tool, governed by permission rules (allow runs it, deny blocks it)
//   5. An "ask" rule resolved by an async approval callback
//   6. Structured output validated against a JSON Schema
//   7. Agent.run + evaluateOutcome — gate a recorded run without another model call

const path = require("node:path");

function loadBinding() {
  const entry = path.join(__dirname, "..", "..", "crates", "aikit-node", "index.js");
  try {
    return require(entry);
  } catch (error) {
    console.error(
      "Could not load the aikit Node binding from crates/aikit-node.\n" +
        "Build it first from the repository root:\n\n" +
        "    ./scripts/build-node.sh\n\n" +
        `Underlying error: ${error.message}`,
    );
    process.exit(1);
  }
}

const { Agent, evaluateOutcome, tool } = loadBinding();

function heading(title) {
  console.log(`\n== ${title}`);
}

async function main() {
  // 1. Agent.fromEnv({}) ignores your shell environment, so no real key is ever picked up here.
  //    `new Agent()` would instead discover ANTHROPIC_API_KEY, OPENAI_API_KEY, ... from
  //    process.env.
  heading("1. keyless agent");
  const agent = Agent.fromEnv({});
  console.log("active providers:", agent.activeProviders()); // [] — only the mock is available

  // 2. generateText resolves once with the full text, token usage, and stop reason.
  heading("2. generateText");
  const answer = await agent.generateText("Say hello");
  console.log("text:", answer.text);
  console.log("usage:", answer.usage.input_tokens, "in /", answer.usage.output_tokens, "out");
  console.log("stop reason:", answer.stop_reason);

  // 3. streamText returns an async iterable of StreamDelta values (see index.d.ts).
  heading("3. streamText");
  let streamed = "";
  for await (const delta of agent.streamText("Say hello")) {
    if (delta.type === "text_delta") streamed += delta.text;
    if (delta.type === "message_stop") console.log("stream stopped:", delta.stop_reason);
  }
  console.log("streamed text matches generateText:", streamed === answer.text);

  // 4. Register a host tool. The callback is a plain async JS function that receives the
  //    model's (schema-shaped) input and returns a string result.
  heading("4. governed tool: allow vs deny");
  let lookups = 0;
  agent.addToolDefinition(
    tool(
      "lookup_price",
      "Look up the latest price for one ticker symbol",
      {
        type: "object",
        required: ["symbol"],
        properties: { symbol: { type: "string" } },
      },
      async (input) => {
        lookups += 1;
        return `price for ${input.symbol ?? "?"}: 42.00`;
      },
    ),
  );

  // A live model decides on its own which tool to call and with what input. The mock is told
  // instead, via its keyless test fixture `providerOptions.mock`; nothing else differs.
  const askForPrice = {
    providerOptions: { mock: { tool_name: "lookup_price", tool_input: { symbol: "AAPL" } } },
  };

  // allow → the tool runs and its result flows back into the conversation.
  agent.setPermissions([{ effect: "allow", tool: "lookup_price" }]);
  for await (const delta of agent.run("What does AAPL cost?", askForPrice)) {
    if (delta.type === "tool_result") {
      console.log(`allow → tool_result (is_error=${delta.is_error}):`, delta.content);
    }
  }
  console.log("tool callback ran", lookups, "time(s)");

  // deny → the engine refuses the call before your callback is reached; the model sees an error.
  agent.setPermissions([{ effect: "deny", tool: "lookup_price" }]);
  const before = lookups;
  for await (const delta of agent.run("What does AAPL cost?", askForPrice)) {
    if (delta.type === "tool_result") {
      console.log(`deny  → tool_result (is_error=${delta.is_error}):`, delta.content);
    }
  }
  console.log("tool callback ran during deny:", lookups - before, "time(s)");
  if (lookups !== before) throw new Error("a denied tool must never run");

  // 5. "ask" defers the decision to canUseTool. The approver can allow, deny, or rewrite input.
  heading("5. ask → approval callback");
  agent.setPermissions([{ effect: "ask", tool: "lookup_price" }]);
  agent.canUseTool(async (request) => {
    console.log("approval requested for", request.tool, "with input", request.input);
    return { decision: "allow", updated_input: { symbol: "MSFT" } }; // approve, with edited input
  });
  for await (const delta of agent.run("What does AAPL cost?", askForPrice)) {
    if (delta.type === "tool_result") console.log("approved tool_result:", delta.content);
  }

  // 6. generateObject validates the model output against a JSON Schema (and repairs/retries
  //    within maxRetries). A Zod v4 schema also works when the optional `zod` peer is installed.
  heading("6. structured output");
  const invoice = await agent.generateObject("Return the invoice status", {
    type: "object",
    required: ["currency", "status"],
    properties: {
      currency: { type: "string", enum: ["EUR"] },
      status: { type: "string", enum: ["ok"] },
    },
  });
  console.log("value:", invoice.value);
  console.log("fidelity:", invoice.fidelity, "| attempts:", invoice.attempts);

  // 7. Agent.run exposes the full RunOptions surface (limits, budget, retry, fallbacks, signal)
  //    and records a RunOutcome you can gate deterministically — no extra model or network work.
  heading("7. run + evaluateOutcome");
  const stream = Agent.fromEnv({}).run("Summarize the quarter", { maxTurns: 2, maxTokens: 256 });
  for await (const _delta of stream) {
    // Drain the stream; outcome() is final once iteration ends.
  }
  const outcome = stream.outcome();
  console.log("terminal status:", outcome.terminal_status);
  console.log("models tried:", outcome.model_attempts);
  const verdict = evaluateOutcome(outcome, [
    { type: "terminal_status", status: "completed" },
    { type: "no_tool_errors" },
    { type: "max_total_tokens", value: 2_000 },
  ]);
  console.log(`gates: ${verdict.passed_checks}/${verdict.total_checks} passed`);
  if (!verdict.passed) throw new Error("evaluation gates failed");

  // Typed errors: startup failures carry a stable `code` you can branch on.
  try {
    agent.run("hi", { model: "not-a-real-model" });
  } catch (error) {
    console.log("typed error code:", error.code);
  }

  console.log("\nquickstart OK. Next: examples/node/README.md lists the deeper examples.");
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
