"use strict";

// Keyless binding conformance for RunOptions, Client, AbortSignal, and early-loop finalization.
const { Agent, Client } = require("../../crates/aikit-node");

async function drain(stream, expectedErrorCode) {
  let seenErrorCode;
  for await (const delta of stream) {
    if (delta.type === "error") seenErrorCode = delta.info.code;
  }
  if (expectedErrorCode != null && seenErrorCode !== expectedErrorCode) {
    throw new Error("Node StreamDelta ErrorInfo drift");
  }
  return stream.outcome();
}

async function main() {
  const agent = Agent.fromEnv({});
  const clientOutcome = await drain(
    new Client(agent).query("client parity", {
      model: "mock-1",
      fallbackModels: ["mock-2"],
      maxTokens: 64,
      maxTurns: 2,
      providerOptions: { mock: { tag: "client" } },
      retry: { maxAttemptsPerModel: 1 },
    }),
  );
  const maxTurnsOutcome = await drain(
    agent.run("turn parity", { maxTurns: 0 }),
    "max_turns",
  );
  const budgetOutcome = await drain(
    agent.run("budget parity", { budget: { maxTotalTokens: 0 } }),
    "budget_exceeded",
  );
  let errorCode;
  try {
    agent.run("typed error parity", { model: "not-a-real-model" });
  } catch (error) {
    errorCode = error.code;
    if (error.info?.code !== errorCode) {
      throw new Error("Node typed AgentError envelope drift");
    }
  }
  if (errorCode == null) throw new Error("unknown model unexpectedly started");

  const beforeController = new AbortController();
  beforeController.abort();
  const before = agent.run("cancel before first pull", {
    signal: beforeController.signal,
  });
  const cancelBeforeOutcome = await before.close();

  const blocked = Agent.fromEnv({});
  let enteredResolve;
  const entered = new Promise((resolve) => {
    enteredResolve = resolve;
  });
  const stopReasons = [];
  let toolCalls = 0;
  blocked.onUserPrompt(async () => {
    enteredResolve();
    await new Promise(() => {});
  });
  blocked.onStop(async (context) => {
    stopReasons.push(context.reason);
  });
  blocked.addTool(
    "forbidden",
    "must not run after cancellation",
    { type: "object" },
    async () => {
      toolCalls += 1;
      return "should not run";
    },
  );
  const controller = new AbortController();
  const during = blocked.run("cancel while UserPrompt is blocked", {
    signal: controller.signal,
  });
  const pending = during.next();
  await entered;
  controller.abort();
  await pending;
  const cancelDuringOutcome = await during.close();

  // `for await ... break` must invoke iterator.return(), which awaits native close().
  const breakStream = Agent.fromEnv({}).run("break finalization");
  for await (const _delta of breakStream) break;
  if (breakStream.outcome().terminal_status === "running") {
    throw new Error("for-await break did not finalize QueryStream");
  }

  let routedModel;
  const routed = agent.run("route through caller catalog", {
    routing: {
      profiles: [
        {
          provider: "mock",
          model: "mock-routed",
          context_window_tokens: 8192,
          max_output_tokens: 1024,
          pricing: null,
          quality_score: 100,
          skills: [],
          capabilities: [],
        },
      ],
      request: {
        policy: { kind: "automatic", objective: "quality" },
        active_providers: [],
        estimated_input_tokens: 8,
        required_output_tokens: 64,
        max_cost_usd: null,
        required_skills: [],
        required_capabilities: [],
      },
    },
  });
  for await (const delta of routed) {
    if (delta.type === "message_start") routedModel = delta.model;
  }
  if (routedModel !== "mock-routed") {
    throw new Error(`normal run routing selected ${routedModel ?? "nothing"}`);
  }

  const result = {
    budget: budgetOutcome.terminal_status,
    cancel_before: cancelBeforeOutcome.terminal_status,
    cancel_during: cancelDuringOutcome.terminal_status,
    client: clientOutcome.terminal_status,
    error_code: errorCode,
    max_turns: maxTurnsOutcome.terminal_status,
    stop_reasons: stopReasons,
    tool_calls: toolCalls,
  };
  console.log(`RUN_OPTIONS_JSON=${JSON.stringify(result)}`);
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
