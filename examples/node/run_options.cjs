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
  for (const [options, field] of [
    [{ budegt: { maxTotalTokens: 0 } }, "budegt"],
    [{ budget: { maxTotalTokenz: 0 } }, "maxTotalTokenz"],
    [{ retry: { maxAttemptsPerModal: 1 } }, "maxAttemptsPerModal"],
    [
      { compaction: { maxContextTokens: 100, keepRecentMessagez: 2 } },
      "keepRecentMessagez",
    ],
    [{ signal: {} }, "AbortSignal"],
  ]) {
    let rejected = false;
    try {
      agent.run("invalid options must fail closed", options);
    } catch (error) {
      rejected = error.message.includes(field);
    }
    if (!rejected) throw new Error(`Node silently ignored invalid option ${field}`);
  }
  for (const [options, snake, camel] of [
    [
      { budget: { max_total_tokens: 100, maxTotalTokens: 100 } },
      "max_total_tokens",
      "maxTotalTokens",
    ],
    [
      {
        budget: {
          pricing: {
            input_per_million_usd: 1,
            inputPerMillionUsd: 1,
            outputPerMillionUsd: 2,
          },
        },
      },
      "input_per_million_usd",
      "inputPerMillionUsd",
    ],
    [
      { retry: { max_attempts_per_model: 1, maxAttemptsPerModel: 1 } },
      "max_attempts_per_model",
      "maxAttemptsPerModel",
    ],
    [
      { compaction: { max_context_tokens: 100, maxContextTokens: 100 } },
      "max_context_tokens",
      "maxContextTokens",
    ],
  ]) {
    let rejected = false;
    try {
      agent.run("duplicate aliases must fail closed", options);
    } catch (error) {
      rejected =
        error.message.includes("duplicate aliases") &&
        error.message.includes(snake) &&
        error.message.includes(camel);
    }
    if (!rejected) {
      throw new Error(`Node accepted duplicate aliases ${snake}/${camel}`);
    }
  }
  for (const [options, field] of [
    [{ budegt: { max_model_calls: 0 } }, "budegt"],
    [{ budget: { max_model_callz: 0 } }, "max_model_callz"],
  ]) {
    let rejected = false;
    try {
      agent.fanOut([], [], options);
    } catch (error) {
      rejected = error.message.includes(field);
    }
    if (!rejected) {
      throw new Error(`Node silently ignored invalid orchestration option ${field}`);
    }
  }
  let subtaskRejected = false;
  try {
    agent.subtask("invalid", "prompt", "mock-1", { maxTurnz: 1 });
  } catch (error) {
    subtaskRejected = error.message.includes("maxTurnz");
  }
  if (!subtaskRejected) throw new Error("Node silently ignored invalid subtask option");
  for (const [rule, field] of [
    [{ effect: "allow", tool: "lookup", pattrn: "AAPL" }, "pattrn"],
    [{ effect: "allow", tool: "lookup", field: "symbol" }, "requires pattern"],
  ]) {
    let rejected = false;
    try {
      agent.setPermissions([rule]);
    } catch (error) {
      rejected = error.message.includes(field);
    }
    if (!rejected) throw new Error(`Node accepted unsafe permission rule ${field}`);
  }
  for (const [operation, field] of [
    [() => agent.streamText("invalid options", { maxTokenz: 1 }), "maxTokenz"],
    [
      () => agent.enableBashWithRequiredContainment({ image: "invalid", pidsLmit: 1 }),
      "pidsLmit",
    ],
  ]) {
    let rejected = false;
    try {
      operation();
    } catch (error) {
      rejected = error.message.includes(field);
    }
    if (!rejected) throw new Error(`Node silently ignored invalid ${field}`);
  }
  let textOptionsRejected = false;
  try {
    await agent.generateText("invalid options", { maxTokenz: 1 });
  } catch (error) {
    textOptionsRejected = error.message.includes("maxTokenz");
  }
  if (!textOptionsRejected) throw new Error("Node ignored invalid generateText options");
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

  // A pre-aborted signal used to start native close() and the iterator's first next() in parallel,
  // intermittently tripping QueryStream's intentional single-consumer guard. Exercise the race
  // repeatedly so cancellation remains deterministic for both direct and for-await consumers.
  for (let attempt = 0; attempt < 128; attempt += 1) {
    const controller = new AbortController();
    controller.abort();
    const preAborted = agent.run("pre-aborted iteration", {
      signal: controller.signal,
    });
    for await (const _delta of preAborted) {
      throw new Error("pre-aborted iteration unexpectedly emitted a delta");
    }
    if (preAborted.outcome().terminal_status !== "cancelled") {
      throw new Error("pre-aborted iteration did not finalize as cancelled");
    }
  }

  for (let attempt = 0; attempt < 64; attempt += 1) {
    const controller = new AbortController();
    controller.abort();
    const preAborted = agent.run("pre-aborted direct pull", {
      signal: controller.signal,
    });
    if ((await preAborted.next()) !== null) {
      throw new Error("pre-aborted direct next unexpectedly emitted a delta");
    }
    if (preAborted.outcome().terminal_status !== "cancelled") {
      throw new Error("pre-aborted direct next did not finalize as cancelled");
    }
  }

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

  const directBlocked = Agent.fromEnv({});
  let directEnteredResolve;
  const directEntered = new Promise((resolve) => {
    directEnteredResolve = resolve;
  });
  directBlocked.onUserPrompt(async () => {
    directEnteredResolve();
    await new Promise(() => {});
  });
  const directClose = directBlocked.run("close while next is blocked");
  const directPending = directClose.next();
  await directEntered;
  let closeTimer;
  const directCloseOutcome = await Promise.race([
    directClose.close(),
    new Promise((_, reject) => {
      closeTimer = setTimeout(
        () => reject(new Error("direct close did not cancel the pending next")),
        1_000,
      );
    }),
  ]).finally(() => clearTimeout(closeTimer));
  await directPending;
  if (directCloseOutcome.terminal_status !== "cancelled") {
    throw new Error("direct close did not finalize as cancelled");
  }

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
