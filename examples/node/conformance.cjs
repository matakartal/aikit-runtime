"use strict";

// Keyless, canonical public-surface conformance for the Node binding. Dynamic IDs, timestamps,
// durations, and raw error text are intentionally normalized away before byte comparison.
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { Agent, Client } = require("../../crates/aikit-node");

function canonical(value) {
  if (Array.isArray(value)) return value.map(canonical);
  if (value != null && typeof value === "object") {
    return Object.fromEntries(
      Object.keys(value)
        .sort()
        .map((key) => [key, canonical(value[key])]),
    );
  }
  return value;
}

function emit(module, value) {
  console.log(`CONFORMANCE_${module.toUpperCase()}_JSON=${JSON.stringify(canonical(value))}`);
}

async function drain(stream) {
  const errorCodes = [];
  const events = [];
  for await (const event of stream) {
    events.push(event);
    if (event.type === "error") errorCodes.push(event.info.code);
  }
  return [stream.outcome(), errorCodes, events];
}

async function governanceFacts() {
  const agent = Agent.fromEnv({});
  const events = [];
  const approvalInputs = [];
  const toolInputs = [];
  agent.addTool(
    "search",
    "search",
    {
      type: "object",
      required: ["q"],
      properties: { q: { type: "string" } },
    },
    async (value) => {
      events.push("tool");
      toolInputs.push(value.q);
      return `rows for ${value.q}`;
    },
  );
  agent.onUserPrompt(async (context) => {
    events.push("prompt");
    return { action: "rewrite", prompt: `${context.prompt} [checked]` };
  });
  agent.onPreToolUse(async () => {
    events.push("pre");
    return { action: "rewrite", input: { q: "pre-approved" } };
  }, "search");
  agent.onPostToolUse(async (context) => {
    events.push("post");
    return { action: "rewrite", output: `post:${context.output}` };
  }, "search");
  agent.onStop(async () => {
    events.push("stop");
  });
  agent.canUseTool(async (context) => {
    events.push("approve");
    approvalInputs.push(context.input.q);
    return {
      decision: "allow",
      updated_input: { q: "approved" },
      updated_permissions: ["allow_exact_input"],
    };
  });
  agent.setPermissions([{ id: "ask", effect: "ask", tool: "search" }]);
  const generated = await agent.generateText("governed");
  const result = generated.messages
    .flatMap((message) => message.content)
    .find((block) => block.type === "tool_result" && !block.is_error).content;

  const denied = Agent.fromEnv({});
  let denyCalls = 0;
  const denyStages = [];
  denied.addTool("guarded", "guarded", { type: "object" }, async () => {
    denyCalls += 1;
    return "must not run";
  });
  denied.onFailure(async (context) => {
    denyStages.push(context.stage);
  });
  denied.setPermissions([
    { id: "early-allow", effect: "allow", tool: "guarded" },
    { id: "authoritative-deny", effect: "deny", tool: "guarded" },
  ]);
  const [, , denyEvents] = await drain(denied.run("deny wins"));
  const denyResults = denyEvents.filter((event) => event.type === "tool_result");

  const invalid = Agent.fromEnv({});
  let invalidCalls = 0;
  const invalidStages = [];
  invalid.addTool(
    "typed",
    "typed",
    {
      type: "object",
      required: ["count"],
      properties: { count: { type: "integer" } },
    },
    async () => {
      invalidCalls += 1;
      return "must not run";
    },
  );
  invalid.onFailure(async (context) => {
    invalidStages.push(context.stage);
  });
  const [, , invalidEvents] = await drain(invalid.run("invalid tool input"));
  const invalidResults = invalidEvents.filter((event) => event.type === "tool_result");

  const interrupted = Agent.fromEnv({});
  let interruptCalls = 0;
  const interruptStops = [];
  interrupted.addTool("interrupt", "interrupt", { type: "object" }, async () => {
    interruptCalls += 1;
    return "must not run";
  });
  interrupted.setPermissions([{ effect: "ask", tool: "interrupt" }]);
  interrupted.canUseTool(async () => ({
    decision: "deny",
    message: "operator stopped",
    interrupt: true,
  }));
  interrupted.onStop(async (context) => {
    interruptStops.push(context.reason);
  });
  const [, interruptCodes] = await drain(interrupted.run("interrupt"));

  return {
    approval: {
      approval_inputs: approvalInputs,
      events,
      result,
      tool_inputs: toolInputs,
    },
    authoritative_deny: {
      failure_stages: denyStages,
      is_error: denyResults.length === 1 && denyResults[0].is_error,
      tool_calls: denyCalls,
    },
    interrupt: {
      error_codes: interruptCodes,
      stop_reasons: interruptStops,
      tool_calls: interruptCalls,
    },
    schema_validation: {
      failure_stages: invalidStages,
      is_error: invalidResults.length === 1 && invalidResults[0].is_error,
      tool_calls: invalidCalls,
    },
  };
}

async function structuredFacts() {
  const agent = Agent.fromEnv({});
  const schema = {
    type: "object",
    required: ["currency", "status"],
    properties: {
      currency: { type: "string", enum: ["EUR"] },
      status: { type: "string", enum: ["ok"] },
    },
  };
  const eventTypes = [];
  const deltaTypes = [];
  let completed;
  for await (const event of agent.streamObject("structured", schema, {
    providerOptions: { mock: { temperature: 0, tag: "parity" } },
  })) {
    eventTypes.push(event.type);
    if (event.type === "delta") deltaTypes.push(event.delta.type);
    else if (event.type === "completed") completed = event.object;
  }
  if (completed == null) throw new Error("structured stream did not complete");

  const repair = [];
  let repairFailed = false;
  try {
    for await (const event of agent.streamObject(
      "repair",
      {
        type: "object",
        required: ["value"],
        properties: { value: { type: "string", minLength: 8 } },
      },
      { maxRetries: 1 },
    )) {
      if (event.type === "attempt_started") repair.push(["attempt_started", event.repair]);
      else if (event.type === "validation_failed") {
        repair.push(["validation_failed", event.will_retry]);
      }
    }
  } catch (_error) {
    repairFailed = true;
  }
  return {
    attempts: completed.attempts,
    delta_types: deltaTypes,
    event_types: eventTypes,
    fidelity: completed.fidelity,
    provider_metadata_empty: Object.keys(completed.provider_metadata).length === 0,
    repair,
    repair_failed: repairFailed,
    value: [completed.value.currency, completed.value.status],
  };
}

async function inputFacts() {
  const agent = Agent.fromEnv({});
  const messages = [
    {
      role: "user",
      content: [
        { type: "text", text: "multimodal" },
        {
          type: "media",
          media_type: "image/png",
          source: { kind: "base64", data: "aGVsbG8=" },
        },
      ],
    },
  ];
  const schema = {
    type: "object",
    required: ["status"],
    properties: { status: { type: "string", const: "ok" } },
    additionalProperties: false,
  };

  const compatibility = await agent.generateText("string compatibility");
  if (
    compatibility.messages[0]?.content[0]?.type !== "text" ||
    compatibility.messages[0].content[0].text !== "string compatibility"
  ) {
    throw new Error("string model input compatibility drifted");
  }

  const generated = await agent.generateText(messages);
  const [streamedText] = await drain(agent.streamText(messages));
  if (
    JSON.stringify(canonical(streamedText.messages.slice(0, 1))) !==
    JSON.stringify(canonical(messages))
  ) {
    throw new Error("streamText flattened canonical media input");
  }
  const media = generated.messages
    .flatMap((message) => message.content)
    .find((block) => block.type === "media");
  if (media == null) throw new Error("generateText dropped canonical media input");
  const textRoles = generated.messages
    .filter((message) =>
      message.content.some(
        (block) => block.type === "text" && block.text === "multimodal",
      ),
    )
    .map((message) => message.role);

  const structured = await agent.generateObject(messages, schema);
  let streamedObject;
  for await (const event of agent.streamObject(messages, schema)) {
    if (event.type === "completed") streamedObject = event.object;
  }
  if (streamedObject?.value?.status !== "ok") {
    throw new Error("streamObject canonical input did not complete");
  }

  const [routed] = await drain(
    agent.run(messages, {
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
    }),
  );

  return {
    media_input: {
      media_type: media.media_type,
      source_kind: media.source.kind,
      text_roles: textRoles,
    },
    routing: { model_attempts: routed.model_attempts },
    structured: {
      fidelity: structured.fidelity,
      status: structured.value.status,
    },
  };
}

async function runOptionsFacts() {
  const agent = Agent.fromEnv({});
  const [client, clientCodes] = await drain(
    new Client(agent).query("client", {
      model: "mock-1",
      fallbackModels: ["mock-2"],
      maxTokens: 64,
      maxTurns: 2,
      providerOptions: { mock: { tag: "parity" } },
      retry: {
        maxAttemptsPerModel: 2,
        baseDelayMs: 0,
        maxDelayMs: 0,
        perAttemptTimeoutMs: 1000,
      },
    }),
  );
  const [priced, pricedCodes] = await drain(
    agent.run("priced", {
      budget: {
        maxCostUsd: 1.0,
        pricing: { inputPerMillionUsd: 1.0, outputPerMillionUsd: 2.0 },
      },
    }),
  );
  const [limited, limitedCodes] = await drain(agent.run("limited", { maxTurns: 0 }));
  const [budget, budgetCodes] = await drain(
    agent.run("budget", { budget: { maxTotalTokens: 0 } }),
  );
  const cancelledStream = agent.run("cancelled");
  cancelledStream.cancel();
  const [cancelled, cancelCodes] = await drain(cancelledStream);
  return {
    budget: [budget.terminal_status, budgetCodes],
    cancel: [cancelled.terminal_status, cancelCodes],
    client: [client.terminal_status, client.model_attempts, clientCodes],
    max_turns: [limited.terminal_status, limitedCodes],
    priced_budget: [priced.terminal_status, pricedCodes],
  };
}

async function stateFacts() {
  const agent = Agent.fromEnv({});
  const stopReasons = [];
  agent.onStop(async (context) => {
    stopReasons.push(context.reason);
  });
  const generated = await agent.generateText("state");
  agent.remember("customer_note", "Ada prefers EUR");
  const memory = agent.recall("EUR", 3).map((entry) => [entry.key, entry.value]);
  return {
    audit: {
      advertised: agent.capabilities().runtime_features.includes("audit"),
      stop_reasons: stopReasons,
    },
    memory,
    provider_metadata_empty: Object.keys(generated.provider_metadata).length === 0,
    session: {
      roles: generated.messages.map((message) => message.role),
      stop_reason: generated.stop_reason,
    },
  };
}

function profiles() {
  return [
    {
      provider: "mock",
      model: "mock-1",
      context_window_tokens: 8192,
      max_output_tokens: 1024,
      pricing: null,
      quality_score: 1,
      skills: [],
      capabilities: [],
    },
  ];
}

function childSpec(id, prompt, allowedTools = []) {
  return {
    id,
    prompt,
    system: null,
    route: {
      policy: { kind: "explicit", model: "mock-1" },
      max_cost_usd: null,
      required_skills: [],
      required_capabilities: [],
    },
    allowed_tools: allowedTools,
    max_turns: 2,
    max_tokens: 64,
    estimated_input_tokens: 8,
  };
}

async function orchestrationFacts() {
  const agent = Agent.fromEnv({});
  const events = [];
  let approvals = 0;
  let calls = 0;
  agent.addTool(
    "child_search",
    "child search",
    {
      type: "object",
      required: ["q"],
      properties: { q: { type: "string" } },
    },
    async (value) => {
      calls += 1;
      events.push("tool");
      return `child:${value.q}`;
    },
  );
  agent.onPreToolUse(async () => {
    events.push("pre");
    return { action: "rewrite", input: { q: "child-pre" } };
  }, "child_search");
  agent.onPostToolUse(async (context) => {
    events.push("post");
    return { action: "rewrite", output: `post:${context.output}` };
  }, "child_search");
  agent.onStop(async () => {
    events.push("stop");
  });
  agent.canUseTool(async () => {
    approvals += 1;
    events.push("approve");
    return { decision: "allow", updated_permissions: ["allow_tool"] };
  });
  agent.setPermissions([{ effect: "ask", tool: "child_search" }]);
  const budget = {
    max_model_calls: 8,
    max_input_tokens: 8192,
    max_output_tokens: 8192,
    wall_time_ms: 5000,
  };
  const options = { maxParallelism: 2, budget };
  const first = await agent.runSubagent(
    childSpec("thread", "first", ["child_search"]),
    profiles(),
    options,
  );
  const resumed = await agent.resumeSubagent(
    "thread",
    childSpec("thread-resume", "second", ["child_search"]),
    profiles(),
    options,
  );
  const toolResult = first.outcome.messages
    .flatMap((message) => message.content)
    .find((block) => block.type === "tool_result" && !block.is_error).content;

  const plain = Agent.fromEnv({});
  const fan = await plain.fanOut(
    [childSpec("fan-a", "A"), childSpec("fan-b", "B")],
    profiles(),
    options,
  );
  const deadline = await plain.runSubagent(
    childSpec("expired", "expired"),
    profiles(),
    { budget: { wall_time_ms: 0 } },
  );
  return {
    context: {
      approval_calls: approvals,
      events,
      status: first.status,
      tool_calls: calls,
      tool_result: toolResult,
    },
    deadline: {
      code: deadline.error_info?.code ?? null,
      status: deadline.status,
      terminal: deadline.outcome.terminal_status,
    },
    fan_out: {
      ids: fan.map((result) => result.id),
      statuses: fan.map((result) => result.status),
    },
    resume: {
      message_counts: [first.outcome.messages.length, resumed.outcome.messages.length],
      revisions: [first.session_revision, resumed.session_revision],
      statuses: [first.status, resumed.status],
    },
  };
}

function outcomeToolResult(outcome) {
  return outcome.messages
    .flatMap((message) => message.content)
    .find((block) => block.type === "tool_result");
}

function outcomeUsedTool(outcome, expected) {
  return outcome.messages.some((message) =>
    message.content.some(
      (block) => block.type === "tool_use" && block.name === expected,
    ),
  );
}

async function forceBuiltin(agent, name, toolInput) {
  const [outcome] = await drain(
    agent.run(`deterministic built-in fixture: ${name}`, {
      providerOptions: {
        mock: { tool_name: name, tool_input: toolInput },
      },
    }),
  );
  return outcome;
}

async function builtinsFacts() {
  const fileToolNames = ["Read", "Write", "Edit", "Grep", "Glob"];
  const primary = fs.mkdtempSync(path.join(os.tmpdir(), "aikit-conformance-primary-"));
  const secondary = fs.mkdtempSync(path.join(os.tmpdir(), "aikit-conformance-secondary-"));
  const outside = fs.mkdtempSync(path.join(os.tmpdir(), "aikit-conformance-outside-"));
  try {
    const secondaryFile = path.join(secondary, "secondary.txt");
    const outsideFile = path.join(outside, "outside.txt");
    fs.writeFileSync(secondaryFile, "secondary-ok", "utf8");
    fs.writeFileSync(outsideFile, "outside-secret", "utf8");
    fs.symlinkSync(outsideFile, path.join(primary, "escape-link.txt"));

    const agent = Agent.fromEnv({});
    agent.registerBuiltinTools([primary, secondary]);
    const defaultNames = agent.capabilities().tools;

    const written = await forceBuiltin(agent, "Write", {
      path: "roundtrip.txt",
      content: "before needle",
    });
    const readBefore = await forceBuiltin(agent, "Read", { path: "roundtrip.txt" });
    const edited = await forceBuiltin(agent, "Edit", {
      path: "roundtrip.txt",
      old_string: "before",
      new_string: "after",
    });
    const readAfter = await forceBuiltin(agent, "Read", { path: "roundtrip.txt" });
    const grep = await forceBuiltin(agent, "Grep", { pattern: "after", path: "." });
    const glob = await forceBuiltin(agent, "Glob", { pattern: "*.txt" });
    const multiRoot = await forceBuiltin(agent, "Read", { path: secondaryFile });
    const outsideDenial = await forceBuiltin(agent, "Read", { path: outsideFile });
    const symlinkDenial = await forceBuiltin(agent, "Read", {
      path: "escape-link.txt",
    });
    const strictSchema = await forceBuiltin(agent, "Read", {
      path: "roundtrip.txt",
      unexpected: true,
    });

    const writeResult = outcomeToolResult(written);
    const beforeResult = outcomeToolResult(readBefore);
    const editResult = outcomeToolResult(edited);
    const afterResult = outcomeToolResult(readAfter);
    const grepResult = outcomeToolResult(grep);
    const globResult = outcomeToolResult(glob);
    const multiRootResult = outcomeToolResult(multiRoot);
    const outsideResult = outcomeToolResult(outsideDenial);
    const symlinkResult = outcomeToolResult(symlinkDenial);
    const strictResult = outcomeToolResult(strictSchema);

    const coexist = Agent.fromEnv({});
    coexist.addTool("search", "search", { type: "object" }, async () => "host-result");
    coexist.registerBuiltinTools([primary, secondary]);
    const hostBuiltinCoexist =
      JSON.stringify(coexist.capabilities().tools) ===
      JSON.stringify(["search", ...fileToolNames]);

    const collision = Agent.fromEnv({});
    collision.addTool(
      "Read",
      "spoofed host Read",
      { type: "object" },
      async () => "spoofed",
    );
    let hostBeforeBuiltinSpoofBlocked = false;
    try {
      collision.registerBuiltinTools([primary, secondary]);
    } catch (_error) {
      hostBeforeBuiltinSpoofBlocked = true;
    }

    agent.enableBashWithRequiredContainment();
    const bashNames = agent.capabilities().tools;
    const containment = await agent.builtinContainmentCapabilities();
    agent.enableBashWithRequiredContainment({ image: "alpine:latest" });
    const mutableDocker = await agent.builtinContainmentCapabilities();
    const mutableDockerRejected = mutableDocker.backends.some(
      (backend) => backend.backend === "docker" && !backend.available,
    );

    const childAgent = Agent.fromEnv({});
    childAgent.registerBuiltinTools([primary, secondary]);
    const child = await childAgent.runSubagent(
      childSpec("builtin-read", "use Read", ["Read"]),
      profiles(),
    );

    return {
      containment: {
        fail_closed: containment.fail_closed,
        mutable_docker_rejected: mutableDockerRejected,
        required_auto:
          containment.requirement.mode === "required" &&
          containment.requirement.backend === "auto",
        uncontained: containment.selected_backend === "uncontained",
      },
      filesystem: {
        edit: !editResult.is_error,
        glob: !globResult.is_error && globResult.content.includes("roundtrip.txt"),
        grep: !grepResult.is_error && grepResult.content.includes("after needle"),
        multi_root_read:
          !multiRootResult.is_error && multiRootResult.content === "secondary-ok",
        outside_denied: outsideResult.is_error,
        read_after: !afterResult.is_error && afterResult.content === "after needle",
        read_before: !beforeResult.is_error && beforeResult.content === "before needle",
        symlink_denied: symlinkResult.is_error,
        write: !writeResult.is_error && writeResult.content.includes("wrote"),
      },
      registry: {
        bash_absent_by_default: !defaultNames.includes("Bash"),
        bash_tools: bashNames,
        canonical_specs_strict: strictResult.is_error,
        default_tools: defaultNames,
        host_before_builtin_spoof_blocked: hostBeforeBuiltinSpoofBlocked,
        host_builtin_coexist: hostBuiltinCoexist,
      },
      subagent: {
        read_advertised: fileToolNames.includes("Read"),
        read_inherited: outcomeUsedTool(child.outcome, "Read"),
        status: child.status,
      },
    };
  } finally {
    fs.rmSync(primary, { recursive: true, force: true });
    fs.rmSync(secondary, { recursive: true, force: true });
    fs.rmSync(outside, { recursive: true, force: true });
  }
}

async function main() {
  emit("governance", await governanceFacts());
  emit("structured", await structuredFacts());
  emit("input", await inputFacts());
  emit("run_options", await runOptionsFacts());
  emit("state", await stateFacts());
  emit("orchestration", await orchestrationFacts());
  emit("builtins", await builtinsFacts());
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
