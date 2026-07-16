"use strict";

// The agent-native + governance surface from Node (keyless) — a byte-for-byte behavioural mirror
// of examples/python/agent_governance.py, driven by the SAME Rust core. This is the cross-language
// parity proof: a permission policy and the "key gir → güçlen" primitive behave identically in
// Python and in Node because both are thin shells over aikit-core.
//
// Build first:  ./scripts/build-node.sh   (then)  node examples/node/agent_governance.cjs

const path = require("node:path");
const { Agent, query } = require(
  path.join(__dirname, "..", "..", "crates", "aikit-node", "index.js"),
);

async function main() {
  // 1. Agent-native: key gir -> güçlen. Providers are activated by key format.
  // new Agent() discovers real provider keys from process env. This deterministic parity demo
  // uses an explicit empty env so a developer's shell cannot change its transcript.
  const agent = Agent.fromEnv({});
  const fresh = agent.activeProviders();
  console.log("providers (fresh):", fresh);

  // Both complete and streaming Agent methods resolve the requested live provider rather than
  // silently falling back to MockProvider.
  try {
    await agent.generateText("hello", { model: "claude-demo" });
    throw new Error("expected a missing live credential to throw");
  } catch (error) {
    if (!String(error.message).includes("no credential active for provider 'anthropic'")) {
      throw error;
    }
  }
  try {
    agent.streamText("hello", { model: "gpt-5" });
    throw new Error("expected a missing live credential to throw");
  } catch (error) {
    if (!String(error.message).includes("no credential active for provider 'openai'")) {
      throw error;
    }
  }

  agent.addKey("sk-ant-DEMOKEY"); // anthropic (by sk-ant- prefix)
  agent.addKey("AIzaDEMOKEY"); // google    (by AIza prefix)

  const caps = agent.capabilities();
  const afterKeys = agent.activeProviders();
  const capabilities = caps.providers.map((p) => [p.provider, p.structured_output]);
  console.log("providers (after keys):", afterKeys);
  console.log("capabilities:", capabilities);
  if (!agent.hasProvider("anthropic") || !agent.hasProvider("google")) {
    throw new Error("expected anthropic + google to be active");
  }

  // An sk- key that could be OpenAI or DeepSeek is ambiguous → throws without a hint.
  let ambiguousRejected = false;
  try {
    agent.addKey("sk-proj-XXXX");
  } catch (e) {
    ambiguousRejected = true;
    console.log("ambiguous sk- key correctly rejected:", String(e.message).slice(0, 60), "...");
  }
  if (!ambiguousRejected) throw new Error("expected ambiguous key to throw");
  agent.addKey("sk-proj-XXXX", "deepseek"); // disambiguated
  if (!agent.hasProvider("deepseek")) throw new Error("deepseek not activated");

  // 2. Governance from Node: the SAME tool under two policies. The tool is a JS `async` function
  //    that echoes its input, exercising the tool-callback seam (Rust loop → JS async → result
  //    back) and proving the input was marshalled across the FFI boundary correctly.
  let calls = 0;
  const tools = {
    search_db: async (input) => {
      calls++;
      return `rows for ${input.q ?? "?"}`;
    },
  };

  // 2a. deny → the tool must NOT run. MockProvider emits the call; the engine denies it before
  //     the executor is ever reached.
  let sawDenial = false;
  let denialMessage = "";
  for await (const ev of query("veritabanında ara", tools, {
    permissions: [{ effect: "deny", tool: "search_db" }],
  })) {
    if (ev.type === "tool_result" && ev.is_error && String(ev.content || "").includes("denied")) {
      sawDenial = true;
      denialMessage = ev.content;
      console.log("deny  → tool_result:", ev.content);
    }
  }
  if (!sawDenial) throw new Error("expected a denial tool_result to reach the model");
  if (calls !== 0) throw new Error("a denied tool must NEVER run");

  // 2b. allow → the tool RUNS; its return value flows back to the model (the callback seam).
  let toolEcho = "";
  for await (const ev of query("veritabanında ara", tools, {
    permissions: [{ effect: "allow", tool: "search_db" }],
  })) {
    if (ev.type === "tool_result" && !ev.is_error) {
      toolEcho = ev.content;
      console.log("allow → tool_result:", ev.content);
    }
  }
  const toolRan = calls === 1;
  if (!toolRan) throw new Error("an allowed tool must run exactly once");

  // 3. The Agent-native path uses the same governed live-provider loop with registered host
  // tools, Ask approval, and every async lifecycle hook. Mock keeps this proof deterministic;
  // changing only model after addKey uses the same callbacks against a live provider.
  const governed = Agent.fromEnv({});
  const hookEvents = [];
  const toolInputs = [];
  const approvalInputs = [];

  governed.addTool(
    "agent_search",
    "search from the governed Agent",
    { type: "object", properties: { q: { type: "string" } } },
    async (input) => {
      hookEvents.push("tool");
      toolInputs.push(input.q);
      return `rows for ${input.q}`;
    },
  );
  governed.onUserPrompt(async (ctx) => {
    hookEvents.push("prompt");
    return { action: "rewrite", prompt: `${ctx.prompt} [checked]` };
  });
  governed.onPreToolUse(async (_ctx) => {
    hookEvents.push("pre");
    return { action: "rewrite", input: { q: "pre-approved" } };
  }, "agent_search");
  governed.onPostToolUse(async (ctx) => {
    hookEvents.push("post");
    return { action: "rewrite", output: `post:${ctx.output}` };
  }, "agent_search");
  governed.onFailure(async (_ctx) => {
    hookEvents.push("failure");
    return { action: "rewrite", error: "safe failure" };
  });
  governed.onStop(async (_ctx) => {
    hookEvents.push("stop");
  });
  governed.canUseTool(async (ctx) => {
    hookEvents.push("approve");
    approvalInputs.push(ctx.input.q);
    return { decision: "allow", updated_input: { q: "approved" } };
  });
  governed.setPermissions([{ id: "ask-search", effect: "ask", tool: "agent_search" }]);

  const governedResult = await governed.generateText("governed agent request");
  const governedToolResult = governedResult.messages
    .flatMap((message) => message.content)
    .find((block) => block.type === "tool_result" && !block.is_error).content;

  governed.setPermissions([{ id: "deny-search", effect: "deny", tool: "agent_search" }]);
  let governedDenial = "";
  for await (const ev of governed.streamText("denied governed request")) {
    if (ev.type === "tool_result" && ev.is_error) governedDenial = ev.content;
  }
  const governedCallbacks = [
    toolInputs,
    approvalInputs,
    governedToolResult,
    governedDenial,
    hookEvents,
  ];
  const expectedGovernedCallbacks = [
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
  ];
  if (JSON.stringify(governedCallbacks) !== JSON.stringify(expectedGovernedCallbacks)) {
    throw new Error(`governed Agent callback drift: ${JSON.stringify(governedCallbacks)}`);
  }

  // 4. Text generation and streaming use Agent's provider resolver. The mock model proves the
  // same live-capable path without making a network request.
  const generated = await agent.generateText("Say hello");
  if (JSON.stringify(generated.provider_metadata) !== "{}") {
    throw new Error("unexpected mock provider metadata");
  }
  const generatedText = [
    generated.text,
    generated.usage.input_tokens,
    generated.usage.output_tokens,
    generated.stop_reason,
  ];

  let streamed = "";
  let streamedOutputTokens = 0;
  let streamedStop = "";
  for await (const ev of agent.streamText("Say hello")) {
    if (ev.type === "text_delta") streamed += ev.text;
    else if (ev.type === "usage") streamedOutputTokens += ev.output_tokens;
    else if (ev.type === "message_stop") streamedStop = ev.stop_reason;
  }
  const streamedText = [streamed, streamedOutputTokens, streamedStop];
  if (streamed !== generated.text) throw new Error("generateText/streamText drifted");

  // 5. Memory is explicit: remember writes, recall searches. Timestamps stay out of parity facts
  // because only semantic content belongs in a cross-process transcript.
  agent.remember("customer_note", "Ada prefers EUR");
  const recalled = agent.recall("EUR", 3);
  const memoryRecall = recalled.map((entry) => [entry.key, entry.value]);
  if (JSON.stringify(memoryRecall) !== JSON.stringify([["customer_note", "Ada prefers EUR"]])) {
    throw new Error("memory recall mismatch");
  }

  // 6. Routing accepts typed core model profiles and a typed route request. Fake demo keys only
  // activate capabilities; routing makes no network request and never receives their values.
  const route = agent.route(
    [
      {
        provider: "anthropic",
        model: "claude-demo",
        context_window_tokens: 100000,
        max_output_tokens: 4096,
        pricing: null,
        quality_score: 80,
        skills: ["general"],
        capabilities: ["tool_use"],
      },
      {
        provider: "google",
        model: "gemini-demo",
        context_window_tokens: 100000,
        max_output_tokens: 4096,
        pricing: null,
        quality_score: 90,
        skills: ["general"],
        capabilities: ["tool_use"],
      },
    ],
    {
      policy: { kind: "automatic", objective: "quality" },
      active_providers: [],
      estimated_input_tokens: 100,
      required_output_tokens: 64,
      max_cost_usd: null,
      required_skills: ["general"],
      required_capabilities: ["tool_use"],
    },
  );
  const routeDecision = [route.profile.provider, route.profile.model, route.eligible_models];
  if (JSON.stringify(routeDecision) !== JSON.stringify(["google", "gemini-demo", 2])) {
    throw new Error("route decision mismatch");
  }

  // 7. Governed orchestration is keyless with a mock catalog. Every child remains bounded by its
  // own limits plus one shared ledger, and the initial binding grants no host tools.
  const mockProfiles = [
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
  const orchestrationOptions = {
    maxParallelism: 2,
    budget: {
      max_model_calls: 8,
      max_input_tokens: 8192,
      max_output_tokens: 8192,
      max_cost_micro_usd: null,
      wall_time_ms: 5000,
    },
  };
  const childSpec = (id, prompt) => ({
    id,
    prompt,
    system: null,
    route: {
      policy: { kind: "explicit", model: "mock-1" },
      max_cost_usd: null,
      required_skills: [],
      required_capabilities: [],
    },
    allowed_tools: [],
    max_turns: 2,
    max_tokens: 64,
    estimated_input_tokens: 8,
  });
  const child = await agent.runSubagent(
    childSpec("worker", "Inspect the request"),
    mockProfiles,
    orchestrationOptions,
  );
  if (child.status !== "succeeded") throw new Error("subagent failed");
  const fan = await agent.fanOut(
    [childSpec("fan-a", "A"), childSpec("fan-b", "B")],
    mockProfiles,
    orchestrationOptions,
  );
  if (JSON.stringify(fan.map((result) => result.id)) !== JSON.stringify(["fan-a", "fan-b"])) {
    throw new Error("fan-out order drifted");
  }
  const council = await agent.council(
    [childSpec("member-a", "Analyze A"), childSpec("member-b", "Analyze B")],
    childSpec("synthesis", "Reach a conclusion"),
    mockProfiles,
    2,
    orchestrationOptions,
  );
  if (council.status.kind !== "succeeded") throw new Error("council failed");

  // 8. Typed structured output crosses napi through the same Rust planner and validator used by
  // live providers. `mock-structured` keeps this parity proof deterministic and keyless.
  const invoiceSchema = {
    type: "object",
    required: ["currency", "status"],
    properties: {
      currency: { type: "string", enum: ["EUR"] },
      status: { type: "string", enum: ["ok"] },
    },
  };
  const structured = await agent.generateObject("Return the invoice status", invoiceSchema);
  const structuredOutput = [
    structured.fidelity,
    structured.attempts,
    structured.value.currency,
    structured.value.status,
  ];
  // Zod is an optional peer. Exercise the direct-schema path whenever the demo environment has
  // Zod v4 installed; raw JSON Schema above remains dependency-free.
  let InvoiceSchema;
  try {
    const { z } = require("zod");
    InvoiceSchema = z.object({ currency: z.literal("EUR"), status: z.literal("ok") });
    const typedStructured = await agent.generateObject(
      "Return the invoice status as a typed model",
      InvoiceSchema,
    );
    if (typedStructured.value.currency !== "EUR" || typedStructured.value.status !== "ok") {
      throw new Error("Zod structured output was not parsed");
    }
  } catch (error) {
    if (error.code !== "MODULE_NOT_FOUND") throw error;
  }

  // 9. A real ObjectStream exposes provider deltas before Completed. When Zod v4 is installed,
  // only the final value is parsed; intermediate events remain visible.
  const objectEvents = [];
  let objectCompleted;
  for await (const event of agent.streamObject("Stream the invoice status", invoiceSchema, {
    providerOptions: { mock: { temperature: 0 } },
  })) {
    objectEvents.push(event.type);
    if (event.type === "completed") objectCompleted = event.object;
  }
  if (objectEvents.indexOf("delta") >= objectEvents.indexOf("completed")) {
    throw new Error("ObjectStream did not expose deltas before completion");
  }
  if (JSON.stringify(objectCompleted.provider_metadata) !== "{}") {
    throw new Error("unexpected mock structured provider metadata");
  }

  let typedStreamValue = objectCompleted.value;
  if (InvoiceSchema != null) {
    const typedEvents = [];
    for await (const event of agent.streamObject("Stream a typed invoice", InvoiceSchema)) {
      typedEvents.push(event.type);
      if (event.type === "completed") typedStreamValue = event.object.value;
    }
    if (!typedEvents.includes("delta") || typedEvents.at(-1) !== "completed") {
      throw new Error("typed ObjectStream hid intermediate events");
    }
  }

  const repairSequence = [];
  try {
    for await (const event of agent.streamObject(
      "Exercise repair events",
      {
        type: "object",
        required: ["value"],
        properties: { value: { type: "string", minLength: 8 } },
      },
      { maxRetries: 1 },
    )) {
      if (event.type === "attempt_started") {
        repairSequence.push(["attempt_started", event.repair]);
      } else if (event.type === "validation_failed") {
        repairSequence.push(["validation_failed", event.will_retry]);
      }
    }
  } catch (error) {
    // Expected: both repair attempts deterministically fail the minLength constraint. The native
    // binding must retain the core's stable machine-readable classification on terminal errors.
    if (error.code !== "structured_output" || error.info?.code !== error.code) {
      throw new Error("typed ObjectStream terminal error envelope drift");
    }
  }
  const expectedRepair = [
    ["attempt_started", false],
    ["validation_failed", true],
    ["attempt_started", true],
    ["validation_failed", false],
  ];

  try {
    await agent.generateObject(
      "Return an impossible value",
      {
        type: "object",
        required: ["value"],
        properties: { value: { type: "string", minLength: 1000 } },
      },
      { maxRetries: 0 },
    );
    throw new Error("impossible generateObject unexpectedly succeeded");
  } catch (error) {
    if (error.code !== "structured_output" || error.info?.code !== error.code) {
      throw new Error("typed generateObject terminal error envelope drift");
    }
  }
  if (JSON.stringify(repairSequence) !== JSON.stringify(expectedRepair)) {
    throw new Error(`repair event drift: ${JSON.stringify(repairSequence)}`);
  }

  const interrupted = Agent.fromEnv({});
  let interruptApprovals = 0;
  let interruptToolCalls = 0;
  const interruptStops = [];
  interrupted.addTool("interrupt_me", "interrupt demo", { type: "object" }, async () => {
    interruptToolCalls++;
    return "must not run";
  });
  interrupted.setPermissions([{ effect: "ask", tool: "interrupt_me" }]);
  interrupted.canUseTool(async () => {
    interruptApprovals++;
    return { decision: "deny", message: "operator stopped", interrupt: true };
  });
  interrupted.onStop(async (context) => {
    interruptStops.push(context.reason);
  });
  const interruptEvents = [];
  for await (const event of interrupted.streamText("stop before tool execution")) {
    interruptEvents.push(event);
  }
  const interruptFact = {
    approval_calls: interruptApprovals,
    errors: interruptEvents.filter((event) => event.type === "error").length,
    message_starts: interruptEvents.filter((event) => event.type === "message_start").length,
    stop_reasons: interruptStops,
    tool_calls: interruptToolCalls,
    tool_results: interruptEvents.filter((event) => event.type === "tool_result").length,
  };
  const expectedInterrupt = {
    approval_calls: 1,
    errors: 1,
    message_starts: 1,
    stop_reasons: ["approval_interrupted"],
    tool_calls: 0,
    tool_results: 0,
  };
  if (JSON.stringify(interruptFact) !== JSON.stringify(expectedInterrupt)) {
    throw new Error(`interrupt semantics drift: ${JSON.stringify(interruptFact)}`);
  }
  const bindingStreamFacts = {
    interrupt: interruptFact,
    repair_sequence: repairSequence,
    structured_delta_before_completed:
      objectEvents.indexOf("delta") < objectEvents.indexOf("completed"),
    structured_types: objectEvents,
    structured_value: [objectCompleted.value.currency, objectCompleted.value.status],
    typed_value: [typedStreamValue.currency, typedStreamValue.status],
  };
  console.log("structured output:", structured);

  console.log("\nSPIKE OK ✅  — napi governed, agent-native surface works from Node:");
  console.log("  1) Agent: key gir -> güçlen (capabilities grow, keys never leak)");
  console.log("  2) permissions=[deny(...)] denies a tool; [allow(...)] runs it (callback seam)");
  console.log("  3) SAME Rust core as Python — identical behaviour across languages");

  // Canonical facts for the cross-language parity check (scripts/parity-check.sh). Keys are in
  // alphabetical order and the output is compact — byte-identical to the Python demo's line.
  const facts = {
    ambiguous_rejected: ambiguousRejected,
    capabilities,
    denial_message: denialMessage,
    denial_seen: sawDenial,
    generated_text: generatedText,
    memory_recall: memoryRecall,
    providers_after_keys: afterKeys,
    providers_fresh: fresh,
    route_decision: routeDecision,
    streamed_text: streamedText,
    structured_output: structuredOutput,
    tool_echo: toolEcho,
    tool_ran: toolRan,
  };
  console.log("GOVERNANCE_JSON=" + JSON.stringify(governedCallbacks));
  console.log("BINDING_STREAM_JSON=" + JSON.stringify(bindingStreamFacts));
  console.log(
    "PARITY_JSON=" +
      JSON.stringify(facts, [
        "ambiguous_rejected",
        "capabilities",
        "denial_message",
        "denial_seen",
        "generated_text",
        "memory_recall",
        "providers_after_keys",
        "providers_fresh",
        "route_decision",
        "streamed_text",
        "structured_output",
        "tool_echo",
        "tool_ran",
      ]),
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
