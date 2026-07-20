"use strict";

// Keyless runtime contract for canonical messages, media, routing, Zod v4, and typed failures.
// Zod is deliberately required at module load: CI must install the pinned v4 dependency and this
// test must fail, never skip, if that real runtime path is unavailable.
const { z } = require("zod");
const { Agent, query } = require("..");

const MESSAGES = [
  {
    role: "system",
    content: [{ type: "text", text: "Inspect every supplied input block." }],
  },
  {
    role: "user",
    content: [
      { type: "text", text: "multimodal" },
      {
        type: "media",
        media_type: "image/png",
        source: { kind: "url", url: "https://example.com/chart.png" },
      },
      {
        type: "media",
        media_type: "image/jpeg",
        source: { kind: "base64", data: "aGVsbG8=" },
      },
      {
        type: "media_input",
        media: {
          media_type: "application/octet-stream",
          source: { kind: "bytes", data: [97, 98, 99] },
          sha256: "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
          size_bytes: 3,
        },
      },
    ],
  },
];

const OBJECT_SCHEMA = z.object({ status: z.literal("ok") }).strict();

async function drain(stream) {
  for await (const _delta of stream) {
    // Drain through the public async-iterator contract.
  }
  return stream.outcome();
}

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

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

function assertInputPreserved(outcome) {
  assert(
    JSON.stringify(canonical(outcome.messages.slice(0, MESSAGES.length))) ===
      JSON.stringify(canonical(MESSAGES)),
    "canonical multimodal input was changed or flattened",
  );
}

async function main() {
  const agent = Agent.fromEnv({});

  const generated = await agent.generateText(MESSAGES);
  assertInputPreserved({ messages: generated.messages });
  assertInputPreserved(await drain(agent.streamText(MESSAGES)));
  assertInputPreserved(await drain(agent.run(MESSAGES)));
  assertInputPreserved(await drain(agent.client().query(MESSAGES)));
  assertInputPreserved(await drain(query(MESSAGES)));

  const compatibility = await agent.generateText("string compatibility");
  assert(
    JSON.stringify(canonical(compatibility.messages[0])) ===
      JSON.stringify(canonical({
        role: "user",
        content: [{ type: "text", text: "string compatibility" }],
      })),
    "string compatibility input changed",
  );

  const structured = await agent.generateObject(MESSAGES, OBJECT_SCHEMA);
  assert(structured.value.status === "ok", "Zod generateObject did not materialize");
  let completed;
  for await (const event of agent.streamObject(MESSAGES, OBJECT_SCHEMA)) {
    if (event.type === "completed") completed = event.object;
  }
  assert(completed?.value?.status === "ok", "Zod streamObject did not complete");

  const routed = await drain(
    agent.run(MESSAGES, {
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
            capabilities: ["vision"],
          },
        ],
        request: {
          policy: { kind: "automatic", objective: "quality" },
          active_providers: [],
          estimated_input_tokens: 8,
          required_output_tokens: 64,
          max_cost_usd: null,
          required_skills: [],
          required_capabilities: ["vision"],
        },
      },
    }),
  );
  assert(
    JSON.stringify(routed.model_attempts) === JSON.stringify(["mock-routed"]),
    `normal Agent.run did not honor routing: ${JSON.stringify(routed.model_attempts)}`,
  );

  for (const invalid of [[], [{ role: "user", content: [{ type: "media" }] }]]) {
    let rejected = false;
    try {
      agent.run(invalid);
    } catch (error) {
      rejected = error.code === "configuration";
    }
    assert(rejected, "malformed canonical input unexpectedly reached the provider");
  }

  let textError;
  try {
    await agent.generateText(MESSAGES, { model: "not-a-real-model" });
  } catch (error) {
    textError = error;
  }
  assert(textError?.code === textError?.info?.code, "generateText error was not typed");
  assert(Boolean(textError?.info?.message), "generateText error omitted ErrorInfo.message");

  let objectError;
  try {
    for await (const _event of agent.streamObject(
      MESSAGES,
      {
        type: "object",
        required: ["value"],
        properties: { value: { type: "string", minLength: 8 } },
      },
      { maxRetries: 0 },
    )) {
      // The terminal validation failure must reject the iterator with a typed error.
    }
  } catch (error) {
    objectError = error;
  }
  assert(objectError?.code === "structured_output", "object stream error code drifted");
  assert(
    objectError?.info?.code === "structured_output",
    "object stream error omitted ErrorInfo",
  );

  const failureContexts = [];
  const failing = Agent.fromEnv({});
  failing.addTool(
    "explode",
    "always fails",
    { type: "object", properties: { q: { type: "string" } } },
    async () => {
      throw new Error("expected host failure");
    },
  );
  failing.onPostToolFailure(async (context) => {
    failureContexts.push(context);
  }, "explode");
  await drain(failing.run("invoke the failing tool"));
  assert(failureContexts.length === 1, "PostToolFailure did not run exactly once");
  assert(failureContexts[0].stage === "tool_execution", "failure stage drifted");
  assert(failureContexts[0].tool === "explode", "failure tool matcher drifted");

  const result = {
    media_sources: ["url", "base64", "strict-bytes"],
    object_error: objectError.code,
    post_tool_failure: failureContexts[0].stage,
    routed_model: routed.model_attempts[0],
    structured: structured.value.status,
    text_surfaces: 5,
    zod_version: 4,
  };
  console.log(`MULTIMODAL_MESSAGES_JSON=${JSON.stringify(result)}`);
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
