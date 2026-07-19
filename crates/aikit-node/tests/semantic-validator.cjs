"use strict";

// Semantic structured-output validator contract for the Node binding.
const assert = require("node:assert/strict");
const { Agent } = require("..");

const schema = {
  type: "object",
  required: ["currency", "status"],
  properties: {
    currency: { type: "string", enum: ["EUR"] },
    status: { type: "string", enum: ["ok"] },
  },
  additionalProperties: false,
};

async function main() {
  const agent = Agent.fromEnv({});
  let retryCalls = 0;
  const generated = await agent.generateObject("invoice", schema, {
    maxRetries: 1,
    validator: async (value) => {
      retryCalls += 1;
      assert.deepEqual(value, { currency: "EUR", status: "ok" });
      return retryCalls === 1
        ? { action: "retry", reason: "semantic policy needs repair" }
        : "accept";
    },
  });
  assert.equal(generated.attempts, 2);
  assert.equal(retryCalls, 2);

  let streamCalls = 0;
  const events = [];
  for await (const event of agent.streamObject("invoice", schema, {
    maxRetries: 1,
    validator: async () => {
      streamCalls += 1;
      return streamCalls === 1
        ? { action: "retry", reason: "stream repair" }
        : { action: "accept" };
    },
  })) {
    events.push(event);
  }
  assert(events.some((event) =>
    event.type === "validation_failed" &&
    event.will_retry === true &&
    event.error.includes("stream repair")
  ));
  assert.equal(events.at(-1).type, "completed");
  assert.equal(events.at(-1).object.attempts, 2);

  await assert.rejects(
    agent.generateObject("invoice", schema, {
      validator: async () => ({
        action: "reject",
        reason: "business policy denied it",
      }),
    }),
    (error) =>
      error.code === "structured_output" &&
      error.message.includes("business policy denied it"),
  );

  await assert.rejects(
    agent.generateObject("invoice", schema, {
      validator: async () => {
        throw new Error("validator exploded");
      },
    }),
    (error) =>
      error.code === "structured_output" &&
      error.message.includes("failed closed"),
  );

  await assert.rejects(
    agent.generateObject("invoice", schema, {
      validator: async () => null,
    }),
    (error) =>
      error.code === "structured_output" &&
      error.message.includes("failed closed"),
  );

  const adversarialDecisions = [
    { decision: "accept" },
    { action: "accept", reason: "accept must not carry a reason" },
    { action: "accept", unexpected: true },
    { action: "retry" },
    { action: "retry", reason: 7 },
    { action: "retry", reason: "repair", unexpected: true },
    { action: "retry", decision: "reject", reason: "conflict" },
    { action: "reject" },
    { action: "reject", reason: "deny", unexpected: true },
  ];
  for (const decision of adversarialDecisions) {
    await assert.rejects(
      agent.generateObject("invoice", schema, {
        validator: async () => decision,
      }),
      (error) =>
        error.code === "structured_output" &&
        error.message.includes("failed closed"),
    );
  }

  const baseline = await agent.generateObject("invoice", schema);
  assert.equal(baseline.attempts, 1);
  console.log("node semantic validator: ok");
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
