"use strict";

const assert = require("node:assert/strict");
const { Agent, tool } = require("..");

const profiles = [
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

function spec(id) {
  return {
    id,
    prompt: `run ${id}`,
    system: null,
    route: {
      policy: { kind: "explicit", model: "mock-1" },
      max_cost_usd: null,
      required_skills: [],
      required_capabilities: [],
    },
    allowed_tools: ["search"],
    max_turns: 3,
    max_tokens: 64,
    estimated_input_tokens: 8,
  };
}

async function main() {
  const agent = Agent.fromEnv({});
  let calls = 0;
  agent.addToolDefinition(
    tool(
      "search",
      "search the host index",
      {
        type: "object",
        required: ["q"],
        properties: { q: { type: "string" } },
        additionalProperties: false,
      },
      async (input) => {
        calls += 1;
        return `found:${input.q}`;
      },
    ),
  );

  const created = await agent.runSubagent(spec("binding-session"), profiles);
  assert.equal(created.status, "succeeded");
  assert.equal(created.session_revision, 1);
  assert.equal(calls, 1);

  const resumed = await agent.resumeSubagent(
    "binding-session",
    spec("binding-session-resumed"),
    profiles,
  );
  assert.equal(resumed.status, "succeeded");
  assert.equal(resumed.session_revision, 2);

  const fan = await agent.fanOut(
    [spec("fan-a"), spec("fan-b")],
    profiles,
    { maxParallelism: 2 },
  );
  assert.deepEqual(
    fan.map((result) => result.status),
    ["succeeded", "succeeded"],
  );
  assert.equal(calls, 3);

  const council = await agent.council(
    [spec("member-a"), spec("member-b")],
    spec("synthesis"),
    profiles,
    2,
    { maxParallelism: 2 },
  );
  assert.deepEqual(council.status, { kind: "succeeded" });
  assert.equal(calls, 6);

  let approvals = 0;
  agent.canUseTool(async () => {
    approvals += 1;
    return { decision: "allow", updated_permissions: ["allow_exact_input"] };
  });
  agent.setPermissions([{ effect: "ask", tool: "search" }]);
  const approved = await agent.runSubagent(spec("approved-session"), profiles);
  assert.equal(approved.status, "succeeded");
  assert.equal(calls, 7);
  assert.equal(approvals, 1);

  // A later deny is authoritative even when an earlier rule allows the same host tool.
  agent.setPermissions([
    { effect: "allow", tool: "search" },
    { effect: "deny", tool: "search" },
  ]);
  const denied = await agent.runSubagent(spec("denied-session"), profiles);
  assert.equal(denied.status, "succeeded");
  assert.equal(calls, 7, "denied subagent reached the host callback");
  assert.equal(approvals, 1, "static deny unexpectedly reached the approver");

  const failing = Agent.fromEnv({});
  const failureOrder = [];
  failing.addTool(
    "search",
    "failing search",
    {
      type: "object",
      required: ["q"],
      properties: { q: { type: "string" } },
      additionalProperties: false,
    },
    async () => {
      throw new Error("host tool exploded");
    },
  );
  failing.onFailure(async (context) => {
    failureOrder.push(["global", context.stage, context.error]);
  });
  // Registering the global hook first must not change the core ordering contract.
  failing.onPostToolFailure(async (context) => {
    failureOrder.push(["post", context.stage, context.error]);
    return { action: "rewrite", error: "safe tool failure" };
  }, "search");
  failing.onPostToolFailure(async () => {
    throw new Error("non-matching PostToolFailure hook ran");
  }, "other");
  const failed = await failing.runSubagent(spec("post-tool-failure"), profiles);
  assert.equal(failed.status, "succeeded");
  assert.equal(failureOrder.length, 2);
  assert.deepEqual(failureOrder[0].slice(0, 2), ["post", "tool_execution"]);
  assert.match(failureOrder[0][2], /host tool exploded/);
  assert.deepEqual(failureOrder[1], [
    "global",
    "tool_execution",
    "safe tool failure",
  ]);

  const ergonomic = Agent.fromEnv({});
  const ergonomicSpec = ergonomic.subtask(
    "ergonomic-subtask",
    "run ergonomic subtask",
    {
      policy: { kind: "explicit", model: "mock-1" },
      max_cost_usd: null,
      required_skills: [],
      required_capabilities: [],
    },
    { maxTurns: 2, maxTokens: 64, estimatedInputTokens: 8 },
  );
  assert.deepEqual(ergonomicSpec, {
    id: "ergonomic-subtask",
    prompt: "run ergonomic subtask",
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
  const parallel = await ergonomic.parallel([ergonomicSpec], profiles, {
    maxParallelism: 1,
  });
  assert.deepEqual(parallel.map((result) => result.status), ["succeeded"]);
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
