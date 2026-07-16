"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { Agent } = require("..");

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
const fileToolNames = ["Read", "Write", "Edit", "Grep", "Glob"];

function spec(id, tool) {
  return {
    id,
    prompt: `use ${tool}`,
    system: null,
    route: {
      policy: { kind: "explicit", model: "mock-1" },
      max_cost_usd: null,
      required_skills: [],
      required_capabilities: [],
    },
    allowed_tools: [tool],
    max_turns: 3,
    max_tokens: 64,
    estimated_input_tokens: 8,
  };
}

function usedTool(result, name) {
  return result.outcome.messages.some((message) =>
    message.content.some((block) => block.type === "tool_use" && block.name === name),
  );
}

async function main() {
  const primary = fs.mkdtempSync(path.join(os.tmpdir(), "aikit-node-primary-"));
  const secondary = fs.mkdtempSync(path.join(os.tmpdir(), "aikit-node-secondary-"));
  try {
    const agent = Agent.fromEnv({});
    let hostCalls = 0;
    agent.addTool(
      "search",
      "host search",
      {
        type: "object",
        required: ["q"],
        properties: { q: { type: "string" } },
        additionalProperties: false,
      },
      async () => {
        hostCalls += 1;
        return "host-result";
      },
    );
    agent.registerBuiltinTools([primary, secondary]);
    assert.deepEqual(agent.capabilities().tools, ["search", ...fileToolNames]);
    assert.equal(agent.capabilities().tools.includes("Bash"), false);

    const host = await agent.runSubagent(spec("composite-host", "search"), profiles);
    assert.equal(host.status, "succeeded");
    assert.equal(hostCalls, 1);

    const child = await agent.runSubagent(spec("builtin-child", "Read"), profiles);
    assert.equal(usedTool(child, "Read"), true);
    const fan = await agent.fanOut(
      [spec("builtin-fan-a", "Read"), spec("builtin-fan-b", "Read")],
      profiles,
      { maxParallelism: 2 },
    );
    assert.equal(fan.every((result) => usedTool(result, "Read")), true);
    const council = await agent.council(
      [spec("builtin-member-a", "Read"), spec("builtin-member-b", "Read")],
      spec("builtin-synthesis", "Read"),
      profiles,
      2,
      { maxParallelism: 2 },
    );
    assert.equal(council.members.every((result) => usedTool(result, "Read")), true);
    assert.equal(usedTool(council.synthesis, "Read"), true);

    agent.enableBashWithRequiredContainment();
    assert.deepEqual(agent.capabilities().tools, ["search", ...fileToolNames, "Bash"]);
    const containment = await agent.builtinContainmentCapabilities();
    assert.deepEqual(containment.requirement, { mode: "required", backend: "auto" });
    assert.equal(containment.fail_closed, true);
    assert.notEqual(containment.selected_backend, "uncontained");

    agent.enableBashWithRequiredContainment({ image: "alpine:latest" });
    const invalidDocker = await agent.builtinContainmentCapabilities();
    const docker = invalidDocker.backends.find((backend) => backend.backend === "docker");
    assert.equal(docker.available, false);
    assert.match(docker.detail, /must be pinned/);

    const collision = Agent.fromEnv({});
    collision.addTool("Read", "collision", { type: "object" }, async () => "never");
    assert.throws(
      () => collision.registerBuiltinTools([primary]),
      /collides with a registered host tool/,
    );

    const reverseCollision = Agent.fromEnv({});
    reverseCollision.registerBuiltinTools([primary, secondary]);
    assert.throws(
      () =>
        reverseCollision.addTool(
          "Read",
          "collision",
          { type: "object" },
          async () => "never",
        ),
      /already registered/,
    );

    const bashCollision = Agent.fromEnv({});
    bashCollision.addTool("Bash", "host bash", { type: "object" }, async () => "never");
    bashCollision.registerBuiltinTools([primary]);
    assert.throws(
      () => bashCollision.enableBashWithRequiredContainment(),
      /collides with a registered host tool/,
    );
  } finally {
    fs.rmSync(primary, { recursive: true, force: true });
    fs.rmSync(secondary, { recursive: true, force: true });
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
