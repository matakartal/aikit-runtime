"use strict";

const assert = require("node:assert/strict");
const { Agent, DurableRun, tool } = require("..");

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
  const nowUnixMs = Date.now();
  const durable = new DurableRun("safe-number-session", "safe-number-run");
  const approvalId = durable.requestTypedApproval({
    logical_key: "safe-number-approval",
    kind: "confirmation",
    prompt: "Proceed?",
    payload: { ratio: 1.5, nested: { sequence: 1.5 } },
    requested_at_unix_ms: nowUnixMs,
    expires_at_unix_ms: nowUnixMs + 10_000,
  });
  const pendingJson = JSON.stringify(durable.snapshot());
  const pendingRestored = DurableRun.fromState(JSON.parse(pendingJson));
  assert.equal(
    pendingRestored.snapshot().projection.approvals[approvalId].resolved_sequence,
    null,
  );
  assert.deepEqual(
    pendingRestored.snapshot().projection.approvals[approvalId].payload,
    { ratio: 1.5, nested: { sequence: 1.5 } },
  );
  pendingRestored.resolveApprovalAt(
    "safe-number-resume",
    approvalId,
    true,
    BigInt(nowUnixMs + 1),
    { ratio: 2.5, nested: { sequence: 2.5 } },
  );
  const resolvedJson = JSON.stringify(pendingRestored.snapshot());
  const resolvedRestored = DurableRun.fromState(JSON.parse(resolvedJson));
  assert.deepEqual(
    resolvedRestored.snapshot().projection.approvals[approvalId].response,
    { ratio: 2.5, nested: { sequence: 2.5 } },
  );

  const highSafe = new DurableRun("high-safe-session", "high-safe-run");
  highSafe.requestTypedApproval({
    logical_key: "high-safe-approval",
    kind: "confirmation",
    prompt: "Proceed?",
    payload: null,
    requested_at_unix_ms: 2 ** 32,
    expires_at_unix_ms: 2 ** 32 + 1,
  });
  DurableRun.fromState(JSON.parse(JSON.stringify(highSafe.snapshot())));
  for (const invalidTimestamp of [1.5, Number.MAX_SAFE_INTEGER + 1]) {
    const invalid = new DurableRun(
      `invalid-time-session-${invalidTimestamp}`,
      `invalid-time-run-${invalidTimestamp}`,
    );
    assert.throws(
      () => invalid.requestTypedApproval({
        logical_key: "invalid-time-approval",
        kind: "confirmation",
        prompt: "Proceed?",
        payload: null,
        requested_at_unix_ms: invalidTimestamp,
        expires_at_unix_ms: invalidTimestamp,
      }),
      /safe integer number/,
    );
  }
  const unsafeClock = BigInt(Number.MAX_SAFE_INTEGER) + 1n;
  const beforeUnsafeClocks = JSON.stringify(highSafe.snapshot());
  assert.throws(
    () => highSafe.expireApprovals("unsafe-expire", unsafeClock),
    /Number\.MAX_SAFE_INTEGER/,
  );
  assert.throws(
    () => highSafe.resolveApprovalAt(
      "unsafe-resolve",
      "missing-approval",
      true,
      unsafeClock,
    ),
    /Number\.MAX_SAFE_INTEGER/,
  );
  assert.throws(
    () => highSafe.applyCommandAt(
      { command: "cancel", command_id: "unsafe-cancel" },
      unsafeClock,
    ),
    /Number\.MAX_SAFE_INTEGER/,
  );
  assert.equal(JSON.stringify(highSafe.snapshot()), beforeUnsafeClocks);

  function trackedAbortSignal() {
    const listeners = new Set();
    return {
      listeners,
      signal: {
        aborted: false,
        addEventListener(type, listener) {
          assert.equal(type, "abort");
          listeners.add(listener);
        },
        removeEventListener(type, listener) {
          assert.equal(type, "abort");
          listeners.delete(listener);
        },
      },
    };
  }

  const exhausted = trackedAbortSignal();
  const eventParent = Agent.fromEnv({}).run("event listener cleanup", {
    signal: exhausted.signal,
  });
  assert.equal(exhausted.listeners.size, 1);
  for await (const _event of eventParent.events("event-listener-cleanup")) {
    // Early return from the event view must also remove the dormant parent listener.
    break;
  }
  assert.equal(exhausted.listeners.size, 0, "event view leaked its parent listener");

  const parentClosed = trackedAbortSignal();
  const parentStream = Agent.fromEnv({}).run("parent closes first", {
    signal: parentClosed.signal,
  });
  parentStream.events("parent-close-child");
  assert.equal(parentClosed.listeners.size, 2);
  await parentStream.close();
  assert.equal(parentClosed.listeners.size, 0, "parent close leaked child listener");

  const childClosed = trackedAbortSignal();
  const childParent = Agent.fromEnv({}).run("child closes first", {
    signal: childClosed.signal,
  });
  const childStream = childParent.events("child-close");
  assert.equal(childClosed.listeners.size, 2);
  await childStream.close();
  assert.equal(childClosed.listeners.size, 0, "child close leaked parent listener");

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

  // Conflicting aliases must fail closed. Previously `action` silently won over `decision`, so
  // this malformed host response authorized the tool despite carrying an explicit denial.
  const malformed = Agent.fromEnv({});
  let malformedCalls = 0;
  malformed.addTool(
    "search",
    "must remain denied",
    { type: "object" },
    async () => {
      malformedCalls += 1;
      return "must not run";
    },
  );
  malformed.canUseTool(async () => ({ action: "allow", decision: "deny" }));
  malformed.setPermissions([{ effect: "ask", tool: "search" }]);
  const malformedResult = await malformed.runSubagent(
    spec("malformed-approval"),
    profiles,
  );
  assert.equal(malformedResult.status, "succeeded");
  assert.equal(malformedCalls, 0, "ambiguous approval reached the host callback");

  // Hook objects use only the documented `action` discriminator. A contradictory alias must
  // fail closed instead of letting `continue` silently override `block`.
  const malformedHook = Agent.fromEnv({});
  let malformedHookCalls = 0;
  malformedHook.addTool(
    "search",
    "must remain blocked",
    { type: "object" },
    async () => {
      malformedHookCalls += 1;
      return "must not run";
    },
  );
  malformedHook.onPreToolUse(
    async () => ({ action: "continue", decision: "block" }),
    "search",
  );
  const malformedHookResult = await malformedHook.runSubagent(
    spec("malformed-hook"),
    profiles,
  );
  assert.equal(malformedHookResult.status, "succeeded");
  assert.equal(
    malformedHookCalls,
    0,
    "ambiguous hook reached the host callback",
  );

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
