"use strict";

const assert = require("node:assert/strict");
const { A2aMapper } = require("..");

const EXPECTED_A2A_MAPPER_SCHEMA_VERSION = 4;

const mapper = new A2aMapper();
const tenantA = {
  subject: "agent-1",
  tenant_id: "tenant-a",
  scopes: ["a2a:message:send", "a2a:tasks:read", "a2a:tasks:cancel"],
};
const tenantB = { ...tenantA, tenant_id: "tenant-b" };
let requestSequence = 0;

function correlation(prefix) {
  requestSequence += 1;
  return {
    correlation_id: `${prefix}-correlation-${requestSequence}`,
    request_id: `${prefix}-request-${requestSequence}`,
  };
}

function targetedMessage(messageId, mapping) {
  return {
    message_id: messageId,
    context_id: mapping.context_id,
    task_id: mapping.task_id,
    role: "ROLE_USER",
    parts: [{ kind: "text", text: messageId }],
  };
}

function serializedState(targetMapper) {
  return Buffer.from(JSON.stringify(targetMapper.snapshot()), "utf8");
}

function dispatchForTask(targetMapper, taskId) {
  const matches = Object.entries(targetMapper.snapshot().dispatch_outbox).filter(
    ([, record]) => record.task_id === taskId && record.state !== "settled",
  );
  assert.equal(matches.length, 1, `expected one open dispatch for ${taskId}`);
  return matches[0];
}

function send(messageId, principal) {
  const result = mapper.sendMessage(
    {
      message_id: messageId,
      context_id: "shared-context",
      role: "ROLE_USER",
      parts: [{ kind: "text", text: messageId }],
    },
    correlation(messageId),
    principal,
  );
  assert.equal(result.envelope.authorization.status, "allowed");
  assert.equal(result.action.kind, "dispatch_message");
  return result.action.mapping.task_id;
}

const tenantAFirst = send("tenant-a-message-1", tenantA);
const tenantBOnly = send("tenant-b-message-1", tenantB);
const tenantASecond = send("tenant-a-message-2", tenantA);

const firstPage = mapper.listTasks(
  { tenant: "tenant-a", pageSize: 1 },
  correlation("tenant-a-page-1"),
  tenantA,
);
assert.equal(firstPage.envelope.authorization.status, "allowed");
assert.equal(firstPage.action.kind, "list_tasks");
assert.equal(firstPage.action.page.totalSize, 2);
assert.equal(firstPage.action.page.tasks.length, 1);
assert.equal(firstPage.action.page.tasks[0].mapping.task_id, tenantASecond);
assert.notEqual(firstPage.action.page.nextPageToken, "");
assert(!firstPage.action.page.tasks.some((task) => task.mapping.task_id === tenantBOnly));

const tenantBPage = mapper.listTasks(
  { tenant: "tenant-b" },
  correlation("tenant-b-page"),
  tenantB,
);
assert.equal(tenantBPage.action.page.totalSize, 1);
assert.equal(tenantBPage.action.page.tasks[0].mapping.task_id, tenantBOnly);

const snapshot = mapper.snapshot();
assert.equal(snapshot.schema_version, EXPECTED_A2A_MAPPER_SCHEMA_VERSION);
const restored = A2aMapper.fromState(snapshot);
assert.deepEqual(restored.snapshot(), snapshot);

for (const safeInteger of [2 ** 32, Number.MAX_SAFE_INTEGER]) {
  // Use an empty current-schema snapshot so the numeric boundary check does not invalidate the
  // immutable hashes and revision bindings of durable dispatch/event records.
  const highRevisionState = {
    ...new A2aMapper().snapshot(),
    revision: safeInteger,
  };
  assert.throws(
    () => A2aMapper.fromState(highRevisionState),
    /revision is not represented by durable mapper state/,
  );

  const highSequenceState = {
    ...new A2aMapper().snapshot(),
    next_sequence: safeInteger,
  };
  assert.deepEqual(A2aMapper.fromState(highSequenceState).snapshot(), highSequenceState);
}

for (const invalidNumber of [-1, 1.5, Number.MAX_SAFE_INTEGER + 1]) {
  assert.throws(
    () => A2aMapper.fromState({
      ...new A2aMapper().snapshot(),
      next_sequence: invalidNumber,
    }),
    /next_sequence must be a non-negative safe integer number/,
  );
}
const invalidNestedRevisionState = structuredClone(snapshot);
Object.values(invalidNestedRevisionState.tasks)[0].created_revision = 1.5;
assert.throws(
  () => A2aMapper.fromState(invalidNestedRevisionState),
  /created_revision must be a non-negative safe integer number/,
);

const secondPage = restored.listTasks(
  {
    tenant: "tenant-a",
    pageSize: 1,
    pageToken: firstPage.action.page.nextPageToken,
  },
  correlation("tenant-a-page-2"),
  tenantA,
);
assert.equal(secondPage.action.page.totalSize, 2);
assert.equal(secondPage.action.page.tasks.length, 1);
assert.equal(secondPage.action.page.tasks[0].mapping.task_id, tenantAFirst);
assert.equal(secondPage.action.page.nextPageToken, "");

const [dispatchId, queuedDispatch] = dispatchForTask(restored, tenantAFirst);
assert.equal(queuedDispatch.attempts, 0);
const claimedState = restored.markDispatchRunning(dispatchId);
const firstAttempt = claimedState.dispatch_outbox[dispatchId].attempts;
assert.equal(firstAttempt, 1);
for (const invalidAttempt of [
  Number.NaN,
  Number.POSITIVE_INFINITY,
  Number.NEGATIVE_INFINITY,
  -1,
  0,
  1.5,
  9,
  2 ** 32 + 1,
]) {
  const beforeInvalidAttempt = restored.snapshot();
  assert.throws(
    () => restored.transitionDispatchTask(
      dispatchId,
      invalidAttempt,
      "TASK_STATE_WORKING",
      "must not apply",
    ),
    /expectedAttempt.*integer between 1 and 8/,
  );
  assert.deepEqual(restored.snapshot(), beforeInvalidAttempt);
}

const firstProgress = restored.transitionDispatchTask(
  dispatchId,
  firstAttempt,
  "TASK_STATE_WORKING",
  "half complete",
);
assert.equal(firstProgress.tasks[tenantAFirst].state, "TASK_STATE_WORKING");
assert.equal(firstProgress.tasks[tenantAFirst].status_message, "half complete");
assert.equal(firstProgress.dispatch_outbox[dispatchId].state, "running");
const secondProgress = restored.transitionDispatchTask(
  dispatchId,
  firstAttempt,
  "TASK_STATE_WORKING",
  "three quarters complete",
);
assert.equal(secondProgress.tasks[tenantAFirst].state, "TASK_STATE_WORKING");
assert.equal(secondProgress.tasks[tenantAFirst].status_message, "three quarters complete");
assert.equal(secondProgress.dispatch_outbox[dispatchId].state, "running");
assert(secondProgress.revision > firstProgress.revision);

const reconcileState = restored.markDispatchReconcilePending(
  dispatchId,
  "Bearer must-never-be-persisted",
);
assert.equal(reconcileState.dispatch_outbox[dispatchId].state, "reconcile_pending");
assert.equal(
  reconcileState.dispatch_outbox[dispatchId].last_error,
  "dispatch requires reconciliation",
);
assert(!JSON.stringify(reconcileState).includes("must-never-be-persisted"));
const reclaimedState = restored.markDispatchRunning(dispatchId);
const currentAttempt = reclaimedState.dispatch_outbox[dispatchId].attempts;
assert.equal(currentAttempt, 2);
const beforeStaleAttempt = restored.snapshot();
assert.throws(
  () => restored.transitionDispatchTask(
    dispatchId,
    firstAttempt,
    "TASK_STATE_COMPLETED",
    "done",
  ),
  /generation is stale/,
);
assert.deepEqual(restored.snapshot(), beforeStaleAttempt);

const transitionedState = restored.transitionDispatchTask(
  dispatchId,
  currentAttempt,
  "TASK_STATE_COMPLETED",
  "done",
);
assert.equal(transitionedState.schema_version, EXPECTED_A2A_MAPPER_SCHEMA_VERSION);
assert.equal(transitionedState.dispatch_outbox[dispatchId].state, "settled");
assert.deepEqual(transitionedState, restored.snapshot());
assert.deepEqual(
  restored.transitionDispatchTask(
    dispatchId,
    currentAttempt,
    "TASK_STATE_COMPLETED",
    "done",
  ),
  transitionedState,
);
const completed = restored.getTask(
  tenantAFirst,
  correlation("completed-task"),
  tenantA,
);
assert.equal(completed.action.task.state, "TASK_STATE_COMPLETED");
assert.equal(completed.action.task.status_message, "done");
assert.throws(
  () => restored.transitionTask(tenantAFirst, "TASK_STATE_WORKING"),
  /A2A protocol error \(invalid_transition\)/,
);

assert.throws(
  () => restored.listTasks(
    { tenant: "tenant-a", pageSize: 1, unexpected: true },
    correlation("strict-input"),
    tenantA,
  ),
  /invalid A2A list-tasks request/,
);

const invalidMapper = new A2aMapper();
const validMessage = {
  message_id: "governed-invalid-message",
  role: "ROLE_USER",
  parts: [{ kind: "text", text: "hello" }],
};
const emptyPrincipal = invalidMapper.sendMessage(
  validMessage,
  correlation("empty-principal"),
  { ...tenantA, subject: "" },
);
assert.equal(emptyPrincipal.envelope.authorization.status, "denied");
assert.equal(emptyPrincipal.envelope.authorization.code, "invalid_request");
assert(!("action" in emptyPrincipal));

const emptyCorrelation = invalidMapper.sendMessage(
  validMessage,
  { correlation_id: "", request_id: "empty-correlation-request" },
  tenantA,
);
assert.equal(emptyCorrelation.envelope.authorization.status, "denied");
assert.equal(emptyCorrelation.envelope.authorization.code, "invalid_request");
assert(!("action" in emptyCorrelation));

const emptyParts = invalidMapper.sendMessage(
  { ...validMessage, message_id: "empty-parts", parts: [] },
  correlation("empty-parts"),
  tenantA,
);
assert.equal(emptyParts.envelope.authorization.status, "denied");
assert.equal(emptyParts.envelope.authorization.code, "invalid_request");
assert(!("action" in emptyParts));

assert.throws(
  () => invalidMapper.sendMessage(
    {
      ...validMessage,
      message_id: "unknown-part-field",
      parts: [{ kind: "text", text: "hello", unexpected: true }],
    },
    correlation("unknown-part-field"),
    tenantA,
  ),
  /unknown field.*unexpected/,
);

const scopedMapper = new A2aMapper();
const sharedMessage = {
  message_id: "shared-message-id",
  context_id: "shared-id-context",
  role: "ROLE_USER",
  parts: [{ kind: "text", text: "same payload" }],
};
const tenantAReceipt = scopedMapper.sendMessage(
  sharedMessage,
  correlation("shared-id-tenant-a"),
  tenantA,
);
const tenantBReceipt = scopedMapper.sendMessage(
  sharedMessage,
  correlation("shared-id-tenant-b"),
  tenantB,
);
assert.equal(tenantAReceipt.action.kind, "dispatch_message");
assert.equal(tenantBReceipt.action.kind, "dispatch_message");
assert.notEqual(
  tenantAReceipt.action.mapping.task_id,
  tenantBReceipt.action.mapping.task_id,
);

const tenantADuplicate = scopedMapper.sendMessage(
  sharedMessage,
  correlation("shared-id-tenant-a-duplicate"),
  tenantA,
);
assert.equal(tenantADuplicate.action.kind, "duplicate_message");
assert.equal(
  tenantADuplicate.action.receipt.mapping.task_id,
  tenantAReceipt.action.mapping.task_id,
);

for (const [index, waitingState] of [
  "TASK_STATE_WORKING",
  "TASK_STATE_INPUT_REQUIRED",
  "TASK_STATE_AUTH_REQUIRED",
].entries()) {
  const fenceMapper = new A2aMapper();
  const initialMessage = {
    message_id: `cancel-fence-initial-${index}`,
    context_id: `cancel-fence-context-${index}`,
    role: "ROLE_USER",
    parts: [{ kind: "text", text: `initial-${index}` }],
  };
  const initial = fenceMapper.sendMessage(
    initialMessage,
    correlation(`cancel-fence-initial-${index}`),
    tenantA,
  );
  assert.equal(initial.action.kind, "dispatch_message");

  if (waitingState !== "TASK_STATE_WORKING") {
    const [waitingDispatchId] = dispatchForTask(
      fenceMapper,
      initial.action.mapping.task_id,
    );
    const claimed = fenceMapper.markDispatchRunning(waitingDispatchId);
    const waitingAttempt = claimed.dispatch_outbox[waitingDispatchId].attempts;
    fenceMapper.transitionDispatchTask(
      waitingDispatchId,
      waitingAttempt,
      waitingState,
      `waiting-${index}`,
    );
  }
  const cancellation = fenceMapper.cancelTask(
    initial.action.mapping.task_id,
    correlation(`cancel-fence-cancel-${index}`),
    tenantA,
  );
  assert.equal(cancellation.envelope.authorization.status, "allowed");
  assert.equal(cancellation.action.kind, "cancel_task");
  assert.equal(cancellation.action.task.state, waitingState);
  assert.equal(cancellation.action.task.status_message, "cancellation requested");

  const beforeSnapshot = fenceMapper.snapshot();
  const beforeBytes = serializedState(fenceMapper);
  const exactRetry = fenceMapper.sendMessage(
    initialMessage,
    correlation(`cancel-fence-exact-retry-${index}`),
    tenantA,
  );
  assert.equal(exactRetry.envelope.authorization.status, "allowed");
  assert.equal(exactRetry.action.kind, "duplicate_message");
  assert.deepEqual(fenceMapper.snapshot(), beforeSnapshot);
  assert(beforeBytes.equals(serializedState(fenceMapper)));

  const blocked = fenceMapper.sendMessage(
    targetedMessage(`cancel-fence-blocked-${index}`, initial.action.mapping),
    correlation(`cancel-fence-blocked-${index}`),
    tenantA,
  );
  assert.deepEqual(blocked.envelope.authorization, {
    status: "denied",
    code: "state_conflict",
    reason: "A2A task has an unsettled cancellation and cannot accept another message",
  });
  assert(!("action" in blocked));
  const afterSnapshot = fenceMapper.snapshot();
  assert.deepEqual(afterSnapshot, beforeSnapshot);
  assert(beforeBytes.equals(serializedState(fenceMapper)));
  assert.equal(
    Object.keys(afterSnapshot.receipts).length,
    Object.keys(beforeSnapshot.receipts).length,
  );
  assert.equal(
    Object.keys(afterSnapshot.dispatch_outbox).length,
    Object.keys(beforeSnapshot.dispatch_outbox).length,
  );
  assert.equal(
    Object.keys(afterSnapshot.pending_events).length,
    Object.keys(beforeSnapshot.pending_events).length,
  );
  assert.equal(afterSnapshot.revision, beforeSnapshot.revision);
}

// The binding's 32 MiB persistence boundary is transactional: the candidate that crosses the
// limit must not remain installed after the thrown error.
const oversizedMapper = new A2aMapper();
const largeText = "x".repeat(240 * 1024);
let oversizedRejected = false;
for (let index = 0; index < 100; index += 1) {
  const before = JSON.stringify(oversizedMapper.snapshot());
  const owner = {
    subject: `large-owner-${index}`,
    tenant_id: `large-tenant-${index}`,
    scopes: ["a2a:message:send"],
  };
  try {
    oversizedMapper.sendMessage(
      {
        message_id: `large-message-${index}`,
        context_id: `large-context-${index}`,
        role: "ROLE_USER",
        parts: [{ kind: "text", text: largeText }],
      },
      correlation(`large-message-${index}`),
      owner,
    );
  } catch (error) {
    assert.match(String(error), /exceeds the 33554432 byte limit/);
    assert.equal(JSON.stringify(oversizedMapper.snapshot()), before);
    oversizedRejected = true;
    break;
  }
}
assert(oversizedRejected, "test data did not reach the A2A snapshot byte limit");

console.log("a2a mapper Node contract: ok");
