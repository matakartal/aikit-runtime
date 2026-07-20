"use strict";

// Runtime contract for binding-owned audit and persistent local state.
const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const {
  Agent,
  DurableRun,
  normalizeCedarDecision,
  modelCapabilityState,
  normalizeOpaDecision,
  resolveModelCatalog,
  sealGovernanceBinding,
  sealPolicySnapshot,
  shippedModelCatalog,
  validateMediaArtifact,
  validateMediaInput,
  validateModelProfile,
} = require("..");

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

const toolSchema = {
  type: "object",
  required: ["q"],
  properties: { q: { type: "string" } },
  additionalProperties: false,
};

const objectSchema = {
  type: "object",
  required: ["currency", "status"],
  properties: {
    currency: { type: "string", enum: ["EUR"] },
    status: { type: "string", enum: ["ok"] },
  },
  additionalProperties: false,
};

function sdkContractHelpers() {
  const digest = "a".repeat(64);
  const media = {
    media_type: "image/png",
    source: { kind: "artifact", artifact_id: "artifact-image-1" },
    sha256: digest,
    size_bytes: 12,
  };
  assert.deepEqual(validateMediaInput(media), media);
  const inlineMedia = {
    media_type: "application/octet-stream",
    source: { kind: "bytes", data: [97, 98, 99] },
    sha256: "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
    size_bytes: 3,
  };
  assert.deepEqual(validateMediaInput(inlineMedia), inlineMedia);
  const artifact = {
    artifact_id: "artifact-image-1",
    media_type: "image/png",
    sha256: digest,
    size_bytes: 12,
  };
  assert.deepEqual(validateMediaArtifact(artifact), artifact);
  const mediaRejected = (candidate) => {
    try {
      validateMediaInput(candidate);
      return false;
    } catch (_error) {
      return true;
    }
  };
  const abcHash = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
  const base64Media = {
    media_type: "application/octet-stream",
    source: { kind: "base64", data: "YWJj" },
    sha256: abcHash,
    size_bytes: 3,
  };
  const urlMedia = {
    media_type: "image/png",
    source: { kind: "url", url: "https://example.com/image.png" },
    sha256: digest,
    size_bytes: 1,
  };
  assert.deepEqual(validateMediaInput(base64Media), base64Media);
  const mediaValidation = {
    artifact_empty_rejected: mediaRejected({
      ...media, source: { kind: "artifact", artifact_id: " " },
    }),
    base64_hash_rejected: mediaRejected({ ...base64Media, sha256: "0".repeat(64) }),
    base64_invalid_rejected: mediaRejected({
      ...base64Media, source: { kind: "base64", data: "%%%INVALID" },
    }),
    base64_size_rejected: mediaRejected({ ...base64Media, size_bytes: 2 }),
    base64_valid: true,
    bytes_hash_rejected: mediaRejected({ ...inlineMedia, sha256: "0".repeat(64) }),
    bytes_size_rejected: mediaRejected({ ...inlineMedia, size_bytes: 2 }),
    bytes_valid: true,
    credential_url_rejected: mediaRejected({
      ...urlMedia,
      source: { kind: "url", url: "https://user:secret@example.com/image.png" },
    }),
    mime_case_insensitive_valid: validateMediaInput({
      ...base64Media, media_type: "Image/PNG",
    }).media_type === "Image/PNG",
    mime_extra_slash_rejected: mediaRejected({ ...urlMedia, media_type: "image/png/extra" }),
    mime_parameter_rejected: mediaRejected({
      ...urlMedia, media_type: "image/png; charset=utf-8",
    }),
    mime_whitespace_rejected: mediaRejected({ ...urlMedia, media_type: "image /png" }),
    relative_url_rejected: mediaRejected({
      ...urlMedia, source: { kind: "url", url: "/image.png" },
    }),
    unknown_field_rejected: mediaRejected({ ...urlMedia, unexpected: true }),
    url_reference_valid: (() => {
      assert.deepEqual(validateMediaInput(urlMedia), urlMedia);
      return true;
    })(),
    url_scheme_rejected: mediaRejected({
      ...urlMedia, source: { kind: "url", url: "file:///tmp/image.png" },
    }),
  };
  assert(Object.values(mediaValidation).every(Boolean));
  assert.throws(() => validateMediaInput({ ...media, sha256: digest.toUpperCase() }));
  assert.throws(() => validateMediaInput({ ...media, size_bytes: 0 }));
  assert.throws(() => validateMediaInput({ ...inlineMedia, sha256: "0".repeat(64) }));
  assert.throws(() => validateMediaArtifact({ ...artifact, artifact_id: "" }));

  const shipped = shippedModelCatalog();
  assert.equal(shipped.sources.length, 8);
  assert.equal(shipped.profiles.length, 8);
  assert.deepEqual(validateModelProfile(shipped.profiles[0]), shipped.profiles[0]);
  assert(["supported", "unsupported", "unknown"].includes(
    modelCapabilityState(shipped.profiles[0], "realtime_duplex"),
  ));
  const originalLimit = shipped.profiles[0].max_output_tokens;
  const override = { ...shipped.profiles[0], max_output_tokens: originalLimit - 1 };
  const resolved = resolveModelCatalog([override]);
  assert.equal(resolved.override_count, 1);
  assert.notEqual(resolved.shipped_hash, resolved.overrides_hash);
  assert.equal(shippedModelCatalog().profiles[0].max_output_tokens, originalLimit);

  const metadata = {
    policy_rule_id: "package/aikit/allow",
    input_summary: "tool=Read path=/workspace/a.txt",
    risk_evidence: ["workspace_path"],
    evaluator_revision: "rev-1",
  };
  const opa = normalizeOpaDecision(
    { result: { effect: "allow", rule_id: "allow.read" } },
    metadata,
  );
  assert.equal(opa.engine, "opa");
  assert.equal(opa.effect, "allow");
  assert.throws(() => normalizeOpaDecision(
    { result: { effect: "allow", partial: true } }, metadata,
  ));
  const cedar = normalizeCedarDecision(
    {
      decision: "Allow",
      permit_policy_ids: ["permit.read"],
      forbid_policy_ids: ["forbid.secret"],
    },
    metadata,
  );
  assert.equal(cedar.engine, "cedar");
  assert.equal(cedar.effect, "deny");

  const requestCases = [
    ["confirmation", (run) => run.requestConfirmation("confirm", "Proceed?")],
    ["missing_input", (run) => run.requestInput(
      "input", "Currency?", { type: "string", enum: ["EUR"] },
    )],
    ["output_review", (run) => run.requestOutputReview(
      "review", "Review output", { status: "draft" },
    )],
    ["edit_retry", (run) => run.requestEditRetry(
      "retry", "Edit or retry", { status: "invalid" }, "status mismatch",
    )],
  ];
  requestCases.forEach(([kind, request], index) => {
    const run = new DurableRun("session-sdk", `run-sdk-${index}`);
    const approvalId = request(run);
    const approval = run.snapshot().projection.approvals[approvalId];
    assert.equal(approval.payload.kind, kind);
    const outcome = run.resolveApproval(
      `resume-${index}`, approvalId, true, { accepted: true },
    );
    assert.equal(outcome.type, "resumed");
    assert.equal(run.status, "running");
  });

  const policySnapshot = sealPolicySnapshot({
    schema_version: 1,
    default_effect: "deny",
    rules: [{
      id: "allow.read",
      scope: { scope: "tool", tool: "Read" },
      effect: "allow",
    }],
  });
  const governed = DurableRun.withPolicySnapshot(
    "session-governed", "run-governed", policySnapshot,
  );
  assert.equal(governed.policySnapshotHash, policySnapshot.hash);
  assert.equal(governed.snapshot().events[1].kind.type, "governance_binding_pinned");
  assert.deepEqual(governed.governanceBinding, governed.snapshot().events[1].kind.binding);
  const typedId = governed.requestTypedApproval({
    logical_key: "customer-id",
    kind: "missing_input",
    prompt: "Customer id?",
    payload: { field: "customer_id" },
    policy_snapshot_hash: policySnapshot.hash,
    requested_at_unix_ms: 100,
    expires_at_unix_ms: 200,
  });
  const typed = governed.snapshot().projection.approvals[typedId];
  assert.equal(typed.kind, "missing_input");
  assert.equal(typed.policy_snapshot_hash, policySnapshot.hash);
  assert.deepEqual(typed.governance_binding, governed.governanceBinding);
  assert.equal(typed.requested_at_unix_ms, 100);
  assert.equal(typed.expires_at_unix_ms, 200);
  const beforeClockRejection = governed.snapshot();
  assert.throws(() => governed.resolveApproval(
    "resume-without-clock", typedId, true, "cust-1",
  ));
  assert.deepEqual(governed.snapshot(), beforeClockRejection);
  const restarted = DurableRun.fromState(governed.snapshot());
  const typedOutcome = restarted.resolveApprovalAt(
    "resume-with-clock", typedId, true, 150n, "cust-1",
  );
  assert.equal(typedOutcome.type, "resumed");
  const resolvedApproval = restarted.snapshot().projection.approvals[typedId];
  assert.equal(resolvedApproval.response, "cust-1");
  assert.equal(resolvedApproval.resolved_at_unix_ms, 150);
  assert.equal(resolvedApproval.timed_out, false);

  const scopedBinding = sealGovernanceBinding(
    policySnapshot, "run-scoped", "tenant-a", "agent-a",
  );
  const scoped = DurableRun.withGovernanceBinding(
    "session-scoped", "run-scoped", scopedBinding,
  );
  assert.deepEqual(scoped.governanceBinding, scopedBinding);
  assert.equal(scoped.policySnapshotHash, policySnapshot.hash);
  assert.throws(() => DurableRun.withGovernanceBinding(
    "session-tampered",
    "run-scoped",
    { ...scopedBinding, tenant_id: "tenant-b" },
  ));
  assert.throws(() => DurableRun.withGovernanceBinding(
    "session-mismatch", "different-run", scopedBinding,
  ));
  mediaValidation.governance_binding_valid = true;

  const timeoutRun = new DurableRun("session-timeout", "run-timeout");
  const timeoutId = timeoutRun.requestTypedApproval({
    logical_key: "review",
    kind: "output_review",
    prompt: "Review output",
    payload: { status: "draft" },
    requested_at_unix_ms: 100,
    expires_at_unix_ms: 110,
  });
  const eventCount = timeoutRun.snapshot().events.length;
  assert.deepEqual(timeoutRun.expireApprovals("sweep-1", 110n), [timeoutId]);
  assert.equal(timeoutRun.snapshot().events.length, eventCount + 1);
  assert.deepEqual(timeoutRun.expireApprovals("sweep-1", 110n), []);
  const expired = timeoutRun.snapshot().projection.approvals[timeoutId];
  assert.equal(expired.status, "rejected");
  assert.equal(expired.timed_out, true);
  assert.equal(expired.resolved_at_unix_ms, 110);
  assert.equal(timeoutRun.applyCommandAt(
    { command: "resume", command_id: "resume-timeout" }, 110n,
  ).type, "resumed");
  return mediaValidation;
}

function childSpec(id) {
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
    allowed_tools: [],
    max_turns: 2,
    max_tokens: 64,
    estimated_input_tokens: 8,
  };
}

async function drain(stream) {
  for await (const _event of stream) {
    // Exhaustion finalizes the canonical outcome and audit trail.
  }
  return stream.outcome();
}

async function drainObject(stream) {
  for await (const _event of stream) {
    // Exhaust every structured-output event.
  }
}

async function compatibilityContract() {
  const agent = Agent.fromEnv({});
  const providerOptions = { mock: { future_option: true } };

  const strictStream = agent.run("strict-default", { providerOptions });
  const strictEvents = [];
  for await (const event of strictStream) strictEvents.push(event);
  const strictOutcome = strictStream.outcome();
  assert.equal(strictOutcome.terminal_status, "failed");
  assert.equal(strictEvents.at(-1)?.type, "error");
  assert.equal(strictEvents.at(-1)?.info?.code, "provider_invalid_request");

  const warningRuns = {};
  for (const compatibilityMode of ["warn", "best_effort"]) {
    const stream = agent.run(compatibilityMode, { providerOptions, compatibilityMode });
    const events = [];
    for await (const event of stream) events.push(event);
    const warning = events.find((event) => event.type === "warning")?.warning;
    const outcome = stream.outcome();
    assert.equal(warning?.parameter, "future_option");
    assert.equal(outcome.warnings?.[0]?.parameter, "future_option");
    warningRuns[compatibilityMode] = true;
  }

  assert.throws(() => agent.run("invalid-mode", { compatibilityMode: "loose" }));

  await assert.rejects(
    agent.generateObject("strict object", objectSchema, { providerOptions }),
    (error) => error?.code === "provider_invalid_request",
  );
  const warnedObject = await agent.generateObject("warn object", objectSchema, {
    providerOptions,
    compatibilityMode: "warn",
  });
  assert.equal(warnedObject.warnings[0].parameter, "future_option");

  const objectStream = agent.streamObject("best effort object", objectSchema, {
    providerOptions,
    compatibilityMode: "best_effort",
  });
  const objectEvents = [];
  for await (const event of objectStream) objectEvents.push(event);
  assert(objectEvents.some((event) =>
    event.type === "delta" &&
    event.delta.type === "warning" &&
    event.delta.warning.parameter === "future_option"));
  assert.equal(
    objectEvents.find((event) => event.type === "completed")?.object.warnings[0].parameter,
    "future_option",
  );

  return {
    compatibility_best_effort_warning: warningRuns.best_effort,
    compatibility_default_strict: true,
    compatibility_invalid_mode_rejected: true,
    compatibility_object_strict: true,
    compatibility_object_warning: true,
    compatibility_warn_warning: warningRuns.warn,
  };
}

function readJsonl(file) {
  return fs
    .readFileSync(file, "utf8")
    .split("\n")
    .filter(Boolean)
    .map((line) => JSON.parse(line));
}

function invalidConfigurationRejected(file) {
  const cases = [
    ["invalid", "fail_closed"],
    ["metadata_only", "invalid"],
  ];
  return cases.every(([payloadPolicy, failureMode]) => {
    assert.throws(() => {
      Agent.fromEnv({}).configureJsonlAudit(file, payloadPolicy, failureMode);
    });
    return true;
  });
}

function symlinkGuard(tmp) {
  if (process.platform === "win32") return "not_applicable";
  const target = path.join(tmp, "audit-target.jsonl");
  const link = path.join(tmp, "audit-link.jsonl");
  fs.writeFileSync(target, "");
  fs.symlinkSync(target, link);
  try {
    Agent.fromEnv({}).configureJsonlAudit(link);
  } catch (_error) {
    return "rejected";
  }
  return "accepted";
}

async function main() {
  const mediaValidation = sdkContractHelpers();
  const compatibilityValidation = await compatibilityContract();
  const sdkValidation = Object.fromEntries(
    Object.entries({ ...mediaValidation, ...compatibilityValidation })
      .sort(([left], [right]) => left < right ? -1 : left > right ? 1 : 0),
  );
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "aikit-production-state-"));
  try {
    const metadataPath = path.join(tmp, "metadata.jsonl");
    const fullPath = path.join(tmp, "full.jsonl");
    const memoryPath = path.join(tmp, "memory.json");
    const sessionPath = path.join(tmp, "sessions.json");
    const sqlitePath = path.join(tmp, "state.db");

    const invalidEnums = invalidConfigurationRejected(path.join(tmp, "invalid.jsonl"));
    const symlink = symlinkGuard(tmp);
    let immediateOpenError = false;
    try {
      Agent.fromEnv({}).configureJsonlAudit(
        path.join(tmp, "missing", "audit.jsonl"),
      );
    } catch (_error) {
      immediateOpenError = true;
    }

    const agent = Agent.fromEnv({});
    agent.configureJsonlAudit(metadataPath);
    agent.useMemoryFile(memoryPath, "tenant-a");
    agent.useSessionFile(sessionPath);

    let toolCalls = 0;
    agent.addTool("search", "search", toolSchema, async () => {
      toolCalls += 1;
      return "META_OUTPUT_SECRET";
    });
    agent.onPreToolUse(async () => ({
      action: "rewrite",
      input: { q: "META_INPUT_SECRET" },
    }), "search");

    await agent.generateText("generate");
    await drain(agent.streamText("stream"));
    await drain(agent.run("run"));
    const client = agent.client();
    await drain(client.query("client"));

    const beforeDeny = toolCalls;
    agent.setPermissions([{ effect: "deny", tool: "search" }]);
    await agent.generateText("denied");
    assert.equal(toolCalls, beforeDeny, "denied tool reached the host callback");
    agent.setPermissions([], "allow");

    await agent.generateObject("object", objectSchema);
    await drainObject(agent.streamObject("object stream", objectSchema));

    const memoryWriter = Agent.fromEnv({});
    memoryWriter.useMemoryFile(memoryPath, "tenant-a");
    memoryWriter.remember("customer_note", "Ada prefers EUR");
    const persistedMemory = JSON.parse(fs.readFileSync(memoryPath, "utf8"));
    const memoryFilePersisted = persistedMemory.some(
      (entry) =>
        entry.namespace === "tenant-a" &&
        entry.key === "customer_note" &&
        entry.value === "Ada prefers EUR",
    );
    const memoryReopened = Agent.fromEnv({});
    memoryReopened.useMemoryFile(memoryPath, "tenant-a");
    const recalled = memoryReopened.recall("EUR");
    const memoryIsolated = Agent.fromEnv({});
    memoryIsolated.useMemoryFile(memoryPath, "tenant-b");

    const sqliteWriter = Agent.fromEnv({});
    sqliteWriter.useSqliteMemory(sqlitePath, "tenant-a");
    sqliteWriter.remember("sqlite_note", "durable SQLite");
    const sqliteReader = Agent.fromEnv({});
    sqliteReader.useSqliteMemory(sqlitePath, "tenant-a");
    assert.equal(sqliteReader.recall("SQLite")[0].key, "sqlite_note");

    const sqliteSessions = Agent.fromEnv({});
    sqliteSessions.useSqliteSessions(sqlitePath);
    const sqliteCreated = await sqliteSessions.runSubagent(
      childSpec("sqlite-session"), profiles,
    );
    assert.equal(sqliteCreated.status, "succeeded");
    const sqliteReopened = Agent.fromEnv({});
    sqliteReopened.useSqliteSessions(sqlitePath);
    const sqliteResumed = await sqliteReopened.resumeSubagent(
      "sqlite-session", childSpec("sqlite-session-resumed"), profiles,
    );
    assert.equal(sqliteResumed.status, "succeeded");

    const networkTools = Agent.fromEnv({});
    networkTools.registerWebTools(
      ["example.com"], "https://example.com/search?q={query}",
    );
    const browserDenied = Agent.fromEnv({});
    assert.throws(
      () => browserDenied.registerBrowserTools(
        "http://127.0.0.1:4444",
        "session",
        ["example.com"],
        { externalEgressEnforced: false },
      ),
      /BrowserEgressPolicy::ExternallyEnforced/,
    );
    assert(!browserDenied.capabilities().tools.includes("BrowserNavigate"));
    networkTools.registerBrowserTools(
      "http://127.0.0.1:4444", "session", ["example.com"],
      { externalEgressEnforced: true },
    );
    const networkNames = new Set(networkTools.capabilities().tools);
    for (const name of ["WebFetch", "WebSearch", "BrowserNavigate", "BrowserSnapshot"]) {
      assert(networkNames.has(name));
    }

    const created = await agent.runSubagent(childSpec("persist-session"), profiles);
    assert.equal(created.status, "succeeded");

    const crashedDatabase = JSON.parse(fs.readFileSync(sessionPath, "utf8"));
    const persistedBeforeRecovery = crashedDatabase.sessions["persist-session"];
    crashedDatabase.execution_leases ??= {};
    crashedDatabase.execution_leases["persist-session"] = {
      owner: "crashed-worker",
      token: `lease-${"00".repeat(16)}`,
      expires_at_unix_ms: 0,
    };
    fs.writeFileSync(sessionPath, JSON.stringify(crashedDatabase));

    const reopened = Agent.fromEnv({});
    reopened.configureJsonlAudit(metadataPath);
    reopened.useSessionFile(sessionPath);
    const beforeDeniedRecovery = fs.readFileSync(sessionPath, "utf8");
    assert.throws(
      () => reopened.recoverExpiredSession("persist-session", false),
      /sideEffectsReconciled=true/,
    );
    assert.equal(fs.readFileSync(sessionPath, "utf8"), beforeDeniedRecovery);

    const blockedResume = await reopened.resumeSubagent(
      "persist-session",
      childSpec("blocked-expired-resume"),
      profiles,
    );
    assert.equal(blockedResume.status, "session_conflict");
    const stillBlocked = JSON.parse(fs.readFileSync(sessionPath, "utf8"));
    assert.deepEqual(stillBlocked.sessions["persist-session"], persistedBeforeRecovery);
    assert(Object.hasOwn(stillBlocked.execution_leases, "persist-session"));

    const recoveredRevision = reopened.recoverExpiredSession("persist-session", true);
    assert.equal(recoveredRevision, 1);
    const recoveredDatabase = JSON.parse(fs.readFileSync(sessionPath, "utf8"));
    assert.deepEqual(recoveredDatabase.sessions["persist-session"], persistedBeforeRecovery);
    assert(!Object.hasOwn(recoveredDatabase.execution_leases ?? {}, "persist-session"));

    const resumed = await reopened.resumeSubagent(
      "persist-session",
      childSpec("persist-session-resumed"),
      profiles,
    );
    assert.equal(resumed.status, "succeeded");

    const freshDatabase = JSON.parse(fs.readFileSync(sessionPath, "utf8"));
    freshDatabase.execution_leases ??= {};
    freshDatabase.execution_leases["fresh-crash"] = {
      owner: "crashed-worker",
      token: `lease-${"11".repeat(16)}`,
      expires_at_unix_ms: 0,
    };
    fs.writeFileSync(sessionPath, JSON.stringify(freshDatabase));
    assert.equal(reopened.recoverExpiredSession("fresh-crash", true), 0);
    const clearedFresh = JSON.parse(fs.readFileSync(sessionPath, "utf8"));
    assert(!Object.hasOwn(clearedFresh.execution_leases ?? {}, "fresh-crash"));
    assert(!Object.hasOwn(clearedFresh.sessions, "fresh-crash"));
    const freshAfterRecovery = await reopened.runSubagent(
      childSpec("fresh-crash"),
      profiles,
    );
    assert.equal(freshAfterRecovery.status, "succeeded");
    assert.equal(freshAfterRecovery.session_revision, 1);

    const fan = await agent.fanOut(
      [childSpec("fan-a"), childSpec("fan-b")],
      profiles,
      { maxParallelism: 2 },
    );
    assert(fan.every((result) => result.status === "succeeded"));
    const council = await agent.council(
      [childSpec("council-a"), childSpec("council-b")],
      childSpec("council-synthesis"),
      profiles,
      2,
      { maxParallelism: 2 },
    );
    assert.deepEqual(council.status, { kind: "succeeded" });

    const full = Agent.fromEnv({});
    full.configureJsonlAudit(fullPath, "full", "best_effort");
    full.addTool("search", "search", toolSchema, async () => "FULL_OUTPUT_SECRET");
    full.onPreToolUse(async () => ({
      action: "rewrite",
      input: { q: "FULL_INPUT_SECRET" },
    }), "search");
    await full.generateText("full");

    const metadataRecords = readJsonl(metadataPath);
    const metadataText = fs.readFileSync(metadataPath, "utf8");
    const fullText = fs.readFileSync(fullPath, "utf8");
    const eventTypes = new Set(metadataRecords.map((record) => record.type));
    const requiredEvents = [
      "permission_decision",
      "run_started",
      "run_stopped",
      "structured_output_attempt",
      "structured_output_completed",
      "subagent_started",
      "subagent_completed",
      "tool_started",
      "tool_completed",
    ];
    assert(requiredEvents.every((event) => eventTypes.has(event)));

    const runIds = metadataRecords
      .filter((record) => record.type === "run_started")
      .map((record) => record.run_id);
    const topLevelRunIds = metadataRecords
      .filter(
        (record) =>
          record.type === "run_started" && record.parent_run_id === undefined,
      )
      .map((record) => record.run_id);
    const structuredIds = new Set(
      metadataRecords
        .filter((record) => record.type === "structured_output_attempt")
        .map((record) => record.run_id),
    );
    const subagentStarts = metadataRecords.filter(
      (record) => record.type === "subagent_started",
    );
    const expectedChildren = new Set([
      "persist-session",
      "persist-session-resumed",
      "fan-a",
      "fan-b",
      "council-a",
      "council-b",
      "council-synthesis",
    ]);
    const observedChildren = new Set(
      subagentStarts.map((record) => record.subagent_id),
    );
    const parentIds = new Set(
      subagentStarts.map((record) => record.parent_run_id),
    );

    const metadataRedacted =
      !metadataText.includes("META_INPUT_SECRET") &&
      !metadataText.includes("META_OUTPUT_SECRET") &&
      !metadataText.includes('"input"') &&
      !metadataText.includes('"output_preview"');
    const fullCaptured =
      fullText.includes("FULL_INPUT_SECRET") &&
      fullText.includes("FULL_OUTPUT_SECRET") &&
      fullText.includes('"input"') &&
      fullText.includes('"output_preview"');

    const summary = {
      audit: {
        deny_recorded: metadataRecords.some(
          (record) =>
            record.type === "permission_decision" && record.decision === "deny",
        ),
        events_present: [...requiredEvents].sort(),
        full_captured: fullCaptured,
        immediate_open_error: immediateOpenError,
        invalid_enums_rejected: invalidEnums,
        metadata_redacted: metadataRedacted,
        orchestration_paths: [...expectedChildren].every((id) => observedChildren.has(id)),
        parent_correlated:
          subagentStarts.every((record) => Boolean(record.parent_run_id)) &&
          parentIds.size >= 4,
        provider_metadata_omitted: !metadataText.includes("provider_metadata"),
        run_ids_unique: runIds.length === new Set(runIds).size,
        structured_run_ids_unique: structuredIds.size === 2,
        symlink_guard: symlink,
        text_paths:
          topLevelRunIds.length === 5 && new Set(topLevelRunIds).size === 5,
      },
      memory: {
        file_persisted: memoryFilePersisted,
        namespace_isolated: memoryIsolated.recall("EUR").length === 0,
        reopened:
          recalled.length > 0 &&
          recalled[0].key === "customer_note" &&
          recalled[0].value === "Ada prefers EUR",
      },
      sdk: sdkValidation,
      session: {
        file_persisted:
          JSON.parse(fs.readFileSync(sessionPath, "utf8")).sessions["persist-session"]
            .revision === 2,
        reopened: resumed.session_revision === 2,
        revisions: [created.session_revision, resumed.session_revision],
      },
    };
    for (const section of Object.values(summary)) {
      for (const [key, value] of Object.entries(section)) {
        if (!["events_present", "revisions", "symlink_guard"].includes(key)) {
          assert.equal(value, true, `${key} failed`);
        }
      }
    }
    assert(["rejected", "not_applicable"].includes(symlink));
    assert.deepEqual(summary.session.revisions, [1, 2]);
    console.log(`PRODUCTION_STATE_JSON=${JSON.stringify(summary)}`);
  } finally {
    try {
      fs.rmSync(tmp, { recursive: true, force: true });
    } catch (_error) {
      // Open native handles may delay cleanup on Windows; the OS owns this temp directory.
    }
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
