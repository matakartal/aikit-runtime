"use strict";

// Runtime contract for binding-owned audit and persistent local state.
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
    networkTools.registerBrowserTools(
      "http://127.0.0.1:4444", "session", ["example.com"],
    );
    const networkNames = new Set(networkTools.capabilities().tools);
    for (const name of ["WebFetch", "WebSearch", "BrowserNavigate", "BrowserSnapshot"]) {
      assert(networkNames.has(name));
    }

    const created = await agent.runSubagent(childSpec("persist-session"), profiles);
    assert.equal(created.status, "succeeded");

    const reopened = Agent.fromEnv({});
    reopened.configureJsonlAudit(metadataPath);
    reopened.useSessionFile(sessionPath);
    const resumed = await reopened.resumeSubagent(
      "persist-session",
      childSpec("persist-session-resumed"),
      profiles,
    );
    assert.equal(resumed.status, "succeeded");

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
