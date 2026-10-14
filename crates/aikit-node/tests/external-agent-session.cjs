"use strict";

// Keyless live-session coverage: spawn a fake ACP agent and drive it through the binding.
const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");

const { spawnExternalAgentSession } = require("..");

const FAKE_ACP_AGENT = `#!/bin/sh
while IFS= read -r line; do
  id=$(printf '%s' "$line" | sed -n 's/.*"id":"\\([^"]*\\)".*/\\1/p')
  case "$line" in
    *'"method":"initialize"'*)
      printf '{"jsonrpc":"2.0","id":"%s","result":{"protocolVersion":1,"agentCapabilities":{},"authMethods":[],"agentInfo":{"name":"fake-acp","version":"9.9.9"}}}\\n' "$id"
      ;;
    *'"method":"session/new"'*)
      printf '{"jsonrpc":"2.0","id":"%s","result":{"sessionId":"session-1"}}\\n' "$id"
      ;;
    *'"method":"session/prompt"'*)
      printf '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"session-1","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"hello from fake"}}}}\\n'
      printf '{"jsonrpc":"2.0","id":"%s","result":{"stopReason":"end_turn"}}\\n' "$id"
      ;;
  esac
done
`;

function withTimeout(promise, label) {
  return Promise.race([
    promise,
    new Promise((_, reject) =>
      setTimeout(() => reject(new Error(`${label} timed out`)), 15_000),
    ),
  ]);
}

async function main() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "aikit-acp-session-"));
  try {
    const executable = path.join(root, "gemini");
    fs.writeFileSync(executable, FAKE_ACP_AGENT, { mode: 0o700 });

    // The Gemini vendor-terms gate fails closed before any process is spawned.
    await assert.rejects(
      spawnExternalAgentSession("gemini", root, { explicitExecutable: executable }),
      /account terms/,
    );

    const session = await withTimeout(
      spawnExternalAgentSession("gemini", root, {
        explicitExecutable: executable,
        acknowledgeVendorTerms: true,
      }),
      "spawn",
    );
    const events = session.events();
    const descriptor = await withTimeout(session.initialize(), "initialize");
    assert.equal(descriptor.kind, "gemini");
    assert.equal(descriptor.version, "9.9.9");

    const runtimeSession = await withTimeout(
      session.newSession({ cwd: root, additional_directories: [] }),
      "newSession",
    );
    assert.equal(runtimeSession.session_id, "session-1");

    await withTimeout(
      session.prompt({
        session_id: "session-1",
        prompt_id: "prompt-1",
        blocks: [{ type: "text", text: "hi" }],
      }),
      "prompt",
    );

    const seen = [];
    for (;;) {
      const event = await withTimeout(events.next(), "event");
      if (event == null) break;
      seen.push(event.event.type);
      if (event.event.type === "completed") break;
    }
    assert.ok(seen.includes("content_delta"), JSON.stringify(seen));
    await withTimeout(session.shutdown(), "shutdown");
    console.log("external agent session Node contract: ok");
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
}

main().then(
  () => process.exit(0),
  (error) => {
    console.error(error);
    process.exit(1);
  },
);
