"use strict";

// Official-SDK conformance for the aikit MCP 2025-11-25 Streamable HTTP server.
//
// Connects the pinned `@modelcontextprotocol/sdk` client to the deterministic
// `mcp_sdk_sut` fixture and asserts initialize/tools/resources/prompts behaviour, including
// bearer authentication and fail-closed negative paths. Run through
// `scripts/mcp-sdk-conformance.sh`, which owns the SUT lifecycle.
const assert = require("node:assert/strict");

const { Client } = require("@modelcontextprotocol/sdk/client/index.js");
const {
  StreamableHTTPClientTransport,
} = require("@modelcontextprotocol/sdk/client/streamableHttp.js");
const {
  discoverOAuthProtectedResourceMetadata,
  extractResourceMetadataUrl,
} = require("@modelcontextprotocol/sdk/client/auth.js");

const SUT_URL = process.env.AIKIT_MCP_SUT_URL ?? "http://127.0.0.1:9998/mcp";
const TOKEN = "mcp-sdk-conformance";

function authenticatedTransport() {
  return new StreamableHTTPClientTransport(new URL(SUT_URL), {
    requestInit: { headers: { Authorization: `Bearer ${TOKEN}` } },
  });
}

async function expectRejection(promise, pattern) {
  try {
    await promise;
  } catch (error) {
    const message = String(error?.message ?? error);
    assert.match(message, pattern, `unexpected rejection message: ${message}`);
    return;
  }
  throw new Error(`expected rejection matching ${pattern}`);
}

async function main() {
  // RFC 9728 discovery through the official SDK: the metadata document is public, and the 401
  // challenge advertises its location.
  const resourceMetadata = await discoverOAuthProtectedResourceMetadata(new URL(SUT_URL));
  assert.equal(resourceMetadata.resource, SUT_URL);
  assert.deepEqual(resourceMetadata.authorization_servers, ["https://auth.example.test"]);
  const unauthenticated = await fetch(SUT_URL, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      accept: "application/json, text/event-stream",
    },
    body: JSON.stringify({
      jsonrpc: "2.0",
      id: 1,
      method: "initialize",
      params: {
        protocolVersion: "2025-11-25",
        capabilities: {},
        clientInfo: { name: "discovery-probe", version: "1.0.0" },
      },
    }),
  });
  assert.equal(unauthenticated.status, 401);
  const advertised = extractResourceMetadataUrl(unauthenticated);
  assert.ok(advertised, "401 challenge must advertise resource_metadata");
  assert.equal(
    String(advertised),
    new URL("/.well-known/oauth-protected-resource", SUT_URL).toString(),
  );

  // The official client refuses unauthenticated servers exactly like a browser would.
  await expectRejection(
    new Client({ name: "unauthenticated", version: "1.0.0" }).connect(
      new StreamableHTTPClientTransport(new URL(SUT_URL)),
    ),
    /401|Unauthorized|authentication/i,
  );

  const client = new Client({ name: "aikit-mcp-sdk-conformance", version: "1.0.0" });
  await client.connect(authenticatedTransport());

  const serverVersion = client.getServerVersion();
  assert.equal(serverVersion.name, "aikit-mcp-sdk-sut");

  const tools = await client.listTools();
  assert.deepEqual(
    tools.tools.map((tool) => tool.name),
    ["echo"],
  );
  assert.equal(tools.tools[0].inputSchema.type, "object");

  const call = await client.callTool({ name: "echo", arguments: { text: "official-sdk" } });
  assert.equal(call.isError ?? false, false);
  assert.deepEqual(call.content, [{ type: "text", text: "echo: official-sdk" }]);

  const resources = await client.listResources();
  assert.deepEqual(
    resources.resources.map((resource) => resource.uri),
    ["demo://greeting"],
  );
  const read = await client.readResource({ uri: "demo://greeting" });
  assert.deepEqual(read.contents, [
    { uri: "demo://greeting", mimeType: "text/plain", text: "deterministic greeting body" },
  ]);

  const prompts = await client.listPrompts();
  assert.deepEqual(
    prompts.prompts.map((prompt) => prompt.name),
    ["greet"],
  );
  const prompt = await client.getPrompt({ name: "greet", arguments: { name: "Ada" } });
  assert.equal(prompt.description, "deterministic greeting prompt");
  assert.deepEqual(prompt.messages, [
    { role: "user", content: { type: "text", text: "Greet Ada politely." } },
  ]);

  // Fail-closed negatives surface as JSON-RPC errors through the official client.
  await expectRejection(
    client.callTool({ name: "missing-tool", arguments: {} }),
    /not registered|unknown|-32602|invalid/i,
  );
  await expectRejection(
    client.getPrompt({ name: "greet", arguments: {} }),
    /required argument|-32602|invalid/i,
  );
  await expectRejection(
    client.readResource({ uri: "demo://missing" }),
    /not registered|unknown|-32602|invalid/i,
  );

  await client.close();
  console.log("official MCP SDK conformance: ok");
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
