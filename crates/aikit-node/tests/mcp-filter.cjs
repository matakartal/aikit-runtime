"use strict";

const assert = require("node:assert/strict");
const http = require("node:http");
const { Agent, McpServer } = require("..");

function mcpReply(id, result) {
  return JSON.stringify({ jsonrpc: "2.0", id, result });
}

async function main() {
  assert.equal(typeof McpServer?.connectHttp, "function");

  await assert.rejects(
    McpServer.connectHttp("http://127.0.0.1:1/mcp", "bad", undefined, {
      unexpected: [],
    }),
    /MCP tool filter contains an unknown field/,
  );
  await assert.rejects(
    McpServer.connectHttp("http://127.0.0.1:1/mcp", "bad", undefined, {
      deny: ["hidden", "hidden"],
    }),
    /duplicate name/,
  );

  const calls = [];
  const server = http.createServer((request, response) => {
    let body = "";
    request.setEncoding("utf8");
    request.on("data", (chunk) => {
      body += chunk;
    });
    request.on("end", () => {
      const message = JSON.parse(body);
      if (message.method === "notifications/initialized") {
        response.writeHead(202).end();
        return;
      }

      let result;
      switch (message.method) {
        case "initialize":
          result = {
            protocolVersion: "2025-06-18",
            serverInfo: { name: "filter-test", version: "1" },
          };
          break;
        case "tools/list":
          result = {
            tools: [
              { name: "safe", description: "safe", inputSchema: { type: "object" } },
              { name: "safe_extra", description: "not an exact match", inputSchema: { type: "object" } },
              { name: "hidden", description: "denied", inputSchema: { type: "object" } },
            ],
          };
          break;
        case "tools/call":
          calls.push(message.params.name);
          result = { content: [{ type: "text", text: "ok" }] };
          break;
        default:
          response.writeHead(400).end();
          return;
      }
      response
        .writeHead(200, { "content-type": "application/json" })
        .end(mcpReply(message.id, result));
    });
  });

  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });

  try {
    const address = server.address();
    assert(address && typeof address === "object");
    const endpoint = `http://127.0.0.1:${address.port}/mcp`;
    const filtered = await McpServer.connectHttp(endpoint, "local", undefined, {
      allow: ["safe", "hidden"],
      deny: ["hidden"],
    });
    const agent = Agent.fromEnv({});
    agent.registerMcp(filtered);
    assert(agent.capabilities().tools.includes("safe"));
    assert(!agent.capabilities().tools.includes("safe_extra"));
    assert(!agent.capabilities().tools.includes("hidden"));

    for await (const _event of agent.run("use the visible tool", {
      model: "mock-1",
      maxTurns: 2,
    })) {
      // Exhaust the run so the deterministic mock completes its one visible tool call.
    }
    assert.deepEqual(calls, ["safe"]);

    const allowAll = await McpServer.connectHttp(endpoint, "default");
    const defaultAgent = Agent.fromEnv({});
    defaultAgent.registerMcp(allowAll);
    assert.deepEqual(
      defaultAgent.capabilities().tools.filter((name) =>
        ["safe", "safe_extra", "hidden"].includes(name)),
      ["safe", "safe_extra", "hidden"],
    );

    const allowNone = await McpServer.connectHttp(endpoint, "none", undefined, {
      allow: [],
    });
    const emptyAgent = Agent.fromEnv({});
    emptyAgent.registerMcp(allowNone);
    assert(!emptyAgent.capabilities().tools.some((name) =>
      ["safe", "safe_extra", "hidden"].includes(name)));
  } finally {
    await new Promise((resolve, reject) => {
      server.close((error) => error ? reject(error) : resolve());
    });
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
