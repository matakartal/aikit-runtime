# aikit Node.js binding

This directory contains the napi binding for the local aikit Rust workspace. It exposes the same
canonical agent, streaming, structured-output, routing, memory, governance, and hook behavior as
the Rust core, with TypeScript declarations in `index.d.ts`.

> The npm distribution name is `aikit-runtime`; the existing bare `aikit` package is unrelated.
> This package remains unpublished until the release evidence gates pass.

Build and load it from the repository checkout:

```bash
./scripts/build-node.sh
node examples/node/agent_governance.cjs
```

```js
const { Agent, tool } = require("./crates/aikit-node");

async function main() {
  const agent = Agent.fromEnv({});
  agent.addToolDefinition(tool(
    "lookup",
    "Look up one symbol",
    { type: "object", properties: { symbol: { type: "string" } } },
    async ({ symbol }) => `price:${symbol}`,
  ));
  const schema = {
    type: "object",
    required: ["status"],
    properties: { status: { type: "string", enum: ["ok"] } },
  };
  for await (const event of agent.streamObject("Return status", schema)) {
    if (event.type === "completed") console.log(event.object.value);
  }
}

main();
```

Every text and structured surface keeps the string convenience form and also accepts canonical
`Message[]` history without flattening media:

```js
const input = [{
  role: "user",
  content: [
    { type: "text", text: "Describe this chart" },
    {
      type: "media",
      media_type: "image/png",
      source: { kind: "base64", data: "aGVsbG8=" },
    },
  ],
}];
const text = await agent.generateText(input);
const object = await agent.generateObject(input, schema);
```

`streamObject` forwards attempt, provider-delta, validation-failure/repair, and completed events.
It also accepts a Zod v4 schema when the optional `zod` peer is installed; only the final completed
value is parsed, so typed convenience does not hide intermediate events. Ask approvers may return
`updated_permissions: ["allow_exact_input" | "allow_tool"]`; a denial may set `interrupt: true` to
stop before the tool callback or another model turn.

`Agent.run()` and reusable `Client.query()` accept the same `RunOptions` (model fallbacks,
provider options, turn/token limits, budget, retry policy, and optional caller-owned
`routing: { profiles, request }`). Their `QueryStream` has
`cancel()`, `close()`, and `outcome()`. `close()` waits for Stop hooks, audit/session recording, and
driver shutdown. The JavaScript wrapper calls it automatically for `for await ... break` and for
an aborted `options.signal`; direct `next()` users should call `await stream.close()` in `finally`.
Async generation and terminal structured-stream failures reject with `AikitError`-shaped values
whose stable `code` and full redacted `info` envelope are safe to branch on.

`agent.subtask(id, prompt, route, options)` builds the canonical child spec, and
`agent.parallel(specs, profiles, options)` is the ergonomic alias for the existing ordered
`fanOut` implementation. Existing `addTool`, `runSubagent`, and `fanOut` names remain supported.
Tool-specific failure hooks use `onPostToolFailure(callback, tool?)` and run before global
`onFailure` hooks.

Production state is opt-in and backed by local files:

```js
agent.configureJsonlAudit("./aikit-audit.jsonl"); // metadata_only + fail_closed
agent.useMemoryFile("./aikit-memory.json", "tenant-a");
agent.useSessionFile("./aikit-sessions.json");
```

Audit configuration opens and validates the JSONL target immediately. Payload capture becomes
`"full"` only when explicitly requested; `"best_effort"` is likewise an explicit alternative to
the default `"fail_closed"`. Memory is written only by `remember()`. File-backed sessions and
memory can be reopened by another Agent, including in a later process, but concurrent coordination
is process-local: these are not distributed stores or cross-process locking primitives.

Built-in tools are opt-in and jailed to every root you register. The first root is used for
relative paths; absolute paths may address any registered root. The initial suite contains only
`Read`, `Write`, `Edit`, `Glob`, and `Grep`:

```js
agent.registerBuiltinTools(["/srv/workspace", "/srv/shared"]);

// Separate explicit opt-in. This always uses fail-closed Required(Auto) OS containment;
// the Node binding has no uncontained Bash mode.
agent.enableBashWithRequiredContainment({
  // Optional Linux fallback. The image must already exist locally and be immutable.
  image: "registry.example/aikit-shell@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  pidsLimit: 64,
  memoryMiB: 512,
  cpus: 1,
  tmpfsMiB: 64,
});
const containment = await agent.builtinContainmentCapabilities();
if (!containment.selected_backend) {
  throw new Error("No required Bash containment backend is available");
}
```

Jailed file tools reject root escapes and symlinks. Windows currently lacks the required
descriptor-relative file-jail implementation, so registration fails closed there; Docker does not
weaken or replace that file-tool boundary. Registering a host callback under a built-in name, or
enabling a built-in whose name already belongs to a callback, throws a deterministic configuration
error. Registered built-ins use the same canonical schemas and executor in normal runs, clients,
fan-out, councils, and resumed subagents.

The generated `aikit_node.node` binary is platform-specific and is never relabeled as portable.
The root `aikit-runtime` package contains the JavaScript/TypeScript surface and exact-version
optional dependencies on `aikit-runtime-{platform}` packages. CI builds, stages, packs, installs,
and loads each supported target independently. Local checkout builds still use the adjacent addon
created by `scripts/build-node.sh`.
Normal examples use the deterministic mock provider and make no billable API call.

Python/Node host callbacks execute in the host process and are not covered by built-in Bash OS
containment. Apply separate process isolation when callback code is untrusted.

Licensed under MIT OR Apache-2.0; both license texts are included in the package.
