# aikit Node.js binding

This directory contains the napi binding for the local aikit Rust workspace. It exposes the same
canonical agent, streaming, structured-output, routing, memory, governance, and hook behavior as
the Rust core, with TypeScript declarations (including TSDoc for every export) in `index.d.ts`.

> The npm distribution name is **`aikit-runtime`**; the existing bare `aikit` package is unrelated.
> Local artifact assembly reserves `aikit-runtime-{platform}` for native packages and the wrapper
> selects that shape automatically. No npm registry publication is currently claimed or planned,
> so today you build the binding from this checkout.

## Prerequisites

- Node.js 18.17 or newer (the package `engines` range)
- A Rust toolchain, to compile the native addon
- Optional: `zod` v4, only if you want to pass Zod schemas to `generateObject` / `streamObject`

## Build from this checkout

```bash
# from the repository root; rerun after changing Rust code
./scripts/build-node.sh            # debug build; pass `release` for an optimized one
node examples/node/quickstart.cjs  # keyless guided tour: start here
```

`build-node.sh` compiles the cdylib with cargo and copies it to `crates/aikit-node/aikit_node.node`,
which `index.js` loads. More runnable examples, all keyless except `live_provider.cjs`, are listed
in the [Node examples guide](../../examples/node/README.md).

## Quick start

```js
const { Agent, tool } = require("./crates/aikit-node"); // path relative to the repository root

async function main() {
  // fromEnv({}) ignores your shell, so only the deterministic keyless mock models are active.
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
    if (event.type === "completed") console.log(event.object.value); // { status: 'ok' }
  }
}

main();
```

Everything above runs offline against the built-in mock provider (`mock-1` for text,
`mock-structured` for objects). Its canned text replies are a fixed Turkish sentence; that is
expected.

### Going live

`new Agent()` activates every supported provider key it finds in `process.env`
(`ANTHROPIC_API_KEY`, `OPENAI_API_KEY`, `GEMINI_API_KEY` / `GOOGLE_API_KEY`, ...). You can also add
keys explicitly; the provider is inferred from the key format unless you name it:

```js
const agent = new Agent();
agent.addKey(process.env.ANTHROPIC_API_KEY);        // returns "anthropic"
agent.addKey(process.env.OPENAI_API_KEY, "openai"); // bare `sk-` keys are ambiguous: name them
console.log(agent.activeProviders());
const { text } = await agent.generateText("Say hello", { model: "<a model id from that provider>" });
```

`shippedModelCatalog()` lists reviewed model ids offline. Live calls are real, billed API requests;
see [`examples/node/live_provider.cjs`](../../examples/node/live_provider.cjs) for a capped,
opt-in example.

## Errors and troubleshooting

Failures that come from the core carry a stable `code` (an `ErrorCode`) and a redacted `info`
envelope (`ErrorInfo`: provider, model, HTTP status, `retryable`, `retry_after_ms`). Branch on
`error.code`, never on message text. `AikitError` is a TypeScript interface, not a runtime class,
so `instanceof` does not work:

```js
try {
  await agent.generateText("hello", { model: "gpt-5" });
} catch (error) {
  if (error.code === "provider_auth") console.error("add a key for this provider first");
  else if (error.info?.retryable) console.error("transient failure; retry later");
  else throw error;
}
```

| Symptom | Cause and fix |
|---|---|
| `aikit-runtime could not load aikit-runtime-<platform>` | The local addon is missing. Run `./scripts/build-node.sh` from the repository root (the wrapper only falls back to platform packages when `aikit_node.node` is absent). |
| `code: "provider_auth"`, message `no credential active for provider '...'` | The requested model's provider has no key. Use `new Agent()` with the env var set, or `agent.addKey(...)`. |
| `TypeError: RunOptions contains unknown field '...'` | Option bags are validated strictly so a typo cannot silently weaken a run. Check the field name against `index.d.ts`. |
| `... is single-consumer; concurrent or re-entrant next() ...` | A stream was read from two places at once. Use one `for await` loop, or await each `next()` before the next call. |
| `aikit received a Zod schema, but optional peer dependency 'zod' is not installed` | Install `zod` v4, or pass a plain JSON Schema object. |
| `registerBuiltinTools must be called before enabling contained Bash` | Call `agent.registerBuiltinTools([root])` before `enableBashWithRequiredContainment()`. |

## API at a glance

| Area | Entry points |
|---|---|
| Agent setup | `new Agent()`, `Agent.fromEnv(env)`, `addKey`, `activeProviders`, `capabilities` |
| Text | `generateText`, `streamText`, `run` (full `RunOptions`), `agent.client()` / `Client.query` |
| Structured output | `generateObject`, `streamObject` (JSON Schema or Zod v4, optional semantic `validator`) |
| Tools | `tool(...)` + `addToolDefinition`, `addTool`, `registerBuiltinTools`, `registerMcp`, `registerWebTools`, `registerBrowserTools` |
| Governance | `setPermissions`, `canUseTool`, `onUserPrompt`, `onPreToolUse`, `onPostToolUse`, `onPostToolFailure`, `onFailure`, `onStop`, `enableCapabilityRequests`, `enableDefaultGuardrails` |
| State | `configureJsonlAudit`, `useMemoryFile` / `useSqliteMemory`, `useSessionFile` / `useSqliteSessions`, `remember`, `rememberCas`, `recall`, `recoverExpiredSession` |
| Orchestration | `subtask`, `runSubagent`, `parallel` / `fanOut`, `council`, `resumeSubagent`, `route` |
| Evaluation | `evaluateOutcome`, `evaluateTrace` |
| Durability and policy | `DurableRun`, `sealPolicySnapshot`, `sealGovernanceBinding`, `normalizeOpaDecision`, `normalizeCedarDecision` |
| Models and media | `shippedModelCatalog`, `resolveModelCatalog`, `modelCapabilityState`, `validateModelProfile`, `validateMediaInput`, `validateMediaArtifact` |
| Protocols | `McpConnection`, `A2aMapper`, external vendor-CLI agents (`spawnExternalAgentSession`, ...; see [External agents](../../docs/EXTERNAL-AGENTS.md)) |

Hover any of these in an editor for TSDoc with defaults, thrown errors, and examples.

## Canonical input and structured output

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

`generateObject` and `streamObject` also accept an async `options.validator`. It sees the raw
schema-valid JSON value before Zod parsing and resolves to `"accept"`,
`{ action: "retry", reason }`, or `{ action: "reject", reason }`. Retry uses `maxRetries`; thrown
errors fail closed as typed structured-output errors. Decision objects are exact: aliases,
unknown fields, conflicting keys, and a reason on `accept` are rejected.
The core rejects more than 32 repair retries and truncates normalized reasons to 1,024 bytes. It
does not add a timeout around the JavaScript callback; wrap slow or remote validation in an
application-owned timeout and keep the callback pure/idempotent.

## Deterministic outcome evaluation

Evaluate a recorded `RunOutcome` deterministically without another model or any tool/network work:

```js
const { evaluateOutcome } = require("./crates/aikit-node");
const verdict = evaluateOutcome(stream.outcome(), [
  { type: "terminal_status", status: "completed" },
  { type: "no_tool_errors" },
  { type: "max_total_tokens", value: 2_000 },
]);
if (!verdict.passed) throw new Error("evaluation gates failed");
```

This uses the same snake-case gate JSON contract as `aikit eval`. Unknown outcome or gate fields
fail closed, and verdict messages expose only lengths, counts, and states rather than raw output.
Text, tool, and turn gates require the runtime-recorded `invocation_start_message_index`, so old
conversation history cannot satisfy the current run; legacy outcomes can still use status/usage
gates.

## Runs, streams, and typed errors

`Agent.run()` and reusable `Client.query()` accept the same `RunOptions` (model fallbacks,
provider options, turn/token limits, budget, retry policy, and optional caller-owned
`routing: { profiles, request }`). Their `QueryStream` has
`cancel()`, `close()`, and `outcome()`. `close()` waits for Stop hooks, audit/session recording, and
driver shutdown. The JavaScript wrapper calls it automatically for `for await ... break` and for
an aborted `options.signal`; direct `next()` users should call `await stream.close()` in `finally`.
Async generation and terminal structured-stream failures reject with `AikitError`-shaped values
whose stable `code` and full redacted `info` envelope are safe to branch on.
Unknown top-level or nested option fields are rejected instead of silently falling back to a
default, so misspelled budget and retry controls cannot weaken a run unnoticed.
Provider-specific options use `compatibilityMode: "strict"` by default. `"warn"` and
`"best_effort"` are explicit opt-ins; both preserve `ProviderWarning` values as normal warning
deltas and on completed results/outcomes instead of silently dropping parameters.

## Governed A2A mapper

`A2aMapper` exposes the shared Rust mapper for owner-scoped contexts/tasks, idempotent messages,
task listing, cancellation decisions, snapshot, and restore. `A2aMapperState` is internal
persistence state—not an official A2A wire DTO—and consumers should not hard-code an older schema
number. The Node class does not start an A2A HTTP or gRPC listener; the bounded experimental wire
listener currently lives on the Rust host side. See the [A2A conformance
guide](../../docs/A2A-CONFORMANCE.md) for the exact tested boundary.

## MCP tool visibility

MCP connections can expose only exact approved tool names before registration:

```js
const { McpConnection } = require("./crates/aikit-node");
const server = await McpConnection.connectHttp(
  "https://mcp.example.com",
  "work",
  undefined,
  { allow: ["search", "read_file"], deny: ["read_file"] },
);
agent.registerMcp(server);
```

Matching is case-sensitive and `deny` always wins. Omitted `allow` keeps the backward-compatible
allow-all default; `allow: []` exposes nothing. Unknown fields, duplicate/empty names, and names
over 128 characters fail closed; each filter accepts at most 1,024 entries. Hidden tools are
neither advertised nor executable. Discovery and transport also fail closed on bounded page,
item, byte, cursor, and response limits instead of retaining unbounded server data: 128 pages,
10,000 incoming items, 8 MiB of serialized items, 4 KiB per cursor, 64 KiB cumulative cursors,
and 4 MiB per transport response/stdio line.

## Orchestration and production state

`agent.subtask(id, prompt, route, options)` builds the canonical child spec, and
`agent.parallel(specs, profiles, options)` is the ergonomic alias for the existing ordered
`fanOut` implementation. Existing `addTool`, `runSubagent`, and `fanOut` names remain supported.
Tool-specific failure hooks use `onPostToolFailure(callback, tool?)` and run before global
`onFailure` hooks.

The reviewed model catalog is available offline through `shippedModelCatalog()`. Use
`resolveModelCatalog(overrides)` for a separate hashed override layer, and
`modelCapabilityState(profile, capability)` to preserve the `supported` / `unsupported` /
`unknown` distinction. `validateModelProfile`, `validateMediaInput`, and
`validateMediaArtifact` fail before provider I/O.

Completed external authorization results are normalized by `normalizeOpaDecision` and
`normalizeCedarDecision`; partial/undefined OPA results and Cedar forbids or diagnostic errors fail
closed. `DurableRun` also provides `requestConfirmation`, `requestInput`, `requestOutputReview`,
and `requestEditRetry` as non-expiring compatibility helpers. Restart-safe typed approvals use
`requestTypedApproval`, `resolveApprovalAt`/`applyCommandAt`, and an explicit trusted `bigint`
timestamp; `expireApprovals` appends idempotent timeout denials. `sealPolicySnapshot` and
`DurableRun.withPolicySnapshot` pin a complete run-scoped governance binding before mutable work.
Use `sealGovernanceBinding` and `DurableRun.withGovernanceBinding` when tenant/agent scope must also
be pinned; typed approvals inherit the replay-validated binding.
Canonical messages may carry strict `{ type: "media_input", media }` blocks instead of legacy
source-only `media` blocks. Credential-free absolute HTTP(S) URLs round-trip as canonical
references, but provider dispatch rejects unresolved URL/artifact references until a trusted host
resolver verifies bytes, size, and SHA-256. Provider MIME matching is case-insensitive.

Production state is opt-in and backed by local files:

```js
agent.configureJsonlAudit("./aikit-audit.jsonl"); // metadata_only + fail_closed
agent.useMemoryFile("./aikit-memory.json", "tenant-a");
agent.useSessionFile("./aikit-sessions.json");
```

Normal run/resume calls fail closed when a persisted execution lease exists, even after expiry.
After confirming the prior worker is stopped and reconciling every possibly completed external
side effect, an operator may clear only the expired lease explicitly:

```js
const revision = agent.recoverExpiredSession("session-id", true);
// No model or tool ran. Retry or resume separately only when it is safe to do so.
```

Passing `false`, or targeting a missing, active, or malformed lease, throws without starting work.

Audit configuration opens and validates the JSONL target immediately. Payload capture becomes
`"full"` only when explicitly requested; `"best_effort"` is likewise an explicit alternative to
the default `"fail_closed"`. Memory is written only by `remember()`. File-backed sessions and
memory can be reopened by another Agent, including in a later process, but concurrent coordination
is process-local: these are not distributed stores or cross-process locking primitives.

## Built-in tools and containment

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

## Native distribution contract

The generated `aikit_node.node` binary is platform-specific and is never relabeled as portable.
The root `aikit-runtime` package contains the JavaScript/TypeScript surface and exact-version
optional dependencies on `aikit-runtime-{platform}` packages. CI builds, stages, packs, installs,
and loads each supported target independently. Local checkout builds still use the adjacent addon
created by `scripts/build-node.sh`; examples in this guide therefore import
`./crates/aikit-node`. Use `require("aikit-runtime")` only inside a locally assembled/installed
package-layout test or a future explicitly published package.
Linux artifacts target glibc 2.28 or newer; musl is not yet supported.
Normal examples use the deterministic mock provider and make no billable API call.

Python/Node host callbacks execute in the host process and are not covered by built-in Bash OS
containment. Apply separate process isolation when callback code is untrusted.

## Documentation

| Guide | Purpose |
|---|---|
| [Root README](../../README.md) | Project overview and multi-language quick start |
| [Node examples](../../examples/node/README.md) | Runnable keyless learning path and the live-provider example |
| [Architecture](../../docs/ARCHITECTURE.md) | Core ownership, run lifecycle, state, and trust boundaries |
| [Feature reference](../../docs/FEATURES.md) | Full capability and governance reference |
| [Threat model](../../docs/THREAT-MODEL.md) | Containment guarantees and exclusions |
| [Competitor parity](../../docs/PARITY-MATRIX.md) | Current evidence, gaps, and v1 gate |
| [0.3 migration](../../docs/MIGRATING-0.3.md) | Streaming, MCP naming, capability and durability changes |
| [Evaluation guide](../../docs/EVALUATIONS.md) | Dataset, gate, report, and CI contracts |
| [Conformance](../../examples/node/conformance.cjs) | Cross-language parity driver |

Cross-language parity:

```bash
./scripts/parity-check.sh
```

Licensed under MIT OR Apache-2.0; both license texts are included in the package.
