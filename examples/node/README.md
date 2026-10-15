# Node examples

Runnable examples for the napi binding in [`crates/aikit-node`](../../crates/aikit-node/README.md).
They load the binding straight from this checkout; nothing is installed from npm.

All examples except `live_provider.cjs` are **keyless and deterministic**: they use the built-in
mock provider, ignore provider keys in your environment, and make no network requests.

## Prerequisites

- Node.js 18.17 or newer (the binding's `engines` range)
- A Rust toolchain, to compile the native addon

Build the addon once from the repository root (and again after changing Rust code):

```bash
./scripts/build-node.sh            # debug build; pass `release` for an optimized one
```

This writes `crates/aikit-node/aikit_node.node`. If an example prints "Could not load the aikit
Node binding", that file is missing or stale — rerun the build.

## Learning path

Run every command from the repository root.

| Example | What it shows | Key? | Run |
| --- | --- | --- | --- |
| [`quickstart.cjs`](quickstart.cjs) | Start here. `generateText`, `streamText`, a host tool under allow/deny permission rules, an `ask` rule resolved by `canUseTool`, `generateObject` with a JSON Schema, and `Agent.run` gated by `evaluateOutcome`. | No | `node examples/node/quickstart.cjs` |
| [`live_provider.cjs`](live_provider.cjs) | The same `generateText` / `streamText` calls against a real provider, with hints for common typed error codes. Without a key it explains what to set and exits without contacting anything. | Yes | `ANTHROPIC_API_KEY=... node examples/node/live_provider.cjs` |
| [`agent_governance.cjs`](agent_governance.cjs) | The broad tour: key-format provider activation, permission policies via `query`, async lifecycle hooks, memory, routing, subagents/fan-out/council, structured-output streaming and repair, approval interrupts. | No | `node examples/node/agent_governance.cjs` |
| [`run_options.cjs`](run_options.cjs) | `RunOptions` and `Client`: fallbacks, retry, turn and budget limits, fail-closed option validation, typed errors, `AbortSignal` cancellation, `close()`, and per-run routing. | No | `node examples/node/run_options.cjs` |
| [`conformance.cjs`](conformance.cjs) | Canonical public-surface facts used by the cross-language conformance check. A test fixture more than a tutorial. | No | `node examples/node/conformance.cjs` |

`live_provider.cjs` recognises `ANTHROPIC_API_KEY`, `OPENAI_API_KEY`, `GEMINI_API_KEY` /
`GOOGLE_API_KEY`, `DEEPSEEK_API_KEY`, `OPENROUTER_API_KEY`, `GROQ_API_KEY`, `MISTRAL_API_KEY`,
and `XAI_API_KEY`. Set `AIKIT_MODEL=<model id>` to choose the model; otherwise it picks a model
from the shipped catalog for a provider you have a key for. Each request is capped at 128 output
tokens, but it is a real, billed API call.

## Things that surprise people

- The mock provider's canned reply is a fixed Turkish sentence. That is expected, not a bug.
- `Agent.fromEnv({})` deliberately ignores your shell, so examples stay reproducible.
  `new Agent()` reads provider keys from `process.env`.
- The keyless tool demos tell the mock which tool to call through
  `providerOptions: { mock: { tool_name, tool_input } }`. A live model makes that choice itself;
  the permission rules, hooks, and approval callback behave the same either way.
- Unknown or misspelled option fields throw instead of being silently ignored, and failures carry
  a stable `error.code` (see `ErrorCode` in [`index.d.ts`](../../crates/aikit-node/index.d.ts)).

## Parity contract

`agent_governance.cjs`, `run_options.cjs`, and `conformance.cjs` mirror the scripts in
[`examples/python/`](../python/). CI runs all three, and
[`scripts/parity-check.sh`](../../scripts/parity-check.sh) compares their `*_JSON=` output lines
byte-for-byte with Python (and, for `PARITY_JSON` and the conformance modules, with the Rust
examples). Comment changes are safe; any change to their behaviour or printed facts must land in
every language together. `quickstart.cjs` and `live_provider.cjs` are not part of that contract.

For the full API, see the [binding README](../../crates/aikit-node/README.md) and the TypeScript
declarations in [`index.d.ts`](../../crates/aikit-node/index.d.ts).
