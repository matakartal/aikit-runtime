# aikit

**One governed, provider-aware agent runtime in Rust, exposed to Rust, Python, and Node.js.**

`aikit` keeps the correctness-sensitive work in one Rust core: canonical messages, streaming,
tool execution, provider-specific reasoning replay, structured output, governance, budgets,
routing, audit, sessions, and memory. The Python and Node.js packages are thin native bindings
over that core.

> **Status: v1 implementation candidate, not a published production release.** The keyless suite
> verifies the core, real-socket mock transports, containment probes, and cross-binding parity.
> The environment-gated live smoke test exists, but no result is claimed until it is run with a
> real provider key and model. Registry publication is also still an external release step.
> The final distribution name is `aikit-runtime`; the Python import and Rust library name remain
> `aikit`. Multi-platform wheel/native-package assembly is verified by the release workflow, but
> no registry artifact or live-provider result is claimed until its external gate passes.

> **Distribution identity:** unrelated projects already use the bare `aikit` registry names.
> This project therefore uses the coordinated `aikit-runtime` distribution name and
> `aikit-runtime-core` internal Rust crate. Until the first release is published, run examples from
> this checkout rather than installing any similarly named third-party package.

## What is implemented

- Native HTTP adapters for Anthropic Messages, OpenAI Responses, Google Gemini, and DeepSeek.
- Provider-owned reasoning replay: signed Anthropic thinking, opaque OpenAI reasoning items,
  Gemini function-call signatures on their exact parts, and DeepSeek's tool-call continuation
  `reasoning_content`.
- `generate_text`, `stream_text`, and schema-validated `generate_object` surfaces in Rust,
  Python, and Node.js, accepting either a string or canonical message history with URL/base64
  media input.
- A high-level Rust `Client` plus one coherent `AgentOptions` object for fallback models, retries,
  provider options, governance, audit, budgets, tools, and run limits.
- Allow/ask/deny permissions, async human approval, and UserPrompt/PreTool/PostTool/
  PostToolFailure/Failure/Stop hooks enforced inside the tool loop.
- Built-in Read/Write/Edit/Glob/Grep/Bash tools, a descriptor-relative path jail, process limits, and
  fail-closed Seatbelt or hardened-Docker containment for built-in Bash.
- Typed audit events, metadata-only redaction by default, JSONL/in-memory core sinks, JSONL
  configuration in all three public languages, an optional Rust-host OpenTelemetry bridge,
  token/USD budgets, typed provider errors, safe retry/fallback, and a deterministic model router.
- Canonical run recording, optimistic-concurrency session stores, and explicit namespaced memory.
- Governed subagent orchestration with inherited governance, narrowed tool scope, shared budget
  accounting, and `subtask`/`parallel` convenience APIs.
- Host-language `tool` helpers, typed terminal errors, and normal-run automatic routing from an
  explicit caller-owned `{profiles, request}` configuration.

The mock provider is deterministic and keyless. It exercises the same core without pretending to
be live-provider proof.

## Provider fidelity matrix

| Provider | Native endpoint | Reasoning continuation | Structured output | Keyless wire proof |
|---|---|---|---|---|
| Anthropic | Messages | Replay signed thinking unchanged | `output_config.format` JSON schema (`native_constrained`) | Real socket + mock server |
| OpenAI | Responses | Replay the provider-owned opaque/encrypted item | Strict JSON schema (`native_constrained`) | Real socket + mock server |
| Google | Gemini `generateContent` | Replay a function-call thought signature on the exact part that carried it | `responseJsonSchema` (`native_constrained`) | Real socket + mock server |
| DeepSeek | Chat Completions | Replay full `reasoning_content` for thinking turns that called tools | JSON mode + validation/repair (`prompted_and_parsed`) | Real socket + mock server |

Fidelity is returned with every generated object. `aikit` does not label prompted JSON or a
generic forced-tool fallback as grammar-constrained decoding. Provider metadata and options remain
available as typed escape hatches when the canonical schema is not enough.

Provider metadata is raw output, not safe telemetry. Logprobs may contain generated tokens and
grounding metadata may contain prompt-derived searches, URLs, or citations. It is excluded from
metadata-only audit events, but returned results and persisted sessions can carry it; protect it
with the same confidentiality as prompts and model output.

## Quickstarts

The packages have not been published yet, so these examples run from a checkout.

### Rust

```bash
cargo run -p aikit-runtime --example quickstart
```

For a small complete response, use the lower-level `Agent` directly:

```rust
use aikit::Agent;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let result = Agent::new()
        .generate_text("Say hello in Turkish", "mock-1", 128)
        .await?;
    println!("{}", result.text);
    Ok(())
}
```

For policy-rich streaming, use the reusable high-level client:

```rust
use aikit::{AgentOptions, Client, StreamDelta};
use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::default(); // deterministic mock; use from_process_env() for live keys
    let mut options = AgentOptions::default();
    options.max_tokens = 128;

    let mut stream = client.query("Say hello in Turkish", options)?;
    while let Some(delta) = stream.next().await {
        if let StreamDelta::TextDelta { text } = delta {
            print!("{text}");
        }
    }
    Ok(())
}
```

`AgentOptions` also carries `fallback_models`, `retry`, provider-keyed `provider_options`,
`governance`, `audit`, `budget`, tools, routing, and turn limits. Free `aikit::query` is the
one-shot process-environment form; canonical-message variants preserve multimodal history, while
`query_with_executor` and `Client::query_with_executor` add host tools.

### Python

```bash
python3 -m venv .venv
.venv/bin/pip install maturin
.venv/bin/maturin develop --manifest-path crates/aikit-py/Cargo.toml
```

```python
import asyncio
import aikit

async def main():
    agent = aikit.Agent()
    messages = [{
        "role": "user",
        "content": [
            {"type": "text", "text": "Describe this image"},
            {"type": "media", "media_type": "image/png",
             "source": {"kind": "base64", "data": "aGVsbG8="}},
        ],
    }]
    result = await agent.generate_text(messages)
    print(result["text"])

asyncio.run(main())
```

Production-facing local state and built-ins are explicit rather than ambient:

```python
agent.configure_jsonl_audit("./state/audit.jsonl")  # metadata-only, fail-closed
agent.use_memory_file("./state/memory.json", namespace="workspace-a")
agent.use_session_file("./state/sessions.json")
agent.register_builtin_tools(["./workspace"])        # Read/Write/Edit/Glob/Grep; no Bash
```

`enable_bash_with_required_containment()` is a separate opt-in. It selects a probed macOS
Seatbelt backend or an explicitly configured digest-pinned Docker fallback and never exposes an
uncontained binding mode.

### Node.js / TypeScript runtime

```bash
./scripts/build-node.sh
```

This creates one addon for the current host. Published `aikit-runtime` installs use exact-version
optional packages for macOS ARM64/x64, Linux ARM64/x64 glibc, and Windows x64; the wrapper selects
the matching package and fails clearly on unsupported targets or omitted optional dependencies.

```js
const { Agent } = require("./crates/aikit-node");

async function main() {
  const agent = new Agent();
  const result = await agent.generateText([{
    role: "user",
    content: [
      { type: "text", text: "Describe this image" },
      { type: "media", media_type: "image/png",
        source: { kind: "base64", data: "aGVsbG8=" } },
    ],
  }]);
  console.log(result.text);
}

main();
```

The corresponding Node configuration is
`configureJsonlAudit(path)`, `useMemoryFile(path, namespace)`,
`useSessionFile(path)`, and `registerBuiltinTools([workspace])`. OpenTelemetry export remains a
Rust-host feature; the bindings expose the same protected JSONL audit contract rather than
installing a host-global exporter.

For governed host-language tools and streaming, see
[`examples/python/agent_governance.py`](examples/python/agent_governance.py) and
[`examples/node/agent_governance.cjs`](examples/node/agent_governance.cjs). The parity gate compares
canonical governance/hooks, structured streaming and repair, run options/errors, state/audit,
orchestration/session/deadline, built-in tools, and multimodal/routed input contracts through
Rust, Python, and Node.js.

## Live providers

`Agent` recognizes these environment variables:

| Provider | Key variable | Model used by the smoke test |
|---|---|---|
| Anthropic | `ANTHROPIC_API_KEY` | `AIKIT_SMOKE_ANTHROPIC_MODEL` |
| OpenAI | `OPENAI_API_KEY` | `AIKIT_SMOKE_OPENAI_MODEL` |
| DeepSeek | `DEEPSEEK_API_KEY` | `AIKIT_SMOKE_DEEPSEEK_MODEL` |
| Google | `GEMINI_API_KEY` or `GOOGLE_API_KEY` | `AIKIT_SMOKE_GOOGLE_MODEL` |

Normal tests never make billable calls. A configured-provider text probe requires the explicit
`AIKIT_LIVE_SMOKE=1` acknowledgement:

```bash
AIKIT_LIVE_SMOKE=1 ./scripts/live-smoke.sh
```

Release-level verification adds `AIKIT_LIVE_SMOKE_FULL=1`. It preflights all four key/model pairs
before the first call, then checks text, native/graded structured output, a governed denied forced
tool, and an allowed two-request tool/reasoning replay for every provider:

```bash
AIKIT_LIVE_SMOKE=1 AIKIT_LIVE_SMOKE_FULL=1 ./scripts/live-smoke.sh
```

The lighter mode attempts only complete configured key/model pairs. Both modes fail with no real
configuration instead of reporting fake success. **Neither mode has been run in this checkout; no
live-provider result is claimed.** See [`docs/LIVE-SMOKE.md`](docs/LIVE-SMOKE.md).

## Security boundary

Built-in Bash defaults to `Required(Auto)`: a probed macOS Seatbelt backend is preferred, with a
configured hardened Docker backend as fallback; otherwise execution is denied. Docker must use a
locally present digest-pinned image and its default seccomp profile. `Uncontained` is an explicit
core-only opt-out, not a safe default, and is not exposed by the Python/Node built-in APIs.

The descriptor-relative Read/Write/Edit/Glob/Grep jail supports Linux and macOS. Other platforms
fail closed for those tools; this release does not claim a native Windows file jail or job-object
sandbox. Linux uses the configured Docker fallback for required Bash containment.

This boundary does **not** contain arbitrary Rust executors or Python/Node callbacks, and it does
not defend against a kernel exploit, a compromised Docker daemon, or privileged host code. Read
the full [`threat model`](docs/THREAT-MODEL.md) before enabling model-generated commands.

## Verification

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
./scripts/parity-check.sh       # Rust + both built bindings
./scripts/release-check.sh --candidate
```

CI also checks the declared Rust MSRV, rustdoc, package dry-runs, Python/Node builds, and that the
live-smoke path stays opt-in when no secrets are present.

## Honest boundaries and post-v1 work

- Real-provider API acceptance is unknown until the live smoke is run; mock HTTP tests do not
  replace it.
- The collision-free distribution identity and multi-platform package layout are implemented.
  Publication still requires registry ownership/authentication, current live-provider evidence,
  a verified source revision, and signing/attestation evidence.
- Full MCP client support, a Web tool, distributed durable sessions, advanced compaction,
  LiteLLM/long-tail adapters, and WASM/browser support are post-v1 work.
- Native Linux namespace/seccomp launching and native Windows sandbox/job-object containment are
  also post-v1; the current required boundary is the documented Seatbelt/Docker selection.
- The model catalog and prices are caller-supplied snapshots. The core intentionally ships no
  stale price table or universal quality ranking.

More detail: [`docs/FEATURES.md`](docs/FEATURES.md), [`docs/THREAT-MODEL.md`](docs/THREAT-MODEL.md),
[`docs/RELEASE.md`](docs/RELEASE.md), and [`docs/README.md`](docs/README.md).

## Contributing and license

See [`CONTRIBUTING.md`](CONTRIBUTING.md), [`SECURITY.md`](SECURITY.md), and
[`CODE_OF_CONDUCT.md`](CODE_OF_CONDUCT.md). The project is dual-licensed under
[MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.
