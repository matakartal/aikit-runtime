<!-- markdownlint-disable MD013 MD033 MD041 -->

<div align="center">

# aikit-runtime

<p><strong>One governed runtime for serious AI agents.</strong></p>

Build provider-aware agents in **Rust**, **Python**, and **TypeScript** without reimplementing
streaming, tools, policy, routing, budgets, audit, memory, or sessions in every language.

[![CI](https://github.com/matakartal/aikit-runtime/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/matakartal/aikit-runtime/actions/workflows/ci.yml)
[![Artifacts](https://github.com/matakartal/aikit-runtime/actions/workflows/release.yml/badge.svg)](https://github.com/matakartal/aikit-runtime/actions/workflows/release.yml)
[![Rust 1.88+](https://img.shields.io/badge/Rust-1.88%2B-000000?logo=rust)](Cargo.toml)
[![Python 3.9+](https://img.shields.io/badge/Python-3.9%2B-3776AB?logo=python&logoColor=white)](crates/aikit-py/pyproject.toml)
[![Node 18.17+](https://img.shields.io/badge/Node-18.17%2B-339933?logo=nodedotjs&logoColor=white)](crates/aikit-node/package.json)
[![License](https://img.shields.io/badge/License-MIT%20OR%20Apache--2.0-blue)](#license)

**One core. Three SDKs. Four native providers. Four compatible endpoints. One policy boundary.**

[Quick start](#quick-start) · [CLI](#command-line-interface) · [Architecture](#architecture) · [Governance](#governed-tool-execution) · [SDKs](#one-runtime-three-sdks) · [Documentation](#documentation)

</div>

---

`aikit-runtime` keeps correctness-sensitive agent behavior in one Rust core and exposes it through
thin native bindings. Anthropic, OpenAI, Google, and DeepSeek retain their native wire formats,
while your application gets one canonical runtime contract.

## Why aikit?

| One runtime | Provider-native | Governed execution |
|---|---|---|
| Rust owns the agent loop; Python and Node call the same implementation. | Provider-specific reasoning, tool calls, citations, usage, and metadata stay intact. | Every side effect passes schema validation, hooks, permissions, budgets, and audit. |
| **Typed developer experience** | **Agent primitives included** | **Production controls built in** |
| Streaming text, structured objects, multimodal messages, typed errors, and cancellation. | Tools, subagents, parallel fan-out, councils, memory, sessions, routing, and retries. | Fail-closed containment, metadata-safe audit, cost limits, scoped tools, and explicit approvals. |

## Architecture

```mermaid
flowchart TB
    subgraph Apps[Application code]
        R[Rust]
        P[Python]
        N[TypeScript / Node.js]
    end

    R --> F[Rust facade: aikit]
    P --> PY[PyO3 binding]
    N --> JS[napi binding]

    F --> CORE
    PY --> CORE
    JS --> CORE

    subgraph CORE[aikit-runtime-core]
        direction LR
        MSG[Canonical messages<br/>streaming + objects]
        LOOP[Agent loop<br/>tools + subagents]
        GOV[Governance<br/>hooks + permissions]
        OPS[Budgets + routing<br/>audit + state]
        MSG --> LOOP --> GOV --> OPS
    end

    CORE --> A[Anthropic<br/>Messages]
    CORE --> O[OpenAI<br/>Responses]
    CORE --> G[Google<br/>Gemini]
    CORE --> D[DeepSeek<br/>Chat Completions]
    CORE --> C[Compatible endpoints<br/>OpenRouter · Groq · Mistral · xAI]

    CORE --> HOST[Host callbacks]
    CORE --> BUILTIN[Jailed built-in tools]
    BUILTIN --> BOUNDARY[Seatbelt · Linux namespaces + seccomp<br/>Windows Job · hardened Docker]
```

The bindings translate host values and async callbacks at the edge. They do not duplicate policy,
provider translation, or orchestration logic.

## Quick start

The repository includes a deterministic provider, so you can exercise the complete runtime with
**no API key and no network call**.

```bash
git clone https://github.com/matakartal/aikit-runtime.git
cd aikit-runtime
cargo run -p aikit-runtime --example quickstart
```

Expected output:

```text
Araç sonucunu aldım; görevi tamamladım.
```

## Command-line interface

The source-first CLI makes the runtime usable without writing an application. Its default model
is the deterministic, offline `mock-1` provider:

```bash
cargo run -p aikit-cli -- run "Explain aikit in one sentence"
cargo run -p aikit-cli -- chat
cargo run -p aikit-cli -- providers
cargo run -p aikit-cli -- doctor
cargo run -p aikit-cli -- eval evals/smoke.json
```

Choose a configured provider explicitly only when you intend to make a network call:

```bash
XAI_API_KEY='...' cargo run -p aikit-cli -- run --model grok-4.5 "Say hello"
```

The CLI supports text, JSON, and JSONL output, stable exit codes, stdin prompts, canonical
multi-turn history, secret-safe provider discovery, containment diagnostics, deterministic eval
gates, and shell completion generation. See the [CLI guide](crates/aikit-cli/README.md) for the
full contract.

### Rust

```rust
use aikit::Agent;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let answer = Agent::new()
        .generate_text("Say hello in Turkish", "mock-1", 128)
        .await?;

    println!("{}", answer.text);
    Ok(())
}
```

Run it from this checkout:

```bash
cargo run -p aikit-runtime --example quickstart
```

### Python

Build the native binding once, then use the same core from Python:

```bash
python3 -m venv .venv
.venv/bin/pip install "maturin==1.14.1"
.venv/bin/maturin develop --manifest-path crates/aikit-py/Cargo.toml
```

```python
import asyncio
import aikit

async def main() -> None:
    agent = aikit.Agent.from_env({})
    answer = await agent.generate_text("Say hello in Turkish")
    print(answer["text"])

asyncio.run(main())
```

### TypeScript / Node.js

```bash
./scripts/build-node.sh
```

```js
const { Agent } = require("./crates/aikit-node");

async function main() {
  const agent = Agent.fromEnv({});
  const answer = await agent.generateText("Say hello in Turkish");
  console.log(answer.text);
}

main();
```

## Built for agent workloads

| Capability | What aikit provides |
|---|---|
| Streaming | Incremental text, reasoning, tool-call, tool-result, usage, and terminal events. |
| Structured output | JSON Schema plus async semantic validation, bounded repair, streaming attempts, Pydantic and Zod materialization. |
| Evaluation | Deterministic text, tool-trajectory, terminal-state, usage, turn, and model-attempt gates over the current invocation. |
| Canonical messages | Text, reasoning, tools, media, citations, usage, and provider-owned metadata without flattening. |
| Governance | Global allow / ask / deny, declarative JSON policy, async approval, lifecycle hooks, and authoritative denial. |
| Plan mode | Whole-approach HITL: the agent proposes a plan; a human approves, revises, or rejects before tools run. |
| Smart approval | Keyless risk scoring auto-allows low-risk `ask` calls and escalates the rest to a human. |
| Reliability rules | Declarative tool ordering, prerequisites, and use caps — separate from security permissions. |
| Off-prompt output | Large or sensitive tool results stored by reference so they do not fill the model context. |
| Guardrails | Deterministic secret/PII redaction, regex blocking, and fail-closed MCP safety-server interop. |
| Self-extension | Human-governed capability requests: the agent asks for a tool it lacks, a human decides, the grant is recorded — never a silent escalation. |
| Tool runtime | Host callbacks plus opt-in Read, Write, Edit, Glob, Grep, and contained Bash. |
| MCP | Bounded stdio and Streamable HTTP client with tools, resources, prompts, auth/session propagation, exact allow/deny visibility filters, and governed execution. |
| Routing | Explicit or automatic model selection from a caller-owned catalog with hard capability and cost gates. |
| Resilience | Typed failures, bounded retry, pre-stream fallback, cancellation, deadlines, and terminal outcomes. |
| Orchestration | Scoped subagents, ordered parallel fan-out, council synthesis, quorum, and shared budget accounting. |
| State | Namespaced memory, revisioned sessions, JSON/transactional SQLite persistence, canonical recording, and resume. |
| Compaction | Opt-in transcript bounding: keep the task anchor and recent tail within a token budget, preserving tool pairing. |
| Web and browser | HTTPS allowlisted fetch/search plus an existing-session WebDriver executor; browser registration requires an explicit assertion of caller-owned pre-request egress enforcement. |
| Observability | Typed audit lifecycle, metadata-only redaction by default, JSONL sinks, and an optional Rust OpenTelemetry bridge. |
| Evaluation | Strict JSON datasets and deterministic text/tool/status/usage gates with keyless CI verdicts. |

## Governed tool execution

Tool authorization happens immediately before the side effect—not in a prompt and not in a UI
convention.

```mermaid
flowchart TD
    START[Model emits tool call] --> SCHEMA[Compile and validate input schema]
    SCHEMA -->|invalid| REJECT[Return typed error]
    SCHEMA --> PRE[Run scoped PreTool hooks]
    PRE -->|block| REJECT
    PRE --> POLICY{Permission decision}
    POLICY -->|deny| DENY[Authoritative denial<br/>executor is never called]
    POLICY -->|ask| APPROVE{Host approval}
    POLICY -->|allow| VALIDATE[Revalidate rewritten input]
    APPROVE -->|deny| DENY
    APPROVE -->|allow / rewrite| VALIDATE
    VALIDATE --> EXEC[Execute exactly once]
    EXEC --> POST[PostTool or failure hooks]
    POST --> AUDIT[Record audit + usage + outcome]
    AUDIT --> CONTINUE[Return result to model]
    DENY --> CONTINUE
    REJECT --> CONTINUE
```

A denied tool never reaches its callback. Approval and hook rewrites are schema-validated again
before execution.

### Example: policy around a host tool

```python
agent = aikit.Agent.from_env({})

async def lookup(payload: dict) -> str:
    return f"price:{payload['symbol']}"

agent.add_tool(
    "lookup",
    "Look up one market symbol",
    {
        "type": "object",
        "properties": {"symbol": {"type": "string"}},
        "required": ["symbol"],
        "additionalProperties": False,
    },
    lookup,
)

agent.set_permissions([
    {"id": "approve-lookups", "effect": "ask", "tool": "lookup"},
])

async def approve(request: dict):
    return "allow" if request["input"]["symbol"] == "AAPL" else "deny"

agent.can_use_tool(approve)
```

The same lifecycle exists in Rust and TypeScript; only the host callback syntax changes.

### Declarative policy, plan mode, and reliability

Rust also ships higher-level governance primitives that compose with the same engine:

```rust
// Declarative JSON → enforcing PermissionEngine
use aikit_core::PolicySpec;
let engine = PolicySpec::from_json(r#"{
  "mode": "allow",
  "deny": ["Bash(rm -rf *)"],
  "ask": ["Bash(git push *)"],
  "allow": ["Read(*)"]
}"#)?.build()?;

// Plan mode: review the whole approach before any tool runs
// cargo run -p aikit-runtime-core --example plan_mode

// Smart approval: auto-allow Low risk, escalate the rest
// cargo run -p aikit-runtime-core --example smart_approval

// Reliability: deploy only after test, at most once
// cargo run -p aikit-runtime-core --example reliability
```

See the [feature reference](docs/FEATURES.md#governance-and-hooks) for plan review, risk scoring,
reliability rules, off-prompt tool output, and capability requests.

## Provider fidelity

`aikit` uses native adapters rather than forcing every provider through one lossy compatibility
shape.

| Provider | Native API | Reasoning continuation | Structured output | Vision |
|---|---|---|---|---:|
| Anthropic | Messages | Signed thinking replayed unchanged | Native JSON Schema constraint | Yes |
| OpenAI | Responses | Opaque reasoning item replayed unchanged | Strict JSON Schema | Yes |
| Google | Gemini `generateContent` | Thought signature retained on the exact function-call part | `responseJsonSchema` | Yes |
| DeepSeek | Chat Completions | Full `reasoning_content` retained for tool continuation | JSON mode + validation and repair | No |

OpenRouter, Groq, Mistral, and xAI are supported through isolated OpenAI-compatible endpoints.
They do not inherit native-provider fidelity claims automatically; capability checks still apply.

### Grok through xAI

Set `XAI_API_KEY`, then select Grok with either its natural model id or the explicit xAI namespace:

```python
agent = aikit.Agent.from_env({"XAI_API_KEY": "..."})
answer = await agent.generate_text("Hello from Grok", model="grok-4.5")
# Equivalent explicit form: model="xai:grok-4.5"
```

The adapter uses xAI's OpenAI-compatible Chat Completions endpoint. Model availability can change;
choose a model currently enabled for your xAI account.

Generated objects report their actual fidelity:

- `native_constrained`
- `forced_tool_call`
- `prompted_and_parsed`

Your application can branch on that grade instead of assuming every JSON-shaped answer has the
same guarantee.

After JSON Schema succeeds, an optional async semantic validator can return `Accept`,
`Retry(reason)`, or `Reject(reason)`. Retry uses the existing bounded repair budget; reject and
callback failures stop with a typed structured-output error. Validators must be pure/idempotent,
and aikit never copies the candidate object into validator errors or audit events automatically.
Retry reasons are provider-facing, so return a safe summary rather than the raw object. Reject and
callback-error details are returned to the host but persist only as generic audit failure markers;
keep those details secret-free too because an application may log exceptions.

## One runtime, three SDKs

| Concept | Rust | Python | TypeScript |
|---|---|---|---|
| Agent | `Agent::new()` | `aikit.Agent()` | `new Agent()` |
| Text | `generate_text` | `generate_text` | `generateText` |
| Streaming | `stream_text` | `stream_text` | `streamText` |
| Structured output | `generate_object` | `generate_object` | `generateObject` |
| Outcome evaluation | `evaluate_outcome` | `evaluate_outcome` | `evaluateOutcome` |
| Tool registration | `tool` / executor | `add_tool` | `addTool` |
| Permissions | `Governance` | `set_permissions` | `setPermissions` |
| Memory | `remember` / `recall` | `remember` / `recall` | `remember` / `recall` |
| Parallel agents | `parallel` | `parallel` | `parallel` |
| Persistent audit | `JsonlAuditSink` | `configure_jsonl_audit` | `configureJsonlAudit` |

Cross-language conformance runs canonical scenarios through all three SDKs and compares normalized
results byte for byte.

## Multimodal and structured input

All text and object surfaces accept a string or canonical message history:

```python
messages = [{
    "role": "user",
    "content": [
        {"type": "text", "text": "Describe this chart"},
        {
            "type": "media",
            "media_type": "image/png",
            "source": {"kind": "url", "url": "https://example.com/chart.png"},
        },
    ],
}]

result = await agent.generate_text(messages, model="your-model")
```

Unsupported media is rejected with a typed error instead of being silently dropped.

## Tools and containment

Built-in tools are explicit and scoped:

```python
agent.register_builtin_tools(["./workspace"])
agent.configure_jsonl_audit("./state/audit.jsonl")
agent.use_memory_file("./state/memory.json", namespace="project-a")
agent.use_session_file("./state/sessions.json")
```

| Surface | Boundary |
|---|---|
| Read / Write / Edit / Glob / Grep | Descriptor-relative jail, registered roots only, no symlink following. |
| Built-in Bash | Required containment: probed macOS Seatbelt, Linux namespaces+seccomp, Windows Job Objects, or hardened digest-pinned Docker. |
| Host callbacks | Your application process and your responsibility to isolate further when needed. |

If required containment is unavailable, built-in Bash is denied before process launch.

## Source module map

`aikit-runtime` is distributed from this GitHub repository. Public registry packages are not part
of the current project plan.

| Ecosystem | Workspace package | Import / library |
|---|---|---|
| Rust facade | `aikit-runtime` | `aikit` |
| Rust core | `aikit-runtime-core` | `aikit_core` |
| Python | `aikit-runtime` | `import aikit` |
| Node wrapper | `aikit-runtime` | local checkout wrapper |
| Node native binaries | `aikit-runtime-{platform}` | selected locally by the wrapper |

The current `v0.2.0` tree is a **source-first implementation preview**. Keyless source, binding,
and local package-layout checks run in CI. Use the checkout commands in [Quick start](#quick-start);
no npm, PyPI, or crates.io publication is claimed or planned at this stage.

### Supported binary targets

| Platform | Python ABI3 wheel | Node native package |
|---|---:|---:|
| Linux x64, glibc >= 2.28 | Yes | Yes |
| Linux ARM64, glibc >= 2.28 | Yes | Yes |
| macOS ARM64 | Yes | Yes |
| macOS x64 | Yes | Yes |
| Windows x64 | Yes | Yes |

## Verification

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-features --locked
./scripts/parity-check.sh
./scripts/release-check.sh --candidate
```

Normal GitHub CI verifies:

- Rust stable, Rust 1.88 MSRV, rustdoc, and doctests
- Rust / Python / Node canonical parity
- Node native packages on five target combinations
- package dry-runs and native-addon loading
- keyless provider wire contracts and fail-closed containment behavior

The manual source-distribution workflow builds the five Python ABI3 wheels, assembles the native
artifacts, records SHA-256 manifests, and produces GitHub provenance attestations without uploading
anything to a package registry.
Linux wheels and Node addons are built in digest-pinned `manylinux_2_28` containers rather than
inheriting the GitHub runner's newer glibc requirement.

## Repository map

```text
.
├── crates/
│   ├── aikit/          # ergonomic Rust facade (package: aikit-runtime)
│   ├── aikit-core/     # canonical runtime, providers, governance
│   ├── aikit-py/       # PyO3 binding + type stubs (import aikit)
│   ├── aikit-node/     # napi binding + TypeScript declarations
│   └── aikit-cli/      # source-first terminal interface
├── examples/
│   ├── python/         # governance, options, and conformance
│   └── node/           # governance, options, and conformance
├── docs/               # features, threat model, release evidence, roadmap
├── scripts/            # build, parity, packaging, and source checks
├── CONTRIBUTING.md     # setup and design rules
├── SECURITY.md         # private vulnerability reporting
└── CHANGELOG.md        # Keep a Changelog history
```

## Documentation

| Guide | Purpose |
|---|---|
| [Documentation index](docs/README.md) | Full map of guides, policies, and historical notes. |
| [Feature reference](docs/FEATURES.md) | Runtime capabilities, governance depth, fidelity, routing, state, and limits. |
| [Threat model](docs/THREAT-MODEL.md) | Security guarantees, containment boundaries, and exclusions. |
| [Distribution guide](docs/RELEASE.md) | Source-first distribution and manual artifact assembly. |
| [Live-provider harness](docs/LIVE-SMOKE.md) | Optional real-provider acceptance test contract. |
| [Completion matrix](docs/V1-COMPLETION-MATRIX.md) | Detailed v1 implementation coverage. |
| [Current project status](docs/PROJECT-STATUS.md) | What is complete, shareable, and intentionally source-only. |
| [Node binding](crates/aikit-node/README.md) | Local TypeScript / Node checkout usage. |
| [Python binding](crates/aikit-py/README.md) | Local Python checkout usage. |
| [CLI guide](crates/aikit-cli/README.md) | Commands, automation contracts, diagnostics, and completions. |
| [Evaluation guide](docs/EVALUATIONS.md) | Keyless datasets, deterministic gates, reports, and CI exit codes. |

## Contributing

Contributions are welcome. Start with [CONTRIBUTING.md](CONTRIBUTING.md), follow the
[Code of Conduct](CODE_OF_CONDUCT.md), and report vulnerabilities through the private process in
[SECURITY.md](SECURITY.md).

Questions and usage help belong in [GitHub Discussions](https://github.com/matakartal/aikit-runtime/discussions)
or the channels listed in [SUPPORT.md](SUPPORT.md).

## License

Licensed under either of the following, at your option:

- [Apache License, Version 2.0](LICENSE-APACHE)
- [MIT License](LICENSE-MIT)
