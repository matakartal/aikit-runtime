# Getting started

This walkthrough goes from a fresh clone to a governed tool call and a first real provider call.
Every command was run against this checkout; outputs shown are the actual `mock-1` output. Steps 1–4
need **no API key, no network call, and cost nothing**: they use the deterministic `mock-1`
provider that ships with the runtime. Only step 5 talks to a real model provider.

`aikit-runtime` is a source-first `v0.3.0-alpha.1` candidate. Nothing is published to crates.io,
PyPI, or npm yet, so cloning the repository is the supported installation path
([distribution guide](RELEASE.md)).

## 0. Prerequisites

| Tool | Version | Needed for |
|---|---|---|
| Rust (`cargo`) | 1.88+ (declared MSRV); CI uses 1.97.1 | Everything |
| C/C++ toolchain | Platform default | Python and Node bindings only |
| Python | 3.9+ | Python binding only (optional) |
| Node.js | 18.17+ | Node binding only (optional) |

The repository has no `rust-toolchain.toml`, so plain `cargo` uses your default toolchain. To match
CI exactly, prefix commands with `+1.97.1` (for example `cargo +1.97.1 run ...`) after
`rustup toolchain install 1.97.1`.

## 1. Clone and run the keyless quick start

```bash
git clone https://github.com/matakartal/aikit-runtime.git
cd aikit-runtime
cargo run -p aikit-runtime --locked --example quickstart
```

The first build compiles the whole dependency tree and takes a few minutes. Expected output:

```text
Araç sonucunu aldım; görevi tamamladım.
```

That line comes from `mock-1`, the in-process deterministic provider (it is Turkish for "I received
the tool result; I completed the task"). It proves the agent loop, canonical messages, and the Rust
facade work end to end. The source is
[`crates/aikit/examples/quickstart.rs`](../crates/aikit/examples/quickstart.rs).

## 2. Use the CLI without a key

The `aikit` CLI defaults to `mock-1`, so every command below is offline and free:

```bash
cargo run -p aikit-cli --locked -- run "Explain aikit in one sentence"
cargo run -p aikit-cli --locked -- providers
cargo run -p aikit-cli --locked -- doctor
```

- `run` prints the same deterministic `mock-1` answer as step 1.
- `providers` lists all eight supported providers as `inactive` until a key is present. It never
  prints credential values.
- `doctor` checks the workspace jail and probes the Bash containment backend. With no keys it
  reports `WARN credentials no provider keys found; mock-1 remains available`, which is expected.
  A containment `WARN` means built-in Bash would be denied before launch on this machine; see the
  [threat model](THREAT-MODEL.md).

Optional: build the binary once so you can call `aikit` directly instead of `cargo run ... --`:

```bash
cargo build -p aikit-cli --release --locked
./target/release/aikit --help
```

The full command, output-format, and exit-code contract is in the
[CLI guide](../crates/aikit-cli/README.md).

## 3. Run a deterministic evaluation

```bash
cargo run -p aikit-cli --locked -- eval evals/smoke.json
```

Expected first line: `PASS  keyless-smoke  9/9 cases`, followed by each case and gate. The process
exits `0` when every case passes and `4` when a case fails, so the same command works as a CI gate.
Add `--format json` for a machine-readable report.

`eval` refuses any non-mock model unless you pass `--allow-live`, so a dataset cannot spend money by
accident. Dataset format and every gate type are in the [evaluation guide](EVALUATIONS.md).

## 4. See governance stop a tool call

Governance runs immediately before a side effect: a denied tool never reaches its executor. Three
keyless ways to watch that happen:

**a. Through the CLI eval harness.** `--demo-tools` registers a side-effect-free probe tool;
`denied` also installs a deny rule:

```bash
cargo run -p aikit-cli --locked -- eval evals/governance-allowed.json --demo-tools allowed
cargo run -p aikit-cli --locked -- eval evals/governance-denied.json --demo-tools denied
```

Both pass. In the denied case the model's tool call is refused, the error goes back to the model as
a tool result, and the run still completes. (Without the matching `--demo-tools` flag the gates
fail with exit code `4` because no tool is advertised.)

**b. The governed built-in tool suite (Rust).** A workspace jail, built-in file tools, contained
Bash, and a permission engine together:

```bash
cargo run -p aikit-runtime-core --locked --example governed_tools
```

It writes and reads a file inside a temporary jail, shows `/etc/hosts` being denied as an escape,
and denies `rm -rf /` (including a tab-separated variant) by rule while allowing `ls -la`.

**c. Declarative policy and human approval (Rust).**

```bash
cargo run -p aikit-runtime-core --locked --example policy          # JSON policy -> allow/ask/deny
cargo run -p aikit-runtime-core --locked --example plan_mode       # approve the whole plan first
cargo run -p aikit-runtime-core --locked --example smart_approval  # risk-scored auto-approval
cargo run -p aikit-runtime-core --locked --example reliability     # ordering and use caps
```

Want the same flow from Python or Node? Build a binding (step 6) and run
[`examples/python/agent_governance.py`](../examples/python/agent_governance.py) or
[`examples/node/agent_governance.cjs`](../examples/node/agent_governance.cjs); both are keyless and
show a denied host tool returning an error result to the model instead of running.

## 5. Make your first real provider call

This step makes a network request and may be billed by the provider. The CLI reads provider keys
from the conventional environment variables:

| Provider | Environment variable | Model ids routed to it |
|---|---|---|
| Anthropic | `ANTHROPIC_API_KEY` | `claude-*` |
| OpenAI | `OPENAI_API_KEY` | `gpt-*`, `o1-*`, `o3-*`, `o4-*` |
| Google | `GEMINI_API_KEY` or `GOOGLE_API_KEY` | `gemini-*` |
| DeepSeek | `DEEPSEEK_API_KEY` | `deepseek-*` |
| xAI | `XAI_API_KEY` | `grok-*`, `xai:grok-*` |
| OpenRouter | `OPENROUTER_API_KEY` | `openrouter:*` |
| Groq | `GROQ_API_KEY` | `groq:*` |
| Mistral | `MISTRAL_API_KEY` | `mistral:*` |

Export one key, confirm it is detected, then select a model explicitly. The model id must be one
your account can currently use; check the provider's own model list.

```bash
export ANTHROPIC_API_KEY='...'                 # or any row above
cargo run -p aikit-cli --locked -- providers   # that provider now shows "active"
cargo run -p aikit-cli --locked -- run --model <model-id> "Say hello"
```

The CLI never switches to a live provider on its own: without `--model` it stays on `mock-1`.

If you pick a model whose provider has no key, the CLI stops before any network call with exit code
`3`:

```text
aikit: runtime error: no credential active for provider 'anthropic' — add a key first
```

Fix it by exporting that provider's variable from the table above, then re-run `providers`.

To gate a live model with the evaluation harness, pass `--allow-live`; aggregate case, input,
output-token, and wall-time caps still apply (see the [CLI guide](../crates/aikit-cli/README.md)).
Maintainer-grade live acceptance runs are described in [LIVE-SMOKE.md](LIVE-SMOKE.md).

## 6. Use Python or Node (optional)

Both bindings call the same Rust core, so `mock-1` works there too.

**Python** (from the repository root):

```bash
python3 -m venv .venv
.venv/bin/pip install --require-hashes -r requirements/maturin.txt
.venv/bin/maturin develop --manifest-path crates/aikit-py/Cargo.toml
.venv/bin/python examples/python/agent_governance.py
```

**Node.js**:

```bash
./scripts/build-node.sh
node examples/node/agent_governance.cjs
```

`build-node.sh` compiles the addon with `cargo` and copies it next to
`crates/aikit-node/index.js`; it does not need the network or `@napi-rs/cli`. Language-specific
APIs, tools, permissions, and streaming are documented in the
[Python README](../crates/aikit-py/README.md) and the [Node README](../crates/aikit-node/README.md).

## 7. Where to go next

| I want to... | Read |
|---|---|
| Understand how a request flows through the runtime | [Architecture](ARCHITECTURE.md) |
| See every capability and its limits | [Feature reference](FEATURES.md) |
| Write permission rules, hooks, plan mode, or approvals | [Governance and hooks](FEATURES.md#governance-and-hooks) |
| Know what built-in Bash/file containment does and does not guarantee | [Threat model](THREAT-MODEL.md) |
| Add deterministic CI gates for agent behavior | [Evaluation guide](EVALUATIONS.md) |
| Reuse an existing Codex, Claude, Gemini, or Grok CLI login | [External agents](EXTERNAL-AGENTS.md) |
| Upgrade code written against 0.2 | [0.3 migration guide](MIGRATING-0.3.md) |
| Check what is proven today versus pending | [Project status](PROJECT-STATUS.md) |
| Find any other document | [Documentation index](README.md) |

Stuck? See [SUPPORT.md](../SUPPORT.md). Report security issues privately via
[SECURITY.md](../SECURITY.md), never in a public issue.
