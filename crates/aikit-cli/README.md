# aikit CLI

The CLI is the source-first terminal interface to the same governed Rust core used by the Rust,
Python, and TypeScript SDKs. It does not reimplement provider routing or agent behavior.

## Run from the repository

```bash
cargo +1.97.1 run -p aikit-cli --locked -- run "Explain this repository in one sentence"
```

`mock-1` is the default model, so the first run is deterministic, keyless, offline, and free.
Use `cargo +1.88.0` when explicitly checking the declared MSRV; normal development and CI use the
pinned 1.97.1 toolchain.

Global syntax:

```text
aikit [--format text|json|jsonl] [--quiet] <command>
```

`--quiet` suppresses non-result informational output. `--version` reports the workspace package
version; this source preview is `0.2.0`.

## Commands

### One-shot run

```bash
cargo run -p aikit-cli -- run "Say hello"
cargo run -p aikit-cli -- run --system "Be concise" "Explain Rust ownership"
printf 'Summarize stdin' | cargo run -p aikit-cli -- run --model mock-1
```

Use a configured provider explicitly:

```bash
export XAI_API_KEY='...'
cargo run -p aikit-cli -- run --model grok-4.5 "Hello from Grok"
```

No command prints credential values.

### Interactive chat

```bash
cargo run -p aikit-cli -- chat --model mock-1
```

Commands inside chat:

- `/help` — show chat commands;
- `/providers` — show provider activation without secrets;
- `/clear` — clear canonical history and retain the system instruction;
- `/exit` or `/quit` — end the session.

Chat continues with the canonical message transcript returned by the runtime, preserving the same
provider metadata and replay rules as the SDKs.

### Provider and capability discovery

```bash
cargo run -p aikit-cli -- providers
cargo run -p aikit-cli -- --format json capabilities
```

Supported provider names are Anthropic, OpenAI, Google, DeepSeek, xAI/Grok, OpenRouter, Groq, and
Mistral. An inactive provider means no non-empty conventional environment variable was found.

### Doctor

```bash
cargo run -p aikit-cli -- doctor --workspace .
cargo run -p aikit-cli -- --format json doctor
```

Doctor checks the workspace jail, reports active providers without values, and actively probes the
fail-closed Bash containment backend. A containment warning means file tools remain available but
Bash would be denied before launch.

### Deterministic evaluations

```bash
cargo run -p aikit-cli -- eval evals/smoke.json
cargo run -p aikit-cli -- --format json eval evals/smoke.json
```

Evaluation datasets are strict JSON and combine output, tool-trajectory, terminal-state, and usage
gates into a CI verdict. They default to `mock-1`; a non-mock model is rejected before provider
construction unless `--allow-live` explicitly acknowledges network use and possible cost. Live
runs also have default aggregate case, input-byte, requested output-token, and wall-time caps;
raising any cap requires an explicit CLI option. See the
[evaluation guide](../../docs/EVALUATIONS.md).

| Live-eval limit | Default | Override |
|---|---:|---|
| Cases | 8 | `--max-live-cases` |
| Prompt/system bytes | 524,288 | `--max-live-input-bytes` |
| Requested output tokens | 32,768 | `--max-live-output-tokens` |
| Total wall time | 600 seconds | `--max-live-wall-seconds` |

The dataset must be a regular file no larger than 4 MiB. Symlinks and special files are rejected.
JSON reports include the runtime/schema version, exact dataset SHA-256, cases, usage, attempts, and
redacted errors; they do not copy model output or provider metadata.

### Shell completions

```bash
cargo run -p aikit-cli -- completions bash > ~/.local/share/bash-completion/completions/aikit
cargo run -p aikit-cli -- completions zsh > ~/.zfunc/_aikit
cargo run -p aikit-cli -- completions fish > ~/.config/fish/completions/aikit.fish
```

## Output and exit-code contract

Global output modes:

- `--format text` — human-readable default;
- `--format json` — one pretty JSON document;
- `--format jsonl` — one compact JSON event per line, required for automated chat streams.

For scripts, select the format explicitly and branch on the process code rather than parsing human
text. JSONL is mandatory for automated chat because one session produces multiple events; JSON is
appropriate for one-shot commands and eval reports.

Stable process codes:

| Code | Meaning |
|---:|---|
| `0` | Success |
| `1` | Local I/O or serialization failure |
| `2` | Invalid input or incompatible CLI mode |
| `3` | Agent/provider/runtime failure, including evaluation infrastructure |
| `4` | Evaluation dataset ran but at least one case failed |

Real-provider commands can make billable network calls. Keyless checks and CI continue to use
`mock-1`; no live provider is contacted implicitly.
