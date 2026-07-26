# aikit CLI

The CLI is the source-first terminal interface to the same governed Rust core used by the Rust,
Python, and TypeScript SDKs. It does not reimplement provider routing or agent behavior.
It is an outbound operator surface; it does not start the experimental inbound A2A or MCP servers.

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
version; this source preview is `0.3.0-alpha.1`.

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

### Existing Codex, Gemini, Claude, and Grok CLI sessions

External coding-agent runtimes are separate from API-key providers: the vendor CLI owns its
account session, conversation, tool loop, and approval requests. AIKit does not read, copy, print,
or exchange the CLI's OAuth tokens.

```bash
# Read-only discovery: candidates are inspected, never executed.
cargo run -p aikit-cli -- agents discover

# Initialize an existing vendor session without issuing a model prompt.
cargo run -p aikit-cli -- agents status codex
cargo run -p aikit-cli -- agents status gemini --acknowledge-vendor-terms
cargo run -p aikit-cli -- agents status grok

# Start the vendor-owned login flow when required.
cargo run -p aikit-cli -- agents login codex

# Run through the CLI account rather than an API key.
cargo run -p aikit-cli -- agents run codex --mode workspace_write "Fix the focused test"
cargo run -p aikit-cli -- agents run gemini --acknowledge-vendor-terms "Explain this workspace"
cargo run -p aikit-cli -- agents run grok "Review the current diff"
```

`agents run` defaults to interactive approval. Automation must select `--approval deny` or
explicitly opt into `--approval allow-once`; multi-event automation uses `--format jsonl`.
Ctrl-C sends protocol cancellation before the managed child process is shut down.

Codex uses the official app-server protocol. Gemini and Grok use their native ACP stdio modes;
Grok is launched with its self-updater disabled so a session cannot change binary mid-run. Claude
CLI does not expose native ACP, so AIKit launches the official bridge through the exact package pin
`@agentclientprotocol/claude-agent-acp@0.61.0`. It passes the discovered Claude CLI as
`CLAUDE_CODE_EXECUTABLE` and masks inherited Anthropic API credentials so a PAYG key cannot silently
win over the logged-in subscription. Node.js 22+ and `npx` are required. An air-gapped or
preinstalled bridge can be supplied explicitly; `@latest` bridge arguments are rejected:

```bash
cargo run -p aikit-cli -- agents status claude --bridge /absolute/path/to/pinned-claude-acp
```

`agents models` never sends a paid probe. Codex can return its catalog directly; ACP runtimes may
publish their model selector only after `session/new`, so an initially empty list is not treated as
an authentication failure.

Google documents Gemini ACP as an IDE/developer-tool integration surface, while its CLI terms also
warn about third-party use of the OAuth-backed service. AIKit therefore never extracts the token
and requires `--acknowledge-vendor-terms` (or
`AIKIT_ACKNOWLEDGE_GEMINI_CLI_TERMS=true`) before launching Gemini. If that policy is unsuitable,
use the normal Google provider with `GEMINI_API_KEY` instead. See the linked official sources and
full boundary in the [external-agent guide](../../docs/EXTERNAL-AGENTS.md).

### Signed runtime updates

Update sources are opt-in and persisted per runtime instance. The public key is raw 32-byte
Ed25519 material encoded as base64; manifest and signature locations must be credential-free HTTPS
URLs without query strings. The example values below are placeholders, not an AIKit production
feed:

```bash
cargo run -p aikit-cli -- agents update configure gemini \
  --manifest-url https://updates.example.com/aikit/gemini/manifest.json \
  --signature-url https://updates.example.com/aikit/gemini/manifest.sig \
  --key-id stable-2026 \
  --public-key-base64 BASE64_RAW_ED25519_PUBLIC_KEY \
  --policy aikit-managed

cargo run -p aikit-cli -- agents update status gemini
cargo run -p aikit-cli -- agents update sync gemini --acknowledge-vendor-terms
cargo run -p aikit-cli -- agents update rollback gemini --acknowledge-vendor-terms
```

Policies are deliberately different:

| Policy | `sync` behavior |
|---|---|
| `check-only` | verifies the due manifest and reports a compatible version; never mutates the vendor install |
| `vendor-managed` | reports that the vendor/package-manager action is required; never invokes it implicitly |
| `aikit-managed` | streams, verifies, stages, health-checks, then atomically activates the signed artifact |

`sync` observes a 24-hour manifest TTL and 1h/6h/24h failure backoff; `--force` is the explicit
operator override. Only one process can update a runtime root at a time. An interrupted activation
is reconciled on the next sync, and a failed or rolled-back version is quarantined until a newer
release appears. Normal `agents status|login|models|run` commands prefer the active managed
artifact unless `--path` explicitly selects another executable. Use `--runtime-root`/`--root` for
a non-default store or `--instance` for separate accounts.

The CLI does not install a daemon and importing an SDK never starts network activity. Schedule
`aikit agents update sync KIND` with the host scheduler when unattended daily checks are desired.

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

`run`, `chat`, and `agents run` can make billable network calls. Agent status/login/model commands
and signed update checks can contact vendor or update services but do not issue a model prompt.
Discovery is process-free, and keyless CI stays offline. `mock-1` remains the default; no live
provider, external agent, or background updater is selected implicitly.
