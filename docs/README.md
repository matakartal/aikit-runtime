# Documentation

**Current-contract review:** 2026-10-15

The root [`README`](../README.md) is the public overview. **New here? Start with
[Getting started](GETTING-STARTED.md)**: a verified clone → build → keyless run → governed tool →
first real provider call walkthrough.

Every page below is tagged with its kind so an old milestone is never mistaken for present
behavior:

- **Guide** — how to do something with the current tree.
- **Reference** — the current contract (what the code does and does not do).
- **Evidence** — status and proof boundaries at a dated snapshot; check the date.
- **Historical** — archived milestone or plan; not current behavior.

When a document and the code disagree, the code and its tests are authoritative.

## I want to...

### Get running

| I want to... | Read | Kind |
|---|---|---|
| Go from clone to a first keyless run, governed tool, and real provider call | [Getting started](GETTING-STARTED.md) | Guide |
| Use the `aikit` command line (run, chat, providers, doctor, eval, completions) | [CLI guide](../crates/aikit-cli/README.md) | Guide |
| Build and use the Python binding | [Python README](../crates/aikit-py/README.md) | Guide |
| Build and use the Node.js / TypeScript binding | [Node README](../crates/aikit-node/README.md) | Guide |
| Read Rust API docs | `cargo doc -p aikit-runtime --open` (facade) or `cargo doc -p aikit-runtime-core --open` (core) | Reference |
| Install from a source checkout and understand what is (not) published | [Distribution guide](RELEASE.md) | Guide |

### Build with the runtime

| I want to... | Read | Kind |
|---|---|---|
| Understand where behavior lives and how a request flows | [Architecture](ARCHITECTURE.md) | Reference |
| Look up a capability, its fidelity, and its limits | [Feature reference](FEATURES.md) | Reference |
| Know what each provider supports (reasoning, structured output, vision) | [Provider capabilities](FEATURES.md#provider-capabilities) | Reference |
| Write permission rules, hooks, plan mode, smart approval, or reliability rules | [Governance and hooks](FEATURES.md#governance-and-hooks) | Reference |
| Persist sessions and memory | [Sessions and memory](FEATURES.md#sessions-and-memory) | Reference |
| Set budgets, routing, retries, and fallbacks | [Budgets, routing, and resilience](FEATURES.md#budgets-routing-and-resilience) | Reference |
| Connect MCP servers, web fetch, a browser, or transcript compaction | [MCP, Web, Browser, and compaction](FEATURES.md#mcp-web-browser-and-compaction) | Reference |
| Use durable execution or the A2A / ACP / MCP protocol surfaces | [Durable execution and protocol interop](FEATURES.md#durable-execution-and-protocol-interop) | Reference |
| Emit audit events or OpenTelemetry spans | [Audit and OpenTelemetry](FEATURES.md#audit-and-opentelemetry) | Reference |
| Reuse an existing Codex, Claude, Gemini, or Grok CLI login; manage signed runtime updates | [External agents](EXTERNAL-AGENTS.md) | Guide |
| Add deterministic, keyless CI gates for agent behavior | [Evaluation guide](EVALUATIONS.md) | Guide |
| Upgrade code from 0.2 to the 0.3 alpha | [0.3 migration guide](MIGRATING-0.3.md) | Guide |
| Upgrade code from the 0.1 preview to 0.2 | [0.2 migration guide](MIGRATING-0.2.md) | Historical |

### Review security, risk, and compliance

| I want to... | Read | Kind |
|---|---|---|
| Know what built-in file tools and Bash containment guarantee, and what they exclude | [Threat model](THREAT-MODEL.md) | Reference |
| Map runtime evidence to assessor questions (explicitly not legal advice) | [Regulatory evidence aids](COMPLIANCE.md) | Reference |
| Report a vulnerability privately | [`SECURITY.md`](../SECURITY.md) | Policy |

### Check what is proven today

| I want to... | Read | Kind |
|---|---|---|
| See what is shareable today and what still needs credentials, signing, or registries | [Project status](PROJECT-STATUS.md) | Evidence |
| See v1 parity against pinned competitors, with honest gaps | [Competitor parity matrix](PARITY-MATRIX.md) | Evidence |
| See the official A2A TCK procedure, raw result, and waiver boundary | [A2A conformance](A2A-CONFORMANCE.md) | Evidence |
| Understand which competitor source patterns were adopted or excluded, and why | [Competitor source review](COMPETITOR-SOURCE-REVIEW.md) | Evidence |
| Run the explicit, billable real-provider acceptance harness | [Live-provider harness](LIVE-SMOKE.md) | Guide (maintainers) |

### Ship a release (maintainers)

| I want to... | Read | Kind |
|---|---|---|
| Run the keyless candidate gates and assemble artifacts by hand | [Distribution guide](RELEASE.md) | Guide |
| Record proof for a version (no secrets) | [Evidence template](RELEASE-EVIDENCE-TEMPLATE.md) | Template |
| Find committed per-version evidence | [Release evidence records](releases/README.md) · [v0.1.0](releases/v0.1.0.md) | Evidence (historical for v0.1.0) |

### Contribute

| I want to... | Read |
|---|---|
| Set up toolchains, run the checks CI runs, and open a PR | [`CONTRIBUTING.md`](../CONTRIBUTING.md) |
| Understand how the project is governed | [`GOVERNANCE.md`](../GOVERNANCE.md) |
| Get usage help | [`SUPPORT.md`](../SUPPORT.md) |
| Read collaboration norms | [`CODE_OF_CONDUCT.md`](../CODE_OF_CONDUCT.md) |
| See user-visible changes by version | [`CHANGELOG.md`](../CHANGELOG.md) |

## Runnable keyless demos

All of these use the deterministic `mock-1` provider or local fixtures: no API key, no network.

```bash
cargo run -p aikit-runtime --example quickstart                    # first agent run
cargo run -p aikit-cli -- eval evals/smoke.json                    # deterministic CI gate
cargo run -p aikit-runtime-core --example governed_tools           # jail + built-ins + permissions
cargo run -p aikit-runtime-core --example policy                   # declarative JSON policy
cargo run -p aikit-runtime-core --example plan_mode                # whole-plan human approval
cargo run -p aikit-runtime-core --example smart_approval           # risk-scored approval
cargo run -p aikit-runtime-core --example reliability              # tool ordering and caps
```

Cross-language governance examples live under [`examples/python/`](../examples/python/README.md) and
[`examples/node/`](../examples/node/README.md) (each with a guide and a keyless `quickstart`
entry point) and need a built binding first
([Getting started, step 6](GETTING-STARTED.md#6-use-python-or-node-optional)).

## Historical and planning notes

These are **not** current status. Prefer `PROJECT-STATUS.md`, `FEATURES.md`, `PARITY-MATRIX.md`,
and `CHANGELOG.md` for what the tree does today.

| Document | Role |
|---|---|
| [Implementation matrix](V1-COMPLETION-MATRIX.md) | 0.2 surface inventory; parity status now lives in `PARITY-MATRIX.md`. |
| [Phase 0 spike](PHASE-0-SPIKE.md) | Archived FFI architecture proof. |
| [Phase 1 progress](PHASE-1-PROGRESS.md) | Archived provider/reasoning milestone. |
| [Competitive roadmap](COMPETITIVE-ROADMAP.md) | Competitor-informed forward plan with a dated status addendum. |
| [0.2 migration guide](MIGRATING-0.2.md) | 0.1 → 0.2 changes at that milestone. |

## Documentation maintenance

Use this ownership order when behavior changes:

1. `CHANGELOG.md` records user-visible additions, changes, and breaking migration points.
2. Root `README.md` stays the concise public overview and source quick start;
   `GETTING-STARTED.md` owns the step-by-step first-run path and must only show commands that were
   actually run.
3. `FEATURES.md` owns the detailed capability contract; `THREAT-MODEL.md` owns security claims.
4. Binding/CLI README files own language-specific syntax and lifecycle details.
5. `PROJECT-STATUS.md`, `PARITY-MATRIX.md`, and release docs own proof boundaries; they must
   never turn a local/keyless check into a live-provider or registry-release claim. They also keep
   dedicated workflow success separate from the overall CI state.
6. Phase and evidence files are historical. Add a current banner or new record instead of rewriting
   old facts, hashes, or workflow URLs.

Before merging documentation changes, validate local links, balanced fenced code blocks,
`git diff --check`, current CLI help/output, package/toolchain version claims, and every example path.
For A2A, preserve both the raw official TCK report and the exact-set verified-waiver result; neither
may be rewritten into an unqualified certification claim.
External provider references should point to primary vendor documentation and be rechecked when the
corresponding adapter contract changes.
