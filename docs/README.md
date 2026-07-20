# Documentation

The root [`README`](../README.md) is the public overview and quick start. Everything under `docs/`
goes deeper: architecture, capabilities, security, migration, verification, release evidence, and
historical design notes. Current-contract documents are listed before archived material so an old
milestone cannot be mistaken for present behavior.

## Start here

| Document | Audience | Purpose |
|---|---|---|
| [Architecture](ARCHITECTURE.md) | Builders and reviewers | Component ownership, request lifecycle, state, and trust boundaries. |
| [Competitor parity matrix](PARITY-MATRIX.md) | Everyone | Exact upstream pins, local evidence, honest gaps, and v1 release gate. |
| [Feature reference](FEATURES.md) | Builders | Runtime capabilities, fidelity, governance, routing, state, and limits. |
| [Threat model](THREAT-MODEL.md) | Security reviewers | Guarantees and exclusions for built-in tools and Bash containment. |
| [Regulatory evidence aids](COMPLIANCE.md) | Risk and audit teams | Honest mapping from runtime evidence to assessor questions; explicitly not legal advice. |
| [Live-provider harness](LIVE-SMOKE.md) | Maintainers | Explicit, billable real-provider acceptance contract. |
| [Distribution guide](RELEASE.md) | Maintainers | Source-first policy and manual artifact assembly. |
| [0.3 migration guide](MIGRATING-0.3.md) | Integrators | Stream, MCP naming, capability and durability migration. |
| [0.2 migration guide](MIGRATING-0.2.md) | Integrators | Historical changes from the 0.1 source preview. |
| [Implementation matrix](V1-COMPLETION-MATRIX.md) | Contributors | Historical 0.2 inventory; parity status now lives in `PARITY-MATRIX.md`. |
| [Project status](PROJECT-STATUS.md) | Everyone | Current shareability and source-distribution boundaries. |
| [Evaluation guide](EVALUATIONS.md) | Builders and CI owners | Keyless datasets, deterministic gates, and reports. |

## Governance deep dive

The feature reference covers the full authorization path plus:

- declarative `PolicySpec` JSON;
- plan mode (whole-approach HITL before tools run);
- risk scoring and `SmartApprover`;
- reliability rules (predictable tool use, separate from security);
- off-prompt tool output (store large/sensitive results out of context);
- capability requests (human-governed tool grants).

Runnable demos:

```bash
cargo run -p aikit-runtime-core --example policy
cargo run -p aikit-runtime-core --example plan_mode
cargo run -p aikit-runtime-core --example smart_approval
cargo run -p aikit-runtime-core --example reliability
cargo run -p aikit-runtime --example quickstart
```

Cross-language governance examples live under `examples/python/` and `examples/node/`.

## Release and evidence

| Document | Purpose |
|---|---|
| [Distribution guide](RELEASE.md) | Source checkout, local artifacts, and non-publishing automation. |
| [Evidence template](RELEASE-EVIDENCE-TEMPLATE.md) | Blank per-version proof record (no secrets). |
| [Release evidence records](releases/README.md) | Index of committed version evidence. |
| [v0.1.0 evidence](releases/v0.1.0.md) | Historical draft artifact-assembly snapshot. |

## Historical and planning notes

These are **not** current status dashboards. Prefer `PROJECT-STATUS.md`, `FEATURES.md`, the
implementation matrix, and `CHANGELOG.md` for what the tree does today.

| Document | Role |
|---|---|
| [Phase 0 spike](PHASE-0-SPIKE.md) | Archived FFI architecture proof. |
| [Phase 1 progress](PHASE-1-PROGRESS.md) | Archived provider/reasoning milestone. |
| [Competitive roadmap](COMPETITIVE-ROADMAP.md) | Competitor-informed forward plan with a 2026-07-20 status addendum. |

## Project policies (repository root)

| File | Purpose |
|---|---|
| [`CONTRIBUTING.md`](../CONTRIBUTING.md) | Setup, design rules, PR expectations. |
| [`SECURITY.md`](../SECURITY.md) | Private vulnerability reporting. |
| [`CODE_OF_CONDUCT.md`](../CODE_OF_CONDUCT.md) | Collaboration norms. |
| [`CHANGELOG.md`](../CHANGELOG.md) | Keep a Changelog history. |
| [`SUPPORT.md`](../SUPPORT.md) | Usage help and support channels. |

## Binding packages

| Surface | Local docs |
|---|---|
| Node.js / TypeScript | [`crates/aikit-node/README.md`](../crates/aikit-node/README.md) |
| Python | [`crates/aikit-py/README.md`](../crates/aikit-py/README.md) |
| Rust facade | crate rustdoc via `cargo doc -p aikit-runtime --open` |
| Rust core | crate rustdoc via `cargo doc -p aikit-runtime-core --open` |

## Command line

| Surface | Local docs |
|---|---|
| `aikit` CLI | [`crates/aikit-cli/README.md`](../crates/aikit-cli/README.md) |

## Documentation maintenance

Use this ownership order when behavior changes:

1. `CHANGELOG.md` records user-visible additions, changes, and breaking migration points.
2. Root `README.md` stays the concise public overview and source quick start.
3. `FEATURES.md` owns the detailed capability contract; `THREAT-MODEL.md` owns security claims.
4. Binding/CLI README files own language-specific syntax and lifecycle details.
5. `PROJECT-STATUS.md`, `PARITY-MATRIX.md`, and release docs own proof boundaries; they must
   never turn a local/keyless check into a live-provider or registry-release claim.
6. Phase and evidence files are historical. Add a current banner or new record instead of rewriting
   old facts, hashes, or workflow URLs.

Before merging documentation changes, validate local links, balanced fenced code blocks,
`git diff --check`, current CLI help/output, package/toolchain version claims, and every example path.
External provider references should point to primary vendor documentation and be rechecked when the
corresponding adapter contract changes.
