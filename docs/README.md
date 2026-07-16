# Documentation

The root [`README`](../README.md) is the public overview and quick start. Everything under `docs/`
goes deeper: capabilities, security, release evidence, and historical design notes.

## Start here

| Document | Audience | Purpose |
|---|---|---|
| [Feature reference](FEATURES.md) | Builders | Runtime capabilities, fidelity, governance, routing, state, and limits. |
| [Threat model](THREAT-MODEL.md) | Security reviewers | Guarantees and exclusions for built-in tools and Bash containment. |
| [Live-provider harness](LIVE-SMOKE.md) | Maintainers | Explicit, billable real-provider acceptance contract. |
| [Release guide](RELEASE.md) | Maintainers | Package identities, publication order, and release gates. |
| [v1 completion matrix](V1-COMPLETION-MATRIX.md) | Contributors | Implementation proof vs external release blockers. |
| [Project status](PROJECT-STATUS.md) | Everyone | Current shareability and package-release gates. |

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
| [Release guide](RELEASE.md) | Checklist, registry identity, publication order. |
| [Evidence template](RELEASE-EVIDENCE-TEMPLATE.md) | Blank per-version proof record (no secrets). |
| [Release evidence records](releases/README.md) | Index of committed version evidence. |
| [v0.1.0 evidence](releases/v0.1.0.md) | Historical draft assembly snapshot; refresh required for the final tag commit. |

## Historical and planning notes

These are **not** current status dashboards. Prefer FEATURES, the completion matrix, and CHANGELOG
for what the tree does today.

| Document | Role |
|---|---|
| [Phase 0 spike](PHASE-0-SPIKE.md) | Archived FFI architecture proof. |
| [Phase 1 progress](PHASE-1-PROGRESS.md) | Archived provider/reasoning milestone. |
| [Competitive roadmap](COMPETITIVE-ROADMAP.md) | Competitor-informed forward plan; Phase 1 core and Phase 2 are implemented. |

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
