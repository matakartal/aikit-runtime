# Project status

**Snapshot date:** 2026-07-20

**Release state:** source-first `v0.2.0` development preview on `main`

This page separates what the repository proves locally and in keyless CI from evidence that still
requires external credentials, registries, or deployment authority. The live workflow badges and
GitHub Actions pages remain the authoritative source for the newest remote run result.

## Implemented and keylessly verifiable

- One canonical Rust core drives the Rust facade, Python/PyO3 binding, Node/napi binding, and CLI.
- Anthropic, OpenAI, Google, and DeepSeek use native adapters with provider-specific reasoning
  replay; OpenRouter, Groq, Mistral, and xAI use isolated compatible endpoints.
- Governance, tools, routing, budgets, sessions, memory, audit, containment, orchestration,
  structured output, and deterministic evaluations run without API keys through `mock-1`.
- Structured output supports async semantic `accept` / `retry` / `reject` validation after JSON
  Schema, with bounded repair and fail-closed callback handling.
- MCP stdio and Streamable HTTP clients support bounded tool/resource/prompt discovery, exact
  allow/deny tool visibility, governed execution, and call-boundary revalidation.
- Eval datasets produce deterministic text/tool/status/usage verdicts over the current invocation,
  with redacted reports and distinct input, infrastructure, and gate-failure exit codes.
- JSON and transactional SQLite session stores use revision checks and execution leases; expired
  leases require explicit side-effect reconciliation before recovery.
- Configured source-artifact targets are Linux x64/ARM64 with glibc 2.28+, macOS x64/ARM64, and
  Windows x64. A specific 0.2 assembly is proven only by its workflow/evidence record. The Windows
  file-tool jail remains intentionally unavailable and fails closed.

## Proof available in the repository

| Proof | Command or location | What it establishes |
|---|---|---|
| Rust workspace | `cargo +1.97.1 test --workspace --all-features --locked` | Core, facade, CLI, and binding-crate behavior. |
| MSRV | `cargo +1.88.0 check --workspace --all-targets --all-features --locked` | Declared Rust 1.88 compatibility. |
| Cross-language parity | `./scripts/parity-check.sh` | Seven canonical modules and transcripts agree across Rust/Python/Node. |
| Deterministic eval | `cargo +1.97.1 run -p aikit-cli --locked -- eval evals/smoke.json` | Keyless dataset parsing, execution, gates, and reporting. |
| Source candidate | `./scripts/release-check.sh --candidate` | Version alignment, immutable CI inputs, history/tag collision checks, and artifact policy. |
| Security automation | CodeQL, `cargo audit`, and Gitleaks | Static, dependency, and committed-secret review; not a formal security proof. |

## Distribution boundaries

- GitHub source is the official usage path. No npm, PyPI, or crates.io publication is claimed.
- The manual release workflow assembles temporary GitHub Actions artifacts; it does not publish to
  a registry and does not grant release authority.
- The current source version is `0.2.0`, but there is no committed `v0.2.0` evidence record or tag.
- [`releases/v0.1.0.md`](releases/v0.1.0.md) is a historical draft artifact snapshot for a different
  commit and must not be reused as proof for current `main`.
- No paid live-provider acceptance result is claimed for `v0.2.0`. Local real-socket wire tests do
  not prove that a changing provider currently accepts the same request.

## Intentionally deferred

- public registry publication and registry ownership/namespace work;
- MCP server mode, ACP/A2A, skills/plugin loading, and WASM packaging;
- durable checkpoint/time-travel replay, which requires an explicit external-side-effect model;
- model-generated compaction and distributed session/memory services;
- microVM containment, a caller-transparent browser egress proxy, and a Windows descriptor-relative
  file jail.

The current tree is suitable for source evaluation and integration with the documented boundaries.
It must not be described as a published package, a completed live-provider certification, or a
general sandbox for arbitrary host callbacks.

See the [architecture](ARCHITECTURE.md), [feature reference](FEATURES.md),
[implementation matrix](V1-COMPLETION-MATRIX.md), [release guide](RELEASE.md), and
[live-provider contract](LIVE-SMOKE.md).
