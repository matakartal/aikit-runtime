# Project status

**Snapshot date:** 2026-07-19

**Release state:** source-first `v0.2.0` development preview

This page records what can be demonstrated from source today.

## Ready today

- The Rust core and Rust, Python, and TypeScript/Node surfaces are implemented.
- Native Anthropic, OpenAI, Google, and DeepSeek adapters are covered by keyless wire-contract
  tests; OpenRouter, Groq, Mistral, and xAI use isolated compatible endpoints.
- Governance, tools, routing, budgets, sessions, memory, containment, audit, and orchestration are
  exercised without API keys through the deterministic mock provider.
- The last pushed main-branch revision passed CI and CodeQL. This uncommitted candidate passes the
  local keyless gates; remote GitHub Actions have not run against it yet.
- The repository is suitable to share publicly as an open-source implementation preview.
- The source-first CLI provides keyless runs, interactive chat, provider/capability discovery,
  containment diagnostics, automation output, and shell completions.

## Distribution boundaries

- Public registry packages are intentionally not distributed. GitHub source is the official path.
- No paid live-provider acceptance result is claimed for the current candidate.
- The existing `v0.1.0` evidence is a historical artifact snapshot, not a registry-release record.
- The Python FFI stack uses patched PyO3 0.29 and the repository lockfile passes `cargo audit`.

## Release decision

```mermaid
flowchart LR
    SRC[Current v0.2 candidate] --> LOCAL[Local keyless gates<br/>passing]
    LOCAL -. commit and push .-> CI[GitHub CI + CodeQL<br/>pending]
    CI --> SHARE[Public repository preview<br/>shareable]
    SHARE --> USE[Clone and build<br/>from source]
    SHARE --> ASM[Optional manual artifact<br/>assembly]
    SHARE -. optional .-> LIVE[Billable live-provider<br/>acceptance]
```

The candidate can be used from source locally. Before announcing this v0.2 source snapshot, commit
and push it, then require its GitHub CI and CodeQL runs to pass. It should not be described as
available through npm, PyPI, or crates.io.

See the [release guide](RELEASE.md), [completion matrix](V1-COMPLETION-MATRIX.md), and
[live-provider contract](LIVE-SMOKE.md) for the detailed checks.
