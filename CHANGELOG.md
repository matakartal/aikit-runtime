# Changelog

All notable changes to this project will be documented in this file. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and published versions will follow
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Declarative permission policy (`PolicySpec`): JSON `mode` / allow / ask / deny rules with
  `Tool(glob)` patterns compile into the enforcing permission engine.
- Plan mode: agents propose a step plan; a host `PlanReviewer` approves, revises, or rejects
  before any tool executes.
- Risk scoring and smart approval: keyless `HeuristicRiskScorer` plus `SmartApprover` that
  auto-allows low-risk calls and escalates the rest to a human approver.
- Reliability rules: declarative tool ordering, prerequisites (`only_after`), use caps, and
  soft forbids — separate from security permissions.
- Off-prompt tool output: large tool results stored by reference with preview; full content
  retrieved through the canonical `retrieve_output` tool schema only when needed.
- Core examples: `policy`, `plan_mode`, `smart_approval`, and `reliability`.
- Strict policy parsing that rejects unknown fields and malformed rules, with multiline-safe glob
  matching to prevent newline-based policy bypasses.
- Isolated OpenAI-compatible endpoints for OpenRouter, Groq, Mistral, and xAI.
- GitHub CodeQL, Dependabot, community-health files, Discussions, and repository metadata.

### Changed

- Node native bindings now target N-API 3 for the declared Node.js compatibility floor.
- Updated Tokio, Regex, and SQLite dependencies; SQLite revisions are converted explicitly at the
  signed storage boundary.

### Documentation

- Expanded feature reference, documentation index, binding READMEs, and GitHub community templates
  to match the current governance surface and release-candidate status.

## [0.1.0] - Unpublished candidate

### Added

- A single Rust runtime with Rust, Python, and Node.js/TypeScript public surfaces.
- Native Anthropic Messages, OpenAI Responses, Google Gemini, and DeepSeek adapters with
  provider-owned reasoning replay rules.
- Governed tool execution with allow/ask/deny policies, enforcing lifecycle hooks, audit events,
  budgets, routing, subagents, sessions, and explicit memory.
- Sandboxed built-in file tools plus required macOS Seatbelt, Linux namespaces+seccomp, Windows
  Job Object, or hardened digest-pinned Docker containment for Bash.
- Typed text and structured-output APIs with explicit fidelity grades and multimodal input.
- Keyless cross-language conformance, package dry-runs, and an opt-in four-provider live-smoke
  contract.

### Release status

This is still an unpublished release candidate. No registry artifact or live-provider pass
is claimed. See [`docs/RELEASE.md`](docs/RELEASE.md) and the
[`v1 completion matrix`](docs/V1-COMPLETION-MATRIX.md) for the remaining external gates.
