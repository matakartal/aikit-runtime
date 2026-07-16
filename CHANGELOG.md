# Changelog

All notable changes to this project will be documented in this file. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and published versions will follow
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- A single Rust runtime with Rust, Python, and Node.js/TypeScript public surfaces.
- Native Anthropic Messages, OpenAI Responses, Google Gemini, and DeepSeek adapters with
  provider-owned reasoning replay rules.
- Governed tool execution with allow/ask/deny policies, enforcing lifecycle hooks, audit events,
  budgets, routing, subagents, sessions, and explicit memory.
- Sandboxed built-in file tools plus required Seatbelt or hardened Docker containment for Bash.
- Typed text and structured-output APIs with explicit fidelity grades and multimodal input.
- Keyless cross-language conformance, package dry-runs, and an opt-in four-provider live-smoke
  contract.

### Release status

This is still an unpublished implementation candidate. No registry artifact or live-provider pass
is claimed. See [`docs/RELEASE.md`](docs/RELEASE.md) and the
[`v1 completion matrix`](docs/V1-COMPLETION-MATRIX.md) for the remaining external gates.
