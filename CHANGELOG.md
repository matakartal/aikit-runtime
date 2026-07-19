# Changelog

All notable changes to this project will be documented in this file. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and tagged versions follow
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Breaking

- Rust `ObjectOptions` now includes `semantic_validator`. Struct-literal callers should add
  `semantic_validator: None` or use `..ObjectOptions::default()` when migrating to 0.2.
- Rust `RunOutcome` now records `invocation_start_message_index`. Struct-literal callers should
  set the boundary explicitly or use `..RunOutcome::default()`; legacy serialized outcomes load
  with no boundary and message-derived eval gates fail closed for them.
- `BrowserTools` and the Python/Node browser registration helpers now require an explicit external
  egress-enforcement assertion; post-navigation URL checks alone are no longer presented as an
  SSRF boundary.
- `OffPromptStore::store` is now fallible and returns `Result<String>` so OS-randomness and
  retention-limit failures cannot be hidden.
- Expired session execution leases are no longer acquired automatically. Recovery now requires an
  explicit `SessionStore::recover_expired_execution_lease` call after side-effect reconciliation.
- `SessionStore` execution-lease methods now exchange the opaque, core-exported
  `SessionExecutionLease` claim: acquire/recovery return it, while commit/release consume it. Custom
  stores that overrode the previous owner-string methods must update their signatures and persist
  the store-generated fencing token. The doc-hidden store-author API
  (`issue_for_store`, `fencing_token`, `expires_at_unix_ms`, and `into_session`) provides the
  complete construction, persistence, validation, and ownership-transfer path needed by those
  overrides.

### Added

- Async semantic structured-output validation across Rust, Python, and Node with explicit
  accept/retry/reject decisions, bounded repair attempts, and fail-closed callback handling.
- Exact, case-sensitive MCP tool visibility filters across Rust, Python, and Node; optional allow
  lists are applied before discovery caching, deny entries always win, and hidden tools cannot be
  advertised or executed.
- Deterministic evaluation datasets and gates over canonical outcomes, plus a keyless `aikit eval`
  command with redacted provenance reports, bounded live runs, hardened dataset loading, and
  distinct infrastructure/gate failure codes.
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
- Grok model discovery through `grok-*` and `xai:grok-*`, backed by `XAI_API_KEY` and a keyless
  xAI wire-contract regression test.
- Source-first `aikit` CLI with one-shot runs, canonical multi-turn chat, provider/capability
  discovery, containment doctor, text/JSON/JSONL output, stable exit codes, and shell completions.
- GitHub CodeQL, Dependabot, community-health files, Discussions, and repository metadata.

### Changed

- MCP discovery and transport now fail closed after bounded page, item, byte, cursor, or response
  limits; stale discovery caches are cleared after refresh failures and repeated cursors cannot
  loop indefinitely.
- Development manifests advance to `0.2.0`; release checks reject reusing an evidenced/tagged
  version for different source bytes.
- Node native bindings use napi-rs 3 with N-API 9 for the declared Node.js 18.17+ compatibility
  floor.
- Linux Python and Node artifacts now build against a digest-pinned glibc 2.28 baseline; workflow
  actions and build tools are immutable or exact-version pinned.
- Run options reject unknown top-level and nested fields, credentials reject blank values, and
  Node AbortSignal cancellation serializes native stream finalization.
- Native provider streams now enforce bounded frames, retained parser state, response lifetime,
  and complete terminal/tool-call invariants; the runtime independently caps retained run output
  and oversized custom-tool results.
- Session execution claims block automatic expired-lease takeover. Recovery is an explicit store
  operation that requires external-side-effect reconciliation before any replay or commit. Random
  per-claim fencing tokens prevent same-owner ABA, and Python/Node expose an assertion-gated atomic
  expired-lease clear that performs no model/tool work.
- Web fetches pin validated public DNS targets with proxy-free, per-hop redirect checks. Browser
  and built-in file-tool inputs, traversal, responses, and outputs have fail-closed size limits.
- SQLite and JSON session files verify descriptor identity on Unix and Windows; unsupported
  platforms fail closed when file identity cannot be proven.
- Browser tool registration now fails closed without an explicit caller assertion of external
  pre-request host/public-IP enforcement. Browser inputs and WebDriver replies are bounded, and
  WebDriver failure payloads are redacted. This changes the Rust, Python, and Node registration
  signatures.
- Updated Tokio, Regex, SQLite, and the Python FFI stack. PyO3, `pyo3-async-runtimes`, and
  `pythonize` now use the patched 0.29 line; SQLite revisions are converted explicitly at the
  signed storage boundary.

### Documentation

- Reworked the complete documentation set for the 0.2 source preview: architecture and migration
  guides, current status, SDK/CLI contracts, security boundaries, MCP/evaluation limits, release
  operations, historical labels, and repository navigation now share one consistent vocabulary.

## [0.1.0] - Source preview

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

This is a source-first open-source preview. No public package registry or live-provider pass is
claimed. See [`docs/RELEASE.md`](docs/RELEASE.md) for the distribution policy.
