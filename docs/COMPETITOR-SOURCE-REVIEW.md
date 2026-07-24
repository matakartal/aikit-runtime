# Competitor source review — 2026-07-24

This review records the source patterns considered for AIKit. Commit pins are immutable evidence,
not a claim that AIKit copied an upstream implementation. License compatibility and the existing
one-Rust-core architecture remain hard boundaries.

## Net result

AIKit's defensible position is a governed runtime whose Rust, Python, and Node surfaces share one
behavioral core. Provider count, UI breadth, and hosted distribution still trail the largest
frameworks. The highest-value work is therefore to connect AIKit's existing safety primitives to
real execution and protocol ingress, not to duplicate every competitor feature.

## Reviewed patterns

| Project and reviewed pin | Useful source pattern | AIKit decision |
|---|---|---|
| [Rig](https://github.com/0xPlaygrounds/rig/tree/87f3f5b77a3caeffa10d60225c41e386753bf05e) (MIT) | Serializable agent-run decisions separated from the I/O driver; capability-oriented provider clients. | Keep the shared Rust loop. Do not reproduce Rig's Rust-only generic API across bindings. |
| [Pydantic AI](https://github.com/pydantic/pydantic-ai/tree/61d751ec55f69804e765509b4e0a35b3cf2b7793) (MIT) | Durable engines wrap existing model/tool I/O boundaries; stable step identities are replay ABI. | Connect `RunState` and `DurableStore` to the real loop through a Sync driver. Do not promise exactly-once. |
| [LangGraph](https://github.com/langchain-ai/langgraph/tree/31f90df3e6b0268fa77fd2d118a917d420b84a68) (MIT) | Checkpoint saver SPI, pending-write recovery, replay/fork and conformance tests. | Preserve AIKit's append-only event authority and CAS; add real-loop crash tests before a graph scheduler. |
| [OpenAI Agents Python](https://github.com/openai/openai-agents-python/tree/34ab93536750dc3e245a07dfa465c599f1f5697e) and [JavaScript](https://github.com/openai/openai-agents-js/tree/d601be6dcea96236b8c5aa9a6f5b4196c070cfb3) (MIT) | Mature tracing, HITL and resumable run state in separately implemented SDKs. | Treat Rust-owned portable state as the differentiator and prove every new projection byte-for-byte. |
| [Agno](https://github.com/agno-agi/agno/tree/1e03b4ef350f7c2706abc553a208e88b3f1e81e1) (Apache-2.0) | AgentOS auth/RBAC and a real A2A router. | Keep AIKit's governed adapter; finish journal/timestamp/history coverage and authenticated deployment proof. |
| [Microsoft Agent Framework](https://github.com/microsoft/agent-framework/tree/0796af0c262df77ca7a8d48f907a5de90b1fca4a) (MIT) | Workflow checkpoints plus a real Durable Task worker and A2A executor. | A real Temporal/worker integration remains a release gate; the reference mapper is not enough. |
| [Google ADK](https://github.com/google/adk-python/tree/f71d9df9179a4d37a54051ffceb6dda5c821e4c4) (Apache-2.0) | A2A request-to-runner adapter, reverse-event rewind, eval service and deployment tooling. | Make transport a wire-to-governed-action adapter; never let it bypass runtime governance. |
| [Claude Agent SDK Python](https://github.com/anthropics/claude-agent-sdk-python/tree/e6e07f1c9b0542217e1cf4913e96b161a6bf92b2) (MIT) | Session-store adapter, transcript fork semantics and W3C trace propagation to child processes. | Consider a small Rust trace-context carrier after the current durable/A2A slice. |
| [Claude Agent SDK TypeScript](https://github.com/anthropics/claude-agent-sdk-typescript/tree/dc71e7c4868d6432d883111c425dc6ba7678a614) | Commercial Terms, not an open-source license at this pin. | Product/API observation only; no source reuse. |
| [A2A specification](https://github.com/a2aproject/A2A/tree/cfc9d34bc41e368827eb6446d31f912e44f795c5) and [Python SDK](https://github.com/a2aproject/a2a-python/tree/3e6fa6a41d64f0581202df214a0515a0b0194832) (Apache-2.0) | Auth-scoped filtering before counts/cursors, bounded pagination and explicit wire DTOs. | Preserve AIKit's governed JSON-RPC/SSE, artifact/direct-Message and protected-cancel subset; finish timestamps/history/artifact updates and production journal wiring without weakening content-bound idempotency. |

## Implemented from this review

- `DurableRunDriver` connects the real provider/tool loop to `RunState` and `DurableStore` in
  `Sync` mode. Activity start commits before I/O; outcome commits before the loop advances;
  completed work is reused; ambiguous unsafe effects require reconciliation.
- A2A task listing filters by authenticated subject and tenant before filters, totals, cursor
  validation, and pagination. Context and receipt identities are owner-scoped; versioned restore
  migrates pre-scoping alpha keys and rejects inconsistent relational state.
- Python and Node expose the same Rust `A2aMapper`; the conformance gate compares canonical A2A
  behavior across all three languages.
- The experimental Rust A2A listener adds bounded JSON-RPC/SSE, artifact and direct-Message
  projection, protected cancellation ingress, and a pinned official TCK raw/exact-waiver gate.

## Deliberately still open

1. Complete A2A timestamp/history/artifact-update DTOs, production journal wiring and authenticated
   deployment proof; retain raw TCK reports until the six pinned upstream false negatives are fixed.
2. Async/Exit durable modes, a real Temporal worker, distributed cancellation and crash injection
   inside the live provider/tool loop.
3. Live provider acceptance, signed cross-platform artifacts and registry publication.
