//! # aikit
//!
//! The native Rust surface of **aikit-runtime**: an agent-native, governed, provider-agnostic
//! AI runtime. This crate (package `aikit-runtime`, library `aikit`) is a thin facade that
//! re-exports [`aikit-runtime-core`](https://github.com/matakartal/aikit-runtime/tree/main/crates/aikit-core).
//! All behavior lives in the core; the Python (PyO3) and TypeScript (napi) bindings call the
//! same core, so provider translation, governance, routing, audit, and structured output are
//! implemented once and behave the same in every language.
//!
//! > **Status:** `v0.3.0-alpha.1`, a source-first implementation preview. Nothing is published
//! > to crates.io; depend on a checkout or the Git repository. The public API may still change
//! > before 1.0 — see the [migration notes](https://github.com/matakartal/aikit-runtime/blob/main/docs/MIGRATING-0.3.md)
//! > and [project status](https://github.com/matakartal/aikit-runtime/blob/main/docs/PROJECT-STATUS.md).
//!
//! ## Installation
//!
//! ```toml
//! [dependencies]
//! # The library name is `aikit`, so code imports `use aikit::...` regardless of the package name.
//! aikit-runtime = { git = "https://github.com/matakartal/aikit-runtime" }
//! tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
//! futures = "0.3" # only needed for consuming streams with `StreamExt`
//! ```
//!
//! The Minimum Supported Rust Version is **1.88**.
//!
//! ## Quick start (keyless)
//!
//! Model ids starting with `mock` route to the built-in deterministic [`MockProvider`], so the
//! snippets below run without credentials or network access. This is the same code path the
//! real providers use, which makes it the fastest way to try the API and to write tests.
//!
//! ```
//! use aikit::prelude::*;
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), AgentError> {
//! let answer = Agent::new()
//!     .generate_text("Say hello in Turkish", "mock-1", 128)
//!     .await?;
//! println!("{}", answer.text);
//! # assert!(!answer.text.is_empty());
//! # Ok(())
//! # }
//! ```
//!
//! The same program lives in `crates/aikit/examples/quickstart.rs`:
//! `cargo run -p aikit-runtime --example quickstart`.
//!
//! ### Using a real provider
//!
//! [`Agent::from_process_env`] activates every provider whose API key is present in the
//! environment (for example `ANTHROPIC_API_KEY`, `OPENAI_API_KEY`, `GEMINI_API_KEY` /
//! `GOOGLE_API_KEY`, `DEEPSEEK_API_KEY`). The model id prefix selects the provider (`claude-*`,
//! `gpt-*`/`o*`, `gemini-*`, `deepseek-*`, ...). Keys can also be added at runtime with
//! [`Agent::add_key`].
//!
//! ```no_run
//! use aikit::prelude::*;
//!
//! # async fn demo() -> Result<(), AgentError> {
//! let agent = Agent::from_process_env();
//! match agent.generate_text("Summarize RFC 9728 in one line", "claude-opus-4-8", 256).await {
//!     Ok(answer) => println!("{}", answer.text),
//!     // A missing key fails fast with a typed error instead of a network round-trip.
//!     Err(AgentError::NoCredential(provider)) => eprintln!("set an API key for `{provider}`"),
//!     Err(other) => {
//!         // `info()` is a stable, redacted classification shared with the language bindings.
//!         eprintln!("run failed: {other} ({:?})", other.info().code);
//!     }
//! }
//! # Ok(())
//! # }
//! ```
//!
//! ## Streaming
//!
//! Every run is a stream of canonical [`StreamDelta`] events (text, reasoning, tool calls, tool
//! results, usage, ...). The `generate_*` helpers simply drain that stream.
//!
//! ```
//! use aikit::prelude::*;
//! use futures::StreamExt;
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), AgentError> {
//! let agent = Agent::new();
//! let mut stream = Box::pin(agent.stream_text("hello", "mock-1", 64)?);
//! while let Some(delta) = stream.next().await {
//!     if let StreamDelta::TextDelta { text } = delta {
//!         print!("{text}");
//!     }
//! }
//! # Ok(())
//! # }
//! ```
//!
//! ## Structured output
//!
//! [`Agent::generate_object`] and [`Agent::stream_object`] validate model output against a JSON
//! Schema (with bounded repair retries) before returning it; [`ObjectStreamEvent`] exposes the
//! partial deltas and the final validated object. [`Agent::generate_object_typed`] derives the
//! schema from a Rust type implementing [`JsonSchema`] and `serde::Deserialize` (deriving
//! `JsonSchema` requires a direct `schemars = "1"` dependency).
//!
//! ```
//! use aikit::prelude::*;
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), AgentError> {
//! let schema = serde_json::json!({
//!     "type": "object",
//!     "required": ["status"],
//!     "properties": { "status": { "const": "ok" } }
//! });
//! let object = Agent::new()
//!     .generate_object("Return a status", schema, "mock-structured", ObjectOptions::default())
//!     .await?;
//! assert_eq!(object.value["status"], "ok");
//! # Ok(())
//! # }
//! ```
//!
//! ## Governed tool execution
//!
//! Tool calls never reach your [`ToolExecutor`] directly. Each one passes, in order, schema
//! validation, guardrails and pre-tool hooks, the deny/ask/allow [`PermissionEngine`] (with
//! optional human approval), reliability prerequisites, and post-tool hooks and audit. A matching
//! deny always wins over an earlier allow. The high-level [`Client`] / [`query`] layer takes its
//! policy through [`AgentOptions`]:
//!
//! ```
//! use aikit::prelude::*;
//! use futures::StreamExt;
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), AgentError> {
//! // Least privilege: deny every tool except the ones explicitly allowed.
//! let permissions =
//!     PermissionEngine::with_rules(PermissionMode::Deny, vec![Rule::allow("read_file")]);
//! let options = AgentOptions {
//!     model: "mock-1".into(),
//!     governance: Governance::new(permissions, HookDispatcher::default()),
//!     ..AgentOptions::default()
//! };
//!
//! let client = Client::new(Agent::new());
//! let mut stream = client.query("hello", options)?;
//! while let Some(delta) = stream.next().await {
//!     if let StreamDelta::TextDelta { text } = delta {
//!         print!("{text}");
//!     }
//! }
//! # Ok(())
//! # }
//! ```
//!
//! See the [governance] module, the
//! [architecture notes](https://github.com/matakartal/aikit-runtime/blob/main/docs/ARCHITECTURE.md#governance-and-tool-execution)
//! and the [threat model](https://github.com/matakartal/aikit-runtime/blob/main/docs/THREAT-MODEL.md)
//! for hooks, approvals, guardrails, containment, egress, and data-flow policy. Governance is a
//! policy boundary inside your process; it is not a compliance certification (see
//! [COMPLIANCE.md](https://github.com/matakartal/aikit-runtime/blob/main/docs/COMPLIANCE.md)).
//!
//! ## Durability and state
//!
//! Durable runs ([`durability`], [`DurableRunDriver`], [`DurableWorker`]) use an append-only
//! event log with checkpoint projections over a [`DurableStore`] ([`InMemoryDurableStore`],
//! [`SqliteDurableStore`], or the optional PostgreSQL store). Activities are classified by
//! [`SideEffectClass`]; an ambiguous external effect stops for operator reconciliation instead of
//! being silently retried. This is store compare-and-swap coordination, **not** a distributed
//! coordinator or an exactly-once guarantee. Sessions ([`session`]) and memory ([`memory`]) are
//! explicit: model output is only remembered when the host calls [`Agent::remember`]. Details:
//! [state and crash safety](https://github.com/matakartal/aikit-runtime/blob/main/docs/ARCHITECTURE.md#state-and-crash-safety).
//!
//! ## Module map
//!
//! Everything from `aikit-runtime-core` is re-exported at the crate root, so both
//! `aikit::Agent` and `aikit::agent::Agent` work. The most useful entry points:
//!
//! | Area | Start here |
//! |---|---|
//! | Agent loop, text and streams | [`Agent`], [`agent`], [`runtime`], [`StreamDelta`] |
//! | High-level options / client | [`Client`], [`AgentOptions`], [`query`], [`client`] |
//! | Structured output | [`Agent::generate_object`], [`dx`], [`ObjectOptions`] |
//! | Tools and MCP | [`tool`], [`ToolExecutor`], [`ToolRouter`], [`BuiltinTools`], [`mcp`] |
//! | Governance | [`governance`], [`Governance`], [`PermissionEngine`], [`HookDispatcher`] |
//! | Routing, budgets, retries | [`routing`], [`budget`], [`resilience`], [`catalog`] |
//! | Orchestration | [`Agent::subtask`], [`Agent::parallel`], [`orchestration`] |
//! | Durability, sessions, memory | [`durability`], [`durable_store`], [`session`], [`memory`], [`sqlite`] |
//! | Audit and tracing | [`observability`], [`AuditTrail`], [`TraceCollector`] |
//! | Evaluation | [`eval`], [`trace_eval`], [`EvalGate`] |
//! | Providers | [`providers`], [`Provider`], [`MockProvider`], [`credentials`] |
//! | External agents and protocols | [`external_agent`], [`protocols`] |
//! | Errors | [`AgentError`], [`AikitError`], [`ErrorInfo`], [`ErrorCode`] |
//!
//! [`prelude`] collects the types a small agent application usually needs.
//!
//! ## Cargo features
//!
//! Both features are off by default and forward to the matching `aikit-runtime-core` feature.
//!
//! | Feature | Enables |
//! |---|---|
//! | `opentelemetry` | `OpenTelemetryAuditSink`, exporting audit records through OpenTelemetry. |
//! | `postgres-store` | `PostgresDurableStore`, a PostgreSQL-backed [`DurableStore`] (row lock plus revision compare-and-swap). |
//!
//! ## More documentation
//!
//! - [Repository README](https://github.com/matakartal/aikit-runtime#readme) — CLI, SDKs, and verification
//! - [Feature inventory](https://github.com/matakartal/aikit-runtime/blob/main/docs/FEATURES.md)
//! - [Architecture](https://github.com/matakartal/aikit-runtime/blob/main/docs/ARCHITECTURE.md)
//! - Runnable examples: `crates/aikit/examples/` and `crates/aikit-core/examples/`
//!   (`cargo run -p aikit-runtime-core --example <name>`)

pub use aikit_core::*;

/// Common imports for a small agent application: `use aikit::prelude::*;`.
///
/// Includes the agent and client entry points, structured-output and evaluation types, tool
/// plumbing, the typed [`AgentError`], and the basic governance building blocks
/// ([`Governance`], [`PermissionEngine`], [`PermissionMode`], [`Rule`], [`HookDispatcher`]).
/// Everything else is available from the crate root.
pub mod prelude {
    pub use aikit_core::{
        tool, Agent, AgentError, AgentOptions, CancellableRun, CancellationHandle,
        CancellationToken, Client, ContentBlock, EvalCase, EvalDataset, EvalGate, EvalReport,
        EvalVerdict, GeneratedObject, GeneratedText, Governance, HookDispatcher, JsonSchema,
        MediaSource, Message, ObjectOptions, ObjectStream, ObjectStreamEvent, PermissionEngine,
        PermissionMode, RoutingOptions, Rule, SemanticValidation, SemanticValidator, StreamDelta,
        ToolExecutor, ToolSpec, TypedGeneratedObject,
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    #[tokio::test]
    async fn native_surface_runs_the_same_keyless_agent() {
        let generated = Agent::new()
            .generate_text("hello", "mock-1", 64)
            .await
            .unwrap();
        assert!(generated.text.contains("görevi tamamladım"));
    }

    #[tokio::test]
    async fn native_surface_exposes_incremental_structured_events() {
        let mut stream = Agent::new()
            .stream_object(
                "Return a status",
                serde_json::json!({
                    "type": "object",
                    "required": ["status"],
                    "properties": { "status": { "const": "ok" } }
                }),
                "mock-structured",
                ObjectOptions::default(),
            )
            .unwrap();
        let mut saw_delta = false;
        let mut completed = false;
        while let Some(event) = stream.next().await {
            match event.unwrap() {
                ObjectStreamEvent::Delta { .. } => saw_delta = true,
                ObjectStreamEvent::Completed { object } => {
                    assert_eq!(object.value["status"], "ok");
                    completed = true;
                }
                _ => {}
            }
        }
        assert!(saw_delta && completed);
    }

    #[test]
    fn native_surface_exposes_tool_and_subtask_ergonomics() {
        let spec = tool(
            "lookup",
            "Lookup one record",
            serde_json::json!({ "type": "object" }),
        );
        assert_eq!(spec.name, "lookup");

        let child = Agent::new().subtask(
            "child-1",
            "inspect",
            ModelRouteRequirements::explicit("mock-1"),
        );
        assert_eq!(child.id, "child-1");
        assert_eq!(child.prompt, "inspect");
    }
}
