//! # aikit-runtime-core
//!
//! The governed, provider-agnostic agent runtime **core**. Pure Rust, no FFI: the PyO3
//! (`aikit-py`) and napi (`aikit-node`) bindings sit on top of this crate, and the
//! `aikit-runtime` crate re-exports it as the Rust package applications depend on.
//! Correctness-critical logic (canonical schema, reasoning-state replay, the agent loop,
//! governance) lives here exactly once, so behaviour is identical across providers *and*
//! across languages.
//!
//! Status: `0.3.0-alpha.1`, source-first. Public APIs may still change between alpha releases.
//!
//! ## Where to start
//!
//! | You want to… | Start with |
//! |---|---|
//! | Run a prompt with whatever provider keys are in the environment | [`Agent::from_process_env`], [`Agent::generate_text`], [`Agent::stream_text`] |
//! | One options object for streaming runs (tools, governance, budget, retry, routing) | [`Client`], [`AgentOptions`], [`query`] |
//! | Typed / structured output | [`generate_object`], [`generate_object_typed`], [`stream_object`] |
//! | Expose your own tools to the model | [`ToolSpec`], [`ToolExecutor`], [`tool`], [`ToolRouter`] |
//! | Sandboxed file tools and opt-in Bash | [`BuiltinTools`], [`Sandbox`], [`BashPolicy`] |
//! | Authorize every tool call before it runs | [`Governance`], [`PermissionEngine`], [`PolicySpec`], [`ToolApprover`] |
//! | Redact secrets/PII or block inputs at the tool boundary | [`GuardedExecutor`], [`GuardrailChain`] |
//! | Drive the loop directly (custom provider, recorder, message history) | [`run_agent`], [`RunConfig`], [`Provider`] |
//! | Test without API keys or network | [`MockProvider`] (any model id starting with `mock`, e.g. `mock-1`) |
//!
//! ## Quick start (keyless)
//!
//! Model ids starting with `mock` resolve to the deterministic [`MockProvider`], so this runs
//! without credentials or network. Swap in a real model id once the matching key (for example
//! `ANTHROPIC_API_KEY` or `OPENAI_API_KEY`) is set in the environment.
//!
//! ```
//! use aikit_core::Agent;
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), aikit_core::AgentError> {
//! let agent = Agent::from_process_env();
//! let reply = agent.generate_text("hello", "mock-1", 64).await?;
//! println!("{}", reply.text);
//! # Ok(())
//! # }
//! ```
//!
//! ## Module map
//!
//! - **Entry points:** [`agent`] (credential-driven [`Agent`]), [`client`] ([`Client`],
//!   [`AgentOptions`], [`query`]), [`dx`] (structured output), [`runtime`] (the loop itself).
//! - **Canonical schema & contracts:** [`types`] ([`Message`], [`ContentBlock`], [`StreamDelta`]),
//!   [`contract`] (versioned [`StreamEvent`]s), [`streaming`], [`error`].
//! - **Providers & models:** [`providers`], [`credentials`], [`capabilities`], [`catalog`],
//!   [`routing`], [`resilience`] (retry/fallback), [`reasoning`] (reasoning-state replay).
//! - **Tools:** [`tools`] (executor boundary, built-in and web tools), [`mcp`] (MCP client).
//! - **Governance:** [`governance`] — permissions, policy, hooks, sandbox, containment,
//!   guardrails, plan review, capability requests, risk scoring, off-prompt outputs, egress.
//! - **Limits & context:** [`budget`], [`compaction`], [`cancellation`].
//! - **State & durability:** [`session`], [`memory`], [`sqlite`], [`durability`],
//!   [`durable_store`], [`durable_runtime`], [`durable_worker`], [`temporal_adapter`]
//!   (and `postgres_store` behind the `postgres-store` feature).
//! - **Multi-agent & protocols:** [`orchestration`], [`external_agent`], [`protocols`].
//! - **Media:** [`multimodal`], [`media_runtime`], [`provider_media`].
//! - **Observability & evaluation:** [`observability`] (audit/trace; an OpenTelemetry sink
//!   behind the `opentelemetry` feature), [`eval`], [`trace_eval`], [`provider_validation`].
//!
//! ## Runnable examples
//!
//! Each example below is keyless and deterministic:
//! `cargo run -p aikit-runtime-core --example <name>`.
//!
//! - `governed_tools` — sandboxed file tools, permission rules, scrubbed Bash environment.
//! - `guardrails` — redacting secrets/PII from tool output, blocking a destructive input.
//! - `plan_mode` — a human reviews (and revises) the agent's plan before any tool runs.
//! - `capability_request` — the agent asks a human to unlock a gated tool.
//! - `defense_in_depth` — the decision and execution layers composed.
//! - `policy`, `smart_approval`, `reliability`, `structured_output` — smaller focused demos.

pub mod agent;
pub mod budget;
pub mod cancellation;
pub mod capabilities;
pub mod catalog;
pub mod client;
pub mod compaction;
pub mod contract;
pub mod credentials;
pub mod durability;
pub mod durable_runtime;
pub mod durable_store;
pub mod durable_worker;
pub mod dx;
pub mod error;
pub mod eval;
pub mod external_agent;
pub mod governance;
pub mod mcp;
pub mod media_runtime;
pub mod memory;
pub mod multimodal;
pub mod observability;
pub mod orchestration;
#[cfg(feature = "postgres-store")]
pub mod postgres_store;
pub mod protocols;
pub mod provider_media;
pub mod provider_validation;
pub mod providers;
pub mod reasoning;
pub mod resilience;
pub mod routing;
pub mod runtime;
pub mod session;
pub mod sqlite;
pub mod streaming;
pub mod temporal_adapter;
pub mod tools;
pub mod trace_eval;
pub mod types;

pub use agent::{
    Agent, AgentCapabilities, AgentCompactionSummarizer, AgentError, GeneratedText,
    ProviderCapabilityView,
};
pub use budget::{
    BillingDisposition, BudgetLedger, BudgetLedgerError, BudgetLedgerResult, BudgetLedgerSnapshot,
    BudgetLimits, BudgetPolicy, BudgetReservation, BudgetSnapshot, BudgetTracker, ModelPricing,
};
pub use cancellation::{CancellationHandle, CancellationToken};
pub use capabilities::{
    Capabilities, CapabilityRegistry, FidelityGrade, StructuredOutputCapabilities,
};
pub use catalog::{
    CatalogSource, ModelCatalogError, ModelCatalogOverrides, ModelCatalogSnapshot,
    ResolvedModelCatalog, MODEL_CATALOG_SCHEMA_VERSION, SHIPPED_MODEL_CATALOG_HASH,
    SHIPPED_MODEL_CATALOG_JSON, SHIPPED_MODEL_CATALOG_VERSION,
};
pub use client::{
    query, query_cancellable, query_messages, query_messages_cancellable,
    query_messages_with_executor, query_with_executor, AgentOptions, CancellableRun, Client,
    DeltaStream, RoutingOptions,
};
pub use compaction::{
    compact_messages, compact_messages_with_summarizer, estimate_tokens, extractive_digest,
    CompactionPolicy, CompactionSummarizer, COMPACTION_SUMMARY_MAX_CHARS,
};
pub use contract::{
    CapabilityState, CompatibilityMode, MediaInput, MediaInputSource, OutputPart, ProviderWarning,
    StreamBlockKind, StreamEvent, StreamEventKind,
};
pub use credentials::{resolve_provider, KeyGuess, ResolveError};
pub use durability::{
    stable_id, stable_input_hash, ActivityAttempt, ActivityAttemptStatus, ActivityDecision,
    ActivityDefinition, ActivityReconciliation, ActivityRecord, AppendOutcome, ApprovalResolution,
    ArtifactMetadata, Checkpoint, CommandOutcome, DurabilityError, DurabilityMode,
    DurabilityResult, DurableApproval, DurableApprovalKind, DurableApprovalRequest,
    DurableApprovalStatus, DurableRunStatus, RunCommand, RunEvent, RunEventKind, RunProjection,
    RunState, SideEffectClass, DURABILITY_SCHEMA_VERSION,
};
pub use durable_runtime::{
    DurableActivity, DurablePayloadPolicy, DurableRunDriver, DurableRunDriverError,
    MAX_DURABLE_COMPLETION_BYTES,
};
pub use durable_store::{
    DurableStore, DurableStoreError, DurableStoreResult, InMemoryDurableStore,
};
pub use durable_worker::{
    DurableWorker, DurableWorkerConfig, DurableWorkerError, DurableWorkerOutcome,
};
pub use dx::{
    generate_object, generate_object_messages, generate_object_messages_observed,
    generate_object_observed, generate_object_typed, generate_object_typed_messages, stream_object,
    stream_object_messages, stream_object_messages_observed, stream_object_observed,
    GeneratedObject, ObjectOptions, ObjectStream, ObjectStreamEvent, SemanticValidation,
    SemanticValidator, TypedGeneratedObject,
};
pub use error::{AikitError, ErrorCode, ErrorInfo, ProviderError, ProviderErrorKind, Result};
pub use eval::{
    evaluate_outcome, EvalCase, EvalCaseReport, EvalCheck, EvalDataset, EvalGate, EvalReport,
    EvalVerdict, EVAL_SCHEMA_VERSION,
};
pub use external_agent::*;
pub use governance::capability::{
    request_capability_tool, CapabilityBroker, CapabilityDecision, CapabilityGate,
    REQUEST_CAPABILITY_TOOL,
};
pub use governance::containment::{
    containment_capabilities, firecracker_capability, ActiveContainmentBackend, BackendCapability,
    BackendSelector, ContainmentCapabilityReport, ContainmentGuarantees, ContainmentPolicy,
    ContainmentRequirement, DockerConfig, FirecrackerConfig, FirecrackerError,
    FirecrackerLaunchPlan, FirecrackerNetwork, FirecrackerResult, FirecrackerStaging,
    FirecrackerVm, ImmutableHostFile,
};
pub use governance::contracts::{
    AgentDefinition, ApprovalCheckContext, ApprovalDenyReason, ApprovalEvidence,
    ApprovalEvidenceDecision, ApprovalEvidenceOutcome, ApprovalScope, DataFlowDecision,
    DataFlowPolicy, DataLabel, DataSink, DataSinkKind, DataSourceKind, EgressDecision,
    EgressPolicy, FilesystemProfile, FlowEffect, GovernanceBinding, GovernanceContractError,
    PolicyDocument, PolicyEffect, PolicyEvaluationContext, PolicyScope, PolicySnapshot, Provenance,
    SandboxProfile, ScopedPolicyDecision, ScopedPolicyRule, SkillManifest, SourceToSinkRule,
    ToolDescriptor, GOVERNANCE_CONTRACT_VERSION,
};
pub use governance::egress_broker::{
    BrowserProxyAssertion, EgressBroker, EgressBrokerBuilder, EgressBrokerError, EgressDnsResolver,
    EgressRequest, EgressResponse, EgressScheme, SystemDnsResolver,
};
pub use governance::guardrail::{
    GuardedExecutor, Guardrail, GuardrailChain, GuardrailVerdict, McpGuardrail, PiiRedactor,
    RegexBlocklist, SecretRedactor,
};
pub use governance::hooks::{
    FailureContext, FailureHookOutcome, FailureStage, HookDispatcher, HookMatcher, HookOutcome,
    PostToolOutcome, PostToolUseContext, PreToolUseContext, PromptContext, PromptHookOutcome,
    StopContext,
};
pub use governance::off_prompt::{
    retrieve_output_tool, OffPromptExecutor, OffPromptStore, RETRIEVE_TOOL,
};
pub use governance::permissions::{
    Outcome, PermissionDecision, PermissionEngine, PermissionMode, Rule, RuleEffect,
};
pub use governance::plan::{review_plan, Plan, PlanOutcome, PlanReview, PlanReviewer, PlanStep};
pub use governance::policy::{PolicyMode, PolicySpec};
pub use governance::policy_adapters::*;
pub use governance::process::{run_bash_with_containment, BashPolicy};
pub use governance::reliability::{
    ReliabilityPolicy, ReliabilityVerdict, RunProgress, ToolRequirement,
};
pub use governance::risk::{HeuristicRiskScorer, RiskLevel, RiskScorer, SmartApprover};
pub use governance::sandbox::{Sandbox, SandboxError};
pub use governance::skills::{
    authorize_executable_skill, ExecutableSkillGrant, LoadedSkill, SkillExecutionMode,
    SkillInspectionPolicy, SkillLoadError, SkillLoader, SkillPackage, SkillSourcePin,
    MAX_SKILL_FILE_BYTES, MAX_SKILL_TOTAL_BYTES,
};
pub use governance::{
    ApprovalDecision, ApprovalRequest, Authorization, AuthorizationContext, AuthorizationReport,
    DurableApproverError, DurableToolApprover, Governance, PermissionUpdate, ToolApprover,
};
pub use mcp::{
    McpClient, McpInputHandler, McpPrompt, McpProtocolVersion, McpRequestMeta, McpResource,
    McpRpcError, McpToolExecutor, McpToolFilter, McpTransport, StdioTransport,
    StreamableHttpTransport, MAX_MCP_INPUT_ROUNDS, MAX_MCP_TOOL_FILTER_NAMES,
    MAX_MCP_TOOL_NAME_CHARS, MCP_PROTOCOL_VERSION, MCP_PROTOCOL_VERSION_2026,
};
pub use media_runtime::*;
pub use memory::{
    InMemoryMemoryStore, JsonFileMemoryStore, MemoryEntry, MemoryPlane, MemoryProvenance,
    MemoryQuery, MemoryStore,
};
pub use multimodal::{
    GeneratedAudio, GeneratedImage, MediaArtifact, ModalityRequirement, RealtimeEvent,
    RealtimeEventKind, RealtimeSession, RealtimeSessionState, Transcript, TranscriptSegment,
    VoiceActivityPolicy, MAX_REALTIME_DEDUPE_EVENTS,
};
#[cfg(feature = "opentelemetry")]
pub use observability::OpenTelemetryAuditSink;
pub use observability::{
    AuditEvent, AuditFailureMode, AuditPayloadPolicy, AuditRecord, AuditSink, AuditTrail,
    InMemoryAuditSink, JsonlAuditSink, TelemetryPolicy, TraceCollector, TraceSpan, TraceSpanKind,
    TraceSpanStatus,
};
pub use orchestration::{
    CouncilResult, CouncilStatus, ExecutionContext, ModelRouteRequirements, Orchestrator,
    ScopedToolExecutor, SubagentFailure, SubagentResult, SubagentSpec, SubagentStatus,
};
#[cfg(feature = "postgres-store")]
pub use postgres_store::PostgresDurableStore;
pub use protocols::*;
pub use provider_media::*;
pub use providers::anthropic::AnthropicProvider;
pub use providers::deepseek::DeepSeekProvider;
pub use providers::google::GeminiProvider;
pub use providers::groq::{GroqConfig, GroqProvider};
pub use providers::mistral::{MistralConfig, MistralProvider};
pub use providers::openai::OpenAiProvider;
pub use providers::openai_responses::OpenAiResponsesProvider;
pub use providers::openrouter::{OpenRouterConfig, OpenRouterProvider};
pub use providers::xai::{XaiConfig, XaiProvider};
pub use providers::{MockProvider, Provider, ProviderRequest};
pub use reasoning::{
    blocks_for_provider_replay, blocks_for_replay, validate_replay, ReplayError, ReplayPolicy,
};
pub use resilience::{
    AttemptOutcome, ExecutionPlan, ModelAttemptRecord, ModelTarget, ResilientProvider, RetryPolicy,
};
pub use routing::{
    estimate_cost_usd, ModelCapability, ModelCatalog, ModelProfile, ModelRejection,
    RejectionReason, RouteDecision, RouteError, RouteObjective, RoutePolicy, RouteRequest,
};
pub use runtime::{run_agent, RunConfig};
pub use schemars::JsonSchema;
pub use session::{
    InMemorySessionStore, JsonFileSessionStore, RunOutcome, RunRecorder, RunTerminalStatus,
    Session, SessionExecutionLease, SessionStore, SessionStoreError, SessionStoreResult,
};
pub use sqlite::{SqliteDurableStore, SqliteMemoryStore, SqliteSessionStore};
pub use streaming::{StreamEncodingError, StreamEncodingResult, StreamEventEncoder};
pub use temporal_adapter::{
    TemporalActivityInvocation, TemporalActivityOutcome, TemporalActivityPlan,
    TemporalActivitySpec, TemporalAdapter, TemporalAdapterConfig, TemporalAdapterError,
    TemporalAdapterResult, TemporalReconciliationPlan, TemporalRetryPolicy,
};
pub use tools::builtin::BuiltinTools;
pub use tools::web::{BrowserEgressPolicy, BrowserTools, WebTools};
pub use tools::{tool, NoTools, ToolExecutor, ToolRouter};
pub use trace_eval::{
    evaluate_trace, EvalResult, EvalSuite, TraceAssertion, TraceCheck, TraceInput,
};
pub use types::{
    ContentBlock, ContentPart, MediaSource, Message, ProviderMetadata, ProviderOptions, Role,
    StreamDelta, ToolSpec, Usage,
};

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use futures::StreamExt;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    /// A tool executor that records how many times it was called and echoes its input.
    struct CountingEcho {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl ToolExecutor for CountingEcho {
        async fn execute(&self, name: &str, input: serde_json::Value) -> Result<String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(format!("{name} çalıştı, input={input}"))
        }
    }

    #[tokio::test]
    async fn mock_loop_runs_one_tool_round_trip() {
        let provider: Arc<dyn Provider> = Arc::new(MockProvider);
        let executor = Arc::new(CountingEcho {
            calls: AtomicUsize::new(0),
        });

        let mut cfg = RunConfig::new("mock-1", vec![Message::user("selam")]);
        cfg.tools = vec![ToolSpec {
            name: "search_db".into(),
            description: "veritabanında ara".into(),
            input_schema: serde_json::json!({"type": "object"}),
        }];

        let stream = run_agent(provider, executor.clone(), cfg);
        futures::pin_mut!(stream);

        let mut kinds: Vec<String> = Vec::new();
        let mut final_text = String::new();
        while let Some(d) = stream.next().await {
            match &d {
                StreamDelta::TextDelta { text } => final_text.push_str(text),
                StreamDelta::ToolResult { content, .. } => {
                    assert!(content.contains("search_db çalıştı"));
                }
                _ => {}
            }
            kinds.push(
                format!("{d:?}")
                    .split_whitespace()
                    .next()
                    .unwrap()
                    .to_string(),
            );
        }

        // The tool was invoked exactly once (one round-trip).
        assert_eq!(executor.calls.load(Ordering::SeqCst), 1);
        // The loop produced a tool call, a tool result, and a final answer.
        assert!(kinds.iter().any(|k| k.contains("ToolCallStart")));
        assert!(kinds.iter().any(|k| k.contains("ToolResult")));
        assert!(final_text.contains("görevi tamamladım"));
    }
}
