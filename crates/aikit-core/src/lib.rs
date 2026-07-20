//! # aikit-runtime-core
//!
//! The agent-native, governed, provider-agnostic runtime **core** — "the brain". Pure Rust,
//! no FFI: the PyO3 (`aikit-py`) and napi (`aikit-node`) bindings sit on top of this crate,
//! and the native Rust API re-exports it. Correctness-critical logic (canonical schema,
//! reasoning-state replay, the agent loop, governance) lives here exactly once, so behaviour
//! is identical across providers *and* across languages.
//!
//! Phase 0 (this milestone) proves the two hard FFI seams end-to-end with a [`MockProvider`]:
//! streaming out to the host, and calling a host tool back in via [`ToolExecutor`].

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
pub mod durable_store;
pub mod dx;
pub mod error;
pub mod eval;
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

pub use agent::{Agent, AgentCapabilities, AgentError, GeneratedText, ProviderCapabilityView};
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
pub use compaction::{compact_messages, estimate_tokens, CompactionPolicy};
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
pub use durable_store::{
    DurableStore, DurableStoreError, DurableStoreResult, InMemoryDurableStore,
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
    McpClient, McpPrompt, McpResource, McpToolExecutor, McpToolFilter, McpTransport,
    StdioTransport, StreamableHttpTransport, MAX_MCP_TOOL_FILTER_NAMES, MAX_MCP_TOOL_NAME_CHARS,
    MCP_PROTOCOL_VERSION,
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
