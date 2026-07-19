//! The in-process agent loop.
//!
//! This is what the Claude Agent SDK hides inside the closed `claude` CLI. Here it is
//! owned in-process: build request → stream deltas (forwarded to the host) → collect tool
//! calls → run them through the [`ToolExecutor`] (the FFI seam) → append tool results →
//! repeat until the model stops or `max_turns` is hit. The governance harness (a permission
//! engine and enforcing PreToolUse hooks) authorizes every tool call at the seam BEFORE it runs;
//! a denied call is surfaced to the model as an error result, never executed.

use crate::providers::{Provider, ProviderRequest};
use crate::tools::ToolExecutor;
use crate::types::{ContentBlock, Message, Role, StreamDelta, Usage};
use async_stream::stream;
use futures::{Stream, StreamExt};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

/// Everything needed to run one agent conversation to completion.
pub struct RunConfig {
    pub model: String,
    pub messages: Vec<Message>,
    pub tools: Vec<crate::types::ToolSpec>,
    pub max_turns: usize,
    /// Output-token ceiling per model call.
    pub max_tokens: u64,
    /// Typed per-provider escape-hatch options carried to the wire.
    /// This flat map is retained for direct single-provider calls. Prefer `provider_options` when
    /// a configured run may route or fall back across providers.
    pub options: serde_json::Map<String, serde_json::Value>,
    /// Provider-keyed, non-lossy wire options. Only the selected provider's map is merged into a
    /// request, preventing Anthropic/OpenAI/Google/DeepSeek options from leaking across fallback
    /// boundaries. Provider-keyed values override the legacy flat map on conflicts.
    pub provider_options: crate::types::ProviderOptions,
    /// The governance harness: enforcing hooks + permission engine, applied at the tool seam.
    /// Default is fully permissive.
    pub governance: crate::governance::Governance,
    /// Structured lifecycle/governance evidence. Defaults to a no-sink, metadata-only trail.
    pub audit: crate::observability::AuditTrail,
    /// Token/USD governor. USD budgets require explicit pricing; unknown models never count as
    /// free. Enforcement occurs before any tool emitted in an over-budget turn is executed.
    pub budget: crate::budget::BudgetPolicy,
    /// Transcript compaction. Disabled by default; when enabled, the working transcript is bounded
    /// to a token budget at the start of each turn (first message + recent tail kept verbatim).
    pub compaction: crate::compaction::CompactionPolicy,
    /// Completion handle retained by the caller. Records canonical messages, not reconstructed
    /// final text, so reasoning/tool history remains resumable.
    pub recorder: crate::session::RunRecorder,
    /// Monotonic cooperative cancellation observed at every side-effect boundary.
    pub cancellation: crate::cancellation::CancellationToken,
    /// Absolute deadline inherited from a shared orchestration ledger. Kept private so the only
    /// producer is the ledger's original start time; callers cannot accidentally reset it per
    /// child.
    shared_wall_time_deadline: Option<Instant>,
    invocation_prepared: bool,
}

impl RunConfig {
    pub fn new(model: impl Into<String>, messages: Vec<Message>) -> Self {
        RunConfig {
            model: model.into(),
            messages,
            tools: Vec::new(),
            max_turns: 16,
            max_tokens: 4096,
            options: serde_json::Map::new(),
            provider_options: crate::types::ProviderOptions::new(),
            governance: crate::governance::Governance::default(),
            audit: crate::observability::AuditTrail::default(),
            budget: crate::budget::BudgetPolicy::default(),
            compaction: crate::compaction::CompactionPolicy::default(),
            recorder: crate::session::RunRecorder::default(),
            cancellation: crate::cancellation::CancellationToken::new(),
            shared_wall_time_deadline: None,
            invocation_prepared: false,
        }
    }

    pub(crate) fn enforce_shared_wall_time(&mut self, ledger: &crate::budget::BudgetLedger) {
        self.shared_wall_time_deadline = ledger.wall_time_deadline();
    }

    /// Establish invocation-local audit identity and approval state exactly once. High-level
    /// fallback wiring calls this before handing the same trail to `ResilientProvider`; direct
    /// `run_agent` callers are prepared at stream start.
    pub(crate) fn prepare_invocation(&mut self) {
        if self.invocation_prepared {
            return;
        }
        self.audit = self.audit.fresh_run();
        self.governance = self.governance.fork_for_run();
        self.invocation_prepared = true;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TerminationTrigger {
    ExternalCancellation,
    BudgetDeadline,
}

impl TerminationTrigger {
    fn terminal_reason(self) -> &'static str {
        match self {
            Self::ExternalCancellation => "cancelled",
            Self::BudgetDeadline => "budget_exceeded",
        }
    }
}

fn current_termination(
    cancellation: &crate::cancellation::CancellationToken,
    deadline: Option<Instant>,
) -> Option<TerminationTrigger> {
    // Preserve an already-requested caller cancellation even when the deadline is also ready.
    if cancellation.is_cancelled() {
        return Some(TerminationTrigger::ExternalCancellation);
    }
    deadline
        .is_some_and(|deadline| Instant::now() >= deadline)
        .then_some(TerminationTrigger::BudgetDeadline)
}

async fn wait_for_termination(
    cancellation: &crate::cancellation::CancellationToken,
    deadline: Option<Instant>,
) -> TerminationTrigger {
    if let Some(trigger) = current_termination(cancellation, deadline) {
        return trigger;
    }
    match deadline {
        Some(deadline) => {
            tokio::select! {
                biased;
                () = cancellation.cancelled() => TerminationTrigger::ExternalCancellation,
                () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                    TerminationTrigger::BudgetDeadline
                }
            }
        }
        None => {
            cancellation.cancelled().await;
            TerminationTrigger::ExternalCancellation
        }
    }
}

#[derive(Debug)]
struct PendingToolCall {
    id: String,
    name: String,
    input: Option<serde_json::Value>,
}

// Provider `max_tokens` is a request hint, not a trustworthy memory boundary. Keep enough room
// for unusually byte-heavy tokens and native metadata while imposing an absolute per-response
// ceiling against buggy, compromised, or custom providers that stream forever.
const MIN_RETAINED_RUN_BYTES: usize = 1024 * 1024;
const MAX_RETAINED_RUN_BYTES: usize = 64 * 1024 * 1024;
const RETAINED_BYTES_PER_REQUESTED_TOKEN: usize = 256;
const MAX_RETAINED_RUN_ITEMS: usize = 16_384;
const MAX_RUN_DELTAS: usize = 100_000;
const MAX_SINGLE_TOOL_RESULT_BYTES: usize = 4 * 1024 * 1024;
const MAX_RETAINED_JSON_DEPTH: usize = 128;
const MAX_RETAINED_JSON_NODES: usize = 65_536;
const RETAINED_JSON_NODE_OVERHEAD: usize = 16;

#[derive(Debug)]
struct RetainedRunBudget {
    bytes: usize,
    items: usize,
    deltas: usize,
    byte_limit: usize,
}

impl RetainedRunBudget {
    fn new(max_tokens: u64) -> Self {
        let requested_tokens = usize::try_from(max_tokens).unwrap_or(usize::MAX);
        let byte_limit = requested_tokens
            .saturating_mul(RETAINED_BYTES_PER_REQUESTED_TOKEN)
            .clamp(MIN_RETAINED_RUN_BYTES, MAX_RETAINED_RUN_BYTES);
        Self {
            bytes: 0,
            items: 0,
            deltas: 0,
            byte_limit,
        }
    }

    fn charge_delta(&mut self, delta: &StreamDelta) -> std::result::Result<(), String> {
        let (bytes, items) = match delta {
            StreamDelta::MessageStart { model } => (model.len(), 1),
            StreamDelta::TextDelta { text } => (text.len(), 0),
            // Count forwarded reasoning too. Native parsers also emit a completed block, so this
            // is deliberately conservative rather than trusting a provider's token hint.
            StreamDelta::ReasoningDelta { text } => (text.len(), 0),
            StreamDelta::Usage(_) => (0, 0),
            StreamDelta::ReasoningComplete {
                text,
                signature,
                opaque,
            } => (
                text.len()
                    .saturating_add(signature.as_ref().map_or(0, String::len))
                    .saturating_add(opaque.as_ref().map_or(0, retained_json_bytes)),
                1,
            ),
            StreamDelta::ToolCallStart { id, name } => {
                (id.len().saturating_mul(2).saturating_add(name.len()), 1)
            }
            StreamDelta::ToolCallInput { input, .. } => (retained_json_bytes(input), 0),
            StreamDelta::ToolResult {
                tool_use_id,
                content,
                ..
            } => (tool_use_id.len().saturating_add(content.len()), 1),
            StreamDelta::Citation {
                text,
                source,
                metadata,
            } => (
                text.len()
                    .saturating_add(source.as_ref().map_or(0, String::len))
                    .saturating_add(metadata.as_ref().map_or(0, retained_json_bytes)),
                1,
            ),
            StreamDelta::ProviderMetadata { provider, metadata } => (
                provider.len().saturating_add(retained_json_bytes(metadata)),
                1,
            ),
            StreamDelta::MessageStop { stop_reason } => (stop_reason.len(), 0),
            StreamDelta::Error { message, info } => (
                message
                    .len()
                    .saturating_add(info.message.len())
                    .saturating_add(info.provider.as_ref().map_or(0, String::len))
                    .saturating_add(info.model.as_ref().map_or(0, String::len)),
                0,
            ),
        };

        self.charge(bytes, items, 1, "provider stream")
    }

    fn charge_tool_result(&mut self, tool_use_id: &str, content: &str) -> Result<(), String> {
        self.charge(
            tool_use_id.len().saturating_add(content.len()),
            1,
            1,
            "tool output",
        )
    }

    fn charge(
        &mut self,
        bytes: usize,
        items: usize,
        deltas: usize,
        source: &str,
    ) -> Result<(), String> {
        let next_deltas = self.deltas.saturating_add(deltas);
        let next_bytes = self.bytes.saturating_add(bytes);
        let next_items = self.items.saturating_add(items);
        if next_deltas > MAX_RUN_DELTAS
            || next_bytes > self.byte_limit
            || next_items > MAX_RETAINED_RUN_ITEMS
        {
            return Err(format!(
                "{source} exceeded the retained-output safety limit ({} bytes, {} items, {} deltas)",
                self.byte_limit, MAX_RETAINED_RUN_ITEMS, MAX_RUN_DELTAS,
            ));
        }
        self.deltas = next_deltas;
        self.bytes = next_bytes;
        self.items = next_items;
        Ok(())
    }
}

fn retained_json_bytes(value: &serde_json::Value) -> usize {
    fn visit(value: &serde_json::Value, depth: usize, nodes: &mut usize, bytes: &mut usize) {
        if depth > MAX_RETAINED_JSON_DEPTH || *nodes >= MAX_RETAINED_JSON_NODES {
            *bytes = usize::MAX;
            return;
        }
        *nodes = nodes.saturating_add(1);
        *bytes = bytes.saturating_add(RETAINED_JSON_NODE_OVERHEAD);
        match value {
            serde_json::Value::Null | serde_json::Value::Bool(_) => {}
            serde_json::Value::Number(_) => *bytes = bytes.saturating_add(24),
            serde_json::Value::String(value) => *bytes = bytes.saturating_add(value.len()),
            serde_json::Value::Array(values) => {
                for value in values {
                    visit(value, depth.saturating_add(1), nodes, bytes);
                    if *bytes == usize::MAX {
                        break;
                    }
                }
            }
            serde_json::Value::Object(values) => {
                for (key, value) in values {
                    *bytes = bytes.saturating_add(key.len());
                    visit(value, depth.saturating_add(1), nodes, bytes);
                    if *bytes == usize::MAX {
                        break;
                    }
                }
            }
        }
    }

    let mut nodes = 0;
    let mut bytes = 0;
    visit(value, 0, &mut nodes, &mut bytes);
    bytes
}

fn compile_tool_validators(
    tools: &[crate::types::ToolSpec],
) -> crate::error::Result<HashMap<String, jsonschema::Validator>> {
    let mut validators = HashMap::with_capacity(tools.len());
    for tool in tools {
        if tool.name.trim().is_empty() {
            return Err(crate::error::AikitError::Configuration(
                "tool name cannot be empty".into(),
            ));
        }
        if !tool.input_schema.is_object() {
            return Err(crate::error::AikitError::Configuration(format!(
                "tool '{}' input_schema must be a JSON Schema object",
                tool.name
            )));
        }
        let validator = jsonschema::validator_for(&tool.input_schema).map_err(|error| {
            crate::error::AikitError::Configuration(format!(
                "tool '{}' has an invalid input_schema: {}",
                tool.name,
                error.masked()
            ))
        })?;
        if validators.insert(tool.name.clone(), validator).is_some() {
            return Err(crate::error::AikitError::Configuration(format!(
                "tool '{}' is advertised more than once",
                tool.name
            )));
        }
    }
    Ok(validators)
}

fn validate_tool_input(
    tool: &str,
    validator: &jsonschema::Validator,
    input: &serde_json::Value,
) -> std::result::Result<(), String> {
    validator.validate(input).map_err(|error| {
        format!(
            "tool '{tool}' input failed JSON Schema validation: {}",
            error.masked()
        )
    })
}

fn add_usage(total: &mut Usage, part: Usage) {
    total.input_tokens = total.input_tokens.saturating_add(part.input_tokens);
    total.output_tokens = total.output_tokens.saturating_add(part.output_tokens);
    total.cache_creation_input_tokens = total
        .cache_creation_input_tokens
        .saturating_add(part.cache_creation_input_tokens);
    total.cache_read_input_tokens = total
        .cache_read_input_tokens
        .saturating_add(part.cache_read_input_tokens);
    total.reasoning_tokens = total.reasoning_tokens.saturating_add(part.reasoning_tokens);
}

fn latest_user_prompt(messages: &[Message]) -> Option<String> {
    messages.iter().rev().find_map(|message| {
        if message.role != Role::User {
            return None;
        }
        message.content.iter().find_map(|block| match block {
            ContentBlock::Text { text } => Some(text.clone()),
            _ => None,
        })
    })
}

fn rewrite_latest_user_prompt(messages: &mut [Message], replacement: String) {
    if let Some(message) = messages.iter_mut().rev().find(|m| m.role == Role::User) {
        if let Some(ContentBlock::Text { text }) = message
            .content
            .iter_mut()
            .find(|b| matches!(b, ContentBlock::Text { .. }))
        {
            *text = replacement;
        }
    }
}

struct FailureHookResult {
    message: String,
    audit_error: Option<crate::error::AikitError>,
}

struct ToolInputRejection {
    reason: String,
    source: &'static str,
}

async fn run_failure_hooks(
    hooks: &crate::governance::hooks::HookDispatcher,
    audit: &crate::observability::AuditTrail,
    ctx: crate::governance::hooks::FailureContext,
    terminal: bool,
) -> FailureHookResult {
    let turn = ctx.turn;
    let stage = ctx.stage.as_str().to_string();
    let tool = ctx.tool.clone();
    let error = hooks.run_failure(ctx).await;
    // Never recurse into Failure hooks when the audit of a failure itself fails.
    let audit_error = audit
        .emit(crate::observability::AuditEvent::Failure {
            turn,
            stage,
            tool,
            error: error.clone(),
            terminal,
        })
        .err();
    FailureHookResult {
        message: error,
        audit_error,
    }
}

async fn reject_tool_input(
    hooks: &crate::governance::hooks::HookDispatcher,
    audit: &crate::observability::AuditTrail,
    run_id: &str,
    turn: usize,
    call: &PendingToolCall,
    input: &serde_json::Value,
    rejection: ToolInputRejection,
) -> crate::error::Result<String> {
    let failure = run_failure_hooks(
        hooks,
        audit,
        crate::governance::hooks::FailureContext {
            run_id: run_id.to_string(),
            turn,
            stage: crate::governance::hooks::FailureStage::ToolInputValidation,
            tool_use_id: Some(call.id.clone()),
            tool: Some(call.name.clone()),
            error: rejection.reason,
        },
        false,
    )
    .await;
    if let Some(error) = failure.audit_error {
        return Err(error);
    }
    audit.emit(crate::observability::AuditEvent::PermissionDecision {
        turn,
        tool_use_id: call.id.clone(),
        tool: call.name.clone(),
        decision: "deny".into(),
        source: rejection.source.into(),
        reason: Some(failure.message.clone()),
        input: audit.capture_value(input),
    })?;
    Ok(failure.message)
}

/// Run the agent loop, yielding canonical [`StreamDelta`]s. The returned stream drives the
/// whole multi-turn tool loop; the caller just iterates it.
pub fn run_agent(
    provider: Arc<dyn Provider>,
    executor: Arc<dyn ToolExecutor>,
    mut cfg: RunConfig,
) -> impl Stream<Item = StreamDelta> {
    stream! {
        use crate::governance::hooks::{
            FailureContext, FailureStage, PostToolOutcome, PostToolUseContext,
            PromptContext, PromptHookOutcome, StopContext,
        };
        use crate::governance::{Authorization, AuthorizationContext};
        use crate::observability::AuditEvent;

        // Invocation-local identity and human approval state: cloned options may run concurrently,
        // but neither audit sequence nor AllowTool grants can bleed into a sibling invocation.
        cfg.prepare_invocation();
        let run_id = cfg.audit.run_id().to_string();
        let validator_result = compile_tool_validators(&cfg.tools);
        let mut tool_validators = HashMap::new();
        let advertised_tools: HashSet<String> = cfg.tools.iter().map(|tool| tool.name.clone()).collect();
        cfg.recorder.begin(cfg.messages.clone());
        let mut turn = 0usize;
        let mut total_usage = Usage::default();
        let mut terminal_reason = "end_turn".to_string();
        let mut budget = None;
        let mut can_run = true;
        let mut budget_error_emitted = false;
        let retained_token_hint = cfg.max_tokens.saturating_mul(
            u64::try_from(cfg.max_turns.max(1)).unwrap_or(u64::MAX),
        );
        let mut retained_output = RetainedRunBudget::new(retained_token_hint);

        if let Err(error) = cfg.audit.emit(AuditEvent::RunStarted {
            model: cfg.model.clone(),
        }) {
            terminal_reason = "audit_failure".into();
            can_run = false;
            yield StreamDelta::from_error(&error);
        }

        if can_run {
            match validator_result {
                Ok(validators) => tool_validators = validators,
                Err(error) => {
                    let failure = run_failure_hooks(
                        &cfg.governance.hooks,
                        &cfg.audit,
                        FailureContext {
                            run_id: run_id.clone(),
                            turn: 0,
                            stage: FailureStage::Configuration,
                            tool_use_id: None,
                            tool: None,
                            error: error.to_string(),
                        },
                        true,
                    ).await;
                    let info = failure.audit_error.as_ref().map_or_else(
                        || error.info(),
                        crate::error::AikitError::info,
                    );
                    terminal_reason = if failure.audit_error.is_some() {
                        "audit_failure".into()
                    } else {
                        "configuration_error".into()
                    };
                    yield StreamDelta::error_with_info(failure.message, info);
                    can_run = false;
                }
            }
        }

        if can_run {
            match crate::budget::BudgetTracker::new(cfg.budget.clone()) {
                Ok(tracker) => budget = Some(tracker),
                Err(error) => {
                    terminal_reason = "budget_configuration_error".into();
                    yield StreamDelta::from_error(&error);
                    can_run = false;
                }
            }
        }

        if can_run {
            if let Some(trigger) = current_termination(
                &cfg.cancellation,
                cfg.shared_wall_time_deadline,
            ) {
                terminal_reason = trigger.terminal_reason().into();
                can_run = false;
            }
        }

        if can_run {
            if let Some(prompt) = latest_user_prompt(&cfg.messages) {
                let prompt_outcome = tokio::select! {
                    biased;
                    trigger = wait_for_termination(
                        &cfg.cancellation,
                        cfg.shared_wall_time_deadline,
                    ) => Err(trigger),
                    outcome = cfg.governance.hooks.run_user_prompt_submit(PromptContext {
                        run_id: run_id.clone(),
                        prompt,
                    }) => Ok(outcome),
                };
                match prompt_outcome {
                    Ok(prompt_outcome) => match prompt_outcome {
                        PromptHookOutcome::Continue => {}
                        PromptHookOutcome::Rewrite(replacement) => {
                            rewrite_latest_user_prompt(&mut cfg.messages, replacement);
                            // `begin` captures input before hooks so early failures remain
                            // inspectable. Once a hook sanitizes the prompt, replace that initial
                            // copy immediately so RunOutcome/session persistence cannot retain the
                            // raw pre-hook text.
                            cfg.recorder.replace_messages(cfg.messages.clone());
                        }
                        PromptHookOutcome::Block(reason) => {
                            let failure = run_failure_hooks(
                                &cfg.governance.hooks,
                                &cfg.audit,
                                FailureContext {
                                    run_id: run_id.clone(),
                                    turn: 0,
                                    stage: FailureStage::PreToolUse,
                                    tool_use_id: None,
                                    tool: None,
                                    error: format!("user prompt blocked by hook: {reason}"),
                                },
                                true,
                            ).await;
                            let info = failure.audit_error.as_ref().map_or_else(
                                || crate::error::ErrorInfo::new(crate::error::ErrorCode::Hook),
                                crate::error::AikitError::info,
                            );
                            terminal_reason = if failure.audit_error.is_some() {
                                "audit_failure".into()
                            } else {
                                "prompt_blocked".into()
                            };
                            can_run = false;
                            yield StreamDelta::error_with_info(
                                failure.message,
                                info,
                            );
                        }
                    },
                    Err(trigger) => {
                        terminal_reason = trigger.terminal_reason().into();
                        can_run = false;
                    }
                }
            }
        }

        if can_run {
        'agent: loop {
            if let Some(trigger) = current_termination(
                &cfg.cancellation,
                cfg.shared_wall_time_deadline,
            ) {
                terminal_reason = trigger.terminal_reason().into();
                break 'agent;
            }
            turn += 1;
            if turn > cfg.max_turns {
                let failure = run_failure_hooks(
                    &cfg.governance.hooks,
                    &cfg.audit,
                    FailureContext {
                        run_id: run_id.clone(),
                        turn,
                        stage: FailureStage::MaxTurns,
                        tool_use_id: None,
                        tool: None,
                        error: "max_turns exceeded".into(),
                    },
                    true,
                ).await;
                let info = failure.audit_error.as_ref().map_or_else(
                    || crate::error::ErrorInfo::new(crate::error::ErrorCode::MaxTurns),
                    crate::error::AikitError::info,
                );
                terminal_reason = if failure.audit_error.is_some()
                    || info.code == crate::error::ErrorCode::Audit
                {
                    "audit_failure".into()
                } else {
                    "max_turns".into()
                };
                yield StreamDelta::error_with_info(
                    failure.message,
                    info,
                );
                break 'agent;
            }

            // Bound the transcript to the context window before building the request. No-op unless
            // compaction is enabled; keeps the task anchor + recent tail and preserves tool pairing.
            if let Some(compacted) = crate::compaction::compact_messages(&cfg.messages, &cfg.compaction)
            {
                cfg.messages = compacted;
            }

            if let Err(error) = cfg.audit.emit(AuditEvent::RequestStarted {
                turn,
                model: cfg.model.clone(),
                message_count: cfg.messages.len(),
                tool_count: cfg.tools.len(),
            }) {
                terminal_reason = "audit_failure".into();
                yield StreamDelta::from_error(&error);
                break 'agent;
            }
            let req = ProviderRequest {
                model: cfg.model.clone(),
                messages: cfg.messages.clone(),
                tools: cfg.tools.clone(),
                max_tokens: cfg.max_tokens,
                options: cfg.options.clone(),
                provider_options: cfg.provider_options.clone(),
            };

            let provider_result = tokio::select! {
                biased;
                trigger = wait_for_termination(
                    &cfg.cancellation,
                    cfg.shared_wall_time_deadline,
                ) => {
                    terminal_reason = trigger.terminal_reason().into();
                    break 'agent;
                }
                result = provider.stream(req) => result,
            };
            let mut inner = match provider_result {
                Ok(s) => s,
                Err(e) => {
                    let public_error = e.info().message;
                    let failure = run_failure_hooks(
                        &cfg.governance.hooks,
                        &cfg.audit,
                        FailureContext {
                            run_id: run_id.clone(),
                            turn,
                            stage: FailureStage::ProviderStart,
                            tool_use_id: None,
                            tool: None,
                            error: public_error,
                        },
                        true,
                    ).await;
                    let info = failure
                        .audit_error
                        .as_ref()
                        .map_or_else(|| e.info(), crate::error::AikitError::info);
                    terminal_reason = if failure.audit_error.is_some() {
                        "audit_failure".into()
                    } else {
                        "provider_error".into()
                    };
                    yield StreamDelta::error_with_info(failure.message, info);
                    break 'agent;
                }
            };

            // Reconstruct the assistant turn from the stream while forwarding every delta.
            let mut text = String::new();
            let mut tool_calls: Vec<PendingToolCall> = Vec::new();
            let mut index_by_id: HashMap<String, usize> = HashMap::new();
            let mut stream_error: Option<(String, crate::error::ErrorInfo)> = None;
            let mut budget_error: Option<String> = None;
            let mut audit_stream_error: Option<crate::error::AikitError> = None;
            let mut model_stop_reason: Option<String> = None;
            let mut recorded_model_attempt = false;
            // Completed reasoning blocks (text, signature, opaque) — MUST be persisted and
            // replayed verbatim per provider (Anthropic signed thinking, OpenAI/Google opaque
            // state, redacted_thinking). Dropping them breaks extended-thinking + tool-use loops.
            let mut reasoning: Vec<(String, Option<String>, Option<serde_json::Value>)> = Vec::new();
            let mut citations: Vec<(String, Option<String>, Option<serde_json::Value>)> = Vec::new();
            loop {
                let next_delta = tokio::select! {
                    biased;
                    trigger = wait_for_termination(
                        &cfg.cancellation,
                        cfg.shared_wall_time_deadline,
                    ) => {
                        terminal_reason = trigger.terminal_reason().into();
                        break 'agent;
                    }
                    delta = inner.next() => delta,
                };
                let Some(delta) = next_delta else {
                    break;
                };
                if let Err(message) = retained_output.charge_delta(&delta) {
                    let info = crate::error::ErrorInfo::new(
                        crate::error::ErrorCode::ProviderProtocol,
                    )
                    .with_provider(provider.name(), &cfg.model);
                    stream_error = Some((message.clone(), info.clone()));
                    text.clear();
                    tool_calls.clear();
                    index_by_id.clear();
                    reasoning.clear();
                    citations.clear();
                    yield StreamDelta::error_with_info(message, info);
                    break;
                }
                match &delta {
                    StreamDelta::MessageStart { model } if !recorded_model_attempt => {
                        cfg.recorder.record_model_attempt(model.clone());
                        recorded_model_attempt = true;
                    }
                    StreamDelta::TextDelta { text: t } => text.push_str(t),
                    StreamDelta::ReasoningComplete { text: rt, signature, opaque } => {
                        reasoning.push((rt.clone(), signature.clone(), opaque.clone()));
                    }
                    StreamDelta::Citation {
                        text,
                        source,
                        metadata,
                    } => citations.push((text.clone(), source.clone(), metadata.clone())),
                    StreamDelta::ProviderMetadata { provider, metadata } => {
                        cfg.recorder
                            .record_provider_metadata(provider.clone(), metadata.clone());
                    }
                    StreamDelta::ToolCallStart { id, name } => {
                        index_by_id.insert(id.clone(), tool_calls.len());
                        tool_calls.push(PendingToolCall {
                            id: id.clone(),
                            name: name.clone(),
                            input: None,
                        });
                    }
                    StreamDelta::ToolCallInput { id, input } => {
                        if let Some(&i) = index_by_id.get(id) {
                            tool_calls[i].input = Some(input.clone());
                        } else {
                            stream_error = Some((
                                format!("tool input arrived for unknown call '{id}'"),
                                crate::error::ErrorInfo::new(
                                    crate::error::ErrorCode::ProviderProtocol,
                                )
                                .with_provider(provider.name(), &cfg.model),
                            ));
                        }
                    }
                    StreamDelta::Usage(usage) => {
                        add_usage(&mut total_usage, *usage);
                        if let Err(error) = cfg.audit.emit(AuditEvent::Usage { turn, usage: *usage }) {
                            audit_stream_error = Some(error);
                        } else if let Some(tracker) = &mut budget {
                            let result = tracker.record(*usage);
                            let snapshot = tracker.snapshot();
                            if let Err(error) = cfg.audit.emit(AuditEvent::BudgetUpdated {
                                turn,
                                total_tokens: snapshot
                                    .usage
                                    .input_tokens
                                    .saturating_add(snapshot.usage.output_tokens),
                                estimated_cost_usd: snapshot.estimated_cost_usd,
                            }) {
                                audit_stream_error = Some(error);
                            }
                            if let Err(error) = result {
                                budget_error = Some(error.to_string());
                            }
                        }
                    }
                    StreamDelta::MessageStop { stop_reason } => {
                        model_stop_reason = Some(stop_reason.clone());
                    }
                    StreamDelta::Error { message, info } => {
                        stream_error = Some((message.clone(), info.clone()));
                    }
                    _ => {}
                }
                yield delta;
                if let Some(error) = audit_stream_error.take() {
                    stream_error = Some((error.to_string(), error.info()));
                    yield StreamDelta::from_error(&error);
                    break;
                }
            }

            // Native adapters identify the selected (including fallback) model in MessageStart.
            // Custom providers are allowed to omit it; retain the requested model in that case
            // instead of silently producing an empty attempt history.
            if !recorded_model_attempt {
                cfg.recorder.record_model_attempt(cfg.model.clone());
            }

            if let Some(trigger) = current_termination(
                &cfg.cancellation,
                cfg.shared_wall_time_deadline,
            ) {
                terminal_reason = trigger.terminal_reason().into();
                break 'agent;
            }

            if let Some((raw_error, info)) = stream_error {
                let original_error = raw_error.clone();
                let failure = run_failure_hooks(
                    &cfg.governance.hooks,
                    &cfg.audit,
                    FailureContext {
                        run_id: run_id.clone(),
                        turn,
                        stage: FailureStage::ProviderStream,
                        tool_use_id: None,
                        tool: None,
                        error: raw_error,
                    },
                    true,
                ).await;
                let info = failure
                    .audit_error
                    .as_ref()
                    .map_or(info, crate::error::AikitError::info);
                terminal_reason = if failure.audit_error.is_some()
                    || info.code == crate::error::ErrorCode::Audit
                {
                    "audit_failure".into()
                } else {
                    "provider_stream_error".into()
                };
                // The provider's raw Error delta was already forwarded. Emit the rewritten form
                // only when a Failure hook materially changed it.
                if failure.message != original_error || failure.audit_error.is_some() {
                    yield StreamDelta::error_with_info(failure.message, info);
                }
                break 'agent;
            }

            if let Some(error) = budget_error {
                terminal_reason = "budget_exceeded".into();
                let failure = run_failure_hooks(
                    &cfg.governance.hooks,
                    &cfg.audit,
                    FailureContext {
                        run_id: run_id.clone(),
                        turn,
                        stage: FailureStage::Budget,
                        tool_use_id: None,
                        tool: None,
                        error,
                    },
                    true,
                ).await;
                let info = failure.audit_error.as_ref().map_or_else(
                    || crate::error::ErrorInfo::new(crate::error::ErrorCode::BudgetExceeded),
                    crate::error::AikitError::info,
                );
                if failure.audit_error.is_some() {
                    terminal_reason = "audit_failure".into();
                }
                yield StreamDelta::error_with_info(failure.message, info);
                budget_error_emitted = true;
                break 'agent;
            }

            if let Some(call) = tool_calls.iter().find(|call| call.input.is_none()) {
                let failure = run_failure_hooks(
                    &cfg.governance.hooks,
                    &cfg.audit,
                    FailureContext {
                        run_id: run_id.clone(),
                        turn,
                        stage: FailureStage::MalformedToolCall,
                        tool_use_id: Some(call.id.clone()),
                        tool: Some(call.name.clone()),
                        error: format!("tool call '{}' ended without valid input", call.id),
                    },
                    true,
                ).await;
                let info = failure.audit_error.as_ref().map_or_else(
                    || {
                        crate::error::ErrorInfo::new(crate::error::ErrorCode::ProviderProtocol)
                            .with_provider(provider.name(), &cfg.model)
                    },
                    crate::error::AikitError::info,
                );
                terminal_reason = if failure.audit_error.is_some() {
                    "audit_failure".into()
                } else {
                    "malformed_tool_call".into()
                };
                yield StreamDelta::error_with_info(failure.message, info);
                break 'agent;
            }

            // Persist the assistant message into history: reasoning FIRST (thinking-first
            // ordering the providers require), then text, then tool_use blocks. Per-provider
            // replay/drop rules are applied by each adapter's build_request.
            let mut assistant_blocks: Vec<ContentBlock> = Vec::new();
            for (rtext, signature, opaque) in &reasoning {
                assistant_blocks.push(ContentBlock::Reasoning {
                    text: rtext.clone(),
                    signature: signature.clone(),
                    provider: Some(provider.name().to_string()),
                    opaque: opaque.clone(),
                });
            }
            if !text.is_empty() {
                assistant_blocks.push(ContentBlock::Text { text: text.clone() });
            }
            for (text, source, metadata) in citations {
                assistant_blocks.push(ContentBlock::Citation {
                    text,
                    source,
                    metadata,
                });
            }
            for call in &tool_calls {
                assistant_blocks.push(ContentBlock::ToolUse {
                    id: call.id.clone(),
                    name: call.name.clone(),
                    input: call.input.clone().expect("validated tool input"),
                });
            }
            let assistant_message = Message {
                role: Role::Assistant,
                content: assistant_blocks,
            };
            cfg.messages.push(assistant_message.clone());
            cfg.recorder.append_message(assistant_message);

            // Done when the model asked for no tools. (Execute whenever tool calls are present,
            // regardless of the exact stop_reason string — providers vary.)
            if tool_calls.is_empty() {
                cfg.recorder.set_final_text(text);
                terminal_reason = model_stop_reason.unwrap_or_else(|| "end_turn".into());
                break 'agent;
            }

            // Execute each tool via the FFI seam — but only AFTER governance authorizes it.
            // A denied call is never executed. Normal denials become error tool-results;
            // interrupting human denials terminate the run. Hooks may rewrite allowed input.
            let mut result_blocks: Vec<ContentBlock> = Vec::new();
            for call in tool_calls {
                if let Some(trigger) = current_termination(
                    &cfg.cancellation,
                    cfg.shared_wall_time_deadline,
                ) {
                    terminal_reason = trigger.terminal_reason().into();
                    break 'agent;
                }
                let input = call.input.clone().expect("validated tool input");
                if !advertised_tools.contains(&call.name) {
                    let failure = run_failure_hooks(
                        &cfg.governance.hooks,
                        &cfg.audit,
                        FailureContext {
                            run_id: run_id.clone(),
                            turn,
                            stage: FailureStage::ToolNotAdvertised,
                            tool_use_id: Some(call.id.clone()),
                            tool: Some(call.name.clone()),
                            error: format!(
                                "tool '{}' was not advertised for this run and is denied",
                                call.name
                            ),
                        },
                        false,
                    )
                    .await;
                    if let Some(error) = failure.audit_error {
                        terminal_reason = "audit_failure".into();
                        yield StreamDelta::from_error(&error);
                        break 'agent;
                    }
                    let reason = if failure.message.len() > MAX_SINGLE_TOOL_RESULT_BYTES {
                        format!(
                            "tool '{}' denial output exceeded {} bytes and was discarded",
                            call.name, MAX_SINGLE_TOOL_RESULT_BYTES
                        )
                    } else {
                        failure.message
                    };
                    if let Err(message) = retained_output.charge_tool_result(&call.id, &reason) {
                        terminal_reason = "retained_output_limit".into();
                        yield StreamDelta::error_with_info(
                            message,
                            crate::error::ErrorInfo::new(
                                crate::error::ErrorCode::ToolExecution,
                            ),
                        );
                        break 'agent;
                    }
                    if let Err(error) = cfg.audit.emit(AuditEvent::PermissionDecision {
                        turn,
                        tool_use_id: call.id.clone(),
                        tool: call.name.clone(),
                        decision: "deny".into(),
                        source: "runtime:advertised_tools".into(),
                        reason: Some(reason.clone()),
                        input: cfg.audit.capture_value(&input),
                    }) {
                        terminal_reason = "audit_failure".into();
                        yield StreamDelta::from_error(&error);
                        break 'agent;
                    }
                    yield StreamDelta::ToolResult {
                        tool_use_id: call.id.clone(),
                        content: reason.clone(),
                        is_error: true,
                    };
                    result_blocks.push(ContentBlock::ToolResult {
                        tool_use_id: call.id,
                        content: reason,
                        is_error: true,
                    });
                    continue;
                }
                let validator = tool_validators
                    .get(&call.name)
                    .expect("advertised tool validator compiled at run start");
                if let Err(reason) = validate_tool_input(&call.name, validator, &input) {
                    let reason = match reject_tool_input(
                        &cfg.governance.hooks,
                        &cfg.audit,
                        &run_id,
                        turn,
                        &call,
                        &input,
                        ToolInputRejection {
                            reason,
                            source: "runtime:tool_input_schema",
                        },
                    )
                    .await
                    {
                        Ok(reason) => reason,
                        Err(error) => {
                            terminal_reason = "audit_failure".into();
                            yield StreamDelta::from_error(&error);
                            break 'agent;
                        }
                    };
                    let reason = if reason.len() > MAX_SINGLE_TOOL_RESULT_BYTES {
                        format!(
                            "tool '{}' validation output exceeded {} bytes and was discarded",
                            call.name, MAX_SINGLE_TOOL_RESULT_BYTES
                        )
                    } else {
                        reason
                    };
                    if let Err(message) = retained_output.charge_tool_result(&call.id, &reason) {
                        terminal_reason = "retained_output_limit".into();
                        yield StreamDelta::error_with_info(
                            message,
                            crate::error::ErrorInfo::new(
                                crate::error::ErrorCode::ToolExecution,
                            ),
                        );
                        break 'agent;
                    }
                    yield StreamDelta::ToolResult {
                        tool_use_id: call.id.clone(),
                        content: reason.clone(),
                        is_error: true,
                    };
                    result_blocks.push(ContentBlock::ToolResult {
                        tool_use_id: call.id,
                        content: reason,
                        is_error: true,
                    });
                    continue;
                }
                let report = tokio::select! {
                    biased;
                    trigger = wait_for_termination(
                        &cfg.cancellation,
                        cfg.shared_wall_time_deadline,
                    ) => {
                        terminal_reason = trigger.terminal_reason().into();
                        break 'agent;
                    }
                    report = cfg.governance.authorize_detailed_with_context(AuthorizationContext {
                        run_id: run_id.clone(),
                        turn,
                        tool_use_id: call.id.clone(),
                        tool: call.name.clone(),
                        input: input.clone(),
                    }) => report,
                };

                if let Some(trigger) = current_termination(
                    &cfg.cancellation,
                    cfg.shared_wall_time_deadline,
                ) {
                    terminal_reason = trigger.terminal_reason().into();
                    break 'agent;
                }

                let pre_hook_audit_error = cfg.audit.emit(AuditEvent::HookCompleted {
                    turn,
                    phase: "pre_tool_use".into(),
                    tool: Some(call.name.clone()),
                    outcome: report.pre_hook_outcome.into(),
                }).err();

                if let Some(error) = pre_hook_audit_error {
                    terminal_reason = "audit_failure".into();
                    yield StreamDelta::from_error(&error);
                    break 'agent;
                }

                if report.interrupt {
                    let message = match &report.authorization {
                        Authorization::Denied { message, .. } => message.clone(),
                        Authorization::Allowed(_) => {
                            "invalid governance report: allowed authorization requested interrupt"
                                .into()
                        }
                    };
                    let failure = run_failure_hooks(
                        &cfg.governance.hooks,
                        &cfg.audit,
                        FailureContext {
                            run_id: run_id.clone(),
                            turn,
                            stage: FailureStage::Permission,
                            tool_use_id: Some(call.id.clone()),
                            tool: Some(call.name.clone()),
                            error: message,
                        },
                        true,
                    )
                    .await;
                    if let Some(error) = failure.audit_error {
                        terminal_reason = "audit_failure".into();
                        yield StreamDelta::from_error(&error);
                        break 'agent;
                    }
                    let reason = failure.message;
                    if let Err(error) = cfg.audit.emit(AuditEvent::PermissionDecision {
                        turn,
                        tool_use_id: call.id,
                        tool: call.name,
                        decision: report.permission_outcome.into(),
                        source: report.permission_source,
                        reason: Some(reason.clone()),
                        input: cfg.audit.capture_value(&input),
                    }) {
                        terminal_reason = "audit_failure".into();
                        yield StreamDelta::from_error(&error);
                        break 'agent;
                    }
                    terminal_reason = "approval_interrupted".into();
                    yield StreamDelta::error_with_info(
                        reason,
                        crate::error::ErrorInfo::new(crate::error::ErrorCode::Cancelled),
                    );
                    break 'agent;
                }

                let (mut content, mut is_error, effective_input) = match report.authorization {
                    Authorization::Denied {
                        message: reason,
                        interrupt: false,
                    } => {
                        let failure = run_failure_hooks(
                            &cfg.governance.hooks,
                            &cfg.audit,
                            FailureContext {
                                run_id: run_id.clone(),
                                turn,
                                stage: FailureStage::Permission,
                                tool_use_id: Some(call.id.clone()),
                                tool: Some(call.name.clone()),
                                error: reason,
                            },
                            false,
                        ).await;
                        if let Some(error) = failure.audit_error {
                            terminal_reason = "audit_failure".into();
                            yield StreamDelta::from_error(&error);
                            break 'agent;
                        }
                        let reason = failure.message;
                        if let Err(error) = cfg.audit.emit(AuditEvent::PermissionDecision {
                            turn,
                            tool_use_id: call.id.clone(),
                            tool: call.name.clone(),
                            decision: report.permission_outcome.into(),
                            source: report.permission_source.clone(),
                            reason: Some(reason.clone()),
                            input: cfg.audit.capture_value(&input),
                        }) {
                            terminal_reason = "audit_failure".into();
                            yield StreamDelta::from_error(&error);
                            break 'agent;
                        }
                        (reason, true, None)
                    }
                    Authorization::Denied {
                        interrupt: true, ..
                    } => unreachable!("interrupting denials are handled before tool results"),
                    Authorization::Allowed(effective) => {
                        if let Some(trigger) = current_termination(
                            &cfg.cancellation,
                            cfg.shared_wall_time_deadline,
                        ) {
                            terminal_reason = trigger.terminal_reason().into();
                            break 'agent;
                        }
                        if let Err(reason) = validate_tool_input(&call.name, validator, &effective) {
                            let reason = match reject_tool_input(
                                &cfg.governance.hooks,
                                &cfg.audit,
                                &run_id,
                                turn,
                                &call,
                                &effective,
                                ToolInputRejection {
                                    reason,
                                    source: "runtime:effective_tool_input_schema",
                                },
                            )
                            .await
                            {
                                Ok(reason) => reason,
                                Err(error) => {
                                    terminal_reason = "audit_failure".into();
                                    yield StreamDelta::from_error(&error);
                                    break 'agent;
                                }
                            };
                            (reason, true, None)
                        } else if let Err(error) = cfg.audit.emit(AuditEvent::PermissionDecision {
                            turn,
                            tool_use_id: call.id.clone(),
                            tool: call.name.clone(),
                            decision: report.permission_outcome.into(),
                            source: report.permission_source.clone(),
                            reason: None,
                            input: cfg.audit.capture_value(&effective),
                        }) {
                            terminal_reason = "audit_failure".into();
                            yield StreamDelta::from_error(&error);
                            break 'agent;
                        } else if let Err(error) = cfg.audit.emit(AuditEvent::ToolStarted {
                            turn,
                            tool_use_id: call.id.clone(),
                            tool: call.name.clone(),
                            input: cfg.audit.capture_value(&effective),
                        }) {
                            terminal_reason = "audit_failure".into();
                            yield StreamDelta::from_error(&error);
                            break 'agent;
                        } else {
                            if let Some(trigger) = current_termination(
                                &cfg.cancellation,
                                cfg.shared_wall_time_deadline,
                            ) {
                                terminal_reason = trigger.terminal_reason().into();
                                break 'agent;
                            }
                            let started = Instant::now();
                            let execution = tokio::select! {
                                biased;
                                trigger = wait_for_termination(
                                    &cfg.cancellation,
                                    cfg.shared_wall_time_deadline,
                                ) => {
                                    terminal_reason = trigger.terminal_reason().into();
                                    break 'agent;
                                }
                                result = executor.execute(&call.name, effective.clone()) => result,
                            };
                            match execution {
                                Ok(raw_output) => {
                                    let output_was_oversized =
                                        raw_output.len() > MAX_SINGLE_TOOL_RESULT_BYTES;
                                    let raw_output = if output_was_oversized {
                                        format!(
                                            "tool '{}' output exceeded {} bytes and was discarded",
                                            call.name, MAX_SINGLE_TOOL_RESULT_BYTES
                                        )
                                    } else {
                                        raw_output
                                    };
                                    let duration_ms = started.elapsed().as_millis();
                                    let post = tokio::select! {
                                        biased;
                                        trigger = wait_for_termination(
                                            &cfg.cancellation,
                                            cfg.shared_wall_time_deadline,
                                        ) => {
                                            terminal_reason = trigger.terminal_reason().into();
                                            break 'agent;
                                        }
                                        post = cfg.governance.hooks.run_post_tool_use(PostToolUseContext {
                                            run_id: run_id.clone(),
                                            turn,
                                            tool_use_id: call.id.clone(),
                                            tool: call.name.clone(),
                                            input: effective.clone(),
                                            output: raw_output.clone(),
                                            duration_ms,
                                        }) => post,
                                    };
                                    if let Err(error) = cfg.audit.emit(AuditEvent::HookCompleted {
                                        turn,
                                        phase: "post_tool_use".into(),
                                        tool: Some(call.name.clone()),
                                        outcome: match &post {
                                            PostToolOutcome::Continue => "continue",
                                            PostToolOutcome::RewriteOutput(_) => "rewrite_output",
                                            PostToolOutcome::MarkError(_) => "mark_error",
                                        }.into(),
                                    }) {
                                        // The tool side effect already completed. Fail closed and
                                        // never advance to another tool or model turn.
                                        terminal_reason = "audit_failure".into();
                                        yield StreamDelta::from_error(&error);
                                        break 'agent;
                                    }
                                    match post {
                                        PostToolOutcome::Continue => {
                                            (raw_output, output_was_oversized, Some(effective))
                                        }
                                        PostToolOutcome::RewriteOutput(output) => {
                                            (output, false, Some(effective))
                                        }
                                        PostToolOutcome::MarkError(error) => {
                                            let failure = run_failure_hooks(
                                                &cfg.governance.hooks,
                                                &cfg.audit,
                                                FailureContext {
                                                    run_id: run_id.clone(),
                                                    turn,
                                                    stage: FailureStage::PostToolUse,
                                                    tool_use_id: Some(call.id.clone()),
                                                    tool: Some(call.name.clone()),
                                                    error,
                                                },
                                                false,
                                            ).await;
                                            if let Some(error) = failure.audit_error {
                                                terminal_reason = "audit_failure".into();
                                                yield StreamDelta::from_error(&error);
                                                break 'agent;
                                            }
                                            (failure.message, true, Some(effective))
                                        }
                                    }
                                }
                                Err(error) => {
                                    let failure = run_failure_hooks(
                                        &cfg.governance.hooks,
                                        &cfg.audit,
                                        FailureContext {
                                            run_id: run_id.clone(),
                                            turn,
                                            stage: FailureStage::ToolExecution,
                                            tool_use_id: Some(call.id.clone()),
                                            tool: Some(call.name.clone()),
                                            error: error.to_string(),
                                        },
                                        false,
                                    ).await;
                                    if let Some(error) = failure.audit_error {
                                        terminal_reason = "audit_failure".into();
                                        yield StreamDelta::from_error(&error);
                                        break 'agent;
                                    }
                                    (failure.message, true, Some(effective))
                                }
                            }
                        }
                    }
                };

                if content.len() > MAX_SINGLE_TOOL_RESULT_BYTES {
                    content = format!(
                        "tool '{}' post-hook output exceeded {} bytes and was discarded",
                        call.name, MAX_SINGLE_TOOL_RESULT_BYTES
                    );
                    is_error = true;
                }
                if let Err(message) = retained_output.charge_tool_result(&call.id, &content) {
                    terminal_reason = "retained_output_limit".into();
                    yield StreamDelta::error_with_info(
                        message,
                        crate::error::ErrorInfo::new(
                            crate::error::ErrorCode::ToolExecution,
                        ),
                    );
                    break 'agent;
                }

                if effective_input.is_some() {
                    if let Err(error) = cfg.audit.emit(AuditEvent::ToolCompleted {
                        turn,
                        tool_use_id: call.id.clone(),
                        tool: call.name.clone(),
                        is_error,
                        output_bytes: content.len(),
                        output_preview: cfg.audit.capture_output(&content),
                    }) {
                        // The side effect may already have happened. Never retry it or advance to
                        // another tool/model turn when fail-closed evidence cannot be written.
                        terminal_reason = "audit_failure".into();
                        yield StreamDelta::from_error(&error);
                        break 'agent;
                    }
                }
                yield StreamDelta::ToolResult {
                    tool_use_id: call.id.clone(),
                    content: content.clone(),
                    is_error,
                };
                result_blocks.push(ContentBlock::ToolResult {
                    tool_use_id: call.id,
                    content,
                    is_error,
                });
            }
            let tool_message = Message {
                role: Role::Tool,
                content: result_blocks,
            };
            cfg.messages.push(tool_message.clone());
            cfg.recorder.append_message(tool_message);
            // ...and loop for the next turn.
        }
        }

        if terminal_reason == "budget_exceeded" && !budget_error_emitted {
            let failure = run_failure_hooks(
                &cfg.governance.hooks,
                &cfg.audit,
                FailureContext {
                    run_id: run_id.clone(),
                    turn: turn.min(cfg.max_turns),
                    stage: FailureStage::Budget,
                    tool_use_id: None,
                    tool: None,
                    error: "shared wall-time budget deadline exceeded".into(),
                },
                true,
            )
            .await;
            let info = failure.audit_error.as_ref().map_or_else(
                || crate::error::ErrorInfo::new(crate::error::ErrorCode::BudgetExceeded),
                crate::error::AikitError::info,
            );
            if failure.audit_error.is_some() {
                terminal_reason = "audit_failure".into();
            }
            yield StreamDelta::error_with_info(failure.message, info);
        }

        if terminal_reason == "cancelled" {
            let error = crate::error::AikitError::Cancelled("run cancellation requested".into());
            yield StreamDelta::from_error(&error);
        }

        if let Err(error) = cfg.governance.clear_run_permissions(&run_id) {
            terminal_reason = "governance_cleanup_failure".into();
            yield StreamDelta::from_error(&error);
        }

        if let Err(error) = cfg.governance.hooks.run_stop(StopContext {
            run_id: run_id.clone(),
            turns: turn.min(cfg.max_turns),
            reason: terminal_reason.clone(),
            usage: total_usage,
        }).await {
            terminal_reason = "hook_failure".into();
            yield StreamDelta::from_error(&error);
        }
        if let Err(error) = cfg.audit.emit(AuditEvent::RunStopped {
            turns: turn.min(cfg.max_turns),
            reason: terminal_reason.clone(),
        }) {
            terminal_reason = "audit_failure".into();
            yield StreamDelta::from_error(&error);
        }
        let status = match terminal_reason.as_str() {
            "end_turn" | "stop" => crate::session::RunTerminalStatus::Completed,
            "budget_exceeded" | "budget_configuration_error" => {
                crate::session::RunTerminalStatus::BudgetExceeded
            }
            "max_turns" => crate::session::RunTerminalStatus::MaxTurns,
            "approval_interrupted" | "cancelled" => {
                crate::session::RunTerminalStatus::Cancelled
            }
            _ => crate::session::RunTerminalStatus::Failed,
        };
        cfg.recorder
            .complete(total_usage, status, terminal_reason);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::providers::{Provider, ProviderRequest};
    use crate::tools::ToolExecutor;
    use crate::types::ToolSpec;
    use async_trait::async_trait;
    use futures::stream::BoxStream;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;

    /// Turn 1 emits a completed reasoning block + a tool call; turn 2 records whether the loop
    /// replayed that reasoning block back into the request (regression for audit finding #1).
    struct ReasoningProvider {
        saw_reasoning_on_turn2: Arc<AtomicBool>,
    }

    #[async_trait]
    impl Provider for ReasoningProvider {
        fn name(&self) -> &str {
            "reasoning-mock"
        }

        async fn stream(
            &self,
            req: ProviderRequest,
        ) -> crate::error::Result<BoxStream<'static, StreamDelta>> {
            let has_tool_result = req.messages.iter().any(|m| {
                m.content
                    .iter()
                    .any(|b| matches!(b, ContentBlock::ToolResult { .. }))
            });
            if has_tool_result {
                let has_reasoning = req.messages.iter().any(|m| {
                    m.content
                        .iter()
                        .any(|b| matches!(b, ContentBlock::Reasoning { .. }))
                });
                if has_reasoning {
                    self.saw_reasoning_on_turn2.store(true, Ordering::SeqCst);
                }
                Ok(Box::pin(futures::stream::iter(vec![
                    StreamDelta::TextDelta {
                        text: "done".into(),
                    },
                    StreamDelta::MessageStop {
                        stop_reason: "end_turn".into(),
                    },
                ])))
            } else {
                Ok(Box::pin(futures::stream::iter(vec![
                    StreamDelta::MessageStart { model: "m".into() },
                    StreamDelta::ReasoningComplete {
                        text: "thinking".into(),
                        signature: Some("sig".into()),
                        opaque: None,
                    },
                    StreamDelta::ToolCallStart {
                        id: "t1".into(),
                        name: "search".into(),
                    },
                    StreamDelta::ToolCallInput {
                        id: "t1".into(),
                        input: serde_json::json!({ "q": "x" }),
                    },
                    StreamDelta::MessageStop {
                        stop_reason: "tool_use".into(),
                    },
                ])))
            }
        }
    }

    struct EchoTool;

    #[async_trait]
    impl ToolExecutor for EchoTool {
        async fn execute(
            &self,
            _name: &str,
            _input: serde_json::Value,
        ) -> crate::error::Result<String> {
            Ok("ok".into())
        }
    }

    struct StartupFailureProvider;

    #[async_trait]
    impl Provider for StartupFailureProvider {
        fn name(&self) -> &str {
            "openai"
        }

        async fn stream(
            &self,
            req: ProviderRequest,
        ) -> crate::error::Result<BoxStream<'static, StreamDelta>> {
            Err(crate::error::ProviderError::from_http(
                "openai",
                req.model,
                429,
                Some(1_500),
                "Authorization: Bearer sk-secret",
            )
            .into())
        }
    }

    #[tokio::test]
    async fn provider_start_error_keeps_safe_typed_metadata_without_raw_body() {
        let stream = run_agent(
            Arc::new(StartupFailureProvider),
            Arc::new(EchoTool),
            RunConfig::new("gpt-test", vec![Message::user("hi")]),
        );
        futures::pin_mut!(stream);
        let mut observed = None;
        while let Some(delta) = stream.next().await {
            if let StreamDelta::Error { message, info } = delta {
                observed = Some((message, info));
            }
        }
        let (message, info) = observed.unwrap();
        assert!(!message.contains("sk-secret"));
        assert_eq!(info.code, crate::error::ErrorCode::ProviderRateLimit);
        assert_eq!(info.provider.as_deref(), Some("openai"));
        assert_eq!(info.model.as_deref(), Some("gpt-test"));
        assert_eq!(info.status, Some(429));
        assert_eq!(info.retry_after_ms, Some(1_500));
        assert!(info.retryable);
    }

    struct FailOnAuditEvent(&'static str);

    impl crate::observability::AuditSink for FailOnAuditEvent {
        fn record(
            &self,
            record: &crate::observability::AuditRecord,
        ) -> std::result::Result<(), String> {
            let matches = match (&record.event, self.0) {
                (crate::observability::AuditEvent::Usage { .. }, "usage") => true,
                (crate::observability::AuditEvent::HookCompleted { phase, .. }, "post_hook") => {
                    phase == "post_tool_use"
                }
                (crate::observability::AuditEvent::RunStopped { .. }, "run_stopped") => true,
                _ => false,
            };
            if matches {
                Err(format!("planned {} audit failure", self.0))
            } else {
                Ok(())
            }
        }
    }

    fn fail_closed_on(event: &'static str) -> crate::observability::AuditTrail {
        crate::observability::AuditTrail::new()
            .with_sink(Arc::new(FailOnAuditEvent(event)))
            .with_failure_mode(crate::observability::AuditFailureMode::FailClosed)
    }

    #[tokio::test]
    async fn reasoning_is_persisted_and_replayed_to_the_next_turn() {
        let flag = Arc::new(AtomicBool::new(false));
        let provider: Arc<dyn Provider> = Arc::new(ReasoningProvider {
            saw_reasoning_on_turn2: flag.clone(),
        });
        let executor: Arc<dyn ToolExecutor> = Arc::new(EchoTool);

        let mut cfg = RunConfig::new("m", vec![Message::user("hi")]);
        cfg.tools = vec![ToolSpec {
            name: "search".into(),
            description: "s".into(),
            input_schema: serde_json::json!({ "type": "object" }),
        }];

        let stream = run_agent(provider, executor, cfg);
        futures::pin_mut!(stream);
        while stream.next().await.is_some() {}

        assert!(
            flag.load(Ordering::SeqCst),
            "reasoning block was NOT replayed into the turn-2 request (finding #1 regression)"
        );
    }

    struct MetadataProvider;

    #[async_trait]
    impl Provider for MetadataProvider {
        fn name(&self) -> &str {
            "metadata-mock"
        }

        async fn stream(
            &self,
            _req: ProviderRequest,
        ) -> crate::error::Result<BoxStream<'static, StreamDelta>> {
            Ok(Box::pin(futures::stream::iter(vec![
                StreamDelta::MessageStart {
                    model: "metadata-model".into(),
                },
                StreamDelta::ProviderMetadata {
                    provider: "metadata-mock".into(),
                    metadata: serde_json::json!({ "finish_reason": "stop" }),
                },
                StreamDelta::ProviderMetadata {
                    provider: "metadata-mock".into(),
                    metadata: serde_json::json!({
                        "usage": { "prompt_cache_hit_tokens": 11 }
                    }),
                },
                StreamDelta::TextDelta {
                    text: "done".into(),
                },
                StreamDelta::MessageStop {
                    stop_reason: "end_turn".into(),
                },
            ])))
        }
    }

    #[tokio::test]
    async fn provider_metadata_is_forwarded_and_aggregated_without_overwrite() {
        let recorder = crate::session::RunRecorder::default();
        let mut cfg = RunConfig::new("metadata-model", vec![Message::user("hi")]);
        cfg.recorder = recorder.clone();
        let stream = run_agent(Arc::new(MetadataProvider), Arc::new(EchoTool), cfg);
        futures::pin_mut!(stream);
        let mut forwarded = Vec::new();
        while let Some(delta) = stream.next().await {
            if let StreamDelta::ProviderMetadata { provider, metadata } = delta {
                forwarded.push((provider, metadata));
            }
        }

        assert_eq!(forwarded.len(), 2);
        let outcome = recorder.outcome();
        assert_eq!(
            outcome.terminal_status,
            crate::session::RunTerminalStatus::Completed
        );
        let metadata = &outcome.provider_metadata["metadata-mock"];
        assert_eq!(metadata.len(), 2);
        assert_eq!(metadata[0]["finish_reason"], "stop");
        assert_eq!(metadata[1]["usage"]["prompt_cache_hit_tokens"], 11);
    }

    struct OversizedStreamProvider;

    #[async_trait]
    impl Provider for OversizedStreamProvider {
        fn name(&self) -> &str {
            "oversized-stream"
        }

        async fn stream(
            &self,
            _req: ProviderRequest,
        ) -> crate::error::Result<BoxStream<'static, StreamDelta>> {
            let deltas = std::iter::repeat_with(|| StreamDelta::TextDelta {
                text: "x".repeat(64 * 1024),
            })
            .take(18)
            .chain(std::iter::once(StreamDelta::MessageStop {
                stop_reason: "end_turn".into(),
            }));
            Ok(Box::pin(futures::stream::iter(deltas)))
        }
    }

    #[tokio::test]
    async fn retained_stream_output_is_bounded_even_when_provider_ignores_max_tokens() {
        let recorder = crate::session::RunRecorder::default();
        let mut cfg = RunConfig::new("hostile-model", vec![Message::user("hi")]);
        cfg.max_tokens = 1;
        cfg.recorder = recorder.clone();
        let stream = run_agent(Arc::new(OversizedStreamProvider), Arc::new(EchoTool), cfg);
        futures::pin_mut!(stream);

        let mut text_deltas = 0;
        let mut protocol_error = None;
        while let Some(delta) = stream.next().await {
            match delta {
                StreamDelta::TextDelta { .. } => text_deltas += 1,
                StreamDelta::Error { message, info } => {
                    protocol_error = Some((message, info));
                }
                _ => {}
            }
        }

        assert_eq!(
            text_deltas, 16,
            "the over-limit delta must not be forwarded"
        );
        let (message, info) = protocol_error.expect("limit emits a typed terminal error");
        assert!(message.contains("retained-output safety limit"));
        assert_eq!(info.code, crate::error::ErrorCode::ProviderProtocol);
        assert_eq!(info.provider.as_deref(), Some("oversized-stream"));
        let outcome = recorder.outcome();
        assert_eq!(
            outcome.terminal_status,
            crate::session::RunTerminalStatus::Failed
        );
        assert!(outcome.final_text.is_none());
    }

    #[test]
    fn retained_json_accounting_bounds_empty_nodes_and_depth() {
        let empty = serde_json::json!([]);
        assert!(retained_json_bytes(&empty) >= RETAINED_JSON_NODE_OVERHEAD);

        let many_empty = serde_json::Value::Array(
            (0..=MAX_RETAINED_JSON_NODES)
                .map(|_| serde_json::Value::Array(Vec::new()))
                .collect(),
        );
        assert_eq!(retained_json_bytes(&many_empty), usize::MAX);

        let mut deeply_nested = serde_json::Value::Null;
        for _ in 0..=MAX_RETAINED_JSON_DEPTH {
            deeply_nested = serde_json::Value::Array(vec![deeply_nested]);
        }
        assert_eq!(retained_json_bytes(&deeply_nested), usize::MAX);
    }

    /// Turn 1 asks for one tool call; turn 2 (once a tool result exists) finishes.
    struct SingleToolProvider {
        tool: String,
        input: serde_json::Value,
    }

    #[async_trait]
    impl Provider for SingleToolProvider {
        fn name(&self) -> &str {
            "single-tool"
        }
        async fn stream(
            &self,
            req: ProviderRequest,
        ) -> crate::error::Result<BoxStream<'static, StreamDelta>> {
            let has_tr = req.messages.iter().any(|m| {
                m.content
                    .iter()
                    .any(|b| matches!(b, ContentBlock::ToolResult { .. }))
            });
            let deltas = if has_tr {
                vec![
                    StreamDelta::TextDelta {
                        text: "done".into(),
                    },
                    StreamDelta::MessageStop {
                        stop_reason: "end_turn".into(),
                    },
                ]
            } else {
                vec![
                    StreamDelta::ToolCallStart {
                        id: "c1".into(),
                        name: self.tool.clone(),
                    },
                    StreamDelta::ToolCallInput {
                        id: "c1".into(),
                        input: self.input.clone(),
                    },
                    StreamDelta::MessageStop {
                        stop_reason: "tool_use".into(),
                    },
                ]
            };
            Ok(Box::pin(futures::stream::iter(deltas)))
        }
    }

    struct CountingSingleToolProvider {
        tool: String,
        input: serde_json::Value,
        requests: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl Provider for CountingSingleToolProvider {
        fn name(&self) -> &str {
            "counting-single-tool"
        }

        async fn stream(
            &self,
            req: ProviderRequest,
        ) -> crate::error::Result<BoxStream<'static, StreamDelta>> {
            self.requests.fetch_add(1, Ordering::SeqCst);
            SingleToolProvider {
                tool: self.tool.clone(),
                input: self.input.clone(),
            }
            .stream(req)
            .await
        }
    }

    struct RuntimeApprover {
        decision: crate::governance::ApprovalDecision,
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl crate::governance::ToolApprover for RuntimeApprover {
        async fn approve(
            &self,
            _request: crate::governance::ApprovalRequest,
        ) -> crate::governance::ApprovalDecision {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.decision.clone()
        }
    }

    struct RecordingExecutor {
        calls: Arc<std::sync::atomic::AtomicUsize>,
        last_input: Arc<std::sync::Mutex<serde_json::Value>>,
    }

    struct OversizedOutputTool;

    #[async_trait]
    impl ToolExecutor for OversizedOutputTool {
        async fn execute(
            &self,
            _name: &str,
            _input: serde_json::Value,
        ) -> crate::error::Result<String> {
            Ok("x".repeat(MAX_SINGLE_TOOL_RESULT_BYTES + 1))
        }
    }

    #[async_trait]
    impl ToolExecutor for RecordingExecutor {
        async fn execute(
            &self,
            _name: &str,
            input: serde_json::Value,
        ) -> crate::error::Result<String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            *self.last_input.lock().unwrap() = input;
            Ok("executed".into())
        }
    }

    fn advertised(name: &str) -> ToolSpec {
        ToolSpec {
            name: name.into(),
            description: "test tool".into(),
            input_schema: serde_json::json!({ "type": "object" }),
        }
    }

    #[tokio::test]
    async fn oversized_custom_tool_output_is_discarded_before_hooks_or_history_clone_it() {
        let mut cfg = RunConfig::new("m", vec![Message::user("hi")]);
        cfg.tools = vec![advertised("huge")];
        let stream = run_agent(
            Arc::new(SingleToolProvider {
                tool: "huge".into(),
                input: serde_json::json!({}),
            }),
            Arc::new(OversizedOutputTool),
            cfg,
        );
        futures::pin_mut!(stream);

        let mut tool_result = None;
        while let Some(delta) = stream.next().await {
            if let StreamDelta::ToolResult {
                content, is_error, ..
            } = delta
            {
                tool_result = Some((content, is_error));
            }
        }

        let (content, is_error) = tool_result.expect("tool result is surfaced");
        assert!(is_error);
        assert!(content.contains("output exceeded"));
        assert!(content.len() < 256);
    }

    fn strict_path_tool(name: &str) -> ToolSpec {
        ToolSpec {
            name: name.into(),
            description: "strict path tool".into(),
            input_schema: serde_json::json!({
                "type": "object",
                "required": ["path"],
                "additionalProperties": false,
                "properties": {
                    "path": { "type": "string", "minLength": 1 }
                }
            }),
        }
    }

    #[tokio::test]
    async fn invalid_tool_schema_fails_before_provider_or_executor() {
        let provider_requests = Arc::new(AtomicUsize::new(0));
        let executor_calls = Arc::new(AtomicUsize::new(0));
        let provider: Arc<dyn Provider> = Arc::new(CountingSingleToolProvider {
            tool: "Write".into(),
            input: serde_json::json!({ "path": "a.txt" }),
            requests: provider_requests.clone(),
        });
        let executor: Arc<dyn ToolExecutor> = Arc::new(RecordingExecutor {
            calls: executor_calls.clone(),
            last_input: Arc::new(std::sync::Mutex::new(serde_json::Value::Null)),
        });
        let recorder = crate::session::RunRecorder::default();
        let mut cfg = RunConfig::new("m", vec![Message::user("hi")]);
        cfg.recorder = recorder.clone();
        cfg.tools = vec![ToolSpec {
            name: "Write".into(),
            description: "invalid schema".into(),
            input_schema: serde_json::json!({ "type": 7 }),
        }];

        let stream = run_agent(provider, executor, cfg);
        futures::pin_mut!(stream);
        let mut error_code = None;
        while let Some(delta) = stream.next().await {
            if let StreamDelta::Error { info, .. } = delta {
                error_code = Some(info.code);
            }
        }

        assert_eq!(error_code, Some(crate::error::ErrorCode::Configuration));
        assert_eq!(provider_requests.load(Ordering::SeqCst), 0);
        assert_eq!(executor_calls.load(Ordering::SeqCst), 0);
        let outcome = recorder.outcome();
        assert_eq!(
            outcome.terminal_status,
            crate::session::RunTerminalStatus::Failed
        );
        assert_eq!(outcome.stop_reason.as_deref(), Some("configuration_error"));
    }

    #[tokio::test]
    async fn model_tool_input_is_schema_checked_before_governance() {
        use crate::governance::hooks::HookDispatcher;
        use crate::governance::permissions::{PermissionEngine, PermissionMode, Rule};
        use crate::governance::{ApprovalDecision, Governance};
        use crate::observability::{AuditEvent, AuditTrail, InMemoryAuditSink};

        let executor_calls = Arc::new(AtomicUsize::new(0));
        let approval_calls = Arc::new(AtomicUsize::new(0));
        let audit_sink = Arc::new(InMemoryAuditSink::default());
        let provider: Arc<dyn Provider> = Arc::new(SingleToolProvider {
            tool: "Write".into(),
            input: serde_json::json!({ "path": 42 }),
        });
        let executor: Arc<dyn ToolExecutor> = Arc::new(RecordingExecutor {
            calls: executor_calls.clone(),
            last_input: Arc::new(std::sync::Mutex::new(serde_json::Value::Null)),
        });
        let mut cfg = RunConfig::new("m", vec![Message::user("hi")]);
        cfg.tools = vec![strict_path_tool("Write")];
        cfg.audit = AuditTrail::new().with_sink(audit_sink.clone());
        cfg.governance = Governance::new(
            PermissionEngine::with_rules(PermissionMode::Allow, vec![Rule::ask("Write")]),
            HookDispatcher::new(),
        )
        .with_approver(Arc::new(RuntimeApprover {
            decision: ApprovalDecision::allow(None),
            calls: approval_calls.clone(),
        }));

        let stream = run_agent(provider, executor, cfg);
        futures::pin_mut!(stream);
        let mut rejected = None;
        while let Some(delta) = stream.next().await {
            if let StreamDelta::ToolResult {
                content,
                is_error: true,
                ..
            } = delta
            {
                rejected = Some(content);
            }
        }

        assert!(rejected
            .as_deref()
            .is_some_and(|message| message.contains("JSON Schema validation")));
        assert_eq!(executor_calls.load(Ordering::SeqCst), 0);
        assert_eq!(approval_calls.load(Ordering::SeqCst), 0);
        let records = audit_sink.records();
        assert!(records.iter().any(|record| matches!(
            &record.event,
            AuditEvent::Failure { stage, .. } if stage == "tool_input_validation"
        )));
        assert!(records.iter().any(|record| matches!(
            &record.event,
            AuditEvent::PermissionDecision { decision, source, .. }
                if decision == "deny" && source == "runtime:tool_input_schema"
        )));
        assert!(!records
            .iter()
            .any(|record| matches!(record.event, AuditEvent::ToolStarted { .. })));
    }

    #[tokio::test]
    async fn hook_rewrite_is_schema_checked_again_before_executor() {
        use crate::governance::hooks::{HookDispatcher, HookMatcher, HookOutcome};
        use crate::governance::Governance;
        use crate::observability::{AuditEvent, AuditTrail, InMemoryAuditSink};

        let executor_calls = Arc::new(AtomicUsize::new(0));
        let audit_sink = Arc::new(InMemoryAuditSink::default());
        let provider: Arc<dyn Provider> = Arc::new(SingleToolProvider {
            tool: "Write".into(),
            input: serde_json::json!({ "path": "safe.txt" }),
        });
        let executor: Arc<dyn ToolExecutor> = Arc::new(RecordingExecutor {
            calls: executor_calls.clone(),
            last_input: Arc::new(std::sync::Mutex::new(serde_json::Value::Null)),
        });
        let mut hooks = HookDispatcher::new();
        hooks.on_pre_tool_use(HookMatcher::tool("Write"), |_tool, _input| {
            HookOutcome::Rewrite(serde_json::json!({ "path": 42 }))
        });
        let mut cfg = RunConfig::new("m", vec![Message::user("hi")]);
        cfg.tools = vec![strict_path_tool("Write")];
        cfg.audit = AuditTrail::new().with_sink(audit_sink.clone());
        cfg.governance = Governance::new(Default::default(), hooks);

        let stream = run_agent(provider, executor, cfg);
        futures::pin_mut!(stream);
        while stream.next().await.is_some() {}

        assert_eq!(executor_calls.load(Ordering::SeqCst), 0);
        assert!(audit_sink.records().iter().any(|record| matches!(
            &record.event,
            AuditEvent::PermissionDecision { decision, source, .. }
                if decision == "deny" && source == "runtime:effective_tool_input_schema"
        )));
    }

    #[tokio::test]
    async fn approval_rewrite_is_schema_checked_again_before_executor() {
        use crate::governance::hooks::HookDispatcher;
        use crate::governance::permissions::{PermissionEngine, PermissionMode, Rule};
        use crate::governance::{ApprovalDecision, Governance};
        use crate::observability::{AuditEvent, AuditTrail, InMemoryAuditSink};

        let executor_calls = Arc::new(AtomicUsize::new(0));
        let approval_calls = Arc::new(AtomicUsize::new(0));
        let audit_sink = Arc::new(InMemoryAuditSink::default());
        let provider: Arc<dyn Provider> = Arc::new(SingleToolProvider {
            tool: "Write".into(),
            input: serde_json::json!({ "path": "safe.txt" }),
        });
        let executor: Arc<dyn ToolExecutor> = Arc::new(RecordingExecutor {
            calls: executor_calls.clone(),
            last_input: Arc::new(std::sync::Mutex::new(serde_json::Value::Null)),
        });
        let mut cfg = RunConfig::new("m", vec![Message::user("hi")]);
        cfg.tools = vec![strict_path_tool("Write")];
        cfg.audit = AuditTrail::new().with_sink(audit_sink.clone());
        cfg.governance = Governance::new(
            PermissionEngine::with_rules(PermissionMode::Allow, vec![Rule::ask("Write")]),
            HookDispatcher::new(),
        )
        .with_approver(Arc::new(RuntimeApprover {
            decision: ApprovalDecision::allow(Some(serde_json::json!({ "path": 42 }))),
            calls: approval_calls.clone(),
        }));

        let stream = run_agent(provider, executor, cfg);
        futures::pin_mut!(stream);
        while stream.next().await.is_some() {}

        assert_eq!(approval_calls.load(Ordering::SeqCst), 1);
        assert_eq!(executor_calls.load(Ordering::SeqCst), 0);
        assert!(audit_sink.records().iter().any(|record| matches!(
            &record.event,
            AuditEvent::PermissionDecision { decision, source, .. }
                if decision == "deny" && source == "runtime:effective_tool_input_schema"
        )));
    }

    #[tokio::test]
    async fn denied_tool_is_not_executed_and_returns_an_error_result() {
        use crate::governance::permissions::{PermissionEngine, PermissionMode, Rule};
        use crate::governance::{hooks::HookDispatcher, Governance};

        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let provider: Arc<dyn Provider> = Arc::new(SingleToolProvider {
            tool: "Bash".into(),
            input: serde_json::json!({ "command": "rm -rf /" }),
        });
        let executor: Arc<dyn ToolExecutor> = Arc::new(RecordingExecutor {
            calls: calls.clone(),
            last_input: Arc::new(std::sync::Mutex::new(serde_json::Value::Null)),
        });

        let mut cfg = RunConfig::new("m", vec![Message::user("hi")]);
        cfg.tools = vec![advertised("Bash")];
        cfg.governance = Governance::new(
            PermissionEngine::with_rules(
                PermissionMode::Allow,
                vec![Rule::deny("Bash").matching(r"rm\s+-rf").unwrap()],
            ),
            HookDispatcher::new(),
        );

        let stream = run_agent(provider, executor, cfg);
        futures::pin_mut!(stream);
        let mut saw_denial = false;
        while let Some(d) = stream.next().await {
            if let StreamDelta::ToolResult {
                is_error, content, ..
            } = &d
            {
                if *is_error && content.contains("denied") {
                    saw_denial = true;
                }
            }
        }
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "a denied tool must NEVER reach the executor"
        );
        assert!(
            saw_denial,
            "the model must receive a denial error tool-result"
        );
    }

    #[tokio::test]
    async fn interrupting_approval_denial_stops_the_run_without_tool_or_model_replay() {
        use crate::governance::hooks::{FailureStage, HookDispatcher};
        use crate::governance::permissions::{PermissionEngine, PermissionMode, Rule};
        use crate::governance::{ApprovalDecision, Governance};
        use crate::observability::{AuditEvent, AuditTrail, InMemoryAuditSink};
        use crate::session::RunTerminalStatus;

        let executor_calls = Arc::new(AtomicUsize::new(0));
        let provider_requests = Arc::new(AtomicUsize::new(0));
        let approval_calls = Arc::new(AtomicUsize::new(0));
        let failure_calls = Arc::new(AtomicUsize::new(0));
        let stop_reasons = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let audit_sink = Arc::new(InMemoryAuditSink::default());
        let recorder = crate::session::RunRecorder::default();

        let mut hooks = HookDispatcher::new();
        let seen_failures = failure_calls.clone();
        hooks.on_failure(move |ctx| {
            assert_eq!(ctx.stage, FailureStage::Permission);
            assert_eq!(ctx.error, "operator stopped the run");
            seen_failures.fetch_add(1, Ordering::SeqCst);
            crate::governance::hooks::FailureHookOutcome::Continue
        });
        let seen_stops = stop_reasons.clone();
        hooks.on_stop(move |ctx| {
            seen_stops.lock().unwrap().push(ctx.reason.clone());
        });

        let provider: Arc<dyn Provider> = Arc::new(CountingSingleToolProvider {
            tool: "Bash".into(),
            input: serde_json::json!({ "command": "git push" }),
            requests: provider_requests.clone(),
        });
        let executor: Arc<dyn ToolExecutor> = Arc::new(RecordingExecutor {
            calls: executor_calls.clone(),
            last_input: Arc::new(std::sync::Mutex::new(serde_json::Value::Null)),
        });
        let mut cfg = RunConfig::new("m", vec![Message::user("hi")]);
        cfg.tools = vec![advertised("Bash")];
        cfg.audit = AuditTrail::new().with_sink(audit_sink.clone());
        cfg.recorder = recorder.clone();
        cfg.governance = Governance::new(
            PermissionEngine::with_rules(
                PermissionMode::Allow,
                vec![Rule::ask("Bash").named("ask-bash")],
            ),
            hooks,
        )
        .with_approver(Arc::new(RuntimeApprover {
            decision: ApprovalDecision::Deny {
                message: "operator stopped the run".into(),
                interrupt: true,
            },
            calls: approval_calls.clone(),
        }));

        let stream = run_agent(provider, executor, cfg);
        futures::pin_mut!(stream);
        let mut saw_terminal_error = false;
        let mut saw_tool_result = false;
        while let Some(delta) = stream.next().await {
            match delta {
                StreamDelta::Error { message, .. } if message == "operator stopped the run" => {
                    saw_terminal_error = true;
                }
                StreamDelta::ToolResult { .. } => saw_tool_result = true,
                _ => {}
            }
        }

        assert!(saw_terminal_error);
        assert!(
            !saw_tool_result,
            "an interrupt must not synthesize a tool result"
        );
        assert_eq!(executor_calls.load(Ordering::SeqCst), 0);
        assert_eq!(provider_requests.load(Ordering::SeqCst), 1);
        assert_eq!(approval_calls.load(Ordering::SeqCst), 1);
        assert_eq!(failure_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            stop_reasons.lock().unwrap().as_slice(),
            &["approval_interrupted"]
        );

        let outcome = recorder.outcome();
        assert_eq!(outcome.terminal_status, RunTerminalStatus::Cancelled);
        assert_eq!(outcome.stop_reason.as_deref(), Some("approval_interrupted"));

        let records = audit_sink.records();
        assert!(records.iter().any(|record| matches!(
            &record.event,
            AuditEvent::PermissionDecision { decision, source, .. }
                if decision == "ask_denied_interrupt"
                    && source == "human_approval:ask-bash"
        )));
        assert!(records.iter().any(|record| matches!(
            &record.event,
            AuditEvent::Failure { stage, terminal: true, .. } if stage == "permission"
        )));
        assert!(records.iter().any(|record| matches!(
            &record.event,
            AuditEvent::RunStopped { reason, .. } if reason == "approval_interrupted"
        )));
        assert!(!records
            .iter()
            .any(|record| matches!(record.event, AuditEvent::ToolStarted { .. })));
    }

    #[tokio::test]
    async fn non_interrupting_approval_denial_keeps_error_tool_result_behavior() {
        use crate::governance::hooks::HookDispatcher;
        use crate::governance::permissions::{PermissionEngine, PermissionMode, Rule};
        use crate::governance::{ApprovalDecision, Governance};
        use crate::session::RunTerminalStatus;

        let executor_calls = Arc::new(AtomicUsize::new(0));
        let provider_requests = Arc::new(AtomicUsize::new(0));
        let approval_calls = Arc::new(AtomicUsize::new(0));
        let recorder = crate::session::RunRecorder::default();
        let provider: Arc<dyn Provider> = Arc::new(CountingSingleToolProvider {
            tool: "Bash".into(),
            input: serde_json::json!({ "command": "git push" }),
            requests: provider_requests.clone(),
        });
        let executor: Arc<dyn ToolExecutor> = Arc::new(RecordingExecutor {
            calls: executor_calls.clone(),
            last_input: Arc::new(std::sync::Mutex::new(serde_json::Value::Null)),
        });
        let mut cfg = RunConfig::new("m", vec![Message::user("hi")]);
        cfg.tools = vec![advertised("Bash")];
        cfg.recorder = recorder.clone();
        cfg.governance = Governance::new(
            PermissionEngine::with_rules(
                PermissionMode::Allow,
                vec![Rule::ask("Bash").named("ask-bash")],
            ),
            HookDispatcher::new(),
        )
        .with_approver(Arc::new(RuntimeApprover {
            decision: ApprovalDecision::Deny {
                message: "not approved".into(),
                interrupt: false,
            },
            calls: approval_calls.clone(),
        }));

        let stream = run_agent(provider, executor, cfg);
        futures::pin_mut!(stream);
        let mut denial_result = None;
        let mut saw_terminal_error = false;
        while let Some(delta) = stream.next().await {
            match delta {
                StreamDelta::ToolResult {
                    content,
                    is_error: true,
                    ..
                } => denial_result = Some(content),
                StreamDelta::Error { .. } => saw_terminal_error = true,
                _ => {}
            }
        }

        assert_eq!(denial_result.as_deref(), Some("not approved"));
        assert!(!saw_terminal_error);
        assert_eq!(executor_calls.load(Ordering::SeqCst), 0);
        assert_eq!(provider_requests.load(Ordering::SeqCst), 2);
        assert_eq!(approval_calls.load(Ordering::SeqCst), 1);
        let outcome = recorder.outcome();
        assert_eq!(outcome.terminal_status, RunTerminalStatus::Completed);
        assert_eq!(outcome.stop_reason.as_deref(), Some("end_turn"));
    }

    #[tokio::test]
    async fn fail_closed_audit_error_takes_precedence_over_an_approval_interrupt() {
        use crate::governance::hooks::HookDispatcher;
        use crate::governance::permissions::{PermissionEngine, PermissionMode, Rule};
        use crate::governance::{ApprovalDecision, Governance};
        use crate::observability::{
            AuditEvent, AuditFailureMode, AuditRecord, AuditSink, AuditTrail,
        };
        use crate::session::RunTerminalStatus;

        struct FailPreHookAudit;
        impl AuditSink for FailPreHookAudit {
            fn record(&self, record: &AuditRecord) -> std::result::Result<(), String> {
                if matches!(record.event, AuditEvent::HookCompleted { .. }) {
                    Err("hook audit unavailable".into())
                } else {
                    Ok(())
                }
            }
        }

        let executor_calls = Arc::new(AtomicUsize::new(0));
        let provider_requests = Arc::new(AtomicUsize::new(0));
        let recorder = crate::session::RunRecorder::default();
        let provider: Arc<dyn Provider> = Arc::new(CountingSingleToolProvider {
            tool: "Bash".into(),
            input: serde_json::json!({ "command": "git push" }),
            requests: provider_requests.clone(),
        });
        let executor: Arc<dyn ToolExecutor> = Arc::new(RecordingExecutor {
            calls: executor_calls.clone(),
            last_input: Arc::new(std::sync::Mutex::new(serde_json::Value::Null)),
        });
        let mut cfg = RunConfig::new("m", vec![Message::user("hi")]);
        cfg.tools = vec![advertised("Bash")];
        cfg.recorder = recorder.clone();
        cfg.audit = AuditTrail::new()
            .with_sink(Arc::new(FailPreHookAudit))
            .with_failure_mode(AuditFailureMode::FailClosed);
        cfg.governance = Governance::new(
            PermissionEngine::with_rules(PermissionMode::Allow, vec![Rule::ask("Bash")]),
            HookDispatcher::new(),
        )
        .with_approver(Arc::new(RuntimeApprover {
            decision: ApprovalDecision::Deny {
                message: "operator stopped the run".into(),
                interrupt: true,
            },
            calls: Arc::new(AtomicUsize::new(0)),
        }));

        let stream = run_agent(provider, executor, cfg);
        futures::pin_mut!(stream);
        let mut error = None;
        let mut saw_tool_result = false;
        while let Some(delta) = stream.next().await {
            match delta {
                StreamDelta::Error { message, info } => error = Some((message, info)),
                StreamDelta::ToolResult { .. } => saw_tool_result = true,
                _ => {}
            }
        }

        let (message, info) = error.expect("fail-closed audit must surface a terminal error");
        assert!(message.contains("hook audit unavailable"));
        assert_eq!(info.code, crate::error::ErrorCode::Audit);
        assert!(!saw_tool_result);
        assert_eq!(executor_calls.load(Ordering::SeqCst), 0);
        assert_eq!(provider_requests.load(Ordering::SeqCst), 1);
        let outcome = recorder.outcome();
        assert_eq!(outcome.terminal_status, RunTerminalStatus::Failed);
        assert_eq!(outcome.stop_reason.as_deref(), Some("audit_failure"));
    }

    #[tokio::test]
    async fn hook_rewrite_reaches_the_executor() {
        use crate::governance::hooks::{HookDispatcher, HookMatcher, HookOutcome};
        use crate::governance::{permissions::PermissionEngine, Governance};

        let last = Arc::new(std::sync::Mutex::new(serde_json::Value::Null));
        let provider: Arc<dyn Provider> = Arc::new(SingleToolProvider {
            tool: "Write".into(),
            input: serde_json::json!({ "path": "a.txt" }),
        });
        let executor: Arc<dyn ToolExecutor> = Arc::new(RecordingExecutor {
            calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            last_input: last.clone(),
        });

        let mut hooks = HookDispatcher::new();
        hooks.on_pre_tool_use(HookMatcher::any(), |_t, input| {
            let mut v = input.clone();
            v["cwd"] = serde_json::json!("/workspace");
            HookOutcome::Rewrite(v)
        });

        let mut cfg = RunConfig::new("m", vec![Message::user("hi")]);
        cfg.tools = vec![advertised("Write")];
        cfg.governance = Governance::new(PermissionEngine::default(), hooks);

        let stream = run_agent(provider, executor, cfg);
        futures::pin_mut!(stream);
        while stream.next().await.is_some() {}

        let got = last.lock().unwrap().clone();
        assert_eq!(
            got["cwd"], "/workspace",
            "the hook rewrite must reach the executor"
        );
        assert_eq!(got["path"], "a.txt");
    }

    #[tokio::test]
    async fn post_hook_rewrite_is_the_result_returned_to_the_model() {
        use crate::governance::hooks::{HookDispatcher, HookMatcher, PostToolOutcome};
        use crate::governance::Governance;

        let provider: Arc<dyn Provider> = Arc::new(SingleToolProvider {
            tool: "Search".into(),
            input: serde_json::json!({ "q": "secret" }),
        });
        let executor: Arc<dyn ToolExecutor> = Arc::new(RecordingExecutor {
            calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            last_input: Arc::new(std::sync::Mutex::new(serde_json::Value::Null)),
        });
        let mut hooks = HookDispatcher::new();
        hooks.on_post_tool_use(HookMatcher::tool("Search"), |_tool, _input, output| {
            PostToolOutcome::RewriteOutput(format!("filtered:{output}"))
        });
        let mut cfg = RunConfig::new("m", vec![Message::user("hi")]);
        cfg.tools = vec![advertised("Search")];
        cfg.governance = Governance::new(Default::default(), hooks);

        let stream = run_agent(provider, executor, cfg);
        futures::pin_mut!(stream);
        let mut result = None;
        while let Some(delta) = stream.next().await {
            if let StreamDelta::ToolResult { content, .. } = delta {
                result = Some(content);
            }
        }
        assert_eq!(result.as_deref(), Some("filtered:executed"));
    }

    struct IncompleteToolProvider;

    #[async_trait]
    impl Provider for IncompleteToolProvider {
        fn name(&self) -> &str {
            "incomplete"
        }

        async fn stream(
            &self,
            _req: ProviderRequest,
        ) -> crate::error::Result<BoxStream<'static, StreamDelta>> {
            Ok(Box::pin(futures::stream::iter(vec![
                StreamDelta::ToolCallStart {
                    id: "bad".into(),
                    name: "Bash".into(),
                },
                StreamDelta::Error {
                    message: "malformed tool arguments".into(),
                    info: crate::error::ErrorInfo::new(crate::error::ErrorCode::ProviderProtocol),
                },
                StreamDelta::MessageStop {
                    stop_reason: "tool_use".into(),
                },
            ])))
        }
    }

    #[tokio::test]
    async fn malformed_tool_call_never_reaches_executor() {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let executor: Arc<dyn ToolExecutor> = Arc::new(RecordingExecutor {
            calls: calls.clone(),
            last_input: Arc::new(std::sync::Mutex::new(serde_json::Value::Null)),
        });
        let stream = run_agent(
            Arc::new(IncompleteToolProvider),
            executor,
            RunConfig::new("m", vec![Message::user("hi")]),
        );
        futures::pin_mut!(stream);
        while stream.next().await.is_some() {}
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn unadvertised_tool_never_reaches_executor() {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let executor: Arc<dyn ToolExecutor> = Arc::new(RecordingExecutor {
            calls: calls.clone(),
            last_input: Arc::new(std::sync::Mutex::new(serde_json::Value::Null)),
        });
        let provider: Arc<dyn Provider> = Arc::new(SingleToolProvider {
            tool: "Hidden".into(),
            input: serde_json::json!({ "secret": true }),
        });
        let mut cfg = RunConfig::new("m", vec![Message::user("hi")]);
        cfg.tools = vec![advertised("Visible")];

        let stream = run_agent(provider, executor, cfg);
        futures::pin_mut!(stream);
        let mut saw_denial = false;
        while let Some(delta) = stream.next().await {
            if matches!(
                delta,
                StreamDelta::ToolResult {
                    is_error: true,
                    ref content,
                    ..
                } if content.contains("not advertised")
            ) {
                saw_denial = true;
            }
        }
        assert!(saw_denial);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn stop_hook_runs_once_and_receives_terminal_usage() {
        use crate::governance::hooks::HookDispatcher;
        use crate::governance::Governance;

        let stops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let seen_usage = Arc::new(std::sync::Mutex::new(Usage::default()));
        let mut hooks = HookDispatcher::new();
        let stops_hook = stops.clone();
        let usage_hook = seen_usage.clone();
        hooks.on_stop(move |ctx| {
            stops_hook.fetch_add(1, Ordering::SeqCst);
            *usage_hook.lock().unwrap() = ctx.usage;
        });
        let mut cfg = RunConfig::new("mock-1", vec![Message::user("hi")]);
        cfg.governance = Governance::new(Default::default(), hooks);
        let stream = run_agent(
            Arc::new(crate::providers::MockProvider),
            Arc::new(EchoTool),
            cfg,
        );
        futures::pin_mut!(stream);
        while stream.next().await.is_some() {}
        assert_eq!(stops.load(Ordering::SeqCst), 1);
        assert_eq!(seen_usage.lock().unwrap().output_tokens, 9);
    }

    #[tokio::test]
    async fn audit_records_governed_lifecycle_without_payloads_by_default() {
        use crate::observability::{AuditEvent, AuditTrail, InMemoryAuditSink};

        let sink = Arc::new(InMemoryAuditSink::default());
        let mut cfg = RunConfig::new("m", vec![Message::user("hi")]);
        cfg.tools = vec![advertised("Search")];
        cfg.audit = AuditTrail::new().with_sink(sink.clone());
        let provider: Arc<dyn Provider> = Arc::new(SingleToolProvider {
            tool: "Search".into(),
            input: serde_json::json!({ "token": "SUPERSECRET" }),
        });
        let executor: Arc<dyn ToolExecutor> = Arc::new(RecordingExecutor {
            calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            last_input: Arc::new(std::sync::Mutex::new(serde_json::Value::Null)),
        });
        let stream = run_agent(provider, executor, cfg);
        futures::pin_mut!(stream);
        while stream.next().await.is_some() {}

        let records = sink.records();
        assert!(matches!(
            records.first().unwrap().event,
            AuditEvent::RunStarted { .. }
        ));
        assert!(matches!(
            records.last().unwrap().event,
            AuditEvent::RunStopped { .. }
        ));
        assert!(records
            .iter()
            .any(|r| matches!(r.event, AuditEvent::ToolStarted { .. })));
        let encoded = serde_json::to_string(&records).unwrap();
        assert!(!encoded.contains("SUPERSECRET"));
    }

    struct OverBudgetToolProvider;

    #[async_trait]
    impl Provider for OverBudgetToolProvider {
        fn name(&self) -> &str {
            "over-budget"
        }

        async fn stream(
            &self,
            _req: ProviderRequest,
        ) -> crate::error::Result<BoxStream<'static, StreamDelta>> {
            Ok(Box::pin(futures::stream::iter(vec![
                StreamDelta::ToolCallStart {
                    id: "costly".into(),
                    name: "Bash".into(),
                },
                StreamDelta::ToolCallInput {
                    id: "costly".into(),
                    input: serde_json::json!({ "command": "echo should-not-run" }),
                },
                StreamDelta::Usage(Usage {
                    input_tokens: 100,
                    output_tokens: 100,
                    ..Usage::default()
                }),
                StreamDelta::MessageStop {
                    stop_reason: "tool_use".into(),
                },
            ])))
        }
    }

    #[tokio::test]
    async fn fail_closed_usage_audit_stops_before_tool_side_effect() {
        let calls = Arc::new(AtomicUsize::new(0));
        let recorder = crate::session::RunRecorder::default();
        let executor: Arc<dyn ToolExecutor> = Arc::new(RecordingExecutor {
            calls: calls.clone(),
            last_input: Arc::new(std::sync::Mutex::new(serde_json::Value::Null)),
        });
        let mut cfg = RunConfig::new("m", vec![Message::user("hi")]);
        cfg.tools = vec![advertised("Bash")];
        cfg.audit = fail_closed_on("usage");
        cfg.recorder = recorder.clone();
        let stream = run_agent(Arc::new(OverBudgetToolProvider), executor, cfg);
        futures::pin_mut!(stream);
        let mut audit_info = None;
        while let Some(delta) = stream.next().await {
            if let StreamDelta::Error { info, .. } = delta {
                audit_info = Some(info);
            }
        }
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(audit_info.unwrap().code, crate::error::ErrorCode::Audit);
        let outcome = recorder.outcome();
        assert_eq!(
            outcome.terminal_status,
            crate::session::RunTerminalStatus::Failed
        );
        assert_eq!(outcome.stop_reason.as_deref(), Some("audit_failure"));
    }

    #[tokio::test]
    async fn fail_closed_post_hook_audit_stops_after_one_side_effect_without_next_turn() {
        let calls = Arc::new(AtomicUsize::new(0));
        let requests = Arc::new(AtomicUsize::new(0));
        let recorder = crate::session::RunRecorder::default();
        let provider: Arc<dyn Provider> = Arc::new(CountingSingleToolProvider {
            tool: "Search".into(),
            input: serde_json::json!({ "q": "x" }),
            requests: requests.clone(),
        });
        let executor: Arc<dyn ToolExecutor> = Arc::new(RecordingExecutor {
            calls: calls.clone(),
            last_input: Arc::new(std::sync::Mutex::new(serde_json::Value::Null)),
        });
        let mut cfg = RunConfig::new("m", vec![Message::user("hi")]);
        cfg.tools = vec![advertised("Search")];
        cfg.audit = fail_closed_on("post_hook");
        cfg.recorder = recorder.clone();
        let stream = run_agent(provider, executor, cfg);
        futures::pin_mut!(stream);
        let mut saw_audit = false;
        while let Some(delta) = stream.next().await {
            if matches!(
                delta,
                StreamDelta::Error { info, .. } if info.code == crate::error::ErrorCode::Audit
            ) {
                saw_audit = true;
            }
        }
        assert!(saw_audit);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(requests.load(Ordering::SeqCst), 1);
        assert_eq!(
            recorder.outcome().terminal_status,
            crate::session::RunTerminalStatus::Failed
        );
        assert_eq!(
            recorder.outcome().stop_reason.as_deref(),
            Some("audit_failure")
        );
    }

    #[tokio::test]
    async fn fail_closed_run_stopped_audit_cannot_record_completed() {
        let recorder = crate::session::RunRecorder::default();
        let mut cfg = RunConfig::new("mock-1", vec![Message::user("hi")]);
        cfg.audit = fail_closed_on("run_stopped");
        cfg.recorder = recorder.clone();
        let stream = run_agent(
            Arc::new(crate::providers::MockProvider),
            Arc::new(EchoTool),
            cfg,
        );
        futures::pin_mut!(stream);
        let mut saw_audit = false;
        while let Some(delta) = stream.next().await {
            if matches!(
                delta,
                StreamDelta::Error { info, .. } if info.code == crate::error::ErrorCode::Audit
            ) {
                saw_audit = true;
            }
        }
        assert!(saw_audit);
        let outcome = recorder.outcome();
        assert_eq!(
            outcome.terminal_status,
            crate::session::RunTerminalStatus::Failed
        );
        assert_eq!(outcome.stop_reason.as_deref(), Some("audit_failure"));
    }

    #[tokio::test]
    async fn over_budget_turn_never_executes_its_tool() {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let executor: Arc<dyn ToolExecutor> = Arc::new(RecordingExecutor {
            calls: calls.clone(),
            last_input: Arc::new(std::sync::Mutex::new(serde_json::Value::Null)),
        });
        let mut cfg = RunConfig::new("m", vec![Message::user("hi")]);
        cfg.budget = crate::budget::BudgetPolicy::token_limit(10);
        let stream = run_agent(Arc::new(OverBudgetToolProvider), executor, cfg);
        futures::pin_mut!(stream);
        let mut saw_budget_error = false;
        while let Some(delta) = stream.next().await {
            if matches!(delta, StreamDelta::Error { ref message, .. } if message.contains("budget"))
            {
                saw_budget_error = true;
            }
        }
        assert!(saw_budget_error);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    struct BlockedStartupProvider {
        calls: Arc<AtomicUsize>,
        entered: Arc<tokio::sync::Notify>,
        dropped: Arc<AtomicBool>,
    }

    struct DropSignal(Arc<AtomicBool>);

    impl Drop for DropSignal {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[async_trait]
    impl Provider for BlockedStartupProvider {
        fn name(&self) -> &str {
            "blocked-startup"
        }

        async fn stream(
            &self,
            _req: ProviderRequest,
        ) -> crate::error::Result<BoxStream<'static, StreamDelta>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.entered.notify_one();
            let _drop_signal = DropSignal(self.dropped.clone());
            std::future::pending().await
        }
    }

    #[tokio::test]
    async fn pre_cancelled_run_never_calls_provider_and_finalizes_as_cancelled() {
        use crate::governance::hooks::HookDispatcher;
        use crate::governance::Governance;
        use crate::observability::{AuditEvent, AuditTrail, InMemoryAuditSink};
        use crate::session::RunTerminalStatus;

        let calls = Arc::new(AtomicUsize::new(0));
        let stopped = Arc::new(std::sync::Mutex::new(Vec::new()));
        let audit = Arc::new(InMemoryAuditSink::default());
        let recorder = crate::session::RunRecorder::default();
        let token = crate::cancellation::CancellationToken::new();
        token.cancel();

        let mut hooks = HookDispatcher::new();
        let seen = stopped.clone();
        hooks.on_stop(move |ctx| seen.lock().unwrap().push(ctx.reason.clone()));
        let mut cfg = RunConfig::new("primary", vec![Message::user("hi")]);
        cfg.cancellation = token;
        cfg.recorder = recorder.clone();
        cfg.audit = AuditTrail::new().with_sink(audit.clone());
        cfg.governance = Governance::new(Default::default(), hooks);

        let stream = run_agent(
            Arc::new(BlockedStartupProvider {
                calls: calls.clone(),
                entered: Arc::new(tokio::sync::Notify::new()),
                dropped: Arc::new(AtomicBool::new(false)),
            }),
            Arc::new(EchoTool),
            cfg,
        );
        futures::pin_mut!(stream);
        let mut cancellation_info = None;
        while let Some(delta) = stream.next().await {
            if let StreamDelta::Error { info, .. } = delta {
                cancellation_info = Some(info);
            }
        }

        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            cancellation_info.unwrap().code,
            crate::error::ErrorCode::Cancelled
        );
        assert_eq!(stopped.lock().unwrap().as_slice(), &["cancelled"]);
        let outcome = recorder.outcome();
        assert_eq!(outcome.terminal_status, RunTerminalStatus::Cancelled);
        assert_eq!(outcome.stop_reason.as_deref(), Some("cancelled"));
        assert!(audit.records().iter().any(|record| matches!(
            &record.event,
            AuditEvent::RunStopped { reason, .. } if reason == "cancelled"
        )));
    }

    #[tokio::test]
    async fn cancellation_drops_blocked_provider_startup_and_runs_all_finalizers() {
        use crate::governance::hooks::HookDispatcher;
        use crate::governance::Governance;
        use crate::observability::{AuditEvent, AuditTrail, InMemoryAuditSink};
        use crate::session::RunTerminalStatus;

        let calls = Arc::new(AtomicUsize::new(0));
        let entered = Arc::new(tokio::sync::Notify::new());
        let dropped = Arc::new(AtomicBool::new(false));
        let stops = Arc::new(AtomicUsize::new(0));
        let audit = Arc::new(InMemoryAuditSink::default());
        let recorder = crate::session::RunRecorder::default();
        let token = crate::cancellation::CancellationToken::new();

        let mut hooks = HookDispatcher::new();
        let stop_count = stops.clone();
        hooks.on_stop(move |ctx| {
            assert_eq!(ctx.reason, "cancelled");
            stop_count.fetch_add(1, Ordering::SeqCst);
        });
        let mut cfg = RunConfig::new("primary", vec![Message::user("hi")]);
        cfg.cancellation = token.clone();
        cfg.recorder = recorder.clone();
        cfg.audit = AuditTrail::new().with_sink(audit.clone());
        cfg.governance = Governance::new(Default::default(), hooks);

        let stream = run_agent(
            Arc::new(BlockedStartupProvider {
                calls: calls.clone(),
                entered: entered.clone(),
                dropped: dropped.clone(),
            }),
            Arc::new(EchoTool),
            cfg,
        );
        let drain = tokio::spawn(async move {
            futures::pin_mut!(stream);
            while stream.next().await.is_some() {}
        });
        entered.notified().await;
        token.cancel();
        tokio::time::timeout(std::time::Duration::from_secs(1), drain)
            .await
            .expect("cancelled provider startup must resolve")
            .unwrap();

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(dropped.load(Ordering::SeqCst));
        assert_eq!(stops.load(Ordering::SeqCst), 1);
        assert_eq!(
            recorder.outcome().terminal_status,
            RunTerminalStatus::Cancelled
        );
        assert!(audit.records().iter().any(|record| matches!(
            &record.event,
            AuditEvent::RunStopped { reason, .. } if reason == "cancelled"
        )));
    }

    struct BlockedResponseProvider;

    #[async_trait]
    impl Provider for BlockedResponseProvider {
        fn name(&self) -> &str {
            "blocked-response"
        }

        async fn stream(
            &self,
            _req: ProviderRequest,
        ) -> crate::error::Result<BoxStream<'static, StreamDelta>> {
            Ok(Box::pin(async_stream::stream! {
                yield StreamDelta::MessageStart { model: "selected-fallback".into() };
                yield StreamDelta::TextDelta { text: "partial".into() };
                std::future::pending::<()>().await;
            }))
        }
    }

    #[tokio::test]
    async fn cancellation_while_consuming_response_stops_without_partial_success() {
        use crate::session::RunTerminalStatus;

        let recorder = crate::session::RunRecorder::default();
        let token = crate::cancellation::CancellationToken::new();
        let mut cfg = RunConfig::new("requested-primary", vec![Message::user("hi")]);
        cfg.cancellation = token.clone();
        cfg.recorder = recorder.clone();
        let stream = run_agent(Arc::new(BlockedResponseProvider), Arc::new(EchoTool), cfg);
        futures::pin_mut!(stream);

        assert!(matches!(
            stream.next().await,
            Some(StreamDelta::MessageStart { .. })
        ));
        assert!(matches!(
            stream.next().await,
            Some(StreamDelta::TextDelta { .. })
        ));
        token.cancel();
        let cancelled = tokio::time::timeout(std::time::Duration::from_secs(1), stream.next())
            .await
            .expect("cancelled response stream must resolve");
        assert!(matches!(
            cancelled,
            Some(StreamDelta::Error { info, .. })
                if info.code == crate::error::ErrorCode::Cancelled
        ));
        assert!(stream.next().await.is_none());

        let outcome = recorder.outcome();
        assert_eq!(outcome.terminal_status, RunTerminalStatus::Cancelled);
        assert_eq!(outcome.stop_reason.as_deref(), Some("cancelled"));
        assert_eq!(outcome.final_text, None);
        assert_eq!(outcome.model_attempts, vec!["selected-fallback"]);
    }

    struct DeferredSideEffectTool {
        entered: Arc<tokio::sync::Notify>,
        side_effects: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl ToolExecutor for DeferredSideEffectTool {
        async fn execute(
            &self,
            _name: &str,
            _input: serde_json::Value,
        ) -> crate::error::Result<String> {
            self.entered.notify_one();
            std::future::pending::<()>().await;
            #[allow(unreachable_code)]
            {
                self.side_effects.fetch_add(1, Ordering::SeqCst);
                Ok("side effect".into())
            }
        }
    }

    #[tokio::test]
    async fn cancellation_drops_blocked_tool_and_never_starts_a_second_model_turn() {
        use crate::session::RunTerminalStatus;

        let provider_requests = Arc::new(AtomicUsize::new(0));
        let tool_entered = Arc::new(tokio::sync::Notify::new());
        let side_effects = Arc::new(AtomicUsize::new(0));
        let recorder = crate::session::RunRecorder::default();
        let token = crate::cancellation::CancellationToken::new();
        let mut cfg = RunConfig::new("m", vec![Message::user("hi")]);
        cfg.tools = vec![advertised("Search")];
        cfg.cancellation = token.clone();
        cfg.recorder = recorder.clone();

        let stream = run_agent(
            Arc::new(CountingSingleToolProvider {
                tool: "Search".into(),
                input: serde_json::json!({ "q": "x" }),
                requests: provider_requests.clone(),
            }),
            Arc::new(DeferredSideEffectTool {
                entered: tool_entered.clone(),
                side_effects: side_effects.clone(),
            }),
            cfg,
        );
        let drain = tokio::spawn(async move {
            futures::pin_mut!(stream);
            while stream.next().await.is_some() {}
        });
        tool_entered.notified().await;
        token.cancel();
        tokio::time::timeout(std::time::Duration::from_secs(1), drain)
            .await
            .expect("cancelled tool future must resolve")
            .unwrap();

        assert_eq!(side_effects.load(Ordering::SeqCst), 0);
        assert_eq!(provider_requests.load(Ordering::SeqCst), 1);
        assert_eq!(
            recorder.outcome().terminal_status,
            RunTerminalStatus::Cancelled
        );
        assert_eq!(recorder.outcome().stop_reason.as_deref(), Some("cancelled"));
    }

    struct NoMessageStartProvider;

    struct SelectedFallbackProvider;

    #[async_trait]
    impl Provider for SelectedFallbackProvider {
        fn name(&self) -> &str {
            "resilient"
        }

        async fn stream(
            &self,
            _req: ProviderRequest,
        ) -> crate::error::Result<BoxStream<'static, StreamDelta>> {
            Ok(Box::pin(futures::stream::iter(vec![
                StreamDelta::MessageStart {
                    model: "fallback-model".into(),
                },
                StreamDelta::TextDelta {
                    text: "done".into(),
                },
                StreamDelta::MessageStop {
                    stop_reason: "end_turn".into(),
                },
            ])))
        }
    }

    #[tokio::test]
    async fn recorder_uses_message_start_model_selected_by_fallback() {
        let recorder = crate::session::RunRecorder::default();
        let mut cfg = RunConfig::new("primary-model", vec![Message::user("hi")]);
        cfg.recorder = recorder.clone();
        let stream = run_agent(Arc::new(SelectedFallbackProvider), Arc::new(EchoTool), cfg);
        futures::pin_mut!(stream);
        while stream.next().await.is_some() {}
        assert_eq!(recorder.outcome().model_attempts, vec!["fallback-model"]);
    }

    #[async_trait]
    impl Provider for NoMessageStartProvider {
        fn name(&self) -> &str {
            "custom"
        }

        async fn stream(
            &self,
            _req: ProviderRequest,
        ) -> crate::error::Result<BoxStream<'static, StreamDelta>> {
            Ok(Box::pin(futures::stream::iter(vec![
                StreamDelta::TextDelta {
                    text: "done".into(),
                },
                StreamDelta::MessageStop {
                    stop_reason: "end_turn".into(),
                },
            ])))
        }
    }

    #[tokio::test]
    async fn recorder_uses_requested_model_when_custom_provider_omits_message_start() {
        let recorder = crate::session::RunRecorder::default();
        let mut cfg = RunConfig::new("requested-model", vec![Message::user("hi")]);
        cfg.recorder = recorder.clone();
        let stream = run_agent(Arc::new(NoMessageStartProvider), Arc::new(EchoTool), cfg);
        futures::pin_mut!(stream);
        while stream.next().await.is_some() {}
        assert_eq!(recorder.outcome().model_attempts, vec!["requested-model"]);
    }
}
