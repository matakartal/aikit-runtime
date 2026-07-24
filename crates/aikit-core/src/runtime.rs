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
    /// Provider-parameter compatibility behavior. Strict is the safe default.
    pub compatibility_mode: crate::contract::CompatibilityMode,
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
    /// Optional synchronous durable coordinator. When present its run ID is the authoritative
    /// runtime, audit, and governance identity.
    pub durable: Option<crate::durable_runtime::DurableRunDriver>,
    /// Absolute deadline inherited from a shared orchestration ledger. Kept private so the only
    /// producer is the ledger's original start time; callers cannot accidentally reset it per
    /// child.
    shared_wall_time_deadline: Option<Instant>,
    invocation_prepared: bool,
    invocation_error: Option<crate::error::AikitError>,
    durable_invocation: Option<crate::durable_runtime::DurableInvocationDisposition>,
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
            compatibility_mode: crate::contract::CompatibilityMode::Strict,
            governance: crate::governance::Governance::default(),
            audit: crate::observability::AuditTrail::default(),
            budget: crate::budget::BudgetPolicy::default(),
            compaction: crate::compaction::CompactionPolicy::default(),
            recorder: crate::session::RunRecorder::default(),
            cancellation: crate::cancellation::CancellationToken::new(),
            durable: None,
            shared_wall_time_deadline: None,
            invocation_prepared: false,
            invocation_error: None,
            durable_invocation: None,
        }
    }

    /// Attach a caller-supplied durable run state and persistence store.
    pub fn with_durable_run(
        mut self,
        state: crate::durability::RunState,
        store: Arc<dyn crate::durable_store::DurableStore>,
    ) -> std::result::Result<Self, crate::durable_runtime::DurableRunDriverError> {
        self.durable = Some(crate::durable_runtime::DurableRunDriver::new(state, store)?);
        Ok(self)
    }

    /// Attach an already-created driver, retaining a clone outside the config for inspection.
    pub fn with_durable_driver(mut self, driver: crate::durable_runtime::DurableRunDriver) -> Self {
        self.durable = Some(driver);
        self
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
        if let Some(durable) = &self.durable {
            match self.governance.clone().with_durable_driver(durable) {
                Ok(governance) => self.governance = governance,
                Err(error) => {
                    self.invocation_error = Some(crate::error::AikitError::Configuration(format!(
                        "durable governance attachment failed: {error}"
                    )));
                }
            }
            if self.invocation_error.is_none() {
                match durable.invocation_disposition() {
                    Ok(disposition) => self.durable_invocation = Some(disposition),
                    Err(error) => self.invocation_error = Some(error.into()),
                }
            }
            if self.invocation_error.is_none() {
                let prepared_audit = match self.durable_invocation.as_ref() {
                    Some(
                        crate::durable_runtime::DurableInvocationDisposition::RetryTerminalAudit(
                            replay,
                        ),
                    ) => Some(self.audit.for_terminal_replay(
                        replay.audit_run_id.clone(),
                        replay.invocation_id.clone(),
                        replay.run_stopped_sequence,
                        &replay.audit_binding,
                    )),
                    Some(_) => Some(
                        durable
                            .run_id()
                            .map_err(crate::error::AikitError::from)
                            .and_then(|run_id| self.audit.for_run_id(run_id)),
                    ),
                    None => None,
                };
                if let Some(prepared_audit) = prepared_audit {
                    match prepared_audit {
                        Ok(audit) => self.audit = audit,
                        Err(error) => self.invocation_error = Some(error),
                    }
                }
            }
        } else {
            self.audit = self.audit.fresh_run();
        }
        if self.invocation_error.is_none() {
            self.governance = self.governance.fork_for_run();
        }
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

#[derive(serde::Serialize, serde::Deserialize)]
struct DurableToolOutput {
    content: String,
    is_error: bool,
}

fn durable_provider_input(provider: &str, req: &ProviderRequest) -> serde_json::Value {
    let transient = serde_json::json!({
        "provider": provider,
        "model": &req.model,
        "messages": &req.messages,
        "tools": &req.tools,
        "max_tokens": req.max_tokens,
        "options": &req.options,
        "provider_options": &req.provider_options,
        "compatibility_mode": req.compatibility_mode,
    });
    durable_hashed_input(&transient)
}

/// Durable activity definitions retain only a deterministic input hash. Raw prompts, provider
/// options, credentials, and tool arguments stay out of the append-only activity schedule.
fn durable_hashed_input(input: &serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        "input_hash": crate::durability::stable_input_hash(input),
    })
}

fn finalize_ambiguous_durable_activity(
    durable: Option<&crate::durable_runtime::DurableRunDriver>,
    attempt: Option<&(String, u32)>,
    reason: &'static str,
) -> crate::error::Result<()> {
    if let (Some(durable), Some((activity_id, attempt))) = (durable, attempt) {
        durable
            .fail_activity(activity_id, *attempt, reason, false, true)
            .map(|_| ())
            .map_err(crate::error::AikitError::from)?;
    }
    Ok(())
}

fn fail_unstarted_durable_activity(
    durable: Option<&crate::durable_runtime::DurableRunDriver>,
    attempt: Option<&(String, u32)>,
    reason: &'static str,
) -> crate::error::Result<()> {
    if let (Some(durable), Some((activity_id, attempt))) = (durable, attempt) {
        durable
            .fail_activity(activity_id, *attempt, reason, false, false)
            .map(|_| ())
            .map_err(crate::error::AikitError::from)?;
    }
    Ok(())
}

fn persist_durable_terminal(
    durable: &crate::durable_runtime::DurableRunDriver,
    receipt: &crate::durable_runtime::DurableRunStoppedReceipt,
) -> Result<(), crate::durable_runtime::DurableRunDriverError> {
    durable.finalize_run_stopped_receipt(receipt)
}

fn durable_run_stopped_replay(
    cfg: &RunConfig,
    kind: crate::durable_runtime::DurableRunStoppedAuditKind,
    terminal_receipt: crate::durable_runtime::DurableRunStoppedReceipt,
    audit_turns: usize,
    audit_reason: impl Into<String>,
) -> crate::error::Result<(
    crate::durable_runtime::DurableRunStoppedAuditReplayEnvelope,
    crate::observability::PreparedAuditRecord,
)> {
    let invocation_id = cfg.audit.invocation_id().ok_or_else(|| {
        crate::error::AikitError::Configuration(
            "durable terminal audit is missing its invocation identity".into(),
        )
    })?;
    let audit_reason = audit_reason.into();
    let prepared = cfg
        .audit
        .prepare_event(crate::observability::AuditEvent::RunStopped {
            turns: audit_turns,
            reason: audit_reason.clone(),
        })?;
    Ok((
        crate::durable_runtime::DurableRunStoppedAuditReplayEnvelope::new(
            kind,
            terminal_receipt,
            cfg.audit.run_id(),
            invocation_id,
            prepared.sequence(),
            audit_turns,
            audit_reason,
            cfg.audit.replay_binding(),
        ),
        prepared,
    ))
}

fn retry_reconciled_terminal_audit(
    cfg: &RunConfig,
    replay: crate::durable_runtime::DurableRunStoppedAuditReplayEnvelope,
) -> Vec<StreamDelta> {
    use crate::observability::AuditEvent;

    let mut deltas = Vec::new();
    let Some(durable) = cfg.durable.as_ref() else {
        let error = crate::error::AikitError::Configuration(
            "terminal audit replay requires a durable driver".into(),
        );
        deltas.push(StreamDelta::from_error(&error));
        cfg.recorder.complete(
            replay.terminal_receipt.usage,
            crate::session::RunTerminalStatus::Failed,
            "durable_commit_failure",
        );
        return deltas;
    };

    let retry = match durable.begin_terminal_audit_retry(&replay) {
        Ok(retry) => retry,
        Err(error) => {
            let error = crate::error::AikitError::from(error);
            deltas.push(StreamDelta::from_error(&error));
            cfg.recorder.complete(
                replay.terminal_receipt.usage,
                crate::session::RunTerminalStatus::Failed,
                "durable_commit_failure",
            );
            return deltas;
        }
    };

    if let Err(error) = cfg.audit.emit(AuditEvent::RunStopped {
        turns: replay.audit_turns,
        reason: replay.audit_reason.clone(),
    }) {
        deltas.push(StreamDelta::from_error(&error));
        let mut recorded_reason = "audit_failure";
        if let Err(error) = durable.fail_terminal_audit_retry(&retry) {
            let error = crate::error::AikitError::from(error);
            deltas.push(StreamDelta::from_error(&error));
            recorded_reason = "durable_commit_failure";
        }
        cfg.recorder.complete(
            replay.terminal_receipt.usage,
            crate::session::RunTerminalStatus::Failed,
            recorded_reason,
        );
        return deltas;
    }

    if let Err(error) = durable.complete_terminal_audit_retry(&retry) {
        let error = crate::error::AikitError::from(error);
        deltas.push(StreamDelta::from_error(&error));
        cfg.recorder.complete(
            replay.terminal_receipt.usage,
            crate::session::RunTerminalStatus::Failed,
            "durable_commit_failure",
        );
        return deltas;
    }

    if let Err(error) = persist_durable_terminal(durable, &replay.terminal_receipt) {
        let error = crate::error::AikitError::from(error);
        deltas.push(StreamDelta::from_error(&error));
        cfg.recorder.complete(
            replay.terminal_receipt.usage,
            crate::session::RunTerminalStatus::Failed,
            "durable_commit_failure",
        );
    } else {
        cfg.recorder.complete(
            replay.terminal_receipt.usage,
            recorded_terminal_status(&replay.terminal_receipt.reason),
            replay.terminal_receipt.reason.clone(),
        );
    }
    deltas
}

struct DurableInvocationLifecycleAttempt {
    invocation_id: String,
    activity_id: String,
    attempt: u32,
}

fn resolve_durable_activity_boundary(
    durable: &crate::durable_runtime::DurableRunDriver,
    error: crate::durable_runtime::DurableRunDriverError,
    active_invocation_id: Option<&str>,
) -> Result<crate::durable_runtime::DurableInvocationDisposition, crate::error::AikitError> {
    if !matches!(
        error,
        crate::durable_runtime::DurableRunDriverError::TerminalRecoveryRequired { .. }
    ) {
        return Err(error.into());
    }

    let disposition = match active_invocation_id {
        Some(invocation_id) => {
            durable.invocation_disposition_for_active_lifecycle(invocation_id)?
        }
        None => durable.invocation_disposition()?,
    };
    match disposition {
        crate::durable_runtime::DurableInvocationDisposition::Execute => {
            Err(crate::error::AikitError::Conflict(
                "terminal audit state disappeared while resolving an activity boundary".into(),
            ))
        }
        disposition => Ok(disposition),
    }
}

fn close_superseded_durable_invocation(
    cfg: &RunConfig,
    lifecycle: &DurableInvocationLifecycleAttempt,
    stopped_turns: usize,
    receipt: crate::durable_runtime::DurableRunStoppedReceipt,
) -> Vec<StreamDelta> {
    let mut deltas = Vec::new();
    let Some(durable) = cfg.durable.as_ref() else {
        let error = crate::error::AikitError::Configuration(
            "durable terminal receipt exists without a durable driver".into(),
        );
        deltas.push(StreamDelta::from_error(&error));
        cfg.recorder.complete(
            receipt.usage,
            crate::session::RunTerminalStatus::Failed,
            "durable_commit_failure",
        );
        return deltas;
    };
    match durable.close_active_lifecycle_from_matching_completed_canonical_replay(
        &lifecycle.activity_id,
        lifecycle.attempt,
    ) {
        Ok(true) => {
            if let Err(error) = persist_durable_terminal(durable, &receipt) {
                let error = crate::error::AikitError::from(error);
                deltas.push(StreamDelta::from_error(&error));
                cfg.recorder.complete(
                    receipt.usage,
                    crate::session::RunTerminalStatus::Failed,
                    "durable_commit_failure",
                );
            } else {
                cfg.recorder.complete(
                    receipt.usage,
                    recorded_terminal_status(&receipt.reason),
                    receipt.reason,
                );
            }
            return deltas;
        }
        Ok(false) => {}
        Err(error) => {
            let error = crate::error::AikitError::from(error);
            deltas.push(StreamDelta::from_error(&error));
            cfg.recorder.complete(
                receipt.usage,
                crate::session::RunTerminalStatus::Failed,
                "durable_commit_failure",
            );
            return deltas;
        }
    }
    let (replay, prepared_audit) = match durable_run_stopped_replay(
        cfg,
        crate::durable_runtime::DurableRunStoppedAuditKind::Recovery,
        receipt.clone(),
        stopped_turns,
        "superseded_by_durable_terminal_receipt",
    ) {
        Ok(replay) => replay,
        Err(error) => {
            deltas.push(StreamDelta::from_error(&error));
            cfg.recorder.complete(
                receipt.usage,
                crate::session::RunTerminalStatus::Failed,
                "durable_commit_failure",
            );
            return deltas;
        }
    };
    let recovery_attempt = match durable.begin_recovery_run_stopped_audit(&replay) {
        Ok(crate::durable_runtime::DurableActivity::Execute {
            activity_id,
            attempt,
            ..
        }) => Some((activity_id, attempt)),
        Ok(crate::durable_runtime::DurableActivity::ReuseCompleted { .. }) => None,
        Err(error) => {
            let error = crate::error::AikitError::from(error);
            deltas.push(StreamDelta::from_error(&error));
            cfg.recorder.complete(
                receipt.usage,
                crate::session::RunTerminalStatus::Failed,
                "durable_commit_failure",
            );
            return deltas;
        }
    };

    let mut lifecycle_closed = false;
    if let Some((activity_id, attempt)) = recovery_attempt {
        match cfg.audit.emit_prepared(prepared_audit) {
            Ok(()) => {
                if let Err(error) = durable
                    .complete_recovery_run_stopped_audit_and_invocation_lifecycle(
                        &activity_id,
                        attempt,
                        &lifecycle.activity_id,
                        lifecycle.attempt,
                        &replay,
                    )
                {
                    let error = crate::error::AikitError::from(error);
                    deltas.push(StreamDelta::from_error(&error));
                    cfg.recorder.complete(
                        receipt.usage,
                        crate::session::RunTerminalStatus::Failed,
                        "durable_commit_failure",
                    );
                    return deltas;
                }
                lifecycle_closed = true;
            }
            Err(error) => {
                deltas.push(StreamDelta::from_error(&error));
                if let Err(marker_error) = durable
                    .fail_recovery_run_stopped_audit_and_invocation_lifecycle(
                        &activity_id,
                        attempt,
                        &lifecycle.activity_id,
                        lifecycle.attempt,
                    )
                {
                    let marker_error = crate::error::AikitError::from(marker_error);
                    deltas.push(StreamDelta::from_error(&marker_error));
                }
                cfg.recorder.complete(
                    receipt.usage,
                    crate::session::RunTerminalStatus::Failed,
                    "audit_failure",
                );
                return deltas;
            }
        }
    }

    if !lifecycle_closed {
        if let Err(error) = durable.complete_invocation_lifecycle_with_replay(
            &lifecycle.activity_id,
            lifecycle.attempt,
            &replay,
        ) {
            let error = crate::error::AikitError::from(error);
            deltas.push(StreamDelta::from_error(&error));
            cfg.recorder.complete(
                receipt.usage,
                crate::session::RunTerminalStatus::Failed,
                "durable_commit_failure",
            );
            return deltas;
        }
    }

    if let Err(error) = persist_durable_terminal(durable, &receipt) {
        let error = crate::error::AikitError::from(error);
        deltas.push(StreamDelta::from_error(&error));
        cfg.recorder.complete(
            receipt.usage,
            crate::session::RunTerminalStatus::Failed,
            "durable_commit_failure",
        );
    } else {
        let status = recorded_terminal_status(&receipt.reason);
        cfg.recorder.complete(receipt.usage, status, receipt.reason);
    }
    deltas
}

fn close_nonexecuting_durable_invocation(
    cfg: &RunConfig,
    lifecycle: &DurableInvocationLifecycleAttempt,
    stopped_turns: usize,
    usage: Usage,
    disposition: crate::durable_runtime::DurableInvocationDisposition,
) -> Vec<StreamDelta> {
    let mut deltas = Vec::new();
    let closure_reason = match &disposition {
        crate::durable_runtime::DurableInvocationDisposition::ReconcileRequired { .. } => {
            "durable_reconciliation_required"
        }
        crate::durable_runtime::DurableInvocationDisposition::AlreadyTerminal { .. } => {
            "superseded_by_durable_terminal_state"
        }
        crate::durable_runtime::DurableInvocationDisposition::AwaitingResume { .. } => {
            "durable_awaiting_resume"
        }
        crate::durable_runtime::DurableInvocationDisposition::Execute => "durable_state_error",
        crate::durable_runtime::DurableInvocationDisposition::FinalizeTerminal(_) => {
            unreachable!("terminal receipts use recovery closure protocol")
        }
        crate::durable_runtime::DurableInvocationDisposition::RetryTerminalAudit(_) => {
            unreachable!("terminal audit retries do not open a RunStarted lifecycle")
        }
    };
    let (replay, prepared_audit) = match durable_run_stopped_replay(
        cfg,
        crate::durable_runtime::DurableRunStoppedAuditKind::Direct,
        crate::durable_runtime::DurableRunStoppedReceipt {
            turns: stopped_turns,
            reason: closure_reason.into(),
            usage,
        },
        stopped_turns,
        closure_reason,
    ) {
        Ok(replay) => replay,
        Err(error) => {
            deltas.push(StreamDelta::from_error(&error));
            cfg.recorder.complete(
                usage,
                crate::session::RunTerminalStatus::Failed,
                "durable_commit_failure",
            );
            return deltas;
        }
    };
    let mut audit_closed = match cfg.audit.emit_prepared(prepared_audit) {
        Ok(()) => true,
        Err(error) => {
            deltas.push(StreamDelta::from_error(&error));
            false
        }
    };
    if let Some(durable) = cfg.durable.as_ref() {
        let lifecycle_result = if audit_closed {
            durable.complete_invocation_lifecycle_with_replay(
                &lifecycle.activity_id,
                lifecycle.attempt,
                &replay,
            )
        } else {
            durable.fail_invocation_lifecycle(&lifecycle.activity_id, lifecycle.attempt)
        };
        if let Err(error) = lifecycle_result {
            let error = crate::error::AikitError::from(error);
            deltas.push(StreamDelta::from_error(&error));
            audit_closed = false;
        }
    }

    match disposition {
        crate::durable_runtime::DurableInvocationDisposition::ReconcileRequired { reason } => {
            let error = crate::error::AikitError::Conflict(format!(
                "durable run requires explicit reconciliation: {reason}"
            ));
            deltas.push(StreamDelta::from_error(&error));
            cfg.recorder.complete(
                usage,
                crate::session::RunTerminalStatus::Failed,
                "durable_reconciliation_required",
            );
        }
        crate::durable_runtime::DurableInvocationDisposition::AlreadyTerminal {
            status,
            reason,
        } => {
            let (recorded_status, fallback_reason) = match status {
                crate::durability::DurableRunStatus::Completed => (
                    crate::session::RunTerminalStatus::Completed,
                    "durable_already_completed",
                ),
                crate::durability::DurableRunStatus::Failed => (
                    crate::session::RunTerminalStatus::Failed,
                    "durable_already_failed",
                ),
                crate::durability::DurableRunStatus::Cancelled => (
                    crate::session::RunTerminalStatus::Cancelled,
                    "durable_already_cancelled",
                ),
                crate::durability::DurableRunStatus::Running
                | crate::durability::DurableRunStatus::Paused
                | crate::durability::DurableRunStatus::ReconcileRequired => {
                    unreachable!("only terminal durable statuses are classified as terminal")
                }
            };
            cfg.recorder.complete(
                usage,
                if audit_closed {
                    recorded_status
                } else {
                    crate::session::RunTerminalStatus::Failed
                },
                if audit_closed {
                    reason.unwrap_or_else(|| fallback_reason.into())
                } else {
                    "audit_failure".into()
                },
            );
        }
        crate::durable_runtime::DurableInvocationDisposition::AwaitingResume { reason } => {
            let detail = reason
                .map(|reason| format!(": {reason}"))
                .unwrap_or_default();
            let error = crate::error::AikitError::Conflict(format!(
                "durable run became paused and requires an explicit resume{detail}"
            ));
            deltas.push(StreamDelta::from_error(&error));
            cfg.recorder.complete(
                usage,
                crate::session::RunTerminalStatus::Failed,
                "durable_awaiting_resume",
            );
        }
        crate::durable_runtime::DurableInvocationDisposition::Execute => {
            let error = crate::error::AikitError::Conflict(
                "durable activity boundary did not resolve to a safe disposition".into(),
            );
            deltas.push(StreamDelta::from_error(&error));
            cfg.recorder.complete(
                usage,
                crate::session::RunTerminalStatus::Failed,
                "durable_state_error",
            );
        }
        crate::durable_runtime::DurableInvocationDisposition::FinalizeTerminal(_) => {
            unreachable!("terminal receipts use recovery closure protocol")
        }
        crate::durable_runtime::DurableInvocationDisposition::RetryTerminalAudit(_) => {
            unreachable!("terminal audit retries do not open a RunStarted lifecycle")
        }
    }
    deltas
}

fn recorded_terminal_status(reason: &str) -> crate::session::RunTerminalStatus {
    match reason {
        "end_turn" | "stop" => crate::session::RunTerminalStatus::Completed,
        "budget_exceeded" | "budget_configuration_error" => {
            crate::session::RunTerminalStatus::BudgetExceeded
        }
        "max_turns" => crate::session::RunTerminalStatus::MaxTurns,
        "approval_interrupted" | "cancelled" => crate::session::RunTerminalStatus::Cancelled,
        _ => crate::session::RunTerminalStatus::Failed,
    }
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
            StreamDelta::Warning { warning } => (
                warning
                    .code
                    .len()
                    .saturating_add(warning.message.len())
                    .saturating_add(warning.parameter.as_ref().map_or(0, String::len))
                    .saturating_add(warning.provider.as_ref().map_or(0, String::len))
                    .saturating_add(warning.model.as_ref().map_or(0, String::len)),
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

        // Every returned stream owns a coherent invocation outcome, including failures that occur
        // before audit identity is allocated or RunStarted is emitted.
        cfg.recorder.begin(cfg.messages.clone());

        // A cloned in-process driver may back several independently-created configs. Claim it
        // before preparing audit identity so a losing sibling emits neither RunStarted nor hooks.
        let _durable_invocation_claim = if let Some(durable) = cfg.durable.as_ref() {
            match durable.claim_invocation() {
                Ok(claim) => Some(claim),
                Err(error) => {
                    let error = crate::error::AikitError::from(error);
                    yield StreamDelta::from_error(&error);
                    cfg.recorder.complete(
                        Usage::default(),
                        crate::session::RunTerminalStatus::Failed,
                        "durable_invocation_already_active",
                    );
                    return;
                }
            }
        } else {
            None
        };

        // Invocation-local identity and human approval state: cloned options may run concurrently,
        // but neither audit sequence nor AllowTool grants can bleed into a sibling invocation.
        cfg.prepare_invocation();
        // High-level fallback wiring may have prepared this config earlier. Revalidate the durable
        // disposition at the last boundary before RunStarted so a receipt committed in between
        // turns this invocation into terminal-only recovery instead of opening an unmatched span.
        if cfg.invocation_error.is_none() {
            if let Some(durable) = &cfg.durable {
                match durable.invocation_disposition() {
                    Ok(disposition) => {
                        match &disposition {
                            crate::durable_runtime::DurableInvocationDisposition::RetryTerminalAudit(
                                replay,
                            ) => {
                                match cfg.audit.for_terminal_replay(
                                    replay.audit_run_id.clone(),
                                    replay.invocation_id.clone(),
                                    replay.run_stopped_sequence,
                                    &replay.audit_binding,
                                ) {
                                    Ok(audit) => cfg.audit = audit,
                                    Err(error) => cfg.invocation_error = Some(error),
                                }
                            }
                            crate::durable_runtime::DurableInvocationDisposition::Execute => {
                                match durable.run_id() {
                                    Ok(run_id)
                                        if cfg.audit.run_id() == run_id
                                            && cfg.audit.invocation_id().is_some() => {}
                                    Ok(_) => {
                                        cfg.invocation_error = Some(
                                            crate::error::AikitError::Configuration(
                                                "durable audit identity changed before RunStarted"
                                                    .into(),
                                            ),
                                        );
                                    }
                                    Err(error) => cfg.invocation_error = Some(error.into()),
                                }
                            }
                            _ => {}
                        }
                        cfg.durable_invocation = Some(disposition);
                    }
                    Err(error) => cfg.invocation_error = Some(error.into()),
                }
            }
        }
        if let Some(error) = cfg.invocation_error.take() {
            yield StreamDelta::from_error(&error);
            cfg.recorder.complete(
                Usage::default(),
                crate::session::RunTerminalStatus::Failed,
                "durable_invocation_preflight_failed",
            );
            return;
        }
        match cfg
            .durable_invocation
            .take()
            .unwrap_or(crate::durable_runtime::DurableInvocationDisposition::Execute)
        {
            crate::durable_runtime::DurableInvocationDisposition::Execute => {}
            crate::durable_runtime::DurableInvocationDisposition::RetryTerminalAudit(replay) => {
                for delta in retry_reconciled_terminal_audit(&cfg, replay) {
                    yield delta;
                }
                return;
            }
            crate::durable_runtime::DurableInvocationDisposition::AwaitingResume { reason } => {
                let detail = reason
                    .map(|reason| format!(": {reason}"))
                    .unwrap_or_default();
                let error = crate::error::AikitError::Conflict(format!(
                    "durable run is paused and requires an explicit resume{detail}"
                ));
                yield StreamDelta::from_error(&error);
                cfg.recorder.complete(
                    Usage::default(),
                    crate::session::RunTerminalStatus::Failed,
                    "durable_awaiting_resume",
                );
                return;
            }
            crate::durable_runtime::DurableInvocationDisposition::ReconcileRequired { reason } => {
                let error = crate::error::AikitError::Conflict(format!(
                    "durable run requires explicit reconciliation: {reason}"
                ));
                yield StreamDelta::from_error(&error);
                cfg.recorder.complete(
                    Usage::default(),
                    crate::session::RunTerminalStatus::Failed,
                    "durable_reconciliation_required",
                );
                return;
            }
            crate::durable_runtime::DurableInvocationDisposition::AlreadyTerminal {
                status,
                reason,
            } => {
                let (recorded_status, fallback_reason) = match status {
                    crate::durability::DurableRunStatus::Completed => (
                        crate::session::RunTerminalStatus::Completed,
                        "durable_already_completed",
                    ),
                    crate::durability::DurableRunStatus::Failed => (
                        crate::session::RunTerminalStatus::Failed,
                        "durable_already_failed",
                    ),
                    crate::durability::DurableRunStatus::Cancelled => (
                        crate::session::RunTerminalStatus::Cancelled,
                        "durable_already_cancelled",
                    ),
                    crate::durability::DurableRunStatus::Running
                    | crate::durability::DurableRunStatus::Paused
                    | crate::durability::DurableRunStatus::ReconcileRequired => unreachable!(
                        "only terminal durable statuses are classified as already terminal"
                    ),
                };
                cfg.recorder.complete(
                    Usage::default(),
                    recorded_status,
                    reason.unwrap_or_else(|| fallback_reason.into()),
                );
                return;
            }
            crate::durable_runtime::DurableInvocationDisposition::FinalizeTerminal(receipt) => {
                let Some(durable) = cfg.durable.as_ref() else {
                    let error = crate::error::AikitError::Configuration(
                        "durable terminal receipt exists without a durable driver".into(),
                    );
                    yield StreamDelta::from_error(&error);
                    cfg.recorder.complete(
                        receipt.usage,
                        crate::session::RunTerminalStatus::Failed,
                        "durable_commit_failure",
                    );
                    return;
                };
                if let Err(error) = persist_durable_terminal(durable, &receipt) {
                    let error = crate::error::AikitError::from(error);
                    yield StreamDelta::from_error(&error);
                    cfg.recorder.complete(
                        receipt.usage,
                        crate::session::RunTerminalStatus::Failed,
                        "durable_commit_failure",
                    );
                } else {
                    let status = recorded_terminal_status(&receipt.reason);
                    cfg.recorder
                        .complete(receipt.usage, status, receipt.reason);
                }
                return;
            }
        }

        // Persist an invocation-level fence before RunStarted can reach an external sink. The
        // fence remains running until the matching RunStopped is accepted, so any later CAS or
        // audit double-fault remains visible to restart even when a narrower recovery marker could
        // not be written.
        let durable_invocation_lifecycle = if let Some(durable) = cfg.durable.as_ref() {
            let Some(invocation_id) = cfg.audit.invocation_id().map(str::to_string) else {
                let error = crate::error::AikitError::Configuration(
                    "durable invocation is missing its audit invocation identity".into(),
                );
                yield StreamDelta::from_error(&error);
                cfg.recorder.complete(
                    Usage::default(),
                    crate::session::RunTerminalStatus::Failed,
                    "durable_invocation_preflight_failed",
                );
                return;
            };
            let lifecycle = match durable.begin_invocation_lifecycle(&invocation_id) {
                Ok(crate::durable_runtime::DurableActivity::Execute {
                    activity_id,
                    attempt,
                    ..
                }) => DurableInvocationLifecycleAttempt {
                    invocation_id,
                    activity_id,
                    attempt,
                },
                Ok(crate::durable_runtime::DurableActivity::ReuseCompleted { .. }) => {
                    let error = crate::error::AikitError::Conflict(
                        "durable invocation lifecycle identity was unexpectedly reused".into(),
                    );
                    yield StreamDelta::from_error(&error);
                    cfg.recorder.complete(
                        Usage::default(),
                        crate::session::RunTerminalStatus::Failed,
                        "durable_invocation_preflight_failed",
                    );
                    return;
                }
                Err(error) => {
                    let error = crate::error::AikitError::from(error);
                    yield StreamDelta::from_error(&error);
                    cfg.recorder.complete(
                        Usage::default(),
                        crate::session::RunTerminalStatus::Failed,
                        "durable_invocation_preflight_failed",
                    );
                    return;
                }
            };

            // Close the last cross-process race between the initial classification and fence CAS.
            // A receipt written by an older runtime is finalized without opening an audit span.
            match durable
                .invocation_disposition_for_active_lifecycle(&lifecycle.invocation_id)
            {
                Ok(crate::durable_runtime::DurableInvocationDisposition::Execute) => {}
                Ok(crate::durable_runtime::DurableInvocationDisposition::FinalizeTerminal(receipt)) => {
                    if let Err(error) = durable.complete_unstarted_invocation_lifecycle(
                        &lifecycle.activity_id,
                        lifecycle.attempt,
                    ) {
                        let error = crate::error::AikitError::from(error);
                        yield StreamDelta::from_error(&error);
                        cfg.recorder.complete(
                            receipt.usage,
                            crate::session::RunTerminalStatus::Failed,
                            "durable_commit_failure",
                        );
                        return;
                    }
                    if let Err(error) = persist_durable_terminal(durable, &receipt) {
                        let error = crate::error::AikitError::from(error);
                        yield StreamDelta::from_error(&error);
                        cfg.recorder.complete(
                            receipt.usage,
                            crate::session::RunTerminalStatus::Failed,
                            "durable_commit_failure",
                        );
                    } else {
                        cfg.recorder.complete(
                            receipt.usage,
                            recorded_terminal_status(&receipt.reason),
                            receipt.reason,
                        );
                    }
                    return;
                }
                Ok(disposition) => {
                    // RunStarted has not been attempted, so this fence can be closed
                    // non-ambiguously even when the concurrent state change itself requires
                    // reconciliation or an explicit resume.
                    if let Err(error) = durable.complete_unstarted_invocation_lifecycle(
                        &lifecycle.activity_id,
                        lifecycle.attempt,
                    ) {
                        let error = crate::error::AikitError::from(error);
                        yield StreamDelta::from_error(&error);
                        cfg.recorder.complete(
                            Usage::default(),
                            crate::session::RunTerminalStatus::Failed,
                            "durable_commit_failure",
                        );
                        return;
                    }
                    let reason = match disposition {
                        crate::durable_runtime::DurableInvocationDisposition::AwaitingResume { .. } => {
                            "durable_awaiting_resume"
                        }
                        crate::durable_runtime::DurableInvocationDisposition::ReconcileRequired { .. } => {
                            "durable_reconciliation_required"
                        }
                        crate::durable_runtime::DurableInvocationDisposition::AlreadyTerminal { .. } => {
                            "durable_already_terminal"
                        }
                        crate::durable_runtime::DurableInvocationDisposition::RetryTerminalAudit(_) => {
                            "durable_terminal_audit_retry_required"
                        }
                        crate::durable_runtime::DurableInvocationDisposition::Execute
                        | crate::durable_runtime::DurableInvocationDisposition::FinalizeTerminal(_) => {
                            unreachable!("handled above")
                        }
                    };
                    let error = crate::error::AikitError::Conflict(format!(
                        "durable invocation changed state before RunStarted: {reason}"
                    ));
                    yield StreamDelta::from_error(&error);
                    cfg.recorder.complete(
                        Usage::default(),
                        crate::session::RunTerminalStatus::Failed,
                        reason,
                    );
                    return;
                }
                Err(error) => {
                    let error = crate::error::AikitError::from(error);
                    yield StreamDelta::from_error(&error);
                    cfg.recorder.complete(
                        Usage::default(),
                        crate::session::RunTerminalStatus::Failed,
                        "durable_invocation_preflight_failed",
                    );
                    return;
                }
            }
            Some(lifecycle)
        } else {
            None
        };

        let run_id = cfg.audit.run_id().to_string();
        let validator_result = compile_tool_validators(&cfg.tools);
        let mut tool_validators = HashMap::new();
        let advertised_tools: HashSet<String> = cfg.tools.iter().map(|tool| tool.name.clone()).collect();
        let mut turn = 0usize;
        let mut total_usage = Usage::default();
        let mut terminal_reason = "end_turn".to_string();
        let mut budget = None;
        let mut can_run = true;
        let mut budget_error_emitted = false;
        let mut durable_boundary_stop = None;
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
            if let (Some(durable), Some(lifecycle)) =
                (cfg.durable.as_ref(), durable_invocation_lifecycle.as_ref())
            {
                if let Err(commit_error) = durable.fail_invocation_lifecycle(
                    &lifecycle.activity_id,
                    lifecycle.attempt,
                ) {
                    terminal_reason = "durable_commit_failure".into();
                    let commit_error = crate::error::AikitError::from(commit_error);
                    yield StreamDelta::from_error(&commit_error);
                }
            }
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
                compatibility_mode: cfg.compatibility_mode,
            };

            let mut durable_provider_attempt = None;
            let reused_provider_deltas = if let Some(durable) = &cfg.durable {
                match durable.begin_activity(
                    &format!("provider-stream-v1:{}", provider.name()),
                    &format!("turn-{turn}"),
                    durable_provider_input(provider.name(), &req),
                    crate::durability::SideEffectClass::ReconcileRequired,
                    None,
                ) {
                    Ok(crate::durable_runtime::DurableActivity::Execute {
                        activity_id,
                        attempt,
                        ..
                    }) => {
                        durable_provider_attempt = Some((activity_id, attempt));
                        None
                    }
                    Ok(crate::durable_runtime::DurableActivity::ReuseCompleted {
                        output,
                        ..
                    }) => match serde_json::from_value::<Vec<StreamDelta>>(output) {
                        Ok(deltas) => Some(deltas),
                        Err(error) => {
                            terminal_reason = "durable_state_error".into();
                            let error = crate::error::AikitError::Conflict(format!(
                                "recorded provider activity output is invalid: {error}"
                            ));
                            yield StreamDelta::from_error(&error);
                            break 'agent;
                        }
                    },
                    Err(error) => {
                        match resolve_durable_activity_boundary(
                            durable,
                            error,
                            durable_invocation_lifecycle
                                .as_ref()
                                .map(|lifecycle| lifecycle.invocation_id.as_str()),
                        ) {
                            Ok(disposition) => {
                                durable_boundary_stop = Some(disposition);
                                break 'agent;
                            }
                            Err(error) => {
                                terminal_reason = "durable_state_error".into();
                                yield StreamDelta::from_error(&error);
                                break 'agent;
                            }
                        }
                    }
                }
            } else {
                None
            };

            let mut inner = if let Some(deltas) = reused_provider_deltas {
                futures::stream::iter(deltas).boxed()
            } else {
                let provider_result = tokio::select! {
                    biased;
                    trigger = wait_for_termination(
                        &cfg.cancellation,
                        cfg.shared_wall_time_deadline,
                    ) => {
                        terminal_reason = trigger.terminal_reason().into();
                        if let Err(error) = finalize_ambiguous_durable_activity(
                            cfg.durable.as_ref(),
                            durable_provider_attempt.as_ref(),
                            "provider dispatch was cancelled before its outcome was committed",
                        ) {
                            terminal_reason = "durable_commit_failure".into();
                            yield StreamDelta::from_error(&error);
                        }
                        break 'agent;
                    }
                    result = provider.stream(req) => result,
                };
                match provider_result {
                    Ok(stream) => stream,
                    Err(e) => {
                        if let (Some(durable), Some((activity_id, attempt))) =
                            (&cfg.durable, durable_provider_attempt.as_ref())
                        {
                            let safe_error = e.info().message;
                            if let Err(error) = durable.fail_activity(
                                activity_id,
                                *attempt,
                                safe_error,
                                false,
                                true,
                            ) {
                                terminal_reason = "durable_commit_failure".into();
                                let error = crate::error::AikitError::from(error);
                                yield StreamDelta::from_error(&error);
                                break 'agent;
                            }
                        }
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
            let mut durable_provider_deltas = durable_provider_attempt
                .as_ref()
                .map(|_| Vec::<StreamDelta>::new());
            loop {
                let next_delta = tokio::select! {
                    biased;
                    trigger = wait_for_termination(
                        &cfg.cancellation,
                        cfg.shared_wall_time_deadline,
                    ) => {
                        terminal_reason = trigger.terminal_reason().into();
                        if let Err(error) = finalize_ambiguous_durable_activity(
                            cfg.durable.as_ref(),
                            durable_provider_attempt.as_ref(),
                            "provider stream was cancelled before its outcome was committed",
                        ) {
                            terminal_reason = "durable_commit_failure".into();
                            yield StreamDelta::from_error(&error);
                        }
                        break 'agent;
                    }
                    delta = inner.next() => delta,
                };
                let Some(delta) = next_delta else {
                    break;
                };
                if let Some(deltas) = &mut durable_provider_deltas {
                    deltas.push(delta.clone());
                }
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
                    StreamDelta::Warning { warning } => {
                        cfg.recorder.record_warning(warning.clone());
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

            if let (Some(durable), Some((activity_id, attempt))) =
                (&cfg.durable, durable_provider_attempt.as_ref())
            {
                let durable_result = if stream_error.is_some() {
                    let safe_message = stream_error
                        .as_ref()
                        .map(|(_, info)| info.message.clone())
                        .unwrap_or_else(|| "provider stream failed".into());
                    durable
                        .fail_activity(activity_id, *attempt, safe_message, false, true)
                        .map(|_| ())
                        .map_err(crate::error::AikitError::from)
                } else {
                    serde_json::to_value(
                        durable_provider_deltas
                            .as_ref()
                            .expect("executing durable provider records output"),
                    )
                        .map_err(|error| {
                            crate::error::AikitError::Conflict(format!(
                                "provider activity output cannot be serialized: {error}"
                            ))
                        })
                        .and_then(|output| {
                            durable
                                .complete_activity(activity_id, *attempt, output)
                                .map_err(crate::error::AikitError::from)
                        })
                };
                if let Err(error) = durable_result {
                    terminal_reason = "durable_commit_failure".into();
                    yield StreamDelta::from_error(&error);
                    break 'agent;
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
                        } else {
                            let mut durable_tool_attempt = None;
                            let reused_tool_output = if let Some(durable) = &cfg.durable {
                                match durable.begin_activity(
                                    &format!("tool-v1:{}", call.name),
                                    &format!("turn-{turn}:{}", call.id),
                                    durable_hashed_input(&serde_json::json!({
                                        "tool": &call.name,
                                        "input": &effective,
                                    })),
                                    crate::durability::SideEffectClass::ReconcileRequired,
                                    None,
                                ) {
                                    Ok(crate::durable_runtime::DurableActivity::Execute {
                                        activity_id,
                                        attempt,
                                        ..
                                    }) => {
                                        durable_tool_attempt = Some((activity_id, attempt));
                                        None
                                    }
                                    Ok(crate::durable_runtime::DurableActivity::ReuseCompleted {
                                        output,
                                        ..
                                    }) => {
                                        match serde_json::from_value::<DurableToolOutput>(output) {
                                            Ok(output) => Some(output),
                                            Err(error) => {
                                                terminal_reason = "durable_state_error".into();
                                                let error = crate::error::AikitError::Conflict(
                                                    format!(
                                                        "recorded tool activity output is invalid: {error}"
                                                    ),
                                                );
                                                yield StreamDelta::from_error(&error);
                                                break 'agent;
                                            }
                                        }
                                    }
                                    Err(error) => {
                                        match resolve_durable_activity_boundary(
                                            durable,
                                            error,
                                            durable_invocation_lifecycle
                                                .as_ref()
                                                .map(|lifecycle| lifecycle.invocation_id.as_str()),
                                        ) {
                                            Ok(disposition) => {
                                                durable_boundary_stop = Some(disposition);
                                                break 'agent;
                                            }
                                            Err(error) => {
                                                terminal_reason = "durable_state_error".into();
                                                yield StreamDelta::from_error(&error);
                                                break 'agent;
                                            }
                                        }
                                    }
                                }
                            } else {
                                None
                            };

                            if let Err(error) = cfg.audit.emit(AuditEvent::ToolStarted {
                                turn,
                                tool_use_id: call.id.clone(),
                                tool: call.name.clone(),
                                input: cfg.audit.capture_value(&effective),
                            }) {
                                terminal_reason = "audit_failure".into();
                                if let Err(commit_error) = fail_unstarted_durable_activity(
                                    cfg.durable.as_ref(),
                                    durable_tool_attempt.as_ref(),
                                    "tool start audit failed before execution",
                                ) {
                                    terminal_reason = "durable_commit_failure".into();
                                    yield StreamDelta::from_error(&commit_error);
                                }
                                yield StreamDelta::from_error(&error);
                                break 'agent;
                            }

                            if let Some(output) = reused_tool_output {
                                (output.content, output.is_error, Some(effective))
                            } else {
                                if let Some(trigger) = current_termination(
                                    &cfg.cancellation,
                                    cfg.shared_wall_time_deadline,
                                ) {
                                    terminal_reason = trigger.terminal_reason().into();
                                    if let Err(error) = finalize_ambiguous_durable_activity(
                                        cfg.durable.as_ref(),
                                        durable_tool_attempt.as_ref(),
                                        "tool dispatch was cancelled before execution completed",
                                    ) {
                                        terminal_reason = "durable_commit_failure".into();
                                        yield StreamDelta::from_error(&error);
                                    }
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
                                        if let Err(error) = finalize_ambiguous_durable_activity(
                                            cfg.durable.as_ref(),
                                            durable_tool_attempt.as_ref(),
                                            "tool execution was cancelled before its outcome was committed",
                                        ) {
                                            terminal_reason = "durable_commit_failure".into();
                                            yield StreamDelta::from_error(&error);
                                        }
                                        break 'agent;
                                    }
                                    result = executor.execute(&call.name, effective.clone()) => result,
                                };
                                let result = match execution {
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
                                                if let Err(error) = finalize_ambiguous_durable_activity(
                                                    cfg.durable.as_ref(),
                                                    durable_tool_attempt.as_ref(),
                                                    "tool post-processing was cancelled after dispatch",
                                                ) {
                                                    terminal_reason = "durable_commit_failure".into();
                                                    yield StreamDelta::from_error(&error);
                                                }
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
                                            if let Err(commit_error) =
                                                finalize_ambiguous_durable_activity(
                                                    cfg.durable.as_ref(),
                                                    durable_tool_attempt.as_ref(),
                                                    "tool audit failed after dispatch",
                                                )
                                            {
                                                terminal_reason = "durable_commit_failure".into();
                                                yield StreamDelta::from_error(&commit_error);
                                            }
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
                                                    if let Err(commit_error) =
                                                        finalize_ambiguous_durable_activity(
                                                            cfg.durable.as_ref(),
                                                            durable_tool_attempt.as_ref(),
                                                            "tool failure audit failed after dispatch",
                                                        )
                                                    {
                                                        terminal_reason =
                                                            "durable_commit_failure".into();
                                                        yield StreamDelta::from_error(&commit_error);
                                                    }
                                                    yield StreamDelta::from_error(&error);
                                                    break 'agent;
                                                }
                                                (failure.message, true, Some(effective))
                                            }
                                        }
                                    }
                                    Err(error) => {
                                        if let (Some(durable), Some((activity_id, attempt))) =
                                            (&cfg.durable, durable_tool_attempt.as_ref())
                                        {
                                            if let Err(commit_error) = durable.fail_activity(
                                                activity_id,
                                                *attempt,
                                                "tool execution failed after dispatch",
                                                false,
                                                true,
                                            ) {
                                                terminal_reason = "durable_commit_failure".into();
                                                let commit_error =
                                                    crate::error::AikitError::from(commit_error);
                                                yield StreamDelta::from_error(&commit_error);
                                                break 'agent;
                                            }
                                            terminal_reason =
                                                "durable_reconciliation_required".into();
                                            let error = crate::error::AikitError::Conflict(
                                                format!(
                                                    "tool '{}' may have produced an external effect; reconciliation is required",
                                                    call.name
                                                ),
                                            );
                                            yield StreamDelta::from_error(&error);
                                            break 'agent;
                                        }
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
                                };

                                if let (Some(durable), Some((activity_id, attempt))) =
                                    (&cfg.durable, durable_tool_attempt.as_ref())
                                {
                                    let output = DurableToolOutput {
                                        content: result.0.clone(),
                                        is_error: result.1,
                                    };
                                    let output = match serde_json::to_value(output) {
                                        Ok(output) => output,
                                        Err(error) => {
                                            terminal_reason = "durable_state_error".into();
                                            if let Err(commit_error) =
                                                finalize_ambiguous_durable_activity(
                                                    cfg.durable.as_ref(),
                                                    durable_tool_attempt.as_ref(),
                                                    "tool output serialization failed after dispatch",
                                                )
                                            {
                                                terminal_reason = "durable_commit_failure".into();
                                                yield StreamDelta::from_error(&commit_error);
                                            }
                                            let error = crate::error::AikitError::Conflict(
                                                format!(
                                                    "tool activity output cannot be serialized: {error}"
                                                ),
                                            );
                                            yield StreamDelta::from_error(&error);
                                            break 'agent;
                                        }
                                    };
                                    if let Err(error) =
                                        durable.complete_activity(activity_id, *attempt, output)
                                    {
                                        terminal_reason = "durable_commit_failure".into();
                                        let error = crate::error::AikitError::from(error);
                                        yield StreamDelta::from_error(&error);
                                        break 'agent;
                                    }
                                }
                                result
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

        if let Some(disposition) = durable_boundary_stop {
            let stopped_turns = turn.min(cfg.max_turns);
            let Some(lifecycle) = durable_invocation_lifecycle.as_ref() else {
                let error = crate::error::AikitError::Conflict(
                    "durable activity boundary has no persisted invocation lifecycle fence"
                        .into(),
                );
                yield StreamDelta::from_error(&error);
                cfg.recorder.complete(
                    total_usage,
                    crate::session::RunTerminalStatus::Failed,
                    "durable_commit_failure",
                );
                return;
            };
            let disposition = match disposition {
                crate::durable_runtime::DurableInvocationDisposition::FinalizeTerminal(receipt) => {
                    for delta in close_superseded_durable_invocation(
                        &cfg,
                        lifecycle,
                        stopped_turns,
                        receipt,
                    ) {
                        yield delta;
                    }
                    return;
                }
                disposition => disposition,
            };
            for delta in close_nonexecuting_durable_invocation(
                &cfg,
                lifecycle,
                stopped_turns,
                total_usage,
                disposition,
            ) {
                yield delta;
            }
            return;
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
        let stopped_turns = turn.min(cfg.max_turns);
        let terminal_receipt = crate::durable_runtime::DurableRunStoppedReceipt {
            turns: stopped_turns,
            reason: terminal_reason.clone(),
            usage: total_usage,
        };
        let mut durable_terminal_ready = terminal_reason != "durable_commit_failure";

        if let Some(durable) = &cfg.durable {
            let Some(lifecycle) = durable_invocation_lifecycle.as_ref() else {
                let error = crate::error::AikitError::Conflict(
                    "durable RunStarted has no persisted invocation lifecycle fence".into(),
                );
                yield StreamDelta::from_error(&error);
                cfg.recorder.complete(
                    total_usage,
                    crate::session::RunTerminalStatus::Failed,
                    "durable_commit_failure",
                );
                return;
            };
            if durable.is_poisoned() {
                terminal_reason = "durable_commit_failure".into();
                durable_terminal_ready = false;
            }
            let already_reconciling = matches!(
                durable.snapshot(),
                Ok(state)
                    if state.status()
                        == crate::durability::DurableRunStatus::ReconcileRequired
            );
            if !durable_terminal_ready || already_reconciling {
                // An earlier ambiguous effect or durable commit error prevents terminal state.
                // Still make a best-effort direct RunStopped delivery so accepting sinks do not
                // retain an unmatched RunStarted. The open/ambiguous lifecycle fence remains the
                // durable authority that forces reconciliation after any partial fan-out.
                durable_terminal_ready = false;
                if let Err(error) = cfg.audit.emit(AuditEvent::RunStopped {
                    turns: stopped_turns,
                    reason: terminal_reason.clone(),
                }) {
                    terminal_reason = "audit_failure".into();
                    yield StreamDelta::from_error(&error);
                }
            }
            if durable_terminal_ready {
                let (replay, prepared_audit) = match durable_run_stopped_replay(
                    &cfg,
                    crate::durable_runtime::DurableRunStoppedAuditKind::Canonical,
                    terminal_receipt.clone(),
                    stopped_turns,
                    terminal_reason.clone(),
                ) {
                    Ok(replay) => replay,
                    Err(error) => {
                        terminal_reason = "durable_commit_failure".into();
                        yield StreamDelta::from_error(&error);
                        if let Err(audit_error) = cfg.audit.emit(AuditEvent::RunStopped {
                            turns: stopped_turns,
                            reason: terminal_reason.clone(),
                        }) {
                            terminal_reason = "audit_failure".into();
                            yield StreamDelta::from_error(&audit_error);
                        }
                        cfg.recorder.complete(
                            total_usage,
                            crate::session::RunTerminalStatus::Failed,
                            terminal_reason.clone(),
                        );
                        return;
                    }
                };
                match durable.begin_invocation_run_stopped_audit(&replay) {
                    Ok(crate::durable_runtime::DurableActivity::Execute {
                        activity_id,
                        attempt,
                        ..
                    }) => {
                        match cfg.audit.emit_prepared(prepared_audit) {
                            Ok(()) => {
                                if let Err(error) = durable
                                    .complete_run_stopped_audit_and_invocation_lifecycle(
                                        &activity_id,
                                        attempt,
                                        &lifecycle.activity_id,
                                        lifecycle.attempt,
                                        &replay,
                                    )
                                {
                                    durable_terminal_ready = false;
                                    terminal_reason = "durable_commit_failure".into();
                                    let error = crate::error::AikitError::from(error);
                                    yield StreamDelta::from_error(&error);
                                }
                            }
                            Err(error) => {
                                durable_terminal_ready = false;
                                terminal_reason = "audit_failure".into();
                                yield StreamDelta::from_error(&error);
                                if let Err(error) = durable
                                    .fail_run_stopped_audit_and_invocation_lifecycle(
                                        &activity_id,
                                        attempt,
                                        &lifecycle.activity_id,
                                        lifecycle.attempt,
                                    )
                                {
                                    terminal_reason = "durable_commit_failure".into();
                                    let error = crate::error::AikitError::from(error);
                                    yield StreamDelta::from_error(&error);
                                }
                            }
                        }
                    }
                    Ok(crate::durable_runtime::DurableActivity::ReuseCompleted { output, .. }) => {
                        match serde_json::from_value::<
                            crate::durable_runtime::DurableRunStoppedReceipt,
                        >(output) {
                            Ok(receipt) => {
                                for delta in close_superseded_durable_invocation(
                                    &cfg,
                                    lifecycle,
                                    stopped_turns,
                                    receipt,
                                ) {
                                    yield delta;
                                }
                            }
                            Err(error) => {
                                let error = crate::error::AikitError::Conflict(format!(
                                    "persisted RunStopped receipt is invalid: {error}"
                                ));
                                yield StreamDelta::from_error(&error);
                                if let Err(audit_error) = cfg.audit.emit(AuditEvent::RunStopped {
                                    turns: stopped_turns,
                                    reason: "durable_commit_failure".into(),
                                }) {
                                    yield StreamDelta::from_error(&audit_error);
                                }
                                cfg.recorder.complete(
                                    total_usage,
                                    crate::session::RunTerminalStatus::Failed,
                                    "durable_commit_failure",
                                );
                            }
                        }
                        return;
                    }
                    Err(error) => {
                        match resolve_durable_activity_boundary(
                            durable,
                            error,
                            durable_invocation_lifecycle
                                .as_ref()
                                .map(|lifecycle| lifecycle.invocation_id.as_str()),
                        ) {
                            Ok(crate::durable_runtime::DurableInvocationDisposition::FinalizeTerminal(receipt)) => {
                                for delta in close_superseded_durable_invocation(
                                    &cfg,
                                    lifecycle,
                                    stopped_turns,
                                    receipt,
                                ) {
                                    yield delta;
                                }
                                return;
                            }
                            Ok(disposition) => {
                                for delta in close_nonexecuting_durable_invocation(
                                    &cfg,
                                    lifecycle,
                                    stopped_turns,
                                    total_usage,
                                    disposition,
                                ) {
                                    yield delta;
                                }
                                return;
                            }
                            Err(error) => {
                                durable_terminal_ready = false;
                                terminal_reason = "durable_commit_failure".into();
                                yield StreamDelta::from_error(&error);
                            }
                        }
                    }
                }
            }

            if durable_terminal_ready {
                let durable_result = persist_durable_terminal(durable, &terminal_receipt);
                if let Err(error) = durable_result {
                    terminal_reason = "durable_commit_failure".into();
                    let error = crate::error::AikitError::from(error);
                    yield StreamDelta::from_error(&error);
                }
            }
        } else if let Err(error) = cfg.audit.emit(AuditEvent::RunStopped {
            turns: stopped_turns,
            reason: terminal_reason.clone(),
        }) {
            terminal_reason = "audit_failure".into();
            yield StreamDelta::from_error(&error);
        }
        let status = recorded_terminal_status(&terminal_reason);
        cfg.recorder
            .complete(total_usage, status, terminal_reason);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::durable_store::DurableStore as _;
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

    struct CountingStopProvider {
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl Provider for CountingStopProvider {
        fn name(&self) -> &str {
            "counting-stop"
        }

        async fn stream(
            &self,
            _req: ProviderRequest,
        ) -> crate::error::Result<BoxStream<'static, StreamDelta>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(Box::pin(futures::stream::iter(vec![
                StreamDelta::MessageStart {
                    model: "durable-model".into(),
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

    struct CountingToolProvider {
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl Provider for CountingToolProvider {
        fn name(&self) -> &str {
            "counting-tool"
        }

        async fn stream(
            &self,
            _req: ProviderRequest,
        ) -> crate::error::Result<BoxStream<'static, StreamDelta>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(Box::pin(futures::stream::iter(vec![
                StreamDelta::MessageStart {
                    model: "durable-model".into(),
                },
                StreamDelta::ToolCallStart {
                    id: "call-1".into(),
                    name: "write".into(),
                },
                StreamDelta::ToolCallInput {
                    id: "call-1".into(),
                    input: serde_json::json!({"value": 1}),
                },
                StreamDelta::MessageStop {
                    stop_reason: "tool_use".into(),
                },
            ])))
        }
    }

    struct BlockingProvider {
        calls: Arc<AtomicUsize>,
        started: Arc<tokio::sync::Notify>,
    }

    #[async_trait]
    impl Provider for BlockingProvider {
        fn name(&self) -> &str {
            "blocking-provider"
        }

        async fn stream(
            &self,
            _req: ProviderRequest,
        ) -> crate::error::Result<BoxStream<'static, StreamDelta>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.started.notify_one();
            futures::future::pending().await
        }
    }

    struct CountingTool {
        calls: Arc<AtomicUsize>,
        fail: bool,
    }

    #[async_trait]
    impl ToolExecutor for CountingTool {
        async fn execute(
            &self,
            _name: &str,
            _input: serde_json::Value,
        ) -> crate::error::Result<String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.fail {
                Err(crate::error::AikitError::ToolExecution(
                    "uncertain external write".into(),
                ))
            } else {
                Ok("written".into())
            }
        }
    }

    #[derive(Clone, Copy)]
    enum TerminalReceiptTrigger {
        RunStarted,
        PermissionDecision,
    }

    impl TerminalReceiptTrigger {
        fn matches(self, event: &crate::observability::AuditEvent) -> bool {
            matches!(
                (self, event),
                (
                    Self::RunStarted,
                    crate::observability::AuditEvent::RunStarted { .. }
                ) | (
                    Self::PermissionDecision,
                    crate::observability::AuditEvent::PermissionDecision { .. }
                )
            )
        }
    }

    struct CommitTerminalReceiptOnAuditEvent {
        records: Arc<crate::observability::InMemoryAuditSink>,
        driver: crate::durable_runtime::DurableRunDriver,
        receipt: crate::durable_runtime::DurableRunStoppedReceipt,
        audit_binding: crate::observability::AuditReplayBinding,
        trigger: TerminalReceiptTrigger,
        committed: AtomicBool,
    }

    fn test_replay_binding(
        sink_count: usize,
        failure_mode: crate::observability::AuditFailureMode,
    ) -> crate::observability::AuditReplayBinding {
        crate::observability::AuditReplayBinding {
            schema_version: 1,
            delivery_id: None,
            sink_count,
            payload_policy: crate::observability::AuditPayloadPolicy::MetadataOnly,
            failure_mode,
            max_preview_bytes: 4096,
        }
    }

    impl crate::observability::AuditSink for CommitTerminalReceiptOnAuditEvent {
        fn record(
            &self,
            record: &crate::observability::AuditRecord,
        ) -> std::result::Result<(), String> {
            crate::observability::AuditSink::record(self.records.as_ref(), record)?;
            if self.trigger.matches(&record.event) && !self.committed.swap(true, Ordering::SeqCst) {
                let replay = crate::durable_runtime::DurableRunStoppedAuditReplayEnvelope::new(
                    crate::durable_runtime::DurableRunStoppedAuditKind::Canonical,
                    self.receipt.clone(),
                    record.run_id.clone(),
                    record.effective_invocation_id(),
                    record.sequence.saturating_add(1),
                    self.receipt.turns,
                    self.receipt.reason.clone(),
                    self.audit_binding.clone(),
                );
                let activity = self
                    .driver
                    .begin_invocation_run_stopped_audit(&replay)
                    .map_err(|error| error.to_string())?;
                let crate::durable_runtime::DurableActivity::Execute {
                    activity_id,
                    attempt,
                    ..
                } = activity
                else {
                    return Err("terminal receipt injection unexpectedly reused an activity".into());
                };
                crate::observability::AuditSink::record(
                    self.records.as_ref(),
                    &crate::observability::AuditRecord {
                        run_id: record.run_id.clone(),
                        invocation_id: record.invocation_id.clone(),
                        parent_run_id: record.parent_run_id.clone(),
                        run_label: record.run_label.clone(),
                        sequence: replay.run_stopped_sequence,
                        unix_ms: record.unix_ms,
                        event: crate::observability::AuditEvent::RunStopped {
                            turns: self.receipt.turns,
                            reason: self.receipt.reason.clone(),
                        },
                    },
                )?;
                self.driver
                    .complete_activity(
                        &activity_id,
                        attempt,
                        serde_json::to_value(&self.receipt).map_err(|error| error.to_string())?,
                    )
                    .map_err(|error| error.to_string())?;
            }
            Ok(())
        }
    }

    struct FailNthCasStore {
        inner: crate::durable_store::InMemoryDurableStore,
        fail_at: usize,
        calls: AtomicUsize,
    }

    impl FailNthCasStore {
        fn new(fail_at: usize) -> Self {
            Self {
                inner: crate::durable_store::InMemoryDurableStore::default(),
                fail_at,
                calls: AtomicUsize::new(0),
            }
        }
    }

    impl crate::durable_store::DurableStore for FailNthCasStore {
        fn create(
            &self,
            state: &crate::durability::RunState,
        ) -> crate::durable_store::DurableStoreResult<()> {
            self.inner.create(state)
        }

        fn load(
            &self,
            run_id: &str,
        ) -> crate::durable_store::DurableStoreResult<crate::durability::RunState> {
            self.inner.load(run_id)
        }

        fn compare_and_swap(
            &self,
            expected_sequence: u64,
            replacement: &crate::durability::RunState,
        ) -> crate::durable_store::DurableStoreResult<()> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            if call == self.fail_at {
                return Err(crate::durable_store::DurableStoreError::Io(
                    "planned CAS failure".into(),
                ));
            }
            self.inner.compare_and_swap(expected_sequence, replacement)
        }
    }

    fn durable_tool_spec() -> ToolSpec {
        ToolSpec {
            name: "write".into(),
            description: "write an external value".into(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {"value": {"type": "integer"}},
                "required": ["value"]
            }),
        }
    }

    async fn drain(stream: impl Stream<Item = StreamDelta>) -> Vec<StreamDelta> {
        futures::pin_mut!(stream);
        let mut deltas = Vec::new();
        while let Some(delta) = stream.next().await {
            deltas.push(delta);
        }
        deltas
    }

    #[tokio::test]
    async fn durable_run_id_is_the_runtime_and_audit_identity() {
        let audit_sink = Arc::new(crate::observability::InMemoryAuditSink::default());
        let store = Arc::new(crate::durable_store::InMemoryDurableStore::default());
        let state = crate::durability::RunState::new(
            "session",
            "authoritative-run",
            crate::durability::DurabilityMode::Sync,
        )
        .unwrap();
        let driver = crate::durable_runtime::DurableRunDriver::new(state, store.clone()).unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let mut cfg = RunConfig::new("durable-model", vec![Message::user("hello")])
            .with_durable_driver(driver);
        cfg.audit = crate::observability::AuditTrail::new().with_sink(audit_sink.clone());

        drain(run_agent(
            Arc::new(CountingStopProvider {
                calls: calls.clone(),
            }),
            Arc::new(EchoTool),
            cfg,
        ))
        .await;

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let records = audit_sink.records();
        assert!(!records.is_empty());
        assert!(records
            .iter()
            .all(|record| record.run_id == "authoritative-run"));
        let stored = store.load("authoritative-run").unwrap();
        assert_eq!(
            stored.status(),
            crate::durability::DurableRunStatus::Completed
        );
    }

    #[tokio::test]
    async fn provider_is_not_called_when_pre_side_effect_cas_fails() {
        // The invocation lifecycle fence is CAS 1; fail provider scheduling at CAS 2.
        let store = Arc::new(FailNthCasStore::new(2));
        let state = crate::durability::RunState::new(
            "session",
            "provider-cas-run",
            crate::durability::DurabilityMode::Sync,
        )
        .unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let audit = Arc::new(crate::observability::InMemoryAuditSink::default());
        let recorder = crate::session::RunRecorder::default();
        let mut cfg = RunConfig::new("durable-model", vec![Message::user("hello")])
            .with_durable_run(state, store.clone())
            .unwrap();
        cfg.audit = crate::observability::AuditTrail::new().with_sink(audit.clone());
        cfg.recorder = recorder.clone();

        let deltas = drain(run_agent(
            Arc::new(CountingStopProvider {
                calls: calls.clone(),
            }),
            Arc::new(EchoTool),
            cfg,
        ))
        .await;

        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(deltas.iter().any(|delta| matches!(
            delta,
            StreamDelta::Error { info, .. }
                if info.code == crate::error::ErrorCode::Conflict
        )));
        let lifecycle = audit
            .records()
            .into_iter()
            .filter(|record| {
                matches!(
                    record.event,
                    crate::observability::AuditEvent::RunStarted { .. }
                        | crate::observability::AuditEvent::RunStopped { .. }
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(lifecycle.len(), 2);
        assert_eq!(
            lifecycle[0].effective_invocation_id(),
            lifecycle[1].effective_invocation_id()
        );
        assert!(matches!(
            &lifecycle[1].event,
            crate::observability::AuditEvent::RunStopped { reason, .. }
                if reason == "durable_commit_failure"
        ));
        let outcome = recorder.outcome();
        assert_eq!(
            outcome.terminal_status,
            crate::session::RunTerminalStatus::Failed
        );
        assert_eq!(
            outcome.stop_reason.as_deref(),
            Some("durable_commit_failure")
        );
        assert_eq!(
            store.load("provider-cas-run").unwrap().status(),
            crate::DurableRunStatus::Running
        );
    }

    #[tokio::test]
    async fn partial_run_started_and_failed_fence_update_still_attempts_run_stopped() {
        // CAS 1 opens the lifecycle fence. Fail CAS 2 while recording that RunStarted fan-out was
        // ambiguous; the runtime must still attempt a direct matching RunStopped.
        let store = Arc::new(FailNthCasStore::new(2));
        let state = crate::RunState::new(
            "session",
            "run-started-fence-double-fault",
            crate::DurabilityMode::Sync,
        )
        .unwrap();
        let accepted = Arc::new(crate::observability::InMemoryAuditSink::default());
        let provider_calls = Arc::new(AtomicUsize::new(0));
        let recorder = crate::session::RunRecorder::default();
        let mut cfg = RunConfig::new("durable-model", vec![Message::user("hello")])
            .with_durable_run(state, store.clone())
            .unwrap();
        cfg.audit = crate::observability::AuditTrail::new()
            .with_sink(accepted.clone())
            .with_sink(Arc::new(FailOnAuditEvent("run_started")))
            .with_failure_mode(crate::observability::AuditFailureMode::FailClosed);
        cfg.recorder = recorder.clone();

        let deltas = drain(run_agent(
            Arc::new(CountingStopProvider {
                calls: provider_calls.clone(),
            }),
            Arc::new(EchoTool),
            cfg,
        ))
        .await;

        assert_eq!(provider_calls.load(Ordering::SeqCst), 0);
        assert!(deltas.iter().any(|delta| matches!(
            delta,
            StreamDelta::Error { info, .. }
                if info.code == crate::error::ErrorCode::Audit
        )));
        assert!(deltas.iter().any(|delta| matches!(
            delta,
            StreamDelta::Error { message, .. } if message.contains("planned CAS failure")
        )));
        let lifecycle = accepted
            .records()
            .into_iter()
            .filter(|record| {
                matches!(
                    record.event,
                    crate::observability::AuditEvent::RunStarted { .. }
                        | crate::observability::AuditEvent::RunStopped { .. }
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(lifecycle.len(), 2);
        assert_eq!(
            lifecycle[0].effective_invocation_id(),
            lifecycle[1].effective_invocation_id()
        );
        assert!(matches!(
            &lifecycle[1].event,
            crate::observability::AuditEvent::RunStopped { reason, .. }
                if reason == "durable_commit_failure"
        ));
        assert_eq!(
            recorder.outcome().stop_reason.as_deref(),
            Some("durable_commit_failure")
        );
        assert_eq!(
            store
                .load("run-started-fence-double-fault")
                .unwrap()
                .status(),
            crate::DurableRunStatus::Running
        );

        let restarted = crate::DurableRunDriver::new(
            store.load("run-started-fence-double-fault").unwrap(),
            store.clone(),
        )
        .unwrap();
        let retry_audit = Arc::new(crate::observability::InMemoryAuditSink::default());
        let mut retry_cfg = RunConfig::new("durable-model", vec![Message::user("hello")])
            .with_durable_driver(restarted);
        retry_cfg.audit = crate::observability::AuditTrail::new().with_sink(retry_audit.clone());
        drain(run_agent(
            Arc::new(CountingStopProvider {
                calls: provider_calls.clone(),
            }),
            Arc::new(EchoTool),
            retry_cfg,
        ))
        .await;

        assert_eq!(provider_calls.load(Ordering::SeqCst), 0);
        assert!(retry_audit.records().is_empty());
        assert_eq!(
            store
                .load("run-started-fence-double-fault")
                .unwrap()
                .status(),
            crate::DurableRunStatus::ReconcileRequired
        );
    }

    #[tokio::test]
    async fn tool_is_not_called_when_its_pre_side_effect_cas_fails() {
        // The lifecycle fence and provider start/completion consume CAS calls 1..=3.
        let store = Arc::new(FailNthCasStore::new(4));
        let state = crate::durability::RunState::new(
            "session",
            "tool-cas-run",
            crate::durability::DurabilityMode::Sync,
        )
        .unwrap();
        let provider_calls = Arc::new(AtomicUsize::new(0));
        let tool_calls = Arc::new(AtomicUsize::new(0));
        let mut cfg = RunConfig::new("durable-model", vec![Message::user("hello")])
            .with_durable_run(state, store)
            .unwrap();
        cfg.tools = vec![durable_tool_spec()];

        drain(run_agent(
            Arc::new(CountingToolProvider {
                calls: provider_calls.clone(),
            }),
            Arc::new(CountingTool {
                calls: tool_calls.clone(),
                fail: false,
            }),
            cfg,
        ))
        .await;

        assert_eq!(provider_calls.load(Ordering::SeqCst), 1);
        assert_eq!(tool_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn completed_provider_activity_is_reused_without_provider_execution() {
        let store = Arc::new(crate::durable_store::InMemoryDurableStore::default());
        let state = crate::durability::RunState::new(
            "session",
            "provider-reuse-run",
            crate::durability::DurabilityMode::Sync,
        )
        .unwrap();
        let driver = crate::durable_runtime::DurableRunDriver::new(state, store.clone()).unwrap();
        let cfg = RunConfig::new("durable-model", vec![Message::user("hello")]);
        let req = ProviderRequest {
            model: cfg.model.clone(),
            messages: cfg.messages.clone(),
            tools: cfg.tools.clone(),
            max_tokens: cfg.max_tokens,
            options: cfg.options.clone(),
            provider_options: cfg.provider_options.clone(),
            compatibility_mode: cfg.compatibility_mode,
        };
        let activity = driver
            .begin_activity(
                "provider-stream-v1:counting-stop",
                "turn-1",
                durable_provider_input("counting-stop", &req),
                crate::durability::SideEffectClass::ReconcileRequired,
                None,
            )
            .unwrap();
        let crate::durable_runtime::DurableActivity::Execute {
            activity_id,
            attempt,
            ..
        } = activity
        else {
            panic!("fresh provider activity must execute");
        };
        let recorded = vec![
            StreamDelta::MessageStart {
                model: "durable-model".into(),
            },
            StreamDelta::TextDelta {
                text: "recorded".into(),
            },
            StreamDelta::MessageStop {
                stop_reason: "end_turn".into(),
            },
        ];
        driver
            .complete_activity(
                &activity_id,
                attempt,
                serde_json::to_value(&recorded).unwrap(),
            )
            .unwrap();
        let calls = Arc::new(AtomicUsize::new(0));

        let deltas = drain(run_agent(
            Arc::new(CountingStopProvider {
                calls: calls.clone(),
            }),
            Arc::new(EchoTool),
            cfg.with_durable_driver(driver),
        ))
        .await;

        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(deltas
            .iter()
            .any(|delta| matches!(delta, StreamDelta::TextDelta { text } if text == "recorded")));
    }

    #[tokio::test]
    async fn ambiguous_tool_failure_requires_reconciliation() {
        let store = Arc::new(crate::durable_store::InMemoryDurableStore::default());
        let state = crate::durability::RunState::new(
            "session",
            "ambiguous-tool-run",
            crate::durability::DurabilityMode::Sync,
        )
        .unwrap();
        let tool_calls = Arc::new(AtomicUsize::new(0));
        let mut cfg = RunConfig::new("durable-model", vec![Message::user("hello")])
            .with_durable_run(state, store.clone())
            .unwrap();
        cfg.tools = vec![durable_tool_spec()];

        let deltas = drain(run_agent(
            Arc::new(CountingToolProvider {
                calls: Arc::new(AtomicUsize::new(0)),
            }),
            Arc::new(CountingTool {
                calls: tool_calls.clone(),
                fail: true,
            }),
            cfg,
        ))
        .await;

        assert_eq!(tool_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            store.load("ambiguous-tool-run").unwrap().status(),
            crate::durability::DurableRunStatus::ReconcileRequired
        );
        assert!(deltas.iter().any(|delta| matches!(
            delta,
            StreamDelta::Error { message, .. } if message.contains("reconciliation")
        )));
    }

    #[tokio::test]
    async fn governed_durable_run_rejects_default_governance_before_provider_io() {
        let policy = crate::governance::PolicySnapshot::seal(crate::governance::PolicyDocument {
            schema_version: crate::governance::GOVERNANCE_CONTRACT_VERSION,
            default_effect: crate::governance::PolicyEffect::Deny,
            rules: Vec::new(),
        })
        .unwrap();
        let bound = crate::governance::Governance::default().with_policy_snapshot(policy);
        let state = bound
            .start_durable_run(
                "session",
                "deny-default-bypass",
                crate::DurabilityMode::Sync,
            )
            .unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let recorder = crate::session::RunRecorder::default();
        let mut cfg = RunConfig::new("durable-model", vec![Message::user("hello")])
            .with_durable_run(
                state,
                Arc::new(crate::durable_store::InMemoryDurableStore::default()),
            )
            .unwrap();
        cfg.recorder = recorder.clone();

        let deltas = drain(run_agent(
            Arc::new(CountingStopProvider {
                calls: calls.clone(),
            }),
            Arc::new(EchoTool),
            cfg,
        ))
        .await;

        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(deltas.iter().any(|delta| matches!(
            delta,
            StreamDelta::Error { info, .. }
                if info.code == crate::error::ErrorCode::Configuration
        )));
        let outcome = recorder.outcome();
        assert_eq!(
            outcome.terminal_status,
            crate::session::RunTerminalStatus::Failed
        );
        assert_eq!(
            outcome.stop_reason.as_deref(),
            Some("durable_invocation_preflight_failed")
        );
    }

    #[tokio::test]
    async fn wrong_policy_tenant_or_agent_is_rejected_before_provider_io() {
        let deny = crate::governance::PolicySnapshot::seal(crate::governance::PolicyDocument {
            schema_version: crate::governance::GOVERNANCE_CONTRACT_VERSION,
            default_effect: crate::governance::PolicyEffect::Deny,
            rules: Vec::new(),
        })
        .unwrap();
        let allow = crate::governance::PolicySnapshot::seal(crate::governance::PolicyDocument {
            schema_version: crate::governance::GOVERNANCE_CONTRACT_VERSION,
            default_effect: crate::governance::PolicyEffect::Allow,
            rules: Vec::new(),
        })
        .unwrap();
        let bound = crate::governance::Governance::default()
            .with_policy_snapshot(deny.clone())
            .with_policy_identity(Some("tenant-a".into()), Some("agent-a".into()));
        let candidates = [
            crate::governance::Governance::default()
                .with_policy_snapshot(allow)
                .with_policy_identity(Some("tenant-a".into()), Some("agent-a".into())),
            crate::governance::Governance::default()
                .with_policy_snapshot(deny.clone())
                .with_policy_identity(Some("tenant-b".into()), Some("agent-a".into())),
            crate::governance::Governance::default()
                .with_policy_snapshot(deny)
                .with_policy_identity(Some("tenant-a".into()), Some("agent-b".into())),
        ];

        for (index, governance) in candidates.into_iter().enumerate() {
            let run_id = format!("identity-mismatch-{index}");
            let state = bound
                .start_durable_run("session", &run_id, crate::DurabilityMode::Sync)
                .unwrap();
            let calls = Arc::new(AtomicUsize::new(0));
            let mut cfg = RunConfig::new("durable-model", vec![Message::user("hello")])
                .with_durable_run(
                    state,
                    Arc::new(crate::durable_store::InMemoryDurableStore::default()),
                )
                .unwrap();
            cfg.governance = governance;

            drain(run_agent(
                Arc::new(CountingStopProvider {
                    calls: calls.clone(),
                }),
                Arc::new(EchoTool),
                cfg,
            ))
            .await;
            assert_eq!(calls.load(Ordering::SeqCst), 0, "case {index}");
        }
    }

    #[tokio::test]
    async fn provider_completion_cas_failure_stays_resumable_and_reconciles_on_restart() {
        // Lifecycle fence and provider start are CAS 1..=2; fail provider completion at CAS 3.
        let store = Arc::new(FailNthCasStore::new(3));
        let state = crate::RunState::new(
            "session",
            "provider-completion-cas",
            crate::DurabilityMode::Sync,
        )
        .unwrap();
        let driver = crate::DurableRunDriver::new(state, store.clone()).unwrap();
        let calls = Arc::new(AtomicUsize::new(0));

        drain(run_agent(
            Arc::new(CountingStopProvider {
                calls: calls.clone(),
            }),
            Arc::new(EchoTool),
            RunConfig::new("durable-model", vec![Message::user("hello")])
                .with_durable_driver(driver.clone()),
        ))
        .await;

        assert!(driver.is_poisoned());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            store.load("provider-completion-cas").unwrap().status(),
            crate::DurableRunStatus::Running
        );

        let restarted = crate::DurableRunDriver::new(
            store.load("provider-completion-cas").unwrap(),
            store.clone(),
        )
        .unwrap();
        drain(run_agent(
            Arc::new(CountingStopProvider {
                calls: calls.clone(),
            }),
            Arc::new(EchoTool),
            RunConfig::new("durable-model", vec![Message::user("hello")])
                .with_durable_driver(restarted),
        ))
        .await;

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            store.load("provider-completion-cas").unwrap().status(),
            crate::DurableRunStatus::ReconcileRequired
        );
    }

    #[tokio::test]
    async fn terminal_cas_retry_reuses_delivered_audit_without_rerunning_provider() {
        // Lifecycle fence, provider start/completion, audit intent, and the atomic audit/fence
        // completion are CAS calls 1..=5. Fail only terminal CAS after both receipts are persisted.
        let store = Arc::new(FailNthCasStore::new(6));
        let state =
            crate::RunState::new("session", "terminal-cas-retry", crate::DurabilityMode::Sync)
                .unwrap();
        let driver = crate::DurableRunDriver::new(state, store.clone()).unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let audit = Arc::new(crate::observability::InMemoryAuditSink::default());

        let mut cfg = RunConfig::new("durable-model", vec![Message::user("hello")])
            .with_durable_driver(driver.clone());
        cfg.audit = crate::observability::AuditTrail::new().with_sink(audit.clone());
        drain(run_agent(
            Arc::new(CountingStopProvider {
                calls: calls.clone(),
            }),
            Arc::new(EchoTool),
            cfg,
        ))
        .await;

        assert!(driver.is_poisoned());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            store.load("terminal-cas-retry").unwrap().status(),
            crate::DurableRunStatus::Running
        );

        let restarted =
            crate::DurableRunDriver::new(store.load("terminal-cas-retry").unwrap(), store.clone())
                .unwrap();
        let audit_records_before_retry = audit.records();
        let retry_recorder = crate::session::RunRecorder::default();
        let mut restarted_cfg = RunConfig::new("durable-model", vec![Message::user("hello")])
            .with_durable_driver(restarted);
        restarted_cfg.audit = crate::observability::AuditTrail::new().with_sink(audit.clone());
        restarted_cfg.recorder = retry_recorder.clone();
        drain(run_agent(
            Arc::new(CountingStopProvider {
                calls: calls.clone(),
            }),
            Arc::new(EchoTool),
            restarted_cfg,
        ))
        .await;

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            store.load("terminal-cas-retry").unwrap().status(),
            crate::DurableRunStatus::Completed
        );
        assert_eq!(
            retry_recorder.outcome().terminal_status,
            crate::session::RunTerminalStatus::Completed
        );
        assert_eq!(
            audit.records(),
            audit_records_before_retry,
            "receipt-only terminal retry must not open an unmatched audit invocation"
        );
        let lifecycle = audit_records_before_retry
            .iter()
            .filter_map(|record| match record.event {
                crate::observability::AuditEvent::RunStarted { .. } => {
                    Some(("started", record.effective_invocation_id().to_string()))
                }
                crate::observability::AuditEvent::RunStopped { .. } => {
                    Some(("stopped", record.effective_invocation_id().to_string()))
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(lifecycle.len(), 2);
        assert_eq!(lifecycle[0].0, "started");
        assert_eq!(lifecycle[1].0, "stopped");
        assert_eq!(lifecycle[0].1, lifecycle[1].1);
        assert_eq!(
            audit
                .records()
                .iter()
                .filter(|record| {
                    matches!(
                        record.event,
                        crate::observability::AuditEvent::RunStopped { .. }
                    )
                })
                .count(),
            1,
            "the persisted delivery receipt must suppress duplicate RunStopped delivery"
        );
    }

    #[tokio::test]
    async fn uncommitted_run_stopped_delivery_requires_reconciliation_before_restart_work() {
        let store = Arc::new(crate::durable_store::InMemoryDurableStore::default());
        let state = crate::RunState::new(
            "session",
            "run-stopped-delivery-crash",
            crate::DurabilityMode::Sync,
        )
        .unwrap();
        let driver = crate::DurableRunDriver::new(state, store.clone()).unwrap();
        let receipt = crate::durable_runtime::DurableRunStoppedReceipt {
            turns: 1,
            reason: "end_turn".into(),
            usage: Usage::default(),
        };
        assert!(matches!(
            driver.begin_run_stopped_audit(&receipt).unwrap(),
            crate::durable_runtime::DurableActivity::Execute { .. }
        ));
        drop(driver);

        let restarted = crate::DurableRunDriver::new(
            store.load("run-stopped-delivery-crash").unwrap(),
            store.clone(),
        )
        .unwrap();
        let provider_calls = Arc::new(AtomicUsize::new(0));
        let tool_calls = Arc::new(AtomicUsize::new(0));
        let audit = Arc::new(crate::observability::InMemoryAuditSink::default());
        let mut cfg = RunConfig::new("durable-model", vec![Message::user("hello")])
            .with_durable_driver(restarted);
        cfg.audit = crate::observability::AuditTrail::new().with_sink(audit.clone());
        let deltas = drain(run_agent(
            Arc::new(CountingStopProvider {
                calls: provider_calls.clone(),
            }),
            Arc::new(CountingTool {
                calls: tool_calls.clone(),
                fail: false,
            }),
            cfg,
        ))
        .await;

        assert!(deltas.iter().any(|delta| matches!(
            delta,
            StreamDelta::Error { message, .. } if message.contains("explicit reconciliation")
        )));
        assert_eq!(provider_calls.load(Ordering::SeqCst), 0);
        assert_eq!(tool_calls.load(Ordering::SeqCst), 0);
        assert!(audit.records().is_empty());
        assert_eq!(
            store.load("run-stopped-delivery-crash").unwrap().status(),
            crate::DurableRunStatus::ReconcileRequired
        );
    }

    #[tokio::test]
    async fn preprepared_execute_is_revalidated_before_run_started() {
        let store = Arc::new(crate::durable_store::InMemoryDurableStore::default());
        let state = crate::RunState::new(
            "session",
            "preprepared-terminal-receipt",
            crate::DurabilityMode::Sync,
        )
        .unwrap();
        let driver = crate::DurableRunDriver::new(state, store.clone()).unwrap();
        let provider_calls = Arc::new(AtomicUsize::new(0));
        let audit = Arc::new(crate::observability::InMemoryAuditSink::default());
        let mut cfg = RunConfig::new("durable-model", vec![Message::user("hello")])
            .with_durable_driver(driver.clone());
        cfg.audit = crate::observability::AuditTrail::new().with_sink(audit.clone());
        cfg.prepare_invocation();
        assert!(matches!(
            cfg.durable_invocation.as_ref(),
            Some(crate::durable_runtime::DurableInvocationDisposition::Execute)
        ));

        let receipt = crate::durable_runtime::DurableRunStoppedReceipt {
            turns: 1,
            reason: "end_turn".into(),
            usage: Usage::default(),
        };
        let crate::durable_runtime::DurableActivity::Execute {
            activity_id,
            attempt,
            ..
        } = driver.begin_run_stopped_audit(&receipt).unwrap()
        else {
            panic!("fresh terminal audit intent must execute");
        };
        driver
            .complete_run_stopped_audit(&activity_id, attempt, receipt)
            .unwrap();

        drain(run_agent(
            Arc::new(CountingStopProvider {
                calls: provider_calls.clone(),
            }),
            Arc::new(EchoTool),
            cfg,
        ))
        .await;

        assert_eq!(provider_calls.load(Ordering::SeqCst), 0);
        assert!(audit.records().is_empty());
        assert_eq!(
            store.load("preprepared-terminal-receipt").unwrap().status(),
            crate::DurableRunStatus::Completed
        );
    }

    #[tokio::test]
    async fn paused_durable_run_waits_for_resume_without_audit_hooks_or_io() {
        use crate::governance::hooks::{HookDispatcher, PromptHookOutcome};
        use crate::governance::Governance;

        let store = Arc::new(crate::durable_store::InMemoryDurableStore::default());
        let mut state = crate::RunState::new(
            "session",
            "paused-awaiting-resume",
            crate::DurabilityMode::Sync,
        )
        .unwrap();
        state
            .pause("operator-pause", "waiting for operator")
            .unwrap();
        let driver = crate::DurableRunDriver::new(state, store.clone()).unwrap();
        let provider_calls = Arc::new(AtomicUsize::new(0));
        let tool_calls = Arc::new(AtomicUsize::new(0));
        let prompt_hooks = Arc::new(AtomicUsize::new(0));
        let stop_hooks = Arc::new(AtomicUsize::new(0));
        let audit = Arc::new(crate::observability::InMemoryAuditSink::default());
        let recorder = crate::session::RunRecorder::default();

        let mut hooks = HookDispatcher::new();
        let prompt_hook_calls = prompt_hooks.clone();
        hooks.on_user_prompt_submit(move |_| {
            prompt_hook_calls.fetch_add(1, Ordering::SeqCst);
            PromptHookOutcome::Continue
        });
        let stop_hook_calls = stop_hooks.clone();
        hooks.on_stop(move |_| {
            stop_hook_calls.fetch_add(1, Ordering::SeqCst);
        });

        let mut cfg = RunConfig::new("durable-model", vec![Message::user("hello")])
            .with_durable_driver(driver);
        cfg.tools = vec![durable_tool_spec()];
        cfg.audit = crate::observability::AuditTrail::new().with_sink(audit.clone());
        cfg.governance = Governance::new(Default::default(), hooks);
        cfg.recorder = recorder.clone();

        let deltas = drain(run_agent(
            Arc::new(CountingStopProvider {
                calls: provider_calls.clone(),
            }),
            Arc::new(CountingTool {
                calls: tool_calls.clone(),
                fail: false,
            }),
            cfg,
        ))
        .await;

        assert!(deltas.iter().any(|delta| matches!(
            delta,
            StreamDelta::Error { message, .. }
                if message.contains("paused") && message.contains("explicit resume")
        )));
        assert_eq!(provider_calls.load(Ordering::SeqCst), 0);
        assert_eq!(tool_calls.load(Ordering::SeqCst), 0);
        assert_eq!(prompt_hooks.load(Ordering::SeqCst), 0);
        assert_eq!(stop_hooks.load(Ordering::SeqCst), 0);
        assert!(audit.records().is_empty());
        let stored = store.load("paused-awaiting-resume").unwrap();
        assert_eq!(stored.status(), crate::DurableRunStatus::Paused);
        assert_eq!(
            stored.projection().pause_reason.as_deref(),
            Some("waiting for operator")
        );
        let outcome = recorder.outcome();
        assert_eq!(
            outcome.terminal_status,
            crate::session::RunTerminalStatus::Failed
        );
        assert_eq!(
            outcome.stop_reason.as_deref(),
            Some("durable_awaiting_resume")
        );
    }

    #[tokio::test]
    async fn shared_driver_rejects_a_second_runtime_before_run_started() {
        let store = Arc::new(crate::durable_store::InMemoryDurableStore::default());
        let state = crate::RunState::new(
            "session",
            "shared-driver-single-invocation",
            crate::DurabilityMode::Sync,
        )
        .unwrap();
        let driver = crate::DurableRunDriver::new(state, store).unwrap();
        let provider_calls = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(tokio::sync::Notify::new());
        let audit = Arc::new(crate::observability::InMemoryAuditSink::default());

        let mut first_cfg = RunConfig::new("durable-model", vec![Message::user("first")])
            .with_durable_driver(driver.clone());
        first_cfg.audit = crate::observability::AuditTrail::new().with_sink(audit.clone());
        let cancel_first = first_cfg.cancellation.handle();
        let first_provider = Arc::new(BlockingProvider {
            calls: provider_calls.clone(),
            started: started.clone(),
        });
        let first = tokio::spawn(async move {
            drain(run_agent(first_provider, Arc::new(EchoTool), first_cfg)).await
        });

        tokio::time::timeout(std::time::Duration::from_secs(2), started.notified())
            .await
            .expect("first provider should start");

        let mut second_cfg = RunConfig::new("durable-model", vec![Message::user("second")])
            .with_durable_driver(driver);
        let second_recorder = crate::session::RunRecorder::default();
        second_cfg.audit = crate::observability::AuditTrail::new().with_sink(audit.clone());
        second_cfg.recorder = second_recorder.clone();
        let second_deltas = drain(run_agent(
            Arc::new(CountingStopProvider {
                calls: provider_calls.clone(),
            }),
            Arc::new(EchoTool),
            second_cfg,
        ))
        .await;

        assert!(second_deltas.iter().any(|delta| matches!(
            delta,
            StreamDelta::Error { message, .. }
                if message.contains("another in-process invocation")
        )));
        assert_eq!(provider_calls.load(Ordering::SeqCst), 1);
        let second_outcome = second_recorder.outcome();
        assert_eq!(
            second_outcome.terminal_status,
            crate::session::RunTerminalStatus::Failed
        );
        assert_eq!(
            second_outcome.stop_reason.as_deref(),
            Some("durable_invocation_already_active")
        );
        assert_eq!(
            audit
                .records()
                .iter()
                .filter(|record| matches!(
                    record.event,
                    crate::observability::AuditEvent::RunStarted { .. }
                ))
                .count(),
            1,
            "the rejected sibling must not open an audit lifecycle"
        );

        cancel_first.cancel();
        tokio::time::timeout(std::time::Duration::from_secs(2), first)
            .await
            .expect("first invocation should stop after cancellation")
            .unwrap();
        let lifecycle = audit
            .records()
            .into_iter()
            .filter(|record| {
                matches!(
                    record.event,
                    crate::observability::AuditEvent::RunStarted { .. }
                        | crate::observability::AuditEvent::RunStopped { .. }
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(lifecycle.len(), 2);
        assert_eq!(
            lifecycle[0].effective_invocation_id(),
            lifecycle[1].effective_invocation_id()
        );
    }

    #[tokio::test]
    async fn terminal_receipt_committed_after_run_started_blocks_provider_and_closes_lifecycle() {
        let store = Arc::new(crate::durable_store::InMemoryDurableStore::default());
        let state = crate::RunState::new(
            "session",
            "receipt-after-disposition",
            crate::DurabilityMode::Sync,
        )
        .unwrap();
        let driver = crate::DurableRunDriver::new(state, store.clone()).unwrap();
        let receipt = crate::durable_runtime::DurableRunStoppedReceipt {
            turns: 4,
            reason: "end_turn".into(),
            usage: Usage::default(),
        };
        let records = Arc::new(crate::observability::InMemoryAuditSink::default());
        let race_sink = Arc::new(CommitTerminalReceiptOnAuditEvent {
            records: records.clone(),
            driver: driver.clone(),
            receipt,
            audit_binding: test_replay_binding(
                1,
                crate::observability::AuditFailureMode::BestEffort,
            ),
            trigger: TerminalReceiptTrigger::RunStarted,
            committed: AtomicBool::new(false),
        });
        let provider_calls = Arc::new(AtomicUsize::new(0));
        let tool_calls = Arc::new(AtomicUsize::new(0));
        let recorder = crate::session::RunRecorder::default();
        let mut cfg = RunConfig::new("durable-model", vec![Message::user("hello")])
            .with_durable_driver(driver);
        cfg.tools = vec![durable_tool_spec()];
        cfg.audit = crate::observability::AuditTrail::new().with_sink(race_sink.clone());
        cfg.recorder = recorder.clone();

        drain(run_agent(
            Arc::new(CountingStopProvider {
                calls: provider_calls.clone(),
            }),
            Arc::new(CountingTool {
                calls: tool_calls.clone(),
                fail: false,
            }),
            cfg,
        ))
        .await;

        assert!(race_sink.committed.load(Ordering::SeqCst));
        assert_eq!(provider_calls.load(Ordering::SeqCst), 0);
        assert_eq!(tool_calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            store.load("receipt-after-disposition").unwrap().status(),
            crate::DurableRunStatus::Completed
        );
        let lifecycle = records
            .records()
            .into_iter()
            .filter(|record| {
                matches!(
                    record.event,
                    crate::observability::AuditEvent::RunStarted { .. }
                        | crate::observability::AuditEvent::RunStopped { .. }
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(lifecycle.len(), 2);
        assert!(matches!(
            lifecycle[0].event,
            crate::observability::AuditEvent::RunStarted { .. }
        ));
        assert!(matches!(
            &lifecycle[1].event,
            crate::observability::AuditEvent::RunStopped { reason, .. }
                if reason == "end_turn"
        ));
        assert_eq!(
            lifecycle[0].effective_invocation_id(),
            lifecycle[1].effective_invocation_id()
        );
        let outcome = recorder.outcome();
        assert_eq!(
            outcome.terminal_status,
            crate::session::RunTerminalStatus::Completed
        );
        assert_eq!(outcome.stop_reason.as_deref(), Some("end_turn"));
    }

    #[tokio::test]
    async fn matching_canonical_receipt_skips_redundant_recovery_run_stopped() {
        let store = Arc::new(crate::durable_store::InMemoryDurableStore::default());
        let state = crate::RunState::new(
            "session",
            "recovery-run-stopped-rejected",
            crate::DurabilityMode::Sync,
        )
        .unwrap();
        let driver = crate::DurableRunDriver::new(state, store.clone()).unwrap();
        let receipt = crate::durable_runtime::DurableRunStoppedReceipt {
            turns: 1,
            reason: "end_turn".into(),
            usage: Usage::default(),
        };
        let accepted = Arc::new(crate::observability::InMemoryAuditSink::default());
        let race_sink = Arc::new(CommitTerminalReceiptOnAuditEvent {
            records: accepted.clone(),
            driver: driver.clone(),
            receipt,
            audit_binding: test_replay_binding(
                2,
                crate::observability::AuditFailureMode::FailClosed,
            ),
            trigger: TerminalReceiptTrigger::RunStarted,
            committed: AtomicBool::new(false),
        });
        let provider_calls = Arc::new(AtomicUsize::new(0));
        let tool_calls = Arc::new(AtomicUsize::new(0));
        let recorder = crate::session::RunRecorder::default();
        let mut cfg = RunConfig::new("durable-model", vec![Message::user("hello")])
            .with_durable_driver(driver);
        cfg.audit = crate::observability::AuditTrail::new()
            .with_sink(race_sink)
            .with_sink(Arc::new(FailOnAuditEvent("run_stopped")))
            .with_failure_mode(crate::observability::AuditFailureMode::FailClosed);
        cfg.recorder = recorder.clone();

        let deltas = drain(run_agent(
            Arc::new(CountingStopProvider {
                calls: provider_calls.clone(),
            }),
            Arc::new(CountingTool {
                calls: tool_calls.clone(),
                fail: false,
            }),
            cfg,
        ))
        .await;

        assert!(!deltas.iter().any(|delta| matches!(
            delta,
            StreamDelta::Error { info, .. }
                if info.code == crate::error::ErrorCode::Audit
        )));
        assert_eq!(provider_calls.load(Ordering::SeqCst), 0);
        assert_eq!(tool_calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            store
                .load("recovery-run-stopped-rejected")
                .unwrap()
                .status(),
            crate::DurableRunStatus::Completed
        );
        let outcome = recorder.outcome();
        assert_eq!(
            outcome.terminal_status,
            crate::session::RunTerminalStatus::Completed
        );
        assert_eq!(outcome.stop_reason.as_deref(), Some("end_turn"));
        let lifecycle = accepted
            .records()
            .into_iter()
            .filter(|record| {
                matches!(
                    record.event,
                    crate::observability::AuditEvent::RunStarted { .. }
                        | crate::observability::AuditEvent::RunStopped { .. }
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(lifecycle.len(), 2);
        assert_eq!(
            lifecycle[0].effective_invocation_id(),
            lifecycle[1].effective_invocation_id()
        );

        let restarted = crate::DurableRunDriver::new(
            store.load("recovery-run-stopped-rejected").unwrap(),
            store.clone(),
        )
        .unwrap();
        let retry_audit = Arc::new(crate::observability::InMemoryAuditSink::default());
        let mut retry_cfg = RunConfig::new("durable-model", vec![Message::user("hello")])
            .with_durable_driver(restarted);
        retry_cfg.audit = crate::observability::AuditTrail::new().with_sink(retry_audit.clone());
        let retry_deltas = drain(run_agent(
            Arc::new(CountingStopProvider {
                calls: provider_calls.clone(),
            }),
            Arc::new(EchoTool),
            retry_cfg,
        ))
        .await;

        assert!(retry_deltas.is_empty());
        assert_eq!(provider_calls.load(Ordering::SeqCst), 0);
        assert!(retry_audit.records().is_empty());
        assert_eq!(
            store
                .load("recovery-run-stopped-rejected")
                .unwrap()
                .status(),
            crate::DurableRunStatus::Completed
        );
    }

    #[tokio::test]
    async fn matching_canonical_lifecycle_close_cas_failure_emits_no_extra_stop() {
        // CAS 1 opens the invocation fence. The RunStarted race writes the canonical terminal
        // intent/receipt at CAS 2/3. Fail CAS 4 while closing the matching lifecycle; the accepted
        // canonical RunStopped remains the only stop event and restart must reconcile the fence.
        let store = Arc::new(FailNthCasStore::new(4));
        let state = crate::RunState::new(
            "session",
            "recovery-marker-and-audit-double-fault",
            crate::DurabilityMode::Sync,
        )
        .unwrap();
        let driver = crate::DurableRunDriver::new(state, store.clone()).unwrap();
        let receipt = crate::durable_runtime::DurableRunStoppedReceipt {
            turns: 1,
            reason: "end_turn".into(),
            usage: Usage::default(),
        };
        let accepted = Arc::new(crate::observability::InMemoryAuditSink::default());
        let race_sink = Arc::new(CommitTerminalReceiptOnAuditEvent {
            records: accepted.clone(),
            driver: driver.clone(),
            receipt,
            audit_binding: test_replay_binding(
                2,
                crate::observability::AuditFailureMode::FailClosed,
            ),
            trigger: TerminalReceiptTrigger::RunStarted,
            committed: AtomicBool::new(false),
        });
        let provider_calls = Arc::new(AtomicUsize::new(0));
        let tool_calls = Arc::new(AtomicUsize::new(0));
        let recorder = crate::session::RunRecorder::default();
        let mut cfg = RunConfig::new("durable-model", vec![Message::user("hello")])
            .with_durable_driver(driver);
        cfg.audit = crate::observability::AuditTrail::new()
            .with_sink(race_sink)
            .with_sink(Arc::new(FailOnAuditEvent("run_stopped")))
            .with_failure_mode(crate::observability::AuditFailureMode::FailClosed);
        cfg.recorder = recorder.clone();

        let deltas = drain(run_agent(
            Arc::new(CountingStopProvider {
                calls: provider_calls.clone(),
            }),
            Arc::new(CountingTool {
                calls: tool_calls.clone(),
                fail: false,
            }),
            cfg,
        ))
        .await;

        assert!(deltas.iter().any(|delta| matches!(
            delta,
            StreamDelta::Error { message, .. } if message.contains("planned CAS failure")
        )));
        assert!(!deltas.iter().any(|delta| matches!(
            delta,
            StreamDelta::Error { info, .. }
                if info.code == crate::error::ErrorCode::Audit
        )));
        assert_eq!(provider_calls.load(Ordering::SeqCst), 0);
        assert_eq!(tool_calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            store
                .load("recovery-marker-and-audit-double-fault")
                .unwrap()
                .status(),
            crate::DurableRunStatus::Running,
            "the failed marker CAS must not terminalize from the older receipt"
        );
        assert_eq!(
            recorder.outcome().stop_reason.as_deref(),
            Some("durable_commit_failure")
        );
        let lifecycle = accepted
            .records()
            .into_iter()
            .filter(|record| {
                matches!(
                    record.event,
                    crate::observability::AuditEvent::RunStarted { .. }
                        | crate::observability::AuditEvent::RunStopped { .. }
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(lifecycle.len(), 2);
        assert!(matches!(
            lifecycle[0].event,
            crate::observability::AuditEvent::RunStarted { .. }
        ));
        assert!(matches!(
            &lifecycle[1].event,
            crate::observability::AuditEvent::RunStopped { reason, .. }
                if reason == "end_turn"
        ));
        assert_eq!(
            lifecycle[0].effective_invocation_id(),
            lifecycle[1].effective_invocation_id()
        );

        let restarted = crate::DurableRunDriver::new(
            store
                .load("recovery-marker-and-audit-double-fault")
                .unwrap(),
            store.clone(),
        )
        .unwrap();
        let retry_audit = Arc::new(crate::observability::InMemoryAuditSink::default());
        let mut retry_cfg = RunConfig::new("durable-model", vec![Message::user("hello")])
            .with_durable_driver(restarted);
        retry_cfg.audit = crate::observability::AuditTrail::new().with_sink(retry_audit.clone());
        let retry_deltas = drain(run_agent(
            Arc::new(CountingStopProvider {
                calls: provider_calls.clone(),
            }),
            Arc::new(EchoTool),
            retry_cfg,
        ))
        .await;

        assert!(retry_deltas.iter().any(|delta| matches!(
            delta,
            StreamDelta::Error { message, .. } if message.contains("explicit reconciliation")
        )));
        assert_eq!(provider_calls.load(Ordering::SeqCst), 0);
        assert!(retry_audit.records().is_empty());
        assert_eq!(
            store
                .load("recovery-marker-and-audit-double-fault")
                .unwrap()
                .status(),
            crate::DurableRunStatus::ReconcileRequired
        );
    }

    #[tokio::test]
    async fn terminal_receipt_at_tool_boundary_never_emits_a_false_tool_start() {
        let store = Arc::new(crate::durable_store::InMemoryDurableStore::default());
        let state = crate::RunState::new(
            "session",
            "receipt-at-tool-boundary",
            crate::DurabilityMode::Sync,
        )
        .unwrap();
        let driver = crate::DurableRunDriver::new(state, store.clone()).unwrap();
        let receipt = crate::durable_runtime::DurableRunStoppedReceipt {
            turns: 1,
            reason: "end_turn".into(),
            usage: Usage::default(),
        };
        let records = Arc::new(crate::observability::InMemoryAuditSink::default());
        let race_sink = Arc::new(CommitTerminalReceiptOnAuditEvent {
            records: records.clone(),
            driver: driver.clone(),
            receipt,
            audit_binding: test_replay_binding(
                1,
                crate::observability::AuditFailureMode::BestEffort,
            ),
            trigger: TerminalReceiptTrigger::PermissionDecision,
            committed: AtomicBool::new(false),
        });
        let provider_calls = Arc::new(AtomicUsize::new(0));
        let tool_calls = Arc::new(AtomicUsize::new(0));
        let mut cfg = RunConfig::new("durable-model", vec![Message::user("hello")])
            .with_durable_driver(driver);
        cfg.tools = vec![durable_tool_spec()];
        cfg.audit = crate::observability::AuditTrail::new().with_sink(race_sink.clone());

        drain(run_agent(
            Arc::new(CountingToolProvider {
                calls: provider_calls.clone(),
            }),
            Arc::new(CountingTool {
                calls: tool_calls.clone(),
                fail: false,
            }),
            cfg,
        ))
        .await;

        assert!(race_sink.committed.load(Ordering::SeqCst));
        assert_eq!(provider_calls.load(Ordering::SeqCst), 1);
        assert_eq!(tool_calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            store.load("receipt-at-tool-boundary").unwrap().status(),
            crate::DurableRunStatus::Completed
        );
        let records = records.records();
        assert!(records.iter().any(|record| matches!(
            record.event,
            crate::observability::AuditEvent::PermissionDecision { .. }
        )));
        assert!(!records.iter().any(|record| matches!(
            record.event,
            crate::observability::AuditEvent::ToolStarted { .. }
                | crate::observability::AuditEvent::ToolCompleted { .. }
        )));
        let lifecycle = records
            .iter()
            .filter(|record| {
                matches!(
                    record.event,
                    crate::observability::AuditEvent::RunStarted { .. }
                        | crate::observability::AuditEvent::RunStopped { .. }
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(lifecycle.len(), 2);
        assert_eq!(
            lifecycle[0].effective_invocation_id(),
            lifecycle[1].effective_invocation_id()
        );
    }

    #[tokio::test]
    async fn tool_completion_cas_failure_does_not_rerun_provider_or_tool_after_restart() {
        // Lifecycle fence, provider start/completion, and tool start are CAS 1..=4.
        let store = Arc::new(FailNthCasStore::new(5));
        let state = crate::RunState::new(
            "session",
            "tool-completion-cas",
            crate::DurabilityMode::Sync,
        )
        .unwrap();
        let driver = crate::DurableRunDriver::new(state, store.clone()).unwrap();
        let provider_calls = Arc::new(AtomicUsize::new(0));
        let tool_calls = Arc::new(AtomicUsize::new(0));
        let mut cfg = RunConfig::new("durable-model", vec![Message::user("hello")])
            .with_durable_driver(driver.clone());
        cfg.tools = vec![durable_tool_spec()];

        drain(run_agent(
            Arc::new(CountingToolProvider {
                calls: provider_calls.clone(),
            }),
            Arc::new(CountingTool {
                calls: tool_calls.clone(),
                fail: false,
            }),
            cfg,
        ))
        .await;

        assert!(driver.is_poisoned());
        assert_eq!(provider_calls.load(Ordering::SeqCst), 1);
        assert_eq!(tool_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            store.load("tool-completion-cas").unwrap().status(),
            crate::DurableRunStatus::Running
        );

        let restarted =
            crate::DurableRunDriver::new(store.load("tool-completion-cas").unwrap(), store.clone())
                .unwrap();
        let mut restarted_cfg = RunConfig::new("durable-model", vec![Message::user("hello")])
            .with_durable_driver(restarted);
        restarted_cfg.tools = vec![durable_tool_spec()];
        drain(run_agent(
            Arc::new(CountingToolProvider {
                calls: provider_calls.clone(),
            }),
            Arc::new(CountingTool {
                calls: tool_calls.clone(),
                fail: false,
            }),
            restarted_cfg,
        ))
        .await;

        assert_eq!(provider_calls.load(Ordering::SeqCst), 1);
        assert_eq!(tool_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            store.load("tool-completion-cas").unwrap().status(),
            crate::DurableRunStatus::ReconcileRequired
        );
    }

    #[tokio::test]
    async fn cancellation_reconciles_started_provider_without_persisting_request_secrets() {
        let store = Arc::new(crate::durable_store::InMemoryDurableStore::default());
        let state =
            crate::RunState::new("session", "cancelled-provider", crate::DurabilityMode::Sync)
                .unwrap();
        let driver = crate::DurableRunDriver::new(state, store.clone()).unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(tokio::sync::Notify::new());
        let mut cfg = RunConfig::new(
            "durable-model",
            vec![Message::user("credential sk-request-secret")],
        )
        .with_durable_driver(driver);
        cfg.options
            .insert("api_key".into(), serde_json::json!("sk-option-secret"));
        let cancellation = cfg.cancellation.handle();
        let observer_store = store.clone();
        let observer_started = started.clone();
        let observer = tokio::spawn(async move {
            tokio::time::timeout(
                std::time::Duration::from_secs(2),
                observer_started.notified(),
            )
            .await
            .expect("provider should start");
            let running = observer_store.load("cancelled-provider").unwrap();
            let serialized = serde_json::to_string(&running).unwrap();
            assert!(!serialized.contains("sk-request-secret"));
            assert!(!serialized.contains("sk-option-secret"));
            assert!(serialized.contains("input_hash"));
            cancellation.cancel();
        });

        drain(run_agent(
            Arc::new(BlockingProvider {
                calls: calls.clone(),
                started,
            }),
            Arc::new(EchoTool),
            cfg,
        ))
        .await;
        observer.await.unwrap();

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            store.load("cancelled-provider").unwrap().status(),
            crate::DurableRunStatus::ReconcileRequired
        );
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
                (crate::observability::AuditEvent::RunStarted { .. }, "run_started") => true,
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
        let durable_store = Arc::new(crate::durable_store::InMemoryDurableStore::default());

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
        let policy = crate::governance::PolicySnapshot::seal(crate::governance::PolicyDocument {
            schema_version: crate::governance::GOVERNANCE_CONTRACT_VERSION,
            default_effect: crate::governance::PolicyEffect::Allow,
            rules: Vec::new(),
        })
        .unwrap();
        let governance = Governance::new(
            PermissionEngine::with_rules(
                PermissionMode::Allow,
                vec![Rule::ask("Bash").named("ask-bash")],
            ),
            hooks,
        )
        .with_policy_snapshot(policy);
        let durable_state = governance
            .start_durable_run(
                "session",
                "approval-interrupted",
                crate::durability::DurabilityMode::Sync,
            )
            .unwrap();
        let durable_driver =
            crate::durable_runtime::DurableRunDriver::new(durable_state, durable_store.clone())
                .unwrap();
        let governance = governance
            .with_persisted_durable_driver_approver(
                Arc::new(RuntimeApprover {
                    decision: ApprovalDecision::Deny {
                        message: "operator stopped the run".into(),
                        interrupt: true,
                    },
                    calls: approval_calls.clone(),
                }),
                &durable_driver,
                std::time::Duration::from_secs(1),
            )
            .unwrap();
        let mut cfg =
            RunConfig::new("m", vec![Message::user("hi")]).with_durable_driver(durable_driver);
        cfg.tools = vec![advertised("Bash")];
        cfg.audit = AuditTrail::new().with_sink(audit_sink.clone());
        cfg.recorder = recorder.clone();
        cfg.governance = governance;

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
        assert_eq!(
            durable_store.load("approval-interrupted").unwrap().status(),
            crate::durability::DurableRunStatus::Cancelled
        );

        let records = audit_sink.records();
        assert!(records.iter().any(|record| matches!(
            &record.event,
            AuditEvent::PermissionDecision { decision, source, .. }
                if decision == "ask_denied_interrupt"
                    && source.contains("human_approval:ask-bash")
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
    async fn durable_worker_governed_ask_keeps_the_fence_and_executes_once() {
        use crate::durable_worker::{DurableWorker, DurableWorkerConfig, DurableWorkerOutcome};
        use crate::governance::hooks::HookDispatcher;
        use crate::governance::permissions::{PermissionEngine, PermissionMode, Rule};
        use crate::governance::{ApprovalDecision, Governance};

        let run_id = "worker-governed-ask";
        let executor_calls = Arc::new(AtomicUsize::new(0));
        let provider_requests = Arc::new(AtomicUsize::new(0));
        let approval_calls = Arc::new(AtomicUsize::new(0));
        let provider: Arc<dyn Provider> = Arc::new(CountingSingleToolProvider {
            tool: "Bash".into(),
            input: serde_json::json!({ "command": "git status" }),
            requests: provider_requests.clone(),
        });
        let executor: Arc<dyn ToolExecutor> = Arc::new(RecordingExecutor {
            calls: executor_calls.clone(),
            last_input: Arc::new(std::sync::Mutex::new(serde_json::Value::Null)),
        });
        let policy = crate::governance::PolicySnapshot::seal(crate::governance::PolicyDocument {
            schema_version: crate::governance::GOVERNANCE_CONTRACT_VERSION,
            default_effect: crate::governance::PolicyEffect::Allow,
            rules: Vec::new(),
        })
        .unwrap();
        let governance = Governance::new(
            PermissionEngine::with_rules(
                PermissionMode::Allow,
                vec![Rule::ask("Bash").named("ask-bash")],
            ),
            HookDispatcher::new(),
        )
        .with_policy_snapshot(policy);
        let state = governance
            .start_durable_run("session", run_id, crate::durability::DurabilityMode::Sync)
            .unwrap();
        let store = Arc::new(crate::durable_store::InMemoryDurableStore::default());
        store.create(&state).unwrap();
        let worker = DurableWorker::new(
            store.clone(),
            DurableWorkerConfig::new("governed-worker").unwrap(),
        )
        .unwrap();
        let callback_calls = approval_calls.clone();

        let outcome = worker
            .run(
                run_id,
                crate::cancellation::CancellationToken::new(),
                move |driver, cancellation| async move {
                    let governance = governance
                        .with_persisted_durable_driver_approver(
                            Arc::new(RuntimeApprover {
                                decision: ApprovalDecision::allow(None),
                                calls: callback_calls,
                            }),
                            &driver,
                            std::time::Duration::from_secs(1),
                        )
                        .unwrap();
                    let mut cfg = RunConfig::new("m", vec![Message::user("hi")])
                        .with_durable_driver(driver.clone());
                    cfg.tools = vec![advertised("Bash")];
                    cfg.governance = governance;
                    cfg.cancellation = cancellation;

                    let stream = run_agent(provider, executor, cfg);
                    futures::pin_mut!(stream);
                    let mut errors = Vec::new();
                    while let Some(delta) = stream.next().await {
                        if let StreamDelta::Error { message, .. } = delta {
                            errors.push(message);
                        }
                    }
                    assert!(errors.is_empty(), "unexpected runtime errors: {errors:?}");
                    assert!(!driver.is_poisoned());
                    driver.snapshot().unwrap().status()
                },
            )
            .await
            .unwrap();

        assert!(matches!(
            outcome,
            DurableWorkerOutcome::Executed {
                value: crate::durability::DurableRunStatus::Completed,
                ..
            }
        ));
        assert_eq!(approval_calls.load(Ordering::SeqCst), 1);
        assert_eq!(executor_calls.load(Ordering::SeqCst), 1);
        assert_eq!(provider_requests.load(Ordering::SeqCst), 2);

        let persisted = store.load(run_id).unwrap();
        assert_eq!(
            persisted.status(),
            crate::durability::DurableRunStatus::Completed
        );
        assert!(persisted.worker_lease().is_none());
        assert_eq!(
            persisted
                .events()
                .iter()
                .filter(|event| matches!(
                    event.kind,
                    crate::durability::RunEventKind::ApprovalRequested { .. }
                ))
                .count(),
            1
        );
        assert_eq!(
            persisted
                .events()
                .iter()
                .filter(|event| matches!(
                    event.kind,
                    crate::durability::RunEventKind::ApprovalResolved { .. }
                ))
                .count(),
            1
        );
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
        let store = Arc::new(crate::durable_store::InMemoryDurableStore::default());
        let state = crate::durability::RunState::new(
            "session",
            "rejected-run-stopped",
            crate::durability::DurabilityMode::Sync,
        )
        .unwrap();
        let recorder = crate::session::RunRecorder::default();
        let mut cfg = RunConfig::new("mock-1", vec![Message::user("hi")])
            .with_durable_run(state, store.clone())
            .unwrap();
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
        assert_eq!(
            store.load("rejected-run-stopped").unwrap().status(),
            crate::durability::DurableRunStatus::ReconcileRequired,
            "a rejected or partially delivered audit record must never leave durable success"
        );
    }

    #[tokio::test]
    async fn partial_run_stopped_fanout_is_not_retried_after_restart() {
        let store = Arc::new(crate::durable_store::InMemoryDurableStore::default());
        let state = crate::durability::RunState::new(
            "session",
            "partial-run-stopped",
            crate::durability::DurabilityMode::Sync,
        )
        .unwrap();
        let provider_calls = Arc::new(AtomicUsize::new(0));
        let tool_calls = Arc::new(AtomicUsize::new(0));
        let accepted = Arc::new(crate::observability::InMemoryAuditSink::default());
        let audit = crate::observability::AuditTrail::new()
            .with_sink(accepted.clone())
            .with_sink(Arc::new(FailOnAuditEvent("run_stopped")))
            .with_failure_mode(crate::observability::AuditFailureMode::FailClosed);
        let driver = crate::DurableRunDriver::new(state, store.clone()).unwrap();
        let mut cfg = RunConfig::new("durable-model", vec![Message::user("hello")])
            .with_durable_driver(driver);
        cfg.tools = vec![durable_tool_spec()];
        cfg.audit = audit.clone();

        drain(run_agent(
            Arc::new(CountingSingleToolProvider {
                tool: "write".into(),
                input: serde_json::json!({"value": 1}),
                requests: provider_calls.clone(),
            }),
            Arc::new(RecordingExecutor {
                calls: tool_calls.clone(),
                last_input: Arc::new(std::sync::Mutex::new(serde_json::Value::Null)),
            }),
            cfg,
        ))
        .await;

        assert_eq!(provider_calls.load(Ordering::SeqCst), 2);
        assert_eq!(tool_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            store.load("partial-run-stopped").unwrap().status(),
            crate::DurableRunStatus::ReconcileRequired
        );
        let records_after_partial_delivery = accepted.records();
        assert_eq!(
            records_after_partial_delivery
                .iter()
                .filter(|record| {
                    matches!(
                        record.event,
                        crate::observability::AuditEvent::RunStarted { .. }
                    )
                })
                .count(),
            1
        );
        assert_eq!(
            records_after_partial_delivery
                .iter()
                .filter(|record| {
                    matches!(
                        record.event,
                        crate::observability::AuditEvent::RunStopped { .. }
                    )
                })
                .count(),
            1,
            "the first sink accepted RunStopped before the second sink rejected it"
        );

        let restarted =
            crate::DurableRunDriver::new(store.load("partial-run-stopped").unwrap(), store.clone())
                .unwrap();
        let retry_recorder = crate::session::RunRecorder::default();
        let mut retry_cfg = RunConfig::new("durable-model", vec![Message::user("hello")])
            .with_durable_driver(restarted);
        retry_cfg.tools = vec![durable_tool_spec()];
        retry_cfg.audit = audit;
        retry_cfg.recorder = retry_recorder.clone();
        let retry_deltas = drain(run_agent(
            Arc::new(CountingSingleToolProvider {
                tool: "write".into(),
                input: serde_json::json!({"value": 1}),
                requests: provider_calls.clone(),
            }),
            Arc::new(RecordingExecutor {
                calls: tool_calls.clone(),
                last_input: Arc::new(std::sync::Mutex::new(serde_json::Value::Null)),
            }),
            retry_cfg,
        ))
        .await;

        assert!(retry_deltas.iter().any(|delta| matches!(
            delta,
            StreamDelta::Error { message, .. } if message.contains("explicit reconciliation")
        )));
        assert_eq!(provider_calls.load(Ordering::SeqCst), 2);
        assert_eq!(tool_calls.load(Ordering::SeqCst), 1);
        assert_eq!(accepted.records(), records_after_partial_delivery);
        assert_eq!(
            retry_recorder.outcome().stop_reason.as_deref(),
            Some("durable_reconciliation_required")
        );
    }

    #[tokio::test]
    async fn reconciled_run_stopped_replays_exact_terminal_event_only_after_resume() {
        let run_id = "reconciled-terminal-audit-replay";
        let delivery_id = "terminal-ledger-v1";
        let store = Arc::new(crate::durable_store::InMemoryDurableStore::default());
        let state = crate::RunState::new("session", run_id, crate::DurabilityMode::Sync).unwrap();
        let provider_calls = Arc::new(AtomicUsize::new(0));
        let tool_calls = Arc::new(AtomicUsize::new(0));
        let unreached = Arc::new(crate::observability::InMemoryAuditSink::default());
        let initial_audit = crate::observability::AuditTrail::new()
            .with_sink(Arc::new(FailOnAuditEvent("run_stopped")))
            .with_sink(unreached.clone())
            .with_failure_mode(crate::observability::AuditFailureMode::FailClosed)
            .with_durable_replay_delivery_id(delivery_id)
            .unwrap();
        let driver = crate::DurableRunDriver::new(state, store.clone()).unwrap();
        let mut cfg = RunConfig::new("durable-model", vec![Message::user("hello")])
            .with_durable_driver(driver);
        cfg.audit = initial_audit;

        let first_deltas = drain(run_agent(
            Arc::new(CountingStopProvider {
                calls: provider_calls.clone(),
            }),
            Arc::new(CountingTool {
                calls: tool_calls.clone(),
                fail: false,
            }),
            cfg,
        ))
        .await;

        assert!(first_deltas.iter().any(|delta| matches!(
            delta,
            StreamDelta::Error { info, .. }
                if info.code == crate::error::ErrorCode::Audit
        )));
        assert_eq!(provider_calls.load(Ordering::SeqCst), 1);
        assert_eq!(tool_calls.load(Ordering::SeqCst), 0);
        let mut reconciled = store.load(run_id).unwrap();
        assert_eq!(
            reconciled.status(),
            crate::DurableRunStatus::ReconcileRequired
        );
        let expected_sequence = reconciled.events().last().map_or(0, |event| event.sequence);
        let canonical = reconciled
            .projection()
            .activities
            .values()
            .find(|record| {
                record.definition.stable_step_id
                    == crate::durability::RUNTIME_RUN_STOPPED_AUDIT_STEP_ID
            })
            .cloned()
            .expect("canonical terminal audit marker must exist");
        let replay_value = canonical
            .definition
            .input
            .get("replay")
            .cloned()
            .expect("typed replay envelope must be persisted");
        let replay: crate::durable_runtime::DurableRunStoppedAuditReplayEnvelope =
            serde_json::from_value(replay_value.clone()).unwrap();
        let lifecycle = reconciled
            .projection()
            .activities
            .values()
            .find(|record| {
                record.definition.stable_step_id
                    == crate::durability::RUNTIME_INVOCATION_LIFECYCLE_STEP_ID
                    && record.definition.logical_key == replay.invocation_id
            })
            .cloned()
            .expect("matching invocation lifecycle must exist");
        reconciled
            .reconcile_activity(
                "terminal-audit-safe-to-retry",
                &canonical.definition.activity_id,
                crate::ActivityReconciliation::SafeToRetry,
            )
            .unwrap();
        reconciled
            .reconcile_activity(
                "terminal-lifecycle-replay-authorized",
                &lifecycle.definition.activity_id,
                crate::ActivityReconciliation::Completed {
                    output: serde_json::json!({
                        "status": "terminal_replay_authorized",
                        "replay": replay_value,
                    }),
                },
            )
            .unwrap();
        assert_eq!(reconciled.status(), crate::DurableRunStatus::Paused);
        store
            .compare_and_swap(expected_sequence, &reconciled)
            .unwrap();

        let paused_sink = Arc::new(crate::observability::InMemoryAuditSink::default());
        let paused_cfg_audit = crate::observability::AuditTrail::new()
            .with_sink(paused_sink.clone())
            .with_sink(Arc::new(crate::observability::InMemoryAuditSink::default()))
            .with_failure_mode(crate::observability::AuditFailureMode::FailClosed)
            .with_durable_replay_delivery_id(delivery_id)
            .unwrap();
        let paused_driver =
            crate::DurableRunDriver::new(store.load(run_id).unwrap(), store.clone()).unwrap();
        let mut paused_cfg = RunConfig::new("durable-model", vec![Message::user("hello")])
            .with_durable_driver(paused_driver);
        paused_cfg.audit = paused_cfg_audit;
        let paused_deltas = drain(run_agent(
            Arc::new(CountingStopProvider {
                calls: provider_calls.clone(),
            }),
            Arc::new(CountingTool {
                calls: tool_calls.clone(),
                fail: false,
            }),
            paused_cfg,
        ))
        .await;
        assert!(paused_deltas.iter().any(|delta| matches!(
            delta,
            StreamDelta::Error { message, .. } if message.contains("explicit resume")
        )));
        assert!(paused_sink.records().is_empty());
        assert_eq!(provider_calls.load(Ordering::SeqCst), 1);
        assert_eq!(tool_calls.load(Ordering::SeqCst), 0);

        let mut resumed = store.load(run_id).unwrap();
        let expected_sequence = resumed.events().last().map_or(0, |event| event.sequence);
        resumed
            .apply_command(crate::RunCommand::Resume {
                command_id: "resume-terminal-audit-replay".into(),
                approvals: Vec::new(),
            })
            .unwrap();
        store.compare_and_swap(expected_sequence, &resumed).unwrap();

        let replay_sink = Arc::new(crate::observability::InMemoryAuditSink::default());
        let replay_recorder = crate::session::RunRecorder::default();
        let replay_driver =
            crate::DurableRunDriver::new(store.load(run_id).unwrap(), store.clone()).unwrap();
        let mut replay_cfg = RunConfig::new("durable-model", vec![Message::user("hello")])
            .with_durable_driver(replay_driver);
        replay_cfg.audit = crate::observability::AuditTrail::new()
            .with_sink(replay_sink.clone())
            .with_sink(Arc::new(crate::observability::InMemoryAuditSink::default()))
            .with_failure_mode(crate::observability::AuditFailureMode::FailClosed)
            .with_durable_replay_delivery_id(delivery_id)
            .unwrap();
        replay_cfg.recorder = replay_recorder.clone();
        let replay_deltas = drain(run_agent(
            Arc::new(CountingStopProvider {
                calls: provider_calls.clone(),
            }),
            Arc::new(CountingTool {
                calls: tool_calls.clone(),
                fail: false,
            }),
            replay_cfg,
        ))
        .await;

        assert!(replay_deltas.is_empty());
        assert_eq!(provider_calls.load(Ordering::SeqCst), 1);
        assert_eq!(tool_calls.load(Ordering::SeqCst), 0);
        let records = replay_sink.records();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].run_id, replay.audit_run_id);
        assert_eq!(
            records[0].invocation_id.as_deref(),
            Some(replay.invocation_id.as_str())
        );
        assert_eq!(records[0].sequence, replay.run_stopped_sequence);
        assert!(matches!(
            &records[0].event,
            crate::observability::AuditEvent::RunStopped { turns, reason }
                if *turns == replay.audit_turns && reason == &replay.audit_reason
        ));
        assert_eq!(
            store.load(run_id).unwrap().status(),
            crate::DurableRunStatus::Completed
        );
        assert_eq!(
            replay_recorder.outcome().terminal_status,
            crate::session::RunTerminalStatus::Completed
        );
    }

    #[tokio::test]
    async fn partial_reconciled_terminal_replay_returns_to_reconciliation_without_blind_retry() {
        let run_id = "partial-reconciled-terminal-replay";
        let delivery_id = "partial-terminal-ledger-v1";
        let store = Arc::new(crate::durable_store::InMemoryDurableStore::default());
        let state = crate::RunState::new("session", run_id, crate::DurabilityMode::Sync).unwrap();
        let driver = crate::DurableRunDriver::new(state, store.clone()).unwrap();
        let template = crate::observability::AuditTrail::new()
            .with_sink(Arc::new(crate::observability::InMemoryAuditSink::default()))
            .with_sink(Arc::new(crate::observability::InMemoryAuditSink::default()))
            .with_failure_mode(crate::observability::AuditFailureMode::FailClosed)
            .with_durable_replay_delivery_id(delivery_id)
            .unwrap();
        let invocation_audit = template.for_run_id(run_id).unwrap();
        let invocation_id = invocation_audit.invocation_id().unwrap().to_string();
        let crate::durable_runtime::DurableActivity::Execute {
            activity_id: lifecycle_id,
            attempt: lifecycle_attempt,
            ..
        } = driver.begin_invocation_lifecycle(&invocation_id).unwrap()
        else {
            panic!("fresh invocation lifecycle must execute");
        };
        let receipt = crate::durable_runtime::DurableRunStoppedReceipt {
            turns: 1,
            reason: "end_turn".into(),
            usage: Usage::default(),
        };
        let replay = crate::durable_runtime::DurableRunStoppedAuditReplayEnvelope::new(
            crate::durable_runtime::DurableRunStoppedAuditKind::Canonical,
            receipt,
            run_id,
            invocation_id,
            2,
            1,
            "end_turn",
            template.replay_binding(),
        );
        let crate::durable_runtime::DurableActivity::Execute {
            activity_id: audit_id,
            attempt: audit_attempt,
            ..
        } = driver.begin_invocation_run_stopped_audit(&replay).unwrap()
        else {
            panic!("fresh canonical terminal marker must execute");
        };
        driver
            .fail_run_stopped_audit_and_invocation_lifecycle(
                &audit_id,
                audit_attempt,
                &lifecycle_id,
                lifecycle_attempt,
            )
            .unwrap();

        let mut reconciled = store.load(run_id).unwrap();
        let expected_sequence = reconciled.events().last().map_or(0, |event| event.sequence);
        reconciled
            .reconcile_activity(
                "partial-replay-safe-to-retry",
                &audit_id,
                crate::ActivityReconciliation::SafeToRetry,
            )
            .unwrap();
        reconciled
            .reconcile_activity(
                "partial-replay-lifecycle-authorized",
                &lifecycle_id,
                crate::ActivityReconciliation::Completed {
                    output: serde_json::json!({
                        "status": "terminal_replay_authorized",
                        "replay": replay,
                    }),
                },
            )
            .unwrap();
        store
            .compare_and_swap(expected_sequence, &reconciled)
            .unwrap();
        let mut resumed = store.load(run_id).unwrap();
        let expected_sequence = resumed.events().last().map_or(0, |event| event.sequence);
        resumed
            .apply_command(crate::RunCommand::Resume {
                command_id: "resume-partial-replay".into(),
                approvals: Vec::new(),
            })
            .unwrap();
        store.compare_and_swap(expected_sequence, &resumed).unwrap();

        let accepted = Arc::new(crate::observability::InMemoryAuditSink::default());
        let provider_calls = Arc::new(AtomicUsize::new(0));
        let tool_calls = Arc::new(AtomicUsize::new(0));
        let replay_driver =
            crate::DurableRunDriver::new(store.load(run_id).unwrap(), store.clone()).unwrap();
        let mut replay_cfg = RunConfig::new("durable-model", vec![Message::user("hello")])
            .with_durable_driver(replay_driver);
        replay_cfg.audit = crate::observability::AuditTrail::new()
            .with_sink(accepted.clone())
            .with_sink(Arc::new(FailOnAuditEvent("run_stopped")))
            .with_failure_mode(crate::observability::AuditFailureMode::FailClosed)
            .with_durable_replay_delivery_id(delivery_id)
            .unwrap();
        let replay_deltas = drain(run_agent(
            Arc::new(CountingStopProvider {
                calls: provider_calls.clone(),
            }),
            Arc::new(CountingTool {
                calls: tool_calls.clone(),
                fail: false,
            }),
            replay_cfg,
        ))
        .await;

        assert!(replay_deltas.iter().any(|delta| matches!(
            delta,
            StreamDelta::Error { info, .. }
                if info.code == crate::error::ErrorCode::Audit
        )));
        assert_eq!(accepted.records().len(), 1);
        assert!(matches!(
            accepted.records()[0].event,
            crate::observability::AuditEvent::RunStopped { .. }
        ));
        assert_eq!(provider_calls.load(Ordering::SeqCst), 0);
        assert_eq!(tool_calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            store.load(run_id).unwrap().status(),
            crate::DurableRunStatus::ReconcileRequired
        );

        let retry_sink = Arc::new(crate::observability::InMemoryAuditSink::default());
        let restarted =
            crate::DurableRunDriver::new(store.load(run_id).unwrap(), store.clone()).unwrap();
        let mut retry_cfg = RunConfig::new("durable-model", vec![Message::user("hello")])
            .with_durable_driver(restarted);
        retry_cfg.audit = crate::observability::AuditTrail::new()
            .with_sink(retry_sink.clone())
            .with_sink(Arc::new(crate::observability::InMemoryAuditSink::default()))
            .with_failure_mode(crate::observability::AuditFailureMode::FailClosed)
            .with_durable_replay_delivery_id(delivery_id)
            .unwrap();
        let retry_deltas = drain(run_agent(
            Arc::new(CountingStopProvider {
                calls: provider_calls.clone(),
            }),
            Arc::new(CountingTool {
                calls: tool_calls.clone(),
                fail: false,
            }),
            retry_cfg,
        ))
        .await;
        assert!(retry_deltas.iter().any(|delta| matches!(
            delta,
            StreamDelta::Error { message, .. } if message.contains("explicit reconciliation")
        )));
        assert!(retry_sink.records().is_empty());
        assert_eq!(provider_calls.load(Ordering::SeqCst), 0);
        assert_eq!(tool_calls.load(Ordering::SeqCst), 0);
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
        let durable_store = Arc::new(crate::durable_store::InMemoryDurableStore::default());
        let durable_state = crate::durability::RunState::new(
            "session",
            "pre-cancelled",
            crate::durability::DurabilityMode::Sync,
        )
        .unwrap();

        let mut hooks = HookDispatcher::new();
        let seen = stopped.clone();
        hooks.on_stop(move |ctx| seen.lock().unwrap().push(ctx.reason.clone()));
        let mut cfg = RunConfig::new("primary", vec![Message::user("hi")])
            .with_durable_run(durable_state, durable_store.clone())
            .unwrap();
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
        assert_eq!(
            durable_store.load("pre-cancelled").unwrap().status(),
            crate::durability::DurableRunStatus::Cancelled
        );
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
