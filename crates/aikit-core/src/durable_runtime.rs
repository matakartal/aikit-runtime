//! Synchronous durable coordination for the in-process agent loop.
//!
//! The driver deliberately implements only [`DurabilityMode::Sync`]. Every activity start is
//! compare-and-swapped before the provider or tool is called, and every observed outcome is
//! compare-and-swapped before the loop advances. This is at-least-once coordination, not
//! exactly-once execution: an unsafe activity left running after a crash requires reconciliation.

use crate::durability::{
    is_reserved_audit_activity, stable_input_hash, ActivityDecision, DurabilityError,
    DurabilityMode, DurableRunStatus, RunCommand, RunState, SideEffectClass,
    RUNTIME_INVOCATION_LIFECYCLE_STEP_ID, RUNTIME_LEGACY_RUN_STOPPED_AUDIT_STEP_ID,
    RUNTIME_RECOVERY_RUN_STOPPED_AUDIT_STEP_ID, RUNTIME_RUN_STOPPED_AUDIT_STEP_ID,
};
use crate::durable_store::{DurableStore, DurableStoreError, DurableStoreLeaseAuthority};
use serde_json::Value;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

/// Absolute safety ceiling for a single durable activity result.
///
/// Durable results are retained for deterministic replay. Bounding each result prevents an
/// unexpectedly large provider/tool response from making every later state CAS unbounded.
pub const MAX_DURABLE_COMPLETION_BYTES: usize = 64 * 1024 * 1024;
const RUN_STOPPED_AUDIT_LOGICAL_KEY: &str = "terminal";

/// Storage policy for durable activity results.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DurablePayloadPolicy {
    max_completion_bytes: usize,
}

impl DurablePayloadPolicy {
    pub fn new(max_completion_bytes: usize) -> Result<Self, DurableRunDriverError> {
        if max_completion_bytes == 0 || max_completion_bytes > MAX_DURABLE_COMPLETION_BYTES {
            return Err(DurableRunDriverError::InvalidPayloadPolicy {
                max_completion_bytes,
                hard_limit: MAX_DURABLE_COMPLETION_BYTES,
            });
        }
        Ok(Self {
            max_completion_bytes,
        })
    }

    pub fn max_completion_bytes(self) -> usize {
        self.max_completion_bytes
    }
}

impl Default for DurablePayloadPolicy {
    fn default() -> Self {
        Self {
            max_completion_bytes: MAX_DURABLE_COMPLETION_BYTES,
        }
    }
}

/// A prepared durable activity that is either safe to execute or already has a committed result.
#[derive(Debug, Clone, PartialEq)]
pub enum DurableActivity {
    Execute {
        activity_id: String,
        attempt: u32,
        idempotency_key: Option<String>,
    },
    ReuseCompleted {
        activity_id: String,
        output: Value,
    },
}

/// Durable work, terminal-only recovery, or an operator-owned reconciliation boundary.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum DurableInvocationDisposition {
    Execute,
    FinalizeTerminal(DurableRunStoppedReceipt),
    RetryTerminalAudit(DurableRunStoppedAuditReplayEnvelope),
    AwaitingResume {
        reason: Option<String>,
    },
    ReconcileRequired {
        reason: String,
    },
    AlreadyTerminal {
        status: DurableRunStatus,
        reason: Option<String>,
    },
}

/// Receipt persisted only after every configured sink accepted `RunStopped`.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DurableRunStoppedReceipt {
    pub turns: usize,
    pub reason: String,
    pub usage: crate::types::Usage,
}

const RUN_STOPPED_REPLAY_SCHEMA_VERSION: u32 = 1;
pub(crate) const LEGACY_RUN_STOPPED_RESOLUTION_SCHEMA_VERSION: u32 = 1;
pub(crate) const LEGACY_RUN_STOPPED_RESOLUTION_KIND: &str = "legacy_run_stopped_v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DurableRunStoppedAuditKind {
    Canonical,
    Recovery,
    Direct,
}

/// Exact metadata required to replay only one reconciled `RunStopped` audit event.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DurableRunStoppedAuditReplayEnvelope {
    pub schema_version: u32,
    pub kind: DurableRunStoppedAuditKind,
    pub terminal_receipt: DurableRunStoppedReceipt,
    pub audit_run_id: String,
    pub invocation_id: String,
    pub run_stopped_sequence: u64,
    pub audit_turns: usize,
    pub audit_reason: String,
    pub audit_binding: crate::observability::AuditReplayBinding,
}

/// Operator-supplied terminal metadata for a v1 audit receipt that predates typed replay.
///
/// The source activity and accepted-output hash bind this attestation to one migrated receipt;
/// it is accepted only by `ActivityReconciled::Completed`, never by raw completion or retry.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DurableLegacyRunStoppedResolutionSource {
    pub schema_version: u32,
    pub kind: String,
    pub source_activity_id: String,
    pub source_attempt: u32,
    pub source_started_sequence: u64,
    pub source_output_hash: Option<String>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DurableLegacyRunStoppedResolutionEnvelope {
    pub schema_version: u32,
    pub kind: String,
    pub source_activity_id: String,
    pub source_attempt: u32,
    pub source_started_sequence: u64,
    pub source_output_hash: Option<String>,
    pub terminal_receipt: DurableRunStoppedReceipt,
}

impl DurableRunStoppedAuditReplayEnvelope {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        kind: DurableRunStoppedAuditKind,
        terminal_receipt: DurableRunStoppedReceipt,
        audit_run_id: impl Into<String>,
        invocation_id: impl Into<String>,
        run_stopped_sequence: u64,
        audit_turns: usize,
        audit_reason: impl Into<String>,
        audit_binding: crate::observability::AuditReplayBinding,
    ) -> Self {
        Self {
            schema_version: RUN_STOPPED_REPLAY_SCHEMA_VERSION,
            kind,
            terminal_receipt,
            audit_run_id: audit_run_id.into(),
            invocation_id: invocation_id.into(),
            run_stopped_sequence,
            audit_turns,
            audit_reason: audit_reason.into(),
            audit_binding,
        }
    }
}

pub(crate) struct DurableTerminalAuditRetryAttempt {
    pub kind: DurableRunStoppedAuditKind,
    pub activity_id: String,
    pub attempt: u32,
    pub replay: DurableRunStoppedAuditReplayEnvelope,
}

enum RawInvocationDisposition {
    Execute,
    FinalizeTerminal(Value),
    RetryTerminalAudit(DurableRunStoppedAuditReplayEnvelope),
    AwaitingResume {
        reason: Option<String>,
    },
    ReconcileRequired {
        reason: String,
    },
    AlreadyTerminal {
        status: DurableRunStatus,
        reason: Option<String>,
    },
}

impl DurableActivity {
    pub fn activity_id(&self) -> &str {
        match self {
            Self::Execute { activity_id, .. } | Self::ReuseCompleted { activity_id, .. } => {
                activity_id
            }
        }
    }
}

fn durable_activity_from_decision(
    decision: ActivityDecision,
) -> Result<DurableActivity, DurableRunDriverError> {
    match decision {
        ActivityDecision::Execute {
            activity_id,
            attempt,
            idempotency_key,
        } => Ok(DurableActivity::Execute {
            activity_id,
            attempt,
            idempotency_key,
        }),
        ActivityDecision::ReuseCompleted {
            activity_id,
            output,
        } => Ok(DurableActivity::ReuseCompleted {
            activity_id,
            output,
        }),
        ActivityDecision::ReconcileRequired {
            activity_id,
            reason,
        } => Err(DurableRunDriverError::ReconciliationRequired {
            activity_id,
            reason,
        }),
        ActivityDecision::Failed { activity_id, error } => {
            Err(DurableRunDriverError::ActivityFailed { activity_id, error })
        }
        ActivityDecision::Cancelled { activity_id } => {
            Err(DurableRunDriverError::ActivityCancelled { activity_id })
        }
    }
}

fn quarantine_unstarted_reserved_activity(
    candidate: &mut RunState,
    record: &crate::durability::ActivityRecord,
    reason: &str,
) -> Result<bool, DurableRunDriverError> {
    if record.latest_attempt().is_some() {
        return Ok(false);
    }
    candidate.quarantine_unstarted_reserved_activity(
        &record.definition.activity_id,
        reason.to_string(),
    )?;
    Ok(true)
}

fn terminal_audit_input(
    replay: &DurableRunStoppedAuditReplayEnvelope,
    expected_output: &Value,
) -> Result<Value, DurableRunDriverError> {
    validate_replay_envelope(
        replay,
        replay.kind,
        &replay.audit_run_id,
        Some(&replay.invocation_id),
    )?;
    Ok(serde_json::json!({
        "replay": replay,
        "expected_output_hash": stable_input_hash(expected_output),
    }))
}

fn legacy_resolution_source(
    record: &crate::durability::ActivityRecord,
) -> Result<DurableLegacyRunStoppedResolutionSource, DurableRunDriverError> {
    let source = record
        .definition
        .input
        .get("legacy_resolution")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            DurableRunDriverError::InvalidCompletionPayload(
                "legacy terminal resolution marker is missing its source binding".into(),
            )
        })?;
    if source.get("schema_version").and_then(Value::as_u64)
        != Some(u64::from(LEGACY_RUN_STOPPED_RESOLUTION_SCHEMA_VERSION))
        || source.get("kind").and_then(Value::as_str) != Some(LEGACY_RUN_STOPPED_RESOLUTION_KIND)
    {
        return Err(DurableRunDriverError::InvalidCompletionPayload(
            "legacy terminal resolution marker has an unsupported schema".into(),
        ));
    }
    let source: DurableLegacyRunStoppedResolutionSource =
        serde_json::from_value(Value::Object(source.clone())).map_err(|error| {
            DurableRunDriverError::InvalidCompletionPayload(format!(
                "legacy terminal resolution marker source is malformed: {error}"
            ))
        })?;
    let valid_output_hash = source.source_output_hash.as_deref().is_none_or(|value| {
        value.starts_with("sha256:")
            && value.len() == 71
            && value[7..].bytes().all(|byte| byte.is_ascii_hexdigit())
    });
    if source.source_activity_id.is_empty()
        || source.source_activity_id.chars().any(char::is_control)
        || source.source_attempt == 0
        || source.source_started_sequence == 0
        || !valid_output_hash
    {
        return Err(DurableRunDriverError::InvalidCompletionPayload(
            "legacy terminal resolution marker has an invalid source fingerprint".into(),
        ));
    }
    Ok(source)
}

fn legacy_resolution_marker_input(
    legacy_record: &crate::durability::ActivityRecord,
) -> Result<Value, DurableRunDriverError> {
    let attempt = legacy_record.latest_attempt().ok_or_else(|| {
        DurableRunDriverError::InvalidCompletionPayload(
            "legacy terminal receipt has no completed delivery attempt".into(),
        )
    })?;
    let source_output_hash = match attempt.status {
        crate::durability::ActivityAttemptStatus::Completed => {
            attempt.output_hash.clone().ok_or_else(|| {
                DurableRunDriverError::InvalidCompletionPayload(
                    "legacy terminal receipt has no accepted output hash".into(),
                )
            })?
        }
        crate::durability::ActivityAttemptStatus::Failed
        | crate::durability::ActivityAttemptStatus::Cancelled => String::new(),
        crate::durability::ActivityAttemptStatus::Running
        | crate::durability::ActivityAttemptStatus::ReconcileRequired => {
            return Err(DurableRunDriverError::InvalidCompletionPayload(
                "legacy terminal receipt must be reconciled before migration".into(),
            ));
        }
    };
    let source = DurableLegacyRunStoppedResolutionSource {
        schema_version: LEGACY_RUN_STOPPED_RESOLUTION_SCHEMA_VERSION,
        kind: LEGACY_RUN_STOPPED_RESOLUTION_KIND.into(),
        source_activity_id: legacy_record.definition.activity_id.clone(),
        source_attempt: attempt.attempt,
        source_started_sequence: attempt.started_sequence,
        source_output_hash: (!source_output_hash.is_empty()).then_some(source_output_hash),
    };
    Ok(serde_json::json!({
        "legacy_resolution": source,
    }))
}

fn legacy_resolution_output(
    record: &crate::durability::ActivityRecord,
    terminal_receipt: DurableRunStoppedReceipt,
) -> Result<Value, DurableRunDriverError> {
    let source = if record.definition.stable_step_id == RUNTIME_LEGACY_RUN_STOPPED_AUDIT_STEP_ID {
        let attempt = record.latest_attempt().ok_or_else(|| {
            DurableRunDriverError::InvalidCompletionPayload(
                "legacy terminal source has no attempt to reconcile".into(),
            )
        })?;
        DurableLegacyRunStoppedResolutionSource {
            schema_version: LEGACY_RUN_STOPPED_RESOLUTION_SCHEMA_VERSION,
            kind: LEGACY_RUN_STOPPED_RESOLUTION_KIND.into(),
            source_activity_id: record.definition.activity_id.clone(),
            source_attempt: attempt.attempt,
            source_started_sequence: attempt.started_sequence,
            source_output_hash: (attempt.status
                == crate::durability::ActivityAttemptStatus::Completed)
                .then(|| attempt.output_hash.clone())
                .flatten(),
        }
    } else {
        legacy_resolution_source(record)?
    };
    serde_json::to_value(DurableLegacyRunStoppedResolutionEnvelope {
        schema_version: LEGACY_RUN_STOPPED_RESOLUTION_SCHEMA_VERSION,
        kind: LEGACY_RUN_STOPPED_RESOLUTION_KIND.into(),
        source_activity_id: source.source_activity_id,
        source_attempt: source.source_attempt,
        source_started_sequence: source.source_started_sequence,
        source_output_hash: source.source_output_hash,
        terminal_receipt,
    })
    .map_err(|error| DurableRunDriverError::InvalidCompletionPayload(error.to_string()))
}

fn replay_from_record(
    record: &crate::durability::ActivityRecord,
    expected_kind: DurableRunStoppedAuditKind,
    run_id: &str,
) -> Result<DurableRunStoppedAuditReplayEnvelope, DurableRunDriverError> {
    let replay = record
        .definition
        .input
        .get("replay")
        .cloned()
        .ok_or_else(|| {
            DurableRunDriverError::InvalidCompletionPayload(
                "terminal audit activity is missing its typed replay envelope".into(),
            )
        })?;
    let replay: DurableRunStoppedAuditReplayEnvelope = serde_json::from_value(replay)
        .map_err(|error| DurableRunDriverError::InvalidCompletionPayload(error.to_string()))?;
    let expected_invocation_id = (expected_kind == DurableRunStoppedAuditKind::Recovery)
        .then_some(record.definition.logical_key.as_str());
    validate_replay_envelope(&replay, expected_kind, run_id, expected_invocation_id)?;
    let expected_output_hash = record
        .definition
        .input
        .get("expected_output_hash")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            DurableRunDriverError::InvalidCompletionPayload(
                "terminal audit replay is missing its expected output hash".into(),
            )
        })?;
    let expected_output = match expected_kind {
        DurableRunStoppedAuditKind::Canonical => serde_json::to_value(&replay.terminal_receipt)
            .map_err(|error| DurableRunDriverError::InvalidCompletionPayload(error.to_string()))?,
        DurableRunStoppedAuditKind::Recovery => serde_json::json!({"accepted": true}),
        DurableRunStoppedAuditKind::Direct => {
            return Err(DurableRunDriverError::InvalidCompletionPayload(
                "direct lifecycle replay is not a terminal audit activity".into(),
            ));
        }
    };
    if stable_input_hash(&expected_output) != expected_output_hash {
        return Err(DurableRunDriverError::InvalidCompletionPayload(
            "terminal audit replay expected output hash is invalid".into(),
        ));
    }
    Ok(replay)
}

fn validate_replay_envelope(
    replay: &DurableRunStoppedAuditReplayEnvelope,
    expected_kind: DurableRunStoppedAuditKind,
    run_id: &str,
    expected_invocation_id: Option<&str>,
) -> Result<(), DurableRunDriverError> {
    if replay.schema_version != RUN_STOPPED_REPLAY_SCHEMA_VERSION {
        return Err(DurableRunDriverError::InvalidCompletionPayload(format!(
            "unsupported terminal audit replay schema {}",
            replay.schema_version
        )));
    }
    if replay.audit_binding.schema_version != 1 {
        return Err(DurableRunDriverError::InvalidCompletionPayload(
            "terminal audit replay delivery policy schema is invalid".into(),
        ));
    }
    if replay.kind != expected_kind
        || replay.audit_run_id != run_id
        || replay.audit_run_id.is_empty()
        || replay.audit_run_id.chars().any(char::is_control)
        || replay.invocation_id.is_empty()
        || replay.invocation_id.chars().any(char::is_control)
        || replay.run_stopped_sequence == 0
        || replay.audit_reason.is_empty()
        || expected_invocation_id.is_some_and(|expected| replay.invocation_id != expected)
    {
        return Err(DurableRunDriverError::InvalidCompletionPayload(
            "terminal audit replay identity or event metadata is invalid".into(),
        ));
    }
    if matches!(
        expected_kind,
        DurableRunStoppedAuditKind::Canonical | DurableRunStoppedAuditKind::Direct
    ) && (replay.audit_turns != replay.terminal_receipt.turns
        || replay.audit_reason != replay.terminal_receipt.reason)
    {
        return Err(DurableRunDriverError::InvalidCompletionPayload(
            "terminal audit replay does not match its terminal receipt".into(),
        ));
    }
    Ok(())
}

fn lifecycle_not_started_output(run_id: &str, invocation_id: &str) -> Value {
    serde_json::json!({
        "status": "not_started",
        "audit_run_id": run_id,
        "invocation_id": invocation_id,
    })
}

fn lifecycle_closed_output(replay: &DurableRunStoppedAuditReplayEnvelope) -> Value {
    serde_json::json!({
        "status": "audit_closed",
        "replay": replay,
    })
}

fn replay_from_lifecycle_record(
    record: &crate::durability::ActivityRecord,
    run_id: &str,
) -> Result<Option<DurableRunStoppedAuditReplayEnvelope>, DurableRunDriverError> {
    let Some(output) = record.completed_output() else {
        return Ok(None);
    };
    let status = output.get("status").and_then(Value::as_str);
    if !matches!(status, Some("audit_closed" | "terminal_replay_authorized")) {
        return Ok(None);
    }
    let replay = output.get("replay").cloned().ok_or_else(|| {
        DurableRunDriverError::InvalidCompletionPayload(
            "closed invocation lifecycle is missing its replay envelope".into(),
        )
    })?;
    let replay: DurableRunStoppedAuditReplayEnvelope = serde_json::from_value(replay)
        .map_err(|error| DurableRunDriverError::InvalidCompletionPayload(error.to_string()))?;
    validate_replay_envelope(
        &replay,
        replay.kind,
        run_id,
        Some(&record.definition.logical_key),
    )?;
    if replay.kind == DurableRunStoppedAuditKind::Direct && status != Some("audit_closed") {
        return Err(DurableRunDriverError::InvalidCompletionPayload(
            "direct invocation lifecycle replay must be audit_closed".into(),
        ));
    }
    Ok(Some(replay))
}

fn validated_canonical_receipt_output(
    record: &crate::durability::ActivityRecord,
) -> Result<Option<Value>, DurableRunDriverError> {
    let Some(output) = record.completed_output() else {
        return Ok(None);
    };
    if record.definition.input.get("legacy_resolution").is_some() {
        let source = legacy_resolution_source(record)?;
        let resolution: DurableLegacyRunStoppedResolutionEnvelope =
            serde_json::from_value(output.clone()).map_err(|error| {
                DurableRunDriverError::InvalidCompletionPayload(format!(
                    "persisted legacy RunStopped resolution is invalid: {error}"
                ))
            })?;
        if resolution.schema_version != LEGACY_RUN_STOPPED_RESOLUTION_SCHEMA_VERSION
            || resolution.kind != LEGACY_RUN_STOPPED_RESOLUTION_KIND
            || resolution.source_activity_id != source.source_activity_id
            || resolution.source_attempt != source.source_attempt
            || resolution.source_started_sequence != source.source_started_sequence
            || resolution.source_output_hash != source.source_output_hash
            || resolution.terminal_receipt.reason.is_empty()
        {
            return Err(DurableRunDriverError::InvalidCompletionPayload(
                "persisted legacy RunStopped resolution does not match its source receipt".into(),
            ));
        }
        return serde_json::to_value(resolution.terminal_receipt)
            .map(Some)
            .map_err(|error| DurableRunDriverError::InvalidCompletionPayload(error.to_string()));
    }
    let expected_hash = record
        .definition
        .input
        .get("expected_output_hash")
        .or_else(|| record.definition.input.get("input_hash"))
        .and_then(Value::as_str)
        .ok_or_else(|| {
            DurableRunDriverError::InvalidCompletionPayload(
                "persisted RunStopped receipt has no expected output hash".into(),
            )
        })?;
    if stable_input_hash(output) != expected_hash {
        return Err(DurableRunDriverError::InvalidCompletionPayload(
            "persisted RunStopped receipt does not match its delivery intent".into(),
        ));
    }
    Ok(Some(output.clone()))
}

/// Fail-closed errors from synchronous durable coordination.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DurableRunDriverError {
    #[error("in-process durable runtime supports only sync mode, received {mode:?}")]
    UnsupportedMode { mode: DurabilityMode },
    #[error("caller-supplied durable state does not match the store for run `{run_id}`")]
    StateMismatch { run_id: String },
    #[error(transparent)]
    Store(#[from] DurableStoreError),
    #[error(transparent)]
    State(#[from] DurabilityError),
    #[error("durable run state lock is unavailable")]
    StateLock,
    #[error(
        "durable run driver is poisoned after a failed compare-and-swap; reload from the store"
    )]
    Poisoned,
    #[error(
        "invalid durable completion limit {max_completion_bytes} bytes; expected 1..={hard_limit}"
    )]
    InvalidPayloadPolicy {
        max_completion_bytes: usize,
        hard_limit: usize,
    },
    #[error(
        "durable activity result is {actual_bytes} bytes, exceeding the configured {max_bytes}-byte limit"
    )]
    CompletionPayloadTooLarge {
        actual_bytes: usize,
        max_bytes: usize,
    },
    #[error("durable activity result could not be serialized: {0}")]
    InvalidCompletionPayload(String),
    #[error("activity `{activity_id}` requires reconciliation: {reason}")]
    ReconciliationRequired { activity_id: String, reason: String },
    #[error("activity `{activity_id}` previously failed: {error}")]
    ActivityFailed { activity_id: String, error: String },
    #[error("activity `{activity_id}` was cancelled")]
    ActivityCancelled { activity_id: String },
    #[error("another in-process invocation is already active for this durable run")]
    InvocationAlreadyActive,
    #[error("durable run is owned by distributed worker `{owner_id}`")]
    WorkerLeaseRequired { owner_id: String },
    #[error(
        "terminal audit state exists; `{stable_step_id}:{logical_key}` cannot start before terminal-only recovery"
    )]
    TerminalRecoveryRequired {
        stable_step_id: String,
        logical_key: String,
    },
    #[error("terminal audit cannot start while durable activities are running: {activity_ids:?}")]
    TerminalAuditBlockedByRunningActivities { activity_ids: Vec<String> },
}

impl From<DurableRunDriverError> for crate::error::AikitError {
    fn from(error: DurableRunDriverError) -> Self {
        match error {
            DurableRunDriverError::UnsupportedMode { .. }
            | DurableRunDriverError::StateMismatch { .. }
            | DurableRunDriverError::InvalidPayloadPolicy { .. } => {
                crate::error::AikitError::Configuration(error.to_string())
            }
            DurableRunDriverError::Store(_)
            | DurableRunDriverError::State(_)
            | DurableRunDriverError::StateLock
            | DurableRunDriverError::Poisoned
            | DurableRunDriverError::CompletionPayloadTooLarge { .. }
            | DurableRunDriverError::InvalidCompletionPayload(_)
            | DurableRunDriverError::ReconciliationRequired { .. }
            | DurableRunDriverError::ActivityFailed { .. }
            | DurableRunDriverError::ActivityCancelled { .. }
            | DurableRunDriverError::InvocationAlreadyActive
            | DurableRunDriverError::WorkerLeaseRequired { .. }
            | DurableRunDriverError::TerminalRecoveryRequired { .. }
            | DurableRunDriverError::TerminalAuditBlockedByRunningActivities { .. } => {
                crate::error::AikitError::Conflict(error.to_string())
            }
        }
    }
}

/// Cloneable handle that keeps the caller-visible state synchronized with its durable store.
#[derive(Clone)]
pub struct DurableRunDriver {
    state: Arc<Mutex<RunState>>,
    store: Arc<dyn DurableStore>,
    poisoned: Arc<AtomicBool>,
    invocation_active: Arc<AtomicBool>,
    payload_policy: DurablePayloadPolicy,
    worker_lease: Option<DurableStoreLeaseAuthority>,
}

/// In-process exclusion for one runtime invocation using a shared driver.
///
/// The durable activity CAS remains the cross-process authority. This claim closes the smaller
/// same-process window before the first activity CAS, ensuring sibling `run_agent` streams cannot
/// both open an audit lifecycle from one cloned driver.
pub(crate) struct DurableInvocationClaim {
    invocation_active: Arc<AtomicBool>,
}

impl Drop for DurableInvocationClaim {
    fn drop(&mut self) {
        self.invocation_active.store(false, Ordering::Release);
    }
}

impl DurableRunDriver {
    /// Attach a caller-supplied state to its store.
    ///
    /// A fresh state is created in the store. An existing state must match byte-for-byte so a
    /// stale worker cannot silently replace newer history.
    pub fn new(
        state: RunState,
        store: Arc<dyn DurableStore>,
    ) -> Result<Self, DurableRunDriverError> {
        Self::new_with_payload_policy(state, store, DurablePayloadPolicy::default())
    }

    /// Attach state with an explicit durable-result storage policy.
    pub fn new_with_payload_policy(
        state: RunState,
        store: Arc<dyn DurableStore>,
        payload_policy: DurablePayloadPolicy,
    ) -> Result<Self, DurableRunDriverError> {
        if state.durability() != DurabilityMode::Sync {
            return Err(DurableRunDriverError::UnsupportedMode {
                mode: state.durability(),
            });
        }
        match store.load(state.run_id()) {
            Ok(stored) if stored != state => {
                return Err(DurableRunDriverError::StateMismatch {
                    run_id: state.run_id().into(),
                });
            }
            Ok(_) => {}
            Err(DurableStoreError::NotFound { .. }) => match store.create(&state) {
                Ok(()) => {}
                // A concurrent creator may win between load and create. Accept only the exact
                // same state; any other revision remains a fail-closed mismatch.
                Err(DurableStoreError::AlreadyExists { .. }) => {
                    let stored = store.load(state.run_id())?;
                    if stored != state {
                        return Err(DurableRunDriverError::StateMismatch {
                            run_id: state.run_id().into(),
                        });
                    }
                }
                Err(error) => return Err(error.into()),
            },
            Err(error) => return Err(error.into()),
        }
        Ok(Self {
            state: Arc::new(Mutex::new(state)),
            store,
            poisoned: Arc::new(AtomicBool::new(false)),
            invocation_active: Arc::new(AtomicBool::new(false)),
            payload_policy,
            worker_lease: None,
        })
    }

    /// The exact shared state authority used by runtime and durable governance.
    pub(crate) fn state_handle(&self) -> Arc<Mutex<RunState>> {
        self.state.clone()
    }

    /// The exact persistence authority used by runtime and durable governance.
    pub(crate) fn store_handle(&self) -> Arc<dyn DurableStore> {
        self.store.clone()
    }

    pub(crate) fn poison_handle(&self) -> Arc<AtomicBool> {
        self.poisoned.clone()
    }

    /// The opaque store fence owned by this driver, when it is executing under a durable worker.
    ///
    /// Runtime collaborators such as durable governance must carry the same authority through
    /// every store CAS instead of falling back to the unfenced compatibility path.
    pub(crate) fn worker_lease_authority(&self) -> Option<DurableStoreLeaseAuthority> {
        self.worker_lease.clone()
    }

    pub fn is_poisoned(&self) -> bool {
        self.poisoned.load(Ordering::Acquire)
    }

    pub fn run_id(&self) -> Result<String, DurableRunDriverError> {
        Ok(self.lock_state()?.run_id().to_string())
    }

    pub fn snapshot(&self) -> Result<RunState, DurableRunDriverError> {
        if self.is_poisoned() {
            return Err(DurableRunDriverError::Poisoned);
        }
        Ok(self.lock_state()?.clone())
    }

    pub(crate) fn claim_worker_lease(
        &self,
        owner_id: &str,
        lease_id: &str,
        claimed_at_unix_ms: u64,
        expires_at_unix_ms: u64,
    ) -> Result<bool, DurableRunDriverError> {
        self.transition_inner(false, |candidate| {
            candidate
                .claim_worker_lease(owner_id, lease_id, claimed_at_unix_ms, expires_at_unix_ms)
                .map_err(DurableRunDriverError::from)
        })
    }

    pub(crate) fn bind_worker_lease(
        mut self,
        owner_id: &str,
        lease_id: &str,
    ) -> Result<Self, DurableRunDriverError> {
        let now_unix_ms = self.store.worker_lease_clock_unix_ms()?;
        {
            let current = self.lock_state()?;
            let lease = current
                .worker_lease()
                .ok_or_else(|| DurabilityError::WorkerLeaseLost {
                    owner_id: owner_id.to_string(),
                })?;
            if lease.owner_id != owner_id
                || lease.lease_id != lease_id
                || lease.expires_at_unix_ms <= now_unix_ms
            {
                return Err(DurabilityError::WorkerLeaseLost {
                    owner_id: owner_id.to_string(),
                }
                .into());
            }
        }
        self.worker_lease = Some(DurableStoreLeaseAuthority::new(owner_id, lease_id));
        Ok(self)
    }

    pub(crate) fn renew_worker_lease(
        &self,
        renewed_at_unix_ms: u64,
        expires_at_unix_ms: u64,
    ) -> Result<(), DurableRunDriverError> {
        let binding = self.worker_lease.clone().ok_or_else(|| {
            DurableRunDriverError::InvalidCompletionPayload(
                "worker lease renewal requires an owner-bound driver".into(),
            )
        })?;
        self.transition_inner(false, |candidate| {
            candidate.renew_worker_lease(
                binding.owner_id(),
                binding.lease_id(),
                renewed_at_unix_ms,
                expires_at_unix_ms,
            )?;
            Ok(())
        })
    }

    pub(crate) fn release_worker_lease(
        &self,
        released_at_unix_ms: u64,
    ) -> Result<(), DurableRunDriverError> {
        let binding = self.worker_lease.clone().ok_or_else(|| {
            DurableRunDriverError::InvalidCompletionPayload(
                "worker lease release requires an owner-bound driver".into(),
            )
        })?;
        self.transition_inner(false, |candidate| {
            candidate.release_worker_lease(
                binding.owner_id(),
                binding.lease_id(),
                released_at_unix_ms,
            )?;
            Ok(())
        })
    }

    pub(crate) fn reconcile_worker_activity(
        &self,
        reconciliation_id: &str,
        activity_id: &str,
        resolution: crate::durability::ActivityReconciliation,
    ) -> Result<(), DurableRunDriverError> {
        self.transition(|candidate| {
            candidate.reconcile_activity(reconciliation_id, activity_id, resolution)?;
            Ok(())
        })
    }

    /// Claim this shared driver for exactly one in-process runtime stream.
    pub(crate) fn claim_invocation(&self) -> Result<DurableInvocationClaim, DurableRunDriverError> {
        self.invocation_active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| DurableRunDriverError::InvocationAlreadyActive)?;
        Ok(DurableInvocationClaim {
            invocation_active: self.invocation_active.clone(),
        })
    }

    /// Classify an invocation before it emits `RunStarted` or performs any replay work.
    ///
    /// A completed terminal-audit receipt means only the terminal run CAS remains. A running or
    /// otherwise ambiguous receipt is converted to reconciliation-required in the store before the
    /// caller is allowed to proceed, preventing blind audit redelivery after a crash or partial
    /// fan-out.
    pub(crate) fn invocation_disposition(
        &self,
    ) -> Result<DurableInvocationDisposition, DurableRunDriverError> {
        self.invocation_disposition_with_active_lifecycle(None)
    }

    /// Reclassify a run while the named invocation owns an already-persisted lifecycle fence.
    ///
    /// Every other incomplete fence is treated as a crashed or concurrent invocation. The active
    /// fence is ignored only by its owner so provider/tool boundaries can still discover a
    /// terminal receipt written by an older runtime without falsely reconciling themselves.
    pub(crate) fn invocation_disposition_for_active_lifecycle(
        &self,
        invocation_id: &str,
    ) -> Result<DurableInvocationDisposition, DurableRunDriverError> {
        self.invocation_disposition_with_active_lifecycle(Some(invocation_id))
    }

    fn invocation_disposition_with_active_lifecycle(
        &self,
        active_invocation_id: Option<&str>,
    ) -> Result<DurableInvocationDisposition, DurableRunDriverError> {
        let raw = self.transition(|candidate| {
            let run_id = candidate.run_id().to_string();
            let invocation_lifecycle_records = candidate
                .projection()
                .activities
                .values()
                .filter(|record| {
                    record.definition.stable_step_id == RUNTIME_INVOCATION_LIFECYCLE_STEP_ID
                        && is_reserved_audit_activity(record)
                })
                .cloned()
                .collect::<Vec<_>>();
            let current_audit_record = candidate
                .projection()
                .activities
                .values()
                .find(|record| {
                    record.definition.stable_step_id == RUNTIME_RUN_STOPPED_AUDIT_STEP_ID
                        && record.definition.logical_key == RUN_STOPPED_AUDIT_LOGICAL_KEY
                        && is_reserved_audit_activity(record)
                })
                .cloned();
            let recovery_audit_records = candidate
                .projection()
                .activities
                .values()
                .filter(|record| {
                    record.definition.stable_step_id
                        == RUNTIME_RECOVERY_RUN_STOPPED_AUDIT_STEP_ID
                        && is_reserved_audit_activity(record)
                })
                .cloned()
                .collect::<Vec<_>>();
            let legacy_audit_record = candidate
                .projection()
                .activities
                .values()
                .find(|record| {
                    record.definition.stable_step_id
                        == RUNTIME_LEGACY_RUN_STOPPED_AUDIT_STEP_ID
                        && record.definition.logical_key == RUN_STOPPED_AUDIT_LOGICAL_KEY
                        && is_reserved_audit_activity(record)
                })
                .cloned();
            let non_reserved_running_reconcile_activities = candidate
                .projection()
                .activities
                .values()
                .filter(|record| {
                    !is_reserved_audit_activity(record)
                        && record.definition.side_effect_class == SideEffectClass::ReconcileRequired
                        && record.latest_attempt().is_some_and(|attempt| {
                            attempt.status == crate::durability::ActivityAttemptStatus::Running
                        })
                })
                .cloned()
                .collect::<Vec<_>>();

            let pending_reconciliation = candidate
                .projection()
                .activities
                .values()
                .any(|record| {
                    record.latest_attempt().is_some_and(|attempt| {
                        attempt.status
                            == crate::durability::ActivityAttemptStatus::ReconcileRequired
                    })
                });

            // Discover every open reserved audit activity in one transition. Returning early on
            // the run-level ReconcileRequired status would hide sibling Running markers and force
            // operators through serial reconcile/resume cycles for one failed closure.
            let mut reserved_ambiguity = candidate.status()
                == DurableRunStatus::ReconcileRequired
                || pending_reconciliation;
            let mut reconciliation_reason = candidate
                .projection()
                .pause_reason
                .clone()
                .unwrap_or_else(|| "durable run requires explicit reconciliation".into());
            for record in invocation_lifecycle_records {
                if quarantine_unstarted_reserved_activity(
                    candidate,
                    &record,
                    "persisted invocation lifecycle schedule has no durable attempt",
                )? {
                    reserved_ambiguity = true;
                    reconciliation_reason =
                        "an orphan invocation lifecycle schedule requires explicit reconciliation"
                            .into();
                    continue;
                }
                if let Some(replay) = replay_from_lifecycle_record(&record, &run_id)? {
                    // A Direct replay closes only this invocation's audit span. It is not a
                    // durable terminal receipt and cannot authorize run finalization.
                    let _ = replay;
                    continue;
                }
                let belongs_to_active_invocation = active_invocation_id
                    .is_some_and(|invocation_id| record.definition.logical_key == invocation_id);
                if let Some(attempt) = record.latest_attempt() {
                    match attempt.status {
                        crate::durability::ActivityAttemptStatus::Running
                            if !belongs_to_active_invocation =>
                        {
                            candidate.fail_activity(
                                &record.definition.activity_id,
                                attempt.attempt,
                                "worker stopped before the invocation audit lifecycle was closed",
                                false,
                                true,
                            )?;
                            reserved_ambiguity = true;
                            reconciliation_reason = "an invocation audit lifecycle may be open and requires explicit reconciliation".into();
                        }
                        crate::durability::ActivityAttemptStatus::ReconcileRequired => {
                            reserved_ambiguity = true;
                        }
                        crate::durability::ActivityAttemptStatus::Failed
                            if attempt.retryable && !attempt.effect_ambiguous =>
                        {
                            // New states cannot reach this branch: lifecycle SafeToRetry is
                            // rejected at reconciliation time. Preserve fail-closed behavior for
                            // pre-contract snapshots instead of opening a new RunStarted.
                            reserved_ambiguity = true;
                            reconciliation_reason = "legacy invocation lifecycle reconciliation is incomplete; record an explicit Completed outcome".into();
                        }
                        crate::durability::ActivityAttemptStatus::Cancelled => {
                            reserved_ambiguity = true;
                            reconciliation_reason = "cancelled invocation lifecycle reconciliation is unsupported".into();
                        }
                        _ => {}
                    }
                }
            }
            for record in &recovery_audit_records {
                if quarantine_unstarted_reserved_activity(
                    candidate,
                    record,
                    "persisted recovery RunStopped schedule has no durable attempt",
                )? {
                    reserved_ambiguity = true;
                    reconciliation_reason =
                        "an orphan recovery RunStopped schedule requires explicit reconciliation"
                            .into();
                    continue;
                }
                if let Some(attempt) = record.latest_attempt() {
                    if attempt.status == crate::durability::ActivityAttemptStatus::Running {
                        candidate.fail_activity(
                            &record.definition.activity_id,
                            attempt.attempt,
                            "worker stopped before recovery RunStopped audit delivery was committed",
                            false,
                            true,
                        )?;
                        reserved_ambiguity = true;
                        reconciliation_reason = "recovery RunStopped audit delivery is ambiguous and requires explicit reconciliation".into();
                    } else if attempt.status
                        == crate::durability::ActivityAttemptStatus::ReconcileRequired
                    {
                        reserved_ambiguity = true;
                    }
                }
            }
            if let Some(record) = &current_audit_record {
                if quarantine_unstarted_reserved_activity(
                    candidate,
                    record,
                    "persisted RunStopped schedule has no durable attempt",
                )? {
                    reserved_ambiguity = true;
                    reconciliation_reason =
                        "an orphan RunStopped schedule requires explicit reconciliation".into();
                }
                if let Some(attempt) = record.latest_attempt() {
                    if attempt.status == crate::durability::ActivityAttemptStatus::Running {
                        candidate.fail_activity(
                            &record.definition.activity_id,
                            attempt.attempt,
                            "worker stopped before RunStopped audit delivery was committed",
                            false,
                            true,
                        )?;
                        reserved_ambiguity = true;
                        reconciliation_reason = "RunStopped audit delivery is ambiguous and requires explicit reconciliation".into();
                    } else if attempt.status
                        == crate::durability::ActivityAttemptStatus::ReconcileRequired
                    {
                        reserved_ambiguity = true;
                    }
                }
            }
            for record in non_reserved_running_reconcile_activities {
                let attempt = record
                    .latest_attempt()
                    .expect("running activity was filtered above");
                if candidate.status().is_terminal() {
                    candidate.quarantine_terminal_legacy_running_activity(
                        &record.definition.activity_id,
                        "worker found a migrated v1 terminal run with an unresolved activity",
                    )?;
                } else {
                    candidate.fail_activity(
                        &record.definition.activity_id,
                        attempt.attempt,
                        "worker stopped before a reconciliation-required activity completed",
                        false,
                        true,
                    )?;
                }
                reserved_ambiguity = true;
                reconciliation_reason =
                    "a reconciliation-required activity may still be open".into();
            }

            if reserved_ambiguity {
                return Ok(RawInvocationDisposition::ReconcileRequired {
                    reason: reconciliation_reason,
                });
            }

            // Reconciliation deliberately leaves the run paused. Terminal receipt finalization or
            // SafeToRetry audit replay must wait for a separate explicit Resume command.
            if candidate.status() == DurableRunStatus::Paused {
                return Ok(RawInvocationDisposition::AwaitingResume {
                    reason: candidate.projection().pause_reason.clone(),
                });
            }
            for record in recovery_audit_records {
                if record.completed_output().is_some() {
                    continue;
                }
                if candidate.status().is_terminal() {
                    return Ok(RawInvocationDisposition::ReconcileRequired {
                        reason: "terminal run still has an unresolved recovery RunStopped audit"
                            .into(),
                    });
                }
                if record.latest_attempt().is_some_and(|attempt| {
                    attempt.status == crate::durability::ActivityAttemptStatus::Failed
                        && attempt.retryable
                        && !attempt.effect_ambiguous
                }) {
                    return Ok(RawInvocationDisposition::RetryTerminalAudit(
                        replay_from_record(
                            &record,
                            DurableRunStoppedAuditKind::Recovery,
                            &run_id,
                        )?,
                    ));
                }
                return Ok(RawInvocationDisposition::ReconcileRequired {
                    reason: "recovery RunStopped audit has no safe terminal disposition".into(),
                });
            }

            if let Some(record) = current_audit_record {
                if let Some(output) = validated_canonical_receipt_output(&record)? {
                    return Ok(RawInvocationDisposition::FinalizeTerminal(output));
                }
                if candidate.status().is_terminal() {
                    return Ok(RawInvocationDisposition::ReconcileRequired {
                        reason: "terminal run still has an unresolved RunStopped audit".into(),
                    });
                }
                if record.latest_attempt().is_some_and(|attempt| {
                    attempt.status == crate::durability::ActivityAttemptStatus::Failed
                        && attempt.retryable
                        && !attempt.effect_ambiguous
                }) {
                    return Ok(RawInvocationDisposition::RetryTerminalAudit(
                        replay_from_record(
                            &record,
                            DurableRunStoppedAuditKind::Canonical,
                            &run_id,
                        )?,
                    ));
                }
                return Ok(RawInvocationDisposition::ReconcileRequired {
                    reason: "RunStopped audit has no safe terminal disposition".into(),
                });
            }

            if let Some(record) = legacy_audit_record {
                if candidate.status().is_terminal() {
                    if record.latest_attempt().is_none() {
                        candidate.quarantine_unstarted_reserved_activity(
                            &record.definition.activity_id,
                            "terminal v1 RunStopped schedule has no durable delivery attempt",
                        )?;
                    }
                    if record.latest_attempt().is_some_and(|attempt| {
                        attempt.status == crate::durability::ActivityAttemptStatus::Completed
                            && attempt.output.is_some()
                            && attempt.output_hash.is_some()
                    })
                    {
                        return Ok(RawInvocationDisposition::AlreadyTerminal {
                            status: candidate.status(),
                            reason: candidate.projection().pause_reason.clone(),
                        });
                    }
                    return Ok(RawInvocationDisposition::ReconcileRequired {
                        reason: "terminal v1 run has an unresolved legacy RunStopped outcome; record a typed completed attestation"
                            .into(),
                    });
                }
                if candidate.status() == DurableRunStatus::Running {
                    if record.latest_attempt().is_none() {
                        candidate.quarantine_unstarted_reserved_activity(
                            &record.definition.activity_id,
                            "migrated v1 RunStopped schedule has no durable attempt",
                        )?;
                    } else if let Some(attempt) = record.latest_attempt() {
                        match attempt.status {
                            crate::durability::ActivityAttemptStatus::Running => {
                                candidate.fail_activity(
                                    &record.definition.activity_id,
                                    attempt.attempt,
                                    "legacy RunStopped receipt outcome is ambiguous",
                                    false,
                                    true,
                                )?;
                            }
                            crate::durability::ActivityAttemptStatus::Completed
                            | crate::durability::ActivityAttemptStatus::Failed
                            | crate::durability::ActivityAttemptStatus::Cancelled => {
                                let prior_attestation = record.completed_output().and_then(|output| {
                                    serde_json::from_value::<
                                        DurableLegacyRunStoppedResolutionEnvelope,
                                    >(output.clone())
                                    .ok()
                                    .filter(|resolution| {
                                        resolution.schema_version
                                            == LEGACY_RUN_STOPPED_RESOLUTION_SCHEMA_VERSION
                                            && resolution.kind
                                                == LEGACY_RUN_STOPPED_RESOLUTION_KIND
                                            && resolution.source_activity_id
                                                == record.definition.activity_id
                                            && resolution.source_attempt == attempt.attempt
                                            && resolution.source_started_sequence
                                                == attempt.started_sequence
                                            && !resolution.terminal_receipt.reason.is_empty()
                                            && candidate.events().iter().any(|event| {
                                                event.schema_version >= 2
                                                    && matches!(
                                                        &event.kind,
                                                        crate::durability::RunEventKind::ActivityReconciled {
                                                            activity_id,
                                                            resolution: crate::durability::ActivityReconciliation::Completed { output },
                                                            ..
                                                        } if activity_id == &record.definition.activity_id
                                                            && output == record.completed_output().expect("completed output exists")
                                                    )
                                            })
                                    })
                                });
                                let marker_input = legacy_resolution_marker_input(&record)?;
                                let marker = candidate.prepare_activity(
                                    RUNTIME_RUN_STOPPED_AUDIT_STEP_ID,
                                    RUN_STOPPED_AUDIT_LOGICAL_KEY,
                                    marker_input,
                                    SideEffectClass::ReconcileRequired,
                                    None,
                                )?;
                                if let ActivityDecision::Execute {
                                    activity_id,
                                    attempt,
                                    ..
                                } = marker
                                {
                                    if let Some(resolution) = prior_attestation {
                                        let bridge = candidate.activity(&activity_id)?.clone();
                                        let output = legacy_resolution_output(
                                            &bridge,
                                            resolution.terminal_receipt.clone(),
                                        )?;
                                        candidate.fail_activity(
                                            &activity_id,
                                            attempt,
                                            "typed legacy source attestation is ready for canonical migration",
                                            false,
                                            true,
                                        )?;
                                        candidate.reconcile_activity(
                                            "runtime-legacy-source-attestation-bridge",
                                            &activity_id,
                                            crate::durability::ActivityReconciliation::Completed {
                                                output,
                                            },
                                        )?;
                                        return Ok(RawInvocationDisposition::AwaitingResume {
                                            reason: Some(
                                                "typed legacy RunStopped attestation migrated; resume once more to finalize"
                                                    .into(),
                                            ),
                                        });
                                    }
                                    candidate.fail_activity(
                                        &activity_id,
                                        attempt,
                                        "legacy RunStopped receipt requires explicit typed terminal metadata",
                                        false,
                                        true,
                                    )?;
                                }
                            }
                            crate::durability::ActivityAttemptStatus::ReconcileRequired => {}
                        }
                    }
                }
                return Ok(RawInvocationDisposition::ReconcileRequired {
                    reason: "legacy RunStopped receipt requires explicit reconciliation".into(),
                });
            }

            if candidate.status().is_terminal() {
                return Ok(RawInvocationDisposition::AlreadyTerminal {
                    status: candidate.status(),
                    reason: candidate.projection().pause_reason.clone(),
                });
            }
            Ok(RawInvocationDisposition::Execute)
        })?;

        match raw {
            RawInvocationDisposition::Execute => Ok(DurableInvocationDisposition::Execute),
            RawInvocationDisposition::RetryTerminalAudit(replay) => {
                Ok(DurableInvocationDisposition::RetryTerminalAudit(replay))
            }
            RawInvocationDisposition::AwaitingResume { reason } => {
                Ok(DurableInvocationDisposition::AwaitingResume { reason })
            }
            RawInvocationDisposition::ReconcileRequired { reason } => {
                Ok(DurableInvocationDisposition::ReconcileRequired { reason })
            }
            RawInvocationDisposition::AlreadyTerminal { status, reason } => {
                Ok(DurableInvocationDisposition::AlreadyTerminal { status, reason })
            }
            RawInvocationDisposition::FinalizeTerminal(output) => {
                let receipt = serde_json::from_value(output).map_err(|error| {
                    DurableRunDriverError::InvalidCompletionPayload(format!(
                        "persisted RunStopped receipt is invalid: {error}"
                    ))
                })?;
                Ok(DurableInvocationDisposition::FinalizeTerminal(receipt))
            }
        }
    }

    /// Persist scheduling and attempt-start events before returning permission to execute.
    pub fn begin_activity(
        &self,
        stable_step_id: &str,
        logical_key: &str,
        input: Value,
        side_effect_class: SideEffectClass,
        idempotency_key: Option<String>,
    ) -> Result<DurableActivity, DurableRunDriverError> {
        let decision = self.transition(|candidate| {
            let is_terminal_audit = (stable_step_id == RUNTIME_RUN_STOPPED_AUDIT_STEP_ID
                || stable_step_id == RUNTIME_LEGACY_RUN_STOPPED_AUDIT_STEP_ID)
                && logical_key == RUN_STOPPED_AUDIT_LOGICAL_KEY;
            let terminal_audit_exists = candidate.projection().activities.values().any(|record| {
                (record.definition.stable_step_id == RUNTIME_RUN_STOPPED_AUDIT_STEP_ID
                    || record.definition.stable_step_id == RUNTIME_LEGACY_RUN_STOPPED_AUDIT_STEP_ID)
                    && record.definition.logical_key == RUN_STOPPED_AUDIT_LOGICAL_KEY
            });
            if is_terminal_audit && terminal_audit_exists {
                return Err(DurableRunDriverError::TerminalRecoveryRequired {
                    stable_step_id: stable_step_id.into(),
                    logical_key: logical_key.into(),
                });
            }
            if is_terminal_audit {
                let running_activity_ids = candidate
                    .projection()
                    .activities
                    .values()
                    .filter(|record| {
                        record.definition.stable_step_id != RUNTIME_RUN_STOPPED_AUDIT_STEP_ID
                            && record.definition.stable_step_id
                                != RUNTIME_LEGACY_RUN_STOPPED_AUDIT_STEP_ID
                            && record.latest_attempt().is_some_and(|attempt| {
                                attempt.status == crate::durability::ActivityAttemptStatus::Running
                            })
                    })
                    .map(|record| record.definition.activity_id.clone())
                    .collect::<Vec<_>>();
                if !running_activity_ids.is_empty() {
                    return Err(
                        DurableRunDriverError::TerminalAuditBlockedByRunningActivities {
                            activity_ids: running_activity_ids,
                        },
                    );
                }
            }
            if !is_terminal_audit && terminal_audit_exists {
                return Err(DurableRunDriverError::TerminalRecoveryRequired {
                    stable_step_id: stable_step_id.into(),
                    logical_key: logical_key.into(),
                });
            }
            candidate
                .prepare_activity(
                    stable_step_id,
                    logical_key,
                    input,
                    side_effect_class,
                    idempotency_key,
                )
                .map_err(DurableRunDriverError::from)
        })?;
        durable_activity_from_decision(decision)
    }

    /// Persist a successful result in one CAS operation.
    ///
    /// `output` is required for deterministic resume and is stored verbatim. Provider/tool output
    /// can contain the same sensitive data as a transcript, so every [`DurableStore`] must receive
    /// the same access control, retention, and at-rest protection as transcript storage. The
    /// driver does not claim or silently add encryption.
    pub fn complete_activity(
        &self,
        activity_id: &str,
        attempt: u32,
        output: Value,
    ) -> Result<(), DurableRunDriverError> {
        let actual_bytes = serde_json::to_vec(&output)
            .map_err(|error| DurableRunDriverError::InvalidCompletionPayload(error.to_string()))?
            .len();
        if actual_bytes > self.payload_policy.max_completion_bytes {
            let size_error = DurableRunDriverError::CompletionPayloadTooLarge {
                actual_bytes,
                max_bytes: self.payload_policy.max_completion_bytes,
            };
            self.transition(|candidate| {
                candidate.fail_activity(
                    activity_id,
                    attempt,
                    "activity result exceeded durable completion size policy after execution",
                    false,
                    true,
                )?;
                Ok(())
            })?;
            return Err(size_error);
        }
        self.transition(|candidate| {
            candidate.complete_activity(activity_id, attempt, output)?;
            Ok(())
        })
    }

    /// Persist failure evidence. Ambiguous unsafe work moves the run to reconciliation-required.
    pub fn fail_activity(
        &self,
        activity_id: &str,
        attempt: u32,
        error: impl Into<String>,
        retryable: bool,
        effect_ambiguous: bool,
    ) -> Result<ActivityDecision, DurableRunDriverError> {
        let error = error.into();
        self.transition(|candidate| {
            let decision = candidate.fail_activity(
                activity_id,
                attempt,
                error,
                retryable,
                effect_ambiguous,
            )?;
            Ok(decision)
        })
    }

    /// Persist a fence before `RunStarted` can reach any audit sink.
    ///
    /// A running fence means the invocation may have opened an external audit lifecycle. It is
    /// completed only after the matching `RunStopped` has been accepted by every configured sink.
    /// Consequently, even a later poisoned driver or failed recovery-marker CAS leaves durable
    /// evidence that restart must reconcile instead of blindly terminalizing an older receipt.
    pub(crate) fn begin_invocation_lifecycle(
        &self,
        invocation_id: &str,
    ) -> Result<DurableActivity, DurableRunDriverError> {
        let decision = self.transition(|candidate| {
            let lifecycle_is_open = candidate.projection().activities.values().any(|record| {
                record.definition.stable_step_id == RUNTIME_INVOCATION_LIFECYCLE_STEP_ID
                    && record.latest_attempt().is_some_and(|attempt| {
                        attempt.status == crate::durability::ActivityAttemptStatus::Running
                    })
            });
            if lifecycle_is_open {
                return Err(DurableRunDriverError::InvocationAlreadyActive);
            }
            let terminal_audit_exists = candidate.projection().activities.values().any(|record| {
                (record.definition.stable_step_id == RUNTIME_RUN_STOPPED_AUDIT_STEP_ID
                    || record.definition.stable_step_id == RUNTIME_LEGACY_RUN_STOPPED_AUDIT_STEP_ID)
                    && record.definition.logical_key == RUN_STOPPED_AUDIT_LOGICAL_KEY
            });
            if terminal_audit_exists {
                return Err(DurableRunDriverError::TerminalRecoveryRequired {
                    stable_step_id: RUNTIME_INVOCATION_LIFECYCLE_STEP_ID.into(),
                    logical_key: invocation_id.into(),
                });
            }
            candidate
                .prepare_activity(
                    RUNTIME_INVOCATION_LIFECYCLE_STEP_ID,
                    invocation_id,
                    serde_json::json!({
                        "schema_version": RUN_STOPPED_REPLAY_SCHEMA_VERSION,
                        "audit_run_id": candidate.run_id(),
                        "invocation_id": invocation_id,
                    }),
                    SideEffectClass::ReconcileRequired,
                    None,
                )
                .map_err(DurableRunDriverError::from)
        })?;
        durable_activity_from_decision(decision)
    }

    /// Close a fence when revalidation proves `RunStarted` was never attempted.
    pub(crate) fn complete_unstarted_invocation_lifecycle(
        &self,
        activity_id: &str,
        attempt: u32,
    ) -> Result<(), DurableRunDriverError> {
        self.transition(|candidate| {
            let record = candidate.activity(activity_id)?.clone();
            let invocation_id = record.definition.logical_key;
            candidate.complete_activity(
                activity_id,
                attempt,
                lifecycle_not_started_output(candidate.run_id(), &invocation_id),
            )?;
            Ok(())
        })
    }

    /// Commit evidence that a direct `RunStopped` closed this invocation's audit lifecycle.
    pub(crate) fn complete_invocation_lifecycle_with_replay(
        &self,
        activity_id: &str,
        attempt: u32,
        replay: &DurableRunStoppedAuditReplayEnvelope,
    ) -> Result<(), DurableRunDriverError> {
        self.transition(|candidate| {
            let record = candidate.activity(activity_id)?.clone();
            validate_replay_envelope(
                replay,
                replay.kind,
                candidate.run_id(),
                Some(&record.definition.logical_key),
            )?;
            candidate.complete_activity(activity_id, attempt, lifecycle_closed_output(replay))?;
            Ok(())
        })
    }

    /// Close an active lifecycle only when the persisted canonical receipt is its exact typed
    /// replay. A legacy-resolution bridge deliberately has no invocation identity, so it cannot
    /// close a lifecycle on this path.
    pub(crate) fn close_active_lifecycle_from_matching_completed_canonical_replay(
        &self,
        lifecycle_activity_id: &str,
        lifecycle_attempt: u32,
    ) -> Result<bool, DurableRunDriverError> {
        self.transition(|candidate| {
            let lifecycle = candidate.activity(lifecycle_activity_id)?.clone();
            if lifecycle.definition.stable_step_id != RUNTIME_INVOCATION_LIFECYCLE_STEP_ID
                || !is_reserved_audit_activity(&lifecycle)
            {
                return Err(DurableRunDriverError::InvalidCompletionPayload(
                    "canonical receipt recovery requires an invocation lifecycle activity".into(),
                ));
            }
            let canonical = candidate
                .projection()
                .activities
                .values()
                .find(|record| {
                    record.definition.stable_step_id == RUNTIME_RUN_STOPPED_AUDIT_STEP_ID
                        && record.definition.logical_key == RUN_STOPPED_AUDIT_LOGICAL_KEY
                        && record.completed_output().is_some()
                })
                .cloned();
            let Some(canonical) = canonical else {
                return Ok(false);
            };
            if canonical
                .definition
                .input
                .get("legacy_resolution")
                .is_some()
            {
                return Ok(false);
            }
            let replay = replay_from_record(
                &canonical,
                DurableRunStoppedAuditKind::Canonical,
                candidate.run_id(),
            )?;
            let receipt_output =
                validated_canonical_receipt_output(&canonical)?.ok_or_else(|| {
                    DurableRunDriverError::InvalidCompletionPayload(
                        "completed canonical receipt output is missing".into(),
                    )
                })?;
            let receipt: DurableRunStoppedReceipt = serde_json::from_value(receipt_output)
                .map_err(|error| {
                    DurableRunDriverError::InvalidCompletionPayload(format!(
                        "persisted RunStopped receipt is invalid: {error}"
                    ))
                })?;
            if replay.terminal_receipt != receipt
                || replay.invocation_id != lifecycle.definition.logical_key
            {
                return Ok(false);
            }
            if let Some(closed_replay) =
                replay_from_lifecycle_record(&lifecycle, candidate.run_id())?
            {
                return Ok(closed_replay == replay);
            }
            let attempt = lifecycle.latest_attempt().ok_or_else(|| {
                DurableRunDriverError::InvalidCompletionPayload(
                    "matching canonical receipt lifecycle has no attempt".into(),
                )
            })?;
            if attempt.attempt != lifecycle_attempt
                || attempt.status != crate::durability::ActivityAttemptStatus::Running
            {
                return Ok(false);
            }
            candidate.complete_activity(
                lifecycle_activity_id,
                lifecycle_attempt,
                lifecycle_closed_output(&replay),
            )?;
            Ok(true)
        })
    }

    /// Preserve an ambiguous invocation audit lifecycle for explicit reconciliation.
    pub(crate) fn fail_invocation_lifecycle(
        &self,
        activity_id: &str,
        attempt: u32,
    ) -> Result<(), DurableRunDriverError> {
        self.fail_activity(
            activity_id,
            attempt,
            "fail-closed invocation audit lifecycle was not accepted by every sink",
            false,
            true,
        )
        .map(|_| ())
    }

    /// Persist a `RunStopped` delivery intent before the fail-closed audit sink is called.
    ///
    /// Audit delivery is an external side effect. A crash after the sink accepts the record but
    /// before its result is committed therefore cannot be retried blindly: on restart the running
    /// activity moves the run to reconciliation-required. A committed delivery result is reused,
    /// allowing the terminal run CAS to be retried without emitting a duplicate audit record or
    /// rerunning provider/tool effects.
    #[cfg(test)]
    pub(crate) fn begin_run_stopped_audit(
        &self,
        receipt: &DurableRunStoppedReceipt,
    ) -> Result<DurableActivity, DurableRunDriverError> {
        let snapshot = self.snapshot()?;
        let running_activity_ids = snapshot
            .projection()
            .activities
            .values()
            .filter(|record| {
                record.latest_attempt().is_some_and(|attempt| {
                    attempt.status == crate::durability::ActivityAttemptStatus::Running
                })
            })
            .map(|record| record.definition.activity_id.clone())
            .collect::<Vec<_>>();
        if !running_activity_ids.is_empty() {
            return Err(
                DurableRunDriverError::TerminalAuditBlockedByRunningActivities {
                    activity_ids: running_activity_ids,
                },
            );
        }
        let invocation_id = format!("test-terminal-{}", snapshot.events().len() + 1);
        let DurableActivity::Execute {
            activity_id: lifecycle_activity_id,
            attempt: lifecycle_attempt,
            ..
        } = self.begin_invocation_lifecycle(&invocation_id)?
        else {
            return Err(DurabilityError::InvalidEvent {
                reason: "test terminal lifecycle was unexpectedly reused".into(),
            }
            .into());
        };
        let replay = DurableRunStoppedAuditReplayEnvelope::new(
            DurableRunStoppedAuditKind::Canonical,
            receipt.clone(),
            snapshot.run_id(),
            invocation_id,
            1,
            receipt.turns,
            receipt.reason.clone(),
            crate::observability::AuditReplayBinding {
                schema_version: 1,
                delivery_id: None,
                sink_count: 0,
                payload_policy: crate::observability::AuditPayloadPolicy::MetadataOnly,
                failure_mode: crate::observability::AuditFailureMode::BestEffort,
                max_preview_bytes: 4096,
            },
        );
        match self.begin_invocation_run_stopped_audit(&replay) {
            Ok(activity) => Ok(activity),
            Err(error) => {
                let _ = self.complete_unstarted_invocation_lifecycle(
                    &lifecycle_activity_id,
                    lifecycle_attempt,
                );
                Err(error)
            }
        }
    }

    /// Persist the canonical terminal audit intent for the invocation that owns an open lifecycle
    /// fence. Other callers use [`Self::begin_run_stopped_audit`] and remain blocked by that fence.
    pub(crate) fn begin_invocation_run_stopped_audit(
        &self,
        replay: &DurableRunStoppedAuditReplayEnvelope,
    ) -> Result<DurableActivity, DurableRunDriverError> {
        let receipt = serde_json::to_value(&replay.terminal_receipt)
            .map_err(|error| DurableRunDriverError::InvalidCompletionPayload(error.to_string()))?;
        let input = terminal_audit_input(replay, &receipt)?;
        let decision = self.transition(|candidate| {
            validate_replay_envelope(
                replay,
                DurableRunStoppedAuditKind::Canonical,
                candidate.run_id(),
                Some(&replay.invocation_id),
            )?;
            let lifecycle_is_active = candidate.projection().activities.values().any(|record| {
                record.definition.stable_step_id == RUNTIME_INVOCATION_LIFECYCLE_STEP_ID
                    && record.definition.logical_key == replay.invocation_id
                    && record.completed_output().is_none()
                    && record.latest_attempt().is_some_and(|attempt| {
                        attempt.status == crate::durability::ActivityAttemptStatus::Running
                    })
            });
            if !lifecycle_is_active {
                return Err(DurabilityError::InvalidEvent {
                    reason: format!(
                        "RunStopped audit requires an active lifecycle fence for invocation `{}`",
                        replay.invocation_id
                    ),
                }
                .into());
            }
            let terminal_audit_exists = candidate.projection().activities.values().any(|record| {
                (record.definition.stable_step_id == RUNTIME_RUN_STOPPED_AUDIT_STEP_ID
                    || record.definition.stable_step_id == RUNTIME_LEGACY_RUN_STOPPED_AUDIT_STEP_ID)
                    && record.definition.logical_key == RUN_STOPPED_AUDIT_LOGICAL_KEY
            });
            if terminal_audit_exists {
                return Err(DurableRunDriverError::TerminalRecoveryRequired {
                    stable_step_id: RUNTIME_RUN_STOPPED_AUDIT_STEP_ID.into(),
                    logical_key: RUN_STOPPED_AUDIT_LOGICAL_KEY.into(),
                });
            }
            let running_activity_ids = candidate
                .projection()
                .activities
                .values()
                .filter(|record| {
                    record.definition.stable_step_id != RUNTIME_INVOCATION_LIFECYCLE_STEP_ID
                        && record.latest_attempt().is_some_and(|attempt| {
                            attempt.status == crate::durability::ActivityAttemptStatus::Running
                        })
                })
                .map(|record| record.definition.activity_id.clone())
                .collect::<Vec<_>>();
            if !running_activity_ids.is_empty() {
                return Err(
                    DurableRunDriverError::TerminalAuditBlockedByRunningActivities {
                        activity_ids: running_activity_ids,
                    },
                );
            }
            candidate
                .prepare_activity(
                    RUNTIME_RUN_STOPPED_AUDIT_STEP_ID,
                    RUN_STOPPED_AUDIT_LOGICAL_KEY,
                    input,
                    SideEffectClass::ReconcileRequired,
                    None,
                )
                .map_err(DurableRunDriverError::from)
        })?;
        durable_activity_from_decision(decision)
    }

    /// Commit evidence that every configured fail-closed sink accepted `RunStopped`.
    #[cfg(test)]
    pub(crate) fn complete_run_stopped_audit(
        &self,
        activity_id: &str,
        attempt: u32,
        receipt: DurableRunStoppedReceipt,
    ) -> Result<(), DurableRunDriverError> {
        let snapshot = self.snapshot()?;
        let audit_record = snapshot.activity(activity_id)?.clone();
        let replay = replay_from_record(
            &audit_record,
            DurableRunStoppedAuditKind::Canonical,
            snapshot.run_id(),
        )?;
        if replay.terminal_receipt != receipt {
            return Err(DurableRunDriverError::InvalidCompletionPayload(
                "terminal receipt does not match its persisted replay envelope".into(),
            ));
        }
        let lifecycle = snapshot
            .projection()
            .activities
            .values()
            .find(|record| {
                record.definition.stable_step_id == RUNTIME_INVOCATION_LIFECYCLE_STEP_ID
                    && record.definition.logical_key == replay.invocation_id
                    && record.latest_attempt().is_some_and(|candidate| {
                        candidate.status == crate::durability::ActivityAttemptStatus::Running
                    })
            })
            .ok_or_else(|| DurabilityError::InvalidEvent {
                reason: "terminal audit test helper has no active lifecycle".into(),
            })?;
        let lifecycle_attempt = lifecycle
            .latest_attempt()
            .expect("running lifecycle attempt exists")
            .attempt;
        self.complete_run_stopped_audit_and_invocation_lifecycle(
            activity_id,
            attempt,
            &lifecycle.definition.activity_id,
            lifecycle_attempt,
            &replay,
        )
    }

    /// Atomically commit both the canonical `RunStopped` receipt and its invocation-lifecycle
    /// closure. A crash or unknown CAS outcome can therefore leave only two durable states:
    /// both open (reconcile) or both complete (safe terminal retry).
    pub(crate) fn complete_run_stopped_audit_and_invocation_lifecycle(
        &self,
        audit_activity_id: &str,
        audit_attempt: u32,
        lifecycle_activity_id: &str,
        lifecycle_attempt: u32,
        replay: &DurableRunStoppedAuditReplayEnvelope,
    ) -> Result<(), DurableRunDriverError> {
        let receipt = serde_json::to_value(&replay.terminal_receipt)
            .map_err(|error| DurableRunDriverError::InvalidCompletionPayload(error.to_string()))?;
        let lifecycle_output = lifecycle_closed_output(replay);
        for output in [&receipt, &lifecycle_output] {
            let actual_bytes = serde_json::to_vec(output)
                .map_err(|error| {
                    DurableRunDriverError::InvalidCompletionPayload(error.to_string())
                })?
                .len();
            if actual_bytes > self.payload_policy.max_completion_bytes {
                return Err(DurableRunDriverError::CompletionPayloadTooLarge {
                    actual_bytes,
                    max_bytes: self.payload_policy.max_completion_bytes,
                });
            }
        }
        self.transition(|candidate| {
            validate_replay_envelope(
                replay,
                DurableRunStoppedAuditKind::Canonical,
                candidate.run_id(),
                Some(&replay.invocation_id),
            )?;
            candidate.complete_activity(audit_activity_id, audit_attempt, receipt)?;
            candidate.complete_activity(
                lifecycle_activity_id,
                lifecycle_attempt,
                lifecycle_output,
            )?;
            Ok(())
        })
    }

    /// Atomically finish the run described by a persisted `RunStopped` receipt.
    ///
    /// A store can commit a CAS and still return an I/O error to its caller. Retrying an already
    /// applied terminal transition is therefore idempotent only when its status matches the
    /// receipt. A conflicting terminal status, pause, or reconciliation boundary fails closed.
    pub(crate) fn finalize_run_stopped_receipt(
        &self,
        receipt: &DurableRunStoppedReceipt,
    ) -> Result<(), DurableRunDriverError> {
        let expected = match receipt.reason.as_str() {
            "end_turn" | "stop" => DurableRunStatus::Completed,
            "approval_interrupted" | "cancelled" => DurableRunStatus::Cancelled,
            _ => DurableRunStatus::Failed,
        };
        self.transition(|candidate| {
            match candidate.status() {
                current if current == expected => return Ok(()),
                DurableRunStatus::Completed
                | DurableRunStatus::Failed
                | DurableRunStatus::Cancelled => {
                    return Err(DurabilityError::InvalidEvent {
                        reason: format!(
                            "persisted terminal receipt expects {expected:?}, but run is {:?}",
                            candidate.status()
                        ),
                    }
                    .into());
                }
                DurableRunStatus::ReconcileRequired => {
                    return Err(DurabilityError::RunRequiresReconciliation.into());
                }
                DurableRunStatus::Paused => {
                    return Err(DurabilityError::InvalidEvent {
                        reason: "paused run cannot be finalized from a RunStopped receipt".into(),
                    }
                    .into());
                }
                DurableRunStatus::Running => {}
            }

            match expected {
                DurableRunStatus::Completed => {
                    candidate.checkpoint("runtime-final", Some("runtime completed".into()))?;
                    candidate.complete_run("runtime")?;
                }
                DurableRunStatus::Failed => {
                    candidate.checkpoint("runtime-failed", Some("runtime failed".into()))?;
                    candidate.fail_run("runtime", receipt.reason.clone())?;
                }
                DurableRunStatus::Cancelled => {
                    candidate.checkpoint("runtime-cancelled", Some("runtime cancelled".into()))?;
                    candidate.apply_command(RunCommand::Cancel {
                        command_id: "runtime".into(),
                        reason: Some(receipt.reason.clone()),
                    })?;
                }
                DurableRunStatus::Running
                | DurableRunStatus::Paused
                | DurableRunStatus::ReconcileRequired => {
                    unreachable!("RunStopped reasons map only to terminal durable statuses")
                }
            }
            Ok(())
        })
    }

    /// Atomically preserve ambiguity for both a rejected canonical `RunStopped` delivery and the
    /// invocation lifecycle it was meant to close. Operators can then reconcile the linked pair
    /// in one explicit cycle instead of discovering a still-running fence on the next restart.
    pub(crate) fn fail_run_stopped_audit_and_invocation_lifecycle(
        &self,
        audit_activity_id: &str,
        audit_attempt: u32,
        lifecycle_activity_id: &str,
        lifecycle_attempt: u32,
    ) -> Result<(), DurableRunDriverError> {
        self.transition(|candidate| {
            candidate.fail_activity(
                audit_activity_id,
                audit_attempt,
                "fail-closed RunStopped audit delivery was not accepted by every sink",
                false,
                true,
            )?;
            candidate.fail_activity(
                lifecycle_activity_id,
                lifecycle_attempt,
                "RunStopped audit rejection left the invocation lifecycle ambiguous",
                false,
                true,
            )?;
            Ok(())
        })
    }

    /// Persist intent for the local `RunStopped` that closes an invocation superseded by an
    /// already-delivered terminal receipt.
    ///
    /// This second receipt is required because the earlier terminal delivery belongs to another
    /// invocation. A crash or partial sink failure while closing the currently-open `RunStarted`
    /// must block terminal CAS and blind retry just like the canonical terminal audit itself.
    pub(crate) fn begin_recovery_run_stopped_audit(
        &self,
        replay: &DurableRunStoppedAuditReplayEnvelope,
    ) -> Result<DurableActivity, DurableRunDriverError> {
        let expected_output = serde_json::json!({"accepted": true});
        let input = terminal_audit_input(replay, &expected_output)?;
        let decision = self.transition(|candidate| {
            validate_replay_envelope(
                replay,
                DurableRunStoppedAuditKind::Recovery,
                candidate.run_id(),
                Some(&replay.invocation_id),
            )?;
            let terminal_receipt = candidate
                .projection()
                .activities
                .values()
                .find(|record| {
                    record.definition.stable_step_id == RUNTIME_RUN_STOPPED_AUDIT_STEP_ID
                        && record.definition.logical_key == RUN_STOPPED_AUDIT_LOGICAL_KEY
                        && record.completed_output().is_some()
                })
                .ok_or_else(|| DurabilityError::InvalidEvent {
                    reason: "recovery RunStopped requires a completed terminal receipt".into(),
                })?;
            let completed_output = validated_canonical_receipt_output(terminal_receipt)?
                .ok_or_else(|| {
                    DurableRunDriverError::InvalidCompletionPayload(
                        "completed terminal receipt output is missing".into(),
                    )
                })?;
            let completed_receipt: DurableRunStoppedReceipt =
                serde_json::from_value(completed_output).map_err(|error| {
                    DurableRunDriverError::InvalidCompletionPayload(format!(
                        "persisted RunStopped receipt is invalid: {error}"
                    ))
                })?;
            if completed_receipt != replay.terminal_receipt {
                return Err(DurableRunDriverError::InvalidCompletionPayload(
                    "recovery replay terminal receipt does not match the canonical receipt".into(),
                ));
            }
            if terminal_receipt
                .definition
                .input
                .get("legacy_resolution")
                .is_none()
            {
                let canonical_replay = replay_from_record(
                    terminal_receipt,
                    DurableRunStoppedAuditKind::Canonical,
                    candidate.run_id(),
                )?;
                if canonical_replay.invocation_id == replay.invocation_id {
                    return Err(DurableRunDriverError::InvalidCompletionPayload(
                        "recovery RunStopped cannot duplicate its canonical invocation closure"
                            .into(),
                    ));
                }
            }
            candidate
                .prepare_activity(
                    RUNTIME_RECOVERY_RUN_STOPPED_AUDIT_STEP_ID,
                    &replay.invocation_id,
                    input,
                    SideEffectClass::ReconcileRequired,
                    None,
                )
                .map_err(DurableRunDriverError::from)
        })?;
        durable_activity_from_decision(decision)
    }

    /// Atomically close the recovery delivery marker and the invocation lifecycle it belongs to.
    pub(crate) fn complete_recovery_run_stopped_audit_and_invocation_lifecycle(
        &self,
        recovery_activity_id: &str,
        recovery_attempt: u32,
        lifecycle_activity_id: &str,
        lifecycle_attempt: u32,
        replay: &DurableRunStoppedAuditReplayEnvelope,
    ) -> Result<(), DurableRunDriverError> {
        self.transition(|candidate| {
            validate_replay_envelope(
                replay,
                DurableRunStoppedAuditKind::Recovery,
                candidate.run_id(),
                Some(&replay.invocation_id),
            )?;
            candidate.complete_activity(
                recovery_activity_id,
                recovery_attempt,
                serde_json::json!({"accepted": true}),
            )?;
            candidate.complete_activity(
                lifecycle_activity_id,
                lifecycle_attempt,
                lifecycle_closed_output(replay),
            )?;
            Ok(())
        })
    }

    /// Atomically preserve ambiguity for a rejected recovery delivery and its invocation fence.
    pub(crate) fn fail_recovery_run_stopped_audit_and_invocation_lifecycle(
        &self,
        recovery_activity_id: &str,
        recovery_attempt: u32,
        lifecycle_activity_id: &str,
        lifecycle_attempt: u32,
    ) -> Result<(), DurableRunDriverError> {
        self.transition(|candidate| {
            candidate.fail_activity(
                recovery_activity_id,
                recovery_attempt,
                "fail-closed recovery RunStopped audit delivery was not accepted by every sink",
                false,
                true,
            )?;
            candidate.fail_activity(
                lifecycle_activity_id,
                lifecycle_attempt,
                "recovery RunStopped rejection left the invocation lifecycle ambiguous",
                false,
                true,
            )?;
            Ok(())
        })
    }

    /// Start only the terminal audit attempt explicitly authorized as SafeToRetry by an operator.
    /// No invocation lifecycle fence is reopened because no new `RunStarted`, provider, or tool
    /// effect is permitted on this path.
    pub(crate) fn begin_terminal_audit_retry(
        &self,
        replay: &DurableRunStoppedAuditReplayEnvelope,
    ) -> Result<DurableTerminalAuditRetryAttempt, DurableRunDriverError> {
        self.transition(|candidate| {
            if replay.audit_binding.delivery_id.is_none()
                || replay.audit_binding.sink_count == 0
                || replay.audit_binding.failure_mode
                    != crate::observability::AuditFailureMode::FailClosed
            {
                return Err(DurableRunDriverError::InvalidCompletionPayload(
                    "terminal audit SafeToRetry requires a stable durable replay delivery ID"
                        .into(),
                ));
            }
            let stable_step_id = match replay.kind {
                DurableRunStoppedAuditKind::Canonical => RUNTIME_RUN_STOPPED_AUDIT_STEP_ID,
                DurableRunStoppedAuditKind::Recovery => RUNTIME_RECOVERY_RUN_STOPPED_AUDIT_STEP_ID,
                DurableRunStoppedAuditKind::Direct => {
                    return Err(DurableRunDriverError::InvalidCompletionPayload(
                        "direct invocation lifecycle delivery is completion-only".into(),
                    ));
                }
            };
            let logical_key = match replay.kind {
                DurableRunStoppedAuditKind::Canonical => RUN_STOPPED_AUDIT_LOGICAL_KEY,
                DurableRunStoppedAuditKind::Recovery => replay.invocation_id.as_str(),
                DurableRunStoppedAuditKind::Direct => unreachable!("handled above"),
            };
            validate_replay_envelope(
                replay,
                replay.kind,
                candidate.run_id(),
                Some(&replay.invocation_id),
            )?;
            let record = candidate
                .projection()
                .activities
                .values()
                .find(|record| {
                    record.definition.stable_step_id == stable_step_id
                        && record.definition.logical_key == logical_key
                })
                .cloned()
                .ok_or_else(|| DurabilityError::InvalidEvent {
                    reason: "terminal audit retry activity was not found".into(),
                })?;
            let persisted_replay = replay_from_record(&record, replay.kind, candidate.run_id())?;
            if persisted_replay != *replay {
                return Err(DurableRunDriverError::InvalidCompletionPayload(
                    "terminal audit retry does not match the persisted replay envelope".into(),
                ));
            }
            let retry_is_authorized = record.latest_attempt().is_some_and(|attempt| {
                attempt.status == crate::durability::ActivityAttemptStatus::Failed
                    && attempt.retryable
                    && !attempt.effect_ambiguous
            });
            if !retry_is_authorized {
                return Err(DurabilityError::InvalidEvent {
                    reason: "terminal audit retry requires an explicit SafeToRetry reconciliation"
                        .into(),
                }
                .into());
            }
            let running_activity_ids = candidate
                .projection()
                .activities
                .values()
                .filter(|candidate_record| {
                    candidate_record.latest_attempt().is_some_and(|attempt| {
                        attempt.status == crate::durability::ActivityAttemptStatus::Running
                    })
                })
                .map(|candidate_record| candidate_record.definition.activity_id.clone())
                .collect::<Vec<_>>();
            if !running_activity_ids.is_empty() {
                return Err(
                    DurableRunDriverError::TerminalAuditBlockedByRunningActivities {
                        activity_ids: running_activity_ids,
                    },
                );
            }
            let decision = candidate.prepare_activity(
                &record.definition.stable_step_id,
                &record.definition.logical_key,
                record.definition.input,
                record.definition.side_effect_class,
                record.definition.idempotency_key,
            )?;
            let ActivityDecision::Execute {
                activity_id,
                attempt,
                ..
            } = decision
            else {
                return Err(DurabilityError::InvalidEvent {
                    reason: "SafeToRetry terminal audit did not start a new attempt".into(),
                }
                .into());
            };
            Ok(DurableTerminalAuditRetryAttempt {
                kind: replay.kind,
                activity_id,
                attempt,
                replay: replay.clone(),
            })
        })
    }

    pub(crate) fn complete_terminal_audit_retry(
        &self,
        retry: &DurableTerminalAuditRetryAttempt,
    ) -> Result<(), DurableRunDriverError> {
        let output = match retry.kind {
            DurableRunStoppedAuditKind::Canonical => {
                serde_json::to_value(&retry.replay.terminal_receipt).map_err(|error| {
                    DurableRunDriverError::InvalidCompletionPayload(error.to_string())
                })?
            }
            DurableRunStoppedAuditKind::Recovery => serde_json::json!({"accepted": true}),
            DurableRunStoppedAuditKind::Direct => {
                return Err(DurableRunDriverError::InvalidCompletionPayload(
                    "direct invocation lifecycle delivery is completion-only".into(),
                ));
            }
        };
        self.complete_activity(&retry.activity_id, retry.attempt, output)
    }

    pub(crate) fn fail_terminal_audit_retry(
        &self,
        retry: &DurableTerminalAuditRetryAttempt,
    ) -> Result<(), DurableRunDriverError> {
        self.fail_activity(
            &retry.activity_id,
            retry.attempt,
            "reconciled RunStopped retry was not accepted by every audit sink",
            false,
            true,
        )
        .map(|_| ())
    }

    /// Commit the terminal successful state after all runtime side effects have completed.
    pub fn complete_run(&self) -> Result<(), DurableRunDriverError> {
        self.transition(|candidate| {
            candidate.checkpoint("runtime-final", Some("runtime completed".into()))?;
            candidate.complete_run("runtime")?;
            Ok(())
        })
    }

    /// Commit a terminal failure unless reconciliation must remain possible.
    pub fn fail_run(&self, error: impl Into<String>) -> Result<(), DurableRunDriverError> {
        let error = error.into();
        self.transition(|candidate| {
            if candidate.status() == DurableRunStatus::ReconcileRequired
                || candidate.status().is_terminal()
            {
                return Ok(());
            }
            candidate.checkpoint("runtime-failed", Some("runtime failed".into()))?;
            candidate.fail_run("runtime", error)?;
            Ok(())
        })
    }

    /// Commit an unambiguous cooperative cancellation unless reconciliation takes precedence.
    pub fn cancel_run(&self, reason: impl Into<String>) -> Result<(), DurableRunDriverError> {
        let reason = reason.into();
        self.transition(|candidate| {
            if candidate.status() == DurableRunStatus::ReconcileRequired
                || candidate.status().is_terminal()
            {
                return Ok(());
            }
            candidate.checkpoint("runtime-cancelled", Some("runtime cancelled".into()))?;
            candidate.apply_command(RunCommand::Cancel {
                command_id: "runtime".into(),
                reason: Some(reason),
            })?;
            Ok(())
        })
    }

    /// Resolve a migrated v1 RunStopped receipt with explicit terminal metadata.
    ///
    /// This is completion-only: the bridge is bound to the exact legacy activity and accepted
    /// output hash, and cannot authorize another external audit delivery.
    pub fn reconcile_legacy_run_stopped_audit(
        &self,
        reconciliation_id: &str,
        activity_id: &str,
        turns: usize,
        reason: impl Into<String>,
        usage: crate::types::Usage,
    ) -> Result<(), DurableRunDriverError> {
        let reason = reason.into();
        if reason.is_empty() {
            return Err(DurableRunDriverError::InvalidCompletionPayload(
                "legacy RunStopped terminal reason cannot be empty".into(),
            ));
        }
        self.transition(|candidate| {
            let record = candidate.activity(activity_id)?.clone();
            let output = legacy_resolution_output(
                &record,
                DurableRunStoppedReceipt {
                    turns,
                    reason,
                    usage,
                },
            )?;
            candidate.reconcile_activity(
                reconciliation_id,
                activity_id,
                crate::durability::ActivityReconciliation::Completed { output },
            )?;
            Ok(())
        })
    }

    fn transition<T>(
        &self,
        mutate: impl FnOnce(&mut RunState) -> Result<T, DurableRunDriverError>,
    ) -> Result<T, DurableRunDriverError> {
        self.transition_inner(true, mutate)
    }

    fn transition_inner<T>(
        &self,
        require_worker_authority: bool,
        mutate: impl FnOnce(&mut RunState) -> Result<T, DurableRunDriverError>,
    ) -> Result<T, DurableRunDriverError> {
        if self.is_poisoned() {
            return Err(DurableRunDriverError::Poisoned);
        }
        let mut current = self.lock_state()?;
        if self.is_poisoned() {
            return Err(DurableRunDriverError::Poisoned);
        }
        match self.store.load(current.run_id()) {
            Ok(persisted) if persisted == *current => {}
            Ok(_) => {
                self.poisoned.store(true, Ordering::Release);
                return Err(DurableRunDriverError::StateMismatch {
                    run_id: current.run_id().into(),
                });
            }
            Err(error) => {
                self.poisoned.store(true, Ordering::Release);
                return Err(error.into());
            }
        }
        if require_worker_authority {
            self.ensure_worker_lease_authority(&current)?;
        }
        let expected_sequence = last_sequence(&current);
        let mut candidate = current.clone();
        let result = mutate(&mut candidate)?;
        if candidate != *current {
            let persisted = match self.worker_lease.as_ref() {
                Some(authority) => {
                    self.store
                        .compare_and_swap_fenced(expected_sequence, &candidate, authority)
                }
                None => self.store.compare_and_swap(expected_sequence, &candidate),
            };
            if let Err(error) = persisted {
                self.poisoned.store(true, Ordering::Release);
                return Err(error.into());
            }
            *current = candidate;
        }
        Ok(result)
    }

    fn ensure_worker_lease_authority(
        &self,
        current: &RunState,
    ) -> Result<(), DurableRunDriverError> {
        match (current.worker_lease(), self.worker_lease.as_ref()) {
            (None, None) => Ok(()),
            (Some(lease), None) => Err(DurableRunDriverError::WorkerLeaseRequired {
                owner_id: lease.owner_id.clone(),
            }),
            (None, Some(binding)) => Err(DurabilityError::WorkerLeaseLost {
                owner_id: binding.owner_id().to_string(),
            }
            .into()),
            (Some(lease), Some(binding))
                if lease.owner_id == binding.owner_id() && lease.lease_id == binding.lease_id() =>
            {
                if lease.expires_at_unix_ms > self.store.worker_lease_clock_unix_ms()? {
                    Ok(())
                } else {
                    Err(DurabilityError::WorkerLeaseLost {
                        owner_id: binding.owner_id().to_string(),
                    }
                    .into())
                }
            }
            (Some(_), Some(binding)) => Err(DurabilityError::WorkerLeaseLost {
                owner_id: binding.owner_id().to_string(),
            }
            .into()),
        }
    }

    fn lock_state(&self) -> Result<std::sync::MutexGuard<'_, RunState>, DurableRunDriverError> {
        self.state
            .lock()
            .map_err(|_| DurableRunDriverError::StateLock)
    }
}

fn last_sequence(state: &RunState) -> u64 {
    state.events().last().map_or(0, |event| event.sequence)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::durable_store::InMemoryDurableStore;
    use serde_json::json;
    use std::sync::atomic::AtomicUsize;

    struct FailFirstCasStore {
        inner: InMemoryDurableStore,
        calls: AtomicUsize,
    }

    impl Default for FailFirstCasStore {
        fn default() -> Self {
            Self {
                inner: InMemoryDurableStore::default(),
                calls: AtomicUsize::new(0),
            }
        }
    }

    impl DurableStore for FailFirstCasStore {
        fn create(&self, state: &RunState) -> Result<(), DurableStoreError> {
            self.inner.create(state)
        }

        fn load(&self, run_id: &str) -> Result<RunState, DurableStoreError> {
            self.inner.load(run_id)
        }

        fn compare_and_swap(
            &self,
            expected_sequence: u64,
            replacement: &RunState,
        ) -> Result<(), DurableStoreError> {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(DurableStoreError::Io("planned CAS failure".into()));
            }
            self.inner.compare_and_swap(expected_sequence, replacement)
        }
    }

    #[derive(Default)]
    struct LegacyCompatibleStore {
        inner: InMemoryDurableStore,
    }

    impl DurableStore for LegacyCompatibleStore {
        fn create(&self, state: &RunState) -> Result<(), DurableStoreError> {
            self.inner.create(state)
        }

        fn load(&self, run_id: &str) -> Result<RunState, DurableStoreError> {
            self.inner.load(run_id)
        }

        fn compare_and_swap(
            &self,
            expected_sequence: u64,
            replacement: &RunState,
        ) -> Result<(), DurableStoreError> {
            self.inner.compare_and_swap(expected_sequence, replacement)
        }
    }

    fn completed_legacy_v1_state(run_id: &str) -> RunState {
        let legacy_activity_id = crate::durability::stable_id(
            "activity",
            &[
                run_id,
                "root",
                RUNTIME_LEGACY_RUN_STOPPED_AUDIT_STEP_ID,
                RUN_STOPPED_AUDIT_LOGICAL_KEY,
            ],
        );
        let legacy_output = json!({"accepted": true});
        let legacy_definition = crate::durability::ActivityDefinition {
            activity_id: legacy_activity_id.clone(),
            stable_step_id: RUNTIME_LEGACY_RUN_STOPPED_AUDIT_STEP_ID.into(),
            logical_key: RUN_STOPPED_AUDIT_LOGICAL_KEY.into(),
            input: json!({"input_hash": "legacy"}),
            input_hash: stable_input_hash(&json!({"input_hash": "legacy"})),
            side_effect_class: SideEffectClass::ReconcileRequired,
            idempotency_key: None,
        };
        RunState::from_events(vec![
            crate::durability::RunEvent {
                schema_version: 1,
                run_id: run_id.into(),
                sequence: 1,
                event_id: "legacy-start".into(),
                kind: crate::durability::RunEventKind::RunStarted {
                    session_id: "session".into(),
                    durability: DurabilityMode::Sync,
                    root_branch_id: "root".into(),
                },
            },
            crate::durability::RunEvent {
                schema_version: 1,
                run_id: run_id.into(),
                sequence: 2,
                event_id: "legacy-scheduled".into(),
                kind: crate::durability::RunEventKind::ActivityScheduled {
                    definition: legacy_definition,
                },
            },
            crate::durability::RunEvent {
                schema_version: 1,
                run_id: run_id.into(),
                sequence: 3,
                event_id: "legacy-started".into(),
                kind: crate::durability::RunEventKind::ActivityAttemptStarted {
                    activity_id: legacy_activity_id.clone(),
                    attempt: 1,
                },
            },
            crate::durability::RunEvent {
                schema_version: 1,
                run_id: run_id.into(),
                sequence: 4,
                event_id: "legacy-completed".into(),
                kind: crate::durability::RunEventKind::ActivityAttemptCompleted {
                    activity_id: legacy_activity_id,
                    attempt: 1,
                    output: legacy_output.clone(),
                    output_hash: stable_input_hash(&legacy_output),
                },
            },
        ])
        .unwrap()
    }

    struct RunningTerminalAuditPair {
        driver: DurableRunDriver,
        replay: DurableRunStoppedAuditReplayEnvelope,
        lifecycle_activity_id: String,
        lifecycle_attempt: u32,
        audit_activity_id: String,
        audit_attempt: u32,
    }

    fn executed_activity(activity: DurableActivity) -> (String, u32) {
        match activity {
            DurableActivity::Execute {
                activity_id,
                attempt,
                ..
            } => (activity_id, attempt),
            DurableActivity::ReuseCompleted { .. } => {
                panic!("test activity must start a fresh attempt")
            }
        }
    }

    fn fail_closed_test_replay(
        kind: DurableRunStoppedAuditKind,
        run_id: &str,
        invocation_id: &str,
        receipt: DurableRunStoppedReceipt,
    ) -> DurableRunStoppedAuditReplayEnvelope {
        let audit_reason = match kind {
            DurableRunStoppedAuditKind::Recovery => {
                "superseded_by_durable_terminal_receipt".to_string()
            }
            DurableRunStoppedAuditKind::Canonical | DurableRunStoppedAuditKind::Direct => {
                receipt.reason.clone()
            }
        };
        DurableRunStoppedAuditReplayEnvelope::new(
            kind,
            receipt.clone(),
            run_id,
            invocation_id,
            1,
            receipt.turns,
            audit_reason,
            crate::observability::AuditReplayBinding {
                schema_version: 1,
                delivery_id: Some(format!("test-{run_id}-{invocation_id}-{kind:?}")),
                sink_count: 1,
                payload_policy: crate::observability::AuditPayloadPolicy::MetadataOnly,
                failure_mode: crate::observability::AuditFailureMode::FailClosed,
                max_preview_bytes: 4096,
            },
        )
    }

    fn start_lifecycle_after_terminal_receipt(
        driver: &DurableRunDriver,
        invocation_id: &str,
    ) -> (String, u32) {
        let lifecycle = driver
            .transition(|candidate| {
                let audit_run_id = candidate.run_id().to_string();
                let decision = candidate.prepare_activity(
                    RUNTIME_INVOCATION_LIFECYCLE_STEP_ID,
                    invocation_id,
                    json!({
                        "schema_version": RUN_STOPPED_REPLAY_SCHEMA_VERSION,
                        "audit_run_id": audit_run_id,
                        "invocation_id": invocation_id,
                    }),
                    SideEffectClass::ReconcileRequired,
                    None,
                )?;
                durable_activity_from_decision(decision)
            })
            .unwrap();
        executed_activity(lifecycle)
    }

    fn complete_test_canonical_source(
        driver: &DurableRunDriver,
        receipt: &DurableRunStoppedReceipt,
    ) {
        let invocation_id = "canonical-source-invocation";
        let (lifecycle_activity_id, lifecycle_attempt) =
            executed_activity(driver.begin_invocation_lifecycle(invocation_id).unwrap());
        let replay = fail_closed_test_replay(
            DurableRunStoppedAuditKind::Canonical,
            &driver.run_id().unwrap(),
            invocation_id,
            receipt.clone(),
        );
        let (audit_activity_id, audit_attempt) =
            executed_activity(driver.begin_invocation_run_stopped_audit(&replay).unwrap());
        driver
            .complete_run_stopped_audit_and_invocation_lifecycle(
                &audit_activity_id,
                audit_attempt,
                &lifecycle_activity_id,
                lifecycle_attempt,
                &replay,
            )
            .unwrap();
    }

    fn running_terminal_audit_pair(
        kind: DurableRunStoppedAuditKind,
        run_id: &str,
    ) -> RunningTerminalAuditPair {
        let receipt = DurableRunStoppedReceipt {
            turns: 2,
            reason: "end_turn".into(),
            usage: crate::types::Usage::default(),
        };
        let driver = DurableRunDriver::new(
            RunState::new("session", run_id, DurabilityMode::Sync).unwrap(),
            Arc::new(InMemoryDurableStore::default()),
        )
        .unwrap();
        if kind == DurableRunStoppedAuditKind::Recovery {
            complete_test_canonical_source(&driver, &receipt);
        }

        let invocation_id = match kind {
            DurableRunStoppedAuditKind::Canonical => "canonical-invocation",
            DurableRunStoppedAuditKind::Recovery => "recovery-invocation",
            DurableRunStoppedAuditKind::Direct => panic!("direct audit has no terminal marker"),
        };
        let (lifecycle_activity_id, lifecycle_attempt) =
            if kind == DurableRunStoppedAuditKind::Canonical {
                executed_activity(driver.begin_invocation_lifecycle(invocation_id).unwrap())
            } else {
                start_lifecycle_after_terminal_receipt(&driver, invocation_id)
            };
        let replay = fail_closed_test_replay(kind, run_id, invocation_id, receipt);
        let (audit_activity_id, audit_attempt) = match kind {
            DurableRunStoppedAuditKind::Canonical => {
                executed_activity(driver.begin_invocation_run_stopped_audit(&replay).unwrap())
            }
            DurableRunStoppedAuditKind::Recovery => {
                executed_activity(driver.begin_recovery_run_stopped_audit(&replay).unwrap())
            }
            DurableRunStoppedAuditKind::Direct => unreachable!("handled above"),
        };
        RunningTerminalAuditPair {
            driver,
            replay,
            lifecycle_activity_id,
            lifecycle_attempt,
            audit_activity_id,
            audit_attempt,
        }
    }

    fn reconciliation_ids(state: &RunState) -> std::collections::BTreeSet<String> {
        state
            .projection()
            .activities
            .values()
            .filter(|record| {
                record.latest_attempt().is_some_and(|attempt| {
                    attempt.status == crate::durability::ActivityAttemptStatus::ReconcileRequired
                })
            })
            .map(|record| record.definition.activity_id.clone())
            .collect()
    }

    #[test]
    fn ordinary_unleased_transition_does_not_require_the_worker_clock_extension() {
        let store = Arc::new(LegacyCompatibleStore::default());
        let state = RunState::new("session", "legacy-unleased-run", DurabilityMode::Sync).unwrap();
        let driver = DurableRunDriver::new(state, store).unwrap();

        assert!(matches!(
            driver
                .begin_activity(
                    "pure-step",
                    "legacy-store",
                    json!({}),
                    SideEffectClass::Pure,
                    None,
                )
                .unwrap(),
            DurableActivity::Execute { .. }
        ));
    }

    #[test]
    fn completed_activity_is_reused_without_a_new_attempt() {
        let store = Arc::new(InMemoryDurableStore::default());
        let state = RunState::new("session", "reuse-run", DurabilityMode::Sync).unwrap();
        let driver = DurableRunDriver::new(state, store).unwrap();
        let started = driver
            .begin_activity(
                "provider-v1",
                "turn-1",
                json!({"prompt": "hello"}),
                SideEffectClass::ReconcileRequired,
                None,
            )
            .unwrap();
        let DurableActivity::Execute {
            activity_id,
            attempt,
            ..
        } = started
        else {
            panic!("fresh activity must execute");
        };
        driver
            .complete_activity(&activity_id, attempt, json!({"answer": "hello"}))
            .unwrap();

        assert_eq!(
            driver
                .begin_activity(
                    "provider-v1",
                    "turn-1",
                    json!({"prompt": "hello"}),
                    SideEffectClass::ReconcileRequired,
                    None,
                )
                .unwrap(),
            DurableActivity::ReuseCompleted {
                activity_id,
                output: json!({"answer": "hello"}),
            }
        );
    }

    #[test]
    fn terminal_receipt_and_nonterminal_activity_claims_are_mutually_exclusive() {
        let store = Arc::new(InMemoryDurableStore::default());
        let state = RunState::new("session", "terminal-boundary", DurabilityMode::Sync).unwrap();
        let driver = DurableRunDriver::new(state, store).unwrap();
        let DurableActivity::Execute {
            activity_id,
            attempt,
            ..
        } = driver
            .begin_activity(
                "provider-v1",
                "turn-1",
                json!({"input_hash": "provider"}),
                SideEffectClass::ReconcileRequired,
                None,
            )
            .unwrap()
        else {
            panic!("fresh provider activity must execute");
        };
        let receipt = DurableRunStoppedReceipt {
            turns: 1,
            reason: "end_turn".into(),
            usage: crate::types::Usage::default(),
        };

        assert!(matches!(
            driver.begin_run_stopped_audit(&receipt),
            Err(DurableRunDriverError::TerminalAuditBlockedByRunningActivities { .. })
        ));

        driver
            .complete_activity(&activity_id, attempt, json!([]))
            .unwrap();
        let DurableActivity::Execute {
            activity_id,
            attempt,
            ..
        } = driver.begin_run_stopped_audit(&receipt).unwrap()
        else {
            panic!("terminal audit must start after provider completion");
        };
        driver
            .complete_run_stopped_audit(&activity_id, attempt, receipt)
            .unwrap();

        assert!(matches!(
            driver.begin_activity(
                "provider-v1",
                "turn-2",
                json!({"input_hash": "provider-2"}),
                SideEffectClass::ReconcileRequired,
                None,
            ),
            Err(DurableRunDriverError::TerminalRecoveryRequired { .. })
        ));
    }

    #[test]
    fn unsafe_running_activity_requires_reconciliation_after_restart() {
        let store = Arc::new(InMemoryDurableStore::default());
        let state = RunState::new("session", "ambiguous-run", DurabilityMode::Sync).unwrap();
        let first = DurableRunDriver::new(state, store.clone()).unwrap();
        first
            .begin_activity(
                "tool-v1",
                "turn-1-call-1",
                json!({"amount": 10}),
                SideEffectClass::ReconcileRequired,
                None,
            )
            .unwrap();
        let restarted_state = store.load("ambiguous-run").unwrap();
        let restarted = DurableRunDriver::new(restarted_state, store.clone()).unwrap();

        assert!(matches!(
            restarted.begin_activity(
                "tool-v1",
                "turn-1-call-1",
                json!({"amount": 10}),
                SideEffectClass::ReconcileRequired,
                None,
            ),
            Err(DurableRunDriverError::ReconciliationRequired { .. })
        ));
        assert_eq!(
            store.load("ambiguous-run").unwrap().status(),
            DurableRunStatus::ReconcileRequired
        );
    }

    #[test]
    fn async_and_exit_modes_fail_closed() {
        for mode in [DurabilityMode::Async, DurabilityMode::Exit] {
            let state = RunState::new("session", format!("{mode:?}"), mode).unwrap();
            let result = DurableRunDriver::new(state, Arc::new(InMemoryDurableStore::default()));
            assert!(matches!(
                result,
                Err(DurableRunDriverError::UnsupportedMode { mode: actual }) if actual == mode
            ));
        }
    }

    #[test]
    fn any_cas_failure_poisons_driver_and_blocks_later_writes() {
        let store = Arc::new(FailFirstCasStore::default());
        let state = RunState::new("session", "poisoned-run", DurabilityMode::Sync).unwrap();
        let driver = DurableRunDriver::new(state, store.clone()).unwrap();

        assert!(matches!(
            driver.begin_activity(
                "provider-v1",
                "turn-1",
                json!({"input_hash": "hash"}),
                SideEffectClass::ReconcileRequired,
                None,
            ),
            Err(DurableRunDriverError::Store(DurableStoreError::Io(_)))
        ));
        assert!(driver.is_poisoned());
        assert!(matches!(
            driver.begin_activity(
                "provider-v1",
                "turn-2",
                json!({"input_hash": "other"}),
                SideEffectClass::ReconcileRequired,
                None,
            ),
            Err(DurableRunDriverError::Poisoned)
        ));
        assert_eq!(store.calls.load(Ordering::SeqCst), 1);
        assert_eq!(store.load("poisoned-run").unwrap().events().len(), 1);
    }

    #[test]
    fn completion_payload_policy_rejects_oversized_results_before_cas() {
        assert!(DurablePayloadPolicy::new(0).is_err());
        assert!(DurablePayloadPolicy::new(MAX_DURABLE_COMPLETION_BYTES + 1).is_err());

        let store = Arc::new(InMemoryDurableStore::default());
        let state = RunState::new("session", "payload-limit", DurabilityMode::Sync).unwrap();
        let driver = DurableRunDriver::new_with_payload_policy(
            state,
            store.clone(),
            DurablePayloadPolicy::new(32).unwrap(),
        )
        .unwrap();
        let DurableActivity::Execute {
            activity_id,
            attempt,
            ..
        } = driver
            .begin_activity(
                "provider-v1",
                "turn-1",
                json!({"input_hash": "hash"}),
                SideEffectClass::ReconcileRequired,
                None,
            )
            .unwrap()
        else {
            panic!("fresh activity must execute");
        };
        assert!(matches!(
            driver.complete_activity(&activity_id, attempt, json!("x".repeat(64))),
            Err(DurableRunDriverError::CompletionPayloadTooLarge { .. })
        ));
        assert!(!driver.is_poisoned());
        let stored = store.load("payload-limit").unwrap();
        assert_eq!(stored.status(), DurableRunStatus::ReconcileRequired);
        let serialized = serde_json::to_string(&stored).unwrap();
        assert!(!serialized.contains(&"x".repeat(64)));
        assert!(serialized.contains("completion size policy"));
    }

    #[test]
    fn activity_completion_does_not_create_quadratic_checkpoints() {
        let store = Arc::new(InMemoryDurableStore::default());
        let state = RunState::new("session", "sparse-checkpoints", DurabilityMode::Sync).unwrap();
        let driver = DurableRunDriver::new(state, store).unwrap();

        for index in 0..16 {
            let DurableActivity::Execute {
                activity_id,
                attempt,
                ..
            } = driver
                .begin_activity(
                    "provider-v1",
                    &format!("turn-{index}"),
                    json!({"input_hash": format!("hash-{index}")}),
                    SideEffectClass::ReconcileRequired,
                    None,
                )
                .unwrap()
            else {
                panic!("fresh activity must execute");
            };
            driver
                .complete_activity(&activity_id, attempt, json!({"turn": index}))
                .unwrap();
        }
        driver.complete_run().unwrap();

        let snapshot = driver.snapshot().unwrap();
        assert_eq!(snapshot.checkpoints().len(), 1);
        assert_eq!(snapshot.status(), DurableRunStatus::Completed);
    }

    #[test]
    fn run_stopped_receipt_finalization_is_typed_and_idempotent() {
        for (index, reason, expected) in [
            (0, "end_turn", DurableRunStatus::Completed),
            (1, "provider_error", DurableRunStatus::Failed),
            (2, "cancelled", DurableRunStatus::Cancelled),
        ] {
            let run_id = format!("receipt-finalization-{index}");
            let store = Arc::new(InMemoryDurableStore::default());
            let state = RunState::new("session", &run_id, DurabilityMode::Sync).unwrap();
            let driver = DurableRunDriver::new(state, store.clone()).unwrap();
            let receipt = DurableRunStoppedReceipt {
                turns: 1,
                reason: reason.into(),
                usage: crate::types::Usage::default(),
            };
            let DurableActivity::Execute {
                activity_id,
                attempt,
                ..
            } = driver.begin_run_stopped_audit(&receipt).unwrap()
            else {
                panic!("fresh terminal audit intent must execute");
            };
            driver
                .complete_run_stopped_audit(&activity_id, attempt, receipt.clone())
                .unwrap();

            driver.finalize_run_stopped_receipt(&receipt).unwrap();
            driver.finalize_run_stopped_receipt(&receipt).unwrap();
            assert_eq!(store.load(&run_id).unwrap().status(), expected);
        }
    }

    #[test]
    fn migrated_v1_completed_receipt_requires_typed_bridge_before_terminal_finalization() {
        let run_id = "legacy-v1-bridge-e2e";
        let state = completed_legacy_v1_state(run_id);
        let store = Arc::new(InMemoryDurableStore::default());
        let driver = DurableRunDriver::new(state, store.clone()).unwrap();

        assert!(matches!(
            driver.invocation_disposition().unwrap(),
            DurableInvocationDisposition::ReconcileRequired { .. }
        ));
        let bridge_id = driver
            .snapshot()
            .unwrap()
            .projection()
            .activities
            .values()
            .find(|record| record.definition.input.get("legacy_resolution").is_some())
            .unwrap()
            .definition
            .activity_id
            .clone();
        let before = driver.snapshot().unwrap();
        for resolution in [
            crate::durability::ActivityReconciliation::SafeToRetry,
            crate::durability::ActivityReconciliation::Cancelled,
            crate::durability::ActivityReconciliation::Completed {
                output: json!({"malformed": true}),
            },
        ] {
            assert!(driver
                .transition(|candidate| {
                    candidate.reconcile_activity(
                        "invalid-legacy-bridge",
                        &bridge_id,
                        resolution,
                    )?;
                    Ok(())
                })
                .is_err());
            assert_eq!(driver.snapshot().unwrap(), before);
        }

        driver
            .reconcile_legacy_run_stopped_audit(
                "typed-legacy-bridge",
                &bridge_id,
                2,
                "end_turn",
                crate::types::Usage::default(),
            )
            .unwrap();
        assert_eq!(
            driver.snapshot().unwrap().status(),
            DurableRunStatus::Paused
        );
        driver
            .transition(|candidate| {
                candidate.apply_command(RunCommand::Resume {
                    command_id: "resume-typed-legacy-bridge".into(),
                    approvals: vec![],
                })?;
                Ok(())
            })
            .unwrap();
        let receipt = match driver.invocation_disposition().unwrap() {
            DurableInvocationDisposition::FinalizeTerminal(receipt) => receipt,
            other => panic!("typed bridge must permit only terminal finalization, got {other:?}"),
        };
        driver.finalize_run_stopped_receipt(&receipt).unwrap();
        assert_eq!(
            store.load(run_id).unwrap().status(),
            DurableRunStatus::Completed
        );
    }

    #[test]
    fn recovery_audit_accepts_completed_legacy_resolution_bridge_and_closes_active_lifecycle() {
        let run_id = "legacy-v1-recovery-bridge";
        let invocation_id = "recovery-invocation";
        let receipt = DurableRunStoppedReceipt {
            turns: 2,
            reason: "end_turn".into(),
            usage: crate::types::Usage::default(),
        };
        let store = Arc::new(InMemoryDurableStore::default());
        let driver =
            DurableRunDriver::new(completed_legacy_v1_state(run_id), store.clone()).unwrap();

        assert!(matches!(
            driver.invocation_disposition().unwrap(),
            DurableInvocationDisposition::ReconcileRequired { .. }
        ));
        let bridge_id = driver
            .snapshot()
            .unwrap()
            .projection()
            .activities
            .values()
            .find(|record| record.definition.input.get("legacy_resolution").is_some())
            .unwrap()
            .definition
            .activity_id
            .clone();
        driver
            .reconcile_legacy_run_stopped_audit(
                "typed-legacy-bridge-for-recovery",
                &bridge_id,
                receipt.turns,
                receipt.reason.clone(),
                receipt.usage,
            )
            .unwrap();
        driver
            .transition(|candidate| {
                candidate.apply_command(RunCommand::Resume {
                    command_id: "resume-typed-legacy-bridge-for-recovery".into(),
                    approvals: vec![],
                })?;
                Ok(())
            })
            .unwrap();
        let (lifecycle_activity_id, lifecycle_attempt) = driver
            .transition(|candidate| {
                let ActivityDecision::Execute {
                    activity_id,
                    attempt,
                    ..
                } = candidate.prepare_activity(
                    RUNTIME_INVOCATION_LIFECYCLE_STEP_ID,
                    invocation_id,
                    json!({
                        "schema_version": RUN_STOPPED_REPLAY_SCHEMA_VERSION,
                        "audit_run_id": candidate.run_id(),
                        "invocation_id": invocation_id,
                    }),
                    SideEffectClass::ReconcileRequired,
                    None,
                )?
                else {
                    return Err(DurabilityError::InvalidEvent {
                        reason: "recovery regression requires an active lifecycle".into(),
                    }
                    .into());
                };
                Ok((activity_id, attempt))
            })
            .unwrap();
        let replay = DurableRunStoppedAuditReplayEnvelope::new(
            DurableRunStoppedAuditKind::Recovery,
            receipt.clone(),
            run_id,
            invocation_id,
            1,
            receipt.turns,
            "superseded_by_durable_terminal_receipt",
            crate::observability::AuditReplayBinding {
                schema_version: 1,
                delivery_id: None,
                sink_count: 0,
                payload_policy: crate::observability::AuditPayloadPolicy::MetadataOnly,
                failure_mode: crate::observability::AuditFailureMode::BestEffort,
                max_preview_bytes: 4096,
            },
        );
        let DurableActivity::Execute {
            activity_id: recovery_activity_id,
            attempt: recovery_attempt,
            ..
        } = driver.begin_recovery_run_stopped_audit(&replay).unwrap()
        else {
            panic!("legacy bridge recovery audit must execute");
        };
        driver
            .complete_recovery_run_stopped_audit_and_invocation_lifecycle(
                &recovery_activity_id,
                recovery_attempt,
                &lifecycle_activity_id,
                lifecycle_attempt,
                &replay,
            )
            .unwrap();

        let receipt_to_finalize = match driver.invocation_disposition().unwrap() {
            DurableInvocationDisposition::FinalizeTerminal(receipt) => receipt,
            other => {
                panic!("completed bridge and recovery must permit finalization, got {other:?}")
            }
        };
        assert_eq!(receipt_to_finalize, receipt);
        driver
            .finalize_run_stopped_receipt(&receipt_to_finalize)
            .unwrap();
        assert_eq!(
            store.load(run_id).unwrap().status(),
            DurableRunStatus::Completed
        );
    }

    #[test]
    fn terminal_v1_zero_attempt_legacy_schedule_requires_reconciliation() {
        let run_id = "legacy-v1-zero-attempt-terminal";
        let legacy_activity_id = crate::durability::stable_id(
            "activity",
            &[
                run_id,
                "root",
                RUNTIME_LEGACY_RUN_STOPPED_AUDIT_STEP_ID,
                RUN_STOPPED_AUDIT_LOGICAL_KEY,
            ],
        );
        let legacy_definition = crate::durability::ActivityDefinition {
            activity_id: legacy_activity_id.clone(),
            stable_step_id: RUNTIME_LEGACY_RUN_STOPPED_AUDIT_STEP_ID.into(),
            logical_key: RUN_STOPPED_AUDIT_LOGICAL_KEY.into(),
            input: json!({"input_hash": "legacy"}),
            input_hash: stable_input_hash(&json!({"input_hash": "legacy"})),
            side_effect_class: SideEffectClass::ReconcileRequired,
            idempotency_key: None,
        };
        let state = RunState::from_events(vec![
            crate::durability::RunEvent {
                schema_version: 1,
                run_id: run_id.into(),
                sequence: 1,
                event_id: "legacy-start".into(),
                kind: crate::durability::RunEventKind::RunStarted {
                    session_id: "session".into(),
                    durability: DurabilityMode::Sync,
                    root_branch_id: "root".into(),
                },
            },
            crate::durability::RunEvent {
                schema_version: 1,
                run_id: run_id.into(),
                sequence: 2,
                event_id: "legacy-scheduled".into(),
                kind: crate::durability::RunEventKind::ActivityScheduled {
                    definition: legacy_definition,
                },
            },
            crate::durability::RunEvent {
                schema_version: 1,
                run_id: run_id.into(),
                sequence: 3,
                event_id: "legacy-terminal".into(),
                kind: crate::durability::RunEventKind::RunCompleted,
            },
        ])
        .unwrap();
        let store = Arc::new(InMemoryDurableStore::default());
        let driver = DurableRunDriver::new(state, store).unwrap();

        assert!(matches!(
            driver.invocation_disposition().unwrap(),
            DurableInvocationDisposition::ReconcileRequired { .. }
        ));
        assert_eq!(
            driver.snapshot().unwrap().status(),
            DurableRunStatus::Completed
        );
        let quarantined = driver.snapshot().unwrap();
        let attempt = quarantined
            .activity(&legacy_activity_id)
            .unwrap()
            .latest_attempt()
            .expect("terminal legacy orphan must receive a synthetic attempt");
        assert_eq!(
            attempt.status,
            crate::durability::ActivityAttemptStatus::ReconcileRequired
        );
        assert_eq!(attempt.attempt, 1);
        assert_eq!(attempt.finished_sequence, Some(attempt.started_sequence));

        let before_invalid_attestation = driver.snapshot().unwrap();
        assert!(driver
            .reconcile_legacy_run_stopped_audit(
                "typed-terminal-zero-attempt-wrong-status",
                &legacy_activity_id,
                0,
                "cancelled",
                crate::types::Usage::default(),
            )
            .is_err());
        assert_eq!(driver.snapshot().unwrap(), before_invalid_attestation);

        driver
            .reconcile_legacy_run_stopped_audit(
                "typed-terminal-zero-attempt",
                &legacy_activity_id,
                0,
                "end_turn",
                crate::types::Usage::default(),
            )
            .unwrap();
        assert_eq!(
            driver.snapshot().unwrap().status(),
            DurableRunStatus::Completed
        );
        assert!(matches!(
            driver.invocation_disposition().unwrap(),
            DurableInvocationDisposition::AlreadyTerminal {
                status: DurableRunStatus::Completed,
                ..
            }
        ));
    }

    #[test]
    fn direct_lifecycle_closure_never_authorizes_terminal_finalization() {
        for (run_id, terminal) in [
            ("direct-lifecycle-running", false),
            ("direct-lifecycle-terminal", true),
        ] {
            let invocation_id = "direct-invocation";
            let mut state = RunState::new("session", run_id, DurabilityMode::Sync).unwrap();
            let (lifecycle_activity_id, lifecycle_attempt) = match state
                .prepare_activity(
                    RUNTIME_INVOCATION_LIFECYCLE_STEP_ID,
                    invocation_id,
                    json!({
                        "schema_version": RUN_STOPPED_REPLAY_SCHEMA_VERSION,
                        "audit_run_id": run_id,
                        "invocation_id": invocation_id,
                    }),
                    SideEffectClass::ReconcileRequired,
                    None,
                )
                .unwrap()
            {
                ActivityDecision::Execute {
                    activity_id,
                    attempt,
                    ..
                } => (activity_id, attempt),
                other => panic!("direct lifecycle must execute, got {other:?}"),
            };
            let replay = DurableRunStoppedAuditReplayEnvelope::new(
                DurableRunStoppedAuditKind::Direct,
                DurableRunStoppedReceipt {
                    turns: 1,
                    reason: "end_turn".into(),
                    usage: crate::types::Usage::default(),
                },
                run_id,
                invocation_id,
                1,
                1,
                "end_turn",
                crate::observability::AuditReplayBinding {
                    schema_version: 1,
                    delivery_id: None,
                    sink_count: 0,
                    payload_policy: crate::observability::AuditPayloadPolicy::MetadataOnly,
                    failure_mode: crate::observability::AuditFailureMode::BestEffort,
                    max_preview_bytes: 4096,
                },
            );
            state
                .complete_activity(
                    &lifecycle_activity_id,
                    lifecycle_attempt,
                    lifecycle_closed_output(&replay),
                )
                .unwrap();
            if terminal {
                state.complete_run("direct-lifecycle-terminal").unwrap();
            }
            let store = Arc::new(InMemoryDurableStore::default());
            let driver = DurableRunDriver::new(state, store).unwrap();

            let disposition = driver.invocation_disposition().unwrap();
            if terminal {
                assert!(matches!(
                    disposition,
                    DurableInvocationDisposition::AlreadyTerminal {
                        status: DurableRunStatus::Completed,
                        ..
                    }
                ));
            } else {
                assert_eq!(disposition, DurableInvocationDisposition::Execute);
            }
        }
    }

    #[test]
    fn disposition_quarantines_well_formed_zero_attempt_lifecycle_once_when_paused_or_reconciling()
    {
        for (index, make_reconcile_required) in [(0, false), (1, true)] {
            let run_id = format!("zero-attempt-lifecycle-{index}");
            let mut state = RunState::new("session", &run_id, DurabilityMode::Sync).unwrap();
            let invocation_id = "orphan-invocation";
            let lifecycle_input = json!({
                "schema_version": 1,
                "audit_run_id": run_id,
                "invocation_id": invocation_id,
            });
            let lifecycle_id = crate::durability::stable_id(
                "activity",
                &[
                    state.run_id(),
                    &state.projection().branch_id,
                    RUNTIME_INVOCATION_LIFECYCLE_STEP_ID,
                    invocation_id,
                ],
            );
            state
                .append_event(crate::durability::RunEvent {
                    schema_version: crate::durability::DURABILITY_SCHEMA_VERSION,
                    run_id: run_id.clone(),
                    sequence: state.next_sequence(),
                    event_id: format!("raw-lifecycle-scheduled-{index}"),
                    kind: crate::durability::RunEventKind::ActivityScheduled {
                        definition: crate::durability::ActivityDefinition {
                            activity_id: lifecycle_id.clone(),
                            stable_step_id: RUNTIME_INVOCATION_LIFECYCLE_STEP_ID.into(),
                            logical_key: invocation_id.into(),
                            input_hash: stable_input_hash(&lifecycle_input),
                            input: lifecycle_input,
                            side_effect_class: SideEffectClass::ReconcileRequired,
                            idempotency_key: None,
                        },
                    },
                })
                .unwrap();
            if make_reconcile_required {
                let (activity_id, attempt) = match state
                    .prepare_activity(
                        "ordinary-unsafe-effect",
                        "needs-operator",
                        json!({"value": index}),
                        SideEffectClass::ReconcileRequired,
                        None,
                    )
                    .unwrap()
                {
                    ActivityDecision::Execute {
                        activity_id,
                        attempt,
                        ..
                    } => (activity_id, attempt),
                    other => panic!("ordinary activity must execute, got {other:?}"),
                };
                state
                    .fail_activity(&activity_id, attempt, "unknown", false, true)
                    .unwrap();
                assert_eq!(state.status(), DurableRunStatus::ReconcileRequired);
            } else {
                state.pause("operator", "paused before recovery").unwrap();
                assert_eq!(state.status(), DurableRunStatus::Paused);
            }

            let store = Arc::new(InMemoryDurableStore::default());
            let driver = DurableRunDriver::new(state, store).unwrap();
            assert!(matches!(
                driver.invocation_disposition().unwrap(),
                DurableInvocationDisposition::ReconcileRequired { .. }
            ));
            let snapshot = driver.snapshot().unwrap();
            let lifecycle = snapshot.activity(&lifecycle_id).unwrap();
            assert_eq!(lifecycle.attempts.len(), 1);
            assert_eq!(
                lifecycle.latest_attempt().unwrap().status,
                crate::durability::ActivityAttemptStatus::ReconcileRequired
            );
            let events_after_first = snapshot.events().len();
            assert!(matches!(
                driver.invocation_disposition().unwrap(),
                DurableInvocationDisposition::ReconcileRequired { .. }
            ));
            assert_eq!(
                driver.snapshot().unwrap().events().len(),
                events_after_first
            );
        }
    }

    #[test]
    fn reconciled_terminal_audits_have_finite_driver_dispositions() {
        for (index, kind) in [
            DurableRunStoppedAuditKind::Canonical,
            DurableRunStoppedAuditKind::Recovery,
        ]
        .into_iter()
        .enumerate()
        {
            let pair = running_terminal_audit_pair(
                kind,
                &format!("finite-terminal-reconciliation-{index}"),
            );
            match kind {
                DurableRunStoppedAuditKind::Canonical => pair
                    .driver
                    .fail_run_stopped_audit_and_invocation_lifecycle(
                        &pair.audit_activity_id,
                        pair.audit_attempt,
                        &pair.lifecycle_activity_id,
                        pair.lifecycle_attempt,
                    )
                    .unwrap(),
                DurableRunStoppedAuditKind::Recovery => pair
                    .driver
                    .fail_recovery_run_stopped_audit_and_invocation_lifecycle(
                        &pair.audit_activity_id,
                        pair.audit_attempt,
                        &pair.lifecycle_activity_id,
                        pair.lifecycle_attempt,
                    )
                    .unwrap(),
                DurableRunStoppedAuditKind::Direct => unreachable!("terminal pair only"),
            }

            let before_cancelled = pair.driver.snapshot().unwrap();
            assert!(pair
                .driver
                .transition(|candidate| {
                    candidate.reconcile_activity(
                        &format!("cancel-terminal-audit-{index}"),
                        &pair.audit_activity_id,
                        crate::durability::ActivityReconciliation::Cancelled,
                    )?;
                    Ok(())
                })
                .is_err());
            assert_eq!(pair.driver.snapshot().unwrap(), before_cancelled);

            pair.driver
                .transition(|candidate| {
                    candidate.reconcile_activity(
                        &format!("retry-terminal-audit-{index}"),
                        &pair.audit_activity_id,
                        crate::durability::ActivityReconciliation::SafeToRetry,
                    )?;
                    candidate.reconcile_activity(
                        &format!("authorize-terminal-replay-{index}"),
                        &pair.lifecycle_activity_id,
                        crate::durability::ActivityReconciliation::Completed {
                            output: json!({
                                "status": "terminal_replay_authorized",
                                "replay": pair.replay.clone(),
                            }),
                        },
                    )?;
                    Ok(())
                })
                .unwrap();
            assert_eq!(
                pair.driver.snapshot().unwrap().status(),
                DurableRunStatus::Paused
            );
            assert!(matches!(
                pair.driver.invocation_disposition().unwrap(),
                DurableInvocationDisposition::AwaitingResume { .. }
            ));

            pair.driver
                .transition(|candidate| {
                    candidate.apply_command(RunCommand::Resume {
                        command_id: format!("resume-terminal-replay-{index}"),
                        approvals: vec![],
                    })?;
                    Ok(())
                })
                .unwrap();
            let replay = match pair.driver.invocation_disposition().unwrap() {
                DurableInvocationDisposition::RetryTerminalAudit(replay) => replay,
                other => panic!("safe terminal retry must be finite, got {other:?}"),
            };
            assert_eq!(replay, pair.replay);

            let retry = pair.driver.begin_terminal_audit_retry(&replay).unwrap();
            assert_eq!(retry.kind, kind);
            assert_eq!(retry.activity_id, pair.audit_activity_id);
            assert_eq!(retry.attempt, 2);
            pair.driver.complete_terminal_audit_retry(&retry).unwrap();

            let receipt = match pair.driver.invocation_disposition().unwrap() {
                DurableInvocationDisposition::FinalizeTerminal(receipt) => receipt,
                other => panic!("accepted terminal retry must finalize, got {other:?}"),
            };
            assert_eq!(receipt, replay.terminal_receipt);
            pair.driver.finalize_run_stopped_receipt(&receipt).unwrap();
            assert_eq!(
                pair.driver.snapshot().unwrap().status(),
                DurableRunStatus::Completed
            );
        }
    }

    #[test]
    fn disposition_exposes_all_running_unsafe_fences_in_one_pass() {
        for (index, kind) in [
            DurableRunStoppedAuditKind::Canonical,
            DurableRunStoppedAuditKind::Recovery,
        ]
        .into_iter()
        .enumerate()
        {
            let pair =
                running_terminal_audit_pair(kind, &format!("one-pass-terminal-fences-{index}"));
            assert!(matches!(
                pair.driver.invocation_disposition().unwrap(),
                DurableInvocationDisposition::ReconcileRequired { .. }
            ));
            let snapshot = pair.driver.snapshot().unwrap();
            let expected = [
                pair.lifecycle_activity_id.clone(),
                pair.audit_activity_id.clone(),
            ]
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>();
            assert_eq!(reconciliation_ids(&snapshot), expected);

            let events_after_first_pass = snapshot.events().len();
            assert!(matches!(
                pair.driver.invocation_disposition().unwrap(),
                DurableInvocationDisposition::ReconcileRequired { .. }
            ));
            assert_eq!(
                pair.driver.snapshot().unwrap().events().len(),
                events_after_first_pass
            );
        }

        let run_id = "one-pass-non-reserved-fences";
        let driver = DurableRunDriver::new(
            RunState::new("session", run_id, DurabilityMode::Sync).unwrap(),
            Arc::new(InMemoryDurableStore::default()),
        )
        .unwrap();
        let (lifecycle_activity_id, _) = executed_activity(
            driver
                .begin_invocation_lifecycle("ordinary-work-invocation")
                .unwrap(),
        );
        let (already_ambiguous_id, already_ambiguous_attempt) = executed_activity(
            driver
                .begin_activity(
                    "unsafe-tool-v1",
                    "tool-1",
                    json!({"call": 1}),
                    SideEffectClass::ReconcileRequired,
                    None,
                )
                .unwrap(),
        );
        let (still_running_id, _) = executed_activity(
            driver
                .begin_activity(
                    "unsafe-tool-v1",
                    "tool-2",
                    json!({"call": 2}),
                    SideEffectClass::ReconcileRequired,
                    None,
                )
                .unwrap(),
        );
        driver
            .fail_activity(
                &already_ambiguous_id,
                already_ambiguous_attempt,
                "unknown tool result",
                false,
                true,
            )
            .unwrap();
        assert_eq!(
            driver.snapshot().unwrap().status(),
            DurableRunStatus::ReconcileRequired
        );

        assert!(matches!(
            driver.invocation_disposition().unwrap(),
            DurableInvocationDisposition::ReconcileRequired { .. }
        ));
        let snapshot = driver.snapshot().unwrap();
        let expected = [
            lifecycle_activity_id,
            already_ambiguous_id,
            still_running_id,
        ]
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(reconciliation_ids(&snapshot), expected);
    }
}
