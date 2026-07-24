//! Append-only durable execution primitives.
//!
//! This module deliberately separates conversation persistence ([`crate::session::Session`]) from
//! execution persistence. A durable run records every state transition as a monotonic event and
//! derives its active projection from that log. External work is at-least-once: an activity whose
//! side effect may have happened but whose result was not committed is never described as
//! exactly-once and, unless it is pure or protected by an idempotency key, requires explicit
//! reconciliation.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

/// Schema version for serialized durable run state and newly emitted events.
///
/// Version 2 makes terminal failure and run-cancellation events fail closed while an activity
/// attempt is still running. Version 3 adds the explicit `ActivityAttemptCancelled` event without
/// changing historical v1/v2 event bytes. Older logs retain their original replay semantics and
/// are migrated to a version 3 [`RunState`] when loaded.
pub const DURABILITY_SCHEMA_VERSION: u32 = 3;
pub(crate) const MIN_SUPPORTED_DURABILITY_SCHEMA_VERSION: u32 = 1;

pub(crate) const RUNTIME_RUN_STOPPED_AUDIT_STEP_ID: &str = "runtime-run-stopped-audit-v2";
pub(crate) const RUNTIME_LEGACY_RUN_STOPPED_AUDIT_STEP_ID: &str = "runtime-run-stopped-audit-v1";
pub(crate) const RUNTIME_RECOVERY_RUN_STOPPED_AUDIT_STEP_ID: &str =
    "runtime-recovery-run-stopped-audit-v1";
pub(crate) const RUNTIME_INVOCATION_LIFECYCLE_STEP_ID: &str = "runtime-invocation-lifecycle-v1";

const TERMINAL_ACTIVITY_INVARIANT_SCHEMA_VERSION: u32 = 2;
const ACTIVITY_ATTEMPT_CANCELLED_SCHEMA_VERSION: u32 = 3;

/// Upper bound for one distributed worker lease. Long-running work stays owned through bounded
/// heartbeat renewals instead of creating an effectively permanent claim.
pub const MAX_DURABLE_WORKER_LEASE_MS: u64 = 60 * 60 * 1_000;

pub(crate) const fn is_supported_durability_schema_version(schema_version: u32) -> bool {
    schema_version >= MIN_SUPPORTED_DURABILITY_SCHEMA_VERSION
        && schema_version <= DURABILITY_SCHEMA_VERSION
}

/// Controls when a coordinator is allowed to acknowledge progress.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DurabilityMode {
    /// Persist a transition before starting the next unit of work.
    Sync,
    /// Persist concurrently with subsequent work. Faster, but a crash can lose recent progress.
    Async,
    /// Persist only when the run exits. Intended for explicitly non-resumable work.
    Exit,
}

/// Declares how an activity may be retried after an ambiguous worker failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SideEffectClass {
    /// Repeating the activity has no externally observable side effect.
    Pure,
    /// Repeating the activity is safe only with the persisted idempotency key.
    Idempotent,
    /// A possibly-started activity must be reconciled by an operator or integration.
    ReconcileRequired,
}

/// Materialized lifecycle of a durable run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DurableRunStatus {
    Running,
    Paused,
    ReconcileRequired,
    Completed,
    Failed,
    Cancelled,
}

impl DurableRunStatus {
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }
}

/// Materialized lifecycle of one activity attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActivityAttemptStatus {
    Running,
    Completed,
    Failed,
    ReconcileRequired,
    Cancelled,
}

/// Immutable identity and retry contract for one logical activity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ActivityDefinition {
    pub activity_id: String,
    /// Stable code/deployment identity. Renaming it changes durable replay compatibility.
    pub stable_step_id: String,
    /// Stable occurrence identity, such as `turn-2/tool-0`.
    pub logical_key: String,
    pub input: Value,
    pub input_hash: String,
    pub side_effect_class: SideEffectClass,
    pub idempotency_key: Option<String>,
}

/// One append-only execution attempt for an activity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ActivityAttempt {
    pub attempt: u32,
    pub status: ActivityAttemptStatus,
    pub started_sequence: u64,
    pub finished_sequence: Option<u64>,
    pub output: Option<Value>,
    pub output_hash: Option<String>,
    pub error: Option<String>,
    pub retryable: bool,
    /// True when the runtime cannot establish whether an external effect occurred.
    pub effect_ambiguous: bool,
}

/// Active projection of a logical activity and all of its attempts.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ActivityRecord {
    pub definition: ActivityDefinition,
    pub attempts: Vec<ActivityAttempt>,
}

impl ActivityRecord {
    pub fn completed_output(&self) -> Option<&Value> {
        self.attempts
            .iter()
            .rev()
            .find(|attempt| attempt.status == ActivityAttemptStatus::Completed)
            .and_then(|attempt| attempt.output.as_ref())
    }

    pub fn latest_attempt(&self) -> Option<&ActivityAttempt> {
        self.attempts.last()
    }
}

/// Versioned metadata for an artifact. Artifact bytes live in a host-selected blob store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactMetadata {
    pub artifact_id: String,
    pub version: u64,
    pub version_id: String,
    pub branch_id: String,
    pub media_type: String,
    pub size_bytes: u64,
    pub content_hash: String,
    pub created_by_activity_id: Option<String>,
    pub previous_version_id: Option<String>,
}

/// Durable approval lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DurableApprovalStatus {
    Pending,
    Approved,
    Rejected,
}

/// Human-in-the-loop interaction semantics persisted with an approval request.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DurableApprovalKind {
    /// A binary confirmation. An optional response may contain operator metadata.
    #[default]
    Confirmation,
    /// The run cannot continue until the caller supplies a non-null value.
    MissingInput,
    /// A produced value must be accepted, replaced, or rejected by a reviewer.
    OutputReview,
    /// The caller must explicitly choose an `edit` or `retry` action.
    EditRetry,
}

/// A persisted human or policy approval request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DurableApproval {
    pub approval_id: String,
    pub logical_key: String,
    pub activity_id: Option<String>,
    #[serde(default)]
    pub kind: DurableApprovalKind,
    pub prompt: String,
    pub payload: Value,
    /// Immutable policy identity that governed the request, when governance is enabled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_snapshot_hash: Option<String>,
    /// Complete immutable policy context for governed runs. Hash-only records remain readable for
    /// legacy runs, but newly bound runs require this value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub governance_binding: Option<crate::governance::GovernanceBinding>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_at_unix_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at_unix_ms: Option<u64>,
    pub status: DurableApprovalStatus,
    pub response: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_at_unix_ms: Option<u64>,
    #[serde(default)]
    pub timed_out: bool,
    pub requested_sequence: u64,
    pub resolved_sequence: Option<u64>,
}

/// A typed, expiring approval request. Unlike the legacy approval helper, this request can be
/// safely resumed after a process restart because its clock and policy binding are event-sourced.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DurableApprovalRequest {
    pub logical_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub activity_id: Option<String>,
    pub kind: DurableApprovalKind,
    pub prompt: String,
    pub payload: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_snapshot_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub governance_binding: Option<crate::governance::GovernanceBinding>,
    pub requested_at_unix_ms: u64,
    pub expires_at_unix_ms: u64,
}

const DURABLE_APPROVAL_ENVELOPE_KEY: &str = "$aikit_durable_approval";
const DURABLE_RESOLUTION_ENVELOPE_KEY: &str = "$aikit_durable_resolution";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DurableApprovalEnvelope {
    schema_version: u32,
    kind: DurableApprovalKind,
    payload: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    policy_snapshot_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    governance_binding: Option<crate::governance::GovernanceBinding>,
    requested_at_unix_ms: u64,
    expires_at_unix_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DurableResolutionEnvelope {
    schema_version: u32,
    resolved_at_unix_ms: u64,
    timed_out: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    response: Option<Value>,
}

/// One response supplied while resuming a paused run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ApprovalResolution {
    pub approval_id: String,
    pub approved: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response: Option<Value>,
}

/// Owner-bound, expiring execution claim materialized from the durable event log.
///
/// `lease_id` is a fencing token, not a promise of exactly-once execution. If a worker disappears,
/// a later owner may recover the expired claim; existing activity semantics then decide whether
/// work is safely retryable or requires explicit reconciliation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DurableWorkerLease {
    pub owner_id: String,
    pub lease_id: String,
    pub acquired_at_unix_ms: u64,
    pub heartbeat_at_unix_ms: u64,
    pub expires_at_unix_ms: u64,
}

/// Materialized active branch derived from the event log.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunProjection {
    pub branch_id: String,
    pub status: DurableRunStatus,
    pub state: Value,
    pub activities: BTreeMap<String, ActivityRecord>,
    pub approvals: BTreeMap<String, DurableApproval>,
    pub artifacts: BTreeMap<String, Vec<ArtifactMetadata>>,
    pub current_checkpoint_id: Option<String>,
    pub pause_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_lease: Option<DurableWorkerLease>,
}

impl RunProjection {
    fn root(run_id: &str) -> Self {
        Self {
            branch_id: stable_identifier("branch", &[run_id, "root"]),
            status: DurableRunStatus::Running,
            state: Value::Object(Default::default()),
            activities: BTreeMap::new(),
            approvals: BTreeMap::new(),
            artifacts: BTreeMap::new(),
            current_checkpoint_id: None,
            pause_reason: None,
            worker_lease: None,
        }
    }

    pub fn pending_approval_ids(&self) -> Vec<&str> {
        self.approvals
            .values()
            .filter(|approval| approval.status == DurableApprovalStatus::Pending)
            .map(|approval| approval.approval_id.as_str())
            .collect()
    }
}

/// An immutable snapshot of a run projection at a monotonic event boundary.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Checkpoint {
    pub checkpoint_id: String,
    pub run_id: String,
    pub event_sequence: u64,
    pub parent_checkpoint_id: Option<String>,
    pub label: Option<String>,
    pub projection: RunProjection,
}

/// Resolution supplied after externally reconciling an ambiguous activity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ActivityReconciliation {
    Completed { output: Value },
    SafeToRetry,
    Cancelled,
}

/// Append-only events from which the active projection is rebuilt.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RunEventKind {
    RunStarted {
        session_id: String,
        durability: DurabilityMode,
        root_branch_id: String,
    },
    /// Pins the immutable policy identity before the run performs any work.
    PolicySnapshotPinned {
        policy_snapshot_hash: String,
    },
    /// Pins the full scoped policy identity for a durable governed run. The older hash-only event
    /// remains accepted so v0.2 snapshots can be replayed and migrated without rewriting history.
    GovernanceBindingPinned {
        binding: crate::governance::GovernanceBinding,
    },
    ForkedFrom {
        session_id: String,
        source_run_id: String,
        durability: DurabilityMode,
        source_checkpoint: Box<Checkpoint>,
        new_branch_id: String,
    },
    StateReplaced {
        state: Value,
    },
    WorkerLeaseClaimed {
        owner_id: String,
        lease_id: String,
        claimed_at_unix_ms: u64,
        expires_at_unix_ms: u64,
    },
    WorkerLeaseRenewed {
        owner_id: String,
        lease_id: String,
        renewed_at_unix_ms: u64,
        expires_at_unix_ms: u64,
    },
    WorkerLeaseReleased {
        owner_id: String,
        lease_id: String,
        released_at_unix_ms: u64,
    },
    ActivityScheduled {
        definition: ActivityDefinition,
    },
    ActivityAttemptStarted {
        activity_id: String,
        attempt: u32,
    },
    ActivityAttemptCompleted {
        activity_id: String,
        attempt: u32,
        output: Value,
        output_hash: String,
    },
    ActivityAttemptFailed {
        activity_id: String,
        attempt: u32,
        error: String,
        retryable: bool,
        effect_ambiguous: bool,
    },
    ActivityAttemptCancelled {
        activity_id: String,
        attempt: u32,
        reason: String,
    },
    ActivityReconciliationRequired {
        activity_id: String,
        attempt: u32,
        reason: String,
    },
    ActivityReconciled {
        activity_id: String,
        attempt: u32,
        resolution: ActivityReconciliation,
    },
    ApprovalRequested {
        approval_id: String,
        logical_key: String,
        activity_id: Option<String>,
        prompt: String,
        payload: Value,
    },
    ApprovalResolved {
        approval_id: String,
        approved: bool,
        response: Option<Value>,
    },
    ArtifactPublished {
        metadata: ArtifactMetadata,
    },
    CheckpointCommitted {
        checkpoint_id: String,
        label: Option<String>,
    },
    RunPaused {
        reason: String,
    },
    RunResumed,
    ForkCreated {
        new_run_id: String,
        checkpoint_id: String,
        new_branch_id: String,
        side_effects_reconciled: bool,
    },
    RunRewound {
        checkpoint_id: String,
        new_branch_id: String,
        side_effects_reconciled: bool,
    },
    RunCompleted,
    RunFailed {
        error: String,
    },
    RunCancelled {
        reason: Option<String>,
    },
}

/// One immutable event in a run log.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunEvent {
    pub schema_version: u32,
    pub run_id: String,
    pub sequence: u64,
    /// Caller-stable deduplication identity. Reusing it with different content is an error.
    pub event_id: String,
    pub kind: RunEventKind,
}

/// Result of appending an event to a run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppendOutcome {
    Appended { sequence: u64 },
    Deduplicated { sequence: u64 },
}

/// Work decision returned for a logical activity.
#[derive(Debug, Clone, PartialEq)]
pub enum ActivityDecision {
    Execute {
        activity_id: String,
        attempt: u32,
        idempotency_key: Option<String>,
    },
    ReuseCompleted {
        activity_id: String,
        output: Value,
    },
    ReconcileRequired {
        activity_id: String,
        reason: String,
    },
    Failed {
        activity_id: String,
        error: String,
    },
    Cancelled {
        activity_id: String,
    },
}

/// Durable control commands. Each command ID must be stable across transport retries.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case")]
pub enum RunCommand {
    Resume {
        command_id: String,
        #[serde(default)]
        approvals: Vec<ApprovalResolution>,
    },
    Fork {
        command_id: String,
        new_run_id: String,
        checkpoint_id: String,
        /// Explicit acknowledgement for non-pure work observed after the checkpoint.
        side_effects_reconciled: bool,
    },
    Rewind {
        command_id: String,
        checkpoint_id: String,
        /// Explicit acknowledgement for non-pure work observed after the checkpoint.
        side_effects_reconciled: bool,
    },
    Cancel {
        command_id: String,
        reason: Option<String>,
    },
}

/// Result of applying a durable control command.
#[derive(Debug, Clone, PartialEq)]
pub enum CommandOutcome {
    Resumed {
        sequence: u64,
    },
    Forked {
        run: Box<RunState>,
    },
    Rewound {
        checkpoint_id: String,
        sequence: u64,
    },
    Cancelled {
        sequence: u64,
    },
}

/// Errors are fail-closed: a rejected transition leaves [`RunState`] unchanged.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DurabilityError {
    #[error("{field} cannot be empty or contain control characters")]
    InvalidIdentifier { field: &'static str },
    #[error("unsupported durability schema version {actual}; expected {expected}")]
    UnsupportedSchema { expected: u32, actual: u32 },
    #[error("event belongs to run `{actual}`, expected `{expected}`")]
    WrongRun { expected: String, actual: String },
    #[error("event `{event_id}` was reused with different content")]
    DuplicateEventConflict { event_id: String },
    #[error("non-monotonic event sequence: expected {expected}, got {actual}")]
    NonMonotonicSequence { expected: u64, actual: u64 },
    #[error("first durable event must start or fork a run")]
    MissingRunStart,
    #[error("run has already been initialized")]
    AlreadyStarted,
    #[error("run is terminal: {status:?}")]
    TerminalRun { status: DurableRunStatus },
    #[error("activity `{activity_id}` was not found")]
    ActivityNotFound { activity_id: String },
    #[error("activity `{activity_id}` input changed for stable step `{stable_step_id}`")]
    ActivityInputChanged {
        activity_id: String,
        stable_step_id: String,
    },
    #[error("activity `{activity_id}` requires an idempotency key")]
    MissingIdempotencyKey { activity_id: String },
    #[error("invalid transition for activity `{activity_id}`: {reason}")]
    InvalidActivityTransition { activity_id: String, reason: String },
    #[error("approval `{approval_id}` was not found")]
    ApprovalNotFound { approval_id: String },
    #[error("approval `{approval_id}` has already been resolved")]
    ApprovalAlreadyResolved { approval_id: String },
    #[error("approval `{approval_id}` requires an explicit trusted clock")]
    ApprovalClockRequired { approval_id: String },
    #[error("invalid resolution for approval `{approval_id}`: {reason}")]
    InvalidApprovalResolution { approval_id: String, reason: String },
    #[error("run still has pending approvals")]
    PendingApprovals,
    #[error("checkpoint `{checkpoint_id}` was not found")]
    CheckpointNotFound { checkpoint_id: String },
    #[error("artifact `{artifact_id}` has invalid version {actual}; expected {expected}")]
    InvalidArtifactVersion {
        artifact_id: String,
        expected: u64,
        actual: u64,
    },
    #[error("reconciliation is required for activities: {activity_ids:?}")]
    ReconcileRequired { activity_ids: Vec<String> },
    #[error("run is not paused")]
    NotPaused,
    #[error("run is not executing: {status:?}")]
    RunNotExecuting { status: DurableRunStatus },
    #[error("run is waiting for reconciliation")]
    RunRequiresReconciliation,
    #[error(
        "durable worker lease is held by `{owner_id}` until unix millisecond {expires_at_unix_ms}"
    )]
    WorkerLeaseHeld {
        owner_id: String,
        expires_at_unix_ms: u64,
    },
    #[error("durable worker `{owner_id}` no longer owns the active lease")]
    WorkerLeaseLost { owner_id: String },
    #[error("invalid durable event: {reason}")]
    InvalidEvent { reason: String },
}

pub type DurabilityResult<T> = Result<T, DurabilityError>;

/// In-memory append-only run state. Persistence adapters should store its events transactionally.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RunState {
    schema_version: u32,
    session_id: String,
    run_id: String,
    durability: DurabilityMode,
    parent_run_id: Option<String>,
    policy_snapshot_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    governance_binding: Option<crate::governance::GovernanceBinding>,
    events: Vec<RunEvent>,
    checkpoints: BTreeMap<String, Checkpoint>,
    projection: RunProjection,
}

#[derive(Deserialize)]
struct SerializedRunState {
    schema_version: u32,
    session_id: String,
    run_id: String,
    durability: DurabilityMode,
    parent_run_id: Option<String>,
    #[serde(default)]
    policy_snapshot_hash: Option<String>,
    #[serde(default)]
    governance_binding: Option<crate::governance::GovernanceBinding>,
    events: Vec<RunEvent>,
    checkpoints: BTreeMap<String, Checkpoint>,
    projection: RunProjection,
}

impl<'de> Deserialize<'de> for RunState {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error as _;

        let serialized = SerializedRunState::deserialize(deserializer)?;
        if !is_supported_durability_schema_version(serialized.schema_version) {
            return Err(D::Error::custom(format!(
                "unsupported durability schema version {}; supported range is {}..={}",
                serialized.schema_version,
                MIN_SUPPORTED_DURABILITY_SCHEMA_VERSION,
                DURABILITY_SCHEMA_VERSION
            )));
        }
        if serialized
            .events
            .iter()
            .any(|event| event.schema_version > serialized.schema_version)
        {
            return Err(D::Error::custom(
                "durable event schema version exceeds serialized run state schema version",
            ));
        }
        let replayed = RunState::from_events(serialized.events.clone())
            .map_err(|error| D::Error::custom(error.to_string()))?;
        if replayed.session_id != serialized.session_id
            || replayed.run_id != serialized.run_id
            || replayed.durability != serialized.durability
            || replayed.parent_run_id != serialized.parent_run_id
            || replayed.policy_snapshot_hash != serialized.policy_snapshot_hash
            || replayed.governance_binding != serialized.governance_binding
            || replayed.events != serialized.events
            || replayed.checkpoints != serialized.checkpoints
            || replayed.projection != serialized.projection
        {
            return Err(D::Error::custom(
                "serialized durable projection does not match its event log",
            ));
        }
        Ok(replayed)
    }
}

impl RunState {
    /// Start an execution for an existing session identifier.
    pub fn new(
        session_id: impl Into<String>,
        run_id: impl Into<String>,
        durability: DurabilityMode,
    ) -> DurabilityResult<Self> {
        let session_id = session_id.into();
        let run_id = run_id.into();
        validate_identifier("session_id", &session_id)?;
        validate_identifier("run_id", &run_id)?;
        let projection = RunProjection::root(&run_id);
        let kind = RunEventKind::RunStarted {
            session_id: session_id.clone(),
            durability,
            root_branch_id: projection.branch_id.clone(),
        };
        let mut state = Self::blank(session_id, run_id.clone(), durability, projection);
        let event_id = stable_identifier("event", &[&run_id, "run_started"]);
        state.emit(event_id, kind)?;
        Ok(state)
    }

    /// Start a run and pin the sealed governance policy before any mutable run state exists.
    pub fn new_with_policy_snapshot(
        session_id: impl Into<String>,
        run_id: impl Into<String>,
        durability: DurabilityMode,
        policy_snapshot: &crate::governance::PolicySnapshot,
    ) -> DurabilityResult<Self> {
        policy_snapshot
            .validate()
            .map_err(|error| DurabilityError::InvalidEvent {
                reason: format!("invalid policy snapshot: {error}"),
            })?;
        let run_id = run_id.into();
        let binding = crate::governance::GovernanceBinding::seal(
            policy_snapshot.hash(),
            None,
            None,
            run_id.clone(),
        )
        .map_err(|error| DurabilityError::InvalidEvent {
            reason: format!("invalid governance binding: {error}"),
        })?;
        Self::new_with_governance_binding(session_id, run_id, durability, binding)
    }

    /// Start a governed run with the complete scoped identity pinned before any work is emitted.
    pub fn new_with_governance_binding(
        session_id: impl Into<String>,
        run_id: impl Into<String>,
        durability: DurabilityMode,
        binding: crate::governance::GovernanceBinding,
    ) -> DurabilityResult<Self> {
        binding
            .validate()
            .map_err(|error| DurabilityError::InvalidEvent {
                reason: format!("invalid governance binding: {error}"),
            })?;
        let run_id = run_id.into();
        if binding.run_id() != run_id {
            return Err(DurabilityError::InvalidEvent {
                reason: "governance binding run_id does not match durable run".into(),
            });
        }
        let mut state = Self::new(session_id, run_id, durability)?;
        state.emit(
            stable_identifier("event", &[state.run_id(), "governance_binding_pinned"]),
            RunEventKind::GovernanceBindingPinned { binding },
        )?;
        Ok(state)
    }

    /// Start a durable execution using the canonical session's identifier.
    pub fn for_session(
        session: &crate::session::Session,
        run_id: impl Into<String>,
        durability: DurabilityMode,
    ) -> DurabilityResult<Self> {
        Self::new(session.id.clone(), run_id, durability)
    }

    /// Rebuild and validate a run exclusively from its append-only event log.
    ///
    /// Version 1 and 2 events are replayed with their historical transition semantics and migrated
    /// in memory; any event subsequently emitted by the returned state uses the current schema.
    pub fn from_events(events: impl IntoIterator<Item = RunEvent>) -> DurabilityResult<Self> {
        let events: Vec<RunEvent> = events.into_iter().collect();
        let first = events.first().ok_or(DurabilityError::MissingRunStart)?;
        let (session_id, durability) = match &first.kind {
            RunEventKind::RunStarted {
                session_id,
                durability,
                ..
            } => (session_id.clone(), *durability),
            RunEventKind::ForkedFrom {
                session_id,
                durability,
                ..
            } => (session_id.clone(), *durability),
            _ => return Err(DurabilityError::MissingRunStart),
        };
        validate_identifier("session_id", &session_id)?;
        validate_identifier("run_id", &first.run_id)?;
        let mut state = Self::blank(
            session_id,
            first.run_id.clone(),
            durability,
            RunProjection::root(&first.run_id),
        );
        for event in events {
            state.append_replayed_event(event)?;
        }
        Ok(state)
    }

    fn blank(
        session_id: String,
        run_id: String,
        durability: DurabilityMode,
        projection: RunProjection,
    ) -> Self {
        Self {
            schema_version: DURABILITY_SCHEMA_VERSION,
            session_id,
            run_id,
            durability,
            parent_run_id: None,
            policy_snapshot_hash: None,
            governance_binding: None,
            events: Vec::new(),
            checkpoints: BTreeMap::new(),
            projection,
        }
    }

    pub fn schema_version(&self) -> u32 {
        self.schema_version
    }

    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    pub fn durability(&self) -> DurabilityMode {
        self.durability
    }

    pub fn parent_run_id(&self) -> Option<&str> {
        self.parent_run_id.as_deref()
    }

    pub fn policy_snapshot_hash(&self) -> Option<&str> {
        self.policy_snapshot_hash.as_deref()
    }

    pub fn governance_binding(&self) -> Option<&crate::governance::GovernanceBinding> {
        self.governance_binding.as_ref()
    }

    pub fn events(&self) -> &[RunEvent] {
        &self.events
    }

    pub fn checkpoints(&self) -> &BTreeMap<String, Checkpoint> {
        &self.checkpoints
    }

    pub fn projection(&self) -> &RunProjection {
        &self.projection
    }

    pub fn status(&self) -> DurableRunStatus {
        self.projection.status
    }

    pub fn worker_lease(&self) -> Option<&DurableWorkerLease> {
        self.projection.worker_lease.as_ref()
    }

    /// Claim an unowned or expired distributed-worker lease.
    ///
    /// Returns `true` when this claim recovered an expired owner. The event is still only a
    /// fencing boundary: an interrupted unsafe activity remains reconciliation-required. A
    /// terminal run may also be claimed for audit reconciliation and stale-lease cleanup; its
    /// terminal status still prevents provider or tool work from starting.
    pub(crate) fn claim_worker_lease(
        &mut self,
        owner_id: &str,
        lease_id: &str,
        claimed_at_unix_ms: u64,
        expires_at_unix_ms: u64,
    ) -> DurabilityResult<bool> {
        validate_identifier("worker_owner_id", owner_id)?;
        validate_identifier("worker_lease_id", lease_id)?;
        validate_worker_lease_window(claimed_at_unix_ms, expires_at_unix_ms)?;
        let recovered = self.projection.worker_lease.is_some();
        self.emit(
            stable_identifier(
                "event",
                &[
                    &self.run_id,
                    "worker_lease_claimed",
                    lease_id,
                    &claimed_at_unix_ms.to_string(),
                ],
            ),
            RunEventKind::WorkerLeaseClaimed {
                owner_id: owner_id.to_string(),
                lease_id: lease_id.to_string(),
                claimed_at_unix_ms,
                expires_at_unix_ms,
            },
        )?;
        Ok(recovered)
    }

    pub(crate) fn renew_worker_lease(
        &mut self,
        owner_id: &str,
        lease_id: &str,
        renewed_at_unix_ms: u64,
        expires_at_unix_ms: u64,
    ) -> DurabilityResult<()> {
        validate_identifier("worker_owner_id", owner_id)?;
        validate_identifier("worker_lease_id", lease_id)?;
        validate_worker_lease_window(renewed_at_unix_ms, expires_at_unix_ms)?;
        self.emit(
            stable_identifier(
                "event",
                &[
                    &self.run_id,
                    "worker_lease_renewed",
                    lease_id,
                    &renewed_at_unix_ms.to_string(),
                ],
            ),
            RunEventKind::WorkerLeaseRenewed {
                owner_id: owner_id.to_string(),
                lease_id: lease_id.to_string(),
                renewed_at_unix_ms,
                expires_at_unix_ms,
            },
        )?;
        Ok(())
    }

    pub(crate) fn release_worker_lease(
        &mut self,
        owner_id: &str,
        lease_id: &str,
        released_at_unix_ms: u64,
    ) -> DurabilityResult<()> {
        validate_identifier("worker_owner_id", owner_id)?;
        validate_identifier("worker_lease_id", lease_id)?;
        self.emit(
            stable_identifier(
                "event",
                &[
                    &self.run_id,
                    "worker_lease_released",
                    lease_id,
                    &released_at_unix_ms.to_string(),
                ],
            ),
            RunEventKind::WorkerLeaseReleased {
                owner_id: owner_id.to_string(),
                lease_id: lease_id.to_string(),
                released_at_unix_ms,
            },
        )?;
        Ok(())
    }

    pub fn next_sequence(&self) -> u64 {
        self.events
            .last()
            .map_or(1, |event| event.sequence.saturating_add(1))
    }

    /// Append a caller-built event with monotonic-sequence and event-ID deduplication checks.
    pub fn append_event(&mut self, event: RunEvent) -> DurabilityResult<AppendOutcome> {
        if event.schema_version != DURABILITY_SCHEMA_VERSION {
            return Err(DurabilityError::UnsupportedSchema {
                expected: DURABILITY_SCHEMA_VERSION,
                actual: event.schema_version,
            });
        }
        self.append_validated_event(event)
    }

    fn append_replayed_event(&mut self, event: RunEvent) -> DurabilityResult<AppendOutcome> {
        if !is_supported_durability_schema_version(event.schema_version) {
            return Err(DurabilityError::UnsupportedSchema {
                expected: DURABILITY_SCHEMA_VERSION,
                actual: event.schema_version,
            });
        }
        if self
            .events
            .last()
            .is_some_and(|previous| previous.schema_version > event.schema_version)
        {
            return Err(DurabilityError::InvalidEvent {
                reason: "durability event schema versions must be monotonic".into(),
            });
        }
        self.append_validated_event(event)
    }

    fn append_validated_event(&mut self, event: RunEvent) -> DurabilityResult<AppendOutcome> {
        validate_identifier("event_id", &event.event_id)?;
        if event.run_id != self.run_id {
            return Err(DurabilityError::WrongRun {
                expected: self.run_id.clone(),
                actual: event.run_id,
            });
        }
        if let Some(existing) = self
            .events
            .iter()
            .find(|existing| existing.event_id == event.event_id)
        {
            if existing.kind == event.kind {
                return Ok(AppendOutcome::Deduplicated {
                    sequence: existing.sequence,
                });
            }
            return Err(DurabilityError::DuplicateEventConflict {
                event_id: event.event_id,
            });
        }
        let expected = self.next_sequence();
        if event.sequence != expected {
            return Err(DurabilityError::NonMonotonicSequence {
                expected,
                actual: event.sequence,
            });
        }

        // Apply to a clone so every rejected transition is atomic from the caller's perspective.
        let mut candidate = self.clone();
        candidate.apply_event_in_place(&event)?;
        candidate.events.push(event);
        *self = candidate;
        Ok(AppendOutcome::Appended { sequence: expected })
    }

    fn emit(
        &mut self,
        event_id: impl Into<String>,
        kind: RunEventKind,
    ) -> DurabilityResult<AppendOutcome> {
        self.append_event(RunEvent {
            schema_version: DURABILITY_SCHEMA_VERSION,
            run_id: self.run_id.clone(),
            sequence: self.next_sequence(),
            event_id: event_id.into(),
            kind,
        })
    }

    fn ensure_active(&self) -> DurabilityResult<()> {
        if self.projection.status.is_terminal() {
            return Err(DurabilityError::TerminalRun {
                status: self.projection.status,
            });
        }
        Ok(())
    }

    fn ensure_reserved_audit_lifecycle_closed(&self) -> DurabilityResult<()> {
        let open = self.projection.activities.values().find(|record| {
            if !is_reserved_audit_activity(record) {
                return false;
            }
            match record.definition.stable_step_id.as_str() {
                RUNTIME_LEGACY_RUN_STOPPED_AUDIT_STEP_ID => {
                    !self.projection.activities.values().any(|bridge| {
                        bridge.completed_output().is_some_and(|output| {
                            validate_legacy_terminal_resolution(
                                &self.projection.activities,
                                bridge,
                                output,
                            )
                            .is_ok()
                                && bridge
                                    .definition
                                    .input
                                    .get("legacy_resolution")
                                    .and_then(|source| source.get("source_activity_id"))
                                    .and_then(Value::as_str)
                                    == Some(record.definition.activity_id.as_str())
                        })
                    })
                }
                RUNTIME_RUN_STOPPED_AUDIT_STEP_ID
                | RUNTIME_RECOVERY_RUN_STOPPED_AUDIT_STEP_ID
                | RUNTIME_INVOCATION_LIFECYCLE_STEP_ID => record.completed_output().is_none(),
                _ => false,
            }
        });
        if let Some(record) = open {
            return Err(DurabilityError::InvalidEvent {
                reason: format!(
                    "run cannot become terminal while reserved audit activity `{}` is unresolved",
                    record.definition.activity_id
                ),
            });
        }
        Ok(())
    }

    fn ensure_executing(&self) -> DurabilityResult<()> {
        self.ensure_active()?;
        if self.projection.status == DurableRunStatus::ReconcileRequired {
            return Err(DurabilityError::RunRequiresReconciliation);
        }
        if self.projection.status != DurableRunStatus::Running {
            return Err(DurabilityError::RunNotExecuting {
                status: self.projection.status,
            });
        }
        Ok(())
    }

    /// Replace the application-defined state projection with an append-only event.
    pub fn replace_state(
        &mut self,
        mutation_id: &str,
        state: Value,
    ) -> DurabilityResult<AppendOutcome> {
        validate_identifier("mutation_id", mutation_id)?;
        self.ensure_executing()?;
        self.emit(
            stable_identifier(
                "event",
                &[
                    &self.run_id,
                    &self.projection.branch_id,
                    "state",
                    mutation_id,
                ],
            ),
            RunEventKind::StateReplaced { state },
        )
    }

    /// Schedule or resume one stable logical activity.
    ///
    /// A completed activity is returned from the ledger and is never executed again. A running
    /// pure activity, or a running idempotent activity with a key, receives a new attempt after a
    /// crash. Any other ambiguous activity transitions the whole run to reconciliation-required.
    pub fn prepare_activity(
        &mut self,
        stable_step_id: &str,
        logical_key: &str,
        input: Value,
        side_effect_class: SideEffectClass,
        idempotency_key: Option<String>,
    ) -> DurabilityResult<ActivityDecision> {
        validate_identifier("stable_step_id", stable_step_id)?;
        validate_identifier("logical_key", logical_key)?;
        self.ensure_executing()?;
        let input_hash = stable_input_hash(&input);

        let existing_id = self
            .projection
            .activities
            .values()
            .find(|record| {
                record.definition.stable_step_id == stable_step_id
                    && record.definition.logical_key == logical_key
            })
            .map(|record| record.definition.activity_id.clone());

        let activity_id = if let Some(activity_id) = existing_id {
            let definition = &self.projection.activities[&activity_id].definition;
            if definition.input_hash != input_hash {
                return Err(DurabilityError::ActivityInputChanged {
                    activity_id,
                    stable_step_id: stable_step_id.to_string(),
                });
            }
            if definition.side_effect_class != side_effect_class
                || definition.idempotency_key != idempotency_key
            {
                return Err(DurabilityError::InvalidActivityTransition {
                    activity_id,
                    reason: "retry contract changed for an existing stable activity".into(),
                });
            }
            activity_id
        } else {
            let activity_id = stable_identifier(
                "activity",
                &[
                    &self.run_id,
                    &self.projection.branch_id,
                    stable_step_id,
                    logical_key,
                ],
            );
            if side_effect_class == SideEffectClass::Idempotent
                && idempotency_key.as_deref().is_none_or(str::is_empty)
            {
                return Err(DurabilityError::MissingIdempotencyKey {
                    activity_id: activity_id.clone(),
                });
            }
            let definition = ActivityDefinition {
                activity_id: activity_id.clone(),
                stable_step_id: stable_step_id.to_string(),
                logical_key: logical_key.to_string(),
                input,
                input_hash,
                side_effect_class,
                idempotency_key,
            };
            self.emit(
                stable_identifier("event", &[&activity_id, "scheduled"]),
                RunEventKind::ActivityScheduled { definition },
            )?;
            activity_id
        };

        self.decide_activity(&activity_id)
    }

    fn decide_activity(&mut self, activity_id: &str) -> DurabilityResult<ActivityDecision> {
        let record = self
            .projection
            .activities
            .get(activity_id)
            .cloned()
            .ok_or_else(|| DurabilityError::ActivityNotFound {
                activity_id: activity_id.to_string(),
            })?;

        if let Some(output) = record.completed_output() {
            return Ok(ActivityDecision::ReuseCompleted {
                activity_id: activity_id.to_string(),
                output: output.clone(),
            });
        }

        if let Some(latest) = record.latest_attempt() {
            match latest.status {
                ActivityAttemptStatus::Running => {
                    if retry_is_safe(&record.definition, true) {
                        self.emit(
                            stable_identifier(
                                "event",
                                &[activity_id, &latest.attempt.to_string(), "interrupted"],
                            ),
                            RunEventKind::ActivityAttemptFailed {
                                activity_id: activity_id.to_string(),
                                attempt: latest.attempt,
                                error: "worker interrupted before completion was committed".into(),
                                retryable: true,
                                effect_ambiguous: true,
                            },
                        )?;
                    } else {
                        let reason =
                            "previous attempt may have produced an uncommitted external effect"
                                .to_string();
                        self.emit(
                            stable_identifier(
                                "event",
                                &[
                                    activity_id,
                                    &latest.attempt.to_string(),
                                    "reconcile_required",
                                ],
                            ),
                            RunEventKind::ActivityReconciliationRequired {
                                activity_id: activity_id.to_string(),
                                attempt: latest.attempt,
                                reason: reason.clone(),
                            },
                        )?;
                        return Ok(ActivityDecision::ReconcileRequired {
                            activity_id: activity_id.to_string(),
                            reason,
                        });
                    }
                }
                ActivityAttemptStatus::Failed => {
                    if !latest.retryable {
                        return Ok(ActivityDecision::Failed {
                            activity_id: activity_id.to_string(),
                            error: latest
                                .error
                                .clone()
                                .unwrap_or_else(|| "activity failed".into()),
                        });
                    }
                    if !retry_is_safe(&record.definition, latest.effect_ambiguous) {
                        let reason = "failed attempt has an ambiguous external effect".to_string();
                        self.emit(
                            stable_identifier(
                                "event",
                                &[
                                    activity_id,
                                    &latest.attempt.to_string(),
                                    "reconcile_required",
                                ],
                            ),
                            RunEventKind::ActivityReconciliationRequired {
                                activity_id: activity_id.to_string(),
                                attempt: latest.attempt,
                                reason: reason.clone(),
                            },
                        )?;
                        return Ok(ActivityDecision::ReconcileRequired {
                            activity_id: activity_id.to_string(),
                            reason,
                        });
                    }
                }
                ActivityAttemptStatus::ReconcileRequired => {
                    return Ok(ActivityDecision::ReconcileRequired {
                        activity_id: activity_id.to_string(),
                        reason: latest.error.clone().unwrap_or_else(|| {
                            "external side effect requires reconciliation".into()
                        }),
                    });
                }
                ActivityAttemptStatus::Cancelled => {
                    return Ok(ActivityDecision::Cancelled {
                        activity_id: activity_id.to_string(),
                    });
                }
                ActivityAttemptStatus::Completed => unreachable!("handled above"),
            }
        }

        let attempt = self.projection.activities[activity_id]
            .attempts
            .len()
            .checked_add(1)
            .and_then(|value| u32::try_from(value).ok())
            .ok_or_else(|| DurabilityError::InvalidActivityTransition {
                activity_id: activity_id.to_string(),
                reason: "activity attempt counter overflowed".into(),
            })?;
        self.emit(
            stable_identifier("event", &[activity_id, &attempt.to_string(), "started"]),
            RunEventKind::ActivityAttemptStarted {
                activity_id: activity_id.to_string(),
                attempt,
            },
        )?;
        Ok(ActivityDecision::Execute {
            activity_id: activity_id.to_string(),
            attempt,
            idempotency_key: self.projection.activities[activity_id]
                .definition
                .idempotency_key
                .clone(),
        })
    }

    /// Commit a completed activity attempt. Retrying the same commit is idempotent.
    pub fn complete_activity(
        &mut self,
        activity_id: &str,
        attempt: u32,
        output: Value,
    ) -> DurabilityResult<AppendOutcome> {
        self.ensure_active()?;
        let record = self.activity(activity_id)?;
        if let Some(existing) = record
            .attempts
            .iter()
            .find(|existing| existing.attempt == attempt)
        {
            if existing.status == ActivityAttemptStatus::Completed {
                let output_hash = stable_input_hash(&output);
                if existing.output_hash.as_deref() == Some(output_hash.as_str()) {
                    return Ok(AppendOutcome::Deduplicated {
                        sequence: existing
                            .finished_sequence
                            .unwrap_or(existing.started_sequence),
                    });
                }
                return Err(DurabilityError::DuplicateEventConflict {
                    event_id: stable_identifier(
                        "event",
                        &[activity_id, &attempt.to_string(), "completed"],
                    ),
                });
            }
        }
        let output_hash = stable_input_hash(&output);
        self.emit(
            stable_identifier("event", &[activity_id, &attempt.to_string(), "completed"]),
            RunEventKind::ActivityAttemptCompleted {
                activity_id: activity_id.to_string(),
                attempt,
                output,
                output_hash,
            },
        )
    }

    /// Commit a failed activity attempt and fail closed if its external effect is ambiguous.
    pub fn fail_activity(
        &mut self,
        activity_id: &str,
        attempt: u32,
        error: impl Into<String>,
        retryable: bool,
        effect_ambiguous: bool,
    ) -> DurabilityResult<ActivityDecision> {
        self.ensure_active()?;
        let error = error.into();
        if is_reserved_audit_activity(self.activity(activity_id)?) {
            let reason = format!("reserved audit activity requires reconciliation: {error}");
            self.emit(
                stable_identifier(
                    "event",
                    &[activity_id, &attempt.to_string(), "reconcile_required"],
                ),
                RunEventKind::ActivityReconciliationRequired {
                    activity_id: activity_id.to_string(),
                    attempt,
                    reason: reason.clone(),
                },
            )?;
            return Ok(ActivityDecision::ReconcileRequired {
                activity_id: activity_id.to_string(),
                reason,
            });
        }
        self.emit(
            stable_identifier("event", &[activity_id, &attempt.to_string(), "failed"]),
            RunEventKind::ActivityAttemptFailed {
                activity_id: activity_id.to_string(),
                attempt,
                error: error.clone(),
                retryable,
                effect_ambiguous,
            },
        )?;
        let definition = &self.activity(activity_id)?.definition;
        if effect_ambiguous && !retry_is_safe(definition, true) {
            let reason = format!("activity failed ambiguously: {error}");
            self.emit(
                stable_identifier(
                    "event",
                    &[activity_id, &attempt.to_string(), "reconcile_required"],
                ),
                RunEventKind::ActivityReconciliationRequired {
                    activity_id: activity_id.to_string(),
                    attempt,
                    reason: reason.clone(),
                },
            )?;
            return Ok(ActivityDecision::ReconcileRequired {
                activity_id: activity_id.to_string(),
                reason,
            });
        }
        if retryable {
            self.decide_activity(activity_id)
        } else {
            Ok(ActivityDecision::Failed {
                activity_id: activity_id.to_string(),
                error,
            })
        }
    }

    /// Commit an unambiguous activity cancellation without misclassifying it as a failure.
    ///
    /// A cancellation reported as effect-ambiguous keeps the existing fail-closed retry and
    /// reconciliation rules. Reserved audit activities also remain reconciliation-only.
    pub fn cancel_activity(
        &mut self,
        activity_id: &str,
        attempt: u32,
        reason: impl Into<String>,
        effect_ambiguous: bool,
    ) -> DurabilityResult<ActivityDecision> {
        self.ensure_active()?;
        let reason = reason.into();
        if effect_ambiguous || is_reserved_audit_activity(self.activity(activity_id)?) {
            return self.fail_activity(activity_id, attempt, reason, false, effect_ambiguous);
        }
        self.emit(
            stable_identifier("event", &[activity_id, &attempt.to_string(), "cancelled"]),
            RunEventKind::ActivityAttemptCancelled {
                activity_id: activity_id.to_string(),
                attempt,
                reason,
            },
        )?;
        Ok(ActivityDecision::Cancelled {
            activity_id: activity_id.to_string(),
        })
    }

    /// Quarantine a reserved schedule that was persisted without its first attempt.
    ///
    /// This transition remains valid while paused or already reconciling, so an incomplete raw
    /// prefix cannot strand the driver before an operator-visible reconciliation record exists.
    pub(crate) fn quarantine_unstarted_reserved_activity(
        &mut self,
        activity_id: &str,
        reason: impl Into<String>,
    ) -> DurabilityResult<()> {
        let record = self.activity(activity_id)?;
        if !is_reserved_audit_activity(record) || !record.attempts.is_empty() {
            return Err(invalid_activity(
                activity_id,
                "only an unstarted reserved audit activity can be quarantined".into(),
            ));
        }
        let terminal_legacy_orphan = self.projection.status.is_terminal()
            && record.definition.stable_step_id == RUNTIME_LEGACY_RUN_STOPPED_AUDIT_STEP_ID;
        if !terminal_legacy_orphan {
            self.ensure_active()?;
        }
        let reason = reason.into();
        self.emit(
            stable_identifier("event", &[activity_id, "1", "orphan_reconcile_required"]),
            RunEventKind::ActivityReconciliationRequired {
                activity_id: activity_id.to_string(),
                attempt: 1,
                reason,
            },
        )?;
        Ok(())
    }

    /// Quarantine an unsafe activity attempt that only a migrated v1 terminal history can carry.
    ///
    /// Current-schema terminal transitions reject running activities. Requiring both the exact
    /// v1 schedule and v1 attempt-start provenance keeps this exception migration-only while
    /// allowing an operator to reconcile an otherwise permanently wedged legacy run.
    pub(crate) fn quarantine_terminal_legacy_running_activity(
        &mut self,
        activity_id: &str,
        reason: impl Into<String>,
    ) -> DurabilityResult<()> {
        if !self.projection.status.is_terminal() {
            return Err(invalid_activity(
                activity_id,
                "legacy terminal quarantine requires a terminal run".into(),
            ));
        }
        let record = self.activity(activity_id)?;
        let attempt = record
            .latest_attempt()
            .ok_or_else(|| invalid_activity(activity_id, "activity has not started".into()))?;
        if is_reserved_audit_activity(record)
            || record.definition.side_effect_class != SideEffectClass::ReconcileRequired
            || attempt.status != ActivityAttemptStatus::Running
            || !self.activity_attempt_has_v1_provenance(activity_id, attempt.attempt)
        {
            return Err(invalid_activity(
                activity_id,
                "only a v1-provenanced terminal reconciliation-required running attempt can be quarantined"
                    .into(),
            ));
        }
        let attempt = attempt.attempt;
        let terminal_status = self.projection.status;
        self.emit(
            stable_identifier(
                "event",
                &[
                    activity_id,
                    &attempt.to_string(),
                    "legacy_terminal_quarantine",
                ],
            ),
            RunEventKind::ActivityReconciliationRequired {
                activity_id: activity_id.to_string(),
                attempt,
                reason: reason.into(),
            },
        )?;
        debug_assert_eq!(self.projection.status, terminal_status);
        Ok(())
    }

    /// Record an explicit operator/integration reconciliation.
    pub fn reconcile_activity(
        &mut self,
        reconciliation_id: &str,
        activity_id: &str,
        resolution: ActivityReconciliation,
    ) -> DurabilityResult<AppendOutcome> {
        validate_identifier("reconciliation_id", reconciliation_id)?;
        let record = self.activity(activity_id)?.clone();
        let lifecycle_schedule_sequence = (record.definition.stable_step_id
            == RUNTIME_INVOCATION_LIFECYCLE_STEP_ID)
            .then(|| {
                self.events.iter().find_map(|event| match &event.kind {
                    RunEventKind::ActivityScheduled { definition }
                        if definition.activity_id == activity_id =>
                    {
                        Some(event.sequence)
                    }
                    _ => None,
                })
            })
            .flatten();
        validate_reserved_audit_reconciliation(
            &self.run_id,
            &self.projection.activities,
            &record,
            &resolution,
            lifecycle_schedule_sequence,
        )?;
        let attempt = record
            .latest_attempt()
            .ok_or_else(|| DurabilityError::InvalidActivityTransition {
                activity_id: activity_id.to_string(),
                reason: "activity has not started".into(),
            })?
            .attempt;
        self.emit(
            stable_identifier(
                "event",
                &[
                    activity_id,
                    &attempt.to_string(),
                    "reconciled",
                    reconciliation_id,
                ],
            ),
            RunEventKind::ActivityReconciled {
                activity_id: activity_id.to_string(),
                attempt,
                resolution,
            },
        )
    }

    pub fn activity(&self, activity_id: &str) -> DurabilityResult<&ActivityRecord> {
        self.projection.activities.get(activity_id).ok_or_else(|| {
            DurabilityError::ActivityNotFound {
                activity_id: activity_id.to_string(),
            }
        })
    }

    /// Persist a human/policy approval request and pause the run.
    pub fn request_approval(
        &mut self,
        logical_key: &str,
        activity_id: Option<String>,
        prompt: impl Into<String>,
        payload: Value,
    ) -> DurabilityResult<String> {
        validate_identifier("logical_key", logical_key)?;
        self.ensure_active()?;
        if self.projection.status == DurableRunStatus::ReconcileRequired {
            return Err(DurabilityError::RunRequiresReconciliation);
        }
        if let Some(activity_id) = activity_id.as_deref() {
            self.activity(activity_id)?;
        }
        let approval_id = stable_identifier(
            "approval",
            &[&self.run_id, &self.projection.branch_id, logical_key],
        );
        let kind = RunEventKind::ApprovalRequested {
            approval_id: approval_id.clone(),
            logical_key: logical_key.to_string(),
            activity_id,
            prompt: prompt.into(),
            payload,
        };
        self.emit(
            stable_identifier("event", &[&approval_id, "requested"]),
            kind,
        )?;
        Ok(approval_id)
    }

    /// Persist a typed approval whose deadline and governing policy survive restart/replay.
    pub fn request_typed_approval(
        &mut self,
        mut request: DurableApprovalRequest,
    ) -> DurabilityResult<String> {
        validate_identifier("logical_key", &request.logical_key)?;
        if request.prompt.trim().is_empty() {
            return Err(DurabilityError::InvalidEvent {
                reason: "approval prompt cannot be empty".into(),
            });
        }
        if request.expires_at_unix_ms <= request.requested_at_unix_ms {
            return Err(DurabilityError::InvalidEvent {
                reason: "approval expiry must be after its request time".into(),
            });
        }
        if self.policy_snapshot_hash.is_some() && self.governance_binding.is_none() {
            return Err(DurabilityError::InvalidEvent {
                reason: "legacy hash-only governed runs are read-only for new approvals".into(),
            });
        }
        if request.governance_binding.is_none() {
            request.governance_binding = self.governance_binding.clone();
        }
        match (&self.policy_snapshot_hash, &request.policy_snapshot_hash) {
            (Some(expected), Some(actual)) if expected == actual => {}
            (None, None) => {}
            (Some(_), None) => {
                return Err(DurabilityError::InvalidEvent {
                    reason: "governed approval is missing its policy snapshot hash".into(),
                });
            }
            (expected, actual) => {
                return Err(DurabilityError::InvalidEvent {
                    reason: format!(
                        "approval policy snapshot does not match run: expected {expected:?}, got {actual:?}"
                    ),
                });
            }
        }
        match (&self.governance_binding, &request.governance_binding) {
            (Some(expected), Some(actual)) if expected == actual => {}
            (None, None) => {}
            (Some(_), None) => {
                return Err(DurabilityError::InvalidEvent {
                    reason: "governed approval is missing its complete governance binding".into(),
                });
            }
            _ => {
                return Err(DurabilityError::InvalidEvent {
                    reason: "approval governance binding does not match run".into(),
                });
            }
        }
        if let Some(binding) = request.governance_binding.as_ref() {
            binding
                .validate()
                .map_err(|error| DurabilityError::InvalidEvent {
                    reason: format!("invalid approval governance binding: {error}"),
                })?;
            if binding.run_id() != self.run_id
                || request.policy_snapshot_hash.as_deref() != Some(binding.policy_snapshot_hash())
            {
                return Err(DurabilityError::InvalidEvent {
                    reason: "approval governance binding identity is inconsistent".into(),
                });
            }
        }
        if let Some(hash) = request.policy_snapshot_hash.as_deref() {
            validate_policy_hash(hash)?;
        }
        let envelope = DurableApprovalEnvelope {
            schema_version: DURABILITY_SCHEMA_VERSION,
            kind: request.kind,
            payload: request.payload,
            policy_snapshot_hash: request.policy_snapshot_hash,
            governance_binding: request.governance_binding,
            requested_at_unix_ms: request.requested_at_unix_ms,
            expires_at_unix_ms: request.expires_at_unix_ms,
        };
        let payload = serde_json::json!({ DURABLE_APPROVAL_ENVELOPE_KEY: envelope });
        self.request_approval(
            &request.logical_key,
            request.activity_id,
            request.prompt,
            payload,
        )
    }

    /// Persist timeout denials without requiring every other pending approval to be resolved.
    /// Repeating the same sweep is idempotent because already-resolved approvals are skipped.
    pub fn expire_approvals(
        &mut self,
        expiration_id: &str,
        now_unix_ms: u64,
    ) -> DurabilityResult<Vec<String>> {
        validate_identifier("expiration_id", expiration_id)?;
        self.ensure_active()?;
        let expired_ids = self
            .projection
            .approvals
            .values()
            .filter(|approval| {
                approval.status == DurableApprovalStatus::Pending
                    && approval
                        .expires_at_unix_ms
                        .is_some_and(|expires_at| now_unix_ms >= expires_at)
            })
            .map(|approval| approval.approval_id.clone())
            .collect::<Vec<_>>();
        let mut candidate = self.clone();
        for approval_id in &expired_ids {
            candidate.resolve_approval(
                expiration_id,
                ApprovalResolution {
                    approval_id: approval_id.clone(),
                    approved: false,
                    response: Some(serde_json::json!({"reason": "approval_timeout"})),
                },
                Some(now_unix_ms),
                true,
            )?;
        }
        *self = candidate;
        Ok(expired_ids)
    }

    /// Publish the next metadata version for an artifact.
    pub fn publish_artifact(
        &mut self,
        artifact_id: &str,
        media_type: impl Into<String>,
        size_bytes: u64,
        content_hash: impl Into<String>,
        created_by_activity_id: Option<String>,
    ) -> DurabilityResult<ArtifactMetadata> {
        validate_identifier("artifact_id", artifact_id)?;
        self.ensure_active()?;
        if let Some(activity_id) = created_by_activity_id.as_deref() {
            self.activity(activity_id)?;
        }
        let prior = self
            .projection
            .artifacts
            .get(artifact_id)
            .and_then(|versions| versions.last());
        let version = prior.map_or(1, |metadata| metadata.version.saturating_add(1));
        let version_id = stable_identifier(
            "artifact_version",
            &[
                &self.run_id,
                &self.projection.branch_id,
                artifact_id,
                &version.to_string(),
            ],
        );
        let metadata = ArtifactMetadata {
            artifact_id: artifact_id.to_string(),
            version,
            version_id: version_id.clone(),
            branch_id: self.projection.branch_id.clone(),
            media_type: media_type.into(),
            size_bytes,
            content_hash: content_hash.into(),
            created_by_activity_id,
            previous_version_id: prior.map(|metadata| metadata.version_id.clone()),
        };
        self.emit(
            stable_identifier("event", &[&version_id, "published"]),
            RunEventKind::ArtifactPublished {
                metadata: metadata.clone(),
            },
        )?;
        Ok(metadata)
    }

    /// Commit a full projection snapshot at the current event boundary.
    pub fn checkpoint(
        &mut self,
        checkpoint_key: &str,
        label: Option<String>,
    ) -> DurabilityResult<Checkpoint> {
        validate_identifier("checkpoint_key", checkpoint_key)?;
        self.ensure_active()?;
        if self.projection.status == DurableRunStatus::ReconcileRequired {
            return Err(DurabilityError::RunRequiresReconciliation);
        }
        let checkpoint_id = stable_identifier(
            "checkpoint",
            &[&self.run_id, &self.projection.branch_id, checkpoint_key],
        );
        self.emit(
            stable_identifier("event", &[&checkpoint_id, "committed"]),
            RunEventKind::CheckpointCommitted {
                checkpoint_id: checkpoint_id.clone(),
                label,
            },
        )?;
        Ok(self.checkpoints[&checkpoint_id].clone())
    }

    pub fn pause(&mut self, pause_id: &str, reason: impl Into<String>) -> DurabilityResult<()> {
        validate_identifier("pause_id", pause_id)?;
        self.ensure_active()?;
        if self.projection.status == DurableRunStatus::ReconcileRequired {
            return Err(DurabilityError::RunRequiresReconciliation);
        }
        self.emit(
            stable_identifier("event", &[&self.run_id, "pause", pause_id]),
            RunEventKind::RunPaused {
                reason: reason.into(),
            },
        )?;
        Ok(())
    }

    pub fn complete_run(&mut self, completion_id: &str) -> DurabilityResult<AppendOutcome> {
        validate_identifier("completion_id", completion_id)?;
        self.ensure_active()?;
        self.ensure_reserved_audit_lifecycle_closed()?;
        self.emit(
            stable_identifier("event", &[&self.run_id, "complete", completion_id]),
            RunEventKind::RunCompleted,
        )
    }

    pub fn fail_run(
        &mut self,
        failure_id: &str,
        error: impl Into<String>,
    ) -> DurabilityResult<AppendOutcome> {
        validate_identifier("failure_id", failure_id)?;
        self.ensure_active()?;
        self.ensure_reserved_audit_lifecycle_closed()?;
        self.emit(
            stable_identifier("event", &[&self.run_id, "fail", failure_id]),
            RunEventKind::RunFailed {
                error: error.into(),
            },
        )
    }

    /// Apply resume/fork/rewind/cancel atomically to this in-memory state.
    pub fn apply_command(&mut self, command: RunCommand) -> DurabilityResult<CommandOutcome> {
        match command {
            RunCommand::Resume {
                command_id,
                approvals,
            } => self.resume(&command_id, approvals, None),
            RunCommand::Fork {
                command_id,
                new_run_id,
                checkpoint_id,
                side_effects_reconciled,
            } => self.fork(
                &command_id,
                &new_run_id,
                &checkpoint_id,
                side_effects_reconciled,
            ),
            RunCommand::Rewind {
                command_id,
                checkpoint_id,
                side_effects_reconciled,
            } => self.rewind(&command_id, &checkpoint_id, side_effects_reconciled),
            RunCommand::Cancel { command_id, reason } => self.cancel(&command_id, reason),
        }
    }

    /// Apply a command with a caller-supplied trusted wall-clock value. Typed approvals require
    /// this path so an expired response cannot become valid merely because a worker restarted.
    pub fn apply_command_at(
        &mut self,
        command: RunCommand,
        now_unix_ms: u64,
    ) -> DurabilityResult<CommandOutcome> {
        match command {
            RunCommand::Resume {
                command_id,
                approvals,
            } => self.resume(&command_id, approvals, Some(now_unix_ms)),
            command => self.apply_command(command),
        }
    }

    fn resume(
        &mut self,
        command_id: &str,
        approvals: Vec<ApprovalResolution>,
        now_unix_ms: Option<u64>,
    ) -> DurabilityResult<CommandOutcome> {
        validate_identifier("command_id", command_id)?;
        let event_id = stable_identifier("event", &[&self.run_id, "resume", command_id]);
        if let Some(event) = self.event_by_id(&event_id) {
            if event.kind == RunEventKind::RunResumed {
                return Ok(CommandOutcome::Resumed {
                    sequence: event.sequence,
                });
            }
            return Err(DurabilityError::DuplicateEventConflict { event_id });
        }
        if self.projection.status == DurableRunStatus::ReconcileRequired {
            return Err(DurabilityError::RunRequiresReconciliation);
        }
        if self.projection.status != DurableRunStatus::Paused {
            return Err(DurabilityError::NotPaused);
        }

        let mut candidate = self.clone();
        let typed_pending = candidate
            .projection
            .approvals
            .values()
            .find(|approval| {
                approval.status == DurableApprovalStatus::Pending
                    && approval.expires_at_unix_ms.is_some()
            })
            .map(|approval| approval.approval_id.clone());
        if let (Some(approval_id), None) = (typed_pending, now_unix_ms) {
            return Err(DurabilityError::ApprovalClockRequired { approval_id });
        }

        let mut expired = BTreeSet::new();
        if let Some(now_unix_ms) = now_unix_ms {
            let expired_ids = candidate
                .projection
                .approvals
                .values()
                .filter(|approval| {
                    approval.status == DurableApprovalStatus::Pending
                        && approval
                            .expires_at_unix_ms
                            .is_some_and(|expires_at| now_unix_ms >= expires_at)
                })
                .map(|approval| approval.approval_id.clone())
                .collect::<Vec<_>>();
            for approval_id in expired_ids {
                expired.insert(approval_id.clone());
                candidate.resolve_approval(
                    command_id,
                    ApprovalResolution {
                        approval_id,
                        approved: false,
                        response: Some(serde_json::json!({"reason": "approval_timeout"})),
                    },
                    Some(now_unix_ms),
                    true,
                )?;
            }
        }
        for resolution in approvals {
            if expired.contains(&resolution.approval_id) {
                continue;
            }
            candidate.resolve_approval(command_id, resolution, now_unix_ms, false)?;
        }
        if !candidate.projection.pending_approval_ids().is_empty() {
            return Err(DurabilityError::PendingApprovals);
        }
        let outcome = candidate.emit(event_id, RunEventKind::RunResumed)?;
        let sequence = append_sequence(outcome);
        *self = candidate;
        Ok(CommandOutcome::Resumed { sequence })
    }

    fn resolve_approval(
        &mut self,
        command_id: &str,
        resolution: ApprovalResolution,
        now_unix_ms: Option<u64>,
        timed_out: bool,
    ) -> DurabilityResult<()> {
        let approval = self
            .projection
            .approvals
            .get(&resolution.approval_id)
            .ok_or_else(|| DurabilityError::ApprovalNotFound {
                approval_id: resolution.approval_id.clone(),
            })?;
        if approval.status != DurableApprovalStatus::Pending {
            return Err(DurabilityError::ApprovalAlreadyResolved {
                approval_id: resolution.approval_id,
            });
        }
        validate_approval_resolution(approval, &resolution, now_unix_ms, timed_out)?;
        let event_id =
            stable_identifier("event", &[&resolution.approval_id, "resolved", command_id]);
        let response = match now_unix_ms {
            Some(resolved_at_unix_ms) if approval.expires_at_unix_ms.is_some() => {
                Some(serde_json::json!({
                    DURABLE_RESOLUTION_ENVELOPE_KEY: DurableResolutionEnvelope {
                        schema_version: DURABILITY_SCHEMA_VERSION,
                        resolved_at_unix_ms,
                        timed_out,
                        response: resolution.response,
                    }
                }))
            }
            _ => resolution.response,
        };
        self.emit(
            event_id,
            RunEventKind::ApprovalResolved {
                approval_id: resolution.approval_id,
                approved: resolution.approved,
                response,
            },
        )?;
        Ok(())
    }

    fn fork(
        &mut self,
        command_id: &str,
        new_run_id: &str,
        checkpoint_id: &str,
        side_effects_reconciled: bool,
    ) -> DurabilityResult<CommandOutcome> {
        validate_identifier("command_id", command_id)?;
        validate_identifier("new_run_id", new_run_id)?;
        if new_run_id == self.run_id {
            return Err(DurabilityError::InvalidEvent {
                reason: "fork destination must differ from its source run".into(),
            });
        }
        let checkpoint = self.checkpoint_by_id(checkpoint_id)?.clone();
        let checkpoint = self.checkpoint_for_current_run(checkpoint)?;
        let new_branch_id = stable_identifier("branch", &[new_run_id, "fork", command_id]);
        let event_id = stable_identifier("event", &[&self.run_id, "fork", command_id]);
        if let Some(event) = self.event_by_id(&event_id) {
            if let RunEventKind::ForkCreated {
                new_run_id: existing_run_id,
                checkpoint_id: existing_checkpoint_id,
                new_branch_id: existing_branch_id,
                side_effects_reconciled: existing_side_effects_reconciled,
            } = &event.kind
            {
                if existing_run_id == new_run_id
                    && existing_checkpoint_id == checkpoint_id
                    && *existing_side_effects_reconciled == side_effects_reconciled
                {
                    return Ok(CommandOutcome::Forked {
                        run: Box::new(self.build_fork(
                            new_run_id,
                            checkpoint,
                            existing_branch_id.clone(),
                            command_id,
                        )?),
                    });
                }
            }
            return Err(DurabilityError::DuplicateEventConflict { event_id });
        }
        self.require_reconciled_after(checkpoint.event_sequence, side_effects_reconciled)?;
        let mut candidate = self.clone();
        candidate.emit(
            event_id,
            RunEventKind::ForkCreated {
                new_run_id: new_run_id.to_string(),
                checkpoint_id: checkpoint_id.to_string(),
                new_branch_id: new_branch_id.clone(),
                side_effects_reconciled,
            },
        )?;
        let forked = candidate.build_fork(new_run_id, checkpoint, new_branch_id, command_id)?;
        *self = candidate;
        Ok(CommandOutcome::Forked {
            run: Box::new(forked),
        })
    }

    fn build_fork(
        &self,
        new_run_id: &str,
        checkpoint: Checkpoint,
        new_branch_id: String,
        command_id: &str,
    ) -> DurabilityResult<RunState> {
        let projection = fork_projection(&checkpoint, new_run_id, &new_branch_id);
        let mut forked = RunState::blank(
            self.session_id.clone(),
            new_run_id.to_string(),
            self.durability,
            projection,
        );
        let kind = RunEventKind::ForkedFrom {
            session_id: self.session_id.clone(),
            source_run_id: self.run_id.clone(),
            durability: self.durability,
            source_checkpoint: Box::new(checkpoint),
            new_branch_id,
        };
        forked.emit(
            stable_identifier("event", &[new_run_id, "forked_from", command_id]),
            kind,
        )?;
        if let Some(binding) = &self.governance_binding {
            let binding =
                binding
                    .for_run(new_run_id)
                    .map_err(|error| DurabilityError::InvalidEvent {
                        reason: format!("cannot bind fork governance identity: {error}"),
                    })?;
            forked.emit(
                stable_identifier("event", &[new_run_id, "governance_binding_pinned"]),
                RunEventKind::GovernanceBindingPinned { binding },
            )?;
        } else if let Some(policy_snapshot_hash) = &self.policy_snapshot_hash {
            forked.emit(
                stable_identifier("event", &[new_run_id, "policy_snapshot_pinned"]),
                RunEventKind::PolicySnapshotPinned {
                    policy_snapshot_hash: policy_snapshot_hash.clone(),
                },
            )?;
        }
        Ok(forked)
    }

    fn checkpoint_for_current_run(
        &self,
        mut checkpoint: Checkpoint,
    ) -> DurabilityResult<Checkpoint> {
        if checkpoint.run_id == self.run_id {
            return Ok(checkpoint);
        }
        let Some(RunEvent {
            sequence,
            kind:
                RunEventKind::ForkedFrom {
                    source_checkpoint,
                    new_branch_id,
                    ..
                },
            ..
        }) = self.events.first()
        else {
            return Err(DurabilityError::InvalidEvent {
                reason: "inherited checkpoint has no fork lineage in the current run".into(),
            });
        };
        if source_checkpoint.checkpoint_id != checkpoint.checkpoint_id
            || source_checkpoint.run_id != checkpoint.run_id
        {
            return Err(DurabilityError::InvalidEvent {
                reason: "inherited checkpoint does not match the current run's fork lineage".into(),
            });
        }
        checkpoint.projection = fork_projection(&checkpoint, &self.run_id, new_branch_id);
        checkpoint.run_id = self.run_id.clone();
        checkpoint.event_sequence = *sequence;
        checkpoint.parent_checkpoint_id = None;
        Ok(checkpoint)
    }

    fn rewind(
        &mut self,
        command_id: &str,
        checkpoint_id: &str,
        side_effects_reconciled: bool,
    ) -> DurabilityResult<CommandOutcome> {
        validate_identifier("command_id", command_id)?;
        let checkpoint = self.checkpoint_by_id(checkpoint_id)?.clone();
        let checkpoint = self.checkpoint_for_current_run(checkpoint)?;
        let new_branch_id = stable_identifier(
            "branch",
            &[&self.run_id, "rewind", command_id, checkpoint_id],
        );
        let event_id = stable_identifier("event", &[&self.run_id, "rewind", command_id]);
        if let Some(event) = self.event_by_id(&event_id) {
            if let RunEventKind::RunRewound {
                checkpoint_id: existing_checkpoint_id,
                side_effects_reconciled: existing_side_effects_reconciled,
                ..
            } = &event.kind
            {
                if existing_checkpoint_id == checkpoint_id
                    && *existing_side_effects_reconciled == side_effects_reconciled
                {
                    return Ok(CommandOutcome::Rewound {
                        checkpoint_id: checkpoint_id.to_string(),
                        sequence: event.sequence,
                    });
                }
            }
            return Err(DurabilityError::DuplicateEventConflict { event_id });
        }
        self.require_reconciled_after(checkpoint.event_sequence, side_effects_reconciled)?;
        let outcome = self.emit(
            event_id,
            RunEventKind::RunRewound {
                checkpoint_id: checkpoint_id.to_string(),
                new_branch_id,
                side_effects_reconciled,
            },
        )?;
        Ok(CommandOutcome::Rewound {
            checkpoint_id: checkpoint_id.to_string(),
            sequence: append_sequence(outcome),
        })
    }

    fn cancel(
        &mut self,
        command_id: &str,
        reason: Option<String>,
    ) -> DurabilityResult<CommandOutcome> {
        validate_identifier("command_id", command_id)?;
        let event_id = stable_identifier("event", &[&self.run_id, "cancel", command_id]);
        if let Some(event) = self.event_by_id(&event_id) {
            if let RunEventKind::RunCancelled {
                reason: existing_reason,
            } = &event.kind
            {
                if existing_reason == &reason {
                    return Ok(CommandOutcome::Cancelled {
                        sequence: event.sequence,
                    });
                }
            }
            return Err(DurabilityError::DuplicateEventConflict { event_id });
        }
        self.ensure_active()?;
        self.ensure_reserved_audit_lifecycle_closed()?;
        let outcome = self.emit(event_id, RunEventKind::RunCancelled { reason })?;
        Ok(CommandOutcome::Cancelled {
            sequence: append_sequence(outcome),
        })
    }

    fn checkpoint_by_id(&self, checkpoint_id: &str) -> DurabilityResult<&Checkpoint> {
        self.checkpoints
            .get(checkpoint_id)
            .ok_or_else(|| DurabilityError::CheckpointNotFound {
                checkpoint_id: checkpoint_id.to_string(),
            })
    }

    fn event_by_id(&self, event_id: &str) -> Option<&RunEvent> {
        self.events.iter().find(|event| event.event_id == event_id)
    }

    fn require_reconciled_after(
        &self,
        event_sequence: u64,
        side_effects_reconciled: bool,
    ) -> DurabilityResult<()> {
        let mut unsafe_activities = BTreeSet::new();
        for event in self
            .events
            .iter()
            .filter(|event| event.sequence > event_sequence)
        {
            if let RunEventKind::ActivityAttemptStarted { activity_id, .. } = &event.kind {
                if self.activity_side_effect_class(activity_id)? != SideEffectClass::Pure {
                    unsafe_activities.insert(activity_id.clone());
                }
            }
        }
        if !unsafe_activities.is_empty() && !side_effects_reconciled {
            return Err(DurabilityError::ReconcileRequired {
                activity_ids: unsafe_activities.into_iter().collect(),
            });
        }
        Ok(())
    }

    fn activity_side_effect_class(&self, activity_id: &str) -> DurabilityResult<SideEffectClass> {
        let projected = self
            .projection
            .activities
            .get(activity_id)
            .map(|record| &record.definition);
        let mut scheduled = None;
        for event in &self.events {
            let RunEventKind::ActivityScheduled { definition } = &event.kind else {
                continue;
            };
            if definition.activity_id != activity_id {
                continue;
            }
            if scheduled.is_some_and(|existing| existing != definition) {
                return Err(DurabilityError::InvalidEvent {
                    reason: format!(
                        "activity `{activity_id}` has conflicting scheduled definitions"
                    ),
                });
            }
            scheduled = Some(definition);
        }
        if matches!((projected, scheduled), (Some(left), Some(right)) if left != right) {
            return Err(DurabilityError::InvalidEvent {
                reason: format!(
                    "activity `{activity_id}` projection conflicts with its scheduled definition"
                ),
            });
        }
        projected
            .or(scheduled)
            .map(|definition| definition.side_effect_class)
            .ok_or_else(|| DurabilityError::InvalidEvent {
                reason: format!(
                    "activity attempt `{activity_id}` has no durable activity definition"
                ),
            })
    }

    fn apply_event_in_place(&mut self, event: &RunEvent) -> DurabilityResult<()> {
        if self.events.is_empty() {
            match &event.kind {
                RunEventKind::RunStarted {
                    session_id,
                    durability,
                    root_branch_id,
                } => {
                    if session_id != &self.session_id {
                        return Err(DurabilityError::InvalidEvent {
                            reason: "run-start session does not match state".into(),
                        });
                    }
                    self.durability = *durability;
                    self.projection = RunProjection::root(&self.run_id);
                    self.projection.branch_id = root_branch_id.clone();
                    return Ok(());
                }
                RunEventKind::ForkedFrom {
                    session_id,
                    source_run_id,
                    durability,
                    source_checkpoint,
                    new_branch_id,
                } => {
                    if session_id != &self.session_id {
                        return Err(DurabilityError::InvalidEvent {
                            reason: "fork session does not match state".into(),
                        });
                    }
                    validate_identifier("source_run_id", source_run_id)?;
                    validate_identifier("new_branch_id", new_branch_id)?;
                    validate_source_checkpoint(&self.run_id, source_run_id, source_checkpoint)?;
                    self.parent_run_id = Some(source_run_id.clone());
                    self.durability = *durability;
                    self.projection =
                        fork_projection(source_checkpoint, &self.run_id, new_branch_id);
                    self.checkpoints.insert(
                        source_checkpoint.checkpoint_id.clone(),
                        (**source_checkpoint).clone(),
                    );
                    return Ok(());
                }
                _ => return Err(DurabilityError::MissingRunStart),
            }
        }

        if matches!(
            event.kind,
            RunEventKind::RunStarted { .. } | RunEventKind::ForkedFrom { .. }
        ) {
            return Err(DurabilityError::AlreadyStarted);
        }

        match &event.kind {
            RunEventKind::PolicySnapshotPinned {
                policy_snapshot_hash,
            } => {
                if self.events.len() != 1
                    || self.policy_snapshot_hash.is_some()
                    || self.governance_binding.is_some()
                {
                    return Err(DurabilityError::InvalidEvent {
                        reason: "policy snapshot must be pinned exactly once before run work"
                            .into(),
                    });
                }
                validate_policy_hash(policy_snapshot_hash)?;
                self.policy_snapshot_hash = Some(policy_snapshot_hash.clone());
            }
            RunEventKind::GovernanceBindingPinned { binding } => {
                if self.events.len() != 1
                    || self.policy_snapshot_hash.is_some()
                    || self.governance_binding.is_some()
                {
                    return Err(DurabilityError::InvalidEvent {
                        reason: "governance binding must be pinned exactly once before run work"
                            .into(),
                    });
                }
                binding
                    .validate()
                    .map_err(|error| DurabilityError::InvalidEvent {
                        reason: format!("invalid governance binding: {error}"),
                    })?;
                if binding.run_id() != self.run_id {
                    return Err(DurabilityError::InvalidEvent {
                        reason: "governance binding run_id does not match event run".into(),
                    });
                }
                self.policy_snapshot_hash = Some(binding.policy_snapshot_hash().to_owned());
                self.governance_binding = Some(binding.clone());
            }
            RunEventKind::StateReplaced { state } => {
                self.ensure_executing()?;
                self.projection.state = state.clone();
            }
            RunEventKind::WorkerLeaseClaimed {
                owner_id,
                lease_id,
                claimed_at_unix_ms,
                expires_at_unix_ms,
            } => {
                if event.schema_version < TERMINAL_ACTIVITY_INVARIANT_SCHEMA_VERSION {
                    return Err(DurabilityError::InvalidEvent {
                        reason: "distributed worker leases require durability schema v2".into(),
                    });
                }
                validate_identifier("worker_owner_id", owner_id)?;
                validate_identifier("worker_lease_id", lease_id)?;
                validate_worker_lease_window(*claimed_at_unix_ms, *expires_at_unix_ms)?;
                if let Some(existing) = &self.projection.worker_lease {
                    if existing.expires_at_unix_ms > *claimed_at_unix_ms {
                        return Err(DurabilityError::WorkerLeaseHeld {
                            owner_id: existing.owner_id.clone(),
                            expires_at_unix_ms: existing.expires_at_unix_ms,
                        });
                    }
                }
                self.projection.worker_lease = Some(DurableWorkerLease {
                    owner_id: owner_id.clone(),
                    lease_id: lease_id.clone(),
                    acquired_at_unix_ms: *claimed_at_unix_ms,
                    heartbeat_at_unix_ms: *claimed_at_unix_ms,
                    expires_at_unix_ms: *expires_at_unix_ms,
                });
            }
            RunEventKind::WorkerLeaseRenewed {
                owner_id,
                lease_id,
                renewed_at_unix_ms,
                expires_at_unix_ms,
            } => {
                if event.schema_version < TERMINAL_ACTIVITY_INVARIANT_SCHEMA_VERSION {
                    return Err(DurabilityError::InvalidEvent {
                        reason: "distributed worker leases require durability schema v2".into(),
                    });
                }
                validate_identifier("worker_owner_id", owner_id)?;
                validate_identifier("worker_lease_id", lease_id)?;
                validate_worker_lease_window(*renewed_at_unix_ms, *expires_at_unix_ms)?;
                let existing = self.projection.worker_lease.as_mut().ok_or_else(|| {
                    DurabilityError::WorkerLeaseLost {
                        owner_id: owner_id.clone(),
                    }
                })?;
                if existing.owner_id != *owner_id
                    || existing.lease_id != *lease_id
                    || existing.expires_at_unix_ms <= *renewed_at_unix_ms
                    || existing.heartbeat_at_unix_ms > *renewed_at_unix_ms
                    || *expires_at_unix_ms <= existing.expires_at_unix_ms
                {
                    return Err(DurabilityError::WorkerLeaseLost {
                        owner_id: owner_id.clone(),
                    });
                }
                existing.heartbeat_at_unix_ms = *renewed_at_unix_ms;
                existing.expires_at_unix_ms = *expires_at_unix_ms;
            }
            RunEventKind::WorkerLeaseReleased {
                owner_id,
                lease_id,
                released_at_unix_ms,
            } => {
                if event.schema_version < TERMINAL_ACTIVITY_INVARIANT_SCHEMA_VERSION {
                    return Err(DurabilityError::InvalidEvent {
                        reason: "distributed worker leases require durability schema v2".into(),
                    });
                }
                validate_identifier("worker_owner_id", owner_id)?;
                validate_identifier("worker_lease_id", lease_id)?;
                let existing = self.projection.worker_lease.as_ref().ok_or_else(|| {
                    DurabilityError::WorkerLeaseLost {
                        owner_id: owner_id.clone(),
                    }
                })?;
                if existing.owner_id != *owner_id
                    || existing.lease_id != *lease_id
                    || existing.expires_at_unix_ms <= *released_at_unix_ms
                    || *released_at_unix_ms < existing.heartbeat_at_unix_ms
                {
                    return Err(DurabilityError::WorkerLeaseLost {
                        owner_id: owner_id.clone(),
                    });
                }
                self.projection.worker_lease = None;
            }
            RunEventKind::ActivityScheduled { definition } => {
                self.ensure_executing()?;
                if stable_input_hash(&definition.input) != definition.input_hash {
                    return Err(DurabilityError::InvalidEvent {
                        reason: format!(
                            "activity `{}` input hash does not match input",
                            definition.activity_id
                        ),
                    });
                }
                if definition.side_effect_class == SideEffectClass::Idempotent
                    && definition
                        .idempotency_key
                        .as_deref()
                        .is_none_or(str::is_empty)
                {
                    return Err(DurabilityError::MissingIdempotencyKey {
                        activity_id: definition.activity_id.clone(),
                    });
                }
                if self
                    .projection
                    .activities
                    .contains_key(&definition.activity_id)
                {
                    return Err(DurabilityError::InvalidActivityTransition {
                        activity_id: definition.activity_id.clone(),
                        reason: "activity was scheduled twice".into(),
                    });
                }
                if event.schema_version < TERMINAL_ACTIVITY_INVARIANT_SCHEMA_VERSION {
                    if matches!(
                        definition.stable_step_id.as_str(),
                        RUNTIME_RUN_STOPPED_AUDIT_STEP_ID
                            | RUNTIME_RECOVERY_RUN_STOPPED_AUDIT_STEP_ID
                            | RUNTIME_INVOCATION_LIFECYCLE_STEP_ID
                    ) {
                        return Err(DurabilityError::InvalidEvent {
                            reason:
                                "v2 reserved audit activities cannot be introduced by a v1 event"
                                    .into(),
                        });
                    }
                    if definition.stable_step_id == RUNTIME_LEGACY_RUN_STOPPED_AUDIT_STEP_ID {
                        let expected_activity_id = stable_identifier(
                            "activity",
                            &[
                                &self.run_id,
                                &self.projection.branch_id,
                                RUNTIME_LEGACY_RUN_STOPPED_AUDIT_STEP_ID,
                                "terminal",
                            ],
                        );
                        let duplicate = self.projection.activities.values().any(|record| {
                            record.definition.stable_step_id
                                == RUNTIME_LEGACY_RUN_STOPPED_AUDIT_STEP_ID
                        });
                        if definition.activity_id != expected_activity_id
                            || definition.logical_key != "terminal"
                            || definition.side_effect_class != SideEffectClass::ReconcileRequired
                            || definition.idempotency_key.is_some()
                            || duplicate
                        {
                            return Err(DurabilityError::InvalidEvent {
                                reason: "legacy RunStopped audit activity has a non-canonical or duplicate definition"
                                    .into(),
                            });
                        }
                    }
                }
                if event.schema_version >= TERMINAL_ACTIVITY_INVARIANT_SCHEMA_VERSION {
                    let definition_is_reserved = matches!(
                        definition.stable_step_id.as_str(),
                        RUNTIME_RUN_STOPPED_AUDIT_STEP_ID
                            | RUNTIME_RECOVERY_RUN_STOPPED_AUDIT_STEP_ID
                            | RUNTIME_INVOCATION_LIFECYCLE_STEP_ID
                    );
                    if !definition_is_reserved
                        && self.projection.activities.values().any(|record| {
                            matches!(
                                record.definition.stable_step_id.as_str(),
                                RUNTIME_RUN_STOPPED_AUDIT_STEP_ID
                                    | RUNTIME_RECOVERY_RUN_STOPPED_AUDIT_STEP_ID
                            ) && is_reserved_audit_activity(record)
                        })
                    {
                        return Err(DurabilityError::InvalidEvent {
                            reason:
                                "ordinary work cannot be scheduled after a terminal audit marker"
                                    .into(),
                        });
                    }
                    validate_reserved_audit_schedule(
                        &self.run_id,
                        &self.projection.branch_id,
                        &self.projection.activities,
                        definition,
                    )?;
                }
                self.projection.activities.insert(
                    definition.activity_id.clone(),
                    ActivityRecord {
                        definition: definition.clone(),
                        attempts: Vec::new(),
                    },
                );
            }
            RunEventKind::ActivityAttemptStarted {
                activity_id,
                attempt,
            } => {
                self.ensure_executing()?;
                if event.schema_version >= TERMINAL_ACTIVITY_INVARIANT_SCHEMA_VERSION {
                    let record = self.activity(activity_id)?.clone();
                    if !is_reserved_audit_activity(&record)
                        && self.projection.activities.values().any(|candidate| {
                            matches!(
                                candidate.definition.stable_step_id.as_str(),
                                RUNTIME_RUN_STOPPED_AUDIT_STEP_ID
                                    | RUNTIME_RECOVERY_RUN_STOPPED_AUDIT_STEP_ID
                            ) && is_reserved_audit_activity(candidate)
                        })
                    {
                        return Err(DurabilityError::InvalidEvent {
                            reason: "ordinary work cannot start after a terminal audit marker"
                                .into(),
                        });
                    }
                    validate_reserved_audit_attempt_start(
                        &self.run_id,
                        &self.projection.activities,
                        &record,
                    )?;
                }
                let sequence = event.sequence;
                let record = self.activity_mut(activity_id)?;
                let expected = record.attempts.len() as u32 + 1;
                if *attempt != expected {
                    return Err(invalid_activity(
                        activity_id,
                        format!("expected attempt {expected}, got {attempt}"),
                    ));
                }
                if let Some(previous) = record.latest_attempt() {
                    if previous.status != ActivityAttemptStatus::Failed || !previous.retryable {
                        return Err(invalid_activity(
                            activity_id,
                            "previous attempt is not retryable".into(),
                        ));
                    }
                    if !retry_is_safe(&record.definition, previous.effect_ambiguous) {
                        return Err(invalid_activity(
                            activity_id,
                            "previous attempt requires reconciliation".into(),
                        ));
                    }
                }
                record.attempts.push(ActivityAttempt {
                    attempt: *attempt,
                    status: ActivityAttemptStatus::Running,
                    started_sequence: sequence,
                    finished_sequence: None,
                    output: None,
                    output_hash: None,
                    error: None,
                    retryable: false,
                    effect_ambiguous: false,
                });
            }
            RunEventKind::ActivityAttemptCompleted {
                activity_id,
                attempt,
                output,
                output_hash,
            } => {
                self.ensure_active()?;
                if stable_input_hash(output) != *output_hash {
                    return Err(DurabilityError::InvalidEvent {
                        reason: format!("activity `{activity_id}` output hash is invalid"),
                    });
                }
                if event.schema_version >= TERMINAL_ACTIVITY_INVARIANT_SCHEMA_VERSION {
                    let record = self.activity(activity_id)?.clone();
                    validate_reserved_audit_attempt_completion(
                        &self.run_id,
                        &self.projection.activities,
                        &record,
                        output,
                    )?;
                }
                let sequence = event.sequence;
                let attempt_state = self.running_attempt_mut(activity_id, *attempt)?;
                attempt_state.status = ActivityAttemptStatus::Completed;
                attempt_state.finished_sequence = Some(sequence);
                attempt_state.output = Some(output.clone());
                attempt_state.output_hash = Some(output_hash.clone());
            }
            RunEventKind::ActivityAttemptFailed {
                activity_id,
                attempt,
                error,
                retryable,
                effect_ambiguous,
            } => {
                self.ensure_active()?;
                if event.schema_version >= TERMINAL_ACTIVITY_INVARIANT_SCHEMA_VERSION {
                    let record = self.activity(activity_id)?.clone();
                    if is_reserved_audit_activity(&record) {
                        return Err(DurabilityError::InvalidEvent {
                            reason: "reserved audit failures must transition directly to reconciliation-required"
                                .into(),
                        });
                    }
                }
                let sequence = event.sequence;
                let attempt_state = self.running_attempt_mut(activity_id, *attempt)?;
                attempt_state.status = ActivityAttemptStatus::Failed;
                attempt_state.finished_sequence = Some(sequence);
                attempt_state.error = Some(error.clone());
                attempt_state.retryable = *retryable;
                attempt_state.effect_ambiguous = *effect_ambiguous;
            }
            RunEventKind::ActivityAttemptCancelled {
                activity_id,
                attempt,
                reason,
            } => {
                self.ensure_active()?;
                if event.schema_version < ACTIVITY_ATTEMPT_CANCELLED_SCHEMA_VERSION {
                    return Err(DurabilityError::InvalidEvent {
                        reason: "activity attempt cancellation events require durability schema v3"
                            .into(),
                    });
                }
                if is_reserved_audit_activity(self.activity(activity_id)?) {
                    return Err(DurabilityError::InvalidEvent {
                        reason: "reserved audit cancellations require reconciliation".into(),
                    });
                }
                let sequence = event.sequence;
                let attempt_state = self.running_attempt_mut(activity_id, *attempt)?;
                attempt_state.status = ActivityAttemptStatus::Cancelled;
                attempt_state.finished_sequence = Some(sequence);
                attempt_state.error = Some(reason.clone());
                attempt_state.retryable = false;
                attempt_state.effect_ambiguous = false;
            }
            RunEventKind::ActivityReconciliationRequired {
                activity_id,
                attempt,
                reason,
            } => {
                let terminal_legacy_orphan = event.schema_version
                    >= TERMINAL_ACTIVITY_INVARIANT_SCHEMA_VERSION
                    && self.projection.status.is_terminal()
                    && *attempt == 1
                    && self.activity(activity_id).is_ok_and(|record| {
                        record.definition.stable_step_id == RUNTIME_LEGACY_RUN_STOPPED_AUDIT_STEP_ID
                            && is_reserved_audit_activity(record)
                            && record.attempts.is_empty()
                    });
                let terminal_legacy_running = event.schema_version
                    >= TERMINAL_ACTIVITY_INVARIANT_SCHEMA_VERSION
                    && self.projection.status.is_terminal()
                    && self.activity(activity_id).is_ok_and(|record| {
                        !is_reserved_audit_activity(record)
                            && record.definition.side_effect_class
                                == SideEffectClass::ReconcileRequired
                            && record.latest_attempt().is_some_and(|candidate| {
                                candidate.attempt == *attempt
                                    && candidate.status == ActivityAttemptStatus::Running
                            })
                            && self.activity_attempt_has_v1_provenance(activity_id, *attempt)
                    });
                let terminal_legacy_migration = terminal_legacy_orphan || terminal_legacy_running;
                if !terminal_legacy_migration {
                    self.ensure_active()?;
                }
                let sequence = event.sequence;
                let record = self.activity_mut(activity_id)?;
                if record.attempts.is_empty() {
                    if event.schema_version < TERMINAL_ACTIVITY_INVARIANT_SCHEMA_VERSION
                        || *attempt != 1
                        || !is_reserved_audit_activity(record)
                    {
                        return Err(invalid_activity(
                            activity_id,
                            "attempt was not found".into(),
                        ));
                    }
                    record.attempts.push(ActivityAttempt {
                        attempt: *attempt,
                        status: ActivityAttemptStatus::ReconcileRequired,
                        started_sequence: sequence,
                        finished_sequence: Some(sequence),
                        output: None,
                        output_hash: None,
                        error: Some(reason.clone()),
                        retryable: false,
                        effect_ambiguous: true,
                    });
                    if !terminal_legacy_migration {
                        self.projection.status = DurableRunStatus::ReconcileRequired;
                        self.projection.pause_reason = Some(reason.clone());
                    }
                    return Ok(());
                }
                let attempt_state = record
                    .attempts
                    .iter_mut()
                    .find(|candidate| candidate.attempt == *attempt)
                    .ok_or_else(|| invalid_activity(activity_id, "attempt was not found".into()))?;
                if !matches!(
                    attempt_state.status,
                    ActivityAttemptStatus::Running | ActivityAttemptStatus::Failed
                ) {
                    return Err(invalid_activity(
                        activity_id,
                        "only running or failed attempts can require reconciliation".into(),
                    ));
                }
                attempt_state.status = ActivityAttemptStatus::ReconcileRequired;
                attempt_state.finished_sequence = Some(sequence);
                attempt_state.error = Some(reason.clone());
                attempt_state.effect_ambiguous = true;
                if !terminal_legacy_migration {
                    self.projection.status = DurableRunStatus::ReconcileRequired;
                    self.projection.pause_reason = Some(reason.clone());
                }
            }
            RunEventKind::ActivityReconciled {
                activity_id,
                attempt,
                resolution,
            } => {
                let record = self.activity(activity_id)?.clone();
                if event.schema_version >= TERMINAL_ACTIVITY_INVARIANT_SCHEMA_VERSION {
                    let lifecycle_schedule_sequence = (record.definition.stable_step_id
                        == RUNTIME_INVOCATION_LIFECYCLE_STEP_ID)
                        .then(|| {
                            self.events.iter().find_map(|prior| match &prior.kind {
                                RunEventKind::ActivityScheduled { definition }
                                    if definition.activity_id == *activity_id =>
                                {
                                    Some(prior.sequence)
                                }
                                _ => None,
                            })
                        })
                        .flatten();
                    validate_reserved_audit_reconciliation(
                        &self.run_id,
                        &self.projection.activities,
                        &record,
                        resolution,
                        lifecycle_schedule_sequence,
                    )?;
                }
                let previous_run_status = self.projection.status;
                let terminal_legacy_attestation = previous_run_status.is_terminal()
                    && record.definition.stable_step_id == RUNTIME_LEGACY_RUN_STOPPED_AUDIT_STEP_ID
                    && record
                        .latest_attempt()
                        .is_some_and(|attempt| attempt.status != ActivityAttemptStatus::Completed)
                    && matches!(resolution, ActivityReconciliation::Completed { .. });
                if terminal_legacy_attestation {
                    let ActivityReconciliation::Completed { output } = resolution else {
                        unreachable!("terminal legacy attestation is completion-only")
                    };
                    let attestation: crate::durable_runtime::DurableLegacyRunStoppedResolutionEnvelope =
                        serde_json::from_value(output.clone()).map_err(|_| {
                            DurabilityError::InvalidEvent {
                                reason: "terminal legacy RunStopped attestation is malformed"
                                    .into(),
                            }
                        })?;
                    let attested_status = match attestation.terminal_receipt.reason.as_str() {
                        "end_turn" | "stop" => DurableRunStatus::Completed,
                        "approval_interrupted" | "cancelled" => DurableRunStatus::Cancelled,
                        _ => DurableRunStatus::Failed,
                    };
                    if attested_status != previous_run_status {
                        return Err(DurabilityError::InvalidEvent {
                            reason: "terminal legacy RunStopped attestation contradicts the persisted terminal status"
                                .into(),
                        });
                    }
                }
                let sequence = event.sequence;
                let attempt_state = self
                    .activity_mut(activity_id)?
                    .attempts
                    .iter_mut()
                    .find(|candidate| candidate.attempt == *attempt)
                    .ok_or_else(|| invalid_activity(activity_id, "attempt was not found".into()))?;
                if attempt_state.status != ActivityAttemptStatus::ReconcileRequired
                    && !terminal_legacy_attestation
                {
                    return Err(invalid_activity(
                        activity_id,
                        "attempt is not awaiting reconciliation".into(),
                    ));
                }
                match resolution {
                    ActivityReconciliation::Completed { output } => {
                        attempt_state.status = ActivityAttemptStatus::Completed;
                        attempt_state.output_hash = Some(stable_input_hash(output));
                        attempt_state.output = Some(output.clone());
                        attempt_state.retryable = false;
                        attempt_state.effect_ambiguous = false;
                    }
                    ActivityReconciliation::SafeToRetry => {
                        attempt_state.status = ActivityAttemptStatus::Failed;
                        attempt_state.retryable = true;
                        attempt_state.effect_ambiguous = false;
                    }
                    ActivityReconciliation::Cancelled => {
                        attempt_state.status = ActivityAttemptStatus::Cancelled;
                        attempt_state.retryable = false;
                        attempt_state.effect_ambiguous = false;
                    }
                }
                attempt_state.finished_sequence = Some(sequence);
                if !previous_run_status.is_terminal() && !self.has_reconciliation_pending() {
                    self.projection.status = DurableRunStatus::Paused;
                    self.projection.pause_reason = Some("reconciliation completed".into());
                }
            }
            RunEventKind::ApprovalRequested {
                approval_id,
                logical_key,
                activity_id,
                prompt,
                payload,
            } => {
                self.ensure_active()?;
                if self.projection.status == DurableRunStatus::ReconcileRequired {
                    return Err(DurabilityError::RunRequiresReconciliation);
                }
                if self.projection.approvals.contains_key(approval_id) {
                    return Err(DurabilityError::InvalidEvent {
                        reason: format!("approval `{approval_id}` was requested twice"),
                    });
                }
                let decoded = decode_approval_payload(payload, event.schema_version)?;
                if let Some(policy_hash) = decoded.policy_snapshot_hash.as_deref() {
                    validate_policy_hash(policy_hash)?;
                }
                if decoded.policy_snapshot_hash != self.policy_snapshot_hash {
                    return Err(DurabilityError::InvalidEvent {
                        reason: format!(
                            "approval policy snapshot does not match pinned run policy: expected {:?}, got {:?}",
                            self.policy_snapshot_hash, decoded.policy_snapshot_hash
                        ),
                    });
                }
                if decoded.governance_binding != self.governance_binding {
                    return Err(DurabilityError::InvalidEvent {
                        reason: "approval governance binding does not match pinned run binding"
                            .into(),
                    });
                }
                if let Some(binding) = decoded.governance_binding.as_ref() {
                    binding
                        .validate()
                        .map_err(|error| DurabilityError::InvalidEvent {
                            reason: format!("invalid approval governance binding: {error}"),
                        })?;
                    if binding.run_id() != self.run_id
                        || decoded.policy_snapshot_hash.as_deref()
                            != Some(binding.policy_snapshot_hash())
                    {
                        return Err(DurabilityError::InvalidEvent {
                            reason: "approval governance binding identity is inconsistent".into(),
                        });
                    }
                }
                self.projection.approvals.insert(
                    approval_id.clone(),
                    DurableApproval {
                        approval_id: approval_id.clone(),
                        logical_key: logical_key.clone(),
                        activity_id: activity_id.clone(),
                        kind: decoded.kind,
                        prompt: prompt.clone(),
                        payload: decoded.payload,
                        policy_snapshot_hash: decoded.policy_snapshot_hash,
                        governance_binding: decoded.governance_binding,
                        requested_at_unix_ms: decoded.requested_at_unix_ms,
                        expires_at_unix_ms: decoded.expires_at_unix_ms,
                        status: DurableApprovalStatus::Pending,
                        response: None,
                        resolved_at_unix_ms: None,
                        timed_out: false,
                        requested_sequence: event.sequence,
                        resolved_sequence: None,
                    },
                );
                self.projection.status = DurableRunStatus::Paused;
                self.projection.pause_reason = Some(format!("approval `{approval_id}` required"));
            }
            RunEventKind::ApprovalResolved {
                approval_id,
                approved,
                response,
            } => {
                let approval = self
                    .projection
                    .approvals
                    .get_mut(approval_id)
                    .ok_or_else(|| DurabilityError::ApprovalNotFound {
                        approval_id: approval_id.clone(),
                    })?;
                if approval.status != DurableApprovalStatus::Pending {
                    return Err(DurabilityError::ApprovalAlreadyResolved {
                        approval_id: approval_id.clone(),
                    });
                }
                let decoded =
                    decode_resolution_payload(approval, *approved, response, event.schema_version)?;
                approval.status = if *approved {
                    DurableApprovalStatus::Approved
                } else {
                    DurableApprovalStatus::Rejected
                };
                approval.response = decoded.response;
                approval.resolved_at_unix_ms = decoded.resolved_at_unix_ms;
                approval.timed_out = decoded.timed_out;
                approval.resolved_sequence = Some(event.sequence);
            }
            RunEventKind::ArtifactPublished { metadata } => {
                self.ensure_active()?;
                let versions = self
                    .projection
                    .artifacts
                    .entry(metadata.artifact_id.clone())
                    .or_default();
                let expected = versions.last().map_or(1, |prior| prior.version + 1);
                if metadata.version != expected {
                    return Err(DurabilityError::InvalidArtifactVersion {
                        artifact_id: metadata.artifact_id.clone(),
                        expected,
                        actual: metadata.version,
                    });
                }
                if metadata.previous_version_id
                    != versions.last().map(|prior| prior.version_id.clone())
                {
                    return Err(DurabilityError::InvalidEvent {
                        reason: format!(
                            "artifact `{}` previous version does not match active projection",
                            metadata.artifact_id
                        ),
                    });
                }
                versions.push(metadata.clone());
            }
            RunEventKind::CheckpointCommitted {
                checkpoint_id,
                label,
            } => {
                self.ensure_active()?;
                if self.checkpoints.contains_key(checkpoint_id) {
                    return Err(DurabilityError::InvalidEvent {
                        reason: format!("checkpoint `{checkpoint_id}` was committed twice"),
                    });
                }
                let checkpoint = Checkpoint {
                    checkpoint_id: checkpoint_id.clone(),
                    run_id: self.run_id.clone(),
                    event_sequence: event.sequence,
                    parent_checkpoint_id: self.projection.current_checkpoint_id.clone(),
                    label: label.clone(),
                    projection: self.projection.clone(),
                };
                self.checkpoints.insert(checkpoint_id.clone(), checkpoint);
                self.projection.current_checkpoint_id = Some(checkpoint_id.clone());
            }
            RunEventKind::RunPaused { reason } => {
                self.ensure_active()?;
                if self.projection.status == DurableRunStatus::ReconcileRequired {
                    return Err(DurabilityError::RunRequiresReconciliation);
                }
                self.projection.status = DurableRunStatus::Paused;
                self.projection.pause_reason = Some(reason.clone());
            }
            RunEventKind::RunResumed => {
                if self.projection.status != DurableRunStatus::Paused {
                    return Err(DurabilityError::NotPaused);
                }
                if !self.projection.pending_approval_ids().is_empty() {
                    return Err(DurabilityError::PendingApprovals);
                }
                if self.has_reconciliation_pending() {
                    return Err(DurabilityError::RunRequiresReconciliation);
                }
                self.projection.status = DurableRunStatus::Running;
                self.projection.pause_reason = None;
            }
            RunEventKind::ForkCreated {
                new_run_id,
                checkpoint_id,
                side_effects_reconciled,
                ..
            } => {
                self.ensure_active()?;
                validate_identifier("new_run_id", new_run_id)?;
                let checkpoint = self.checkpoint_by_id(checkpoint_id)?.clone();
                let checkpoint = self.checkpoint_for_current_run(checkpoint)?;
                self.require_reconciled_after(checkpoint.event_sequence, *side_effects_reconciled)?;
            }
            RunEventKind::RunRewound {
                checkpoint_id,
                new_branch_id,
                side_effects_reconciled,
            } => {
                self.ensure_active()?;
                let checkpoint = self.checkpoint_by_id(checkpoint_id)?.clone();
                let checkpoint = self.checkpoint_for_current_run(checkpoint)?;
                self.require_reconciled_after(checkpoint.event_sequence, *side_effects_reconciled)?;
                self.projection = fork_projection(&checkpoint, &self.run_id, new_branch_id);
                self.projection.status = DurableRunStatus::Paused;
                self.projection.pause_reason = Some(format!("rewound to `{checkpoint_id}`"));
            }
            RunEventKind::RunCompleted => {
                self.ensure_active()?;
                if event.schema_version >= TERMINAL_ACTIVITY_INVARIANT_SCHEMA_VERSION {
                    self.ensure_reserved_audit_lifecycle_closed()?;
                }
                if !self.projection.pending_approval_ids().is_empty() {
                    return Err(DurabilityError::PendingApprovals);
                }
                if self.has_reconciliation_pending() {
                    return Err(DurabilityError::RunRequiresReconciliation);
                }
                self.ensure_no_running_activities("complete")?;
                self.projection.status = DurableRunStatus::Completed;
                self.projection.pause_reason = None;
            }
            RunEventKind::RunFailed { error } => {
                self.ensure_active()?;
                if event.schema_version >= TERMINAL_ACTIVITY_INVARIANT_SCHEMA_VERSION {
                    self.ensure_reserved_audit_lifecycle_closed()?;
                    self.ensure_no_running_activities("fail")?;
                }
                self.projection.status = DurableRunStatus::Failed;
                self.projection.pause_reason = Some(error.clone());
            }
            RunEventKind::RunCancelled { reason } => {
                self.ensure_active()?;
                if event.schema_version >= TERMINAL_ACTIVITY_INVARIANT_SCHEMA_VERSION {
                    self.ensure_reserved_audit_lifecycle_closed()?;
                    self.ensure_no_running_activities("cancel")?;
                }
                self.projection.status = DurableRunStatus::Cancelled;
                self.projection.pause_reason = reason.clone();
            }
            RunEventKind::RunStarted { .. } | RunEventKind::ForkedFrom { .. } => unreachable!(),
        }
        Ok(())
    }

    fn activity_mut(&mut self, activity_id: &str) -> DurabilityResult<&mut ActivityRecord> {
        self.projection
            .activities
            .get_mut(activity_id)
            .ok_or_else(|| DurabilityError::ActivityNotFound {
                activity_id: activity_id.to_string(),
            })
    }

    fn ensure_no_running_activities(&self, transition: &str) -> DurabilityResult<()> {
        let activity_ids = self
            .projection
            .activities
            .values()
            .filter(|record| {
                record
                    .attempts
                    .iter()
                    .any(|attempt| attempt.status == ActivityAttemptStatus::Running)
            })
            .map(|record| record.definition.activity_id.clone())
            .collect::<Vec<_>>();
        if activity_ids.is_empty() {
            return Ok(());
        }
        Err(DurabilityError::InvalidEvent {
            reason: format!(
                "run cannot {transition} while activities are running: {activity_ids:?}"
            ),
        })
    }

    fn running_attempt_mut(
        &mut self,
        activity_id: &str,
        attempt: u32,
    ) -> DurabilityResult<&mut ActivityAttempt> {
        let attempt_state = self
            .activity_mut(activity_id)?
            .attempts
            .iter_mut()
            .find(|candidate| candidate.attempt == attempt)
            .ok_or_else(|| invalid_activity(activity_id, "attempt was not found".into()))?;
        if attempt_state.status != ActivityAttemptStatus::Running {
            return Err(invalid_activity(
                activity_id,
                "attempt is not running".into(),
            ));
        }
        Ok(attempt_state)
    }

    fn has_reconciliation_pending(&self) -> bool {
        self.projection.activities.values().any(|record| {
            record
                .latest_attempt()
                .is_some_and(|attempt| attempt.status == ActivityAttemptStatus::ReconcileRequired)
        })
    }

    fn activity_attempt_has_v1_provenance(&self, activity_id: &str, attempt: u32) -> bool {
        let scheduled = self.events.iter().any(|event| {
            event.schema_version < TERMINAL_ACTIVITY_INVARIANT_SCHEMA_VERSION
                && matches!(
                    &event.kind,
                    RunEventKind::ActivityScheduled { definition }
                        if definition.activity_id == activity_id
                )
        });
        let started = self.events.iter().any(|event| {
            event.schema_version < TERMINAL_ACTIVITY_INVARIANT_SCHEMA_VERSION
                && matches!(
                    &event.kind,
                    RunEventKind::ActivityAttemptStarted {
                        activity_id: candidate,
                        attempt: candidate_attempt,
                        ..
                    } if candidate == activity_id && *candidate_attempt == attempt
                )
        });
        scheduled && started
    }
}

fn append_sequence(outcome: AppendOutcome) -> u64 {
    match outcome {
        AppendOutcome::Appended { sequence } | AppendOutcome::Deduplicated { sequence } => sequence,
    }
}

fn fork_projection(
    checkpoint: &Checkpoint,
    destination_run_id: &str,
    new_branch_id: &str,
) -> RunProjection {
    let mut projection = checkpoint.projection.clone();
    // Audit delivery markers are bound to one run/branch/invocation identity. Carrying them into a
    // fork or rewind can either strand the new branch on the old identity or falsely terminalize
    // it from an earlier branch's receipt. Strip the whole reserved stable-step namespace rather
    // than only well-formed markers: imported `ForkedFrom` checkpoints are public replay input,
    // and a malformed reserved record must not survive merely because its envelope is invalid.
    // The source event log remains the authority for those external effects; the new branch opens
    // a fresh lifecycle if it executes again.
    let destination_terminal_activity_id = stable_identifier(
        "activity",
        &[
            destination_run_id,
            new_branch_id,
            RUNTIME_RUN_STOPPED_AUDIT_STEP_ID,
            "terminal",
        ],
    );
    projection.activities.retain(|activity_id, record| {
        let reserved_step = matches!(
            record.definition.stable_step_id.as_str(),
            RUNTIME_LEGACY_RUN_STOPPED_AUDIT_STEP_ID
                | RUNTIME_RUN_STOPPED_AUDIT_STEP_ID
                | RUNTIME_RECOVERY_RUN_STOPPED_AUDIT_STEP_ID
                | RUNTIME_INVOCATION_LIFECYCLE_STEP_ID
        );
        // The canonical terminal identity is fixed for the destination run and branch. Remove an
        // imported map-key or definition-ID squatter even when it lies about its stable step;
        // otherwise the valid terminal schedule would deterministically collide with it forever.
        let destination_terminal_identity_squatter = activity_id
            == &destination_terminal_activity_id
            || record.definition.activity_id == destination_terminal_activity_id;
        !reserved_step && !destination_terminal_identity_squatter
    });
    // Execution claims are bound to the source run and branch. A fork or rewind always starts
    // unowned; ordinary completed activity results remain reusable through their own records.
    projection.worker_lease = None;
    projection.branch_id = new_branch_id.to_string();
    projection.current_checkpoint_id = Some(checkpoint.checkpoint_id.clone());
    if projection.status == DurableRunStatus::ReconcileRequired
        && projection.activities.values().any(|record| {
            record
                .latest_attempt()
                .is_some_and(|attempt| attempt.status == ActivityAttemptStatus::ReconcileRequired)
        })
    {
        return projection;
    }
    if projection.pending_approval_ids().is_empty() {
        projection.status = DurableRunStatus::Running;
        projection.pause_reason = None;
    } else {
        projection.status = DurableRunStatus::Paused;
        projection.pause_reason = Some("fork restored pending approvals".into());
    }
    projection
}

fn invalid_activity(activity_id: &str, reason: String) -> DurabilityError {
    DurabilityError::InvalidActivityTransition {
        activity_id: activity_id.to_string(),
        reason,
    }
}

struct DecodedApprovalPayload {
    kind: DurableApprovalKind,
    payload: Value,
    policy_snapshot_hash: Option<String>,
    governance_binding: Option<crate::governance::GovernanceBinding>,
    requested_at_unix_ms: Option<u64>,
    expires_at_unix_ms: Option<u64>,
}

fn decode_approval_payload(
    payload: &Value,
    event_schema_version: u32,
) -> DurabilityResult<DecodedApprovalPayload> {
    let Some(envelope) = payload
        .as_object()
        .and_then(|object| object.get(DURABLE_APPROVAL_ENVELOPE_KEY))
    else {
        return Ok(DecodedApprovalPayload {
            kind: DurableApprovalKind::Confirmation,
            payload: payload.clone(),
            policy_snapshot_hash: None,
            governance_binding: None,
            requested_at_unix_ms: None,
            expires_at_unix_ms: None,
        });
    };
    let envelope: DurableApprovalEnvelope =
        serde_json::from_value(envelope.clone()).map_err(|error| {
            DurabilityError::InvalidEvent {
                reason: format!("invalid durable approval envelope: {error}"),
            }
        })?;
    if envelope.schema_version != event_schema_version {
        return Err(DurabilityError::UnsupportedSchema {
            expected: event_schema_version,
            actual: envelope.schema_version,
        });
    }
    if envelope.expires_at_unix_ms <= envelope.requested_at_unix_ms {
        return Err(DurabilityError::InvalidEvent {
            reason: "approval expiry must be after its request time".into(),
        });
    }
    Ok(DecodedApprovalPayload {
        kind: envelope.kind,
        payload: envelope.payload,
        policy_snapshot_hash: envelope.policy_snapshot_hash,
        governance_binding: envelope.governance_binding,
        requested_at_unix_ms: Some(envelope.requested_at_unix_ms),
        expires_at_unix_ms: Some(envelope.expires_at_unix_ms),
    })
}

struct DecodedResolutionPayload {
    response: Option<Value>,
    resolved_at_unix_ms: Option<u64>,
    timed_out: bool,
}

fn decode_resolution_payload(
    approval: &DurableApproval,
    approved: bool,
    response: &Option<Value>,
    event_schema_version: u32,
) -> DurabilityResult<DecodedResolutionPayload> {
    if approval.expires_at_unix_ms.is_none() {
        return Ok(DecodedResolutionPayload {
            response: response.clone(),
            resolved_at_unix_ms: None,
            timed_out: false,
        });
    }
    let envelope = response
        .as_ref()
        .and_then(Value::as_object)
        .and_then(|object| object.get(DURABLE_RESOLUTION_ENVELOPE_KEY))
        .ok_or_else(|| DurabilityError::InvalidApprovalResolution {
            approval_id: approval.approval_id.clone(),
            reason: "typed approval resolution is missing its trusted clock envelope".into(),
        })?;
    let envelope: DurableResolutionEnvelope =
        serde_json::from_value(envelope.clone()).map_err(|error| {
            DurabilityError::InvalidApprovalResolution {
                approval_id: approval.approval_id.clone(),
                reason: format!("invalid resolution envelope: {error}"),
            }
        })?;
    if envelope.schema_version != event_schema_version {
        return Err(DurabilityError::UnsupportedSchema {
            expected: event_schema_version,
            actual: envelope.schema_version,
        });
    }
    let resolution = ApprovalResolution {
        approval_id: approval.approval_id.clone(),
        approved,
        response: envelope.response.clone(),
    };
    validate_approval_resolution(
        approval,
        &resolution,
        Some(envelope.resolved_at_unix_ms),
        envelope.timed_out,
    )?;
    Ok(DecodedResolutionPayload {
        response: envelope.response,
        resolved_at_unix_ms: Some(envelope.resolved_at_unix_ms),
        timed_out: envelope.timed_out,
    })
}

fn validate_approval_resolution(
    approval: &DurableApproval,
    resolution: &ApprovalResolution,
    now_unix_ms: Option<u64>,
    timed_out: bool,
) -> DurabilityResult<()> {
    let invalid = |reason: &str| DurabilityError::InvalidApprovalResolution {
        approval_id: approval.approval_id.clone(),
        reason: reason.into(),
    };
    if let (Some(requested_at), Some(expires_at)) =
        (approval.requested_at_unix_ms, approval.expires_at_unix_ms)
    {
        let now = now_unix_ms.ok_or_else(|| DurabilityError::ApprovalClockRequired {
            approval_id: approval.approval_id.clone(),
        })?;
        if now < requested_at {
            return Err(invalid("resolution predates the approval request"));
        }
        if timed_out {
            if resolution.approved || now < expires_at {
                return Err(invalid(
                    "timeout denial is before expiry or marked approved",
                ));
            }
        } else if now >= expires_at {
            return Err(invalid("approval has timed out"));
        }
    } else if timed_out {
        return Err(invalid("legacy approval cannot carry a timeout resolution"));
    }
    if resolution.approved {
        match approval.kind {
            DurableApprovalKind::MissingInput
                if resolution.response.as_ref().is_none_or(Value::is_null) =>
            {
                return Err(invalid("missing_input requires a non-null response"));
            }
            DurableApprovalKind::EditRetry => {
                let Some(action) = resolution
                    .response
                    .as_ref()
                    .and_then(Value::as_object)
                    .and_then(|response| response.get("action"))
                    .and_then(Value::as_str)
                else {
                    return Err(invalid("edit_retry requires an edit or retry action"));
                };
                if !matches!(action, "edit" | "retry") {
                    return Err(invalid("edit_retry action must be edit or retry"));
                }
                if action == "edit"
                    && resolution
                        .response
                        .as_ref()
                        .and_then(Value::as_object)
                        .is_none_or(|response| !response.contains_key("value"))
                {
                    return Err(invalid("edit action requires a value"));
                }
            }
            DurableApprovalKind::Confirmation
            | DurableApprovalKind::OutputReview
            | DurableApprovalKind::MissingInput => {}
        }
    }
    Ok(())
}

fn validate_policy_hash(hash: &str) -> DurabilityResult<()> {
    let digest = hash
        .strip_prefix("sha256:")
        .filter(|digest| digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .ok_or_else(|| DurabilityError::InvalidEvent {
            reason: "policy snapshot hash must be a sha256 digest".into(),
        })?;
    debug_assert_eq!(digest.len(), 64);
    Ok(())
}

fn retry_is_safe(definition: &ActivityDefinition, effect_ambiguous: bool) -> bool {
    !effect_ambiguous
        || definition.side_effect_class == SideEffectClass::Pure
        || (definition.side_effect_class == SideEffectClass::Idempotent
            && definition
                .idempotency_key
                .as_deref()
                .is_some_and(|key| !key.is_empty()))
}

fn validate_reserved_audit_schedule(
    run_id: &str,
    branch_id: &str,
    activities: &BTreeMap<String, ActivityRecord>,
    definition: &ActivityDefinition,
) -> DurabilityResult<()> {
    let stable_step_id = definition.stable_step_id.as_str();
    if stable_step_id == RUNTIME_LEGACY_RUN_STOPPED_AUDIT_STEP_ID {
        return Err(DurabilityError::InvalidEvent {
            reason: "new v2 runs cannot schedule the quarantined v1 RunStopped audit activity"
                .into(),
        });
    }
    if !matches!(
        stable_step_id,
        RUNTIME_RUN_STOPPED_AUDIT_STEP_ID
            | RUNTIME_RECOVERY_RUN_STOPPED_AUDIT_STEP_ID
            | RUNTIME_INVOCATION_LIFECYCLE_STEP_ID
    ) {
        return Ok(());
    }
    let expected_activity_id = stable_identifier(
        "activity",
        &[run_id, branch_id, stable_step_id, &definition.logical_key],
    );
    if definition.activity_id != expected_activity_id
        || definition.side_effect_class != SideEffectClass::ReconcileRequired
        || definition.idempotency_key.is_some()
    {
        return Err(DurabilityError::InvalidEvent {
            reason: "reserved audit activity has a non-canonical durable definition".into(),
        });
    }
    let duplicate = activities.values().any(|record| {
        record.definition.stable_step_id == stable_step_id
            && match stable_step_id {
                RUNTIME_RUN_STOPPED_AUDIT_STEP_ID => record.definition.logical_key == "terminal",
                _ => record.definition.logical_key == definition.logical_key,
            }
            && is_reserved_audit_activity(record)
    });
    if duplicate {
        return Err(DurabilityError::InvalidEvent {
            reason: "reserved audit activity identity was scheduled more than once".into(),
        });
    }

    match stable_step_id {
        RUNTIME_RUN_STOPPED_AUDIT_STEP_ID => {
            if definition.logical_key != "terminal" {
                return Err(DurabilityError::InvalidEvent {
                    reason: "canonical terminal audit must use the reserved terminal logical key"
                        .into(),
                });
            }
            if definition.input.get("legacy_resolution").is_some() {
                validate_legacy_resolution_source(activities, definition)
            } else {
                validate_terminal_audit_schedule_input(activities, definition, "canonical", run_id)
            }
        }
        RUNTIME_RECOVERY_RUN_STOPPED_AUDIT_STEP_ID => {
            validate_terminal_audit_schedule_input(activities, definition, "recovery", run_id)
        }
        RUNTIME_INVOCATION_LIFECYCLE_STEP_ID => {
            let input =
                definition
                    .input
                    .as_object()
                    .ok_or_else(|| DurabilityError::InvalidEvent {
                        reason: "invocation lifecycle input must be an object".into(),
                    })?;
            if input.len() != 3
                || input.get("schema_version").and_then(Value::as_u64) != Some(1)
                || input.get("audit_run_id").and_then(Value::as_str) != Some(run_id)
                || input.get("invocation_id").and_then(Value::as_str)
                    != Some(definition.logical_key.as_str())
                || definition.logical_key.is_empty()
                || definition.logical_key.chars().any(char::is_control)
            {
                return Err(DurabilityError::InvalidEvent {
                    reason: "invocation lifecycle durable identity is malformed".into(),
                });
            }
            Ok(())
        }
        _ => unreachable!("reserved step filtered above"),
    }
}

fn validate_terminal_audit_schedule_input(
    activities: &BTreeMap<String, ActivityRecord>,
    definition: &ActivityDefinition,
    kind: &str,
    run_id: &str,
) -> DurabilityResult<()> {
    let input = definition
        .input
        .as_object()
        .ok_or_else(|| DurabilityError::InvalidEvent {
            reason: "terminal audit input must be an object".into(),
        })?;
    if input.len() != 2 {
        return Err(DurabilityError::InvalidEvent {
            reason: "terminal audit input must contain only replay and expected output hash".into(),
        });
    }
    let replay = input
        .get("replay")
        .ok_or_else(|| DurabilityError::InvalidEvent {
            reason: "terminal audit input is missing its replay envelope".into(),
        })?;
    validate_replay_value(replay, kind)?;
    if replay.get("audit_run_id").and_then(Value::as_str) != Some(run_id) {
        return Err(DurabilityError::InvalidEvent {
            reason: "terminal audit replay does not belong to the durable run".into(),
        });
    }
    let invocation_id = replay
        .get("invocation_id")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty() && !value.chars().any(char::is_control))
        .ok_or_else(|| DurabilityError::InvalidEvent {
            reason: "terminal audit replay has an invalid invocation identity".into(),
        })?;
    if kind == "recovery" && invocation_id != definition.logical_key {
        return Err(DurabilityError::InvalidEvent {
            reason: "recovery terminal audit logical key does not match its invocation".into(),
        });
    }
    let matching_lifecycles = activities
        .values()
        .filter(|record| {
            record.definition.stable_step_id == RUNTIME_INVOCATION_LIFECYCLE_STEP_ID
                && is_reserved_audit_activity(record)
                && record.definition.logical_key == invocation_id
                && record
                    .definition
                    .input
                    .get("audit_run_id")
                    .and_then(Value::as_str)
                    == Some(run_id)
                && record
                    .latest_attempt()
                    .is_some_and(|attempt| attempt.status == ActivityAttemptStatus::Running)
        })
        .count();
    if matching_lifecycles != 1 {
        return Err(DurabilityError::InvalidEvent {
            reason: "terminal audit schedule requires one exact active invocation lifecycle".into(),
        });
    }
    let matching_lifecycle_id = activities.values().find_map(|record| {
        (record.definition.stable_step_id == RUNTIME_INVOCATION_LIFECYCLE_STEP_ID
            && is_reserved_audit_activity(record)
            && record.definition.logical_key == invocation_id
            && record
                .latest_attempt()
                .is_some_and(|attempt| attempt.status == ActivityAttemptStatus::Running))
        .then_some(record.definition.activity_id.as_str())
    });
    if activities.values().any(|record| {
        record.definition.activity_id.as_str() != matching_lifecycle_id.unwrap_or_default()
            && record
                .latest_attempt()
                .is_some_and(|attempt| attempt.status == ActivityAttemptStatus::Running)
    }) {
        return Err(DurabilityError::InvalidEvent {
            reason: "terminal audit schedule is blocked by another running activity".into(),
        });
    }
    let expected_output = if kind == "recovery" {
        serde_json::json!({"accepted": true})
    } else {
        replay
            .get("terminal_receipt")
            .cloned()
            .ok_or_else(|| DurabilityError::InvalidEvent {
                reason: "canonical terminal audit replay has no terminal receipt".into(),
            })?
    };
    if input.get("expected_output_hash").and_then(Value::as_str)
        != Some(stable_input_hash(&expected_output).as_str())
    {
        return Err(DurabilityError::InvalidEvent {
            reason: "terminal audit delivery intent has an invalid expected output hash".into(),
        });
    }
    if kind == "recovery"
        && !activities.values().any(|record| {
            if record.definition.stable_step_id != RUNTIME_RUN_STOPPED_AUDIT_STEP_ID
                || record.definition.logical_key != "terminal"
            {
                return false;
            }
            let Some(output) = record.completed_output() else {
                return false;
            };
            if record.definition.input.get("legacy_resolution").is_some() {
                let Ok(resolution) = serde_json::from_value::<
                    crate::durable_runtime::DurableLegacyRunStoppedResolutionEnvelope,
                >(output.clone()) else {
                    return false;
                };
                validate_legacy_terminal_resolution(activities, record, output).is_ok()
                    && serde_json::to_value(resolution.terminal_receipt)
                        .ok()
                        .as_ref()
                        == replay.get("terminal_receipt")
            } else {
                record
                    .definition
                    .input
                    .get("replay")
                    .and_then(|source| source.get("invocation_id"))
                    .and_then(Value::as_str)
                    != Some(invocation_id)
                    && validate_reserved_audit_completion_output(record, output).is_ok()
                    && Some(output) == replay.get("terminal_receipt")
            }
        })
    {
        return Err(DurabilityError::InvalidEvent {
            reason: "recovery terminal audit requires the exact completed canonical receipt".into(),
        });
    }
    Ok(())
}

fn validate_legacy_resolution_source(
    activities: &BTreeMap<String, ActivityRecord>,
    definition: &ActivityDefinition,
) -> DurabilityResult<()> {
    let input = definition
        .input
        .as_object()
        .ok_or_else(|| DurabilityError::InvalidEvent {
            reason: "legacy terminal resolution input must be an object".into(),
        })?;
    let source_value =
        input
            .get("legacy_resolution")
            .ok_or_else(|| DurabilityError::InvalidEvent {
                reason: "legacy terminal resolution input is missing its source binding".into(),
            })?;
    let source: crate::durable_runtime::DurableLegacyRunStoppedResolutionSource =
        serde_json::from_value(source_value.clone()).map_err(|_| {
            DurabilityError::InvalidEvent {
                reason: "legacy terminal resolution source binding is malformed".into(),
            }
        })?;
    if input.len() != 1
        || source.schema_version
            != crate::durable_runtime::LEGACY_RUN_STOPPED_RESOLUTION_SCHEMA_VERSION
        || source.kind != crate::durable_runtime::LEGACY_RUN_STOPPED_RESOLUTION_KIND
        || source.source_activity_id.is_empty()
        || source.source_activity_id.chars().any(char::is_control)
        || source.source_attempt == 0
        || source.source_started_sequence == 0
    {
        return Err(DurabilityError::InvalidEvent {
            reason: "legacy terminal resolution source binding is malformed".into(),
        });
    }
    let legacy = activities
        .get(&source.source_activity_id)
        .filter(|record| {
            record.definition.stable_step_id == RUNTIME_LEGACY_RUN_STOPPED_AUDIT_STEP_ID
                && record.definition.logical_key == "terminal"
        })
        .ok_or_else(|| DurabilityError::InvalidEvent {
            reason: "legacy terminal resolution source activity was not found".into(),
        })?;
    let attempt = legacy
        .latest_attempt()
        .filter(|attempt| {
            attempt.attempt == source.source_attempt
                && attempt.started_sequence == source.source_started_sequence
        })
        .ok_or_else(|| DurabilityError::InvalidEvent {
            reason: "legacy terminal resolution source attempt does not match".into(),
        })?;
    let source_matches = match source.source_output_hash.as_deref() {
        Some(expected_hash) => {
            attempt.status == ActivityAttemptStatus::Completed
                && attempt.output_hash.as_deref() == Some(expected_hash)
                && attempt.output.as_ref().map(stable_input_hash).as_deref() == Some(expected_hash)
        }
        None => {
            matches!(
                attempt.status,
                ActivityAttemptStatus::Failed | ActivityAttemptStatus::Cancelled
            ) && attempt.output.is_none()
                && attempt.output_hash.is_none()
        }
    };
    if !source_matches {
        return Err(DurabilityError::InvalidEvent {
            reason: "legacy terminal resolution source fingerprint does not match".into(),
        });
    }
    Ok(())
}

pub(crate) fn is_reserved_audit_activity(record: &ActivityRecord) -> bool {
    match record.definition.stable_step_id.as_str() {
        RUNTIME_LEGACY_RUN_STOPPED_AUDIT_STEP_ID => record.definition.logical_key == "terminal",
        RUNTIME_RUN_STOPPED_AUDIT_STEP_ID => {
            record.definition.logical_key == "terminal"
                && (record.definition.input.get("replay").is_some()
                    || record.definition.input.get("legacy_resolution").is_some())
        }
        RUNTIME_RECOVERY_RUN_STOPPED_AUDIT_STEP_ID => {
            record.definition.input.get("replay").is_some()
        }
        RUNTIME_INVOCATION_LIFECYCLE_STEP_ID => {
            record
                .definition
                .input
                .get("schema_version")
                .and_then(Value::as_u64)
                == Some(1)
                && record
                    .definition
                    .input
                    .get("invocation_id")
                    .and_then(Value::as_str)
                    == Some(record.definition.logical_key.as_str())
        }
        _ => false,
    }
}

fn validate_reserved_audit_completion_output(
    record: &ActivityRecord,
    output: &Value,
) -> DurabilityResult<()> {
    let stable_step_id = record.definition.stable_step_id.as_str();
    if stable_step_id != RUNTIME_RUN_STOPPED_AUDIT_STEP_ID
        && stable_step_id != RUNTIME_RECOVERY_RUN_STOPPED_AUDIT_STEP_ID
    {
        return Ok(());
    }
    if stable_step_id == RUNTIME_RUN_STOPPED_AUDIT_STEP_ID
        && record.definition.logical_key != "terminal"
    {
        return Err(DurabilityError::InvalidEvent {
            reason: "canonical terminal audit must use the reserved terminal logical key".into(),
        });
    }
    if stable_step_id == RUNTIME_RUN_STOPPED_AUDIT_STEP_ID
        && record.definition.input.get("legacy_resolution").is_some()
    {
        return Err(DurabilityError::InvalidEvent {
            reason:
                "legacy terminal audit bridge is completion-only through explicit reconciliation"
                    .into(),
        });
    }
    let expected_hash = record
        .definition
        .input
        .get("expected_output_hash")
        .or_else(|| {
            (stable_step_id == RUNTIME_RUN_STOPPED_AUDIT_STEP_ID)
                .then(|| record.definition.input.get("input_hash"))
                .flatten()
        })
        .and_then(Value::as_str)
        .ok_or_else(|| DurabilityError::InvalidEvent {
            reason: "reserved terminal audit is missing its expected output hash".into(),
        })?;
    if stable_input_hash(output) != expected_hash {
        return Err(DurabilityError::InvalidEvent {
            reason: "reserved terminal audit completion does not match its delivery intent".into(),
        });
    }
    Ok(())
}

fn validate_reserved_audit_attempt_start(
    run_id: &str,
    activities: &BTreeMap<String, ActivityRecord>,
    record: &ActivityRecord,
) -> DurabilityResult<()> {
    if !is_reserved_audit_activity(record) {
        return Ok(());
    }
    match record.definition.stable_step_id.as_str() {
        RUNTIME_LEGACY_RUN_STOPPED_AUDIT_STEP_ID => Err(DurabilityError::InvalidEvent {
            reason: "quarantined v1 RunStopped audit cannot start a new v2 attempt".into(),
        }),
        RUNTIME_INVOCATION_LIFECYCLE_STEP_ID => {
            if record
                .definition
                .input
                .get("audit_run_id")
                .and_then(Value::as_str)
                != Some(run_id)
            {
                return Err(DurabilityError::InvalidEvent {
                    reason: "invocation lifecycle attempt does not belong to the durable run"
                        .into(),
                });
            }
            Ok(())
        }
        RUNTIME_RUN_STOPPED_AUDIT_STEP_ID
            if record.definition.input.get("legacy_resolution").is_some() =>
        {
            validate_legacy_resolution_source(activities, &record.definition)
        }
        RUNTIME_RUN_STOPPED_AUDIT_STEP_ID | RUNTIME_RECOVERY_RUN_STOPPED_AUDIT_STEP_ID => {
            let kind = if record.definition.stable_step_id == RUNTIME_RUN_STOPPED_AUDIT_STEP_ID {
                "canonical"
            } else {
                "recovery"
            };
            if record.attempts.is_empty() {
                return validate_terminal_audit_schedule_input(
                    activities,
                    &record.definition,
                    kind,
                    run_id,
                );
            }
            validate_terminal_replay_input(record, kind == "recovery", run_id)?;
            let replay = record.definition.input.get("replay");
            let invocation_id = replay
                .and_then(|value| value.get("invocation_id"))
                .and_then(Value::as_str);
            let authorized = activities.values().any(|lifecycle| {
                lifecycle.definition.stable_step_id == RUNTIME_INVOCATION_LIFECYCLE_STEP_ID
                    && is_reserved_audit_activity(lifecycle)
                    && Some(lifecycle.definition.logical_key.as_str()) == invocation_id
                    && lifecycle.completed_output().is_some_and(|output| {
                        output.get("status").and_then(Value::as_str)
                            == Some("terminal_replay_authorized")
                            && output.get("replay") == replay
                            && validate_lifecycle_reconciliation_output(
                                run_id, activities, lifecycle, output,
                            )
                            .is_ok()
                    })
            });
            if !authorized {
                return Err(DurabilityError::InvalidEvent {
                    reason: "terminal audit retry has no exact authorized lifecycle closure".into(),
                });
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn validate_lifecycle_not_started_output(
    run_id: &str,
    activities: &BTreeMap<String, ActivityRecord>,
    record: &ActivityRecord,
    output: &Value,
    boundary_sequence: u64,
    require_synthetic_orphan: bool,
) -> DurabilityResult<()> {
    let expected_run_id = record
        .definition
        .input
        .get("audit_run_id")
        .and_then(Value::as_str);
    let expected_invocation_id = record
        .definition
        .input
        .get("invocation_id")
        .and_then(Value::as_str);
    let exact_shape = output.as_object().is_some_and(|object| object.len() == 3);
    let linked_terminal_marker = activities.values().any(|candidate| {
        matches!(
            candidate.definition.stable_step_id.as_str(),
            RUNTIME_RUN_STOPPED_AUDIT_STEP_ID | RUNTIME_RECOVERY_RUN_STOPPED_AUDIT_STEP_ID
        ) && candidate
            .definition
            .input
            .get("replay")
            .and_then(|replay| replay.get("invocation_id"))
            .and_then(Value::as_str)
            == expected_invocation_id
    });
    let post_fence_attempt = activities.values().any(|candidate| {
        candidate.definition.activity_id != record.definition.activity_id
            && candidate
                .attempts
                .iter()
                .any(|attempt| attempt.started_sequence > boundary_sequence)
    });
    let synthetic_orphan = record.latest_attempt().is_some_and(|attempt| {
        attempt.status == ActivityAttemptStatus::ReconcileRequired
            && attempt.finished_sequence == Some(attempt.started_sequence)
    });
    if !exact_shape
        || expected_run_id != Some(run_id)
        || output.get("audit_run_id").and_then(Value::as_str) != expected_run_id
        || output.get("invocation_id").and_then(Value::as_str) != expected_invocation_id
        || linked_terminal_marker
        || post_fence_attempt
        || (require_synthetic_orphan && !synthetic_orphan)
    {
        return Err(DurabilityError::InvalidEvent {
            reason:
                "unstarted invocation lifecycle completion has invalid identity or post-fence work"
                    .into(),
        });
    }
    Ok(())
}

fn validate_reserved_audit_attempt_completion(
    run_id: &str,
    activities: &BTreeMap<String, ActivityRecord>,
    record: &ActivityRecord,
    output: &Value,
) -> DurabilityResult<()> {
    let stable_step_id = record.definition.stable_step_id.as_str();
    if !is_reserved_audit_activity(record) {
        return Ok(());
    }
    if stable_step_id == RUNTIME_LEGACY_RUN_STOPPED_AUDIT_STEP_ID {
        return Err(DurabilityError::InvalidEvent {
            reason: "quarantined v1 RunStopped audit cannot receive a new v2 completion".into(),
        });
    }
    if stable_step_id == RUNTIME_RUN_STOPPED_AUDIT_STEP_ID
        || stable_step_id == RUNTIME_RECOVERY_RUN_STOPPED_AUDIT_STEP_ID
    {
        return validate_reserved_audit_completion_output(record, output);
    }
    if stable_step_id != RUNTIME_INVOCATION_LIFECYCLE_STEP_ID {
        return Ok(());
    }
    match output.get("status").and_then(Value::as_str) {
        Some("not_started") => {
            let boundary_sequence = record
                .latest_attempt()
                .map(|attempt| attempt.started_sequence)
                .ok_or_else(|| DurabilityError::InvalidEvent {
                    reason: "unstarted invocation lifecycle has no durable attempt".into(),
                })?;
            validate_lifecycle_not_started_output(
                run_id,
                activities,
                record,
                output,
                boundary_sequence,
                false,
            )
        }
        Some("audit_closed") => {
            validate_lifecycle_reconciliation_output(run_id, activities, record, output)
        }
        Some("terminal_replay_authorized") => Err(DurabilityError::InvalidEvent {
            reason: "terminal replay authorization requires an ActivityReconciled event".into(),
        }),
        _ => Err(DurabilityError::InvalidEvent {
            reason: "invocation lifecycle completion has an unsupported outcome".into(),
        }),
    }
}

fn validate_reserved_audit_reconciliation(
    run_id: &str,
    activities: &BTreeMap<String, ActivityRecord>,
    record: &ActivityRecord,
    resolution: &ActivityReconciliation,
    lifecycle_schedule_sequence: Option<u64>,
) -> DurabilityResult<()> {
    let stable_step_id = record.definition.stable_step_id.as_str();
    let reserved = is_reserved_audit_activity(record);
    let is_terminal_audit = reserved && stable_step_id == RUNTIME_RUN_STOPPED_AUDIT_STEP_ID;
    let is_recovery_audit =
        reserved && stable_step_id == RUNTIME_RECOVERY_RUN_STOPPED_AUDIT_STEP_ID;
    let is_lifecycle = reserved && stable_step_id == RUNTIME_INVOCATION_LIFECYCLE_STEP_ID;
    let is_legacy = reserved && stable_step_id == RUNTIME_LEGACY_RUN_STOPPED_AUDIT_STEP_ID;
    if !is_terminal_audit && !is_recovery_audit && !is_lifecycle && !is_legacy {
        return Ok(());
    }

    if is_legacy {
        return match resolution {
            ActivityReconciliation::Completed { output } => {
                validate_legacy_source_attestation(record, output)
            }
            ActivityReconciliation::SafeToRetry | ActivityReconciliation::Cancelled => {
                Err(DurabilityError::InvalidEvent {
                    reason: "quarantined v1 RunStopped audit accepts only a typed completed operator attestation"
                        .into(),
                })
            }
        };
    }

    match resolution {
        ActivityReconciliation::Cancelled => Err(DurabilityError::InvalidEvent {
            reason: format!(
                "reserved fail-closed audit activity `{}` cannot be reconciled as cancelled",
                record.definition.activity_id
            ),
        }),
        ActivityReconciliation::SafeToRetry if is_lifecycle => {
            Err(DurabilityError::InvalidEvent {
                reason: "invocation audit lifecycle reconciliation is completion-only; reconcile the external RunStopped outcome explicitly"
                    .into(),
            })
        }
        ActivityReconciliation::SafeToRetry => {
            validate_terminal_replay_input(record, is_recovery_audit, run_id)
        }
        ActivityReconciliation::Completed { output } if is_lifecycle => {
            if output.get("status").and_then(Value::as_str) == Some("not_started") {
                let boundary_sequence = lifecycle_schedule_sequence.ok_or_else(|| {
                    DurabilityError::InvalidEvent {
                        reason: "orphan invocation lifecycle schedule boundary was not found"
                            .into(),
                    }
                })?;
                validate_lifecycle_not_started_output(
                    run_id,
                    activities,
                    record,
                    output,
                    boundary_sequence,
                    true,
                )
            } else {
                validate_lifecycle_reconciliation_output(run_id, activities, record, output)
            }
        }
        ActivityReconciliation::Completed { output }
            if is_terminal_audit
                && record.definition.input.get("legacy_resolution").is_some() =>
        {
            validate_legacy_terminal_resolution(activities, record, output)
        }
        ActivityReconciliation::Completed { output } => {
            validate_reserved_audit_completion_output(record, output)
        }
    }
}

fn validate_legacy_source_attestation(
    source: &ActivityRecord,
    output: &Value,
) -> DurabilityResult<()> {
    let attempt = source
        .latest_attempt()
        .ok_or_else(|| DurabilityError::InvalidEvent {
            reason: "legacy RunStopped source has no attempt to attest".into(),
        })?;
    let resolution: crate::durable_runtime::DurableLegacyRunStoppedResolutionEnvelope =
        serde_json::from_value(output.clone()).map_err(|_| DurabilityError::InvalidEvent {
            reason: "legacy RunStopped source attestation is malformed".into(),
        })?;
    let expected_output_hash = None;
    if source.definition.stable_step_id != RUNTIME_LEGACY_RUN_STOPPED_AUDIT_STEP_ID
        || source.definition.logical_key != "terminal"
        || attempt.status == ActivityAttemptStatus::Completed
        || resolution.schema_version
            != crate::durable_runtime::LEGACY_RUN_STOPPED_RESOLUTION_SCHEMA_VERSION
        || resolution.kind != crate::durable_runtime::LEGACY_RUN_STOPPED_RESOLUTION_KIND
        || resolution.source_activity_id != source.definition.activity_id
        || resolution.source_attempt != attempt.attempt
        || resolution.source_started_sequence != attempt.started_sequence
        || resolution.source_output_hash != expected_output_hash
        || resolution.terminal_receipt.reason.is_empty()
    {
        return Err(DurabilityError::InvalidEvent {
            reason: "legacy RunStopped source attestation does not match its durable attempt"
                .into(),
        });
    }
    Ok(())
}

fn validate_legacy_terminal_resolution(
    activities: &BTreeMap<String, ActivityRecord>,
    bridge: &ActivityRecord,
    output: &Value,
) -> DurabilityResult<()> {
    if bridge.definition.stable_step_id != RUNTIME_RUN_STOPPED_AUDIT_STEP_ID
        || bridge.definition.logical_key != "terminal"
    {
        return Err(DurabilityError::InvalidEvent {
            reason: "legacy terminal resolution must use the canonical terminal bridge".into(),
        });
    }
    validate_legacy_resolution_source(activities, &bridge.definition)?;
    let source: crate::durable_runtime::DurableLegacyRunStoppedResolutionSource = bridge
        .definition
        .input
        .get("legacy_resolution")
        .cloned()
        .and_then(|value| serde_json::from_value(value).ok())
        .ok_or_else(|| DurabilityError::InvalidEvent {
            reason: "legacy terminal resolution bridge has a malformed source binding".into(),
        })?;
    let resolution: crate::durable_runtime::DurableLegacyRunStoppedResolutionEnvelope =
        serde_json::from_value(output.clone()).map_err(|_| DurabilityError::InvalidEvent {
            reason: "legacy terminal resolution envelope is malformed".into(),
        })?;
    if resolution.schema_version
        != crate::durable_runtime::LEGACY_RUN_STOPPED_RESOLUTION_SCHEMA_VERSION
        || resolution.kind != crate::durable_runtime::LEGACY_RUN_STOPPED_RESOLUTION_KIND
        || resolution.source_activity_id != source.source_activity_id
        || resolution.source_attempt != source.source_attempt
        || resolution.source_started_sequence != source.source_started_sequence
        || resolution.source_output_hash != source.source_output_hash
        || resolution.terminal_receipt.reason.is_empty()
    {
        return Err(DurabilityError::InvalidEvent {
            reason: "legacy terminal resolution does not match its accepted source receipt".into(),
        });
    }
    Ok(())
}

fn validate_terminal_replay_input(
    record: &ActivityRecord,
    recovery: bool,
    run_id: &str,
) -> DurabilityResult<()> {
    let replay_value = record
        .definition
        .input
        .get("replay")
        .ok_or_else(|| DurabilityError::InvalidEvent {
            reason: "legacy/hash-only terminal audit intent cannot be retried automatically; reconcile it as Completed with the exact accepted output"
                .into(),
        })?;
    let replay = replay_value
        .as_object()
        .ok_or_else(|| DurabilityError::InvalidEvent {
            reason: "reserved audit replay envelope must be an object".into(),
        })?;
    let valid_schema = replay.get("schema_version").and_then(Value::as_u64) == Some(1);
    let valid_run_id = replay
        .get("audit_run_id")
        .and_then(Value::as_str)
        .is_some_and(|value| {
            value == run_id && !value.is_empty() && !value.chars().any(char::is_control)
        });
    let invocation_id = replay.get("invocation_id").and_then(Value::as_str);
    let valid_invocation_id = invocation_id
        .is_some_and(|value| !value.is_empty() && !value.chars().any(char::is_control));
    let valid_sequence = replay
        .get("run_stopped_sequence")
        .and_then(Value::as_u64)
        .is_some_and(|sequence| sequence > 0);
    let valid_receipt = replay.get("terminal_receipt").is_some_and(Value::is_object);
    let valid_audit_reason = replay
        .get("audit_reason")
        .and_then(Value::as_str)
        .is_some_and(|reason| !reason.is_empty());
    let valid_audit_turns = replay.get("audit_turns").and_then(Value::as_u64).is_some();
    let valid_kind = replay.get("kind").and_then(Value::as_str)
        == Some(if recovery { "recovery" } else { "canonical" });
    let logical_key_matches = if recovery {
        invocation_id.is_some_and(|value| value == record.definition.logical_key)
    } else {
        record.definition.logical_key == "terminal"
    };
    if !valid_schema
        || !valid_run_id
        || !valid_invocation_id
        || !valid_sequence
        || !valid_receipt
        || !valid_audit_reason
        || !valid_audit_turns
        || !valid_kind
        || !logical_key_matches
    {
        return Err(DurabilityError::InvalidEvent {
            reason: format!(
                "reserved audit replay envelope is invalid for `{}`",
                record.definition.activity_id
            ),
        });
    }
    validate_replay_value(
        replay_value,
        if recovery { "recovery" } else { "canonical" },
    )?;
    let expected_output = if recovery {
        serde_json::json!({"accepted": true})
    } else {
        replay
            .get("terminal_receipt")
            .cloned()
            .ok_or_else(|| DurabilityError::InvalidEvent {
                reason: "terminal audit replay is missing its terminal receipt".into(),
            })?
    };
    let expected_hash = record
        .definition
        .input
        .get("expected_output_hash")
        .and_then(Value::as_str)
        .ok_or_else(|| DurabilityError::InvalidEvent {
            reason: "typed terminal audit replay is missing its expected output hash".into(),
        })?;
    if stable_input_hash(&expected_output) != expected_hash {
        return Err(DurabilityError::InvalidEvent {
            reason: "terminal audit replay expected output hash is invalid".into(),
        });
    }
    validate_audit_replay_binding(replay_value, true)?;
    Ok(())
}

fn validate_lifecycle_reconciliation_output(
    run_id: &str,
    activities: &BTreeMap<String, ActivityRecord>,
    record: &ActivityRecord,
    output: &Value,
) -> DurabilityResult<()> {
    let expected_run_id = record
        .definition
        .input
        .get("audit_run_id")
        .and_then(Value::as_str)
        .ok_or_else(|| DurabilityError::InvalidEvent {
            reason: "invocation lifecycle is missing its durable audit run identity".into(),
        })?;
    if expected_run_id != run_id {
        return Err(DurabilityError::InvalidEvent {
            reason: "invocation lifecycle audit identity does not match its durable run".into(),
        });
    }
    let expected_invocation_id = record
        .definition
        .input
        .get("invocation_id")
        .and_then(Value::as_str)
        .ok_or_else(|| DurabilityError::InvalidEvent {
            reason: "invocation lifecycle is missing its durable audit invocation identity".into(),
        })?;
    let status = output.get("status").and_then(Value::as_str);
    let identity = match status {
        Some("audit_closed" | "terminal_replay_authorized") => output
            .get("replay")
            .ok_or_else(|| {
            DurabilityError::InvalidEvent {
                reason: "closed invocation lifecycle reconciliation requires a replay envelope"
                    .into(),
            }
        })?,
        _ => {
            return Err(DurabilityError::InvalidEvent {
                reason: "ambiguous invocation lifecycle reconciliation requires an explicit audit_closed outcome"
                    .into(),
            })
        }
    };
    let actual_run_id = identity.get("audit_run_id").and_then(Value::as_str);
    let actual_invocation_id = identity.get("invocation_id").and_then(Value::as_str);
    if actual_run_id != Some(expected_run_id)
        || actual_invocation_id != Some(expected_invocation_id)
    {
        return Err(DurabilityError::InvalidEvent {
            reason: "invocation lifecycle reconciliation identity does not match its durable fence"
                .into(),
        });
    }
    let kind = identity
        .get("kind")
        .and_then(Value::as_str)
        .ok_or_else(|| DurabilityError::InvalidEvent {
            reason: "invocation lifecycle replay is missing its delivery kind".into(),
        })?;
    if !matches!(kind, "canonical" | "recovery" | "direct") {
        return Err(DurabilityError::InvalidEvent {
            reason: "invocation lifecycle replay has an unsupported delivery kind".into(),
        });
    }
    validate_replay_value(identity, kind)?;

    let linked_audit = activities.values().find(|candidate| {
        let candidate_kind = match candidate.definition.stable_step_id.as_str() {
            RUNTIME_RUN_STOPPED_AUDIT_STEP_ID => "canonical",
            RUNTIME_RECOVERY_RUN_STOPPED_AUDIT_STEP_ID => "recovery",
            _ => return false,
        };
        candidate_kind == kind
            && candidate.definition.input.get("replay") == Some(identity)
            && match candidate_kind {
                "canonical" => candidate.definition.logical_key == "terminal",
                "recovery" => candidate.definition.logical_key == expected_invocation_id,
                _ => false,
            }
    });
    if matches!(kind, "canonical" | "recovery") && linked_audit.is_none() {
        return Err(DurabilityError::InvalidEvent {
            reason: "invocation lifecycle replay does not match its linked terminal audit activity"
                .into(),
        });
    }
    if matches!(kind, "canonical" | "recovery") {
        let accepted_delivery = if status == Some("audit_closed") {
            if let Some((candidate, accepted_output)) = linked_audit.and_then(|candidate| {
                candidate
                    .completed_output()
                    .map(|output| (candidate, output))
            }) {
                validate_reserved_audit_completion_output(candidate, accepted_output)?;
                true
            } else {
                false
            }
        } else {
            false
        };
        let authorized_retry = if status == Some("terminal_replay_authorized") {
            if let Some(candidate) = linked_audit.filter(|candidate| {
                candidate.latest_attempt().is_some_and(|attempt| {
                    attempt.status == ActivityAttemptStatus::Failed
                        && attempt.retryable
                        && !attempt.effect_ambiguous
                })
            }) {
                validate_terminal_replay_input(candidate, kind == "recovery", expected_run_id)?;
                true
            } else {
                false
            }
        } else {
            false
        };
        if !accepted_delivery && !authorized_retry {
            return Err(DurabilityError::InvalidEvent {
                reason: "linked terminal audit lifecycle must record either an accepted delivery or an authorized retry"
                    .into(),
            });
        }
    } else if status != Some("audit_closed") {
        return Err(DurabilityError::InvalidEvent {
            reason: "direct invocation lifecycle reconciliation requires an audit_closed outcome"
                .into(),
        });
    }
    if kind == "direct"
        && activities.values().any(|candidate| {
            matches!(
                candidate.definition.stable_step_id.as_str(),
                RUNTIME_RUN_STOPPED_AUDIT_STEP_ID | RUNTIME_RECOVERY_RUN_STOPPED_AUDIT_STEP_ID
            ) && candidate
                .definition
                .input
                .get("replay")
                .and_then(|replay| replay.get("invocation_id"))
                .and_then(Value::as_str)
                == Some(expected_invocation_id)
        })
    {
        return Err(DurabilityError::InvalidEvent {
            reason: "invocation lifecycle must use the replay envelope from its linked terminal audit activity"
                .into(),
        });
    }
    Ok(())
}

fn validate_replay_value(replay: &Value, expected_kind: &str) -> DurabilityResult<()> {
    let object = replay
        .as_object()
        .ok_or_else(|| DurabilityError::InvalidEvent {
            reason: "terminal audit replay envelope must be an object".into(),
        })?;
    let valid = object.get("schema_version").and_then(Value::as_u64) == Some(1)
        && object.get("kind").and_then(Value::as_str) == Some(expected_kind)
        && object
            .get("run_stopped_sequence")
            .and_then(Value::as_u64)
            .is_some_and(|sequence| sequence > 0)
        && object.get("terminal_receipt").is_some_and(Value::is_object)
        && object
            .get("audit_reason")
            .and_then(Value::as_str)
            .is_some_and(|reason| !reason.is_empty())
        && object.get("audit_turns").and_then(Value::as_u64).is_some();
    if !valid {
        return Err(DurabilityError::InvalidEvent {
            reason: "terminal audit replay envelope is incomplete or malformed".into(),
        });
    }
    serde_json::from_value::<crate::durable_runtime::DurableRunStoppedAuditReplayEnvelope>(
        replay.clone(),
    )
    .map_err(|_| DurabilityError::InvalidEvent {
        reason: "terminal audit replay envelope contains unsupported fields or values".into(),
    })?;
    validate_audit_replay_binding(replay, false)?;
    let Some(receipt) = object.get("terminal_receipt").and_then(Value::as_object) else {
        return Err(DurabilityError::InvalidEvent {
            reason: "terminal audit replay receipt is malformed".into(),
        });
    };
    let valid_receipt = receipt
        .get("turns")
        .and_then(Value::as_u64)
        .is_some_and(|turns| usize::try_from(turns).is_ok())
        && receipt
            .get("reason")
            .and_then(Value::as_str)
            .is_some_and(|reason| !reason.is_empty())
        && receipt.get("usage").is_some_and(|usage| {
            usage.as_object().is_some_and(|usage| {
                usage.len() == 5
                    && usage.keys().all(|key| {
                        matches!(
                            key.as_str(),
                            "input_tokens"
                                | "output_tokens"
                                | "cache_creation_input_tokens"
                                | "cache_read_input_tokens"
                                | "reasoning_tokens"
                        )
                    })
                    && serde_json::from_value::<crate::types::Usage>(Value::Object(usage.clone()))
                        .is_ok()
            })
        });
    if !valid_receipt {
        return Err(DurabilityError::InvalidEvent {
            reason: "terminal audit replay receipt is malformed".into(),
        });
    }
    if expected_kind != "recovery" {
        let audit_turns = object.get("audit_turns").and_then(Value::as_u64);
        let audit_reason = object.get("audit_reason").and_then(Value::as_str);
        if audit_turns != receipt.get("turns").and_then(Value::as_u64)
            || audit_reason != receipt.get("reason").and_then(Value::as_str)
        {
            return Err(DurabilityError::InvalidEvent {
                reason: "terminal audit replay event does not match its terminal receipt".into(),
            });
        }
    }
    Ok(())
}

fn validate_audit_replay_binding(replay: &Value, require_stable: bool) -> DurabilityResult<()> {
    let binding =
        replay
            .get("audit_binding")
            .cloned()
            .ok_or_else(|| DurabilityError::InvalidEvent {
                reason: "terminal audit replay is missing its audit delivery binding".into(),
            })?;
    let binding: crate::observability::AuditReplayBinding = serde_json::from_value(binding)
        .map_err(|_| DurabilityError::InvalidEvent {
            reason: "terminal audit replay delivery binding is malformed".into(),
        })?;
    let valid_delivery_id = binding
        .delivery_id
        .as_deref()
        .is_none_or(|value| !value.is_empty() && !value.chars().any(char::is_control));
    if binding.schema_version != 1 || binding.max_preview_bytes == 0 || !valid_delivery_id {
        return Err(DurabilityError::InvalidEvent {
            reason: "terminal audit replay delivery binding is malformed".into(),
        });
    }
    if require_stable
        && (binding.delivery_id.is_none()
            || binding.sink_count == 0
            || binding.failure_mode != crate::observability::AuditFailureMode::FailClosed)
    {
        return Err(DurabilityError::InvalidEvent {
            reason: "terminal audit SafeToRetry requires a stable fail-closed delivery binding"
                .into(),
        });
    }
    Ok(())
}

fn validate_identifier(field: &'static str, value: &str) -> DurabilityResult<()> {
    if value.is_empty() || value.chars().any(char::is_control) {
        return Err(DurabilityError::InvalidIdentifier { field });
    }
    Ok(())
}

fn validate_source_checkpoint(
    destination_run_id: &str,
    source_run_id: &str,
    checkpoint: &Checkpoint,
) -> DurabilityResult<()> {
    validate_identifier("checkpoint_id", &checkpoint.checkpoint_id)?;
    validate_identifier("checkpoint_run_id", &checkpoint.run_id)?;
    validate_identifier("checkpoint_branch_id", &checkpoint.projection.branch_id)?;
    validate_projection_keyed_identities(&checkpoint.projection)?;
    if source_run_id == destination_run_id {
        return Err(DurabilityError::InvalidEvent {
            reason: "fork source and destination run IDs must differ".into(),
        });
    }
    if checkpoint.run_id != source_run_id {
        return Err(DurabilityError::InvalidEvent {
            reason: "fork checkpoint does not belong to its declared source run".into(),
        });
    }
    if checkpoint.event_sequence == 0 {
        return Err(DurabilityError::InvalidEvent {
            reason: "fork checkpoint must reference a persisted source event".into(),
        });
    }
    Ok(())
}

fn validate_projection_keyed_identities(projection: &RunProjection) -> DurabilityResult<()> {
    for (activity_id, record) in &projection.activities {
        if activity_id != &record.definition.activity_id {
            return Err(DurabilityError::InvalidEvent {
                reason: "fork checkpoint activity map key does not match its definition identity"
                    .into(),
            });
        }
        validate_identifier("activity_id", activity_id)?;
        validate_identifier("stable_step_id", &record.definition.stable_step_id)?;
        validate_identifier("logical_key", &record.definition.logical_key)?;
        if stable_input_hash(&record.definition.input) != record.definition.input_hash {
            return Err(DurabilityError::InvalidEvent {
                reason: "fork checkpoint activity input hash does not match its input".into(),
            });
        }
        if record.definition.side_effect_class == SideEffectClass::Idempotent
            && record
                .definition
                .idempotency_key
                .as_deref()
                .is_none_or(str::is_empty)
        {
            return Err(DurabilityError::InvalidEvent {
                reason: "fork checkpoint idempotent activity has no idempotency key".into(),
            });
        }
        for (index, attempt) in record.attempts.iter().enumerate() {
            let expected_attempt = u32::try_from(index)
                .ok()
                .and_then(|value| value.checked_add(1))
                .ok_or_else(|| DurabilityError::InvalidEvent {
                    reason: "fork checkpoint activity attempt counter overflowed".into(),
                })?;
            if attempt.attempt != expected_attempt || attempt.started_sequence == 0 {
                return Err(DurabilityError::InvalidEvent {
                    reason: "fork checkpoint activity attempts are not sequential".into(),
                });
            }
            if attempt
                .finished_sequence
                .is_some_and(|finished| finished < attempt.started_sequence)
            {
                return Err(DurabilityError::InvalidEvent {
                    reason: "fork checkpoint activity attempt finishes before it starts".into(),
                });
            }
            let terminal_shape = attempt.finished_sequence.is_some();
            let payload_matches_status = match attempt.status {
                ActivityAttemptStatus::Running => {
                    !terminal_shape
                        && attempt.output.is_none()
                        && attempt.output_hash.is_none()
                        && attempt.error.is_none()
                        && !attempt.retryable
                        && !attempt.effect_ambiguous
                }
                ActivityAttemptStatus::Completed => {
                    terminal_shape
                        && attempt.output.as_ref().is_some_and(|output| {
                            attempt.output_hash.as_deref()
                                == Some(stable_input_hash(output).as_str())
                        })
                        && !attempt.retryable
                        && !attempt.effect_ambiguous
                }
                ActivityAttemptStatus::Failed => {
                    terminal_shape && attempt.output.is_none() && attempt.output_hash.is_none()
                }
                ActivityAttemptStatus::ReconcileRequired => {
                    terminal_shape
                        && attempt.output.is_none()
                        && attempt.output_hash.is_none()
                        && attempt.effect_ambiguous
                }
                ActivityAttemptStatus::Cancelled => {
                    terminal_shape
                        && attempt.output.is_none()
                        && attempt.output_hash.is_none()
                        && !attempt.retryable
                        && !attempt.effect_ambiguous
                }
            };
            if !payload_matches_status {
                return Err(DurabilityError::InvalidEvent {
                    reason: "fork checkpoint activity attempt payload contradicts its status"
                        .into(),
                });
            }
            if index + 1 < record.attempts.len()
                && (attempt.status != ActivityAttemptStatus::Failed
                    || !attempt.retryable
                    || !retry_is_safe(&record.definition, attempt.effect_ambiguous))
            {
                return Err(DurabilityError::InvalidEvent {
                    reason: "fork checkpoint activity has an attempt after a terminal outcome"
                        .into(),
                });
            }
        }
    }
    for (approval_id, approval) in &projection.approvals {
        if approval_id != &approval.approval_id {
            return Err(DurabilityError::InvalidEvent {
                reason: "fork checkpoint approval map key does not match its record identity"
                    .into(),
            });
        }
        validate_identifier("approval_id", approval_id)?;
        if approval.requested_sequence == 0
            || (approval.status == DurableApprovalStatus::Pending
                && (approval.resolved_sequence.is_some()
                    || approval.resolved_at_unix_ms.is_some()
                    || approval.timed_out))
            || (approval.status != DurableApprovalStatus::Pending
                && approval.resolved_sequence.is_none())
        {
            return Err(DurabilityError::InvalidEvent {
                reason: "fork checkpoint approval lifecycle is inconsistent".into(),
            });
        }
    }
    for (artifact_id, versions) in &projection.artifacts {
        if versions
            .iter()
            .any(|metadata| artifact_id != &metadata.artifact_id)
        {
            return Err(DurabilityError::InvalidEvent {
                reason: "fork checkpoint artifact map key does not match its metadata identity"
                    .into(),
            });
        }
        validate_identifier("artifact_id", artifact_id)?;
        if versions.is_empty() {
            return Err(DurabilityError::InvalidEvent {
                reason: "fork checkpoint artifact has an empty version history".into(),
            });
        }
        for (index, metadata) in versions.iter().enumerate() {
            let expected_version = u64::try_from(index)
                .ok()
                .and_then(|value| value.checked_add(1))
                .ok_or_else(|| DurabilityError::InvalidEvent {
                    reason: "fork checkpoint artifact version counter overflowed".into(),
                })?;
            let expected_previous = index
                .checked_sub(1)
                .map(|previous| versions[previous].version_id.as_str());
            if metadata.version != expected_version
                || metadata.previous_version_id.as_deref() != expected_previous
            {
                return Err(DurabilityError::InvalidEvent {
                    reason: "fork checkpoint artifact version history is inconsistent".into(),
                });
            }
            validate_identifier("artifact_version_id", &metadata.version_id)?;
        }
    }
    Ok(())
}

fn validate_worker_lease_window(
    starts_at_unix_ms: u64,
    expires_at_unix_ms: u64,
) -> DurabilityResult<()> {
    let duration = expires_at_unix_ms
        .checked_sub(starts_at_unix_ms)
        .filter(|duration| *duration > 0 && *duration <= MAX_DURABLE_WORKER_LEASE_MS)
        .ok_or_else(|| DurabilityError::InvalidEvent {
            reason: format!(
                "durable worker lease must expire within {MAX_DURABLE_WORKER_LEASE_MS}ms after its claim or heartbeat"
            ),
        })?;
    debug_assert!(duration > 0);
    Ok(())
}

/// Compute a deterministic SHA-256 input hash over canonical JSON.
pub fn stable_input_hash(value: &Value) -> String {
    let mut canonical = Vec::new();
    write_canonical_json(value, &mut canonical);
    format!("sha256:{}", hex_sha256(&canonical))
}

/// Compute a deterministic opaque ID from length-framed UTF-8 fields.
pub fn stable_id(namespace: &str, parts: &[&str]) -> String {
    stable_identifier(namespace, parts)
}

fn stable_identifier(namespace: &str, parts: &[&str]) -> String {
    let mut framed = Vec::new();
    push_hash_field(&mut framed, namespace.as_bytes());
    for part in parts {
        push_hash_field(&mut framed, part.as_bytes());
    }
    format!("{namespace}_{}", hex_sha256(&framed))
}

fn push_hash_field(output: &mut Vec<u8>, field: &[u8]) {
    output.extend_from_slice(&(field.len() as u64).to_be_bytes());
    output.extend_from_slice(field);
}

fn write_canonical_json(value: &Value, output: &mut Vec<u8>) {
    match value {
        Value::Null => output.extend_from_slice(b"null"),
        Value::Bool(value) => output.extend_from_slice(if *value { b"true" } else { b"false" }),
        Value::Number(value) => output.extend_from_slice(value.to_string().as_bytes()),
        Value::String(value) => output.extend_from_slice(
            serde_json::to_string(value)
                .expect("serializing a JSON string cannot fail")
                .as_bytes(),
        ),
        Value::Array(values) => {
            output.push(b'[');
            for (index, value) in values.iter().enumerate() {
                if index != 0 {
                    output.push(b',');
                }
                write_canonical_json(value, output);
            }
            output.push(b']');
        }
        Value::Object(values) => {
            output.push(b'{');
            let mut keys: Vec<&String> = values.keys().collect();
            keys.sort_unstable();
            for (index, key) in keys.into_iter().enumerate() {
                if index != 0 {
                    output.push(b',');
                }
                output.extend_from_slice(
                    serde_json::to_string(key)
                        .expect("serializing a JSON key cannot fail")
                        .as_bytes(),
                );
                output.push(b':');
                write_canonical_json(&values[key], output);
            }
            output.push(b'}');
        }
    }
}

// A small self-contained SHA-256 keeps stable durable IDs available without expanding the core
// crate's dependency surface. It is intentionally private; callers consume the hex helpers above.
fn hex_sha256(input: &[u8]) -> String {
    const INITIAL: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
        0x5be0cd19,
    ];
    const K: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
        0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
        0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
        0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
        0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
        0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
        0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
        0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
        0xc67178f2,
    ];

    let bit_len = (input.len() as u64).wrapping_mul(8);
    let mut message = input.to_vec();
    message.push(0x80);
    while message.len() % 64 != 56 {
        message.push(0);
    }
    message.extend_from_slice(&bit_len.to_be_bytes());

    let mut hash = INITIAL;
    for chunk in message.chunks_exact(64) {
        let mut words = [0u32; 64];
        for (index, word) in words.iter_mut().take(16).enumerate() {
            let offset = index * 4;
            *word = u32::from_be_bytes([
                chunk[offset],
                chunk[offset + 1],
                chunk[offset + 2],
                chunk[offset + 3],
            ]);
        }
        for index in 16..64 {
            let s0 = words[index - 15].rotate_right(7)
                ^ words[index - 15].rotate_right(18)
                ^ (words[index - 15] >> 3);
            let s1 = words[index - 2].rotate_right(17)
                ^ words[index - 2].rotate_right(19)
                ^ (words[index - 2] >> 10);
            words[index] = words[index - 16]
                .wrapping_add(s0)
                .wrapping_add(words[index - 7])
                .wrapping_add(s1);
        }

        let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut h] = hash;
        for index in 0..64 {
            let sum1 = h
                .wrapping_add(e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25))
                .wrapping_add((e & f) ^ ((!e) & g))
                .wrapping_add(K[index])
                .wrapping_add(words[index]);
            let sum0 = (a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22))
                .wrapping_add((a & b) ^ (a & c) ^ (b & c));
            h = g;
            g = f;
            f = e;
            e = d.wrapping_add(sum1);
            d = c;
            c = b;
            b = a;
            a = sum0.wrapping_add(sum1);
        }
        hash[0] = hash[0].wrapping_add(a);
        hash[1] = hash[1].wrapping_add(b);
        hash[2] = hash[2].wrapping_add(c);
        hash[3] = hash[3].wrapping_add(d);
        hash[4] = hash[4].wrapping_add(e);
        hash[5] = hash[5].wrapping_add(f);
        hash[6] = hash[6].wrapping_add(g);
        hash[7] = hash[7].wrapping_add(h);
    }

    hash.iter().map(|word| format!("{word:08x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::governance::{PolicyDocument, PolicyEffect, PolicySnapshot};
    use serde_json::json;

    fn policy_snapshot(effect: PolicyEffect) -> PolicySnapshot {
        PolicySnapshot::seal(PolicyDocument {
            schema_version: crate::governance::GOVERNANCE_CONTRACT_VERSION,
            default_effect: effect,
            rules: Vec::new(),
        })
        .unwrap()
    }

    fn execute(decision: ActivityDecision) -> (String, u32) {
        match decision {
            ActivityDecision::Execute {
                activity_id,
                attempt,
                ..
            } => (activity_id, attempt),
            other => panic!("expected execute decision, got {other:?}"),
        }
    }

    fn test_terminal_receipt() -> Value {
        json!({
            "turns": 2,
            "reason": "completed",
            "usage": {
                "input_tokens": 3,
                "output_tokens": 5,
                "cache_creation_input_tokens": 0,
                "cache_read_input_tokens": 0,
                "reasoning_tokens": 0,
            },
        })
    }

    fn test_terminal_replay(kind: &str, invocation_id: &str) -> Value {
        json!({
            "schema_version": 1,
            "kind": kind,
            "terminal_receipt": test_terminal_receipt(),
            "audit_run_id": "audit-run",
            "invocation_id": invocation_id,
            "run_stopped_sequence": 7,
            "audit_turns": 2,
            "audit_reason": "completed",
            "audit_binding": {
                "schema_version": 1,
                "delivery_id": "test-audit-destination",
                "sink_count": 1,
                "payload_policy": "metadata_only",
                "failure_mode": "fail_closed",
                "max_preview_bytes": 4096,
            },
        })
    }

    fn test_terminal_audit_input(replay: &Value, expected_output: &Value) -> Value {
        json!({
            "replay": replay,
            "expected_output_hash": stable_input_hash(expected_output),
        })
    }

    fn test_lifecycle_input(invocation_id: &str) -> Value {
        json!({
            "schema_version": 1,
            "audit_run_id": "audit-run",
            "invocation_id": invocation_id,
        })
    }

    fn complete_canonical_source(run: &mut RunState, invocation_id: &str) {
        let canonical_replay = test_terminal_replay("canonical", invocation_id);
        let (lifecycle_id, lifecycle_attempt) = execute(
            run.prepare_activity(
                RUNTIME_INVOCATION_LIFECYCLE_STEP_ID,
                invocation_id,
                test_lifecycle_input(invocation_id),
                SideEffectClass::ReconcileRequired,
                None,
            )
            .unwrap(),
        );
        let (canonical_id, canonical_attempt) = execute(
            run.prepare_activity(
                RUNTIME_RUN_STOPPED_AUDIT_STEP_ID,
                "terminal",
                test_terminal_audit_input(&canonical_replay, &test_terminal_receipt()),
                SideEffectClass::ReconcileRequired,
                None,
            )
            .unwrap(),
        );
        run.complete_activity(&canonical_id, canonical_attempt, test_terminal_receipt())
            .unwrap();
        run.complete_activity(
            &lifecycle_id,
            lifecycle_attempt,
            json!({"status": "audit_closed", "replay": canonical_replay}),
        )
        .unwrap();
    }

    fn test_reconciliation_run(
        stable_step_id: &str,
        logical_key: &str,
        input: Value,
    ) -> (RunState, String) {
        let mut run = RunState::new("session", "audit-run", DurabilityMode::Sync).unwrap();
        prepare_terminal_audit_prerequisites(&mut run, stable_step_id, &input);
        let (activity_id, attempt) = execute(
            run.prepare_activity(
                stable_step_id,
                logical_key,
                input,
                SideEffectClass::ReconcileRequired,
                None,
            )
            .unwrap(),
        );
        assert!(matches!(
            run.fail_activity(&activity_id, attempt, "unknown sink result", true, true)
                .unwrap(),
            ActivityDecision::ReconcileRequired { .. }
        ));
        (run, activity_id)
    }

    fn test_running_reserved_run(
        stable_step_id: &str,
        logical_key: &str,
        input: Value,
    ) -> (RunState, String, u32) {
        let mut run = RunState::new("session", "audit-run", DurabilityMode::Sync).unwrap();
        prepare_terminal_audit_prerequisites(&mut run, stable_step_id, &input);
        let (activity_id, attempt) = execute(
            run.prepare_activity(
                stable_step_id,
                logical_key,
                input,
                SideEffectClass::ReconcileRequired,
                None,
            )
            .unwrap(),
        );
        (run, activity_id, attempt)
    }

    fn prepare_terminal_audit_prerequisites(
        run: &mut RunState,
        stable_step_id: &str,
        input: &Value,
    ) {
        if !matches!(
            stable_step_id,
            RUNTIME_RUN_STOPPED_AUDIT_STEP_ID | RUNTIME_RECOVERY_RUN_STOPPED_AUDIT_STEP_ID
        ) {
            return;
        }
        let Some(invocation_id) = input
            .get("replay")
            .and_then(|replay| replay.get("invocation_id"))
            .and_then(Value::as_str)
        else {
            return;
        };
        if stable_step_id == RUNTIME_RECOVERY_RUN_STOPPED_AUDIT_STEP_ID {
            complete_canonical_source(run, "canonical-source-invocation");
        }
        execute(
            run.prepare_activity(
                RUNTIME_INVOCATION_LIFECYCLE_STEP_ID,
                invocation_id,
                test_lifecycle_input(invocation_id),
                SideEffectClass::ReconcileRequired,
                None,
            )
            .unwrap(),
        );
    }

    fn assert_raw_v2_event_rejected(run: &RunState, event_id: &str, kind: RunEventKind) {
        let event = RunEvent {
            schema_version: DURABILITY_SCHEMA_VERSION,
            run_id: run.run_id().to_string(),
            sequence: run.next_sequence(),
            event_id: event_id.to_string(),
            kind,
        };

        let mut appended = run.clone();
        assert!(matches!(
            appended.append_event(event.clone()),
            Err(DurabilityError::InvalidEvent { .. })
        ));
        assert_eq!(&appended, run);

        let mut replayed_events = run.events().to_vec();
        replayed_events.push(event.clone());
        assert!(matches!(
            RunState::from_events(replayed_events),
            Err(DurabilityError::InvalidEvent { .. })
        ));

        // Preserve the historical v2 contract independently of the current writer schema. The
        // recursive rewrite also migrates typed envelope versions embedded in event payloads.
        fn rewrite_v3_to_v2(value: &mut Value) {
            match value {
                Value::Object(object) => {
                    for (key, value) in object {
                        if key == "schema_version"
                            && value.as_u64() == Some(u64::from(DURABILITY_SCHEMA_VERSION))
                        {
                            *value = json!(TERMINAL_ACTIVITY_INVARIANT_SCHEMA_VERSION);
                        } else {
                            rewrite_v3_to_v2(value);
                        }
                    }
                }
                Value::Array(values) => values.iter_mut().for_each(rewrite_v3_to_v2),
                _ => {}
            }
        }
        let mut historical = serde_json::to_value(run.events()).unwrap();
        rewrite_v3_to_v2(&mut historical);
        let mut historical: Vec<RunEvent> = serde_json::from_value(historical).unwrap();
        let mut historical_event = serde_json::to_value(&event).unwrap();
        rewrite_v3_to_v2(&mut historical_event);
        historical.push(serde_json::from_value(historical_event).unwrap());
        assert!(matches!(
            RunState::from_events(historical),
            Err(DurabilityError::InvalidEvent { .. })
        ));
    }

    fn test_linked_lifecycle_run(kind: &str) -> (RunState, String, String, Value) {
        let invocation_id = "invocation-1";
        let replay = test_terminal_replay(kind, invocation_id);
        let expected_output = if kind == "canonical" {
            test_terminal_receipt()
        } else {
            json!({"accepted": true})
        };
        let (audit_step, audit_logical_key) = if kind == "canonical" {
            (RUNTIME_RUN_STOPPED_AUDIT_STEP_ID, "terminal")
        } else {
            (RUNTIME_RECOVERY_RUN_STOPPED_AUDIT_STEP_ID, invocation_id)
        };
        let mut run = RunState::new("session", "audit-run", DurabilityMode::Sync).unwrap();
        if kind == "recovery" {
            complete_canonical_source(&mut run, "canonical-source-invocation");
        }
        let (lifecycle_id, lifecycle_attempt) = execute(
            run.prepare_activity(
                RUNTIME_INVOCATION_LIFECYCLE_STEP_ID,
                invocation_id,
                test_lifecycle_input(invocation_id),
                SideEffectClass::ReconcileRequired,
                None,
            )
            .unwrap(),
        );
        let (audit_id, audit_attempt) = execute(
            run.prepare_activity(
                audit_step,
                audit_logical_key,
                test_terminal_audit_input(&replay, &expected_output),
                SideEffectClass::ReconcileRequired,
                None,
            )
            .unwrap(),
        );
        run.fail_activity(
            &audit_id,
            audit_attempt,
            "unknown terminal audit result",
            true,
            true,
        )
        .unwrap();
        run.fail_activity(
            &lifecycle_id,
            lifecycle_attempt,
            "unknown invocation lifecycle result",
            false,
            true,
        )
        .unwrap();
        (run, lifecycle_id, audit_id, replay)
    }

    fn legacy_v1_terminal_fixture(status: DurableRunStatus, reconcile_required: bool) -> Value {
        let run_id = match (status, reconcile_required) {
            (DurableRunStatus::Failed, false) => "legacy-v1-failed-running",
            (DurableRunStatus::Cancelled, false) => "legacy-v1-cancelled-running",
            (DurableRunStatus::Failed, true) => "legacy-v1-failed-reconcile-required",
            _ => panic!("unsupported legacy fixture shape"),
        };
        let mut run = RunState::new("legacy-session", run_id, DurabilityMode::Sync).unwrap();
        let (activity_id, attempt) = execute(
            run.prepare_activity(
                "legacy-external-write-v1",
                "write-1",
                json!({"value": 1}),
                SideEffectClass::ReconcileRequired,
                None,
            )
            .unwrap(),
        );
        if reconcile_required {
            run.fail_activity(&activity_id, attempt, "unknown outcome", false, true)
                .unwrap();
        }

        let sequence = run.next_sequence();
        let (kind, serialized_status, pause_reason) = match status {
            DurableRunStatus::Failed => (
                RunEventKind::RunFailed {
                    error: "legacy failure".into(),
                },
                "failed",
                Some("legacy failure"),
            ),
            DurableRunStatus::Cancelled => (
                RunEventKind::RunCancelled {
                    reason: Some("legacy cancellation".into()),
                },
                "cancelled",
                Some("legacy cancellation"),
            ),
            _ => unreachable!(),
        };
        let terminal_event = RunEvent {
            schema_version: 1,
            run_id: run_id.into(),
            sequence,
            event_id: format!("legacy-terminal-{sequence}"),
            kind,
        };
        let mut fixture = serde_json::to_value(run).unwrap();
        fixture["schema_version"] = json!(1);
        for event in fixture["events"].as_array_mut().unwrap() {
            event["schema_version"] = json!(1);
        }
        fixture["events"]
            .as_array_mut()
            .unwrap()
            .push(serde_json::to_value(terminal_event).unwrap());
        fixture["projection"]["status"] = json!(serialized_status);
        fixture["projection"]["pause_reason"] = json!(pause_reason);
        fixture
    }

    #[test]
    fn canonical_input_hash_is_order_independent_and_sha256_is_correct() {
        assert_eq!(
            hex_sha256(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(
            stable_input_hash(&json!({"b": 2, "a": [1, true]})),
            stable_input_hash(&json!({"a": [1, true], "b": 2}))
        );
    }

    #[test]
    fn event_log_is_monotonic_and_deduplicates_by_stable_id() {
        let mut run = RunState::new("session-1", "run-1", DurabilityMode::Sync).unwrap();
        let kind = RunEventKind::StateReplaced {
            state: json!({"n": 1}),
        };
        let event = RunEvent {
            schema_version: DURABILITY_SCHEMA_VERSION,
            run_id: "run-1".into(),
            sequence: 2,
            event_id: "state-1".into(),
            kind: kind.clone(),
        };
        assert_eq!(
            run.append_event(event.clone()).unwrap(),
            AppendOutcome::Appended { sequence: 2 }
        );
        let mut retry = event;
        retry.sequence = 999;
        assert_eq!(
            run.append_event(retry).unwrap(),
            AppendOutcome::Deduplicated { sequence: 2 }
        );

        let gap = RunEvent {
            schema_version: DURABILITY_SCHEMA_VERSION,
            run_id: "run-1".into(),
            sequence: 4,
            event_id: "state-2".into(),
            kind,
        };
        assert_eq!(
            run.append_event(gap).unwrap_err(),
            DurabilityError::NonMonotonicSequence {
                expected: 3,
                actual: 4
            }
        );
        assert_eq!(run.projection().state, json!({"n": 1}));
    }

    #[test]
    fn crash_replay_retries_pure_attempt_but_reuses_committed_output() {
        let mut run = RunState::new("session", "run", DurabilityMode::Sync).unwrap();
        let (activity_id, first_attempt) = execute(
            run.prepare_activity(
                "fetch-profile-v1",
                "turn-1/profile",
                json!({"user": 7}),
                SideEffectClass::Pure,
                None,
            )
            .unwrap(),
        );
        assert_eq!(first_attempt, 1);

        let mut recovered = RunState::from_events(run.events().to_vec()).unwrap();
        let (recovered_id, second_attempt) = execute(
            recovered
                .prepare_activity(
                    "fetch-profile-v1",
                    "turn-1/profile",
                    json!({"user": 7}),
                    SideEffectClass::Pure,
                    None,
                )
                .unwrap(),
        );
        assert_eq!(recovered_id, activity_id);
        assert_eq!(second_attempt, 2);
        recovered
            .complete_activity(&activity_id, second_attempt, json!({"name": "Ada"}))
            .unwrap();

        let mut replayed = RunState::from_events(recovered.events().to_vec()).unwrap();
        assert_eq!(
            replayed
                .prepare_activity(
                    "fetch-profile-v1",
                    "turn-1/profile",
                    json!({"user": 7}),
                    SideEffectClass::Pure,
                    None,
                )
                .unwrap(),
            ActivityDecision::ReuseCompleted {
                activity_id,
                output: json!({"name": "Ada"})
            }
        );
    }

    #[test]
    fn ambiguous_external_effect_requires_explicit_reconciliation() {
        let mut run = RunState::new("session", "run", DurabilityMode::Sync).unwrap();
        let (activity_id, attempt) = execute(
            run.prepare_activity(
                "send-payment-v1",
                "invoice-42",
                json!({"amount": 100}),
                SideEffectClass::ReconcileRequired,
                None,
            )
            .unwrap(),
        );
        let decision = run
            .fail_activity(&activity_id, attempt, "connection lost", true, true)
            .unwrap();
        assert!(matches!(
            decision,
            ActivityDecision::ReconcileRequired { .. }
        ));
        assert_eq!(run.status(), DurableRunStatus::ReconcileRequired);
        assert_eq!(
            run.prepare_activity(
                "send-payment-v1",
                "invoice-42",
                json!({"amount": 100}),
                SideEffectClass::ReconcileRequired,
                None,
            )
            .unwrap_err(),
            DurabilityError::RunRequiresReconciliation
        );

        run.reconcile_activity(
            "operator-check-1",
            &activity_id,
            ActivityReconciliation::SafeToRetry,
        )
        .unwrap();
        run.apply_command(RunCommand::Resume {
            command_id: "resume-after-check".into(),
            approvals: vec![],
        })
        .unwrap();
        let (_, second_attempt) = execute(
            run.prepare_activity(
                "send-payment-v1",
                "invoice-42",
                json!({"amount": 100}),
                SideEffectClass::ReconcileRequired,
                None,
            )
            .unwrap(),
        );
        assert_eq!(second_attempt, 2);
    }

    #[test]
    fn reserved_terminal_audit_activities_reject_cancelled_reconciliation_atomically() {
        let invocation_id = "invocation-1";
        let canonical_replay = test_terminal_replay("canonical", invocation_id);
        let recovery_replay = test_terminal_replay("recovery", invocation_id);
        let cases = [
            (
                RUNTIME_RUN_STOPPED_AUDIT_STEP_ID,
                "terminal",
                test_terminal_audit_input(&canonical_replay, &test_terminal_receipt()),
            ),
            (
                RUNTIME_RECOVERY_RUN_STOPPED_AUDIT_STEP_ID,
                invocation_id,
                test_terminal_audit_input(&recovery_replay, &json!({"accepted": true})),
            ),
            (
                RUNTIME_INVOCATION_LIFECYCLE_STEP_ID,
                invocation_id,
                test_lifecycle_input(invocation_id),
            ),
        ];

        for (index, (stable_step_id, logical_key, input)) in cases.into_iter().enumerate() {
            let (mut run, activity_id) =
                test_reconciliation_run(stable_step_id, logical_key, input);
            let before = run.clone();
            assert!(matches!(
                run.reconcile_activity(
                    &format!("cancel-reserved-{index}"),
                    &activity_id,
                    ActivityReconciliation::Cancelled,
                ),
                Err(DurabilityError::InvalidEvent { .. })
            ));
            assert_eq!(run, before);
        }
    }

    #[test]
    fn raw_v2_events_cannot_bypass_reserved_reconciliation_validation() {
        let invocation_id = "invocation-1";
        let (lifecycle, lifecycle_id) = test_reconciliation_run(
            RUNTIME_INVOCATION_LIFECYCLE_STEP_ID,
            invocation_id,
            test_lifecycle_input(invocation_id),
        );
        let lifecycle_attempt = lifecycle
            .activity(&lifecycle_id)
            .unwrap()
            .latest_attempt()
            .unwrap()
            .attempt;
        assert_raw_v2_event_rejected(
            &lifecycle,
            "raw-lifecycle-safe-to-retry",
            RunEventKind::ActivityReconciled {
                activity_id: lifecycle_id,
                attempt: lifecycle_attempt,
                resolution: ActivityReconciliation::SafeToRetry,
            },
        );

        let cancellation_cases = [
            (
                RUNTIME_RUN_STOPPED_AUDIT_STEP_ID,
                "terminal",
                test_terminal_audit_input(
                    &test_terminal_replay("canonical", invocation_id),
                    &test_terminal_receipt(),
                ),
            ),
            (
                RUNTIME_RECOVERY_RUN_STOPPED_AUDIT_STEP_ID,
                invocation_id,
                test_terminal_audit_input(
                    &test_terminal_replay("recovery", invocation_id),
                    &json!({"accepted": true}),
                ),
            ),
            (
                RUNTIME_INVOCATION_LIFECYCLE_STEP_ID,
                invocation_id,
                test_lifecycle_input(invocation_id),
            ),
        ];
        for (index, (stable_step_id, logical_key, input)) in
            cancellation_cases.into_iter().enumerate()
        {
            let (run, activity_id) = test_reconciliation_run(stable_step_id, logical_key, input);
            let attempt = run
                .activity(&activity_id)
                .unwrap()
                .latest_attempt()
                .unwrap()
                .attempt;
            assert_raw_v2_event_rejected(
                &run,
                &format!("raw-reserved-cancelled-{index}"),
                RunEventKind::ActivityReconciled {
                    activity_id,
                    attempt,
                    resolution: ActivityReconciliation::Cancelled,
                },
            );
        }

        for kind in ["canonical", "recovery"] {
            let replay = test_terminal_replay(kind, invocation_id);
            let (stable_step_id, logical_key, expected_output) = if kind == "canonical" {
                (
                    RUNTIME_RUN_STOPPED_AUDIT_STEP_ID,
                    "terminal",
                    test_terminal_receipt(),
                )
            } else {
                (
                    RUNTIME_RECOVERY_RUN_STOPPED_AUDIT_STEP_ID,
                    invocation_id,
                    json!({"accepted": true}),
                )
            };
            let (run, activity_id) = test_reconciliation_run(
                stable_step_id,
                logical_key,
                test_terminal_audit_input(&replay, &expected_output),
            );
            let attempt = run
                .activity(&activity_id)
                .unwrap()
                .latest_attempt()
                .unwrap()
                .attempt;
            assert_raw_v2_event_rejected(
                &run,
                &format!("raw-tampered-completed-{kind}"),
                RunEventKind::ActivityReconciled {
                    activity_id,
                    attempt,
                    resolution: ActivityReconciliation::Completed {
                        output: json!({"tampered": true}),
                    },
                },
            );
        }
    }

    #[test]
    fn raw_v2_reserved_failures_are_rejected_without_state_changes() {
        let invocation_id = "invocation-1";
        let cases = [
            (
                RUNTIME_RUN_STOPPED_AUDIT_STEP_ID,
                "terminal",
                test_terminal_audit_input(
                    &test_terminal_replay("canonical", invocation_id),
                    &test_terminal_receipt(),
                ),
            ),
            (
                RUNTIME_RECOVERY_RUN_STOPPED_AUDIT_STEP_ID,
                invocation_id,
                test_terminal_audit_input(
                    &test_terminal_replay("recovery", invocation_id),
                    &json!({"accepted": true}),
                ),
            ),
            (
                RUNTIME_INVOCATION_LIFECYCLE_STEP_ID,
                invocation_id,
                test_lifecycle_input(invocation_id),
            ),
        ];

        for (index, (stable_step_id, logical_key, input)) in cases.into_iter().enumerate() {
            let (run, activity_id, attempt) =
                test_running_reserved_run(stable_step_id, logical_key, input);
            assert_raw_v2_event_rejected(
                &run,
                &format!("raw-reserved-failed-{index}"),
                RunEventKind::ActivityAttemptFailed {
                    activity_id,
                    attempt,
                    error: "forged reserved failure".into(),
                    retryable: true,
                    effect_ambiguous: true,
                },
            );
        }
    }

    #[test]
    fn reserved_fail_activity_transitions_directly_to_reconciliation_required() {
        let invocation_id = "invocation-1";
        let cases = [
            (
                RUNTIME_RUN_STOPPED_AUDIT_STEP_ID,
                "terminal",
                test_terminal_audit_input(
                    &test_terminal_replay("canonical", invocation_id),
                    &test_terminal_receipt(),
                ),
            ),
            (
                RUNTIME_RECOVERY_RUN_STOPPED_AUDIT_STEP_ID,
                invocation_id,
                test_terminal_audit_input(
                    &test_terminal_replay("recovery", invocation_id),
                    &json!({"accepted": true}),
                ),
            ),
            (
                RUNTIME_INVOCATION_LIFECYCLE_STEP_ID,
                invocation_id,
                test_lifecycle_input(invocation_id),
            ),
        ];

        for (stable_step_id, logical_key, input) in cases {
            let (mut run, activity_id, attempt) =
                test_running_reserved_run(stable_step_id, logical_key, input);
            let before_event_count = run.events().len();

            assert!(matches!(
                run.fail_activity(
                    &activity_id,
                    attempt,
                    "unknown reserved audit outcome",
                    true,
                    true,
                )
                .unwrap(),
                ActivityDecision::ReconcileRequired { .. }
            ));
            assert_eq!(run.events().len(), before_event_count + 1);
            assert!(matches!(
                run.events().last().map(|event| &event.kind),
                Some(RunEventKind::ActivityReconciliationRequired {
                    activity_id: reconciled_id,
                    attempt: reconciled_attempt,
                    ..
                }) if reconciled_id == &activity_id && *reconciled_attempt == attempt
            ));
            assert!(!run.events().iter().any(|event| matches!(
                &event.kind,
                RunEventKind::ActivityAttemptFailed {
                    activity_id: failed_id,
                    ..
                } if failed_id == &activity_id
            )));
            assert_eq!(
                run.activity(&activity_id)
                    .unwrap()
                    .latest_attempt()
                    .unwrap()
                    .status,
                ActivityAttemptStatus::ReconcileRequired
            );
        }
    }

    #[test]
    fn raw_v2_reserved_completions_cannot_bypass_delivery_intent() {
        let invocation_id = "invocation-1";
        for kind in ["canonical", "recovery"] {
            let replay = test_terminal_replay(kind, invocation_id);
            let (stable_step_id, logical_key, expected_output) = if kind == "canonical" {
                (
                    RUNTIME_RUN_STOPPED_AUDIT_STEP_ID,
                    "terminal",
                    test_terminal_receipt(),
                )
            } else {
                (
                    RUNTIME_RECOVERY_RUN_STOPPED_AUDIT_STEP_ID,
                    invocation_id,
                    json!({"accepted": true}),
                )
            };
            let (run, activity_id, attempt) = test_running_reserved_run(
                stable_step_id,
                logical_key,
                test_terminal_audit_input(&replay, &expected_output),
            );
            let forged_output = json!({"accepted": false, "tampered": true});
            assert_raw_v2_event_rejected(
                &run,
                &format!("raw-wrong-terminal-output-{kind}"),
                RunEventKind::ActivityAttemptCompleted {
                    activity_id,
                    attempt,
                    output: forged_output.clone(),
                    output_hash: stable_input_hash(&forged_output),
                },
            );
        }

        let lifecycle_cases = [
            (
                "raw-lifecycle-replay-authorization",
                json!({
                    "status": "terminal_replay_authorized",
                    "replay": test_terminal_replay("canonical", invocation_id),
                }),
            ),
            ("raw-lifecycle-invalid-direct-close", {
                let mut replay = test_terminal_replay("direct", invocation_id);
                replay["audit_reason"] = json!("does-not-match-receipt");
                json!({"status": "audit_closed", "replay": replay})
            }),
            (
                "raw-lifecycle-unlinked-close",
                json!({
                    "status": "audit_closed",
                    "replay": test_terminal_replay("canonical", invocation_id),
                }),
            ),
        ];
        for (event_id, output) in lifecycle_cases {
            let (run, activity_id, attempt) = test_running_reserved_run(
                RUNTIME_INVOCATION_LIFECYCLE_STEP_ID,
                invocation_id,
                test_lifecycle_input(invocation_id),
            );
            assert_raw_v2_event_rejected(
                &run,
                event_id,
                RunEventKind::ActivityAttemptCompleted {
                    activity_id,
                    attempt,
                    output: output.clone(),
                    output_hash: stable_input_hash(&output),
                },
            );
        }
    }

    #[test]
    fn raw_v2_terminal_events_reject_unresolved_reserved_audit_marker() {
        let invocation_id = "invocation-1";
        let replay = test_terminal_replay("canonical", invocation_id);
        let (run, _) = test_reconciliation_run(
            RUNTIME_RUN_STOPPED_AUDIT_STEP_ID,
            "terminal",
            test_terminal_audit_input(&replay, &test_terminal_receipt()),
        );

        let terminal_cases = [
            ("raw-run-completed", RunEventKind::RunCompleted),
            (
                "raw-run-failed",
                RunEventKind::RunFailed {
                    error: "forged failure".into(),
                },
            ),
            (
                "raw-run-cancelled",
                RunEventKind::RunCancelled {
                    reason: Some("forged cancellation".into()),
                },
            ),
        ];
        for (event_id, kind) in terminal_cases {
            assert_raw_v2_event_rejected(&run, event_id, kind);
        }
    }

    #[test]
    fn invocation_lifecycle_rejects_safe_to_retry_atomically() {
        let invocation_id = "invocation-1";
        let (mut run, activity_id) = test_reconciliation_run(
            RUNTIME_INVOCATION_LIFECYCLE_STEP_ID,
            invocation_id,
            test_lifecycle_input(invocation_id),
        );
        let before = run.clone();

        assert!(matches!(
            run.reconcile_activity(
                "retry-lifecycle",
                &activity_id,
                ActivityReconciliation::SafeToRetry,
            ),
            Err(DurabilityError::InvalidEvent { .. })
        ));
        assert_eq!(run, before);
    }

    #[test]
    fn v2_hash_only_terminal_audit_schedule_is_rejected() {
        let receipt = test_terminal_receipt();
        let run = RunState::new("session", "hash-only-v2", DurabilityMode::Sync).unwrap();
        let input = json!({"input_hash": stable_input_hash(&receipt)});
        let definition = ActivityDefinition {
            activity_id: stable_id(
                "activity",
                &[
                    run.run_id(),
                    &run.projection().branch_id,
                    RUNTIME_RUN_STOPPED_AUDIT_STEP_ID,
                    "terminal",
                ],
            ),
            stable_step_id: RUNTIME_RUN_STOPPED_AUDIT_STEP_ID.into(),
            logical_key: "terminal".into(),
            input_hash: stable_input_hash(&input),
            input,
            side_effect_class: SideEffectClass::ReconcileRequired,
            idempotency_key: None,
        };
        assert_raw_v2_event_rejected(
            &run,
            "v2-hash-only-terminal-schedule",
            RunEventKind::ActivityScheduled { definition },
        );
    }

    #[test]
    fn typed_terminal_audit_replay_allows_safe_to_retry_for_canonical_and_recovery() {
        for (index, kind) in ["canonical", "recovery"].into_iter().enumerate() {
            let invocation_id = "invocation-1";
            let replay = test_terminal_replay(kind, invocation_id);
            let (stable_step_id, logical_key, expected_output) = if kind == "canonical" {
                (
                    RUNTIME_RUN_STOPPED_AUDIT_STEP_ID,
                    "terminal",
                    test_terminal_receipt(),
                )
            } else {
                (
                    RUNTIME_RECOVERY_RUN_STOPPED_AUDIT_STEP_ID,
                    invocation_id,
                    json!({"accepted": true}),
                )
            };
            let (mut run, activity_id) = test_reconciliation_run(
                stable_step_id,
                logical_key,
                test_terminal_audit_input(&replay, &expected_output),
            );

            run.reconcile_activity(
                &format!("retry-typed-audit-{index}"),
                &activity_id,
                ActivityReconciliation::SafeToRetry,
            )
            .unwrap();

            let attempt = run
                .activity(&activity_id)
                .unwrap()
                .latest_attempt()
                .unwrap();
            assert_eq!(attempt.status, ActivityAttemptStatus::Failed);
            assert!(attempt.retryable);
            assert!(!attempt.effect_ambiguous);
            assert_eq!(run.status(), DurableRunStatus::Paused);
        }
    }

    #[test]
    fn v2_terminal_audit_schedule_rejects_malformed_but_allows_non_replayable_binding() {
        let invocation_id = "invocation-1";
        for invalid_binding in 0..9 {
            let mut replay = test_terminal_replay("canonical", invocation_id);
            match invalid_binding {
                0 => {
                    replay.as_object_mut().unwrap().remove("audit_binding");
                }
                1 => {
                    replay["audit_binding"]
                        .as_object_mut()
                        .unwrap()
                        .remove("delivery_id");
                }
                2 => replay["audit_binding"]["sink_count"] = json!(0),
                3 => replay["audit_binding"]["failure_mode"] = json!("best_effort"),
                4 => replay["terminal_receipt"]["unexpected"] = json!(true),
                5 => replay["terminal_receipt"]["usage"]["unexpected"] = json!(1),
                6 => replay["audit_binding"]["unexpected"] = json!(true),
                7 => replay["unexpected"] = json!(true),
                8 => replay["terminal_receipt"]["usage"] = json!({}),
                _ => unreachable!(),
            }
            let mut run = RunState::new("session", "audit-run", DurabilityMode::Sync).unwrap();
            execute(
                run.prepare_activity(
                    RUNTIME_INVOCATION_LIFECYCLE_STEP_ID,
                    invocation_id,
                    test_lifecycle_input(invocation_id),
                    SideEffectClass::ReconcileRequired,
                    None,
                )
                .unwrap(),
            );
            let input = test_terminal_audit_input(&replay, &test_terminal_receipt());
            let event = RunEventKind::ActivityScheduled {
                definition: ActivityDefinition {
                    activity_id: stable_id(
                        "activity",
                        &[
                            run.run_id(),
                            &run.projection().branch_id,
                            RUNTIME_RUN_STOPPED_AUDIT_STEP_ID,
                            "terminal",
                        ],
                    ),
                    stable_step_id: RUNTIME_RUN_STOPPED_AUDIT_STEP_ID.into(),
                    logical_key: "terminal".into(),
                    input_hash: stable_input_hash(&input),
                    input,
                    side_effect_class: SideEffectClass::ReconcileRequired,
                    idempotency_key: None,
                },
            };
            if invalid_binding == 0 || invalid_binding >= 4 {
                assert_raw_v2_event_rejected(
                    &run,
                    &format!("malformed-binding-{invalid_binding}"),
                    event,
                );
            } else {
                run.append_event(RunEvent {
                    schema_version: DURABILITY_SCHEMA_VERSION,
                    run_id: run.run_id().into(),
                    sequence: run.next_sequence(),
                    event_id: format!("non-replayable-binding-{invalid_binding}"),
                    kind: event,
                })
                .unwrap();
            }
        }
    }

    #[test]
    fn v1_events_cannot_smuggle_v2_reserved_audit_activities_into_a_mixed_schema_log() {
        let run_id = "mixed-schema-audit-run";
        let invocation_id = "invocation-1";
        let cases = [
            (
                RUNTIME_RUN_STOPPED_AUDIT_STEP_ID,
                "terminal",
                test_terminal_audit_input(
                    &test_terminal_replay("canonical", invocation_id),
                    &test_terminal_receipt(),
                ),
            ),
            (
                RUNTIME_RECOVERY_RUN_STOPPED_AUDIT_STEP_ID,
                invocation_id,
                test_terminal_audit_input(
                    &test_terminal_replay("recovery", invocation_id),
                    &json!({"accepted": true}),
                ),
            ),
            (
                RUNTIME_INVOCATION_LIFECYCLE_STEP_ID,
                invocation_id,
                test_lifecycle_input(invocation_id),
            ),
        ];

        for (index, (stable_step_id, logical_key, input)) in cases.into_iter().enumerate() {
            let seed = RunState::new("session", run_id, DurabilityMode::Sync).unwrap();
            let branch_id = seed.projection().branch_id.clone();
            let activity_id = stable_id(
                "activity",
                &[run_id, &branch_id, stable_step_id, logical_key],
            );
            let mut start = seed.events()[0].clone();
            start.schema_version = 1;
            let schedule = RunEvent {
                schema_version: 1,
                run_id: run_id.into(),
                sequence: 2,
                event_id: format!("smuggled-v1-schedule-{index}"),
                kind: RunEventKind::ActivityScheduled {
                    definition: ActivityDefinition {
                        activity_id: activity_id.clone(),
                        stable_step_id: stable_step_id.into(),
                        logical_key: logical_key.into(),
                        input_hash: stable_input_hash(&input),
                        input,
                        side_effect_class: SideEffectClass::ReconcileRequired,
                        idempotency_key: None,
                    },
                },
            };
            let v2_attempt = RunEvent {
                schema_version: DURABILITY_SCHEMA_VERSION,
                run_id: run_id.into(),
                sequence: 3,
                event_id: format!("smuggled-v2-attempt-{index}"),
                kind: RunEventKind::ActivityAttemptStarted {
                    activity_id,
                    attempt: 1,
                },
            };

            assert!(matches!(
                RunState::from_events([start, schedule, v2_attempt]),
                Err(DurabilityError::InvalidEvent { .. })
            ));
        }
    }

    #[test]
    fn reserved_terminal_audit_completed_reconciliation_rejects_tampered_output() {
        for kind in ["canonical", "recovery"] {
            let invocation_id = "invocation-1";
            let replay = test_terminal_replay(kind, invocation_id);
            let (stable_step_id, logical_key, expected_output) = if kind == "canonical" {
                (
                    RUNTIME_RUN_STOPPED_AUDIT_STEP_ID,
                    "terminal",
                    test_terminal_receipt(),
                )
            } else {
                (
                    RUNTIME_RECOVERY_RUN_STOPPED_AUDIT_STEP_ID,
                    invocation_id,
                    json!({"accepted": true}),
                )
            };
            let (mut run, activity_id) = test_reconciliation_run(
                stable_step_id,
                logical_key,
                test_terminal_audit_input(&replay, &expected_output),
            );
            let before = run.clone();

            assert!(matches!(
                run.reconcile_activity(
                    &format!("complete-tampered-{kind}"),
                    &activity_id,
                    ActivityReconciliation::Completed {
                        output: json!({"tampered": true}),
                    },
                ),
                Err(DurabilityError::InvalidEvent { .. })
            ));
            assert_eq!(run, before);
        }
    }

    #[test]
    fn lifecycle_completed_reconciliation_requires_exact_linked_terminal_replay() {
        for kind in ["canonical", "recovery"] {
            let (mut premature, lifecycle_id, _, replay) = test_linked_lifecycle_run(kind);
            let before = premature.clone();
            assert!(matches!(
                premature.reconcile_activity(
                    &format!("authorize-before-safe-retry-{kind}"),
                    &lifecycle_id,
                    ActivityReconciliation::Completed {
                        output: json!({
                            "status": "terminal_replay_authorized",
                            "replay": replay,
                        }),
                    },
                ),
                Err(DurabilityError::InvalidEvent { .. })
            ));
            assert_eq!(premature, before);

            let (mut exact, exact_lifecycle_id, exact_audit_id, exact_replay) =
                test_linked_lifecycle_run(kind);
            exact
                .reconcile_activity(
                    &format!("retry-linked-audit-{kind}"),
                    &exact_audit_id,
                    ActivityReconciliation::SafeToRetry,
                )
                .unwrap();
            let before_wrong_status = exact.clone();
            assert!(matches!(
                exact.reconcile_activity(
                    &format!("close-linked-audit-{kind}"),
                    &exact_lifecycle_id,
                    ActivityReconciliation::Completed {
                        output: json!({
                            "status": "audit_closed",
                            "replay": exact_replay,
                        }),
                    },
                ),
                Err(DurabilityError::InvalidEvent { .. })
            ));
            assert_eq!(exact, before_wrong_status);
            exact
                .reconcile_activity(
                    &format!("authorize-linked-replay-{kind}"),
                    &exact_lifecycle_id,
                    ActivityReconciliation::Completed {
                        output: json!({
                            "status": "terminal_replay_authorized",
                            "replay": exact_replay,
                        }),
                    },
                )
                .unwrap();
            assert_eq!(
                exact
                    .activity(&exact_lifecycle_id)
                    .unwrap()
                    .latest_attempt()
                    .unwrap()
                    .status,
                ActivityAttemptStatus::Completed
            );

            let (mut wrong_identity, lifecycle_id, audit_id, mut replay) =
                test_linked_lifecycle_run(kind);
            wrong_identity
                .reconcile_activity(
                    &format!("retry-before-wrong-identity-{kind}"),
                    &audit_id,
                    ActivityReconciliation::SafeToRetry,
                )
                .unwrap();
            replay["invocation_id"] = json!("another-invocation");
            let before = wrong_identity.clone();
            assert!(matches!(
                wrong_identity.reconcile_activity(
                    &format!("wrong-identity-{kind}"),
                    &lifecycle_id,
                    ActivityReconciliation::Completed {
                        output: json!({
                            "status": "terminal_replay_authorized",
                            "replay": replay,
                        }),
                    },
                ),
                Err(DurabilityError::InvalidEvent { .. })
            ));
            assert_eq!(wrong_identity, before);

            let (mut wrong_replay, lifecycle_id, audit_id, mut replay) =
                test_linked_lifecycle_run(kind);
            wrong_replay
                .reconcile_activity(
                    &format!("retry-before-wrong-replay-{kind}"),
                    &audit_id,
                    ActivityReconciliation::SafeToRetry,
                )
                .unwrap();
            replay["run_stopped_sequence"] = json!(8);
            let before = wrong_replay.clone();
            assert!(matches!(
                wrong_replay.reconcile_activity(
                    &format!("wrong-linked-replay-{kind}"),
                    &lifecycle_id,
                    ActivityReconciliation::Completed {
                        output: json!({
                            "status": "terminal_replay_authorized",
                            "replay": replay,
                        }),
                    },
                ),
                Err(DurabilityError::InvalidEvent { .. })
            ));
            assert_eq!(wrong_replay, before);
        }
    }

    #[test]
    fn direct_lifecycle_completion_is_allowed_only_without_a_linked_terminal_marker() {
        let invocation_id = "invocation-1";
        let direct_replay = test_terminal_replay("direct", invocation_id);
        let (mut unlinked, lifecycle_id) = test_reconciliation_run(
            RUNTIME_INVOCATION_LIFECYCLE_STEP_ID,
            invocation_id,
            test_lifecycle_input(invocation_id),
        );
        unlinked
            .reconcile_activity(
                "complete-direct-unlinked",
                &lifecycle_id,
                ActivityReconciliation::Completed {
                    output: json!({
                        "status": "audit_closed",
                        "replay": direct_replay,
                    }),
                },
            )
            .unwrap();
        assert_eq!(unlinked.status(), DurableRunStatus::Paused);

        for kind in ["canonical", "recovery"] {
            let (mut linked, lifecycle_id, _, _) = test_linked_lifecycle_run(kind);
            let before = linked.clone();
            assert!(matches!(
                linked.reconcile_activity(
                    &format!("complete-direct-with-{kind}"),
                    &lifecycle_id,
                    ActivityReconciliation::Completed {
                        output: json!({
                            "status": "audit_closed",
                            "replay": test_terminal_replay("direct", invocation_id),
                        }),
                    },
                ),
                Err(DurabilityError::InvalidEvent { .. })
            ));
            assert_eq!(linked, before);
        }
    }

    #[test]
    fn direct_lifecycle_completion_rejects_receipt_event_mismatch_atomically() {
        let invocation_id = "invocation-1";
        for mismatch in ["turns", "reason"] {
            let mut direct_replay = test_terminal_replay("direct", invocation_id);
            if mismatch == "turns" {
                direct_replay["audit_turns"] = json!(3);
            } else {
                direct_replay["audit_reason"] = json!("different-reason");
            }
            let (mut run, lifecycle_id) = test_reconciliation_run(
                RUNTIME_INVOCATION_LIFECYCLE_STEP_ID,
                invocation_id,
                test_lifecycle_input(invocation_id),
            );
            let before = run.clone();

            assert!(matches!(
                run.reconcile_activity(
                    &format!("direct-{mismatch}-mismatch"),
                    &lifecycle_id,
                    ActivityReconciliation::Completed {
                        output: json!({
                            "status": "audit_closed",
                            "replay": direct_replay,
                        }),
                    },
                ),
                Err(DurabilityError::InvalidEvent { .. })
            ));
            assert_eq!(run, before);
        }
    }

    #[test]
    fn completed_parallel_activity_is_not_rerun_after_crash() {
        let mut run = RunState::new("session", "parallel", DurabilityMode::Sync).unwrap();
        let (left_id, left_attempt) = execute(
            run.prepare_activity(
                "parallel-step-v1",
                "left",
                json!({"side": "left"}),
                SideEffectClass::Pure,
                None,
            )
            .unwrap(),
        );
        let (right_id, right_attempt) = execute(
            run.prepare_activity(
                "parallel-step-v1",
                "right",
                json!({"side": "right"}),
                SideEffectClass::Pure,
                None,
            )
            .unwrap(),
        );
        run.complete_activity(&left_id, left_attempt, json!("left-result"))
            .unwrap();

        let mut recovered = RunState::from_events(run.events().to_vec()).unwrap();
        assert!(matches!(
            recovered
                .prepare_activity(
                    "parallel-step-v1",
                    "left",
                    json!({"side": "left"}),
                    SideEffectClass::Pure,
                    None,
                )
                .unwrap(),
            ActivityDecision::ReuseCompleted { .. }
        ));
        let (recovered_right_id, recovered_right_attempt) = execute(
            recovered
                .prepare_activity(
                    "parallel-step-v1",
                    "right",
                    json!({"side": "right"}),
                    SideEffectClass::Pure,
                    None,
                )
                .unwrap(),
        );
        assert_eq!(recovered_right_id, right_id);
        assert_eq!(right_attempt, 1);
        assert_eq!(recovered_right_attempt, 2);
    }

    #[test]
    fn fork_reuses_checkpointed_work_and_keeps_session_identity() {
        let mut source = RunState::new("session", "source", DurabilityMode::Sync).unwrap();
        source.replace_state("v1", json!({"value": 1})).unwrap();
        let (activity_id, attempt) = execute(
            source
                .prepare_activity(
                    "calculate-v1",
                    "answer",
                    json!({"x": 2}),
                    SideEffectClass::Pure,
                    None,
                )
                .unwrap(),
        );
        source
            .complete_activity(&activity_id, attempt, json!(4))
            .unwrap();
        let checkpoint = source.checkpoint("base", Some("base".into())).unwrap();
        source.replace_state("v2", json!({"value": 2})).unwrap();

        let fork = match source
            .apply_command(RunCommand::Fork {
                command_id: "fork-1".into(),
                new_run_id: "forked".into(),
                checkpoint_id: checkpoint.checkpoint_id,
                side_effects_reconciled: false,
            })
            .unwrap()
        {
            CommandOutcome::Forked { run } => run,
            other => panic!("unexpected command result: {other:?}"),
        };
        assert_eq!(fork.session_id(), source.session_id());
        assert_eq!(fork.parent_run_id(), Some("source"));
        assert_eq!(fork.projection().state, json!({"value": 1}));
        let mut fork = *fork;
        assert_eq!(
            fork.prepare_activity(
                "calculate-v1",
                "answer",
                json!({"x": 2}),
                SideEffectClass::Pure,
                None,
            )
            .unwrap(),
            ActivityDecision::ReuseCompleted {
                activity_id,
                output: json!(4)
            }
        );
    }

    #[test]
    fn fork_and_rewind_command_ids_bind_the_full_command_payload() {
        let mut source = RunState::new("session", "source", DurabilityMode::Sync).unwrap();
        let checkpoint = source.checkpoint("base", None).unwrap();

        source
            .apply_command(RunCommand::Fork {
                command_id: "fork-command".into(),
                new_run_id: "destination".into(),
                checkpoint_id: checkpoint.checkpoint_id.clone(),
                side_effects_reconciled: false,
            })
            .unwrap();
        assert!(matches!(
            source.apply_command(RunCommand::Fork {
                command_id: "fork-command".into(),
                new_run_id: "destination".into(),
                checkpoint_id: checkpoint.checkpoint_id.clone(),
                side_effects_reconciled: true,
            }),
            Err(DurabilityError::DuplicateEventConflict { .. })
        ));

        let mut rewind = RunState::new("session", "rewind-command", DurabilityMode::Sync).unwrap();
        let checkpoint = rewind.checkpoint("base", None).unwrap();
        rewind
            .apply_command(RunCommand::Rewind {
                command_id: "rewind-command".into(),
                checkpoint_id: checkpoint.checkpoint_id.clone(),
                side_effects_reconciled: false,
            })
            .unwrap();
        assert!(matches!(
            rewind.apply_command(RunCommand::Rewind {
                command_id: "rewind-command".into(),
                checkpoint_id: checkpoint.checkpoint_id,
                side_effects_reconciled: true,
            }),
            Err(DurabilityError::DuplicateEventConflict { .. })
        ));
    }

    #[test]
    fn fork_rejects_self_parenting_and_false_checkpoint_lineage() {
        let mut source = RunState::new("session", "source", DurabilityMode::Sync).unwrap();
        let checkpoint = source.checkpoint("base", None).unwrap();
        let before = source.clone();
        assert!(matches!(
            source.apply_command(RunCommand::Fork {
                command_id: "self-fork".into(),
                new_run_id: "source".into(),
                checkpoint_id: checkpoint.checkpoint_id.clone(),
                side_effects_reconciled: false,
            }),
            Err(DurabilityError::InvalidEvent { .. })
        ));
        assert_eq!(source, before);

        let mut false_checkpoint = checkpoint;
        false_checkpoint.run_id = "another-source".into();
        let replay = RunState::from_events([RunEvent {
            schema_version: DURABILITY_SCHEMA_VERSION,
            run_id: "destination".into(),
            sequence: 1,
            event_id: "false-lineage".into(),
            kind: RunEventKind::ForkedFrom {
                session_id: "session".into(),
                source_run_id: "source".into(),
                durability: DurabilityMode::Sync,
                source_checkpoint: Box::new(false_checkpoint),
                new_branch_id: "destination-branch".into(),
            },
        }]);
        assert!(matches!(replay, Err(DurabilityError::InvalidEvent { .. })));
    }

    #[test]
    fn fork_rejects_snapshot_map_keys_that_lie_about_record_identities() {
        let mut source = RunState::new("session", "source", DurabilityMode::Sync).unwrap();
        let ActivityDecision::Execute { activity_id, .. } = source
            .prepare_activity(
                "stable-step",
                "work-1",
                json!({"value": 1}),
                SideEffectClass::Pure,
                None,
            )
            .unwrap()
        else {
            panic!("expected activity execution");
        };
        let mut checkpoint = source.checkpoint("malicious-map-key", None).unwrap();
        let record = checkpoint
            .projection
            .activities
            .remove(&activity_id)
            .unwrap();
        checkpoint
            .projection
            .activities
            .insert("forged-map-key".into(), record);

        let replay = RunState::from_events([RunEvent {
            schema_version: DURABILITY_SCHEMA_VERSION,
            run_id: "destination".into(),
            sequence: 1,
            event_id: "malicious-snapshot-fork".into(),
            kind: RunEventKind::ForkedFrom {
                session_id: "session".into(),
                source_run_id: "source".into(),
                durability: DurabilityMode::Sync,
                source_checkpoint: Box::new(checkpoint),
                new_branch_id: "destination-branch".into(),
            },
        }]);
        assert!(matches!(replay, Err(DurabilityError::InvalidEvent { .. })));
    }

    #[test]
    fn fork_rejects_completed_attempt_without_output_instead_of_panicking() {
        let mut source = RunState::new("session", "source", DurabilityMode::Sync).unwrap();
        let (activity_id, attempt) = execute(
            source
                .prepare_activity(
                    "stable-step",
                    "work-1",
                    json!({"value": 1}),
                    SideEffectClass::Pure,
                    None,
                )
                .unwrap(),
        );
        source
            .complete_activity(&activity_id, attempt, json!({"done": true}))
            .unwrap();
        let mut checkpoint = source.checkpoint("malicious-completion", None).unwrap();
        checkpoint
            .projection
            .activities
            .get_mut(&activity_id)
            .unwrap()
            .attempts[0]
            .output = None;

        let replay = RunState::from_events([RunEvent {
            schema_version: DURABILITY_SCHEMA_VERSION,
            run_id: "destination".into(),
            sequence: 1,
            event_id: "malicious-completion-fork".into(),
            kind: RunEventKind::ForkedFrom {
                session_id: "session".into(),
                source_run_id: "source".into(),
                durability: DurabilityMode::Sync,
                source_checkpoint: Box::new(checkpoint),
                new_branch_id: "destination-branch".into(),
            },
        }]);
        assert!(matches!(replay, Err(DurabilityError::InvalidEvent { .. })));
    }

    #[test]
    fn fork_of_fork_rebinds_the_inherited_checkpoint_to_its_immediate_parent() {
        let mut source = RunState::new("session", "source", DurabilityMode::Sync).unwrap();
        let checkpoint = source.checkpoint("base", None).unwrap();
        let CommandOutcome::Forked { run: child, .. } = source
            .apply_command(RunCommand::Fork {
                command_id: "source-to-child".into(),
                new_run_id: "child".into(),
                checkpoint_id: checkpoint.checkpoint_id.clone(),
                side_effects_reconciled: false,
            })
            .unwrap()
        else {
            panic!("expected child fork");
        };
        let mut child = *child;
        let CommandOutcome::Forked {
            run: grandchild, ..
        } = child
            .apply_command(RunCommand::Fork {
                command_id: "child-to-grandchild".into(),
                new_run_id: "grandchild".into(),
                checkpoint_id: checkpoint.checkpoint_id,
                side_effects_reconciled: false,
            })
            .unwrap()
        else {
            panic!("expected grandchild fork");
        };
        assert_eq!(grandchild.parent_run_id(), Some("child"));
        let RunEventKind::ForkedFrom {
            source_run_id,
            source_checkpoint,
            ..
        } = &grandchild.events()[0].kind
        else {
            panic!("expected fork lineage event");
        };
        assert_eq!(source_run_id, "child");
        assert_eq!(source_checkpoint.run_id, "child");
        assert_eq!(source_checkpoint.event_sequence, 1);
        assert_eq!(
            RunState::from_events(grandchild.events().to_vec()).unwrap(),
            *grandchild
        );
    }

    #[test]
    fn inherited_checkpoint_uses_the_child_event_boundary_for_commands_and_replay() {
        let mut source =
            RunState::new("session", "high-sequence-source", DurabilityMode::Sync).unwrap();
        for index in 0..8 {
            source
                .replace_state(&format!("padding-{index}"), json!({"index": index}))
                .unwrap();
        }
        let checkpoint = source.checkpoint("high-boundary", None).unwrap();
        assert!(checkpoint.event_sequence > 4);
        let CommandOutcome::Forked { run: child } = source
            .apply_command(RunCommand::Fork {
                command_id: "high-sequence-fork".into(),
                new_run_id: "high-sequence-child".into(),
                checkpoint_id: checkpoint.checkpoint_id.clone(),
                side_effects_reconciled: false,
            })
            .unwrap()
        else {
            panic!("expected child fork");
        };
        let mut child = *child;
        let (activity_id, attempt) = execute(
            child
                .prepare_activity(
                    "send-v1",
                    "message",
                    json!({"to": "a@example.com"}),
                    SideEffectClass::Idempotent,
                    Some("message:a@example.com".into()),
                )
                .unwrap(),
        );
        child
            .complete_activity(&activity_id, attempt, json!({"sent": true}))
            .unwrap();
        let expected = DurabilityError::ReconcileRequired {
            activity_ids: vec![activity_id],
        };

        assert_eq!(
            child
                .apply_command(RunCommand::Rewind {
                    command_id: "child-rewind".into(),
                    checkpoint_id: checkpoint.checkpoint_id.clone(),
                    side_effects_reconciled: false,
                })
                .unwrap_err(),
            expected
        );
        assert_eq!(
            child
                .apply_command(RunCommand::Fork {
                    command_id: "child-fork".into(),
                    new_run_id: "grandchild".into(),
                    checkpoint_id: checkpoint.checkpoint_id.clone(),
                    side_effects_reconciled: false,
                })
                .unwrap_err(),
            expected
        );

        let forged_kinds = [
            RunEventKind::RunRewound {
                checkpoint_id: checkpoint.checkpoint_id.clone(),
                new_branch_id: "forged-rewind-branch".into(),
                side_effects_reconciled: false,
            },
            RunEventKind::ForkCreated {
                new_run_id: "forged-grandchild".into(),
                checkpoint_id: checkpoint.checkpoint_id,
                new_branch_id: "forged-fork-branch".into(),
                side_effects_reconciled: false,
            },
        ];
        for (index, kind) in forged_kinds.into_iter().enumerate() {
            let before = child.clone();
            let event = RunEvent {
                schema_version: DURABILITY_SCHEMA_VERSION,
                run_id: child.run_id().to_string(),
                sequence: child.next_sequence(),
                event_id: format!("forged-local-boundary-{index}"),
                kind,
            };
            assert_eq!(child.append_event(event).unwrap_err(), expected);
            assert_eq!(child, before);
        }
    }

    #[test]
    fn child_retry_of_inherited_non_pure_activity_requires_reconciliation() {
        let mut source = RunState::new("session", "retry-source", DurabilityMode::Sync).unwrap();
        let (activity_id, attempt) = execute(
            source
                .prepare_activity(
                    "send-v1",
                    "message",
                    json!({"to": "a@example.com"}),
                    SideEffectClass::Idempotent,
                    Some("message:a@example.com".into()),
                )
                .unwrap(),
        );
        let (_, source_retry_attempt) = execute(
            source
                .fail_activity(&activity_id, attempt, "retry", true, false)
                .unwrap(),
        );
        let checkpoint = source.checkpoint("retryable", None).unwrap();
        let CommandOutcome::Forked { run: child } = source
            .apply_command(RunCommand::Fork {
                command_id: "retry-fork".into(),
                new_run_id: "retry-child".into(),
                checkpoint_id: checkpoint.checkpoint_id.clone(),
                side_effects_reconciled: false,
            })
            .unwrap()
        else {
            panic!("expected child fork");
        };
        let mut child = *child;
        let (retried_activity_id, retry_attempt) = execute(
            child
                .prepare_activity(
                    "send-v1",
                    "message",
                    json!({"to": "a@example.com"}),
                    SideEffectClass::Idempotent,
                    Some("message:a@example.com".into()),
                )
                .unwrap(),
        );
        assert_eq!(retried_activity_id, activity_id);
        assert_eq!(retry_attempt, source_retry_attempt + 1);
        child
            .complete_activity(&activity_id, retry_attempt, json!({"sent": true}))
            .unwrap();

        assert_eq!(
            child
                .apply_command(RunCommand::Rewind {
                    command_id: "discard-inherited-retry".into(),
                    checkpoint_id: checkpoint.checkpoint_id,
                    side_effects_reconciled: false,
                })
                .unwrap_err(),
            DurabilityError::ReconcileRequired {
                activity_ids: vec![activity_id],
            }
        );
    }

    #[test]
    fn rewind_is_append_only_restores_projection_and_creates_new_branch() {
        let mut run = RunState::new("session", "rewind", DurabilityMode::Sync).unwrap();
        run.replace_state("v1", json!({"value": 1})).unwrap();
        let checkpoint = run.checkpoint("one", None).unwrap();
        let original_branch = run.projection().branch_id.clone();
        run.replace_state("v2", json!({"value": 2})).unwrap();
        let events_before = run.events().len();

        run.apply_command(RunCommand::Rewind {
            command_id: "rewind-1".into(),
            checkpoint_id: checkpoint.checkpoint_id,
            side_effects_reconciled: false,
        })
        .unwrap();
        assert!(run.events().len() > events_before);
        assert_eq!(run.projection().state, json!({"value": 1}));
        assert_ne!(run.projection().branch_id, original_branch);
        assert_eq!(run.status(), DurableRunStatus::Paused);

        let replayed = RunState::from_events(run.events().to_vec()).unwrap();
        assert_eq!(replayed.projection(), run.projection());
    }

    #[test]
    fn rewind_after_external_activity_requires_acknowledgement() {
        let mut run = RunState::new("session", "rewind-effect", DurabilityMode::Sync).unwrap();
        let checkpoint = run.checkpoint("before", None).unwrap();
        let (activity_id, attempt) = execute(
            run.prepare_activity(
                "email-v1",
                "welcome",
                json!({"to": "a@example.com"}),
                SideEffectClass::Idempotent,
                Some("welcome:a@example.com".into()),
            )
            .unwrap(),
        );
        run.complete_activity(&activity_id, attempt, json!({"sent": true}))
            .unwrap();
        let original = run.clone();
        let error = run
            .apply_command(RunCommand::Rewind {
                command_id: "rewind-unsafe".into(),
                checkpoint_id: checkpoint.checkpoint_id.clone(),
                side_effects_reconciled: false,
            })
            .unwrap_err();
        assert_eq!(
            error,
            DurabilityError::ReconcileRequired {
                activity_ids: vec![activity_id]
            }
        );
        assert_eq!(run, original);

        run.apply_command(RunCommand::Rewind {
            command_id: "rewind-safe".into(),
            checkpoint_id: checkpoint.checkpoint_id,
            side_effects_reconciled: true,
        })
        .unwrap();
    }

    #[test]
    fn approval_is_durable_and_resume_is_command_idempotent() {
        let mut run = RunState::new("session", "approval", DurabilityMode::Sync).unwrap();
        let approval_id = run
            .request_approval("deploy", None, "Deploy?", json!({"env": "prod"}))
            .unwrap();
        let command = RunCommand::Resume {
            command_id: "resume-1".into(),
            approvals: vec![ApprovalResolution {
                approval_id: approval_id.clone(),
                approved: true,
                response: Some(json!({"by": "operator"})),
            }],
        };
        let first = run.apply_command(command.clone()).unwrap();
        let event_count = run.events().len();
        let second = run.apply_command(command).unwrap();
        assert_eq!(first, second);
        assert_eq!(run.events().len(), event_count);
        assert_eq!(
            run.projection().approvals[&approval_id].status,
            DurableApprovalStatus::Approved
        );
        assert_eq!(run.status(), DurableRunStatus::Running);
    }

    #[test]
    fn paused_run_cannot_start_new_activity() {
        let mut run = RunState::new("session", "paused", DurabilityMode::Sync).unwrap();
        run.pause("operator", "waiting for operator").unwrap();
        assert_eq!(
            run.prepare_activity("tool-v1", "call-1", json!({}), SideEffectClass::Pure, None,)
                .unwrap_err(),
            DurabilityError::RunNotExecuting {
                status: DurableRunStatus::Paused
            }
        );
    }

    #[test]
    fn forged_events_cannot_replace_schedule_or_start_work_while_paused() {
        let mut run = RunState::new("session", "paused-forgery", DurabilityMode::Sync).unwrap();
        let (existing_activity_id, existing_attempt) = execute(
            run.prepare_activity(
                "existing-step-v1",
                "existing-work",
                json!({"value": 0}),
                SideEffectClass::Pure,
                None,
            )
            .unwrap(),
        );
        run.pause("operator", "waiting for operator").unwrap();
        let before = run.clone();

        let replacement = RunEvent {
            schema_version: DURABILITY_SCHEMA_VERSION,
            run_id: run.run_id().to_string(),
            sequence: run.next_sequence(),
            event_id: "forged-state-replacement".into(),
            kind: RunEventKind::StateReplaced {
                state: json!({"forged": true}),
            },
        };
        assert_eq!(
            run.append_event(replacement).unwrap_err(),
            DurabilityError::RunNotExecuting {
                status: DurableRunStatus::Paused
            }
        );
        assert_eq!(run, before);

        let definition = ActivityDefinition {
            activity_id: "forged-activity".into(),
            stable_step_id: "forged-step-v1".into(),
            logical_key: "forged-work".into(),
            input: json!({"value": 1}),
            input_hash: stable_input_hash(&json!({"value": 1})),
            side_effect_class: SideEffectClass::Pure,
            idempotency_key: None,
        };
        let scheduled = RunEvent {
            schema_version: DURABILITY_SCHEMA_VERSION,
            run_id: run.run_id().to_string(),
            sequence: run.next_sequence(),
            event_id: "forged-activity-schedule".into(),
            kind: RunEventKind::ActivityScheduled { definition },
        };
        assert_eq!(
            run.append_event(scheduled).unwrap_err(),
            DurabilityError::RunNotExecuting {
                status: DurableRunStatus::Paused
            }
        );
        assert_eq!(run, before);

        let started = RunEvent {
            schema_version: DURABILITY_SCHEMA_VERSION,
            run_id: run.run_id().to_string(),
            sequence: run.next_sequence(),
            event_id: "forged-activity-start".into(),
            kind: RunEventKind::ActivityAttemptStarted {
                activity_id: existing_activity_id,
                attempt: existing_attempt + 1,
            },
        };
        assert_eq!(
            run.append_event(started).unwrap_err(),
            DurabilityError::RunNotExecuting {
                status: DurableRunStatus::Paused
            }
        );
        assert_eq!(run, before);
    }

    #[test]
    fn forged_events_cannot_replace_schedule_or_start_work_during_reconciliation() {
        let mut run = RunState::new("session", "reconcile-forgery", DurabilityMode::Sync).unwrap();
        let (activity_id, attempt) = execute(
            run.prepare_activity(
                "charge-v1",
                "charge-1",
                json!({"amount": 10}),
                SideEffectClass::ReconcileRequired,
                None,
            )
            .unwrap(),
        );
        run.fail_activity(&activity_id, attempt, "lost response", false, true)
            .unwrap();
        let before = run.clone();

        let forged_kinds = [
            RunEventKind::StateReplaced {
                state: json!({"forged": true}),
            },
            RunEventKind::ActivityScheduled {
                definition: ActivityDefinition {
                    activity_id: "forged-activity".into(),
                    stable_step_id: "forged-step-v1".into(),
                    logical_key: "forged-work".into(),
                    input: json!({"value": 1}),
                    input_hash: stable_input_hash(&json!({"value": 1})),
                    side_effect_class: SideEffectClass::Pure,
                    idempotency_key: None,
                },
            },
            RunEventKind::ActivityAttemptStarted {
                activity_id: activity_id.clone(),
                attempt: attempt + 1,
            },
        ];

        for (index, kind) in forged_kinds.into_iter().enumerate() {
            let event = RunEvent {
                schema_version: DURABILITY_SCHEMA_VERSION,
                run_id: run.run_id().to_string(),
                sequence: run.next_sequence(),
                event_id: format!("forged-reconcile-event-{index}"),
                kind,
            };
            assert_eq!(
                run.append_event(event).unwrap_err(),
                DurabilityError::RunRequiresReconciliation
            );
            assert_eq!(run, before);
        }
    }

    #[test]
    fn direct_event_replay_cannot_bypass_rewind_reconciliation() {
        let mut run = RunState::new("session", "forged-rewind", DurabilityMode::Sync).unwrap();
        let checkpoint = run.checkpoint("before", None).unwrap();
        let (activity_id, attempt) = execute(
            run.prepare_activity(
                "email-v1",
                "message-1",
                json!({"to": "a@example.com"}),
                SideEffectClass::Idempotent,
                Some("message-1".into()),
            )
            .unwrap(),
        );
        run.complete_activity(&activity_id, attempt, json!({"sent": true}))
            .unwrap();
        let before = run.clone();
        let forged = RunEvent {
            schema_version: DURABILITY_SCHEMA_VERSION,
            run_id: run.run_id().to_string(),
            sequence: run.next_sequence(),
            event_id: "forged-rewind-event".into(),
            kind: RunEventKind::RunRewound {
                checkpoint_id: checkpoint.checkpoint_id,
                new_branch_id: "forged-branch".into(),
                side_effects_reconciled: false,
            },
        };
        assert_eq!(
            run.append_event(forged).unwrap_err(),
            DurabilityError::ReconcileRequired {
                activity_ids: vec![activity_id]
            }
        );
        assert_eq!(run, before);
    }

    #[test]
    fn terminal_run_stays_terminal_after_late_reconciliation() {
        let mut run = RunState::new("session", "late-reconcile", DurabilityMode::Sync).unwrap();
        let (activity_id, attempt) = execute(
            run.prepare_activity(
                "charge-v1",
                "charge-1",
                json!({"amount": 10}),
                SideEffectClass::ReconcileRequired,
                None,
            )
            .unwrap(),
        );
        run.fail_activity(&activity_id, attempt, "lost response", false, true)
            .unwrap();
        run.fail_run("abort", "run aborted").unwrap();
        run.reconcile_activity(
            "late-operator-check",
            &activity_id,
            ActivityReconciliation::Completed {
                output: json!({"charged": true}),
            },
        )
        .unwrap();
        assert_eq!(run.status(), DurableRunStatus::Failed);
    }

    #[test]
    fn terminal_transitions_reject_running_activity_atomically() {
        let mut run = RunState::new("session", "terminal-guard", DurabilityMode::Sync).unwrap();
        run.prepare_activity(
            "external-write-v1",
            "write-1",
            json!({"value": 1}),
            SideEffectClass::ReconcileRequired,
            None,
        )
        .unwrap();
        let before = run.clone();

        assert!(matches!(
            run.complete_run("complete"),
            Err(DurabilityError::InvalidEvent { .. })
        ));
        assert_eq!(run, before);
        assert!(matches!(
            run.fail_run("fail", "failed"),
            Err(DurabilityError::InvalidEvent { .. })
        ));
        assert_eq!(run, before);
        assert!(matches!(
            run.apply_command(RunCommand::Cancel {
                command_id: "cancel".into(),
                reason: Some("cancelled".into()),
            }),
            Err(DurabilityError::InvalidEvent { .. })
        ));
        assert_eq!(run, before);
    }

    #[test]
    fn legacy_v1_terminal_snapshots_migrate_without_weakening_current_writes() {
        let cases = [
            (
                DurableRunStatus::Failed,
                false,
                ActivityAttemptStatus::Running,
            ),
            (
                DurableRunStatus::Cancelled,
                false,
                ActivityAttemptStatus::Running,
            ),
            (
                DurableRunStatus::Failed,
                true,
                ActivityAttemptStatus::ReconcileRequired,
            ),
        ];

        for (status, reconcile_required, activity_status) in cases {
            let fixture = legacy_v1_terminal_fixture(status, reconcile_required);
            let migrated: RunState = serde_json::from_value(fixture).unwrap();
            assert_eq!(migrated.schema_version(), DURABILITY_SCHEMA_VERSION);
            assert_eq!(migrated.status(), status);
            assert!(migrated
                .events()
                .iter()
                .all(|event| event.schema_version == 1));
            assert_eq!(
                migrated
                    .projection()
                    .activities
                    .values()
                    .next()
                    .unwrap()
                    .latest_attempt()
                    .unwrap()
                    .status,
                activity_status
            );

            let encoded = serde_json::to_value(&migrated).unwrap();
            assert_eq!(encoded["schema_version"], json!(DURABILITY_SCHEMA_VERSION));
            assert_eq!(
                serde_json::from_value::<RunState>(encoded).unwrap(),
                migrated
            );
        }

        let fresh = RunState::new("session", "reject-v1-append", DurabilityMode::Sync).unwrap();
        let mut legacy_event = fresh.events()[0].clone();
        legacy_event.schema_version = 1;
        let mut blank = RunState::new("session", "other-run", DurabilityMode::Sync).unwrap();
        legacy_event.run_id = blank.run_id().into();
        legacy_event.sequence = blank.next_sequence();
        legacy_event.event_id = "legacy-v1-new-write".into();
        legacy_event.kind = RunEventKind::StateReplaced {
            state: json!({"legacy": true}),
        };
        assert_eq!(
            blank.append_event(legacy_event).unwrap_err(),
            DurabilityError::UnsupportedSchema {
                expected: DURABILITY_SCHEMA_VERSION,
                actual: 1,
            }
        );

        let mut v1_events = fresh.events().to_vec();
        for event in &mut v1_events {
            event.schema_version = 1;
        }
        let mut migrated_open = RunState::from_events(v1_events).unwrap();
        migrated_open
            .replace_state(
                "post-migration",
                json!({"schema": DURABILITY_SCHEMA_VERSION}),
            )
            .unwrap();
        assert_eq!(migrated_open.events()[0].schema_version, 1);
        assert_eq!(
            migrated_open.events().last().unwrap().schema_version,
            DURABILITY_SCHEMA_VERSION
        );
    }

    #[test]
    fn v2_logs_migrate_to_v3_and_cancellation_remains_v3_only() {
        let mut current =
            RunState::new("session", "v2-cancellation", DurabilityMode::Sync).unwrap();
        let (activity_id, attempt) = execute(
            current
                .prepare_activity(
                    "v2-compatible-step",
                    "work-1",
                    json!({"value": 1}),
                    SideEffectClass::Pure,
                    None,
                )
                .unwrap(),
        );
        let mut v2_events = current.events().to_vec();
        for event in &mut v2_events {
            event.schema_version = TERMINAL_ACTIVITY_INVARIANT_SCHEMA_VERSION;
        }

        let mut forged_v2_cancellation = v2_events.clone();
        forged_v2_cancellation.push(RunEvent {
            schema_version: TERMINAL_ACTIVITY_INVARIANT_SCHEMA_VERSION,
            run_id: current.run_id().into(),
            sequence: current.next_sequence(),
            event_id: "forged-v2-cancellation".into(),
            kind: RunEventKind::ActivityAttemptCancelled {
                activity_id: activity_id.clone(),
                attempt,
                reason: "v3-only transition".into(),
            },
        });
        assert!(matches!(
            RunState::from_events(forged_v2_cancellation),
            Err(DurabilityError::InvalidEvent { .. })
        ));

        let mut migrated = RunState::from_events(v2_events).unwrap();
        assert_eq!(migrated.schema_version(), DURABILITY_SCHEMA_VERSION);
        assert!(migrated
            .events()
            .iter()
            .all(|event| event.schema_version == TERMINAL_ACTIVITY_INVARIANT_SCHEMA_VERSION));
        assert!(matches!(
            migrated
                .cancel_activity(&activity_id, attempt, "cancelled", false)
                .unwrap(),
            ActivityDecision::Cancelled { .. }
        ));
        assert_eq!(
            migrated.events().last().unwrap().schema_version,
            ACTIVITY_ATTEMPT_CANCELLED_SCHEMA_VERSION
        );
        assert_eq!(
            RunState::from_events(migrated.events().to_vec()).unwrap(),
            migrated
        );
    }

    #[test]
    fn legacy_v1_typed_approval_envelopes_survive_schema_migration() {
        let mut run =
            RunState::new("legacy-session", "legacy-approval", DurabilityMode::Sync).unwrap();
        let approval_id = run
            .request_typed_approval(DurableApprovalRequest {
                logical_key: "customer-id".into(),
                activity_id: None,
                kind: DurableApprovalKind::MissingInput,
                prompt: "Customer id?".into(),
                payload: json!({"field": "customer_id"}),
                policy_snapshot_hash: None,
                governance_binding: None,
                requested_at_unix_ms: 100,
                expires_at_unix_ms: 200,
            })
            .unwrap();
        run.apply_command_at(
            RunCommand::Resume {
                command_id: "legacy-resume".into(),
                approvals: vec![ApprovalResolution {
                    approval_id: approval_id.clone(),
                    approved: true,
                    response: Some(json!("cust-1")),
                }],
            },
            150,
        )
        .unwrap();

        let mut fixture = serde_json::to_value(&run).unwrap();
        fixture["schema_version"] = json!(1);
        for event in fixture["events"].as_array_mut().unwrap() {
            event["schema_version"] = json!(1);
            match event["kind"]["type"].as_str() {
                Some("approval_requested") => {
                    event["kind"]["payload"][DURABLE_APPROVAL_ENVELOPE_KEY]["schema_version"] =
                        json!(1);
                }
                Some("approval_resolved") => {
                    event["kind"]["response"][DURABLE_RESOLUTION_ENVELOPE_KEY]["schema_version"] =
                        json!(1);
                }
                _ => {}
            }
        }

        let migrated: RunState = serde_json::from_value(fixture).unwrap();
        assert_eq!(migrated.schema_version(), DURABILITY_SCHEMA_VERSION);
        assert_eq!(migrated.status(), DurableRunStatus::Running);
        assert_eq!(
            migrated.projection().approvals[&approval_id].status,
            DurableApprovalStatus::Approved
        );
        assert_eq!(
            migrated.projection().approvals[&approval_id].response,
            Some(json!("cust-1"))
        );
    }

    #[test]
    fn deserialization_rebuilds_projection_and_rejects_tampering() {
        let mut run = RunState::new("session", "serialized", DurabilityMode::Async).unwrap();
        run.replace_state("state-1", json!({"trusted": true}))
            .unwrap();
        let serialized = serde_json::to_value(&run).unwrap();
        let restored: RunState = serde_json::from_value(serialized.clone()).unwrap();
        assert_eq!(restored, run);

        let mut tampered = serialized;
        tampered["projection"]["state"] = json!({"trusted": false});
        let error = serde_json::from_value::<RunState>(tampered).unwrap_err();
        assert!(error
            .to_string()
            .contains("projection does not match its event log"));
    }

    #[test]
    fn policy_snapshot_is_pinned_before_work_and_survives_replay_and_fork() {
        let snapshot = policy_snapshot(PolicyEffect::Allow);
        let mut run = RunState::new_with_policy_snapshot(
            "session",
            "governed",
            DurabilityMode::Sync,
            &snapshot,
        )
        .unwrap();
        assert_eq!(run.policy_snapshot_hash(), Some(snapshot.hash()));
        assert!(matches!(
            run.events()[1].kind,
            RunEventKind::GovernanceBindingPinned { .. }
        ));
        assert_eq!(run.governance_binding().unwrap().run_id(), "governed");

        let replayed = RunState::from_events(run.events().to_vec()).unwrap();
        assert_eq!(replayed, run);

        let before_drift = run.clone();
        let drift = RunEvent {
            schema_version: DURABILITY_SCHEMA_VERSION,
            run_id: run.run_id().into(),
            sequence: run.next_sequence(),
            event_id: "policy-drift".into(),
            kind: RunEventKind::PolicySnapshotPinned {
                policy_snapshot_hash: policy_snapshot(PolicyEffect::Deny).hash().into(),
            },
        };
        assert!(matches!(
            run.append_event(drift),
            Err(DurabilityError::InvalidEvent { .. })
        ));
        assert_eq!(run, before_drift);

        let mut legacy = RunState::new("session", "legacy-governed", DurabilityMode::Sync).unwrap();
        legacy
            .append_event(RunEvent {
                schema_version: DURABILITY_SCHEMA_VERSION,
                run_id: legacy.run_id().into(),
                sequence: legacy.next_sequence(),
                event_id: "legacy-policy-pin".into(),
                kind: RunEventKind::PolicySnapshotPinned {
                    policy_snapshot_hash: snapshot.hash().into(),
                },
            })
            .unwrap();
        assert_eq!(legacy.policy_snapshot_hash(), Some(snapshot.hash()));
        assert!(legacy.governance_binding().is_none());
        assert_eq!(
            RunState::from_events(legacy.events().to_vec()).unwrap(),
            legacy
        );

        let checkpoint = run.checkpoint("fork-point", None).unwrap();
        let CommandOutcome::Forked { run: forked } = run
            .apply_command(RunCommand::Fork {
                command_id: "fork-1".into(),
                new_run_id: "governed-fork".into(),
                checkpoint_id: checkpoint.checkpoint_id,
                side_effects_reconciled: true,
            })
            .unwrap()
        else {
            panic!("expected fork");
        };
        assert_eq!(forked.policy_snapshot_hash(), Some(snapshot.hash()));
        assert_eq!(
            forked.governance_binding().unwrap().run_id(),
            "governed-fork"
        );
        assert_eq!(
            RunState::from_events(forked.events().to_vec()).unwrap(),
            *forked
        );
    }

    #[test]
    fn complete_governance_binding_is_event_sourced_and_required_by_approvals() {
        let snapshot = policy_snapshot(PolicyEffect::Ask);
        let binding = crate::governance::GovernanceBinding::seal(
            snapshot.hash(),
            Some("tenant-a".into()),
            Some("agent-a".into()),
            "bound-run",
        )
        .unwrap();
        let mut run = RunState::new_with_governance_binding(
            "session",
            "bound-run",
            DurabilityMode::Sync,
            binding.clone(),
        )
        .unwrap();
        assert_eq!(run.governance_binding(), Some(&binding));
        assert!(matches!(
            run.events()[1].kind,
            RunEventKind::GovernanceBindingPinned { .. }
        ));
        assert_eq!(RunState::from_events(run.events().to_vec()).unwrap(), run);

        let mismatched = crate::governance::GovernanceBinding::seal(
            snapshot.hash(),
            Some("tenant-b".into()),
            Some("agent-a".into()),
            "bound-run",
        )
        .unwrap();
        let wrong = DurableApprovalRequest {
            logical_key: "publish".into(),
            activity_id: None,
            kind: DurableApprovalKind::Confirmation,
            prompt: "Publish?".into(),
            payload: json!({"artifact": "report"}),
            policy_snapshot_hash: Some(snapshot.hash().into()),
            governance_binding: Some(mismatched),
            requested_at_unix_ms: 10,
            expires_at_unix_ms: 20,
        };
        assert!(matches!(
            run.request_typed_approval(wrong),
            Err(DurabilityError::InvalidEvent { .. })
        ));
        run.request_typed_approval(DurableApprovalRequest {
            logical_key: "publish".into(),
            activity_id: None,
            kind: DurableApprovalKind::Confirmation,
            prompt: "Publish?".into(),
            payload: json!({"artifact": "report"}),
            policy_snapshot_hash: Some(snapshot.hash().into()),
            governance_binding: None,
            requested_at_unix_ms: 10,
            expires_at_unix_ms: 20,
        })
        .unwrap();
        assert_eq!(
            run.projection()
                .approvals
                .values()
                .next()
                .unwrap()
                .governance_binding
                .as_ref(),
            Some(&binding)
        );

        let mut encoded = serde_json::to_value(&run).unwrap();
        encoded["governance_binding"]["tenant_id"] = json!("tenant-b");
        assert!(serde_json::from_value::<RunState>(encoded).is_err());
    }

    #[test]
    fn typed_hitl_semantics_and_clock_are_fail_closed() {
        let mut missing = RunState::new("session", "missing", DurabilityMode::Sync).unwrap();
        let approval_id = missing
            .request_typed_approval(DurableApprovalRequest {
                logical_key: "customer-id".into(),
                activity_id: None,
                kind: DurableApprovalKind::MissingInput,
                prompt: "Customer id?".into(),
                payload: json!({"field": "customer_id"}),
                policy_snapshot_hash: None,
                governance_binding: None,
                requested_at_unix_ms: 100,
                expires_at_unix_ms: 200,
            })
            .unwrap();
        let resume = |response| RunCommand::Resume {
            command_id: "resume-missing".into(),
            approvals: vec![ApprovalResolution {
                approval_id: approval_id.clone(),
                approved: true,
                response,
            }],
        };
        assert_eq!(
            missing
                .apply_command(resume(Some(json!("cust-1"))))
                .unwrap_err(),
            DurabilityError::ApprovalClockRequired {
                approval_id: approval_id.clone()
            }
        );
        assert!(matches!(
            missing.apply_command_at(resume(None), 150),
            Err(DurabilityError::InvalidApprovalResolution { .. })
        ));
        missing
            .apply_command_at(resume(Some(json!("cust-1"))), 150)
            .unwrap();
        assert_eq!(
            missing.projection().approvals[&approval_id].status,
            DurableApprovalStatus::Approved
        );

        let mut edit = RunState::new("session", "edit", DurabilityMode::Sync).unwrap();
        let edit_id = edit
            .request_typed_approval(DurableApprovalRequest {
                logical_key: "review-edit".into(),
                activity_id: None,
                kind: DurableApprovalKind::EditRetry,
                prompt: "Edit or retry?".into(),
                payload: json!({"draft": "old"}),
                policy_snapshot_hash: None,
                governance_binding: None,
                requested_at_unix_ms: 100,
                expires_at_unix_ms: 200,
            })
            .unwrap();
        assert!(matches!(
            edit.apply_command_at(
                RunCommand::Resume {
                    command_id: "invalid-edit".into(),
                    approvals: vec![ApprovalResolution {
                        approval_id: edit_id,
                        approved: true,
                        response: Some(json!({"action": "edit"})),
                    }],
                },
                150,
            ),
            Err(DurabilityError::InvalidApprovalResolution { .. })
        ));
    }

    #[test]
    fn expired_approval_is_durably_denied_after_restart() {
        let snapshot = policy_snapshot(PolicyEffect::Ask);
        let mut run = RunState::new_with_policy_snapshot(
            "session",
            "timeout",
            DurabilityMode::Sync,
            &snapshot,
        )
        .unwrap();
        let approval_id = run
            .request_typed_approval(DurableApprovalRequest {
                logical_key: "deploy".into(),
                activity_id: None,
                kind: DurableApprovalKind::Confirmation,
                prompt: "Deploy?".into(),
                payload: json!({"env": "production"}),
                policy_snapshot_hash: Some(snapshot.hash().into()),
                governance_binding: None,
                requested_at_unix_ms: 100,
                expires_at_unix_ms: 110,
            })
            .unwrap();
        let mut restarted = RunState::from_events(run.events().to_vec()).unwrap();
        restarted
            .apply_command_at(
                RunCommand::Resume {
                    command_id: "late-resume".into(),
                    approvals: vec![ApprovalResolution {
                        approval_id: approval_id.clone(),
                        approved: true,
                        response: None,
                    }],
                },
                110,
            )
            .unwrap();
        let approval = &restarted.projection().approvals[&approval_id];
        assert_eq!(approval.status, DurableApprovalStatus::Rejected);
        assert!(approval.timed_out);
        assert_eq!(approval.resolved_at_unix_ms, Some(110));
        assert_eq!(restarted.status(), DurableRunStatus::Running);
        assert_eq!(
            RunState::from_events(restarted.events().to_vec()).unwrap(),
            restarted
        );
    }

    #[test]
    fn timeout_sweep_persists_even_when_another_approval_is_still_pending() {
        let mut run = RunState::new("session", "sweep", DurabilityMode::Sync).unwrap();
        let request = |logical_key: &str, expires_at_unix_ms| DurableApprovalRequest {
            logical_key: logical_key.into(),
            activity_id: None,
            kind: DurableApprovalKind::OutputReview,
            prompt: "Review?".into(),
            payload: json!({"key": logical_key}),
            policy_snapshot_hash: None,
            governance_binding: None,
            requested_at_unix_ms: 100,
            expires_at_unix_ms,
        };
        let expired_id = run.request_typed_approval(request("expired", 110)).unwrap();
        let pending_id = run.request_typed_approval(request("pending", 200)).unwrap();

        assert_eq!(
            run.expire_approvals("sweep-1", 110).unwrap(),
            vec![expired_id.clone()]
        );
        assert_eq!(
            run.projection().approvals[&expired_id].status,
            DurableApprovalStatus::Rejected
        );
        assert_eq!(
            run.projection().approvals[&pending_id].status,
            DurableApprovalStatus::Pending
        );
        assert_eq!(run.status(), DurableRunStatus::Paused);
        assert!(run.expire_approvals("sweep-1", 110).unwrap().is_empty());
        assert_eq!(RunState::from_events(run.events().to_vec()).unwrap(), run);
    }

    #[test]
    fn raw_v2_reserved_schedules_require_the_exact_lifecycle_and_completed_canonical_receipt() {
        let invocation_id = "invocation-1";
        let without_lifecycle =
            RunState::new("session", "raw-schedule", DurabilityMode::Sync).unwrap();
        let canonical_replay = test_terminal_replay("canonical", invocation_id);
        let canonical_definition = ActivityDefinition {
            activity_id: stable_identifier(
                "activity",
                &[
                    without_lifecycle.run_id(),
                    &without_lifecycle.projection().branch_id,
                    RUNTIME_RUN_STOPPED_AUDIT_STEP_ID,
                    "terminal",
                ],
            ),
            stable_step_id: RUNTIME_RUN_STOPPED_AUDIT_STEP_ID.into(),
            logical_key: "terminal".into(),
            input: test_terminal_audit_input(&canonical_replay, &test_terminal_receipt()),
            input_hash: stable_input_hash(&test_terminal_audit_input(
                &canonical_replay,
                &test_terminal_receipt(),
            )),
            side_effect_class: SideEffectClass::ReconcileRequired,
            idempotency_key: None,
        };
        assert_raw_v2_event_rejected(
            &without_lifecycle,
            "raw-canonical-without-lifecycle",
            RunEventKind::ActivityScheduled {
                definition: canonical_definition.clone(),
            },
        );

        let (mut wrong_lifecycle, _, _) = test_running_reserved_run(
            RUNTIME_INVOCATION_LIFECYCLE_STEP_ID,
            "another-invocation",
            test_lifecycle_input("another-invocation"),
        );
        let mut wrong_definition = canonical_definition.clone();
        wrong_definition.activity_id = stable_identifier(
            "activity",
            &[
                wrong_lifecycle.run_id(),
                &wrong_lifecycle.projection().branch_id,
                RUNTIME_RUN_STOPPED_AUDIT_STEP_ID,
                "terminal",
            ],
        );
        assert_raw_v2_event_rejected(
            &wrong_lifecycle,
            "raw-canonical-wrong-lifecycle",
            RunEventKind::ActivityScheduled {
                definition: wrong_definition.clone(),
            },
        );
        execute(
            wrong_lifecycle
                .prepare_activity(
                    RUNTIME_INVOCATION_LIFECYCLE_STEP_ID,
                    invocation_id,
                    test_lifecycle_input(invocation_id),
                    SideEffectClass::ReconcileRequired,
                    None,
                )
                .unwrap(),
        );
        let recovery_replay = test_terminal_replay("recovery", invocation_id);
        let recovery_input =
            test_terminal_audit_input(&recovery_replay, &json!({"accepted": true}));
        assert_raw_v2_event_rejected(
            &wrong_lifecycle,
            "raw-recovery-without-completed-canonical",
            RunEventKind::ActivityScheduled {
                definition: ActivityDefinition {
                    activity_id: stable_identifier(
                        "activity",
                        &[
                            wrong_lifecycle.run_id(),
                            &wrong_lifecycle.projection().branch_id,
                            RUNTIME_RECOVERY_RUN_STOPPED_AUDIT_STEP_ID,
                            invocation_id,
                        ],
                    ),
                    stable_step_id: RUNTIME_RECOVERY_RUN_STOPPED_AUDIT_STEP_ID.into(),
                    logical_key: invocation_id.into(),
                    input_hash: stable_input_hash(&recovery_input),
                    input: recovery_input,
                    side_effect_class: SideEffectClass::ReconcileRequired,
                    idempotency_key: None,
                },
            },
        );

        let mut duplicate = RunState::new("session", "audit-run", DurabilityMode::Sync).unwrap();
        let (_, lifecycle_attempt) = execute(
            duplicate
                .prepare_activity(
                    RUNTIME_INVOCATION_LIFECYCLE_STEP_ID,
                    invocation_id,
                    test_lifecycle_input(invocation_id),
                    SideEffectClass::ReconcileRequired,
                    None,
                )
                .unwrap(),
        );
        assert_eq!(lifecycle_attempt, 1);
        let canonical = duplicate
            .prepare_activity(
                RUNTIME_RUN_STOPPED_AUDIT_STEP_ID,
                "terminal",
                test_terminal_audit_input(&canonical_replay, &test_terminal_receipt()),
                SideEffectClass::ReconcileRequired,
                None,
            )
            .unwrap();
        let ActivityDecision::Execute {
            activity_id: canonical_id,
            ..
        } = canonical
        else {
            panic!("canonical audit must begin its first attempt");
        };
        let duplicate_definition = duplicate
            .activity(&canonical_id)
            .unwrap()
            .definition
            .clone();
        let before_duplicate = duplicate.clone();
        assert!(duplicate
            .append_event(RunEvent {
                schema_version: DURABILITY_SCHEMA_VERSION,
                run_id: duplicate.run_id().into(),
                sequence: duplicate.next_sequence(),
                event_id: "raw-duplicate-canonical".into(),
                kind: RunEventKind::ActivityScheduled {
                    definition: duplicate_definition
                },
            })
            .is_err());
        assert_eq!(duplicate, before_duplicate);
        let legacy_input = json!({"input_hash": stable_input_hash(&test_terminal_receipt())});
        assert_raw_v2_event_rejected(
            &duplicate,
            "raw-v2-legacy-schedule",
            RunEventKind::ActivityScheduled {
                definition: ActivityDefinition {
                    activity_id: stable_identifier(
                        "activity",
                        &[
                            duplicate.run_id(),
                            &duplicate.projection().branch_id,
                            RUNTIME_LEGACY_RUN_STOPPED_AUDIT_STEP_ID,
                            "terminal",
                        ],
                    ),
                    stable_step_id: RUNTIME_LEGACY_RUN_STOPPED_AUDIT_STEP_ID.into(),
                    logical_key: "terminal".into(),
                    input_hash: stable_input_hash(&legacy_input),
                    input: legacy_input,
                    side_effect_class: SideEffectClass::ReconcileRequired,
                    idempotency_key: None,
                },
            },
        );
    }

    #[test]
    fn raw_lifecycle_audit_run_id_forgery_rejects_direct_and_reconciled_completion_atomically() {
        let invocation_id = "invocation-1";
        let (run, activity_id, attempt) = test_running_reserved_run(
            RUNTIME_INVOCATION_LIFECYCLE_STEP_ID,
            invocation_id,
            test_lifecycle_input(invocation_id),
        );
        let mut replay = test_terminal_replay("direct", invocation_id);
        replay["audit_run_id"] = json!("another-run");
        let output = json!({"status": "audit_closed", "replay": replay});
        assert_raw_v2_event_rejected(
            &run,
            "raw-forged-lifecycle-audit-run",
            RunEventKind::ActivityAttemptCompleted {
                activity_id: activity_id.clone(),
                attempt,
                output: output.clone(),
                output_hash: stable_input_hash(&output),
            },
        );

        let (mut reconciled, reconciliation_id) = test_reconciliation_run(
            RUNTIME_INVOCATION_LIFECYCLE_STEP_ID,
            invocation_id,
            test_lifecycle_input(invocation_id),
        );
        let before = reconciled.clone();
        assert!(matches!(
            reconciled.reconcile_activity(
                "forged-lifecycle-audit-run",
                &reconciliation_id,
                ActivityReconciliation::Completed { output },
            ),
            Err(DurabilityError::InvalidEvent { .. })
        ));
        assert_eq!(reconciled, before);
    }

    #[test]
    fn v1_legacy_run_stopped_marker_must_be_canonical_and_unique_on_replay() {
        let seed = RunState::new("session", "legacy-marker", DurabilityMode::Sync).unwrap();
        let run_id = seed.run_id().to_string();
        let branch_id = seed.projection().branch_id.clone();
        let mut start = seed.events()[0].clone();
        start.schema_version = 1;
        let input = json!({"input_hash": stable_input_hash(&test_terminal_receipt())});
        let canonical = ActivityDefinition {
            activity_id: stable_identifier(
                "activity",
                &[
                    &run_id,
                    &branch_id,
                    RUNTIME_LEGACY_RUN_STOPPED_AUDIT_STEP_ID,
                    "terminal",
                ],
            ),
            stable_step_id: RUNTIME_LEGACY_RUN_STOPPED_AUDIT_STEP_ID.into(),
            logical_key: "terminal".into(),
            input_hash: stable_input_hash(&input),
            input,
            side_effect_class: SideEffectClass::ReconcileRequired,
            idempotency_key: None,
        };
        let scheduled = RunEvent {
            schema_version: 1,
            run_id: run_id.clone(),
            sequence: 2,
            event_id: "legacy-terminal-scheduled".into(),
            kind: RunEventKind::ActivityScheduled {
                definition: canonical.clone(),
            },
        };
        assert!(RunState::from_events([start.clone(), scheduled.clone()]).is_ok());

        let duplicate = RunEvent {
            schema_version: 1,
            run_id: run_id.clone(),
            sequence: 3,
            event_id: "legacy-terminal-duplicated".into(),
            kind: RunEventKind::ActivityScheduled {
                definition: canonical.clone(),
            },
        };
        assert!(matches!(
            RunState::from_events([start.clone(), scheduled.clone(), duplicate]),
            Err(DurabilityError::InvalidActivityTransition { .. })
                | Err(DurabilityError::InvalidEvent { .. })
        ));

        let mut non_canonical = canonical;
        non_canonical.activity_id = "legacy-terminal-forged-id".into();
        let wrong_identity = RunEvent {
            schema_version: 1,
            run_id,
            sequence: 3,
            event_id: "legacy-terminal-wrong-id".into(),
            kind: RunEventKind::ActivityScheduled {
                definition: non_canonical,
            },
        };
        assert!(matches!(
            RunState::from_events([start, scheduled, wrong_identity]),
            Err(DurabilityError::InvalidEvent { .. })
        ));
    }

    #[test]
    fn raw_not_started_lifecycle_completion_rejects_post_fence_ordinary_work() {
        let invocation_id = "invocation-1";
        let (mut run, lifecycle_id, lifecycle_attempt) = test_running_reserved_run(
            RUNTIME_INVOCATION_LIFECYCLE_STEP_ID,
            invocation_id,
            test_lifecycle_input(invocation_id),
        );
        execute(
            run.prepare_activity(
                "ordinary-post-fence",
                "work-1",
                json!({"value": 1}),
                SideEffectClass::Pure,
                None,
            )
            .unwrap(),
        );
        let output = json!({
            "status": "not_started",
            "audit_run_id": run.run_id(),
            "invocation_id": invocation_id,
        });
        assert_raw_v2_event_rejected(
            &run,
            "raw-not-started-after-ordinary-work",
            RunEventKind::ActivityAttemptCompleted {
                activity_id: lifecycle_id,
                attempt: lifecycle_attempt,
                output: output.clone(),
                output_hash: stable_input_hash(&output),
            },
        );
    }

    #[test]
    fn quarantined_unstarted_lifecycle_can_close_as_not_started_then_resume_work() {
        let invocation_id = "invocation-1";
        let mut run = RunState::new("session", "audit-run", DurabilityMode::Sync).unwrap();
        let lifecycle_definition = ActivityDefinition {
            activity_id: stable_identifier(
                "activity",
                &[
                    run.run_id(),
                    &run.projection().branch_id,
                    RUNTIME_INVOCATION_LIFECYCLE_STEP_ID,
                    invocation_id,
                ],
            ),
            stable_step_id: RUNTIME_INVOCATION_LIFECYCLE_STEP_ID.into(),
            logical_key: invocation_id.into(),
            input: test_lifecycle_input(invocation_id),
            input_hash: stable_input_hash(&test_lifecycle_input(invocation_id)),
            side_effect_class: SideEffectClass::ReconcileRequired,
            idempotency_key: None,
        };
        let lifecycle_id = lifecycle_definition.activity_id.clone();
        run.append_event(RunEvent {
            schema_version: DURABILITY_SCHEMA_VERSION,
            run_id: run.run_id().into(),
            sequence: run.next_sequence(),
            event_id: "raw-unstarted-lifecycle".into(),
            kind: RunEventKind::ActivityScheduled {
                definition: lifecycle_definition,
            },
        })
        .unwrap();
        run.quarantine_unstarted_reserved_activity(&lifecycle_id, "operator review")
            .unwrap();
        let output = json!({
            "status": "not_started",
            "audit_run_id": run.run_id(),
            "invocation_id": invocation_id,
        });
        run.reconcile_activity(
            "reconcile-unstarted-lifecycle",
            &lifecycle_id,
            ActivityReconciliation::Completed {
                output: output.clone(),
            },
        )
        .unwrap();
        assert_eq!(
            run.activity(&lifecycle_id).unwrap().completed_output(),
            Some(&output)
        );
        assert_eq!(run.status(), DurableRunStatus::Paused);
        run.apply_command(RunCommand::Resume {
            command_id: "resume-after-unstarted-lifecycle".into(),
            approvals: vec![],
        })
        .unwrap();
        assert!(matches!(
            run.prepare_activity(
                "ordinary-after-resume",
                "work-1",
                json!({"value": 1}),
                SideEffectClass::Pure,
                None,
            )
            .unwrap(),
            ActivityDecision::Execute { .. }
        ));
        assert_eq!(RunState::from_events(run.events().to_vec()).unwrap(), run);
    }

    #[test]
    fn recovery_audit_cannot_reuse_the_completed_canonical_invocation() {
        let invocation_id = "invocation-1";
        let canonical_replay = test_terminal_replay("canonical", invocation_id);
        let mut run = RunState::new("session", "audit-run", DurabilityMode::Sync).unwrap();
        execute(
            run.prepare_activity(
                RUNTIME_INVOCATION_LIFECYCLE_STEP_ID,
                invocation_id,
                test_lifecycle_input(invocation_id),
                SideEffectClass::ReconcileRequired,
                None,
            )
            .unwrap(),
        );
        let (canonical_id, canonical_attempt) = execute(
            run.prepare_activity(
                RUNTIME_RUN_STOPPED_AUDIT_STEP_ID,
                "terminal",
                test_terminal_audit_input(&canonical_replay, &test_terminal_receipt()),
                SideEffectClass::ReconcileRequired,
                None,
            )
            .unwrap(),
        );
        run.complete_activity(&canonical_id, canonical_attempt, test_terminal_receipt())
            .unwrap();

        let recovery_replay = test_terminal_replay("recovery", invocation_id);
        let recovery_input =
            test_terminal_audit_input(&recovery_replay, &json!({"accepted": true}));
        assert_raw_v2_event_rejected(
            &run,
            "recovery-reuses-canonical-invocation",
            RunEventKind::ActivityScheduled {
                definition: ActivityDefinition {
                    activity_id: stable_identifier(
                        "activity",
                        &[
                            run.run_id(),
                            &run.projection().branch_id,
                            RUNTIME_RECOVERY_RUN_STOPPED_AUDIT_STEP_ID,
                            invocation_id,
                        ],
                    ),
                    stable_step_id: RUNTIME_RECOVERY_RUN_STOPPED_AUDIT_STEP_ID.into(),
                    logical_key: invocation_id.into(),
                    input_hash: stable_input_hash(&recovery_input),
                    input: recovery_input,
                    side_effect_class: SideEffectClass::ReconcileRequired,
                    idempotency_key: None,
                },
            },
        );
    }

    #[test]
    fn terminal_markers_block_running_work_and_later_ordinary_schedule_or_start() {
        let invocation_id = "invocation-1";
        let canonical_replay = test_terminal_replay("canonical", invocation_id);
        let canonical_input =
            test_terminal_audit_input(&canonical_replay, &test_terminal_receipt());

        let (mut canonical_run, _, _) = test_running_reserved_run(
            RUNTIME_INVOCATION_LIFECYCLE_STEP_ID,
            invocation_id,
            test_lifecycle_input(invocation_id),
        );
        execute(
            canonical_run
                .prepare_activity(
                    "ordinary-running-before-canonical",
                    "work-1",
                    json!({"value": 1}),
                    SideEffectClass::Pure,
                    None,
                )
                .unwrap(),
        );
        assert_raw_v2_event_rejected(
            &canonical_run,
            "canonical-blocked-by-running-ordinary-work",
            RunEventKind::ActivityScheduled {
                definition: ActivityDefinition {
                    activity_id: stable_identifier(
                        "activity",
                        &[
                            canonical_run.run_id(),
                            &canonical_run.projection().branch_id,
                            RUNTIME_RUN_STOPPED_AUDIT_STEP_ID,
                            "terminal",
                        ],
                    ),
                    stable_step_id: RUNTIME_RUN_STOPPED_AUDIT_STEP_ID.into(),
                    logical_key: "terminal".into(),
                    input_hash: stable_input_hash(&canonical_input),
                    input: canonical_input.clone(),
                    side_effect_class: SideEffectClass::ReconcileRequired,
                    idempotency_key: None,
                },
            },
        );

        let mut recovery_run = RunState::new("session", "audit-run", DurabilityMode::Sync).unwrap();
        let (canonical_lifecycle_id, canonical_lifecycle_attempt) = execute(
            recovery_run
                .prepare_activity(
                    RUNTIME_INVOCATION_LIFECYCLE_STEP_ID,
                    "canonical-invocation",
                    test_lifecycle_input("canonical-invocation"),
                    SideEffectClass::ReconcileRequired,
                    None,
                )
                .unwrap(),
        );
        let canonical_replay = test_terminal_replay("canonical", "canonical-invocation");
        let (canonical_id, canonical_attempt) = execute(
            recovery_run
                .prepare_activity(
                    RUNTIME_RUN_STOPPED_AUDIT_STEP_ID,
                    "terminal",
                    test_terminal_audit_input(&canonical_replay, &test_terminal_receipt()),
                    SideEffectClass::ReconcileRequired,
                    None,
                )
                .unwrap(),
        );
        recovery_run
            .complete_activity(&canonical_id, canonical_attempt, test_terminal_receipt())
            .unwrap();
        recovery_run
            .fail_activity(
                &canonical_lifecycle_id,
                canonical_lifecycle_attempt,
                "close canonical lifecycle",
                true,
                true,
            )
            .unwrap();
        recovery_run
            .reconcile_activity(
                "close-canonical-lifecycle",
                &canonical_lifecycle_id,
                ActivityReconciliation::Completed {
                    output: json!({"status": "audit_closed", "replay": canonical_replay}),
                },
            )
            .unwrap();
        recovery_run
            .apply_command(RunCommand::Resume {
                command_id: "resume-before-recovery".into(),
                approvals: vec![],
            })
            .unwrap();
        let recovery_invocation = "recovery-invocation";
        execute(
            recovery_run
                .prepare_activity(
                    RUNTIME_INVOCATION_LIFECYCLE_STEP_ID,
                    recovery_invocation,
                    test_lifecycle_input(recovery_invocation),
                    SideEffectClass::ReconcileRequired,
                    None,
                )
                .unwrap(),
        );
        // A legal canonical marker already forbids scheduling new ordinary work. Model a
        // recovered legacy projection with an ordinary attempt that was already in flight, so
        // this assertion reaches the recovery-marker schedule guard itself.
        let ordinary_id = "recovered-ordinary-running".to_string();
        recovery_run.projection.activities.insert(
            ordinary_id.clone(),
            ActivityRecord {
                definition: ActivityDefinition {
                    activity_id: ordinary_id,
                    stable_step_id: "ordinary-recovered-work".into(),
                    logical_key: "work-1".into(),
                    input: json!({"value": 1}),
                    input_hash: stable_input_hash(&json!({"value": 1})),
                    side_effect_class: SideEffectClass::Pure,
                    idempotency_key: None,
                },
                attempts: vec![ActivityAttempt {
                    attempt: 1,
                    status: ActivityAttemptStatus::Running,
                    started_sequence: recovery_run.next_sequence(),
                    finished_sequence: None,
                    output: None,
                    output_hash: None,
                    error: None,
                    retryable: false,
                    effect_ambiguous: false,
                }],
            },
        );
        let recovery_replay = test_terminal_replay("recovery", recovery_invocation);
        let recovery_input =
            test_terminal_audit_input(&recovery_replay, &json!({"accepted": true}));
        let before_recovery_schedule = recovery_run.clone();
        assert!(matches!(
            recovery_run.append_event(RunEvent {
                schema_version: DURABILITY_SCHEMA_VERSION,
                run_id: recovery_run.run_id().into(),
                sequence: recovery_run.next_sequence(),
                event_id: "recovery-blocked-by-running-ordinary-work".into(),
                kind: RunEventKind::ActivityScheduled {
                    definition: ActivityDefinition {
                        activity_id: stable_identifier(
                            "activity",
                            &[
                                recovery_run.run_id(),
                                &recovery_run.projection().branch_id,
                                RUNTIME_RECOVERY_RUN_STOPPED_AUDIT_STEP_ID,
                                recovery_invocation,
                            ],
                        ),
                        stable_step_id: RUNTIME_RECOVERY_RUN_STOPPED_AUDIT_STEP_ID.into(),
                        logical_key: recovery_invocation.into(),
                        input_hash: stable_input_hash(&recovery_input),
                        input: recovery_input,
                        side_effect_class: SideEffectClass::ReconcileRequired,
                        idempotency_key: None,
                    },
                },
            }),
            Err(DurabilityError::InvalidEvent { .. })
        ));
        assert_eq!(recovery_run, before_recovery_schedule);

        let mut post_marker = RunState::new("session", "audit-run", DurabilityMode::Sync).unwrap();
        let ordinary = ActivityDefinition {
            activity_id: stable_identifier(
                "activity",
                &[
                    post_marker.run_id(),
                    &post_marker.projection().branch_id,
                    "ordinary-before-marker",
                    "work-1",
                ],
            ),
            stable_step_id: "ordinary-before-marker".into(),
            logical_key: "work-1".into(),
            input: json!({"value": 1}),
            input_hash: stable_input_hash(&json!({"value": 1})),
            side_effect_class: SideEffectClass::Pure,
            idempotency_key: None,
        };
        post_marker
            .append_event(RunEvent {
                schema_version: DURABILITY_SCHEMA_VERSION,
                run_id: post_marker.run_id().into(),
                sequence: post_marker.next_sequence(),
                event_id: "ordinary-scheduled-before-marker".into(),
                kind: RunEventKind::ActivityScheduled {
                    definition: ordinary.clone(),
                },
            })
            .unwrap();
        let marker_invocation = "marker-invocation";
        execute(
            post_marker
                .prepare_activity(
                    RUNTIME_INVOCATION_LIFECYCLE_STEP_ID,
                    marker_invocation,
                    test_lifecycle_input(marker_invocation),
                    SideEffectClass::ReconcileRequired,
                    None,
                )
                .unwrap(),
        );
        let marker_replay = test_terminal_replay("canonical", marker_invocation);
        post_marker
            .append_event(RunEvent {
                schema_version: DURABILITY_SCHEMA_VERSION,
                run_id: post_marker.run_id().into(),
                sequence: post_marker.next_sequence(),
                event_id: "canonical-marker-scheduled".into(),
                kind: RunEventKind::ActivityScheduled {
                    definition: ActivityDefinition {
                        activity_id: stable_identifier(
                            "activity",
                            &[
                                post_marker.run_id(),
                                &post_marker.projection().branch_id,
                                RUNTIME_RUN_STOPPED_AUDIT_STEP_ID,
                                "terminal",
                            ],
                        ),
                        stable_step_id: RUNTIME_RUN_STOPPED_AUDIT_STEP_ID.into(),
                        logical_key: "terminal".into(),
                        input_hash: stable_input_hash(&test_terminal_audit_input(
                            &marker_replay,
                            &test_terminal_receipt(),
                        )),
                        input: test_terminal_audit_input(&marker_replay, &test_terminal_receipt()),
                        side_effect_class: SideEffectClass::ReconcileRequired,
                        idempotency_key: None,
                    },
                },
            })
            .unwrap();
        let later_ordinary = ActivityDefinition {
            activity_id: stable_identifier(
                "activity",
                &[
                    post_marker.run_id(),
                    &post_marker.projection().branch_id,
                    "ordinary-after-marker",
                    "work-2",
                ],
            ),
            stable_step_id: "ordinary-after-marker".into(),
            logical_key: "work-2".into(),
            input: json!({"value": 2}),
            input_hash: stable_input_hash(&json!({"value": 2})),
            side_effect_class: SideEffectClass::Pure,
            idempotency_key: None,
        };
        assert_raw_v2_event_rejected(
            &post_marker,
            "ordinary-scheduled-after-marker",
            RunEventKind::ActivityScheduled {
                definition: later_ordinary,
            },
        );
        assert_raw_v2_event_rejected(
            &post_marker,
            "ordinary-started-after-marker",
            RunEventKind::ActivityAttemptStarted {
                activity_id: ordinary.activity_id,
                attempt: 1,
            },
        );
    }

    #[test]
    fn raw_fork_strips_malformed_reserved_identity_before_canonical_marker_begins() {
        let fork_run_id = "malformed-marker-fork";
        let new_branch_id = "fork-branch";
        let squatted_activity_id = stable_identifier(
            "activity",
            &[
                fork_run_id,
                new_branch_id,
                RUNTIME_RUN_STOPPED_AUDIT_STEP_ID,
                "terminal",
            ],
        );
        let malformed_input = json!({});
        let mut source_projection = RunProjection::root("source-run");
        source_projection.activities.insert(
            squatted_activity_id.clone(),
            ActivityRecord {
                definition: ActivityDefinition {
                    activity_id: squatted_activity_id.clone(),
                    stable_step_id: "ordinary-squatter".into(),
                    logical_key: "ordinary".into(),
                    input_hash: stable_input_hash(&malformed_input),
                    input: malformed_input,
                    side_effect_class: SideEffectClass::ReconcileRequired,
                    idempotency_key: None,
                },
                attempts: Vec::new(),
            },
        );
        let checkpoint = Checkpoint {
            checkpoint_id: "malformed-terminal-marker-checkpoint".into(),
            run_id: "source-run".into(),
            event_sequence: 1,
            parent_checkpoint_id: None,
            label: None,
            projection: source_projection,
        };
        let mut forked = RunState::from_events([RunEvent {
            schema_version: DURABILITY_SCHEMA_VERSION,
            run_id: fork_run_id.into(),
            sequence: 1,
            event_id: "raw-forked-from-malformed-terminal-marker".into(),
            kind: RunEventKind::ForkedFrom {
                session_id: "session".into(),
                source_run_id: "source-run".into(),
                durability: DurabilityMode::Sync,
                source_checkpoint: Box::new(checkpoint),
                new_branch_id: new_branch_id.into(),
            },
        }])
        .unwrap();

        assert!(matches!(
            forked.activity(&squatted_activity_id),
            Err(DurabilityError::ActivityNotFound { .. })
        ));

        let invocation_id = "fork-invocation";
        execute(
            forked
                .prepare_activity(
                    RUNTIME_INVOCATION_LIFECYCLE_STEP_ID,
                    invocation_id,
                    json!({
                        "schema_version": 1,
                        "audit_run_id": fork_run_id,
                        "invocation_id": invocation_id,
                    }),
                    SideEffectClass::ReconcileRequired,
                    None,
                )
                .unwrap(),
        );
        let mut replay = test_terminal_replay("canonical", invocation_id);
        replay["audit_run_id"] = json!(fork_run_id);
        let (canonical_activity_id, attempt) = execute(
            forked
                .prepare_activity(
                    RUNTIME_RUN_STOPPED_AUDIT_STEP_ID,
                    "terminal",
                    test_terminal_audit_input(&replay, &test_terminal_receipt()),
                    SideEffectClass::ReconcileRequired,
                    None,
                )
                .unwrap(),
        );
        assert_eq!(canonical_activity_id, squatted_activity_id);
        assert_eq!(attempt, 1);
    }

    #[test]
    fn fork_and_rewind_strip_reserved_markers_and_completed_terminal_receipts() {
        let invocation_id = "invocation-1";
        let mut source = RunState::new("session", "audit-run", DurabilityMode::Sync).unwrap();
        execute(
            source
                .prepare_activity(
                    RUNTIME_INVOCATION_LIFECYCLE_STEP_ID,
                    invocation_id,
                    test_lifecycle_input(invocation_id),
                    SideEffectClass::ReconcileRequired,
                    None,
                )
                .unwrap(),
        );
        let replay = test_terminal_replay("canonical", invocation_id);
        let (terminal_id, terminal_attempt) = execute(
            source
                .prepare_activity(
                    RUNTIME_RUN_STOPPED_AUDIT_STEP_ID,
                    "terminal",
                    test_terminal_audit_input(&replay, &test_terminal_receipt()),
                    SideEffectClass::ReconcileRequired,
                    None,
                )
                .unwrap(),
        );
        source
            .complete_activity(&terminal_id, terminal_attempt, test_terminal_receipt())
            .unwrap();
        let checkpoint = source
            .checkpoint("terminal-marker-checkpoint", None)
            .unwrap();
        assert!(source
            .projection()
            .activities
            .values()
            .any(is_reserved_audit_activity));

        let CommandOutcome::Forked { run: forked, .. } = source
            .apply_command(RunCommand::Fork {
                command_id: "fork-with-terminal-marker".into(),
                new_run_id: "audit-fork".into(),
                checkpoint_id: checkpoint.checkpoint_id.clone(),
                side_effects_reconciled: false,
            })
            .unwrap()
        else {
            panic!("expected a forked run");
        };
        assert!(forked
            .projection()
            .activities
            .values()
            .all(|record| !is_reserved_audit_activity(record)));
        assert!(matches!(
            forked.activity(&terminal_id),
            Err(DurabilityError::ActivityNotFound { .. })
        ));

        source
            .apply_command(RunCommand::Rewind {
                command_id: "rewind-with-terminal-marker".into(),
                checkpoint_id: checkpoint.checkpoint_id,
                side_effects_reconciled: false,
            })
            .unwrap();
        assert!(source
            .projection()
            .activities
            .values()
            .all(|record| !is_reserved_audit_activity(record)));
        assert!(matches!(
            source.activity(&terminal_id),
            Err(DurabilityError::ActivityNotFound { .. })
        ));
        assert_eq!(
            RunState::from_events(source.events().to_vec()).unwrap(),
            source
        );
    }
}
