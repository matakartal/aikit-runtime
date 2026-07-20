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

/// Schema version for serialized durable run state and events.
pub const DURABILITY_SCHEMA_VERSION: u32 = 1;

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
        if serialized.schema_version != DURABILITY_SCHEMA_VERSION {
            return Err(D::Error::custom(format!(
                "unsupported durability schema version {}; expected {}",
                serialized.schema_version, DURABILITY_SCHEMA_VERSION
            )));
        }
        let replayed = RunState::from_events(serialized.events.clone())
            .map_err(|error| D::Error::custom(error.to_string()))?;
        if replayed.schema_version != serialized.schema_version
            || replayed.session_id != serialized.session_id
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
            state.append_event(event)?;
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

    /// Record an explicit operator/integration reconciliation.
    pub fn reconcile_activity(
        &mut self,
        reconciliation_id: &str,
        activity_id: &str,
        resolution: ActivityReconciliation,
    ) -> DurabilityResult<AppendOutcome> {
        validate_identifier("reconciliation_id", reconciliation_id)?;
        let attempt = self
            .activity(activity_id)?
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
        let checkpoint = self.checkpoint_by_id(checkpoint_id)?.clone();
        let new_branch_id = stable_identifier("branch", &[new_run_id, "fork", command_id]);
        let event_id = stable_identifier("event", &[&self.run_id, "fork", command_id]);
        if let Some(event) = self.event_by_id(&event_id) {
            if let RunEventKind::ForkCreated {
                new_run_id: existing_run_id,
                checkpoint_id: existing_checkpoint_id,
                new_branch_id: existing_branch_id,
                ..
            } = &event.kind
            {
                if existing_run_id == new_run_id && existing_checkpoint_id == checkpoint_id {
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
        let projection = fork_projection(&checkpoint, &new_branch_id);
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

    fn rewind(
        &mut self,
        command_id: &str,
        checkpoint_id: &str,
        side_effects_reconciled: bool,
    ) -> DurabilityResult<CommandOutcome> {
        validate_identifier("command_id", command_id)?;
        let checkpoint = self.checkpoint_by_id(checkpoint_id)?.clone();
        let new_branch_id = stable_identifier(
            "branch",
            &[&self.run_id, "rewind", command_id, checkpoint_id],
        );
        let event_id = stable_identifier("event", &[&self.run_id, "rewind", command_id]);
        if let Some(event) = self.event_by_id(&event_id) {
            if let RunEventKind::RunRewound {
                checkpoint_id: existing_checkpoint_id,
                ..
            } = &event.kind
            {
                if existing_checkpoint_id == checkpoint_id {
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
                let class = self
                    .events
                    .iter()
                    .find_map(|candidate| match &candidate.kind {
                        RunEventKind::ActivityScheduled { definition }
                            if definition.activity_id == *activity_id =>
                        {
                            Some(definition.side_effect_class)
                        }
                        _ => None,
                    });
                if class.is_some_and(|class| class != SideEffectClass::Pure) {
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
                    self.parent_run_id = Some(source_run_id.clone());
                    self.durability = *durability;
                    self.projection = fork_projection(source_checkpoint, new_branch_id);
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
                let sequence = event.sequence;
                let attempt_state = self.running_attempt_mut(activity_id, *attempt)?;
                attempt_state.status = ActivityAttemptStatus::Failed;
                attempt_state.finished_sequence = Some(sequence);
                attempt_state.error = Some(error.clone());
                attempt_state.retryable = *retryable;
                attempt_state.effect_ambiguous = *effect_ambiguous;
            }
            RunEventKind::ActivityReconciliationRequired {
                activity_id,
                attempt,
                reason,
            } => {
                self.ensure_active()?;
                let sequence = event.sequence;
                let record = self.activity_mut(activity_id)?;
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
                self.projection.status = DurableRunStatus::ReconcileRequired;
                self.projection.pause_reason = Some(reason.clone());
            }
            RunEventKind::ActivityReconciled {
                activity_id,
                attempt,
                resolution,
            } => {
                let previous_run_status = self.projection.status;
                let sequence = event.sequence;
                let attempt_state = self
                    .activity_mut(activity_id)?
                    .attempts
                    .iter_mut()
                    .find(|candidate| candidate.attempt == *attempt)
                    .ok_or_else(|| invalid_activity(activity_id, "attempt was not found".into()))?;
                if attempt_state.status != ActivityAttemptStatus::ReconcileRequired {
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
                let decoded = decode_approval_payload(payload)?;
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
                let decoded = decode_resolution_payload(approval, *approved, response)?;
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
                self.require_reconciled_after(checkpoint.event_sequence, *side_effects_reconciled)?;
            }
            RunEventKind::RunRewound {
                checkpoint_id,
                new_branch_id,
                side_effects_reconciled,
            } => {
                self.ensure_active()?;
                let checkpoint = self.checkpoint_by_id(checkpoint_id)?.clone();
                self.require_reconciled_after(checkpoint.event_sequence, *side_effects_reconciled)?;
                self.projection = checkpoint.projection;
                self.projection.branch_id = new_branch_id.clone();
                self.projection.current_checkpoint_id = Some(checkpoint_id.clone());
                self.projection.status = DurableRunStatus::Paused;
                self.projection.pause_reason = Some(format!("rewound to `{checkpoint_id}`"));
            }
            RunEventKind::RunCompleted => {
                self.ensure_active()?;
                if !self.projection.pending_approval_ids().is_empty() {
                    return Err(DurabilityError::PendingApprovals);
                }
                if self.has_reconciliation_pending() {
                    return Err(DurabilityError::RunRequiresReconciliation);
                }
                if self.projection.activities.values().any(|record| {
                    record
                        .latest_attempt()
                        .is_some_and(|attempt| attempt.status == ActivityAttemptStatus::Running)
                }) {
                    return Err(DurabilityError::InvalidEvent {
                        reason: "run cannot complete while an activity is running".into(),
                    });
                }
                self.projection.status = DurableRunStatus::Completed;
                self.projection.pause_reason = None;
            }
            RunEventKind::RunFailed { error } => {
                self.ensure_active()?;
                self.projection.status = DurableRunStatus::Failed;
                self.projection.pause_reason = Some(error.clone());
            }
            RunEventKind::RunCancelled { reason } => {
                self.ensure_active()?;
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
}

fn append_sequence(outcome: AppendOutcome) -> u64 {
    match outcome {
        AppendOutcome::Appended { sequence } | AppendOutcome::Deduplicated { sequence } => sequence,
    }
}

fn fork_projection(checkpoint: &Checkpoint, new_branch_id: &str) -> RunProjection {
    let mut projection = checkpoint.projection.clone();
    projection.branch_id = new_branch_id.to_string();
    projection.current_checkpoint_id = Some(checkpoint.checkpoint_id.clone());
    if projection.status == DurableRunStatus::ReconcileRequired {
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

fn decode_approval_payload(payload: &Value) -> DurabilityResult<DecodedApprovalPayload> {
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
    if envelope.schema_version != DURABILITY_SCHEMA_VERSION {
        return Err(DurabilityError::UnsupportedSchema {
            expected: DURABILITY_SCHEMA_VERSION,
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
    if envelope.schema_version != DURABILITY_SCHEMA_VERSION {
        return Err(DurabilityError::UnsupportedSchema {
            expected: DURABILITY_SCHEMA_VERSION,
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

fn validate_identifier(field: &'static str, value: &str) -> DurabilityResult<()> {
    if value.is_empty() || value.chars().any(char::is_control) {
        return Err(DurabilityError::InvalidIdentifier { field });
    }
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
}
