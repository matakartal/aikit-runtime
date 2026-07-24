//! Deterministic Temporal mapping for AIKit durable activities.
//!
//! This module is deliberately an SDK boundary rather than a second execution engine. A Temporal
//! workflow calls [`TemporalAdapter::prepare_activity`], translates an `Execute` plan to its SDK's
//! activity command, and feeds the history-recorded result back through
//! [`TemporalAdapter::record_outcome`]. The adapter itself performs no I/O, reads no clock, and
//! generates no random values, so the mapping is safe to repeat during workflow replay.

use crate::durability::{
    stable_id, ActivityAttemptStatus, ActivityDecision, ActivityReconciliation, DurabilityError,
    RunState, SideEffectClass,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

/// Host configuration copied into every Temporal activity command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TemporalAdapterConfig {
    pub task_queue: String,
    /// Temporal `start_to_close_timeout` represented without a runtime-specific duration type.
    pub start_to_close_timeout_ms: u64,
    /// Automatic Temporal attempts for pure or externally idempotent activities.
    pub maximum_automatic_attempts: u32,
}

impl TemporalAdapterConfig {
    pub fn new(task_queue: impl Into<String>) -> Self {
        Self {
            task_queue: task_queue.into(),
            start_to_close_timeout_ms: 60_000,
            maximum_automatic_attempts: 3,
        }
    }
}

/// Logical activity input supplied by an AIKit coordinator.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TemporalActivitySpec {
    pub stable_step_id: String,
    pub logical_key: String,
    pub input: Value,
    pub side_effect_class: SideEffectClass,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
}

/// Temporal retry configuration derived from AIKit side-effect semantics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum TemporalRetryPolicy {
    /// Temporal may retry a lost or failed activity result. External work carries a stable key.
    Automatic { maximum_attempts: u32 },
    /// Temporal must schedule exactly one attempt and surface ambiguity to AIKit reconciliation.
    Disabled,
}

/// SDK-neutral command that maps directly to a Temporal activity invocation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TemporalActivityInvocation {
    pub workflow_id: String,
    pub run_id: String,
    pub session_id: String,
    /// Unique per AIKit attempt; stable across Temporal workflow replay.
    pub temporal_activity_id: String,
    pub activity_id: String,
    pub attempt: u32,
    /// Stable deployment identifier. Renaming it is a workflow compatibility change.
    pub activity_type: String,
    pub task_queue: String,
    pub start_to_close_timeout_ms: u64,
    pub retry_policy: TemporalRetryPolicy,
    pub input: Value,
    pub input_hash: String,
    pub idempotency_key: Option<String>,
    /// Deterministically ordered headers for SDK propagation and worker-side validation.
    pub headers: BTreeMap<String, String>,
}

/// Deterministic plan returned to a Temporal workflow host.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "disposition", rename_all = "snake_case", deny_unknown_fields)]
pub enum TemporalActivityPlan {
    Execute {
        invocation: Box<TemporalActivityInvocation>,
    },
    UseRecorded {
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

/// History-recorded result returned by the host's Temporal SDK.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
pub enum TemporalActivityOutcome {
    Completed {
        output: Value,
    },
    Failed {
        error: String,
        retryable: bool,
        /// True when a worker may have completed its external effect before losing the result.
        effect_ambiguous: bool,
    },
    Cancelled {
        /// Cancellation does not prove that an already-started external operation was stopped.
        effect_ambiguous: bool,
    },
}

/// Next workflow action after an operator or integration reconciles an ambiguous activity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "disposition", rename_all = "snake_case", deny_unknown_fields)]
pub enum TemporalReconciliationPlan {
    UseReconciledOutput {
        activity_id: String,
        output: Value,
    },
    /// The host must append an explicit AIKit `Resume` command before scheduling another attempt.
    ResumeRequired {
        activity_id: String,
    },
    Cancelled {
        activity_id: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TemporalAdapterError {
    #[error("invalid Temporal adapter configuration: {0}")]
    InvalidConfiguration(String),
    #[error("Temporal invocation does not match durable run state: {0}")]
    InvocationMismatch(String),
    #[error(transparent)]
    Durability(#[from] DurabilityError),
}

pub type TemporalAdapterResult<T> = Result<T, TemporalAdapterError>;

/// Pure mapping between AIKit durable state and Temporal SDK commands/history results.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TemporalAdapter {
    config: TemporalAdapterConfig,
}

impl TemporalAdapter {
    pub fn new(config: TemporalAdapterConfig) -> TemporalAdapterResult<Self> {
        if config.task_queue.trim().is_empty() {
            return Err(TemporalAdapterError::InvalidConfiguration(
                "task_queue cannot be empty".into(),
            ));
        }
        if config.start_to_close_timeout_ms == 0 {
            return Err(TemporalAdapterError::InvalidConfiguration(
                "start_to_close_timeout_ms must be positive".into(),
            ));
        }
        if config.maximum_automatic_attempts == 0 {
            return Err(TemporalAdapterError::InvalidConfiguration(
                "maximum_automatic_attempts must be positive".into(),
            ));
        }
        Ok(Self { config })
    }

    pub fn config(&self) -> &TemporalAdapterConfig {
        &self.config
    }

    /// Stable Temporal workflow ID for one AIKit durable run.
    pub fn workflow_id(&self, state: &RunState) -> String {
        stable_id("temporal_workflow", &[state.session_id(), state.run_id()])
    }

    /// Prepare or replay one logical activity without performing I/O.
    pub fn prepare_activity(
        &self,
        state: &mut RunState,
        spec: TemporalActivitySpec,
    ) -> TemporalAdapterResult<TemporalActivityPlan> {
        let decision = state.prepare_activity(
            &spec.stable_step_id,
            &spec.logical_key,
            spec.input,
            spec.side_effect_class,
            spec.idempotency_key,
        )?;
        self.plan_from_decision(state, decision)
    }

    /// Apply one result already made durable in Temporal workflow history.
    pub fn record_outcome(
        &self,
        state: &mut RunState,
        invocation: &TemporalActivityInvocation,
        outcome: TemporalActivityOutcome,
    ) -> TemporalAdapterResult<TemporalActivityPlan> {
        self.validate_invocation(state, invocation)?;
        match outcome {
            TemporalActivityOutcome::Completed { output } => {
                state.complete_activity(&invocation.activity_id, invocation.attempt, output)?;
                let output = state
                    .activity(&invocation.activity_id)?
                    .completed_output()
                    .cloned()
                    .ok_or_else(|| {
                        TemporalAdapterError::InvocationMismatch(
                            "completed activity has no recorded output".into(),
                        )
                    })?;
                Ok(TemporalActivityPlan::UseRecorded {
                    activity_id: invocation.activity_id.clone(),
                    output,
                })
            }
            TemporalActivityOutcome::Failed {
                error,
                retryable,
                effect_ambiguous,
            } => {
                let decision = state.fail_activity(
                    &invocation.activity_id,
                    invocation.attempt,
                    error,
                    retryable,
                    effect_ambiguous,
                )?;
                self.plan_from_decision(state, decision)
            }
            TemporalActivityOutcome::Cancelled { effect_ambiguous } => {
                let decision = state.cancel_activity(
                    &invocation.activity_id,
                    invocation.attempt,
                    "Temporal activity was cancelled",
                    effect_ambiguous,
                )?;
                self.plan_from_decision(state, decision)
            }
        }
    }

    /// Apply explicit reconciliation without silently resuming the workflow.
    ///
    /// Keeping resume separate preserves the durable operator decision and prevents a stale
    /// Temporal worker from turning reconciliation into automatic replay.
    pub fn record_reconciliation(
        &self,
        state: &mut RunState,
        reconciliation_id: &str,
        activity_id: &str,
        resolution: ActivityReconciliation,
    ) -> TemporalAdapterResult<TemporalReconciliationPlan> {
        state.reconcile_activity(reconciliation_id, activity_id, resolution.clone())?;
        Ok(match resolution {
            ActivityReconciliation::Completed { .. } => {
                let output = state
                    .activity(activity_id)?
                    .completed_output()
                    .cloned()
                    .ok_or_else(|| {
                        TemporalAdapterError::InvocationMismatch(
                            "reconciled completion has no recorded output".into(),
                        )
                    })?;
                TemporalReconciliationPlan::UseReconciledOutput {
                    activity_id: activity_id.into(),
                    output,
                }
            }
            ActivityReconciliation::SafeToRetry => TemporalReconciliationPlan::ResumeRequired {
                activity_id: activity_id.into(),
            },
            ActivityReconciliation::Cancelled => TemporalReconciliationPlan::Cancelled {
                activity_id: activity_id.into(),
            },
        })
    }

    fn plan_from_decision(
        &self,
        state: &RunState,
        decision: ActivityDecision,
    ) -> TemporalAdapterResult<TemporalActivityPlan> {
        match decision {
            ActivityDecision::Execute {
                activity_id,
                attempt,
                ..
            } => Ok(TemporalActivityPlan::Execute {
                invocation: Box::new(self.expected_invocation(state, &activity_id, attempt)?),
            }),
            ActivityDecision::ReuseCompleted {
                activity_id,
                output,
            } => Ok(TemporalActivityPlan::UseRecorded {
                activity_id,
                output,
            }),
            ActivityDecision::ReconcileRequired {
                activity_id,
                reason,
            } => Ok(TemporalActivityPlan::ReconcileRequired {
                activity_id,
                reason,
            }),
            ActivityDecision::Failed { activity_id, error } => {
                Ok(TemporalActivityPlan::Failed { activity_id, error })
            }
            ActivityDecision::Cancelled { activity_id } => {
                Ok(TemporalActivityPlan::Cancelled { activity_id })
            }
        }
    }

    fn validate_invocation(
        &self,
        state: &RunState,
        invocation: &TemporalActivityInvocation,
    ) -> TemporalAdapterResult<()> {
        let expected =
            self.expected_invocation(state, &invocation.activity_id, invocation.attempt)?;
        if invocation != &expected {
            return Err(TemporalAdapterError::InvocationMismatch(
                "activity invocation fields changed".into(),
            ));
        }
        Ok(())
    }

    /// Rebuild every invocation invariant from trusted durable state and adapter configuration.
    /// The same builder is used for command generation and outcome validation so fields added to
    /// the wire contract cannot silently become validation gaps.
    fn expected_invocation(
        &self,
        state: &RunState,
        activity_id: &str,
        attempt: u32,
    ) -> TemporalAdapterResult<TemporalActivityInvocation> {
        let record = state.activity(activity_id).map_err(|_| {
            TemporalAdapterError::InvocationMismatch("activity identifier changed".into())
        })?;
        let latest = record.latest_attempt().ok_or_else(|| {
            TemporalAdapterError::InvocationMismatch("activity has no started attempt".into())
        })?;
        if latest.attempt != attempt || latest.status != ActivityAttemptStatus::Running {
            return Err(TemporalAdapterError::InvocationMismatch(
                "activity attempt is stale or not running".into(),
            ));
        }

        let definition = &record.definition;
        let retry_policy = match definition.side_effect_class {
            SideEffectClass::Pure | SideEffectClass::Idempotent => TemporalRetryPolicy::Automatic {
                maximum_attempts: self.config.maximum_automatic_attempts,
            },
            SideEffectClass::ReconcileRequired => TemporalRetryPolicy::Disabled,
        };
        let mut headers = BTreeMap::from([
            ("aikit-run-id".into(), state.run_id().into()),
            ("aikit-activity-id".into(), activity_id.into()),
            ("aikit-input-hash".into(), definition.input_hash.clone()),
            ("aikit-attempt".into(), attempt.to_string()),
            (
                "aikit-side-effect-class".into(),
                side_effect_name(definition.side_effect_class).into(),
            ),
        ]);
        if let Some(key) = definition.idempotency_key.as_ref() {
            headers.insert("aikit-idempotency-key".into(), key.clone());
        }

        Ok(TemporalActivityInvocation {
            workflow_id: self.workflow_id(state),
            run_id: state.run_id().into(),
            session_id: state.session_id().into(),
            temporal_activity_id: stable_id(
                "temporal_activity",
                &[state.run_id(), activity_id, &attempt.to_string()],
            ),
            activity_id: activity_id.into(),
            attempt,
            activity_type: definition.stable_step_id.clone(),
            task_queue: self.config.task_queue.clone(),
            start_to_close_timeout_ms: self.config.start_to_close_timeout_ms,
            retry_policy,
            input: definition.input.clone(),
            input_hash: definition.input_hash.clone(),
            idempotency_key: definition.idempotency_key.clone(),
            headers,
        })
    }
}

fn side_effect_name(class: SideEffectClass) -> &'static str {
    match class {
        SideEffectClass::Pure => "pure",
        SideEffectClass::Idempotent => "idempotent",
        SideEffectClass::ReconcileRequired => "reconcile_required",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::durability::DurableRunStatus;
    use serde_json::json;

    fn adapter() -> TemporalAdapter {
        TemporalAdapter::new(TemporalAdapterConfig {
            task_queue: "aikit-workers".into(),
            start_to_close_timeout_ms: 30_000,
            maximum_automatic_attempts: 5,
        })
        .unwrap()
    }

    fn execute(plan: TemporalActivityPlan) -> TemporalActivityInvocation {
        match plan {
            TemporalActivityPlan::Execute { invocation } => *invocation,
            other => panic!("expected execute plan, got {other:?}"),
        }
    }

    #[test]
    fn mapping_is_deterministic_across_workflow_replay() {
        let initial = RunState::new("session", "run", crate::DurabilityMode::Sync).unwrap();
        let spec = TemporalActivitySpec {
            stable_step_id: "lookup-v1".into(),
            logical_key: "turn-1/lookup".into(),
            input: json!({"query": "aikit"}),
            side_effect_class: SideEffectClass::Pure,
            idempotency_key: None,
        };
        let mut first_state = initial.clone();
        let mut replayed_state = initial;
        let first = adapter()
            .prepare_activity(&mut first_state, spec.clone())
            .unwrap();
        let replayed = adapter()
            .prepare_activity(&mut replayed_state, spec)
            .unwrap();
        assert_eq!(first, replayed);
        assert_eq!(first_state.events(), replayed_state.events());
    }

    #[test]
    fn pure_and_idempotent_work_map_to_automatic_temporal_retry() {
        let mut pure = RunState::new("session", "pure", crate::DurabilityMode::Sync).unwrap();
        let pure_invocation = execute(
            adapter()
                .prepare_activity(
                    &mut pure,
                    TemporalActivitySpec {
                        stable_step_id: "calculate-v1".into(),
                        logical_key: "calculation".into(),
                        input: json!({"x": 2}),
                        side_effect_class: SideEffectClass::Pure,
                        idempotency_key: None,
                    },
                )
                .unwrap(),
        );
        assert_eq!(
            pure_invocation.retry_policy,
            TemporalRetryPolicy::Automatic {
                maximum_attempts: 5
            }
        );

        let mut external =
            RunState::new("session", "external", crate::DurabilityMode::Sync).unwrap();
        let external_invocation = execute(
            adapter()
                .prepare_activity(
                    &mut external,
                    TemporalActivitySpec {
                        stable_step_id: "charge-v1".into(),
                        logical_key: "invoice-1".into(),
                        input: json!({"amount": 10}),
                        side_effect_class: SideEffectClass::Idempotent,
                        idempotency_key: Some("invoice-1".into()),
                    },
                )
                .unwrap(),
        );
        assert_eq!(
            external_invocation.headers["aikit-idempotency-key"],
            "invoice-1"
        );
        assert!(matches!(
            external_invocation.retry_policy,
            TemporalRetryPolicy::Automatic { .. }
        ));
    }

    #[test]
    fn unsafe_activity_disables_temporal_retry_and_ambiguity_requires_reconciliation() {
        let mut run = RunState::new("session", "unsafe", crate::DurabilityMode::Sync).unwrap();
        let invocation = execute(
            adapter()
                .prepare_activity(
                    &mut run,
                    TemporalActivitySpec {
                        stable_step_id: "wire-transfer-v1".into(),
                        logical_key: "transfer-1".into(),
                        input: json!({"amount": 100}),
                        side_effect_class: SideEffectClass::ReconcileRequired,
                        idempotency_key: None,
                    },
                )
                .unwrap(),
        );
        assert_eq!(invocation.retry_policy, TemporalRetryPolicy::Disabled);
        let plan = adapter()
            .record_outcome(
                &mut run,
                &invocation,
                TemporalActivityOutcome::Failed {
                    error: "worker lost after dispatch".into(),
                    retryable: true,
                    effect_ambiguous: true,
                },
            )
            .unwrap();
        assert!(matches!(
            plan,
            TemporalActivityPlan::ReconcileRequired { .. }
        ));
        assert_eq!(run.status(), DurableRunStatus::ReconcileRequired);

        let reconciled = adapter()
            .record_reconciliation(
                &mut run,
                "operator-check-1",
                &invocation.activity_id,
                ActivityReconciliation::SafeToRetry,
            )
            .unwrap();
        assert_eq!(
            reconciled,
            TemporalReconciliationPlan::ResumeRequired {
                activity_id: invocation.activity_id
            }
        );
        assert_eq!(run.status(), DurableRunStatus::Paused);
    }

    #[test]
    fn recorded_completion_is_reused_without_second_temporal_command() {
        let adapter = adapter();
        let mut run = RunState::new("session", "completed", crate::DurabilityMode::Sync).unwrap();
        let spec = TemporalActivitySpec {
            stable_step_id: "lookup-v1".into(),
            logical_key: "lookup".into(),
            input: json!({"id": 1}),
            side_effect_class: SideEffectClass::Pure,
            idempotency_key: None,
        };
        let invocation = execute(adapter.prepare_activity(&mut run, spec.clone()).unwrap());
        let recorded = adapter
            .record_outcome(
                &mut run,
                &invocation,
                TemporalActivityOutcome::Completed {
                    output: json!({"name": "Ada"}),
                },
            )
            .unwrap();
        assert!(matches!(recorded, TemporalActivityPlan::UseRecorded { .. }));
        let event_count = run.events().len();
        let replay = adapter.prepare_activity(&mut run, spec).unwrap();
        assert_eq!(recorded, replay);
        assert_eq!(run.events().len(), event_count);
    }

    #[test]
    fn idempotent_failure_creates_new_aikit_attempt_with_same_external_key() {
        let adapter = adapter();
        let mut run = RunState::new("session", "retry", crate::DurabilityMode::Sync).unwrap();
        let first = execute(
            adapter
                .prepare_activity(
                    &mut run,
                    TemporalActivitySpec {
                        stable_step_id: "write-v1".into(),
                        logical_key: "write-1".into(),
                        input: json!({"value": 1}),
                        side_effect_class: SideEffectClass::Idempotent,
                        idempotency_key: Some("write-1".into()),
                    },
                )
                .unwrap(),
        );
        let second = execute(
            adapter
                .record_outcome(
                    &mut run,
                    &first,
                    TemporalActivityOutcome::Failed {
                        error: "retryable".into(),
                        retryable: true,
                        effect_ambiguous: true,
                    },
                )
                .unwrap(),
        );
        assert_eq!(second.attempt, 2);
        assert_ne!(first.temporal_activity_id, second.temporal_activity_id);
        assert_eq!(first.idempotency_key, second.idempotency_key);
    }

    #[test]
    fn unambiguous_cancellation_is_durable_and_replays_as_cancelled() {
        let adapter = adapter();
        let mut run = RunState::new("session", "cancelled", crate::DurabilityMode::Sync).unwrap();
        let spec = TemporalActivitySpec {
            stable_step_id: "cancelled-read-v1".into(),
            logical_key: "read-1".into(),
            input: json!({"id": 1}),
            side_effect_class: SideEffectClass::Pure,
            idempotency_key: None,
        };
        let invocation = execute(adapter.prepare_activity(&mut run, spec.clone()).unwrap());
        let cancelled = adapter
            .record_outcome(
                &mut run,
                &invocation,
                TemporalActivityOutcome::Cancelled {
                    effect_ambiguous: false,
                },
            )
            .unwrap();
        assert_eq!(
            cancelled,
            TemporalActivityPlan::Cancelled {
                activity_id: invocation.activity_id.clone(),
            }
        );
        assert_eq!(
            run.activity(&invocation.activity_id)
                .unwrap()
                .latest_attempt()
                .unwrap()
                .status,
            crate::durability::ActivityAttemptStatus::Cancelled
        );

        let mut replayed = RunState::from_events(run.events().to_vec()).unwrap();
        assert_eq!(replayed, run);
        assert_eq!(
            adapter.prepare_activity(&mut replayed, spec).unwrap(),
            cancelled
        );
    }

    #[test]
    fn stale_or_tampered_invocation_is_rejected_before_state_change() {
        let adapter = adapter();
        let mut run = RunState::new("session", "tampered", crate::DurabilityMode::Sync).unwrap();
        let mut invocation = execute(
            adapter
                .prepare_activity(
                    &mut run,
                    TemporalActivitySpec {
                        stable_step_id: "read-v1".into(),
                        logical_key: "read".into(),
                        input: json!({"id": 1}),
                        side_effect_class: SideEffectClass::Pure,
                        idempotency_key: None,
                    },
                )
                .unwrap(),
        );
        invocation.input_hash = "sha256:tampered".into();
        let before = run.clone();
        assert!(matches!(
            adapter.record_outcome(
                &mut run,
                &invocation,
                TemporalActivityOutcome::Completed { output: json!(1) }
            ),
            Err(TemporalAdapterError::InvocationMismatch(_))
        ));
        assert_eq!(run, before);
    }

    #[test]
    fn every_security_critical_invocation_field_is_revalidated() {
        let adapter = adapter();
        let mut run = RunState::new("session", "all-fields", crate::DurabilityMode::Sync).unwrap();
        let invocation = execute(
            adapter
                .prepare_activity(
                    &mut run,
                    TemporalActivitySpec {
                        stable_step_id: "write-v1".into(),
                        logical_key: "write".into(),
                        input: json!({"value": 1}),
                        side_effect_class: SideEffectClass::Idempotent,
                        idempotency_key: Some("write-key".into()),
                    },
                )
                .unwrap(),
        );

        let mut tampered = Vec::new();
        let mut candidate = invocation.clone();
        candidate.workflow_id = "other-workflow".into();
        tampered.push(candidate);
        let mut candidate = invocation.clone();
        candidate.run_id = "other-run".into();
        tampered.push(candidate);
        let mut candidate = invocation.clone();
        candidate.session_id = "other-session".into();
        tampered.push(candidate);
        let mut candidate = invocation.clone();
        candidate.temporal_activity_id = "other-temporal-activity".into();
        tampered.push(candidate);
        let mut candidate = invocation.clone();
        candidate.activity_id = "other-activity".into();
        tampered.push(candidate);
        let mut candidate = invocation.clone();
        candidate.attempt += 1;
        tampered.push(candidate);
        let mut candidate = invocation.clone();
        candidate.activity_type = "other-type-v1".into();
        tampered.push(candidate);
        let mut candidate = invocation.clone();
        candidate.task_queue = "other-queue".into();
        tampered.push(candidate);
        let mut candidate = invocation.clone();
        candidate.start_to_close_timeout_ms += 1;
        tampered.push(candidate);
        let mut candidate = invocation.clone();
        candidate.retry_policy = TemporalRetryPolicy::Disabled;
        tampered.push(candidate);
        let mut candidate = invocation.clone();
        candidate.input = json!({"value": 2});
        tampered.push(candidate);
        let mut candidate = invocation.clone();
        candidate.input_hash = "sha256:other".into();
        tampered.push(candidate);
        let mut candidate = invocation.clone();
        candidate.idempotency_key = Some("other-key".into());
        tampered.push(candidate);
        let mut candidate = invocation.clone();
        candidate
            .headers
            .insert("aikit-run-id".into(), "other-run".into());
        tampered.push(candidate);
        let mut candidate = invocation.clone();
        candidate
            .headers
            .insert("unexpected".into(), "header".into());
        tampered.push(candidate);

        for candidate in tampered {
            let before = run.clone();
            assert!(matches!(
                adapter.record_outcome(
                    &mut run,
                    &candidate,
                    TemporalActivityOutcome::Completed { output: json!(1) }
                ),
                Err(TemporalAdapterError::InvocationMismatch(_))
            ));
            assert_eq!(run, before);
        }

        adapter
            .record_outcome(
                &mut run,
                &invocation,
                TemporalActivityOutcome::Completed { output: json!(1) },
            )
            .unwrap();
        let completed = run.clone();
        assert!(matches!(
            adapter.record_outcome(
                &mut run,
                &invocation,
                TemporalActivityOutcome::Completed { output: json!(1) }
            ),
            Err(TemporalAdapterError::InvocationMismatch(_))
        ));
        assert_eq!(run, completed);
    }

    #[test]
    fn invalid_configuration_fails_before_workflow_execution() {
        assert!(matches!(
            TemporalAdapter::new(TemporalAdapterConfig {
                task_queue: " ".into(),
                start_to_close_timeout_ms: 1,
                maximum_automatic_attempts: 1,
            }),
            Err(TemporalAdapterError::InvalidConfiguration(_))
        ));
    }

    #[test]
    fn temporal_wire_types_reject_unknown_fields() {
        let config = serde_json::from_value::<TemporalAdapterConfig>(json!({
            "task_queue": "workers",
            "start_to_close_timeout_ms": 1000,
            "maximum_automatic_attempts": 2,
            "future_field": true
        }));
        assert!(config.is_err());

        let outcome = serde_json::from_value::<TemporalActivityOutcome>(json!({
            "outcome": "completed",
            "output": 1,
            "future_field": true
        }));
        assert!(outcome.is_err());
    }
}
