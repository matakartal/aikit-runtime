//! Persistence SPI for the append-only durable run state.

use crate::durability::RunState;
use std::collections::HashMap;
use std::sync::Mutex;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum DurableStoreError {
    #[error("durable run '{run_id}' was not found")]
    NotFound { run_id: String },
    #[error("durable run '{run_id}' already exists")]
    AlreadyExists { run_id: String },
    #[error("durable run '{run_id}' revision conflict: expected {expected}, found {actual}")]
    Conflict {
        run_id: String,
        expected: u64,
        actual: u64,
    },
    #[error("durable store I/O failed: {0}")]
    Io(String),
    #[error("durable store state is invalid: {0}")]
    Invalid(String),
}

pub type DurableStoreResult<T> = Result<T, DurableStoreError>;

/// Checkpoint store boundary shared by local and distributed durable engines.
pub trait DurableStore: Send + Sync {
    fn create(&self, state: &RunState) -> DurableStoreResult<()>;
    fn load(&self, run_id: &str) -> DurableStoreResult<RunState>;
    /// Atomically replace the serialized projection only when the event-log sequence still
    /// matches `expected_sequence`.
    fn compare_and_swap(
        &self,
        expected_sequence: u64,
        replacement: &RunState,
    ) -> DurableStoreResult<()>;
}

#[derive(Default)]
pub struct InMemoryDurableStore {
    runs: Mutex<HashMap<String, RunState>>,
}

fn last_sequence(state: &RunState) -> u64 {
    state.events().last().map_or(0, |event| event.sequence)
}

/// Enforce the event log as the durable authority while a backend holds its CAS lock.
pub(crate) fn validate_append_only(
    current: &RunState,
    replacement: &RunState,
) -> DurableStoreResult<()> {
    if current.run_id() != replacement.run_id() {
        return Err(DurableStoreError::Invalid(
            "replacement run ID does not match current run".into(),
        ));
    }
    if current.session_id() != replacement.session_id() {
        return Err(DurableStoreError::Invalid(
            "replacement session ID does not match current run".into(),
        ));
    }
    let current_events = current.events();
    let replacement_events = replacement.events();
    if replacement_events.len() < current_events.len()
        || replacement_events[..current_events.len()] != *current_events
    {
        return Err(DurableStoreError::Invalid(
            "replacement event log is not an append-only extension".into(),
        ));
    }
    if replacement_events.len() == current_events.len() && replacement != current {
        return Err(DurableStoreError::Invalid(
            "same-revision replacement changed the durable projection".into(),
        ));
    }
    Ok(())
}

impl DurableStore for InMemoryDurableStore {
    fn create(&self, state: &RunState) -> DurableStoreResult<()> {
        let mut runs = self
            .runs
            .lock()
            .map_err(|_| DurableStoreError::Io("durable store mutex poisoned".into()))?;
        if runs.contains_key(state.run_id()) {
            return Err(DurableStoreError::AlreadyExists {
                run_id: state.run_id().into(),
            });
        }
        runs.insert(state.run_id().into(), state.clone());
        Ok(())
    }

    fn load(&self, run_id: &str) -> DurableStoreResult<RunState> {
        self.runs
            .lock()
            .map_err(|_| DurableStoreError::Io("durable store mutex poisoned".into()))?
            .get(run_id)
            .cloned()
            .ok_or_else(|| DurableStoreError::NotFound {
                run_id: run_id.into(),
            })
    }

    fn compare_and_swap(
        &self,
        expected_sequence: u64,
        replacement: &RunState,
    ) -> DurableStoreResult<()> {
        let mut runs = self
            .runs
            .lock()
            .map_err(|_| DurableStoreError::Io("durable store mutex poisoned".into()))?;
        let current =
            runs.get(replacement.run_id())
                .ok_or_else(|| DurableStoreError::NotFound {
                    run_id: replacement.run_id().into(),
                })?;
        let actual = last_sequence(current);
        if actual != expected_sequence {
            return Err(DurableStoreError::Conflict {
                run_id: replacement.run_id().into(),
                expected: expected_sequence,
                actual,
            });
        }
        validate_append_only(current, replacement)?;
        runs.insert(replacement.run_id().into(), replacement.clone());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::durability::{
        ApprovalResolution, DurabilityMode, DurableApprovalKind, DurableApprovalRequest,
        DurableApprovalStatus, RunCommand,
    };
    use crate::governance::{
        PolicyDocument, PolicyEffect, PolicySnapshot, GOVERNANCE_CONTRACT_VERSION,
    };
    use serde_json::json;

    #[test]
    fn cas_prevents_two_workers_from_committing_the_same_revision() {
        let store = InMemoryDurableStore::default();
        let initial = RunState::new("session-1", "run-1", DurabilityMode::Sync).unwrap();
        store.create(&initial).unwrap();
        let expected = last_sequence(&initial);

        let mut first = initial.clone();
        first
            .replace_state("worker-1", json!({"worker": 1}))
            .unwrap();
        store.compare_and_swap(expected, &first).unwrap();

        let mut second = initial;
        second
            .replace_state("worker-2", json!({"worker": 2}))
            .unwrap();
        assert!(matches!(
            store.compare_and_swap(expected, &second),
            Err(DurableStoreError::Conflict { .. })
        ));
    }

    #[test]
    fn cas_rejects_same_revision_divergent_history() {
        let store = InMemoryDurableStore::default();
        let initial = RunState::new("session-1", "run-1", DurabilityMode::Sync).unwrap();
        store.create(&initial).unwrap();

        let mut committed = initial.clone();
        committed
            .replace_state("committed", json!({"worker": 1}))
            .unwrap();
        store
            .compare_and_swap(last_sequence(&initial), &committed)
            .unwrap();

        let mut divergent = initial;
        divergent
            .replace_state("divergent", json!({"worker": 2}))
            .unwrap();
        assert!(matches!(
            store.compare_and_swap(last_sequence(&committed), &divergent),
            Err(DurableStoreError::Invalid(_))
        ));
        assert_eq!(store.load("run-1").unwrap(), committed);
    }

    #[test]
    fn restart_preserves_policy_and_turns_expired_approval_into_a_durable_deny() {
        let policy = PolicySnapshot::seal(PolicyDocument {
            schema_version: GOVERNANCE_CONTRACT_VERSION,
            default_effect: PolicyEffect::Ask,
            rules: Vec::new(),
        })
        .unwrap();
        let mut initial =
            RunState::new_with_policy_snapshot("session", "restart", DurabilityMode::Sync, &policy)
                .unwrap();
        let approval_id = initial
            .request_typed_approval(DurableApprovalRequest {
                logical_key: "publish".into(),
                activity_id: None,
                kind: DurableApprovalKind::OutputReview,
                prompt: "Publish output?".into(),
                payload: json!({"artifact": "report"}),
                policy_snapshot_hash: Some(policy.hash().into()),
                governance_binding: None,
                requested_at_unix_ms: 10,
                expires_at_unix_ms: 20,
            })
            .unwrap();
        let store = InMemoryDurableStore::default();
        store.create(&initial).unwrap();

        let mut restarted = store.load("restart").unwrap();
        let revision = last_sequence(&restarted);
        restarted
            .apply_command_at(
                RunCommand::Resume {
                    command_id: "resume-after-restart".into(),
                    approvals: vec![ApprovalResolution {
                        approval_id: approval_id.clone(),
                        approved: true,
                        response: None,
                    }],
                },
                20,
            )
            .unwrap();
        store.compare_and_swap(revision, &restarted).unwrap();

        let persisted = store.load("restart").unwrap();
        assert_eq!(persisted.policy_snapshot_hash(), Some(policy.hash()));
        assert_eq!(
            persisted.projection().approvals[&approval_id].status,
            DurableApprovalStatus::Rejected
        );
        assert!(persisted.projection().approvals[&approval_id].timed_out);
    }
}
