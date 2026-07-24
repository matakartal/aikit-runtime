//! Persistence SPI for the append-only durable run state.

use crate::durability::{RunEventKind, RunState, MAX_DURABLE_WORKER_LEASE_MS};
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};
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
    #[error("durable run '{run_id}' has an active worker lease owned by '{owner_id}'")]
    WorkerLeaseRequired { run_id: String, owner_id: String },
    #[error("durable run '{run_id}' rejected worker lease fence for owner '{owner_id}'")]
    WorkerLeaseConflict { run_id: String, owner_id: String },
    #[error("durable run '{run_id}' rejected an invalid worker lease claim")]
    InvalidWorkerLeaseClaim { run_id: String },
    #[error("durable run '{run_id}' has an expired worker lease; only an atomic recovery claim is allowed")]
    WorkerLeaseRecoveryRequired { run_id: String },
    #[error("durable store backend does not support worker lease fencing for run '{run_id}'")]
    LeaseFencingUnsupported { run_id: String },
    #[error("durable store backend does not provide a trusted worker lease clock")]
    LeaseClockUnsupported,
    #[error(
        "durable store backend does not support atomic approval resolution for run '{run_id}'"
    )]
    ApprovalResolutionUnsupported { run_id: String },
    #[error(
        "durable approval '{approval_id}' for run '{run_id}' requires the atomic approval-resolution CAS"
    )]
    ApprovalResolutionGuardRequired { run_id: String, approval_id: String },
    #[error(
        "durable approval '{approval_id}' for run '{run_id}' expired before its atomic resolution at {observed_at_unix_ms}"
    )]
    ApprovalExpired {
        run_id: String,
        approval_id: String,
        observed_at_unix_ms: u64,
    },
    #[error("durable store I/O failed: {0}")]
    Io(String),
    #[error("durable store state is invalid: {0}")]
    Invalid(String),
}

pub type DurableStoreResult<T> = Result<T, DurableStoreError>;

/// Opaque authority proving that a worker successfully claimed and bound a durable lease.
///
/// The owner and token are readable so third-party stores can validate them atomically, but only
/// AIKit's worker binding path can construct this value. It deliberately has no Serde contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableStoreLeaseAuthority {
    owner_id: String,
    lease_id: String,
}

impl DurableStoreLeaseAuthority {
    pub(crate) fn new(owner_id: impl Into<String>, lease_id: impl Into<String>) -> Self {
        Self {
            owner_id: owner_id.into(),
            lease_id: lease_id.into(),
        }
    }

    pub fn owner_id(&self) -> &str {
        &self.owner_id
    }

    pub fn lease_id(&self) -> &str {
        &self.lease_id
    }
}

/// Checkpoint store boundary shared by local and distributed durable engines.
pub trait DurableStore: Send + Sync {
    fn create(&self, state: &RunState) -> DurableStoreResult<()>;
    fn load(&self, run_id: &str) -> DurableStoreResult<RunState>;
    /// Atomically replace the serialized projection only when the event-log sequence still
    /// matches `expected_sequence`.
    ///
    /// Implementations must reject ordinary writes while a worker lease is active. Once a lease
    /// expires, only a single-event atomic recovery claim may use this unfenced entry point.
    fn compare_and_swap(
        &self,
        expected_sequence: u64,
        replacement: &RunState,
    ) -> DurableStoreResult<()>;

    /// Read the backend's trusted worker-lease and approval clock.
    ///
    /// Distributed backends should use their database clock. The default fails closed so a custom
    /// store cannot silently substitute the worker process clock.
    fn worker_lease_clock_unix_ms(&self) -> DurableStoreResult<u64> {
        Err(DurableStoreError::LeaseClockUnsupported)
    }

    /// Whether this backend implements [`Self::compare_and_swap_approval_resolution`] with one
    /// trusted-clock transaction. Persisted approval adapters reject stores that leave the
    /// default disabled, before an approval callback can run.
    fn supports_atomic_approval_resolution(&self) -> bool {
        false
    }

    /// Atomically replace the state only when the current active worker lease has the supplied
    /// owner and fencing token.
    ///
    /// The default fails closed so existing third-party store implementations remain source
    /// compatible without silently claiming atomic fencing support.
    fn compare_and_swap_fenced(
        &self,
        expected_sequence: u64,
        replacement: &RunState,
        authority: &DurableStoreLeaseAuthority,
    ) -> DurableStoreResult<()> {
        let _ = (expected_sequence, authority);
        Err(DurableStoreError::LeaseFencingUnsupported {
            run_id: replacement.run_id().into(),
        })
    }

    /// Atomically resolve a typed approval using the backend's trusted clock.
    ///
    /// Implementations must validate the append-only replacement, any supplied worker lease,
    /// and the named newly approved resolution against the currently persisted approval expiry
    /// while holding the same lock or transaction used for the CAS. Other newly approved IDs must
    /// be rejected; automatically expired denials may share the replacement. Reading the clock
    /// before entering the CAS is not sufficient. The default fails closed so third-party stores
    /// must explicitly provide this atomic boundary before persisted approvals can be allowed.
    /// An expired allow must return [`DurableStoreError::ApprovalExpired`] before writing; its
    /// observed timestamp must come from the same trusted-clock transaction.
    fn compare_and_swap_approval_resolution(
        &self,
        expected_sequence: u64,
        replacement: &RunState,
        approval_id: &str,
        authority: Option<&DurableStoreLeaseAuthority>,
    ) -> DurableStoreResult<()> {
        let _ = (expected_sequence, approval_id, authority);
        Err(DurableStoreError::ApprovalResolutionUnsupported {
            run_id: replacement.run_id().into(),
        })
    }
}

#[derive(Default)]
pub struct InMemoryDurableStore {
    runs: Mutex<HashMap<String, RunState>>,
}

fn last_sequence(state: &RunState) -> u64 {
    state.events().last().map_or(0, |event| event.sequence)
}

pub(crate) fn system_worker_lease_clock_unix_ms() -> DurableStoreResult<u64> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| {
            DurableStoreError::Io(format!("system clock is before Unix epoch: {error}"))
        })?
        .as_millis();
    u64::try_from(millis)
        .map_err(|_| DurableStoreError::Io("system clock exceeds durable lease range".into()))
}

/// Validate worker-lease authority while the backend still holds its CAS lock or transaction.
pub(crate) fn validate_worker_lease_fence(
    current: &RunState,
    replacement: &RunState,
    authority: Option<&DurableStoreLeaseAuthority>,
    now_unix_ms: u64,
) -> DurableStoreResult<()> {
    let active_lease = current
        .worker_lease()
        .filter(|lease| lease.expires_at_unix_ms > now_unix_ms);
    match (current.worker_lease(), active_lease, authority) {
        (None, None, None) if !appends_worker_lease_claim(current, replacement) => Ok(()),
        (None, None, None) if is_atomic_lease_claim(current, replacement, now_unix_ms) => Ok(()),
        (None, None, None) => Err(DurableStoreError::InvalidWorkerLeaseClaim {
            run_id: current.run_id().into(),
        }),
        (Some(_), None, None) if is_atomic_lease_claim(current, replacement, now_unix_ms) => Ok(()),
        (Some(_), None, None) => Err(DurableStoreError::WorkerLeaseRecoveryRequired {
            run_id: current.run_id().into(),
        }),
        (_, Some(lease), None) => Err(DurableStoreError::WorkerLeaseRequired {
            run_id: current.run_id().into(),
            owner_id: lease.owner_id.clone(),
        }),
        (_, Some(lease), Some(authority))
            if lease.owner_id == authority.owner_id && lease.lease_id == authority.lease_id =>
        {
            Ok(())
        }
        (_, _, Some(authority)) => Err(DurableStoreError::WorkerLeaseConflict {
            run_id: current.run_id().into(),
            owner_id: authority.owner_id.clone(),
        }),
    }
}

fn appends_worker_lease_claim(current: &RunState, replacement: &RunState) -> bool {
    replacement.events()[current.events().len().min(replacement.events().len())..]
        .iter()
        .any(|event| matches!(event.kind, RunEventKind::WorkerLeaseClaimed { .. }))
}

fn is_atomic_lease_claim(current: &RunState, replacement: &RunState, now_unix_ms: u64) -> bool {
    let current_len = current.events().len();
    let Some(event) = replacement.events().get(current_len) else {
        return false;
    };
    if replacement.events().len() != current_len + 1 {
        return false;
    }
    let RunEventKind::WorkerLeaseClaimed {
        owner_id,
        lease_id,
        claimed_at_unix_ms,
        expires_at_unix_ms,
    } = &event.kind
    else {
        return false;
    };
    let Some(replacement_lease) = replacement.worker_lease() else {
        return false;
    };
    *claimed_at_unix_ms <= now_unix_ms
        && *expires_at_unix_ms > now_unix_ms
        && expires_at_unix_ms
            .checked_sub(*claimed_at_unix_ms)
            .is_some_and(|duration| duration > 0 && duration <= MAX_DURABLE_WORKER_LEASE_MS)
        && replacement_lease.owner_id == *owner_id
        && replacement_lease.lease_id == *lease_id
        && replacement_lease.acquired_at_unix_ms == *claimed_at_unix_ms
        && replacement_lease.expires_at_unix_ms == *expires_at_unix_ms
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

/// Validate approval expiry while the backend still holds its CAS lock or transaction.
///
/// The named approval must be resolved by this append, and every newly appended allow must still
/// be before the expiry recorded in the current persisted projection. Checking all allows keeps a
/// caller from naming a conservative denial while smuggling a different late approval into the
/// same replacement.
pub(crate) fn validate_approval_resolution_deadline(
    current: &RunState,
    replacement: &RunState,
    approval_id: &str,
    now_unix_ms: u64,
) -> DurableStoreResult<()> {
    let current_approval = current
        .projection()
        .approvals
        .get(approval_id)
        .ok_or_else(|| {
            DurableStoreError::Invalid(format!(
                "atomic approval resolution references unknown approval '{approval_id}'"
            ))
        })?;
    if current_approval.status != crate::durability::DurableApprovalStatus::Pending
        || current_approval.expires_at_unix_ms.is_none()
    {
        return Err(DurableStoreError::Invalid(format!(
            "atomic approval resolution requires pending typed approval '{approval_id}'"
        )));
    }

    let appended = replacement
        .events()
        .get(current.events().len()..)
        .ok_or_else(|| {
            DurableStoreError::Invalid(
                "atomic approval resolution replacement truncated the event log".into(),
            )
        })?;
    let mut target_resolutions = 0usize;
    let mut target_expired = false;
    for event in appended {
        let RunEventKind::ApprovalResolved {
            approval_id: resolved_id,
            approved,
            ..
        } = &event.kind
        else {
            continue;
        };
        let persisted = current
            .projection()
            .approvals
            .get(resolved_id)
            .ok_or_else(|| {
                DurableStoreError::Invalid(format!(
                    "atomic approval resolution appended unknown approval '{resolved_id}'"
                ))
            })?;
        if persisted.status != crate::durability::DurableApprovalStatus::Pending {
            return Err(DurableStoreError::Invalid(format!(
                "atomic approval resolution appended non-pending approval '{resolved_id}'"
            )));
        }
        if resolved_id == approval_id {
            target_resolutions += 1;
        }
        if *approved {
            if resolved_id != approval_id {
                return Err(DurableStoreError::Invalid(format!(
                    "atomic approval resolution for '{approval_id}' also approved '{resolved_id}'"
                )));
            }
            let expires_at_unix_ms = persisted.expires_at_unix_ms.ok_or_else(|| {
                DurableStoreError::Invalid(format!(
                    "atomic approval resolution cannot allow untyped approval '{resolved_id}'"
                ))
            })?;
            if now_unix_ms >= expires_at_unix_ms {
                target_expired = true;
            }
        }
    }
    if target_resolutions != 1 {
        return Err(DurableStoreError::Invalid(format!(
            "atomic approval resolution must append exactly one resolution for '{approval_id}'"
        )));
    }
    let resolved = replacement
        .projection()
        .approvals
        .get(approval_id)
        .ok_or_else(|| {
            DurableStoreError::Invalid(format!(
                "atomic approval replacement lost approval '{approval_id}'"
            ))
        })?;
    if resolved.status == crate::durability::DurableApprovalStatus::Pending {
        return Err(DurableStoreError::Invalid(format!(
            "atomic approval replacement did not resolve approval '{approval_id}'"
        )));
    }
    if target_expired {
        return Err(DurableStoreError::ApprovalExpired {
            run_id: current.run_id().into(),
            approval_id: approval_id.into(),
            observed_at_unix_ms: now_unix_ms,
        });
    }
    Ok(())
}

/// Keep typed approval resolutions off ordinary CAS paths.
///
/// Without this rejection, a caller could build a replacement with an old process timestamp and
/// bypass [`DurableStore::compare_and_swap_approval_resolution`] entirely.
pub(crate) fn reject_unvalidated_approval_resolutions(
    current: &RunState,
    replacement: &RunState,
) -> DurableStoreResult<()> {
    let appended = replacement
        .events()
        .get(current.events().len()..)
        .ok_or_else(|| {
            DurableStoreError::Invalid(
                "approval resolution replacement truncated the event log".into(),
            )
        })?;
    for event in appended {
        let RunEventKind::ApprovalResolved { approval_id, .. } = &event.kind else {
            continue;
        };
        let approval = current
            .projection()
            .approvals
            .get(approval_id)
            .ok_or_else(|| {
                DurableStoreError::Invalid(format!(
                    "approval resolution appended unknown approval '{approval_id}'"
                ))
            })?;
        if approval.expires_at_unix_ms.is_some() {
            return Err(DurableStoreError::ApprovalResolutionGuardRequired {
                run_id: current.run_id().into(),
                approval_id: approval_id.clone(),
            });
        }
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

    fn worker_lease_clock_unix_ms(&self) -> DurableStoreResult<u64> {
        system_worker_lease_clock_unix_ms()
    }

    fn supports_atomic_approval_resolution(&self) -> bool {
        true
    }

    fn compare_and_swap(
        &self,
        expected_sequence: u64,
        replacement: &RunState,
    ) -> DurableStoreResult<()> {
        self.compare_and_swap_inner(expected_sequence, replacement, None, None)
    }

    fn compare_and_swap_fenced(
        &self,
        expected_sequence: u64,
        replacement: &RunState,
        authority: &DurableStoreLeaseAuthority,
    ) -> DurableStoreResult<()> {
        self.compare_and_swap_inner(expected_sequence, replacement, Some(authority), None)
    }

    fn compare_and_swap_approval_resolution(
        &self,
        expected_sequence: u64,
        replacement: &RunState,
        approval_id: &str,
        authority: Option<&DurableStoreLeaseAuthority>,
    ) -> DurableStoreResult<()> {
        self.compare_and_swap_inner(expected_sequence, replacement, authority, Some(approval_id))
    }
}

impl InMemoryDurableStore {
    fn compare_and_swap_inner(
        &self,
        expected_sequence: u64,
        replacement: &RunState,
        authority: Option<&DurableStoreLeaseAuthority>,
        approval_id: Option<&str>,
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
        let now_unix_ms = system_worker_lease_clock_unix_ms()?;
        validate_worker_lease_fence(current, replacement, authority, now_unix_ms)?;
        validate_append_only(current, replacement)?;
        match approval_id {
            Some(approval_id) => validate_approval_resolution_deadline(
                current,
                replacement,
                approval_id,
                now_unix_ms,
            )?,
            None => reject_unvalidated_approval_resolutions(current, replacement)?,
        }
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

    #[derive(Default)]
    struct LegacyCompatibleStore {
        inner: InMemoryDurableStore,
    }

    impl DurableStore for LegacyCompatibleStore {
        fn create(&self, state: &RunState) -> DurableStoreResult<()> {
            self.inner.create(state)
        }

        fn load(&self, run_id: &str) -> DurableStoreResult<RunState> {
            self.inner.load(run_id)
        }

        fn compare_and_swap(
            &self,
            expected_sequence: u64,
            replacement: &RunState,
        ) -> DurableStoreResult<()> {
            self.inner.compare_and_swap(expected_sequence, replacement)
        }
    }

    #[test]
    fn legacy_store_implementations_compile_but_new_lease_operations_fail_closed() {
        let store = LegacyCompatibleStore::default();
        let state = RunState::new("session-1", "legacy-store", DurabilityMode::Sync).unwrap();
        store.create(&state).unwrap();
        assert_eq!(
            store.worker_lease_clock_unix_ms().unwrap_err(),
            DurableStoreError::LeaseClockUnsupported
        );
        assert!(!store.supports_atomic_approval_resolution());
        let authority = DurableStoreLeaseAuthority::new("worker-a", "lease-a");
        assert_eq!(
            store
                .compare_and_swap_fenced(last_sequence(&state), &state, &authority)
                .unwrap_err(),
            DurableStoreError::LeaseFencingUnsupported {
                run_id: "legacy-store".into()
            }
        );
    }

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
    fn active_worker_lease_requires_matching_opaque_fence() {
        let store = InMemoryDurableStore::default();
        let initial = RunState::new("session-1", "leased-run", DurabilityMode::Sync).unwrap();
        store.create(&initial).unwrap();
        let now = store.worker_lease_clock_unix_ms().unwrap();
        let mut claimed = initial.clone();
        claimed
            .claim_worker_lease("worker-a", "lease-a", now, now + 60_000)
            .unwrap();
        store
            .compare_and_swap(last_sequence(&initial), &claimed)
            .unwrap();
        let expected = last_sequence(&claimed);

        let mut ordinary = claimed.clone();
        ordinary
            .replace_state("unfenced", json!({"forged": true}))
            .unwrap();
        assert!(matches!(
            store.compare_and_swap(expected, &ordinary),
            Err(DurableStoreError::WorkerLeaseRequired { ref owner_id, .. })
                if owner_id == "worker-a"
        ));
        assert_eq!(store.load("leased-run").unwrap(), claimed);

        let mut forged_release = claimed.clone();
        forged_release
            .release_worker_lease("worker-a", "lease-a", now + 1)
            .unwrap();
        assert!(matches!(
            store.compare_and_swap(expected, &forged_release),
            Err(DurableStoreError::WorkerLeaseRequired { .. })
        ));
        assert_eq!(store.load("leased-run").unwrap(), claimed);

        let wrong = DurableStoreLeaseAuthority::new("worker-a", "wrong-token");
        assert!(matches!(
            store.compare_and_swap_fenced(expected, &ordinary, &wrong),
            Err(DurableStoreError::WorkerLeaseConflict { .. })
        ));
        assert_eq!(store.load("leased-run").unwrap(), claimed);

        let authority = DurableStoreLeaseAuthority::new("worker-a", "lease-a");
        store
            .compare_and_swap_fenced(expected, &ordinary, &authority)
            .unwrap();
        assert_eq!(store.load("leased-run").unwrap(), ordinary);
    }

    #[test]
    fn initial_worker_claim_is_store_clock_bounded_and_atomic() {
        let store = InMemoryDurableStore::default();
        let initial = RunState::new("session-1", "initial-claim", DurabilityMode::Sync).unwrap();
        store.create(&initial).unwrap();
        let expected = last_sequence(&initial);
        let now = store.worker_lease_clock_unix_ms().unwrap();

        let mut future = initial.clone();
        future
            .claim_worker_lease(
                "worker-a",
                "future-lease",
                now + MAX_DURABLE_WORKER_LEASE_MS,
                now + MAX_DURABLE_WORKER_LEASE_MS + 1_000,
            )
            .unwrap();
        assert!(matches!(
            store.compare_and_swap(expected, &future),
            Err(DurableStoreError::InvalidWorkerLeaseClaim { .. })
        ));
        assert_eq!(store.load("initial-claim").unwrap(), initial);

        let mut bundled = initial.clone();
        bundled
            .claim_worker_lease("worker-a", "bundled-lease", now, now + 60_000)
            .unwrap();
        bundled
            .replace_state("bundled-write", json!({"forged": true}))
            .unwrap();
        assert!(matches!(
            store.compare_and_swap(expected, &bundled),
            Err(DurableStoreError::InvalidWorkerLeaseClaim { .. })
        ));
        assert_eq!(store.load("initial-claim").unwrap(), initial);
    }

    #[test]
    fn expired_worker_lease_allows_only_one_event_atomic_recovery_claim() {
        let store = InMemoryDurableStore::default();
        let mut expired =
            RunState::new("session-1", "expired-lease-run", DurabilityMode::Sync).unwrap();
        expired
            .claim_worker_lease("worker-a", "lease-a", 1, 2)
            .unwrap();
        store.create(&expired).unwrap();
        let expected = last_sequence(&expired);

        let mut ordinary = expired.clone();
        ordinary
            .replace_state("unfenced", json!({"forged": true}))
            .unwrap();
        assert!(matches!(
            store.compare_and_swap(expected, &ordinary),
            Err(DurableStoreError::WorkerLeaseRecoveryRequired { .. })
        ));

        let now = store.worker_lease_clock_unix_ms().unwrap();
        let mut recovered = expired.clone();
        recovered
            .claim_worker_lease("worker-b", "lease-b", now, now + 60_000)
            .unwrap();
        store.compare_and_swap(expected, &recovered).unwrap();
        assert_eq!(
            store
                .load("expired-lease-run")
                .unwrap()
                .worker_lease()
                .unwrap()
                .owner_id,
            "worker-b"
        );
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
        assert_eq!(
            store.compare_and_swap(revision, &restarted).unwrap_err(),
            DurableStoreError::ApprovalResolutionGuardRequired {
                run_id: "restart".into(),
                approval_id: approval_id.clone(),
            }
        );
        store
            .compare_and_swap_approval_resolution(revision, &restarted, &approval_id, None)
            .unwrap();

        let persisted = store.load("restart").unwrap();
        assert_eq!(persisted.policy_snapshot_hash(), Some(policy.hash()));
        assert_eq!(
            persisted.projection().approvals[&approval_id].status,
            DurableApprovalStatus::Rejected
        );
        assert!(persisted.projection().approvals[&approval_id].timed_out);
    }

    #[test]
    fn fenced_cas_cannot_bypass_the_atomic_typed_approval_deadline() {
        let store = InMemoryDurableStore::default();
        let now = store.worker_lease_clock_unix_ms().unwrap();
        let mut pending =
            RunState::new("session", "fenced-approval", DurabilityMode::Sync).unwrap();
        let approval_id = pending
            .request_typed_approval(DurableApprovalRequest {
                logical_key: "publish".into(),
                activity_id: None,
                kind: DurableApprovalKind::Confirmation,
                prompt: "Publish?".into(),
                payload: json!({"artifact": "report"}),
                policy_snapshot_hash: None,
                governance_binding: None,
                requested_at_unix_ms: now,
                expires_at_unix_ms: now + 60_000,
            })
            .unwrap();
        pending
            .claim_worker_lease("worker-a", "lease-a", now, now + 60_000)
            .unwrap();
        store.create(&pending).unwrap();

        let revision = last_sequence(&pending);
        let mut approved = pending.clone();
        approved
            .apply_command_at(
                RunCommand::Resume {
                    command_id: "approve".into(),
                    approvals: vec![ApprovalResolution {
                        approval_id: approval_id.clone(),
                        approved: true,
                        response: None,
                    }],
                },
                now,
            )
            .unwrap();
        let authority = DurableStoreLeaseAuthority::new("worker-a", "lease-a");

        assert_eq!(
            store
                .compare_and_swap_fenced(revision, &approved, &authority)
                .unwrap_err(),
            DurableStoreError::ApprovalResolutionGuardRequired {
                run_id: "fenced-approval".into(),
                approval_id: approval_id.clone(),
            }
        );
        store
            .compare_and_swap_approval_resolution(
                revision,
                &approved,
                &approval_id,
                Some(&authority),
            )
            .unwrap();
        assert_eq!(
            store
                .load("fenced-approval")
                .unwrap()
                .projection()
                .approvals[&approval_id]
                .status,
            DurableApprovalStatus::Approved
        );
    }

    #[test]
    fn expired_target_does_not_hide_another_approved_resolution() {
        let mut pending =
            RunState::new("session", "batched-approval", DurabilityMode::Sync).unwrap();
        let first_id = pending
            .request_typed_approval(DurableApprovalRequest {
                logical_key: "first".into(),
                activity_id: None,
                kind: DurableApprovalKind::Confirmation,
                prompt: "First?".into(),
                payload: json!({}),
                policy_snapshot_hash: None,
                governance_binding: None,
                requested_at_unix_ms: 10,
                expires_at_unix_ms: 20,
            })
            .unwrap();
        let second_id = pending
            .request_typed_approval(DurableApprovalRequest {
                logical_key: "second".into(),
                activity_id: None,
                kind: DurableApprovalKind::Confirmation,
                prompt: "Second?".into(),
                payload: json!({}),
                policy_snapshot_hash: None,
                governance_binding: None,
                requested_at_unix_ms: 10,
                expires_at_unix_ms: 30,
            })
            .unwrap();
        let mut batched = pending.clone();
        batched
            .apply_command_at(
                RunCommand::Resume {
                    command_id: "batched".into(),
                    approvals: vec![
                        ApprovalResolution {
                            approval_id: first_id.clone(),
                            approved: true,
                            response: None,
                        },
                        ApprovalResolution {
                            approval_id: second_id.clone(),
                            approved: true,
                            response: None,
                        },
                    ],
                },
                15,
            )
            .unwrap();

        assert!(matches!(
            validate_approval_resolution_deadline(&pending, &batched, &first_id, 25),
            Err(DurableStoreError::Invalid(ref message))
                if message.contains(&format!("also approved '{second_id}'"))
        ));
    }
}
