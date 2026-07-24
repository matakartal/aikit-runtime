//! PostgreSQL persistence for append-only durable runs.
//!
//! The adapter serializes access through one synchronous PostgreSQL client per instance while the
//! database row lock and revision comparison provide cross-process coordination. Production hosts
//! that require TLS should construct a configured [`postgres::Client`] and pass it to
//! [`PostgresDurableStore::from_client`].

#![cfg(feature = "postgres-store")]

use crate::durability::{
    is_supported_durability_schema_version, RunState, DURABILITY_SCHEMA_VERSION,
    MIN_SUPPORTED_DURABILITY_SCHEMA_VERSION,
};
use crate::durable_store::{
    reject_unvalidated_approval_resolutions, validate_append_only,
    validate_approval_resolution_deadline, validate_worker_lease_fence, DurableStore,
    DurableStoreError, DurableStoreLeaseAuthority, DurableStoreResult,
};
use postgres::{Client, GenericClient, NoTls};
use std::sync::{Mutex, MutexGuard};

const CREATE_DURABLE_RUNS_TABLE: &str = r#"
CREATE TABLE IF NOT EXISTS aikit_durable_runs (
    run_id TEXT PRIMARY KEY CHECK (char_length(run_id) > 0),
    revision BIGINT NOT NULL CHECK (revision >= 0),
    schema_version INTEGER NOT NULL CHECK (schema_version > 0),
    state_json TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
)
"#;

/// Transactional PostgreSQL implementation of [`DurableStore`].
pub struct PostgresDurableStore {
    client: Mutex<Client>,
}

impl PostgresDurableStore {
    /// Connect without transport encryption.
    ///
    /// This is intended for local development or an already-encrypted private transport. Use
    /// [`Self::from_client`] with a TLS-configured client for production networks.
    pub fn connect_no_tls(config: &str) -> DurableStoreResult<Self> {
        let client = Client::connect(config, NoTls).map_err(postgres_io)?;
        Self::from_client(client)
    }

    /// Build the store around a caller-configured PostgreSQL client and run the idempotent schema
    /// migration in a transaction.
    pub fn from_client(mut client: Client) -> DurableStoreResult<Self> {
        let mut transaction = client.transaction().map_err(postgres_io)?;
        transaction
            .batch_execute(CREATE_DURABLE_RUNS_TABLE)
            .map_err(postgres_io)?;
        transaction.commit().map_err(postgres_io)?;
        Ok(Self {
            client: Mutex::new(client),
        })
    }

    fn client(&self) -> DurableStoreResult<MutexGuard<'_, Client>> {
        self.client
            .lock()
            .map_err(|_| DurableStoreError::Io("PostgreSQL client mutex poisoned".into()))
    }

    #[cfg(test)]
    fn delete_run_for_test(&self, run_id: &str) -> DurableStoreResult<()> {
        self.client()?
            .execute("DELETE FROM aikit_durable_runs WHERE run_id=$1", &[&run_id])
            .map_err(postgres_io)?;
        Ok(())
    }
}

impl DurableStore for PostgresDurableStore {
    fn create(&self, state: &RunState) -> DurableStoreResult<()> {
        let revision = revision_to_postgres(last_sequence(state))?;
        let schema_version = schema_version_to_postgres(state.schema_version())?;
        let json = serialize_state(state)?;
        let mut client = self.client()?;
        let mut transaction = client.transaction().map_err(postgres_io)?;
        let inserted = transaction
            .execute(
                "INSERT INTO aikit_durable_runs(run_id,revision,schema_version,state_json) \
                 VALUES($1,$2,$3,$4) ON CONFLICT(run_id) DO NOTHING",
                &[&state.run_id(), &revision, &schema_version, &json],
            )
            .map_err(postgres_io)?;
        if inserted != 1 {
            transaction.rollback().map_err(postgres_io)?;
            return Err(DurableStoreError::AlreadyExists {
                run_id: state.run_id().into(),
            });
        }
        transaction.commit().map_err(postgres_io)
    }

    fn load(&self, run_id: &str) -> DurableStoreResult<RunState> {
        let row = self
            .client()?
            .query_opt(
                "SELECT revision,schema_version,state_json FROM aikit_durable_runs WHERE run_id=$1",
                &[&run_id],
            )
            .map_err(postgres_io)?
            .ok_or_else(|| DurableStoreError::NotFound {
                run_id: run_id.into(),
            })?;
        decode_row(
            run_id,
            row.get::<_, i64>(0),
            row.get::<_, i32>(1),
            row.get::<_, String>(2),
        )
    }

    fn worker_lease_clock_unix_ms(&self) -> DurableStoreResult<u64> {
        postgres_worker_lease_clock_unix_ms(&mut *self.client()?)
    }

    fn supports_atomic_approval_resolution(&self) -> bool {
        true
    }

    fn compare_and_swap(
        &self,
        expected_sequence: u64,
        replacement: &RunState,
    ) -> DurableStoreResult<()> {
        postgres_compare_and_swap(self, expected_sequence, replacement, None, None)
    }

    fn compare_and_swap_fenced(
        &self,
        expected_sequence: u64,
        replacement: &RunState,
        authority: &DurableStoreLeaseAuthority,
    ) -> DurableStoreResult<()> {
        postgres_compare_and_swap(self, expected_sequence, replacement, Some(authority), None)
    }

    fn compare_and_swap_approval_resolution(
        &self,
        expected_sequence: u64,
        replacement: &RunState,
        approval_id: &str,
        authority: Option<&DurableStoreLeaseAuthority>,
    ) -> DurableStoreResult<()> {
        postgres_compare_and_swap(
            self,
            expected_sequence,
            replacement,
            authority,
            Some(approval_id),
        )
    }
}

fn postgres_compare_and_swap(
    store: &PostgresDurableStore,
    expected_sequence: u64,
    replacement: &RunState,
    authority: Option<&DurableStoreLeaseAuthority>,
    approval_id: Option<&str>,
) -> DurableStoreResult<()> {
    let expected = revision_to_postgres(expected_sequence)?;
    let replacement_revision = revision_to_postgres(last_sequence(replacement))?;
    let replacement_schema = schema_version_to_postgres(replacement.schema_version())?;
    let replacement_json = serialize_state(replacement)?;
    let mut client = store.client()?;
    let mut transaction = client.transaction().map_err(postgres_io)?;

    // The row lock keeps the validation and conditional update in one serializable critical
    // section even when different AIKit processes race on the same durable run.
    let row = transaction
        .query_opt(
            "SELECT revision,schema_version,state_json FROM aikit_durable_runs \
                 WHERE run_id=$1 FOR UPDATE",
            &[&replacement.run_id()],
        )
        .map_err(postgres_io)?;
    let Some(row) = row else {
        transaction.rollback().map_err(postgres_io)?;
        return Err(DurableStoreError::NotFound {
            run_id: replacement.run_id().into(),
        });
    };
    let actual_sql = row.get::<_, i64>(0);
    let actual = revision_from_postgres(actual_sql)?;
    if actual != expected_sequence {
        transaction.rollback().map_err(postgres_io)?;
        return Err(DurableStoreError::Conflict {
            run_id: replacement.run_id().into(),
            expected: expected_sequence,
            actual,
        });
    }
    let current = decode_row(
        replacement.run_id(),
        actual_sql,
        row.get::<_, i32>(1),
        row.get::<_, String>(2),
    )?;
    let now_unix_ms = postgres_worker_lease_clock_unix_ms(&mut transaction)?;
    validate_worker_lease_fence(&current, replacement, authority, now_unix_ms)?;
    validate_append_only(&current, replacement)?;
    match approval_id {
        Some(approval_id) => {
            validate_approval_resolution_deadline(&current, replacement, approval_id, now_unix_ms)?
        }
        None => reject_unvalidated_approval_resolutions(&current, replacement)?,
    }

    let updated = transaction
        .execute(
            "UPDATE aikit_durable_runs \
                 SET revision=$1,schema_version=$2,state_json=$3,updated_at=CURRENT_TIMESTAMP \
                 WHERE run_id=$4 AND revision=$5",
            &[
                &replacement_revision,
                &replacement_schema,
                &replacement_json,
                &replacement.run_id(),
                &expected,
            ],
        )
        .map_err(postgres_io)?;
    if updated != 1 {
        transaction.rollback().map_err(postgres_io)?;
        return Err(DurableStoreError::Conflict {
            run_id: replacement.run_id().into(),
            expected: expected_sequence,
            actual,
        });
    }
    transaction.commit().map_err(postgres_io)
}

fn postgres_worker_lease_clock_unix_ms(client: &mut impl GenericClient) -> DurableStoreResult<u64> {
    let row = client
        .query_one(
            "SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint",
            &[],
        )
        .map_err(postgres_io)?;
    revision_from_postgres(row.get::<_, i64>(0))
}

fn decode_row(
    run_id: &str,
    revision: i64,
    schema_version: i32,
    json: String,
) -> DurableStoreResult<RunState> {
    let revision = revision_from_postgres(revision)?;
    let schema_version = u32::try_from(schema_version)
        .map_err(|_| DurableStoreError::Invalid("negative PostgreSQL schema version".into()))?;
    if !is_supported_durability_schema_version(schema_version) {
        return Err(DurableStoreError::Invalid(format!(
            "unsupported PostgreSQL durability schema {schema_version}; supported range is {MIN_SUPPORTED_DURABILITY_SCHEMA_VERSION}..={DURABILITY_SCHEMA_VERSION}"
        )));
    }
    let serialized: serde_json::Value = serde_json::from_str(&json)
        .map_err(|error| DurableStoreError::Invalid(error.to_string()))?;
    let serialized_schema_version = serialized
        .get("schema_version")
        .and_then(serde_json::Value::as_u64)
        .and_then(|value| u32::try_from(value).ok())
        .ok_or_else(|| {
            DurableStoreError::Invalid(
                "serialized durable run has an invalid schema version".into(),
            )
        })?;
    if serialized_schema_version != schema_version {
        return Err(DurableStoreError::Invalid(
            "PostgreSQL schema version does not match serialized run state".into(),
        ));
    }
    let state: RunState = serde_json::from_value(serialized)
        .map_err(|error| DurableStoreError::Invalid(error.to_string()))?;
    debug_assert_eq!(state.schema_version(), DURABILITY_SCHEMA_VERSION);
    if state.run_id() != run_id {
        return Err(DurableStoreError::Invalid(
            "PostgreSQL row key does not match serialized run ID".into(),
        ));
    }
    if last_sequence(&state) != revision {
        return Err(DurableStoreError::Invalid(
            "PostgreSQL revision does not match serialized event log".into(),
        ));
    }
    Ok(state)
}

fn serialize_state(state: &RunState) -> DurableStoreResult<String> {
    serde_json::to_string(state).map_err(|error| DurableStoreError::Invalid(error.to_string()))
}

fn last_sequence(state: &RunState) -> u64 {
    state.events().last().map_or(0, |event| event.sequence)
}

fn revision_to_postgres(revision: u64) -> DurableStoreResult<i64> {
    i64::try_from(revision).map_err(|_| {
        DurableStoreError::Invalid("durable revision exceeds PostgreSQL BIGINT".into())
    })
}

fn revision_from_postgres(revision: i64) -> DurableStoreResult<u64> {
    u64::try_from(revision)
        .map_err(|_| DurableStoreError::Invalid("negative durable revision in PostgreSQL".into()))
}

fn schema_version_to_postgres(schema_version: u32) -> DurableStoreResult<i32> {
    i32::try_from(schema_version).map_err(|_| {
        DurableStoreError::Invalid("durability schema version exceeds PostgreSQL INTEGER".into())
    })
}

fn postgres_io(error: postgres::Error) -> DurableStoreError {
    DurableStoreError::Io(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::durability::{DurabilityMode, RunEvent, RunState};
    use serde_json::json;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn append_only_validation_accepts_extension_and_rejects_divergence() {
        let initial = RunState::new("session", "run", DurabilityMode::Sync).unwrap();
        let mut extension = initial.clone();
        extension
            .replace_state("winner", json!({"worker": "first"}))
            .unwrap();
        validate_append_only(&initial, &extension).unwrap();

        let mut divergent = initial.clone();
        divergent
            .replace_state("loser", json!({"worker": "second"}))
            .unwrap();
        assert!(matches!(
            validate_append_only(&extension, &divergent),
            Err(DurableStoreError::Invalid(_))
        ));
    }

    #[test]
    fn postgres_revision_conversion_fails_closed() {
        assert_eq!(
            revision_from_postgres(-1).unwrap_err(),
            DurableStoreError::Invalid("negative durable revision in PostgreSQL".into())
        );
        assert_eq!(
            revision_to_postgres(u64::MAX).unwrap_err(),
            DurableStoreError::Invalid("durable revision exceeds PostgreSQL BIGINT".into())
        );
    }

    #[test]
    fn postgres_decode_migrates_v1_row_metadata_and_snapshot() {
        let initial = RunState::new("legacy-session", "legacy-run", DurabilityMode::Sync).unwrap();
        let mut legacy = serde_json::to_value(&initial).unwrap();
        legacy["schema_version"] = json!(1);
        for event in legacy["events"].as_array_mut().unwrap() {
            event["schema_version"] = json!(1);
        }

        let migrated = decode_row(
            initial.run_id(),
            1,
            1,
            serde_json::to_string(&legacy).unwrap(),
        )
        .unwrap();
        assert_eq!(migrated.schema_version(), DURABILITY_SCHEMA_VERSION);
        assert!(migrated
            .events()
            .iter()
            .all(|event| event.schema_version == 1));
    }

    #[test]
    #[ignore = "requires AIKIT_TEST_POSTGRES_URL pointing to a disposable PostgreSQL database"]
    fn postgres_store_enforces_cross_connection_cas() {
        let url = std::env::var("AIKIT_TEST_POSTGRES_URL")
            .expect("set AIKIT_TEST_POSTGRES_URL for the ignored PostgreSQL integration test");
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let run_id = format!("postgres-test-{}-{unique}", std::process::id());
        let first = PostgresDurableStore::connect_no_tls(&url).unwrap();
        let second = PostgresDurableStore::connect_no_tls(&url).unwrap();
        let initial =
            RunState::new("postgres-test-session", &run_id, DurabilityMode::Sync).unwrap();
        first.create(&initial).unwrap();
        let expected = last_sequence(&initial);

        let mut winner = initial.clone();
        winner
            .replace_state("winner", json!({"worker": "first"}))
            .unwrap();
        first.compare_and_swap(expected, &winner).unwrap();

        let mut stale = initial;
        stale
            .replace_state("stale", json!({"worker": "second"}))
            .unwrap();
        assert!(matches!(
            second.compare_and_swap(expected, &stale),
            Err(DurableStoreError::Conflict { .. })
        ));
        assert_eq!(second.load(&run_id).unwrap(), winner);

        let now = first.worker_lease_clock_unix_ms().unwrap();
        let mut claimed = winner.clone();
        claimed
            .claim_worker_lease("worker-a", "lease-a", now, now + 60_000)
            .unwrap();
        first
            .compare_and_swap(last_sequence(&winner), &claimed)
            .unwrap();
        let claimed_revision = last_sequence(&claimed);
        let mut ordinary = claimed.clone();
        ordinary
            .replace_state("unfenced", json!({"forged": true}))
            .unwrap();
        assert!(matches!(
            first.compare_and_swap(claimed_revision, &ordinary),
            Err(DurableStoreError::WorkerLeaseRequired { .. })
        ));
        assert_eq!(first.load(&run_id).unwrap(), claimed);

        let wrong = DurableStoreLeaseAuthority::new("worker-a", "wrong-token");
        assert!(matches!(
            first.compare_and_swap_fenced(claimed_revision, &ordinary, &wrong),
            Err(DurableStoreError::WorkerLeaseConflict { .. })
        ));
        let authority = DurableStoreLeaseAuthority::new("worker-a", "lease-a");
        first
            .compare_and_swap_fenced(claimed_revision, &ordinary, &authority)
            .unwrap();
        assert_eq!(first.load(&run_id).unwrap(), ordinary);
        first.delete_run_for_test(&run_id).unwrap();
    }

    #[test]
    fn run_event_type_remains_send_sync_for_store_threads() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<RunEvent>();
    }
}
