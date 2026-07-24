//! Typed, single-writer persistence contract for incremental A2A mapper mutations.
//!
//! This module is intentionally additive: the current A2A mapper and transport do not yet emit or
//! replay these deltas. It defines the durable ordering, checkpoint, bounded-restore, and safe-GC
//! boundary needed to replace full-snapshot writes without introducing per-tenant forks.

use super::a2a::{
    A2aCancellationOutboxRecord, A2aDispatchOutboxRecord, A2aMapper, A2aMessageReceipt,
    A2aPendingEventIntent, A2aTaskRecord,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use tokio::sync::Mutex;

/// Current on-disk/wire format for mapper deltas and journal metadata.
pub const A2A_MAPPER_DELTA_FORMAT_VERSION: u32 = 1;
/// One delta is deliberately small; large mapper state belongs in an occasional checkpoint.
pub const A2A_MAPPER_DELTA_MAX_BYTES: usize = 256 * 1024;
/// Prevent a single mutation from hiding an unbounded operation list inside the byte ceiling.
pub const A2A_MAPPER_DELTA_MAX_OPERATIONS: usize = 1_024;
/// Checkpoints are bounded independently from hot-path deltas.
pub const A2A_MAPPER_CHECKPOINT_MAX_BYTES: usize = 32 * 1024 * 1024;
/// Maximum number of deltas returned by one restore call.
pub const A2A_MAPPER_JOURNAL_MAX_PAGE_DELTAS: u16 = 128;
/// Maximum serialized bytes returned by one restore call.
pub const A2A_MAPPER_JOURNAL_MAX_PAGE_BYTES: u32 = 8 * 1024 * 1024;

const A2A_MAPPER_JOURNAL_MIN_PAGE_BYTES: u32 = A2A_MAPPER_DELTA_MAX_BYTES as u32;
const A2A_MAPPER_JOURNAL_ID_MAX_BYTES: usize = 512;

/// Position in the one global A2A mutation stream.
///
/// `sequence` orders journal records. `revision` is the mapper revision after that record. They are
/// both present because a bootstrap checkpoint may import an existing mapper at sequence zero.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct A2aJournalToken {
    sequence: u64,
    revision: u64,
    delta_hash: String,
}

impl A2aJournalToken {
    /// Empty-journal position for a newly-created mapper.
    pub fn genesis() -> Self {
        Self {
            sequence: 0,
            revision: 0,
            delta_hash: journal_genesis_hash(),
        }
    }

    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    pub fn revision(&self) -> u64 {
        self.revision
    }

    pub fn delta_hash(&self) -> &str {
        &self.delta_hash
    }

    /// Construct a token loaded by an external journal implementation.
    pub fn new(
        sequence: u64,
        revision: u64,
        delta_hash: impl Into<String>,
    ) -> A2aJournalResult<Self> {
        let token = Self {
            sequence,
            revision,
            delta_hash: delta_hash.into(),
        };
        token.validate()?;
        Ok(token)
    }

    fn validate(&self) -> A2aJournalResult<()> {
        validate_hash("journal token hash", &self.delta_hash)?;
        if self.sequence > self.revision {
            return Err(A2aJournalError::Invalid {
                reason: "journal sequence cannot exceed mapper revision".into(),
            });
        }
        Ok(())
    }
}

/// Typed changes that can reproduce mapper state without serializing the entire mapper.
///
/// Storage keys are explicit because mapper indexes are owner-scoped. Upserts replace one exact
/// key; removals delete one exact key. Operations are applied in vector order by a future mapper
/// integration, but the containing delta remains the atomic durability unit.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum A2aMapperOperation {
    PutContext {
        storage_key: String,
        context_id: String,
        session_id: String,
        owner_subject: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        owner_tenant_id: Option<String>,
    },
    RemoveContext {
        storage_key: String,
    },
    PutTask {
        storage_key: String,
        task: A2aTaskRecord,
    },
    RemoveTask {
        storage_key: String,
    },
    PutReceipt {
        storage_key: String,
        receipt: A2aMessageReceipt,
    },
    RemoveReceipt {
        storage_key: String,
    },
    PutDispatch {
        storage_key: String,
        dispatch: Box<A2aDispatchOutboxRecord>,
    },
    RemoveDispatch {
        storage_key: String,
    },
    PutCancellation {
        storage_key: String,
        cancellation: A2aCancellationOutboxRecord,
    },
    RemoveCancellation {
        storage_key: String,
    },
    PutPendingEvent {
        storage_key: String,
        event: A2aPendingEventIntent,
    },
    RemovePendingEvent {
        storage_key: String,
    },
    SetNextSequence {
        next_sequence: u64,
    },
}

impl A2aMapperOperation {
    fn validate(&self) -> A2aJournalResult<()> {
        match self {
            Self::PutContext {
                storage_key,
                context_id,
                session_id,
                owner_subject,
                owner_tenant_id,
            } => {
                validate_id("context storage key", storage_key)?;
                validate_id("context id", context_id)?;
                validate_id("session id", session_id)?;
                validate_id("owner subject", owner_subject)?;
                if let Some(tenant_id) = owner_tenant_id {
                    validate_id("owner tenant id", tenant_id)?;
                }
                Ok(())
            }
            Self::RemoveContext { storage_key }
            | Self::RemoveTask { storage_key }
            | Self::RemoveReceipt { storage_key }
            | Self::RemoveDispatch { storage_key }
            | Self::RemoveCancellation { storage_key }
            | Self::RemovePendingEvent { storage_key } => {
                validate_id("mapper storage key", storage_key)
            }
            Self::PutTask { storage_key, task } => {
                validate_id("task storage key", storage_key)?;
                validate_id("task id", &task.mapping.task_id)
            }
            Self::PutReceipt {
                storage_key,
                receipt,
            } => {
                validate_id("receipt storage key", storage_key)?;
                receipt
                    .message
                    .validate()
                    .map_err(|error| A2aJournalError::Invalid {
                        reason: format!("invalid receipt message: {error}"),
                    })
            }
            Self::PutDispatch {
                storage_key,
                dispatch,
            } => {
                validate_id("dispatch storage key", storage_key)?;
                validate_id("dispatch id", &dispatch.dispatch_id)
            }
            Self::PutCancellation {
                storage_key,
                cancellation,
            } => {
                validate_id("cancellation storage key", storage_key)?;
                validate_id("cancellation id", &cancellation.cancellation_id)
            }
            Self::PutPendingEvent { storage_key, event } => {
                validate_id("pending-event storage key", storage_key)?;
                validate_id("event id", &event.event_id)
            }
            Self::SetNextSequence { .. } => Ok(()),
        }
    }
}

/// One atomic mapper mutation in the single global journal.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct A2aMapperDelta {
    format_version: u32,
    mutation_id: String,
    expected_token: A2aJournalToken,
    from_revision: u64,
    to_revision: u64,
    previous_hash: String,
    operations: Vec<A2aMapperOperation>,
    delta_hash: String,
}

impl A2aMapperDelta {
    pub fn new(
        mutation_id: impl Into<String>,
        expected_token: A2aJournalToken,
        to_revision: u64,
        operations: Vec<A2aMapperOperation>,
    ) -> A2aJournalResult<Self> {
        let previous_hash = expected_token.delta_hash.clone();
        let mut delta = Self {
            format_version: A2A_MAPPER_DELTA_FORMAT_VERSION,
            mutation_id: mutation_id.into(),
            from_revision: expected_token.revision,
            expected_token,
            to_revision,
            previous_hash,
            operations,
            delta_hash: String::new(),
        };
        delta.delta_hash = delta.compute_hash()?;
        delta.validate()?;
        Ok(delta)
    }

    pub fn format_version(&self) -> u32 {
        self.format_version
    }

    pub fn mutation_id(&self) -> &str {
        &self.mutation_id
    }

    pub fn expected_token(&self) -> &A2aJournalToken {
        &self.expected_token
    }

    pub fn from_revision(&self) -> u64 {
        self.from_revision
    }

    pub fn to_revision(&self) -> u64 {
        self.to_revision
    }

    /// Final mapper revision; retained as a concise compatibility accessor.
    pub fn revision(&self) -> u64 {
        self.to_revision
    }

    pub fn revision_delta(&self) -> u64 {
        self.to_revision - self.from_revision
    }

    pub fn previous_hash(&self) -> &str {
        &self.previous_hash
    }

    pub fn operations(&self) -> &[A2aMapperOperation] {
        &self.operations
    }

    pub fn delta_hash(&self) -> &str {
        &self.delta_hash
    }

    pub fn token(&self) -> A2aJournalResult<A2aJournalToken> {
        Ok(A2aJournalToken {
            sequence: self.expected_token.sequence.checked_add(1).ok_or_else(|| {
                A2aJournalError::Invalid {
                    reason: "journal sequence overflow".into(),
                }
            })?,
            revision: self.to_revision,
            delta_hash: self.delta_hash.clone(),
        })
    }

    pub fn validate(&self) -> A2aJournalResult<()> {
        if self.format_version != A2A_MAPPER_DELTA_FORMAT_VERSION {
            return Err(A2aJournalError::UnsupportedFormat {
                found: self.format_version,
            });
        }
        validate_id("mutation id", &self.mutation_id)?;
        self.expected_token.validate()?;
        if self.previous_hash != self.expected_token.delta_hash {
            return Err(A2aJournalError::Corruption {
                reason: "delta previous_hash does not match its expected token".into(),
            });
        }
        if self.from_revision != self.expected_token.revision {
            return Err(A2aJournalError::Corruption {
                reason: "delta from_revision does not match its expected token".into(),
            });
        }
        if self.to_revision <= self.from_revision {
            return Err(A2aJournalError::Invalid {
                reason: format!(
                    "delta to_revision {} must be greater than from_revision {}",
                    self.to_revision, self.from_revision
                ),
            });
        }
        if self.operations.is_empty() || self.operations.len() > A2A_MAPPER_DELTA_MAX_OPERATIONS {
            return Err(A2aJournalError::Invalid {
                reason: format!(
                    "delta operations must contain between 1 and {A2A_MAPPER_DELTA_MAX_OPERATIONS} entries"
                ),
            });
        }
        for operation in &self.operations {
            operation.validate()?;
        }
        validate_hash("delta hash", &self.delta_hash)?;
        if self.compute_hash()? != self.delta_hash {
            return Err(A2aJournalError::Corruption {
                reason: "delta content does not match delta_hash".into(),
            });
        }
        let encoded = serde_json::to_vec(self).map_err(serialization_error)?;
        if encoded.len() > A2A_MAPPER_DELTA_MAX_BYTES {
            return Err(A2aJournalError::LimitExceeded {
                reason: format!(
                    "delta is {} bytes; maximum is {A2A_MAPPER_DELTA_MAX_BYTES}",
                    encoded.len()
                ),
            });
        }
        Ok(())
    }

    fn compute_hash(&self) -> A2aJournalResult<String> {
        #[derive(Serialize)]
        struct HashMaterial<'a> {
            domain: &'static str,
            format_version: u32,
            mutation_id: &'a str,
            expected_token: &'a A2aJournalToken,
            from_revision: u64,
            to_revision: u64,
            previous_hash: &'a str,
            operations: &'a [A2aMapperOperation],
        }

        stable_hash(&HashMaterial {
            domain: "aikit:a2a:mapper-delta:v1",
            format_version: self.format_version,
            mutation_id: &self.mutation_id,
            expected_token: &self.expected_token,
            from_revision: self.from_revision,
            to_revision: self.to_revision,
            previous_hash: &self.previous_hash,
            operations: &self.operations,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct A2aMapperCheckpointMetadata {
    checkpoint_id: String,
    high_water: A2aJournalToken,
    mapper_schema_version: u32,
    state_hash: String,
    state_bytes: u64,
}

impl A2aMapperCheckpointMetadata {
    pub fn checkpoint_id(&self) -> &str {
        &self.checkpoint_id
    }

    pub fn high_water(&self) -> &A2aJournalToken {
        &self.high_water
    }

    pub fn mapper_schema_version(&self) -> u32 {
        self.mapper_schema_version
    }

    pub fn state_hash(&self) -> &str {
        &self.state_hash
    }

    pub fn state_bytes(&self) -> u64 {
        self.state_bytes
    }
}

/// Immutable mapper checkpoint. The checkpoint ID binds its high-water token and state hash.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct A2aMapperCheckpoint {
    format_version: u32,
    metadata: A2aMapperCheckpointMetadata,
    state: Vec<u8>,
}

impl A2aMapperCheckpoint {
    fn from_validated_mapper_state(
        high_water: A2aJournalToken,
        mapper_schema_version: u32,
        state: Vec<u8>,
    ) -> A2aJournalResult<Self> {
        high_water.validate()?;
        if mapper_schema_version == 0 {
            return Err(A2aJournalError::Invalid {
                reason: "mapper schema version must be non-zero".into(),
            });
        }
        if state.is_empty() || state.len() > A2A_MAPPER_CHECKPOINT_MAX_BYTES {
            return Err(A2aJournalError::LimitExceeded {
                reason: format!(
                    "checkpoint state must contain 1..={A2A_MAPPER_CHECKPOINT_MAX_BYTES} bytes"
                ),
            });
        }
        let state_hash = hash_bytes(&state);
        let checkpoint_id = stable_hash(&CheckpointIdMaterial {
            domain: "aikit:a2a:mapper-checkpoint:v1",
            high_water: &high_water,
            mapper_schema_version,
            state_hash: &state_hash,
        })?;
        let checkpoint = Self {
            format_version: A2A_MAPPER_DELTA_FORMAT_VERSION,
            metadata: A2aMapperCheckpointMetadata {
                checkpoint_id,
                high_water,
                mapper_schema_version,
                state_hash,
                state_bytes: state.len() as u64,
            },
            state,
        };
        checkpoint.validate_integrity()?;
        Ok(checkpoint)
    }

    /// Serialize a validated mapper checkpoint at an existing journal high-water token.
    pub fn from_mapper(high_water: A2aJournalToken, mapper: &A2aMapper) -> A2aJournalResult<Self> {
        if mapper.revision() != high_water.revision {
            return Err(A2aJournalError::Invalid {
                reason: "checkpoint mapper revision does not match high-water token".into(),
            });
        }
        let state = serde_json::to_vec(mapper).map_err(serialization_error)?;
        Self::from_validated_mapper_state(high_water, mapper.schema_version(), state)
    }

    /// Create a sequence-zero checkpoint for importing an existing snapshot into an empty journal.
    pub fn bootstrap_from_mapper(mapper: &A2aMapper) -> A2aJournalResult<Self> {
        let state = serde_json::to_vec(mapper).map_err(serialization_error)?;
        if state.is_empty() || state.len() > A2A_MAPPER_CHECKPOINT_MAX_BYTES {
            return Err(A2aJournalError::LimitExceeded {
                reason: format!(
                    "checkpoint state must contain 1..={A2A_MAPPER_CHECKPOINT_MAX_BYTES} bytes"
                ),
            });
        }
        let state_hash = hash_bytes(&state);
        let high_water = bootstrap_token(mapper.revision(), &state_hash)?;
        Self::from_validated_mapper_state(high_water, mapper.schema_version(), state)
    }

    pub fn metadata(&self) -> &A2aMapperCheckpointMetadata {
        &self.metadata
    }

    pub fn state(&self) -> &[u8] {
        &self.state
    }

    pub fn decode_mapper(&self) -> A2aJournalResult<A2aMapper> {
        self.validate_integrity()?;
        let mapper: A2aMapper = serde_json::from_slice(&self.state).map_err(serialization_error)?;
        if mapper.schema_version() != self.metadata.mapper_schema_version
            || mapper.revision() != self.metadata.high_water.revision
        {
            return Err(A2aJournalError::Corruption {
                reason: "checkpoint mapper metadata does not match its decoded state".into(),
            });
        }
        Ok(mapper)
    }

    pub fn validate(&self) -> A2aJournalResult<()> {
        self.validate_integrity()?;
        self.decode_mapper().map(|_| ())
    }

    fn validate_integrity(&self) -> A2aJournalResult<()> {
        if self.format_version != A2A_MAPPER_DELTA_FORMAT_VERSION {
            return Err(A2aJournalError::UnsupportedFormat {
                found: self.format_version,
            });
        }
        validate_id("checkpoint id", &self.metadata.checkpoint_id)?;
        self.metadata.high_water.validate()?;
        if self.metadata.mapper_schema_version == 0
            || self.state.is_empty()
            || self.state.len() > A2A_MAPPER_CHECKPOINT_MAX_BYTES
            || self.metadata.state_bytes != self.state.len() as u64
        {
            return Err(A2aJournalError::Corruption {
                reason: "checkpoint metadata has invalid size or schema version".into(),
            });
        }
        let state_hash = hash_bytes(&self.state);
        if state_hash != self.metadata.state_hash {
            return Err(A2aJournalError::Corruption {
                reason: "checkpoint state does not match state_hash".into(),
            });
        }
        let expected_id = stable_hash(&CheckpointIdMaterial {
            domain: "aikit:a2a:mapper-checkpoint:v1",
            high_water: &self.metadata.high_water,
            mapper_schema_version: self.metadata.mapper_schema_version,
            state_hash: &self.metadata.state_hash,
        })?;
        if expected_id != self.metadata.checkpoint_id {
            return Err(A2aJournalError::Corruption {
                reason: "checkpoint metadata does not match checkpoint_id".into(),
            });
        }
        Ok(())
    }
}

#[derive(Serialize)]
struct CheckpointIdMaterial<'a> {
    domain: &'static str,
    high_water: &'a A2aJournalToken,
    mapper_schema_version: u32,
    state_hash: &'a str,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct A2aJournalManifestToken {
    generation: u64,
    manifest_hash: String,
}

impl A2aJournalManifestToken {
    pub fn new(generation: u64, manifest_hash: impl Into<String>) -> A2aJournalResult<Self> {
        let token = Self {
            generation,
            manifest_hash: manifest_hash.into(),
        };
        validate_hash("manifest token hash", &token.manifest_hash)?;
        Ok(token)
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn manifest_hash(&self) -> &str {
        &self.manifest_hash
    }
}

/// Compact exact-id receipt retained after the corresponding delta payload is garbage-collected.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct A2aCompactedMutationTombstone {
    mutation_id: String,
    delta_hash: String,
    token: A2aJournalToken,
}

impl A2aCompactedMutationTombstone {
    pub fn mutation_id(&self) -> &str {
        &self.mutation_id
    }

    pub fn delta_hash(&self) -> &str {
        &self.delta_hash
    }

    pub fn token(&self) -> &A2aJournalToken {
        &self.token
    }

    fn from_delta(delta: &A2aMapperDelta) -> A2aJournalResult<Self> {
        Ok(Self {
            mutation_id: delta.mutation_id.clone(),
            delta_hash: delta.delta_hash.clone(),
            token: delta.token()?,
        })
    }
}

/// Atomic journal view. Its head and active checkpoint are changed under one store transaction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct A2aJournalManifest {
    format_version: u32,
    generation: u64,
    head: A2aJournalToken,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    active_checkpoint: Option<A2aMapperCheckpointMetadata>,
    retained_from_sequence: u64,
    compacted_mutation_count: u64,
    compacted_mutations_hash: String,
    manifest_hash: String,
}

impl A2aJournalManifest {
    fn empty() -> A2aJournalResult<Self> {
        let compacted_mutations_hash = compacted_mutations_hash(&BTreeMap::new())?;
        let mut manifest = Self {
            format_version: A2A_MAPPER_DELTA_FORMAT_VERSION,
            generation: 0,
            head: A2aJournalToken::genesis(),
            active_checkpoint: None,
            retained_from_sequence: 1,
            compacted_mutation_count: 0,
            compacted_mutations_hash,
            manifest_hash: String::new(),
        };
        manifest.refresh_hash()?;
        Ok(manifest)
    }

    pub fn head(&self) -> &A2aJournalToken {
        &self.head
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn active_checkpoint(&self) -> Option<&A2aMapperCheckpointMetadata> {
        self.active_checkpoint.as_ref()
    }

    pub fn retained_from_sequence(&self) -> u64 {
        self.retained_from_sequence
    }

    pub fn compacted_mutation_count(&self) -> u64 {
        self.compacted_mutation_count
    }

    pub fn compacted_mutations_hash(&self) -> &str {
        &self.compacted_mutations_hash
    }

    /// Construct manifest metadata loaded by an external store. The implementation must also
    /// load the exact compacted tombstone set and verify its count/root before serving it.
    pub fn new(
        generation: u64,
        head: A2aJournalToken,
        active_checkpoint: Option<A2aMapperCheckpointMetadata>,
        retained_from_sequence: u64,
        compacted_mutation_count: u64,
        compacted_mutations_hash: impl Into<String>,
    ) -> A2aJournalResult<Self> {
        let mut manifest = Self {
            format_version: A2A_MAPPER_DELTA_FORMAT_VERSION,
            generation,
            head,
            active_checkpoint,
            retained_from_sequence,
            compacted_mutation_count,
            compacted_mutations_hash: compacted_mutations_hash.into(),
            manifest_hash: String::new(),
        };
        manifest.refresh_hash()?;
        manifest.validate()?;
        Ok(manifest)
    }

    pub fn token(&self) -> A2aJournalManifestToken {
        A2aJournalManifestToken {
            generation: self.generation,
            manifest_hash: self.manifest_hash.clone(),
        }
    }

    fn validate(&self) -> A2aJournalResult<()> {
        if self.format_version != A2A_MAPPER_DELTA_FORMAT_VERSION {
            return Err(A2aJournalError::UnsupportedFormat {
                found: self.format_version,
            });
        }
        self.head.validate()?;
        validate_hash("manifest hash", &self.manifest_hash)?;
        validate_hash(
            "compacted mutation tombstone hash",
            &self.compacted_mutations_hash,
        )?;
        if self.retained_from_sequence == 0
            || self.retained_from_sequence > self.head.sequence.saturating_add(1)
        {
            return Err(A2aJournalError::Corruption {
                reason: "manifest retained range is invalid".into(),
            });
        }
        if let Some(checkpoint) = &self.active_checkpoint {
            checkpoint.high_water.validate()?;
            if checkpoint.high_water.sequence > self.head.sequence
                || checkpoint.high_water.revision > self.head.revision
            {
                return Err(A2aJournalError::Corruption {
                    reason: "active checkpoint is ahead of the journal head".into(),
                });
            }
        }
        if self.compute_hash()? != self.manifest_hash {
            return Err(A2aJournalError::Corruption {
                reason: "manifest content does not match manifest_hash".into(),
            });
        }
        Ok(())
    }

    fn advance_generation(&mut self) -> A2aJournalResult<()> {
        self.generation =
            self.generation
                .checked_add(1)
                .ok_or_else(|| A2aJournalError::Invalid {
                    reason: "manifest generation overflow".into(),
                })?;
        self.refresh_hash()
    }

    fn refresh_hash(&mut self) -> A2aJournalResult<()> {
        self.manifest_hash = self.compute_hash()?;
        Ok(())
    }

    fn compute_hash(&self) -> A2aJournalResult<String> {
        #[derive(Serialize)]
        struct HashMaterial<'a> {
            domain: &'static str,
            format_version: u32,
            generation: u64,
            head: &'a A2aJournalToken,
            active_checkpoint: &'a Option<A2aMapperCheckpointMetadata>,
            retained_from_sequence: u64,
            compacted_mutation_count: u64,
            compacted_mutations_hash: &'a str,
        }

        stable_hash(&HashMaterial {
            domain: "aikit:a2a:mapper-journal-manifest:v1",
            format_version: self.format_version,
            generation: self.generation,
            head: &self.head,
            active_checkpoint: &self.active_checkpoint,
            retained_from_sequence: self.retained_from_sequence,
            compacted_mutation_count: self.compacted_mutation_count,
            compacted_mutations_hash: &self.compacted_mutations_hash,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum A2aJournalAppendOutcome {
    Applied,
    AlreadyApplied,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum A2aCheckpointInstallOutcome {
    Applied,
    AlreadyApplied,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum A2aJournalGcOutcome {
    Collected { removed_deltas: u64 },
    AlreadyCollected,
}

/// Bounded page request pinned to one restore session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct A2aJournalPageRequest {
    restore_id: String,
    after: A2aJournalToken,
    max_deltas: u16,
    max_bytes: u32,
}

impl A2aJournalPageRequest {
    pub fn new(
        restore_id: impl Into<String>,
        after: A2aJournalToken,
        max_deltas: u16,
        max_bytes: u32,
    ) -> A2aJournalResult<Self> {
        let request = Self {
            restore_id: restore_id.into(),
            after,
            max_deltas,
            max_bytes,
        };
        request.validate()?;
        Ok(request)
    }

    pub fn restore_id(&self) -> &str {
        &self.restore_id
    }

    pub fn after(&self) -> &A2aJournalToken {
        &self.after
    }

    pub fn max_deltas(&self) -> u16 {
        self.max_deltas
    }

    pub fn max_bytes(&self) -> u32 {
        self.max_bytes
    }

    fn validate(&self) -> A2aJournalResult<()> {
        validate_id("restore id", &self.restore_id)?;
        self.after.validate()?;
        if self.max_deltas == 0 || self.max_deltas > A2A_MAPPER_JOURNAL_MAX_PAGE_DELTAS {
            return Err(A2aJournalError::LimitExceeded {
                reason: format!(
                    "restore page delta limit must be 1..={A2A_MAPPER_JOURNAL_MAX_PAGE_DELTAS}"
                ),
            });
        }
        if !(A2A_MAPPER_JOURNAL_MIN_PAGE_BYTES..=A2A_MAPPER_JOURNAL_MAX_PAGE_BYTES)
            .contains(&self.max_bytes)
        {
            return Err(A2aJournalError::LimitExceeded {
                reason: format!(
                    "restore page byte limit must be {A2A_MAPPER_JOURNAL_MIN_PAGE_BYTES}..={A2A_MAPPER_JOURNAL_MAX_PAGE_BYTES}"
                ),
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct A2aJournalPage {
    deltas: Vec<A2aMapperDelta>,
    next_after: A2aJournalToken,
    through: A2aJournalToken,
    done: bool,
    encoded_bytes: u32,
}

impl A2aJournalPage {
    pub fn new(
        deltas: Vec<A2aMapperDelta>,
        next_after: A2aJournalToken,
        through: A2aJournalToken,
        done: bool,
        encoded_bytes: u32,
    ) -> A2aJournalResult<Self> {
        if deltas.len() > usize::from(A2A_MAPPER_JOURNAL_MAX_PAGE_DELTAS)
            || encoded_bytes > A2A_MAPPER_JOURNAL_MAX_PAGE_BYTES
            || next_after.sequence() > through.sequence()
            || done != (next_after == through)
        {
            return Err(A2aJournalError::Invalid {
                reason: "journal page metadata is inconsistent or exceeds its bounds".into(),
            });
        }
        for delta in &deltas {
            delta.validate()?;
        }
        Ok(Self {
            deltas,
            next_after,
            through,
            done,
            encoded_bytes,
        })
    }

    pub fn deltas(&self) -> &[A2aMapperDelta] {
        &self.deltas
    }

    pub fn next_after(&self) -> &A2aJournalToken {
        &self.next_after
    }

    pub fn through(&self) -> &A2aJournalToken {
        &self.through
    }

    pub fn is_done(&self) -> bool {
        self.done
    }

    pub fn encoded_bytes(&self) -> u32 {
        self.encoded_bytes
    }
}

/// Atomically captured checkpoint + journal-head pair. The store pins its required tail until the
/// caller invokes `finish_restore`, preventing a concurrent compactor from deleting needed pages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct A2aJournalRestorePoint {
    restore_id: String,
    manifest: A2aJournalManifest,
    checkpoint: Option<A2aMapperCheckpoint>,
    start: A2aJournalToken,
}

impl A2aJournalRestorePoint {
    pub fn new(
        restore_id: impl Into<String>,
        manifest: A2aJournalManifest,
        checkpoint: Option<A2aMapperCheckpoint>,
    ) -> A2aJournalResult<Self> {
        let restore_id = restore_id.into();
        validate_id("restore id", &restore_id)?;
        manifest.validate()?;
        if let Some(checkpoint) = &checkpoint {
            checkpoint.validate()?;
            if manifest.active_checkpoint() != Some(checkpoint.metadata()) {
                return Err(A2aJournalError::Corruption {
                    reason: "restore checkpoint does not match the captured manifest".into(),
                });
            }
        } else if manifest.active_checkpoint().is_some() {
            return Err(A2aJournalError::Corruption {
                reason: "restore point omitted the manifest's active checkpoint".into(),
            });
        }
        let start = checkpoint
            .as_ref()
            .map_or_else(A2aJournalToken::genesis, |checkpoint| {
                checkpoint.metadata.high_water.clone()
            });
        Ok(Self {
            restore_id,
            manifest,
            checkpoint,
            start,
        })
    }

    pub fn restore_id(&self) -> &str {
        &self.restore_id
    }

    pub fn manifest(&self) -> &A2aJournalManifest {
        &self.manifest
    }

    pub fn checkpoint(&self) -> Option<&A2aMapperCheckpoint> {
        self.checkpoint.as_ref()
    }

    pub fn start(&self) -> &A2aJournalToken {
        &self.start
    }
}

/// Exact active-checkpoint CAS. Appends may advance the head while a checkpoint is being built;
/// only another checkpoint install invalidates this expectation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct A2aCheckpointInstallRequest {
    expected_active: Option<A2aMapperCheckpointMetadata>,
    checkpoint: A2aMapperCheckpoint,
}

impl A2aCheckpointInstallRequest {
    pub fn new(
        expected_active: Option<A2aMapperCheckpointMetadata>,
        checkpoint: A2aMapperCheckpoint,
    ) -> Self {
        Self {
            expected_active,
            checkpoint,
        }
    }

    pub fn expected_active(&self) -> Option<&A2aMapperCheckpointMetadata> {
        self.expected_active.as_ref()
    }

    pub fn checkpoint(&self) -> &A2aMapperCheckpoint {
        &self.checkpoint
    }
}

/// GC authorization bound to one exact manifest and its active checkpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct A2aJournalGcRequest {
    expected_manifest: A2aJournalManifestToken,
    checkpoint: A2aMapperCheckpointMetadata,
}

impl A2aJournalGcRequest {
    pub fn new(
        expected_manifest: A2aJournalManifestToken,
        checkpoint: A2aMapperCheckpointMetadata,
    ) -> Self {
        Self {
            expected_manifest,
            checkpoint,
        }
    }

    pub fn from_manifest(manifest: &A2aJournalManifest) -> Option<Self> {
        Some(Self {
            expected_manifest: manifest.token(),
            checkpoint: manifest.active_checkpoint.clone()?,
        })
    }

    pub fn expected_manifest(&self) -> &A2aJournalManifestToken {
        &self.expected_manifest
    }

    pub fn checkpoint(&self) -> &A2aMapperCheckpointMetadata {
        &self.checkpoint
    }
}

pub type A2aJournalResult<T> = Result<T, A2aJournalError>;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum A2aJournalError {
    #[error("invalid A2A journal value: {reason}")]
    Invalid { reason: String },
    #[error("unsupported A2A journal format version {found}")]
    UnsupportedFormat { found: u32 },
    #[error("A2A journal limit exceeded: {reason}")]
    LimitExceeded { reason: String },
    #[error("A2A journal corruption: {reason}")]
    Corruption { reason: String },
    #[error("A2A journal head changed")]
    StaleHead {
        expected: A2aJournalToken,
        actual: A2aJournalToken,
    },
    #[error("A2A mutation id {mutation_id} was reused with different content")]
    MutationConflict { mutation_id: String },
    #[error("A2A active checkpoint changed")]
    StaleCheckpoint,
    #[error("A2A checkpoint id was reused with different content")]
    CheckpointConflict,
    #[error("A2A journal manifest changed")]
    StaleManifest,
    #[error("A2A journal history required by this request was compacted")]
    HistoryCompacted,
    #[error("A2A restore session was not found")]
    RestoreNotFound,
    #[error("A2A journal GC is blocked by an active restore session")]
    RestorePinned,
    /// A persistent implementation may have committed the write. Retry the exact same mutation,
    /// checkpoint, or GC request; the idempotency rules resolve the outcome safely.
    #[error("A2A journal write outcome is unknown: {operation}")]
    OutcomeUnknown { operation: String },
}

/// Durable, one-global-stream journal boundary. Implementations must linearize every method with
/// append/checkpoint/GC, retain mutation-id tombstones across GC, and never route by tenant.
#[async_trait]
pub trait A2aMapperJournalStore: Send + Sync {
    async fn manifest(&self) -> A2aJournalResult<A2aJournalManifest>;

    /// Linearizable head probe used to resolve [`A2aJournalError::OutcomeUnknown`].
    async fn lookup_head(&self) -> A2aJournalResult<A2aJournalToken>;

    /// Atomic CAS append. Exact duplicate mutation IDs are idempotent; different content is a
    /// permanent conflict even after the original delta was compacted.
    async fn append_delta(
        &self,
        delta: A2aMapperDelta,
    ) -> A2aJournalResult<A2aJournalAppendOutcome>;

    /// Atomically switch the active checkpoint without changing or truncating the current tail.
    async fn install_checkpoint(
        &self,
        request: A2aCheckpointInstallRequest,
    ) -> A2aJournalResult<A2aCheckpointInstallOutcome>;

    /// Capture a consistent checkpoint/head pair and pin its tail against GC.
    async fn begin_restore(&self) -> A2aJournalResult<A2aJournalRestorePoint>;

    /// Return a count- and byte-bounded, hash-verified page through the pinned restore head.
    async fn read_delta_page(
        &self,
        request: A2aJournalPageRequest,
    ) -> A2aJournalResult<A2aJournalPage>;

    /// Release a restore pin. This is idempotent so callers can safely retry cleanup.
    async fn finish_restore(&self, restore_id: &str) -> A2aJournalResult<()>;

    /// Delete only the active checkpoint's covered prefix. Implementations must preserve the tail,
    /// checkpoint bytes, mutation tombstones, and any prefix pinned by an active restore.
    async fn garbage_collect(
        &self,
        request: A2aJournalGcRequest,
    ) -> A2aJournalResult<A2aJournalGcOutcome>;
}

#[derive(Debug, Clone)]
struct StoredMutationIdentity {
    delta: A2aMapperDelta,
    token: A2aJournalToken,
}

#[derive(Debug, Clone)]
struct RestorePin {
    start: A2aJournalToken,
    through: A2aJournalToken,
}

#[derive(Debug)]
struct InMemoryJournalState {
    manifest: A2aJournalManifest,
    deltas: BTreeMap<u64, A2aMapperDelta>,
    mutation_identities: BTreeMap<String, StoredMutationIdentity>,
    compacted_mutations: BTreeMap<String, A2aCompactedMutationTombstone>,
    checkpoints: BTreeMap<String, A2aMapperCheckpoint>,
    restore_pins: BTreeMap<String, RestorePin>,
    next_restore_id: u64,
}

impl InMemoryJournalState {
    fn new() -> A2aJournalResult<Self> {
        Ok(Self {
            manifest: A2aJournalManifest::empty()?,
            deltas: BTreeMap::new(),
            mutation_identities: BTreeMap::new(),
            compacted_mutations: BTreeMap::new(),
            checkpoints: BTreeMap::new(),
            restore_pins: BTreeMap::new(),
            next_restore_id: 1,
        })
    }

    fn token_at(&self, sequence: u64) -> Option<A2aJournalToken> {
        if sequence == self.manifest.head.sequence {
            return Some(self.manifest.head.clone());
        }
        if sequence == 0 {
            if let Some(checkpoint) = &self.manifest.active_checkpoint {
                if checkpoint.high_water.sequence == 0 {
                    return Some(checkpoint.high_water.clone());
                }
            }
            return Some(A2aJournalToken::genesis());
        }
        self.deltas
            .get(&sequence)
            .and_then(|delta| delta.token().ok())
            .or_else(|| {
                self.manifest
                    .active_checkpoint
                    .as_ref()
                    .filter(|checkpoint| checkpoint.high_water.sequence == sequence)
                    .map(|checkpoint| checkpoint.high_water.clone())
            })
    }

    fn validate_head(&self) -> A2aJournalResult<()> {
        self.manifest.validate()?;
        if self.manifest.compacted_mutation_count != self.compacted_mutations.len() as u64
            || self.manifest.compacted_mutations_hash
                != compacted_mutations_hash(&self.compacted_mutations)?
        {
            return Err(A2aJournalError::Corruption {
                reason: "compacted mutation tombstones do not match the manifest root".into(),
            });
        }
        if self.manifest.head.sequence == 0 {
            if self.manifest.head != A2aJournalToken::genesis() {
                let checkpoint = self.manifest.active_checkpoint.as_ref().ok_or_else(|| {
                    A2aJournalError::Corruption {
                        reason: "non-genesis sequence-zero head has no checkpoint".into(),
                    }
                })?;
                if checkpoint.high_water != self.manifest.head {
                    return Err(A2aJournalError::Corruption {
                        reason: "bootstrap checkpoint does not match journal head".into(),
                    });
                }
            }
            return Ok(());
        }
        if self.manifest.head.sequence >= self.manifest.retained_from_sequence {
            let delta = self
                .deltas
                .get(&self.manifest.head.sequence)
                .ok_or_else(|| A2aJournalError::Corruption {
                    reason: "journal head delta is missing".into(),
                })?;
            delta.validate()?;
            if delta.token()? != self.manifest.head {
                return Err(A2aJournalError::Corruption {
                    reason: "journal head token does not match head delta".into(),
                });
            }
        }
        Ok(())
    }

    fn remove_unpinned_compacted_payloads(&mut self) -> u64 {
        let removable: Vec<u64> =
            self.deltas
                .keys()
                .copied()
                .filter(|sequence| *sequence < self.manifest.retained_from_sequence)
                .filter(|sequence| {
                    !self.restore_pins.values().any(|pin| {
                        *sequence > pin.start.sequence && *sequence <= pin.through.sequence
                    })
                })
                .collect();
        for sequence in &removable {
            self.deltas.remove(sequence);
        }
        removable.len() as u64
    }
}

/// Linearizable ephemeral implementation and executable conformance reference for persistent
/// stores. One mutex protects the single global stream; there is intentionally no tenant shard.
#[derive(Debug)]
pub struct InMemoryA2aMapperJournalStore {
    state: Mutex<InMemoryJournalState>,
}

impl InMemoryA2aMapperJournalStore {
    pub fn new() -> A2aJournalResult<Self> {
        Ok(Self {
            state: Mutex::new(InMemoryJournalState::new()?),
        })
    }
}

impl Default for InMemoryA2aMapperJournalStore {
    fn default() -> Self {
        Self::new().expect("static A2A journal genesis metadata is valid")
    }
}

#[async_trait]
impl A2aMapperJournalStore for InMemoryA2aMapperJournalStore {
    async fn manifest(&self) -> A2aJournalResult<A2aJournalManifest> {
        let state = self.state.lock().await;
        state.validate_head()?;
        Ok(state.manifest.clone())
    }

    async fn lookup_head(&self) -> A2aJournalResult<A2aJournalToken> {
        let state = self.state.lock().await;
        state.validate_head()?;
        Ok(state.manifest.head.clone())
    }

    async fn append_delta(
        &self,
        delta: A2aMapperDelta,
    ) -> A2aJournalResult<A2aJournalAppendOutcome> {
        delta.validate()?;
        let mut state = self.state.lock().await;
        state.validate_head()?;

        if let Some(existing) = state.mutation_identities.get(delta.mutation_id()) {
            return if existing.delta == delta && existing.token == delta.token()? {
                Ok(A2aJournalAppendOutcome::AlreadyApplied)
            } else {
                Err(A2aJournalError::MutationConflict {
                    mutation_id: delta.mutation_id.clone(),
                })
            };
        }
        if let Some(existing) = state.compacted_mutations.get(delta.mutation_id()) {
            return if existing.delta_hash == delta.delta_hash && existing.token == delta.token()? {
                Ok(A2aJournalAppendOutcome::AlreadyApplied)
            } else {
                Err(A2aJournalError::MutationConflict {
                    mutation_id: delta.mutation_id.clone(),
                })
            };
        }
        if delta.expected_token != state.manifest.head {
            return Err(A2aJournalError::StaleHead {
                expected: delta.expected_token.clone(),
                actual: state.manifest.head.clone(),
            });
        }
        let token = delta.token()?;
        if state.deltas.contains_key(&token.sequence) {
            return Err(A2aJournalError::Corruption {
                reason: "journal sequence already contains a different mutation".into(),
            });
        }
        state.deltas.insert(token.sequence, delta.clone());
        state.mutation_identities.insert(
            delta.mutation_id.clone(),
            StoredMutationIdentity {
                delta,
                token: token.clone(),
            },
        );
        state.manifest.head = token;
        state.manifest.advance_generation()?;
        Ok(A2aJournalAppendOutcome::Applied)
    }

    async fn install_checkpoint(
        &self,
        request: A2aCheckpointInstallRequest,
    ) -> A2aJournalResult<A2aCheckpointInstallOutcome> {
        request.checkpoint.validate()?;
        let mut state = self.state.lock().await;
        state.validate_head()?;
        let metadata = request.checkpoint.metadata.clone();

        if state.manifest.active_checkpoint.as_ref() == Some(&metadata) {
            let stored = state
                .checkpoints
                .get(metadata.checkpoint_id())
                .ok_or_else(|| A2aJournalError::Corruption {
                    reason: "active checkpoint bytes are missing".into(),
                })?;
            return if stored == &request.checkpoint {
                Ok(A2aCheckpointInstallOutcome::AlreadyApplied)
            } else {
                Err(A2aJournalError::CheckpointConflict)
            };
        }
        if let Some(stored) = state.checkpoints.get(metadata.checkpoint_id()) {
            return if stored == &request.checkpoint {
                Err(A2aJournalError::StaleCheckpoint)
            } else {
                Err(A2aJournalError::CheckpointConflict)
            };
        }
        if request.expected_active != state.manifest.active_checkpoint {
            return Err(A2aJournalError::StaleCheckpoint);
        }

        let is_bootstrap = metadata.high_water.sequence == 0
            && state.manifest.head == A2aJournalToken::genesis()
            && state.manifest.active_checkpoint.is_none()
            && state.deltas.is_empty();
        if is_bootstrap {
            let expected = bootstrap_token(metadata.high_water.revision, &metadata.state_hash)?;
            if metadata.high_water != expected {
                return Err(A2aJournalError::CheckpointConflict);
            }
            state.manifest.head = metadata.high_water.clone();
        } else {
            let known = state
                .token_at(metadata.high_water.sequence)
                .ok_or(A2aJournalError::HistoryCompacted)?;
            if known != metadata.high_water {
                return Err(A2aJournalError::CheckpointConflict);
            }
            if let Some(active) = &state.manifest.active_checkpoint {
                if metadata.high_water.sequence < active.high_water.sequence {
                    return Err(A2aJournalError::CheckpointConflict);
                }
                if metadata.high_water.sequence == active.high_water.sequence {
                    return Err(A2aJournalError::CheckpointConflict);
                }
            }
        }

        state
            .checkpoints
            .insert(metadata.checkpoint_id.clone(), request.checkpoint);
        state.manifest.active_checkpoint = Some(metadata);
        state.manifest.advance_generation()?;
        Ok(A2aCheckpointInstallOutcome::Applied)
    }

    async fn begin_restore(&self) -> A2aJournalResult<A2aJournalRestorePoint> {
        let mut state = self.state.lock().await;
        state.validate_head()?;
        let checkpoint = match &state.manifest.active_checkpoint {
            Some(metadata) => Some(
                state
                    .checkpoints
                    .get(metadata.checkpoint_id())
                    .cloned()
                    .ok_or_else(|| A2aJournalError::Corruption {
                        reason: "active checkpoint bytes are missing".into(),
                    })?,
            ),
            None => None,
        };
        if let Some(checkpoint) = &checkpoint {
            checkpoint.validate()?;
        }
        let start = checkpoint
            .as_ref()
            .map_or_else(A2aJournalToken::genesis, |checkpoint| {
                checkpoint.metadata.high_water.clone()
            });
        let restore_id = format!("a2a-restore-{}", state.next_restore_id);
        state.next_restore_id =
            state
                .next_restore_id
                .checked_add(1)
                .ok_or_else(|| A2aJournalError::Invalid {
                    reason: "restore id sequence overflow".into(),
                })?;
        let through = state.manifest.head.clone();
        state.restore_pins.insert(
            restore_id.clone(),
            RestorePin {
                start: start.clone(),
                through,
            },
        );
        Ok(A2aJournalRestorePoint {
            restore_id,
            manifest: state.manifest.clone(),
            checkpoint,
            start,
        })
    }

    async fn read_delta_page(
        &self,
        request: A2aJournalPageRequest,
    ) -> A2aJournalResult<A2aJournalPage> {
        request.validate()?;
        let state = self.state.lock().await;
        state.validate_head()?;
        let pin = state
            .restore_pins
            .get(&request.restore_id)
            .ok_or(A2aJournalError::RestoreNotFound)?;
        if request.after.sequence < pin.start.sequence
            || request.after.sequence > pin.through.sequence
        {
            return Err(A2aJournalError::Invalid {
                reason: "restore cursor is outside its pinned range".into(),
            });
        }
        let known_after = if request.after == pin.start {
            pin.start.clone()
        } else {
            state
                .token_at(request.after.sequence)
                .ok_or(A2aJournalError::HistoryCompacted)?
        };
        if known_after != request.after {
            return Err(A2aJournalError::Corruption {
                reason: "restore cursor token does not match journal history".into(),
            });
        }

        let through = pin.through.clone();
        if request.after.sequence > through.sequence {
            return Err(A2aJournalError::Invalid {
                reason: "restore cursor is ahead of its captured head".into(),
            });
        }

        let mut deltas = Vec::new();
        let mut encoded_bytes = 0_u32;
        let mut cursor = request.after.clone();
        for sequence in request.after.sequence.saturating_add(1)..=through.sequence {
            if deltas.len() >= usize::from(request.max_deltas) {
                break;
            }
            let delta = state
                .deltas
                .get(&sequence)
                .ok_or(A2aJournalError::HistoryCompacted)?;
            delta.validate()?;
            if delta.expected_token != cursor || delta.previous_hash != cursor.delta_hash {
                return Err(A2aJournalError::Corruption {
                    reason: format!("broken delta hash chain at sequence {sequence}"),
                });
            }
            let bytes = serde_json::to_vec(delta)
                .map_err(serialization_error)?
                .len() as u32;
            if encoded_bytes.saturating_add(bytes) > request.max_bytes {
                break;
            }
            encoded_bytes += bytes;
            cursor = delta.token()?;
            deltas.push(delta.clone());
        }
        let done = cursor == through;
        Ok(A2aJournalPage {
            deltas,
            next_after: cursor,
            through,
            done,
            encoded_bytes,
        })
    }

    async fn finish_restore(&self, restore_id: &str) -> A2aJournalResult<()> {
        validate_id("restore id", restore_id)?;
        let mut state = self.state.lock().await;
        state.restore_pins.remove(restore_id);
        state.remove_unpinned_compacted_payloads();
        Ok(())
    }

    async fn garbage_collect(
        &self,
        request: A2aJournalGcRequest,
    ) -> A2aJournalResult<A2aJournalGcOutcome> {
        let mut state = self.state.lock().await;
        state.validate_head()?;
        let active = state
            .manifest
            .active_checkpoint
            .clone()
            .ok_or(A2aJournalError::StaleCheckpoint)?;
        if active != request.checkpoint {
            return Err(A2aJournalError::StaleCheckpoint);
        }
        let next_retained = active.high_water.sequence.saturating_add(1);
        if state.manifest.retained_from_sequence >= next_retained {
            return Ok(A2aJournalGcOutcome::AlreadyCollected);
        }
        if state.manifest.token() != request.expected_manifest {
            return Err(A2aJournalError::StaleManifest);
        }
        let covered: Vec<u64> = state
            .deltas
            .range(..=active.high_water.sequence)
            .map(|(sequence, _)| *sequence)
            .collect();
        for sequence in &covered {
            let (tombstone, mutation_id) = {
                let delta =
                    state
                        .deltas
                        .get(sequence)
                        .ok_or_else(|| A2aJournalError::Corruption {
                            reason: "GC-covered delta disappeared during the atomic transaction"
                                .into(),
                        })?;
                (
                    A2aCompactedMutationTombstone::from_delta(delta)?,
                    delta.mutation_id().to_owned(),
                )
            };
            state
                .compacted_mutations
                .insert(tombstone.mutation_id.clone(), tombstone);
            state.mutation_identities.remove(&mutation_id);
        }
        state.manifest.retained_from_sequence = next_retained;
        state.manifest.compacted_mutation_count = state.compacted_mutations.len() as u64;
        state.manifest.compacted_mutations_hash =
            compacted_mutations_hash(&state.compacted_mutations)?;
        let removed_deltas = state.remove_unpinned_compacted_payloads();
        state.manifest.advance_generation()?;
        Ok(A2aJournalGcOutcome::Collected { removed_deltas })
    }
}

fn validate_id(field: &'static str, value: &str) -> A2aJournalResult<()> {
    if value.is_empty()
        || value.len() > A2A_MAPPER_JOURNAL_ID_MAX_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(A2aJournalError::Invalid {
            reason: format!("{field} is empty, oversized, or contains control characters"),
        });
    }
    Ok(())
}

fn validate_hash(field: &'static str, value: &str) -> A2aJournalResult<()> {
    let Some(hex) = value.strip_prefix("sha256:") else {
        return Err(A2aJournalError::Corruption {
            reason: format!("{field} is not a SHA-256 digest"),
        });
    };
    if hex.len() != 64 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(A2aJournalError::Corruption {
            reason: format!("{field} is not a SHA-256 digest"),
        });
    }
    Ok(())
}

fn stable_hash<T: Serialize>(value: &T) -> A2aJournalResult<String> {
    let value = serde_json::to_value(value).map_err(serialization_error)?;
    Ok(crate::durability::stable_input_hash(&value))
}

fn compacted_mutations_hash(
    tombstones: &BTreeMap<String, A2aCompactedMutationTombstone>,
) -> A2aJournalResult<String> {
    stable_hash(&serde_json::json!({
        "domain": "aikit:a2a:compacted-mutation-tombstones:v1",
        "tombstones": tombstones,
    }))
}

fn hash_bytes(bytes: &[u8]) -> String {
    use sha2::{Digest as _, Sha256};

    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity(71);
    encoded.push_str("sha256:");
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(encoded, "{byte:02x}");
    }
    encoded
}

fn journal_genesis_hash() -> String {
    crate::durability::stable_input_hash(&serde_json::json!({
        "domain": "aikit:a2a:mapper-journal-genesis:v1"
    }))
}

fn bootstrap_token(revision: u64, state_hash: &str) -> A2aJournalResult<A2aJournalToken> {
    Ok(A2aJournalToken {
        sequence: 0,
        revision,
        delta_hash: stable_hash(&serde_json::json!({
            "domain": "aikit:a2a:mapper-journal-bootstrap:v1",
            "revision": revision,
            "state_hash": state_hash,
        }))?,
    })
}

fn serialization_error(error: serde_json::Error) -> A2aJournalError {
    A2aJournalError::Invalid {
        reason: format!("journal serialization failed: {error}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocols::{A2aMessage, A2aPart, A2aRole, CorrelationIdentity, ProtocolPrincipal};

    fn operation(value: u64) -> A2aMapperOperation {
        A2aMapperOperation::SetNextSequence {
            next_sequence: value,
        }
    }

    fn delta(mutation_id: &str, expected: A2aJournalToken, value: u64) -> A2aMapperDelta {
        let revision = expected.revision() + 1;
        A2aMapperDelta::new(mutation_id, expected, revision, vec![operation(value)]).unwrap()
    }

    async fn append_chain(
        store: &InMemoryA2aMapperJournalStore,
        count: u64,
    ) -> Vec<A2aMapperDelta> {
        let mut expected = store.manifest().await.unwrap().head().clone();
        let mut deltas = Vec::new();
        for value in 1..=count {
            let to_revision = expected
                .revision()
                .checked_add(if expected.sequence() == 0 { 2 } else { 1 })
                .unwrap();
            let next = A2aMapperDelta::new(
                format!("mutation-{value}"),
                expected,
                to_revision,
                vec![operation(value)],
            )
            .unwrap();
            assert_eq!(
                store.append_delta(next.clone()).await.unwrap(),
                A2aJournalAppendOutcome::Applied
            );
            expected = next.token().unwrap();
            deltas.push(next);
        }
        deltas
    }

    fn mapper_at_revision(revision: u64) -> A2aMapper {
        assert!(revision == 0 || (2..=4).contains(&revision));
        let principal = ProtocolPrincipal::new("journal-owner", ["a2a:message:send"])
            .unwrap()
            .with_tenant("journal-tenant")
            .unwrap();
        let mut mapper = A2aMapper::new();
        if revision == 0 {
            return mapper;
        }

        mapper
            .prepare_send_message(
                A2aMessage {
                    message_id: "journal-checkpoint-seed".into(),
                    context_id: None,
                    task_id: None,
                    role: A2aRole::User,
                    parts: vec![A2aPart::Text {
                        text: "journal checkpoint fixture".into(),
                    }],
                    metadata: BTreeMap::new(),
                },
                CorrelationIdentity::new("journal-correlation", "journal-request").unwrap(),
                Some(&principal),
            )
            .into_authorized()
            .unwrap();
        let dispatch_id = mapper
            .dispatch_for_message("journal-checkpoint-seed", &principal)
            .unwrap()
            .dispatch_id
            .clone();
        if revision >= 3 {
            mapper.mark_dispatch_running(&dispatch_id).unwrap();
        }
        if revision >= 4 {
            mapper
                .mark_dispatch_reconcile_pending(&dispatch_id, "fixture")
                .unwrap();
        }
        assert_eq!(mapper.revision(), revision);
        mapper
    }

    fn checkpoint_at(token: A2aJournalToken, _marker: &str) -> A2aMapperCheckpoint {
        A2aMapperCheckpoint::from_mapper(token.clone(), &mapper_at_revision(token.revision()))
            .unwrap()
    }

    #[tokio::test]
    async fn append_is_exact_idempotent_but_rejects_same_id_with_new_content() {
        let store = InMemoryA2aMapperJournalStore::default();
        let first = delta("stable-id", A2aJournalToken::genesis(), 1);
        assert_eq!(
            store.append_delta(first.clone()).await.unwrap(),
            A2aJournalAppendOutcome::Applied
        );
        // Models an applied write whose response was lost: replaying the exact bytes settles it.
        assert_eq!(
            store.append_delta(first.clone()).await.unwrap(),
            A2aJournalAppendOutcome::AlreadyApplied
        );

        let mut conflicting = first;
        conflicting.operations = vec![operation(2)];
        conflicting.delta_hash = conflicting.compute_hash().unwrap();
        assert!(matches!(
            store.append_delta(conflicting).await,
            Err(A2aJournalError::MutationConflict { mutation_id }) if mutation_id == "stable-id"
        ));
    }

    #[tokio::test]
    async fn append_rejects_stale_head_without_mutating_the_stream() {
        let store = InMemoryA2aMapperJournalStore::default();
        let stale = delta("stale", A2aJournalToken::genesis(), 1);
        append_chain(&store, 1).await;
        assert!(matches!(
            store.append_delta(stale).await,
            Err(A2aJournalError::StaleHead { .. })
        ));
        assert_eq!(store.manifest().await.unwrap().head().sequence(), 1);
    }

    #[tokio::test]
    async fn restore_is_bounded_and_follows_the_exact_hash_chain() {
        let store = InMemoryA2aMapperJournalStore::default();
        append_chain(&store, 5).await;
        let restore = store.begin_restore().await.unwrap();
        let mut after = restore.start().clone();
        let mut seen = Vec::new();
        loop {
            let page = store
                .read_delta_page(
                    A2aJournalPageRequest::new(
                        restore.restore_id(),
                        after,
                        2,
                        A2A_MAPPER_JOURNAL_MIN_PAGE_BYTES,
                    )
                    .unwrap(),
                )
                .await
                .unwrap();
            assert!(page.deltas().len() <= 2);
            assert!(page.encoded_bytes() <= A2A_MAPPER_JOURNAL_MIN_PAGE_BYTES);
            seen.extend(page.deltas().iter().map(|delta| delta.revision()));
            after = page.next_after().clone();
            if page.is_done() {
                break;
            }
        }
        assert_eq!(seen, vec![2, 3, 4, 5, 6]);
        store.finish_restore(restore.restore_id()).await.unwrap();
    }

    #[tokio::test]
    async fn checkpoint_install_preserves_concurrent_tail_and_restore_replays_it() {
        let store = InMemoryA2aMapperJournalStore::default();
        let deltas = append_chain(&store, 2).await;
        let before = store.manifest().await.unwrap();
        let checkpoint = checkpoint_at(deltas[1].token().unwrap(), "at-two");
        let request = A2aCheckpointInstallRequest::new(
            before.active_checkpoint().cloned(),
            checkpoint.clone(),
        );

        // Tail arrives after checkpoint construction but before its manifest switch.
        let tail = delta("mutation-3", before.head().clone(), 3);
        store.append_delta(tail.clone()).await.unwrap();
        assert_eq!(
            store.install_checkpoint(request.clone()).await.unwrap(),
            A2aCheckpointInstallOutcome::Applied
        );
        // Lost checkpoint response is safely resolved by exact replay.
        assert_eq!(
            store.install_checkpoint(request).await.unwrap(),
            A2aCheckpointInstallOutcome::AlreadyApplied
        );
        assert_eq!(
            store.manifest().await.unwrap().head(),
            &tail.token().unwrap()
        );

        let restore = store.begin_restore().await.unwrap();
        assert_eq!(restore.checkpoint(), Some(&checkpoint));
        let page = store
            .read_delta_page(
                A2aJournalPageRequest::new(
                    restore.restore_id(),
                    restore.start().clone(),
                    8,
                    A2A_MAPPER_JOURNAL_MIN_PAGE_BYTES,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(page.deltas(), std::slice::from_ref(&tail));
        assert!(page.is_done());
        store.finish_restore(restore.restore_id()).await.unwrap();
    }

    #[tokio::test]
    async fn gc_preserves_tail_tombstones_and_is_retry_safe() {
        let store = InMemoryA2aMapperJournalStore::default();
        let deltas = append_chain(&store, 3).await;
        let checkpoint = checkpoint_at(deltas[1].token().unwrap(), "at-two");
        store
            .install_checkpoint(A2aCheckpointInstallRequest::new(None, checkpoint))
            .await
            .unwrap();
        let manifest = store.manifest().await.unwrap();
        let request = A2aJournalGcRequest::from_manifest(&manifest).unwrap();
        assert_eq!(
            store.garbage_collect(request.clone()).await.unwrap(),
            A2aJournalGcOutcome::Collected { removed_deltas: 2 }
        );
        // Models a committed GC whose response was lost.
        assert_eq!(
            store.garbage_collect(request).await.unwrap(),
            A2aJournalGcOutcome::AlreadyCollected
        );
        assert_eq!(store.manifest().await.unwrap().retained_from_sequence(), 3);
        assert!(matches!(
            store.append_delta(deltas[0].clone()).await,
            Ok(A2aJournalAppendOutcome::AlreadyApplied)
        ));

        let restore = store.begin_restore().await.unwrap();
        let page = store
            .read_delta_page(
                A2aJournalPageRequest::new(
                    restore.restore_id(),
                    restore.start().clone(),
                    8,
                    A2A_MAPPER_JOURNAL_MIN_PAGE_BYTES,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(page.deltas(), std::slice::from_ref(&deltas[2]));
        assert!(page.is_done());
    }

    #[tokio::test]
    async fn active_restore_pin_survives_checkpoint_rotation_and_gc() {
        let store = InMemoryA2aMapperJournalStore::default();
        let deltas = append_chain(&store, 3).await;
        let first_checkpoint = checkpoint_at(deltas[0].token().unwrap(), "at-one");
        store
            .install_checkpoint(A2aCheckpointInstallRequest::new(
                None,
                first_checkpoint.clone(),
            ))
            .await
            .unwrap();
        let restore = store.begin_restore().await.unwrap();
        let second_checkpoint = checkpoint_at(deltas[2].token().unwrap(), "at-three");
        store
            .install_checkpoint(A2aCheckpointInstallRequest::new(
                Some(first_checkpoint.metadata().clone()),
                second_checkpoint,
            ))
            .await
            .unwrap();
        let request = A2aJournalGcRequest::from_manifest(&store.manifest().await.unwrap()).unwrap();
        assert_eq!(
            store.garbage_collect(request).await.unwrap(),
            A2aJournalGcOutcome::Collected { removed_deltas: 1 }
        );
        let page = store
            .read_delta_page(
                A2aJournalPageRequest::new(
                    restore.restore_id(),
                    restore.start().clone(),
                    8,
                    A2A_MAPPER_JOURNAL_MIN_PAGE_BYTES,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(page.deltas(), &deltas[1..]);
        assert!(page.is_done());
        store.finish_restore(restore.restore_id()).await.unwrap();
        assert!(store.state.lock().await.deltas.is_empty());
    }

    #[tokio::test]
    async fn corruption_in_delta_content_or_hash_chain_fails_closed() {
        let store = InMemoryA2aMapperJournalStore::default();
        append_chain(&store, 2).await;
        let restore = store.begin_restore().await.unwrap();
        {
            let mut state = store.state.lock().await;
            state.deltas.get_mut(&2).unwrap().operations = vec![operation(99)];
        }
        assert!(matches!(
            store
                .read_delta_page(
                    A2aJournalPageRequest::new(
                        restore.restore_id(),
                        restore.start().clone(),
                        8,
                        A2A_MAPPER_JOURNAL_MIN_PAGE_BYTES,
                    )
                    .unwrap()
                )
                .await,
            Err(A2aJournalError::Corruption { .. })
        ));
    }

    #[tokio::test]
    async fn manifest_and_checkpoint_corruption_are_detected_before_restore() {
        let store = InMemoryA2aMapperJournalStore::default();
        let deltas = append_chain(&store, 1).await;
        let checkpoint = checkpoint_at(deltas[0].token().unwrap(), "valid");
        store
            .install_checkpoint(A2aCheckpointInstallRequest::new(None, checkpoint.clone()))
            .await
            .unwrap();
        {
            let mut state = store.state.lock().await;
            state
                .checkpoints
                .get_mut(checkpoint.metadata().checkpoint_id())
                .unwrap()
                .state[0] ^= 1;
        }
        assert!(matches!(
            store.begin_restore().await,
            Err(A2aJournalError::Corruption { .. })
        ));
    }

    #[tokio::test]
    async fn bootstrap_checkpoint_imports_an_existing_mapper_revision() {
        let store = InMemoryA2aMapperJournalStore::default();
        let mapper = A2aMapper::default();
        let checkpoint = A2aMapperCheckpoint::bootstrap_from_mapper(&mapper).unwrap();
        assert_eq!(
            store
                .install_checkpoint(A2aCheckpointInstallRequest::new(None, checkpoint.clone()))
                .await
                .unwrap(),
            A2aCheckpointInstallOutcome::Applied
        );
        let manifest = store.manifest().await.unwrap();
        assert_eq!(manifest.head(), checkpoint.metadata().high_water());
        assert_eq!(checkpoint.decode_mapper().unwrap(), mapper);
    }
}
