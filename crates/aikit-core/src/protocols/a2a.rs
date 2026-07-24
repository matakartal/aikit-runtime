//! A2A 1.0 identifiers, idempotency, and task lifecycle mapping.

use super::common::{
    scopes, validate_identifier, validate_scope_set, CorrelationIdentity, GovernanceDenialCode,
    GovernanceEnvelope, GovernedAction, ProtocolError, ProtocolErrorCode, ProtocolKind,
    ProtocolPrincipal, ProtocolResult, PROTOCOL_CONTRACT_VERSION,
};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::io::{self, Write};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

fn is_zero_u32(value: &u32) -> bool {
    *value == 0
}

#[derive(Debug, Clone, PartialEq)]
struct CowMap<K, V>(Arc<BTreeMap<K, V>>);

impl<K, V> Default for CowMap<K, V> {
    fn default() -> Self {
        Self(Arc::new(BTreeMap::new()))
    }
}

impl<K, V> From<BTreeMap<K, V>> for CowMap<K, V> {
    fn from(value: BTreeMap<K, V>) -> Self {
        Self(Arc::new(value))
    }
}

impl<K, V> Deref for CowMap<K, V> {
    type Target = BTreeMap<K, V>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<K, V> DerefMut for CowMap<K, V>
where
    K: Clone + Ord,
    V: Clone,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        Arc::make_mut(&mut self.0)
    }
}

impl<'a, K, V> IntoIterator for &'a CowMap<K, V> {
    type Item = (&'a K, &'a V);
    type IntoIter = std::collections::btree_map::Iter<'a, K, V>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<K, V> Serialize for CowMap<K, V>
where
    K: Serialize + Ord,
    V: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<'de, K, V> Deserialize<'de> for CowMap<K, V>
where
    K: Deserialize<'de> + Ord,
    V: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        BTreeMap::deserialize(deserializer).map(Self::from)
    }
}

#[derive(Debug, Clone, PartialEq)]
struct CowSet<T>(Arc<BTreeSet<T>>);

impl<T> Default for CowSet<T> {
    fn default() -> Self {
        Self(Arc::new(BTreeSet::new()))
    }
}

impl<T> From<BTreeSet<T>> for CowSet<T> {
    fn from(value: BTreeSet<T>) -> Self {
        Self(Arc::new(value))
    }
}

impl<T> Deref for CowSet<T> {
    type Target = BTreeSet<T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> DerefMut for CowSet<T>
where
    T: Clone + Ord,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        Arc::make_mut(&mut self.0)
    }
}

impl<'a, T> IntoIterator for &'a CowSet<T> {
    type Item = &'a T;
    type IntoIter = std::collections::btree_set::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<T> Serialize for CowSet<T>
where
    T: Serialize + Ord,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<'de, T> Deserialize<'de> for CowSet<T>
where
    T: Deserialize<'de> + Ord,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        BTreeSet::deserialize(deserializer).map(Self::from)
    }
}

pub const A2A_PROTOCOL_VERSION: &str = "1.0";
pub const A2A_MAPPER_SCHEMA_VERSION: u32 = 4;
pub const A2A_DEFAULT_LIST_TASKS_PAGE_SIZE: u16 = 50;
pub const A2A_MAX_LIST_TASKS_PAGE_SIZE: u16 = 100;
/// Fail-closed permanent-state limits. Terminal tasks, receipts, dispatch tombstones, and settled
/// or quarantined logical event identities are intentionally not discarded automatically: removing
/// any one would make a formerly accepted retry or event publication ambiguous. A host that needs
/// archival compaction must rotate the whole validated snapshot out-of-band. At capacity, new work
/// is rejected while exact retries remain readable.
pub const A2A_MAX_MESSAGE_BYTES: usize = 256 * 1024;
pub const A2A_MAX_ARTIFACTS_PER_TASK: usize = 64;
pub const A2A_MAX_ARTIFACT_BYTES_PER_TASK: usize = 1024 * 1024;
pub const A2A_MAX_TASKS: usize = 10_000;
pub const A2A_MAX_CONTEXTS: usize = 10_000;
pub const A2A_MAX_RECEIPTS: usize = 50_000;
pub const A2A_MAX_TASKS_PER_OWNER: usize = 1_000;
pub const A2A_MAX_CONTEXTS_PER_OWNER: usize = 1_000;
pub const A2A_MAX_RECEIPTS_PER_OWNER: usize = 5_000;
pub const A2A_MAX_RECEIPT_BYTES: usize = 64 * 1024 * 1024;
pub const A2A_MAX_RECEIPT_BYTES_PER_OWNER: usize = 8 * 1024 * 1024;
pub const A2A_MAX_DISPATCHES: usize = A2A_MAX_RECEIPTS;
pub const A2A_MAX_DISPATCHES_PER_OWNER: usize = A2A_MAX_RECEIPTS_PER_OWNER;
pub const A2A_MAX_DISPATCH_BYTES: usize = 128 * 1024 * 1024;
pub const A2A_MAX_DISPATCH_BYTES_PER_OWNER: usize = 16 * 1024 * 1024;
pub const A2A_MAX_PENDING_EVENTS: usize = 100_000;
pub const A2A_MAX_PENDING_EVENTS_PER_OWNER: usize = 10_000;
pub const A2A_MAX_PENDING_EVENT_BYTES: usize = 128 * 1024 * 1024;
pub const A2A_MAX_PENDING_EVENT_BYTES_PER_OWNER: usize = 16 * 1024 * 1024;
pub const A2A_MAX_CANCELLATIONS: usize = A2A_MAX_TASKS;
pub const A2A_MAX_CANCELLATIONS_PER_OWNER: usize = A2A_MAX_TASKS_PER_OWNER;
pub const A2A_MAX_CANCELLATION_BYTES: usize = 32 * 1024 * 1024;
pub const A2A_MAX_CANCELLATION_BYTES_PER_OWNER: usize = 4 * 1024 * 1024;
pub const A2A_MAX_DISPATCH_ATTEMPTS: u32 = 8;
pub const A2A_MAX_EVENT_ATTEMPTS: u32 = 16;
pub const A2A_MAX_CANCELLATION_ATTEMPTS: u32 = 8;
/// Default and absolute serialized mapper snapshot ceiling. The full canonical JSON snapshot is
/// bounded independently from its individual collections so high-cardinality combinations cannot
/// exhaust memory while crossing a persistence boundary.
pub const A2A_DEFAULT_MAPPER_SNAPSHOT_BYTES: usize = 32 * 1024 * 1024;
pub const A2A_MAX_MAPPER_SNAPSHOT_BYTES: usize = 32 * 1024 * 1024;
const A2A_EVENT_RETRY_BASE_MS: u64 = 250;
pub(crate) const A2A_EVENT_RETRY_MAX_MS: u64 = 30_000;

const A2A_MAX_PAGE_TOKEN_BYTES: usize = 1024;
const A2A_PAGE_TOKEN_SCHEMA_VERSION: u32 = 2;
const A2A_MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;
const A2A_CONTEXT_RANDOM_BYTES: usize = 16;
const A2A_CONTEXT_GENERATION_ATTEMPTS: usize = 8;
const A2A_LEGACY_MAPPER_SCHEMA_VERSION: u32 = 1;
const A2A_PREVIOUS_MAPPER_SCHEMA_VERSION: u32 = 2;
const A2A_PRE_ARTIFACT_MAPPER_SCHEMA_VERSION: u32 = 3;
const A2A_RECONCILE_REASON: &str = "dispatch requires reconciliation";
const A2A_EVENT_RECONCILE_REASON: &str = "event publication requires reconciliation";
const A2A_EVENT_ATTEMPTS_EXHAUSTED_REASON: &str = "event publication attempt limit exhausted";
const A2A_EVENT_DETERMINISTIC_POISON_REASON: &str =
    "event publication rejected as deterministic poison";
const A2A_CANCELLATION_RECONCILE_REASON: &str = "cancellation requires reconciliation";
const A2A_UNSETTLED_CANCELLATION_SEND_REASON: &str =
    "A2A task has an unsettled cancellation and cannot accept another message";
const A2A_UNSETTLED_MESSAGE_EVENT_SEND_REASON: &str =
    "A2A task has an earlier message event awaiting durable settlement";
const A2A_UNSETTLED_MESSAGE_DISPATCH_SEND_REASON: &str =
    "A2A task has an earlier message dispatch awaiting durable settlement";
pub(crate) const A2A_PART_WIRE_EXTENSIONS_METADATA_KEY: &str =
    "https://aikit.dev/a2a/part-wire-extensions/v1";

const SEND_MESSAGE_SCOPE: &str = "a2a:message:send";
const TASK_READ_SCOPE: &str = "a2a:tasks:read";
const TASK_CANCEL_SCOPE: &str = "a2a:tasks:cancel";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum A2aRole {
    #[serde(rename = "ROLE_USER")]
    User,
    #[serde(rename = "ROLE_AGENT")]
    Agent,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum A2aPart {
    Text { text: String },
    Data { data: Value },
    File { uri: String, media_type: String },
}

/// Additive wire attributes for part forms that cannot be represented by the original public
/// `A2aPart` enum without breaking exhaustive downstream matches. The canonical bytes live in a
/// reserved metadata sidecar, while the public enum retains its 0.2 source-compatible shape.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct A2aPartWireExtension {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filename: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw: Option<Vec<u8>>,
}

fn decode_part_wire_extensions(
    metadata: &BTreeMap<String, Value>,
) -> ProtocolResult<BTreeMap<usize, A2aPartWireExtension>> {
    let Some(value) = metadata.get(A2A_PART_WIRE_EXTENSIONS_METADATA_KEY) else {
        return Ok(BTreeMap::new());
    };
    let encoded: BTreeMap<String, A2aPartWireExtension> = serde_json::from_value(value.clone())
        .map_err(|_| ProtocolError::invalid("A2A part wire extensions are invalid"))?;
    let mut decoded = BTreeMap::new();
    for (encoded_index, extension) in encoded {
        let index = encoded_index
            .parse::<usize>()
            .map_err(|_| ProtocolError::invalid("A2A part wire extension index is invalid"))?;
        if index.to_string() != encoded_index || decoded.insert(index, extension).is_some() {
            return Err(ProtocolError::invalid(
                "A2A part wire extension index is not canonical",
            ));
        }
    }
    Ok(decoded)
}

fn encode_part_wire_extensions(
    metadata: &mut BTreeMap<String, Value>,
    extensions: BTreeMap<usize, A2aPartWireExtension>,
) -> ProtocolResult<()> {
    if extensions.is_empty() {
        metadata.remove(A2A_PART_WIRE_EXTENSIONS_METADATA_KEY);
        return Ok(());
    }
    let encoded = extensions
        .into_iter()
        .map(|(index, extension)| (index.to_string(), extension))
        .collect::<BTreeMap<_, _>>();
    let value = serde_json::to_value(encoded)
        .map_err(|error| ProtocolError::invalid(format!("encode A2A part extensions: {error}")))?;
    metadata.insert(A2A_PART_WIRE_EXTENSIONS_METADATA_KEY.to_owned(), value);
    Ok(())
}

pub(crate) fn a2a_wire_metadata_without_part_extensions(
    metadata: &BTreeMap<String, Value>,
) -> BTreeMap<String, Value> {
    let mut visible = metadata.clone();
    visible.remove(A2A_PART_WIRE_EXTENSIONS_METADATA_KEY);
    visible
}

pub(crate) fn set_a2a_part_wire_extension(
    metadata: &mut BTreeMap<String, Value>,
    index: usize,
    extension: A2aPartWireExtension,
) -> ProtocolResult<()> {
    let mut extensions = decode_part_wire_extensions(metadata)?;
    if extension == A2aPartWireExtension::default() {
        extensions.remove(&index);
    } else {
        extensions.insert(index, extension);
    }
    encode_part_wire_extensions(metadata, extensions)
}

fn validate_part_wire_extensions(
    parts: &[A2aPart],
    metadata: &BTreeMap<String, Value>,
) -> ProtocolResult<()> {
    for (index, extension) in decode_part_wire_extensions(metadata)? {
        let part = parts.get(index).ok_or_else(|| {
            ProtocolError::invalid("A2A part wire extension index is out of range")
        })?;
        if extension.media_type.as_ref().is_some_and(|media_type| {
            media_type.is_empty() || media_type.len() > 255 || media_type.contains(['\r', '\n'])
        }) || extension.filename.as_ref().is_some_and(|filename| {
            filename.is_empty() || filename.len() > 1024 || filename.contains(['\r', '\n'])
        }) {
            return Err(ProtocolError::invalid(
                "A2A part wire extension metadata is invalid",
            ));
        }
        match (part, extension.raw.as_ref()) {
            (A2aPart::Data { data }, Some(_)) if data.is_null() => {
                if extension.media_type.is_none() {
                    return Err(ProtocolError::invalid(
                        "A2A raw part wire extension requires a media type",
                    ));
                }
            }
            (A2aPart::Text { .. } | A2aPart::Data { .. }, None) => {
                if extension.filename.is_some() {
                    return Err(ProtocolError::invalid(
                        "A2A text and data parts cannot carry a filename",
                    ));
                }
            }
            (A2aPart::File { .. }, None) => {}
            _ => {
                return Err(ProtocolError::invalid(
                    "A2A part wire extension does not match its canonical part",
                ))
            }
        }
    }
    Ok(())
}

/// Rich A2A content parts used by the additive artifact API. `A2aPart` intentionally remains the
/// source-compatible message model; inbound message-only wire attributes are preserved in its
/// reserved canonical metadata sidecar.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum A2aContentPart {
    Text {
        text: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        media_type: Option<String>,
    },
    Data {
        data: Value,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        media_type: Option<String>,
    },
    File {
        uri: String,
        media_type: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        filename: Option<String>,
    },
    Raw {
        raw: Vec<u8>,
        media_type: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        filename: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct A2aArtifact {
    pub artifact_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub parts: Vec<A2aContentPart>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, Value>,
}

impl A2aArtifact {
    pub fn validate(&self) -> ProtocolResult<()> {
        validate_identifier("A2A artifact_id", &self.artifact_id)?;
        if self.parts.is_empty() {
            return Err(ProtocolError::invalid(
                "A2A artifact must contain at least one part",
            ));
        }
        if self
            .name
            .as_ref()
            .is_some_and(|name| name.is_empty() || name.len() > 1024)
            || self
                .description
                .as_ref()
                .is_some_and(|description| description.len() > 16 * 1024)
        {
            return Err(ProtocolError::invalid(
                "A2A artifact name or description is invalid",
            ));
        }
        for part in &self.parts {
            match part {
                A2aContentPart::Text { media_type, .. }
                | A2aContentPart::Data { media_type, .. } => {
                    if media_type.as_ref().is_some_and(|media_type| {
                        media_type.is_empty()
                            || media_type.len() > 255
                            || media_type.contains(['\r', '\n'])
                    }) {
                        return Err(ProtocolError::invalid(
                            "A2A artifact text or data media type is invalid",
                        ));
                    }
                }
                A2aContentPart::File {
                    uri,
                    media_type,
                    filename,
                } => {
                    if uri.is_empty()
                        || uri.len() > 8 * 1024
                        || media_type.is_empty()
                        || media_type.len() > 255
                        || uri.contains(['\r', '\n'])
                        || media_type.contains(['\r', '\n'])
                        || filename.as_ref().is_some_and(|filename| {
                            filename.is_empty()
                                || filename.len() > 1024
                                || filename.contains(['\r', '\n'])
                        })
                    {
                        return Err(ProtocolError::invalid(
                            "A2A artifact file URI or media type is invalid",
                        ));
                    }
                }
                A2aContentPart::Raw {
                    media_type,
                    filename,
                    ..
                } => {
                    if media_type.is_empty()
                        || media_type.len() > 255
                        || media_type.contains(['\r', '\n'])
                        || filename.as_ref().is_some_and(|filename| {
                            filename.is_empty()
                                || filename.len() > 1024
                                || filename.contains(['\r', '\n'])
                        })
                    {
                        return Err(ProtocolError::invalid(
                            "A2A artifact raw file metadata is invalid",
                        ));
                    }
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct A2aMessage {
    pub message_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    pub role: A2aRole,
    pub parts: Vec<A2aPart>,
    #[serde(default)]
    pub metadata: BTreeMap<String, Value>,
}

impl A2aMessage {
    pub fn validate(&self) -> ProtocolResult<()> {
        validate_identifier("A2A message_id", &self.message_id)?;
        if let Some(context_id) = &self.context_id {
            validate_identifier("A2A context_id", context_id)?;
        }
        if let Some(task_id) = &self.task_id {
            validate_identifier("A2A task_id", task_id)?;
        }
        if self.parts.is_empty() {
            return Err(ProtocolError::invalid(
                "A2A message must contain at least one part",
            ));
        }
        validate_part_wire_extensions(&self.parts, &self.metadata)?;
        if message_storage_bytes(self)? > A2A_MAX_MESSAGE_BYTES {
            return Err(ProtocolError::invalid(format!(
                "A2A message must not exceed {A2A_MAX_MESSAGE_BYTES} serialized bytes"
            )));
        }
        Ok(())
    }

    /// Lossless rich view of the message's wire parts. This additive accessor exposes raw bytes,
    /// filenames, and optional text/data media types while leaving the original public `parts`
    /// field source compatible for existing exhaustive matches and struct literals.
    pub fn content_parts(&self) -> ProtocolResult<Vec<A2aContentPart>> {
        let extensions = decode_part_wire_extensions(&self.metadata)?;
        self.parts
            .iter()
            .enumerate()
            .map(|(index, part)| {
                let extension = extensions.get(&index);
                if let Some(raw) = extension.and_then(|extension| extension.raw.as_ref()) {
                    return Ok(A2aContentPart::Raw {
                        raw: raw.clone(),
                        media_type: extension
                            .and_then(|extension| extension.media_type.clone())
                            .ok_or_else(|| {
                                ProtocolError::invalid(
                                    "A2A raw part wire extension requires a media type",
                                )
                            })?,
                        filename: extension.and_then(|extension| extension.filename.clone()),
                    });
                }
                Ok(match part {
                    A2aPart::Text { text } => A2aContentPart::Text {
                        text: text.clone(),
                        media_type: extension.and_then(|extension| extension.media_type.clone()),
                    },
                    A2aPart::Data { data } => A2aContentPart::Data {
                        data: data.clone(),
                        media_type: extension.and_then(|extension| extension.media_type.clone()),
                    },
                    A2aPart::File { uri, media_type } => A2aContentPart::File {
                        uri: uri.clone(),
                        media_type: media_type.clone(),
                        filename: extension.and_then(|extension| extension.filename.clone()),
                    },
                })
            })
            .collect()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum A2aTaskState {
    #[serde(rename = "TASK_STATE_SUBMITTED")]
    Submitted,
    #[serde(rename = "TASK_STATE_WORKING")]
    Working,
    #[serde(rename = "TASK_STATE_INPUT_REQUIRED")]
    InputRequired,
    #[serde(rename = "TASK_STATE_AUTH_REQUIRED")]
    AuthRequired,
    #[serde(rename = "TASK_STATE_COMPLETED")]
    Completed,
    #[serde(rename = "TASK_STATE_FAILED")]
    Failed,
    #[serde(rename = "TASK_STATE_CANCELED")]
    Cancelled,
    #[serde(rename = "TASK_STATE_REJECTED")]
    Rejected,
}

impl A2aTaskState {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Cancelled | Self::Rejected
        )
    }
}

/// Canonical A2A 1.0 task-list filters.
///
/// `historyLength`, `statusTimestampAfter`, and `includeArtifacts` remain wire concerns until the
/// canonical task record carries those exact official fields. The mapper intentionally exposes
/// only filters it can enforce without pretending that absent history or artifact data exists.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct A2aListTasksRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<A2aTaskState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page_size: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page_token: Option<String>,
}

impl A2aListTasksRequest {
    fn validate(&self) -> ProtocolResult<()> {
        if let Some(tenant) = &self.tenant {
            validate_identifier("A2A list tenant", tenant)?;
        }
        if let Some(context_id) = &self.context_id {
            validate_identifier("A2A list context_id", context_id)?;
        }
        if let Some(page_size) = self.page_size {
            if !(1..=A2A_MAX_LIST_TASKS_PAGE_SIZE).contains(&page_size) {
                return Err(ProtocolError::invalid(format!(
                    "A2A page_size must be between 1 and {A2A_MAX_LIST_TASKS_PAGE_SIZE}"
                )));
            }
        }
        if let Some(page_token) = &self.page_token {
            if !page_token.is_empty() {
                decode_page_token(page_token)?;
            }
        }
        Ok(())
    }
}

/// Authorized, filtered, stably ordered canonical page.
///
/// A wire binding must project each internal [`A2aTaskRecord`] into the official A2A `Task` DTO;
/// it must never serialize owner, runtime, or revision fields directly onto the network.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct A2aTaskPage {
    pub tasks: Vec<A2aTaskRecord>,
    /// Always present. An empty token means there is no next page, as required by A2A 1.0.
    pub next_page_token: String,
    pub page_size: u16,
    pub total_size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct A2aPageCursor {
    schema_version: u32,
    snapshot_hash: String,
    query_hash: String,
    next_task_id: String,
}

/// Exact A2A-to-runtime identity projection required for replay and audit correlation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct A2aRunMapping {
    pub context_id: String,
    pub session_id: String,
    pub task_id: String,
    pub run_id: String,
    pub message_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct A2aTaskRecord {
    pub mapping: A2aRunMapping,
    pub state: A2aTaskState,
    pub owner_subject: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_tenant_id: Option<String>,
    pub created_revision: u64,
    pub updated_revision: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status_message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct A2aMessageReceipt {
    pub message: A2aMessage,
    pub mapping: A2aRunMapping,
    pub owner_subject: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_tenant_id: Option<String>,
    pub accepted_revision: u64,
}

/// Mapper-snapshot outbox state. `Running` is never treated as safe to rerun after restore: a
/// startup worker must first move it to `ReconcilePending`, reconcile the unknown host outcome,
/// and only then claim a bounded retry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum A2aDispatchOutboxState {
    Queued,
    Running,
    ReconcilePending,
    Settled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum A2aSendResponsePolicy {
    #[default]
    Blocking,
    Immediate,
    Streaming,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum A2aDispatchResponse {
    Task {
        #[serde(default)]
        finalized_by_dispatch: bool,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        artifacts: Vec<A2aArtifact>,
    },
    Message {
        message: A2aMessage,
    },
}

impl Default for A2aDispatchResponse {
    fn default() -> Self {
        Self::Task {
            finalized_by_dispatch: false,
            artifacts: Vec::new(),
        }
    }
}

enum A2aDispatchCompletionOutput {
    Task { artifacts: Vec<A2aArtifact> },
    Message { message: A2aMessage },
}

/// Canonical host-dispatch intent stored in the same mapper snapshot as its task and receipt.
///
/// The normalized message and original authorized envelope are intentionally retained. They let
/// a host reconstruct the exact governed action after a crash without accepting replacement wire
/// data. The public reconstruction API still requires an exact subject+tenant owner check.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct A2aDispatchOutboxRecord {
    pub dispatch_id: String,
    pub owner_subject: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_tenant_id: Option<String>,
    pub message_id: String,
    pub task_id: String,
    pub context_id: String,
    pub session_id: String,
    pub run_id: String,
    pub message: A2aMessage,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resumed_from: Option<A2aTaskState>,
    pub envelope: GovernanceEnvelope,
    pub state: A2aDispatchOutboxState,
    #[serde(default)]
    pub response: A2aDispatchResponse,
    #[serde(default)]
    pub(crate) response_policy: A2aSendResponsePolicy,
    /// Frozen response for the request that first accepted this idempotency key in immediate
    /// mode. Host completion may update the durable task and output, but retries still project
    /// this exact task snapshot and therefore cannot flip their response oneof.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub immediate_response: Option<A2aTaskRecord>,
    pub attempts: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    pub created_revision: u64,
    pub updated_revision: u64,
}

impl A2aDispatchOutboxRecord {
    fn mapping(&self) -> A2aRunMapping {
        A2aRunMapping {
            context_id: self.context_id.clone(),
            session_id: self.session_id.clone(),
            task_id: self.task_id.clone(),
            run_id: self.run_id.clone(),
            message_id: self.message_id.clone(),
        }
    }

    fn action(&self) -> A2aAction {
        A2aAction::DispatchMessage {
            message: self.message.clone(),
            mapping: self.mapping(),
            resumed_from: self.resumed_from,
        }
    }
}

/// Durable control-plane cancellation state. A restored `Running` cancellation has an unknown
/// host outcome and must be reconciled before the bounded retry can be claimed again.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum A2aCancellationOutboxState {
    Queued,
    Running,
    ReconcilePending,
    Settled,
}

/// Canonical cancellation intent stored atomically with the task intent and logical event.
///
/// `cancellation_id` is stable for one exact owner + task + run tuple. The immutable task snapshot
/// and governed envelope let a host recover the precise authorized cancel action after restart;
/// reconstruction still requires the same subject, tenant, and cancellation scope.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct A2aCancellationOutboxRecord {
    pub cancellation_id: String,
    pub owner_subject: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_tenant_id: Option<String>,
    pub task_id: String,
    pub context_id: String,
    pub session_id: String,
    pub run_id: String,
    pub task: A2aTaskRecord,
    pub envelope: GovernanceEnvelope,
    pub state: A2aCancellationOutboxState,
    pub attempts: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    pub created_revision: u64,
    pub updated_revision: u64,
}

impl A2aCancellationOutboxRecord {
    fn action(&self) -> A2aAction {
        A2aAction::CancelTask {
            task: self.task.clone(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum A2aPendingEventState {
    Pending,
    ReconcilePending,
    Settled,
    /// Durable terminal outbox state for an event that was never proven published. The bound task
    /// is deliberately left untouched and must remain available for operator reconciliation.
    Quarantined,
}

/// Sanitized, durable reason why a logical event left the retry queue without being settled.
///
/// This deliberately carries categories rather than host or wire error text, which may contain
/// credentials or provider payloads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum A2aEventQuarantineReason {
    AttemptsExhausted,
    DeterministicPoison,
}

impl A2aEventQuarantineReason {
    fn diagnostic(self) -> &'static str {
        match self {
            Self::AttemptsExhausted => A2A_EVENT_ATTEMPTS_EXHAUSTED_REASON,
            Self::DeterministicPoison => A2A_EVENT_DETERMINISTIC_POISON_REASON,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum A2aPendingEventKind {
    TaskCreated,
    MessageAccepted,
    StatusChanged,
    CancellationRequested,
    DirectMessageResponse,
    /// Deterministic repair intent created only when an old v1 snapshot predates event intents.
    RecoveredSnapshot,
}

/// Durable logical stream-event intent. `event_id` is the idempotency identity an external event
/// store should use; this mapper snapshot is not itself an external event store.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct A2aPendingEventIntent {
    pub event_id: String,
    pub owner_subject: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_tenant_id: Option<String>,
    pub task_id: String,
    pub context_id: String,
    pub session_id: String,
    pub run_id: String,
    pub source_revision: u64,
    pub kind: A2aPendingEventKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_id: Option<String>,
    /// Hash of the immutable canonical event kind + task snapshot payload.
    pub payload_hash: String,
    pub task: A2aTaskRecord,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_message: Option<A2aMessage>,
    pub state: A2aPendingEventState,
    /// Reserved for deterministic poison accounting in legacy snapshots. Transient event-store
    /// failures use `transient_failures` and never consume this terminal budget.
    pub attempts: u32,
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    pub transient_failures: u32,
    /// Durable wall-clock gate for recovery. Zero is accepted only while migrating an older
    /// reconcile-pending snapshot and means the retry is immediately due.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_attempt_at_unix_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quarantine_reason: Option<A2aEventQuarantineReason>,
    pub created_revision: u64,
    pub updated_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct A2aContextOwner {
    subject: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    tenant_id: Option<String>,
}

impl A2aContextOwner {
    fn from_principal(principal: &ProtocolPrincipal) -> Self {
        Self {
            subject: principal.subject.clone(),
            tenant_id: principal.tenant_id.clone(),
        }
    }

    fn matches(&self, principal: &ProtocolPrincipal) -> bool {
        principal.matches_identity(&self.subject, self.tenant_id.as_deref())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct A2aPendingEventScheduleKey {
    next_attempt_at_unix_ms: u64,
    source_revision: u64,
    event_id: String,
}

impl A2aPendingEventScheduleKey {
    fn from_event(event: &A2aPendingEventIntent) -> Option<Self> {
        matches!(
            event.state,
            A2aPendingEventState::Pending | A2aPendingEventState::ReconcilePending
        )
        .then(|| Self {
            next_attempt_at_unix_ms: event.next_attempt_at_unix_ms.unwrap_or(0),
            source_revision: event.source_revision,
            event_id: event.event_id.clone(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct A2aDispatchEventBinding {
    owner_subject: String,
    owner_tenant_id: Option<String>,
    task_id: String,
    context_id: String,
    session_id: String,
    run_id: String,
    message_id: String,
    source_revision: u64,
}

impl A2aDispatchEventBinding {
    fn from_event(event: &A2aPendingEventIntent) -> Option<Self> {
        Some(Self {
            owner_subject: event.owner_subject.clone(),
            owner_tenant_id: event.owner_tenant_id.clone(),
            task_id: event.task_id.clone(),
            context_id: event.context_id.clone(),
            session_id: event.session_id.clone(),
            run_id: event.run_id.clone(),
            message_id: event.message_id.clone()?,
            source_revision: event.source_revision,
        })
    }

    fn from_dispatch(record: &A2aDispatchOutboxRecord) -> Self {
        Self {
            owner_subject: record.owner_subject.clone(),
            owner_tenant_id: record.owner_tenant_id.clone(),
            task_id: record.task_id.clone(),
            context_id: record.context_id.clone(),
            session_id: record.session_id.clone(),
            run_id: record.run_id.clone(),
            message_id: record.message_id.clone(),
            source_revision: record.created_revision,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum A2aAction {
    DispatchMessage {
        message: A2aMessage,
        mapping: A2aRunMapping,
        resumed_from: Option<A2aTaskState>,
    },
    DuplicateMessage {
        receipt: A2aMessageReceipt,
    },
    GetTask {
        task: A2aTaskRecord,
    },
    ListTasks {
        page: A2aTaskPage,
    },
    CancelTask {
        task: A2aTaskRecord,
    },
}

/// Serializable state machine implementing context/session and task/run mappings.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct A2aMapper {
    schema_version: u32,
    contexts: BTreeMap<String, String>,
    context_owners: BTreeMap<String, A2aContextOwner>,
    tasks: CowMap<String, A2aTaskRecord>,
    receipts: CowMap<String, A2aMessageReceipt>,
    #[serde(skip)]
    receipt_bytes: usize,
    dispatch_outbox: CowMap<String, A2aDispatchOutboxRecord>,
    #[serde(skip)]
    dispatch_bytes: usize,
    cancellation_outbox: CowMap<String, A2aCancellationOutboxRecord>,
    #[serde(skip)]
    cancellation_bytes: usize,
    pending_events: CowMap<String, A2aPendingEventIntent>,
    #[serde(skip)]
    pending_event_bytes: usize,
    #[serde(skip)]
    pending_event_schedule: CowSet<A2aPendingEventScheduleKey>,
    #[serde(skip)]
    pending_event_schedule_by_owner: CowMap<A2aContextOwner, CowSet<A2aPendingEventScheduleKey>>,
    #[serde(skip)]
    dispatch_event_readiness: CowMap<A2aDispatchEventBinding, (usize, usize)>,
    next_sequence: u64,
    revision: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct A2aMapperWire {
    #[serde(default = "a2a_mapper_schema_version")]
    schema_version: u32,
    contexts: BTreeMap<String, String>,
    context_owners: BTreeMap<String, A2aContextOwner>,
    tasks: BTreeMap<String, A2aTaskRecord>,
    receipts: BTreeMap<String, A2aMessageReceipt>,
    #[serde(default)]
    dispatch_outbox: Option<BTreeMap<String, A2aDispatchOutboxRecord>>,
    #[serde(default)]
    cancellation_outbox: Option<BTreeMap<String, A2aCancellationOutboxRecord>>,
    #[serde(default)]
    pending_events: Option<BTreeMap<String, A2aPendingEventIntent>>,
    next_sequence: u64,
    revision: u64,
}

impl<'de> Deserialize<'de> for A2aMapper {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = A2aMapperWire::deserialize(deserializer)?;
        let (contexts, context_owners) =
            normalize_restored_contexts(wire.contexts, wire.context_owners, &wire.tasks)
                .map_err(de::Error::custom)?;
        let receipts = normalize_restored_receipts(wire.receipts).map_err(de::Error::custom)?;
        let receipt_bytes = receipt_storage_bytes(&receipts).map_err(de::Error::custom)?;
        let dispatch_outbox = match wire.dispatch_outbox {
            Some(dispatch_outbox) => dispatch_outbox,
            None => {
                rebuild_legacy_dispatch_outbox(&wire.tasks, &receipts).map_err(de::Error::custom)?
            }
        };
        let dispatch_bytes = dispatch_storage_bytes(&dispatch_outbox).map_err(de::Error::custom)?;
        let mut pending_events = match wire.pending_events {
            Some(pending_events) => pending_events,
            None => rebuild_legacy_pending_events(&wire.tasks).map_err(de::Error::custom)?,
        };
        migrate_restored_pending_events(wire.schema_version, &mut pending_events)
            .map_err(de::Error::custom)?;
        let pending_event_bytes =
            pending_event_storage_bytes(&pending_events).map_err(de::Error::custom)?;
        let pending_event_schedule = rebuild_pending_event_schedule(&pending_events);
        let pending_event_schedule_by_owner =
            rebuild_pending_event_schedule_by_owner(&pending_events);
        let dispatch_event_readiness = rebuild_dispatch_event_readiness(&pending_events);
        let cancellation_outbox = match wire.cancellation_outbox {
            Some(cancellation_outbox) => cancellation_outbox,
            None => rebuild_legacy_cancellation_outbox(&wire.tasks, &pending_events)
                .map_err(de::Error::custom)?,
        };
        let cancellation_bytes =
            cancellation_storage_bytes(&cancellation_outbox).map_err(de::Error::custom)?;
        let mapper = Self {
            schema_version: match wire.schema_version {
                A2A_LEGACY_MAPPER_SCHEMA_VERSION
                | A2A_PREVIOUS_MAPPER_SCHEMA_VERSION
                | A2A_PRE_ARTIFACT_MAPPER_SCHEMA_VERSION
                | A2A_MAPPER_SCHEMA_VERSION => A2A_MAPPER_SCHEMA_VERSION,
                unsupported => unsupported,
            },
            contexts,
            context_owners,
            tasks: wire.tasks.into(),
            receipts: receipts.into(),
            receipt_bytes,
            dispatch_outbox: dispatch_outbox.into(),
            dispatch_bytes,
            cancellation_outbox: cancellation_outbox.into(),
            cancellation_bytes,
            pending_events: pending_events.into(),
            pending_event_bytes,
            pending_event_schedule,
            pending_event_schedule_by_owner,
            dispatch_event_readiness,
            next_sequence: wire.next_sequence,
            revision: wire.revision,
        };
        mapper.validate_snapshot().map_err(de::Error::custom)?;
        Ok(mapper)
    }
}

impl Default for A2aMapper {
    fn default() -> Self {
        Self::new()
    }
}

impl A2aMapper {
    pub fn new() -> Self {
        Self {
            schema_version: A2A_MAPPER_SCHEMA_VERSION,
            contexts: BTreeMap::new(),
            context_owners: BTreeMap::new(),
            tasks: CowMap::default(),
            receipts: CowMap::default(),
            receipt_bytes: 0,
            dispatch_outbox: CowMap::default(),
            dispatch_bytes: 0,
            cancellation_outbox: CowMap::default(),
            cancellation_bytes: 0,
            pending_events: CowMap::default(),
            pending_event_bytes: 0,
            pending_event_schedule: CowSet::default(),
            pending_event_schedule_by_owner: CowMap::default(),
            dispatch_event_readiness: CowMap::default(),
            next_sequence: 1,
            revision: 0,
        }
    }

    /// Internal context index. Keys are length-framed identities, not wire ids; deserialization
    /// migrates pre-scoping snapshots into this canonical namespace.
    /// Prefer [`Self::context_session`] when resolving one authenticated context.
    pub fn contexts(&self) -> &BTreeMap<String, String> {
        &self.contexts
    }

    pub fn context_session(&self, context_id: &str, principal: &ProtocolPrincipal) -> Option<&str> {
        let scoped_key = scoped_context_key(principal, context_id);
        self.contexts.get(&scoped_key).and_then(|session_id| {
            self.context_owners
                .get(&scoped_key)
                .is_some_and(|owner| owner.matches(principal))
                .then_some(session_id.as_str())
        })
    }

    pub fn tasks(&self) -> &BTreeMap<String, A2aTaskRecord> {
        &self.tasks
    }

    /// Internal owner-scoped receipt index. Prefer [`Self::message_receipt`] for one principal.
    pub fn receipts(&self) -> &BTreeMap<String, A2aMessageReceipt> {
        &self.receipts
    }

    /// Internal durable dispatch index. Keys are stable dispatch identities, not run ids: one
    /// task run may legitimately accept more than one message.
    pub fn dispatch_outbox(&self) -> &BTreeMap<String, A2aDispatchOutboxRecord> {
        &self.dispatch_outbox
    }

    /// Durable output artifacts for the task's unique finalized dispatch. The snapshot validator
    /// enforces both uniqueness and latest-message binding, so callers never merge generations.
    pub fn artifacts_for_task(&self, task_id: &str) -> &[A2aArtifact] {
        self.dispatch_outbox
            .values()
            .find_map(|dispatch| {
                if dispatch.task_id != task_id || dispatch.state != A2aDispatchOutboxState::Settled
                {
                    return None;
                }
                match &dispatch.response {
                    A2aDispatchResponse::Task {
                        finalized_by_dispatch: true,
                        artifacts,
                    } => Some(artifacts.as_slice()),
                    _ => None,
                }
            })
            .unwrap_or(&[])
    }

    pub fn pending_event_intents(&self) -> &BTreeMap<String, A2aPendingEventIntent> {
        &self.pending_events
    }

    pub fn message_receipt(
        &self,
        message_id: &str,
        principal: &ProtocolPrincipal,
    ) -> Option<&A2aMessageReceipt> {
        self.receipts
            .get(&scoped_receipt_key(principal, message_id))
    }

    pub fn dispatch_for_message(
        &self,
        message_id: &str,
        principal: &ProtocolPrincipal,
    ) -> Option<&A2aDispatchOutboxRecord> {
        self.dispatch_outbox
            .get(&scoped_dispatch_key(principal, message_id))
            .filter(|record| dispatch_owner_matches(record, principal))
    }

    /// Resolve the one outbox record bound to an already-validated receipt identity.
    pub fn dispatch_for_receipt(
        &self,
        receipt: &A2aMessageReceipt,
        principal: &ProtocolPrincipal,
    ) -> ProtocolResult<&A2aDispatchOutboxRecord> {
        if !receipt_owner_matches(receipt, principal) {
            return Err(ProtocolError::not_found(
                "A2A dispatch receipt is not accessible",
            ));
        }
        let canonical = self
            .message_receipt(&receipt.message.message_id, principal)
            .filter(|canonical| *canonical == receipt)
            .ok_or_else(|| ProtocolError::not_found("A2A dispatch receipt is not registered"))?;
        let record = self
            .dispatch_for_message(&canonical.message.message_id, principal)
            .ok_or_else(|| ProtocolError::conflict("A2A receipt is missing its dispatch record"))?;
        if record.task_id != canonical.mapping.task_id
            || record.context_id != canonical.mapping.context_id
            || record.session_id != canonical.mapping.session_id
            || record.run_id != canonical.mapping.run_id
        {
            return Err(ProtocolError::conflict(
                "A2A dispatch does not match its receipt runtime identity",
            ));
        }
        Ok(record)
    }

    /// Resolve the one stable cancellation control bound to an exact owner-scoped task/run.
    pub fn cancellation_for_task(
        &self,
        task_id: &str,
        principal: &ProtocolPrincipal,
    ) -> Option<&A2aCancellationOutboxRecord> {
        let task = self
            .tasks
            .get(task_id)
            .filter(|task| task_owner_matches(task, principal))?;
        let cancellation_id = cancellation_id_for_task(task);
        self.cancellation_outbox
            .get(&cancellation_id)
            .filter(|record| cancellation_owner_matches(record, principal))
    }

    /// Deterministic startup view. `Running` is intentionally included because its host outcome
    /// is unknown after restore and must be moved to reconciliation before any retry.
    pub fn pending_cancellations(&self) -> Vec<A2aCancellationOutboxRecord> {
        let mut pending: Vec<_> = self
            .cancellation_outbox
            .values()
            .filter(|record| record.state != A2aCancellationOutboxState::Settled)
            .filter(|record| {
                self.tasks
                    .get(&record.task_id)
                    .is_some_and(|task| !task.state.is_terminal())
            })
            .cloned()
            .collect();
        pending.sort_by(|left, right| {
            left.created_revision
                .cmp(&right.created_revision)
                .then_with(|| left.cancellation_id.cmp(&right.cancellation_id))
        });
        pending
    }

    /// Rebuild the exact cancellation action after current owner and cancel-scope checks.
    pub fn reconstruct_cancel(
        &self,
        cancellation_id: &str,
        principal: &ProtocolPrincipal,
    ) -> ProtocolResult<(GovernanceEnvelope, A2aAction)> {
        let record = self
            .cancellation_outbox
            .get(cancellation_id)
            .filter(|record| cancellation_owner_matches(record, principal))
            .ok_or_else(|| ProtocolError::not_found("A2A cancellation is not accessible"))?;
        let task = self
            .tasks
            .get(&record.task_id)
            .ok_or_else(|| ProtocolError::conflict("A2A cancellation task is not registered"))?;
        if record.state == A2aCancellationOutboxState::Settled || task.state.is_terminal() {
            return Err(ProtocolError::invalid_transition(
                "A2A cancellation is not runnable in its current task state",
            ));
        }

        let mut envelope = GovernanceEnvelope::evaluate(
            ProtocolKind::A2a,
            record.envelope.correlation.clone(),
            Some(principal),
            "tasks/cancel",
            record.task_id.clone(),
            scopes(&[TASK_CANCEL_SCOPE]),
        );
        if !envelope.authorization.is_allowed() {
            return GovernedAction::<A2aAction>::denied(envelope).into_authorized();
        }
        envelope.correlation.session_id = Some(record.session_id.clone());
        envelope.correlation.run_id = Some(record.run_id.clone());
        Ok((envelope, record.action()))
    }

    /// Deterministic startup/reconciliation view. A `Running` record is returned so the caller can
    /// reconcile an unknown outcome, but tasks waiting for input/authentication and terminal tasks
    /// are never returned as runnable work.
    pub fn pending_dispatches(&self) -> Vec<A2aDispatchOutboxRecord> {
        let mut pending: Vec<_> = self
            .dispatch_outbox
            .values()
            .filter(|record| record.state != A2aDispatchOutboxState::Settled)
            .filter(|record| {
                self.tasks.get(&record.task_id).is_some_and(|task| {
                    !task.state.is_terminal()
                        && !matches!(
                            task.state,
                            A2aTaskState::InputRequired | A2aTaskState::AuthRequired
                        )
                })
            })
            .cloned()
            .collect();
        pending.sort_by(|left, right| {
            left.created_revision
                .cmp(&right.created_revision)
                .then_with(|| left.dispatch_id.cmp(&right.dispatch_id))
        });
        pending
    }

    /// Rebuild the exact host action from durable canonical data after current owner/scope checks.
    pub fn reconstruct_dispatch(
        &self,
        dispatch_id: &str,
        principal: &ProtocolPrincipal,
    ) -> ProtocolResult<(GovernanceEnvelope, A2aAction)> {
        let record = self
            .dispatch_outbox
            .get(dispatch_id)
            .filter(|record| dispatch_owner_matches(record, principal))
            .ok_or_else(|| ProtocolError::not_found("A2A dispatch is not accessible"))?;
        let task = self
            .tasks
            .get(&record.task_id)
            .ok_or_else(|| ProtocolError::conflict("A2A dispatch task is not registered"))?;
        if record.state == A2aDispatchOutboxState::Settled
            || task.state.is_terminal()
            || matches!(
                task.state,
                A2aTaskState::InputRequired | A2aTaskState::AuthRequired
            )
        {
            return Err(ProtocolError::invalid_transition(
                "A2A dispatch is not runnable in its current task state",
            ));
        }

        let mut envelope = GovernanceEnvelope::evaluate(
            ProtocolKind::A2a,
            record.envelope.correlation.clone(),
            Some(principal),
            "message/send",
            record.envelope.target.clone(),
            scopes(&[SEND_MESSAGE_SCOPE]),
        );
        if !envelope.authorization.is_allowed() {
            return GovernedAction::<A2aAction>::denied(envelope).into_authorized();
        }
        envelope.correlation.session_id = Some(record.session_id.clone());
        envelope.correlation.run_id = Some(record.run_id.clone());
        Ok((envelope, record.action()))
    }

    pub fn pending_events(&self) -> Vec<A2aPendingEventIntent> {
        let mut pending: Vec<_> = self
            .pending_events
            .values()
            .filter(|event| {
                matches!(
                    event.state,
                    A2aPendingEventState::Pending | A2aPendingEventState::ReconcilePending
                )
            })
            .cloned()
            .collect();
        pending.sort_by(|left, right| {
            left.source_revision
                .cmp(&right.source_revision)
                .then_with(|| left.event_id.cmp(&right.event_id))
        });
        pending
    }

    /// Return an owner-fair, bounded due batch using the non-serialized retry indexes. One owner's
    /// deep queue cannot hide another owner's event, and a no-due lookup is O(1) regardless of the
    /// number of retained settled event identities.
    pub fn pending_events_due_fair_batch(
        &self,
        now_unix_ms: u64,
        max_total: usize,
        max_per_owner: usize,
        cursor: &mut Option<(String, Option<String>)>,
    ) -> Vec<A2aPendingEventIntent> {
        if max_total == 0
            || max_per_owner == 0
            || self
                .pending_event_schedule
                .first()
                .is_none_or(|key| key.next_attempt_at_unix_ms > now_unix_ms)
        {
            return Vec::new();
        }
        let due_owners: Vec<_> = self
            .pending_event_schedule_by_owner
            .iter()
            .filter_map(|(owner, schedule)| {
                schedule
                    .first()
                    .is_some_and(|key| key.next_attempt_at_unix_ms <= now_unix_ms)
                    .then_some(owner.clone())
            })
            .collect();
        if due_owners.is_empty() {
            return Vec::new();
        }
        let start = cursor
            .as_ref()
            .map(|(subject, tenant_id)| A2aContextOwner {
                subject: subject.clone(),
                tenant_id: tenant_id.clone(),
            })
            .map(|cursor_owner| due_owners.partition_point(|owner| owner <= &cursor_owner))
            .unwrap_or_default();
        let mut selected = Vec::with_capacity(max_total.min(due_owners.len()));
        for owner_item in 0..max_per_owner {
            for owner_offset in 0..due_owners.len() {
                let owner = &due_owners[(start + owner_offset) % due_owners.len()];
                let Some(key) =
                    self.pending_event_schedule_by_owner
                        .get(owner)
                        .and_then(|schedule| {
                            schedule
                                .iter()
                                .take_while(|key| key.next_attempt_at_unix_ms <= now_unix_ms)
                                .nth(owner_item)
                        })
                else {
                    continue;
                };
                if let Some(event) = self.pending_events.get(&key.event_id) {
                    selected.push(event.clone());
                    *cursor = Some((owner.subject.clone(), owner.tenant_id.clone()));
                    if selected.len() == max_total {
                        return selected;
                    }
                }
            }
        }
        selected
    }

    /// Earliest durable event retry deadline, if any. This is O(1) even when the snapshot retains
    /// the maximum number of settled event identities.
    pub fn next_pending_event_attempt_at(&self) -> Option<u64> {
        self.pending_event_schedule
            .first()
            .map(|key| key.next_attempt_at_unix_ms)
    }

    /// Clamp restored retry deadlines to the protocol's maximum backoff window.
    ///
    /// Retry timestamps are persisted as wall-clock milliseconds so they survive a restart. If
    /// the host clock moves backwards between writes or restarts, an otherwise bounded retry can
    /// appear arbitrarily far in the future. Rewriting only those outliers preserves normal
    /// backoff while guaranteeing that accepted events become due within one maximum window.
    pub(crate) fn clamp_restored_event_retry_deadlines(
        &mut self,
        now_unix_ms: u64,
    ) -> ProtocolResult<usize> {
        let latest_allowed = now_unix_ms.saturating_add(A2A_EVENT_RETRY_MAX_MS);
        let event_ids: Vec<String> = self
            .pending_events
            .iter()
            .filter(|(_, event)| {
                event.state == A2aPendingEventState::ReconcilePending
                    && event
                        .next_attempt_at_unix_ms
                        .is_some_and(|deadline| deadline > latest_allowed)
            })
            .map(|(event_id, _)| event_id.clone())
            .collect();
        if event_ids.is_empty() {
            return Ok(0);
        }

        // Keep the repair atomic for direct mapper callers too. The COW clone shares all maps
        // until the first replacement, and the complete candidate is installed only after every
        // capacity/invariant check succeeds.
        let mut candidate = self.clone();
        let revision = candidate.next_revision()?;
        for event_id in &event_ids {
            let mut replacement = candidate
                .pending_events
                .get(event_id)
                .cloned()
                .ok_or_else(|| ProtocolError::conflict("A2A retry event disappeared"))?;
            replacement.next_attempt_at_unix_ms = Some(latest_allowed);
            replacement.updated_revision = revision;
            candidate.replace_pending_event(event_id, replacement)?;
        }
        candidate.revision = revision;
        *self = candidate;
        Ok(event_ids.len())
    }

    /// Whether exactly one immutable acceptance event is bound to this dispatch and is settled.
    /// The derived index makes the live duplicate and startup recovery gates share one O(log N)
    /// fail-closed predicate.
    pub fn dispatch_event_ready(&self, record: &A2aDispatchOutboxRecord) -> bool {
        self.dispatch_event_readiness
            .get(&A2aDispatchEventBinding::from_dispatch(record))
            == Some(&(1, 1))
    }

    /// Durable dead-letter records retained for operator reconciliation and audit.
    pub fn quarantined_events(&self) -> Vec<A2aPendingEventIntent> {
        let mut quarantined: Vec<_> = self
            .pending_events
            .values()
            .filter(|event| event.state == A2aPendingEventState::Quarantined)
            .cloned()
            .collect();
        quarantined.sort_by(|left, right| {
            left.source_revision
                .cmp(&right.source_revision)
                .then_with(|| left.event_id.cmp(&right.event_id))
        });
        quarantined
    }

    pub fn mark_dispatch_running(&mut self, dispatch_id: &str) -> ProtocolResult<()> {
        let current = self
            .dispatch_outbox
            .get(dispatch_id)
            .cloned()
            .ok_or_else(|| ProtocolError::not_found("A2A dispatch is not registered"))?;
        if !matches!(
            current.state,
            A2aDispatchOutboxState::Queued | A2aDispatchOutboxState::ReconcilePending
        ) {
            return Err(ProtocolError::invalid_transition(
                "A2A dispatch can only run from queued or reconcile-pending state",
            ));
        }
        let task = self
            .tasks
            .get(&current.task_id)
            .ok_or_else(|| ProtocolError::conflict("A2A dispatch task is not registered"))?;
        if task.state.is_terminal()
            || matches!(
                task.state,
                A2aTaskState::InputRequired | A2aTaskState::AuthRequired
            )
        {
            return Err(ProtocolError::invalid_transition(
                "A2A dispatch task is not runnable",
            ));
        }
        let attempts = current
            .attempts
            .checked_add(1)
            .filter(|attempts| *attempts <= A2A_MAX_DISPATCH_ATTEMPTS)
            .ok_or_else(|| ProtocolError::conflict("A2A dispatch attempt limit is exhausted"))?;
        let revision = self.next_revision()?;
        let mut replacement = current;
        replacement.state = A2aDispatchOutboxState::Running;
        replacement.attempts = attempts;
        replacement.last_error = None;
        replacement.updated_revision = revision;
        self.replace_dispatch_record(dispatch_id, replacement)?;
        self.revision = revision;
        Ok(())
    }

    pub fn mark_dispatch_reconcile_pending(
        &mut self,
        dispatch_id: &str,
        _error: impl AsRef<str>,
    ) -> ProtocolResult<()> {
        let current = self
            .dispatch_outbox
            .get(dispatch_id)
            .cloned()
            .ok_or_else(|| ProtocolError::not_found("A2A dispatch is not registered"))?;
        if !matches!(
            current.state,
            A2aDispatchOutboxState::Queued | A2aDispatchOutboxState::Running
        ) {
            return Err(ProtocolError::invalid_transition(
                "A2A dispatch cannot enter reconcile-pending from its current state",
            ));
        }
        let revision = self.next_revision()?;
        let mut replacement = current;
        replacement.state = A2aDispatchOutboxState::ReconcilePending;
        // Never persist a raw host/wire error: it can contain bearer tokens or provider payloads.
        replacement.last_error = Some(A2A_RECONCILE_REASON.into());
        replacement.updated_revision = revision;
        self.replace_dispatch_record(dispatch_id, replacement)?;
        self.revision = revision;
        Ok(())
    }

    pub fn mark_dispatch_settled(&mut self, dispatch_id: &str) -> ProtocolResult<()> {
        let current = self
            .dispatch_outbox
            .get(dispatch_id)
            .cloned()
            .ok_or_else(|| ProtocolError::not_found("A2A dispatch is not registered"))?;
        if current.state == A2aDispatchOutboxState::Settled {
            return Ok(());
        }
        if !matches!(
            current.state,
            A2aDispatchOutboxState::Queued
                | A2aDispatchOutboxState::Running
                | A2aDispatchOutboxState::ReconcilePending
        ) {
            return Err(ProtocolError::invalid_transition(
                "A2A dispatch cannot be settled from its current state",
            ));
        }
        let revision = self.next_revision()?;
        let mut replacement = current;
        replacement.state = A2aDispatchOutboxState::Settled;
        replacement.last_error = None;
        replacement.updated_revision = revision;
        self.replace_dispatch_record(dispatch_id, replacement)?;
        self.revision = revision;
        Ok(())
    }

    pub fn mark_cancellation_running(&mut self, cancellation_id: &str) -> ProtocolResult<()> {
        let current = self
            .cancellation_outbox
            .get(cancellation_id)
            .cloned()
            .ok_or_else(|| ProtocolError::not_found("A2A cancellation is not registered"))?;
        if !matches!(
            current.state,
            A2aCancellationOutboxState::Queued | A2aCancellationOutboxState::ReconcilePending
        ) {
            return Err(ProtocolError::invalid_transition(
                "A2A cancellation can only run from queued or reconcile-pending state",
            ));
        }
        let task = self
            .tasks
            .get(&current.task_id)
            .ok_or_else(|| ProtocolError::conflict("A2A cancellation task is not registered"))?;
        if task.state.is_terminal() {
            return Err(ProtocolError::invalid_transition(
                "A2A cancellation task is already terminal",
            ));
        }
        let attempts = current
            .attempts
            .checked_add(1)
            .filter(|attempts| *attempts <= A2A_MAX_CANCELLATION_ATTEMPTS)
            .ok_or_else(|| {
                ProtocolError::conflict("A2A cancellation attempt limit is exhausted")
            })?;
        let revision = self.next_revision()?;
        let mut replacement = current;
        replacement.state = A2aCancellationOutboxState::Running;
        replacement.attempts = attempts;
        replacement.last_error = None;
        replacement.updated_revision = revision;
        self.replace_cancellation_record(cancellation_id, replacement)?;
        self.revision = revision;
        Ok(())
    }

    pub fn mark_cancellation_reconcile_pending(
        &mut self,
        cancellation_id: &str,
        _error: impl AsRef<str>,
    ) -> ProtocolResult<()> {
        let current = self
            .cancellation_outbox
            .get(cancellation_id)
            .cloned()
            .ok_or_else(|| ProtocolError::not_found("A2A cancellation is not registered"))?;
        if !matches!(
            current.state,
            A2aCancellationOutboxState::Queued | A2aCancellationOutboxState::Running
        ) {
            return Err(ProtocolError::invalid_transition(
                "A2A cancellation cannot enter reconcile-pending from its current state",
            ));
        }
        let revision = self.next_revision()?;
        let mut replacement = current;
        replacement.state = A2aCancellationOutboxState::ReconcilePending;
        // Persist only this fixed category; raw host errors can contain tokens or payloads.
        replacement.last_error = Some(A2A_CANCELLATION_RECONCILE_REASON.into());
        replacement.updated_revision = revision;
        self.replace_cancellation_record(cancellation_id, replacement)?;
        self.revision = revision;
        Ok(())
    }

    pub fn mark_cancellation_settled(&mut self, cancellation_id: &str) -> ProtocolResult<()> {
        let current = self
            .cancellation_outbox
            .get(cancellation_id)
            .cloned()
            .ok_or_else(|| ProtocolError::not_found("A2A cancellation is not registered"))?;
        if current.state == A2aCancellationOutboxState::Settled {
            return Ok(());
        }
        let revision = self.next_revision()?;
        let mut replacement = current;
        replacement.state = A2aCancellationOutboxState::Settled;
        replacement.last_error = None;
        replacement.updated_revision = revision;
        self.replace_cancellation_record(cancellation_id, replacement)?;
        self.revision = revision;
        Ok(())
    }

    /// Record one sanitized transient publication failure with a durable exponential retry gate.
    /// Backend availability failures never consume the deterministic poison budget and therefore
    /// cannot permanently quarantine an accepted event. Raw host error text is never persisted.
    pub fn mark_event_reconcile_pending(
        &mut self,
        event_id: &str,
        _error: impl AsRef<str>,
        failed_at_unix_ms: u64,
    ) -> ProtocolResult<A2aPendingEventState> {
        let current = self
            .pending_events
            .get(event_id)
            .cloned()
            .ok_or_else(|| ProtocolError::not_found("A2A event intent is not registered"))?;
        if matches!(
            current.state,
            A2aPendingEventState::Settled | A2aPendingEventState::Quarantined
        ) {
            return Err(ProtocolError::invalid_transition(
                "terminal A2A event intent cannot be reconciled",
            ));
        }
        let transient_failures = current.transient_failures.saturating_add(1);
        let retry_exponent = transient_failures.saturating_sub(1).min(31);
        let retry_delay_ms = A2A_EVENT_RETRY_BASE_MS
            .checked_shl(retry_exponent)
            .unwrap_or(u64::MAX)
            .min(A2A_EVENT_RETRY_MAX_MS);
        let next_attempt_at_unix_ms = failed_at_unix_ms.saturating_add(retry_delay_ms);
        let revision = self.next_revision()?;
        let mut replacement = current;
        replacement.state = A2aPendingEventState::ReconcilePending;
        replacement.transient_failures = transient_failures;
        replacement.next_attempt_at_unix_ms = Some(next_attempt_at_unix_ms);
        replacement.last_error = Some(A2A_EVENT_RECONCILE_REASON.into());
        replacement.quarantine_reason = None;
        replacement.updated_revision = revision;
        self.replace_pending_event(event_id, replacement)?;
        self.revision = revision;
        Ok(A2aPendingEventState::ReconcilePending)
    }

    /// Move a deterministic poison event to the durable dead-letter set without storing raw
    /// provider or wire error text. Attempt exhaustion is normally handled automatically by
    /// [`Self::mark_event_reconcile_pending`].
    pub fn mark_event_quarantined(
        &mut self,
        event_id: &str,
        reason: A2aEventQuarantineReason,
    ) -> ProtocolResult<()> {
        let current = self
            .pending_events
            .get(event_id)
            .cloned()
            .ok_or_else(|| ProtocolError::not_found("A2A event intent is not registered"))?;
        if current.state == A2aPendingEventState::Settled {
            return Err(ProtocolError::invalid_transition(
                "settled A2A event intent cannot be quarantined",
            ));
        }
        if current.state == A2aPendingEventState::Quarantined {
            return if current.quarantine_reason == Some(reason) {
                Ok(())
            } else {
                Err(ProtocolError::invalid_transition(
                    "quarantined A2A event reason cannot be replaced",
                ))
            };
        }
        if reason == A2aEventQuarantineReason::AttemptsExhausted
            && current.attempts != A2A_MAX_EVENT_ATTEMPTS
        {
            return Err(ProtocolError::invalid_transition(
                "A2A event attempts are not exhausted",
            ));
        }
        let revision = self.next_revision()?;
        let mut replacement = current;
        replacement.state = A2aPendingEventState::Quarantined;
        replacement.transient_failures = 0;
        replacement.next_attempt_at_unix_ms = None;
        replacement.last_error = Some(reason.diagnostic().into());
        replacement.quarantine_reason = Some(reason);
        replacement.updated_revision = revision;
        self.replace_pending_event(event_id, replacement)?;
        self.revision = revision;
        Ok(())
    }

    pub fn mark_event_settled(&mut self, event_id: &str) -> ProtocolResult<()> {
        let current = self
            .pending_events
            .get(event_id)
            .cloned()
            .ok_or_else(|| ProtocolError::not_found("A2A event intent is not registered"))?;
        if current.state == A2aPendingEventState::Settled {
            return Ok(());
        }
        if current.state == A2aPendingEventState::Quarantined {
            return Err(ProtocolError::invalid_transition(
                "quarantined A2A event intent cannot be falsely settled",
            ));
        }
        let revision = self.next_revision()?;
        let mut replacement = current;
        replacement.state = A2aPendingEventState::Settled;
        replacement.transient_failures = 0;
        replacement.next_attempt_at_unix_ms = None;
        replacement.last_error = None;
        replacement.quarantine_reason = None;
        replacement.updated_revision = revision;
        self.replace_pending_event(event_id, replacement)?;
        self.revision = revision;
        Ok(())
    }

    pub fn schema_version(&self) -> u32 {
        self.schema_version
    }

    pub fn revision(&self) -> u64 {
        self.revision
    }

    /// Validate a message and all global/owner capacity fences without cloning mapper state.
    /// Exact content-bound duplicates remain admissible at capacity or while cancellation is
    /// unsettled because they only return the existing durable receipt; they never create or
    /// reschedule task, dispatch, receipt, event, or revision state.
    pub fn preflight_send_message(
        &self,
        message: &A2aMessage,
        principal: &ProtocolPrincipal,
    ) -> ProtocolResult<()> {
        message.validate()?;
        let receipt_key = scoped_receipt_key(principal, &message.message_id);
        if let Some(receipt) = self.receipts.get(&receipt_key) {
            return if receipt.message == *message {
                Ok(())
            } else {
                Err(ProtocolError::conflict(
                    "A2A message_id was reused with different content",
                ))
            };
        }

        if let Some(task_id) = message.task_id.as_deref() {
            let task = self
                .tasks
                .get(task_id)
                .filter(|task| {
                    principal.matches_identity(&task.owner_subject, task.owner_tenant_id.as_deref())
                })
                .ok_or_else(|| ProtocolError::not_found("A2A task is not accessible"))?;
            if message
                .context_id
                .as_ref()
                .is_some_and(|context_id| context_id != &task.mapping.context_id)
            {
                return Err(ProtocolError::conflict(
                    "A2A context_id does not match task_id",
                ));
            }
            if self.cancellation_outbox.values().any(|record| {
                record.task_id == task_id && record.state != A2aCancellationOutboxState::Settled
            }) {
                return Err(ProtocolError::invalid_transition(
                    A2A_UNSETTLED_CANCELLATION_SEND_REASON,
                ));
            }
            if self.pending_events.values().any(|event| {
                event.task_id == task_id
                    && event.message_id.is_some()
                    && event.state != A2aPendingEventState::Settled
            }) {
                return Err(ProtocolError::invalid_transition(
                    A2A_UNSETTLED_MESSAGE_EVENT_SEND_REASON,
                ));
            }
            if self.dispatch_outbox.values().any(|record| {
                record.task_id == task_id && record.state != A2aDispatchOutboxState::Settled
            }) {
                return Err(ProtocolError::invalid_transition(
                    A2A_UNSETTLED_MESSAGE_DISPATCH_SEND_REASON,
                ));
            }
            if task.state.is_terminal() {
                return Err(ProtocolError::invalid_transition(
                    "terminal A2A task cannot accept another message",
                ));
            }
        }

        let (projected_receipt_key, projected_receipt) =
            self.projected_receipt(message, principal)?;
        let added_receipt_bytes =
            receipt_entry_storage_bytes(&projected_receipt_key, &projected_receipt)?;
        ensure_count_capacity(self.receipts.len(), A2A_MAX_RECEIPTS, "receipt")?;
        ensure_byte_capacity(
            self.receipt_bytes,
            added_receipt_bytes,
            A2A_MAX_RECEIPT_BYTES,
            "receipt",
        )?;

        let owner_receipts = self
            .receipts
            .values()
            .filter(|receipt| receipt_owner_matches(receipt, principal));
        let mut owner_receipt_count = 0_usize;
        let mut owner_receipt_bytes = 0_usize;
        for receipt in owner_receipts {
            owner_receipt_count = owner_receipt_count.saturating_add(1);
            let owner = A2aContextOwner {
                subject: receipt.owner_subject.clone(),
                tenant_id: receipt.owner_tenant_id.clone(),
            };
            let key = scoped_receipt_key_for_owner(&owner, &receipt.message.message_id);
            owner_receipt_bytes = owner_receipt_bytes
                .checked_add(receipt_entry_storage_bytes(&key, receipt)?)
                .ok_or_else(|| ProtocolError::conflict("A2A owner receipt bytes overflowed"))?;
        }
        ensure_count_capacity(
            owner_receipt_count,
            A2A_MAX_RECEIPTS_PER_OWNER,
            "owner receipt",
        )?;
        ensure_byte_capacity(
            owner_receipt_bytes,
            added_receipt_bytes,
            A2A_MAX_RECEIPT_BYTES_PER_OWNER,
            "owner receipt",
        )?;

        if message.task_id.is_none() {
            ensure_count_capacity(self.tasks.len(), A2A_MAX_TASKS, "task")?;
            ensure_count_capacity(
                self.tasks
                    .values()
                    .filter(|task| task_owner_matches(task, principal))
                    .count(),
                A2A_MAX_TASKS_PER_OWNER,
                "owner task",
            )?;
            let creates_context = message.context_id.as_deref().is_none_or(|context_id| {
                !self
                    .contexts
                    .contains_key(&scoped_context_key(principal, context_id))
            });
            if creates_context {
                ensure_count_capacity(self.contexts.len(), A2A_MAX_CONTEXTS, "context")?;
                ensure_count_capacity(
                    self.context_owners
                        .values()
                        .filter(|owner| owner.matches(principal))
                        .count(),
                    A2A_MAX_CONTEXTS_PER_OWNER,
                    "owner context",
                )?;
            }
        }
        Ok(())
    }

    fn projected_receipt(
        &self,
        message: &A2aMessage,
        principal: &ProtocolPrincipal,
    ) -> ProtocolResult<(String, A2aMessageReceipt)> {
        let (mapping, revision_steps) = if let Some(task_id) = message.task_id.as_deref() {
            let task = self
                .tasks
                .get(task_id)
                .filter(|task| task_owner_matches(task, principal))
                .ok_or_else(|| ProtocolError::not_found("A2A task is not accessible"))?;
            let mut mapping = task.mapping.clone();
            mapping.message_id = message.message_id.clone();
            let revision_steps = if matches!(
                task.state,
                A2aTaskState::InputRequired | A2aTaskState::AuthRequired
            ) {
                2
            } else {
                1
            };
            (mapping, revision_steps)
        } else {
            let context_id = message.context_id.clone().unwrap_or_else(|| {
                // The real random context has the same fixed ASCII byte length, so this produces
                // exact serialized receipt accounting without consuming randomness in preflight.
                format!("a2a-context-random-{}", "0".repeat(32))
            });
            let context_key = scoped_context_key(principal, &context_id);
            let mut generated_offset = 0_u64;
            let session_id = if let Some(session_id) = self.contexts.get(&context_key) {
                session_id.clone()
            } else {
                let session_id = self.preview_id("a2a-session", generated_offset)?;
                generated_offset += 1;
                session_id
            };
            let task_id = self.preview_id("a2a-task", generated_offset)?;
            generated_offset += 1;
            let run_id = self.preview_id("a2a-run", generated_offset)?;
            (
                A2aRunMapping {
                    context_id,
                    session_id,
                    task_id,
                    run_id,
                    message_id: message.message_id.clone(),
                },
                2,
            )
        };
        let accepted_revision = self
            .revision
            .checked_add(revision_steps)
            .filter(|revision| *revision <= A2A_MAX_SAFE_INTEGER)
            .ok_or_else(|| {
                ProtocolError::conflict(
                    "A2A mapper revision reached the cross-language integer limit",
                )
            })?;
        let receipt = A2aMessageReceipt {
            message: message.clone(),
            mapping,
            owner_subject: principal.subject.clone(),
            owner_tenant_id: principal.tenant_id.clone(),
            accepted_revision,
        };
        Ok((scoped_receipt_key(principal, &message.message_id), receipt))
    }

    fn insert_new_dispatch(&mut self, record: A2aDispatchOutboxRecord) -> ProtocolResult<()> {
        if self.dispatch_outbox.contains_key(&record.dispatch_id) {
            return Err(ProtocolError::conflict(
                "A2A dispatch identity is already registered",
            ));
        }
        ensure_count_capacity(self.dispatch_outbox.len(), A2A_MAX_DISPATCHES, "dispatch")?;
        let owner = dispatch_owner(&record);
        ensure_count_capacity(
            self.dispatch_outbox
                .values()
                .filter(|current| dispatch_owner(current) == owner)
                .count(),
            A2A_MAX_DISPATCHES_PER_OWNER,
            "owner dispatch",
        )?;
        let added = dispatch_entry_storage_bytes(&record.dispatch_id, &record)?;
        ensure_byte_capacity(
            self.dispatch_bytes,
            added,
            A2A_MAX_DISPATCH_BYTES,
            "dispatch",
        )?;
        let owner_bytes = dispatch_bytes_for_owner(&self.dispatch_outbox, &owner)?;
        ensure_byte_capacity(
            owner_bytes,
            added,
            A2A_MAX_DISPATCH_BYTES_PER_OWNER,
            "owner dispatch",
        )?;
        self.dispatch_bytes = self
            .dispatch_bytes
            .checked_add(added)
            .expect("A2A dispatch byte capacity was checked");
        self.dispatch_outbox
            .insert(record.dispatch_id.clone(), record);
        Ok(())
    }

    fn insert_new_cancellation(
        &mut self,
        record: A2aCancellationOutboxRecord,
    ) -> ProtocolResult<()> {
        self.preflight_new_cancellation(&record)?;
        let added = cancellation_entry_storage_bytes(&record.cancellation_id, &record)?;
        self.cancellation_bytes = self
            .cancellation_bytes
            .checked_add(added)
            .expect("A2A cancellation byte capacity was checked");
        self.cancellation_outbox
            .insert(record.cancellation_id.clone(), record);
        Ok(())
    }

    fn preflight_new_cancellation(
        &self,
        record: &A2aCancellationOutboxRecord,
    ) -> ProtocolResult<()> {
        if self
            .cancellation_outbox
            .contains_key(&record.cancellation_id)
        {
            return Err(ProtocolError::conflict(
                "A2A cancellation identity is already registered",
            ));
        }
        ensure_count_capacity(
            self.cancellation_outbox.len(),
            A2A_MAX_CANCELLATIONS,
            "cancellation",
        )?;
        let owner = cancellation_owner(record);
        ensure_count_capacity(
            self.cancellation_outbox
                .values()
                .filter(|current| cancellation_owner(current) == owner)
                .count(),
            A2A_MAX_CANCELLATIONS_PER_OWNER,
            "owner cancellation",
        )?;
        let added = cancellation_entry_storage_bytes(&record.cancellation_id, record)?;
        ensure_byte_capacity(
            self.cancellation_bytes,
            added,
            A2A_MAX_CANCELLATION_BYTES,
            "cancellation",
        )?;
        let owner_bytes = cancellation_bytes_for_owner(&self.cancellation_outbox, &owner)?;
        ensure_byte_capacity(
            owner_bytes,
            added,
            A2A_MAX_CANCELLATION_BYTES_PER_OWNER,
            "owner cancellation",
        )?;
        Ok(())
    }

    fn insert_new_pending_event(&mut self, event: A2aPendingEventIntent) -> ProtocolResult<()> {
        self.preflight_new_pending_event(&event)?;
        let added = pending_event_entry_storage_bytes(&event.event_id, &event)?;
        self.pending_event_bytes = self
            .pending_event_bytes
            .checked_add(added)
            .expect("A2A pending event byte capacity was checked");
        if let Some(schedule_key) = A2aPendingEventScheduleKey::from_event(&event) {
            self.pending_event_schedule.insert(schedule_key.clone());
            self.pending_event_schedule_by_owner
                .entry(event_owner(&event))
                .or_default()
                .insert(schedule_key);
        }
        add_dispatch_event_readiness(&mut self.dispatch_event_readiness, &event);
        self.pending_events.insert(event.event_id.clone(), event);
        Ok(())
    }

    fn preflight_new_pending_event(&self, event: &A2aPendingEventIntent) -> ProtocolResult<()> {
        if self.pending_events.contains_key(&event.event_id) {
            return Err(ProtocolError::conflict(
                "A2A event logical identity is already registered",
            ));
        }
        ensure_count_capacity(
            self.pending_events.len(),
            A2A_MAX_PENDING_EVENTS,
            "pending event",
        )?;
        let owner = event_owner(event);
        ensure_count_capacity(
            self.pending_events
                .values()
                .filter(|current| event_owner(current) == owner)
                .count(),
            A2A_MAX_PENDING_EVENTS_PER_OWNER,
            "owner pending event",
        )?;
        let added = pending_event_entry_storage_bytes(&event.event_id, event)?;
        ensure_byte_capacity(
            self.pending_event_bytes,
            added,
            A2A_MAX_PENDING_EVENT_BYTES,
            "pending event",
        )?;
        let owner_bytes = pending_event_bytes_for_owner(&self.pending_events, &owner)?;
        ensure_byte_capacity(
            owner_bytes,
            added,
            A2A_MAX_PENDING_EVENT_BYTES_PER_OWNER,
            "owner pending event",
        )?;
        Ok(())
    }

    fn replace_dispatch_record(
        &mut self,
        dispatch_id: &str,
        replacement: A2aDispatchOutboxRecord,
    ) -> ProtocolResult<()> {
        let current = self
            .dispatch_outbox
            .get(dispatch_id)
            .ok_or_else(|| ProtocolError::not_found("A2A dispatch is not registered"))?;
        if replacement.dispatch_id != dispatch_id
            || dispatch_owner(&replacement) != dispatch_owner(current)
            || replacement.message_id != current.message_id
            || replacement.task_id != current.task_id
            || replacement.context_id != current.context_id
            || replacement.session_id != current.session_id
            || replacement.run_id != current.run_id
            || replacement.message != current.message
            || replacement.envelope != current.envelope
            || replacement.created_revision != current.created_revision
        {
            return Err(ProtocolError::conflict(
                "A2A dispatch replacement changed immutable identity",
            ));
        }
        let old_bytes = dispatch_entry_storage_bytes(dispatch_id, current)?;
        let new_bytes = dispatch_entry_storage_bytes(dispatch_id, &replacement)?;
        let next_bytes = self
            .dispatch_bytes
            .checked_sub(old_bytes)
            .and_then(|bytes| bytes.checked_add(new_bytes))
            .ok_or_else(|| ProtocolError::conflict("A2A dispatch bytes overflowed"))?;
        if next_bytes > A2A_MAX_DISPATCH_BYTES {
            return Err(ProtocolError::conflict(
                "A2A dispatch byte capacity is exhausted",
            ));
        }
        let owner = dispatch_owner(current);
        let owner_bytes = dispatch_bytes_for_owner(&self.dispatch_outbox, &owner)?;
        let next_owner_bytes = owner_bytes
            .checked_sub(old_bytes)
            .and_then(|bytes| bytes.checked_add(new_bytes))
            .ok_or_else(|| ProtocolError::conflict("A2A owner dispatch bytes overflowed"))?;
        if next_owner_bytes > A2A_MAX_DISPATCH_BYTES_PER_OWNER {
            return Err(ProtocolError::conflict(
                "A2A owner dispatch byte capacity is exhausted",
            ));
        }
        self.dispatch_bytes = next_bytes;
        self.dispatch_outbox
            .insert(dispatch_id.to_owned(), replacement);
        Ok(())
    }

    fn replace_cancellation_record(
        &mut self,
        cancellation_id: &str,
        replacement: A2aCancellationOutboxRecord,
    ) -> ProtocolResult<()> {
        let current = self
            .cancellation_outbox
            .get(cancellation_id)
            .ok_or_else(|| ProtocolError::not_found("A2A cancellation is not registered"))?;
        if replacement.cancellation_id != cancellation_id
            || cancellation_owner(&replacement) != cancellation_owner(current)
            || replacement.task_id != current.task_id
            || replacement.context_id != current.context_id
            || replacement.session_id != current.session_id
            || replacement.run_id != current.run_id
            || replacement.task != current.task
            || replacement.envelope != current.envelope
            || replacement.created_revision != current.created_revision
        {
            return Err(ProtocolError::conflict(
                "A2A cancellation replacement changed immutable identity",
            ));
        }
        let old_bytes = cancellation_entry_storage_bytes(cancellation_id, current)?;
        let new_bytes = cancellation_entry_storage_bytes(cancellation_id, &replacement)?;
        let next_bytes = self
            .cancellation_bytes
            .checked_sub(old_bytes)
            .and_then(|bytes| bytes.checked_add(new_bytes))
            .ok_or_else(|| ProtocolError::conflict("A2A cancellation bytes overflowed"))?;
        if next_bytes > A2A_MAX_CANCELLATION_BYTES {
            return Err(ProtocolError::conflict(
                "A2A cancellation byte capacity is exhausted",
            ));
        }
        let owner = cancellation_owner(current);
        let owner_bytes = cancellation_bytes_for_owner(&self.cancellation_outbox, &owner)?;
        let next_owner_bytes = owner_bytes
            .checked_sub(old_bytes)
            .and_then(|bytes| bytes.checked_add(new_bytes))
            .ok_or_else(|| ProtocolError::conflict("A2A owner cancellation bytes overflowed"))?;
        if next_owner_bytes > A2A_MAX_CANCELLATION_BYTES_PER_OWNER {
            return Err(ProtocolError::conflict(
                "A2A owner cancellation byte capacity is exhausted",
            ));
        }
        self.cancellation_bytes = next_bytes;
        self.cancellation_outbox
            .insert(cancellation_id.to_owned(), replacement);
        Ok(())
    }

    fn replace_pending_event(
        &mut self,
        event_id: &str,
        replacement: A2aPendingEventIntent,
    ) -> ProtocolResult<()> {
        let current = self
            .pending_events
            .get(event_id)
            .ok_or_else(|| ProtocolError::not_found("A2A event intent is not registered"))?;
        if replacement.event_id != event_id
            || event_owner(&replacement) != event_owner(current)
            || replacement.task_id != current.task_id
            || replacement.context_id != current.context_id
            || replacement.session_id != current.session_id
            || replacement.run_id != current.run_id
            || replacement.source_revision != current.source_revision
            || replacement.kind != current.kind
            || replacement.message_id != current.message_id
            || replacement.payload_hash != current.payload_hash
            || replacement.task != current.task
            || replacement.created_revision != current.created_revision
        {
            return Err(ProtocolError::conflict(
                "A2A event replacement changed immutable identity",
            ));
        }
        let old_bytes = pending_event_entry_storage_bytes(event_id, current)?;
        let new_bytes = pending_event_entry_storage_bytes(event_id, &replacement)?;
        let next_bytes = self
            .pending_event_bytes
            .checked_sub(old_bytes)
            .and_then(|bytes| bytes.checked_add(new_bytes))
            .ok_or_else(|| ProtocolError::conflict("A2A pending event bytes overflowed"))?;
        if next_bytes > A2A_MAX_PENDING_EVENT_BYTES {
            return Err(ProtocolError::conflict(
                "A2A pending event byte capacity is exhausted",
            ));
        }
        let owner = event_owner(current);
        let owner_bytes = pending_event_bytes_for_owner(&self.pending_events, &owner)?;
        let next_owner_bytes = owner_bytes
            .checked_sub(old_bytes)
            .and_then(|bytes| bytes.checked_add(new_bytes))
            .ok_or_else(|| ProtocolError::conflict("A2A owner event bytes overflowed"))?;
        if next_owner_bytes > A2A_MAX_PENDING_EVENT_BYTES_PER_OWNER {
            return Err(ProtocolError::conflict(
                "A2A owner pending event byte capacity is exhausted",
            ));
        }
        let old_schedule_key = A2aPendingEventScheduleKey::from_event(current);
        let new_schedule_key = A2aPendingEventScheduleKey::from_event(&replacement);
        let owner = event_owner(current);
        self.pending_event_bytes = next_bytes;
        if let Some(schedule_key) = old_schedule_key {
            self.pending_event_schedule.remove(&schedule_key);
            if let Some(owner_schedule) = self.pending_event_schedule_by_owner.get_mut(&owner) {
                owner_schedule.remove(&schedule_key);
                if owner_schedule.is_empty() {
                    self.pending_event_schedule_by_owner.remove(&owner);
                }
            }
        }
        if let Some(schedule_key) = new_schedule_key {
            self.pending_event_schedule.insert(schedule_key.clone());
            self.pending_event_schedule_by_owner
                .entry(owner)
                .or_default()
                .insert(schedule_key);
        }
        remove_dispatch_event_readiness(&mut self.dispatch_event_readiness, current);
        add_dispatch_event_readiness(&mut self.dispatch_event_readiness, &replacement);
        self.pending_events.insert(event_id.to_owned(), replacement);
        Ok(())
    }

    fn settle_task_dispatches_at_revision(
        &mut self,
        task_id: &str,
        revision: u64,
    ) -> ProtocolResult<()> {
        let replacements: Vec<_> = self
            .dispatch_outbox
            .iter()
            .filter(|(_, record)| {
                record.task_id == task_id && record.state != A2aDispatchOutboxState::Settled
            })
            .map(|(dispatch_id, record)| {
                let mut replacement = record.clone();
                replacement.state = A2aDispatchOutboxState::Settled;
                replacement.last_error = None;
                replacement.updated_revision = revision;
                (dispatch_id.clone(), replacement)
            })
            .collect();
        let mut next_bytes = self.dispatch_bytes;
        let mut owner_bytes: BTreeMap<A2aContextOwner, usize> = BTreeMap::new();
        for (dispatch_id, replacement) in &replacements {
            let current = self
                .dispatch_outbox
                .get(dispatch_id)
                .expect("A2A dispatch replacement was collected from the map");
            let owner = dispatch_owner(current);
            let current_owner_bytes = match owner_bytes.get(&owner) {
                Some(bytes) => *bytes,
                None => dispatch_bytes_for_owner(&self.dispatch_outbox, &owner)?,
            };
            let old_bytes = dispatch_entry_storage_bytes(dispatch_id, current)?;
            let new_bytes = dispatch_entry_storage_bytes(dispatch_id, replacement)?;
            next_bytes = next_bytes
                .checked_sub(old_bytes)
                .and_then(|bytes| bytes.checked_add(new_bytes))
                .ok_or_else(|| ProtocolError::conflict("A2A dispatch bytes overflowed"))?;
            let next_owner_bytes = current_owner_bytes
                .checked_sub(old_bytes)
                .and_then(|bytes| bytes.checked_add(new_bytes))
                .ok_or_else(|| ProtocolError::conflict("A2A owner dispatch bytes overflowed"))?;
            owner_bytes.insert(owner, next_owner_bytes);
        }
        if next_bytes > A2A_MAX_DISPATCH_BYTES
            || owner_bytes
                .values()
                .any(|bytes| *bytes > A2A_MAX_DISPATCH_BYTES_PER_OWNER)
        {
            return Err(ProtocolError::conflict(
                "A2A dispatch byte capacity cannot represent settlement",
            ));
        }
        self.dispatch_bytes = next_bytes;
        for (dispatch_id, replacement) in replacements {
            self.dispatch_outbox.insert(dispatch_id, replacement);
        }
        Ok(())
    }

    fn settle_task_cancellation_at_revision(
        &mut self,
        task_id: &str,
        revision: u64,
    ) -> ProtocolResult<()> {
        let Some((cancellation_id, current)) = self
            .cancellation_outbox
            .iter()
            .find(|(_, record)| record.task_id == task_id)
            .map(|(id, record)| (id.clone(), record.clone()))
        else {
            return Ok(());
        };
        if current.state == A2aCancellationOutboxState::Settled {
            return Ok(());
        }
        let mut replacement = current;
        replacement.state = A2aCancellationOutboxState::Settled;
        replacement.last_error = None;
        replacement.updated_revision = revision;
        self.replace_cancellation_record(&cancellation_id, replacement)
    }

    fn preview_id(&self, prefix: &str, offset: u64) -> ProtocolResult<String> {
        let sequence = self
            .next_sequence
            .checked_add(offset)
            .filter(|sequence| *sequence < A2A_MAX_SAFE_INTEGER)
            .ok_or_else(|| {
                ProtocolError::conflict(
                    "A2A mapper identifier reached the cross-language integer limit",
                )
            })?;
        Ok(format!("{prefix}-{sequence:016}"))
    }

    fn next_revision(&self) -> ProtocolResult<u64> {
        self.revision
            .checked_add(1)
            .filter(|revision| *revision <= A2A_MAX_SAFE_INTEGER)
            .ok_or_else(|| {
                ProtocolError::conflict(
                    "A2A mapper revision reached the cross-language integer limit",
                )
            })
    }

    fn validate_snapshot(&self) -> ProtocolResult<()> {
        if self.schema_version != A2A_MAPPER_SCHEMA_VERSION {
            return Err(ProtocolError::invalid(format!(
                "unsupported A2A mapper schema version {}",
                self.schema_version
            )));
        }
        if self.next_sequence == 0 || self.next_sequence > A2A_MAX_SAFE_INTEGER {
            return Err(ProtocolError::invalid(
                "A2A mapper next_sequence is outside the cross-language executable range",
            ));
        }
        if self.revision > A2A_MAX_SAFE_INTEGER {
            return Err(ProtocolError::invalid(
                "A2A mapper revision is outside the cross-language executable range",
            ));
        }
        self.validate_capacity_snapshot()?;
        if self.contexts.len() != self.context_owners.len()
            || self.contexts.keys().ne(self.context_owners.keys())
        {
            return Err(ProtocolError::invalid(
                "A2A context and owner indexes do not match",
            ));
        }

        let mut max_revision = 0_u64;
        let mut max_generated_sequence = 0_u64;
        let mut run_ids = BTreeSet::new();
        let mut session_identities = BTreeMap::new();
        for (task_id, task) in &self.tasks {
            validate_identifier("A2A task index key", task_id)?;
            validate_mapping(&task.mapping)?;
            validate_owner(&task.owner_subject, task.owner_tenant_id.as_deref())?;
            if task.mapping.task_id != *task_id {
                return Err(ProtocolError::invalid(
                    "A2A task index key does not match its mapping",
                ));
            }
            if task.created_revision == 0
                || task.created_revision > task.updated_revision
                || task.updated_revision > self.revision
            {
                return Err(ProtocolError::invalid(
                    "A2A task revision history is invalid",
                ));
            }
            max_revision = max_revision.max(task.updated_revision);
            update_generated_sequence(
                &task.mapping.session_id,
                "a2a-session-",
                &mut max_generated_sequence,
            )?;
            update_generated_sequence(
                &task.mapping.task_id,
                "a2a-task-",
                &mut max_generated_sequence,
            )?;
            update_generated_sequence(
                &task.mapping.run_id,
                "a2a-run-",
                &mut max_generated_sequence,
            )?;
            if !run_ids.insert(task.mapping.run_id.clone()) {
                return Err(ProtocolError::invalid(
                    "A2A run_id must be globally unique across tasks",
                ));
            }
            let session_identity = (
                task.owner_subject.clone(),
                task.owner_tenant_id.clone(),
                task.mapping.context_id.clone(),
            );
            if let Some(existing) = session_identities.get(&task.mapping.session_id) {
                if existing != &session_identity {
                    return Err(ProtocolError::invalid(
                        "A2A session_id cannot cross an owner-scoped context boundary",
                    ));
                }
            } else {
                session_identities.insert(task.mapping.session_id.clone(), session_identity);
            }
            let owner = A2aContextOwner {
                subject: task.owner_subject.clone(),
                tenant_id: task.owner_tenant_id.clone(),
            };
            let context_key = scoped_context_key_for_owner(&owner, &task.mapping.context_id);
            if self.contexts.get(&context_key) != Some(&task.mapping.session_id)
                || self.context_owners.get(&context_key) != Some(&owner)
            {
                return Err(ProtocolError::invalid(
                    "A2A task does not match its owner-scoped context",
                ));
            }
            let first_receipt_key = scoped_receipt_key_for_owner(&owner, &task.mapping.message_id);
            let Some(first_receipt) = self.receipts.get(&first_receipt_key) else {
                return Err(ProtocolError::invalid(
                    "A2A task is missing its initial message receipt",
                ));
            };
            if !mapping_runtime_identity_matches(&task.mapping, &first_receipt.mapping) {
                return Err(ProtocolError::invalid(
                    "A2A task and initial receipt runtime identities differ",
                ));
            }
        }

        for (receipt_key, receipt) in &self.receipts {
            receipt.message.validate()?;
            validate_mapping(&receipt.mapping)?;
            validate_owner(&receipt.owner_subject, receipt.owner_tenant_id.as_deref())?;
            let owner = A2aContextOwner {
                subject: receipt.owner_subject.clone(),
                tenant_id: receipt.owner_tenant_id.clone(),
            };
            if *receipt_key != scoped_receipt_key_for_owner(&owner, &receipt.message.message_id)
                || receipt.mapping.message_id != receipt.message.message_id
            {
                return Err(ProtocolError::invalid(
                    "A2A receipt index or message identity is invalid",
                ));
            }
            if receipt.accepted_revision == 0 || receipt.accepted_revision > self.revision {
                return Err(ProtocolError::invalid("A2A receipt revision is invalid"));
            }
            max_revision = max_revision.max(receipt.accepted_revision);
            let Some(task) = self.tasks.get(&receipt.mapping.task_id) else {
                return Err(ProtocolError::invalid(
                    "A2A receipt references an unknown task",
                ));
            };
            if receipt.accepted_revision < task.created_revision {
                return Err(ProtocolError::invalid(
                    "A2A receipt predates the task it references",
                ));
            }
            if task.owner_subject != receipt.owner_subject
                || task.owner_tenant_id != receipt.owner_tenant_id
                || !mapping_runtime_identity_matches(&task.mapping, &receipt.mapping)
            {
                return Err(ProtocolError::invalid(
                    "A2A receipt does not match its task owner or runtime identity",
                ));
            }
            if receipt
                .message
                .task_id
                .as_ref()
                .is_some_and(|task_id| task_id != &receipt.mapping.task_id)
                || receipt
                    .message
                    .context_id
                    .as_ref()
                    .is_some_and(|context_id| context_id != &receipt.mapping.context_id)
            {
                return Err(ProtocolError::invalid(
                    "A2A receipt message does not match its normalized mapping",
                ));
            }
        }

        if self.dispatch_outbox.len() != self.receipts.len() {
            return Err(ProtocolError::invalid(
                "A2A dispatch outbox and receipt counts do not match",
            ));
        }
        let mut tasks_with_unsettled_dispatches = BTreeSet::new();
        let mut tasks_with_finalized_outputs = BTreeSet::new();
        for (dispatch_id, dispatch) in &self.dispatch_outbox {
            validate_owner(&dispatch.owner_subject, dispatch.owner_tenant_id.as_deref())?;
            validate_identifier("A2A dispatch message_id", &dispatch.message_id)?;
            validate_identifier("A2A dispatch task_id", &dispatch.task_id)?;
            validate_identifier("A2A dispatch context_id", &dispatch.context_id)?;
            validate_identifier("A2A dispatch session_id", &dispatch.session_id)?;
            validate_identifier("A2A dispatch run_id", &dispatch.run_id)?;
            dispatch.message.validate()?;
            let owner = dispatch_owner(dispatch);
            if dispatch.dispatch_id != *dispatch_id
                || *dispatch_id != scoped_dispatch_key_for_owner(&owner, &dispatch.message_id)
            {
                return Err(ProtocolError::invalid(
                    "A2A dispatch index or stable identity is invalid",
                ));
            }
            let receipt_key = scoped_receipt_key_for_owner(&owner, &dispatch.message_id);
            let receipt = self.receipts.get(&receipt_key).ok_or_else(|| {
                ProtocolError::invalid("A2A dispatch is missing its exact receipt")
            })?;
            let task = self
                .tasks
                .get(&dispatch.task_id)
                .ok_or_else(|| ProtocolError::invalid("A2A dispatch references an unknown task"))?;
            if dispatch.state != A2aDispatchOutboxState::Settled
                && !tasks_with_unsettled_dispatches.insert(dispatch.task_id.clone())
            {
                return Err(ProtocolError::invalid(
                    "A2A task has more than one unsettled message dispatch",
                ));
            }
            if receipt.owner_subject != dispatch.owner_subject
                || receipt.owner_tenant_id != dispatch.owner_tenant_id
                || receipt.mapping.message_id != dispatch.message_id
                || receipt.mapping.task_id != dispatch.task_id
                || receipt.mapping.context_id != dispatch.context_id
                || receipt.mapping.session_id != dispatch.session_id
                || receipt.mapping.run_id != dispatch.run_id
                || task.owner_subject != dispatch.owner_subject
                || task.owner_tenant_id != dispatch.owner_tenant_id
                || task.mapping.task_id != dispatch.task_id
                || task.mapping.context_id != dispatch.context_id
                || task.mapping.session_id != dispatch.session_id
                || task.mapping.run_id != dispatch.run_id
            {
                return Err(ProtocolError::invalid(
                    "A2A dispatch owner or runtime identity is invalid",
                ));
            }
            let mut normalized = receipt.message.clone();
            normalized.context_id = Some(dispatch.context_id.clone());
            normalized.task_id = Some(dispatch.task_id.clone());
            if dispatch.message != normalized || dispatch.message.message_id != dispatch.message_id
            {
                return Err(ProtocolError::invalid(
                    "A2A dispatch canonical message does not match its receipt",
                ));
            }
            if dispatch.created_revision != receipt.accepted_revision
                || dispatch.created_revision == 0
                || dispatch.created_revision > dispatch.updated_revision
                || dispatch.updated_revision > self.revision
                || dispatch.attempts > A2A_MAX_DISPATCH_ATTEMPTS
            {
                return Err(ProtocolError::invalid(
                    "A2A dispatch revision or attempt history is invalid",
                ));
            }
            if dispatch.resumed_from.is_some_and(|state| {
                !matches!(
                    state,
                    A2aTaskState::InputRequired | A2aTaskState::AuthRequired
                )
            }) {
                return Err(ProtocolError::invalid(
                    "A2A dispatch resumed_from state is invalid",
                ));
            }
            if (dispatch.response_policy == A2aSendResponsePolicy::Immediate)
                != dispatch.immediate_response.is_some()
            {
                return Err(ProtocolError::invalid(
                    "A2A dispatch response policy and frozen response differ",
                ));
            }
            if let Some(immediate) = &dispatch.immediate_response {
                let acceptance_matches = self
                    .pending_events
                    .values()
                    .filter(|event| {
                        event.task_id == dispatch.task_id
                            && event.message_id.as_deref() == Some(dispatch.message_id.as_str())
                            && event.source_revision == dispatch.created_revision
                            && event.task == *immediate
                    })
                    .count();
                if immediate.owner_subject != dispatch.owner_subject
                    || immediate.owner_tenant_id != dispatch.owner_tenant_id
                    || immediate.mapping.task_id != dispatch.task_id
                    || immediate.mapping.context_id != dispatch.context_id
                    || immediate.mapping.session_id != dispatch.session_id
                    || immediate.mapping.run_id != dispatch.run_id
                    || immediate.created_revision != task.created_revision
                    || immediate.updated_revision >= dispatch.created_revision
                    || immediate.state != A2aTaskState::Working
                    || immediate.status_message.is_some()
                    || acceptance_matches != 1
                {
                    return Err(ProtocolError::invalid(
                        "A2A immediate response is not the exact accepted task snapshot",
                    ));
                }
            }
            let diagnostic_valid = match dispatch.state {
                A2aDispatchOutboxState::Queued => {
                    dispatch.attempts == 0 && dispatch.last_error.is_none()
                }
                A2aDispatchOutboxState::Running => {
                    dispatch.attempts > 0 && dispatch.last_error.is_none()
                }
                A2aDispatchOutboxState::ReconcilePending => {
                    dispatch.last_error.as_deref() == Some(A2A_RECONCILE_REASON)
                }
                A2aDispatchOutboxState::Settled => dispatch.last_error.is_none(),
            };
            if !diagnostic_valid {
                return Err(ProtocolError::invalid(
                    "A2A dispatch state diagnostics are invalid",
                ));
            }
            let finalized_output = match &dispatch.response {
                A2aDispatchResponse::Task {
                    finalized_by_dispatch,
                    artifacts,
                } => {
                    validate_task_artifacts(artifacts, task.state)?;
                    if *finalized_by_dispatch
                        && (dispatch.state != A2aDispatchOutboxState::Settled
                            || task.state != A2aTaskState::Completed
                            || task.updated_revision != dispatch.updated_revision)
                    {
                        return Err(ProtocolError::invalid(
                            "A2A finalized task response is not bound to its completed dispatch",
                        ));
                    }
                    if !*finalized_by_dispatch && !artifacts.is_empty() {
                        return Err(ProtocolError::invalid(
                            "A2A unfinished task response cannot carry artifacts",
                        ));
                    }
                    *finalized_by_dispatch
                }
                A2aDispatchResponse::Message { message } => {
                    message.validate()?;
                    if dispatch.state != A2aDispatchOutboxState::Settled
                        || task.state != A2aTaskState::Completed
                        || task.updated_revision != dispatch.updated_revision
                        || message.role != A2aRole::Agent
                        || message.context_id.as_deref() != Some(dispatch.context_id.as_str())
                        || message.task_id.is_some()
                    {
                        return Err(ProtocolError::invalid(
                            "A2A direct message response is not bound to a settled dispatch",
                        ));
                    }
                    true
                }
            };
            if finalized_output {
                if !tasks_with_finalized_outputs.insert(dispatch.task_id.clone()) {
                    return Err(ProtocolError::invalid(
                        "A2A task has more than one finalized dispatch response",
                    ));
                }
                let latest_message_id = self
                    .receipts
                    .values()
                    .filter(|receipt| receipt.mapping.task_id == dispatch.task_id)
                    .max_by_key(|receipt| receipt.accepted_revision)
                    .map(|receipt| receipt.message.message_id.as_str());
                if latest_message_id != Some(dispatch.message_id.as_str()) {
                    return Err(ProtocolError::invalid(
                        "A2A finalized dispatch response is not bound to the latest task message",
                    ));
                }
            }
            if (task.state.is_terminal()
                || matches!(
                    task.state,
                    A2aTaskState::InputRequired | A2aTaskState::AuthRequired
                ))
                && dispatch.state != A2aDispatchOutboxState::Settled
            {
                return Err(ProtocolError::invalid(
                    "A2A non-runnable task has an unsettled dispatch",
                ));
            }
            let envelope_principal = dispatch.envelope.principal.as_ref().ok_or_else(|| {
                ProtocolError::invalid("A2A dispatch envelope is missing its principal")
            })?;
            let expected_target = receipt
                .message
                .task_id
                .as_ref()
                .unwrap_or(&receipt.message.message_id);
            dispatch.envelope.correlation.validate()?;
            validate_scope_set(&envelope_principal.scopes)?;
            if dispatch.envelope.schema_version != PROTOCOL_CONTRACT_VERSION
                || dispatch.envelope.protocol != ProtocolKind::A2a
                || dispatch.envelope.operation != "message/send"
                || dispatch.envelope.target != *expected_target
                || dispatch.envelope.required_scopes != scopes(&[SEND_MESSAGE_SCOPE])
                || !dispatch.envelope.authorization.is_allowed()
                || !envelope_principal
                    .matches_identity(&dispatch.owner_subject, dispatch.owner_tenant_id.as_deref())
                || !envelope_principal.allows(&scopes(&[SEND_MESSAGE_SCOPE]))
                || dispatch.envelope.correlation.session_id.as_deref()
                    != Some(dispatch.session_id.as_str())
                || dispatch.envelope.correlation.run_id.as_deref() != Some(dispatch.run_id.as_str())
            {
                return Err(ProtocolError::invalid(
                    "A2A dispatch governed envelope binding is invalid",
                ));
            }
            max_revision = max_revision.max(dispatch.updated_revision);
        }

        let mut tasks_with_cancellations = BTreeSet::new();
        for (cancellation_id, cancellation) in &self.cancellation_outbox {
            validate_identifier("A2A cancellation identity", cancellation_id)?;
            validate_owner(
                &cancellation.owner_subject,
                cancellation.owner_tenant_id.as_deref(),
            )?;
            validate_identifier("A2A cancellation task_id", &cancellation.task_id)?;
            validate_identifier("A2A cancellation context_id", &cancellation.context_id)?;
            validate_identifier("A2A cancellation session_id", &cancellation.session_id)?;
            validate_identifier("A2A cancellation run_id", &cancellation.run_id)?;
            validate_mapping(&cancellation.task.mapping)?;
            let task = self.tasks.get(&cancellation.task_id).ok_or_else(|| {
                ProtocolError::invalid("A2A cancellation references an unknown task")
            })?;
            if cancellation.cancellation_id != *cancellation_id
                || *cancellation_id != cancellation_id_for_task(&cancellation.task)
                || cancellation.owner_subject != cancellation.task.owner_subject
                || cancellation.owner_tenant_id != cancellation.task.owner_tenant_id
                || cancellation.task_id != cancellation.task.mapping.task_id
                || cancellation.context_id != cancellation.task.mapping.context_id
                || cancellation.session_id != cancellation.task.mapping.session_id
                || cancellation.run_id != cancellation.task.mapping.run_id
                || task.owner_subject != cancellation.owner_subject
                || task.owner_tenant_id != cancellation.owner_tenant_id
                || task.mapping != cancellation.task.mapping
                || task.created_revision != cancellation.task.created_revision
                || cancellation.task.updated_revision > task.updated_revision
            {
                return Err(ProtocolError::invalid(
                    "A2A cancellation owner or runtime identity is invalid",
                ));
            }
            if cancellation.task.status_message.as_deref() != Some("cancellation requested") {
                return Err(ProtocolError::invalid(
                    "A2A cancellation task snapshot is missing its durable intent",
                ));
            }
            if cancellation.task.state.is_terminal() && cancellation.task.state != task.state {
                return Err(ProtocolError::invalid(
                    "A2A cancellation terminal task snapshot cannot transition",
                ));
            }
            if cancellation.created_revision == 0
                || cancellation.created_revision != cancellation.task.updated_revision
                || cancellation.created_revision > cancellation.updated_revision
                || cancellation.updated_revision > self.revision
                || cancellation.attempts > A2A_MAX_CANCELLATION_ATTEMPTS
            {
                return Err(ProtocolError::invalid(
                    "A2A cancellation revision or attempt history is invalid",
                ));
            }
            let diagnostic_valid = match cancellation.state {
                A2aCancellationOutboxState::Queued => {
                    cancellation.attempts == 0 && cancellation.last_error.is_none()
                }
                A2aCancellationOutboxState::Running => {
                    cancellation.attempts > 0 && cancellation.last_error.is_none()
                }
                A2aCancellationOutboxState::ReconcilePending => {
                    cancellation.last_error.as_deref() == Some(A2A_CANCELLATION_RECONCILE_REASON)
                }
                A2aCancellationOutboxState::Settled => cancellation.last_error.is_none(),
            };
            if !diagnostic_valid {
                return Err(ProtocolError::invalid(
                    "A2A cancellation state diagnostics are invalid",
                ));
            }
            if task.state.is_terminal() && cancellation.state != A2aCancellationOutboxState::Settled
            {
                return Err(ProtocolError::invalid(
                    "A2A terminal task has an unsettled cancellation",
                ));
            }
            let envelope_principal = cancellation.envelope.principal.as_ref().ok_or_else(|| {
                ProtocolError::invalid("A2A cancellation envelope is missing its principal")
            })?;
            cancellation.envelope.correlation.validate()?;
            validate_scope_set(&envelope_principal.scopes)?;
            if cancellation.envelope.schema_version != PROTOCOL_CONTRACT_VERSION
                || cancellation.envelope.protocol != ProtocolKind::A2a
                || cancellation.envelope.operation != "tasks/cancel"
                || cancellation.envelope.target != cancellation.task_id
                || cancellation.envelope.required_scopes != scopes(&[TASK_CANCEL_SCOPE])
                || !cancellation.envelope.authorization.is_allowed()
                || !envelope_principal.matches_identity(
                    &cancellation.owner_subject,
                    cancellation.owner_tenant_id.as_deref(),
                )
                || !envelope_principal.allows(&scopes(&[TASK_CANCEL_SCOPE]))
                || cancellation.envelope.correlation.session_id.as_deref()
                    != Some(cancellation.session_id.as_str())
                || cancellation.envelope.correlation.run_id.as_deref()
                    != Some(cancellation.run_id.as_str())
            {
                return Err(ProtocolError::invalid(
                    "A2A cancellation governed envelope binding is invalid",
                ));
            }
            let mut matching_events = self.pending_events.values().filter(|event| {
                event.kind == A2aPendingEventKind::CancellationRequested
                    && event.task_id == cancellation.task_id
                    && event.source_revision == cancellation.created_revision
            });
            let Some(cancellation_event) = matching_events.next() else {
                return Err(ProtocolError::invalid(
                    "A2A cancellation is missing its exact logical event",
                ));
            };
            if matching_events.next().is_some() || cancellation_event.task != cancellation.task {
                return Err(ProtocolError::invalid(
                    "A2A cancellation logical event binding is invalid",
                ));
            }
            tasks_with_cancellations.insert(cancellation.task_id.clone());
            max_revision = max_revision.max(cancellation.updated_revision);
        }
        if self.tasks.values().any(|task| {
            task.status_message.as_deref() == Some("cancellation requested")
                && !tasks_with_cancellations.contains(&task.mapping.task_id)
        }) {
            return Err(ProtocolError::invalid(
                "A2A cancellation intent is missing its durable control",
            ));
        }

        let mut tasks_with_events = BTreeSet::new();
        let mut tasks_with_unsettled_message_events = BTreeSet::new();
        for (event_id, event) in &self.pending_events {
            validate_identifier("A2A event logical identity", event_id)?;
            validate_owner(&event.owner_subject, event.owner_tenant_id.as_deref())?;
            validate_identifier("A2A event task_id", &event.task_id)?;
            validate_identifier("A2A event context_id", &event.context_id)?;
            validate_identifier("A2A event session_id", &event.session_id)?;
            validate_identifier("A2A event run_id", &event.run_id)?;
            if event.event_id != *event_id {
                return Err(ProtocolError::invalid(
                    "A2A event index does not match its logical identity",
                ));
            }
            if event.message_id.is_some()
                && event.state != A2aPendingEventState::Settled
                && !tasks_with_unsettled_message_events.insert(event.task_id.clone())
            {
                return Err(ProtocolError::invalid(
                    "A2A task has more than one unsettled message acceptance event",
                ));
            }
            let task = self
                .tasks
                .get(&event.task_id)
                .ok_or_else(|| ProtocolError::invalid("A2A event references an unknown task"))?;
            if event.task.owner_subject != event.owner_subject
                || event.task.owner_tenant_id != event.owner_tenant_id
                || task.owner_subject != event.owner_subject
                || task.owner_tenant_id != event.owner_tenant_id
                || event.task.mapping.task_id != event.task_id
                || event.task.mapping.context_id != event.context_id
                || event.task.mapping.session_id != event.session_id
                || event.task.mapping.run_id != event.run_id
                || task.mapping.task_id != event.task_id
                || task.mapping.context_id != event.context_id
                || task.mapping.session_id != event.session_id
                || task.mapping.run_id != event.run_id
                || event.task.mapping.message_id != task.mapping.message_id
                || event.task.created_revision != task.created_revision
                || event.task.updated_revision > task.updated_revision
            {
                return Err(ProtocolError::invalid(
                    "A2A event owner, task, context, or run binding is invalid",
                ));
            }
            if event.task.updated_revision == task.updated_revision && event.task != *task {
                return Err(ProtocolError::invalid(
                    "A2A event current task snapshot is not exact",
                ));
            }
            if event.task.state.is_terminal() && event.task.state != task.state {
                return Err(ProtocolError::invalid(
                    "A2A event terminal task state cannot transition",
                ));
            }
            if event.source_revision == 0
                || event.task.updated_revision > event.source_revision
                || event.source_revision > event.created_revision
                || event.created_revision > event.updated_revision
                || event.updated_revision > self.revision
                || event.attempts > A2A_MAX_EVENT_ATTEMPTS
            {
                return Err(ProtocolError::invalid(
                    "A2A event revision or attempt history is invalid",
                ));
            }
            let expected = make_pending_event_with_response(
                &event.task,
                event.source_revision,
                event.kind,
                event.message_id.as_deref(),
                event.response_message.clone(),
                event.state,
                event.created_revision,
            );
            if expected.event_id != *event_id {
                return Err(ProtocolError::invalid(
                    "A2A event logical identity is not canonical",
                ));
            }
            if expected.payload_hash != event.payload_hash {
                return Err(ProtocolError::invalid(
                    "A2A event canonical payload hash is invalid",
                ));
            }
            match (event.kind, event.response_message.as_ref()) {
                (A2aPendingEventKind::DirectMessageResponse, Some(message)) => {
                    message.validate()?;
                    let matching_dispatches = self
                        .dispatch_outbox
                        .values()
                        .filter(|dispatch| {
                            dispatch.task_id == event.task_id
                                && dispatch.updated_revision == event.source_revision
                                && dispatch.state == A2aDispatchOutboxState::Settled
                                && matches!(
                                    &dispatch.response,
                                    A2aDispatchResponse::Message { message: stored }
                                        if stored == message
                                )
                        })
                        .count();
                    if event.task.state != A2aTaskState::Completed
                        || message.role != A2aRole::Agent
                        || message.context_id.as_deref() != Some(event.context_id.as_str())
                        || message.task_id.is_some()
                        || matching_dispatches != 1
                    {
                        return Err(ProtocolError::invalid(
                            "A2A direct message event is not bound to one completed dispatch",
                        ));
                    }
                }
                (A2aPendingEventKind::DirectMessageResponse, None) => {
                    return Err(ProtocolError::invalid(
                        "A2A direct message event is missing its response",
                    ))
                }
                (_, Some(_)) => {
                    return Err(ProtocolError::invalid(
                        "A2A non-message event cannot carry a direct response",
                    ))
                }
                (_, None) => {}
            }
            let diagnostic_valid = match event.state {
                A2aPendingEventState::Pending => {
                    event.attempts == 0
                        && event.transient_failures == 0
                        && event.next_attempt_at_unix_ms.is_none()
                        && event.last_error.is_none()
                        && event.quarantine_reason.is_none()
                }
                A2aPendingEventState::ReconcilePending => {
                    event.last_error.as_deref() == Some(A2A_EVENT_RECONCILE_REASON)
                        && event.attempts == 0
                        && event.transient_failures > 0
                        && event.next_attempt_at_unix_ms.is_some()
                        && event.quarantine_reason.is_none()
                }
                A2aPendingEventState::Settled => {
                    event.transient_failures == 0
                        && event.next_attempt_at_unix_ms.is_none()
                        && event.last_error.is_none()
                        && event.quarantine_reason.is_none()
                }
                A2aPendingEventState::Quarantined => {
                    let Some(reason) = event.quarantine_reason else {
                        return Err(ProtocolError::invalid(
                            "A2A quarantined event reason is missing",
                        ));
                    };
                    event.transient_failures == 0
                        && event.next_attempt_at_unix_ms.is_none()
                        && event.last_error.as_deref() == Some(reason.diagnostic())
                        && (reason != A2aEventQuarantineReason::AttemptsExhausted
                            || event.attempts == A2A_MAX_EVENT_ATTEMPTS)
                }
            };
            if !diagnostic_valid {
                return Err(ProtocolError::invalid(
                    "A2A event state diagnostics are invalid",
                ));
            }
            let matching_receipt = event.message_id.as_deref().and_then(|message_id| {
                let owner = event_owner(event);
                self.receipts
                    .get(&scoped_receipt_key_for_owner(&owner, message_id))
            });
            let kind_valid = match event.kind {
                A2aPendingEventKind::TaskCreated => {
                    event.task.created_revision == event.task.updated_revision
                        && event.task.state == A2aTaskState::Working
                        && matching_receipt.is_some_and(|receipt| {
                            receipt.mapping.task_id == event.task_id
                                && receipt.accepted_revision == event.source_revision
                                && receipt.mapping.message_id == event.task.mapping.message_id
                        })
                }
                A2aPendingEventKind::MessageAccepted => {
                    event.task.state == A2aTaskState::Working
                        && matching_receipt.is_some_and(|receipt| {
                            receipt.mapping.task_id == event.task_id
                                && receipt.accepted_revision == event.source_revision
                        })
                }
                A2aPendingEventKind::StatusChanged => match matching_receipt {
                    Some(receipt) => {
                        event.task.state == A2aTaskState::Working
                            && receipt.mapping.task_id == event.task_id
                            && receipt.accepted_revision == event.source_revision
                    }
                    None => event.source_revision == event.task.updated_revision,
                },
                A2aPendingEventKind::CancellationRequested => {
                    let cancellation_id = cancellation_id_for_task(&event.task);
                    event.message_id.is_none()
                        && event.source_revision == event.task.updated_revision
                        && !event.task.state.is_terminal()
                        && event.task.status_message.as_deref() == Some("cancellation requested")
                        && self.cancellation_outbox.get(&cancellation_id).is_some_and(
                            |cancellation| {
                                cancellation.task == event.task
                                    && cancellation.created_revision == event.source_revision
                            },
                        )
                }
                A2aPendingEventKind::DirectMessageResponse => {
                    event.message_id.is_none()
                        && event.source_revision == event.task.updated_revision
                        && event.task.state == A2aTaskState::Completed
                        && event.response_message.is_some()
                }
                A2aPendingEventKind::RecoveredSnapshot => {
                    event.message_id.is_none()
                        && event.source_revision == event.task.updated_revision
                }
            };
            if !kind_valid {
                return Err(ProtocolError::invalid("A2A event kind binding is invalid"));
            }
            tasks_with_events.insert(event.task_id.clone());
            max_revision = max_revision.max(event.updated_revision);
        }
        if self.pending_event_schedule != rebuild_pending_event_schedule(&self.pending_events) {
            return Err(ProtocolError::invalid(
                "A2A pending event retry index is not canonical",
            ));
        }
        if self.pending_event_schedule_by_owner
            != rebuild_pending_event_schedule_by_owner(&self.pending_events)
        {
            return Err(ProtocolError::invalid(
                "A2A owner pending event retry index is not canonical",
            ));
        }
        if self.dispatch_event_readiness != rebuild_dispatch_event_readiness(&self.pending_events) {
            return Err(ProtocolError::invalid(
                "A2A dispatch event readiness index is not canonical",
            ));
        }
        if self
            .tasks
            .keys()
            .any(|task_id| !tasks_with_events.contains(task_id))
        {
            return Err(ProtocolError::invalid(
                "A2A task is missing its durable event identity",
            ));
        }

        for (context_key, session_id) in &self.contexts {
            validate_identifier("A2A context session_id", session_id)?;
            let owner = self
                .context_owners
                .get(context_key)
                .expect("context index key equality was checked");
            validate_owner(&owner.subject, owner.tenant_id.as_deref())?;
            let mut matching = self.tasks.values().filter(|task| {
                task.mapping.session_id == *session_id
                    && task.owner_subject == owner.subject
                    && task.owner_tenant_id == owner.tenant_id
            });
            let Some(first) = matching.next() else {
                return Err(ProtocolError::invalid(
                    "A2A context does not reference an owned task",
                ));
            };
            if *context_key != scoped_context_key_for_owner(owner, &first.mapping.context_id)
                || matching.any(|task| task.mapping.context_id != first.mapping.context_id)
            {
                return Err(ProtocolError::invalid("A2A context index is not canonical"));
            }
        }

        if max_revision != self.revision {
            return Err(ProtocolError::invalid(
                "A2A mapper revision is not represented by durable mapper state",
            ));
        }
        if self.next_sequence <= max_generated_sequence {
            return Err(ProtocolError::invalid(
                "A2A mapper next_sequence would reuse a generated identity",
            ));
        }
        Ok(())
    }

    fn validate_capacity_snapshot(&self) -> ProtocolResult<()> {
        if self.tasks.len() > A2A_MAX_TASKS
            || self.contexts.len() > A2A_MAX_CONTEXTS
            || self.receipts.len() > A2A_MAX_RECEIPTS
            || self.dispatch_outbox.len() > A2A_MAX_DISPATCHES
            || self.cancellation_outbox.len() > A2A_MAX_CANCELLATIONS
            || self.pending_events.len() > A2A_MAX_PENDING_EVENTS
        {
            return Err(ProtocolError::invalid(
                "A2A mapper snapshot exceeds a global collection limit",
            ));
        }
        let measured_receipt_bytes = receipt_storage_bytes(&self.receipts)?;
        if measured_receipt_bytes != self.receipt_bytes
            || measured_receipt_bytes > A2A_MAX_RECEIPT_BYTES
        {
            return Err(ProtocolError::invalid(
                "A2A mapper snapshot receipt byte accounting is invalid",
            ));
        }
        let measured_dispatch_bytes = dispatch_storage_bytes(&self.dispatch_outbox)?;
        if measured_dispatch_bytes != self.dispatch_bytes
            || measured_dispatch_bytes > A2A_MAX_DISPATCH_BYTES
        {
            return Err(ProtocolError::invalid(
                "A2A mapper snapshot dispatch byte accounting is invalid",
            ));
        }
        let measured_cancellation_bytes = cancellation_storage_bytes(&self.cancellation_outbox)?;
        if measured_cancellation_bytes != self.cancellation_bytes
            || measured_cancellation_bytes > A2A_MAX_CANCELLATION_BYTES
        {
            return Err(ProtocolError::invalid(
                "A2A mapper snapshot cancellation byte accounting is invalid",
            ));
        }
        let measured_event_bytes = pending_event_storage_bytes(&self.pending_events)?;
        if measured_event_bytes != self.pending_event_bytes
            || measured_event_bytes > A2A_MAX_PENDING_EVENT_BYTES
        {
            return Err(ProtocolError::invalid(
                "A2A mapper snapshot event byte accounting is invalid",
            ));
        }
        let mut tasks_by_owner: BTreeMap<A2aContextOwner, usize> = BTreeMap::new();
        let mut contexts_by_owner: BTreeMap<A2aContextOwner, usize> = BTreeMap::new();
        let mut receipts_by_owner: BTreeMap<A2aContextOwner, (usize, usize)> = BTreeMap::new();
        let mut dispatches_by_owner: BTreeMap<A2aContextOwner, (usize, usize)> = BTreeMap::new();
        let mut cancellations_by_owner: BTreeMap<A2aContextOwner, (usize, usize)> = BTreeMap::new();
        let mut events_by_owner: BTreeMap<A2aContextOwner, (usize, usize)> = BTreeMap::new();
        for task in self.tasks.values() {
            let owner = A2aContextOwner {
                subject: task.owner_subject.clone(),
                tenant_id: task.owner_tenant_id.clone(),
            };
            *tasks_by_owner.entry(owner).or_default() += 1;
        }
        for owner in self.context_owners.values() {
            *contexts_by_owner.entry(owner.clone()).or_default() += 1;
        }
        for (key, receipt) in &self.receipts {
            let owner = A2aContextOwner {
                subject: receipt.owner_subject.clone(),
                tenant_id: receipt.owner_tenant_id.clone(),
            };
            let entry = receipts_by_owner.entry(owner).or_default();
            entry.0 += 1;
            entry.1 = entry
                .1
                .checked_add(receipt_entry_storage_bytes(key, receipt)?)
                .ok_or_else(|| ProtocolError::invalid("A2A receipt bytes overflowed"))?;
        }
        for (key, dispatch) in &self.dispatch_outbox {
            let entry = dispatches_by_owner
                .entry(dispatch_owner(dispatch))
                .or_default();
            entry.0 += 1;
            entry.1 = entry
                .1
                .checked_add(dispatch_entry_storage_bytes(key, dispatch)?)
                .ok_or_else(|| ProtocolError::invalid("A2A dispatch bytes overflowed"))?;
        }
        for (key, event) in &self.pending_events {
            let entry = events_by_owner.entry(event_owner(event)).or_default();
            entry.0 += 1;
            entry.1 = entry
                .1
                .checked_add(pending_event_entry_storage_bytes(key, event)?)
                .ok_or_else(|| ProtocolError::invalid("A2A event bytes overflowed"))?;
        }
        for (key, cancellation) in &self.cancellation_outbox {
            let entry = cancellations_by_owner
                .entry(cancellation_owner(cancellation))
                .or_default();
            entry.0 += 1;
            entry.1 = entry
                .1
                .checked_add(cancellation_entry_storage_bytes(key, cancellation)?)
                .ok_or_else(|| ProtocolError::invalid("A2A cancellation bytes overflowed"))?;
        }
        if tasks_by_owner
            .values()
            .any(|count| *count > A2A_MAX_TASKS_PER_OWNER)
            || contexts_by_owner
                .values()
                .any(|count| *count > A2A_MAX_CONTEXTS_PER_OWNER)
            || receipts_by_owner.values().any(|(count, bytes)| {
                *count > A2A_MAX_RECEIPTS_PER_OWNER || *bytes > A2A_MAX_RECEIPT_BYTES_PER_OWNER
            })
            || dispatches_by_owner.values().any(|(count, bytes)| {
                *count > A2A_MAX_DISPATCHES_PER_OWNER || *bytes > A2A_MAX_DISPATCH_BYTES_PER_OWNER
            })
            || cancellations_by_owner.values().any(|(count, bytes)| {
                *count > A2A_MAX_CANCELLATIONS_PER_OWNER
                    || *bytes > A2A_MAX_CANCELLATION_BYTES_PER_OWNER
            })
            || events_by_owner.values().any(|(count, bytes)| {
                *count > A2A_MAX_PENDING_EVENTS_PER_OWNER
                    || *bytes > A2A_MAX_PENDING_EVENT_BYTES_PER_OWNER
            })
        {
            return Err(ProtocolError::invalid(
                "A2A mapper snapshot exceeds an owner-scoped limit",
            ));
        }
        Ok(())
    }

    /// Prepare a `SendMessage` operation. Accepted message ids are durable idempotency keys.
    pub fn prepare_send_message(
        &mut self,
        message: A2aMessage,
        correlation: CorrelationIdentity,
        principal: Option<&ProtocolPrincipal>,
    ) -> GovernedAction<A2aAction> {
        // Keep the mapper mutation atomic even for direct callers. All task, receipt, dispatch,
        // and event-intent mutations are installed together only after the complete candidate has
        // passed its byte/count checks.
        let response_policy = principal
            .and_then(|principal| {
                self.dispatch_outbox
                    .get(&scoped_dispatch_key(principal, &message.message_id))
            })
            .map(|dispatch| dispatch.response_policy)
            .unwrap_or(A2aSendResponsePolicy::Blocking);
        let mut candidate = self.clone();
        let governed = candidate.prepare_send_message_candidate_with_response_policy(
            message,
            correlation,
            principal,
            response_policy,
        );
        if governed.is_authorized() && candidate.revision != self.revision {
            *self = candidate;
        }
        governed
    }

    /// Transport-only first-acceptance policy. The immediate snapshot is installed atomically
    /// with the receipt and dispatch, so racing retries cannot change the eventual response kind.
    pub(crate) fn prepare_send_message_candidate_with_response_policy(
        &mut self,
        message: A2aMessage,
        correlation: CorrelationIdentity,
        principal: Option<&ProtocolPrincipal>,
        response_policy: A2aSendResponsePolicy,
    ) -> GovernedAction<A2aAction> {
        let target = message
            .task_id
            .as_deref()
            .unwrap_or(message.message_id.as_str())
            .to_owned();
        let mut envelope = GovernanceEnvelope::evaluate(
            ProtocolKind::A2a,
            correlation,
            principal,
            "message/send",
            target,
            scopes(&[SEND_MESSAGE_SCOPE]),
        );
        if !envelope.authorization.is_allowed() {
            return GovernedAction::denied(envelope);
        }
        if let Err(error) = message.validate() {
            envelope = envelope.deny(GovernanceDenialCode::InvalidRequest, error.message);
            return GovernedAction::denied(envelope);
        }
        let principal = principal.expect("allowed envelope always has a principal");

        let receipt_key = scoped_receipt_key(principal, &message.message_id);
        if let Some(receipt) = self.receipts.get(&receipt_key).cloned() {
            if receipt.message != message {
                envelope = envelope.deny(
                    GovernanceDenialCode::DuplicateConflict,
                    "A2A message_id was reused with different content",
                );
                return GovernedAction::denied(envelope);
            }
            let dispatch_policy = self
                .dispatch_outbox
                .get(&scoped_dispatch_key(principal, &message.message_id))
                .map(|dispatch| dispatch.response_policy);
            if dispatch_policy != Some(response_policy) {
                envelope = envelope.deny(
                    GovernanceDenialCode::DuplicateConflict,
                    "A2A message_id retry changed its response policy",
                );
                return GovernedAction::denied(envelope);
            }
            envelope.correlation.session_id = Some(receipt.mapping.session_id.clone());
            envelope.correlation.run_id = Some(receipt.mapping.run_id.clone());
            return GovernedAction::from_envelope(
                envelope,
                A2aAction::DuplicateMessage { receipt },
            );
        }
        if let Err(error) = self.preflight_send_message(&message, principal) {
            let denial = match error.code {
                ProtocolErrorCode::NotFound => GovernanceDenialCode::UnknownTarget,
                ProtocolErrorCode::InvalidRequest => GovernanceDenialCode::InvalidRequest,
                ProtocolErrorCode::InvalidTransition | ProtocolErrorCode::Conflict => {
                    GovernanceDenialCode::StateConflict
                }
                ProtocolErrorCode::Unauthorized
                | ProtocolErrorCode::Forbidden
                | ProtocolErrorCode::Cancelled => GovernanceDenialCode::StateConflict,
            };
            envelope = envelope.deny(denial, error.message);
            return GovernedAction::denied(envelope);
        }
        let (mapping, resumed_from) = if let Some(task_id) = message.task_id.as_deref() {
            let Some(existing) = self.tasks.get(task_id).cloned() else {
                envelope = envelope.deny(
                    GovernanceDenialCode::UnknownTarget,
                    "A2A task is not accessible",
                );
                return GovernedAction::denied(envelope);
            };
            if !principal
                .matches_identity(&existing.owner_subject, existing.owner_tenant_id.as_deref())
            {
                envelope = envelope.deny(
                    GovernanceDenialCode::UnknownTarget,
                    "A2A task is not accessible",
                );
                return GovernedAction::denied(envelope);
            }
            if message
                .context_id
                .as_ref()
                .is_some_and(|context_id| context_id != &existing.mapping.context_id)
            {
                envelope = envelope.deny(
                    GovernanceDenialCode::StateConflict,
                    "A2A context_id does not match task_id",
                );
                return GovernedAction::denied(envelope);
            }
            if existing.state.is_terminal() {
                envelope = envelope.deny(
                    GovernanceDenialCode::StateConflict,
                    "terminal A2A task cannot accept another message",
                );
                return GovernedAction::denied(envelope);
            }

            let resumed_from = matches!(
                existing.state,
                A2aTaskState::InputRequired | A2aTaskState::AuthRequired
            )
            .then_some(existing.state);
            let revision_steps = if resumed_from.is_some() { 2 } else { 1 };
            if !self.has_counter_capacity(0, revision_steps) {
                envelope = envelope.deny(
                    GovernanceDenialCode::StateConflict,
                    "A2A mapper counters reached the cross-language integer limit",
                );
                return GovernedAction::denied(envelope);
            }
            if resumed_from.is_some() {
                self.bump_revision();
                let task = self.tasks.get_mut(task_id).expect("task was resolved");
                task.state = A2aTaskState::Working;
                task.status_message = None;
                task.updated_revision = self.revision;
            }
            let mut mapping = existing.mapping;
            mapping.message_id = message.message_id.clone();
            (mapping, resumed_from)
        } else {
            let generated_ids = match message.context_id.as_deref() {
                None => 3,
                Some(context_id)
                    if self
                        .contexts
                        .contains_key(&scoped_context_key(principal, context_id)) =>
                {
                    2
                }
                Some(_) => 3,
            };
            if !self.has_counter_capacity(generated_ids, 2) {
                envelope = envelope.deny(
                    GovernanceDenialCode::StateConflict,
                    "A2A mapper counters reached the cross-language integer limit",
                );
                return GovernedAction::denied(envelope);
            }
            let context_id = match message.context_id.clone() {
                Some(context_id) => context_id,
                None => match self.generate_context_id(principal) {
                    Ok(context_id) => context_id,
                    Err(error) => {
                        envelope =
                            envelope.deny(GovernanceDenialCode::StateConflict, error.message);
                        return GovernedAction::denied(envelope);
                    }
                },
            };
            let context_key = scoped_context_key(principal, &context_id);
            let session_id = if self.contexts.contains_key(&context_key) {
                let session_id = self
                    .contexts
                    .get(&context_key)
                    .expect("resolved A2A context key exists");
                if self
                    .context_owners
                    .get(&context_key)
                    .is_none_or(|owner| !owner.matches(principal))
                {
                    envelope = envelope.deny(
                        GovernanceDenialCode::PrincipalMismatch,
                        "A2A context is not accessible",
                    );
                    return GovernedAction::denied(envelope);
                }
                session_id.clone()
            } else {
                let session_id = self.next_id("a2a-session");
                self.contexts
                    .insert(context_key.clone(), session_id.clone());
                self.context_owners
                    .insert(context_key, A2aContextOwner::from_principal(principal));
                session_id
            };
            let task_id = self.next_id("a2a-task");
            let run_id = self.next_id("a2a-run");
            self.bump_revision();
            let mapping = A2aRunMapping {
                context_id,
                session_id,
                task_id: task_id.clone(),
                run_id,
                message_id: message.message_id.clone(),
            };
            self.tasks.insert(
                task_id,
                A2aTaskRecord {
                    mapping: mapping.clone(),
                    state: A2aTaskState::Working,
                    owner_subject: principal.subject.clone(),
                    owner_tenant_id: principal.tenant_id.clone(),
                    created_revision: self.revision,
                    updated_revision: self.revision,
                    status_message: None,
                },
            );
            (mapping, None)
        };

        envelope.correlation.session_id = Some(mapping.session_id.clone());
        envelope.correlation.run_id = Some(mapping.run_id.clone());
        let mut normalized = message.clone();
        normalized.context_id = Some(mapping.context_id.clone());
        normalized.task_id = Some(mapping.task_id.clone());

        self.bump_revision();
        let receipt = A2aMessageReceipt {
            message,
            mapping: mapping.clone(),
            owner_subject: principal.subject.clone(),
            owner_tenant_id: principal.tenant_id.clone(),
            accepted_revision: self.revision,
        };
        let receipt_entry_bytes = receipt_entry_storage_bytes(&receipt_key, &receipt)
            .expect("A2A receipt byte capacity was preflighted");
        self.receipt_bytes = self
            .receipt_bytes
            .checked_add(receipt_entry_bytes)
            .expect("A2A receipt byte capacity was preflighted");
        self.receipts.insert(receipt_key, receipt.clone());

        let task = self
            .tasks
            .get(&mapping.task_id)
            .cloned()
            .expect("A2A accepted message task exists");
        let dispatch = make_dispatch_record(
            principal,
            normalized.clone(),
            &mapping,
            resumed_from,
            envelope.clone(),
            self.revision,
            A2aDispatchAcceptanceResponse {
                policy: response_policy,
                immediate_task: (response_policy == A2aSendResponsePolicy::Immediate)
                    .then(|| task.clone()),
            },
        );
        if let Err(error) = self.insert_new_dispatch(dispatch) {
            envelope = envelope.deny(GovernanceDenialCode::StateConflict, error.message);
            return GovernedAction::denied(envelope);
        }

        let event_kind = if task.created_revision == task.updated_revision
            && receipt.mapping.message_id == task.mapping.message_id
        {
            A2aPendingEventKind::TaskCreated
        } else if resumed_from.is_some() {
            A2aPendingEventKind::StatusChanged
        } else {
            A2aPendingEventKind::MessageAccepted
        };
        let event = make_pending_event(
            &task,
            self.revision,
            event_kind,
            Some(&receipt.message.message_id),
            A2aPendingEventState::Pending,
            self.revision,
        );
        if let Err(error) = self.insert_new_pending_event(event) {
            envelope = envelope.deny(GovernanceDenialCode::StateConflict, error.message);
            return GovernedAction::denied(envelope);
        }
        GovernedAction::from_envelope(
            envelope,
            A2aAction::DispatchMessage {
                message: normalized,
                mapping,
                resumed_from,
            },
        )
    }

    pub fn prepare_get_task(
        &self,
        task_id: &str,
        correlation: CorrelationIdentity,
        principal: Option<&ProtocolPrincipal>,
    ) -> GovernedAction<A2aAction> {
        let Some(task) = self.tasks.get(task_id).cloned() else {
            return denied_unknown_task(
                correlation,
                principal,
                "tasks/get",
                task_id,
                TASK_READ_SCOPE,
            );
        };
        let envelope = task_envelope(&task, correlation, principal, "tasks/get", TASK_READ_SCOPE);
        GovernedAction::from_envelope(envelope, A2aAction::GetTask { task })
    }

    /// Prepare the A2A 1.0 `ListTasks` operation.
    ///
    /// Authorization scoping happens before filters, counts, cursor validation, and pagination;
    /// therefore no response metadata can reveal another principal's tasks. The stable order is
    /// most-recent `updated_revision` first with task id as the deterministic tie-breaker.
    pub fn prepare_list_tasks(
        &self,
        request: A2aListTasksRequest,
        correlation: CorrelationIdentity,
        principal: Option<&ProtocolPrincipal>,
    ) -> GovernedAction<A2aAction> {
        let target = request.tenant.clone().unwrap_or_else(|| "tasks".into());
        let mut envelope = GovernanceEnvelope::evaluate(
            ProtocolKind::A2a,
            correlation,
            principal,
            "tasks/list",
            target,
            scopes(&[TASK_READ_SCOPE]),
        );
        if !envelope.authorization.is_allowed() {
            return GovernedAction::denied(envelope);
        }
        let principal = principal.expect("allowed envelope always has a principal");

        if let Err(error) = request.validate() {
            envelope = envelope.deny(GovernanceDenialCode::InvalidRequest, error.message);
            return GovernedAction::denied(envelope);
        }
        if request
            .tenant
            .as_deref()
            .is_some_and(|tenant| principal.tenant_id.as_deref() != Some(tenant))
        {
            envelope = envelope.deny(
                GovernanceDenialCode::PrincipalMismatch,
                "A2A tenant is not accessible",
            );
            return GovernedAction::denied(envelope);
        }

        // Scope first. Filters, total_size, and cursor membership are evaluated only inside the
        // authenticated subject+tenant slice.
        let query_hash = list_query_hash(principal, &request);
        let mut tasks: Vec<A2aTaskRecord> = self
            .tasks
            .values()
            .filter(|task| {
                principal.matches_identity(&task.owner_subject, task.owner_tenant_id.as_deref())
            })
            .filter(|task| {
                request
                    .context_id
                    .as_ref()
                    .is_none_or(|context_id| task.mapping.context_id == *context_id)
            })
            .filter(|task| request.status.is_none_or(|status| task.state == status))
            .cloned()
            .collect();
        tasks.sort_by(|left, right| {
            right
                .updated_revision
                .cmp(&left.updated_revision)
                .then_with(|| right.mapping.task_id.cmp(&left.mapping.task_id))
        });
        let snapshot_hash = list_snapshot_hash(&tasks);

        let total_size = u64::try_from(tasks.len()).unwrap_or(u64::MAX);
        let start = match request
            .page_token
            .as_deref()
            .filter(|token| !token.is_empty())
        {
            Some(token) => {
                let cursor = match decode_page_token(token) {
                    Ok(cursor) => cursor,
                    Err(error) => {
                        envelope =
                            envelope.deny(GovernanceDenialCode::InvalidRequest, error.message);
                        return GovernedAction::denied(envelope);
                    }
                };
                if cursor.snapshot_hash != snapshot_hash {
                    envelope = envelope.deny(
                        GovernanceDenialCode::InvalidRequest,
                        "A2A page_token is stale for the authorized task snapshot",
                    );
                    return GovernedAction::denied(envelope);
                }
                if cursor.query_hash != query_hash {
                    envelope = envelope.deny(
                        GovernanceDenialCode::InvalidRequest,
                        "A2A page_token does not match the authorized list query",
                    );
                    return GovernedAction::denied(envelope);
                }
                let Some(index) = tasks
                    .iter()
                    .position(|task| task.mapping.task_id == cursor.next_task_id)
                else {
                    envelope = envelope.deny(
                        GovernanceDenialCode::InvalidRequest,
                        "A2A page_token is not valid for the authorized task set",
                    );
                    return GovernedAction::denied(envelope);
                };
                index
            }
            None => 0,
        };
        let page_size = request
            .page_size
            .unwrap_or(A2A_DEFAULT_LIST_TASKS_PAGE_SIZE);
        let end = start
            .saturating_add(usize::from(page_size))
            .min(tasks.len());
        let next_page_token = tasks
            .get(end)
            .map(|task| encode_page_token(&task.mapping.task_id, &snapshot_hash, &query_hash))
            .unwrap_or_default();
        let tasks = tasks[start..end].to_vec();

        GovernedAction::from_envelope(
            envelope,
            A2aAction::ListTasks {
                page: A2aTaskPage {
                    tasks,
                    next_page_token,
                    page_size,
                    total_size,
                },
            },
        )
    }

    pub fn prepare_cancel_task(
        &mut self,
        task_id: &str,
        correlation: CorrelationIdentity,
        principal: Option<&ProtocolPrincipal>,
    ) -> GovernedAction<A2aAction> {
        // Task marker, logical event, and host cancellation control are one persistence unit.
        let mut candidate = self.clone();
        let governed = candidate.prepare_cancel_task_candidate(task_id, correlation, principal);
        if governed.is_authorized() && candidate.revision != self.revision {
            *self = candidate;
        }
        governed
    }

    /// Apply one cancellation to an already-isolated persistence candidate.
    pub(crate) fn prepare_cancel_task_candidate(
        &mut self,
        task_id: &str,
        correlation: CorrelationIdentity,
        principal: Option<&ProtocolPrincipal>,
    ) -> GovernedAction<A2aAction> {
        let Some(existing) = self.tasks.get(task_id).cloned() else {
            return denied_unknown_task(
                correlation,
                principal,
                "tasks/cancel",
                task_id,
                TASK_CANCEL_SCOPE,
            );
        };
        let mut envelope = task_envelope(
            &existing,
            correlation,
            principal,
            "tasks/cancel",
            TASK_CANCEL_SCOPE,
        );
        if !envelope.authorization.is_allowed() {
            return GovernedAction::denied(envelope);
        }
        if existing.state.is_terminal() {
            envelope = envelope.deny(
                GovernanceDenialCode::StateConflict,
                "terminal A2A task cannot be cancelled",
            );
            return GovernedAction::denied(envelope);
        }
        let cancellation_id = cancellation_id_for_task(&existing);
        if let Some(record) = self.cancellation_outbox.get(&cancellation_id) {
            return GovernedAction::from_envelope(envelope, record.action());
        }
        if !self.has_counter_capacity(0, 1) {
            envelope = envelope.deny(
                GovernanceDenialCode::StateConflict,
                "A2A mapper revision reached the cross-language integer limit",
            );
            return GovernedAction::denied(envelope);
        }
        let revision = self
            .next_revision()
            .expect("A2A revision capacity was checked");
        let mut projected = existing;
        // Cancellation is a persisted, non-terminal intent until the dispatch host confirms its
        // runtime fence has stopped. The transport performs the terminal transition only after
        // that confirmation; a callback failure leaves this task subscribable and reconcilable.
        projected.status_message = Some("cancellation requested".into());
        projected.updated_revision = revision;
        let event = make_pending_event(
            &projected,
            revision,
            A2aPendingEventKind::CancellationRequested,
            None,
            A2aPendingEventState::Pending,
            revision,
        );
        let cancellation = make_cancellation_record(
            projected.clone(),
            envelope.clone(),
            A2aCancellationOutboxState::Queued,
            revision,
        );
        if let Err(error) = self
            .preflight_new_pending_event(&event)
            .and_then(|_| self.preflight_new_cancellation(&cancellation))
        {
            envelope = envelope.deny(GovernanceDenialCode::StateConflict, error.message);
            return GovernedAction::denied(envelope);
        }
        self.insert_new_pending_event(event)
            .expect("A2A cancel event was preflighted");
        self.insert_new_cancellation(cancellation)
            .expect("A2A cancellation control was preflighted");
        self.revision = revision;
        self.tasks.insert(task_id.to_owned(), projected.clone());
        GovernedAction::from_envelope(envelope, A2aAction::CancelTask { task: projected })
    }

    /// Receiver-side transition used when an agent asks the A2A client for more input.
    pub fn require_input(
        &mut self,
        task_id: &str,
        status_message: impl Into<String>,
    ) -> ProtocolResult<()> {
        self.transition_task(
            task_id,
            A2aTaskState::InputRequired,
            Some(status_message.into()),
        )
    }

    pub fn transition_task(
        &mut self,
        task_id: &str,
        next: A2aTaskState,
        status_message: Option<String>,
    ) -> ProtocolResult<()> {
        let mut candidate = self.clone();
        candidate.transition_task_candidate(task_id, next, status_message, None)?;
        *self = candidate;
        Ok(())
    }

    /// Atomically apply a host-proven cancellation fence. Ordinary task transitions cannot settle
    /// a non-settled cancellation; the exact durable control generation must acknowledge `Stopped`
    /// through the transport before the task becomes cancelled.
    pub(crate) fn acknowledge_cancellation(
        &mut self,
        cancellation_id: &str,
        expected_attempt: u32,
        status_message: Option<String>,
    ) -> ProtocolResult<()> {
        let task_id = self
            .cancellation_outbox
            .get(cancellation_id)
            .map(|record| record.task_id.clone())
            .ok_or_else(|| {
                ProtocolError::not_found("A2A cancellation control is not registered")
            })?;
        let mut candidate = self.clone();
        candidate.transition_task_candidate(
            &task_id,
            A2aTaskState::Cancelled,
            status_message,
            Some((cancellation_id, expected_attempt)),
        )?;
        *self = candidate;
        Ok(())
    }

    /// Atomically publish durable task artifacts and complete the exact running host dispatch.
    /// The dispatch generation fence prevents a late callback from completing a replacement
    /// attempt, while the persisted response marker makes an exact same-generation retry a no-op.
    pub(crate) fn complete_dispatch_with_artifacts(
        &mut self,
        dispatch_id: &str,
        expected_attempt: u32,
        artifacts: Vec<A2aArtifact>,
    ) -> ProtocolResult<A2aTaskRecord> {
        self.complete_dispatch_output(
            dispatch_id,
            expected_attempt,
            A2aDispatchCompletionOutput::Task { artifacts },
        )
    }

    /// Atomically persist a direct agent Message response and complete the task behind the exact
    /// running dispatch. The response is dispatch-scoped and never masquerades as a task field.
    pub(crate) fn complete_dispatch_with_message(
        &mut self,
        dispatch_id: &str,
        expected_attempt: u32,
        message: A2aMessage,
    ) -> ProtocolResult<A2aTaskRecord> {
        self.complete_dispatch_output(
            dispatch_id,
            expected_attempt,
            A2aDispatchCompletionOutput::Message { message },
        )
    }

    fn complete_dispatch_output(
        &mut self,
        dispatch_id: &str,
        expected_attempt: u32,
        output: A2aDispatchCompletionOutput,
    ) -> ProtocolResult<A2aTaskRecord> {
        let mut candidate = self.clone();
        let task =
            candidate.complete_dispatch_output_candidate(dispatch_id, expected_attempt, output)?;
        *self = candidate;
        Ok(task)
    }

    fn complete_dispatch_output_candidate(
        &mut self,
        dispatch_id: &str,
        expected_attempt: u32,
        output: A2aDispatchCompletionOutput,
    ) -> ProtocolResult<A2aTaskRecord> {
        let dispatch = self
            .dispatch_outbox
            .get(dispatch_id)
            .cloned()
            .ok_or_else(|| ProtocolError::not_found("A2A dispatch is not registered"))?;
        if dispatch.attempts != expected_attempt {
            return Err(ProtocolError::invalid_transition(
                "A2A dispatch completion generation is stale",
            ));
        }
        let existing = self
            .tasks
            .get(&dispatch.task_id)
            .cloned()
            .ok_or_else(|| ProtocolError::not_found("A2A task is not registered"))?;

        let exact_existing_output = match (&dispatch.response, &output) {
            (
                A2aDispatchResponse::Task {
                    finalized_by_dispatch: true,
                    artifacts: current,
                },
                A2aDispatchCompletionOutput::Task { artifacts },
            ) => current == artifacts,
            (
                A2aDispatchResponse::Message { message: current },
                A2aDispatchCompletionOutput::Message { message },
            ) => current == message,
            _ => false,
        };
        if dispatch.state == A2aDispatchOutboxState::Settled {
            return if exact_existing_output
                && existing.state == A2aTaskState::Completed
                && existing.updated_revision == dispatch.updated_revision
            {
                Ok(existing)
            } else {
                Err(ProtocolError::invalid_transition(
                    "A2A dispatch completion is already settled with another response",
                ))
            };
        }
        if dispatch.state != A2aDispatchOutboxState::Running {
            return Err(ProtocolError::invalid_transition(
                "A2A dispatch completion requires the exact running generation",
            ));
        }
        if self.cancellation_outbox.values().any(|record| {
            record.task_id == dispatch.task_id
                && record.state != A2aCancellationOutboxState::Settled
        }) {
            return Err(ProtocolError::invalid_transition(
                "A2A dispatch completion cannot cross a durable cancellation fence",
            ));
        }
        if !valid_transition(existing.state, A2aTaskState::Completed) {
            return Err(ProtocolError::invalid_transition(format!(
                "invalid A2A task transition: {:?} -> {:?}",
                existing.state,
                A2aTaskState::Completed
            )));
        }

        let revision = self.next_revision()?;
        let mut projected = existing;
        projected.state = A2aTaskState::Completed;
        projected.status_message = None;
        projected.updated_revision = revision;
        let response = match output {
            A2aDispatchCompletionOutput::Task { artifacts } => {
                validate_task_artifacts(&artifacts, projected.state)?;
                A2aDispatchResponse::Task {
                    finalized_by_dispatch: true,
                    artifacts,
                }
            }
            A2aDispatchCompletionOutput::Message { message } => {
                message.validate()?;
                if message.role != A2aRole::Agent
                    || message.context_id.as_deref() != Some(dispatch.context_id.as_str())
                    || message.task_id.is_some()
                {
                    return Err(ProtocolError::invalid(
                        "A2A direct message response must be an agent message bound only to the dispatch context",
                    ));
                }
                A2aDispatchResponse::Message { message }
            }
        };
        let (event_kind, response_message) = match &response {
            A2aDispatchResponse::Message { message } => (
                A2aPendingEventKind::DirectMessageResponse,
                Some(message.clone()),
            ),
            A2aDispatchResponse::Task { .. } => (A2aPendingEventKind::StatusChanged, None),
        };
        let event = make_pending_event_with_response(
            &projected,
            revision,
            event_kind,
            None,
            response_message,
            A2aPendingEventState::Pending,
            revision,
        );
        self.preflight_new_pending_event(&event)?;

        let mut replacement = dispatch;
        replacement.response = response;
        replacement.updated_revision = revision;
        self.replace_dispatch_record(dispatch_id, replacement)?;
        self.settle_task_dispatches_at_revision(&projected.mapping.task_id, revision)?;
        self.settle_task_cancellation_at_revision(&projected.mapping.task_id, revision)?;
        self.insert_new_pending_event(event)
            .expect("A2A output completion event was preflighted");
        self.revision = revision;
        self.tasks
            .insert(projected.mapping.task_id.clone(), projected.clone());
        Ok(projected)
    }

    fn transition_task_candidate(
        &mut self,
        task_id: &str,
        next: A2aTaskState,
        status_message: Option<String>,
        cancellation_ack: Option<(&str, u32)>,
    ) -> ProtocolResult<()> {
        let existing = self
            .tasks
            .get(task_id)
            .cloned()
            .ok_or_else(|| ProtocolError::not_found("A2A task is not registered"))?;
        let pending_cancellation = self
            .cancellation_outbox
            .values()
            .find(|record| {
                record.task_id == task_id && record.state != A2aCancellationOutboxState::Settled
            })
            .cloned();
        match (pending_cancellation.as_ref(), cancellation_ack) {
            (Some(record), Some((cancellation_id, expected_attempt)))
                if next == A2aTaskState::Cancelled
                    && record.cancellation_id == cancellation_id
                    && matches!(
                        record.state,
                        A2aCancellationOutboxState::Running
                            | A2aCancellationOutboxState::ReconcilePending
                    )
                    && record.attempts == expected_attempt => {}
            (Some(_), _) => {
                return Err(ProtocolError::invalid_transition(
                    "A2A task has a non-settled cancellation control; only its exact stopped acknowledgement may terminalize the task",
                ));
            }
            (None, Some(_)) => {
                return Err(ProtocolError::invalid_transition(
                    "A2A cancellation acknowledgement is stale",
                ));
            }
            (None, None) => {}
        }
        let current = existing.state;
        if !valid_transition(current, next) {
            return Err(ProtocolError::invalid_transition(format!(
                "invalid A2A task transition: {current:?} -> {next:?}"
            )));
        }
        let revision = self.next_revision()?;
        let mut projected = existing;
        projected.state = next;
        projected.status_message = status_message;
        projected.updated_revision = revision;
        let event = make_pending_event(
            &projected,
            revision,
            A2aPendingEventKind::StatusChanged,
            None,
            A2aPendingEventState::Pending,
            revision,
        );
        self.preflight_new_pending_event(&event)?;
        if next.is_terminal()
            || matches!(
                next,
                A2aTaskState::InputRequired | A2aTaskState::AuthRequired
            )
        {
            self.settle_task_dispatches_at_revision(task_id, revision)?;
        }
        if next.is_terminal() {
            self.settle_task_cancellation_at_revision(task_id, revision)?;
        }
        self.insert_new_pending_event(event)
            .expect("A2A task transition event was preflighted");
        self.revision = revision;
        self.tasks.insert(task_id.to_owned(), projected);
        Ok(())
    }

    fn next_id(&mut self, prefix: &str) -> String {
        let sequence = self.next_sequence;
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .expect("A2A identifier capacity was preflighted");
        format!("{prefix}-{sequence:016}")
    }

    fn generate_context_id(&self, principal: &ProtocolPrincipal) -> ProtocolResult<String> {
        for _ in 0..A2A_CONTEXT_GENERATION_ATTEMPTS {
            let mut random = [0_u8; A2A_CONTEXT_RANDOM_BYTES];
            getrandom::fill(&mut random).map_err(|_| {
                ProtocolError::invalid("secure randomness for A2A context_id is unavailable")
            })?;
            let mut context_id = String::with_capacity("a2a-context-random-".len() + 32);
            context_id.push_str("a2a-context-random-");
            for byte in random {
                use std::fmt::Write as _;
                write!(context_id, "{byte:02x}").expect("writing to a String cannot fail");
            }
            if !self
                .contexts
                .contains_key(&scoped_context_key(principal, &context_id))
            {
                return Ok(context_id);
            }
        }
        Err(ProtocolError::invalid(
            "could not generate a unique A2A context_id",
        ))
    }

    fn bump_revision(&mut self) {
        self.revision = self
            .revision
            .checked_add(1)
            .expect("A2A revision capacity was preflighted");
    }

    fn has_counter_capacity(&self, generated_ids: u64, revision_steps: u64) -> bool {
        self.next_sequence
            .checked_add(generated_ids)
            .is_some_and(|next| next <= A2A_MAX_SAFE_INTEGER)
            && self
                .revision
                .checked_add(revision_steps)
                .is_some_and(|revision| revision <= A2A_MAX_SAFE_INTEGER)
    }
}

/// Serialize one canonical mapper snapshot without ever growing the output past `max_bytes`.
///
/// `A2aMapper` uses ordered maps, so compact serde JSON is stable for an identical mapper. The
/// writer rejects the first write that would cross the configured ceiling rather than allocating
/// the complete value and checking its length afterwards.
pub fn serialize_a2a_mapper_snapshot_bounded(
    mapper: &A2aMapper,
    max_bytes: usize,
) -> ProtocolResult<Vec<u8>> {
    if max_bytes == 0 || max_bytes > A2A_MAX_MAPPER_SNAPSHOT_BYTES {
        return Err(ProtocolError::invalid(format!(
            "A2A mapper snapshot byte limit must be between 1 and {A2A_MAX_MAPPER_SNAPSHOT_BYTES}"
        )));
    }
    let mut writer = BoundedSnapshotWriter::new(max_bytes);
    if let Err(error) = serde_json::to_writer(&mut writer, mapper) {
        if writer.exceeded {
            return Err(ProtocolError::conflict(format!(
                "A2A mapper snapshot exceeds the {max_bytes} byte limit"
            )));
        }
        return Err(ProtocolError::conflict(format!(
            "serialize A2A mapper snapshot: {error}"
        )));
    }
    Ok(writer.bytes)
}

/// Decode persisted mapper bytes only after enforcing the raw-byte ceiling. Deserialization then
/// performs the mapper's existing schema, index, revision, ownership, and capacity validation.
pub fn deserialize_a2a_mapper_snapshot_bounded(
    bytes: &[u8],
    max_bytes: usize,
) -> ProtocolResult<A2aMapper> {
    if max_bytes == 0 || max_bytes > A2A_MAX_MAPPER_SNAPSHOT_BYTES {
        return Err(ProtocolError::invalid(format!(
            "A2A mapper snapshot byte limit must be between 1 and {A2A_MAX_MAPPER_SNAPSHOT_BYTES}"
        )));
    }
    if bytes.len() > max_bytes {
        return Err(ProtocolError::invalid(format!(
            "persisted A2A mapper snapshot exceeds the {max_bytes} byte limit"
        )));
    }
    serde_json::from_slice(bytes)
        .map_err(|error| ProtocolError::invalid(format!("decode A2A mapper snapshot: {error}")))
}

struct BoundedSnapshotWriter {
    bytes: Vec<u8>,
    max_bytes: usize,
    exceeded: bool,
}

impl BoundedSnapshotWriter {
    fn new(max_bytes: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(max_bytes.min(64 * 1024)),
            max_bytes,
            exceeded: false,
        }
    }
}

impl Write for BoundedSnapshotWriter {
    fn write(&mut self, input: &[u8]) -> io::Result<usize> {
        if input.len() > self.max_bytes.saturating_sub(self.bytes.len()) {
            self.exceeded = true;
            return Err(io::Error::other("A2A mapper snapshot byte limit exceeded"));
        }
        self.bytes.extend_from_slice(input);
        Ok(input.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn validate_task_artifacts(
    artifacts: &[A2aArtifact],
    task_state: A2aTaskState,
) -> ProtocolResult<()> {
    if artifacts.len() > A2A_MAX_ARTIFACTS_PER_TASK {
        return Err(ProtocolError::invalid(format!(
            "A2A task must not contain more than {A2A_MAX_ARTIFACTS_PER_TASK} artifacts"
        )));
    }
    if !artifacts.is_empty() && task_state != A2aTaskState::Completed {
        return Err(ProtocolError::invalid(
            "A2A task artifacts require a completed task",
        ));
    }
    let mut artifact_ids = BTreeSet::new();
    for artifact in artifacts {
        artifact.validate()?;
        if !artifact_ids.insert(&artifact.artifact_id) {
            return Err(ProtocolError::invalid(
                "A2A task artifact ids must be unique",
            ));
        }
    }
    let artifact_bytes = serde_json::to_vec(artifacts)
        .map_err(|error| ProtocolError::invalid(format!("serialize A2A artifacts: {error}")))?
        .len();
    if artifact_bytes > A2A_MAX_ARTIFACT_BYTES_PER_TASK {
        return Err(ProtocolError::invalid(format!(
            "A2A task artifacts must not exceed {A2A_MAX_ARTIFACT_BYTES_PER_TASK} serialized bytes"
        )));
    }
    Ok(())
}

fn message_storage_bytes(message: &A2aMessage) -> ProtocolResult<usize> {
    serde_json::to_vec(message)
        .map(|value| value.len())
        .map_err(|error| ProtocolError::invalid(format!("serialize A2A message: {error}")))
}

fn receipt_storage_bytes(receipts: &BTreeMap<String, A2aMessageReceipt>) -> ProtocolResult<usize> {
    receipts.iter().try_fold(0_usize, |total, (key, receipt)| {
        total
            .checked_add(receipt_entry_storage_bytes(key, receipt)?)
            .ok_or_else(|| ProtocolError::invalid("A2A receipt bytes overflowed"))
    })
}

fn receipt_entry_storage_bytes(key: &str, receipt: &A2aMessageReceipt) -> ProtocolResult<usize> {
    serialized_entry_storage_bytes(key, receipt, "receipt")
}

fn dispatch_storage_bytes(
    dispatches: &BTreeMap<String, A2aDispatchOutboxRecord>,
) -> ProtocolResult<usize> {
    dispatches.iter().try_fold(0_usize, |total, (key, record)| {
        total
            .checked_add(dispatch_entry_storage_bytes(key, record)?)
            .ok_or_else(|| ProtocolError::invalid("A2A dispatch bytes overflowed"))
    })
}

fn dispatch_entry_storage_bytes(
    key: &str,
    record: &A2aDispatchOutboxRecord,
) -> ProtocolResult<usize> {
    serialized_entry_storage_bytes(key, record, "dispatch")
}

fn cancellation_storage_bytes(
    cancellations: &BTreeMap<String, A2aCancellationOutboxRecord>,
) -> ProtocolResult<usize> {
    cancellations
        .iter()
        .try_fold(0_usize, |total, (key, record)| {
            total
                .checked_add(cancellation_entry_storage_bytes(key, record)?)
                .ok_or_else(|| ProtocolError::invalid("A2A cancellation bytes overflowed"))
        })
}

fn cancellation_entry_storage_bytes(
    key: &str,
    record: &A2aCancellationOutboxRecord,
) -> ProtocolResult<usize> {
    serialized_entry_storage_bytes(key, record, "cancellation")
}

fn pending_event_storage_bytes(
    events: &BTreeMap<String, A2aPendingEventIntent>,
) -> ProtocolResult<usize> {
    events.iter().try_fold(0_usize, |total, (key, event)| {
        total
            .checked_add(pending_event_entry_storage_bytes(key, event)?)
            .ok_or_else(|| ProtocolError::invalid("A2A pending event bytes overflowed"))
    })
}

fn rebuild_pending_event_schedule(
    events: &BTreeMap<String, A2aPendingEventIntent>,
) -> CowSet<A2aPendingEventScheduleKey> {
    events
        .values()
        .filter_map(A2aPendingEventScheduleKey::from_event)
        .collect::<BTreeSet<_>>()
        .into()
}

fn rebuild_pending_event_schedule_by_owner(
    events: &BTreeMap<String, A2aPendingEventIntent>,
) -> CowMap<A2aContextOwner, CowSet<A2aPendingEventScheduleKey>> {
    let mut schedules: CowMap<_, CowSet<_>> = CowMap::default();
    for event in events.values() {
        let Some(key) = A2aPendingEventScheduleKey::from_event(event) else {
            continue;
        };
        schedules.entry(event_owner(event)).or_default().insert(key);
    }
    schedules
}

fn rebuild_dispatch_event_readiness(
    events: &BTreeMap<String, A2aPendingEventIntent>,
) -> CowMap<A2aDispatchEventBinding, (usize, usize)> {
    let mut readiness = CowMap::default();
    for event in events.values() {
        add_dispatch_event_readiness(&mut readiness, event);
    }
    readiness
}

fn add_dispatch_event_readiness(
    readiness: &mut BTreeMap<A2aDispatchEventBinding, (usize, usize)>,
    event: &A2aPendingEventIntent,
) {
    let Some(binding) = A2aDispatchEventBinding::from_event(event) else {
        return;
    };
    let counts = readiness.entry(binding).or_default();
    counts.0 = counts.0.saturating_add(1);
    if event.state == A2aPendingEventState::Settled {
        counts.1 = counts.1.saturating_add(1);
    }
}

fn remove_dispatch_event_readiness(
    readiness: &mut BTreeMap<A2aDispatchEventBinding, (usize, usize)>,
    event: &A2aPendingEventIntent,
) {
    let Some(binding) = A2aDispatchEventBinding::from_event(event) else {
        return;
    };
    let remove = if let Some(counts) = readiness.get_mut(&binding) {
        counts.0 = counts.0.saturating_sub(1);
        if event.state == A2aPendingEventState::Settled {
            counts.1 = counts.1.saturating_sub(1);
        }
        counts.0 == 0
    } else {
        false
    };
    if remove {
        readiness.remove(&binding);
    }
}

fn pending_event_entry_storage_bytes(
    key: &str,
    event: &A2aPendingEventIntent,
) -> ProtocolResult<usize> {
    serialized_entry_storage_bytes(key, event, "pending event")
}

fn serialized_entry_storage_bytes<T: Serialize>(
    key: &str,
    value: &T,
    kind: &str,
) -> ProtocolResult<usize> {
    let value_bytes = serde_json::to_vec(value)
        .map_err(|_| ProtocolError::invalid(format!("serialize A2A {kind}")))?
        .len();
    key.len()
        .checked_add(value_bytes)
        .ok_or_else(|| ProtocolError::invalid(format!("A2A {kind} bytes overflowed")))
}

fn dispatch_owner(record: &A2aDispatchOutboxRecord) -> A2aContextOwner {
    A2aContextOwner {
        subject: record.owner_subject.clone(),
        tenant_id: record.owner_tenant_id.clone(),
    }
}

fn cancellation_owner(record: &A2aCancellationOutboxRecord) -> A2aContextOwner {
    A2aContextOwner {
        subject: record.owner_subject.clone(),
        tenant_id: record.owner_tenant_id.clone(),
    }
}

fn event_owner(event: &A2aPendingEventIntent) -> A2aContextOwner {
    A2aContextOwner {
        subject: event.owner_subject.clone(),
        tenant_id: event.owner_tenant_id.clone(),
    }
}

fn dispatch_bytes_for_owner(
    dispatches: &BTreeMap<String, A2aDispatchOutboxRecord>,
    owner: &A2aContextOwner,
) -> ProtocolResult<usize> {
    dispatches
        .iter()
        .filter(|(_, record)| dispatch_owner(record) == *owner)
        .try_fold(0_usize, |total, (key, record)| {
            total
                .checked_add(dispatch_entry_storage_bytes(key, record)?)
                .ok_or_else(|| ProtocolError::invalid("A2A owner dispatch bytes overflowed"))
        })
}

fn pending_event_bytes_for_owner(
    events: &BTreeMap<String, A2aPendingEventIntent>,
    owner: &A2aContextOwner,
) -> ProtocolResult<usize> {
    events
        .iter()
        .filter(|(_, event)| event_owner(event) == *owner)
        .try_fold(0_usize, |total, (key, event)| {
            total
                .checked_add(pending_event_entry_storage_bytes(key, event)?)
                .ok_or_else(|| ProtocolError::invalid("A2A owner event bytes overflowed"))
        })
}

fn cancellation_bytes_for_owner(
    cancellations: &BTreeMap<String, A2aCancellationOutboxRecord>,
    owner: &A2aContextOwner,
) -> ProtocolResult<usize> {
    cancellations
        .iter()
        .filter(|(_, record)| cancellation_owner(record) == *owner)
        .try_fold(0_usize, |total, (key, record)| {
            total
                .checked_add(cancellation_entry_storage_bytes(key, record)?)
                .ok_or_else(|| ProtocolError::invalid("A2A owner cancellation bytes overflowed"))
        })
}

struct A2aDispatchAcceptanceResponse {
    policy: A2aSendResponsePolicy,
    immediate_task: Option<A2aTaskRecord>,
}

fn make_dispatch_record(
    principal: &ProtocolPrincipal,
    message: A2aMessage,
    mapping: &A2aRunMapping,
    resumed_from: Option<A2aTaskState>,
    envelope: GovernanceEnvelope,
    revision: u64,
    accepted_response: A2aDispatchAcceptanceResponse,
) -> A2aDispatchOutboxRecord {
    A2aDispatchOutboxRecord {
        dispatch_id: scoped_dispatch_key(principal, &mapping.message_id),
        owner_subject: principal.subject.clone(),
        owner_tenant_id: principal.tenant_id.clone(),
        message_id: mapping.message_id.clone(),
        task_id: mapping.task_id.clone(),
        context_id: mapping.context_id.clone(),
        session_id: mapping.session_id.clone(),
        run_id: mapping.run_id.clone(),
        message,
        resumed_from,
        envelope,
        state: A2aDispatchOutboxState::Queued,
        response: A2aDispatchResponse::default(),
        response_policy: accepted_response.policy,
        immediate_response: accepted_response.immediate_task,
        attempts: 0,
        last_error: None,
        created_revision: revision,
        updated_revision: revision,
    }
}

fn make_cancellation_record(
    task: A2aTaskRecord,
    envelope: GovernanceEnvelope,
    state: A2aCancellationOutboxState,
    revision: u64,
) -> A2aCancellationOutboxRecord {
    A2aCancellationOutboxRecord {
        cancellation_id: cancellation_id_for_task(&task),
        owner_subject: task.owner_subject.clone(),
        owner_tenant_id: task.owner_tenant_id.clone(),
        task_id: task.mapping.task_id.clone(),
        context_id: task.mapping.context_id.clone(),
        session_id: task.mapping.session_id.clone(),
        run_id: task.mapping.run_id.clone(),
        task,
        envelope,
        state,
        attempts: 0,
        last_error: None,
        created_revision: revision,
        updated_revision: revision,
    }
}

fn make_pending_event(
    task: &A2aTaskRecord,
    source_revision: u64,
    kind: A2aPendingEventKind,
    message_id: Option<&str>,
    state: A2aPendingEventState,
    created_revision: u64,
) -> A2aPendingEventIntent {
    make_pending_event_with_response(
        task,
        source_revision,
        kind,
        message_id,
        None,
        state,
        created_revision,
    )
}

fn make_pending_event_with_response(
    task: &A2aTaskRecord,
    source_revision: u64,
    kind: A2aPendingEventKind,
    message_id: Option<&str>,
    response_message: Option<A2aMessage>,
    state: A2aPendingEventState,
    created_revision: u64,
) -> A2aPendingEventIntent {
    let mut payload = serde_json::json!({
        "kind": kind,
        "task": task,
    });
    if let Some(message) = &response_message {
        payload
            .as_object_mut()
            .expect("A2A event payload is an object")
            .insert("response_message".into(), serde_json::json!(message));
    }
    let payload_hash = crate::durability::stable_input_hash(&payload);
    let identity = serde_json::json!({
        "owner_subject": task.owner_subject,
        "owner_tenant_id": task.owner_tenant_id,
        "task_id": task.mapping.task_id,
        "context_id": task.mapping.context_id,
        "session_id": task.mapping.session_id,
        "run_id": task.mapping.run_id,
        "source_revision": source_revision,
        "kind": kind,
        "message_id": message_id,
    });
    let digest = crate::durability::stable_input_hash(&identity);
    A2aPendingEventIntent {
        event_id: format!("a2a-event-{digest}"),
        owner_subject: task.owner_subject.clone(),
        owner_tenant_id: task.owner_tenant_id.clone(),
        task_id: task.mapping.task_id.clone(),
        context_id: task.mapping.context_id.clone(),
        session_id: task.mapping.session_id.clone(),
        run_id: task.mapping.run_id.clone(),
        source_revision,
        kind,
        message_id: message_id.map(str::to_owned),
        payload_hash,
        task: task.clone(),
        response_message,
        state,
        attempts: 0,
        transient_failures: 0,
        next_attempt_at_unix_ms: None,
        last_error: None,
        quarantine_reason: None,
        created_revision,
        updated_revision: created_revision,
    }
}

fn ensure_count_capacity(current: usize, maximum: usize, kind: &str) -> ProtocolResult<()> {
    if current >= maximum {
        Err(ProtocolError::conflict(format!(
            "A2A {kind} capacity is exhausted"
        )))
    } else {
        Ok(())
    }
}

fn ensure_byte_capacity(
    current: usize,
    added: usize,
    maximum: usize,
    kind: &str,
) -> ProtocolResult<()> {
    if current
        .checked_add(added)
        .is_none_or(|total| total > maximum)
    {
        Err(ProtocolError::conflict(format!(
            "A2A {kind} byte capacity is exhausted"
        )))
    } else {
        Ok(())
    }
}

fn task_owner_matches(task: &A2aTaskRecord, principal: &ProtocolPrincipal) -> bool {
    principal.matches_identity(&task.owner_subject, task.owner_tenant_id.as_deref())
}

fn receipt_owner_matches(receipt: &A2aMessageReceipt, principal: &ProtocolPrincipal) -> bool {
    principal.matches_identity(&receipt.owner_subject, receipt.owner_tenant_id.as_deref())
}

fn dispatch_owner_matches(
    dispatch: &A2aDispatchOutboxRecord,
    principal: &ProtocolPrincipal,
) -> bool {
    principal.matches_identity(&dispatch.owner_subject, dispatch.owner_tenant_id.as_deref())
}

fn cancellation_owner_matches(
    cancellation: &A2aCancellationOutboxRecord,
    principal: &ProtocolPrincipal,
) -> bool {
    principal.matches_identity(
        &cancellation.owner_subject,
        cancellation.owner_tenant_id.as_deref(),
    )
}

fn valid_transition(current: A2aTaskState, next: A2aTaskState) -> bool {
    if current == next {
        return !current.is_terminal();
    }
    match current {
        A2aTaskState::Submitted => matches!(
            next,
            A2aTaskState::Working | A2aTaskState::Rejected | A2aTaskState::Cancelled
        ),
        A2aTaskState::Working => matches!(
            next,
            A2aTaskState::InputRequired
                | A2aTaskState::AuthRequired
                | A2aTaskState::Completed
                | A2aTaskState::Failed
                | A2aTaskState::Cancelled
                | A2aTaskState::Rejected
        ),
        A2aTaskState::InputRequired | A2aTaskState::AuthRequired => matches!(
            next,
            A2aTaskState::Working
                | A2aTaskState::Failed
                | A2aTaskState::Cancelled
                | A2aTaskState::Rejected
        ),
        A2aTaskState::Completed
        | A2aTaskState::Failed
        | A2aTaskState::Cancelled
        | A2aTaskState::Rejected => false,
    }
}

fn a2a_mapper_schema_version() -> u32 {
    A2A_MAPPER_SCHEMA_VERSION
}

fn validate_mapping(mapping: &A2aRunMapping) -> ProtocolResult<()> {
    validate_identifier("A2A mapping context_id", &mapping.context_id)?;
    validate_identifier("A2A mapping session_id", &mapping.session_id)?;
    validate_identifier("A2A mapping task_id", &mapping.task_id)?;
    validate_identifier("A2A mapping run_id", &mapping.run_id)?;
    validate_identifier("A2A mapping message_id", &mapping.message_id)
}

fn validate_owner(subject: &str, tenant_id: Option<&str>) -> ProtocolResult<()> {
    validate_identifier("A2A owner subject", subject)?;
    if let Some(tenant_id) = tenant_id {
        validate_identifier("A2A owner tenant_id", tenant_id)?;
    }
    Ok(())
}

fn update_generated_sequence(value: &str, prefix: &str, maximum: &mut u64) -> ProtocolResult<()> {
    let suffix = value.strip_prefix(prefix).ok_or_else(|| {
        ProtocolError::invalid(format!("A2A generated identity must start with {prefix}"))
    })?;
    if suffix.len() != 16 || !suffix.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(ProtocolError::invalid(
            "A2A generated identity has an invalid sequence",
        ));
    }
    let sequence = suffix
        .parse::<u64>()
        .map_err(|_| ProtocolError::invalid("A2A generated identity sequence overflowed"))?;
    if sequence == 0 {
        return Err(ProtocolError::invalid(
            "A2A generated identity sequence must be positive",
        ));
    }
    *maximum = (*maximum).max(sequence);
    Ok(())
}

fn mapping_runtime_identity_matches(left: &A2aRunMapping, right: &A2aRunMapping) -> bool {
    left.context_id == right.context_id
        && left.session_id == right.session_id
        && left.task_id == right.task_id
        && left.run_id == right.run_id
}

fn normalize_restored_contexts(
    contexts: BTreeMap<String, String>,
    mut context_owners: BTreeMap<String, A2aContextOwner>,
    tasks: &BTreeMap<String, A2aTaskRecord>,
) -> ProtocolResult<(BTreeMap<String, String>, BTreeMap<String, A2aContextOwner>)> {
    if contexts.len() != context_owners.len() || contexts.keys().ne(context_owners.keys()) {
        return Err(ProtocolError::invalid(
            "A2A context and owner indexes do not match",
        ));
    }

    let mut normalized_contexts = BTreeMap::new();
    let mut normalized_owners = BTreeMap::new();
    for (stored_key, session_id) in contexts {
        validate_identifier("A2A context session_id", &session_id)?;
        let owner = context_owners
            .remove(&stored_key)
            .expect("context keys were checked for equality");
        validate_owner(&owner.subject, owner.tenant_id.as_deref())?;

        let mut matching_tasks = tasks.values().filter(|task| {
            task.mapping.session_id == session_id
                && task.owner_subject == owner.subject
                && task.owner_tenant_id == owner.tenant_id
        });
        let first = matching_tasks.next().ok_or_else(|| {
            ProtocolError::invalid("A2A context does not reference an owned task")
        })?;
        if matching_tasks.any(|task| task.mapping.context_id != first.mapping.context_id) {
            return Err(ProtocolError::invalid(
                "A2A context session maps to multiple context ids",
            ));
        }

        let canonical_key = scoped_context_key_for_owner(&owner, &first.mapping.context_id);
        if stored_key != first.mapping.context_id && stored_key != canonical_key {
            return Err(ProtocolError::invalid(
                "A2A context index key is neither legacy nor canonical",
            ));
        }
        if normalized_contexts
            .insert(canonical_key.clone(), session_id)
            .is_some()
            || normalized_owners.insert(canonical_key, owner).is_some()
        {
            return Err(ProtocolError::invalid(
                "A2A context migration produced a duplicate scoped identity",
            ));
        }
    }
    Ok((normalized_contexts, normalized_owners))
}

fn normalize_restored_receipts(
    receipts: BTreeMap<String, A2aMessageReceipt>,
) -> ProtocolResult<BTreeMap<String, A2aMessageReceipt>> {
    let mut normalized = BTreeMap::new();
    for (stored_key, receipt) in receipts {
        validate_owner(&receipt.owner_subject, receipt.owner_tenant_id.as_deref())?;
        validate_identifier("A2A receipt message_id", &receipt.message.message_id)?;
        let owner = A2aContextOwner {
            subject: receipt.owner_subject.clone(),
            tenant_id: receipt.owner_tenant_id.clone(),
        };
        let canonical_key = scoped_receipt_key_for_owner(&owner, &receipt.message.message_id);
        if stored_key != receipt.message.message_id && stored_key != canonical_key {
            return Err(ProtocolError::invalid(
                "A2A receipt index key is neither legacy nor canonical",
            ));
        }
        if normalized.insert(canonical_key, receipt).is_some() {
            return Err(ProtocolError::invalid(
                "A2A receipt migration produced a duplicate scoped identity",
            ));
        }
    }
    Ok(normalized)
}

fn rebuild_legacy_dispatch_outbox(
    tasks: &BTreeMap<String, A2aTaskRecord>,
    receipts: &BTreeMap<String, A2aMessageReceipt>,
) -> ProtocolResult<BTreeMap<String, A2aDispatchOutboxRecord>> {
    let mut rebuilt = BTreeMap::new();
    for (receipt_key, receipt) in receipts {
        let task = tasks.get(&receipt.mapping.task_id).ok_or_else(|| {
            ProtocolError::invalid("legacy A2A receipt references an unknown task")
        })?;
        let mut principal =
            ProtocolPrincipal::new(receipt.owner_subject.clone(), [SEND_MESSAGE_SCOPE])?;
        if let Some(tenant_id) = &receipt.owner_tenant_id {
            principal = principal.with_tenant(tenant_id.clone())?;
        }
        let migration_hash = crate::durability::stable_input_hash(&serde_json::json!({
            "receipt_key": receipt_key,
            "accepted_revision": receipt.accepted_revision,
        }));
        let mut correlation = CorrelationIdentity::new(
            format!("a2a-migrated-{migration_hash}"),
            format!("a2a-migrated-{migration_hash}"),
        )?;
        correlation.session_id = Some(receipt.mapping.session_id.clone());
        correlation.run_id = Some(receipt.mapping.run_id.clone());
        let target = receipt
            .message
            .task_id
            .clone()
            .unwrap_or_else(|| receipt.message.message_id.clone());
        let envelope = GovernanceEnvelope::evaluate(
            ProtocolKind::A2a,
            correlation,
            Some(&principal),
            "message/send",
            target,
            scopes(&[SEND_MESSAGE_SCOPE]),
        );
        if !envelope.authorization.is_allowed() {
            return Err(ProtocolError::invalid(
                "legacy A2A dispatch envelope could not be rebuilt",
            ));
        }
        let mut normalized = receipt.message.clone();
        normalized.context_id = Some(receipt.mapping.context_id.clone());
        normalized.task_id = Some(receipt.mapping.task_id.clone());
        let mut record = make_dispatch_record(
            &principal,
            normalized,
            &receipt.mapping,
            None,
            envelope,
            receipt.accepted_revision,
            A2aDispatchAcceptanceResponse {
                policy: A2aSendResponsePolicy::Blocking,
                immediate_task: None,
            },
        );
        if task.state.is_terminal()
            || matches!(
                task.state,
                A2aTaskState::InputRequired | A2aTaskState::AuthRequired
            )
        {
            record.state = A2aDispatchOutboxState::Settled;
        } else {
            // Old snapshots cannot prove whether the host side effect happened. Never queue a
            // blind replay; require explicit reconciliation first.
            record.state = A2aDispatchOutboxState::ReconcilePending;
            record.last_error = Some(A2A_RECONCILE_REASON.into());
        }
        if rebuilt.insert(record.dispatch_id.clone(), record).is_some() {
            return Err(ProtocolError::invalid(
                "legacy A2A dispatch migration produced a duplicate identity",
            ));
        }
    }
    Ok(rebuilt)
}

fn rebuild_legacy_cancellation_outbox(
    tasks: &BTreeMap<String, A2aTaskRecord>,
    pending_events: &BTreeMap<String, A2aPendingEventIntent>,
) -> ProtocolResult<BTreeMap<String, A2aCancellationOutboxRecord>> {
    let mut cancellation_tasks = BTreeMap::new();
    for event in pending_events
        .values()
        .filter(|event| event.kind == A2aPendingEventKind::CancellationRequested)
    {
        if cancellation_tasks
            .insert(event.task_id.clone(), event.task.clone())
            .is_some()
        {
            return Err(ProtocolError::invalid(
                "legacy A2A snapshot has duplicate cancellation events",
            ));
        }
    }
    for task in tasks
        .values()
        .filter(|task| task.status_message.as_deref() == Some("cancellation requested"))
    {
        cancellation_tasks
            .entry(task.mapping.task_id.clone())
            .or_insert_with(|| task.clone());
    }

    let mut rebuilt = BTreeMap::new();
    for task in cancellation_tasks.values() {
        let current = tasks.get(&task.mapping.task_id).ok_or_else(|| {
            ProtocolError::invalid("legacy A2A cancellation references an unknown task")
        })?;
        let mut principal =
            ProtocolPrincipal::new(task.owner_subject.clone(), [TASK_CANCEL_SCOPE])?;
        if let Some(tenant_id) = &task.owner_tenant_id {
            principal = principal.with_tenant(tenant_id.clone())?;
        }
        let migration_hash = crate::durability::stable_input_hash(&serde_json::json!({
            "owner_subject": task.owner_subject,
            "owner_tenant_id": task.owner_tenant_id,
            "task_id": task.mapping.task_id,
            "run_id": task.mapping.run_id,
            "updated_revision": task.updated_revision,
        }));
        let correlation = CorrelationIdentity::new(
            format!("a2a-migrated-cancel-{migration_hash}"),
            format!("a2a-migrated-cancel-{migration_hash}"),
        )?;
        let envelope = task_envelope(
            task,
            correlation,
            Some(&principal),
            "tasks/cancel",
            TASK_CANCEL_SCOPE,
        );
        if !envelope.authorization.is_allowed() {
            return Err(ProtocolError::invalid(
                "legacy A2A cancellation envelope could not be rebuilt",
            ));
        }
        let state = if current.state.is_terminal() {
            A2aCancellationOutboxState::Settled
        } else {
            A2aCancellationOutboxState::ReconcilePending
        };
        let mut record =
            make_cancellation_record(task.clone(), envelope, state, task.updated_revision);
        if state == A2aCancellationOutboxState::ReconcilePending {
            record.last_error = Some(A2A_CANCELLATION_RECONCILE_REASON.into());
        }
        if rebuilt
            .insert(record.cancellation_id.clone(), record)
            .is_some()
        {
            return Err(ProtocolError::invalid(
                "legacy A2A cancellation migration produced a duplicate identity",
            ));
        }
    }
    Ok(rebuilt)
}

fn rebuild_legacy_pending_events(
    tasks: &BTreeMap<String, A2aTaskRecord>,
) -> ProtocolResult<BTreeMap<String, A2aPendingEventIntent>> {
    let mut rebuilt = BTreeMap::new();
    for task in tasks.values() {
        let kind = if !task.state.is_terminal()
            && task.status_message.as_deref() == Some("cancellation requested")
        {
            A2aPendingEventKind::CancellationRequested
        } else {
            A2aPendingEventKind::RecoveredSnapshot
        };
        let mut event = make_pending_event(
            task,
            task.updated_revision,
            kind,
            None,
            A2aPendingEventState::ReconcilePending,
            task.updated_revision,
        );
        event.transient_failures = 1;
        event.next_attempt_at_unix_ms = Some(0);
        event.last_error = Some(A2A_EVENT_RECONCILE_REASON.into());
        if rebuilt.insert(event.event_id.clone(), event).is_some() {
            return Err(ProtocolError::invalid(
                "legacy A2A event migration produced a duplicate identity",
            ));
        }
    }
    Ok(rebuilt)
}

fn migrate_restored_pending_events(
    schema_version: u32,
    pending_events: &mut BTreeMap<String, A2aPendingEventIntent>,
) -> ProtocolResult<()> {
    if !matches!(
        schema_version,
        A2A_LEGACY_MAPPER_SCHEMA_VERSION | A2A_PREVIOUS_MAPPER_SCHEMA_VERSION
    ) {
        return Ok(());
    }
    for event in pending_events.values_mut() {
        // Older snapshots classified untyped backend failures as terminal attempt exhaustion.
        // They are safe to resume because deterministic payload/binding poison has its own reason.
        if event.state == A2aPendingEventState::Quarantined
            && event.quarantine_reason == Some(A2aEventQuarantineReason::AttemptsExhausted)
        {
            event.state = A2aPendingEventState::ReconcilePending;
            event.transient_failures = event
                .transient_failures
                .max(event.attempts)
                .max(A2A_MAX_EVENT_ATTEMPTS);
            event.attempts = 0;
            event.next_attempt_at_unix_ms = Some(0);
            event.last_error = Some(A2A_EVENT_RECONCILE_REASON.into());
            event.quarantine_reason = None;
        } else if event.state == A2aPendingEventState::ReconcilePending {
            event.transient_failures = event.transient_failures.max(event.attempts).max(1);
            event.attempts = 0;
            if event.next_attempt_at_unix_ms.is_none() {
                event.next_attempt_at_unix_ms = Some(0);
            }
        }
    }
    Ok(())
}

fn scoped_context_key(principal: &ProtocolPrincipal, context_id: &str) -> String {
    scoped_index_key(
        "context",
        &principal.subject,
        principal.tenant_id.as_deref(),
        context_id,
    )
}

fn scoped_context_key_for_owner(owner: &A2aContextOwner, context_id: &str) -> String {
    scoped_index_key(
        "context",
        &owner.subject,
        owner.tenant_id.as_deref(),
        context_id,
    )
}

fn scoped_receipt_key(principal: &ProtocolPrincipal, message_id: &str) -> String {
    scoped_index_key(
        "receipt",
        &principal.subject,
        principal.tenant_id.as_deref(),
        message_id,
    )
}

fn scoped_receipt_key_for_owner(owner: &A2aContextOwner, message_id: &str) -> String {
    scoped_index_key(
        "receipt",
        &owner.subject,
        owner.tenant_id.as_deref(),
        message_id,
    )
}

fn scoped_dispatch_key(principal: &ProtocolPrincipal, message_id: &str) -> String {
    scoped_index_key(
        "dispatch",
        &principal.subject,
        principal.tenant_id.as_deref(),
        message_id,
    )
}

fn scoped_dispatch_key_for_owner(owner: &A2aContextOwner, message_id: &str) -> String {
    scoped_index_key(
        "dispatch",
        &owner.subject,
        owner.tenant_id.as_deref(),
        message_id,
    )
}

fn cancellation_id_for_task(task: &A2aTaskRecord) -> String {
    let digest = crate::durability::stable_input_hash(&serde_json::json!({
        "owner_subject": task.owner_subject,
        "owner_tenant_id": task.owner_tenant_id,
        "task_id": task.mapping.task_id,
        "run_id": task.mapping.run_id,
    }));
    format!("a2a-cancellation-{digest}")
}

fn scoped_index_key(kind: &str, subject: &str, tenant_id: Option<&str>, value: &str) -> String {
    let tenant_id = tenant_id.unwrap_or("");
    // The version and index kind make canonical keys disjoint from each other. Length framing
    // makes the tuple injective even when user-controlled ids contain separators.
    format!(
        "a2a:v1:{kind}:{}:{}{}:{}{}:{}",
        subject.len(),
        subject,
        tenant_id.len(),
        tenant_id,
        value.len(),
        value
    )
}

fn task_envelope(
    task: &A2aTaskRecord,
    correlation: CorrelationIdentity,
    principal: Option<&ProtocolPrincipal>,
    operation: &str,
    scope: &str,
) -> GovernanceEnvelope {
    let mut envelope = GovernanceEnvelope::evaluate(
        ProtocolKind::A2a,
        correlation,
        principal,
        operation,
        task.mapping.task_id.clone(),
        scopes(&[scope]),
    );
    if envelope.authorization.is_allowed()
        && principal.is_none_or(|value| {
            !value.matches_identity(&task.owner_subject, task.owner_tenant_id.as_deref())
        })
    {
        envelope = envelope.deny(
            GovernanceDenialCode::UnknownTarget,
            "A2A task is not accessible",
        );
    } else if envelope.authorization.is_allowed() {
        // Runtime identities are disclosed only after both scope and exact owner checks pass.
        envelope.correlation.session_id = Some(task.mapping.session_id.clone());
        envelope.correlation.run_id = Some(task.mapping.run_id.clone());
    }
    envelope
}

fn denied_unknown_task(
    correlation: CorrelationIdentity,
    principal: Option<&ProtocolPrincipal>,
    operation: &str,
    task_id: &str,
    scope: &str,
) -> GovernedAction<A2aAction> {
    let mut envelope = GovernanceEnvelope::evaluate(
        ProtocolKind::A2a,
        correlation,
        principal,
        operation,
        task_id,
        scopes(&[scope]),
    );
    if envelope.authorization.is_allowed() {
        envelope = envelope.deny(
            GovernanceDenialCode::UnknownTarget,
            "A2A task is not accessible",
        );
    }
    GovernedAction::denied(envelope)
}

fn list_query_hash(principal: &ProtocolPrincipal, request: &A2aListTasksRequest) -> String {
    crate::durability::stable_input_hash(&serde_json::json!({
        "subject": principal.subject,
        "principal_tenant": principal.tenant_id,
        "requested_tenant": request.tenant,
        "context_id": request.context_id,
        "status": request.status,
    }))
}

fn list_snapshot_hash(tasks: &[A2aTaskRecord]) -> String {
    crate::durability::stable_input_hash(
        &serde_json::to_value(tasks).expect("A2A task snapshot is always serializable"),
    )
}

fn encode_page_token(task_id: &str, snapshot_hash: &str, query_hash: &str) -> String {
    let cursor = A2aPageCursor {
        schema_version: A2A_PAGE_TOKEN_SCHEMA_VERSION,
        snapshot_hash: snapshot_hash.to_owned(),
        query_hash: query_hash.to_owned(),
        next_task_id: task_id.to_owned(),
    };
    let encoded = serde_json::to_vec(&cursor).expect("A2A page cursor is always serializable");
    URL_SAFE_NO_PAD.encode(encoded)
}

fn decode_page_token(page_token: &str) -> ProtocolResult<A2aPageCursor> {
    if page_token.is_empty() {
        return Err(ProtocolError::invalid("A2A page_token must not be empty"));
    }
    if page_token.len() > A2A_MAX_PAGE_TOKEN_BYTES {
        return Err(ProtocolError::invalid(format!(
            "A2A page_token must not exceed {A2A_MAX_PAGE_TOKEN_BYTES} bytes"
        )));
    }
    let decoded = URL_SAFE_NO_PAD
        .decode(page_token)
        .map_err(|_| ProtocolError::invalid("A2A page_token is not a valid opaque cursor"))?;
    let cursor: A2aPageCursor = serde_json::from_slice(&decoded)
        .map_err(|_| ProtocolError::invalid("A2A page_token has an invalid cursor payload"))?;
    if cursor.schema_version != A2A_PAGE_TOKEN_SCHEMA_VERSION {
        return Err(ProtocolError::invalid(
            "A2A page_token uses an unsupported cursor version",
        ));
    }
    validate_identifier("A2A page_token snapshot hash", &cursor.snapshot_hash)?;
    validate_identifier("A2A page_token query hash", &cursor.query_hash)?;
    validate_identifier("A2A page_token task id", &cursor.next_task_id)?;
    Ok(cursor)
}

#[cfg(test)]
mod capacity_tests {
    use super::*;
    use crate::protocols::GovernanceAuthorization;

    #[derive(Serialize)]
    struct LegacyMapperProxy<'a> {
        schema_version: u32,
        contexts: &'a BTreeMap<String, String>,
        context_owners: &'a BTreeMap<String, A2aContextOwner>,
        tasks: &'a BTreeMap<String, A2aTaskRecord>,
        receipts: &'a BTreeMap<String, A2aMessageReceipt>,
        dispatch_outbox: &'a BTreeMap<String, A2aDispatchOutboxRecord>,
        cancellation_outbox: &'a BTreeMap<String, A2aCancellationOutboxRecord>,
        pending_events: &'a BTreeMap<String, A2aPendingEventIntent>,
        next_sequence: u64,
        revision: u64,
    }

    fn legacy_mapper_proxy(mapper: &A2aMapper) -> LegacyMapperProxy<'_> {
        LegacyMapperProxy {
            schema_version: mapper.schema_version,
            contexts: &mapper.contexts,
            context_owners: &mapper.context_owners,
            tasks: &mapper.tasks,
            receipts: &mapper.receipts,
            dispatch_outbox: &mapper.dispatch_outbox,
            cancellation_outbox: &mapper.cancellation_outbox,
            pending_events: &mapper.pending_events,
            next_sequence: mapper.next_sequence,
            revision: mapper.revision,
        }
    }

    fn principal() -> ProtocolPrincipal {
        ProtocolPrincipal::new(
            "capacity-owner",
            [SEND_MESSAGE_SCOPE, TASK_READ_SCOPE, TASK_CANCEL_SCOPE],
        )
        .unwrap()
        .with_tenant("tenant-a")
        .unwrap()
    }

    fn correlation(id: &str) -> CorrelationIdentity {
        CorrelationIdentity::new(format!("correlation-{id}"), format!("request-{id}")).unwrap()
    }

    fn message(id: &str) -> A2aMessage {
        A2aMessage {
            message_id: id.into(),
            context_id: None,
            task_id: None,
            role: A2aRole::User,
            parts: vec![A2aPart::Text {
                text: "bounded".into(),
            }],
            metadata: BTreeMap::new(),
        }
    }

    fn targeted_message(id: &str, mapping: &A2aRunMapping) -> A2aMessage {
        A2aMessage {
            message_id: id.into(),
            context_id: Some(mapping.context_id.clone()),
            task_id: Some(mapping.task_id.clone()),
            ..message(id)
        }
    }

    #[test]
    fn cow_clone_detaches_only_the_written_map() {
        let principal = principal();
        let mut mapper = A2aMapper::new();
        mapper
            .prepare_send_message(
                message("cow-clone"),
                correlation("cow-clone"),
                Some(&principal),
            )
            .into_authorized()
            .unwrap();
        let dispatch_id = mapper.dispatch_outbox.keys().next().unwrap().clone();
        let mut candidate = mapper.clone();

        assert!(Arc::ptr_eq(&mapper.tasks.0, &candidate.tasks.0));
        assert!(Arc::ptr_eq(&mapper.receipts.0, &candidate.receipts.0));
        assert!(Arc::ptr_eq(
            &mapper.dispatch_outbox.0,
            &candidate.dispatch_outbox.0
        ));
        assert!(Arc::ptr_eq(
            &mapper.cancellation_outbox.0,
            &candidate.cancellation_outbox.0
        ));
        assert!(Arc::ptr_eq(
            &mapper.pending_events.0,
            &candidate.pending_events.0
        ));
        assert!(Arc::ptr_eq(
            &mapper.pending_event_schedule.0,
            &candidate.pending_event_schedule.0
        ));
        assert!(Arc::ptr_eq(
            &mapper.pending_event_schedule_by_owner.0,
            &candidate.pending_event_schedule_by_owner.0
        ));
        assert!(Arc::ptr_eq(
            &mapper.dispatch_event_readiness.0,
            &candidate.dispatch_event_readiness.0
        ));

        candidate.mark_dispatch_running(&dispatch_id).unwrap();

        assert!(!Arc::ptr_eq(
            &mapper.dispatch_outbox.0,
            &candidate.dispatch_outbox.0
        ));
        assert!(Arc::ptr_eq(&mapper.tasks.0, &candidate.tasks.0));
        assert!(Arc::ptr_eq(&mapper.receipts.0, &candidate.receipts.0));
        assert!(Arc::ptr_eq(
            &mapper.cancellation_outbox.0,
            &candidate.cancellation_outbox.0
        ));
        assert!(Arc::ptr_eq(
            &mapper.pending_events.0,
            &candidate.pending_events.0
        ));
        assert!(Arc::ptr_eq(
            &mapper.pending_event_schedule.0,
            &candidate.pending_event_schedule.0
        ));
        assert!(Arc::ptr_eq(
            &mapper.pending_event_schedule_by_owner.0,
            &candidate.pending_event_schedule_by_owner.0
        ));
        assert!(Arc::ptr_eq(
            &mapper.dispatch_event_readiness.0,
            &candidate.dispatch_event_readiness.0
        ));
    }

    #[test]
    fn cow_mapper_compact_json_matches_legacy_btree_wire_bytes() {
        let empty = A2aMapper::new();
        let empty_golden = br#"{"schema_version":4,"contexts":{},"context_owners":{},"tasks":{},"receipts":{},"dispatch_outbox":{},"cancellation_outbox":{},"pending_events":{},"next_sequence":1,"revision":0}"#;
        assert_eq!(serde_json::to_vec(&empty).unwrap(), empty_golden);
        assert_eq!(
            serde_json::to_vec(&empty).unwrap(),
            serde_json::to_vec(&legacy_mapper_proxy(&empty)).unwrap()
        );

        let principal = principal();
        let mut populated = A2aMapper::new();
        populated
            .prepare_send_message(
                message("cow-wire"),
                correlation("cow-wire"),
                Some(&principal),
            )
            .into_authorized()
            .unwrap();
        assert_eq!(
            serde_json::to_vec(&populated).unwrap(),
            serde_json::to_vec(&legacy_mapper_proxy(&populated)).unwrap()
        );
    }

    #[test]
    fn cow_candidate_mutation_preserves_original_bytes_and_canonical_indexes() {
        let principal = principal();
        let mut original = A2aMapper::new();
        let (_, action) = original
            .prepare_send_message(
                message("cow-candidate"),
                correlation("cow-candidate"),
                Some(&principal),
            )
            .into_authorized()
            .unwrap();
        let A2aAction::DispatchMessage { mapping, .. } = action else {
            panic!("expected dispatch action");
        };
        let original_bytes = serde_json::to_vec(&original).unwrap();
        let mut candidate = original.clone();

        candidate
            .transition_task(
                &mapping.task_id,
                A2aTaskState::InputRequired,
                Some("need input".into()),
            )
            .unwrap();

        assert_eq!(serde_json::to_vec(&original).unwrap(), original_bytes);
        original.validate_snapshot().unwrap();
        candidate.validate_snapshot().unwrap();
        assert_eq!(
            original.pending_event_schedule,
            rebuild_pending_event_schedule(&original.pending_events)
        );
        assert_eq!(
            original.pending_event_schedule_by_owner,
            rebuild_pending_event_schedule_by_owner(&original.pending_events)
        );
        assert_eq!(
            original.dispatch_event_readiness,
            rebuild_dispatch_event_readiness(&original.pending_events)
        );
    }

    #[test]
    fn full_snapshot_serializer_and_restore_enforce_the_raw_byte_cap() {
        let mapper = A2aMapper::new();
        let canonical =
            serialize_a2a_mapper_snapshot_bounded(&mapper, A2A_DEFAULT_MAPPER_SNAPSHOT_BYTES)
                .unwrap();
        assert_eq!(
            canonical,
            serialize_a2a_mapper_snapshot_bounded(&mapper, A2A_DEFAULT_MAPPER_SNAPSHOT_BYTES,)
                .unwrap()
        );
        let too_small = canonical.len().saturating_sub(1).max(1);
        let error = serialize_a2a_mapper_snapshot_bounded(&mapper, too_small).unwrap_err();
        assert!(error.message.contains("snapshot exceeds"));

        let oversized_raw = vec![b' '; too_small + 1];
        let error = deserialize_a2a_mapper_snapshot_bounded(&oversized_raw, too_small).unwrap_err();
        assert!(error
            .message
            .contains("persisted A2A mapper snapshot exceeds"));
        assert_eq!(
            A2A_DEFAULT_MAPPER_SNAPSHOT_BYTES,
            A2A_MAX_MAPPER_SNAPSHOT_BYTES
        );
    }

    #[test]
    fn byte_capacity_preserves_exact_idempotency_tombstone() {
        let principal = principal();
        let mut mapper = A2aMapper::new();
        let accepted = message("message-1");
        mapper
            .prepare_send_message(accepted.clone(), correlation("first"), Some(&principal))
            .into_authorized()
            .unwrap();

        mapper.receipt_bytes = A2A_MAX_RECEIPT_BYTES;
        assert!(mapper.preflight_send_message(&accepted, &principal).is_ok());
        let error = mapper
            .preflight_send_message(&message("message-2"), &principal)
            .unwrap_err();
        assert_eq!(error.code, ProtocolErrorCode::Conflict);

        let oversized = A2aMessage {
            parts: vec![A2aPart::Text {
                text: "x".repeat(A2A_MAX_MESSAGE_BYTES),
            }],
            ..message("oversized")
        };
        assert_eq!(
            oversized.validate().unwrap_err().code,
            ProtocolErrorCode::InvalidRequest
        );
    }

    #[test]
    fn cancellation_is_nonterminal_until_runtime_confirmation() {
        let principal = principal();
        let mut mapper = A2aMapper::new();
        let (_, action) = mapper
            .prepare_send_message(message("cancel-me"), correlation("send"), Some(&principal))
            .into_authorized()
            .unwrap();
        let A2aAction::DispatchMessage { mapping, .. } = action else {
            panic!("expected dispatch action");
        };

        let (_, action) = mapper
            .prepare_cancel_task(&mapping.task_id, correlation("cancel"), Some(&principal))
            .into_authorized()
            .unwrap();
        let A2aAction::CancelTask { task } = action else {
            panic!("expected cancellation intent");
        };
        assert_eq!(task.state, A2aTaskState::Working);
        assert_eq!(
            task.status_message.as_deref(),
            Some("cancellation requested")
        );
        assert!(!mapper.tasks()[&mapping.task_id].state.is_terminal());

        let cancellation = mapper
            .cancellation_for_task(&mapping.task_id, &principal)
            .cloned()
            .unwrap();
        assert_eq!(
            mapper
                .transition_task(&mapping.task_id, A2aTaskState::Cancelled, None)
                .unwrap_err()
                .code,
            ProtocolErrorCode::InvalidTransition
        );
        assert_eq!(
            mapper
                .acknowledge_cancellation(&cancellation.cancellation_id, 0, None)
                .unwrap_err()
                .code,
            ProtocolErrorCode::InvalidTransition
        );
        mapper
            .mark_cancellation_running(&cancellation.cancellation_id)
            .unwrap();
        let attempt = mapper
            .cancellation_for_task(&mapping.task_id, &principal)
            .unwrap()
            .attempts;
        assert_eq!(
            mapper
                .acknowledge_cancellation(&cancellation.cancellation_id, attempt - 1, None)
                .unwrap_err()
                .code,
            ProtocolErrorCode::InvalidTransition
        );
        mapper
            .acknowledge_cancellation(&cancellation.cancellation_id, attempt, None)
            .unwrap();
        let denied = mapper.prepare_cancel_task(
            &mapping.task_id,
            correlation("cancel-again"),
            Some(&principal),
        );
        assert!(matches!(
            denied.envelope.authorization,
            GovernanceAuthorization::Denied {
                code: GovernanceDenialCode::StateConflict,
                ..
            }
        ));
    }

    #[test]
    fn unsettled_cancellation_blocks_new_and_resumed_sends_without_mutation() {
        let principal = principal();
        for (label, waiting_state) in [
            ("working", None),
            ("input-required", Some(A2aTaskState::InputRequired)),
            ("auth-required", Some(A2aTaskState::AuthRequired)),
        ] {
            let mut mapper = A2aMapper::new();
            let (_, action) = mapper
                .prepare_send_message(
                    message(&format!("{label}-initial")),
                    correlation(&format!("{label}-initial")),
                    Some(&principal),
                )
                .into_authorized()
                .unwrap();
            let A2aAction::DispatchMessage { mapping, .. } = action else {
                panic!("expected initial dispatch action");
            };
            for event in mapper.pending_events() {
                mapper.mark_event_settled(&event.event_id).unwrap();
            }
            let initial_dispatch_id = mapper
                .dispatch_for_message(&format!("{label}-initial"), &principal)
                .unwrap()
                .dispatch_id
                .clone();
            mapper.mark_dispatch_settled(&initial_dispatch_id).unwrap();

            // This accepted task-targeted message becomes the content-bound idempotency proof.
            // Retrying it after cancellation may read its receipt, but may not reschedule work.
            let accepted_retry =
                targeted_message(&format!("{label}-accepted-before-cancel"), &mapping);
            mapper
                .prepare_send_message(
                    accepted_retry.clone(),
                    correlation(&format!("{label}-accepted-before-cancel")),
                    Some(&principal),
                )
                .into_authorized()
                .unwrap();
            for event in mapper.pending_events() {
                mapper.mark_event_settled(&event.event_id).unwrap();
            }
            let retry_dispatch_id = mapper
                .dispatch_for_message(&accepted_retry.message_id, &principal)
                .unwrap()
                .dispatch_id
                .clone();
            mapper.mark_dispatch_settled(&retry_dispatch_id).unwrap();

            if let Some(waiting_state) = waiting_state {
                mapper
                    .transition_task(
                        &mapping.task_id,
                        waiting_state,
                        Some(format!("{label} wait")),
                    )
                    .unwrap();
            }
            mapper
                .prepare_cancel_task(
                    &mapping.task_id,
                    correlation(&format!("{label}-cancel")),
                    Some(&principal),
                )
                .into_authorized()
                .unwrap();

            // Cover every non-settled outbox state across the task-state matrix.
            let cancellation_id = mapper
                .cancellation_for_task(&mapping.task_id, &principal)
                .unwrap()
                .cancellation_id
                .clone();
            if label != "working" {
                mapper.mark_cancellation_running(&cancellation_id).unwrap();
            }
            if label == "auth-required" {
                mapper
                    .mark_cancellation_reconcile_pending(&cancellation_id, "sanitized")
                    .unwrap();
            }

            let before = mapper.clone();
            let before_serialized = serde_json::to_vec(&mapper).unwrap();
            assert!(mapper
                .preflight_send_message(&accepted_retry, &principal)
                .is_ok());
            let (_, duplicate) = mapper
                .prepare_send_message(
                    accepted_retry,
                    correlation(&format!("{label}-exact-retry")),
                    Some(&principal),
                )
                .into_authorized()
                .unwrap();
            assert!(matches!(duplicate, A2aAction::DuplicateMessage { .. }));
            assert_eq!(mapper, before);
            assert_eq!(serde_json::to_vec(&mapper).unwrap(), before_serialized);

            let blocked = targeted_message(&format!("{label}-blocked"), &mapping);
            let preflight_error = mapper
                .preflight_send_message(&blocked, &principal)
                .unwrap_err();
            assert_eq!(preflight_error.code, ProtocolErrorCode::InvalidTransition);
            assert_eq!(
                preflight_error.message,
                A2A_UNSETTLED_CANCELLATION_SEND_REASON
            );

            let mut candidate = mapper.clone();
            let candidate_denied = candidate.prepare_send_message_candidate_with_response_policy(
                blocked.clone(),
                correlation(&format!("{label}-candidate")),
                Some(&principal),
                A2aSendResponsePolicy::Blocking,
            );
            assert!(matches!(
                &candidate_denied.envelope.authorization,
                GovernanceAuthorization::Denied {
                    code: GovernanceDenialCode::StateConflict,
                    reason,
                } if reason == A2A_UNSETTLED_CANCELLATION_SEND_REASON
            ));
            assert_eq!(candidate, before);
            assert_eq!(serde_json::to_vec(&candidate).unwrap(), before_serialized);

            let denied = mapper.prepare_send_message(
                blocked,
                correlation(&format!("{label}-direct")),
                Some(&principal),
            );
            assert!(matches!(
                &denied.envelope.authorization,
                GovernanceAuthorization::Denied {
                    code: GovernanceDenialCode::StateConflict,
                    reason,
                } if reason == A2A_UNSETTLED_CANCELLATION_SEND_REASON
            ));
            assert_eq!(mapper.revision(), before.revision());
            assert_eq!(mapper.receipts(), before.receipts());
            assert_eq!(mapper.dispatch_outbox(), before.dispatch_outbox());
            assert_eq!(
                mapper.pending_event_intents(),
                before.pending_event_intents()
            );
            assert_eq!(mapper, before);
            assert_eq!(serde_json::to_vec(&mapper).unwrap(), before_serialized);
        }
    }

    #[test]
    fn cancellation_intent_event_and_control_are_atomic_and_duplicate_safe() {
        let principal = principal();
        let mut mapper = A2aMapper::new();
        let (_, action) = mapper
            .prepare_send_message(
                message("cancel-atomic"),
                correlation("cancel-atomic-send"),
                Some(&principal),
            )
            .into_authorized()
            .unwrap();
        let A2aAction::DispatchMessage { mapping, .. } = action else {
            panic!("expected dispatch action");
        };

        let (_, first_action) = mapper
            .prepare_cancel_task(
                &mapping.task_id,
                correlation("cancel-atomic-first"),
                Some(&principal),
            )
            .into_authorized()
            .unwrap();
        let first_record = mapper
            .cancellation_for_task(&mapping.task_id, &principal)
            .cloned()
            .expect("cancellation control exists");
        assert_eq!(first_record.state, A2aCancellationOutboxState::Queued);
        assert_eq!(first_record.task_id, mapping.task_id);
        assert_eq!(first_record.context_id, mapping.context_id);
        assert_eq!(first_record.session_id, mapping.session_id);
        assert_eq!(first_record.run_id, mapping.run_id);
        assert_eq!(first_record.created_revision, mapper.revision());
        assert_eq!(mapper.pending_cancellations(), vec![first_record.clone()]);
        assert_eq!(
            mapper
                .pending_events()
                .iter()
                .filter(|event| event.kind == A2aPendingEventKind::CancellationRequested)
                .count(),
            1
        );

        let revision = mapper.revision();
        let event_count = mapper.pending_event_intents().len();
        let (_, duplicate_action) = mapper
            .prepare_cancel_task(
                &mapping.task_id,
                correlation("cancel-atomic-duplicate"),
                Some(&principal),
            )
            .into_authorized()
            .unwrap();
        assert_eq!(duplicate_action, first_action);
        assert_eq!(mapper.revision(), revision);
        assert_eq!(mapper.cancellation_outbox.len(), 1);
        assert_eq!(mapper.pending_event_intents().len(), event_count);
        assert_eq!(
            mapper.cancellation_for_task(&mapping.task_id, &principal),
            Some(&first_record)
        );

        let mut exhausted = A2aMapper::new();
        let (_, action) = exhausted
            .prepare_send_message(
                message("cancel-capacity"),
                correlation("cancel-capacity-send"),
                Some(&principal),
            )
            .into_authorized()
            .unwrap();
        let A2aAction::DispatchMessage { mapping, .. } = action else {
            panic!("expected dispatch action");
        };
        exhausted.cancellation_bytes = A2A_MAX_CANCELLATION_BYTES;
        let before_task = exhausted.tasks()[&mapping.task_id].clone();
        let before_revision = exhausted.revision();
        let before_events = exhausted.pending_event_intents().len();
        let denied = exhausted.prepare_cancel_task(
            &mapping.task_id,
            correlation("cancel-capacity-denied"),
            Some(&principal),
        );
        assert!(!denied.is_authorized());
        assert_eq!(exhausted.revision(), before_revision);
        assert_eq!(exhausted.tasks()[&mapping.task_id], before_task);
        assert_eq!(exhausted.pending_event_intents().len(), before_events);
        assert!(exhausted.cancellation_outbox.is_empty());
    }

    #[test]
    fn cancellation_restart_reconstructs_exact_action_and_checks_owner_scope() {
        let principal = principal();
        let mut mapper = A2aMapper::new();
        let (_, action) = mapper
            .prepare_send_message(
                message("cancel-restore"),
                correlation("cancel-restore-send"),
                Some(&principal),
            )
            .into_authorized()
            .unwrap();
        let A2aAction::DispatchMessage { mapping, .. } = action else {
            panic!("expected dispatch action");
        };
        let (_, original_action) = mapper
            .prepare_cancel_task(
                &mapping.task_id,
                correlation("cancel-restore-request"),
                Some(&principal),
            )
            .into_authorized()
            .unwrap();
        let original = mapper
            .cancellation_for_task(&mapping.task_id, &principal)
            .cloned()
            .unwrap();

        let restored: A2aMapper =
            serde_json::from_value(serde_json::to_value(&mapper).unwrap()).unwrap();
        assert_eq!(restored.pending_cancellations(), vec![original.clone()]);
        let (envelope, action) = restored
            .reconstruct_cancel(&original.cancellation_id, &principal)
            .unwrap();
        assert_eq!(action, original_action);
        assert_eq!(
            envelope.correlation.session_id.as_deref(),
            Some(original.session_id.as_str())
        );
        assert_eq!(
            envelope.correlation.run_id.as_deref(),
            Some(original.run_id.as_str())
        );

        let wrong_owner = ProtocolPrincipal::new("capacity-owner", [TASK_CANCEL_SCOPE])
            .unwrap()
            .with_tenant("tenant-b")
            .unwrap();
        assert_eq!(
            restored
                .reconstruct_cancel(&original.cancellation_id, &wrong_owner)
                .unwrap_err()
                .code,
            ProtocolErrorCode::NotFound
        );
        let missing_scope = ProtocolPrincipal::new("capacity-owner", [TASK_READ_SCOPE])
            .unwrap()
            .with_tenant("tenant-a")
            .unwrap();
        assert_eq!(
            restored
                .reconstruct_cancel(&original.cancellation_id, &missing_scope)
                .unwrap_err()
                .code,
            ProtocolErrorCode::Forbidden
        );
    }

    #[test]
    fn cancellation_attempts_are_bounded_sanitized_and_terminally_settled() {
        let principal = principal();
        let mut mapper = A2aMapper::new();
        let (_, action) = mapper
            .prepare_send_message(
                message("cancel-state"),
                correlation("cancel-state-send"),
                Some(&principal),
            )
            .into_authorized()
            .unwrap();
        let A2aAction::DispatchMessage { mapping, .. } = action else {
            panic!("expected dispatch action");
        };
        mapper
            .prepare_cancel_task(
                &mapping.task_id,
                correlation("cancel-state-request"),
                Some(&principal),
            )
            .into_authorized()
            .unwrap();
        let cancellation_id = mapper
            .cancellation_for_task(&mapping.task_id, &principal)
            .unwrap()
            .cancellation_id
            .clone();

        assert_eq!(
            mapper
                .transition_task(
                    &mapping.task_id,
                    A2aTaskState::Working,
                    Some("still working".into()),
                )
                .unwrap_err()
                .code,
            ProtocolErrorCode::InvalidTransition
        );
        for attempt in 1..=A2A_MAX_CANCELLATION_ATTEMPTS {
            mapper.mark_cancellation_running(&cancellation_id).unwrap();
            assert_eq!(
                mapper.cancellation_outbox[&cancellation_id].attempts,
                attempt
            );
            mapper
                .mark_cancellation_reconcile_pending(
                    &cancellation_id,
                    "Bearer must-never-enter-the-snapshot",
                )
                .unwrap();
        }
        let record = &mapper.cancellation_outbox[&cancellation_id];
        assert_eq!(record.state, A2aCancellationOutboxState::ReconcilePending);
        assert_eq!(
            record.last_error.as_deref(),
            Some(A2A_CANCELLATION_RECONCILE_REASON)
        );
        assert!(!serde_json::to_string(&mapper)
            .unwrap()
            .contains("must-never-enter-the-snapshot"));
        assert_eq!(
            mapper
                .mark_cancellation_running(&cancellation_id)
                .unwrap_err()
                .code,
            ProtocolErrorCode::Conflict
        );

        assert_eq!(
            mapper
                .transition_task(&mapping.task_id, A2aTaskState::Cancelled, None)
                .unwrap_err()
                .code,
            ProtocolErrorCode::InvalidTransition
        );
        mapper
            .acknowledge_cancellation(&cancellation_id, A2A_MAX_CANCELLATION_ATTEMPTS, None)
            .unwrap();
        let settled = &mapper.cancellation_outbox[&cancellation_id];
        assert_eq!(settled.state, A2aCancellationOutboxState::Settled);
        assert_eq!(settled.updated_revision, mapper.revision());
        assert!(mapper.pending_cancellations().is_empty());
        let revision = mapper.revision();
        mapper.mark_cancellation_settled(&cancellation_id).unwrap();
        assert_eq!(mapper.revision(), revision);
    }

    #[test]
    fn cancellation_legacy_restore_and_snapshot_bindings_are_strict() {
        let principal = principal();
        let mut mapper = A2aMapper::new();
        let (_, action) = mapper
            .prepare_send_message(
                message("cancel-legacy"),
                correlation("cancel-legacy-send"),
                Some(&principal),
            )
            .into_authorized()
            .unwrap();
        let A2aAction::DispatchMessage { mapping, .. } = action else {
            panic!("expected dispatch action");
        };
        mapper
            .prepare_cancel_task(
                &mapping.task_id,
                correlation("cancel-legacy-request"),
                Some(&principal),
            )
            .into_authorized()
            .unwrap();

        let mut legacy = serde_json::to_value(&mapper).unwrap();
        legacy
            .as_object_mut()
            .unwrap()
            .remove("cancellation_outbox");
        let restored: A2aMapper = serde_json::from_value(legacy).unwrap();
        let rebuilt = restored
            .cancellation_for_task(&mapping.task_id, &principal)
            .unwrap();
        assert_eq!(rebuilt.state, A2aCancellationOutboxState::ReconcilePending);
        assert_eq!(
            rebuilt.last_error.as_deref(),
            Some(A2A_CANCELLATION_RECONCILE_REASON)
        );

        let mut legacy_without_events = serde_json::to_value(&mapper).unwrap();
        legacy_without_events
            .as_object_mut()
            .unwrap()
            .remove("cancellation_outbox");
        legacy_without_events
            .as_object_mut()
            .unwrap()
            .remove("pending_events");
        let rebuilt_all: A2aMapper = serde_json::from_value(legacy_without_events).unwrap();
        assert_eq!(rebuilt_all.pending_cancellations().len(), 1);
        assert_eq!(
            rebuilt_all.pending_events()[0].kind,
            A2aPendingEventKind::CancellationRequested
        );

        let mut missing_control = serde_json::to_value(&mapper).unwrap();
        missing_control["cancellation_outbox"] = serde_json::json!({});
        assert!(serde_json::from_value::<A2aMapper>(missing_control).is_err());

        let mut wrong_run = serde_json::to_value(&mapper).unwrap();
        wrong_run["cancellation_outbox"]
            .as_object_mut()
            .unwrap()
            .values_mut()
            .next()
            .unwrap()["run_id"] = Value::String("forged-run".into());
        assert!(serde_json::from_value::<A2aMapper>(wrong_run).is_err());
    }

    #[test]
    fn receipt_byte_accounting_is_rebuilt_on_snapshot_restore() {
        let principal = principal();
        let mut mapper = A2aMapper::new();
        mapper
            .prepare_send_message(
                message("roundtrip"),
                correlation("roundtrip"),
                Some(&principal),
            )
            .into_authorized()
            .unwrap();
        let expected = receipt_storage_bytes(mapper.receipts()).unwrap();
        let message_only: usize = mapper
            .receipts()
            .values()
            .map(|receipt| message_storage_bytes(&receipt.message).unwrap())
            .sum();
        assert_eq!(mapper.receipt_bytes, expected);
        assert!(expected > message_only);
        let snapshot = serde_json::to_value(&mapper).unwrap();
        assert!(snapshot.get("receipt_bytes").is_none());
        let restored: A2aMapper = serde_json::from_value(snapshot).unwrap();
        assert_eq!(restored, mapper);
    }

    #[test]
    fn accepted_send_atomically_creates_queued_dispatch_and_event() {
        let principal = principal();
        let mut mapper = A2aMapper::new();
        let (_, action) = mapper
            .prepare_send_message(message("atomic"), correlation("atomic"), Some(&principal))
            .into_authorized()
            .unwrap();
        let A2aAction::DispatchMessage { mapping, .. } = action else {
            panic!("expected dispatch action");
        };
        let dispatch = mapper
            .dispatch_for_message("atomic", &principal)
            .expect("accepted send has an outbox record");
        assert_eq!(dispatch.state, A2aDispatchOutboxState::Queued);
        assert_eq!(dispatch.task_id, mapping.task_id);
        assert_eq!(dispatch.context_id, mapping.context_id);
        assert_eq!(dispatch.session_id, mapping.session_id);
        assert_eq!(dispatch.run_id, mapping.run_id);
        assert_eq!(mapper.tasks().len(), 1);
        assert_eq!(mapper.receipts().len(), 1);
        assert_eq!(mapper.dispatch_outbox().len(), 1);
        assert_eq!(mapper.pending_events().len(), 1);
        assert_eq!(
            mapper.pending_events()[0].kind,
            A2aPendingEventKind::TaskCreated
        );

        let mut exhausted = A2aMapper::new();
        exhausted.dispatch_bytes = A2A_MAX_DISPATCH_BYTES;
        let denied = exhausted.prepare_send_message(
            message("must-rollback"),
            correlation("must-rollback"),
            Some(&principal),
        );
        assert!(!denied.is_authorized());
        assert_eq!(exhausted.revision(), 0);
        assert!(exhausted.tasks().is_empty());
        assert!(exhausted.receipts().is_empty());
        assert!(exhausted.dispatch_outbox().is_empty());
        assert!(exhausted.pending_event_intents().is_empty());
    }

    #[test]
    fn exact_retry_reuses_one_dispatch_record_without_revision_change() {
        let principal = principal();
        let mut mapper = A2aMapper::new();
        let request = message("retry");
        mapper
            .prepare_send_message(request.clone(), correlation("first"), Some(&principal))
            .into_authorized()
            .unwrap();
        let revision = mapper.revision();
        let original = mapper
            .dispatch_for_message("retry", &principal)
            .cloned()
            .unwrap();
        let (_, duplicate) = mapper
            .prepare_send_message(request, correlation("retry"), Some(&principal))
            .into_authorized()
            .unwrap();
        assert!(matches!(duplicate, A2aAction::DuplicateMessage { .. }));
        assert_eq!(mapper.revision(), revision);
        assert_eq!(mapper.dispatch_outbox().len(), 1);
        assert_eq!(
            mapper.dispatch_for_message("retry", &principal),
            Some(&original)
        );
    }

    #[test]
    fn dispatch_running_reconcile_retry_and_settlement_are_bounded() {
        let principal = principal();
        let mut mapper = A2aMapper::new();
        mapper
            .prepare_send_message(
                message("state-machine"),
                correlation("state"),
                Some(&principal),
            )
            .into_authorized()
            .unwrap();
        let dispatch_id = mapper
            .dispatch_for_message("state-machine", &principal)
            .unwrap()
            .dispatch_id
            .clone();
        mapper.mark_dispatch_running(&dispatch_id).unwrap();
        mapper
            .mark_dispatch_reconcile_pending(&dispatch_id, "Bearer super-secret-wire-value")
            .unwrap();
        let reconciled = &mapper.dispatch_outbox()[&dispatch_id];
        assert_eq!(reconciled.state, A2aDispatchOutboxState::ReconcilePending);
        assert_eq!(reconciled.last_error.as_deref(), Some(A2A_RECONCILE_REASON));
        assert!(!serde_json::to_string(&mapper)
            .unwrap()
            .contains("super-secret-wire-value"));
        mapper.mark_dispatch_running(&dispatch_id).unwrap();
        assert_eq!(mapper.dispatch_outbox()[&dispatch_id].attempts, 2);
        mapper.mark_dispatch_settled(&dispatch_id).unwrap();
        assert_eq!(
            mapper.dispatch_outbox()[&dispatch_id].state,
            A2aDispatchOutboxState::Settled
        );
        assert!(mapper.pending_dispatches().is_empty());
        assert_eq!(
            mapper.mark_dispatch_running(&dispatch_id).unwrap_err().code,
            ProtocolErrorCode::InvalidTransition
        );

        let mut bounded = A2aMapper::new();
        bounded
            .prepare_send_message(
                message("attempt-bound"),
                correlation("bound"),
                Some(&principal),
            )
            .into_authorized()
            .unwrap();
        let bounded_id = bounded
            .dispatch_for_message("attempt-bound", &principal)
            .unwrap()
            .dispatch_id
            .clone();
        for attempt in 1..=A2A_MAX_DISPATCH_ATTEMPTS {
            bounded.mark_dispatch_running(&bounded_id).unwrap();
            assert_eq!(bounded.dispatch_outbox()[&bounded_id].attempts, attempt);
            bounded
                .mark_dispatch_reconcile_pending(&bounded_id, "opaque failure")
                .unwrap();
        }
        assert_eq!(
            bounded.mark_dispatch_running(&bounded_id).unwrap_err().code,
            ProtocolErrorCode::Conflict
        );
    }

    #[test]
    fn snapshot_restart_recovers_same_dispatch_identity_and_action() {
        let principal = principal();
        let mut mapper = A2aMapper::new();
        let (_, original_action) = mapper
            .prepare_send_message(message("restore"), correlation("restore"), Some(&principal))
            .into_authorized()
            .unwrap();
        let original = mapper
            .dispatch_for_message("restore", &principal)
            .cloned()
            .unwrap();
        let restored: A2aMapper = serde_json::from_value(serde_json::to_value(&mapper).unwrap())
            .expect("snapshot restores");
        let pending = restored.pending_dispatches();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].dispatch_id, original.dispatch_id);
        assert_eq!(pending[0].run_id, original.run_id);
        let (envelope, action) = restored
            .reconstruct_dispatch(&original.dispatch_id, &principal)
            .unwrap();
        assert_eq!(action, original_action);
        assert_eq!(
            envelope.correlation.run_id.as_deref(),
            Some(original.run_id.as_str())
        );

        let wrong_owner = ProtocolPrincipal::new("capacity-owner", [SEND_MESSAGE_SCOPE])
            .unwrap()
            .with_tenant("tenant-b")
            .unwrap();
        assert_eq!(
            restored
                .reconstruct_dispatch(&original.dispatch_id, &wrong_owner)
                .unwrap_err()
                .code,
            ProtocolErrorCode::NotFound
        );
    }

    #[test]
    fn input_required_and_terminal_tasks_never_reenter_pending_dispatches() {
        let principal = principal();
        for state in [A2aTaskState::InputRequired, A2aTaskState::Completed] {
            let mut mapper = A2aMapper::new();
            let (_, action) = mapper
                .prepare_send_message(
                    message(&format!("suppressed-{state:?}")),
                    correlation(&format!("suppressed-{state:?}")),
                    Some(&principal),
                )
                .into_authorized()
                .unwrap();
            let A2aAction::DispatchMessage { mapping, .. } = action else {
                panic!("expected dispatch action");
            };
            let dispatch_id = mapper
                .dispatch_for_message(&format!("suppressed-{state:?}"), &principal)
                .unwrap()
                .dispatch_id
                .clone();
            mapper
                .transition_task(&mapping.task_id, state, None)
                .unwrap();
            assert_eq!(
                mapper.dispatch_outbox()[&dispatch_id].state,
                A2aDispatchOutboxState::Settled
            );
            assert!(mapper.pending_dispatches().is_empty());
            assert_eq!(
                mapper
                    .reconstruct_dispatch(&dispatch_id, &principal)
                    .unwrap_err()
                    .code,
                ProtocolErrorCode::InvalidTransition
            );
        }
    }

    #[test]
    fn transient_event_failures_remain_retryable_without_blocking_healthy_work() {
        let principal = principal();
        let mut mapper = A2aMapper::new();
        for message_id in ["poison-event", "healthy-event"] {
            mapper
                .prepare_send_message(
                    message(message_id),
                    correlation(message_id),
                    Some(&principal),
                )
                .into_authorized()
                .unwrap();
        }
        let poison = mapper
            .pending_events()
            .into_iter()
            .find(|event| event.message_id.as_deref() == Some("poison-event"))
            .unwrap();
        let healthy = mapper
            .pending_events()
            .into_iter()
            .find(|event| event.message_id.as_deref() == Some("healthy-event"))
            .unwrap();
        let task_before = mapper.tasks()[&poison.task_id].clone();

        for failure in 1..=A2A_MAX_EVENT_ATTEMPTS + 2 {
            let state = mapper
                .mark_event_reconcile_pending(
                    &poison.event_id,
                    "Bearer must-never-enter-the-snapshot",
                    u64::from(failure) * (A2A_EVENT_RETRY_MAX_MS + 1),
                )
                .unwrap();
            assert_eq!(mapper.pending_events[&poison.event_id].attempts, 0);
            assert_eq!(
                mapper.pending_events[&poison.event_id].transient_failures,
                failure
            );
            assert_eq!(state, A2aPendingEventState::ReconcilePending);
        }

        let retryable = mapper.pending_events[&poison.event_id].clone();
        assert_eq!(retryable.state, A2aPendingEventState::ReconcilePending);
        assert_eq!(retryable.quarantine_reason, None);
        assert_eq!(
            retryable.last_error.as_deref(),
            Some(A2A_EVENT_RECONCILE_REASON)
        );
        assert!(!serde_json::to_string(&mapper)
            .unwrap()
            .contains("must-never-enter-the-snapshot"));
        assert_eq!(mapper.tasks()[&poison.task_id], task_before);
        assert!(!mapper.tasks()[&poison.task_id].state.is_terminal());
        assert!(mapper.quarantined_events().is_empty());

        mapper.mark_event_settled(&healthy.event_id).unwrap();
        assert_eq!(mapper.pending_events(), vec![retryable]);
        mapper.mark_event_settled(&poison.event_id).unwrap();
        assert!(mapper.pending_events().is_empty());
        assert!(mapper.quarantined_events().is_empty());
    }

    #[test]
    fn task_targeted_message_waits_for_earlier_failed_acceptance_event() {
        let principal = principal();
        for quarantine in [false, true] {
            let mut mapper = A2aMapper::new();
            let first = message(if quarantine {
                "ordered-poison-first"
            } else {
                "ordered-backoff-first"
            });
            let (_, action) = mapper
                .prepare_send_message(
                    first.clone(),
                    correlation("ordered-first"),
                    Some(&principal),
                )
                .into_authorized()
                .unwrap();
            let A2aAction::DispatchMessage { mapping, .. } = action else {
                panic!("expected initial dispatch action");
            };
            let event_id = mapper
                .pending_events()
                .into_iter()
                .find(|event| event.message_id.as_deref() == Some(first.message_id.as_str()))
                .unwrap()
                .event_id;
            let next = targeted_message("ordered-second", &mapping);
            let pending_error = mapper
                .preflight_send_message(&next, &principal)
                .unwrap_err();
            assert_eq!(pending_error.code, ProtocolErrorCode::InvalidTransition);
            assert_eq!(
                pending_error.message,
                A2A_UNSETTLED_MESSAGE_EVENT_SEND_REASON
            );
            if quarantine {
                mapper
                    .mark_event_quarantined(
                        &event_id,
                        A2aEventQuarantineReason::DeterministicPoison,
                    )
                    .unwrap();
            } else {
                mapper
                    .mark_event_reconcile_pending(&event_id, "sanitized", 1_000_000)
                    .unwrap();
            }

            let before = mapper.clone();
            let error = mapper
                .preflight_send_message(&next, &principal)
                .unwrap_err();
            assert_eq!(error.code, ProtocolErrorCode::InvalidTransition);
            assert_eq!(error.message, A2A_UNSETTLED_MESSAGE_EVENT_SEND_REASON);
            let denied =
                mapper.prepare_send_message(next, correlation("ordered-second"), Some(&principal));
            assert!(matches!(
                &denied.envelope.authorization,
                GovernanceAuthorization::Denied {
                    reason,
                    ..
                } if reason == A2A_UNSETTLED_MESSAGE_EVENT_SEND_REASON
            ));
            assert_eq!(mapper, before);

            // The content-bound retry remains readable and cannot create a second dispatch.
            let (_, retry) = mapper
                .prepare_send_message(first, correlation("ordered-retry"), Some(&principal))
                .into_authorized()
                .unwrap();
            assert!(matches!(retry, A2aAction::DuplicateMessage { .. }));
            assert_eq!(mapper, before);
        }
    }

    #[test]
    fn task_targeted_message_waits_for_earlier_active_dispatch() {
        let principal = principal();
        let mut mapper = A2aMapper::new();
        let first = message("ordered-active-first");
        let (_, action) = mapper
            .prepare_send_message(first.clone(), correlation("active-first"), Some(&principal))
            .into_authorized()
            .unwrap();
        let A2aAction::DispatchMessage { mapping, .. } = action else {
            panic!("expected initial dispatch action");
        };
        for event in mapper.pending_events() {
            mapper.mark_event_settled(&event.event_id).unwrap();
        }
        let dispatch_id = mapper
            .dispatch_for_message(&first.message_id, &principal)
            .unwrap()
            .dispatch_id
            .clone();
        let next = targeted_message("ordered-active-second", &mapping);

        for state in [
            A2aDispatchOutboxState::Queued,
            A2aDispatchOutboxState::Running,
            A2aDispatchOutboxState::ReconcilePending,
        ] {
            if state == A2aDispatchOutboxState::Running {
                mapper.mark_dispatch_running(&dispatch_id).unwrap();
            } else if state == A2aDispatchOutboxState::ReconcilePending {
                mapper
                    .mark_dispatch_reconcile_pending(&dispatch_id, "sanitized")
                    .unwrap();
            }
            let before = mapper.clone();
            let error = mapper
                .preflight_send_message(&next, &principal)
                .unwrap_err();
            assert_eq!(error.code, ProtocolErrorCode::InvalidTransition);
            assert_eq!(error.message, A2A_UNSETTLED_MESSAGE_DISPATCH_SEND_REASON);
            assert_eq!(mapper, before);
            let (_, duplicate) = mapper
                .prepare_send_message(first.clone(), correlation("active-retry"), Some(&principal))
                .into_authorized()
                .unwrap();
            assert!(matches!(duplicate, A2aAction::DuplicateMessage { .. }));
            assert_eq!(mapper, before);
        }

        mapper.mark_dispatch_settled(&dispatch_id).unwrap();
        assert!(mapper.preflight_send_message(&next, &principal).is_ok());
    }

    #[test]
    fn output_completion_rejects_stale_attempt_and_durable_cancellation_fence() {
        let principal = principal();
        let mut mapper = A2aMapper::new();
        let first = message("output-fence");
        let (_, action) = mapper
            .prepare_send_message(first.clone(), correlation("output-fence"), Some(&principal))
            .into_authorized()
            .unwrap();
        let A2aAction::DispatchMessage { mapping, .. } = action else {
            panic!("expected output dispatch action");
        };
        for event in mapper.pending_events() {
            mapper.mark_event_settled(&event.event_id).unwrap();
        }
        let dispatch_id = mapper
            .dispatch_for_message(&first.message_id, &principal)
            .unwrap()
            .dispatch_id
            .clone();
        mapper.mark_dispatch_running(&dispatch_id).unwrap();
        let attempt = mapper.dispatch_outbox()[&dispatch_id].attempts;
        let artifact = A2aArtifact {
            artifact_id: "fenced-artifact".into(),
            name: None,
            description: None,
            parts: vec![A2aContentPart::Text {
                text: "fenced".into(),
                media_type: None,
            }],
            metadata: BTreeMap::new(),
        };

        let before_stale = mapper.clone();
        let stale = mapper
            .complete_dispatch_with_artifacts(&dispatch_id, attempt - 1, vec![artifact.clone()])
            .unwrap_err();
        assert_eq!(stale.code, ProtocolErrorCode::InvalidTransition);
        assert_eq!(mapper, before_stale);

        mapper
            .prepare_cancel_task(
                &mapping.task_id,
                correlation("output-fence-cancel"),
                Some(&principal),
            )
            .into_authorized()
            .unwrap();
        let before_cancelled_completion = mapper.clone();
        let cancelled = mapper
            .complete_dispatch_with_artifacts(&dispatch_id, attempt, vec![artifact])
            .unwrap_err();
        assert_eq!(cancelled.code, ProtocolErrorCode::InvalidTransition);
        assert!(cancelled.message.contains("cancellation fence"));
        assert_eq!(mapper, before_cancelled_completion);
    }

    #[test]
    fn restored_clock_rollback_clamps_event_retry_to_one_maximum_window() {
        let principal = principal();
        let mut mapper = A2aMapper::new();
        mapper
            .prepare_send_message(
                message("rollback-retry"),
                correlation("rollback-retry"),
                Some(&principal),
            )
            .into_authorized()
            .unwrap();
        let event_id = mapper.pending_events().pop().unwrap().event_id;
        mapper
            .mark_event_reconcile_pending(&event_id, "sanitized", 1_000_000)
            .unwrap();
        let restored: A2aMapper = deserialize_a2a_mapper_snapshot_bounded(
            &serialize_a2a_mapper_snapshot_bounded(&mapper, A2A_DEFAULT_MAPPER_SNAPSHOT_BYTES)
                .unwrap(),
            A2A_DEFAULT_MAPPER_SNAPSHOT_BYTES,
        )
        .unwrap();
        assert!(
            restored.pending_event_intents()[&event_id]
                .next_attempt_at_unix_ms
                .unwrap()
                > A2A_EVENT_RETRY_MAX_MS
        );

        let mut repaired = restored;
        let previous_revision = repaired.revision();
        assert_eq!(repaired.clamp_restored_event_retry_deadlines(0).unwrap(), 1);
        assert_eq!(repaired.revision(), previous_revision + 1);
        assert_eq!(
            repaired.pending_event_intents()[&event_id].next_attempt_at_unix_ms,
            Some(A2A_EVENT_RETRY_MAX_MS)
        );
        assert_eq!(repaired.clamp_restored_event_retry_deadlines(0).unwrap(), 0);
    }

    #[test]
    fn owner_fair_due_index_does_not_hide_a_small_owner_behind_ten_thousand_events() {
        let principal = principal();
        let mut mapper = A2aMapper::new();
        mapper
            .prepare_send_message(
                message("due-template"),
                correlation("due-template"),
                Some(&principal),
            )
            .into_authorized()
            .unwrap();
        let mut template = mapper.pending_events().pop().unwrap();
        template.state = A2aPendingEventState::ReconcilePending;
        template.transient_failures = 1;
        template.next_attempt_at_unix_ms = Some(0);
        template.last_error = Some(A2A_EVENT_RECONCILE_REASON.into());
        mapper.pending_events.clear();
        for sequence in 0..10_000_u32 {
            let mut event = template.clone();
            event.event_id = format!("owner-a-event-{sequence:05}");
            mapper.pending_events.insert(event.event_id.clone(), event);
        }
        let mut owner_b = template;
        owner_b.event_id = "owner-b-event".into();
        owner_b.owner_subject = "capacity-owner-b".into();
        mapper
            .pending_events
            .insert(owner_b.event_id.clone(), owner_b);
        mapper.pending_event_schedule = rebuild_pending_event_schedule(&mapper.pending_events);
        mapper.pending_event_schedule_by_owner =
            rebuild_pending_event_schedule_by_owner(&mapper.pending_events);

        let batch = mapper.pending_events_due_fair_batch(0, 2, 1, &mut None);
        assert_eq!(batch.len(), 2);
        assert!(batch
            .iter()
            .any(|event| event.owner_subject == "capacity-owner"));
        assert!(batch
            .iter()
            .any(|event| event.owner_subject == "capacity-owner-b"));
    }

    #[test]
    fn deterministic_poison_quarantine_is_sanitized_durable_and_strictly_bound() {
        let principal = principal();
        let mut mapper = A2aMapper::new();
        mapper
            .prepare_send_message(
                message("deterministic-poison"),
                correlation("deterministic-poison"),
                Some(&principal),
            )
            .into_authorized()
            .unwrap();
        let event = mapper.pending_events().pop().unwrap();
        mapper
            .mark_event_quarantined(
                &event.event_id,
                A2aEventQuarantineReason::DeterministicPoison,
            )
            .unwrap();
        let revision = mapper.revision();
        mapper
            .mark_event_quarantined(
                &event.event_id,
                A2aEventQuarantineReason::DeterministicPoison,
            )
            .unwrap();
        assert_eq!(mapper.revision(), revision);

        let snapshot = serde_json::to_value(&mapper).unwrap();
        let restored: A2aMapper = serde_json::from_value(snapshot.clone()).unwrap();
        assert_eq!(restored.schema_version(), A2A_MAPPER_SCHEMA_VERSION);
        assert!(restored.pending_events().is_empty());
        assert_eq!(restored.quarantined_events().len(), 1);
        assert_eq!(
            restored.quarantined_events()[0].quarantine_reason,
            Some(A2aEventQuarantineReason::DeterministicPoison)
        );
        assert_eq!(
            restored.quarantined_events()[0].last_error.as_deref(),
            Some(A2A_EVENT_DETERMINISTIC_POISON_REASON)
        );

        let mut wrong_task_binding = snapshot;
        wrong_task_binding["pending_events"]
            .as_object_mut()
            .unwrap()
            .get_mut(&event.event_id)
            .unwrap()["task_id"] = Value::String("other-task".into());
        assert!(serde_json::from_value::<A2aMapper>(wrong_task_binding).is_err());
    }

    #[test]
    fn v2_exhausted_event_is_migrated_to_due_retry_on_restore() {
        let principal = principal();
        let mut mapper = A2aMapper::new();
        mapper
            .prepare_send_message(
                message("legacy-exhausted-event"),
                correlation("legacy-exhausted-event"),
                Some(&principal),
            )
            .into_authorized()
            .unwrap();
        let event_id = mapper.pending_events().pop().unwrap().event_id;
        let mut legacy = serde_json::to_value(&mapper).unwrap();
        legacy["schema_version"] = Value::from(A2A_PREVIOUS_MAPPER_SCHEMA_VERSION);
        let event = legacy["pending_events"]
            .as_object_mut()
            .unwrap()
            .get_mut(&event_id)
            .unwrap();
        event["attempts"] = Value::from(A2A_MAX_EVENT_ATTEMPTS);
        event["state"] = serde_json::json!(A2aPendingEventState::ReconcilePending);
        event["last_error"] = Value::String(A2A_EVENT_RECONCILE_REASON.into());
        event.as_object_mut().unwrap().remove("transient_failures");
        event
            .as_object_mut()
            .unwrap()
            .remove("next_attempt_at_unix_ms");
        event.as_object_mut().unwrap().remove("quarantine_reason");

        let restored: A2aMapper = serde_json::from_value(legacy).unwrap();
        assert_eq!(restored.schema_version(), A2A_MAPPER_SCHEMA_VERSION);
        let pending = restored.pending_events_due_fair_batch(0, 1, 1, &mut None);
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].state, A2aPendingEventState::ReconcilePending);
        assert_eq!(pending[0].attempts, 0);
        assert_eq!(pending[0].transient_failures, A2A_MAX_EVENT_ATTEMPTS);
        assert_eq!(pending[0].next_attempt_at_unix_ms, Some(0));
        assert!(restored.quarantined_events().is_empty());
        let roundtrip: A2aMapper =
            serde_json::from_value(serde_json::to_value(&restored).unwrap()).unwrap();
        assert_eq!(roundtrip, restored);
    }

    #[test]
    fn v2_attempts_exhausted_quarantine_is_reopened_as_due_transient_retry() {
        let principal = principal();
        let mut mapper = A2aMapper::new();
        mapper
            .prepare_send_message(
                message("v2-quarantined-event"),
                correlation("v2-quarantined-event"),
                Some(&principal),
            )
            .into_authorized()
            .unwrap();
        let event_id = mapper.pending_events().pop().unwrap().event_id;
        let mut legacy = serde_json::to_value(&mapper).unwrap();
        legacy["schema_version"] = Value::from(A2A_PREVIOUS_MAPPER_SCHEMA_VERSION);
        let event = legacy["pending_events"]
            .as_object_mut()
            .unwrap()
            .get_mut(&event_id)
            .unwrap();
        event["attempts"] = Value::from(A2A_MAX_EVENT_ATTEMPTS);
        event["state"] = serde_json::json!(A2aPendingEventState::Quarantined);
        event["last_error"] = Value::String(A2A_EVENT_ATTEMPTS_EXHAUSTED_REASON.into());
        event["quarantine_reason"] = serde_json::json!(A2aEventQuarantineReason::AttemptsExhausted);
        event.as_object_mut().unwrap().remove("transient_failures");
        event
            .as_object_mut()
            .unwrap()
            .remove("next_attempt_at_unix_ms");

        let restored: A2aMapper = serde_json::from_value(legacy).unwrap();
        let pending = restored.pending_events_due_fair_batch(0, 1, 1, &mut None);
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].state, A2aPendingEventState::ReconcilePending);
        assert_eq!(pending[0].attempts, 0);
        assert_eq!(pending[0].transient_failures, A2A_MAX_EVENT_ATTEMPTS);
        assert_eq!(pending[0].next_attempt_at_unix_ms, Some(0));
        assert_eq!(pending[0].quarantine_reason, None);
        assert!(restored.quarantined_events().is_empty());
        let roundtrip: A2aMapper =
            serde_json::from_value(serde_json::to_value(&restored).unwrap()).unwrap();
        assert_eq!(roundtrip, restored);
    }

    #[test]
    fn legacy_snapshot_rejects_two_unsettled_message_events_for_one_task() {
        let principal = principal();
        let mut mapper = A2aMapper::new();
        let first = message("legacy-ordered-first");
        let (_, action) = mapper
            .prepare_send_message(first.clone(), correlation("legacy-first"), Some(&principal))
            .into_authorized()
            .unwrap();
        let A2aAction::DispatchMessage { mapping, .. } = action else {
            panic!("expected first dispatch action");
        };
        let first_event_id = mapper
            .pending_events()
            .into_iter()
            .find(|event| event.message_id.as_deref() == Some(first.message_id.as_str()))
            .unwrap()
            .event_id;
        mapper.mark_event_settled(&first_event_id).unwrap();
        let first_dispatch_id = mapper
            .dispatch_for_message(&first.message_id, &principal)
            .unwrap()
            .dispatch_id
            .clone();
        mapper.mark_dispatch_settled(&first_dispatch_id).unwrap();
        mapper
            .prepare_send_message(
                targeted_message("legacy-ordered-second", &mapping),
                correlation("legacy-second"),
                Some(&principal),
            )
            .into_authorized()
            .unwrap();

        let mut legacy = serde_json::to_value(&mapper).unwrap();
        legacy["schema_version"] = Value::from(A2A_PREVIOUS_MAPPER_SCHEMA_VERSION);
        let first_event = legacy["pending_events"]
            .as_object_mut()
            .unwrap()
            .get_mut(&first_event_id)
            .unwrap();
        first_event["state"] = serde_json::json!(A2aPendingEventState::ReconcilePending);
        first_event["attempts"] = Value::from(0);
        first_event["transient_failures"] = Value::from(1);
        first_event["next_attempt_at_unix_ms"] = Value::from(0);
        first_event["last_error"] = Value::String(A2A_EVENT_RECONCILE_REASON.into());
        first_event["quarantine_reason"] = Value::Null;

        let error = serde_json::from_value::<A2aMapper>(legacy).unwrap_err();
        assert!(error
            .to_string()
            .contains("more than one unsettled message acceptance event"));
    }

    #[test]
    fn terminal_event_intent_survives_restore_and_settles_idempotently() {
        let principal = principal();
        let mut mapper = A2aMapper::new();
        let (_, action) = mapper
            .prepare_send_message(
                message("terminal-event"),
                correlation("terminal"),
                Some(&principal),
            )
            .into_authorized()
            .unwrap();
        let A2aAction::DispatchMessage { mapping, .. } = action else {
            panic!("expected dispatch action");
        };
        for event in mapper.pending_events() {
            mapper.mark_event_settled(&event.event_id).unwrap();
        }
        mapper
            .transition_task(&mapping.task_id, A2aTaskState::Completed, None)
            .unwrap();
        let restored: A2aMapper =
            serde_json::from_value(serde_json::to_value(&mapper).unwrap()).unwrap();
        let pending = restored.pending_events();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].kind, A2aPendingEventKind::StatusChanged);
        assert_eq!(pending[0].task.state, A2aTaskState::Completed);

        let mut repaired = restored;
        let event_id = pending[0].event_id.clone();
        repaired
            .mark_event_reconcile_pending(&event_id, "token=must-not-persist", 1)
            .unwrap();
        assert!(!serde_json::to_string(&repaired)
            .unwrap()
            .contains("must-not-persist"));
        repaired.mark_event_settled(&event_id).unwrap();
        let revision = repaired.revision();
        repaired.mark_event_settled(&event_id).unwrap();
        assert_eq!(repaired.revision(), revision);
        assert!(repaired.pending_events().is_empty());
    }

    #[test]
    fn cross_owner_outbox_and_event_snapshots_are_rejected() {
        let principal = principal();
        let mut mapper = A2aMapper::new();
        mapper
            .prepare_send_message(
                message("cross-owner"),
                correlation("cross"),
                Some(&principal),
            )
            .into_authorized()
            .unwrap();

        let mut dispatch_snapshot = serde_json::to_value(&mapper).unwrap();
        dispatch_snapshot["dispatch_outbox"]
            .as_object_mut()
            .unwrap()
            .values_mut()
            .next()
            .unwrap()["owner_subject"] = Value::String("other-owner".into());
        assert!(serde_json::from_value::<A2aMapper>(dispatch_snapshot).is_err());

        let mut event_snapshot = serde_json::to_value(&mapper).unwrap();
        event_snapshot["pending_events"]
            .as_object_mut()
            .unwrap()
            .values_mut()
            .next()
            .unwrap()["owner_subject"] = Value::String("other-owner".into());
        assert!(serde_json::from_value::<A2aMapper>(event_snapshot).is_err());
    }

    #[test]
    fn legacy_v1_snapshot_rebuilds_safe_reconciliation_records() {
        let principal = principal();
        let mut mapper = A2aMapper::new();
        let (_, action) = mapper
            .prepare_send_message(message("legacy"), correlation("legacy"), Some(&principal))
            .into_authorized()
            .unwrap();
        let A2aAction::DispatchMessage { mapping, .. } = action else {
            panic!("expected dispatch action");
        };
        let mut legacy = serde_json::to_value(&mapper).unwrap();
        legacy.as_object_mut().unwrap().remove("dispatch_outbox");
        legacy.as_object_mut().unwrap().remove("pending_events");

        let restored: A2aMapper = serde_json::from_value(legacy).unwrap();
        let dispatch = restored.dispatch_for_message("legacy", &principal).unwrap();
        assert_eq!(dispatch.run_id, mapping.run_id);
        assert_eq!(dispatch.state, A2aDispatchOutboxState::ReconcilePending);
        assert_eq!(restored.pending_dispatches().len(), 1);
        assert_eq!(restored.pending_events().len(), 1);
        assert_eq!(
            restored.pending_events()[0].kind,
            A2aPendingEventKind::RecoveredSnapshot
        );
        assert_eq!(
            restored.pending_events()[0].state,
            A2aPendingEventState::ReconcilePending
        );
    }
}
