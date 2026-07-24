//! Bounded A2A 1.0 JSON-RPC/HTTP transport and resumable SSE delivery.
//!
//! The transport is deliberately an ingress adapter: task authorization and lifecycle decisions
//! stay in [`A2aMapper`]. Mutating requests use copy-on-write persistence so an unpersisted mapper
//! snapshot is never installed as live state.

use super::{
    a2a_wire_metadata_without_part_extensions, deserialize_a2a_mapper_snapshot_bounded,
    serialize_a2a_mapper_snapshot_bounded, set_a2a_part_wire_extension, A2aAction, A2aArtifact,
    A2aCancellationOutboxRecord, A2aCancellationOutboxState, A2aContentPart,
    A2aDispatchOutboxRecord, A2aDispatchOutboxState, A2aDispatchResponse, A2aEventQuarantineReason,
    A2aListTasksRequest, A2aMapper, A2aMessage, A2aPart, A2aPartWireExtension,
    A2aPendingEventIntent, A2aPendingEventKind, A2aPendingEventState, A2aRole,
    A2aSendResponsePolicy, A2aTaskPage, A2aTaskRecord, A2aTaskState, CorrelationIdentity,
    GovernanceAuthorization, GovernanceDenialCode, GovernanceEnvelope, GovernedAction,
    ProtocolError, ProtocolErrorCode, ProtocolPrincipal, ProtocolResult,
    A2A_DEFAULT_MAPPER_SNAPSHOT_BYTES, A2A_EVENT_RETRY_MAX_MS, A2A_MAX_CANCELLATION_ATTEMPTS,
    A2A_MAX_DISPATCH_ATTEMPTS, A2A_PART_WIRE_EXTENSIONS_METADATA_KEY, A2A_PROTOCOL_VERSION,
};
use crate::CancellationToken;
use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD as STANDARD_BASE64, Engine as _};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::future::{pending, Future};
use std::io::Write as _;
use std::net::{IpAddr, SocketAddr};
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool as StdAtomicBool, Ordering as AtomicOrdering};
use std::sync::Once;
use std::sync::{Arc, Mutex as StdMutex};
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{
    broadcast, mpsc, oneshot, Mutex, Notify, OwnedRwLockReadGuard, OwnedSemaphorePermit, RwLock,
    Semaphore,
};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{sleep_until, timeout, Instant};

pub const A2A_AGENT_CARD_PATH: &str = "/.well-known/agent-card.json";
const A2A_TASK_CANCEL_SCOPE: &str = "a2a:tasks:cancel";
pub const DEFAULT_A2A_HTTP_HEADER_BYTES: usize = 32 * 1024;
pub const DEFAULT_A2A_HTTP_BODY_BYTES: usize = 1024 * 1024;
pub const DEFAULT_A2A_HTTP_CONCURRENCY: usize = 64;
pub const DEFAULT_A2A_HTTP_PREAUTH_PER_IP: usize = 8;
pub const DEFAULT_A2A_HTTP_STREAMS: usize = 64;
pub const DEFAULT_A2A_HTTP_STREAMS_PER_OWNER: usize = 8;
pub const DEFAULT_A2A_CONTROL_CONCURRENCY: usize = 8;
pub const DEFAULT_A2A_CONTROL_CONCURRENCY_PER_OWNER: usize = 2;
pub const DEFAULT_A2A_CONTROL_QUEUE: usize = 64;
pub const DEFAULT_A2A_CONTROL_QUEUE_PER_OWNER: usize = 8;
pub const DEFAULT_A2A_CONTROL_PROBE_BODY_BYTES: usize = 64 * 1024;
pub const DEFAULT_A2A_CONTROL_PROBE_TIMEOUT: Duration = Duration::from_millis(250);
pub const DEFAULT_A2A_CONTROL_PROBES_PER_IP: usize = 2;
pub const DEFAULT_A2A_CONTROL_PROBES_PER_OWNER: usize = 1;
pub const DEFAULT_A2A_CONTROL_PROBES_PER_MINUTE: u32 = 120;
pub const DEFAULT_A2A_CONTROL_REQUESTS_PER_IP: usize = 4;
pub const DEFAULT_A2A_CONTROL_REQUESTS_PER_OWNER: usize = 2;
const A2A_LAST_CHANCE_HANDSHAKES: usize = 1;
pub const DEFAULT_A2A_STARTUP_RECOVERY_BUDGET: Duration = Duration::from_millis(500);
pub const DEFAULT_A2A_RECOVERY_CONCURRENCY: usize = 8;
pub const DEFAULT_A2A_RECOVERY_CONCURRENCY_PER_OWNER: usize = 1;
pub const DEFAULT_A2A_HTTP_RATE_PER_MINUTE: u32 = 600;
pub const DEFAULT_A2A_HTTP_RATE_BUCKETS: usize = 4096;
pub const DEFAULT_A2A_RETAINED_EVENTS: usize = 1024;
pub const DEFAULT_A2A_EVENT_BYTES: usize = 1024 * 1024;
pub const DEFAULT_A2A_REPLAY_BYTES: usize = 8 * 1024 * 1024;
pub const DEFAULT_A2A_STREAM_EVENTS: usize = 4096;
pub const DEFAULT_A2A_STREAM_BYTES: usize = 32 * 1024 * 1024;
pub const DEFAULT_A2A_EVENT_BUCKETS: usize = 4096;
pub const DEFAULT_A2A_EVENT_BUCKETS_PER_OWNER: usize = 1024;
const A2A_STREAM_HEAD_PROBE_INTERVAL: Duration = Duration::from_secs(5);

thread_local! {
    static A2A_UNTRUSTED_CALLBACK_DEPTH: Cell<u32> = const { Cell::new(0) };
}

static INSTALL_A2A_PANIC_HOOK: Once = Once::new();

/// Install one process-wide hook that redacts panic payloads only while an untrusted A2A host
/// callback is being polled. All other panics retain the application's previous hook. Per-call
/// hook swapping would be global and racy, so callback scope is carried in thread-local state.
fn install_a2a_sanitizing_panic_hook() {
    INSTALL_A2A_PANIC_HOOK.call_once(|| {
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |panic_info| {
            let untrusted = A2A_UNTRUSTED_CALLBACK_DEPTH
                .try_with(|depth| depth.get() > 0)
                .unwrap_or(false);
            if untrusted {
                // Panic hooks must not panic recursively when stderr is unavailable.
                let _ = writeln!(
                    std::io::stderr().lock(),
                    "AIKit suppressed an untrusted A2A callback panic"
                );
            } else {
                previous(panic_info);
            }
        }));
    });
}

struct A2aUntrustedCallbackGuard;

impl A2aUntrustedCallbackGuard {
    fn enter() -> Self {
        A2A_UNTRUSTED_CALLBACK_DEPTH.with(|depth| depth.set(depth.get().saturating_add(1)));
        Self
    }
}

impl Drop for A2aUntrustedCallbackGuard {
    fn drop(&mut self) {
        A2A_UNTRUSTED_CALLBACK_DEPTH.with(|depth| depth.set(depth.get().saturating_sub(1)));
    }
}

struct A2aUntrustedCallback<F> {
    inner: Option<Pin<Box<F>>>,
}

impl<F> A2aUntrustedCallback<F> {
    fn new(inner: F) -> Self {
        Self {
            inner: Some(Box::pin(inner)),
        }
    }

    fn finish_drop(&mut self) -> Result<(), ()> {
        let Some(inner) = self.inner.take() else {
            return Ok(());
        };
        std::panic::catch_unwind(AssertUnwindSafe(|| {
            let _guard = A2aUntrustedCallbackGuard::enter();
            drop(inner);
        }))
        .map_err(|_| ())
    }
}

impl<F: Future> Future for A2aUntrustedCallback<F> {
    type Output = Result<F::Output, ()>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let _guard = A2aUntrustedCallbackGuard::enter();
        let poll = std::panic::catch_unwind(AssertUnwindSafe(|| {
            self.get_mut()
                .inner
                .as_mut()
                .expect("A2A callback polled after its guarded drop")
                .as_mut()
                .poll(context)
        }));
        match poll {
            Ok(Poll::Ready(output)) => Poll::Ready(Ok(output)),
            Ok(Poll::Pending) => Poll::Pending,
            Err(_) => Poll::Ready(Err(())),
        }
    }
}

impl<F> Drop for A2aUntrustedCallback<F> {
    fn drop(&mut self) {
        // This also covers outer task aborts and timeout combinators that drop the callback
        // without returning through the normal explicit cleanup path.
        let _ = self.finish_drop();
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct A2aAgentInterface {
    pub url: String,
    pub protocol_binding: String,
    pub protocol_version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct A2aAgentCapabilities {
    pub streaming: bool,
    pub push_notifications: bool,
    pub extended_agent_card: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct A2aAgentSkill {
    pub id: String,
    pub name: String,
    pub description: String,
    pub tags: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub examples: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub input_modes: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub output_modes: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub security_requirements: Vec<Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct A2aAgentCard {
    pub name: String,
    pub description: String,
    pub version: String,
    pub capabilities: A2aAgentCapabilities,
    pub skills: Vec<A2aAgentSkill>,
    pub supported_interfaces: Vec<A2aAgentInterface>,
    pub default_input_modes: Vec<String>,
    pub default_output_modes: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub security_schemes: BTreeMap<String, Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub security_requirements: Vec<Value>,
}

impl A2aAgentCard {
    fn validate(&self) -> ProtocolResult<()> {
        if self.name.trim().is_empty()
            || self.description.trim().is_empty()
            || self.version.trim().is_empty()
            || self.supported_interfaces.is_empty()
            || self.default_input_modes.is_empty()
            || self.default_output_modes.is_empty()
            || self.skills.is_empty()
        {
            return Err(ProtocolError::invalid(
                "A2A Agent Card is missing a required field",
            ));
        }
        if !self.capabilities.streaming
            || self.capabilities.push_notifications
            || self.capabilities.extended_agent_card
        {
            return Err(ProtocolError::invalid(
                "A2A Agent Card must advertise streaming=true, pushNotifications=false, and extendedAgentCard=false",
            ));
        }
        if self.supported_interfaces.iter().any(|interface| {
            interface.url.trim().is_empty()
                || interface.url.contains(['\r', '\n'])
                || interface.protocol_binding != "JSONRPC"
                || interface.protocol_version != A2A_PROTOCOL_VERSION
        }) {
            return Err(ProtocolError::invalid(
                "A2A Agent Card interfaces must be JSONRPC 1.0 URLs",
            ));
        }
        if self
            .default_input_modes
            .iter()
            .chain(self.default_output_modes.iter())
            .any(|mode| mode.trim().is_empty() || mode.contains(['\r', '\n']))
        {
            return Err(ProtocolError::invalid("invalid A2A Agent Card media mode"));
        }
        if self.skills.iter().any(|skill| {
            invalid_card_text(&skill.id)
                || invalid_card_text(&skill.name)
                || invalid_card_text(&skill.description)
                || skill.tags.is_empty()
                || skill.tags.iter().any(|tag| invalid_card_text(tag))
                || skill
                    .examples
                    .iter()
                    .any(|example| invalid_card_text(example))
                || skill
                    .input_modes
                    .iter()
                    .chain(skill.output_modes.iter())
                    .any(|mode| invalid_card_text(mode))
        }) {
            return Err(ProtocolError::invalid(
                "A2A Agent Card contains an invalid skill",
            ));
        }
        Ok(())
    }
}

fn invalid_card_text(value: &str) -> bool {
    value.trim().is_empty() || value.len() > 4096 || value.chars().any(char::is_control)
}

#[derive(Debug, Clone)]
pub struct A2aHttpHeaders {
    values: BTreeMap<String, String>,
}

impl A2aHttpHeaders {
    pub fn get(&self, name: &str) -> Option<&str> {
        self.values
            .get(&name.to_ascii_lowercase())
            .map(String::as_str)
    }
}

#[derive(Debug, Clone)]
pub struct A2aHttpAuthError {
    pub status: u16,
    pub message: String,
    pub www_authenticate: Option<String>,
}

pub trait A2aHttpAuthenticator: Send + Sync {
    fn authenticate(&self, headers: &A2aHttpHeaders)
        -> Result<ProtocolPrincipal, A2aHttpAuthError>;
}

/// Independently authenticated control-plane listener supervised with the public A2A listener.
///
/// The listener must be reachable only through a protected transport boundary (for example mTLS,
/// a private service mesh address, or a local authenticated proxy). Its authenticator is separate
/// from public ingress credentials, and the transport confines every accepted request to
/// `CancelTask`. This makes public partial-header saturation unable to consume protected ingress
/// capacity while preserving the canonical cancellation governance and scheduler path.
pub struct A2aProtectedControlIngress {
    listener: TcpListener,
    authenticator: Arc<dyn A2aHttpAuthenticator>,
}

impl A2aProtectedControlIngress {
    pub fn new(listener: TcpListener, authenticator: Arc<dyn A2aHttpAuthenticator>) -> Self {
        Self {
            listener,
            authenticator,
        }
    }

    pub fn local_addr(&self) -> ProtocolResult<SocketAddr> {
        self.listener
            .local_addr()
            .map_err(|error| protocol_io("read protected A2A control address", error))
    }
}

pub struct StaticBearerA2aAuthenticator {
    token: Vec<u8>,
    principal: ProtocolPrincipal,
    challenge: String,
}

impl StaticBearerA2aAuthenticator {
    pub fn new(
        token: impl AsRef<[u8]>,
        principal: ProtocolPrincipal,
        challenge: impl Into<String>,
    ) -> ProtocolResult<Self> {
        let token = token.as_ref();
        let challenge = challenge.into();
        if token.is_empty() || token.len() > 4096 {
            return Err(ProtocolError::invalid("invalid A2A bearer token"));
        }
        if challenge.contains(['\r', '\n']) {
            return Err(ProtocolError::invalid("invalid WWW-Authenticate challenge"));
        }
        Ok(Self {
            token: token.to_vec(),
            principal,
            challenge,
        })
    }
}

impl A2aHttpAuthenticator for StaticBearerA2aAuthenticator {
    fn authenticate(
        &self,
        headers: &A2aHttpHeaders,
    ) -> Result<ProtocolPrincipal, A2aHttpAuthError> {
        let supplied = headers
            .get("authorization")
            .and_then(|value| value.strip_prefix("Bearer "))
            .map(str::as_bytes);
        if supplied.is_none_or(|value| !constant_time_eq(value, &self.token)) {
            return Err(A2aHttpAuthError {
                status: 401,
                message: "authentication required".into(),
                www_authenticate: Some(self.challenge.clone()),
            });
        }
        Ok(self.principal.clone())
    }
}

#[async_trait]
pub trait A2aMapperSnapshotStore: Send + Sync {
    /// Atomically persist `candidate` only when the stored `(revision, digest)` equals `expected`.
    /// `AlreadyApplied` is valid only for the candidate's exact same revision and digest.
    async fn compare_and_swap_snapshot(
        &self,
        expected: Option<A2aSnapshotVersion>,
        candidate: A2aSerializedMapperSnapshot,
    ) -> Result<A2aSnapshotCommitOutcome, A2aSnapshotStoreError>;

    /// Definitive metadata probe used after an ambiguous transport/storage error.
    ///
    /// This read must be linearizable with `compare_and_swap_snapshot`: after that call returns,
    /// the probe must report the complete current durable head at a point later than the CAS. It
    /// must neither synthesize a version nor return a stale replica/cache view. The commit boundary
    /// relies on observing the exact expected head to classify an unknown outcome as not applied.
    async fn lookup_snapshot_version(&self) -> ProtocolResult<Option<A2aSnapshotVersion>>;

    /// Load the exact canonical/raw bytes and version of the current durable head.
    ///
    /// This read must be linearizable with `compare_and_swap_snapshot` and return the same head
    /// that `lookup_snapshot_version` would report. The server uses the bytes once when attaching
    /// to an existing store so legacy v1/v2 snapshots can be proven equivalent and atomically
    /// advanced to the current schema by the first mutation without a same-revision digest clash.
    async fn load_serialized_snapshot(&self)
        -> ProtocolResult<Option<A2aSerializedMapperSnapshot>>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum A2aSnapshotCommitOutcome {
    Applied,
    AlreadyApplied,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum A2aSnapshotStoreError {
    /// The store guarantees the candidate was not durably applied.
    DefiniteNotApplied(ProtocolError),
    /// The call may have applied the candidate; callers must probe before retrying or serving.
    OutcomeUnknown(ProtocolError),
}

impl A2aSnapshotStoreError {
    pub fn definite(error: ProtocolError) -> Self {
        Self::DefiniteNotApplied(error)
    }

    pub fn unknown(error: ProtocolError) -> Self {
        Self::OutcomeUnknown(error)
    }

    fn into_protocol_error(self) -> ProtocolError {
        match self {
            Self::DefiniteNotApplied(error) | Self::OutcomeUnknown(error) => error,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct A2aSnapshotVersion {
    pub revision: u64,
    pub digest: String,
}

/// Canonical, size-bounded mapper bytes supplied to a snapshot-store CAS operation.
#[derive(Debug, Clone)]
pub struct A2aSerializedMapperSnapshot {
    revision: u64,
    digest: String,
    mutation_id: String,
    bytes: Arc<[u8]>,
}

impl A2aSerializedMapperSnapshot {
    pub fn from_mapper(mapper: &A2aMapper) -> ProtocolResult<Self> {
        let bytes =
            serialize_a2a_mapper_snapshot_bounded(mapper, A2A_DEFAULT_MAPPER_SNAPSHOT_BYTES)?;
        let digest = snapshot_digest(&bytes);
        Ok(Self {
            revision: mapper.revision(),
            mutation_id: format!("bootstrap-{digest}"),
            digest,
            bytes: Arc::from(bytes),
        })
    }

    /// Validate and wrap exact persisted bytes, including legacy mapper schemas.
    pub fn from_persisted_bytes(bytes: Vec<u8>) -> ProtocolResult<Self> {
        let mapper =
            deserialize_a2a_mapper_snapshot_bounded(&bytes, A2A_DEFAULT_MAPPER_SNAPSHOT_BYTES)?;
        let digest = snapshot_digest(&bytes);
        Ok(Self {
            revision: mapper.revision(),
            mutation_id: format!("restored-{digest}"),
            digest,
            bytes: Arc::from(bytes),
        })
    }

    fn bind_expected(mut self, expected: Option<&A2aSnapshotVersion>) -> Self {
        let expected_identity = expected.map_or_else(
            || "empty".to_owned(),
            |version| format!("{}:{}", version.revision, version.digest),
        );
        self.mutation_id = snapshot_digest(
            format!("{expected_identity}:{}:{}", self.revision, self.digest).as_bytes(),
        );
        self
    }

    pub fn revision(&self) -> u64 {
        self.revision
    }

    pub fn digest(&self) -> &str {
        &self.digest
    }

    pub fn mutation_id(&self) -> &str {
        &self.mutation_id
    }

    pub fn version(&self) -> A2aSnapshotVersion {
        A2aSnapshotVersion {
            revision: self.revision,
            digest: self.digest.clone(),
        }
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn decode(&self) -> ProtocolResult<A2aMapper> {
        deserialize_a2a_mapper_snapshot_bounded(&self.bytes, A2A_DEFAULT_MAPPER_SNAPSHOT_BYTES)
    }
}

/// Explicitly ephemeral mapper persistence useful for tests and single-process hosts.
#[derive(Debug, Default)]
pub struct InMemoryA2aMapperSnapshotStore {
    snapshot: Mutex<Option<A2aSerializedMapperSnapshot>>,
}

impl InMemoryA2aMapperSnapshotStore {
    pub async fn load_snapshot(&self) -> Option<A2aMapper> {
        let snapshot = self.snapshot.lock().await.clone()?;
        snapshot.decode().ok()
    }

    /// Initialize an empty ephemeral store or replay the exact same snapshot. This compatibility
    /// helper deliberately cannot overwrite an existing revision.
    pub async fn persist_snapshot(&self, candidate: &A2aMapper) -> ProtocolResult<()> {
        let candidate = A2aSerializedMapperSnapshot::from_mapper(candidate)?;
        self.compare_and_swap_snapshot(None, candidate)
            .await
            .map_err(A2aSnapshotStoreError::into_protocol_error)?;
        Ok(())
    }
}

#[async_trait]
impl A2aMapperSnapshotStore for InMemoryA2aMapperSnapshotStore {
    async fn compare_and_swap_snapshot(
        &self,
        expected: Option<A2aSnapshotVersion>,
        candidate: A2aSerializedMapperSnapshot,
    ) -> Result<A2aSnapshotCommitOutcome, A2aSnapshotStoreError> {
        let mut stored = self.snapshot.lock().await;
        if let Some(current) = stored.as_ref() {
            if current.revision == candidate.revision {
                return if current.digest == candidate.digest {
                    Ok(A2aSnapshotCommitOutcome::AlreadyApplied)
                } else {
                    Err(A2aSnapshotStoreError::definite(ProtocolError::conflict(
                        "A2A snapshot revision already exists with a different digest",
                    )))
                };
            }
            if expected.as_ref() != Some(&current.version())
                || candidate.revision <= current.revision
            {
                return Err(A2aSnapshotStoreError::definite(ProtocolError::conflict(
                    "A2A snapshot compare-and-swap revision conflict",
                )));
            }
        } else if expected.is_some() {
            return Err(A2aSnapshotStoreError::definite(ProtocolError::conflict(
                "A2A snapshot compare-and-swap expected a missing revision",
            )));
        }
        *stored = Some(candidate);
        Ok(A2aSnapshotCommitOutcome::Applied)
    }

    async fn lookup_snapshot_version(&self) -> ProtocolResult<Option<A2aSnapshotVersion>> {
        Ok(self
            .snapshot
            .lock()
            .await
            .as_ref()
            .map(|value| value.version()))
    }

    async fn load_serialized_snapshot(
        &self,
    ) -> ProtocolResult<Option<A2aSerializedMapperSnapshot>> {
        Ok(self.snapshot.lock().await.clone())
    }
}

fn snapshot_digest(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(encoded, "{byte:02x}");
    }
    encoded
}

async fn persist_snapshot_with_exact_probe(
    store: &Arc<dyn A2aMapperSnapshotStore>,
    expected: Option<A2aSnapshotVersion>,
    candidate: A2aSerializedMapperSnapshot,
) -> Result<(), A2aSnapshotStoreError> {
    let candidate_version = candidate.version();
    match store
        .compare_and_swap_snapshot(expected.clone(), candidate.clone())
        .await
    {
        Ok(A2aSnapshotCommitOutcome::Applied | A2aSnapshotCommitOutcome::AlreadyApplied) => Ok(()),
        Err(A2aSnapshotStoreError::DefiniteNotApplied(error)) => {
            classify_definite_snapshot_failure(store, &expected, error).await
        }
        Err(A2aSnapshotStoreError::OutcomeUnknown(first_error)) => {
            let observed = store
                .lookup_snapshot_version()
                .await
                .map_err(A2aSnapshotStoreError::unknown)?;
            if observed.as_ref() == Some(&candidate_version) {
                return Ok(());
            }
            if observed != expected {
                return Err(A2aSnapshotStoreError::unknown(ProtocolError::conflict(
                    "A2A snapshot outcome is ambiguous and the durable version diverged",
                )));
            }
            match store
                .compare_and_swap_snapshot(expected.clone(), candidate)
                .await
            {
                Ok(
                    A2aSnapshotCommitOutcome::Applied | A2aSnapshotCommitOutcome::AlreadyApplied,
                ) => Ok(()),
                Err(A2aSnapshotStoreError::DefiniteNotApplied(error)) => {
                    classify_definite_snapshot_failure(store, &expected, error).await
                }
                Err(A2aSnapshotStoreError::OutcomeUnknown(second_error)) => {
                    let observed = store
                        .lookup_snapshot_version()
                        .await
                        .map_err(A2aSnapshotStoreError::unknown)?;
                    if observed.as_ref() == Some(&candidate_version) {
                        Ok(())
                    } else if observed == expected {
                        Err(A2aSnapshotStoreError::DefiniteNotApplied(second_error))
                    } else {
                        Err(A2aSnapshotStoreError::OutcomeUnknown(first_error))
                    }
                }
            }
        }
    }
}

async fn classify_definite_snapshot_failure(
    store: &Arc<dyn A2aMapperSnapshotStore>,
    expected: &Option<A2aSnapshotVersion>,
    error: ProtocolError,
) -> Result<(), A2aSnapshotStoreError> {
    let observed = store
        .lookup_snapshot_version()
        .await
        .map_err(A2aSnapshotStoreError::unknown)?;
    if &observed == expected {
        Err(A2aSnapshotStoreError::DefiniteNotApplied(error))
    } else {
        Err(A2aSnapshotStoreError::unknown(ProtocolError::conflict(
            "A2A snapshot store reported no apply but the durable head diverged",
        )))
    }
}

fn validate_new_pending_event_wire_bytes(
    mapper: &A2aMapper,
    after_revision: u64,
    max_event_bytes: usize,
) -> ProtocolResult<()> {
    for intent in mapper
        .pending_event_intents()
        .values()
        .filter(|intent| intent.created_revision > after_revision)
    {
        let response = match intent.kind {
            A2aPendingEventKind::TaskCreated
            | A2aPendingEventKind::MessageAccepted
            | A2aPendingEventKind::RecoveredSnapshot => A2aStreamResponse::task(&intent.task),
            A2aPendingEventKind::StatusChanged | A2aPendingEventKind::CancellationRequested => {
                A2aStreamResponse::status(&intent.task)
            }
            A2aPendingEventKind::DirectMessageResponse => A2aStreamResponse::message(
                intent
                    .response_message
                    .as_ref()
                    .ok_or_else(|| ProtocolError::conflict("A2A direct event lost its message"))?,
            ),
        };
        let bytes = serde_json::to_vec(&response)
            .map_err(|error| ProtocolError::invalid(format!("serialize A2A event: {error}")))?
            .len();
        if bytes > max_event_bytes {
            return Err(ProtocolError::conflict(format!(
                "A2A event exceeds the configured {max_event_bytes} byte limit"
            )));
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct A2aEventOwner {
    pub subject: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<String>,
}

impl A2aEventOwner {
    fn from_task(task: &A2aTaskRecord) -> Self {
        Self {
            subject: task.owner_subject.clone(),
            tenant_id: task.owner_tenant_id.clone(),
        }
    }

    fn matches(&self, principal: &ProtocolPrincipal) -> bool {
        self.subject == principal.subject && self.tenant_id == principal.tenant_id
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct A2aPersistedEvent {
    pub logical_event_id: String,
    pub event_id: u64,
    pub owner: A2aEventOwner,
    pub task_id: String,
    pub response: A2aStreamResponse,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct A2aEventRetention {
    pub max_events: usize,
    pub max_event_bytes: usize,
    pub max_retained_bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct A2aReplayLimits {
    pub max_events: usize,
    pub max_bytes: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct A2aReplayPage {
    pub events: Vec<A2aPersistedEvent>,
    /// Inclusive task-local sequence captured by the first page. Follow-up pages must pass this
    /// value back so concurrent appends cannot move the replay boundary.
    pub high_water: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum A2aEventStoreError {
    RetentionGap,
    InvalidEventId,
    Store(ProtocolError),
}

/// Typed append failure boundary for an A2A event backend.
///
/// A backend outage or timeout is intentionally a unit variant: callers can persist a stable
/// retry category without copying provider diagnostics into a mapper snapshot or RPC response.
/// Deterministic input, identity, capacity, and binding failures are permanent and must not be
/// retried indefinitely.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum A2aEventAppendError {
    Retryable,
    Permanent(ProtocolError),
}

impl A2aEventAppendError {
    pub fn retryable() -> Self {
        Self::Retryable
    }

    pub fn permanent(error: ProtocolError) -> Self {
        Self::Permanent(error)
    }

    pub fn permanent_error(&self) -> Option<&ProtocolError> {
        match self {
            Self::Retryable => None,
            Self::Permanent(error) => Some(error),
        }
    }

    fn into_protocol_error(self) -> ProtocolError {
        match self {
            Self::Retryable => {
                ProtocolError::conflict("A2A event store is temporarily unavailable")
            }
            Self::Permanent(error) => error,
        }
    }
}

impl From<ProtocolError> for A2aEventAppendError {
    fn from(error: ProtocolError) -> Self {
        Self::Permanent(error)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum A2aEventAppendOutcome {
    Inserted(A2aPersistedEvent),
    Existing(A2aPersistedEvent),
}

impl A2aEventAppendOutcome {
    pub fn event(&self) -> &A2aPersistedEvent {
        match self {
            Self::Inserted(event) | Self::Existing(event) => event,
        }
    }

    pub fn into_event(self) -> A2aPersistedEvent {
        match self {
            Self::Inserted(event) | Self::Existing(event) => event,
        }
    }
}

#[async_trait]
pub trait A2aEventStore: Send + Sync {
    /// Atomically append a content-bound logical event.
    ///
    /// If the exact `(logical_event_id, owner, task_id, response)` was committed before a caller
    /// timed out or dropped, a retry must return [`A2aEventAppendOutcome::Existing`]. Reusing the
    /// logical id with different content is a permanent conflict. Backends must return
    /// [`A2aEventAppendError::Retryable`] only for sanitized availability/timeout/backpressure
    /// failures that can later succeed.
    async fn append(
        &self,
        logical_event_id: &str,
        owner: &A2aEventOwner,
        task_id: &str,
        response: &A2aStreamResponse,
        retention: A2aEventRetention,
    ) -> Result<A2aEventAppendOutcome, A2aEventAppendError>;

    async fn replay_page(
        &self,
        owner: &A2aEventOwner,
        task_id: &str,
        after_event_id: Option<u64>,
        through_high_water: Option<u64>,
        limits: A2aReplayLimits,
    ) -> Result<A2aReplayPage, A2aEventStoreError>;
}

/// Host seam for scheduling authorized A2A work after ingress state is durably accepted.
///
/// Implementations must reconcile by the action's stable task/run identities. A callback error
/// does not roll back the already accepted mapper snapshot or its stream event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum A2aExecutionMode {
    Blocking,
    Immediate,
    Streaming,
}

#[derive(Debug, Clone)]
pub struct A2aDispatchContext {
    pub mode: A2aExecutionMode,
    pub cancellation: CancellationToken,
    dispatch_fence: Option<A2aDispatchFence>,
}

#[derive(Debug, Clone)]
struct A2aDispatchFence {
    dispatch_id: String,
    expected_attempt: u32,
}

/// Explicit acknowledgement from the host-side execution fence.
///
/// `Settled` means the host has durably moved the task to a terminal or interrupted state.
/// `Stopped` means the host observed cancellation and fenced any external effect, allowing the
/// transport to durably mark the task cancelled. A dropped future or an error is deliberately not
/// an acknowledgement: the accepted task must remain nonterminal for later reconciliation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum A2aDispatchAck {
    Settled,
    Stopped,
}

/// Host proof required before a dispatch with an unknown prior outcome can be executed again.
/// The fail-closed default prevents effect-before-receipt crashes from duplicating side effects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum A2aUnknownDispatchDecision {
    ReconcileRequired,
    SafeToRetry,
    /// The host proved that the exact durable cancellation generation already stopped its
    /// external effect before the prior acknowledgement was lost. This decision is only valid for
    /// `reconcile_unknown_cancel`; message reconciliation treats it as fail-closed.
    AlreadyStopped,
}

#[async_trait]
/// Untrusted execution seam for effectful host work and unknown-outcome reconciliation.
///
/// Constructing an [`A2aHttpJsonRpcServer`] installs one process-wide panic hook. The hook
/// delegates unrelated panics to the previously installed application hook, but emits only a
/// fixed diagnostic while one of these callback futures is being polled so secret-bearing panic
/// payloads do not reach stderr. Applications must not replace that hook after server
/// construction. This containment requires unwinding; a binary built with `panic = "abort"`
/// cannot keep the listener alive after a callback panic and should isolate host work in another
/// process instead.
pub trait A2aDispatchHost: Send + Sync {
    async fn reconcile_unknown(
        &self,
        _server: Arc<A2aHttpJsonRpcServer>,
        _record: &A2aDispatchOutboxRecord,
    ) -> ProtocolResult<A2aUnknownDispatchDecision> {
        Ok(A2aUnknownDispatchDecision::ReconcileRequired)
    }

    /// Prove whether a restored cancellation with an unknown prior host outcome may run again.
    /// The fail-closed default prevents effect-before-receipt crashes from invoking cancellation
    /// callbacks twice unless the host can attest that the control action is safe to retry.
    async fn reconcile_unknown_cancel(
        &self,
        _server: Arc<A2aHttpJsonRpcServer>,
        _record: &A2aCancellationOutboxRecord,
    ) -> ProtocolResult<A2aUnknownDispatchDecision> {
        Ok(A2aUnknownDispatchDecision::ReconcileRequired)
    }

    async fn handle(
        &self,
        server: Arc<A2aHttpJsonRpcServer>,
        context: &A2aDispatchContext,
        envelope: &GovernanceEnvelope,
        action: &A2aAction,
    ) -> ProtocolResult<A2aDispatchAck>;
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct A2aEventKey {
    owner: A2aEventOwner,
    task_id: String,
}

#[derive(Debug, Default)]
struct InMemoryEventBucket {
    retained_bytes: usize,
    events: VecDeque<(A2aPersistedEvent, usize)>,
    last_touched: u64,
    terminal: bool,
}

#[derive(Debug, Clone)]
struct InMemoryLogicalEvent {
    key: A2aEventKey,
    event: A2aPersistedEvent,
}

#[derive(Debug, Default)]
struct InMemoryEventStoreState {
    buckets: BTreeMap<A2aEventKey, InMemoryEventBucket>,
    logical_events: BTreeMap<String, InMemoryLogicalEvent>,
    next_ids: BTreeMap<A2aEventKey, u64>,
    touch_sequence: u64,
}

/// Ephemeral event store. Production hosts should supply a durable implementation.
///
/// Replay buckets are bounded. Capacity reclamation removes only the least-recently-touched
/// terminal bucket; an active task's replay history is never silently displaced. Logical event
/// tombstones remain for the lifetime of the store so retry idempotency and monotonic task event
/// ids survive replay-bucket eviction. Upstream mapper hard limits bound those tombstones.
#[derive(Debug)]
pub struct InMemoryA2aEventStore {
    state: Mutex<InMemoryEventStoreState>,
    max_buckets: usize,
    max_buckets_per_owner: usize,
}

impl Default for InMemoryA2aEventStore {
    fn default() -> Self {
        Self {
            state: Mutex::new(InMemoryEventStoreState::default()),
            max_buckets: DEFAULT_A2A_EVENT_BUCKETS,
            max_buckets_per_owner: DEFAULT_A2A_EVENT_BUCKETS_PER_OWNER,
        }
    }
}

impl InMemoryA2aEventStore {
    pub fn new(max_buckets: usize, max_buckets_per_owner: usize) -> ProtocolResult<Self> {
        if max_buckets == 0 || max_buckets_per_owner == 0 || max_buckets_per_owner > max_buckets {
            return Err(ProtocolError::invalid(
                "invalid in-memory A2A event bucket limits",
            ));
        }
        Ok(Self {
            state: Mutex::new(InMemoryEventStoreState::default()),
            max_buckets,
            max_buckets_per_owner,
        })
    }
}

#[async_trait]
impl A2aEventStore for InMemoryA2aEventStore {
    async fn append(
        &self,
        logical_event_id: &str,
        owner: &A2aEventOwner,
        task_id: &str,
        response: &A2aStreamResponse,
        retention: A2aEventRetention,
    ) -> Result<A2aEventAppendOutcome, A2aEventAppendError> {
        validate_event_limits(retention)?;
        validate_logical_event_id(logical_event_id)?;
        let bytes = serde_json::to_vec(response)
            .map_err(|error| ProtocolError::invalid(format!("invalid A2A event: {error}")))?
            .len();
        if bytes > retention.max_event_bytes {
            return Err(A2aEventAppendError::permanent(ProtocolError::conflict(
                "A2A event exceeds the configured event byte limit",
            )));
        }
        let key = A2aEventKey {
            owner: owner.clone(),
            task_id: task_id.to_owned(),
        };
        let mut state = self.state.lock().await;
        if let Some(existing) = state.logical_events.get(logical_event_id) {
            if existing.key != key
                || existing.event.logical_event_id != logical_event_id
                || existing.event.owner != *owner
                || existing.event.task_id != task_id
                || existing.event.response != *response
            {
                return Err(A2aEventAppendError::permanent(ProtocolError::conflict(
                    "A2A logical event identity was reused with different content",
                )));
            }
            return Ok(A2aEventAppendOutcome::Existing(existing.event.clone()));
        }
        if !state.buckets.contains_key(&key) {
            // Full active buckets can become reclaimable when a task terminalizes, so surface
            // this bounded backpressure as retryable rather than dead-lettering the event.
            ensure_event_bucket_capacity(
                &mut state,
                owner,
                self.max_buckets,
                self.max_buckets_per_owner,
            )
            .map_err(|_| A2aEventAppendError::retryable())?;
        } else if state
            .buckets
            .get(&key)
            .is_some_and(|bucket| bucket.terminal)
        {
            return Err(A2aEventAppendError::permanent(ProtocolError::conflict(
                "terminal A2A event bucket cannot accept a new logical event",
            )));
        }
        let next_id = state.next_ids.get(&key).copied().unwrap_or(1);
        if next_id == 0 || next_id > 9_007_199_254_740_991 {
            return Err(A2aEventAppendError::permanent(ProtocolError::conflict(
                "A2A event id reached the cross-language integer limit",
            )));
        }
        let event = A2aPersistedEvent {
            logical_event_id: logical_event_id.to_owned(),
            event_id: next_id,
            owner: owner.clone(),
            task_id: task_id.to_owned(),
            response: response.clone(),
        };
        state.next_ids.insert(
            key.clone(),
            next_id
                .checked_add(1)
                .ok_or_else(|| ProtocolError::conflict("A2A event id overflow"))?,
        );
        state.touch_sequence = state
            .touch_sequence
            .checked_add(1)
            .ok_or_else(|| ProtocolError::conflict("A2A event touch sequence overflow"))?;
        let touch_sequence = state.touch_sequence;
        let bucket = state.buckets.entry(key.clone()).or_default();
        bucket.retained_bytes = bucket.retained_bytes.saturating_add(bytes);
        bucket.events.push_back((event.clone(), bytes));
        bucket.last_touched = touch_sequence;
        bucket.terminal = response.is_terminal();
        while bucket.events.len() > retention.max_events
            || bucket.retained_bytes > retention.max_retained_bytes
        {
            if let Some((_, removed)) = bucket.events.pop_front() {
                bucket.retained_bytes = bucket.retained_bytes.saturating_sub(removed);
            } else {
                break;
            }
        }
        state.logical_events.insert(
            logical_event_id.to_owned(),
            InMemoryLogicalEvent {
                key,
                event: event.clone(),
            },
        );
        Ok(A2aEventAppendOutcome::Inserted(event))
    }

    async fn replay_page(
        &self,
        owner: &A2aEventOwner,
        task_id: &str,
        after_event_id: Option<u64>,
        through_high_water: Option<u64>,
        limits: A2aReplayLimits,
    ) -> Result<A2aReplayPage, A2aEventStoreError> {
        if limits.max_events == 0 || limits.max_bytes == 0 {
            return Err(A2aEventStoreError::Store(ProtocolError::invalid(
                "invalid A2A replay limits",
            )));
        }
        let key = A2aEventKey {
            owner: owner.clone(),
            task_id: task_id.to_owned(),
        };
        let state = self.state.lock().await;
        let Some(bucket) = state.buckets.get(&key) else {
            return match after_event_id {
                Some(_) => Err(A2aEventStoreError::RetentionGap),
                None => Ok(A2aReplayPage {
                    events: Vec::new(),
                    high_water: 0,
                }),
            };
        };
        let current_high_water = state
            .next_ids
            .get(&key)
            .copied()
            .unwrap_or(1)
            .saturating_sub(1);
        let high_water = through_high_water.unwrap_or(current_high_water);
        if high_water > current_high_water || after_event_id.is_some_and(|after| after > high_water)
        {
            return Err(A2aEventStoreError::InvalidEventId);
        }
        if let Some(after) = after_event_id {
            let earliest = bucket
                .events
                .front()
                .map(|(event, _)| event.event_id)
                .unwrap_or_else(|| state.next_ids.get(&key).copied().unwrap_or(1));
            if after.saturating_add(1) < earliest {
                return Err(A2aEventStoreError::RetentionGap);
            }
        }
        let mut output = Vec::new();
        let mut bytes = 0_usize;
        for (event, event_bytes) in &bucket.events {
            if after_event_id.is_some_and(|after| event.event_id <= after) {
                continue;
            }
            if event.event_id > high_water {
                break;
            }
            if output.len() >= limits.max_events
                || bytes.saturating_add(*event_bytes) > limits.max_bytes
            {
                break;
            }
            bytes = bytes.saturating_add(*event_bytes);
            output.push(event.clone());
        }
        Ok(A2aReplayPage {
            events: output,
            high_water,
        })
    }
}

fn validate_logical_event_id(logical_event_id: &str) -> ProtocolResult<()> {
    if logical_event_id.is_empty()
        || logical_event_id.len() > 1024
        || logical_event_id.chars().any(char::is_control)
    {
        return Err(ProtocolError::invalid("invalid A2A logical event identity"));
    }
    Ok(())
}

fn ensure_event_bucket_capacity(
    state: &mut InMemoryEventStoreState,
    owner: &A2aEventOwner,
    max_buckets: usize,
    max_buckets_per_owner: usize,
) -> ProtocolResult<()> {
    let owner_count = state
        .buckets
        .keys()
        .filter(|key| key.owner == *owner)
        .count();
    if owner_count >= max_buckets_per_owner && !evict_oldest_terminal_bucket(state, Some(owner)) {
        return Err(ProtocolError::conflict(
            "active A2A event buckets exhausted the per-owner limit",
        ));
    }
    if state.buckets.len() >= max_buckets && !evict_oldest_terminal_bucket(state, None) {
        return Err(ProtocolError::conflict(
            "active A2A event buckets exhausted the global limit",
        ));
    }
    Ok(())
}

fn evict_oldest_terminal_bucket(
    state: &mut InMemoryEventStoreState,
    owner: Option<&A2aEventOwner>,
) -> bool {
    let candidate = state
        .buckets
        .iter()
        .filter(|(key, bucket)| bucket.terminal && owner.is_none_or(|owner| key.owner == *owner))
        .min_by(|(left_key, left), (right_key, right)| {
            left.last_touched
                .cmp(&right.last_touched)
                .then_with(|| left_key.cmp(right_key))
        })
        .map(|(key, _)| key.clone());
    candidate.is_some_and(|key| state.buckets.remove(&key).is_some())
}

fn validate_event_limits(limits: A2aEventRetention) -> ProtocolResult<()> {
    if limits.max_events == 0
        || limits.max_event_bytes == 0
        || limits.max_retained_bytes < limits.max_event_bytes
    {
        return Err(ProtocolError::invalid("invalid A2A event retention limits"));
    }
    Ok(())
}

#[cfg(test)]
mod in_memory_event_store_tests {
    use super::*;

    fn owner(subject: &str) -> A2aEventOwner {
        A2aEventOwner {
            subject: subject.to_owned(),
            tenant_id: Some("tenant".to_owned()),
        }
    }

    fn task_response(task_id: &str, state: A2aTaskState) -> A2aStreamResponse {
        A2aStreamResponse::Task(A2aWireTask {
            id: task_id.to_owned(),
            context_id: format!("context-{task_id}"),
            status: A2aWireTaskStatus { state },
            artifacts: None,
        })
    }

    fn retention(max_events: usize) -> A2aEventRetention {
        A2aEventRetention {
            max_events,
            max_event_bytes: 4096,
            max_retained_bytes: 4096 * max_events,
        }
    }

    #[tokio::test]
    async fn logical_append_is_idempotent_mismatch_safe_and_monotonic() {
        let store = InMemoryA2aEventStore::new(4, 2).unwrap();
        let owner = owner("owner-a");
        let working = task_response("task-a", A2aTaskState::Working);
        let first = store
            .append("logical-1", &owner, "task-a", &working, retention(1))
            .await
            .unwrap();
        assert!(matches!(first, A2aEventAppendOutcome::Inserted(_)));
        assert_eq!(first.event().logical_event_id, "logical-1");
        assert_eq!(first.event().event_id, 1);

        let duplicate = store
            .append("logical-1", &owner, "task-a", &working, retention(1))
            .await
            .unwrap();
        assert!(matches!(duplicate, A2aEventAppendOutcome::Existing(_)));
        assert_eq!(duplicate.event().logical_event_id, "logical-1");
        assert_eq!(duplicate.event(), first.event());

        let mismatch = store
            .append(
                "logical-1",
                &owner,
                "task-a",
                &task_response("task-a", A2aTaskState::InputRequired),
                retention(1),
            )
            .await
            .unwrap_err();
        assert_eq!(
            mismatch.permanent_error().unwrap().code,
            ProtocolErrorCode::Conflict
        );

        let second = store
            .append(
                "logical-2",
                &owner,
                "task-a",
                &task_response("task-a", A2aTaskState::InputRequired),
                retention(1),
            )
            .await
            .unwrap();
        assert_eq!(second.event().event_id, 2);
        assert_eq!(second.event().logical_event_id, "logical-2");
        let replayed = store
            .replay_page(
                &owner,
                "task-a",
                None,
                None,
                A2aReplayLimits {
                    max_events: 8,
                    max_bytes: 8192,
                },
            )
            .await
            .unwrap();
        assert_eq!(replayed.events.len(), 1);
        assert_eq!(replayed.events[0].logical_event_id, "logical-2");
        assert_eq!(replayed.events[0].event_id, 2);
        assert!(matches!(
            store
                .replay_page(
                    &owner,
                    "task-a",
                    Some(0),
                    None,
                    A2aReplayLimits {
                        max_events: 8,
                        max_bytes: 8192,
                    },
                )
                .await,
            Err(A2aEventStoreError::RetentionGap)
        ));
        assert_eq!(
            store
                .append("logical-1", &owner, "task-a", &working, retention(1))
                .await
                .unwrap()
                .event()
                .event_id,
            1
        );
    }

    #[tokio::test]
    async fn event_bucket_backpressure_is_retryable_and_terminal_bucket_is_permanent() {
        let store = InMemoryA2aEventStore::new(1, 1).unwrap();
        let owner = owner("owner-a");
        store
            .append(
                "task-a-working",
                &owner,
                "task-a",
                &task_response("task-a", A2aTaskState::Working),
                retention(8),
            )
            .await
            .unwrap();
        assert_eq!(
            store
                .append(
                    "task-b-blocked",
                    &owner,
                    "task-b",
                    &task_response("task-b", A2aTaskState::Working),
                    retention(8),
                )
                .await
                .unwrap_err(),
            A2aEventAppendError::Retryable
        );

        store
            .append(
                "task-a-completed",
                &owner,
                "task-a",
                &task_response("task-a", A2aTaskState::Completed),
                retention(8),
            )
            .await
            .unwrap();
        let terminal_rejection = store
            .append(
                "task-a-after-terminal",
                &owner,
                "task-a",
                &task_response("task-a", A2aTaskState::Working),
                retention(8),
            )
            .await
            .unwrap_err();
        assert_eq!(
            terminal_rejection.permanent_error().unwrap().code,
            ProtocolErrorCode::Conflict
        );

        assert!(matches!(
            store
                .append(
                    "task-b-reclaimed",
                    &owner,
                    "task-b",
                    &task_response("task-b", A2aTaskState::Working),
                    retention(8),
                )
                .await
                .unwrap(),
            A2aEventAppendOutcome::Inserted(_)
        ));
    }

    #[tokio::test]
    async fn bucket_limits_evict_only_oldest_terminal_and_keep_identity_tombstones() {
        assert!(InMemoryA2aEventStore::new(0, 0).is_err());
        assert!(InMemoryA2aEventStore::new(1, 2).is_err());

        let store = InMemoryA2aEventStore::new(2, 1).unwrap();
        let owner_a = owner("owner-a");
        store
            .append(
                "task-a-working",
                &owner_a,
                "task-a",
                &task_response("task-a", A2aTaskState::Working),
                retention(8),
            )
            .await
            .unwrap();
        assert_eq!(
            store
                .append(
                    "task-b-working-rejected",
                    &owner_a,
                    "task-b",
                    &task_response("task-b", A2aTaskState::Working),
                    retention(8),
                )
                .await
                .unwrap_err(),
            A2aEventAppendError::Retryable
        );
        store
            .append(
                "task-a-completed",
                &owner_a,
                "task-a",
                &task_response("task-a", A2aTaskState::Completed),
                retention(8),
            )
            .await
            .unwrap();
        store
            .append(
                "task-b-working",
                &owner_a,
                "task-b",
                &task_response("task-b", A2aTaskState::Working),
                retention(8),
            )
            .await
            .unwrap();
        assert_eq!(
            store
                .append(
                    "task-a-working",
                    &owner_a,
                    "task-a",
                    &task_response("task-a", A2aTaskState::Working),
                    retention(8),
                )
                .await
                .unwrap()
                .event()
                .event_id,
            1
        );
        store
            .append(
                "task-b-completed",
                &owner_a,
                "task-b",
                &task_response("task-b", A2aTaskState::Completed),
                retention(8),
            )
            .await
            .unwrap();
        let resumed_identity = store
            .append(
                "task-a-rejected-after-terminal",
                &owner_a,
                "task-a",
                &task_response("task-a", A2aTaskState::Working),
                retention(8),
            )
            .await
            .unwrap();
        assert_eq!(resumed_identity.event().event_id, 3);
        assert_eq!(
            resumed_identity.event().logical_event_id,
            "task-a-rejected-after-terminal"
        );

        let global = InMemoryA2aEventStore::new(2, 2).unwrap();
        let owner_b = owner("owner-b");
        let owner_c = owner("owner-c");
        global
            .append(
                "global-a",
                &owner_a,
                "global-a",
                &task_response("global-a", A2aTaskState::Working),
                retention(8),
            )
            .await
            .unwrap();
        global
            .append(
                "global-b",
                &owner_b,
                "global-b",
                &task_response("global-b", A2aTaskState::Working),
                retention(8),
            )
            .await
            .unwrap();
        assert_eq!(
            global
                .append(
                    "global-c-rejected",
                    &owner_c,
                    "global-c",
                    &task_response("global-c", A2aTaskState::Working),
                    retention(8),
                )
                .await
                .unwrap_err(),
            A2aEventAppendError::Retryable
        );
        global
            .append(
                "global-a-completed",
                &owner_a,
                "global-a",
                &task_response("global-a", A2aTaskState::Completed),
                retention(8),
            )
            .await
            .unwrap();
        global
            .append(
                "global-c",
                &owner_c,
                "global-c",
                &task_response("global-c", A2aTaskState::Working),
                retention(8),
            )
            .await
            .unwrap();
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct A2aWireTaskStatus {
    pub state: A2aTaskState,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum A2aWirePart {
    Text {
        text: String,
        #[serde(default, skip_serializing_if = "Option::is_none", rename = "mediaType")]
        media_type: Option<String>,
    },
    Data {
        data: Value,
        #[serde(default, skip_serializing_if = "Option::is_none", rename = "mediaType")]
        media_type: Option<String>,
    },
    FileUrl {
        url: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        filename: Option<String>,
        #[serde(rename = "mediaType")]
        media_type: String,
    },
    FileRaw {
        raw: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        filename: Option<String>,
        #[serde(rename = "mediaType")]
        media_type: String,
    },
}

impl From<&A2aPart> for A2aWirePart {
    fn from(part: &A2aPart) -> Self {
        match part {
            A2aPart::Text { text } => Self::Text {
                text: text.clone(),
                media_type: None,
            },
            A2aPart::Data { data } => Self::Data {
                data: data.clone(),
                media_type: None,
            },
            A2aPart::File { uri, media_type } => Self::FileUrl {
                url: uri.clone(),
                filename: None,
                media_type: media_type.clone(),
            },
        }
    }
}

impl From<&A2aContentPart> for A2aWirePart {
    fn from(part: &A2aContentPart) -> Self {
        match part {
            A2aContentPart::Text { text, media_type } => Self::Text {
                text: text.clone(),
                media_type: media_type.clone(),
            },
            A2aContentPart::Data { data, media_type } => Self::Data {
                data: data.clone(),
                media_type: media_type.clone(),
            },
            A2aContentPart::File {
                uri,
                media_type,
                filename,
            } => Self::FileUrl {
                url: uri.clone(),
                filename: filename.clone(),
                media_type: media_type.clone(),
            },
            A2aContentPart::Raw {
                raw,
                media_type,
                filename,
            } => Self::FileRaw {
                raw: STANDARD_BASE64.encode(raw),
                filename: filename.clone(),
                media_type: media_type.clone(),
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct A2aWireArtifact {
    pub artifact_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub parts: Vec<A2aWirePart>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, Value>,
}

impl From<&A2aArtifact> for A2aWireArtifact {
    fn from(artifact: &A2aArtifact) -> Self {
        Self {
            artifact_id: artifact.artifact_id.clone(),
            name: artifact.name.clone(),
            description: artifact.description.clone(),
            parts: artifact.parts.iter().map(A2aWirePart::from).collect(),
            metadata: artifact.metadata.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct A2aWireMessage {
    pub message_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    pub role: A2aRole,
    pub parts: Vec<A2aWirePart>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, Value>,
}

impl From<&A2aMessage> for A2aWireMessage {
    fn from(message: &A2aMessage) -> Self {
        let rich_parts = message.content_parts().ok();
        Self {
            message_id: message.message_id.clone(),
            context_id: message.context_id.clone(),
            task_id: message.task_id.clone(),
            role: message.role,
            parts: rich_parts
                .as_ref()
                .map(|parts| parts.iter().map(A2aWirePart::from).collect())
                .unwrap_or_else(|| message.parts.iter().map(A2aWirePart::from).collect()),
            metadata: a2a_wire_metadata_without_part_extensions(&message.metadata),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct A2aWireTask {
    pub id: String,
    pub context_id: String,
    pub status: A2aWireTaskStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifacts: Option<Vec<A2aWireArtifact>>,
}

impl From<&A2aTaskRecord> for A2aWireTask {
    fn from(task: &A2aTaskRecord) -> Self {
        Self::from_task(task, &[], false)
    }
}

impl A2aWireTask {
    fn from_task(task: &A2aTaskRecord, artifacts: &[A2aArtifact], include_artifacts: bool) -> Self {
        Self {
            id: task.mapping.task_id.clone(),
            context_id: task.mapping.context_id.clone(),
            status: A2aWireTaskStatus { state: task.state },
            artifacts: include_artifacts
                .then(|| artifacts.iter().map(A2aWireArtifact::from).collect()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct A2aTaskStatusUpdateEvent {
    pub task_id: String,
    pub context_id: String,
    pub status: A2aWireTaskStatus,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum A2aStreamResponse {
    Task(A2aWireTask),
    StatusUpdate(A2aTaskStatusUpdateEvent),
    Message(A2aWireMessage),
}

impl A2aStreamResponse {
    fn task(task: &A2aTaskRecord) -> Self {
        Self::Task(A2aWireTask::from(task))
    }

    fn status(task: &A2aTaskRecord) -> Self {
        Self::StatusUpdate(A2aTaskStatusUpdateEvent {
            task_id: task.mapping.task_id.clone(),
            context_id: task.mapping.context_id.clone(),
            status: A2aWireTaskStatus { state: task.state },
        })
    }

    fn message(message: &A2aMessage) -> Self {
        Self::Message(A2aWireMessage::from(message))
    }

    fn is_terminal(&self) -> bool {
        match self {
            Self::Task(task) => task.status.state.is_terminal(),
            Self::StatusUpdate(update) => update.status.state.is_terminal(),
            Self::Message(_) => true,
        }
    }
}

fn validate_persisted_event_binding(
    event: &A2aPersistedEvent,
    logical_event_id: Option<&str>,
    owner: &A2aEventOwner,
    task_id: &str,
    context_id: &str,
    expected_response: Option<&A2aStreamResponse>,
) -> ProtocolResult<()> {
    let response_matches =
        validate_stream_response_binding(&event.response, task_id, context_id).is_ok();
    if event.event_id == 0
        || event.event_id > 9_007_199_254_740_991
        || logical_event_id.is_some_and(|expected| event.logical_event_id != expected)
        || event.owner != *owner
        || event.task_id != task_id
        || !response_matches
        || expected_response.is_some_and(|expected| event.response != *expected)
    {
        return Err(ProtocolError::conflict(
            "A2A event store returned an invalid owner, task, context, id, or payload binding",
        ));
    }
    Ok(())
}

fn validate_stream_response_binding(
    response: &A2aStreamResponse,
    task_id: &str,
    context_id: &str,
) -> ProtocolResult<()> {
    let matches = match response {
        A2aStreamResponse::Task(task) => task.id == task_id && task.context_id == context_id,
        A2aStreamResponse::StatusUpdate(update) => {
            update.task_id == task_id && update.context_id == context_id
        }
        A2aStreamResponse::Message(message) => {
            message.role == A2aRole::Agent
                && message.context_id.as_deref() == Some(context_id)
                && message.task_id.is_none()
        }
    };
    if !matches {
        return Err(ProtocolError::conflict(
            "A2A stream response does not match its task and context",
        ));
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub struct A2aHttpConfig {
    pub path: String,
    pub allowed_hosts: BTreeSet<String>,
    pub allowed_origins: BTreeSet<String>,
    pub max_header_bytes: usize,
    pub max_body_bytes: usize,
    pub max_concurrency: usize,
    pub max_preauth_per_ip: usize,
    pub max_requests_per_minute: u32,
    pub max_rate_buckets: usize,
    pub max_retained_events: usize,
    pub max_event_bytes: usize,
    pub max_retained_event_bytes: usize,
    pub max_replay_events: usize,
    pub max_replay_bytes: usize,
    pub max_stream_events: usize,
    pub max_stream_bytes: usize,
    pub max_streams: usize,
    pub max_streams_per_owner: usize,
    pub live_channel_capacity: usize,
    pub handshake_timeout: Duration,
    pub request_timeout: Duration,
    pub blocking_dispatch_timeout: Duration,
    pub background_dispatch_timeout: Duration,
    pub dispatch_ack_timeout: Duration,
    pub max_background_dispatches: usize,
    pub max_background_dispatches_per_owner: usize,
    pub max_queued_dispatches: usize,
    pub max_queued_dispatches_per_owner: usize,
    pub max_control_dispatches: usize,
    pub max_control_dispatches_per_owner: usize,
    pub max_queued_control_dispatches: usize,
    pub max_queued_control_dispatches_per_owner: usize,
    pub max_control_probe_body_bytes: usize,
    pub control_probe_timeout: Duration,
    pub max_control_probes_per_ip: usize,
    pub max_control_probes_per_owner: usize,
    pub max_control_probes_per_minute: u32,
    pub max_control_requests_per_ip: usize,
    pub max_control_requests_per_owner: usize,
    pub startup_recovery_budget: Duration,
    pub max_recovery_concurrency: usize,
    pub max_recovery_concurrency_per_owner: usize,
    pub stream_idle_timeout: Duration,
    pub graceful_shutdown_timeout: Duration,
}

impl Default for A2aHttpConfig {
    fn default() -> Self {
        Self {
            path: "/a2a".into(),
            allowed_hosts: ["localhost".into(), "127.0.0.1".into(), "::1".into()]
                .into_iter()
                .collect(),
            allowed_origins: BTreeSet::new(),
            max_header_bytes: DEFAULT_A2A_HTTP_HEADER_BYTES,
            max_body_bytes: DEFAULT_A2A_HTTP_BODY_BYTES,
            max_concurrency: DEFAULT_A2A_HTTP_CONCURRENCY,
            max_preauth_per_ip: DEFAULT_A2A_HTTP_PREAUTH_PER_IP,
            max_requests_per_minute: DEFAULT_A2A_HTTP_RATE_PER_MINUTE,
            max_rate_buckets: DEFAULT_A2A_HTTP_RATE_BUCKETS,
            max_retained_events: DEFAULT_A2A_RETAINED_EVENTS,
            max_event_bytes: DEFAULT_A2A_EVENT_BYTES,
            max_retained_event_bytes: DEFAULT_A2A_REPLAY_BYTES,
            max_replay_events: DEFAULT_A2A_RETAINED_EVENTS,
            max_replay_bytes: DEFAULT_A2A_REPLAY_BYTES,
            max_stream_events: DEFAULT_A2A_STREAM_EVENTS,
            max_stream_bytes: DEFAULT_A2A_STREAM_BYTES,
            max_streams: DEFAULT_A2A_HTTP_STREAMS,
            max_streams_per_owner: DEFAULT_A2A_HTTP_STREAMS_PER_OWNER,
            live_channel_capacity: DEFAULT_A2A_RETAINED_EVENTS,
            handshake_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(60),
            blocking_dispatch_timeout: Duration::from_secs(55),
            background_dispatch_timeout: Duration::from_secs(300),
            dispatch_ack_timeout: Duration::from_secs(2),
            max_background_dispatches: 64,
            max_background_dispatches_per_owner: 8,
            max_queued_dispatches: 256,
            max_queued_dispatches_per_owner: 32,
            max_control_dispatches: DEFAULT_A2A_CONTROL_CONCURRENCY,
            max_control_dispatches_per_owner: DEFAULT_A2A_CONTROL_CONCURRENCY_PER_OWNER,
            max_queued_control_dispatches: DEFAULT_A2A_CONTROL_QUEUE,
            max_queued_control_dispatches_per_owner: DEFAULT_A2A_CONTROL_QUEUE_PER_OWNER,
            max_control_probe_body_bytes: DEFAULT_A2A_CONTROL_PROBE_BODY_BYTES,
            control_probe_timeout: DEFAULT_A2A_CONTROL_PROBE_TIMEOUT,
            max_control_probes_per_ip: DEFAULT_A2A_CONTROL_PROBES_PER_IP,
            max_control_probes_per_owner: DEFAULT_A2A_CONTROL_PROBES_PER_OWNER,
            max_control_probes_per_minute: DEFAULT_A2A_CONTROL_PROBES_PER_MINUTE,
            max_control_requests_per_ip: DEFAULT_A2A_CONTROL_REQUESTS_PER_IP,
            max_control_requests_per_owner: DEFAULT_A2A_CONTROL_REQUESTS_PER_OWNER,
            startup_recovery_budget: DEFAULT_A2A_STARTUP_RECOVERY_BUDGET,
            max_recovery_concurrency: DEFAULT_A2A_RECOVERY_CONCURRENCY,
            max_recovery_concurrency_per_owner: DEFAULT_A2A_RECOVERY_CONCURRENCY_PER_OWNER,
            stream_idle_timeout: Duration::from_secs(300),
            graceful_shutdown_timeout: Duration::from_secs(5),
        }
    }
}

impl A2aHttpConfig {
    fn validate(&self) -> ProtocolResult<()> {
        if !self.path.starts_with('/')
            || self.path == A2A_AGENT_CARD_PATH
            || self.path.contains(['\r', '\n', '?', '#'])
            || self.allowed_hosts.is_empty()
            || self.max_header_bytes == 0
            || self.max_body_bytes == 0
            || self.max_concurrency == 0
            || self.max_preauth_per_ip == 0
            || self.max_requests_per_minute == 0
            || self.max_rate_buckets == 0
            || self.max_replay_events == 0
            || self.max_replay_bytes == 0
            || self.max_stream_events == 0
            || self.max_stream_bytes == 0
            || self.max_streams == 0
            || self.max_streams_per_owner == 0
            || self.max_streams_per_owner > self.max_streams
            || self.live_channel_capacity == 0
            || self.handshake_timeout.is_zero()
            || self.request_timeout.is_zero()
            || self.handshake_timeout >= self.request_timeout
            || self.blocking_dispatch_timeout.is_zero()
            || self.blocking_dispatch_timeout >= self.request_timeout
            || self.background_dispatch_timeout.is_zero()
            || self.dispatch_ack_timeout.is_zero()
            || self.dispatch_ack_timeout >= self.graceful_shutdown_timeout
            || self.max_background_dispatches == 0
            || self.max_background_dispatches_per_owner == 0
            || self.max_background_dispatches_per_owner > self.max_background_dispatches
            || self.max_queued_dispatches == 0
            || self.max_queued_dispatches_per_owner == 0
            || self.max_queued_dispatches_per_owner > self.max_queued_dispatches
            || self.max_control_dispatches == 0
            || self.max_control_dispatches_per_owner == 0
            || self.max_control_dispatches_per_owner > self.max_control_dispatches
            || self.max_queued_control_dispatches == 0
            || self.max_queued_control_dispatches_per_owner == 0
            || self.max_queued_control_dispatches_per_owner > self.max_queued_control_dispatches
            || self.max_control_probe_body_bytes == 0
            || self.control_probe_timeout.is_zero()
            || self.max_control_probes_per_ip == 0
            || self.max_control_probes_per_ip > self.max_control_dispatches
            || self.max_control_probes_per_owner != 1
            || self.max_control_probes_per_minute == 0
            || self.max_control_requests_per_ip == 0
            || self.max_control_requests_per_ip > self.max_control_dispatches
            || self.max_control_requests_per_owner == 0
            || self.max_control_requests_per_owner > self.max_control_dispatches
            || (self.max_control_dispatches > 1
                && (self.max_control_requests_per_ip >= self.max_control_dispatches
                    || self.max_control_requests_per_owner >= self.max_control_dispatches))
            || self.startup_recovery_budget < Duration::from_millis(3)
            || self.max_recovery_concurrency == 0
            || self.max_recovery_concurrency_per_owner == 0
            || self.max_recovery_concurrency_per_owner > self.max_recovery_concurrency
            || self.stream_idle_timeout.is_zero()
            || self.graceful_shutdown_timeout.is_zero()
        {
            return Err(ProtocolError::invalid("invalid A2A HTTP configuration"));
        }
        validate_event_limits(A2aEventRetention {
            max_events: self.max_retained_events,
            max_event_bytes: self.max_event_bytes,
            max_retained_bytes: self.max_retained_event_bytes,
        })?;
        if self.allowed_hosts.iter().any(|host| {
            host.trim().is_empty()
                || host.contains(['\r', '\n', '/', '@'])
                || normalize_host(host).as_deref() != Some(host.as_str())
        }) {
            return Err(ProtocolError::invalid("invalid A2A Host allowlist"));
        }
        if self
            .allowed_origins
            .iter()
            .any(|origin| origin.is_empty() || origin.contains(['\r', '\n']))
        {
            return Err(ProtocolError::invalid("invalid A2A Origin allowlist"));
        }
        Ok(())
    }

    fn retention(&self) -> A2aEventRetention {
        A2aEventRetention {
            max_events: self.max_retained_events,
            max_event_bytes: self.max_event_bytes,
            max_retained_bytes: self.max_retained_event_bytes,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct RateBucket {
    minute: u64,
    count: u32,
}

#[derive(Debug, Default)]
struct PreAuthLimiterState {
    closed: bool,
    active_per_ip: BTreeMap<IpAddr, usize>,
}

#[derive(Debug)]
struct PreAuthLimiter {
    max_per_ip: usize,
    state: StdMutex<PreAuthLimiterState>,
}

impl PreAuthLimiter {
    fn new(max_per_ip: usize) -> Self {
        Self {
            max_per_ip,
            state: StdMutex::new(PreAuthLimiterState::default()),
        }
    }

    fn try_acquire(self: &Arc<Self>, ip: IpAddr) -> ProtocolResult<Option<PreAuthPermit>> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| ProtocolError::conflict("A2A pre-auth limiter lock poisoned"))?;
        if state.closed {
            return Ok(None);
        }
        let active = state.active_per_ip.get(&ip).copied().unwrap_or_default();
        if active >= self.max_per_ip {
            return Ok(None);
        }
        state.active_per_ip.insert(ip, active + 1);
        Ok(Some(PreAuthPermit {
            limiter: self.clone(),
            ip,
        }))
    }

    fn close(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.closed = true;
    }

    fn release(&self, ip: IpAddr) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(active) = state.active_per_ip.get_mut(&ip) {
            *active = active.saturating_sub(1);
            if *active == 0 {
                state.active_per_ip.remove(&ip);
            }
        }
    }
}

#[derive(Debug)]
struct PreAuthPermit {
    limiter: Arc<PreAuthLimiter>,
    ip: IpAddr,
}

impl Drop for PreAuthPermit {
    fn drop(&mut self) {
        self.limiter.release(self.ip);
    }
}

#[derive(Debug)]
struct ControlProbeWaiterRecord {
    ip: IpAddr,
    owner: A2aEventOwner,
    owner_rate_committed: bool,
    sender: oneshot::Sender<ControlProbeLease>,
}

#[derive(Debug, Default)]
struct ControlProbeLimiterState {
    closed: bool,
    active_total: usize,
    active_per_ip: BTreeMap<IpAddr, usize>,
    active_per_owner: BTreeMap<A2aEventOwner, usize>,
    rate_minute: u64,
    attempts_per_ip: BTreeMap<IpAddr, u32>,
    attempts_per_owner: BTreeMap<A2aEventOwner, u32>,
    next_ticket: u64,
    waiters: BTreeMap<u64, ControlProbeWaiterRecord>,
    waiters_per_ip: BTreeMap<IpAddr, usize>,
    waiters_per_owner: BTreeMap<A2aEventOwner, usize>,
    last_granted_owner: Option<A2aEventOwner>,
}

#[derive(Debug)]
struct ControlProbeLimiter {
    max_active: usize,
    max_per_ip: usize,
    max_per_owner: usize,
    max_per_minute: u32,
    max_rate_buckets: usize,
    max_waiters: usize,
    max_waiters_per_ip: usize,
    max_waiters_per_owner: usize,
    state: StdMutex<ControlProbeLimiterState>,
}

#[derive(Debug, Clone, Copy)]
struct ControlProbeLimits {
    max_active: usize,
    max_per_ip: usize,
    max_per_owner: usize,
    max_per_minute: u32,
    max_rate_buckets: usize,
    max_waiters: usize,
    max_waiters_per_ip: usize,
    max_waiters_per_owner: usize,
}

impl ControlProbeLimiter {
    fn new(limits: ControlProbeLimits) -> Self {
        Self {
            max_active: limits.max_active,
            max_per_ip: limits.max_per_ip,
            max_per_owner: limits.max_per_owner,
            max_per_minute: limits.max_per_minute,
            max_rate_buckets: limits.max_rate_buckets,
            max_waiters: limits.max_waiters,
            max_waiters_per_ip: limits.max_waiters_per_ip,
            max_waiters_per_owner: limits.max_waiters_per_owner,
            state: StdMutex::new(ControlProbeLimiterState::default()),
        }
    }

    fn try_acquire(
        self: &Arc<Self>,
        ip: IpAddr,
        owner: A2aEventOwner,
    ) -> ProtocolResult<Option<ControlProbeLease>> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| ProtocolError::conflict("A2A control admission limiter lock poisoned"))?;
        if state.closed {
            return Ok(None);
        }
        let ip_active = state.active_per_ip.get(&ip).copied().unwrap_or_default();
        let owner_active = state
            .active_per_owner
            .get(&owner)
            .copied()
            .unwrap_or_default();
        if state.active_total >= self.max_active
            || ip_active >= self.max_per_ip
            || owner_active >= self.max_per_owner
        {
            return Ok(None);
        }
        let minute = current_minute();
        if state.rate_minute != minute {
            state.rate_minute = minute;
            state.attempts_per_ip.clear();
            state.attempts_per_owner.clear();
        }
        if !state.attempts_per_owner.contains_key(&owner)
            && state.attempts_per_owner.len() >= self.max_rate_buckets
        {
            return Ok(None);
        }
        let owner_attempts = state
            .attempts_per_owner
            .get(&owner)
            .copied()
            .unwrap_or_default();
        if owner_attempts >= self.max_per_minute {
            return Ok(None);
        }
        state.active_total += 1;
        state.active_per_ip.insert(ip, ip_active + 1);
        state
            .active_per_owner
            .insert(owner.clone(), owner_active + 1);
        Ok(Some(ControlProbeLease {
            limiter: self.clone(),
            ip,
            owner,
            owner_rate_committed: false,
        }))
    }

    fn decrement_waiter_counts(
        state: &mut ControlProbeLimiterState,
        ip: IpAddr,
        owner: &A2aEventOwner,
    ) {
        if let Some(count) = state.waiters_per_ip.get_mut(&ip) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                state.waiters_per_ip.remove(&ip);
            }
        }
        if let Some(count) = state.waiters_per_owner.get_mut(owner) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                state.waiters_per_owner.remove(owner);
            }
        }
    }

    fn remove_waiter(
        state: &mut ControlProbeLimiterState,
        ticket: u64,
    ) -> Option<ControlProbeWaiterRecord> {
        let record = state.waiters.remove(&ticket)?;
        Self::decrement_waiter_counts(state, record.ip, &record.owner);
        Some(record)
    }

    /// Grant bounded waiter slots round-robin across owners, preserving FIFO order within one
    /// owner. This prevents two owners with deep slow-body backlogs from repeatedly taking each
    /// newly released shared-NAT slot ahead of a third owner.
    fn grant_waiters_locked(
        self: &Arc<Self>,
        state: &mut ControlProbeLimiterState,
    ) -> Vec<ControlProbeLease> {
        if state.closed || state.active_total >= self.max_active || state.waiters.is_empty() {
            return Vec::new();
        }
        let mut owners = state
            .waiters
            .values()
            .map(|record| record.owner.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        if let Some(previous) = state.last_granted_owner.as_ref() {
            let start = owners
                .iter()
                .position(|owner| owner > previous)
                .unwrap_or_default();
            owners.rotate_left(start);
        }
        let mut failed_deliveries = Vec::new();
        for owner in owners {
            if state.active_total >= self.max_active {
                break;
            }
            let Some(ticket) = state
                .waiters
                .iter()
                .find_map(|(ticket, record)| (record.owner == owner).then_some(*ticket))
            else {
                continue;
            };
            let Some(record) = state.waiters.get(&ticket) else {
                continue;
            };
            let ip_active = state
                .active_per_ip
                .get(&record.ip)
                .copied()
                .unwrap_or_default();
            let owner_active = state
                .active_per_owner
                .get(&record.owner)
                .copied()
                .unwrap_or_default();
            if ip_active >= self.max_per_ip || owner_active >= self.max_per_owner {
                continue;
            }
            let record = Self::remove_waiter(state, ticket)
                .expect("the selected A2A control waiter still exists");
            state.active_total += 1;
            state.active_per_ip.insert(record.ip, ip_active + 1);
            state
                .active_per_owner
                .insert(record.owner.clone(), owner_active + 1);
            state.last_granted_owner = Some(record.owner.clone());
            let lease = ControlProbeLease {
                limiter: self.clone(),
                ip: record.ip,
                owner: record.owner,
                owner_rate_committed: record.owner_rate_committed,
            };
            if let Err(lease) = record.sender.send(lease) {
                // Drop outside the limiter lock: dropping a lease re-enters release().
                failed_deliveries.push(lease);
            }
        }
        failed_deliveries
    }

    async fn acquire_until(
        self: &Arc<Self>,
        ip: IpAddr,
        owner: A2aEventOwner,
        deadline: Instant,
        charge_owner_rate: bool,
    ) -> ProtocolResult<Option<ControlProbeLease>> {
        let (ticket, receiver, failed_deliveries, evicted_waiter) = {
            let mut state = self.state.lock().map_err(|_| {
                ProtocolError::conflict("A2A control admission limiter lock poisoned")
            })?;
            if state.closed || Instant::now() >= deadline {
                return Ok(None);
            }
            let owner_attempts = if charge_owner_rate {
                let minute = current_minute();
                if state.rate_minute != minute {
                    state.rate_minute = minute;
                    state.attempts_per_ip.clear();
                    state.attempts_per_owner.clear();
                }
                if !state.attempts_per_owner.contains_key(&owner)
                    && state.attempts_per_owner.len() >= self.max_rate_buckets
                {
                    return Ok(None);
                }
                let owner_attempts = state
                    .attempts_per_owner
                    .get(&owner)
                    .copied()
                    .unwrap_or_default();
                if owner_attempts >= self.max_per_minute {
                    return Ok(None);
                }
                Some(owner_attempts)
            } else {
                None
            };

            let ip_active = state.active_per_ip.get(&ip).copied().unwrap_or_default();
            let owner_active = state
                .active_per_owner
                .get(&owner)
                .copied()
                .unwrap_or_default();
            let can_activate = state.active_total < self.max_active
                && ip_active < self.max_per_ip
                && owner_active < self.max_per_owner;

            let mut evicted_waiter = None;
            let mut ip_waiters = state.waiters_per_ip.get(&ip).copied().unwrap_or_default();
            let mut owner_waiters = state
                .waiters_per_owner
                .get(&owner)
                .copied()
                .unwrap_or_default();
            if !can_activate
                && (state.waiters.len() >= self.max_waiters
                    || ip_waiters >= self.max_waiters_per_ip
                    || owner_waiters >= self.max_waiters_per_owner)
            {
                // Preserve one cross-owner reachability lane when the entire bounded queue is
                // monopolized by the owner already holding active capacity. Evict that owner's
                // newest waiter (never its active request); FIFO order among retained waiters is
                // unchanged, and an incumbent owner cannot evict another owner while still active.
                let evict_ticket = (state.waiters.len() >= self.max_waiters
                    && owner_active == 0
                    && owner_waiters == 0)
                    .then(|| {
                        state.waiters.iter().rev().find_map(|(ticket, record)| {
                            (record.owner != owner
                                && state
                                    .active_per_owner
                                    .get(&record.owner)
                                    .copied()
                                    .unwrap_or_default()
                                    > 0)
                            .then_some(*ticket)
                        })
                    })
                    .flatten();
                if let Some(ticket) = evict_ticket {
                    evicted_waiter = Self::remove_waiter(&mut state, ticket);
                    ip_waiters = state.waiters_per_ip.get(&ip).copied().unwrap_or_default();
                    owner_waiters = state
                        .waiters_per_owner
                        .get(&owner)
                        .copied()
                        .unwrap_or_default();
                }
                if state.waiters.len() >= self.max_waiters
                    || ip_waiters >= self.max_waiters_per_ip
                    || owner_waiters >= self.max_waiters_per_owner
                {
                    return Ok(None);
                }
            }

            let owner_rate_committed = owner_attempts.is_some();
            if let Some(owner_attempts) = owner_attempts {
                state
                    .attempts_per_owner
                    .insert(owner.clone(), owner_attempts + 1);
            }
            if can_activate {
                state.active_total += 1;
                state.active_per_ip.insert(ip, ip_active + 1);
                state
                    .active_per_owner
                    .insert(owner.clone(), owner_active + 1);
                drop(evicted_waiter);
                return Ok(Some(ControlProbeLease {
                    limiter: self.clone(),
                    ip,
                    owner,
                    owner_rate_committed,
                }));
            }
            let ticket = state.next_ticket;
            state.next_ticket = state.next_ticket.checked_add(1).ok_or_else(|| {
                ProtocolError::conflict("A2A control admission ticket capacity is exhausted")
            })?;
            let (sender, receiver) = oneshot::channel();
            state.waiters.insert(
                ticket,
                ControlProbeWaiterRecord {
                    ip,
                    owner: owner.clone(),
                    owner_rate_committed,
                    sender,
                },
            );
            state.waiters_per_ip.insert(ip, ip_waiters + 1);
            state
                .waiters_per_owner
                .insert(owner.clone(), owner_waiters + 1);
            let failed_deliveries = self.grant_waiters_locked(&mut state);
            (ticket, receiver, failed_deliveries, evicted_waiter)
        };
        drop(evicted_waiter);
        drop(failed_deliveries);
        match tokio::time::timeout_at(deadline, receiver).await {
            Ok(Ok(lease)) => Ok(Some(lease)),
            Ok(Err(_)) => Ok(None),
            Err(_) => {
                let failed_deliveries = {
                    let mut state = self.state.lock().map_err(|_| {
                        ProtocolError::conflict("A2A control admission limiter lock poisoned")
                    })?;
                    Self::remove_waiter(&mut state, ticket);
                    self.grant_waiters_locked(&mut state)
                };
                drop(failed_deliveries);
                Ok(None)
            }
        }
    }

    /// Reserve a standard pre-method cancellation probe fairly for this IP and owner. The attempt
    /// budget is charged before the body is read and is a hard cap: once exhausted, this method
    /// returns `None` until the minute rolls over.
    async fn acquire_probe_until(
        self: &Arc<Self>,
        ip: IpAddr,
        owner: A2aEventOwner,
        deadline: Instant,
    ) -> ProtocolResult<Option<ControlProbeLease>> {
        self.acquire_until(ip, owner, deadline, true).await
    }

    /// Reserve the separately bounded last-chance body-classification lane. Callers must use a
    /// distinct limiter instance, enforce the same short absolute deadline, reject every parsed
    /// non-`CancelTask`, and still apply the governed control-request limiter to an exact match.
    async fn acquire_exact_classification_until(
        self: &Arc<Self>,
        ip: IpAddr,
        owner: A2aEventOwner,
        deadline: Instant,
    ) -> ProtocolResult<Option<ControlProbeLease>> {
        self.acquire_until(ip, owner, deadline, false).await
    }

    fn commit_rate(&self, _ip: IpAddr, owner: &A2aEventOwner) -> ProtocolResult<bool> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| ProtocolError::conflict("A2A control admission limiter lock poisoned"))?;
        if state.closed {
            return Ok(false);
        }
        let minute = current_minute();
        if state.rate_minute != minute {
            state.rate_minute = minute;
            state.attempts_per_ip.clear();
            state.attempts_per_owner.clear();
        }
        if !state.attempts_per_owner.contains_key(owner)
            && state.attempts_per_owner.len() >= self.max_rate_buckets
        {
            return Ok(false);
        }
        let owner_attempts = state
            .attempts_per_owner
            .get(owner)
            .copied()
            .unwrap_or_default();
        if owner_attempts >= self.max_per_minute {
            return Ok(false);
        }
        state
            .attempts_per_owner
            .insert(owner.clone(), owner_attempts + 1);
        Ok(true)
    }

    /// Charge the shared-network rate only after mapper governance proves that this principal
    /// owns the referenced task. Pre-body probes and foreign/malformed cancellation requests must
    /// never consume another principal's shared-NAT allowance.
    fn commit_ip_rate(&self, ip: IpAddr) -> ProtocolResult<bool> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| ProtocolError::conflict("A2A control admission limiter lock poisoned"))?;
        if state.closed {
            return Ok(false);
        }
        let minute = current_minute();
        if state.rate_minute != minute {
            state.rate_minute = minute;
            state.attempts_per_ip.clear();
            state.attempts_per_owner.clear();
        }
        if !state.attempts_per_ip.contains_key(&ip)
            && state.attempts_per_ip.len() >= self.max_rate_buckets
        {
            return Ok(false);
        }
        let attempts = state.attempts_per_ip.get(&ip).copied().unwrap_or_default();
        if attempts >= self.max_per_minute {
            return Ok(false);
        }
        state.attempts_per_ip.insert(ip, attempts + 1);
        Ok(true)
    }

    fn close(&self) {
        let waiters = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.closed = true;
            state.waiters_per_ip.clear();
            state.waiters_per_owner.clear();
            std::mem::take(&mut state.waiters)
        };
        drop(waiters);
    }

    fn release(self: &Arc<Self>, ip: IpAddr, owner: &A2aEventOwner) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.active_total = state.active_total.saturating_sub(1);
        if let Some(active) = state.active_per_ip.get_mut(&ip) {
            *active = active.saturating_sub(1);
            if *active == 0 {
                state.active_per_ip.remove(&ip);
            }
        }
        if let Some(active) = state.active_per_owner.get_mut(owner) {
            *active = active.saturating_sub(1);
            if *active == 0 {
                state.active_per_owner.remove(owner);
            }
        }
        let failed_deliveries = self.grant_waiters_locked(&mut state);
        drop(state);
        drop(failed_deliveries);
    }
}

#[derive(Debug)]
#[must_use = "dropping the control probe lease releases its per-IP and per-owner quota"]
struct ControlProbeLease {
    limiter: Arc<ControlProbeLimiter>,
    ip: IpAddr,
    owner: A2aEventOwner,
    owner_rate_committed: bool,
}

impl ControlProbeLease {
    fn commit_rate(&mut self) -> ProtocolResult<bool> {
        if self.owner_rate_committed {
            return Ok(true);
        }
        self.owner_rate_committed = self.limiter.commit_rate(self.ip, &self.owner)?;
        Ok(self.owner_rate_committed)
    }
}

impl Drop for ControlProbeLease {
    fn drop(&mut self) {
        self.limiter.release(self.ip, &self.owner);
    }
}

fn current_minute() -> u64 {
    current_unix_millis() / 60_000
}

fn current_unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[derive(Debug, Default)]
struct StreamQuotaState {
    closed: bool,
    active: usize,
    active_per_owner: BTreeMap<A2aEventOwner, usize>,
}

#[derive(Debug)]
struct StreamQuota {
    max_streams: usize,
    max_streams_per_owner: usize,
    state: StdMutex<StreamQuotaState>,
}

impl StreamQuota {
    fn new(max_streams: usize, max_streams_per_owner: usize) -> Self {
        Self {
            max_streams,
            max_streams_per_owner,
            state: StdMutex::new(StreamQuotaState::default()),
        }
    }

    fn try_acquire(self: &Arc<Self>, owner: A2aEventOwner) -> ProtocolResult<Option<StreamLease>> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| ProtocolError::conflict("A2A stream quota lock poisoned"))?;
        if state.closed || state.active >= self.max_streams {
            return Ok(None);
        }
        let owner_active = state
            .active_per_owner
            .get(&owner)
            .copied()
            .unwrap_or_default();
        if owner_active >= self.max_streams_per_owner {
            return Ok(None);
        }
        state.active += 1;
        state
            .active_per_owner
            .insert(owner.clone(), owner_active + 1);
        Ok(Some(StreamLease {
            quota: self.clone(),
            owner,
        }))
    }

    fn close(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.closed = true;
    }

    fn release(&self, owner: &A2aEventOwner) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.active = state.active.saturating_sub(1);
        if let Some(active) = state.active_per_owner.get_mut(owner) {
            *active = active.saturating_sub(1);
            if *active == 0 {
                state.active_per_owner.remove(owner);
            }
        }
    }
}

#[derive(Debug)]
#[must_use = "dropping the stream lease releases its global and per-owner quota"]
struct StreamLease {
    quota: Arc<StreamQuota>,
    owner: A2aEventOwner,
}

impl Drop for StreamLease {
    fn drop(&mut self) {
        self.quota.release(&self.owner);
    }
}

#[derive(Debug, Clone)]
struct LiveEvent(A2aPersistedEvent);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum DispatchLane {
    Message,
    Cancellation,
}

#[derive(Debug, Clone, Copy)]
enum CancellationIngress {
    Public(IpAddr),
    Protected,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct DispatchTaskKey {
    owner: A2aEventOwner,
    task_id: String,
    lane: DispatchLane,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct DispatchMessageKey {
    owner: A2aEventOwner,
    task_id: String,
    message_id: String,
    run_id: String,
    lane: DispatchLane,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct DispatchOwnerKey {
    owner: A2aEventOwner,
    lane: DispatchLane,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct DispatchRuntimeKey {
    owner: A2aEventOwner,
    task_id: String,
}

#[derive(Debug)]
struct InflightDispatch {
    completion: broadcast::Sender<ProtocolResult<()>>,
}

#[derive(Debug, Default)]
struct DispatchSchedulerState {
    accepted: usize,
    accepted_by_lane: BTreeMap<DispatchLane, usize>,
    per_owner: BTreeMap<DispatchOwnerKey, usize>,
    inflight_messages: BTreeMap<DispatchMessageKey, InflightDispatch>,
    task_semaphores: BTreeMap<DispatchTaskKey, (Arc<Semaphore>, usize)>,
    owner_semaphores: BTreeMap<DispatchOwnerKey, (Arc<Semaphore>, usize)>,
}

#[derive(Debug)]
struct ScheduledA2aDispatch {
    job: A2aDispatchJob,
    owner: A2aEventOwner,
    task_key: DispatchTaskKey,
    message_key: DispatchMessageKey,
    task_semaphore: Arc<Semaphore>,
    owner_semaphore: Arc<Semaphore>,
    completion: broadcast::Sender<ProtocolResult<()>>,
    commit: oneshot::Receiver<DispatchCommit>,
    start: Option<oneshot::Receiver<()>>,
}

#[derive(Debug)]
struct DispatchAcceptance {
    completion: broadcast::Receiver<ProtocolResult<()>>,
    commit: Option<oneshot::Sender<DispatchCommit>>,
    start: Option<oneshot::Sender<()>>,
    newly_reserved: bool,
}

#[derive(Debug)]
struct PreparedLiveDispatch {
    dispatch_id: String,
    job: A2aDispatchJob,
    reservation: DispatchReservation,
}

#[derive(Debug)]
struct StagedLiveDispatch {
    dispatch_id: String,
    acceptance: Option<DispatchAcceptance>,
    recovery_claim: RecoveryAttemptClaim,
}

#[derive(Debug)]
struct DispatchCommit {
    expected_dispatch_attempt: Option<u32>,
    expected_cancellation_attempt: Option<u32>,
    host_already_fenced: bool,
    dispatch_recovery_claim: Option<RecoveryAttemptClaim>,
}

#[derive(Debug)]
struct RecoveryAttemptClaim {
    attempted: Arc<StdMutex<BTreeSet<String>>>,
    durable_id: String,
    release_on_drop: bool,
}

impl RecoveryAttemptClaim {
    fn quarantine(mut self) {
        self.release_on_drop = false;
    }
}

impl Drop for RecoveryAttemptClaim {
    fn drop(&mut self) {
        if self.release_on_drop {
            self.attempted
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&self.durable_id);
        }
    }
}

#[derive(Debug)]
struct ScheduledDispatchCompletion {
    owner: A2aEventOwner,
    task_key: DispatchTaskKey,
    message_key: DispatchMessageKey,
    recovery_claim: ScheduledRecoveryClaim,
}

#[derive(Debug, Default)]
enum ScheduledRecoveryClaim {
    #[default]
    None,
    Dispatch {
        claim: RecoveryAttemptClaim,
    },
    Cancellation {
        cancellation_id: String,
    },
}

#[derive(Debug, Clone, Copy)]
enum CancellationReconciliationCache {
    InFlight {
        attempts: u32,
    },
    Complete {
        attempts: u32,
        decision: A2aUnknownDispatchDecision,
    },
}

struct CancellationReconciliationOwnerGuard<'a> {
    reconciliations: &'a StdMutex<BTreeMap<String, CancellationReconciliationCache>>,
    notify: &'a Notify,
    cancellation_id: String,
    attempts: u32,
    completed: bool,
}

impl CancellationReconciliationOwnerGuard<'_> {
    fn complete(mut self, decision: A2aUnknownDispatchDecision) {
        let mut reconciliations = self
            .reconciliations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if matches!(
            reconciliations.get(&self.cancellation_id),
            Some(CancellationReconciliationCache::InFlight { attempts })
                if *attempts == self.attempts
        ) {
            reconciliations.insert(
                self.cancellation_id.clone(),
                CancellationReconciliationCache::Complete {
                    attempts: self.attempts,
                    decision,
                },
            );
        }
        self.completed = true;
        drop(reconciliations);
        self.notify.notify_waiters();
    }
}

impl Drop for CancellationReconciliationOwnerGuard<'_> {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        let mut reconciliations = self
            .reconciliations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if matches!(
            reconciliations.get(&self.cancellation_id),
            Some(CancellationReconciliationCache::InFlight { attempts })
                if *attempts == self.attempts
        ) {
            reconciliations.remove(&self.cancellation_id);
        }
        drop(reconciliations);
        self.notify.notify_waiters();
    }
}

#[derive(Debug)]
struct SnapshotCommitRegistry {
    accepting: bool,
    next_id: u64,
    handles: BTreeMap<u64, JoinHandle<()>>,
}

impl Default for SnapshotCommitRegistry {
    fn default() -> Self {
        Self {
            accepting: true,
            next_id: 1,
            handles: BTreeMap::new(),
        }
    }
}

#[cfg(test)]
#[derive(Debug)]
struct CancellationCommitAfterSendHook {
    sent: Notify,
    resume_sender: Notify,
}

#[cfg(test)]
impl CancellationCommitAfterSendHook {
    fn new() -> Self {
        Self {
            sent: Notify::new(),
            resume_sender: Notify::new(),
        }
    }
}

/// Bounded, experimental A2A network server backed by legacy full-snapshot CAS persistence.
/// High-throughput production deployments should wait for or provide the delta-journal path.
pub struct A2aHttpJsonRpcServer {
    mapper: Arc<Mutex<A2aMapper>>,
    snapshot_commit: Arc<RwLock<()>>,
    snapshot_version: Arc<StdMutex<Option<A2aSnapshotVersion>>>,
    snapshot_store_initialized: Arc<StdAtomicBool>,
    snapshot_commit_failed: Arc<StdAtomicBool>,
    snapshot_failure: Arc<StdMutex<Option<ProtocolError>>>,
    snapshot_fail_stop: CancellationToken,
    snapshot_commit_tasks: Arc<StdMutex<SnapshotCommitRegistry>>,
    serving: Arc<StdAtomicBool>,
    snapshots: Arc<dyn A2aMapperSnapshotStore>,
    events: Arc<dyn A2aEventStore>,
    authenticator: Arc<dyn A2aHttpAuthenticator>,
    host: Arc<dyn A2aDispatchHost>,
    agent_card: A2aAgentCard,
    config: A2aHttpConfig,
    rate: StdMutex<BTreeMap<IpAddr, RateBucket>>,
    preauth_limiter: Arc<PreAuthLimiter>,
    control_probe_limiter: Arc<ControlProbeLimiter>,
    control_exact_probe_limiter: Arc<ControlProbeLimiter>,
    control_request_limiter: Arc<ControlProbeLimiter>,
    stream_quota: Arc<StreamQuota>,
    live: broadcast::Sender<LiveEvent>,
    dispatch_sender: mpsc::Sender<ScheduledA2aDispatch>,
    dispatch_receiver: Mutex<Option<mpsc::Receiver<ScheduledA2aDispatch>>>,
    control_sender: mpsc::Sender<ScheduledA2aDispatch>,
    control_receiver: Mutex<Option<mpsc::Receiver<ScheduledA2aDispatch>>>,
    dispatch_state: StdMutex<DispatchSchedulerState>,
    recovery_attempted: Arc<StdMutex<BTreeSet<String>>>,
    cancellation_reconciliations: StdMutex<BTreeMap<String, CancellationReconciliationCache>>,
    cancellation_reconciliation_notify: Notify,
    running_dispatch_cancellations: StdMutex<BTreeMap<DispatchRuntimeKey, CancellationToken>>,
    #[cfg(test)]
    cancellation_commit_after_send_hook: StdMutex<Option<Arc<CancellationCommitAfterSendHook>>>,
    #[cfg(test)]
    cancellation_claim_release_hook: StdMutex<Option<Arc<Notify>>>,
    request_global: Arc<Semaphore>,
    control_probe_global: Arc<Semaphore>,
    control_exact_probe_global: Arc<Semaphore>,
    control_request_global: Arc<Semaphore>,
    response_write_global: Arc<Semaphore>,
    control_response_write_global: Arc<Semaphore>,
    dispatch_global: Arc<Semaphore>,
    control_global: Arc<Semaphore>,
}

fn fair_recovery_batch<T>(
    records: Vec<T>,
    cursor: &mut Option<A2aEventOwner>,
    max_records: usize,
    max_per_owner: usize,
    owner_of: impl Fn(&T) -> A2aEventOwner,
) -> Vec<T> {
    let mut by_owner: BTreeMap<A2aEventOwner, VecDeque<T>> = BTreeMap::new();
    for record in records {
        by_owner
            .entry(owner_of(&record))
            .or_default()
            .push_back(record);
    }
    let mut owners: Vec<A2aEventOwner> = by_owner.keys().cloned().collect();
    if owners.is_empty() {
        return Vec::new();
    }
    if let Some(previous) = cursor.as_ref() {
        let start = owners
            .iter()
            .position(|owner| owner > previous)
            .unwrap_or_default();
        owners.rotate_left(start);
    }
    let mut selected = Vec::with_capacity(max_records.min(records_capacity(&by_owner)));
    let mut selected_per_owner: BTreeMap<A2aEventOwner, usize> = BTreeMap::new();
    loop {
        let mut progressed = false;
        for owner in &owners {
            if selected.len() >= max_records {
                break;
            }
            let owner_count = selected_per_owner.get(owner).copied().unwrap_or_default();
            if owner_count >= max_per_owner {
                continue;
            }
            if let Some(record) = by_owner.get_mut(owner).and_then(VecDeque::pop_front) {
                selected.push(record);
                selected_per_owner.insert(owner.clone(), owner_count + 1);
                *cursor = Some(owner.clone());
                progressed = true;
            }
        }
        if !progressed || selected.len() >= max_records {
            break;
        }
    }
    selected
}

fn records_capacity<T>(records: &BTreeMap<A2aEventOwner, VecDeque<T>>) -> usize {
    records.values().map(VecDeque::len).sum()
}

impl A2aHttpJsonRpcServer {
    /// Legacy/test-only shared-state constructor. Callers inside this crate must not retain or
    /// mutate another clone of `mapper`; production callers use [`Self::new_owned`].
    fn new(
        mapper: Arc<Mutex<A2aMapper>>,
        snapshots: Arc<dyn A2aMapperSnapshotStore>,
        events: Arc<dyn A2aEventStore>,
        authenticator: Arc<dyn A2aHttpAuthenticator>,
        host: Arc<dyn A2aDispatchHost>,
        agent_card: A2aAgentCard,
        config: A2aHttpConfig,
    ) -> ProtocolResult<Self> {
        agent_card.validate()?;
        config.validate()?;
        // Host/provider callbacks are outside the runtime's trust boundary. Install the
        // process-wide, callback-scoped redaction hook before any such future can be polled.
        install_a2a_sanitizing_panic_hook();
        let (live, _) = broadcast::channel(config.live_channel_capacity);
        let (dispatch_sender, dispatch_receiver) = mpsc::channel(config.max_queued_dispatches);
        let (control_sender, control_receiver) =
            mpsc::channel(config.max_queued_control_dispatches);
        let dispatch_global = Arc::new(Semaphore::new(config.max_background_dispatches));
        let control_global = Arc::new(Semaphore::new(config.max_control_dispatches));
        let request_global = Arc::new(Semaphore::new(config.max_concurrency));
        let control_probe_global = Arc::new(Semaphore::new(config.max_control_dispatches));
        let control_exact_probe_global = Arc::new(Semaphore::new(config.max_control_dispatches));
        let control_request_global = Arc::new(Semaphore::new(config.max_control_dispatches));
        let response_write_global = Arc::new(Semaphore::new(config.max_concurrency));
        let control_response_write_global = Arc::new(Semaphore::new(config.max_control_dispatches));
        let preauth_limiter = Arc::new(PreAuthLimiter::new(config.max_preauth_per_ip));
        let control_probe_limiter = Arc::new(ControlProbeLimiter::new(ControlProbeLimits {
            max_active: config.max_control_dispatches,
            max_per_ip: config.max_control_probes_per_ip,
            max_per_owner: config.max_control_probes_per_owner,
            max_per_minute: config.max_control_probes_per_minute,
            max_rate_buckets: config.max_rate_buckets,
            max_waiters: config.max_queued_control_dispatches,
            max_waiters_per_ip: config.max_queued_control_dispatches,
            max_waiters_per_owner: config.max_queued_control_dispatches_per_owner,
        }));
        // This is intentionally a separate, equally bounded lane. It can inspect a small body
        // after the standard per-minute probe cap is exhausted, but only an exact `CancelTask`
        // may leave classification and it is still governed by `control_request_limiter`.
        let control_exact_probe_limiter = Arc::new(ControlProbeLimiter::new(ControlProbeLimits {
            max_active: config.max_control_dispatches,
            max_per_ip: config.max_control_probes_per_ip,
            max_per_owner: config.max_control_probes_per_owner,
            max_per_minute: config.max_control_probes_per_minute,
            max_rate_buckets: config.max_rate_buckets,
            max_waiters: config.max_queued_control_dispatches,
            max_waiters_per_ip: config.max_queued_control_dispatches,
            max_waiters_per_owner: config.max_queued_control_dispatches_per_owner,
        }));
        let control_request_limiter = Arc::new(ControlProbeLimiter::new(ControlProbeLimits {
            max_active: config.max_control_dispatches,
            max_per_ip: config.max_control_requests_per_ip,
            max_per_owner: config.max_control_requests_per_owner,
            max_per_minute: config.max_requests_per_minute,
            max_rate_buckets: config.max_rate_buckets,
            max_waiters: config.max_queued_control_dispatches,
            max_waiters_per_ip: config.max_queued_control_dispatches,
            max_waiters_per_owner: config.max_queued_control_dispatches_per_owner,
        }));
        let stream_quota = Arc::new(StreamQuota::new(
            config.max_streams,
            config.max_streams_per_owner,
        ));
        Ok(Self {
            mapper,
            snapshot_commit: Arc::new(RwLock::new(())),
            snapshot_version: Arc::new(StdMutex::new(None)),
            snapshot_store_initialized: Arc::new(StdAtomicBool::new(false)),
            snapshot_commit_failed: Arc::new(StdAtomicBool::new(false)),
            snapshot_failure: Arc::new(StdMutex::new(None)),
            snapshot_fail_stop: CancellationToken::new(),
            snapshot_commit_tasks: Arc::new(StdMutex::new(SnapshotCommitRegistry::default())),
            serving: Arc::new(StdAtomicBool::new(false)),
            snapshots,
            events,
            authenticator,
            host,
            agent_card,
            config,
            rate: StdMutex::new(BTreeMap::new()),
            preauth_limiter,
            control_probe_limiter,
            control_exact_probe_limiter,
            control_request_limiter,
            stream_quota,
            live,
            dispatch_sender,
            dispatch_receiver: Mutex::new(Some(dispatch_receiver)),
            control_sender,
            control_receiver: Mutex::new(Some(control_receiver)),
            dispatch_state: StdMutex::new(DispatchSchedulerState::default()),
            recovery_attempted: Arc::new(StdMutex::new(BTreeSet::new())),
            cancellation_reconciliations: StdMutex::new(BTreeMap::new()),
            cancellation_reconciliation_notify: Notify::new(),
            running_dispatch_cancellations: StdMutex::new(BTreeMap::new()),
            #[cfg(test)]
            cancellation_commit_after_send_hook: StdMutex::new(None),
            #[cfg(test)]
            cancellation_claim_release_hook: StdMutex::new(None),
            request_global,
            control_probe_global,
            control_exact_probe_global,
            control_request_global,
            response_write_global,
            control_response_write_global,
            dispatch_global,
            control_global,
        })
    }

    pub fn new_owned(
        mapper: A2aMapper,
        snapshots: Arc<dyn A2aMapperSnapshotStore>,
        events: Arc<dyn A2aEventStore>,
        authenticator: Arc<dyn A2aHttpAuthenticator>,
        host: Arc<dyn A2aDispatchHost>,
        agent_card: A2aAgentCard,
        config: A2aHttpConfig,
    ) -> ProtocolResult<Self> {
        Self::new(
            Arc::new(Mutex::new(mapper)),
            snapshots,
            events,
            authenticator,
            host,
            agent_card,
            config,
        )
    }

    pub async fn mapper_snapshot(&self) -> A2aMapper {
        self.mapper.lock().await.clone()
    }

    /// True only while the listener is serving and snapshot state is safe to read.
    pub fn is_ready(&self) -> bool {
        self.serving.load(AtomicOrdering::Acquire)
            && !self.snapshot_commit_failed.load(AtomicOrdering::Acquire)
            && !self.snapshot_fail_stop.is_cancelled()
    }

    fn ensure_snapshot_available(&self) -> ProtocolResult<()> {
        if self.snapshot_commit_failed.load(AtomicOrdering::Acquire)
            || self.snapshot_fail_stop.is_cancelled()
        {
            return Err(self
                .snapshot_failure
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
                .unwrap_or_else(|| {
                    ProtocolError::conflict(
                        "A2A snapshot persistence is fail-stopped after an ambiguous commit",
                    )
                }));
        }
        Ok(())
    }

    fn fail_stop_snapshot(&self, error: ProtocolError) {
        self.snapshot_commit_failed
            .store(true, AtomicOrdering::Release);
        let mut failure = self
            .snapshot_failure
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if failure.is_none() {
            *failure = Some(error);
        }
        drop(failure);
        self.snapshot_fail_stop.cancel();
    }

    fn spawn_snapshot_commit(
        &self,
        future: impl Future<Output = ()> + Send + 'static,
    ) -> ProtocolResult<()> {
        let registry = self.snapshot_commit_tasks.clone();
        let mut state = registry
            .lock()
            .map_err(|_| ProtocolError::conflict("A2A snapshot task registry lock poisoned"))?;
        if !state.accepting {
            return Err(ProtocolError::conflict(
                "A2A snapshot task registry is closed",
            ));
        }
        let task_id = state.next_id;
        state.next_id = state.next_id.checked_add(1).ok_or_else(|| {
            ProtocolError::conflict("A2A snapshot task identifier capacity exhausted")
        })?;
        let weak_registry = Arc::downgrade(&registry);
        let handle = tokio::spawn(async move {
            future.await;
            if let Some(registry) = weak_registry.upgrade() {
                registry
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .handles
                    .remove(&task_id);
            }
        });
        state.handles.insert(task_id, handle);
        Ok(())
    }

    fn close_snapshot_commit_registry(&self) -> ProtocolResult<Vec<JoinHandle<()>>> {
        let mut state = self
            .snapshot_commit_tasks
            .lock()
            .map_err(|_| ProtocolError::conflict("A2A snapshot task registry lock poisoned"))?;
        state.accepting = false;
        Ok(std::mem::take(&mut state.handles).into_values().collect())
    }

    /// Bind the live mapper to the linearizable durable head before the listener becomes ready.
    /// Read-only RPCs otherwise have no commit path on which to discover a stale or mis-restored
    /// mapper and could serve divergent task state until the first later mutation.
    async fn initialize_snapshot_store(&self) -> ProtocolResult<()> {
        self.ensure_snapshot_available()?;
        let _commit_guard = self.snapshot_commit.write().await;
        self.ensure_snapshot_available()?;

        if self
            .snapshot_store_initialized
            .load(AtomicOrdering::Acquire)
        {
            let live_revision = self.mapper.lock().await.revision();
            let cached = self.cached_snapshot_version()?;
            if cached.revision != live_revision {
                let error = ProtocolError::conflict(
                    "A2A live mapper revision diverged from its snapshot version",
                );
                self.fail_stop_snapshot(error.clone());
                return Err(error);
            }
            let observed = self.snapshots.lookup_snapshot_version().await?;
            if observed.as_ref() != Some(&cached) {
                let error = ProtocolError::conflict(
                    "A2A durable snapshot head diverged from the live mapper",
                );
                self.fail_stop_snapshot(error.clone());
                return Err(error);
            }
            return Ok(());
        }

        let live = self.mapper.lock().await.clone();
        let live_snapshot = A2aSerializedMapperSnapshot::from_mapper(&live)?;
        let version = match self.snapshots.load_serialized_snapshot().await {
            Ok(Some(stored_snapshot)) => {
                let stored_version = stored_snapshot.version();
                let stored_mapper = stored_snapshot.decode().inspect_err(|error| {
                    self.fail_stop_snapshot(error.clone());
                })?;
                if stored_version.revision != live.revision() || stored_mapper != live {
                    let error = ProtocolError::conflict(
                        "A2A durable snapshot content diverged from the restored live mapper",
                    );
                    self.fail_stop_snapshot(error.clone());
                    return Err(error);
                }
                stored_version
            }
            Ok(None) => {
                let version = live_snapshot.version();
                match persist_snapshot_with_exact_probe(
                    &self.snapshots,
                    None,
                    live_snapshot.bind_expected(None),
                )
                .await
                {
                    Ok(()) => version,
                    Err(A2aSnapshotStoreError::DefiniteNotApplied(error)) => return Err(error),
                    Err(A2aSnapshotStoreError::OutcomeUnknown(error)) => {
                        self.fail_stop_snapshot(error.clone());
                        return Err(error);
                    }
                }
            }
            Err(error) => {
                self.fail_stop_snapshot(error.clone());
                return Err(error);
            }
        };

        *self
            .snapshot_version
            .lock()
            .map_err(|_| ProtocolError::conflict("A2A snapshot version lock poisoned"))? =
            Some(version);
        self.snapshot_store_initialized
            .store(true, AtomicOrdering::Release);
        Ok(())
    }

    fn cached_snapshot_version(&self) -> ProtocolResult<A2aSnapshotVersion> {
        self.snapshot_version
            .lock()
            .map_err(|_| ProtocolError::conflict("A2A snapshot version lock poisoned"))?
            .clone()
            .ok_or_else(|| ProtocolError::conflict("A2A snapshot version is unavailable"))
    }

    /// Give an externally served read a linearization point against the shared durable store.
    /// The remote lookup intentionally runs before the local read barrier so an unavailable store
    /// cannot block mapper writes or priority cancellation. If a local commit advances while the
    /// lookup is in flight, retry against its new cached version before exposing mapper state.
    async fn acquire_current_snapshot_read(&self) -> ProtocolResult<OwnedRwLockReadGuard<()>> {
        if !self
            .snapshot_store_initialized
            .load(AtomicOrdering::Acquire)
        {
            self.initialize_snapshot_store().await?;
        }
        loop {
            self.ensure_snapshot_available()?;
            let before = self.cached_snapshot_version()?;
            let observed = self.snapshots.lookup_snapshot_version().await?;
            let guard = self.snapshot_commit.clone().read_owned().await;
            self.ensure_snapshot_available()?;
            let current = self.cached_snapshot_version()?;
            if current != before {
                drop(guard);
                continue;
            }
            if observed.as_ref() != Some(&current) {
                let error = ProtocolError::conflict(
                    "A2A durable snapshot head diverged from the live mapper",
                );
                self.fail_stop_snapshot(error.clone());
                return Err(error);
            }
            return Ok(guard);
        }
    }

    async fn persist_mapper_mutation<T>(
        &self,
        mutation: impl FnOnce(&mut A2aMapper) -> ProtocolResult<T>,
    ) -> ProtocolResult<T> {
        self.persist_mapper_mutation_with_post_commit(mutation, None)
            .await
    }

    async fn persist_mapper_mutation_with_post_commit<T>(
        &self,
        mutation: impl FnOnce(&mut A2aMapper) -> ProtocolResult<T>,
        post_commit: Option<Box<dyn FnOnce() -> ProtocolResult<()> + Send>>,
    ) -> ProtocolResult<T> {
        self.ensure_snapshot_available()?;
        // The owned guard moves into an independent commit task. Once the candidate is prepared,
        // dropping the request future cannot cancel a store-applied commit before live install.
        let commit_guard = self.snapshot_commit.clone().write_owned().await;
        self.ensure_snapshot_available()?;
        let mut candidate = {
            let live = self.mapper.lock().await;
            live.clone()
        };
        let expected_revision = candidate.revision();
        let initialize_store = !self
            .snapshot_store_initialized
            .load(AtomicOrdering::Acquire);
        let (base, expected) = if initialize_store {
            let live_snapshot = A2aSerializedMapperSnapshot::from_mapper(&candidate)?;
            let stored_snapshot = match self.snapshots.load_serialized_snapshot().await {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    self.fail_stop_snapshot(error.clone());
                    return Err(error);
                }
            };
            if let Some(stored_snapshot) = stored_snapshot {
                let stored_version = stored_snapshot.version();
                if stored_version.revision != expected_revision {
                    let error = ProtocolError::conflict(
                        "A2A durable snapshot revision diverged from the restored live mapper",
                    );
                    self.fail_stop_snapshot(error.clone());
                    return Err(error);
                }
                let stored_mapper = match stored_snapshot.decode() {
                    Ok(mapper) => mapper,
                    Err(error) => {
                        self.fail_stop_snapshot(error.clone());
                        return Err(error);
                    }
                };
                if stored_mapper != candidate {
                    let error = ProtocolError::conflict(
                        "A2A durable snapshot content diverged from the restored live mapper",
                    );
                    self.fail_stop_snapshot(error.clone());
                    return Err(error);
                }
                (None, stored_version)
            } else {
                let version = live_snapshot.version();
                (Some(live_snapshot), version)
            }
        } else {
            let version = self
                .snapshot_version
                .lock()
                .map_err(|_| ProtocolError::conflict("A2A snapshot version lock poisoned"))?
                .clone()
                .ok_or_else(|| ProtocolError::conflict("A2A snapshot version is unavailable"))?;
            (None, version)
        };
        {
            let mut cached = self
                .snapshot_version
                .lock()
                .map_err(|_| ProtocolError::conflict("A2A snapshot version lock poisoned"))?;
            if cached.as_ref().is_some_and(|cached| cached != &expected) {
                let error = ProtocolError::conflict(
                    "A2A live mapper diverged from its cached snapshot digest",
                );
                self.fail_stop_snapshot(error.clone());
                return Err(error);
            }
            *cached = Some(expected.clone());
        }
        if initialize_store && base.is_none() {
            // Publish the cached version before the Release flag. Concurrent first-use readers
            // that observe initialization must never see an unavailable snapshot version.
            self.snapshot_store_initialized
                .store(true, AtomicOrdering::Release);
        }
        if expected.revision != expected_revision {
            let error = ProtocolError::conflict(
                "A2A live mapper revision diverged from its snapshot version",
            );
            self.fail_stop_snapshot(error.clone());
            return Err(error);
        }
        let output = mutation(&mut candidate)?;
        if candidate.revision() == expected_revision {
            if let Some(base) = base {
                match persist_snapshot_with_exact_probe(
                    &self.snapshots,
                    None,
                    base.bind_expected(None),
                )
                .await
                {
                    Ok(()) => self
                        .snapshot_store_initialized
                        .store(true, AtomicOrdering::Release),
                    Err(A2aSnapshotStoreError::DefiniteNotApplied(error)) => return Err(error),
                    Err(A2aSnapshotStoreError::OutcomeUnknown(error)) => {
                        self.fail_stop_snapshot(error.clone());
                        return Err(error);
                    }
                }
            }
            // No-op retries still need a shared-store linearization point. Release the writer
            // before remote I/O so a slow lookup cannot freeze cancellation or other mutations.
            drop(commit_guard);
            let _snapshot_read = self.acquire_current_snapshot_read().await?;
            if self.cached_snapshot_version()? != expected {
                return Err(ProtocolError::conflict(
                    "A2A mapper changed while validating a no-op mutation",
                ));
            }
            if let Some(post_commit) = post_commit {
                post_commit()?;
            }
            return Ok(output);
        }
        validate_new_pending_event_wire_bytes(
            &candidate,
            expected.revision,
            self.config.retention().max_event_bytes,
        )?;
        let serialized =
            A2aSerializedMapperSnapshot::from_mapper(&candidate)?.bind_expected(Some(&expected));
        let mapper = self.mapper.clone();
        let snapshots = self.snapshots.clone();
        let snapshot_version = self.snapshot_version.clone();
        let store_initialized = self.snapshot_store_initialized.clone();
        let commit_failed = self.snapshot_commit_failed.clone();
        let snapshot_failure = self.snapshot_failure.clone();
        let snapshot_fail_stop = self.snapshot_fail_stop.clone();
        let (acknowledge, acknowledged) = oneshot::channel();
        self.spawn_snapshot_commit(async move {
            let result: Result<(), A2aSnapshotStoreError> = async {
                if let Some(base) = base {
                    persist_snapshot_with_exact_probe(&snapshots, None, base.bind_expected(None))
                        .await?;
                    store_initialized.store(true, AtomicOrdering::Release);
                }
                persist_snapshot_with_exact_probe(
                    &snapshots,
                    Some(expected.clone()),
                    serialized.clone(),
                )
                .await?;
                let mut live = mapper.lock().await;
                if live.revision() != expected_revision {
                    return Err(A2aSnapshotStoreError::unknown(ProtocolError::conflict(
                        "A2A live mapper changed during snapshot commit",
                    )));
                }
                *live = candidate;
                *snapshot_version.lock().map_err(|_| {
                    A2aSnapshotStoreError::unknown(ProtocolError::conflict(
                        "A2A snapshot version lock poisoned",
                    ))
                })? = Some(serialized.version());
                if let Some(post_commit) = post_commit {
                    post_commit().map_err(A2aSnapshotStoreError::definite)?;
                }
                Ok(())
            }
            .await;
            if let Err(A2aSnapshotStoreError::OutcomeUnknown(error)) = &result {
                commit_failed.store(true, AtomicOrdering::Release);
                let mut failure = snapshot_failure
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if failure.is_none() {
                    *failure = Some(error.clone());
                }
                drop(failure);
                snapshot_fail_stop.cancel();
            }
            let _ = acknowledge.send(result.map_err(A2aSnapshotStoreError::into_protocol_error));
            drop(commit_guard);
        })?;
        let result = acknowledged.await.map_err(|_| {
            ProtocolError::conflict("A2A snapshot commit acknowledgement task stopped")
        });
        let result = match result {
            Ok(result) => result,
            Err(error) => {
                self.fail_stop_snapshot(error.clone());
                return Err(error);
            }
        };
        result?;
        Ok(output)
    }

    async fn mark_dispatch_running_with_commit(
        self: &Arc<Self>,
        dispatch_id: &str,
        commit: oneshot::Sender<DispatchCommit>,
        recovery_claim: RecoveryAttemptClaim,
    ) -> ProtocolResult<()> {
        let expected_attempt = Arc::new(StdMutex::new(None));
        let expected_for_mutation = expected_attempt.clone();
        let expected_for_commit = expected_attempt;
        let handoff = Arc::new(StdMutex::new(Some((commit, recovery_claim))));
        let handoff_for_commit = handoff;
        let dispatch_id = dispatch_id.to_owned();
        self.persist_mapper_mutation_with_post_commit(
            move |candidate| {
                candidate.mark_dispatch_running(&dispatch_id)?;
                let attempt = candidate
                    .dispatch_outbox()
                    .get(&dispatch_id)
                    .map(|record| record.attempts)
                    .ok_or_else(|| {
                        ProtocolError::conflict(
                            "A2A running dispatch disappeared before scheduler handoff",
                        )
                    })?;
                *expected_for_mutation
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(attempt);
                Ok(())
            },
            Some(Box::new(move || {
                let expected_dispatch_attempt = expected_for_commit
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take()
                    .ok_or_else(|| {
                        ProtocolError::conflict(
                            "A2A running dispatch lost its committed generation",
                        )
                    })?;
                let (commit, recovery_claim) = handoff_for_commit
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take()
                    .ok_or_else(|| {
                        ProtocolError::conflict(
                            "A2A running dispatch scheduler handoff was already consumed",
                        )
                    })?;
                commit
                    .send(DispatchCommit {
                        expected_dispatch_attempt: Some(expected_dispatch_attempt),
                        expected_cancellation_attempt: None,
                        host_already_fenced: false,
                        dispatch_recovery_claim: Some(recovery_claim),
                    })
                    .map_err(|_| {
                        ProtocolError::conflict(
                            "A2A running dispatch scheduler commit receiver disappeared",
                        )
                    })?;
                Ok(())
            })),
        )
        .await
    }

    async fn mark_dispatch_reconcile_pending(
        &self,
        dispatch_id: &str,
        detail: &str,
    ) -> ProtocolResult<()> {
        self.persist_mapper_mutation(|candidate| {
            candidate.mark_dispatch_reconcile_pending(dispatch_id, detail)
        })
        .await
    }

    async fn mark_dispatch_settled(&self, dispatch_id: &str) -> ProtocolResult<()> {
        self.persist_mapper_mutation(|candidate| candidate.mark_dispatch_settled(dispatch_id))
            .await
    }

    async fn mark_cancellation_running(&self, cancellation_id: &str) -> ProtocolResult<()> {
        self.persist_mapper_mutation(|candidate| {
            candidate.mark_cancellation_running(cancellation_id)
        })
        .await
    }

    async fn mark_cancellation_reconcile_pending(
        &self,
        cancellation_id: &str,
        detail: &str,
    ) -> ProtocolResult<()> {
        self.persist_mapper_mutation(|candidate| {
            candidate.mark_cancellation_reconcile_pending(cancellation_id, detail)
        })
        .await
    }

    async fn mark_cancellation_settled(&self, cancellation_id: &str) -> ProtocolResult<()> {
        self.persist_mapper_mutation(|candidate| {
            candidate.mark_cancellation_settled(cancellation_id)
        })
        .await
    }

    async fn acknowledge_cancellation_fence(
        &self,
        cancellation_id: &str,
        expected_attempt: u32,
        task_id: &str,
        detail: &str,
    ) -> ProtocolResult<()> {
        self.persist_cancellation_fence_with_post_commit(
            cancellation_id,
            expected_attempt,
            task_id,
            detail,
            None,
        )
        .await?;
        self.flush_pending_events_for_task(task_id).await?;
        Ok(())
    }

    async fn persist_cancellation_fence_with_post_commit(
        &self,
        cancellation_id: &str,
        expected_attempt: u32,
        task_id: &str,
        detail: &str,
        post_commit: Option<Box<dyn FnOnce() -> ProtocolResult<()> + Send>>,
    ) -> ProtocolResult<A2aTaskRecord> {
        self.persist_mapper_mutation_with_post_commit(
            |candidate| {
                candidate.acknowledge_cancellation(
                    cancellation_id,
                    expected_attempt,
                    Some(detail.to_owned()),
                )?;
                candidate.tasks().get(task_id).cloned().ok_or_else(|| {
                    ProtocolError::conflict("A2A cancellation task disappeared after its fence")
                })
            },
            post_commit,
        )
        .await
    }

    async fn mark_task_dispatches_reconcile_pending(&self, task_id: &str) -> ProtocolResult<()> {
        let dispatch_ids: Vec<String> = self
            .mapper
            .lock()
            .await
            .dispatch_outbox()
            .values()
            .filter(|record| {
                record.task_id == task_id
                    && matches!(
                        record.state,
                        A2aDispatchOutboxState::Queued | A2aDispatchOutboxState::Running
                    )
            })
            .map(|record| record.dispatch_id.clone())
            .collect();
        if dispatch_ids.is_empty() {
            return Ok(());
        }
        self.persist_mapper_mutation(|candidate| {
            for dispatch_id in &dispatch_ids {
                candidate.mark_dispatch_reconcile_pending(
                    dispatch_id,
                    "cancellation outcome is unknown",
                )?;
            }
            Ok(())
        })
        .await
    }

    fn signal_running_dispatch_cancellation(
        &self,
        owner: &A2aEventOwner,
        task_id: &str,
    ) -> ProtocolResult<bool> {
        let key = DispatchRuntimeKey {
            owner: owner.clone(),
            task_id: task_id.to_owned(),
        };
        let token = self
            .running_dispatch_cancellations
            .lock()
            .map_err(|_| ProtocolError::conflict("A2A runtime cancellation lock poisoned"))?
            .get(&key)
            .cloned();
        if let Some(token) = token {
            token.cancel();
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn register_running_dispatch_cancellation(
        &self,
        owner: &A2aEventOwner,
        task_id: &str,
        token: CancellationToken,
    ) -> ProtocolResult<DispatchRuntimeKey> {
        let key = DispatchRuntimeKey {
            owner: owner.clone(),
            task_id: task_id.to_owned(),
        };
        let mut running = self
            .running_dispatch_cancellations
            .lock()
            .map_err(|_| ProtocolError::conflict("A2A runtime cancellation lock poisoned"))?;
        if running.contains_key(&key) {
            return Err(ProtocolError::conflict(
                "A2A task has more than one running message dispatch",
            ));
        }
        running.insert(key.clone(), token);
        Ok(key)
    }

    fn unregister_running_dispatch_cancellation(&self, key: &DispatchRuntimeKey) {
        self.running_dispatch_cancellations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(key);
    }

    async fn deliver_pending_event(
        &self,
        logical_event_id: &str,
    ) -> ProtocolResult<A2aPersistedEvent> {
        let snapshot_read = self.acquire_current_snapshot_read().await?;
        let intent = self
            .mapper
            .lock()
            .await
            .pending_event_intents()
            .get(logical_event_id)
            .cloned()
            .ok_or_else(|| ProtocolError::not_found("A2A event intent is not registered"))?;
        drop(snapshot_read);
        if intent.state == A2aPendingEventState::Quarantined {
            return Err(ProtocolError::invalid_transition(
                "quarantined A2A event intent cannot be delivered",
            ));
        }
        if intent
            .next_attempt_at_unix_ms
            .is_some_and(|next_attempt| next_attempt > current_unix_millis())
        {
            return Err(ProtocolError::conflict(
                "A2A event publication retry backoff is active",
            ));
        }
        let owner = A2aEventOwner {
            subject: intent.owner_subject.clone(),
            tenant_id: intent.owner_tenant_id.clone(),
        };
        let response = match intent.kind {
            A2aPendingEventKind::TaskCreated
            | A2aPendingEventKind::MessageAccepted
            | A2aPendingEventKind::RecoveredSnapshot => A2aStreamResponse::task(&intent.task),
            A2aPendingEventKind::StatusChanged | A2aPendingEventKind::CancellationRequested => {
                A2aStreamResponse::status(&intent.task)
            }
            A2aPendingEventKind::DirectMessageResponse => A2aStreamResponse::message(
                intent
                    .response_message
                    .as_ref()
                    .ok_or_else(|| ProtocolError::conflict("A2A direct event lost its message"))?,
            ),
        };
        let appended = match self
            .events
            .append(
                logical_event_id,
                &owner,
                &intent.task_id,
                &response,
                self.config.retention(),
            )
            .await
        {
            Ok(appended) => appended,
            Err(A2aEventAppendError::Retryable) => {
                let failed_at_unix_ms = current_unix_millis();
                let _ = self
                    .persist_mapper_mutation(|candidate| {
                        candidate.mark_event_reconcile_pending(
                            logical_event_id,
                            "event store temporarily unavailable",
                            failed_at_unix_ms,
                        )
                    })
                    .await;
                return Err(A2aEventAppendError::retryable().into_protocol_error());
            }
            Err(A2aEventAppendError::Permanent(error)) => {
                let _ = self
                    .persist_mapper_mutation(|candidate| {
                        candidate.mark_event_quarantined(
                            logical_event_id,
                            A2aEventQuarantineReason::DeterministicPoison,
                        )
                    })
                    .await;
                return Err(error);
            }
        };
        let event = appended.into_event();
        if let Err(error) = validate_persisted_event_binding(
            &event,
            Some(logical_event_id),
            &owner,
            &intent.task_id,
            &intent.context_id,
            Some(&response),
        ) {
            let _ = self
                .persist_mapper_mutation(|candidate| {
                    candidate.mark_event_quarantined(
                        logical_event_id,
                        A2aEventQuarantineReason::DeterministicPoison,
                    )
                })
                .await;
            return Err(error);
        }
        let became_settled = self
            .persist_mapper_mutation(|candidate| {
                let was_settled = candidate
                    .pending_event_intents()
                    .get(logical_event_id)
                    .is_some_and(|pending| pending.state == A2aPendingEventState::Settled);
                candidate.mark_event_settled(logical_event_id)?;
                Ok(!was_settled)
            })
            .await?;
        // An append may have succeeded while the mapper snapshot settlement failed. Recovery then
        // observes `Existing`, but the event still has not reached already-open live subscribers.
        // Broadcast exactly once when the durable intent first becomes settled, independently of
        // whether this append inserted the event or recovered an existing append.
        if became_settled {
            let _ = self.live.send(LiveEvent(event.clone()));
        }
        Ok(event)
    }

    async fn flush_pending_events_for_task(
        &self,
        task_id: &str,
    ) -> ProtocolResult<Option<A2aPersistedEvent>> {
        let pending: Vec<A2aPendingEventIntent> = self
            .mapper
            .lock()
            .await
            .pending_events()
            .into_iter()
            .filter(|intent| intent.task_id == task_id)
            .collect();
        let mut latest = None;
        for intent in pending {
            latest = Some(self.deliver_pending_event(&intent.event_id).await?);
        }
        Ok(latest)
    }

    async fn recover_one_pending_event(
        self: Arc<Self>,
        intent: A2aPendingEventIntent,
        cancellation: CancellationToken,
        recovery_deadline: Instant,
    ) -> usize {
        let event_id = intent.event_id.clone();
        let delivery = self.deliver_pending_event(&event_id);
        tokio::pin!(delivery);
        tokio::select! {
            () = cancellation.cancelled() => 0,
            result = tokio::time::timeout_at(recovery_deadline, &mut delivery) => {
                match result {
                    Ok(Ok(_)) => 1,
                    Ok(Err(_)) => 0,
                    Err(_) => {
                        // A hung backend is a transient publication failure. Persisted exponential
                        // backoff prevents hot-looping without dead-lettering an accepted event.
                        let failed_at_unix_ms = current_unix_millis();
                        let _ = self
                            .persist_mapper_mutation(|candidate| {
                                candidate.mark_event_reconcile_pending(
                                    &event_id,
                                    "event delivery timed out",
                                    failed_at_unix_ms,
                                )
                            })
                            .await;
                        0
                    }
                }
            }
        }
    }

    async fn recover_pending_events(
        self: &Arc<Self>,
        cancellation: &CancellationToken,
        recovery_deadline: Instant,
        cursor: &mut Option<(String, Option<String>)>,
    ) -> usize {
        let pending = self.mapper.lock().await.pending_events_due_fair_batch(
            current_unix_millis(),
            self.config.max_recovery_concurrency,
            self.config.max_recovery_concurrency_per_owner,
            cursor,
        );
        let mut recoveries = JoinSet::new();
        let now = Instant::now();
        let item_deadline = now + recovery_deadline.saturating_duration_since(now) / 2;
        for intent in pending {
            recoveries.spawn(self.clone().recover_one_pending_event(
                intent,
                cancellation.clone(),
                item_deadline,
            ));
        }
        let collect = async {
            let mut delivered = 0_usize;
            while let Some(result) = recoveries.join_next().await {
                if let Ok(count) = result {
                    delivered = delivered.saturating_add(count);
                }
            }
            delivered
        };
        tokio::time::timeout_at(recovery_deadline, collect)
            .await
            .unwrap_or_default()
    }

    fn recovery_was_attempted(&self, durable_id: &str) -> ProtocolResult<bool> {
        Ok(self
            .recovery_attempted
            .lock()
            .map_err(|_| ProtocolError::conflict("A2A recovery lock poisoned"))?
            .contains(durable_id))
    }

    fn quarantine_recovery(&self, durable_id: &str) {
        self.recovery_attempted
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(durable_id.to_owned());
    }

    fn claim_recovery_attempt(&self, durable_id: &str) -> Option<RecoveryAttemptClaim> {
        let mut attempted = self
            .recovery_attempted
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !attempted.insert(durable_id.to_owned()) {
            return None;
        }
        Some(RecoveryAttemptClaim {
            attempted: self.recovery_attempted.clone(),
            durable_id: durable_id.to_owned(),
            release_on_drop: true,
        })
    }

    fn release_recovery_attempt(&self, durable_id: &str) {
        self.recovery_attempted
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(durable_id);
    }

    async fn reconcile_unknown_dispatch(
        self: &Arc<Self>,
        record: &A2aDispatchOutboxRecord,
        cancellation: &CancellationToken,
        recovery_deadline: Instant,
    ) -> A2aUnknownDispatchDecision {
        let deadline = (Instant::now() + self.config.dispatch_ack_timeout).min(recovery_deadline);
        let mut reconciliation = Box::pin(A2aUntrustedCallback::new(async {
            self.host.reconcile_unknown(self.clone(), record).await
        }));
        let decision = tokio::select! {
            () = cancellation.cancelled() => A2aUnknownDispatchDecision::ReconcileRequired,
            result = tokio::time::timeout_at(deadline, &mut reconciliation) => match result {
                Ok(Ok(Ok(decision))) => decision,
                Ok(Ok(Err(_))) | Ok(Err(_)) | Err(_) => {
                    A2aUnknownDispatchDecision::ReconcileRequired
                }
            },
        };
        if reconciliation.as_mut().get_mut().finish_drop().is_err() {
            return A2aUnknownDispatchDecision::ReconcileRequired;
        }
        decision
    }

    async fn reconcile_unknown_cancellation(
        self: &Arc<Self>,
        record: &A2aCancellationOutboxRecord,
        cancellation: &CancellationToken,
        recovery_deadline: Instant,
    ) -> A2aUnknownDispatchDecision {
        let deadline = (Instant::now() + self.config.dispatch_ack_timeout).min(recovery_deadline);
        let mut reconciliation = Box::pin(A2aUntrustedCallback::new(async {
            self.host
                .reconcile_unknown_cancel(self.clone(), record)
                .await
        }));
        let decision = tokio::select! {
            () = cancellation.cancelled() => A2aUnknownDispatchDecision::ReconcileRequired,
            result = tokio::time::timeout_at(deadline, &mut reconciliation) => match result {
                Ok(Ok(Ok(decision))) => decision,
                Ok(Ok(Err(_))) | Ok(Err(_)) | Err(_) => {
                    A2aUnknownDispatchDecision::ReconcileRequired
                }
            },
        };
        if reconciliation.as_mut().get_mut().finish_drop().is_err() {
            return A2aUnknownDispatchDecision::ReconcileRequired;
        }
        decision
    }

    /// Share one host reconciliation probe between background recovery and concurrent live
    /// retries for the same durable cancellation generation. A fail-closed result is cached for
    /// that attempt; incrementing the durable attempt is the only operation that opens a new
    /// generation and therefore permits another probe.
    async fn reconcile_unknown_cancellation_once(
        self: &Arc<Self>,
        record: &A2aCancellationOutboxRecord,
        cancellation: &CancellationToken,
        recovery_deadline: Instant,
    ) -> A2aUnknownDispatchDecision {
        loop {
            let notified = self.cancellation_reconciliation_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let owner = {
                let Ok(mut reconciliations) = self.cancellation_reconciliations.lock() else {
                    return A2aUnknownDispatchDecision::ReconcileRequired;
                };
                match reconciliations.get(&record.cancellation_id).copied() {
                    Some(CancellationReconciliationCache::Complete { attempts, decision })
                        if attempts == record.attempts =>
                    {
                        return decision;
                    }
                    Some(CancellationReconciliationCache::InFlight { attempts })
                        if attempts == record.attempts =>
                    {
                        false
                    }
                    _ => {
                        reconciliations.insert(
                            record.cancellation_id.clone(),
                            CancellationReconciliationCache::InFlight {
                                attempts: record.attempts,
                            },
                        );
                        true
                    }
                }
            };
            if !owner {
                let woke = tokio::select! {
                    () = cancellation.cancelled() => false,
                    result = tokio::time::timeout_at(recovery_deadline, &mut notified) => {
                        result.is_ok()
                    }
                };
                if !woke {
                    return A2aUnknownDispatchDecision::ReconcileRequired;
                }
                continue;
            }

            // The reconciliation future is owned by the surrounding request/recovery task. If
            // that task is aborted by an outer timeout, leaving `InFlight` behind would poison
            // this cancellation generation until process restart. The guard releases only the
            // exact generation it owns and wakes waiters on every drop path.
            let owner_guard = CancellationReconciliationOwnerGuard {
                reconciliations: &self.cancellation_reconciliations,
                notify: &self.cancellation_reconciliation_notify,
                cancellation_id: record.cancellation_id.clone(),
                attempts: record.attempts,
                completed: false,
            };
            let decision = self
                .reconcile_unknown_cancellation(record, cancellation, recovery_deadline)
                .await;
            owner_guard.complete(decision);
            return decision;
        }
    }

    async fn recover_one_pending_cancellation(
        self: Arc<Self>,
        record: A2aCancellationOutboxRecord,
        cancellation: CancellationToken,
        recovery_deadline: Instant,
    ) -> usize {
        if cancellation.is_cancelled()
            || Instant::now() >= recovery_deadline
            || self
                .recovery_was_attempted(&record.cancellation_id)
                .unwrap_or(true)
        {
            return 0;
        }
        let Some(principal) = record.envelope.principal.clone() else {
            self.quarantine_recovery(&record.cancellation_id);
            return 0;
        };
        if principal.subject != record.owner_subject
            || principal.tenant_id != record.owner_tenant_id
        {
            self.quarantine_recovery(&record.cancellation_id);
            return 0;
        }
        let unknown = matches!(
            record.state,
            A2aCancellationOutboxState::Running | A2aCancellationOutboxState::ReconcilePending
        );
        if unknown {
            // Claim the probe before calling untrusted host code. A panicking hook is quarantined
            // for this server generation instead of being retried in a tight loop.
            self.quarantine_recovery(&record.cancellation_id);
            let decision = self
                .reconcile_unknown_cancellation_once(&record, &cancellation, recovery_deadline)
                .await;
            match decision {
                A2aUnknownDispatchDecision::AlreadyStopped => {
                    let settled = self
                        .acknowledge_cancellation_fence(
                            &record.cancellation_id,
                            record.attempts,
                            &record.task_id,
                            "host reconciled the previously stopped cancellation fence",
                        )
                        .await;
                    if settled.is_err() {
                        self.release_recovery_attempt(&record.cancellation_id);
                        return 0;
                    }
                    return 1;
                }
                A2aUnknownDispatchDecision::SafeToRetry => {
                    self.release_recovery_attempt(&record.cancellation_id);
                }
                A2aUnknownDispatchDecision::ReconcileRequired => {
                    if record.state == A2aCancellationOutboxState::Running {
                        let _ = self
                            .mark_cancellation_reconcile_pending(
                                &record.cancellation_id,
                                "unknown cancellation outcome requires host reconciliation",
                            )
                            .await;
                    }
                    return 0;
                }
            }
        }
        // An exact AlreadyStopped proof above may settle even an exhausted generation. Only a
        // retry that could repeat the external control effect is subject to the attempt cap.
        if record.attempts >= A2A_MAX_CANCELLATION_ATTEMPTS {
            if record.state == A2aCancellationOutboxState::Running {
                let _ = self
                    .mark_cancellation_reconcile_pending(
                        &record.cancellation_id,
                        "cancellation attempt limit is exhausted",
                    )
                    .await;
            }
            self.quarantine_recovery(&record.cancellation_id);
            return 0;
        }
        let reconstructed = self
            .mapper
            .lock()
            .await
            .reconstruct_cancel(&record.cancellation_id, &principal);
        let Ok((envelope, action)) = reconstructed else {
            self.quarantine_recovery(&record.cancellation_id);
            return 0;
        };
        let owner = A2aEventOwner {
            subject: record.owner_subject.clone(),
            tenant_id: record.owner_tenant_id.clone(),
        };
        if self
            .signal_running_dispatch_cancellation(&owner, &record.task_id)
            .is_err()
        {
            self.quarantine_recovery(&record.cancellation_id);
            return 0;
        }
        let job = A2aDispatchJob {
            durable_dispatch_id: None,
            durable_cancellation_id: Some(record.cancellation_id.clone()),
            lane: DispatchLane::Cancellation,
            mode: A2aExecutionMode::Blocking,
            envelope,
            action,
        };
        let mut acceptance = match self.reserve_dispatch(
            job,
            DispatchReservation {
                owner,
                task_id: record.task_id.clone(),
                message_id: record.cancellation_id.clone(),
                run_id: record.run_id.clone(),
                lane: DispatchLane::Cancellation,
                delay_start: false,
                allow_new: true,
            },
        ) {
            Ok(acceptance) => acceptance,
            Err(_) => {
                if unknown {
                    self.release_recovery_attempt(&record.cancellation_id);
                }
                return 0;
            }
        };
        let Some(acceptance_ref) = acceptance.as_mut() else {
            return 0;
        };
        if !acceptance_ref.newly_reserved {
            return 0;
        }
        if record.state == A2aCancellationOutboxState::Running
            && self
                .mark_cancellation_reconcile_pending(
                    &record.cancellation_id,
                    "recovering cancellation control with unknown outcome",
                )
                .await
                .is_err()
        {
            self.release_recovery_attempt(&record.cancellation_id);
            return 0;
        }
        if self
            .mark_cancellation_running(&record.cancellation_id)
            .await
            .is_err()
        {
            self.release_recovery_attempt(&record.cancellation_id);
            return 0;
        }
        let expected_attempt = self
            .mapper
            .lock()
            .await
            .cancellation_for_task(&record.task_id, &principal)
            .map(|record| record.attempts);
        let Some(expected_attempt) = expected_attempt else {
            self.release_recovery_attempt(&record.cancellation_id);
            return 0;
        };
        if let Some(commit) = acceptance_ref.commit.take() {
            // Publish the recovery claim before waking the scheduled task. On a multi-threaded
            // runtime the host may fail and complete immediately after `send`; inserting the
            // claim afterwards could resurrect a stale claim after completion released it.
            self.quarantine_recovery(&record.cancellation_id);
            if commit
                .send(DispatchCommit {
                    expected_dispatch_attempt: None,
                    expected_cancellation_attempt: Some(expected_attempt),
                    host_already_fenced: false,
                    dispatch_recovery_claim: None,
                })
                .is_err()
            {
                self.release_recovery_attempt(&record.cancellation_id);
                return 0;
            }
        } else {
            self.release_recovery_attempt(&record.cancellation_id);
            return 0;
        }
        1
    }

    async fn recover_pending_cancellations(
        self: &Arc<Self>,
        cancellation: &CancellationToken,
        recovery_deadline: Instant,
        cursor: &mut Option<A2aEventOwner>,
    ) -> usize {
        let pending: Vec<A2aCancellationOutboxRecord> = self
            .mapper
            .lock()
            .await
            .pending_cancellations()
            .into_iter()
            .filter(|record| {
                !self
                    .recovery_was_attempted(&record.cancellation_id)
                    .unwrap_or(true)
            })
            .collect();
        let batch = fair_recovery_batch(
            pending,
            cursor,
            self.config.max_recovery_concurrency,
            self.config.max_recovery_concurrency_per_owner,
            |record| A2aEventOwner {
                subject: record.owner_subject.clone(),
                tenant_id: record.owner_tenant_id.clone(),
            },
        );
        let mut recoveries = JoinSet::new();
        for record in batch {
            recoveries.spawn(self.clone().recover_one_pending_cancellation(
                record,
                cancellation.clone(),
                recovery_deadline,
            ));
        }
        let collect = async {
            let mut scheduled = 0_usize;
            while let Some(result) = recoveries.join_next().await {
                if let Ok(count) = result {
                    scheduled = scheduled.saturating_add(count);
                }
            }
            scheduled
        };
        tokio::time::timeout_at(recovery_deadline, collect)
            .await
            .unwrap_or_default()
    }

    async fn recover_one_pending_dispatch(
        self: Arc<Self>,
        record: A2aDispatchOutboxRecord,
        cancellation: CancellationToken,
        recovery_deadline: Instant,
    ) -> usize {
        if cancellation.is_cancelled() || Instant::now() >= recovery_deadline {
            return 0;
        }
        let Some(recovery_claim) = self.claim_recovery_attempt(&record.dispatch_id) else {
            return 0;
        };
        if record.attempts >= A2A_MAX_DISPATCH_ATTEMPTS {
            if record.state == A2aDispatchOutboxState::Running {
                let _ = self
                    .mark_dispatch_reconcile_pending(
                        &record.dispatch_id,
                        "dispatch attempt limit is exhausted",
                    )
                    .await;
            }
            recovery_claim.quarantine();
            return 0;
        }
        let Some(principal) = record.envelope.principal.clone() else {
            recovery_claim.quarantine();
            return 0;
        };
        if principal.subject != record.owner_subject
            || principal.tenant_id != record.owner_tenant_id
        {
            recovery_claim.quarantine();
            return 0;
        }
        let unknown = matches!(
            record.state,
            A2aDispatchOutboxState::Running | A2aDispatchOutboxState::ReconcilePending
        );
        if unknown {
            let decision = self
                .reconcile_unknown_dispatch(&record, &cancellation, recovery_deadline)
                .await;
            if decision != A2aUnknownDispatchDecision::SafeToRetry {
                if record.state == A2aDispatchOutboxState::Running
                    && self
                        .mark_dispatch_reconcile_pending(
                            &record.dispatch_id,
                            "unknown external outcome requires host reconciliation",
                        )
                        .await
                        .is_err()
                {
                    return 0;
                }
                recovery_claim.quarantine();
                return 0;
            }
        }
        let reconstructed = self
            .mapper
            .lock()
            .await
            .reconstruct_dispatch(&record.dispatch_id, &principal);
        let Ok((envelope, action)) = reconstructed else {
            recovery_claim.quarantine();
            return 0;
        };
        let owner = A2aEventOwner {
            subject: record.owner_subject.clone(),
            tenant_id: record.owner_tenant_id.clone(),
        };
        let job = A2aDispatchJob {
            durable_dispatch_id: Some(record.dispatch_id.clone()),
            durable_cancellation_id: None,
            lane: DispatchLane::Message,
            mode: A2aExecutionMode::Immediate,
            envelope,
            action,
        };
        let mut acceptance = match self.reserve_dispatch(
            job,
            DispatchReservation {
                owner,
                task_id: record.task_id.clone(),
                message_id: record.message_id.clone(),
                run_id: record.run_id.clone(),
                lane: DispatchLane::Message,
                delay_start: false,
                allow_new: true,
            },
        ) {
            Ok(acceptance) => acceptance,
            Err(_) => return 0,
        };
        let Some(acceptance_ref) = acceptance.as_mut() else {
            return 0;
        };
        if !acceptance_ref.newly_reserved {
            return 0;
        }
        if record.state == A2aDispatchOutboxState::Running
            && self
                .mark_dispatch_reconcile_pending(
                    &record.dispatch_id,
                    "recovering a dispatch with unknown outcome",
                )
                .await
                .is_err()
        {
            return 0;
        }
        let Some(commit) = acceptance_ref.commit.take() else {
            return 0;
        };
        if self
            .mark_dispatch_running_with_commit(&record.dispatch_id, commit, recovery_claim)
            .await
            .is_err()
        {
            return 0;
        }
        1
    }

    async fn recover_pending_dispatches(
        self: &Arc<Self>,
        cancellation: &CancellationToken,
        recovery_deadline: Instant,
        cursor: &mut Option<A2aEventOwner>,
    ) -> usize {
        let (pending, cancellation_tasks): (Vec<A2aDispatchOutboxRecord>, BTreeSet<String>) = {
            let mapper = self.mapper.lock().await;
            let pending_dispatches = mapper.pending_dispatches();
            if pending_dispatches.is_empty() {
                return 0;
            }
            let pending = pending_dispatches
                .into_iter()
                .filter(|record| mapper.dispatch_event_ready(record))
                .collect();
            (
                pending,
                mapper
                    .pending_cancellations()
                    .into_iter()
                    .map(|record| record.task_id)
                    .collect(),
            )
        };
        let pending: Vec<_> = pending
            .into_iter()
            .filter(|record| {
                if cancellation_tasks.contains(&record.task_id) {
                    self.quarantine_recovery(&record.dispatch_id);
                    return false;
                }
                !self
                    .recovery_was_attempted(&record.dispatch_id)
                    .unwrap_or(true)
            })
            .collect();
        let batch = fair_recovery_batch(
            pending,
            cursor,
            self.config.max_recovery_concurrency,
            self.config.max_recovery_concurrency_per_owner,
            |record| A2aEventOwner {
                subject: record.owner_subject.clone(),
                tenant_id: record.owner_tenant_id.clone(),
            },
        );
        let mut recoveries = JoinSet::new();
        for record in batch {
            recoveries.spawn(self.clone().recover_one_pending_dispatch(
                record,
                cancellation.clone(),
                recovery_deadline,
            ));
        }
        let collect = async {
            let mut scheduled = 0_usize;
            while let Some(result) = recoveries.join_next().await {
                if let Ok(count) = result {
                    scheduled = scheduled.saturating_add(count);
                }
            }
            scheduled
        };
        tokio::time::timeout_at(recovery_deadline, collect)
            .await
            .unwrap_or_default()
    }

    async fn run_recovery_driver(
        self: Arc<Self>,
        cancellation: CancellationToken,
        notify: Arc<Notify>,
    ) {
        let mut cancellation_cursor = None;
        let mut event_cursor = None;
        let mut dispatch_cursor = None;
        let mut idle_backoff = Duration::from_millis(50);
        while !cancellation.is_cancelled() {
            let retry_clock_repaired = self
                .persist_mapper_mutation(|candidate| {
                    candidate.clamp_restored_event_retry_deadlines(current_unix_millis())
                })
                .await;
            if retry_clock_repaired.is_err() {
                if self.snapshot_fail_stop.is_cancelled() {
                    break;
                }
                tokio::select! {
                    () = cancellation.cancelled() => break,
                    () = notify.notified() => {},
                    () = tokio::time::sleep(Duration::from_millis(50)) => {},
                }
                continue;
            }
            let cycle_started = Instant::now();
            let stage_budget = self.config.startup_recovery_budget / 3;
            let cancellation_deadline = cycle_started + stage_budget;
            let event_deadline = cancellation_deadline + stage_budget;
            let recovery_deadline = cycle_started + self.config.startup_recovery_budget;
            // Cancellation controls are admitted first. They are merely committed to the reserved
            // control lane here; the serving loop can launch them while slower event/message
            // recovery continues in this separately tracked task.
            let cancellation_progress = self
                .recover_pending_cancellations(
                    &cancellation,
                    cancellation_deadline,
                    &mut cancellation_cursor,
                )
                .await;
            if cancellation.is_cancelled() {
                break;
            }
            // Event delivery is best-effort in the recovery driver and never a listener-readiness
            // gate. Poison events are durably quarantined by recover_pending_events.
            let event_progress = self
                .recover_pending_events(&cancellation, event_deadline, &mut event_cursor)
                .await;
            let dispatch_progress = if Instant::now() < recovery_deadline {
                self.recover_pending_dispatches(
                    &cancellation,
                    recovery_deadline,
                    &mut dispatch_cursor,
                )
                .await
            } else {
                0
            };
            if cancellation.is_cancelled() {
                break;
            }
            if cancellation_progress == 0 && event_progress == 0 && dispatch_progress == 0 {
                let now_unix_ms = current_unix_millis();
                let next_event_attempt = self.mapper.lock().await.next_pending_event_attempt_at();
                let (sleep_for, waiting_for_event) = match next_event_attempt {
                    Some(next_attempt) if next_attempt > now_unix_ms => {
                        let jitter_ms = now_unix_ms % 29;
                        (
                            Duration::from_millis(
                                next_attempt
                                    .saturating_sub(now_unix_ms)
                                    .min(A2A_EVENT_RETRY_MAX_MS)
                                    .saturating_add(jitter_ms),
                            ),
                            true,
                        )
                    }
                    _ => (idle_backoff, false),
                };
                let notified = tokio::select! {
                    () = cancellation.cancelled() => break,
                    () = notify.notified() => true,
                    () = tokio::time::sleep(sleep_for) => false,
                };
                if notified || waiting_for_event {
                    idle_backoff = Duration::from_millis(50);
                } else {
                    idle_backoff = idle_backoff.saturating_mul(2).min(Duration::from_secs(1));
                }
            } else {
                idle_backoff = Duration::from_millis(50);
                tokio::task::yield_now().await;
            }
        }
    }

    /// Publish the current status after a host-managed mapper transition.
    ///
    /// The host remains responsible for durably persisting that external transition before it
    /// calls this method. This method re-authorizes task visibility, persists the event, and only
    /// then broadcasts it.
    pub async fn publish_task_status(
        &self,
        task_id: &str,
        correlation: CorrelationIdentity,
        principal: &ProtocolPrincipal,
    ) -> ProtocolResult<A2aPersistedEvent> {
        let snapshot_read = self.acquire_current_snapshot_read().await?;
        let mapper = self.mapper.lock().await;
        let governed = mapper.prepare_get_task(task_id, correlation, Some(principal));
        let (_, action) = governed.into_authorized()?;
        let A2aAction::GetTask { task } = action else {
            return Err(ProtocolError::conflict(
                "unexpected A2A task status publication action",
            ));
        };
        drop(mapper);
        drop(snapshot_read);
        self.flush_pending_events_for_task(task_id)
            .await?
            .ok_or_else(|| {
                ProtocolError::conflict(format!(
                    "A2A task {} has no durable event intent to publish",
                    task.mapping.task_id
                ))
            })
    }

    /// Persist and publish a receiver-side task transition to all bound SSE subscribers.
    pub async fn transition_task(
        &self,
        task_id: &str,
        next: A2aTaskState,
        status_message: Option<String>,
    ) -> ProtocolResult<A2aPersistedEvent> {
        self.persist_mapper_mutation(|candidate| {
            candidate.transition_task(task_id, next, status_message)
        })
        .await?;
        self.flush_pending_events_for_task(task_id)
            .await?
            .ok_or_else(|| ProtocolError::conflict("A2A transition created no event intent"))
    }

    /// Persist a non-runnable task transition for the exact host callback generation and publish
    /// its durable event intent. A stale callback cannot transition a replacement dispatch
    /// attempt, and an exact idempotent retry returns the already persisted task.
    pub async fn transition_dispatch_task(
        &self,
        context: &A2aDispatchContext,
        next: A2aTaskState,
        status_message: Option<String>,
    ) -> ProtocolResult<A2aTaskRecord> {
        let fence = context.dispatch_fence.as_ref().ok_or_else(|| {
            ProtocolError::invalid_transition(
                "A2A dispatch transition requires a message dispatch context",
            )
        })?;
        self.transition_dispatch_fence(fence, next, status_message)
            .await
    }

    async fn transition_dispatch_fence(
        &self,
        fence: &A2aDispatchFence,
        next: A2aTaskState,
        status_message: Option<String>,
    ) -> ProtocolResult<A2aTaskRecord> {
        let task = self
            .persist_mapper_mutation(|candidate| {
                let task_id = candidate
                    .dispatch_outbox()
                    .get(&fence.dispatch_id)
                    .map(|dispatch| dispatch.task_id.clone())
                    .ok_or_else(|| ProtocolError::not_found("A2A dispatch is not registered"))?;
                candidate.transition_dispatch_task(
                    &fence.dispatch_id,
                    fence.expected_attempt,
                    next,
                    status_message,
                )?;
                candidate
                    .tasks()
                    .get(&task_id)
                    .cloned()
                    .ok_or_else(|| ProtocolError::not_found("A2A task is not registered"))
            })
            .await?;
        self.flush_pending_events_for_task(&task.mapping.task_id)
            .await?;
        Ok(task)
    }

    /// Complete the exact host callback generation with durable task artifacts. Artifact output,
    /// terminal task state, dispatch settlement, and the terminal event intent share one mapper
    /// persistence commit; a stale or cancellation-crossing callback is rejected.
    pub async fn complete_task_with_artifacts(
        &self,
        context: &A2aDispatchContext,
        artifacts: Vec<A2aArtifact>,
    ) -> ProtocolResult<A2aTaskRecord> {
        let fence = context.dispatch_fence.as_ref().ok_or_else(|| {
            ProtocolError::invalid_transition(
                "A2A output completion requires a message dispatch context",
            )
        })?;
        let task = self
            .persist_mapper_mutation(|candidate| {
                candidate.complete_dispatch_with_artifacts(
                    &fence.dispatch_id,
                    fence.expected_attempt,
                    artifacts,
                )
            })
            .await?;
        self.flush_pending_events_for_task(&task.mapping.task_id)
            .await?;
        Ok(task)
    }

    /// Complete the exact host callback generation with a durable direct agent Message response.
    /// The task still becomes terminal internally for Get/List consistency, while SendMessage
    /// projects the response as `result.message` from the settled dispatch record.
    pub async fn complete_with_direct_message(
        &self,
        context: &A2aDispatchContext,
        message: A2aMessage,
    ) -> ProtocolResult<A2aTaskRecord> {
        let fence = context.dispatch_fence.as_ref().ok_or_else(|| {
            ProtocolError::invalid_transition(
                "A2A direct response requires a message dispatch context",
            )
        })?;
        let task = self
            .persist_mapper_mutation(|candidate| {
                candidate.complete_dispatch_with_message(
                    &fence.dispatch_id,
                    fence.expected_attempt,
                    message,
                )
            })
            .await?;
        self.flush_pending_events_for_task(&task.mapping.task_id)
            .await?;
        Ok(task)
    }

    /// Serve an already-bound listener until cancellation and then drain in-flight work.
    pub async fn serve(
        self: Arc<Self>,
        listener: TcpListener,
        cancellation: CancellationToken,
    ) -> ProtocolResult<()> {
        self.serve_supervised(listener, None, cancellation).await
    }

    /// Supervise the public listener and an independently authenticated, CancelTask-only ingress
    /// with one canonical scheduler, recovery driver, snapshot lifecycle, and shutdown boundary.
    pub async fn serve_with_protected_control(
        self: Arc<Self>,
        public_listener: TcpListener,
        protected_control: A2aProtectedControlIngress,
        cancellation: CancellationToken,
    ) -> ProtocolResult<()> {
        if self.config.max_control_dispatches < 2 {
            return Err(ProtocolError::invalid(
                "protected control ingress requires at least two control dispatch slots",
            ));
        }
        self.serve_supervised(public_listener, Some(protected_control), cancellation)
            .await
    }

    async fn serve_supervised(
        self: Arc<Self>,
        listener: TcpListener,
        protected_control: Option<A2aProtectedControlIngress>,
        cancellation: CancellationToken,
    ) -> ProtocolResult<()> {
        let mut dispatch_receiver = self
            .dispatch_receiver
            .lock()
            .await
            .take()
            .ok_or_else(|| ProtocolError::conflict("A2A HTTP server is already serving"))?;
        let mut control_receiver = self
            .control_receiver
            .lock()
            .await
            .take()
            .ok_or_else(|| ProtocolError::conflict("A2A control server is already serving"))?;
        self.ensure_snapshot_available()?;
        tokio::select! {
            biased;
            () = cancellation.cancelled() => return Ok(()),
            initialized = self.initialize_snapshot_store() => initialized?,
        }
        self.serving.store(true, AtomicOrdering::Release);
        let service_cancellation = CancellationToken::new();
        let (mut protected_listener, protected_authenticator) = protected_control.map_or_else(
            || (None, None),
            |ingress| (Some(ingress.listener), Some(ingress.authenticator)),
        );
        let handshake_semaphore = Arc::new(Semaphore::new(self.config.max_concurrency));
        let control_handshake_semaphore =
            Arc::new(Semaphore::new(self.config.max_control_dispatches));
        let control_handshake_waiters =
            Arc::new(Semaphore::new(self.config.max_queued_control_dispatches));
        // Authentication is only knowable after reading the shared listener's headers. Keep one
        // final, short-deadline classification socket outside the general/control/waiter pools so
        // filling those pools with unauthenticated partial headers cannot deterministically drop
        // the next genuine cancellation. This is bounded mitigation, not absolute auth priority:
        // an attacker that also occupies this single slot can still delay one deadline.
        let last_chance_handshakes = Arc::new(Semaphore::new(A2A_LAST_CHANCE_HANDSHAKES));
        let protected_admission_capacity = self
            .config
            .max_control_dispatches
            .checked_add(self.config.max_queued_control_dispatches)
            .ok_or_else(|| {
                ProtocolError::invalid("protected control admission capacity overflowed")
            })?;
        // Protected sockets stay bounded across both authenticated active bodies and bounded
        // waiters. Per-owner waiter caps leave capacity for another authenticated owner even
        // while one owner floods valid headers and slow bodies.
        let protected_handshakes = Arc::new(Semaphore::new(protected_admission_capacity));
        // One separately bounded classification socket remains reachable when all ordinary
        // active+waiter header permits are occupied. As on the public listener, this is a bounded
        // last-chance lane; deployments requiring absolute pre-header identity must use a
        // transport-authenticated protected listener (for example mTLS or a local socket proxy).
        let protected_last_chance_handshakes = Arc::new(Semaphore::new(A2A_LAST_CHANCE_HANDSHAKES));
        let protected_requests = Arc::new(ControlProbeLimiter::new(ControlProbeLimits {
            max_active: self.config.max_control_dispatches,
            max_per_ip: protected_admission_capacity,
            max_per_owner: self.config.max_control_requests_per_owner,
            max_per_minute: self.config.max_requests_per_minute,
            max_rate_buckets: self.config.max_rate_buckets,
            max_waiters: self.config.max_queued_control_dispatches,
            max_waiters_per_ip: self.config.max_queued_control_dispatches,
            max_waiters_per_owner: self.config.max_queued_control_dispatches_per_owner,
        }));
        let protected_writers = Arc::new(Semaphore::new(self.config.max_control_dispatches));
        let mut connections = JoinSet::new();
        let mut protected_connections = JoinSet::new();
        let mut dispatches = JoinSet::new();
        let mut recoveries = JoinSet::new();
        let recovery_notify = Arc::new(Notify::new());
        recoveries.spawn(
            self.clone()
                .run_recovery_driver(service_cancellation.clone(), recovery_notify.clone()),
        );
        let mut serve_error = None;
        let snapshot_fail_stopped = loop {
            tokio::select! {
                () = cancellation.cancelled() => break false,
                () = self.snapshot_fail_stop.cancelled() => break true,
                completed = recoveries.join_next(), if !recoveries.is_empty() => {
                    if completed.is_some()
                        && !service_cancellation.is_cancelled()
                        && !self.snapshot_fail_stop.is_cancelled()
                    {
                        recoveries.spawn(self.clone().run_recovery_driver(
                            service_cancellation.clone(),
                            recovery_notify.clone(),
                        ));
                    }
                }
                completed = connections.join_next(), if !connections.is_empty() => {
                    if let Some(Err(error)) = completed {
                        serve_error = Some(ProtocolError::conflict(format!(
                            "A2A HTTP task failed: {error}"
                        )));
                        break false;
                    }
                }
                completed = protected_connections.join_next(), if !protected_connections.is_empty() => {
                    if let Some(Err(error)) = completed {
                        serve_error = Some(ProtocolError::conflict(format!(
                            "protected A2A control task failed: {error}"
                        )));
                        break false;
                    }
                }
                completed = dispatches.join_next(), if !dispatches.is_empty() => {
                    match completed {
                        Some(Ok(completion)) => {
                            self.finish_scheduled_dispatch(completion)?;
                            recovery_notify.notify_one();
                        }
                        Some(Err(error)) => {
                            serve_error = Some(ProtocolError::conflict(format!(
                                "A2A dispatch task failed: {error}"
                            )));
                            break false;
                        }
                        None => {}
                    }
                }
                scheduled = dispatch_receiver.recv() => {
                    if let Some(scheduled) = scheduled {
                        let server = self.clone();
                        let dispatch_cancellation = service_cancellation.clone();
                        dispatches.spawn(async move {
                            server
                                .run_scheduled_dispatch(scheduled, dispatch_cancellation)
                                .await
                        });
                    }
                }
                scheduled = control_receiver.recv() => {
                    if let Some(scheduled) = scheduled {
                        let server = self.clone();
                        let dispatch_cancellation = service_cancellation.clone();
                        dispatches.spawn(async move {
                            server
                                .run_scheduled_dispatch(scheduled, dispatch_cancellation)
                                .await
                        });
                    }
                }
                accepted = listener.accept() => {
                    let (mut stream, peer) = match accepted {
                        Ok(accepted) => accepted,
                        Err(error) => {
                            serve_error = Some(protocol_io("accept A2A HTTP connection", error));
                            break false;
                        }
                    };
                    let general_handshake = handshake_semaphore
                        .clone()
                        .try_acquire_owned()
                        .ok();
                    let mut last_chance_handshake = None;
                    let overflow_waiter = if general_handshake.is_none() {
                        match control_handshake_waiters.clone().try_acquire_owned() {
                            Ok(waiter) => Some(waiter),
                            Err(_) => {
                                match last_chance_handshakes.clone().try_acquire_owned() {
                                    Ok(permit) => {
                                        last_chance_handshake = Some(permit);
                                        None
                                    }
                                    Err(_) => {
                                        // Every bounded classification slot is full. Dropping the
                                        // socket avoids creating another writer/waiter task.
                                        drop(stream);
                                        continue;
                                    }
                                }
                            }
                        }
                    } else {
                        None
                    };
                    let server = self.clone();
                    let connection_cancellation = service_cancellation.clone();
                    let control_handshakes = control_handshake_semaphore.clone();
                    let connection_recovery_notify = recovery_notify.clone();
                    connections.spawn(async move {
                        let (permit, control_overflow) = match general_handshake {
                            Some(permit) => (permit, false),
                            None if last_chance_handshake.is_some() => (
                                last_chance_handshake
                                    .take()
                                    .expect("checked last-chance handshake permit"),
                                true,
                            ),
                            None => {
                                let permit = match timeout(
                                    server.config.control_probe_timeout,
                                    control_handshakes.acquire_owned(),
                                )
                                .await
                                {
                                    Ok(Ok(permit)) => permit,
                                    Ok(Err(_)) | Err(_) => {
                                        drop(overflow_waiter);
                                        let _ = timeout(
                                            server.config.control_probe_timeout,
                                            stream.shutdown(),
                                        )
                                        .await;
                                        return;
                                    }
                                };
                                drop(overflow_waiter);
                                (permit, true)
                            }
                        };
                        let outcome = match timeout(
                            server.config.request_timeout,
                            server.handle_connection(
                                &mut stream,
                                peer,
                                connection_cancellation.clone(),
                                permit,
                                control_overflow,
                            ),
                        ).await {
                            Ok(Ok(outcome)) => outcome,
                            Ok(Err(error)) => ConnectionOutcome::Response(protocol_http_response(&error)),
                            Err(_) => ConnectionOutcome::Response(HttpResponse::text(408, "request timeout")),
                        };
                        match outcome {
                            ConnectionOutcome::Response(response) => {
                                let writer = if response.control_priority {
                                    server.control_response_write_global.clone()
                                } else {
                                    server.response_write_global.clone()
                                };
                                // Never queue an unbounded set of sockets behind clients that stop
                                // reading. The dedicated control writer pool prevents ordinary
                                // response backpressure from consuming cancellation delivery.
                                if let Ok(_writer_permit) = writer.try_acquire_owned() {
                                    let _ = timeout(
                                        server.config.handshake_timeout,
                                        write_http_response(&mut stream, response),
                                    )
                                    .await;
                                }
                            }
                            ConnectionOutcome::Stream(plan) => {
                                let _ = server
                                    .write_sse_stream(&mut stream, *plan, connection_cancellation)
                                    .await;
                            }
                        }
                        let _ = timeout(server.config.handshake_timeout, stream.shutdown()).await;
                        connection_recovery_notify.notify_one();
                    });
                }
                accepted = async {
                    match protected_listener.as_ref() {
                        Some(listener) => Some(listener.accept().await),
                        None => pending().await,
                    }
                } => {
                    let Some(accepted) = accepted else {
                        continue;
                    };
                    let (mut stream, peer) = match accepted {
                        Ok(accepted) => accepted,
                        Err(error) => {
                            serve_error = Some(protocol_io(
                                "accept protected A2A control connection",
                                error,
                            ));
                            break false;
                        }
                    };
                    let handshake_permit = match protected_handshakes
                        .clone()
                        .try_acquire_owned()
                    {
                        Ok(permit) => permit,
                        Err(_) => match protected_last_chance_handshakes
                            .clone()
                            .try_acquire_owned()
                        {
                            Ok(permit) => permit,
                            Err(_) => {
                                drop(stream);
                                continue;
                            }
                        },
                    };
                    let server = self.clone();
                    let connection_cancellation = service_cancellation.clone();
                    let authenticator = protected_authenticator
                        .as_ref()
                        .expect("protected listener has an authenticator")
                        .clone();
                    let request_slots = protected_requests.clone();
                    let writers = protected_writers.clone();
                    let connection_recovery_notify = recovery_notify.clone();
                    protected_connections.spawn(async move {
                        let outcome = match timeout(
                            server.config.request_timeout,
                            server.handle_protected_control_connection(
                                &mut stream,
                                peer,
                                connection_cancellation,
                                authenticator,
                                request_slots,
                                handshake_permit,
                            ),
                        )
                        .await
                        {
                            Ok(Ok(outcome)) => outcome,
                            Ok(Err(error)) => {
                                ConnectionOutcome::Response(protocol_http_response(&error))
                            }
                            Err(_) => ConnectionOutcome::Response(HttpResponse::text(
                                408,
                                "protected control request timeout",
                            )),
                        };
                        let response = match outcome {
                            ConnectionOutcome::Response(response) => response,
                            ConnectionOutcome::Stream(_) => HttpResponse::text(
                                403,
                                "protected control ingress does not permit streaming",
                            ),
                        };
                        if let Ok(_writer_permit) = writers.try_acquire_owned() {
                            let _ = timeout(
                                server.config.control_probe_timeout,
                                write_http_response(&mut stream, response),
                            )
                            .await;
                        }
                        let _ = timeout(
                            server.config.control_probe_timeout,
                            stream.shutdown(),
                        )
                        .await;
                        connection_recovery_notify.notify_one();
                    });
                }
            }
        };
        self.serving.store(false, AtomicOrdering::Release);
        service_cancellation.cancel();
        drop(listener);
        drop(protected_listener.take());
        handshake_semaphore.close();
        control_handshake_semaphore.close();
        control_handshake_waiters.close();
        last_chance_handshakes.close();
        protected_handshakes.close();
        protected_last_chance_handshakes.close();
        protected_requests.close();
        protected_writers.close();
        self.request_global.close();
        self.control_probe_global.close();
        self.control_exact_probe_global.close();
        self.control_request_global.close();
        self.response_write_global.close();
        self.control_response_write_global.close();
        self.preauth_limiter.close();
        self.control_probe_limiter.close();
        self.control_exact_probe_limiter.close();
        self.control_request_limiter.close();
        self.stream_quota.close();
        recovery_notify.notify_waiters();
        dispatch_receiver.close();
        control_receiver.close();
        while let Ok(scheduled) = dispatch_receiver.try_recv() {
            let server = self.clone();
            let dispatch_cancellation = service_cancellation.clone();
            dispatches.spawn(async move {
                server
                    .run_scheduled_dispatch(scheduled, dispatch_cancellation)
                    .await
            });
        }
        while let Ok(scheduled) = control_receiver.try_recv() {
            let server = self.clone();
            let dispatch_cancellation = service_cancellation.clone();
            dispatches.spawn(async move {
                server
                    .run_scheduled_dispatch(scheduled, dispatch_cancellation)
                    .await
            });
        }
        let drain = async {
            while let Some(result) = connections.join_next().await {
                result.map_err(|error| {
                    ProtocolError::conflict(format!("A2A HTTP task failed: {error}"))
                })?;
            }
            while let Some(result) = protected_connections.join_next().await {
                result.map_err(|error| {
                    ProtocolError::conflict(format!("protected A2A control task failed: {error}"))
                })?;
            }
            while let Some(result) = dispatches.join_next().await {
                let completion = result.map_err(|error| {
                    ProtocolError::conflict(format!("A2A dispatch task failed: {error}"))
                })?;
                self.finish_scheduled_dispatch(completion)?;
            }
            while let Some(result) = recoveries.join_next().await {
                let _ = result;
            }
            Ok::<(), ProtocolError>(())
        };
        match timeout(self.config.graceful_shutdown_timeout, drain).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                if serve_error.is_none() {
                    serve_error = Some(error);
                }
            }
            Err(_) => {
                connections.abort_all();
                protected_connections.abort_all();
                dispatches.abort_all();
                recoveries.abort_all();
                while connections.join_next().await.is_some() {}
                while protected_connections.join_next().await.is_some() {}
                while dispatches.join_next().await.is_some() {}
                while recoveries.join_next().await.is_some() {}
            }
        }

        // Snapshot commits are detached from their request futures by design, but never from the
        // server lifecycle. A commit may already have reached durable storage, so shutdown must
        // join it rather than aborting it at the graceful request/dispatch deadline.
        for handle in self.close_snapshot_commit_registry()? {
            if let Err(error) = handle.await {
                let failure = ProtocolError::conflict(format!(
                    "A2A snapshot commit task failed during shutdown: {error}"
                ));
                self.fail_stop_snapshot(failure.clone());
                if serve_error.is_none() {
                    serve_error = Some(failure);
                }
            }
        }
        if snapshot_fail_stopped || self.snapshot_commit_failed.load(AtomicOrdering::Acquire) {
            return Err(self
                .snapshot_failure
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
                .unwrap_or_else(|| {
                    ProtocolError::conflict(
                        "A2A server fail-stopped after an ambiguous snapshot commit",
                    )
                }));
        }
        match serve_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    async fn handle_protected_control_connection(
        self: &Arc<Self>,
        stream: &mut TcpStream,
        peer: SocketAddr,
        cancellation: CancellationToken,
        authenticator: Arc<dyn A2aHttpAuthenticator>,
        request_slots: Arc<ControlProbeLimiter>,
        handshake_permit: OwnedSemaphorePermit,
    ) -> ProtocolResult<ConnectionOutcome> {
        let deadline = Instant::now() + self.config.control_probe_timeout;
        let request_head = match tokio::time::timeout_at(
            deadline,
            read_http_request_head(
                stream,
                self.config.max_header_bytes,
                self.config.max_control_probe_body_bytes,
            ),
        )
        .await
        {
            Ok(Ok(request)) => request,
            Ok(Err(error)) => {
                return Ok(ConnectionOutcome::Response(HttpResponse::text(
                    error.status,
                    &error.message,
                )))
            }
            Err(_) => {
                return Ok(ConnectionOutcome::Response(HttpResponse::text(
                    408,
                    "protected control handshake timeout",
                )))
            }
        };
        if !self.allowed_host(&request_head.headers) {
            return Ok(ConnectionOutcome::Response(HttpResponse::text(
                421,
                "Host is not allowed",
            )));
        }
        if request_head
            .headers
            .get("origin")
            .is_some_and(|origin| !self.config.allowed_origins.contains(origin))
        {
            return Ok(ConnectionOutcome::Response(HttpResponse::text(
                403,
                "origin is not allowed",
            )));
        }
        if request_head.path != self.config.path {
            return Ok(ConnectionOutcome::Response(HttpResponse::text(
                404,
                "not found",
            )));
        }
        if request_head.method != "POST" {
            return Ok(ConnectionOutcome::Response(
                HttpResponse::text(405, "protected control ingress requires POST")
                    .with_header("Allow", "POST"),
            ));
        }
        let principal = match authenticator.authenticate(&request_head.headers) {
            Ok(principal) => principal,
            Err(error) => {
                let mut response = HttpResponse::text(error.status, &error.message);
                if let Some(challenge) = error.www_authenticate {
                    response = response.with_header("WWW-Authenticate", &challenge);
                }
                return Ok(ConnectionOutcome::Response(response));
            }
        };
        if !principal.scopes.contains("*") && !principal.scopes.contains(A2A_TASK_CANCEL_SCOPE) {
            return Ok(ConnectionOutcome::Response(HttpResponse::text(
                403,
                "protected control principal lacks cancel scope",
            )));
        }
        let content_type = request_head
            .headers
            .get("content-type")
            .unwrap_or_default()
            .split(';')
            .next()
            .unwrap_or_default()
            .trim();
        if !content_type.eq_ignore_ascii_case("application/json") {
            return Ok(ConnectionOutcome::Response(HttpResponse::text(
                415,
                "Content-Type must be application/json",
            )));
        }
        if request_head.headers.get("a2a-version") != Some(A2A_PROTOCOL_VERSION) {
            return Ok(ConnectionOutcome::Response(HttpResponse::json(
                400,
                jsonrpc_error(
                    Value::Null,
                    -32009,
                    "Version not supported",
                    Some(a2a_error_data(
                        "VERSION_NOT_SUPPORTED",
                        [("supportedVersions", json!([A2A_PROTOCOL_VERSION]))],
                    )),
                ),
            )));
        }
        if !accepts_media_type(
            request_head.headers.get("accept").unwrap_or("*/*"),
            "application/json",
        ) {
            return Ok(ConnectionOutcome::Response(HttpResponse::text(
                406,
                "Accept must include application/json",
            )));
        }

        let owner = A2aEventOwner {
            subject: principal.subject.clone(),
            tenant_id: principal.tenant_id.clone(),
        };
        // Register in the globally and per-owner bounded fair queue before releasing the socket's
        // header permit. The handshake pool itself covers active+queued capacity, so authenticated
        // header churn cannot manufacture an unbounded task/FD queue outside this limiter.
        let Some(_request_lease) = request_slots
            .acquire_exact_classification_until(peer.ip(), owner, deadline)
            .await?
        else {
            return Ok(ConnectionOutcome::Response(
                HttpResponse::text(503, "protected control admission is full")
                    .with_header("Retry-After", "1"),
            ));
        };
        drop(handshake_permit);
        let request =
            match tokio::time::timeout_at(deadline, read_http_request_body(stream, request_head))
                .await
            {
                Ok(Ok(request)) => request,
                Ok(Err(error)) => {
                    return Ok(ConnectionOutcome::Response(HttpResponse::text(
                        error.status,
                        &error.message,
                    )))
                }
                Err(_) => {
                    return Ok(ConnectionOutcome::Response(HttpResponse::text(
                        408,
                        "protected control body timeout",
                    )))
                }
            };
        let value: Value = match serde_json::from_slice(&request.body) {
            Ok(value) => value,
            Err(_) => {
                return Ok(ConnectionOutcome::Response(HttpResponse::json(
                    400,
                    jsonrpc_error(Value::Null, -32700, "Parse error", None),
                )))
            }
        };
        let rpc = match JsonRpcRequest::parse(value, &request.headers) {
            Ok(rpc) => rpc,
            Err(error) => {
                return Ok(ConnectionOutcome::Response(HttpResponse::json(
                    400,
                    jsonrpc_error(error.id, error.code, error.message, None),
                )))
            }
        };
        if rpc.last_event_id.is_some() {
            return Ok(rpc_invalid_params(
                rpc.id,
                "Last-Event-ID is not valid on protected control ingress",
            ));
        }
        if rpc.method != "CancelTask" {
            return Ok(ConnectionOutcome::Response(HttpResponse::json(
                200,
                jsonrpc_error(
                    rpc.id,
                    -32601,
                    "Protected control ingress only permits CancelTask",
                    None,
                ),
            )));
        }
        let mut outcome = self
            .handle_cancel_task(rpc, principal, CancellationIngress::Protected, cancellation)
            .await?;
        if let ConnectionOutcome::Response(response) = &mut outcome {
            response.control_priority = true;
        }
        // Peer identity remains available to the canonical audit path through the independently
        // authenticated principal; no shared-network IP quota is charged on protected ingress.
        let _ = peer;
        Ok(outcome)
    }

    async fn handle_connection(
        self: &Arc<Self>,
        stream: &mut TcpStream,
        peer: SocketAddr,
        cancellation: CancellationToken,
        handshake_permit: OwnedSemaphorePermit,
        mut control_overflow: bool,
    ) -> ProtocolResult<ConnectionOutcome> {
        let preauth_permit = if control_overflow {
            None
        } else if let Some(permit) = self.preauth_limiter.try_acquire(peer.ip())? {
            Some(permit)
        } else {
            // Do not let ordinary same-NAT leases exclude cancellation classification. This
            // connection remains globally bounded by its handshake permit and switches to the
            // strict short overflow deadline; authenticated owner/IP limits apply after parsing.
            control_overflow = true;
            None
        };
        // Record the ordinary ingress rate immediately, while the per-IP pre-authentication lease
        // still bounds the extra work below. Do not reject here: an authenticated, small
        // `CancelTask` uses separate probe/request rate limits and must remain reachable even when
        // unrelated traffic from the same NAT has exhausted the ordinary bucket.
        let ordinary_rate_available = self.consume_rate(peer.ip())?;
        let request_head = match timeout(
            if control_overflow {
                self.config.control_probe_timeout
            } else {
                self.config.handshake_timeout
            },
            read_http_request_head(
                stream,
                self.config.max_header_bytes,
                self.config.max_body_bytes,
            ),
        )
        .await
        {
            Ok(Ok(request)) => request,
            Ok(Err(error)) => {
                return Ok(ConnectionOutcome::Response(HttpResponse::text(
                    error.status,
                    &error.message,
                )))
            }
            Err(_) => {
                return Ok(ConnectionOutcome::Response(HttpResponse::text(
                    408,
                    "request handshake timeout",
                )))
            }
        };
        if !self.allowed_host(&request_head.headers) {
            return Ok(ConnectionOutcome::Response(HttpResponse::text(
                421,
                "Host is not allowed",
            )));
        }
        if let Some(origin) = request_head.headers.get("origin") {
            if !self.config.allowed_origins.contains(origin) {
                return Ok(ConnectionOutcome::Response(HttpResponse::text(
                    403,
                    "origin is not allowed",
                )));
            }
        }
        if request_head.path == A2A_AGENT_CARD_PATH {
            if control_overflow {
                return Ok(ConnectionOutcome::Response(
                    HttpResponse::text(503, "cancellation handshake reserve requires CancelTask")
                        .with_header("Retry-After", "1"),
                ));
            }
            if !ordinary_rate_available {
                return Ok(ConnectionOutcome::Response(
                    HttpResponse::text(429, "rate limit exceeded").with_header("Retry-After", "60"),
                ));
            }
            if request_head.method != "GET" {
                return Ok(ConnectionOutcome::Response(
                    HttpResponse::text(405, "method not allowed").with_header("Allow", "GET"),
                ));
            }
            drop(preauth_permit);
            drop(handshake_permit);
            let body = serde_json::to_value(&self.agent_card).map_err(|error| {
                ProtocolError::conflict(format!("serialize Agent Card: {error}"))
            })?;
            return Ok(ConnectionOutcome::Response(HttpResponse::json(200, body)));
        }
        if request_head.path != self.config.path {
            if !ordinary_rate_available {
                return Ok(ConnectionOutcome::Response(
                    HttpResponse::text(429, "rate limit exceeded").with_header("Retry-After", "60"),
                ));
            }
            return Ok(ConnectionOutcome::Response(HttpResponse::text(
                404,
                "not found",
            )));
        }
        if request_head.method != "POST" {
            if !ordinary_rate_available {
                return Ok(ConnectionOutcome::Response(
                    HttpResponse::text(429, "rate limit exceeded").with_header("Retry-After", "60"),
                ));
            }
            return Ok(ConnectionOutcome::Response(
                HttpResponse::text(405, "method not allowed").with_header("Allow", "POST"),
            ));
        }
        let principal = match self.authenticator.authenticate(&request_head.headers) {
            Ok(principal) => principal,
            Err(error) => {
                if !ordinary_rate_available {
                    return Ok(ConnectionOutcome::Response(
                        HttpResponse::text(429, "rate limit exceeded")
                            .with_header("Retry-After", "60"),
                    ));
                }
                let mut response = HttpResponse::text(error.status, &error.message);
                if let Some(challenge) = error.www_authenticate {
                    response = response.with_header("WWW-Authenticate", &challenge);
                }
                return Ok(ConnectionOutcome::Response(response));
            }
        };
        if control_overflow
            && !principal.scopes.contains("*")
            && !principal.scopes.contains(A2A_TASK_CANCEL_SCOPE)
        {
            return Ok(ConnectionOutcome::Response(
                HttpResponse::text(503, "cancellation handshake reserve requires cancel scope")
                    .with_header("Retry-After", "1"),
            ));
        }
        drop(preauth_permit);
        drop(handshake_permit);
        let content_type = request_head
            .headers
            .get("content-type")
            .unwrap_or_default()
            .split(';')
            .next()
            .unwrap_or_default()
            .trim();
        if !content_type.eq_ignore_ascii_case("application/json") {
            if !ordinary_rate_available {
                return Ok(ConnectionOutcome::Response(
                    HttpResponse::text(429, "rate limit exceeded").with_header("Retry-After", "60"),
                ));
            }
            return Ok(ConnectionOutcome::Response(HttpResponse::text(
                415,
                "Content-Type must be application/json",
            )));
        }
        if request_head.headers.get("a2a-version") != Some(A2A_PROTOCOL_VERSION) {
            if !ordinary_rate_available {
                return Ok(ConnectionOutcome::Response(
                    HttpResponse::text(429, "rate limit exceeded").with_header("Retry-After", "60"),
                ));
            }
            return Ok(ConnectionOutcome::Response(HttpResponse::json(
                400,
                jsonrpc_error(
                    Value::Null,
                    -32009,
                    "Version not supported",
                    Some(a2a_error_data(
                        "VERSION_NOT_SUPPORTED",
                        [("supportedVersions", json!([A2A_PROTOCOL_VERSION]))],
                    )),
                ),
            )));
        }
        // Normal requests must reserve their full request slot before reading a potentially slow
        // body. If those slots are saturated, only a small authenticated body may use the short
        // control probe. The probe has its own capacity and one absolute deadline, so ordinary
        // partial POSTs cannot hold cancellation admission indefinitely.
        let mut general_admission = if ordinary_rate_available && !control_overflow {
            self.request_global.clone().try_acquire_owned().ok()
        } else {
            None
        };
        let mut control_probe_admission = None;
        let mut control_probe_lease = None;
        let control_probe_deadline = if general_admission.is_none() {
            if !principal.scopes.contains("*") && !principal.scopes.contains(A2A_TASK_CANCEL_SCOPE)
            {
                return Ok(ConnectionOutcome::Response(
                    HttpResponse::text(
                        if ordinary_rate_available { 503 } else { 429 },
                        if ordinary_rate_available {
                            "request admission is full"
                        } else {
                            "rate limit exceeded"
                        },
                    )
                    .with_header(
                        "Retry-After",
                        if ordinary_rate_available { "1" } else { "60" },
                    ),
                ));
            }
            if request_head.content_length > self.config.max_control_probe_body_bytes {
                return Ok(ConnectionOutcome::Response(
                    HttpResponse::text(503, "request admission is full")
                        .with_header("Retry-After", "1"),
                ));
            }
            let wait_deadline = Instant::now() + self.config.control_probe_timeout;
            let owner = A2aEventOwner {
                subject: principal.subject.clone(),
                tenant_id: principal.tenant_id.clone(),
            };
            let standard_lease = self
                .control_probe_limiter
                .acquire_probe_until(peer.ip(), owner.clone(), wait_deadline)
                .await?;
            let (lease, probe_global) = if let Some(lease) = standard_lease {
                (lease, self.control_probe_global.clone())
            } else {
                // The standard minute cap is a hard stop. A separately bounded lane gets one
                // last short inspection opportunity so a malformed/ordinary probe cannot spend
                // the owner's standard allowance and thereby hide an exact cancellation. Parsed
                // non-cancellation methods are rejected below, while an exact match must still
                // pass the independent governed control-request limiter.
                let Some(lease) = self
                    .control_exact_probe_limiter
                    .acquire_exact_classification_until(peer.ip(), owner, wait_deadline)
                    .await?
                else {
                    return Ok(ConnectionOutcome::Response(
                        HttpResponse::text(429, "cancellation control probe quota is exhausted")
                            .with_header("Retry-After", "1"),
                    ));
                };
                (lease, self.control_exact_probe_global.clone())
            };
            control_probe_lease = Some(lease);
            let permit =
                match tokio::time::timeout_at(wait_deadline, probe_global.acquire_owned()).await {
                    Ok(Ok(permit)) => permit,
                    Ok(Err(_)) | Err(_) => {
                        return Ok(ConnectionOutcome::Response(
                            HttpResponse::text(503, "cancellation control probe is busy")
                                .with_header("Retry-After", "1"),
                        ))
                    }
                };
            control_probe_admission = Some(permit);
            // The limiter's per-IP/per-owner ticket and Tokio's global semaphore are FIFO, so a
            // refill cannot jump an already-waiting cancellation. Give the admitted request its
            // own short body deadline; wait + body has a fixed two-timeout upper bound.
            Some(Instant::now() + self.config.control_probe_timeout)
        } else {
            None
        };
        let request_result = if let Some(deadline) = control_probe_deadline {
            match tokio::time::timeout_at(deadline, read_http_request_body(stream, request_head))
                .await
            {
                Ok(result) => result,
                Err(_) => Err(http_parse_error(
                    408,
                    "cancellation control probe timed out",
                )),
            }
        } else {
            read_http_request_body(stream, request_head).await
        };
        let request = match request_result {
            Ok(request) => request,
            Err(error) => {
                return Ok(ConnectionOutcome::Response(HttpResponse::text(
                    error.status,
                    &error.message,
                )))
            }
        };
        let value: Value = match serde_json::from_slice(&request.body) {
            Ok(value) => value,
            Err(_) => {
                return Ok(ConnectionOutcome::Response(HttpResponse::json(
                    400,
                    jsonrpc_error(Value::Null, -32700, "Parse error", None),
                )))
            }
        };
        let rpc = match JsonRpcRequest::parse(value, &request.headers) {
            Ok(rpc) => rpc,
            Err(error) => {
                return Ok(ConnectionOutcome::Response(HttpResponse::json(
                    400,
                    jsonrpc_error(error.id, error.code, error.message, None),
                )))
            }
        };
        if let Some(deadline) = control_probe_deadline {
            if Instant::now() >= deadline || rpc.method != "CancelTask" {
                drop(control_probe_admission.take());
                drop(control_probe_lease.take());
                return Ok(ConnectionOutcome::Response(
                    HttpResponse::text(503, "request admission is full")
                        .with_header("Retry-After", "1"),
                ));
            }
        }
        if rpc.last_event_id.is_some() && rpc.method != "SubscribeToTask" {
            return Ok(rpc_invalid_params(
                rpc.id,
                "Last-Event-ID is only valid for SubscribeToTask",
            ));
        }
        let streaming = matches!(
            rpc.method.as_str(),
            "SendStreamingMessage" | "SubscribeToTask"
        );
        let accept = request.headers.get("accept").unwrap_or("*/*");
        let accepts = if streaming {
            accepts_media_type(accept, "text/event-stream")
        } else {
            accepts_media_type(accept, "application/json")
        };
        if !accepts {
            return Ok(ConnectionOutcome::Response(HttpResponse::text(
                406,
                if streaming {
                    "Accept must include text/event-stream"
                } else {
                    "Accept must include application/json"
                },
            )));
        }
        let mut control_request_lease = None;
        let has_cancel_scope =
            principal.scopes.contains("*") || principal.scopes.contains(A2A_TASK_CANCEL_SCOPE);
        let control_admission = if rpc.method == "CancelTask" && has_cancel_scope {
            drop(general_admission.take());
            drop(control_probe_admission.take());
            drop(control_probe_lease.take());
            let owner = A2aEventOwner {
                subject: principal.subject.clone(),
                tenant_id: principal.tenant_id.clone(),
            };
            let Some(mut lease) = self.control_request_limiter.try_acquire(peer.ip(), owner)?
            else {
                return Ok(ConnectionOutcome::Response(
                    HttpResponse::text(429, "cancellation control request quota is exhausted")
                        .with_header("Retry-After", "1"),
                ));
            };
            if !lease.commit_rate()? {
                return Ok(ConnectionOutcome::Response(
                    HttpResponse::text(429, "cancellation control request rate is exhausted")
                        .with_header("Retry-After", "60"),
                ));
            }
            control_request_lease = Some(lease);
            Some(
                match timeout(
                    self.config.control_probe_timeout,
                    self.control_request_global.clone().acquire_owned(),
                )
                .await
                {
                    Ok(Ok(permit)) => permit,
                    Ok(Err(_)) | Err(_) => {
                        return Ok(ConnectionOutcome::Response(
                            HttpResponse::text(503, "cancellation control admission is full")
                                .with_header("Retry-After", "1"),
                        ))
                    }
                },
            )
        } else {
            if general_admission.is_none() {
                return Ok(ConnectionOutcome::Response(
                    HttpResponse::text(503, "request admission is full")
                        .with_header("Retry-After", "1"),
                ));
            }
            None
        };
        drop(control_probe_admission);
        drop(control_probe_lease);
        let control_priority = control_admission.is_some();
        let _general_admission = general_admission;
        let _control_admission = control_admission;
        let _control_request_lease = control_request_lease;
        let mut outcome = self
            .dispatch(rpc, principal, peer.ip(), cancellation)
            .await?;
        if control_priority {
            if let ConnectionOutcome::Response(response) = &mut outcome {
                response.control_priority = true;
            }
        }
        Ok(outcome)
    }

    async fn dispatch(
        self: &Arc<Self>,
        rpc: JsonRpcRequest,
        principal: ProtocolPrincipal,
        peer_ip: IpAddr,
        cancellation: CancellationToken,
    ) -> ProtocolResult<ConnectionOutcome> {
        match rpc.method.as_str() {
            "SendMessage" => self.handle_send(rpc, principal, false, cancellation).await,
            "SendStreamingMessage" => self.handle_send(rpc, principal, true, cancellation).await,
            "GetTask" => self.handle_get_task(rpc, principal).await,
            "ListTasks" => self.handle_list_tasks(rpc, principal).await,
            "CancelTask" => {
                self.handle_cancel_task(
                    rpc,
                    principal,
                    CancellationIngress::Public(peer_ip),
                    cancellation,
                )
                .await
            }
            "SubscribeToTask" => self.handle_subscribe(rpc, principal).await,
            "CreateTaskPushNotificationConfig"
            | "GetTaskPushNotificationConfig"
            | "ListTaskPushNotificationConfigs"
            | "DeleteTaskPushNotificationConfig" => {
                Ok(ConnectionOutcome::Response(HttpResponse::json(
                    200,
                    jsonrpc_error(
                        rpc.id,
                        -32003,
                        "Push notification not supported",
                        Some(a2a_error_data("PUSH_NOTIFICATION_NOT_SUPPORTED", [])),
                    ),
                )))
            }
            "GetExtendedAgentCard" => Ok(ConnectionOutcome::Response(HttpResponse::json(
                200,
                jsonrpc_error(
                    rpc.id,
                    -32004,
                    "Unsupported operation",
                    Some(a2a_error_data(
                        "UNSUPPORTED_OPERATION",
                        [("detail", json!("extended Agent Card is not configured"))],
                    )),
                ),
            ))),
            _ => Ok(ConnectionOutcome::Response(HttpResponse::json(
                200,
                jsonrpc_error(rpc.id, -32601, "Method not found", None),
            ))),
        }
    }

    async fn handle_send(
        self: &Arc<Self>,
        rpc: JsonRpcRequest,
        principal: ProtocolPrincipal,
        streaming: bool,
        cancellation: CancellationToken,
    ) -> ProtocolResult<ConnectionOutcome> {
        let wire = match WireSendMessageRequest::parse(rpc.params.clone()) {
            Ok(wire) => wire,
            Err(message) => return Ok(rpc_invalid_params(rpc.id, &message)),
        };
        if let Err(message) = bind_tenant(wire.tenant.as_deref(), &principal) {
            return Ok(rpc_invalid_params(rpc.id, &message));
        }
        let return_immediately = match wire.validate_configuration() {
            Ok(value) => value,
            Err(message) => return Ok(rpc_unsupported(rpc.id, &message)),
        };
        let mode = if streaming {
            A2aExecutionMode::Streaming
        } else if return_immediately {
            A2aExecutionMode::Immediate
        } else {
            A2aExecutionMode::Blocking
        };
        let response_policy = match mode {
            A2aExecutionMode::Blocking => A2aSendResponsePolicy::Blocking,
            A2aExecutionMode::Immediate => A2aSendResponsePolicy::Immediate,
            A2aExecutionMode::Streaming => A2aSendResponsePolicy::Streaming,
        };
        if let Some(media_type) =
            unsupported_message_media_type(&wire.message, &self.agent_card.default_input_modes)
        {
            return Ok(rpc_content_type_not_supported(rpc.id, &media_type));
        }
        let message = match wire.into_message() {
            Ok(message) => message,
            Err(message) => return Ok(rpc_invalid_params(rpc.id, &message)),
        };
        let correlation = match correlation_identity(&rpc) {
            Ok(value) => value,
            Err(error) => return Ok(rpc_invalid_params(rpc.id, &error.message)),
        };
        let stream_lease = if streaming {
            let owner = A2aEventOwner {
                subject: principal.subject.clone(),
                tenant_id: principal.tenant_id.clone(),
            };
            match self.stream_quota.try_acquire(owner)? {
                Some(lease) => Some(lease),
                None => return Ok(rpc_stream_capacity_exhausted(rpc.id)),
            }
        } else {
            None
        };
        // Exact retries may be read-only and bypass the mapper CAS below. Linearize them against
        // the shared store before consulting the local receipt/dispatch projection.
        {
            let _snapshot_read = self.acquire_current_snapshot_read().await?;
        }
        let duplicate_governed = {
            let mut live = self.mapper.lock().await;
            if let Some(task_id) = message.task_id.as_deref() {
                if live
                    .cancellation_for_task(task_id, &principal)
                    .is_some_and(|record| record.state != A2aCancellationOutboxState::Settled)
                {
                    let task = live.tasks().get(task_id).cloned().ok_or_else(|| {
                        ProtocolError::conflict(
                            "A2A cancellation control references a missing task",
                        )
                    })?;
                    return Ok(rpc_reconciliation_error(
                        rpc.id,
                        &task,
                        "the task has a durable cancellation intent and cannot accept messages",
                    ));
                }
            }
            if let Err(error) = live.preflight_send_message(&message, &principal) {
                return Ok(rpc_mapper_send_error(rpc.id, error));
            }
            live.message_receipt(&message.message_id, &principal)
                .is_some()
                .then(|| {
                    // Exact retries do not clone, serialize, or write the durable snapshot.
                    live.prepare_send_message_candidate_with_response_policy(
                        message.clone(),
                        correlation.clone(),
                        Some(&principal),
                        response_policy,
                    )
                })
        };
        let mut staged_live_dispatch = None;
        let governed = match duplicate_governed {
            Some(governed) => governed,
            None => {
                // The commit helper rechecks against its isolated candidate. If another request
                // accepted the same exact id before this writer entered, it becomes a no-op retry.
                let prepared_slot: Arc<StdMutex<Option<PreparedLiveDispatch>>> =
                    Arc::new(StdMutex::new(None));
                let staged_slot: Arc<StdMutex<Option<ProtocolResult<StagedLiveDispatch>>>> =
                    Arc::new(StdMutex::new(None));
                let prepared_for_mutation = prepared_slot.clone();
                let prepared_for_commit = prepared_slot;
                let staged_for_commit = staged_slot.clone();
                let stage_server = self.clone();
                let stage_message_id = message.message_id.clone();
                let stage_principal = principal.clone();
                let governed = self
                    .persist_mapper_mutation_with_post_commit(
                        move |candidate| {
                            let before_revision = candidate.revision();
                            let governed = candidate
                                .prepare_send_message_candidate_with_response_policy(
                                    message,
                                    correlation,
                                    Some(&stage_principal),
                                    response_policy,
                                );
                            if governed.is_authorized() && candidate.revision() != before_revision {
                                let record = candidate
                                    .dispatch_for_message(&stage_message_id, &stage_principal)
                                    .cloned()
                                    .ok_or_else(|| {
                                        ProtocolError::conflict(
                                            "A2A accepted live message has no durable dispatch",
                                        )
                                    })?;
                                let (envelope, action) = candidate
                                    .reconstruct_dispatch(&record.dispatch_id, &stage_principal)?;
                                *prepared_for_mutation
                                    .lock()
                                    .unwrap_or_else(std::sync::PoisonError::into_inner) =
                                    Some(PreparedLiveDispatch {
                                        dispatch_id: record.dispatch_id.clone(),
                                        job: A2aDispatchJob {
                                            durable_dispatch_id: Some(record.dispatch_id),
                                            durable_cancellation_id: None,
                                            lane: DispatchLane::Message,
                                            mode,
                                            envelope,
                                            action,
                                        },
                                        reservation: DispatchReservation {
                                            owner: A2aEventOwner {
                                                subject: record.owner_subject,
                                                tenant_id: record.owner_tenant_id,
                                            },
                                            task_id: record.task_id,
                                            message_id: record.message_id,
                                            run_id: record.run_id,
                                            lane: DispatchLane::Message,
                                            delay_start: streaming,
                                            allow_new: true,
                                        },
                                    });
                            }
                            Ok(governed)
                        },
                        Some(Box::new(move || {
                            let prepared = prepared_for_commit
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner)
                                .take();
                            if let Some(prepared) = prepared {
                                let staged = stage_server.stage_live_dispatch(prepared);
                                *staged_for_commit
                                    .lock()
                                    .unwrap_or_else(std::sync::PoisonError::into_inner) =
                                    Some(staged);
                            }
                            // The durable mapper is already installed. A staging error is returned
                            // to the request through the slot so the snapshot commit itself remains
                            // a successful, recoverable acceptance.
                            Ok(())
                        })),
                    )
                    .await?;
                staged_live_dispatch = staged_slot
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take()
                    .and_then(Result::ok);
                governed
            }
        };
        let (_, accepted_action) = match governed_action(governed, OperationKind::Send) {
            Ok(action) => action,
            Err(error) => return Ok(rpc_governance_error(rpc.id, error)),
        };
        let message_id = match &accepted_action {
            A2aAction::DispatchMessage { message, .. } => message.message_id.clone(),
            A2aAction::DuplicateMessage { receipt } => receipt.message.message_id.clone(),
            _ => return Err(ProtocolError::conflict("unexpected A2A SendMessage action")),
        };
        let live = self.mapper.lock().await;
        let record = live
            .dispatch_for_message(&message_id, &principal)
            .cloned()
            .ok_or_else(|| ProtocolError::conflict("A2A send result has no durable dispatch"))?;
        let (envelope, action) = live
            .reconstruct_dispatch(&record.dispatch_id, &principal)
            .or_else(|error| {
                if record.state == A2aDispatchOutboxState::Settled {
                    Ok((record.envelope.clone(), accepted_action.clone()))
                } else {
                    Err(error)
                }
            })?;
        let task_id = record.task_id.clone();
        let run_id = record.run_id.clone();
        let mut task = live
            .tasks()
            .get(&task_id)
            .cloned()
            .ok_or_else(|| ProtocolError::conflict("A2A send result has no task"))?;
        let owner = A2aEventOwner::from_task(&task);
        let updates = (!return_immediately || streaming).then(|| self.live.subscribe());
        drop(live);

        let event = match self.flush_pending_events_for_task(&task_id).await {
            Ok(event) => event,
            Err(_) => {
                task = self
                    .mapper
                    .lock()
                    .await
                    .tasks()
                    .get(&task_id)
                    .cloned()
                    .unwrap_or(task);
                return Ok(rpc_reconciliation_error(
                    rpc.id,
                    &task,
                    "persisting the task stream event failed",
                ));
            }
        };
        // Re-read the post-flush snapshot. A dispatch is runnable only when its exact immutable
        // acceptance event is uniquely settled; pending, quarantined, missing, or duplicate
        // bindings fail closed for both live retries and startup recovery.
        let (record, refreshed_task, cancellation_pending, event_ready) = {
            let live = self.mapper.lock().await;
            let record = live
                .dispatch_outbox()
                .get(&record.dispatch_id)
                .cloned()
                .ok_or_else(|| {
                    ProtocolError::conflict("A2A send result lost its durable dispatch")
                })?;
            let refreshed_task = live
                .tasks()
                .get(&task_id)
                .cloned()
                .ok_or_else(|| ProtocolError::conflict("A2A send result lost its task"))?;
            let cancellation_pending = live
                .cancellation_for_task(&task_id, &principal)
                .is_some_and(|record| record.state != A2aCancellationOutboxState::Settled);
            let event_ready = live.dispatch_event_ready(&record);
            (record, refreshed_task, cancellation_pending, event_ready)
        };
        task = refreshed_task;
        let otherwise_schedulable = record.state != A2aDispatchOutboxState::Settled
            && !cancellation_pending
            && !task.state.is_terminal()
            && !matches!(
                task.state,
                A2aTaskState::InputRequired | A2aTaskState::AuthRequired
            );
        if otherwise_schedulable && !event_ready {
            return Ok(rpc_reconciliation_error(
                rpc.id,
                &task,
                "the exact durable acceptance event is not uniquely settled",
            ));
        }
        let job = A2aDispatchJob {
            durable_dispatch_id: Some(record.dispatch_id.clone()),
            durable_cancellation_id: None,
            lane: DispatchLane::Message,
            mode,
            envelope,
            action,
        };
        let should_schedule = otherwise_schedulable && event_ready;
        if staged_live_dispatch
            .as_ref()
            .is_some_and(|staged| staged.dispatch_id != record.dispatch_id)
        {
            return Err(ProtocolError::conflict(
                "A2A staged live dispatch does not match its durable acceptance",
            ));
        }
        let mut dispatch_recovery_claim = None;
        let mut staged_reservation_failed = false;
        let staged_acceptance = if should_schedule {
            if let Some(staged) = staged_live_dispatch.take() {
                dispatch_recovery_claim = Some(staged.recovery_claim);
                staged_reservation_failed = staged.acceptance.is_none();
                staged.acceptance
            } else {
                None
            }
        } else {
            drop(staged_live_dispatch.take());
            None
        };
        let mut acceptance = if should_schedule {
            if staged_reservation_failed {
                return Ok(ConnectionOutcome::Response(HttpResponse::json(
                    503,
                    jsonrpc_error(rpc.id, -32603, "Dispatch capacity exhausted", None),
                )));
            }
            match staged_acceptance {
                Some(acceptance) => Some(acceptance),
                None => match self.reserve_dispatch(
                    job,
                    DispatchReservation {
                        owner: owner.clone(),
                        task_id: task_id.clone(),
                        message_id: message_id.clone(),
                        run_id: run_id.clone(),
                        lane: DispatchLane::Message,
                        delay_start: streaming,
                        allow_new: true,
                    },
                ) {
                    Ok(acceptance) => acceptance,
                    Err(_) => {
                        return Ok(ConnectionOutcome::Response(HttpResponse::json(
                            503,
                            jsonrpc_error(rpc.id, -32603, "Dispatch capacity exhausted", None),
                        )))
                    }
                },
            }
        } else {
            None
        };
        let mut reconciliation = Vec::new();
        let mut retain_recovery_claim = false;
        if acceptance
            .as_ref()
            .is_some_and(|acceptance| acceptance.newly_reserved)
        {
            if dispatch_recovery_claim.is_none() {
                dispatch_recovery_claim = self.claim_recovery_attempt(&record.dispatch_id);
            }
            if dispatch_recovery_claim.is_none() {
                reconciliation.push("dispatch recovery ownership is already claimed".to_owned());
            }
            if reconciliation.is_empty()
                && matches!(
                    record.state,
                    A2aDispatchOutboxState::Running | A2aDispatchOutboxState::ReconcilePending
                )
            {
                let decision = self
                    .reconcile_unknown_dispatch(
                        &record,
                        &cancellation,
                        Instant::now() + self.config.dispatch_ack_timeout,
                    )
                    .await;
                if decision != A2aUnknownDispatchDecision::SafeToRetry {
                    retain_recovery_claim = true;
                    if record.state == A2aDispatchOutboxState::Running
                        && self
                            .mark_dispatch_reconcile_pending(
                                &record.dispatch_id,
                                "unknown external outcome requires host reconciliation",
                            )
                            .await
                            .is_err()
                    {
                        retain_recovery_claim = false;
                    }
                    reconciliation.push(
                        "dispatch outcome is unknown and the host did not attest a safe retry"
                            .to_owned(),
                    );
                }
            }
            if record.state == A2aDispatchOutboxState::Running
                && reconciliation.is_empty()
                && self
                    .mark_dispatch_reconcile_pending(
                        &record.dispatch_id,
                        "recovering a dispatch with unknown outcome",
                    )
                    .await
                    .is_err()
            {
                reconciliation.push("persisting dispatch reconciliation failed".to_owned());
            }
        }
        if reconciliation.is_empty() {
            if let Some(commit) = acceptance
                .as_mut()
                .and_then(|acceptance| acceptance.commit.take())
            {
                let Some(recovery_claim) = dispatch_recovery_claim.take() else {
                    reconciliation.push("dispatch recovery ownership disappeared".to_owned());
                    return Ok(rpc_reconciliation_error(
                        rpc.id,
                        &task,
                        &reconciliation.join("; "),
                    ));
                };
                if self
                    .mark_dispatch_running_with_commit(&record.dispatch_id, commit, recovery_claim)
                    .await
                    .is_err()
                {
                    reconciliation.push(
                        "persisting or committing the running dispatch fence failed".to_owned(),
                    );
                }
            }
        }
        if !reconciliation.is_empty() {
            if retain_recovery_claim {
                if let Some(claim) = dispatch_recovery_claim.take() {
                    claim.quarantine();
                }
            }
            return Ok(rpc_reconciliation_error(
                rpc.id,
                &task,
                &reconciliation.join("; "),
            ));
        }
        if mode == A2aExecutionMode::Immediate {
            return Ok(ConnectionOutcome::Response(HttpResponse::json(
                200,
                jsonrpc_result(rpc.id, send_result_value(&task, &record)),
            )));
        }
        if mode == A2aExecutionMode::Blocking {
            if let Some(acceptance) = acceptance {
                match self
                    .wait_for_dispatch_acceptance(
                        acceptance.completion,
                        updates.expect("blocking dispatch has a task update receiver"),
                        &task_id,
                        &owner,
                        cancellation,
                    )
                    .await
                {
                    Ok(completed) => task = completed,
                    Err(error) => {
                        task = self
                            .mapper
                            .lock()
                            .await
                            .tasks()
                            .get(&task_id)
                            .cloned()
                            .unwrap_or(task);
                        reconciliation.push(format!("host dispatch failed: {}", error.message));
                    }
                }
            }
            if !reconciliation.is_empty() {
                return Ok(rpc_reconciliation_error(
                    rpc.id,
                    &task,
                    &reconciliation.join("; "),
                ));
            }
            let result_record = self
                .mapper
                .lock()
                .await
                .dispatch_outbox()
                .get(&record.dispatch_id)
                .cloned()
                .ok_or_else(|| ProtocolError::conflict("A2A completed send lost its response"))?;
            return Ok(ConnectionOutcome::Response(HttpResponse::json(
                200,
                jsonrpc_result(rpc.id, send_result_value(&task, &result_record)),
            )));
        }
        let dispatch_start = acceptance.and_then(|acceptance| acceptance.start);
        let settled_stream_response = (record.state == A2aDispatchOutboxState::Settled
            && task.state.is_terminal()
            && record.immediate_response.is_none())
        .then(|| match &record.response {
            A2aDispatchResponse::Message { message } => A2aStreamResponse::message(message),
            A2aDispatchResponse::Task { artifacts, .. } => A2aStreamResponse::Task(
                A2aWireTask::from_task(&task, artifacts, !artifacts.is_empty()),
            ),
        });
        let plan = SsePlan {
            id: rpc.id,
            owner,
            task_id,
            context_id: task.mapping.context_id.clone(),
            initial: event
                .map(|value| (Some(value.event_id), value.response))
                .or_else(|| settled_stream_response.map(|response| (None, response)))
                .or_else(|| Some((None, A2aStreamResponse::task(&task)))),
            replay: Vec::new(),
            receiver: updates.expect("stream receiver is created for streaming sends"),
            dispatch_start,
            defer_initial_until_response: !task.state.is_terminal(),
            last_event_id: None,
            _stream_lease: stream_lease.expect("streaming send acquired a stream lease"),
        };
        Ok(ConnectionOutcome::Stream(Box::new(plan)))
    }

    fn stage_live_dispatch(
        self: &Arc<Self>,
        prepared: PreparedLiveDispatch,
    ) -> ProtocolResult<StagedLiveDispatch> {
        let recovery_claim = self
            .claim_recovery_attempt(&prepared.dispatch_id)
            .ok_or_else(|| {
                ProtocolError::conflict(
                    "A2A live dispatch lost ownership before its post-commit reservation",
                )
            })?;
        // Capacity can be transient between durable acceptance and event settlement. Keep the
        // exact claim even when staging cannot reserve: the request returns a retryable capacity
        // response after flushing the immutable acceptance event, and recovery cannot steal the
        // execution mode while that request still owns the acceptance.
        let acceptance = self
            .reserve_dispatch(prepared.job, prepared.reservation)
            .ok()
            .flatten()
            .filter(|acceptance| acceptance.newly_reserved);
        Ok(StagedLiveDispatch {
            dispatch_id: prepared.dispatch_id,
            acceptance,
            recovery_claim,
        })
    }

    fn reserve_dispatch(
        self: &Arc<Self>,
        job: A2aDispatchJob,
        reservation: DispatchReservation,
    ) -> ProtocolResult<Option<DispatchAcceptance>> {
        let DispatchReservation {
            owner,
            task_id,
            message_id,
            run_id,
            lane,
            delay_start,
            allow_new,
        } = reservation;
        if job.lane != lane {
            return Err(ProtocolError::conflict(
                "A2A dispatch job and reservation lane differ",
            ));
        }
        let task_key = DispatchTaskKey {
            owner: owner.clone(),
            task_id: task_id.clone(),
            lane,
        };
        let message_key = DispatchMessageKey {
            owner: owner.clone(),
            task_id,
            message_id,
            run_id,
            lane,
        };
        let mut state = self
            .dispatch_state
            .lock()
            .map_err(|_| ProtocolError::conflict("A2A dispatch scheduler lock poisoned"))?;
        if let Some(inflight) = state.inflight_messages.get(&message_key) {
            return Ok(Some(DispatchAcceptance {
                completion: inflight.completion.subscribe(),
                commit: None,
                start: None,
                newly_reserved: false,
            }));
        }
        if !allow_new {
            return Ok(None);
        }
        let owner_key = DispatchOwnerKey {
            owner: owner.clone(),
            lane,
        };
        let owner_accepted = state.per_owner.get(&owner_key).copied().unwrap_or_default();
        let lane_accepted = state
            .accepted_by_lane
            .get(&lane)
            .copied()
            .unwrap_or_default();
        let (max_accepted, max_owner_accepted, max_owner_running) = match lane {
            DispatchLane::Message => (
                self.config
                    .max_background_dispatches
                    .saturating_add(self.config.max_queued_dispatches),
                self.config
                    .max_background_dispatches_per_owner
                    .saturating_add(self.config.max_queued_dispatches_per_owner),
                self.config.max_background_dispatches_per_owner,
            ),
            DispatchLane::Cancellation => (
                self.config
                    .max_control_dispatches
                    .saturating_add(self.config.max_queued_control_dispatches),
                self.config
                    .max_control_dispatches_per_owner
                    .saturating_add(self.config.max_queued_control_dispatches_per_owner),
                self.config.max_control_dispatches_per_owner,
            ),
        };
        if lane_accepted >= max_accepted || owner_accepted >= max_owner_accepted {
            return Err(ProtocolError::conflict(
                "A2A dispatch scheduler capacity is exhausted",
            ));
        }
        let task_semaphore = {
            let entry = state
                .task_semaphores
                .entry(task_key.clone())
                .or_insert_with(|| (Arc::new(Semaphore::new(1)), 0));
            entry.1 += 1;
            entry.0.clone()
        };
        let owner_semaphore = {
            let entry = state
                .owner_semaphores
                .entry(owner_key.clone())
                .or_insert_with(|| (Arc::new(Semaphore::new(max_owner_running)), 0));
            entry.1 += 1;
            entry.0.clone()
        };
        let (completion, completion_receiver) = broadcast::channel(1);
        let (commit_sender, commit) = oneshot::channel();
        let (start_sender, start) = if delay_start {
            let (sender, receiver) = oneshot::channel();
            (Some(sender), Some(receiver))
        } else {
            (None, None)
        };
        state.accepted += 1;
        *state.accepted_by_lane.entry(lane).or_default() += 1;
        *state.per_owner.entry(owner_key).or_default() += 1;
        state.inflight_messages.insert(
            message_key.clone(),
            InflightDispatch {
                completion: completion.clone(),
            },
        );
        let scheduled = ScheduledA2aDispatch {
            job,
            owner: owner.clone(),
            task_key: task_key.clone(),
            message_key: message_key.clone(),
            task_semaphore,
            owner_semaphore,
            completion,
            commit,
            start,
        };
        let send_failed = match lane {
            DispatchLane::Message => self.dispatch_sender.try_send(scheduled).is_err(),
            DispatchLane::Cancellation => self.control_sender.try_send(scheduled).is_err(),
        };
        if send_failed {
            Self::remove_dispatch_state(&mut state, &owner, &task_key, &message_key);
            return Err(ProtocolError::conflict(
                "A2A dispatch scheduler queue is full or unavailable",
            ));
        }
        Ok(Some(DispatchAcceptance {
            completion: completion_receiver,
            commit: Some(commit_sender),
            start: start_sender,
            newly_reserved: true,
        }))
    }

    async fn message_execution_is_current(
        &self,
        job: &A2aDispatchJob,
        task_id: &str,
        owner: &A2aEventOwner,
        expected_attempt: Option<u32>,
    ) -> ProtocolResult<bool> {
        let dispatch_id = job.durable_dispatch_id.as_deref().ok_or_else(|| {
            ProtocolError::conflict("A2A message job has no durable dispatch identity")
        })?;
        let expected_attempt = expected_attempt.ok_or_else(|| {
            ProtocolError::conflict("A2A message job has no committed dispatch generation")
        })?;
        let principal =
            job.envelope.principal.as_ref().ok_or_else(|| {
                ProtocolError::conflict("A2A message job has no governed principal")
            })?;
        let mapper = self.mapper.lock().await;
        let task = mapper
            .tasks()
            .get(task_id)
            .filter(|task| {
                task.owner_subject == owner.subject && task.owner_tenant_id == owner.tenant_id
            })
            .ok_or_else(|| ProtocolError::not_found("A2A dispatch task is not registered"))?;
        let record = mapper
            .dispatch_outbox()
            .get(dispatch_id)
            .ok_or_else(|| ProtocolError::conflict("A2A durable dispatch is not registered"))?;
        if task.state.is_terminal()
            || matches!(
                task.state,
                A2aTaskState::InputRequired | A2aTaskState::AuthRequired
            )
            || record.state == A2aDispatchOutboxState::Settled
        {
            return Ok(false);
        }
        if mapper
            .cancellation_for_task(task_id, principal)
            .is_some_and(|record| record.state != A2aCancellationOutboxState::Settled)
        {
            return Err(ProtocolError::conflict(
                "A2A message dispatch was superseded by a durable cancellation intent",
            ));
        }
        let A2aAction::DispatchMessage {
            message, mapping, ..
        } = &job.action
        else {
            return Err(ProtocolError::conflict(
                "A2A message lane contains a non-message action",
            ));
        };
        if record.state != A2aDispatchOutboxState::Running
            || record.attempts != expected_attempt
            || record.owner_subject != owner.subject
            || record.owner_tenant_id != owner.tenant_id
            || record.task_id != task_id
            || record.message_id != message.message_id
            || record.context_id != mapping.context_id
            || record.session_id != mapping.session_id
            || record.run_id != mapping.run_id
            || record.envelope != job.envelope
        {
            return Err(ProtocolError::conflict(
                "A2A message dispatch generation or durable fence is stale",
            ));
        }
        Ok(true)
    }

    async fn cancellation_execution_is_current(
        &self,
        job: &A2aDispatchJob,
        task_id: &str,
        owner: &A2aEventOwner,
        expected_attempt: Option<u32>,
    ) -> ProtocolResult<bool> {
        let cancellation_id = job.durable_cancellation_id.as_deref().ok_or_else(|| {
            ProtocolError::conflict("A2A cancellation job has no durable control identity")
        })?;
        let expected_attempt = expected_attempt.ok_or_else(|| {
            ProtocolError::conflict("A2A cancellation job has no committed control generation")
        })?;
        let principal = job.envelope.principal.as_ref().ok_or_else(|| {
            ProtocolError::conflict("A2A cancellation job has no governed principal")
        })?;
        if principal.subject != owner.subject || principal.tenant_id != owner.tenant_id {
            return Err(ProtocolError::conflict(
                "A2A cancellation job owner binding is invalid",
            ));
        }
        let mapper = self.mapper.lock().await;
        let task = mapper
            .tasks()
            .get(task_id)
            .filter(|task| {
                task.owner_subject == owner.subject && task.owner_tenant_id == owner.tenant_id
            })
            .ok_or_else(|| ProtocolError::not_found("A2A cancellation task is not registered"))?;
        let record = mapper
            .cancellation_for_task(task_id, principal)
            .ok_or_else(|| ProtocolError::conflict("A2A cancellation control is not registered"))?;
        if task.state.is_terminal() || record.state == A2aCancellationOutboxState::Settled {
            return Ok(false);
        }
        let A2aAction::CancelTask { task: action_task } = &job.action else {
            return Err(ProtocolError::conflict(
                "A2A cancellation lane contains a non-cancel action",
            ));
        };
        if record.cancellation_id != cancellation_id
            || record.state != A2aCancellationOutboxState::Running
            || record.attempts != expected_attempt
            || record.owner_subject != owner.subject
            || record.owner_tenant_id != owner.tenant_id
            || record.task_id != task_id
            || record.task.mapping.task_id != action_task.mapping.task_id
            || record.context_id != action_task.mapping.context_id
            || record.session_id != action_task.mapping.session_id
            || record.run_id != action_task.mapping.run_id
            || record.envelope != job.envelope
            || task.status_message.as_deref() != Some("cancellation requested")
        {
            return Err(ProtocolError::conflict(
                "A2A cancellation control generation or durable fence is stale",
            ));
        }
        Ok(true)
    }

    async fn invoke_scheduled_host(
        self: &Arc<Self>,
        job: &A2aDispatchJob,
        task_id: &str,
        owner: &A2aEventOwner,
        commit: &DispatchCommit,
        server_cancellation: CancellationToken,
    ) -> ProtocolResult<()> {
        if job.lane == DispatchLane::Message
            && !self
                .message_execution_is_current(job, task_id, owner, commit.expected_dispatch_attempt)
                .await?
        {
            return Ok(());
        }
        if job.lane == DispatchLane::Cancellation
            && !self
                .cancellation_execution_is_current(
                    job,
                    task_id,
                    owner,
                    commit.expected_cancellation_attempt,
                )
                .await?
        {
            return Ok(());
        }
        let host_cancellation = CancellationToken::new();
        let runtime_key = if job.lane == DispatchLane::Message {
            Some(self.register_running_dispatch_cancellation(
                owner,
                task_id,
                host_cancellation.clone(),
            )?)
        } else {
            None
        };
        if job.lane == DispatchLane::Message {
            match self
                .message_execution_is_current(job, task_id, owner, commit.expected_dispatch_attempt)
                .await
            {
                Ok(true) => {}
                Ok(false) => {
                    if let Some(key) = &runtime_key {
                        self.unregister_running_dispatch_cancellation(key);
                    }
                    return Ok(());
                }
                Err(error) => {
                    host_cancellation.cancel();
                    if let Some(key) = &runtime_key {
                        self.unregister_running_dispatch_cancellation(key);
                    }
                    return Err(error);
                }
            }
        }
        let dispatch_fence = if job.lane == DispatchLane::Message {
            Some(A2aDispatchFence {
                dispatch_id: job.durable_dispatch_id.clone().ok_or_else(|| {
                    ProtocolError::conflict("A2A message host context has no durable dispatch")
                })?,
                expected_attempt: commit.expected_dispatch_attempt.ok_or_else(|| {
                    ProtocolError::conflict("A2A message host context has no dispatch generation")
                })?,
            })
        } else {
            None
        };
        let context = A2aDispatchContext {
            mode: job.mode,
            cancellation: host_cancellation.clone(),
            dispatch_fence,
        };
        let mut host_call = Box::pin(A2aUntrustedCallback::new(async {
            self.host
                .handle(self.clone(), &context, &job.envelope, &job.action)
                .await
        }));
        let execution_timeout = match job.lane {
            DispatchLane::Message => self.config.background_dispatch_timeout,
            DispatchLane::Cancellation => self.config.blocking_dispatch_timeout,
        };
        let mut execution_timeout = Box::pin(tokio::time::sleep(execution_timeout));
        let interrupted = tokio::select! {
            result = &mut host_call => Some(result),
            () = server_cancellation.cancelled() => {
                host_cancellation.cancel();
                None
            }
            () = &mut execution_timeout => {
                host_cancellation.cancel();
                None
            }
        };
        let acknowledgement = match interrupted {
            Some(Ok(result)) => result,
            Some(Err(())) => Err(ProtocolError::conflict(
                "A2A host panicked; dispatch outcome requires reconciliation",
            )),
            None => match timeout(self.config.dispatch_ack_timeout, &mut host_call).await {
                Ok(Ok(result)) => result,
                Ok(Err(())) => Err(ProtocolError::conflict(
                    "A2A host panicked; dispatch outcome requires reconciliation",
                )),
                Err(_) => Err(ProtocolError::conflict(
                    "A2A host did not acknowledge the dispatch cancellation fence; reconciliation is pending",
                )),
            },
        };
        let acknowledgement = if host_call.as_mut().get_mut().finish_drop().is_err() {
            Err(ProtocolError::conflict(
                "A2A host callback panicked while being dropped; outcome requires reconciliation",
            ))
        } else {
            acknowledgement
        };
        if let Some(key) = &runtime_key {
            self.unregister_running_dispatch_cancellation(key);
        }
        match acknowledgement {
            Ok(ack) => {
                self.apply_dispatch_ack(job, task_id, owner, commit, ack)
                    .await
            }
            Err(error) => Err(error),
        }
    }

    async fn run_scheduled_dispatch(
        self: Arc<Self>,
        scheduled: ScheduledA2aDispatch,
        server_cancellation: CancellationToken,
    ) -> ScheduledDispatchCompletion {
        // A reservation reaches this queue before its mapper/event persistence. Always observe the
        // commit gate itself: shutdown may race the accepting connection, and treating that race as
        // cancellation could strand a snapshot accepted a few instructions later.
        let mut commit = match scheduled.commit.await {
            Ok(commit) => commit,
            Err(_) => {
                let _ = scheduled.completion.send(Err(ProtocolError::new(
                    ProtocolErrorCode::Cancelled,
                    "A2A dispatch reservation was not committed",
                )));
                return ScheduledDispatchCompletion {
                    owner: scheduled.owner,
                    task_key: scheduled.task_key,
                    message_key: scheduled.message_key,
                    recovery_claim: ScheduledRecoveryClaim::None,
                };
            }
        };

        let task_id = scheduled.task_key.task_id.clone();
        let owner = scheduled.owner.clone();
        let durable_dispatch_id = scheduled.job.durable_dispatch_id.clone();
        let durable_cancellation_id = scheduled.job.durable_cancellation_id.clone();
        let lane = scheduled.job.lane;
        let recovery_claim = match lane {
            DispatchLane::Message => commit
                .dispatch_recovery_claim
                .take()
                .map_or(ScheduledRecoveryClaim::None, |claim| {
                    ScheduledRecoveryClaim::Dispatch { claim }
                }),
            DispatchLane::Cancellation => durable_cancellation_id.as_ref().map_or(
                ScheduledRecoveryClaim::None,
                |cancellation_id| ScheduledRecoveryClaim::Cancellation {
                    cancellation_id: cancellation_id.clone(),
                },
            ),
        };
        let acquire_gates = async {
            if let Some(start) = scheduled.start {
                start.await.map_err(|_| {
                    ProtocolError::conflict("A2A streaming dispatch start gate was dropped")
                })?;
            }
            let _task_permit = scheduled
                .task_semaphore
                .acquire_owned()
                .await
                .map_err(|_| ProtocolError::conflict("A2A task dispatch gate closed"))?;
            let _owner_permit = scheduled
                .owner_semaphore
                .acquire_owned()
                .await
                .map_err(|_| ProtocolError::conflict("A2A owner dispatch gate closed"))?;
            let global = match lane {
                DispatchLane::Message => self.dispatch_global.clone(),
                DispatchLane::Cancellation => self.control_global.clone(),
            };
            let _global_permit = global
                .acquire_owned()
                .await
                .map_err(|_| ProtocolError::conflict("A2A global dispatch gate closed"))?;
            Ok::<_, ProtocolError>((_task_permit, _owner_permit, _global_permit))
        };
        let result = if commit.host_already_fenced {
            if lane == DispatchLane::Cancellation {
                Ok(())
            } else {
                Err(ProtocolError::conflict(
                    "A2A message dispatch cannot bypass its host fence",
                ))
            }
        } else {
            let gates = tokio::select! {
                () = server_cancellation.cancelled() => Err(ProtocolError::new(
                    ProtocolErrorCode::Cancelled,
                    "A2A queued dispatch was interrupted during server shutdown",
                )),
                result = timeout(
                    match lane {
                        DispatchLane::Message => self.config.background_dispatch_timeout,
                        DispatchLane::Cancellation => self.config.blocking_dispatch_timeout,
                    },
                    acquire_gates,
                ) => {
                    result.unwrap_or_else(|_| Err(ProtocolError::conflict(
                        "A2A dispatch timed out while waiting for a scheduler gate",
                    )))
                }
            };
            match gates {
                Err(error) => Err(error),
                Ok(_permits) => {
                    self.invoke_scheduled_host(
                        &scheduled.job,
                        &task_id,
                        &owner,
                        &commit,
                        server_cancellation,
                    )
                    .await
                }
            }
        };
        let result = match (lane, result) {
            (DispatchLane::Cancellation, result) => {
                let Some(cancellation_id) = durable_cancellation_id else {
                    let result = Err(ProtocolError::conflict(
                        "A2A cancellation job has no durable control identity",
                    ));
                    let _ = scheduled.completion.send(result);
                    return ScheduledDispatchCompletion {
                        owner: scheduled.owner,
                        task_key: scheduled.task_key,
                        message_key: scheduled.message_key,
                        recovery_claim: ScheduledRecoveryClaim::None,
                    };
                };
                match result {
                    Ok(()) => self.mark_cancellation_settled(&cancellation_id).await,
                    Err(_) => {
                        let cancellation_reconcile = self
                            .mark_cancellation_reconcile_pending(
                                &cancellation_id,
                                "host cancellation outcome is unknown",
                            )
                            .await;
                        let dispatch_reconcile =
                            self.mark_task_dispatches_reconcile_pending(&task_id).await;
                        match (cancellation_reconcile, dispatch_reconcile) {
                            (Ok(()), Ok(())) => Err(ProtocolError::conflict(
                                "A2A host cancellation did not settle; reconciliation is pending",
                            )),
                            (Err(error), _) | (_, Err(error)) => Err(error),
                        }
                    }
                }
            }
            (DispatchLane::Message, result) => {
                let Some(dispatch_id) = durable_dispatch_id else {
                    let result = Err(ProtocolError::conflict(
                        "A2A message job has no durable dispatch identity",
                    ));
                    let _ = scheduled.completion.send(result);
                    return ScheduledDispatchCompletion {
                        owner: scheduled.owner,
                        task_key: scheduled.task_key,
                        message_key: scheduled.message_key,
                        recovery_claim: ScheduledRecoveryClaim::None,
                    };
                };
                match result {
                    Ok(()) => self.mark_dispatch_settled(&dispatch_id).await,
                    Err(_) => {
                        let reconcile = self
                            .mark_dispatch_reconcile_pending(
                                &dispatch_id,
                                "host outcome is unknown and requires reconciliation",
                            )
                            .await;
                        match reconcile {
                            Ok(()) => Err(ProtocolError::conflict(
                                "A2A host dispatch did not settle; reconciliation is pending",
                            )),
                            Err(mark_error) => {
                                let settled = self
                                    .mapper
                                    .lock()
                                    .await
                                    .dispatch_outbox()
                                    .get(&dispatch_id)
                                    .is_some_and(|record| {
                                        record.state == A2aDispatchOutboxState::Settled
                                    });
                                if settled {
                                    Ok(())
                                } else {
                                    Err(ProtocolError::conflict(format!(
                                        "A2A dispatch reconciliation persistence failed: {}",
                                        mark_error.message
                                    )))
                                }
                            }
                        }
                    }
                }
            }
        };
        let _ = scheduled.completion.send(result);
        ScheduledDispatchCompletion {
            owner: scheduled.owner,
            task_key: scheduled.task_key,
            message_key: scheduled.message_key,
            recovery_claim,
        }
    }

    async fn wait_for_task_settlement(
        &self,
        task_id: &str,
        owner: &A2aEventOwner,
        receiver: &mut broadcast::Receiver<LiveEvent>,
    ) -> ProtocolResult<A2aTaskRecord> {
        loop {
            let current = self
                .mapper
                .lock()
                .await
                .tasks()
                .get(task_id)
                .filter(|task| {
                    task.owner_subject == owner.subject && task.owner_tenant_id == owner.tenant_id
                })
                .cloned()
                .ok_or_else(|| ProtocolError::conflict("accepted A2A task disappeared"))?;
            if !matches!(
                current.state,
                A2aTaskState::Submitted | A2aTaskState::Working
            ) {
                return Ok(current);
            }
            match receiver.recv().await {
                Ok(LiveEvent(event)) if event.task_id == task_id && event.owner == *owner => {}
                Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => {}
                Err(broadcast::error::RecvError::Closed) => {
                    return Err(ProtocolError::conflict(
                        "A2A task update stream closed before settlement",
                    ));
                }
            }
        }
    }

    async fn wait_for_dispatch_acceptance(
        &self,
        mut completion: broadcast::Receiver<ProtocolResult<()>>,
        mut updates: broadcast::Receiver<LiveEvent>,
        task_id: &str,
        owner: &A2aEventOwner,
        cancellation: CancellationToken,
    ) -> ProtocolResult<A2aTaskRecord> {
        let wait = async {
            let dispatch_result = loop {
                match completion.recv().await {
                    Ok(result) => break result,
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => {
                        break Err(ProtocolError::conflict(
                            "A2A dispatch completion channel closed",
                        ))
                    }
                }
            };
            dispatch_result?;
            self.wait_for_task_settlement(task_id, owner, &mut updates)
                .await
        };
        tokio::select! {
            () = cancellation.cancelled() => Err(ProtocolError::new(
                ProtocolErrorCode::Cancelled,
                "A2A blocking dispatch wait was cancelled",
            )),
            result = timeout(self.config.blocking_dispatch_timeout, wait) => {
                result.unwrap_or_else(|_| Err(ProtocolError::conflict(
                    "A2A blocking dispatch timed out before task interruption",
                )))
            }
        }
    }

    async fn apply_dispatch_ack(
        &self,
        job: &A2aDispatchJob,
        task_id: &str,
        owner: &A2aEventOwner,
        commit: &DispatchCommit,
        acknowledgement: A2aDispatchAck,
    ) -> ProtocolResult<()> {
        let current = self
            .mapper
            .lock()
            .await
            .tasks()
            .get(task_id)
            .filter(|task| {
                task.owner_subject == owner.subject && task.owner_tenant_id == owner.tenant_id
            })
            .cloned()
            .ok_or_else(|| ProtocolError::not_found("A2A task is not registered"))?;
        match acknowledgement {
            A2aDispatchAck::Settled => {
                if job.lane == DispatchLane::Cancellation {
                    let cancellation_id =
                        job.durable_cancellation_id.as_deref().ok_or_else(|| {
                            ProtocolError::conflict(
                                "A2A cancellation settlement has no durable control identity",
                            )
                        })?;
                    let expected_attempt =
                        commit.expected_cancellation_attempt.ok_or_else(|| {
                            ProtocolError::conflict(
                                "A2A cancellation settlement has no control generation",
                            )
                        })?;
                    let mapper = self.mapper.lock().await;
                    let record = mapper
                        .cancellation_for_task(
                            task_id,
                            job.envelope.principal.as_ref().ok_or_else(|| {
                                ProtocolError::conflict(
                                    "A2A cancellation settlement has no governed principal",
                                )
                            })?,
                        )
                        .filter(|record| {
                            record.cancellation_id == cancellation_id
                                && record.attempts == expected_attempt
                                && record.state == A2aCancellationOutboxState::Settled
                        });
                    if record.is_none() || current.state != A2aTaskState::Cancelled {
                        return Err(ProtocolError::conflict(
                            "A2A host settlement did not prove the exact durable cancellation generation",
                        ));
                    }
                    return Ok(());
                }
                if matches!(
                    current.state,
                    A2aTaskState::Submitted | A2aTaskState::Working
                ) {
                    return Err(ProtocolError::conflict(
                        "A2A host acknowledged settlement while the task remained active; reconciliation is pending",
                    ));
                }
                Ok(())
            }
            A2aDispatchAck::Stopped => {
                if job.lane == DispatchLane::Cancellation {
                    let cancellation_id =
                        job.durable_cancellation_id.as_deref().ok_or_else(|| {
                            ProtocolError::conflict(
                                "A2A stopped acknowledgement has no durable cancellation identity",
                            )
                        })?;
                    let expected_attempt =
                        commit.expected_cancellation_attempt.ok_or_else(|| {
                            ProtocolError::conflict(
                                "A2A stopped acknowledgement has no cancellation generation",
                            )
                        })?;
                    self.acknowledge_cancellation_fence(
                        cancellation_id,
                        expected_attempt,
                        task_id,
                        "host acknowledged the cancellation fence",
                    )
                    .await?;
                    return Ok(());
                }
                let has_pending_cancellation = self
                    .mapper
                    .lock()
                    .await
                    .pending_cancellations()
                    .into_iter()
                    .any(|record| record.task_id == task_id);
                if has_pending_cancellation {
                    return Err(ProtocolError::conflict(
                        "A2A message stop cannot settle a durable cancellation control",
                    ));
                }
                if current.state.is_terminal() {
                    return Ok(());
                }
                let fence = A2aDispatchFence {
                    dispatch_id: job.durable_dispatch_id.clone().ok_or_else(|| {
                        ProtocolError::conflict(
                            "A2A stopped message acknowledgement has no durable dispatch identity",
                        )
                    })?,
                    expected_attempt: commit.expected_dispatch_attempt.ok_or_else(|| {
                        ProtocolError::conflict(
                            "A2A stopped message acknowledgement has no dispatch generation",
                        )
                    })?,
                };
                self.transition_dispatch_fence(
                    &fence,
                    A2aTaskState::Cancelled,
                    Some("host acknowledged the cancellation fence".to_owned()),
                )
                .await
                .map(|_| ())
            }
        }
    }

    fn finish_scheduled_dispatch(
        &self,
        completion: ScheduledDispatchCompletion,
    ) -> ProtocolResult<()> {
        let mut state = self
            .dispatch_state
            .lock()
            .map_err(|_| ProtocolError::conflict("A2A dispatch scheduler lock poisoned"))?;
        Self::remove_dispatch_state(
            &mut state,
            &completion.owner,
            &completion.task_key,
            &completion.message_key,
        );
        drop(state);
        match completion.recovery_claim {
            ScheduledRecoveryClaim::None => {}
            ScheduledRecoveryClaim::Dispatch { claim } => drop(claim),
            ScheduledRecoveryClaim::Cancellation { cancellation_id } => {
                self.release_recovery_attempt(&cancellation_id);
                #[cfg(test)]
                if let Some(hook) = self
                    .cancellation_claim_release_hook
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone()
                {
                    hook.notify_one();
                }
            }
        }
        Ok(())
    }

    fn remove_dispatch_state(
        state: &mut DispatchSchedulerState,
        owner: &A2aEventOwner,
        task_key: &DispatchTaskKey,
        message_key: &DispatchMessageKey,
    ) {
        let lane = task_key.lane;
        let owner_key = DispatchOwnerKey {
            owner: owner.clone(),
            lane,
        };
        state.inflight_messages.remove(message_key);
        state.accepted = state.accepted.saturating_sub(1);
        if let Some(count) = state.accepted_by_lane.get_mut(&lane) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                state.accepted_by_lane.remove(&lane);
            }
        }
        if let Some(count) = state.per_owner.get_mut(&owner_key) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                state.per_owner.remove(&owner_key);
            }
        }
        if let Some((_, count)) = state.task_semaphores.get_mut(task_key) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                state.task_semaphores.remove(task_key);
            }
        }
        if let Some((_, count)) = state.owner_semaphores.get_mut(&owner_key) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                state.owner_semaphores.remove(&owner_key);
            }
        }
    }

    async fn handle_get_task(
        &self,
        rpc: JsonRpcRequest,
        principal: ProtocolPrincipal,
    ) -> ProtocolResult<ConnectionOutcome> {
        let wire = match WireGetTaskRequest::parse(rpc.params.clone()) {
            Ok(wire) => wire,
            Err(message) => return Ok(rpc_invalid_params(rpc.id, &message)),
        };
        if let Err(message) = bind_tenant(wire.tenant.as_deref(), &principal) {
            return Ok(rpc_invalid_params(rpc.id, &message));
        }
        // The canonical mapper stores no message history. Omitting the field returns zero
        // messages, which is within every validated non-negative historyLength limit.
        let _history_limit = wire.history_length;
        let correlation = match correlation_identity(&rpc) {
            Ok(value) => value,
            Err(error) => return Ok(rpc_invalid_params(rpc.id, &error.message)),
        };
        let _snapshot_read = self.acquire_current_snapshot_read().await?;
        let mapper = self.mapper.lock().await;
        let action = mapper.prepare_get_task(&wire.id, correlation, Some(&principal));
        let (_, action) = match governed_action(action, OperationKind::Get) {
            Ok(action) => action,
            Err(error) => return Ok(rpc_governance_error(rpc.id, error)),
        };
        let A2aAction::GetTask { task } = action else {
            return Err(ProtocolError::conflict("unexpected A2A GetTask action"));
        };
        let artifacts = mapper.artifacts_for_task(&task.mapping.task_id);
        Ok(ConnectionOutcome::Response(HttpResponse::json(
            200,
            jsonrpc_result(
                rpc.id,
                json!(A2aWireTask::from_task(
                    &task,
                    artifacts,
                    !artifacts.is_empty(),
                )),
            ),
        )))
    }

    async fn handle_list_tasks(
        &self,
        rpc: JsonRpcRequest,
        principal: ProtocolPrincipal,
    ) -> ProtocolResult<ConnectionOutcome> {
        let wire = match WireListTasksRequest::parse(rpc.params.clone()) {
            Ok(wire) => wire,
            Err(message) => return Ok(rpc_invalid_params(rpc.id, &message)),
        };
        if let Err(message) = bind_tenant(wire.tenant.as_deref(), &principal) {
            return Ok(rpc_invalid_params(rpc.id, &message));
        }
        let _history_limit = wire.history_length;
        if wire.status_timestamp_after.is_some() {
            return Ok(rpc_unsupported(
                rpc.id,
                "statusTimestampAfter is not available",
            ));
        }
        let include_artifacts = wire.include_artifacts.unwrap_or(false);
        let correlation = match correlation_identity(&rpc) {
            Ok(value) => value,
            Err(error) => return Ok(rpc_invalid_params(rpc.id, &error.message)),
        };
        let request = A2aListTasksRequest {
            tenant: wire.tenant,
            context_id: wire.context_id,
            status: wire.status,
            page_size: wire.page_size,
            page_token: wire.page_token,
        };
        let _snapshot_read = self.acquire_current_snapshot_read().await?;
        let mapper = self.mapper.lock().await;
        let action = mapper.prepare_list_tasks(request, correlation, Some(&principal));
        let (_, action) = match governed_action(action, OperationKind::List) {
            Ok(action) => action,
            Err(error) => return Ok(rpc_governance_error(rpc.id, error)),
        };
        let A2aAction::ListTasks { page } = action else {
            return Err(ProtocolError::conflict("unexpected A2A ListTasks action"));
        };
        Ok(ConnectionOutcome::Response(HttpResponse::json(
            200,
            jsonrpc_result(rpc.id, page_to_wire(page, &mapper, include_artifacts)),
        )))
    }

    async fn handle_cancel_task(
        self: &Arc<Self>,
        rpc: JsonRpcRequest,
        principal: ProtocolPrincipal,
        ingress: CancellationIngress,
        cancellation: CancellationToken,
    ) -> ProtocolResult<ConnectionOutcome> {
        let wire = match WireCancelTaskRequest::parse(rpc.params.clone()) {
            Ok(wire) => wire,
            Err(message) => return Ok(rpc_invalid_params(rpc.id, &message)),
        };
        if let Err(message) = bind_tenant(wire.tenant.as_deref(), &principal) {
            return Ok(rpc_invalid_params(rpc.id, &message));
        }
        if wire
            .metadata
            .as_ref()
            .is_some_and(|metadata| !metadata.is_empty())
        {
            return Ok(rpc_unsupported(rpc.id, "cancel metadata is not supported"));
        }
        let correlation = match correlation_identity(&rpc) {
            Ok(value) => value,
            Err(error) => return Ok(rpc_invalid_params(rpc.id, &error.message)),
        };
        // An exact cancellation retry can also be a mapper no-op. It must not reconstruct or
        // schedule control work from a stale local projection after another CAS writer advanced.
        {
            let _snapshot_read = self.acquire_current_snapshot_read().await?;
        }
        let retry_action = {
            let mut live = self.mapper.lock().await;
            live.cancellation_for_task(&wire.id, &principal)
                .is_some()
                .then(|| {
                    live.prepare_cancel_task_candidate(
                        &wire.id,
                        correlation.clone(),
                        Some(&principal),
                    )
                })
        };
        const CANCEL_RATE_EXHAUSTED: &str = "A2A cancellation shared-network rate is exhausted";
        let action = match retry_action {
            Some(action) => action,
            None => match self
                .persist_mapper_mutation(|candidate| {
                    let before = candidate.revision();
                    let action = candidate.prepare_cancel_task_candidate(
                        &wire.id,
                        correlation,
                        Some(&principal),
                    );
                    // Only a newly accepted, governed intent consumes the shared-IP rate. The
                    // candidate is still isolated here, so a 429 cannot hide durable acceptance.
                    if candidate.revision() != before
                        && matches!(
                            ingress,
                            CancellationIngress::Public(peer_ip)
                                if !self.control_request_limiter.commit_ip_rate(peer_ip)?
                        )
                    {
                        return Err(ProtocolError::conflict(CANCEL_RATE_EXHAUSTED));
                    }
                    Ok(action)
                })
                .await
            {
                Ok(action) => action,
                Err(error) if error.message == CANCEL_RATE_EXHAUSTED => {
                    return Ok(ConnectionOutcome::Response(
                        HttpResponse::text(429, "cancellation shared-network rate is exhausted")
                            .with_header("Retry-After", "60"),
                    ));
                }
                Err(error) => return Err(error),
            },
        };
        let (_request_envelope, action) = match governed_action(action, OperationKind::Cancel) {
            Ok(action) => action,
            Err(error) => return Ok(rpc_governance_error(rpc.id, error)),
        };
        let A2aAction::CancelTask { task } = &action else {
            return Err(ProtocolError::conflict("unexpected A2A CancelTask action"));
        };
        let task = task.clone();
        let live = self.mapper.lock().await;
        let cancellation_record = live
            .cancellation_for_task(&task.mapping.task_id, &principal)
            .cloned()
            .ok_or_else(|| {
                ProtocolError::conflict("A2A cancellation intent has no durable control record")
            })?;
        let (envelope, action) = live
            .reconstruct_cancel(&cancellation_record.cancellation_id, &principal)
            .map_err(|error| {
                ProtocolError::conflict(format!(
                    "reconstruct canonical A2A cancellation control: {}",
                    error.message
                ))
            })?;
        drop(live);
        let owner = A2aEventOwner::from_task(&task);
        // The durable cancellation snapshot is the safety boundary. Interrupt a running message
        // immediately and reserve its canonical control action before attempting event delivery;
        // an unavailable event backend must never let the external effect keep running.
        self.signal_running_dispatch_cancellation(&owner, &task.mapping.task_id)?;
        let cancel_job_id = cancellation_record.cancellation_id.clone();
        let job = A2aDispatchJob {
            durable_dispatch_id: None,
            durable_cancellation_id: Some(cancellation_record.cancellation_id.clone()),
            lane: DispatchLane::Cancellation,
            mode: A2aExecutionMode::Blocking,
            envelope,
            action,
        };
        let mut acceptance = match self.reserve_dispatch(
            job,
            DispatchReservation {
                owner,
                task_id: task.mapping.task_id.clone(),
                message_id: cancel_job_id,
                run_id: task.mapping.run_id.clone(),
                lane: DispatchLane::Cancellation,
                delay_start: false,
                allow_new: true,
            },
        ) {
            Ok(Some(acceptance)) => acceptance,
            Ok(None) => {
                return Err(ProtocolError::conflict(
                    "A2A cancellation reservation disappeared",
                ))
            }
            Err(_) => {
                return Ok(ConnectionOutcome::Response(HttpResponse::json(
                    503,
                    jsonrpc_error(rpc.id, -32603, "Dispatch capacity exhausted", None),
                )))
            }
        };
        if acceptance.newly_reserved {
            if matches!(
                cancellation_record.state,
                A2aCancellationOutboxState::Running | A2aCancellationOutboxState::ReconcilePending
            ) {
                let decision = self
                    .reconcile_unknown_cancellation_once(
                        &cancellation_record,
                        &cancellation,
                        Instant::now() + self.config.dispatch_ack_timeout,
                    )
                    .await;
                match decision {
                    A2aUnknownDispatchDecision::AlreadyStopped => {
                        let Some(commit) = acceptance.commit.take() else {
                            return Ok(rpc_reconciliation_error(
                                rpc.id,
                                &task,
                                "reconciled cancellation lost its scheduler commit",
                            ));
                        };
                        let cancellation_id = cancellation_record.cancellation_id.clone();
                        let expected_attempt = cancellation_record.attempts;
                        let commit_server = self.clone();
                        let post_commit = Box::new(move || {
                            // This hook runs inside the detached snapshot commit task after both
                            // durable CAS and live mapper install. A request abort can no longer
                            // drop the scheduler sender after the cancellation became durable.
                            commit_server.quarantine_recovery(&cancellation_id);
                            if commit
                                .send(DispatchCommit {
                                    expected_dispatch_attempt: None,
                                    expected_cancellation_attempt: Some(expected_attempt),
                                    host_already_fenced: true,
                                    dispatch_recovery_claim: None,
                                })
                                .is_err()
                            {
                                commit_server.release_recovery_attempt(&cancellation_id);
                                return Err(ProtocolError::conflict(
                                    "reconciled cancellation scheduler commit failed",
                                ));
                            }
                            Ok(())
                        });
                        let current = match self
                            .persist_cancellation_fence_with_post_commit(
                                &cancellation_record.cancellation_id,
                                cancellation_record.attempts,
                                &task.mapping.task_id,
                                "host reconciled the previously stopped cancellation fence",
                                Some(post_commit),
                            )
                            .await
                        {
                            Ok(current) => current,
                            Err(_) => return Ok(rpc_reconciliation_error(
                                rpc.id,
                                &task,
                                "persisting or scheduling the reconciled cancellation fence failed",
                            )),
                        };
                        if self
                            .flush_pending_events_for_task(&task.mapping.task_id)
                            .await
                            .is_err()
                        {
                            return Ok(rpc_reconciliation_error(
                                rpc.id,
                                &current,
                                "host cancellation was fenced but its event is pending reconciliation",
                            ));
                        }
                        return Ok(ConnectionOutcome::Response(HttpResponse::json(
                            200,
                            jsonrpc_result(rpc.id, json!(A2aWireTask::from(&current))),
                        )));
                    }
                    A2aUnknownDispatchDecision::SafeToRetry => {}
                    A2aUnknownDispatchDecision::ReconcileRequired => {
                        if cancellation_record.state == A2aCancellationOutboxState::Running {
                            let _ = self
                                .mark_cancellation_reconcile_pending(
                                    &cancellation_record.cancellation_id,
                                    "unknown cancellation outcome requires host reconciliation",
                                )
                                .await;
                        }
                        let _ = self
                            .flush_pending_events_for_task(&task.mapping.task_id)
                            .await;
                        return Ok(rpc_reconciliation_error(
                            rpc.id,
                            &task,
                            "cancellation outcome is unknown and the host did not attest a safe retry",
                        ));
                    }
                }
            }
            if cancellation_record.state == A2aCancellationOutboxState::Running
                && self
                    .mark_cancellation_reconcile_pending(
                        &cancellation_record.cancellation_id,
                        "recovering cancellation control with unknown outcome",
                    )
                    .await
                    .is_err()
            {
                return Ok(rpc_reconciliation_error(
                    rpc.id,
                    &task,
                    "persisting cancellation reconciliation failed",
                ));
            }
            if self
                .mark_cancellation_running(&cancellation_record.cancellation_id)
                .await
                .is_err()
            {
                return Ok(rpc_reconciliation_error(
                    rpc.id,
                    &task,
                    "persisting the cancellation control fence failed",
                ));
            }
        }
        if let Some(commit) = acceptance.commit.take() {
            let expected_cancellation_attempt = self
                .mapper
                .lock()
                .await
                .cancellation_for_task(&task.mapping.task_id, &principal)
                .map(|record| record.attempts);
            // The scheduler can finish on another worker as soon as the commit is sent. Make its
            // release linearize after this insertion, and roll the claim back if send fails.
            self.quarantine_recovery(&cancellation_record.cancellation_id);
            if commit
                .send(DispatchCommit {
                    expected_dispatch_attempt: None,
                    expected_cancellation_attempt,
                    host_already_fenced: false,
                    dispatch_recovery_claim: None,
                })
                .is_err()
            {
                self.release_recovery_attempt(&cancellation_record.cancellation_id);
                return Ok(rpc_reconciliation_error(
                    rpc.id,
                    &task,
                    "cancellation scheduler commit failed",
                ));
            }
            #[cfg(test)]
            let commit_hook = {
                self.cancellation_commit_after_send_hook
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone()
            };
            #[cfg(test)]
            if let Some(hook) = commit_hook {
                hook.sent.notify_one();
                hook.resume_sender.notified().await;
            }
        }
        let cancellation_event_failed = self
            .flush_pending_events_for_task(&task.mapping.task_id)
            .await
            .is_err();
        let completion = async {
            loop {
                match acceptance.completion.recv().await {
                    Ok(result) => break result,
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => {
                        break Err(ProtocolError::conflict(
                            "A2A cancellation completion channel closed",
                        ))
                    }
                }
            }
        };
        let completion = tokio::select! {
            () = cancellation.cancelled() => Err(ProtocolError::new(
                ProtocolErrorCode::Cancelled,
                "cancellation response wait was cancelled",
            )),
            result = timeout(self.config.blocking_dispatch_timeout, completion) => {
                result.unwrap_or_else(|_| Err(ProtocolError::conflict(
                    "cancellation response wait timed out",
                )))
            }
        };
        if completion.is_err() || cancellation_event_failed {
            let current = self
                .mapper
                .lock()
                .await
                .tasks()
                .get(&task.mapping.task_id)
                .cloned()
                .unwrap_or(task);
            let detail = match (completion.is_err(), cancellation_event_failed) {
                (true, true) => {
                    "host cancellation was not fenced and its event is pending reconciliation"
                }
                (true, false) => "host cancellation was not fenced; reconciliation is pending",
                (false, true) => {
                    "host cancellation was fenced but its event is pending reconciliation"
                }
                (false, false) => unreachable!("guarded by reconciliation failure"),
            };
            return Ok(rpc_reconciliation_error(rpc.id, &current, detail));
        }
        let task = self
            .mapper
            .lock()
            .await
            .tasks()
            .get(&wire.id)
            .cloned()
            .ok_or_else(|| ProtocolError::conflict("cancelled A2A task disappeared"))?;
        Ok(ConnectionOutcome::Response(HttpResponse::json(
            200,
            jsonrpc_result(rpc.id, json!(A2aWireTask::from(&task))),
        )))
    }

    async fn handle_subscribe(
        &self,
        rpc: JsonRpcRequest,
        principal: ProtocolPrincipal,
    ) -> ProtocolResult<ConnectionOutcome> {
        let wire = match WireSubscribeRequest::parse(rpc.params.clone()) {
            Ok(wire) => wire,
            Err(message) => return Ok(rpc_invalid_params(rpc.id, &message)),
        };
        if let Err(message) = bind_tenant(wire.tenant.as_deref(), &principal) {
            return Ok(rpc_invalid_params(rpc.id, &message));
        }
        let correlation = match correlation_identity(&rpc) {
            Ok(value) => value,
            Err(error) => return Ok(rpc_invalid_params(rpc.id, &error.message)),
        };
        // Subscribe before reading/replaying so the replay-to-live handoff cannot lose an event.
        let receiver = self.live.subscribe();
        let _snapshot_read = self.acquire_current_snapshot_read().await?;
        let mapper = self.mapper.lock().await;
        let action = mapper.prepare_get_task(&wire.id, correlation, Some(&principal));
        let (_, action) = match governed_action(action, OperationKind::Get) {
            Ok(action) => action,
            Err(error) => return Ok(rpc_governance_error(rpc.id, error)),
        };
        let A2aAction::GetTask { task } = action else {
            return Err(ProtocolError::conflict(
                "unexpected A2A SubscribeToTask action",
            ));
        };
        if task.state.is_terminal() {
            return Ok(rpc_unsupported(
                rpc.id,
                "terminal tasks cannot be subscribed",
            ));
        }
        drop(mapper);
        drop(_snapshot_read);
        let owner = A2aEventOwner::from_task(&task);
        if !owner.matches(&principal) {
            return Ok(rpc_task_not_found(rpc.id));
        }
        let stream_lease = match self.stream_quota.try_acquire(owner.clone())? {
            Some(lease) => lease,
            None => return Ok(rpc_stream_capacity_exhausted(rpc.id)),
        };
        self.flush_pending_events_for_task(&wire.id).await?;
        let replay = if let Some(last_event_id) = rpc.last_event_id {
            match self
                .replay_to_high_water(
                    &owner,
                    &wire.id,
                    &task.mapping.context_id,
                    Some(last_event_id),
                    self.config.max_stream_events,
                    self.config.max_stream_bytes,
                )
                .await
            {
                Ok(events) => events,
                Err(error) if error.message.contains("retention gap") => {
                    return Ok(ConnectionOutcome::Response(HttpResponse::json(
                        200,
                        jsonrpc_error(
                            rpc.id,
                            -32004,
                            "Unsupported operation",
                            Some(a2a_error_data(
                                "UNSUPPORTED_OPERATION",
                                [("detail", json!("Last-Event-ID is no longer retained"))],
                            )),
                        ),
                    )))
                }
                Err(error) if error.code == ProtocolErrorCode::InvalidRequest => {
                    return Ok(rpc_invalid_params(rpc.id, "Last-Event-ID is invalid"))
                }
                Err(error) => return Err(error),
            }
        } else {
            Vec::new()
        };
        let initial = if rpc.last_event_id.is_none() {
            let retained = self
                .replay_to_high_water(
                    &owner,
                    &wire.id,
                    &task.mapping.context_id,
                    None,
                    self.config.max_stream_events,
                    self.config.max_stream_bytes,
                )
                .await
                .map_err(|_| ProtocolError::conflict("A2A fresh subscription replay failed"))?;
            let latest = match retained.last() {
                Some(event) => event,
                None => {
                    return Err(ProtocolError::conflict(
                        "A2A task has no retained durable event baseline",
                    ))
                }
            };
            Some((Some(latest.event_id), latest.response.clone()))
        } else {
            None
        };
        Ok(ConnectionOutcome::Stream(Box::new(SsePlan {
            id: rpc.id,
            owner,
            task_id: wire.id,
            context_id: task.mapping.context_id,
            initial,
            replay,
            receiver,
            dispatch_start: None,
            defer_initial_until_response: false,
            last_event_id: rpc.last_event_id,
            _stream_lease: stream_lease,
        })))
    }

    async fn write_sse_stream(
        self: &Arc<Self>,
        stream: &mut TcpStream,
        mut plan: SsePlan,
        cancellation: CancellationToken,
    ) -> ProtocolResult<()> {
        stream
            .write_all(
                b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\nConnection: close\r\nX-Accel-Buffering: no\r\n\r\n",
            )
            .await
            .map_err(|error| protocol_io("write A2A SSE headers", error))?;
        let mut count = 0_usize;
        let mut bytes = 0_usize;
        let mut last_seen = plan.last_event_id.or_else(|| {
            plan.replay
                .first()
                .and_then(|event| event.event_id.checked_sub(1))
        });
        let mut idle_deadline = Instant::now() + self.config.stream_idle_timeout;
        let half_idle = self.config.stream_idle_timeout / 2;
        let head_probe_interval = if half_idle.is_zero() {
            self.config.stream_idle_timeout
        } else {
            half_idle.min(A2A_STREAM_HEAD_PROBE_INTERVAL)
        };
        let mut head_probe_deadline = Instant::now() + head_probe_interval;
        let mut deferred_initial = None;
        if plan.defer_initial_until_response {
            if let Some((event_id, response)) = plan.initial.take() {
                validate_stream_response_binding(&response, &plan.task_id, &plan.context_id)?;
                if response.is_terminal() {
                    return Err(ProtocolError::conflict(
                        "A2A deferred stream baseline cannot already be terminal",
                    ));
                }
                if let Some(event_id) = event_id {
                    last_seen = Some(event_id);
                }
                deferred_initial = Some((event_id, response));
            }
            if let Some(start) = plan.dispatch_start.take() {
                let _ = start.send(());
            }
        } else if let Some((event_id, response)) = plan.initial.take() {
            validate_stream_response_binding(&response, &plan.task_id, &plan.context_id)?;
            let terminal = response.is_terminal();
            write_sse_event(
                stream,
                &plan.id,
                event_id,
                &response,
                &mut count,
                &mut bytes,
                &self.config,
            )
            .await?;
            if let Some(start) = plan.dispatch_start.take() {
                let _ = start.send(());
            }
            if let Some(event_id) = event_id {
                last_seen = Some(event_id);
            }
            idle_deadline = Instant::now() + self.config.stream_idle_timeout;
            if terminal {
                return Ok(());
            }
        }
        if let Some(start) = plan.dispatch_start.take() {
            let _ = start.send(());
        }
        let replay = std::mem::take(&mut plan.replay);
        for event in replay {
            validate_persisted_event_binding(
                &event,
                None,
                &plan.owner,
                &plan.task_id,
                &plan.context_id,
                None,
            )?;
            if event.logical_event_id.is_empty()
                || last_seen.is_some_and(|last| event.event_id != last.saturating_add(1))
            {
                return Err(ProtocolError::conflict(
                    "A2A SSE initial replay is not contiguous",
                ));
            }
            resolve_deferred_sse_initial(
                stream,
                &plan,
                &mut deferred_initial,
                &event.response,
                &mut count,
                &mut bytes,
                &self.config,
            )
            .await?;
            let terminal = event.response.is_terminal();
            write_sse_event(
                stream,
                &plan.id,
                Some(event.event_id),
                &event.response,
                &mut count,
                &mut bytes,
                &self.config,
            )
            .await?;
            last_seen = Some(event.event_id);
            idle_deadline = Instant::now() + self.config.stream_idle_timeout;
            if terminal {
                return Ok(());
            }
        }
        loop {
            let received = tokio::select! {
                () = cancellation.cancelled() => return Ok(()),
                () = sleep_until(idle_deadline) => return Ok(()),
                () = sleep_until(head_probe_deadline) => {
                    let probe = async {
                        let _snapshot_read = self.acquire_current_snapshot_read().await?;
                        Ok::<(), ProtocolError>(())
                    };
                    timeout(self.config.control_probe_timeout, probe)
                        .await
                        .map_err(|_| ProtocolError::conflict(
                            "A2A SSE durable-head probe timed out",
                        ))??;
                    head_probe_deadline = Instant::now() + head_probe_interval;
                    continue;
                }
                result = plan.receiver.recv() => result,
            };
            let event = match received {
                Ok(LiveEvent(event)) => event,
                Err(broadcast::error::RecvError::Closed) => return Ok(()),
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    let recovered = self
                        .replay_to_high_water(
                            &plan.owner,
                            &plan.task_id,
                            &plan.context_id,
                            last_seen,
                            self.config.max_stream_events.saturating_sub(count),
                            self.config.max_stream_bytes.saturating_sub(bytes),
                        )
                        .await?;
                    for event in recovered {
                        resolve_deferred_sse_initial(
                            stream,
                            &plan,
                            &mut deferred_initial,
                            &event.response,
                            &mut count,
                            &mut bytes,
                            &self.config,
                        )
                        .await?;
                        let terminal = event.response.is_terminal();
                        write_sse_event(
                            stream,
                            &plan.id,
                            Some(event.event_id),
                            &event.response,
                            &mut count,
                            &mut bytes,
                            &self.config,
                        )
                        .await?;
                        last_seen = Some(event.event_id);
                        idle_deadline = Instant::now() + self.config.stream_idle_timeout;
                        if terminal {
                            return Ok(());
                        }
                    }
                    continue;
                }
            };
            if event.task_id != plan.task_id {
                continue;
            }
            if event.owner != plan.owner {
                return Err(ProtocolError::conflict(
                    "A2A live event crossed its task owner boundary",
                ));
            }
            validate_persisted_event_binding(
                &event,
                None,
                &plan.owner,
                &plan.task_id,
                &plan.context_id,
                None,
            )?;
            if event.logical_event_id.is_empty() {
                return Err(ProtocolError::conflict(
                    "A2A live event has no durable logical identity",
                ));
            }
            if last_seen.is_some_and(|last| event.event_id <= last) {
                continue;
            }
            if last_seen.is_some_and(|last| event.event_id > last.saturating_add(1)) {
                let recovered = self
                    .replay_to_high_water(
                        &plan.owner,
                        &plan.task_id,
                        &plan.context_id,
                        last_seen,
                        self.config.max_stream_events.saturating_sub(count),
                        self.config.max_stream_bytes.saturating_sub(bytes),
                    )
                    .await?;
                for replayed in recovered {
                    resolve_deferred_sse_initial(
                        stream,
                        &plan,
                        &mut deferred_initial,
                        &replayed.response,
                        &mut count,
                        &mut bytes,
                        &self.config,
                    )
                    .await?;
                    let terminal = replayed.response.is_terminal();
                    write_sse_event(
                        stream,
                        &plan.id,
                        Some(replayed.event_id),
                        &replayed.response,
                        &mut count,
                        &mut bytes,
                        &self.config,
                    )
                    .await?;
                    last_seen = Some(replayed.event_id);
                    idle_deadline = Instant::now() + self.config.stream_idle_timeout;
                    if terminal {
                        return Ok(());
                    }
                }
                if last_seen.is_some_and(|last| event.event_id <= last) {
                    continue;
                }
            }
            if last_seen.is_some_and(|last| event.event_id != last.saturating_add(1)) {
                return Err(ProtocolError::conflict(
                    "A2A SSE live event is not contiguous",
                ));
            }
            resolve_deferred_sse_initial(
                stream,
                &plan,
                &mut deferred_initial,
                &event.response,
                &mut count,
                &mut bytes,
                &self.config,
            )
            .await?;
            let terminal = event.response.is_terminal();
            write_sse_event(
                stream,
                &plan.id,
                Some(event.event_id),
                &event.response,
                &mut count,
                &mut bytes,
                &self.config,
            )
            .await?;
            last_seen = Some(event.event_id);
            idle_deadline = Instant::now() + self.config.stream_idle_timeout;
            if terminal {
                return Ok(());
            }
        }
    }

    async fn replay_to_high_water(
        &self,
        owner: &A2aEventOwner,
        task_id: &str,
        context_id: &str,
        after_event_id: Option<u64>,
        max_events: usize,
        max_bytes: usize,
    ) -> ProtocolResult<Vec<A2aPersistedEvent>> {
        if max_events == 0 || max_bytes == 0 {
            return Err(ProtocolError::conflict(
                "A2A SSE replay exhausted its stream budget",
            ));
        }
        let mut output = Vec::new();
        let mut output_bytes = 0_usize;
        let mut cursor = after_event_id;
        let mut high_water = None;
        loop {
            let remaining_events = max_events.saturating_sub(output.len());
            let remaining_bytes = max_bytes.saturating_sub(output_bytes);
            if remaining_events == 0 || remaining_bytes == 0 {
                return Err(ProtocolError::conflict(
                    "A2A SSE replay exceeds the configured stream limit",
                ));
            }
            let page = self
                .events
                .replay_page(
                    owner,
                    task_id,
                    cursor,
                    high_water,
                    A2aReplayLimits {
                        max_events: remaining_events.min(self.config.max_replay_events),
                        max_bytes: remaining_bytes.min(self.config.max_replay_bytes),
                    },
                )
                .await
                .map_err(|error| match error {
                    A2aEventStoreError::RetentionGap => {
                        ProtocolError::conflict("A2A SSE replay retention gap")
                    }
                    A2aEventStoreError::InvalidEventId => {
                        ProtocolError::invalid("A2A SSE replay event id is invalid")
                    }
                    A2aEventStoreError::Store(error) => error,
                })?;
            if let Some(expected) = high_water {
                if page.high_water != expected {
                    return Err(ProtocolError::conflict(
                        "A2A event store moved a fixed replay high-water boundary",
                    ));
                }
            } else {
                high_water = Some(page.high_water);
            }
            let boundary = high_water.expect("replay high-water was initialized");
            if page.events.is_empty() {
                if cursor.unwrap_or_default() < boundary {
                    return Err(ProtocolError::conflict(
                        "A2A event store returned a non-progressing replay page",
                    ));
                }
                break;
            }
            let mut expected_id = cursor.and_then(|value| value.checked_add(1));
            for event in page.events {
                validate_persisted_event_binding(&event, None, owner, task_id, context_id, None)?;
                if event.logical_event_id.is_empty()
                    || expected_id.is_some_and(|expected| event.event_id != expected)
                    || event.event_id > boundary
                {
                    return Err(ProtocolError::conflict(
                        "A2A event store returned a non-contiguous replay",
                    ));
                }
                let event_bytes = serde_json::to_vec(&event.response)
                    .map_err(|error| {
                        ProtocolError::conflict(format!("serialize A2A replay event: {error}"))
                    })?
                    .len();
                output_bytes = output_bytes.checked_add(event_bytes).ok_or_else(|| {
                    ProtocolError::conflict("A2A SSE replay byte accounting overflowed")
                })?;
                if output.len() >= max_events || output_bytes > max_bytes {
                    return Err(ProtocolError::conflict(
                        "A2A SSE replay exceeds the configured stream limit",
                    ));
                }
                expected_id = event.event_id.checked_add(1);
                cursor = Some(event.event_id);
                let terminal = event.response.is_terminal();
                output.push(event);
                if terminal {
                    if cursor != Some(boundary) {
                        return Err(ProtocolError::conflict(
                            "A2A event store returned events after a terminal task event",
                        ));
                    }
                    return Ok(output);
                }
            }
            if cursor == Some(boundary) {
                break;
            }
        }
        Ok(output)
    }

    fn allowed_host(&self, headers: &A2aHttpHeaders) -> bool {
        headers
            .get("host")
            .and_then(normalize_host)
            .is_some_and(|host| self.config.allowed_hosts.contains(&host))
    }

    fn consume_rate(&self, ip: IpAddr) -> ProtocolResult<bool> {
        let minute = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            / 60;
        let mut buckets = self
            .rate
            .lock()
            .map_err(|_| ProtocolError::conflict("A2A rate limiter lock poisoned"))?;
        if !buckets.contains_key(&ip) && buckets.len() >= self.config.max_rate_buckets {
            buckets.retain(|_, bucket| bucket.minute == minute);
            if buckets.len() >= self.config.max_rate_buckets {
                return Ok(false);
            }
        }
        let bucket = buckets.entry(ip).or_insert(RateBucket { minute, count: 0 });
        if bucket.minute != minute {
            *bucket = RateBucket { minute, count: 0 };
        }
        if bucket.count >= self.config.max_requests_per_minute {
            return Ok(false);
        }
        bucket.count += 1;
        Ok(true)
    }
}

#[derive(Debug)]
enum ConnectionOutcome {
    Response(HttpResponse),
    Stream(Box<SsePlan>),
}

#[derive(Debug)]
struct SsePlan {
    id: Value,
    owner: A2aEventOwner,
    task_id: String,
    context_id: String,
    initial: Option<(Option<u64>, A2aStreamResponse)>,
    replay: Vec<A2aPersistedEvent>,
    receiver: broadcast::Receiver<LiveEvent>,
    dispatch_start: Option<oneshot::Sender<()>>,
    defer_initial_until_response: bool,
    last_event_id: Option<u64>,
    _stream_lease: StreamLease,
}

#[derive(Debug, Clone)]
struct A2aDispatchJob {
    durable_dispatch_id: Option<String>,
    durable_cancellation_id: Option<String>,
    lane: DispatchLane,
    mode: A2aExecutionMode,
    envelope: GovernanceEnvelope,
    action: A2aAction,
}

#[derive(Debug, Clone)]
struct DispatchReservation {
    owner: A2aEventOwner,
    task_id: String,
    message_id: String,
    run_id: String,
    lane: DispatchLane,
    delay_start: bool,
    allow_new: bool,
}

#[derive(Debug, Clone)]
struct JsonRpcRequest {
    id: Value,
    method: String,
    params: Value,
    correlation_id: Option<String>,
    last_event_id: Option<u64>,
}

#[derive(Debug)]
struct JsonRpcRequestError {
    id: Value,
    code: i64,
    message: &'static str,
}

impl JsonRpcRequest {
    fn parse(value: Value, headers: &A2aHttpHeaders) -> Result<Self, JsonRpcRequestError> {
        let mut object = value.as_object().cloned().ok_or(JsonRpcRequestError {
            id: Value::Null,
            code: -32600,
            message: "Invalid Request",
        })?;
        let id = object.remove("id").ok_or(JsonRpcRequestError {
            id: Value::Null,
            code: -32600,
            message: "Invalid Request",
        })?;
        let valid_number = id.as_number().is_some_and(|number| {
            number
                .as_i64()
                .is_some_and(|value| value.unsigned_abs() <= 9_007_199_254_740_991)
                || number
                    .as_u64()
                    .is_some_and(|value| value <= 9_007_199_254_740_991)
        });
        if !(id.as_str().is_some_and(|value| {
            !value.is_empty() && value.len() <= 512 && !value.chars().any(char::is_control)
        }) || valid_number)
        {
            return Err(JsonRpcRequestError {
                id: Value::Null,
                code: -32600,
                message: "Invalid Request",
            });
        }
        if object.remove("jsonrpc") != Some(Value::String("2.0".into())) {
            return Err(JsonRpcRequestError {
                id,
                code: -32600,
                message: "Invalid Request",
            });
        }
        let method = object
            .remove("method")
            .and_then(|value| value.as_str().map(str::to_owned))
            .ok_or_else(|| JsonRpcRequestError {
                id: id.clone(),
                code: -32600,
                message: "Invalid Request",
            })?;
        if method.is_empty() || method.len() > 128 || method.chars().any(char::is_control) {
            return Err(JsonRpcRequestError {
                id,
                code: -32600,
                message: "Invalid Request",
            });
        }
        let params = object.remove("params").unwrap_or_else(|| json!({}));
        if !params.is_object() || !object.is_empty() {
            return Err(JsonRpcRequestError {
                id,
                code: -32600,
                message: "Invalid Request",
            });
        }
        let last_event_id = match headers.get("last-event-id") {
            Some(value) => {
                let parsed = value
                    .parse::<u64>()
                    .ok()
                    .filter(|id| *id > 0 && *id <= 9_007_199_254_740_991);
                parsed
                    .ok_or_else(|| JsonRpcRequestError {
                        id: id.clone(),
                        code: -32602,
                        message: "Invalid params",
                    })?
                    .into()
            }
            None => None,
        };
        let correlation_id = headers.get("x-correlation-id").map(str::to_owned);
        Ok(Self {
            id,
            method,
            params,
            correlation_id,
            last_event_id,
        })
    }
}

#[derive(Debug)]
struct WireSendMessageRequest {
    tenant: Option<String>,
    message: Value,
    configuration: Option<Map<String, Value>>,
    metadata: Option<Map<String, Value>>,
}

impl WireSendMessageRequest {
    fn parse(value: Value) -> Result<Self, String> {
        let mut object = expect_object(value, "SendMessage params")?;
        let tenant = take_optional_string(&mut object, &["tenant"], "tenant")?;
        let message = take_required(&mut object, &["message"], "message")?;
        let configuration = take_optional_object(&mut object, &["configuration"], "configuration")?;
        let metadata = take_optional_object(&mut object, &["metadata"], "metadata")?;
        Ok(Self {
            tenant,
            message,
            configuration,
            metadata,
        })
    }

    fn validate_configuration(&self) -> Result<bool, String> {
        if self
            .metadata
            .as_ref()
            .is_some_and(|metadata| !metadata.is_empty())
        {
            return Err("request metadata is not supported".into());
        }
        let Some(mut configuration) = self.configuration.clone() else {
            return Ok(false);
        };
        let accepted = take_optional_array(
            &mut configuration,
            &["acceptedOutputModes", "accepted_output_modes"],
            "acceptedOutputModes",
        )?;
        if accepted.as_ref().is_some_and(|modes| !modes.is_empty()) {
            return Err("acceptedOutputModes negotiation is not supported".into());
        }
        if take_optional(
            &mut configuration,
            &[
                "taskPushNotificationConfig",
                "task_push_notification_config",
            ],
            "taskPushNotificationConfig",
        )?
        .is_some()
        {
            return Err("push notification configuration is not supported".into());
        }
        let _history = take_optional_i64(
            &mut configuration,
            &["historyLength", "history_length"],
            "historyLength",
        )?;
        let return_immediately = take_optional_bool(
            &mut configuration,
            &["returnImmediately", "return_immediately"],
            "returnImmediately",
        )?;
        // A2A 1.0 recommends ignoring fields added by newer protocol revisions. Known fields
        // remain strictly typed above; an unknown field cannot alter the authenticated tenant.
        Ok(return_immediately.unwrap_or(false))
    }

    fn into_message(self) -> Result<A2aMessage, String> {
        parse_message(self.message)
    }
}

#[derive(Debug)]
struct WireGetTaskRequest {
    tenant: Option<String>,
    id: String,
    history_length: Option<i64>,
}

impl WireGetTaskRequest {
    fn parse(value: Value) -> Result<Self, String> {
        let mut object = expect_object(value, "GetTask params")?;
        let tenant = take_optional_string(&mut object, &["tenant"], "tenant")?;
        let id = take_required_string(&mut object, &["id"], "id")?;
        let history_length = take_optional_i64(
            &mut object,
            &["historyLength", "history_length"],
            "historyLength",
        )?;
        Ok(Self {
            tenant,
            id,
            history_length,
        })
    }
}

#[derive(Debug)]
struct WireListTasksRequest {
    tenant: Option<String>,
    context_id: Option<String>,
    status: Option<A2aTaskState>,
    page_size: Option<u16>,
    page_token: Option<String>,
    history_length: Option<i64>,
    status_timestamp_after: Option<Value>,
    include_artifacts: Option<bool>,
}

impl WireListTasksRequest {
    fn parse(value: Value) -> Result<Self, String> {
        let mut object = expect_object(value, "ListTasks params")?;
        let tenant = take_optional_string(&mut object, &["tenant"], "tenant")?;
        let context_id =
            take_optional_string(&mut object, &["contextId", "context_id"], "contextId")?;
        let status = take_optional(&mut object, &["status"], "status")?
            .map(|value| {
                serde_json::from_value(value)
                    .map_err(|_| "status is not a supported TaskState".to_owned())
            })
            .transpose()?;
        let page_size = take_optional_i64(&mut object, &["pageSize", "page_size"], "pageSize")?
            .map(|value| {
                u16::try_from(value)
                    .map_err(|_| "pageSize is outside the supported range".to_owned())
            })
            .transpose()?;
        let page_token =
            take_optional_string(&mut object, &["pageToken", "page_token"], "pageToken")?;
        let history_length = take_optional_i64(
            &mut object,
            &["historyLength", "history_length"],
            "historyLength",
        )?;
        let status_timestamp_after = take_optional(
            &mut object,
            &["statusTimestampAfter", "status_timestamp_after"],
            "statusTimestampAfter",
        )?;
        let include_artifacts = take_optional_bool(
            &mut object,
            &["includeArtifacts", "include_artifacts"],
            "includeArtifacts",
        )?;
        Ok(Self {
            tenant,
            context_id,
            status,
            page_size,
            page_token,
            history_length,
            status_timestamp_after,
            include_artifacts,
        })
    }
}

#[derive(Debug)]
struct WireCancelTaskRequest {
    tenant: Option<String>,
    id: String,
    metadata: Option<Map<String, Value>>,
}

impl WireCancelTaskRequest {
    fn parse(value: Value) -> Result<Self, String> {
        let mut object = expect_object(value, "CancelTask params")?;
        let tenant = take_optional_string(&mut object, &["tenant"], "tenant")?;
        let id = take_required_string(&mut object, &["id"], "id")?;
        let metadata = take_optional_object(&mut object, &["metadata"], "metadata")?;
        Ok(Self {
            tenant,
            id,
            metadata,
        })
    }
}

#[derive(Debug)]
struct WireSubscribeRequest {
    tenant: Option<String>,
    id: String,
}

impl WireSubscribeRequest {
    fn parse(value: Value) -> Result<Self, String> {
        let mut object = expect_object(value, "SubscribeToTask params")?;
        let tenant = take_optional_string(&mut object, &["tenant"], "tenant")?;
        let id = take_required_string(&mut object, &["id"], "id")?;
        Ok(Self { tenant, id })
    }
}

fn parse_message(value: Value) -> Result<A2aMessage, String> {
    let mut object = expect_object(value, "message")?;
    let message_id = take_required_string(&mut object, &["messageId", "message_id"], "messageId")?;
    let context_id = take_optional_string(&mut object, &["contextId", "context_id"], "contextId")?;
    let task_id = take_optional_string(&mut object, &["taskId", "task_id"], "taskId")?;
    let role = match take_required_string(&mut object, &["role"], "role")?.as_str() {
        "ROLE_USER" => A2aRole::User,
        // A2A clients can only submit user messages. ROLE_AGENT is reserved for outbound
        // agent-authored messages and accepting it here would cross the ingress trust boundary.
        "ROLE_AGENT" => return Err("ROLE_AGENT is not valid for an inbound message".into()),
        _ => return Err("role is not supported".into()),
    };
    let part_values = take_required(&mut object, &["parts"], "parts")?
        .as_array()
        .cloned()
        .ok_or_else(|| "parts must be an array".to_owned())?;
    if part_values.is_empty() {
        return Err("parts must not be empty".into());
    }
    let mut parts = Vec::with_capacity(part_values.len());
    let mut part_extensions = Vec::with_capacity(part_values.len());
    for value in part_values {
        let (part, extension) = parse_part(value)?;
        parts.push(part);
        part_extensions.push(extension);
    }
    let mut metadata: BTreeMap<String, Value> =
        take_optional_object(&mut object, &["metadata"], "metadata")?
            .unwrap_or_default()
            .into_iter()
            .collect();
    if metadata.contains_key(A2A_PART_WIRE_EXTENSIONS_METADATA_KEY) {
        return Err("message metadata uses a reserved AIKit part-extension key".into());
    }
    for (index, extension) in part_extensions.into_iter().enumerate() {
        if let Some(extension) = extension {
            set_a2a_part_wire_extension(&mut metadata, index, extension)
                .map_err(|error| error.message)?;
        }
    }
    let extensions = take_optional_array(&mut object, &["extensions"], "extensions")?;
    let references = take_optional_array(
        &mut object,
        &["referenceTaskIds", "reference_task_ids"],
        "referenceTaskIds",
    )?;
    if extensions.as_ref().is_some_and(|values| !values.is_empty())
        || references.as_ref().is_some_and(|values| !values.is_empty())
    {
        return Err("message extensions and task references are not supported".into());
    }
    Ok(A2aMessage {
        message_id,
        context_id,
        task_id,
        role,
        parts,
        metadata,
    })
}

fn unsupported_message_media_type(message: &Value, supported: &[String]) -> Option<String> {
    let parts = message.as_object()?.get("parts")?.as_array()?;
    for part in parts {
        let Some(part) = part.as_object() else {
            continue;
        };
        let explicit = part
            .get("mediaType")
            .or_else(|| part.get("media_type"))
            .and_then(Value::as_str);
        let effective = explicit.or_else(|| {
            (part.contains_key("raw") || part.contains_key("url"))
                .then_some("application/octet-stream")
        });
        if let Some(media_type) = effective {
            if !supported
                .iter()
                .any(|mode| mode.eq_ignore_ascii_case(media_type))
            {
                return Some(media_type.to_owned());
            }
        }
    }
    None
}

fn parse_part(value: Value) -> Result<(A2aPart, Option<A2aPartWireExtension>), String> {
    let mut object = expect_object(value, "part")?;
    let text = take_optional_string(&mut object, &["text"], "text")?;
    let raw = take_optional_string(&mut object, &["raw"], "raw")?;
    let url = take_optional_string(&mut object, &["url"], "url")?;
    let data = take_optional(&mut object, &["data"], "data")?;
    let metadata = take_optional_object(&mut object, &["metadata"], "metadata")?;
    let filename = take_optional_string(&mut object, &["filename"], "filename")?;
    let media_type = take_optional_string(&mut object, &["mediaType", "media_type"], "mediaType")?;
    if metadata.as_ref().is_some_and(|value| !value.is_empty()) {
        return Err("part metadata is not supported".into());
    }
    let content_count = usize::from(text.is_some())
        + usize::from(raw.is_some())
        + usize::from(url.is_some())
        + usize::from(data.is_some());
    if content_count != 1 {
        return Err("part must contain exactly one of text, raw, url, or data".into());
    }
    if let Some(text) = text {
        if filename.is_some() {
            return Err("filename is only supported for URL and raw parts".into());
        }
        let extension = media_type.map(|media_type| A2aPartWireExtension {
            media_type: Some(media_type),
            ..A2aPartWireExtension::default()
        });
        return Ok((A2aPart::Text { text }, extension));
    }
    if let Some(data) = data {
        if filename.is_some() {
            return Err("filename is only supported for URL and raw parts".into());
        }
        let extension = media_type.map(|media_type| A2aPartWireExtension {
            media_type: Some(media_type),
            ..A2aPartWireExtension::default()
        });
        return Ok((A2aPart::Data { data }, extension));
    }
    if let Some(raw) = raw {
        let decoded = STANDARD_BASE64
            .decode(raw.as_bytes())
            .map_err(|_| "raw must be canonical padded base64".to_owned())?;
        if STANDARD_BASE64.encode(&decoded) != raw {
            return Err("raw must be canonical padded base64".into());
        }
        return Ok((
            A2aPart::Data { data: Value::Null },
            Some(A2aPartWireExtension {
                media_type: Some(media_type.unwrap_or_else(|| "application/octet-stream".into())),
                filename,
                raw: Some(decoded),
            }),
        ));
    }
    Ok((
        A2aPart::File {
            uri: url.expect("content count established a URL part"),
            media_type: media_type.unwrap_or_else(|| "application/octet-stream".into()),
        },
        filename.map(|filename| A2aPartWireExtension {
            filename: Some(filename),
            ..A2aPartWireExtension::default()
        }),
    ))
}

fn expect_object(value: Value, field: &str) -> Result<Map<String, Value>, String> {
    value
        .as_object()
        .cloned()
        .ok_or_else(|| format!("{field} must be an object"))
}

fn take_optional(
    object: &mut Map<String, Value>,
    aliases: &[&str],
    field: &str,
) -> Result<Option<Value>, String> {
    let present: Vec<&str> = aliases
        .iter()
        .copied()
        .filter(|alias| object.contains_key(*alias))
        .collect();
    if present.len() > 1 {
        return Err(format!("{field} was supplied through multiple aliases"));
    }
    Ok(present.first().and_then(|alias| object.remove(*alias)))
}

fn take_required(
    object: &mut Map<String, Value>,
    aliases: &[&str],
    field: &str,
) -> Result<Value, String> {
    take_optional(object, aliases, field)?.ok_or_else(|| format!("{field} is required"))
}

fn take_optional_string(
    object: &mut Map<String, Value>,
    aliases: &[&str],
    field: &str,
) -> Result<Option<String>, String> {
    take_optional(object, aliases, field)?
        .map(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| format!("{field} must be a string"))
        })
        .transpose()
}

fn take_required_string(
    object: &mut Map<String, Value>,
    aliases: &[&str],
    field: &str,
) -> Result<String, String> {
    take_optional_string(object, aliases, field)?.ok_or_else(|| format!("{field} is required"))
}

fn take_optional_object(
    object: &mut Map<String, Value>,
    aliases: &[&str],
    field: &str,
) -> Result<Option<Map<String, Value>>, String> {
    take_optional(object, aliases, field)?
        .map(|value| {
            value
                .as_object()
                .cloned()
                .ok_or_else(|| format!("{field} must be an object"))
        })
        .transpose()
}

fn take_optional_array(
    object: &mut Map<String, Value>,
    aliases: &[&str],
    field: &str,
) -> Result<Option<Vec<Value>>, String> {
    take_optional(object, aliases, field)?
        .map(|value| {
            value
                .as_array()
                .cloned()
                .ok_or_else(|| format!("{field} must be an array"))
        })
        .transpose()
}

fn take_optional_bool(
    object: &mut Map<String, Value>,
    aliases: &[&str],
    field: &str,
) -> Result<Option<bool>, String> {
    take_optional(object, aliases, field)?
        .map(|value| {
            value
                .as_bool()
                .ok_or_else(|| format!("{field} must be a boolean"))
        })
        .transpose()
}

fn take_optional_i64(
    object: &mut Map<String, Value>,
    aliases: &[&str],
    field: &str,
) -> Result<Option<i64>, String> {
    take_optional(object, aliases, field)?
        .map(|value| {
            value
                .as_i64()
                .filter(|value| *value >= 0)
                .ok_or_else(|| format!("{field} must be a non-negative integer"))
        })
        .transpose()
}

fn bind_tenant(tenant: Option<&str>, principal: &ProtocolPrincipal) -> Result<(), String> {
    if tenant.is_some_and(|tenant| principal.tenant_id.as_deref() != Some(tenant)) {
        return Err("tenant is not accessible".into());
    }
    Ok(())
}

fn correlation_identity(rpc: &JsonRpcRequest) -> ProtocolResult<CorrelationIdentity> {
    let request_id = rpc
        .id
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| rpc.id.to_string());
    let correlation_id = rpc
        .correlation_id
        .clone()
        .unwrap_or_else(|| format!("a2a-http-{request_id}"));
    CorrelationIdentity::new(correlation_id, request_id)
}

#[derive(Debug, Clone, Copy)]
enum OperationKind {
    Send,
    Get,
    List,
    Cancel,
}

#[derive(Debug)]
struct RpcGovernanceError {
    code: i64,
    message: &'static str,
    detail: Option<String>,
}

fn governed_action<T>(
    governed: GovernedAction<T>,
    operation: OperationKind,
) -> Result<(GovernanceEnvelope, T), RpcGovernanceError> {
    let authorization = governed.envelope.authorization.clone();
    match governed.into_authorized() {
        Ok(value) => Ok(value),
        Err(error) => {
            let denial = match authorization {
                GovernanceAuthorization::Denied { code, .. } => Some(code),
                GovernanceAuthorization::Allowed => None,
            };
            let (code, message) = match (denial, operation) {
                (Some(GovernanceDenialCode::UnknownTarget), _)
                | (Some(GovernanceDenialCode::PrincipalMismatch), OperationKind::Get)
                | (Some(GovernanceDenialCode::PrincipalMismatch), OperationKind::Cancel) => {
                    (-32001, "Task not found")
                }
                (Some(GovernanceDenialCode::StateConflict), OperationKind::Cancel) => {
                    (-32002, "Task not cancelable")
                }
                (Some(GovernanceDenialCode::MissingPrincipal), _) => (-32603, "Internal error"),
                (Some(GovernanceDenialCode::MissingScope), OperationKind::Get)
                | (Some(GovernanceDenialCode::MissingScope), OperationKind::Cancel) => {
                    (-32001, "Task not found")
                }
                (Some(_), _) => (-32602, "Invalid params"),
                (None, _) => (-32603, "Internal error"),
            };
            Err(RpcGovernanceError {
                code,
                message,
                detail: (code == -32602).then_some(error.message),
            })
        }
    }
}

fn send_result_value(task: &A2aTaskRecord, dispatch: &A2aDispatchOutboxRecord) -> Value {
    if let Some(immediate) = &dispatch.immediate_response {
        return json!({"task": A2aWireTask::from_task(immediate, &[], false)});
    }
    match &dispatch.response {
        A2aDispatchResponse::Message { message } => {
            json!({"message": A2aWireMessage::from(message)})
        }
        A2aDispatchResponse::Task { artifacts, .. } => json!({
            "task": A2aWireTask::from_task(task, artifacts, !artifacts.is_empty())
        }),
    }
}

fn page_to_wire(page: A2aTaskPage, mapper: &A2aMapper, include_artifacts: bool) -> Value {
    json!({
        "tasks": page.tasks
            .iter()
            .map(|task| A2aWireTask::from_task(
                task,
                mapper.artifacts_for_task(&task.mapping.task_id),
                include_artifacts,
            ))
            .collect::<Vec<_>>(),
        "nextPageToken": page.next_page_token,
        "pageSize": page.page_size,
        "totalSize": page.total_size,
    })
}

fn jsonrpc_result(id: Value, result: Value) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "result": result})
}

fn jsonrpc_error(id: Value, code: i64, message: &str, data: Option<Value>) -> Value {
    let mut error = Map::new();
    error.insert("code".into(), json!(code));
    error.insert("message".into(), json!(message));
    if let Some(data) = data {
        error.insert("data".into(), data);
    }
    json!({"jsonrpc": "2.0", "id": id, "error": error})
}

fn a2a_error_data(reason: &str, fields: impl IntoIterator<Item = (&'static str, Value)>) -> Value {
    let mut info = Map::new();
    info.insert(
        "@type".into(),
        Value::String("type.googleapis.com/google.rpc.ErrorInfo".into()),
    );
    info.insert("reason".into(), Value::String(reason.into()));
    info.insert("domain".into(), Value::String("a2a-protocol.org".into()));
    let metadata: Map<String, Value> = fields
        .into_iter()
        .map(|(key, value)| {
            let value = value
                .as_str()
                .map(str::to_owned)
                .unwrap_or_else(|| value.to_string());
            (key.into(), Value::String(value))
        })
        .collect();
    if !metadata.is_empty() {
        info.insert("metadata".into(), Value::Object(metadata));
    }
    Value::Array(vec![Value::Object(info)])
}

fn rpc_invalid_params(id: Value, _detail: &str) -> ConnectionOutcome {
    ConnectionOutcome::Response(HttpResponse::json(
        200,
        jsonrpc_error(id, -32602, "Invalid params", None),
    ))
}

fn rpc_unsupported(id: Value, detail: &str) -> ConnectionOutcome {
    ConnectionOutcome::Response(HttpResponse::json(
        200,
        jsonrpc_error(
            id,
            -32004,
            "Unsupported operation",
            Some(a2a_error_data(
                "UNSUPPORTED_OPERATION",
                [("detail", json!(detail))],
            )),
        ),
    ))
}

fn rpc_content_type_not_supported(id: Value, media_type: &str) -> ConnectionOutcome {
    ConnectionOutcome::Response(HttpResponse::json(
        200,
        jsonrpc_error(
            id,
            -32005,
            "Content type not supported",
            Some(a2a_error_data(
                "CONTENT_TYPE_NOT_SUPPORTED",
                [("mediaType", json!(media_type))],
            )),
        ),
    ))
}

fn rpc_stream_capacity_exhausted(id: Value) -> ConnectionOutcome {
    ConnectionOutcome::Response(HttpResponse::json(
        503,
        jsonrpc_error(
            id,
            -32603,
            "Stream capacity exhausted",
            Some(a2a_error_data(
                "RESOURCE_EXHAUSTED",
                [("retryable", json!(true))],
            )),
        ),
    ))
}

fn rpc_task_not_found(id: Value) -> ConnectionOutcome {
    ConnectionOutcome::Response(HttpResponse::json(
        200,
        jsonrpc_error(
            id,
            -32001,
            "Task not found",
            Some(a2a_error_data("TASK_NOT_FOUND", [])),
        ),
    ))
}

fn rpc_mapper_send_error(id: Value, error: ProtocolError) -> ConnectionOutcome {
    match error.code {
        ProtocolErrorCode::NotFound | ProtocolErrorCode::Forbidden => rpc_task_not_found(id),
        ProtocolErrorCode::InvalidRequest
        | ProtocolErrorCode::InvalidTransition
        | ProtocolErrorCode::Conflict => rpc_invalid_params(id, &error.message),
        ProtocolErrorCode::Unauthorized | ProtocolErrorCode::Cancelled => {
            ConnectionOutcome::Response(HttpResponse::json(
                200,
                jsonrpc_error(id, -32603, "Internal error", None),
            ))
        }
    }
}

fn rpc_governance_error(id: Value, error: RpcGovernanceError) -> ConnectionOutcome {
    let reason = match error.code {
        -32001 => Some("TASK_NOT_FOUND"),
        -32002 => Some("TASK_NOT_CANCELABLE"),
        _ => None,
    };
    let fields = error
        .detail
        .map(|detail| vec![("detail", json!(detail))])
        .unwrap_or_default();
    let data = reason.map(|reason| a2a_error_data(reason, fields));
    ConnectionOutcome::Response(HttpResponse::json(
        200,
        jsonrpc_error(id, error.code, error.message, data),
    ))
}

fn rpc_reconciliation_error(id: Value, task: &A2aTaskRecord, detail: &str) -> ConnectionOutcome {
    ConnectionOutcome::Response(HttpResponse::json(
        200,
        jsonrpc_error(
            id,
            -32006,
            "Invalid agent response",
            Some(a2a_error_data(
                "INVALID_AGENT_RESPONSE",
                [
                    ("taskId", json!(task.mapping.task_id)),
                    ("state", json!(task.state)),
                    ("detail", json!(detail)),
                    ("reconciliationRequired", json!(true)),
                ],
            )),
        ),
    ))
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let mut difference = left.len() ^ right.len();
    let length = left.len().max(right.len());
    for index in 0..length {
        difference |= usize::from(
            left.get(index).copied().unwrap_or(0) ^ right.get(index).copied().unwrap_or(0),
        );
    }
    difference == 0
}

fn accepts_media_type(accept: &str, expected: &str) -> bool {
    let (expected_type, _) = expected.split_once('/').unwrap_or((expected, ""));
    accept.split(',').any(|value| {
        let mut parts = value.split(';');
        let media_type = parts.next().unwrap_or_default().trim();
        let quality_allows = parts.all(|parameter| {
            let parameter = parameter.trim();
            parameter
                .strip_prefix("q=")
                .or_else(|| parameter.strip_prefix("Q="))
                .is_none_or(|quality| quality.parse::<f32>().is_ok_and(|quality| quality > 0.0))
        });
        quality_allows
            && (media_type == "*/*"
                || media_type.eq_ignore_ascii_case(expected)
                || media_type.eq_ignore_ascii_case(&format!("{expected_type}/*")))
    })
}

fn normalize_host(value: &str) -> Option<String> {
    let value = value.trim();
    if value.is_empty()
        || value.chars().any(char::is_control)
        || value.bytes().any(|byte| byte.is_ascii_whitespace())
    {
        return None;
    }
    if let Some(rest) = value.strip_prefix('[') {
        let close = rest.find(']')?;
        let host = &rest[..close];
        let suffix = &rest[close + 1..];
        if host.is_empty()
            || (!suffix.is_empty()
                && (!suffix.starts_with(':')
                    || suffix[1..].is_empty()
                    || !suffix[1..].bytes().all(|byte| byte.is_ascii_digit())))
        {
            return None;
        }
        return Some(host.to_ascii_lowercase());
    }
    let colon_count = value.bytes().filter(|byte| *byte == b':').count();
    let host = if colon_count == 1 {
        let (host, port) = value.rsplit_once(':')?;
        if host.is_empty() || port.is_empty() || !port.bytes().all(|byte| byte.is_ascii_digit()) {
            return None;
        }
        host
    } else {
        value
    };
    Some(host.to_ascii_lowercase())
}

async fn write_sse_event(
    stream: &mut TcpStream,
    id: &Value,
    event_id: Option<u64>,
    response: &A2aStreamResponse,
    count: &mut usize,
    bytes: &mut usize,
    config: &A2aHttpConfig,
) -> ProtocolResult<()> {
    if *count >= config.max_stream_events {
        return Err(ProtocolError::conflict(
            "A2A SSE stream event limit reached",
        ));
    }
    let envelope = jsonrpc_result(
        id.clone(),
        serde_json::to_value(response)
            .map_err(|error| ProtocolError::conflict(format!("serialize A2A event: {error}")))?,
    );
    let data = serde_json::to_vec(&envelope)
        .map_err(|error| ProtocolError::conflict(format!("serialize A2A SSE envelope: {error}")))?;
    let mut frame = Vec::with_capacity(data.len().saturating_add(64));
    if let Some(event_id) = event_id {
        frame.extend_from_slice(format!("id: {event_id}\n").as_bytes());
    }
    frame.extend_from_slice(b"data: ");
    frame.extend_from_slice(&data);
    frame.extend_from_slice(b"\n\n");
    if frame.len() > config.max_event_bytes
        || bytes.saturating_add(frame.len()) > config.max_stream_bytes
    {
        return Err(ProtocolError::conflict("A2A SSE stream byte limit reached"));
    }
    stream
        .write_all(&frame)
        .await
        .map_err(|error| protocol_io("write A2A SSE event", error))?;
    *count += 1;
    *bytes = bytes.saturating_add(frame.len());
    Ok(())
}

async fn resolve_deferred_sse_initial(
    stream: &mut TcpStream,
    plan: &SsePlan,
    deferred: &mut Option<(Option<u64>, A2aStreamResponse)>,
    next_response: &A2aStreamResponse,
    count: &mut usize,
    bytes: &mut usize,
    config: &A2aHttpConfig,
) -> ProtocolResult<()> {
    let Some((event_id, response)) = deferred.take() else {
        return Ok(());
    };
    // A direct response is a oneof, not a task lifecycle. Suppress the accepted Working task so
    // the stream contains exactly the one terminal Message. Task/status workflows retain their
    // accepted task baseline before the first subsequent update.
    if matches!(next_response, A2aStreamResponse::Message(_)) {
        return Ok(());
    }
    validate_stream_response_binding(&response, &plan.task_id, &plan.context_id)?;
    write_sse_event(stream, &plan.id, event_id, &response, count, bytes, config).await
}

#[derive(Debug)]
struct HttpRequest {
    headers: A2aHttpHeaders,
    body: Vec<u8>,
}

#[derive(Debug)]
struct HttpRequestHead {
    method: String,
    path: String,
    headers: A2aHttpHeaders,
    content_length: usize,
    body_prefix: Vec<u8>,
}

#[derive(Debug)]
struct HttpParseError {
    status: u16,
    message: String,
}

#[derive(Debug)]
struct HttpResponse {
    status: u16,
    content_type: Option<&'static str>,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
    control_priority: bool,
}

impl HttpResponse {
    fn text(status: u16, message: &str) -> Self {
        Self {
            status,
            content_type: Some("text/plain; charset=utf-8"),
            headers: Vec::new(),
            body: message.as_bytes().to_vec(),
            control_priority: false,
        }
    }

    fn json(status: u16, value: Value) -> Self {
        Self {
            status,
            content_type: Some("application/json"),
            headers: Vec::new(),
            body: serde_json::to_vec(&value).unwrap_or_else(|_| b"{}".to_vec()),
            control_priority: false,
        }
    }

    fn with_header(mut self, name: &str, value: &str) -> Self {
        if !name.contains(['\r', '\n']) && !value.contains(['\r', '\n']) {
            self.headers.push((name.to_owned(), value.to_owned()));
        }
        self
    }
}

async fn read_http_request_head(
    stream: &mut TcpStream,
    max_header_bytes: usize,
    max_body_bytes: usize,
) -> Result<HttpRequestHead, HttpParseError> {
    let mut buffer = Vec::new();
    let header_end = loop {
        if buffer.len() >= max_header_bytes {
            return Err(http_parse_error(431, "request headers are too large"));
        }
        let mut chunk = [0_u8; 2048];
        let read = stream
            .read(&mut chunk)
            .await
            .map_err(|_| http_parse_error(400, "failed to read request"))?;
        if read == 0 {
            return Err(http_parse_error(400, "incomplete request headers"));
        }
        buffer.extend_from_slice(&chunk[..read]);
        if let Some(index) = find_subslice(&buffer, b"\r\n\r\n") {
            break index + 4;
        }
        if buffer.len() >= max_header_bytes {
            return Err(http_parse_error(431, "request headers are too large"));
        }
    };
    if header_end > max_header_bytes {
        return Err(http_parse_error(431, "request headers are too large"));
    }
    let header = std::str::from_utf8(&buffer[..header_end - 4])
        .map_err(|_| http_parse_error(400, "request headers are not UTF-8"))?;
    let mut lines = header.split("\r\n");
    let request_line = lines
        .next()
        .ok_or_else(|| http_parse_error(400, "missing request line"))?;
    let mut parts = request_line.split(' ');
    let method = parts.next().unwrap_or_default();
    let path = parts.next().unwrap_or_default();
    let version = parts.next().unwrap_or_default();
    if parts.next().is_some()
        || !matches!(method, "POST" | "GET")
        || !path.starts_with('/')
        || path.contains(['?', '#'])
        || version != "HTTP/1.1"
    {
        return Err(http_parse_error(400, "invalid HTTP request line"));
    }
    let mut headers = BTreeMap::new();
    for line in lines {
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| http_parse_error(400, "invalid HTTP header"))?;
        let name = name.trim().to_ascii_lowercase();
        let value = value.trim();
        if name.is_empty()
            || name
                .chars()
                .any(|character| !character.is_ascii_alphanumeric() && character != '-')
            || value.chars().any(char::is_control)
            || headers.insert(name, value.to_owned()).is_some()
        {
            return Err(http_parse_error(400, "invalid or duplicate HTTP header"));
        }
    }
    if !headers.contains_key("host") {
        return Err(http_parse_error(400, "Host header is required"));
    }
    if headers.contains_key("transfer-encoding") {
        return Err(http_parse_error(400, "Transfer-Encoding is not supported"));
    }
    let content_length = match headers.get("content-length") {
        Some(value) => value
            .parse::<usize>()
            .map_err(|_| http_parse_error(400, "invalid Content-Length"))?,
        None if method == "POST" => {
            return Err(http_parse_error(411, "Content-Length is required"));
        }
        None => 0,
    };
    if content_length > max_body_bytes {
        return Err(http_parse_error(413, "request body is too large"));
    }
    if method == "GET" && content_length != 0 {
        return Err(http_parse_error(400, "GET requests must not have a body"));
    }
    let already_read = buffer.len().saturating_sub(header_end);
    if already_read > content_length {
        return Err(http_parse_error(400, "HTTP pipelining is not supported"));
    }
    Ok(HttpRequestHead {
        method: method.to_owned(),
        path: path.to_owned(),
        headers: A2aHttpHeaders { values: headers },
        content_length,
        body_prefix: buffer[header_end..].to_vec(),
    })
}

async fn read_http_request_body(
    stream: &mut TcpStream,
    head: HttpRequestHead,
) -> Result<HttpRequest, HttpParseError> {
    let already_read = head.body_prefix.len();
    let mut body = head.body_prefix;
    body.resize(head.content_length, 0);
    if already_read < head.content_length {
        stream
            .read_exact(&mut body[already_read..])
            .await
            .map_err(|_| http_parse_error(400, "incomplete request body"))?;
    }
    Ok(HttpRequest {
        headers: head.headers,
        body,
    })
}

async fn write_http_response(stream: &mut TcpStream, response: HttpResponse) -> ProtocolResult<()> {
    let reason = match response.status {
        200 => "OK",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        406 => "Not Acceptable",
        408 => "Request Timeout",
        411 => "Length Required",
        413 => "Payload Too Large",
        415 => "Unsupported Media Type",
        421 => "Misdirected Request",
        429 => "Too Many Requests",
        431 => "Request Header Fields Too Large",
        500 => "Internal Server Error",
        503 => "Service Unavailable",
        _ => "Error",
    };
    let mut head = format!(
        "HTTP/1.1 {} {}\r\nContent-Length: {}\r\nConnection: close\r\n",
        response.status,
        reason,
        response.body.len()
    );
    if let Some(content_type) = response.content_type {
        head.push_str(&format!("Content-Type: {content_type}\r\n"));
    }
    for (name, value) in response.headers {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    head.push_str("\r\n");
    stream
        .write_all(head.as_bytes())
        .await
        .map_err(|error| protocol_io("write A2A HTTP headers", error))?;
    stream
        .write_all(&response.body)
        .await
        .map_err(|error| protocol_io("write A2A HTTP body", error))
}

fn http_parse_error(status: u16, message: &str) -> HttpParseError {
    HttpParseError {
        status,
        message: message.into(),
    }
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn protocol_http_response(error: &ProtocolError) -> HttpResponse {
    let status = match error.code {
        ProtocolErrorCode::InvalidRequest | ProtocolErrorCode::InvalidTransition => 400,
        ProtocolErrorCode::Unauthorized => 401,
        ProtocolErrorCode::Forbidden => 403,
        ProtocolErrorCode::NotFound => 404,
        ProtocolErrorCode::Cancelled => 408,
        ProtocolErrorCode::Conflict => 500,
    };
    HttpResponse::text(status, "internal transport error")
}

fn protocol_io(operation: &str, error: std::io::Error) -> ProtocolError {
    ProtocolError::conflict(format!("{operation} failed: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::A2aRunMapping;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use tokio::io::{AsyncBufReadExt, BufReader};
    use tokio::sync::Notify;

    struct TestAuthenticator;

    impl A2aHttpAuthenticator for TestAuthenticator {
        fn authenticate(
            &self,
            headers: &A2aHttpHeaders,
        ) -> Result<ProtocolPrincipal, A2aHttpAuthError> {
            let token = headers
                .get("authorization")
                .and_then(|value| value.strip_prefix("Bearer "));
            let (subject, tenant) = match token {
                Some("owner") => ("owner", "tenant-a"),
                Some("other") => ("other", "tenant-b"),
                Some("third") => ("third", "tenant-c"),
                _ => {
                    return Err(A2aHttpAuthError {
                        status: 401,
                        message: "authentication required".into(),
                        www_authenticate: Some("Bearer realm=\"a2a-test\"".into()),
                    })
                }
            };
            ProtocolPrincipal::new(
                subject,
                ["a2a:message:send", "a2a:tasks:read", "a2a:tasks:cancel"],
            )
            .and_then(|principal| principal.with_tenant(tenant))
            .map_err(|error| A2aHttpAuthError {
                status: 500,
                message: error.message,
                www_authenticate: None,
            })
        }
    }

    struct ProtectedTestAuthenticator;

    impl A2aHttpAuthenticator for ProtectedTestAuthenticator {
        fn authenticate(
            &self,
            headers: &A2aHttpHeaders,
        ) -> Result<ProtocolPrincipal, A2aHttpAuthError> {
            if headers.get("authorization") != Some("Bearer protected-owner") {
                return Err(A2aHttpAuthError {
                    status: 401,
                    message: "protected authentication required".into(),
                    www_authenticate: Some("Bearer realm=\"a2a-protected-test\"".into()),
                });
            }
            ProtocolPrincipal::new("owner", ["a2a:tasks:cancel"])
                .and_then(|principal| principal.with_tenant("tenant-a"))
                .map_err(|error| A2aHttpAuthError {
                    status: 500,
                    message: error.message,
                    www_authenticate: None,
                })
        }
    }

    struct MultiOwnerProtectedTestAuthenticator;

    impl A2aHttpAuthenticator for MultiOwnerProtectedTestAuthenticator {
        fn authenticate(
            &self,
            headers: &A2aHttpHeaders,
        ) -> Result<ProtocolPrincipal, A2aHttpAuthError> {
            let (subject, tenant) = match headers.get("authorization") {
                Some("Bearer protected-owner") => ("owner", "tenant-a"),
                Some("Bearer protected-other") => ("other", "tenant-b"),
                _ => {
                    return Err(A2aHttpAuthError {
                        status: 401,
                        message: "protected authentication required".into(),
                        www_authenticate: Some("Bearer realm=\"a2a-protected-test\"".into()),
                    })
                }
            };
            ProtocolPrincipal::new(subject, ["a2a:tasks:cancel"])
                .and_then(|principal| principal.with_tenant(tenant))
                .map_err(|error| A2aHttpAuthError {
                    status: 500,
                    message: error.message,
                    www_authenticate: None,
                })
        }
    }

    struct ScopedTestAuthenticator;

    impl A2aHttpAuthenticator for ScopedTestAuthenticator {
        fn authenticate(
            &self,
            headers: &A2aHttpHeaders,
        ) -> Result<ProtocolPrincipal, A2aHttpAuthError> {
            if headers.get("authorization") == Some("Bearer send-only") {
                return ProtocolPrincipal::new("sender", ["a2a:message:send"])
                    .and_then(|principal| principal.with_tenant("tenant-a"))
                    .map_err(|error| A2aHttpAuthError {
                        status: 500,
                        message: error.message,
                        www_authenticate: None,
                    });
            }
            TestAuthenticator.authenticate(headers)
        }
    }

    struct SignalingAuthenticator {
        authenticated: Notify,
    }

    impl SignalingAuthenticator {
        fn new() -> Self {
            Self {
                authenticated: Notify::new(),
            }
        }
    }

    impl A2aHttpAuthenticator for SignalingAuthenticator {
        fn authenticate(
            &self,
            headers: &A2aHttpHeaders,
        ) -> Result<ProtocolPrincipal, A2aHttpAuthError> {
            let result = TestAuthenticator.authenticate(headers);
            self.authenticated.notify_one();
            result
        }
    }

    #[derive(Default)]
    struct CompletingHost {
        calls: AtomicUsize,
        reconcile_calls: AtomicUsize,
        safe_retry_unknown: bool,
    }

    #[async_trait]
    impl A2aDispatchHost for CompletingHost {
        async fn reconcile_unknown(
            &self,
            _server: Arc<A2aHttpJsonRpcServer>,
            _record: &A2aDispatchOutboxRecord,
        ) -> ProtocolResult<A2aUnknownDispatchDecision> {
            self.reconcile_calls.fetch_add(1, Ordering::SeqCst);
            Ok(if self.safe_retry_unknown {
                A2aUnknownDispatchDecision::SafeToRetry
            } else {
                A2aUnknownDispatchDecision::ReconcileRequired
            })
        }

        async fn handle(
            &self,
            server: Arc<A2aHttpJsonRpcServer>,
            context: &A2aDispatchContext,
            _envelope: &GovernanceEnvelope,
            action: &A2aAction,
        ) -> ProtocolResult<A2aDispatchAck> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let (task_id, should_complete) = match action {
                A2aAction::DispatchMessage {
                    message, mapping, ..
                } => (
                    mapping.task_id.as_str(),
                    message.metadata.get("complete") == Some(&Value::Bool(true)),
                ),
                A2aAction::DuplicateMessage { receipt } => (
                    receipt.mapping.task_id.as_str(),
                    receipt.message.metadata.get("complete") == Some(&Value::Bool(true)),
                ),
                A2aAction::CancelTask { .. } => return Ok(A2aDispatchAck::Stopped),
                _ => return Ok(A2aDispatchAck::Settled),
            };
            if should_complete {
                let snapshot = server.mapper_snapshot().await;
                if snapshot
                    .tasks()
                    .get(task_id)
                    .is_some_and(|task| !task.state.is_terminal())
                {
                    server
                        .transition_dispatch_task(context, A2aTaskState::Completed, None)
                        .await?;
                }
            }
            Ok(A2aDispatchAck::Settled)
        }
    }

    struct ExactStateTransitionHost {
        next: A2aTaskState,
        status_message: String,
        calls: AtomicUsize,
    }

    impl ExactStateTransitionHost {
        fn new(next: A2aTaskState, status_message: impl Into<String>) -> Self {
            Self {
                next,
                status_message: status_message.into(),
                calls: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl A2aDispatchHost for ExactStateTransitionHost {
        async fn handle(
            &self,
            server: Arc<A2aHttpJsonRpcServer>,
            context: &A2aDispatchContext,
            _envelope: &GovernanceEnvelope,
            action: &A2aAction,
        ) -> ProtocolResult<A2aDispatchAck> {
            if !matches!(
                action,
                A2aAction::DispatchMessage { .. } | A2aAction::DuplicateMessage { .. }
            ) {
                return if matches!(action, A2aAction::CancelTask { .. }) {
                    Ok(A2aDispatchAck::Stopped)
                } else {
                    Ok(A2aDispatchAck::Settled)
                };
            }
            self.calls.fetch_add(1, Ordering::SeqCst);
            server
                .transition_dispatch_task(context, self.next, Some(self.status_message.clone()))
                .await?;
            Ok(A2aDispatchAck::Settled)
        }
    }

    #[derive(Default)]
    struct DurableOutputHost {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl A2aDispatchHost for DurableOutputHost {
        async fn handle(
            &self,
            server: Arc<A2aHttpJsonRpcServer>,
            context: &A2aDispatchContext,
            _envelope: &GovernanceEnvelope,
            action: &A2aAction,
        ) -> ProtocolResult<A2aDispatchAck> {
            let A2aAction::DispatchMessage {
                message, mapping, ..
            } = action
            else {
                return if matches!(action, A2aAction::CancelTask { .. }) {
                    Ok(A2aDispatchAck::Stopped)
                } else {
                    Ok(A2aDispatchAck::Settled)
                };
            };
            self.calls.fetch_add(1, Ordering::SeqCst);
            if message.message_id.starts_with("durable-direct") {
                server
                    .complete_with_direct_message(
                        context,
                        A2aMessage {
                            message_id: format!("{}-response", message.message_id),
                            context_id: Some(mapping.context_id.clone()),
                            task_id: None,
                            role: A2aRole::Agent,
                            parts: vec![A2aPart::Text {
                                text: "Direct message response".into(),
                            }],
                            metadata: BTreeMap::new(),
                        },
                    )
                    .await?;
            } else {
                server
                    .complete_task_with_artifacts(
                        context,
                        vec![
                            A2aArtifact {
                                artifact_id: "artifact-text".into(),
                                name: None,
                                description: None,
                                parts: vec![A2aContentPart::Text {
                                    text: "Generated text content".into(),
                                    media_type: None,
                                }],
                                metadata: BTreeMap::new(),
                            },
                            A2aArtifact {
                                artifact_id: "artifact-raw".into(),
                                name: None,
                                description: None,
                                parts: vec![A2aContentPart::Raw {
                                    raw: b"tck".to_vec(),
                                    media_type: "text/plain".into(),
                                    filename: Some("output.txt".into()),
                                }],
                                metadata: BTreeMap::new(),
                            },
                            A2aArtifact {
                                artifact_id: "artifact-url".into(),
                                name: None,
                                description: None,
                                parts: vec![A2aContentPart::File {
                                    uri: "https://example.com/output.txt".into(),
                                    media_type: "text/plain".into(),
                                    filename: Some("output.txt".into()),
                                }],
                                metadata: BTreeMap::new(),
                            },
                            A2aArtifact {
                                artifact_id: "artifact-data".into(),
                                name: None,
                                description: None,
                                parts: vec![A2aContentPart::Data {
                                    data: json!({"key": "value", "count": 42}),
                                    media_type: None,
                                }],
                                metadata: BTreeMap::new(),
                            },
                        ],
                    )
                    .await?;
            }
            Ok(A2aDispatchAck::Settled)
        }
    }

    struct DelayedHost {
        delay: Duration,
        started: Notify,
        finished: Notify,
        modes: StdMutex<Vec<A2aExecutionMode>>,
    }

    impl DelayedHost {
        fn new(delay: Duration) -> Self {
            Self {
                delay,
                started: Notify::new(),
                finished: Notify::new(),
                modes: StdMutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl A2aDispatchHost for DelayedHost {
        async fn handle(
            &self,
            server: Arc<A2aHttpJsonRpcServer>,
            context: &A2aDispatchContext,
            _envelope: &GovernanceEnvelope,
            action: &A2aAction,
        ) -> ProtocolResult<A2aDispatchAck> {
            self.modes.lock().unwrap().push(context.mode);
            self.started.notify_one();
            tokio::select! {
                () = context.cancellation.cancelled() => {
                    return Ok(A2aDispatchAck::Stopped);
                }
                () = tokio::time::sleep(self.delay) => {}
            }
            let task_id = match action {
                A2aAction::DispatchMessage { mapping, .. } => &mapping.task_id,
                A2aAction::DuplicateMessage { receipt } => &receipt.mapping.task_id,
                _ => return Ok(A2aDispatchAck::Settled),
            };
            let snapshot = server.mapper_snapshot().await;
            if snapshot
                .tasks()
                .get(task_id)
                .is_some_and(|task| !task.state.is_terminal())
            {
                server
                    .transition_dispatch_task(context, A2aTaskState::Completed, None)
                    .await?;
            }
            self.finished.notify_one();
            Ok(A2aDispatchAck::Settled)
        }
    }

    struct FailingHost;

    #[async_trait]
    impl A2aDispatchHost for FailingHost {
        async fn handle(
            &self,
            _server: Arc<A2aHttpJsonRpcServer>,
            _context: &A2aDispatchContext,
            _envelope: &GovernanceEnvelope,
            _action: &A2aAction,
        ) -> ProtocolResult<A2aDispatchAck> {
            Err(ProtocolError::conflict("test host dispatch failed"))
        }
    }

    struct UncooperativeHost {
        delay: Duration,
        started: Notify,
        calls: AtomicUsize,
    }

    impl UncooperativeHost {
        fn new(delay: Duration) -> Self {
            Self {
                delay,
                started: Notify::new(),
                calls: AtomicUsize::new(0),
            }
        }
    }

    #[derive(Debug, Clone, Copy)]
    enum CancelHostBehavior {
        Stopped,
        Error,
        Timeout,
    }

    struct CancelHost {
        behavior: CancelHostBehavior,
        cancel_calls: AtomicUsize,
    }

    impl CancelHost {
        fn new(behavior: CancelHostBehavior) -> Self {
            Self {
                behavior,
                cancel_calls: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl A2aDispatchHost for CancelHost {
        async fn handle(
            &self,
            _server: Arc<A2aHttpJsonRpcServer>,
            _context: &A2aDispatchContext,
            _envelope: &GovernanceEnvelope,
            action: &A2aAction,
        ) -> ProtocolResult<A2aDispatchAck> {
            if !matches!(action, A2aAction::CancelTask { .. }) {
                return Ok(A2aDispatchAck::Settled);
            }
            self.cancel_calls.fetch_add(1, Ordering::SeqCst);
            match self.behavior {
                CancelHostBehavior::Stopped => Ok(A2aDispatchAck::Stopped),
                CancelHostBehavior::Error => {
                    Err(ProtocolError::conflict("test cancellation fence failed"))
                }
                CancelHostBehavior::Timeout => {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    Ok(A2aDispatchAck::Stopped)
                }
            }
        }
    }

    #[derive(Default)]
    struct ClaimObservingFastErrorHost {
        observed_claim: AtomicBool,
        called: Notify,
    }

    #[async_trait]
    impl A2aDispatchHost for ClaimObservingFastErrorHost {
        async fn handle(
            &self,
            server: Arc<A2aHttpJsonRpcServer>,
            _context: &A2aDispatchContext,
            _envelope: &GovernanceEnvelope,
            action: &A2aAction,
        ) -> ProtocolResult<A2aDispatchAck> {
            let A2aAction::CancelTask { task } = action else {
                return Ok(A2aDispatchAck::Settled);
            };
            let cancellation_id = server
                .mapper_snapshot()
                .await
                .pending_cancellations()
                .into_iter()
                .find(|record| record.task_id == task.mapping.task_id)
                .map(|record| record.cancellation_id)
                .ok_or_else(|| ProtocolError::conflict("test cancellation control disappeared"))?;
            self.observed_claim.store(
                server.recovery_was_attempted(&cancellation_id)?,
                Ordering::SeqCst,
            );
            self.called.notify_one();
            Err(ProtocolError::conflict("fast cancellation host error"))
        }
    }

    #[derive(Default)]
    struct AlreadyStoppedCancelHost {
        reconcile_cancel_calls: AtomicUsize,
        cancel_calls: AtomicUsize,
    }

    #[async_trait]
    impl A2aDispatchHost for AlreadyStoppedCancelHost {
        async fn reconcile_unknown_cancel(
            &self,
            _server: Arc<A2aHttpJsonRpcServer>,
            _record: &A2aCancellationOutboxRecord,
        ) -> ProtocolResult<A2aUnknownDispatchDecision> {
            self.reconcile_cancel_calls.fetch_add(1, Ordering::SeqCst);
            Ok(A2aUnknownDispatchDecision::AlreadyStopped)
        }

        async fn handle(
            &self,
            _server: Arc<A2aHttpJsonRpcServer>,
            _context: &A2aDispatchContext,
            _envelope: &GovernanceEnvelope,
            action: &A2aAction,
        ) -> ProtocolResult<A2aDispatchAck> {
            if matches!(action, A2aAction::CancelTask { .. }) {
                self.cancel_calls.fetch_add(1, Ordering::SeqCst);
                Ok(A2aDispatchAck::Stopped)
            } else {
                Ok(A2aDispatchAck::Settled)
            }
        }
    }

    #[derive(Default)]
    struct BlockingAlreadyStoppedCancelHost {
        reconcile_cancel_calls: AtomicUsize,
        reconcile_started: Notify,
        release_reconcile: Notify,
        cancel_calls: AtomicUsize,
    }

    #[async_trait]
    impl A2aDispatchHost for BlockingAlreadyStoppedCancelHost {
        async fn reconcile_unknown_cancel(
            &self,
            _server: Arc<A2aHttpJsonRpcServer>,
            _record: &A2aCancellationOutboxRecord,
        ) -> ProtocolResult<A2aUnknownDispatchDecision> {
            self.reconcile_cancel_calls.fetch_add(1, Ordering::SeqCst);
            self.reconcile_started.notify_one();
            self.release_reconcile.notified().await;
            Ok(A2aUnknownDispatchDecision::AlreadyStopped)
        }

        async fn handle(
            &self,
            _server: Arc<A2aHttpJsonRpcServer>,
            _context: &A2aDispatchContext,
            _envelope: &GovernanceEnvelope,
            action: &A2aAction,
        ) -> ProtocolResult<A2aDispatchAck> {
            if matches!(action, A2aAction::CancelTask { .. }) {
                self.cancel_calls.fetch_add(1, Ordering::SeqCst);
            }
            Ok(A2aDispatchAck::Stopped)
        }
    }

    #[derive(Default)]
    struct AbortableCancellationReconcileHost {
        reconcile_cancel_calls: AtomicUsize,
        first_reconcile_started: Notify,
        release_first_reconcile: Notify,
    }

    #[async_trait]
    impl A2aDispatchHost for AbortableCancellationReconcileHost {
        async fn reconcile_unknown_cancel(
            &self,
            _server: Arc<A2aHttpJsonRpcServer>,
            _record: &A2aCancellationOutboxRecord,
        ) -> ProtocolResult<A2aUnknownDispatchDecision> {
            let call = self.reconcile_cancel_calls.fetch_add(1, Ordering::SeqCst);
            if call == 0 {
                self.first_reconcile_started.notify_one();
                self.release_first_reconcile.notified().await;
            }
            Ok(A2aUnknownDispatchDecision::SafeToRetry)
        }

        async fn handle(
            &self,
            _server: Arc<A2aHttpJsonRpcServer>,
            _context: &A2aDispatchContext,
            _envelope: &GovernanceEnvelope,
            _action: &A2aAction,
        ) -> ProtocolResult<A2aDispatchAck> {
            Ok(A2aDispatchAck::Stopped)
        }
    }

    struct CancelRegressionHost {
        safe_retry_unknown_cancel: bool,
        fail_first_cancel: bool,
        block_cancel: bool,
        message_calls: AtomicUsize,
        cancel_calls: AtomicUsize,
        reconcile_cancel_calls: AtomicUsize,
        cancel_started: Notify,
        release_cancel: Notify,
        observed_controls: StdMutex<Vec<(GovernanceEnvelope, A2aAction)>>,
    }

    impl CancelRegressionHost {
        fn new(safe_retry_unknown_cancel: bool, fail_first_cancel: bool) -> Self {
            Self {
                safe_retry_unknown_cancel,
                fail_first_cancel,
                block_cancel: false,
                message_calls: AtomicUsize::new(0),
                cancel_calls: AtomicUsize::new(0),
                reconcile_cancel_calls: AtomicUsize::new(0),
                cancel_started: Notify::new(),
                release_cancel: Notify::new(),
                observed_controls: StdMutex::new(Vec::new()),
            }
        }

        fn blocking() -> Self {
            Self {
                block_cancel: true,
                ..Self::new(false, false)
            }
        }
    }

    #[async_trait]
    impl A2aDispatchHost for CancelRegressionHost {
        async fn reconcile_unknown_cancel(
            &self,
            _server: Arc<A2aHttpJsonRpcServer>,
            _record: &A2aCancellationOutboxRecord,
        ) -> ProtocolResult<A2aUnknownDispatchDecision> {
            self.reconcile_cancel_calls.fetch_add(1, Ordering::SeqCst);
            Ok(if self.safe_retry_unknown_cancel {
                A2aUnknownDispatchDecision::SafeToRetry
            } else {
                A2aUnknownDispatchDecision::ReconcileRequired
            })
        }

        async fn handle(
            &self,
            _server: Arc<A2aHttpJsonRpcServer>,
            _context: &A2aDispatchContext,
            envelope: &GovernanceEnvelope,
            action: &A2aAction,
        ) -> ProtocolResult<A2aDispatchAck> {
            if !matches!(action, A2aAction::CancelTask { .. }) {
                self.message_calls.fetch_add(1, Ordering::SeqCst);
                return Ok(A2aDispatchAck::Settled);
            }
            let attempt = self.cancel_calls.fetch_add(1, Ordering::SeqCst) + 1;
            self.observed_controls
                .lock()
                .unwrap()
                .push((envelope.clone(), action.clone()));
            self.cancel_started.notify_one();
            if self.block_cancel {
                self.release_cancel.notified().await;
            }
            if self.fail_first_cancel && attempt == 1 {
                Err(ProtocolError::conflict(
                    "test cancellation effect completed before receipt",
                ))
            } else {
                Ok(A2aDispatchAck::Stopped)
            }
        }
    }

    struct CompletionAfterCancelHost {
        message_started: Notify,
        completion_attempts: AtomicUsize,
        completion_side_effects: AtomicUsize,
        completion_rejections: AtomicUsize,
        cancel_calls: AtomicUsize,
    }

    impl CompletionAfterCancelHost {
        fn new() -> Self {
            Self {
                message_started: Notify::new(),
                completion_attempts: AtomicUsize::new(0),
                completion_side_effects: AtomicUsize::new(0),
                completion_rejections: AtomicUsize::new(0),
                cancel_calls: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl A2aDispatchHost for CompletionAfterCancelHost {
        async fn handle(
            &self,
            server: Arc<A2aHttpJsonRpcServer>,
            context: &A2aDispatchContext,
            _envelope: &GovernanceEnvelope,
            action: &A2aAction,
        ) -> ProtocolResult<A2aDispatchAck> {
            match action {
                A2aAction::DispatchMessage { .. } | A2aAction::DuplicateMessage { .. } => {}
                A2aAction::CancelTask { .. } => {
                    self.cancel_calls.fetch_add(1, Ordering::SeqCst);
                    return Ok(A2aDispatchAck::Stopped);
                }
                _ => return Ok(A2aDispatchAck::Settled),
            }
            self.message_started.notify_one();
            context.cancellation.cancelled().await;
            self.completion_attempts.fetch_add(1, Ordering::SeqCst);
            match server
                .transition_dispatch_task(
                    context,
                    A2aTaskState::Completed,
                    Some("late completion after cancellation".into()),
                )
                .await
            {
                Ok(_) => {
                    self.completion_side_effects.fetch_add(1, Ordering::SeqCst);
                }
                Err(_) => {
                    self.completion_rejections.fetch_add(1, Ordering::SeqCst);
                }
            }
            Err(ProtocolError::conflict(
                "message execution stopped after durable cancellation",
            ))
        }
    }

    struct SelectiveBlockingCancelHost {
        blocked_task_id: String,
        blocked_started: Notify,
        release_blocked: Notify,
        blocked_calls: AtomicUsize,
        other_calls: AtomicUsize,
    }

    #[async_trait]
    impl A2aDispatchHost for SelectiveBlockingCancelHost {
        async fn handle(
            &self,
            _server: Arc<A2aHttpJsonRpcServer>,
            _context: &A2aDispatchContext,
            _envelope: &GovernanceEnvelope,
            action: &A2aAction,
        ) -> ProtocolResult<A2aDispatchAck> {
            let A2aAction::CancelTask { task } = action else {
                return Ok(A2aDispatchAck::Settled);
            };
            if task.mapping.task_id == self.blocked_task_id {
                self.blocked_calls.fetch_add(1, Ordering::SeqCst);
                self.blocked_started.notify_one();
                self.release_blocked.notified().await;
            } else {
                self.other_calls.fetch_add(1, Ordering::SeqCst);
            }
            Ok(A2aDispatchAck::Stopped)
        }
    }

    const PANIC_PAYLOAD: &str = "host-panic-payload-must-not-leak";

    struct PanicIsolationHost {
        panic_message_id: Option<String>,
        panic_dispatch_reconcile: bool,
        panic_cancel_reconcile: bool,
        handle_calls: AtomicUsize,
        healthy_calls: AtomicUsize,
        dispatch_reconcile_calls: AtomicUsize,
        cancel_reconcile_calls: AtomicUsize,
    }

    impl PanicIsolationHost {
        fn panicking_handle(message_id: &str) -> Self {
            Self {
                panic_message_id: Some(message_id.to_owned()),
                panic_dispatch_reconcile: false,
                panic_cancel_reconcile: false,
                handle_calls: AtomicUsize::new(0),
                healthy_calls: AtomicUsize::new(0),
                dispatch_reconcile_calls: AtomicUsize::new(0),
                cancel_reconcile_calls: AtomicUsize::new(0),
            }
        }

        fn panicking_reconciliation() -> Self {
            Self {
                panic_message_id: None,
                panic_dispatch_reconcile: true,
                panic_cancel_reconcile: true,
                handle_calls: AtomicUsize::new(0),
                healthy_calls: AtomicUsize::new(0),
                dispatch_reconcile_calls: AtomicUsize::new(0),
                cancel_reconcile_calls: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl A2aDispatchHost for PanicIsolationHost {
        async fn reconcile_unknown(
            &self,
            _server: Arc<A2aHttpJsonRpcServer>,
            _record: &A2aDispatchOutboxRecord,
        ) -> ProtocolResult<A2aUnknownDispatchDecision> {
            self.dispatch_reconcile_calls.fetch_add(1, Ordering::SeqCst);
            if self.panic_dispatch_reconcile {
                panic!("{PANIC_PAYLOAD}");
            }
            Ok(A2aUnknownDispatchDecision::ReconcileRequired)
        }

        async fn reconcile_unknown_cancel(
            &self,
            _server: Arc<A2aHttpJsonRpcServer>,
            _record: &A2aCancellationOutboxRecord,
        ) -> ProtocolResult<A2aUnknownDispatchDecision> {
            self.cancel_reconcile_calls.fetch_add(1, Ordering::SeqCst);
            if self.panic_cancel_reconcile {
                panic!("{PANIC_PAYLOAD}");
            }
            Ok(A2aUnknownDispatchDecision::ReconcileRequired)
        }

        async fn handle(
            &self,
            server: Arc<A2aHttpJsonRpcServer>,
            context: &A2aDispatchContext,
            _envelope: &GovernanceEnvelope,
            action: &A2aAction,
        ) -> ProtocolResult<A2aDispatchAck> {
            self.handle_calls.fetch_add(1, Ordering::SeqCst);
            let message_id = match action {
                A2aAction::DispatchMessage { message, .. } => message.message_id.as_str(),
                A2aAction::DuplicateMessage { receipt } => receipt.message.message_id.as_str(),
                A2aAction::CancelTask { .. } => return Ok(A2aDispatchAck::Stopped),
                _ => return Ok(A2aDispatchAck::Settled),
            };
            if self.panic_message_id.as_deref() == Some(message_id) {
                panic!("{PANIC_PAYLOAD}");
            }
            self.healthy_calls.fetch_add(1, Ordering::SeqCst);
            server
                .transition_dispatch_task(context, A2aTaskState::Completed, None)
                .await?;
            Ok(A2aDispatchAck::Settled)
        }
    }

    #[derive(Debug, Clone, Copy)]
    enum DropPanicFutureMode {
        Ready,
        Pending,
    }

    struct DropPanickingHost {
        mode: DropPanicFutureMode,
        drop_count: Arc<AtomicUsize>,
        started: Arc<Notify>,
    }

    struct DropPanickingHostFuture {
        mode: DropPanicFutureMode,
        drop_count: Arc<AtomicUsize>,
        started: Arc<Notify>,
    }

    impl Future for DropPanickingHostFuture {
        type Output = ProtocolResult<A2aDispatchAck>;

        fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
            let this = self.get_mut();
            this.started.notify_one();
            match this.mode {
                DropPanicFutureMode::Ready => Poll::Ready(Ok(A2aDispatchAck::Settled)),
                DropPanicFutureMode::Pending => Poll::Pending,
            }
        }
    }

    impl Drop for DropPanickingHostFuture {
        fn drop(&mut self) {
            self.drop_count.fetch_add(1, Ordering::SeqCst);
            panic!("{PANIC_PAYLOAD}");
        }
    }

    impl A2aDispatchHost for DropPanickingHost {
        fn handle<'life0, 'life1, 'life2, 'life3, 'async_trait>(
            &'life0 self,
            _server: Arc<A2aHttpJsonRpcServer>,
            _context: &'life1 A2aDispatchContext,
            _envelope: &'life2 GovernanceEnvelope,
            _action: &'life3 A2aAction,
        ) -> Pin<Box<dyn Future<Output = ProtocolResult<A2aDispatchAck>> + Send + 'async_trait>>
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            'life2: 'async_trait,
            'life3: 'async_trait,
            Self: 'async_trait,
        {
            Box::pin(DropPanickingHostFuture {
                mode: self.mode,
                drop_count: self.drop_count.clone(),
                started: self.started.clone(),
            })
        }
    }

    #[derive(Debug, Clone, Copy)]
    enum AppendCorruption {
        Tenant,
        Task,
        Context,
        LogicalId,
    }

    struct MaliciousAppendStore {
        corruption: AppendCorruption,
    }

    #[async_trait]
    impl A2aEventStore for MaliciousAppendStore {
        async fn append(
            &self,
            logical_event_id: &str,
            owner: &A2aEventOwner,
            task_id: &str,
            response: &A2aStreamResponse,
            _retention: A2aEventRetention,
        ) -> Result<A2aEventAppendOutcome, A2aEventAppendError> {
            let mut returned_owner = owner.clone();
            let mut returned_task_id = task_id.to_owned();
            let mut returned_response = response.clone();
            let mut returned_logical_id = logical_event_id.to_owned();
            match self.corruption {
                AppendCorruption::Tenant => returned_owner.tenant_id = Some("tenant-evil".into()),
                AppendCorruption::Task => returned_task_id = "task-evil".into(),
                AppendCorruption::Context => match &mut returned_response {
                    A2aStreamResponse::Task(task) => task.context_id = "context-evil".into(),
                    A2aStreamResponse::StatusUpdate(update) => {
                        update.context_id = "context-evil".into();
                    }
                    A2aStreamResponse::Message(message) => {
                        message.context_id = Some("context-evil".into());
                    }
                },
                AppendCorruption::LogicalId => returned_logical_id = "event-evil".into(),
            }
            Ok(A2aEventAppendOutcome::Inserted(A2aPersistedEvent {
                logical_event_id: returned_logical_id,
                event_id: 1,
                owner: returned_owner,
                task_id: returned_task_id,
                response: returned_response,
            }))
        }

        async fn replay_page(
            &self,
            _owner: &A2aEventOwner,
            _task_id: &str,
            _after_event_id: Option<u64>,
            _through_high_water: Option<u64>,
            _limits: A2aReplayLimits,
        ) -> Result<A2aReplayPage, A2aEventStoreError> {
            Ok(A2aReplayPage {
                events: Vec::new(),
                high_water: 0,
            })
        }
    }

    #[derive(Debug, Clone, Copy)]
    enum ReplayAttack {
        NonMonotonic,
        Gap,
    }

    struct MaliciousReplayStore {
        attack: ReplayAttack,
    }

    #[async_trait]
    impl A2aEventStore for MaliciousReplayStore {
        async fn append(
            &self,
            _logical_event_id: &str,
            _owner: &A2aEventOwner,
            _task_id: &str,
            _response: &A2aStreamResponse,
            _retention: A2aEventRetention,
        ) -> Result<A2aEventAppendOutcome, A2aEventAppendError> {
            Err(A2aEventAppendError::permanent(ProtocolError::conflict(
                "append is not used by replay test",
            )))
        }

        async fn replay_page(
            &self,
            owner: &A2aEventOwner,
            task_id: &str,
            _after_event_id: Option<u64>,
            _through_high_water: Option<u64>,
            _limits: A2aReplayLimits,
        ) -> Result<A2aReplayPage, A2aEventStoreError> {
            match self.attack {
                ReplayAttack::Gap => Err(A2aEventStoreError::RetentionGap),
                ReplayAttack::NonMonotonic => Ok(A2aReplayPage {
                    events: vec![A2aPersistedEvent {
                        logical_event_id: "logical-replay-2".into(),
                        event_id: 2,
                        owner: owner.clone(),
                        task_id: task_id.to_owned(),
                        response: A2aStreamResponse::Task(A2aWireTask {
                            id: task_id.to_owned(),
                            context_id: "context-replay".into(),
                            status: A2aWireTaskStatus {
                                state: A2aTaskState::Working,
                            },
                            artifacts: None,
                        }),
                    }],
                    high_water: 2,
                }),
            }
        }
    }

    struct FailFirstAppendStore {
        inner: InMemoryA2aEventStore,
        attempts: AtomicUsize,
        inserted_terminal: AtomicUsize,
    }

    impl Default for FailFirstAppendStore {
        fn default() -> Self {
            Self {
                inner: InMemoryA2aEventStore::default(),
                attempts: AtomicUsize::new(0),
                inserted_terminal: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl A2aEventStore for FailFirstAppendStore {
        async fn append(
            &self,
            logical_event_id: &str,
            owner: &A2aEventOwner,
            task_id: &str,
            response: &A2aStreamResponse,
            retention: A2aEventRetention,
        ) -> Result<A2aEventAppendOutcome, A2aEventAppendError> {
            if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(A2aEventAppendError::retryable());
            }
            let outcome = self
                .inner
                .append(logical_event_id, owner, task_id, response, retention)
                .await?;
            if matches!(outcome, A2aEventAppendOutcome::Inserted(_))
                && outcome.event().response.is_terminal()
            {
                self.inserted_terminal.fetch_add(1, Ordering::SeqCst);
            }
            Ok(outcome)
        }

        async fn replay_page(
            &self,
            owner: &A2aEventOwner,
            task_id: &str,
            after_event_id: Option<u64>,
            through_high_water: Option<u64>,
            limits: A2aReplayLimits,
        ) -> Result<A2aReplayPage, A2aEventStoreError> {
            self.inner
                .replay_page(owner, task_id, after_event_id, through_high_water, limits)
                .await
        }
    }

    struct CommitThenAckLossEventStore {
        inner: InMemoryA2aEventStore,
        lose_first_ack: AtomicBool,
        acceptance_inserted: AtomicUsize,
        acceptance_existing: AtomicUsize,
        terminal_inserted: AtomicUsize,
        acceptance_logical_id: StdMutex<Option<String>>,
    }

    impl Default for CommitThenAckLossEventStore {
        fn default() -> Self {
            Self {
                inner: InMemoryA2aEventStore::default(),
                lose_first_ack: AtomicBool::new(true),
                acceptance_inserted: AtomicUsize::new(0),
                acceptance_existing: AtomicUsize::new(0),
                terminal_inserted: AtomicUsize::new(0),
                acceptance_logical_id: StdMutex::new(None),
            }
        }
    }

    #[async_trait]
    impl A2aEventStore for CommitThenAckLossEventStore {
        async fn append(
            &self,
            logical_event_id: &str,
            owner: &A2aEventOwner,
            task_id: &str,
            response: &A2aStreamResponse,
            retention: A2aEventRetention,
        ) -> Result<A2aEventAppendOutcome, A2aEventAppendError> {
            let outcome = self
                .inner
                .append(logical_event_id, owner, task_id, response, retention)
                .await?;
            if matches!(outcome, A2aEventAppendOutcome::Inserted(_))
                && self.lose_first_ack.swap(false, Ordering::SeqCst)
            {
                self.acceptance_inserted.fetch_add(1, Ordering::SeqCst);
                *self.acceptance_logical_id.lock().unwrap() = Some(logical_event_id.to_owned());
                return Err(A2aEventAppendError::retryable());
            }
            let acceptance_logical_id = self.acceptance_logical_id.lock().unwrap().clone();
            if matches!(outcome, A2aEventAppendOutcome::Existing(_))
                && acceptance_logical_id.as_deref() == Some(logical_event_id)
            {
                self.acceptance_existing.fetch_add(1, Ordering::SeqCst);
            }
            if matches!(outcome, A2aEventAppendOutcome::Inserted(_))
                && outcome.event().response.is_terminal()
            {
                self.terminal_inserted.fetch_add(1, Ordering::SeqCst);
            }
            Ok(outcome)
        }

        async fn replay_page(
            &self,
            owner: &A2aEventOwner,
            task_id: &str,
            after_event_id: Option<u64>,
            through_high_water: Option<u64>,
            limits: A2aReplayLimits,
        ) -> Result<A2aReplayPage, A2aEventStoreError> {
            self.inner
                .replay_page(owner, task_id, after_event_id, through_high_water, limits)
                .await
        }
    }

    struct BlockingAppendEventStore {
        inner: InMemoryA2aEventStore,
        blocked: AtomicBool,
        attempts: AtomicUsize,
        entered: Notify,
        release: Notify,
    }

    impl Default for BlockingAppendEventStore {
        fn default() -> Self {
            Self {
                inner: InMemoryA2aEventStore::default(),
                blocked: AtomicBool::new(true),
                attempts: AtomicUsize::new(0),
                entered: Notify::new(),
                release: Notify::new(),
            }
        }
    }

    impl BlockingAppendEventStore {
        fn release(&self) {
            self.blocked.store(false, Ordering::SeqCst);
            self.release.notify_waiters();
        }
    }

    #[async_trait]
    impl A2aEventStore for BlockingAppendEventStore {
        async fn append(
            &self,
            logical_event_id: &str,
            owner: &A2aEventOwner,
            task_id: &str,
            response: &A2aStreamResponse,
            retention: A2aEventRetention,
        ) -> Result<A2aEventAppendOutcome, A2aEventAppendError> {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            self.entered.notify_one();
            loop {
                let released = self.release.notified();
                if !self.blocked.load(Ordering::SeqCst) {
                    break;
                }
                released.await;
            }
            self.inner
                .append(logical_event_id, owner, task_id, response, retention)
                .await
        }

        async fn replay_page(
            &self,
            owner: &A2aEventOwner,
            task_id: &str,
            after_event_id: Option<u64>,
            through_high_water: Option<u64>,
            limits: A2aReplayLimits,
        ) -> Result<A2aReplayPage, A2aEventStoreError> {
            self.inner
                .replay_page(owner, task_id, after_event_id, through_high_water, limits)
                .await
        }
    }

    struct FirstAppendBarrierEventStore {
        inner: InMemoryA2aEventStore,
        attempts: AtomicUsize,
        first_entered: Notify,
        first_released: AtomicBool,
        release_first: Notify,
    }

    impl Default for FirstAppendBarrierEventStore {
        fn default() -> Self {
            Self {
                inner: InMemoryA2aEventStore::default(),
                attempts: AtomicUsize::new(0),
                first_entered: Notify::new(),
                first_released: AtomicBool::new(false),
                release_first: Notify::new(),
            }
        }
    }

    impl FirstAppendBarrierEventStore {
        fn release_first(&self) {
            self.first_released.store(true, Ordering::SeqCst);
            self.release_first.notify_waiters();
        }
    }

    #[async_trait]
    impl A2aEventStore for FirstAppendBarrierEventStore {
        async fn append(
            &self,
            logical_event_id: &str,
            owner: &A2aEventOwner,
            task_id: &str,
            response: &A2aStreamResponse,
            retention: A2aEventRetention,
        ) -> Result<A2aEventAppendOutcome, A2aEventAppendError> {
            if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                self.first_entered.notify_one();
                loop {
                    let released = self.release_first.notified();
                    if self.first_released.load(Ordering::SeqCst) {
                        break;
                    }
                    released.await;
                }
            }
            self.inner
                .append(logical_event_id, owner, task_id, response, retention)
                .await
        }

        async fn replay_page(
            &self,
            owner: &A2aEventOwner,
            task_id: &str,
            after_event_id: Option<u64>,
            through_high_water: Option<u64>,
            limits: A2aReplayLimits,
        ) -> Result<A2aReplayPage, A2aEventStoreError> {
            self.inner
                .replay_page(owner, task_id, after_event_id, through_high_water, limits)
                .await
        }
    }

    struct AmbiguousTwiceEventSettlementSnapshotStore {
        snapshot: Mutex<Option<A2aSerializedMapperSnapshot>>,
        failures_remaining: AtomicUsize,
        failures: AtomicUsize,
    }

    impl Default for AmbiguousTwiceEventSettlementSnapshotStore {
        fn default() -> Self {
            Self {
                snapshot: Mutex::new(None),
                failures_remaining: AtomicUsize::new(2),
                failures: AtomicUsize::new(0),
            }
        }
    }

    impl AmbiguousTwiceEventSettlementSnapshotStore {
        async fn load_snapshot(&self) -> Option<A2aMapper> {
            self.snapshot.lock().await.clone()?.decode().ok()
        }

        async fn persist_snapshot(&self, candidate: &A2aMapper) -> ProtocolResult<()> {
            let candidate = A2aSerializedMapperSnapshot::from_mapper(candidate)?;
            self.compare_and_swap_snapshot(None, candidate)
                .await
                .map_err(A2aSnapshotStoreError::into_protocol_error)?;
            Ok(())
        }
    }

    #[async_trait]
    impl A2aMapperSnapshotStore for AmbiguousTwiceEventSettlementSnapshotStore {
        async fn compare_and_swap_snapshot(
            &self,
            expected: Option<A2aSnapshotVersion>,
            candidate: A2aSerializedMapperSnapshot,
        ) -> Result<A2aSnapshotCommitOutcome, A2aSnapshotStoreError> {
            let mut snapshot = self.snapshot.lock().await;
            if let Some(current) = snapshot.as_ref() {
                if current.revision() == candidate.revision() {
                    return if current.digest() == candidate.digest() {
                        Ok(A2aSnapshotCommitOutcome::AlreadyApplied)
                    } else {
                        Err(A2aSnapshotStoreError::definite(ProtocolError::conflict(
                            "A2A snapshot revision already exists with a different digest",
                        )))
                    };
                }
                if expected.as_ref() != Some(&current.version())
                    || candidate.revision() <= current.revision()
                {
                    return Err(A2aSnapshotStoreError::definite(ProtocolError::conflict(
                        "A2A snapshot compare-and-swap revision conflict",
                    )));
                }
            } else if expected.is_some() {
                return Err(A2aSnapshotStoreError::definite(ProtocolError::conflict(
                    "A2A snapshot compare-and-swap expected a missing revision",
                )));
            }
            let candidate_mapper = candidate
                .decode()
                .map_err(A2aSnapshotStoreError::definite)?;
            let previous_mapper = snapshot
                .as_ref()
                .and_then(|previous| previous.decode().ok());
            let newly_settled = previous_mapper.as_ref().is_some_and(|previous| {
                candidate_mapper
                    .pending_event_intents()
                    .iter()
                    .any(|(event_id, event)| {
                        event.state == A2aPendingEventState::Settled
                            && previous.pending_event_intents().get(event_id).is_some_and(
                                |previous| previous.state != A2aPendingEventState::Settled,
                            )
                    })
            });
            if newly_settled
                && self
                    .failures_remaining
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                        remaining.checked_sub(1)
                    })
                    .is_ok()
            {
                self.failures.fetch_add(1, Ordering::SeqCst);
                return Err(A2aSnapshotStoreError::unknown(ProtocolError::conflict(
                    "injected ambiguous event-settlement snapshot failure before apply",
                )));
            }
            *snapshot = Some(candidate);
            Ok(A2aSnapshotCommitOutcome::Applied)
        }

        async fn lookup_snapshot_version(&self) -> ProtocolResult<Option<A2aSnapshotVersion>> {
            Ok(self
                .snapshot
                .lock()
                .await
                .as_ref()
                .map(|value| value.version()))
        }

        async fn load_serialized_snapshot(
            &self,
        ) -> ProtocolResult<Option<A2aSerializedMapperSnapshot>> {
            Ok(self.snapshot.lock().await.clone())
        }
    }

    #[derive(Default)]
    struct BlockingSnapshotStore {
        inner: InMemoryA2aMapperSnapshotStore,
        block_next: AtomicBool,
        entered: Notify,
        release: Notify,
    }

    impl BlockingSnapshotStore {
        async fn initialize(&self, mapper: &A2aMapper) -> ProtocolResult<()> {
            self.inner.persist_snapshot(mapper).await
        }
    }

    #[async_trait]
    impl A2aMapperSnapshotStore for BlockingSnapshotStore {
        async fn compare_and_swap_snapshot(
            &self,
            expected: Option<A2aSnapshotVersion>,
            candidate: A2aSerializedMapperSnapshot,
        ) -> Result<A2aSnapshotCommitOutcome, A2aSnapshotStoreError> {
            if self.block_next.swap(false, Ordering::SeqCst) {
                self.entered.notify_one();
                self.release.notified().await;
            }
            self.inner
                .compare_and_swap_snapshot(expected, candidate)
                .await
        }

        async fn lookup_snapshot_version(&self) -> ProtocolResult<Option<A2aSnapshotVersion>> {
            self.inner.lookup_snapshot_version().await
        }

        async fn load_serialized_snapshot(
            &self,
        ) -> ProtocolResult<Option<A2aSerializedMapperSnapshot>> {
            self.inner.load_serialized_snapshot().await
        }
    }

    #[derive(Default)]
    struct AckLossSnapshotStore {
        inner: InMemoryA2aMapperSnapshotStore,
        errors_remaining: AtomicUsize,
        injected: AtomicUsize,
    }

    #[allow(dead_code)]
    #[derive(Default)]
    struct FailNextSnapshotStore {
        inner: InMemoryA2aMapperSnapshotStore,
        failures_remaining: AtomicUsize,
    }

    #[derive(Default)]
    struct CompetingAdvanceThenDefiniteStore {
        inner: InMemoryA2aMapperSnapshotStore,
        inject_next: AtomicBool,
        task_id: StdMutex<Option<String>>,
    }

    impl CompetingAdvanceThenDefiniteStore {
        async fn initialize(&self, mapper: &A2aMapper, task_id: &str) -> ProtocolResult<()> {
            self.inner.persist_snapshot(mapper).await?;
            *self.task_id.lock().unwrap() = Some(task_id.to_owned());
            Ok(())
        }

        fn inject_next(&self) {
            self.inject_next.store(true, Ordering::SeqCst);
        }
    }

    #[allow(dead_code)]
    impl FailNextSnapshotStore {
        async fn initialize(&self, mapper: &A2aMapper) -> ProtocolResult<()> {
            self.inner.persist_snapshot(mapper).await
        }

        fn fail_next_commit(&self) {
            self.failures_remaining.store(1, Ordering::SeqCst);
        }
    }

    #[async_trait]
    impl A2aMapperSnapshotStore for FailNextSnapshotStore {
        async fn compare_and_swap_snapshot(
            &self,
            expected: Option<A2aSnapshotVersion>,
            candidate: A2aSerializedMapperSnapshot,
        ) -> Result<A2aSnapshotCommitOutcome, A2aSnapshotStoreError> {
            if expected.is_some()
                && self
                    .failures_remaining
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                        remaining.checked_sub(1)
                    })
                    .is_ok()
            {
                return Err(A2aSnapshotStoreError::definite(ProtocolError::conflict(
                    "injected snapshot failure before durable apply",
                )));
            }
            self.inner
                .compare_and_swap_snapshot(expected, candidate)
                .await
        }

        async fn lookup_snapshot_version(&self) -> ProtocolResult<Option<A2aSnapshotVersion>> {
            self.inner.lookup_snapshot_version().await
        }

        async fn load_serialized_snapshot(
            &self,
        ) -> ProtocolResult<Option<A2aSerializedMapperSnapshot>> {
            self.inner.load_serialized_snapshot().await
        }
    }

    #[async_trait]
    impl A2aMapperSnapshotStore for CompetingAdvanceThenDefiniteStore {
        async fn compare_and_swap_snapshot(
            &self,
            expected: Option<A2aSnapshotVersion>,
            candidate: A2aSerializedMapperSnapshot,
        ) -> Result<A2aSnapshotCommitOutcome, A2aSnapshotStoreError> {
            if expected.is_some() && self.inject_next.swap(false, Ordering::SeqCst) {
                let current = self
                    .inner
                    .load_serialized_snapshot()
                    .await
                    .map_err(A2aSnapshotStoreError::unknown)?
                    .ok_or_else(|| {
                        A2aSnapshotStoreError::unknown(ProtocolError::conflict(
                            "competing snapshot test store lost its durable head",
                        ))
                    })?;
                let mut competing = current.decode().map_err(A2aSnapshotStoreError::unknown)?;
                let task_id = self
                    .task_id
                    .lock()
                    .unwrap()
                    .clone()
                    .expect("competing snapshot test task is initialized");
                competing
                    .transition_task(
                        &task_id,
                        A2aTaskState::Failed,
                        Some("competing writer".into()),
                    )
                    .map_err(A2aSnapshotStoreError::unknown)?;
                let competing = A2aSerializedMapperSnapshot::from_mapper(&competing)
                    .map_err(A2aSnapshotStoreError::unknown)?;
                self.inner
                    .compare_and_swap_snapshot(expected, competing)
                    .await?;
                return Err(A2aSnapshotStoreError::definite(ProtocolError::conflict(
                    "injected definite conflict after a competing durable advance",
                )));
            }
            self.inner
                .compare_and_swap_snapshot(expected, candidate)
                .await
        }

        async fn lookup_snapshot_version(&self) -> ProtocolResult<Option<A2aSnapshotVersion>> {
            self.inner.lookup_snapshot_version().await
        }

        async fn load_serialized_snapshot(
            &self,
        ) -> ProtocolResult<Option<A2aSerializedMapperSnapshot>> {
            self.inner.load_serialized_snapshot().await
        }
    }

    impl AckLossSnapshotStore {
        async fn initialize(&self, mapper: &A2aMapper) -> ProtocolResult<()> {
            self.inner.persist_snapshot(mapper).await
        }

        fn fail_after_apply(&self, count: usize) {
            self.errors_remaining.store(count, Ordering::SeqCst);
        }
    }

    #[async_trait]
    impl A2aMapperSnapshotStore for AckLossSnapshotStore {
        async fn compare_and_swap_snapshot(
            &self,
            expected: Option<A2aSnapshotVersion>,
            candidate: A2aSerializedMapperSnapshot,
        ) -> Result<A2aSnapshotCommitOutcome, A2aSnapshotStoreError> {
            let outcome = self
                .inner
                .compare_and_swap_snapshot(expected, candidate)
                .await?;
            if self
                .errors_remaining
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
            {
                self.injected.fetch_add(1, Ordering::SeqCst);
                return Err(A2aSnapshotStoreError::unknown(ProtocolError::conflict(
                    "injected acknowledgement loss after snapshot apply",
                )));
            }
            Ok(outcome)
        }

        async fn lookup_snapshot_version(&self) -> ProtocolResult<Option<A2aSnapshotVersion>> {
            self.inner.lookup_snapshot_version().await
        }

        async fn load_serialized_snapshot(
            &self,
        ) -> ProtocolResult<Option<A2aSerializedMapperSnapshot>> {
            self.inner.load_serialized_snapshot().await
        }
    }

    #[derive(Default)]
    struct FinalProbeFailureSnapshotStore {
        inner: InMemoryA2aMapperSnapshotStore,
        candidate_cas_calls: AtomicUsize,
        fail_next_probe: AtomicBool,
    }

    #[async_trait]
    impl A2aMapperSnapshotStore for FinalProbeFailureSnapshotStore {
        async fn compare_and_swap_snapshot(
            &self,
            expected: Option<A2aSnapshotVersion>,
            candidate: A2aSerializedMapperSnapshot,
        ) -> Result<A2aSnapshotCommitOutcome, A2aSnapshotStoreError> {
            if expected.is_none() {
                return self
                    .inner
                    .compare_and_swap_snapshot(expected, candidate)
                    .await;
            }
            match self.candidate_cas_calls.fetch_add(1, Ordering::SeqCst) {
                0 => Err(A2aSnapshotStoreError::unknown(ProtocolError::conflict(
                    "injected first ambiguous snapshot outcome before apply",
                ))),
                1 => {
                    self.inner
                        .compare_and_swap_snapshot(expected, candidate)
                        .await?;
                    self.fail_next_probe.store(true, Ordering::SeqCst);
                    Err(A2aSnapshotStoreError::unknown(ProtocolError::conflict(
                        "injected second ambiguous snapshot outcome after apply",
                    )))
                }
                _ => {
                    self.inner
                        .compare_and_swap_snapshot(expected, candidate)
                        .await
                }
            }
        }

        async fn lookup_snapshot_version(&self) -> ProtocolResult<Option<A2aSnapshotVersion>> {
            if self.fail_next_probe.swap(false, Ordering::SeqCst) {
                return Err(ProtocolError::conflict(
                    "injected final snapshot version probe failure",
                ));
            }
            self.inner.lookup_snapshot_version().await
        }

        async fn load_serialized_snapshot(
            &self,
        ) -> ProtocolResult<Option<A2aSerializedMapperSnapshot>> {
            self.inner.load_serialized_snapshot().await
        }
    }

    #[derive(Default)]
    struct AppliedBlockingSnapshotStore {
        inner: InMemoryA2aMapperSnapshotStore,
        block_after_next_apply: AtomicBool,
        entered: Notify,
        release: Notify,
    }

    impl AppliedBlockingSnapshotStore {
        async fn initialize(&self, mapper: &A2aMapper) -> ProtocolResult<()> {
            self.inner.persist_snapshot(mapper).await
        }
    }

    #[async_trait]
    impl A2aMapperSnapshotStore for AppliedBlockingSnapshotStore {
        async fn compare_and_swap_snapshot(
            &self,
            expected: Option<A2aSnapshotVersion>,
            candidate: A2aSerializedMapperSnapshot,
        ) -> Result<A2aSnapshotCommitOutcome, A2aSnapshotStoreError> {
            let outcome = self
                .inner
                .compare_and_swap_snapshot(expected, candidate)
                .await?;
            if outcome == A2aSnapshotCommitOutcome::Applied
                && self.block_after_next_apply.swap(false, Ordering::SeqCst)
            {
                self.entered.notify_one();
                self.release.notified().await;
            }
            Ok(outcome)
        }

        async fn lookup_snapshot_version(&self) -> ProtocolResult<Option<A2aSnapshotVersion>> {
            self.inner.lookup_snapshot_version().await
        }

        async fn load_serialized_snapshot(
            &self,
        ) -> ProtocolResult<Option<A2aSerializedMapperSnapshot>> {
            self.inner.load_serialized_snapshot().await
        }
    }

    #[derive(Default)]
    struct ObservingEventStore {
        inner: InMemoryA2aEventStore,
        inserted: AtomicUsize,
        existing: AtomicUsize,
    }

    #[async_trait]
    impl A2aEventStore for ObservingEventStore {
        async fn append(
            &self,
            logical_event_id: &str,
            owner: &A2aEventOwner,
            task_id: &str,
            response: &A2aStreamResponse,
            retention: A2aEventRetention,
        ) -> Result<A2aEventAppendOutcome, A2aEventAppendError> {
            let outcome = self
                .inner
                .append(logical_event_id, owner, task_id, response, retention)
                .await?;
            match &outcome {
                A2aEventAppendOutcome::Inserted(_) => {
                    self.inserted.fetch_add(1, Ordering::SeqCst);
                }
                A2aEventAppendOutcome::Existing(_) => {
                    self.existing.fetch_add(1, Ordering::SeqCst);
                }
            }
            Ok(outcome)
        }

        async fn replay_page(
            &self,
            owner: &A2aEventOwner,
            task_id: &str,
            after_event_id: Option<u64>,
            through_high_water: Option<u64>,
            limits: A2aReplayLimits,
        ) -> Result<A2aReplayPage, A2aEventStoreError> {
            self.inner
                .replay_page(owner, task_id, after_event_id, through_high_water, limits)
                .await
        }
    }

    #[derive(Default)]
    struct GatedAppendStore {
        inner: InMemoryA2aEventStore,
        open: AtomicBool,
        attempts: AtomicUsize,
    }

    #[async_trait]
    impl A2aEventStore for GatedAppendStore {
        async fn append(
            &self,
            logical_event_id: &str,
            owner: &A2aEventOwner,
            task_id: &str,
            response: &A2aStreamResponse,
            retention: A2aEventRetention,
        ) -> Result<A2aEventAppendOutcome, A2aEventAppendError> {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            if !self.open.load(Ordering::SeqCst) {
                return Err(A2aEventAppendError::retryable());
            }
            self.inner
                .append(logical_event_id, owner, task_id, response, retention)
                .await
        }

        async fn replay_page(
            &self,
            owner: &A2aEventOwner,
            task_id: &str,
            after_event_id: Option<u64>,
            through_high_water: Option<u64>,
            limits: A2aReplayLimits,
        ) -> Result<A2aReplayPage, A2aEventStoreError> {
            self.inner
                .replay_page(owner, task_id, after_event_id, through_high_water, limits)
                .await
        }
    }

    #[derive(Default)]
    struct PermanentFailAppendStore {
        attempts: AtomicUsize,
    }

    #[async_trait]
    impl A2aEventStore for PermanentFailAppendStore {
        async fn append(
            &self,
            _logical_event_id: &str,
            _owner: &A2aEventOwner,
            _task_id: &str,
            _response: &A2aStreamResponse,
            _retention: A2aEventRetention,
        ) -> Result<A2aEventAppendOutcome, A2aEventAppendError> {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            Err(A2aEventAppendError::permanent(ProtocolError::conflict(
                "injected deterministic append rejection",
            )))
        }

        async fn replay_page(
            &self,
            _owner: &A2aEventOwner,
            _task_id: &str,
            _after_event_id: Option<u64>,
            _through_high_water: Option<u64>,
            _limits: A2aReplayLimits,
        ) -> Result<A2aReplayPage, A2aEventStoreError> {
            Ok(A2aReplayPage {
                events: Vec::new(),
                high_water: 0,
            })
        }
    }

    struct SelectivePoisonAppendStore {
        inner: InMemoryA2aEventStore,
        poisoned_task_id: String,
        poisoned: AtomicUsize,
        healthy: AtomicUsize,
    }

    #[async_trait]
    impl A2aEventStore for SelectivePoisonAppendStore {
        async fn append(
            &self,
            logical_event_id: &str,
            owner: &A2aEventOwner,
            task_id: &str,
            response: &A2aStreamResponse,
            retention: A2aEventRetention,
        ) -> Result<A2aEventAppendOutcome, A2aEventAppendError> {
            if task_id == self.poisoned_task_id {
                self.poisoned.fetch_add(1, Ordering::SeqCst);
                let mut corrupted = response.clone();
                match &mut corrupted {
                    A2aStreamResponse::Task(task) => task.context_id = "poisoned-context".into(),
                    A2aStreamResponse::StatusUpdate(update) => {
                        update.context_id = "poisoned-context".into();
                    }
                    A2aStreamResponse::Message(message) => {
                        message.context_id = Some("poisoned-context".into());
                    }
                }
                return Ok(A2aEventAppendOutcome::Inserted(A2aPersistedEvent {
                    logical_event_id: logical_event_id.to_owned(),
                    event_id: 1,
                    owner: owner.clone(),
                    task_id: task_id.to_owned(),
                    response: corrupted,
                }));
            }
            self.healthy.fetch_add(1, Ordering::SeqCst);
            self.inner
                .append(logical_event_id, owner, task_id, response, retention)
                .await
        }

        async fn replay_page(
            &self,
            owner: &A2aEventOwner,
            task_id: &str,
            after_event_id: Option<u64>,
            through_high_water: Option<u64>,
            limits: A2aReplayLimits,
        ) -> Result<A2aReplayPage, A2aEventStoreError> {
            self.inner
                .replay_page(owner, task_id, after_event_id, through_high_water, limits)
                .await
        }
    }

    #[async_trait]
    impl A2aDispatchHost for UncooperativeHost {
        async fn handle(
            &self,
            _server: Arc<A2aHttpJsonRpcServer>,
            _context: &A2aDispatchContext,
            _envelope: &GovernanceEnvelope,
            _action: &A2aAction,
        ) -> ProtocolResult<A2aDispatchAck> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.started.notify_one();
            tokio::time::sleep(self.delay).await;
            Ok(A2aDispatchAck::Settled)
        }
    }

    fn agent_card() -> A2aAgentCard {
        A2aAgentCard {
            name: "AIKit test agent".into(),
            description: "A governed A2A test agent".into(),
            version: "1.0.0".into(),
            capabilities: A2aAgentCapabilities {
                streaming: true,
                push_notifications: false,
                extended_agent_card: false,
            },
            skills: vec![A2aAgentSkill {
                id: "echo".into(),
                name: "Echo".into(),
                description: "Test task".into(),
                tags: vec!["test".into()],
                examples: vec!["Echo this text".into()],
                input_modes: vec!["text/plain".into()],
                output_modes: vec!["text/plain".into()],
                security_requirements: Vec::new(),
            }],
            supported_interfaces: vec![A2aAgentInterface {
                url: "http://localhost/a2a".into(),
                protocol_binding: "JSONRPC".into(),
                protocol_version: "1.0".into(),
                tenant: None,
            }],
            default_input_modes: vec!["text/plain".into()],
            default_output_modes: vec!["text/plain".into()],
            security_schemes: BTreeMap::from([(
                "bearer".into(),
                json!({"httpAuthSecurityScheme":{"scheme":"bearer"}}),
            )]),
            security_requirements: vec![json!({"schemes":{"bearer":{"list":[]}}})],
        }
    }

    fn large_agent_card() -> A2aAgentCard {
        let mut card = agent_card();
        let description = "x".repeat(4096);
        card.skills = (0..2048)
            .map(|index| A2aAgentSkill {
                id: format!("bulk-skill-{index}"),
                name: format!("Bulk skill {index}"),
                description: description.clone(),
                tags: vec!["bulk".into()],
                examples: Vec::new(),
                input_modes: vec!["text/plain".into()],
                output_modes: vec!["text/plain".into()],
                security_requirements: Vec::new(),
            })
            .collect();
        assert!(serde_json::to_vec(&card).unwrap().len() >= 8 * 1024 * 1024);
        card
    }

    async fn start_server(
        config: A2aHttpConfig,
    ) -> (
        SocketAddr,
        Arc<A2aHttpJsonRpcServer>,
        crate::cancellation::CancellationHandle,
        tokio::task::JoinHandle<ProtocolResult<()>>,
    ) {
        start_server_with_host(config, Arc::new(CompletingHost::default())).await
    }

    async fn start_server_with_host(
        config: A2aHttpConfig,
        host: Arc<dyn A2aDispatchHost>,
    ) -> (
        SocketAddr,
        Arc<A2aHttpJsonRpcServer>,
        crate::cancellation::CancellationHandle,
        tokio::task::JoinHandle<ProtocolResult<()>>,
    ) {
        start_server_with_dependencies(
            config,
            Arc::new(Mutex::new(A2aMapper::new())),
            Arc::new(InMemoryA2aMapperSnapshotStore::default()),
            Arc::new(InMemoryA2aEventStore::default()),
            host,
            agent_card(),
        )
        .await
    }

    async fn start_server_with_dependencies(
        mut config: A2aHttpConfig,
        mapper: Arc<Mutex<A2aMapper>>,
        snapshots: Arc<dyn A2aMapperSnapshotStore>,
        events: Arc<dyn A2aEventStore>,
        host: Arc<dyn A2aDispatchHost>,
        card: A2aAgentCard,
    ) -> (
        SocketAddr,
        Arc<A2aHttpJsonRpcServer>,
        crate::cancellation::CancellationHandle,
        tokio::task::JoinHandle<ProtocolResult<()>>,
    ) {
        config.allowed_hosts = ["localhost".into()].into_iter().collect();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let cancellation = CancellationToken::new();
        let handle = cancellation.handle();
        let server = Arc::new(
            A2aHttpJsonRpcServer::new(
                mapper,
                snapshots,
                events,
                Arc::new(TestAuthenticator),
                host,
                card,
                config,
            )
            .unwrap(),
        );
        let task = tokio::spawn(server.clone().serve(listener, cancellation));
        (address, server, handle, task)
    }

    async fn raw_http(address: SocketAddr, request: Vec<u8>) -> String {
        try_raw_http(address, request).await.unwrap()
    }

    async fn try_raw_http(address: SocketAddr, request: Vec<u8>) -> std::io::Result<String> {
        let mut stream = TcpStream::connect(address).await?;
        stream.write_all(&request).await?;
        stream.shutdown().await?;
        let mut response = Vec::new();
        timeout(Duration::from_secs(3), stream.read_to_end(&mut response))
            .await
            .expect("response timed out")?;
        String::from_utf8(response)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))
    }

    fn request(method: &str, path: &str, headers: &[(&str, &str)], body: &str) -> Vec<u8> {
        let mut request = format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\n");
        for (name, value) in headers {
            request.push_str(&format!("{name}: {value}\r\n"));
        }
        if method == "POST" {
            request.push_str(&format!("Content-Length: {}\r\n", body.len()));
        }
        request.push_str("\r\n");
        request.push_str(body);
        request.into_bytes()
    }

    fn rpc_headers(accept: &str) -> Vec<(&'static str, &str)> {
        vec![
            ("Authorization", "Bearer owner"),
            ("A2A-Version", "1.0"),
            ("Content-Type", "application/json"),
            ("Accept", accept),
        ]
    }

    fn body(response: &str) -> Value {
        serde_json::from_str(response.split_once("\r\n\r\n").unwrap().1).unwrap()
    }

    fn send_body(message_id: &str, complete: bool, return_immediately: Option<bool>) -> String {
        send_body_for_tenant(message_id, "tenant-a", complete, return_immediately)
    }

    fn send_rpc_request(message_id: &str) -> JsonRpcRequest {
        let value: Value = serde_json::from_str(&send_body(message_id, true, None)).unwrap();
        JsonRpcRequest {
            id: value["id"].clone(),
            method: value["method"].as_str().unwrap().to_owned(),
            params: value["params"].clone(),
            correlation_id: None,
            last_event_id: None,
        }
    }

    fn send_body_for_tenant(
        message_id: &str,
        tenant: &str,
        complete: bool,
        return_immediately: Option<bool>,
    ) -> String {
        let mut request = json!({
            "jsonrpc": "2.0",
            "id": message_id,
            "method": "SendMessage",
            "params": {
                "tenant": tenant,
                "message": {
                    "messageId": message_id,
                    "role": "ROLE_USER",
                    "parts": [{"text": "hello"}],
                    "metadata": {"complete": complete}
                }
            }
        });
        if let Some(return_immediately) = return_immediately {
            request["params"]["configuration"] = json!({"returnImmediately": return_immediately});
        }
        request.to_string()
    }

    fn owner_principal() -> ProtocolPrincipal {
        ProtocolPrincipal::new(
            "owner",
            ["a2a:message:send", "a2a:tasks:read", "a2a:tasks:cancel"],
        )
        .unwrap()
        .with_tenant("tenant-a")
        .unwrap()
    }

    fn other_principal() -> ProtocolPrincipal {
        ProtocolPrincipal::new(
            "other",
            ["a2a:message:send", "a2a:tasks:read", "a2a:tasks:cancel"],
        )
        .unwrap()
        .with_tenant("tenant-b")
        .unwrap()
    }

    fn third_principal() -> ProtocolPrincipal {
        ProtocolPrincipal::new(
            "third",
            ["a2a:message:send", "a2a:tasks:read", "a2a:tasks:cancel"],
        )
        .unwrap()
        .with_tenant("tenant-c")
        .unwrap()
    }

    fn seeded_message(message_id: &str) -> A2aMessage {
        A2aMessage {
            message_id: message_id.to_owned(),
            context_id: None,
            task_id: None,
            role: A2aRole::User,
            parts: vec![A2aPart::Text {
                text: "recovery".into(),
            }],
            metadata: BTreeMap::from([("complete".into(), Value::Bool(true))]),
        }
    }

    fn prepare_seeded_message_for(
        mapper: &mut A2aMapper,
        message_id: &str,
        principal: &ProtocolPrincipal,
    ) -> A2aRunMapping {
        let (_, action) = mapper
            .prepare_send_message(
                seeded_message(message_id),
                CorrelationIdentity::new(
                    format!("{message_id}-correlation"),
                    format!("{message_id}-request"),
                )
                .unwrap(),
                Some(principal),
            )
            .into_authorized()
            .unwrap();
        let A2aAction::DispatchMessage { mapping, .. } = action else {
            panic!("seeded message did not create a dispatch");
        };
        mapping
    }

    fn event_id_for_message(
        mapper: &A2aMapper,
        message_id: &str,
        principal: &ProtocolPrincipal,
    ) -> String {
        let matches: Vec<_> = mapper
            .pending_event_intents()
            .values()
            .filter(|event| {
                event.message_id.as_deref() == Some(message_id)
                    && event.owner_subject == principal.subject
                    && event.owner_tenant_id == principal.tenant_id
            })
            .map(|event| event.event_id.clone())
            .collect();
        assert_eq!(matches.len(), 1);
        matches[0].clone()
    }

    fn seeded_task(message_id: &str, task_state: A2aTaskState) -> (A2aMapper, A2aRunMapping) {
        let principal = owner_principal();
        let mut mapper = A2aMapper::new();
        let (_, action) = mapper
            .prepare_send_message(
                seeded_message(message_id),
                CorrelationIdentity::new(
                    format!("{message_id}-send-correlation"),
                    format!("{message_id}-send-request"),
                )
                .unwrap(),
                Some(&principal),
            )
            .into_authorized()
            .unwrap();
        let A2aAction::DispatchMessage { mapping, .. } = action else {
            panic!("seeded message did not create a dispatch");
        };
        let dispatch_id = mapper
            .dispatch_for_message(message_id, &principal)
            .unwrap()
            .dispatch_id
            .clone();
        mapper.mark_dispatch_settled(&dispatch_id).unwrap();
        if task_state != A2aTaskState::Working {
            mapper
                .transition_task(
                    &mapping.task_id,
                    task_state,
                    Some(format!("waiting in {task_state:?}")),
                )
                .unwrap();
        }
        (mapper, mapping)
    }

    fn seeded_cancellation(
        message_id: &str,
        task_state: A2aTaskState,
        running: bool,
    ) -> (A2aMapper, A2aRunMapping, A2aCancellationOutboxRecord) {
        let principal = owner_principal();
        let (mut mapper, mapping) = seeded_task(message_id, task_state);
        mapper
            .prepare_cancel_task(
                &mapping.task_id,
                CorrelationIdentity::new(
                    format!("{message_id}-cancel-correlation"),
                    format!("{message_id}-cancel-request"),
                )
                .unwrap(),
                Some(&principal),
            )
            .into_authorized()
            .unwrap();
        let cancellation = mapper
            .cancellation_for_task(&mapping.task_id, &principal)
            .cloned()
            .unwrap();
        if running {
            mapper
                .mark_cancellation_running(&cancellation.cancellation_id)
                .unwrap();
        }
        let cancellation = mapper
            .cancellation_for_task(&mapping.task_id, &principal)
            .cloned()
            .unwrap();
        (mapper, mapping, cancellation)
    }

    fn exhaust_cancellation_attempts(
        mapper: &mut A2aMapper,
        task_id: &str,
        principal: &ProtocolPrincipal,
    ) -> A2aCancellationOutboxRecord {
        loop {
            let current = mapper
                .cancellation_for_task(task_id, principal)
                .cloned()
                .unwrap();
            if current.attempts >= A2A_MAX_CANCELLATION_ATTEMPTS {
                if current.state == A2aCancellationOutboxState::Running {
                    mapper
                        .mark_cancellation_reconcile_pending(
                            &current.cancellation_id,
                            "seed exhausted cancellation",
                        )
                        .unwrap();
                }
                return mapper
                    .cancellation_for_task(task_id, principal)
                    .cloned()
                    .unwrap();
            }
            if current.state == A2aCancellationOutboxState::Running {
                mapper
                    .mark_cancellation_reconcile_pending(
                        &current.cancellation_id,
                        "seed retryable cancellation",
                    )
                    .unwrap();
            }
            mapper
                .mark_cancellation_running(&current.cancellation_id)
                .unwrap();
        }
    }

    fn resume_body(task_id: &str, context_id: &str, message_id: &str) -> String {
        json!({
            "jsonrpc": "2.0",
            "id": message_id,
            "method": "SendMessage",
            "params": {
                "tenant": "tenant-a",
                "message": {
                    "messageId": message_id,
                    "contextId": context_id,
                    "taskId": task_id,
                    "role": "ROLE_USER",
                    "parts": [{"text": "resume"}],
                    "metadata": {"complete": false}
                },
                "configuration": {"returnImmediately": true}
            }
        })
        .to_string()
    }

    fn cancel_body(task_id: &str, request_id: &str) -> String {
        cancel_body_for_tenant(task_id, request_id, "tenant-a")
    }

    fn cancel_body_for_tenant(task_id: &str, request_id: &str, tenant: &str) -> String {
        json!({
            "jsonrpc": "2.0",
            "id": request_id,
            "method": "CancelTask",
            "params": {"tenant": tenant, "id": task_id}
        })
        .to_string()
    }

    async fn wait_for_task_state(
        server: &A2aHttpJsonRpcServer,
        task_id: &str,
        expected: A2aTaskState,
    ) {
        timeout(Duration::from_secs(2), async {
            loop {
                if server
                    .mapper_snapshot()
                    .await
                    .tasks()
                    .get(task_id)
                    .is_some_and(|task| task.state == expected)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("task did not reach the expected state");
    }

    async fn wait_for_scheduler_idle(server: &A2aHttpJsonRpcServer) {
        timeout(Duration::from_secs(1), async {
            loop {
                let accepted = server.dispatch_state.lock().unwrap().accepted;
                if accepted == 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("dispatch scheduler did not become idle");
    }

    async fn wait_for_cancellation_state(
        server: &A2aHttpJsonRpcServer,
        task_id: &str,
        expected: A2aCancellationOutboxState,
    ) {
        let principal = owner_principal();
        timeout(Duration::from_secs(2), async {
            loop {
                if server
                    .mapper_snapshot()
                    .await
                    .cancellation_for_task(task_id, &principal)
                    .is_some_and(|record| record.state == expected)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("cancellation did not reach the expected state");
    }

    async fn wait_for_cancel_completion_subscribers(
        server: &A2aHttpJsonRpcServer,
        expected: usize,
    ) {
        timeout(Duration::from_secs(1), async {
            loop {
                let subscribers = server
                    .dispatch_state
                    .lock()
                    .unwrap()
                    .inflight_messages
                    .values()
                    .find(|inflight| inflight.completion.receiver_count() >= expected)
                    .map(|inflight| inflight.completion.receiver_count())
                    .unwrap_or_default();
                if subscribers >= expected {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("duplicate cancellation did not join the single flight");
    }

    #[tokio::test]
    async fn malicious_event_store_append_bindings_fail_closed_before_dispatch() {
        for (index, corruption) in [
            AppendCorruption::Tenant,
            AppendCorruption::Task,
            AppendCorruption::Context,
            AppendCorruption::LogicalId,
        ]
        .into_iter()
        .enumerate()
        {
            let host = Arc::new(CompletingHost::default());
            let (address, _server, handle, task) = start_server_with_dependencies(
                A2aHttpConfig::default(),
                Arc::new(Mutex::new(A2aMapper::new())),
                Arc::new(InMemoryA2aMapperSnapshotStore::default()),
                Arc::new(MaliciousAppendStore { corruption }),
                host.clone(),
                agent_card(),
            )
            .await;
            let response = raw_http(
                address,
                request(
                    "POST",
                    "/a2a",
                    &rpc_headers("application/json"),
                    &send_body(&format!("malicious-append-{index}"), true, None),
                ),
            )
            .await;
            assert_eq!(body(&response)["error"]["code"], -32006);
            assert_eq!(host.calls.load(Ordering::SeqCst), 0);
            handle.cancel();
            task.await.unwrap().unwrap();
        }
    }

    #[tokio::test]
    async fn malicious_replay_nonmonotonic_and_gap_pages_fail_closed() {
        let owner = A2aEventOwner {
            subject: "owner".into(),
            tenant_id: Some("tenant-a".into()),
        };
        for attack in [ReplayAttack::NonMonotonic, ReplayAttack::Gap] {
            let server = Arc::new(
                A2aHttpJsonRpcServer::new(
                    Arc::new(Mutex::new(A2aMapper::new())),
                    Arc::new(InMemoryA2aMapperSnapshotStore::default()),
                    Arc::new(MaliciousReplayStore { attack }),
                    Arc::new(TestAuthenticator),
                    Arc::new(CompletingHost::default()),
                    agent_card(),
                    A2aHttpConfig::default(),
                )
                .unwrap(),
            );
            let error = server
                .replay_to_high_water(&owner, "task-replay", "context-replay", Some(0), 8, 8192)
                .await
                .unwrap_err();
            assert_eq!(error.code, ProtocolErrorCode::Conflict);
        }
    }

    #[tokio::test]
    async fn failed_first_event_append_exact_retry_dispatches_and_terminalizes_once() {
        let events = Arc::new(FailFirstAppendStore::default());
        let host = Arc::new(CompletingHost::default());
        let snapshots = Arc::new(InMemoryA2aMapperSnapshotStore::default());
        let (address, server, handle, task) = start_server_with_dependencies(
            A2aHttpConfig::default(),
            Arc::new(Mutex::new(A2aMapper::new())),
            snapshots.clone(),
            events.clone(),
            host.clone(),
            agent_card(),
        )
        .await;
        let payload = send_body("event-fault-retry", true, None);
        let failed = raw_http(
            address,
            request("POST", "/a2a", &rpc_headers("application/json"), &payload),
        )
        .await;
        let failed_body = body(&failed);
        assert_eq!(failed_body["error"]["code"], -32006);
        let task_id = failed_body["error"]["data"][0]["metadata"]["taskId"]
            .as_str()
            .unwrap()
            .to_owned();
        assert_eq!(host.calls.load(Ordering::SeqCst), 0);

        let retried = raw_http(
            address,
            request("POST", "/a2a", &rpc_headers("application/json"), &payload),
        )
        .await;
        assert_eq!(body(&retried)["error"]["code"], -32006);

        wait_for_task_state(&server, &task_id, A2aTaskState::Completed).await;
        let duplicate = raw_http(
            address,
            request("POST", "/a2a", &rpc_headers("application/json"), &payload),
        )
        .await;
        assert_eq!(
            body(&duplicate)["result"]["task"]["status"]["state"],
            "TASK_STATE_COMPLETED"
        );
        wait_for_scheduler_idle(&server).await;
        assert_eq!(host.calls.load(Ordering::SeqCst), 1);
        assert_eq!(events.inserted_terminal.load(Ordering::SeqCst), 1);
        let persisted = snapshots.load_snapshot().await.unwrap();
        assert!(persisted.pending_dispatches().is_empty());
        assert!(persisted.pending_events().is_empty());

        handle.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn committed_acceptance_ack_loss_recovers_existing_and_dispatches_once() {
        let message_id = "event-commit-ack-loss";
        let events = Arc::new(CommitThenAckLossEventStore::default());
        let host = Arc::new(CompletingHost::default());
        let snapshots = Arc::new(InMemoryA2aMapperSnapshotStore::default());
        let (address, server, handle, task) = start_server_with_dependencies(
            A2aHttpConfig::default(),
            Arc::new(Mutex::new(A2aMapper::new())),
            snapshots.clone(),
            events.clone(),
            host.clone(),
            agent_card(),
        )
        .await;
        let payload = send_body(message_id, true, None);
        let failed = raw_http(
            address,
            request("POST", "/a2a", &rpc_headers("application/json"), &payload),
        )
        .await;
        let failed_body = body(&failed);
        assert_eq!(failed_body["error"]["code"], -32006);
        let task_id = failed_body["error"]["data"][0]["metadata"]["taskId"]
            .as_str()
            .unwrap()
            .to_owned();
        assert_eq!(host.calls.load(Ordering::SeqCst), 0);
        assert_eq!(events.acceptance_inserted.load(Ordering::SeqCst), 1);

        let immediate_retry = raw_http(
            address,
            request("POST", "/a2a", &rpc_headers("application/json"), &payload),
        )
        .await;
        assert_eq!(body(&immediate_retry)["error"]["code"], -32006);

        wait_for_task_state(&server, &task_id, A2aTaskState::Completed).await;
        let duplicate = raw_http(
            address,
            request("POST", "/a2a", &rpc_headers("application/json"), &payload),
        )
        .await;
        assert_eq!(
            body(&duplicate)["result"]["task"]["status"]["state"],
            "TASK_STATE_COMPLETED"
        );
        wait_for_scheduler_idle(&server).await;

        assert_eq!(host.calls.load(Ordering::SeqCst), 1);
        assert_eq!(events.acceptance_inserted.load(Ordering::SeqCst), 1);
        assert_eq!(events.acceptance_existing.load(Ordering::SeqCst), 1);
        assert_eq!(events.terminal_inserted.load(Ordering::SeqCst), 1);
        let acceptance_logical_id = events
            .acceptance_logical_id
            .lock()
            .unwrap()
            .clone()
            .unwrap();
        let persisted = snapshots.load_snapshot().await.unwrap();
        let acceptance = &persisted.pending_event_intents()[&acceptance_logical_id];
        assert_eq!(acceptance.message_id.as_deref(), Some(message_id));
        assert_eq!(acceptance.task_id, task_id);
        assert_eq!(acceptance.state, A2aPendingEventState::Settled);
        let dispatch = persisted
            .dispatch_for_message(message_id, &owner_principal())
            .unwrap();
        assert!(persisted.dispatch_event_ready(dispatch));
        assert!(persisted.pending_dispatches().is_empty());
        assert!(persisted.pending_events().is_empty());

        handle.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn permanent_append_error_quarantines_once_without_recalling_backend() {
        let principal = owner_principal();
        let (seeded, _) = seeded_task("permanent-append-error", A2aTaskState::Working);
        let event_id = event_id_for_message(&seeded, "permanent-append-error", &principal);
        let snapshots = Arc::new(InMemoryA2aMapperSnapshotStore::default());
        snapshots.persist_snapshot(&seeded).await.unwrap();
        let events = Arc::new(PermanentFailAppendStore::default());
        let server = Arc::new(
            A2aHttpJsonRpcServer::new(
                Arc::new(Mutex::new(seeded)),
                snapshots,
                events.clone(),
                Arc::new(TestAuthenticator),
                Arc::new(CompletingHost::default()),
                agent_card(),
                A2aHttpConfig::default(),
            )
            .unwrap(),
        );

        let error = server.deliver_pending_event(&event_id).await.unwrap_err();
        assert_eq!(error.code, ProtocolErrorCode::Conflict);
        let quarantined = server.mapper_snapshot().await;
        assert_eq!(
            quarantined.pending_event_intents()[&event_id].state,
            A2aPendingEventState::Quarantined
        );
        assert_eq!(
            quarantined.pending_event_intents()[&event_id].quarantine_reason,
            Some(A2aEventQuarantineReason::DeterministicPoison)
        );

        assert!(server.deliver_pending_event(&event_id).await.is_err());
        assert_eq!(events.attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn quarantined_acceptance_exact_duplicate_never_schedules_or_mutates_mapper() {
        let principal = owner_principal();
        let message_id = "quarantined-exact-duplicate";
        let mut seeded = A2aMapper::new();
        let exact_message = A2aMessage {
            message_id: message_id.into(),
            context_id: None,
            task_id: None,
            role: A2aRole::User,
            parts: vec![A2aPart::Text {
                text: "hello".into(),
            }],
            metadata: BTreeMap::from([("complete".into(), Value::Bool(true))]),
        };
        seeded
            .prepare_send_message(
                exact_message,
                CorrelationIdentity::new("quarantine-correlation", "quarantine-request").unwrap(),
                Some(&principal),
            )
            .into_authorized()
            .unwrap();
        let event_id = event_id_for_message(&seeded, message_id, &principal);
        seeded
            .mark_event_quarantined(&event_id, A2aEventQuarantineReason::DeterministicPoison)
            .unwrap();
        let before = seeded.clone();
        let snapshots = Arc::new(InMemoryA2aMapperSnapshotStore::default());
        snapshots.persist_snapshot(&seeded).await.unwrap();
        let host = Arc::new(CompletingHost::default());
        let (address, server, handle, task) = start_server_with_dependencies(
            A2aHttpConfig::default(),
            Arc::new(Mutex::new(seeded)),
            snapshots,
            Arc::new(InMemoryA2aEventStore::default()),
            host.clone(),
            agent_card(),
        )
        .await;

        let response = raw_http(
            address,
            request(
                "POST",
                "/a2a",
                &rpc_headers("application/json"),
                &send_body(message_id, true, None),
            ),
        )
        .await;
        assert_eq!(body(&response)["error"]["code"], -32006);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(host.calls.load(Ordering::SeqCst), 0);
        assert_eq!(server.mapper_snapshot().await, before);
        {
            let scheduler = server.dispatch_state.lock().unwrap();
            assert_eq!(scheduler.accepted, 0);
            assert!(scheduler.inflight_messages.is_empty());
        }

        handle.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn existing_event_recovery_republishes_to_already_open_live_subscriber() {
        let (mut seeded, mapping) =
            seeded_task("existing-event-live-republish", A2aTaskState::Working);
        let initial_event_ids: Vec<String> = seeded
            .pending_events()
            .into_iter()
            .map(|intent| intent.event_id)
            .collect();
        assert_eq!(initial_event_ids.len(), 1);
        for event_id in initial_event_ids {
            seeded.mark_event_settled(&event_id).unwrap();
        }

        let snapshots = Arc::new(AmbiguousTwiceEventSettlementSnapshotStore::default());
        snapshots.persist_snapshot(&seeded).await.unwrap();
        let restored = snapshots.load_snapshot().await.unwrap();
        let events = Arc::new(ObservingEventStore::default());
        let server = Arc::new(
            A2aHttpJsonRpcServer::new(
                Arc::new(Mutex::new(restored)),
                snapshots.clone(),
                events.clone(),
                Arc::new(TestAuthenticator),
                Arc::new(CompletingHost::default()),
                agent_card(),
                A2aHttpConfig::default(),
            )
            .unwrap(),
        );
        let mut subscriber = server.live.subscribe();

        let error = server
            .transition_task(&mapping.task_id, A2aTaskState::Completed, None)
            .await
            .unwrap_err();
        assert_eq!(error.code, ProtocolErrorCode::Conflict);
        assert_eq!(snapshots.failures.load(Ordering::SeqCst), 2);
        assert_eq!(events.inserted.load(Ordering::SeqCst), 1);
        assert!(!server.snapshot_commit_failed.load(Ordering::SeqCst));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let cancellation = CancellationToken::new();
        let handle = cancellation.handle();
        let task = tokio::spawn(server.clone().serve(listener, cancellation));

        let recovered = timeout(Duration::from_secs(1), async {
            loop {
                let LiveEvent(event) = subscriber.recv().await.unwrap();
                if event.task_id == mapping.task_id && event.response.is_terminal() {
                    break event;
                }
            }
        })
        .await
        .expect("recovered Existing event was not republished to the open subscriber");
        assert_eq!(recovered.task_id, mapping.task_id);
        assert_eq!(events.inserted.load(Ordering::SeqCst), 1);
        assert_eq!(events.existing.load(Ordering::SeqCst), 1);
        let persisted = snapshots.load_snapshot().await.unwrap();
        assert!(persisted.pending_events().is_empty());

        handle.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn host_exact_dispatch_transition_accepts_input_auth_and_failed_states() {
        for (suffix, next) in [
            ("input", A2aTaskState::InputRequired),
            ("auth", A2aTaskState::AuthRequired),
            ("failed", A2aTaskState::Failed),
        ] {
            let status_message = format!("exact {suffix} transition");
            let host = Arc::new(ExactStateTransitionHost::new(next, &status_message));
            let snapshots = Arc::new(InMemoryA2aMapperSnapshotStore::default());
            let (address, server, handle, task) = start_server_with_dependencies(
                A2aHttpConfig::default(),
                Arc::new(Mutex::new(A2aMapper::new())),
                snapshots.clone(),
                Arc::new(InMemoryA2aEventStore::default()),
                host.clone(),
                agent_card(),
            )
            .await;
            let message_id = format!("exact-state-{suffix}");
            let response = raw_http(
                address,
                request(
                    "POST",
                    "/a2a",
                    &rpc_headers("application/json"),
                    &send_body(&message_id, false, None),
                ),
            )
            .await;
            let response_body = body(&response);
            assert!(response_body.get("error").is_none(), "{response_body}");
            let task_id = response_body["result"]["task"]["id"]
                .as_str()
                .unwrap()
                .to_owned();
            wait_for_scheduler_idle(&server).await;

            let persisted = snapshots.load_snapshot().await.unwrap();
            assert_eq!(host.calls.load(Ordering::SeqCst), 1);
            assert_eq!(persisted.tasks()[&task_id].state, next);
            assert_eq!(
                persisted.tasks()[&task_id].status_message.as_deref(),
                Some(status_message.as_str())
            );
            let dispatch = persisted
                .dispatch_for_message(&message_id, &owner_principal())
                .unwrap();
            assert_eq!(dispatch.state, A2aDispatchOutboxState::Settled);
            assert_eq!(dispatch.attempts, 1);
            assert_eq!(
                dispatch.updated_revision,
                persisted.tasks()[&task_id].updated_revision
            );

            handle.cancel();
            task.await.unwrap().unwrap();
        }
    }

    #[tokio::test]
    async fn exact_dispatch_transition_rejects_stale_attempt_without_mutation() {
        for (suffix, next) in [
            ("input", A2aTaskState::InputRequired),
            ("auth", A2aTaskState::AuthRequired),
            ("failed", A2aTaskState::Failed),
        ] {
            let principal = owner_principal();
            let message_id = format!("stale-exact-state-{suffix}");
            let mut seeded = A2aMapper::new();
            let mapping = prepare_seeded_message_for(&mut seeded, &message_id, &principal);
            let event_id = event_id_for_message(&seeded, &message_id, &principal);
            seeded.mark_event_settled(&event_id).unwrap();
            let dispatch_id = seeded
                .dispatch_for_message(&message_id, &principal)
                .unwrap()
                .dispatch_id
                .clone();
            seeded.mark_dispatch_running(&dispatch_id).unwrap();
            let stale_attempt = seeded.dispatch_outbox()[&dispatch_id].attempts;
            let stale_context = A2aDispatchContext {
                mode: A2aExecutionMode::Immediate,
                cancellation: CancellationToken::new(),
                dispatch_fence: Some(A2aDispatchFence {
                    dispatch_id: dispatch_id.clone(),
                    expected_attempt: stale_attempt,
                }),
            };
            seeded
                .mark_dispatch_reconcile_pending(&dispatch_id, "seed replacement attempt")
                .unwrap();
            seeded.mark_dispatch_running(&dispatch_id).unwrap();
            let current_attempt = seeded.dispatch_outbox()[&dispatch_id].attempts;
            assert_eq!(current_attempt, stale_attempt + 1);

            let snapshots = Arc::new(InMemoryA2aMapperSnapshotStore::default());
            snapshots.persist_snapshot(&seeded).await.unwrap();
            let server = Arc::new(
                A2aHttpJsonRpcServer::new(
                    Arc::new(Mutex::new(seeded)),
                    snapshots.clone(),
                    Arc::new(InMemoryA2aEventStore::default()),
                    Arc::new(TestAuthenticator),
                    Arc::new(CompletingHost::default()),
                    agent_card(),
                    A2aHttpConfig::default(),
                )
                .unwrap(),
            );
            let before = server.mapper_snapshot().await;
            let error = server
                .transition_dispatch_task(
                    &stale_context,
                    next,
                    Some(format!("stale {suffix} transition")),
                )
                .await
                .unwrap_err();
            assert_eq!(error.code, ProtocolErrorCode::InvalidTransition);
            assert!(error.message.contains("stale"));
            assert_eq!(server.mapper_snapshot().await, before);
            assert_eq!(snapshots.load_snapshot().await.unwrap(), before);

            let current_context = A2aDispatchContext {
                mode: A2aExecutionMode::Immediate,
                cancellation: CancellationToken::new(),
                dispatch_fence: Some(A2aDispatchFence {
                    dispatch_id: dispatch_id.clone(),
                    expected_attempt: current_attempt,
                }),
            };
            let progress = server
                .transition_dispatch_task(
                    &current_context,
                    A2aTaskState::Working,
                    Some(format!("current {suffix} progress")),
                )
                .await
                .unwrap();
            assert_eq!(progress.state, A2aTaskState::Working);
            assert_eq!(
                server.mapper_snapshot().await.dispatch_outbox()[&dispatch_id].state,
                A2aDispatchOutboxState::Running
            );
            let status_message = format!("current {suffix} transition");
            let task = server
                .transition_dispatch_task(&current_context, next, Some(status_message.clone()))
                .await
                .unwrap();
            assert_eq!(task.mapping.task_id, mapping.task_id);
            assert_eq!(task.state, next);
            assert_eq!(
                task.status_message.as_deref(),
                Some(status_message.as_str())
            );
            let after = server.mapper_snapshot().await;
            assert_eq!(
                after.dispatch_outbox()[&dispatch_id].state,
                A2aDispatchOutboxState::Settled
            );
            assert_eq!(
                after.dispatch_outbox()[&dispatch_id].attempts,
                current_attempt
            );
            assert_eq!(
                after.dispatch_outbox()[&dispatch_id].updated_revision,
                after.tasks()[&mapping.task_id].updated_revision
            );
        }
    }

    #[tokio::test]
    async fn restored_dispatch_waits_for_exact_event_settlement_then_runs_once() {
        let principal = owner_principal();
        let message_id = "restored-dispatch-event-gate";
        let mut seeded = A2aMapper::new();
        let (_, action) = seeded
            .prepare_send_message(
                seeded_message(message_id),
                CorrelationIdentity::new(
                    "restored-dispatch-event-gate-correlation",
                    "restored-dispatch-event-gate-request",
                )
                .unwrap(),
                Some(&principal),
            )
            .into_authorized()
            .unwrap();
        let A2aAction::DispatchMessage { mapping, .. } = action else {
            panic!("seeded recovery message did not create a dispatch");
        };
        let dispatch = seeded
            .dispatch_for_message(message_id, &principal)
            .cloned()
            .unwrap();
        let matching_events: Vec<_> = seeded
            .pending_event_intents()
            .values()
            .filter(|intent| {
                intent.owner_subject == dispatch.owner_subject
                    && intent.owner_tenant_id == dispatch.owner_tenant_id
                    && intent.task_id == dispatch.task_id
                    && intent.context_id == dispatch.context_id
                    && intent.session_id == dispatch.session_id
                    && intent.run_id == dispatch.run_id
                    && intent.message_id.as_deref() == Some(dispatch.message_id.as_str())
                    && intent.source_revision == dispatch.created_revision
            })
            .map(|intent| intent.event_id.clone())
            .collect();
        assert_eq!(matching_events.len(), 1);
        let event_id = matching_events[0].clone();

        let snapshots = Arc::new(InMemoryA2aMapperSnapshotStore::default());
        snapshots.persist_snapshot(&seeded).await.unwrap();
        let restored = snapshots.load_snapshot().await.unwrap();
        let events = Arc::new(GatedAppendStore::default());
        let host = Arc::new(CompletingHost::default());
        let (_address, server, handle, task) = start_server_with_dependencies(
            A2aHttpConfig::default(),
            Arc::new(Mutex::new(restored)),
            snapshots,
            events.clone(),
            host.clone(),
            agent_card(),
        )
        .await;

        timeout(Duration::from_secs(1), async {
            loop {
                let snapshot = server.mapper_snapshot().await;
                if snapshot.pending_event_intents()[&event_id].state
                    == A2aPendingEventState::ReconcilePending
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("recovery did not record the gated event failure");
        assert!(events.attempts.load(Ordering::SeqCst) >= 1);
        assert_eq!(host.calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            server.mapper_snapshot().await.dispatch_outbox()[&dispatch.dispatch_id].state,
            A2aDispatchOutboxState::Queued
        );

        events.open.store(true, Ordering::SeqCst);
        wait_for_task_state(&server, &mapping.task_id, A2aTaskState::Completed).await;
        wait_for_scheduler_idle(&server).await;
        let repaired = server.mapper_snapshot().await;
        assert_eq!(host.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            repaired.pending_event_intents()[&event_id].state,
            A2aPendingEventState::Settled
        );
        assert_eq!(
            repaired.dispatch_outbox()[&dispatch.dispatch_id].state,
            A2aDispatchOutboxState::Settled
        );

        handle.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn poisoned_event_does_not_block_other_owner_event_or_cancellation_recovery() {
        let bad_principal = owner_principal();
        let healthy_principal = other_principal();
        let mut seeded = A2aMapper::new();

        let bad_message_id = "poison-owner-event";
        let bad_mapping = prepare_seeded_message_for(&mut seeded, bad_message_id, &bad_principal);
        let bad_dispatch_id = seeded
            .dispatch_for_message(bad_message_id, &bad_principal)
            .unwrap()
            .dispatch_id
            .clone();
        seeded.mark_dispatch_settled(&bad_dispatch_id).unwrap();
        let bad_event_id = event_id_for_message(&seeded, bad_message_id, &bad_principal);

        let healthy_message_id = "healthy-owner-event";
        let _healthy_mapping =
            prepare_seeded_message_for(&mut seeded, healthy_message_id, &healthy_principal);
        let healthy_dispatch_id = seeded
            .dispatch_for_message(healthy_message_id, &healthy_principal)
            .unwrap()
            .dispatch_id
            .clone();
        seeded.mark_dispatch_settled(&healthy_dispatch_id).unwrap();
        let healthy_event_id =
            event_id_for_message(&seeded, healthy_message_id, &healthy_principal);

        let cancel_message_id = "healthy-owner-cancel";
        let cancel_mapping =
            prepare_seeded_message_for(&mut seeded, cancel_message_id, &healthy_principal);
        let cancel_dispatch_id = seeded
            .dispatch_for_message(cancel_message_id, &healthy_principal)
            .unwrap()
            .dispatch_id
            .clone();
        seeded.mark_dispatch_settled(&cancel_dispatch_id).unwrap();
        let cancel_initial_event =
            event_id_for_message(&seeded, cancel_message_id, &healthy_principal);
        seeded.mark_event_settled(&cancel_initial_event).unwrap();
        seeded
            .prepare_cancel_task(
                &cancel_mapping.task_id,
                CorrelationIdentity::new(
                    "healthy-owner-cancel-correlation",
                    "healthy-owner-cancel-request",
                )
                .unwrap(),
                Some(&healthy_principal),
            )
            .into_authorized()
            .unwrap();

        let snapshots = Arc::new(InMemoryA2aMapperSnapshotStore::default());
        snapshots.persist_snapshot(&seeded).await.unwrap();
        let restored = snapshots.load_snapshot().await.unwrap();
        let events = Arc::new(SelectivePoisonAppendStore {
            inner: InMemoryA2aEventStore::default(),
            poisoned_task_id: bad_mapping.task_id.clone(),
            poisoned: AtomicUsize::new(0),
            healthy: AtomicUsize::new(0),
        });
        let host = Arc::new(CancelHost::new(CancelHostBehavior::Stopped));
        let (_address, server, handle, task) = start_server_with_dependencies(
            A2aHttpConfig::default(),
            Arc::new(Mutex::new(restored)),
            snapshots,
            events.clone(),
            host.clone(),
            agent_card(),
        )
        .await;

        timeout(Duration::from_secs(2), async {
            loop {
                let snapshot = server.mapper_snapshot().await;
                let bad_quarantined = snapshot.pending_event_intents()[&bad_event_id].state
                    == A2aPendingEventState::Quarantined;
                let healthy_settled = snapshot.pending_event_intents()[&healthy_event_id].state
                    == A2aPendingEventState::Settled;
                let cancel_settled = snapshot
                    .cancellation_for_task(&cancel_mapping.task_id, &healthy_principal)
                    .is_some_and(|record| record.state == A2aCancellationOutboxState::Settled);
                if bad_quarantined && healthy_settled && cancel_settled {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("poisoned event blocked independent recovery work");
        let recovered = server.mapper_snapshot().await;
        assert_eq!(
            recovered.pending_event_intents()[&bad_event_id].quarantine_reason,
            Some(A2aEventQuarantineReason::DeterministicPoison)
        );
        assert_eq!(
            recovered.tasks()[&cancel_mapping.task_id].state,
            A2aTaskState::Cancelled
        );
        assert_eq!(events.poisoned.load(Ordering::SeqCst), 1);
        assert!(events.healthy.load(Ordering::SeqCst) >= 2);
        assert_eq!(host.cancel_calls.load(Ordering::SeqCst), 1);

        handle.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn startup_recovers_queued_and_reconcile_pending_dispatches_once() {
        for (suffix, reconcile_pending) in [("queued", false), ("reconcile", true)] {
            let principal = owner_principal();
            let message_id = format!("startup-{suffix}");
            let mut seeded = A2aMapper::new();
            seeded
                .prepare_send_message(
                    seeded_message(&message_id),
                    CorrelationIdentity::new(
                        format!("correlation-{suffix}"),
                        format!("request-{suffix}"),
                    )
                    .unwrap(),
                    Some(&principal),
                )
                .into_authorized()
                .unwrap();
            let dispatch_id = seeded
                .dispatch_for_message(&message_id, &principal)
                .unwrap()
                .dispatch_id
                .clone();
            let task_id = seeded
                .dispatch_for_message(&message_id, &principal)
                .unwrap()
                .task_id
                .clone();
            if reconcile_pending {
                seeded
                    .mark_dispatch_reconcile_pending(&dispatch_id, "seeded crash")
                    .unwrap();
            }
            let snapshots = Arc::new(InMemoryA2aMapperSnapshotStore::default());
            snapshots.persist_snapshot(&seeded).await.unwrap();
            let restored = snapshots.load_snapshot().await.unwrap();
            let host = Arc::new(CompletingHost {
                safe_retry_unknown: reconcile_pending,
                ..CompletingHost::default()
            });
            let (_address, server, handle, task) = start_server_with_dependencies(
                A2aHttpConfig::default(),
                Arc::new(Mutex::new(restored)),
                snapshots.clone(),
                Arc::new(InMemoryA2aEventStore::default()),
                host.clone(),
                agent_card(),
            )
            .await;
            wait_for_task_state(&server, &task_id, A2aTaskState::Completed).await;
            wait_for_scheduler_idle(&server).await;
            assert_eq!(host.calls.load(Ordering::SeqCst), 1);
            let persisted = snapshots.load_snapshot().await.unwrap();
            assert_eq!(
                persisted.dispatch_outbox()[&dispatch_id].state,
                A2aDispatchOutboxState::Settled
            );
            assert!(persisted.pending_dispatches().is_empty());
            assert!(persisted.pending_events().is_empty());
            handle.cancel();
            task.await.unwrap().unwrap();
        }
    }

    #[tokio::test]
    async fn cancel_requires_stopped_ack_and_persists_nonterminal_intent_on_error_or_timeout() {
        for (suffix, behavior, expect_cancelled) in [
            ("stopped", CancelHostBehavior::Stopped, true),
            ("error", CancelHostBehavior::Error, false),
            ("timeout", CancelHostBehavior::Timeout, false),
        ] {
            let config = if matches!(behavior, CancelHostBehavior::Timeout) {
                A2aHttpConfig {
                    blocking_dispatch_timeout: Duration::from_millis(30),
                    ..A2aHttpConfig::default()
                }
            } else {
                A2aHttpConfig::default()
            };
            let host = Arc::new(CancelHost::new(behavior));
            let snapshots = Arc::new(InMemoryA2aMapperSnapshotStore::default());
            let (address, server, handle, task) = start_server_with_dependencies(
                config,
                Arc::new(Mutex::new(A2aMapper::new())),
                snapshots.clone(),
                Arc::new(InMemoryA2aEventStore::default()),
                host.clone(),
                agent_card(),
            )
            .await;
            let sent = raw_http(
                address,
                request(
                    "POST",
                    "/a2a",
                    &rpc_headers("application/json"),
                    &send_body(&format!("cancel-{suffix}"), false, Some(true)),
                ),
            )
            .await;
            let task_id = body(&sent)["result"]["task"]["id"]
                .as_str()
                .unwrap()
                .to_owned();
            wait_for_scheduler_idle(&server).await;
            let cancelled = raw_http(
                address,
                request(
                    "POST",
                    "/a2a",
                    &rpc_headers("application/json"),
                    &cancel_body(&task_id, &format!("cancel-request-{suffix}")),
                ),
            )
            .await;
            let persisted = snapshots.load_snapshot().await.unwrap();
            let persisted_task = &persisted.tasks()[&task_id];
            assert_eq!(host.cancel_calls.load(Ordering::SeqCst), 1);
            if expect_cancelled {
                assert_eq!(
                    body(&cancelled)["result"]["status"]["state"],
                    "TASK_STATE_CANCELED"
                );
                assert_eq!(persisted_task.state, A2aTaskState::Cancelled);
            } else {
                assert_eq!(body(&cancelled)["error"]["code"], -32006);
                assert_eq!(persisted_task.state, A2aTaskState::Working);
                assert_eq!(
                    persisted_task.status_message.as_deref(),
                    Some("cancellation requested")
                );
            }
            handle.cancel();
            task.await.unwrap().unwrap();
        }
    }

    #[tokio::test]
    async fn durable_cancel_blocks_input_and_auth_resume_without_new_receipt_or_dispatch() {
        for (suffix, task_state) in [
            ("input", A2aTaskState::InputRequired),
            ("auth", A2aTaskState::AuthRequired),
        ] {
            let (mapper, mapping, cancellation) =
                seeded_cancellation(&format!("cancel-resume-{suffix}"), task_state, true);
            let changed_status = format!("status text changed after {suffix} cancellation");
            let mut serialized = serde_json::to_value(&mapper).unwrap();
            let changed_revision = serialized["revision"].as_u64().unwrap() + 1;
            serialized["tasks"][mapping.task_id.as_str()]["status_message"] =
                Value::String(changed_status.clone());
            serialized["tasks"][mapping.task_id.as_str()]["updated_revision"] =
                Value::from(changed_revision);
            serialized["cancellation_outbox"][cancellation.cancellation_id.as_str()]
                ["updated_revision"] = Value::from(changed_revision);
            serialized["revision"] = Value::from(changed_revision);
            let restored: A2aMapper = serde_json::from_value(serialized).unwrap();
            assert_eq!(
                restored.tasks()[&mapping.task_id].status_message.as_deref(),
                Some(changed_status.as_str())
            );

            let snapshots = Arc::new(InMemoryA2aMapperSnapshotStore::default());
            snapshots.persist_snapshot(&restored).await.unwrap();
            let host = Arc::new(CancelRegressionHost::new(false, false));
            let (address, server, handle, task) = start_server_with_dependencies(
                A2aHttpConfig::default(),
                Arc::new(Mutex::new(restored)),
                snapshots,
                Arc::new(InMemoryA2aEventStore::default()),
                host.clone(),
                agent_card(),
            )
            .await;
            wait_for_cancellation_state(
                &server,
                &mapping.task_id,
                A2aCancellationOutboxState::ReconcilePending,
            )
            .await;

            let before = server.mapper_snapshot().await;
            let resumed = raw_http(
                address,
                request(
                    "POST",
                    "/a2a",
                    &rpc_headers("application/json"),
                    &resume_body(
                        &mapping.task_id,
                        &mapping.context_id,
                        &format!("resume-after-{suffix}-cancel"),
                    ),
                ),
            )
            .await;
            assert_eq!(body(&resumed)["error"]["code"], -32006);
            let after = server.mapper_snapshot().await;
            assert_eq!(after.receipts().len(), before.receipts().len());
            assert_eq!(
                after.dispatch_outbox().len(),
                before.dispatch_outbox().len()
            );
            assert!(after
                .message_receipt(&format!("resume-after-{suffix}-cancel"), &owner_principal())
                .is_none());
            assert_eq!(after.tasks()[&mapping.task_id].state, task_state);
            assert_eq!(
                after.tasks()[&mapping.task_id].status_message.as_deref(),
                Some(changed_status.as_str())
            );
            assert_eq!(host.message_calls.load(Ordering::SeqCst), 0);
            assert_eq!(host.cancel_calls.load(Ordering::SeqCst), 0);
            assert_eq!(host.reconcile_cancel_calls.load(Ordering::SeqCst), 1);

            handle.cancel();
            task.await.unwrap().unwrap();
        }
    }

    #[tokio::test]
    async fn live_cancel_retry_requires_safe_reconcile_and_reuses_canonical_control() {
        for (suffix, safe_retry) in [("default", false), ("safe", true)] {
            let (seeded, mapping) = seeded_task(
                &format!("live-cancel-retry-{suffix}"),
                A2aTaskState::Working,
            );
            let snapshots = Arc::new(InMemoryA2aMapperSnapshotStore::default());
            snapshots.persist_snapshot(&seeded).await.unwrap();
            let host = Arc::new(CancelRegressionHost::new(safe_retry, true));
            let (address, server, handle, task) = start_server_with_dependencies(
                A2aHttpConfig::default(),
                Arc::new(Mutex::new(seeded)),
                snapshots,
                Arc::new(InMemoryA2aEventStore::default()),
                host.clone(),
                agent_card(),
            )
            .await;

            let first = raw_http(
                address,
                request(
                    "POST",
                    "/a2a",
                    &rpc_headers("application/json"),
                    &cancel_body(&mapping.task_id, &format!("cancel-{suffix}-id1")),
                ),
            )
            .await;
            assert_eq!(body(&first)["error"]["code"], -32006);
            if safe_retry {
                wait_for_task_state(&server, &mapping.task_id, A2aTaskState::Cancelled).await;
                wait_for_scheduler_idle(&server).await;
            }
            let after_first = server.mapper_snapshot().await;
            let canonical = after_first
                .cancellation_for_task(&mapping.task_id, &owner_principal())
                .cloned()
                .unwrap();
            if safe_retry {
                assert_eq!(canonical.state, A2aCancellationOutboxState::Settled);
                assert_eq!(canonical.attempts, 2);
            } else {
                assert_eq!(
                    canonical.state,
                    A2aCancellationOutboxState::ReconcilePending
                );
                assert_eq!(canonical.attempts, 1);
            }

            let second = raw_http(
                address,
                request(
                    "POST",
                    "/a2a",
                    &rpc_headers("application/json"),
                    &cancel_body(&mapping.task_id, &format!("cancel-{suffix}-id2")),
                ),
            )
            .await;
            let final_snapshot = server.mapper_snapshot().await;
            let final_record = final_snapshot
                .cancellation_for_task(&mapping.task_id, &owner_principal())
                .unwrap();
            assert_eq!(host.reconcile_cancel_calls.load(Ordering::SeqCst), 1);
            if safe_retry {
                // Automatic same-process recovery already settled the task. A later CancelTask
                // is a terminal-state conflict and must not replay either the probe or effect.
                assert_eq!(body(&second)["error"]["code"], -32002);
                assert_eq!(host.cancel_calls.load(Ordering::SeqCst), 2);
                assert_eq!(
                    final_snapshot.tasks()[&mapping.task_id].state,
                    A2aTaskState::Cancelled
                );
                assert_eq!(final_record.state, A2aCancellationOutboxState::Settled);
                assert_eq!(final_record.attempts, 2);
            } else {
                assert_eq!(body(&second)["error"]["code"], -32006);
                assert_eq!(host.cancel_calls.load(Ordering::SeqCst), 1);
                assert_eq!(
                    final_snapshot.tasks()[&mapping.task_id].state,
                    A2aTaskState::Working
                );
                assert_eq!(
                    final_record.state,
                    A2aCancellationOutboxState::ReconcilePending
                );
                assert_eq!(final_record.attempts, 1);
            }
            {
                let observed = host.observed_controls.lock().unwrap();
                assert_eq!(observed.len(), if safe_retry { 2 } else { 1 });
                for (envelope, action) in observed.iter() {
                    assert_eq!(envelope, &canonical.envelope);
                    let A2aAction::CancelTask { task } = action else {
                        panic!("cancellation host observed a non-canonical action");
                    };
                    assert_eq!(task, &canonical.task);
                }
            }

            handle.cancel();
            task.await.unwrap().unwrap();
        }
    }

    #[tokio::test]
    async fn restored_running_cancel_fails_closed_unless_host_attests_safe_retry() {
        for (suffix, safe_retry) in [("default", false), ("safe", true)] {
            let (seeded, mapping, cancellation) = seeded_cancellation(
                &format!("restored-running-cancel-{suffix}"),
                A2aTaskState::Working,
                true,
            );
            assert_eq!(cancellation.state, A2aCancellationOutboxState::Running);
            assert_eq!(cancellation.attempts, 1);
            let snapshots = Arc::new(InMemoryA2aMapperSnapshotStore::default());
            snapshots.persist_snapshot(&seeded).await.unwrap();
            let restored = snapshots.load_snapshot().await.unwrap();
            let host = Arc::new(CancelRegressionHost::new(safe_retry, false));
            let (_address, server, handle, task) = start_server_with_dependencies(
                A2aHttpConfig::default(),
                Arc::new(Mutex::new(restored)),
                snapshots,
                Arc::new(InMemoryA2aEventStore::default()),
                host.clone(),
                agent_card(),
            )
            .await;

            if safe_retry {
                wait_for_task_state(&server, &mapping.task_id, A2aTaskState::Cancelled).await;
                wait_for_scheduler_idle(&server).await;
            } else {
                wait_for_cancellation_state(
                    &server,
                    &mapping.task_id,
                    A2aCancellationOutboxState::ReconcilePending,
                )
                .await;
            }
            let recovered = server.mapper_snapshot().await;
            let record = recovered
                .cancellation_for_task(&mapping.task_id, &owner_principal())
                .unwrap();
            assert_eq!(host.reconcile_cancel_calls.load(Ordering::SeqCst), 1);
            if safe_retry {
                assert_eq!(host.cancel_calls.load(Ordering::SeqCst), 1);
                assert_eq!(
                    recovered.tasks()[&mapping.task_id].state,
                    A2aTaskState::Cancelled
                );
                assert_eq!(record.state, A2aCancellationOutboxState::Settled);
                assert_eq!(record.attempts, 2);
            } else {
                assert_eq!(host.cancel_calls.load(Ordering::SeqCst), 0);
                assert_eq!(
                    recovered.tasks()[&mapping.task_id].state,
                    A2aTaskState::Working
                );
                assert_eq!(record.state, A2aCancellationOutboxState::ReconcilePending);
                assert_eq!(record.attempts, 1);
            }

            handle.cancel();
            task.await.unwrap().unwrap();
        }
    }

    #[tokio::test]
    async fn live_cancel_already_stopped_proof_settles_without_replaying_host_effect() {
        let (mut seeded, mapping, cancellation) =
            seeded_cancellation("live-cancel-already-stopped", A2aTaskState::Working, true);
        seeded
            .mark_cancellation_reconcile_pending(
                &cancellation.cancellation_id,
                "seed unknown live cancellation outcome",
            )
            .unwrap();
        let snapshots = Arc::new(InMemoryA2aMapperSnapshotStore::default());
        snapshots.persist_snapshot(&seeded).await.unwrap();
        let host = Arc::new(AlreadyStoppedCancelHost::default());
        let server = Arc::new(
            A2aHttpJsonRpcServer::new(
                Arc::new(Mutex::new(seeded)),
                snapshots.clone(),
                Arc::new(InMemoryA2aEventStore::default()),
                Arc::new(TestAuthenticator),
                host.clone(),
                agent_card(),
                A2aHttpConfig::default(),
            )
            .unwrap(),
        );
        let outcome = server
            .handle_cancel_task(
                JsonRpcRequest {
                    id: json!("live-already-stopped-retry"),
                    method: "CancelTask".into(),
                    params: json!({"tenant": "tenant-a", "id": mapping.task_id}),
                    correlation_id: None,
                    last_event_id: None,
                },
                owner_principal(),
                CancellationIngress::Public("127.0.0.1".parse().unwrap()),
                CancellationToken::new(),
            )
            .await
            .unwrap();
        let ConnectionOutcome::Response(response) = outcome else {
            panic!("live AlreadyStopped cancellation unexpectedly streamed")
        };
        assert_eq!(response.status, 200);
        let payload: Value = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(payload["result"]["status"]["state"], "TASK_STATE_CANCELED");
        assert_eq!(host.reconcile_cancel_calls.load(Ordering::SeqCst), 1);
        assert_eq!(host.cancel_calls.load(Ordering::SeqCst), 0);

        let persisted = snapshots.load_snapshot().await.unwrap();
        let record = persisted
            .cancellation_for_task(&mapping.task_id, &owner_principal())
            .unwrap();
        assert_eq!(
            persisted.tasks()[&mapping.task_id].state,
            A2aTaskState::Cancelled
        );
        assert_eq!(record.state, A2aCancellationOutboxState::Settled);
        assert_eq!(record.attempts, cancellation.attempts);
    }

    #[tokio::test]
    async fn parallel_already_stopped_retries_share_successful_control_completion() {
        let (mut seeded, mapping, cancellation_record) = seeded_cancellation(
            "parallel-already-stopped-cancel",
            A2aTaskState::Working,
            true,
        );
        seeded
            .mark_cancellation_reconcile_pending(
                &cancellation_record.cancellation_id,
                "seed parallel unknown cancellation outcome",
            )
            .unwrap();
        let snapshots = Arc::new(InMemoryA2aMapperSnapshotStore::default());
        snapshots.persist_snapshot(&seeded).await.unwrap();
        let host = Arc::new(BlockingAlreadyStoppedCancelHost::default());
        let server = Arc::new(
            A2aHttpJsonRpcServer::new(
                Arc::new(Mutex::new(seeded)),
                snapshots,
                Arc::new(InMemoryA2aEventStore::default()),
                Arc::new(TestAuthenticator),
                host.clone(),
                agent_card(),
                A2aHttpConfig::default(),
            )
            .unwrap(),
        );
        let _saturated_control = server
            .control_global
            .clone()
            .acquire_many_owned(server.config.max_control_dispatches.try_into().unwrap())
            .await
            .unwrap();
        let mut control_receiver = server.control_receiver.lock().await.take().unwrap();
        let scheduler_server = server.clone();
        let scheduler = tokio::spawn(async move {
            let scheduled = control_receiver
                .recv()
                .await
                .expect("parallel cancellation was not scheduled");
            let completion = scheduler_server
                .clone()
                .run_scheduled_dispatch(scheduled, CancellationToken::new())
                .await;
            scheduler_server
                .finish_scheduled_dispatch(completion)
                .unwrap();
        });

        let first_server = server.clone();
        let first_task_id = mapping.task_id.clone();
        let first = tokio::spawn(async move {
            first_server
                .handle_cancel_task(
                    JsonRpcRequest {
                        id: json!("parallel-already-stopped-first"),
                        method: "CancelTask".into(),
                        params: json!({"tenant": "tenant-a", "id": first_task_id}),
                        correlation_id: None,
                        last_event_id: None,
                    },
                    owner_principal(),
                    CancellationIngress::Public("127.0.0.1".parse().unwrap()),
                    CancellationToken::new(),
                )
                .await
                .unwrap()
        });
        timeout(Duration::from_secs(1), host.reconcile_started.notified())
            .await
            .expect("AlreadyStopped reconciliation did not start");

        let second_server = server.clone();
        let second_task_id = mapping.task_id.clone();
        let second = tokio::spawn(async move {
            second_server
                .handle_cancel_task(
                    JsonRpcRequest {
                        id: json!("parallel-already-stopped-second"),
                        method: "CancelTask".into(),
                        params: json!({"tenant": "tenant-a", "id": second_task_id}),
                        correlation_id: None,
                        last_event_id: None,
                    },
                    owner_principal(),
                    CancellationIngress::Public("127.0.0.1".parse().unwrap()),
                    CancellationToken::new(),
                )
                .await
                .unwrap()
        });
        wait_for_cancel_completion_subscribers(&server, 2).await;
        host.release_reconcile.notify_one();

        for outcome in [first.await.unwrap(), second.await.unwrap()] {
            let ConnectionOutcome::Response(response) = outcome else {
                panic!("parallel AlreadyStopped cancellation unexpectedly streamed")
            };
            let payload: Value = serde_json::from_slice(&response.body).unwrap();
            assert_eq!(payload["result"]["status"]["state"], "TASK_STATE_CANCELED");
        }
        timeout(Duration::from_secs(1), scheduler)
            .await
            .expect("parallel cancellation scheduler did not finish")
            .unwrap();
        assert_eq!(host.reconcile_cancel_calls.load(Ordering::SeqCst), 1);
        assert_eq!(host.cancel_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn already_stopped_commit_survives_owner_abort_during_event_flush() {
        let (mut seeded, mapping, cancellation_record) = seeded_cancellation(
            "already-stopped-aborted-event-flush",
            A2aTaskState::Working,
            true,
        );
        seeded
            .mark_cancellation_reconcile_pending(
                &cancellation_record.cancellation_id,
                "seed unknown cancellation before blocked event delivery",
            )
            .unwrap();
        let snapshots = Arc::new(InMemoryA2aMapperSnapshotStore::default());
        snapshots.persist_snapshot(&seeded).await.unwrap();
        let events = Arc::new(BlockingAppendEventStore::default());
        let host = Arc::new(BlockingAlreadyStoppedCancelHost::default());
        let server = Arc::new(
            A2aHttpJsonRpcServer::new(
                Arc::new(Mutex::new(seeded)),
                snapshots.clone(),
                events.clone(),
                Arc::new(TestAuthenticator),
                host.clone(),
                agent_card(),
                A2aHttpConfig::default(),
            )
            .unwrap(),
        );
        let mut control_receiver = server.control_receiver.lock().await.take().unwrap();
        let scheduler_server = server.clone();
        let scheduler = tokio::spawn(async move {
            let scheduled = control_receiver
                .recv()
                .await
                .expect("blocked-flush cancellation was not scheduled");
            let completion = scheduler_server
                .clone()
                .run_scheduled_dispatch(scheduled, CancellationToken::new())
                .await;
            scheduler_server
                .finish_scheduled_dispatch(completion)
                .unwrap();
        });

        let first_server = server.clone();
        let first_task_id = mapping.task_id.clone();
        let first = tokio::spawn(async move {
            first_server
                .handle_cancel_task(
                    JsonRpcRequest {
                        id: json!("already-stopped-aborted-flush-first"),
                        method: "CancelTask".into(),
                        params: json!({"tenant": "tenant-a", "id": first_task_id}),
                        correlation_id: None,
                        last_event_id: None,
                    },
                    owner_principal(),
                    CancellationIngress::Public("127.0.0.1".parse().unwrap()),
                    CancellationToken::new(),
                )
                .await
                .unwrap()
        });
        timeout(Duration::from_secs(1), host.reconcile_started.notified())
            .await
            .expect("blocked-flush AlreadyStopped reconciliation did not start");

        let second_server = server.clone();
        let second_task_id = mapping.task_id.clone();
        let second = tokio::spawn(async move {
            second_server
                .handle_cancel_task(
                    JsonRpcRequest {
                        id: json!("already-stopped-aborted-flush-second"),
                        method: "CancelTask".into(),
                        params: json!({"tenant": "tenant-a", "id": second_task_id}),
                        correlation_id: None,
                        last_event_id: None,
                    },
                    owner_principal(),
                    CancellationIngress::Public("127.0.0.1".parse().unwrap()),
                    CancellationToken::new(),
                )
                .await
                .unwrap()
        });
        wait_for_cancel_completion_subscribers(&server, 2).await;
        host.release_reconcile.notify_one();
        timeout(Duration::from_secs(1), async {
            while events.attempts.load(Ordering::SeqCst) < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("both cancellation requests did not reach blocked event delivery");

        timeout(Duration::from_secs(1), async {
            while !scheduler.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("durable AlreadyStopped proof did not commit its scheduler flight before event delivery");
        let durable = snapshots.load_snapshot().await.unwrap();
        assert_eq!(
            durable.tasks()[&mapping.task_id].state,
            A2aTaskState::Cancelled
        );
        assert_eq!(
            durable
                .cancellation_for_task(&mapping.task_id, &owner_principal())
                .unwrap()
                .state,
            A2aCancellationOutboxState::Settled
        );
        assert!(server
            .mapper_snapshot()
            .await
            .pending_events()
            .into_iter()
            .any(|event| {
                event.task_id == mapping.task_id && event.task.state == A2aTaskState::Cancelled
            }));

        first.abort();
        let _ = first.await;
        events.release();
        let outcome = timeout(Duration::from_secs(2), second)
            .await
            .expect("duplicate cancellation did not resume after event delivery recovered")
            .unwrap();
        let ConnectionOutcome::Response(response) = outcome else {
            panic!("duplicate blocked-flush cancellation unexpectedly streamed")
        };
        let payload: Value = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(payload["result"]["status"]["state"], "TASK_STATE_CANCELED");
        scheduler.await.unwrap();

        server
            .flush_pending_events_for_task(&mapping.task_id)
            .await
            .unwrap();
        assert!(server
            .mapper_snapshot()
            .await
            .pending_events()
            .into_iter()
            .all(|event| event.task_id != mapping.task_id));
        assert_eq!(host.reconcile_cancel_calls.load(Ordering::SeqCst), 1);
        assert_eq!(host.cancel_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn recovery_already_stopped_proof_settles_running_cancel_without_host_replay() {
        let (seeded, mapping, cancellation) = seeded_cancellation(
            "recovery-cancel-already-stopped",
            A2aTaskState::Working,
            true,
        );
        let snapshots = Arc::new(InMemoryA2aMapperSnapshotStore::default());
        snapshots.persist_snapshot(&seeded).await.unwrap();
        let host = Arc::new(AlreadyStoppedCancelHost::default());
        let (_address, server, handle, task) = start_server_with_dependencies(
            A2aHttpConfig::default(),
            Arc::new(Mutex::new(seeded)),
            snapshots.clone(),
            Arc::new(InMemoryA2aEventStore::default()),
            host.clone(),
            agent_card(),
        )
        .await;

        wait_for_task_state(&server, &mapping.task_id, A2aTaskState::Cancelled).await;
        wait_for_scheduler_idle(&server).await;
        assert_eq!(host.reconcile_cancel_calls.load(Ordering::SeqCst), 1);
        assert_eq!(host.cancel_calls.load(Ordering::SeqCst), 0);
        let persisted = snapshots.load_snapshot().await.unwrap();
        let record = persisted
            .cancellation_for_task(&mapping.task_id, &owner_principal())
            .unwrap();
        assert_eq!(record.state, A2aCancellationOutboxState::Settled);
        assert_eq!(record.attempts, cancellation.attempts);

        handle.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn aborted_cancellation_reconcile_owner_releases_single_flight_for_retry() {
        let (seeded, _mapping, cancellation_record) = seeded_cancellation(
            "aborted-cancel-reconcile-owner",
            A2aTaskState::Working,
            true,
        );
        let host = Arc::new(AbortableCancellationReconcileHost::default());
        let server = Arc::new(
            A2aHttpJsonRpcServer::new(
                Arc::new(Mutex::new(seeded)),
                Arc::new(InMemoryA2aMapperSnapshotStore::default()),
                Arc::new(InMemoryA2aEventStore::default()),
                Arc::new(TestAuthenticator),
                host.clone(),
                agent_card(),
                A2aHttpConfig::default(),
            )
            .unwrap(),
        );

        let first_server = server.clone();
        let first_record = cancellation_record.clone();
        let first = tokio::spawn(async move {
            first_server
                .reconcile_unknown_cancellation_once(
                    &first_record,
                    &CancellationToken::new(),
                    Instant::now() + Duration::from_secs(5),
                )
                .await
        });
        timeout(
            Duration::from_secs(1),
            host.first_reconcile_started.notified(),
        )
        .await
        .expect("first cancellation reconciliation did not acquire the single flight");
        first.abort();
        let _ = first.await;

        let decision = timeout(
            Duration::from_secs(1),
            server.reconcile_unknown_cancellation_once(
                &cancellation_record,
                &CancellationToken::new(),
                Instant::now() + Duration::from_millis(200),
            ),
        )
        .await
        .expect("retry remained stuck behind an aborted reconciliation owner");
        assert_eq!(decision, A2aUnknownDispatchDecision::SafeToRetry);
        assert_eq!(host.reconcile_cancel_calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancellation_claim_is_published_before_fast_host_completion_can_release_it() {
        let (mut seeded, mapping) =
            seeded_task("cancel-claim-publish-order", A2aTaskState::Working);
        for event in seeded.pending_events() {
            seeded.mark_event_settled(&event.event_id).unwrap();
        }
        let snapshots = Arc::new(InMemoryA2aMapperSnapshotStore::default());
        snapshots.persist_snapshot(&seeded).await.unwrap();
        let host = Arc::new(ClaimObservingFastErrorHost::default());
        let (address, server, handle, task) = start_server_with_dependencies(
            A2aHttpConfig::default(),
            Arc::new(Mutex::new(seeded)),
            snapshots,
            Arc::new(InMemoryA2aEventStore::default()),
            host.clone(),
            agent_card(),
        )
        .await;
        let hook = Arc::new(CancellationCommitAfterSendHook::new());
        *server.cancellation_commit_after_send_hook.lock().unwrap() = Some(hook.clone());
        let claim_released = Arc::new(Notify::new());
        *server.cancellation_claim_release_hook.lock().unwrap() = Some(claim_released.clone());

        let request_task = tokio::spawn(raw_http(
            address,
            request(
                "POST",
                "/a2a",
                &rpc_headers("application/json"),
                &cancel_body(&mapping.task_id, "claim-publish-order"),
            ),
        ));
        timeout(Duration::from_secs(1), hook.sent.notified())
            .await
            .expect("cancellation commit was not sent");
        timeout(Duration::from_secs(1), host.called.notified())
            .await
            .expect("fast cancellation host was not called");
        assert!(host.observed_claim.load(Ordering::SeqCst));
        // The scheduler must be allowed to finish while the request sender is still paused. The
        // host observation above proves the claim existed before `send`; this completion hook
        // proves that exact scheduler generation released it. Do not assert the transient set's
        // later value: the independent recovery loop may legitimately reclaim the still-unsettled
        // durable cancellation before shutdown.
        timeout(Duration::from_secs(1), claim_released.notified())
            .await
            .expect("fast host completion did not release the published claim");
        wait_for_scheduler_idle(&server).await;
        hook.resume_sender.notify_one();
        let _response = request_task.await.unwrap();
        handle.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn transient_running_fence_snapshot_failure_releases_recovery_claim_for_next_tick() {
        let (seeded, mapping, _) =
            seeded_cancellation("cancel-running-fence-retry", A2aTaskState::Working, false);
        let snapshots = Arc::new(FailNextSnapshotStore::default());
        snapshots.initialize(&seeded).await.unwrap();
        snapshots.fail_next_commit();
        let host = Arc::new(CancelHost::new(CancelHostBehavior::Stopped));
        let (_address, server, handle, task) = start_server_with_dependencies(
            A2aHttpConfig::default(),
            Arc::new(Mutex::new(seeded)),
            snapshots.clone(),
            Arc::new(InMemoryA2aEventStore::default()),
            host.clone(),
            agent_card(),
        )
        .await;

        wait_for_task_state(&server, &mapping.task_id, A2aTaskState::Cancelled).await;
        wait_for_scheduler_idle(&server).await;
        assert_eq!(host.cancel_calls.load(Ordering::SeqCst), 1);
        let persisted = snapshots.inner.load_snapshot().await.unwrap();
        let record = persisted
            .cancellation_for_task(&mapping.task_id, &owner_principal())
            .unwrap();
        assert_eq!(record.state, A2aCancellationOutboxState::Settled);
        assert_eq!(
            persisted.tasks()[&mapping.task_id].state,
            A2aTaskState::Cancelled
        );

        handle.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn exhausted_cancel_still_accepts_exact_already_stopped_proof() {
        let (mut seeded, mapping, _) = seeded_cancellation(
            "exhausted-cancel-already-stopped",
            A2aTaskState::Working,
            true,
        );
        let exhausted =
            exhaust_cancellation_attempts(&mut seeded, &mapping.task_id, &owner_principal());
        assert_eq!(exhausted.attempts, A2A_MAX_CANCELLATION_ATTEMPTS);
        assert_eq!(
            exhausted.state,
            A2aCancellationOutboxState::ReconcilePending
        );
        let snapshots = Arc::new(InMemoryA2aMapperSnapshotStore::default());
        snapshots.persist_snapshot(&seeded).await.unwrap();
        let host = Arc::new(AlreadyStoppedCancelHost::default());
        let (_address, server, handle, task) = start_server_with_dependencies(
            A2aHttpConfig::default(),
            Arc::new(Mutex::new(seeded)),
            snapshots.clone(),
            Arc::new(InMemoryA2aEventStore::default()),
            host.clone(),
            agent_card(),
        )
        .await;

        wait_for_task_state(&server, &mapping.task_id, A2aTaskState::Cancelled).await;
        wait_for_scheduler_idle(&server).await;
        assert_eq!(host.reconcile_cancel_calls.load(Ordering::SeqCst), 1);
        assert_eq!(host.cancel_calls.load(Ordering::SeqCst), 0);
        let persisted = snapshots.load_snapshot().await.unwrap();
        let record = persisted
            .cancellation_for_task(&mapping.task_id, &owner_principal())
            .unwrap();
        assert_eq!(record.state, A2aCancellationOutboxState::Settled);
        assert_eq!(record.attempts, A2A_MAX_CANCELLATION_ATTEMPTS);

        handle.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn parallel_duplicate_cancel_requests_share_one_control_single_flight() {
        let (seeded, mapping) = seeded_task("parallel-duplicate-cancel", A2aTaskState::Working);
        let snapshots = Arc::new(InMemoryA2aMapperSnapshotStore::default());
        snapshots.persist_snapshot(&seeded).await.unwrap();
        let host = Arc::new(CancelRegressionHost::blocking());
        let (address, server, handle, task) = start_server_with_dependencies(
            A2aHttpConfig::default(),
            Arc::new(Mutex::new(seeded)),
            snapshots,
            Arc::new(InMemoryA2aEventStore::default()),
            host.clone(),
            agent_card(),
        )
        .await;

        let first_request = request(
            "POST",
            "/a2a",
            &rpc_headers("application/json"),
            &cancel_body(&mapping.task_id, "parallel-cancel-id1"),
        );
        let first = tokio::spawn(raw_http(address, first_request));
        timeout(Duration::from_secs(1), host.cancel_started.notified())
            .await
            .expect("first cancellation host callback did not start");

        let second_request = request(
            "POST",
            "/a2a",
            &rpc_headers("application/json"),
            &cancel_body(&mapping.task_id, "parallel-cancel-id2"),
        );
        let second = tokio::spawn(raw_http(address, second_request));
        wait_for_cancel_completion_subscribers(&server, 2).await;
        assert_eq!(host.cancel_calls.load(Ordering::SeqCst), 1);
        host.release_cancel.notify_one();

        let first = first.await.unwrap();
        let second = second.await.unwrap();
        for response in [&first, &second] {
            assert_eq!(
                body(response)["result"]["status"]["state"],
                "TASK_STATE_CANCELED"
            );
        }
        wait_for_scheduler_idle(&server).await;
        let settled = server.mapper_snapshot().await;
        let record = settled
            .cancellation_for_task(&mapping.task_id, &owner_principal())
            .unwrap();
        assert_eq!(host.cancel_calls.load(Ordering::SeqCst), 1);
        assert_eq!(host.observed_controls.lock().unwrap().len(), 1);
        assert_eq!(record.state, A2aCancellationOutboxState::Settled);
        assert_eq!(record.attempts, 1);

        handle.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn same_owner_duplicate_cancels_leave_control_admission_for_another_owner() {
        let owner = owner_principal();
        let other = other_principal();
        let (mut seeded, blocked_mapping) =
            seeded_task("blocked-owner-cancel", A2aTaskState::Working);
        let blocked_event_ids: Vec<_> = seeded
            .pending_events()
            .into_iter()
            .map(|event| event.event_id)
            .collect();
        for event_id in blocked_event_ids {
            seeded.mark_event_settled(&event_id).unwrap();
        }
        let other_message_id = "independent-owner-cancel";
        let other_mapping = prepare_seeded_message_for(&mut seeded, other_message_id, &other);
        let other_dispatch_id = seeded
            .dispatch_for_message(other_message_id, &other)
            .unwrap()
            .dispatch_id
            .clone();
        seeded.mark_dispatch_settled(&other_dispatch_id).unwrap();
        let other_event_id = event_id_for_message(&seeded, other_message_id, &other);
        seeded.mark_event_settled(&other_event_id).unwrap();

        let snapshots = Arc::new(InMemoryA2aMapperSnapshotStore::default());
        snapshots.persist_snapshot(&seeded).await.unwrap();
        let host = Arc::new(SelectiveBlockingCancelHost {
            blocked_task_id: blocked_mapping.task_id.clone(),
            blocked_started: Notify::new(),
            release_blocked: Notify::new(),
            blocked_calls: AtomicUsize::new(0),
            other_calls: AtomicUsize::new(0),
        });
        let config = A2aHttpConfig {
            max_control_dispatches: 4,
            max_control_dispatches_per_owner: 2,
            max_control_requests_per_ip: 3,
            max_control_requests_per_owner: 2,
            ..A2aHttpConfig::default()
        };
        let (address, server, handle, task) = start_server_with_dependencies(
            config,
            Arc::new(Mutex::new(seeded)),
            snapshots,
            Arc::new(InMemoryA2aEventStore::default()),
            host.clone(),
            agent_card(),
        )
        .await;

        let first = tokio::spawn(raw_http(
            address,
            request(
                "POST",
                "/a2a",
                &rpc_headers("application/json"),
                &cancel_body(&blocked_mapping.task_id, "blocked-cancel-1"),
            ),
        ));
        timeout(Duration::from_secs(1), host.blocked_started.notified())
            .await
            .expect("blocked owner cancellation did not start");
        let second = tokio::spawn(raw_http(
            address,
            request(
                "POST",
                "/a2a",
                &rpc_headers("application/json"),
                &cancel_body(&blocked_mapping.task_id, "blocked-cancel-2"),
            ),
        ));
        wait_for_cancel_completion_subscribers(&server, 2).await;

        let blocked_owner = A2aEventOwner {
            subject: owner.subject.clone(),
            tenant_id: owner.tenant_id.clone(),
        };
        timeout(Duration::from_secs(1), async {
            loop {
                let active = server
                    .control_request_limiter
                    .state
                    .lock()
                    .unwrap()
                    .active_per_owner
                    .get(&blocked_owner)
                    .copied()
                    .unwrap_or_default();
                if active == 2 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("duplicate cancellation did not occupy its bounded owner quota");

        let third = raw_http(
            address,
            request(
                "POST",
                "/a2a",
                &rpc_headers("application/json"),
                &cancel_body(&blocked_mapping.task_id, "blocked-cancel-3"),
            ),
        )
        .await;
        assert!(third.starts_with("HTTP/1.1 429"));
        assert_eq!(server.control_request_global.available_permits(), 2);

        let mut other_headers = rpc_headers("application/json");
        other_headers[0] = ("Authorization", "Bearer other");
        let independent = raw_http(
            address,
            request(
                "POST",
                "/a2a",
                &other_headers,
                &cancel_body_for_tenant(
                    &other_mapping.task_id,
                    "independent-owner-control",
                    "tenant-b",
                ),
            ),
        )
        .await;
        assert_eq!(
            body(&independent)["result"]["status"]["state"],
            "TASK_STATE_CANCELED"
        );
        assert_eq!(host.other_calls.load(Ordering::SeqCst), 1);
        assert_eq!(server.control_request_global.available_permits(), 2);

        host.release_blocked.notify_one();
        for response in [first.await.unwrap(), second.await.unwrap()] {
            assert_eq!(
                body(&response)["result"]["status"]["state"],
                "TASK_STATE_CANCELED"
            );
        }
        wait_for_scheduler_idle(&server).await;
        assert_eq!(host.blocked_calls.load(Ordering::SeqCst), 1);
        assert_eq!(server.control_request_global.available_permits(), 4);

        handle.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn durable_cancel_rejects_late_message_completion_before_cancel_ack_settles() {
        let host = Arc::new(CompletionAfterCancelHost::new());
        let (address, server, handle, task) =
            start_server_with_host(A2aHttpConfig::default(), host.clone()).await;
        let sent = raw_http(
            address,
            request(
                "POST",
                "/a2a",
                &rpc_headers("application/json"),
                &send_body("completion-after-cancel", false, Some(true)),
            ),
        )
        .await;
        let task_id = body(&sent)["result"]["task"]["id"]
            .as_str()
            .unwrap()
            .to_owned();
        timeout(Duration::from_secs(1), host.message_started.notified())
            .await
            .expect("message host did not start before cancellation");

        let cancelled = raw_http(
            address,
            request(
                "POST",
                "/a2a",
                &rpc_headers("application/json"),
                &cancel_body(&task_id, "completion-after-cancel-request"),
            ),
        )
        .await;
        assert_eq!(
            body(&cancelled)["result"]["status"]["state"],
            "TASK_STATE_CANCELED",
            "unexpected cancellation response: {cancelled}"
        );
        wait_for_scheduler_idle(&server).await;
        let settled = server.mapper_snapshot().await;
        let cancellation = settled
            .cancellation_for_task(&task_id, &owner_principal())
            .unwrap();
        assert_eq!(settled.tasks()[&task_id].state, A2aTaskState::Cancelled);
        assert_eq!(cancellation.state, A2aCancellationOutboxState::Settled);
        assert_eq!(host.completion_attempts.load(Ordering::SeqCst), 1);
        assert_eq!(host.completion_rejections.load(Ordering::SeqCst), 1);
        assert_eq!(host.completion_side_effects.load(Ordering::SeqCst), 0);
        assert_eq!(host.cancel_calls.load(Ordering::SeqCst), 1);

        handle.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn card_and_send_contract_edges_are_strict_without_rejecting_extensions_or_history() {
        let mut empty_skills = agent_card();
        empty_skills.skills.clear();
        assert_eq!(
            A2aHttpJsonRpcServer::new(
                Arc::new(Mutex::new(A2aMapper::new())),
                Arc::new(InMemoryA2aMapperSnapshotStore::default()),
                Arc::new(InMemoryA2aEventStore::default()),
                Arc::new(TestAuthenticator),
                Arc::new(CompletingHost::default()),
                empty_skills,
                A2aHttpConfig::default(),
            )
            .err()
            .unwrap()
            .code,
            ProtocolErrorCode::InvalidRequest
        );
        let mut empty_tags = agent_card();
        empty_tags.skills[0].tags.clear();
        assert_eq!(
            A2aHttpJsonRpcServer::new(
                Arc::new(Mutex::new(A2aMapper::new())),
                Arc::new(InMemoryA2aMapperSnapshotStore::default()),
                Arc::new(InMemoryA2aEventStore::default()),
                Arc::new(TestAuthenticator),
                Arc::new(CompletingHost::default()),
                empty_tags,
                A2aHttpConfig::default(),
            )
            .err()
            .unwrap()
            .code,
            ProtocolErrorCode::InvalidRequest
        );

        let (address, _server, handle, task) = start_server(A2aHttpConfig::default()).await;
        let mut agent_role: Value =
            serde_json::from_str(&send_body("agent-role", true, None)).unwrap();
        agent_role["params"]["message"]["role"] = json!("ROLE_AGENT");
        let rejected_role = raw_http(
            address,
            request(
                "POST",
                "/a2a",
                &rpc_headers("application/json"),
                &agent_role.to_string(),
            ),
        )
        .await;
        assert_eq!(body(&rejected_role)["error"]["code"], -32602);

        let mut extended: Value =
            serde_json::from_str(&send_body("unknown-send-fields", true, None)).unwrap();
        extended["params"]["futureSendOption"] = json!({"enabled": true});
        extended["params"]["message"]["futureMessageOption"] = json!("preserve-compatible");
        let accepted_extension = raw_http(
            address,
            request(
                "POST",
                "/a2a",
                &rpc_headers("application/json"),
                &extended.to_string(),
            ),
        )
        .await;
        let task_id = body(&accepted_extension)["result"]["task"]["id"]
            .as_str()
            .unwrap()
            .to_owned();
        assert_eq!(
            body(&accepted_extension)["result"]["task"]["status"]["state"],
            "TASK_STATE_COMPLETED"
        );

        let unsupported = json!({
            "jsonrpc":"2.0",
            "id":"unsupported-media",
            "method":"SendMessage",
            "params":{
                "tenant":"tenant-a",
                "message":{
                    "messageId":"unsupported-media",
                    "role":"ROLE_USER",
                    "parts":[{"url":"https://example.invalid/image.png","mediaType":"image/png"}]
                }
            }
        })
        .to_string();
        let rejected_media = raw_http(
            address,
            request(
                "POST",
                "/a2a",
                &rpc_headers("application/json"),
                &unsupported,
            ),
        )
        .await;
        assert_eq!(body(&rejected_media)["error"]["code"], -32005);

        let get_with_history = json!({
            "jsonrpc":"2.0",
            "id":"history-get",
            "method":"GetTask",
            "params":{"tenant":"tenant-a","id":task_id,"historyLength":5}
        })
        .to_string();
        let history = raw_http(
            address,
            request(
                "POST",
                "/a2a",
                &rpc_headers("application/json"),
                &get_with_history,
            ),
        )
        .await;
        assert_eq!(body(&history)["result"]["id"], task_id);

        handle.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn inbound_raw_and_text_data_media_types_are_canonical_durable_and_idempotent() {
        let host = Arc::new(CompletingHost::default());
        let mut card = agent_card();
        card.default_input_modes = vec![
            "text/plain".into(),
            "application/json".into(),
            "application/octet-stream".into(),
        ];
        let (address, server, handle, task) = start_server_with_dependencies(
            A2aHttpConfig::default(),
            Arc::new(Mutex::new(A2aMapper::new())),
            Arc::new(InMemoryA2aMapperSnapshotStore::default()),
            Arc::new(InMemoryA2aEventStore::default()),
            host.clone(),
            card,
        )
        .await;

        for (index, raw) in ["dGNr", "dA==", ""].into_iter().enumerate() {
            let message_id = format!("raw-valid-{index}");
            let payload = json!({
                "jsonrpc":"2.0","id":message_id,"method":"SendMessage",
                "params":{"tenant":"tenant-a","message":{
                    "messageId":message_id,"role":"ROLE_USER",
                    "parts":[{"raw":raw,"mediaType":"text/plain","filename":"input.txt"}],
                    "metadata":{"complete":true}
                }}
            })
            .to_string();
            let accepted = raw_http(
                address,
                request("POST", "/a2a", &rpc_headers("application/json"), &payload),
            )
            .await;
            assert_eq!(
                body(&accepted)["result"]["task"]["status"]["state"],
                "TASK_STATE_COMPLETED"
            );
            let snapshot = server.mapper_snapshot().await;
            let receipt = snapshot
                .message_receipt(&format!("raw-valid-{index}"), &owner_principal())
                .unwrap();
            let rich = receipt.message.content_parts().unwrap();
            assert!(matches!(
                &rich[0],
                A2aContentPart::Raw {
                    raw: bytes,
                    media_type,
                    filename: Some(filename),
                } if STANDARD_BASE64.encode(bytes) == raw
                    && media_type == "text/plain"
                    && filename == "input.txt"
            ));
            let wire = serde_json::to_value(A2aWireMessage::from(&receipt.message)).unwrap();
            assert_eq!(wire["parts"][0]["raw"], raw);
            assert_eq!(wire["parts"][0]["mediaType"], "text/plain");
            assert_eq!(wire["parts"][0]["filename"], "input.txt");
        }

        for (index, raw) in ["!!!!", "dA=", "dA===", "Zh=="].into_iter().enumerate() {
            let before = server.mapper_snapshot().await;
            let message_id = format!("raw-invalid-{index}");
            let payload = json!({
                "jsonrpc":"2.0","id":message_id,"method":"SendMessage",
                "params":{"tenant":"tenant-a","message":{
                    "messageId":message_id,"role":"ROLE_USER",
                    "parts":[{"raw":raw,"mediaType":"text/plain"}],
                    "metadata":{"complete":true}
                }}
            })
            .to_string();
            let rejected = raw_http(
                address,
                request("POST", "/a2a", &rpc_headers("application/json"), &payload),
            )
            .await;
            assert_eq!(body(&rejected)["error"]["code"], -32602);
            assert_eq!(server.mapper_snapshot().await, before);
        }

        for (message_id, part) in [
            (
                "typed-text",
                json!({"text":"hello","mediaType":"text/plain"}),
            ),
            (
                "typed-data",
                json!({"data":{"key":"value"},"mediaType":"application/json"}),
            ),
        ] {
            let payload = json!({
                "jsonrpc":"2.0","id":message_id,"method":"SendMessage",
                "params":{"tenant":"tenant-a","message":{
                    "messageId":message_id,"role":"ROLE_USER","parts":[part],
                    "metadata":{"complete":true}
                }}
            })
            .to_string();
            let accepted = raw_http(
                address,
                request("POST", "/a2a", &rpc_headers("application/json"), &payload),
            )
            .await;
            assert_eq!(
                body(&accepted)["result"]["task"]["status"]["state"],
                "TASK_STATE_COMPLETED"
            );
        }

        let snapshot = server.mapper_snapshot().await;
        let restored: A2aMapper = serde_json::from_slice(&serde_json::to_vec(&snapshot).unwrap())
            .expect("rich input snapshot must round-trip");
        for (message_id, media_type) in [
            ("typed-text", "text/plain"),
            ("typed-data", "application/json"),
        ] {
            let receipt = restored
                .message_receipt(message_id, &owner_principal())
                .unwrap();
            let wire = serde_json::to_value(A2aWireMessage::from(&receipt.message)).unwrap();
            assert_eq!(wire["parts"][0]["mediaType"], media_type);
        }

        let exact_text = json!({
            "jsonrpc":"2.0","id":"typed-text-retry","method":"SendMessage",
            "params":{"tenant":"tenant-a","message":{
                "messageId":"typed-text","role":"ROLE_USER",
                "parts":[{"text":"hello","mediaType":"text/plain"}],
                "metadata":{"complete":true}
            }}
        })
        .to_string();
        let exact = raw_http(
            address,
            request(
                "POST",
                "/a2a",
                &rpc_headers("application/json"),
                &exact_text,
            ),
        )
        .await;
        assert!(body(&exact)["result"]["task"].is_object());
        let before_conflict = server.mapper_snapshot().await;
        let changed_media = exact_text.replace("text/plain", "application/json");
        let conflict = raw_http(
            address,
            request(
                "POST",
                "/a2a",
                &rpc_headers("application/json"),
                &changed_media,
            ),
        )
        .await;
        assert_eq!(body(&conflict)["error"]["code"], -32602);
        assert_eq!(server.mapper_snapshot().await, before_conflict);
        assert_eq!(host.calls.load(Ordering::SeqCst), 5);

        handle.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn blocking_send_default_and_false_wait_for_host_settlement() {
        let host = Arc::new(DelayedHost::new(Duration::from_millis(40)));
        let (address, _server, handle, task) =
            start_server_with_host(A2aHttpConfig::default(), host.clone()).await;

        for (message_id, return_immediately) in
            [("blocking-default", None), ("blocking-false", Some(false))]
        {
            let started = Instant::now();
            let response = raw_http(
                address,
                request(
                    "POST",
                    "/a2a",
                    &rpc_headers("application/json"),
                    &send_body(message_id, true, return_immediately),
                ),
            )
            .await;
            assert!(
                started.elapsed() >= Duration::from_millis(30),
                "blocking response returned before the delayed host settled"
            );
            assert_eq!(
                body(&response)["result"]["task"]["status"]["state"],
                "TASK_STATE_COMPLETED"
            );
        }
        assert_eq!(
            host.modes.lock().unwrap().as_slice(),
            [A2aExecutionMode::Blocking, A2aExecutionMode::Blocking]
        );

        handle.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn immediate_send_returns_working_before_delayed_host_finishes() {
        let host = Arc::new(DelayedHost::new(Duration::from_millis(300)));
        let snapshots = Arc::new(InMemoryA2aMapperSnapshotStore::default());
        let (address, server, handle, task) = start_server_with_dependencies(
            A2aHttpConfig::default(),
            Arc::new(Mutex::new(A2aMapper::new())),
            snapshots.clone(),
            Arc::new(InMemoryA2aEventStore::default()),
            host.clone(),
            agent_card(),
        )
        .await;
        let payload = send_body("immediate-delayed", true, Some(true));
        let started = Instant::now();
        let response = raw_http(
            address,
            request("POST", "/a2a", &rpc_headers("application/json"), &payload),
        )
        .await;
        let first_result = body(&response)["result"].clone();
        let task_id = body(&response)["result"]["task"]["id"]
            .as_str()
            .unwrap()
            .to_owned();
        assert_eq!(
            body(&response)["result"]["task"]["status"]["state"],
            "TASK_STATE_WORKING"
        );
        assert!(started.elapsed() < Duration::from_millis(250));
        timeout(Duration::from_secs(1), host.started.notified())
            .await
            .expect("immediate host did not start");
        assert!(
            timeout(Duration::from_millis(20), host.finished.notified())
                .await
                .is_err(),
            "immediate response waited for host completion"
        );
        timeout(Duration::from_secs(1), host.finished.notified())
            .await
            .expect("immediate host did not finish");
        wait_for_scheduler_idle(&server).await;
        assert_eq!(
            server.mapper_snapshot().await.tasks()[&task_id].state,
            A2aTaskState::Completed
        );
        assert_eq!(
            host.modes.lock().unwrap().as_slice(),
            [A2aExecutionMode::Immediate]
        );

        let duplicate = raw_http(
            address,
            request("POST", "/a2a", &rpc_headers("application/json"), &payload),
        )
        .await;
        assert_eq!(body(&duplicate)["result"], first_result);
        assert_eq!(host.modes.lock().unwrap().len(), 1);

        handle.cancel();
        task.await.unwrap().unwrap();

        let restored = snapshots.load_snapshot().await.unwrap();
        let (restart_address, _restart_server, restart_handle, restart_task) =
            start_server_with_dependencies(
                A2aHttpConfig::default(),
                Arc::new(Mutex::new(restored)),
                snapshots,
                Arc::new(InMemoryA2aEventStore::default()),
                host.clone(),
                agent_card(),
            )
            .await;
        let restarted_duplicate = raw_http(
            restart_address,
            request("POST", "/a2a", &rpc_headers("application/json"), &payload),
        )
        .await;
        assert_eq!(body(&restarted_duplicate)["result"], first_result);
        assert_eq!(host.modes.lock().unwrap().len(), 1);
        restart_handle.cancel();
        restart_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn same_task_successor_is_not_accepted_while_earlier_dispatch_is_active() {
        let host = Arc::new(DelayedHost::new(Duration::from_millis(200)));
        let (address, server, handle, task) =
            start_server_with_host(A2aHttpConfig::default(), host.clone()).await;
        let first = raw_http(
            address,
            request(
                "POST",
                "/a2a",
                &rpc_headers("application/json"),
                &send_body("active-predecessor", true, Some(true)),
            ),
        )
        .await;
        let first_body = body(&first);
        let task_id = first_body["result"]["task"]["id"]
            .as_str()
            .unwrap()
            .to_owned();
        let context_id = first_body["result"]["task"]["contextId"]
            .as_str()
            .unwrap()
            .to_owned();
        timeout(Duration::from_secs(1), host.started.notified())
            .await
            .expect("predecessor host dispatch did not start");

        let successor = raw_http(
            address,
            request(
                "POST",
                "/a2a",
                &rpc_headers("application/json"),
                &resume_body(&task_id, &context_id, "active-successor"),
            ),
        )
        .await;
        assert_eq!(body(&successor)["error"]["code"], -32602);
        let snapshot = server.mapper_snapshot().await;
        assert!(snapshot
            .dispatch_for_message("active-successor", &owner_principal())
            .is_none());
        assert!(snapshot
            .message_receipt("active-successor", &owner_principal())
            .is_none());

        timeout(Duration::from_secs(1), host.finished.notified())
            .await
            .expect("predecessor host dispatch did not finish");
        wait_for_scheduler_idle(&server).await;
        assert_eq!(
            host.modes.lock().unwrap().as_slice(),
            [A2aExecutionMode::Immediate]
        );
        assert_eq!(
            server.mapper_snapshot().await.tasks()[&task_id].state,
            A2aTaskState::Completed
        );

        handle.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn streaming_opens_sse_and_writes_initial_task_before_host_completion() {
        let host = Arc::new(DelayedHost::new(Duration::from_millis(120)));
        let (address, _server, handle, task) =
            start_server_with_host(A2aHttpConfig::default(), host.clone()).await;
        let mut payload: Value =
            serde_json::from_str(&send_body("stream-delayed", true, None)).unwrap();
        payload["method"] = json!("SendStreamingMessage");
        let mut stream = TcpStream::connect(address).await.unwrap();
        stream
            .write_all(&request(
                "POST",
                "/a2a",
                &rpc_headers("text/event-stream"),
                &payload.to_string(),
            ))
            .await
            .unwrap();
        stream.shutdown().await.unwrap();
        let mut reader = BufReader::new(stream);
        let mut headers = String::new();
        loop {
            let mut line = String::new();
            reader.read_line(&mut line).await.unwrap();
            if line == "\r\n" {
                break;
            }
            headers.push_str(&line);
        }
        assert!(headers.starts_with("HTTP/1.1 200"));
        assert!(headers.contains("Content-Type: text/event-stream"));

        let first = timeout(Duration::from_secs(1), async {
            loop {
                let mut line = String::new();
                assert_ne!(reader.read_line(&mut line).await.unwrap(), 0);
                if let Some(data) = line.strip_prefix("data: ") {
                    break serde_json::from_str::<Value>(data.trim()).unwrap();
                }
            }
        })
        .await
        .expect("initial SSE task timed out");
        assert_eq!(
            first["result"]["task"]["status"]["state"],
            "TASK_STATE_WORKING"
        );
        timeout(Duration::from_secs(1), host.started.notified())
            .await
            .expect("streaming host did not start after the initial task");

        let terminal = timeout(Duration::from_secs(1), async {
            loop {
                let mut line = String::new();
                assert_ne!(reader.read_line(&mut line).await.unwrap(), 0);
                if let Some(data) = line.strip_prefix("data: ") {
                    let value: Value = serde_json::from_str(data.trim()).unwrap();
                    if value["result"]["statusUpdate"]["status"]["state"] == "TASK_STATE_COMPLETED"
                    {
                        break value;
                    }
                }
            }
        })
        .await
        .expect("terminal SSE status timed out");
        assert_eq!(
            terminal["result"]["statusUpdate"]["status"]["state"],
            "TASK_STATE_COMPLETED"
        );
        assert_eq!(
            host.modes.lock().unwrap().as_slice(),
            [A2aExecutionMode::Streaming]
        );

        handle.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn queued_live_streaming_intent_blocks_immediate_recovery_until_start_gate() {
        let snapshots = Arc::new(InMemoryA2aMapperSnapshotStore::default());
        let events = Arc::new(FirstAppendBarrierEventStore::default());
        let host = Arc::new(DelayedHost::new(Duration::from_millis(20)));
        let server = Arc::new(
            A2aHttpJsonRpcServer::new(
                Arc::new(Mutex::new(A2aMapper::new())),
                snapshots,
                events.clone(),
                Arc::new(TestAuthenticator),
                host.clone(),
                agent_card(),
                A2aHttpConfig::default(),
            )
            .unwrap(),
        );
        server.initialize_snapshot_store().await.unwrap();
        let mut dispatch_receiver = server.dispatch_receiver.lock().await.take().unwrap();
        let scheduler_server = server.clone();
        let scheduler = tokio::spawn(async move {
            let scheduled = dispatch_receiver
                .recv()
                .await
                .expect("post-commit live streaming dispatch was not staged");
            let completion = scheduler_server
                .clone()
                .run_scheduled_dispatch(scheduled, CancellationToken::new())
                .await;
            scheduler_server
                .finish_scheduled_dispatch(completion)
                .unwrap();
        });

        let request_server = server.clone();
        let request = tokio::spawn(async move {
            request_server
                .handle_send(
                    send_rpc_request("queued-live-streaming-intent"),
                    owner_principal(),
                    true,
                    CancellationToken::new(),
                )
                .await
        });
        timeout(Duration::from_secs(1), events.first_entered.notified())
            .await
            .expect("live streaming send did not block in its acceptance-event append");

        let queued = server.mapper_snapshot().await;
        let dispatch = queued
            .dispatch_for_message("queued-live-streaming-intent", &owner_principal())
            .unwrap()
            .clone();
        assert_eq!(dispatch.state, A2aDispatchOutboxState::Queued);
        assert!(server
            .recovery_was_attempted(&dispatch.dispatch_id)
            .unwrap());
        assert_eq!(server.dispatch_state.lock().unwrap().accepted, 1);

        // A concurrent recovery publisher settles the exact event while the live request remains
        // paused in its first append. Dispatch recovery now sees a runnable Queued record, but the
        // post-commit live reservation and claim must prevent an Immediate takeover.
        let recovery_cancellation = CancellationToken::new();
        let mut event_cursor = None;
        assert_eq!(
            server
                .recover_pending_events(
                    &recovery_cancellation,
                    Instant::now() + Duration::from_secs(1),
                    &mut event_cursor,
                )
                .await,
            1
        );
        assert!(server
            .mapper_snapshot()
            .await
            .dispatch_event_ready(&dispatch));
        let mut dispatch_cursor = None;
        assert_eq!(
            server
                .recover_pending_dispatches(
                    &recovery_cancellation,
                    Instant::now() + Duration::from_secs(1),
                    &mut dispatch_cursor,
                )
                .await,
            0
        );
        assert!(host.modes.lock().unwrap().is_empty());
        assert_eq!(
            server.mapper_snapshot().await.dispatch_outbox()[&dispatch.dispatch_id].state,
            A2aDispatchOutboxState::Queued
        );

        events.release_first();
        let outcome = timeout(Duration::from_secs(2), request)
            .await
            .expect("live streaming send did not resume")
            .unwrap()
            .unwrap();
        let ConnectionOutcome::Stream(mut plan) = outcome else {
            panic!("live streaming send did not retain its stream response")
        };
        assert!(plan.dispatch_start.is_some());
        assert!(host.modes.lock().unwrap().is_empty());
        plan.dispatch_start
            .take()
            .unwrap()
            .send(())
            .expect("streaming start gate receiver disappeared");
        timeout(Duration::from_secs(1), host.started.notified())
            .await
            .expect("streaming host did not start after the explicit gate");
        wait_for_task_state(&server, &dispatch.task_id, A2aTaskState::Completed).await;
        scheduler.await.unwrap();
        assert_eq!(
            host.modes.lock().unwrap().as_slice(),
            [A2aExecutionMode::Streaming]
        );
        assert_eq!(
            server.mapper_snapshot().await.dispatch_outbox()[&dispatch.dispatch_id].state,
            A2aDispatchOutboxState::Settled
        );
        assert!(!server
            .recovery_was_attempted(&dispatch.dispatch_id)
            .unwrap());
        assert_eq!(server.dispatch_state.lock().unwrap().accepted, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn aborted_queued_live_activation_releases_stage_for_later_recovery() {
        let snapshots = Arc::new(InMemoryA2aMapperSnapshotStore::default());
        let events = Arc::new(FirstAppendBarrierEventStore::default());
        let host = Arc::new(CompletingHost::default());
        let server = Arc::new(
            A2aHttpJsonRpcServer::new(
                Arc::new(Mutex::new(A2aMapper::new())),
                snapshots,
                events.clone(),
                Arc::new(TestAuthenticator),
                host.clone(),
                agent_card(),
                A2aHttpConfig::default(),
            )
            .unwrap(),
        );
        server.initialize_snapshot_store().await.unwrap();
        let mut dispatch_receiver = server.dispatch_receiver.lock().await.take().unwrap();
        let scheduler_server = server.clone();
        let scheduler = tokio::spawn(async move {
            for _ in 0..2 {
                let scheduled = dispatch_receiver
                    .recv()
                    .await
                    .expect("expected staged or recovered dispatch was not scheduled");
                let completion = scheduler_server
                    .clone()
                    .run_scheduled_dispatch(scheduled, CancellationToken::new())
                    .await;
                scheduler_server
                    .finish_scheduled_dispatch(completion)
                    .unwrap();
            }
        });

        let request_server = server.clone();
        let request = tokio::spawn(async move {
            request_server
                .handle_send(
                    send_rpc_request("aborted-queued-live-activation"),
                    owner_principal(),
                    false,
                    CancellationToken::new(),
                )
                .await
        });
        timeout(Duration::from_secs(1), events.first_entered.notified())
            .await
            .expect("live send did not reach the queued event barrier");
        let dispatch = server
            .mapper_snapshot()
            .await
            .dispatch_for_message("aborted-queued-live-activation", &owner_principal())
            .unwrap()
            .clone();
        assert_eq!(dispatch.state, A2aDispatchOutboxState::Queued);
        assert!(server
            .recovery_was_attempted(&dispatch.dispatch_id)
            .unwrap());

        request.abort();
        assert!(request.await.unwrap_err().is_cancelled());
        wait_for_scheduler_idle(&server).await;
        assert!(!server
            .recovery_was_attempted(&dispatch.dispatch_id)
            .unwrap());

        events.release_first();
        let recovery_cancellation = CancellationToken::new();
        let mut event_cursor = None;
        assert_eq!(
            server
                .recover_pending_events(
                    &recovery_cancellation,
                    Instant::now() + Duration::from_secs(1),
                    &mut event_cursor,
                )
                .await,
            1
        );
        let mut dispatch_cursor = None;
        assert_eq!(
            server
                .recover_pending_dispatches(
                    &recovery_cancellation,
                    Instant::now() + Duration::from_secs(1),
                    &mut dispatch_cursor,
                )
                .await,
            1
        );
        wait_for_task_state(&server, &dispatch.task_id, A2aTaskState::Completed).await;
        scheduler.await.unwrap();
        assert_eq!(host.reconcile_calls.load(Ordering::SeqCst), 0);
        assert_eq!(host.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            server.mapper_snapshot().await.dispatch_outbox()[&dispatch.dispatch_id].state,
            A2aDispatchOutboxState::Settled
        );
        assert!(!server
            .recovery_was_attempted(&dispatch.dispatch_id)
            .unwrap());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn live_dispatch_claim_blocks_recovery_between_running_and_host_callback() {
        let snapshots = Arc::new(InMemoryA2aMapperSnapshotStore::default());
        let host = Arc::new(CompletingHost::default());
        let server = Arc::new(
            A2aHttpJsonRpcServer::new(
                Arc::new(Mutex::new(A2aMapper::new())),
                snapshots,
                Arc::new(InMemoryA2aEventStore::default()),
                Arc::new(TestAuthenticator),
                host.clone(),
                agent_card(),
                A2aHttpConfig::default(),
            )
            .unwrap(),
        );
        server.initialize_snapshot_store().await.unwrap();
        let saturated_dispatch = server
            .dispatch_global
            .clone()
            .acquire_many_owned(server.config.max_background_dispatches.try_into().unwrap())
            .await
            .unwrap();
        let mut dispatch_receiver = server.dispatch_receiver.lock().await.take().unwrap();
        let scheduler_server = server.clone();
        let scheduler = tokio::spawn(async move {
            let scheduled = dispatch_receiver
                .recv()
                .await
                .expect("live dispatch was not scheduled");
            let completion = scheduler_server
                .clone()
                .run_scheduled_dispatch(scheduled, CancellationToken::new())
                .await;
            scheduler_server
                .finish_scheduled_dispatch(completion)
                .unwrap();
        });

        let request_server = server.clone();
        let request = tokio::spawn(async move {
            request_server
                .handle_send(
                    send_rpc_request("live-running-recovery-race"),
                    owner_principal(),
                    false,
                    CancellationToken::new(),
                )
                .await
        });
        let dispatch = timeout(Duration::from_secs(1), async {
            loop {
                if let Some(dispatch) = server
                    .mapper_snapshot()
                    .await
                    .dispatch_for_message("live-running-recovery-race", &owner_principal())
                    .filter(|dispatch| dispatch.state == A2aDispatchOutboxState::Running)
                    .cloned()
                {
                    break dispatch;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("live dispatch did not atomically persist and commit its running fence");
        assert!(server
            .recovery_was_attempted(&dispatch.dispatch_id)
            .unwrap());
        assert_eq!(host.calls.load(Ordering::SeqCst), 0);

        let recovery_cancellation = CancellationToken::new();
        let mut recovery_cursor = None;
        let recovered = server
            .recover_pending_dispatches(
                &recovery_cancellation,
                Instant::now() + Duration::from_secs(1),
                &mut recovery_cursor,
            )
            .await;
        assert_eq!(recovered, 0);
        assert_eq!(host.reconcile_calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            server.mapper_snapshot().await.dispatch_outbox()[&dispatch.dispatch_id].state,
            A2aDispatchOutboxState::Running
        );

        drop(saturated_dispatch);
        let outcome = timeout(Duration::from_secs(2), request)
            .await
            .expect("live send did not finish")
            .unwrap()
            .unwrap();
        let ConnectionOutcome::Response(response) = outcome else {
            panic!("blocking live send unexpectedly streamed")
        };
        let payload: Value = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(
            payload["result"]["task"]["status"]["state"],
            "TASK_STATE_COMPLETED"
        );
        scheduler.await.unwrap();
        assert_eq!(host.calls.load(Ordering::SeqCst), 1);
        assert_eq!(host.reconcile_calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            server.mapper_snapshot().await.dispatch_outbox()[&dispatch.dispatch_id].state,
            A2aDispatchOutboxState::Settled
        );
        assert!(!server
            .recovery_was_attempted(&dispatch.dispatch_id)
            .unwrap());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn caller_abort_after_running_cas_apply_still_commits_host_once() {
        let principal = owner_principal();
        let message_id = "running-cas-abort-handoff";
        let mut seeded = A2aMapper::new();
        let mapping = prepare_seeded_message_for(&mut seeded, message_id, &principal);
        let event_id = event_id_for_message(&seeded, message_id, &principal);
        seeded.mark_event_settled(&event_id).unwrap();
        let record = seeded
            .dispatch_for_message(message_id, &principal)
            .unwrap()
            .clone();
        let (envelope, action) = seeded
            .reconstruct_dispatch(&record.dispatch_id, &principal)
            .unwrap();
        let snapshots = Arc::new(AppliedBlockingSnapshotStore::default());
        snapshots.initialize(&seeded).await.unwrap();
        let host = Arc::new(CompletingHost::default());
        let server = Arc::new(
            A2aHttpJsonRpcServer::new_owned(
                seeded,
                snapshots.clone(),
                Arc::new(InMemoryA2aEventStore::default()),
                Arc::new(TestAuthenticator),
                host.clone(),
                agent_card(),
                A2aHttpConfig::default(),
            )
            .unwrap(),
        );
        server.initialize_snapshot_store().await.unwrap();
        let recovery_claim = server
            .claim_recovery_attempt(&record.dispatch_id)
            .expect("test dispatch ownership was not available");
        let mut acceptance = server
            .reserve_dispatch(
                A2aDispatchJob {
                    durable_dispatch_id: Some(record.dispatch_id.clone()),
                    durable_cancellation_id: None,
                    lane: DispatchLane::Message,
                    mode: A2aExecutionMode::Blocking,
                    envelope,
                    action,
                },
                DispatchReservation {
                    owner: A2aEventOwner {
                        subject: record.owner_subject.clone(),
                        tenant_id: record.owner_tenant_id.clone(),
                    },
                    task_id: mapping.task_id.clone(),
                    message_id: record.message_id.clone(),
                    run_id: record.run_id.clone(),
                    lane: DispatchLane::Message,
                    delay_start: false,
                    allow_new: true,
                },
            )
            .unwrap()
            .expect("test dispatch was not reserved");
        assert!(acceptance.newly_reserved);
        let commit = acceptance
            .commit
            .take()
            .expect("new test reservation had no commit sender");
        let mut dispatch_receiver = server.dispatch_receiver.lock().await.take().unwrap();
        let scheduler_server = server.clone();
        let scheduler = tokio::spawn(async move {
            let scheduled = dispatch_receiver
                .recv()
                .await
                .expect("running handoff dispatch was not scheduled");
            let completion = scheduler_server
                .clone()
                .run_scheduled_dispatch(scheduled, CancellationToken::new())
                .await;
            scheduler_server
                .finish_scheduled_dispatch(completion)
                .unwrap();
        });

        snapshots
            .block_after_next_apply
            .store(true, Ordering::SeqCst);
        let marker_server = server.clone();
        let dispatch_id = record.dispatch_id.clone();
        let marker = tokio::spawn(async move {
            marker_server
                .mark_dispatch_running_with_commit(&dispatch_id, commit, recovery_claim)
                .await
        });
        timeout(Duration::from_secs(1), snapshots.entered.notified())
            .await
            .expect("running CAS was not durably applied");
        marker.abort();
        assert!(marker.await.unwrap_err().is_cancelled());
        snapshots.release.notify_one();

        wait_for_task_state(&server, &record.task_id, A2aTaskState::Completed).await;
        scheduler.await.unwrap();
        assert_eq!(host.reconcile_calls.load(Ordering::SeqCst), 0);
        assert_eq!(host.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            server.mapper_snapshot().await.dispatch_outbox()[&record.dispatch_id].state,
            A2aDispatchOutboxState::Settled
        );
        assert!(!server.recovery_was_attempted(&record.dispatch_id).unwrap());
    }

    #[tokio::test]
    async fn blocking_host_error_is_typed_and_preserves_the_accepted_task() {
        let (address, server, handle, task) =
            start_server_with_host(A2aHttpConfig::default(), Arc::new(FailingHost)).await;
        let response = raw_http(
            address,
            request(
                "POST",
                "/a2a",
                &rpc_headers("application/json"),
                &send_body("host-error", true, None),
            ),
        )
        .await;
        let value = body(&response);
        assert_eq!(value["error"]["code"], -32006);
        assert_eq!(
            value["error"]["data"][0]["reason"],
            "INVALID_AGENT_RESPONSE"
        );
        let task_id = value["error"]["data"][0]["metadata"]["taskId"]
            .as_str()
            .unwrap();
        assert_eq!(
            value["error"]["data"][0]["metadata"]["state"],
            "TASK_STATE_WORKING"
        );
        wait_for_scheduler_idle(&server).await;
        assert_eq!(
            server.mapper_snapshot().await.tasks()[task_id].state,
            A2aTaskState::Working
        );

        handle.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn panicking_host_is_sanitized_reconciled_and_releases_scheduler_capacity() {
        let panic_message_id = "panic-handle-isolation";
        let host = Arc::new(PanicIsolationHost::panicking_handle(panic_message_id));
        let (address, server, handle, task) =
            start_server_with_host(A2aHttpConfig::default(), host.clone()).await;

        let failed = raw_http(
            address,
            request(
                "POST",
                "/a2a",
                &rpc_headers("application/json"),
                &send_body(panic_message_id, true, None),
            ),
        )
        .await;
        let failed_body = body(&failed);
        assert_eq!(failed_body["error"]["code"], -32006);
        assert!(!failed.contains(PANIC_PAYLOAD));
        let task_id = failed_body["error"]["data"][0]["metadata"]["taskId"]
            .as_str()
            .unwrap()
            .to_owned();
        wait_for_scheduler_idle(&server).await;
        let failed_snapshot = server.mapper_snapshot().await;
        let failed_dispatch = failed_snapshot
            .dispatch_for_message(panic_message_id, &owner_principal())
            .unwrap();
        assert_eq!(
            failed_dispatch.state,
            A2aDispatchOutboxState::ReconcilePending
        );
        assert_eq!(
            failed_snapshot.tasks()[&task_id].state,
            A2aTaskState::Working
        );
        assert!(!failed_dispatch
            .last_error
            .as_deref()
            .unwrap_or_default()
            .contains(PANIC_PAYLOAD));
        assert!(!serde_json::to_string(&failed_snapshot)
            .unwrap()
            .contains(PANIC_PAYLOAD));
        {
            let state = server.dispatch_state.lock().unwrap();
            assert_eq!(state.accepted, 0);
            assert!(state.inflight_messages.is_empty());
        }

        let mut other_headers = rpc_headers("application/json");
        other_headers[0] = ("Authorization", "Bearer other");
        let healthy = raw_http(
            address,
            request(
                "POST",
                "/a2a",
                &other_headers,
                &send_body_for_tenant("healthy-after-handle-panic", "tenant-b", true, None),
            ),
        )
        .await;
        assert_eq!(
            body(&healthy)["result"]["task"]["status"]["state"],
            "TASK_STATE_COMPLETED"
        );
        assert!(!healthy.contains(PANIC_PAYLOAD));
        wait_for_scheduler_idle(&server).await;
        assert_eq!(host.handle_calls.load(Ordering::SeqCst), 2);
        assert_eq!(host.healthy_calls.load(Ordering::SeqCst), 1);

        handle.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn panicking_reconcile_hooks_fail_closed_without_blocking_listener_or_capacity() {
        let owner = owner_principal();
        let other = other_principal();
        let mut seeded = A2aMapper::new();

        let dispatch_message_id = "panic-dispatch-reconcile";
        let _dispatch_mapping =
            prepare_seeded_message_for(&mut seeded, dispatch_message_id, &owner);
        let dispatch_event_id = event_id_for_message(&seeded, dispatch_message_id, &owner);
        seeded.mark_event_settled(&dispatch_event_id).unwrap();
        let dispatch_id = seeded
            .dispatch_for_message(dispatch_message_id, &owner)
            .unwrap()
            .dispatch_id
            .clone();
        seeded.mark_dispatch_running(&dispatch_id).unwrap();

        let cancel_message_id = "panic-cancel-reconcile";
        let cancel_mapping = prepare_seeded_message_for(&mut seeded, cancel_message_id, &other);
        let cancel_dispatch_id = seeded
            .dispatch_for_message(cancel_message_id, &other)
            .unwrap()
            .dispatch_id
            .clone();
        seeded.mark_dispatch_settled(&cancel_dispatch_id).unwrap();
        let cancel_initial_event = event_id_for_message(&seeded, cancel_message_id, &other);
        seeded.mark_event_settled(&cancel_initial_event).unwrap();
        seeded
            .prepare_cancel_task(
                &cancel_mapping.task_id,
                CorrelationIdentity::new(
                    "panic-cancel-reconcile-correlation",
                    "panic-cancel-reconcile-request",
                )
                .unwrap(),
                Some(&other),
            )
            .into_authorized()
            .unwrap();
        let cancellation_id = seeded
            .cancellation_for_task(&cancel_mapping.task_id, &other)
            .unwrap()
            .cancellation_id
            .clone();
        seeded.mark_cancellation_running(&cancellation_id).unwrap();
        let cancel_event_ids: Vec<_> = seeded
            .pending_events()
            .into_iter()
            .filter(|event| event.task_id == cancel_mapping.task_id)
            .map(|event| event.event_id)
            .collect();
        for event_id in cancel_event_ids {
            seeded.mark_event_settled(&event_id).unwrap();
        }

        let snapshots = Arc::new(InMemoryA2aMapperSnapshotStore::default());
        snapshots.persist_snapshot(&seeded).await.unwrap();
        let restored = snapshots.load_snapshot().await.unwrap();
        let host = Arc::new(PanicIsolationHost::panicking_reconciliation());
        let (address, server, handle, task) = start_server_with_dependencies(
            A2aHttpConfig::default(),
            Arc::new(Mutex::new(restored)),
            snapshots,
            Arc::new(InMemoryA2aEventStore::default()),
            host.clone(),
            agent_card(),
        )
        .await;

        timeout(Duration::from_secs(2), async {
            loop {
                let snapshot = server.mapper_snapshot().await;
                let dispatch_reconcile = snapshot.dispatch_outbox()[&dispatch_id].state
                    == A2aDispatchOutboxState::ReconcilePending;
                let cancel_reconcile = snapshot
                    .cancellation_for_task(&cancel_mapping.task_id, &other)
                    .is_some_and(|record| {
                        record.state == A2aCancellationOutboxState::ReconcilePending
                    });
                if dispatch_reconcile && cancel_reconcile {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("panicking reconciliation hook did not fail closed");
        assert_eq!(host.dispatch_reconcile_calls.load(Ordering::SeqCst), 1);
        assert_eq!(host.cancel_reconcile_calls.load(Ordering::SeqCst), 1);
        assert_eq!(host.handle_calls.load(Ordering::SeqCst), 0);
        let reconciled = server.mapper_snapshot().await;
        assert!(!serde_json::to_string(&reconciled)
            .unwrap()
            .contains(PANIC_PAYLOAD));
        assert!(!reconciled.dispatch_outbox()[&dispatch_id]
            .last_error
            .as_deref()
            .unwrap_or_default()
            .contains(PANIC_PAYLOAD));
        assert!(!reconciled
            .cancellation_for_task(&cancel_mapping.task_id, &other)
            .unwrap()
            .last_error
            .as_deref()
            .unwrap_or_default()
            .contains(PANIC_PAYLOAD));
        {
            let state = server.dispatch_state.lock().unwrap();
            assert_eq!(state.accepted, 0);
            assert!(state.inflight_messages.is_empty());
        }

        let mut other_headers = rpc_headers("application/json");
        other_headers[0] = ("Authorization", "Bearer other");
        let healthy = raw_http(
            address,
            request(
                "POST",
                "/a2a",
                &other_headers,
                &send_body_for_tenant("healthy-after-reconcile-panics", "tenant-b", true, None),
            ),
        )
        .await;
        assert_eq!(
            body(&healthy)["result"]["task"]["status"]["state"],
            "TASK_STATE_COMPLETED"
        );
        assert!(!healthy.contains(PANIC_PAYLOAD));
        wait_for_scheduler_idle(&server).await;
        assert_eq!(host.handle_calls.load(Ordering::SeqCst), 1);
        assert_eq!(host.healthy_calls.load(Ordering::SeqCst), 1);

        handle.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn ready_host_future_drop_panic_reconciles_once_and_keeps_listener_alive() {
        let drop_count = Arc::new(AtomicUsize::new(0));
        let host = Arc::new(DropPanickingHost {
            mode: DropPanicFutureMode::Ready,
            drop_count: drop_count.clone(),
            started: Arc::new(Notify::new()),
        });
        let (address, server, handle, task) =
            start_server_with_host(A2aHttpConfig::default(), host).await;
        let response = raw_http(
            address,
            request(
                "POST",
                "/a2a",
                &rpc_headers("application/json"),
                &send_body("ready-drop-panic", false, None),
            ),
        )
        .await;
        assert_eq!(body(&response)["error"]["code"], -32006);
        assert!(!response.contains(PANIC_PAYLOAD));
        wait_for_scheduler_idle(&server).await;
        let snapshot = server.mapper_snapshot().await;
        let dispatch = snapshot
            .dispatch_for_message("ready-drop-panic", &owner_principal())
            .unwrap();
        assert_eq!(dispatch.state, A2aDispatchOutboxState::ReconcilePending);
        assert_eq!(drop_count.load(Ordering::SeqCst), 1);
        assert!(!serde_json::to_string(&snapshot)
            .unwrap()
            .contains(PANIC_PAYLOAD));

        let card = raw_http(address, request("GET", A2A_AGENT_CARD_PATH, &[], "")).await;
        assert!(card.starts_with("HTTP/1.1 200"));
        handle.cancel();
        task.await.unwrap().unwrap();
        assert_eq!(drop_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn pending_host_future_drop_panic_is_caught_during_server_shutdown_once() {
        let drop_count = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(Notify::new());
        let host = Arc::new(DropPanickingHost {
            mode: DropPanicFutureMode::Pending,
            drop_count: drop_count.clone(),
            started: started.clone(),
        });
        let config = A2aHttpConfig {
            dispatch_ack_timeout: Duration::from_millis(30),
            graceful_shutdown_timeout: Duration::from_millis(500),
            ..A2aHttpConfig::default()
        };
        let (address, server, handle, task) = start_server_with_host(config, host).await;
        let response = raw_http(
            address,
            request(
                "POST",
                "/a2a",
                &rpc_headers("application/json"),
                &send_body("pending-drop-panic", false, Some(true)),
            ),
        )
        .await;
        assert_eq!(
            body(&response)["result"]["task"]["status"]["state"],
            "TASK_STATE_WORKING"
        );
        timeout(Duration::from_secs(1), started.notified())
            .await
            .expect("pending host future was not polled");

        handle.cancel();
        timeout(Duration::from_secs(1), task)
            .await
            .expect("server shutdown propagated or hung on the Drop panic")
            .unwrap()
            .unwrap();
        assert_eq!(drop_count.load(Ordering::SeqCst), 1);
        let snapshot = server.mapper_snapshot().await;
        let dispatch = snapshot
            .dispatch_for_message("pending-drop-panic", &owner_principal())
            .unwrap();
        assert_eq!(dispatch.state, A2aDispatchOutboxState::ReconcilePending);
        assert!(!serde_json::to_string(&snapshot)
            .unwrap()
            .contains(PANIC_PAYLOAD));
        let state = server.dispatch_state.lock().unwrap();
        assert_eq!(state.accepted, 0);
        assert!(state.inflight_messages.is_empty());
    }

    #[test]
    fn untrusted_callback_panic_payload_is_redacted_in_subprocess_stderr() {
        for test_name in [
            "protocols::a2a_transport::tests::panicking_host_is_sanitized_reconciled_and_releases_scheduler_capacity",
            "protocols::a2a_transport::tests::ready_host_future_drop_panic_reconciles_once_and_keeps_listener_alive",
            "protocols::a2a_transport::tests::pending_host_future_drop_panic_is_caught_during_server_shutdown_once",
        ] {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .arg(test_name)
                .arg("--exact")
                .arg("--nocapture")
                .output()
                .unwrap();
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert!(
                output.status.success(),
                "panic-isolation subprocess failed for {test_name}:\nstdout:\n{stdout}\nstderr:\n{stderr}"
            );
            assert!(!stdout.contains(PANIC_PAYLOAD));
            assert!(!stderr.contains(PANIC_PAYLOAD));
            assert!(stderr.contains("AIKit suppressed an untrusted A2A callback panic"));
        }
    }

    #[tokio::test]
    async fn exact_duplicate_observes_one_inflight_host_dispatch() {
        let host = Arc::new(DelayedHost::new(Duration::from_millis(160)));
        let (address, server, handle, task) =
            start_server_with_host(A2aHttpConfig::default(), host.clone()).await;
        let payload = send_body("single-flight", true, Some(true));
        let first = raw_http(
            address,
            request("POST", "/a2a", &rpc_headers("application/json"), &payload),
        )
        .await;
        timeout(Duration::from_secs(1), host.started.notified())
            .await
            .expect("first dispatch did not start");
        let duplicate = raw_http(
            address,
            request("POST", "/a2a", &rpc_headers("application/json"), &payload),
        )
        .await;
        assert_eq!(
            body(&first)["result"]["task"]["id"],
            body(&duplicate)["result"]["task"]["id"]
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(host.modes.lock().unwrap().len(), 1);
        timeout(Duration::from_secs(1), host.finished.notified())
            .await
            .expect("single host dispatch did not finish");
        wait_for_scheduler_idle(&server).await;
        assert_eq!(host.modes.lock().unwrap().len(), 1);

        handle.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn flood_is_bounded_by_global_queue_and_per_owner_capacity() {
        let global_config = A2aHttpConfig {
            max_background_dispatches: 1,
            max_background_dispatches_per_owner: 1,
            max_queued_dispatches: 1,
            max_queued_dispatches_per_owner: 1,
            background_dispatch_timeout: Duration::from_secs(10),
            dispatch_ack_timeout: Duration::from_millis(50),
            graceful_shutdown_timeout: Duration::from_millis(500),
            ..A2aHttpConfig::default()
        };
        let global_host = Arc::new(DelayedHost::new(Duration::from_secs(10)));
        let (address, _server, handle, task) =
            start_server_with_host(global_config, global_host.clone()).await;

        let first = raw_http(
            address,
            request(
                "POST",
                "/a2a",
                &rpc_headers("application/json"),
                &send_body("global-running", true, Some(true)),
            ),
        )
        .await;
        assert_eq!(
            body(&first)["result"]["task"]["status"]["state"],
            "TASK_STATE_WORKING"
        );
        timeout(Duration::from_secs(1), global_host.started.notified())
            .await
            .expect("global running dispatch did not start");
        let mut other_headers = rpc_headers("application/json");
        other_headers[0] = ("Authorization", "Bearer other");
        let queued = raw_http(
            address,
            request(
                "POST",
                "/a2a",
                &other_headers,
                &send_body_for_tenant("global-queued", "tenant-b", true, Some(true)),
            ),
        )
        .await;
        assert_eq!(
            body(&queued)["result"]["task"]["status"]["state"],
            "TASK_STATE_WORKING"
        );
        let rejected = raw_http(
            address,
            request(
                "POST",
                "/a2a",
                &rpc_headers("application/json"),
                &send_body("global-overflow", true, Some(true)),
            ),
        )
        .await;
        assert!(rejected.starts_with("HTTP/1.1 503"));
        assert_eq!(body(&rejected)["error"]["code"], -32603);
        assert_eq!(global_host.modes.lock().unwrap().len(), 1);
        handle.cancel();
        task.await.unwrap().unwrap();

        let owner_config = A2aHttpConfig {
            max_background_dispatches: 2,
            max_background_dispatches_per_owner: 1,
            max_queued_dispatches: 4,
            max_queued_dispatches_per_owner: 1,
            background_dispatch_timeout: Duration::from_secs(10),
            dispatch_ack_timeout: Duration::from_millis(50),
            graceful_shutdown_timeout: Duration::from_millis(500),
            ..A2aHttpConfig::default()
        };
        let owner_host = Arc::new(DelayedHost::new(Duration::from_secs(10)));
        let (address, _server, handle, task) =
            start_server_with_host(owner_config, owner_host.clone()).await;
        let _ = raw_http(
            address,
            request(
                "POST",
                "/a2a",
                &rpc_headers("application/json"),
                &send_body("owner-running", true, Some(true)),
            ),
        )
        .await;
        timeout(Duration::from_secs(1), owner_host.started.notified())
            .await
            .expect("owner running dispatch did not start");
        let _ = raw_http(
            address,
            request(
                "POST",
                "/a2a",
                &rpc_headers("application/json"),
                &send_body("owner-queued", true, Some(true)),
            ),
        )
        .await;
        let owner_rejected = raw_http(
            address,
            request(
                "POST",
                "/a2a",
                &rpc_headers("application/json"),
                &send_body("owner-overflow", true, Some(true)),
            ),
        )
        .await;
        assert!(owner_rejected.starts_with("HTTP/1.1 503"));
        let other_accepted = raw_http(
            address,
            request(
                "POST",
                "/a2a",
                &other_headers,
                &send_body_for_tenant("other-running", "tenant-b", true, Some(true)),
            ),
        )
        .await;
        assert_eq!(
            body(&other_accepted)["result"]["task"]["status"]["state"],
            "TASK_STATE_WORKING"
        );
        timeout(Duration::from_secs(1), owner_host.started.notified())
            .await
            .expect("second owner should have independent running capacity");
        assert_eq!(owner_host.modes.lock().unwrap().len(), 2);
        handle.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn cooperative_stop_ack_terminalizes_a_timed_out_dispatch() {
        let config = A2aHttpConfig {
            background_dispatch_timeout: Duration::from_millis(40),
            dispatch_ack_timeout: Duration::from_millis(100),
            graceful_shutdown_timeout: Duration::from_millis(500),
            ..A2aHttpConfig::default()
        };
        let host = Arc::new(DelayedHost::new(Duration::from_secs(5)));
        let (address, server, handle, task) = start_server_with_host(config, host.clone()).await;
        let response = raw_http(
            address,
            request(
                "POST",
                "/a2a",
                &rpc_headers("application/json"),
                &send_body("cooperative-stop", true, Some(true)),
            ),
        )
        .await;
        let task_id = body(&response)["result"]["task"]["id"]
            .as_str()
            .unwrap()
            .to_owned();
        timeout(Duration::from_secs(1), host.started.notified())
            .await
            .expect("cooperative host did not start");
        wait_for_scheduler_idle(&server).await;
        assert_eq!(
            server.mapper_snapshot().await.tasks()[&task_id].state,
            A2aTaskState::Cancelled
        );

        handle.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn uncooperative_timeout_preserves_nonterminal_task_for_reconciliation() {
        let config = A2aHttpConfig {
            background_dispatch_timeout: Duration::from_millis(30),
            dispatch_ack_timeout: Duration::from_millis(30),
            graceful_shutdown_timeout: Duration::from_millis(300),
            ..A2aHttpConfig::default()
        };
        let host = Arc::new(UncooperativeHost::new(Duration::from_secs(5)));
        let (address, server, handle, task) = start_server_with_host(config, host.clone()).await;
        let response = raw_http(
            address,
            request(
                "POST",
                "/a2a",
                &rpc_headers("application/json"),
                &send_body("uncooperative-timeout", true, Some(true)),
            ),
        )
        .await;
        let task_id = body(&response)["result"]["task"]["id"]
            .as_str()
            .unwrap()
            .to_owned();
        timeout(Duration::from_secs(1), host.started.notified())
            .await
            .expect("uncooperative host did not start");
        wait_for_scheduler_idle(&server).await;
        assert_eq!(host.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            server.mapper_snapshot().await.tasks()[&task_id].state,
            A2aTaskState::Working
        );

        handle.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn shutdown_drops_unacknowledged_host_but_preserves_nonterminal_task() {
        let config = A2aHttpConfig {
            background_dispatch_timeout: Duration::from_secs(10),
            dispatch_ack_timeout: Duration::from_millis(30),
            graceful_shutdown_timeout: Duration::from_millis(300),
            ..A2aHttpConfig::default()
        };
        let host = Arc::new(UncooperativeHost::new(Duration::from_secs(5)));
        let (address, server, handle, task) = start_server_with_host(config, host.clone()).await;
        let response = raw_http(
            address,
            request(
                "POST",
                "/a2a",
                &rpc_headers("application/json"),
                &send_body("uncooperative-shutdown", true, Some(true)),
            ),
        )
        .await;
        let task_id = body(&response)["result"]["task"]["id"]
            .as_str()
            .unwrap()
            .to_owned();
        timeout(Duration::from_secs(1), host.started.notified())
            .await
            .expect("uncooperative host did not start");
        let shutdown_started = Instant::now();
        handle.cancel();
        timeout(Duration::from_secs(1), task)
            .await
            .expect("server did not bound shutdown")
            .unwrap()
            .unwrap();
        assert!(shutdown_started.elapsed() < Duration::from_secs(1));
        assert_eq!(host.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            server.mapper_snapshot().await.tasks()[&task_id].state,
            A2aTaskState::Working
        );
    }

    #[tokio::test]
    async fn authenticated_surface_projects_official_tasks_without_internal_fields() {
        let (address, _server, handle, task) = start_server(A2aHttpConfig::default()).await;
        let card = raw_http(address, request("GET", A2A_AGENT_CARD_PATH, &[], "")).await;
        assert!(card.starts_with("HTTP/1.1 200"));
        assert_eq!(body(&card)["capabilities"]["streaming"], true);
        assert_eq!(
            body(&card)["supportedInterfaces"][0]["protocolBinding"],
            "JSONRPC"
        );

        let sent = raw_http(
            address,
            request(
                "POST",
                "/a2a",
                &rpc_headers("*/*"),
                &send_body("message-1", true, None),
            ),
        )
        .await;
        let sent_json = body(&sent);
        let task_id = sent_json["result"]["task"]["id"]
            .as_str()
            .unwrap()
            .to_owned();
        assert_eq!(
            sent_json["result"]["task"]["status"]["state"],
            "TASK_STATE_COMPLETED"
        );
        for leaked in [
            "ownerSubject",
            "owner_subject",
            "sessionId",
            "runId",
            "createdRevision",
            "updatedRevision",
        ] {
            assert!(!sent.contains(leaked), "wire response leaked {leaked}");
        }

        let get = json!({
            "jsonrpc":"2.0","id":2,"method":"GetTask",
            "params":{"tenant":"tenant-a","id":task_id}
        })
        .to_string();
        let got = raw_http(
            address,
            request("POST", "/a2a", &rpc_headers("application/json"), &get),
        )
        .await;
        assert_eq!(body(&got)["result"]["id"], task_id);

        let list = json!({
            "jsonrpc":"2.0","id":3,"method":"ListTasks",
            "params":{"tenant":"tenant-a","page_size":1}
        })
        .to_string();
        let listed = raw_http(
            address,
            request("POST", "/a2a", &rpc_headers("application/json"), &list),
        )
        .await;
        assert_eq!(body(&listed)["result"]["totalSize"], 1);
        assert_eq!(body(&listed)["result"]["nextPageToken"], "");

        let inaccessible = json!({
            "jsonrpc":"2.0","id":4,"method":"GetTask",
            "params":{"tenant":"tenant-b","id":task_id}
        })
        .to_string();
        let mut other_headers = rpc_headers("application/json");
        other_headers[0] = ("Authorization", "Bearer other");
        let hidden = raw_http(
            address,
            request("POST", "/a2a", &other_headers, &inaccessible),
        )
        .await;
        assert_eq!(body(&hidden)["error"]["code"], -32001);

        handle.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn durable_artifacts_and_direct_message_share_send_get_list_and_restart_projection() {
        let snapshots = Arc::new(InMemoryA2aMapperSnapshotStore::default());
        let host = Arc::new(DurableOutputHost::default());
        let (address, _server, handle, task) = start_server_with_dependencies(
            A2aHttpConfig::default(),
            Arc::new(Mutex::new(A2aMapper::new())),
            snapshots.clone(),
            Arc::new(InMemoryA2aEventStore::default()),
            host.clone(),
            agent_card(),
        )
        .await;

        let artifact_payload = send_body("durable-artifacts", true, None);
        let artifact_response = raw_http(
            address,
            request(
                "POST",
                "/a2a",
                &rpc_headers("application/json"),
                &artifact_payload,
            ),
        )
        .await;
        let artifact_result = &body(&artifact_response)["result"]["task"];
        let task_id = artifact_result["id"].as_str().unwrap().to_owned();
        let context_id = artifact_result["contextId"].as_str().unwrap().to_owned();
        let artifacts = artifact_result["artifacts"].as_array().unwrap();
        assert_eq!(artifacts.len(), 4);
        assert_eq!(artifacts[0]["artifactId"], "artifact-text");
        assert_eq!(artifacts[0]["parts"][0]["text"], "Generated text content");
        assert_eq!(artifacts[1]["parts"][0]["raw"], "dGNr");
        assert_eq!(artifacts[1]["parts"][0]["filename"], "output.txt");
        assert_eq!(artifacts[1]["parts"][0]["mediaType"], "text/plain");
        assert_eq!(
            artifacts[2]["parts"][0]["url"],
            "https://example.com/output.txt"
        );
        assert_eq!(artifacts[2]["parts"][0]["filename"], "output.txt");
        assert_eq!(
            artifacts[3]["parts"][0]["data"],
            json!({"key":"value","count":42})
        );

        let get = json!({
            "jsonrpc":"2.0","id":"artifact-get","method":"GetTask",
            "params":{"tenant":"tenant-a","id":task_id}
        })
        .to_string();
        let got = raw_http(
            address,
            request("POST", "/a2a", &rpc_headers("application/json"), &get),
        )
        .await;
        assert_eq!(
            body(&got)["result"]["artifacts"],
            artifact_result["artifacts"]
        );

        let list_without = json!({
            "jsonrpc":"2.0","id":"artifact-list-without","method":"ListTasks",
            "params":{"tenant":"tenant-a","contextId":context_id,"includeArtifacts":false}
        })
        .to_string();
        let listed_without = raw_http(
            address,
            request(
                "POST",
                "/a2a",
                &rpc_headers("application/json"),
                &list_without,
            ),
        )
        .await;
        assert!(body(&listed_without)["result"]["tasks"][0]
            .as_object()
            .unwrap()
            .get("artifacts")
            .is_none());
        let list_with = json!({
            "jsonrpc":"2.0","id":"artifact-list-with","method":"ListTasks",
            "params":{"tenant":"tenant-a","contextId":context_id,"includeArtifacts":true}
        })
        .to_string();
        let listed_with = raw_http(
            address,
            request("POST", "/a2a", &rpc_headers("application/json"), &list_with),
        )
        .await;
        assert_eq!(
            body(&listed_with)["result"]["tasks"][0]["artifacts"],
            artifact_result["artifacts"]
        );

        let direct_payload = send_body("durable-direct-response", true, None);
        let direct = raw_http(
            address,
            request(
                "POST",
                "/a2a",
                &rpc_headers("application/json"),
                &direct_payload,
            ),
        )
        .await;
        let direct_result = &body(&direct)["result"];
        assert!(direct_result.get("task").is_none());
        assert_eq!(direct_result["message"]["role"], "ROLE_AGENT");
        assert_eq!(
            direct_result["message"]["parts"][0]["text"],
            "Direct message response"
        );
        assert!(direct_result["message"].get("taskId").is_none());
        assert_eq!(host.calls.load(Ordering::SeqCst), 2);
        let mut changed_policy: Value = serde_json::from_str(&direct_payload).unwrap();
        changed_policy["id"] = json!("durable-direct-response-policy-conflict");
        changed_policy["params"]["configuration"] = json!({"returnImmediately": true});
        let policy_conflict = raw_http(
            address,
            request(
                "POST",
                "/a2a",
                &rpc_headers("application/json"),
                &changed_policy.to_string(),
            ),
        )
        .await;
        assert_eq!(body(&policy_conflict)["error"]["code"], -32602);
        assert_eq!(host.calls.load(Ordering::SeqCst), 2);

        handle.cancel();
        task.await.unwrap().unwrap();
        let restored = snapshots.load_snapshot().await.unwrap();
        let (restart_address, _restart_server, restart_handle, restart_task) =
            start_server_with_dependencies(
                A2aHttpConfig::default(),
                Arc::new(Mutex::new(restored)),
                snapshots,
                Arc::new(InMemoryA2aEventStore::default()),
                host.clone(),
                agent_card(),
            )
            .await;
        let retried = raw_http(
            restart_address,
            request(
                "POST",
                "/a2a",
                &rpc_headers("application/json"),
                &direct_payload,
            ),
        )
        .await;
        let retried_result = &body(&retried)["result"];
        assert!(retried_result.get("task").is_none());
        assert_eq!(
            retried_result["message"]["parts"][0]["text"],
            "Direct message response"
        );
        assert_eq!(host.calls.load(Ordering::SeqCst), 2);

        restart_handle.cancel();
        restart_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn streaming_direct_response_emits_one_terminal_message_without_terminal_task_status() {
        let host = Arc::new(DurableOutputHost::default());
        let (address, _server, handle, task) =
            start_server_with_host(A2aHttpConfig::default(), host.clone()).await;
        let mut payload: Value =
            serde_json::from_str(&send_body("durable-direct-stream", true, None)).unwrap();
        payload["method"] = json!("SendStreamingMessage");
        let response = raw_http(
            address,
            request(
                "POST",
                "/a2a",
                &rpc_headers("text/event-stream"),
                &payload.to_string(),
            ),
        )
        .await;
        let events = response
            .lines()
            .filter_map(|line| line.strip_prefix("data: "))
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        let messages = events
            .iter()
            .filter(|event| event["result"]["message"].is_object())
            .collect::<Vec<_>>();
        assert_eq!(events.len(), 1);
        assert_eq!(messages.len(), 1);
        assert_eq!(
            messages[0]["result"]["message"]["parts"][0]["text"],
            "Direct message response"
        );
        assert!(events.iter().all(|event| {
            event["result"]["task"]["status"]["state"] != "TASK_STATE_COMPLETED"
                && !event["result"]["statusUpdate"].is_object()
        }));
        assert_eq!(host.calls.load(Ordering::SeqCst), 1);

        let retried = raw_http(
            address,
            request(
                "POST",
                "/a2a",
                &rpc_headers("text/event-stream"),
                &payload.to_string(),
            ),
        )
        .await;
        let retried_events = retried
            .lines()
            .filter_map(|line| line.strip_prefix("data: "))
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(retried_events.len(), 1);
        assert!(retried_events[0]["result"]["message"].is_object());
        assert_eq!(host.calls.load(Ordering::SeqCst), 1);

        handle.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn immediate_direct_response_keeps_first_task_oneof_across_completion_and_restart() {
        let snapshots = Arc::new(InMemoryA2aMapperSnapshotStore::default());
        let host = Arc::new(DurableOutputHost::default());
        let (address, server, handle, task) = start_server_with_dependencies(
            A2aHttpConfig::default(),
            Arc::new(Mutex::new(A2aMapper::new())),
            snapshots.clone(),
            Arc::new(InMemoryA2aEventStore::default()),
            host.clone(),
            agent_card(),
        )
        .await;
        let payload = send_body("durable-direct-immediate", true, Some(true));
        let first = raw_http(
            address,
            request("POST", "/a2a", &rpc_headers("application/json"), &payload),
        )
        .await;
        let first_result = body(&first)["result"].clone();
        assert!(first_result["message"].is_null());
        assert_eq!(
            first_result["task"]["status"]["state"],
            "TASK_STATE_WORKING"
        );
        let task_id = first_result["task"]["id"].as_str().unwrap();
        wait_for_task_state(&server, task_id, A2aTaskState::Completed).await;
        wait_for_scheduler_idle(&server).await;

        let duplicate = raw_http(
            address,
            request("POST", "/a2a", &rpc_headers("application/json"), &payload),
        )
        .await;
        assert_eq!(body(&duplicate)["result"], first_result);
        assert_eq!(host.calls.load(Ordering::SeqCst), 1);

        let mut streaming_retry: Value = serde_json::from_str(&payload).unwrap();
        streaming_retry["id"] = json!("durable-direct-immediate-stream-conflict");
        streaming_retry["method"] = json!("SendStreamingMessage");
        let policy_conflict = raw_http(
            address,
            request(
                "POST",
                "/a2a",
                &rpc_headers("text/event-stream"),
                &streaming_retry.to_string(),
            ),
        )
        .await;
        assert_eq!(body(&policy_conflict)["error"]["code"], -32602);
        assert_eq!(host.calls.load(Ordering::SeqCst), 1);

        handle.cancel();
        task.await.unwrap().unwrap();
        let restored = snapshots.load_snapshot().await.unwrap();
        let (restart_address, _restart_server, restart_handle, restart_task) =
            start_server_with_dependencies(
                A2aHttpConfig::default(),
                Arc::new(Mutex::new(restored)),
                snapshots,
                Arc::new(InMemoryA2aEventStore::default()),
                host.clone(),
                agent_card(),
            )
            .await;
        let restarted = raw_http(
            restart_address,
            request("POST", "/a2a", &rpc_headers("application/json"), &payload),
        )
        .await;
        assert_eq!(body(&restarted)["result"], first_result);
        assert_eq!(host.calls.load(Ordering::SeqCst), 1);
        restart_handle.cancel();
        restart_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn sse_is_enveloped_terminal_replayable_and_reports_retention_gaps() {
        let config = A2aHttpConfig {
            max_retained_events: 2,
            max_retained_event_bytes: 2 * DEFAULT_A2A_EVENT_BYTES,
            ..A2aHttpConfig::default()
        };
        let (address, _server, handle, task) = start_server(config).await;

        let mut streaming: Value =
            serde_json::from_str(&send_body("stream-1", true, None)).unwrap();
        streaming["method"] = json!("SendStreamingMessage");
        let streamed = raw_http(
            address,
            request(
                "POST",
                "/a2a",
                &rpc_headers("text/event-stream"),
                &streaming.to_string(),
            ),
        )
        .await;
        assert!(streamed.starts_with("HTTP/1.1 200"));
        let data: Vec<Value> = streamed
            .lines()
            .filter_map(|line| line.strip_prefix("data: "))
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert!(data.len() >= 2);
        assert!(data[0]["result"]["task"].is_object());
        assert_eq!(
            data.last().unwrap()["result"]["statusUpdate"]["status"]["state"],
            "TASK_STATE_COMPLETED"
        );
        assert!(!streamed.contains("\"final\""));

        let gap_config = A2aHttpConfig {
            max_retained_events: 2,
            max_retained_event_bytes: 2 * DEFAULT_A2A_EVENT_BYTES,
            ..A2aHttpConfig::default()
        };
        let gap_host = Arc::new(ExactStateTransitionHost::new(
            A2aTaskState::InputRequired,
            "input",
        ));
        let (gap_address, gap_server, gap_handle, gap_task) =
            start_server_with_host(gap_config, gap_host).await;
        let working = raw_http(
            gap_address,
            request(
                "POST",
                "/a2a",
                &rpc_headers("application/json"),
                &send_body("gap-1", false, Some(true)),
            ),
        )
        .await;
        let task_id = body(&working)["result"]["task"]["id"]
            .as_str()
            .unwrap()
            .to_owned();
        wait_for_task_state(&gap_server, &task_id, A2aTaskState::InputRequired).await;
        wait_for_scheduler_idle(&gap_server).await;
        gap_server
            .persist_mapper_mutation(|candidate| {
                candidate.transition_task(&task_id, A2aTaskState::Working, None)
            })
            .await
            .unwrap();
        gap_server
            .flush_pending_events_for_task(&task_id)
            .await
            .unwrap();
        gap_server
            .persist_mapper_mutation(|candidate| {
                candidate.transition_task(
                    &task_id,
                    A2aTaskState::InputRequired,
                    Some("again".into()),
                )
            })
            .await
            .unwrap();
        gap_server
            .flush_pending_events_for_task(&task_id)
            .await
            .unwrap();
        let subscribe = json!({
            "jsonrpc":"2.0","id":"resume","method":"SubscribeToTask",
            "params":{"tenant":"tenant-a","id":task_id}
        })
        .to_string();
        let mut headers = rpc_headers("text/event-stream");
        headers.push(("Last-Event-ID", "1"));
        let gap = raw_http(gap_address, request("POST", "/a2a", &headers, &subscribe)).await;
        assert_eq!(body(&gap)["error"]["code"], -32004);
        assert!(body(&gap)["error"]["data"][0]["@type"]
            .as_str()
            .unwrap()
            .ends_with("google.rpc.ErrorInfo"));
        assert_eq!(
            body(&gap)["error"]["data"][0]["reason"],
            "UNSUPPORTED_OPERATION"
        );

        handle.cancel();
        task.await.unwrap().unwrap();
        gap_handle.cancel();
        gap_task.await.unwrap().unwrap();
    }

    #[test]
    fn quota_leases_are_bounded_released_and_closed() {
        let ip: IpAddr = "127.0.0.1".parse().unwrap();
        let preauth = Arc::new(PreAuthLimiter::new(1));
        let first = preauth.try_acquire(ip).unwrap().unwrap();
        assert!(preauth.try_acquire(ip).unwrap().is_none());
        drop(first);
        let reacquired = preauth.try_acquire(ip).unwrap().unwrap();
        drop(reacquired);
        preauth.close();
        assert!(preauth.try_acquire(ip).unwrap().is_none());

        let owner_a = A2aEventOwner {
            subject: "owner-a".into(),
            tenant_id: Some("tenant-a".into()),
        };
        let owner_b = A2aEventOwner {
            subject: "owner-b".into(),
            tenant_id: Some("tenant-b".into()),
        };
        let owner_c = A2aEventOwner {
            subject: "owner-c".into(),
            tenant_id: Some("tenant-c".into()),
        };
        let streams = Arc::new(StreamQuota::new(2, 1));
        let stream_a = streams.try_acquire(owner_a.clone()).unwrap().unwrap();
        assert!(streams.try_acquire(owner_a).unwrap().is_none());
        let stream_b = streams.try_acquire(owner_b).unwrap().unwrap();
        assert!(streams.try_acquire(owner_c.clone()).unwrap().is_none());
        drop(stream_a);
        let stream_c = streams.try_acquire(owner_c).unwrap().unwrap();
        drop(stream_b);
        drop(stream_c);
        streams.close();
        assert!(streams
            .try_acquire(A2aEventOwner {
                subject: "owner-d".into(),
                tenant_id: None,
            })
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn one_slot_control_queue_reserves_cross_owner_reachability() {
        let ip: IpAddr = "127.0.0.1".parse().unwrap();
        let owner_a = A2aEventOwner {
            subject: "queue-owner-a".into(),
            tenant_id: Some("tenant-a".into()),
        };
        let owner_b = A2aEventOwner {
            subject: "queue-owner-b".into(),
            tenant_id: Some("tenant-b".into()),
        };
        let limiter = Arc::new(ControlProbeLimiter::new(ControlProbeLimits {
            max_active: 1,
            max_per_ip: 1,
            max_per_owner: 1,
            max_per_minute: 100,
            max_rate_buckets: 4,
            max_waiters: 1,
            max_waiters_per_ip: 1,
            max_waiters_per_owner: 1,
        }));
        let active_a = limiter
            .acquire_exact_classification_until(
                ip,
                owner_a.clone(),
                Instant::now() + Duration::from_secs(2),
            )
            .await
            .unwrap()
            .unwrap();
        let queued_a = {
            let limiter = limiter.clone();
            let owner = owner_a.clone();
            tokio::spawn(async move {
                limiter
                    .acquire_exact_classification_until(
                        ip,
                        owner,
                        Instant::now() + Duration::from_secs(2),
                    )
                    .await
                    .unwrap()
            })
        };
        timeout(Duration::from_secs(1), async {
            while limiter
                .state
                .lock()
                .unwrap()
                .waiters_per_owner
                .get(&owner_a)
                != Some(&1)
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        let queued_b = {
            let limiter = limiter.clone();
            let owner = owner_b.clone();
            tokio::spawn(async move {
                limiter
                    .acquire_exact_classification_until(
                        ip,
                        owner,
                        Instant::now() + Duration::from_secs(2),
                    )
                    .await
                    .unwrap()
            })
        };
        timeout(Duration::from_secs(1), async {
            while limiter
                .state
                .lock()
                .unwrap()
                .waiters_per_owner
                .get(&owner_b)
                != Some(&1)
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(timeout(Duration::from_secs(1), queued_a)
            .await
            .unwrap()
            .unwrap()
            .is_none());
        drop(active_a);
        let admitted_b = timeout(Duration::from_secs(1), queued_b)
            .await
            .unwrap()
            .unwrap()
            .expect("owner B did not inherit the reserved cross-owner waiter");
        drop(admitted_b);
        limiter.close();
    }

    #[tokio::test]
    async fn standard_control_probe_minute_cap_is_a_hard_stop() {
        let ip: IpAddr = "127.0.0.1".parse().unwrap();
        let owner = A2aEventOwner {
            subject: "rate-owner".into(),
            tenant_id: Some("rate-tenant".into()),
        };
        for _ in 0..3 {
            let limiter = Arc::new(ControlProbeLimiter::new(ControlProbeLimits {
                max_active: 1,
                max_per_ip: 1,
                max_per_owner: 1,
                max_per_minute: 1,
                max_rate_buckets: 4,
                max_waiters: 1,
                max_waiters_per_ip: 1,
                max_waiters_per_owner: 1,
            }));
            let minute = current_minute();
            let first = limiter
                .acquire_probe_until(
                    ip,
                    owner.clone(),
                    tokio::time::Instant::now() + Duration::from_secs(1),
                )
                .await
                .unwrap()
                .expect("first standard probe should be admitted");
            drop(first);
            let second = limiter
                .acquire_probe_until(
                    ip,
                    owner.clone(),
                    tokio::time::Instant::now() + Duration::from_secs(1),
                )
                .await
                .unwrap();
            if current_minute() == minute {
                assert!(second.is_none(), "same-minute standard cap was bypassed");
                return;
            }
        }
        panic!("clock crossed a minute boundary during every hard-cap attempt");
    }

    #[test]
    fn quota_configuration_rejects_invalid_limits() {
        let config = A2aHttpConfig {
            max_preauth_per_ip: 0,
            ..A2aHttpConfig::default()
        };
        assert!(config.validate().is_err());

        let defaults = A2aHttpConfig::default();
        let config = A2aHttpConfig {
            max_streams_per_owner: defaults.max_streams + 1,
            ..defaults
        };
        assert!(config.validate().is_err());

        let defaults = A2aHttpConfig::default();
        let config = A2aHttpConfig {
            handshake_timeout: defaults.request_timeout,
            ..defaults
        };
        assert!(config.validate().is_err());
    }

    #[tokio::test]
    async fn incomplete_http_handshake_is_closed_by_the_short_deadline() {
        let config = A2aHttpConfig {
            handshake_timeout: Duration::from_millis(30),
            ..A2aHttpConfig::default()
        };
        let (address, _server, handle, task) = start_server(config).await;

        let mut stream = TcpStream::connect(address).await.unwrap();
        let mut response = Vec::new();
        timeout(Duration::from_secs(1), stream.read_to_end(&mut response))
            .await
            .expect("incomplete handshake was not closed")
            .unwrap();
        assert!(String::from_utf8(response)
            .unwrap()
            .starts_with("HTTP/1.1 408"));

        handle.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn authenticated_slow_body_releases_preauth_slot() {
        let config = A2aHttpConfig {
            max_preauth_per_ip: 1,
            allowed_hosts: ["localhost".into()].into_iter().collect(),
            ..A2aHttpConfig::default()
        };
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let cancellation = CancellationToken::new();
        let handle = cancellation.handle();
        let authenticator = Arc::new(SignalingAuthenticator::new());
        let server = Arc::new(
            A2aHttpJsonRpcServer::new(
                Arc::new(Mutex::new(A2aMapper::new())),
                Arc::new(InMemoryA2aMapperSnapshotStore::default()),
                Arc::new(InMemoryA2aEventStore::default()),
                authenticator.clone(),
                Arc::new(CompletingHost::default()),
                agent_card(),
                config,
            )
            .unwrap(),
        );
        let task = tokio::spawn(server.clone().serve(listener, cancellation));

        let stalled_body = send_body("slow-authenticated-body", true, None);
        let stalled_head = format!(
            "POST /a2a HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer owner\r\nA2A-Version: 1.0\r\nContent-Type: application/json\r\nAccept: application/json\r\nContent-Length: {}\r\n\r\n",
            stalled_body.len()
        );
        let mut stalled = TcpStream::connect(address).await.unwrap();
        stalled.write_all(stalled_head.as_bytes()).await.unwrap();
        timeout(
            Duration::from_secs(1),
            authenticator.authenticated.notified(),
        )
        .await
        .expect("stalled request was not authenticated");
        let ip: IpAddr = "127.0.0.1".parse().unwrap();
        timeout(Duration::from_secs(1), async {
            loop {
                let active = server
                    .preauth_limiter
                    .state
                    .lock()
                    .unwrap()
                    .active_per_ip
                    .get(&ip)
                    .copied()
                    .unwrap_or_default();
                if active == 0 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("authenticated request retained its pre-auth permit");

        let second = raw_http(
            address,
            request(
                "POST",
                "/a2a",
                &rpc_headers("application/json"),
                &send_body("second-after-slow-body", true, None),
            ),
        )
        .await;
        assert!(!second.starts_with("HTTP/1.1 429"));
        assert_eq!(
            body(&second)["result"]["task"]["status"]["state"],
            "TASK_STATE_COMPLETED"
        );

        drop(stalled);
        handle.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn saturated_body_lane_preserves_scoped_cancel_probe_without_rate_poisoning() {
        let (mut seeded, mapping) = seeded_task("control-probe-target", A2aTaskState::Working);
        let initial_events: Vec<_> = seeded
            .pending_events()
            .into_iter()
            .map(|event| event.event_id)
            .collect();
        for event_id in initial_events {
            seeded.mark_event_settled(&event_id).unwrap();
        }
        let snapshots = Arc::new(InMemoryA2aMapperSnapshotStore::default());
        snapshots.persist_snapshot(&seeded).await.unwrap();
        let host = Arc::new(CancelHost::new(CancelHostBehavior::Stopped));
        let config = A2aHttpConfig {
            max_concurrency: 1,
            max_control_probes_per_minute: 1,
            allowed_hosts: ["localhost".into()].into_iter().collect(),
            ..A2aHttpConfig::default()
        };
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let cancellation = CancellationToken::new();
        let handle = cancellation.handle();
        let server = Arc::new(
            A2aHttpJsonRpcServer::new(
                Arc::new(Mutex::new(seeded)),
                snapshots,
                Arc::new(InMemoryA2aEventStore::default()),
                Arc::new(ScopedTestAuthenticator),
                host.clone(),
                agent_card(),
                config,
            )
            .unwrap(),
        );
        let task = tokio::spawn(server.clone().serve(listener, cancellation));

        let stalled_body = send_body("ordinary-body-holder", true, None);
        let stalled_head = format!(
            "POST /a2a HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer owner\r\nA2A-Version: 1.0\r\nContent-Type: application/json\r\nAccept: application/json\r\nContent-Length: {}\r\n\r\n",
            stalled_body.len()
        );
        let mut stalled = TcpStream::connect(address).await.unwrap();
        stalled.write_all(stalled_head.as_bytes()).await.unwrap();
        timeout(Duration::from_secs(1), async {
            loop {
                if server.request_global.available_permits() == 0 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("ordinary slow body did not occupy the sole request slot");

        let ordinary_probe = json!({
            "jsonrpc": "2.0",
            "id": "ordinary-probe",
            "method": "GetTask",
            "params": {"tenant": "tenant-a", "id": mapping.task_id}
        })
        .to_string();
        for _ in 0..2 {
            let rejected = raw_http(
                address,
                request(
                    "POST",
                    "/a2a",
                    &rpc_headers("application/json"),
                    &ordinary_probe,
                ),
            )
            .await;
            assert!(rejected.starts_with("HTTP/1.1 503"));
        }

        let mut send_only_headers = rpc_headers("application/json");
        send_only_headers[0] = ("Authorization", "Bearer send-only");
        let send_only = raw_http(
            address,
            request(
                "POST",
                "/a2a",
                &send_only_headers,
                &cancel_body(&mapping.task_id, "send-only-probe"),
            ),
        )
        .await;
        assert!(send_only.starts_with("HTTP/1.1 503"));
        assert_eq!(host.cancel_calls.load(Ordering::SeqCst), 0);

        let cancelled = raw_http(
            address,
            request(
                "POST",
                "/a2a",
                &rpc_headers("application/json"),
                &cancel_body(&mapping.task_id, "valid-control-probe"),
            ),
        )
        .await;
        assert_eq!(
            body(&cancelled)["result"]["status"]["state"],
            "TASK_STATE_CANCELED"
        );
        assert_eq!(host.cancel_calls.load(Ordering::SeqCst), 1);
        let ip: IpAddr = "127.0.0.1".parse().unwrap();
        let owner = A2aEventOwner {
            subject: "owner".into(),
            tenant_id: Some("tenant-a".into()),
        };
        {
            let probe_state = server.control_probe_limiter.state.lock().unwrap();
            assert_eq!(probe_state.attempts_per_owner.get(&owner), Some(&1));
        }
        {
            let request_state = server.control_request_limiter.state.lock().unwrap();
            assert_eq!(request_state.attempts_per_ip.get(&ip), Some(&1));
            assert_eq!(request_state.attempts_per_owner.get(&owner), Some(&1));
        }
        let persisted = server.mapper_snapshot().await;
        assert_eq!(
            persisted
                .cancellation_for_task(&mapping.task_id, &owner_principal())
                .unwrap()
                .state,
            A2aCancellationOutboxState::Settled
        );

        drop(stalled);
        handle.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn malformed_and_foreign_cancel_flood_does_not_charge_shared_ip_intent_rate() {
        let other = other_principal();
        let mut seeded = A2aMapper::new();
        let mapping = prepare_seeded_message_for(&mut seeded, "flood-legitimate-target", &other);
        let dispatch_id = seeded
            .dispatch_for_message("flood-legitimate-target", &other)
            .unwrap()
            .dispatch_id
            .clone();
        seeded.mark_dispatch_settled(&dispatch_id).unwrap();
        let initial_event = event_id_for_message(&seeded, "flood-legitimate-target", &other);
        seeded.mark_event_settled(&initial_event).unwrap();
        let snapshots = Arc::new(InMemoryA2aMapperSnapshotStore::default());
        snapshots.persist_snapshot(&seeded).await.unwrap();
        let host = Arc::new(CancelHost::new(CancelHostBehavior::Stopped));
        let config = A2aHttpConfig {
            max_requests_per_minute: 512,
            max_control_probes_per_minute: 512,
            ..A2aHttpConfig::default()
        };
        let (address, server, handle, task) = start_server_with_dependencies(
            config,
            Arc::new(Mutex::new(seeded)),
            snapshots,
            Arc::new(InMemoryA2aEventStore::default()),
            host.clone(),
            agent_card(),
        )
        .await;

        for attempt in 0..60 {
            let malformed = raw_http(
                address,
                request(
                    "POST",
                    "/a2a",
                    &rpc_headers("application/json"),
                    &format!("{{\"malformed\":{attempt}"),
                ),
            )
            .await;
            assert!(malformed.starts_with("HTTP/1.1 400"));
        }
        for attempt in 0..60 {
            let foreign = raw_http(
                address,
                request(
                    "POST",
                    "/a2a",
                    &rpc_headers("application/json"),
                    &cancel_body_for_tenant(
                        &mapping.task_id,
                        &format!("foreign-cancel-{attempt}"),
                        "tenant-a",
                    ),
                ),
            )
            .await;
            assert_eq!(body(&foreign)["error"]["code"], -32001);
        }
        assert_eq!(host.cancel_calls.load(Ordering::SeqCst), 0);
        let ip: IpAddr = "127.0.0.1".parse().unwrap();
        {
            let request_state = server.control_request_limiter.state.lock().unwrap();
            assert_eq!(request_state.attempts_per_ip.get(&ip), None);
        }

        let mut other_headers = rpc_headers("application/json");
        other_headers[0] = ("Authorization", "Bearer other");
        let cancelled = raw_http(
            address,
            request(
                "POST",
                "/a2a",
                &other_headers,
                &cancel_body_for_tenant(
                    &mapping.task_id,
                    "legitimate-other-owner-cancel",
                    "tenant-b",
                ),
            ),
        )
        .await;
        assert_eq!(
            body(&cancelled)["result"]["status"]["state"],
            "TASK_STATE_CANCELED"
        );
        assert_eq!(host.cancel_calls.load(Ordering::SeqCst), 1);
        {
            let request_state = server.control_request_limiter.state.lock().unwrap();
            assert_eq!(request_state.attempts_per_ip.get(&ip), Some(&1));
            assert_eq!(
                request_state.attempts_per_owner.get(&A2aEventOwner {
                    subject: "other".into(),
                    tenant_id: Some("tenant-b".into()),
                }),
                Some(&1)
            );
        }
        let persisted = server.mapper_snapshot().await;
        assert_eq!(
            persisted
                .cancellation_for_task(&mapping.task_id, &other)
                .unwrap()
                .state,
            A2aCancellationOutboxState::Settled
        );

        handle.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn saturated_same_ip_handshakes_keep_exact_scoped_cancel_overflow_reachable() {
        let (mut seeded, mapping) = seeded_task("handshake-overflow-target", A2aTaskState::Working);
        let initial_events: Vec<_> = seeded
            .pending_events()
            .into_iter()
            .map(|event| event.event_id)
            .collect();
        for event_id in initial_events {
            seeded.mark_event_settled(&event_id).unwrap();
        }
        let snapshots = Arc::new(InMemoryA2aMapperSnapshotStore::default());
        snapshots.persist_snapshot(&seeded).await.unwrap();
        let host = Arc::new(CancelHost::new(CancelHostBehavior::Stopped));
        let config = A2aHttpConfig {
            max_concurrency: 1,
            max_preauth_per_ip: 1,
            handshake_timeout: Duration::from_secs(2),
            allowed_hosts: ["localhost".into()].into_iter().collect(),
            ..A2aHttpConfig::default()
        };
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let cancellation = CancellationToken::new();
        let handle = cancellation.handle();
        let server = Arc::new(
            A2aHttpJsonRpcServer::new(
                Arc::new(Mutex::new(seeded)),
                snapshots,
                Arc::new(InMemoryA2aEventStore::default()),
                Arc::new(ScopedTestAuthenticator),
                host.clone(),
                agent_card(),
                config,
            )
            .unwrap(),
        );
        let task = tokio::spawn(server.clone().serve(listener, cancellation));

        let mut stalled = TcpStream::connect(address).await.unwrap();
        stalled
            .write_all(b"POST /a2a HTTP/1.1\r\nHost: localhost\r\n")
            .await
            .unwrap();
        let ip: IpAddr = "127.0.0.1".parse().unwrap();
        timeout(Duration::from_secs(1), async {
            loop {
                let active = server
                    .preauth_limiter
                    .state
                    .lock()
                    .unwrap()
                    .active_per_ip
                    .get(&ip)
                    .copied()
                    .unwrap_or_default();
                if active == 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("slow header did not saturate ordinary same-IP pre-auth admission");

        let ordinary_method = json!({
            "jsonrpc": "2.0",
            "id": "overflow-get",
            "method": "GetTask",
            "params": {"tenant": "tenant-a", "id": mapping.task_id}
        })
        .to_string();
        let rejected_method = raw_http(
            address,
            request(
                "POST",
                "/a2a",
                &rpc_headers("application/json"),
                &ordinary_method,
            ),
        )
        .await;
        assert!(rejected_method.starts_with("HTTP/1.1 503"));

        let mut send_only_headers = rpc_headers("application/json");
        send_only_headers[0] = ("Authorization", "Bearer send-only");
        let rejected_scope = raw_http(
            address,
            request(
                "POST",
                "/a2a",
                &send_only_headers,
                &cancel_body(&mapping.task_id, "overflow-send-only"),
            ),
        )
        .await;
        assert!(rejected_scope.starts_with("HTTP/1.1 503"));
        assert_eq!(host.cancel_calls.load(Ordering::SeqCst), 0);

        let cancelled = raw_http(
            address,
            request(
                "POST",
                "/a2a",
                &rpc_headers("application/json"),
                &cancel_body(&mapping.task_id, "overflow-valid-cancel"),
            ),
        )
        .await;
        assert_eq!(
            body(&cancelled)["result"]["status"]["state"],
            "TASK_STATE_CANCELED"
        );
        assert_eq!(host.cancel_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            server
                .preauth_limiter
                .state
                .lock()
                .unwrap()
                .active_per_ip
                .get(&ip),
            Some(&1)
        );

        drop(stalled);
        handle.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn exact_cancel_gets_last_chance_after_all_shared_handshake_lanes_are_full() {
        let (mut seeded, mapping) =
            seeded_task("last-chance-handshake-target", A2aTaskState::Working);
        let initial_events: Vec<_> = seeded
            .pending_events()
            .into_iter()
            .map(|event| event.event_id)
            .collect();
        for event_id in initial_events {
            seeded.mark_event_settled(&event_id).unwrap();
        }
        let snapshots = Arc::new(InMemoryA2aMapperSnapshotStore::default());
        snapshots.persist_snapshot(&seeded).await.unwrap();
        let host = Arc::new(CancelHost::new(CancelHostBehavior::Stopped));
        let config = A2aHttpConfig {
            max_concurrency: 1,
            max_preauth_per_ip: 1,
            max_control_dispatches: 2,
            max_control_dispatches_per_owner: 1,
            max_queued_control_dispatches: 2,
            max_queued_control_dispatches_per_owner: 1,
            max_control_requests_per_ip: 1,
            max_control_requests_per_owner: 1,
            control_probe_timeout: Duration::from_secs(2),
            handshake_timeout: Duration::from_secs(4),
            allowed_hosts: ["localhost".into()].into_iter().collect(),
            ..A2aHttpConfig::default()
        };
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let cancellation = CancellationToken::new();
        let handle = cancellation.handle();
        let server = Arc::new(
            A2aHttpJsonRpcServer::new(
                Arc::new(Mutex::new(seeded)),
                snapshots,
                Arc::new(InMemoryA2aEventStore::default()),
                Arc::new(ScopedTestAuthenticator),
                host.clone(),
                agent_card(),
                config,
            )
            .unwrap(),
        );
        let task = tokio::spawn(server.clone().serve(listener, cancellation));

        let partial_head = b"POST /a2a HTTP/1.1\r\nHost: localhost\r\n";
        let mut general = TcpStream::connect(address).await.unwrap();
        general.write_all(partial_head).await.unwrap();
        let ip: IpAddr = "127.0.0.1".parse().unwrap();
        timeout(Duration::from_secs(1), async {
            loop {
                if server
                    .preauth_limiter
                    .state
                    .lock()
                    .unwrap()
                    .active_per_ip
                    .get(&ip)
                    == Some(&1)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("general handshake lane was not occupied");

        let baseline = Arc::strong_count(&server);
        let mut overflow = Vec::new();
        for _ in 0..4 {
            let mut stream = TcpStream::connect(address).await.unwrap();
            stream.write_all(partial_head).await.unwrap();
            overflow.push(stream);
        }
        timeout(Duration::from_secs(1), async {
            loop {
                if Arc::strong_count(&server) >= baseline + 4 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("control handshake and waiter lanes were not all occupied");

        let cancelled = timeout(
            Duration::from_millis(500),
            raw_http(
                address,
                request(
                    "POST",
                    "/a2a",
                    &rpc_headers("application/json"),
                    &cancel_body(&mapping.task_id, "last-chance-exact-cancel"),
                ),
            ),
        )
        .await
        .expect("exact cancellation did not use the bounded last-chance handshake slot");
        assert_eq!(
            body(&cancelled)["result"]["status"]["state"],
            "TASK_STATE_CANCELED"
        );
        assert_eq!(host.cancel_calls.load(Ordering::SeqCst), 1);

        drop(overflow);
        drop(general);
        handle.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn protected_control_listener_isolated_from_public_partial_header_saturation() {
        let (mut seeded, mapping) = seeded_task("protected-control-target", A2aTaskState::Working);
        for event in seeded.pending_events() {
            seeded.mark_event_settled(&event.event_id).unwrap();
        }
        let snapshots = Arc::new(InMemoryA2aMapperSnapshotStore::default());
        snapshots.persist_snapshot(&seeded).await.unwrap();
        let host = Arc::new(CancelHost::new(CancelHostBehavior::Stopped));
        let config = A2aHttpConfig {
            max_concurrency: 1,
            max_preauth_per_ip: 1,
            max_control_dispatches: 2,
            max_control_dispatches_per_owner: 1,
            max_queued_control_dispatches: 2,
            max_queued_control_dispatches_per_owner: 1,
            max_control_requests_per_ip: 1,
            max_control_requests_per_owner: 1,
            control_probe_timeout: Duration::from_secs(2),
            handshake_timeout: Duration::from_secs(5),
            allowed_hosts: ["localhost".into()].into_iter().collect(),
            ..A2aHttpConfig::default()
        };
        let public_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let public_address = public_listener.local_addr().unwrap();
        let protected_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let protected = A2aProtectedControlIngress::new(
            protected_listener,
            Arc::new(ProtectedTestAuthenticator),
        );
        let protected_address = protected.local_addr().unwrap();
        let cancellation = CancellationToken::new();
        let handle = cancellation.handle();
        let server = Arc::new(
            A2aHttpJsonRpcServer::new(
                Arc::new(Mutex::new(seeded.clone())),
                snapshots,
                Arc::new(InMemoryA2aEventStore::default()),
                Arc::new(TestAuthenticator),
                host.clone(),
                agent_card(),
                config,
            )
            .unwrap(),
        );
        let task = tokio::spawn(server.clone().serve_with_protected_control(
            public_listener,
            protected,
            cancellation,
        ));
        timeout(Duration::from_secs(1), async {
            while !server.is_ready() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        let get_body = json!({
            "jsonrpc":"2.0","id":"protected-get","method":"GetTask",
            "params":{"tenant":"tenant-a","id":mapping.task_id}
        })
        .to_string();
        let mut protected_headers = rpc_headers("application/json");
        protected_headers[0] = ("Authorization", "Bearer protected-owner");
        let confined = raw_http(
            protected_address,
            request("POST", "/a2a", &protected_headers, &get_body),
        )
        .await;
        assert_eq!(body(&confined)["error"]["code"], -32601);
        assert_eq!(server.mapper_snapshot().await, seeded);
        assert_eq!(host.cancel_calls.load(Ordering::SeqCst), 0);

        let public_spoof = raw_http(
            public_address,
            request("POST", "/a2a", &protected_headers, &get_body),
        )
        .await;
        assert!(public_spoof.starts_with("HTTP/1.1 401"));
        let protected_spoof = raw_http(
            protected_address,
            request(
                "POST",
                "/a2a",
                &rpc_headers("application/json"),
                &cancel_body(&mapping.task_id, "protected-public-token"),
            ),
        )
        .await;
        assert!(protected_spoof.starts_with("HTTP/1.1 401"));

        let partial_head = b"POST /a2a HTTP/1.1\r\nHost: localhost\r\n";
        let mut general = TcpStream::connect(public_address).await.unwrap();
        general.write_all(partial_head).await.unwrap();
        let ip: IpAddr = "127.0.0.1".parse().unwrap();
        timeout(Duration::from_secs(1), async {
            while server
                .preauth_limiter
                .state
                .lock()
                .unwrap()
                .active_per_ip
                .get(&ip)
                != Some(&1)
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("public general handshake lane was not occupied");
        let baseline = Arc::strong_count(&server);
        let mut overflow = Vec::new();
        for _ in 0..5 {
            let mut stream = TcpStream::connect(public_address).await.unwrap();
            stream.write_all(partial_head).await.unwrap();
            overflow.push(stream);
        }
        timeout(Duration::from_secs(1), async {
            while Arc::strong_count(&server) < baseline + 5 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("public handshake, waiter, and last-chance lanes were not saturated");

        let public_cancel_request = request(
            "POST",
            "/a2a",
            &rpc_headers("application/json"),
            &cancel_body(&mapping.task_id, "public-starved-cancel"),
        );
        let public_attempt = async {
            let mut stream = TcpStream::connect(public_address).await.unwrap();
            let _ = stream.write_all(&public_cancel_request).await;
            let _ = stream.shutdown().await;
            let mut response = Vec::new();
            let _ = stream.read_to_end(&mut response).await;
            String::from_utf8_lossy(&response).into_owned()
        };
        if let Ok(response) = timeout(Duration::from_millis(300), public_attempt).await {
            assert!(response.is_empty() || !response.contains("TASK_STATE_CANCELED"));
        }
        assert_eq!(host.cancel_calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            server.mapper_snapshot().await.tasks()[&mapping.task_id].state,
            A2aTaskState::Working
        );

        let protected_cancel = timeout(
            Duration::from_secs(1),
            raw_http(
                protected_address,
                request(
                    "POST",
                    "/a2a",
                    &protected_headers,
                    &cancel_body(&mapping.task_id, "protected-isolated-cancel"),
                ),
            ),
        )
        .await
        .expect("protected cancellation was delayed by public handshake saturation");
        assert_eq!(
            body(&protected_cancel)["result"]["status"]["state"],
            "TASK_STATE_CANCELED"
        );
        assert_eq!(host.cancel_calls.load(Ordering::SeqCst), 1);

        drop(overflow);
        drop(general);
        handle.cancel();
        task.await.unwrap().unwrap();
        assert!(TcpStream::connect(public_address).await.is_err());
        assert!(TcpStream::connect(protected_address).await.is_err());
    }

    #[tokio::test]
    async fn protected_authenticated_slow_body_flood_is_bounded_and_fair_across_owners() {
        let owner = owner_principal();
        let other = other_principal();
        let mut seeded = A2aMapper::new();
        let owner_mapping =
            prepare_seeded_message_for(&mut seeded, "protected-flood-owner", &owner);
        let other_mapping =
            prepare_seeded_message_for(&mut seeded, "protected-flood-other", &other);
        for (message_id, principal) in [
            ("protected-flood-owner", &owner),
            ("protected-flood-other", &other),
        ] {
            let dispatch_id = seeded
                .dispatch_for_message(message_id, principal)
                .unwrap()
                .dispatch_id
                .clone();
            seeded.mark_dispatch_settled(&dispatch_id).unwrap();
        }
        for event in seeded.pending_events() {
            seeded.mark_event_settled(&event.event_id).unwrap();
        }
        assert_eq!(
            seeded.tasks()[&owner_mapping.task_id].state,
            A2aTaskState::Working
        );
        assert_eq!(
            seeded.tasks()[&other_mapping.task_id].state,
            A2aTaskState::Working
        );

        let snapshots = Arc::new(InMemoryA2aMapperSnapshotStore::default());
        snapshots.persist_snapshot(&seeded).await.unwrap();
        let host = Arc::new(CancelHost::new(CancelHostBehavior::Stopped));
        let config = A2aHttpConfig {
            max_control_dispatches: 2,
            max_control_dispatches_per_owner: 1,
            max_queued_control_dispatches: 1,
            max_queued_control_dispatches_per_owner: 1,
            max_control_requests_per_ip: 1,
            max_control_requests_per_owner: 1,
            control_probe_timeout: Duration::from_millis(500),
            allowed_hosts: ["localhost".into()].into_iter().collect(),
            ..A2aHttpConfig::default()
        };
        let protected_capacity =
            config.max_control_dispatches + config.max_queued_control_dispatches + 1;
        let public_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let protected_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let protected = A2aProtectedControlIngress::new(
            protected_listener,
            Arc::new(MultiOwnerProtectedTestAuthenticator),
        );
        let protected_address = protected.local_addr().unwrap();
        let cancellation = CancellationToken::new();
        let handle = cancellation.handle();
        let server = Arc::new(
            A2aHttpJsonRpcServer::new(
                Arc::new(Mutex::new(seeded)),
                snapshots,
                Arc::new(InMemoryA2aEventStore::default()),
                Arc::new(TestAuthenticator),
                host.clone(),
                agent_card(),
                config,
            )
            .unwrap(),
        );
        let task = tokio::spawn(server.clone().serve_with_protected_control(
            public_listener,
            protected,
            cancellation,
        ));
        timeout(Duration::from_secs(1), async {
            while !server.is_ready() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        let slow_head = b"POST /a2a HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer protected-owner\r\nA2A-Version: 1.0\r\nContent-Type: application/json\r\nAccept: application/json\r\nContent-Length: 4096\r\n\r\n";
        let baseline = Arc::strong_count(&server);
        let mut slow = Vec::new();
        // Owner A takes its sole active body lease and the sole global/owner waiter.
        for _ in 0..2 {
            let mut stream = TcpStream::connect(protected_address).await.unwrap();
            stream.write_all(slow_head).await.unwrap();
            slow.push(stream);
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
        // Fill every remaining ordinary protected header slot with incomplete unauthenticated
        // heads. Owner B must still reach authentication through the one last-chance lane.
        let partial_head = b"POST /a2a HTTP/1.1\r\nHost: localhost\r\n";
        let mut partial = Vec::new();
        for _ in 0..2 {
            let mut stream = TcpStream::connect(protected_address).await.unwrap();
            stream.write_all(partial_head).await.unwrap();
            partial.push(stream);
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(
            Arc::strong_count(&server) <= baseline + protected_capacity + 2,
            "protected connection tasks exceeded active plus bounded waiter capacity"
        );

        let mut other_headers = rpc_headers("application/json");
        other_headers[0] = ("Authorization", "Bearer protected-other");
        let cancelled = timeout(
            Duration::from_secs(1),
            raw_http(
                protected_address,
                request(
                    "POST",
                    "/a2a",
                    &other_headers,
                    &cancel_body_for_tenant(
                        &other_mapping.task_id,
                        "protected-fair-other-cancel",
                        "tenant-b",
                    ),
                ),
            ),
        )
        .await
        .expect("owner B was starved behind owner A protected slow-body flood");
        assert_eq!(
            body(&cancelled)["result"]["status"]["state"],
            "TASK_STATE_CANCELED"
        );
        assert_eq!(host.cancel_calls.load(Ordering::SeqCst), 1);

        drop(slow);
        drop(partial);
        handle.cancel();
        task.await.unwrap().unwrap();
        assert!(TcpStream::connect(protected_address).await.is_err());
    }

    #[tokio::test]
    async fn protected_control_rejects_single_slot_configuration_without_reserved_reachability() {
        let config = A2aHttpConfig {
            max_control_dispatches: 1,
            max_control_dispatches_per_owner: 1,
            max_control_probes_per_ip: 1,
            max_control_requests_per_ip: 1,
            max_control_requests_per_owner: 1,
            allowed_hosts: ["localhost".into()].into_iter().collect(),
            ..A2aHttpConfig::default()
        };
        let server = Arc::new(
            A2aHttpJsonRpcServer::new(
                Arc::new(Mutex::new(A2aMapper::new())),
                Arc::new(InMemoryA2aMapperSnapshotStore::default()),
                Arc::new(InMemoryA2aEventStore::default()),
                Arc::new(TestAuthenticator),
                Arc::new(CompletingHost::default()),
                agent_card(),
                config,
            )
            .unwrap(),
        );
        let public = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let protected = A2aProtectedControlIngress::new(
            TcpListener::bind("127.0.0.1:0").await.unwrap(),
            Arc::new(ProtectedTestAuthenticator),
        );
        let error = server
            .serve_with_protected_control(public, protected, CancellationToken::new())
            .await
            .unwrap_err();
        assert_eq!(error.code, ProtocolErrorCode::InvalidRequest);
        assert!(error.message.contains("at least two"));
    }

    #[tokio::test]
    async fn queued_control_probe_survives_slow_and_ordinary_same_owner_overflow_fifo() {
        let (mut seeded, mapping) = seeded_task("fifo-control-probe-target", A2aTaskState::Working);
        let initial_events: Vec<_> = seeded
            .pending_events()
            .into_iter()
            .map(|event| event.event_id)
            .collect();
        for event_id in initial_events {
            seeded.mark_event_settled(&event_id).unwrap();
        }
        let snapshots = Arc::new(InMemoryA2aMapperSnapshotStore::default());
        snapshots.persist_snapshot(&seeded).await.unwrap();
        let host = Arc::new(CancelHost::new(CancelHostBehavior::Stopped));
        let config = A2aHttpConfig {
            max_concurrency: 1,
            max_preauth_per_ip: 1,
            control_probe_timeout: Duration::from_millis(500),
            max_control_probes_per_minute: 8,
            handshake_timeout: Duration::from_secs(2),
            allowed_hosts: ["localhost".into()].into_iter().collect(),
            ..A2aHttpConfig::default()
        };
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let cancellation = CancellationToken::new();
        let handle = cancellation.handle();
        let server = Arc::new(
            A2aHttpJsonRpcServer::new(
                Arc::new(Mutex::new(seeded)),
                snapshots,
                Arc::new(InMemoryA2aEventStore::default()),
                Arc::new(ScopedTestAuthenticator),
                host.clone(),
                agent_card(),
                config,
            )
            .unwrap(),
        );
        let task = tokio::spawn(server.clone().serve(listener, cancellation));

        let mut stalled_handshake = TcpStream::connect(address).await.unwrap();
        stalled_handshake
            .write_all(b"POST /a2a HTTP/1.1\r\nHost: localhost\r\n")
            .await
            .unwrap();
        let ip: IpAddr = "127.0.0.1".parse().unwrap();
        timeout(Duration::from_secs(1), async {
            loop {
                let active = server
                    .preauth_limiter
                    .state
                    .lock()
                    .unwrap()
                    .active_per_ip
                    .get(&ip)
                    .copied()
                    .unwrap_or_default();
                if active == 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("slow header did not saturate the ordinary same-IP handshake lane");

        let owner = A2aEventOwner {
            subject: "owner".into(),
            tenant_id: Some("tenant-a".into()),
        };
        let slow_body = cancel_body(&mapping.task_id, "slow-overflow-probe");
        let slow_head = format!(
            "POST /a2a HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer owner\r\nA2A-Version: 1.0\r\nContent-Type: application/json\r\nAccept: application/json\r\nContent-Length: {}\r\n\r\n",
            slow_body.len()
        );
        let mut slow_probe = TcpStream::connect(address).await.unwrap();
        slow_probe.write_all(slow_head.as_bytes()).await.unwrap();
        timeout(Duration::from_secs(1), async {
            loop {
                let active = server
                    .control_probe_limiter
                    .state
                    .lock()
                    .unwrap()
                    .active_per_owner
                    .get(&owner)
                    .copied()
                    .unwrap_or_default();
                if active == 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("slow overflow body did not occupy the owner control-probe slot");

        let ordinary_body = json!({
            "jsonrpc": "2.0",
            "id": "queued-ordinary-overflow",
            "method": "GetTask",
            "params": {"tenant": "tenant-a", "id": mapping.task_id}
        })
        .to_string();
        let ordinary = tokio::spawn(raw_http(
            address,
            request(
                "POST",
                "/a2a",
                &rpc_headers("application/json"),
                &ordinary_body,
            ),
        ));
        timeout(Duration::from_secs(1), async {
            loop {
                if server
                    .control_probe_limiter
                    .state
                    .lock()
                    .unwrap()
                    .waiters
                    .len()
                    == 1
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("ordinary overflow did not queue behind the slow owner probe");

        let valid = tokio::spawn(raw_http(
            address,
            request(
                "POST",
                "/a2a",
                &rpc_headers("application/json"),
                &cancel_body(&mapping.task_id, "queued-valid-cancel"),
            ),
        ));
        timeout(Duration::from_secs(1), async {
            loop {
                if server
                    .control_probe_limiter
                    .state
                    .lock()
                    .unwrap()
                    .waiters
                    .len()
                    == 2
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("valid cancellation did not queue behind the earlier owner probe");

        let mut slow_response = Vec::new();
        timeout(
            Duration::from_secs(2),
            slow_probe.read_to_end(&mut slow_response),
        )
        .await
        .expect("slow owner probe did not reach its bounded body deadline")
        .unwrap();
        assert!(String::from_utf8(slow_response)
            .unwrap()
            .starts_with("HTTP/1.1 408"));

        let ordinary_response = timeout(Duration::from_secs(1), ordinary)
            .await
            .expect("ordinary FIFO predecessor did not finish")
            .unwrap();
        assert!(ordinary_response.starts_with("HTTP/1.1 503"));
        let cancelled = timeout(Duration::from_secs(1), valid)
            .await
            .expect("valid cancellation did not advance through the FIFO queue")
            .unwrap();
        assert_eq!(
            body(&cancelled)["result"]["status"]["state"],
            "TASK_STATE_CANCELED"
        );
        assert_eq!(host.cancel_calls.load(Ordering::SeqCst), 1);
        {
            let state = server.control_probe_limiter.state.lock().unwrap();
            assert!(state.waiters.is_empty());
            assert!(state.active_per_ip.is_empty());
            assert!(state.active_per_owner.is_empty());
        }

        drop(stalled_handshake);
        handle.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn same_ip_slow_owner_probes_cannot_starve_other_owner_cancel() {
        let owner = owner_principal();
        let other = other_principal();
        let (mut seeded, owner_mapping) =
            seeded_task("shared-nat-slow-owner", A2aTaskState::Working);
        let owner_event_ids: Vec<_> = seeded
            .pending_events()
            .into_iter()
            .map(|event| event.event_id)
            .collect();
        for event_id in owner_event_ids {
            seeded.mark_event_settled(&event_id).unwrap();
        }
        let other_message_id = "shared-nat-other-owner";
        let other_mapping = prepare_seeded_message_for(&mut seeded, other_message_id, &other);
        let other_dispatch_id = seeded
            .dispatch_for_message(other_message_id, &other)
            .unwrap()
            .dispatch_id
            .clone();
        seeded.mark_dispatch_settled(&other_dispatch_id).unwrap();
        let other_event_id = event_id_for_message(&seeded, other_message_id, &other);
        seeded.mark_event_settled(&other_event_id).unwrap();

        let snapshots = Arc::new(InMemoryA2aMapperSnapshotStore::default());
        snapshots.persist_snapshot(&seeded).await.unwrap();
        let host = Arc::new(CancelHost::new(CancelHostBehavior::Stopped));
        let config = A2aHttpConfig {
            max_concurrency: 1,
            max_preauth_per_ip: 1,
            control_probe_timeout: Duration::from_secs(5),
            max_control_probes_per_ip: 2,
            max_control_probes_per_owner: 1,
            allowed_hosts: ["localhost".into()].into_iter().collect(),
            ..A2aHttpConfig::default()
        };
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let cancellation = CancellationToken::new();
        let handle = cancellation.handle();
        let server = Arc::new(
            A2aHttpJsonRpcServer::new(
                Arc::new(Mutex::new(seeded)),
                snapshots,
                Arc::new(InMemoryA2aEventStore::default()),
                Arc::new(TestAuthenticator),
                host.clone(),
                agent_card(),
                config,
            )
            .unwrap(),
        );
        let task = tokio::spawn(server.clone().serve(listener, cancellation));

        let mut stalled_handshake = TcpStream::connect(address).await.unwrap();
        stalled_handshake
            .write_all(b"POST /a2a HTTP/1.1\r\nHost: localhost\r\n")
            .await
            .unwrap();
        let ip: IpAddr = "127.0.0.1".parse().unwrap();
        timeout(Duration::from_secs(1), async {
            loop {
                let active = server
                    .preauth_limiter
                    .state
                    .lock()
                    .unwrap()
                    .active_per_ip
                    .get(&ip)
                    .copied()
                    .unwrap_or_default();
                if active == 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("ordinary shared-IP handshake lane was not saturated");

        let owner_key = A2aEventOwner {
            subject: owner.subject.clone(),
            tenant_id: owner.tenant_id.clone(),
        };
        let slow_body = cancel_body(&owner_mapping.task_id, "shared-nat-slow-probe");
        let slow_head = format!(
            "POST /a2a HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer owner\r\nA2A-Version: 1.0\r\nContent-Type: application/json\r\nAccept: application/json\r\nContent-Length: {}\r\n\r\n",
            slow_body.len()
        );
        let mut active_owner_probe = TcpStream::connect(address).await.unwrap();
        active_owner_probe
            .write_all(slow_head.as_bytes())
            .await
            .unwrap();
        timeout(Duration::from_secs(1), async {
            loop {
                let active = server
                    .control_probe_limiter
                    .state
                    .lock()
                    .unwrap()
                    .active_per_owner
                    .get(&owner_key)
                    .copied()
                    .unwrap_or_default();
                if active == 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("slow owner probe did not occupy its bounded active slot");

        let mut queued_owner_probe = TcpStream::connect(address).await.unwrap();
        queued_owner_probe
            .write_all(slow_head.as_bytes())
            .await
            .unwrap();
        timeout(Duration::from_secs(1), async {
            loop {
                let queued = server
                    .control_probe_limiter
                    .state
                    .lock()
                    .unwrap()
                    .waiters_per_owner
                    .get(&owner_key)
                    .copied()
                    .unwrap_or_default();
                if queued == 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("second slow owner probe did not enter the bounded FIFO queue");

        let mut other_headers = rpc_headers("application/json");
        other_headers[0] = ("Authorization", "Bearer other");
        let cancelled = raw_http(
            address,
            request(
                "POST",
                "/a2a",
                &other_headers,
                &cancel_body_for_tenant(
                    &other_mapping.task_id,
                    "shared-nat-other-cancel",
                    "tenant-b",
                ),
            ),
        )
        .await;
        assert_eq!(
            body(&cancelled)["result"]["status"]["state"],
            "TASK_STATE_CANCELED"
        );
        assert_eq!(host.cancel_calls.load(Ordering::SeqCst), 1);
        {
            let state = server.control_probe_limiter.state.lock().unwrap();
            assert_eq!(state.active_per_owner.get(&owner_key), Some(&1));
            assert_eq!(state.waiters_per_owner.get(&owner_key), Some(&1));
        }

        drop(queued_owner_probe);
        drop(active_owner_probe);
        drop(stalled_handshake);
        handle.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn two_shared_nat_owner_backlogs_cannot_starve_a_third_exact_cancel() {
        let owner = owner_principal();
        let other = other_principal();
        let third = third_principal();
        let mut seeded = A2aMapper::new();
        let owner_mapping = prepare_seeded_message_for(&mut seeded, "nat-owner-a", &owner);
        let other_mapping = prepare_seeded_message_for(&mut seeded, "nat-owner-b", &other);
        let third_mapping = prepare_seeded_message_for(&mut seeded, "nat-owner-c", &third);
        for (message_id, principal) in [
            ("nat-owner-a", &owner),
            ("nat-owner-b", &other),
            ("nat-owner-c", &third),
        ] {
            let dispatch_id = seeded
                .dispatch_for_message(message_id, principal)
                .unwrap()
                .dispatch_id
                .clone();
            seeded.mark_dispatch_settled(&dispatch_id).unwrap();
            let event_id = event_id_for_message(&seeded, message_id, principal);
            seeded.mark_event_settled(&event_id).unwrap();
        }

        let snapshots = Arc::new(InMemoryA2aMapperSnapshotStore::default());
        snapshots.persist_snapshot(&seeded).await.unwrap();
        let host = Arc::new(CancelHost::new(CancelHostBehavior::Stopped));
        let config = A2aHttpConfig {
            max_concurrency: 1,
            max_preauth_per_ip: 1,
            control_probe_timeout: Duration::from_millis(300),
            max_control_probes_per_ip: 2,
            max_control_probes_per_owner: 1,
            max_control_probes_per_minute: 8,
            handshake_timeout: Duration::from_secs(2),
            allowed_hosts: ["localhost".into()].into_iter().collect(),
            ..A2aHttpConfig::default()
        };
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let cancellation = CancellationToken::new();
        let handle = cancellation.handle();
        let server = Arc::new(
            A2aHttpJsonRpcServer::new(
                Arc::new(Mutex::new(seeded)),
                snapshots,
                Arc::new(InMemoryA2aEventStore::default()),
                Arc::new(TestAuthenticator),
                host.clone(),
                agent_card(),
                config,
            )
            .unwrap(),
        );
        let task = tokio::spawn(server.clone().serve(listener, cancellation));

        let mut general = TcpStream::connect(address).await.unwrap();
        general
            .write_all(b"POST /a2a HTTP/1.1\r\nHost: localhost\r\n")
            .await
            .unwrap();
        let ip: IpAddr = "127.0.0.1".parse().unwrap();
        timeout(Duration::from_secs(1), async {
            loop {
                if server
                    .preauth_limiter
                    .state
                    .lock()
                    .unwrap()
                    .active_per_ip
                    .get(&ip)
                    == Some(&1)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("ordinary shared-NAT handshake lane was not occupied");

        let slow_head = |token: &str, body: String| {
            format!(
                "POST /a2a HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {token}\r\nA2A-Version: 1.0\r\nContent-Type: application/json\r\nAccept: application/json\r\nContent-Length: {}\r\n\r\n",
                body.len()
            )
        };
        let owner_head = slow_head(
            "owner",
            cancel_body(&owner_mapping.task_id, "nat-owner-a-slow"),
        );
        let other_head = slow_head(
            "other",
            cancel_body_for_tenant(&other_mapping.task_id, "nat-owner-b-slow", "tenant-b"),
        );
        let mut owner_active = TcpStream::connect(address).await.unwrap();
        owner_active.write_all(owner_head.as_bytes()).await.unwrap();
        let mut other_active = TcpStream::connect(address).await.unwrap();
        other_active.write_all(other_head.as_bytes()).await.unwrap();
        timeout(Duration::from_secs(1), async {
            loop {
                if server
                    .control_probe_limiter
                    .state
                    .lock()
                    .unwrap()
                    .active_total
                    == 2
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("two scoped owners did not occupy the shared-NAT probe slots");

        let mut owner_backlog = TcpStream::connect(address).await.unwrap();
        owner_backlog
            .write_all(owner_head.as_bytes())
            .await
            .unwrap();
        let mut other_backlog = TcpStream::connect(address).await.unwrap();
        other_backlog
            .write_all(other_head.as_bytes())
            .await
            .unwrap();
        timeout(Duration::from_secs(1), async {
            loop {
                let backlog_ready = {
                    let state = server.control_probe_limiter.state.lock().unwrap();
                    state
                        .waiters_per_owner
                        .values()
                        .filter(|count| **count == 1)
                        .count()
                        >= 2
                };
                if backlog_ready {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the two slow owners did not build their bounded backlogs");

        let mut third_headers = rpc_headers("application/json");
        third_headers[0] = ("Authorization", "Bearer third");
        let started = Instant::now();
        let cancelled = timeout(
            Duration::from_millis(600),
            raw_http(
                address,
                request(
                    "POST",
                    "/a2a",
                    &third_headers,
                    &cancel_body_for_tenant(
                        &third_mapping.task_id,
                        "nat-third-exact-cancel",
                        "tenant-c",
                    ),
                ),
            ),
        )
        .await
        .expect("third owner was delayed behind the two slow-owner backlogs");
        assert!(started.elapsed() < Duration::from_millis(600));
        assert_eq!(
            body(&cancelled)["result"]["status"]["state"],
            "TASK_STATE_CANCELED"
        );
        assert_eq!(host.cancel_calls.load(Ordering::SeqCst), 1);

        drop(other_backlog);
        drop(owner_backlog);
        drop(other_active);
        drop(owner_active);
        drop(general);
        handle.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn incomplete_owner_probe_flood_is_bounded_without_poisoning_shared_nat_cancel() {
        let owner = owner_principal();
        let other = other_principal();
        let (mut seeded, owner_mapping) =
            seeded_task("shared-nat-probe-flood", A2aTaskState::Working);
        let owner_event_ids: Vec<_> = seeded
            .pending_events()
            .into_iter()
            .map(|event| event.event_id)
            .collect();
        for event_id in owner_event_ids {
            seeded.mark_event_settled(&event_id).unwrap();
        }
        let other_message_id = "shared-nat-flood-survivor";
        let other_mapping = prepare_seeded_message_for(&mut seeded, other_message_id, &other);
        let other_dispatch_id = seeded
            .dispatch_for_message(other_message_id, &other)
            .unwrap()
            .dispatch_id
            .clone();
        seeded.mark_dispatch_settled(&other_dispatch_id).unwrap();
        let other_event_id = event_id_for_message(&seeded, other_message_id, &other);
        seeded.mark_event_settled(&other_event_id).unwrap();

        let snapshots = Arc::new(InMemoryA2aMapperSnapshotStore::default());
        snapshots.persist_snapshot(&seeded).await.unwrap();
        let host = Arc::new(CancelHost::new(CancelHostBehavior::Stopped));
        let config = A2aHttpConfig {
            max_concurrency: 1,
            max_preauth_per_ip: 1,
            max_control_probes_per_minute: 120,
            handshake_timeout: Duration::from_secs(10),
            allowed_hosts: ["localhost".into()].into_iter().collect(),
            ..A2aHttpConfig::default()
        };
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let cancellation = CancellationToken::new();
        let handle = cancellation.handle();
        let server = Arc::new(
            A2aHttpJsonRpcServer::new(
                Arc::new(Mutex::new(seeded)),
                snapshots,
                Arc::new(InMemoryA2aEventStore::default()),
                Arc::new(TestAuthenticator),
                host.clone(),
                agent_card(),
                config,
            )
            .unwrap(),
        );
        let task = tokio::spawn(server.clone().serve(listener, cancellation));

        let mut stalled_handshake = TcpStream::connect(address).await.unwrap();
        stalled_handshake
            .write_all(b"POST /a2a HTTP/1.1\r\nHost: localhost\r\n")
            .await
            .unwrap();
        let ip: IpAddr = "127.0.0.1".parse().unwrap();
        timeout(Duration::from_secs(1), async {
            loop {
                let active = server
                    .preauth_limiter
                    .state
                    .lock()
                    .unwrap()
                    .active_per_ip
                    .get(&ip)
                    .copied()
                    .unwrap_or_default();
                if active == 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("ordinary shared-IP handshake lane was not saturated");

        let incomplete_body = cancel_body(&owner_mapping.task_id, "incomplete-owner-probe");
        let incomplete_head = format!(
            "POST /a2a HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer owner\r\nA2A-Version: 1.0\r\nContent-Type: application/json\r\nAccept: application/json\r\nContent-Length: {}\r\n\r\n",
            incomplete_body.len()
        )
        .into_bytes();
        timeout(Duration::from_secs(5), async {
            for attempt in 0..120 {
                let response = raw_http(address, incomplete_head.clone()).await;
                assert!(
                    response.starts_with("HTTP/1.1 400"),
                    "incomplete probe {attempt} was not rejected after bounded parsing"
                );
            }
        })
        .await
        .expect("the bounded incomplete-probe quota did not drain promptly");

        let owner_key = A2aEventOwner {
            subject: owner.subject.clone(),
            tenant_id: owner.tenant_id.clone(),
        };
        {
            let state = server.control_probe_limiter.state.lock().unwrap();
            assert!(state
                .attempts_per_owner
                .get(&owner_key)
                .is_some_and(|attempts| (1..=120).contains(attempts)));
            assert!(state.attempts_per_ip.is_empty());
            assert_eq!(state.active_total, 0);
            assert!(state.waiters.is_empty());
        }

        let excess_head = incomplete_head.clone();
        let excess = tokio::spawn(async move {
            let mut rejected = 0;
            for _ in 0..16 {
                let response = raw_http(address, excess_head.clone()).await;
                // The ingress may still inspect a quota-exhausted probe so an attacker cannot
                // pre-body-poison a real cancellation, but the incomplete body is always rejected
                // and never grows the bounded owner counter beyond its configured ceiling.
                assert!(response.starts_with("HTTP/1.1 400"));
                rejected += 1;
            }
            rejected
        });
        let mut other_headers = rpc_headers("application/json");
        other_headers[0] = ("Authorization", "Bearer other");
        let cancelled = timeout(
            Duration::from_secs(2),
            raw_http(
                address,
                request(
                    "POST",
                    "/a2a",
                    &other_headers,
                    &cancel_body_for_tenant(
                        &other_mapping.task_id,
                        "shared-nat-flood-survivor-cancel",
                        "tenant-b",
                    ),
                ),
            ),
        )
        .await
        .expect("another owner cancellation was starved by over-quota shared-NAT probes");
        assert_eq!(
            body(&cancelled)["result"]["status"]["state"],
            "TASK_STATE_CANCELED"
        );
        assert_eq!(host.cancel_calls.load(Ordering::SeqCst), 1);

        let rejected = timeout(Duration::from_secs(5), excess)
            .await
            .expect("over-quota owner probes did not remain time-bounded")
            .unwrap();
        assert_eq!(rejected, 16);
        {
            let state = server.control_probe_limiter.state.lock().unwrap();
            assert!(state
                .attempts_per_owner
                .get(&owner_key)
                .is_some_and(|attempts| *attempts <= 120));
            assert_eq!(state.active_total, 0);
            assert!(state.waiters.is_empty());
        }

        drop(stalled_handshake);
        handle.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn slow_general_writer_drops_overflow_without_blocking_control_response() {
        let other = other_principal();
        let mut seeded = A2aMapper::new();
        let mapping = prepare_seeded_message_for(&mut seeded, "writer-control-target", &other);
        let dispatch_id = seeded
            .dispatch_for_message("writer-control-target", &other)
            .unwrap()
            .dispatch_id
            .clone();
        seeded.mark_dispatch_settled(&dispatch_id).unwrap();
        let event_id = event_id_for_message(&seeded, "writer-control-target", &other);
        seeded.mark_event_settled(&event_id).unwrap();
        let snapshots = Arc::new(InMemoryA2aMapperSnapshotStore::default());
        snapshots.persist_snapshot(&seeded).await.unwrap();
        let host = Arc::new(CancelHost::new(CancelHostBehavior::Stopped));
        let config = A2aHttpConfig {
            max_concurrency: 1,
            handshake_timeout: Duration::from_secs(10),
            ..A2aHttpConfig::default()
        };
        let (address, server, handle, task) = start_server_with_dependencies(
            config,
            Arc::new(Mutex::new(seeded)),
            snapshots,
            Arc::new(InMemoryA2aEventStore::default()),
            host.clone(),
            large_agent_card(),
        )
        .await;

        let socket = tokio::net::TcpSocket::new_v4().unwrap();
        socket.set_recv_buffer_size(1024).unwrap();
        let mut slow_reader = socket.connect(address).await.unwrap();
        slow_reader
            .write_all(&request("GET", A2A_AGENT_CARD_PATH, &[], ""))
            .await
            .unwrap();
        slow_reader.shutdown().await.unwrap();
        timeout(Duration::from_secs(2), async {
            loop {
                if server.response_write_global.available_permits() == 0 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("zero-read Agent Card client did not occupy the general writer");
        let saturated_strong_count = Arc::strong_count(&server);

        let mut overflow_headers = rpc_headers("application/json");
        overflow_headers[0] = ("Authorization", "Bearer other");
        let overflow_body = json!({
            "jsonrpc": "2.0",
            "id": "writer-overflow-get",
            "method": "GetTask",
            "params": {"tenant": "tenant-b", "id": mapping.task_id}
        })
        .to_string();
        let mut overflow = JoinSet::new();
        for _ in 0..16 {
            overflow.spawn(raw_http(
                address,
                request("POST", "/a2a", &overflow_headers, &overflow_body),
            ));
        }
        let overflow_responses = timeout(Duration::from_secs(4), async {
            let mut responses = Vec::new();
            while let Some(result) = overflow.join_next().await {
                responses.push(result.unwrap());
            }
            responses
        })
        .await
        .expect("ordinary response overflow queued behind the slow reader");
        assert_eq!(overflow_responses.len(), 16);
        assert!(overflow_responses.iter().all(String::is_empty));
        timeout(Duration::from_secs(1), async {
            loop {
                if Arc::strong_count(&server) <= saturated_strong_count + 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("completed overflow connections retained unbounded server tasks");
        assert_eq!(server.mapper_snapshot().await.tasks().len(), 1);
        assert_eq!(server.response_write_global.available_permits(), 0);

        let mut other_headers = rpc_headers("application/json");
        other_headers[0] = ("Authorization", "Bearer other");
        let cancelled = timeout(
            Duration::from_secs(2),
            raw_http(
                address,
                request(
                    "POST",
                    "/a2a",
                    &other_headers,
                    &cancel_body_for_tenant(&mapping.task_id, "writer-priority-cancel", "tenant-b"),
                ),
            ),
        )
        .await
        .expect("control response waited behind the saturated general writer");
        assert_eq!(
            body(&cancelled)["result"]["status"]["state"],
            "TASK_STATE_CANCELED"
        );
        assert_eq!(host.cancel_calls.load(Ordering::SeqCst), 1);
        assert_eq!(server.mapper_snapshot().await.tasks().len(), 1);
        assert_eq!(server.response_write_global.available_permits(), 0);
        assert_eq!(
            server.control_response_write_global.available_permits(),
            server.config.max_control_dispatches
        );

        drop(slow_reader);
        handle.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn http_gates_fail_closed_before_dispatch() {
        let config = A2aHttpConfig {
            max_body_bytes: 4096,
            ..A2aHttpConfig::default()
        };
        let (address, _server, handle, task) = start_server(config).await;
        let body_value = send_body("gate-1", true, None);

        let mut bad_host = request("GET", A2A_AGENT_CARD_PATH, &[], "");
        let request_text = String::from_utf8(bad_host)
            .unwrap()
            .replace("Host: localhost", "Host: evil.example");
        bad_host = request_text.into_bytes();
        assert!(raw_http(address, bad_host)
            .await
            .starts_with("HTTP/1.1 421"));

        let unauthenticated = raw_http(
            address,
            request(
                "POST",
                "/a2a",
                &[
                    ("A2A-Version", "1.0"),
                    ("Content-Type", "application/json"),
                    ("Accept", "application/json"),
                ],
                &body_value,
            ),
        )
        .await;
        assert!(unauthenticated.starts_with("HTTP/1.1 401"));
        assert!(unauthenticated.contains("WWW-Authenticate"));

        let bad_origin = raw_http(
            address,
            request(
                "POST",
                "/a2a",
                &[
                    ("Authorization", "Bearer owner"),
                    ("Origin", "https://evil.example"),
                    ("A2A-Version", "1.0"),
                    ("Content-Type", "application/json"),
                    ("Accept", "application/json"),
                ],
                &body_value,
            ),
        )
        .await;
        assert!(bad_origin.starts_with("HTTP/1.1 403"));

        let no_version = raw_http(
            address,
            request(
                "POST",
                "/a2a",
                &[
                    ("Authorization", "Bearer owner"),
                    ("Content-Type", "application/json"),
                    ("Accept", "application/json"),
                ],
                &body_value,
            ),
        )
        .await;
        assert_eq!(body(&no_version)["error"]["code"], -32009);

        let rejected_accept = raw_http(
            address,
            request(
                "POST",
                "/a2a",
                &rpc_headers("application/json;q=0"),
                &body_value,
            ),
        )
        .await;
        assert!(rejected_accept.starts_with("HTTP/1.1 406"));

        let oversized = "x".repeat(4097);
        let too_large = raw_http(
            address,
            request("POST", "/a2a", &rpc_headers("application/json"), &oversized),
        )
        .await;
        assert!(too_large.starts_with("HTTP/1.1 413"));

        handle.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn snapshot_store_cas_is_idempotent_only_for_same_revision_and_digest() {
        let store = InMemoryA2aMapperSnapshotStore::default();
        let initial = A2aSerializedMapperSnapshot::from_mapper(&A2aMapper::new()).unwrap();
        assert_eq!(
            store
                .compare_and_swap_snapshot(None, initial.clone())
                .await
                .unwrap(),
            A2aSnapshotCommitOutcome::Applied
        );
        assert_eq!(
            store
                .compare_and_swap_snapshot(None, initial.clone())
                .await
                .unwrap(),
            A2aSnapshotCommitOutcome::AlreadyApplied
        );

        let mut different_digest = initial.clone();
        different_digest.digest = "0".repeat(64);
        let error = store
            .compare_and_swap_snapshot(None, different_digest)
            .await
            .unwrap_err();
        assert!(error
            .into_protocol_error()
            .message
            .contains("different digest"));

        let mut advanced = A2aMapper::new();
        prepare_seeded_message_for(&mut advanced, "cas-advanced", &owner_principal());
        let advanced = A2aSerializedMapperSnapshot::from_mapper(&advanced).unwrap();
        store
            .compare_and_swap_snapshot(Some(initial.version()), advanced)
            .await
            .unwrap();
        let error = store
            .compare_and_swap_snapshot(None, initial)
            .await
            .unwrap_err();
        assert!(error
            .into_protocol_error()
            .message
            .contains("revision conflict"));
    }

    #[tokio::test]
    async fn raw_v1_and_v2_store_heads_atomically_advance_to_current_schema() {
        for legacy_schema in [1_u64, 2_u64] {
            let mut seeded = A2aMapper::new();
            prepare_seeded_message_for(
                &mut seeded,
                &format!("raw-schema-{legacy_schema}"),
                &owner_principal(),
            );
            let mut raw = serde_json::to_value(&seeded).unwrap();
            raw["schema_version"] = Value::from(legacy_schema);
            if legacy_schema == 1 {
                let object = raw.as_object_mut().unwrap();
                object.remove("dispatch_outbox");
                object.remove("cancellation_outbox");
                object.remove("pending_events");
            }
            let raw_snapshot = A2aSerializedMapperSnapshot::from_persisted_bytes(
                serde_json::to_vec(&raw).unwrap(),
            )
            .unwrap();
            let raw_version = raw_snapshot.version();
            let restored = raw_snapshot.decode().unwrap();
            let event_id = restored.pending_events().first().unwrap().event_id.clone();
            let store = Arc::new(InMemoryA2aMapperSnapshotStore::default());
            *store.snapshot.lock().await = Some(raw_snapshot);
            let server = A2aHttpJsonRpcServer::new_owned(
                restored,
                store.clone(),
                Arc::new(InMemoryA2aEventStore::default()),
                Arc::new(TestAuthenticator),
                Arc::new(CompletingHost::default()),
                agent_card(),
                A2aHttpConfig::default(),
            )
            .unwrap();

            server
                .persist_mapper_mutation(|candidate| candidate.mark_event_settled(&event_id))
                .await
                .unwrap();

            let stored = store.load_serialized_snapshot().await.unwrap().unwrap();
            assert!(stored.revision() > raw_version.revision);
            assert_ne!(stored.digest(), raw_version.digest);
            let stored_json: Value = serde_json::from_slice(stored.bytes()).unwrap();
            assert_eq!(
                stored_json["schema_version"],
                Value::from(u64::from(A2aMapper::new().schema_version()))
            );
            assert_eq!(stored.decode().unwrap(), server.mapper_snapshot().await);
        }
    }

    #[tokio::test]
    async fn serve_rejects_a_divergent_durable_head_before_readiness_or_reads() {
        let (durable, durable_mapping) = seeded_task("startup-durable-head", A2aTaskState::Working);
        let (stale_live, stale_mapping) = seeded_task("startup-stale-live", A2aTaskState::Working);
        assert_eq!(durable.revision(), stale_live.revision());
        assert_ne!(durable, stale_live);
        assert_ne!(durable_mapping.message_id, stale_mapping.message_id);
        let stale_live_before_serve = stale_live.clone();

        let snapshots = Arc::new(InMemoryA2aMapperSnapshotStore::default());
        snapshots.persist_snapshot(&durable).await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = Arc::new(
            A2aHttpJsonRpcServer::new_owned(
                stale_live,
                snapshots.clone(),
                Arc::new(InMemoryA2aEventStore::default()),
                Arc::new(TestAuthenticator),
                Arc::new(CompletingHost::default()),
                agent_card(),
                A2aHttpConfig::default(),
            )
            .unwrap(),
        );

        let error = timeout(
            Duration::from_secs(1),
            server.clone().serve(listener, CancellationToken::new()),
        )
        .await
        .expect("startup durable-head verification hung")
        .unwrap_err();
        assert!(error.message.contains("durable snapshot content diverged"));
        assert!(!server.is_ready());
        let stored = snapshots.load_snapshot().await.unwrap();
        assert_eq!(stored, durable);
        assert_ne!(stored, stale_live_before_serve);
        assert!(
            timeout(Duration::from_millis(100), TcpStream::connect(address))
                .await
                .unwrap()
                .is_err()
        );
    }

    #[tokio::test]
    async fn startup_snapshot_initialization_is_cancellable_while_the_store_is_blocked() {
        let snapshots = Arc::new(BlockingSnapshotStore::default());
        snapshots.block_next.store(true, Ordering::SeqCst);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let cancellation = CancellationToken::new();
        let handle = cancellation.handle();
        let server = Arc::new(
            A2aHttpJsonRpcServer::new_owned(
                A2aMapper::new(),
                snapshots.clone(),
                Arc::new(InMemoryA2aEventStore::default()),
                Arc::new(TestAuthenticator),
                Arc::new(CompletingHost::default()),
                agent_card(),
                A2aHttpConfig::default(),
            )
            .unwrap(),
        );
        let serve = tokio::spawn(server.clone().serve(listener, cancellation));
        timeout(Duration::from_secs(1), snapshots.entered.notified())
            .await
            .expect("startup snapshot initialization did not reach the blocked store");

        handle.cancel();
        timeout(Duration::from_millis(250), serve)
            .await
            .expect("startup ignored cancellation while snapshot initialization was blocked")
            .unwrap()
            .unwrap();
        assert!(!server.is_ready());
        assert!(
            timeout(Duration::from_millis(100), TcpStream::connect(address))
                .await
                .unwrap()
                .is_err()
        );
    }

    #[tokio::test]
    async fn externally_served_reads_fail_stop_after_a_competing_head_advance() {
        for method in ["GetTask", "ListTasks", "SubscribeToTask"] {
            let message_id = format!("shared-head-{}", method.to_ascii_lowercase());
            let (seeded, mapping) = seeded_task(&message_id, A2aTaskState::Working);
            let snapshots = Arc::new(InMemoryA2aMapperSnapshotStore::default());
            snapshots.persist_snapshot(&seeded).await.unwrap();
            let (address, server, _handle, task) = start_server_with_dependencies(
                A2aHttpConfig::default(),
                Arc::new(Mutex::new(seeded.clone())),
                snapshots.clone(),
                Arc::new(InMemoryA2aEventStore::default()),
                Arc::new(CompletingHost::default()),
                agent_card(),
            )
            .await;
            timeout(Duration::from_secs(1), async {
                while !server.is_ready() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("server never became ready");

            let current = snapshots.load_serialized_snapshot().await.unwrap().unwrap();
            let mut advanced = current.decode().unwrap();
            advanced
                .transition_task(
                    &mapping.task_id,
                    A2aTaskState::InputRequired,
                    Some("advanced by a competing writer".into()),
                )
                .unwrap();
            snapshots
                .compare_and_swap_snapshot(
                    Some(current.version()),
                    A2aSerializedMapperSnapshot::from_mapper(&advanced).unwrap(),
                )
                .await
                .unwrap();

            let params = if method == "ListTasks" {
                json!({"tenant": "tenant-a"})
            } else {
                json!({"tenant": "tenant-a", "id": mapping.task_id})
            };
            let body = json!({
                "jsonrpc": "2.0",
                "id": format!("{method}-shared-head"),
                "method": method,
                "params": params,
            })
            .to_string();
            let accept = if method == "SubscribeToTask" {
                "text/event-stream"
            } else {
                "application/json"
            };
            let response = try_raw_http(
                address,
                request("POST", "/a2a", &rpc_headers(accept), &body),
            )
            .await
            .unwrap_or_default();

            assert!(
                !response.starts_with("HTTP/1.1 200"),
                "{method} served a stale result after a competing durable-head advance: {response}"
            );
            let serve_error = timeout(Duration::from_secs(2), task)
                .await
                .expect("divergent shared head did not stop the listener")
                .unwrap()
                .unwrap_err();
            assert!(serve_error
                .message
                .contains("durable snapshot head diverged"));
            assert!(!server.is_ready());
            assert_eq!(snapshots.load_snapshot().await.unwrap(), advanced);
        }
    }

    #[tokio::test]
    async fn exact_send_and_cancel_retries_probe_the_shared_head_before_fast_paths() {
        let (seeded_send, send_mapping) =
            seeded_task("shared-head-send-retry", A2aTaskState::Working);
        let send_snapshots = Arc::new(InMemoryA2aMapperSnapshotStore::default());
        send_snapshots.persist_snapshot(&seeded_send).await.unwrap();
        let send_initial = send_snapshots
            .load_serialized_snapshot()
            .await
            .unwrap()
            .unwrap();
        let send_server = Arc::new(
            A2aHttpJsonRpcServer::new_owned(
                seeded_send.clone(),
                send_snapshots.clone(),
                Arc::new(InMemoryA2aEventStore::default()),
                Arc::new(TestAuthenticator),
                Arc::new(CompletingHost::default()),
                agent_card(),
                A2aHttpConfig::default(),
            )
            .unwrap(),
        );
        send_server.initialize_snapshot_store().await.unwrap();
        let mut advanced_send = seeded_send;
        advanced_send
            .transition_task(
                &send_mapping.task_id,
                A2aTaskState::InputRequired,
                Some("advanced send head".into()),
            )
            .unwrap();
        send_snapshots
            .compare_and_swap_snapshot(
                Some(send_initial.version()),
                A2aSerializedMapperSnapshot::from_mapper(&advanced_send).unwrap(),
            )
            .await
            .unwrap();
        let send_error = send_server
            .handle_send(
                JsonRpcRequest {
                    id: json!("shared-head-send-retry"),
                    method: "SendMessage".into(),
                    params: json!({
                        "tenant": "tenant-a",
                        "message": {
                            "messageId": "shared-head-send-retry",
                            "role": "ROLE_USER",
                            "parts": [{"text": "recovery"}],
                            "metadata": {"complete": true},
                        },
                    }),
                    correlation_id: None,
                    last_event_id: None,
                },
                owner_principal(),
                false,
                CancellationToken::new(),
            )
            .await
            .expect_err("exact SendMessage retry served a stale fast-path result");
        assert!(send_error
            .message
            .contains("durable snapshot head diverged"));

        let (seeded_cancel, cancel_mapping, cancellation_record) =
            seeded_cancellation("shared-head-cancel-retry", A2aTaskState::Working, false);
        let cancel_snapshots = Arc::new(InMemoryA2aMapperSnapshotStore::default());
        cancel_snapshots
            .persist_snapshot(&seeded_cancel)
            .await
            .unwrap();
        let cancel_initial = cancel_snapshots
            .load_serialized_snapshot()
            .await
            .unwrap()
            .unwrap();
        let cancel_server = Arc::new(
            A2aHttpJsonRpcServer::new_owned(
                seeded_cancel.clone(),
                cancel_snapshots.clone(),
                Arc::new(InMemoryA2aEventStore::default()),
                Arc::new(TestAuthenticator),
                Arc::new(CompletingHost::default()),
                agent_card(),
                A2aHttpConfig::default(),
            )
            .unwrap(),
        );
        cancel_server.initialize_snapshot_store().await.unwrap();
        let mut advanced_cancel = seeded_cancel;
        advanced_cancel
            .mark_cancellation_running(&cancellation_record.cancellation_id)
            .unwrap();
        cancel_snapshots
            .compare_and_swap_snapshot(
                Some(cancel_initial.version()),
                A2aSerializedMapperSnapshot::from_mapper(&advanced_cancel).unwrap(),
            )
            .await
            .unwrap();
        let cancel_error = cancel_server
            .handle_cancel_task(
                JsonRpcRequest {
                    id: json!("shared-head-cancel-retry"),
                    method: "CancelTask".into(),
                    params: json!({"tenant": "tenant-a", "id": cancel_mapping.task_id}),
                    correlation_id: None,
                    last_event_id: None,
                },
                owner_principal(),
                CancellationIngress::Public("127.0.0.1".parse().unwrap()),
                CancellationToken::new(),
            )
            .await
            .expect_err("exact CancelTask retry served a stale fast-path result");
        assert!(cancel_error
            .message
            .contains("durable snapshot head diverged"));
    }

    #[tokio::test]
    async fn open_sse_stream_probes_and_fail_stops_after_a_competing_head_advance() {
        let (seeded, mapping) = seeded_task("shared-head-open-stream", A2aTaskState::Working);
        let snapshots = Arc::new(InMemoryA2aMapperSnapshotStore::default());
        snapshots.persist_snapshot(&seeded).await.unwrap();
        let config = A2aHttpConfig {
            stream_idle_timeout: Duration::from_millis(500),
            ..A2aHttpConfig::default()
        };
        let (address, server, _handle, task) = start_server_with_dependencies(
            config,
            Arc::new(Mutex::new(seeded)),
            snapshots.clone(),
            Arc::new(InMemoryA2aEventStore::default()),
            Arc::new(CompletingHost::default()),
            agent_card(),
        )
        .await;
        timeout(Duration::from_secs(1), async {
            while !server.is_ready() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("server never became ready");

        let subscribe_body = json!({
            "jsonrpc": "2.0",
            "id": "shared-head-open-stream",
            "method": "SubscribeToTask",
            "params": {"tenant": "tenant-a", "id": mapping.task_id},
        })
        .to_string();
        let mut stream = TcpStream::connect(address).await.unwrap();
        stream
            .write_all(&request(
                "POST",
                "/a2a",
                &rpc_headers("text/event-stream"),
                &subscribe_body,
            ))
            .await
            .unwrap();
        let mut initial_bytes = Vec::new();
        timeout(Duration::from_secs(1), async {
            loop {
                let mut chunk = [0_u8; 4096];
                let read = stream.read(&mut chunk).await.unwrap();
                assert!(read > 0, "SSE stream closed before its initial event");
                initial_bytes.extend_from_slice(&chunk[..read]);
                let initial = String::from_utf8_lossy(&initial_bytes);
                if initial.contains("HTTP/1.1 200") && initial.contains("data:") {
                    break;
                }
            }
        })
        .await
        .expect("SSE initial event was not written");

        let current = snapshots.load_serialized_snapshot().await.unwrap().unwrap();
        let mut advanced = current.decode().unwrap();
        advanced
            .transition_task(
                &mapping.task_id,
                A2aTaskState::InputRequired,
                Some("advanced while SSE was open".into()),
            )
            .unwrap();
        snapshots
            .compare_and_swap_snapshot(
                Some(current.version()),
                A2aSerializedMapperSnapshot::from_mapper(&advanced).unwrap(),
            )
            .await
            .unwrap();

        let mut tail = Vec::new();
        timeout(Duration::from_secs(2), stream.read_to_end(&mut tail))
            .await
            .expect("open SSE stream did not close after durable-head divergence")
            .unwrap();
        let serve_error = timeout(Duration::from_secs(2), task)
            .await
            .expect("SSE durable-head divergence did not stop the listener")
            .unwrap()
            .unwrap_err();
        assert!(serve_error
            .message
            .contains("durable snapshot head diverged"));
        assert!(!server.is_ready());
    }

    #[tokio::test]
    async fn final_probe_error_fail_stops_listener_and_prevents_stale_reads() {
        let (mut seeded, _) = seeded_task("final-probe-seed", A2aTaskState::Working);
        for event in seeded.pending_events() {
            seeded.mark_event_settled(&event.event_id).unwrap();
        }
        let snapshots = Arc::new(FinalProbeFailureSnapshotStore::default());
        snapshots.inner.persist_snapshot(&seeded).await.unwrap();
        let initial_task_count = seeded.tasks().len();
        let (address, server, _handle, task) = start_server_with_dependencies(
            A2aHttpConfig::default(),
            Arc::new(Mutex::new(seeded)),
            snapshots.clone(),
            Arc::new(InMemoryA2aEventStore::default()),
            Arc::new(CompletingHost::default()),
            agent_card(),
        )
        .await;
        timeout(Duration::from_secs(1), async {
            while !server.is_ready() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("server never became ready");

        let response = raw_http(
            address,
            request(
                "POST",
                "/a2a",
                &rpc_headers("application/json"),
                &send_body("final-probe-candidate", true, None),
            ),
        )
        .await;
        assert!(
            !response.starts_with("HTTP/1.1 200"),
            "ambiguous commit unexpectedly returned a successful response"
        );
        let serve_error = timeout(Duration::from_secs(2), task)
            .await
            .expect("fail-stopped listener did not exit")
            .unwrap()
            .unwrap_err();
        assert!(serve_error.message.contains("probe failure"));
        assert!(!server.is_ready());
        assert_eq!(
            server.mapper_snapshot().await.tasks().len(),
            initial_task_count
        );
        assert_eq!(
            snapshots.inner.load_snapshot().await.unwrap().tasks().len(),
            initial_task_count + 1
        );
        assert!(timeout(Duration::from_secs(1), TcpStream::connect(address))
            .await
            .unwrap()
            .is_err());
    }

    #[tokio::test]
    async fn get_waits_behind_unresolved_snapshot_commit() {
        let (mut seeded, mapping) = seeded_task("read-barrier-seed", A2aTaskState::Working);
        for event in seeded.pending_events() {
            seeded.mark_event_settled(&event.event_id).unwrap();
        }
        let snapshots = Arc::new(AppliedBlockingSnapshotStore::default());
        snapshots.initialize(&seeded).await.unwrap();
        let (address, server, handle, task) = start_server_with_dependencies(
            A2aHttpConfig::default(),
            Arc::new(Mutex::new(seeded)),
            snapshots.clone(),
            Arc::new(InMemoryA2aEventStore::default()),
            Arc::new(CompletingHost::default()),
            agent_card(),
        )
        .await;
        timeout(Duration::from_secs(1), async {
            while !server.is_ready() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        snapshots
            .block_after_next_apply
            .store(true, Ordering::SeqCst);
        let send = tokio::spawn(raw_http(
            address,
            request(
                "POST",
                "/a2a",
                &rpc_headers("application/json"),
                &send_body("read-barrier-candidate", true, None),
            ),
        ));
        timeout(Duration::from_secs(1), snapshots.entered.notified())
            .await
            .expect("snapshot CAS was not applied");
        let get_body = json!({
            "jsonrpc": "2.0",
            "id": "read-barrier-get",
            "method": "GetTask",
            "params": {"tenant": "tenant-a", "id": mapping.task_id}
        })
        .to_string();
        let get = tokio::spawn(raw_http(
            address,
            request("POST", "/a2a", &rpc_headers("application/json"), &get_body),
        ));
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !get.is_finished(),
            "GetTask crossed an unresolved CAS boundary"
        );
        snapshots.release.notify_one();
        let _ = send.await.unwrap();
        let get = get.await.unwrap();
        assert_eq!(body(&get)["result"]["id"], mapping.task_id);

        handle.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn snapshot_store_io_does_not_hold_the_live_mapper_lock() {
        let (seeded, mapping) = seeded_task("snapshot-io-lock", A2aTaskState::Working);
        let snapshots = Arc::new(BlockingSnapshotStore::default());
        snapshots.initialize(&seeded).await.unwrap();
        snapshots.block_next.store(true, Ordering::SeqCst);
        let server = Arc::new(
            A2aHttpJsonRpcServer::new(
                Arc::new(Mutex::new(seeded)),
                snapshots.clone(),
                Arc::new(InMemoryA2aEventStore::default()),
                Arc::new(TestAuthenticator),
                Arc::new(CompletingHost::default()),
                agent_card(),
                A2aHttpConfig::default(),
            )
            .unwrap(),
        );
        let transition_server = server.clone();
        let task_id = mapping.task_id.clone();
        let transition = tokio::spawn(async move {
            transition_server
                .transition_task(&task_id, A2aTaskState::InputRequired, Some("input".into()))
                .await
        });
        timeout(Duration::from_secs(1), snapshots.entered.notified())
            .await
            .expect("snapshot store was not entered");

        let visible = timeout(Duration::from_millis(100), server.mapper_snapshot())
            .await
            .expect("snapshot I/O retained the live mapper lock");
        assert_eq!(
            visible.tasks()[&mapping.task_id].state,
            A2aTaskState::Working
        );
        snapshots.release.notify_one();
        transition.await.unwrap().unwrap();
        assert_eq!(
            server.mapper_snapshot().await.tasks()[&mapping.task_id].state,
            A2aTaskState::InputRequired
        );
    }

    #[tokio::test]
    async fn snapshot_ack_loss_is_resolved_by_definitive_exact_version_probe() {
        let (seeded, mapping) = seeded_task("snapshot-ack-loss", A2aTaskState::Working);
        let snapshots = Arc::new(AckLossSnapshotStore::default());
        snapshots.initialize(&seeded).await.unwrap();
        snapshots.fail_after_apply(2);
        let server = A2aHttpJsonRpcServer::new_owned(
            seeded,
            snapshots.clone(),
            Arc::new(InMemoryA2aEventStore::default()),
            Arc::new(TestAuthenticator),
            Arc::new(CompletingHost::default()),
            agent_card(),
            A2aHttpConfig::default(),
        )
        .unwrap();

        server
            .transition_task(
                &mapping.task_id,
                A2aTaskState::InputRequired,
                Some("input".into()),
            )
            .await
            .unwrap();
        assert_eq!(snapshots.injected.load(Ordering::SeqCst), 2);
        assert_eq!(snapshots.errors_remaining.load(Ordering::SeqCst), 0);
        assert_eq!(
            snapshots.inner.load_snapshot().await.unwrap(),
            server.mapper_snapshot().await
        );
    }

    #[tokio::test]
    async fn definite_pre_apply_snapshot_error_does_not_fail_stop_later_writes() {
        let (seeded, mapping) = seeded_task("snapshot-definite-error", A2aTaskState::Working);
        let snapshots = Arc::new(FailNextSnapshotStore::default());
        snapshots.initialize(&seeded).await.unwrap();
        snapshots.fail_next_commit();
        let server = A2aHttpJsonRpcServer::new_owned(
            seeded.clone(),
            snapshots.clone(),
            Arc::new(InMemoryA2aEventStore::default()),
            Arc::new(TestAuthenticator),
            Arc::new(CompletingHost::default()),
            agent_card(),
            A2aHttpConfig::default(),
        )
        .unwrap();

        let first = server
            .transition_task(
                &mapping.task_id,
                A2aTaskState::InputRequired,
                Some("first".into()),
            )
            .await
            .unwrap_err();
        assert!(first.message.contains("before durable apply"));
        assert_eq!(server.mapper_snapshot().await, seeded);
        assert_eq!(snapshots.inner.load_snapshot().await.unwrap(), seeded);

        server
            .transition_task(
                &mapping.task_id,
                A2aTaskState::InputRequired,
                Some("second".into()),
            )
            .await
            .unwrap();
        assert_eq!(
            server.mapper_snapshot().await.tasks()[&mapping.task_id].state,
            A2aTaskState::InputRequired
        );
    }

    #[tokio::test]
    async fn definite_snapshot_error_with_competing_head_fail_stops_reads_and_listener() {
        let (mut seeded, mapping) =
            seeded_task("snapshot-definite-competing", A2aTaskState::Working);
        for event in seeded.pending_events() {
            seeded.mark_event_settled(&event.event_id).unwrap();
        }
        let snapshots = Arc::new(CompetingAdvanceThenDefiniteStore::default());
        snapshots
            .initialize(&seeded, &mapping.task_id)
            .await
            .unwrap();
        let (address, server, _handle, task) = start_server_with_dependencies(
            A2aHttpConfig::default(),
            Arc::new(Mutex::new(seeded.clone())),
            snapshots.clone(),
            Arc::new(InMemoryA2aEventStore::default()),
            Arc::new(CompletingHost::default()),
            agent_card(),
        )
        .await;
        snapshots.inject_next();

        let error = server
            .transition_task(
                &mapping.task_id,
                A2aTaskState::InputRequired,
                Some("losing writer".into()),
            )
            .await
            .unwrap_err();
        assert!(error.message.contains("durable head diverged"));
        let serve_error = timeout(Duration::from_secs(2), task)
            .await
            .expect("competing durable head did not stop the listener")
            .unwrap()
            .unwrap_err();
        assert!(serve_error.message.contains("durable head diverged"));
        assert!(!server.is_ready());
        assert_eq!(server.mapper_snapshot().await, seeded);
        assert_eq!(
            snapshots.inner.load_snapshot().await.unwrap().tasks()[&mapping.task_id].state,
            A2aTaskState::Failed
        );

        let read_error = server
            .handle_get_task(
                JsonRpcRequest {
                    id: json!("stale-read"),
                    method: "GetTask".into(),
                    params: json!({"tenant": "tenant-a", "id": mapping.task_id}),
                    correlation_id: None,
                    last_event_id: None,
                },
                owner_principal(),
            )
            .await
            .unwrap_err();
        assert!(read_error.message.contains("durable head diverged"));
        assert!(timeout(Duration::from_secs(1), TcpStream::connect(address))
            .await
            .unwrap()
            .is_err());
    }

    #[tokio::test]
    async fn caller_abort_after_store_apply_still_installs_the_same_live_candidate() {
        let (seeded, mapping) = seeded_task("snapshot-caller-abort", A2aTaskState::Working);
        let snapshots = Arc::new(AppliedBlockingSnapshotStore::default());
        snapshots.initialize(&seeded).await.unwrap();
        snapshots
            .block_after_next_apply
            .store(true, Ordering::SeqCst);
        let server = Arc::new(
            A2aHttpJsonRpcServer::new_owned(
                seeded,
                snapshots.clone(),
                Arc::new(InMemoryA2aEventStore::default()),
                Arc::new(TestAuthenticator),
                Arc::new(CompletingHost::default()),
                agent_card(),
                A2aHttpConfig::default(),
            )
            .unwrap(),
        );
        let transition_server = server.clone();
        let task_id = mapping.task_id.clone();
        let transition = tokio::spawn(async move {
            transition_server
                .transition_task(&task_id, A2aTaskState::InputRequired, Some("input".into()))
                .await
        });
        timeout(Duration::from_secs(1), snapshots.entered.notified())
            .await
            .expect("snapshot CAS was not applied");
        transition.abort();
        let _ = transition.await;
        snapshots.release.notify_one();

        timeout(Duration::from_secs(1), async {
            loop {
                let live = server.mapper_snapshot().await;
                let stored = snapshots.inner.load_snapshot().await.unwrap();
                if live == stored
                    && live.tasks()[&mapping.task_id].state == A2aTaskState::InputRequired
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("detached snapshot commit did not converge live and durable state");
    }

    #[tokio::test]
    async fn shutdown_joins_detached_commit_after_request_abort() {
        let (mut seeded, mapping) = seeded_task("snapshot-shutdown-join", A2aTaskState::Working);
        for event in seeded.pending_events() {
            seeded.mark_event_settled(&event.event_id).unwrap();
        }
        let snapshots = Arc::new(AppliedBlockingSnapshotStore::default());
        snapshots.initialize(&seeded).await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let cancellation = CancellationToken::new();
        let handle = cancellation.handle();
        let server = Arc::new(
            A2aHttpJsonRpcServer::new_owned(
                seeded,
                snapshots.clone(),
                Arc::new(InMemoryA2aEventStore::default()),
                Arc::new(TestAuthenticator),
                Arc::new(CompletingHost::default()),
                agent_card(),
                A2aHttpConfig::default(),
            )
            .unwrap(),
        );
        let serve = tokio::spawn(server.clone().serve(listener, cancellation));
        timeout(Duration::from_secs(1), async {
            while !server.is_ready() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        snapshots
            .block_after_next_apply
            .store(true, Ordering::SeqCst);
        let mutation_server = server.clone();
        let task_id = mapping.task_id.clone();
        let mutation = tokio::spawn(async move {
            mutation_server
                .transition_task(&task_id, A2aTaskState::InputRequired, Some("input".into()))
                .await
        });
        timeout(Duration::from_secs(1), snapshots.entered.notified())
            .await
            .expect("snapshot CAS was not applied");
        mutation.abort();
        let _ = mutation.await;
        handle.cancel();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !serve.is_finished(),
            "server returned before its detached snapshot commit completed"
        );
        snapshots.release.notify_one();
        timeout(Duration::from_secs(2), serve)
            .await
            .expect("server did not join detached snapshot commit")
            .unwrap()
            .unwrap();
        assert_eq!(
            server.mapper_snapshot().await,
            snapshots.inner.load_snapshot().await.unwrap()
        );
        assert_eq!(
            server.mapper_snapshot().await.tasks()[&mapping.task_id].state,
            A2aTaskState::InputRequired
        );
    }

    #[tokio::test]
    async fn concurrent_snapshot_writers_serialize_without_lost_updates() {
        let (mut seeded, first) = seeded_task("snapshot-writer-one", A2aTaskState::Working);
        let second =
            prepare_seeded_message_for(&mut seeded, "snapshot-writer-two", &owner_principal());
        let second_dispatch = seeded
            .dispatch_for_message("snapshot-writer-two", &owner_principal())
            .unwrap()
            .dispatch_id
            .clone();
        seeded.mark_dispatch_settled(&second_dispatch).unwrap();
        let snapshots = Arc::new(InMemoryA2aMapperSnapshotStore::default());
        snapshots.persist_snapshot(&seeded).await.unwrap();
        let server = Arc::new(
            A2aHttpJsonRpcServer::new_owned(
                seeded,
                snapshots.clone(),
                Arc::new(InMemoryA2aEventStore::default()),
                Arc::new(TestAuthenticator),
                Arc::new(CompletingHost::default()),
                agent_card(),
                A2aHttpConfig::default(),
            )
            .unwrap(),
        );
        let first_server = server.clone();
        let first_id = first.task_id.clone();
        let first_writer = tokio::spawn(async move {
            first_server
                .persist_mapper_mutation(|candidate| {
                    candidate.transition_task(&first_id, A2aTaskState::Completed, None)
                })
                .await
        });
        let second_server = server.clone();
        let second_id = second.task_id.clone();
        let second_writer = tokio::spawn(async move {
            second_server
                .persist_mapper_mutation(|candidate| {
                    candidate.transition_task(&second_id, A2aTaskState::Completed, None)
                })
                .await
        });
        first_writer.await.unwrap().unwrap();
        second_writer.await.unwrap().unwrap();

        let live = server.mapper_snapshot().await;
        assert_eq!(live.tasks()[&first.task_id].state, A2aTaskState::Completed);
        assert_eq!(live.tasks()[&second.task_id].state, A2aTaskState::Completed);
        assert_eq!(snapshots.load_snapshot().await.unwrap(), live);
    }

    #[tokio::test]
    async fn oversized_new_event_is_rejected_before_snapshot_acceptance() {
        let (seeded, mapping) = seeded_task("snapshot-event-cap", A2aTaskState::Working);
        let snapshots = Arc::new(InMemoryA2aMapperSnapshotStore::default());
        snapshots.persist_snapshot(&seeded).await.unwrap();
        let config = A2aHttpConfig {
            max_event_bytes: 32,
            max_retained_event_bytes: 32,
            ..A2aHttpConfig::default()
        };
        let server = A2aHttpJsonRpcServer::new_owned(
            seeded.clone(),
            snapshots.clone(),
            Arc::new(InMemoryA2aEventStore::default()),
            Arc::new(TestAuthenticator),
            Arc::new(CompletingHost::default()),
            agent_card(),
            config,
        )
        .unwrap();
        let error = server
            .transition_task(
                &mapping.task_id,
                A2aTaskState::InputRequired,
                Some("oversized-status".repeat(32)),
            )
            .await
            .unwrap_err();
        assert!(error.message.contains("event exceeds"));
        assert_eq!(server.mapper_snapshot().await, seeded);
        assert_eq!(snapshots.load_snapshot().await.unwrap(), seeded);
    }
}
