//! Concrete MCP stdio and Streamable HTTP listener adapters.
//!
//! The listeners provide framing, session/authentication binding, bounded HTTP parsing, SSE replay
//! and graceful shutdown. Authorized runtime work is delegated to [`McpDispatchHost`].

use super::{
    McpJsonRpcDispatch, McpJsonRpcServer, McpOutboundMessage, McpServerAction, ProtocolError,
    ProtocolErrorCode, ProtocolPrincipal, ProtocolResult, MCP_SERVER_CONTRACT_REVISION,
};
use crate::CancellationToken;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{
    AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader,
};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio::time::timeout;

pub const DEFAULT_MCP_STDIO_FRAME_BYTES: usize = 1024 * 1024;
pub const DEFAULT_MCP_HTTP_HEADER_BYTES: usize = 32 * 1024;
pub const DEFAULT_MCP_HTTP_BODY_BYTES: usize = 1024 * 1024;
pub const DEFAULT_MCP_HTTP_CONCURRENCY: usize = 64;
pub const DEFAULT_MCP_HTTP_RATE_PER_MINUTE: u32 = 600;
pub const DEFAULT_MCP_HTTP_RATE_BUCKETS: usize = 4096;
pub const DEFAULT_MCP_SSE_RETAINED_EVENTS: usize = 1024;
pub const DEFAULT_MCP_SSE_EVENT_BYTES: usize = 1024 * 1024;
pub const DEFAULT_MCP_SSE_RESPONSE_BYTES: usize = 8 * 1024 * 1024;
pub const DEFAULT_MCP_HTTP_MAX_SESSIONS: usize = 4096;
pub const DEFAULT_MCP_HTTP_SESSION_TTL_MS: u64 = 86_400_000;

#[async_trait]
pub trait McpDispatchHost: Send + Sync {
    /// Execute or schedule the governed action and return protocol messages produced by completion.
    /// Implementations must use the stable task id as their idempotency/reconciliation key.
    /// A `CancelTask` action is stricter: `Ok` confirms that the underlying activity has stopped;
    /// scheduling-only or ambiguous outcomes must return an error so the task remains reconcilable.
    async fn handle(
        &self,
        server: &McpJsonRpcServer,
        dispatch: &McpJsonRpcDispatch,
    ) -> ProtocolResult<Vec<McpOutboundMessage>>;
}

/// Host that rejects executable actions, useful for metadata-only MCP servers.
#[derive(Debug, Default)]
pub struct RejectingMcpDispatchHost;

#[async_trait]
impl McpDispatchHost for RejectingMcpDispatchHost {
    async fn handle(
        &self,
        _server: &McpJsonRpcServer,
        dispatch: &McpJsonRpcDispatch,
    ) -> ProtocolResult<Vec<McpOutboundMessage>> {
        if dispatch
            .governed_action
            .as_ref()
            .is_some_and(|decision| decision.action().is_some())
        {
            return Err(ProtocolError::new(
                ProtocolErrorCode::Conflict,
                "MCP host action handler is not configured",
            ));
        }
        Ok(Vec::new())
    }
}

#[derive(Debug, Clone)]
pub struct McpStdioConfig {
    pub connection_id: String,
    pub max_frame_bytes: usize,
    pub operation_timeout: Duration,
}

impl Default for McpStdioConfig {
    fn default() -> Self {
        Self {
            connection_id: "mcp-stdio".into(),
            max_frame_bytes: DEFAULT_MCP_STDIO_FRAME_BYTES,
            operation_timeout: Duration::from_secs(60),
        }
    }
}

/// Serve newline-delimited MCP JSON-RPC over any async reader/writer pair.
pub async fn serve_mcp_stdio<R, W>(
    server: Arc<McpJsonRpcServer>,
    principal: ProtocolPrincipal,
    host: Arc<dyn McpDispatchHost>,
    reader: R,
    mut writer: W,
    config: McpStdioConfig,
    cancellation: CancellationToken,
) -> ProtocolResult<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    if config.connection_id.is_empty() || config.max_frame_bytes == 0 {
        return Err(ProtocolError::invalid("invalid MCP stdio configuration"));
    }
    let mut reader = BufReader::new(reader);
    let result = async {
        loop {
            let frame = tokio::select! {
                () = cancellation.cancelled() => break,
                frame = read_bounded_line(&mut reader, config.max_frame_bytes) => frame?,
            };
            let Some(frame) = frame else {
                break;
            };
            if frame.iter().all(u8::is_ascii_whitespace) {
                continue;
            }
            let dispatch = server.handle(&config.connection_id, Some(&principal), &frame)?;
            if let Some(response) = &dispatch.response {
                write_json_line(&mut writer, response).await?;
            }
            for outbound in &dispatch.outbound {
                if outbound.connection_id == config.connection_id {
                    write_json_line(&mut writer, &outbound.message).await?;
                }
            }
            let produced = tokio::select! {
                () = cancellation.cancelled() => break,
                result = execute_host_dispatch(&server, &host, &dispatch, config.operation_timeout) => result?,
            };
            for outbound in produced {
                if outbound.connection_id == config.connection_id {
                    write_json_line(&mut writer, &outbound.message).await?;
                }
            }
            writer
                .flush()
                .await
                .map_err(|error| protocol_io("flush MCP stdio", error))?;
        }
        Ok::<(), ProtocolError>(())
    }
    .await;
    let cleanup = teardown_connection(
        &server,
        &host,
        &config.connection_id,
        &principal,
        config.operation_timeout,
    )
    .await;
    let shutdown = writer
        .shutdown()
        .await
        .map_err(|error| protocol_io("shutdown MCP stdio", error));
    result.and(cleanup).and(shutdown)
}

async fn execute_host_dispatch(
    server: &McpJsonRpcServer,
    host: &Arc<dyn McpDispatchHost>,
    dispatch: &McpJsonRpcDispatch,
    operation_timeout: Duration,
) -> ProtocolResult<Vec<McpOutboundMessage>> {
    let cancellation_task = dispatch
        .governed_action
        .as_ref()
        .and_then(|decision| decision.action())
        .and_then(|action| match action {
            McpServerAction::CancelTask { task } => Some(task.task_id.clone()),
            _ => None,
        });
    let handled = timeout(operation_timeout, host.handle(server, dispatch)).await;
    let mut produced = match handled {
        Ok(Ok(produced)) => produced,
        Ok(Err(error)) => {
            if let Some(task_id) = cancellation_task.as_deref() {
                server.mark_cancellation_reconcile_required(task_id, error.message.clone())?;
            }
            return Err(error);
        }
        Err(_) => {
            let error =
                ProtocolError::new(ProtocolErrorCode::Cancelled, "MCP host action timed out");
            if let Some(task_id) = cancellation_task.as_deref() {
                server.mark_cancellation_reconcile_required(task_id, error.message.clone())?;
            }
            return Err(error);
        }
    };
    if let Some(task_id) = cancellation_task {
        produced.extend(server.confirm_cancellation(&task_id)?);
    }
    server.validate_outbound_messages(&produced)?;
    Ok(produced)
}

async fn teardown_connection(
    server: &McpJsonRpcServer,
    host: &Arc<dyn McpDispatchHost>,
    connection_id: &str,
    principal: &ProtocolPrincipal,
    operation_timeout: Duration,
) -> ProtocolResult<()> {
    let decisions = match server.terminate_connection(connection_id, Some(principal)) {
        Ok(decisions) => decisions,
        Err(error) if error.code == ProtocolErrorCode::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    for decision in decisions {
        let dispatch = McpJsonRpcDispatch {
            response: None,
            outbound: Vec::new(),
            governed_action: Some(decision),
            pending: None,
        };
        execute_host_dispatch(server, host, &dispatch, operation_timeout).await?;
    }
    server.finalize_connection_termination(connection_id, Some(principal))
}

async fn read_bounded_line<R>(reader: &mut R, max_bytes: usize) -> ProtocolResult<Option<Vec<u8>>>
where
    R: AsyncBufRead + Unpin,
{
    let mut output = Vec::new();
    loop {
        let available = reader
            .fill_buf()
            .await
            .map_err(|error| protocol_io("read MCP stdio", error))?;
        if available.is_empty() {
            return Ok((!output.is_empty()).then_some(output));
        }
        let take = available
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(available.len(), |index| index + 1);
        if output.len().saturating_add(take) > max_bytes {
            return Err(ProtocolError::invalid(
                "MCP stdio frame exceeds the configured byte limit",
            ));
        }
        output.extend_from_slice(&available[..take]);
        reader.consume(take);
        if output.last() == Some(&b'\n') {
            if output.get(output.len().saturating_sub(2)) == Some(&b'\r') {
                output.truncate(output.len() - 2);
            } else {
                output.pop();
            }
            return Ok(Some(output));
        }
    }
}

async fn write_json_line<W>(writer: &mut W, value: &Value) -> ProtocolResult<()>
where
    W: AsyncWrite + Unpin,
{
    let encoded = serde_json::to_vec(value)
        .map_err(|error| ProtocolError::invalid(format!("MCP JSON encoding failed: {error}")))?;
    writer
        .write_all(&encoded)
        .await
        .map_err(|error| protocol_io("write MCP stdio", error))?;
    writer
        .write_all(b"\n")
        .await
        .map_err(|error| protocol_io("write MCP stdio delimiter", error))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpHttpSession {
    pub session_id: String,
    pub connection_id: String,
    pub subject: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<String>,
    #[serde(default)]
    pub created_at_unix_ms: u64,
    #[serde(default)]
    pub expires_at_unix_ms: u64,
}

impl McpHttpSession {
    fn matches(&self, principal: &ProtocolPrincipal) -> bool {
        self.subject == principal.subject && self.tenant_id == principal.tenant_id
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpSseEvent {
    pub event_id: String,
    pub sequence: u64,
    pub data: Value,
}

pub trait McpHttpSessionStore: Send + Sync {
    fn create_session(&self, session: &McpHttpSession, max_sessions: usize) -> ProtocolResult<()>;
    fn load_session(&self, session_id: &str) -> ProtocolResult<Option<McpHttpSession>>;
    fn delete_session(&self, session_id: &str) -> ProtocolResult<bool>;
    fn purge_expired(&self, now_unix_ms: u64) -> ProtocolResult<usize>;
    fn append_event(
        &self,
        session_id: &str,
        data: &Value,
        max_events: usize,
        max_event_bytes: usize,
    ) -> ProtocolResult<McpSseEvent>;
    fn replay_events(
        &self,
        session_id: &str,
        last_event_id: Option<&str>,
        limit: usize,
        max_bytes: usize,
    ) -> ProtocolResult<Vec<McpSseEvent>>;
}

#[derive(Debug, Default)]
pub struct InMemoryMcpHttpSessionStore {
    state: Mutex<InMemoryHttpState>,
}

#[derive(Debug, Default)]
struct InMemoryHttpState {
    sessions: BTreeMap<String, McpHttpSession>,
    events: BTreeMap<String, VecDeque<McpSseEvent>>,
    next_sequence: BTreeMap<String, u64>,
}

impl McpHttpSessionStore for InMemoryMcpHttpSessionStore {
    fn create_session(&self, session: &McpHttpSession, max_sessions: usize) -> ProtocolResult<()> {
        let mut state = self.state.lock().map_err(|_| store_lock_error())?;
        let expired: Vec<_> = state
            .sessions
            .iter()
            .filter(|(_, existing)| {
                existing.expires_at_unix_ms == 0
                    || session.created_at_unix_ms >= existing.expires_at_unix_ms
            })
            .map(|(session_id, _)| session_id.clone())
            .collect();
        for session_id in expired {
            state.sessions.remove(&session_id);
            state.events.remove(&session_id);
            state.next_sequence.remove(&session_id);
        }
        if state.sessions.len() >= max_sessions.max(1) {
            return Err(ProtocolError::conflict(
                "MCP HTTP session capacity exhausted",
            ));
        }
        if state.sessions.contains_key(&session.session_id)
            || state
                .sessions
                .values()
                .any(|existing| existing.connection_id == session.connection_id)
        {
            return Err(ProtocolError::conflict("MCP HTTP session already exists"));
        }
        state
            .sessions
            .insert(session.session_id.clone(), session.clone());
        state
            .events
            .insert(session.session_id.clone(), VecDeque::new());
        state.next_sequence.insert(session.session_id.clone(), 1);
        Ok(())
    }

    fn load_session(&self, session_id: &str) -> ProtocolResult<Option<McpHttpSession>> {
        self.state
            .lock()
            .map_err(|_| store_lock_error())
            .map(|state| state.sessions.get(session_id).cloned())
    }

    fn delete_session(&self, session_id: &str) -> ProtocolResult<bool> {
        let mut state = self.state.lock().map_err(|_| store_lock_error())?;
        let existed = state.sessions.remove(session_id).is_some();
        state.events.remove(session_id);
        state.next_sequence.remove(session_id);
        Ok(existed)
    }

    fn purge_expired(&self, now_unix_ms: u64) -> ProtocolResult<usize> {
        let mut state = self.state.lock().map_err(|_| store_lock_error())?;
        let expired: Vec<_> = state
            .sessions
            .iter()
            .filter(|(_, session)| {
                session.expires_at_unix_ms == 0 || now_unix_ms >= session.expires_at_unix_ms
            })
            .map(|(session_id, _)| session_id.clone())
            .collect();
        for session_id in &expired {
            state.sessions.remove(session_id);
            state.events.remove(session_id);
            state.next_sequence.remove(session_id);
        }
        Ok(expired.len())
    }

    fn append_event(
        &self,
        session_id: &str,
        data: &Value,
        max_events: usize,
        max_event_bytes: usize,
    ) -> ProtocolResult<McpSseEvent> {
        let encoded = serde_json::to_vec(data)
            .map_err(|error| ProtocolError::invalid(format!("invalid SSE event: {error}")))?;
        if encoded.len() > max_event_bytes.max(1) {
            return Err(ProtocolError::invalid(
                "MCP SSE event exceeds the configured byte limit",
            ));
        }
        let mut state = self.state.lock().map_err(|_| store_lock_error())?;
        if !state.sessions.contains_key(session_id) {
            return Err(ProtocolError::not_found(
                "MCP HTTP session is not registered",
            ));
        }
        let sequence = *state.next_sequence.get(session_id).unwrap_or(&1);
        let sequence_text = sequence.to_string();
        let event = McpSseEvent {
            event_id: crate::durability::stable_id("mcp_sse", &[session_id, &sequence_text]),
            sequence,
            data: data.clone(),
        };
        let events = state.events.entry(session_id.to_owned()).or_default();
        events.push_back(event.clone());
        while events.len() > max_events.max(1) {
            events.pop_front();
        }
        state
            .next_sequence
            .insert(session_id.to_owned(), sequence.saturating_add(1));
        Ok(event)
    }

    fn replay_events(
        &self,
        session_id: &str,
        last_event_id: Option<&str>,
        limit: usize,
        max_bytes: usize,
    ) -> ProtocolResult<Vec<McpSseEvent>> {
        let state = self.state.lock().map_err(|_| store_lock_error())?;
        if !state.sessions.contains_key(session_id) {
            return Err(ProtocolError::not_found(
                "MCP HTTP session is not registered",
            ));
        }
        let events = state.events.get(session_id);
        let start = match (last_event_id, events) {
            (None, _) => 0,
            (Some(last), Some(events)) => events
                .iter()
                .position(|event| event.event_id == last)
                .map(|index| index + 1)
                .ok_or_else(|| {
                    ProtocolError::invalid("Last-Event-ID is not retained for this MCP session")
                })?,
            (Some(_), None) => {
                return Err(ProtocolError::invalid(
                    "Last-Event-ID is not retained for this MCP session",
                ))
            }
        };
        let mut retained = Vec::new();
        let mut retained_bytes = 0_usize;
        for event in events
            .into_iter()
            .flat_map(|events| events.iter().skip(start).take(limit.max(1)))
        {
            let event_bytes = serde_json::to_vec(&event.data)
                .map_err(|error| ProtocolError::invalid(format!("invalid SSE event: {error}")))?
                .len();
            if retained_bytes.saturating_add(event_bytes) > max_bytes.max(1) {
                if retained.is_empty() {
                    return Err(ProtocolError::new(
                        ProtocolErrorCode::Conflict,
                        "persisted MCP SSE event exceeds the replay byte limit",
                    ));
                }
                break;
            }
            retained_bytes = retained_bytes.saturating_add(event_bytes);
            retained.push(event.clone());
        }
        Ok(retained)
    }
}

#[derive(Debug, Clone)]
pub struct McpHttpHeaders {
    values: BTreeMap<String, String>,
}

impl McpHttpHeaders {
    pub fn get(&self, name: &str) -> Option<&str> {
        self.values
            .get(&name.to_ascii_lowercase())
            .map(String::as_str)
    }
}

#[derive(Debug, Clone)]
pub struct McpHttpAuthError {
    pub status: u16,
    pub message: String,
    pub www_authenticate: Option<String>,
}

pub trait McpHttpAuthenticator: Send + Sync {
    fn authenticate(&self, headers: &McpHttpHeaders)
        -> Result<ProtocolPrincipal, McpHttpAuthError>;
}

pub struct StaticBearerMcpAuthenticator {
    token: Vec<u8>,
    principal: ProtocolPrincipal,
    challenge: String,
}

impl StaticBearerMcpAuthenticator {
    pub fn new(
        token: impl AsRef<[u8]>,
        principal: ProtocolPrincipal,
        challenge: impl Into<String>,
    ) -> ProtocolResult<Self> {
        let token = token.as_ref();
        if token.is_empty() || token.len() > 4096 {
            return Err(ProtocolError::invalid("invalid MCP bearer token"));
        }
        let challenge = challenge.into();
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

impl McpHttpAuthenticator for StaticBearerMcpAuthenticator {
    fn authenticate(
        &self,
        headers: &McpHttpHeaders,
    ) -> Result<ProtocolPrincipal, McpHttpAuthError> {
        let supplied = headers
            .get("authorization")
            .and_then(|value| value.strip_prefix("Bearer "))
            .map(str::as_bytes);
        if supplied.is_none_or(|supplied| !constant_time_eq(supplied, &self.token)) {
            return Err(McpHttpAuthError {
                status: 401,
                message: "authentication required".into(),
                www_authenticate: Some(self.challenge.clone()),
            });
        }
        Ok(self.principal.clone())
    }
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let mut difference = left.len() ^ right.len();
    let length = left.len().max(right.len());
    for index in 0..length {
        let left_byte = left.get(index).copied().unwrap_or(0);
        let right_byte = right.get(index).copied().unwrap_or(0);
        difference |= usize::from(left_byte ^ right_byte);
    }
    difference == 0
}

fn accepts_media_type(accept: &str, expected: &str) -> bool {
    accept.split(',').any(|value| {
        value
            .split(';')
            .next()
            .is_some_and(|media_type| media_type.trim().eq_ignore_ascii_case(expected))
    })
}

fn valid_protocol_version(headers: &McpHttpHeaders) -> bool {
    headers.get("mcp-protocol-version") == Some(MCP_SERVER_CONTRACT_REVISION)
}

#[derive(Debug, Clone)]
pub struct McpStreamableHttpConfig {
    pub path: String,
    pub allowed_origins: BTreeSet<String>,
    pub max_header_bytes: usize,
    pub max_body_bytes: usize,
    pub max_concurrency: usize,
    pub max_requests_per_minute: u32,
    pub max_rate_buckets: usize,
    pub max_sse_events: usize,
    pub max_sse_event_bytes: usize,
    pub max_sse_response_bytes: usize,
    pub max_sessions: usize,
    pub session_ttl: Duration,
    pub request_timeout: Duration,
    pub graceful_shutdown_timeout: Duration,
}

impl Default for McpStreamableHttpConfig {
    fn default() -> Self {
        Self {
            path: "/mcp".into(),
            allowed_origins: BTreeSet::new(),
            max_header_bytes: DEFAULT_MCP_HTTP_HEADER_BYTES,
            max_body_bytes: DEFAULT_MCP_HTTP_BODY_BYTES,
            max_concurrency: DEFAULT_MCP_HTTP_CONCURRENCY,
            max_requests_per_minute: DEFAULT_MCP_HTTP_RATE_PER_MINUTE,
            max_rate_buckets: DEFAULT_MCP_HTTP_RATE_BUCKETS,
            max_sse_events: DEFAULT_MCP_SSE_RETAINED_EVENTS,
            max_sse_event_bytes: DEFAULT_MCP_SSE_EVENT_BYTES,
            max_sse_response_bytes: DEFAULT_MCP_SSE_RESPONSE_BYTES,
            max_sessions: DEFAULT_MCP_HTTP_MAX_SESSIONS,
            session_ttl: Duration::from_millis(DEFAULT_MCP_HTTP_SESSION_TTL_MS),
            request_timeout: Duration::from_secs(60),
            graceful_shutdown_timeout: Duration::from_secs(5),
        }
    }
}

impl McpStreamableHttpConfig {
    fn validate(&self) -> ProtocolResult<()> {
        if !self.path.starts_with('/')
            || self.path.contains(['\r', '\n', '?', '#'])
            || self.max_header_bytes == 0
            || self.max_body_bytes == 0
            || self.max_concurrency == 0
            || self.max_requests_per_minute == 0
            || self.max_rate_buckets == 0
            || self.max_sse_events == 0
            || self.max_sse_event_bytes == 0
            || self.max_sse_response_bytes == 0
            || self.max_sessions == 0
            || self.session_ttl.is_zero()
            || self.max_sse_event_bytes.saturating_add(256) > self.max_sse_response_bytes
        {
            return Err(ProtocolError::invalid(
                "invalid MCP Streamable HTTP configuration",
            ));
        }
        if self
            .allowed_origins
            .iter()
            .any(|origin| origin.is_empty() || origin.contains(['\r', '\n']))
        {
            return Err(ProtocolError::invalid("invalid MCP Origin allowlist"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
struct RateBucket {
    minute: u64,
    count: u32,
}

pub struct McpStreamableHttpServer {
    server: Arc<McpJsonRpcServer>,
    sessions: Arc<dyn McpHttpSessionStore>,
    authenticator: Arc<dyn McpHttpAuthenticator>,
    host: Arc<dyn McpDispatchHost>,
    config: McpStreamableHttpConfig,
    rate: Mutex<BTreeMap<IpAddr, RateBucket>>,
}

impl McpStreamableHttpServer {
    pub fn new(
        server: Arc<McpJsonRpcServer>,
        sessions: Arc<dyn McpHttpSessionStore>,
        authenticator: Arc<dyn McpHttpAuthenticator>,
        host: Arc<dyn McpDispatchHost>,
        config: McpStreamableHttpConfig,
    ) -> ProtocolResult<Self> {
        config.validate()?;
        Ok(Self {
            server,
            sessions,
            authenticator,
            host,
            config,
            rate: Mutex::new(BTreeMap::new()),
        })
    }

    /// Serve an already-bound listener until cancellation, then drain bounded in-flight work.
    pub async fn serve(
        self: Arc<Self>,
        listener: TcpListener,
        cancellation: CancellationToken,
    ) -> ProtocolResult<()> {
        let semaphore = Arc::new(Semaphore::new(self.config.max_concurrency));
        let mut tasks = JoinSet::new();
        loop {
            tokio::select! {
                () = cancellation.cancelled() => break,
                completed = tasks.join_next(), if !tasks.is_empty() => {
                    if let Some(Err(error)) = completed {
                        return Err(ProtocolError::new(ProtocolErrorCode::Conflict, format!("MCP HTTP task failed: {error}")));
                    }
                }
                accepted = listener.accept() => {
                    let (mut stream, peer) = accepted.map_err(|error| protocol_io("accept MCP HTTP connection", error))?;
                    let Ok(permit) = semaphore.clone().try_acquire_owned() else {
                        let _ = write_http_response(&mut stream, HttpResponse::text(503, "server busy").with_header("Retry-After", "1")).await;
                        continue;
                    };
                    let server = self.clone();
                    tasks.spawn(async move {
                        let _permit = permit;
                        let response = match timeout(server.config.request_timeout, server.handle_connection(&mut stream, peer)).await {
                            Ok(Ok(response)) => response,
                            Ok(Err(error)) => protocol_http_response(&error),
                            Err(_) => HttpResponse::text(408, "request timeout"),
                        };
                        let _ = write_http_response(&mut stream, response).await;
                        let _ = stream.shutdown().await;
                    });
                }
            }
        }
        let drain = async {
            while let Some(result) = tasks.join_next().await {
                result.map_err(|error| {
                    ProtocolError::new(
                        ProtocolErrorCode::Conflict,
                        format!("MCP HTTP task failed: {error}"),
                    )
                })?;
            }
            Ok::<(), ProtocolError>(())
        };
        if timeout(self.config.graceful_shutdown_timeout, drain)
            .await
            .is_err()
        {
            tasks.abort_all();
        }
        Ok(())
    }

    async fn handle_connection(
        &self,
        stream: &mut TcpStream,
        peer: SocketAddr,
    ) -> ProtocolResult<HttpResponse> {
        if !self.consume_rate(peer.ip())? {
            return Ok(
                HttpResponse::text(429, "rate limit exceeded").with_header("Retry-After", "60")
            );
        }
        let request = match read_http_request(
            stream,
            self.config.max_header_bytes,
            self.config.max_body_bytes,
        )
        .await
        {
            Ok(request) => request,
            Err(error) => return Ok(HttpResponse::text(error.status, &error.message)),
        };
        if request.path != self.config.path {
            return Ok(HttpResponse::text(404, "not found"));
        }
        if let Some(origin) = request.headers.get("origin") {
            if !self.config.allowed_origins.contains(origin) {
                return Ok(HttpResponse::text(403, "origin is not allowed"));
            }
        }
        let principal = match self.authenticator.authenticate(&request.headers) {
            Ok(principal) => principal,
            Err(error) => {
                let mut response = HttpResponse::text(error.status, &error.message);
                if let Some(challenge) = error.www_authenticate {
                    response = response.with_header("WWW-Authenticate", &challenge);
                }
                return Ok(response);
            }
        };
        match request.method.as_str() {
            "POST" => self.handle_post(request, principal).await,
            "GET" => self.handle_get(request, principal),
            "DELETE" => self.handle_delete(request, principal).await,
            _ => Ok(HttpResponse::text(405, "method not allowed")
                .with_header("Allow", "POST, GET, DELETE")),
        }
    }

    async fn handle_post(
        &self,
        request: HttpRequest,
        principal: ProtocolPrincipal,
    ) -> ProtocolResult<HttpResponse> {
        let content_type = request
            .headers
            .get("content-type")
            .unwrap_or_default()
            .split(';')
            .next()
            .unwrap_or_default()
            .trim();
        if !content_type.eq_ignore_ascii_case("application/json") {
            return Ok(HttpResponse::text(
                415,
                "Content-Type must be application/json",
            ));
        }
        let accept = request.headers.get("accept").unwrap_or_default();
        if !accepts_media_type(accept, "application/json")
            || !accepts_media_type(accept, "text/event-stream")
        {
            return Ok(HttpResponse::text(
                406,
                "Accept must include application/json and text/event-stream",
            ));
        }
        let value: Value = match serde_json::from_slice(&request.body) {
            Ok(value) => value,
            Err(_) => return Ok(HttpResponse::json(400, jsonrpc_parse_error())),
        };
        let is_initialize = value.get("method").and_then(Value::as_str) == Some("initialize");
        if !is_initialize && !valid_protocol_version(&request.headers) {
            return Ok(HttpResponse::text(
                400,
                "MCP-Protocol-Version is missing or unsupported",
            ));
        }
        let (session, new_session) = if is_initialize {
            if request.headers.get("mcp-session-id").is_some() {
                return Ok(HttpResponse::text(
                    400,
                    "initialize must not reuse MCP-Session-Id",
                ));
            }
            let session_id = random_session_id()?;
            let now_unix_ms = self.server.now_unix_ms();
            let session_ttl_ms =
                u64::try_from(self.config.session_ttl.as_millis()).unwrap_or(u64::MAX);
            (
                McpHttpSession {
                    connection_id: format!("mcp-http-{session_id}"),
                    session_id,
                    subject: principal.subject.clone(),
                    tenant_id: principal.tenant_id.clone(),
                    created_at_unix_ms: now_unix_ms,
                    expires_at_unix_ms: now_unix_ms.saturating_add(session_ttl_ms),
                },
                true,
            )
        } else {
            (self.bound_session(&request.headers, &principal)?, false)
        };
        let dispatch =
            self.server
                .handle(&session.connection_id, Some(&principal), &request.body)?;
        let initialize_succeeded = dispatch
            .response
            .as_ref()
            .is_some_and(|response| response.get("result").is_some());
        let registered_new_session = new_session && initialize_succeeded;
        if registered_new_session {
            if let Err(error) = self
                .sessions
                .create_session(&session, self.config.max_sessions)
            {
                let _ = teardown_connection(
                    &self.server,
                    &self.host,
                    &session.connection_id,
                    &principal,
                    self.config.request_timeout,
                )
                .await;
                return Err(error);
            }
        }
        let (primary, events) = match self
            .complete_dispatch(&session, &dispatch, &principal)
            .await
        {
            Ok(completed) => completed,
            Err(error) => {
                if registered_new_session {
                    self.rollback_new_session(&session, &principal).await;
                }
                return Err(error);
            }
        };
        for event in events {
            if let Err(error) = self.sessions.append_event(
                &session.session_id,
                &event,
                self.config.max_sse_events,
                self.config.max_sse_event_bytes,
            ) {
                if registered_new_session {
                    self.rollback_new_session(&session, &principal).await;
                }
                return Err(error);
            }
        }
        let mut response = match primary {
            Some(response) => HttpResponse::json(200, response),
            None if value.get("id").is_none() => HttpResponse::empty(202),
            None => HttpResponse::text(504, "MCP request did not complete before timeout"),
        };
        if new_session && initialize_succeeded {
            response = response.with_header("MCP-Session-Id", &session.session_id);
        }
        Ok(response)
    }

    async fn rollback_new_session(&self, session: &McpHttpSession, principal: &ProtocolPrincipal) {
        let _ = self.sessions.delete_session(&session.session_id);
        let _ = teardown_connection(
            &self.server,
            &self.host,
            &session.connection_id,
            principal,
            self.config.request_timeout,
        )
        .await;
    }

    fn handle_get(
        &self,
        request: HttpRequest,
        principal: ProtocolPrincipal,
    ) -> ProtocolResult<HttpResponse> {
        if !valid_protocol_version(&request.headers) {
            return Ok(HttpResponse::text(
                400,
                "MCP-Protocol-Version is missing or unsupported",
            ));
        }
        if !request
            .headers
            .get("accept")
            .is_some_and(|accept| accepts_media_type(accept, "text/event-stream"))
        {
            return Ok(HttpResponse::text(
                406,
                "Accept must include text/event-stream",
            ));
        }
        let session = self.bound_session(&request.headers, &principal)?;
        let events = match self.sessions.replay_events(
            &session.session_id,
            request.headers.get("last-event-id"),
            self.config.max_sse_events,
            self.config.max_sse_response_bytes,
        ) {
            Ok(events) => events,
            Err(error) if error.code == ProtocolErrorCode::InvalidRequest => {
                return Ok(HttpResponse::text(400, &error.message))
            }
            Err(error) => return Err(error),
        };
        let mut body = Vec::new();
        if events.is_empty() {
            body.extend_from_slice(b": keepalive\n\n");
        } else {
            for event in events {
                let data = serde_json::to_string(&event.data).map_err(|error| {
                    ProtocolError::invalid(format!("MCP SSE encoding failed: {error}"))
                })?;
                let frame = format!("id: {}\nevent: message\ndata: {data}\n\n", event.event_id);
                if body.len().saturating_add(frame.len()) > self.config.max_sse_response_bytes {
                    if body.is_empty() {
                        return Err(ProtocolError::new(
                            ProtocolErrorCode::Conflict,
                            "persisted MCP SSE event exceeds the response byte limit",
                        ));
                    }
                    break;
                }
                body.extend_from_slice(frame.as_bytes());
            }
        }
        Ok(HttpResponse::bytes(200, "text/event-stream", body)
            .with_header("Cache-Control", "no-cache")
            .with_header("MCP-Session-Id", &session.session_id))
    }

    async fn handle_delete(
        &self,
        request: HttpRequest,
        principal: ProtocolPrincipal,
    ) -> ProtocolResult<HttpResponse> {
        if !valid_protocol_version(&request.headers) {
            return Ok(HttpResponse::text(
                400,
                "MCP-Protocol-Version is missing or unsupported",
            ));
        }
        let session = self.bound_session(&request.headers, &principal)?;
        teardown_connection(
            &self.server,
            &self.host,
            &session.connection_id,
            &principal,
            self.config.request_timeout,
        )
        .await?;
        self.sessions.delete_session(&session.session_id)?;
        Ok(HttpResponse::empty(204))
    }

    async fn complete_dispatch(
        &self,
        session: &McpHttpSession,
        dispatch: &McpJsonRpcDispatch,
        _principal: &ProtocolPrincipal,
    ) -> ProtocolResult<(Option<Value>, Vec<Value>)> {
        let mut primary = dispatch.response.clone();
        let mut outbound = dispatch.outbound.clone();
        let produced = execute_host_dispatch(
            &self.server,
            &self.host,
            dispatch,
            self.config.request_timeout,
        )
        .await?;
        outbound.extend(produced);
        let pending_id = dispatch.pending.as_ref().map(|pending| &pending.request_id);
        let mut events = Vec::new();
        for message in outbound {
            if message.connection_id != session.connection_id {
                return Err(ProtocolError::new(
                    ProtocolErrorCode::Forbidden,
                    "MCP host emitted a message for another connection",
                ));
            }
            let is_primary = primary.is_none()
                && pending_id.is_some_and(|id| message.message.get("id") == Some(id));
            if is_primary {
                primary = Some(message.message);
            } else {
                events.push(message.message);
            }
        }
        Ok((primary, events))
    }

    fn bound_session(
        &self,
        headers: &McpHttpHeaders,
        principal: &ProtocolPrincipal,
    ) -> ProtocolResult<McpHttpSession> {
        let session_id = headers
            .get("mcp-session-id")
            .ok_or_else(|| ProtocolError::invalid("MCP-Session-Id is required"))?;
        if !valid_session_id(session_id) {
            return Err(ProtocolError::invalid("MCP-Session-Id is invalid"));
        }
        self.sessions.purge_expired(self.server.now_unix_ms())?;
        let session = self
            .sessions
            .load_session(session_id)?
            .ok_or_else(|| ProtocolError::not_found("MCP HTTP session was not found"))?;
        if !session.matches(principal) {
            return Err(ProtocolError::new(
                ProtocolErrorCode::Forbidden,
                "MCP HTTP session authorization context changed",
            ));
        }
        Ok(session)
    }

    fn consume_rate(&self, address: IpAddr) -> ProtocolResult<bool> {
        let minute = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |duration| duration.as_secs() / 60);
        let mut rate = self.rate.lock().map_err(|_| store_lock_error())?;
        rate.retain(|_, bucket| bucket.minute.saturating_add(2) >= minute);
        if !rate.contains_key(&address) && rate.len() >= self.config.max_rate_buckets {
            return Ok(false);
        }
        let bucket = rate
            .entry(address)
            .or_insert(RateBucket { minute, count: 0 });
        if bucket.minute != minute {
            *bucket = RateBucket { minute, count: 0 };
        }
        if bucket.count >= self.config.max_requests_per_minute {
            return Ok(false);
        }
        bucket.count = bucket.count.saturating_add(1);
        Ok(true)
    }
}

#[derive(Debug)]
struct HttpRequest {
    method: String,
    path: String,
    headers: McpHttpHeaders,
    body: Vec<u8>,
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
}

impl HttpResponse {
    fn text(status: u16, message: &str) -> Self {
        Self::bytes(
            status,
            "text/plain; charset=utf-8",
            message.as_bytes().to_vec(),
        )
    }

    fn json(status: u16, value: Value) -> Self {
        match serde_json::to_vec(&value) {
            Ok(body) => Self::bytes(status, "application/json", body),
            Err(_) => Self::text(500, "response encoding failed"),
        }
    }

    fn bytes(status: u16, content_type: &'static str, body: Vec<u8>) -> Self {
        Self {
            status,
            content_type: Some(content_type),
            headers: Vec::new(),
            body,
        }
    }

    fn empty(status: u16) -> Self {
        Self {
            status,
            content_type: None,
            headers: Vec::new(),
            body: Vec::new(),
        }
    }

    fn with_header(mut self, name: &str, value: &str) -> Self {
        if !name.contains(['\r', '\n']) && !value.contains(['\r', '\n']) {
            self.headers.push((name.to_owned(), value.to_owned()));
        }
        self
    }
}

async fn read_http_request(
    stream: &mut TcpStream,
    max_header_bytes: usize,
    max_body_bytes: usize,
) -> Result<HttpRequest, HttpParseError> {
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
        if buffer.len() > max_header_bytes && find_subslice(&buffer, b"\r\n\r\n").is_none() {
            return Err(http_parse_error(431, "request headers are too large"));
        }
        if let Some(index) = find_subslice(&buffer, b"\r\n\r\n") {
            break index + 4;
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
        || !matches!(method, "POST" | "GET" | "DELETE")
        || path.is_empty()
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
            return Err(http_parse_error(411, "Content-Length is required"))
        }
        None => 0,
    };
    if content_length > max_body_bytes {
        return Err(http_parse_error(413, "request body is too large"));
    }
    if method != "POST" && content_length != 0 {
        return Err(http_parse_error(
            400,
            "GET and DELETE requests must not have a body",
        ));
    }
    let mut body = buffer[header_end..].to_vec();
    if body.len() > content_length {
        return Err(http_parse_error(400, "HTTP pipelining is not supported"));
    }
    body.resize(content_length, 0);
    if buffer.len().saturating_sub(header_end) < content_length {
        stream
            .read_exact(&mut body[buffer.len() - header_end..])
            .await
            .map_err(|_| http_parse_error(400, "incomplete request body"))?;
    }
    Ok(HttpRequest {
        method: method.to_owned(),
        path: path.to_owned(),
        headers: McpHttpHeaders { values: headers },
        body,
    })
}

async fn write_http_response(stream: &mut TcpStream, response: HttpResponse) -> ProtocolResult<()> {
    let reason = match response.status {
        200 => "OK",
        202 => "Accepted",
        204 => "No Content",
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
        429 => "Too Many Requests",
        431 => "Request Header Fields Too Large",
        500 => "Internal Server Error",
        503 => "Service Unavailable",
        504 => "Gateway Timeout",
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
        .map_err(|error| protocol_io("write MCP HTTP headers", error))?;
    stream
        .write_all(&response.body)
        .await
        .map_err(|error| protocol_io("write MCP HTTP body", error))
}

fn random_session_id() -> ProtocolResult<String> {
    let mut bytes = [0_u8; 24];
    getrandom::fill(&mut bytes).map_err(|error| {
        ProtocolError::new(
            ProtocolErrorCode::Conflict,
            format!("secure MCP session id generation failed: {error}"),
        )
    })?;
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        write!(encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    Ok(encoded)
}

fn valid_session_id(value: &str) -> bool {
    (16..=128).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

fn jsonrpc_parse_error() -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": null,
        "error": {"code": -32700, "message": "Parse error"}
    })
}

fn protocol_http_response(error: &ProtocolError) -> HttpResponse {
    let status = match error.code {
        ProtocolErrorCode::InvalidRequest | ProtocolErrorCode::InvalidTransition => 400,
        ProtocolErrorCode::Unauthorized => 401,
        ProtocolErrorCode::Forbidden => 403,
        ProtocolErrorCode::NotFound => 404,
        ProtocolErrorCode::Cancelled => 504,
        ProtocolErrorCode::Conflict => 500,
    };
    HttpResponse::text(status, &error.message)
}

fn protocol_io(operation: &str, error: std::io::Error) -> ProtocolError {
    ProtocolError::new(
        ProtocolErrorCode::Conflict,
        format!("{operation} failed: {error}"),
    )
}

fn store_lock_error() -> ProtocolError {
    ProtocolError::new(
        ProtocolErrorCode::Conflict,
        "MCP transport store lock poisoned",
    )
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocols::{
        InMemoryMcpServerStateStore, McpServerAction, McpServerInfo, McpServerRegistry,
        McpTaskSupport, McpToolDefinition,
    };
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{duplex, split, AsyncBufReadExt, BufReader};

    fn principal(tenant: &str) -> ProtocolPrincipal {
        ProtocolPrincipal::new(
            "user",
            [
                "mcp:tools:list",
                "mcp:tools:call",
                "mcp:tasks:read",
                "mcp:tasks:cancel",
            ],
        )
        .unwrap()
        .with_tenant(tenant)
        .unwrap()
    }

    fn runtime_server() -> Arc<McpJsonRpcServer> {
        let mut registry = McpServerRegistry::new();
        let mut tool = McpToolDefinition::new(
            "echo",
            "echo",
            json!({
                "type":"object",
                "properties":{"text":{"type":"string"}},
                "required":["text"],
                "additionalProperties":false
            }),
        )
        .unwrap();
        tool.task_support = McpTaskSupport::Optional;
        registry.register_tool(tool).unwrap();
        Arc::new(
            McpJsonRpcServer::new(
                "io-tests",
                McpServerInfo::new("io-tests", "1").unwrap(),
                Arc::new(InMemoryMcpServerStateStore::default()),
                registry,
            )
            .unwrap(),
        )
    }

    #[derive(Debug)]
    struct EchoHost;

    #[async_trait]
    impl McpDispatchHost for EchoHost {
        async fn handle(
            &self,
            server: &McpJsonRpcServer,
            dispatch: &McpJsonRpcDispatch,
        ) -> ProtocolResult<Vec<McpOutboundMessage>> {
            match dispatch
                .governed_action
                .as_ref()
                .and_then(|decision| decision.action())
            {
                Some(McpServerAction::InvokeTool { task }) => server.complete_tool(
                    &task.task_id,
                    json!({"content":[{"type":"text","text":"echoed"}],"isError":false}),
                ),
                Some(McpServerAction::ReadResource { .. }) => {
                    let pending = dispatch.pending.as_ref().ok_or_else(|| {
                        ProtocolError::invalid("resource dispatch omitted pending handle")
                    })?;
                    server
                        .complete_pending(pending, Ok(json!({"contents":[]})))
                        .map(|message| vec![message])
                }
                _ => Ok(Vec::new()),
            }
        }
    }

    #[derive(Debug)]
    struct CancellationHost {
        cancel_calls: AtomicUsize,
        fail_cancellation: bool,
    }

    #[async_trait]
    impl McpDispatchHost for CancellationHost {
        async fn handle(
            &self,
            _server: &McpJsonRpcServer,
            dispatch: &McpJsonRpcDispatch,
        ) -> ProtocolResult<Vec<McpOutboundMessage>> {
            if matches!(
                dispatch
                    .governed_action
                    .as_ref()
                    .and_then(|decision| decision.action()),
                Some(McpServerAction::CancelTask { .. })
            ) {
                self.cancel_calls.fetch_add(1, Ordering::SeqCst);
                if self.fail_cancellation {
                    return Err(ProtocolError::conflict(
                        "host cancellation outcome is ambiguous",
                    ));
                }
            }
            Ok(Vec::new())
        }
    }

    #[tokio::test]
    async fn stdio_listener_frames_real_duplex_io_and_executes_through_host_seam() {
        let (client, transport) = duplex(128 * 1024);
        let (client_read, mut client_write) = split(client);
        let (transport_read, transport_write) = split(transport);
        let cancellation = CancellationToken::new();
        let handle = cancellation.handle();
        let server_task = tokio::spawn(serve_mcp_stdio(
            runtime_server(),
            principal("tenant-a"),
            Arc::new(EchoHost),
            transport_read,
            transport_write,
            McpStdioConfig::default(),
            cancellation,
        ));
        let mut reader = BufReader::new(client_read);
        client_write
            .write_all(
                serde_json::to_string(&json!({
                    "jsonrpc":"2.0","id":1,"method":"initialize","params":{
                        "protocolVersion":"2025-11-25","capabilities":{},
                        "clientInfo":{"name":"stdio-test","version":"1"}
                    }
                }))
                .unwrap()
                .as_bytes(),
            )
            .await
            .unwrap();
        client_write.write_all(b"\n").await.unwrap();
        let mut line = String::new();
        timeout(Duration::from_secs(2), reader.read_line(&mut line))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&line).unwrap()["result"]["protocolVersion"],
            "2025-11-25"
        );
        client_write
            .write_all(
                b"{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\",\"params\":{}}\n",
            )
            .await
            .unwrap();
        client_write
            .write_all(
                b"{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/call\",\"params\":{\"name\":\"echo\",\"arguments\":{\"text\":\"hello\"}}}\n",
            )
            .await
            .unwrap();
        line.clear();
        timeout(Duration::from_secs(2), reader.read_line(&mut line))
            .await
            .unwrap()
            .unwrap();
        let response: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(response["id"], 2);
        assert_eq!(response["result"]["content"][0]["text"], "echoed");
        handle.cancel();
        client_write.shutdown().await.unwrap();
        drop(client_write);
        timeout(Duration::from_secs(2), server_task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn stdio_listener_cleans_up_initialized_connection_after_frame_failure() {
        let (client, transport) = duplex(128 * 1024);
        let (client_read, mut client_write) = split(client);
        let (transport_read, transport_write) = split(transport);
        let server = runtime_server();
        let owner = principal("tenant-a");
        let server_task = tokio::spawn(serve_mcp_stdio(
            server.clone(),
            owner.clone(),
            Arc::new(EchoHost),
            transport_read,
            transport_write,
            McpStdioConfig {
                connection_id: "bounded-stdio".into(),
                max_frame_bytes: 512,
                ..McpStdioConfig::default()
            },
            CancellationToken::new(),
        ));
        let initialize = json!({
            "jsonrpc":"2.0","id":1,"method":"initialize","params":{
                "protocolVersion":"2025-11-25","capabilities":{},
                "clientInfo":{"name":"cleanup-test","version":"1"}
            }
        })
        .to_string();
        client_write.write_all(initialize.as_bytes()).await.unwrap();
        client_write.write_all(b"\n").await.unwrap();
        let mut reader = BufReader::new(client_read);
        let mut response = String::new();
        timeout(Duration::from_secs(2), reader.read_line(&mut response))
            .await
            .unwrap()
            .unwrap();
        assert!(response.contains("2025-11-25"));

        client_write.write_all(&vec![b'x'; 513]).await.unwrap();
        client_write.write_all(b"\n").await.unwrap();
        let error = timeout(Duration::from_secs(2), server_task)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code, ProtocolErrorCode::InvalidRequest);
        assert_eq!(
            server
                .terminate_connection("bounded-stdio", Some(&owner))
                .unwrap_err()
                .code,
            ProtocolErrorCode::NotFound
        );
    }

    #[tokio::test]
    async fn stdio_teardown_executes_cancellation_before_finalizing_connection() {
        let (client, transport) = duplex(128 * 1024);
        let (client_read, mut client_write) = split(client);
        let (transport_read, transport_write) = split(transport);
        let server = runtime_server();
        let owner = principal("tenant-a");
        let host = Arc::new(CancellationHost {
            cancel_calls: AtomicUsize::new(0),
            fail_cancellation: false,
        });
        let server_task = tokio::spawn(serve_mcp_stdio(
            server.clone(),
            owner,
            host.clone(),
            transport_read,
            transport_write,
            McpStdioConfig {
                connection_id: "teardown-stdio".into(),
                operation_timeout: Duration::from_secs(1),
                ..McpStdioConfig::default()
            },
            CancellationToken::new(),
        ));
        let mut reader = BufReader::new(client_read);
        client_write
            .write_all(
                b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{\"protocolVersion\":\"2025-11-25\",\"capabilities\":{},\"clientInfo\":{\"name\":\"teardown\",\"version\":\"1\"}}}\n",
            )
            .await
            .unwrap();
        let mut line = String::new();
        timeout(Duration::from_secs(2), reader.read_line(&mut line))
            .await
            .unwrap()
            .unwrap();
        client_write
            .write_all(
                b"{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\",\"params\":{}}\n",
            )
            .await
            .unwrap();
        client_write
            .write_all(
                b"{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/call\",\"params\":{\"name\":\"echo\",\"arguments\":{\"text\":\"pending\"},\"task\":{\"ttl\":60000}}}\n",
            )
            .await
            .unwrap();
        line.clear();
        timeout(Duration::from_secs(2), reader.read_line(&mut line))
            .await
            .unwrap()
            .unwrap();
        assert!(line.contains("\"status\":\"working\""));
        client_write.shutdown().await.unwrap();
        drop(client_write);
        timeout(Duration::from_secs(2), server_task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(host.cancel_calls.load(Ordering::SeqCst), 1);
        let snapshot = server.snapshot().unwrap();
        assert!(snapshot
            .registry()
            .tasks()
            .values()
            .all(|task| task.status == crate::protocols::McpTaskStatus::Cancelled));
    }

    struct TestAuthenticator;

    impl McpHttpAuthenticator for TestAuthenticator {
        fn authenticate(
            &self,
            headers: &McpHttpHeaders,
        ) -> Result<ProtocolPrincipal, McpHttpAuthError> {
            match headers.get("authorization") {
                Some("Bearer owner") => Ok(principal("tenant-a")),
                Some("Bearer attacker") => Ok(principal("tenant-b")),
                _ => Err(McpHttpAuthError {
                    status: 401,
                    message: "authentication required".into(),
                    www_authenticate: Some("Bearer realm=\"mcp-test\"".into()),
                }),
            }
        }
    }

    #[test]
    fn static_bearer_and_in_memory_sse_bounds_fail_closed() {
        let authenticator = StaticBearerMcpAuthenticator::new(
            "expected-token",
            principal("tenant-a"),
            "Bearer realm=\"mcp-test\"",
        )
        .unwrap();
        let headers = McpHttpHeaders {
            values: BTreeMap::from([("authorization".into(), "Bearer wrong-token".into())]),
        };
        let auth_error = authenticator.authenticate(&headers).unwrap_err();
        assert_eq!(auth_error.status, 401);
        assert_eq!(
            auth_error.www_authenticate.as_deref(),
            Some("Bearer realm=\"mcp-test\"")
        );

        let store = InMemoryMcpHttpSessionStore::default();
        store
            .create_session(
                &McpHttpSession {
                    session_id: "bounded-session".into(),
                    connection_id: "bounded-connection".into(),
                    subject: "owner".into(),
                    tenant_id: None,
                    created_at_unix_ms: 1,
                    expires_at_unix_ms: 10_000,
                },
                10,
            )
            .unwrap();
        assert_eq!(
            store
                .append_event("bounded-session", &json!({"oversized":"payload"}), 10, 8,)
                .unwrap_err()
                .code,
            ProtocolErrorCode::InvalidRequest
        );
        store
            .append_event("bounded-session", &json!({"ok":true}), 10, 1024)
            .unwrap();
        assert_eq!(
            store
                .replay_events("bounded-session", None, 10, 1)
                .unwrap_err()
                .code,
            ProtocolErrorCode::Conflict
        );
        assert!(store
            .replay_events("bounded-session", Some("missing-event"), 10, 1024)
            .is_err());
        assert_eq!(
            store
                .replay_events("bounded-session", None, 10, 1024)
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            store
                .create_session(
                    &McpHttpSession {
                        session_id: "over-capacity".into(),
                        connection_id: "over-capacity".into(),
                        subject: "owner".into(),
                        tenant_id: None,
                        created_at_unix_ms: 2,
                        expires_at_unix_ms: 20_000,
                    },
                    1,
                )
                .unwrap_err()
                .code,
            ProtocolErrorCode::Conflict
        );
        assert_eq!(store.purge_expired(10_000).unwrap(), 1);
        assert!(store.load_session("bounded-session").unwrap().is_none());
    }

    async fn raw_http(address: SocketAddr, request: &[u8]) -> String {
        let mut stream = TcpStream::connect(address).await.unwrap();
        stream.write_all(request).await.unwrap();
        stream.shutdown().await.unwrap();
        let mut response = Vec::new();
        timeout(Duration::from_secs(2), stream.read_to_end(&mut response))
            .await
            .unwrap()
            .unwrap();
        String::from_utf8(response).unwrap()
    }

    fn post(body: &str, extra_headers: &str) -> Vec<u8> {
        format!(
            "POST /mcp HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nAccept: application/json, text/event-stream\r\nContent-Length: {}\r\n{}\r\n{}",
            body.len(),
            extra_headers,
            body
        )
        .into_bytes()
    }

    fn response_header<'a>(response: &'a str, name: &str) -> Option<&'a str> {
        response.split("\r\n").find_map(|line| {
            let (header, value) = line.split_once(':')?;
            header.eq_ignore_ascii_case(name).then(|| value.trim())
        })
    }

    #[tokio::test]
    async fn streamable_http_enforces_origin_auth_session_sse_resume_and_delete() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let cancellation = CancellationToken::new();
        let handle = cancellation.handle();
        let mut config = McpStreamableHttpConfig::default();
        config
            .allowed_origins
            .insert("https://client.example".into());
        let transport = Arc::new(
            McpStreamableHttpServer::new(
                runtime_server(),
                Arc::new(InMemoryMcpHttpSessionStore::default()),
                Arc::new(TestAuthenticator),
                Arc::new(EchoHost),
                config,
            )
            .unwrap(),
        );
        let task = tokio::spawn(transport.serve(listener, cancellation));

        let initialize = json!({
            "jsonrpc":"2.0","id":1,"method":"initialize","params":{
                "protocolVersion":"2025-11-25","capabilities":{},
                "clientInfo":{"name":"http-test","version":"1"}
            }
        })
        .to_string();
        let bad_origin = raw_http(
            address,
            &post(
                &initialize,
                "Authorization: Bearer owner\r\nOrigin: https://evil.example\r\n",
            ),
        )
        .await;
        assert!(bad_origin.starts_with("HTTP/1.1 403"));
        let unauthenticated = raw_http(
            address,
            &post(&initialize, "Origin: https://client.example\r\n"),
        )
        .await;
        assert!(unauthenticated.starts_with("HTTP/1.1 401"));
        assert!(unauthenticated.contains("WWW-Authenticate: Bearer realm=\"mcp-test\""));

        let initialized = raw_http(
            address,
            &post(
                &initialize,
                "Authorization: Bearer owner\r\nOrigin: https://client.example\r\n",
            ),
        )
        .await;
        assert!(initialized.starts_with("HTTP/1.1 200"));
        let session_id = response_header(&initialized, "MCP-Session-Id")
            .unwrap()
            .to_owned();
        assert!(valid_session_id(&session_id));

        let initialized_notification = json!({
            "jsonrpc":"2.0","method":"notifications/initialized","params":{}
        })
        .to_string();
        let missing_version = raw_http(
            address,
            &post(
                &initialized_notification,
                &format!("Authorization: Bearer owner\r\nOrigin: https://client.example\r\nMCP-Session-Id: {session_id}\r\n"),
            ),
        )
        .await;
        assert!(missing_version.starts_with("HTTP/1.1 400"));
        let ready = raw_http(
            address,
            &post(
                &initialized_notification,
                &format!("Authorization: Bearer owner\r\nOrigin: https://client.example\r\nMCP-Session-Id: {session_id}\r\nMCP-Protocol-Version: 2025-11-25\r\n"),
            ),
        )
        .await;
        assert!(ready.starts_with("HTTP/1.1 202"));

        let task_call = json!({
            "jsonrpc":"2.0","id":2,"method":"tools/call","params":{
                "name":"echo","arguments":{"text":"hello"},"task":{"ttl":60000}
            }
        })
        .to_string();
        let task_response = raw_http(
            address,
            &post(
                &task_call,
                &format!("Authorization: Bearer owner\r\nOrigin: https://client.example\r\nMCP-Session-Id: {session_id}\r\nMCP-Protocol-Version: 2025-11-25\r\n"),
            ),
        )
        .await;
        assert!(task_response.starts_with("HTTP/1.1 200"));
        assert!(task_response.contains("\"status\":\"working\""));

        let get = format!(
            "GET /mcp HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer owner\r\nOrigin: https://client.example\r\nMCP-Session-Id: {session_id}\r\nMCP-Protocol-Version: 2025-11-25\r\nAccept: text/event-stream\r\n\r\n"
        );
        let events = raw_http(address, get.as_bytes()).await;
        assert!(events.starts_with("HTTP/1.1 200"));
        assert!(events.contains("notifications/tasks/status"));
        let event_id = events
            .lines()
            .find_map(|line| line.strip_prefix("id: "))
            .unwrap()
            .trim_end_matches('\r')
            .to_owned();

        let attacker_get = format!(
            "GET /mcp HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer attacker\r\nOrigin: https://client.example\r\nMCP-Session-Id: {session_id}\r\nMCP-Protocol-Version: 2025-11-25\r\nAccept: text/event-stream\r\n\r\n"
        );
        assert!(raw_http(address, attacker_get.as_bytes())
            .await
            .starts_with("HTTP/1.1 403"));

        let forged_resume = format!(
            "GET /mcp HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer owner\r\nOrigin: https://client.example\r\nMCP-Session-Id: {session_id}\r\nMCP-Protocol-Version: 2025-11-25\r\nAccept: text/event-stream\r\nLast-Event-ID: forged-event-id\r\n\r\n"
        );
        assert!(raw_http(address, forged_resume.as_bytes())
            .await
            .starts_with("HTTP/1.1 400"));

        let resume = format!(
            "GET /mcp HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer owner\r\nOrigin: https://client.example\r\nMCP-Session-Id: {session_id}\r\nMCP-Protocol-Version: 2025-11-25\r\nAccept: text/event-stream\r\nLast-Event-ID: {event_id}\r\n\r\n"
        );
        let resumed = raw_http(address, resume.as_bytes()).await;
        assert!(resumed.starts_with("HTTP/1.1 200"));
        assert!(resumed.contains(": keepalive"));

        let delete = format!(
            "DELETE /mcp HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer owner\r\nOrigin: https://client.example\r\nMCP-Session-Id: {session_id}\r\nMCP-Protocol-Version: 2025-11-25\r\n\r\n"
        );
        assert!(raw_http(address, delete.as_bytes())
            .await
            .starts_with("HTTP/1.1 204"));
        assert!(raw_http(address, get.as_bytes())
            .await
            .starts_with("HTTP/1.1 404"));

        handle.cancel();
        timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn real_http_delete_keeps_session_and_reconciliation_state_on_cancel_error() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let cancellation = CancellationToken::new();
        let handle = cancellation.handle();
        let mut config = McpStreamableHttpConfig::default();
        config
            .allowed_origins
            .insert("https://client.example".into());
        config.request_timeout = Duration::from_secs(1);
        let server = runtime_server();
        let host = Arc::new(CancellationHost {
            cancel_calls: AtomicUsize::new(0),
            fail_cancellation: true,
        });
        let transport = Arc::new(
            McpStreamableHttpServer::new(
                server.clone(),
                Arc::new(InMemoryMcpHttpSessionStore::default()),
                Arc::new(TestAuthenticator),
                host.clone(),
                config,
            )
            .unwrap(),
        );
        let task = tokio::spawn(transport.serve(listener, cancellation));

        let initialize = json!({
            "jsonrpc":"2.0","id":1,"method":"initialize","params":{
                "protocolVersion":"2025-11-25","capabilities":{},
                "clientInfo":{"name":"cancel-test","version":"1"}
            }
        })
        .to_string();
        let initialized = raw_http(
            address,
            &post(
                &initialize,
                "Authorization: Bearer owner\r\nOrigin: https://client.example\r\n",
            ),
        )
        .await;
        let session_id = response_header(&initialized, "MCP-Session-Id")
            .unwrap()
            .to_owned();
        let headers = format!(
            "Authorization: Bearer owner\r\nOrigin: https://client.example\r\nMCP-Session-Id: {session_id}\r\nMCP-Protocol-Version: 2025-11-25\r\n"
        );
        let ready = json!({
            "jsonrpc":"2.0","method":"notifications/initialized","params":{}
        })
        .to_string();
        assert!(raw_http(address, &post(&ready, &headers))
            .await
            .starts_with("HTTP/1.1 202"));
        let call = json!({
            "jsonrpc":"2.0","id":2,"method":"tools/call","params":{
                "name":"echo","arguments":{"text":"pending"},"task":{"ttl":60000}
            }
        })
        .to_string();
        assert!(raw_http(address, &post(&call, &headers))
            .await
            .contains("\"status\":\"working\""));

        let delete = format!(
            "DELETE /mcp HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer owner\r\nOrigin: https://client.example\r\nMCP-Session-Id: {session_id}\r\nMCP-Protocol-Version: 2025-11-25\r\n\r\n"
        );
        assert!(raw_http(address, delete.as_bytes())
            .await
            .starts_with("HTTP/1.1 500"));
        assert_eq!(host.cancel_calls.load(Ordering::SeqCst), 1);
        let snapshot = server.snapshot().unwrap();
        let pending = snapshot.registry().tasks().values().next().unwrap();
        assert_eq!(pending.status, crate::protocols::McpTaskStatus::Working);
        assert_eq!(
            pending.cancellation,
            Some(crate::protocols::McpCancellationState::ReconcileRequired)
        );
        let get = format!(
            "GET /mcp HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer owner\r\nOrigin: https://client.example\r\nMCP-Session-Id: {session_id}\r\nMCP-Protocol-Version: 2025-11-25\r\nAccept: text/event-stream\r\n\r\n"
        );
        assert!(raw_http(address, get.as_bytes())
            .await
            .starts_with("HTTP/1.1 200"));

        handle.cancel();
        timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}
