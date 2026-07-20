//! Byte-level MCP JSON-RPC server dispatcher with durable, compare-and-swap state.
//!
//! The dispatcher owns wire validation, lifecycle negotiation, request deduplication, durable task
//! metadata, progress/cancellation routing and authorization-context binding. It deliberately emits
//! governed host actions instead of executing tools or reading resources itself.

use super::{
    CorrelationIdentity, GovernanceAuthorization, GovernanceEnvelope, McpProgress, McpServerAction,
    McpServerRegistry, McpTask, McpTaskStatus, McpToolCallRequest, McpToolDefinition,
    McpToolExecutionMode, ProtocolError, ProtocolErrorCode, ProtocolPrincipal, ProtocolResult,
    MCP_SERVER_CONTRACT_REVISION,
};
use crate::durability::{stable_id, stable_input_hash};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

pub const MCP_JSONRPC_VERSION: &str = "2.0";
pub const MCP_SERVER_STATE_VERSION: u32 = 1;
pub const DEFAULT_MCP_MAX_REQUEST_BYTES: usize = 1024 * 1024;
pub const DEFAULT_MCP_MAX_RECEIPTS: usize = 4096;
pub const DEFAULT_MCP_MAX_TASKS: usize = 4096;
pub const DEFAULT_MCP_RETENTION_MS: u64 = 86_400_000;
pub const DEFAULT_MCP_MAX_RESULT_BYTES: usize = 1024 * 1024;
pub const DEFAULT_MCP_MAX_RESULT_ITEMS: usize = 4096;
pub const DEFAULT_MCP_MAX_RESULT_DEPTH: usize = 32;
pub const DEFAULT_MCP_TASK_TTL_MS: u64 = 3_600_000;
pub const MAX_MCP_TASK_TTL_MS: u64 = 86_400_000;
pub const DEFAULT_MCP_POLL_INTERVAL_MS: u64 = 1_000;

const JSONRPC_PARSE_ERROR: i64 = -32700;
const JSONRPC_INVALID_REQUEST: i64 = -32600;
const JSONRPC_METHOD_NOT_FOUND: i64 = -32601;
const JSONRPC_INVALID_PARAMS: i64 = -32602;
const JSONRPC_INTERNAL_ERROR: i64 = -32603;
const MCP_NOT_INITIALIZED: i64 = -32002;
const MCP_FORBIDDEN: i64 = -32003;
const MCP_CONFLICT: i64 = -32009;
const MCP_RECONCILIATION_REQUIRED: i64 = -32010;
const MCP_CANCELLED: i64 = -32800;

/// Public server identity returned by the MCP `initialize` handshake.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpServerInfo {
    pub name: String,
    pub version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

impl McpServerInfo {
    pub fn new(name: impl Into<String>, version: impl Into<String>) -> ProtocolResult<Self> {
        let info = Self {
            name: name.into(),
            version: version.into(),
            title: None,
            description: None,
        };
        if info.name.trim().is_empty() || info.version.trim().is_empty() {
            return Err(ProtocolError::invalid(
                "MCP server name and version must not be empty",
            ));
        }
        Ok(info)
    }
}

/// A response that a concrete stdio or Streamable HTTP adapter must write to a connection.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpOutboundMessage {
    pub connection_id: String,
    pub message: Value,
}

/// Opaque completion handle for a host operation whose JSON-RPC response is still pending.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpPendingResponse {
    pub connection_id: String,
    pub request_id: Value,
    pub operation: String,
}

/// Result of dispatching one complete JSON-RPC object.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct McpJsonRpcDispatch {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response: Option<Value>,
    #[serde(default)]
    pub outbound: Vec<McpOutboundMessage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub governed_action: Option<super::GovernedAction<McpServerAction>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending: Option<McpPendingResponse>,
}

impl McpJsonRpcDispatch {
    fn response(response: Value) -> Self {
        Self {
            response: Some(response),
            outbound: Vec::new(),
            governed_action: None,
            pending: None,
        }
    }

    fn notification() -> Self {
        Self {
            response: None,
            outbound: Vec::new(),
            governed_action: None,
            pending: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct McpConnectionState {
    subject: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    tenant_id: Option<String>,
    initialized: bool,
    client_name: String,
    client_version: String,
    /// Once a side-effect receipt ages out, this connection's request-id namespace can no longer
    /// prove whether an unknown request was already executed. The connection therefore remains
    /// fail-closed until the transport completes teardown and creates a fresh session namespace.
    #[serde(default)]
    receipt_namespace_retired: bool,
}

impl McpConnectionState {
    fn matches(&self, principal: &ProtocolPrincipal) -> bool {
        self.subject == principal.subject && self.tenant_id == principal.tenant_id
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
enum McpReceiptState {
    PendingHost {
        operation: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        task_id: Option<String>,
    },
    WaitingForTask {
        task_id: String,
    },
    Completed {
        response: Value,
        #[serde(default)]
        completed_at_unix_ms: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct McpRequestReceipt {
    connection_id: String,
    request_id: Value,
    #[serde(default)]
    method: String,
    request_hash: String,
    #[serde(default)]
    created_at_unix_ms: u64,
    state: McpReceiptState,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct McpTaskWireMetadata {
    connection_id: String,
    created_at: String,
    last_updated_at: String,
    #[serde(default)]
    created_at_unix_ms: u64,
    #[serde(default)]
    last_updated_at_unix_ms: u64,
    #[serde(default)]
    expires_at_unix_ms: u64,
    ttl: u64,
    poll_interval: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    progress_token: Option<Value>,
}

/// Serializable server snapshot committed atomically with request receipts.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpServerState {
    schema_version: u32,
    storage_revision: u64,
    registry: McpServerRegistry,
    connections: BTreeMap<String, McpConnectionState>,
    receipts: BTreeMap<String, McpRequestReceipt>,
    task_metadata: BTreeMap<String, McpTaskWireMetadata>,
}

impl McpServerState {
    fn new(registry: McpServerRegistry) -> Self {
        Self {
            schema_version: MCP_SERVER_STATE_VERSION,
            storage_revision: 0,
            registry,
            connections: BTreeMap::new(),
            receipts: BTreeMap::new(),
            task_metadata: BTreeMap::new(),
        }
    }

    fn validate(&self) -> ProtocolResult<()> {
        if self.schema_version != MCP_SERVER_STATE_VERSION {
            return Err(ProtocolError::invalid(format!(
                "unsupported MCP server state version: {}",
                self.schema_version
            )));
        }
        for tool in self.registry.tools().values() {
            tool.validate()?;
        }
        for resource in self.registry.resources().values() {
            resource.validate()?;
        }
        for prompt in self.registry.prompts().values() {
            prompt.validate()?;
        }
        for (task_id, task) in self.registry.tasks() {
            if task_id != &task.task_id {
                return Err(ProtocolError::invalid(
                    "MCP task map key does not match task_id",
                ));
            }
            if task.advertised && !self.task_metadata.contains_key(task_id) {
                return Err(ProtocolError::invalid(
                    "advertised MCP task is missing durable wire metadata",
                ));
            }
            if let Some(result) = &task.result {
                validate_json_bounds(
                    result,
                    DEFAULT_MCP_MAX_RESULT_BYTES,
                    DEFAULT_MCP_MAX_RESULT_ITEMS,
                    DEFAULT_MCP_MAX_RESULT_DEPTH,
                )?;
            }
        }
        Ok(())
    }

    pub fn schema_version(&self) -> u32 {
        self.schema_version
    }

    pub fn storage_revision(&self) -> u64 {
        self.storage_revision
    }

    pub fn registry(&self) -> &McpServerRegistry {
        &self.registry
    }
}

/// Persistence seam for durable MCP tasks, connection lifecycle and dedupe receipts.
///
/// Implementations must atomically reject a stale `expected_revision`.
pub trait McpServerStateStore: Send + Sync {
    fn load(&self, namespace: &str) -> ProtocolResult<Option<McpServerState>>;
    fn compare_and_swap(
        &self,
        namespace: &str,
        expected_revision: Option<u64>,
        state: &McpServerState,
    ) -> ProtocolResult<()>;
}

/// Injectable time source so replay/conformance tests can pin task timestamps.
pub trait McpClock: Send + Sync {
    fn now_unix_ms(&self) -> u64;

    fn now_rfc3339(&self) -> String {
        unix_seconds_rfc3339(self.now_unix_ms() / 1_000)
    }
}

#[derive(Debug, Default)]
pub struct SystemMcpClock;

impl McpClock for SystemMcpClock {
    fn now_unix_ms(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |duration| {
                u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
            })
    }
}

/// Process-local reference store used by tests and single-process applications.
#[derive(Debug, Default)]
pub struct InMemoryMcpServerStateStore {
    states: Mutex<BTreeMap<String, McpServerState>>,
}

impl McpServerStateStore for InMemoryMcpServerStateStore {
    fn load(&self, namespace: &str) -> ProtocolResult<Option<McpServerState>> {
        self.states
            .lock()
            .map_err(|_| ProtocolError::new(ProtocolErrorCode::Conflict, "MCP store lock poisoned"))
            .map(|states| states.get(namespace).cloned())
    }

    fn compare_and_swap(
        &self,
        namespace: &str,
        expected_revision: Option<u64>,
        state: &McpServerState,
    ) -> ProtocolResult<()> {
        state.validate()?;
        let mut states = self.states.lock().map_err(|_| {
            ProtocolError::new(ProtocolErrorCode::Conflict, "MCP store lock poisoned")
        })?;
        let actual_revision = states.get(namespace).map(|value| value.storage_revision);
        if actual_revision != expected_revision {
            return Err(ProtocolError::conflict(
                "MCP server state changed concurrently",
            ));
        }
        states.insert(namespace.to_owned(), state.clone());
        Ok(())
    }
}

/// Durable MCP JSON-RPC server core. Concrete transports feed it one complete JSON object at a
/// time and deliver the returned response/outbound messages on the indicated connection.
pub struct McpJsonRpcServer {
    namespace: String,
    server_info: McpServerInfo,
    store: Arc<dyn McpServerStateStore>,
    clock: Arc<dyn McpClock>,
    max_request_bytes: usize,
    max_receipts: usize,
    max_tasks: usize,
    retention_ms: u64,
    max_result_bytes: usize,
    max_result_items: usize,
    max_result_depth: usize,
}

struct WireRequestContext<'a> {
    state: McpServerState,
    expected_revision: Option<u64>,
    connection_id: &'a str,
    request: ParsedRequest,
    request_hash: String,
}

impl McpJsonRpcServer {
    pub fn new(
        namespace: impl Into<String>,
        server_info: McpServerInfo,
        store: Arc<dyn McpServerStateStore>,
        registry: McpServerRegistry,
    ) -> ProtocolResult<Self> {
        let namespace = namespace.into();
        if namespace.trim().is_empty() {
            return Err(ProtocolError::invalid(
                "MCP server namespace must not be empty",
            ));
        }
        let server = Self {
            namespace,
            server_info,
            store,
            clock: Arc::new(SystemMcpClock),
            max_request_bytes: DEFAULT_MCP_MAX_REQUEST_BYTES,
            max_receipts: DEFAULT_MCP_MAX_RECEIPTS,
            max_tasks: DEFAULT_MCP_MAX_TASKS,
            retention_ms: DEFAULT_MCP_RETENTION_MS,
            max_result_bytes: DEFAULT_MCP_MAX_RESULT_BYTES,
            max_result_items: DEFAULT_MCP_MAX_RESULT_ITEMS,
            max_result_depth: DEFAULT_MCP_MAX_RESULT_DEPTH,
        };
        match server.store.load(&server.namespace)? {
            Some(existing) => existing.validate()?,
            None => {
                let mut initial = McpServerState::new(registry);
                initial.storage_revision = 1;
                server
                    .store
                    .compare_and_swap(&server.namespace, None, &initial)?;
            }
        }
        Ok(server)
    }

    pub fn with_limits(mut self, max_request_bytes: usize, max_receipts: usize) -> Self {
        self.max_request_bytes = max_request_bytes.max(1);
        self.max_receipts = max_receipts.max(1);
        self
    }

    pub fn with_clock(mut self, clock: Arc<dyn McpClock>) -> Self {
        self.clock = clock;
        self
    }

    pub fn with_retention_limits(mut self, max_tasks: usize, retention_ms: u64) -> Self {
        self.max_tasks = max_tasks.max(1);
        self.retention_ms = retention_ms.max(1);
        self
    }

    pub fn with_result_limits(
        mut self,
        max_bytes: usize,
        max_items: usize,
        max_depth: usize,
    ) -> Self {
        self.max_result_bytes = max_bytes.max(1);
        self.max_result_items = max_items.max(1);
        self.max_result_depth = max_depth.max(1);
        self
    }

    pub(crate) fn now_unix_ms(&self) -> u64 {
        self.clock.now_unix_ms()
    }

    /// Atomically update a tool contract. Active tasks are moved to `input_required` by the
    /// registry when the definition changed, preventing stale-schema execution after restart.
    pub fn upsert_tool(&self, definition: McpToolDefinition) -> ProtocolResult<bool> {
        let (mut state, expected) = self.load_state()?;
        let changed = state.registry.upsert_tool(definition)?;
        if changed {
            let now = self.clock.now_rfc3339();
            for task in state.registry.tasks().values() {
                if task.status == McpTaskStatus::InputRequired {
                    if let Some(metadata) = state.task_metadata.get_mut(&task.task_id) {
                        metadata.last_updated_at = now.clone();
                    }
                }
            }
            self.commit(state, expected)?;
        }
        Ok(changed)
    }

    /// Parse and dispatch one MCP JSON-RPC request or notification.
    pub fn handle(
        &self,
        connection_id: &str,
        principal: Option<&ProtocolPrincipal>,
        payload: &[u8],
    ) -> ProtocolResult<McpJsonRpcDispatch> {
        if connection_id.trim().is_empty() {
            return Err(ProtocolError::invalid(
                "MCP connection_id must not be empty",
            ));
        }
        if connection_id.len() > 512 || connection_id.chars().any(char::is_control) {
            return Err(ProtocolError::invalid("MCP connection_id is invalid"));
        }
        if payload.len() > self.max_request_bytes {
            return Ok(McpJsonRpcDispatch::response(jsonrpc_error(
                Value::Null,
                JSONRPC_INVALID_REQUEST,
                "MCP JSON-RPC request exceeds the configured byte limit",
                None,
            )));
        }
        let raw: Value = match serde_json::from_slice(payload) {
            Ok(value) => value,
            Err(error) => {
                return Ok(McpJsonRpcDispatch::response(jsonrpc_error(
                    Value::Null,
                    JSONRPC_PARSE_ERROR,
                    "Parse error",
                    Some(json!({"detail": error.to_string()})),
                )))
            }
        };
        let request = match ParsedRequest::parse(&raw) {
            Ok(request) => request,
            Err((id, message)) => {
                return Ok(McpJsonRpcDispatch::response(jsonrpc_error(
                    id,
                    JSONRPC_INVALID_REQUEST,
                    message,
                    None,
                )))
            }
        };

        let (mut state, mut expected) = self.load_state()?;
        if self.collect_garbage(&mut state)? {
            self.commit(state, expected)?;
            (state, expected) = self.load_state()?;
        }
        let receipt_key = request.id.as_ref().map(|id| receipt_key(connection_id, id));
        let request_hash = stable_input_hash(&raw);
        if let Some(key) = &receipt_key {
            if let Some(receipt) = state.receipts.get(key) {
                if receipt.request_hash != request_hash {
                    return Ok(McpJsonRpcDispatch::response(jsonrpc_error(
                        request.id.clone().unwrap_or(Value::Null),
                        MCP_CONFLICT,
                        "JSON-RPC id was reused with different request content",
                        None,
                    )));
                }
                return Ok(replay_receipt(receipt));
            }
            if state
                .connections
                .get(connection_id)
                .is_some_and(|connection| connection.receipt_namespace_retired)
            {
                return Ok(reconciliation_required(request.id.clone()));
            }
            if state.receipts.len() >= self.max_receipts {
                prune_completed_safe_receipt(
                    &mut state,
                    self.clock.now_unix_ms(),
                    self.retention_ms,
                );
            }
            if state.receipts.len() >= self.max_receipts {
                return Ok(McpJsonRpcDispatch::response(jsonrpc_error(
                    request.id.clone().unwrap_or(Value::Null),
                    JSONRPC_INTERNAL_ERROR,
                    "MCP request receipt capacity exhausted",
                    None,
                )));
            }
        }

        if request.method == "initialize" {
            return self.handle_initialize(
                &mut state,
                expected,
                connection_id,
                principal,
                request,
                request_hash,
            );
        }
        if request.method == "notifications/initialized" {
            return self.handle_initialized_notification(
                &mut state,
                expected,
                connection_id,
                principal,
                request,
            );
        }

        let Some(connection) = state.connections.get(connection_id) else {
            return Ok(
                request.error_or_ignore(MCP_NOT_INITIALIZED, "MCP connection is not initialized")
            );
        };
        let Some(principal) = principal else {
            return Ok(
                request.error_or_ignore(MCP_FORBIDDEN, "authenticated principal is required")
            );
        };
        if !connection.matches(principal) {
            return Ok(request.error_or_ignore(
                MCP_FORBIDDEN,
                "MCP connection authorization context changed",
            ));
        }
        if !connection.initialized {
            return Ok(request.error_or_ignore(
                MCP_NOT_INITIALIZED,
                "notifications/initialized must complete the MCP handshake",
            ));
        }

        self.dispatch_initialized(
            state,
            expected,
            connection_id,
            principal,
            request,
            request_hash,
        )
    }

    fn handle_initialize(
        &self,
        state: &mut McpServerState,
        expected: Option<u64>,
        connection_id: &str,
        principal: Option<&ProtocolPrincipal>,
        request: ParsedRequest,
        request_hash: String,
    ) -> ProtocolResult<McpJsonRpcDispatch> {
        let Some(id) = request.id.clone() else {
            return Ok(McpJsonRpcDispatch::notification());
        };
        let Some(principal) = principal else {
            let response = jsonrpc_error(
                id,
                MCP_FORBIDDEN,
                "authenticated principal is required",
                None,
            );
            self.record_completed(
                state,
                connection_id,
                &request,
                request_hash,
                response.clone(),
            )?;
            self.commit(state.clone(), expected)?;
            return Ok(McpJsonRpcDispatch::response(response));
        };
        if state.connections.contains_key(connection_id) {
            let response = jsonrpc_error(
                id,
                JSONRPC_INVALID_REQUEST,
                "MCP connection was already initialized",
                None,
            );
            self.record_completed(
                state,
                connection_id,
                &request,
                request_hash,
                response.clone(),
            )?;
            self.commit(state.clone(), expected)?;
            return Ok(McpJsonRpcDispatch::response(response));
        }

        let version = request
            .params
            .get("protocolVersion")
            .and_then(Value::as_str);
        let client_name = request
            .params
            .pointer("/clientInfo/name")
            .and_then(Value::as_str);
        let client_version = request
            .params
            .pointer("/clientInfo/version")
            .and_then(Value::as_str);
        let capabilities_valid = request
            .params
            .get("capabilities")
            .is_some_and(Value::is_object);
        if version != Some(MCP_SERVER_CONTRACT_REVISION)
            || client_name.is_none_or(str::is_empty)
            || client_version.is_none_or(str::is_empty)
            || !capabilities_valid
        {
            let response = jsonrpc_error(
                id,
                JSONRPC_INVALID_PARAMS,
                "unsupported protocolVersion or invalid clientInfo",
                Some(json!({"supportedProtocolVersion": MCP_SERVER_CONTRACT_REVISION})),
            );
            self.record_completed(
                state,
                connection_id,
                &request,
                request_hash,
                response.clone(),
            )?;
            self.commit(state.clone(), expected)?;
            return Ok(McpJsonRpcDispatch::response(response));
        }

        state.connections.insert(
            connection_id.to_owned(),
            McpConnectionState {
                subject: principal.subject.clone(),
                tenant_id: principal.tenant_id.clone(),
                initialized: false,
                client_name: client_name.unwrap_or_default().to_owned(),
                client_version: client_version.unwrap_or_default().to_owned(),
                receipt_namespace_retired: false,
            },
        );
        let response = jsonrpc_result(
            id,
            json!({
                "protocolVersion": MCP_SERVER_CONTRACT_REVISION,
                "capabilities": {
                    "tools": {"listChanged": true},
                    "resources": {"listChanged": true},
                    "prompts": {"listChanged": true},
                    "tasks": {
                        "list": {},
                        "cancel": {},
                        "requests": {"tools": {"call": {}}}
                    }
                },
                "serverInfo": server_info_wire(&self.server_info)
            }),
        );
        self.record_completed(
            state,
            connection_id,
            &request,
            request_hash,
            response.clone(),
        )?;
        self.commit(state.clone(), expected)?;
        Ok(McpJsonRpcDispatch::response(response))
    }

    fn handle_initialized_notification(
        &self,
        state: &mut McpServerState,
        expected: Option<u64>,
        connection_id: &str,
        principal: Option<&ProtocolPrincipal>,
        request: ParsedRequest,
    ) -> ProtocolResult<McpJsonRpcDispatch> {
        if request.id.is_some() {
            return Ok(request.error_or_ignore(
                JSONRPC_INVALID_REQUEST,
                "notifications/initialized must not contain an id",
            ));
        }
        let Some(principal) = principal else {
            return Ok(McpJsonRpcDispatch::notification());
        };
        let Some(connection) = state.connections.get_mut(connection_id) else {
            return Ok(McpJsonRpcDispatch::notification());
        };
        if !connection.matches(principal) {
            return Ok(McpJsonRpcDispatch::notification());
        }
        connection.initialized = true;
        self.commit(state.clone(), expected)?;
        Ok(McpJsonRpcDispatch::notification())
    }

    fn dispatch_initialized(
        &self,
        mut state: McpServerState,
        expected: Option<u64>,
        connection_id: &str,
        principal: &ProtocolPrincipal,
        request: ParsedRequest,
        request_hash: String,
    ) -> ProtocolResult<McpJsonRpcDispatch> {
        match request.method.as_str() {
            "tools/list" => self.list_tools(
                state,
                expected,
                connection_id,
                principal,
                request,
                request_hash,
            ),
            "resources/list" => self.list_resources(
                state,
                expected,
                connection_id,
                principal,
                request,
                request_hash,
            ),
            "prompts/list" => self.list_prompts(
                state,
                expected,
                connection_id,
                principal,
                request,
                request_hash,
            ),
            "tools/call" => self.call_tool(
                state,
                expected,
                connection_id,
                principal,
                request,
                request_hash,
            ),
            "resources/read" => self.read_resource(
                state,
                expected,
                connection_id,
                principal,
                request,
                request_hash,
            ),
            "prompts/get" => self.get_prompt(
                state,
                expected,
                connection_id,
                principal,
                request,
                request_hash,
            ),
            "tasks/get" => self.get_task(
                state,
                expected,
                connection_id,
                principal,
                request,
                request_hash,
            ),
            "tasks/list" => self.list_tasks(
                state,
                expected,
                connection_id,
                principal,
                request,
                request_hash,
            ),
            "tasks/cancel" => self.cancel_task(
                state,
                expected,
                connection_id,
                principal,
                request,
                request_hash,
            ),
            "tasks/result" => self.task_result(
                state,
                expected,
                connection_id,
                principal,
                request,
                request_hash,
            ),
            "notifications/cancelled" => {
                self.cancel_request_notification(state, expected, connection_id, principal, request)
            }
            _ => {
                if request.id.is_none() {
                    Ok(McpJsonRpcDispatch::notification())
                } else {
                    let response = jsonrpc_error(
                        request.id.clone().unwrap_or(Value::Null),
                        JSONRPC_METHOD_NOT_FOUND,
                        "Method not found",
                        None,
                    );
                    self.record_completed(
                        &mut state,
                        connection_id,
                        &request,
                        request_hash,
                        response.clone(),
                    )?;
                    self.commit(state, expected)?;
                    Ok(McpJsonRpcDispatch::response(response))
                }
            }
        }
    }

    fn list_tools(
        &self,
        mut state: McpServerState,
        expected: Option<u64>,
        connection_id: &str,
        principal: &ProtocolPrincipal,
        request: ParsedRequest,
        request_hash: String,
    ) -> ProtocolResult<McpJsonRpcDispatch> {
        let Some(id) = request.id.clone() else {
            return Ok(McpJsonRpcDispatch::notification());
        };
        let decision = state
            .registry
            .prepare_list_tools(correlation(connection_id, &request)?, Some(principal));
        let response = match decision.action() {
            Some(McpServerAction::ListTools { tools }) => jsonrpc_result(
                id,
                json!({"tools": tools.iter().map(tool_wire).collect::<ProtocolResult<Vec<_>>>()?}),
            ),
            _ => decision_error(id, &decision.envelope),
        };
        self.record_completed(
            &mut state,
            connection_id,
            &request,
            request_hash,
            response.clone(),
        )?;
        self.commit(state, expected)?;
        Ok(McpJsonRpcDispatch {
            response: Some(response),
            outbound: Vec::new(),
            governed_action: Some(decision),
            pending: None,
        })
    }

    fn list_resources(
        &self,
        mut state: McpServerState,
        expected: Option<u64>,
        connection_id: &str,
        principal: &ProtocolPrincipal,
        request: ParsedRequest,
        request_hash: String,
    ) -> ProtocolResult<McpJsonRpcDispatch> {
        let Some(id) = request.id.clone() else {
            return Ok(McpJsonRpcDispatch::notification());
        };
        let decision = state
            .registry
            .prepare_list_resources(correlation(connection_id, &request)?, Some(principal));
        let response = match decision.action() {
            Some(McpServerAction::ListResources { resources }) => jsonrpc_result(
                id,
                json!({"resources": resources.iter().map(|resource| json!({
                    "uri": resource.uri,
                    "name": resource.name,
                    "description": resource.description,
                    "mimeType": resource.mime_type
                })).collect::<Vec<_>>() }),
            ),
            _ => decision_error(id, &decision.envelope),
        };
        self.record_completed(
            &mut state,
            connection_id,
            &request,
            request_hash,
            response.clone(),
        )?;
        self.commit(state, expected)?;
        Ok(McpJsonRpcDispatch {
            response: Some(response),
            outbound: Vec::new(),
            governed_action: Some(decision),
            pending: None,
        })
    }

    fn list_prompts(
        &self,
        mut state: McpServerState,
        expected: Option<u64>,
        connection_id: &str,
        principal: &ProtocolPrincipal,
        request: ParsedRequest,
        request_hash: String,
    ) -> ProtocolResult<McpJsonRpcDispatch> {
        let Some(id) = request.id.clone() else {
            return Ok(McpJsonRpcDispatch::notification());
        };
        let decision = state
            .registry
            .prepare_list_prompts(correlation(connection_id, &request)?, Some(principal));
        let response = match decision.action() {
            Some(McpServerAction::ListPrompts { prompts }) => jsonrpc_result(
                id,
                json!({"prompts": prompts.iter().map(|prompt| json!({
                    "name": prompt.name,
                    "description": prompt.description,
                    "arguments": prompt.arguments
                })).collect::<Vec<_>>() }),
            ),
            _ => decision_error(id, &decision.envelope),
        };
        self.record_completed(
            &mut state,
            connection_id,
            &request,
            request_hash,
            response.clone(),
        )?;
        self.commit(state, expected)?;
        Ok(McpJsonRpcDispatch {
            response: Some(response),
            outbound: Vec::new(),
            governed_action: Some(decision),
            pending: None,
        })
    }

    fn call_tool(
        &self,
        mut state: McpServerState,
        expected: Option<u64>,
        connection_id: &str,
        principal: &ProtocolPrincipal,
        request: ParsedRequest,
        request_hash: String,
    ) -> ProtocolResult<McpJsonRpcDispatch> {
        let Some(id) = request.id.clone() else {
            return Ok(McpJsonRpcDispatch::notification());
        };
        if state.registry.tasks().len() >= self.max_tasks {
            return self.complete_wire_error(
                WireRequestContext {
                    state,
                    expected_revision: expected,
                    connection_id,
                    request,
                    request_hash,
                },
                JSONRPC_INTERNAL_ERROR,
                "MCP task capacity exhausted",
            );
        }
        let Some(name) = request.params.get("name").and_then(Value::as_str) else {
            return self.complete_wire_error(
                WireRequestContext {
                    state,
                    expected_revision: expected,
                    connection_id,
                    request,
                    request_hash,
                },
                JSONRPC_INVALID_PARAMS,
                "tools/call requires name",
            );
        };
        let arguments = request
            .params
            .get("arguments")
            .cloned()
            .unwrap_or_else(|| json!({}));
        if !arguments.is_object() {
            return self.complete_wire_error(
                WireRequestContext {
                    state,
                    expected_revision: expected,
                    connection_id,
                    request,
                    request_hash,
                },
                JSONRPC_INVALID_PARAMS,
                "tools/call arguments must be an object",
            );
        }
        let input_error = state.registry.tools().get(name).and_then(|tool| {
            jsonschema::validator_for(&tool.input_schema)
                .ok()
                .and_then(|validator| {
                    validator
                        .validate(&arguments)
                        .err()
                        .map(|_| format!("tool `{name}` arguments do not match its inputSchema"))
                })
        });
        let task_params = request.params.get("task");
        let execution_mode = if task_params.is_some() {
            McpToolExecutionMode::Task
        } else {
            McpToolExecutionMode::Direct
        };
        let ttl = task_params
            .and_then(|value| value.get("ttl"))
            .map_or(Some(DEFAULT_MCP_TASK_TTL_MS), Value::as_u64);
        if task_params.is_some_and(|value| !value.is_object())
            || ttl.is_none_or(|ttl| ttl == 0 || ttl > MAX_MCP_TASK_TTL_MS)
        {
            return self.complete_wire_error(
                WireRequestContext {
                    state,
                    expected_revision: expected,
                    connection_id,
                    request,
                    request_hash,
                },
                JSONRPC_INVALID_PARAMS,
                "task.ttl is invalid or exceeds the configured maximum",
            );
        }
        let ttl = ttl.expect("validated task ttl is present");
        let progress_token = request.params.pointer("/_meta/progressToken").cloned();
        if progress_token
            .as_ref()
            .is_some_and(|value| !valid_progress_token(value))
        {
            return self.complete_wire_error(
                WireRequestContext {
                    state,
                    expected_revision: expected,
                    connection_id,
                    request,
                    request_hash,
                },
                JSONRPC_INVALID_PARAMS,
                "progressToken must be a string or number",
            );
        }

        let decision = state.registry.prepare_tool_call(
            McpToolCallRequest {
                name: name.to_owned(),
                arguments,
                execution_mode,
            },
            correlation(connection_id, &request)?,
            Some(principal),
        );
        let Some(action) = decision.action() else {
            let code = if matches!(
                decision.envelope.authorization,
                GovernanceAuthorization::Denied {
                    code: super::GovernanceDenialCode::StateConflict,
                    ..
                }
            ) {
                JSONRPC_METHOD_NOT_FOUND
            } else {
                MCP_FORBIDDEN
            };
            let response = decision_error_with_code(id, &decision.envelope, code);
            self.record_completed(
                &mut state,
                connection_id,
                &request,
                request_hash,
                response.clone(),
            )?;
            self.commit(state, expected)?;
            return Ok(McpJsonRpcDispatch {
                response: Some(response),
                outbound: Vec::new(),
                governed_action: Some(decision),
                pending: None,
            });
        };
        let task = action_task(action)
            .expect("tool call decisions always carry a task")
            .clone();
        let now_unix_ms = self.clock.now_unix_ms();
        let now = self.clock.now_rfc3339();
        state.task_metadata.insert(
            task.task_id.clone(),
            McpTaskWireMetadata {
                connection_id: connection_id.to_owned(),
                created_at: now.clone(),
                last_updated_at: now,
                created_at_unix_ms: now_unix_ms,
                last_updated_at_unix_ms: now_unix_ms,
                expires_at_unix_ms: now_unix_ms.saturating_add(ttl),
                ttl,
                poll_interval: DEFAULT_MCP_POLL_INTERVAL_MS,
                progress_token,
            },
        );

        if let Some(message) = input_error {
            let tool_result = tool_execution_error(&message);
            state
                .registry
                .complete_tool_result(&task.task_id, tool_result.clone(), true)?;
            let failed_task = state.registry.tasks()[&task.task_id].clone();
            update_task_time(
                &mut state,
                &failed_task,
                &self.clock.now_rfc3339(),
                now_unix_ms,
            );
            let response = if execution_mode == McpToolExecutionMode::Task {
                jsonrpc_result(
                    id,
                    json!({"task": task_wire(&failed_task, state.task_metadata.get(&task.task_id))}),
                )
            } else {
                jsonrpc_result(id, tool_result)
            };
            self.record_completed(
                &mut state,
                connection_id,
                &request,
                request_hash,
                response.clone(),
            )?;
            self.commit(state, expected)?;
            return Ok(McpJsonRpcDispatch::response(response));
        }

        let (response, pending_state, pending) = if execution_mode == McpToolExecutionMode::Task {
            let response = jsonrpc_result(
                id.clone(),
                json!({"task": task_wire(&task, state.task_metadata.get(&task.task_id))}),
            );
            (
                Some(response.clone()),
                McpReceiptState::Completed {
                    response,
                    completed_at_unix_ms: now_unix_ms,
                },
                None,
            )
        } else {
            (
                None,
                McpReceiptState::PendingHost {
                    operation: "tools/call".into(),
                    task_id: Some(task.task_id.clone()),
                },
                Some(McpPendingResponse {
                    connection_id: connection_id.to_owned(),
                    request_id: id.clone(),
                    operation: "tools/call".into(),
                }),
            )
        };
        self.insert_receipt(
            &mut state,
            connection_id,
            &request,
            request_hash,
            pending_state,
        )?;
        self.commit(state, expected)?;
        Ok(McpJsonRpcDispatch {
            response,
            outbound: Vec::new(),
            governed_action: Some(decision),
            pending,
        })
    }

    fn read_resource(
        &self,
        state: McpServerState,
        expected: Option<u64>,
        connection_id: &str,
        principal: &ProtocolPrincipal,
        request: ParsedRequest,
        request_hash: String,
    ) -> ProtocolResult<McpJsonRpcDispatch> {
        let Some(id) = request.id.clone() else {
            return Ok(McpJsonRpcDispatch::notification());
        };
        let Some(uri) = request.params.get("uri").and_then(Value::as_str) else {
            return self.complete_wire_error(
                WireRequestContext {
                    state,
                    expected_revision: expected,
                    connection_id,
                    request,
                    request_hash,
                },
                JSONRPC_INVALID_PARAMS,
                "resources/read requires uri",
            );
        };
        let decision = state.registry.prepare_read_resource(
            uri,
            correlation(connection_id, &request)?,
            Some(principal),
        );
        self.pending_host_dispatch(
            WireRequestContext {
                state,
                expected_revision: expected,
                connection_id,
                request,
                request_hash,
            },
            id,
            decision,
            "resources/read",
        )
    }

    fn get_prompt(
        &self,
        state: McpServerState,
        expected: Option<u64>,
        connection_id: &str,
        principal: &ProtocolPrincipal,
        request: ParsedRequest,
        request_hash: String,
    ) -> ProtocolResult<McpJsonRpcDispatch> {
        let Some(id) = request.id.clone() else {
            return Ok(McpJsonRpcDispatch::notification());
        };
        let Some(name) = request.params.get("name").and_then(Value::as_str) else {
            return self.complete_wire_error(
                WireRequestContext {
                    state,
                    expected_revision: expected,
                    connection_id,
                    request,
                    request_hash,
                },
                JSONRPC_INVALID_PARAMS,
                "prompts/get requires name",
            );
        };
        let arguments = match request.params.get("arguments") {
            None => BTreeMap::new(),
            Some(Value::Object(values)) if values.values().all(Value::is_string) => values
                .iter()
                .map(|(key, value)| {
                    (
                        key.clone(),
                        value
                            .as_str()
                            .expect("checked prompt argument type")
                            .to_owned(),
                    )
                })
                .collect(),
            Some(_) => {
                return self.complete_wire_error(
                    WireRequestContext {
                        state,
                        expected_revision: expected,
                        connection_id,
                        request,
                        request_hash,
                    },
                    JSONRPC_INVALID_PARAMS,
                    "prompts/get arguments must be an object of strings",
                )
            }
        };
        let decision = state.registry.prepare_render_prompt(
            name,
            arguments,
            correlation(connection_id, &request)?,
            Some(principal),
        );
        self.pending_host_dispatch(
            WireRequestContext {
                state,
                expected_revision: expected,
                connection_id,
                request,
                request_hash,
            },
            id,
            decision,
            "prompts/get",
        )
    }

    fn pending_host_dispatch(
        &self,
        context: WireRequestContext<'_>,
        id: Value,
        decision: super::GovernedAction<McpServerAction>,
        operation: &str,
    ) -> ProtocolResult<McpJsonRpcDispatch> {
        let WireRequestContext {
            mut state,
            expected_revision,
            connection_id,
            request,
            request_hash,
        } = context;
        if !decision.is_authorized() {
            let response = decision_error(id, &decision.envelope);
            self.record_completed(
                &mut state,
                connection_id,
                &request,
                request_hash,
                response.clone(),
            )?;
            self.commit(state, expected_revision)?;
            return Ok(McpJsonRpcDispatch {
                response: Some(response),
                outbound: Vec::new(),
                governed_action: Some(decision),
                pending: None,
            });
        }
        self.insert_receipt(
            &mut state,
            connection_id,
            &request,
            request_hash,
            McpReceiptState::PendingHost {
                operation: operation.into(),
                task_id: None,
            },
        )?;
        self.commit(state, expected_revision)?;
        Ok(McpJsonRpcDispatch {
            response: None,
            outbound: Vec::new(),
            governed_action: Some(decision),
            pending: Some(McpPendingResponse {
                connection_id: connection_id.to_owned(),
                request_id: id,
                operation: operation.into(),
            }),
        })
    }

    fn get_task(
        &self,
        mut state: McpServerState,
        expected: Option<u64>,
        connection_id: &str,
        principal: &ProtocolPrincipal,
        request: ParsedRequest,
        request_hash: String,
    ) -> ProtocolResult<McpJsonRpcDispatch> {
        let maintenance = self.expire_due_tasks(&mut state)?;
        let Some(id) = request.id.clone() else {
            return Ok(McpJsonRpcDispatch::notification());
        };
        let Some(task_id) = request.params.get("taskId").and_then(Value::as_str) else {
            return self.complete_wire_error(
                WireRequestContext {
                    state,
                    expected_revision: expected,
                    connection_id,
                    request,
                    request_hash,
                },
                JSONRPC_INVALID_PARAMS,
                "tasks/get requires taskId",
            );
        };
        let decision = state.registry.prepare_get_task(
            task_id,
            correlation(connection_id, &request)?,
            Some(principal),
        );
        let response = match decision.action() {
            Some(McpServerAction::GetTask { task }) => {
                jsonrpc_result(id, task_wire(task, state.task_metadata.get(task_id)))
            }
            _ => decision_error(id, &decision.envelope),
        };
        self.record_completed(
            &mut state,
            connection_id,
            &request,
            request_hash,
            response.clone(),
        )?;
        self.commit(state, expected)?;
        Ok(McpJsonRpcDispatch {
            response: Some(response),
            outbound: maintenance,
            governed_action: Some(decision),
            pending: None,
        })
    }

    fn list_tasks(
        &self,
        mut state: McpServerState,
        expected: Option<u64>,
        connection_id: &str,
        principal: &ProtocolPrincipal,
        request: ParsedRequest,
        request_hash: String,
    ) -> ProtocolResult<McpJsonRpcDispatch> {
        let maintenance = self.expire_due_tasks(&mut state)?;
        let Some(id) = request.id.clone() else {
            return Ok(McpJsonRpcDispatch::notification());
        };
        let decision = state
            .registry
            .prepare_list_tasks(correlation(connection_id, &request)?, Some(principal));
        let response = match decision.action() {
            Some(McpServerAction::ListTasks { tasks }) => {
                let cursor = request.params.get("cursor").and_then(Value::as_str);
                let start = if let Some(cursor) = cursor {
                    let Some(index) = tasks.iter().position(|task| task.task_id == cursor) else {
                        let response = jsonrpc_error(
                            id,
                            JSONRPC_INVALID_PARAMS,
                            "tasks/list cursor is invalid",
                            None,
                        );
                        self.record_completed(
                            &mut state,
                            connection_id,
                            &request,
                            request_hash,
                            response.clone(),
                        )?;
                        self.commit(state, expected)?;
                        return Ok(McpJsonRpcDispatch {
                            response: Some(response),
                            outbound: maintenance,
                            governed_action: Some(decision),
                            pending: None,
                        });
                    };
                    index + 1
                } else {
                    0
                };
                let page: Vec<_> = tasks.iter().skip(start).take(100).collect();
                let next_cursor = (start + page.len() < tasks.len())
                    .then(|| page.last().map(|task| task.task_id.clone()))
                    .flatten();
                let mut result = json!({"tasks": page.iter().map(|task| task_wire(task, state.task_metadata.get(&task.task_id))).collect::<Vec<_>>()});
                if let Some(next_cursor) = next_cursor {
                    result["nextCursor"] = Value::String(next_cursor);
                }
                jsonrpc_result(id, result)
            }
            _ => decision_error(id, &decision.envelope),
        };
        self.record_completed(
            &mut state,
            connection_id,
            &request,
            request_hash,
            response.clone(),
        )?;
        self.commit(state, expected)?;
        Ok(McpJsonRpcDispatch {
            response: Some(response),
            outbound: maintenance,
            governed_action: Some(decision),
            pending: None,
        })
    }

    fn cancel_task(
        &self,
        mut state: McpServerState,
        expected: Option<u64>,
        connection_id: &str,
        principal: &ProtocolPrincipal,
        request: ParsedRequest,
        request_hash: String,
    ) -> ProtocolResult<McpJsonRpcDispatch> {
        let maintenance = self.expire_due_tasks(&mut state)?;
        let Some(id) = request.id.clone() else {
            return Ok(McpJsonRpcDispatch::notification());
        };
        let Some(task_id) = request.params.get("taskId").and_then(Value::as_str) else {
            return self.complete_wire_error(
                WireRequestContext {
                    state,
                    expected_revision: expected,
                    connection_id,
                    request,
                    request_hash,
                },
                JSONRPC_INVALID_PARAMS,
                "tasks/cancel requires taskId",
            );
        };
        if state
            .registry
            .tasks()
            .get(task_id)
            .is_none_or(|task| !task.advertised)
        {
            return self.complete_wire_error(
                WireRequestContext {
                    state,
                    expected_revision: expected,
                    connection_id,
                    request,
                    request_hash,
                },
                JSONRPC_INVALID_PARAMS,
                "MCP task is not accessible",
            );
        }
        let decision = state.registry.prepare_cancel_task(
            task_id,
            correlation(connection_id, &request)?,
            Some(principal),
        );
        let response = match decision.action() {
            Some(McpServerAction::CancelTask { task }) => {
                update_task_time(
                    &mut state,
                    task,
                    &self.clock.now_rfc3339(),
                    self.clock.now_unix_ms(),
                );
                jsonrpc_result(id, task_wire(task, state.task_metadata.get(task_id)))
            }
            _ => decision_error_with_code(id, &decision.envelope, JSONRPC_INVALID_PARAMS),
        };
        self.record_completed(
            &mut state,
            connection_id,
            &request,
            request_hash,
            response.clone(),
        )?;
        self.commit(state, expected)?;
        Ok(McpJsonRpcDispatch {
            response: Some(response),
            outbound: maintenance,
            governed_action: Some(decision),
            pending: None,
        })
    }

    fn task_result(
        &self,
        mut state: McpServerState,
        expected: Option<u64>,
        connection_id: &str,
        principal: &ProtocolPrincipal,
        request: ParsedRequest,
        request_hash: String,
    ) -> ProtocolResult<McpJsonRpcDispatch> {
        let maintenance = self.expire_due_tasks(&mut state)?;
        let Some(id) = request.id.clone() else {
            return Ok(McpJsonRpcDispatch::notification());
        };
        let Some(task_id) = request.params.get("taskId").and_then(Value::as_str) else {
            return self.complete_wire_error(
                WireRequestContext {
                    state,
                    expected_revision: expected,
                    connection_id,
                    request,
                    request_hash,
                },
                JSONRPC_INVALID_PARAMS,
                "tasks/result requires taskId",
            );
        };
        let decision = state.registry.prepare_get_task(
            task_id,
            correlation(connection_id, &request)?,
            Some(principal),
        );
        let Some(McpServerAction::GetTask { task }) = decision.action() else {
            let response = decision_error(id, &decision.envelope);
            self.record_completed(
                &mut state,
                connection_id,
                &request,
                request_hash,
                response.clone(),
            )?;
            self.commit(state, expected)?;
            return Ok(McpJsonRpcDispatch {
                response: Some(response),
                outbound: maintenance,
                governed_action: Some(decision),
                pending: None,
            });
        };
        if task.status.is_terminal() {
            let response = terminal_task_response(id, task);
            self.record_completed(
                &mut state,
                connection_id,
                &request,
                request_hash,
                response.clone(),
            )?;
            self.commit(state, expected)?;
            return Ok(McpJsonRpcDispatch {
                response: Some(response),
                outbound: maintenance,
                governed_action: Some(decision),
                pending: None,
            });
        }
        self.insert_receipt(
            &mut state,
            connection_id,
            &request,
            request_hash,
            McpReceiptState::WaitingForTask {
                task_id: task_id.to_owned(),
            },
        )?;
        self.commit(state, expected)?;
        Ok(McpJsonRpcDispatch {
            response: None,
            outbound: maintenance,
            governed_action: Some(decision),
            pending: Some(McpPendingResponse {
                connection_id: connection_id.to_owned(),
                request_id: id,
                operation: "tasks/result".into(),
            }),
        })
    }

    fn cancel_request_notification(
        &self,
        mut state: McpServerState,
        expected: Option<u64>,
        connection_id: &str,
        principal: &ProtocolPrincipal,
        request: ParsedRequest,
    ) -> ProtocolResult<McpJsonRpcDispatch> {
        let maintenance = self.expire_due_tasks(&mut state)?;
        if request.id.is_some() {
            return Ok(request.error_or_ignore(
                JSONRPC_INVALID_REQUEST,
                "notifications/cancelled must not contain an id",
            ));
        }
        let Some(request_id) = request
            .params
            .get("requestId")
            .filter(|value| valid_request_id(value))
            .cloned()
        else {
            return Ok(McpJsonRpcDispatch::notification());
        };
        let key = receipt_key(connection_id, &request_id);
        let task_id = state
            .receipts
            .get(&key)
            .and_then(|receipt| match &receipt.state {
                McpReceiptState::PendingHost { task_id, .. } => task_id.clone(),
                McpReceiptState::WaitingForTask { task_id } => Some(task_id.clone()),
                McpReceiptState::Completed { .. } => None,
            });
        let Some(task_id) = task_id else {
            return Ok(McpJsonRpcDispatch::notification());
        };
        let decision = state.registry.prepare_cancel_task(
            &task_id,
            correlation(connection_id, &request)?,
            Some(principal),
        );
        if !decision.is_authorized() {
            return Ok(McpJsonRpcDispatch {
                response: None,
                outbound: Vec::new(),
                governed_action: Some(decision),
                pending: None,
            });
        }
        self.commit(state, expected)?;
        Ok(McpJsonRpcDispatch {
            response: None,
            outbound: maintenance,
            governed_action: Some(decision),
            pending: None,
        })
    }

    /// Complete a host action. For direct requests this releases the original response; for task
    /// execution it also releases all pending `tasks/result` requests and emits a status update.
    pub fn complete_tool(
        &self,
        task_id: &str,
        result: Value,
    ) -> ProtocolResult<Vec<McpOutboundMessage>> {
        validate_tool_result(
            &result,
            self.max_result_bytes,
            self.max_result_items,
            self.max_result_depth,
        )?;
        let (mut state, expected) = self.load_state()?;
        let is_error = result
            .get("isError")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        state
            .registry
            .complete_tool_result(task_id, result.clone(), is_error)?;
        let now_unix_ms = self.clock.now_unix_ms();
        let now = self.clock.now_rfc3339();
        if let Some(task) = state.registry.tasks().get(task_id).cloned() {
            update_task_time(&mut state, &task, &now, now_unix_ms);
        }
        let mut outbound =
            complete_direct_tool_receipts(&mut state, task_id, Ok(result.clone()), now_unix_ms);
        outbound.extend(complete_task_waiters(
            &mut state,
            task_id,
            Ok(result),
            now_unix_ms,
        ));
        outbound.extend(task_notifications(&state, task_id));
        self.commit(state, expected)?;
        Ok(outbound)
    }

    pub fn fail_tool(
        &self,
        task_id: &str,
        error: ProtocolError,
    ) -> ProtocolResult<Vec<McpOutboundMessage>> {
        let (mut state, expected) = self.load_state()?;
        state.registry.fail_task(task_id, error.clone())?;
        let now_unix_ms = self.clock.now_unix_ms();
        let now = self.clock.now_rfc3339();
        if let Some(task) = state.registry.tasks().get(task_id).cloned() {
            update_task_time(&mut state, &task, &now, now_unix_ms);
        }
        let mut outbound =
            complete_direct_tool_receipts(&mut state, task_id, Err(error.clone()), now_unix_ms);
        outbound.extend(complete_task_waiters(
            &mut state,
            task_id,
            Err(error),
            now_unix_ms,
        ));
        outbound.extend(task_notifications(&state, task_id));
        self.commit(state, expected)?;
        Ok(outbound)
    }

    /// Finish a pending resource or prompt host action with its official result object.
    pub fn complete_pending(
        &self,
        pending: &McpPendingResponse,
        result: ProtocolResult<Value>,
    ) -> ProtocolResult<McpOutboundMessage> {
        if let Ok(value) = &result {
            validate_json_bounds(
                value,
                self.max_result_bytes,
                self.max_result_items,
                self.max_result_depth,
            )?;
        }
        let (mut state, expected) = self.load_state()?;
        let key = receipt_key(&pending.connection_id, &pending.request_id);
        let receipt = state
            .receipts
            .get_mut(&key)
            .ok_or_else(|| ProtocolError::not_found("MCP pending response is not registered"))?;
        let operation = match &receipt.state {
            McpReceiptState::PendingHost {
                operation,
                task_id: None,
            } => operation.clone(),
            _ => {
                return Err(ProtocolError::invalid_transition(
                    "MCP response is not pending host completion",
                ))
            }
        };
        if operation != pending.operation {
            return Err(ProtocolError::conflict(
                "MCP pending response operation mismatch",
            ));
        }
        let response = match result {
            Ok(result) => {
                validate_host_result(
                    &operation,
                    &result,
                    self.max_result_bytes,
                    self.max_result_items,
                    self.max_result_depth,
                )?;
                jsonrpc_result(pending.request_id.clone(), result)
            }
            Err(error) => protocol_error_response(pending.request_id.clone(), &error),
        };
        receipt.state = McpReceiptState::Completed {
            response: response.clone(),
            completed_at_unix_ms: self.clock.now_unix_ms(),
        };
        self.commit(state, expected)?;
        Ok(McpOutboundMessage {
            connection_id: pending.connection_id.clone(),
            message: response,
        })
    }

    /// Record monotonic task progress and emit official progress plus task status notifications.
    pub fn record_progress(
        &self,
        task_id: &str,
        progress: McpProgress,
    ) -> ProtocolResult<Vec<McpOutboundMessage>> {
        let (mut state, expected) = self.load_state()?;
        if self.task_is_expired(&state, task_id) {
            let outbound = self.expire_due_tasks(&mut state)?;
            self.commit(state, expected)?;
            return if outbound.is_empty() {
                Err(ProtocolError::conflict("MCP task TTL expired"))
            } else {
                Err(ProtocolError::conflict(
                    "MCP task TTL expired; pending clients were released",
                ))
            };
        }
        state.registry.record_progress(task_id, progress.clone())?;
        let task = state
            .registry
            .tasks()
            .get(task_id)
            .cloned()
            .ok_or_else(|| ProtocolError::not_found("MCP task is not registered"))?;
        update_task_time(
            &mut state,
            &task,
            &self.clock.now_rfc3339(),
            self.clock.now_unix_ms(),
        );
        let mut outbound = task_notifications(&state, task_id);
        if let Some(metadata) = state.task_metadata.get(task_id) {
            if let Some(token) = &metadata.progress_token {
                for connection_id in task_connections(&state, task_id) {
                    outbound.push(McpOutboundMessage {
                        connection_id,
                        message: json!({
                            "jsonrpc": MCP_JSONRPC_VERSION,
                            "method": "notifications/progress",
                            "params": {
                                "progressToken": token,
                                "progress": progress.progress,
                                "total": progress.total,
                                "message": progress.message,
                                "_meta": related_task_meta(task_id)
                            }
                        }),
                    });
                }
            }
        }
        self.commit(state, expected)?;
        Ok(outbound)
    }

    /// Persist a durable human decision and return the governed continuation action. This method
    /// is called by the approval subsystem after its own policy/audit checks; it cannot bypass the
    /// task owner or required scopes because the registry re-evaluates both.
    pub fn resolve_approval(
        &self,
        task_id: &str,
        response: super::McpApprovalResponse,
        principal: Option<&ProtocolPrincipal>,
    ) -> ProtocolResult<super::GovernedAction<McpServerAction>> {
        let (mut state, expected) = self.load_state()?;
        if self.task_is_expired(&state, task_id) {
            let _ = self.expire_due_tasks(&mut state)?;
            self.commit(state, expected)?;
            return Err(ProtocolError::conflict("MCP task TTL expired"));
        }
        let registry_revision = state.registry.revision();
        let correlation = CorrelationIdentity::new(
            stable_id("mcp_approval", &[task_id]),
            stable_id("mcp_approval_request", &[task_id, &response.approval_id]),
        )?;
        let decision =
            state
                .registry
                .prepare_resume_approval(task_id, response, correlation, principal);
        if state.registry.revision() != registry_revision {
            if let Some(task) = state.registry.tasks().get(task_id).cloned() {
                update_task_time(
                    &mut state,
                    &task,
                    &self.clock.now_rfc3339(),
                    self.clock.now_unix_ms(),
                );
            }
            self.commit(state, expected)?;
        }
        Ok(decision)
    }

    pub fn snapshot(&self) -> ProtocolResult<McpServerState> {
        self.load_state().map(|(state, _)| state)
    }

    /// Return resumable host actions after restart without allocating replacement task ids.
    pub fn recover_actions(
        &self,
        principal: Option<&ProtocolPrincipal>,
    ) -> ProtocolResult<Vec<super::GovernedAction<McpServerAction>>> {
        let (mut state, expected) = self.load_state()?;
        let revision = state.registry.revision();
        let _ = self.expire_due_tasks(&mut state)?;
        if state.registry.revision() != revision {
            self.commit(state, expected)?;
            (state, _) = self.load_state()?;
        }
        let mut actions = Vec::new();
        for task_id in state.registry.tasks().keys() {
            let correlation = CorrelationIdentity::new(
                stable_id("mcp_recovery", &[task_id]),
                stable_id("mcp_recovery_request", &[task_id]),
            )?;
            let decision = state
                .registry
                .prepare_recover_task(task_id, correlation, principal);
            if decision.is_authorized() {
                actions.push(decision);
            }
        }
        Ok(actions)
    }

    /// Close a transport connection and durably cancel its non-terminal tasks. Session identity is
    /// checked before any state is changed.
    pub fn terminate_connection(
        &self,
        connection_id: &str,
        principal: Option<&ProtocolPrincipal>,
    ) -> ProtocolResult<Vec<super::GovernedAction<McpServerAction>>> {
        let (mut state, expected) = self.load_state()?;
        let principal = principal.ok_or_else(|| {
            ProtocolError::new(
                ProtocolErrorCode::Unauthorized,
                "authenticated principal is required",
            )
        })?;
        let connection = state
            .connections
            .get(connection_id)
            .ok_or_else(|| ProtocolError::not_found("MCP connection is not registered"))?;
        if !connection.matches(principal) {
            return Err(ProtocolError::new(
                ProtocolErrorCode::Forbidden,
                "MCP connection authorization context changed",
            ));
        }
        let task_ids: Vec<_> = state
            .task_metadata
            .iter()
            .filter(|(_, metadata)| metadata.connection_id == connection_id)
            .filter_map(|(task_id, _)| {
                state
                    .registry
                    .tasks()
                    .get(task_id)
                    .filter(|task| !task.status.is_terminal())
                    .map(|_| task_id.clone())
            })
            .collect();
        let mut decisions = Vec::new();
        for task_id in task_ids {
            let correlation = CorrelationIdentity::new(
                stable_id("mcp_disconnect", &[connection_id, &task_id]),
                stable_id("mcp_disconnect_request", &[connection_id, &task_id]),
            )?;
            let decision =
                state
                    .registry
                    .prepare_cancel_task(&task_id, correlation, Some(principal));
            if !decision.is_authorized() {
                return Err(ProtocolError::new(
                    ProtocolErrorCode::Forbidden,
                    "MCP connection teardown could not authorize task cancellation",
                ));
            }
            decisions.push(decision);
        }
        self.commit(state, expected)?;
        Ok(decisions)
    }

    /// Confirm a host cancellation and only then release waiters with a terminal cancelled result.
    pub fn confirm_cancellation(&self, task_id: &str) -> ProtocolResult<Vec<McpOutboundMessage>> {
        let (mut state, expected) = self.load_state()?;
        state.registry.confirm_cancel_task(task_id)?;
        let now_unix_ms = self.clock.now_unix_ms();
        let now = self.clock.now_rfc3339();
        let task = state
            .registry
            .tasks()
            .get(task_id)
            .cloned()
            .ok_or_else(|| ProtocolError::not_found("MCP task is not registered"))?;
        update_task_time(&mut state, &task, &now, now_unix_ms);
        let error = ProtocolError::new(ProtocolErrorCode::Cancelled, "MCP task was cancelled");
        let mut outbound =
            complete_direct_tool_receipts(&mut state, task_id, Err(error.clone()), now_unix_ms);
        outbound.extend(complete_task_waiters(
            &mut state,
            task_id,
            Err(error),
            now_unix_ms,
        ));
        outbound.extend(task_notifications(&state, task_id));
        self.commit(state, expected)?;
        Ok(outbound)
    }

    /// Preserve an ambiguous host cancellation for deterministic restart reconciliation.
    pub fn mark_cancellation_reconcile_required(
        &self,
        task_id: &str,
        reason: impl Into<String>,
    ) -> ProtocolResult<()> {
        let (mut state, expected) = self.load_state()?;
        state
            .registry
            .mark_cancel_reconcile_required(task_id, reason)?;
        let task = state
            .registry
            .tasks()
            .get(task_id)
            .cloned()
            .ok_or_else(|| ProtocolError::not_found("MCP task is not registered"))?;
        update_task_time(
            &mut state,
            &task,
            &self.clock.now_rfc3339(),
            self.clock.now_unix_ms(),
        );
        self.commit(state, expected)
    }

    /// Remove connection-bound receipts only after every requested cancellation was confirmed.
    pub fn finalize_connection_termination(
        &self,
        connection_id: &str,
        principal: Option<&ProtocolPrincipal>,
    ) -> ProtocolResult<()> {
        let (mut state, expected) = self.load_state()?;
        let principal = principal.ok_or_else(|| {
            ProtocolError::new(
                ProtocolErrorCode::Unauthorized,
                "authenticated principal is required",
            )
        })?;
        let connection = state
            .connections
            .get(connection_id)
            .ok_or_else(|| ProtocolError::not_found("MCP connection is not registered"))?;
        if !connection.matches(principal) {
            return Err(ProtocolError::new(
                ProtocolErrorCode::Forbidden,
                "MCP connection authorization context changed",
            ));
        }
        let unresolved = state.task_metadata.iter().any(|(task_id, metadata)| {
            metadata.connection_id == connection_id
                && state
                    .registry
                    .tasks()
                    .get(task_id)
                    .is_some_and(|task| !task.status.is_terminal())
        });
        if unresolved {
            return Err(ProtocolError::conflict(
                "MCP connection has unresolved task cancellation",
            ));
        }
        state.connections.remove(connection_id);
        state
            .receipts
            .retain(|_, receipt| receipt.connection_id != connection_id);
        self.commit(state, expected)
    }

    /// Validate host-produced messages before transport cloning or encoding.
    pub fn validate_outbound_messages(
        &self,
        messages: &[McpOutboundMessage],
    ) -> ProtocolResult<()> {
        for message in messages {
            validate_json_bounds(
                &message.message,
                self.max_result_bytes,
                self.max_result_items,
                self.max_result_depth,
            )?;
        }
        Ok(())
    }

    fn task_is_expired(&self, state: &McpServerState, task_id: &str) -> bool {
        let now = self.clock.now_unix_ms();
        state.task_metadata.get(task_id).is_some_and(|metadata| {
            metadata.expires_at_unix_ms == 0 || now >= metadata.expires_at_unix_ms
        }) && state
            .registry
            .tasks()
            .get(task_id)
            .is_some_and(|task| !task.status.is_terminal())
    }

    fn expire_due_tasks(
        &self,
        state: &mut McpServerState,
    ) -> ProtocolResult<Vec<McpOutboundMessage>> {
        let now_unix_ms = self.clock.now_unix_ms();
        let now = self.clock.now_rfc3339();
        let due: Vec<_> = state
            .task_metadata
            .iter()
            .filter(|(_, metadata)| {
                metadata.expires_at_unix_ms == 0 || now_unix_ms >= metadata.expires_at_unix_ms
            })
            .filter_map(|(task_id, _)| {
                state
                    .registry
                    .tasks()
                    .get(task_id)
                    .filter(|task| !task.status.is_terminal())
                    .map(|_| task_id.clone())
            })
            .collect();
        let mut outbound = Vec::new();
        for task_id in due {
            state.registry.expire_task(&task_id)?;
            let task = state
                .registry
                .tasks()
                .get(&task_id)
                .cloned()
                .expect("expired task remains registered");
            update_task_time(state, &task, &now, now_unix_ms);
            let error = ProtocolError::conflict("MCP task TTL expired");
            outbound.extend(complete_direct_tool_receipts(
                state,
                &task_id,
                Err(error.clone()),
                now_unix_ms,
            ));
            outbound.extend(complete_task_waiters(
                state,
                &task_id,
                Err(error),
                now_unix_ms,
            ));
            outbound.extend(task_notifications(state, &task_id));
        }
        Ok(outbound)
    }

    fn collect_garbage(&self, state: &mut McpServerState) -> ProtocolResult<bool> {
        let now = self.clock.now_unix_ms();
        let before_receipts = state.receipts.len();
        let retired_connections: std::collections::BTreeSet<_> = state
            .receipts
            .values()
            .filter_map(|receipt| {
                let McpReceiptState::Completed {
                    completed_at_unix_ms,
                    ..
                } = receipt.state
                else {
                    return None;
                };
                (receipt_requires_replay_proof(receipt)
                    && receipt_retention_elapsed(completed_at_unix_ms, now, self.retention_ms))
                .then(|| receipt.connection_id.clone())
            })
            .collect();
        for connection_id in &retired_connections {
            if let Some(connection) = state.connections.get_mut(connection_id) {
                connection.receipt_namespace_retired = true;
            }
        }
        state.receipts.retain(|_, receipt| {
            let McpReceiptState::Completed {
                completed_at_unix_ms,
                ..
            } = &receipt.state
            else {
                return true;
            };
            if retired_connections.contains(&receipt.connection_id) {
                return false;
            }
            !receipt_retention_elapsed(*completed_at_unix_ms, now, self.retention_ms)
        });

        let retained_task_ids: std::collections::BTreeSet<_> = state
            .receipts
            .values()
            .filter_map(receipt_task_id)
            .collect();
        let removable_tasks: Vec<_> = state
            .task_metadata
            .iter()
            .filter(|(task_id, metadata)| {
                !retained_task_ids.contains(*task_id)
                    && metadata.last_updated_at_unix_ms != 0
                    && now
                        >= metadata
                            .last_updated_at_unix_ms
                            .saturating_add(self.retention_ms)
                    && state
                        .registry
                        .tasks()
                        .get(*task_id)
                        .is_some_and(|task| task.status.is_terminal())
            })
            .map(|(task_id, _)| task_id.clone())
            .collect();
        for task_id in &removable_tasks {
            state.registry.remove_terminal_task(task_id)?;
            state.task_metadata.remove(task_id);
        }
        Ok(before_receipts != state.receipts.len() || !removable_tasks.is_empty())
    }

    fn load_state(&self) -> ProtocolResult<(McpServerState, Option<u64>)> {
        let state = self
            .store
            .load(&self.namespace)?
            .ok_or_else(|| ProtocolError::not_found("MCP server state is not initialized"))?;
        state.validate()?;
        let revision = state.storage_revision;
        Ok((state, Some(revision)))
    }

    fn commit(&self, mut state: McpServerState, expected: Option<u64>) -> ProtocolResult<()> {
        state.storage_revision = expected.unwrap_or(0).saturating_add(1);
        self.store
            .compare_and_swap(&self.namespace, expected, &state)
    }

    fn insert_receipt(
        &self,
        state: &mut McpServerState,
        connection_id: &str,
        request: &ParsedRequest,
        request_hash: String,
        receipt_state: McpReceiptState,
    ) -> ProtocolResult<()> {
        let id = request
            .id
            .clone()
            .ok_or_else(|| ProtocolError::invalid("JSON-RPC request id is required"))?;
        state.receipts.insert(
            receipt_key(connection_id, &id),
            McpRequestReceipt {
                connection_id: connection_id.to_owned(),
                request_id: id,
                method: request.method.clone(),
                request_hash,
                created_at_unix_ms: self.clock.now_unix_ms(),
                state: receipt_state,
            },
        );
        Ok(())
    }

    fn record_completed(
        &self,
        state: &mut McpServerState,
        connection_id: &str,
        request: &ParsedRequest,
        request_hash: String,
        response: Value,
    ) -> ProtocolResult<()> {
        self.insert_receipt(
            state,
            connection_id,
            request,
            request_hash,
            McpReceiptState::Completed {
                response,
                completed_at_unix_ms: self.clock.now_unix_ms(),
            },
        )
    }

    fn complete_wire_error(
        &self,
        context: WireRequestContext<'_>,
        code: i64,
        message: &str,
    ) -> ProtocolResult<McpJsonRpcDispatch> {
        let WireRequestContext {
            mut state,
            expected_revision,
            connection_id,
            request,
            request_hash,
        } = context;
        let response = jsonrpc_error(
            request.id.clone().unwrap_or(Value::Null),
            code,
            message,
            None,
        );
        self.record_completed(
            &mut state,
            connection_id,
            &request,
            request_hash,
            response.clone(),
        )?;
        self.commit(state, expected_revision)?;
        Ok(McpJsonRpcDispatch::response(response))
    }
}

#[derive(Debug, Clone)]
struct ParsedRequest {
    id: Option<Value>,
    method: String,
    params: Value,
}

impl ParsedRequest {
    fn parse(raw: &Value) -> Result<Self, (Value, &'static str)> {
        let Some(object) = raw.as_object() else {
            return Err((Value::Null, "JSON-RPC message must be an object"));
        };
        let id = object.get("id").cloned();
        if id.as_ref().is_some_and(|id| !valid_request_id(id)) {
            return Err((Value::Null, "JSON-RPC id must be a string or integer"));
        }
        if object.get("jsonrpc").and_then(Value::as_str) != Some(MCP_JSONRPC_VERSION) {
            return Err((id.unwrap_or(Value::Null), "jsonrpc must equal 2.0"));
        }
        let Some(method) = object.get("method").and_then(Value::as_str) else {
            return Err((id.unwrap_or(Value::Null), "JSON-RPC method is required"));
        };
        if method.is_empty() || method.len() > 256 || method.chars().any(char::is_control) {
            return Err((id.unwrap_or(Value::Null), "JSON-RPC method is invalid"));
        }
        let params = object.get("params").cloned().unwrap_or_else(|| json!({}));
        if !params.is_object() {
            return Err((id.unwrap_or(Value::Null), "MCP params must be an object"));
        }
        Ok(Self {
            id,
            method: method.to_owned(),
            params,
        })
    }

    fn error_or_ignore(self, code: i64, message: &str) -> McpJsonRpcDispatch {
        self.id.map_or_else(McpJsonRpcDispatch::notification, |id| {
            McpJsonRpcDispatch::response(jsonrpc_error(id, code, message, None))
        })
    }
}

fn correlation(
    connection_id: &str,
    request: &ParsedRequest,
) -> ProtocolResult<CorrelationIdentity> {
    let request_identity = request.id.as_ref().map_or_else(
        || stable_id("mcp_notification", &[connection_id, &request.method]),
        |id| stable_id("mcp_request", &[connection_id, &canonical_id(id)]),
    );
    let correlation_id = request
        .params
        .pointer("/_meta/aikit/correlationId")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| stable_id("mcp_correlation", &[connection_id]));
    CorrelationIdentity::new(correlation_id, request_identity)
}

fn valid_request_id(value: &Value) -> bool {
    value.as_str().is_some() || value.as_i64().is_some() || value.as_u64().is_some()
}

fn valid_progress_token(value: &Value) -> bool {
    valid_request_id(value)
}

fn canonical_id(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "null".into())
}

fn receipt_key(connection_id: &str, id: &Value) -> String {
    stable_id("mcp_receipt", &[connection_id, &canonical_id(id)])
}

fn replay_receipt(receipt: &McpRequestReceipt) -> McpJsonRpcDispatch {
    match &receipt.state {
        McpReceiptState::Completed { response, .. } => {
            McpJsonRpcDispatch::response(response.clone())
        }
        McpReceiptState::PendingHost { operation, .. } => {
            McpJsonRpcDispatch::response(jsonrpc_error(
                receipt.request_id.clone(),
                MCP_CONFLICT,
                "Identical MCP request is still in progress",
                Some(json!({"operation": operation})),
            ))
        }
        McpReceiptState::WaitingForTask { task_id } => McpJsonRpcDispatch::response(jsonrpc_error(
            receipt.request_id.clone(),
            MCP_CONFLICT,
            "MCP tasks/result request is already waiting",
            Some(json!({"taskId": task_id})),
        )),
    }
}

fn reconciliation_required(id: Option<Value>) -> McpJsonRpcDispatch {
    let Some(id) = id else {
        return McpJsonRpcDispatch::notification();
    };
    McpJsonRpcDispatch::response(jsonrpc_error(
        id,
        MCP_RECONCILIATION_REQUIRED,
        "MCP request receipt expired; reconnect before submitting more work",
        Some(json!({
            "type": "receipt_reconciliation_required",
            "reconciliationRequired": true,
        })),
    ))
}

fn receipt_requires_replay_proof(receipt: &McpRequestReceipt) -> bool {
    receipt.method.is_empty()
        || matches!(
            receipt.method.as_str(),
            "tools/call" | "resources/read" | "prompts/get" | "tasks/cancel"
        )
}

fn receipt_retention_elapsed(completed_at_unix_ms: u64, now: u64, retention_ms: u64) -> bool {
    completed_at_unix_ms == 0 || now >= completed_at_unix_ms.saturating_add(retention_ms)
}

fn prune_completed_safe_receipt(state: &mut McpServerState, now: u64, retention_ms: u64) {
    let removable = state.receipts.iter().find_map(|(key, receipt)| {
        let completed_at = match receipt.state {
            McpReceiptState::Completed {
                completed_at_unix_ms,
                ..
            } => completed_at_unix_ms,
            _ => return None,
        };
        (!receipt_requires_replay_proof(receipt)
            && receipt_retention_elapsed(completed_at, now, retention_ms))
        .then(|| key.clone())
    });
    if let Some(key) = removable {
        state.receipts.remove(&key);
    }
}

fn jsonrpc_result(id: Value, result: Value) -> Value {
    json!({"jsonrpc": MCP_JSONRPC_VERSION, "id": id, "result": result})
}

fn jsonrpc_error(id: Value, code: i64, message: &str, data: Option<Value>) -> Value {
    let mut error = Map::new();
    error.insert("code".into(), Value::from(code));
    error.insert("message".into(), Value::String(message.into()));
    if let Some(data) = data {
        error.insert("data".into(), data);
    }
    json!({"jsonrpc": MCP_JSONRPC_VERSION, "id": id, "error": error})
}

fn protocol_error_response(id: Value, error: &ProtocolError) -> Value {
    let code = match error.code {
        ProtocolErrorCode::InvalidRequest | ProtocolErrorCode::InvalidTransition => {
            JSONRPC_INVALID_PARAMS
        }
        ProtocolErrorCode::Unauthorized | ProtocolErrorCode::Forbidden => MCP_FORBIDDEN,
        ProtocolErrorCode::NotFound => JSONRPC_INVALID_PARAMS,
        ProtocolErrorCode::Conflict => MCP_CONFLICT,
        ProtocolErrorCode::Cancelled => MCP_CANCELLED,
    };
    jsonrpc_error(id, code, &error.message, None)
}

fn tool_execution_error(message: &str) -> Value {
    json!({
        "content": [{"type": "text", "text": message}],
        "isError": true
    })
}

fn validate_tool_result(
    result: &Value,
    max_bytes: usize,
    max_items: usize,
    max_depth: usize,
) -> ProtocolResult<()> {
    validate_json_bounds(result, max_bytes, max_items, max_depth)?;
    let Some(object) = result.as_object() else {
        return Err(ProtocolError::invalid(
            "MCP CallToolResult must be a JSON object",
        ));
    };
    if !object.get("content").is_some_and(Value::is_array) {
        return Err(ProtocolError::invalid(
            "MCP CallToolResult.content must be an array",
        ));
    }
    if object
        .get("isError")
        .is_some_and(|value| !value.is_boolean())
    {
        return Err(ProtocolError::invalid(
            "MCP CallToolResult.isError must be a boolean",
        ));
    }
    Ok(())
}

fn validate_host_result(
    operation: &str,
    result: &Value,
    max_bytes: usize,
    max_items: usize,
    max_depth: usize,
) -> ProtocolResult<()> {
    validate_json_bounds(result, max_bytes, max_items, max_depth)?;
    let field = match operation {
        "resources/read" => "contents",
        "prompts/get" => "messages",
        _ => {
            return Err(ProtocolError::invalid(
                "unsupported MCP pending host operation",
            ))
        }
    };
    if !result
        .as_object()
        .and_then(|object| object.get(field))
        .is_some_and(Value::is_array)
    {
        return Err(ProtocolError::invalid(format!(
            "MCP {operation} result.{field} must be an array"
        )));
    }
    Ok(())
}

fn validate_json_bounds(
    value: &Value,
    max_bytes: usize,
    max_items: usize,
    max_depth: usize,
) -> ProtocolResult<()> {
    let encoded = serde_json::to_vec(value)
        .map_err(|error| ProtocolError::invalid(format!("MCP JSON encoding failed: {error}")))?;
    if encoded.len() > max_bytes {
        return Err(ProtocolError::invalid(
            "MCP result exceeds the configured byte limit",
        ));
    }
    let mut stack = vec![(value, 1_usize)];
    let mut items = 0_usize;
    while let Some((current, depth)) = stack.pop() {
        if depth > max_depth {
            return Err(ProtocolError::invalid(
                "MCP result exceeds the configured nesting depth",
            ));
        }
        items = items.saturating_add(1);
        if items > max_items {
            return Err(ProtocolError::invalid(
                "MCP result exceeds the configured item limit",
            ));
        }
        match current {
            Value::Array(values) => {
                stack.extend(values.iter().map(|value| (value, depth.saturating_add(1))));
            }
            Value::Object(values) => {
                stack.extend(
                    values
                        .values()
                        .map(|value| (value, depth.saturating_add(1))),
                );
            }
            _ => {}
        }
    }
    Ok(())
}

fn decision_error(id: Value, envelope: &GovernanceEnvelope) -> Value {
    decision_error_with_code(id, envelope, MCP_FORBIDDEN)
}

fn decision_error_with_code(id: Value, envelope: &GovernanceEnvelope, code: i64) -> Value {
    let reason = match &envelope.authorization {
        GovernanceAuthorization::Denied { reason, .. } => reason.as_str(),
        GovernanceAuthorization::Allowed => "MCP operation produced no action",
    };
    jsonrpc_error(
        id,
        code,
        reason,
        Some(json!({"operation": envelope.operation, "target": envelope.target})),
    )
}

fn server_info_wire(info: &McpServerInfo) -> Value {
    let mut value = json!({"name": info.name, "version": info.version});
    if let Some(title) = &info.title {
        value["title"] = Value::String(title.clone());
    }
    if let Some(description) = &info.description {
        value["description"] = Value::String(description.clone());
    }
    value
}

fn tool_wire(tool: &McpToolDefinition) -> ProtocolResult<Value> {
    let mut value = json!({
        "name": tool.name,
        "description": tool.description,
        "inputSchema": tool.input_schema,
        "execution": {"taskSupport": match tool.task_support {
            super::McpTaskSupport::Forbidden => "forbidden",
            super::McpTaskSupport::Optional => "optional",
            super::McpTaskSupport::Required => "required",
        }},
        "_meta": {"aikit/definitionHash": tool.definition_hash()?}
    });
    if tool.requires_approval {
        value["_meta"]["aikit/requiresApproval"] = Value::Bool(true);
    }
    Ok(value)
}

fn action_task(action: &McpServerAction) -> Option<&McpTask> {
    match action {
        McpServerAction::InvokeTool { task } | McpServerAction::AwaitApproval { task, .. } => {
            Some(task)
        }
        _ => None,
    }
}

fn task_wire(task: &McpTask, metadata: Option<&McpTaskWireMetadata>) -> Value {
    let mut value = json!({
        "taskId": task.task_id,
        "status": match task.status {
            McpTaskStatus::Working => "working",
            McpTaskStatus::InputRequired => "input_required",
            McpTaskStatus::Completed => "completed",
            McpTaskStatus::Failed => "failed",
            McpTaskStatus::Cancelled => "cancelled",
        },
        "statusMessage": task.status_message,
        "createdAt": metadata.map_or_else(|| unix_seconds_rfc3339(0), |value| value.created_at.clone()),
        "lastUpdatedAt": metadata.map_or_else(|| unix_seconds_rfc3339(0), |value| value.last_updated_at.clone()),
        "ttl": metadata.map_or(DEFAULT_MCP_TASK_TTL_MS, |value| value.ttl),
        "pollInterval": metadata.map_or(DEFAULT_MCP_POLL_INTERVAL_MS, |value| value.poll_interval)
    });
    if value["statusMessage"].is_null() {
        value
            .as_object_mut()
            .expect("task is an object")
            .remove("statusMessage");
    }
    value
}

fn update_task_time(state: &mut McpServerState, task: &McpTask, now: &str, now_unix_ms: u64) {
    if let Some(metadata) = state.task_metadata.get_mut(&task.task_id) {
        metadata.last_updated_at = now.to_owned();
        metadata.last_updated_at_unix_ms = now_unix_ms;
    }
}

fn related_task_meta(task_id: &str) -> Value {
    json!({"io.modelcontextprotocol/related-task": {"taskId": task_id}})
}

fn terminal_task_response(id: Value, task: &McpTask) -> Value {
    match task.status {
        McpTaskStatus::Completed => {
            let mut result = task.result.clone().unwrap_or_else(|| json!({}));
            if let Some(object) = result.as_object_mut() {
                object.insert("_meta".into(), related_task_meta(&task.task_id));
            } else {
                result = json!({"content": result, "_meta": related_task_meta(&task.task_id)});
            }
            jsonrpc_result(id, result)
        }
        McpTaskStatus::Failed => {
            if let Some(result) = &task.result {
                let mut result = result.clone();
                if let Some(object) = result.as_object_mut() {
                    object.insert("_meta".into(), related_task_meta(&task.task_id));
                }
                jsonrpc_result(id, result)
            } else {
                protocol_error_response(
                    id,
                    task.error.as_ref().unwrap_or(&ProtocolError::new(
                        ProtocolErrorCode::Conflict,
                        "MCP task failed",
                    )),
                )
            }
        }
        McpTaskStatus::Cancelled => jsonrpc_error(
            id,
            MCP_CANCELLED,
            "MCP task was cancelled",
            Some(related_task_meta(&task.task_id)),
        ),
        McpTaskStatus::Working | McpTaskStatus::InputRequired => {
            jsonrpc_error(id, MCP_CONFLICT, "MCP task is not terminal", None)
        }
    }
}

fn complete_direct_tool_receipts(
    state: &mut McpServerState,
    task_id: &str,
    result: ProtocolResult<Value>,
    completed_at_unix_ms: u64,
) -> Vec<McpOutboundMessage> {
    let mut outbound = Vec::new();
    for receipt in state.receipts.values_mut() {
        let matches = matches!(&receipt.state, McpReceiptState::PendingHost { operation, task_id: Some(value) } if operation == "tools/call" && value == task_id);
        if !matches {
            continue;
        }
        let response = match &result {
            Ok(result) => jsonrpc_result(receipt.request_id.clone(), result.clone()),
            Err(error) => protocol_error_response(receipt.request_id.clone(), error),
        };
        receipt.state = McpReceiptState::Completed {
            response: response.clone(),
            completed_at_unix_ms,
        };
        outbound.push(McpOutboundMessage {
            connection_id: receipt.connection_id.clone(),
            message: response,
        });
    }
    outbound
}

fn complete_task_waiters(
    state: &mut McpServerState,
    task_id: &str,
    result: ProtocolResult<Value>,
    completed_at_unix_ms: u64,
) -> Vec<McpOutboundMessage> {
    let mut outbound = Vec::new();
    for receipt in state.receipts.values_mut() {
        let matches = matches!(&receipt.state, McpReceiptState::WaitingForTask { task_id: value } if value == task_id);
        if !matches {
            continue;
        }
        let response = match &result {
            Ok(result) => {
                let mut result = result.clone();
                if let Some(object) = result.as_object_mut() {
                    object.insert("_meta".into(), related_task_meta(task_id));
                } else {
                    result = json!({"content": result, "_meta": related_task_meta(task_id)});
                }
                jsonrpc_result(receipt.request_id.clone(), result)
            }
            Err(error) => protocol_error_response(receipt.request_id.clone(), error),
        };
        receipt.state = McpReceiptState::Completed {
            response: response.clone(),
            completed_at_unix_ms,
        };
        outbound.push(McpOutboundMessage {
            connection_id: receipt.connection_id.clone(),
            message: response,
        });
    }
    outbound
}

fn receipt_task_id(receipt: &McpRequestReceipt) -> Option<String> {
    match &receipt.state {
        McpReceiptState::PendingHost {
            task_id: Some(task_id),
            ..
        }
        | McpReceiptState::WaitingForTask { task_id } => Some(task_id.clone()),
        McpReceiptState::PendingHost { task_id: None, .. } | McpReceiptState::Completed { .. } => {
            None
        }
    }
}

fn task_connections(state: &McpServerState, task_id: &str) -> Vec<String> {
    let mut values: Vec<_> = state
        .receipts
        .values()
        .filter_map(|receipt| match &receipt.state {
            McpReceiptState::PendingHost {
                task_id: Some(value),
                ..
            }
            | McpReceiptState::WaitingForTask { task_id: value }
                if value == task_id =>
            {
                Some(receipt.connection_id.clone())
            }
            _ => None,
        })
        .collect();
    if let Some(metadata) = state.task_metadata.get(task_id) {
        values.push(metadata.connection_id.clone());
    }
    values.sort();
    values.dedup();
    values
}

fn unix_seconds_rfc3339(seconds: u64) -> String {
    let days = (seconds / 86_400) as i64;
    let seconds_of_day = seconds % 86_400;
    // Howard Hinnant's civil-from-days algorithm, with day zero at 1970-01-01.
    let shifted = days + 719_468;
    let era = if shifted >= 0 {
        shifted
    } else {
        shifted - 146_096
    } / 146_097;
    let day_of_era = shifted - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    year += i64::from(month <= 2);
    let hour = seconds_of_day / 3_600;
    let minute = (seconds_of_day % 3_600) / 60;
    let second = seconds_of_day % 60;
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z")
}

fn task_notifications(state: &McpServerState, task_id: &str) -> Vec<McpOutboundMessage> {
    let Some(task) = state.registry.tasks().get(task_id) else {
        return Vec::new();
    };
    task_connections(state, task_id)
        .into_iter()
        .map(|connection_id| McpOutboundMessage {
            connection_id,
            message: json!({
                "jsonrpc": MCP_JSONRPC_VERSION,
                "method": "notifications/tasks/status",
                "params": task_wire(task, state.task_metadata.get(task_id))
            }),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocols::{
        McpApprovalResponse, McpPromptArgument, McpPromptDefinition, McpResourceDefinition,
        McpTaskSupport,
    };
    use std::collections::BTreeSet;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[derive(Debug)]
    struct FixedClock;

    impl McpClock for FixedClock {
        fn now_unix_ms(&self) -> u64 {
            1_784_550_896_000
        }
    }

    #[derive(Debug)]
    struct MutableClock {
        now_unix_ms: AtomicU64,
    }

    impl MutableClock {
        fn new(now_unix_ms: u64) -> Self {
            Self {
                now_unix_ms: AtomicU64::new(now_unix_ms),
            }
        }

        fn set(&self, now_unix_ms: u64) {
            self.now_unix_ms.store(now_unix_ms, Ordering::SeqCst);
        }
    }

    impl McpClock for MutableClock {
        fn now_unix_ms(&self) -> u64 {
            self.now_unix_ms.load(Ordering::SeqCst)
        }
    }

    struct FixedStateStore {
        state: McpServerState,
    }

    impl McpServerStateStore for FixedStateStore {
        fn load(&self, _namespace: &str) -> ProtocolResult<Option<McpServerState>> {
            Ok(Some(self.state.clone()))
        }

        fn compare_and_swap(
            &self,
            _namespace: &str,
            _expected_revision: Option<u64>,
            _state: &McpServerState,
        ) -> ProtocolResult<()> {
            Err(ProtocolError::conflict("fixed test store is read-only"))
        }
    }

    fn actor(tenant: &str) -> ProtocolPrincipal {
        ProtocolPrincipal::new(
            "user-1",
            [
                "mcp:tools:list",
                "mcp:tools:call",
                "mcp:resources:list",
                "mcp:resources:read",
                "mcp:prompts:list",
                "mcp:prompts:get",
                "mcp:tasks:read",
                "mcp:tasks:cancel",
            ],
        )
        .unwrap()
        .with_tenant(tenant)
        .unwrap()
    }

    fn tool(name: &str, support: McpTaskSupport) -> McpToolDefinition {
        let mut tool = McpToolDefinition::new(
            name,
            "test tool",
            json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "properties": {"value": {"type": "string"}},
                "additionalProperties": false
            }),
        )
        .unwrap();
        tool.task_support = support;
        tool
    }

    fn server_with_registry(
        registry: McpServerRegistry,
    ) -> (McpJsonRpcServer, Arc<InMemoryMcpServerStateStore>) {
        let store = Arc::new(InMemoryMcpServerStateStore::default());
        let server = McpJsonRpcServer::new(
            "tests",
            McpServerInfo::new("aikit-tests", "1.0.0").unwrap(),
            store.clone(),
            registry,
        )
        .unwrap()
        .with_clock(Arc::new(FixedClock));
        (server, store)
    }

    fn server_with_mutable_clock(
        registry: McpServerRegistry,
        clock: Arc<MutableClock>,
    ) -> (McpJsonRpcServer, Arc<InMemoryMcpServerStateStore>) {
        let store = Arc::new(InMemoryMcpServerStateStore::default());
        let server = McpJsonRpcServer::new(
            "mutable-clock-tests",
            McpServerInfo::new("aikit-tests", "1.0.0").unwrap(),
            store.clone(),
            registry,
        )
        .unwrap()
        .with_clock(clock);
        (server, store)
    }

    fn send(
        server: &McpJsonRpcServer,
        connection: &str,
        principal: Option<&ProtocolPrincipal>,
        value: &Value,
    ) -> McpJsonRpcDispatch {
        server
            .handle(
                connection,
                principal,
                serde_json::to_vec(value).unwrap().as_slice(),
            )
            .unwrap()
    }

    fn initialize(server: &McpJsonRpcServer, connection: &str, principal: &ProtocolPrincipal) {
        let mut initialize_request: Value = serde_json::from_str(include_str!(
            "fixtures/mcp_2025_11_25/initialize_request.json"
        ))
        .unwrap();
        initialize_request["id"] = Value::String("init".into());
        let response = send(server, connection, Some(principal), &initialize_request);
        assert_eq!(
            response.response.unwrap()["result"]["protocolVersion"],
            "2025-11-25"
        );
        let notification = send(
            server,
            connection,
            Some(principal),
            &json!({
                "jsonrpc": "2.0",
                "method": "notifications/initialized",
                "params": {}
            }),
        );
        assert!(notification.response.is_none());
    }

    #[test]
    fn official_lifecycle_shape_is_enforced_and_capabilities_are_truthful() {
        let mut registry = McpServerRegistry::new();
        registry
            .register_tool(tool("echo", McpTaskSupport::Optional))
            .unwrap();
        let (server, _) = server_with_registry(registry);
        let owner = actor("tenant-a");

        let early = send(
            &server,
            "connection-a",
            Some(&owner),
            &json!({"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}),
        );
        assert_eq!(
            early.response.unwrap()["error"]["code"],
            MCP_NOT_INITIALIZED
        );

        let initialized = send(
            &server,
            "connection-a",
            Some(&owner),
            &json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-11-25",
                    "capabilities": {},
                    "clientInfo": {"name": "fixture", "version": "1.0"}
                }
            }),
        )
        .response
        .unwrap();
        assert_eq!(
            initialized["result"]["capabilities"]["tasks"]["requests"]["tools"]["call"],
            json!({})
        );

        let before_ready = send(
            &server,
            "connection-a",
            Some(&owner),
            &json!({"jsonrpc":"2.0","id":3,"method":"tools/list","params":{}}),
        );
        assert_eq!(
            before_ready.response.unwrap()["error"]["code"],
            MCP_NOT_INITIALIZED
        );
        send(
            &server,
            "connection-a",
            Some(&owner),
            &json!({"jsonrpc":"2.0","method":"notifications/initialized","params":{}}),
        );
        let listed = send(
            &server,
            "connection-a",
            Some(&owner),
            &json!({"jsonrpc":"2.0","id":4,"method":"tools/list","params":{}}),
        )
        .response
        .unwrap();
        assert_eq!(listed["result"]["tools"][0]["name"], "echo");
        assert_eq!(
            listed["result"]["tools"][0]["execution"]["taskSupport"],
            "optional"
        );
        assert!(
            listed["result"]["tools"][0]["_meta"]["aikit/definitionHash"]
                .as_str()
                .unwrap()
                .starts_with("sha256:")
        );
    }

    #[test]
    fn task_dedupe_restart_progress_result_and_auth_context_are_durable() {
        let mut registry = McpServerRegistry::new();
        registry
            .register_tool(tool("long_job", McpTaskSupport::Optional))
            .unwrap();
        let (server, store) = server_with_registry(registry);
        let owner = actor("tenant-a");
        initialize(&server, "owner", &owner);

        let mut call: Value = serde_json::from_str(include_str!(
            "fixtures/mcp_2025_11_25/task_tool_call_request.json"
        ))
        .unwrap();
        call["id"] = Value::String("call-1".into());
        call["params"]["name"] = Value::String("long_job".into());
        call["params"]["arguments"] = json!({"value":"x"});
        call["params"]["_meta"]["progressToken"] = Value::String("progress-1".into());
        let created = send(&server, "owner", Some(&owner), &call);
        assert!(matches!(
            created
                .governed_action
                .as_ref()
                .and_then(|value| value.action()),
            Some(McpServerAction::InvokeTool { .. })
        ));
        let created_response = created.response.unwrap();
        let task_id = created_response["result"]["task"]["taskId"]
            .as_str()
            .unwrap()
            .to_owned();
        assert_eq!(created_response["result"]["task"]["ttl"], 60000);
        assert_eq!(
            created_response["result"]["task"]["createdAt"],
            "2026-07-20T12:34:56Z"
        );

        let duplicate = send(&server, "owner", Some(&owner), &call);
        assert_eq!(duplicate.response.unwrap(), created_response);
        assert!(duplicate.governed_action.is_none());

        let restarted = McpJsonRpcServer::new(
            "tests",
            McpServerInfo::new("aikit-tests", "1.0.0").unwrap(),
            store,
            McpServerRegistry::new(),
        )
        .unwrap()
        .with_clock(Arc::new(FixedClock));
        let replayed = send(&restarted, "owner", Some(&owner), &call);
        assert_eq!(replayed.response.unwrap(), created_response);
        let recovered = restarted.recover_actions(Some(&owner)).unwrap();
        assert!(matches!(
            recovered.as_slice(),
            [decision]
                if matches!(decision.action(), Some(McpServerAction::InvokeTool { task }) if task.task_id == task_id)
        ));

        let attacker = actor("tenant-b");
        initialize(&restarted, "attacker", &attacker);
        let denied = send(
            &restarted,
            "attacker",
            Some(&attacker),
            &json!({"jsonrpc":"2.0","id":9,"method":"tasks/get","params":{"taskId":task_id}}),
        );
        assert_eq!(denied.response.unwrap()["error"]["code"], MCP_FORBIDDEN);
        assert!(restarted
            .recover_actions(Some(&attacker))
            .unwrap()
            .is_empty());

        let progress = restarted
            .record_progress(
                &task_id,
                McpProgress {
                    progress: 1,
                    total: Some(2),
                    message: Some("half".into()),
                },
            )
            .unwrap();
        assert!(progress.iter().any(|message| {
            message.connection_id == "owner"
                && message.message["method"] == "notifications/progress"
                && message.message["params"]["progressToken"] == "progress-1"
        }));

        let mut result_request: Value = serde_json::from_str(include_str!(
            "fixtures/mcp_2025_11_25/task_result_request.json"
        ))
        .unwrap();
        result_request["id"] = Value::String("result-1".into());
        result_request["params"]["taskId"] = Value::String(task_id.clone());
        let waiting = send(&restarted, "owner", Some(&owner), &result_request);
        assert!(waiting.response.is_none());
        assert_eq!(waiting.pending.unwrap().operation, "tasks/result");
        let released = restarted
            .complete_tool(
                &task_id,
                json!({"content":[{"type":"text","text":"done"}],"isError":false}),
            )
            .unwrap();
        assert!(released.iter().any(|message| {
            message.message["id"] == "result-1"
                && message.message["result"]["_meta"]["io.modelcontextprotocol/related-task"]
                    ["taskId"]
                    == task_id
        }));
    }

    #[test]
    fn cancellation_and_json_rpc_id_reuse_fail_closed_without_double_dispatch() {
        let mut registry = McpServerRegistry::new();
        registry
            .register_tool(tool("write", McpTaskSupport::Forbidden))
            .unwrap();
        let (server, store) = server_with_registry(registry);
        let owner = actor("tenant-a");
        initialize(&server, "owner", &owner);
        let call = json!({"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"write","arguments":{"value":"one"}}});
        let first = send(&server, "owner", Some(&owner), &call);
        assert!(first.response.is_none());
        assert!(first.governed_action.unwrap().is_authorized());

        let duplicate = send(&server, "owner", Some(&owner), &call);
        assert_eq!(duplicate.response.unwrap()["error"]["code"], MCP_CONFLICT);
        assert!(duplicate.governed_action.is_none());
        let changed = send(
            &server,
            "owner",
            Some(&owner),
            &json!({"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"write","arguments":{"value":"two"}}}),
        );
        assert_eq!(changed.response.unwrap()["error"]["code"], MCP_CONFLICT);

        let cancelled = send(
            &server,
            "owner",
            Some(&owner),
            &json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":7,"reason":"client timeout"}}),
        );
        assert!(matches!(
            cancelled
                .governed_action
                .as_ref()
                .and_then(|value| value.action()),
            Some(McpServerAction::CancelTask { .. })
        ));
        assert!(cancelled.outbound.is_empty());
        let snapshot = server.snapshot().unwrap();
        let task = snapshot.registry.tasks().values().next().unwrap();
        assert_eq!(task.status, McpTaskStatus::Working);
        assert_eq!(
            task.cancellation,
            Some(crate::protocols::McpCancellationState::Requested)
        );

        let restarted = McpJsonRpcServer::new(
            "tests",
            McpServerInfo::new("aikit-tests", "1.0.0").unwrap(),
            store,
            McpServerRegistry::new(),
        )
        .unwrap()
        .with_clock(Arc::new(FixedClock));
        assert!(matches!(
            restarted.recover_actions(Some(&owner)).unwrap().as_slice(),
            [decision]
                if matches!(decision.action(), Some(McpServerAction::CancelTask { task }) if task.status == McpTaskStatus::Working)
        ));
        let task_id = restarted
            .snapshot()
            .unwrap()
            .registry
            .tasks()
            .keys()
            .next()
            .unwrap()
            .clone();
        restarted
            .mark_cancellation_reconcile_required(&task_id, "host timeout")
            .unwrap();
        let reconciled = restarted.snapshot().unwrap();
        assert_eq!(
            reconciled.registry.tasks()[&task_id].cancellation,
            Some(crate::protocols::McpCancellationState::ReconcileRequired)
        );
        let released = restarted.confirm_cancellation(&task_id).unwrap();
        assert_eq!(released[0].message["error"]["code"], MCP_CANCELLED);
        assert_eq!(
            restarted.snapshot().unwrap().registry.tasks()[&task_id].status,
            McpTaskStatus::Cancelled
        );
        let replay = send(&restarted, "owner", Some(&owner), &call);
        assert_eq!(replay.response.unwrap()["error"]["code"], MCP_CANCELLED);
    }

    #[test]
    fn connection_teardown_without_cancel_scope_retains_connection_and_running_work() {
        let mut registry = McpServerRegistry::new();
        registry
            .register_tool(tool("write", McpTaskSupport::Forbidden))
            .unwrap();
        let (server, _) = server_with_registry(registry);
        let caller = ProtocolPrincipal::new(
            "user-1",
            ["mcp:tools:call", "mcp:tools:list", "mcp:tasks:read"],
        )
        .unwrap()
        .with_tenant("tenant-a")
        .unwrap();
        initialize(&server, "scope-limited", &caller);
        let started = send(
            &server,
            "scope-limited",
            Some(&caller),
            &json!({"jsonrpc":"2.0","id":70,"method":"tools/call","params":{"name":"write","arguments":{"value":"one"}}}),
        );
        let task_id = match started.governed_action.unwrap().action().unwrap() {
            McpServerAction::InvokeTool { task } => task.task_id.clone(),
            other => panic!("unexpected action: {other:?}"),
        };
        assert_eq!(
            server
                .terminate_connection("scope-limited", Some(&caller))
                .unwrap_err()
                .code,
            ProtocolErrorCode::Forbidden
        );
        let snapshot = server.snapshot().unwrap();
        assert!(snapshot.connections.contains_key("scope-limited"));
        assert_eq!(
            snapshot.registry.tasks()[&task_id].status,
            McpTaskStatus::Working
        );
        assert!(snapshot.registry.tasks()[&task_id].cancellation.is_none());
    }

    #[test]
    fn schema_drift_invalidates_work_and_requires_durable_reapproval() {
        let mut registry = McpServerRegistry::new();
        registry
            .register_tool(tool("deploy", McpTaskSupport::Forbidden))
            .unwrap();
        let (server, _) = server_with_registry(registry);
        let owner = actor("tenant-a");
        initialize(&server, "owner", &owner);
        let call = send(
            &server,
            "owner",
            Some(&owner),
            &json!({"jsonrpc":"2.0","id":8,"method":"tools/call","params":{"name":"deploy","arguments":{"value":"v1"}}}),
        );
        let task_id = match call.governed_action.unwrap().action().unwrap() {
            McpServerAction::InvokeTool { task } => task.task_id.clone(),
            other => panic!("unexpected action: {other:?}"),
        };

        let mut changed = tool("deploy", McpTaskSupport::Forbidden);
        changed.description = "changed executable contract".into();
        assert!(server.upsert_tool(changed).unwrap());
        assert!(server
            .complete_tool(
                &task_id,
                json!({"content":[{"type":"text","text":"late"}],"isError":false}),
            )
            .is_err());
        let snapshot = server.snapshot().unwrap();
        let task = &snapshot.registry.tasks()[&task_id];
        assert_eq!(task.status, McpTaskStatus::InputRequired);
        let approval_id = task.pending_approval.as_ref().unwrap().approval_id.clone();
        let resumed = server
            .resolve_approval(
                &task_id,
                McpApprovalResponse {
                    approval_id,
                    approved: true,
                },
                Some(&owner),
            )
            .unwrap();
        assert!(matches!(
            resumed.action(),
            Some(McpServerAction::InvokeTool { task }) if task.status == McpTaskStatus::Working
        ));
        server
            .complete_tool(
                &task_id,
                json!({"content":[{"type":"text","text":"done"}],"isError":false}),
            )
            .unwrap();
    }

    #[test]
    fn incompatible_schema_drift_fails_stored_arguments_instead_of_resuming() {
        let mut registry = McpServerRegistry::new();
        registry
            .register_tool(tool("deploy", McpTaskSupport::Forbidden))
            .unwrap();
        let (server, _) = server_with_registry(registry);
        let owner = actor("tenant-a");
        initialize(&server, "owner", &owner);
        let call = send(
            &server,
            "owner",
            Some(&owner),
            &json!({"jsonrpc":"2.0","id":81,"method":"tools/call","params":{"name":"deploy","arguments":{"value":"v1"}}}),
        );
        let task_id = match call.governed_action.unwrap().action().unwrap() {
            McpServerAction::InvokeTool { task } => task.task_id.clone(),
            other => panic!("unexpected action: {other:?}"),
        };

        let mut changed = tool("deploy", McpTaskSupport::Forbidden);
        changed.input_schema = json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": {"value": {"type": "integer"}},
            "required": ["value"],
            "additionalProperties": false
        });
        assert!(server.upsert_tool(changed).unwrap());
        let approval_id = server.snapshot().unwrap().registry.tasks()[&task_id]
            .pending_approval
            .as_ref()
            .unwrap()
            .approval_id
            .clone();
        let resumed = server
            .resolve_approval(
                &task_id,
                McpApprovalResponse {
                    approval_id,
                    approved: true,
                },
                Some(&owner),
            )
            .unwrap();
        assert!(matches!(
            resumed.action(),
            Some(McpServerAction::ApprovalDenied { task })
                if task.status == McpTaskStatus::Failed
        ));
        let snapshot = server.snapshot().unwrap();
        let failed = &snapshot.registry.tasks()[&task_id];
        assert_eq!(failed.status, McpTaskStatus::Failed);
        assert!(failed
            .status_message
            .as_deref()
            .unwrap()
            .contains("no longer match"));
    }

    #[test]
    fn trusted_clock_enforces_task_ttl_on_approval_poll_result_and_recovery() {
        let mut registry = McpServerRegistry::new();
        registry
            .register_tool(tool("task", McpTaskSupport::Optional))
            .unwrap();
        let mut approval_tool = tool("approve", McpTaskSupport::Optional);
        approval_tool.requires_approval = true;
        registry.register_tool(approval_tool).unwrap();
        let clock = Arc::new(MutableClock::new(1_000));
        let (server, _) = server_with_mutable_clock(registry, clock.clone());
        let owner = actor("tenant-a");
        initialize(&server, "owner", &owner);

        let created = send(
            &server,
            "owner",
            Some(&owner),
            &json!({"jsonrpc":"2.0","id":90,"method":"tools/call","params":{"name":"task","arguments":{"value":"x"},"task":{"ttl":10}}}),
        );
        let task_id = created.response.unwrap()["result"]["task"]["taskId"]
            .as_str()
            .unwrap()
            .to_owned();
        let approval = send(
            &server,
            "owner",
            Some(&owner),
            &json!({"jsonrpc":"2.0","id":91,"method":"tools/call","params":{"name":"approve","arguments":{"value":"x"},"task":{"ttl":10}}}),
        );
        let approval_task = match approval.governed_action.unwrap().action().unwrap() {
            McpServerAction::AwaitApproval { task, challenge } => {
                (task.task_id.clone(), challenge.approval_id.clone())
            }
            other => panic!("unexpected action: {other:?}"),
        };

        clock.set(1_011);
        assert_eq!(
            server
                .resolve_approval(
                    &approval_task.0,
                    McpApprovalResponse {
                        approval_id: approval_task.1,
                        approved: true,
                    },
                    Some(&owner),
                )
                .unwrap_err()
                .code,
            ProtocolErrorCode::Conflict
        );
        assert!(server.recover_actions(Some(&owner)).unwrap().is_empty());
        let polled = send(
            &server,
            "owner",
            Some(&owner),
            &json!({"jsonrpc":"2.0","id":92,"method":"tasks/get","params":{"taskId":task_id}}),
        );
        assert_eq!(polled.response.unwrap()["result"]["status"], "failed");
        let result = send(
            &server,
            "owner",
            Some(&owner),
            &json!({"jsonrpc":"2.0","id":93,"method":"tasks/result","params":{"taskId":task_id}}),
        );
        assert_eq!(result.response.unwrap()["error"]["code"], MCP_CONFLICT);
    }

    #[test]
    fn retention_gc_bounds_receipts_and_retires_side_effect_namespace_fail_closed() {
        let mut registry = McpServerRegistry::new();
        registry
            .register_tool(tool("task", McpTaskSupport::Optional))
            .unwrap();
        let clock = Arc::new(MutableClock::new(1_000));
        let (base, _) = server_with_mutable_clock(registry, clock.clone());
        let server = base.with_retention_limits(1, 10);
        let owner = actor("tenant-a");
        initialize(&server, "owner", &owner);
        let first = send(
            &server,
            "owner",
            Some(&owner),
            &json!({"jsonrpc":"2.0","id":100,"method":"tools/call","params":{"name":"task","arguments":{"value":"one"},"task":{"ttl":1000}}}),
        );
        let task_id = first.response.unwrap()["result"]["task"]["taskId"]
            .as_str()
            .unwrap()
            .to_owned();
        server
            .complete_tool(
                &task_id,
                json!({"content":[{"type":"text","text":"done"}],"isError":false}),
            )
            .unwrap();
        clock.set(1_011);
        initialize(&server, "fresh-owner", &owner);
        let second = send(
            &server,
            "fresh-owner",
            Some(&owner),
            &json!({"jsonrpc":"2.0","id":101,"method":"tools/call","params":{"name":"task","arguments":{"value":"two"},"task":{"ttl":1000}}}),
        );
        assert!(second.governed_action.is_some());

        let mut registry = McpServerRegistry::new();
        registry
            .register_tool(tool("write", McpTaskSupport::Forbidden))
            .unwrap();
        let receipt_clock = Arc::new(MutableClock::new(2_000));
        let (base, _) = server_with_mutable_clock(registry, receipt_clock.clone());
        let receipt_server = base.with_limits(DEFAULT_MCP_MAX_REQUEST_BYTES, 1);
        initialize(&receipt_server, "receipt-owner", &owner);
        receipt_clock.set(2_000 + DEFAULT_MCP_RETENTION_MS + 1);
        let call = json!({"jsonrpc":"2.0","id":110,"method":"tools/call","params":{"name":"write","arguments":{"value":"one"}}});
        let started = send(&receipt_server, "receipt-owner", Some(&owner), &call);
        let task_id = match started.governed_action.unwrap().action().unwrap() {
            McpServerAction::InvokeTool { task } => task.task_id.clone(),
            other => panic!("unexpected action: {other:?}"),
        };
        receipt_server
            .complete_tool(
                &task_id,
                json!({"content":[{"type":"text","text":"written"}],"isError":false}),
            )
            .unwrap();
        receipt_clock.set(2_000 + DEFAULT_MCP_RETENTION_MS * 2 + 2);
        let fresh_connection = send(
            &receipt_server,
            "fresh-owner",
            Some(&owner),
            &json!({
                "jsonrpc": "2.0",
                "id": "fresh-init",
                "method": "initialize",
                "params": {
                    "protocolVersion": MCP_SERVER_CONTRACT_REVISION,
                    "capabilities": {},
                    "clientInfo": {"name": "fixture", "version": "1.0"}
                }
            }),
        );
        assert_eq!(
            fresh_connection.response.unwrap()["result"]["protocolVersion"],
            MCP_SERVER_CONTRACT_REVISION
        );
        let replay = send(&receipt_server, "receipt-owner", Some(&owner), &call);
        assert_eq!(
            replay.response.as_ref().unwrap()["error"]["code"],
            MCP_RECONCILIATION_REQUIRED
        );
        assert_eq!(
            replay.response.unwrap()["error"]["data"],
            json!({
                "type": "receipt_reconciliation_required",
                "reconciliationRequired": true,
            })
        );
        assert!(replay.governed_action.is_none());
        assert_eq!(receipt_server.snapshot().unwrap().receipts.len(), 1);
    }

    #[test]
    fn sqlite_restart_persists_retired_receipt_namespace_and_never_replays_side_effect() {
        use crate::protocols::SqliteMcpStore;
        use rusqlite::Connection;

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("mcp-receipt-retirement.sqlite3");
        let owner = actor("tenant-a");
        let clock = Arc::new(MutableClock::new(10_000));
        let mut registry = McpServerRegistry::new();
        registry
            .register_tool(tool("write", McpTaskSupport::Forbidden))
            .unwrap();
        let store =
            Arc::new(SqliteMcpStore::from_connection(Connection::open(&path).unwrap()).unwrap());
        let server = McpJsonRpcServer::new(
            "sqlite-receipt-retirement",
            McpServerInfo::new("aikit-tests", "1.0.0").unwrap(),
            store,
            registry,
        )
        .unwrap()
        .with_clock(clock.clone())
        .with_limits(DEFAULT_MCP_MAX_REQUEST_BYTES, 1)
        .with_retention_limits(DEFAULT_MCP_MAX_TASKS, 10);
        initialize(&server, "sqlite-owner", &owner);
        clock.set(10_011);
        let call = json!({
            "jsonrpc":"2.0",
            "id":"write-once",
            "method":"tools/call",
            "params":{"name":"write","arguments":{"value":"one"}}
        });
        let started = send(&server, "sqlite-owner", Some(&owner), &call);
        let task_id = match started.governed_action.unwrap().action().unwrap() {
            McpServerAction::InvokeTool { task } => task.task_id.clone(),
            other => panic!("unexpected action: {other:?}"),
        };
        server
            .complete_tool(
                &task_id,
                json!({"content":[{"type":"text","text":"written"}],"isError":false}),
            )
            .unwrap();
        drop(server);

        clock.set(10_022);
        let reopened =
            Arc::new(SqliteMcpStore::from_connection(Connection::open(&path).unwrap()).unwrap());
        let restarted = McpJsonRpcServer::new(
            "sqlite-receipt-retirement",
            McpServerInfo::new("aikit-tests", "1.0.0").unwrap(),
            reopened,
            McpServerRegistry::new(),
        )
        .unwrap()
        .with_clock(clock)
        .with_limits(DEFAULT_MCP_MAX_REQUEST_BYTES, 1)
        .with_retention_limits(DEFAULT_MCP_MAX_TASKS, 10);
        let replay = send(&restarted, "sqlite-owner", Some(&owner), &call);
        assert_eq!(
            replay.response.unwrap()["error"]["code"],
            MCP_RECONCILIATION_REQUIRED
        );
        assert!(replay.governed_action.is_none());
        let fresh_connection = send(
            &restarted,
            "sqlite-fresh-owner",
            Some(&owner),
            &json!({
                "jsonrpc": "2.0",
                "id": "fresh-init",
                "method": "initialize",
                "params": {
                    "protocolVersion": MCP_SERVER_CONTRACT_REVISION,
                    "capabilities": {},
                    "clientInfo": {"name": "fixture", "version": "1.0"}
                }
            }),
        );
        assert_eq!(
            fresh_connection.response.unwrap()["result"]["protocolVersion"],
            MCP_SERVER_CONTRACT_REVISION
        );

        let restarted_again = McpJsonRpcServer::new(
            "sqlite-receipt-retirement",
            McpServerInfo::new("aikit-tests", "1.0.0").unwrap(),
            Arc::new(SqliteMcpStore::from_connection(Connection::open(&path).unwrap()).unwrap()),
            McpServerRegistry::new(),
        )
        .unwrap()
        .with_clock(Arc::new(MutableClock::new(10_023)))
        .with_limits(DEFAULT_MCP_MAX_REQUEST_BYTES, 1)
        .with_retention_limits(DEFAULT_MCP_MAX_TASKS, 10);
        let replay_after_second_restart =
            send(&restarted_again, "sqlite-owner", Some(&owner), &call);
        assert_eq!(
            replay_after_second_restart.response.unwrap()["error"]["code"],
            MCP_RECONCILIATION_REQUIRED
        );
        assert!(replay_after_second_restart.governed_action.is_none());
    }

    #[test]
    fn result_byte_item_and_depth_limits_reject_before_persistence() {
        let mut registry = McpServerRegistry::new();
        registry
            .register_tool(tool("bounded", McpTaskSupport::Forbidden))
            .unwrap();
        let (base, _) = server_with_registry(registry);
        let server = base.with_result_limits(128, 8, 4);
        let owner = actor("tenant-a");
        initialize(&server, "owner", &owner);
        let started = send(
            &server,
            "owner",
            Some(&owner),
            &json!({"jsonrpc":"2.0","id":120,"method":"tools/call","params":{"name":"bounded","arguments":{"value":"x"}}}),
        );
        let task_id = match started.governed_action.unwrap().action().unwrap() {
            McpServerAction::InvokeTool { task } => task.task_id.clone(),
            other => panic!("unexpected action: {other:?}"),
        };
        assert!(server
            .complete_tool(
                &task_id,
                json!({"content":[{"type":"text","text":"x".repeat(256)}]})
            )
            .is_err());
        assert!(server
            .complete_tool(&task_id, json!({"content":[],"extra":{"a":{"b":{"c":1}}}}))
            .is_err());
        assert!(server
            .complete_tool(&task_id, json!({"content":[1,2,3,4,5,6,7,8]}))
            .is_err());
        assert_eq!(
            server.snapshot().unwrap().registry.tasks()[&task_id].status,
            McpTaskStatus::Working
        );
        server
            .complete_tool(&task_id, json!({"content":[],"isError":false}))
            .unwrap();
    }

    #[test]
    fn resources_prompts_and_malformed_frames_follow_json_rpc_boundaries() {
        let mut registry = McpServerRegistry::new();
        registry
            .register_resource(McpResourceDefinition {
                uri: "file:///safe.txt".into(),
                name: "safe".into(),
                description: Some("fixture".into()),
                mime_type: Some("text/plain".into()),
                required_scopes: BTreeSet::new(),
            })
            .unwrap();
        registry
            .register_prompt(McpPromptDefinition {
                name: "review".into(),
                description: Some("fixture".into()),
                arguments: vec![McpPromptArgument {
                    name: "file".into(),
                    description: None,
                    required: true,
                }],
                required_scopes: BTreeSet::new(),
            })
            .unwrap();
        let (server, _) = server_with_registry(registry);
        let owner = actor("tenant-a");
        initialize(&server, "owner", &owner);

        let read = send(
            &server,
            "owner",
            Some(&owner),
            &json!({"jsonrpc":"2.0","id":"read","method":"resources/read","params":{"uri":"file:///safe.txt"}}),
        );
        assert!(matches!(
            read.governed_action
                .as_ref()
                .and_then(|value| value.action()),
            Some(McpServerAction::ReadResource { .. })
        ));
        let pending = read.pending.unwrap();
        let completed = server
            .complete_pending(
                &pending,
                Ok(json!({"contents":[{"uri":"file:///safe.txt","mimeType":"text/plain","text":"safe"}]})),
            )
            .unwrap();
        assert_eq!(completed.message["id"], "read");

        let missing_argument = send(
            &server,
            "owner",
            Some(&owner),
            &json!({"jsonrpc":"2.0","id":"prompt","method":"prompts/get","params":{"name":"review","arguments":{}}}),
        );
        assert_eq!(
            missing_argument.response.unwrap()["error"]["code"],
            MCP_FORBIDDEN
        );
        let invalid_argument_type = send(
            &server,
            "owner",
            Some(&owner),
            &json!({"jsonrpc":"2.0","id":"prompt-type","method":"prompts/get","params":{"name":"review","arguments":{"file":42}}}),
        );
        assert_eq!(
            invalid_argument_type.response.unwrap()["error"]["code"],
            JSONRPC_INVALID_PARAMS
        );

        let batch = server
            .handle(
                "owner",
                Some(&owner),
                br#"[{"jsonrpc":"2.0","id":1,"method":"tools/list"}]"#,
            )
            .unwrap();
        assert_eq!(
            batch.response.unwrap()["error"]["code"],
            JSONRPC_INVALID_REQUEST
        );
        let invalid_id = send(
            &server,
            "owner",
            Some(&owner),
            &json!({"jsonrpc":"2.0","id":null,"method":"tools/list","params":{}}),
        );
        assert_eq!(
            invalid_id.response.unwrap()["error"]["code"],
            JSONRPC_INVALID_REQUEST
        );
    }

    #[test]
    fn system_clock_formats_epoch_and_known_dates_as_rfc3339() {
        assert_eq!(unix_seconds_rfc3339(0), "1970-01-01T00:00:00Z");
        assert_eq!(unix_seconds_rfc3339(1_720_440_000), "2024-07-08T12:00:00Z");
    }

    #[test]
    fn invalid_tool_input_is_a_model_visible_result_and_malformed_output_is_rejected() {
        let mut registry = McpServerRegistry::new();
        registry
            .register_tool(tool("typed", McpTaskSupport::Forbidden))
            .unwrap();
        let (server, _) = server_with_registry(registry);
        let owner = actor("tenant-a");
        initialize(&server, "owner", &owner);

        let invalid = send(
            &server,
            "owner",
            Some(&owner),
            &json!({"jsonrpc":"2.0","id":20,"method":"tools/call","params":{"name":"typed","arguments":{"value":42}}}),
        );
        assert_eq!(invalid.response.unwrap()["result"]["isError"], true);
        assert!(invalid.governed_action.is_none());

        let valid = send(
            &server,
            "owner",
            Some(&owner),
            &json!({"jsonrpc":"2.0","id":21,"method":"tools/call","params":{"name":"typed","arguments":{"value":"ok"}}}),
        );
        let task_id = match valid.governed_action.unwrap().action().unwrap() {
            McpServerAction::InvokeTool { task } => task.task_id.clone(),
            other => panic!("unexpected action: {other:?}"),
        };
        let direct_tasks_are_private = send(
            &server,
            "owner",
            Some(&owner),
            &json!({"jsonrpc":"2.0","id":22,"method":"tasks/list","params":{}}),
        );
        assert_eq!(
            direct_tasks_are_private.response.unwrap()["result"]["tasks"],
            json!([])
        );
        assert!(server
            .complete_tool(&task_id, json!({"wrong":true}))
            .is_err());
        server
            .complete_tool(
                &task_id,
                json!({"content":[{"type":"text","text":"ok"}],"isError":false}),
            )
            .unwrap();
    }

    #[test]
    fn invalid_task_ttl_and_opaque_cursor_tampering_are_rejected() {
        let mut registry = McpServerRegistry::new();
        registry
            .register_tool(tool("task", McpTaskSupport::Optional))
            .unwrap();
        let (server, _) = server_with_registry(registry);
        let owner = actor("tenant-a");
        initialize(&server, "owner", &owner);
        let invalid_ttl = send(
            &server,
            "owner",
            Some(&owner),
            &json!({"jsonrpc":"2.0","id":30,"method":"tools/call","params":{"name":"task","arguments":{"value":"x"},"task":{"ttl":-1}}}),
        );
        assert_eq!(
            invalid_ttl.response.unwrap()["error"]["code"],
            JSONRPC_INVALID_PARAMS
        );
        let cursor = send(
            &server,
            "owner",
            Some(&owner),
            &json!({"jsonrpc":"2.0","id":31,"method":"tasks/list","params":{"cursor":"forged"}}),
        );
        assert_eq!(
            cursor.response.unwrap()["error"]["code"],
            JSONRPC_INVALID_PARAMS
        );
    }

    #[test]
    fn persisted_task_key_tampering_is_rejected_before_serving_requests() {
        let mut registry = McpServerRegistry::new();
        registry
            .register_tool(tool("task", McpTaskSupport::Optional))
            .unwrap();
        let (server, _) = server_with_registry(registry);
        let owner = actor("tenant-a");
        initialize(&server, "owner", &owner);
        let created = send(
            &server,
            "owner",
            Some(&owner),
            &json!({"jsonrpc":"2.0","id":40,"method":"tools/call","params":{"name":"task","arguments":{"value":"x"},"task":{"ttl":60000}}}),
        );
        let task_id = created.response.unwrap()["result"]["task"]["taskId"]
            .as_str()
            .unwrap()
            .to_owned();
        let mut encoded = serde_json::to_value(server.snapshot().unwrap()).unwrap();
        encoded["registry"]["tasks"][&task_id]["task_id"] = Value::String("tampered".into());
        let corrupted: McpServerState = serde_json::from_value(encoded).unwrap();
        let result = McpJsonRpcServer::new(
            "tests",
            McpServerInfo::new("aikit-tests", "1.0.0").unwrap(),
            Arc::new(FixedStateStore { state: corrupted }),
            McpServerRegistry::new(),
        );
        assert!(matches!(
            result,
            Err(ProtocolError {
                code: ProtocolErrorCode::InvalidRequest,
                ..
            })
        ));
    }
}
