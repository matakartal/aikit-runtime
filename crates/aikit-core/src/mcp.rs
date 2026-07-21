//! A minimal **MCP (Model Context Protocol) client** — connect an agent to external tool servers.
//!
//! MCP is JSON-RPC 2.0 over a transport. This module gives you:
//!   - [`McpTransport`] — the transport seam (send a request / a notification).
//!   - [`StdioTransport`] — the production transport: spawn an MCP server subprocess and exchange
//!     newline-delimited JSON-RPC over its stdin/stdout, correlating replies by id.
//!   - [`McpClient`] — the handshake (`initialize`), filtered tool discovery
//!     (`tools/list` → [`ToolSpec`]s), and invocation (`tools/call`).
//!   - [`McpToolFilter`] — an exact allow/deny visibility boundary applied before discovery cache.
//!   - [`McpToolExecutor`] — adapts one or more clients to a [`ToolExecutor`], so MCP tools run
//!     through the **same** agent loop and governance (permissions/hooks/sandbox) as native tools.
//!
//! The protocol logic is transport-agnostic and unit-tested against an in-memory mock, so it is
//! verified without needing a real MCP server binary.

use crate::error::{AikitError, Result};
use crate::tools::ToolExecutor;
use crate::types::ToolSpec;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::{self, Write};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex};

/// The MCP revision this client advertises in the legacy `initialize` handshake.
pub const MCP_PROTOCOL_VERSION: &str = "2025-06-18";

/// The stateless MCP revision (2026-07-28): no `initialize` handshake, client identity in
/// `_meta`, and proactive capability discovery via `server/discover`. Finalized July 28, 2026;
/// aikit tracks the release candidate and keeps every version-specific detail inside
/// [`McpProtocolVersion`] resolution so a late spec change stays contained.
pub const MCP_PROTOCOL_VERSION_2026: &str = "2026-07-28";

/// Legacy protocol revisions this client accepts from an `initialize` reply. They all share the
/// initialize/initialized lifecycle, so a compliant server selecting any of them is usable.
const LEGACY_ACCEPTED_VERSIONS: [&str; 3] = ["2025-06-18", "2025-03-26", "2025-11-25"];

/// `_meta` keys that carry client identity on every 2026-07-28 request.
const META_PROTOCOL_VERSION_KEY: &str = "io.modelcontextprotocol/protocolVersion";
const META_CLIENT_INFO_KEY: &str = "io.modelcontextprotocol/clientInfo";
const META_CLIENT_CAPABILITIES_KEY: &str = "io.modelcontextprotocol/clientCapabilities";

/// Maximum number of `inputRequired` round trips honored for one `tools/call` before failing
/// closed. Bounds a hostile or looping server.
pub const MAX_MCP_INPUT_ROUNDS: usize = 4;

/// Which MCP protocol revision an [`McpClient`] speaks.
///
/// The default is the legacy initialize-handshake flow, byte-identical to previous releases.
/// `V2026_07_28` and `Auto` are explicit opt-ins while the 2026-07-28 revision finalizes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpProtocolVersion {
    /// Today's behavior: `initialize` + `notifications/initialized`, session headers honored.
    #[default]
    V2025_06_18,
    /// Stateless 2026-07-28: `server/discover`, `_meta` client identity, no session header.
    V2026_07_28,
    /// Probe `server/discover`; fall back to the legacy handshake when the server answers it
    /// with a JSON-RPC error. A transport failure is a failure, not a fallback.
    Auto,
}

/// The dialect actually resolved for a connection after [`McpClient::initialize`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResolvedDialect {
    Legacy,
    Rc2026,
}

/// A JSON-RPC-level error reply (as opposed to a transport failure), surfaced with its code so
/// protocol logic can branch — e.g. `Auto` falling back to the legacy handshake on
/// method-not-found — without string matching.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpRpcError {
    pub code: Option<i64>,
    pub message: String,
}

/// Per-request routing metadata for the 2026-07-28 revision. HTTP transports project it into the
/// `MCP-Protocol-Version`, `Mcp-Method`, and `Mcp-Name` headers; stdio needs no header analogue.
#[derive(Debug, Clone)]
pub struct McpRequestMeta {
    pub protocol_version: &'static str,
    pub method: String,
    pub name: Option<String>,
}

/// Host-provided answers for a 2026-07-28 `inputRequired` result: return `inputResponses` keyed
/// identically to the server's `inputRequests`. Errors abort the call. Without a configured
/// handler, an input-requiring tool call fails closed — including for MCP safety-server
/// guardrails, where a server demanding input is itself a guardrail failure.
#[async_trait]
pub trait McpInputHandler: Send + Sync {
    async fn provide(&self, server: &str, tool: &str, input_requests: Value) -> Result<Value>;
}

/// Maximum number of Unicode scalar values accepted in one configured MCP tool name.
///
/// The current MCP guidance caps tool names at 128 characters. Keeping the same bound on filter
/// input prevents an untrusted configuration document from retaining arbitrarily large names.
pub const MAX_MCP_TOOL_NAME_CHARS: usize = 128;

/// Maximum number of names retained across one MCP tool filter's allow and deny sets.
pub const MAX_MCP_TOOL_FILTER_NAMES: usize = 1_024;

/// Maximum number of requests made by one paginated MCP discovery operation.
pub const MAX_MCP_DISCOVERY_PAGES: usize = 128;

/// Maximum number of items accepted by one MCP discovery operation, before visibility filtering.
pub const MAX_MCP_DISCOVERY_ITEMS: usize = 10_000;

/// Maximum cumulative serialized size of items accepted by one MCP discovery operation.
pub const MAX_MCP_DISCOVERY_ITEM_BYTES: usize = 8 << 20;

/// Maximum UTF-8 size of one opaque MCP pagination cursor.
pub const MAX_MCP_CURSOR_BYTES: usize = 4 << 10;

/// Maximum cumulative UTF-8 size retained for distinct cursors during one discovery operation.
pub const MAX_MCP_DISCOVERY_CURSOR_BYTES: usize = 64 << 10;

/// Maximum response size accepted from either MCP transport before JSON decoding.
pub const MAX_MCP_TRANSPORT_RESPONSE_BYTES: usize = 4 << 20;

#[derive(Default)]
struct DiscoveryBudget {
    pages: usize,
    items: usize,
    item_bytes: usize,
    cursor_bytes: usize,
    seen_cursors: BTreeSet<String>,
}

impl DiscoveryBudget {
    fn with_initial_cursor(method: &str, cursor: Option<&str>) -> Result<Self> {
        let mut budget = Self::default();
        if let Some(cursor) = cursor {
            budget.retain_cursor(method, cursor)?;
        }
        Ok(budget)
    }

    fn begin_page(&mut self, method: &str) -> Result<()> {
        if self.pages >= MAX_MCP_DISCOVERY_PAGES {
            return Err(discovery_limit_error(
                method,
                format!("exceeded {MAX_MCP_DISCOVERY_PAGES} pages"),
            ));
        }
        self.pages += 1;
        Ok(())
    }

    fn observe_items(&mut self, method: &str, items: &[Value]) -> Result<()> {
        if self.items.saturating_add(items.len()) > MAX_MCP_DISCOVERY_ITEMS {
            return Err(discovery_limit_error(
                method,
                format!("exceeded {MAX_MCP_DISCOVERY_ITEMS} items"),
            ));
        }
        self.items += items.len();

        for item in items {
            let remaining = MAX_MCP_DISCOVERY_ITEM_BYTES.saturating_sub(self.item_bytes);
            let mut counter = BoundedJsonCounter::new(remaining);
            if serde_json::to_writer(&mut counter, item).is_err() {
                return Err(discovery_limit_error(
                    method,
                    format!(
                        "exceeded {} cumulative item bytes",
                        MAX_MCP_DISCOVERY_ITEM_BYTES
                    ),
                ));
            }
            self.item_bytes += counter.written;
        }
        Ok(())
    }

    fn next_cursor(&mut self, method: &str, result: &Value) -> Result<Option<String>> {
        let Some(cursor) = result.get("nextCursor").and_then(Value::as_str) else {
            // Preserve the historical behaviour: an omitted or non-string cursor ends discovery.
            return Ok(None);
        };
        self.retain_cursor(method, cursor)?;
        Ok(Some(cursor.to_owned()))
    }

    fn retain_cursor(&mut self, method: &str, cursor: &str) -> Result<()> {
        let bytes = cursor.len();
        if bytes > MAX_MCP_CURSOR_BYTES {
            return Err(discovery_limit_error(
                method,
                format!("cursor exceeded {MAX_MCP_CURSOR_BYTES} bytes"),
            ));
        }
        if self.cursor_bytes.saturating_add(bytes) > MAX_MCP_DISCOVERY_CURSOR_BYTES {
            return Err(discovery_limit_error(
                method,
                format!(
                    "cursors exceeded {} cumulative bytes",
                    MAX_MCP_DISCOVERY_CURSOR_BYTES
                ),
            ));
        }
        if !self.seen_cursors.insert(cursor.to_owned()) {
            return Err(AikitError::ToolExecution(format!(
                "MCP {method} repeated a pagination cursor"
            )));
        }
        self.cursor_bytes += bytes;
        Ok(())
    }
}

struct BoundedJsonCounter {
    remaining: usize,
    written: usize,
}

impl BoundedJsonCounter {
    fn new(remaining: usize) -> Self {
        Self {
            remaining,
            written: 0,
        }
    }
}

impl Write for BoundedJsonCounter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.remaining {
            return Err(io::Error::other("MCP discovery byte budget exceeded"));
        }
        self.remaining -= bytes.len();
        self.written += bytes.len();
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn discovery_limit_error(method: &str, detail: String) -> AikitError {
    AikitError::ToolExecution(format!("MCP {method} discovery {detail}"))
}

fn valid_discovered_tool_name(name: &str) -> bool {
    !name.trim().is_empty()
        && name.chars().count() <= MAX_MCP_TOOL_NAME_CHARS
        && !name
            .chars()
            .any(|character| character.is_control() || is_unsafe_display_character(character))
}

fn is_unsafe_display_character(character: char) -> bool {
    matches!(
        character,
        '\u{061c}'
            | '\u{200e}'
            | '\u{200f}'
            | '\u{202a}'..='\u{202e}'
            | '\u{2066}'..='\u{206f}'
    )
}

/// Exact, case-sensitive visibility policy for tools discovered from one MCP server.
///
/// `allow = None` preserves the historical allow-all behaviour. `allow = Some([])` intentionally
/// exposes no tools. A deny entry is always authoritative, including when the same exact name is
/// also present in the allow set.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct McpToolFilter {
    allow: Option<BTreeSet<String>>,
    deny: BTreeSet<String>,
}

impl McpToolFilter {
    /// Build and validate an exact-name visibility policy.
    pub fn new(allow: Option<Vec<String>>, deny: Vec<String>) -> Result<Self> {
        let total = allow
            .as_ref()
            .map_or(deny.len(), |allow| allow.len().saturating_add(deny.len()));
        if total > MAX_MCP_TOOL_FILTER_NAMES {
            return Err(AikitError::Configuration(format!(
                "MCP tool filter accepts at most {MAX_MCP_TOOL_FILTER_NAMES} names"
            )));
        }
        Ok(Self {
            allow: allow
                .map(|names| validate_filter_names("allow", names))
                .transpose()?,
            deny: validate_filter_names("deny", deny)?,
        })
    }

    /// Parse the binding-friendly `{ "allow": [...], "deny": [...] }` shape without accepting
    /// unknown fields or `null` in place of a list.
    pub fn from_value(value: Value) -> Result<Self> {
        let object = value
            .as_object()
            .ok_or_else(|| AikitError::Configuration("MCP tool filter must be an object".into()))?;
        if object
            .keys()
            .any(|key| key.as_str() != "allow" && key.as_str() != "deny")
        {
            return Err(AikitError::Configuration(
                "MCP tool filter contains an unknown field".into(),
            ));
        }
        let total = ["allow", "deny"].into_iter().fold(0usize, |total, key| {
            total.saturating_add(
                object
                    .get(key)
                    .and_then(Value::as_array)
                    .map_or(0, Vec::len),
            )
        });
        if total > MAX_MCP_TOOL_FILTER_NAMES {
            return Err(AikitError::Configuration(format!(
                "MCP tool filter accepts at most {MAX_MCP_TOOL_FILTER_NAMES} names"
            )));
        }
        let allow = object
            .get("allow")
            .map(|value| filter_name_list("allow", value))
            .transpose()?;
        let deny = object
            .get("deny")
            .map(|value| filter_name_list("deny", value))
            .transpose()?
            .unwrap_or_default();
        Self::new(allow, deny)
    }

    /// Whether an exact tool name may be advertised and invoked.
    pub fn allows(&self, name: &str) -> bool {
        !self.deny.contains(name)
            && self
                .allow
                .as_ref()
                .is_none_or(|allowed| allowed.contains(name))
    }

    /// The configured allow set, or `None` when all non-denied names are allowed.
    pub fn allow(&self) -> Option<&BTreeSet<String>> {
        self.allow.as_ref()
    }

    /// The authoritative exact-name deny set.
    pub fn deny(&self) -> &BTreeSet<String> {
        &self.deny
    }

    fn is_unrestricted(&self) -> bool {
        self.allow.is_none() && self.deny.is_empty()
    }
}

fn filter_name_list(kind: &str, value: &Value) -> Result<Vec<String>> {
    let values = value.as_array().ok_or_else(|| {
        AikitError::Configuration(format!(
            "MCP tool filter {kind} must be an array of strings"
        ))
    })?;
    values
        .iter()
        .enumerate()
        .map(|(index, value)| {
            value.as_str().map(str::to_owned).ok_or_else(|| {
                AikitError::Configuration(format!(
                    "MCP tool filter {kind}[{index}] must be a string"
                ))
            })
        })
        .collect()
}

fn validate_filter_names(kind: &str, names: Vec<String>) -> Result<BTreeSet<String>> {
    let mut validated = BTreeSet::new();
    for name in names {
        let characters = name.chars().count();
        if name.trim().is_empty() {
            return Err(AikitError::Configuration(format!(
                "MCP tool filter {kind} names must not be empty"
            )));
        }
        if characters > MAX_MCP_TOOL_NAME_CHARS {
            return Err(AikitError::Configuration(format!(
                "MCP tool filter {kind} name exceeds {MAX_MCP_TOOL_NAME_CHARS} characters"
            )));
        }
        if name
            .chars()
            .any(|character| character.is_control() || is_unsafe_display_character(character))
        {
            return Err(AikitError::Configuration(format!(
                "MCP tool filter {kind} names must not contain control or bidirectional formatting characters"
            )));
        }
        if !validated.insert(name) {
            return Err(AikitError::Configuration(format!(
                "MCP tool filter {kind} contains a duplicate name"
            )));
        }
    }
    Ok(validated)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct McpResource {
    pub uri: String,
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(rename = "mimeType", default)]
    pub mime_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct McpPrompt {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub arguments: Vec<Value>,
}

/// A JSON-RPC transport to an MCP server. Implementations correlate a request with its reply.
#[async_trait]
pub trait McpTransport: Send + Sync {
    /// Send a JSON-RPC request and await its `result` (or surface its `error`).
    async fn request(&self, method: &str, params: Value) -> Result<Value>;
    /// Send a fire-and-forget JSON-RPC notification (no `id`, no reply).
    async fn notify(&self, method: &str, params: Value) -> Result<()>;

    /// Like [`request`](Self::request), but a JSON-RPC-level error reply is `Ok(Err(...))` with
    /// its code, while transport failures stay `Err`. The default cannot distinguish the two, so
    /// custom transports that keep it also cannot support `Auto` version probing — declare the
    /// protocol version explicitly instead.
    async fn request_detailed(
        &self,
        method: &str,
        params: Value,
    ) -> Result<std::result::Result<Value, McpRpcError>> {
        self.request(method, params).await.map(Ok)
    }

    /// Like [`request`](Self::request), carrying 2026-07-28 routing metadata. HTTP transports
    /// project it into headers; the default ignores it, which is correct for stdio because the
    /// decorated `_meta` params already carry the client identity.
    async fn request_with_meta(
        &self,
        method: &str,
        params: Value,
        _meta: McpRequestMeta,
    ) -> Result<Value> {
        self.request(method, params).await
    }
}

/// An MCP client over some [`McpTransport`]. Discovered tools are cached as [`ToolSpec`]s.
pub struct McpClient {
    transport: Arc<dyn McpTransport>,
    server: String,
    tool_filter: McpToolFilter,
    tools: Vec<ToolSpec>,
    initialized: AtomicBool,
    configured_version: McpProtocolVersion,
    dialect: std::sync::OnceLock<ResolvedDialect>,
    input_handler: Option<Arc<dyn McpInputHandler>>,
}

impl McpClient {
    pub fn new(transport: Arc<dyn McpTransport>, server: impl Into<String>) -> Self {
        Self::new_with_tool_filter(transport, server, McpToolFilter::default())
    }

    /// Construct a client with an exact-name visibility filter. The filter is applied to every
    /// discovery page before any [`ToolSpec`] is cached.
    pub fn new_with_tool_filter(
        transport: Arc<dyn McpTransport>,
        server: impl Into<String>,
        tool_filter: McpToolFilter,
    ) -> Self {
        McpClient {
            transport,
            server: server.into(),
            tool_filter,
            tools: Vec::new(),
            initialized: AtomicBool::new(false),
            configured_version: McpProtocolVersion::default(),
            dialect: std::sync::OnceLock::new(),
            input_handler: None,
        }
    }

    /// Select the protocol revision this client speaks. The default is the legacy
    /// initialize-handshake flow; call before [`initialize`](Self::initialize).
    pub fn with_protocol_version(mut self, version: McpProtocolVersion) -> Self {
        self.configured_version = version;
        self
    }

    /// Install the host callback that answers 2026-07-28 `inputRequired` results. Without one,
    /// an input-requiring tool call fails closed.
    pub fn with_input_handler(mut self, handler: Arc<dyn McpInputHandler>) -> Self {
        self.input_handler = Some(handler);
        self
    }

    pub fn server_name(&self) -> &str {
        &self.server
    }

    /// Establish the connection for the configured protocol revision and arm the client.
    ///
    /// Legacy: the `initialize` handshake plus `notifications/initialized`, returning the raw
    /// server info/capabilities object. 2026-07-28: a `server/discover` capability fetch (the
    /// revision has no handshake), returning the raw discover result. `Auto` probes
    /// `server/discover` and falls back to the legacy handshake when the server answers it with
    /// a JSON-RPC error; a transport failure fails instead of falling back.
    pub async fn initialize(&self) -> Result<Value> {
        match self.configured_version {
            McpProtocolVersion::V2025_06_18 => self.initialize_legacy().await,
            McpProtocolVersion::V2026_07_28 => match self.discover_2026().await? {
                Ok(result) => Ok(result),
                Err(rpc) => Err(AikitError::ToolExecution(format!(
                    "MCP server does not support protocol {MCP_PROTOCOL_VERSION_2026}: \
                     server/discover failed: {}",
                    rpc.message
                ))),
            },
            McpProtocolVersion::Auto => match self.discover_2026().await? {
                Ok(result) => Ok(result),
                // Any JSON-RPC error reply (method-not-found, pre-handshake rejection, ...)
                // means "this server does not speak 2026-07-28"; fall back to the handshake.
                Err(_rpc) => self.initialize_legacy().await,
            },
        }
    }

    async fn initialize_legacy(&self) -> Result<Value> {
        let result = self
            .transport
            .request(
                "initialize",
                json!({
                    "protocolVersion": MCP_PROTOCOL_VERSION,
                    "capabilities": {},
                    "clientInfo": { "name": "aikit", "version": env!("CARGO_PKG_VERSION") }
                }),
            )
            .await?;
        let version = result
            .get("protocolVersion")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                AikitError::ToolExecution("MCP initialize omitted protocolVersion".into())
            })?;
        if !LEGACY_ACCEPTED_VERSIONS.contains(&version) {
            return Err(AikitError::ToolExecution(format!(
                "MCP server selected unsupported protocol version '{version}'"
            )));
        }
        self.transport
            .notify("notifications/initialized", json!({}))
            .await?;
        let _ = self.dialect.set(ResolvedDialect::Legacy);
        self.initialized.store(true, Ordering::Release);
        Ok(result)
    }

    /// Probe/perform 2026-07-28 startup. Outer `Err` is a transport failure; inner `Err` is the
    /// server's JSON-RPC error reply (the `Auto` fallback signal); inner `Ok` armed the client.
    async fn discover_2026(&self) -> Result<std::result::Result<Value, McpRpcError>> {
        let reply = self
            .transport
            .request_detailed("server/discover", json!({}))
            .await?;
        let result = match reply {
            Ok(result) => result,
            Err(rpc) => return Ok(Err(rpc)),
        };
        if !discover_supports_2026(&result) {
            // The server answers `server/discover` but does not advertise the revision this
            // client would speak; treat it like a JSON-RPC refusal so Auto can fall back.
            return Ok(Err(McpRpcError {
                code: None,
                message: format!(
                    "server/discover did not advertise protocol {MCP_PROTOCOL_VERSION_2026}"
                ),
            }));
        }
        let _ = self.dialect.set(ResolvedDialect::Rc2026);
        self.initialized.store(true, Ordering::Release);
        Ok(Ok(result))
    }

    fn ensure_initialized(&self) -> Result<()> {
        if self.initialized.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(AikitError::ToolExecution(
                "MCP client must be initialized before use".into(),
            ))
        }
    }

    fn resolved_dialect(&self) -> ResolvedDialect {
        *self.dialect.get().unwrap_or(&ResolvedDialect::Legacy)
    }

    /// The one choke point every post-startup request flows through. Legacy is byte-identical to
    /// the historical direct `transport.request` path; 2026-07-28 decorates `_meta` with the
    /// client identity triplet and forwards routing metadata for header-based transports.
    async fn dispatch(&self, method: &str, params: Value) -> Result<Value> {
        match self.resolved_dialect() {
            ResolvedDialect::Legacy => self.transport.request(method, params).await,
            ResolvedDialect::Rc2026 => {
                let name = params
                    .get("name")
                    .or_else(|| params.get("uri"))
                    .and_then(Value::as_str)
                    .map(str::to_owned);
                let mut params = params;
                let object = params.as_object_mut().ok_or_else(|| {
                    AikitError::ToolExecution(format!(
                        "MCP '{method}' params must be an object to carry _meta"
                    ))
                })?;
                let meta = object
                    .entry("_meta")
                    .or_insert_with(|| json!({}))
                    .as_object_mut()
                    .ok_or_else(|| {
                        AikitError::ToolExecution(format!(
                            "MCP '{method}' params carried a non-object _meta"
                        ))
                    })?;
                meta.insert(
                    META_PROTOCOL_VERSION_KEY.into(),
                    Value::String(MCP_PROTOCOL_VERSION_2026.into()),
                );
                meta.insert(
                    META_CLIENT_INFO_KEY.into(),
                    json!({ "name": "aikit", "version": env!("CARGO_PKG_VERSION") }),
                );
                meta.insert(META_CLIENT_CAPABILITIES_KEY.into(), json!({}));
                self.transport
                    .request_with_meta(
                        method,
                        params,
                        McpRequestMeta {
                            protocol_version: MCP_PROTOCOL_VERSION_2026,
                            method: method.to_owned(),
                            name,
                        },
                    )
                    .await
            }
        }
    }

    /// Call `tools/list` and cache the result as [`ToolSpec`]s (which the loop can advertise).
    pub async fn list_tools(&mut self) -> Result<Vec<ToolSpec>> {
        self.ensure_initialized()?;
        // A failed refresh must not leave previously advertised tools callable through a stale
        // restricted cache.
        self.tools.clear();
        const METHOD: &str = "tools/list";
        let mut budget = DiscoveryBudget::default();
        let mut cursor = None;
        let mut specs = Vec::new();
        let mut visible_names = BTreeSet::new();
        loop {
            budget.begin_page(METHOD)?;
            let params = cursor
                .as_deref()
                .map_or_else(|| json!({}), |cursor| json!({"cursor":cursor}));
            let result = self.dispatch(METHOD, params).await?;
            let page = result
                .get("tools")
                .and_then(Value::as_array)
                .map_or(&[][..], Vec::as_slice);
            budget.observe_items(METHOD, page)?;

            for tool in page {
                let Some(name) = tool.get("name").and_then(Value::as_str) else {
                    // Keep the established tolerant discovery behaviour for malformed entries.
                    continue;
                };
                if !valid_discovered_tool_name(name) || !self.tool_filter.allows(name) {
                    continue;
                }
                // Deterministic first-wins de-duplication avoids advertising ambiguous schemas
                // without making an otherwise useful server fail discovery.
                if !visible_names.insert(name.to_owned()) {
                    continue;
                }
                let description = tool
                    .get("description")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string();
                let input_schema = tool
                    .get("inputSchema")
                    .cloned()
                    .unwrap_or_else(|| json!({ "type": "object" }));
                specs.push(ToolSpec {
                    name: name.to_owned(),
                    description,
                    input_schema,
                });
            }

            let Some(next) = budget.next_cursor(METHOD, &result)? else {
                break;
            };
            cursor = Some(next);
        }
        self.tools = specs.clone();
        Ok(specs)
    }

    /// Whether this server advertises `name` (after [`list_tools`](Self::list_tools)).
    pub fn provides(&self, name: &str) -> bool {
        self.tools.iter().any(|t| t.name == name)
    }

    /// The discovered tool specs.
    pub fn tools(&self) -> &[ToolSpec] {
        &self.tools
    }

    /// Invoke `tools/call` and flatten the returned content blocks to a string. An MCP-level
    /// `isError` becomes an [`AikitError::ToolExecution`], matching native tool-failure semantics.
    pub async fn call_tool(&self, name: &str, arguments: Value) -> Result<String> {
        self.ensure_initialized()?;
        if !self.tool_filter.allows(name) {
            return Err(AikitError::ToolExecution(format!(
                "MCP tool '{name}' is hidden by its visibility filter"
            )));
        }
        if !self.tool_filter.is_unrestricted() && !self.provides(name) {
            return Err(AikitError::ToolExecution(format!(
                "MCP tool '{name}' was not advertised after visibility filtering"
            )));
        }
        let mut params = json!({ "name": name, "arguments": arguments });
        let mut result = self.dispatch("tools/call", params.clone()).await?;

        // 2026-07-28 multi-round-trip requests: a server may return `inputRequired` with its
        // questions and an opaque `requestState`; the client gathers answers and re-issues the
        // original call. Bounded, fail-closed, and the state blob is echoed verbatim — never
        // parsed. Honored defensively regardless of dialect: a legacy server should never send
        // `resultType`, and an unrecognized one is an error rather than a silent misparse.
        let mut rounds = 0usize;
        while let Some(result_type) = result.get("resultType").and_then(Value::as_str) {
            if result_type != "inputRequired" {
                return Err(AikitError::ToolExecution(format!(
                    "MCP tool '{name}' returned unrecognized resultType '{result_type}'"
                )));
            }
            let Some(handler) = &self.input_handler else {
                return Err(AikitError::ToolExecution(format!(
                    "MCP tool '{name}' requires additional input and no input handler is \
                     configured"
                )));
            };
            rounds += 1;
            if rounds > MAX_MCP_INPUT_ROUNDS {
                return Err(AikitError::ToolExecution(format!(
                    "MCP tool '{name}' exceeded {MAX_MCP_INPUT_ROUNDS} input round trips"
                )));
            }
            let request_state = result.get("requestState").cloned().ok_or_else(|| {
                AikitError::ToolExecution(format!(
                    "MCP tool '{name}' requested input without a requestState"
                ))
            })?;
            let input_requests = result.get("inputRequests").cloned().unwrap_or(json!({}));
            let responses = handler.provide(&self.server, name, input_requests).await?;
            let object = params.as_object_mut().expect("tools/call params object");
            object.insert("inputResponses".into(), responses);
            object.insert("requestState".into(), request_state);
            result = self.dispatch("tools/call", params.clone()).await?;
        }

        let body = flatten_content(&result);
        if result
            .get("isError")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            return Err(AikitError::ToolExecution(format!(
                "MCP tool '{name}' reported an error: {body}"
            )));
        }
        Ok(body)
    }

    pub async fn list_resources(&self, cursor: Option<&str>) -> Result<Vec<McpResource>> {
        self.ensure_initialized()?;
        const METHOD: &str = "resources/list";
        let mut budget = DiscoveryBudget::with_initial_cursor(METHOD, cursor)?;
        let mut cursor = cursor.map(str::to_owned);
        let mut resources = Vec::new();
        loop {
            budget.begin_page(METHOD)?;
            let params = cursor
                .as_deref()
                .map_or_else(|| json!({}), |cursor| json!({ "cursor": cursor }));
            let result = self.dispatch(METHOD, params).await?;
            let page: &[Value] = match result.get("resources") {
                Some(value) => value.as_array().map(Vec::as_slice).ok_or_else(|| {
                    AikitError::ToolExecution("invalid MCP resources: expected an array".into())
                })?,
                None => &[],
            };
            budget.observe_items(METHOD, page)?;
            for value in page {
                resources.push(serde_json::from_value(value.clone()).map_err(|error| {
                    AikitError::ToolExecution(format!("invalid MCP resources: {error}"))
                })?);
            }

            let Some(next) = budget.next_cursor(METHOD, &result)? else {
                break;
            };
            cursor = Some(next);
        }
        Ok(resources)
    }

    pub async fn read_resource(&self, uri: &str) -> Result<Value> {
        self.ensure_initialized()?;
        self.dispatch("resources/read", json!({ "uri": uri })).await
    }

    pub async fn list_prompts(&self, cursor: Option<&str>) -> Result<Vec<McpPrompt>> {
        self.ensure_initialized()?;
        const METHOD: &str = "prompts/list";
        let mut budget = DiscoveryBudget::with_initial_cursor(METHOD, cursor)?;
        let mut cursor = cursor.map(str::to_owned);
        let mut prompts = Vec::new();
        loop {
            budget.begin_page(METHOD)?;
            let params = cursor
                .as_deref()
                .map_or_else(|| json!({}), |cursor| json!({ "cursor": cursor }));
            let result = self.dispatch(METHOD, params).await?;
            let page: &[Value] = match result.get("prompts") {
                Some(value) => value.as_array().map(Vec::as_slice).ok_or_else(|| {
                    AikitError::ToolExecution("invalid MCP prompts: expected an array".into())
                })?,
                None => &[],
            };
            budget.observe_items(METHOD, page)?;
            for value in page {
                prompts.push(serde_json::from_value(value.clone()).map_err(|error| {
                    AikitError::ToolExecution(format!("invalid MCP prompts: {error}"))
                })?);
            }

            let Some(next) = budget.next_cursor(METHOD, &result)? else {
                break;
            };
            cursor = Some(next);
        }
        Ok(prompts)
    }

    pub async fn get_prompt(&self, name: &str, arguments: Value) -> Result<Value> {
        self.ensure_initialized()?;
        self.dispatch(
            "prompts/get",
            json!({ "name": name, "arguments": arguments }),
        )
        .await
    }
}

/// Whether a `server/discover` result advertises the 2026-07-28 revision. Tolerant to both
/// shapes seen across release-candidate snapshots — a `protocolVersions` array or a single
/// `protocolVersion` string — so a finalization tweak lands here and nowhere else.
fn discover_supports_2026(result: &Value) -> bool {
    if let Some(versions) = result.get("protocolVersions").and_then(Value::as_array) {
        return versions
            .iter()
            .any(|version| version.as_str() == Some(MCP_PROTOCOL_VERSION_2026));
    }
    result.get("protocolVersion").and_then(Value::as_str) == Some(MCP_PROTOCOL_VERSION_2026)
}

/// Flatten an MCP `{ content: [ { type: "text", text }, ... ] }` result to a string.
fn flatten_content(result: &Value) -> String {
    result
        .get("content")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|content| {
                    content
                        .get("text")
                        .and_then(Value::as_str)
                        .map(str::to_string)
                        .or_else(|| serde_json::to_string(content).ok())
                })
                .collect::<Vec<_>>()
                .join("\n")
        })
        .unwrap_or_default()
}

/// MCP Streamable HTTP transport with optional bearer authentication and session propagation.
/// The caller owns OAuth/token acquisition; this transport never persists credentials.
pub struct StreamableHttpTransport {
    client: reqwest::Client,
    endpoint: reqwest::Url,
    bearer_token: Option<String>,
    session_id: Mutex<Option<String>>,
    next_id: AtomicU64,
}

impl StreamableHttpTransport {
    pub fn new(endpoint: &str, bearer_token: Option<String>) -> Result<Self> {
        let endpoint = reqwest::Url::parse(endpoint).map_err(|error| {
            AikitError::Configuration(format!("invalid MCP HTTP endpoint: {error}"))
        })?;
        if !matches!(endpoint.scheme(), "https" | "http") {
            return Err(AikitError::Configuration(
                "MCP HTTP endpoint must use http or https".into(),
            ));
        }
        if endpoint.scheme() == "http" {
            let loopback = endpoint.host_str().is_some_and(|host| {
                host.eq_ignore_ascii_case("localhost")
                    || host
                        .parse::<std::net::IpAddr>()
                        .is_ok_and(|address| address.is_loopback())
            });
            if !loopback {
                return Err(AikitError::Configuration(
                    "remote MCP HTTP endpoints must use HTTPS".into(),
                ));
            }
        }
        Ok(Self {
            client: reqwest::Client::new(),
            endpoint,
            bearer_token,
            session_id: Mutex::new(None),
            next_id: AtomicU64::new(1),
        })
    }

    pub async fn session_id(&self) -> Option<String> {
        self.session_id.lock().await.clone()
    }

    async fn post(&self, payload: Value) -> Result<Option<Value>> {
        self.post_with(payload, None).await
    }

    /// POST one JSON-RPC payload. With 2026-07-28 metadata, the revision's routing headers are
    /// attached and the legacy `Mcp-Session-Id` header is neither sent nor recorded — the
    /// stateless revision removed sessions entirely.
    async fn post_with(
        &self,
        payload: Value,
        meta: Option<&McpRequestMeta>,
    ) -> Result<Option<Value>> {
        let mut request = self
            .client
            .post(self.endpoint.clone())
            .header(
                reqwest::header::ACCEPT,
                "application/json, text/event-stream",
            )
            .json(&payload);
        if let Some(token) = &self.bearer_token {
            request = request.bearer_auth(token);
        }
        if let Some(meta) = meta {
            request = request
                .header("MCP-Protocol-Version", meta.protocol_version)
                .header("Mcp-Method", meta.method.as_str());
            if let Some(name) = &meta.name {
                request = request.header("Mcp-Name", name.as_str());
            }
        } else if let Some(session) = self.session_id.lock().await.clone() {
            request = request.header("Mcp-Session-Id", session);
        }
        let response = request.send().await.map_err(|error| {
            AikitError::ToolExecution(format!("MCP HTTP request failed: {error}"))
        })?;
        let status = response.status();
        if meta.is_none() {
            if let Some(session) = response
                .headers()
                .get("Mcp-Session-Id")
                .and_then(|value| value.to_str().ok())
            {
                *self.session_id.lock().await = Some(session.to_string());
            }
        }
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("")
            .to_string();
        let mut stream = response.bytes_stream();
        let mut bytes = Vec::new();
        while let Some(chunk) = futures::StreamExt::next(&mut stream).await {
            let chunk = chunk.map_err(|error| {
                AikitError::ToolExecution(format!("MCP HTTP response failed: {error}"))
            })?;
            if bytes.len().saturating_add(chunk.len()) > MAX_MCP_TRANSPORT_RESPONSE_BYTES {
                return Err(AikitError::ToolExecution(
                    "MCP HTTP response exceeded 4 MiB".into(),
                ));
            }
            bytes.extend_from_slice(&chunk);
        }
        let body = String::from_utf8(bytes)
            .map_err(|_| AikitError::ToolExecution("MCP HTTP response was not UTF-8".into()))?;
        if !status.is_success() {
            return Err(AikitError::ToolExecution(format!(
                "MCP HTTP request returned {}",
                status.as_u16()
            )));
        }
        if body.trim().is_empty() {
            return Ok(None);
        }
        if content_type.contains("text/event-stream") {
            let value = body
                .lines()
                .filter_map(|line| line.strip_prefix("data:"))
                .filter_map(|data| serde_json::from_str::<Value>(data.trim()).ok())
                .next_back()
                .ok_or_else(|| {
                    AikitError::ToolExecution("MCP HTTP SSE contained no JSON message".into())
                })?;
            Ok(Some(value))
        } else {
            serde_json::from_str(&body)
                .map(Some)
                .map_err(|error| AikitError::ToolExecution(format!("invalid MCP JSON: {error}")))
        }
    }
}

impl StreamableHttpTransport {
    async fn rpc(
        &self,
        method: &str,
        params: Value,
        meta: Option<McpRequestMeta>,
    ) -> Result<std::result::Result<Value, McpRpcError>> {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let response = self
            .post_with(
                json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params }),
                meta.as_ref(),
            )
            .await?
            .ok_or_else(|| AikitError::ToolExecution("MCP request returned no response".into()))?;
        if let Some(error) = response.get("error") {
            return Ok(Err(McpRpcError {
                code: error.get("code").and_then(Value::as_i64),
                message: error
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown error")
                    .to_string(),
            }));
        }
        Ok(Ok(response.get("result").cloned().unwrap_or(Value::Null)))
    }
}

fn flatten_rpc_reply(
    method: &str,
    reply: std::result::Result<Value, McpRpcError>,
) -> Result<Value> {
    reply
        .map_err(|rpc| AikitError::ToolExecution(format!("MCP '{method}' failed: {}", rpc.message)))
}

#[async_trait]
impl McpTransport for StreamableHttpTransport {
    async fn request(&self, method: &str, params: Value) -> Result<Value> {
        let reply = self.rpc(method, params, None).await?;
        flatten_rpc_reply(method, reply)
    }

    async fn request_detailed(
        &self,
        method: &str,
        params: Value,
    ) -> Result<std::result::Result<Value, McpRpcError>> {
        self.rpc(method, params, None).await
    }

    async fn request_with_meta(
        &self,
        method: &str,
        params: Value,
        meta: McpRequestMeta,
    ) -> Result<Value> {
        let reply = self.rpc(method, params, Some(meta)).await?;
        flatten_rpc_reply(method, reply)
    }

    async fn notify(&self, method: &str, params: Value) -> Result<()> {
        self.post(json!({ "jsonrpc": "2.0", "method": method, "params": params }))
            .await?;
        Ok(())
    }
}

/// A [`ToolExecutor`] backed by one or more initialized [`McpClient`]s. A tool call is routed to
/// the first client that advertises it, so MCP tools are governed exactly like native tools.
pub struct McpToolExecutor {
    clients: Vec<Arc<McpClient>>,
}

impl McpToolExecutor {
    pub fn new(clients: Vec<Arc<McpClient>>) -> Self {
        McpToolExecutor { clients }
    }

    /// The union of all advertised tool specs (for advertising to the model).
    pub fn tool_specs(&self) -> Vec<ToolSpec> {
        self.clients
            .iter()
            .flat_map(|c| c.tools().iter().cloned())
            .collect()
    }
}

#[async_trait]
impl ToolExecutor for McpToolExecutor {
    async fn execute(&self, name: &str, input: Value) -> Result<String> {
        for client in &self.clients {
            if client.provides(name) {
                return client.call_tool(name, input).await;
            }
        }
        Err(AikitError::ToolExecution(format!(
            "no connected MCP server provides tool '{name}'"
        )))
    }
}

// ---------------------------------------------------------------------------------------------
// Stdio transport (production): spawn an MCP server subprocess and speak newline-delimited JSON-RPC
// ---------------------------------------------------------------------------------------------

/// Why a pending stdio request did not resolve with a `result`.
#[derive(Debug, Clone)]
enum StdioFailure {
    /// The server replied with a JSON-RPC `error` object.
    Rpc(McpRpcError),
    /// The reader stopped (EOF, oversized frame, I/O failure) before or instead of a reply.
    Stopped(String),
}

impl StdioFailure {
    fn message(&self) -> &str {
        match self {
            StdioFailure::Rpc(rpc) => &rpc.message,
            StdioFailure::Stopped(message) => message,
        }
    }
}

type PendingSender = oneshot::Sender<std::result::Result<Value, StdioFailure>>;

#[derive(Default)]
struct StdioReaderState {
    pending: HashMap<u64, PendingSender>,
    stopped: Option<String>,
}

type Pending = Arc<Mutex<StdioReaderState>>;

async fn read_bounded_stdio_line<R>(reader: &mut R, line: &mut Vec<u8>) -> io::Result<bool>
where
    R: tokio::io::AsyncBufRead + Unpin,
{
    use tokio::io::AsyncBufReadExt;

    line.clear();
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            return Ok(!line.is_empty());
        }

        let newline = available.iter().position(|byte| *byte == b'\n');
        let payload_bytes = newline.unwrap_or(available.len());
        if line.len().saturating_add(payload_bytes) > MAX_MCP_TRANSPORT_RESPONSE_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "MCP stdio response exceeded 4 MiB",
            ));
        }
        line.extend_from_slice(&available[..payload_bytes]);
        let consumed = payload_bytes + usize::from(newline.is_some());
        reader.consume(consumed);
        if newline.is_some() {
            return Ok(true);
        }
    }
}

async fn stop_stdio_reader(state: &Pending, message: String) {
    let mut state = state.lock().await;
    if state.stopped.is_none() {
        state.stopped = Some(message.clone());
    }
    for (_, sender) in state.pending.drain() {
        let _ = sender.send(Err(StdioFailure::Stopped(message.clone())));
    }
}

async fn run_stdio_reader<R>(stdout: R, state: Pending)
where
    R: tokio::io::AsyncRead + Unpin,
{
    use tokio::io::BufReader;

    let mut reader = BufReader::new(stdout);
    let mut line = Vec::new();
    loop {
        match read_bounded_stdio_line(&mut reader, &mut line).await {
            Ok(true) => {}
            Ok(false) => {
                stop_stdio_reader(&state, "MCP server closed the connection".into()).await;
                return;
            }
            Err(error) => {
                stop_stdio_reader(
                    &state,
                    format!("MCP server response reader failed: {error}"),
                )
                .await;
                return;
            }
        }

        let Ok(message) = serde_json::from_slice::<Value>(&line) else {
            // Preserve the established behaviour: malformed lines and notifications are ignored.
            continue;
        };
        let Some(id) = message.get("id").and_then(Value::as_u64) else {
            continue;
        };
        let outcome = if let Some(error) = message.get("error") {
            Err(StdioFailure::Rpc(McpRpcError {
                code: error.get("code").and_then(Value::as_i64),
                message: error
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("MCP error")
                    .to_string(),
            }))
        } else {
            Ok(message.get("result").cloned().unwrap_or(Value::Null))
        };
        if let Some(sender) = state.lock().await.pending.remove(&id) {
            let _ = sender.send(outcome);
        }
    }
}

/// Speaks JSON-RPC to an MCP server over a spawned subprocess's stdio. A background task reads
/// replies and resolves the matching pending request by `id`.
pub struct StdioTransport {
    stdin: Mutex<tokio::process::ChildStdin>,
    pending: Pending,
    next_id: AtomicU64,
    // Keep the child alive for the transport's lifetime; killed on drop.
    _child: Mutex<tokio::process::Child>,
}

impl StdioTransport {
    /// Spawn `program args...` as an MCP server and start the reply reader.
    pub async fn spawn<S: AsRef<std::ffi::OsStr>>(program: S, args: &[S]) -> Result<Self> {
        let mut command = tokio::process::Command::new(program);
        command.args(args);
        Self::spawn_command(command).await
    }

    /// Spawn with an explicit environment. `inherit_env=false` is the safest default for MCP
    /// servers that should receive only deliberately selected credentials.
    pub async fn spawn_with_env(
        program: &str,
        args: &[String],
        env: &BTreeMap<String, String>,
        inherit_env: bool,
    ) -> Result<Self> {
        let mut command = tokio::process::Command::new(program);
        command.args(args);
        if !inherit_env {
            command.env_clear();
        }
        command.envs(env);
        Self::spawn_command(command).await
    }

    async fn spawn_command(mut command: tokio::process::Command) -> Result<Self> {
        use std::process::Stdio;
        let mut child = command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| AikitError::ToolExecution(format!("failed to spawn MCP server: {e}")))?;

        let stdin = child.stdin.take().expect("piped stdin");
        let stdout = child.stdout.take().expect("piped stdout");
        let pending: Pending = Arc::new(Mutex::new(StdioReaderState::default()));

        // Reader task: one JSON object per line; resolve pending requests by id.
        let reader_pending = pending.clone();
        tokio::spawn(run_stdio_reader(stdout, reader_pending));

        Ok(StdioTransport {
            stdin: Mutex::new(stdin),
            pending,
            next_id: AtomicU64::new(1),
            _child: Mutex::new(child),
        })
    }

    async fn write_line(&self, value: &Value) -> Result<()> {
        use tokio::io::AsyncWriteExt;
        let mut line =
            serde_json::to_string(value).map_err(|e| AikitError::ToolExecution(e.to_string()))?;
        line.push('\n');
        let mut stdin = self.stdin.lock().await;
        stdin
            .write_all(line.as_bytes())
            .await
            .map_err(|e| AikitError::ToolExecution(format!("MCP write failed: {e}")))?;
        stdin
            .flush()
            .await
            .map_err(|e| AikitError::ToolExecution(format!("MCP flush failed: {e}")))?;
        Ok(())
    }
}

impl StdioTransport {
    async fn rpc(
        &self,
        method: &str,
        params: Value,
    ) -> Result<std::result::Result<Value, StdioFailure>> {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = oneshot::channel();
        {
            let mut state = self.pending.lock().await;
            if let Some(error) = &state.stopped {
                return Err(AikitError::ToolExecution(format!(
                    "MCP '{method}' unavailable: {error}"
                )));
            }
            state.pending.insert(id, tx);
        }
        let payload = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
        if let Err(e) = self.write_line(&payload).await {
            self.pending.lock().await.pending.remove(&id);
            return Err(e);
        }
        rx.await.map_err(|_| {
            AikitError::ToolExecution(format!("MCP '{method}' response channel dropped"))
        })
    }
}

#[async_trait]
impl McpTransport for StdioTransport {
    async fn request(&self, method: &str, params: Value) -> Result<Value> {
        match self.rpc(method, params).await? {
            Ok(value) => Ok(value),
            Err(failure) => Err(AikitError::ToolExecution(format!(
                "MCP '{method}' failed: {}",
                failure.message()
            ))),
        }
    }

    async fn request_detailed(
        &self,
        method: &str,
        params: Value,
    ) -> Result<std::result::Result<Value, McpRpcError>> {
        match self.rpc(method, params).await? {
            Ok(value) => Ok(Ok(value)),
            // A JSON-RPC error reply carries protocol meaning (e.g. Auto fallback); a stopped
            // reader is a transport failure and must not be mistaken for one.
            Err(StdioFailure::Rpc(rpc)) => Ok(Err(rpc)),
            Err(StdioFailure::Stopped(message)) => Err(AikitError::ToolExecution(format!(
                "MCP '{method}' failed: {message}"
            ))),
        }
    }

    async fn notify(&self, method: &str, params: Value) -> Result<()> {
        if let Some(error) = self.pending.lock().await.stopped.clone() {
            return Err(AikitError::ToolExecution(format!(
                "MCP '{method}' unavailable: {error}"
            )));
        }
        let payload = json!({ "jsonrpc": "2.0", "method": method, "params": params });
        self.write_line(&payload).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    /// An in-memory transport that answers `initialize`, `tools/list`, and `tools/call` with canned
    /// data, and records the notifications it received — enough to verify the client's protocol.
    struct MockTransport {
        notified: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl McpTransport for MockTransport {
        async fn request(&self, method: &str, params: Value) -> Result<Value> {
            match method {
                "initialize" => Ok(json!({
                    "protocolVersion": MCP_PROTOCOL_VERSION,
                    "serverInfo": { "name": "mock-mcp", "version": "0.1" }
                })),
                "tools/list" => Ok(json!({
                    "tools": [
                        {
                            "name": "get_weather",
                            "description": "Get the weather",
                            "inputSchema": { "type": "object", "properties": { "city": { "type": "string" } } }
                        }
                    ]
                })),
                "tools/call" => {
                    let name = params.get("name").and_then(Value::as_str).unwrap_or("");
                    if name == "get_weather" {
                        let city = params
                            .get("arguments")
                            .and_then(|a| a.get("city"))
                            .and_then(Value::as_str)
                            .unwrap_or("?");
                        Ok(
                            json!({ "content": [ { "type": "text", "text": format!("sunny in {city}") } ] }),
                        )
                    } else {
                        Ok(
                            json!({ "isError": true, "content": [ { "type": "text", "text": "unknown tool" } ] }),
                        )
                    }
                }
                "resources/list" => Ok(
                    json!({"resources":[{"uri":"file:///guide","name":"Guide","mimeType":"text/plain"}]}),
                ),
                "resources/read" => Ok(json!({"contents":[{"uri":params["uri"],"text":"hello"}]})),
                "prompts/list" => Ok(
                    json!({"prompts":[{"name":"review","description":"Review code","arguments":[]}]}),
                ),
                "prompts/get" => Ok(
                    json!({"description":"Review code","messages":[{"role":"user","content":{"type":"text","text":"review this"}}]}),
                ),
                other => Err(AikitError::ToolExecution(format!(
                    "unexpected method {other}"
                ))),
            }
        }

        async fn notify(&self, method: &str, _params: Value) -> Result<()> {
            self.notified.lock().await.push(method.to_string());
            Ok(())
        }
    }

    fn client() -> McpClient {
        McpClient::new(
            Arc::new(MockTransport {
                notified: Mutex::new(Vec::new()),
            }),
            "mock",
        )
    }

    struct FilterTransport {
        tool_calls: AtomicUsize,
    }

    #[async_trait]
    impl McpTransport for FilterTransport {
        async fn request(&self, method: &str, _params: Value) -> Result<Value> {
            match method {
                "initialize" => Ok(json!({
                    "protocolVersion": MCP_PROTOCOL_VERSION,
                    "serverInfo": { "name": "filter-mock", "version": "0.1" }
                })),
                "tools/list" => Ok(json!({
                    "tools": [
                        { "name": "get_weather", "description": "exact", "inputSchema": { "type": "object" } },
                        { "name": "get_weather_v2", "description": "prefix", "inputSchema": { "type": "object" } },
                        { "name": "Get_Weather", "description": "different case", "inputSchema": { "type": "object" } },
                        { "name": "delete_user", "description": "denied", "inputSchema": { "type": "object" } }
                    ]
                })),
                "tools/call" => {
                    self.tool_calls.fetch_add(1, Ordering::SeqCst);
                    Ok(json!({ "content": [{ "type": "text", "text": "called" }] }))
                }
                other => Err(AikitError::ToolExecution(format!(
                    "unexpected method {other}"
                ))),
            }
        }

        async fn notify(&self, _method: &str, _params: Value) -> Result<()> {
            Ok(())
        }
    }

    type DiscoveryHandler = dyn Fn(&str, Value, usize) -> Result<Value> + Send + Sync;

    struct DiscoveryTransport {
        discovery_calls: AtomicUsize,
        handler: Box<DiscoveryHandler>,
    }

    impl DiscoveryTransport {
        fn new(
            handler: impl Fn(&str, Value, usize) -> Result<Value> + Send + Sync + 'static,
        ) -> Self {
            Self {
                discovery_calls: AtomicUsize::new(0),
                handler: Box::new(handler),
            }
        }
    }

    #[async_trait]
    impl McpTransport for DiscoveryTransport {
        async fn request(&self, method: &str, params: Value) -> Result<Value> {
            if method == "initialize" {
                return Ok(json!({
                    "protocolVersion": MCP_PROTOCOL_VERSION,
                    "serverInfo": { "name": "discovery-mock", "version": "0.1" }
                }));
            }
            let call = self.discovery_calls.fetch_add(1, Ordering::SeqCst) + 1;
            (self.handler)(method, params, call)
        }

        async fn notify(&self, _method: &str, _params: Value) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn initialize_handshake_sends_initialized_notification() {
        let transport = Arc::new(MockTransport {
            notified: Mutex::new(Vec::new()),
        });
        let c = McpClient::new(transport.clone(), "mock");
        let info = c.initialize().await.unwrap();
        assert_eq!(info["serverInfo"]["name"], "mock-mcp");
        assert_eq!(
            transport.notified.lock().await.as_slice(),
            ["notifications/initialized"]
        );
    }

    #[tokio::test]
    async fn discovers_tools_as_specs() {
        let mut c = client();
        c.initialize().await.unwrap();
        let specs = c.list_tools().await.unwrap();
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].name, "get_weather");
        assert_eq!(
            specs[0].input_schema["properties"]["city"]["type"],
            "string"
        );
        assert!(c.provides("get_weather"));
    }

    #[tokio::test]
    async fn calls_a_tool_and_flattens_content() {
        let mut c = client();
        c.initialize().await.unwrap();
        c.list_tools().await.unwrap();
        let out = c
            .call_tool("get_weather", json!({ "city": "İstanbul" }))
            .await
            .unwrap();
        assert_eq!(out, "sunny in İstanbul");
    }

    #[tokio::test]
    async fn tool_error_becomes_a_tool_execution_error() {
        let c = client();
        c.initialize().await.unwrap();
        let err = c.call_tool("nope", json!({})).await.unwrap_err();
        assert!(matches!(err, AikitError::ToolExecution(_)));
    }

    #[tokio::test]
    async fn executor_routes_to_the_advertising_client() {
        let mut c = client();
        c.initialize().await.unwrap();
        c.list_tools().await.unwrap();
        let exec = McpToolExecutor::new(vec![Arc::new(c)]);
        assert_eq!(exec.tool_specs().len(), 1);
        // A known tool routes to the MCP client...
        let out = exec
            .execute("get_weather", json!({ "city": "Ankara" }))
            .await
            .unwrap();
        assert_eq!(out, "sunny in Ankara");
        // ...an unknown tool is a clear error, not a panic.
        assert!(exec.execute("unknown", json!({})).await.is_err());
    }

    #[tokio::test]
    async fn visibility_filter_is_exact_and_deny_wins_before_cache() {
        let transport = Arc::new(FilterTransport {
            tool_calls: AtomicUsize::new(0),
        });
        let filter = McpToolFilter::new(
            Some(vec![
                "get_weather".into(),
                "delete_user".into(),
                "not_advertised".into(),
            ]),
            vec!["delete_user".into()],
        )
        .unwrap();
        let mut client = McpClient::new_with_tool_filter(transport.clone(), "filtered", filter);
        client.initialize().await.unwrap();

        let specs = client.list_tools().await.unwrap();
        assert_eq!(
            specs
                .iter()
                .map(|spec| spec.name.as_str())
                .collect::<Vec<_>>(),
            ["get_weather"]
        );
        assert!(client.provides("get_weather"));
        assert!(!client.provides("get_weather_v2"));
        assert!(!client.provides("Get_Weather"));
        assert!(!client.provides("delete_user"));
        assert!(!client.provides("not_advertised"));

        let client = Arc::new(client);
        assert!(client.call_tool("delete_user", json!({})).await.is_err());
        assert!(client.call_tool("not_advertised", json!({})).await.is_err());
        assert_eq!(transport.tool_calls.load(Ordering::SeqCst), 0);
        let executor = McpToolExecutor::new(vec![client]);
        assert!(executor.execute("delete_user", json!({})).await.is_err());
        assert!(executor.execute("not_advertised", json!({})).await.is_err());
        assert_eq!(transport.tool_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn unique_cursors_cannot_create_unbounded_tool_discovery() {
        let transport = Arc::new(DiscoveryTransport::new(|method, _params, call| {
            assert_eq!(method, "tools/list");
            Ok(json!({
                "tools": [],
                "nextCursor": format!("unique-cursor-{call}")
            }))
        }));
        let mut client = McpClient::new(transport.clone(), "endless");
        client.initialize().await.unwrap();

        let error = client.list_tools().await.unwrap_err();
        assert!(matches!(error, AikitError::ToolExecution(_)));
        assert!(error.to_string().contains("exceeded 128 pages"));
        assert_eq!(
            transport.discovery_calls.load(Ordering::SeqCst),
            MAX_MCP_DISCOVERY_PAGES
        );
    }

    #[tokio::test]
    async fn empty_allow_filter_still_counts_every_incoming_tool() {
        let transport = Arc::new(DiscoveryTransport::new(|method, _params, _call| {
            assert_eq!(method, "tools/list");
            let tools = (0..=MAX_MCP_DISCOVERY_ITEMS)
                .map(|index| json!({ "name": format!("hidden_{index}") }))
                .collect::<Vec<_>>();
            Ok(json!({ "tools": tools }))
        }));
        let filter = McpToolFilter::new(Some(Vec::new()), Vec::new()).unwrap();
        let mut client = McpClient::new_with_tool_filter(transport.clone(), "hidden", filter);
        client.initialize().await.unwrap();

        let error = client.list_tools().await.unwrap_err();
        assert!(matches!(error, AikitError::ToolExecution(_)));
        assert!(error.to_string().contains("exceeded 10000 items"));
        assert!(client.tools().is_empty());
        assert_eq!(transport.discovery_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn failed_tool_refresh_clears_the_previous_visibility_cache() {
        let transport = Arc::new(DiscoveryTransport::new(|method, _params, call| {
            assert_eq!(method, "tools/list");
            if call == 1 {
                return Ok(json!({
                    "tools": [{ "name": "safe", "inputSchema": { "type": "object" } }]
                }));
            }
            let tools = (0..=MAX_MCP_DISCOVERY_ITEMS)
                .map(|index| json!({ "name": format!("tool_{index}") }))
                .collect::<Vec<_>>();
            Ok(json!({ "tools": tools }))
        }));
        let filter = McpToolFilter::new(Some(vec!["safe".into()]), Vec::new()).unwrap();
        let mut client =
            McpClient::new_with_tool_filter(transport.clone(), "refresh-cache", filter);
        client.initialize().await.unwrap();
        assert_eq!(client.list_tools().await.unwrap().len(), 1);
        assert!(client.provides("safe"));

        assert!(client.list_tools().await.is_err());
        assert!(!client.provides("safe"));
        assert!(client.call_tool("safe", json!({})).await.is_err());
        assert_eq!(transport.discovery_calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn resources_and_prompts_enforce_item_budgets() {
        let transport = Arc::new(DiscoveryTransport::new(|method, _params, _call| {
            let items = vec![json!({}); MAX_MCP_DISCOVERY_ITEMS + 1];
            match method {
                "resources/list" => Ok(json!({ "resources": items })),
                "prompts/list" => Ok(json!({ "prompts": items })),
                other => Err(AikitError::ToolExecution(format!(
                    "unexpected method {other}"
                ))),
            }
        }));
        let client = McpClient::new(transport.clone(), "bounded-collections");
        client.initialize().await.unwrap();

        let resources_error = client.list_resources(None).await.unwrap_err();
        let prompts_error = client.list_prompts(None).await.unwrap_err();
        assert!(matches!(resources_error, AikitError::ToolExecution(_)));
        assert!(matches!(prompts_error, AikitError::ToolExecution(_)));
        assert!(resources_error.to_string().contains("exceeded 10000 items"));
        assert!(prompts_error.to_string().contains("exceeded 10000 items"));
        assert_eq!(transport.discovery_calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn discovery_rejects_oversized_cursors() {
        let transport = Arc::new(DiscoveryTransport::new(|method, _params, _call| {
            assert_eq!(method, "tools/list");
            Ok(json!({
                "tools": [],
                "nextCursor": "x".repeat(MAX_MCP_CURSOR_BYTES + 1)
            }))
        }));
        let mut client = McpClient::new(transport, "large-cursor");
        client.initialize().await.unwrap();

        let error = client.list_tools().await.unwrap_err();
        assert!(matches!(error, AikitError::ToolExecution(_)));
        assert!(error.to_string().contains("cursor exceeded 4096 bytes"));
    }

    #[tokio::test]
    async fn malformed_and_duplicate_tool_entries_are_hidden_deterministically() {
        let transport = Arc::new(DiscoveryTransport::new(|method, _params, _call| {
            assert_eq!(method, "tools/list");
            Ok(json!({
                "tools": [
                    null,
                    { "name": "" },
                    { "name": "bad\nname" },
                    { "name": "bidi\u{202e}name" },
                    { "name": "mark\u{061c}name" },
                    { "name": "x".repeat(MAX_MCP_TOOL_NAME_CHARS + 1) },
                    { "name": "safe", "description": "first" },
                    { "name": "safe", "description": "second" }
                ]
            }))
        }));
        let mut client = McpClient::new(transport, "malformed-tools");
        client.initialize().await.unwrap();

        let tools = client.list_tools().await.unwrap();
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0].name, "safe");
        assert_eq!(tools[0].description, "first");
    }

    #[test]
    fn visibility_filter_validates_bounds_and_duplicates() {
        let empty_allow = McpToolFilter::new(Some(Vec::new()), Vec::new()).unwrap();
        assert!(!empty_allow.allows("anything"));

        let deny_only = McpToolFilter::new(None, vec!["delete_user".into()]).unwrap();
        assert!(deny_only.allows("get_weather"));
        assert!(!deny_only.allows("delete_user"));

        assert!(McpToolFilter::new(Some(vec![String::new()]), Vec::new()).is_err());
        assert!(McpToolFilter::new(None, vec!["   ".into()]).is_err());
        assert!(McpToolFilter::new(Some(vec!["x".repeat(129)]), Vec::new()).is_err());
        assert!(McpToolFilter::new(Some(vec!["a".into(), "a".into()]), Vec::new()).is_err());
        assert!(McpToolFilter::new(None, vec!["a".into(), "a".into()]).is_err());
        assert!(McpToolFilter::new(Some(vec!["a\n".into()]), Vec::new()).is_err());
        for unsafe_character in [
            '\u{061c}', '\u{200e}', '\u{200f}', '\u{202a}', '\u{202e}', '\u{2066}', '\u{2069}',
            '\u{206a}', '\u{206f}',
        ] {
            assert!(McpToolFilter::new(
                Some(vec![format!("safe{unsafe_character}spoof")]),
                Vec::new()
            )
            .is_err());
        }

        // The same name may appear once in each set because deny is explicitly authoritative.
        let overlap = McpToolFilter::new(Some(vec!["same".into()]), vec!["same".into()]).unwrap();
        assert!(!overlap.allows("same"));

        let too_many = (0..=MAX_MCP_TOOL_FILTER_NAMES)
            .map(|index| format!("tool_{index}"))
            .collect();
        assert!(McpToolFilter::new(Some(too_many), Vec::new()).is_err());

        let unknown_field = format!("secret-{}", "x".repeat(4_096));
        let mut unknown_filter = serde_json::Map::new();
        unknown_filter.insert(unknown_field.clone(), json!([]));
        let error = McpToolFilter::from_value(Value::Object(unknown_filter)).unwrap_err();
        assert!(error.to_string().contains("unknown field"));
        assert!(!error.to_string().contains(&unknown_field));
        assert!(McpToolFilter::from_value(json!({ "allow": null })).is_err());
        assert!(McpToolFilter::from_value(json!({ "deny": [1] })).is_err());
        assert!(McpToolFilter::from_value(json!([])).is_err());
        assert!(McpToolFilter::from_value(json!({
            "allow": (0..=MAX_MCP_TOOL_FILTER_NAMES).map(|index| format!("tool_{index}")).collect::<Vec<_>>()
        }))
        .is_err());
    }

    #[tokio::test]
    async fn resources_and_prompts_use_the_initialized_lifecycle() {
        let c = client();
        assert!(c.list_resources(None).await.is_err());
        c.initialize().await.unwrap();
        let resources = c.list_resources(None).await.unwrap();
        assert_eq!(resources[0].uri, "file:///guide");
        assert_eq!(
            c.read_resource("file:///guide").await.unwrap()["contents"][0]["text"],
            "hello"
        );
        let prompts = c.list_prompts(None).await.unwrap();
        assert_eq!(prompts[0].name, "review");
        assert!(c.get_prompt("review", json!({})).await.unwrap()["messages"].is_array());
    }

    #[tokio::test]
    async fn stdio_reader_rejects_an_oversized_newline_less_response() {
        use tokio::io::AsyncWriteExt;

        let state: Pending = Arc::new(Mutex::new(StdioReaderState::default()));
        let (first_sender, first_receiver) = oneshot::channel();
        let (second_sender, second_receiver) = oneshot::channel();
        {
            let mut state = state.lock().await;
            state.pending.insert(1, first_sender);
            state.pending.insert(2, second_sender);
        }

        let (mut writer, reader) = tokio::io::duplex(64 << 10);
        let reader_task = tokio::spawn(run_stdio_reader(reader, state.clone()));
        let writer_task = tokio::spawn(async move {
            let oversized = vec![b'x'; MAX_MCP_TRANSPORT_RESPONSE_BYTES + 1];
            let _ = writer.write_all(&oversized).await;
        });

        tokio::time::timeout(std::time::Duration::from_secs(5), reader_task)
            .await
            .expect("bounded reader must terminate")
            .unwrap();
        writer_task.await.unwrap();

        for receiver in [first_receiver, second_receiver] {
            let error = receiver.await.unwrap().unwrap_err();
            // Reader shutdown is a transport failure, not a JSON-RPC error reply.
            assert!(matches!(&error, StdioFailure::Stopped(_)));
            assert!(error.message().contains("exceeded 4 MiB"));
        }
        let state = state.lock().await;
        assert!(state.pending.is_empty());
        assert!(state
            .stopped
            .as_deref()
            .is_some_and(|error| error.contains("exceeded 4 MiB")));
    }

    #[tokio::test]
    async fn streamable_http_sends_auth_and_tracks_session() {
        use wiremock::matchers::{header, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/mcp"))
            .and(header("authorization", "Bearer secret-token"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "text/event-stream")
                    .insert_header("Mcp-Session-Id", "session-1")
                    .set_body_raw(
                        "event: message\ndata: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"ok\":true}}\n\n",
                        "text/event-stream",
                    ),
            )
            .mount(&server)
            .await;
        let transport = StreamableHttpTransport::new(
            &format!("{}/mcp", server.uri()),
            Some("secret-token".into()),
        )
        .unwrap();
        assert_eq!(
            transport.request("ping", json!({})).await.unwrap()["ok"],
            true
        );
        assert_eq!(transport.session_id().await.as_deref(), Some("session-1"));
    }

    // -----------------------------------------------------------------------------------------
    // 2026-07-28 dual-version tests
    // -----------------------------------------------------------------------------------------

    /// A 2026-07-28 server: rejects `initialize` with method-not-found, answers
    /// `server/discover`, and asserts the `_meta` client-identity triplet on every other call.
    struct Rc2026MockTransport {
        /// Rounds of `inputRequired` to demand before `tools/call` succeeds.
        input_rounds: usize,
        seen_calls: Mutex<Vec<Value>>,
        notified: Mutex<Vec<String>>,
    }

    impl Rc2026MockTransport {
        fn new(input_rounds: usize) -> Self {
            Rc2026MockTransport {
                input_rounds,
                seen_calls: Mutex::new(Vec::new()),
                notified: Mutex::new(Vec::new()),
            }
        }

        fn assert_meta(params: &Value) {
            let meta = params
                .get("_meta")
                .and_then(Value::as_object)
                .expect("2026-07-28 request must carry _meta");
            assert_eq!(
                meta.get(META_PROTOCOL_VERSION_KEY).and_then(Value::as_str),
                Some(MCP_PROTOCOL_VERSION_2026)
            );
            assert_eq!(
                meta.get(META_CLIENT_INFO_KEY)
                    .and_then(|info| info.get("name"))
                    .and_then(Value::as_str),
                Some("aikit")
            );
            assert!(meta.get(META_CLIENT_CAPABILITIES_KEY).is_some());
        }
    }

    #[async_trait]
    impl McpTransport for Rc2026MockTransport {
        async fn request(&self, method: &str, params: Value) -> Result<Value> {
            match self.request_detailed(method, params).await? {
                Ok(value) => Ok(value),
                Err(rpc) => Err(AikitError::ToolExecution(format!(
                    "MCP '{method}' failed: {}",
                    rpc.message
                ))),
            }
        }

        async fn request_detailed(
            &self,
            method: &str,
            params: Value,
        ) -> Result<std::result::Result<Value, McpRpcError>> {
            match method {
                "initialize" => Ok(Err(McpRpcError {
                    code: Some(-32601),
                    message: "method not found".into(),
                })),
                "server/discover" => Ok(Ok(json!({
                    "protocolVersions": [MCP_PROTOCOL_VERSION_2026],
                    "serverInfo": { "name": "mock-2026", "version": "0.1" },
                    "capabilities": {}
                }))),
                "tools/list" => {
                    Self::assert_meta(&params);
                    Ok(Ok(json!({
                        "tools": [{
                            "name": "get_weather",
                            "description": "Get the weather",
                            "inputSchema": { "type": "object" }
                        }]
                    })))
                }
                "tools/call" => {
                    Self::assert_meta(&params);
                    let rounds_seen = {
                        let mut calls = self.seen_calls.lock().await;
                        calls.push(params.clone());
                        calls
                            .iter()
                            .filter(|call| call.get("requestState").is_some())
                            .count()
                    };
                    if rounds_seen < self.input_rounds {
                        Ok(Ok(json!({
                            "resultType": "inputRequired",
                            "requestState": "opaque-state-blob",
                            "inputRequests": { "confirm": { "type": "string" } }
                        })))
                    } else {
                        Ok(Ok(json!({
                            "content": [{ "type": "text", "text": "sunny" }]
                        })))
                    }
                }
                other => Err(AikitError::ToolExecution(format!(
                    "unexpected method {other}"
                ))),
            }
        }

        async fn notify(&self, method: &str, _params: Value) -> Result<()> {
            self.notified.lock().await.push(method.to_string());
            Ok(())
        }
    }

    struct CountingInputHandler {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl McpInputHandler for CountingInputHandler {
        async fn provide(
            &self,
            _server: &str,
            _tool: &str,
            input_requests: Value,
        ) -> Result<Value> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            assert!(input_requests.get("confirm").is_some());
            Ok(json!({ "confirm": "yes" }))
        }
    }

    fn rc2026_client(transport: Arc<Rc2026MockTransport>) -> McpClient {
        McpClient::new(transport, "mock-2026")
            .with_protocol_version(McpProtocolVersion::V2026_07_28)
    }

    #[tokio::test]
    async fn legacy_accepts_every_handshake_revision_it_shares_a_lifecycle_with() {
        struct VersionMock(&'static str);
        #[async_trait]
        impl McpTransport for VersionMock {
            async fn request(&self, method: &str, _params: Value) -> Result<Value> {
                assert_eq!(method, "initialize");
                Ok(json!({ "protocolVersion": self.0 }))
            }
            async fn notify(&self, _method: &str, _params: Value) -> Result<()> {
                Ok(())
            }
        }
        for version in LEGACY_ACCEPTED_VERSIONS {
            let client = McpClient::new(Arc::new(VersionMock(version)), "legacy");
            assert!(
                client.initialize().await.is_ok(),
                "{version} must be accepted"
            );
        }
        let client = McpClient::new(Arc::new(VersionMock("2099-01-01")), "legacy");
        let error = client.initialize().await.unwrap_err();
        assert!(error.to_string().contains("unsupported protocol version"));
    }

    #[tokio::test]
    async fn explicit_2026_startup_discovers_and_decorates_every_request() {
        let transport = Arc::new(Rc2026MockTransport::new(0));
        let mut client = rc2026_client(transport.clone());
        let discover = client.initialize().await.unwrap();
        assert_eq!(discover["serverInfo"]["name"], "mock-2026");
        // The stateless revision has no initialized notification.
        assert!(transport.notified.lock().await.is_empty());
        // Every post-startup request carries the _meta triplet (asserted inside the mock).
        let tools = client.list_tools().await.unwrap();
        assert_eq!(tools[0].name, "get_weather");
        let body = client.call_tool("get_weather", json!({})).await.unwrap();
        assert_eq!(body, "sunny");
    }

    #[tokio::test]
    async fn auto_selects_2026_when_the_server_discovers() {
        let transport = Arc::new(Rc2026MockTransport::new(0));
        let client = McpClient::new(transport.clone(), "auto")
            .with_protocol_version(McpProtocolVersion::Auto);
        client.initialize().await.unwrap();
        assert!(transport.notified.lock().await.is_empty());
    }

    #[tokio::test]
    async fn auto_falls_back_to_legacy_on_a_json_rpc_refusal() {
        struct LegacyOnlyMock {
            notified: Mutex<Vec<String>>,
        }
        #[async_trait]
        impl McpTransport for LegacyOnlyMock {
            async fn request(&self, method: &str, params: Value) -> Result<Value> {
                match self.request_detailed(method, params).await? {
                    Ok(value) => Ok(value),
                    Err(rpc) => Err(AikitError::ToolExecution(rpc.message)),
                }
            }
            async fn request_detailed(
                &self,
                method: &str,
                params: Value,
            ) -> Result<std::result::Result<Value, McpRpcError>> {
                match method {
                    "server/discover" => Ok(Err(McpRpcError {
                        code: Some(-32601),
                        message: "method not found".into(),
                    })),
                    "initialize" => Ok(Ok(json!({ "protocolVersion": MCP_PROTOCOL_VERSION }))),
                    "tools/list" => {
                        // A legacy connection must NOT be decorated with 2026 _meta.
                        assert!(params.get("_meta").is_none());
                        Ok(Ok(json!({ "tools": [] })))
                    }
                    other => Err(AikitError::ToolExecution(format!("unexpected {other}"))),
                }
            }
            async fn notify(&self, method: &str, _params: Value) -> Result<()> {
                self.notified.lock().await.push(method.to_string());
                Ok(())
            }
        }
        let transport = Arc::new(LegacyOnlyMock {
            notified: Mutex::new(Vec::new()),
        });
        let mut client = McpClient::new(transport.clone(), "auto-legacy")
            .with_protocol_version(McpProtocolVersion::Auto);
        client.initialize().await.unwrap();
        assert_eq!(
            transport.notified.lock().await.as_slice(),
            ["notifications/initialized"]
        );
        client.list_tools().await.unwrap();
    }

    #[tokio::test]
    async fn auto_falls_back_when_discover_lacks_the_2026_revision() {
        struct OldDiscoverMock;
        #[async_trait]
        impl McpTransport for OldDiscoverMock {
            async fn request(&self, method: &str, _params: Value) -> Result<Value> {
                assert_eq!(method, "initialize");
                Ok(json!({ "protocolVersion": MCP_PROTOCOL_VERSION }))
            }
            async fn request_detailed(
                &self,
                method: &str,
                params: Value,
            ) -> Result<std::result::Result<Value, McpRpcError>> {
                if method == "server/discover" {
                    return Ok(Ok(json!({ "protocolVersions": ["2027-06-06"] })));
                }
                self.request(method, params).await.map(Ok)
            }
            async fn notify(&self, _method: &str, _params: Value) -> Result<()> {
                Ok(())
            }
        }
        let client = McpClient::new(Arc::new(OldDiscoverMock), "auto")
            .with_protocol_version(McpProtocolVersion::Auto);
        assert!(client.initialize().await.is_ok());
    }

    #[tokio::test]
    async fn auto_propagates_a_transport_failure_instead_of_falling_back() {
        struct BrokenTransport;
        #[async_trait]
        impl McpTransport for BrokenTransport {
            async fn request(&self, _method: &str, _params: Value) -> Result<Value> {
                Err(AikitError::ToolExecution("connection refused".into()))
            }
            async fn request_detailed(
                &self,
                _method: &str,
                _params: Value,
            ) -> Result<std::result::Result<Value, McpRpcError>> {
                Err(AikitError::ToolExecution("connection refused".into()))
            }
            async fn notify(&self, _method: &str, _params: Value) -> Result<()> {
                Ok(())
            }
        }
        let client = McpClient::new(Arc::new(BrokenTransport), "auto")
            .with_protocol_version(McpProtocolVersion::Auto);
        let error = client.initialize().await.unwrap_err();
        assert!(error.to_string().contains("connection refused"));
    }

    #[tokio::test]
    async fn explicit_2026_fails_clearly_when_the_server_cannot_discover() {
        struct NoDiscoverMock;
        #[async_trait]
        impl McpTransport for NoDiscoverMock {
            async fn request(&self, _method: &str, _params: Value) -> Result<Value> {
                unreachable!("request_detailed answers everything")
            }
            async fn request_detailed(
                &self,
                _method: &str,
                _params: Value,
            ) -> Result<std::result::Result<Value, McpRpcError>> {
                Ok(Err(McpRpcError {
                    code: Some(-32601),
                    message: "method not found".into(),
                }))
            }
            async fn notify(&self, _method: &str, _params: Value) -> Result<()> {
                Ok(())
            }
        }
        let client = McpClient::new(Arc::new(NoDiscoverMock), "strict-2026")
            .with_protocol_version(McpProtocolVersion::V2026_07_28);
        let error = client.initialize().await.unwrap_err().to_string();
        assert!(error.contains("does not support protocol 2026-07-28"));
    }

    #[tokio::test]
    async fn mrtr_round_trip_echoes_the_request_state_verbatim() {
        let transport = Arc::new(Rc2026MockTransport::new(1));
        let handler = Arc::new(CountingInputHandler {
            calls: AtomicUsize::new(0),
        });
        let mut client = rc2026_client(transport.clone()).with_input_handler(handler.clone());
        client.initialize().await.unwrap();
        client.list_tools().await.unwrap();
        let body = client.call_tool("get_weather", json!({})).await.unwrap();
        assert_eq!(body, "sunny");
        assert_eq!(handler.calls.load(Ordering::SeqCst), 1);
        let calls = transport.seen_calls.lock().await;
        assert_eq!(calls.len(), 2);
        // The re-issued call keeps the original name/arguments, answers the input requests, and
        // echoes the opaque state byte-for-byte.
        assert_eq!(calls[1]["requestState"], "opaque-state-blob");
        assert_eq!(calls[1]["inputResponses"]["confirm"], "yes");
        assert_eq!(calls[1]["name"], "get_weather");
    }

    #[tokio::test]
    async fn mrtr_without_a_handler_fails_closed() {
        let transport = Arc::new(Rc2026MockTransport::new(1));
        let mut client = rc2026_client(transport);
        client.initialize().await.unwrap();
        client.list_tools().await.unwrap();
        let error = client
            .call_tool("get_weather", json!({}))
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("requires additional input"));
        assert!(error.contains("no input handler"));
    }

    #[tokio::test]
    async fn mrtr_round_cap_fails_closed_against_a_looping_server() {
        // The server demands input forever; the client must stop after MAX_MCP_INPUT_ROUNDS.
        let transport = Arc::new(Rc2026MockTransport::new(usize::MAX));
        let handler = Arc::new(CountingInputHandler {
            calls: AtomicUsize::new(0),
        });
        let mut client = rc2026_client(transport).with_input_handler(handler.clone());
        client.initialize().await.unwrap();
        client.list_tools().await.unwrap();
        let error = client
            .call_tool("get_weather", json!({}))
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("exceeded"));
        assert_eq!(handler.calls.load(Ordering::SeqCst), MAX_MCP_INPUT_ROUNDS);
    }

    #[tokio::test]
    async fn mrtr_handler_error_propagates_and_aborts_the_call() {
        struct RefusingHandler;
        #[async_trait]
        impl McpInputHandler for RefusingHandler {
            async fn provide(
                &self,
                _server: &str,
                _tool: &str,
                _input_requests: Value,
            ) -> Result<Value> {
                Err(AikitError::ToolExecution("operator declined".into()))
            }
        }
        let transport = Arc::new(Rc2026MockTransport::new(1));
        let mut client = rc2026_client(transport).with_input_handler(Arc::new(RefusingHandler));
        client.initialize().await.unwrap();
        client.list_tools().await.unwrap();
        let error = client
            .call_tool("get_weather", json!({}))
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("operator declined"));
    }

    #[tokio::test]
    async fn unknown_result_type_is_an_error_not_a_silent_misparse() {
        struct TaskHandleMock;
        #[async_trait]
        impl McpTransport for TaskHandleMock {
            async fn request(&self, method: &str, _params: Value) -> Result<Value> {
                match method {
                    "initialize" => Ok(json!({ "protocolVersion": MCP_PROTOCOL_VERSION })),
                    "tools/call" => Ok(json!({
                        "resultType": "taskHandle",
                        "taskId": "t-1"
                    })),
                    other => Err(AikitError::ToolExecution(format!("unexpected {other}"))),
                }
            }
            async fn notify(&self, _method: &str, _params: Value) -> Result<()> {
                Ok(())
            }
        }
        // This client never declared the tasks extension, so a task handle must be rejected.
        let client = McpClient::new(Arc::new(TaskHandleMock), "tasky");
        client.initialize().await.unwrap();
        let error = client
            .call_tool("anything", json!({}))
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("unrecognized resultType 'taskHandle'"));
    }

    #[tokio::test]
    async fn streamable_http_2026_sends_routing_headers_and_never_a_session() {
        use wiremock::matchers::{header, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/mcp"))
            .and(header("MCP-Protocol-Version", MCP_PROTOCOL_VERSION_2026))
            .and(header("Mcp-Method", "tools/call"))
            .and(header("Mcp-Name", "get_weather"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "application/json")
                    // Even a server that emits a session header must not create one client-side.
                    .insert_header("Mcp-Session-Id", "must-not-stick")
                    .set_body_raw(
                        "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"ok\":true}}",
                        "application/json",
                    ),
            )
            .expect(1)
            .mount(&server)
            .await;
        let transport =
            StreamableHttpTransport::new(&format!("{}/mcp", server.uri()), None).unwrap();
        let result = transport
            .request_with_meta(
                "tools/call",
                json!({ "name": "get_weather" }),
                McpRequestMeta {
                    protocol_version: MCP_PROTOCOL_VERSION_2026,
                    method: "tools/call".into(),
                    name: Some("get_weather".into()),
                },
            )
            .await
            .unwrap();
        assert_eq!(result["ok"], true);
        assert_eq!(transport.session_id().await, None);
    }
}
