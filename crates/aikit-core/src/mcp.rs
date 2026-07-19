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

/// The MCP revision this client advertises in the `initialize` handshake.
pub const MCP_PROTOCOL_VERSION: &str = "2025-06-18";

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
}

/// An MCP client over some [`McpTransport`]. Discovered tools are cached as [`ToolSpec`]s.
pub struct McpClient {
    transport: Arc<dyn McpTransport>,
    server: String,
    tool_filter: McpToolFilter,
    tools: Vec<ToolSpec>,
    initialized: AtomicBool,
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
        }
    }

    pub fn server_name(&self) -> &str {
        &self.server
    }

    /// Perform the `initialize` handshake and send `notifications/initialized`. Returns the raw
    /// server info/capabilities object.
    pub async fn initialize(&self) -> Result<Value> {
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
        if version != MCP_PROTOCOL_VERSION {
            return Err(AikitError::ToolExecution(format!(
                "MCP server selected unsupported protocol version '{version}'"
            )));
        }
        self.transport
            .notify("notifications/initialized", json!({}))
            .await?;
        self.initialized.store(true, Ordering::Release);
        Ok(result)
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
            let result = self.transport.request(METHOD, params).await?;
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
        let result = self
            .transport
            .request(
                "tools/call",
                json!({ "name": name, "arguments": arguments }),
            )
            .await?;
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
            let result = self.transport.request(METHOD, params).await?;
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
        self.transport
            .request("resources/read", json!({ "uri": uri }))
            .await
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
            let result = self.transport.request(METHOD, params).await?;
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
        self.transport
            .request(
                "prompts/get",
                json!({ "name": name, "arguments": arguments }),
            )
            .await
    }
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
        if let Some(session) = self.session_id.lock().await.clone() {
            request = request.header("Mcp-Session-Id", session);
        }
        let response = request.send().await.map_err(|error| {
            AikitError::ToolExecution(format!("MCP HTTP request failed: {error}"))
        })?;
        let status = response.status();
        if let Some(session) = response
            .headers()
            .get("Mcp-Session-Id")
            .and_then(|value| value.to_str().ok())
        {
            *self.session_id.lock().await = Some(session.to_string());
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

#[async_trait]
impl McpTransport for StreamableHttpTransport {
    async fn request(&self, method: &str, params: Value) -> Result<Value> {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let response = self
            .post(json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params }))
            .await?
            .ok_or_else(|| AikitError::ToolExecution("MCP request returned no response".into()))?;
        if let Some(error) = response.get("error") {
            return Err(AikitError::ToolExecution(format!(
                "MCP '{method}' failed: {}",
                error
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown error")
            )));
        }
        Ok(response.get("result").cloned().unwrap_or(Value::Null))
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

type PendingSender = oneshot::Sender<std::result::Result<Value, String>>;

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
        let _ = sender.send(Err(message.clone()));
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
            Err(error
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("MCP error")
                .to_string())
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

#[async_trait]
impl McpTransport for StdioTransport {
    async fn request(&self, method: &str, params: Value) -> Result<Value> {
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
        match rx.await {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(msg)) => Err(AikitError::ToolExecution(format!(
                "MCP '{method}' failed: {msg}"
            ))),
            Err(_) => Err(AikitError::ToolExecution(format!(
                "MCP '{method}' response channel dropped"
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
            assert!(error.contains("exceeded 4 MiB"));
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
}
