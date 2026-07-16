//! A minimal **MCP (Model Context Protocol) client** — connect an agent to external tool servers.
//!
//! MCP is JSON-RPC 2.0 over a transport. This module gives you:
//!   - [`McpTransport`] — the transport seam (send a request / a notification).
//!   - [`StdioTransport`] — the production transport: spawn an MCP server subprocess and exchange
//!     newline-delimited JSON-RPC over its stdin/stdout, correlating replies by id.
//!   - [`McpClient`] — the handshake (`initialize`), tool discovery (`tools/list` → [`ToolSpec`]s),
//!     and invocation (`tools/call`).
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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex};

/// The MCP revision this client advertises in the `initialize` handshake.
pub const MCP_PROTOCOL_VERSION: &str = "2025-06-18";

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
    tools: Vec<ToolSpec>,
    initialized: AtomicBool,
}

impl McpClient {
    pub fn new(transport: Arc<dyn McpTransport>, server: impl Into<String>) -> Self {
        McpClient {
            transport,
            server: server.into(),
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
        let mut raw = Vec::new();
        let mut cursor = None;
        let mut seen = BTreeSet::new();
        loop {
            let params = cursor
                .as_deref()
                .map_or_else(|| json!({}), |cursor| json!({"cursor":cursor}));
            let result = self.transport.request("tools/list", params).await?;
            raw.extend(
                result
                    .get("tools")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default(),
            );
            let next = result
                .get("nextCursor")
                .and_then(Value::as_str)
                .map(str::to_owned);
            match next {
                Some(next) if seen.insert(next.clone()) => cursor = Some(next),
                Some(_) => {
                    return Err(AikitError::ToolExecution(
                        "MCP tools/list repeated a pagination cursor".into(),
                    ))
                }
                None => break,
            }
        }
        let specs: Vec<ToolSpec> = raw
            .iter()
            .filter_map(|t| {
                let name = t.get("name")?.as_str()?.to_string();
                let description = t
                    .get("description")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string();
                let input_schema = t
                    .get("inputSchema")
                    .cloned()
                    .unwrap_or_else(|| json!({ "type": "object" }));
                Some(ToolSpec {
                    name,
                    description,
                    input_schema,
                })
            })
            .collect();
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
        let mut cursor = cursor.map(str::to_owned);
        let mut seen = BTreeSet::new();
        let mut resources = Vec::new();
        loop {
            let params = cursor
                .as_deref()
                .map_or_else(|| json!({}), |cursor| json!({ "cursor": cursor }));
            let result = self.transport.request("resources/list", params).await?;
            let mut page: Vec<McpResource> = serde_json::from_value(
                result
                    .get("resources")
                    .cloned()
                    .unwrap_or_else(|| json!([])),
            )
            .map_err(|error| {
                AikitError::ToolExecution(format!("invalid MCP resources: {error}"))
            })?;
            resources.append(&mut page);
            match result
                .get("nextCursor")
                .and_then(Value::as_str)
                .map(str::to_owned)
            {
                Some(next) if seen.insert(next.clone()) => cursor = Some(next),
                Some(_) => {
                    return Err(AikitError::ToolExecution(
                        "MCP resources/list repeated a pagination cursor".into(),
                    ))
                }
                None => break,
            }
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
        let mut cursor = cursor.map(str::to_owned);
        let mut seen = BTreeSet::new();
        let mut prompts = Vec::new();
        loop {
            let params = cursor
                .as_deref()
                .map_or_else(|| json!({}), |cursor| json!({ "cursor": cursor }));
            let result = self.transport.request("prompts/list", params).await?;
            let mut page: Vec<McpPrompt> =
                serde_json::from_value(result.get("prompts").cloned().unwrap_or_else(|| json!([])))
                    .map_err(|error| {
                        AikitError::ToolExecution(format!("invalid MCP prompts: {error}"))
                    })?;
            prompts.append(&mut page);
            match result
                .get("nextCursor")
                .and_then(Value::as_str)
                .map(str::to_owned)
            {
                Some(next) if seen.insert(next.clone()) => cursor = Some(next),
                Some(_) => {
                    return Err(AikitError::ToolExecution(
                        "MCP prompts/list repeated a pagination cursor".into(),
                    ))
                }
                None => break,
            }
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
        const MAX_MCP_RESPONSE_BYTES: usize = 4 << 20;
        let mut stream = response.bytes_stream();
        let mut bytes = Vec::new();
        while let Some(chunk) = futures::StreamExt::next(&mut stream).await {
            let chunk = chunk.map_err(|error| {
                AikitError::ToolExecution(format!("MCP HTTP response failed: {error}"))
            })?;
            if bytes.len().saturating_add(chunk.len()) > MAX_MCP_RESPONSE_BYTES {
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

type Pending = Arc<Mutex<HashMap<u64, oneshot::Sender<std::result::Result<Value, String>>>>>;

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
        let pending: Pending = Arc::new(Mutex::new(HashMap::new()));

        // Reader task: one JSON object per line; resolve pending requests by id.
        let reader_pending = pending.clone();
        tokio::spawn(async move {
            use tokio::io::{AsyncBufReadExt, BufReader};
            let mut lines = BufReader::new(stdout).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                let Ok(msg) = serde_json::from_str::<Value>(&line) else {
                    continue;
                };
                // Only responses carry an id we await; server-initiated notifications are ignored.
                let Some(id) = msg.get("id").and_then(Value::as_u64) else {
                    continue;
                };
                let outcome = if let Some(err) = msg.get("error") {
                    Err(err
                        .get("message")
                        .and_then(Value::as_str)
                        .unwrap_or("MCP error")
                        .to_string())
                } else {
                    Ok(msg.get("result").cloned().unwrap_or(Value::Null))
                };
                if let Some(tx) = reader_pending.lock().await.remove(&id) {
                    let _ = tx.send(outcome);
                }
            }
            // Stream closed: fail every remaining waiter so callers don't hang.
            let mut guard = reader_pending.lock().await;
            for (_, tx) in guard.drain() {
                let _ = tx.send(Err("MCP server closed the connection".into()));
            }
        });

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
        self.pending.lock().await.insert(id, tx);
        let payload = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
        if let Err(e) = self.write_line(&payload).await {
            self.pending.lock().await.remove(&id);
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
        let payload = json!({ "jsonrpc": "2.0", "method": method, "params": params });
        self.write_line(&payload).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
