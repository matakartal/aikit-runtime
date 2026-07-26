use super::{ExternalAgentError, ExternalAgentErrorCode, ExternalAgentResult, RuntimeInstanceId};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{broadcast, mpsc, oneshot, Mutex};

pub const MAX_AGENT_RPC_FRAME_BYTES: usize = 4 << 20;
pub const MAX_AGENT_RPC_PENDING_REQUESTS: usize = 1_024;
pub const MAX_AGENT_RPC_OUTBOUND_QUEUE: usize = 256;
pub const MAX_AGENT_STDERR_TAIL_BYTES: usize = 64 << 10;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonRpcDialect {
    Standard,
    CodexAppServer,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RpcInbound {
    Notification {
        method: String,
        params: Value,
    },
    Request {
        id: Value,
        method: String,
        params: Value,
    },
    Closed {
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RpcError {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<i64>,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

type RpcResponse = Result<Value, RpcError>;
type PendingSender = oneshot::Sender<ExternalAgentResult<RpcResponse>>;

struct ConnectionState {
    pending: StdMutex<HashMap<i64, PendingSender>>,
    stopped: StdMutex<Option<String>>,
    inbound: broadcast::Sender<RpcInbound>,
}

impl ConnectionState {
    fn stop(&self, reason: impl Into<String>) {
        let reason = reason.into();
        let mut stopped = self
            .stopped
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if stopped.is_some() {
            return;
        }
        *stopped = Some(reason.clone());
        drop(stopped);

        let mut pending = self
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for (_, sender) in pending.drain() {
            let _ = sender.send(Err(ExternalAgentError::new(
                ExternalAgentErrorCode::Protocol,
                reason.clone(),
            )));
        }
        drop(pending);
        let _ = self.inbound.send(RpcInbound::Closed { reason });
    }

    fn stopped_reason(&self) -> Option<String> {
        self.stopped
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

#[derive(Clone)]
pub struct JsonRpcConnection {
    dialect: JsonRpcDialect,
    writer: mpsc::Sender<Vec<u8>>,
    state: Arc<ConnectionState>,
    next_id: Arc<AtomicI64>,
}

impl JsonRpcConnection {
    pub fn new<R, W>(reader: R, writer: W, dialect: JsonRpcDialect) -> Self
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let (outbound_tx, outbound_rx) = mpsc::channel(MAX_AGENT_RPC_OUTBOUND_QUEUE);
        let (inbound, _) = broadcast::channel(MAX_AGENT_RPC_OUTBOUND_QUEUE);
        let state = Arc::new(ConnectionState {
            pending: StdMutex::new(HashMap::new()),
            stopped: StdMutex::new(None),
            inbound,
        });
        tokio::spawn(run_rpc_writer(writer, outbound_rx, state.clone()));
        tokio::spawn(run_rpc_reader(reader, state.clone()));
        Self {
            dialect,
            writer: outbound_tx,
            state,
            next_id: Arc::new(AtomicI64::new(1)),
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<RpcInbound> {
        self.state.inbound.subscribe()
    }

    pub fn is_closed(&self) -> bool {
        self.state.stopped_reason().is_some()
    }

    pub async fn request(
        &self,
        method: &str,
        params: Value,
        timeout: Duration,
    ) -> ExternalAgentResult<Value> {
        validate_method(method)?;
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        if id <= 0 {
            return Err(ExternalAgentError::new(
                ExternalAgentErrorCode::Conflict,
                "external-agent JSON-RPC request id space is exhausted",
            ));
        }
        let (sender, receiver) = oneshot::channel();
        {
            let mut pending = self
                .state
                .pending
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(reason) = self.state.stopped_reason() {
                return Err(ExternalAgentError::new(
                    ExternalAgentErrorCode::Protocol,
                    reason,
                ));
            }
            if pending.len() >= MAX_AGENT_RPC_PENDING_REQUESTS {
                return Err(ExternalAgentError::new(
                    ExternalAgentErrorCode::Conflict,
                    "external-agent JSON-RPC pending request capacity was reached",
                ));
            }
            pending.insert(id, sender);
        }

        let payload = self.message(json!({"id": id, "method": method, "params": params}));
        if let Err(error) = self.send_value(payload).await {
            self.remove_pending(id);
            return Err(error);
        }

        match tokio::time::timeout(timeout, receiver).await {
            Ok(Ok(Ok(Ok(result)))) => Ok(result),
            Ok(Ok(Ok(Err(error)))) => Err(ExternalAgentError::new(
                ExternalAgentErrorCode::Protocol,
                format!("external-agent RPC `{method}` failed: {}", error.message),
            )),
            Ok(Ok(Err(error))) => Err(error),
            Ok(Err(_)) => Err(ExternalAgentError::new(
                ExternalAgentErrorCode::Protocol,
                format!("external-agent RPC `{method}` response channel closed"),
            )),
            Err(_) => {
                self.remove_pending(id);
                Err(ExternalAgentError::new(
                    ExternalAgentErrorCode::Timeout,
                    format!("external-agent RPC `{method}` timed out"),
                ))
            }
        }
    }

    pub async fn notify(&self, method: &str, params: Value) -> ExternalAgentResult<()> {
        validate_method(method)?;
        let payload = self.message(json!({"method": method, "params": params}));
        self.send_value(payload).await
    }

    pub async fn respond_result(&self, id: Value, result: Value) -> ExternalAgentResult<()> {
        validate_peer_id(&id)?;
        let payload = self.message(json!({"id": id, "result": result}));
        self.send_value(payload).await
    }

    pub async fn respond_error(&self, id: Value, error: RpcError) -> ExternalAgentResult<()> {
        validate_peer_id(&id)?;
        let payload = self.message(json!({"id": id, "error": error}));
        self.send_value(payload).await
    }

    pub fn close(&self, reason: impl Into<String>) {
        self.state.stop(reason);
    }

    fn message(&self, mut value: Value) -> Value {
        if self.dialect == JsonRpcDialect::Standard {
            if let Some(object) = value.as_object_mut() {
                object.insert("jsonrpc".into(), Value::String("2.0".into()));
            }
        }
        value
    }

    async fn send_value(&self, value: Value) -> ExternalAgentResult<()> {
        if let Some(reason) = self.state.stopped_reason() {
            return Err(ExternalAgentError::new(
                ExternalAgentErrorCode::Protocol,
                reason,
            ));
        }
        let mut bytes = serde_json::to_vec(&value).map_err(|error| {
            ExternalAgentError::new(
                ExternalAgentErrorCode::Protocol,
                format!("failed to encode external-agent RPC frame: {error}"),
            )
        })?;
        if bytes.len() > MAX_AGENT_RPC_FRAME_BYTES {
            return Err(ExternalAgentError::new(
                ExternalAgentErrorCode::InvalidInput,
                "external-agent RPC request exceeded 4 MiB",
            ));
        }
        bytes.push(b'\n');
        self.writer.send(bytes).await.map_err(|_| {
            ExternalAgentError::new(
                ExternalAgentErrorCode::Protocol,
                "external-agent RPC writer is closed",
            )
        })
    }

    fn remove_pending(&self, id: i64) {
        self.state
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&id);
    }
}

fn validate_method(method: &str) -> ExternalAgentResult<()> {
    if method.is_empty() || method.len() > 512 || method.chars().any(char::is_control) {
        return Err(ExternalAgentError::invalid_input(
            "external-agent RPC method is invalid",
        ));
    }
    Ok(())
}

fn validate_peer_id(id: &Value) -> ExternalAgentResult<()> {
    if id.is_string() || id.as_i64().is_some() || id.as_u64().is_some() {
        Ok(())
    } else {
        Err(ExternalAgentError::invalid_input(
            "external-agent peer request id must be a string or integer",
        ))
    }
}

async fn run_rpc_writer<W>(
    mut writer: W,
    mut outbound: mpsc::Receiver<Vec<u8>>,
    state: Arc<ConnectionState>,
) where
    W: AsyncWrite + Unpin,
{
    use tokio::io::AsyncWriteExt;

    while let Some(frame) = outbound.recv().await {
        if let Err(error) = writer.write_all(&frame).await {
            state.stop(format!("external-agent RPC write failed: {error}"));
            return;
        }
        if let Err(error) = writer.flush().await {
            state.stop(format!("external-agent RPC flush failed: {error}"));
            return;
        }
    }
}

async fn read_bounded_line<R>(reader: &mut R, line: &mut Vec<u8>) -> std::io::Result<bool>
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
        if line.len().saturating_add(payload_bytes) > MAX_AGENT_RPC_FRAME_BYTES {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "external-agent RPC frame exceeded 4 MiB",
            ));
        }
        line.extend_from_slice(&available[..payload_bytes]);
        reader.consume(payload_bytes + usize::from(newline.is_some()));
        if newline.is_some() {
            return Ok(true);
        }
    }
}

async fn run_rpc_reader<R>(reader: R, state: Arc<ConnectionState>)
where
    R: AsyncRead + Unpin,
{
    use tokio::io::BufReader;

    let mut reader = BufReader::new(reader);
    let mut line = Vec::new();
    loop {
        match read_bounded_line(&mut reader, &mut line).await {
            Ok(true) if line.is_empty() => continue,
            Ok(true) => {}
            Ok(false) => {
                state.stop("external-agent RPC peer closed stdout");
                return;
            }
            Err(error) => {
                state.stop(format!("external-agent RPC read failed: {error}"));
                return;
            }
        }

        let message: Value = match serde_json::from_slice(&line) {
            Ok(message) => message,
            Err(error) => {
                state.stop(format!("external-agent RPC emitted invalid JSON: {error}"));
                return;
            }
        };
        if let Err(error) = dispatch_message(message, &state) {
            state.stop(error.message);
            return;
        }
    }
}

fn dispatch_message(message: Value, state: &ConnectionState) -> ExternalAgentResult<()> {
    let object = message.as_object().ok_or_else(|| {
        ExternalAgentError::new(
            ExternalAgentErrorCode::Protocol,
            "external-agent RPC frame must be a JSON object",
        )
    })?;

    if let Some(method) = object.get("method").and_then(Value::as_str) {
        validate_method(method)?;
        let params = object.get("params").cloned().unwrap_or(Value::Null);
        let event = match object.get("id") {
            Some(id) => {
                validate_peer_id(id)?;
                RpcInbound::Request {
                    id: id.clone(),
                    method: method.to_string(),
                    params,
                }
            }
            None => RpcInbound::Notification {
                method: method.to_string(),
                params,
            },
        };
        let _ = state.inbound.send(event);
        return Ok(());
    }

    let id = object
        .get("id")
        .and_then(normalize_response_id)
        .ok_or_else(|| {
            ExternalAgentError::new(
                ExternalAgentErrorCode::Protocol,
                "external-agent RPC response omitted a supported id",
            )
        })?;
    let response = if let Some(error) = object.get("error") {
        Err(parse_rpc_error(error)?)
    } else if let Some(result) = object.get("result") {
        Ok(result.clone())
    } else {
        return Err(ExternalAgentError::new(
            ExternalAgentErrorCode::Protocol,
            "external-agent RPC response omitted both result and error",
        ));
    };
    if let Some(sender) = state
        .pending
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remove(&id)
    {
        let _ = sender.send(Ok(response));
    }
    Ok(())
}

fn normalize_response_id(id: &Value) -> Option<i64> {
    id.as_i64()
        .or_else(|| id.as_u64().and_then(|value| i64::try_from(value).ok()))
}

fn parse_rpc_error(value: &Value) -> ExternalAgentResult<RpcError> {
    let object = value.as_object().ok_or_else(|| {
        ExternalAgentError::new(
            ExternalAgentErrorCode::Protocol,
            "external-agent RPC error must be an object",
        )
    })?;
    Ok(RpcError {
        code: object.get("code").and_then(Value::as_i64),
        message: object
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("external-agent RPC failed")
            .to_string(),
        data: object.get("data").cloned(),
    })
}

#[derive(Clone)]
pub struct AgentProcessLaunch {
    pub instance_id: RuntimeInstanceId,
    pub program: PathBuf,
    pub args: Vec<String>,
    pub cwd: Option<PathBuf>,
    pub env: BTreeMap<String, String>,
    pub inherit_env: bool,
    pub dialect: JsonRpcDialect,
}

use std::collections::BTreeMap;

impl std::fmt::Debug for AgentProcessLaunch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AgentProcessLaunch")
            .field("instance_id", &self.instance_id)
            .field("program", &self.program)
            .field("args", &self.args)
            .field("cwd", &self.cwd)
            .field("env_keys", &self.env.keys().collect::<Vec<_>>())
            .field("inherit_env", &self.inherit_env)
            .field("dialect", &self.dialect)
            .finish()
    }
}

pub struct JsonRpcProcess {
    connection: JsonRpcConnection,
    child: Mutex<Option<tokio::process::Child>>,
    stderr_tail: Arc<StdMutex<StderrTail>>,
    process_group: Option<u32>,
    stopped: AtomicBool,
}

impl JsonRpcProcess {
    pub async fn spawn(launch: AgentProcessLaunch) -> ExternalAgentResult<Self> {
        use std::process::Stdio;

        if !launch.program.is_absolute() {
            return Err(ExternalAgentError::invalid_input(
                "external-agent process path must be absolute",
            ));
        }
        let mut command = tokio::process::Command::new(&launch.program);
        command.args(&launch.args);
        if let Some(cwd) = &launch.cwd {
            command.current_dir(cwd);
        }
        if !launch.inherit_env {
            command.env_clear();
        }
        command.envs(&launch.env);
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt as _;
            command.as_std_mut().process_group(0);
        }
        let mut child = command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(|error| {
                ExternalAgentError::new(
                    ExternalAgentErrorCode::SpawnFailed,
                    format!(
                        "failed to spawn external agent `{}`: {error}",
                        launch.instance_id
                    ),
                )
            })?;
        let process_group = child.id();
        let stdin = child.stdin.take().ok_or_else(|| {
            ExternalAgentError::new(
                ExternalAgentErrorCode::SpawnFailed,
                "external-agent child stdin was not piped",
            )
        })?;
        let stdout = child.stdout.take().ok_or_else(|| {
            ExternalAgentError::new(
                ExternalAgentErrorCode::SpawnFailed,
                "external-agent child stdout was not piped",
            )
        })?;
        let stderr = child.stderr.take().ok_or_else(|| {
            ExternalAgentError::new(
                ExternalAgentErrorCode::SpawnFailed,
                "external-agent child stderr was not piped",
            )
        })?;
        let stderr_tail = Arc::new(StdMutex::new(StderrTail::default()));
        tokio::spawn(capture_stderr(stderr, stderr_tail.clone()));
        Ok(Self {
            connection: JsonRpcConnection::new(stdout, stdin, launch.dialect),
            child: Mutex::new(Some(child)),
            stderr_tail,
            process_group,
            stopped: AtomicBool::new(false),
        })
    }

    pub fn connection(&self) -> &JsonRpcConnection {
        &self.connection
    }

    pub fn stderr_tail(&self) -> String {
        self.stderr_tail
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .render()
    }

    pub async fn shutdown(&self, grace: Duration) -> ExternalAgentResult<()> {
        if self.stopped.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        self.connection
            .close("external-agent process is shutting down");
        let mut child = self.child.lock().await;
        let Some(mut child) = child.take() else {
            return Ok(());
        };
        if tokio::time::timeout(grace, child.wait()).await.is_ok() {
            return Ok(());
        }
        kill_process_group(self.process_group);
        let _ = child.start_kill();
        let _ = tokio::time::timeout(Duration::from_secs(1), child.wait()).await;
        Ok(())
    }
}

impl Drop for JsonRpcProcess {
    fn drop(&mut self) {
        if !self.stopped.swap(true, Ordering::AcqRel) {
            kill_process_group(self.process_group);
        }
    }
}

fn kill_process_group(process_group: Option<u32>) {
    #[cfg(unix)]
    if let Some(pid) = process_group.and_then(|pid| i32::try_from(pid).ok()) {
        // SAFETY: the child was created as the leader of a fresh process group. A negative pid
        // targets only that group; failure simply means it has already exited.
        unsafe {
            libc::kill(-pid, libc::SIGKILL);
        }
    }
    #[cfg(not(unix))]
    let _ = process_group;
}

#[derive(Default)]
struct StderrTail {
    bytes: VecDeque<u8>,
    truncated: bool,
}

impl StderrTail {
    fn push(&mut self, bytes: &[u8]) {
        let bytes = redact_stderr(bytes);
        if bytes.len() >= MAX_AGENT_STDERR_TAIL_BYTES {
            self.truncated = true;
            self.bytes.clear();
            self.bytes.extend(
                bytes[bytes.len() - MAX_AGENT_STDERR_TAIL_BYTES..]
                    .iter()
                    .copied(),
            );
            return;
        }
        let overflow = self
            .bytes
            .len()
            .saturating_add(bytes.len())
            .saturating_sub(MAX_AGENT_STDERR_TAIL_BYTES);
        if overflow > 0 {
            self.truncated = true;
            self.bytes.drain(..overflow.min(self.bytes.len()));
        }
        self.bytes.extend(bytes);
    }

    fn render(&self) -> String {
        let bytes = self.bytes.iter().copied().collect::<Vec<_>>();
        let mut output = String::from_utf8_lossy(&bytes).into_owned();
        if self.truncated {
            output.insert_str(0, "[earlier stderr truncated]\n");
        }
        output
    }
}

fn redact_stderr(bytes: &[u8]) -> Vec<u8> {
    let text = String::from_utf8_lossy(bytes);
    let lower = text.to_ascii_lowercase();
    if [
        "authorization",
        "api_key",
        "apikey",
        "access_token",
        "refresh_token",
        "password",
        "secret",
    ]
    .iter()
    .any(|pattern| lower.contains(pattern))
    {
        b"[sensitive stderr line redacted]\n".to_vec()
    } else {
        bytes.to_vec()
    }
}

async fn capture_stderr<R>(mut stderr: R, tail: Arc<StdMutex<StderrTail>>)
where
    R: AsyncRead + Unpin,
{
    use tokio::io::AsyncReadExt;

    let mut buffer = [0_u8; 8 << 10];
    loop {
        match stderr.read(&mut buffer).await {
            Ok(0) => return,
            Ok(read) => tail
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(&buffer[..read]),
            Err(_) => return,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    #[tokio::test]
    async fn request_correlates_response_and_uses_standard_header() {
        let (client, server) = tokio::io::duplex(16 << 10);
        let (client_read, client_write) = tokio::io::split(client);
        let connection =
            JsonRpcConnection::new(client_read, client_write, JsonRpcDialect::Standard);
        let peer = tokio::spawn(async move {
            let (read, mut write) = tokio::io::split(server);
            let mut read = BufReader::new(read);
            let mut line = String::new();
            read.read_line(&mut line).await.unwrap();
            let request: Value = serde_json::from_str(&line).unwrap();
            assert_eq!(request["jsonrpc"], "2.0");
            assert_eq!(request["method"], "model/list");
            let response = json!({"jsonrpc":"2.0","id":request["id"],"result":{"models":[]}});
            write
                .write_all(format!("{response}\n").as_bytes())
                .await
                .unwrap();
        });
        let result = connection
            .request("model/list", json!({}), Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(result, json!({"models":[]}));
        peer.await.unwrap();
    }

    #[tokio::test]
    async fn codex_dialect_handles_server_requests_without_jsonrpc_header() {
        let (client, server) = tokio::io::duplex(16 << 10);
        let (client_read, client_write) = tokio::io::split(client);
        let connection =
            JsonRpcConnection::new(client_read, client_write, JsonRpcDialect::CodexAppServer);
        let mut inbound = connection.subscribe();
        let peer = tokio::spawn(async move {
            let (read, mut write) = tokio::io::split(server);
            write
                .write_all(b"{\"id\":\"approval-1\",\"method\":\"item/commandExecution/requestApproval\",\"params\":{}}\n")
                .await
                .unwrap();
            let mut read = BufReader::new(read);
            let mut response = String::new();
            read.read_line(&mut response).await.unwrap();
            let response: Value = serde_json::from_str(&response).unwrap();
            assert!(response.get("jsonrpc").is_none());
            assert_eq!(response["id"], "approval-1");
            assert_eq!(response["result"]["decision"], "decline");
        });
        let event = inbound.recv().await.unwrap();
        let RpcInbound::Request { id, method, .. } = event else {
            panic!("expected server request")
        };
        assert_eq!(method, "item/commandExecution/requestApproval");
        connection
            .respond_result(id, json!({"decision":"decline"}))
            .await
            .unwrap();
        peer.await.unwrap();
    }

    #[tokio::test]
    async fn timed_out_request_is_removed_and_late_response_is_ignored() {
        let (client, _server) = tokio::io::duplex(16 << 10);
        let (client_read, client_write) = tokio::io::split(client);
        let connection =
            JsonRpcConnection::new(client_read, client_write, JsonRpcDialect::Standard);
        let error = connection
            .request("slow", json!({}), Duration::from_millis(10))
            .await
            .unwrap_err();
        assert_eq!(error.code, ExternalAgentErrorCode::Timeout);
        assert!(connection
            .state
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_empty());
    }

    #[test]
    fn stderr_redaction_is_fail_safe_and_bounded() {
        let mut tail = StderrTail::default();
        tail.push(b"Authorization: Bearer secret-value\n");
        assert_eq!(tail.render(), "[sensitive stderr line redacted]\n");
        tail.push(&vec![b'x'; MAX_AGENT_STDERR_TAIL_BYTES + 1]);
        assert!(tail.bytes.len() <= MAX_AGENT_STDERR_TAIL_BYTES);
        assert!(tail.truncated);
    }
}
