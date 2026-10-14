//! ACP stdio wire transport — serve aikit **as** an ACP agent.
//!
//! This is the serving-side counterpart of the external-agent ACP *client*: an editor (or any
//! ACP client) spawns this process, speaks newline-delimited JSON-RPC 2.0 over stdio, and drives
//! the governed [`AcpSessionMapper`]. The transport routes `initialize`, `session/new`,
//! `session/prompt`, and the `session/cancel` notification; every side effect goes through the
//! mapper's governance envelope and then a host seam ([`AcpPromptHost`]) — the wire layer itself
//! can never execute tools.
//!
//! Wire shapes follow the official ACP v1 JSON (camelCase envelopes, snake_case enum values),
//! matching the shapes the external-agent client already consumes. Unsupported optional methods
//! (`authenticate`, `session/load`, mode/config setters) return JSON-RPC `-32601`; the
//! `initialize` response honestly advertises no auth methods and no optional capabilities.

use super::acp::{
    AcpPromptBlock, AcpPromptRequest, AcpRunMapping, AcpRuntimeEvent, AcpSessionMapper,
    AcpSessionUpdate, ACP_PROTOCOL_VERSION,
};
use super::common::{GovernanceDenialCode, GovernedAction, ProtocolPrincipal};
use super::mcp_io::{protocol_io, read_bounded_line, write_json_line};
use super::{CorrelationIdentity, ProtocolError, ProtocolResult};
use crate::cancellation::CancellationToken;
use crate::protocols::acp::AcpAction;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::sync::{mpsc, Mutex};

/// Default and maximum accepted JSON-RPC frame size, matching the MCP stdio listener.
pub const ACP_STDIO_FRAME_BYTES: usize = 1024 * 1024;
const ACP_OUTBOUND_QUEUE: usize = 256;
const ACP_EVENT_QUEUE: usize = 256;

/// Terminal outcome of one prompt run, serialized as the official `stopReason` value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AcpStopReason {
    EndTurn,
    MaxTokens,
    Cancelled,
    Refusal,
}

/// Bounded channel through which a prompt host streams runtime events; the transport maps each
/// one to an official `session/update` notification in order.
pub struct AcpEventEmitter {
    sender: mpsc::Sender<AcpRuntimeEvent>,
}

impl AcpEventEmitter {
    /// Emit one runtime event. Fails when the connection is gone, so a host observes teardown
    /// instead of streaming into the void.
    pub async fn emit(&self, event: AcpRuntimeEvent) -> ProtocolResult<()> {
        self.sender
            .send(event)
            .await
            .map_err(|_| ProtocolError::conflict("ACP connection closed during prompt streaming"))
    }
}

/// Host seam that executes authorized prompt runs. The transport owns wire framing, governance
/// routing, and run bookkeeping; the host owns the actual agent work.
#[async_trait]
pub trait AcpPromptHost: Send + Sync {
    /// Execute one authorized prompt run, streaming progress through `events`. Returning is the
    /// only way to terminate the run: terminal runtime events are not accepted as substitutes.
    async fn prompt(
        &self,
        request: &AcpPromptRequest,
        mapping: &AcpRunMapping,
        events: &AcpEventEmitter,
    ) -> ProtocolResult<AcpStopReason>;

    /// Cooperatively cancel the identified running prompt. The transport still waits for
    /// [`AcpPromptHost::prompt`] to return (with `Cancelled`) before answering the prompt call.
    async fn cancel(&self, mapping: &AcpRunMapping) -> ProtocolResult<()>;
}

#[derive(Debug, Clone)]
pub struct AcpStdioConfig {
    /// Connection identity used for correlation ids; also the audit-visible transport name.
    pub connection_id: String,
    pub agent_name: String,
    pub agent_version: String,
    pub max_frame_bytes: usize,
    /// Upper bound on one prompt run; expiry cancels the host and reports `cancelled`.
    pub prompt_timeout: Duration,
}

impl Default for AcpStdioConfig {
    fn default() -> Self {
        Self {
            connection_id: "acp-stdio".into(),
            agent_name: "aikit".into(),
            agent_version: env!("CARGO_PKG_VERSION").into(),
            max_frame_bytes: ACP_STDIO_FRAME_BYTES,
            prompt_timeout: Duration::from_secs(300),
        }
    }
}

impl AcpStdioConfig {
    fn validate(&self) -> ProtocolResult<()> {
        if self.connection_id.is_empty()
            || self.connection_id.len() > 128
            || self.connection_id.chars().any(char::is_control)
            || self.agent_name.is_empty()
            || self.agent_version.is_empty()
            || self.max_frame_bytes == 0
            || self.max_frame_bytes > ACP_STDIO_FRAME_BYTES
            || self.prompt_timeout.is_zero()
        {
            return Err(ProtocolError::invalid("invalid ACP stdio configuration"));
        }
        Ok(())
    }
}

struct ActivePrompt {
    mapping: AcpRunMapping,
    request_id: Value,
    task: tokio::task::JoinHandle<ProtocolResult<AcpStopReason>>,
    events: mpsc::Receiver<AcpRuntimeEvent>,
}

/// Serve one ACP client over newline-delimited JSON-RPC until EOF or cancellation.
///
/// `principal` is the authenticated identity of the connecting client: on stdio the spawning
/// host decides who is talking, exactly like the MCP stdio listener. One prompt runs at a time;
/// the mapper's single-active-run invariant turns a concurrent prompt into a typed denial.
pub async fn serve_acp_stdio<R, W>(
    mapper: Arc<Mutex<AcpSessionMapper>>,
    principal: ProtocolPrincipal,
    host: Arc<dyn AcpPromptHost>,
    reader: R,
    mut writer: W,
    config: AcpStdioConfig,
    cancellation: CancellationToken,
) -> ProtocolResult<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    config.validate()?;
    let mut reader = BufReader::new(reader);
    let (outbound_tx, mut outbound_rx) = mpsc::channel::<Value>(ACP_OUTBOUND_QUEUE);
    let mut sequence: u64 = 0;
    let mut active: Option<ActivePrompt> = None;
    let mut result = Ok(());

    'serve: loop {
        tokio::select! {
            () = cancellation.cancelled() => break 'serve,
            outbound = outbound_rx.recv() => {
                if let Some(message) = outbound {
                    if let Err(error) = write_json_line(&mut writer, &message).await {
                        result = Err(error);
                        break 'serve;
                    }
                    let _ = writer.flush().await;
                }
            }
            event = recv_active_event(&mut active) => {
                if let Some(event) = event {
                    if let Err(error) =
                        forward_prompt_event(&mapper, &mut active, event, &mut writer).await
                    {
                        result = Err(error);
                        break 'serve;
                    }
                } else if let Some(Err(error)) =
                    finish_active_prompt(&mapper, &mut active, &mut writer).await?
                {
                    result = Err(error);
                    break 'serve;
                }
            }
            frame = read_bounded_line(&mut reader, config.max_frame_bytes) => {
                let frame = match frame {
                    Ok(Some(frame)) => frame,
                    Ok(None) => break 'serve,
                    Err(error) => {
                        result = Err(error);
                        break 'serve;
                    }
                };
                if frame.iter().all(u8::is_ascii_whitespace) {
                    continue;
                }
                sequence = sequence.saturating_add(1);
                let responses = handle_frame(
                    &mapper,
                    &principal,
                    &host,
                    &config,
                    &outbound_tx,
                    &mut active,
                    sequence,
                    &frame,
                )
                .await?;
                for response in responses {
                    if let Err(error) = write_json_line(&mut writer, &response).await {
                        result = Err(error);
                        break 'serve;
                    }
                }
                let _ = writer.flush().await;
            }
        }
    }

    // Teardown: cooperatively cancel any running prompt, wait briefly, and clear the run so the
    // mapper is consistent for a future connection over the same state.
    if let Some(mut prompt) = active.take() {
        let _ = host.cancel(&prompt.mapping).await;
        let waited = tokio::time::timeout(Duration::from_secs(5), &mut prompt.task).await;
        if waited.is_err() {
            prompt.task.abort();
        }
        let mut mapper = mapper.lock().await;
        let _ = mapper.finish_run(&prompt.mapping.acp_session_id, &prompt.mapping.run_id);
    }
    let shutdown = writer
        .shutdown()
        .await
        .map_err(|error| protocol_io("shut down ACP stdio writer", error));
    result.and(shutdown)
}

/// Await one streamed event from the active prompt without blocking the main loop when idle.
async fn recv_active_event(active: &mut Option<ActivePrompt>) -> Option<AcpRuntimeEvent> {
    match active {
        Some(prompt) => prompt.events.recv().await,
        None => std::future::pending().await,
    }
}

async fn forward_prompt_event<W: AsyncWrite + Unpin>(
    mapper: &Arc<Mutex<AcpSessionMapper>>,
    active: &mut Option<ActivePrompt>,
    event: AcpRuntimeEvent,
    writer: &mut W,
) -> ProtocolResult<()> {
    let Some(prompt) = active.as_ref() else {
        return Ok(());
    };
    // Terminal state is reported through the host's return value only; a terminal runtime event
    // would double-report, so it is rejected here instead of silently rewritten.
    if matches!(
        event,
        AcpRuntimeEvent::Completed | AcpRuntimeEvent::Failed { .. } | AcpRuntimeEvent::Cancelled
    ) {
        return Ok(());
    }
    let update = {
        let mapper = mapper.lock().await;
        mapper.map_event(&prompt.mapping, event)?
    };
    let notification = json!({
        "jsonrpc": "2.0",
        "method": "session/update",
        "params": {
            "sessionId": update.acp_session_id,
            "update": official_session_update(&update.update)?,
        }
    });
    write_json_line(writer, &notification).await?;
    let _ = writer.flush().await;
    Ok(())
}

/// When the event stream ends, join the host task, clear the run, and answer the prompt call.
async fn finish_active_prompt<W: AsyncWrite + Unpin>(
    mapper: &Arc<Mutex<AcpSessionMapper>>,
    active: &mut Option<ActivePrompt>,
    writer: &mut W,
) -> ProtocolResult<Option<ProtocolResult<()>>> {
    let Some(prompt) = active.take() else {
        return Ok(None);
    };
    let outcome = prompt
        .task
        .await
        .map_err(|error| ProtocolError::conflict(format!("ACP prompt task failed: {error}")));
    {
        let mut mapper = mapper.lock().await;
        mapper.finish_run(&prompt.mapping.acp_session_id, &prompt.mapping.run_id)?;
    }
    let response = match outcome {
        Ok(Ok(stop_reason)) => json!({
            "jsonrpc": "2.0",
            "id": prompt.request_id,
            "result": {"stopReason": stop_reason_value(stop_reason)}
        }),
        Ok(Err(error)) | Err(error) => rpc_error(
            prompt.request_id,
            -32000,
            &format!("prompt execution failed: {}", error.message),
        ),
    };
    let written = write_json_line(writer, &response).await;
    let _ = writer.flush().await;
    Ok(Some(written))
}

#[allow(clippy::too_many_arguments)]
async fn handle_frame(
    mapper: &Arc<Mutex<AcpSessionMapper>>,
    principal: &ProtocolPrincipal,
    host: &Arc<dyn AcpPromptHost>,
    config: &AcpStdioConfig,
    outbound: &mpsc::Sender<Value>,
    active: &mut Option<ActivePrompt>,
    sequence: u64,
    frame: &[u8],
) -> ProtocolResult<Vec<Value>> {
    let _ = outbound;
    let Ok(message) = serde_json::from_slice::<Value>(frame) else {
        return Ok(vec![rpc_error(Value::Null, -32700, "parse error")]);
    };
    let Some(object) = message.as_object() else {
        return Ok(vec![rpc_error(Value::Null, -32600, "invalid request")]);
    };
    let id = object.get("id").cloned();
    let Some(method) = object.get("method").and_then(Value::as_str) else {
        return Ok(vec![rpc_error(
            id.unwrap_or(Value::Null),
            -32600,
            "invalid request",
        )]);
    };
    let params = object.get("params").cloned().unwrap_or(Value::Null);
    let correlation = CorrelationIdentity::new(
        format!("{}-{sequence:016}", config.connection_id),
        format!("request-{sequence:016}"),
    )?;

    match (method, id) {
        ("initialize", Some(id)) => Ok(vec![handle_initialize(config, id, &params)]),
        ("session/new", Some(id)) => {
            let governed = {
                let mut mapper = mapper.lock().await;
                mapper.prepare_new_session(None, correlation, Some(principal))
            };
            Ok(vec![match into_authorized(id.clone(), governed) {
                Err(error) => error,
                Ok(AcpAction::OpenSession { mapping }) => json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": {"sessionId": mapping.acp_session_id}
                }),
                Ok(_) => rpc_error(id, -32603, "unexpected ACP session action"),
            }])
        }
        ("session/prompt", Some(id)) => {
            let request = match parse_prompt_request(&params, sequence) {
                Ok(request) => request,
                Err(reason) => return Ok(vec![rpc_error(id, -32602, &reason)]),
            };
            let governed = {
                let mut mapper = mapper.lock().await;
                mapper.prepare_prompt(request, correlation, Some(principal))
            };
            match into_authorized(id.clone(), governed) {
                Err(error) => Ok(vec![error]),
                Ok(AcpAction::Prompt { request, mapping }) => {
                    let (event_tx, event_rx) = mpsc::channel(ACP_EVENT_QUEUE);
                    let emitter = AcpEventEmitter { sender: event_tx };
                    let task_host = host.clone();
                    let task_request = request.clone();
                    let task_mapping = mapping.clone();
                    let timeout = config.prompt_timeout;
                    let cancel_host = host.clone();
                    let cancel_mapping = mapping.clone();
                    let task = tokio::spawn(async move {
                        match tokio::time::timeout(
                            timeout,
                            task_host.prompt(&task_request, &task_mapping, &emitter),
                        )
                        .await
                        {
                            Ok(outcome) => outcome,
                            Err(_) => {
                                let _ = cancel_host.cancel(&cancel_mapping).await;
                                Ok(AcpStopReason::Cancelled)
                            }
                        }
                    });
                    *active = Some(ActivePrompt {
                        mapping,
                        request_id: id,
                        task,
                        events: event_rx,
                    });
                    // The response is produced when the host task terminates.
                    Ok(Vec::new())
                }
                Ok(_) => Ok(vec![rpc_error(id, -32603, "unexpected ACP prompt action")]),
            }
        }
        ("session/cancel", request_id) => {
            let session_id = params
                .get("sessionId")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned();
            let governed = {
                let mapper = mapper.lock().await;
                mapper.prepare_cancel(&session_id, correlation, Some(principal))
            };
            match governed.into_authorized() {
                Ok((_, AcpAction::Cancel { mapping })) => {
                    host.cancel(&mapping).await?;
                }
                Ok(_) | Err(_) => {
                    // Cancellation is a notification: an unauthorized or unknown target simply
                    // does not cancel anything. A caller that used request form still receives a
                    // typed error.
                }
            }
            Ok(match request_id {
                Some(id) => vec![json!({"jsonrpc": "2.0", "id": id, "result": null})],
                None => Vec::new(),
            })
        }
        (_, Some(id)) => Ok(vec![rpc_error(id, -32601, "method not found")]),
        (_, None) => Ok(Vec::new()),
    }
}

fn handle_initialize(config: &AcpStdioConfig, id: Value, params: &Value) -> Value {
    let requested = params.get("protocolVersion").and_then(Value::as_u64);
    if requested != Some(u64::from(ACP_PROTOCOL_VERSION)) {
        return rpc_error_with_data(
            id,
            -32602,
            "unsupported protocolVersion",
            json!({"supportedProtocolVersion": ACP_PROTOCOL_VERSION}),
        );
    }
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": {
            "protocolVersion": ACP_PROTOCOL_VERSION,
            "agentCapabilities": {},
            "authMethods": [],
            "agentInfo": {"name": config.agent_name, "version": config.agent_version}
        }
    })
}

fn parse_prompt_request(params: &Value, sequence: u64) -> Result<AcpPromptRequest, String> {
    let session_id = params
        .get("sessionId")
        .and_then(Value::as_str)
        .ok_or("session/prompt requires sessionId")?;
    let blocks = params
        .get("prompt")
        .and_then(Value::as_array)
        .ok_or("session/prompt requires a prompt block array")?;
    let mut mapped = Vec::with_capacity(blocks.len());
    for block in blocks {
        mapped.push(parse_prompt_block(block)?);
    }
    Ok(AcpPromptRequest {
        acp_session_id: session_id.to_owned(),
        // ACP prompt calls carry no client-supplied idempotency id; the connection-local
        // sequence provides a stable one for the mapper.
        prompt_id: format!("prompt-{sequence:016}"),
        blocks: mapped,
        metadata: BTreeMap::new(),
    })
}

fn parse_prompt_block(block: &Value) -> Result<AcpPromptBlock, String> {
    let kind = block
        .get("type")
        .and_then(Value::as_str)
        .ok_or("prompt blocks require a type")?;
    match kind {
        "text" => Ok(AcpPromptBlock::Text {
            text: block
                .get("text")
                .and_then(Value::as_str)
                .ok_or("text blocks require text")?
                .to_owned(),
        }),
        "resource_link" => Ok(AcpPromptBlock::Resource {
            uri: block
                .get("uri")
                .and_then(Value::as_str)
                .ok_or("resource_link blocks require uri")?
                .to_owned(),
            name: block
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
        }),
        "image" => Ok(AcpPromptBlock::Image {
            uri: block
                .get("uri")
                .and_then(Value::as_str)
                .ok_or("image blocks require uri")?
                .to_owned(),
            media_type: block
                .get("mimeType")
                .and_then(Value::as_str)
                .ok_or("image blocks require mimeType")?
                .to_owned(),
        }),
        other => Err(format!("unsupported prompt block type: {other}")),
    }
}

/// Project the mapper's SDK-neutral update into the official ACP `session/update` shape.
fn official_session_update(update: &AcpSessionUpdate) -> ProtocolResult<Value> {
    Ok(match update {
        AcpSessionUpdate::AgentMessageChunk { text } => json!({
            "sessionUpdate": "agent_message_chunk",
            "content": {"type": "text", "text": text}
        }),
        AcpSessionUpdate::AgentThoughtChunk { text } => json!({
            "sessionUpdate": "agent_thought_chunk",
            "content": {"type": "text", "text": text}
        }),
        AcpSessionUpdate::ToolCall {
            tool_call_id,
            title,
            raw_input,
        } => json!({
            "sessionUpdate": "tool_call",
            "toolCallId": tool_call_id,
            "title": title,
            "status": "pending",
            "rawInput": raw_input
        }),
        AcpSessionUpdate::ToolCallUpdate {
            tool_call_id,
            status,
            message,
        } => {
            let mut value = json!({
                "sessionUpdate": "tool_call_update",
                "toolCallId": tool_call_id,
                "status": serde_json::to_value(status).map_err(|error| {
                    ProtocolError::invalid(format!("serialize ACP tool status: {error}"))
                })?,
            });
            if let Some(message) = message {
                value
                    .as_object_mut()
                    .expect("tool update is an object")
                    .insert(
                        "content".into(),
                        json!([{ "type": "text", "text": message }]),
                    );
            }
            value
        }
        AcpSessionUpdate::Plan { entries } => json!({
            "sessionUpdate": "plan",
            "entries": entries
                .iter()
                .map(|entry| {
                    Ok(json!({
                        "content": entry.content,
                        "status": serde_json::to_value(&entry.status).map_err(|error| {
                            ProtocolError::invalid(format!("serialize ACP plan status: {error}"))
                        })?,
                        "priority": "medium",
                    }))
                })
                .collect::<ProtocolResult<Vec<Value>>>()?
        }),
        AcpSessionUpdate::UsageUpdate {
            input_tokens,
            output_tokens,
        } => json!({
            "sessionUpdate": "usage_update",
            "inputTokens": input_tokens,
            "outputTokens": output_tokens
        }),
        AcpSessionUpdate::Completed
        | AcpSessionUpdate::Failed { .. }
        | AcpSessionUpdate::Cancelled => {
            return Err(ProtocolError::conflict(
                "terminal ACP updates are reported through the prompt result",
            ))
        }
    })
}

fn stop_reason_value(stop_reason: AcpStopReason) -> Value {
    serde_json::to_value(stop_reason).expect("stop reason serializes to a string")
}

fn into_authorized(id: Value, governed: GovernedAction<AcpAction>) -> Result<AcpAction, Value> {
    if !governed.is_authorized() {
        let (code, reason) = denial_details(&governed);
        return Err(rpc_error(id, code, &reason));
    }
    governed
        .into_authorized()
        .map(|(_, action)| action)
        .map_err(|error| rpc_error(id, -32603, &error.message))
}

fn denial_details(governed: &GovernedAction<AcpAction>) -> (i64, String) {
    match &governed.envelope.authorization {
        super::common::GovernanceAuthorization::Denied { code, reason } => {
            let rpc_code = match code {
                GovernanceDenialCode::UnknownTarget => -32602,
                GovernanceDenialCode::InvalidRequest => -32602,
                GovernanceDenialCode::StateConflict | GovernanceDenialCode::DuplicateConflict => {
                    -32000
                }
                _ => -32000,
            };
            (rpc_code, reason.clone())
        }
        super::common::GovernanceAuthorization::Allowed => (-32603, "unreachable".into()),
    }
}

fn rpc_error(id: Value, code: i64, message: &str) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": message}})
}

fn rpc_error_with_data(id: Value, code: i64, message: &str, data: Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {"code": code, "message": message, "data": data}
    })
}

/// Metadata-only default host: any authorized prompt is a configuration error.
#[derive(Debug, Default)]
pub struct RejectingAcpPromptHost;

#[async_trait]
impl AcpPromptHost for RejectingAcpPromptHost {
    async fn prompt(
        &self,
        _request: &AcpPromptRequest,
        _mapping: &AcpRunMapping,
        _events: &AcpEventEmitter,
    ) -> ProtocolResult<AcpStopReason> {
        Err(ProtocolError::invalid(
            "this ACP listener has no prompt host configured",
        ))
    }

    async fn cancel(&self, _mapping: &AcpRunMapping) -> ProtocolResult<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocols::acp::AcpPlanEntry;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::io::{duplex, split, AsyncBufReadExt};
    use tokio::sync::Notify;

    fn principal(scopes: &[&str]) -> ProtocolPrincipal {
        ProtocolPrincipal::new("acp-user", scopes.iter().copied()).unwrap()
    }

    fn full_scopes() -> ProtocolPrincipal {
        principal(&[
            "acp:sessions:open",
            "acp:sessions:prompt",
            "acp:sessions:cancel",
        ])
    }

    /// Streams a scripted update sequence; optionally holds until cancelled.
    struct ScriptedHost {
        hold_for_cancel: Option<Arc<Notify>>,
        cancelled: AtomicBool,
    }

    #[async_trait]
    impl AcpPromptHost for ScriptedHost {
        async fn prompt(
            &self,
            request: &AcpPromptRequest,
            _mapping: &AcpRunMapping,
            events: &AcpEventEmitter,
        ) -> ProtocolResult<AcpStopReason> {
            events
                .emit(AcpRuntimeEvent::AgentThoughtChunk {
                    text: "planning".into(),
                })
                .await?;
            events
                .emit(AcpRuntimeEvent::Plan {
                    entries: vec![AcpPlanEntry {
                        content: "answer the prompt".into(),
                        status: crate::protocols::acp::AcpPlanEntryStatus::InProgress,
                    }],
                })
                .await?;
            events
                .emit(AcpRuntimeEvent::ToolCall {
                    tool_call_id: "call-1".into(),
                    title: "search".into(),
                    raw_input: serde_json::json!({"q": "acp"}),
                })
                .await?;
            events
                .emit(AcpRuntimeEvent::ToolCallUpdate {
                    tool_call_id: "call-1".into(),
                    status: crate::protocols::acp::AcpToolCallStatus::Completed,
                    message: Some("3 rows".into()),
                })
                .await?;
            events
                .emit(AcpRuntimeEvent::AgentMessageChunk {
                    text: format!("echo:{}", request.prompt_id),
                })
                .await?;
            events
                .emit(AcpRuntimeEvent::Usage {
                    input_tokens: 11,
                    output_tokens: 7,
                })
                .await?;
            if let Some(hold) = &self.hold_for_cancel {
                hold.notified().await;
                return Ok(AcpStopReason::Cancelled);
            }
            Ok(AcpStopReason::EndTurn)
        }

        async fn cancel(&self, _mapping: &AcpRunMapping) -> ProtocolResult<()> {
            self.cancelled.store(true, Ordering::SeqCst);
            if let Some(hold) = &self.hold_for_cancel {
                hold.notify_one();
            }
            Ok(())
        }
    }

    async fn read_json_line<R: tokio::io::AsyncBufRead + Unpin>(reader: &mut R) -> Value {
        let mut line = String::new();
        tokio::time::timeout(Duration::from_secs(2), reader.read_line(&mut line))
            .await
            .expect("read within deadline")
            .expect("read succeeds");
        serde_json::from_str(line.trim()).expect("valid JSON frame")
    }

    async fn send<W: AsyncWrite + Unpin>(writer: &mut W, value: Value) {
        writer
            .write_all(format!("{value}\n").as_bytes())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn serves_initialize_session_prompt_streamed_updates_and_stop_reason() {
        let (client, server_io) = duplex(256 * 1024);
        let (client_read, mut client_write) = split(client);
        let (server_read, server_write) = split(server_io);
        let cancellation = CancellationToken::new();
        let handle = cancellation.handle();
        let mapper = Arc::new(Mutex::new(AcpSessionMapper::new()));
        let server = tokio::spawn(serve_acp_stdio(
            mapper.clone(),
            full_scopes(),
            Arc::new(ScriptedHost {
                hold_for_cancel: None,
                cancelled: AtomicBool::new(false),
            }),
            server_read,
            server_write,
            AcpStdioConfig::default(),
            cancellation,
        ));
        let mut replies = tokio::io::BufReader::new(client_read);

        // Version negotiation fails closed before anything else.
        send(
            &mut client_write,
            serde_json::json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":9}}),
        )
        .await;
        let rejected = read_json_line(&mut replies).await;
        assert_eq!(rejected["error"]["code"], -32602);
        assert_eq!(
            rejected["error"]["data"]["supportedProtocolVersion"],
            ACP_PROTOCOL_VERSION
        );

        send(
            &mut client_write,
            serde_json::json!({"jsonrpc":"2.0","id":2,"method":"initialize","params":{"protocolVersion":1,"clientCapabilities":{}}}),
        )
        .await;
        let initialized = read_json_line(&mut replies).await;
        assert_eq!(initialized["result"]["protocolVersion"], 1);
        assert_eq!(initialized["result"]["agentInfo"]["name"], "aikit");
        assert_eq!(initialized["result"]["authMethods"], serde_json::json!([]));

        send(
            &mut client_write,
            serde_json::json!({"jsonrpc":"2.0","id":3,"method":"session/new","params":{"cwd":"/tmp"}}),
        )
        .await;
        let session = read_json_line(&mut replies).await;
        let session_id = session["result"]["sessionId"].as_str().unwrap().to_owned();

        send(
            &mut client_write,
            serde_json::json!({"jsonrpc":"2.0","id":4,"method":"session/prompt","params":{
                "sessionId": session_id,
                "prompt": [
                    {"type":"text","text":"hello"},
                    {"type":"resource_link","uri":"file:///tmp/a.rs","name":"a.rs"},
                ]
            }}),
        )
        .await;

        // Updates arrive as official session/update notifications, in emission order.
        let mut updates = Vec::new();
        loop {
            let frame = read_json_line(&mut replies).await;
            if frame.get("method").is_some() {
                assert_eq!(frame["method"], "session/update");
                assert_eq!(frame["params"]["sessionId"].as_str().unwrap(), session_id);
                updates.push(frame["params"]["update"].clone());
                continue;
            }
            assert_eq!(frame["id"], 4);
            assert_eq!(frame["result"]["stopReason"], "end_turn");
            break;
        }
        let kinds: Vec<&str> = updates
            .iter()
            .map(|update| update["sessionUpdate"].as_str().unwrap())
            .collect();
        assert_eq!(
            kinds,
            [
                "agent_thought_chunk",
                "plan",
                "tool_call",
                "tool_call_update",
                "agent_message_chunk",
                "usage_update"
            ]
        );
        assert_eq!(updates[2]["toolCallId"], "call-1");
        assert_eq!(updates[3]["status"], "completed");
        assert_eq!(updates[4]["content"]["type"], "text");
        assert_eq!(updates[5]["inputTokens"], 11);

        // The run is cleared, so the same session accepts the next prompt.
        send(
            &mut client_write,
            serde_json::json!({"jsonrpc":"2.0","id":5,"method":"session/prompt","params":{
                "sessionId": session_id,
                "prompt": [{"type":"text","text":"again"}]
            }}),
        )
        .await;
        loop {
            let frame = read_json_line(&mut replies).await;
            if frame.get("method").is_some() {
                continue;
            }
            assert_eq!(frame["id"], 5);
            assert_eq!(frame["result"]["stopReason"], "end_turn");
            break;
        }

        handle.cancel();
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn cancel_notification_cancels_the_running_prompt() {
        let (client, server_io) = duplex(256 * 1024);
        let (client_read, mut client_write) = split(client);
        let (server_read, server_write) = split(server_io);
        let cancellation = CancellationToken::new();
        let handle = cancellation.handle();
        let mapper = Arc::new(Mutex::new(AcpSessionMapper::new()));
        let host = Arc::new(ScriptedHost {
            hold_for_cancel: Some(Arc::new(Notify::new())),
            cancelled: AtomicBool::new(false),
        });
        let server = tokio::spawn(serve_acp_stdio(
            mapper.clone(),
            full_scopes(),
            host.clone(),
            server_read,
            server_write,
            AcpStdioConfig::default(),
            cancellation,
        ));
        let mut replies = tokio::io::BufReader::new(client_read);
        send(
            &mut client_write,
            serde_json::json!({"jsonrpc":"2.0","id":1,"method":"session/new","params":{}}),
        )
        .await;
        let session = read_json_line(&mut replies).await;
        let session_id = session["result"]["sessionId"].as_str().unwrap().to_owned();
        send(
            &mut client_write,
            serde_json::json!({"jsonrpc":"2.0","id":2,"method":"session/prompt","params":{
                "sessionId": session_id,
                "prompt": [{"type":"text","text":"long job"}]
            }}),
        )
        .await;
        // Drain the scripted updates, then cancel while the host holds.
        for _ in 0..6 {
            let frame = read_json_line(&mut replies).await;
            assert_eq!(frame["method"], "session/update");
        }
        send(
            &mut client_write,
            serde_json::json!({"jsonrpc":"2.0","method":"session/cancel","params":{"sessionId": session_id}}),
        )
        .await;
        let finished = read_json_line(&mut replies).await;
        assert_eq!(finished["id"], 2);
        assert_eq!(finished["result"]["stopReason"], "cancelled");
        assert!(host.cancelled.load(Ordering::SeqCst));

        handle.cancel();
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn rejects_cross_tenant_unknown_methods_and_concurrent_prompts_fail_closed() {
        let (client, server_io) = duplex(256 * 1024);
        let (client_read, mut client_write) = split(client);
        let (server_read, server_write) = split(server_io);
        let cancellation = CancellationToken::new();
        let handle = cancellation.handle();
        let mapper = Arc::new(Mutex::new(AcpSessionMapper::new()));
        // Seed a session owned by another tenant directly through the mapper.
        {
            let owner = ProtocolPrincipal::new("acp-user", ["acp:sessions:open"])
                .unwrap()
                .with_tenant("tenant-a")
                .unwrap();
            let mut mapper = mapper.try_lock().unwrap();
            let opened = mapper.prepare_new_session(
                Some("foreign-session"),
                CorrelationIdentity::new("corr-seed", "seed").unwrap(),
                Some(&owner),
            );
            assert!(opened.is_authorized());
        }
        let host = Arc::new(ScriptedHost {
            hold_for_cancel: Some(Arc::new(Notify::new())),
            cancelled: AtomicBool::new(false),
        });
        let server = tokio::spawn(serve_acp_stdio(
            mapper.clone(),
            full_scopes(),
            host.clone(),
            server_read,
            server_write,
            AcpStdioConfig::default(),
            cancellation,
        ));
        let mut replies = tokio::io::BufReader::new(client_read);

        // Parse failures and unknown methods answer fail-closed without touching state.
        client_write.write_all(b"{not json}\n").await.unwrap();
        let parse = read_json_line(&mut replies).await;
        assert_eq!(parse["error"]["code"], -32700);
        send(
            &mut client_write,
            serde_json::json!({"jsonrpc":"2.0","id":10,"method":"session/load","params":{}}),
        )
        .await;
        let unsupported = read_json_line(&mut replies).await;
        assert_eq!(unsupported["error"]["code"], -32601);

        // A session owned by another tenant is not promptable through this connection.
        send(
            &mut client_write,
            serde_json::json!({"jsonrpc":"2.0","id":11,"method":"session/prompt","params":{
                "sessionId": "foreign-session",
                "prompt": [{"type":"text","text":"steal"}]
            }}),
        )
        .await;
        let foreign = read_json_line(&mut replies).await;
        assert!(foreign.get("error").is_some(), "{foreign}");

        // A second prompt while one is active is a typed state conflict.
        send(
            &mut client_write,
            serde_json::json!({"jsonrpc":"2.0","id":12,"method":"session/new","params":{}}),
        )
        .await;
        let session = read_json_line(&mut replies).await;
        let session_id = session["result"]["sessionId"].as_str().unwrap().to_owned();
        send(
            &mut client_write,
            serde_json::json!({"jsonrpc":"2.0","id":13,"method":"session/prompt","params":{
                "sessionId": session_id,
                "prompt": [{"type":"text","text":"first"}]
            }}),
        )
        .await;
        for _ in 0..6 {
            let frame = read_json_line(&mut replies).await;
            assert_eq!(frame["method"], "session/update");
        }
        send(
            &mut client_write,
            serde_json::json!({"jsonrpc":"2.0","id":14,"method":"session/prompt","params":{
                "sessionId": session_id,
                "prompt": [{"type":"text","text":"second"}]
            }}),
        )
        .await;
        let conflict = read_json_line(&mut replies).await;
        assert_eq!(conflict["id"], 14);
        assert_eq!(conflict["error"]["code"], -32000);

        // Unsupported prompt block types are rejected instead of silently dropped.
        send(
            &mut client_write,
            serde_json::json!({"jsonrpc":"2.0","id":15,"method":"session/cancel","params":{"sessionId": session_id}}),
        )
        .await;
        let mut cancelled_seen = false;
        let mut cancel_ack_seen = false;
        while !(cancelled_seen && cancel_ack_seen) {
            let frame = read_json_line(&mut replies).await;
            if frame.get("id") == Some(&serde_json::json!(13)) {
                assert_eq!(frame["result"]["stopReason"], "cancelled");
                cancelled_seen = true;
            } else if frame.get("id") == Some(&serde_json::json!(15)) {
                cancel_ack_seen = true;
            }
        }
        send(
            &mut client_write,
            serde_json::json!({"jsonrpc":"2.0","id":16,"method":"session/prompt","params":{
                "sessionId": session_id,
                "prompt": [{"type":"audio","data":"zzz"}]
            }}),
        )
        .await;
        let bad_block = read_json_line(&mut replies).await;
        assert_eq!(bad_block["error"]["code"], -32602);

        handle.cancel();
        server.await.unwrap().unwrap();
    }
}
