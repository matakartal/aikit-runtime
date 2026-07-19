//! DeepSeek (OpenAI-compatible) wire ↔ canonical translation.
//!
//! DeepSeek speaks the OpenAI Chat Completions shape. The load-bearing difference from
//! Anthropic is that its reasoning-replay rule depends on tool use. An ordinary assistant turn
//! may drop prior `reasoning_content`, but an assistant turn containing tool calls must replay its
//! complete DeepSeek reasoning in subsequent requests. Foreign-provider reasoning is never sent.
//! Pure and keyless — testable without a network.

use super::{Provider, ProviderRequest};
use crate::error::{ProviderError, ProviderErrorKind};
use crate::types::{ContentBlock, Message, Role, StreamDelta, ToolSpec, Usage};
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::StreamExt;
use serde_json::{Map, Value};
use std::collections::BTreeMap;

/// Build a DeepSeek (OpenAI-compatible) `chat/completions` request body from canonical inputs.
/// Tool-call assistant turns replay their complete DeepSeek `reasoning_content`; ordinary turns
/// omit it because DeepSeek ignores it outside a tool-call continuation.
pub fn build_request(
    model: &str,
    max_tokens: u64,
    messages: &[Message],
    tools: &[ToolSpec],
    provider_options: Option<&serde_json::Map<String, Value>>,
) -> crate::error::Result<Value> {
    super::reject_protected_options(
        "deepseek",
        model,
        provider_options,
        &[
            "model",
            "messages",
            "max_tokens",
            "max_output_tokens",
            "max_completion_tokens",
            "tools",
            "stream",
            "stream_options",
        ],
    )?;
    if messages.iter().any(|message| {
        message
            .content
            .iter()
            .any(|block| matches!(block, ContentBlock::Media { .. }))
    }) {
        return Err(ProviderError::new(
            "deepseek",
            model,
            ProviderErrorKind::InvalidRequest,
            "DeepSeek adapter does not support media input; route to a vision-capable provider",
        )
        .into());
    }
    let mut wire: Vec<Value> = Vec::new();

    for m in messages {
        match m.role {
            Role::System => wire.push(serde_json::json!({
                "role": "system", "content": join_text(&m.content),
            })),
            Role::User => wire.push(serde_json::json!({
                "role": "user", "content": join_text(&m.content),
            })),
            Role::Assistant => {
                let text = join_text(&m.content);
                let tool_calls: Vec<Value> = m
                    .content
                    .iter()
                    .filter_map(|b| match b {
                        ContentBlock::ToolUse { id, name, input } => Some(serde_json::json!({
                            "id": id,
                            "type": "function",
                            "function": {
                                "name": name,
                                "arguments": serde_json::to_string(input).unwrap_or_else(|_| "{}".into()),
                            },
                        })),
                        _ => None,
                    })
                    .collect();

                let mut msg = serde_json::json!({ "role": "assistant" });
                // OpenAI allows null content when tool_calls are present.
                msg["content"] = if text.is_empty() {
                    Value::Null
                } else {
                    Value::String(text)
                };
                if !tool_calls.is_empty() {
                    msg["tool_calls"] = Value::Array(tool_calls);
                    if let Some(reasoning_content) = deepseek_reasoning_content(&m.content) {
                        msg["reasoning_content"] = Value::String(reasoning_content);
                    }
                }
                wire.push(msg);
            }
            Role::Tool => {
                // One `role: "tool"` message per tool result, keyed by tool_call_id.
                for b in &m.content {
                    if let ContentBlock::ToolResult {
                        tool_use_id,
                        content,
                        ..
                    } = b
                    {
                        wire.push(serde_json::json!({
                            "role": "tool",
                            "tool_call_id": tool_use_id,
                            "content": content,
                        }));
                    }
                }
            }
        }
    }

    let mut req = serde_json::json!({
        "model": model,
        "max_tokens": max_tokens,
        "messages": wire,
        "stream": true,
        // DeepSeek only emits the terminal usage-only chunk when this is explicitly enabled.
        "stream_options": { "include_usage": true },
    });
    if !tools.is_empty() {
        req["tools"] = Value::Array(
            tools
                .iter()
                .map(|t| {
                    serde_json::json!({
                        "type": "function",
                        "function": {
                            "name": t.name,
                            "description": t.description,
                            "parameters": t.input_schema,
                        },
                    })
                })
                .collect(),
        );
    }
    if let (Some(opts), Value::Object(map)) = (provider_options, &mut req) {
        for (k, v) in opts {
            map.insert(k.clone(), v.clone());
        }
    }
    Ok(req)
}

/// Collapse canonical DeepSeek reasoning blocks into the single wire field without translating
/// state from another provider. Untagged blocks remain accepted for legacy single-provider
/// transcripts; current runtime records always carry the provider name.
fn deepseek_reasoning_content(blocks: &[ContentBlock]) -> Option<String> {
    let mut replay = None::<String>;
    for block in blocks {
        if let ContentBlock::Reasoning { text, provider, .. } = block {
            if provider
                .as_deref()
                .is_some_and(|provider| provider != "deepseek")
            {
                continue;
            }
            replay.get_or_insert_with(String::new).push_str(text);
        }
    }
    replay
}

fn join_text(blocks: &[ContentBlock]) -> String {
    blocks
        .iter()
        .filter_map(|b| match b {
            ContentBlock::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("")
}

/// Accumulator for one streamed tool call (OpenAI groups by `index`; id/name/args arrive in
/// fragments across chunks).
#[derive(Default)]
struct ToolCallAccum {
    id: String,
    name: String,
    args: String,
}

/// Stateful translator for DeepSeek/OpenAI-compatible streaming chunks → canonical deltas.
/// Feed decoded `data:` chunk objects via [`DeepSeekStreamParser::push_chunk`], then call
/// [`DeepSeekStreamParser::finish`] on `[DONE]`.
#[derive(Default)]
pub struct DeepSeekStreamParser {
    started: bool,
    terminal: bool,
    reasoning_content: String,
    reasoning_emitted: bool,
    tool_calls: BTreeMap<u64, ToolCallAccum>,
    tool_calls_emitted: bool,
    stop_reason: String,
    usage: Usage,
    metadata: Map<String, Value>,
    retention: super::StreamRetentionBudget,
}

impl DeepSeekStreamParser {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_terminal(&self) -> bool {
        self.terminal
    }

    pub fn push_chunk(&mut self, chunk: &Value) -> Vec<StreamDelta> {
        if self.terminal {
            return Vec::new();
        }
        let mut out = Vec::new();

        if !self.capture_response_metadata(chunk) {
            return self.retained_state_failure();
        }

        if !self.started {
            self.started = true;
            let model = chunk
                .get("model")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            out.push(StreamDelta::MessageStart { model });
        }

        if let Some(u) = chunk.get("usage").filter(|u| u.is_object()) {
            if !self.absorb_usage(u) {
                return self.retained_state_failure();
            }
        }

        let choice = &chunk["choices"][0];
        let delta = &choice["delta"];

        if let Some(index) = choice.get("index") {
            if !self.retain_metadata_value("choice_index", index) {
                return self.retained_state_failure();
            }
            self.metadata.insert("choice_index".into(), index.clone());
        }
        if let Some(logprobs) = choice.get("logprobs").filter(|value| !value.is_null()) {
            if !self.retention.retain_json(logprobs, 1) {
                return self.retained_state_failure();
            }
            self.metadata
                .entry("logprobs")
                .or_insert_with(|| Value::Array(Vec::new()))
                .as_array_mut()
                .expect("logprobs metadata is initialized as an array")
                .push(logprobs.clone());
        }

        if let Some(text) = delta.get("content").and_then(Value::as_str) {
            if !text.is_empty() {
                out.push(StreamDelta::TextDelta { text: text.into() });
            }
        }
        if let Some(rc) = delta.get("reasoning_content").and_then(Value::as_str) {
            if !rc.is_empty() {
                if !self.retention.retain(rc.len(), 0) {
                    return self.retained_state_failure();
                }
                self.reasoning_content.push_str(rc);
                out.push(StreamDelta::ReasoningDelta { text: rc.into() });
            }
        }
        if let Some(tcs) = delta.get("tool_calls").and_then(Value::as_array) {
            for tc in tcs {
                let index = tc.get("index").and_then(Value::as_u64).unwrap_or(0);
                let id = tc.get("id").and_then(Value::as_str).unwrap_or_default();
                let name = tc["function"]
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let args = tc["function"]
                    .get("arguments")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let new_call = usize::from(!self.tool_calls.contains_key(&index));
                if !self.retention.retain(
                    id.len()
                        .saturating_add(name.len())
                        .saturating_add(args.len()),
                    new_call,
                ) {
                    return self.retained_state_failure();
                }
                let entry = self.tool_calls.entry(index).or_default();
                if !id.is_empty() {
                    entry.id = id.to_string();
                }
                if !name.is_empty() {
                    entry.name = name.to_string();
                }
                if !args.is_empty() {
                    entry.args.push_str(args);
                }
            }
        }

        if let Some(fr) = choice.get("finish_reason").and_then(Value::as_str) {
            let bytes = fr.len().saturating_mul(2);
            let items = usize::from(!self.metadata.contains_key("finish_reason"));
            if !self.retention.retain(bytes, items) {
                return self.retained_state_failure();
            }
            self.metadata
                .insert("finish_reason".into(), Value::String(fr.into()));
            self.stop_reason = map_finish_reason(fr);
            out.extend(self.emit_reasoning_complete());
            out.extend(self.emit_tool_calls());
        }

        out
    }

    /// Emit the complete reasoning block exactly once so the runtime can persist and replay it
    /// when this assistant turn contains tool calls.
    fn emit_reasoning_complete(&mut self) -> Vec<StreamDelta> {
        if self.reasoning_emitted || self.reasoning_content.is_empty() {
            return Vec::new();
        }
        self.reasoning_emitted = true;
        vec![StreamDelta::ReasoningComplete {
            text: std::mem::take(&mut self.reasoning_content),
            signature: None,
            opaque: None,
        }]
    }

    /// Emit accumulated tool calls once (ToolCallStart + ToolCallInput per call, in index order).
    fn emit_tool_calls(&mut self) -> Vec<StreamDelta> {
        if self.tool_calls_emitted || self.tool_calls.is_empty() {
            return Vec::new();
        }
        if let Some(message) = self.tool_calls.values().find_map(|call| {
            if call.id.is_empty() {
                Some("DeepSeek tool call is missing its id".to_string())
            } else if call.name.is_empty() {
                Some(format!(
                    "DeepSeek tool call {} is missing its name",
                    call.id
                ))
            } else if !call.args.trim().is_empty()
                && serde_json::from_str::<Value>(&call.args).is_err()
            {
                Some(format!("malformed tool_call arguments for {}", call.id))
            } else {
                None
            }
        }) {
            return self.terminal_protocol_failure(message);
        }
        self.tool_calls_emitted = true;
        let mut out = Vec::new();
        for accum in self.tool_calls.values() {
            out.push(StreamDelta::ToolCallStart {
                id: accum.id.clone(),
                name: accum.name.clone(),
            });
            if accum.args.trim().is_empty() {
                // Empty args are a valid, empty object — never a parse failure.
                out.push(StreamDelta::ToolCallInput {
                    id: accum.id.clone(),
                    input: Value::Object(Default::default()),
                });
            } else if let Ok(input) = serde_json::from_str::<Value>(&accum.args) {
                out.push(StreamDelta::ToolCallInput {
                    id: accum.id.clone(),
                    input,
                });
            }
        }
        self.tool_calls.clear();
        out
    }

    /// Call on `[DONE]` / stream end: flush any un-emitted tool calls, then Usage + MessageStop.
    pub fn finish(&mut self) -> Vec<StreamDelta> {
        if self.terminal {
            return Vec::new();
        }
        if self.stop_reason.is_empty() {
            return self.terminal_protocol_failure("DeepSeek [DONE] arrived before finish_reason");
        }
        let mut out = self.emit_reasoning_complete();
        out.extend(self.emit_tool_calls());
        if self.terminal {
            return out;
        }
        self.terminal = true;
        if !self.metadata.is_empty() {
            out.push(StreamDelta::ProviderMetadata {
                provider: "deepseek".into(),
                metadata: Value::Object(std::mem::take(&mut self.metadata)),
            });
        }
        out.push(StreamDelta::Usage(self.usage));
        let stop_reason = if self.stop_reason.is_empty() {
            "end_turn".to_string()
        } else {
            std::mem::take(&mut self.stop_reason)
        };
        out.push(StreamDelta::MessageStop { stop_reason });
        out
    }

    fn absorb_usage(&mut self, u: &Value) -> bool {
        if let Some(raw) = u.as_object().filter(|raw| !raw.is_empty()) {
            if !self.retention.retain_json(u, 1) {
                return false;
            }
            self.metadata
                .entry("usage")
                .or_insert_with(|| Value::Array(Vec::new()))
                .as_array_mut()
                .expect("usage metadata is initialized as an array")
                .push(Value::Object(raw.clone()));
        }
        if let Some(n) = u.get("prompt_tokens").and_then(Value::as_u64) {
            self.usage.input_tokens = n;
        }
        if let Some(n) = u.get("completion_tokens").and_then(Value::as_u64) {
            self.usage.output_tokens = n;
        }
        // DeepSeek reports reasoning tokens nested under completion_tokens_details.
        if let Some(n) = u
            .get("completion_tokens_details")
            .and_then(|d| d.get("reasoning_tokens"))
            .and_then(Value::as_u64)
        {
            self.usage.reasoning_tokens = n;
        }
        if let Some(n) = u.get("prompt_cache_hit_tokens").and_then(Value::as_u64) {
            self.usage.cache_read_input_tokens = n;
        }
        true
    }

    fn capture_response_metadata(&mut self, chunk: &Value) -> bool {
        for field in [
            "id",
            "object",
            "created",
            "model",
            "system_fingerprint",
            "service_tier",
        ] {
            if let Some(value) = chunk.get(field) {
                if !self.retain_metadata_value(field, value) {
                    return false;
                }
                self.metadata.insert(field.into(), value.clone());
            }
        }
        true
    }

    fn retain_metadata_value(&mut self, key: &str, value: &Value) -> bool {
        self.retention.retain(
            key.len().saturating_add(super::json_retained_bytes(value)),
            usize::from(!self.metadata.contains_key(key)),
        )
    }

    fn retained_state_failure(&mut self) -> Vec<StreamDelta> {
        self.terminal = true;
        self.reasoning_content.clear();
        self.tool_calls.clear();
        self.stop_reason.clear();
        self.metadata.clear();
        vec![super::retained_state_failure("deepseek")]
    }

    fn terminal_protocol_failure(&mut self, message: impl Into<String>) -> Vec<StreamDelta> {
        self.terminal = true;
        self.reasoning_content.clear();
        self.tool_calls.clear();
        self.stop_reason.clear();
        self.metadata.clear();
        vec![super::protocol_failure("deepseek", message)]
    }
}

fn map_finish_reason(fr: &str) -> String {
    match fr {
        "stop" => "end_turn",
        "tool_calls" => "tool_use",
        "length" => "max_tokens",
        other => other,
    }
    .to_string()
}

const DEEPSEEK_DEFAULT_BASE: &str = "https://api.deepseek.com";

/// Live DeepSeek (OpenAI-compatible) adapter: `build_request` → POST (SSE) →
/// `DeepSeekStreamParser` → canonical [`StreamDelta`]s. `base_url` is overridable for tests.
pub struct DeepSeekProvider {
    api_key: String,
    base_url: String,
    client: reqwest::Client,
}

impl DeepSeekProvider {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self::with_base_url(api_key, DEEPSEEK_DEFAULT_BASE)
    }

    pub fn with_base_url(api_key: impl Into<String>, base_url: impl Into<String>) -> Self {
        DeepSeekProvider {
            api_key: api_key.into(),
            base_url: base_url.into(),
            client: super::native_http_client(),
        }
    }
}

#[async_trait]
impl Provider for DeepSeekProvider {
    fn name(&self) -> &str {
        "deepseek"
    }

    async fn stream(
        &self,
        req: ProviderRequest,
    ) -> crate::error::Result<BoxStream<'static, StreamDelta>> {
        let options = req.options_for(self.name());
        let body = build_request(
            &req.model,
            req.max_tokens,
            &req.messages,
            &req.tools,
            Some(&options),
        )
        .map_err(|error| {
            crate::error::ProviderError::new(
                "deepseek",
                &req.model,
                crate::error::ProviderErrorKind::InvalidRequest,
                error.to_string(),
            )
        })?;
        let url = format!("{}/chat/completions", self.base_url.trim_end_matches('/'));
        let resp = self
            .client
            .post(&url)
            .header("authorization", format!("Bearer {}", self.api_key))
            .header("accept", "text/event-stream")
            .json(&body)
            .send()
            .await
            .map_err(|error| super::transport_failure("deepseek", &req.model, error))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let retry_after = resp.headers().get(reqwest::header::RETRY_AFTER).cloned();
            let text = super::read_error_body(resp).await;
            return Err(super::http_failure(
                "deepseek",
                &req.model,
                status,
                retry_after.as_ref(),
                text,
            ));
        }

        let model = req.model.clone();
        let mut byte_stream = resp.bytes_stream().boxed();
        let out = async_stream::stream! {
            let mut parser = DeepSeekStreamParser::new();
            let mut buf: Vec<u8> = Vec::new();
            let mut done = false;
            while let Some(chunk) = byte_stream.next().await {
                match chunk {
                    Ok(bytes) => {
                        if !super::append_sse_chunk(&mut buf, &bytes) {
                            yield super::stream_failure(
                                "deepseek",
                                &model,
                                crate::error::ProviderErrorKind::Protocol,
                                "DeepSeek SSE event exceeded the size limit",
                            );
                            return;
                        }
                        while let Some(pos) = buf.iter().position(|&b| b == b'\n') {
                            let line_bytes: Vec<u8> = buf.drain(..=pos).collect();
                            let line = String::from_utf8_lossy(&line_bytes);
                            if let Some(data) = line.trim().strip_prefix("data:") {
                                let data = data.trim();
                                if data.is_empty() {
                                    continue;
                                }
                                if data == "[DONE]" {
                                    for d in parser.finish() {
                                        yield super::with_stream_context(d, "deepseek", &model);
                                    }
                                    done = true;
                                    break;
                                }
                                match serde_json::from_str::<Value>(data) {
                                    Ok(json) => {
                                        for d in parser.push_chunk(&json) {
                                            yield super::with_stream_context(d, "deepseek", &model);
                                        }
                                        if parser.is_terminal() {
                                            return;
                                        }
                                    }
                                    Err(_) => {
                                        yield super::stream_failure(
                                            "deepseek",
                                            &model,
                                            crate::error::ProviderErrorKind::Protocol,
                                            "malformed DeepSeek SSE data",
                                        );
                                        return;
                                    }
                                }
                            }
                        }
                        if done {
                            break;
                        }
                    }
                    Err(error) => {
                        yield super::response_stream_failure(
                            "deepseek",
                            &model,
                            error,
                            "DeepSeek",
                        );
                        return;
                    }
                }
            }
            // `[DONE]` is the protocol commit marker; a clean EOF before it is still truncated.
            if !done {
                yield super::stream_failure(
                    "deepseek",
                    &model,
                    crate::error::ProviderErrorKind::Protocol,
                    "DeepSeek stream ended before [DONE]",
                );
            }
        };
        Ok(Box::pin(out))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn media_input_fails_instead_of_silently_disappearing() {
        let messages = vec![Message {
            role: Role::User,
            content: vec![ContentBlock::Media {
                media_type: "image/png".into(),
                source: crate::types::MediaSource::Url {
                    url: "https://example.test/a.png".into(),
                },
            }],
        }];
        let error = build_request("deepseek-chat", 100, &messages, &[], None).unwrap_err();
        assert!(matches!(
            error.provider_error(),
            Some(error) if error.kind == ProviderErrorKind::InvalidRequest
        ));
    }

    #[test]
    fn build_request_replays_deepseek_reasoning_for_tool_calls() {
        let messages = vec![
            Message::system("You are helpful."),
            Message::user("search merhaba"),
            Message {
                role: Role::Assistant,
                content: vec![
                    ContentBlock::Reasoning {
                        text: "reasoning that must be replayed exactly".into(),
                        signature: None,
                        provider: Some("deepseek".into()),
                        opaque: None,
                    },
                    ContentBlock::Reasoning {
                        text: "foreign reasoning must never cross providers".into(),
                        signature: Some("anthropic-signature".into()),
                        provider: Some("anthropic".into()),
                        opaque: None,
                    },
                    ContentBlock::ToolUse {
                        id: "call_1".into(),
                        name: "search".into(),
                        input: json!({ "q": "merhaba" }),
                    },
                ],
            },
            Message {
                role: Role::Tool,
                content: vec![ContentBlock::ToolResult {
                    tool_use_id: "call_1".into(),
                    content: "3 results".into(),
                    is_error: false,
                }],
            },
            Message::user("now answer a subsequent question"),
        ];
        let tools = vec![ToolSpec {
            name: "search".into(),
            description: "search the web".into(),
            input_schema: json!({ "type": "object" }),
        }];

        let req = build_request("deepseek-reasoner", 2048, &messages, &tools, None).unwrap();

        let msgs = req["messages"].as_array().unwrap();
        // The tool-call assistant remains intact even in a later user interaction turn.
        assert_eq!(msgs.len(), 5);

        // Thinking-mode tool calls require the complete DeepSeek reasoning to be replayed.
        let assistant = &msgs[2];
        assert_eq!(assistant["role"], "assistant");
        assert_eq!(
            assistant["reasoning_content"],
            "reasoning that must be replayed exactly"
        );
        let serialized = serde_json::to_string(&req).unwrap();
        assert!(!serialized.contains("foreign reasoning"));
        assert!(!serialized.contains("anthropic-signature"));

        // Assistant maps ToolUse → OpenAI tool_calls with stringified arguments.
        assert_eq!(assistant["tool_calls"][0]["id"], "call_1");
        assert_eq!(assistant["tool_calls"][0]["type"], "function");
        assert_eq!(assistant["tool_calls"][0]["function"]["name"], "search");
        assert_eq!(
            assistant["tool_calls"][0]["function"]["arguments"],
            "{\"q\":\"merhaba\"}"
        );

        // Tool result → a role:"tool" message keyed by tool_call_id.
        assert_eq!(msgs[3]["role"], "tool");
        assert_eq!(msgs[3]["tool_call_id"], "call_1");
        assert_eq!(msgs[4]["role"], "user");

        // Tools mapped to the function shape.
        assert_eq!(req["tools"][0]["type"], "function");
        assert_eq!(req["tools"][0]["function"]["name"], "search");
    }

    #[test]
    fn build_request_drops_reasoning_without_tool_calls() {
        let messages = vec![
            Message::user("answer normally"),
            Message {
                role: Role::Assistant,
                content: vec![
                    ContentBlock::Reasoning {
                        text: "this prior reasoning may be dropped".into(),
                        signature: None,
                        provider: Some("deepseek".into()),
                        opaque: None,
                    },
                    ContentBlock::Text {
                        text: "final answer".into(),
                    },
                ],
            },
        ];

        let req = build_request("deepseek-reasoner", 256, &messages, &[], None).unwrap();
        let assistant = &req["messages"][1];
        assert_eq!(assistant["content"], "final answer");
        assert!(assistant.get("reasoning_content").is_none());
    }

    #[test]
    fn request_forces_usage_streaming_and_rejects_contract_overrides() {
        let request =
            build_request("deepseek-chat", 100, &[Message::user("hello")], &[], None).unwrap();
        assert_eq!(request["stream"], true);
        assert_eq!(request["stream_options"]["include_usage"], true);

        for (key, value) in [
            ("model", json!("other-model")),
            ("messages", json!([])),
            ("max_tokens", json!(1)),
            ("max_output_tokens", json!(1)),
            ("max_completion_tokens", json!(1)),
            ("tools", json!([])),
            ("stream", json!(false)),
            ("stream_options", json!({ "include_usage": false })),
        ] {
            let options = serde_json::Map::from_iter([(key.to_string(), value)]);
            let error = build_request(
                "deepseek-chat",
                100,
                &[Message::user("hello")],
                &[],
                Some(&options),
            )
            .unwrap_err();
            let error = error.provider_error().expect("typed provider error");
            assert_eq!(error.kind, ProviderErrorKind::InvalidRequest);
            assert!(error.message.contains(key));
        }

        let options = serde_json::Map::from_iter([("temperature".into(), json!(0.1))]);
        let request = build_request(
            "deepseek-chat",
            100,
            &[Message::user("hello")],
            &[],
            Some(&options),
        )
        .unwrap();
        assert_eq!(request["temperature"], 0.1);
        assert_eq!(request["stream_options"]["include_usage"], true);

        let options = serde_json::Map::from_iter([("tool_choice".into(), json!("required"))]);
        let request = build_request(
            "deepseek-chat",
            100,
            &[Message::user("hello")],
            &[],
            Some(&options),
        )
        .unwrap();
        assert_eq!(request["tool_choice"], "required");
        assert_eq!(request["stream_options"]["include_usage"], true);
    }

    #[test]
    fn stream_parser_handles_reasoning_content_and_fragmented_tool_calls() {
        let chunks = vec![
            json!({
                "id":"chatcmpl_123",
                "object":"chat.completion.chunk",
                "created":1720000000,
                "model":"deepseek-reasoner",
                "system_fingerprint":"fp_123",
                "choices":[{"index":0,"delta":{"role":"assistant","reasoning_content":"Let me think."},"finish_reason":null}]
            }),
            json!({"choices":[{"delta":{"reasoning_content":" I'll search."},"finish_reason":null}]}),
            json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1","function":{"name":"search","arguments":"{\"q\":"}}]},"finish_reason":null}]}),
            json!({"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"merhaba\"}"}}]},"finish_reason":null}]}),
            json!({"choices":[{"index":0,"delta":{},"finish_reason":"tool_calls","logprobs":{"content":[{"token":"search","logprob":-0.1}]}}]}),
            json!({"choices":[],"usage":{"prompt_tokens":50,"completion_tokens":20,"prompt_cache_hit_tokens":7,"prompt_cache_miss_tokens":43,"completion_tokens_details":{"reasoning_tokens":12}}}),
        ];
        let mut p = DeepSeekStreamParser::new();
        let mut out: Vec<StreamDelta> = Vec::new();
        for c in &chunks {
            out.extend(p.push_chunk(c));
        }
        out.extend(p.finish());

        assert_eq!(
            out[0],
            StreamDelta::MessageStart {
                model: "deepseek-reasoner".into()
            }
        );
        // reasoning_content surfaces as canonical reasoning deltas.
        assert!(out.contains(&StreamDelta::ReasoningDelta {
            text: "Let me think.".into()
        }));
        assert!(out.contains(&StreamDelta::ReasoningComplete {
            text: "Let me think. I'll search.".into(),
            signature: None,
            opaque: None,
        }));
        // Fragmented tool-call arguments reassemble into a real object.
        assert!(out.contains(&StreamDelta::ToolCallStart {
            id: "call_1".into(),
            name: "search".into()
        }));
        assert!(out.contains(&StreamDelta::ToolCallInput {
            id: "call_1".into(),
            input: json!({ "q": "merhaba" }),
        }));
        // finish_reason tool_calls → canonical tool_use, with usage incl. reasoning tokens.
        assert!(out.contains(&StreamDelta::MessageStop {
            stop_reason: "tool_use".into()
        }));
        assert!(out.contains(&StreamDelta::Usage(Usage {
            input_tokens: 50,
            output_tokens: 20,
            cache_read_input_tokens: 7,
            reasoning_tokens: 12,
            ..Default::default()
        })));
        let metadata = out
            .iter()
            .find_map(|delta| match delta {
                StreamDelta::ProviderMetadata { provider, metadata } if provider == "deepseek" => {
                    Some(metadata)
                }
                _ => None,
            })
            .expect("deepseek provider metadata");
        assert_eq!(metadata["id"], "chatcmpl_123");
        assert_eq!(metadata["system_fingerprint"], "fp_123");
        assert_eq!(metadata["finish_reason"], "tool_calls");
        assert_eq!(metadata["usage"][0]["prompt_cache_hit_tokens"], 7);
        assert_eq!(metadata["logprobs"][0]["content"][0]["token"], "search");
        assert!(metadata.get("choices").is_none());
    }

    #[test]
    fn stream_parser_surfaces_malformed_tool_call_arguments_as_error() {
        // Fragments reassemble into `{"q":` — invalid JSON (no closing). The parser must NOT
        // coerce this to null and run the tool with it; it must emit an Error delta instead.
        let chunks = vec![
            json!({"model":"deepseek-reasoner","choices":[{"delta":{"role":"assistant","tool_calls":[{"index":0,"id":"call_bad","function":{"name":"search","arguments":"{\"q\":"}}]},"finish_reason":null}]}),
            json!({"choices":[{"delta":{},"finish_reason":"tool_calls"}]}),
        ];
        let mut p = DeepSeekStreamParser::new();
        let mut out: Vec<StreamDelta> = Vec::new();
        for c in &chunks {
            out.extend(p.push_chunk(c));
        }
        out.extend(p.finish());

        // The malformed arguments surface as an Error delta mentioning "malformed".
        assert!(out.iter().any(|d| matches!(
            d,
            StreamDelta::Error { message, .. } if message.contains("malformed")
        )));
        // Crucially, no ToolCallInput with a null input ever reaches the loop.
        assert!(!out.iter().any(|d| matches!(
            d,
            StreamDelta::ToolCallInput { input, .. } if input.is_null()
        )));
    }

    #[test]
    fn many_small_reasoning_fragments_fail_terminally() {
        let mut parser = DeepSeekStreamParser::new();
        parser.retention = crate::providers::StreamRetentionBudget::with_limits(8, 8);
        let fragment = json!({
            "choices": [{"delta": {"reasoning_content": "x"}, "finish_reason": null}]
        });
        let failure = (0..16)
            .find_map(|_| {
                parser
                    .push_chunk(&fragment)
                    .into_iter()
                    .find(|delta| matches!(delta, StreamDelta::Error { .. }))
            })
            .expect("many small reasoning fragments must exceed retained state");
        assert!(
            matches!(failure, StreamDelta::Error { message, .. } if message.contains("retained parser-state"))
        );
        assert!(parser.terminal);
        assert!(parser.reasoning_content.is_empty());
        assert!(parser.push_chunk(&fragment).is_empty());
        assert!(parser.finish().is_empty());
    }

    #[test]
    fn done_without_finish_reason_is_a_terminal_protocol_failure() {
        let mut parser = DeepSeekStreamParser::new();
        let out = parser.finish();
        assert!(out.iter().any(|delta| matches!(
            delta,
            StreamDelta::Error { message, .. } if message.contains("before finish_reason")
        )));
        assert!(!out
            .iter()
            .any(|delta| matches!(delta, StreamDelta::MessageStop { .. })));
    }

    #[test]
    fn tool_call_without_id_fails_before_emission() {
        let mut parser = DeepSeekStreamParser::new();
        let out = parser.push_chunk(&json!({
            "choices": [{
                "delta": {"tool_calls": [{"index": 0, "function": {"name": "search", "arguments": "{}"}}]},
                "finish_reason": "tool_calls"
            }]
        }));
        assert!(out.iter().any(|delta| matches!(
            delta,
            StreamDelta::Error { message, .. } if message.contains("missing its id")
        )));
        assert!(!out
            .iter()
            .any(|delta| matches!(delta, StreamDelta::ToolCallStart { .. })));
        assert!(parser.terminal);
    }

    #[tokio::test]
    async fn provider_streams_openai_compatible_sse_over_real_http() {
        use futures::StreamExt;
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let sse = concat!(
            "data: {\"model\":\"deepseek-reasoner\",\"choices\":[{\"delta\":{\"role\":\"assistant\",\"content\":\"Merhaba\"},\"finish_reason\":null}]}\n\n",
            "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
            "data: [DONE]\n\n",
        );
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_raw(sse, "text/event-stream"))
            .mount(&server)
            .await;

        let provider = DeepSeekProvider::with_base_url("sk-ds-test", server.uri());
        let req = ProviderRequest {
            model: "deepseek-reasoner".into(),
            messages: vec![Message::user("selam")],
            tools: vec![],
            max_tokens: 100,
            options: serde_json::Map::new(),
            provider_options: crate::types::ProviderOptions::new(),
        };
        let out: Vec<StreamDelta> = provider.stream(req).await.unwrap().collect().await;

        assert!(out.contains(&StreamDelta::MessageStart {
            model: "deepseek-reasoner".into()
        }));
        assert!(out.contains(&StreamDelta::TextDelta {
            text: "Merhaba".into()
        }));
        assert!(out.contains(&StreamDelta::MessageStop {
            stop_reason: "end_turn".into()
        }));

        let requests = server.received_requests().await.unwrap();
        let body: Value = serde_json::from_slice(&requests[0].body).unwrap();
        assert_eq!(body["stream"], true);
        assert_eq!(body["stream_options"]["include_usage"], true);
    }

    #[tokio::test]
    async fn runtime_rejects_attempt_to_disable_usage_streaming() {
        use wiremock::MockServer;

        let server = MockServer::start().await;
        let provider = DeepSeekProvider::with_base_url("sk-ds-test", server.uri());
        let mut options = crate::types::ProviderOptions::new();
        options.insert(
            "deepseek".into(),
            serde_json::Map::from_iter([(
                "stream_options".into(),
                json!({ "include_usage": false }),
            )]),
        );
        let error = provider
            .stream(ProviderRequest {
                model: "deepseek-chat".into(),
                messages: vec![Message::user("hello")],
                tools: vec![],
                max_tokens: 100,
                options: serde_json::Map::new(),
                provider_options: options,
            })
            .await
            .err()
            .expect("invalid request");
        let error = error.provider_error().expect("typed provider error");
        assert_eq!(error.kind, ProviderErrorKind::InvalidRequest);
        assert!(server.received_requests().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn clean_eof_before_done_is_a_protocol_failure() {
        use futures::StreamExt;
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_raw(
                "data: {\"model\":\"deepseek-chat\",\"choices\":[{\"delta\":{\"content\":\"partial\"},\"finish_reason\":\"stop\"}]}\n\n",
                "text/event-stream",
            ))
            .mount(&server)
            .await;

        let provider = DeepSeekProvider::with_base_url("sk-ds-test", server.uri());
        let out: Vec<_> = provider
            .stream(ProviderRequest {
                model: "deepseek-chat".into(),
                messages: vec![Message::user("hello")],
                tools: vec![],
                max_tokens: 100,
                options: serde_json::Map::new(),
                provider_options: crate::types::ProviderOptions::new(),
            })
            .await
            .unwrap()
            .collect()
            .await;

        assert!(!out
            .iter()
            .any(|delta| matches!(delta, StreamDelta::MessageStop { .. })));
        assert!(matches!(
            out.last(),
            Some(StreamDelta::Error { info, .. })
                if info.code == crate::error::ErrorCode::ProviderProtocol
        ));
    }
}
