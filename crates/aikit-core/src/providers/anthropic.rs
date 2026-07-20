//! Anthropic Messages API wire ↔ canonical translation.
//!
//! This module owns the *serialization boundary* that makes proof-point #1 possible: it maps
//! Anthropic's streaming SSE events to canonical [`StreamDelta`]s, keeping `thinking` blocks'
//! `signature` intact so they can be replayed verbatim (see `crate::reasoning`). The parser is
//! a pure state machine — no network, no key — so it is fully testable against documented
//! event fixtures. The reqwest transport that feeds it live bytes lands in a later phase.
//!
//! Event shapes (Anthropic Messages API streaming): `message_start` → `content_block_start`
//! → `content_block_delta`* → `content_block_stop` (per block) → `message_delta` →
//! `message_stop`, plus `ping` and `error`.

use super::{Provider, ProviderRequest};
use crate::types::{ContentBlock, MediaSource, Message, Role, StreamDelta, ToolSpec, Usage};
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::StreamExt;
use serde_json::{Map, Value};
use std::collections::HashMap;

/// Build an Anthropic Messages API request body from canonical inputs. The serialize
/// counterpart of [`AnthropicStreamParser`], and the *request* side of proof-point #1:
/// assistant `Reasoning` blocks are re-emitted as `thinking` blocks **with their signature
/// intact** (dropping/editing it → 400). `provider_options` is the typed, non-lossy escape
/// hatch — its keys (e.g. `thinking`, `cache_control`, `output_config`) are merged verbatim
/// onto the request, never flattened away. Pure and keyless — testable without a network.
pub fn build_request(
    model: &str,
    max_tokens: u64,
    messages: &[Message],
    tools: &[ToolSpec],
    provider_options: Option<&serde_json::Map<String, Value>>,
) -> crate::error::Result<Value> {
    super::reject_protected_options(
        "anthropic",
        model,
        provider_options,
        &[
            "model",
            "messages",
            "system",
            "max_tokens",
            "max_output_tokens",
            "max_completion_tokens",
            "tools",
            "stream",
            "stream_options",
        ],
    )?;
    super::validate_media_input_roles(messages, "anthropic", model)?;
    let mut system: Vec<Value> = Vec::new();
    let mut wire: Vec<Value> = Vec::new();

    for m in messages {
        match m.role {
            // Anthropic takes the system prompt at the top level, not as a message.
            Role::System => {
                for b in &m.content {
                    if let ContentBlock::Text { text } = b {
                        system.push(serde_json::json!({ "type": "text", "text": text }));
                    }
                }
            }
            // Anthropic has no "tool" role — tool results ride in a user message.
            Role::Tool => wire.push(serde_json::json!({
                "role": "user",
                "content": map_blocks(model, &m.content)?,
            })),
            Role::User => wire.push(serde_json::json!({
                "role": "user",
                "content": map_blocks(model, &m.content)?,
            })),
            Role::Assistant => wire.push(serde_json::json!({
                "role": "assistant",
                "content": map_blocks(model, &m.content)?,
            })),
        }
    }

    let mut req = serde_json::json!({
        "model": model,
        "max_tokens": max_tokens,
        "messages": wire,
        "stream": true,
    });
    if !system.is_empty() {
        req["system"] = Value::Array(system);
    }
    if !tools.is_empty() {
        req["tools"] = Value::Array(
            tools
                .iter()
                .map(|t| {
                    serde_json::json!({
                        "name": t.name,
                        "description": t.description,
                        "input_schema": t.input_schema,
                    })
                })
                .collect(),
        );
    }
    // Typed escape hatch: merge provider_options verbatim (thinking, cache_control, ...).
    if let (Some(opts), Value::Object(map)) = (provider_options, &mut req) {
        for (k, v) in opts {
            map.insert(k.clone(), v.clone());
        }
    }
    Ok(req)
}

/// Map canonical content blocks to Anthropic wire blocks.
fn map_blocks(model: &str, blocks: &[ContentBlock]) -> crate::error::Result<Vec<Value>> {
    let mut mapped = Vec::new();
    for block in blocks {
        let value = match block {
            ContentBlock::Text { text } => {
                Some(serde_json::json!({ "type": "text", "text": text }))
            }
            // Replay reasoning as a signed `thinking` block — signature MUST survive. A block
            // carrying only opaque data (no signature/text) is redacted_thinking → replay verbatim.
            ContentBlock::Reasoning {
                text,
                signature,
                provider,
                opaque,
            } => {
                if provider
                    .as_deref()
                    .is_some_and(|source| source != "anthropic")
                {
                    None
                } else if signature.is_none() && text.is_empty() {
                    match opaque.as_ref() {
                        Some(Value::String(data)) => {
                            Some(serde_json::json!({ "type": "redacted_thinking", "data": data }))
                        }
                        _ => Some(serde_json::json!({ "type": "thinking", "thinking": text })),
                    }
                } else {
                    let mut value = serde_json::json!({ "type": "thinking", "thinking": text });
                    if let Some(signature) = signature {
                        value["signature"] = Value::String(signature.clone());
                    }
                    Some(value)
                }
            }
            ContentBlock::ToolUse { id, name, input } => Some(serde_json::json!({
                "type": "tool_use", "id": id, "name": name, "input": input,
            })),
            ContentBlock::ToolResult {
                tool_use_id,
                content,
                is_error,
            } => Some(serde_json::json!({
                "type": "tool_result", "tool_use_id": tool_use_id, "content": content, "is_error": is_error,
            })),
            ContentBlock::Media { media_type, source } => {
                let src = match source {
                    MediaSource::Url { url } => serde_json::json!({ "type": "url", "url": url }),
                    MediaSource::Base64 { data } => serde_json::json!({
                        "type": "base64", "media_type": media_type, "data": data,
                    }),
                };
                Some(serde_json::json!({ "type": "image", "source": src }))
            }
            ContentBlock::MediaInput { media } => {
                if !super::is_image_media_type(&media.media_type) {
                    return Err(crate::error::ProviderError::new(
                        "anthropic",
                        model,
                        crate::error::ProviderErrorKind::InvalidRequest,
                        "Anthropic message media input currently supports image MIME types only",
                    )
                    .into());
                }
                let source = match super::resolve_media_input(media, "anthropic", model)? {
                    super::ResolvedMediaInput::Base64(data) => serde_json::json!({
                        "type": "base64",
                        "media_type": media.media_type,
                        "data": data,
                    }),
                };
                Some(serde_json::json!({ "type": "image", "source": source }))
            }
            // Citations are output-only; never sent on a request.
            ContentBlock::Citation { .. } => None,
        };
        if let Some(value) = value {
            mapped.push(value);
        }
    }
    Ok(mapped)
}

const ANTHROPIC_DEFAULT_BASE: &str = "https://api.anthropic.com";
const ANTHROPIC_VERSION: &str = "2023-06-01";

/// Live Anthropic Messages API adapter: `build_request` → POST (SSE) → `AnthropicStreamParser`
/// → canonical [`StreamDelta`]s. `base_url` is overridable for tests (point it at a mock server).
pub struct AnthropicProvider {
    api_key: String,
    base_url: String,
    client: reqwest::Client,
}

impl AnthropicProvider {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self::with_base_url(api_key, ANTHROPIC_DEFAULT_BASE)
    }

    pub fn with_base_url(api_key: impl Into<String>, base_url: impl Into<String>) -> Self {
        AnthropicProvider {
            api_key: api_key.into(),
            base_url: base_url.into(),
            client: super::native_http_client(),
        }
    }
}

#[async_trait]
impl Provider for AnthropicProvider {
    fn name(&self) -> &str {
        "anthropic"
    }

    async fn stream(
        &self,
        req: ProviderRequest,
    ) -> crate::error::Result<BoxStream<'static, StreamDelta>> {
        let validated = req.validated_options_for(self.name(), super::ANTHROPIC_OPTIONS)?;
        let options = validated.options;
        let warnings = validated.warnings;
        let body = build_request(
            &req.model,
            req.max_tokens,
            &req.messages,
            &req.tools,
            Some(&options),
        )
        .map_err(|error| {
            crate::error::ProviderError::new(
                "anthropic",
                &req.model,
                crate::error::ProviderErrorKind::InvalidRequest,
                error.to_string(),
            )
            .with_warnings(warnings.clone())
        })?;
        let url = format!("{}/v1/messages", self.base_url.trim_end_matches('/'));
        let resp = self
            .client
            .post(&url)
            .header("x-api-key", &self.api_key)
            .header("anthropic-version", ANTHROPIC_VERSION)
            .header("accept", "text/event-stream")
            .json(&body)
            .send()
            .await
            .map_err(|error| {
                super::transport_failure("anthropic", &req.model, error)
                    .with_provider_warnings(warnings.clone())
            })?;

        if !resp.status().is_success() {
            let status = resp.status();
            let retry_after = resp.headers().get(reqwest::header::RETRY_AFTER).cloned();
            let text = super::read_error_body(resp).await;
            return Err(super::http_failure(
                "anthropic",
                &req.model,
                status,
                retry_after.as_ref(),
                text,
            )
            .with_provider_warnings(warnings));
        }

        let model = req.model.clone();
        let mut byte_stream = resp.bytes_stream().boxed();
        let out = async_stream::stream! {
            let mut parser = AnthropicStreamParser::new();
            let mut buf: Vec<u8> = Vec::new();
            while let Some(chunk) = byte_stream.next().await {
                match chunk {
                    Ok(bytes) => {
                        if !super::append_sse_chunk(&mut buf, &bytes) {
                            yield super::stream_failure(
                                "anthropic",
                                &model,
                                crate::error::ProviderErrorKind::Protocol,
                                "Anthropic SSE event exceeded the size limit",
                            );
                            return;
                        }
                        // Dispatch each complete SSE `data:` line to the parser.
                        while let Some(pos) = buf.iter().position(|&b| b == b'\n') {
                            let line_bytes: Vec<u8> = buf.drain(..=pos).collect();
                            let line = String::from_utf8_lossy(&line_bytes);
                            if let Some(data) = line.trim().strip_prefix("data:") {
                                let data = data.trim();
                                if data.is_empty() {
                                    continue;
                                }
                                match serde_json::from_str::<Value>(data) {
                                    Ok(json) => {
                                        for d in parser.push_event(&json) {
                                            yield super::with_stream_context(d, "anthropic", &model);
                                        }
                                    }
                                    Err(_) => {
                                        yield super::stream_failure(
                                            "anthropic",
                                            &model,
                                            crate::error::ProviderErrorKind::Protocol,
                                            "malformed Anthropic SSE data",
                                        );
                                        return;
                                    }
                                }
                            }
                            if parser.is_terminal() {
                                break;
                            }
                        }
                        if parser.is_terminal() {
                            break;
                        }
                    }
                    Err(error) => {
                        yield super::response_stream_failure(
                            "anthropic",
                            &model,
                            error,
                            "Anthropic",
                        );
                        return;
                    }
                }
            }
            if !parser.is_terminal() {
                yield super::stream_failure(
                    "anthropic",
                    &model,
                    crate::error::ProviderErrorKind::Protocol,
                    "Anthropic stream ended before message_stop",
                );
            }
        };
        Ok(super::prepend_provider_warnings(Box::pin(out), warnings))
    }
}

/// Per-content-block accumulation state.
enum BlockState {
    Text,
    Thinking {
        text: String,
        signature: Option<String>,
    },
    /// Safety-triggered opaque reasoning; its `data` arrives complete on block_start and MUST
    /// be replayed verbatim in the next turn (like a signature).
    RedactedThinking {
        data: String,
    },
    ToolUse {
        id: String,
        json_buf: String,
    },
}

/// Stateful translator: feed it decoded SSE event objects; it yields canonical deltas.
#[derive(Default)]
pub struct AnthropicStreamParser {
    blocks: HashMap<u64, BlockState>,
    usage: Usage,
    stop_reason: String,
    metadata: Map<String, Value>,
    terminal: bool,
    retention: super::StreamRetentionBudget,
}

impl AnthropicStreamParser {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_terminal(&self) -> bool {
        self.terminal
    }

    /// Translate one decoded Anthropic SSE event into zero or more canonical deltas.
    pub fn push_event(&mut self, ev: &Value) -> Vec<StreamDelta> {
        if self.terminal {
            return Vec::new();
        }
        let kind = ev.get("type").and_then(Value::as_str).unwrap_or("");
        match kind {
            "message_start" => {
                let msg = &ev["message"];
                if !self.capture_message_metadata(msg) || !self.absorb_usage(&msg["usage"]) {
                    return self.retained_state_failure();
                }
                let model = msg
                    .get("model")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                vec![StreamDelta::MessageStart { model }]
            }
            "content_block_start" => self.on_block_start(ev),
            "content_block_delta" => self.on_block_delta(ev),
            "content_block_stop" => self.on_block_stop(ev),
            "message_delta" => {
                let delta = &ev["delta"];
                if let Some(sr) = delta.get("stop_reason").and_then(Value::as_str) {
                    if !self.retention.retain(sr.len(), 0) {
                        return self.retained_state_failure();
                    }
                    self.stop_reason = sr.to_string();
                }
                for field in ["stop_reason", "stop_sequence"] {
                    if let Some(value) = delta.get(field) {
                        if !self.retain_metadata_value(field, value) {
                            return self.retained_state_failure();
                        }
                        self.metadata.insert(field.into(), value.clone());
                    }
                }
                if !self.absorb_usage(&ev["usage"]) {
                    return self.retained_state_failure();
                }
                Vec::new()
            }
            "message_stop" => {
                if !self.blocks.is_empty() {
                    return self.terminal_protocol_failure(
                        "Anthropic message_stop arrived before every content block stopped",
                    );
                }
                if self.stop_reason.is_empty() {
                    return self.terminal_protocol_failure(
                        "Anthropic message_stop arrived without a stop_reason",
                    );
                }
                self.terminal = true;
                let mut out = Vec::new();
                if !self.metadata.is_empty() {
                    out.push(StreamDelta::ProviderMetadata {
                        provider: "anthropic".into(),
                        metadata: Value::Object(std::mem::take(&mut self.metadata)),
                    });
                }
                out.push(StreamDelta::Usage(self.usage));
                out.push(StreamDelta::MessageStop {
                    stop_reason: std::mem::take(&mut self.stop_reason),
                });
                out
            }
            "error" => {
                self.terminal = true;
                let kind = match ev["error"]["type"].as_str() {
                    Some("authentication_error" | "permission_error") => {
                        crate::error::ProviderErrorKind::Authentication
                    }
                    Some("rate_limit_error") => crate::error::ProviderErrorKind::RateLimited,
                    Some("overloaded_error") => crate::error::ProviderErrorKind::Server,
                    Some("invalid_request_error") => {
                        crate::error::ProviderErrorKind::InvalidRequest
                    }
                    _ => crate::error::ProviderErrorKind::Unknown,
                };
                vec![super::stream_failure_without_model(
                    "anthropic",
                    kind,
                    "Anthropic stream reported an error",
                )]
            }
            // "ping" and anything unknown: no canonical output.
            _ => Vec::new(),
        }
    }

    fn on_block_start(&mut self, ev: &Value) -> Vec<StreamDelta> {
        let index = ev["index"].as_u64().unwrap_or(0);
        let cb = &ev["content_block"];
        match cb.get("type").and_then(Value::as_str) {
            Some("text") => {
                if !self
                    .retention
                    .retain(0, usize::from(!self.blocks.contains_key(&index)))
                {
                    return self.retained_state_failure();
                }
                self.blocks.insert(index, BlockState::Text);
                Vec::new()
            }
            Some("thinking") => {
                if !self
                    .retention
                    .retain(0, usize::from(!self.blocks.contains_key(&index)))
                {
                    return self.retained_state_failure();
                }
                self.blocks.insert(
                    index,
                    BlockState::Thinking {
                        text: String::new(),
                        signature: None,
                    },
                );
                Vec::new()
            }
            Some("redacted_thinking") => {
                let data = cb["data"].as_str().unwrap_or_default().to_string();
                if !self
                    .retention
                    .retain(data.len(), usize::from(!self.blocks.contains_key(&index)))
                {
                    return self.retained_state_failure();
                }
                self.blocks
                    .insert(index, BlockState::RedactedThinking { data });
                Vec::new()
            }
            Some("tool_use") => {
                let id = cb["id"].as_str().unwrap_or_default().to_string();
                let name = cb["name"].as_str().unwrap_or_default().to_string();
                if !self
                    .retention
                    .retain(id.len(), usize::from(!self.blocks.contains_key(&index)))
                {
                    return self.retained_state_failure();
                }
                self.blocks.insert(
                    index,
                    BlockState::ToolUse {
                        id: id.clone(),
                        json_buf: String::new(),
                    },
                );
                vec![StreamDelta::ToolCallStart { id, name }]
            }
            _ => Vec::new(),
        }
    }

    fn on_block_delta(&mut self, ev: &Value) -> Vec<StreamDelta> {
        let index = ev["index"].as_u64().unwrap_or(0);
        let delta = &ev["delta"];
        match delta.get("type").and_then(Value::as_str) {
            Some("text_delta") => {
                let text = delta["text"].as_str().unwrap_or_default().to_string();
                vec![StreamDelta::TextDelta { text }]
            }
            Some("thinking_delta") => {
                let chunk = delta["thinking"].as_str().unwrap_or_default();
                if let Some(BlockState::Thinking { text, .. }) = self.blocks.get_mut(&index) {
                    if !self.retention.retain(chunk.len(), 0) {
                        return self.retained_state_failure();
                    }
                    text.push_str(chunk);
                }
                vec![StreamDelta::ReasoningDelta {
                    text: chunk.to_string(),
                }]
            }
            Some("signature_delta") => {
                let sig = delta["signature"].as_str().unwrap_or_default().to_string();
                if let Some(BlockState::Thinking { signature, .. }) = self.blocks.get_mut(&index) {
                    if !self.retention.retain(sig.len(), 0) {
                        return self.retained_state_failure();
                    }
                    *signature = Some(sig);
                }
                Vec::new()
            }
            Some("input_json_delta") => {
                let partial = delta["partial_json"].as_str().unwrap_or_default();
                if let Some(BlockState::ToolUse { json_buf, .. }) = self.blocks.get_mut(&index) {
                    if !self.retention.retain(partial.len(), 0) {
                        return self.retained_state_failure();
                    }
                    json_buf.push_str(partial);
                }
                Vec::new()
            }
            Some("citations_delta") => {
                let citation = delta.get("citation").cloned().unwrap_or(Value::Null);
                let text = citation
                    .get("cited_text")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                let source = citation
                    .get("url")
                    .or_else(|| citation.get("document_title"))
                    .and_then(Value::as_str)
                    .map(str::to_string);
                vec![StreamDelta::Citation {
                    text,
                    source,
                    metadata: Some(citation),
                }]
            }
            _ => Vec::new(),
        }
    }

    fn on_block_stop(&mut self, ev: &Value) -> Vec<StreamDelta> {
        let index = ev["index"].as_u64().unwrap_or(0);
        match self.blocks.remove(&index) {
            Some(BlockState::ToolUse { id, json_buf }) => {
                if json_buf.trim().is_empty() {
                    vec![StreamDelta::ToolCallInput {
                        id,
                        input: Value::Object(Default::default()),
                    }]
                } else {
                    match serde_json::from_str(&json_buf) {
                        Ok(input) => vec![StreamDelta::ToolCallInput { id, input }],
                        // Surface the parse failure (with the tool id) rather than run the tool
                        // with garbage `null` input.
                        Err(_) => vec![super::protocol_failure(
                            "anthropic",
                            format!("malformed tool_use input for {id}"),
                        )],
                    }
                }
            }
            Some(BlockState::Thinking { text, signature }) => {
                vec![StreamDelta::ReasoningComplete {
                    text,
                    signature,
                    opaque: None,
                }]
            }
            Some(BlockState::RedactedThinking { data }) => {
                vec![StreamDelta::ReasoningComplete {
                    text: String::new(),
                    signature: None,
                    opaque: Some(Value::String(data)),
                }]
            }
            _ => Vec::new(),
        }
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
        if let Some(n) = u.get("input_tokens").and_then(Value::as_u64) {
            self.usage.input_tokens = n;
        }
        if let Some(n) = u.get("output_tokens").and_then(Value::as_u64) {
            self.usage.output_tokens = n;
        }
        if let Some(n) = u.get("cache_creation_input_tokens").and_then(Value::as_u64) {
            self.usage.cache_creation_input_tokens = n;
        }
        if let Some(n) = u.get("cache_read_input_tokens").and_then(Value::as_u64) {
            self.usage.cache_read_input_tokens = n;
        }
        true
    }

    fn capture_message_metadata(&mut self, message: &Value) -> bool {
        for field in ["id", "model", "type", "role", "service_tier"] {
            if let Some(value) = message.get(field) {
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
        self.blocks.clear();
        self.stop_reason.clear();
        self.metadata.clear();
        vec![super::retained_state_failure("anthropic")]
    }

    fn terminal_protocol_failure(&mut self, message: impl Into<String>) -> Vec<StreamDelta> {
        self.terminal = true;
        self.blocks.clear();
        self.stop_reason.clear();
        self.metadata.clear();
        vec![super::protocol_failure("anthropic", message)]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contract::{MediaInput, MediaInputSource};
    use serde_json::json;

    #[test]
    fn strict_inline_media_reaches_anthropic_only_after_integrity_validation() {
        let media = MediaInput {
            media_type: "image/png".into(),
            source: MediaInputSource::Base64 {
                data: "YWJj".into(),
            },
            sha256: "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad".into(),
            size_bytes: 3,
        };
        let message = Message::user("inspect").with_media_input(media).unwrap();
        let request = build_request("claude-test", 64, &[message], &[], None).unwrap();
        let source = &request["messages"][0]["content"][1]["source"];
        assert_eq!(source["type"], "base64");
        assert_eq!(source["media_type"], "image/png");
        assert_eq!(source["data"], "YWJj");
    }

    #[test]
    fn citations_delta_preserves_provider_metadata() {
        let mut parser = AnthropicStreamParser::new();
        let deltas = parser.push_event(&json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": {
                "type": "citations_delta",
                "citation": {
                    "type": "char_location",
                    "cited_text": "The grass is green.",
                    "document_index": 0,
                    "document_title": "Example",
                    "start_char_index": 0,
                    "end_char_index": 19
                }
            }
        }));
        assert!(matches!(
            &deltas[0],
            StreamDelta::Citation {
                text,
                source: Some(source),
                metadata: Some(metadata),
            } if text == "The grass is green."
                && source == "Example"
                && metadata["start_char_index"] == 0
        ));
    }

    fn drain(parser: &mut AnthropicStreamParser, events: &[Value]) -> Vec<StreamDelta> {
        events.iter().flat_map(|e| parser.push_event(e)).collect()
    }

    #[test]
    fn build_request_replays_signed_thinking_and_maps_tool_result() {
        let messages = vec![
            Message::system("You are helpful."),
            Message::user("search for merhaba"),
            Message {
                role: Role::Assistant,
                content: vec![
                    ContentBlock::Reasoning {
                        text: "I'll search.".into(),
                        signature: Some("sig_XYZ".into()),
                        provider: Some("anthropic".into()),
                        opaque: None,
                    },
                    ContentBlock::ToolUse {
                        id: "toolu_1".into(),
                        name: "search".into(),
                        input: json!({ "q": "merhaba" }),
                    },
                ],
            },
            Message {
                role: Role::Tool,
                content: vec![ContentBlock::ToolResult {
                    tool_use_id: "toolu_1".into(),
                    content: "3 results".into(),
                    is_error: false,
                }],
            },
        ];
        let tools = vec![ToolSpec {
            name: "search".into(),
            description: "search the web".into(),
            input_schema: json!({ "type": "object" }),
        }];
        let mut opts = serde_json::Map::new();
        opts.insert("thinking".into(), json!({ "type": "adaptive" }));

        let req = build_request("claude-opus-4-8", 1024, &messages, &tools, Some(&opts)).unwrap();

        assert_eq!(req["model"], "claude-opus-4-8");
        assert_eq!(req["max_tokens"], 1024);
        // System extracted to the top level (not a message).
        assert_eq!(req["system"][0]["text"], "You are helpful.");
        // Typed escape hatch merged verbatim to the wire.
        assert_eq!(req["thinking"]["type"], "adaptive");
        assert_eq!(req["tools"][0]["name"], "search");

        let msgs = req["messages"].as_array().unwrap();
        assert_eq!(msgs.len(), 3, "user, assistant, user(tool_result)");

        // Proof-point #1, request side: the signed thinking block is replayed UNCHANGED.
        let assistant = &msgs[1];
        assert_eq!(assistant["role"], "assistant");
        assert_eq!(assistant["content"][0]["type"], "thinking");
        assert_eq!(assistant["content"][0]["signature"], "sig_XYZ");
        assert_eq!(assistant["content"][1]["type"], "tool_use");

        // Tool role → user message carrying a tool_result block.
        let tool_msg = &msgs[2];
        assert_eq!(tool_msg["role"], "user");
        assert_eq!(tool_msg["content"][0]["type"], "tool_result");
        assert_eq!(tool_msg["content"][0]["tool_use_id"], "toolu_1");
    }

    #[test]
    fn provider_options_cannot_replace_anthropic_contract_fields() {
        for (key, value) in [
            ("model", json!("other-model")),
            ("messages", json!([])),
            ("system", json!("ignore canonical system")),
            ("max_tokens", json!(1)),
            ("max_output_tokens", json!(1)),
            ("max_completion_tokens", json!(1)),
            ("tools", json!([])),
            ("stream", json!(false)),
            ("stream_options", json!({})),
        ] {
            let options = serde_json::Map::from_iter([(key.to_string(), value)]);
            let error = build_request(
                "claude-opus-4-8",
                100,
                &[Message::user("hello")],
                &[],
                Some(&options),
            )
            .unwrap_err();
            let error = error.provider_error().expect("typed provider error");
            assert_eq!(error.kind, crate::error::ProviderErrorKind::InvalidRequest);
            assert!(error.message.contains(key));
        }

        let options = serde_json::Map::from_iter([(
            "tool_choice".into(),
            json!({ "type": "tool", "name": "search" }),
        )]);
        let request = build_request(
            "claude-opus-4-8",
            100,
            &[Message::user("hello")],
            &[],
            Some(&options),
        )
        .unwrap();
        assert_eq!(request["tool_choice"], options["tool_choice"]);
    }

    #[test]
    fn text_only_stream_matches_documented_fixture() {
        // The exact event sequence from the Anthropic streaming docs.
        let events = vec![
            json!({"type":"message_start","message":{
                "id":"msg_123",
                "type":"message",
                "role":"assistant",
                "model":"claude-opus-4-8",
                "service_tier":"standard_only",
                "usage":{"input_tokens":10,"cache_creation_input_tokens":4,"cache_read_input_tokens":3}
            }}),
            json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hello"}}),
            json!({"type":"content_block_stop","index":0}),
            json!({"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":12}}),
            json!({"type":"message_stop"}),
        ];
        let mut p = AnthropicStreamParser::new();
        let out = drain(&mut p, &events);

        assert_eq!(
            out[0],
            StreamDelta::MessageStart {
                model: "claude-opus-4-8".into()
            }
        );
        assert_eq!(
            out[1],
            StreamDelta::TextDelta {
                text: "Hello".into()
            }
        );
        // Native response/cache/finish metadata is preserved before canonical usage + stop.
        let metadata = out
            .iter()
            .find_map(|delta| match delta {
                StreamDelta::ProviderMetadata { provider, metadata } if provider == "anthropic" => {
                    Some(metadata)
                }
                _ => None,
            })
            .expect("anthropic provider metadata");
        assert_eq!(metadata["id"], "msg_123");
        assert_eq!(metadata["service_tier"], "standard_only");
        assert_eq!(metadata["stop_reason"], "end_turn");
        assert_eq!(metadata["usage"][0]["cache_creation_input_tokens"], 4);
        assert_eq!(metadata["usage"][0]["cache_read_input_tokens"], 3);
        assert!(metadata.get("content").is_none());
        assert!(out.contains(&StreamDelta::Usage(Usage {
            input_tokens: 10,
            output_tokens: 12,
            cache_creation_input_tokens: 4,
            cache_read_input_tokens: 3,
            ..Default::default()
        })));
        assert!(out.contains(&StreamDelta::MessageStop {
            stop_reason: "end_turn".into()
        }));
    }

    #[test]
    fn thinking_block_preserves_signature_and_tool_input_is_accumulated() {
        // Extended-thinking + tool-use turn: signature must survive to ReasoningComplete, and
        // the tool input JSON is streamed in fragments that must reassemble.
        let events = vec![
            json!({"type":"message_start","message":{"model":"claude-opus-4-8","usage":{"input_tokens":42}}}),
            // Thinking block.
            json!({"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"I should call the tool."}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"sig_XYZ"}}),
            json!({"type":"content_block_stop","index":0}),
            // Tool-use block with fragmented input JSON.
            json!({"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"toolu_1","name":"search"}}),
            json!({"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\"q\":"}}),
            json!({"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"\"merhaba\"}"}}),
            json!({"type":"content_block_stop","index":1}),
            json!({"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":30}}),
            json!({"type":"message_stop"}),
        ];
        let mut p = AnthropicStreamParser::new();
        let out = drain(&mut p, &events);

        // Reasoning streamed, then completed WITH its signature intact (the replay contract).
        assert!(out.contains(&StreamDelta::ReasoningDelta {
            text: "I should call the tool.".into()
        }));
        assert!(out.contains(&StreamDelta::ReasoningComplete {
            text: "I should call the tool.".into(),
            signature: Some("sig_XYZ".into()),
            opaque: None,
        }));

        // Tool call started, and the fragmented JSON reassembled into a real object.
        assert!(out.contains(&StreamDelta::ToolCallStart {
            id: "toolu_1".into(),
            name: "search".into()
        }));
        assert!(out.contains(&StreamDelta::ToolCallInput {
            id: "toolu_1".into(),
            input: json!({ "q": "merhaba" }),
        }));

        // Ends on tool_use with accumulated usage.
        assert!(out.contains(&StreamDelta::MessageStop {
            stop_reason: "tool_use".into()
        }));
    }

    #[test]
    fn redacted_thinking_round_trips_via_opaque() {
        // Parser: redacted_thinking `data` arrives complete on block_start → ReasoningComplete
        // carrying it as opaque (must be replayed verbatim, like a signature).
        let events = vec![
            json!({"type":"content_block_start","index":0,"content_block":{"type":"redacted_thinking","data":"ENCRYPTED_BLOB"}}),
            json!({"type":"content_block_stop","index":0}),
        ];
        let mut p = AnthropicStreamParser::new();
        let out = drain(&mut p, &events);
        assert_eq!(
            out,
            vec![StreamDelta::ReasoningComplete {
                text: String::new(),
                signature: None,
                opaque: Some(Value::String("ENCRYPTED_BLOB".into())),
            }]
        );

        // Request side: that opaque-only block replays as a redacted_thinking wire block.
        let msg = Message {
            role: Role::Assistant,
            content: vec![ContentBlock::Reasoning {
                text: String::new(),
                signature: None,
                provider: Some("anthropic".into()),
                opaque: Some(Value::String("ENCRYPTED_BLOB".into())),
            }],
        };
        let req = build_request("claude-opus-4-8", 1024, &[msg], &[], None).unwrap();
        let block = &req["messages"][0]["content"][0];
        assert_eq!(block["type"], "redacted_thinking");
        assert_eq!(block["data"], "ENCRYPTED_BLOB");
    }

    #[test]
    fn malformed_tool_input_surfaces_an_error_not_null() {
        let events = vec![
            json!({"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"toolu_x","name":"search"}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{ not json"}}),
            json!({"type":"content_block_stop","index":0}),
        ];
        let mut p = AnthropicStreamParser::new();
        let out = drain(&mut p, &events);
        // The tool call is announced, but the bad input becomes an Error — never a null input.
        assert!(out.contains(&StreamDelta::ToolCallStart {
            id: "toolu_x".into(),
            name: "search".into()
        }));
        assert!(out.iter().any(
            |d| matches!(d, StreamDelta::Error { message, .. } if message.contains("malformed"))
        ));
        assert!(!out
            .iter()
            .any(|d| matches!(d, StreamDelta::ToolCallInput { input, .. } if input.is_null())));
    }

    #[test]
    fn many_small_thinking_fragments_fail_terminally() {
        let mut parser = AnthropicStreamParser::new();
        parser.retention = crate::providers::StreamRetentionBudget::with_limits(8, 8);
        parser.push_event(&json!({
            "type": "content_block_start",
            "index": 0,
            "content_block": {"type": "thinking", "thinking": ""}
        }));
        let fragment = json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": {"type": "thinking_delta", "thinking": "x"}
        });
        let failure = (0..16)
            .find_map(|_| {
                parser
                    .push_event(&fragment)
                    .into_iter()
                    .find(|delta| matches!(delta, StreamDelta::Error { .. }))
            })
            .expect("many small thinking fragments must exceed retained state");
        assert!(
            matches!(failure, StreamDelta::Error { message, .. } if message.contains("retained parser-state"))
        );
        assert!(parser.terminal);
        assert!(parser.blocks.is_empty());
        assert!(parser.push_event(&fragment).is_empty());
    }

    #[test]
    fn message_stop_with_an_open_block_is_a_terminal_protocol_failure() {
        let mut parser = AnthropicStreamParser::new();
        parser.push_event(&json!({
            "type": "content_block_start",
            "index": 0,
            "content_block": {"type": "tool_use", "id": "call", "name": "search"}
        }));
        parser.push_event(&json!({
            "type": "message_delta",
            "delta": {"stop_reason": "tool_use"},
            "usage": {"output_tokens": 1}
        }));
        let out = parser.push_event(&json!({"type": "message_stop"}));
        assert!(out.iter().any(|delta| matches!(
            delta,
            StreamDelta::Error { message, .. } if message.contains("before every content block stopped")
        )));
        assert!(!out
            .iter()
            .any(|delta| matches!(delta, StreamDelta::MessageStop { .. })));
        assert!(parser.terminal);
        assert!(parser.blocks.is_empty());
    }

    #[tokio::test]
    async fn provider_streams_sse_over_real_http() {
        use futures::StreamExt;
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let sse = concat!(
            "event: message_start\n",
            "data: {\"type\":\"message_start\",\"message\":{\"model\":\"claude-opus-4-8\",\"usage\":{\"input_tokens\":5}}}\n\n",
            "event: content_block_start\n",
            "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
            "event: content_block_delta\n",
            "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Merhaba\"}}\n\n",
            "event: content_block_stop\n",
            "data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
            "event: message_delta\n",
            "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":3}}\n\n",
            "event: message_stop\n",
            "data: {\"type\":\"message_stop\"}\n\n",
        );
        Mock::given(method("POST"))
            .and(path("/v1/messages"))
            .respond_with(ResponseTemplate::new(200).set_body_raw(sse, "text/event-stream"))
            .mount(&server)
            .await;

        let provider = AnthropicProvider::with_base_url("sk-ant-test", server.uri());
        let req = ProviderRequest {
            model: "claude-opus-4-8".into(),
            messages: vec![Message::user("selam")],
            tools: vec![],
            max_tokens: 100,
            options: serde_json::Map::new(),
            provider_options: crate::types::ProviderOptions::new(),
            compatibility_mode: crate::contract::CompatibilityMode::Strict,
        };
        let out: Vec<StreamDelta> = provider.stream(req).await.unwrap().collect().await;

        assert!(out.contains(&StreamDelta::MessageStart {
            model: "claude-opus-4-8".into()
        }));
        assert!(out.contains(&StreamDelta::TextDelta {
            text: "Merhaba".into()
        }));
        assert!(out.contains(&StreamDelta::MessageStop {
            stop_reason: "end_turn".into()
        }));
    }

    #[tokio::test]
    async fn clean_eof_before_message_stop_is_a_protocol_failure() {
        use futures::StreamExt;
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let sse = concat!(
            "data: {\"type\":\"message_start\",\"message\":{\"model\":\"claude-opus-4-8\",\"usage\":{}}}\n\n",
            "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"partial\"}}\n\n",
        );
        Mock::given(method("POST"))
            .and(path("/v1/messages"))
            .respond_with(ResponseTemplate::new(200).set_body_raw(sse, "text/event-stream"))
            .mount(&server)
            .await;

        let provider = AnthropicProvider::with_base_url("sk-ant-test", server.uri());
        let out: Vec<_> = provider
            .stream(ProviderRequest {
                model: "claude-opus-4-8".into(),
                messages: vec![Message::user("hello")],
                tools: vec![],
                max_tokens: 100,
                options: serde_json::Map::new(),
                provider_options: crate::types::ProviderOptions::new(),
                compatibility_mode: crate::contract::CompatibilityMode::Strict,
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
