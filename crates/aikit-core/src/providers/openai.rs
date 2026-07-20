//! OpenAI Chat Completions wire ↔ canonical translation.
//!
//! OpenAI's `/v1/chat/completions` is the archetype of the "OpenAI-compatible" shape that
//! DeepSeek, Together, Groq, etc. clone; this adapter is ~95% identical to [`super::deepseek`],
//! differing only in the default base URL and the (absent) reasoning-replay concerns below.
//!
//! NOTE — reasoning: the Chat Completions streaming API does **not** return reasoning items in
//! the stream. On o-series models the reasoning is done server-side and is not surfaced as
//! deltas (there is no `reasoning_content` field, unlike DeepSeek), so this adapter emits **no**
//! `Reasoning` blocks and drops any reasoning blocks from the assistant turn on the request side
//! (Chat Completions does not accept them). Faithful reasoning-item passthrough — the encrypted
//! reasoning item / `OpaquePassthrough` replay policy — lives on the *Responses* API (`/v1/responses`)
//! and is a documented follow-up, out of scope for this Chat Completions adapter.
//!
//! Wire fields below are verified against the official OpenAI API reference (the
//! `platform.openai.com/docs/api-reference/chat` pages 301-redirect to `developers.openai.com`):
//!  - Request (Create chat completion): `model`, `messages` (roles system/user/assistant/tool;
//!    assistant `tool_calls[{id,type:"function",function:{name,arguments}}]`; tool
//!    `{role:"tool",tool_call_id,content}`), `tools[{type:"function",function:{name,description,
//!    parameters}}]`, `max_tokens`/`max_completion_tokens`, `stream`, `stream_options.include_usage`.
//!  - Streaming chunk: `model`, `choices[].delta.content`, `choices[].delta.tool_calls[]` with
//!    `index`/`id`/`type`/`function.name`/`function.arguments` streamed in fragments,
//!    `choices[].finish_reason` (`stop`/`tool_calls`/`length`), and the terminal `usage` object
//!    (`prompt_tokens`/`completion_tokens`) emitted when `stream_options.include_usage=true`.
//!    The stream terminates with `data: [DONE]`.
//!
//! Pure and keyless where it matters: [`build_request`] and [`OpenAiStreamParser`] are testable
//! without a network.

use super::{Provider, ProviderRequest};
use crate::types::{ContentBlock, MediaSource, Message, Role, StreamDelta, ToolSpec, Usage};
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::StreamExt;
use serde_json::Value;
use std::collections::BTreeMap;

/// Build an OpenAI `chat/completions` request body from canonical inputs.
///
/// `Reasoning` blocks are dropped from the assistant turn by construction (Chat Completions has
/// no wire slot for them), so only `Text` (→ `content`) and `ToolUse` (→ `tool_calls`) survive.
pub fn build_request(
    model: &str,
    max_tokens: u64,
    messages: &[Message],
    tools: &[ToolSpec],
    provider_options: Option<&serde_json::Map<String, Value>>,
) -> crate::error::Result<Value> {
    build_compatible_request(
        "openai",
        "max_completion_tokens",
        true,
        model,
        max_tokens,
        messages,
        tools,
        provider_options,
    )
}

/// Shared OpenAI-compatible request translation. Provider adapters select their own canonical
/// name and output-token field while preserving the same canonical message/tool contract.
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_compatible_request(
    provider: &str,
    output_token_field: &str,
    include_usage_stream_option: bool,
    model: &str,
    max_tokens: u64,
    messages: &[Message],
    tools: &[ToolSpec],
    provider_options: Option<&serde_json::Map<String, Value>>,
) -> crate::error::Result<Value> {
    super::reject_protected_options(
        provider,
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
    let mut wire: Vec<Value> = Vec::new();

    for m in messages {
        match m.role {
            Role::System => wire.push(serde_json::json!({
                "role": "system", "content": join_text(&m.content),
            })),
            Role::User => wire.push(serde_json::json!({
                "role": "user", "content": openai_user_content(&m.content),
            })),
            Role::Assistant => {
                // Reasoning blocks are intentionally dropped here: Chat Completions does not
                // accept them on input. join_text keeps only Text; the filter_map keeps only
                // ToolUse — so reasoning never reaches the wire.
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
                // OpenAI allows null content only when tool_calls are present. A reasoning-only
                // turn (empty text, no tool_calls) must send an empty string, not null, or the
                // API rejects it with HTTP 400.
                msg["content"] = if text.is_empty() && !tool_calls.is_empty() {
                    Value::Null
                } else {
                    Value::String(text)
                };
                if !tool_calls.is_empty() {
                    msg["tool_calls"] = Value::Array(tool_calls);
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
        "messages": wire,
        "stream": true,
    });
    if include_usage_stream_option {
        // OpenAI, OpenRouter, Groq, and xAI accept this opt-in for a terminal usage chunk.
        // Mistral's native schema has no `stream_options` request field and already reports usage.
        req["stream_options"] = serde_json::json!({ "include_usage": true });
    }
    // OpenAI/Groq prefer `max_completion_tokens`; Mistral uses `max_tokens`. The adapter owns
    // that compatibility choice and escape-hatch options may replace neither spelling.
    req[output_token_field] = Value::from(max_tokens);
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
    // Typed escape hatch: merge provider_options verbatim (reasoning_effort, temperature, ...).
    if let (Some(opts), Value::Object(map)) = (provider_options, &mut req) {
        for (k, v) in opts {
            map.insert(k.clone(), v.clone());
        }
    }
    Ok(req)
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

fn openai_user_content(blocks: &[ContentBlock]) -> Value {
    if !blocks
        .iter()
        .any(|block| matches!(block, ContentBlock::Media { .. }))
    {
        return Value::String(join_text(blocks));
    }
    Value::Array(
        blocks
            .iter()
            .filter_map(|block| match block {
                ContentBlock::Text { text } => {
                    Some(serde_json::json!({ "type": "text", "text": text }))
                }
                ContentBlock::Media { media_type, source } => {
                    let url = match source {
                        MediaSource::Url { url } => url.clone(),
                        MediaSource::Base64 { data } => {
                            format!("data:{media_type};base64,{data}")
                        }
                    };
                    Some(serde_json::json!({
                        "type": "image_url",
                        "image_url": { "url": url },
                    }))
                }
                _ => None,
            })
            .collect(),
    )
}

/// Accumulator for one streamed tool call (OpenAI groups by `index`; id/name/args arrive in
/// fragments across chunks).
#[derive(Default)]
struct ToolCallAccum {
    id: String,
    name: String,
    args: String,
}

/// Stateful translator for OpenAI Chat Completions streaming chunks → canonical deltas.
/// Feed decoded `data:` chunk objects via [`OpenAiStreamParser::push_chunk`], then call
/// [`OpenAiStreamParser::finish`] on `[DONE]`.
///
/// Unlike [`super::deepseek::DeepSeekStreamParser`], there is no `reasoning_content` to handle:
/// Chat Completions never streams reasoning items, so no `Reasoning*` deltas are emitted.
pub struct OpenAiStreamParser {
    provider_name: String,
    public_provider_name: String,
    started: bool,
    terminal: bool,
    tool_calls: BTreeMap<u64, ToolCallAccum>,
    tool_calls_emitted: bool,
    stop_reason: String,
    usage: Usage,
    retention: super::StreamRetentionBudget,
}

impl Default for OpenAiStreamParser {
    fn default() -> Self {
        Self::for_provider("openai", "OpenAI Chat")
    }
}

impl OpenAiStreamParser {
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn for_provider(provider_name: &str, public_provider_name: &str) -> Self {
        Self {
            provider_name: provider_name.to_string(),
            public_provider_name: public_provider_name.to_string(),
            started: false,
            terminal: false,
            tool_calls: BTreeMap::new(),
            tool_calls_emitted: false,
            stop_reason: String::new(),
            usage: Usage::default(),
            retention: super::StreamRetentionBudget::default(),
        }
    }

    pub fn is_terminal(&self) -> bool {
        self.terminal
    }

    pub fn push_chunk(&mut self, chunk: &Value) -> Vec<StreamDelta> {
        if self.terminal {
            return Vec::new();
        }
        if let Some(error) = chunk.get("error").filter(|error| error.is_object()) {
            return self.terminal_failure(
                compatible_stream_error_kind(error),
                format!("{} stream reported an error", self.public_provider_name),
            );
        }
        let mut out = Vec::new();

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
            self.absorb_usage(u);
        }

        let choice = &chunk["choices"][0];
        let delta = &choice["delta"];

        if let Some(text) = delta.get("content").and_then(Value::as_str) {
            if !text.is_empty() {
                out.push(StreamDelta::TextDelta { text: text.into() });
            }
        }
        // A refusal streams in `delta.refusal` (never `delta.content`); surface it as text so the
        // refusal isn't lost as empty output. A dedicated error path would be nicer, but TextDelta
        // is the safe minimal fix.
        if let Some(refusal) = delta.get("refusal").and_then(Value::as_str) {
            if !refusal.is_empty() {
                out.push(StreamDelta::TextDelta {
                    text: refusal.into(),
                });
            }
        }
        // No reasoning_content on Chat Completions — nothing to translate here (see module note).
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
            if !self.retention.retain(fr.len(), 0) {
                return self.retained_state_failure();
            }
            self.stop_reason = map_finish_reason(fr);
            out.extend(self.emit_tool_calls());
        }

        out
    }

    /// Emit accumulated tool calls once (ToolCallStart + ToolCallInput per call, in index order).
    fn emit_tool_calls(&mut self) -> Vec<StreamDelta> {
        if self.tool_calls_emitted || self.tool_calls.is_empty() {
            return Vec::new();
        }
        if let Some(message) = self.tool_calls.values().find_map(|call| {
            if call.id.is_empty() {
                Some(format!(
                    "{} tool call is missing its id",
                    self.public_provider_name
                ))
            } else if call.name.is_empty() {
                Some(format!(
                    "{} tool call {} is missing its name",
                    self.public_provider_name, call.id
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
            return self.terminal_protocol_failure(format!(
                "{} [DONE] arrived before finish_reason",
                self.public_provider_name
            ));
        }
        let mut out = self.emit_tool_calls();
        if self.terminal {
            return out;
        }
        self.terminal = true;
        out.push(StreamDelta::Usage(self.usage));
        let stop_reason = if self.stop_reason.is_empty() {
            "end_turn".to_string()
        } else {
            std::mem::take(&mut self.stop_reason)
        };
        out.push(StreamDelta::MessageStop { stop_reason });
        out
    }

    fn retained_state_failure(&mut self) -> Vec<StreamDelta> {
        self.terminal = true;
        self.tool_calls.clear();
        self.stop_reason.clear();
        vec![super::retained_state_failure(&self.provider_name)]
    }

    fn terminal_protocol_failure(&mut self, message: impl Into<String>) -> Vec<StreamDelta> {
        self.terminal_failure(crate::error::ProviderErrorKind::Protocol, message)
    }

    fn terminal_failure(
        &mut self,
        kind: crate::error::ProviderErrorKind,
        message: impl Into<String>,
    ) -> Vec<StreamDelta> {
        self.terminal = true;
        self.tool_calls.clear();
        self.stop_reason.clear();
        vec![super::stream_failure_without_model(
            &self.provider_name,
            kind,
            message,
        )]
    }

    fn absorb_usage(&mut self, u: &Value) {
        if let Some(n) = u.get("prompt_tokens").and_then(Value::as_u64) {
            self.usage.input_tokens = n;
        }
        if let Some(n) = u.get("completion_tokens").and_then(Value::as_u64) {
            self.usage.output_tokens = n;
        }
        // OpenAI nests cache/reasoning accounting under *_tokens_details on the same usage object.
        if let Some(n) = u
            .get("prompt_tokens_details")
            .and_then(|d| d.get("cached_tokens"))
            .and_then(Value::as_u64)
        {
            self.usage.cache_read_input_tokens = n;
        }
        if let Some(n) = u
            .get("completion_tokens_details")
            .and_then(|d| d.get("reasoning_tokens"))
            .and_then(Value::as_u64)
        {
            self.usage.reasoning_tokens = n;
        }
    }
}

fn compatible_stream_error_kind(error: &Value) -> crate::error::ProviderErrorKind {
    let status = error
        .get("status")
        .or_else(|| error.get("code"))
        .and_then(Value::as_u64)
        .and_then(|status| u16::try_from(status).ok());
    match status {
        Some(401 | 403) => return crate::error::ProviderErrorKind::Authentication,
        Some(408) => return crate::error::ProviderErrorKind::Timeout,
        Some(429) => return crate::error::ProviderErrorKind::RateLimited,
        Some(500..=599) => return crate::error::ProviderErrorKind::Server,
        Some(400..=499) => return crate::error::ProviderErrorKind::InvalidRequest,
        _ => {}
    }

    match error
        .get("type")
        .or_else(|| error.get("code"))
        .and_then(Value::as_str)
        .unwrap_or_default()
    {
        "authentication" | "authentication_error" | "invalid_api_key" => {
            crate::error::ProviderErrorKind::Authentication
        }
        "rate_limit_exceeded" | "rate_limit_error" => crate::error::ProviderErrorKind::RateLimited,
        "request_timeout" | "timeout" => crate::error::ProviderErrorKind::Timeout,
        "server_error" | "overloaded_error" => crate::error::ProviderErrorKind::Server,
        "content_filter" | "safety" => crate::error::ProviderErrorKind::Safety,
        "bad_request" | "invalid_request_error" => crate::error::ProviderErrorKind::InvalidRequest,
        _ => crate::error::ProviderErrorKind::Protocol,
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

const OPENAI_DEFAULT_BASE: &str = "https://api.openai.com/v1";

/// Live OpenAI Chat Completions adapter: `build_request` → POST (SSE) → [`OpenAiStreamParser`]
/// → canonical [`StreamDelta`]s. `base_url` is overridable for tests (point it at a mock server).
pub struct OpenAiProvider {
    provider_name: String,
    api_key: String,
    base_url: String,
    model_prefix: Option<String>,
    client: reqwest::Client,
}

impl OpenAiProvider {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self::with_base_url(api_key, OPENAI_DEFAULT_BASE)
    }

    pub fn with_base_url(api_key: impl Into<String>, base_url: impl Into<String>) -> Self {
        OpenAiProvider {
            provider_name: "openai".into(),
            api_key: api_key.into(),
            base_url: base_url.into(),
            model_prefix: None,
            client: super::native_http_client(),
        }
    }

    /// Connect an OpenAI Chat Completions compatible provider without mislabelling its
    /// credentials, options, audit errors, or model namespace as OpenAI.
    pub fn compatible(
        provider_name: impl Into<String>,
        api_key: impl Into<String>,
        base_url: impl Into<String>,
    ) -> Self {
        let provider_name = provider_name.into();
        Self {
            model_prefix: Some(format!("{provider_name}:")),
            provider_name,
            api_key: api_key.into(),
            base_url: base_url.into(),
            client: super::native_http_client(),
        }
    }
}

#[async_trait]
impl Provider for OpenAiProvider {
    fn name(&self) -> &str {
        &self.provider_name
    }

    async fn stream(
        &self,
        req: ProviderRequest,
    ) -> crate::error::Result<BoxStream<'static, StreamDelta>> {
        stream_compatible(
            self.name(),
            "OpenAI Chat",
            &self.api_key,
            &self.base_url,
            self.model_prefix.as_deref(),
            "max_completion_tokens",
            true,
            &self.client,
            req,
        )
        .await
    }
}

/// Shared HTTP/SSE machinery for providers that implement the OpenAI Chat Completions wire
/// contract. First-class provider modules supply their own endpoint, namespace, token field,
/// display label, and credential while errors remain attributed to the real provider.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn stream_compatible(
    provider_name: &str,
    public_provider_name: &str,
    api_key: &str,
    base_url: &str,
    model_prefix: Option<&str>,
    output_token_field: &str,
    include_usage_stream_option: bool,
    client: &reqwest::Client,
    req: ProviderRequest,
) -> crate::error::Result<BoxStream<'static, StreamDelta>> {
    let options = req.options_for(provider_name);
    let wire_model = model_prefix
        .and_then(|prefix| req.model.strip_prefix(prefix))
        .unwrap_or(&req.model);
    let body = build_compatible_request(
        provider_name,
        output_token_field,
        include_usage_stream_option,
        wire_model,
        req.max_tokens,
        &req.messages,
        &req.tools,
        Some(&options),
    )
    .map_err(|error| {
        crate::error::ProviderError::new(
            provider_name,
            &req.model,
            crate::error::ProviderErrorKind::InvalidRequest,
            error.to_string(),
        )
    })?;
    let url = format!("{}/chat/completions", base_url.trim_end_matches('/'));
    let resp = client
        .post(&url)
        .bearer_auth(api_key)
        .header("accept", "text/event-stream")
        .json(&body)
        .send()
        .await
        .map_err(|error| super::transport_failure(provider_name, &req.model, error))?;

    if !resp.status().is_success() {
        let status = resp.status();
        let retry_after = resp.headers().get(reqwest::header::RETRY_AFTER).cloned();
        let text = super::read_error_body(resp).await;
        return Err(super::http_failure(
            provider_name,
            &req.model,
            status,
            retry_after.as_ref(),
            text,
        ));
    }

    let model = req.model.clone();
    let provider_name = provider_name.to_string();
    let public_provider_name = public_provider_name.to_string();
    let mut byte_stream = resp.bytes_stream().boxed();
    let out = async_stream::stream! {
        let mut parser = OpenAiStreamParser::for_provider(
            &provider_name,
            &public_provider_name,
        );
        let mut buf: Vec<u8> = Vec::new();
        let mut done = false;
        while let Some(chunk) = byte_stream.next().await {
            match chunk {
                Ok(bytes) => {
                    if !super::append_sse_chunk(&mut buf, &bytes) {
                        yield super::stream_failure(
                            &provider_name,
                            &model,
                            crate::error::ProviderErrorKind::Protocol,
                            format!("{public_provider_name} SSE event exceeded the size limit"),
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
                                    yield super::with_stream_context(d, &provider_name, &model);
                                }
                                done = true;
                                break;
                            }
                            match serde_json::from_str::<Value>(data) {
                                Ok(json) => {
                                    for d in parser.push_chunk(&json) {
                                        yield super::with_stream_context(d, &provider_name, &model);
                                    }
                                    if parser.is_terminal() {
                                        return;
                                    }
                                }
                                Err(_) => {
                                    yield super::stream_failure(
                                        &provider_name,
                                        &model,
                                        crate::error::ProviderErrorKind::Protocol,
                                        format!("malformed {public_provider_name} SSE data"),
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
                        &provider_name,
                        &model,
                        error,
                        &public_provider_name,
                    );
                    return;
                }
            }
        }
        // `[DONE]` is the Chat Completions commit marker. Never turn a truncated clean EOF
        // into a successful MessageStop.
        if !done {
            yield super::stream_failure(
                &provider_name,
                &model,
                crate::error::ProviderErrorKind::Protocol,
                format!("{public_provider_name} stream ended before [DONE]"),
            );
        }
    };
    Ok(Box::pin(out))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn build_request_maps_tool_calls_and_drops_reasoning() {
        let messages = vec![
            Message::system("You are helpful."),
            Message::user("search merhaba"),
            Message {
                role: Role::Assistant,
                content: vec![
                    // Chat Completions has no reasoning slot — this MUST be dropped.
                    ContentBlock::Reasoning {
                        text: "reasoning that must not be sent".into(),
                        signature: None,
                        provider: Some("openai".into()),
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
        ];
        let tools = vec![ToolSpec {
            name: "search".into(),
            description: "search the web".into(),
            input_schema: json!({ "type": "object" }),
        }];

        let req = build_request("gpt-4o", 2048, &messages, &tools, None).unwrap();

        // stream + usage opt-in set on the body.
        assert_eq!(req["stream"], true);
        assert_eq!(req["stream_options"]["include_usage"], true);

        // Token cap is emitted as `max_completion_tokens` (accepted by reasoning + non-reasoning
        // models); the legacy `max_tokens` key must NOT be present.
        assert_eq!(req["max_completion_tokens"], 2048);
        assert!(req.get("max_tokens").is_none());
        let serialized_body = serde_json::to_string(&req).unwrap();
        assert!(
            !serialized_body.contains("max_tokens"),
            "legacy max_tokens leaked into the OpenAI request"
        );

        let msgs = req["messages"].as_array().unwrap();
        // system, user, assistant, tool → 4 messages.
        assert_eq!(msgs.len(), 4);

        // Reasoning never reaches the wire.
        let serialized = serde_json::to_string(&req).unwrap();
        assert!(
            !serialized.contains("must not be sent"),
            "reasoning leaked into the OpenAI request"
        );

        // Assistant maps ToolUse → OpenAI tool_calls with stringified arguments.
        let assistant = &msgs[2];
        assert_eq!(assistant["role"], "assistant");
        // Empty text + tool_calls present → content is null (OpenAI requires it in this case).
        assert!(assistant["content"].is_null());
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
        assert_eq!(msgs[3]["content"], "3 results");

        // Tools mapped to the function shape.
        assert_eq!(req["tools"][0]["type"], "function");
        assert_eq!(req["tools"][0]["function"]["name"], "search");
        assert_eq!(req["tools"][0]["function"]["parameters"]["type"], "object");
    }

    #[test]
    fn build_request_reasoning_only_assistant_turn_sends_string_content_not_null() {
        // A text-only assistant turn (no tool_calls) must serialize content as a string. Even a
        // reasoning-only turn (all Reasoning blocks dropped → empty text, no tool_calls) sends an
        // empty string, never null — OpenAI rejects {role:"assistant", content:null} with no
        // tool_calls at HTTP 400.
        let text_only = vec![Message {
            role: Role::Assistant,
            content: vec![ContentBlock::Text {
                text: "Here you go.".into(),
            }],
        }];
        let req = build_request("gpt-4o", 128, &text_only, &[], None).unwrap();
        let assistant = &req["messages"].as_array().unwrap()[0];
        assert_eq!(assistant["content"], "Here you go.");
        assert!(assistant.get("tool_calls").is_none());

        // Reasoning-only (dropped) → empty string content, still not null.
        let reasoning_only = vec![Message {
            role: Role::Assistant,
            content: vec![ContentBlock::Reasoning {
                text: "thinking".into(),
                signature: None,
                provider: Some("openai".into()),
                opaque: None,
            }],
        }];
        let req = build_request("gpt-4o", 128, &reasoning_only, &[], None).unwrap();
        let assistant = &req["messages"].as_array().unwrap()[0];
        assert_eq!(assistant["content"], "");
        assert!(!assistant["content"].is_null());
    }

    #[test]
    fn provider_options_cannot_replace_openai_chat_contract_fields() {
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
                "gpt-4o",
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

        let options = serde_json::Map::from_iter([("temperature".into(), json!(0.2))]);
        let request = build_request(
            "gpt-4o",
            100,
            &[Message::user("hello")],
            &[],
            Some(&options),
        )
        .unwrap();
        assert_eq!(request["temperature"], 0.2);

        let options = serde_json::Map::from_iter([("tool_choice".into(), json!("required"))]);
        let request = build_request(
            "gpt-4o",
            100,
            &[Message::user("hello")],
            &[],
            Some(&options),
        )
        .unwrap();
        assert_eq!(request["tool_choice"], "required");
    }

    #[test]
    fn stream_parser_surfaces_refusal_as_text() {
        // An OpenAI refusal streams in `delta.refusal`, not `delta.content`; it must not be lost.
        let chunk = json!({
            "model": "gpt-4o",
            "choices": [{ "delta": { "refusal": "I can't help with that" }, "finish_reason": null }]
        });
        let mut p = OpenAiStreamParser::new();
        let out = p.push_chunk(&chunk);
        assert!(out.contains(&StreamDelta::TextDelta {
            text: "I can't help with that".into()
        }));
    }

    #[test]
    fn stream_parser_reassembles_fragmented_tool_calls_and_usage() {
        let chunks = vec![
            json!({"model":"gpt-4o","choices":[{"delta":{"role":"assistant","content":""},"finish_reason":null}]}),
            json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"search","arguments":"{\"q\":"}}]},"finish_reason":null}]}),
            json!({"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"merhaba\"}"}}]},"finish_reason":null}]}),
            json!({"choices":[{"delta":{},"finish_reason":"tool_calls"}]}),
            json!({"choices":[],"usage":{"prompt_tokens":50,"completion_tokens":20,"prompt_tokens_details":{"cached_tokens":8},"completion_tokens_details":{"reasoning_tokens":12}}}),
        ];
        let mut p = OpenAiStreamParser::new();
        let mut out: Vec<StreamDelta> = Vec::new();
        for c in &chunks {
            out.extend(p.push_chunk(c));
        }
        out.extend(p.finish());

        assert_eq!(
            out[0],
            StreamDelta::MessageStart {
                model: "gpt-4o".into()
            }
        );
        // Fragmented tool-call arguments reassemble into a real object.
        assert!(out.contains(&StreamDelta::ToolCallStart {
            id: "call_1".into(),
            name: "search".into()
        }));
        assert!(out.contains(&StreamDelta::ToolCallInput {
            id: "call_1".into(),
            input: json!({ "q": "merhaba" }),
        }));
        // finish_reason tool_calls → canonical tool_use, with usage incl. cache + reasoning tokens.
        assert!(out.contains(&StreamDelta::MessageStop {
            stop_reason: "tool_use".into()
        }));
        assert!(out.contains(&StreamDelta::Usage(Usage {
            input_tokens: 50,
            output_tokens: 20,
            cache_read_input_tokens: 8,
            reasoning_tokens: 12,
            ..Default::default()
        })));
    }

    #[test]
    fn stream_parser_surfaces_malformed_tool_call_arguments_as_error() {
        // Fragments reassemble into `{"q":` — invalid JSON. The parser must NOT coerce this to
        // null and run the tool with it; it must emit an Error delta instead.
        let chunks = vec![
            json!({"model":"gpt-4o","choices":[{"delta":{"role":"assistant","tool_calls":[{"index":0,"id":"call_bad","type":"function","function":{"name":"search","arguments":"{\"q\":"}}]},"finish_reason":null}]}),
            json!({"choices":[{"delta":{},"finish_reason":"tool_calls"}]}),
        ];
        let mut p = OpenAiStreamParser::new();
        let mut out: Vec<StreamDelta> = Vec::new();
        for c in &chunks {
            out.extend(p.push_chunk(c));
        }
        out.extend(p.finish());

        assert!(out.iter().any(|d| matches!(
            d,
            StreamDelta::Error { message, .. } if message.contains("malformed")
        )));
        assert!(!out.iter().any(|d| matches!(
            d,
            StreamDelta::ToolCallInput { input, .. } if input.is_null()
        )));
    }

    #[test]
    fn many_small_tool_argument_fragments_fail_terminally() {
        let mut parser = OpenAiStreamParser::new();
        parser.retention = crate::providers::StreamRetentionBudget::with_limits(32, 8);
        let start = json!({
            "choices": [{"delta": {"tool_calls": [{
                "index": 0,
                "id": "c",
                "function": {"name": "f", "arguments": ""}
            }]}}]
        });
        assert!(!parser
            .push_chunk(&start)
            .iter()
            .any(|delta| matches!(delta, StreamDelta::Error { .. })));

        let fragment = json!({
            "choices": [{"delta": {"tool_calls": [{
                "index": 0,
                "function": {"arguments": "x"}
            }]}}]
        });
        let failure = (0..64)
            .find_map(|_| {
                parser
                    .push_chunk(&fragment)
                    .into_iter()
                    .find(|delta| matches!(delta, StreamDelta::Error { .. }))
            })
            .expect("many small fragments must exceed retained state");
        assert!(
            matches!(failure, StreamDelta::Error { message, .. } if message.contains("retained parser-state"))
        );
        assert!(parser.terminal);
        assert!(parser.tool_calls.is_empty());
        assert!(parser.push_chunk(&fragment).is_empty());
        assert!(parser.finish().is_empty());
    }

    #[test]
    fn done_without_finish_reason_is_a_terminal_protocol_failure() {
        let mut parser = OpenAiStreamParser::new();
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
    fn tool_call_without_name_fails_before_emission() {
        let mut parser = OpenAiStreamParser::new();
        let out = parser.push_chunk(&json!({
            "choices": [{
                "delta": {"tool_calls": [{"index": 0, "id": "call", "function": {"arguments": "{}"}}]},
                "finish_reason": "tool_calls"
            }]
        }));
        assert!(out.iter().any(|delta| matches!(
            delta,
            StreamDelta::Error { message, .. } if message.contains("missing its name")
        )));
        assert!(!out
            .iter()
            .any(|delta| matches!(delta, StreamDelta::ToolCallStart { .. })));
        assert!(parser.terminal);
    }

    #[tokio::test]
    async fn provider_streams_openai_chat_sse_over_real_http() {
        use futures::StreamExt;
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let sse = concat!(
            "data: {\"model\":\"gpt-4o\",\"choices\":[{\"delta\":{\"role\":\"assistant\",\"content\":\"Merhaba\"},\"finish_reason\":null}]}\n\n",
            "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
            "data: [DONE]\n\n",
        );
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_raw(sse, "text/event-stream"))
            .mount(&server)
            .await;

        let provider = OpenAiProvider::with_base_url("sk-openai-test", server.uri());
        let req = ProviderRequest {
            model: "gpt-4o".into(),
            messages: vec![Message::user("selam")],
            tools: vec![],
            max_tokens: 100,
            options: serde_json::Map::new(),
            provider_options: crate::types::ProviderOptions::new(),
        };
        let out: Vec<StreamDelta> = provider.stream(req).await.unwrap().collect().await;

        assert!(out.contains(&StreamDelta::MessageStart {
            model: "gpt-4o".into()
        }));
        assert!(out.contains(&StreamDelta::TextDelta {
            text: "Merhaba".into()
        }));
        assert!(out.contains(&StreamDelta::MessageStop {
            stop_reason: "end_turn".into()
        }));
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
                "data: {\"model\":\"gpt-4o\",\"choices\":[{\"delta\":{\"content\":\"partial\"},\"finish_reason\":\"stop\"}]}\n\n",
                "text/event-stream",
            ))
            .mount(&server)
            .await;

        let provider = OpenAiProvider::with_base_url("sk-openai-test", server.uri());
        let out: Vec<_> = provider
            .stream(ProviderRequest {
                model: "gpt-4o".into(),
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

    #[tokio::test]
    async fn compatible_provider_strips_only_its_routing_prefix() {
        use futures::StreamExt;
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_raw(
                "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n",
                "text/event-stream",
            ))
            .mount(&server)
            .await;
        let provider = OpenAiProvider::compatible("groq", "key", server.uri());
        let req = ProviderRequest {
            model: "groq:llama-3.3".into(),
            messages: vec![Message::user("hello")],
            tools: vec![],
            max_tokens: 10,
            options: serde_json::Map::new(),
            provider_options: crate::types::ProviderOptions::new(),
        };
        let _: Vec<_> = provider.stream(req).await.unwrap().collect().await;
        let requests = server.received_requests().await.unwrap();
        let body: Value = serde_json::from_slice(&requests[0].body).unwrap();
        assert_eq!(body["model"], "llama-3.3");
        assert_eq!(provider.name(), "groq");
    }

    #[tokio::test]
    async fn xai_grok_adapter_sends_the_unprefixed_model_and_bearer_key() {
        use futures::StreamExt;
        use wiremock::matchers::{header, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .and(header("authorization", "Bearer xai-test-key"))
            .respond_with(ResponseTemplate::new(200).set_body_raw(
                "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n",
                "text/event-stream",
            ))
            .mount(&server)
            .await;

        let provider = OpenAiProvider::compatible("xai", "xai-test-key", server.uri());
        let req = ProviderRequest {
            model: "xai:grok-4.5".into(),
            messages: vec![Message::user("hello")],
            tools: vec![],
            max_tokens: 10,
            options: serde_json::Map::new(),
            provider_options: crate::types::ProviderOptions::new(),
        };

        let _: Vec<_> = provider.stream(req).await.unwrap().collect().await;
        let requests = server.received_requests().await.unwrap();
        let body: Value = serde_json::from_slice(&requests[0].body).unwrap();
        assert_eq!(body["model"], "grok-4.5");
        assert_eq!(provider.name(), "xai");
    }
}
