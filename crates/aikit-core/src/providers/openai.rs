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
) -> Value {
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
        // `max_completion_tokens` is accepted by BOTH reasoning (o1/o3/o4-mini/gpt-5) and current
        // non-reasoning chat models; the legacy `max_tokens` is rejected (HTTP 400) by reasoning
        // models. provider_options is merged afterward, so callers can still override.
        "max_completion_tokens": max_tokens,
        "messages": wire,
        "stream": true,
        // Ask for the terminal usage chunk (choices:[], usage:{...}) on the stream.
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
    // Typed escape hatch: merge provider_options verbatim (reasoning_effort, temperature, ...).
    if let (Some(opts), Value::Object(map)) = (provider_options, &mut req) {
        for (k, v) in opts {
            map.insert(k.clone(), v.clone());
        }
    }
    req
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
#[derive(Default)]
pub struct OpenAiStreamParser {
    started: bool,
    tool_calls: BTreeMap<u64, ToolCallAccum>,
    tool_calls_emitted: bool,
    stop_reason: String,
    usage: Usage,
}

impl OpenAiStreamParser {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push_chunk(&mut self, chunk: &Value) -> Vec<StreamDelta> {
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
                let entry = self.tool_calls.entry(index).or_default();
                if let Some(id) = tc.get("id").and_then(Value::as_str) {
                    if !id.is_empty() {
                        entry.id = id.to_string();
                    }
                }
                if let Some(name) = tc["function"].get("name").and_then(Value::as_str) {
                    if !name.is_empty() {
                        entry.name = name.to_string();
                    }
                }
                if let Some(args) = tc["function"].get("arguments").and_then(Value::as_str) {
                    entry.args.push_str(args);
                }
            }
        }

        if let Some(fr) = choice.get("finish_reason").and_then(Value::as_str) {
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
            } else {
                // Never coerce malformed arguments to null and run the tool with it; surface
                // the parse failure as an Error delta so the loop can reject the tool call.
                match serde_json::from_str::<Value>(&accum.args) {
                    Ok(input) => out.push(StreamDelta::ToolCallInput {
                        id: accum.id.clone(),
                        input,
                    }),
                    Err(_) => out.push(super::protocol_failure(
                        "openai",
                        format!("malformed tool_call arguments for {}", accum.id),
                    )),
                }
            }
        }
        out
    }

    /// Call on `[DONE]` / stream end: flush any un-emitted tool calls, then Usage + MessageStop.
    pub fn finish(&mut self) -> Vec<StreamDelta> {
        let mut out = self.emit_tool_calls();
        out.push(StreamDelta::Usage(self.usage));
        let stop_reason = if self.stop_reason.is_empty() {
            "end_turn".to_string()
        } else {
            std::mem::take(&mut self.stop_reason)
        };
        out.push(StreamDelta::MessageStop { stop_reason });
        out
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
    api_key: String,
    base_url: String,
    client: reqwest::Client,
}

impl OpenAiProvider {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self::with_base_url(api_key, OPENAI_DEFAULT_BASE)
    }

    pub fn with_base_url(api_key: impl Into<String>, base_url: impl Into<String>) -> Self {
        OpenAiProvider {
            api_key: api_key.into(),
            base_url: base_url.into(),
            client: reqwest::Client::new(),
        }
    }
}

#[async_trait]
impl Provider for OpenAiProvider {
    fn name(&self) -> &str {
        "openai"
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
        );
        let url = format!("{}/chat/completions", self.base_url.trim_end_matches('/'));
        let resp = self
            .client
            .post(&url)
            .header("authorization", format!("Bearer {}", self.api_key))
            .header("accept", "text/event-stream")
            .json(&body)
            .send()
            .await
            .map_err(|error| super::transport_failure("openai", &req.model, error))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let retry_after = resp.headers().get(reqwest::header::RETRY_AFTER).cloned();
            let text = resp.text().await.unwrap_or_default();
            return Err(super::http_failure(
                "openai",
                &req.model,
                status,
                retry_after.as_ref(),
                text,
            ));
        }

        let model = req.model.clone();
        let mut byte_stream = resp.bytes_stream().boxed();
        let out = async_stream::stream! {
            let mut parser = OpenAiStreamParser::new();
            let mut buf: Vec<u8> = Vec::new();
            let mut done = false;
            while let Some(chunk) = byte_stream.next().await {
                match chunk {
                    Ok(bytes) => {
                        buf.extend_from_slice(&bytes);
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
                                        yield super::with_stream_context(d, "openai", &model);
                                    }
                                    done = true;
                                    break;
                                }
                                match serde_json::from_str::<Value>(data) {
                                    Ok(json) => {
                                        for d in parser.push_chunk(&json) {
                                            yield super::with_stream_context(d, "openai", &model);
                                        }
                                    }
                                    Err(_) => {
                                        yield super::stream_failure(
                                            "openai",
                                            &model,
                                            crate::error::ProviderErrorKind::Protocol,
                                            "malformed OpenAI Chat SSE data",
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
                    Err(_) => {
                        yield super::stream_failure(
                            "openai",
                            &model,
                            crate::error::ProviderErrorKind::Transport,
                            "OpenAI Chat response stream transport failed",
                        );
                        break;
                    }
                }
            }
            // Flush a terminal Usage + MessageStop even if the server closed without `[DONE]`.
            if !done {
                for d in parser.finish() {
                    yield super::with_stream_context(d, "openai", &model);
                }
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

        let req = build_request("gpt-4o", 2048, &messages, &tools, None);

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
        let req = build_request("gpt-4o", 128, &text_only, &[], None);
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
        let req = build_request("gpt-4o", 128, &reasoning_only, &[], None);
        let assistant = &req["messages"].as_array().unwrap()[0];
        assert_eq!(assistant["content"], "");
        assert!(!assistant["content"].is_null());
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

        assert!(out.contains(&StreamDelta::ToolCallStart {
            id: "call_bad".into(),
            name: "search".into()
        }));
        assert!(out.iter().any(|d| matches!(
            d,
            StreamDelta::Error { message, .. } if message.contains("malformed")
        )));
        assert!(!out.iter().any(|d| matches!(
            d,
            StreamDelta::ToolCallInput { input, .. } if input.is_null()
        )));
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
}
