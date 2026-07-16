//! OpenAI Responses API wire ↔ canonical translation.
//!
//! This adapter intentionally lives beside, rather than replacing, [`super::openai`]. The latter
//! remains the Chat Completions adapter (and keeps its public API and wire behavior); this module
//! owns the Responses API's item-based history and typed SSE events. Keeping the protocols
//! separate prevents a reasoning-fidelity feature from regressing OpenAI-compatible chat users.

use super::{Provider, ProviderRequest};
use crate::error::{AikitError, Result};
use crate::reasoning::{blocks_for_provider_replay, validate_replay, ReplayPolicy};
use crate::types::{ContentBlock, MediaSource, Message, Role, StreamDelta, ToolSpec, Usage};
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::StreamExt;
use serde_json::{Map, Value};
use std::collections::{BTreeMap, BTreeSet};

/// Build an OpenAI `/v1/responses` request from canonical conversation state.
///
/// Responses history is an ordered list of heterogeneous items. In particular, completed
/// reasoning items are replayed from `ContentBlock::Reasoning.opaque` **verbatim**; reconstructing
/// only their id would discard the encrypted state required by stateless tool continuations.
pub fn build_request(
    model: &str,
    max_tokens: u64,
    messages: &[Message],
    tools: &[ToolSpec],
    provider_options: Option<&Map<String, Value>>,
) -> Result<Value> {
    let stateless = provider_options
        .and_then(|options| options.get("store"))
        .and_then(Value::as_bool)
        .map(|store| !store)
        .unwrap_or(true);

    let mut input = Vec::new();
    for message in messages {
        match message.role {
            Role::System => input.push(serde_json::json!({
                "role": "system",
                "content": join_text(&message.content),
            })),
            Role::User => input.push(serde_json::json!({
                "role": "user",
                "content": openai_user_content(&message.content),
            })),
            Role::Assistant => {
                let replay = blocks_for_provider_replay(
                    "openai",
                    ReplayPolicy::OpaquePassthrough,
                    &message.content,
                );
                validate_replay(ReplayPolicy::OpaquePassthrough, &replay)
                    .map_err(|error| AikitError::Provider(error.to_string()))?;

                let assistant_text = join_text(&message.content);
                let mut text_emitted = false;
                for block in &message.content {
                    match block {
                        ContentBlock::Reasoning {
                            provider, opaque, ..
                        } => {
                            if provider.as_deref().is_some_and(|source| source != "openai") {
                                continue;
                            }
                            let raw = opaque.as_ref().ok_or_else(|| {
                                AikitError::Provider(
                                    "OpenAI Responses reasoning replay is missing its raw item"
                                        .into(),
                                )
                            })?;
                            validate_reasoning_item(raw, stateless)?;
                            // Do not normalize, prune, or rebuild this object. Fields added by the
                            // API must survive future turns without an aikit release.
                            input.push(raw.clone());
                        }
                        ContentBlock::Text { .. } if !text_emitted => {
                            text_emitted = true;
                            if !assistant_text.is_empty() {
                                input.push(serde_json::json!({
                                    "role": "assistant",
                                    "content": assistant_text,
                                }));
                            }
                        }
                        ContentBlock::ToolUse {
                            id,
                            name,
                            input: args,
                        } => {
                            input.push(serde_json::json!({
                                "type": "function_call",
                                // Responses has both an output-item `id` (fc_...) and a `call_id`
                                // (call_...). Canonical ToolUse.id is the latter because tool
                                // outputs must reference call_id.
                                "call_id": id,
                                "name": name,
                                "arguments": serde_json::to_string(args).map_err(|error| {
                                    AikitError::Provider(format!(
                                        "failed to serialize OpenAI function arguments: {error}"
                                    ))
                                })?,
                            }));
                        }
                        _ => {}
                    }
                }
            }
            Role::Tool => {
                for block in &message.content {
                    if let ContentBlock::ToolResult {
                        tool_use_id,
                        content,
                        ..
                    } = block
                    {
                        input.push(serde_json::json!({
                            "type": "function_call_output",
                            "call_id": tool_use_id,
                            "output": content,
                        }));
                    }
                }
            }
        }
    }

    let mut request = serde_json::json!({
        "model": model,
        "input": input,
        "max_output_tokens": max_tokens,
        "stream": true,
        // Self-contained history is the only mode in which the in-process loop can guarantee
        // replay without retaining a server-side response id.
        "store": false,
        // Current Responses returns this automatically for store:false; the legacy include value
        // remains accepted and keeps older API revisions stateless-compatible.
        "include": ["reasoning.encrypted_content"],
    });

    if !tools.is_empty() {
        request["tools"] = Value::Array(
            tools
                .iter()
                .map(|tool| {
                    serde_json::json!({
                        "type": "function",
                        "name": tool.name,
                        "description": tool.description,
                        "parameters": tool.input_schema,
                    })
                })
                .collect(),
        );
    }

    merge_provider_options(&mut request, provider_options)?;
    Ok(request)
}

fn join_text(blocks: &[ContentBlock]) -> String {
    blocks
        .iter()
        .filter_map(|block| match block {
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
                    Some(serde_json::json!({ "type": "input_text", "text": text }))
                }
                ContentBlock::Media { media_type, source } => {
                    let image_url = match source {
                        MediaSource::Url { url } => url.clone(),
                        MediaSource::Base64 { data } => {
                            format!("data:{media_type};base64,{data}")
                        }
                    };
                    Some(serde_json::json!({
                        "type": "input_image",
                        "image_url": image_url,
                    }))
                }
                _ => None,
            })
            .collect(),
    )
}

fn validate_reasoning_item(item: &Value, stateless: bool) -> Result<()> {
    let object = item.as_object().ok_or_else(|| {
        AikitError::Provider("OpenAI reasoning opaque payload must be an object".into())
    })?;
    if object.get("type").and_then(Value::as_str) != Some("reasoning") {
        return Err(AikitError::Provider(
            "OpenAI reasoning opaque payload must have type='reasoning'".into(),
        ));
    }
    if object
        .get("id")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .is_none()
    {
        return Err(AikitError::Provider(
            "OpenAI reasoning item is missing its id".into(),
        ));
    }
    if !object.get("summary").is_some_and(Value::is_array) {
        return Err(AikitError::Provider(
            "OpenAI reasoning item is missing its summary array".into(),
        ));
    }
    if stateless
        && object
            .get("encrypted_content")
            .and_then(Value::as_str)
            .filter(|content| !content.is_empty())
            .is_none()
    {
        return Err(AikitError::Provider(
            "stateless OpenAI reasoning replay requires encrypted_content".into(),
        ));
    }
    Ok(())
}

/// Merge native Responses options. `dx.rs` currently expresses OpenAI structured output in the
/// Chat Completions `response_format` shape; translate that one internal compatibility value to
/// Responses `text.format` at the protocol boundary, leaving the Chat adapter untouched.
fn merge_provider_options(request: &mut Value, options: Option<&Map<String, Value>>) -> Result<()> {
    let Some(options) = options else {
        return Ok(());
    };
    let Value::Object(request) = request else {
        return Err(AikitError::Provider(
            "OpenAI Responses request body was not an object".into(),
        ));
    };

    let mut options = options.clone();
    if let Some(response_format) = options.remove("response_format") {
        if options.contains_key("text") {
            return Err(AikitError::Provider(
                "OpenAI Responses options cannot contain both response_format and text".into(),
            ));
        }
        options.insert("text".into(), translate_response_format(&response_format)?);
    }

    // Preserve caller-requested include values while always requesting the opaque reasoning state
    // this adapter exists to carry.
    let mut includes = match options.remove("include") {
        Some(Value::Array(values)) => values,
        Some(_) => {
            return Err(AikitError::Provider(
                "OpenAI Responses include option must be an array".into(),
            ))
        }
        None => Vec::new(),
    };
    if !includes
        .iter()
        .any(|value| value.as_str() == Some("reasoning.encrypted_content"))
    {
        includes.push(Value::String("reasoning.encrypted_content".into()));
    }
    options.insert("include".into(), Value::Array(includes));

    for (key, value) in options {
        request.insert(key, value);
    }
    Ok(())
}

fn translate_response_format(response_format: &Value) -> Result<Value> {
    let object = response_format.as_object().ok_or_else(|| {
        AikitError::Provider("OpenAI response_format option must be an object".into())
    })?;
    match object.get("type").and_then(Value::as_str) {
        Some("json_schema") => {
            let schema = object
                .get("json_schema")
                .and_then(Value::as_object)
                .ok_or_else(|| {
                    AikitError::Provider(
                        "OpenAI json_schema response_format is missing json_schema".into(),
                    )
                })?;
            let mut format = schema.clone();
            format.insert("type".into(), Value::String("json_schema".into()));
            Ok(serde_json::json!({ "format": Value::Object(format) }))
        }
        Some("json_object") => Ok(serde_json::json!({
            "format": { "type": "json_object" }
        })),
        Some("text") => Ok(serde_json::json!({
            "format": { "type": "text" }
        })),
        Some(other) => Err(AikitError::Provider(format!(
            "unsupported OpenAI Responses output format '{other}'"
        ))),
        None => Err(AikitError::Provider(
            "OpenAI response_format option is missing its type".into(),
        )),
    }
}

#[derive(Default)]
struct FunctionCallAccum {
    call_id: String,
    name: String,
    arguments: String,
    start_emitted: bool,
    input_emitted: bool,
}

fn openai_stream_error_kind(code: Option<&str>) -> crate::error::ProviderErrorKind {
    match code.unwrap_or_default().to_ascii_lowercase().as_str() {
        "invalid_api_key" | "authentication_error" | "permission_error" => {
            crate::error::ProviderErrorKind::Authentication
        }
        "rate_limit_exceeded" | "rate_limit_error" => crate::error::ProviderErrorKind::RateLimited,
        "server_error" | "internal_server_error" | "overloaded_error" => {
            crate::error::ProviderErrorKind::Server
        }
        "request_timeout" | "timeout" => crate::error::ProviderErrorKind::Timeout,
        "content_filter" | "safety" => crate::error::ProviderErrorKind::Safety,
        "invalid_request_error" | "invalid_request" => {
            crate::error::ProviderErrorKind::InvalidRequest
        }
        _ => crate::error::ProviderErrorKind::Unknown,
    }
}

/// Stateful translator for typed OpenAI Responses SSE events.
#[derive(Default)]
pub struct OpenAiResponsesStreamParser {
    started: bool,
    terminal: bool,
    saw_tool_call: bool,
    calls: BTreeMap<String, FunctionCallAccum>,
    reasoning_emitted: BTreeSet<String>,
    usage: Usage,
    metadata: Map<String, Value>,
}

impl OpenAiResponsesStreamParser {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_terminal(&self) -> bool {
        self.terminal
    }

    pub fn push_event(&mut self, event: &Value) -> Vec<StreamDelta> {
        if self.terminal {
            return Vec::new();
        }

        let mut out = Vec::new();
        if let Some(response) = event.get("response").filter(|value| value.is_object()) {
            self.capture_response_metadata(response);
            self.start_from_response(response, &mut out);
        }
        self.capture_event_logprobs(event);

        match event
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default()
        {
            "response.created" | "response.in_progress" => {}
            "response.output_text.delta" | "response.refusal.delta" => {
                if let Some(delta) = event.get("delta").and_then(Value::as_str) {
                    if !delta.is_empty() {
                        out.push(StreamDelta::TextDelta {
                            text: delta.to_string(),
                        });
                    }
                }
            }
            "response.output_item.added" => {
                if let Some(item) = event.get("item") {
                    out.extend(self.observe_item(item, false));
                }
            }
            "response.function_call_arguments.delta" => {
                if let (Some(item_id), Some(delta)) = (
                    event.get("item_id").and_then(Value::as_str),
                    event.get("delta").and_then(Value::as_str),
                ) {
                    self.calls
                        .entry(item_id.to_string())
                        .or_default()
                        .arguments
                        .push_str(delta);
                }
            }
            "response.function_call_arguments.done" => {
                if let Some(item_id) = event.get("item_id").and_then(Value::as_str) {
                    let arguments = event.get("arguments").and_then(Value::as_str);
                    if let Some(name) = event.get("name").and_then(Value::as_str) {
                        self.calls.entry(item_id.to_string()).or_default().name = name.to_string();
                    }
                    out.extend(self.emit_call_input(item_id, arguments));
                }
            }
            "response.output_item.done" => {
                if let Some(item) = event.get("item") {
                    out.extend(self.observe_item(item, true));
                }
            }
            "response.completed" => {
                if let Some(response) = event.get("response") {
                    out.extend(self.absorb_final_response(response));
                }
                out.extend(self.complete("completed"));
            }
            "response.incomplete" => {
                let reason = event
                    .get("response")
                    .and_then(|response| response.get("incomplete_details"))
                    .and_then(|details| details.get("reason"))
                    .and_then(Value::as_str)
                    .unwrap_or("incomplete");
                if let Some(response) = event.get("response") {
                    out.extend(self.absorb_final_response(response));
                }
                let stop_reason = if reason == "max_output_tokens" {
                    "max_tokens"
                } else {
                    reason
                };
                out.extend(self.complete(stop_reason));
            }
            "response.failed" => {
                if let Some(response) = event.get("response") {
                    out.extend(self.absorb_final_response(response));
                }
                let code = event
                    .get("response")
                    .and_then(|response| response.get("error"))
                    .and_then(|error| error.get("code").or_else(|| error.get("type")))
                    .and_then(Value::as_str);
                out.push(super::stream_failure_without_model(
                    "openai",
                    openai_stream_error_kind(code),
                    "OpenAI response failed",
                ));
                out.extend(self.complete("error"));
            }
            "error" => {
                let code = event
                    .get("error")
                    .and_then(|error| error.get("code").or_else(|| error.get("type")))
                    .and_then(Value::as_str);
                out.push(super::stream_failure_without_model(
                    "openai",
                    openai_stream_error_kind(code),
                    "OpenAI Responses stream reported an error",
                ));
                out.extend(self.complete("error"));
            }
            _ => {}
        }
        out
    }

    pub fn finish(&mut self) -> Vec<StreamDelta> {
        if self.terminal {
            Vec::new()
        } else {
            self.complete(if self.saw_tool_call {
                "tool_use"
            } else {
                "end_turn"
            })
        }
    }

    fn start_from_response(&mut self, response: &Value, out: &mut Vec<StreamDelta>) {
        if self.started {
            return;
        }
        self.started = true;
        out.push(StreamDelta::MessageStart {
            model: response
                .get("model")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
        });
    }

    fn observe_item(&mut self, item: &Value, done: bool) -> Vec<StreamDelta> {
        match item.get("type").and_then(Value::as_str) {
            Some("function_call") => {
                self.saw_tool_call = true;
                let item_id = item.get("id").and_then(Value::as_str).unwrap_or_default();
                if item_id.is_empty() {
                    return vec![super::protocol_failure(
                        "openai",
                        "OpenAI function_call item is missing its item id",
                    )];
                }

                let mut out = Vec::new();
                let entry = self.calls.entry(item_id.to_string()).or_default();
                if let Some(call_id) = item.get("call_id").and_then(Value::as_str) {
                    entry.call_id = call_id.to_string();
                }
                if let Some(name) = item.get("name").and_then(Value::as_str) {
                    entry.name = name.to_string();
                }
                if !entry.start_emitted && !entry.call_id.is_empty() && !entry.name.is_empty() {
                    entry.start_emitted = true;
                    out.push(StreamDelta::ToolCallStart {
                        id: entry.call_id.clone(),
                        name: entry.name.clone(),
                    });
                }

                if done {
                    let arguments = item.get("arguments").and_then(Value::as_str);
                    out.extend(self.emit_call_input(item_id, arguments));
                } else if let Some(arguments) = item.get("arguments").and_then(Value::as_str) {
                    if !arguments.is_empty() {
                        entry.arguments = arguments.to_string();
                    }
                }
                out
            }
            Some("reasoning") if done => {
                let id = item.get("id").and_then(Value::as_str).unwrap_or_default();
                if id.is_empty() {
                    return vec![super::protocol_failure(
                        "openai",
                        "OpenAI reasoning item is missing its id",
                    )];
                }
                if !self.reasoning_emitted.insert(id.to_string()) {
                    return Vec::new();
                }
                vec![StreamDelta::ReasoningComplete {
                    text: visible_reasoning_text(item),
                    signature: None,
                    // The complete output item is the replay unit. Keeping only selected fields
                    // would silently lose future protocol additions.
                    opaque: Some(item.clone()),
                }]
            }
            _ => Vec::new(),
        }
    }

    fn emit_call_input(
        &mut self,
        item_id: &str,
        authoritative_arguments: Option<&str>,
    ) -> Vec<StreamDelta> {
        let Some(entry) = self.calls.get_mut(item_id) else {
            return vec![super::protocol_failure(
                "openai",
                format!("OpenAI arguments referenced unknown item {item_id}"),
            )];
        };
        if entry.input_emitted {
            return Vec::new();
        }
        entry.input_emitted = true;
        if let Some(arguments) = authoritative_arguments {
            entry.arguments = arguments.to_string();
        }

        if entry.call_id.is_empty() {
            return vec![super::protocol_failure(
                "openai",
                format!("OpenAI function_call item {item_id} is missing call_id"),
            )];
        }
        if entry.arguments.trim().is_empty() {
            return vec![StreamDelta::ToolCallInput {
                id: entry.call_id.clone(),
                input: Value::Object(Map::new()),
            }];
        }
        match serde_json::from_str::<Value>(&entry.arguments) {
            Ok(input) => vec![StreamDelta::ToolCallInput {
                id: entry.call_id.clone(),
                input,
            }],
            Err(_) => vec![super::protocol_failure(
                "openai",
                format!(
                    "malformed OpenAI function_call arguments for {}",
                    entry.call_id
                ),
            )],
        }
    }

    fn absorb_final_response(&mut self, response: &Value) -> Vec<StreamDelta> {
        let mut out = Vec::new();
        if let Some(items) = response.get("output").and_then(Value::as_array) {
            for item in items {
                self.capture_output_logprobs(item);
                out.extend(self.observe_item(item, true));
            }
        }
        if let Some(usage) = response.get("usage").filter(|value| value.is_object()) {
            self.absorb_usage(usage);
        }
        out
    }

    fn absorb_usage(&mut self, usage: &Value) {
        if let Some(raw) = usage.as_object().filter(|raw| !raw.is_empty()) {
            self.metadata
                .entry("usage")
                .or_insert_with(|| Value::Array(Vec::new()))
                .as_array_mut()
                .expect("usage metadata is initialized as an array")
                .push(Value::Object(raw.clone()));
        }
        if let Some(tokens) = usage.get("input_tokens").and_then(Value::as_u64) {
            self.usage.input_tokens = tokens;
        }
        if let Some(tokens) = usage.get("output_tokens").and_then(Value::as_u64) {
            self.usage.output_tokens = tokens;
        }
        if let Some(tokens) = usage
            .get("input_tokens_details")
            .and_then(|details| details.get("cached_tokens"))
            .and_then(Value::as_u64)
        {
            self.usage.cache_read_input_tokens = tokens;
        }
        if let Some(tokens) = usage
            .get("output_tokens_details")
            .and_then(|details| details.get("reasoning_tokens"))
            .and_then(Value::as_u64)
        {
            self.usage.reasoning_tokens = tokens;
        }
    }

    fn complete(&mut self, requested_reason: &str) -> Vec<StreamDelta> {
        if self.terminal {
            return Vec::new();
        }
        self.terminal = true;
        let stop_reason = if requested_reason == "completed" {
            if self.saw_tool_call {
                "tool_use"
            } else {
                "end_turn"
            }
        } else {
            requested_reason
        };
        let mut out = Vec::new();
        if !self.metadata.is_empty() {
            out.push(StreamDelta::ProviderMetadata {
                provider: "openai".into(),
                metadata: Value::Object(std::mem::take(&mut self.metadata)),
            });
        }
        out.push(StreamDelta::Usage(self.usage));
        out.push(StreamDelta::MessageStop {
            stop_reason: stop_reason.to_string(),
        });
        out
    }

    fn capture_response_metadata(&mut self, response: &Value) {
        for field in [
            "id",
            "object",
            "created_at",
            "completed_at",
            "model",
            "status",
            "incomplete_details",
            "error",
            "service_tier",
            "system_fingerprint",
            "parallel_tool_calls",
            "temperature",
            "top_p",
            "truncation",
            "max_output_tokens",
            "previous_response_id",
            "reasoning",
        ] {
            if let Some(value) = response.get(field) {
                self.metadata.insert(field.into(), value.clone());
            }
        }
    }

    fn capture_event_logprobs(&mut self, event: &Value) {
        let Some(logprobs) = event.get("logprobs").filter(|value| !value.is_null()) else {
            return;
        };
        let mut entry = Map::new();
        entry.insert("source".into(), Value::String("stream".into()));
        for field in ["item_id", "output_index", "content_index"] {
            if let Some(value) = event.get(field) {
                entry.insert(field.into(), value.clone());
            }
        }
        entry.insert("logprobs".into(), logprobs.clone());
        self.metadata
            .entry("logprobs")
            .or_insert_with(|| Value::Array(Vec::new()))
            .as_array_mut()
            .expect("logprobs metadata is initialized as an array")
            .push(Value::Object(entry));
    }

    fn capture_output_logprobs(&mut self, item: &Value) {
        let Some(content) = item.get("content").and_then(Value::as_array) else {
            return;
        };
        for (content_index, part) in content.iter().enumerate() {
            let Some(logprobs) = part.get("logprobs").filter(|value| !value.is_null()) else {
                continue;
            };
            let mut entry = Map::new();
            entry.insert("source".into(), Value::String("final_response".into()));
            if let Some(item_id) = item.get("id") {
                entry.insert("item_id".into(), item_id.clone());
            }
            entry.insert("content_index".into(), Value::from(content_index));
            entry.insert("logprobs".into(), logprobs.clone());
            self.metadata
                .entry("logprobs")
                .or_insert_with(|| Value::Array(Vec::new()))
                .as_array_mut()
                .expect("logprobs metadata is initialized as an array")
                .push(Value::Object(entry));
        }
    }
}

fn visible_reasoning_text(item: &Value) -> String {
    let from = |field: &str| {
        item.get(field)
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|part| part.get("text").and_then(Value::as_str))
            .collect::<Vec<_>>()
            .join("")
    };
    let summary = from("summary");
    if summary.is_empty() {
        from("content")
    } else {
        summary
    }
}

const OPENAI_DEFAULT_BASE: &str = "https://api.openai.com/v1";

/// Live OpenAI Responses adapter. The existing [`super::openai::OpenAiProvider`] remains the
/// Chat Completions transport.
pub struct OpenAiResponsesProvider {
    api_key: String,
    base_url: String,
    client: reqwest::Client,
}

impl OpenAiResponsesProvider {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self::with_base_url(api_key, OPENAI_DEFAULT_BASE)
    }

    pub fn with_base_url(api_key: impl Into<String>, base_url: impl Into<String>) -> Self {
        Self {
            api_key: api_key.into(),
            base_url: base_url.into(),
            client: reqwest::Client::new(),
        }
    }
}

#[async_trait]
impl Provider for OpenAiResponsesProvider {
    fn name(&self) -> &str {
        "openai"
    }

    async fn stream(&self, req: ProviderRequest) -> Result<BoxStream<'static, StreamDelta>> {
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
                "openai",
                &req.model,
                crate::error::ProviderErrorKind::InvalidRequest,
                error.to_string(),
            )
        })?;
        let url = format!("{}/responses", self.base_url.trim_end_matches('/'));
        let response = self
            .client
            .post(&url)
            .header("authorization", format!("Bearer {}", self.api_key))
            .header("accept", "text/event-stream")
            .json(&body)
            .send()
            .await
            .map_err(|error| super::transport_failure("openai", &req.model, error))?;

        if !response.status().is_success() {
            let status = response.status();
            let retry_after = response
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .cloned();
            let text = response.text().await.unwrap_or_default();
            return Err(super::http_failure(
                "openai",
                &req.model,
                status,
                retry_after.as_ref(),
                text,
            ));
        }

        let model = req.model.clone();
        let mut bytes = response.bytes_stream().boxed();
        let stream = async_stream::stream! {
            let mut parser = OpenAiResponsesStreamParser::new();
            let mut buffer = Vec::new();
            while let Some(chunk) = bytes.next().await {
                match chunk {
                    Ok(chunk) => {
                        buffer.extend_from_slice(&chunk);
                        while let Some(position) = buffer.iter().position(|byte| *byte == b'\n') {
                            let line_bytes: Vec<u8> = buffer.drain(..=position).collect();
                            let line = String::from_utf8_lossy(&line_bytes);
                            let Some(data) = line.trim().strip_prefix("data:") else {
                                continue;
                            };
                            let data = data.trim();
                            if data.is_empty() || data == "[DONE]" {
                                continue;
                            }
                            match serde_json::from_str::<Value>(data) {
                                Ok(event) => {
                                    for delta in parser.push_event(&event) {
                                        yield super::with_stream_context(delta, "openai", &model);
                                    }
                                }
                                Err(_) => {
                                    yield super::stream_failure(
                                        "openai",
                                        &model,
                                        crate::error::ProviderErrorKind::Protocol,
                                        "malformed OpenAI Responses SSE data",
                                    );
                                    return;
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
                    Err(_) => {
                        yield super::stream_failure(
                            "openai",
                            &model,
                            crate::error::ProviderErrorKind::Transport,
                            "OpenAI Responses stream transport failed",
                        );
                        break;
                    }
                }
            }
            for delta in parser.finish() {
                yield super::with_stream_context(delta, "openai", &model);
            }
        };
        Ok(Box::pin(stream))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use serde_json::json;

    fn raw_reasoning() -> Value {
        json!({
            "type": "reasoning",
            "id": "rs_123",
            "summary": [{ "type": "summary_text", "text": "Checked the inputs." }],
            "encrypted_content": "encrypted-state",
            "status": "completed"
        })
    }

    #[test]
    fn request_maps_url_and_inline_image_input_without_flattening() {
        let messages = vec![Message {
            role: Role::User,
            content: vec![
                ContentBlock::Text {
                    text: "compare".into(),
                },
                ContentBlock::Media {
                    media_type: "image/png".into(),
                    source: MediaSource::Url {
                        url: "https://example.test/a.png".into(),
                    },
                },
                ContentBlock::Media {
                    media_type: "image/jpeg".into(),
                    source: MediaSource::Base64 {
                        data: "AQID".into(),
                    },
                },
            ],
        }];
        let request = build_request("gpt-test", 100, &messages, &[], None).unwrap();
        let content = request["input"][0]["content"].as_array().unwrap();
        assert_eq!(
            content[0],
            json!({ "type": "input_text", "text": "compare" })
        );
        assert_eq!(content[1]["image_url"], "https://example.test/a.png");
        assert_eq!(content[2]["image_url"], "data:image/jpeg;base64,AQID");
    }

    #[test]
    fn request_replays_raw_reasoning_and_maps_function_items() {
        let reasoning = raw_reasoning();
        let messages = vec![
            Message::user("Find it"),
            Message {
                role: Role::Assistant,
                content: vec![
                    ContentBlock::Reasoning {
                        text: "Checked the inputs.".into(),
                        signature: None,
                        provider: Some("openai".into()),
                        opaque: Some(reasoning.clone()),
                    },
                    ContentBlock::ToolUse {
                        id: "call_123".into(),
                        name: "search".into(),
                        input: json!({ "q": "aikit" }),
                    },
                ],
            },
            Message {
                role: Role::Tool,
                content: vec![ContentBlock::ToolResult {
                    tool_use_id: "call_123".into(),
                    content: "found".into(),
                    is_error: false,
                }],
            },
        ];
        let tools = vec![ToolSpec {
            name: "search".into(),
            description: "Search".into(),
            input_schema: json!({ "type": "object" }),
        }];

        let request = build_request("o3", 2048, &messages, &tools, None).unwrap();
        assert_eq!(request["max_output_tokens"], 2048);
        assert!(request.get("max_completion_tokens").is_none());
        assert!(request.get("messages").is_none());
        assert_eq!(request["store"], false);
        assert!(request["include"]
            .as_array()
            .unwrap()
            .contains(&json!("reasoning.encrypted_content")));

        let input = request["input"].as_array().unwrap();
        assert_eq!(input.len(), 4);
        assert_eq!(
            input[1], reasoning,
            "reasoning item was not replayed verbatim"
        );
        assert_eq!(input[2]["type"], "function_call");
        assert_eq!(input[2]["call_id"], "call_123");
        assert!(input[2].get("id").is_none());
        assert_eq!(input[2]["arguments"], "{\"q\":\"aikit\"}");
        assert_eq!(input[3]["type"], "function_call_output");
        assert_eq!(input[3]["call_id"], "call_123");
        assert_eq!(request["tools"][0]["type"], "function");
        assert_eq!(request["tools"][0]["name"], "search");
        assert!(request["tools"][0].get("function").is_none());
    }

    #[test]
    fn stateless_request_rejects_missing_or_malformed_reasoning_opaque() {
        let missing = Message {
            role: Role::Assistant,
            content: vec![ContentBlock::Reasoning {
                text: String::new(),
                signature: None,
                provider: Some("openai".into()),
                opaque: None,
            }],
        };
        assert!(build_request("o3", 100, &[missing], &[], None).is_err());

        let malformed = Message {
            role: Role::Assistant,
            content: vec![ContentBlock::Reasoning {
                text: String::new(),
                signature: None,
                provider: Some("openai".into()),
                opaque: Some(json!({ "type": "reasoning", "id": "rs_1", "summary": [] })),
            }],
        };
        let error = build_request("o3", 100, &[malformed], &[], None).unwrap_err();
        assert!(error.to_string().contains("encrypted_content"));
    }

    #[test]
    fn chat_structured_option_is_translated_to_responses_text_format() {
        let mut options = Map::new();
        options.insert(
            "response_format".into(),
            json!({
                "type": "json_schema",
                "json_schema": {
                    "name": "invoice",
                    "strict": true,
                    "schema": { "type": "object" }
                }
            }),
        );
        options.insert("include".into(), json!(["message.output_text.logprobs"]));

        let request = build_request(
            "gpt-5",
            512,
            &[Message::user("invoice")],
            &[],
            Some(&options),
        )
        .unwrap();
        assert!(request.get("response_format").is_none());
        assert_eq!(request["text"]["format"]["type"], "json_schema");
        assert_eq!(request["text"]["format"]["name"], "invoice");
        assert_eq!(request["text"]["format"]["strict"], true);
        let includes = request["include"].as_array().unwrap();
        assert!(includes.contains(&json!("message.output_text.logprobs")));
        assert!(includes.contains(&json!("reasoning.encrypted_content")));
    }

    #[test]
    fn parser_preserves_reasoning_and_uses_call_id_for_fragmented_tools() {
        let reasoning = raw_reasoning();
        let events = vec![
            json!({
                "type": "response.created",
                "response": {
                    "id": "resp_123",
                    "object": "response",
                    "created_at": 1720000000,
                    "model": "o3",
                    "status": "in_progress",
                    "service_tier": "default"
                }
            }),
            json!({
                "type": "response.output_text.delta",
                "item_id": "msg_123",
                "output_index": 2,
                "content_index": 0,
                "delta": "",
                "logprobs": [{ "token": "done", "logprob": -0.2 }]
            }),
            json!({
                "type": "response.output_item.done",
                "output_index": 0,
                "item": reasoning.clone()
            }),
            json!({
                "type": "response.output_item.added",
                "output_index": 1,
                "item": {
                    "type": "function_call",
                    "id": "fc_123",
                    "call_id": "call_123",
                    "name": "search",
                    "arguments": "",
                    "status": "in_progress"
                }
            }),
            json!({
                "type": "response.function_call_arguments.delta",
                "item_id": "fc_123",
                "output_index": 1,
                "delta": "{\"q\":"
            }),
            json!({
                "type": "response.function_call_arguments.delta",
                "item_id": "fc_123",
                "output_index": 1,
                "delta": "\"aikit\"}"
            }),
            json!({
                "type": "response.function_call_arguments.done",
                "item_id": "fc_123",
                "output_index": 1,
                "name": "search",
                "arguments": "{\"q\":\"aikit\"}"
            }),
            json!({
                "type": "response.output_item.done",
                "output_index": 1,
                "item": {
                    "type": "function_call",
                    "id": "fc_123",
                    "call_id": "call_123",
                    "name": "search",
                    "arguments": "{\"q\":\"aikit\"}",
                    "status": "completed"
                }
            }),
            json!({
                "type": "response.completed",
                "response": {
                    "id": "resp_123",
                    "object": "response",
                    "model": "o3",
                    "status": "completed",
                    "completed_at": 1720000001,
                    "service_tier": "default",
                    "output": [
                        reasoning.clone(),
                        {
                            "type": "function_call",
                            "id": "fc_123",
                            "call_id": "call_123",
                            "name": "search",
                            "arguments": "{\"q\":\"aikit\"}",
                            "status": "completed"
                        }
                    ],
                    "usage": {
                        "input_tokens": 40,
                        "output_tokens": 18,
                        "input_tokens_details": { "cached_tokens": 7 },
                        "output_tokens_details": { "reasoning_tokens": 12 }
                    }
                }
            }),
        ];

        let mut parser = OpenAiResponsesStreamParser::new();
        let mut out = Vec::new();
        for event in &events {
            out.extend(parser.push_event(event));
        }
        assert!(
            parser.finish().is_empty(),
            "terminal deltas were emitted twice"
        );

        assert_eq!(out[0], StreamDelta::MessageStart { model: "o3".into() });
        assert!(out.contains(&StreamDelta::ReasoningComplete {
            text: "Checked the inputs.".into(),
            signature: None,
            opaque: Some(reasoning),
        }));
        assert!(out.contains(&StreamDelta::ToolCallStart {
            id: "call_123".into(),
            name: "search".into(),
        }));
        assert!(!out.iter().any(|delta| matches!(
            delta,
            StreamDelta::ToolCallStart { id, .. } if id == "fc_123"
        )));
        assert_eq!(
            out.iter()
                .filter(|delta| matches!(delta, StreamDelta::ToolCallInput { .. }))
                .count(),
            1,
            "arguments.done and output_item.done duplicated the tool input"
        );
        assert!(out.contains(&StreamDelta::ToolCallInput {
            id: "call_123".into(),
            input: json!({ "q": "aikit" }),
        }));
        assert!(out.contains(&StreamDelta::Usage(Usage {
            input_tokens: 40,
            output_tokens: 18,
            cache_read_input_tokens: 7,
            reasoning_tokens: 12,
            ..Default::default()
        })));
        assert!(out.contains(&StreamDelta::MessageStop {
            stop_reason: "tool_use".into(),
        }));
        let metadata = out
            .iter()
            .find_map(|delta| match delta {
                StreamDelta::ProviderMetadata { provider, metadata } if provider == "openai" => {
                    Some(metadata)
                }
                _ => None,
            })
            .expect("openai provider metadata");
        assert_eq!(metadata["id"], "resp_123");
        assert_eq!(metadata["status"], "completed");
        assert_eq!(metadata["service_tier"], "default");
        assert_eq!(
            metadata["usage"][0]["input_tokens_details"]["cached_tokens"],
            7
        );
        assert_eq!(metadata["logprobs"][0]["item_id"], "msg_123");
        assert_eq!(metadata["logprobs"][0]["logprobs"][0]["token"], "done");
        assert!(metadata.get("output").is_none());
    }

    #[test]
    fn parser_reports_malformed_arguments_instead_of_running_null_input() {
        let events = [
            json!({
                "type": "response.created",
                "response": { "model": "o3" }
            }),
            json!({
                "type": "response.output_item.added",
                "item": {
                    "type": "function_call",
                    "id": "fc_bad",
                    "call_id": "call_bad",
                    "name": "search",
                    "arguments": ""
                }
            }),
            json!({
                "type": "response.function_call_arguments.done",
                "item_id": "fc_bad",
                "name": "search",
                "arguments": "{\"q\":"
            }),
        ];
        let mut parser = OpenAiResponsesStreamParser::new();
        let out: Vec<_> = events
            .iter()
            .flat_map(|event| parser.push_event(event))
            .collect();
        assert!(out.iter().any(|delta| matches!(
            delta,
            StreamDelta::Error { message, .. } if message.contains("malformed")
        )));
        assert!(!out.iter().any(|delta| matches!(
            delta,
            StreamDelta::ToolCallInput { input, .. } if input.is_null()
        )));
    }

    #[tokio::test]
    async fn provider_streams_typed_responses_sse_over_real_http() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let sse = concat!(
            "data: {\"type\":\"response.created\",\"response\":{\"model\":\"gpt-5\"}}\n\n",
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"Merhaba\",\"item_id\":\"msg_1\",\"output_index\":0,\"content_index\":0}\n\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"model\":\"gpt-5\",\"status\":\"completed\",\"output\":[],\"usage\":{\"input_tokens\":3,\"output_tokens\":2,\"input_tokens_details\":{\"cached_tokens\":0},\"output_tokens_details\":{\"reasoning_tokens\":0}}}}\n\n",
        );
        Mock::given(method("POST"))
            .and(path("/responses"))
            .respond_with(ResponseTemplate::new(200).set_body_raw(sse, "text/event-stream"))
            .mount(&server)
            .await;

        let provider = OpenAiResponsesProvider::with_base_url("sk-test", server.uri());
        let request = ProviderRequest {
            model: "gpt-5".into(),
            messages: vec![Message::user("selam")],
            tools: vec![],
            max_tokens: 100,
            options: Map::new(),
            provider_options: crate::types::ProviderOptions::new(),
        };
        let out: Vec<_> = provider.stream(request).await.unwrap().collect().await;
        assert!(out.contains(&StreamDelta::TextDelta {
            text: "Merhaba".into()
        }));
        assert!(out.contains(&StreamDelta::MessageStop {
            stop_reason: "end_turn".into()
        }));

        let received = server.received_requests().await.unwrap();
        assert_eq!(received.len(), 1);
        let body: Value = serde_json::from_slice(&received[0].body).unwrap();
        assert_eq!(body["model"], "gpt-5");
        assert_eq!(body["store"], false);
        assert_eq!(body["input"][0]["role"], "user");
        assert!(body.get("messages").is_none());
    }
}
