//! Google Gemini (`generateContent`) wire ↔ canonical translation.
//!
//! Gemini speaks its own REST shape (not OpenAI-compatible). The load-bearing detail here is the
//! reasoning-replay rule: Gemini uses a **PreserveThoughtSignature** policy — on a follow-up turn
//! you replay the model's thought part carrying its `thoughtSignature` so the server can validate
//! the thought chain (see `crate::reasoning`). [`build_request`] therefore re-emits assistant
//! `Reasoning` blocks as `{ text: "", thought: true, thoughtSignature: <sig> }` parts.
//!
//! Wire-format field names verified against the
//! [official docs](https://ai.google.dev/api/generate-content):
//!   - Request: `contents[]{ role:"user"|"model", parts:[...] }`, `systemInstruction{ parts:[{text}] }`,
//!     `tools[]{ functionDeclarations[]{ name, description, parameters } }`,
//!     `generationConfig{ maxOutputTokens, thinkingConfig }`.
//!   - Part variants: `{text}`, `{functionCall:{name, args}}`, `{functionResponse:{name, response}}`,
//!     and thinking parts carrying `thought: true` + `thoughtSignature`.
//!   - Streaming (`streamGenerateContent?alt=sse`): SSE `data: {GenerateContentResponse}` chunks with
//!     `candidates[0].content.parts[]`, `candidates[0].finishReason` (`STOP`, `MAX_TOKENS`, ...),
//!     `usageMetadata{ promptTokenCount, candidatesTokenCount, thoughtsTokenCount }`. There is NO
//!     `[DONE]` sentinel — the byte stream simply ends.

use super::{Provider, ProviderRequest};
use crate::types::{ContentBlock, MediaSource, Message, Role, StreamDelta, ToolSpec, Usage};
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::StreamExt;
use serde_json::{Map, Value};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};

/// Process-global counter for synthesizing tool-call ids. Gemini's stream does not carry call ids,
/// so we mint our own; they must be unique across the WHOLE process (not merely within one parser)
/// or a multi-turn tool loop would restart at `call_0` each turn, colliding with a prior turn's id
/// and making `build_request`'s id→function-name lookup attribute a tool result to the WRONG call.
static NEXT_TOOL_CALL: AtomicU64 = AtomicU64::new(0);

fn next_tool_call_id(counter: &AtomicU64) -> Option<String> {
    counter
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            current.checked_add(1)
        })
        .ok()
        .map(|current| format!("call_{current}"))
}

/// Build a Gemini `generateContent` request body from canonical inputs. The serialize
/// counterpart of [`GeminiStreamParser`]. Role mapping:
///  - `System` → top-level `systemInstruction: { parts: [{text}] }`.
///  - `User` → `contents[]{ role: "user", parts: [{text}] }`.
///  - `Assistant` → `contents[]{ role: "model", parts: [ text, {functionCall}, thought parts ] }`.
///    `Reasoning` blocks replay as `{ text: "", thought: true, thoughtSignature: <sig> }` to
///    PRESERVE the thought signature (PreserveThoughtSignature policy) — the signature, not the
///    thought text, is what Gemini validates on replay.
///  - `Tool` (tool results) → `contents[]{ role: "user", parts: [{functionResponse}] }`, keyed by
///    the tool *name* (looked up from a prior `ToolUse` by id) rather than the opaque call id.
///
/// `provider_options` is the typed escape hatch — merged into the top level (e.g.
/// `generationConfig`/`thinkingConfig` overrides, `safetySettings`); nested objects like
/// `generationConfig` are deep-merged field-by-field so caller overrides do not drop built
/// siblings such as `maxOutputTokens`. Pure and keyless.
pub fn build_request(
    model: &str,
    max_tokens: u64,
    messages: &[Message],
    tools: &[ToolSpec],
    provider_options: Option<&serde_json::Map<String, Value>>,
) -> crate::error::Result<Value> {
    super::reject_protected_options(
        "google",
        model,
        provider_options,
        &[
            "model",
            "contents",
            "systemInstruction",
            "max_tokens",
            "max_output_tokens",
            "max_completion_tokens",
            "generationConfig.maxOutputTokens",
            "tools",
            "stream",
            "stream_options",
        ],
    )?;
    super::validate_media_input_roles(messages, "google", model)?;
    // `functionResponse.name` must echo the called function's name; ContentBlock::ToolResult only
    // carries the opaque tool_use_id, so build an id→name lookup from prior ToolUse blocks.
    let mut tool_names: HashMap<&str, &str> = HashMap::new();
    for m in messages {
        for b in &m.content {
            if let ContentBlock::ToolUse { id, name, .. } = b {
                tool_names.insert(id.as_str(), name.as_str());
            }
        }
    }
    // `model` is only referenced through the URL for Gemini; keep the binding meaningful.
    let _ = model;

    let mut system_parts: Vec<Value> = Vec::new();
    let mut contents: Vec<Value> = Vec::new();

    for m in messages {
        match m.role {
            // Gemini takes the system prompt at the top level as `systemInstruction`.
            Role::System => {
                for b in &m.content {
                    if let ContentBlock::Text { text } = b {
                        system_parts.push(serde_json::json!({ "text": text }));
                    }
                }
            }
            Role::User => {
                let mut parts = Vec::new();
                for block in &m.content {
                    let part = match block {
                        ContentBlock::Text { text } => Some(serde_json::json!({ "text": text })),
                        ContentBlock::Media { media_type, source } => Some(match source {
                            MediaSource::Url { url } => {
                                validate_google_file_uri(model, url)?;
                                serde_json::json!({
                                    "fileData": { "mimeType": media_type, "fileUri": url },
                                })
                            }
                            MediaSource::Base64 { data } => serde_json::json!({
                                "inlineData": { "mimeType": media_type, "data": data },
                            }),
                        }),
                        ContentBlock::MediaInput { media } => {
                            Some(match super::resolve_media_input(media, "google", model)? {
                                super::ResolvedMediaInput::Base64(data) => serde_json::json!({
                                    "inlineData": {
                                        "mimeType": media.media_type,
                                        "data": data,
                                    },
                                }),
                            })
                        }
                        _ => None,
                    };
                    if let Some(part) = part {
                        parts.push(part);
                    }
                }
                contents.push(serde_json::json!({ "role": "user", "parts": parts }));
            }
            Role::Assistant => {
                let mut parts: Vec<Value> = Vec::new();
                let mut function_call_signatures = BTreeMap::new();
                for block in &m.content {
                    let ContentBlock::Reasoning {
                        provider, opaque, ..
                    } = block
                    else {
                        continue;
                    };
                    if provider.as_deref().is_some_and(|source| source != "google") {
                        continue;
                    }
                    let Some(signatures) = opaque
                        .as_ref()
                        .and_then(|value| value.get("google"))
                        .and_then(|value| value.get("function_call_signatures"))
                        .and_then(Value::as_object)
                    else {
                        continue;
                    };
                    for (id, signature) in signatures {
                        if let Some(signature) = signature.as_str() {
                            function_call_signatures.insert(id.clone(), signature.to_string());
                        }
                    }
                }
                for b in &m.content {
                    match b {
                        ContentBlock::Text { text } => {
                            parts.push(serde_json::json!({ "text": text }));
                        }
                        // PreserveThoughtSignature: replay the signed thought part so Gemini can
                        // validate the reasoning chain. The signature is load-bearing; the text is not.
                        ContentBlock::Reasoning {
                            text,
                            signature,
                            provider,
                            ..
                        } => {
                            if provider.as_deref().is_some_and(|source| source != "google") {
                                continue;
                            }
                            // An empty reasoning block may exist only to carry opaque per-tool
                            // signatures. Those signatures belong on their original functionCall
                            // parts, not on a synthetic thought part.
                            if text.is_empty() && signature.is_none() {
                                continue;
                            }
                            let mut p = serde_json::json!({ "text": text, "thought": true });
                            if let Some(sig) = signature {
                                p["thoughtSignature"] = Value::String(sig.clone());
                            }
                            parts.push(p);
                        }
                        ContentBlock::ToolUse {
                            id, name, input, ..
                        } => {
                            let mut part = serde_json::json!({
                                "functionCall": { "name": name, "args": input },
                            });
                            if let Some(signature) = function_call_signatures.get(id) {
                                part["thoughtSignature"] = Value::String(signature.clone());
                            }
                            parts.push(part);
                        }
                        _ => {}
                    }
                }
                contents.push(serde_json::json!({ "role": "model", "parts": parts }));
            }
            // Gemini has no "tool" role — tool results ride in a user turn as functionResponse parts.
            Role::Tool => {
                let mut parts: Vec<Value> = Vec::new();
                for b in &m.content {
                    if let ContentBlock::ToolResult {
                        tool_use_id,
                        content,
                        is_error,
                    } = b
                    {
                        let name = tool_names
                            .get(tool_use_id.as_str())
                            .copied()
                            .unwrap_or(tool_use_id.as_str());
                        // Gemini's functionResponse.response is a free-form object; surface the
                        // error flag so the model can tell a failed tool call from a successful one.
                        let response = if *is_error {
                            serde_json::json!({ "error": content })
                        } else {
                            serde_json::json!({ "result": content })
                        };
                        parts.push(serde_json::json!({
                            "functionResponse": {
                                "name": name,
                                "response": response,
                            },
                        }));
                    }
                }
                contents.push(serde_json::json!({ "role": "user", "parts": parts }));
            }
        }
    }

    let mut req = serde_json::json!({
        "contents": contents,
        "generationConfig": { "maxOutputTokens": max_tokens },
    });
    if !system_parts.is_empty() {
        req["systemInstruction"] = serde_json::json!({ "parts": system_parts });
    }
    if !tools.is_empty() {
        let declarations: Vec<Value> = tools
            .iter()
            .map(|t| {
                serde_json::json!({
                    "name": t.name,
                    "description": t.description,
                    "parameters": t.input_schema,
                })
            })
            .collect();
        req["tools"] = serde_json::json!([{ "functionDeclarations": declarations }]);
    }
    // Typed escape hatch: merge provider_options at the top level (thinkingConfig, safetySettings,
    // generationConfig overrides, ...). Nested objects (e.g. `generationConfig`) are DEEP-merged
    // field-by-field so a caller overriding one field (temperature) does not clobber built siblings
    // (maxOutputTokens); caller fields win on conflict. Non-object values overwrite as before.
    if let (Some(opts), Value::Object(map)) = (provider_options, &mut req) {
        for (k, v) in opts {
            match (map.get_mut(k), v) {
                (Some(Value::Object(existing)), Value::Object(incoming)) => {
                    for (nk, nv) in incoming {
                        existing.insert(nk.clone(), nv.clone());
                    }
                }
                _ => {
                    map.insert(k.clone(), v.clone());
                }
            }
        }
    }
    Ok(req)
}

fn validate_google_file_uri(model: &str, uri: &str) -> crate::error::Result<()> {
    let parsed = url::Url::parse(uri).ok();
    let is_managed_uri = parsed.as_ref().is_some_and(|parsed| {
        let has_clean_authority = parsed.username().is_empty()
            && parsed.password().is_none()
            && parsed.port().is_none()
            && parsed.query().is_none()
            && parsed.fragment().is_none();
        if !has_clean_authority {
            return false;
        }

        match parsed.scheme() {
            // A bucket alone is not a file. Require both a canonical bucket authority and a
            // non-empty object path so `gs://bucket/` cannot escape local preflight.
            "gs" => parsed
                .host_str()
                .is_some_and(|bucket| !bucket.is_empty())
                && !parsed.path().trim_matches('/').is_empty(),
            "https" => {
                if parsed.host_str() != Some("generativelanguage.googleapis.com") {
                    return false;
                }
                let segments = parsed
                    .path_segments()
                    .map(|segments| segments.collect::<Vec<_>>())
                    .unwrap_or_default();
                matches!(segments.as_slice(), ["v1" | "v1beta", "files", file_id] if !file_id.is_empty())
            }
            _ => false,
        }
    });
    if is_managed_uri {
        return Ok(());
    }
    Err(crate::error::ProviderError::new(
        "google",
        model,
        crate::error::ProviderErrorKind::InvalidRequest,
        "Google fileData requires a Google-managed gs:// or Files API URI; fetch ordinary web URLs through governed egress and send verified inline bytes",
    )
    .into())
}

/// Stateful translator for Gemini `streamGenerateContent` SSE chunks → canonical [`StreamDelta`]s.
///
/// Feed decoded `GenerateContentResponse` objects via [`GeminiStreamParser::push_chunk`]; call
/// [`GeminiStreamParser::finish`] when the byte stream ends (Gemini has no `[DONE]` sentinel).
/// Gemini streams parts across chunks: text/thought are accumulated and thought text emitted
/// incrementally; tool calls are emitted the moment a `functionCall` part is seen; the reasoning
/// block is completed (with its signature) on `finish`.
pub struct GeminiStreamParser {
    model: String,
    started: bool,
    reasoning_text: String,
    reasoning_signature: Option<String>,
    function_call_signatures: BTreeMap<String, String>,
    saw_reasoning: bool,
    has_function_call: bool,
    stop_reason: String,
    usage: Usage,
    seen_citations: BTreeSet<String>,
    metadata: Map<String, Value>,
    terminal: bool,
    failed: bool,
    retention: super::StreamRetentionBudget,
}

impl GeminiStreamParser {
    pub fn new(model: impl Into<String>) -> Self {
        let model = model.into();
        GeminiStreamParser {
            model: if model.is_empty() {
                "gemini".into()
            } else {
                model
            },
            started: false,
            reasoning_text: String::new(),
            reasoning_signature: None,
            function_call_signatures: BTreeMap::new(),
            saw_reasoning: false,
            has_function_call: false,
            stop_reason: String::new(),
            usage: Usage::default(),
            seen_citations: BTreeSet::new(),
            metadata: Map::new(),
            terminal: false,
            failed: false,
            retention: super::StreamRetentionBudget::default(),
        }
    }

    pub fn is_terminal(&self) -> bool {
        self.terminal
    }

    /// Translate one decoded Gemini SSE chunk into zero or more canonical deltas.
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
            out.push(StreamDelta::MessageStart {
                model: self.model.clone(),
            });
        }

        if let Some(u) = chunk.get("usageMetadata").filter(|u| u.is_object()) {
            if !self.absorb_usage(u) {
                return self.retained_state_failure();
            }
        }

        // Gemini nests output under candidates[0].content.parts[].
        let candidate = chunk
            .get("candidates")
            .and_then(Value::as_array)
            .and_then(|c| c.first());
        let Some(candidate) = candidate else {
            return out;
        };
        if !self.capture_candidate_metadata(candidate) {
            return self.retained_state_failure();
        }

        if let Some(parts) = candidate
            .get("content")
            .and_then(|c| c.get("parts"))
            .and_then(Value::as_array)
        {
            for part in parts {
                let is_thought = part
                    .get("thought")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                if is_thought {
                    // A thinking part: `text` holds the reasoning; `thoughtSignature` (when present)
                    // must be captured so it can be replayed verbatim next turn.
                    if let Some(sig) = part.get("thoughtSignature").and_then(Value::as_str) {
                        if !self.retention.retain(sig.len(), 0) {
                            return self.retained_state_failure();
                        }
                        self.reasoning_signature = Some(sig.to_string());
                    }
                    self.saw_reasoning = true;
                    if let Some(text) = part.get("text").and_then(Value::as_str) {
                        if !text.is_empty() {
                            if !self.retention.retain(text.len(), 0) {
                                return self.retained_state_failure();
                            }
                            self.reasoning_text.push_str(text);
                            out.push(StreamDelta::ReasoningDelta { text: text.into() });
                        }
                    }
                } else if let Some(fc) = part.get("functionCall") {
                    // Gemini attaches `thoughtSignature` to functionCall parts too (not just
                    // `thought:true` parts), so on a thinking+tools turn the signature can ride
                    // here — capture it or PreserveThoughtSignature would DROP it.
                    let name = fc
                        .get("name")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string();
                    if name.is_empty() {
                        return self
                            .terminal_protocol_failure("Gemini functionCall is missing its name");
                    }
                    let input = fc
                        .get("args")
                        .cloned()
                        .unwrap_or_else(|| Value::Object(Default::default()));
                    // Globally-unique id (see NEXT_TOOL_CALL) so multi-turn tool loops never collide.
                    let Some(id) = next_tool_call_id(&NEXT_TOOL_CALL) else {
                        return self
                            .terminal_protocol_failure("Gemini tool-call id space is exhausted");
                    };
                    if let Some(signature) = part.get("thoughtSignature").and_then(Value::as_str) {
                        // Keep the signature associated with this exact canonical call id. Gemini
                        // 3 rejects a replay when it is moved onto a synthetic thought part.
                        if !self.retention.retain(
                            id.len().saturating_add(signature.len()),
                            usize::from(!self.function_call_signatures.contains_key(&id)),
                        ) {
                            return self.retained_state_failure();
                        }
                        self.function_call_signatures
                            .insert(id.clone(), signature.to_string());
                        self.saw_reasoning = true;
                    }
                    self.has_function_call = true;
                    out.push(StreamDelta::ToolCallStart {
                        id: id.clone(),
                        name,
                    });
                    out.push(StreamDelta::ToolCallInput { id, input });
                } else if let Some(text) = part.get("text").and_then(Value::as_str) {
                    // A plain-text part can also carry the turn's `thoughtSignature`; capture it so
                    // PreserveThoughtSignature works even when no `thought:true` part was streamed.
                    if let Some(sig) = part.get("thoughtSignature").and_then(Value::as_str) {
                        if !self.retention.retain(sig.len(), 0) {
                            return self.retained_state_failure();
                        }
                        self.reasoning_signature = Some(sig.to_string());
                        self.saw_reasoning = true;
                    }
                    if !text.is_empty() {
                        out.push(StreamDelta::TextDelta { text: text.into() });
                    }
                } else if part.get("inlineData").is_some() || part.get("fileData").is_some() {
                    return self.terminal_protocol_failure(
                        "Gemini returned media output that this text stream cannot represent",
                    );
                }
            }
        }

        if let Some(fr) = candidate.get("finishReason").and_then(Value::as_str) {
            if fr.is_empty() {
                return self.terminal_protocol_failure("Gemini finishReason must not be empty");
            }
            if !self.retention.retain(fr.len(), 0) {
                return self.retained_state_failure();
            }
            self.terminal = true;
            self.stop_reason = map_finish_reason(fr);
        }

        if let Some(chunks) = candidate
            .get("groundingMetadata")
            .and_then(|metadata| metadata.get("groundingChunks"))
            .and_then(Value::as_array)
        {
            for chunk in chunks {
                let Some(web) = chunk.get("web") else {
                    continue;
                };
                let source = web.get("uri").and_then(Value::as_str).map(str::to_string);
                let text = web
                    .get("title")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                let key = serde_json::to_string(chunk).unwrap_or_default();
                let is_new = !self.seen_citations.contains(&key);
                if is_new && !self.retention.retain(key.len(), 1) {
                    return self.retained_state_failure();
                }
                if self.seen_citations.insert(key) {
                    out.push(StreamDelta::Citation {
                        text,
                        source,
                        metadata: Some(chunk.clone()),
                    });
                }
            }
        }

        out
    }

    /// Call at end-of-stream: complete any reasoning block (with its signature), then Usage +
    /// MessageStop. A `functionCall` seen anywhere forces `tool_use` regardless of `finishReason`.
    pub fn finish(&mut self) -> Vec<StreamDelta> {
        if self.failed {
            return Vec::new();
        }
        if !self.terminal {
            return vec![super::protocol_failure(
                "google",
                "Gemini stream ended before finishReason",
            )];
        }
        let mut out = Vec::new();
        if self.saw_reasoning {
            let function_call_signatures = std::mem::take(&mut self.function_call_signatures);
            let opaque = (!function_call_signatures.is_empty()).then(|| {
                serde_json::json!({
                    "google": {
                        "function_call_signatures": function_call_signatures,
                    }
                })
            });
            out.push(StreamDelta::ReasoningComplete {
                text: std::mem::take(&mut self.reasoning_text),
                signature: self.reasoning_signature.take(),
                opaque,
            });
        }
        if !self.metadata.is_empty() {
            out.push(StreamDelta::ProviderMetadata {
                provider: "google".into(),
                metadata: Value::Object(std::mem::take(&mut self.metadata)),
            });
        }
        out.push(StreamDelta::Usage(self.usage));
        let stop_reason = if self.has_function_call {
            "tool_use".to_string()
        } else if !self.stop_reason.is_empty() {
            std::mem::take(&mut self.stop_reason)
        } else {
            "end_turn".to_string()
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
                .entry("usageMetadata")
                .or_insert_with(|| Value::Array(Vec::new()))
                .as_array_mut()
                .expect("usage metadata is initialized as an array")
                .push(Value::Object(raw.clone()));
        }
        if let Some(n) = u.get("promptTokenCount").and_then(Value::as_u64) {
            self.usage.input_tokens = n;
        }
        if let Some(n) = u.get("candidatesTokenCount").and_then(Value::as_u64) {
            self.usage.output_tokens = n;
        }
        if let Some(n) = u.get("thoughtsTokenCount").and_then(Value::as_u64) {
            self.usage.reasoning_tokens = n;
        }
        if let Some(n) = u.get("cachedContentTokenCount").and_then(Value::as_u64) {
            self.usage.cache_read_input_tokens = n;
        }
        true
    }

    fn capture_response_metadata(&mut self, chunk: &Value) -> bool {
        for field in ["responseId", "modelVersion", "promptFeedback"] {
            if let Some(value) = chunk.get(field) {
                if !self.retain_metadata_value(field, value) {
                    return false;
                }
                self.metadata.insert(field.into(), value.clone());
            }
        }
        true
    }

    fn capture_candidate_metadata(&mut self, candidate: &Value) -> bool {
        let mut metadata = Map::new();
        for field in [
            "index",
            "finishReason",
            "finishMessage",
            "safetyRatings",
            "citationMetadata",
            "groundingMetadata",
            "urlContextMetadata",
            "avgLogprobs",
            "logprobsResult",
        ] {
            if let Some(value) = candidate.get(field) {
                metadata.insert(field.into(), value.clone());
            }
        }
        if !metadata.is_empty() {
            let value = Value::Object(metadata);
            if !self.retention.retain_json(&value, 1) {
                return false;
            }
            self.metadata
                .entry("candidates")
                .or_insert_with(|| Value::Array(Vec::new()))
                .as_array_mut()
                .expect("candidate metadata is initialized as an array")
                .push(value);
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
        self.failed = true;
        self.reasoning_text.clear();
        self.reasoning_signature = None;
        self.function_call_signatures.clear();
        self.stop_reason.clear();
        self.seen_citations.clear();
        self.metadata.clear();
        vec![super::retained_state_failure("google")]
    }

    fn terminal_protocol_failure(&mut self, message: impl Into<String>) -> Vec<StreamDelta> {
        self.terminal = true;
        self.failed = true;
        self.reasoning_text.clear();
        self.reasoning_signature = None;
        self.function_call_signatures.clear();
        self.stop_reason.clear();
        self.seen_citations.clear();
        self.metadata.clear();
        vec![super::protocol_failure("google", message)]
    }
}

fn map_finish_reason(fr: &str) -> String {
    match fr {
        "STOP" => "end_turn",
        "MAX_TOKENS" => "max_tokens",
        other => other,
    }
    .to_string()
}

const GOOGLE_DEFAULT_BASE: &str = "https://generativelanguage.googleapis.com";

/// Live Google Gemini adapter: `build_request` → POST (SSE) → [`GeminiStreamParser`] → canonical
/// [`StreamDelta`]s. `base_url` is overridable for tests (point it at a mock server).
pub struct GeminiProvider {
    api_key: String,
    base_url: String,
    client: reqwest::Client,
}

impl GeminiProvider {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self::with_base_url(api_key, GOOGLE_DEFAULT_BASE)
    }

    pub fn with_base_url(api_key: impl Into<String>, base_url: impl Into<String>) -> Self {
        GeminiProvider {
            api_key: api_key.into(),
            base_url: base_url.into(),
            client: super::native_http_client(),
        }
    }
}

#[async_trait]
impl Provider for GeminiProvider {
    fn name(&self) -> &str {
        "google"
    }

    async fn stream(
        &self,
        req: ProviderRequest,
    ) -> crate::error::Result<BoxStream<'static, StreamDelta>> {
        let validated = req.validated_options_for(self.name(), super::GOOGLE_OPTIONS)?;
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
                "google",
                &req.model,
                crate::error::ProviderErrorKind::InvalidRequest,
                error.to_string(),
            )
            .with_warnings(warnings.clone())
        })?;
        // The model name lives in the URL path for Gemini; `?alt=sse` selects SSE framing.
        let url = format!(
            "{}/v1beta/models/{}:streamGenerateContent?alt=sse",
            self.base_url.trim_end_matches('/'),
            req.model,
        );
        let resp = self
            .client
            .post(&url)
            .header("x-goog-api-key", &self.api_key)
            .header("accept", "text/event-stream")
            .json(&body)
            .send()
            .await
            .map_err(|error| {
                super::transport_failure("google", &req.model, error)
                    .with_provider_warnings(warnings.clone())
            })?;

        if !resp.status().is_success() {
            let status = resp.status();
            let retry_after = resp.headers().get(reqwest::header::RETRY_AFTER).cloned();
            let text = super::read_error_body(resp).await;
            return Err(super::http_failure(
                "google",
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
            let mut parser = GeminiStreamParser::new(model.clone());
            let mut buf: Vec<u8> = Vec::new();
            while let Some(chunk) = byte_stream.next().await {
                match chunk {
                    Ok(bytes) => {
                        if !super::append_sse_chunk(&mut buf, &bytes) {
                            yield super::stream_failure(
                                "google",
                                &model,
                                crate::error::ProviderErrorKind::Protocol,
                                "Gemini SSE event exceeded the size limit",
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
                                // Gemini SSE has no `[DONE]` sentinel; every data line is JSON.
                                match serde_json::from_str::<Value>(data) {
                                    Ok(json) => {
                                        for d in parser.push_chunk(&json) {
                                            yield super::with_stream_context(d, "google", &model);
                                        }
                                        if parser.is_terminal() {
                                            for d in parser.finish() {
                                                yield super::with_stream_context(d, "google", &model);
                                            }
                                            return;
                                        }
                                    }
                                    Err(_) => {
                                        yield super::stream_failure(
                                            "google",
                                            &model,
                                            crate::error::ProviderErrorKind::Protocol,
                                            "malformed Gemini SSE data",
                                        );
                                        return;
                                    }
                                }
                            }
                        }
                    }
                    Err(error) => {
                        yield super::response_stream_failure(
                            "google",
                            &model,
                            error,
                            "Gemini",
                        );
                        return;
                    }
                }
            }
            // End of byte stream → flush terminal Usage + MessageStop (no `[DONE]` from Gemini).
            for d in parser.finish() {
                yield super::with_stream_context(d, "google", &model);
            }
        };
        Ok(super::prepend_provider_warnings(Box::pin(out), warnings))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contract::{MediaInput, MediaInputSource};
    use serde_json::json;

    #[test]
    fn strict_media_preserves_mime_and_validated_bytes() {
        let media = MediaInput {
            media_type: "audio/wav".into(),
            source: MediaInputSource::Bytes {
                data: b"abc".to_vec(),
            },
            sha256: "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad".into(),
            size_bytes: 3,
        };
        let message = Message::user("transcribe").with_media_input(media).unwrap();
        let request = build_request("gemini-test", 64, &[message], &[], None).unwrap();
        let part = &request["contents"][0]["parts"][1]["inlineData"];
        assert_eq!(part["mimeType"], "audio/wav");
        assert_eq!(part["data"], "YWJj");
    }

    #[test]
    fn tool_call_counter_exhaustion_never_wraps_to_a_duplicate_id() {
        let counter = AtomicU64::new(u64::MAX - 1);
        assert_eq!(
            next_tool_call_id(&counter),
            Some(format!("call_{}", u64::MAX - 1))
        );
        assert_eq!(next_tool_call_id(&counter), None);
        assert_eq!(next_tool_call_id(&counter), None);
    }

    #[test]
    fn build_request_maps_url_and_inline_media_parts() {
        let messages = vec![Message {
            role: Role::User,
            content: vec![
                ContentBlock::Text { text: "see".into() },
                ContentBlock::Media {
                    media_type: "image/png".into(),
                    source: MediaSource::Url {
                        url: "gs://bucket/image.png".into(),
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
        let request = build_request("gemini-test", 100, &messages, &[], None).unwrap();
        let parts = request["contents"][0]["parts"].as_array().unwrap();
        assert_eq!(parts[1]["fileData"]["fileUri"], "gs://bucket/image.png");
        assert_eq!(parts[2]["inlineData"]["mimeType"], "image/jpeg");
        assert_eq!(parts[2]["inlineData"]["data"], "AQID");
    }

    #[test]
    fn build_request_rejects_ordinary_web_url_as_file_data() {
        let message = Message {
            role: Role::User,
            content: vec![ContentBlock::Media {
                media_type: "image/png".into(),
                source: MediaSource::Url {
                    url: "https://example.test/image.png".into(),
                },
            }],
        };
        let error = build_request("gemini-test", 100, &[message], &[], None).unwrap_err();
        assert!(matches!(
            error.provider_error(),
            Some(error) if error.kind == crate::error::ProviderErrorKind::InvalidRequest
        ));
    }

    #[test]
    fn build_request_accepts_canonical_google_files_api_uri() {
        let message = Message {
            role: Role::User,
            content: vec![ContentBlock::Media {
                media_type: "image/png".into(),
                source: MediaSource::Url {
                    url: "https://generativelanguage.googleapis.com/v1beta/files/file-123".into(),
                },
            }],
        };
        let request = build_request("gemini-test", 100, &[message], &[], None).unwrap();
        assert_eq!(
            request["contents"][0]["parts"][0]["fileData"]["fileUri"],
            "https://generativelanguage.googleapis.com/v1beta/files/file-123"
        );
    }

    #[test]
    fn build_request_rejects_malformed_or_lookalike_google_file_uris() {
        let invalid = [
            "gs://bucket/",
            "https://generativelanguage.googleapis.com/",
            "https://generativelanguage.googleapis.com/v1beta/models/gemini",
            "https://generativelanguage.googleapis.com.evil.test/v1beta/files/file-123",
            "https://user@generativelanguage.googleapis.com/v1beta/files/file-123",
            "https://generativelanguage.googleapis.com:444/v1beta/files/file-123",
        ];

        for uri in invalid {
            let message = Message {
                role: Role::User,
                content: vec![ContentBlock::Media {
                    media_type: "image/png".into(),
                    source: MediaSource::Url { url: uri.into() },
                }],
            };
            let error = build_request("gemini-test", 100, &[message], &[], None).unwrap_err();
            assert!(
                matches!(
                    error.provider_error(),
                    Some(error) if error.kind == crate::error::ProviderErrorKind::InvalidRequest
                ),
                "URI should fail local preflight: {uri}"
            );
        }
    }

    #[test]
    fn grounding_chunks_emit_deduplicated_citations_with_raw_metadata() {
        let mut parser = GeminiStreamParser::new("gemini-test");
        let chunk = json!({
            "candidates": [{
                "content": { "parts": [{ "text": "Grounded." }] },
                "groundingMetadata": {
                    "groundingChunks": [{
                        "web": { "uri": "https://example.test", "title": "Example" }
                    }]
                }
            }]
        });
        let first = parser.push_chunk(&chunk);
        let second = parser.push_chunk(&chunk);
        assert!(first.iter().any(|delta| matches!(
            delta,
            StreamDelta::Citation {
                text,
                source: Some(source),
                metadata: Some(_),
            } if text == "Example" && source == "https://example.test"
        )));
        assert!(!second
            .iter()
            .any(|delta| matches!(delta, StreamDelta::Citation { .. })));
    }

    #[test]
    fn build_request_extracts_system_and_preserves_thought_signature() {
        let messages = vec![
            Message::system("You are helpful."),
            Message::user("search for merhaba"),
            Message {
                role: Role::Assistant,
                content: vec![
                    // PreserveThoughtSignature: this signature MUST survive to the wire.
                    ContentBlock::Reasoning {
                        text: "I'll search.".into(),
                        signature: Some("thought_sig_XYZ".into()),
                        provider: Some("google".into()),
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

        let req = build_request("gemini-2.5-pro", 2048, &messages, &tools, None).unwrap();

        // System extracted to the top-level systemInstruction (not a content entry).
        assert_eq!(
            req["systemInstruction"]["parts"][0]["text"],
            "You are helpful."
        );
        // generationConfig.maxOutputTokens carries the token ceiling.
        assert_eq!(req["generationConfig"]["maxOutputTokens"], 2048);

        let contents = req["contents"].as_array().unwrap();
        // user, model, user(functionResponse) → 3 content entries.
        assert_eq!(contents.len(), 3);
        assert_eq!(contents[0]["role"], "user");

        // Assistant → role "model" with a preserved thoughtSignature and a functionCall part.
        let model_turn = &contents[1];
        assert_eq!(model_turn["role"], "model");
        let parts = model_turn["parts"].as_array().unwrap();
        // The thought part carries thought:true + the preserved thoughtSignature.
        assert_eq!(parts[0]["thought"], true);
        assert_eq!(parts[0]["thoughtSignature"], "thought_sig_XYZ");
        // The functionCall part maps ToolUse → {name, args}.
        assert_eq!(parts[1]["functionCall"]["name"], "search");
        assert_eq!(parts[1]["functionCall"]["args"]["q"], "merhaba");

        // Tool result → a user turn with a functionResponse keyed by the tool NAME (looked up by id).
        let tool_turn = &contents[2];
        assert_eq!(tool_turn["role"], "user");
        assert_eq!(tool_turn["parts"][0]["functionResponse"]["name"], "search");
        assert_eq!(
            tool_turn["parts"][0]["functionResponse"]["response"]["result"],
            "3 results"
        );

        // Tools → functionDeclarations with parameters = input_schema.
        assert_eq!(req["tools"][0]["functionDeclarations"][0]["name"], "search");
        assert_eq!(
            req["tools"][0]["functionDeclarations"][0]["parameters"]["type"],
            "object"
        );
    }

    #[test]
    fn stream_parser_maps_text_thought_and_function_call() {
        // Thought part (with signature), then a functionCall part, then a finishReason+usage chunk.
        let mut p = GeminiStreamParser::new("gemini-2.5-pro");
        let mut out: Vec<StreamDelta> = Vec::new();
        out.extend(p.push_chunk(&json!({
            "candidates": [{ "content": { "role": "model", "parts": [
                { "text": "Let me think.", "thought": true, "thoughtSignature": "sig_abc" }
            ]}}]
        })));
        out.extend(p.push_chunk(&json!({
            "candidates": [{ "content": { "role": "model", "parts": [
                { "functionCall": { "name": "search", "args": { "q": "merhaba" } } }
            ]}}]
        })));
        out.extend(p.push_chunk(&json!({
            "responseId": "resp_123",
            "modelVersion": "gemini-2.5-pro-001",
            "candidates": [{
                "index": 0,
                "content": { "role": "model", "parts": [] },
                "finishReason": "STOP",
                "avgLogprobs": -0.25,
                "logprobsResult": { "chosenCandidates": [{ "token": "done", "logProbability": -0.25 }] },
                "groundingMetadata": {
                    "webSearchQueries": ["aikit"],
                    "groundingSupports": [{ "groundingChunkIndices": [0] }],
                    "groundingChunks": [{ "web": { "uri": "https://example.test", "title": "Example" } }]
                }
            }],
            "usageMetadata": { "promptTokenCount": 40, "candidatesTokenCount": 20, "thoughtsTokenCount": 12, "cachedContentTokenCount": 7 }
        })));
        out.extend(p.finish());

        assert_eq!(
            out[0],
            StreamDelta::MessageStart {
                model: "gemini-2.5-pro".into()
            }
        );
        // thought:true surfaces as a reasoning delta, completed with its captured signature.
        assert!(out.contains(&StreamDelta::ReasoningDelta {
            text: "Let me think.".into()
        }));
        assert!(out.contains(&StreamDelta::ReasoningComplete {
            text: "Let me think.".into(),
            signature: Some("sig_abc".into()),
            opaque: None,
        }));
        // functionCall → a generated id (globally unique, so assert the prefix, not "call_0") plus
        // the args as tool input under that same id.
        let tool_start_id = out
            .iter()
            .find_map(|d| match d {
                StreamDelta::ToolCallStart { id, name } if name == "search" => Some(id.clone()),
                _ => None,
            })
            .expect("expected a ToolCallStart for search");
        assert!(tool_start_id.starts_with("call_"));
        assert!(out.contains(&StreamDelta::ToolCallInput {
            id: tool_start_id,
            input: json!({ "q": "merhaba" }),
        }));
        // A functionCall anywhere forces tool_use even though finishReason was STOP.
        assert!(out.contains(&StreamDelta::MessageStop {
            stop_reason: "tool_use".into()
        }));
        // usageMetadata maps prompt/candidates/thoughts → input/output/reasoning tokens.
        assert!(out.contains(&StreamDelta::Usage(Usage {
            input_tokens: 40,
            output_tokens: 20,
            cache_read_input_tokens: 7,
            reasoning_tokens: 12,
            ..Default::default()
        })));
        let metadata = out
            .iter()
            .find_map(|delta| match delta {
                StreamDelta::ProviderMetadata { provider, metadata } if provider == "google" => {
                    Some(metadata)
                }
                _ => None,
            })
            .expect("google provider metadata");
        assert_eq!(metadata["responseId"], "resp_123");
        assert_eq!(metadata["modelVersion"], "gemini-2.5-pro-001");
        assert_eq!(metadata["usageMetadata"][0]["cachedContentTokenCount"], 7);
        assert_eq!(metadata["candidates"][0]["finishReason"], "STOP");
        assert_eq!(
            metadata["candidates"][0]["groundingMetadata"]["groundingSupports"][0]
                ["groundingChunkIndices"][0],
            0
        );
        assert_eq!(
            metadata["candidates"][0]["logprobsResult"]["chosenCandidates"][0]["token"],
            "done"
        );
        assert!(metadata["candidates"][0].get("content").is_none());
    }

    #[test]
    fn many_small_thought_fragments_fail_terminally() {
        let mut parser = GeminiStreamParser::new("gemini-test");
        parser.retention = crate::providers::StreamRetentionBudget::with_limits(8, 8);
        let fragment = json!({
            "candidates": [{"content": {"parts": [{"thought": true, "text": "x"}]}}]
        });
        let failure = (0..16)
            .find_map(|_| {
                parser
                    .push_chunk(&fragment)
                    .into_iter()
                    .find(|delta| matches!(delta, StreamDelta::Error { .. }))
            })
            .expect("many small thought fragments must exceed retained state");
        assert!(
            matches!(failure, StreamDelta::Error { message, .. } if message.contains("retained parser-state"))
        );
        assert!(parser.terminal);
        assert!(parser.failed);
        assert!(parser.reasoning_text.is_empty());
        assert!(parser.push_chunk(&fragment).is_empty());
        assert!(parser.finish().is_empty());
    }

    #[test]
    fn function_call_without_name_is_a_terminal_protocol_failure() {
        let mut parser = GeminiStreamParser::new("gemini-test");
        let out = parser.push_chunk(&json!({
            "candidates": [{"content": {"parts": [{"functionCall": {"args": {}}}]}}]
        }));
        assert!(out.iter().any(|delta| matches!(
            delta,
            StreamDelta::Error { message, .. } if message.contains("missing its name")
        )));
        assert!(!out
            .iter()
            .any(|delta| matches!(delta, StreamDelta::ToolCallStart { .. })));
        assert!(parser.terminal);
        assert!(parser.failed);
    }

    #[test]
    fn unsupported_media_output_is_a_terminal_protocol_failure() {
        for part in [
            json!({"inlineData": {"mimeType": "image/png", "data": "AQID"}}),
            json!({"fileData": {"mimeType": "audio/wav", "fileUri": "gs://bucket/audio.wav"}}),
        ] {
            let mut parser = GeminiStreamParser::new("gemini-test");
            let out = parser.push_chunk(&json!({
                "candidates": [{
                    "content": {"parts": [part]},
                    "finishReason": "STOP"
                }]
            }));
            assert!(out.iter().any(|delta| matches!(
                delta,
                StreamDelta::Error { message, .. }
                    if message.contains("media output")
            )));
            assert!(!out
                .iter()
                .any(|delta| matches!(delta, StreamDelta::MessageStop { .. })));
            assert!(parser.failed);
            assert!(parser.finish().is_empty());
        }
    }

    #[tokio::test]
    async fn provider_streams_gemini_sse_over_real_http() {
        use futures::StreamExt;
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        // Two `data: {GenerateContentResponse}` chunks: one text part, then finishReason + usage.
        let sse = concat!(
            "data: {\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"Merhaba\"}]}}]}\n\n",
            "data: {\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":5,\"candidatesTokenCount\":3}}\n\n",
        );
        // The model name is in the URL path (…/models/{model}:streamGenerateContent), so match on
        // POST alone rather than an exact path.
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_raw(sse, "text/event-stream"))
            .mount(&server)
            .await;

        let provider = GeminiProvider::with_base_url("test-key", server.uri());
        let req = ProviderRequest {
            model: "gemini-2.5-flash".into(),
            messages: vec![Message::user("selam")],
            tools: vec![],
            max_tokens: 100,
            options: serde_json::Map::new(),
            provider_options: crate::types::ProviderOptions::new(),
            compatibility_mode: crate::contract::CompatibilityMode::Strict,
        };
        let out: Vec<StreamDelta> = provider.stream(req).await.unwrap().collect().await;

        assert!(out.contains(&StreamDelta::MessageStart {
            model: "gemini-2.5-flash".into()
        }));
        assert!(out.contains(&StreamDelta::TextDelta {
            text: "Merhaba".into()
        }));
        assert!(out.contains(&StreamDelta::MessageStop {
            stop_reason: "end_turn".into()
        }));
    }

    #[tokio::test]
    async fn clean_eof_before_finish_reason_is_a_protocol_failure() {
        use futures::StreamExt;
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(
                "/v1beta/models/gemini-2.5-flash:streamGenerateContent",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_raw(
                "data: {\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"partial\"}]}}]}\n\n",
                "text/event-stream",
            ))
            .mount(&server)
            .await;

        let provider = GeminiProvider::with_base_url("google-test-key", server.uri());
        let out: Vec<_> = provider
            .stream(ProviderRequest {
                model: "gemini-2.5-flash".into(),
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

    #[test]
    fn function_call_signature_is_replayed_on_the_exact_original_part() {
        // Gemini can attach thoughtSignature to a functionCall part (not just a thought:true part)
        // on a thinking+tools turn. It must remain associated with that exact functionCall.
        let mut p = GeminiStreamParser::new("gemini-2.5-pro");
        let mut out: Vec<StreamDelta> = Vec::new();
        out.extend(p.push_chunk(&json!({
            "candidates": [{ "content": { "role": "model", "parts": [
                { "functionCall": { "name": "search", "args": { "q": "merhaba" } },
                  "thoughtSignature": "sig_on_fc" }
            ]}, "finishReason": "STOP" }]
        })));
        out.extend(p.finish());

        let call_id = out
            .iter()
            .find_map(|delta| match delta {
                StreamDelta::ToolCallStart { id, .. } => Some(id.clone()),
                _ => None,
            })
            .expect("tool call id");
        let opaque = out
            .iter()
            .find_map(|delta| match delta {
                StreamDelta::ReasoningComplete { opaque, .. } => opaque.clone(),
                _ => None,
            })
            .expect("opaque signature map");
        assert_eq!(
            opaque["google"]["function_call_signatures"][&call_id],
            "sig_on_fc"
        );

        let replay = Message {
            role: Role::Assistant,
            content: vec![
                ContentBlock::Reasoning {
                    text: String::new(),
                    signature: None,
                    provider: Some("google".into()),
                    opaque: Some(opaque),
                },
                ContentBlock::ToolUse {
                    id: call_id,
                    name: "search".into(),
                    input: json!({ "q": "merhaba" }),
                },
            ],
        };
        let request = build_request("gemini-3-flash", 128, &[replay], &[], None).unwrap();
        let parts = request["contents"][0]["parts"].as_array().unwrap();
        assert_eq!(parts.len(), 1, "must not synthesize an extra thought part");
        assert_eq!(parts[0]["functionCall"]["name"], "search");
        assert_eq!(parts[0]["thoughtSignature"], "sig_on_fc");
    }

    #[test]
    fn usage_maps_cached_content_tokens() {
        let mut parser = GeminiStreamParser::new("gemini-test");
        let out = parser.push_chunk(&json!({
            "usageMetadata": {
                "promptTokenCount": 12,
                "candidatesTokenCount": 3,
                "thoughtsTokenCount": 2,
                "cachedContentTokenCount": 7
            },
            "candidates": [{ "content": { "parts": [] }, "finishReason": "STOP" }]
        }));
        assert!(out.is_empty() || matches!(out[0], StreamDelta::MessageStart { .. }));
        let finished = parser.finish();
        assert!(finished.contains(&StreamDelta::Usage(Usage {
            input_tokens: 12,
            output_tokens: 3,
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 7,
            reasoning_tokens: 2,
        })));
    }

    #[test]
    fn provider_options_deep_merge_preserves_built_generation_config() {
        // A caller overriding one generationConfig field must not clobber the built maxOutputTokens.
        let mut opts = serde_json::Map::new();
        opts.insert("generationConfig".into(), json!({ "temperature": 0.5 }));
        let req = build_request(
            "gemini-2.5-pro",
            2048,
            &[Message::user("hi")],
            &[],
            Some(&opts),
        )
        .unwrap();
        // Deep merge: built maxOutputTokens survives AND the caller's temperature is applied.
        assert_eq!(req["generationConfig"]["maxOutputTokens"], 2048);
        assert_eq!(req["generationConfig"]["temperature"], 0.5);
    }

    #[test]
    fn provider_options_cannot_replace_gemini_contract_fields() {
        for (key, value, expected_path) in [
            ("model", json!("other-model"), "model"),
            ("contents", json!([]), "contents"),
            (
                "systemInstruction",
                json!({ "parts": [] }),
                "systemInstruction",
            ),
            ("max_tokens", json!(1), "max_tokens"),
            ("max_output_tokens", json!(1), "max_output_tokens"),
            ("max_completion_tokens", json!(1), "max_completion_tokens"),
            (
                "generationConfig",
                json!({ "maxOutputTokens": 1 }),
                "generationConfig.maxOutputTokens",
            ),
            (
                "generationConfig",
                Value::Null,
                "generationConfig.maxOutputTokens",
            ),
            ("tools", json!([]), "tools"),
            ("stream", json!(false), "stream"),
            ("stream_options", json!({}), "stream_options"),
        ] {
            let options = serde_json::Map::from_iter([(key.to_string(), value)]);
            let error = build_request(
                "gemini-2.5-pro",
                100,
                &[Message::user("hello")],
                &[],
                Some(&options),
            )
            .unwrap_err();
            let error = error.provider_error().expect("typed provider error");
            assert_eq!(error.kind, crate::error::ProviderErrorKind::InvalidRequest);
            assert!(error.message.contains(expected_path));
        }

        let options =
            serde_json::Map::from_iter([("labels".into(), json!({ "environment": "test" }))]);
        let request = build_request(
            "gemini-2.5-pro",
            100,
            &[Message::user("hello")],
            &[],
            Some(&options),
        )
        .unwrap();
        assert_eq!(request["labels"], options["labels"]);

        let options = serde_json::Map::from_iter([(
            "toolConfig".into(),
            json!({ "functionCallingConfig": { "mode": "ANY" } }),
        )]);
        let request = build_request(
            "gemini-2.5-pro",
            100,
            &[Message::user("hello")],
            &[],
            Some(&options),
        )
        .unwrap();
        assert_eq!(request["toolConfig"], options["toolConfig"]);
    }

    #[test]
    fn tool_result_is_error_encoded_in_function_response() {
        let messages = vec![
            Message {
                role: Role::Assistant,
                content: vec![ContentBlock::ToolUse {
                    id: "call_1".into(),
                    name: "search".into(),
                    input: json!({ "q": "merhaba" }),
                }],
            },
            Message {
                role: Role::Tool,
                content: vec![ContentBlock::ToolResult {
                    tool_use_id: "call_1".into(),
                    content: "boom".into(),
                    is_error: true,
                }],
            },
        ];
        let req = build_request("gemini-2.5-pro", 128, &messages, &[], None).unwrap();
        let contents = req["contents"].as_array().unwrap();
        let tool_turn = contents.last().unwrap();
        let response = &tool_turn["parts"][0]["functionResponse"]["response"];
        // An errored ToolResult surfaces under an "error" key (not "result").
        assert_eq!(response["error"], "boom");
        assert!(response.get("result").is_none());
    }
}
