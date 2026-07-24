//! Provider adapter layer.
//!
//! Each provider (Anthropic, OpenAI, Google, DeepSeek, openai-compat) implements
//! [`Provider`] by speaking its native wire format over raw HTTP. The wire ↔ canonical
//! translation for each lives in its submodule (e.g. [`anthropic`]); [`MockProvider`] is the
//! deterministic in-memory provider used for tests and the FFI spike.

pub mod anthropic;
pub mod deepseek;
pub mod google;
pub mod groq;
pub mod mistral;
pub mod openai;
pub mod openai_responses;
pub mod openrouter;
pub mod xai;

use crate::contract::{CompatibilityMode, MediaInput, MediaInputSource, ProviderWarning};
use crate::error::{AikitError, ProviderError, ProviderErrorKind, Result};
use crate::types::{ContentBlock, Message, Role, StreamDelta, ToolSpec};
use async_trait::async_trait;
use base64::Engine as _;
use futures::stream::BoxStream;
use futures::StreamExt;
use serde_json::{Map, Value};
use std::time::Duration;
use std::{borrow::Cow, fmt};

/// Provider-ready media whose inline bytes have passed canonical size/hash validation.
pub(crate) enum ResolvedMediaInput<'a> {
    Base64(Cow<'a, str>),
}

impl fmt::Debug for ResolvedMediaInput<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Base64(_) => formatter.write_str("Base64([redacted])"),
        }
    }
}

/// Validate strict media and resolve only sources that are safe to serialize to a provider.
/// Artifact references deliberately fail closed until an artifact store resolves them.
pub(crate) fn resolve_media_input<'a>(
    media: &'a MediaInput,
    provider: &str,
    model: &str,
) -> Result<ResolvedMediaInput<'a>> {
    media.validate().map_err(|message| {
        ProviderError::new(
            provider,
            model,
            ProviderErrorKind::InvalidRequest,
            format!("invalid media input: {message}"),
        )
    })?;
    match &media.source {
        MediaInputSource::Url { .. } => Err(ProviderError::new(
            provider,
            model,
            ProviderErrorKind::InvalidRequest,
            "strict URL media must be fetched through governed egress, verified against size_bytes and sha256, then supplied as bytes or base64",
        )
        .into()),
        MediaInputSource::Base64 { data } => Ok(ResolvedMediaInput::Base64(Cow::Borrowed(data))),
        MediaInputSource::Bytes { data } => Ok(ResolvedMediaInput::Base64(Cow::Owned(
            base64::engine::general_purpose::STANDARD.encode(data),
        ))),
        MediaInputSource::Artifact { .. } => Err(ProviderError::new(
            provider,
            model,
            ProviderErrorKind::InvalidRequest,
            "artifact media must be resolved to verified bytes or base64 before provider dispatch",
        )
        .into()),
    }
}

pub(crate) fn is_image_media_type(media_type: &str) -> bool {
    media_type
        .get(..6)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("image/"))
}

/// Integrity-bound media is an input block and must never disappear from a system, assistant, or
/// tool message merely because a provider wire format has no slot for it.
pub(crate) fn validate_media_input_roles(
    messages: &[Message],
    provider: &str,
    model: &str,
) -> Result<()> {
    if messages.iter().any(|message| {
        message.role != Role::User
            && message
                .content
                .iter()
                .any(|block| matches!(block, ContentBlock::MediaInput { .. }))
    }) {
        return Err(ProviderError::new(
            provider,
            model,
            ProviderErrorKind::InvalidRequest,
            "integrity-bound media input is only valid in user messages",
        )
        .into());
    }
    Ok(())
}

/// Maximum unparsed SSE bytes retained by a provider transport. A single provider event larger
/// than this is rejected as a protocol failure instead of growing an attacker-controlled buffer
/// without bound.
pub(crate) const MAX_SSE_BUFFER_BYTES: usize = 1024 * 1024;

/// Maximum provider-controlled state retained after SSE events have been parsed. The transport
/// frame bound above is not enough on its own: a peer can send an unlimited number of individually
/// small tool-argument, reasoning, metadata, or item fragments. Every stateful wire parser charges
/// this shared per-response budget before extending one of those accumulators.
pub(crate) const MAX_STREAM_RETAINED_BYTES: usize = 8 * 1024 * 1024;
pub(crate) const MAX_STREAM_RETAINED_ITEMS: usize = 4096;
const MAX_JSON_ACCOUNTING_DEPTH: usize = 128;
const MAX_JSON_ACCOUNTING_NODES: usize = 100_000;

#[derive(Debug, Clone)]
pub(crate) struct StreamRetentionBudget {
    retained_bytes: usize,
    retained_items: usize,
    max_bytes: usize,
    max_items: usize,
}

impl Default for StreamRetentionBudget {
    fn default() -> Self {
        Self {
            retained_bytes: 0,
            retained_items: 0,
            max_bytes: MAX_STREAM_RETAINED_BYTES,
            max_items: MAX_STREAM_RETAINED_ITEMS,
        }
    }
}

impl StreamRetentionBudget {
    /// Reserve capacity before retaining provider-controlled data. A failed reservation never
    /// changes the counters, so callers can fail the stream and release their parser state.
    pub(crate) fn retain(&mut self, bytes: usize, items: usize) -> bool {
        let Some(retained_bytes) = self.retained_bytes.checked_add(bytes) else {
            return false;
        };
        let Some(retained_items) = self.retained_items.checked_add(items) else {
            return false;
        };
        if retained_bytes > self.max_bytes || retained_items > self.max_items {
            return false;
        }
        self.retained_bytes = retained_bytes;
        self.retained_items = retained_items;
        true
    }

    pub(crate) fn retain_json(&mut self, value: &Value, items: usize) -> bool {
        self.retain(json_retained_bytes(value), items)
    }

    #[cfg(test)]
    pub(crate) fn with_limits(max_bytes: usize, max_items: usize) -> Self {
        Self {
            max_bytes,
            max_items,
            ..Self::default()
        }
    }
}

/// A depth/node-bounded estimate for JSON cloned into parser state. Structural bytes are included
/// so empty containers and collections of tiny values cannot bypass the byte budget. Returning
/// `usize::MAX` makes pathological nesting/cardinality fail the caller's retention reservation.
pub(crate) fn json_retained_bytes(value: &Value) -> usize {
    let mut total = 0_usize;
    let mut nodes = 0_usize;
    let mut stack = vec![(value, 0_usize)];
    while let Some((value, depth)) = stack.pop() {
        nodes = nodes.saturating_add(1);
        if nodes > MAX_JSON_ACCOUNTING_NODES || depth > MAX_JSON_ACCOUNTING_DEPTH {
            return usize::MAX;
        }
        let bytes = match value {
            Value::Null => 4,
            Value::Bool(_) => 5,
            Value::Number(number) => number.to_string().len(),
            Value::String(value) => value.len().saturating_add(2),
            Value::Array(values) => {
                let child_depth = depth.saturating_add(1);
                if (!values.is_empty() && child_depth > MAX_JSON_ACCOUNTING_DEPTH)
                    || nodes
                        .saturating_add(stack.len())
                        .saturating_add(values.len())
                        > MAX_JSON_ACCOUNTING_NODES
                {
                    return usize::MAX;
                }
                stack.extend(values.iter().map(|value| (value, child_depth)));
                2_usize.saturating_add(values.len())
            }
            Value::Object(values) => {
                let child_depth = depth.saturating_add(1);
                if (!values.is_empty() && child_depth > MAX_JSON_ACCOUNTING_DEPTH)
                    || nodes
                        .saturating_add(stack.len())
                        .saturating_add(values.len())
                        > MAX_JSON_ACCOUNTING_NODES
                {
                    return usize::MAX;
                }
                for (key, value) in values {
                    total = total.saturating_add(key.len()).saturating_add(4);
                    stack.push((value, child_depth));
                }
                2
            }
        };
        total = total.saturating_add(bytes);
        if total == usize::MAX {
            return usize::MAX;
        }
    }
    total
}

pub(crate) fn retained_state_failure(provider: &str) -> StreamDelta {
    protocol_failure(
        provider,
        format!("{provider} stream exceeded the retained parser-state limit"),
    )
}

/// Maximum error-body bytes retained for provider diagnostics. The public error envelope is
/// redacted separately; this bound prevents `Response::text()` from first allocating an
/// arbitrarily large body.
const MAX_ERROR_BODY_BYTES: usize = 4096;
const PROVIDER_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const PROVIDER_RESPONSE_TIMEOUT: Duration = Duration::from_secs(10 * 60);

/// Shared native provider client. Both connection establishment and the full streamed response
/// lifetime are bounded; otherwise a peer can keep a response pending forever without ever
/// violating the SSE or retained-state size limits.
pub(crate) fn native_http_client() -> reqwest::Client {
    native_http_client_with_timeouts(PROVIDER_CONNECT_TIMEOUT, PROVIDER_RESPONSE_TIMEOUT)
        .expect("static native provider HTTP client configuration is valid")
}

fn native_http_client_with_timeouts(
    connect_timeout: Duration,
    response_timeout: Duration,
) -> std::result::Result<reqwest::Client, reqwest::Error> {
    reqwest::Client::builder()
        .connect_timeout(connect_timeout)
        .timeout(response_timeout)
        .build()
}

/// Append a transport chunk only when it fits in the bounded, not-yet-parsed SSE buffer.
pub(crate) fn append_sse_chunk(buffer: &mut Vec<u8>, chunk: &[u8]) -> bool {
    if chunk.len() > MAX_SSE_BUFFER_BYTES.saturating_sub(buffer.len()) {
        return false;
    }
    buffer.extend_from_slice(chunk);
    true
}

/// Read a provider error response without ever retaining more than [`MAX_ERROR_BODY_BYTES`].
pub(crate) async fn read_error_body(response: reqwest::Response) -> String {
    let mut stream = response.bytes_stream();
    let mut body = Vec::with_capacity(MAX_ERROR_BODY_BYTES.min(1024));
    let mut truncated = false;

    while let Some(chunk) = stream.next().await {
        let Ok(chunk) = chunk else {
            break;
        };
        let remaining = MAX_ERROR_BODY_BYTES.saturating_sub(body.len());
        if remaining == 0 {
            truncated = true;
            break;
        }
        let take = chunk.len().min(remaining);
        body.extend_from_slice(&chunk[..take]);
        if take < chunk.len() {
            truncated = true;
            break;
        }
    }

    let mut text = String::from_utf8_lossy(&body).into_owned();
    if truncated {
        text.push('…');
    }
    text
}

/// Reject provider escape-hatch keys that would replace canonical request state. Paths may be
/// top-level (`model`) or nested (`generationConfig.maxOutputTokens`). The error is deliberately
/// typed as a provider invalid request so bindings and routing never need to parse its message.
pub(crate) fn reject_protected_options(
    provider: &str,
    model: &str,
    options: Option<&Map<String, Value>>,
    protected_paths: &[&str],
) -> Result<()> {
    let Some(options) = options else {
        return Ok(());
    };
    for path in protected_paths {
        let mut parts = path.split('.');
        let Some(first) = parts.next() else {
            continue;
        };
        let Some(mut value) = options.get(first) else {
            continue;
        };
        let mut present = true;
        for part in parts {
            let Some(next) = value.get(part) else {
                // A scalar/null replacement of a canonical object also destroys every protected
                // descendant, while an object that merely omits this leaf is safe to deep-merge.
                present = !value.is_object();
                break;
            };
            value = next;
        }
        if present {
            return Err(ProviderError::new(
                provider,
                model,
                ProviderErrorKind::InvalidRequest,
                format!("provider option '{path}' cannot override aikit's canonical request field"),
            )
            .into());
        }
    }
    Ok(())
}

/// Build a redacted stream error with provider/model context. `message` must be a deliberately
/// selected public description, never a raw HTTP response body or credential-bearing debug dump.
pub(crate) fn stream_failure(
    provider: &str,
    model: &str,
    kind: ProviderErrorKind,
    message: impl Into<String>,
) -> StreamDelta {
    let message = message.into();
    let failure = ProviderError::new(provider, model, kind, message.clone());
    StreamDelta::error_with_info(message, (&failure).into())
}

/// Parser-only protocol failure. Pure parser tests do not necessarily have the requested model;
/// the provider remains available for classification and live adapters use `stream_failure` when
/// model context exists at the transport boundary.
pub(crate) fn protocol_failure(provider: &str, message: impl Into<String>) -> StreamDelta {
    stream_failure_without_model(provider, ProviderErrorKind::Protocol, message)
}

pub(crate) fn stream_failure_without_model(
    provider: &str,
    kind: ProviderErrorKind,
    message: impl Into<String>,
) -> StreamDelta {
    let message = message.into();
    let mut info = crate::error::ErrorInfo::new(kind.into());
    info.provider = Some(provider.to_string());
    info.retryable = matches!(
        kind,
        ProviderErrorKind::RateLimited
            | ProviderErrorKind::Timeout
            | ProviderErrorKind::Transport
            | ProviderErrorKind::Server
    );
    StreamDelta::error_with_info(message, info)
}

pub(crate) fn with_stream_context(
    mut delta: StreamDelta,
    provider: &str,
    model: &str,
) -> StreamDelta {
    if let StreamDelta::Error { info, .. } = &mut delta {
        if info.provider.is_none() {
            info.provider = Some(provider.to_string());
        }
        if info.model.is_none() {
            info.model = Some(model.to_string());
        }
    }
    delta
}

/// A model-generation request in canonical form.
#[derive(Clone)]
pub struct ProviderRequest {
    pub model: String,
    pub messages: Vec<Message>,
    pub tools: Vec<ToolSpec>,
    /// Output-token ceiling for this call.
    pub max_tokens: u64,
    /// Typed, per-provider escape hatch (thinking / cache_control / reasoning_effort / ...),
    /// carried verbatim to the wire by each adapter's `build_request`.
    pub options: serde_json::Map<String, serde_json::Value>,
    /// Provider-keyed options retained across routing/fallback. Each adapter merges only its own
    /// entry, so vendor-native fields cannot leak into a different provider's request.
    pub provider_options: crate::types::ProviderOptions,
    /// Controls preflight handling for provider parameters that are absent from the shipped
    /// adapter catalog. The default on every high-level surface is fail-closed strict mode.
    pub compatibility_mode: CompatibilityMode,
}

impl ProviderRequest {
    pub fn options_for(&self, provider: &str) -> serde_json::Map<String, serde_json::Value> {
        let mut options = self.options.clone();
        if let Some(provider_options) = self.provider_options.get(provider) {
            options.extend(provider_options.clone());
        }
        options
    }

    pub(crate) fn validated_options_for(
        &self,
        provider: &str,
        allowed: &[&str],
    ) -> Result<ValidatedProviderOptions> {
        let options = self.options_for(provider);
        let mut warnings = Vec::new();
        for (parameter, value) in &options {
            if !allowed.contains(&parameter.as_str()) {
                self.record_option_issue(
                    provider,
                    parameter,
                    "unverified_provider_parameter",
                    "is not in the shipped adapter catalog and was forwarded without semantic adaptation",
                    &mut warnings,
                )?;
                continue;
            }
            let Some(kind) = provider_option_kind(provider, allowed, parameter) else {
                self.record_option_issue(
                    provider,
                    parameter,
                    "unverified_provider_parameter",
                    "has no shipped value-type contract and was forwarded without semantic adaptation",
                    &mut warnings,
                )?;
                continue;
            };
            if !kind.matches(value) {
                self.record_option_issue(
                    provider,
                    parameter,
                    "invalid_provider_parameter_type",
                    &format!(
                        "must be {}, but received {}",
                        kind.description(),
                        json_type_name(value)
                    ),
                    &mut warnings,
                )?;
                continue;
            }
            self.validate_nested_options(provider, allowed, parameter, value, &mut warnings)?;
        }
        Ok(ValidatedProviderOptions { options, warnings })
    }

    fn validate_nested_options(
        &self,
        provider: &str,
        allowed: &[&str],
        parameter: &str,
        value: &Value,
        warnings: &mut Vec<ProviderWarning>,
    ) -> Result<()> {
        let Some(fields) = provider_nested_option_fields(provider, allowed, parameter) else {
            return Ok(());
        };
        let Some(object) = value.as_object() else {
            return Ok(());
        };
        for (field, nested_value) in object {
            let path = format!("{parameter}.{}", safe_parameter_name(field));
            let Some((_, kind)) = fields.iter().find(|(known, _)| known == &field.as_str()) else {
                self.record_option_issue(
                    provider,
                    &path,
                    "unverified_provider_parameter",
                    "is not in the shipped adapter catalog and was forwarded without semantic adaptation",
                    warnings,
                )?;
                continue;
            };
            if !kind.matches(nested_value) {
                self.record_option_issue(
                    provider,
                    &path,
                    "invalid_provider_parameter_type",
                    &format!(
                        "must be {}, but received {}",
                        kind.description(),
                        json_type_name(nested_value)
                    ),
                    warnings,
                )?;
                continue;
            }
            self.validate_nested_options(provider, allowed, &path, nested_value, warnings)?;
            // Schema object paths intentionally have no deeper field catalog. Validating JSON
            // Schema keywords belongs to the provider/schema boundary, not compatibility checks.
        }
        Ok(())
    }

    fn record_option_issue(
        &self,
        provider: &str,
        parameter: &str,
        code: &str,
        detail: &str,
        warnings: &mut Vec<ProviderWarning>,
    ) -> Result<()> {
        let safe_parameter = safe_parameter_path(parameter);
        if self.compatibility_mode == CompatibilityMode::Strict {
            return Err(ProviderError::new(
                provider,
                &self.model,
                ProviderErrorKind::InvalidRequest,
                format!("provider option `{safe_parameter}` {detail} in strict compatibility mode"),
            )
            .into());
        }
        warnings.push(ProviderWarning {
            code: code.into(),
            message: format!("provider option `{safe_parameter}` {detail}"),
            parameter: Some(safe_parameter),
            provider: Some(provider.to_string()),
            model: Some(self.model.clone()),
        });
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct ValidatedProviderOptions {
    pub options: serde_json::Map<String, serde_json::Value>,
    pub warnings: Vec<ProviderWarning>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProviderOptionKind {
    Any,
    Boolean,
    Number,
    Integer,
    String,
    Object,
    StringMap,
    StringArray,
    ObjectArray,
    StringOrArray,
    StringOrObject,
}

impl ProviderOptionKind {
    fn matches(self, value: &Value) -> bool {
        match self {
            Self::Any => true,
            Self::Boolean => value.is_boolean(),
            Self::Number => value.is_number(),
            Self::Integer => value.as_i64().is_some() || value.as_u64().is_some(),
            Self::String => value.is_string(),
            Self::Object => value.is_object(),
            Self::StringMap => value
                .as_object()
                .is_some_and(|values| values.values().all(Value::is_string)),
            Self::StringArray => value
                .as_array()
                .is_some_and(|values| values.iter().all(Value::is_string)),
            Self::ObjectArray => value
                .as_array()
                .is_some_and(|values| values.iter().all(Value::is_object)),
            Self::StringOrArray => value.is_string() || value.is_array(),
            Self::StringOrObject => value.is_string() || value.is_object(),
        }
    }

    fn description(self) -> &'static str {
        match self {
            Self::Any => "any JSON value",
            Self::Boolean => "a boolean",
            Self::Number => "a number",
            Self::Integer => "an integer",
            Self::String => "a string",
            Self::Object => "an object",
            Self::StringMap => "an object with string values",
            Self::StringArray => "an array of strings",
            Self::ObjectArray => "an array of objects",
            Self::StringOrArray => "a string or array",
            Self::StringOrObject => "a string or object",
        }
    }
}

fn provider_option_kind(
    provider: &str,
    allowed: &[&str],
    parameter: &str,
) -> Option<ProviderOptionKind> {
    use ProviderOptionKind as Kind;
    if provider == "mock" && matches!(parameter, "tool_name" | "tool_input" | "response_format") {
        return Some(Kind::Any);
    }
    let responses_api = provider == "openai" && allowed.contains(&"max_tool_calls");
    match parameter {
        "temperature" | "top_p" | "presence_penalty" | "frequency_penalty" => Some(Kind::Number),
        "seed" | "top_logprobs" | "top_k" | "max_tool_calls" | "random_seed" => Some(Kind::Integer),
        "logprobs" | "parallel_tool_calls" | "store" | "safe_prompt" => Some(Kind::Boolean),
        "reasoning_effort"
        | "service_tier"
        | "user"
        | "truncation"
        | "safety_identifier"
        | "prompt_cache_key"
        | "prompt_cache_retention"
        | "speed"
        | "inference_geo"
        | "reasoning_format"
        | "route"
        | "user_id"
        | "cachedContent" => Some(Kind::String),
        "stop" => Some(Kind::StringOrArray),
        "stop_sequences" | "modalities" | "include" | "models" | "transforms" => {
            Some(Kind::StringArray)
        }
        "mcp_servers" | "safetySettings" | "plugins" => Some(Kind::ObjectArray),
        "tool_choice" => Some(Kind::StringOrObject),
        "metadata" | "labels" => Some(Kind::StringMap),
        "response_format" | "prediction" | "audio" | "thinking" | "output_config"
        | "generationConfig" | "toolConfig" => Some(Kind::Object),
        "reasoning" | "text" if responses_api => Some(Kind::Object),
        _ => None,
    }
}

const GOOGLE_GENERATION_CONFIG_FIELDS: &[(&str, ProviderOptionKind)] = &[
    ("temperature", ProviderOptionKind::Number),
    ("topP", ProviderOptionKind::Number),
    ("topK", ProviderOptionKind::Integer),
    ("candidateCount", ProviderOptionKind::Integer),
    ("maxOutputTokens", ProviderOptionKind::Integer),
    ("stopSequences", ProviderOptionKind::StringArray),
    ("responseMimeType", ProviderOptionKind::String),
    ("responseSchema", ProviderOptionKind::Object),
    ("responseJsonSchema", ProviderOptionKind::Object),
    ("responseFormat", ProviderOptionKind::Object),
    ("presencePenalty", ProviderOptionKind::Number),
    ("frequencyPenalty", ProviderOptionKind::Number),
    ("responseLogprobs", ProviderOptionKind::Boolean),
    ("logprobs", ProviderOptionKind::Integer),
    ("seed", ProviderOptionKind::Integer),
    ("thinkingConfig", ProviderOptionKind::Object),
    ("mediaResolution", ProviderOptionKind::String),
    ("imageConfig", ProviderOptionKind::Object),
    ("speechConfig", ProviderOptionKind::Object),
];

const OPENAI_REASONING_FIELDS: &[(&str, ProviderOptionKind)] = &[
    ("effort", ProviderOptionKind::String),
    ("summary", ProviderOptionKind::String),
];

const OPENAI_TEXT_FIELDS: &[(&str, ProviderOptionKind)] = &[
    ("format", ProviderOptionKind::Object),
    ("verbosity", ProviderOptionKind::String),
];

const GOOGLE_TOOL_CONFIG_FIELDS: &[(&str, ProviderOptionKind)] =
    &[("functionCallingConfig", ProviderOptionKind::Object)];

const GOOGLE_FUNCTION_CALLING_FIELDS: &[(&str, ProviderOptionKind)] = &[
    ("mode", ProviderOptionKind::String),
    ("allowedFunctionNames", ProviderOptionKind::StringArray),
    ("streamFunctionCallArguments", ProviderOptionKind::Boolean),
];

const GOOGLE_THINKING_CONFIG_FIELDS: &[(&str, ProviderOptionKind)] = &[
    ("includeThoughts", ProviderOptionKind::Boolean),
    ("thinkingBudget", ProviderOptionKind::Integer),
    ("thinkingLevel", ProviderOptionKind::String),
];

const GOOGLE_IMAGE_CONFIG_FIELDS: &[(&str, ProviderOptionKind)] = &[
    ("aspectRatio", ProviderOptionKind::String),
    ("imageSize", ProviderOptionKind::String),
];

const GOOGLE_RESPONSE_FORMAT_FIELDS: &[(&str, ProviderOptionKind)] =
    &[("text", ProviderOptionKind::Object)];

const GOOGLE_RESPONSE_FORMAT_TEXT_FIELDS: &[(&str, ProviderOptionKind)] = &[
    ("mimeType", ProviderOptionKind::String),
    ("schema", ProviderOptionKind::Object),
];

const OPENAI_RESPONSE_FORMAT_FIELDS: &[(&str, ProviderOptionKind)] = &[
    ("type", ProviderOptionKind::String),
    ("json_schema", ProviderOptionKind::Object),
];

const OPENAI_JSON_SCHEMA_FIELDS: &[(&str, ProviderOptionKind)] = &[
    ("name", ProviderOptionKind::String),
    ("description", ProviderOptionKind::String),
    ("strict", ProviderOptionKind::Boolean),
    ("schema", ProviderOptionKind::Object),
];

const OPENAI_TEXT_FORMAT_FIELDS: &[(&str, ProviderOptionKind)] = &[
    ("type", ProviderOptionKind::String),
    ("name", ProviderOptionKind::String),
    ("description", ProviderOptionKind::String),
    ("strict", ProviderOptionKind::Boolean),
    ("schema", ProviderOptionKind::Object),
];

const OPENAI_TOOL_CHOICE_FIELDS: &[(&str, ProviderOptionKind)] = &[
    ("type", ProviderOptionKind::String),
    ("name", ProviderOptionKind::String),
    ("server_label", ProviderOptionKind::String),
    ("function", ProviderOptionKind::Object),
];

const OPENAI_TOOL_CHOICE_FUNCTION_FIELDS: &[(&str, ProviderOptionKind)] =
    &[("name", ProviderOptionKind::String)];

const OPENAI_PREDICTION_FIELDS: &[(&str, ProviderOptionKind)] = &[
    ("type", ProviderOptionKind::String),
    ("content", ProviderOptionKind::StringOrArray),
];

const OPENAI_AUDIO_FIELDS: &[(&str, ProviderOptionKind)] = &[
    ("voice", ProviderOptionKind::String),
    ("format", ProviderOptionKind::String),
];

const ANTHROPIC_OUTPUT_CONFIG_FIELDS: &[(&str, ProviderOptionKind)] = &[
    ("effort", ProviderOptionKind::String),
    ("format", ProviderOptionKind::Object),
];

const ANTHROPIC_OUTPUT_FORMAT_FIELDS: &[(&str, ProviderOptionKind)] = &[
    ("type", ProviderOptionKind::String),
    ("name", ProviderOptionKind::String),
    ("schema", ProviderOptionKind::Object),
];

const ANTHROPIC_TOOL_CHOICE_FIELDS: &[(&str, ProviderOptionKind)] = &[
    ("type", ProviderOptionKind::String),
    ("name", ProviderOptionKind::String),
    ("disable_parallel_tool_use", ProviderOptionKind::Boolean),
];

const ANTHROPIC_THINKING_FIELDS: &[(&str, ProviderOptionKind)] = &[
    ("type", ProviderOptionKind::String),
    ("budget_tokens", ProviderOptionKind::Integer),
];

const DEEPSEEK_THINKING_FIELDS: &[(&str, ProviderOptionKind)] =
    &[("type", ProviderOptionKind::String)];

fn provider_nested_option_fields(
    provider: &str,
    allowed: &[&str],
    path: &str,
) -> Option<&'static [(&'static str, ProviderOptionKind)]> {
    let responses_api = provider == "openai" && allowed.contains(&"max_tool_calls");
    let openai_compatible = matches!(
        provider,
        "openai" | "deepseek" | "openrouter" | "groq" | "mistral" | "xai"
    );
    match (provider, path) {
        ("google", "generationConfig") => Some(GOOGLE_GENERATION_CONFIG_FIELDS),
        ("google", "generationConfig.thinkingConfig") => Some(GOOGLE_THINKING_CONFIG_FIELDS),
        ("google", "generationConfig.imageConfig") => Some(GOOGLE_IMAGE_CONFIG_FIELDS),
        ("google", "generationConfig.responseFormat") => Some(GOOGLE_RESPONSE_FORMAT_FIELDS),
        ("google", "generationConfig.responseFormat.text") => {
            Some(GOOGLE_RESPONSE_FORMAT_TEXT_FIELDS)
        }
        ("google", "toolConfig") => Some(GOOGLE_TOOL_CONFIG_FIELDS),
        ("google", "toolConfig.functionCallingConfig") => Some(GOOGLE_FUNCTION_CALLING_FIELDS),
        ("openai", "reasoning") if responses_api => Some(OPENAI_REASONING_FIELDS),
        ("openai", "text") if responses_api => Some(OPENAI_TEXT_FIELDS),
        ("openai", "text.format") if responses_api => Some(OPENAI_TEXT_FORMAT_FIELDS),
        ("anthropic", "thinking") => Some(ANTHROPIC_THINKING_FIELDS),
        ("deepseek", "thinking") => Some(DEEPSEEK_THINKING_FIELDS),
        ("anthropic", "output_config") => Some(ANTHROPIC_OUTPUT_CONFIG_FIELDS),
        ("anthropic", "output_config.format") => Some(ANTHROPIC_OUTPUT_FORMAT_FIELDS),
        ("anthropic", "tool_choice") => Some(ANTHROPIC_TOOL_CHOICE_FIELDS),
        (_, "response_format") if openai_compatible => Some(OPENAI_RESPONSE_FORMAT_FIELDS),
        (_, "response_format.json_schema") if openai_compatible => Some(OPENAI_JSON_SCHEMA_FIELDS),
        (_, "tool_choice") if openai_compatible => Some(OPENAI_TOOL_CHOICE_FIELDS),
        (_, "tool_choice.function") if openai_compatible => {
            Some(OPENAI_TOOL_CHOICE_FUNCTION_FIELDS)
        }
        (_, "prediction") if openai_compatible => Some(OPENAI_PREDICTION_FIELDS),
        (_, "audio") if openai_compatible => Some(OPENAI_AUDIO_FIELDS),
        _ => None,
    }
}

fn json_type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "a boolean",
        Value::Number(number) if number.as_i64().is_some() || number.as_u64().is_some() => {
            "an integer"
        }
        Value::Number(_) => "a number",
        Value::String(_) => "a string",
        Value::Array(_) => "an array",
        Value::Object(_) => "an object",
    }
}

fn safe_parameter_name(parameter: &str) -> &str {
    if parameter.len() <= 128
        && parameter
            .bytes()
            .all(|byte| byte.is_ascii_graphic() && byte != b'`' && byte != b'.')
    {
        parameter
    } else {
        "[invalid parameter name]"
    }
}

fn safe_parameter_path(path: &str) -> String {
    path.split('.')
        .map(safe_parameter_name)
        .collect::<Vec<_>>()
        .join(".")
}

pub(crate) const OPENAI_CHAT_OPTIONS: &[&str] = &[
    "temperature",
    "top_p",
    "stop",
    "seed",
    "presence_penalty",
    "frequency_penalty",
    "logprobs",
    "top_logprobs",
    "reasoning_effort",
    "response_format",
    "tool_choice",
    "parallel_tool_calls",
    "service_tier",
    "user",
    "metadata",
    "store",
    "prediction",
    "modalities",
    "audio",
];

pub(crate) const OPENAI_RESPONSES_OPTIONS: &[&str] = &[
    "temperature",
    "top_p",
    "reasoning",
    "text",
    "response_format",
    "tool_choice",
    "parallel_tool_calls",
    "service_tier",
    "user",
    "metadata",
    "store",
    "include",
    "truncation",
    "max_tool_calls",
    "safety_identifier",
    "prompt_cache_key",
    "prompt_cache_retention",
];

pub(crate) const ANTHROPIC_OPTIONS: &[&str] = &[
    "temperature",
    "top_p",
    "top_k",
    "stop_sequences",
    "metadata",
    "service_tier",
    "thinking",
    "output_config",
    "speed",
    "inference_geo",
    "tool_choice",
];

pub(crate) const GOOGLE_OPTIONS: &[&str] = &[
    "generationConfig",
    "safetySettings",
    "toolConfig",
    "cachedContent",
    "labels",
];

pub(crate) const DEEPSEEK_OPTIONS: &[&str] = &[
    "temperature",
    "top_p",
    "stop",
    "frequency_penalty",
    "presence_penalty",
    "thinking",
    "reasoning_effort",
    "response_format",
    "tool_choice",
    "logprobs",
    "top_logprobs",
    "user_id",
];

pub(crate) fn openai_compatible_options(provider: &str) -> &'static [&'static str] {
    match provider {
        "openrouter" => &[
            "temperature",
            "top_p",
            "stop",
            "seed",
            "presence_penalty",
            "frequency_penalty",
            "logprobs",
            "top_logprobs",
            "reasoning_effort",
            "response_format",
            "tool_choice",
            "parallel_tool_calls",
            "user",
            "route",
            "transforms",
            "models",
        ],
        "groq" => &[
            "temperature",
            "top_p",
            "stop",
            "seed",
            "presence_penalty",
            "frequency_penalty",
            "logprobs",
            "top_logprobs",
            "response_format",
            "tool_choice",
            "parallel_tool_calls",
            "service_tier",
            "user",
            "reasoning_format",
            "reasoning_effort",
        ],
        "mistral" => &[
            "temperature",
            "top_p",
            "stop",
            "seed",
            "presence_penalty",
            "frequency_penalty",
            "response_format",
            "tool_choice",
            "parallel_tool_calls",
            "random_seed",
            "safe_prompt",
            "prediction",
        ],
        "xai" => &[
            "temperature",
            "top_p",
            "stop",
            "seed",
            "presence_penalty",
            "frequency_penalty",
            "logprobs",
            "top_logprobs",
            "response_format",
            "tool_choice",
            "parallel_tool_calls",
            "reasoning_effort",
        ],
        _ => OPENAI_CHAT_OPTIONS,
    }
}

pub(crate) fn prepend_provider_warnings(
    stream: BoxStream<'static, StreamDelta>,
    warnings: Vec<ProviderWarning>,
) -> BoxStream<'static, StreamDelta> {
    futures::stream::iter(
        warnings
            .into_iter()
            .map(|warning| StreamDelta::Warning { warning }),
    )
    .chain(stream)
    .boxed()
}

fn transport_error_kind(error: &reqwest::Error) -> ProviderErrorKind {
    if error.is_timeout() {
        ProviderErrorKind::Timeout
    } else {
        ProviderErrorKind::Transport
    }
}

pub(crate) fn transport_failure(provider: &str, model: &str, error: reqwest::Error) -> AikitError {
    let kind = transport_error_kind(&error);
    ProviderError::new(provider, model, kind, error.to_string()).into()
}

/// Classify a response-body transport failure without reflecting reqwest's error text. Body errors
/// can contain request details, while callers only need a stable typed code and a selected public
/// description. The client's total timeout remains distinguishable from other network failures.
pub(crate) fn response_stream_failure(
    provider: &str,
    model: &str,
    error: reqwest::Error,
    public_provider_name: &str,
) -> StreamDelta {
    let kind = transport_error_kind(&error);
    let condition = if kind == ProviderErrorKind::Timeout {
        "timed out"
    } else {
        "transport failed"
    };
    stream_failure(
        provider,
        model,
        kind,
        format!("{public_provider_name} response stream {condition}"),
    )
}

pub(crate) fn http_failure(
    provider: &str,
    model: &str,
    status: reqwest::StatusCode,
    retry_after: Option<&reqwest::header::HeaderValue>,
    mut body: String,
) -> AikitError {
    if body.len() > MAX_ERROR_BODY_BYTES {
        let mut end = MAX_ERROR_BODY_BYTES;
        while !body.is_char_boundary(end) {
            end -= 1;
        }
        body.truncate(end);
        body.push('…');
    }
    let retry_after_ms = parse_retry_after_ms(retry_after, std::time::SystemTime::now());
    ProviderError::from_http(provider, model, status.as_u16(), retry_after_ms, body).into()
}

fn parse_retry_after_ms(
    retry_after: Option<&reqwest::header::HeaderValue>,
    now: std::time::SystemTime,
) -> Option<u64> {
    let value = retry_after?.to_str().ok()?.trim();
    if let Ok(seconds) = value.parse::<u64>() {
        return Some(seconds.saturating_mul(1000));
    }
    let retry_at = httpdate::parse_http_date(value).ok()?;
    let delay = retry_at.duration_since(now).unwrap_or_default();
    Some(u64::try_from(delay.as_millis()).unwrap_or(u64::MAX))
}

#[async_trait]
pub trait Provider: Send + Sync {
    fn name(&self) -> &str;

    /// Produce the streamed response for `req` as a stream of canonical [`StreamDelta`]s.
    async fn stream(&self, req: ProviderRequest) -> Result<BoxStream<'static, StreamDelta>>;
}

/// Deterministic provider for tests and the FFI spike.
///
/// Turn 1 (no tool result in history, at least one tool available): stream a bit of text,
/// then request the first tool. Turn 2 (a tool result is present): stream a final answer
/// and stop. This drives the agent loop through exactly one tool round-trip.
///
/// Tests that need an exact tool call may set both `mock.tool_name` and `mock.tool_input` in
/// [`ProviderRequest::provider_options`]. This is deliberately an explicit, mock-only fixture
/// control; malformed pairs and names that were not advertised fail as typed configuration
/// errors. With neither field present, the long-standing first-tool/`{"q":"merhaba"}` behavior
/// is unchanged.
pub struct MockProvider;

fn mock_tool_fixture(req: &ProviderRequest) -> Result<Option<(&ToolSpec, Value)>> {
    let Some(options) = req.provider_options.get("mock") else {
        return Ok(None);
    };
    let tool_name = options.get("tool_name");
    let tool_input = options.get("tool_input");
    if tool_name.is_none() && tool_input.is_none() {
        return Ok(None);
    }
    let name = tool_name.and_then(Value::as_str).ok_or_else(|| {
        AikitError::Configuration(
            "mock tool fixture requires string provider_options.mock.tool_name".into(),
        )
    })?;
    let input = tool_input.cloned().ok_or_else(|| {
        AikitError::Configuration(
            "mock tool fixture requires provider_options.mock.tool_input".into(),
        )
    })?;
    let tool = req
        .tools
        .iter()
        .find(|tool| tool.name == name)
        .ok_or_else(|| {
            AikitError::Configuration(format!(
                "mock tool fixture named unadvertised tool '{name}'"
            ))
        })?;
    Ok(Some((tool, input)))
}

#[async_trait]
impl Provider for MockProvider {
    fn name(&self) -> &str {
        "mock"
    }

    async fn stream(&self, req: ProviderRequest) -> Result<BoxStream<'static, StreamDelta>> {
        let validated = req
            .validated_options_for(self.name(), &["tool_name", "tool_input", "response_format"])?;
        let warnings = validated.warnings;
        // Validate explicit fixture controls even when another mock mode (such as structured
        // output) would otherwise return early. A misspelled or unadvertised tool must never be
        // silently ignored.
        let fixture = mock_tool_fixture(&req)?;

        // Structured-output binding demos use the same planner + validator as live models while
        // remaining keyless. The native-constrained mock receives the schema through the
        // response_format escape hatch and deterministically materializes one valid value.
        if let Some(schema) = validated
            .options
            .get("response_format")
            .and_then(|v| v.get("json_schema"))
            .and_then(|v| v.get("schema"))
        {
            let value = mock_value_for_schema(schema);
            let deltas = vec![
                StreamDelta::MessageStart {
                    model: req.model.clone(),
                },
                StreamDelta::TextDelta {
                    text: serde_json::to_string(&value).unwrap_or_else(|_| "{}".into()),
                },
                StreamDelta::MessageStop {
                    stop_reason: "end_turn".into(),
                },
            ];
            return Ok(prepend_provider_warnings(
                Box::pin(futures::stream::iter(deltas)),
                warnings,
            ));
        }

        let has_tool_result = req.messages.iter().any(|m| {
            m.content
                .iter()
                .any(|b| matches!(b, ContentBlock::ToolResult { .. }))
        });

        let deltas: Vec<StreamDelta> = if !has_tool_result && !req.tools.is_empty() {
            let (tool, input) =
                fixture.unwrap_or_else(|| (&req.tools[0], serde_json::json!({ "q": "merhaba" })));
            vec![
                StreamDelta::MessageStart {
                    model: req.model.clone(),
                },
                StreamDelta::TextDelta {
                    text: "Bir aracı çağırıyorum: ".into(),
                },
                StreamDelta::TextDelta {
                    text: tool.name.clone(),
                },
                StreamDelta::ToolCallStart {
                    id: "call_1".into(),
                    name: tool.name.clone(),
                },
                StreamDelta::ToolCallInput {
                    id: "call_1".into(),
                    input,
                },
                StreamDelta::MessageStop {
                    stop_reason: "tool_use".into(),
                },
            ]
        } else {
            vec![
                StreamDelta::MessageStart {
                    model: req.model.clone(),
                },
                StreamDelta::TextDelta {
                    text: "Araç sonucunu aldım; görevi tamamladım.".into(),
                },
                StreamDelta::Usage(crate::types::Usage {
                    input_tokens: 12,
                    output_tokens: 9,
                    ..Default::default()
                }),
                StreamDelta::MessageStop {
                    stop_reason: "end_turn".into(),
                },
            ]
        };

        Ok(prepend_provider_warnings(
            Box::pin(futures::stream::iter(deltas)),
            warnings,
        ))
    }
}

/// Deterministically construct a small value accepted by the validator's JSON-Schema subset.
fn mock_value_for_schema(schema: &Value) -> Value {
    if let Some(value) = schema.get("const") {
        return value.clone();
    }
    if let Some(value) = schema
        .get("enum")
        .and_then(Value::as_array)
        .and_then(|values| values.first())
    {
        return value.clone();
    }

    match schema.get("type").and_then(Value::as_str) {
        Some("object") => {
            let required = schema
                .get("required")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            let properties = schema
                .get("properties")
                .and_then(Value::as_object)
                .cloned()
                .unwrap_or_default();
            let mut object = Map::new();
            for name in required.iter().filter_map(Value::as_str) {
                let property = properties.get(name).unwrap_or(&Value::Null);
                object.insert(name.to_string(), mock_value_for_schema(property));
            }
            Value::Object(object)
        }
        Some("array") => Value::Array(Vec::new()),
        Some("string") => Value::String("mock".into()),
        Some("integer") | Some("number") => Value::from(0),
        Some("boolean") => Value::Bool(false),
        Some("null") | None | Some(_) => Value::Null,
    }
}

#[cfg(test)]
mod request_tests {
    use super::*;
    use futures::StreamExt;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread::JoinHandle;

    fn raw_http_body(
        content_length: usize,
        body_prefix: &'static [u8],
        hold_open: Duration,
    ) -> (String, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(1)))
                .unwrap();
            let mut request = [0_u8; 1024];
            assert!(socket.read(&mut request).unwrap() > 0);
            write!(
                socket,
                "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {content_length}\r\nConnection: close\r\n\r\n"
            )
            .unwrap();
            socket.write_all(body_prefix).unwrap();
            socket.flush().unwrap();
            std::thread::sleep(hold_open);
        });
        (format!("http://{address}/stream"), server)
    }

    async fn response_body_error(response: reqwest::Response) -> reqwest::Error {
        let mut body = response.bytes_stream();
        while let Some(part) = body.next().await {
            if let Err(error) = part {
                return error;
            }
        }
        panic!("response body unexpectedly completed without an error");
    }

    fn assert_stream_failure(
        delta: StreamDelta,
        expected_code: crate::error::ErrorCode,
        expected_message: &str,
    ) {
        match delta {
            StreamDelta::Error { message, info } => {
                assert_eq!(message, expected_message);
                assert_eq!(info.code, expected_code);
                assert_eq!(info.provider.as_deref(), Some("test"));
                assert_eq!(info.model.as_deref(), Some("model"));
            }
            other => panic!("expected stream error, got {other:?}"),
        }
    }

    fn request_with_tools(names: &[&str]) -> ProviderRequest {
        ProviderRequest {
            model: "mock-1".into(),
            messages: vec![Message::user("fixture")],
            tools: names
                .iter()
                .map(|name| ToolSpec {
                    name: (*name).into(),
                    description: (*name).into(),
                    input_schema: serde_json::json!({ "type": "object" }),
                })
                .collect(),
            max_tokens: 64,
            options: Map::new(),
            provider_options: crate::types::ProviderOptions::new(),
            compatibility_mode: CompatibilityMode::Strict,
        }
    }

    fn fixture_options(tool_name: Value, tool_input: Option<Value>) -> Map<String, Value> {
        let mut options = Map::from_iter([("tool_name".into(), tool_name)]);
        if let Some(input) = tool_input {
            options.insert("tool_input".into(), input);
        }
        options
    }

    #[test]
    fn provider_options_are_selected_without_cross_vendor_leakage() {
        let mut request = ProviderRequest {
            model: "m".into(),
            messages: Vec::new(),
            tools: Vec::new(),
            max_tokens: 1,
            options: serde_json::Map::from_iter([("shared".into(), Value::Bool(true))]),
            provider_options: crate::types::ProviderOptions::new(),
            compatibility_mode: CompatibilityMode::Strict,
        };
        request.provider_options.insert(
            "anthropic".into(),
            serde_json::Map::from_iter([(
                "thinking".into(),
                serde_json::json!({ "type": "enabled" }),
            )]),
        );
        request.provider_options.insert(
            "google".into(),
            serde_json::Map::from_iter([(
                "toolConfig".into(),
                serde_json::json!({ "mode": "ANY" }),
            )]),
        );

        let anthropic = request.options_for("anthropic");
        assert_eq!(anthropic.get("shared"), Some(&Value::Bool(true)));
        assert!(anthropic.contains_key("thinking"));
        assert!(!anthropic.contains_key("toolConfig"));

        let google = request.options_for("google");
        assert!(google.contains_key("toolConfig"));
        assert!(!google.contains_key("thinking"));
    }

    #[test]
    fn compatibility_mode_rejects_or_warns_without_dropping_parameters() {
        let mut request = ProviderRequest {
            model: "gpt-test".into(),
            messages: vec![Message::user("hello")],
            tools: Vec::new(),
            max_tokens: 64,
            options: Map::from_iter([("future_option".into(), Value::Bool(true))]),
            provider_options: crate::types::ProviderOptions::new(),
            compatibility_mode: CompatibilityMode::Strict,
        };

        let error = request
            .validated_options_for("openai", OPENAI_CHAT_OPTIONS)
            .unwrap_err();
        assert!(matches!(
            error.provider_error(),
            Some(error) if error.kind == ProviderErrorKind::InvalidRequest
        ));

        request.compatibility_mode = CompatibilityMode::Warn;
        let validated = request
            .validated_options_for("openai", OPENAI_CHAT_OPTIONS)
            .unwrap();
        assert_eq!(validated.options["future_option"], Value::Bool(true));
        assert_eq!(validated.warnings.len(), 1);
        assert_eq!(
            validated.warnings[0].parameter.as_deref(),
            Some("future_option")
        );

        request.compatibility_mode = CompatibilityMode::BestEffort;
        let best_effort = request
            .validated_options_for("openai", OPENAI_CHAT_OPTIONS)
            .unwrap();
        assert_eq!(best_effort.options["future_option"], Value::Bool(true));
        assert_eq!(best_effort.warnings.len(), 1);
    }

    #[test]
    fn google_nested_option_typo_is_typed_in_strict_and_path_warned_otherwise() {
        let mut request = ProviderRequest {
            model: "gemini-test".into(),
            messages: vec![Message::user("hello")],
            tools: Vec::new(),
            max_tokens: 64,
            options: Map::from_iter([(
                "generationConfig".into(),
                serde_json::json!({"temprature": 0.5}),
            )]),
            provider_options: crate::types::ProviderOptions::new(),
            compatibility_mode: CompatibilityMode::Strict,
        };

        let error = request
            .validated_options_for("google", GOOGLE_OPTIONS)
            .unwrap_err();
        assert!(matches!(
            error.provider_error(),
            Some(error)
                if error.kind == ProviderErrorKind::InvalidRequest
                    && error.message.contains("generationConfig.temprature")
        ));

        for mode in [CompatibilityMode::Warn, CompatibilityMode::BestEffort] {
            request.compatibility_mode = mode;
            let validated = request
                .validated_options_for("google", GOOGLE_OPTIONS)
                .unwrap();
            assert_eq!(
                validated.options["generationConfig"]["temprature"],
                serde_json::json!(0.5)
            );
            assert_eq!(validated.warnings.len(), 1);
            assert_eq!(
                validated.warnings[0].parameter.as_deref(),
                Some("generationConfig.temprature")
            );
            assert_eq!(validated.warnings[0].code, "unverified_provider_parameter");
        }
    }

    #[test]
    fn current_gemini_response_format_is_strict_cataloged_and_nested_typed() {
        let mut request = ProviderRequest {
            model: "gemini-3.5-flash".into(),
            messages: vec![Message::user("hello")],
            tools: Vec::new(),
            max_tokens: 64,
            options: Map::from_iter([(
                "generationConfig".into(),
                serde_json::json!({
                    "responseFormat": {
                        "text": {"mimeType": "application/json", "schema": {"type": "object"}}
                    }
                }),
            )]),
            provider_options: crate::types::ProviderOptions::new(),
            compatibility_mode: CompatibilityMode::Strict,
        };
        assert!(request
            .validated_options_for("google", GOOGLE_OPTIONS)
            .unwrap()
            .warnings
            .is_empty());

        request.options["generationConfig"]["responseFormat"]["text"]["mimeType"] =
            Value::Bool(true);
        assert!(matches!(
            request
                .validated_options_for("google", GOOGLE_OPTIONS)
                .unwrap_err()
                .provider_error(),
            Some(error)
                if error.kind == ProviderErrorKind::InvalidRequest
                    && error.message.contains("generationConfig.responseFormat.text.mimeType")
        ));

        for unsupported in ["image", "audio"] {
            request.options["generationConfig"]["responseFormat"] =
                serde_json::json!({ unsupported: {"mimeType": "application/octet-stream"} });
            assert!(matches!(
                request
                    .validated_options_for("google", GOOGLE_OPTIONS)
                    .unwrap_err()
                    .provider_error(),
                Some(error)
                    if error.kind == ProviderErrorKind::InvalidRequest
                        && error.message.contains(&format!(
                            "generationConfig.responseFormat.{unsupported}"
                        ))
            ));
        }
    }

    #[test]
    fn governed_object_options_validate_recursively_and_opaque_options_require_warn_mode() {
        let mut google = ProviderRequest {
            model: "gemini-test".into(),
            messages: vec![Message::user("hello")],
            tools: Vec::new(),
            max_tokens: 64,
            options: Map::from_iter([(
                "toolConfig".into(),
                serde_json::json!({"functionCallingConfigg": {"mode": "ANY"}}),
            )]),
            provider_options: crate::types::ProviderOptions::new(),
            compatibility_mode: CompatibilityMode::Strict,
        };
        let error = google
            .validated_options_for("google", GOOGLE_OPTIONS)
            .unwrap_err();
        assert!(matches!(
            error.provider_error(),
            Some(error)
                if error.kind == ProviderErrorKind::InvalidRequest
                    && error.message.contains("toolConfig.functionCallingConfigg")
        ));

        google.compatibility_mode = CompatibilityMode::Warn;
        let warned = google
            .validated_options_for("google", GOOGLE_OPTIONS)
            .unwrap();
        assert_eq!(
            warned.warnings[0].parameter.as_deref(),
            Some("toolConfig.functionCallingConfigg")
        );
        assert_eq!(
            warned.options["toolConfig"]["functionCallingConfigg"]["mode"],
            "ANY"
        );

        let anthropic = ProviderRequest {
            model: "claude-test".into(),
            options: Map::from_iter([(
                "output_config".into(),
                serde_json::json!({"formatt": {"type": "json_schema"}}),
            )]),
            compatibility_mode: CompatibilityMode::Strict,
            ..google.clone()
        };
        assert!(matches!(
            anthropic
                .validated_options_for("anthropic", ANTHROPIC_OPTIONS)
                .unwrap_err()
                .provider_error(),
            Some(error)
                if error.kind == ProviderErrorKind::InvalidRequest
                    && error.message.contains("output_config.formatt")
        ));

        // Complex vendor beta objects are not advertised as strict-known until every nested
        // field is cataloged. Warn mode can still forward them with explicit evidence.
        let mut beta = ProviderRequest {
            options: Map::from_iter([(
                "context_management".into(),
                serde_json::json!({"edits": []}),
            )]),
            ..anthropic
        };
        assert!(beta
            .validated_options_for("anthropic", ANTHROPIC_OPTIONS)
            .is_err());
        beta.compatibility_mode = CompatibilityMode::Warn;
        let warned = beta
            .validated_options_for("anthropic", ANTHROPIC_OPTIONS)
            .unwrap();
        assert_eq!(warned.warnings.len(), 1);
        assert_eq!(
            warned.warnings[0].parameter.as_deref(),
            Some("context_management")
        );
    }

    #[test]
    fn cataloged_option_value_types_are_enforced_without_dropping_warned_values() {
        let mut request = ProviderRequest {
            model: "gpt-test".into(),
            messages: vec![Message::user("hello")],
            tools: Vec::new(),
            max_tokens: 64,
            options: Map::from_iter([("temperature".into(), Value::String("hot".into()))]),
            provider_options: crate::types::ProviderOptions::new(),
            compatibility_mode: CompatibilityMode::Strict,
        };
        let error = request
            .validated_options_for("openai", OPENAI_CHAT_OPTIONS)
            .unwrap_err();
        assert!(matches!(
            error.provider_error(),
            Some(error)
                if error.kind == ProviderErrorKind::InvalidRequest
                    && error.message.contains("temperature")
        ));

        request.compatibility_mode = CompatibilityMode::Warn;
        let validated = request
            .validated_options_for("openai", OPENAI_CHAT_OPTIONS)
            .unwrap();
        assert_eq!(validated.options["temperature"], "hot");
        assert_eq!(
            validated.warnings[0].parameter.as_deref(),
            Some("temperature")
        );
        assert_eq!(
            validated.warnings[0].code,
            "invalid_provider_parameter_type"
        );
    }

    #[test]
    fn schema_payload_contents_remain_opaque_to_option_keyword_validation() {
        let google = ProviderRequest {
            model: "gemini-test".into(),
            messages: Vec::new(),
            tools: Vec::new(),
            max_tokens: 64,
            options: Map::from_iter([(
                "generationConfig".into(),
                serde_json::json!({
                    "responseSchema": {
                        "type": "object",
                        "properties": {"temprature": {"type": "number"}}
                    }
                }),
            )]),
            provider_options: crate::types::ProviderOptions::new(),
            compatibility_mode: CompatibilityMode::Strict,
        };
        assert!(google
            .validated_options_for("google", GOOGLE_OPTIONS)
            .unwrap()
            .warnings
            .is_empty());

        let openai = ProviderRequest {
            model: "gpt-test".into(),
            options: Map::from_iter([(
                "response_format".into(),
                serde_json::json!({
                    "type": "json_schema",
                    "json_schema": {"schema": {"unknownValidationKeyword": true}}
                }),
            )]),
            ..google
        };
        assert!(openai
            .validated_options_for("openai", OPENAI_CHAT_OPTIONS)
            .unwrap()
            .warnings
            .is_empty());
    }

    #[test]
    fn every_shipped_option_has_a_value_type_and_responses_stateless_is_not_cataloged() {
        let catalogs = [
            ("openai", OPENAI_CHAT_OPTIONS),
            ("openai", OPENAI_RESPONSES_OPTIONS),
            ("anthropic", ANTHROPIC_OPTIONS),
            ("google", GOOGLE_OPTIONS),
            ("deepseek", DEEPSEEK_OPTIONS),
            ("openrouter", openai_compatible_options("openrouter")),
            ("groq", openai_compatible_options("groq")),
            ("mistral", openai_compatible_options("mistral")),
            ("xai", openai_compatible_options("xai")),
            (
                "mock",
                &["tool_name", "tool_input", "response_format"] as &[&str],
            ),
        ];
        for (provider, allowed) in catalogs {
            for parameter in allowed {
                assert!(
                    provider_option_kind(provider, allowed, parameter).is_some(),
                    "missing type contract for {provider}.{parameter}"
                );
            }
        }
        assert!(!OPENAI_RESPONSES_OPTIONS.contains(&"stateless"));

        let request = ProviderRequest {
            model: "gpt-test".into(),
            messages: Vec::new(),
            tools: Vec::new(),
            max_tokens: 64,
            options: Map::from_iter([("stateless".into(), Value::Bool(true))]),
            provider_options: crate::types::ProviderOptions::new(),
            compatibility_mode: CompatibilityMode::Strict,
        };
        assert!(matches!(
            request
                .validated_options_for("openai", OPENAI_RESPONSES_OPTIONS)
                .unwrap_err()
                .provider_error(),
            Some(error) if error.kind == ProviderErrorKind::InvalidRequest
        ));
    }

    #[test]
    fn current_deepseek_v4_controls_are_strict_cataloged_and_nested_typed() {
        let mut request = ProviderRequest {
            model: "deepseek-v4-pro".into(),
            messages: vec![Message::user("hello")],
            tools: Vec::new(),
            max_tokens: 64,
            options: Map::from_iter([
                ("thinking".into(), serde_json::json!({"type": "enabled"})),
                ("reasoning_effort".into(), Value::String("high".into())),
                ("user_id".into(), Value::String("tenant-42".into())),
            ]),
            provider_options: crate::types::ProviderOptions::new(),
            compatibility_mode: CompatibilityMode::Strict,
        };
        let validated = request
            .validated_options_for("deepseek", DEEPSEEK_OPTIONS)
            .unwrap();
        assert!(validated.warnings.is_empty());
        assert_eq!(validated.options["thinking"]["type"], "enabled");

        request
            .options
            .insert("thinking".into(), serde_json::json!({"mode": "enabled"}));
        assert!(matches!(
            request
                .validated_options_for("deepseek", DEEPSEEK_OPTIONS)
                .unwrap_err()
                .provider_error(),
            Some(error)
                if error.kind == ProviderErrorKind::InvalidRequest
                    && error.message.contains("thinking.mode")
        ));
    }

    #[test]
    fn sse_buffer_limit_rejects_before_growing_the_buffer() {
        let mut buffer = vec![b'x'; MAX_SSE_BUFFER_BYTES - 1];
        let original_len = buffer.len();
        assert!(!append_sse_chunk(&mut buffer, b"yz"));
        assert_eq!(buffer.len(), original_len);
        assert!(append_sse_chunk(&mut buffer, b"y"));
        assert_eq!(buffer.len(), MAX_SSE_BUFFER_BYTES);
    }

    #[test]
    fn retry_after_accepts_delta_seconds_and_http_dates_without_overflow() {
        use std::time::{Duration, UNIX_EPOCH};

        let seconds = reqwest::header::HeaderValue::from_static("7");
        assert_eq!(
            parse_retry_after_ms(Some(&seconds), UNIX_EPOCH),
            Some(7_000)
        );

        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let future = now + Duration::from_secs(3);
        let date =
            reqwest::header::HeaderValue::from_str(&httpdate::fmt_http_date(future)).unwrap();
        assert_eq!(parse_retry_after_ms(Some(&date), now), Some(3_000));

        let past = reqwest::header::HeaderValue::from_str(&httpdate::fmt_http_date(now)).unwrap();
        assert_eq!(
            parse_retry_after_ms(Some(&past), now + Duration::from_secs(1)),
            Some(0)
        );
    }

    #[test]
    fn json_retention_accounting_charges_empty_containers_and_rejects_deep_nesting() {
        assert!(json_retained_bytes(&serde_json::json!([])) >= 2);
        assert!(json_retained_bytes(&serde_json::json!({})) >= 2);

        let mut deeply_nested = Value::Null;
        for _ in 0..=MAX_JSON_ACCOUNTING_DEPTH {
            deeply_nested = Value::Array(vec![deeply_nested]);
        }
        assert_eq!(json_retained_bytes(&deeply_nested), usize::MAX);
        let mut budget = StreamRetentionBudget::default();
        assert!(!budget.retain_json(&deeply_nested, 1));
    }

    #[tokio::test]
    async fn error_response_reader_retains_only_the_bounded_prefix() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/large-error"))
            .respond_with(
                ResponseTemplate::new(500)
                    .set_body_raw("x".repeat(MAX_ERROR_BODY_BYTES * 4), "text/plain"),
            )
            .mount(&server)
            .await;

        let response = reqwest::Client::new()
            .get(format!("{}/large-error", server.uri()))
            .send()
            .await
            .unwrap();
        let body = read_error_body(response).await;
        assert!(body.ends_with('…'));
        assert_eq!(body.trim_end_matches('…').len(), MAX_ERROR_BODY_BYTES);
    }

    #[tokio::test]
    async fn native_http_timeout_before_headers_is_classified_as_provider_timeout() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/slow"))
            .respond_with(
                ResponseTemplate::new(200).set_delay(std::time::Duration::from_millis(250)),
            )
            .mount(&server)
            .await;

        let client = native_http_client_with_timeouts(
            std::time::Duration::from_secs(1),
            std::time::Duration::from_millis(25),
        )
        .unwrap();
        let error = client
            .get(format!("{}/slow", server.uri()))
            .send()
            .await
            .expect_err("delayed response must hit the total request timeout");
        assert!(error.is_timeout());
        let mapped = transport_failure("test", "model", error);
        assert_eq!(mapped.info().code, crate::error::ErrorCode::ProviderTimeout);
    }

    #[tokio::test]
    async fn native_http_timeout_while_streaming_body_is_provider_timeout() {
        let (url, server) = raw_http_body(32, b"x", Duration::from_millis(250));
        let client =
            native_http_client_with_timeouts(Duration::from_secs(1), Duration::from_millis(25))
                .unwrap();
        let response = client
            .get(url)
            .send()
            .await
            .expect("headers must arrive before the total timeout");
        let error = response_body_error(response).await;
        assert!(error.is_timeout());

        let delta = response_stream_failure("test", "model", error, "Test provider");
        assert_stream_failure(
            delta,
            crate::error::ErrorCode::ProviderTimeout,
            "Test provider response stream timed out",
        );
        server.join().unwrap();
    }

    #[tokio::test]
    async fn non_timeout_body_failure_remains_redacted_provider_transport() {
        let (url, server) = raw_http_body(32, b"x", Duration::ZERO);
        let client =
            native_http_client_with_timeouts(Duration::from_secs(1), Duration::from_secs(1))
                .unwrap();
        let response = client.get(url).send().await.unwrap();
        let error = response_body_error(response).await;
        assert!(!error.is_timeout());

        let delta = response_stream_failure("test", "model", error, "Test provider");
        assert_stream_failure(
            delta,
            crate::error::ErrorCode::ProviderTransport,
            "Test provider response stream transport failed",
        );
        server.join().unwrap();
    }

    #[tokio::test]
    async fn mock_fixture_calls_the_exact_advertised_tool_with_exact_input() {
        let expected = serde_json::json!({ "path": "notes.txt", "nested": [1, true] });
        let mut request = request_with_tools(&["first", "Read"]);
        request.provider_options.insert(
            "mock".into(),
            fixture_options(Value::String("Read".into()), Some(expected.clone())),
        );

        let deltas = MockProvider
            .stream(request)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        assert!(deltas.iter().any(|delta| matches!(
            delta,
            StreamDelta::ToolCallStart { name, .. } if name == "Read"
        )));
        assert!(deltas.iter().any(|delta| matches!(
            delta,
            StreamDelta::ToolCallInput { input, .. } if input == &expected
        )));
    }

    #[tokio::test]
    async fn mock_fixture_rejects_incomplete_or_unadvertised_controls() {
        let cases = [
            fixture_options(Value::String("Read".into()), None),
            Map::from_iter([("tool_input".into(), serde_json::json!({ "path": "x" }))]),
            fixture_options(
                Value::String("Missing".into()),
                Some(serde_json::json!({ "path": "x" })),
            ),
            fixture_options(Value::Bool(true), Some(serde_json::json!({ "path": "x" }))),
        ];

        for options in cases {
            let mut request = request_with_tools(&["Read"]);
            request.provider_options.insert("mock".into(), options);
            assert!(matches!(
                MockProvider.stream(request).await,
                Err(AikitError::Configuration(_))
            ));
        }
    }

    #[tokio::test]
    async fn mock_fixture_is_provider_key_isolated_and_default_behavior_is_unchanged() {
        let mut request = request_with_tools(&["first", "Read"]);
        request.provider_options.insert(
            "openai".into(),
            fixture_options(
                Value::String("Read".into()),
                Some(serde_json::json!({ "path": "notes.txt" })),
            ),
        );
        let deltas = MockProvider
            .stream(request)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;

        assert!(deltas.iter().any(|delta| matches!(
            delta,
            StreamDelta::ToolCallStart { name, .. } if name == "first"
        )));
        assert!(deltas.iter().any(|delta| matches!(
            delta,
            StreamDelta::ToolCallInput { input, .. }
                if input == &serde_json::json!({ "q": "merhaba" })
        )));
    }
}
