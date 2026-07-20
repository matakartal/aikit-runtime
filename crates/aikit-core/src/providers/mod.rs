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

use crate::error::{AikitError, ProviderError, ProviderErrorKind, Result};
use crate::types::{ContentBlock, Message, StreamDelta, ToolSpec};
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::StreamExt;
use serde_json::{Map, Value};
use std::time::Duration;

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
}

impl ProviderRequest {
    pub fn options_for(&self, provider: &str) -> serde_json::Map<String, serde_json::Value> {
        let mut options = self.options.clone();
        if let Some(provider_options) = self.provider_options.get(provider) {
            options.extend(provider_options.clone());
        }
        options
    }
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
    let retry_after_ms = retry_after
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<u64>().ok())
        .map(|seconds| seconds.saturating_mul(1000));
    ProviderError::from_http(provider, model, status.as_u16(), retry_after_ms, body).into()
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
        // Validate explicit fixture controls even when another mock mode (such as structured
        // output) would otherwise return early. A misspelled or unadvertised tool must never be
        // silently ignored.
        let fixture = mock_tool_fixture(&req)?;

        // Structured-output binding demos use the same planner + validator as live models while
        // remaining keyless. The native-constrained mock receives the schema through the
        // response_format escape hatch and deterministically materializes one valid value.
        if let Some(schema) = req
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
            return Ok(Box::pin(futures::stream::iter(deltas)));
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

        Ok(Box::pin(futures::stream::iter(deltas)))
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
    fn sse_buffer_limit_rejects_before_growing_the_buffer() {
        let mut buffer = vec![b'x'; MAX_SSE_BUFFER_BYTES - 1];
        let original_len = buffer.len();
        assert!(!append_sse_chunk(&mut buffer, b"yz"));
        assert_eq!(buffer.len(), original_len);
        assert!(append_sse_chunk(&mut buffer, b"y"));
        assert_eq!(buffer.len(), MAX_SSE_BUFFER_BYTES);
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
