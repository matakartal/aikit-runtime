//! Provider adapter layer.
//!
//! Each provider (Anthropic, OpenAI, Google, DeepSeek, openai-compat) implements
//! [`Provider`] by speaking its native wire format over raw HTTP. The wire ↔ canonical
//! translation for each lives in its submodule (e.g. [`anthropic`]); [`MockProvider`] is the
//! deterministic in-memory provider used for tests and the FFI spike.

pub mod anthropic;
pub mod deepseek;
pub mod google;
pub mod openai;
pub mod openai_responses;

use crate::error::{AikitError, ProviderError, ProviderErrorKind, Result};
use crate::types::{ContentBlock, Message, StreamDelta, ToolSpec};
use async_trait::async_trait;
use futures::stream::BoxStream;
use serde_json::{Map, Value};

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

pub(crate) fn transport_failure(provider: &str, model: &str, error: reqwest::Error) -> AikitError {
    let kind = if error.is_timeout() {
        ProviderErrorKind::Timeout
    } else {
        ProviderErrorKind::Transport
    };
    ProviderError::new(provider, model, kind, error.to_string()).into()
}

pub(crate) fn http_failure(
    provider: &str,
    model: &str,
    status: reqwest::StatusCode,
    retry_after: Option<&reqwest::header::HeaderValue>,
    mut body: String,
) -> AikitError {
    const MAX_ERROR_BYTES: usize = 4096;
    if body.len() > MAX_ERROR_BYTES {
        let mut end = MAX_ERROR_BYTES;
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
