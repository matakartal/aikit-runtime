use aikit_core::providers::anthropic::AnthropicProvider;
use aikit_core::providers::deepseek::DeepSeekProvider;
use aikit_core::providers::google::GeminiProvider;
use aikit_core::providers::groq::{GroqConfig, GroqProvider};
use aikit_core::providers::mistral::{MistralConfig, MistralProvider};
use aikit_core::providers::openai_responses::OpenAiResponsesProvider;
use aikit_core::providers::openrouter::{OpenRouterConfig, OpenRouterProvider};
use aikit_core::providers::xai::{XaiConfig, XaiProvider};
use aikit_core::{
    ErrorCode, ErrorInfo, Message, Provider, ProviderErrorKind, ProviderOptions, ProviderRequest,
    StreamDelta,
};
use futures::StreamExt;
use serde_json::{json, Value};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const FIXTURE_KEY: &str = "fixture-key-not-a-secret";
const PRIVATE_ERROR_DETAIL: &str = "fixture-private-provider-detail";

#[derive(Clone, Copy, Debug)]
enum ProviderCase {
    Anthropic,
    DeepSeek,
    Google,
    OpenAi,
    OpenRouter,
    Groq,
    Mistral,
    Xai,
}

impl ProviderCase {
    const ALL: [Self; 8] = [
        Self::Anthropic,
        Self::DeepSeek,
        Self::Google,
        Self::OpenAi,
        Self::OpenRouter,
        Self::Groq,
        Self::Mistral,
        Self::Xai,
    ];

    fn name(self) -> &'static str {
        match self {
            Self::Anthropic => "anthropic",
            Self::DeepSeek => "deepseek",
            Self::Google => "google",
            Self::OpenAi => "openai",
            Self::OpenRouter => "openrouter",
            Self::Groq => "groq",
            Self::Mistral => "mistral",
            Self::Xai => "xai",
        }
    }

    fn requested_model(self) -> &'static str {
        match self {
            Self::Anthropic => "claude-test",
            Self::DeepSeek => "deepseek-chat",
            Self::Google => "gemini-test",
            Self::OpenAi => "gpt-test",
            Self::OpenRouter => "openrouter:openai/gpt-test",
            Self::Groq => "groq:llama-test",
            Self::Mistral => "mistral:mistral-test",
            Self::Xai => "xai:grok-test",
        }
    }

    fn wire_model(self) -> &'static str {
        match self {
            Self::OpenRouter => "openai/gpt-test",
            Self::Groq => "llama-test",
            Self::Mistral => "mistral-test",
            Self::Xai => "grok-test",
            _ => self.requested_model(),
        }
    }

    fn request_path(self) -> String {
        match self {
            Self::Anthropic => "/v1/messages".into(),
            Self::Google => format!(
                "/v1beta/models/{}:streamGenerateContent",
                self.requested_model()
            ),
            Self::OpenAi => "/responses".into(),
            _ => "/chat/completions".into(),
        }
    }

    fn success_sse(self) -> &'static str {
        match self {
            Self::Anthropic => concat!(
                "data: {\"type\":\"message_start\",\"message\":{\"model\":\"claude-test\",\"usage\":{\"input_tokens\":2}}}\n\n",
                "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
                "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"ok\"}}\n\n",
                "data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
                "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":1}}\n\n",
                "data: {\"type\":\"message_stop\"}\n\n",
            ),
            Self::Google => concat!(
                "data: {\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"ok\"}]}}]}\n\n",
                "data: {\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":2,\"candidatesTokenCount\":1}}\n\n",
            ),
            Self::OpenAi => concat!(
                "data: {\"type\":\"response.created\",\"response\":{\"model\":\"gpt-test\"}}\n\n",
                "data: {\"type\":\"response.output_text.delta\",\"delta\":\"ok\",\"item_id\":\"msg_1\",\"output_index\":0,\"content_index\":0}\n\n",
                "data: {\"type\":\"response.completed\",\"response\":{\"model\":\"gpt-test\",\"status\":\"completed\",\"output\":[],\"usage\":{\"input_tokens\":2,\"output_tokens\":1}}}\n\n",
            ),
            _ => concat!(
                "data: {\"model\":\"wire-test\",\"choices\":[{\"delta\":{\"role\":\"assistant\",\"content\":\"ok\"},\"finish_reason\":null}]}\n\n",
                "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
                "data: [DONE]\n\n",
            ),
        }
    }

    fn provider(self, base_url: String) -> Box<dyn Provider> {
        match self {
            Self::Anthropic => Box::new(AnthropicProvider::with_base_url(FIXTURE_KEY, base_url)),
            Self::DeepSeek => Box::new(DeepSeekProvider::with_base_url(FIXTURE_KEY, base_url)),
            Self::Google => Box::new(GeminiProvider::with_base_url(FIXTURE_KEY, base_url)),
            Self::OpenAi => Box::new(OpenAiResponsesProvider::with_base_url(
                FIXTURE_KEY,
                base_url,
            )),
            Self::OpenRouter => Box::new(OpenRouterProvider::with_base_url(FIXTURE_KEY, base_url)),
            Self::Groq => Box::new(GroqProvider::with_base_url(FIXTURE_KEY, base_url)),
            Self::Mistral => Box::new(MistralProvider::with_base_url(FIXTURE_KEY, base_url)),
            Self::Xai => Box::new(XaiProvider::with_base_url(FIXTURE_KEY, base_url)),
        }
    }

    fn request(self) -> ProviderRequest {
        ProviderRequest {
            model: self.requested_model().into(),
            messages: vec![Message::user("keyless fixture")],
            tools: vec![],
            max_tokens: 17,
            options: serde_json::Map::new(),
            provider_options: ProviderOptions::new(),
        }
    }
}

#[test]
fn first_class_configs_pin_the_official_default_endpoints() {
    assert_eq!(
        OpenRouterConfig::default().base_url,
        "https://openrouter.ai/api/v1"
    );
    assert_eq!(
        GroqConfig::default().base_url,
        "https://api.groq.com/openai/v1"
    );
    assert_eq!(
        MistralConfig::default().base_url,
        "https://api.mistral.ai/v1"
    );
    assert_eq!(XaiConfig::default().base_url, "https://api.x.ai/v1");
}

#[tokio::test]
async fn all_eight_adapters_honor_request_stream_auth_and_model_contracts_keylessly() {
    for case in ProviderCase::ALL {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(case.request_path()))
            .respond_with(
                ResponseTemplate::new(200).set_body_raw(case.success_sse(), "text/event-stream"),
            )
            .mount(&server)
            .await;

        let provider = case.provider(server.uri());
        assert_eq!(provider.name(), case.name());
        let output: Vec<_> = provider
            .stream(case.request())
            .await
            .unwrap_or_else(|error| panic!("{case:?} stream setup failed: {error}"))
            .collect()
            .await;
        assert!(
            output
                .iter()
                .any(|delta| matches!(delta, StreamDelta::TextDelta { text } if text == "ok")),
            "{case:?} did not emit its text delta: {output:?}"
        );
        assert!(
            output
                .iter()
                .any(|delta| matches!(delta, StreamDelta::MessageStop { .. })),
            "{case:?} did not terminate successfully: {output:?}"
        );

        let requests = server.received_requests().await.unwrap();
        assert_eq!(requests.len(), 1, "{case:?} request count");
        let request = &requests[0];
        let expected_auth = match case {
            ProviderCase::Anthropic => ("x-api-key", FIXTURE_KEY.to_string()),
            ProviderCase::Google => ("x-goog-api-key", FIXTURE_KEY.to_string()),
            _ => ("authorization", format!("Bearer {FIXTURE_KEY}")),
        };
        assert_eq!(
            request
                .headers
                .get(expected_auth.0)
                .and_then(|value| value.to_str().ok()),
            Some(expected_auth.1.as_str()),
            "{case:?} auth contract"
        );

        let body: Value = serde_json::from_slice(&request.body).unwrap();
        match case {
            ProviderCase::Google => {
                assert_eq!(body["generationConfig"]["maxOutputTokens"], 17);
                assert!(body.get("model").is_none());
            }
            ProviderCase::OpenAi => {
                assert_eq!(body["model"], case.wire_model());
                assert_eq!(body["max_output_tokens"], 17);
            }
            ProviderCase::Groq | ProviderCase::OpenRouter | ProviderCase::Xai => {
                assert_eq!(body["model"], case.wire_model());
                assert_eq!(body["max_completion_tokens"], 17);
                assert!(body.get("max_tokens").is_none());
            }
            ProviderCase::Anthropic | ProviderCase::DeepSeek | ProviderCase::Mistral => {
                assert_eq!(body["model"], case.wire_model());
                assert_eq!(body["max_tokens"], 17);
                assert!(body.get("max_completion_tokens").is_none());
                if matches!(case, ProviderCase::Mistral) {
                    assert!(body.get("stream_options").is_none());
                }
            }
        }
    }
}

#[tokio::test]
async fn all_eight_adapters_reject_protected_options_before_network_io() {
    for case in ProviderCase::ALL {
        let server = MockServer::start().await;
        let provider = case.provider(server.uri());
        let mut request = case.request();
        request.provider_options.insert(
            case.name().into(),
            serde_json::Map::from_iter([("model".into(), json!("forbidden-override"))]),
        );

        let error = provider
            .stream(request)
            .await
            .err()
            .unwrap_or_else(|| panic!("{case:?} accepted a protected model override"));
        let error = error
            .provider_error()
            .unwrap_or_else(|| panic!("{case:?} did not return a typed provider error"));
        assert_eq!(error.provider, case.name(), "{case:?} provider context");
        assert_eq!(error.kind, ProviderErrorKind::InvalidRequest, "{case:?}");
        assert!(server.received_requests().await.unwrap().is_empty());
    }
}

#[tokio::test]
async fn all_eight_adapters_classify_http_errors_and_expose_only_scrubbed_error_info() {
    for case in ProviderCase::ALL {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(case.request_path()))
            .respond_with(
                ResponseTemplate::new(429)
                    .insert_header("retry-after", "2")
                    .set_body_json(json!({"error": {"message": PRIVATE_ERROR_DETAIL}})),
            )
            .mount(&server)
            .await;

        let provider = case.provider(server.uri());
        let error = provider
            .stream(case.request())
            .await
            .err()
            .unwrap_or_else(|| panic!("{case:?} unexpectedly accepted HTTP 429"));
        let provider_error = error
            .provider_error()
            .unwrap_or_else(|| panic!("{case:?} did not return a typed provider error"));
        assert_eq!(provider_error.provider, case.name(), "{case:?}");
        assert_eq!(provider_error.model, case.requested_model(), "{case:?}");
        assert_eq!(
            provider_error.kind,
            ProviderErrorKind::RateLimited,
            "{case:?}"
        );
        assert_eq!(provider_error.status, Some(429), "{case:?}");
        assert_eq!(provider_error.retry_after_ms, Some(2_000), "{case:?}");

        let safe = ErrorInfo::from(provider_error);
        assert_eq!(safe.code, ErrorCode::ProviderRateLimit, "{case:?}");
        assert_eq!(safe.provider.as_deref(), Some(case.name()), "{case:?}");
        assert!(safe.retryable, "{case:?}");
        assert!(!safe.message.contains(PRIVATE_ERROR_DETAIL), "{case:?}");
        assert!(!safe.message.contains(FIXTURE_KEY), "{case:?}");
    }
}

#[tokio::test]
async fn compatible_stream_errors_keep_the_first_class_provider_identity() {
    for case in [
        ProviderCase::OpenRouter,
        ProviderCase::Groq,
        ProviderCase::Mistral,
        ProviderCase::Xai,
    ] {
        let server = MockServer::start().await;
        let sse = format!(
            "data: {{\"error\":{{\"code\":429,\"message\":\"{PRIVATE_ERROR_DETAIL}\"}}}}\n\n"
        );
        Mock::given(method("POST"))
            .and(path(case.request_path()))
            .respond_with(ResponseTemplate::new(200).set_body_raw(sse, "text/event-stream"))
            .mount(&server)
            .await;

        let provider = case.provider(server.uri());
        let output: Vec<_> = provider
            .stream(case.request())
            .await
            .unwrap()
            .collect()
            .await;
        assert!(matches!(
            output.as_slice(),
            [StreamDelta::Error { message, info }]
                if info.code == ErrorCode::ProviderRateLimit
                    && info.provider.as_deref() == Some(case.name())
                    && info.model.as_deref() == Some(case.requested_model())
                    && !message.contains(PRIVATE_ERROR_DETAIL)
        ));
    }
}

#[tokio::test]
async fn xai_accepts_the_natural_grok_model_without_rewriting_it() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_raw(ProviderCase::Xai.success_sse(), "text/event-stream"),
        )
        .mount(&server)
        .await;

    let provider = XaiProvider::with_base_url(FIXTURE_KEY, server.uri());
    let mut request = ProviderCase::Xai.request();
    request.model = "grok-natural-test".into();
    let _: Vec<_> = provider.stream(request).await.unwrap().collect().await;

    let requests = server.received_requests().await.unwrap();
    let body: Value = serde_json::from_slice(&requests[0].body).unwrap();
    assert_eq!(body["model"], "grok-natural-test");
}
