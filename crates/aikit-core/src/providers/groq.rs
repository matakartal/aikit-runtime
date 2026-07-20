//! First-class Groq adapter over the OpenAI Chat Completions wire contract.

use super::openai::stream_compatible;
use super::{Provider, ProviderRequest};
use crate::types::StreamDelta;
use async_trait::async_trait;
use futures::stream::BoxStream;

pub const DEFAULT_BASE_URL: &str = "https://api.groq.com/openai/v1";
pub const API_KEY_ENV: &str = "GROQ_API_KEY";
pub const MODEL_PREFIX: &str = "groq:";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GroqConfig {
    pub base_url: String,
}

impl Default for GroqConfig {
    fn default() -> Self {
        Self {
            base_url: DEFAULT_BASE_URL.to_string(),
        }
    }
}

/// Groq uses Bearer authentication and the current `max_completion_tokens` spelling. The local
/// `groq:` namespace is stripped before the request leaves aikit.
pub struct GroqProvider {
    api_key: String,
    config: GroqConfig,
    client: reqwest::Client,
}

impl GroqProvider {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self::with_config(api_key, GroqConfig::default())
    }

    pub fn with_base_url(api_key: impl Into<String>, base_url: impl Into<String>) -> Self {
        Self::with_config(
            api_key,
            GroqConfig {
                base_url: base_url.into(),
            },
        )
    }

    pub fn with_config(api_key: impl Into<String>, config: GroqConfig) -> Self {
        Self {
            api_key: api_key.into(),
            config,
            client: super::native_http_client(),
        }
    }
}

#[async_trait]
impl Provider for GroqProvider {
    fn name(&self) -> &str {
        "groq"
    }

    async fn stream(
        &self,
        req: ProviderRequest,
    ) -> crate::error::Result<BoxStream<'static, StreamDelta>> {
        stream_compatible(
            self.name(),
            "Groq Chat",
            &self.api_key,
            &self.config.base_url,
            Some(MODEL_PREFIX),
            "max_completion_tokens",
            true,
            &self.client,
            req,
        )
        .await
    }
}
