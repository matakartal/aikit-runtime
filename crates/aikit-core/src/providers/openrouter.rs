//! First-class OpenRouter adapter over the OpenAI Chat Completions wire contract.

use super::openai::stream_compatible;
use super::{Provider, ProviderRequest};
use crate::types::StreamDelta;
use async_trait::async_trait;
use futures::stream::BoxStream;

pub const DEFAULT_BASE_URL: &str = "https://openrouter.ai/api/v1";
pub const API_KEY_ENV: &str = "OPENROUTER_API_KEY";
pub const MODEL_PREFIX: &str = "openrouter:";

/// Non-secret OpenRouter transport configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpenRouterConfig {
    pub base_url: String,
}

impl Default for OpenRouterConfig {
    fn default() -> Self {
        Self {
            base_url: DEFAULT_BASE_URL.to_string(),
        }
    }
}

/// OpenRouter uses Bearer authentication and keeps its explicit `openrouter:` routing namespace
/// out of the wire model (for example `openrouter:anthropic/claude-sonnet-4` becomes
/// `anthropic/claude-sonnet-4`).
pub struct OpenRouterProvider {
    api_key: String,
    config: OpenRouterConfig,
    client: reqwest::Client,
}

impl OpenRouterProvider {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self::with_config(api_key, OpenRouterConfig::default())
    }

    pub fn with_base_url(api_key: impl Into<String>, base_url: impl Into<String>) -> Self {
        Self::with_config(
            api_key,
            OpenRouterConfig {
                base_url: base_url.into(),
            },
        )
    }

    pub fn with_config(api_key: impl Into<String>, config: OpenRouterConfig) -> Self {
        Self {
            api_key: api_key.into(),
            config,
            client: super::native_http_client(),
        }
    }
}

#[async_trait]
impl Provider for OpenRouterProvider {
    fn name(&self) -> &str {
        "openrouter"
    }

    async fn stream(
        &self,
        req: ProviderRequest,
    ) -> crate::error::Result<BoxStream<'static, StreamDelta>> {
        stream_compatible(
            self.name(),
            "OpenRouter Chat",
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
