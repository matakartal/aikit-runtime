//! First-class Mistral adapter over the OpenAI Chat Completions wire contract.

use super::openai::stream_compatible;
use super::{Provider, ProviderRequest};
use crate::types::StreamDelta;
use async_trait::async_trait;
use futures::stream::BoxStream;

pub const DEFAULT_BASE_URL: &str = "https://api.mistral.ai/v1";
pub const API_KEY_ENV: &str = "MISTRAL_API_KEY";
pub const MODEL_PREFIX: &str = "mistral:";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MistralConfig {
    pub base_url: String,
}

impl Default for MistralConfig {
    fn default() -> Self {
        Self {
            base_url: DEFAULT_BASE_URL.to_string(),
        }
    }
}

/// Mistral uses Bearer authentication but names its output ceiling `max_tokens`. The local
/// `mistral:` namespace is stripped before sending the model id.
pub struct MistralProvider {
    api_key: String,
    config: MistralConfig,
    client: reqwest::Client,
}

impl MistralProvider {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self::with_config(api_key, MistralConfig::default())
    }

    pub fn with_base_url(api_key: impl Into<String>, base_url: impl Into<String>) -> Self {
        Self::with_config(
            api_key,
            MistralConfig {
                base_url: base_url.into(),
            },
        )
    }

    pub fn with_config(api_key: impl Into<String>, config: MistralConfig) -> Self {
        Self {
            api_key: api_key.into(),
            config,
            client: super::native_http_client(),
        }
    }
}

#[async_trait]
impl Provider for MistralProvider {
    fn name(&self) -> &str {
        "mistral"
    }

    async fn stream(
        &self,
        req: ProviderRequest,
    ) -> crate::error::Result<BoxStream<'static, StreamDelta>> {
        stream_compatible(
            self.name(),
            "Mistral Chat",
            &self.api_key,
            &self.config.base_url,
            Some(MODEL_PREFIX),
            "max_tokens",
            false,
            &self.client,
            req,
        )
        .await
    }
}
