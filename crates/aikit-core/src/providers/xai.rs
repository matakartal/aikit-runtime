//! First-class xAI/Grok adapter over the OpenAI Chat Completions wire contract.

use super::openai::stream_compatible;
use super::{Provider, ProviderRequest};
use crate::types::StreamDelta;
use async_trait::async_trait;
use futures::stream::BoxStream;

pub const DEFAULT_BASE_URL: &str = "https://api.x.ai/v1";
pub const API_KEY_ENV: &str = "XAI_API_KEY";
pub const MODEL_PREFIX: &str = "xai:";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct XaiConfig {
    pub base_url: String,
}

impl Default for XaiConfig {
    fn default() -> Self {
        Self {
            base_url: DEFAULT_BASE_URL.to_string(),
        }
    }
}

/// xAI uses Bearer authentication. Explicit `xai:grok-*` model ids lose the routing namespace;
/// natural `grok-*` ids already match the wire contract and pass through unchanged.
pub struct XaiProvider {
    api_key: String,
    config: XaiConfig,
    client: reqwest::Client,
}

impl XaiProvider {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self::with_config(api_key, XaiConfig::default())
    }

    pub fn with_base_url(api_key: impl Into<String>, base_url: impl Into<String>) -> Self {
        Self::with_config(
            api_key,
            XaiConfig {
                base_url: base_url.into(),
            },
        )
    }

    pub fn with_config(api_key: impl Into<String>, config: XaiConfig) -> Self {
        Self {
            api_key: api_key.into(),
            config,
            client: super::native_http_client(),
        }
    }
}

#[async_trait]
impl Provider for XaiProvider {
    fn name(&self) -> &str {
        "xai"
    }

    async fn stream(
        &self,
        req: ProviderRequest,
    ) -> crate::error::Result<BoxStream<'static, StreamDelta>> {
        stream_compatible(
            self.name(),
            "xAI Chat",
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
