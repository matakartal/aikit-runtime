//! Canonical schema — the spine that prevents lowest-common-denominator flattening.
//!
//! Every provider adapter maps its wire format to/from these types. Provider-unique
//! capabilities ride *on* these structures (via `provider_options` / `provider_metadata`)
//! rather than being averaged away. Reasoning state is kept with its signature/opaque
//! payload intact and replayed verbatim per each provider's rules (see `reasoning`).

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

/// Who authored a message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    System,
    User,
    Assistant,
    Tool,
}

/// A single piece of message content. The union is deliberately closed and typed:
/// this is what keeps reasoning/citations/tool-calls from collapsing into plain text.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContentBlock {
    Text {
        text: String,
    },
    /// Model reasoning ("thinking"). Provider-specific opaque state travels WITH the block
    /// so it can be replayed verbatim in later turns — see the `reasoning` module:
    ///  - Anthropic: `signature` is a signed hash; the API rejects a replayed thinking block
    ///    whose text or signature was tampered with.
    ///  - Google: `signature` carries the thought signature.
    ///  - OpenAI: `opaque` carries the reasoning-item id / encrypted content.
    ///  - DeepSeek: `reasoning_content` is captured here and replayed for tool-call turns only.
    Reasoning {
        text: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        signature: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        provider: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        opaque: Option<Value>,
    },
    ToolUse {
        id: String,
        name: String,
        input: Value,
    },
    ToolResult {
        tool_use_id: String,
        content: String,
        #[serde(default)]
        is_error: bool,
    },
    /// Multimodal input (image / other media), referenced by URL or inline base64.
    Media {
        media_type: String,
        source: MediaSource,
    },
    /// A citation emitted by a provider that supports grounded output.
    Citation {
        text: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        source: Option<String>,
        /// Provider-native citation/grounding object, retained to avoid flattening ranges,
        /// document ids, redirects, and future fields into one URL string.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        metadata: Option<Value>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MediaSource {
    Url { url: String },
    Base64 { data: String },
}

/// One turn of the conversation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Message {
    pub role: Role,
    pub content: Vec<ContentBlock>,
}

impl Message {
    pub fn user(text: impl Into<String>) -> Self {
        Message {
            role: Role::User,
            content: vec![ContentBlock::Text { text: text.into() }],
        }
    }

    pub fn system(text: impl Into<String>) -> Self {
        Message {
            role: Role::System,
            content: vec![ContentBlock::Text { text: text.into() }],
        }
    }

    /// The reasoning blocks in this message, in order.
    pub fn reasoning_blocks(&self) -> Vec<&ContentBlock> {
        self.content
            .iter()
            .filter(|b| matches!(b, ContentBlock::Reasoning { .. }))
            .collect()
    }
}

/// A tool the model may call. `input_schema` is a JSON Schema object.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolSpec {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
}

impl ToolSpec {
    pub fn new(
        name: impl Into<String>,
        description: impl Into<String>,
        input_schema: Value,
    ) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            input_schema,
        }
    }
}

/// Typed, per-provider escape hatch carried untouched to the wire (Vercel `providerOptions`
/// style). Keyed by provider name → arbitrary options object (e.g. Anthropic `cache_control`
/// + `thinking`, OpenAI `reasoning_effort`, Gemini `thinking_budget`). Never flattened away.
pub type ProviderOptions = BTreeMap<String, serde_json::Map<String, Value>>;

/// Provider-unique data carried back OUT on results (cache details, logprobs, grounding and the
/// raw finish reason). Keyed by provider name; each value is the ordered list of response-level
/// metadata objects observed across a possibly multi-turn run. Keeping the response boundaries
/// avoids overwriting an earlier turn's native fields. The anti-LCD counterpart of
/// [`ProviderOptions`].
///
/// # Sensitive data
///
/// This is raw provider output, not sanitized telemetry. Depending on requested features it can
/// contain generated tokens, search queries, URLs, citations, or other prompt-derived data. Treat
/// it with the same confidentiality as model output before logging or persisting it.
pub type ProviderMetadata = BTreeMap<String, Vec<Value>>;

/// Token accounting. Cache + reasoning fields are `0` when a provider does not report them.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Usage {
    #[serde(default)]
    pub input_tokens: u64,
    #[serde(default)]
    pub output_tokens: u64,
    #[serde(default)]
    pub cache_creation_input_tokens: u64,
    #[serde(default)]
    pub cache_read_input_tokens: u64,
    #[serde(default)]
    pub reasoning_tokens: u64,
}

/// A typed part of the streamed response. The host receives these one by one as an
/// async iterator — reasoning / text / tool-call / tool-result arrive as distinct parts
/// in a single surface (the Vercel-AI-SDK-style multi-part stream).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StreamDelta {
    MessageStart {
        model: String,
    },
    TextDelta {
        text: String,
    },
    ReasoningDelta {
        text: String,
    },
    /// Emitted once a reasoning block completes, carrying its signature/opaque payload so the
    /// host (and the loop) can persist it for faithful replay.
    ReasoningComplete {
        text: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        signature: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        opaque: Option<Value>,
    },
    ToolCallStart {
        id: String,
        name: String,
    },
    ToolCallInput {
        id: String,
        input: Value,
    },
    ToolResult {
        tool_use_id: String,
        content: String,
        is_error: bool,
    },
    Citation {
        text: String,
        source: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        metadata: Option<Value>,
    },
    /// Raw provider-native response metadata. It excludes credentials and request bodies, but it
    /// is still sensitive: logprobs can contain generated tokens and grounding data can contain
    /// prompt-derived searches/URLs. Runtime aggregation groups these objects by `provider`
    /// without flattening their native shape; callers must explicitly protect any copy they log
    /// or persist.
    ProviderMetadata {
        provider: String,
        metadata: Value,
    },
    Usage(Usage),
    MessageStop {
        stop_reason: String,
    },
    Error {
        message: String,
        /// Stable, redacted machine-readable classification. `default` keeps transcripts written
        /// by pre-typed-error releases readable; newly emitted errors always populate it.
        #[serde(default)]
        info: crate::error::ErrorInfo,
    },
}

impl StreamDelta {
    /// Build a terminal/error delta from a typed core error without parsing its display text.
    pub fn from_error(error: &crate::error::AikitError) -> Self {
        let info = error.info();
        let message = if matches!(
            error,
            crate::error::AikitError::ProviderFailure(_) | crate::error::AikitError::Provider(_)
        ) {
            info.message.clone()
        } else {
            error.to_string()
        };
        Self::Error { message, info }
    }

    /// Build an error delta when a compatibility message has already been selected by a hook.
    /// `info` remains redacted and is never derived from that arbitrary message.
    pub fn error_with_info(message: impl Into<String>, info: crate::error::ErrorInfo) -> Self {
        Self::Error {
            message: message.into(),
            info,
        }
    }

    pub fn error_info(&self) -> Option<&crate::error::ErrorInfo> {
        match self {
            Self::Error { info, .. } => Some(info),
            _ => None,
        }
    }
}

#[cfg(test)]
mod stream_error_tests {
    use super::*;
    use crate::error::{ErrorCode, ProviderError};

    #[test]
    fn typed_stream_error_has_stable_nested_shape_and_redacts_provider_body() {
        let secret = "Authorization: Bearer sk-secret";
        let error = crate::error::AikitError::from(ProviderError::from_http(
            "openai",
            "gpt-test",
            429,
            Some(2_000),
            secret,
        ));
        let delta = StreamDelta::from_error(&error);
        let encoded = serde_json::to_value(&delta).unwrap();
        assert_eq!(encoded["type"], "error");
        assert_eq!(encoded["message"], "provider rate limit exceeded");
        assert_eq!(encoded["info"]["code"], "provider_rate_limit");
        assert_eq!(encoded["info"]["provider"], "openai");
        assert_eq!(encoded["info"]["model"], "gpt-test");
        assert_eq!(encoded["info"]["status"], 429);
        assert_eq!(encoded["info"]["retryable"], true);
        assert!(!encoded.to_string().contains(secret));
    }

    #[test]
    fn legacy_error_delta_deserializes_with_unknown_info() {
        let delta: StreamDelta = serde_json::from_value(serde_json::json!({
            "type": "error",
            "message": "legacy"
        }))
        .unwrap();
        assert!(matches!(
            delta,
            StreamDelta::Error { info, .. } if info.code == ErrorCode::Unknown
        ));
    }
}
