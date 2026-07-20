//! Versioned, provider-neutral public contracts.
//!
//! These types complement the legacy [`crate::types::StreamDelta`] surface.  They make
//! capability uncertainty, compatibility decisions, media provenance, and stream block
//! lifecycles explicit without forcing provider-native data into a lowest-common-denominator
//! representation.

use crate::error::ErrorInfo;
use crate::types::{MediaSource, Usage};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Whether a concrete model is known to support a capability.
///
/// `Unknown` deliberately differs from `Unsupported`: required capabilities fail closed in both
/// cases, while diagnostics can still tell users whether the model was tested or rejected.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityState {
    Supported,
    Unsupported,
    #[default]
    Unknown,
}

/// How provider-specific parameters that cannot be represented safely should be handled.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompatibilityMode {
    /// Reject unsupported and unknown parameters before network I/O.
    #[default]
    Strict,
    /// Continue only where the adapter can preserve semantics and return a warning.
    Warn,
    /// Permit explicitly documented adapter fallbacks. Silent parameter dropping is still
    /// forbidden and every fallback must produce a warning.
    BestEffort,
}

/// A machine-readable, non-fatal compatibility decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderWarning {
    pub code: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parameter: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
}

/// Lossless media input with caller-supplied integrity and size metadata.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MediaInputSource {
    Url { url: String },
    Base64 { data: String },
    Bytes { data: Vec<u8> },
    Artifact { artifact_id: String },
}

impl From<MediaSource> for MediaInputSource {
    fn from(source: MediaSource) -> Self {
        match source {
            MediaSource::Url { url } => Self::Url { url },
            MediaSource::Base64 { data } => Self::Base64 { data },
        }
    }
}

/// Lossless media input with caller-supplied integrity and size metadata.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MediaInput {
    pub media_type: String,
    pub source: MediaInputSource,
    /// Lowercase hexadecimal SHA-256 of the resolved bytes.
    pub sha256: String,
    pub size_bytes: u64,
}

impl MediaInput {
    pub fn validate(&self) -> std::result::Result<(), String> {
        if self.media_type.trim().is_empty() || !self.media_type.contains('/') {
            return Err("media_type must be a non-empty MIME type".into());
        }
        if self.size_bytes == 0 {
            return Err("size_bytes must be greater than zero".into());
        }
        if self.sha256.len() != 64
            || !self
                .sha256
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        {
            return Err("sha256 must be 64 lowercase hexadecimal characters".into());
        }
        Ok(())
    }
}

/// A completed, provider-neutral output part.
///
/// Streaming uses [`StreamEvent`]; this enum is the stable materialized representation used by
/// artifacts, durable checkpoints, protocol adapters, and host SDKs. Media outputs retain the
/// same MIME, size, hash, and artifact-reference requirements as [`MediaInput`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OutputPart {
    Text {
        text: String,
    },
    Reasoning {
        text: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        signature: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        provider: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        opaque: Option<Value>,
    },
    Image {
        media: MediaInput,
    },
    Audio {
        media: MediaInput,
    },
    File {
        media: MediaInput,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        filename: Option<String>,
    },
    Transcript {
        text: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        language: Option<String>,
    },
    ToolCall {
        id: String,
        name: String,
        input: Value,
    },
    ToolResult {
        tool_use_id: String,
        content: String,
        is_error: bool,
    },
    StructuredData {
        value: Value,
    },
    Citation {
        text: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        source: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        metadata: Option<Value>,
    },
}

/// Stable categories for interleavable response blocks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamBlockKind {
    Text,
    Reasoning,
    ToolCall,
    ToolResult,
    Citation,
    Image,
    Audio,
    Transcript,
    StructuredData,
}

/// One ordered v2 stream event. `event_id` identifies the event itself while `block_id` groups
/// start/delta/end events belonging to the same interleavable content block.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamEvent {
    pub event_id: String,
    pub sequence: u64,
    #[serde(flatten)]
    pub kind: StreamEventKind,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StreamEventKind {
    ResponseStart {
        response_id: String,
        model: String,
    },
    BlockStart {
        block_id: String,
        block_kind: StreamBlockKind,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        name: Option<String>,
    },
    BlockDelta {
        block_id: String,
        delta: Value,
    },
    BlockEnd {
        block_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        value: Option<Value>,
    },
    ProviderMetadata {
        provider: String,
        metadata: Value,
    },
    Usage {
        usage: Usage,
    },
    Warning {
        warning: ProviderWarning,
    },
    ResponseEnd {
        response_id: String,
        stop_reason: String,
    },
    Error {
        message: String,
        info: ErrorInfo,
    },
    RawProviderEvent {
        provider: String,
        event: Value,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compatibility_defaults_to_strict() {
        assert_eq!(CompatibilityMode::default(), CompatibilityMode::Strict);
        assert_eq!(CapabilityState::default(), CapabilityState::Unknown);
    }

    #[test]
    fn media_integrity_is_fail_closed() {
        let valid = MediaInput {
            media_type: "image/png".into(),
            source: MediaInputSource::Url {
                url: "https://example.test/image.png".into(),
            },
            sha256: "a".repeat(64),
            size_bytes: 1,
        };
        assert!(valid.validate().is_ok());

        let mut invalid = valid;
        invalid.sha256 = "UNKNOWN".into();
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn stream_event_has_stable_envelope() {
        let event = StreamEvent {
            event_id: "evt-1".into(),
            sequence: 1,
            kind: StreamEventKind::BlockStart {
                block_id: "text-1".into(),
                block_kind: StreamBlockKind::Text,
                name: None,
            },
        };
        let json = serde_json::to_value(event).unwrap();
        assert_eq!(json["event_id"], "evt-1");
        assert_eq!(json["type"], "block_start");
        assert_eq!(json["block_kind"], "text");
    }

    #[test]
    fn output_parts_preserve_typed_media_and_structured_data() {
        let media = MediaInput {
            media_type: "image/png".into(),
            source: MediaInputSource::Artifact {
                artifact_id: "artifact-1".into(),
            },
            sha256: "a".repeat(64),
            size_bytes: 12,
        };
        let image = serde_json::to_value(OutputPart::Image { media }).unwrap();
        let structured = serde_json::to_value(OutputPart::StructuredData {
            value: serde_json::json!({"ok": true}),
        })
        .unwrap();

        assert_eq!(image["type"], "image");
        assert_eq!(image["media"]["source"]["kind"], "artifact");
        assert_eq!(structured["type"], "structured_data");
        assert_eq!(structured["value"]["ok"], true);
    }
}
