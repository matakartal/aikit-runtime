//! Versioned, provider-neutral public contracts.
//!
//! These types complement the legacy [`crate::types::StreamDelta`] surface.  They make
//! capability uncertainty, compatibility decisions, media provenance, and stream block
//! lifecycles explicit without forcing provider-native data into a lowest-common-denominator
//! representation.

use crate::error::ErrorInfo;
use crate::types::{MediaSource, Usage};
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::read::DecoderReader;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::io::Read as _;

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
        let Some((media_type, media_subtype)) = self.media_type.split_once('/') else {
            return Err("media_type must be a type/subtype MIME token".into());
        };
        if media_type.is_empty()
            || media_subtype.is_empty()
            || media_subtype.contains('/')
            || !media_type.bytes().all(is_mime_token_byte)
            || !media_subtype.bytes().all(is_mime_token_byte)
        {
            return Err("media_type must be a type/subtype MIME token".into());
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
        match &self.source {
            MediaInputSource::Url { url } => {
                let parsed = url::Url::parse(url)
                    .map_err(|_| "media URL must be an absolute http(s) URL".to_string())?;
                if !matches!(parsed.scheme(), "http" | "https") || parsed.host_str().is_none() {
                    return Err("media URL must be an absolute http(s) URL".into());
                }
                if !parsed.username().is_empty() || parsed.password().is_some() {
                    return Err("media URL must not contain credentials".into());
                }
            }
            MediaInputSource::Artifact { artifact_id } => {
                if artifact_id.trim().is_empty() {
                    return Err("artifact_id must not be empty".into());
                }
            }
            MediaInputSource::Bytes { data } => {
                self.validate_resolved_bytes(data.len() as u64, Sha256::digest(data))?;
            }
            MediaInputSource::Base64 { data } => {
                if data.is_empty() {
                    return Err("base64 media data must not be empty".into());
                }
                let mut decoder = DecoderReader::new(data.as_bytes(), &BASE64_STANDARD);
                let mut digest = Sha256::new();
                let mut decoded_size = 0_u64;
                let mut buffer = [0_u8; 8 * 1024];
                loop {
                    let read = decoder
                        .read(&mut buffer)
                        .map_err(|_| "base64 media data is invalid".to_string())?;
                    if read == 0 {
                        break;
                    }
                    decoded_size = decoded_size
                        .checked_add(read as u64)
                        .ok_or_else(|| "decoded media size exceeds u64".to_string())?;
                    if decoded_size > self.size_bytes {
                        return Err("decoded media exceeds declared size_bytes".into());
                    }
                    digest.update(&buffer[..read]);
                }
                self.validate_resolved_bytes(decoded_size, digest.finalize())?;
            }
        }
        Ok(())
    }

    fn validate_resolved_bytes(
        &self,
        actual_size: u64,
        digest: impl AsRef<[u8]>,
    ) -> std::result::Result<(), String> {
        if actual_size != self.size_bytes {
            return Err(format!(
                "resolved media size {actual_size} does not match declared size_bytes {}",
                self.size_bytes
            ));
        }
        let actual_hash = lowercase_hex(digest.as_ref());
        if actual_hash != self.sha256 {
            return Err("resolved media sha256 does not match declared sha256".into());
        }
        Ok(())
    }
}

fn lowercase_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn is_mime_token_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(
            byte,
            b'!' | b'#'
                | b'$'
                | b'%'
                | b'&'
                | b'\''
                | b'*'
                | b'+'
                | b'-'
                | b'.'
                | b'^'
                | b'_'
                | b'`'
                | b'|'
                | b'~'
        )
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
    fn inline_media_integrity_is_verified_against_resolved_bytes() {
        let valid = MediaInput {
            media_type: "application/octet-stream".into(),
            source: MediaInputSource::Base64 {
                data: "YWJj".into(),
            },
            sha256: "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad".into(),
            size_bytes: 3,
        };
        assert!(valid.validate().is_ok());

        let mut wrong_size = valid.clone();
        wrong_size.size_bytes = 2;
        assert_eq!(
            wrong_size.validate().unwrap_err(),
            "decoded media exceeds declared size_bytes"
        );

        let mut wrong_hash = valid;
        wrong_hash.sha256 = "0".repeat(64);
        assert_eq!(
            wrong_hash.validate().unwrap_err(),
            "resolved media sha256 does not match declared sha256"
        );
    }

    #[test]
    fn media_references_reject_empty_or_credentialed_sources() {
        let metadata = |source| MediaInput {
            media_type: "image/png".into(),
            source,
            sha256: "a".repeat(64),
            size_bytes: 1,
        };
        assert!(metadata(MediaInputSource::Artifact {
            artifact_id: " ".into()
        })
        .validate()
        .is_err());
        assert!(metadata(MediaInputSource::Url {
            url: "https://user:secret@example.test/media".into()
        })
        .validate()
        .is_err());
        assert!(metadata(MediaInputSource::Url {
            url: "file:///tmp/media".into()
        })
        .validate()
        .is_err());
    }

    #[test]
    fn media_type_rejects_parameters_whitespace_and_extra_segments() {
        let media = |media_type: &str| MediaInput {
            media_type: media_type.into(),
            source: MediaInputSource::Url {
                url: "https://example.test/media".into(),
            },
            sha256: "a".repeat(64),
            size_bytes: 1,
        };
        assert!(media("image/png").validate().is_ok());
        assert!(media("image/png; charset=utf-8").validate().is_err());
        assert!(media("image /png").validate().is_err());
        assert!(media("image/png/extra").validate().is_err());
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
