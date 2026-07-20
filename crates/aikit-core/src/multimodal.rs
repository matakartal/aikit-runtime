//! Provider-neutral multimodal artifacts and realtime session state.
//!
//! Provider support is advertised separately through model profiles. These types never imply
//! that every provider can execute every modality.

use crate::contract::MediaInput;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

pub const MAX_REALTIME_DEDUPE_EVENTS: usize = 65_536;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModalityRequirement {
    Text,
    Reasoning,
    ImageInput,
    ImageGeneration,
    DocumentInput,
    AudioInput,
    Transcription,
    SpeechGeneration,
    RealtimeDuplex,
    ToolUse,
    StructuredOutput,
}

/// Immutable, content-addressed media persisted by the host or a durable artifact store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MediaArtifact {
    pub artifact_id: String,
    pub media_type: String,
    pub sha256: String,
    pub size_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
}

impl MediaArtifact {
    pub fn validate(&self) -> std::result::Result<(), String> {
        let input = MediaInput {
            media_type: self.media_type.clone(),
            source: crate::contract::MediaInputSource::Artifact {
                artifact_id: self.artifact_id.clone(),
            },
            sha256: self.sha256.clone(),
            size_bytes: self.size_bytes,
        };
        if self.artifact_id.trim().is_empty() {
            return Err("artifact_id must not be empty".into());
        }
        input.validate()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GeneratedImage {
    pub artifact: MediaArtifact,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revised_prompt: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_metadata: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GeneratedAudio {
    pub artifact: MediaArtifact,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub voice: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_metadata: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TranscriptSegment {
    pub start_ms: u64,
    pub end_ms: u64,
    pub text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub speaker: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confidence: Option<f32>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Transcript {
    pub text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub language: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub segments: Vec<TranscriptSegment>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_metadata: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VoiceActivityPolicy {
    pub enabled: bool,
    pub threshold: f32,
    pub prefix_padding_ms: u32,
    pub silence_duration_ms: u32,
    pub interrupt_response: bool,
}

impl Default for VoiceActivityPolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            threshold: 0.5,
            prefix_padding_ms: 300,
            silence_duration_ms: 500,
            interrupt_response: true,
        }
    }
}

impl VoiceActivityPolicy {
    pub fn validate(&self) -> std::result::Result<(), String> {
        if !self.threshold.is_finite() || !(0.0..=1.0).contains(&self.threshold) {
            return Err("voice activity threshold must be within 0..=1".into());
        }
        if self.silence_duration_ms == 0 {
            return Err("silence_duration_ms must be greater than zero".into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RealtimeSessionState {
    #[default]
    Connecting,
    Active,
    Interrupted,
    Closed,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RealtimeEvent {
    pub event_id: String,
    pub sequence: u64,
    #[serde(flatten)]
    pub kind: RealtimeEventKind,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RealtimeEventKind {
    Connected,
    AudioInput {
        artifact: MediaArtifact,
    },
    AudioDelta {
        data_base64: String,
    },
    TranscriptDelta {
        text: String,
    },
    TextDelta {
        text: String,
    },
    ToolCall {
        id: String,
        name: String,
        input: Value,
    },
    ToolResult {
        id: String,
        output: Value,
        is_error: bool,
    },
    ResponseInterrupted,
    Usage {
        input_tokens: u64,
        output_tokens: u64,
    },
    Closed {
        reason: String,
    },
    Error {
        message: String,
    },
    RawProviderEvent {
        provider: String,
        event: Value,
    },
}

/// Serializable state machine. The transport lives in provider adapters; this core enforces
/// ordering and dedupe before events reach tools, transcripts, or durable storage.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RealtimeSession {
    pub session_id: String,
    pub provider: String,
    pub model: String,
    pub state: RealtimeSessionState,
    pub voice_activity: VoiceActivityPolicy,
    pub last_sequence: u64,
    /// Persisted event fingerprints make reconnect/restart dedupe deterministic. The bound avoids
    /// turning a long-lived voice connection into an unbounded in-memory set.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    seen_events: BTreeMap<String, String>,
}

impl RealtimeSession {
    pub fn new(
        session_id: impl Into<String>,
        provider: impl Into<String>,
        model: impl Into<String>,
        voice_activity: VoiceActivityPolicy,
    ) -> std::result::Result<Self, String> {
        let session_id = session_id.into();
        let provider = provider.into();
        let model = model.into();
        if session_id.trim().is_empty() || provider.trim().is_empty() || model.trim().is_empty() {
            return Err("session_id, provider, and model must not be empty".into());
        }
        voice_activity.validate()?;
        Ok(Self {
            session_id,
            provider,
            model,
            state: RealtimeSessionState::Connecting,
            voice_activity,
            last_sequence: 0,
            seen_events: BTreeMap::new(),
        })
    }

    /// Apply exactly one new event. Duplicate IDs are ignored idempotently; reused or skipped
    /// sequence numbers fail closed so reconnect code must explicitly replay the missing range.
    pub fn apply(&mut self, event: &RealtimeEvent) -> std::result::Result<bool, String> {
        let event_value = serde_json::to_value(event)
            .map_err(|error| format!("cannot fingerprint realtime event: {error}"))?;
        let event_hash = crate::durability::stable_input_hash(&event_value);
        if let Some(previous_hash) = self.seen_events.get(&event.event_id) {
            if previous_hash == &event_hash {
                return Ok(false);
            }
            return Err(format!(
                "realtime event id '{}' was reused with different content",
                event.event_id
            ));
        }
        if self.seen_events.len() >= MAX_REALTIME_DEDUPE_EVENTS {
            return Err(
                "realtime dedupe window is full; checkpoint and reconnect explicitly".into(),
            );
        }
        if matches!(
            self.state,
            RealtimeSessionState::Closed | RealtimeSessionState::Failed
        ) {
            return Err("cannot apply events after a terminal realtime state".into());
        }
        let expected = self.last_sequence.saturating_add(1);
        if event.sequence != expected {
            return Err(format!(
                "realtime event sequence mismatch: expected {expected}, got {}",
                event.sequence
            ));
        }

        self.state = match &event.kind {
            RealtimeEventKind::Connected => RealtimeSessionState::Active,
            RealtimeEventKind::ResponseInterrupted => RealtimeSessionState::Interrupted,
            RealtimeEventKind::Closed { .. } => RealtimeSessionState::Closed,
            RealtimeEventKind::Error { .. } => RealtimeSessionState::Failed,
            _ => self.state,
        };
        self.last_sequence = event.sequence;
        self.seen_events.insert(event.event_id.clone(), event_hash);
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(sequence: u64, kind: RealtimeEventKind) -> RealtimeEvent {
        RealtimeEvent {
            event_id: format!("event-{sequence}"),
            sequence,
            kind,
        }
    }

    #[test]
    fn realtime_session_dedupes_and_fails_on_gaps() {
        let mut session = RealtimeSession::new(
            "session-1",
            "openai",
            "realtime-model",
            VoiceActivityPolicy::default(),
        )
        .unwrap();
        let connected = event(1, RealtimeEventKind::Connected);
        assert_eq!(session.apply(&connected), Ok(true));
        assert_eq!(session.apply(&connected), Ok(false));
        assert_eq!(session.state, RealtimeSessionState::Active);
        assert!(session
            .apply(&event(
                3,
                RealtimeEventKind::TextDelta { text: "gap".into() }
            ))
            .is_err());
    }

    #[test]
    fn terminal_realtime_state_rejects_more_content() {
        let mut session = RealtimeSession::new(
            "session-2",
            "google",
            "live-model",
            VoiceActivityPolicy::default(),
        )
        .unwrap();
        session
            .apply(&event(
                1,
                RealtimeEventKind::Closed {
                    reason: "done".into(),
                },
            ))
            .unwrap();
        assert!(session
            .apply(&event(
                2,
                RealtimeEventKind::TextDelta {
                    text: "late".into(),
                },
            ))
            .is_err());
        assert!(session
            .apply(&event(2, RealtimeEventKind::Connected))
            .is_err());
        assert_eq!(session.state, RealtimeSessionState::Closed);
        assert_eq!(session.last_sequence, 1);
    }

    #[test]
    fn realtime_dedupe_survives_restart_and_rejects_id_reuse() {
        let mut session = RealtimeSession::new(
            "session-3",
            "xai",
            "voice-model",
            VoiceActivityPolicy::default(),
        )
        .unwrap();
        let connected = event(1, RealtimeEventKind::Connected);
        session.apply(&connected).unwrap();

        let encoded = serde_json::to_string(&session).unwrap();
        let mut restored: RealtimeSession = serde_json::from_str(&encoded).unwrap();
        assert_eq!(restored.apply(&connected), Ok(false));

        let conflicting = RealtimeEvent {
            event_id: connected.event_id,
            sequence: 1,
            kind: RealtimeEventKind::Error {
                message: "different".into(),
            },
        };
        assert!(restored.apply(&conflicting).is_err());
    }
}
