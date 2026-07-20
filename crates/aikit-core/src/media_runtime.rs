//! Capability-aware, provider-neutral multimodal execution contracts.
//!
//! This module separates model selection from provider transport. A request is routed only when
//! every required capability is explicitly `supported`; both `unknown` and `unsupported` fail
//! closed. Exact selections never move to another model or provider unless the caller opts into a
//! typed fallback policy.

use crate::cancellation::CancellationToken;
use crate::catalog::ModelCatalogSnapshot;
use crate::contract::{CapabilityState, MediaInput};
use crate::multimodal::{
    GeneratedAudio, GeneratedImage, ModalityRequirement, RealtimeEvent, RealtimeSession,
    Transcript, VoiceActivityPolicy,
};
use crate::routing::{ModelCapability, ModelProfile};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeSet;
use thiserror::Error;

pub type MediaRuntimeResult<T> = std::result::Result<T, MediaRuntimeError>;

/// An exact provider/model pair. Provider adapters receive this only after routing succeeds.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MediaTarget {
    pub provider: String,
    pub model: String,
}

impl MediaTarget {
    pub fn new(provider: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            provider: provider.into(),
            model: model.into(),
        }
    }

    fn validate(&self) -> MediaRuntimeResult<()> {
        if self.provider.trim().is_empty() {
            return Err(MediaRuntimeError::InvalidRequest {
                field: "target.provider".into(),
                reason: "provider must not be empty".into(),
            });
        }
        if self.model.trim().is_empty() {
            return Err(MediaRuntimeError::InvalidRequest {
                field: "target.model".into(),
                reason: "model must not be empty".into(),
            });
        }
        Ok(())
    }
}

/// Fallback is disabled by default. Each enabled value is an explicit user-policy decision.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MediaFallbackPolicy {
    #[default]
    Disabled,
    SameProvider,
    AnyProvider,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum MediaModelSelection {
    Exact {
        target: MediaTarget,
    },
    /// Automatic selection is itself explicit. `provider_preference` is evaluated in order.
    Automatic {
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        provider_preference: Vec<String>,
    },
}

/// Routing facts for a single multimodal operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MediaRouteRequest {
    pub selection: MediaModelSelection,
    pub requirements: BTreeSet<ModalityRequirement>,
    /// Only providers with a currently usable credential/activation belong here.
    pub active_providers: BTreeSet<String>,
    #[serde(default)]
    pub fallback: MediaFallbackPolicy,
}

impl MediaRouteRequest {
    pub fn exact(
        target: MediaTarget,
        requirement: ModalityRequirement,
        active_providers: impl IntoIterator<Item = String>,
    ) -> Self {
        Self {
            selection: MediaModelSelection::Exact { target },
            requirements: [requirement].into_iter().collect(),
            active_providers: active_providers.into_iter().collect(),
            fallback: MediaFallbackPolicy::Disabled,
        }
    }

    pub fn automatic(
        requirement: ModalityRequirement,
        active_providers: impl IntoIterator<Item = String>,
    ) -> Self {
        Self {
            selection: MediaModelSelection::Automatic {
                provider_preference: Vec::new(),
            },
            requirements: [requirement].into_iter().collect(),
            active_providers: active_providers.into_iter().collect(),
            fallback: MediaFallbackPolicy::Disabled,
        }
    }

    fn validate(&self) -> MediaRuntimeResult<()> {
        if self.requirements.is_empty() {
            return Err(MediaRuntimeError::InvalidRequest {
                field: "requirements".into(),
                reason: "at least one modality requirement is required".into(),
            });
        }
        if self.active_providers.is_empty()
            || self
                .active_providers
                .iter()
                .any(|provider| provider.trim().is_empty())
        {
            return Err(MediaRuntimeError::InvalidRequest {
                field: "active_providers".into(),
                reason: "at least one non-empty active provider is required".into(),
            });
        }
        match &self.selection {
            MediaModelSelection::Exact { target } => target.validate(),
            MediaModelSelection::Automatic {
                provider_preference,
            } => {
                if provider_preference
                    .iter()
                    .any(|provider| provider.trim().is_empty())
                {
                    return Err(MediaRuntimeError::InvalidRequest {
                        field: "selection.provider_preference".into(),
                        reason: "provider preference must not contain an empty value".into(),
                    });
                }
                Ok(())
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MediaAlternative {
    pub target: MediaTarget,
    pub quality_score: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MediaCapabilityEvidence {
    pub target: MediaTarget,
    pub requirement: ModalityRequirement,
    pub state: CapabilityState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MediaRouteDecision {
    pub target: MediaTarget,
    pub requirements: BTreeSet<ModalityRequirement>,
    pub used_fallback: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_target: Option<MediaTarget>,
    /// Present when routing used a versioned [`ModelCatalogSnapshot`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog_version: Option<String>,
}

/// Safe, typed routing and transport failures. Provider bodies and credentials do not belong in
/// these variants.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MediaRuntimeError {
    #[error("invalid media request field `{field}`: {reason}")]
    InvalidRequest { field: String, reason: String },
    #[error("model `{target:?}` is not present in the catalog")]
    ModelNotFound {
        target: MediaTarget,
        alternatives: Vec<MediaAlternative>,
    },
    #[error("provider `{provider}` is not active")]
    ProviderInactive { provider: String },
    #[error("model `{target:?}` has {state:?} support for {requirement:?}")]
    CapabilityUnavailable {
        target: MediaTarget,
        requirement: ModalityRequirement,
        state: CapabilityState,
        alternatives: Vec<MediaAlternative>,
    },
    #[error("no active model explicitly supports every required modality")]
    NoCapableModel {
        requirements: BTreeSet<ModalityRequirement>,
        evidence: Vec<MediaCapabilityEvidence>,
    },
    #[error("multimodal operation was cancelled")]
    Cancelled,
    #[error("provider `{provider}` failed for model `{model}` ({code})")]
    ProviderFailure {
        provider: String,
        model: String,
        code: String,
        retryable: bool,
    },
    #[error("provider `{provider}` violated the multimodal protocol: {reason}")]
    Protocol { provider: String, reason: String },
}

/// Pure router over a reviewed offline snapshot or an equivalent caller-owned profile slice.
#[derive(Debug, Clone, Copy)]
pub struct MediaRouter<'a> {
    profiles: &'a [ModelProfile],
    catalog_version: Option<&'a str>,
}

impl<'a> MediaRouter<'a> {
    pub fn from_snapshot(snapshot: &'a ModelCatalogSnapshot) -> Self {
        Self {
            profiles: &snapshot.profiles,
            catalog_version: Some(&snapshot.catalog_version),
        }
    }

    pub fn from_profiles(profiles: &'a [ModelProfile]) -> Self {
        Self {
            profiles,
            catalog_version: None,
        }
    }

    pub fn route(&self, request: &MediaRouteRequest) -> MediaRuntimeResult<MediaRouteDecision> {
        request.validate()?;
        let mut supported = self.supported_candidates(request);

        match &request.selection {
            MediaModelSelection::Automatic {
                provider_preference,
            } => {
                sort_candidates(&mut supported, provider_preference);
                let Some(profile) = supported.first().copied() else {
                    return Err(MediaRuntimeError::NoCapableModel {
                        requirements: request.requirements.clone(),
                        evidence: self.capability_evidence(request),
                    });
                };
                Ok(decision(
                    profile,
                    request,
                    false,
                    None,
                    self.catalog_version,
                ))
            }
            MediaModelSelection::Exact { target } => {
                let Some(requested) = self.profiles.iter().find(|profile| {
                    profile.provider == target.provider && profile.model == target.model
                }) else {
                    sort_candidates(&mut supported, &[]);
                    return Err(MediaRuntimeError::ModelNotFound {
                        target: target.clone(),
                        alternatives: alternatives(&supported),
                    });
                };

                if !request.active_providers.contains(&requested.provider) {
                    return Err(MediaRuntimeError::ProviderInactive {
                        provider: requested.provider.clone(),
                    });
                }

                if first_unavailable(requested, &request.requirements).is_none() {
                    return Ok(decision(
                        requested,
                        request,
                        false,
                        Some(target.clone()),
                        self.catalog_version,
                    ));
                }

                // Fallback selection must be stable even when caller-owned profiles arrive in a
                // different order.
                sort_candidates(&mut supported, &[]);
                let eligible_fallbacks: Vec<&ModelProfile> = supported
                    .iter()
                    .copied()
                    .filter(|profile| profile.model != requested.model)
                    .filter(|profile| match request.fallback {
                        MediaFallbackPolicy::Disabled => false,
                        MediaFallbackPolicy::SameProvider => profile.provider == requested.provider,
                        MediaFallbackPolicy::AnyProvider => true,
                    })
                    .collect();

                if let Some(profile) = eligible_fallbacks.first().copied() {
                    return Ok(decision(
                        profile,
                        request,
                        true,
                        Some(target.clone()),
                        self.catalog_version,
                    ));
                }

                let (requirement, state) = first_unavailable(requested, &request.requirements)
                    .expect("unsupported exact model was checked above");
                sort_candidates(&mut supported, &[]);
                Err(MediaRuntimeError::CapabilityUnavailable {
                    target: target.clone(),
                    requirement,
                    state,
                    alternatives: alternatives(&supported),
                })
            }
        }
    }

    fn supported_candidates(&self, request: &MediaRouteRequest) -> Vec<&'a ModelProfile> {
        self.profiles
            .iter()
            .filter(|profile| request.active_providers.contains(&profile.provider))
            .filter(|profile| first_unavailable(profile, &request.requirements).is_none())
            .collect()
    }

    fn capability_evidence(&self, request: &MediaRouteRequest) -> Vec<MediaCapabilityEvidence> {
        let mut evidence = Vec::new();
        for profile in self
            .profiles
            .iter()
            .filter(|profile| request.active_providers.contains(&profile.provider))
        {
            for requirement in &request.requirements {
                let state = capability_state(profile, *requirement);
                if state != CapabilityState::Supported {
                    evidence.push(MediaCapabilityEvidence {
                        target: MediaTarget::new(&profile.provider, &profile.model),
                        requirement: *requirement,
                        state,
                    });
                }
            }
        }
        evidence.sort_by(|left, right| {
            left.target
                .cmp(&right.target)
                .then_with(|| left.requirement.cmp(&right.requirement))
        });
        evidence
    }
}

fn model_capability(requirement: ModalityRequirement) -> Option<ModelCapability> {
    match requirement {
        // Every profile in this catalog denotes a text generation model. Text is the base
        // contract, not an optional capability bit.
        ModalityRequirement::Text => None,
        ModalityRequirement::Reasoning => Some(ModelCapability::Reasoning),
        ModalityRequirement::ImageInput => Some(ModelCapability::Vision),
        ModalityRequirement::ImageGeneration => Some(ModelCapability::ImageGeneration),
        ModalityRequirement::DocumentInput => Some(ModelCapability::DocumentInput),
        ModalityRequirement::AudioInput => Some(ModelCapability::AudioInput),
        ModalityRequirement::Transcription => Some(ModelCapability::Transcription),
        ModalityRequirement::SpeechGeneration => Some(ModelCapability::SpeechGeneration),
        ModalityRequirement::RealtimeDuplex => Some(ModelCapability::RealtimeDuplex),
        ModalityRequirement::ToolUse => Some(ModelCapability::ToolUse),
        ModalityRequirement::StructuredOutput => Some(ModelCapability::NativeStructuredOutput),
    }
}

fn capability_state(profile: &ModelProfile, requirement: ModalityRequirement) -> CapabilityState {
    model_capability(requirement)
        .map(|capability| profile.capability_state(&capability))
        .unwrap_or(CapabilityState::Supported)
}

fn first_unavailable(
    profile: &ModelProfile,
    requirements: &BTreeSet<ModalityRequirement>,
) -> Option<(ModalityRequirement, CapabilityState)> {
    requirements.iter().find_map(|requirement| {
        let state = capability_state(profile, *requirement);
        (state != CapabilityState::Supported).then_some((*requirement, state))
    })
}

fn sort_candidates(candidates: &mut Vec<&ModelProfile>, provider_preference: &[String]) {
    candidates.sort_by(|left, right| {
        let provider_rank = |provider: &str| {
            provider_preference
                .iter()
                .position(|preferred| preferred == provider)
                .unwrap_or(usize::MAX)
        };
        provider_rank(&left.provider)
            .cmp(&provider_rank(&right.provider))
            .then_with(|| right.quality_score.cmp(&left.quality_score))
            .then_with(|| left.provider.cmp(&right.provider))
            .then_with(|| left.model.cmp(&right.model))
    });
}

fn alternatives(candidates: &[&ModelProfile]) -> Vec<MediaAlternative> {
    candidates
        .iter()
        .map(|profile| MediaAlternative {
            target: MediaTarget::new(&profile.provider, &profile.model),
            quality_score: profile.quality_score,
        })
        .collect()
}

fn decision(
    profile: &ModelProfile,
    request: &MediaRouteRequest,
    used_fallback: bool,
    requested_target: Option<MediaTarget>,
    catalog_version: Option<&str>,
) -> MediaRouteDecision {
    MediaRouteDecision {
        target: MediaTarget::new(&profile.provider, &profile.model),
        requirements: request.requirements.clone(),
        used_fallback,
        requested_target,
        catalog_version: catalog_version.map(str::to_owned),
    }
}

/// Correlation and routing context common to all multimodal provider calls.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MediaRequestContext {
    pub request_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<String>,
    pub target: MediaTarget,
    pub requirements: BTreeSet<ModalityRequirement>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog_version: Option<String>,
    /// Raw vendor events may contain user content and are never returned without explicit opt-in.
    #[serde(default)]
    pub include_raw_provider_events: bool,
}

impl MediaRequestContext {
    pub fn from_route(request_id: impl Into<String>, route: &MediaRouteDecision) -> Self {
        Self {
            request_id: request_id.into(),
            correlation_id: None,
            target: route.target.clone(),
            requirements: route.requirements.clone(),
            catalog_version: route.catalog_version.clone(),
            include_raw_provider_events: false,
        }
    }

    fn validate_for(&self, requirement: ModalityRequirement) -> MediaRuntimeResult<()> {
        if self.request_id.trim().is_empty() {
            return Err(MediaRuntimeError::InvalidRequest {
                field: "context.request_id".into(),
                reason: "request_id must not be empty".into(),
            });
        }
        if self
            .correlation_id
            .as_ref()
            .is_some_and(|value| value.trim().is_empty())
        {
            return Err(MediaRuntimeError::InvalidRequest {
                field: "context.correlation_id".into(),
                reason: "correlation_id must not be empty when present".into(),
            });
        }
        self.target.validate()?;
        if !self.requirements.contains(&requirement) {
            return Err(MediaRuntimeError::InvalidRequest {
                field: "context.requirements".into(),
                reason: format!("routed context does not attest {requirement:?}"),
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MediaUsage {
    #[serde(default)]
    pub input_tokens: u64,
    #[serde(default)]
    pub output_tokens: u64,
    #[serde(default)]
    pub input_audio_ms: u64,
    #[serde(default)]
    pub output_audio_ms: u64,
    #[serde(default)]
    pub generated_images: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ImageDimensions {
    pub width: u32,
    pub height: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ImageGenerationRequest {
    pub context: MediaRequestContext,
    pub prompt: String,
    #[serde(default = "default_image_count")]
    pub count: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dimensions: Option<ImageDimensions>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_media_type: Option<String>,
    #[serde(default)]
    pub provider_options: Value,
}

fn default_image_count() -> u32 {
    1
}

impl ImageGenerationRequest {
    pub fn validate(&self) -> MediaRuntimeResult<()> {
        self.context
            .validate_for(ModalityRequirement::ImageGeneration)?;
        if self.prompt.trim().is_empty() {
            return Err(invalid("prompt", "prompt must not be empty"));
        }
        if self.count == 0 {
            return Err(invalid("count", "count must be greater than zero"));
        }
        if self
            .dimensions
            .is_some_and(|dimensions| dimensions.width == 0 || dimensions.height == 0)
        {
            return Err(invalid(
                "dimensions",
                "width and height must be greater than zero",
            ));
        }
        if self
            .output_media_type
            .as_ref()
            .is_some_and(|media_type| !media_type.starts_with("image/"))
        {
            return Err(invalid(
                "output_media_type",
                "image generation output must use an image MIME type",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ImageGenerationResponse {
    pub request_id: String,
    pub target: MediaTarget,
    pub images: Vec<GeneratedImage>,
    #[serde(default)]
    pub usage: MediaUsage,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_metadata: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TranscriptionRequest {
    pub context: MediaRequestContext,
    pub audio: MediaInput,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub language: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt: Option<String>,
    #[serde(default)]
    pub include_segments: bool,
    #[serde(default)]
    pub provider_options: Value,
}

impl TranscriptionRequest {
    pub fn validate(&self) -> MediaRuntimeResult<()> {
        self.context
            .validate_for(ModalityRequirement::Transcription)?;
        self.audio
            .validate()
            .map_err(|reason| invalid("audio", &reason))?;
        if !self.audio.media_type.starts_with("audio/") {
            return Err(invalid("audio.media_type", "input must be audio media"));
        }
        if self
            .language
            .as_ref()
            .is_some_and(|language| language.trim().is_empty())
        {
            return Err(invalid(
                "language",
                "language must not be empty when present",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TranscriptionResponse {
    pub request_id: String,
    pub target: MediaTarget,
    pub transcript: Transcript,
    #[serde(default)]
    pub usage: MediaUsage,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_metadata: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpeechGenerationRequest {
    pub context: MediaRequestContext,
    pub text: String,
    pub voice: String,
    pub output_media_type: String,
    #[serde(default = "default_speech_speed")]
    pub speed: f32,
    #[serde(default)]
    pub provider_options: Value,
}

fn default_speech_speed() -> f32 {
    1.0
}

impl SpeechGenerationRequest {
    pub fn validate(&self) -> MediaRuntimeResult<()> {
        self.context
            .validate_for(ModalityRequirement::SpeechGeneration)?;
        if self.text.trim().is_empty() {
            return Err(invalid("text", "speech text must not be empty"));
        }
        if self.voice.trim().is_empty() {
            return Err(invalid("voice", "voice must not be empty"));
        }
        if !self.output_media_type.starts_with("audio/") {
            return Err(invalid(
                "output_media_type",
                "speech output must use an audio MIME type",
            ));
        }
        if !self.speed.is_finite() || self.speed <= 0.0 {
            return Err(invalid("speed", "speed must be finite and positive"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpeechGenerationResponse {
    pub request_id: String,
    pub target: MediaTarget,
    pub audio: GeneratedAudio,
    #[serde(default)]
    pub usage: MediaUsage,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_metadata: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RealtimeConnectRequest {
    pub context: MediaRequestContext,
    pub session_id: String,
    pub modalities: BTreeSet<ModalityRequirement>,
    #[serde(default)]
    pub voice_activity: VoiceActivityPolicy,
    #[serde(default)]
    pub provider_options: Value,
}

impl RealtimeConnectRequest {
    pub fn validate(&self) -> MediaRuntimeResult<()> {
        self.context
            .validate_for(ModalityRequirement::RealtimeDuplex)?;
        if self.session_id.trim().is_empty() {
            return Err(invalid("session_id", "session_id must not be empty"));
        }
        if self.modalities.is_empty() {
            return Err(invalid(
                "modalities",
                "at least one realtime modality is required",
            ));
        }
        self.voice_activity
            .validate()
            .map_err(|reason| invalid("voice_activity", &reason))
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RealtimeCommand {
    pub command_id: String,
    #[serde(flatten)]
    pub kind: RealtimeCommandKind,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RealtimeCommandKind {
    AppendAudio {
        audio: MediaInput,
    },
    AppendText {
        text: String,
    },
    CommitInput,
    ToolResult {
        call_id: String,
        output: Value,
        is_error: bool,
    },
    Interrupt,
    Close {
        reason: String,
    },
}

/// Validate cancellation before starting provider I/O. Provider implementations must also race
/// their in-flight transport against `CancellationToken::cancelled()`.
pub fn ensure_media_not_cancelled(cancellation: &CancellationToken) -> MediaRuntimeResult<()> {
    if cancellation.is_cancelled() {
        Err(MediaRuntimeError::Cancelled)
    } else {
        Ok(())
    }
}

#[async_trait]
pub trait ImageGenerationProvider: Send + Sync {
    fn provider_name(&self) -> &str;

    async fn generate_image(
        &self,
        request: ImageGenerationRequest,
        cancellation: CancellationToken,
    ) -> MediaRuntimeResult<ImageGenerationResponse>;
}

#[async_trait]
pub trait TranscriptionProvider: Send + Sync {
    fn provider_name(&self) -> &str;

    async fn transcribe(
        &self,
        request: TranscriptionRequest,
        cancellation: CancellationToken,
    ) -> MediaRuntimeResult<TranscriptionResponse>;
}

#[async_trait]
pub trait SpeechGenerationProvider: Send + Sync {
    fn provider_name(&self) -> &str;

    async fn generate_speech(
        &self,
        request: SpeechGenerationRequest,
        cancellation: CancellationToken,
    ) -> MediaRuntimeResult<SpeechGenerationResponse>;
}

/// A duplex connection owns provider transport state. Commands and events remain typed, ordered,
/// cancellable, and independent from any particular websocket/WebRTC implementation.
#[async_trait]
pub trait RealtimeConnection: Send {
    fn session(&self) -> &RealtimeSession;

    async fn send(
        &mut self,
        command: RealtimeCommand,
        cancellation: CancellationToken,
    ) -> MediaRuntimeResult<()>;

    async fn next_event(
        &mut self,
        cancellation: CancellationToken,
    ) -> MediaRuntimeResult<Option<RealtimeEvent>>;
}

#[async_trait]
pub trait RealtimeProvider: Send + Sync {
    fn provider_name(&self) -> &str;

    async fn connect_realtime(
        &self,
        request: RealtimeConnectRequest,
        cancellation: CancellationToken,
    ) -> MediaRuntimeResult<Box<dyn RealtimeConnection>>;
}

fn invalid(field: &str, reason: &str) -> MediaRuntimeError {
    MediaRuntimeError::InvalidRequest {
        field: field.into(),
        reason: reason.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn profile(
        provider: &str,
        model: &str,
        capability: ModelCapability,
        state: CapabilityState,
        quality: u8,
    ) -> ModelProfile {
        ModelProfile::new(provider, model, 100_000, 10_000, quality)
            .with_capability_state(capability, state)
    }

    fn active(providers: &[&str]) -> Vec<String> {
        providers
            .iter()
            .map(|provider| (*provider).into())
            .collect()
    }

    #[test]
    fn unknown_and_unsupported_are_distinct_fail_closed_results() {
        let profiles = vec![
            profile(
                "unknown-provider",
                "unknown-model",
                ModelCapability::ImageGeneration,
                CapabilityState::Unknown,
                90,
            ),
            profile(
                "unsupported-provider",
                "unsupported-model",
                ModelCapability::ImageGeneration,
                CapabilityState::Unsupported,
                80,
            ),
        ];
        let router = MediaRouter::from_profiles(&profiles);

        for (provider, model, expected) in [
            (
                "unknown-provider",
                "unknown-model",
                CapabilityState::Unknown,
            ),
            (
                "unsupported-provider",
                "unsupported-model",
                CapabilityState::Unsupported,
            ),
        ] {
            let request = MediaRouteRequest::exact(
                MediaTarget::new(provider, model),
                ModalityRequirement::ImageGeneration,
                active(&[provider]),
            );
            let MediaRuntimeError::CapabilityUnavailable { state, .. } =
                router.route(&request).unwrap_err()
            else {
                panic!("unexpected routing error")
            };
            assert_eq!(state, expected);
        }
    }

    #[test]
    fn exact_model_fallback_requires_explicit_opt_in() {
        let profiles = vec![
            profile(
                "provider-a",
                "requested",
                ModelCapability::SpeechGeneration,
                CapabilityState::Unsupported,
                100,
            ),
            profile(
                "provider-b",
                "supported",
                ModelCapability::SpeechGeneration,
                CapabilityState::Supported,
                70,
            ),
        ];
        let router = MediaRouter::from_profiles(&profiles);
        let mut request = MediaRouteRequest::exact(
            MediaTarget::new("provider-a", "requested"),
            ModalityRequirement::SpeechGeneration,
            active(&["provider-a", "provider-b"]),
        );

        let MediaRuntimeError::CapabilityUnavailable { alternatives, .. } =
            router.route(&request).unwrap_err()
        else {
            panic!("fallback must be disabled by default")
        };
        assert_eq!(alternatives[0].target.provider, "provider-b");

        request.fallback = MediaFallbackPolicy::AnyProvider;
        let decision = router.route(&request).unwrap();
        assert_eq!(decision.target, MediaTarget::new("provider-b", "supported"));
        assert!(decision.used_fallback);
        assert_eq!(
            decision.requested_target,
            Some(MediaTarget::new("provider-a", "requested"))
        );
    }

    #[test]
    fn automatic_routing_requires_every_requested_modality() {
        let profiles = vec![
            profile(
                "audio-only",
                "transcriber",
                ModelCapability::Transcription,
                CapabilityState::Supported,
                100,
            ),
            profile(
                "duplex",
                "voice-agent",
                ModelCapability::Transcription,
                CapabilityState::Supported,
                80,
            )
            .with_capability_state(ModelCapability::RealtimeDuplex, CapabilityState::Supported),
        ];
        let router = MediaRouter::from_profiles(&profiles);
        let mut request = MediaRouteRequest::automatic(
            ModalityRequirement::Transcription,
            active(&["audio-only", "duplex"]),
        );
        request
            .requirements
            .insert(ModalityRequirement::RealtimeDuplex);

        let decision = router.route(&request).unwrap();
        assert_eq!(decision.target, MediaTarget::new("duplex", "voice-agent"));
        assert!(!decision.used_fallback);
    }

    #[test]
    fn automatic_provider_preference_is_deterministic() {
        let profiles = vec![
            profile(
                "alpha",
                "alpha-speech",
                ModelCapability::SpeechGeneration,
                CapabilityState::Supported,
                100,
            ),
            profile(
                "beta",
                "beta-speech",
                ModelCapability::SpeechGeneration,
                CapabilityState::Supported,
                10,
            ),
        ];
        let router = MediaRouter::from_profiles(&profiles);
        let mut request = MediaRouteRequest::automatic(
            ModalityRequirement::SpeechGeneration,
            active(&["alpha", "beta"]),
        );
        request.selection = MediaModelSelection::Automatic {
            provider_preference: vec!["beta".into(), "alpha".into()],
        };
        assert_eq!(
            router.route(&request).unwrap().target,
            MediaTarget::new("beta", "beta-speech")
        );
    }

    #[test]
    fn cancellation_is_typed_and_monotonic() {
        let token = CancellationToken::new();
        ensure_media_not_cancelled(&token).unwrap();
        token.cancel();
        assert_eq!(
            ensure_media_not_cancelled(&token),
            Err(MediaRuntimeError::Cancelled)
        );
    }
}
