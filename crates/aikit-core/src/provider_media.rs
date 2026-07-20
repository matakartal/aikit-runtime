//! Provider-native HTTP transports for capability-gated multimodal operations.
//!
//! The shipped model catalog remains the source of truth: `unknown` and `unsupported` both stop
//! before I/O. This module intentionally implements only OpenAI's HTTP image, transcription,
//! speech, and WebRTC-call contracts. Other providers return a typed unsupported result; there is
//! no implicit compatible-provider fallback.

use crate::cancellation::CancellationToken;
use crate::catalog::ModelCatalogSnapshot;
use crate::contract::{CapabilityState, MediaInputSource};
use crate::media_runtime::{
    ImageGenerationRequest, ImageGenerationResponse, MediaUsage, RealtimeConnectRequest,
    SpeechGenerationRequest, SpeechGenerationResponse, TranscriptionRequest, TranscriptionResponse,
};
use crate::multimodal::{
    GeneratedAudio, GeneratedImage, MediaArtifact, ModalityRequirement, RealtimeEvent,
    RealtimeEventKind, RealtimeSession, Transcript, TranscriptSegment,
};
use crate::routing::ModelCapability;
use async_trait::async_trait;
use futures::StreamExt;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, LOCATION, RETRY_AFTER};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;
use std::sync::Arc;
use thiserror::Error;

pub type ProviderMediaResult<T> = std::result::Result<T, ProviderMediaError>;

const MAX_PROVIDER_MEDIA_RESPONSE_BYTES: usize = 64 * 1024 * 1024;
const MAX_INLINE_TRANSCRIPTION_BYTES: usize = 25 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProviderMediaError {
    #[error("invalid provider media request field `{field}`: {reason}")]
    InvalidRequest { field: String, reason: String },
    #[error("{provider}/{model} cannot execute {modality:?} ({state:?})")]
    UnsupportedCapability {
        provider: String,
        model: String,
        modality: ModalityRequirement,
        state: CapabilityState,
    },
    #[error("provider parameter `{parameter}` is unsupported for {provider}")]
    UnsupportedParameter { provider: String, parameter: String },
    #[error("provider media operation was cancelled")]
    Cancelled,
    #[error("{provider} HTTP transport failed ({code})")]
    Http {
        provider: String,
        code: String,
        status: Option<u16>,
        retry_after_ms: Option<u64>,
        retryable: bool,
    },
    #[error("{provider} returned an invalid media response: {reason}")]
    Protocol { provider: String, reason: String },
    #[error("media artifact lifecycle failed during {stage}")]
    Artifact { stage: String },
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpenAiMediaConfig {
    pub base_url: String,
    #[serde(skip_serializing)]
    api_key: String,
}

impl OpenAiMediaConfig {
    pub fn new(api_key: impl Into<String>) -> ProviderMediaResult<Self> {
        Self::with_base_url(api_key, "https://api.openai.com")
    }

    pub fn with_base_url(
        api_key: impl Into<String>,
        base_url: impl Into<String>,
    ) -> ProviderMediaResult<Self> {
        let api_key = api_key.into();
        let base_url = base_url.into();
        if api_key.trim().is_empty() {
            return Err(invalid("api_key", "API key must not be empty"));
        }
        let parsed = url::Url::parse(&base_url)
            .map_err(|_| invalid("base_url", "base URL must be an absolute HTTP(S) URL"))?;
        if !matches!(parsed.scheme(), "http" | "https") {
            return Err(invalid("base_url", "base URL must use HTTP(S)"));
        }
        Ok(Self { base_url, api_key })
    }

    fn endpoint(&self, path: &str) -> String {
        format!("{}{}", self.base_url.trim_end_matches('/'), path)
    }
}

impl fmt::Debug for OpenAiMediaConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpenAiMediaConfig")
            .field("base_url", &self.base_url)
            .field("api_key", &"[REDACTED]")
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArtifactPayload {
    Bytes(Vec<u8>),
    Base64(String),
    Url(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactDraft {
    pub request_id: String,
    pub provider: String,
    pub model: String,
    pub media_type: String,
    pub payload: ArtifactPayload,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagedMediaArtifact {
    pub staging_id: String,
    pub artifact: MediaArtifact,
}

/// Host-owned durable storage boundary.
///
/// `stage` must be cancellation-safe. `commit` must persist and return the exact artifact exposed
/// by `stage`. `abort` is idempotent and must remove that artifact whether it is still staged or a
/// completed `commit` needs to be compensated.
#[async_trait]
pub trait MediaArtifactStore: Send + Sync {
    async fn stage(&self, draft: ArtifactDraft) -> ProviderMediaResult<StagedMediaArtifact>;
    async fn commit(&self, staged: &StagedMediaArtifact) -> ProviderMediaResult<MediaArtifact>;
    async fn abort(&self, staged: &StagedMediaArtifact) -> ProviderMediaResult<()>;
}

#[derive(Clone)]
pub struct OpenAiMediaTransport {
    config: OpenAiMediaConfig,
    catalog: Arc<ModelCatalogSnapshot>,
    artifacts: Arc<dyn MediaArtifactStore>,
    client: reqwest::Client,
}

impl fmt::Debug for OpenAiMediaTransport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpenAiMediaTransport")
            .field("config", &self.config)
            .field("catalog_version", &self.catalog.catalog_version)
            .finish_non_exhaustive()
    }
}

impl OpenAiMediaTransport {
    pub fn new(
        config: OpenAiMediaConfig,
        catalog: Arc<ModelCatalogSnapshot>,
        artifacts: Arc<dyn MediaArtifactStore>,
    ) -> Self {
        Self {
            config,
            catalog,
            artifacts,
            client: reqwest::Client::new(),
        }
    }

    pub async fn generate_image(
        &self,
        request: ImageGenerationRequest,
        cancellation: CancellationToken,
    ) -> ProviderMediaResult<ImageGenerationResponse> {
        request.validate().map_err(runtime_validation)?;
        self.gate(
            &request.context.target.provider,
            &request.context.target.model,
            ModalityRequirement::ImageGeneration,
        )?;
        if request.count != 1 {
            return Err(ProviderMediaError::UnsupportedParameter {
                provider: "openai".into(),
                parameter: "count>1".into(),
            });
        }
        let model = wire_model(&request.context.target.model);
        let mut body = serde_json::json!({
            "model": model,
            "prompt": request.prompt,
            "n": 1
        });
        if let Some(dimensions) = request.dimensions {
            body["size"] = Value::String(format!("{}x{}", dimensions.width, dimensions.height));
        }
        if let Some(media_type) = &request.output_media_type {
            body["output_format"] = Value::String(image_format(media_type)?.into());
        }
        merge_options(
            "openai",
            &mut body,
            &request.provider_options,
            &["model", "prompt", "n", "size", "output_format"],
        )?;
        let bytes = self
            .send_bytes(
                self.authorized(
                    self.client
                        .post(self.config.endpoint("/v1/images/generations")),
                )
                .json(&body),
                cancellation.clone(),
            )
            .await?;
        let response: Value = serde_json::from_slice(&bytes)
            .map_err(|_| protocol("openai", "image response was not JSON"))?;
        let item = response
            .get("data")
            .and_then(Value::as_array)
            .and_then(|items| (items.len() == 1).then(|| &items[0]))
            .ok_or_else(|| {
                protocol("openai", "image response must contain exactly one artifact")
            })?;
        let (payload, media_type) = image_payload(item, request.output_media_type.as_deref())?;
        let artifact = self
            .persist(
                ArtifactDraft {
                    request_id: request.context.request_id.clone(),
                    provider: "openai".into(),
                    model: request.context.target.model.clone(),
                    media_type,
                    payload,
                },
                cancellation,
            )
            .await?;
        Ok(ImageGenerationResponse {
            request_id: request.context.request_id,
            target: request.context.target,
            images: vec![GeneratedImage {
                artifact,
                revised_prompt: item
                    .get("revised_prompt")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                provider_metadata: None,
            }],
            usage: image_usage(&response),
            provider_metadata: None,
        })
    }

    pub async fn transcribe(
        &self,
        request: TranscriptionRequest,
        cancellation: CancellationToken,
    ) -> ProviderMediaResult<TranscriptionResponse> {
        request.validate().map_err(runtime_validation)?;
        self.gate(
            &request.context.target.provider,
            &request.context.target.model,
            ModalityRequirement::Transcription,
        )?;
        reject_nonempty_options("openai", &request.provider_options)?;
        let inline_audio = match &request.audio.source {
            MediaInputSource::Bytes { data } => data,
            _ => {
                return Err(ProviderMediaError::UnsupportedParameter {
                    provider: "openai".into(),
                    parameter: "non-inline transcription source".into(),
                })
            }
        };
        validate_inline_transcription(
            inline_audio,
            request.audio.size_bytes,
            &request.audio.sha256,
        )?;
        validate_multipart_media_type(&request.audio.media_type)?;
        let boundary = multipart_boundary()?;
        let mut fields = vec![(
            "model",
            wire_model(&request.context.target.model).to_owned(),
        )];
        fields.push((
            "response_format",
            if request.include_segments {
                "verbose_json"
            } else {
                "json"
            }
            .into(),
        ));
        if let Some(language) = &request.language {
            fields.push(("language", language.clone()));
        }
        if let Some(prompt) = &request.prompt {
            fields.push(("prompt", prompt.clone()));
        }
        let audio = match request.audio.source {
            MediaInputSource::Bytes { data } => data,
            _ => unreachable!("inline source was checked before multipart construction"),
        };
        let body = multipart_stream_body(
            &boundary,
            &fields,
            "file",
            "audio.bin",
            &request.audio.media_type,
            audio,
        );
        let bytes = self
            .send_bytes(
                self.authorized(
                    self.client
                        .post(self.config.endpoint("/v1/audio/transcriptions")),
                )
                .header(
                    CONTENT_TYPE,
                    format!("multipart/form-data; boundary={boundary}"),
                )
                .body(body),
                cancellation,
            )
            .await?;
        let value: Value = serde_json::from_slice(&bytes)
            .map_err(|_| protocol("openai", "transcription response was not JSON"))?;
        let text = value
            .get("text")
            .and_then(Value::as_str)
            .ok_or_else(|| protocol("openai", "transcription response omitted text"))?;
        let segments = value
            .get("segments")
            .and_then(Value::as_array)
            .map(|items| items.iter().filter_map(transcript_segment).collect())
            .unwrap_or_default();
        Ok(TranscriptionResponse {
            request_id: request.context.request_id,
            target: request.context.target,
            transcript: Transcript {
                text: text.into(),
                language: value
                    .get("language")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                segments,
                provider_metadata: None,
            },
            usage: transcription_usage(&value),
            provider_metadata: None,
        })
    }

    pub async fn generate_speech(
        &self,
        request: SpeechGenerationRequest,
        cancellation: CancellationToken,
    ) -> ProviderMediaResult<SpeechGenerationResponse> {
        request.validate().map_err(runtime_validation)?;
        self.gate(
            &request.context.target.provider,
            &request.context.target.model,
            ModalityRequirement::SpeechGeneration,
        )?;
        let mut body = serde_json::json!({
            "model": wire_model(&request.context.target.model),
            "input": request.text,
            "voice": request.voice,
            "response_format": audio_format(&request.output_media_type)?,
            "speed": request.speed
        });
        merge_options(
            "openai",
            &mut body,
            &request.provider_options,
            &["model", "input", "voice", "response_format", "speed"],
        )?;
        let bytes = self
            .send_bytes(
                self.authorized(self.client.post(self.config.endpoint("/v1/audio/speech")))
                    .json(&body),
                cancellation.clone(),
            )
            .await?;
        let artifact = self
            .persist(
                ArtifactDraft {
                    request_id: request.context.request_id.clone(),
                    provider: "openai".into(),
                    model: request.context.target.model.clone(),
                    media_type: request.output_media_type.clone(),
                    payload: ArtifactPayload::Bytes(bytes.to_vec()),
                },
                cancellation,
            )
            .await?;
        Ok(SpeechGenerationResponse {
            request_id: request.context.request_id,
            target: request.context.target,
            audio: GeneratedAudio {
                artifact,
                duration_ms: None,
                voice: Some(request.voice),
                provider_metadata: None,
            },
            usage: MediaUsage::default(),
            provider_metadata: None,
        })
    }

    pub async fn create_realtime_call(
        &self,
        request: OpenAiRealtimeCallRequest,
        cancellation: CancellationToken,
    ) -> ProviderMediaResult<OpenAiRealtimeCallResponse> {
        request.connect.validate().map_err(runtime_validation)?;
        self.gate(
            &request.connect.context.target.provider,
            &request.connect.context.target.model,
            ModalityRequirement::RealtimeDuplex,
        )?;
        if request.sdp_offer.trim().is_empty() {
            return Err(invalid("sdp_offer", "SDP offer must not be empty"));
        }
        let mut session = serde_json::json!({
            "type": "realtime",
            "model": wire_model(&request.connect.context.target.model)
        });
        merge_options(
            "openai",
            &mut session,
            &request.connect.provider_options,
            &["type", "model"],
        )?;
        let boundary = multipart_boundary()?;
        let fields = vec![("sdp", request.sdp_offer), ("session", session.to_string())];
        let body = multipart_fields(&boundary, &fields);
        let response = self
            .send(
                self.authorized(self.client.post(self.config.endpoint("/v1/realtime/calls")))
                    .header(
                        CONTENT_TYPE,
                        format!("multipart/form-data; boundary={boundary}"),
                    )
                    .body(body),
                cancellation.clone(),
            )
            .await?;
        let call_id = response
            .headers()
            .get(LOCATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|location| location.trim_end_matches('/').rsplit('/').next())
            .filter(|id| !id.is_empty())
            .map(str::to_owned);
        let sdp_answer = self.response_bytes(response, cancellation).await?;
        let sdp_answer = String::from_utf8(sdp_answer.to_vec())
            .map_err(|_| protocol("openai", "realtime SDP answer was not UTF-8"))?;
        let mut canonical = RealtimeSession::new(
            &request.connect.session_id,
            "openai",
            &request.connect.context.target.model,
            request.connect.voice_activity,
        )
        .map_err(|reason| invalid("realtime_session", &reason))?;
        canonical
            .apply(&RealtimeEvent {
                event_id: format!("{}:connected", request.connect.context.request_id),
                sequence: 1,
                kind: RealtimeEventKind::Connected,
            })
            .map_err(|reason| protocol("openai", &reason))?;
        Ok(OpenAiRealtimeCallResponse {
            request_id: request.connect.context.request_id,
            target_model: request.connect.context.target.model,
            call_id,
            sdp_answer,
            session: canonical,
        })
    }

    fn authorized(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        request.header(AUTHORIZATION, format!("Bearer {}", self.config.api_key))
    }

    fn gate(
        &self,
        provider: &str,
        model: &str,
        modality: ModalityRequirement,
    ) -> ProviderMediaResult<()> {
        let state = self
            .catalog
            .profiles
            .iter()
            .find(|profile| profile.provider == provider && profile.model == model)
            .map(|profile| profile.capability_state(&capability(modality)))
            .unwrap_or(CapabilityState::Unknown);
        if provider != "openai" || state != CapabilityState::Supported {
            return Err(ProviderMediaError::UnsupportedCapability {
                provider: provider.into(),
                model: model.into(),
                modality,
                state,
            });
        }
        Ok(())
    }

    async fn send_bytes(
        &self,
        request: reqwest::RequestBuilder,
        cancellation: CancellationToken,
    ) -> ProviderMediaResult<Vec<u8>> {
        let response = self.send(request, cancellation.clone()).await?;
        self.response_bytes(response, cancellation).await
    }

    async fn send(
        &self,
        request: reqwest::RequestBuilder,
        cancellation: CancellationToken,
    ) -> ProviderMediaResult<reqwest::Response> {
        if cancellation.is_cancelled() {
            return Err(ProviderMediaError::Cancelled);
        }
        let response = tokio::select! {
            _ = cancellation.cancelled() => return Err(ProviderMediaError::Cancelled),
            result = request.send() => result.map_err(|_| ProviderMediaError::Http { provider: "openai".into(), code: "transport".into(), status: None, retry_after_ms: None, retryable: true })?,
        };
        if response.status().is_success() {
            return Ok(response);
        }
        let status = response.status().as_u16();
        let retry_after_ms = response
            .headers()
            .get(RETRY_AFTER)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .and_then(|seconds| seconds.checked_mul(1_000));
        let (code, retryable) = match status {
            401 | 403 => ("authentication", false),
            408 => ("timeout", true),
            429 => ("rate_limit", true),
            500..=599 => ("server", true),
            _ => ("invalid_request", false),
        };
        Err(ProviderMediaError::Http {
            provider: "openai".into(),
            code: code.into(),
            status: Some(status),
            retry_after_ms,
            retryable,
        })
    }

    async fn response_bytes(
        &self,
        response: reqwest::Response,
        cancellation: CancellationToken,
    ) -> ProviderMediaResult<Vec<u8>> {
        if response
            .content_length()
            .is_some_and(|length| length > MAX_PROVIDER_MEDIA_RESPONSE_BYTES as u64)
        {
            return Err(protocol("openai", "media response exceeded the byte limit"));
        }
        let mut body = Vec::new();
        let mut stream = response.bytes_stream();
        loop {
            let chunk = tokio::select! {
                _ = cancellation.cancelled() => return Err(ProviderMediaError::Cancelled),
                result = stream.next() => result,
            };
            let Some(chunk) = chunk else { break };
            let chunk = chunk.map_err(|_| ProviderMediaError::Http {
                provider: "openai".into(),
                code: "transport".into(),
                status: None,
                retry_after_ms: None,
                retryable: true,
            })?;
            if body.len().saturating_add(chunk.len()) > MAX_PROVIDER_MEDIA_RESPONSE_BYTES {
                return Err(protocol("openai", "media response exceeded the byte limit"));
            }
            body.extend_from_slice(&chunk);
        }
        Ok(body)
    }

    async fn persist(
        &self,
        draft: ArtifactDraft,
        cancellation: CancellationToken,
    ) -> ProviderMediaResult<MediaArtifact> {
        let expected_provider = draft.provider.clone();
        let expected_model = draft.model.clone();
        let expected_media_type = draft.media_type.clone();
        let staged = tokio::select! {
            _ = cancellation.cancelled() => return Err(ProviderMediaError::Cancelled),
            result = self.artifacts.stage(draft) => result?,
        };
        if cancellation.is_cancelled() {
            let _ = self.artifacts.abort(&staged).await;
            return Err(ProviderMediaError::Cancelled);
        }
        let staged_validation =
            staged
                .artifact
                .validate()
                .map_err(|_| ProviderMediaError::Artifact {
                    stage: "validate".into(),
                });
        let staged_validation = staged_validation.and_then(|()| {
            if staged.artifact.provider.as_deref() != Some(&expected_provider)
                || staged.artifact.model.as_deref() != Some(&expected_model)
                || staged.artifact.media_type != expected_media_type
            {
                Err(ProviderMediaError::Artifact {
                    stage: "provenance".into(),
                })
            } else {
                Ok(())
            }
        });
        if let Err(error) = staged_validation {
            self.artifacts
                .abort(&staged)
                .await
                .map_err(|_| ProviderMediaError::Artifact {
                    stage: "abort".into(),
                })?;
            return Err(error);
        }
        let committed = tokio::select! {
            _ = cancellation.cancelled() => {
                let _ = self.artifacts.abort(&staged).await;
                return Err(ProviderMediaError::Cancelled);
            },
            result = self.artifacts.commit(&staged) => match result {
                Ok(artifact) => artifact,
                Err(error) => { let _ = self.artifacts.abort(&staged).await; return Err(error); }
            },
        };
        if committed != staged.artifact {
            self.artifacts
                .abort(&staged)
                .await
                .map_err(|_| ProviderMediaError::Artifact {
                    stage: "abort".into(),
                })?;
            return Err(ProviderMediaError::Artifact {
                stage: "commit".into(),
            });
        }
        Ok(committed)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpenAiRealtimeCallRequest {
    pub connect: RealtimeConnectRequest,
    pub sdp_offer: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpenAiRealtimeCallResponse {
    pub request_id: String,
    pub target_model: String,
    pub call_id: Option<String>,
    pub sdp_answer: String,
    pub session: RealtimeSession,
}

fn capability(modality: ModalityRequirement) -> ModelCapability {
    match modality {
        ModalityRequirement::ImageGeneration => ModelCapability::ImageGeneration,
        ModalityRequirement::Transcription => ModelCapability::Transcription,
        ModalityRequirement::SpeechGeneration => ModelCapability::SpeechGeneration,
        ModalityRequirement::RealtimeDuplex => ModelCapability::RealtimeDuplex,
        _ => ModelCapability::Custom(format!("media:{modality:?}")),
    }
}

fn wire_model(model: &str) -> &str {
    model.strip_prefix("openai:").unwrap_or(model)
}

fn image_format(media_type: &str) -> ProviderMediaResult<&'static str> {
    match media_type {
        "image/png" => Ok("png"),
        "image/jpeg" => Ok("jpeg"),
        "image/webp" => Ok("webp"),
        _ => Err(ProviderMediaError::UnsupportedParameter {
            provider: "openai".into(),
            parameter: "output_media_type".into(),
        }),
    }
}

fn audio_format(media_type: &str) -> ProviderMediaResult<&'static str> {
    match media_type {
        "audio/mpeg" => Ok("mp3"),
        "audio/opus" => Ok("opus"),
        "audio/aac" => Ok("aac"),
        "audio/flac" => Ok("flac"),
        "audio/wav" | "audio/x-wav" => Ok("wav"),
        "audio/pcm" => Ok("pcm"),
        _ => Err(ProviderMediaError::UnsupportedParameter {
            provider: "openai".into(),
            parameter: "output_media_type".into(),
        }),
    }
}

fn image_payload(
    item: &Value,
    requested: Option<&str>,
) -> ProviderMediaResult<(ArtifactPayload, String)> {
    let media_type = requested
        .or_else(|| item.get("mime_type").and_then(Value::as_str))
        .unwrap_or("image/png")
        .to_owned();
    if let Some(data) = item.get("b64_json").and_then(Value::as_str) {
        return Ok((ArtifactPayload::Base64(data.into()), media_type));
    }
    if let Some(url) = item.get("url").and_then(Value::as_str) {
        return Ok((ArtifactPayload::Url(url.into()), media_type));
    }
    Err(protocol(
        "openai",
        "image item omitted both b64_json and url",
    ))
}

fn image_usage(value: &Value) -> MediaUsage {
    let usage = value.get("usage").unwrap_or(&Value::Null);
    MediaUsage {
        input_tokens: usage
            .get("input_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        output_tokens: usage
            .get("output_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        generated_images: 1,
        ..MediaUsage::default()
    }
}

fn transcription_usage(value: &Value) -> MediaUsage {
    let usage = value.get("usage").unwrap_or(&Value::Null);
    let input_audio_ms = usage
        .get("seconds")
        .and_then(Value::as_f64)
        .filter(|value| value.is_finite() && *value >= 0.0)
        .map(|seconds| (seconds * 1_000.0).round() as u64)
        .unwrap_or(0);
    MediaUsage {
        input_tokens: usage
            .get("input_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        output_tokens: usage
            .get("output_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        input_audio_ms,
        ..MediaUsage::default()
    }
}

fn transcript_segment(value: &Value) -> Option<TranscriptSegment> {
    let start = value.get("start")?.as_f64()?;
    let end = value.get("end")?.as_f64()?;
    if !start.is_finite() || !end.is_finite() || start < 0.0 || end < start {
        return None;
    }
    Some(TranscriptSegment {
        start_ms: (start * 1_000.0).round() as u64,
        end_ms: (end * 1_000.0).round() as u64,
        text: value.get("text")?.as_str()?.into(),
        speaker: value
            .get("speaker")
            .and_then(Value::as_str)
            .map(str::to_owned),
        confidence: None,
    })
}

fn merge_options(
    provider: &str,
    body: &mut Value,
    options: &Value,
    protected: &[&str],
) -> ProviderMediaResult<()> {
    if options.is_null() {
        return Ok(());
    }
    let options = options.as_object().ok_or_else(|| {
        invalid(
            "provider_options",
            "provider options must be an object or null",
        )
    })?;
    let body = body
        .as_object_mut()
        .expect("provider request body is an object");
    for (key, value) in options {
        if protected.contains(&key.as_str()) {
            return Err(ProviderMediaError::UnsupportedParameter {
                provider: provider.into(),
                parameter: key.clone(),
            });
        }
        body.insert(key.clone(), value.clone());
    }
    Ok(())
}

fn reject_nonempty_options(provider: &str, options: &Value) -> ProviderMediaResult<()> {
    match options {
        Value::Null => Ok(()),
        Value::Object(map) if map.is_empty() => Ok(()),
        Value::Object(_) => Err(ProviderMediaError::UnsupportedParameter {
            provider: provider.into(),
            parameter: "provider_options".into(),
        }),
        _ => Err(invalid(
            "provider_options",
            "provider options must be an object or null",
        )),
    }
}

fn multipart_boundary() -> ProviderMediaResult<String> {
    let mut random = [0_u8; 24];
    getrandom::fill(&mut random)
        .map_err(|_| invalid("multipart_boundary", "secure randomness was unavailable"))?;
    let mut boundary = String::with_capacity(5 + random.len() * 2);
    boundary.push_str("aikit");
    for byte in random {
        use std::fmt::Write as _;
        write!(&mut boundary, "{byte:02x}")
            .expect("writing hexadecimal bytes to a String cannot fail");
    }
    Ok(boundary)
}

fn validate_multipart_media_type(media_type: &str) -> ProviderMediaResult<()> {
    if media_type.is_empty()
        || !media_type.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'+' | b'-' | b'.' | b';' | b'=')
        })
    {
        return Err(invalid(
            "audio.media_type",
            "multipart MIME type contains unsafe characters",
        ));
    }
    Ok(())
}

fn validate_inline_transcription(
    data: &[u8],
    expected_size_bytes: u64,
    expected_sha256: &str,
) -> ProviderMediaResult<()> {
    if expected_size_bytes > MAX_INLINE_TRANSCRIPTION_BYTES as u64
        || data.len() > MAX_INLINE_TRANSCRIPTION_BYTES
    {
        return Err(invalid(
            "audio.size_bytes",
            "inline transcription audio exceeds the 25 MiB byte limit",
        ));
    }
    if expected_size_bytes != data.len() as u64 {
        return Err(invalid(
            "audio.size_bytes",
            "declared size does not match inline audio bytes",
        ));
    }
    let actual = crate::governance::contracts::sha256_digest(data);
    if actual.strip_prefix("sha256:") != Some(expected_sha256) {
        return Err(invalid(
            "audio.sha256",
            "declared SHA-256 does not match inline audio bytes",
        ));
    }
    Ok(())
}

fn multipart_fields(boundary: &str, fields: &[(&str, String)]) -> Vec<u8> {
    let mut body = Vec::new();
    for (name, value) in fields {
        body.extend_from_slice(format!("--{boundary}\r\nContent-Disposition: form-data; name=\"{name}\"\r\n\r\n{value}\r\n").as_bytes());
    }
    body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
    body
}

fn multipart_stream_body(
    boundary: &str,
    fields: &[(&str, String)],
    file_field: &str,
    filename: &str,
    media_type: &str,
    data: Vec<u8>,
) -> reqwest::Body {
    let mut prefix = multipart_fields_without_end(boundary, fields);
    prefix.extend_from_slice(format!("--{boundary}\r\nContent-Disposition: form-data; name=\"{file_field}\"; filename=\"{filename}\"\r\nContent-Type: {media_type}\r\n\r\n").as_bytes());
    let suffix = format!("\r\n--{boundary}--\r\n").into_bytes();
    reqwest::Body::wrap_stream(futures::stream::iter([
        Ok::<Vec<u8>, std::convert::Infallible>(prefix),
        Ok(data),
        Ok(suffix),
    ]))
}

fn multipart_fields_without_end(boundary: &str, fields: &[(&str, String)]) -> Vec<u8> {
    let mut body = Vec::new();
    for (name, value) in fields {
        body.extend_from_slice(format!("--{boundary}\r\nContent-Disposition: form-data; name=\"{name}\"\r\n\r\n{value}\r\n").as_bytes());
    }
    body
}

fn runtime_validation(error: crate::media_runtime::MediaRuntimeError) -> ProviderMediaError {
    invalid("canonical_request", &error.to_string())
}
fn invalid(field: &str, reason: &str) -> ProviderMediaError {
    ProviderMediaError::InvalidRequest {
        field: field.into(),
        reason: reason.into(),
    }
}
fn protocol(provider: &str, reason: &str) -> ProviderMediaError {
    ProviderMediaError::Protocol {
        provider: provider.into(),
        reason: reason.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contract::{MediaInput, MediaInputSource};
    use crate::media_runtime::{ImageDimensions, MediaRequestContext, MediaTarget};
    use crate::multimodal::{RealtimeSessionState, VoiceActivityPolicy};
    use std::collections::BTreeSet;
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Mutex,
    };
    use std::time::Duration;
    use wiremock::matchers::{body_json, body_string_contains, header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[derive(Default)]
    struct RecordingStore {
        events: Mutex<Vec<String>>,
    }

    #[derive(Default)]
    struct InvalidStagingStore {
        events: Mutex<Vec<String>>,
        committed: AtomicBool,
        invalid_provenance: bool,
    }

    impl InvalidStagingStore {
        fn with_invalid_provenance() -> Self {
            Self {
                invalid_provenance: true,
                ..Self::default()
            }
        }
    }

    #[async_trait]
    impl MediaArtifactStore for InvalidStagingStore {
        async fn stage(&self, draft: ArtifactDraft) -> ProviderMediaResult<StagedMediaArtifact> {
            self.events.lock().unwrap().push("stage".into());
            Ok(StagedMediaArtifact {
                staging_id: "invalid-stage".into(),
                artifact: MediaArtifact {
                    artifact_id: if self.invalid_provenance {
                        "artifact-1".into()
                    } else {
                        String::new()
                    },
                    media_type: draft.media_type,
                    sha256: "a".repeat(64),
                    size_bytes: 4,
                    provider: Some(if self.invalid_provenance {
                        "wrong-provider".into()
                    } else {
                        draft.provider
                    }),
                    model: Some(draft.model),
                },
            })
        }

        async fn commit(&self, staged: &StagedMediaArtifact) -> ProviderMediaResult<MediaArtifact> {
            self.events.lock().unwrap().push("commit".into());
            self.committed.store(true, Ordering::SeqCst);
            Ok(staged.artifact.clone())
        }

        async fn abort(&self, _staged: &StagedMediaArtifact) -> ProviderMediaResult<()> {
            self.events.lock().unwrap().push("abort".into());
            self.committed.store(false, Ordering::SeqCst);
            Ok(())
        }
    }

    #[async_trait]
    impl MediaArtifactStore for RecordingStore {
        async fn stage(&self, draft: ArtifactDraft) -> ProviderMediaResult<StagedMediaArtifact> {
            self.events.lock().unwrap().push("stage".into());
            Ok(StagedMediaArtifact {
                staging_id: "stage-1".into(),
                artifact: MediaArtifact {
                    artifact_id: "artifact-1".into(),
                    media_type: draft.media_type,
                    sha256: "a".repeat(64),
                    size_bytes: 4,
                    provider: Some(draft.provider),
                    model: Some(draft.model),
                },
            })
        }
        async fn commit(&self, staged: &StagedMediaArtifact) -> ProviderMediaResult<MediaArtifact> {
            self.events.lock().unwrap().push("commit".into());
            Ok(staged.artifact.clone())
        }
        async fn abort(&self, _staged: &StagedMediaArtifact) -> ProviderMediaResult<()> {
            self.events.lock().unwrap().push("abort".into());
            Ok(())
        }
    }

    fn context(model: &str, requirement: ModalityRequirement) -> MediaRequestContext {
        MediaRequestContext {
            request_id: "req-1".into(),
            correlation_id: None,
            target: MediaTarget::new("openai", model),
            requirements: BTreeSet::from([requirement]),
            catalog_version: None,
            include_raw_provider_events: false,
        }
    }

    fn catalog_with(model: &str, capability: ModelCapability) -> Arc<ModelCatalogSnapshot> {
        let mut catalog = ModelCatalogSnapshot::shipped().unwrap();
        let profile = catalog
            .profiles
            .iter_mut()
            .find(|profile| profile.provider == "openai")
            .unwrap();
        profile.model = model.into();
        *profile = profile
            .clone()
            .with_capability_state(capability, CapabilityState::Supported);
        Arc::new(catalog)
    }

    fn make_transport(
        server: &MockServer,
        model: &str,
        capability: ModelCapability,
        store: Arc<RecordingStore>,
    ) -> OpenAiMediaTransport {
        OpenAiMediaTransport::new(
            OpenAiMediaConfig::with_base_url("secret", server.uri()).unwrap(),
            catalog_with(model, capability),
            store,
        )
    }

    #[tokio::test]
    async fn shipped_catalog_fails_closed_before_network_io() {
        let store = Arc::new(RecordingStore::default());
        let transport = OpenAiMediaTransport::new(
            OpenAiMediaConfig::with_base_url("secret", "http://127.0.0.1:9").unwrap(),
            Arc::new(ModelCatalogSnapshot::shipped().unwrap()),
            store,
        );
        let error = transport
            .generate_image(
                ImageGenerationRequest {
                    context: context("gpt-5.6-sol", ModalityRequirement::ImageGeneration),
                    prompt: "cat".into(),
                    count: 1,
                    dimensions: None,
                    output_media_type: None,
                    provider_options: Value::Null,
                },
                CancellationToken::new(),
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            ProviderMediaError::UnsupportedCapability {
                state: CapabilityState::Unsupported,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn image_contract_proves_auth_endpoint_body_and_artifact_commit() {
        let server = MockServer::start().await;
        Mock::given(method("POST")).and(path("/v1/images/generations")).and(header("authorization", "Bearer secret")).and(body_json(serde_json::json!({"model":"gpt-image-test","prompt":"cat","n":1,"size":"1024x1024","output_format":"png"}))).respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"data":[{"b64_json":"AAAA","revised_prompt":"a cat"}],"usage":{"input_tokens":2,"output_tokens":3}}))).mount(&server).await;
        let store = Arc::new(RecordingStore::default());
        let transport = make_transport(
            &server,
            "gpt-image-test",
            ModelCapability::ImageGeneration,
            store.clone(),
        );
        let response = transport
            .generate_image(
                ImageGenerationRequest {
                    context: context("gpt-image-test", ModalityRequirement::ImageGeneration),
                    prompt: "cat".into(),
                    count: 1,
                    dimensions: Some(ImageDimensions {
                        width: 1024,
                        height: 1024,
                    }),
                    output_media_type: Some("image/png".into()),
                    provider_options: Value::Null,
                },
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(response.images[0].revised_prompt.as_deref(), Some("a cat"));
        assert_eq!(store.events.lock().unwrap().as_slice(), ["stage", "commit"]);
    }

    #[tokio::test]
    async fn transcription_upload_mapping_and_header_injection_guard_are_keyless() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/audio/transcriptions"))
            .and(header("authorization", "Bearer secret"))
            .and(body_string_contains("name=\"model\"\r\n\r\ngpt-stt"))
            .and(body_string_contains(
                "name=\"file\"; filename=\"audio.bin\"",
            ))
            .and(body_string_contains("RIFF"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "text":"hello", "language":"en",
                "segments":[{"start":0.0,"end":1.25,"text":"hello"}]
            })))
            .mount(&server)
            .await;
        let store = Arc::new(RecordingStore::default());
        let transport = make_transport(&server, "gpt-stt", ModelCapability::Transcription, store);
        let mut request = TranscriptionRequest {
            context: context("gpt-stt", ModalityRequirement::Transcription),
            audio: MediaInput {
                media_type: "audio/wav".into(),
                source: MediaInputSource::Bytes {
                    data: b"RIFF".to_vec(),
                },
                sha256: crate::governance::contracts::sha256_digest(b"RIFF")
                    .trim_start_matches("sha256:")
                    .into(),
                size_bytes: 4,
            },
            language: Some("en".into()),
            prompt: None,
            include_segments: true,
            provider_options: Value::Null,
        };
        let response = transport
            .transcribe(request.clone(), CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(response.transcript.segments[0].end_ms, 1_250);

        request.audio.media_type = "audio/wav\r\nX-Evil: yes".into();
        let error = transport
            .transcribe(request, CancellationToken::new())
            .await
            .unwrap_err();
        assert!(matches!(error, ProviderMediaError::InvalidRequest { .. }));
    }

    #[tokio::test]
    async fn transcription_rejects_inline_size_and_hash_mismatch_before_network_io() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/audio/transcriptions"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"text":"must not be reached"})),
            )
            .expect(0)
            .mount(&server)
            .await;
        let store = Arc::new(RecordingStore::default());
        let transport = make_transport(&server, "gpt-stt", ModelCapability::Transcription, store);

        for (size_bytes, sha256) in [
            (
                5,
                crate::governance::contracts::sha256_digest(b"RIFF")
                    .trim_start_matches("sha256:")
                    .to_owned(),
            ),
            (4, "b".repeat(64)),
        ] {
            let error = transport
                .transcribe(
                    TranscriptionRequest {
                        context: context("gpt-stt", ModalityRequirement::Transcription),
                        audio: MediaInput {
                            media_type: "audio/wav".into(),
                            source: MediaInputSource::Bytes {
                                data: b"RIFF".to_vec(),
                            },
                            sha256,
                            size_bytes,
                        },
                        language: None,
                        prompt: None,
                        include_segments: false,
                        provider_options: Value::Null,
                    },
                    CancellationToken::new(),
                )
                .await
                .unwrap_err();
            assert!(matches!(error, ProviderMediaError::InvalidRequest { .. }));
        }
    }

    #[test]
    fn transcription_rejects_inline_bytes_over_the_hard_limit() {
        let oversized = vec![0; MAX_INLINE_TRANSCRIPTION_BYTES + 1];
        let error =
            validate_inline_transcription(&oversized, oversized.len() as u64, &"a".repeat(64))
                .unwrap_err();
        assert!(matches!(error, ProviderMediaError::InvalidRequest { .. }));
    }

    #[tokio::test]
    async fn invalid_staged_artifact_is_aborted_before_commit() {
        let store = Arc::new(InvalidStagingStore::default());
        let transport = OpenAiMediaTransport::new(
            OpenAiMediaConfig::new("secret").unwrap(),
            catalog_with("gpt-image-test", ModelCapability::ImageGeneration),
            store.clone(),
        );
        let error = transport
            .persist(
                ArtifactDraft {
                    request_id: "req-1".into(),
                    provider: "openai".into(),
                    model: "gpt-image-test".into(),
                    media_type: "image/png".into(),
                    payload: ArtifactPayload::Bytes(vec![0, 1, 2, 3]),
                },
                CancellationToken::new(),
            )
            .await
            .unwrap_err();

        assert_eq!(
            error,
            ProviderMediaError::Artifact {
                stage: "validate".into()
            }
        );
        assert_eq!(store.events.lock().unwrap().as_slice(), ["stage", "abort"]);
        assert!(!store.committed.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn invalid_staged_provenance_is_aborted_before_commit() {
        let store = Arc::new(InvalidStagingStore::with_invalid_provenance());
        let transport = OpenAiMediaTransport::new(
            OpenAiMediaConfig::new("secret").unwrap(),
            catalog_with("gpt-image-test", ModelCapability::ImageGeneration),
            store.clone(),
        );
        let error = transport
            .persist(
                ArtifactDraft {
                    request_id: "req-1".into(),
                    provider: "openai".into(),
                    model: "gpt-image-test".into(),
                    media_type: "image/png".into(),
                    payload: ArtifactPayload::Bytes(vec![0, 1, 2, 3]),
                },
                CancellationToken::new(),
            )
            .await
            .unwrap_err();

        assert_eq!(
            error,
            ProviderMediaError::Artifact {
                stage: "provenance".into()
            }
        );
        assert_eq!(store.events.lock().unwrap().as_slice(), ["stage", "abort"]);
        assert!(!store.committed.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn speech_cancellation_and_provider_errors_are_typed() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/audio/speech"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_delay(Duration::from_secs(2))
                    .set_body_bytes(b"audio"),
            )
            .mount(&server)
            .await;
        let store = Arc::new(RecordingStore::default());
        let transport = make_transport(
            &server,
            "gpt-tts",
            ModelCapability::SpeechGeneration,
            store.clone(),
        );
        let cancellation = CancellationToken::new();
        let handle = cancellation.handle();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            handle.cancel();
        });
        let error = transport
            .generate_speech(
                SpeechGenerationRequest {
                    context: context("gpt-tts", ModalityRequirement::SpeechGeneration),
                    text: "hello".into(),
                    voice: "alloy".into(),
                    output_media_type: "audio/mpeg".into(),
                    speed: 1.0,
                    provider_options: Value::Null,
                },
                cancellation,
            )
            .await
            .unwrap_err();
        assert_eq!(error, ProviderMediaError::Cancelled);
        assert!(store.events.lock().unwrap().is_empty());

        let error_server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/audio/speech"))
            .respond_with(
                ResponseTemplate::new(429)
                    .insert_header("retry-after", "2")
                    .set_body_string("secret-body"),
            )
            .mount(&error_server)
            .await;
        let transport = make_transport(
            &error_server,
            "gpt-tts",
            ModelCapability::SpeechGeneration,
            store,
        );
        let error = transport
            .generate_speech(
                SpeechGenerationRequest {
                    context: context("gpt-tts", ModalityRequirement::SpeechGeneration),
                    text: "hello".into(),
                    voice: "alloy".into(),
                    output_media_type: "audio/mpeg".into(),
                    speed: 1.0,
                    provider_options: Value::Null,
                },
                CancellationToken::new(),
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            ProviderMediaError::Http {
                status: Some(429),
                retry_after_ms: Some(2_000),
                retryable: true,
                ..
            }
        ));
        assert!(!error.to_string().contains("secret-body"));
    }

    #[tokio::test]
    async fn realtime_http_contract_posts_sdp_and_activates_session() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/realtime/calls"))
            .and(header("authorization", "Bearer secret"))
            .and(body_string_contains("name=\"sdp\"\r\n\r\nv=0"))
            .and(body_string_contains("\"model\":\"gpt-realtime\""))
            .respond_with(
                ResponseTemplate::new(201)
                    .insert_header("location", "/v1/realtime/calls/call_1")
                    .set_body_string("v=0 answer"),
            )
            .mount(&server)
            .await;
        let store = Arc::new(RecordingStore::default());
        let transport = make_transport(
            &server,
            "gpt-realtime",
            ModelCapability::RealtimeDuplex,
            store,
        );
        let response = transport
            .create_realtime_call(
                OpenAiRealtimeCallRequest {
                    connect: RealtimeConnectRequest {
                        context: context("gpt-realtime", ModalityRequirement::RealtimeDuplex),
                        session_id: "session-1".into(),
                        modalities: BTreeSet::from([ModalityRequirement::Text]),
                        voice_activity: VoiceActivityPolicy::default(),
                        provider_options: Value::Null,
                    },
                    sdp_offer: "v=0".into(),
                },
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(response.call_id.as_deref(), Some("call_1"));
        assert_eq!(response.sdp_answer, "v=0 answer");
        assert_eq!(response.session.state, RealtimeSessionState::Active);
    }
}
