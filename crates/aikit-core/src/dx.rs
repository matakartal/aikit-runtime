//! Typed structured output — `generate_object` with an **honest** per-provider fidelity grade.
//!
//! The differentiator is not "structured output" (everyone has some); it is that aikit picks the
//! *strongest mechanism each provider actually offers* and tells you which one it used, instead of
//! silently degrading:
//!
//! | Provider  | Mechanism                                             | [`FidelityGrade`]      |
//! |-----------|-------------------------------------------------------|------------------------|
//! | OpenAI    | `response_format: json_schema` (strict, constrained)  | `NativeConstrained`    |
//! | Google    | `generationConfig.responseJsonSchema` (constrained)   | `NativeConstrained`    |
//! | Anthropic | `output_config.format` JSON schema (constrained)      | `NativeConstrained`    |
//! | DeepSeek  | `response_format: json_object` + schema in the prompt | `PromptedAndParsed`    |
//!
//! We do **not** claim grammar-constrained decoding where the API cannot give it (DeepSeek). The
//! result carries the grade so the caller can trust — or distrust — accordingly.
//!
//! The encoding rides the existing `provider_options` escape hatch (each adapter merges it to the
//! wire verbatim), so this layer adds no per-provider wire code — it just knows *what to ask for*.

use crate::capabilities::FidelityGrade;
use crate::error::{AikitError, Result};
use crate::providers::{Provider, ProviderRequest};
use crate::types::{Message, StreamDelta, ToolSpec};
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::{FutureExt, StreamExt};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::{json, Map, Value};
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

const MAX_SEMANTIC_VALIDATION_REASON_BYTES: usize = 1_024;
const MAX_STRUCTURED_OUTPUT_RETRIES: u32 = 32;

/// A host semantic validator's decision after JSON parsing and JSON-Schema validation succeed.
///
/// [`Retry`](Self::Retry) consumes the same bounded repair budget as a schema failure, while
/// [`Reject`](Self::Reject) terminates immediately with a typed structured-output error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SemanticValidation {
    Accept,
    Retry(String),
    Reject(String),
}

/// Async application-level validation for a schema-valid structured object.
///
/// Implementations must be pure and idempotent: cancellation or a caller retry may invoke the
/// validator again with the same owned value. The value is never copied into aikit's error or
/// audit payloads automatically. A `Retry` reason is provider-facing and audit-recorded, so the
/// callback must return a safe summary rather than the raw value. Callback failures and panics
/// fail closed.
#[async_trait]
pub trait SemanticValidator: Send + Sync {
    async fn validate(&self, value: Value) -> std::result::Result<SemanticValidation, String>;
}

#[async_trait]
impl<F, Fut> SemanticValidator for F
where
    F: Fn(Value) -> Fut + Send + Sync,
    Fut: Future<Output = std::result::Result<SemanticValidation, String>> + Send,
{
    async fn validate(&self, value: Value) -> std::result::Result<SemanticValidation, String> {
        (self)(value).await
    }
}

/// The outcome of [`generate_object`]: the parsed value, plus the honest grade and attempt count.
#[derive(Debug, Clone, Serialize)]
pub struct GeneratedObject {
    /// The parsed, schema-validated object.
    pub value: Value,
    /// Which mechanism actually produced it — never silently weaker than promised.
    pub fidelity: FidelityGrade,
    /// How many model round-trips it took (1 = first try; more = validation-retry repairs).
    pub attempts: u32,
    /// Ordered provider-native response metadata observed across all attempts. This preserves
    /// anti-LCD escape hatches (cache, grounding, logprobs, service tier, raw finish details)
    /// without mixing them into the schema-validated value. It is raw and potentially sensitive;
    /// protect it like model output before logging or persistence.
    #[serde(default)]
    pub provider_metadata: crate::types::ProviderMetadata,
    /// Ordered adapter compatibility warnings observed across all attempts.
    #[serde(default)]
    pub warnings: Vec<crate::contract::ProviderWarning>,
}

impl GeneratedObject {
    /// Whether the object passed schema validation on the very first model round-trip. This is the
    /// honest counterpart to [`fidelity`](Self::fidelity): the grade names the *mechanism* aikit
    /// asked for, and this says whether that mechanism actually produced a valid object first-try.
    /// A `NativeConstrained` grade with `held_on_first_try() == false` means the "constrained"
    /// decoding still needed a repair round this call (e.g. a schema edge the constraint missed) —
    /// surfaced, never hidden.
    pub fn held_on_first_try(&self) -> bool {
        self.attempts == 1
    }
}

/// A schema-derived Rust value. `T` supplies both its JSON Schema and its deserializer, so native
/// Rust callers never need to manually duplicate a serde type as an untyped JSON schema.
#[derive(Debug, Clone)]
pub struct TypedGeneratedObject<T> {
    pub value: T,
    pub fidelity: FidelityGrade,
    pub attempts: u32,
    /// Raw, potentially sensitive provider-native response metadata.
    pub provider_metadata: crate::types::ProviderMetadata,
    pub warnings: Vec<crate::contract::ProviderWarning>,
}

/// One observable step of [`stream_object`]. Provider deltas are forwarded as they arrive; the
/// final object is emitted only after complete JSON parsing, JSON-Schema validation, and any
/// configured semantic validator accepts it.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ObjectStreamEvent {
    /// A model round-trip is about to begin. Attempts after the first are validation repairs.
    AttemptStarted {
        attempt: u32,
        total_attempts: u32,
        fidelity: FidelityGrade,
        repair: bool,
    },
    /// An unmodified canonical provider delta, delivered before the full object is available.
    Delta { attempt: u32, delta: StreamDelta },
    /// The completed candidate could not be parsed, did not satisfy the schema, or requested a
    /// semantic repair.
    ValidationFailed {
        attempt: u32,
        error: String,
        will_retry: bool,
    },
    /// The first fully parsed, schema-valid, and semantically accepted result.
    Completed { object: GeneratedObject },
}

/// A fallible incremental structured-output stream. Transport/audit failures and exhausted
/// validation repairs are errors; validation failures that can be repaired are also surfaced as
/// [`ObjectStreamEvent::ValidationFailed`] before the next attempt begins.
pub type ObjectStream = BoxStream<'static, Result<ObjectStreamEvent>>;

/// Tunables for [`generate_object`].
#[derive(Clone)]
pub struct ObjectOptions {
    /// Extra validation-repair round-trips after the first attempt (maximum 32).
    pub max_retries: u32,
    /// Output-token ceiling per attempt.
    pub max_tokens: u64,
    /// The tool/schema name used for forced-tool-call and `json_schema` encodings.
    pub name: String,
    /// Provider-keyed native options for the selected provider. Structured-output contract fields
    /// are applied after these options, so a caller cannot accidentally override the schema or
    /// weaken the reported fidelity grade.
    pub provider_options: crate::types::ProviderOptions,
    pub compatibility_mode: crate::contract::CompatibilityMode,
    /// Optional application-level validator, run only after JSON Schema succeeds. The callback
    /// must be pure/idempotent and may accept, request a bounded repair, or reject immediately.
    pub semantic_validator: Option<Arc<dyn SemanticValidator>>,
}

impl fmt::Debug for ObjectOptions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ObjectOptions")
            .field("max_retries", &self.max_retries)
            .field("max_tokens", &self.max_tokens)
            .field("name", &self.name)
            .field("provider_options", &self.provider_options)
            .field("compatibility_mode", &self.compatibility_mode)
            .field(
                "semantic_validator",
                &self.semantic_validator.as_ref().map(|_| "<registered>"),
            )
            .finish()
    }
}

impl Default for ObjectOptions {
    fn default() -> Self {
        ObjectOptions {
            max_retries: 2,
            max_tokens: 1024,
            name: "respond".into(),
            provider_options: crate::types::ProviderOptions::new(),
            compatibility_mode: crate::contract::CompatibilityMode::Strict,
            semantic_validator: None,
        }
    }
}

fn bounded_semantic_reason(reason: String, fallback: &str) -> String {
    const ELLIPSIS: &str = "...";
    let mut normalized = String::with_capacity(MAX_SEMANTIC_VALIDATION_REASON_BYTES);
    let mut truncated = false;
    for character in reason.trim().chars() {
        let character = if is_unsafe_display_character(character) {
            ' '
        } else {
            character
        };
        if normalized.len().saturating_add(character.len_utf8())
            > MAX_SEMANTIC_VALIDATION_REASON_BYTES
        {
            truncated = true;
            break;
        }
        normalized.push(character);
    }
    if normalized.trim().is_empty() {
        return fallback.to_string();
    }
    if truncated {
        while normalized.len().saturating_add(ELLIPSIS.len()) > MAX_SEMANTIC_VALIDATION_REASON_BYTES
        {
            normalized.pop();
        }
        normalized.push_str(ELLIPSIS);
    }
    normalized
}

fn is_unsafe_display_character(character: char) -> bool {
    character.is_control()
        || matches!(
            character,
            '\u{061c}'
                | '\u{200e}'
                | '\u{200f}'
                | '\u{202a}'..='\u{202e}'
                | '\u{2066}'..='\u{206f}'
        )
}

async fn validate_semantics(
    validator: Arc<dyn SemanticValidator>,
    value: Value,
) -> Result<SemanticValidation> {
    match AssertUnwindSafe(validator.validate(value))
        .catch_unwind()
        .await
    {
        Ok(Ok(SemanticValidation::Accept)) => Ok(SemanticValidation::Accept),
        Ok(Ok(SemanticValidation::Retry(reason))) => Ok(SemanticValidation::Retry(
            bounded_semantic_reason(reason, "semantic validator requested repair"),
        )),
        Ok(Ok(SemanticValidation::Reject(reason))) => Ok(SemanticValidation::Reject(
            bounded_semantic_reason(reason, "semantic validator rejected object"),
        )),
        Ok(Err(reason)) => Err(AikitError::StructuredOutput(format!(
            "semantic validator failed closed: {}",
            bounded_semantic_reason(reason, "callback returned an error")
        ))),
        Err(_) => Err(AikitError::StructuredOutput(
            "semantic validator panicked; object rejected".into(),
        )),
    }
}

/// Where the object is read back from the response.
enum ResultSource {
    /// Accumulated assistant text is the JSON.
    Text,
    /// The input of a forced tool call (already a JSON object) with this name.
    ToolCall(String),
}

struct StructuredPlan {
    extra_tools: Vec<ToolSpec>,
    options: Map<String, Value>,
    source: ResultSource,
    prompt_suffix: Option<String>,
}

fn fidelity_name(grade: FidelityGrade) -> &'static str {
    match grade {
        FidelityGrade::NativeConstrained => "native_constrained",
        FidelityGrade::ForcedToolCall => "forced_tool_call",
        FidelityGrade::PromptedAndParsed => "prompted_and_parsed",
    }
}

/// Encode the structured-output request for `provider` at `grade` as provider-options (+ maybe a
/// forced tool). This is the "ask the right way" decision, isolated and unit-testable.
fn plan_structured(
    provider: &str,
    model: &str,
    grade: FidelityGrade,
    name: &str,
    schema: &Value,
) -> StructuredPlan {
    match grade {
        // Anthropic: a single tool whose input schema IS the target, forced via tool_choice.
        FidelityGrade::ForcedToolCall => {
            let tool = ToolSpec {
                name: name.to_string(),
                description: "Return the result as structured data matching the schema.".into(),
                input_schema: schema.clone(),
            };
            let mut options = Map::new();
            options.insert(
                "tool_choice".into(),
                json!({ "type": "tool", "name": name }),
            );
            StructuredPlan {
                extra_tools: vec![tool],
                options,
                source: ResultSource::ToolCall(name.to_string()),
                prompt_suffix: None,
            }
        }
        // Constrained decoding using each provider's current native JSON-schema surface.
        FidelityGrade::NativeConstrained => {
            let mut options = Map::new();
            if provider == "google" {
                let generation_config = if model.starts_with("gemini-3") {
                    json!({
                        "responseFormat": {
                            "text": { "mimeType": "application/json", "schema": schema }
                        }
                    })
                } else {
                    json!({
                        "responseMimeType": "application/json",
                        "responseJsonSchema": schema
                    })
                };
                options.insert("generationConfig".into(), generation_config);
            } else if provider == "anthropic" {
                options.insert(
                    "output_config".into(),
                    json!({ "format": { "type": "json_schema", "schema": schema } }),
                );
            } else {
                options.insert(
                    "response_format".into(),
                    json!({
                        "type": "json_schema",
                        "json_schema": { "name": name, "strict": true, "schema": schema }
                    }),
                );
            }
            StructuredPlan {
                extra_tools: vec![],
                options,
                source: ResultSource::Text,
                prompt_suffix: None,
            }
        }
        // DeepSeek/openai-compat: json_object mode + the schema pinned into the prompt.
        FidelityGrade::PromptedAndParsed => {
            let mut options = Map::new();
            options.insert("response_format".into(), json!({ "type": "json_object" }));
            let suffix = format!(
                "Respond with ONLY a JSON object matching this schema (no prose, no markdown):\n{}",
                serde_json::to_string(schema).unwrap_or_default()
            );
            StructuredPlan {
                extra_tools: vec![],
                options,
                source: ResultSource::Text,
                prompt_suffix: Some(suffix),
            }
        }
    }
}

/// Generate a schema-validated object from `provider` at the given fidelity `grade`.
///
/// `provider_name` selects the wire encoding (e.g. `"openai"` vs `"google"` for
/// `NativeConstrained`); `grade` is the honest tier reported back in the result. Retries up to
/// `options.max_retries` times, feeding the validation error back so the model can repair.
pub async fn generate_object(
    provider: Arc<dyn Provider>,
    provider_name: &str,
    grade: FidelityGrade,
    model: &str,
    prompt: &str,
    schema: &Value,
    options: &ObjectOptions,
) -> Result<GeneratedObject> {
    generate_object_messages(
        provider,
        provider_name,
        grade,
        model,
        vec![Message::user(prompt)],
        schema,
        options,
    )
    .await
}

/// Canonical-message form of [`generate_object`], preserving multimodal input blocks.
#[allow(clippy::too_many_arguments)]
pub async fn generate_object_messages(
    provider: Arc<dyn Provider>,
    provider_name: &str,
    grade: FidelityGrade,
    model: &str,
    messages: Vec<Message>,
    schema: &Value,
    options: &ObjectOptions,
) -> Result<GeneratedObject> {
    generate_object_messages_observed(
        provider,
        provider_name,
        grade,
        model,
        messages,
        schema,
        options,
        None,
    )
    .await
}

/// Rust-native typed structured output. The schema is derived from `T`, the provider response is
/// validated against that schema, and only then deserialized into `T`.
pub async fn generate_object_typed<T>(
    provider: Arc<dyn Provider>,
    provider_name: &str,
    grade: FidelityGrade,
    model: &str,
    prompt: &str,
    options: &ObjectOptions,
) -> Result<TypedGeneratedObject<T>>
where
    T: DeserializeOwned + schemars::JsonSchema,
{
    let schema = serde_json::to_value(schemars::schema_for!(T)).map_err(|error| {
        AikitError::StructuredOutput(format!("schema encoding failed: {error}"))
    })?;
    let generated = generate_object(
        provider,
        provider_name,
        grade,
        model,
        prompt,
        &schema,
        options,
    )
    .await?;
    let value = serde_json::from_value(generated.value).map_err(|error| {
        AikitError::StructuredOutput(format!(
            "validated result could not be decoded into the Rust type: {error}"
        ))
    })?;
    Ok(TypedGeneratedObject {
        value,
        fidelity: generated.fidelity,
        attempts: generated.attempts,
        provider_metadata: generated.provider_metadata,
        warnings: generated.warnings,
    })
}

/// Rust-native typed structured output over canonical/multimodal message history.
#[allow(clippy::too_many_arguments)]
pub async fn generate_object_typed_messages<T>(
    provider: Arc<dyn Provider>,
    provider_name: &str,
    grade: FidelityGrade,
    model: &str,
    messages: Vec<Message>,
    options: &ObjectOptions,
) -> Result<TypedGeneratedObject<T>>
where
    T: DeserializeOwned + schemars::JsonSchema,
{
    let schema = serde_json::to_value(schemars::schema_for!(T)).map_err(|error| {
        AikitError::StructuredOutput(format!("schema encoding failed: {error}"))
    })?;
    let generated = generate_object_messages(
        provider,
        provider_name,
        grade,
        model,
        messages,
        &schema,
        options,
    )
    .await?;
    let value = serde_json::from_value(generated.value).map_err(|error| {
        AikitError::StructuredOutput(format!(
            "validated result could not be decoded into the Rust type: {error}"
        ))
    })?;
    Ok(TypedGeneratedObject {
        value,
        fidelity: generated.fidelity,
        attempts: generated.attempts,
        provider_metadata: generated.provider_metadata,
        warnings: generated.warnings,
    })
}

/// [`generate_object`] with structured audit events for each attempt and validation outcome.
#[allow(clippy::too_many_arguments)]
pub async fn generate_object_observed(
    provider: Arc<dyn Provider>,
    provider_name: &str,
    grade: FidelityGrade,
    model: &str,
    prompt: &str,
    schema: &Value,
    options: &ObjectOptions,
    audit: Option<&crate::observability::AuditTrail>,
) -> Result<GeneratedObject> {
    generate_object_messages_observed(
        provider,
        provider_name,
        grade,
        model,
        vec![Message::user(prompt)],
        schema,
        options,
        audit,
    )
    .await
}

/// [`generate_object_messages`] with structured audit events.
#[allow(clippy::too_many_arguments)]
pub async fn generate_object_messages_observed(
    provider: Arc<dyn Provider>,
    provider_name: &str,
    grade: FidelityGrade,
    model: &str,
    messages: Vec<Message>,
    schema: &Value,
    options: &ObjectOptions,
    audit: Option<&crate::observability::AuditTrail>,
) -> Result<GeneratedObject> {
    let mut stream = stream_object_messages_observed(
        provider,
        provider_name,
        grade,
        model,
        messages,
        schema,
        options,
        audit,
    );
    while let Some(event) = stream.next().await {
        if let ObjectStreamEvent::Completed { object } = event? {
            return Ok(object);
        }
    }
    Err(AikitError::StructuredOutput(
        "structured-output stream ended without a completed object".into(),
    ))
}

/// Stream a schema-validated object incrementally.
///
/// Unlike a one-shot wrapper around [`generate_object`], this forwards each canonical provider
/// delta as it is received. Parsing and validation happen at the end of every attempt. A failed
/// candidate emits [`ObjectStreamEvent::ValidationFailed`], then a real repair round-trip starts
/// (up to [`ObjectOptions::max_retries`]); only a validated candidate emits `Completed`.
pub fn stream_object(
    provider: Arc<dyn Provider>,
    provider_name: &str,
    grade: FidelityGrade,
    model: &str,
    prompt: &str,
    schema: &Value,
    options: &ObjectOptions,
) -> ObjectStream {
    stream_object_messages(
        provider,
        provider_name,
        grade,
        model,
        vec![Message::user(prompt)],
        schema,
        options,
    )
}

/// Canonical-message form of [`stream_object`], preserving multimodal input blocks.
#[allow(clippy::too_many_arguments)]
pub fn stream_object_messages(
    provider: Arc<dyn Provider>,
    provider_name: &str,
    grade: FidelityGrade,
    model: &str,
    messages: Vec<Message>,
    schema: &Value,
    options: &ObjectOptions,
) -> ObjectStream {
    stream_object_messages_observed(
        provider,
        provider_name,
        grade,
        model,
        messages,
        schema,
        options,
        None,
    )
}

/// [`stream_object`] with the same structured audit events as [`generate_object_observed`].
#[allow(clippy::too_many_arguments)]
pub fn stream_object_observed(
    provider: Arc<dyn Provider>,
    provider_name: &str,
    grade: FidelityGrade,
    model: &str,
    prompt: &str,
    schema: &Value,
    options: &ObjectOptions,
    audit: Option<&crate::observability::AuditTrail>,
) -> ObjectStream {
    stream_object_messages_observed(
        provider,
        provider_name,
        grade,
        model,
        vec![Message::user(prompt)],
        schema,
        options,
        audit,
    )
}

fn validate_object_messages(messages: Vec<Message>) -> Result<Vec<Message>> {
    if messages.is_empty() {
        return Err(AikitError::Configuration(
            "structured input messages must not be empty".into(),
        ));
    }
    if !messages
        .iter()
        .any(|message| message.role == crate::types::Role::User)
    {
        return Err(AikitError::Configuration(
            "structured input messages require at least one user message".into(),
        ));
    }
    Ok(messages)
}

fn append_user_instruction(
    mut messages: Vec<Message>,
    instruction: impl Into<String>,
) -> Result<Vec<Message>> {
    let user = messages
        .iter_mut()
        .rev()
        .find(|message| message.role == crate::types::Role::User)
        .ok_or_else(|| {
            AikitError::Configuration(
                "structured input messages require at least one user message".into(),
            )
        })?;
    user.content.push(crate::types::ContentBlock::Text {
        text: instruction.into(),
    });
    Ok(messages)
}

fn structured_protocol_error(
    provider: &str,
    model: &str,
    message: impl Into<String>,
    warnings: &[crate::contract::ProviderWarning],
) -> AikitError {
    crate::error::ProviderError::new(
        provider,
        model,
        crate::error::ProviderErrorKind::Protocol,
        message,
    )
    .with_warnings(warnings.to_vec())
    .into()
}

fn warning_retained_bytes(warning: &crate::contract::ProviderWarning) -> usize {
    warning
        .code
        .len()
        .saturating_add(warning.message.len())
        .saturating_add(warning.parameter.as_deref().map_or(0, str::len))
        .saturating_add(warning.provider.as_deref().map_or(0, str::len))
        .saturating_add(warning.model.as_deref().map_or(0, str::len))
}

fn merge_framework_options(target: &mut Map<String, Value>, framework: &Map<String, Value>) {
    for (key, framework_value) in framework {
        match (target.get_mut(key), framework_value) {
            (Some(Value::Object(target)), Value::Object(framework)) => {
                merge_framework_options(target, framework);
            }
            _ => {
                target.insert(key.clone(), framework_value.clone());
            }
        }
    }
}

fn clear_framework_owned_structured_output_conflicts(
    target: &mut Map<String, Value>,
    framework: &Map<String, Value>,
) {
    // OpenAI-compatible providers treat response_format as one discriminated union. Keeping
    // caller-owned keys from a different variant can make the wire constraint differ from the
    // schema AIKit validates locally (or make the provider reject the request outright).
    if framework.contains_key("response_format") {
        target.remove("response_format");
    }

    // Anthropic owns only output_config.format for structured output. Preserve unrelated native
    // controls under output_config while replacing the complete format subtree atomically.
    if framework
        .get("output_config")
        .and_then(Value::as_object)
        .is_some_and(|output| output.contains_key("format"))
    {
        if let Some(output) = target
            .get_mut("output_config")
            .and_then(Value::as_object_mut)
        {
            output.remove("format");
        }
    }

    clear_google_structured_output_conflicts(target, framework);
}

fn clear_google_structured_output_conflicts(
    target: &mut Map<String, Value>,
    framework: &Map<String, Value>,
) {
    let Some(framework_generation) = framework.get("generationConfig").and_then(Value::as_object)
    else {
        return;
    };
    let framework_owns_format = framework_generation.contains_key("responseFormat")
        || framework_generation.contains_key("responseMimeType")
        || framework_generation.contains_key("responseSchema")
        || framework_generation.contains_key("responseJsonSchema");
    if !framework_owns_format {
        return;
    }
    let Some(target_generation) = target
        .get_mut("generationConfig")
        .and_then(Value::as_object_mut)
    else {
        return;
    };
    for key in [
        "responseFormat",
        "responseMimeType",
        "responseSchema",
        "responseJsonSchema",
        "responseModalities",
    ] {
        target_generation.remove(key);
    }
}

/// [`stream_object_messages`] with structured audit events.
#[allow(clippy::too_many_arguments)]
pub fn stream_object_messages_observed(
    provider: Arc<dyn Provider>,
    provider_name: &str,
    grade: FidelityGrade,
    model: &str,
    messages: Vec<Message>,
    schema: &Value,
    options: &ObjectOptions,
    audit: Option<&crate::observability::AuditTrail>,
) -> ObjectStream {
    let plan = plan_structured(provider_name, model, grade, &options.name, schema);
    let base_messages = match &plan.prompt_suffix {
        Some(s) => append_user_instruction(messages, s.clone()),
        None => validate_object_messages(messages),
    };
    let model = model.to_string();
    let provider_name = provider_name.to_string();
    let schema = schema.clone();
    let options = options.clone();
    let audit = audit.cloned();
    let total_attempts = options
        .max_retries
        .checked_add(1)
        .filter(|_| options.max_retries <= MAX_STRUCTURED_OUTPUT_RETRIES);
    Box::pin(async_stream::try_stream! {
        let total_attempts = total_attempts.ok_or_else(|| {
            AikitError::Configuration(format!(
                "structured output max_retries cannot exceed {MAX_STRUCTURED_OUTPUT_RETRIES}"
            ))
        })?;
        let base_messages = base_messages?;
        let mut last_error = String::new();
        let mut provider_metadata = crate::types::ProviderMetadata::new();
        let mut warnings = Vec::new();
        // Warnings and provider metadata survive repair attempts, while rejected text/tool
        // candidates do not. Keep the persistent budget across attempts and clone it for each
        // attempt so released candidate reservations cannot accumulate indefinitely.
        let mut persistent_retained = crate::providers::StreamRetentionBudget::default();
        for attempt_index in 0..total_attempts {
            let attempt = attempt_index + 1;
            if let Some(audit) = &audit {
                audit.emit(crate::observability::AuditEvent::StructuredOutputAttempt {
                    attempt,
                    fidelity: fidelity_name(grade).into(),
                })?;
            }
            yield ObjectStreamEvent::AttemptStarted {
                attempt,
                total_attempts,
                fidelity: grade,
                repair: attempt_index > 0,
            };

            // On a repair attempt, tell the model exactly what was wrong with the last output.
            let messages = if attempt_index == 0 {
                base_messages.clone()
            } else {
                append_user_instruction(
                    base_messages.clone(),
                    format!("Your previous response was invalid ({last_error}). Return a corrected JSON object."),
                )?
            };
            let mut wire_options = options
                .provider_options
                .get(provider_name.as_str())
                .cloned()
                .unwrap_or_default();
            // Framework-owned structured-output fields must win over a conflicting escape hatch;
            // otherwise the result could be labelled NativeConstrained after the caller disabled
            // the native schema mode on the wire.
            clear_framework_owned_structured_output_conflicts(&mut wire_options, &plan.options);
            merge_framework_options(&mut wire_options, &plan.options);
            let req = ProviderRequest {
                model: model.clone(),
                messages,
                tools: plan.extra_tools.clone(),
                max_tokens: options.max_tokens,
                options: wire_options,
                provider_options: crate::types::ProviderOptions::new(),
                compatibility_mode: options.compatibility_mode,
            };

            let mut provider_stream = provider.stream(req).await?;
            let mut text = String::new();
            let mut names: HashMap<String, String> = HashMap::new();
            let mut tool_inputs: Vec<(String, Value)> = Vec::new();
            let mut tool_inputs_seen = HashSet::new();
            let mut stream_started = false;
            let mut stream_terminal = false;
            let mut stream_error = None;
            let mut attempt_retained = persistent_retained.clone();
            while let Some(delta) = provider_stream.next().await {
                if stream_terminal {
                    stream_error = Some(structured_protocol_error(
                        &provider_name,
                        &model,
                        "provider emitted a structured-output delta after MessageStop",
                        &warnings,
                    ));
                    break;
                }
                match &delta {
                    StreamDelta::Warning { warning } => {
                        let warning_bytes = warning_retained_bytes(warning);
                        if !attempt_retained.retain(warning_bytes, 1)
                            || !persistent_retained.retain(warning_bytes, 1)
                        {
                            stream_error = Some(structured_protocol_error(
                                &provider_name,
                                &model,
                                "structured-output stream exceeded the retained state limit",
                                &warnings,
                            ));
                        } else {
                            warnings.push(warning.clone());
                        }
                    }
                    StreamDelta::Error { message, info } => {
                        stream_error = Some(
                            stream_delta_error(
                                message.clone(),
                                info.clone(),
                                &provider_name,
                                &model,
                            )
                            .with_provider_warnings(warnings.clone()),
                        );
                    }
                    StreamDelta::MessageStart { .. } => {
                        if stream_started {
                            stream_error = Some(structured_protocol_error(
                                &provider_name,
                                &model,
                                "provider started the structured-output response more than once",
                                &warnings,
                            ));
                        } else {
                            stream_started = true;
                        }
                    }
                    _ if !stream_started => {
                        stream_error = Some(structured_protocol_error(
                            &provider_name,
                            &model,
                            "provider emitted structured output before MessageStart",
                            &warnings,
                        ));
                    }
                    StreamDelta::TextDelta { text: part } => {
                        if !attempt_retained.retain(part.len(), 0) {
                            stream_error = Some(structured_protocol_error(
                                &provider_name,
                                &model,
                                "structured-output stream exceeded the retained state limit",
                                &warnings,
                            ));
                        } else {
                            text.push_str(part);
                        }
                    }
                    StreamDelta::ToolCallStart { id, name } => {
                        if !attempt_retained.retain(id.len().saturating_add(name.len()), 1) {
                            stream_error = Some(structured_protocol_error(
                                &provider_name,
                                &model,
                                "structured-output stream exceeded the retained state limit",
                                &warnings,
                            ));
                        } else if names.insert(id.clone(), name.clone()).is_some() {
                            stream_error = Some(structured_protocol_error(
                                &provider_name,
                                &model,
                                format!("provider started structured-output tool call {id} more than once"),
                                &warnings,
                            ));
                        }
                    }
                    StreamDelta::ToolCallInput { id, input } => {
                        if !names.contains_key(id) {
                            stream_error = Some(structured_protocol_error(
                                &provider_name,
                                &model,
                                format!("provider emitted input before starting structured-output tool call {id}"),
                                &warnings,
                            ));
                        } else if !tool_inputs_seen.insert(id.clone()) {
                            stream_error = Some(structured_protocol_error(
                                &provider_name,
                                &model,
                                format!("provider emitted structured-output tool input {id} more than once"),
                                &warnings,
                            ));
                        } else if !attempt_retained.retain_json(input, 1) {
                            stream_error = Some(structured_protocol_error(
                                &provider_name,
                                &model,
                                "structured-output stream exceeded the retained state limit",
                                &warnings,
                            ));
                        } else {
                            let name = names
                                .get(id)
                                .expect("tool name exists after ordering validation")
                                .clone();
                            tool_inputs.push((name, input.clone()));
                        }
                    }
                    StreamDelta::ProviderMetadata { provider, metadata } => {
                        let metadata_bytes = crate::providers::json_retained_bytes(metadata)
                            .saturating_add(provider.len());
                        if !attempt_retained.retain(metadata_bytes, 1)
                            || !persistent_retained.retain(metadata_bytes, 1)
                        {
                            stream_error = Some(structured_protocol_error(
                                &provider_name,
                                &model,
                                "structured-output stream exceeded the retained state limit",
                                &warnings,
                            ));
                        } else {
                            provider_metadata
                                .entry(provider.clone())
                                .or_default()
                                .push(metadata.clone());
                        }
                    }
                    StreamDelta::MessageStop { .. } => stream_terminal = true,
                    _ => {}
                }
                yield ObjectStreamEvent::Delta { attempt, delta };
                if stream_error.is_some() {
                    break;
                }
            }
            if stream_error.is_none() && !stream_terminal {
                stream_error = Some(structured_protocol_error(
                    &provider_name,
                    &model,
                    "provider stream ended before structured-output MessageStop",
                    &warnings,
                ));
            }
            if stream_error.is_none() {
                if let Some(id) = names
                    .keys()
                    .find(|id| !tool_inputs_seen.contains(id.as_str()))
                {
                    stream_error = Some(structured_protocol_error(
                        &provider_name,
                        &model,
                        format!("structured-output tool call {id} ended without input"),
                        &warnings,
                    ));
                }
            }
            if let Some(error) = stream_error {
                Err(error)?;
            }

            // Extract the candidate object per the plan's source only after that attempt's real
            // stream completes; partial JSON is never mislabeled as a validated object.
            let candidate = match &plan.source {
                ResultSource::ToolCall(name) => tool_inputs
                    .into_iter()
                    .find(|(candidate_name, _)| candidate_name == name)
                    .map(|(_, input)| input)
                    .ok_or_else(|| "model did not call the structured-output tool".to_string()),
                ResultSource::Text => serde_json::from_str::<Value>(strip_fences(&text))
                    .map_err(|error| format!("response was not valid JSON: {error}")),
            };

            let schema_validated = candidate.and_then(|value| {
                validate(&schema, &value)?;
                Ok(value)
            });
            match schema_validated {
                Ok(value) => {
                    if let Some(validator) = options.semantic_validator.clone() {
                        match validate_semantics(validator, value.clone()).await {
                            Ok(SemanticValidation::Accept) => {}
                            Ok(SemanticValidation::Retry(reason)) => {
                                last_error = format!("semantic validation requested repair: {reason}");
                                if let Some(audit) = &audit {
                                    audit.emit(crate::observability::AuditEvent::StructuredOutputValidationFailed {
                                        attempt,
                                        error: last_error.clone(),
                                    })?;
                                }
                                let will_retry = attempt < total_attempts;
                                yield ObjectStreamEvent::ValidationFailed {
                                    attempt,
                                    error: last_error.clone(),
                                    will_retry,
                                };
                                if !will_retry {
                                    Err(AikitError::StructuredOutput(format!(
                                        "failed to produce a valid object after {total_attempts} attempt(s): {last_error}"
                                    )))?;
                                }
                                continue;
                            }
                            Ok(SemanticValidation::Reject(reason)) => {
                                last_error =
                                    format!("semantic validator rejected object: {reason}");
                                if let Some(audit) = &audit {
                                    audit.emit(crate::observability::AuditEvent::StructuredOutputValidationFailed {
                                        attempt,
                                        error: "semantic validator rejected object".into(),
                                    })?;
                                }
                                Err(AikitError::StructuredOutput(last_error))?;
                            }
                            Err(error) => {
                                if let Some(audit) = &audit {
                                    audit.emit(crate::observability::AuditEvent::StructuredOutputValidationFailed {
                                        attempt,
                                        error: "semantic validator failed closed".into(),
                                    })?;
                                }
                                Err(error)?;
                            }
                        }
                    }
                    let object = GeneratedObject {
                        value,
                        fidelity: grade,
                        attempts: attempt,
                        provider_metadata,
                        warnings,
                    };
                    if let Some(audit) = &audit {
                        audit.emit(crate::observability::AuditEvent::StructuredOutputCompleted {
                            attempts: object.attempts,
                            fidelity: fidelity_name(grade).into(),
                        })?;
                    }
                    yield ObjectStreamEvent::Completed { object };
                    return;
                }
                Err(error) => last_error = error,
            }

            if let Some(audit) = &audit {
                audit.emit(crate::observability::AuditEvent::StructuredOutputValidationFailed {
                    attempt,
                    error: last_error.clone(),
                })?;
            }
            let will_retry = attempt < total_attempts;
            yield ObjectStreamEvent::ValidationFailed {
                attempt,
                error: last_error.clone(),
                will_retry,
            };
            if !will_retry {
                Err(AikitError::StructuredOutput(format!(
                    "failed to produce a valid object after {total_attempts} attempt(s): {last_error}"
                )))?;
            }
        }
        Err(AikitError::StructuredOutput(format!(
            "failed to produce a valid object after {total_attempts} attempt(s): {last_error}"
        )))?;
    })
}

fn stream_delta_error(
    message: String,
    info: crate::error::ErrorInfo,
    fallback_provider: &str,
    fallback_model: &str,
) -> AikitError {
    use crate::error::{ErrorCode, ProviderError, ProviderErrorKind};

    let provider_kind = match info.code {
        ErrorCode::ProviderAuth => Some(ProviderErrorKind::Authentication),
        ErrorCode::ProviderRateLimit => Some(ProviderErrorKind::RateLimited),
        ErrorCode::ProviderTimeout => Some(ProviderErrorKind::Timeout),
        ErrorCode::ProviderTransport => Some(ProviderErrorKind::Transport),
        ErrorCode::ProviderServer => Some(ProviderErrorKind::Server),
        ErrorCode::ProviderInvalidRequest => Some(ProviderErrorKind::InvalidRequest),
        ErrorCode::ProviderProtocol => Some(ProviderErrorKind::Protocol),
        ErrorCode::ProviderSafety => Some(ProviderErrorKind::Safety),
        ErrorCode::Unknown if info.provider.is_some() || info.model.is_some() => {
            Some(ProviderErrorKind::Unknown)
        }
        _ => None,
    };
    if let Some(kind) = provider_kind {
        return ProviderError {
            provider: info
                .provider
                .unwrap_or_else(|| fallback_provider.to_string()),
            model: info.model.unwrap_or_else(|| fallback_model.to_string()),
            kind,
            status: info.status,
            retry_after_ms: info.retry_after_ms,
            message,
            warnings: info.warnings,
        }
        .into();
    }

    match info.code {
        ErrorCode::PermissionDenied => AikitError::PermissionDenied(message),
        ErrorCode::Sandbox => AikitError::Sandbox(message),
        ErrorCode::Configuration => AikitError::Configuration(message),
        ErrorCode::BudgetExceeded => AikitError::BudgetExceeded,
        ErrorCode::ToolExecution => AikitError::ToolExecution(message),
        ErrorCode::StructuredOutput => AikitError::StructuredOutput(message),
        ErrorCode::Session => AikitError::Session(message),
        ErrorCode::Conflict => AikitError::Conflict(message),
        ErrorCode::Cancelled => AikitError::Cancelled(message),
        ErrorCode::MaxTurns => AikitError::MaxTurns,
        ErrorCode::Audit => AikitError::Audit(message),
        ErrorCode::Hook => AikitError::Hook(message),
        ErrorCode::Unknown => AikitError::Other(message),
        ErrorCode::ProviderAuth
        | ErrorCode::ProviderRateLimit
        | ErrorCode::ProviderTimeout
        | ErrorCode::ProviderTransport
        | ErrorCode::ProviderServer
        | ErrorCode::ProviderInvalidRequest
        | ErrorCode::ProviderProtocol
        | ErrorCode::ProviderSafety => unreachable!("provider errors returned above"),
    }
}

/// Strip a ```json … ``` (or bare ``` … ```) fence some providers wrap JSON in.
fn strip_fences(s: &str) -> &str {
    let t = s.trim();
    if let Some(rest) = t.strip_prefix("```") {
        let rest = rest.strip_prefix("json").unwrap_or(rest);
        let rest = rest.trim_start_matches(['\n', '\r']);
        if let Some(end) = rest.rfind("```") {
            if rest[end + 3..].trim().is_empty() {
                return rest[..end].trim();
            }
        }
    }
    t
}

/// Validate against the complete supported JSON Schema draft rather than a hand-written subset.
/// Invalid schemas fail loudly too; a result is never called "schema-validated" after checking
/// only `type`/`required` while silently ignoring constraints such as `enum` or `anyOf`.
fn validate(schema: &Value, value: &Value) -> std::result::Result<(), String> {
    let validator = jsonschema::validator_for(schema)
        .map_err(|error| format!("invalid JSON Schema: {error}"))?;
    validator
        .validate(value)
        .map_err(|error| format!("schema validation failed: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result as AikitResult;
    use async_trait::async_trait;
    use futures::stream::BoxStream;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;
    use std::time::Duration;

    fn invoice_schema() -> Value {
        json!({
            "type": "object",
            "required": ["total", "currency"],
            "properties": {
                "total": { "type": "number" },
                "currency": { "type": "string" }
            }
        })
    }

    /// A provider that returns a fixed assistant-text body (the `Text` source path).
    struct TextMock(String);
    #[async_trait]
    impl Provider for TextMock {
        fn name(&self) -> &str {
            "text-mock"
        }
        async fn stream(
            &self,
            _req: ProviderRequest,
        ) -> AikitResult<BoxStream<'static, StreamDelta>> {
            let deltas = vec![
                StreamDelta::MessageStart { model: "m".into() },
                StreamDelta::TextDelta {
                    text: self.0.clone(),
                },
                StreamDelta::ProviderMetadata {
                    provider: "openai".into(),
                    metadata: json!({ "service_tier": "test" }),
                },
                StreamDelta::MessageStop {
                    stop_reason: "end_turn".into(),
                },
            ];
            Ok(Box::pin(futures::stream::iter(deltas)))
        }
    }

    struct ErrorAfterValidJsonMock {
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl Provider for ErrorAfterValidJsonMock {
        fn name(&self) -> &str {
            "error-after-valid-json"
        }

        async fn stream(
            &self,
            req: ProviderRequest,
        ) -> AikitResult<BoxStream<'static, StreamDelta>> {
            assert_eq!(
                req.compatibility_mode,
                crate::contract::CompatibilityMode::Warn
            );
            self.calls.fetch_add(1, Ordering::SeqCst);
            let mut info = crate::error::ErrorInfo::new(crate::error::ErrorCode::ProviderRateLimit)
                .with_provider("openai", "gpt-x");
            info.status = Some(429);
            info.retry_after_ms = Some(2_500);
            info.retryable = true;
            Ok(Box::pin(futures::stream::iter(vec![
                StreamDelta::MessageStart { model: "m".into() },
                StreamDelta::Warning {
                    warning: crate::contract::ProviderWarning {
                        code: "unverified_provider_parameter".into(),
                        message: "provider option `future_option` is unverified".into(),
                        parameter: Some("future_option".into()),
                        provider: Some("openai".into()),
                        model: Some("gpt-x".into()),
                    },
                },
                StreamDelta::TextDelta {
                    text: r#"{"total":42,"currency":"USD"}"#.into(),
                },
                StreamDelta::Error {
                    message: "rate limited during response stream".into(),
                    info,
                },
                // The consumer must stop at Error rather than accepting a later terminal marker.
                StreamDelta::MessageStop {
                    stop_reason: "end_turn".into(),
                },
            ])))
        }
    }

    /// A provider that returns a forced tool call with a fixed input (the `ToolCall` source path).
    struct ToolMock {
        name: String,
        input: Value,
    }
    #[async_trait]
    impl Provider for ToolMock {
        fn name(&self) -> &str {
            "tool-mock"
        }
        async fn stream(
            &self,
            _req: ProviderRequest,
        ) -> AikitResult<BoxStream<'static, StreamDelta>> {
            let deltas = vec![
                StreamDelta::MessageStart { model: "m".into() },
                StreamDelta::ToolCallStart {
                    id: "c1".into(),
                    name: self.name.clone(),
                },
                StreamDelta::ToolCallInput {
                    id: "c1".into(),
                    input: self.input.clone(),
                },
                StreamDelta::MessageStop {
                    stop_reason: "tool_use".into(),
                },
            ];
            Ok(Box::pin(futures::stream::iter(deltas)))
        }
    }

    /// Returns invalid JSON on the first call, valid on the second — to exercise repair-retry.
    struct FlakyMock {
        calls: AtomicUsize,
    }
    #[async_trait]
    impl Provider for FlakyMock {
        fn name(&self) -> &str {
            "flaky-mock"
        }
        async fn stream(
            &self,
            _req: ProviderRequest,
        ) -> AikitResult<BoxStream<'static, StreamDelta>> {
            let n = self.calls.fetch_add(1, Ordering::SeqCst);
            let body = if n == 0 {
                r#"{"total": 42}"#.to_string() // missing required "currency"
            } else {
                r#"{"total": 42, "currency": "USD"}"#.to_string()
            };
            let deltas = vec![
                StreamDelta::MessageStart { model: "m".into() },
                StreamDelta::TextDelta { text: body },
                StreamDelta::MessageStop {
                    stop_reason: "end_turn".into(),
                },
            ];
            Ok(Box::pin(futures::stream::iter(deltas)))
        }
    }

    struct LargeRepairMock {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl Provider for LargeRepairMock {
        fn name(&self) -> &str {
            "large-repair-mock"
        }

        async fn stream(
            &self,
            _req: ProviderRequest,
        ) -> AikitResult<BoxStream<'static, StreamDelta>> {
            let attempt = self.calls.fetch_add(1, Ordering::SeqCst);
            let padding = "x".repeat(crate::providers::MAX_STREAM_RETAINED_BYTES / 2 + 4_096);
            let body = if attempt == 0 {
                json!({"total": 42, "padding": padding}).to_string()
            } else {
                json!({"total": 42, "currency": "USD", "padding": padding}).to_string()
            };
            Ok(Box::pin(futures::stream::iter(vec![
                StreamDelta::MessageStart { model: "m".into() },
                StreamDelta::TextDelta { text: body },
                StreamDelta::Warning {
                    warning: crate::contract::ProviderWarning {
                        code: "repair_attempt".into(),
                        message: format!("attempt {}", attempt + 1),
                        parameter: None,
                        provider: Some("openai".into()),
                        model: Some("gpt-x".into()),
                    },
                },
                StreamDelta::ProviderMetadata {
                    provider: "openai".into(),
                    metadata: json!({"attempt": attempt + 1}),
                },
                StreamDelta::MessageStop {
                    stop_reason: "end_turn".into(),
                },
            ])))
        }
    }

    struct NeverCalledMock;

    struct FixedStreamMock {
        calls: Arc<AtomicUsize>,
        deltas: Vec<StreamDelta>,
    }

    #[async_trait]
    impl Provider for FixedStreamMock {
        fn name(&self) -> &str {
            "fixed-stream"
        }

        async fn stream(
            &self,
            _req: ProviderRequest,
        ) -> AikitResult<BoxStream<'static, StreamDelta>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(Box::pin(futures::stream::iter(self.deltas.clone())))
        }
    }

    #[async_trait]
    impl Provider for NeverCalledMock {
        fn name(&self) -> &str {
            "never-called"
        }

        async fn stream(
            &self,
            _req: ProviderRequest,
        ) -> AikitResult<BoxStream<'static, StreamDelta>> {
            panic!("invalid structured-output options reached the provider")
        }
    }

    /// Splits one JSON object across a synchronization gate so a test can prove that callers see
    /// the first delta before the provider has completed the object.
    struct GatedMock {
        release: Arc<tokio::sync::Notify>,
    }

    struct CapturingMock {
        options: Arc<Mutex<Map<String, Value>>>,
    }

    struct MessageCapturingMock {
        messages: Arc<Mutex<Vec<Message>>>,
    }

    #[async_trait]
    impl Provider for CapturingMock {
        fn name(&self) -> &str {
            "capture-mock"
        }

        async fn stream(
            &self,
            req: ProviderRequest,
        ) -> AikitResult<BoxStream<'static, StreamDelta>> {
            *self.options.lock().unwrap() = req.options;
            Ok(Box::pin(futures::stream::iter(vec![
                StreamDelta::MessageStart { model: "m".into() },
                StreamDelta::TextDelta {
                    text: r#"{"total":1,"currency":"USD"}"#.into(),
                },
                StreamDelta::MessageStop {
                    stop_reason: "end_turn".into(),
                },
            ])))
        }
    }

    #[async_trait]
    impl Provider for MessageCapturingMock {
        fn name(&self) -> &str {
            "message-capture-mock"
        }

        async fn stream(
            &self,
            req: ProviderRequest,
        ) -> AikitResult<BoxStream<'static, StreamDelta>> {
            *self.messages.lock().unwrap() = req.messages;
            Ok(Box::pin(futures::stream::iter(vec![
                StreamDelta::MessageStart { model: "m".into() },
                StreamDelta::TextDelta {
                    text: r#"{"total":1,"currency":"USD"}"#.into(),
                },
                StreamDelta::MessageStop {
                    stop_reason: "end_turn".into(),
                },
            ])))
        }
    }

    #[async_trait]
    impl Provider for GatedMock {
        fn name(&self) -> &str {
            "gated-mock"
        }

        async fn stream(
            &self,
            _req: ProviderRequest,
        ) -> AikitResult<BoxStream<'static, StreamDelta>> {
            let release = self.release.clone();
            Ok(Box::pin(async_stream::stream! {
                yield StreamDelta::MessageStart { model: "m".into() };
                yield StreamDelta::TextDelta { text: r#"{"total":42,"#.into() };
                release.notified().await;
                yield StreamDelta::TextDelta { text: r#""currency":"USD"}"#.into() };
                yield StreamDelta::MessageStop { stop_reason: "end_turn".into() };
            }))
        }
    }

    #[test]
    fn plans_the_right_wire_encoding_per_provider() {
        let schema = invoice_schema();
        // OpenAI → strict json_schema response_format, read from text.
        let p = plan_structured(
            "openai",
            "gpt-5.6-sol",
            FidelityGrade::NativeConstrained,
            "invoice",
            &schema,
        );
        assert_eq!(p.options["response_format"]["type"], "json_schema");
        assert_eq!(p.options["response_format"]["json_schema"]["strict"], true);
        assert!(matches!(p.source, ResultSource::Text));
        assert!(p.extra_tools.is_empty());

        // Current Gemini 3 → generationConfig.responseFormat.text.
        let p = plan_structured(
            "google",
            "gemini-3.5-flash",
            FidelityGrade::NativeConstrained,
            "invoice",
            &schema,
        );
        assert_eq!(
            p.options["generationConfig"]["responseFormat"]["text"]["mimeType"],
            "application/json"
        );
        assert_eq!(
            p.options["generationConfig"]["responseFormat"]["text"]["schema"],
            schema
        );

        // Gemini 2.x retains the previous generateContent JSON-Schema fields.
        let legacy = plan_structured(
            "google",
            "gemini-2.5-pro",
            FidelityGrade::NativeConstrained,
            "invoice",
            &schema,
        );
        assert_eq!(
            legacy.options["generationConfig"]["responseMimeType"],
            "application/json"
        );
        assert_eq!(
            legacy.options["generationConfig"]["responseJsonSchema"],
            schema
        );

        // Anthropic → native output_config.format JSON schema, read from text.
        let p = plan_structured(
            "anthropic",
            "claude-opus-4-8",
            FidelityGrade::NativeConstrained,
            "invoice",
            &schema,
        );
        assert_eq!(p.options["output_config"]["format"]["type"], "json_schema");
        assert_eq!(p.options["output_config"]["format"]["schema"], schema);
        assert!(p.extra_tools.is_empty());
        assert!(matches!(p.source, ResultSource::Text));

        // DeepSeek → json_object mode + schema pinned into the prompt.
        let p = plan_structured(
            "deepseek",
            "deepseek-v4-pro",
            FidelityGrade::PromptedAndParsed,
            "invoice",
            &schema,
        );
        assert_eq!(
            p.options["response_format"],
            json!({ "type": "json_object" })
        );
        assert!(p.prompt_suffix.unwrap().contains("currency"));
    }

    #[tokio::test]
    async fn native_constrained_text_path_parses_and_grades() {
        let provider = Arc::new(TextMock(r#"{"total": 9.99, "currency": "EUR"}"#.into()));
        let got = generate_object(
            provider,
            "openai",
            FidelityGrade::NativeConstrained,
            "gpt-x",
            "Extract the invoice",
            &invoice_schema(),
            &ObjectOptions::default(),
        )
        .await
        .unwrap();
        assert_eq!(got.value["currency"], "EUR");
        assert_eq!(got.fidelity, FidelityGrade::NativeConstrained);
        assert_eq!(got.attempts, 1);
        assert_eq!(
            got.provider_metadata["openai"],
            vec![json!({ "service_tier": "test" })]
        );
    }

    #[tokio::test]
    async fn object_provider_options_pass_through_but_cannot_weaken_fidelity() {
        let captured = Arc::new(Mutex::new(Map::new()));
        let mut provider_options = crate::types::ProviderOptions::new();
        provider_options.insert(
            "openai".into(),
            Map::from_iter([
                ("service_tier".into(), json!("flex")),
                (
                    "response_format".into(),
                    json!({
                        "type": "json_schema",
                        "json_schema": {
                            "name": "hostile",
                            "strict": false,
                            "schema": {"not": {}},
                            "unexpected": true
                        }
                    }),
                ),
            ]),
        );
        let options = ObjectOptions {
            provider_options,
            ..ObjectOptions::default()
        };
        generate_object(
            Arc::new(CapturingMock {
                options: captured.clone(),
            }),
            "openai",
            FidelityGrade::NativeConstrained,
            "gpt-x",
            "x",
            &invoice_schema(),
            &options,
        )
        .await
        .unwrap();

        let captured = captured.lock().unwrap();
        assert_eq!(captured["service_tier"], "flex");
        assert_eq!(captured["response_format"]["type"], "json_schema");
        assert_eq!(
            captured["response_format"]["json_schema"]["schema"],
            invoice_schema()
        );
        assert_eq!(
            captured["response_format"]["json_schema"]["name"],
            "respond"
        );
        assert_eq!(captured["response_format"]["json_schema"]["strict"], true);
        assert!(captured["response_format"]["json_schema"]
            .get("unexpected")
            .is_none());
    }

    #[tokio::test]
    async fn anthropic_format_is_replaced_atomically_but_output_controls_survive() {
        let captured = Arc::new(Mutex::new(Map::new()));
        let mut provider_options = crate::types::ProviderOptions::new();
        provider_options.insert(
            "anthropic".into(),
            Map::from_iter([(
                "output_config".into(),
                json!({
                    "effort": "high",
                    "format": {
                        "type": "json_schema",
                        "schema": {"not": {}},
                        "unexpected": true
                    }
                }),
            )]),
        );

        generate_object(
            Arc::new(CapturingMock {
                options: captured.clone(),
            }),
            "anthropic",
            FidelityGrade::NativeConstrained,
            "claude-test",
            "x",
            &invoice_schema(),
            &ObjectOptions {
                provider_options,
                ..ObjectOptions::default()
            },
        )
        .await
        .unwrap();

        let captured = captured.lock().unwrap();
        assert_eq!(captured["output_config"]["effort"], "high");
        assert_eq!(
            captured["output_config"]["format"],
            json!({"type": "json_schema", "schema": invoice_schema()})
        );
    }

    #[tokio::test]
    async fn deepseek_json_object_format_drops_caller_json_schema_variant() {
        let captured = Arc::new(Mutex::new(Map::new()));
        let mut provider_options = crate::types::ProviderOptions::new();
        provider_options.insert(
            "deepseek".into(),
            Map::from_iter([
                ("temperature".into(), json!(0.2)),
                (
                    "response_format".into(),
                    json!({
                        "type": "json_schema",
                        "json_schema": {"schema": {"not": {}}}
                    }),
                ),
            ]),
        );

        generate_object(
            Arc::new(CapturingMock {
                options: captured.clone(),
            }),
            "deepseek",
            FidelityGrade::PromptedAndParsed,
            "deepseek-chat",
            "x",
            &invoice_schema(),
            &ObjectOptions {
                provider_options,
                ..ObjectOptions::default()
            },
        )
        .await
        .unwrap();

        let captured = captured.lock().unwrap();
        assert_eq!(captured["temperature"], 0.2);
        assert_eq!(captured["response_format"], json!({"type": "json_object"}));
    }

    #[tokio::test]
    async fn google_structured_contract_deep_merges_without_dropping_generation_controls() {
        let captured = Arc::new(Mutex::new(Map::new()));
        let mut provider_options = crate::types::ProviderOptions::new();
        provider_options.insert(
            "google".into(),
            Map::from_iter([(
                "generationConfig".into(),
                json!({
                    "temperature": 0.25,
                    "responseMimeType": "text/plain",
                    "responseSchema": {"type": "string"},
                    "responseJsonSchema": {"type": "boolean"},
                    "responseModalities": ["TEXT", "IMAGE"],
                    "responseFormat": {
                        "text": {"mimeType": "text/plain", "schema": {"type": "string"}},
                        "image": {"mimeType": "image/png"},
                        "audio": {"mimeType": "audio/wav"}
                    }
                }),
            )]),
        );
        generate_object(
            Arc::new(CapturingMock {
                options: captured.clone(),
            }),
            "google",
            FidelityGrade::NativeConstrained,
            "gemini-3.5-flash",
            "x",
            &invoice_schema(),
            &ObjectOptions {
                provider_options,
                ..ObjectOptions::default()
            },
        )
        .await
        .unwrap();

        let captured = captured.lock().unwrap();
        assert_eq!(captured["generationConfig"]["temperature"], 0.25);
        assert_eq!(
            captured["generationConfig"]["responseFormat"]["text"]["mimeType"],
            "application/json"
        );
        assert_eq!(
            captured["generationConfig"]["responseFormat"]["text"]["schema"],
            invoice_schema()
        );
        let generation = captured["generationConfig"].as_object().unwrap();
        assert!(!generation.contains_key("responseMimeType"));
        assert!(!generation.contains_key("responseSchema"));
        assert!(!generation.contains_key("responseJsonSchema"));
        assert!(!generation.contains_key("responseModalities"));
        let response_format = generation["responseFormat"].as_object().unwrap();
        assert_eq!(response_format.len(), 1);
        assert!(response_format.contains_key("text"));
    }

    #[tokio::test]
    async fn structured_messages_preserve_media_and_append_schema_instruction() {
        use crate::types::{ContentBlock, MediaSource, Role};

        let captured = Arc::new(Mutex::new(Vec::new()));
        let input = Message {
            role: Role::User,
            content: vec![
                ContentBlock::Text {
                    text: "Extract the invoice".into(),
                },
                ContentBlock::Media {
                    media_type: "image/png".into(),
                    source: MediaSource::Base64 {
                        data: "aGVsbG8=".into(),
                    },
                },
            ],
        };
        let got = generate_object_messages(
            Arc::new(MessageCapturingMock {
                messages: captured.clone(),
            }),
            "deepseek",
            FidelityGrade::PromptedAndParsed,
            "deepseek-chat",
            vec![input],
            &invoice_schema(),
            &ObjectOptions::default(),
        )
        .await
        .unwrap();

        assert_eq!(got.value["currency"], "USD");
        let messages = captured.lock().unwrap();
        assert!(matches!(messages[0].content[1], ContentBlock::Media { .. }));
        assert!(messages[0].content.iter().any(|block| matches!(
            block,
            ContentBlock::Text { text } if text.contains("Respond with ONLY a JSON object")
        )));
    }

    #[tokio::test]
    async fn strips_markdown_fences_before_parsing() {
        let provider = Arc::new(TextMock(
            "```json\n{\"total\": 1, \"currency\": \"GBP\"}\n```".into(),
        ));
        let got = generate_object(
            provider,
            "deepseek",
            FidelityGrade::PromptedAndParsed,
            "deepseek-chat",
            "x",
            &invoice_schema(),
            &ObjectOptions {
                compatibility_mode: crate::contract::CompatibilityMode::Warn,
                ..ObjectOptions::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(got.value["currency"], "GBP");
        assert_eq!(got.fidelity, FidelityGrade::PromptedAndParsed);
    }

    #[test]
    fn markdown_fence_with_trailing_prose_is_not_silently_accepted() {
        let response = "```json\n{\"total\": 42, \"currency\": \"TRY\"}\n```\nextra prose";
        assert_eq!(strip_fences(response), response);
        assert!(serde_json::from_str::<Value>(strip_fences(response)).is_err());
    }

    #[tokio::test]
    async fn forced_tool_call_path_reads_the_tool_input() {
        let provider = Arc::new(ToolMock {
            name: "respond".into(),
            input: json!({ "total": 5, "currency": "TRY" }),
        });
        let got = generate_object(
            provider,
            "anthropic",
            FidelityGrade::ForcedToolCall,
            "claude-opus-4-8",
            "x",
            &invoice_schema(),
            &ObjectOptions::default(),
        )
        .await
        .unwrap();
        assert_eq!(got.value["currency"], "TRY");
        assert_eq!(got.fidelity, FidelityGrade::ForcedToolCall);
    }

    #[tokio::test]
    async fn retries_on_validation_error_then_succeeds() {
        let provider = Arc::new(FlakyMock {
            calls: AtomicUsize::new(0),
        });
        let got = generate_object(
            provider,
            "openai",
            FidelityGrade::NativeConstrained,
            "gpt-x",
            "x",
            &invoice_schema(),
            &ObjectOptions::default(),
        )
        .await
        .unwrap();
        // First attempt missed "currency"; the repair round-trip fixed it.
        assert_eq!(got.attempts, 2);
        assert_eq!(got.value["currency"], "USD");
    }

    #[tokio::test]
    async fn stream_object_forwards_a_delta_before_the_object_finishes() {
        let release = Arc::new(tokio::sync::Notify::new());
        let mut stream = stream_object(
            Arc::new(GatedMock {
                release: release.clone(),
            }),
            "openai",
            FidelityGrade::NativeConstrained,
            "gpt-x",
            "x",
            &invoice_schema(),
            &ObjectOptions::default(),
        );

        assert!(matches!(
            stream.next().await.unwrap().unwrap(),
            ObjectStreamEvent::AttemptStarted {
                attempt: 1,
                repair: false,
                ..
            }
        ));
        assert!(matches!(
            stream.next().await.unwrap().unwrap(),
            ObjectStreamEvent::Delta {
                delta: StreamDelta::MessageStart { .. },
                ..
            }
        ));
        let first_text = tokio::time::timeout(Duration::from_millis(100), stream.next())
            .await
            .expect("the first text delta must not wait for the complete JSON object")
            .unwrap()
            .unwrap();
        assert!(matches!(
            first_text,
            ObjectStreamEvent::Delta {
                delta: StreamDelta::TextDelta { ref text },
                ..
            } if text == r#"{"total":42,"#
        ));

        release.notify_one();
        let mut completed = None;
        while let Some(event) = stream.next().await {
            if let ObjectStreamEvent::Completed { object } = event.unwrap() {
                completed = Some(object);
            }
        }
        let object = completed.expect("validated completion event");
        assert_eq!(object.value["currency"], "USD");
        assert_eq!(object.attempts, 1);
    }

    #[tokio::test]
    async fn stream_error_after_valid_json_fails_closed_with_typed_provider_info() {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut stream = stream_object(
            Arc::new(ErrorAfterValidJsonMock {
                calls: calls.clone(),
            }),
            "openai",
            FidelityGrade::NativeConstrained,
            "gpt-x",
            "x",
            &invoice_schema(),
            &ObjectOptions {
                compatibility_mode: crate::contract::CompatibilityMode::Warn,
                ..ObjectOptions::default()
            },
        );
        let mut saw_error_delta = false;
        let mut terminal_error = None;
        while let Some(event) = stream.next().await {
            match event {
                Ok(ObjectStreamEvent::Delta {
                    delta: StreamDelta::Error { .. },
                    ..
                }) => saw_error_delta = true,
                Ok(ObjectStreamEvent::Completed { .. }) => {
                    panic!("valid partial output must not complete after a provider stream error")
                }
                Err(error) => terminal_error = Some(error),
                _ => {}
            }
        }

        assert!(
            saw_error_delta,
            "the original provider error delta stays observable"
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "provider errors are not validation retries"
        );
        let terminal_error =
            terminal_error.expect("provider stream error must terminate the object stream");
        let provider_error = terminal_error
            .provider_error()
            .expect("provider classification must survive structured-output handling");
        assert_eq!(provider_error.provider, "openai");
        assert_eq!(provider_error.model, "gpt-x");
        assert_eq!(
            provider_error.kind,
            crate::error::ProviderErrorKind::RateLimited
        );
        assert_eq!(provider_error.status, Some(429));
        assert_eq!(provider_error.retry_after_ms, Some(2_500));
        assert!(provider_error.retryable());
        assert_eq!(provider_error.warnings.len(), 1);
        assert_eq!(
            provider_error.warnings[0].parameter.as_deref(),
            Some("future_option")
        );
    }

    #[tokio::test]
    async fn valid_json_without_message_stop_is_a_protocol_error_not_a_repair_retry() {
        let calls = Arc::new(AtomicUsize::new(0));
        let error = generate_object(
            Arc::new(FixedStreamMock {
                calls: calls.clone(),
                deltas: vec![
                    StreamDelta::MessageStart { model: "m".into() },
                    StreamDelta::TextDelta {
                        text: r#"{"total":42,"currency":"USD"}"#.into(),
                    },
                ],
            }),
            "openai",
            FidelityGrade::NativeConstrained,
            "gpt-x",
            "x",
            &invoice_schema(),
            &ObjectOptions::default(),
        )
        .await
        .unwrap_err();

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(matches!(
            error.provider_error(),
            Some(error)
                if error.kind == crate::error::ProviderErrorKind::Protocol
                    && error.message.contains("MessageStop")
        ));
    }

    #[tokio::test]
    async fn forced_tool_input_before_start_is_a_protocol_error_not_a_validation_retry() {
        let calls = Arc::new(AtomicUsize::new(0));
        let error = generate_object(
            Arc::new(FixedStreamMock {
                calls: calls.clone(),
                deltas: vec![
                    StreamDelta::MessageStart { model: "m".into() },
                    StreamDelta::ToolCallInput {
                        id: "c1".into(),
                        input: json!({"total":42,"currency":"USD"}),
                    },
                    StreamDelta::MessageStop {
                        stop_reason: "tool_use".into(),
                    },
                ],
            }),
            "anthropic",
            FidelityGrade::ForcedToolCall,
            "claude-x",
            "x",
            &invoice_schema(),
            &ObjectOptions::default(),
        )
        .await
        .unwrap_err();

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(matches!(
            error.provider_error(),
            Some(error)
                if error.kind == crate::error::ProviderErrorKind::Protocol
                    && error.message.contains("before starting")
        ));
    }

    #[tokio::test]
    async fn structured_text_retention_is_bounded_across_provider_deltas() {
        let options = ObjectOptions {
            max_retries: 0,
            ..ObjectOptions::default()
        };
        let error = generate_object(
            Arc::new(TextMock(
                "x".repeat(crate::providers::MAX_STREAM_RETAINED_BYTES + 1),
            )),
            "openai",
            FidelityGrade::NativeConstrained,
            "gpt-x",
            "x",
            &invoice_schema(),
            &options,
        )
        .await
        .unwrap_err();

        assert!(matches!(
            error.provider_error(),
            Some(error)
                if error.kind == crate::error::ProviderErrorKind::Protocol
                    && error.message.contains("retained state limit")
        ));
    }

    #[tokio::test]
    async fn rejected_candidate_retention_is_released_before_the_repair_attempt() {
        let provider = Arc::new(LargeRepairMock {
            calls: AtomicUsize::new(0),
        });
        let options = ObjectOptions {
            max_retries: 1,
            ..ObjectOptions::default()
        };

        let object = generate_object(
            provider.clone(),
            "openai",
            FidelityGrade::NativeConstrained,
            "gpt-x",
            "x",
            &invoice_schema(),
            &options,
        )
        .await
        .expect("each candidate is independently below the retention limit");

        assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
        assert_eq!(object.attempts, 2);
        assert_eq!(
            object.value["padding"].as_str().unwrap().len(),
            crate::providers::MAX_STREAM_RETAINED_BYTES / 2 + 4_096
        );
        assert_eq!(object.warnings.len(), 2);
        assert_eq!(object.provider_metadata["openai"].len(), 2);
    }

    #[tokio::test]
    async fn stream_object_surfaces_validation_and_real_repair_attempts() {
        let mut stream = stream_object(
            Arc::new(FlakyMock {
                calls: AtomicUsize::new(0),
            }),
            "openai",
            FidelityGrade::NativeConstrained,
            "gpt-x",
            "x",
            &invoice_schema(),
            &ObjectOptions::default(),
        );
        let mut saw_validation_failure = false;
        let mut saw_repair = false;
        let mut completed = None;
        while let Some(event) = stream.next().await {
            match event.unwrap() {
                ObjectStreamEvent::ValidationFailed {
                    attempt: 1,
                    will_retry: true,
                    error,
                } => {
                    assert!(error.contains("currency"));
                    saw_validation_failure = true;
                }
                ObjectStreamEvent::AttemptStarted {
                    attempt: 2,
                    repair: true,
                    ..
                } => saw_repair = true,
                ObjectStreamEvent::Completed { object } => completed = Some(object),
                _ => {}
            }
        }

        assert!(saw_validation_failure);
        assert!(saw_repair);
        let object = completed.expect("repair should eventually validate");
        assert_eq!(object.attempts, 2);
        assert_eq!(object.value["currency"], "USD");
    }

    #[tokio::test]
    async fn semantic_validator_runs_only_after_schema_validation() {
        let calls = Arc::new(AtomicUsize::new(0));
        let validator_calls = calls.clone();
        let options = ObjectOptions {
            semantic_validator: Some(Arc::new(move |_value: Value| {
                let validator_calls = validator_calls.clone();
                async move {
                    validator_calls.fetch_add(1, Ordering::SeqCst);
                    Ok(SemanticValidation::Accept)
                }
            })),
            ..ObjectOptions::default()
        };
        let result = generate_object(
            Arc::new(FlakyMock {
                calls: AtomicUsize::new(0),
            }),
            "openai",
            FidelityGrade::NativeConstrained,
            "gpt-x",
            "x",
            &invoice_schema(),
            &options,
        )
        .await
        .unwrap();

        assert_eq!(result.attempts, 2);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn semantic_retry_uses_the_bounded_repair_stream() {
        let calls = Arc::new(AtomicUsize::new(0));
        let validator_calls = calls.clone();
        let options = ObjectOptions {
            max_retries: 1,
            semantic_validator: Some(Arc::new(move |_value: Value| {
                let call = validator_calls.fetch_add(1, Ordering::SeqCst);
                async move {
                    if call == 0 {
                        Ok(SemanticValidation::Retry("currency policy mismatch".into()))
                    } else {
                        Ok(SemanticValidation::Accept)
                    }
                }
            })),
            ..ObjectOptions::default()
        };
        let mut stream = stream_object(
            Arc::new(TextMock(r#"{"total": 9.99, "currency": "EUR"}"#.into())),
            "openai",
            FidelityGrade::NativeConstrained,
            "gpt-x",
            "x",
            &invoice_schema(),
            &options,
        );
        let mut saw_semantic_retry = false;
        let mut completed = None;
        while let Some(event) = stream.next().await {
            match event.unwrap() {
                ObjectStreamEvent::ValidationFailed {
                    attempt: 1,
                    will_retry: true,
                    error,
                } => {
                    assert!(error.contains("currency policy mismatch"));
                    saw_semantic_retry = true;
                }
                ObjectStreamEvent::Completed { object } => completed = Some(object),
                _ => {}
            }
        }

        assert!(saw_semantic_retry);
        assert_eq!(completed.unwrap().attempts, 2);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn semantic_reject_is_immediate_bounded_and_does_not_echo_the_object() {
        use crate::observability::{AuditEvent, AuditTrail, InMemoryAuditSink};

        let secret = "RAW_OBJECT_SECRET_MUST_NOT_LEAK";
        let sink = Arc::new(InMemoryAuditSink::default());
        let audit = AuditTrail::new().with_sink(sink.clone());
        let calls = Arc::new(AtomicUsize::new(0));
        let validator_calls = calls.clone();
        let options = ObjectOptions {
            max_retries: 4,
            semantic_validator: Some(Arc::new(move |_value: Value| {
                validator_calls.fetch_add(1, Ordering::SeqCst);
                async move {
                    Ok(SemanticValidation::Reject(format!(
                        "visible\u{061c}\u{202e}spoof{}",
                        "x".repeat(10_000)
                    )))
                }
            })),
            ..ObjectOptions::default()
        };
        let error = generate_object_observed(
            Arc::new(TextMock(format!(
                r#"{{"total": 9.99, "currency": "EUR", "secret": "{secret}"}}"#
            ))),
            "openai",
            FidelityGrade::NativeConstrained,
            "gpt-x",
            "x",
            &invoice_schema(),
            &options,
            Some(&audit),
        )
        .await
        .unwrap_err();

        match error {
            AikitError::StructuredOutput(message) => {
                assert!(message.starts_with("semantic validator rejected object:"));
                let reason = message
                    .strip_prefix("semantic validator rejected object: ")
                    .unwrap();
                assert!(reason.len() <= MAX_SEMANTIC_VALIDATION_REASON_BYTES);
                assert!(!reason.contains('\u{061c}'));
                assert!(!reason.contains('\u{202e}'));
                assert!(!message.contains(secret));
            }
            other => panic!("expected structured-output rejection, got {other:?}"),
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(sink.records().iter().any(|record| matches!(
            record.event,
            AuditEvent::StructuredOutputValidationFailed { .. }
        )));
        let audit_json = serde_json::to_string(&sink.records()).unwrap();
        assert!(!audit_json.contains(secret));
        assert!(!audit_json.contains("visible"));
    }

    #[tokio::test]
    async fn semantic_retry_audit_records_only_the_bounded_reason() {
        use crate::observability::{AuditTrail, InMemoryAuditSink};

        let secret = "RAW_SEMANTIC_OBJECT_AUDIT_SECRET";
        let sink = Arc::new(InMemoryAuditSink::default());
        let audit = AuditTrail::new().with_sink(sink.clone());
        let options = ObjectOptions {
            max_retries: 0,
            semantic_validator: Some(Arc::new(|_value: Value| async move {
                Ok(SemanticValidation::Retry(
                    "business invariant failed".into(),
                ))
            })),
            ..ObjectOptions::default()
        };
        let error = generate_object_observed(
            Arc::new(TextMock(format!(
                r#"{{"total": 9.99, "currency": "EUR", "secret": "{secret}"}}"#
            ))),
            "openai",
            FidelityGrade::NativeConstrained,
            "gpt-x",
            "x",
            &invoice_schema(),
            &options,
            Some(&audit),
        )
        .await
        .unwrap_err();

        assert!(!error.to_string().contains(secret));
        let audit_json = serde_json::to_string(&sink.records()).unwrap();
        assert!(audit_json.contains("business invariant failed"));
        assert!(!audit_json.contains(secret));
    }

    #[tokio::test]
    async fn semantic_validator_errors_and_panics_fail_closed() {
        use crate::observability::{AuditEvent, AuditTrail, InMemoryAuditSink};

        let secret = "RAW_TERMINAL_SEMANTIC_AUDIT_SECRET";
        let error_sink = Arc::new(InMemoryAuditSink::default());
        let error_audit = AuditTrail::new().with_sink(error_sink.clone());
        let error_options = ObjectOptions {
            semantic_validator: Some(Arc::new(|_value: Value| async move {
                Err("host callback exception".to_string())
            })),
            ..ObjectOptions::default()
        };
        let error = generate_object_observed(
            Arc::new(TextMock(format!(
                r#"{{"total":9.99,"currency":"EUR","secret":"{secret}"}}"#
            ))),
            "openai",
            FidelityGrade::NativeConstrained,
            "gpt-x",
            "x",
            &invoice_schema(),
            &error_options,
            Some(&error_audit),
        )
        .await
        .unwrap_err();
        assert!(matches!(
            error,
            AikitError::StructuredOutput(message) if message.contains("failed closed")
        ));
        assert!(error_sink.records().iter().any(|record| matches!(
            record.event,
            AuditEvent::StructuredOutputValidationFailed { .. }
        )));
        let error_audit_json = serde_json::to_string(&error_sink.records()).unwrap();
        assert!(!error_audit_json.contains(secret));
        assert!(!error_audit_json.contains("host callback exception"));

        let panic_sink = Arc::new(InMemoryAuditSink::default());
        let panic_audit = AuditTrail::new().with_sink(panic_sink.clone());
        let panic_options = ObjectOptions {
            semantic_validator: Some(Arc::new(|_value: Value| async move {
                panic!("validator panic payload");
                #[allow(unreachable_code)]
                Ok(SemanticValidation::Accept)
            })),
            ..ObjectOptions::default()
        };
        let error = generate_object_observed(
            Arc::new(TextMock(r#"{"total": 9.99, "currency": "EUR"}"#.into())),
            "openai",
            FidelityGrade::NativeConstrained,
            "gpt-x",
            "x",
            &invoice_schema(),
            &panic_options,
            Some(&panic_audit),
        )
        .await
        .unwrap_err();
        assert!(matches!(
            error,
            AikitError::StructuredOutput(message) if message.contains("panicked")
        ));
        assert!(panic_sink.records().iter().any(|record| matches!(
            record.event,
            AuditEvent::StructuredOutputValidationFailed { .. }
        )));
    }

    #[tokio::test]
    async fn stream_object_reports_terminal_validation_failure_then_errors() {
        let options = ObjectOptions {
            max_retries: 0,
            ..ObjectOptions::default()
        };
        let mut stream = stream_object(
            Arc::new(TextMock(r#"{"total":1}"#.into())),
            "openai",
            FidelityGrade::NativeConstrained,
            "gpt-x",
            "x",
            &invoice_schema(),
            &options,
        );
        let mut terminal_validation_event = false;
        let mut terminal_error = None;
        while let Some(event) = stream.next().await {
            match event {
                Ok(ObjectStreamEvent::ValidationFailed {
                    attempt: 1,
                    will_retry: false,
                    ..
                }) => terminal_validation_event = true,
                Err(error) => terminal_error = Some(error),
                _ => {}
            }
        }
        assert!(terminal_validation_event);
        assert!(matches!(
            terminal_error,
            Some(AikitError::StructuredOutput(message)) if message.contains("currency")
        ));
    }

    #[tokio::test]
    async fn gives_up_with_a_typed_error_after_retries() {
        let provider = Arc::new(TextMock(r#"{"total": 1}"#.into())); // never has "currency"
        let opts = ObjectOptions {
            max_retries: 1,
            ..ObjectOptions::default()
        };
        let err = generate_object(
            provider,
            "openai",
            FidelityGrade::NativeConstrained,
            "gpt-x",
            "x",
            &invoice_schema(),
            &opts,
        )
        .await
        .unwrap_err();
        assert!(matches!(err, AikitError::StructuredOutput(m) if m.contains("currency")));
    }

    #[tokio::test]
    async fn excessive_retry_counts_fail_before_calling_the_provider() {
        for max_retries in [MAX_STRUCTURED_OUTPUT_RETRIES + 1, u32::MAX] {
            let options = ObjectOptions {
                max_retries,
                ..ObjectOptions::default()
            };
            let error = generate_object(
                Arc::new(NeverCalledMock),
                "openai",
                FidelityGrade::NativeConstrained,
                "gpt-x",
                "x",
                &invoice_schema(),
                &options,
            )
            .await
            .unwrap_err();
            assert!(matches!(
                error,
                AikitError::Configuration(message) if message.contains("max_retries")
            ));
        }
    }

    #[tokio::test]
    async fn observed_generation_audits_attempt_and_fidelity() {
        use crate::observability::{AuditEvent, AuditTrail, InMemoryAuditSink};

        let sink = Arc::new(InMemoryAuditSink::default());
        let audit = AuditTrail::new().with_sink(sink.clone());
        let got = generate_object_observed(
            Arc::new(TextMock(r#"{"total": 9.99, "currency": "EUR"}"#.into())),
            "openai",
            FidelityGrade::NativeConstrained,
            "gpt-x",
            "Extract the invoice",
            &invoice_schema(),
            &ObjectOptions::default(),
            Some(&audit),
        )
        .await
        .unwrap();
        assert_eq!(got.attempts, 1);
        let records = sink.records();
        assert!(matches!(
            records[0].event,
            AuditEvent::StructuredOutputAttempt {
                attempt: 1,
                ref fidelity
            } if fidelity == "native_constrained"
        ));
        assert!(matches!(
            records[1].event,
            AuditEvent::StructuredOutputCompleted { attempts: 1, .. }
        ));
    }

    #[test]
    fn validator_checks_types_required_and_nesting() {
        let schema = json!({
            "type": "object",
            "required": ["items"],
            "properties": {
                "items": { "type": "array", "items": { "type": "string" } }
            }
        });
        assert!(validate(&schema, &json!({ "items": ["a", "b"] })).is_ok());
        assert!(validate(&schema, &json!({ "items": [1, 2] })).is_err()); // wrong item type
        assert!(validate(&schema, &json!({})).is_err()); // missing required
    }

    #[test]
    fn validator_enforces_constraints_beyond_the_old_subset() {
        let schema = json!({
            "type": "object",
            "additionalProperties": false,
            "required": ["currency", "amount"],
            "properties": {
                "currency": { "enum": ["EUR", "TRY"] },
                "amount": { "anyOf": [
                    { "type": "integer", "minimum": 1 },
                    { "type": "string", "pattern": "^[1-9][0-9]*$" }
                ] }
            }
        });
        assert!(validate(&schema, &json!({ "currency": "EUR", "amount": 2 })).is_ok());
        assert!(validate(&schema, &json!({ "currency": "USD", "amount": 2 })).is_err());
        assert!(validate(
            &schema,
            &json!({ "currency": "EUR", "amount": 2, "extra": true })
        )
        .is_err());
        assert!(validate(&schema, &json!({ "currency": "EUR", "amount": 0 })).is_err());
    }
}
