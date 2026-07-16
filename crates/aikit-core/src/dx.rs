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
use futures::stream::BoxStream;
use futures::StreamExt;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::{json, Map, Value};
use std::collections::HashMap;
use std::sync::Arc;

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
}

/// One observable step of [`stream_object`]. Provider deltas are forwarded as they arrive; the
/// final object is emitted only after complete JSON parsing and JSON-Schema validation.
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
    /// The completed candidate could not be parsed or did not satisfy the schema.
    ValidationFailed {
        attempt: u32,
        error: String,
        will_retry: bool,
    },
    /// The first fully parsed and schema-validated result.
    Completed { object: GeneratedObject },
}

/// A fallible incremental structured-output stream. Transport/audit failures and exhausted
/// validation repairs are errors; validation failures that can be repaired are also surfaced as
/// [`ObjectStreamEvent::ValidationFailed`] before the next attempt begins.
pub type ObjectStream = BoxStream<'static, Result<ObjectStreamEvent>>;

/// Tunables for [`generate_object`].
#[derive(Debug, Clone)]
pub struct ObjectOptions {
    /// Extra validation-repair round-trips after the first attempt.
    pub max_retries: u32,
    /// Output-token ceiling per attempt.
    pub max_tokens: u64,
    /// The tool/schema name used for forced-tool-call and `json_schema` encodings.
    pub name: String,
    /// Provider-keyed native options for the selected provider. Structured-output contract fields
    /// are applied after these options, so a caller cannot accidentally override the schema or
    /// weaken the reported fidelity grade.
    pub provider_options: crate::types::ProviderOptions,
}

impl Default for ObjectOptions {
    fn default() -> Self {
        ObjectOptions {
            max_retries: 2,
            max_tokens: 1024,
            name: "respond".into(),
            provider_options: crate::types::ProviderOptions::new(),
        }
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
                options.insert(
                    "generationConfig".into(),
                    json!({ "responseMimeType": "application/json", "responseJsonSchema": schema }),
                );
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
    let plan = plan_structured(provider_name, grade, &options.name, schema);
    let base_messages = match &plan.prompt_suffix {
        Some(s) => append_user_instruction(messages, s.clone()),
        None => validate_object_messages(messages),
    };
    let model = model.to_string();
    let provider_name = provider_name.to_string();
    let schema = schema.clone();
    let options = options.clone();
    let audit = audit.cloned();
    let total_attempts = options.max_retries + 1;
    Box::pin(async_stream::try_stream! {
        let base_messages = base_messages?;
        let mut last_error = String::new();
        let mut provider_metadata = crate::types::ProviderMetadata::new();
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
            wire_options.extend(plan.options.clone());
            let req = ProviderRequest {
                model: model.clone(),
                messages,
                tools: plan.extra_tools.clone(),
                max_tokens: options.max_tokens,
                options: wire_options,
                provider_options: crate::types::ProviderOptions::new(),
            };

            let mut provider_stream = provider.stream(req).await?;
            let mut text = String::new();
            let mut names: HashMap<String, String> = HashMap::new();
            let mut tool_inputs: Vec<(String, Value)> = Vec::new();
            while let Some(delta) = provider_stream.next().await {
                match &delta {
                    StreamDelta::TextDelta { text: part } => text.push_str(part),
                    StreamDelta::ToolCallStart { id, name } => {
                        names.insert(id.clone(), name.clone());
                    }
                    StreamDelta::ToolCallInput { id, input } => {
                        let name = names.get(id).cloned().unwrap_or_default();
                        tool_inputs.push((name, input.clone()));
                    }
                    StreamDelta::ProviderMetadata { provider, metadata } => {
                        provider_metadata
                            .entry(provider.clone())
                            .or_default()
                            .push(metadata.clone());
                    }
                    _ => {}
                }
                yield ObjectStreamEvent::Delta { attempt, delta };
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

            let validated = candidate.and_then(|value| {
                validate(&schema, &value)?;
                Ok(value)
            });
            match validated {
                Ok(value) => {
                    let object = GeneratedObject {
                        value,
                        fidelity: grade,
                        attempts: attempt,
                        provider_metadata,
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
    })
}

/// Strip a ```json … ``` (or bare ``` … ```) fence some providers wrap JSON in.
fn strip_fences(s: &str) -> &str {
    let t = s.trim();
    if let Some(rest) = t.strip_prefix("```") {
        let rest = rest.strip_prefix("json").unwrap_or(rest);
        let rest = rest.trim_start_matches(['\n', '\r']);
        if let Some(end) = rest.rfind("```") {
            return rest[..end].trim();
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
            FidelityGrade::NativeConstrained,
            "invoice",
            &schema,
        );
        assert_eq!(p.options["response_format"]["type"], "json_schema");
        assert_eq!(p.options["response_format"]["json_schema"]["strict"], true);
        assert!(matches!(p.source, ResultSource::Text));
        assert!(p.extra_tools.is_empty());

        // Google → generationConfig.responseJsonSchema (JSON Schema, not the older OpenAPI type).
        let p = plan_structured(
            "google",
            FidelityGrade::NativeConstrained,
            "invoice",
            &schema,
        );
        assert_eq!(
            p.options["generationConfig"]["responseMimeType"],
            "application/json"
        );
        assert_eq!(p.options["generationConfig"]["responseJsonSchema"], schema);

        // Anthropic → native output_config.format JSON schema, read from text.
        let p = plan_structured(
            "anthropic",
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
                ("response_format".into(), json!({ "type": "text" })),
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
            &ObjectOptions::default(),
        )
        .await
        .unwrap();
        assert_eq!(got.value["currency"], "GBP");
        assert_eq!(got.fidelity, FidelityGrade::PromptedAndParsed);
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
