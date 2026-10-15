//! Offline validation for sanitized provider HTTP cassettes.
//!
//! Cassettes are evidence, not mocks hidden inside adapter tests. The validator deliberately
//! accepts only a small header allowlist, verifies complete scenario coverage, and checks the
//! normalized stream lifecycle before any fixture can be used as parity proof.
//!
//! The checked-in evidence lives under `crates/aikit-core/cassettes/providers/v1/`, one
//! `<provider>.json` per entry in [`SUPPORTED_PROVIDERS`], each covering every
//! [`CassetteScenario::REQUIRED`] scenario exactly once. Verify the whole set offline with:
//!
//! ```text
//! cargo test -p aikit-runtime-core --test provider_cassettes --locked
//! ```
//!
//! Validation fails closed. Every [`CassetteValidationError::Invalid`] names the cassette and
//! interaction (`provider:model/interaction-id`) and says what to change; messages never echo a
//! suspected credential value, only the marker or field name that triggered the rejection.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use thiserror::Error;

/// The only `schema_version` this validator accepts.
pub const CASSETTE_SCHEMA_VERSION: u32 = 1;
/// Providers that must each have exactly one cassette; mirrors
/// [`crate::credentials::KNOWN_PROVIDERS`].
pub const SUPPORTED_PROVIDERS: [&str; 8] = [
    "anthropic",
    "openai",
    "google",
    "deepseek",
    "openrouter",
    "groq",
    "mistral",
    "xai",
];

const ALLOWED_HEADERS: [&str; 6] = [
    "accept",
    "anthropic-version",
    "content-type",
    "retry-after",
    "user-agent",
    "x-request-id",
];
const SECRET_HEADERS: [&str; 5] = [
    "authorization",
    "proxy-authorization",
    "x-api-key",
    "x-goog-api-key",
    "api-key",
];
const SECRET_BODY_FIELDS: [&str; 8] = [
    "api_key",
    "apikey",
    "access_token",
    "client_secret",
    "credential",
    "password",
    "secret",
    "token",
];

/// The behaviour an interaction proves. Serialized in `snake_case` (e.g. `"structured_output"`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CassetteScenario {
    /// A non-streaming text completion with a 2xx HTTP exchange.
    Text,
    /// A 2xx streaming exchange plus its normalized `response_start` … `response_end` events.
    Streaming,
    /// A 2xx exchange that exercises tool calling.
    Tool,
    /// A 2xx exchange that exercises structured (JSON-schema) output.
    StructuredOutput,
    /// An HTTP status >= 400 with a matching typed error classification.
    ProviderError,
    /// A request rejected before any network call because of a named unsupported parameter.
    Unsupported,
}

impl CassetteScenario {
    /// Every cassette must contain each of these scenarios exactly once.
    pub const REQUIRED: [Self; 6] = [
        Self::Text,
        Self::Streaming,
        Self::Tool,
        Self::StructuredOutput,
        Self::ProviderError,
        Self::Unsupported,
    ];
}

/// One provider's sanitized evidence file. Unknown JSON fields are rejected.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderCassette {
    pub schema_version: u32,
    pub fixture_version: String,
    pub provider: String,
    pub model: String,
    pub sanitized: bool,
    pub source: CassetteSource,
    pub interactions: Vec<CassetteInteraction>,
}

/// Where the recorded shape came from: the adapter contract name and the 40-character lowercase
/// git SHA it was recorded against.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CassetteSource {
    pub adapter_contract: String,
    pub reference_commit: String,
}

/// A single scenario's evidence. `request`/`response` are required when `network_performed` is
/// true and forbidden otherwise.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CassetteInteraction {
    pub id: String,
    pub scenario: CassetteScenario,
    pub provider: String,
    pub model: String,
    pub network_performed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request: Option<RecordedHttpRequest>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response: Option<RecordedHttpResponse>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub normalized_events: Vec<RecordedStreamEvent>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub typed_error: Option<RecordedTypedError>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unsupported_parameter: Option<String>,
    pub assertions: Vec<String>,
}

/// A sanitized outbound request. Only allowlisted headers may be stored; the provider's auth
/// header must instead be named in `redacted_headers` (`x-api-key` for Anthropic,
/// `x-goog-api-key` for Google, `authorization` for the rest).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordedHttpRequest {
    pub method: String,
    pub url: String,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    #[serde(default)]
    pub redacted_headers: Vec<String>,
    pub body: Value,
}

/// A sanitized provider response.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordedHttpResponse {
    pub status: u16,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    pub body: Value,
}

/// One normalized stream event; `sequence` must be contiguous from zero.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordedStreamEvent {
    pub sequence: u64,
    pub event_id: String,
    pub event_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub block_id: Option<String>,
}

/// The typed error aikit is expected to surface for a failure scenario.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordedTypedError {
    pub code: String,
    pub kind: String,
    pub provider: String,
    pub model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<u16>,
    pub retryable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_after_ms: Option<u64>,
    pub message: String,
}

/// Summary returned by [`load_and_validate_directory`] when every cassette passed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CassetteValidationReport {
    pub providers: Vec<String>,
    pub cassette_count: usize,
    pub interaction_count: usize,
}

/// Why a cassette (or cassette directory) was rejected.
#[derive(Debug, Error)]
pub enum CassetteValidationError {
    /// The file or directory could not be read.
    #[error("cannot read provider cassette at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    /// The file is not valid JSON for [`ProviderCassette`] (including unknown fields).
    #[error("invalid provider cassette JSON at {path}: {source}")]
    Json {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    /// The cassette parsed but broke a sanitization, coverage, or lifecycle rule. `context` is
    /// `provider:model[/interaction-id]` or a path.
    #[error("invalid provider cassette ({context}): {message}")]
    Invalid { context: String, message: String },
}

fn invalid(context: impl Into<String>, message: impl Into<String>) -> CassetteValidationError {
    CassetteValidationError::Invalid {
        context: context.into(),
        message: message.into(),
    }
}

/// Read and parse one cassette file without validating it; pair with [`validate_cassette`].
pub fn load_cassette(path: impl AsRef<Path>) -> Result<ProviderCassette, CassetteValidationError> {
    let path = path.as_ref();
    let bytes = fs::read(path).map_err(|source| CassetteValidationError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    serde_json::from_slice(&bytes).map_err(|source| CassetteValidationError::Json {
        path: path.to_path_buf(),
        source,
    })
}

/// Validate one parsed cassette: identity, sanitization, scenario coverage, and per-scenario
/// evidence. Returns the first violation found.
pub fn validate_cassette(cassette: &ProviderCassette) -> Result<(), CassetteValidationError> {
    let context = format!("{}:{}", cassette.provider, cassette.model);
    if cassette.schema_version != CASSETTE_SCHEMA_VERSION {
        return Err(invalid(
            &context,
            format!(
                "schema_version must be {CASSETTE_SCHEMA_VERSION}, got {}",
                cassette.schema_version
            ),
        ));
    }
    if cassette.fixture_version.trim().is_empty() {
        return Err(invalid(
            &context,
            "fixture_version must be pinned to a non-empty value",
        ));
    }
    if !SUPPORTED_PROVIDERS.contains(&cassette.provider.as_str()) {
        return Err(invalid(
            &context,
            format!(
                "provider is outside the supported set; expected one of: {}",
                SUPPORTED_PROVIDERS.join(", ")
            ),
        ));
    }
    if cassette.model.trim().is_empty() {
        return Err(invalid(&context, "model identity must be present"));
    }
    if !cassette.sanitized {
        return Err(invalid(
            &context,
            "cassette must be marked sanitized (set \"sanitized\": true only after removing credentials)",
        ));
    }
    if cassette.source.adapter_contract.trim().is_empty()
        || cassette.source.reference_commit.trim().is_empty()
    {
        return Err(invalid(
            &context,
            "source.adapter_contract and source.reference_commit must be pinned",
        ));
    }
    if cassette.source.reference_commit.len() != 40
        || !cassette
            .source
            .reference_commit
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(invalid(
            &context,
            "source reference_commit must be a 40-character lowercase git SHA",
        ));
    }
    for value in [
        cassette.fixture_version.as_str(),
        cassette.provider.as_str(),
        cassette.model.as_str(),
        cassette.source.adapter_contract.as_str(),
        cassette.source.reference_commit.as_str(),
    ] {
        scan_string_for_secrets(value, &context)?;
    }

    let mut ids = BTreeSet::new();
    let mut scenarios = BTreeSet::new();
    for interaction in &cassette.interactions {
        let interaction_context = format!("{context}/{}", interaction.id);
        if interaction.id.trim().is_empty() || !ids.insert(interaction.id.as_str()) {
            return Err(invalid(
                &interaction_context,
                "interaction id is empty or duplicated",
            ));
        }
        if !scenarios.insert(interaction.scenario) {
            return Err(invalid(
                &interaction_context,
                format!("scenario {:?} is duplicated", interaction.scenario),
            ));
        }
        validate_interaction(cassette, interaction, &interaction_context)?;
    }

    let required = BTreeSet::from(CassetteScenario::REQUIRED);
    if scenarios != required {
        let missing: Vec<_> = required.difference(&scenarios).copied().collect();
        let unexpected: Vec<_> = scenarios.difference(&required).copied().collect();
        return Err(invalid(
            context,
            format!("scenario coverage mismatch; missing={missing:?}, unexpected={unexpected:?}"),
        ));
    }
    Ok(())
}

/// Recursively load and validate every `*.json` under `directory`, then require exactly one
/// cassette per [`SUPPORTED_PROVIDERS`] entry. Symlinks are rejected.
pub fn load_and_validate_directory(
    directory: impl AsRef<Path>,
) -> Result<CassetteValidationReport, CassetteValidationError> {
    let directory = directory.as_ref();
    let mut paths = Vec::new();
    collect_json_files(directory, &mut paths)?;
    paths.sort();

    let mut providers = BTreeSet::new();
    let mut interaction_count = 0;
    for path in &paths {
        let cassette = load_cassette(path)?;
        validate_cassette(&cassette)?;
        if !providers.insert(cassette.provider.clone()) {
            return Err(invalid(
                path.display().to_string(),
                format!(
                    "duplicate cassette for provider {}; keep exactly one file per provider",
                    cassette.provider
                ),
            ));
        }
        interaction_count += cassette.interactions.len();
    }

    let expected = BTreeSet::from(SUPPORTED_PROVIDERS.map(str::to_string));
    if providers != expected {
        let missing: Vec<_> = expected.difference(&providers).cloned().collect();
        let unexpected: Vec<_> = providers.difference(&expected).cloned().collect();
        return Err(invalid(
            directory.display().to_string(),
            format!("provider coverage mismatch; missing={missing:?}, unexpected={unexpected:?}"),
        ));
    }

    Ok(CassetteValidationReport {
        providers: providers.into_iter().collect(),
        cassette_count: paths.len(),
        interaction_count,
    })
}

fn collect_json_files(
    directory: &Path,
    paths: &mut Vec<PathBuf>,
) -> Result<(), CassetteValidationError> {
    let entries = fs::read_dir(directory).map_err(|source| CassetteValidationError::Io {
        path: directory.to_path_buf(),
        source,
    })?;
    for entry in entries {
        let entry = entry.map_err(|source| CassetteValidationError::Io {
            path: directory.to_path_buf(),
            source,
        })?;
        let file_type = entry
            .file_type()
            .map_err(|source| CassetteValidationError::Io {
                path: entry.path(),
                source,
            })?;
        if file_type.is_symlink() {
            return Err(invalid(
                entry.path().display().to_string(),
                "symlinks are not valid cassette evidence",
            ));
        }
        if file_type.is_dir() {
            collect_json_files(&entry.path(), paths)?;
        } else if entry.path().extension().and_then(|value| value.to_str()) == Some("json") {
            paths.push(entry.path());
        }
    }
    Ok(())
}

fn validate_interaction(
    cassette: &ProviderCassette,
    interaction: &CassetteInteraction,
    context: &str,
) -> Result<(), CassetteValidationError> {
    if interaction.provider != cassette.provider || interaction.model != cassette.model {
        return Err(invalid(
            context,
            "provider/model identity drifted from cassette root",
        ));
    }
    if interaction.assertions.is_empty()
        || interaction
            .assertions
            .iter()
            .any(|assertion| assertion.trim().is_empty())
    {
        return Err(invalid(
            context,
            "at least one non-empty assertion is required",
        ));
    }
    for assertion in &interaction.assertions {
        scan_string_for_secrets(assertion, context)?;
    }
    if let Some(parameter) = &interaction.unsupported_parameter {
        scan_string_for_secrets(parameter, context)?;
    }

    if interaction.network_performed {
        let request = interaction
            .request
            .as_ref()
            .ok_or_else(|| invalid(context, "network interaction is missing its request"))?;
        let response = interaction
            .response
            .as_ref()
            .ok_or_else(|| invalid(context, "network interaction is missing its response"))?;
        validate_request(&cassette.provider, request, context)?;
        validate_request_model(cassette, request, context)?;
        validate_headers(&response.headers, context)?;
        scan_value_for_secrets(&response.body, context)?;
    } else if interaction.request.is_some() || interaction.response.is_some() {
        return Err(invalid(
            context,
            "non-network interaction must not contain HTTP request/response data",
        ));
    }

    match interaction.scenario {
        CassetteScenario::Streaming => {
            validate_stream(&interaction.normalized_events, context)?;
            ensure_success_response(interaction, context)?;
        }
        CassetteScenario::ProviderError => validate_provider_error(interaction, context)?,
        CassetteScenario::Unsupported => validate_unsupported(interaction, context)?,
        CassetteScenario::Text | CassetteScenario::Tool | CassetteScenario::StructuredOutput => {
            ensure_success_response(interaction, context)?;
            validate_success_response_model(cassette, interaction, context)?;
            if !interaction.normalized_events.is_empty()
                || interaction.typed_error.is_some()
                || interaction.unsupported_parameter.is_some()
            {
                return Err(invalid(
                    context,
                    "success scenario contains incompatible evidence",
                ));
            }
        }
    }
    Ok(())
}

fn validate_request(
    provider: &str,
    request: &RecordedHttpRequest,
    context: &str,
) -> Result<(), CassetteValidationError> {
    if request.method != "POST" {
        return Err(invalid(context, "recorded provider request must use POST"));
    }
    let parsed = url::Url::parse(&request.url)
        .map_err(|error| invalid(context, format!("request URL is invalid: {error}")))?;
    if parsed.scheme() != "https" || parsed.host_str().is_none() {
        return Err(invalid(context, "request URL must be absolute HTTPS"));
    }
    if !parsed.username().is_empty() || parsed.password().is_some() || parsed.query().is_some() {
        return Err(invalid(
            context,
            "request URL must not contain credentials or query parameters",
        ));
    }
    validate_headers(&request.headers, context)?;
    scan_value_for_secrets(&request.body, context)?;

    let expected_auth_header = match provider {
        "anthropic" => "x-api-key",
        "google" => "x-goog-api-key",
        _ => "authorization",
    };
    let redacted: BTreeSet<_> = request
        .redacted_headers
        .iter()
        .map(|header| header.to_ascii_lowercase())
        .collect();
    if redacted.len() != request.redacted_headers.len() {
        return Err(invalid(context, "redacted_headers contains duplicates"));
    }
    if !redacted.contains(expected_auth_header) {
        return Err(invalid(
            context,
            format!(
                "redacted auth header {expected_auth_header} is not declared; add \
                 \"{expected_auth_header}\" to request.redacted_headers"
            ),
        ));
    }
    if redacted
        .iter()
        .any(|header| !SECRET_HEADERS.contains(&header.as_str()))
    {
        return Err(invalid(
            context,
            format!(
                "redacted_headers contains a non-secret header; only these may be listed: {}",
                SECRET_HEADERS.join(", ")
            ),
        ));
    }
    Ok(())
}

fn validate_request_model(
    cassette: &ProviderCassette,
    request: &RecordedHttpRequest,
    context: &str,
) -> Result<(), CassetteValidationError> {
    let wire_model = cassette
        .model
        .strip_prefix(&format!("{}:", cassette.provider))
        .unwrap_or(&cassette.model);
    if cassette.provider == "google" {
        let expected_path_part = format!("/models/{wire_model}:");
        if !request.url.contains(&expected_path_part) {
            return Err(invalid(
                context,
                "Google request URL does not contain cassette model identity",
            ));
        }
        return Ok(());
    }
    if request.body.get("model").and_then(Value::as_str) != Some(wire_model) {
        return Err(invalid(
            context,
            "HTTP request model does not match cassette model identity",
        ));
    }
    Ok(())
}

fn validate_success_response_model(
    cassette: &ProviderCassette,
    interaction: &CassetteInteraction,
    context: &str,
) -> Result<(), CassetteValidationError> {
    let response = interaction
        .response
        .as_ref()
        .ok_or_else(|| invalid(context, "successful interaction is missing response"))?;
    let wire_model = cassette
        .model
        .strip_prefix(&format!("{}:", cassette.provider))
        .unwrap_or(&cassette.model);
    let field = if cassette.provider == "google" {
        "modelVersion"
    } else {
        "model"
    };
    if response.body.get(field).and_then(Value::as_str) != Some(wire_model) {
        return Err(invalid(
            context,
            "HTTP response model does not match cassette model identity",
        ));
    }
    Ok(())
}

fn validate_headers(
    headers: &BTreeMap<String, String>,
    context: &str,
) -> Result<(), CassetteValidationError> {
    let mut normalized_names = BTreeSet::new();
    for (name, value) in headers {
        let normalized = name.to_ascii_lowercase();
        if !normalized_names.insert(normalized.clone()) {
            return Err(invalid(
                context,
                format!("header {name} is duplicated with different casing"),
            ));
        }
        if !ALLOWED_HEADERS.contains(&normalized.as_str()) {
            return Err(invalid(
                context,
                format!(
                    "header {name} is not allowlisted for cassette storage; remove it (auth \
                     headers belong in redacted_headers) or keep only: {}",
                    ALLOWED_HEADERS.join(", ")
                ),
            ));
        }
        scan_string_for_secrets(value, context)?;
    }
    Ok(())
}

fn ensure_success_response(
    interaction: &CassetteInteraction,
    context: &str,
) -> Result<(), CassetteValidationError> {
    if !interaction.network_performed {
        return Err(invalid(
            context,
            "success scenario must contain an HTTP exchange",
        ));
    }
    let status = interaction
        .response
        .as_ref()
        .map(|response| response.status);
    if !matches!(status, Some(200..=299)) {
        return Err(invalid(
            context,
            "success scenario must have a 2xx response",
        ));
    }
    Ok(())
}

fn validate_provider_error(
    interaction: &CassetteInteraction,
    context: &str,
) -> Result<(), CassetteValidationError> {
    if !interaction.network_performed || !interaction.normalized_events.is_empty() {
        return Err(invalid(
            context,
            "provider error must be an HTTP failure without stream events",
        ));
    }
    let response = interaction
        .response
        .as_ref()
        .ok_or_else(|| invalid(context, "provider error response is missing"))?;
    if response.status < 400 {
        return Err(invalid(
            context,
            "provider error response must have status >= 400",
        ));
    }
    let error = interaction
        .typed_error
        .as_ref()
        .ok_or_else(|| invalid(context, "provider error is missing typed error metadata"))?;
    validate_typed_error(interaction, error, context)?;
    if error.status != Some(response.status) {
        return Err(invalid(
            context,
            "typed error status differs from HTTP status",
        ));
    }
    let expected = match response.status {
        401 | 403 => ("provider_auth", "authentication", false),
        408 => ("provider_timeout", "timeout", true),
        429 => ("provider_rate_limit", "rate_limited", true),
        500..=599 => ("provider_server", "server", true),
        400..=499 => ("provider_invalid_request", "invalid_request", false),
        _ => ("unknown", "unknown", false),
    };
    if (error.code.as_str(), error.kind.as_str(), error.retryable) != expected {
        return Err(invalid(
            context,
            format!(
                "typed error classification does not match HTTP status {}; expected \
                 code={}, kind={}, retryable={}",
                response.status, expected.0, expected.1, expected.2
            ),
        ));
    }
    if !error.retryable && error.retry_after_ms.is_some() {
        return Err(invalid(
            context,
            "non-retryable provider error cannot carry retry_after_ms",
        ));
    }
    if interaction.unsupported_parameter.is_some() {
        return Err(invalid(
            context,
            "provider HTTP error cannot declare unsupported parameter",
        ));
    }
    Ok(())
}

fn validate_unsupported(
    interaction: &CassetteInteraction,
    context: &str,
) -> Result<(), CassetteValidationError> {
    if interaction.network_performed
        || !interaction.normalized_events.is_empty()
        || interaction
            .unsupported_parameter
            .as_deref()
            .is_none_or(str::is_empty)
    {
        return Err(invalid(
            context,
            "unsupported scenario must fail before network with a named parameter",
        ));
    }
    let error = interaction.typed_error.as_ref().ok_or_else(|| {
        invalid(
            context,
            "unsupported scenario is missing typed error metadata",
        )
    })?;
    validate_typed_error(interaction, error, context)?;
    if error.status.is_some()
        || error.retry_after_ms.is_some()
        || error.retryable
        || error.kind != "invalid_request"
        || error.code != "provider_invalid_request"
    {
        return Err(invalid(
            context,
            "unsupported parameter must be a non-retryable typed invalid_request",
        ));
    }
    Ok(())
}

fn validate_typed_error(
    interaction: &CassetteInteraction,
    error: &RecordedTypedError,
    context: &str,
) -> Result<(), CassetteValidationError> {
    if error.provider != interaction.provider || error.model != interaction.model {
        return Err(invalid(
            context,
            "typed error provider/model identity mismatch",
        ));
    }
    if error.code.trim().is_empty()
        || error.kind.trim().is_empty()
        || error.message.trim().is_empty()
        || !error.code.starts_with("provider_")
    {
        return Err(invalid(context, "typed error metadata is incomplete"));
    }
    scan_string_for_secrets(&error.message, context)
}

fn validate_stream(
    events: &[RecordedStreamEvent],
    context: &str,
) -> Result<(), CassetteValidationError> {
    if events.len() < 4 {
        return Err(invalid(
            context,
            "stream must include start/delta/end lifecycle",
        ));
    }
    let mut event_ids = BTreeSet::new();
    let mut open_blocks = BTreeSet::new();
    let mut saw_block_start = false;
    let mut saw_block_delta = false;
    let mut saw_block_end = false;
    for (expected_sequence, event) in events.iter().enumerate() {
        let expected_sequence = u64::try_from(expected_sequence)
            .map_err(|_| invalid(context, "stream sequence overflow"))?;
        if event.sequence != expected_sequence {
            return Err(invalid(
                context,
                "stream event sequence is not contiguous from zero",
            ));
        }
        if event.event_id.trim().is_empty() || !event_ids.insert(event.event_id.as_str()) {
            return Err(invalid(context, "stream event id is empty or duplicated"));
        }
        match event.event_type.as_str() {
            "response_start" => {
                if expected_sequence != 0 || event.block_id.is_some() {
                    return Err(invalid(
                        context,
                        "response_start must be the first envelope event",
                    ));
                }
            }
            "block_start" => {
                saw_block_start = true;
                let block_id = required_block_id(event, context)?;
                if !open_blocks.insert(block_id) {
                    return Err(invalid(context, "stream block started twice"));
                }
            }
            "block_delta" => {
                saw_block_delta = true;
                let block_id = required_block_id(event, context)?;
                if !open_blocks.contains(block_id) {
                    return Err(invalid(
                        context,
                        "stream delta arrived outside an open block",
                    ));
                }
            }
            "block_end" => {
                saw_block_end = true;
                let block_id = required_block_id(event, context)?;
                if !open_blocks.remove(block_id) {
                    return Err(invalid(context, "stream block ended before it started"));
                }
            }
            "usage" | "warning" | "provider_metadata" => {
                if event.block_id.is_some() {
                    return Err(invalid(context, "metadata event must not carry block_id"));
                }
            }
            "response_end" => {
                if expected_sequence + 1 != u64::try_from(events.len()).unwrap_or(u64::MAX)
                    || event.block_id.is_some()
                    || !open_blocks.is_empty()
                {
                    return Err(invalid(
                        context,
                        "response_end must be last and all blocks must be closed",
                    ));
                }
            }
            other => {
                return Err(invalid(
                    context,
                    format!("unknown normalized stream event type {other}"),
                ));
            }
        }
    }
    if events.first().map(|event| event.event_type.as_str()) != Some("response_start")
        || events.last().map(|event| event.event_type.as_str()) != Some("response_end")
    {
        return Err(invalid(
            context,
            "stream envelope is missing response start/end",
        ));
    }
    if !(saw_block_start && saw_block_delta && saw_block_end) {
        return Err(invalid(
            context,
            "stream must include block start, delta, and end events",
        ));
    }
    Ok(())
}

fn required_block_id<'a>(
    event: &'a RecordedStreamEvent,
    context: &str,
) -> Result<&'a str, CassetteValidationError> {
    event
        .block_id
        .as_deref()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| invalid(context, "block lifecycle event is missing block_id"))
}

fn scan_value_for_secrets(value: &Value, context: &str) -> Result<(), CassetteValidationError> {
    match value {
        Value::String(value) => scan_string_for_secrets(value, context),
        Value::Array(values) => {
            for value in values {
                scan_value_for_secrets(value, context)?;
            }
            Ok(())
        }
        Value::Object(values) => {
            for (key, value) in values {
                let normalized_key = key.to_ascii_lowercase().replace('-', "_");
                if SECRET_HEADERS.contains(&key.to_ascii_lowercase().as_str())
                    || SECRET_BODY_FIELDS.contains(&normalized_key.as_str())
                {
                    // The field name is safe to report; its value is not.
                    return Err(invalid(
                        context,
                        format!(
                            "secret-shaped field {key:?} is present in HTTP body; remove it \
                             from the recorded JSON"
                        ),
                    ));
                }
                scan_value_for_secrets(value, context)?;
            }
            Ok(())
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => Ok(()),
    }
}

fn scan_string_for_secrets(value: &str, context: &str) -> Result<(), CassetteValidationError> {
    let normalized = value.to_ascii_lowercase();
    let suspicious = [
        "bearer ",
        "basic ",
        "sk-",
        "gsk_",
        "aiza",
        "api_key=",
        "api-key=",
        "apikey=",
        "secret=",
        "token=",
        "-----begin private key-----",
    ];
    if let Some(needle) = suspicious
        .iter()
        .find(|needle| normalized.contains(*needle))
    {
        // Report the marker that matched, never the surrounding value.
        return Err(invalid(
            context,
            format!(
                "possible credential material found in cassette (matched marker {:?}); \
                 replace the value with a sanitized placeholder",
                needle.trim_end()
            ),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> ProviderCassette {
        ProviderCassette {
            schema_version: 1,
            fixture_version: "test.v1".into(),
            provider: "openai".into(),
            model: "gpt-fixture".into(),
            sanitized: true,
            source: CassetteSource {
                adapter_contract: "responses-v1".into(),
                reference_commit: "d34e7ad73682c4e443984201f0069b714b3f6b68".into(),
            },
            interactions: CassetteScenario::REQUIRED
                .into_iter()
                .map(|scenario| {
                    let unsupported = scenario == CassetteScenario::Unsupported;
                    let provider_error = scenario == CassetteScenario::ProviderError;
                    let streaming = scenario == CassetteScenario::Streaming;
                    CassetteInteraction {
                        id: format!("{scenario:?}"),
                        scenario,
                        provider: "openai".into(),
                        model: "gpt-fixture".into(),
                        network_performed: !unsupported,
                        request: (!unsupported).then(|| RecordedHttpRequest {
                            method: "POST".into(),
                            url: "https://api.openai.test/v1/responses".into(),
                            headers: BTreeMap::from([(
                                "content-type".into(),
                                "application/json".into(),
                            )]),
                            redacted_headers: vec!["authorization".into()],
                            body: serde_json::json!({"model": "gpt-fixture"}),
                        }),
                        response: (!unsupported).then(|| RecordedHttpResponse {
                            status: if provider_error { 429 } else { 200 },
                            headers: BTreeMap::from([(
                                "content-type".into(),
                                "application/json".into(),
                            )]),
                            body: serde_json::json!({"model": "gpt-fixture", "fixture": true}),
                        }),
                        normalized_events: if streaming {
                            [
                                ("response_start", None),
                                ("block_start", Some("text-0")),
                                ("block_delta", Some("text-0")),
                                ("block_end", Some("text-0")),
                                ("response_end", None),
                            ]
                            .into_iter()
                            .enumerate()
                            .map(|(sequence, (event_type, block_id))| RecordedStreamEvent {
                                sequence: sequence as u64,
                                event_id: format!("evt-{sequence}"),
                                event_type: event_type.into(),
                                block_id: block_id.map(str::to_string),
                            })
                            .collect()
                        } else {
                            vec![]
                        },
                        typed_error: (provider_error || unsupported).then(|| RecordedTypedError {
                            code: if unsupported {
                                "provider_invalid_request"
                            } else {
                                "provider_rate_limit"
                            }
                            .into(),
                            kind: if unsupported {
                                "invalid_request"
                            } else {
                                "rate_limited"
                            }
                            .into(),
                            provider: "openai".into(),
                            model: "gpt-fixture".into(),
                            status: provider_error.then_some(429),
                            retryable: provider_error,
                            retry_after_ms: provider_error.then_some(2_000),
                            message: "sanitized fixture error".into(),
                        }),
                        unsupported_parameter: unsupported.then(|| "parallel_tool_calls".into()),
                        assertions: vec!["fixture assertion".into()],
                    }
                })
                .collect(),
        }
    }

    #[test]
    fn valid_complete_cassette_passes() {
        validate_cassette(&fixture()).unwrap();
    }

    #[test]
    fn missing_scenario_fails_closed() {
        let mut cassette = fixture();
        cassette
            .interactions
            .retain(|item| item.scenario != CassetteScenario::Tool);
        assert!(validate_cassette(&cassette).is_err());
    }

    #[test]
    fn secret_header_and_broken_stream_are_rejected() {
        let mut cassette = fixture();
        cassette.interactions[0]
            .request
            .as_mut()
            .unwrap()
            .headers
            .insert("authorization".into(), "Bearer fixture-secret".into());
        let message = validate_cassette(&cassette).unwrap_err().to_string();
        assert!(message.contains("redacted_headers"), "{message}");
        assert!(!message.contains("fixture-secret"), "{message}");

        let mut cassette = fixture();
        let stream = cassette
            .interactions
            .iter_mut()
            .find(|item| item.scenario == CassetteScenario::Streaming)
            .unwrap();
        stream.normalized_events.swap(1, 2);
        assert!(validate_cassette(&cassette).is_err());
    }

    #[test]
    fn credential_rejections_name_the_marker_or_field_not_the_value() {
        let mut cassette = fixture();
        cassette.interactions[0].request.as_mut().unwrap().body =
            serde_json::json!({"model": "gpt-fixture", "note": "sk-live-VALUE123"});
        let message = validate_cassette(&cassette).unwrap_err().to_string();
        assert!(message.contains("\"sk-\""), "{message}");
        assert!(!message.contains("VALUE123"), "{message}");

        let mut cassette = fixture();
        cassette.interactions[0].request.as_mut().unwrap().body =
            serde_json::json!({"model": "gpt-fixture", "api_key": "placeholder-VALUE"});
        let message = validate_cassette(&cassette).unwrap_err().to_string();
        assert!(message.contains("\"api_key\""), "{message}");
        assert!(!message.contains("placeholder-VALUE"), "{message}");
    }

    #[test]
    fn unsupported_provider_lists_the_supported_set() {
        let mut cassette = fixture();
        cassette.provider = "acme".into();
        let message = validate_cassette(&cassette).unwrap_err().to_string();
        assert!(message.contains("anthropic, openai"), "{message}");
    }
}
