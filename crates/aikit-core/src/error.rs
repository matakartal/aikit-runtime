//! Typed error hierarchy shared by the core, the bindings, and every host-facing envelope.
//!
//! Two layers exist on purpose:
//!
//! - [`AikitError`] is the in-process Rust error. Its `Display` text names what failed and carries
//!   the internal detail string, which is useful for local debugging but may contain provider
//!   bodies, paths, or tool arguments. Treat it like log output, not like a wire contract.
//! - [`ErrorInfo`] is the redacted, serializable envelope that Python, Node, audit logs, and RPC
//!   surfaces receive. Its `message` is always the fixed [`ErrorCode::message`] summary, so it is
//!   safe to show or persist. Hosts should branch on [`ErrorInfo::code`], never on message text.
//!
//! [`ErrorCode::hint`] adds a fixed, secret-free remediation hint per code for CLIs, logs, and UI
//! surfaces that want to tell a user what to try next.

use std::fmt;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::contract::ProviderWarning;

/// Stable, host-facing error classification. These serialized names are part of the public wire
/// contract: bindings should branch on `code`, never parse a display message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    /// The provider rejected the credentials (HTTP 401/403): missing, invalid, or unauthorized
    /// API key for the requested model. Not retryable.
    ProviderAuth,
    /// The provider throttled the request (HTTP 429). Retryable; honor
    /// [`ErrorInfo::retry_after_ms`] when present.
    ProviderRateLimit,
    /// The provider request timed out (HTTP 408 or a client-side timeout). Retryable.
    ProviderTimeout,
    /// The request never completed at the network layer (DNS, connect, TLS, reset). Retryable.
    ProviderTransport,
    /// The provider failed on its side (HTTP 5xx). Retryable.
    ProviderServer,
    /// The provider rejected the request as malformed or unsupported (other HTTP 4xx), or the
    /// adapter refused an unsupported parameter before network I/O. Not retryable as sent.
    ProviderInvalidRequest,
    /// The provider response violated the expected wire protocol (malformed stream, events out of
    /// order, missing terminal marker). Usually an adapter or API-version mismatch.
    ProviderProtocol,
    /// The provider's safety system blocked the request or the generated response.
    ProviderSafety,
    /// A governance policy, capability check, guardrail, or host approval denied the action.
    PermissionDenied,
    /// A sandbox boundary (filesystem root, network egress, process policy) denied the operation.
    Sandbox,
    /// The runtime, agent, tool, or request configuration is invalid. Fix the configuration; a
    /// retry with the same input fails the same way.
    Configuration,
    /// A configured token, cost, model-call, or wall-time budget was exhausted.
    BudgetExceeded,
    /// A host or built-in tool failed while executing.
    ToolExecution,
    /// Structured output could not be produced: the response was not valid JSON, failed the JSON
    /// Schema or a semantic validator, or repairs were exhausted.
    StructuredOutput,
    /// A session store operation (load, save, resume) failed.
    Session,
    /// Optimistic-concurrency or lifecycle conflict: the state changed underneath the caller
    /// (stale generation, duplicate run, invalid transition).
    Conflict,
    /// The operation was cancelled by the host or a cancellation token.
    Cancelled,
    /// The agent loop reached its `max_turns` limit before the model finished.
    MaxTurns,
    /// The audit trail could not record an event. Audited runs fail closed on this error.
    Audit,
    /// A lifecycle hook failed, timed out, or rejected the operation.
    Hook,
    /// The failure could not be classified more precisely (for example an untyped provider
    /// error). Not retryable, because guessing would make retry/fallback unsafe.
    Unknown,
}

impl ErrorCode {
    /// A safe, stable summary. Arbitrary provider bodies and internal error strings are excluded
    /// deliberately because they may contain credentials, headers, prompts, or tool arguments.
    ///
    /// This text is part of the serialized [`ErrorInfo`] wire contract (recorded in provider
    /// cassettes and cross-language conformance), so it never changes casually. Use
    /// [`ErrorCode::hint`] for remediation guidance.
    pub const fn message(self) -> &'static str {
        match self {
            ErrorCode::ProviderAuth => "provider authentication failed",
            ErrorCode::ProviderRateLimit => "provider rate limit exceeded",
            ErrorCode::ProviderTimeout => "provider request timed out",
            ErrorCode::ProviderTransport => "provider transport failed",
            ErrorCode::ProviderServer => "provider server failed",
            ErrorCode::ProviderInvalidRequest => "provider rejected the request",
            ErrorCode::ProviderProtocol => "provider protocol failed",
            ErrorCode::ProviderSafety => "provider safety policy blocked the request",
            ErrorCode::PermissionDenied => "permission denied",
            ErrorCode::Sandbox => "sandbox boundary denied the operation",
            ErrorCode::Configuration => "runtime configuration is invalid",
            ErrorCode::BudgetExceeded => "budget exceeded",
            ErrorCode::ToolExecution => "tool execution failed",
            ErrorCode::StructuredOutput => "structured output failed",
            ErrorCode::Session => "session operation failed",
            ErrorCode::Conflict => "state conflict",
            ErrorCode::Cancelled => "operation cancelled",
            ErrorCode::MaxTurns => "maximum agent turns exceeded",
            ErrorCode::Audit => "audit operation failed",
            ErrorCode::Hook => "lifecycle hook failed",
            ErrorCode::Unknown => "unknown error",
        }
    }

    /// A fixed, secret-free remediation hint: the likely cause and what to try next.
    ///
    /// Like [`message`](Self::message), the text never includes request data, so it is safe to
    /// print in a CLI, log, or UI. Unlike `message`, it is *not* part of the serialized
    /// [`ErrorInfo`] contract and may be reworded between releases; never match on it.
    pub const fn hint(self) -> &'static str {
        match self {
            ErrorCode::ProviderAuth => {
                "check that this provider's API key is set, valid, and allowed to use the \
                 requested model"
            }
            ErrorCode::ProviderRateLimit => {
                "slow down or retry later; honor retry_after_ms when present, or configure \
                 retries or a fallback provider"
            }
            ErrorCode::ProviderTimeout => {
                "retry the request; if it keeps timing out, reduce max_tokens or the prompt size"
            }
            ErrorCode::ProviderTransport => {
                "check network connectivity, proxy/TLS settings, and the provider base URL, then \
                 retry"
            }
            ErrorCode::ProviderServer => {
                "the provider failed on its side; retry with backoff or fall back to another \
                 provider"
            }
            ErrorCode::ProviderInvalidRequest => {
                "check the model name, parameters, and message shape against what this provider \
                 supports; retrying the same request will fail again"
            }
            ErrorCode::ProviderProtocol => {
                "the provider response did not match the expected wire protocol; this usually \
                 means an adapter or API-version mismatch and is worth reporting"
            }
            ErrorCode::ProviderSafety => {
                "the provider's safety system blocked the input or output; rephrase the request \
                 or review the provider's usage policy"
            }
            ErrorCode::PermissionDenied => {
                "a governance policy, guardrail, or approval callback denied the action; review \
                 the permission policy or approve the action explicitly"
            }
            ErrorCode::Sandbox => {
                "the operation crossed a sandbox boundary; widen the sandbox policy deliberately \
                 or keep the operation inside the allowed roots and hosts"
            }
            ErrorCode::Configuration => {
                "fix the reported configuration value; retrying with the same configuration will \
                 fail again"
            }
            ErrorCode::BudgetExceeded => {
                "a configured token, cost, model-call, or time budget ran out; raise the budget \
                 or reduce the requested work"
            }
            ErrorCode::ToolExecution => {
                "the tool itself failed; inspect the tool's error detail and its input arguments"
            }
            ErrorCode::StructuredOutput => {
                "the model did not return a schema-valid object; simplify the schema, clarify \
                 the prompt, or allow more repair retries"
            }
            ErrorCode::Session => {
                "check that the session store is reachable and the session id exists and is \
                 readable"
            }
            ErrorCode::Conflict => {
                "the state changed underneath this call; reload the latest state and retry with \
                 its current generation"
            }
            ErrorCode::Cancelled => "the operation was cancelled on request; no retry is implied",
            ErrorCode::MaxTurns => {
                "the agent loop hit max_turns before the model finished; raise max_turns or check \
                 for a tool call that repeats without progress"
            }
            ErrorCode::Audit => {
                "the audit sink could not record an event, so the run failed closed; check the \
                 audit sink's storage and permissions"
            }
            ErrorCode::Hook => {
                "a lifecycle hook failed, timed out, or rejected the operation; check the hook \
                 implementation"
            }
            ErrorCode::Unknown => {
                "the failure could not be classified; inspect the error detail or logs"
            }
        }
    }
}

/// Serializable error envelope for Python, Node, logs, and future RPC boundaries.
///
/// `message` is always derived from [`ErrorCode`], never copied from an untrusted provider body or
/// arbitrary internal error string. Provider routing metadata and retry semantics remain intact
/// without exposing request bodies, prompts, tool inputs, headers, or credentials.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ErrorInfo {
    /// Stable classification. Branch on this, never on `message`.
    pub code: ErrorCode,
    /// Fixed redacted summary, always equal to [`ErrorCode::message`] for `code`.
    pub message: String,
    /// Provider that produced the failure, when it came from a typed provider error.
    pub provider: Option<String>,
    /// Model the failing request targeted, when known.
    pub model: Option<String>,
    /// HTTP status returned by the provider, when the failure was an HTTP response.
    pub status: Option<u16>,
    /// Provider-requested wait before retrying, in milliseconds (from `Retry-After`).
    pub retry_after_ms: Option<u64>,
    /// Whether retrying the same request can reasonably succeed. Only rate-limit, timeout,
    /// transport, and server provider failures are retryable; everything else is `false`.
    pub retryable: bool,
    /// Compatibility warnings collected before a provider failure. Keeping them on the typed
    /// envelope ensures `warn`/`best_effort` never lose preflight evidence when HTTP never starts
    /// or returns an error response.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<ProviderWarning>,
}

impl ErrorInfo {
    /// An envelope for `code` with the fixed summary message and no provider metadata.
    pub fn new(code: ErrorCode) -> Self {
        ErrorInfo {
            code,
            message: code.message().to_string(),
            provider: None,
            model: None,
            status: None,
            retry_after_ms: None,
            retryable: false,
            warnings: Vec::new(),
        }
    }

    /// Attach safe routing metadata without ever accepting a provider response body.
    pub fn with_provider(mut self, provider: impl Into<String>, model: impl Into<String>) -> Self {
        self.provider = Some(provider.into());
        self.model = Some(model.into());
        self
    }

    /// The fixed remediation hint for this envelope's code. See [`ErrorCode::hint`].
    pub fn hint(&self) -> &'static str {
        self.code.hint()
    }
}

impl Default for ErrorInfo {
    fn default() -> Self {
        Self::new(ErrorCode::Unknown)
    }
}

/// Stable provider failure classes used by retry/fallback policy. Host, schema, permission and
/// tool failures never enter this classifier, so resilience cannot accidentally replay side
/// effects or retry a denied action.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderErrorKind {
    /// HTTP 401/403: credentials missing, invalid, or not authorized. Maps to
    /// [`ErrorCode::ProviderAuth`].
    Authentication,
    /// HTTP 429: throttled. Retryable. Maps to [`ErrorCode::ProviderRateLimit`].
    RateLimited,
    /// HTTP 408 or a client-side timeout. Retryable. Maps to [`ErrorCode::ProviderTimeout`].
    Timeout,
    /// Network-level failure before a response arrived. Retryable. Maps to
    /// [`ErrorCode::ProviderTransport`].
    Transport,
    /// HTTP 5xx. Retryable. Maps to [`ErrorCode::ProviderServer`].
    Server,
    /// Other HTTP 4xx, or a request the adapter refused before sending. Maps to
    /// [`ErrorCode::ProviderInvalidRequest`].
    InvalidRequest,
    /// The response violated the expected wire protocol. Maps to
    /// [`ErrorCode::ProviderProtocol`].
    Protocol,
    /// Blocked by the provider's safety system. Maps to [`ErrorCode::ProviderSafety`].
    Safety,
    /// Unclassified provider failure. Not retryable. Maps to [`ErrorCode::Unknown`].
    Unknown,
}

/// A typed failure reported by a provider adapter, carrying the routing and retry metadata that
/// resilience policy needs.
///
/// `message` is the adapter's detail string and may echo a (size-bounded) provider response body,
/// so it appears in `Display` output but is deliberately dropped when converting to
/// [`ErrorInfo`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderError {
    /// Provider name, e.g. `"openai"`.
    pub provider: String,
    /// Model the failing request targeted.
    pub model: String,
    /// Stable failure class used for retry/fallback decisions.
    pub kind: ProviderErrorKind,
    /// HTTP status, when the failure was an HTTP response.
    pub status: Option<u16>,
    /// Provider-requested wait before retrying, in milliseconds.
    pub retry_after_ms: Option<u64>,
    /// Adapter detail. Potentially sensitive; never serialized into [`ErrorInfo`].
    pub message: String,
    /// Compatibility warnings collected before the failure.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<ProviderWarning>,
}

impl fmt::Display for ProviderError {
    /// `"<provider> provider error (<Kind>[, HTTP <status>]) for model <model>: <detail>[; retry
    /// after <n> ms]; hint: <hint>"`. The leading `"<provider> provider error (<Kind>"` prefix is
    /// kept stable for log scrapers; hosts should still branch on [`ErrorInfo::code`].
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} provider error ({:?}", self.provider, self.kind)?;
        if let Some(status) = self.status {
            write!(f, ", HTTP {status}")?;
        }
        write!(f, ")")?;
        if !self.model.is_empty() {
            write!(f, " for model {}", self.model)?;
        }
        write!(f, ": {}", self.message)?;
        if let Some(retry_after_ms) = self.retry_after_ms {
            write!(f, "; retry after {retry_after_ms} ms")?;
        }
        write!(f, "; hint: {}", ErrorCode::from(self.kind).hint())
    }
}

impl std::error::Error for ProviderError {}

impl ProviderError {
    /// A provider failure of an explicit `kind` with no HTTP status (for example a transport or
    /// protocol failure detected by the adapter).
    pub fn new(
        provider: impl Into<String>,
        model: impl Into<String>,
        kind: ProviderErrorKind,
        message: impl Into<String>,
    ) -> Self {
        ProviderError {
            provider: provider.into(),
            model: model.into(),
            kind,
            status: None,
            retry_after_ms: None,
            message: message.into(),
            warnings: Vec::new(),
        }
    }

    /// Classify an HTTP error response: 401/403 authentication, 408 timeout, 429 rate limit, other
    /// 4xx invalid request, 5xx server, anything else unknown.
    pub fn from_http(
        provider: impl Into<String>,
        model: impl Into<String>,
        status: u16,
        retry_after_ms: Option<u64>,
        message: impl Into<String>,
    ) -> Self {
        let kind = match status {
            401 | 403 => ProviderErrorKind::Authentication,
            408 => ProviderErrorKind::Timeout,
            429 => ProviderErrorKind::RateLimited,
            500..=599 => ProviderErrorKind::Server,
            400..=499 => ProviderErrorKind::InvalidRequest,
            _ => ProviderErrorKind::Unknown,
        };
        ProviderError {
            provider: provider.into(),
            model: model.into(),
            kind,
            status: Some(status),
            retry_after_ms,
            message: message.into(),
            warnings: Vec::new(),
        }
    }

    /// Preserve compatibility evidence without weakening the underlying typed failure.
    pub fn with_warnings(mut self, warnings: Vec<ProviderWarning>) -> Self {
        for warning in warnings {
            if !self.warnings.contains(&warning) {
                self.warnings.push(warning);
            }
        }
        self
    }

    /// Whether retrying the same request can reasonably succeed: true only for rate-limit,
    /// timeout, transport, and server failures.
    pub fn retryable(&self) -> bool {
        matches!(
            self.kind,
            ProviderErrorKind::RateLimited
                | ProviderErrorKind::Timeout
                | ProviderErrorKind::Transport
                | ProviderErrorKind::Server
        )
    }
}

impl From<ProviderErrorKind> for ErrorCode {
    fn from(kind: ProviderErrorKind) -> Self {
        match kind {
            ProviderErrorKind::Authentication => ErrorCode::ProviderAuth,
            ProviderErrorKind::RateLimited => ErrorCode::ProviderRateLimit,
            ProviderErrorKind::Timeout => ErrorCode::ProviderTimeout,
            ProviderErrorKind::Transport => ErrorCode::ProviderTransport,
            ProviderErrorKind::Server => ErrorCode::ProviderServer,
            ProviderErrorKind::InvalidRequest => ErrorCode::ProviderInvalidRequest,
            ProviderErrorKind::Protocol => ErrorCode::ProviderProtocol,
            ProviderErrorKind::Safety => ErrorCode::ProviderSafety,
            ProviderErrorKind::Unknown => ErrorCode::Unknown,
        }
    }
}

impl From<&ProviderError> for ErrorInfo {
    fn from(error: &ProviderError) -> Self {
        let mut info = ErrorInfo::new(error.kind.into());
        info.provider = Some(error.provider.clone());
        info.model = Some(error.model.clone());
        info.status = error.status;
        info.retry_after_ms = error.retry_after_ms;
        info.retryable = error.retryable();
        info.warnings = error.warnings.clone();
        info
    }
}

impl From<ProviderError> for ErrorInfo {
    fn from(error: ProviderError) -> Self {
        ErrorInfo::from(&error)
    }
}

/// The core runtime error.
///
/// `Display` names what failed followed by the internal detail. The detail can contain provider
/// bodies, file paths, or tool arguments, so do not forward it to untrusted parties; use
/// [`AikitError::info`] for the redacted envelope and [`AikitError::hint`] for a safe next step.
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum AikitError {
    /// A typed provider failure with retry/fallback metadata. Classified by
    /// [`ProviderErrorKind`].
    #[error(transparent)]
    ProviderFailure(Box<ProviderError>),

    /// Compatibility variant for local/custom providers that have not adopted typed failures.
    /// Classified as [`ErrorCode::Unknown`] and never retried; return a [`ProviderError`] instead
    /// to get retry/fallback support.
    #[error("provider error: {0}")]
    Provider(String),

    /// A host or built-in tool failed while executing. See [`ErrorCode::ToolExecution`].
    #[error("tool execution error: {0}")]
    ToolExecution(String),

    /// A policy, guardrail, capability check, or approval denied the action. See
    /// [`ErrorCode::PermissionDenied`].
    #[error("permission denied: {0}")]
    PermissionDenied(String),

    /// A sandbox boundary denied the operation. See [`ErrorCode::Sandbox`].
    #[error("sandbox error: {0}")]
    Sandbox(String),

    /// Invalid runtime, agent, tool, or request configuration. See [`ErrorCode::Configuration`].
    #[error("configuration error: {0}")]
    Configuration(String),

    /// Structured output could not be produced or decoded. See [`ErrorCode::StructuredOutput`].
    #[error("structured output error: {0}")]
    StructuredOutput(String),

    /// A configured token, cost, model-call, or time budget was exhausted. See
    /// [`ErrorCode::BudgetExceeded`].
    #[error(
        "budget exceeded: a configured token, cost, model-call, or time limit was reached; raise \
         the budget or reduce the requested work"
    )]
    BudgetExceeded,

    /// A session store operation failed. See [`ErrorCode::Session`].
    #[error("session error: {0}")]
    Session(String),

    /// The state changed underneath the caller. See [`ErrorCode::Conflict`].
    #[error("state conflict: {0}")]
    Conflict(String),

    /// The operation was cancelled. See [`ErrorCode::Cancelled`].
    #[error("operation cancelled: {0}")]
    Cancelled(String),

    /// The agent loop reached `max_turns`. See [`ErrorCode::MaxTurns`].
    #[error(
        "maximum agent turns exceeded: the agent loop reached max_turns before the model \
         finished; raise max_turns or check for a tool call that repeats without progress"
    )]
    MaxTurns,

    /// The audit trail could not record an event; audited runs fail closed. See
    /// [`ErrorCode::Audit`].
    #[error("audit error: {0}")]
    Audit(String),

    /// A lifecycle hook failed, timed out, or rejected the operation. See [`ErrorCode::Hook`].
    #[error("lifecycle hook error: {0}")]
    Hook(String),

    /// Any other failure. Classified as [`ErrorCode::Unknown`].
    #[error("{0}")]
    Other(String),
}

impl AikitError {
    /// The typed provider failure, if this error came from a provider adapter.
    pub fn provider_error(&self) -> Option<&ProviderError> {
        match self {
            AikitError::ProviderFailure(error) => Some(error.as_ref()),
            _ => None,
        }
    }

    /// Produce the redacted, stable host-facing envelope for this error.
    pub fn info(&self) -> ErrorInfo {
        self.into()
    }

    /// The stable classification of this error, equal to `self.info().code`.
    pub fn code(&self) -> ErrorCode {
        match self {
            AikitError::ProviderFailure(error) => error.kind.into(),
            other => other.info().code,
        }
    }

    /// A fixed, secret-free remediation hint for this error. See [`ErrorCode::hint`].
    pub fn hint(&self) -> &'static str {
        self.code().hint()
    }

    /// Attach preflight compatibility evidence when the failure is a typed provider error.
    /// Non-provider failures deliberately remain unchanged because inventing provider context
    /// would make retry and fallback classification unsafe.
    pub fn with_provider_warnings(self, warnings: Vec<ProviderWarning>) -> Self {
        match self {
            AikitError::ProviderFailure(error) => {
                AikitError::ProviderFailure(Box::new((*error).with_warnings(warnings)))
            }
            other => other,
        }
    }
}

impl From<ProviderError> for AikitError {
    fn from(error: ProviderError) -> Self {
        AikitError::ProviderFailure(Box::new(error))
    }
}

impl From<&AikitError> for ErrorInfo {
    fn from(error: &AikitError) -> Self {
        match error {
            AikitError::ProviderFailure(error) => error.as_ref().into(),
            // Compatibility provider failures have no trustworthy structured classification or
            // routing metadata. Guessing would make retry/fallback unsafe, so they remain unknown.
            AikitError::Provider(_) | AikitError::Other(_) => ErrorInfo::new(ErrorCode::Unknown),
            AikitError::ToolExecution(_) => ErrorInfo::new(ErrorCode::ToolExecution),
            AikitError::PermissionDenied(_) => ErrorInfo::new(ErrorCode::PermissionDenied),
            AikitError::Sandbox(_) => ErrorInfo::new(ErrorCode::Sandbox),
            AikitError::Configuration(_) => ErrorInfo::new(ErrorCode::Configuration),
            AikitError::StructuredOutput(_) => ErrorInfo::new(ErrorCode::StructuredOutput),
            AikitError::BudgetExceeded => ErrorInfo::new(ErrorCode::BudgetExceeded),
            AikitError::Session(_) => ErrorInfo::new(ErrorCode::Session),
            AikitError::Conflict(_) => ErrorInfo::new(ErrorCode::Conflict),
            AikitError::Cancelled(_) => ErrorInfo::new(ErrorCode::Cancelled),
            AikitError::MaxTurns => ErrorInfo::new(ErrorCode::MaxTurns),
            AikitError::Audit(_) => ErrorInfo::new(ErrorCode::Audit),
            AikitError::Hook(_) => ErrorInfo::new(ErrorCode::Hook),
        }
    }
}

impl From<AikitError> for ErrorInfo {
    fn from(error: AikitError) -> Self {
        ErrorInfo::from(&error)
    }
}

/// Core result alias over [`AikitError`].
pub type Result<T> = std::result::Result<T, AikitError>;

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn error_codes_have_stable_serialized_names() {
        let cases = [
            (ErrorCode::ProviderAuth, "provider_auth"),
            (ErrorCode::ProviderRateLimit, "provider_rate_limit"),
            (ErrorCode::ProviderTimeout, "provider_timeout"),
            (ErrorCode::ProviderTransport, "provider_transport"),
            (ErrorCode::ProviderServer, "provider_server"),
            (
                ErrorCode::ProviderInvalidRequest,
                "provider_invalid_request",
            ),
            (ErrorCode::ProviderProtocol, "provider_protocol"),
            (ErrorCode::ProviderSafety, "provider_safety"),
            (ErrorCode::PermissionDenied, "permission_denied"),
            (ErrorCode::Sandbox, "sandbox"),
            (ErrorCode::Configuration, "configuration"),
            (ErrorCode::BudgetExceeded, "budget_exceeded"),
            (ErrorCode::ToolExecution, "tool_execution"),
            (ErrorCode::StructuredOutput, "structured_output"),
            (ErrorCode::Session, "session"),
            (ErrorCode::Conflict, "conflict"),
            (ErrorCode::Cancelled, "cancelled"),
            (ErrorCode::MaxTurns, "max_turns"),
            (ErrorCode::Audit, "audit"),
            (ErrorCode::Hook, "hook"),
            (ErrorCode::Unknown, "unknown"),
        ];

        for (code, serialized_name) in cases {
            let encoded = serde_json::to_string(&code).unwrap();
            assert_eq!(encoded, format!("\"{serialized_name}\""));
            assert_eq!(serde_json::from_str::<ErrorCode>(&encoded).unwrap(), code);
        }
    }

    #[test]
    fn provider_kinds_map_to_stable_codes_and_retryability() {
        let cases = [
            (
                ProviderErrorKind::Authentication,
                ErrorCode::ProviderAuth,
                false,
            ),
            (
                ProviderErrorKind::RateLimited,
                ErrorCode::ProviderRateLimit,
                true,
            ),
            (ProviderErrorKind::Timeout, ErrorCode::ProviderTimeout, true),
            (
                ProviderErrorKind::Transport,
                ErrorCode::ProviderTransport,
                true,
            ),
            (ProviderErrorKind::Server, ErrorCode::ProviderServer, true),
            (
                ProviderErrorKind::InvalidRequest,
                ErrorCode::ProviderInvalidRequest,
                false,
            ),
            (
                ProviderErrorKind::Protocol,
                ErrorCode::ProviderProtocol,
                false,
            ),
            (ProviderErrorKind::Safety, ErrorCode::ProviderSafety, false),
            (ProviderErrorKind::Unknown, ErrorCode::Unknown, false),
        ];

        for (kind, code, retryable) in cases {
            let info = ErrorInfo::from(ProviderError::new(
                "provider",
                "model",
                kind,
                "untrusted detail",
            ));
            assert_eq!(info.code, code);
            assert_eq!(info.retryable, retryable);
        }
    }

    #[test]
    fn provider_envelope_preserves_safe_metadata_and_drops_raw_body() {
        let raw_secret = "Authorization: Bearer sk-super-secret";
        let error = ProviderError::from_http("openai", "gpt-5", 429, Some(2_000), raw_secret);
        let info = ErrorInfo::from(error);
        let value = serde_json::to_value(&info).unwrap();

        assert_eq!(
            value,
            json!({
                "code": "provider_rate_limit",
                "message": "provider rate limit exceeded",
                "provider": "openai",
                "model": "gpt-5",
                "status": 429,
                "retry_after_ms": 2_000,
                "retryable": true
            })
        );
        assert!(!serde_json::to_string(&value).unwrap().contains(raw_secret));
    }

    #[test]
    fn local_errors_classify_without_serializing_untrusted_details() {
        let secret = "sk-local-secret";
        let cases = [
            (AikitError::Provider(secret.into()), ErrorCode::Unknown),
            (
                AikitError::ToolExecution(secret.into()),
                ErrorCode::ToolExecution,
            ),
            (
                AikitError::PermissionDenied(secret.into()),
                ErrorCode::PermissionDenied,
            ),
            (AikitError::Sandbox(secret.into()), ErrorCode::Sandbox),
            (
                AikitError::Configuration(secret.into()),
                ErrorCode::Configuration,
            ),
            (
                AikitError::StructuredOutput(secret.into()),
                ErrorCode::StructuredOutput,
            ),
            (AikitError::BudgetExceeded, ErrorCode::BudgetExceeded),
            (AikitError::Session(secret.into()), ErrorCode::Session),
            (AikitError::Conflict(secret.into()), ErrorCode::Conflict),
            (AikitError::Cancelled(secret.into()), ErrorCode::Cancelled),
            (AikitError::MaxTurns, ErrorCode::MaxTurns),
            (AikitError::Audit(secret.into()), ErrorCode::Audit),
            (AikitError::Hook(secret.into()), ErrorCode::Hook),
            (AikitError::Other(secret.into()), ErrorCode::Unknown),
        ];

        for (error, expected_code) in cases {
            let info = error.info();
            assert_eq!(info.code, expected_code);
            assert!(!info.retryable);
            assert!(!serde_json::to_string(&info).unwrap().contains(secret));
        }
    }

    #[test]
    fn every_code_has_a_distinct_nonempty_hint_that_is_not_serialized() {
        let codes = [
            ErrorCode::ProviderAuth,
            ErrorCode::ProviderRateLimit,
            ErrorCode::ProviderTimeout,
            ErrorCode::ProviderTransport,
            ErrorCode::ProviderServer,
            ErrorCode::ProviderInvalidRequest,
            ErrorCode::ProviderProtocol,
            ErrorCode::ProviderSafety,
            ErrorCode::PermissionDenied,
            ErrorCode::Sandbox,
            ErrorCode::Configuration,
            ErrorCode::BudgetExceeded,
            ErrorCode::ToolExecution,
            ErrorCode::StructuredOutput,
            ErrorCode::Session,
            ErrorCode::Conflict,
            ErrorCode::Cancelled,
            ErrorCode::MaxTurns,
            ErrorCode::Audit,
            ErrorCode::Hook,
            ErrorCode::Unknown,
        ];
        let mut seen = std::collections::HashSet::new();
        for code in codes {
            let hint = code.hint();
            assert!(!hint.trim().is_empty(), "{code:?} has an empty hint");
            assert!(seen.insert(hint), "{code:?} reuses another code's hint");
            let info = ErrorInfo::new(code);
            assert_eq!(info.hint(), hint);
            // The hint is a local convenience, never part of the wire envelope.
            assert!(!serde_json::to_string(&info).unwrap().contains(hint));
        }
    }

    #[test]
    fn provider_display_names_cause_and_next_step_but_info_stays_redacted() {
        let error = ProviderError::from_http("openai", "gpt-5", 429, Some(2_000), "slow down");
        let display = error.to_string();
        assert!(display.starts_with("openai provider error (RateLimited, HTTP 429)"));
        assert!(display.contains("for model gpt-5: slow down"));
        assert!(display.contains("retry after 2000 ms"));
        assert!(display.contains(ErrorCode::ProviderRateLimit.hint()));

        let transport =
            ProviderError::new("p", "m", ProviderErrorKind::Transport, "connection reset");
        assert_eq!(
            transport.to_string(),
            format!(
                "p provider error (Transport) for model m: connection reset; hint: {}",
                ErrorCode::ProviderTransport.hint()
            )
        );

        let wrapped = AikitError::from(error);
        assert_eq!(wrapped.code(), ErrorCode::ProviderRateLimit);
        assert_eq!(wrapped.hint(), ErrorCode::ProviderRateLimit.hint());
        assert_eq!(
            wrapped.info().message,
            ErrorCode::ProviderRateLimit.message()
        );
    }

    #[test]
    fn local_error_code_matches_info_code() {
        for error in [
            AikitError::Provider("x".into()),
            AikitError::Configuration("x".into()),
            AikitError::BudgetExceeded,
            AikitError::MaxTurns,
            AikitError::Other("x".into()),
        ] {
            assert_eq!(error.code(), error.info().code);
        }
        assert!(AikitError::BudgetExceeded
            .to_string()
            .starts_with("budget exceeded"));
        assert!(AikitError::MaxTurns
            .to_string()
            .starts_with("maximum agent turns exceeded"));
    }

    #[test]
    fn http_statuses_have_stable_retry_classes() {
        for status in [429, 500, 503, 408] {
            assert!(ProviderError::from_http("p", "m", status, None, "x").retryable());
        }
        for status in [400, 401, 403, 404, 422] {
            assert!(!ProviderError::from_http("p", "m", status, None, "x").retryable());
        }
        assert_eq!(
            ProviderError::from_http("p", "m", 429, Some(2_000), "x").retry_after_ms,
            Some(2_000)
        );
    }
}
