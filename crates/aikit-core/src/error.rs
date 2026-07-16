//! Typed error hierarchy. Kept small for the FFI spike; grows with governance/sandbox/budget.

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Stable, host-facing error classification. These serialized names are part of the public wire
/// contract: bindings should branch on `code`, never parse a display message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    ProviderAuth,
    ProviderRateLimit,
    ProviderTimeout,
    ProviderTransport,
    ProviderServer,
    ProviderInvalidRequest,
    ProviderProtocol,
    ProviderSafety,
    PermissionDenied,
    Sandbox,
    Configuration,
    BudgetExceeded,
    ToolExecution,
    StructuredOutput,
    Session,
    Conflict,
    Cancelled,
    MaxTurns,
    Audit,
    Hook,
    Unknown,
}

impl ErrorCode {
    /// A safe, stable summary. Arbitrary provider bodies and internal error strings are excluded
    /// deliberately because they may contain credentials, headers, prompts, or tool arguments.
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
}

/// Serializable error envelope for Python, Node, logs, and future RPC boundaries.
///
/// `message` is always derived from [`ErrorCode`], never copied from an untrusted provider body or
/// arbitrary internal error string. Provider routing metadata and retry semantics remain intact
/// without exposing request bodies, prompts, tool inputs, headers, or credentials.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorInfo {
    pub code: ErrorCode,
    pub message: String,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub status: Option<u16>,
    pub retry_after_ms: Option<u64>,
    pub retryable: bool,
}

impl ErrorInfo {
    pub fn new(code: ErrorCode) -> Self {
        ErrorInfo {
            code,
            message: code.message().to_string(),
            provider: None,
            model: None,
            status: None,
            retry_after_ms: None,
            retryable: false,
        }
    }

    /// Attach safe routing metadata without ever accepting a provider response body.
    pub fn with_provider(mut self, provider: impl Into<String>, model: impl Into<String>) -> Self {
        self.provider = Some(provider.into());
        self.model = Some(model.into());
        self
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
    Authentication,
    RateLimited,
    Timeout,
    Transport,
    Server,
    InvalidRequest,
    Protocol,
    Safety,
    Unknown,
}

#[derive(Error, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[error("{provider} provider error ({kind:?}): {message}")]
pub struct ProviderError {
    pub provider: String,
    pub model: String,
    pub kind: ProviderErrorKind,
    pub status: Option<u16>,
    pub retry_after_ms: Option<u64>,
    pub message: String,
}

impl ProviderError {
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
        }
    }

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
        }
    }

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
        info
    }
}

impl From<ProviderError> for ErrorInfo {
    fn from(error: ProviderError) -> Self {
        ErrorInfo::from(&error)
    }
}

#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum AikitError {
    #[error(transparent)]
    ProviderFailure(#[from] ProviderError),

    /// Compatibility variant for local/custom providers that have not adopted typed failures.
    #[error("provider error: {0}")]
    Provider(String),

    #[error("tool execution error: {0}")]
    ToolExecution(String),

    #[error("permission denied: {0}")]
    PermissionDenied(String),

    #[error("sandbox error: {0}")]
    Sandbox(String),

    #[error("configuration error: {0}")]
    Configuration(String),

    #[error("structured output error: {0}")]
    StructuredOutput(String),

    #[error("budget exceeded")]
    BudgetExceeded,

    #[error("session error: {0}")]
    Session(String),

    #[error("state conflict: {0}")]
    Conflict(String),

    #[error("operation cancelled: {0}")]
    Cancelled(String),

    #[error("maximum agent turns exceeded")]
    MaxTurns,

    #[error("audit error: {0}")]
    Audit(String),

    #[error("lifecycle hook error: {0}")]
    Hook(String),

    #[error("{0}")]
    Other(String),
}

impl AikitError {
    pub fn provider_error(&self) -> Option<&ProviderError> {
        match self {
            AikitError::ProviderFailure(error) => Some(error),
            _ => None,
        }
    }

    /// Produce the redacted, stable host-facing envelope for this error.
    pub fn info(&self) -> ErrorInfo {
        self.into()
    }
}

impl From<&AikitError> for ErrorInfo {
    fn from(error: &AikitError) -> Self {
        match error {
            AikitError::ProviderFailure(error) => error.into(),
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
