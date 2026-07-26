use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fmt;
use std::path::PathBuf;
use std::str::FromStr;
use tokio::sync::broadcast;

pub const EXTERNAL_AGENT_CONTRACT_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExternalAgentKind {
    Codex,
    Gemini,
    Claude,
    Grok,
}

impl ExternalAgentKind {
    pub const ALL: [Self; 4] = [Self::Codex, Self::Gemini, Self::Claude, Self::Grok];

    pub const fn command_name(self) -> &'static str {
        match self {
            Self::Codex => "codex",
            Self::Gemini => "gemini",
            Self::Claude => "claude",
            Self::Grok => "grok",
        }
    }

    pub const fn protocol_name(self) -> &'static str {
        match self {
            Self::Codex => "codex-app-server",
            Self::Gemini | Self::Claude | Self::Grok => "acp",
        }
    }
}

impl fmt::Display for ExternalAgentKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.command_name())
    }
}

impl FromStr for ExternalAgentKind {
    type Err = ExternalAgentError;

    fn from_str(value: &str) -> ExternalAgentResult<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "codex" => Ok(Self::Codex),
            "gemini" => Ok(Self::Gemini),
            "claude" => Ok(Self::Claude),
            "grok" => Ok(Self::Grok),
            _ => Err(ExternalAgentError::invalid_input(format!(
                "unknown external agent kind `{value}`"
            ))),
        }
    }
}

/// Environment overrides that keep an external runtime on its vendor-owned CLI session instead
/// of silently preferring a PAYG API key inherited from the host process.
pub fn vendor_session_environment(kind: ExternalAgentKind) -> BTreeMap<String, String> {
    let names: &[&str] = match kind {
        ExternalAgentKind::Codex => &["OPENAI_API_KEY"],
        ExternalAgentKind::Gemini => &[
            "GEMINI_API_KEY",
            "GOOGLE_API_KEY",
            "GOOGLE_APPLICATION_CREDENTIALS",
            "GOOGLE_GENAI_USE_VERTEXAI",
        ],
        ExternalAgentKind::Claude => &[
            "ANTHROPIC_API_KEY",
            "ANTHROPIC_AUTH_TOKEN",
            "ANTHROPIC_BASE_URL",
            "ANTHROPIC_CUSTOM_HEADERS",
            "CLAUDE_CODE_USE_BEDROCK",
            "CLAUDE_CODE_USE_VERTEX",
        ],
        ExternalAgentKind::Grok => &["XAI_API_KEY"],
    };
    names
        .iter()
        .map(|name| ((*name).to_string(), String::new()))
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RuntimeInstanceId(String);

impl RuntimeInstanceId {
    pub fn new(value: impl Into<String>) -> ExternalAgentResult<Self> {
        let value = value.into();
        let valid = !value.is_empty()
            && value.len() <= 128
            && value.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b':' | b'.')
            });
        if !valid {
            return Err(ExternalAgentError::invalid_input(
                "runtime instance id must contain 1-128 ASCII letters, digits, '-', '_', ':' or '.'",
            ));
        }
        Ok(Self(value))
    }

    pub fn default_for(kind: ExternalAgentKind) -> Self {
        Self(kind.command_name().to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for RuntimeInstanceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for RuntimeInstanceId {
    type Err = ExternalAgentError;

    fn from_str(value: &str) -> ExternalAgentResult<Self> {
        Self::new(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeProfile {
    External,
    Managed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeInstallOrigin {
    AikitManaged,
    VendorNative,
    Homebrew,
    Npm,
    ApplicationBundle,
    ExplicitPath,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeDiscoverySource {
    Explicit,
    Path,
    KnownLocation,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeCandidate {
    pub kind: ExternalAgentKind,
    pub executable: PathBuf,
    pub real_executable: PathBuf,
    pub origin: RuntimeInstallOrigin,
    pub discovered_by: Vec<RuntimeDiscoverySource>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub aliases: Vec<PathBuf>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeAuthState {
    Authenticated,
    Unauthenticated,
    LoginPending,
    Expired,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeAuthStatus {
    pub state: RuntimeAuthState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub method: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account_label: Option<String>,
}

impl RuntimeAuthStatus {
    pub fn unknown() -> Self {
        Self {
            state: RuntimeAuthState::Unknown,
            method: None,
            account_label: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeHealthState {
    Healthy,
    Degraded,
    AuthRequired,
    Incompatible,
    Blocked,
    Stopped,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionOwnership {
    HostBrokered,
    VendorContained,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeDescriptor {
    pub schema_version: u32,
    pub instance_id: RuntimeInstanceId,
    pub kind: ExternalAgentKind,
    pub profile: RuntimeProfile,
    pub executable: PathBuf,
    pub real_executable: PathBuf,
    pub origin: RuntimeInstallOrigin,
    pub protocol: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    pub auth: RuntimeAuthStatus,
    pub health: RuntimeHealthState,
    pub execution_ownership: ExecutionOwnership,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_hash: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeModel {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(default)]
    pub capabilities: BTreeMap<String, Value>,
    #[serde(default)]
    pub available: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeCatalogSnapshot {
    pub schema_version: u32,
    pub instance_id: RuntimeInstanceId,
    pub observed_unix_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stale_after_unix_ms: Option<u64>,
    pub models: Vec<RuntimeModel>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeSessionState {
    Starting,
    Ready,
    Running,
    WaitingForApproval,
    Stopped,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeSession {
    pub instance_id: RuntimeInstanceId,
    pub session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_session_id: Option<String>,
    pub state: RuntimeSessionState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_hash: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NewRuntimeSession {
    pub cwd: PathBuf,
    #[serde(default)]
    pub additional_directories: Vec<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ExternalPromptBlock {
    Text { text: String },
    Resource { uri: String, name: String },
    Image { uri: String, media_type: String },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExternalPrompt {
    pub session_id: String,
    pub prompt_id: String,
    pub blocks: Vec<ExternalPromptBlock>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExternalApprovalDecision {
    Accept,
    AcceptForSession,
    Decline,
    Cancel,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RuntimeEventKind {
    SessionState {
        state: RuntimeSessionState,
    },
    ContentDelta {
        stream: String,
        text: String,
    },
    PlanUpdated {
        entries: Vec<Value>,
    },
    ToolStarted {
        tool_call_id: String,
        detail: Value,
    },
    ToolUpdated {
        tool_call_id: String,
        detail: Value,
    },
    ApprovalRequested {
        request_id: String,
        detail: Value,
    },
    UserInputRequested {
        request_id: String,
        detail: Value,
    },
    UsageUpdated {
        usage: Value,
    },
    LoginStarted {
        login_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        url: Option<String>,
    },
    AuthUpdated {
        auth: RuntimeAuthStatus,
    },
    Warning {
        message: String,
    },
    Failed {
        message: String,
    },
    Completed,
    Cancelled,
    Unknown {
        method: String,
        payload: Value,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeEvent {
    pub schema_version: u32,
    pub instance_id: RuntimeInstanceId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<String>,
    pub sequence: u64,
    pub event: RuntimeEventKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExternalAgentErrorCode {
    InvalidInput,
    NotFound,
    NotExecutable,
    SpawnFailed,
    Protocol,
    Timeout,
    Authentication,
    Permission,
    Conflict,
    Update,
    Io,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
#[error("{code:?}: {message}")]
#[serde(deny_unknown_fields)]
pub struct ExternalAgentError {
    pub code: ExternalAgentErrorCode,
    pub message: String,
}

impl ExternalAgentError {
    pub fn new(code: ExternalAgentErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub fn invalid_input(message: impl Into<String>) -> Self {
        Self::new(ExternalAgentErrorCode::InvalidInput, message)
    }

    pub fn io(message: impl Into<String>) -> Self {
        Self::new(ExternalAgentErrorCode::Io, message)
    }
}

pub type ExternalAgentResult<T> = Result<T, ExternalAgentError>;

#[async_trait]
pub trait ExternalAgentRuntime: Send + Sync {
    fn descriptor(&self) -> RuntimeDescriptor;

    fn subscribe(&self) -> broadcast::Receiver<RuntimeEvent>;

    async fn initialize(&self) -> ExternalAgentResult<RuntimeDescriptor>;

    async fn auth_status(&self, refresh: bool) -> ExternalAgentResult<RuntimeAuthStatus>;

    async fn login(&self) -> ExternalAgentResult<RuntimeAuthStatus>;

    async fn logout(&self) -> ExternalAgentResult<()>;

    async fn list_models(&self) -> ExternalAgentResult<RuntimeCatalogSnapshot>;

    async fn new_session(&self, request: NewRuntimeSession) -> ExternalAgentResult<RuntimeSession>;

    async fn load_session(&self, session_id: &str) -> ExternalAgentResult<RuntimeSession>;

    async fn prompt(&self, prompt: ExternalPrompt) -> ExternalAgentResult<()>;

    async fn cancel(&self, session_id: &str, turn_id: Option<&str>) -> ExternalAgentResult<()>;

    async fn respond_approval(
        &self,
        session_id: &str,
        request_id: &str,
        decision: ExternalApprovalDecision,
    ) -> ExternalAgentResult<()>;

    async fn respond_user_input(
        &self,
        session_id: &str,
        request_id: &str,
        answers: BTreeMap<String, String>,
    ) -> ExternalAgentResult<()>;

    async fn shutdown(&self) -> ExternalAgentResult<()>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kinds_have_stable_command_and_protocol_names() {
        assert_eq!(ExternalAgentKind::ALL.len(), 4);
        assert_eq!(ExternalAgentKind::Codex.command_name(), "codex");
        assert_eq!(ExternalAgentKind::Gemini.protocol_name(), "acp");
        assert_eq!("claude".parse(), Ok(ExternalAgentKind::Claude));
    }

    #[test]
    fn instance_ids_reject_paths_and_control_characters() {
        assert!(RuntimeInstanceId::new("codex:personal").is_ok());
        assert!(RuntimeInstanceId::new("../codex").is_err());
        assert!(RuntimeInstanceId::new("codex\nother").is_err());
        assert!(RuntimeInstanceId::new("").is_err());
    }

    #[test]
    fn vendor_session_environment_masks_payg_keys_without_reading_values() {
        let claude = vendor_session_environment(ExternalAgentKind::Claude);
        assert_eq!(
            claude.get("ANTHROPIC_API_KEY").map(String::as_str),
            Some("")
        );
        assert_eq!(
            claude.get("ANTHROPIC_AUTH_TOKEN").map(String::as_str),
            Some("")
        );
        assert!(!claude.contains_key("HOME"));
    }
}
