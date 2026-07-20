//! Thin Agent Client Protocol (ACP) session and event mapping for editors such as Zed.

use super::common::{
    scopes, validate_identifier, CorrelationIdentity, GovernanceDenialCode, GovernanceEnvelope,
    GovernedAction, ProtocolError, ProtocolKind, ProtocolPrincipal, ProtocolResult,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

pub const ACP_PROTOCOL_VERSION: u32 = 1;

const SESSION_OPEN_SCOPE: &str = "acp:sessions:open";
const PROMPT_SCOPE: &str = "acp:sessions:prompt";
const CANCEL_SCOPE: &str = "acp:sessions:cancel";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcpSessionMapping {
    pub acp_session_id: String,
    pub session_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcpRunMapping {
    pub acp_session_id: String,
    pub session_id: String,
    pub prompt_id: String,
    pub run_id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AcpPromptBlock {
    Text { text: String },
    Resource { uri: String, name: String },
    Image { uri: String, media_type: String },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcpPromptRequest {
    pub acp_session_id: String,
    pub prompt_id: String,
    pub blocks: Vec<AcpPromptBlock>,
    #[serde(default)]
    pub metadata: BTreeMap<String, Value>,
}

impl AcpPromptRequest {
    fn validate(&self) -> ProtocolResult<()> {
        validate_identifier("ACP session id", &self.acp_session_id)?;
        validate_identifier("ACP prompt id", &self.prompt_id)?;
        if self.blocks.is_empty() {
            return Err(ProtocolError::invalid(
                "ACP prompt must contain at least one block",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AcpAction {
    OpenSession {
        mapping: AcpSessionMapping,
    },
    Prompt {
        request: AcpPromptRequest,
        mapping: AcpRunMapping,
    },
    Cancel {
        mapping: AcpRunMapping,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AcpToolCallStatus {
    Pending,
    InProgress,
    Completed,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AcpPlanEntryStatus {
    Pending,
    InProgress,
    Completed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcpPlanEntry {
    pub content: String,
    pub status: AcpPlanEntryStatus,
}

/// Minimal runtime events understood by the ACP adapter. Provider-native events stay outside this
/// boundary; runtime code maps them to this stable provider-neutral set first.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AcpRuntimeEvent {
    AgentMessageChunk {
        text: String,
    },
    AgentThoughtChunk {
        text: String,
    },
    ToolCall {
        tool_call_id: String,
        title: String,
        raw_input: Value,
    },
    ToolCallUpdate {
        tool_call_id: String,
        status: AcpToolCallStatus,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        message: Option<String>,
    },
    Plan {
        entries: Vec<AcpPlanEntry>,
    },
    Usage {
        input_tokens: u64,
        output_tokens: u64,
    },
    Completed,
    Failed {
        message: String,
    },
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "session_update", rename_all = "snake_case")]
pub enum AcpSessionUpdate {
    AgentMessageChunk {
        text: String,
    },
    AgentThoughtChunk {
        text: String,
    },
    ToolCall {
        tool_call_id: String,
        title: String,
        raw_input: Value,
    },
    ToolCallUpdate {
        tool_call_id: String,
        status: AcpToolCallStatus,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        message: Option<String>,
    },
    Plan {
        entries: Vec<AcpPlanEntry>,
    },
    UsageUpdate {
        input_tokens: u64,
        output_tokens: u64,
    },
    Completed,
    Failed {
        message: String,
    },
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcpSessionEvent {
    pub acp_session_id: String,
    pub run_id: String,
    pub update: AcpSessionUpdate,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AcpSessionRecord {
    mapping: AcpSessionMapping,
    owner_subject: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    owner_tenant_id: Option<String>,
    active_run: Option<AcpRunMapping>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcpSessionMapper {
    sessions: BTreeMap<String, AcpSessionRecord>,
    next_sequence: u64,
}

impl Default for AcpSessionMapper {
    fn default() -> Self {
        Self::new()
    }
}

impl AcpSessionMapper {
    pub fn new() -> Self {
        Self {
            sessions: BTreeMap::new(),
            next_sequence: 1,
        }
    }

    pub fn session(&self, acp_session_id: &str) -> Option<&AcpSessionMapping> {
        self.sessions
            .get(acp_session_id)
            .map(|record| &record.mapping)
    }

    pub fn active_run(&self, acp_session_id: &str) -> Option<&AcpRunMapping> {
        self.sessions
            .get(acp_session_id)
            .and_then(|record| record.active_run.as_ref())
    }

    pub fn prepare_new_session(
        &mut self,
        requested_acp_session_id: Option<&str>,
        correlation: CorrelationIdentity,
        principal: Option<&ProtocolPrincipal>,
    ) -> GovernedAction<AcpAction> {
        let target = requested_acp_session_id.unwrap_or("new-session");
        let mut envelope = GovernanceEnvelope::evaluate(
            ProtocolKind::Acp,
            correlation,
            principal,
            "session/new",
            target,
            scopes(&[SESSION_OPEN_SCOPE]),
        );
        if !envelope.authorization.is_allowed() {
            return GovernedAction::denied(envelope);
        }
        if requested_acp_session_id
            .is_some_and(|session_id| validate_identifier("ACP session id", session_id).is_err())
        {
            envelope = envelope.deny(
                GovernanceDenialCode::StateConflict,
                "ACP session id is invalid",
            );
            return GovernedAction::denied(envelope);
        }
        let principal = principal.expect("allowed envelope always has a principal");
        if let Some(session_id) = requested_acp_session_id {
            if let Some(existing) = self.sessions.get(session_id) {
                if !principal
                    .matches_identity(&existing.owner_subject, existing.owner_tenant_id.as_deref())
                {
                    envelope = envelope.deny(
                        GovernanceDenialCode::PrincipalMismatch,
                        "ACP session is not accessible",
                    );
                    return GovernedAction::denied(envelope);
                }
                envelope.correlation.session_id = Some(existing.mapping.session_id.clone());
                return GovernedAction::from_envelope(
                    envelope,
                    AcpAction::OpenSession {
                        mapping: existing.mapping.clone(),
                    },
                );
            }
        }

        let acp_session_id = requested_acp_session_id
            .map(str::to_owned)
            .unwrap_or_else(|| self.next_id("acp-session"));
        let mapping = AcpSessionMapping {
            acp_session_id: acp_session_id.clone(),
            session_id: self.next_id("session"),
        };
        envelope.correlation.session_id = Some(mapping.session_id.clone());
        self.sessions.insert(
            acp_session_id,
            AcpSessionRecord {
                mapping: mapping.clone(),
                owner_subject: principal.subject.clone(),
                owner_tenant_id: principal.tenant_id.clone(),
                active_run: None,
            },
        );
        GovernedAction::from_envelope(envelope, AcpAction::OpenSession { mapping })
    }

    pub fn prepare_prompt(
        &mut self,
        request: AcpPromptRequest,
        correlation: CorrelationIdentity,
        principal: Option<&ProtocolPrincipal>,
    ) -> GovernedAction<AcpAction> {
        let mut envelope = GovernanceEnvelope::evaluate(
            ProtocolKind::Acp,
            correlation,
            principal,
            "session/prompt",
            request.acp_session_id.clone(),
            scopes(&[PROMPT_SCOPE]),
        );
        if !envelope.authorization.is_allowed() {
            return GovernedAction::denied(envelope);
        }
        if let Err(error) = request.validate() {
            envelope = envelope.deny(GovernanceDenialCode::StateConflict, error.message);
            return GovernedAction::denied(envelope);
        }
        let Some(existing) = self.sessions.get(&request.acp_session_id).cloned() else {
            envelope = envelope.deny(
                GovernanceDenialCode::UnknownTarget,
                "ACP session is not accessible",
            );
            return GovernedAction::denied(envelope);
        };
        let principal = principal.expect("allowed envelope always has a principal");
        if !principal.matches_identity(&existing.owner_subject, existing.owner_tenant_id.as_deref())
        {
            envelope = envelope.deny(
                GovernanceDenialCode::PrincipalMismatch,
                "ACP session is not accessible",
            );
            return GovernedAction::denied(envelope);
        }
        if existing.active_run.is_some() {
            envelope = envelope.deny(
                GovernanceDenialCode::StateConflict,
                "ACP session already has an active run",
            );
            return GovernedAction::denied(envelope);
        }

        let mapping = AcpRunMapping {
            acp_session_id: existing.mapping.acp_session_id,
            session_id: existing.mapping.session_id,
            prompt_id: request.prompt_id.clone(),
            run_id: self.next_id("acp-run"),
        };
        envelope.correlation.session_id = Some(mapping.session_id.clone());
        envelope.correlation.run_id = Some(mapping.run_id.clone());
        self.sessions
            .get_mut(&request.acp_session_id)
            .expect("session was resolved")
            .active_run = Some(mapping.clone());
        GovernedAction::from_envelope(envelope, AcpAction::Prompt { request, mapping })
    }

    pub fn prepare_cancel(
        &self,
        acp_session_id: &str,
        correlation: CorrelationIdentity,
        principal: Option<&ProtocolPrincipal>,
    ) -> GovernedAction<AcpAction> {
        let mut envelope = GovernanceEnvelope::evaluate(
            ProtocolKind::Acp,
            correlation,
            principal,
            "session/cancel",
            acp_session_id,
            scopes(&[CANCEL_SCOPE]),
        );
        let Some(record) = self.sessions.get(acp_session_id) else {
            envelope = envelope.deny(
                GovernanceDenialCode::UnknownTarget,
                "ACP session is not accessible",
            );
            return GovernedAction::denied(envelope);
        };
        if !envelope.authorization.is_allowed() {
            return GovernedAction::denied(envelope);
        }
        if principal.is_none_or(|value| {
            !value.matches_identity(&record.owner_subject, record.owner_tenant_id.as_deref())
        }) {
            envelope = envelope.deny(
                GovernanceDenialCode::PrincipalMismatch,
                "ACP session is not accessible",
            );
            return GovernedAction::denied(envelope);
        }
        let Some(mapping) = record.active_run.clone() else {
            envelope = envelope.deny(
                GovernanceDenialCode::StateConflict,
                "ACP session has no active run",
            );
            return GovernedAction::denied(envelope);
        };
        envelope.correlation.session_id = Some(mapping.session_id.clone());
        envelope.correlation.run_id = Some(mapping.run_id.clone());
        GovernedAction::from_envelope(envelope, AcpAction::Cancel { mapping })
    }

    /// Runtime-side terminal acknowledgement; no protocol request enters through this method.
    pub fn finish_run(&mut self, acp_session_id: &str, run_id: &str) -> ProtocolResult<()> {
        let record = self
            .sessions
            .get_mut(acp_session_id)
            .ok_or_else(|| ProtocolError::not_found("ACP session is not registered"))?;
        if record
            .active_run
            .as_ref()
            .is_none_or(|mapping| mapping.run_id != run_id)
        {
            return Err(ProtocolError::conflict("ACP run is not active"));
        }
        record.active_run = None;
        Ok(())
    }

    pub fn map_event(
        &self,
        mapping: &AcpRunMapping,
        event: AcpRuntimeEvent,
    ) -> ProtocolResult<AcpSessionEvent> {
        let record = self
            .sessions
            .get(&mapping.acp_session_id)
            .ok_or_else(|| ProtocolError::not_found("ACP session is not registered"))?;
        if record.active_run.as_ref() != Some(mapping) {
            return Err(ProtocolError::conflict(
                "ACP event does not belong to the active run",
            ));
        }
        let update = match event {
            AcpRuntimeEvent::AgentMessageChunk { text } => {
                AcpSessionUpdate::AgentMessageChunk { text }
            }
            AcpRuntimeEvent::AgentThoughtChunk { text } => {
                AcpSessionUpdate::AgentThoughtChunk { text }
            }
            AcpRuntimeEvent::ToolCall {
                tool_call_id,
                title,
                raw_input,
            } => AcpSessionUpdate::ToolCall {
                tool_call_id,
                title,
                raw_input,
            },
            AcpRuntimeEvent::ToolCallUpdate {
                tool_call_id,
                status,
                message,
            } => AcpSessionUpdate::ToolCallUpdate {
                tool_call_id,
                status,
                message,
            },
            AcpRuntimeEvent::Plan { entries } => AcpSessionUpdate::Plan { entries },
            AcpRuntimeEvent::Usage {
                input_tokens,
                output_tokens,
            } => AcpSessionUpdate::UsageUpdate {
                input_tokens,
                output_tokens,
            },
            AcpRuntimeEvent::Completed => AcpSessionUpdate::Completed,
            AcpRuntimeEvent::Failed { message } => AcpSessionUpdate::Failed { message },
            AcpRuntimeEvent::Cancelled => AcpSessionUpdate::Cancelled,
        };
        Ok(AcpSessionEvent {
            acp_session_id: mapping.acp_session_id.clone(),
            run_id: mapping.run_id.clone(),
            update,
        })
    }

    fn next_id(&mut self, prefix: &str) -> String {
        let sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.saturating_add(1);
        format!("{prefix}-{sequence:016}")
    }
}
