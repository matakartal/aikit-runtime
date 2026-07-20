//! A2A 1.0 identifiers, idempotency, and task lifecycle mapping.

use super::common::{
    scopes, validate_identifier, CorrelationIdentity, GovernanceDenialCode, GovernanceEnvelope,
    GovernedAction, ProtocolError, ProtocolKind, ProtocolPrincipal, ProtocolResult,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

pub const A2A_PROTOCOL_VERSION: &str = "1.0";

const SEND_MESSAGE_SCOPE: &str = "a2a:message:send";
const TASK_READ_SCOPE: &str = "a2a:tasks:read";
const TASK_CANCEL_SCOPE: &str = "a2a:tasks:cancel";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum A2aRole {
    #[serde(rename = "ROLE_USER")]
    User,
    #[serde(rename = "ROLE_AGENT")]
    Agent,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum A2aPart {
    Text { text: String },
    Data { data: Value },
    File { uri: String, media_type: String },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct A2aMessage {
    pub message_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    pub role: A2aRole,
    pub parts: Vec<A2aPart>,
    #[serde(default)]
    pub metadata: BTreeMap<String, Value>,
}

impl A2aMessage {
    pub fn validate(&self) -> ProtocolResult<()> {
        validate_identifier("A2A message_id", &self.message_id)?;
        if let Some(context_id) = &self.context_id {
            validate_identifier("A2A context_id", context_id)?;
        }
        if let Some(task_id) = &self.task_id {
            validate_identifier("A2A task_id", task_id)?;
        }
        if self.parts.is_empty() {
            return Err(ProtocolError::invalid(
                "A2A message must contain at least one part",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum A2aTaskState {
    #[serde(rename = "TASK_STATE_SUBMITTED")]
    Submitted,
    #[serde(rename = "TASK_STATE_WORKING")]
    Working,
    #[serde(rename = "TASK_STATE_INPUT_REQUIRED")]
    InputRequired,
    #[serde(rename = "TASK_STATE_AUTH_REQUIRED")]
    AuthRequired,
    #[serde(rename = "TASK_STATE_COMPLETED")]
    Completed,
    #[serde(rename = "TASK_STATE_FAILED")]
    Failed,
    #[serde(rename = "TASK_STATE_CANCELED")]
    Cancelled,
    #[serde(rename = "TASK_STATE_REJECTED")]
    Rejected,
}

impl A2aTaskState {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Cancelled | Self::Rejected
        )
    }
}

/// Exact A2A-to-runtime identity projection required for replay and audit correlation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct A2aRunMapping {
    pub context_id: String,
    pub session_id: String,
    pub task_id: String,
    pub run_id: String,
    pub message_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct A2aTaskRecord {
    pub mapping: A2aRunMapping,
    pub state: A2aTaskState,
    pub owner_subject: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_tenant_id: Option<String>,
    pub created_revision: u64,
    pub updated_revision: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status_message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct A2aMessageReceipt {
    pub message: A2aMessage,
    pub mapping: A2aRunMapping,
    pub owner_subject: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_tenant_id: Option<String>,
    pub accepted_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct A2aContextOwner {
    subject: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    tenant_id: Option<String>,
}

impl A2aContextOwner {
    fn from_principal(principal: &ProtocolPrincipal) -> Self {
        Self {
            subject: principal.subject.clone(),
            tenant_id: principal.tenant_id.clone(),
        }
    }

    fn matches(&self, principal: &ProtocolPrincipal) -> bool {
        principal.matches_identity(&self.subject, self.tenant_id.as_deref())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum A2aAction {
    DispatchMessage {
        message: A2aMessage,
        mapping: A2aRunMapping,
        resumed_from: Option<A2aTaskState>,
    },
    DuplicateMessage {
        receipt: A2aMessageReceipt,
    },
    GetTask {
        task: A2aTaskRecord,
    },
    CancelTask {
        task: A2aTaskRecord,
    },
}

/// Serializable state machine implementing context/session and task/run mappings.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct A2aMapper {
    contexts: BTreeMap<String, String>,
    context_owners: BTreeMap<String, A2aContextOwner>,
    tasks: BTreeMap<String, A2aTaskRecord>,
    receipts: BTreeMap<String, A2aMessageReceipt>,
    next_sequence: u64,
    revision: u64,
}

impl Default for A2aMapper {
    fn default() -> Self {
        Self::new()
    }
}

impl A2aMapper {
    pub fn new() -> Self {
        Self {
            contexts: BTreeMap::new(),
            context_owners: BTreeMap::new(),
            tasks: BTreeMap::new(),
            receipts: BTreeMap::new(),
            next_sequence: 1,
            revision: 0,
        }
    }

    pub fn contexts(&self) -> &BTreeMap<String, String> {
        &self.contexts
    }

    pub fn tasks(&self) -> &BTreeMap<String, A2aTaskRecord> {
        &self.tasks
    }

    pub fn receipts(&self) -> &BTreeMap<String, A2aMessageReceipt> {
        &self.receipts
    }

    pub fn revision(&self) -> u64 {
        self.revision
    }

    /// Prepare a `SendMessage` operation. Accepted message ids are durable idempotency keys.
    pub fn prepare_send_message(
        &mut self,
        message: A2aMessage,
        correlation: CorrelationIdentity,
        principal: Option<&ProtocolPrincipal>,
    ) -> GovernedAction<A2aAction> {
        let target = message
            .task_id
            .as_deref()
            .unwrap_or(message.message_id.as_str())
            .to_owned();
        let mut envelope = GovernanceEnvelope::evaluate(
            ProtocolKind::A2a,
            correlation,
            principal,
            "message/send",
            target,
            scopes(&[SEND_MESSAGE_SCOPE]),
        );
        if !envelope.authorization.is_allowed() {
            return GovernedAction::denied(envelope);
        }
        if let Err(error) = message.validate() {
            envelope = envelope.deny(GovernanceDenialCode::StateConflict, error.message);
            return GovernedAction::denied(envelope);
        }
        let principal = principal.expect("allowed envelope always has a principal");

        if let Some(receipt) = self.receipts.get(&message.message_id).cloned() {
            envelope.correlation.session_id = Some(receipt.mapping.session_id.clone());
            envelope.correlation.run_id = Some(receipt.mapping.run_id.clone());
            if !principal
                .matches_identity(&receipt.owner_subject, receipt.owner_tenant_id.as_deref())
            {
                envelope = envelope.deny(
                    GovernanceDenialCode::PrincipalMismatch,
                    "A2A message is not accessible",
                );
                return GovernedAction::denied(envelope);
            }
            if receipt.message != message {
                envelope = envelope.deny(
                    GovernanceDenialCode::DuplicateConflict,
                    "A2A message_id was reused with different content",
                );
                return GovernedAction::denied(envelope);
            }
            return GovernedAction::from_envelope(
                envelope,
                A2aAction::DuplicateMessage { receipt },
            );
        }

        let (mapping, resumed_from) = if let Some(task_id) = message.task_id.as_deref() {
            let Some(existing) = self.tasks.get(task_id).cloned() else {
                envelope = envelope.deny(
                    GovernanceDenialCode::UnknownTarget,
                    "A2A task is not accessible",
                );
                return GovernedAction::denied(envelope);
            };
            if !principal
                .matches_identity(&existing.owner_subject, existing.owner_tenant_id.as_deref())
            {
                envelope = envelope.deny(
                    GovernanceDenialCode::PrincipalMismatch,
                    "A2A task is not accessible",
                );
                return GovernedAction::denied(envelope);
            }
            if message
                .context_id
                .as_ref()
                .is_some_and(|context_id| context_id != &existing.mapping.context_id)
            {
                envelope = envelope.deny(
                    GovernanceDenialCode::StateConflict,
                    "A2A context_id does not match task_id",
                );
                return GovernedAction::denied(envelope);
            }
            if existing.state.is_terminal() {
                envelope = envelope.deny(
                    GovernanceDenialCode::StateConflict,
                    "terminal A2A task cannot accept another message",
                );
                return GovernedAction::denied(envelope);
            }

            let resumed_from = matches!(
                existing.state,
                A2aTaskState::InputRequired | A2aTaskState::AuthRequired
            )
            .then_some(existing.state);
            if resumed_from.is_some() {
                self.bump_revision();
                let task = self.tasks.get_mut(task_id).expect("task was resolved");
                task.state = A2aTaskState::Working;
                task.status_message = None;
                task.updated_revision = self.revision;
            }
            let mut mapping = existing.mapping;
            mapping.message_id = message.message_id.clone();
            (mapping, resumed_from)
        } else {
            let context_id = message
                .context_id
                .clone()
                .unwrap_or_else(|| self.next_id("a2a-context"));
            let session_id = if let Some(session_id) = self.contexts.get(&context_id) {
                if self
                    .context_owners
                    .get(&context_id)
                    .is_none_or(|owner| !owner.matches(principal))
                {
                    envelope = envelope.deny(
                        GovernanceDenialCode::PrincipalMismatch,
                        "A2A context is not accessible",
                    );
                    return GovernedAction::denied(envelope);
                }
                session_id.clone()
            } else {
                let session_id = self.next_id("a2a-session");
                self.contexts.insert(context_id.clone(), session_id.clone());
                self.context_owners.insert(
                    context_id.clone(),
                    A2aContextOwner::from_principal(principal),
                );
                session_id
            };
            let task_id = self.next_id("a2a-task");
            let run_id = self.next_id("a2a-run");
            self.bump_revision();
            let mapping = A2aRunMapping {
                context_id,
                session_id,
                task_id: task_id.clone(),
                run_id,
                message_id: message.message_id.clone(),
            };
            self.tasks.insert(
                task_id,
                A2aTaskRecord {
                    mapping: mapping.clone(),
                    state: A2aTaskState::Working,
                    owner_subject: principal.subject.clone(),
                    owner_tenant_id: principal.tenant_id.clone(),
                    created_revision: self.revision,
                    updated_revision: self.revision,
                    status_message: None,
                },
            );
            (mapping, None)
        };

        envelope.correlation.session_id = Some(mapping.session_id.clone());
        envelope.correlation.run_id = Some(mapping.run_id.clone());
        let mut normalized = message.clone();
        normalized.context_id = Some(mapping.context_id.clone());
        normalized.task_id = Some(mapping.task_id.clone());

        self.bump_revision();
        let receipt = A2aMessageReceipt {
            message,
            mapping: mapping.clone(),
            owner_subject: principal.subject.clone(),
            owner_tenant_id: principal.tenant_id.clone(),
            accepted_revision: self.revision,
        };
        self.receipts
            .insert(receipt.message.message_id.clone(), receipt);
        GovernedAction::from_envelope(
            envelope,
            A2aAction::DispatchMessage {
                message: normalized,
                mapping,
                resumed_from,
            },
        )
    }

    pub fn prepare_get_task(
        &self,
        task_id: &str,
        correlation: CorrelationIdentity,
        principal: Option<&ProtocolPrincipal>,
    ) -> GovernedAction<A2aAction> {
        let Some(task) = self.tasks.get(task_id).cloned() else {
            return denied_unknown_task(
                correlation,
                principal,
                "tasks/get",
                task_id,
                TASK_READ_SCOPE,
            );
        };
        let envelope = task_envelope(&task, correlation, principal, "tasks/get", TASK_READ_SCOPE);
        GovernedAction::from_envelope(envelope, A2aAction::GetTask { task })
    }

    pub fn prepare_cancel_task(
        &mut self,
        task_id: &str,
        correlation: CorrelationIdentity,
        principal: Option<&ProtocolPrincipal>,
    ) -> GovernedAction<A2aAction> {
        let Some(existing) = self.tasks.get(task_id).cloned() else {
            return denied_unknown_task(
                correlation,
                principal,
                "tasks/cancel",
                task_id,
                TASK_CANCEL_SCOPE,
            );
        };
        let mut envelope = task_envelope(
            &existing,
            correlation,
            principal,
            "tasks/cancel",
            TASK_CANCEL_SCOPE,
        );
        if !envelope.authorization.is_allowed() {
            return GovernedAction::denied(envelope);
        }
        if existing.state == A2aTaskState::Cancelled {
            return GovernedAction::from_envelope(
                envelope,
                A2aAction::CancelTask { task: existing },
            );
        }
        if existing.state.is_terminal() {
            envelope = envelope.deny(
                GovernanceDenialCode::StateConflict,
                "terminal A2A task cannot be cancelled",
            );
            return GovernedAction::denied(envelope);
        }
        self.bump_revision();
        let task = self.tasks.get_mut(task_id).expect("task was resolved");
        task.state = A2aTaskState::Cancelled;
        task.status_message = Some("cancellation requested".into());
        task.updated_revision = self.revision;
        GovernedAction::from_envelope(envelope, A2aAction::CancelTask { task: task.clone() })
    }

    /// Receiver-side transition used when an agent asks the A2A client for more input.
    pub fn require_input(
        &mut self,
        task_id: &str,
        status_message: impl Into<String>,
    ) -> ProtocolResult<()> {
        self.transition_task(
            task_id,
            A2aTaskState::InputRequired,
            Some(status_message.into()),
        )
    }

    pub fn transition_task(
        &mut self,
        task_id: &str,
        next: A2aTaskState,
        status_message: Option<String>,
    ) -> ProtocolResult<()> {
        let current = self
            .tasks
            .get(task_id)
            .ok_or_else(|| ProtocolError::not_found("A2A task is not registered"))?
            .state;
        if !valid_transition(current, next) {
            return Err(ProtocolError::invalid_transition(format!(
                "invalid A2A task transition: {current:?} -> {next:?}"
            )));
        }
        self.bump_revision();
        let task = self.tasks.get_mut(task_id).expect("task exists");
        task.state = next;
        task.status_message = status_message;
        task.updated_revision = self.revision;
        Ok(())
    }

    fn next_id(&mut self, prefix: &str) -> String {
        let sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.saturating_add(1);
        format!("{prefix}-{sequence:016}")
    }

    fn bump_revision(&mut self) {
        self.revision = self.revision.saturating_add(1);
    }
}

fn valid_transition(current: A2aTaskState, next: A2aTaskState) -> bool {
    if current == next {
        return true;
    }
    match current {
        A2aTaskState::Submitted => matches!(
            next,
            A2aTaskState::Working | A2aTaskState::Rejected | A2aTaskState::Cancelled
        ),
        A2aTaskState::Working => matches!(
            next,
            A2aTaskState::InputRequired
                | A2aTaskState::AuthRequired
                | A2aTaskState::Completed
                | A2aTaskState::Failed
                | A2aTaskState::Cancelled
                | A2aTaskState::Rejected
        ),
        A2aTaskState::InputRequired | A2aTaskState::AuthRequired => matches!(
            next,
            A2aTaskState::Working
                | A2aTaskState::Failed
                | A2aTaskState::Cancelled
                | A2aTaskState::Rejected
        ),
        A2aTaskState::Completed
        | A2aTaskState::Failed
        | A2aTaskState::Cancelled
        | A2aTaskState::Rejected => false,
    }
}

fn task_envelope(
    task: &A2aTaskRecord,
    mut correlation: CorrelationIdentity,
    principal: Option<&ProtocolPrincipal>,
    operation: &str,
    scope: &str,
) -> GovernanceEnvelope {
    correlation.session_id = Some(task.mapping.session_id.clone());
    correlation.run_id = Some(task.mapping.run_id.clone());
    let mut envelope = GovernanceEnvelope::evaluate(
        ProtocolKind::A2a,
        correlation,
        principal,
        operation,
        task.mapping.task_id.clone(),
        scopes(&[scope]),
    );
    if envelope.authorization.is_allowed()
        && principal.is_none_or(|value| {
            !value.matches_identity(&task.owner_subject, task.owner_tenant_id.as_deref())
        })
    {
        envelope = envelope.deny(
            GovernanceDenialCode::PrincipalMismatch,
            "A2A task is not accessible",
        );
    }
    envelope
}

fn denied_unknown_task(
    correlation: CorrelationIdentity,
    principal: Option<&ProtocolPrincipal>,
    operation: &str,
    task_id: &str,
    scope: &str,
) -> GovernedAction<A2aAction> {
    let envelope = GovernanceEnvelope::evaluate(
        ProtocolKind::A2a,
        correlation,
        principal,
        operation,
        task_id,
        scopes(&[scope]),
    )
    .deny(
        GovernanceDenialCode::UnknownTarget,
        "A2A task is not accessible",
    );
    GovernedAction::denied(envelope)
}
