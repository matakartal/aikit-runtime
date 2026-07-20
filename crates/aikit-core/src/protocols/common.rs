//! Shared, transport-neutral ingress contracts for governed protocols.
//!
//! Protocol adapters stop at [`GovernedAction`]. They never call a provider or tool executor.
//! A host must first inspect the embedded [`GovernanceEnvelope`] and can only extract an action
//! when authorization succeeded. Denied requests still carry correlation data for deterministic
//! audit trails.

use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use thiserror::Error;

pub const PROTOCOL_CONTRACT_VERSION: u32 = 1;

pub type ProtocolResult<T> = Result<T, ProtocolError>;

#[derive(Debug, Clone, PartialEq, Eq, Error, Serialize, Deserialize)]
#[error("{code:?}: {message}")]
pub struct ProtocolError {
    pub code: ProtocolErrorCode,
    pub message: String,
}

impl ProtocolError {
    pub fn invalid(message: impl Into<String>) -> Self {
        Self::new(ProtocolErrorCode::InvalidRequest, message)
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(ProtocolErrorCode::NotFound, message)
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        Self::new(ProtocolErrorCode::Conflict, message)
    }

    pub fn invalid_transition(message: impl Into<String>) -> Self {
        Self::new(ProtocolErrorCode::InvalidTransition, message)
    }

    pub fn new(code: ProtocolErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProtocolErrorCode {
    InvalidRequest,
    Unauthorized,
    Forbidden,
    NotFound,
    Conflict,
    InvalidTransition,
    Cancelled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProtocolKind {
    Mcp,
    A2a,
    Acp,
}

/// Stable identity propagated from protocol ingress into runtime governance and audit records.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CorrelationIdentity {
    pub correlation_id: String,
    pub request_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
}

impl CorrelationIdentity {
    pub fn new(
        correlation_id: impl Into<String>,
        request_id: impl Into<String>,
    ) -> ProtocolResult<Self> {
        let identity = Self {
            correlation_id: correlation_id.into(),
            request_id: request_id.into(),
            session_id: None,
            run_id: None,
        };
        identity.validate()?;
        Ok(identity)
    }

    pub fn with_runtime(
        mut self,
        session_id: impl Into<String>,
        run_id: impl Into<String>,
    ) -> ProtocolResult<Self> {
        self.session_id = Some(session_id.into());
        self.run_id = Some(run_id.into());
        self.validate()?;
        Ok(self)
    }

    pub fn validate(&self) -> ProtocolResult<()> {
        validate_identifier("correlation_id", &self.correlation_id)?;
        validate_identifier("request_id", &self.request_id)?;
        if let Some(session_id) = &self.session_id {
            validate_identifier("session_id", session_id)?;
        }
        if let Some(run_id) = &self.run_id {
            validate_identifier("run_id", run_id)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProtocolPrincipal {
    pub subject: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<String>,
    #[serde(default)]
    pub scopes: BTreeSet<String>,
}

impl ProtocolPrincipal {
    pub fn new(
        subject: impl Into<String>,
        scopes: impl IntoIterator<Item = impl Into<String>>,
    ) -> ProtocolResult<Self> {
        let principal = Self {
            subject: subject.into(),
            tenant_id: None,
            scopes: scopes.into_iter().map(Into::into).collect(),
        };
        principal.validate()?;
        Ok(principal)
    }

    pub fn with_tenant(mut self, tenant_id: impl Into<String>) -> ProtocolResult<Self> {
        self.tenant_id = Some(tenant_id.into());
        self.validate()?;
        Ok(self)
    }

    pub fn allows(&self, required_scopes: &BTreeSet<String>) -> bool {
        self.scopes.contains("*") || required_scopes.is_subset(&self.scopes)
    }

    pub(crate) fn matches_identity(
        &self,
        owner_subject: &str,
        owner_tenant_id: Option<&str>,
    ) -> bool {
        self.subject == owner_subject && self.tenant_id.as_deref() == owner_tenant_id
    }

    fn validate(&self) -> ProtocolResult<()> {
        validate_identifier("principal subject", &self.subject)?;
        if let Some(tenant_id) = &self.tenant_id {
            validate_identifier("tenant_id", tenant_id)?;
        }
        for scope in &self.scopes {
            validate_identifier("scope", scope)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GovernanceDenialCode {
    MissingPrincipal,
    MissingScope,
    PrincipalMismatch,
    UnknownTarget,
    StateConflict,
    InvalidApproval,
    DuplicateConflict,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum GovernanceAuthorization {
    Allowed,
    Denied {
        code: GovernanceDenialCode,
        reason: String,
    },
}

impl GovernanceAuthorization {
    pub fn is_allowed(&self) -> bool {
        matches!(self, Self::Allowed)
    }
}

/// Audit-ready authorization context emitted for every typed inbound operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GovernanceEnvelope {
    pub schema_version: u32,
    pub protocol: ProtocolKind,
    pub correlation: CorrelationIdentity,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub principal: Option<ProtocolPrincipal>,
    pub operation: String,
    pub target: String,
    #[serde(default)]
    pub required_scopes: BTreeSet<String>,
    pub authorization: GovernanceAuthorization,
}

impl GovernanceEnvelope {
    pub(crate) fn evaluate(
        protocol: ProtocolKind,
        correlation: CorrelationIdentity,
        principal: Option<&ProtocolPrincipal>,
        operation: impl Into<String>,
        target: impl Into<String>,
        required_scopes: BTreeSet<String>,
    ) -> Self {
        let authorization = match principal {
            None => GovernanceAuthorization::Denied {
                code: GovernanceDenialCode::MissingPrincipal,
                reason: "authenticated principal is required".into(),
            },
            Some(principal) if !principal.allows(&required_scopes) => {
                GovernanceAuthorization::Denied {
                    code: GovernanceDenialCode::MissingScope,
                    reason: "principal lacks a required protocol scope".into(),
                }
            }
            Some(_) => GovernanceAuthorization::Allowed,
        };
        Self {
            schema_version: PROTOCOL_CONTRACT_VERSION,
            protocol,
            correlation,
            principal: principal.cloned(),
            operation: operation.into(),
            target: target.into(),
            required_scopes,
            authorization,
        }
    }

    pub(crate) fn deny(mut self, code: GovernanceDenialCode, reason: impl Into<String>) -> Self {
        self.authorization = GovernanceAuthorization::Denied {
            code,
            reason: reason.into(),
        };
        self
    }
}

/// A transport-neutral protocol intent guarded by its authorization envelope.
///
/// The action is deliberately private. Callers cannot accidentally extract it from a denied
/// request; [`Self::into_authorized`] is the only consuming path.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct GovernedAction<T> {
    pub envelope: GovernanceEnvelope,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    action: Option<T>,
}

impl<T> GovernedAction<T> {
    pub(crate) fn from_envelope(envelope: GovernanceEnvelope, action: T) -> Self {
        let action = envelope.authorization.is_allowed().then_some(action);
        Self { envelope, action }
    }

    pub(crate) fn denied(envelope: GovernanceEnvelope) -> Self {
        Self {
            envelope,
            action: None,
        }
    }

    pub fn is_authorized(&self) -> bool {
        self.action.is_some() && self.envelope.authorization.is_allowed()
    }

    pub fn action(&self) -> Option<&T> {
        self.action.as_ref()
    }

    pub fn into_authorized(self) -> ProtocolResult<(GovernanceEnvelope, T)> {
        match (self.envelope.authorization.clone(), self.action) {
            (GovernanceAuthorization::Allowed, Some(action)) => Ok((self.envelope, action)),
            (GovernanceAuthorization::Denied { code, reason }, _) => Err(ProtocolError::new(
                match code {
                    GovernanceDenialCode::MissingPrincipal => ProtocolErrorCode::Unauthorized,
                    _ => ProtocolErrorCode::Forbidden,
                },
                reason,
            )),
            (GovernanceAuthorization::Allowed, None) => Err(ProtocolError::new(
                ProtocolErrorCode::Conflict,
                "authorized protocol envelope has no action",
            )),
        }
    }
}

pub(crate) fn scopes(values: &[&str]) -> BTreeSet<String> {
    values.iter().map(|value| (*value).to_owned()).collect()
}

pub(crate) fn validate_scope_set(scopes: &BTreeSet<String>) -> ProtocolResult<()> {
    for scope in scopes {
        validate_identifier("protocol scope", scope)?;
    }
    Ok(())
}

pub(crate) fn validate_identifier(field: &str, value: &str) -> ProtocolResult<()> {
    if value.trim().is_empty() {
        return Err(ProtocolError::invalid(format!("{field} must not be empty")));
    }
    if value.len() > 512 {
        return Err(ProtocolError::invalid(format!(
            "{field} must not exceed 512 bytes"
        )));
    }
    if value.chars().any(char::is_control) {
        return Err(ProtocolError::invalid(format!(
            "{field} must not contain control characters"
        )));
    }
    Ok(())
}
