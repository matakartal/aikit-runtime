//! Agent-native self-extension — **human-governed**.
//!
//! The rest of the governance harness answers "may the agent *use* this already-registered tool?".
//! This closes the loop the agent-native pitch actually promises: the agent can **request a
//! capability it does not yet have** (e.g. `Bash`), a human decides, and the grant is recorded —
//! *nothing is ever granted silently*. It is the alignment invariant made concrete: agent requests
//! → human policy decides → audited → capability unlocked.
//!
//! It reuses the existing [`ToolApprover`] seam (the same human callback that approves `ask` tools),
//! so a host wires one approver and gets both tool-use approval and capability requests. The
//! [`CapabilityGate`] executor demonstrates the whole flow: it answers the built-in
//! `request_capability` tool, and it refuses a *gated* tool until its capability has been granted.

use super::{ApprovalDecision, ApprovalRequest, ToolApprover};
use crate::error::{AikitError, Result};
use crate::tools::ToolExecutor;
use crate::types::ToolSpec;
use async_trait::async_trait;
use serde_json::{json, Value};
use std::collections::HashSet;
use std::sync::{Arc, RwLock};

/// The name of the built-in capability-request tool the agent calls.
pub const REQUEST_CAPABILITY_TOOL: &str = "request_capability";

/// The outcome of a capability request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CapabilityDecision {
    /// The human granted the capability; it is now usable for the rest of the run.
    Granted,
    /// The human declined, with a reason.
    Denied(String),
}

/// Routes an agent's capability requests to a human [`ToolApprover`] and records what was granted.
/// Grants are per-broker (per-run) — they never leak across runs, mirroring the approval cache.
pub struct CapabilityBroker {
    approver: Arc<dyn ToolApprover>,
    run_id: String,
    granted: RwLock<HashSet<String>>,
}

impl CapabilityBroker {
    pub fn new(approver: Arc<dyn ToolApprover>, run_id: impl Into<String>) -> Self {
        CapabilityBroker {
            approver,
            run_id: run_id.into(),
            granted: RwLock::new(HashSet::new()),
        }
    }

    /// Ask the human to grant `capability` (with the agent's stated `reason`). On approval the
    /// grant is recorded so later [`is_granted`](Self::is_granted) checks pass. Never grants without
    /// the human's decision.
    pub async fn request(&self, capability: &str, reason: &str) -> CapabilityDecision {
        let decision = self
            .approver
            .approve(ApprovalRequest {
                run_id: self.run_id.clone(),
                turn: 0,
                tool_use_id: format!("capreq:{capability}"),
                tool: REQUEST_CAPABILITY_TOOL.to_string(),
                input: json!({ "capability": capability, "reason": reason }),
            })
            .await;
        match decision {
            ApprovalDecision::Allow { .. } => {
                self.granted
                    .write()
                    .expect("capability lock")
                    .insert(capability.to_string());
                CapabilityDecision::Granted
            }
            ApprovalDecision::Deny { message, .. } => CapabilityDecision::Denied(message),
        }
    }

    /// Whether `capability` has been granted this run.
    pub fn is_granted(&self, capability: &str) -> bool {
        self.granted
            .read()
            .expect("capability lock")
            .contains(capability)
    }

    /// The capabilities granted so far (sorted).
    pub fn granted(&self) -> Vec<String> {
        let mut v: Vec<String> = self
            .granted
            .read()
            .expect("capability lock")
            .iter()
            .cloned()
            .collect();
        v.sort();
        v
    }
}

/// The [`ToolSpec`] for the built-in capability-request tool (advertise it so the model knows it can
/// ask for more power).
pub fn request_capability_tool() -> ToolSpec {
    ToolSpec {
        name: REQUEST_CAPABILITY_TOOL.to_string(),
        description: "Request a capability you do not currently have (for example \"Bash\"). \
             A human decides whether to grant it; nothing is granted silently. \
             Provide the capability name and a short reason."
            .to_string(),
        input_schema: json!({
            "type": "object",
            "required": ["capability", "reason"],
            "properties": {
                "capability": { "type": "string", "description": "The capability/tool to unlock." },
                "reason": { "type": "string", "description": "Why you need it." }
            }
        }),
    }
}

/// A [`ToolExecutor`] that adds human-governed self-extension around an inner executor:
///   - it answers the built-in `request_capability` tool by routing to the [`CapabilityBroker`], and
///   - it refuses any *gated* tool until its capability has been granted.
///
/// Non-gated tools pass straight through, so this composes over the built-in tools, MCP tools, or
/// any host executor.
pub struct CapabilityGate {
    broker: Arc<CapabilityBroker>,
    inner: Arc<dyn ToolExecutor>,
    gated: HashSet<String>,
}

impl CapabilityGate {
    /// Gate `gated` tool names behind a granted capability of the same name; everything else in
    /// `inner` runs normally.
    pub fn new(
        broker: Arc<CapabilityBroker>,
        inner: Arc<dyn ToolExecutor>,
        gated: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        CapabilityGate {
            broker,
            inner,
            gated: gated.into_iter().map(Into::into).collect(),
        }
    }
}

#[async_trait]
impl ToolExecutor for CapabilityGate {
    async fn execute(&self, name: &str, input: Value) -> Result<String> {
        if name == REQUEST_CAPABILITY_TOOL {
            let capability = input
                .get("capability")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    AikitError::ToolExecution("request_capability needs a 'capability'".into())
                })?;
            let reason = input.get("reason").and_then(Value::as_str).unwrap_or("");
            return Ok(match self.broker.request(capability, reason).await {
                CapabilityDecision::Granted => {
                    format!("capability '{capability}' granted — you may now use it")
                }
                CapabilityDecision::Denied(msg) => {
                    format!("capability '{capability}' denied: {msg}")
                }
            });
        }
        if self.gated.contains(name) && !self.broker.is_granted(name) {
            return Err(AikitError::PermissionDenied(format!(
                "tool '{name}' is gated; call {REQUEST_CAPABILITY_TOOL} to ask a human to grant it first"
            )));
        }
        self.inner.execute(name, input).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An approver that grants or denies based on a fixed answer.
    struct FixedApprover {
        grant: bool,
    }
    #[async_trait]
    impl ToolApprover for FixedApprover {
        async fn approve(&self, _request: ApprovalRequest) -> ApprovalDecision {
            if self.grant {
                ApprovalDecision::allow(None)
            } else {
                ApprovalDecision::deny("not this time")
            }
        }
    }

    /// An inner executor that just echoes which tool ran.
    struct Echo;
    #[async_trait]
    impl ToolExecutor for Echo {
        async fn execute(&self, name: &str, _input: Value) -> Result<String> {
            Ok(format!("ran {name}"))
        }
    }

    fn gate(grant: bool) -> CapabilityGate {
        let broker = Arc::new(CapabilityBroker::new(
            Arc::new(FixedApprover { grant }),
            "run-1",
        ));
        CapabilityGate::new(broker, Arc::new(Echo), ["Bash"])
    }

    #[tokio::test]
    async fn gated_tool_is_refused_until_requested() {
        let g = gate(true);
        // Before requesting, Bash is refused.
        let err = g.execute("Bash", json!({})).await.unwrap_err();
        assert!(matches!(err, AikitError::PermissionDenied(_)));
    }

    #[tokio::test]
    async fn request_then_use_when_the_human_grants() {
        let g = gate(true);
        let reply = g
            .execute(
                REQUEST_CAPABILITY_TOOL,
                json!({ "capability": "Bash", "reason": "run the tests" }),
            )
            .await
            .unwrap();
        assert!(reply.contains("granted"));
        // Now the gated tool runs.
        assert_eq!(g.execute("Bash", json!({})).await.unwrap(), "ran Bash");
    }

    #[tokio::test]
    async fn denied_request_leaves_the_tool_gated() {
        let g = gate(false);
        let reply = g
            .execute(
                REQUEST_CAPABILITY_TOOL,
                json!({ "capability": "Bash", "reason": "trust me" }),
            )
            .await
            .unwrap();
        assert!(reply.contains("denied"), "got: {reply}");
        // Still gated.
        assert!(g.execute("Bash", json!({})).await.is_err());
    }

    #[tokio::test]
    async fn non_gated_tools_pass_through() {
        let g = gate(true);
        assert_eq!(g.execute("Read", json!({})).await.unwrap(), "ran Read");
    }

    #[tokio::test]
    async fn broker_records_grants() {
        let broker = Arc::new(CapabilityBroker::new(
            Arc::new(FixedApprover { grant: true }),
            "run-1",
        ));
        assert!(!broker.is_granted("Bash"));
        assert_eq!(
            broker.request("Bash", "why").await,
            CapabilityDecision::Granted
        );
        assert!(broker.is_granted("Bash"));
        assert_eq!(broker.granted(), vec!["Bash".to_string()]);
    }
}
