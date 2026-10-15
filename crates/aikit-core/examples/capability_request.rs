//! Agent self-extension, **human-governed**. The agent doesn't silently gain power: it *requests*
//! a capability it lacks, a human decides, the grant is recorded, and only then can it use the
//! tool. Keyless and offline — the agent's tool calls are made by hand to show each step.
//!
//! What it demonstrates:
//! - `CapabilityGate` wraps your real tools and refuses the gated ones (`Bash` here) until granted.
//! - The model asks via the `request_capability` tool (`REQUEST_CAPABILITY_TOOL`; expose
//!   `request_capability_tool()` in the run's tool list so the model can call it).
//! - `CapabilityBroker` forwards each request to a `ToolApprover` (your human/policy) and records
//!   grants for the run; `broker.granted()` lists them.
//!
//! Run: `cargo run -p aikit-runtime-core --example capability_request`
//!
//! Expected: step 1 is refused, step 2 is denied ("trust me"), step 3 is granted, step 4 runs.

use aikit_core::governance::capability::{
    CapabilityBroker, CapabilityGate, REQUEST_CAPABILITY_TOOL,
};
use aikit_core::governance::{ApprovalDecision, ApprovalRequest, ToolApprover};
use aikit_core::tools::ToolExecutor;
use aikit_core::Result;
use async_trait::async_trait;
use serde_json::json;
use std::sync::Arc;

/// A human who grants a request only if the reason mentions "test" — stands in for a real approval
/// UI / policy. The point: the DECISION is the human's, always.
struct PickyHuman;
#[async_trait]
impl ToolApprover for PickyHuman {
    async fn approve(&self, request: ApprovalRequest) -> ApprovalDecision {
        let reason = request
            .input
            .get("reason")
            .and_then(|r| r.as_str())
            .unwrap_or("");
        if reason.contains("test") {
            println!("  [human] approved: {}", reason);
            ApprovalDecision::allow(None)
        } else {
            println!("  [human] denied: {}", reason);
            ApprovalDecision::deny("insufficient justification")
        }
    }
}

/// The real tools behind the gate (here just an echo stand-in for Bash).
struct RealTools;
#[async_trait]
impl ToolExecutor for RealTools {
    async fn execute(&self, name: &str, _input: serde_json::Value) -> Result<String> {
        Ok(format!("<{name} ran>"))
    }
}

#[tokio::main]
async fn main() {
    // Bash is gated: the agent starts WITHOUT it and must ask a human to unlock it.
    let broker = Arc::new(CapabilityBroker::new(Arc::new(PickyHuman), "demo-run"));
    let gate = CapabilityGate::new(broker.clone(), Arc::new(RealTools), ["Bash"]);

    println!("1. Agent tries Bash before asking:");
    match gate.execute("Bash", json!({})).await {
        Ok(o) => panic!("Bash should have been gated before a grant, got: {o}"),
        Err(e) => println!("   -> refused: {e}"),
    }

    println!("\n2. Agent asks with a weak reason:");
    let r = gate
        .execute(
            REQUEST_CAPABILITY_TOOL,
            json!({ "capability": "Bash", "reason": "trust me" }),
        )
        .await
        .unwrap();
    println!("   -> {r}");

    println!("\n3. Agent asks with a good reason (mentions the tests):");
    let r = gate
        .execute(
            REQUEST_CAPABILITY_TOOL,
            json!({ "capability": "Bash", "reason": "run the test suite to verify my change" }),
        )
        .await
        .unwrap();
    println!("   -> {r}");

    println!("\n4. Now the agent uses the granted capability:");
    match gate
        .execute("Bash", json!({ "command": "cargo test" }))
        .await
    {
        Ok(o) => println!("   -> {o}"),
        Err(e) => println!("   -> refused: {e}"),
    }

    println!("\ngranted so far: {:?}", broker.granted());
    println!(
        "\nOK: self-extension stayed human-governed — the agent requested power, a human decided,\n\
         the grant was recorded, and nothing escalated silently."
    );
}
