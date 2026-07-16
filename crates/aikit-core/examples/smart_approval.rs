//! Risk-based smart approval demo for `aikit_core`.
//!
//! Shows `HeuristicRiskScorer` classifying tool calls and `SmartApprover::heuristic`
//! auto-approving low risk while escalating medium/high risk to a human approver.
//!
//! Run: cargo run -p aikit-runtime-core --example smart_approval

use aikit_core::{
    ApprovalDecision, ApprovalRequest, HeuristicRiskScorer, RiskScorer, SmartApprover, ToolApprover,
};
use async_trait::async_trait;
use serde_json::json;
use std::sync::Arc;

struct AlwaysDenyHuman;

#[async_trait]
impl ToolApprover for AlwaysDenyHuman {
    async fn approve(&self, _request: ApprovalRequest) -> ApprovalDecision {
        println!("  [human consulted]");
        ApprovalDecision::deny("human declined")
    }
}

fn req(tool: &str, input: serde_json::Value) -> ApprovalRequest {
    ApprovalRequest {
        run_id: "run-demo".to_string(),
        turn: 0,
        tool_use_id: "tool-use-0".to_string(),
        tool: tool.to_string(),
        input,
    }
}

#[tokio::main]
async fn main() {
    println!("=== Part 1: HeuristicRiskScorer ===");
    let scorer = HeuristicRiskScorer::default();

    let cases = [
        ("Read", json!({"path": "notes.txt"})),
        ("Bash", json!({"command": "ls -la"})),
        ("Bash", json!({"command": "rm -rf /"})),
    ];

    for (tool, input) in &cases {
        let level = scorer.score(tool, input);
        println!("{tool} {input} -> {level:?}");
    }

    println!();
    println!("=== Part 2: SmartApprover (auto-approve Low, escalate rest) ===");
    let approver = SmartApprover::heuristic(Arc::new(AlwaysDenyHuman));

    let d1 = approver
        .approve(req("Read", json!({"path": "notes.txt"})))
        .await;
    if matches!(d1, ApprovalDecision::Allow { .. }) {
        println!("Read {{\"path\":\"notes.txt\"}} -> Allowed");
    } else {
        println!("Read {{\"path\":\"notes.txt\"}} -> Denied");
    }

    let d2 = approver
        .approve(req("Bash", json!({"command": "rm -rf /"})))
        .await;
    if matches!(d2, ApprovalDecision::Allow { .. }) {
        println!("Bash {{\"command\":\"rm -rf /\"}} -> Allowed");
    } else {
        println!("Bash {{\"command\":\"rm -rf /\"}} -> Denied");
    }

    println!();
    println!("SmartApprover keeps the gate but only spends human attention on risky calls.");
}
