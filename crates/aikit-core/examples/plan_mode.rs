//! Plan mode: the agent proposes a plan; a human reviews the whole approach BEFORE anything runs.
//!
//! Run: cargo run -p aikit-runtime-core --example plan_mode

use aikit_core::{review_plan, Plan, PlanOutcome, PlanReview, PlanReviewer};
use async_trait::async_trait;

/// A cautious human: approves read-only plans as-is, but revises a risky plan down to its
/// non-tool (read/think) steps before approving.
struct CautiousHuman;

#[async_trait]
impl PlanReviewer for CautiousHuman {
    async fn review(&self, plan: &Plan) -> PlanReview {
        let risky = plan
            .tools()
            .iter()
            .any(|t| matches!(*t, "Bash" | "Edit" | "Write"));
        if !risky {
            return PlanReview::Approve;
        }
        let mut safe = Plan::new(format!("{} (read-only, revised by a human)", plan.goal));
        for step in &plan.steps {
            if step.tool.is_none() {
                safe = safe.step(step.description.clone());
            }
        }
        PlanReview::ApproveRevised(safe)
    }
}

#[tokio::main]
async fn main() {
    let proposed = Plan::new("refactor the auth module")
        .step("read the current auth code")
        .tool_step("run the tests", "Bash")
        .tool_step("apply the refactor", "Edit");

    println!("Agent proposes this plan:\n{}\n", proposed.to_json());

    match review_plan(proposed, &CautiousHuman).await {
        PlanOutcome::Approved(plan) => {
            println!("Human approved (revised):\n{}", plan.to_json());
        }
        PlanOutcome::Rejected(reason) => println!("Human rejected: {reason}"),
    }

    println!("\n✅ Plan mode: the human saw the whole approach and gated it BEFORE any tool ran.");
}
