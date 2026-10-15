//! Plan mode: the agent proposes a plan; a human reviews the whole approach BEFORE anything runs.
//! Keyless and offline — no model is called; the plan is built by hand to show the review step.
//!
//! What it demonstrates:
//! - `Plan` / `PlanStep`: a goal plus ordered steps, each optionally naming the tool it will use.
//! - `PlanReviewer`: your approval surface (a UI, a policy, a person). It can `Approve`,
//!   `ApproveRevised(plan)` or reject.
//! - `review_plan`: runs the reviewer and returns a `PlanOutcome` the host acts on.
//!
//! Here `CautiousHuman` approves read-only plans as-is and strips tool steps from risky ones, so
//! the proposed 3-step plan comes back as a single read-only step.
//!
//! Run: `cargo run -p aikit-runtime-core --example plan_mode`

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
            println!(
                "Human approved this (possibly revised) plan:\n{}",
                plan.to_json()
            );
        }
        PlanOutcome::Rejected(reason) => println!("Human rejected: {reason}"),
    }

    println!(
        "\nOK: plan mode — the human saw the whole approach and gated it BEFORE any tool ran.\n\
         Note: review_plan only returns the decision; the host is responsible for executing just\n\
         the approved plan."
    );
}
