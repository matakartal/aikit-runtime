//! Plan mode — the agent proposes a plan; a human approves, edits, or rejects it *before* anything
//! executes.
//!
//! This is the strongest human-in-the-loop primitive (Claude Code's `plan` mode, grok-build's plan
//! mode): instead of approving tool calls one by one after the agent has already committed to a
//! direction, the human reviews the whole intended approach up front. On approval the (possibly
//! human-edited) plan is what runs; on rejection the reason is fed back so the agent replans.
//!
//! The primitive is transport-agnostic: a model emits a [`Plan`] (parse it from JSON), a
//! [`PlanReviewer`] (a human UI, a policy, or an approval callback) gates it, and [`review_plan`]
//! yields the approved plan or a rejection. Nothing here executes tools — it decides *what* to run.

use crate::error::{AikitError, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// A single step of a proposed plan.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct PlanStep {
    pub description: String,
    /// The tool this step intends to use, if known (useful for risk display and gating).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool: Option<String>,
}

impl PlanStep {
    pub fn new(description: impl Into<String>) -> Self {
        PlanStep {
            description: description.into(),
            tool: None,
        }
    }
    pub fn with_tool(description: impl Into<String>, tool: impl Into<String>) -> Self {
        PlanStep {
            description: description.into(),
            tool: Some(tool.into()),
        }
    }
}

/// A plan the agent proposes before acting.
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize, Serialize)]
pub struct Plan {
    pub goal: String,
    #[serde(default)]
    pub steps: Vec<PlanStep>,
}

impl Plan {
    pub fn new(goal: impl Into<String>) -> Self {
        Plan {
            goal: goal.into(),
            steps: Vec::new(),
        }
    }

    /// Builder: append a plain step.
    pub fn step(mut self, description: impl Into<String>) -> Self {
        self.steps.push(PlanStep::new(description));
        self
    }

    /// Builder: append a step that will use `tool`.
    pub fn tool_step(mut self, description: impl Into<String>, tool: impl Into<String>) -> Self {
        self.steps.push(PlanStep::with_tool(description, tool));
        self
    }

    /// Parse a plan the model emitted as JSON: `{ "goal": "...", "steps": [ {"description": "..."} ] }`.
    pub fn from_json(s: &str) -> Result<Self> {
        serde_json::from_str(s).map_err(|e| AikitError::Other(format!("invalid plan JSON: {e}")))
    }

    /// Serialize the plan to JSON (for display / persistence / re-prompting).
    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).unwrap_or_else(|_| "{}".to_string())
    }

    /// The distinct tools this plan intends to use, in first-seen order.
    pub fn tools(&self) -> Vec<&str> {
        let mut seen = Vec::new();
        for step in &self.steps {
            if let Some(t) = &step.tool {
                if !seen.contains(&t.as_str()) {
                    seen.push(t.as_str());
                }
            }
        }
        seen
    }
}

/// A human/host decision on a proposed plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlanReview {
    /// Run the plan as proposed.
    Approve,
    /// Run this human-edited plan instead of the proposed one.
    ApproveRevised(Plan),
    /// Do not run; the reason is fed back to the agent to replan.
    Reject(String),
}

/// Gates a proposed plan before execution. This is the plan-mode analogue of a tool approver.
#[async_trait]
pub trait PlanReviewer: Send + Sync {
    async fn review(&self, plan: &Plan) -> PlanReview;
}

/// The result of reviewing a plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlanOutcome {
    /// Execute this (possibly human-edited) plan.
    Approved(Plan),
    /// Do not execute; feed this reason back to the agent.
    Rejected(String),
}

impl PlanOutcome {
    pub fn is_approved(&self) -> bool {
        matches!(self, PlanOutcome::Approved(_))
    }
}

/// Run `plan` through `reviewer`. In plan mode NOTHING executes until this returns
/// [`PlanOutcome::Approved`] — the human sees the whole approach first.
pub async fn review_plan(plan: Plan, reviewer: &dyn PlanReviewer) -> PlanOutcome {
    match reviewer.review(&plan).await {
        PlanReview::Approve => PlanOutcome::Approved(plan),
        PlanReview::ApproveRevised(revised) => PlanOutcome::Approved(revised),
        PlanReview::Reject(reason) => PlanOutcome::Rejected(reason),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_plan() -> Plan {
        Plan::new("refactor the auth module")
            .step("read the current auth code")
            .tool_step("run the tests", "Bash")
            .tool_step("apply the refactor", "Edit")
    }

    struct FixedReviewer(PlanReview);
    #[async_trait]
    impl PlanReviewer for FixedReviewer {
        async fn review(&self, _plan: &Plan) -> PlanReview {
            self.0.clone()
        }
    }

    #[test]
    fn builder_and_tools() {
        let plan = sample_plan();
        assert_eq!(plan.goal, "refactor the auth module");
        assert_eq!(plan.steps.len(), 3);
        assert_eq!(plan.tools(), vec!["Bash", "Edit"]);
    }

    #[test]
    fn json_round_trip() {
        let plan = sample_plan();
        let json = plan.to_json();
        let parsed = Plan::from_json(&json).unwrap();
        assert_eq!(parsed, plan);
    }

    #[test]
    fn parses_a_minimal_model_plan() {
        let plan =
            Plan::from_json(r#"{ "goal": "do X", "steps": [ { "description": "step one" } ] }"#)
                .unwrap();
        assert_eq!(plan.goal, "do X");
        assert_eq!(plan.steps[0].description, "step one");
        assert_eq!(plan.steps[0].tool, None);
    }

    #[tokio::test]
    async fn approve_yields_the_original_plan() {
        let plan = sample_plan();
        let outcome = review_plan(plan.clone(), &FixedReviewer(PlanReview::Approve)).await;
        assert_eq!(outcome, PlanOutcome::Approved(plan));
        assert!(outcome.is_approved());
    }

    #[tokio::test]
    async fn approve_revised_yields_the_edited_plan() {
        let proposed = sample_plan();
        let edited = Plan::new("refactor the auth module")
            .step("read the code only — skip the risky refactor");
        let outcome = review_plan(
            proposed,
            &FixedReviewer(PlanReview::ApproveRevised(edited.clone())),
        )
        .await;
        assert_eq!(outcome, PlanOutcome::Approved(edited));
    }

    #[tokio::test]
    async fn reject_yields_the_reason() {
        let outcome = review_plan(
            sample_plan(),
            &FixedReviewer(PlanReview::Reject(
                "too risky; propose a read-only plan".into(),
            )),
        )
        .await;
        assert_eq!(
            outcome,
            PlanOutcome::Rejected("too risky; propose a read-only plan".into())
        );
        assert!(!outcome.is_approved());
    }
}
