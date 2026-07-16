//! Reliability rules — make tool use *predictable*, separate from whether it is *safe*.
//!
//! The permission engine answers "is this call allowed?" (security). Reliability rules answer "does
//! this call make sense *right now*?" (control flow) — the pattern IBM's RequirementAgent
//! popularized: forbid a tool, require another tool to have run first, cap how many times a tool may
//! be used, or gate it until a minimum step. This curbs agent flailing (deploying before testing,
//! searching the web ten times, committing before the tests pass) while leaving reasoning free.
//!
//! It is pure, synchronous logic over a [`RunProgress`] record — deterministic and keyless. Rules
//! are declarative and load from JSON, mirroring [`PolicySpec`](super::policy::PolicySpec).

use serde::{Deserialize, Serialize};

/// A single reliability constraint on one tool.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct ToolRequirement {
    /// The tool this requirement governs.
    pub tool: String,
    /// Never allow this tool (a soft, control-flow forbid — distinct from a security deny).
    #[serde(default)]
    pub forbidden: bool,
    /// Require every one of these tools to have run at least once before this tool may run.
    #[serde(default)]
    pub only_after: Vec<String>,
    /// Cap the total number of times this tool may run in a single run.
    #[serde(default)]
    pub max_uses: Option<u32>,
    /// Do not allow this tool before this step index (0-based count of prior tool calls).
    #[serde(default)]
    pub min_step: Option<usize>,
}

impl ToolRequirement {
    /// Start a requirement for `tool` (allowed by default until a condition is added).
    pub fn for_tool(tool: impl Into<String>) -> Self {
        ToolRequirement {
            tool: tool.into(),
            ..Default::default()
        }
    }
    pub fn forbidden(mut self) -> Self {
        self.forbidden = true;
        self
    }
    pub fn only_after(mut self, tools: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.only_after = tools.into_iter().map(Into::into).collect();
        self
    }
    pub fn max_uses(mut self, n: u32) -> Self {
        self.max_uses = Some(n);
        self
    }
    pub fn min_step(mut self, step: usize) -> Self {
        self.min_step = Some(step);
        self
    }
}

/// A set of reliability requirements.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct ReliabilityPolicy {
    #[serde(default)]
    pub requirements: Vec<ToolRequirement>,
}

/// The outcome of a reliability check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReliabilityVerdict {
    /// The call is consistent with the reliability rules.
    Allow,
    /// The call violates a rule; the reason is model-facing (fed back so the agent adapts).
    Forbid(String),
}

/// The ordered history of tools already run in the current run. The caller records a tool AFTER it
/// executes; [`ReliabilityPolicy::check`] consults it before the next call.
#[derive(Debug, Clone, Default)]
pub struct RunProgress {
    used: Vec<String>,
}

impl RunProgress {
    pub fn new() -> Self {
        RunProgress::default()
    }
    /// Record that `tool` has run.
    pub fn record(&mut self, tool: impl Into<String>) {
        self.used.push(tool.into());
    }
    /// The current step index (number of prior tool calls).
    pub fn step(&self) -> usize {
        self.used.len()
    }
    /// How many times `tool` has run.
    pub fn count(&self, tool: &str) -> usize {
        self.used.iter().filter(|t| t.as_str() == tool).count()
    }
    /// Whether `tool` has run at least once.
    pub fn has_used(&self, tool: &str) -> bool {
        self.used.iter().any(|t| t.as_str() == tool)
    }
}

impl ReliabilityPolicy {
    pub fn new(requirements: Vec<ToolRequirement>) -> Self {
        ReliabilityPolicy { requirements }
    }

    /// Parse from JSON: `{ "requirements": [ { "tool": "deploy", "only_after": ["test"] }, ... ] }`.
    pub fn from_json(s: &str) -> crate::error::Result<Self> {
        serde_json::from_str(s)
            .map_err(|e| crate::error::AikitError::Other(format!("invalid reliability JSON: {e}")))
    }

    /// Check whether `tool` may run given the run's `progress`. Every requirement for `tool` must be
    /// satisfied; the first violation wins.
    pub fn check(&self, tool: &str, progress: &RunProgress) -> ReliabilityVerdict {
        for req in self.requirements.iter().filter(|r| r.tool == tool) {
            if req.forbidden {
                return ReliabilityVerdict::Forbid(format!("tool '{tool}' is forbidden by policy"));
            }
            if let Some(max) = req.max_uses {
                if progress.count(tool) >= max as usize {
                    return ReliabilityVerdict::Forbid(format!(
                        "tool '{tool}' already used {} time(s); max is {max}",
                        progress.count(tool)
                    ));
                }
            }
            for prereq in &req.only_after {
                if !progress.has_used(prereq) {
                    return ReliabilityVerdict::Forbid(format!(
                        "tool '{tool}' requires '{prereq}' to run first"
                    ));
                }
            }
            if let Some(min) = req.min_step {
                if progress.step() < min {
                    return ReliabilityVerdict::Forbid(format!(
                        "tool '{tool}' is not allowed before step {min} (now at step {})",
                        progress.step()
                    ));
                }
            }
        }
        ReliabilityVerdict::Allow
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn allow() -> ReliabilityVerdict {
        ReliabilityVerdict::Allow
    }

    #[test]
    fn no_requirement_means_allow() {
        let p = ReliabilityPolicy::default();
        assert_eq!(p.check("anything", &RunProgress::new()), allow());
    }

    #[test]
    fn forbidden_tool_is_forbidden() {
        let p = ReliabilityPolicy::new(vec![ToolRequirement::for_tool("Bash").forbidden()]);
        assert!(matches!(
            p.check("Bash", &RunProgress::new()),
            ReliabilityVerdict::Forbid(_)
        ));
        // A different tool is unaffected.
        assert_eq!(p.check("Read", &RunProgress::new()), allow());
    }

    #[test]
    fn only_after_requires_prerequisite() {
        let p = ReliabilityPolicy::new(vec![
            ToolRequirement::for_tool("deploy").only_after(["test"])
        ]);
        // Prerequisite not yet run → forbidden.
        assert!(matches!(
            p.check("deploy", &RunProgress::new()),
            ReliabilityVerdict::Forbid(_)
        ));
        // After the prerequisite runs → allowed.
        let mut progress = RunProgress::new();
        progress.record("test");
        assert_eq!(p.check("deploy", &progress), allow());
    }

    #[test]
    fn max_uses_caps_repeats() {
        let p = ReliabilityPolicy::new(vec![ToolRequirement::for_tool("web_search").max_uses(2)]);
        let mut progress = RunProgress::new();
        assert_eq!(p.check("web_search", &progress), allow()); // 0 used
        progress.record("web_search");
        assert_eq!(p.check("web_search", &progress), allow()); // 1 used
        progress.record("web_search");
        // 2 used, max 2 → the 3rd is forbidden.
        assert!(matches!(
            p.check("web_search", &progress),
            ReliabilityVerdict::Forbid(_)
        ));
    }

    #[test]
    fn min_step_gates_early_calls() {
        let p = ReliabilityPolicy::new(vec![ToolRequirement::for_tool("finalize").min_step(3)]);
        let mut progress = RunProgress::new();
        assert!(matches!(
            p.check("finalize", &progress),
            ReliabilityVerdict::Forbid(_)
        ));
        progress.record("a");
        progress.record("b");
        progress.record("c"); // now at step 3
        assert_eq!(p.check("finalize", &progress), allow());
    }

    #[test]
    fn multiple_conditions_on_one_tool_all_apply() {
        // deploy: only after test AND at most once.
        let p = ReliabilityPolicy::new(vec![ToolRequirement::for_tool("deploy")
            .only_after(["test"])
            .max_uses(1)]);
        let mut progress = RunProgress::new();
        progress.record("test");
        assert_eq!(p.check("deploy", &progress), allow());
        progress.record("deploy");
        // Second deploy exceeds max_uses even though the prerequisite is satisfied.
        assert!(matches!(
            p.check("deploy", &progress),
            ReliabilityVerdict::Forbid(_)
        ));
    }

    #[test]
    fn loads_from_json() {
        let p = ReliabilityPolicy::from_json(
            r#"{ "requirements": [
                { "tool": "deploy", "only_after": ["test"], "max_uses": 1 },
                { "tool": "rm_database", "forbidden": true }
            ] }"#,
        )
        .unwrap();
        assert!(matches!(
            p.check("rm_database", &RunProgress::new()),
            ReliabilityVerdict::Forbid(_)
        ));
        assert!(matches!(
            p.check("deploy", &RunProgress::new()),
            ReliabilityVerdict::Forbid(_) // test hasn't run
        ));
    }

    #[test]
    fn progress_counts_and_steps() {
        let mut progress = RunProgress::new();
        progress.record("a");
        progress.record("a");
        progress.record("b");
        assert_eq!(progress.count("a"), 2);
        assert_eq!(progress.count("b"), 1);
        assert_eq!(progress.count("c"), 0);
        assert!(progress.has_used("a"));
        assert!(!progress.has_used("c"));
        assert_eq!(progress.step(), 3);
    }
}
