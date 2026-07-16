//! The declarative permission engine — the governance pillar that lets a human hand an agent
//! powerful tools (bash, filesystem, network) *safely*. Every tool call is evaluated against a
//! [`PermissionMode`] default plus ordered allow/ask [`Rule`]s and globally authoritative deny
//! rules BEFORE it runs.

use regex::Regex;
use serde_json::Value;

/// The default posture when no rule matches.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PermissionMode {
    /// Allow by default; deny only what a `deny` rule matches.
    #[default]
    Allow,
    /// Deny by default; allow only what an `allow` rule matches (least-privilege).
    Deny,
    /// Ask by default; every unmatched tool escalates for approval.
    Ask,
}

/// What a matching rule does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuleEffect {
    Allow,
    Deny,
    Ask,
}

/// A single allow/deny/ask rule: matches a tool by name (exact or `*`) and, optionally, its input
/// by regex (e.g. deny `Bash` whose command matches `rm\s+-rf`).
///
/// The regex is tested against the **decoded** string value(s) of the input — NOT the serialized
/// JSON. Matching the JSON blob was a security hole: a literal tab/newline in a value serializes
/// to `\t`/`\n` (two ASCII chars), so a `\s`-class deny pattern would miss `rm<TAB>-rf` even
/// though the shell splits on the tab and runs `rm -rf`. Anchors (`^`/`$`) also behave as authors
/// expect against the raw value. Use [`Rule::matching_field`] to scope the match to one field.
#[derive(Debug, Clone)]
pub struct Rule {
    id: Option<String>,
    effect: RuleEffect,
    tool: String,
    pattern: Option<Regex>,
    field: Option<String>,
}

impl Rule {
    pub fn allow(tool: impl Into<String>) -> Self {
        Rule {
            id: None,
            effect: RuleEffect::Allow,
            tool: tool.into(),
            pattern: None,
            field: None,
        }
    }
    pub fn deny(tool: impl Into<String>) -> Self {
        Rule {
            id: None,
            effect: RuleEffect::Deny,
            tool: tool.into(),
            pattern: None,
            field: None,
        }
    }
    pub fn ask(tool: impl Into<String>) -> Self {
        Rule {
            id: None,
            effect: RuleEffect::Ask,
            tool: tool.into(),
            pattern: None,
            field: None,
        }
    }

    /// Stable audit identifier. Without one, the engine reports the ordered `rule:<index>` id.
    pub fn named(mut self, id: impl Into<String>) -> Self {
        self.id = Some(id.into());
        self
    }

    /// Constrain this rule to inputs where ANY decoded string value matches `pattern`.
    pub fn matching(mut self, pattern: &str) -> Result<Self, regex::Error> {
        self.pattern = Some(Regex::new(pattern)?);
        Ok(self)
    }

    /// Constrain this rule to a specific input FIELD (e.g. `"command"`), matched by regex against
    /// that field's raw string value. Stronger than [`matching`](Rule::matching): no cross-field
    /// over-match, and anchors bind to the value.
    pub fn matching_field(
        mut self,
        field: impl Into<String>,
        pattern: &str,
    ) -> Result<Self, regex::Error> {
        self.field = Some(field.into());
        self.pattern = Some(Regex::new(pattern)?);
        Ok(self)
    }

    fn matches(&self, tool: &str, input: &Value) -> bool {
        if self.tool != "*" && self.tool != tool {
            return false;
        }
        match &self.pattern {
            None => true,
            Some(re) => match &self.field {
                // Field-scoped: match only that field's raw string value.
                Some(field) => input
                    .get(field)
                    .and_then(Value::as_str)
                    .is_some_and(|v| re.is_match(v)),
                // Any-field: match against each decoded string leaf of the input.
                None => {
                    let mut leaves = Vec::new();
                    collect_strings(input, &mut leaves);
                    leaves.iter().any(|s| re.is_match(s))
                }
            },
        }
    }
}

/// Collect every decoded string leaf of a JSON value (so a regex sees raw values, control
/// characters and all — never the escaped JSON representation).
fn collect_strings<'a>(v: &'a Value, out: &mut Vec<&'a str>) {
    match v {
        Value::String(s) => out.push(s),
        Value::Array(a) => a.iter().for_each(|x| collect_strings(x, out)),
        Value::Object(m) => m.values().for_each(|x| collect_strings(x, out)),
        _ => {}
    }
}

/// The result of evaluating a tool call against the engine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Allow,
    Deny(String),
    /// Escalate to a human/approver — the engine alone cannot resolve it.
    Ask,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PermissionDecision {
    pub outcome: Outcome,
    pub source: String,
}

/// A permission mode plus an ordered rule list. Any matching deny rule is authoritative; when no
/// deny matches, the first matching allow/ask rule wins.
#[derive(Debug, Clone)]
pub struct PermissionEngine {
    pub mode: PermissionMode,
    pub rules: Vec<Rule>,
}

impl Default for PermissionEngine {
    /// Permissive: allow-by-default, no rules.
    fn default() -> Self {
        PermissionEngine {
            mode: PermissionMode::Allow,
            rules: Vec::new(),
        }
    }
}

impl PermissionEngine {
    pub fn new(mode: PermissionMode) -> Self {
        PermissionEngine {
            mode,
            rules: Vec::new(),
        }
    }

    pub fn with_rules(mode: PermissionMode, rules: Vec<Rule>) -> Self {
        PermissionEngine { mode, rules }
    }

    pub fn push(&mut self, rule: Rule) {
        self.rules.push(rule);
    }

    /// Evaluate a tool call. Any matching deny rule wins. Otherwise the first matching allow/ask
    /// rule decides, and the mode default applies when no rule matches.
    pub fn evaluate(&self, tool: &str, input: &Value) -> Outcome {
        self.evaluate_detailed(tool, input).outcome
    }

    pub fn evaluate_detailed(&self, tool: &str, input: &Value) -> PermissionDecision {
        let mut first_non_deny = None;
        for (index, rule) in self.rules.iter().enumerate() {
            if rule.matches(tool, input) {
                let source = rule.id.clone().unwrap_or_else(|| format!("rule:{index}"));
                match rule.effect {
                    RuleEffect::Deny => {
                        return PermissionDecision {
                            outcome: Outcome::Deny(format!("tool '{tool}' denied by rule")),
                            source,
                        };
                    }
                    RuleEffect::Allow | RuleEffect::Ask if first_non_deny.is_none() => {
                        let outcome = match rule.effect {
                            RuleEffect::Allow => Outcome::Allow,
                            RuleEffect::Ask => Outcome::Ask,
                            RuleEffect::Deny => unreachable!("deny returned above"),
                        };
                        first_non_deny = Some(PermissionDecision { outcome, source });
                    }
                    RuleEffect::Allow | RuleEffect::Ask => {}
                }
            }
        }
        if let Some(decision) = first_non_deny {
            return decision;
        }
        let outcome = match self.mode {
            PermissionMode::Allow => Outcome::Allow,
            PermissionMode::Deny => {
                Outcome::Deny(format!("tool '{tool}' denied by default (deny mode)"))
            }
            PermissionMode::Ask => Outcome::Ask,
        };
        PermissionDecision {
            outcome,
            source: "default_mode".into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn allow_mode_permits_by_default_but_deny_rule_wins() {
        let engine = PermissionEngine::with_rules(
            PermissionMode::Allow,
            vec![Rule::deny("Bash").matching(r"rm\s+-rf").unwrap()],
        );
        // A benign Bash command is allowed.
        assert_eq!(
            engine.evaluate("Bash", &json!({ "command": "ls -la" })),
            Outcome::Allow
        );
        // The dangerous one is denied by the rule.
        assert!(matches!(
            engine.evaluate("Bash", &json!({ "command": "rm -rf /" })),
            Outcome::Deny(_)
        ));
        // A different tool is unaffected.
        assert_eq!(
            engine.evaluate("Read", &json!({ "path": "a" })),
            Outcome::Allow
        );
    }

    #[test]
    fn deny_mode_is_least_privilege() {
        let engine = PermissionEngine::with_rules(
            PermissionMode::Deny,
            vec![Rule::allow("Read"), Rule::allow("Glob")],
        );
        assert_eq!(engine.evaluate("Read", &json!({})), Outcome::Allow);
        // Anything not explicitly allowed is denied.
        assert!(matches!(
            engine.evaluate("Bash", &json!({})),
            Outcome::Deny(_)
        ));
    }

    #[test]
    fn ask_rule_and_wildcard_tool() {
        let engine = PermissionEngine::with_rules(
            PermissionMode::Allow,
            vec![Rule::ask("*").matching(r"git\s+push").unwrap()],
        );
        assert_eq!(
            engine.evaluate("Bash", &json!({ "command": "git push origin" })),
            Outcome::Ask
        );
        assert_eq!(
            engine.evaluate("Bash", &json!({ "command": "git status" })),
            Outcome::Allow
        );
    }

    #[test]
    fn default_engine_is_permissive() {
        assert_eq!(
            PermissionEngine::default().evaluate("Anything", &json!({})),
            Outcome::Allow
        );
    }

    #[test]
    fn detailed_decision_reports_stable_rule_identity() {
        let engine = PermissionEngine::with_rules(
            PermissionMode::Allow,
            vec![Rule::deny("Bash").named("no-destructive-shell")],
        );
        let decision = engine.evaluate_detailed("Bash", &json!({}));
        assert_eq!(decision.source, "no-destructive-shell");
        assert!(matches!(decision.outcome, Outcome::Deny(_)));
    }

    #[test]
    fn later_deny_cannot_be_shadowed_by_an_earlier_allow_or_ask() {
        for earlier in [Rule::allow("Bash"), Rule::ask("Bash")] {
            let engine = PermissionEngine::with_rules(
                PermissionMode::Allow,
                vec![
                    earlier,
                    Rule::deny("Bash")
                        .matching_field("command", r"rm\s+-rf")
                        .unwrap()
                        .named("no-destructive-shell"),
                ],
            );

            let decision =
                engine.evaluate_detailed("Bash", &json!({ "command": "rm -rf /tmp/important" }));
            assert!(matches!(decision.outcome, Outcome::Deny(_)));
            assert_eq!(decision.source, "no-destructive-shell");
        }
    }

    #[test]
    fn first_non_deny_rule_still_wins_when_no_deny_matches() {
        let engine = PermissionEngine::with_rules(
            PermissionMode::Deny,
            vec![Rule::ask("Bash").named("review-shell"), Rule::allow("Bash")],
        );

        let decision = engine.evaluate_detailed("Bash", &json!({ "command": "pwd" }));
        assert_eq!(decision.outcome, Outcome::Ask);
        assert_eq!(decision.source, "review-shell");
    }

    #[test]
    fn control_char_escape_does_not_evade_a_deny_rule() {
        // Regression (fail-open bypass): a real TAB/newline between `rm` and `-rf` must still be
        // denied. Matching the serialized JSON (where a tab became the 2 chars `\t`) let it through;
        // matching the decoded value fixes it — the shell splits on tab and would run `rm -rf`.
        let engine = PermissionEngine::with_rules(
            PermissionMode::Allow,
            vec![Rule::deny("Bash").matching(r"rm\s+-rf").unwrap()],
        );
        assert!(matches!(
            engine.evaluate("Bash", &json!({ "command": "rm\t-rf /" })),
            Outcome::Deny(_)
        ));
        assert!(matches!(
            engine.evaluate("Bash", &json!({ "command": "rm\n-rf /" })),
            Outcome::Deny(_)
        ));
    }

    #[test]
    fn anchored_pattern_binds_to_the_raw_value() {
        let engine = PermissionEngine::with_rules(
            PermissionMode::Allow,
            vec![Rule::deny("Bash").matching(r"^git push$").unwrap()],
        );
        assert!(matches!(
            engine.evaluate("Bash", &json!({ "command": "git push" })),
            Outcome::Deny(_)
        ));
        // Anchored → does NOT match a superset command (against the JSON blob it never matched).
        assert_eq!(
            engine.evaluate("Bash", &json!({ "command": "git push origin main" })),
            Outcome::Allow
        );
    }

    #[test]
    fn field_scoped_rule_ignores_other_fields() {
        let engine = PermissionEngine::with_rules(
            PermissionMode::Allow,
            vec![Rule::deny("Bash")
                .matching_field("command", r"rm\s+-rf")
                .unwrap()],
        );
        assert!(matches!(
            engine.evaluate("Bash", &json!({ "command": "rm -rf /" })),
            Outcome::Deny(_)
        ));
        // The pattern only inspects `command`; a match in another field must NOT deny.
        assert_eq!(
            engine.evaluate("Bash", &json!({ "note": "rm -rf", "command": "ls" })),
            Outcome::Allow
        );
    }
}
