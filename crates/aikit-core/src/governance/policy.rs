//! Declarative permission policy — the ergonomic front door to the permission engine.
//!
//! A serious harness ships governance as *rules in a config*, not hand-written Rust. aikit already
//! has the enforcing engine ([`PermissionEngine`]); this compiles a declarative [`PolicySpec`]
//! (JSON, Claude-Code-style) into it:
//!
//! ```json
//! { "mode": "allow",
//!   "deny":  ["Bash(rm -rf *)", "Read(*.env)"],
//!   "ask":   ["Bash(git push *)"],
//!   "allow": ["Read(*)", "Write(./workspace/**)"] }
//! ```
//!
//! Each rule spec is `Tool` (any input) or `Tool(glob)`. The glob is anchored to the **whole**
//! decoded string value: `Bash(rm -rf *)` matches a command that *is* `rm -rf …`. For "…anywhere in
//! the value", lead with `*`: `Bash(* rm -rf *)` also matches `sudo rm -rf /`. Wildcards: `*` = any
//! run, `?` = one char; every other regex metacharacter is escaped. For matching beyond globs, the
//! code API ([`Rule::matching`](crate::governance::permissions::Rule::matching)) still takes a full
//! regex. Deny is authoritative in the engine regardless of ordering.

use crate::error::{AikitError, Result};
use crate::governance::permissions::{PermissionEngine, PermissionMode, Rule};
use serde::{Deserialize, Serialize};

/// The default decision when no rule matches.
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum PolicyMode {
    /// Allow by default; deny only what a `deny` rule matches.
    #[default]
    Allow,
    /// Deny by default; allow only what an `allow` rule matches (least-privilege).
    Deny,
    /// Ask by default; every unmatched tool escalates for approval.
    Ask,
}

impl From<PolicyMode> for PermissionMode {
    fn from(m: PolicyMode) -> Self {
        match m {
            PolicyMode::Allow => PermissionMode::Allow,
            PolicyMode::Deny => PermissionMode::Deny,
            PolicyMode::Ask => PermissionMode::Ask,
        }
    }
}

/// A declarative permission policy: a default mode plus allow / ask / deny rule specs.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct PolicySpec {
    #[serde(default)]
    pub mode: PolicyMode,
    #[serde(default)]
    pub allow: Vec<String>,
    #[serde(default)]
    pub ask: Vec<String>,
    #[serde(default)]
    pub deny: Vec<String>,
}

impl PolicySpec {
    /// Parse a policy from a JSON string.
    pub fn from_json(s: &str) -> Result<Self> {
        serde_json::from_str(s).map_err(|e| AikitError::Other(format!("invalid policy JSON: {e}")))
    }

    /// Load a policy from a JSON file.
    pub fn from_file(path: impl AsRef<std::path::Path>) -> Result<Self> {
        let text = std::fs::read_to_string(path.as_ref())
            .map_err(|e| AikitError::Other(format!("cannot read policy file: {e}")))?;
        Self::from_json(&text)
    }

    /// Compile the policy into an enforcing [`PermissionEngine`].
    pub fn build(&self) -> Result<PermissionEngine> {
        let mut rules = Vec::new();
        // Order is cosmetic — the engine makes any matching deny authoritative — but keeping deny
        // first mirrors precedence for anyone reading the compiled rule list.
        for spec in &self.deny {
            rules.push(compile(RuleKind::Deny, spec)?);
        }
        for spec in &self.ask {
            rules.push(compile(RuleKind::Ask, spec)?);
        }
        for spec in &self.allow {
            rules.push(compile(RuleKind::Allow, spec)?);
        }
        Ok(PermissionEngine::with_rules(self.mode.into(), rules))
    }
}

#[derive(Clone, Copy)]
enum RuleKind {
    Allow,
    Ask,
    Deny,
}

/// Compile one `Tool` / `Tool(glob)` spec into a [`Rule`] of the given kind.
fn compile(kind: RuleKind, spec: &str) -> Result<Rule> {
    let (tool, glob) = parse_spec(spec);
    if tool.is_empty() {
        return Err(AikitError::Other(format!(
            "policy rule has no tool name: '{spec}'"
        )));
    }
    let base = match kind {
        RuleKind::Allow => Rule::allow(tool),
        RuleKind::Ask => Rule::ask(tool),
        RuleKind::Deny => Rule::deny(tool),
    };
    match glob {
        Some(g) => {
            let pattern = format!("^{}$", glob_to_regex(&g));
            base.matching(&pattern)
                .map_err(|e| AikitError::Other(format!("bad glob '{g}': {e}")))
        }
        None => Ok(base),
    }
}

/// Split `Tool(glob)` → `(Tool, Some(glob))`; `Tool` or `Tool()` → `(Tool, None)`.
fn parse_spec(spec: &str) -> (String, Option<String>) {
    let spec = spec.trim();
    if spec.ends_with(')') {
        if let Some(open) = spec.find('(') {
            let tool = spec[..open].trim().to_string();
            let inner = spec[open + 1..spec.len() - 1].trim();
            let glob = if inner.is_empty() {
                None
            } else {
                Some(inner.to_string())
            };
            return (tool, glob);
        }
    }
    (spec.to_string(), None)
}

/// Convert a glob body to a regex body (caller anchors with `^…$`). `*`→`.*`, `?`→`.`, every regex
/// metacharacter is escaped so a literal `.` or `(` in the glob can never change the pattern.
fn glob_to_regex(glob: &str) -> String {
    let mut re = String::with_capacity(glob.len() * 2);
    for ch in glob.chars() {
        match ch {
            '*' => re.push_str(".*"),
            '?' => re.push('.'),
            '.' | '+' | '(' | ')' | '|' | '[' | ']' | '{' | '}' | '^' | '$' | '\\' => {
                re.push('\\');
                re.push(ch);
            }
            _ => re.push(ch),
        }
    }
    re
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::governance::permissions::Outcome;
    use serde_json::json;

    #[test]
    fn parse_spec_splits_tool_and_glob() {
        assert_eq!(parse_spec("Read"), ("Read".into(), None));
        assert_eq!(
            parse_spec("Bash(rm -rf *)"),
            ("Bash".into(), Some("rm -rf *".into()))
        );
        assert_eq!(parse_spec("Write()"), ("Write".into(), None));
        assert_eq!(parse_spec("  Read  "), ("Read".into(), None));
        assert_eq!(parse_spec("*"), ("*".into(), None));
    }

    #[test]
    fn glob_escapes_metacharacters() {
        // `-` is not a regex metacharacter outside a character class, so it is left literal.
        assert_eq!(glob_to_regex("rm -rf *"), r"rm -rf .*");
        assert_eq!(glob_to_regex("*.env"), r".*\.env");
        assert_eq!(glob_to_regex("a(b)"), r"a\(b\)");
        assert_eq!(glob_to_regex("x?y"), "x.y");
    }

    fn engine(json_str: &str) -> PermissionEngine {
        PolicySpec::from_json(json_str).unwrap().build().unwrap()
    }

    #[test]
    fn deny_glob_blocks_the_exact_command_shape() {
        let e = engine(r#"{ "mode": "allow", "deny": ["Bash(rm -rf *)"] }"#);
        // The `Deny` variant carries a human-readable reason, not the rule id.
        assert!(matches!(
            e.evaluate("Bash", &json!({ "command": "rm -rf /" })),
            Outcome::Deny(_)
        ));
        // A different command is allowed (allow-by-default mode).
        assert_eq!(
            e.evaluate("Bash", &json!({ "command": "ls -la" })),
            Outcome::Allow
        );
        // Anchored: `sudo rm -rf` is NOT caught by `rm -rf *` (documented — lead with `*` to catch).
        assert_eq!(
            e.evaluate("Bash", &json!({ "command": "sudo rm -rf /" })),
            Outcome::Allow
        );
    }

    #[test]
    fn leading_star_matches_anywhere() {
        let e = engine(r#"{ "deny": ["Bash(* rm -rf *)"] }"#);
        assert!(matches!(
            e.evaluate("Bash", &json!({ "command": "sudo rm -rf /" })),
            Outcome::Deny(_)
        ));
        assert!(matches!(
            e.evaluate("Bash", &json!({ "command": "x; rm -rf /home" })),
            Outcome::Deny(_)
        ));
    }

    #[test]
    fn deny_wins_over_allow_regardless_of_lists() {
        let e = engine(r#"{ "allow": ["Bash(*)"], "deny": ["Bash(rm -rf *)"] }"#);
        assert!(matches!(
            e.evaluate("Bash", &json!({ "command": "rm -rf /" })),
            Outcome::Deny(_)
        ));
        assert_eq!(
            e.evaluate("Bash", &json!({ "command": "echo hi" })),
            Outcome::Allow
        );
    }

    #[test]
    fn deny_mode_is_least_privilege() {
        let e = engine(r#"{ "mode": "deny", "allow": ["Read(*)"] }"#);
        assert_eq!(
            e.evaluate("Read", &json!({ "path": "a.txt" })),
            Outcome::Allow
        );
        // Unlisted tool → denied by default.
        assert!(matches!(
            e.evaluate("Bash", &json!({ "command": "ls" })),
            Outcome::Deny(_)
        ));
    }

    #[test]
    fn ask_rule_escalates() {
        let e = engine(r#"{ "ask": ["Bash(git push *)"] }"#);
        assert_eq!(
            e.evaluate("Bash", &json!({ "command": "git push origin main" })),
            Outcome::Ask
        );
        assert_eq!(
            e.evaluate("Bash", &json!({ "command": "git status" })),
            Outcome::Allow
        );
    }

    #[test]
    fn path_glob_matches_the_path_value() {
        let e = engine(r#"{ "deny": ["Write(./secrets/**)"] }"#);
        assert!(matches!(
            e.evaluate(
                "Write",
                &json!({ "path": "./secrets/prod.key", "content": "x" })
            ),
            Outcome::Deny(_)
        ));
        assert_eq!(
            e.evaluate(
                "Write",
                &json!({ "path": "./workspace/notes.txt", "content": "x" })
            ),
            Outcome::Allow
        );
    }

    #[test]
    fn tool_without_glob_matches_any_input() {
        let e = engine(r#"{ "mode": "deny", "allow": ["Read"] }"#);
        assert_eq!(
            e.evaluate("Read", &json!({ "path": "anything" })),
            Outcome::Allow
        );
    }

    #[test]
    fn empty_policy_is_permissive() {
        let e = engine(r#"{}"#);
        assert_eq!(
            e.evaluate("Bash", &json!({ "command": "anything" })),
            Outcome::Allow
        );
    }

    #[test]
    fn invalid_json_is_an_error() {
        assert!(PolicySpec::from_json("{ not json").is_err());
    }

    #[test]
    fn empty_tool_name_is_rejected() {
        assert!(PolicySpec::from_json(r#"{ "deny": ["(rm *)"] }"#)
            .unwrap()
            .build()
            .is_err());
    }

    #[test]
    fn loads_from_a_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("policy.json");
        std::fs::write(&path, r#"{ "deny": ["Bash(rm -rf *)"] }"#).unwrap();
        let e = PolicySpec::from_file(&path).unwrap().build().unwrap();
        assert!(matches!(
            e.evaluate("Bash", &json!({ "command": "rm -rf /" })),
            Outcome::Deny(_)
        ));
    }

    #[test]
    fn round_trips_through_serde() {
        let spec = PolicySpec::from_json(
            r#"{ "mode": "ask", "deny": ["Bash(rm -rf *)"], "allow": ["Read(*)"] }"#,
        )
        .unwrap();
        assert_eq!(spec.mode, PolicyMode::Ask);
        assert_eq!(spec.deny, vec!["Bash(rm -rf *)"]);
        let reserialized = serde_json::to_string(&spec).unwrap();
        let again = PolicySpec::from_json(&reserialized).unwrap();
        assert_eq!(again.mode, PolicyMode::Ask);
    }
}
