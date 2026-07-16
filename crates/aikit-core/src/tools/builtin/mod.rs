//! Batteries-included tools — the built-in suite (Read/Write/Edit/Grep/Glob/Bash) that no other
//! multi-provider SDK ships. They are a [`ToolExecutor`], so they plug straight into `run_agent`;
//! combined with the permission engine + hooks, this is the governed, tool-equipped agent.

pub mod bash;
pub mod fs;

use crate::error::{AikitError, Result};
use crate::governance::containment::{
    containment_capabilities as probe_containment, ContainmentCapabilityReport, ContainmentPolicy,
};
use crate::governance::process::BashPolicy;
use crate::governance::sandbox::Sandbox;
use crate::tools::ToolExecutor;
use crate::types::ToolSpec;
use async_trait::async_trait;
use serde_json::{json, Value};

pub(crate) const ALL_TOOL_NAMES: [&str; 6] = ["Read", "Write", "Edit", "Grep", "Glob", "Bash"];

/// The built-in tool suite, guarded by a filesystem [`Sandbox`]. `Bash` is opt-in and, when
/// enabled, requires OS containment by default in addition to [`BashPolicy`] process hardening.
/// If no backend is available, execution fails before the host shell starts.
pub struct BuiltinTools {
    sandbox: Sandbox,
    allow_bash: bool,
    bash_policy: BashPolicy,
    bash_containment: ContainmentPolicy,
}

impl BuiltinTools {
    /// A suite jailed to `sandbox`, with Bash disabled.
    pub fn new(sandbox: Sandbox) -> Self {
        BuiltinTools {
            sandbox,
            allow_bash: false,
            bash_policy: BashPolicy::default(),
            bash_containment: ContainmentPolicy::default(),
        }
    }

    /// Enable Bash under the default process hardening and fail-closed `Required(Auto)` OS
    /// containment. On macOS Auto selects Seatbelt; elsewhere it needs a configured Docker
    /// fallback. Use [`Self::containment_capabilities`] during service startup to preflight it.
    pub fn with_bash(mut self) -> Self {
        self.allow_bash = true;
        self
    }

    /// Enable Bash with a custom [`BashPolicy`] (e.g. a tighter timeout, or `permissive()`).
    pub fn with_bash_policy(mut self, policy: BashPolicy) -> Self {
        self.allow_bash = true;
        self.bash_policy = policy;
        self
    }

    /// Enable Bash with an explicit containment policy. Required policies fail closed.
    pub fn with_containment_policy(mut self, policy: ContainmentPolicy) -> Self {
        self.allow_bash = true;
        self.bash_containment = policy;
        self
    }

    /// Explicitly opt out of OS containment while retaining [`BashPolicy`] hardening. This exists
    /// for compatibility and trusted local development; never treat it as a security boundary.
    pub fn with_uncontained_bash(self) -> Self {
        self.with_containment_policy(ContainmentPolicy::uncontained())
    }

    /// Actively probe the configured containment posture and report the selected backend and its
    /// mechanism-level guarantees.
    pub async fn containment_capabilities(&self) -> ContainmentCapabilityReport {
        probe_containment(&self.bash_containment, self.sandbox.primary_root()).await
    }

    /// The tool names this suite answers to (for advertising to a model / registry).
    pub fn tool_names(&self) -> Vec<&'static str> {
        let mut v = vec!["Read", "Write", "Edit", "Grep", "Glob"];
        if self.allow_bash {
            v.push("Bash");
        }
        v
    }

    /// Canonical model-facing schemas for every tool this suite will execute. Bash is advertised
    /// only when it is enabled; all schemas reject unknown fields so the model contract cannot
    /// silently drift away from the executor contract.
    pub fn specs(&self) -> Vec<ToolSpec> {
        self.tool_names().into_iter().map(canonical_spec).collect()
    }
}

fn canonical_spec(name: &str) -> ToolSpec {
    let (description, properties, required) = match name {
        "Read" => (
            "Read a UTF-8 text file inside the configured workspace jail.",
            json!({
                "path": {
                    "type": "string",
                    "description": "Workspace-relative path, or an absolute path inside the jail."
                }
            }),
            vec!["path"],
        ),
        "Write" => (
            "Write UTF-8 text to a file inside the configured workspace jail.",
            json!({
                "path": {
                    "type": "string",
                    "description": "Workspace-relative path, or an absolute path inside the jail."
                },
                "content": {
                    "type": "string",
                    "description": "Complete text content to write."
                }
            }),
            vec!["path", "content"],
        ),
        "Edit" => (
            "Replace one unique text occurrence in a file inside the configured workspace jail.",
            json!({
                "path": {
                    "type": "string",
                    "description": "Workspace-relative path, or an absolute path inside the jail."
                },
                "old_string": {
                    "type": "string",
                    "description": "Text that must occur exactly once."
                },
                "new_string": {
                    "type": "string",
                    "description": "Replacement text."
                }
            }),
            vec!["path", "old_string", "new_string"],
        ),
        "Grep" => (
            "Search UTF-8 files recursively with a regular expression inside the workspace jail.",
            json!({
                "pattern": {
                    "type": "string",
                    "description": "Rust regular expression matched against each line."
                },
                "path": {
                    "type": "string",
                    "description": "Optional file or directory inside the jail; defaults to its primary root."
                }
            }),
            vec!["pattern"],
        ),
        "Glob" => (
            "Find files recursively by basename using * and ? wildcards inside the workspace jail.",
            json!({
                "pattern": {
                    "type": "string",
                    "description": "Basename pattern; * matches any run and ? matches one character."
                }
            }),
            vec!["pattern"],
        ),
        "Bash" => (
            "Run a shell command from the workspace root under the configured process and OS containment policies.",
            json!({
                "command": {
                    "type": "string",
                    "description": "Shell command to execute."
                }
            }),
            vec!["command"],
        ),
        _ => unreachable!("canonical_spec called for an unknown built-in tool"),
    };

    ToolSpec {
        name: name.to_string(),
        description: description.to_string(),
        input_schema: json!({
            "type": "object",
            "properties": properties,
            "required": required,
            "additionalProperties": false
        }),
    }
}

#[async_trait]
impl ToolExecutor for BuiltinTools {
    async fn execute(&self, name: &str, input: Value) -> Result<String> {
        match name {
            "Read" => fs::read(&self.sandbox, &input),
            "Write" => fs::write(&self.sandbox, &input),
            "Edit" => fs::edit(&self.sandbox, &input),
            "Grep" => fs::grep(&self.sandbox, &input),
            "Glob" => fs::glob(&self.sandbox, &input),
            "Bash" if self.allow_bash => {
                bash::run(
                    &self.sandbox,
                    &self.bash_policy,
                    &self.bash_containment,
                    &input,
                )
                .await
            }
            "Bash" => Err(AikitError::PermissionDenied(
                "Bash is disabled on this tool suite (enable with .with_bash())".into(),
            )),
            other => Err(AikitError::ToolExecution(format!(
                "unknown built-in tool '{other}'"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::BTreeSet;

    fn suite() -> (tempfile::TempDir, BuiltinTools) {
        let dir = tempfile::tempdir().unwrap();
        let sb = Sandbox::jail(dir.path()).unwrap();
        // Unit tests exercise tool routing without depending on a host containment backend. The
        // production-facing `.with_bash()` default is tested separately below.
        (dir, BuiltinTools::new(sb).with_uncontained_bash())
    }

    #[test]
    fn canonical_specs_match_the_dispatch_contract() {
        let dir = tempfile::tempdir().unwrap();
        let without_bash = BuiltinTools::new(Sandbox::jail(dir.path()).unwrap());
        assert!(!without_bash.specs().iter().any(|spec| spec.name == "Bash"));

        let tools = without_bash.with_bash();
        let specs = tools.specs();
        assert_eq!(
            specs
                .iter()
                .map(|spec| spec.name.as_str())
                .collect::<Vec<_>>(),
            tools.tool_names()
        );

        let cases: &[(&str, &[&str], &[&str])] = &[
            ("Read", &["path"], &["path"]),
            ("Write", &["path", "content"], &["path", "content"]),
            (
                "Edit",
                &["path", "old_string", "new_string"],
                &["path", "old_string", "new_string"],
            ),
            ("Grep", &["pattern", "path"], &["pattern"]),
            ("Glob", &["pattern"], &["pattern"]),
            ("Bash", &["command"], &["command"]),
        ];

        for (name, expected_properties, expected_required) in cases {
            let spec = specs.iter().find(|spec| spec.name == *name).unwrap();
            assert!(!spec.description.is_empty());
            assert_eq!(spec.input_schema["type"], "object");
            assert_eq!(spec.input_schema["additionalProperties"], false);

            let actual_properties = spec.input_schema["properties"]
                .as_object()
                .unwrap()
                .iter()
                .map(|(key, schema)| {
                    assert_eq!(schema["type"], "string");
                    key.as_str()
                })
                .collect::<BTreeSet<_>>();
            assert_eq!(
                actual_properties,
                expected_properties.iter().copied().collect()
            );

            let actual_required = spec.input_schema["required"]
                .as_array()
                .unwrap()
                .iter()
                .map(|value| value.as_str().unwrap())
                .collect::<BTreeSet<_>>();
            assert_eq!(actual_required, expected_required.iter().copied().collect());
        }
    }

    #[tokio::test]
    async fn every_advertised_name_routes_to_an_executable_builtin() {
        let (_dir, tools) = suite();
        let calls = [
            (
                "Write",
                json!({ "path": "contract.txt", "content": "before" }),
            ),
            ("Read", json!({ "path": "contract.txt" })),
            (
                "Edit",
                json!({
                    "path": "contract.txt",
                    "old_string": "before",
                    "new_string": "after"
                }),
            ),
            ("Grep", json!({ "pattern": "after" })),
            ("Glob", json!({ "pattern": "*.txt" })),
            ("Bash", json!({ "command": "echo executable" })),
        ];

        for (name, input) in &calls {
            assert!(
                tools.execute(name, input.clone()).await.is_ok(),
                "advertised built-in {name} did not execute"
            );
        }
        assert_eq!(
            tools
                .specs()
                .iter()
                .map(|spec| spec.name.as_str())
                .collect::<BTreeSet<_>>(),
            calls.iter().map(|(name, _)| *name).collect()
        );
    }

    #[tokio::test]
    async fn write_then_read_roundtrip() {
        let (_dir, tools) = suite();
        let w = tools
            .execute(
                "Write",
                json!({ "path": "notes.txt", "content": "merhaba" }),
            )
            .await
            .unwrap();
        assert!(w.contains("wrote"));
        let r = tools
            .execute("Read", json!({ "path": "notes.txt" }))
            .await
            .unwrap();
        assert_eq!(r, "merhaba");
    }

    #[tokio::test]
    async fn edit_requires_a_unique_match() {
        let (_dir, tools) = suite();
        tools
            .execute("Write", json!({ "path": "f.txt", "content": "a b a" }))
            .await
            .unwrap();
        // Non-unique old_string is rejected.
        assert!(tools
            .execute(
                "Edit",
                json!({ "path": "f.txt", "old_string": "a", "new_string": "X" })
            )
            .await
            .is_err());
        // Unique edit works.
        tools
            .execute(
                "Edit",
                json!({ "path": "f.txt", "old_string": "b", "new_string": "B" }),
            )
            .await
            .unwrap();
        let r = tools
            .execute("Read", json!({ "path": "f.txt" }))
            .await
            .unwrap();
        assert_eq!(r, "a B a");
    }

    #[tokio::test]
    async fn read_outside_the_jail_is_denied() {
        let (_dir, tools) = suite();
        let err = tools
            .execute("Read", json!({ "path": "/etc/hostname" }))
            .await
            .unwrap_err();
        assert!(matches!(err, AikitError::Sandbox(_)));
        assert_eq!(err.info().code, crate::error::ErrorCode::Sandbox);
    }

    #[tokio::test]
    async fn grep_and_glob_find_within_the_root() {
        let (_dir, tools) = suite();
        tools
            .execute(
                "Write",
                json!({ "path": "a.log", "content": "hello\nERROR boom\nok" }),
            )
            .await
            .unwrap();
        let g = tools
            .execute("Grep", json!({ "pattern": "ERROR" }))
            .await
            .unwrap();
        assert!(g.contains("ERROR boom"));
        let gl = tools
            .execute("Glob", json!({ "pattern": "*.log" }))
            .await
            .unwrap();
        assert!(gl.contains("a.log"));
    }

    #[tokio::test]
    async fn bash_runs_when_enabled() {
        let (_dir, tools) = suite();
        let out = tools
            .execute("Bash", json!({ "command": "echo governed" }))
            .await
            .unwrap();
        assert!(out.contains("governed"));
        assert!(out.contains("[exit 0]"));
    }

    #[tokio::test]
    async fn bash_is_denied_when_disabled() {
        let dir = tempfile::tempdir().unwrap();
        let tools = BuiltinTools::new(Sandbox::jail(dir.path()).unwrap()); // no .with_bash()
        assert!(matches!(
            tools.execute("Bash", json!({ "command": "echo x" })).await,
            Err(AikitError::PermissionDenied(_))
        ));
    }

    #[tokio::test]
    async fn bash_defaults_to_fail_closed_required_auto_containment() {
        use crate::governance::containment::{BackendSelector, ContainmentRequirement};

        let dir = tempfile::tempdir().unwrap();
        let tools = BuiltinTools::new(Sandbox::jail(dir.path()).unwrap()).with_bash();
        assert_eq!(
            tools.bash_containment.requirement,
            ContainmentRequirement::Required(BackendSelector::Auto)
        );
        assert!(tools.containment_capabilities().await.fail_closed);
    }

    #[tokio::test]
    async fn uncontained_opt_out_is_reported_honestly() {
        use crate::governance::containment::ActiveContainmentBackend;

        let dir = tempfile::tempdir().unwrap();
        let tools = BuiltinTools::new(Sandbox::jail(dir.path()).unwrap()).with_uncontained_bash();
        let report = tools.containment_capabilities().await;
        assert_eq!(
            report.selected_backend,
            Some(ActiveContainmentBackend::Uncontained)
        );
        assert!(!report.fail_closed);
    }
}
