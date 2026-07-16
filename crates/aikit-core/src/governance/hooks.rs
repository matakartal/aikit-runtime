//! Enforcing, async-capable lifecycle hooks.
//!
//! Hooks run inside the agent loop, not beside it. `PreToolUse` can block/rewrite input;
//! `PostToolUse` can rewrite or reject output; failure hooks can normalize the error returned to
//! the model. Async registration is the host-language FFI seam, while sync registration remains a
//! convenient zero-cost wrapper for native Rust callers.

use futures::future::{ready, BoxFuture, FutureExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

/// What a PreToolUse hook decides.
#[derive(Debug, Clone, PartialEq)]
pub enum HookOutcome {
    Continue,
    Block(String),
    Rewrite(Value),
}

/// What a successful PostToolUse hook does to the tool result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PostToolOutcome {
    Continue,
    RewriteOutput(String),
    MarkError(String),
}

/// What a failure hook does to the error surfaced to the model/audit trail.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FailureHookOutcome {
    Continue,
    RewriteError(String),
}

/// What a UserPromptSubmit hook decides.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PromptHookOutcome {
    Continue,
    Block(String),
    Rewrite(String),
}

/// Failure stage is typed so audit consumers do not need to parse strings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureStage {
    Configuration,
    ProviderStart,
    ProviderStream,
    ToolNotAdvertised,
    PreToolUse,
    Permission,
    ToolExecution,
    ToolInputValidation,
    PostToolUse,
    MaxTurns,
    MalformedToolCall,
    Budget,
    Audit,
}

impl FailureStage {
    /// Stable snake_case identifier used by audit records and host-language callbacks.
    pub const fn as_str(self) -> &'static str {
        match self {
            FailureStage::Configuration => "configuration",
            FailureStage::ProviderStart => "provider_start",
            FailureStage::ProviderStream => "provider_stream",
            FailureStage::ToolNotAdvertised => "tool_not_advertised",
            FailureStage::PreToolUse => "pre_tool_use",
            FailureStage::Permission => "permission",
            FailureStage::ToolExecution => "tool_execution",
            FailureStage::ToolInputValidation => "tool_input_validation",
            FailureStage::PostToolUse => "post_tool_use",
            FailureStage::MaxTurns => "max_turns",
            FailureStage::MalformedToolCall => "malformed_tool_call",
            FailureStage::Budget => "budget",
            FailureStage::Audit => "audit",
        }
    }
}

#[derive(Debug, Clone)]
pub struct PromptContext {
    pub run_id: String,
    pub prompt: String,
}

#[derive(Debug, Clone)]
pub struct PreToolUseContext {
    pub run_id: String,
    pub turn: usize,
    pub tool_use_id: String,
    pub tool: String,
    pub input: Value,
}

#[derive(Debug, Clone)]
pub struct PostToolUseContext {
    pub run_id: String,
    pub turn: usize,
    pub tool_use_id: String,
    pub tool: String,
    pub input: Value,
    pub output: String,
    pub duration_ms: u128,
}

#[derive(Debug, Clone)]
pub struct FailureContext {
    pub run_id: String,
    pub turn: usize,
    pub stage: FailureStage,
    pub tool_use_id: Option<String>,
    pub tool: Option<String>,
    pub error: String,
}

#[derive(Debug, Clone)]
pub struct StopContext {
    pub run_id: String,
    pub turns: usize,
    pub reason: String,
    pub usage: crate::types::Usage,
}

/// Matches hooks to tools by name (exact, or `*` for any).
#[derive(Debug, Clone)]
pub struct HookMatcher {
    tool: String,
}

impl HookMatcher {
    pub fn tool(tool: impl Into<String>) -> Self {
        HookMatcher { tool: tool.into() }
    }

    pub fn any() -> Self {
        HookMatcher { tool: "*".into() }
    }

    fn matches(&self, tool: &str) -> bool {
        self.tool == "*" || self.tool == tool
    }
}

type PromptHook = Arc<dyn Fn(PromptContext) -> BoxFuture<'static, PromptHookOutcome> + Send + Sync>;
type PreHook = Arc<dyn Fn(PreToolUseContext) -> BoxFuture<'static, HookOutcome> + Send + Sync>;
type PostHook =
    Arc<dyn Fn(PostToolUseContext) -> BoxFuture<'static, PostToolOutcome> + Send + Sync>;
type FailureHook =
    Arc<dyn Fn(FailureContext) -> BoxFuture<'static, FailureHookOutcome> + Send + Sync>;
type StopHook = Arc<dyn Fn(StopContext) -> BoxFuture<'static, ()> + Send + Sync>;

#[derive(Clone)]
pub struct HookDispatcher {
    user_prompt_submit: Vec<PromptHook>,
    pre_tool_use: Vec<(HookMatcher, PreHook)>,
    post_tool_use: Vec<(HookMatcher, PostHook)>,
    post_tool_failure: Vec<(HookMatcher, FailureHook)>,
    failure: Vec<FailureHook>,
    stop: Vec<StopHook>,
    timeout: Duration,
}

impl Default for HookDispatcher {
    fn default() -> Self {
        HookDispatcher {
            user_prompt_submit: Vec::new(),
            pre_tool_use: Vec::new(),
            post_tool_use: Vec::new(),
            post_tool_failure: Vec::new(),
            failure: Vec::new(),
            stop: Vec::new(),
            timeout: Duration::from_secs(5),
        }
    }
}

impl HookDispatcher {
    pub fn new() -> Self {
        HookDispatcher::default()
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn on_user_prompt_submit<F>(&mut self, hook: F)
    where
        F: Fn(&str) -> PromptHookOutcome + Send + Sync + 'static,
    {
        self.user_prompt_submit
            .push(Arc::new(move |ctx| ready(hook(&ctx.prompt)).boxed()));
    }

    pub fn on_user_prompt_submit_async<F, Fut>(&mut self, hook: F)
    where
        F: Fn(PromptContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = PromptHookOutcome> + Send + 'static,
    {
        self.user_prompt_submit
            .push(Arc::new(move |ctx| hook(ctx).boxed()));
    }

    pub fn on_pre_tool_use<F>(&mut self, matcher: HookMatcher, hook: F)
    where
        F: Fn(&str, &Value) -> HookOutcome + Send + Sync + 'static,
    {
        self.pre_tool_use.push((
            matcher,
            Arc::new(move |ctx| ready(hook(&ctx.tool, &ctx.input)).boxed()),
        ));
    }

    pub fn on_pre_tool_use_async<F, Fut>(&mut self, matcher: HookMatcher, hook: F)
    where
        F: Fn(PreToolUseContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = HookOutcome> + Send + 'static,
    {
        self.pre_tool_use
            .push((matcher, Arc::new(move |ctx| hook(ctx).boxed())));
    }

    pub fn on_post_tool_use<F>(&mut self, matcher: HookMatcher, hook: F)
    where
        F: Fn(&str, &Value, &str) -> PostToolOutcome + Send + Sync + 'static,
    {
        self.post_tool_use.push((
            matcher,
            Arc::new(move |ctx| ready(hook(&ctx.tool, &ctx.input, &ctx.output)).boxed()),
        ));
    }

    pub fn on_post_tool_use_async<F, Fut>(&mut self, matcher: HookMatcher, hook: F)
    where
        F: Fn(PostToolUseContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = PostToolOutcome> + Send + 'static,
    {
        self.post_tool_use
            .push((matcher, Arc::new(move |ctx| hook(ctx).boxed())));
    }

    /// Tool-scoped failure hook (permission, execution, malformed call, or post-hook failure).
    pub fn on_post_tool_failure<F>(&mut self, matcher: HookMatcher, hook: F)
    where
        F: Fn(&FailureContext) -> FailureHookOutcome + Send + Sync + 'static,
    {
        self.post_tool_failure
            .push((matcher, Arc::new(move |ctx| ready(hook(&ctx)).boxed())));
    }

    /// Async tool-scoped failure hook used by Python/Node host callbacks.
    pub fn on_post_tool_failure_async<F, Fut>(&mut self, matcher: HookMatcher, hook: F)
    where
        F: Fn(FailureContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = FailureHookOutcome> + Send + 'static,
    {
        self.post_tool_failure
            .push((matcher, Arc::new(move |ctx| hook(ctx).boxed())));
    }

    pub fn on_failure<F>(&mut self, hook: F)
    where
        F: Fn(&FailureContext) -> FailureHookOutcome + Send + Sync + 'static,
    {
        self.failure
            .push(Arc::new(move |ctx| ready(hook(&ctx)).boxed()));
    }

    pub fn on_failure_async<F, Fut>(&mut self, hook: F)
    where
        F: Fn(FailureContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = FailureHookOutcome> + Send + 'static,
    {
        self.failure.push(Arc::new(move |ctx| hook(ctx).boxed()));
    }

    pub fn on_stop<F>(&mut self, hook: F)
    where
        F: Fn(&StopContext) + Send + Sync + 'static,
    {
        self.stop.push(Arc::new(move |ctx| {
            hook(&ctx);
            ready(()).boxed()
        }));
    }

    pub fn on_stop_async<F, Fut>(&mut self, hook: F)
    where
        F: Fn(StopContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.stop.push(Arc::new(move |ctx| hook(ctx).boxed()));
    }

    pub async fn run_user_prompt_submit(&self, mut ctx: PromptContext) -> PromptHookOutcome {
        let mut rewritten = false;
        for hook in &self.user_prompt_submit {
            let outcome = match tokio::time::timeout(self.timeout, hook(ctx.clone())).await {
                Ok(outcome) => outcome,
                Err(_) => {
                    return PromptHookOutcome::Block("UserPromptSubmit hook timed out".into())
                }
            };
            match outcome {
                PromptHookOutcome::Block(reason) => return PromptHookOutcome::Block(reason),
                PromptHookOutcome::Rewrite(prompt) => {
                    ctx.prompt = prompt;
                    rewritten = true;
                }
                PromptHookOutcome::Continue => {}
            }
        }
        if rewritten {
            PromptHookOutcome::Rewrite(ctx.prompt)
        } else {
            PromptHookOutcome::Continue
        }
    }

    pub async fn run_pre_tool_use(&self, mut ctx: PreToolUseContext) -> HookOutcome {
        let mut rewritten = false;
        for (matcher, hook) in &self.pre_tool_use {
            if !matcher.matches(&ctx.tool) {
                continue;
            }
            let outcome = match tokio::time::timeout(self.timeout, hook(ctx.clone())).await {
                Ok(outcome) => outcome,
                Err(_) => return HookOutcome::Block("PreToolUse hook timed out".into()),
            };
            match outcome {
                HookOutcome::Block(reason) => return HookOutcome::Block(reason),
                HookOutcome::Rewrite(input) => {
                    ctx.input = input;
                    rewritten = true;
                }
                HookOutcome::Continue => {}
            }
        }
        if rewritten {
            HookOutcome::Rewrite(ctx.input)
        } else {
            HookOutcome::Continue
        }
    }

    pub async fn run_post_tool_use(&self, mut ctx: PostToolUseContext) -> PostToolOutcome {
        let mut rewritten = false;
        for (matcher, hook) in &self.post_tool_use {
            if !matcher.matches(&ctx.tool) {
                continue;
            }
            let outcome = match tokio::time::timeout(self.timeout, hook(ctx.clone())).await {
                Ok(outcome) => outcome,
                Err(_) => {
                    return PostToolOutcome::MarkError(
                        "PostToolUse hook timed out after tool execution".into(),
                    )
                }
            };
            match outcome {
                PostToolOutcome::MarkError(reason) => return PostToolOutcome::MarkError(reason),
                PostToolOutcome::RewriteOutput(output) => {
                    ctx.output = output;
                    rewritten = true;
                }
                PostToolOutcome::Continue => {}
            }
        }
        if rewritten {
            PostToolOutcome::RewriteOutput(ctx.output)
        } else {
            PostToolOutcome::Continue
        }
    }

    pub async fn run_failure(&self, mut ctx: FailureContext) -> String {
        if let (Some(tool), Some(_)) = (&ctx.tool, &ctx.tool_use_id) {
            for (matcher, hook) in &self.post_tool_failure {
                if !matcher.matches(tool) {
                    continue;
                }
                let Ok(outcome) = tokio::time::timeout(self.timeout, hook(ctx.clone())).await
                else {
                    continue;
                };
                if let FailureHookOutcome::RewriteError(error) = outcome {
                    ctx.error = error;
                }
            }
        }
        for hook in &self.failure {
            let Ok(outcome) = tokio::time::timeout(self.timeout, hook(ctx.clone())).await else {
                continue;
            };
            if let FailureHookOutcome::RewriteError(error) = outcome {
                ctx.error = error;
            }
        }
        ctx.error
    }

    pub async fn run_stop(&self, ctx: StopContext) -> crate::error::Result<()> {
        for hook in &self.stop {
            tokio::time::timeout(self.timeout, hook(ctx.clone()))
                .await
                .map_err(|_| crate::error::AikitError::Hook("Stop hook timed out".into()))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::{Arc, Mutex};

    fn pre_ctx(input: Value) -> PreToolUseContext {
        PreToolUseContext {
            run_id: "r".into(),
            turn: 1,
            tool_use_id: "c".into(),
            tool: "Fetch".into(),
            input,
        }
    }

    #[tokio::test]
    async fn pre_hooks_rewrite_in_registration_order_and_block_short_circuits() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let mut d = HookDispatcher::new();
        d.on_pre_tool_use(HookMatcher::any(), |_t, input| {
            let mut value = input.clone();
            value["cwd"] = json!("/workspace");
            HookOutcome::Rewrite(value)
        });
        let seen_second = seen.clone();
        d.on_pre_tool_use(HookMatcher::tool("Fetch"), move |_t, input| {
            seen_second.lock().unwrap().push(input["cwd"].clone());
            HookOutcome::Block("offline".into())
        });
        assert_eq!(
            d.run_pre_tool_use(pre_ctx(json!({ "url": "x" }))).await,
            HookOutcome::Block("offline".into())
        );
        assert_eq!(*seen.lock().unwrap(), vec![json!("/workspace")]);
    }

    #[tokio::test]
    async fn post_hooks_chain_output_rewrites() {
        let mut d = HookDispatcher::new();
        d.on_post_tool_use(HookMatcher::any(), |_t, _i, out| {
            PostToolOutcome::RewriteOutput(format!("{out}-one"))
        });
        d.on_post_tool_use(HookMatcher::any(), |_t, _i, out| {
            PostToolOutcome::RewriteOutput(format!("{out}-two"))
        });
        let out = d
            .run_post_tool_use(PostToolUseContext {
                run_id: "r".into(),
                turn: 1,
                tool_use_id: "c".into(),
                tool: "T".into(),
                input: json!({}),
                output: "raw".into(),
                duration_ms: 1,
            })
            .await;
        assert_eq!(out, PostToolOutcome::RewriteOutput("raw-one-two".into()));
    }

    #[tokio::test]
    async fn failure_hooks_rewrite_and_stop_runs_in_order() {
        let stopped = Arc::new(Mutex::new(Vec::new()));
        let mut d = HookDispatcher::new();
        d.on_failure(|ctx| FailureHookOutcome::RewriteError(format!("safe: {}", ctx.error)));
        let first = stopped.clone();
        d.on_stop(move |_| first.lock().unwrap().push(1));
        let second = stopped.clone();
        d.on_stop(move |_| second.lock().unwrap().push(2));
        let error = d
            .run_failure(FailureContext {
                run_id: "r".into(),
                turn: 1,
                stage: FailureStage::ProviderStart,
                tool_use_id: None,
                tool: None,
                error: "boom".into(),
            })
            .await;
        assert_eq!(error, "safe: boom");
        d.run_stop(StopContext {
            run_id: "r".into(),
            turns: 1,
            reason: "failed".into(),
            usage: Default::default(),
        })
        .await
        .unwrap();
        assert_eq!(*stopped.lock().unwrap(), vec![1, 2]);
    }

    #[tokio::test]
    async fn async_post_tool_failure_is_scoped_before_general_failure_hooks() {
        let mut d = HookDispatcher::new();
        d.on_post_tool_failure_async(HookMatcher::tool("Fetch"), |ctx| async move {
            FailureHookOutcome::RewriteError(format!("tool: {}", ctx.error))
        });
        d.on_failure(|ctx| FailureHookOutcome::RewriteError(format!("general: {}", ctx.error)));

        let error = d
            .run_failure(FailureContext {
                run_id: "r".into(),
                turn: 1,
                stage: FailureStage::ToolExecution,
                tool_use_id: Some("c".into()),
                tool: Some("Fetch".into()),
                error: "boom".into(),
            })
            .await;
        assert_eq!(error, "general: tool: boom");
    }

    #[tokio::test]
    async fn async_pre_hook_timeout_fails_closed() {
        let mut d = HookDispatcher::new().with_timeout(Duration::from_millis(10));
        d.on_pre_tool_use_async(HookMatcher::any(), |_ctx| async {
            futures::future::pending::<()>().await;
            HookOutcome::Continue
        });
        assert_eq!(
            d.run_pre_tool_use(pre_ctx(json!({}))).await,
            HookOutcome::Block("PreToolUse hook timed out".into())
        );
    }
}
