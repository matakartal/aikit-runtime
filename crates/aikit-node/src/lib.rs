//! Node (napi-rs) binding over the shared `aikit-runtime-core` runtime.
//!
//! Mirrors the Python (`aikit-py`) surface exactly, so behaviour is identical across languages
//! (the whole point of the "one brain, many bindings" architecture). Both hard FFI seams are
//! preserved here, exactly as in Python:
//!
//!   - [`Agent`] — the agent-native "key gir → güçlen" primitive (`addKey` / `capabilities`).
//!   - [`query`] — a streaming async iterator over canonical `StreamDelta`s, with the SAME
//!     governance (`permissions`) enforced inside the Rust loop's tool-execution seam.
//!
//! **Streaming-out seam**: the Rust `tokio` stream surfaces in JS via [`QueryStream::next`]
//! (wrapped into a `for await` iterator in `index.js`).
//!
//! **Tool-callback-in seam** (`NodeToolExecutor`): when the loop hits a tool call, it awaits a
//! *JS* `async` function back across the FFI boundary. Each JS tool is turned into a napi
//! `ThreadsafeFunction`; calling it (`call_async`) dispatches to the JS event loop and returns the
//! tool's `Promise<string>`, which we then await on the Rust side. This is the napi analogue of
//! PyO3's `into_future` — and it composes with the streaming seam without deadlock because the JS
//! main thread stays in its event loop while Rust awaits the oneshot the promise resolves.

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use aikit_core::orchestration::{ExecutionContext, Orchestrator, SubagentSpec};
use aikit_core::{
    evaluate_outcome as core_evaluate_outcome, evaluate_trace as core_evaluate_trace,
    request_capability_tool, Agent as CoreAgent, AgentError, AgentOptions as CoreAgentOptions,
    AikitError, ApprovalDecision, ApprovalRequest, AuditFailureMode, AuditPayloadPolicy,
    AuditTrail, BrowserEgressPolicy, BrowserTools, BudgetLedger, BudgetLimits, BudgetPolicy,
    BuiltinTools, CancellableRun, CancellationHandle, CapabilityBroker, CapabilityGate,
    Client as CoreClient, CompactionPolicy, ContainmentPolicy, DockerConfig, DurabilityMode,
    ErrorCode, ErrorInfo, EvalGate, EvalSuite, FailureContext, FailureHookOutcome, GeneratedText,
    Governance, GuardedExecutor, GuardrailChain, HookDispatcher, HookMatcher, HookOutcome,
    InMemorySessionStore, JsonFileMemoryStore, JsonFileSessionStore, JsonlAuditSink, McpClient,
    McpToolExecutor, McpToolFilter, Message, ModelCatalog, ModelPricing, ModelProfile, NoTools,
    ObjectOptions, ObjectStream as CoreObjectStream, PermissionEngine, PermissionMode,
    PermissionUpdate, PiiRedactor, PostToolOutcome, PostToolUseContext, PreToolUseContext,
    PromptContext, PromptHookOutcome, ProviderOptions, RegexBlocklist, RetryPolicy, RouteRequest,
    RoutingOptions as CoreRoutingOptions, Rule, RunCommand, RunConfig, RunOutcome, RunRecorder,
    RunState, RunTerminalStatus, Sandbox, SecretRedactor, SemanticValidation, SemanticValidator,
    Session, SessionStore, SessionStoreError, SqliteMemoryStore, SqliteSessionStore,
    StdioTransport, StopContext, StreamDelta, StreamEvent, StreamEventEncoder,
    StreamableHttpTransport, ToolApprover, ToolExecutor, ToolRouter, ToolSpec, TraceInput,
    WebTools,
};
use async_trait::async_trait;
use futures::StreamExt;
use napi::bindgen_prelude::*;
use napi::threadsafe_function::{ThreadsafeCallContext, ThreadsafeFunction};
use napi_derive::napi;
use serde_json::Value;
use tokio::sync::Mutex as TokioMutex;

/// A [`ToolExecutor`] that runs JS `async` tools — the "tool callback in" seam. Each entry is a
/// napi `ThreadsafeFunction` (safe to call from the tokio worker thread the agent loop polls on).
type HostArgs = FnArgs<(Value,)>;
type HostFunction<'scope> = Function<'scope, HostArgs, Promise<Option<Value>>>;
type HostCallback =
    Arc<ThreadsafeFunction<Value, Promise<Option<Value>>, HostArgs, Status, false, true>>;

#[derive(Default)]
struct NodeToolExecutor {
    tools: RwLock<HashMap<String, HostCallback>>,
}

/// Binding-local composite: canonical built-ins dispatch to the exact registered core suite;
/// every other name dispatches to the Node host-callback registry. Registration rejects name
/// collisions, so this routing order can never shadow a host tool.
struct NodeAgentToolExecutor {
    host: Arc<NodeToolExecutor>,
    builtins: Option<Arc<BuiltinTools>>,
    external: Arc<ToolRouter>,
}

/// Optional immutable Docker fallback for `Required(Auto)` Bash containment. Images are never
/// pulled implicitly; core validates the digest and all resource limits before launch.
#[napi(object)]
pub struct DockerContainmentOptions {
    pub image: String,
    pub executable: Option<String>,
    pub pids_limit: Option<u32>,
    pub memory_mib: Option<u32>,
    pub cpus: Option<u32>,
    pub tmpfs_mib: Option<u32>,
}

/// Required caller assertion for browser registration. Setting this to true does not install an
/// egress boundary; it asserts that the caller already configured one outside aikit.
#[napi(object)]
pub struct BrowserToolsOptions {
    pub external_egress_enforced: bool,
}

fn required_auto_containment(docker: Option<DockerContainmentOptions>) -> ContainmentPolicy {
    let policy = ContainmentPolicy::required_auto();
    let Some(docker) = docker else {
        return policy;
    };
    let mut config = DockerConfig::new(docker.image);
    if let Some(executable) = docker.executable {
        config = config.with_executable(executable);
    }
    if let Some(pids_limit) = docker.pids_limit {
        config.pids_limit = pids_limit;
    }
    if let Some(memory_mib) = docker.memory_mib {
        config.memory_bytes = u64::from(memory_mib) << 20;
    }
    if let Some(cpus) = docker.cpus {
        config.cpus = cpus;
    }
    if let Some(tmpfs_mib) = docker.tmpfs_mib {
        config.tmpfs_bytes = u64::from(tmpfs_mib) << 20;
    }
    policy.with_docker_fallback(config)
}

#[async_trait]
impl ToolExecutor for NodeAgentToolExecutor {
    async fn execute(&self, name: &str, input: Value) -> aikit_core::Result<String> {
        if let Some(builtins) = &self.builtins {
            if builtins.tool_names().contains(&name) {
                return builtins.execute(name, input).await;
            }
        }
        if self.external.contains(name) {
            return self.external.execute(name, input).await;
        }
        self.host.execute(name, input).await
    }
}

fn host_callback(function: HostFunction<'_>) -> Result<HostCallback> {
    function
        .build_threadsafe_function()
        .weak::<true>()
        .build_callback(|ctx: ThreadsafeCallContext<Value>| Ok(FnArgs::from((ctx.value,))))
        .map(Arc::new)
}

async fn call_node(callback: HostCallback, payload: Value) -> std::result::Result<Value, String> {
    let promise = callback
        .call_async(payload)
        .await
        .map_err(|error| error.to_string())?;
    promise
        .await
        .map(|value| value.unwrap_or(Value::Null))
        .map_err(|error| error.to_string())
}

async fn call_node_void(callback: HostCallback, payload: Value) -> std::result::Result<(), String> {
    let promise = callback
        .call_async(payload)
        .await
        .map_err(|error| error.to_string())?;
    promise.await.map(|_| ()).map_err(|error| error.to_string())
}

struct NodeToolApprover {
    callback: HostCallback,
}

#[async_trait]
impl ToolApprover for NodeToolApprover {
    async fn approve(&self, request: ApprovalRequest) -> ApprovalDecision {
        let payload = serde_json::json!({
            "run_id": request.run_id,
            "turn": request.turn,
            "tool_use_id": request.tool_use_id,
            "tool": request.tool,
            "input": request.input,
        });
        match call_node(self.callback.clone(), payload).await {
            Ok(Value::Bool(true)) => ApprovalDecision::Allow {
                updated_input: None,
                updated_permissions: Vec::new(),
            },
            Ok(Value::Bool(false)) => ApprovalDecision::Deny {
                message: "tool use denied by Node approver".into(),
                interrupt: false,
            },
            Ok(value) => parse_approval(value).unwrap_or_else(|message| ApprovalDecision::Deny {
                message,
                interrupt: false,
            }),
            Err(error) => ApprovalDecision::Deny {
                message: format!("Node approver failed: {error}"),
                interrupt: false,
            },
        }
    }
}

fn action(value: &Value) -> Option<&str> {
    value
        .as_str()
        .or_else(|| value.get("action").and_then(Value::as_str))
        .or_else(|| value.get("decision").and_then(Value::as_str))
}

fn parse_semantic_validation(value: Value) -> std::result::Result<SemanticValidation, String> {
    match value {
        Value::String(action) if action == "accept" => Ok(SemanticValidation::Accept),
        Value::Object(object) => match object.get("action").and_then(Value::as_str) {
            Some("accept") if object.len() == 1 => Ok(SemanticValidation::Accept),
            Some("retry") if object.len() == 2 => object
                .get("reason")
                .and_then(Value::as_str)
                .map(|reason| SemanticValidation::Retry(reason.to_string()))
                .ok_or_else(|| {
                    "semantic validator retry decision requires exactly action and string reason"
                        .into()
                }),
            Some("reject") if object.len() == 2 => object
                .get("reason")
                .and_then(Value::as_str)
                .map(|reason| SemanticValidation::Reject(reason.to_string()))
                .ok_or_else(|| {
                    "semantic validator reject decision requires exactly action and string reason"
                        .into()
                }),
            _ => Err(
                "semantic validator must resolve to 'accept' or an exact action object with retry/reject and reason"
                    .into(),
            ),
        },
        _ => Err(
            "semantic validator must resolve to 'accept' or an exact action object with retry/reject and reason"
                .into(),
        ),
    }
}

struct NodeSemanticValidator {
    callback: HostCallback,
}

#[async_trait]
impl SemanticValidator for NodeSemanticValidator {
    async fn validate(&self, value: Value) -> std::result::Result<SemanticValidation, String> {
        parse_semantic_validation(call_node(self.callback.clone(), value).await?)
    }
}

fn parse_approval(value: Value) -> std::result::Result<ApprovalDecision, String> {
    match action(&value) {
        Some("allow") => Ok(ApprovalDecision::Allow {
            updated_input: value
                .get("updated_input")
                .filter(|input| !input.is_null())
                .cloned(),
            updated_permissions: parse_permission_updates(&value)?,
        }),
        Some("deny") => Ok(ApprovalDecision::Deny {
            message: value
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("tool use denied by Node approver")
                .to_string(),
            interrupt: optional_bool(&value, "interrupt")?.unwrap_or(false),
        }),
        _ => Err(
            "Node approver returned an invalid decision; expected bool or {decision: allow|deny}"
                .into(),
        ),
    }
}

fn parse_permission_updates(value: &Value) -> std::result::Result<Vec<PermissionUpdate>, String> {
    let Some(updates) = value.get("updated_permissions") else {
        return Ok(Vec::new());
    };
    if updates.is_null() {
        return Ok(Vec::new());
    }
    let updates = updates.as_array().ok_or_else(|| {
        "Node approver updated_permissions must be an array of allow_exact_input|allow_tool"
            .to_string()
    })?;
    updates
        .iter()
        .map(|update| match update.as_str() {
            Some("allow_exact_input") => Ok(PermissionUpdate::AllowExactInput),
            Some("allow_tool") => Ok(PermissionUpdate::AllowTool),
            _ => Err(
                "Node approver updated_permissions contains an unsafe value; expected allow_exact_input|allow_tool"
                    .to_string(),
            ),
        })
        .collect()
}

fn optional_bool(value: &Value, field: &str) -> std::result::Result<Option<bool>, String> {
    match value.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Bool(value)) => Ok(Some(*value)),
        Some(_) => Err(format!("Node approver {field} must be a bool")),
    }
}

fn parse_prompt_hook(value: Value) -> PromptHookOutcome {
    if value.is_null() || matches!(action(&value), Some("continue")) {
        PromptHookOutcome::Continue
    } else {
        match action(&value) {
            Some("rewrite") => value
                .get("prompt")
                .and_then(Value::as_str)
                .map(|prompt| PromptHookOutcome::Rewrite(prompt.to_string()))
                .unwrap_or_else(|| {
                    PromptHookOutcome::Block("UserPrompt hook rewrite omitted prompt".into())
                }),
            Some("block") => PromptHookOutcome::Block(
                value
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("blocked by UserPrompt hook")
                    .to_string(),
            ),
            _ => PromptHookOutcome::Block("UserPrompt hook returned an invalid action".into()),
        }
    }
}

fn parse_pre_hook(value: Value) -> HookOutcome {
    if value.is_null() || matches!(action(&value), Some("continue")) {
        HookOutcome::Continue
    } else {
        match action(&value) {
            Some("rewrite") => value
                .get("input")
                .cloned()
                .map(HookOutcome::Rewrite)
                .unwrap_or_else(|| HookOutcome::Block("PreToolUse rewrite omitted input".into())),
            Some("block") => HookOutcome::Block(
                value
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("blocked by PreToolUse hook")
                    .to_string(),
            ),
            _ => HookOutcome::Block("PreToolUse hook returned an invalid action".into()),
        }
    }
}

fn parse_post_hook(value: Value) -> PostToolOutcome {
    if value.is_null() || matches!(action(&value), Some("continue")) {
        PostToolOutcome::Continue
    } else {
        match action(&value) {
            Some("rewrite") => value
                .get("output")
                .and_then(Value::as_str)
                .map(|output| PostToolOutcome::RewriteOutput(output.to_string()))
                .unwrap_or_else(|| {
                    PostToolOutcome::MarkError("PostToolUse rewrite omitted output".into())
                }),
            Some("error") | Some("mark_error") => PostToolOutcome::MarkError(
                value
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("marked as error by PostToolUse hook")
                    .to_string(),
            ),
            _ => PostToolOutcome::MarkError("PostToolUse hook returned an invalid action".into()),
        }
    }
}

fn parse_failure_hook(value: Value) -> FailureHookOutcome {
    if value.is_null() || matches!(action(&value), Some("continue")) {
        FailureHookOutcome::Continue
    } else if matches!(action(&value), Some("rewrite")) {
        value
            .get("error")
            .and_then(Value::as_str)
            .map(|error| FailureHookOutcome::RewriteError(error.to_string()))
            .unwrap_or(FailureHookOutcome::Continue)
    } else {
        FailureHookOutcome::Continue
    }
}

async fn run_node_failure_hook(callback: HostCallback, ctx: FailureContext) -> FailureHookOutcome {
    let stage = serde_json::to_value(ctx.stage).unwrap_or(Value::Null);
    let payload = serde_json::json!({
        "run_id": ctx.run_id,
        "turn": ctx.turn,
        "stage": stage,
        "tool_use_id": ctx.tool_use_id,
        "tool": ctx.tool,
        "error": ctx.error,
    });
    match call_node(callback, payload).await {
        Ok(value) => parse_failure_hook(value),
        Err(_) => FailureHookOutcome::Continue,
    }
}

/// Preserve the core's stable redacted classification on a real JavaScript Error object. This
/// keeps `error.code` and `error.info` branchable without parsing display text.
fn node_agent_error(env: &Env, error: AgentError) -> Error {
    let fallback = Error::from_reason(error.to_string());
    let info = error.info();
    let Ok(mut js_error) = env.create_error(Error::from_reason(error.to_string())) else {
        return fallback;
    };
    if let Ok(info_value) = env.to_js_value(&info) {
        let _ = js_error.set_named_property("info", info_value);
    }
    if let Some(code) = serde_json::to_value(info.code)
        .ok()
        .and_then(|value| value.as_str().map(str::to_string))
    {
        if let Ok(code_value) = env.create_string(&code) {
            let _ = js_error.set_named_property("code", code_value);
        }
    }
    match js_error.into_unknown(env) {
        Ok(value) => Error::from(value),
        Err(_) => fallback,
    }
}

// napi's async methods run on a Send future and therefore cannot retain `Env` long enough to add
// custom properties to a JavaScript Error. Carry the same redacted envelope through the rejection
// reason; `index.js` recognizes this private marker and reconstructs `error.code` / `error.info`.
const TYPED_ERROR_MARKER: &str = "__AIKIT_TYPED_ERROR__";

fn encoded_agent_error(error: AgentError) -> Error {
    let info = error.info();
    let payload = serde_json::json!({
        "message": error.to_string(),
        "info": info,
    });
    Error::from_reason(format!("{TYPED_ERROR_MARKER}{payload}"))
}

/// Preserve the string convenience API while also accepting the core's canonical message list.
/// Media, reasoning, tool, and citation blocks are deserialized without flattening.
fn model_input_messages(input: Value) -> std::result::Result<Vec<Message>, AikitError> {
    let messages = match input {
        Value::String(prompt) => vec![Message::user(prompt)],
        Value::Array(values) => serde_json::from_value(Value::Array(values)).map_err(|error| {
            AikitError::Configuration(format!("invalid canonical model messages: {error}"))
        })?,
        _ => {
            return Err(AikitError::Configuration(
                "model input must be a string or an array of canonical messages".into(),
            ))
        }
    };
    if messages.is_empty() {
        return Err(AikitError::Configuration(
            "canonical model messages must not be empty".into(),
        ));
    }
    Ok(messages)
}

fn node_model_input(env: &Env, input: Value) -> Result<Vec<Message>> {
    model_input_messages(input).map_err(|error| node_agent_error(env, AgentError::Core(error)))
}

fn audit_payload_policy(value: &str) -> Result<AuditPayloadPolicy> {
    match value {
        "metadata_only" => Ok(AuditPayloadPolicy::MetadataOnly),
        "full" => Ok(AuditPayloadPolicy::Full),
        other => Err(Error::from_reason(format!(
            "unknown audit payload policy '{other}' (expected metadata_only/full)"
        ))),
    }
}

fn audit_failure_mode(value: &str) -> Result<AuditFailureMode> {
    match value {
        "fail_closed" => Ok(AuditFailureMode::FailClosed),
        "best_effort" => Ok(AuditFailureMode::BestEffort),
        other => Err(Error::from_reason(format!(
            "unknown audit failure mode '{other}' (expected fail_closed/best_effort)"
        ))),
    }
}

fn jsonl_audit_trail(
    path: &str,
    payload_policy: Option<&str>,
    failure_mode: Option<&str>,
) -> Result<AuditTrail> {
    let payload_policy = audit_payload_policy(payload_policy.unwrap_or("metadata_only"))?;
    let failure_mode = audit_failure_mode(failure_mode.unwrap_or("fail_closed"))?;
    let sink = JsonlAuditSink::open(path)
        .map_err(|error| Error::from_reason(format!("failed to open audit log: {error}")))?;
    Ok(AuditTrail::new()
        .with_sink(Arc::new(sink))
        .with_payload_policy(payload_policy)
        .with_failure_mode(failure_mode))
}

fn build_orchestrator(
    binding: &Agent,
    profiles: Vec<ModelProfile>,
    options: OrchestrationOptions,
) -> Result<(Orchestrator, ExecutionContext)> {
    let catalog = ModelCatalog::new(profiles).map_err(|e| Error::from_reason(e.to_string()))?;
    let budget = options
        .budget
        .map(serde_json::from_value::<BudgetLimits>)
        .transpose()
        .map_err(|e| Error::from_reason(format!("invalid budget limits: {e}")))?
        .unwrap_or_default();
    let budget = BudgetLedger::new(budget).map_err(|e| Error::from_reason(e.to_string()))?;
    let allowed_tools = binding
        .inner
        .tool_specs()
        .iter()
        .map(|tool| tool.name.clone())
        .collect();
    let context = ExecutionContext::new(
        binding.governance(),
        binding.audit.fresh_run(),
        budget,
        allowed_tools,
    );
    let executor = binding.tool_executor();
    let orchestrator = Orchestrator::new(
        Arc::new(binding.inner.clone()),
        catalog,
        executor,
        binding.session_store.clone(),
        options.max_parallelism.unwrap_or(4) as usize,
    );
    Ok((orchestrator, context))
}

#[async_trait]
impl ToolExecutor for NodeToolExecutor {
    async fn execute(&self, name: &str, input: Value) -> aikit_core::Result<String> {
        let tsfn = self
            .tools
            .read()
            .map_err(|_| AikitError::ToolExecution("tool registry poisoned".into()))?
            .get(name)
            .cloned()
            .ok_or_else(|| AikitError::ToolExecution(format!("unknown tool '{name}'")))?;
        let value = call_node(tsfn, input)
            .await
            .map_err(AikitError::ToolExecution)?;
        value.as_str().map(str::to_owned).ok_or_else(|| {
            AikitError::ToolExecution("Node tool callback must resolve to a string".into())
        })
    }
}

#[napi]
pub struct McpServer {
    specs: Vec<ToolSpec>,
    executor: Arc<McpToolExecutor>,
    client: Arc<McpClient>,
}

fn node_mcp_tool_filter(value: Option<Value>) -> Result<McpToolFilter> {
    let Some(value) = value else {
        return Ok(McpToolFilter::default());
    };
    McpToolFilter::from_value(value).map_err(|error| Error::from_reason(error.to_string()))
}

/// Public evaluation input is stricter than backward-compatible persisted session decoding.
/// Check closed canonical shapes first, then let the shared core serde types perform all actual
/// type and enum decoding.
fn validate_eval_outcome_shape(value: &Value) -> std::result::Result<(), String> {
    fn reject_unknown(
        value: &Value,
        allowed: &[&str],
        context: &str,
    ) -> std::result::Result<(), String> {
        let Some(object) = value.as_object() else {
            return Ok(());
        };
        if object
            .keys()
            .any(|key| !allowed.iter().any(|allowed| key == allowed))
        {
            return Err(format!("{context} contains an unknown field"));
        }
        Ok(())
    }

    reject_unknown(
        value,
        &[
            "messages",
            "usage",
            "provider_metadata",
            "terminal_status",
            "stop_reason",
            "model_attempts",
            "final_text",
            "invocation_start_message_index",
        ],
        "RunOutcome",
    )?;

    let Some(outcome) = value.as_object() else {
        return Ok(());
    };
    if let Some(usage) = outcome.get("usage") {
        reject_unknown(
            usage,
            &[
                "input_tokens",
                "output_tokens",
                "cache_creation_input_tokens",
                "cache_read_input_tokens",
                "reasoning_tokens",
            ],
            "RunOutcome.usage",
        )?;
    }

    let Some(messages) = outcome.get("messages").and_then(Value::as_array) else {
        return Ok(());
    };
    for (message_index, message) in messages.iter().enumerate() {
        reject_unknown(
            message,
            &["role", "content"],
            &format!("RunOutcome.messages[{message_index}]"),
        )?;
        let Some(blocks) = message.get("content").and_then(Value::as_array) else {
            continue;
        };
        for (block_index, block) in blocks.iter().enumerate() {
            let context = format!("RunOutcome.messages[{message_index}].content[{block_index}]");
            match block.get("type").and_then(Value::as_str) {
                Some("text") => reject_unknown(block, &["type", "text"], &context)?,
                Some("reasoning") => reject_unknown(
                    block,
                    &["type", "text", "signature", "provider", "opaque"],
                    &context,
                )?,
                Some("tool_use") => {
                    reject_unknown(block, &["type", "id", "name", "input"], &context)?
                }
                Some("tool_result") => reject_unknown(
                    block,
                    &["type", "tool_use_id", "content", "is_error"],
                    &context,
                )?,
                Some("media") => {
                    reject_unknown(block, &["type", "media_type", "source"], &context)?;
                    if let Some(source) = block.get("source") {
                        let source_context = format!("{context}.source");
                        match source.get("kind").and_then(Value::as_str) {
                            Some("url") => {
                                reject_unknown(source, &["kind", "url"], &source_context)?
                            }
                            Some("base64") => {
                                reject_unknown(source, &["kind", "data"], &source_context)?
                            }
                            _ => {}
                        }
                    }
                }
                Some("citation") => {
                    reject_unknown(block, &["type", "text", "source", "metadata"], &context)?
                }
                _ => {}
            }
        }
    }
    Ok(())
}

fn parse_eval_outcome(value: Value) -> Result<RunOutcome> {
    validate_eval_outcome_shape(&value).map_err(Error::from_reason)?;
    serde_json::from_value(value)
        .map_err(|_| Error::from_reason("invalid canonical RunOutcome structure"))
}

fn validate_eval_gate_shapes(value: &Value) -> Result<()> {
    let gates = value
        .as_array()
        .ok_or_else(|| Error::from_reason("eval gates must be an array"))?;
    for (index, gate) in gates.iter().enumerate() {
        let Some(object) = gate.as_object() else {
            continue;
        };
        let allowed: &[&str] = match object.get("type").and_then(Value::as_str) {
            Some("output_exact" | "output_contains" | "output_not_contains") => &["type", "value"],
            Some("terminal_status") => &["type", "status"],
            Some("called_tool" | "did_not_call_tool") => &["type", "name"],
            Some("tool_sequence") => &["type", "names", "exact"],
            Some("no_tool_errors") => &["type"],
            Some(
                "max_turns" | "max_input_tokens" | "max_output_tokens" | "max_total_tokens"
                | "max_model_attempts",
            ) => &["type", "value"],
            _ => continue,
        };
        if object
            .keys()
            .any(|key| !allowed.iter().any(|allowed| key == allowed))
        {
            return Err(Error::from_reason(format!(
                "eval gates[{index}] contains an unknown field"
            )));
        }
    }
    Ok(())
}

/// Pure deterministic evaluation over a previously recorded canonical outcome.
#[napi]
pub fn evaluate_outcome(outcome: Value, gates: Value) -> Result<Value> {
    let outcome = parse_eval_outcome(outcome)?;
    validate_eval_gate_shapes(&gates)?;
    let gates: Vec<EvalGate> = serde_json::from_value(gates)
        .map_err(|_| Error::from_reason("invalid eval gate sequence"))?;
    let verdict = core_evaluate_outcome(&outcome, &gates)
        .map_err(|error| Error::from_reason(error.to_string()))?;
    serde_json::to_value(verdict).map_err(|_| Error::from_reason("failed to encode EvalVerdict"))
}

fn node_durability_mode(value: Option<String>) -> Result<DurabilityMode> {
    match value.as_deref().unwrap_or("sync") {
        "sync" => Ok(DurabilityMode::Sync),
        "async" => Ok(DurabilityMode::Async),
        "exit" => Ok(DurabilityMode::Exit),
        _ => Err(Error::new(
            Status::InvalidArg,
            "durability must be one of: sync, async, exit",
        )),
    }
}

fn encoded_durability_error(error: aikit_core::DurabilityError) -> Error {
    let payload = serde_json::json!({
        "message": error.to_string(),
        "info": ErrorInfo::new(ErrorCode::Conflict),
    });
    Error::from_reason(format!("{TYPED_ERROR_MARKER}{payload}"))
}

fn node_command_outcome_value(outcome: aikit_core::CommandOutcome) -> Result<Value> {
    match outcome {
        aikit_core::CommandOutcome::Resumed { sequence } => {
            Ok(serde_json::json!({"type": "resumed", "sequence": sequence}))
        }
        aikit_core::CommandOutcome::Forked { run } => Ok(serde_json::json!({
            "type": "forked",
            "run": serde_json::to_value(run)
                .map_err(|_| Error::from_reason("failed to encode forked durable run"))?,
        })),
        aikit_core::CommandOutcome::Rewound {
            checkpoint_id,
            sequence,
        } => Ok(serde_json::json!({
            "type": "rewound",
            "checkpoint_id": checkpoint_id,
            "sequence": sequence,
        })),
        aikit_core::CommandOutcome::Cancelled { sequence } => {
            Ok(serde_json::json!({"type": "cancelled", "sequence": sequence}))
        }
    }
}

/// Stateful binding over the canonical append-only Rust durability engine.
#[napi]
pub struct DurableRun {
    inner: RunState,
}

#[napi]
impl DurableRun {
    #[napi(constructor)]
    pub fn new(session_id: String, run_id: String, durability: Option<String>) -> Result<Self> {
        let inner = RunState::new(session_id, run_id, node_durability_mode(durability)?)
            .map_err(encoded_durability_error)?;
        Ok(Self { inner })
    }

    #[napi(factory)]
    pub fn from_state(state: Value) -> Result<Self> {
        let inner = serde_json::from_value(state).map_err(|error| {
            Error::new(
                Status::InvalidArg,
                format!("invalid durable state: {error}"),
            )
        })?;
        Ok(Self { inner })
    }

    #[napi]
    pub fn snapshot(&self) -> Result<Value> {
        serde_json::to_value(&self.inner)
            .map_err(|_| Error::from_reason("failed to encode durable state"))
    }

    #[napi(getter)]
    pub fn schema_version(&self) -> u32 {
        self.inner.schema_version()
    }

    #[napi(getter)]
    pub fn session_id(&self) -> String {
        self.inner.session_id().to_owned()
    }

    #[napi(getter)]
    pub fn run_id(&self) -> String {
        self.inner.run_id().to_owned()
    }

    #[napi(getter)]
    pub fn durability(&self) -> String {
        match self.inner.durability() {
            DurabilityMode::Sync => "sync",
            DurabilityMode::Async => "async",
            DurabilityMode::Exit => "exit",
        }
        .into()
    }

    #[napi(getter)]
    pub fn status(&self) -> Result<String> {
        serde_json::to_value(self.inner.status())
            .ok()
            .and_then(|value| value.as_str().map(str::to_owned))
            .ok_or_else(|| Error::from_reason("failed to encode durable status"))
    }

    #[napi]
    pub fn replace_state(&mut self, mutation_id: String, state: Value) -> Result<Value> {
        self.inner
            .replace_state(&mutation_id, state)
            .map_err(encoded_durability_error)?;
        self.snapshot()
    }

    #[napi]
    pub fn checkpoint(&mut self, checkpoint_key: String, label: Option<String>) -> Result<Value> {
        let checkpoint = self
            .inner
            .checkpoint(&checkpoint_key, label)
            .map_err(encoded_durability_error)?;
        serde_json::to_value(checkpoint)
            .map_err(|_| Error::from_reason("failed to encode durable checkpoint"))
    }

    #[napi]
    pub fn pause(&mut self, pause_id: String, reason: String) -> Result<()> {
        self.inner
            .pause(&pause_id, reason)
            .map_err(encoded_durability_error)
    }

    #[napi]
    pub fn request_approval(
        &mut self,
        logical_key: String,
        prompt: String,
        payload: Value,
        activity_id: Option<String>,
    ) -> Result<String> {
        self.inner
            .request_approval(&logical_key, activity_id, prompt, payload)
            .map_err(encoded_durability_error)
    }

    #[napi]
    pub fn complete(&mut self, completion_id: String) -> Result<()> {
        self.inner
            .complete_run(&completion_id)
            .map(|_| ())
            .map_err(encoded_durability_error)
    }

    #[napi]
    pub fn fail(&mut self, failure_id: String, error: String) -> Result<()> {
        self.inner
            .fail_run(&failure_id, error)
            .map(|_| ())
            .map_err(encoded_durability_error)
    }

    #[napi]
    pub fn apply_command(&mut self, command: Value) -> Result<Value> {
        let command: RunCommand = serde_json::from_value(command).map_err(|error| {
            Error::new(
                Status::InvalidArg,
                format!("invalid durable command: {error}"),
            )
        })?;
        let outcome = self
            .inner
            .apply_command(command)
            .map_err(encoded_durability_error)?;
        node_command_outcome_value(outcome)
    }
}

/// Evaluate stream/durability traces deterministically without provider or tool execution.
#[napi]
pub fn evaluate_trace(suite: Value, trace: Value) -> Result<Value> {
    let suite: EvalSuite = serde_json::from_value(suite)
        .map_err(|error| Error::new(Status::InvalidArg, format!("invalid EvalSuite: {error}")))?;
    let trace: TraceInput = serde_json::from_value(trace)
        .map_err(|error| Error::new(Status::InvalidArg, format!("invalid TraceInput: {error}")))?;
    serde_json::to_value(core_evaluate_trace(&suite, &trace))
        .map_err(|_| Error::from_reason("failed to encode TraceEvalResult"))
}

#[napi]
impl McpServer {
    #[napi(factory)]
    pub async fn connect_http(
        endpoint: String,
        name: String,
        bearer_token: Option<String>,
        tool_filter: Option<Value>,
    ) -> Result<Self> {
        let tool_filter = node_mcp_tool_filter(tool_filter)?;
        let transport = Arc::new(
            StreamableHttpTransport::new(&endpoint, bearer_token)
                .map_err(|error| Error::from_reason(error.to_string()))?,
        );
        let mut client = McpClient::new_with_tool_filter(transport, name, tool_filter);
        client
            .initialize()
            .await
            .map_err(|error| Error::from_reason(error.to_string()))?;
        let specs = client
            .list_tools()
            .await
            .map_err(|error| Error::from_reason(error.to_string()))?;
        let client = Arc::new(client);
        Ok(Self {
            specs,
            executor: Arc::new(McpToolExecutor::new(vec![client.clone()])),
            client,
        })
    }

    #[napi(factory)]
    pub async fn connect_stdio(
        program: String,
        args: Vec<String>,
        name: String,
        env: Option<HashMap<String, String>>,
        inherit_env: Option<bool>,
        tool_filter: Option<Value>,
    ) -> Result<Self> {
        let tool_filter = node_mcp_tool_filter(tool_filter)?;
        let env = env.unwrap_or_default().into_iter().collect();
        let transport = Arc::new(
            StdioTransport::spawn_with_env(&program, &args, &env, inherit_env.unwrap_or(false))
                .await
                .map_err(|error| Error::from_reason(error.to_string()))?,
        );
        let mut client = McpClient::new_with_tool_filter(transport, name, tool_filter);
        client
            .initialize()
            .await
            .map_err(|error| Error::from_reason(error.to_string()))?;
        let specs = client
            .list_tools()
            .await
            .map_err(|error| Error::from_reason(error.to_string()))?;
        let client = Arc::new(client);
        Ok(Self {
            specs,
            executor: Arc::new(McpToolExecutor::new(vec![client.clone()])),
            client,
        })
    }

    #[napi]
    pub async fn list_resources(&self, cursor: Option<String>) -> Result<Value> {
        serde_json::to_value(
            self.client
                .list_resources(cursor.as_deref())
                .await
                .map_err(|error| Error::from_reason(error.to_string()))?,
        )
        .map_err(|error| Error::from_reason(error.to_string()))
    }

    #[napi]
    pub async fn read_resource(&self, uri: String) -> Result<Value> {
        self.client
            .read_resource(&uri)
            .await
            .map_err(|error| Error::from_reason(error.to_string()))
    }

    #[napi]
    pub async fn list_prompts(&self, cursor: Option<String>) -> Result<Value> {
        serde_json::to_value(
            self.client
                .list_prompts(cursor.as_deref())
                .await
                .map_err(|error| Error::from_reason(error.to_string()))?,
        )
        .map_err(|error| Error::from_reason(error.to_string()))
    }

    #[napi]
    pub async fn get_prompt(&self, name: String, arguments: Value) -> Result<Value> {
        self.client
            .get_prompt(&name, arguments)
            .await
            .map_err(|error| Error::from_reason(error.to_string()))
    }
}

/// The agent-native primitive: drop in an API key → the agent gets stronger. Identical surface
/// to `aikit.Agent` in Python.
#[napi]
pub struct Agent {
    inner: CoreAgent,
    executor: Arc<NodeToolExecutor>,
    builtin_sandbox: Option<Sandbox>,
    builtin_tools: Option<Arc<BuiltinTools>>,
    external_tools: Arc<ToolRouter>,
    session_store: Arc<dyn SessionStore>,
    audit: AuditTrail,
    permissions: PermissionEngine,
    hooks: HookDispatcher,
    approver: Option<Arc<dyn ToolApprover>>,
    gated_tools: Vec<String>,
    input_guardrails: Arc<GuardrailChain>,
    output_guardrails: Arc<GuardrailChain>,
}

impl Agent {
    fn from_core(inner: CoreAgent) -> Self {
        Self {
            inner,
            executor: Arc::new(NodeToolExecutor::default()),
            builtin_sandbox: None,
            builtin_tools: None,
            external_tools: Arc::new(ToolRouter::default()),
            session_store: Arc::new(InMemorySessionStore::default()),
            audit: AuditTrail::new(),
            permissions: PermissionEngine::default(),
            hooks: HookDispatcher::new(),
            approver: None,
            gated_tools: Vec::new(),
            input_guardrails: Arc::new(GuardrailChain::default()),
            output_guardrails: Arc::new(GuardrailChain::default()),
        }
    }

    fn governance(&self) -> Governance {
        let governance = Governance::new(self.permissions.clone(), self.hooks.clone());
        match &self.approver {
            Some(approver) => governance.with_approver(approver.clone()),
            None => governance,
        }
    }

    fn tool_executor(&self) -> Arc<dyn ToolExecutor> {
        let executor: Arc<dyn ToolExecutor> = Arc::new(NodeAgentToolExecutor {
            host: self.executor.clone(),
            builtins: self.builtin_tools.clone(),
            external: self.external_tools.clone(),
        });
        let executor = match (&self.approver, self.gated_tools.is_empty()) {
            (Some(approver), false) => Arc::new(CapabilityGate::new(
                Arc::new(CapabilityBroker::new(
                    approver.clone(),
                    self.audit.run_id().to_string(),
                )),
                executor,
                self.gated_tools.clone(),
            )),
            _ => executor,
        };
        Arc::new(GuardedExecutor::new(
            executor,
            self.input_guardrails.clone(),
            self.output_guardrails.clone(),
        ))
    }

    fn install_external_tools(
        &mut self,
        specs: Vec<ToolSpec>,
        executor: Arc<dyn ToolExecutor>,
    ) -> Result<()> {
        if let Some(collision) = specs.iter().find(|spec| {
            self.inner
                .tool_specs()
                .iter()
                .any(|existing| existing.name == spec.name)
        }) {
            return Err(Error::from_reason(format!(
                "tool '{}' is already registered",
                collision.name
            )));
        }
        self.external_tools
            .register(&specs, executor)
            .map_err(|error| Error::from_reason(error.to_string()))?;
        for spec in specs {
            self.inner.add_tool(spec);
        }
        Ok(())
    }

    fn install_builtin_tools(&mut self, sandbox: Sandbox, tools: Arc<BuiltinTools>) -> Result<()> {
        let host_tools = self
            .executor
            .tools
            .read()
            .map_err(|_| Error::from_reason("tool registry poisoned"))?;
        if let Some(spec) = tools
            .specs()
            .into_iter()
            .find(|spec| host_tools.contains_key(&spec.name))
        {
            return Err(Error::from_reason(format!(
                "built-in tool name '{}' collides with a registered host tool",
                spec.name
            )));
        }
        drop(host_tools);

        let tools = self.inner.register_builtin_tools(tools);
        self.builtin_sandbox = Some(sandbox);
        self.builtin_tools = Some(tools);
        Ok(())
    }

    fn start_run(
        &self,
        env: &Env,
        input: Value,
        mut options: CoreAgentOptions,
    ) -> Result<QueryStream> {
        let messages = node_model_input(env, input)?;
        options.tools = self.inner.tool_specs().to_vec();
        options.audit = self.audit.clone();
        let executor = self.tool_executor();
        let run = within_runtime_if_available(|| {
            CoreClient::new(self.inner.clone())
                .query_cancellable_messages_with_executor(messages, options, executor)
        })
        .map_err(|error| node_agent_error(env, error))?;
        Ok(query_stream(run))
    }
}

async fn generate_configured(
    agent: CoreAgent,
    executor: Arc<dyn ToolExecutor>,
    governance: Governance,
    audit: AuditTrail,
    messages: Vec<Message>,
    model: String,
    max_tokens: u64,
) -> std::result::Result<GeneratedText, AgentError> {
    let recorder = RunRecorder::default();
    let mut config = RunConfig::new(model, messages);
    config.max_tokens = max_tokens;
    config.governance = governance;
    config.audit = audit;
    config.recorder = recorder.clone();
    let stream = agent.run_with_config(config, executor)?;
    futures::pin_mut!(stream);
    let mut stream_error = None;
    while let Some(delta) = stream.next().await {
        if let StreamDelta::Error { message, info } = delta {
            stream_error = Some((message, info));
        }
    }
    let outcome = recorder.outcome();
    if outcome.terminal_status != RunTerminalStatus::Completed {
        return Err(stream_error.map_or_else(
            || {
                AgentError::Run(
                    outcome
                        .stop_reason
                        .clone()
                        .unwrap_or_else(|| "run failed".into()),
                )
            },
            |(message, info)| AgentError::Stream { message, info },
        ));
    }
    Ok(GeneratedText {
        text: outcome.final_text.unwrap_or_default(),
        usage: outcome.usage,
        stop_reason: outcome.stop_reason,
        messages: outcome.messages,
        provider_metadata: outcome.provider_metadata,
    })
}

#[napi]
impl Agent {
    #[napi(constructor)]
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Agent::from_core(CoreAgent::from_process_env())
    }

    /// Build from an explicit environment object. Passing `{}` is useful for deterministic,
    /// keyless tests; the normal constructor discovers supported keys from the process env.
    #[napi(factory)]
    pub fn from_env(env: HashMap<String, String>) -> Self {
        Agent::from_core(CoreAgent::from_env(env))
    }

    /// Persist structured audit records as owner-only JSONL. Once configured, metadata-only and
    /// fail-closed are the defaults; leaving audit unconfigured preserves the no-sink behavior.
    #[napi]
    pub fn configure_jsonl_audit(
        &mut self,
        path: String,
        payload_policy: Option<String>,
        failure_mode: Option<String>,
    ) -> Result<()> {
        self.audit = jsonl_audit_trail(&path, payload_policy.as_deref(), failure_mode.as_deref())?;
        Ok(())
    }

    /// Reopen a crash-safe local JSON memory store under an explicit tenant namespace.
    #[napi]
    pub fn use_memory_file(&mut self, path: String, namespace: Option<String>) -> Result<()> {
        let namespace = namespace.unwrap_or_else(|| "default".into());
        if namespace.trim().is_empty() {
            return Err(Error::from_reason("memory namespace must not be empty"));
        }
        let store = JsonFileMemoryStore::open(&path)
            .map_err(|error| Error::from_reason(format!("failed to open memory file: {error}")))?;
        self.inner.set_memory_store(Arc::new(store), namespace);
        Ok(())
    }

    /// Use the process-local-CAS JSON session store for subagent execute/resume operations.
    #[napi]
    pub fn use_session_file(&mut self, path: String) -> Result<()> {
        if path.trim().is_empty() {
            return Err(Error::from_reason("session file path must not be empty"));
        }
        self.session_store = Arc::new(JsonFileSessionStore::new(path));
        Ok(())
    }

    #[napi]
    pub fn use_sqlite_memory(&mut self, path: String, namespace: Option<String>) -> Result<()> {
        let namespace = namespace.unwrap_or_else(|| "default".into());
        if namespace.trim().is_empty() {
            return Err(Error::from_reason("memory namespace must not be empty"));
        }
        let store = SqliteMemoryStore::open(path)
            .map_err(|error| Error::from_reason(format!("failed to open SQLite: {error}")))?;
        self.inner.set_memory_store(Arc::new(store), namespace);
        Ok(())
    }

    #[napi]
    pub fn use_sqlite_sessions(&mut self, path: String) -> Result<()> {
        self.session_store = Arc::new(
            SqliteSessionStore::open(path)
                .map_err(|error| Error::from_reason(error.to_string()))?,
        );
        Ok(())
    }

    /// Clear one expired execution lease after the caller has reconciled every possibly completed
    /// external side effect. This never runs a provider or tool; retry/resume remains a separate
    /// explicit call.
    #[napi]
    pub fn recover_expired_session(
        &self,
        session_id: String,
        side_effects_reconciled: bool,
    ) -> Result<u64> {
        if !side_effects_reconciled {
            return Err(Error::from_reason(
                "expired session recovery requires sideEffectsReconciled=true",
            ));
        }
        let base = match self.session_store.load_session(&session_id) {
            Ok(session) => session,
            Err(SessionStoreError::NotFound { .. }) => Session::new(session_id, Vec::new()),
            Err(error) => return Err(Error::from_reason(error.to_string())),
        };
        self.session_store
            .clear_expired_execution_lease(base)
            .map(|session| session.revision)
            .map_err(|error| Error::from_reason(error.to_string()))
    }

    #[napi]
    pub fn register_web_tools(
        &mut self,
        allowed_hosts: Vec<String>,
        search_endpoint: Option<String>,
        max_response_bytes: Option<u32>,
    ) -> Result<()> {
        let mut tools =
            WebTools::new(allowed_hosts).map_err(|error| Error::from_reason(error.to_string()))?;
        if let Some(endpoint) = search_endpoint {
            tools = tools
                .with_search_endpoint(endpoint)
                .map_err(|error| Error::from_reason(error.to_string()))?;
        }
        if let Some(bytes) = max_response_bytes {
            tools = tools.with_max_response_bytes(bytes as usize);
        }
        let specs = tools.specs();
        self.install_external_tools(specs, Arc::new(tools))
    }

    #[napi]
    pub fn register_browser_tools(
        &mut self,
        webdriver_endpoint: String,
        session_id: String,
        allowed_hosts: Vec<String>,
        options: BrowserToolsOptions,
    ) -> Result<()> {
        let policy = if options.external_egress_enforced {
            BrowserEgressPolicy::ExternallyEnforced
        } else {
            BrowserEgressPolicy::Deny
        };
        let tools = BrowserTools::new(&webdriver_endpoint, &session_id, allowed_hosts, policy)
            .map_err(|error| Error::from_reason(error.to_string()))?;
        let specs = tools.specs();
        self.install_external_tools(specs, Arc::new(tools))
    }

    #[napi]
    pub fn register_mcp(&mut self, server: &McpServer) -> Result<()> {
        self.install_external_tools(server.specs.clone(), server.executor.clone())
    }

    #[napi]
    pub fn enable_capability_requests(&mut self, gated_tools: Vec<String>) -> Result<()> {
        if self.approver.is_none() {
            return Err(Error::from_reason(
                "configure canUseTool before enabling capability requests",
            ));
        }
        if let Some(name) = gated_tools.iter().find(|name| {
            !self
                .inner
                .tool_specs()
                .iter()
                .any(|tool| tool.name == **name)
        }) {
            return Err(Error::from_reason(format!(
                "cannot gate unregistered tool '{name}'"
            )));
        }
        if !self
            .inner
            .tool_specs()
            .iter()
            .any(|tool| tool.name == "request_capability")
        {
            self.inner.add_tool(request_capability_tool());
        }
        self.gated_tools = gated_tools;
        Ok(())
    }

    #[napi]
    pub fn enable_default_guardrails(
        &mut self,
        blocked_input_patterns: Option<Vec<String>>,
    ) -> Result<()> {
        let patterns = blocked_input_patterns.unwrap_or_default();
        let pairs: Vec<_> = patterns
            .iter()
            .enumerate()
            .map(|(index, pattern)| (pattern.as_str(), format!("rule_{index}")))
            .collect();
        let blocklist = RegexBlocklist::new("blocked_input", pairs)
            .map_err(|error| Error::from_reason(error.to_string()))?;
        self.input_guardrails = Arc::new(GuardrailChain::new(vec![Arc::new(blocklist)]));
        self.output_guardrails = Arc::new(GuardrailChain::new(vec![
            Arc::new(SecretRedactor::default()),
            Arc::new(PiiRedactor::default()),
        ]));
        Ok(())
    }

    /// Add an API key; the provider is inferred from the key's format unless `provider` is given.
    /// Returns the activated provider name. An ambiguous bare `sk-` key throws (as in Python).
    #[napi]
    pub fn add_key(&mut self, key: String, provider: Option<String>) -> Result<String> {
        self.inner
            .add_key(&key, provider.as_deref(), None)
            .map(|p| p.to_string())
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Register one advertised tool and its JS async implementation.
    #[napi]
    pub fn add_tool(
        &mut self,
        name: String,
        description: String,
        input_schema: Value,
        callback: HostFunction<'_>,
    ) -> Result<()> {
        if self.inner.tool_specs().iter().any(|tool| tool.name == name) {
            return Err(Error::from_reason(format!(
                "tool '{name}' is already registered"
            )));
        }
        let callback = host_callback(callback)?;
        self.executor
            .tools
            .write()
            .map_err(|_| Error::from_reason("tool registry poisoned"))?
            .insert(name.clone(), callback);
        self.inner.add_tool(ToolSpec {
            name,
            description,
            input_schema,
        });
        Ok(())
    }

    /// Register the canonical Read/Write/Edit/Glob/Grep suite inside one or more descriptor-
    /// relative filesystem jails. Bash is intentionally not part of this call.
    #[napi]
    pub fn register_builtin_tools(&mut self, roots: Vec<String>) -> Result<()> {
        if roots.is_empty() || roots.iter().any(|root| root.trim().is_empty()) {
            return Err(Error::from_reason(
                "registerBuiltinTools requires at least one non-empty jail root",
            ));
        }
        let sandbox = Sandbox::with_roots(roots.into_iter().map(PathBuf::from))
            .map_err(|error| Error::from_reason(format!("invalid built-in jail roots: {error}")))?;
        let tools = Arc::new(BuiltinTools::new(sandbox.clone()));
        self.install_builtin_tools(sandbox, tools)
    }

    /// Add Bash to an already registered built-in suite using the core's fail-closed
    /// `Required(Auto)` OS containment. An optional immutable Docker fallback makes the same
    /// contract usable off macOS. This binding exposes no uncontained Bash mode.
    #[napi]
    pub fn enable_bash_with_required_containment(
        &mut self,
        docker: Option<DockerContainmentOptions>,
    ) -> Result<()> {
        let sandbox = self.builtin_sandbox.clone().ok_or_else(|| {
            Error::from_reason("registerBuiltinTools must be called before enabling contained Bash")
        })?;
        let tools = Arc::new(
            BuiltinTools::new(sandbox.clone())
                .with_containment_policy(required_auto_containment(docker)),
        );
        self.install_builtin_tools(sandbox, tools)
    }

    /// Actively probe the required Bash containment backends. A missing backend is reported as
    /// unavailable and Bash execution remains fail-closed.
    #[napi(ts_return_type = "Promise<any>")]
    pub async fn builtin_containment_capabilities(&self) -> Result<Value> {
        let tools = self.builtin_tools.clone().ok_or_else(|| {
            Error::from_reason("enableBashWithRequiredContainment has not been called")
        })?;
        if !tools.tool_names().contains(&"Bash") {
            return Err(Error::from_reason(
                "enableBashWithRequiredContainment has not been called",
            ));
        }
        let report = tools.containment_capabilities().await;
        serde_json::to_value(report).map_err(|error| Error::from_reason(error.to_string()))
    }

    /// Replace this Agent's declarative permission policy.
    #[napi]
    pub fn set_permissions(
        &mut self,
        rules: Option<Vec<RuleSpec>>,
        default_mode: Option<String>,
    ) -> Result<()> {
        let mode = permission_mode(default_mode.as_deref().unwrap_or("allow"))?;
        self.permissions = build_permissions(rules, mode)?;
        Ok(())
    }

    /// Register an async human/host approval callback for `ask` permission decisions.
    #[napi]
    pub fn can_use_tool(&mut self, callback: HostFunction<'_>) -> Result<()> {
        self.approver = Some(Arc::new(NodeToolApprover {
            callback: host_callback(callback)?,
        }));
        Ok(())
    }

    #[napi]
    pub fn on_user_prompt(&mut self, callback: HostFunction<'_>) -> Result<()> {
        let callback = host_callback(callback)?;
        self.hooks
            .on_user_prompt_submit_async(move |ctx: PromptContext| {
                let callback = callback.clone();
                async move {
                    let payload = serde_json::json!({
                        "run_id": ctx.run_id,
                        "prompt": ctx.prompt,
                    });
                    match call_node(callback, payload).await {
                        Ok(value) => parse_prompt_hook(value),
                        Err(error) => PromptHookOutcome::Block(format!(
                            "Node UserPrompt callback failed: {error}"
                        )),
                    }
                }
            });
        Ok(())
    }

    #[napi]
    pub fn on_pre_tool_use(
        &mut self,
        callback: HostFunction<'_>,
        tool: Option<String>,
    ) -> Result<()> {
        let callback = host_callback(callback)?;
        let matcher = tool.map(HookMatcher::tool).unwrap_or_else(HookMatcher::any);
        self.hooks
            .on_pre_tool_use_async(matcher, move |ctx: PreToolUseContext| {
                let callback = callback.clone();
                async move {
                    let payload = serde_json::json!({
                        "run_id": ctx.run_id,
                        "turn": ctx.turn,
                        "tool_use_id": ctx.tool_use_id,
                        "tool": ctx.tool,
                        "input": ctx.input,
                    });
                    match call_node(callback, payload).await {
                        Ok(value) => parse_pre_hook(value),
                        Err(error) => {
                            HookOutcome::Block(format!("Node PreToolUse callback failed: {error}"))
                        }
                    }
                }
            });
        Ok(())
    }

    #[napi]
    pub fn on_post_tool_use(
        &mut self,
        callback: HostFunction<'_>,
        tool: Option<String>,
    ) -> Result<()> {
        let callback = host_callback(callback)?;
        let matcher = tool.map(HookMatcher::tool).unwrap_or_else(HookMatcher::any);
        self.hooks
            .on_post_tool_use_async(matcher, move |ctx: PostToolUseContext| {
                let callback = callback.clone();
                async move {
                    let duration_ms = u64::try_from(ctx.duration_ms).unwrap_or(u64::MAX);
                    let payload = serde_json::json!({
                        "run_id": ctx.run_id,
                        "turn": ctx.turn,
                        "tool_use_id": ctx.tool_use_id,
                        "tool": ctx.tool,
                        "input": ctx.input,
                        "output": ctx.output,
                        "duration_ms": duration_ms,
                    });
                    match call_node(callback, payload).await {
                        Ok(value) => parse_post_hook(value),
                        Err(error) => PostToolOutcome::MarkError(format!(
                            "Node PostToolUse callback failed: {error}"
                        )),
                    }
                }
            });
        Ok(())
    }

    #[napi]
    pub fn on_failure(&mut self, callback: HostFunction<'_>) -> Result<()> {
        let callback = host_callback(callback)?;
        self.hooks.on_failure_async(move |ctx: FailureContext| {
            let callback = callback.clone();
            async move { run_node_failure_hook(callback, ctx).await }
        });
        Ok(())
    }

    /// Register an async failure callback limited to failures associated with one tool use. These
    /// callbacks always run before global `onFailure` callbacks, matching the core hook contract.
    #[napi]
    pub fn on_post_tool_failure(
        &mut self,
        callback: HostFunction<'_>,
        tool: Option<String>,
    ) -> Result<()> {
        let callback = host_callback(callback)?;
        let matcher = tool.map(HookMatcher::tool).unwrap_or_else(HookMatcher::any);
        self.hooks
            .on_post_tool_failure_async(matcher, move |ctx: FailureContext| {
                let callback = callback.clone();
                async move { run_node_failure_hook(callback, ctx).await }
            });
        Ok(())
    }

    #[napi]
    pub fn on_stop(&mut self, callback: HostFunction<'_>) -> Result<()> {
        let callback = host_callback(callback)?;
        self.hooks.on_stop_async(move |ctx: StopContext| {
            let callback = callback.clone();
            async move {
                let payload = serde_json::json!({
                    "run_id": ctx.run_id,
                    "turns": ctx.turns,
                    "reason": ctx.reason,
                    "usage": ctx.usage,
                });
                let _ = call_node_void(callback, payload).await;
            }
        });
        Ok(())
    }

    /// The activated provider names.
    #[napi]
    pub fn active_providers(&self) -> Vec<String> {
        self.inner
            .active_providers()
            .into_iter()
            .map(String::from)
            .collect()
    }

    /// Whether a provider is currently activated (has a credential).
    #[napi]
    pub fn has_provider(&self, provider: String) -> bool {
        self.inner.has_provider(&provider)
    }

    /// Introspect what the agent can do *right now* — grows as keys are added. Returns the same
    /// shape as Python's `Agent.capabilities()`.
    #[napi(ts_return_type = "any")]
    pub fn capabilities(&self) -> Result<Value> {
        serde_json::to_value(self.inner.capabilities())
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Generate one complete text response with the provider selected from `options.model`.
    /// The default mock model is deterministic and keyless; live models require `addKey` first.
    #[napi(ts_return_type = "Promise<any>")]
    pub async fn generate_text(
        &self,
        input: Value,
        options: Option<GenerateTextOptions>,
    ) -> Result<Value> {
        let options = options.unwrap_or(GenerateTextOptions {
            model: None,
            max_tokens: None,
        });
        let generated = generate_configured(
            self.inner.clone(),
            self.tool_executor(),
            self.governance(),
            self.audit.clone(),
            model_input_messages(input)
                .map_err(|error| encoded_agent_error(AgentError::Core(error)))?,
            options.model.unwrap_or_else(|| "mock-1".into()),
            u64::from(options.max_tokens.unwrap_or(1024)),
        )
        .await
        .map_err(encoded_agent_error)?;
        serde_json::to_value(generated).map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Stream canonical deltas with the provider selected from `options.model`.
    #[napi(ts_return_type = "QueryStream")]
    pub fn stream_text(
        &self,
        env: Env,
        input: Value,
        options: Option<GenerateTextOptions>,
    ) -> Result<QueryStream> {
        let options = options.unwrap_or(GenerateTextOptions {
            model: None,
            max_tokens: None,
        });
        let run_options = CoreAgentOptions {
            model: options.model.unwrap_or_else(|| "mock-1".into()),
            max_tokens: u64::from(options.max_tokens.unwrap_or(1024)),
            governance: self.governance(),
            ..CoreAgentOptions::default()
        };
        self.start_run(&env, input, run_options)
    }

    /// Start a cancellable governed run using the complete shared core RunOptions surface.
    #[napi(ts_return_type = "QueryStream")]
    pub fn run(&self, env: Env, input: Value, options: Option<RunOptions>) -> Result<QueryStream> {
        let options = build_agent_options(options, self.governance())?;
        self.start_run(&env, input, options)
    }

    /// Snapshot this configured Agent into a reusable high-level Client.
    #[napi]
    pub fn client(&self) -> Client {
        Client::from_agent(self)
    }

    /// Explicitly persist one JSON-compatible value. Model output is never remembered
    /// automatically.
    #[napi]
    pub fn remember(&self, key: String, value: Value) -> Result<()> {
        self.inner
            .remember(key, value)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Plane-aware compare-and-swap memory update for concurrent agents.
    #[napi]
    pub fn remember_cas(
        &self,
        key: String,
        value: Value,
        expected_revision: BigInt,
        plane: Option<String>,
        provenance: Option<Value>,
    ) -> Result<BigInt> {
        let (signed, expected_revision, lossless) = expected_revision.get_u64();
        if signed || !lossless {
            return Err(Error::from_reason(
                "expectedRevision must be a non-negative u64 bigint",
            ));
        }
        let plane = match plane.as_deref().unwrap_or("working") {
            "working" => aikit_core::MemoryPlane::Working,
            "episodic" => aikit_core::MemoryPlane::Episodic,
            "semantic" => aikit_core::MemoryPlane::Semantic,
            _ => {
                return Err(Error::from_reason(
                    "plane must be working, episodic, or semantic",
                ))
            }
        };
        let provenance = provenance
            .map(serde_json::from_value)
            .transpose()
            .map_err(|error| Error::from_reason(format!("invalid memory provenance: {error}")))?
            .unwrap_or_default();
        self.inner
            .remember_cas(key, value, plane, provenance, expected_revision)
            .map(BigInt::from)
            .map_err(|error| Error::from_reason(error.to_string()))
    }

    /// Search explicit memories in this agent's namespace.
    #[napi(ts_return_type = "any")]
    pub fn recall(&self, query: String, limit: Option<u32>) -> Result<Value> {
        let entries = self
            .inner
            .recall(&query, limit.unwrap_or(10) as usize)
            .map_err(|e| Error::from_reason(e.to_string()))?;
        serde_json::to_value(entries).map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Deterministically route over caller-supplied model profiles. The core replaces the
    /// request's provider set with this agent's active provider names.
    #[napi(ts_return_type = "any")]
    pub fn route(&self, profiles: Value, request: Value) -> Result<Value> {
        let profiles: Vec<ModelProfile> = serde_json::from_value(profiles)
            .map_err(|e| Error::from_reason(format!("invalid model profiles: {e}")))?;
        let request: RouteRequest = serde_json::from_value(request)
            .map_err(|e| Error::from_reason(format!("invalid route request: {e}")))?;
        let catalog = ModelCatalog::new(profiles).map_err(|e| Error::from_reason(e.to_string()))?;
        let decision = self
            .inner
            .route(&catalog, request)
            .map_err(|e| Error::from_reason(e.to_string()))?;
        serde_json::to_value(decision).map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Run one governed, budget-aware child. Registered host tools, hooks, approvals, and
    /// permission policy are inherited, then narrowed by the child's allowed-tools scope.
    #[napi(ts_return_type = "Promise<any>")]
    pub async fn run_subagent(
        &self,
        spec: Value,
        profiles: Value,
        options: Option<OrchestrationOptions>,
    ) -> Result<Value> {
        let spec: SubagentSpec = serde_json::from_value(spec)
            .map_err(|e| Error::from_reason(format!("invalid subagent spec: {e}")))?;
        let profiles: Vec<ModelProfile> = serde_json::from_value(profiles)
            .map_err(|e| Error::from_reason(format!("invalid model profiles: {e}")))?;
        let (orchestrator, context) =
            build_orchestrator(self, profiles, options.unwrap_or_default())?;
        let result = orchestrator.execute(spec, &context).await;
        serde_json::to_value(result).map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Run independent children with bounded concurrency while preserving input order.
    #[napi(ts_return_type = "Promise<any>")]
    pub async fn fan_out(
        &self,
        specs: Value,
        profiles: Value,
        options: Option<OrchestrationOptions>,
    ) -> Result<Value> {
        let specs: Vec<SubagentSpec> = serde_json::from_value(specs)
            .map_err(|e| Error::from_reason(format!("invalid subagent specs: {e}")))?;
        let profiles: Vec<ModelProfile> = serde_json::from_value(profiles)
            .map_err(|e| Error::from_reason(format!("invalid model profiles: {e}")))?;
        let (orchestrator, context) =
            build_orchestrator(self, profiles, options.unwrap_or_default())?;
        let results = orchestrator.fan_out(specs, &context).await;
        serde_json::to_value(results).map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Run a parallel council and synthesize only after the requested quorum succeeds.
    #[napi(ts_return_type = "Promise<any>")]
    pub async fn council(
        &self,
        members: Value,
        synthesizer: Value,
        profiles: Value,
        min_successes: Option<u32>,
        options: Option<OrchestrationOptions>,
    ) -> Result<Value> {
        let members: Vec<SubagentSpec> = serde_json::from_value(members)
            .map_err(|e| Error::from_reason(format!("invalid council members: {e}")))?;
        let synthesizer: SubagentSpec = serde_json::from_value(synthesizer)
            .map_err(|e| Error::from_reason(format!("invalid synthesizer spec: {e}")))?;
        let profiles: Vec<ModelProfile> = serde_json::from_value(profiles)
            .map_err(|e| Error::from_reason(format!("invalid model profiles: {e}")))?;
        let (orchestrator, context) =
            build_orchestrator(self, profiles, options.unwrap_or_default())?;
        let result = orchestrator
            .council(
                members,
                synthesizer,
                min_successes.unwrap_or(1) as usize,
                &context,
            )
            .await;
        serde_json::to_value(result).map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Resume a previously persisted child session through the same per-Agent store and CAS
    /// contract used by the Rust core.
    #[napi(ts_return_type = "Promise<any>")]
    pub async fn resume_subagent(
        &self,
        session_id: String,
        spec: Value,
        profiles: Value,
        options: Option<OrchestrationOptions>,
    ) -> Result<Value> {
        let spec: SubagentSpec = serde_json::from_value(spec)
            .map_err(|e| Error::from_reason(format!("invalid subagent spec: {e}")))?;
        let profiles: Vec<ModelProfile> = serde_json::from_value(profiles)
            .map_err(|e| Error::from_reason(format!("invalid model profiles: {e}")))?;
        let (orchestrator, context) =
            build_orchestrator(self, profiles, options.unwrap_or_default())?;
        let result = orchestrator.resume(&session_id, spec, &context).await;
        serde_json::to_value(result).map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Generate a schema-validated object. Defaults to the deterministic keyless
    /// `mock-structured` model; use a live model after activating its provider with `addKey`.
    #[napi(ts_return_type = "Promise<any>")]
    pub fn generate_object(
        &self,
        env: Env,
        input: Value,
        schema: Value,
        options: Option<GenerateObjectOptions>,
        validator: Option<HostFunction<'_>>,
    ) -> Result<AsyncBlock<Value>> {
        let semantic_validator = validator.map(host_callback).transpose()?.map(|callback| {
            Arc::new(NodeSemanticValidator { callback }) as Arc<dyn SemanticValidator>
        });
        let options = options.unwrap_or(GenerateObjectOptions {
            model: None,
            max_retries: None,
            max_tokens: None,
            name: None,
            provider_options: None,
        });
        let provider_options: ProviderOptions = options
            .provider_options
            .map(serde_json::from_value)
            .transpose()
            .map_err(|error| {
                Error::from_reason(format!("invalid structured providerOptions: {error}"))
            })?
            .unwrap_or_default();
        let agent = self.inner.clone();
        let messages = model_input_messages(input)
            .map_err(|error| encoded_agent_error(AgentError::Core(error)))?;
        let audit = self.audit.fresh_run();
        let model = options.model.unwrap_or_else(|| "mock-structured".into());
        let object_options = ObjectOptions {
            max_retries: options.max_retries.unwrap_or(2),
            max_tokens: u64::from(options.max_tokens.unwrap_or(1024)),
            name: options.name.unwrap_or_else(|| "respond".into()),
            provider_options,
            semantic_validator,
        };
        AsyncBlockBuilder::new(async move {
            let result = agent
                .generate_object_messages_with_audit(
                    messages,
                    schema,
                    &model,
                    object_options,
                    Some(&audit),
                )
                .await
                .map_err(encoded_agent_error)?;
            serde_json::to_value(result).map_err(|error| Error::from_reason(error.to_string()))
        })
        .build(&env)
    }

    /// Stream every structured-output attempt and provider delta as it occurs. Validation failures
    /// and repair attempts remain visible; only a schema-validated value produces `completed`.
    #[napi(ts_return_type = "ObjectStream")]
    pub fn stream_object(
        &self,
        env: Env,
        input: Value,
        schema: Value,
        options: Option<GenerateObjectOptions>,
        validator: Option<HostFunction<'_>>,
    ) -> Result<ObjectStream> {
        let semantic_validator = validator.map(host_callback).transpose()?.map(|callback| {
            Arc::new(NodeSemanticValidator { callback }) as Arc<dyn SemanticValidator>
        });
        let options = options.unwrap_or(GenerateObjectOptions {
            model: None,
            max_retries: None,
            max_tokens: None,
            name: None,
            provider_options: None,
        });
        let provider_options: ProviderOptions = options
            .provider_options
            .map(serde_json::from_value)
            .transpose()
            .map_err(|error| {
                Error::from_reason(format!("invalid structured providerOptions: {error}"))
            })?
            .unwrap_or_default();
        let audit = self.audit.fresh_run();
        let messages = node_model_input(&env, input)?;
        let stream = self
            .inner
            .stream_object_messages_with_audit(
                messages,
                schema,
                options.model.as_deref().unwrap_or("mock-structured"),
                ObjectOptions {
                    max_retries: options.max_retries.unwrap_or(2),
                    max_tokens: u64::from(options.max_tokens.unwrap_or(1024)),
                    name: options.name.unwrap_or_else(|| "respond".into()),
                    provider_options,
                    semantic_validator,
                },
                Some(&audit),
            )
            .map_err(|error| node_agent_error(&env, error))?;
        Ok(ObjectStream {
            inner: Arc::new(TokioMutex::new(stream)),
        })
    }
}

/// Reusable high-level client that snapshots one configured Agent while retaining credentials,
/// registered tools, and governance callbacks across queries.
#[napi]
pub struct Client {
    inner: CoreClient,
    executor: Arc<dyn ToolExecutor>,
    governance: Governance,
    audit: AuditTrail,
}

impl Client {
    fn from_agent(agent: &Agent) -> Self {
        Self {
            inner: CoreClient::new(agent.inner.clone()),
            executor: agent.tool_executor(),
            governance: agent.governance(),
            audit: agent.audit.clone(),
        }
    }
}

#[napi]
impl Client {
    #[napi(constructor)]
    pub fn new(agent: ClassInstance<Agent>) -> Self {
        Self::from_agent(&agent)
    }

    #[napi(ts_return_type = "QueryStream")]
    pub fn query(
        &self,
        env: Env,
        input: Value,
        options: Option<RunOptions>,
    ) -> Result<QueryStream> {
        let messages = node_model_input(&env, input)?;
        let mut options = build_agent_options(options, self.governance.clone())?;
        options.tools = self.inner.agent().tool_specs().to_vec();
        options.audit = self.audit.clone();
        let executor = self.executor.clone();
        let run = within_runtime_if_available(|| {
            self.inner
                .query_cancellable_messages_with_executor(messages, options, executor)
        })
        .map_err(|error| node_agent_error(&env, error))?;
        Ok(query_stream(run))
    }
}

/// Options shared by [`Agent::generate_text`] and [`Agent::stream_text`].
#[napi(object)]
pub struct GenerateTextOptions {
    pub model: Option<String>,
    pub max_tokens: Option<u32>,
}

/// High-level per-run controls, mapped directly onto the shared Rust `AgentOptions` contract.
#[napi(object)]
#[derive(Default)]
pub struct RunOptions {
    pub model: Option<String>,
    pub fallback_models: Option<Vec<String>>,
    pub max_tokens: Option<u32>,
    pub max_turns: Option<u32>,
    pub provider_options: Option<Value>,
    pub budget: Option<Value>,
    pub retry: Option<Value>,
    pub routing: Option<Value>,
    pub compaction: Option<Value>,
    /// Wrapper-private bridge for an AbortSignal that was already aborted before `run()`.
    pub cancel_before_start: Option<bool>,
}

fn parse_routing_options(value: Option<Value>) -> Result<Option<CoreRoutingOptions>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let object = value
        .as_object()
        .ok_or_else(|| Error::from_reason("RunOptions.routing must be an object"))?;
    if object
        .keys()
        .any(|key| key != "profiles" && key != "request")
    {
        return Err(Error::from_reason(
            "RunOptions.routing accepts only profiles and request",
        ));
    }
    let profiles = object
        .get("profiles")
        .cloned()
        .ok_or_else(|| Error::from_reason("RunOptions.routing.profiles is required"))?;
    let request = object
        .get("request")
        .cloned()
        .ok_or_else(|| Error::from_reason("RunOptions.routing.request is required"))?;
    let profiles: Vec<ModelProfile> = serde_json::from_value(profiles)
        .map_err(|error| Error::from_reason(format!("invalid routing profiles: {error}")))?;
    let request: RouteRequest = serde_json::from_value(request)
        .map_err(|error| Error::from_reason(format!("invalid routing request: {error}")))?;
    let catalog = ModelCatalog::new(profiles)
        .map_err(|error| Error::from_reason(format!("invalid routing catalog: {error}")))?;
    Ok(Some(CoreRoutingOptions::new(catalog, request)))
}

fn field<'a>(
    object: &'a serde_json::Map<String, Value>,
    context: &str,
    snake: &str,
    camel: &str,
) -> Result<Option<&'a Value>> {
    if snake != camel && object.contains_key(snake) && object.contains_key(camel) {
        return Err(Error::from_reason(format!(
            "{context} contains duplicate aliases '{snake}' and '{camel}'"
        )));
    }
    Ok(object.get(snake).or_else(|| object.get(camel)))
}

fn optional_u64(
    object: &serde_json::Map<String, Value>,
    context: &str,
    snake: &str,
    camel: &str,
) -> Result<Option<u64>> {
    match field(object, context, snake, camel)? {
        None | Some(Value::Null) => Ok(None),
        Some(value) => value.as_u64().map(Some).ok_or_else(|| {
            Error::from_reason(format!("{context}.{camel} must be a non-negative integer"))
        }),
    }
}

fn optional_f64(
    object: &serde_json::Map<String, Value>,
    context: &str,
    snake: &str,
    camel: &str,
) -> Result<Option<f64>> {
    match field(object, context, snake, camel)? {
        None | Some(Value::Null) => Ok(None),
        Some(value) => value
            .as_f64()
            .filter(|value| value.is_finite())
            .map(Some)
            .ok_or_else(|| Error::from_reason(format!("{context}.{camel} must be finite"))),
    }
}

fn reject_unknown_fields(
    object: &serde_json::Map<String, Value>,
    context: &str,
    allowed: &[&str],
) -> Result<()> {
    if let Some(field) = object
        .keys()
        .find(|field| !allowed.contains(&field.as_str()))
    {
        return Err(Error::from_reason(format!(
            "{context} contains unknown field '{field}'"
        )));
    }
    Ok(())
}

fn parse_budget_policy(value: Option<Value>) -> Result<BudgetPolicy> {
    let Some(value) = value.filter(|value| !value.is_null()) else {
        return Ok(BudgetPolicy::default());
    };
    let object = value
        .as_object()
        .ok_or_else(|| Error::from_reason("RunOptions.budget must be an object"))?;
    reject_unknown_fields(
        object,
        "RunOptions.budget",
        &[
            "max_total_tokens",
            "maxTotalTokens",
            "max_cost_usd",
            "maxCostUsd",
            "pricing",
        ],
    )?;
    let pricing = match object.get("pricing").filter(|value| !value.is_null()) {
        None => None,
        Some(pricing) => {
            let pricing = pricing
                .as_object()
                .ok_or_else(|| Error::from_reason("RunOptions.budget.pricing must be an object"))?;
            reject_unknown_fields(
                pricing,
                "RunOptions.budget.pricing",
                &[
                    "input_per_million_usd",
                    "inputPerMillionUsd",
                    "output_per_million_usd",
                    "outputPerMillionUsd",
                    "cache_read_per_million_usd",
                    "cacheReadPerMillionUsd",
                    "cache_write_per_million_usd",
                    "cacheWritePerMillionUsd",
                ],
            )?;
            Some(ModelPricing {
                input_per_million_usd: optional_f64(
                    pricing,
                    "RunOptions.budget.pricing",
                    "input_per_million_usd",
                    "inputPerMillionUsd",
                )?
                .ok_or_else(|| {
                    Error::from_reason("RunOptions.budget.pricing.inputPerMillionUsd is required")
                })?,
                output_per_million_usd: optional_f64(
                    pricing,
                    "RunOptions.budget.pricing",
                    "output_per_million_usd",
                    "outputPerMillionUsd",
                )?
                .ok_or_else(|| {
                    Error::from_reason("RunOptions.budget.pricing.outputPerMillionUsd is required")
                })?,
                cache_read_per_million_usd: optional_f64(
                    pricing,
                    "RunOptions.budget.pricing",
                    "cache_read_per_million_usd",
                    "cacheReadPerMillionUsd",
                )?,
                cache_write_per_million_usd: optional_f64(
                    pricing,
                    "RunOptions.budget.pricing",
                    "cache_write_per_million_usd",
                    "cacheWritePerMillionUsd",
                )?,
            })
        }
    };
    let policy = BudgetPolicy {
        max_total_tokens: optional_u64(
            object,
            "RunOptions.budget",
            "max_total_tokens",
            "maxTotalTokens",
        )?,
        max_cost_usd: optional_f64(object, "RunOptions.budget", "max_cost_usd", "maxCostUsd")?,
        pricing,
    };
    policy
        .validate()
        .map_err(|error| Error::from_reason(error.to_string()))?;
    Ok(policy)
}

fn parse_retry_policy(value: Option<Value>) -> Result<RetryPolicy> {
    let Some(value) = value.filter(|value| !value.is_null()) else {
        return Ok(RetryPolicy::default());
    };
    let object = value
        .as_object()
        .ok_or_else(|| Error::from_reason("RunOptions.retry must be an object"))?;
    reject_unknown_fields(
        object,
        "RunOptions.retry",
        &[
            "max_attempts_per_model",
            "maxAttemptsPerModel",
            "base_delay_ms",
            "baseDelayMs",
            "max_delay_ms",
            "maxDelayMs",
            "per_attempt_timeout_ms",
            "perAttemptTimeoutMs",
        ],
    )?;
    let mut retry = RetryPolicy::default();
    if let Some(value) = optional_u64(
        object,
        "RunOptions.retry",
        "max_attempts_per_model",
        "maxAttemptsPerModel",
    )? {
        retry.max_attempts_per_model = u32::try_from(value)
            .map_err(|_| Error::from_reason("RunOptions.retry.maxAttemptsPerModel exceeds u32"))?;
    }
    if let Some(value) = optional_u64(object, "RunOptions.retry", "base_delay_ms", "baseDelayMs")? {
        retry.base_delay_ms = value;
    }
    if let Some(value) = optional_u64(object, "RunOptions.retry", "max_delay_ms", "maxDelayMs")? {
        retry.max_delay_ms = value;
    }
    if let Some(value) = optional_u64(
        object,
        "RunOptions.retry",
        "per_attempt_timeout_ms",
        "perAttemptTimeoutMs",
    )? {
        retry.per_attempt_timeout_ms = value;
    }
    Ok(retry)
}

fn build_agent_options(
    options: Option<RunOptions>,
    governance: Governance,
) -> Result<CoreAgentOptions> {
    let options = options.unwrap_or_default();
    let mut mapped = CoreAgentOptions {
        governance,
        ..CoreAgentOptions::default()
    };
    if let Some(model) = options.model {
        mapped.model = model;
    }
    mapped.fallback_models = options.fallback_models.unwrap_or_default();
    if let Some(max_tokens) = options.max_tokens {
        mapped.max_tokens = u64::from(max_tokens);
    }
    if let Some(max_turns) = options.max_turns {
        mapped.max_turns = max_turns as usize;
    }
    if let Some(provider_options) = options.provider_options {
        mapped.provider_options = serde_json::from_value(provider_options).map_err(|error| {
            Error::from_reason(format!("invalid RunOptions.providerOptions: {error}"))
        })?;
    }
    mapped.budget = parse_budget_policy(options.budget)?;
    mapped.retry = parse_retry_policy(options.retry)?;
    mapped.routing = parse_routing_options(options.routing)?;
    if let Some(compaction) = options.compaction.filter(|value| !value.is_null()) {
        let object = compaction
            .as_object()
            .ok_or_else(|| Error::from_reason("RunOptions.compaction must be an object"))?;
        reject_unknown_fields(
            object,
            "RunOptions.compaction",
            &[
                "max_context_tokens",
                "maxContextTokens",
                "keep_recent_messages",
                "keepRecentMessages",
            ],
        )?;
        let max_context_tokens = optional_u64(
            object,
            "RunOptions.compaction",
            "max_context_tokens",
            "maxContextTokens",
        )?
        .ok_or_else(|| Error::from_reason("RunOptions.compaction.maxContextTokens is required"))?;
        let keep_recent_messages = optional_u64(
            object,
            "RunOptions.compaction",
            "keep_recent_messages",
            "keepRecentMessages",
        )?
        .unwrap_or(8);
        mapped.compaction = CompactionPolicy::new(
            max_context_tokens,
            usize::try_from(keep_recent_messages).map_err(|_| {
                Error::from_reason("RunOptions.compaction.keepRecentMessages exceeds usize")
            })?,
        );
    }
    if options.cancel_before_start.unwrap_or(false) {
        mapped.cancellation.cancel();
    }
    Ok(mapped)
}

/// Shared orchestration controls. Budget values use the Rust core's serialized
/// `BudgetLimits` shape (`max_model_calls`, token limits, micro-USD, and wall time).
#[napi(object)]
#[derive(Default)]
pub struct OrchestrationOptions {
    pub max_parallelism: Option<u32>,
    pub budget: Option<Value>,
}

/// Options for [`Agent::generate_object`].
#[napi(object)]
pub struct GenerateObjectOptions {
    pub model: Option<String>,
    pub max_retries: Option<u32>,
    pub max_tokens: Option<u32>,
    pub name: Option<String>,
    pub provider_options: Option<Value>,
}

/// A single permission rule, mirroring the Python `{"effect","tool","pattern"?,"field"?}` dict.
/// `pattern` is a regex matched against the tool input's decoded string values.
#[napi(object)]
pub struct RuleSpec {
    pub id: Option<String>,
    pub effect: String,
    pub tool: String,
    pub pattern: Option<String>,
    pub field: Option<String>,
}

/// Options for [`query`], mirroring the Python keyword arguments.
#[napi(object)]
pub struct QueryOptions {
    pub model: Option<String>,
    pub fallback_models: Option<Vec<String>>,
    pub max_tokens: Option<u32>,
    pub max_turns: Option<u32>,
    pub provider_options: Option<Value>,
    pub budget: Option<Value>,
    pub retry: Option<Value>,
    pub routing: Option<Value>,
    pub compaction: Option<Value>,
    pub permissions: Option<Vec<RuleSpec>>,
    pub default_mode: Option<String>,
    /// Wrapper-private bridge for an AbortSignal that was already aborted before `query()`.
    pub cancel_before_start: Option<bool>,
}

fn permission_mode(mode: &str) -> Result<PermissionMode> {
    match mode {
        "allow" => Ok(PermissionMode::Allow),
        "deny" => Ok(PermissionMode::Deny),
        "ask" => Ok(PermissionMode::Ask),
        other => Err(Error::from_reason(format!(
            "unknown permission mode '{other}' (expected allow/deny/ask)"
        ))),
    }
}

/// Parse permission-rule specs into the shared permission engine.
fn build_permissions(
    rules: Option<Vec<RuleSpec>>,
    mode: PermissionMode,
) -> Result<PermissionEngine> {
    let mut parsed: Vec<Rule> = Vec::new();
    if let Some(rules) = rules {
        for r in rules {
            let mut base = match r.effect.as_str() {
                "allow" => Rule::allow(r.tool),
                "deny" => Rule::deny(r.tool),
                "ask" => Rule::ask(r.tool),
                other => {
                    return Err(Error::from_reason(format!(
                        "unknown permission effect '{other}' (expected allow/deny/ask)"
                    )))
                }
            };
            if let Some(id) = r.id {
                base = base.named(id);
            }
            let rule = match (r.field, r.pattern) {
                (Some(f), Some(p)) => base
                    .matching_field(f, &p)
                    .map_err(|e| Error::from_reason(e.to_string()))?,
                (None, Some(p)) => base
                    .matching(&p)
                    .map_err(|e| Error::from_reason(e.to_string()))?,
                (Some(_), None) => {
                    return Err(Error::from_reason("permission rule field requires pattern"))
                }
                _ => base,
            };
            parsed.push(rule);
        }
    }
    Ok(PermissionEngine::with_rules(mode, parsed))
}

/// `query(prompt, tools?, { model?, permissions? })` → a `QueryStream` async iterator of
/// stream-delta objects.
///
/// `tools` is an object mapping a tool name to a JS `async` function `(input) => Promise<string>`;
/// the model can call it and — unless a `permission` denies it — the loop awaits it back across
/// the FFI boundary (the tool-callback seam). A denied tool never runs; the model gets an error
/// tool-result instead. Uses the in-process `MockProvider`, so no API key is needed.
#[napi(ts_return_type = "QueryStream")]
pub fn query(
    env: Env,
    input: Value,
    tools: Option<HashMap<String, HostFunction<'_>>>,
    options: Option<QueryOptions>,
) -> Result<QueryStream> {
    let options = options.unwrap_or(QueryOptions {
        model: None,
        fallback_models: None,
        max_tokens: None,
        max_turns: None,
        provider_options: None,
        budget: None,
        retry: None,
        routing: None,
        compaction: None,
        permissions: None,
        default_mode: None,
        cancel_before_start: None,
    });

    // Build the tool specs (advertised to the model) and, for each JS function, a thread-safe
    // handle the agent loop can call from its worker thread.
    let mut tool_specs: Vec<ToolSpec> = Vec::new();
    let mut tsfns: HashMap<String, HostCallback> = HashMap::new();
    if let Some(tools) = tools {
        for (name, func) in tools {
            let tsfn = host_callback(func)?;
            tool_specs.push(ToolSpec {
                name: name.clone(),
                description: "tool".into(),
                input_schema: serde_json::json!({ "type": "object" }),
            });
            tsfns.insert(name, tsfn);
        }
    }

    let QueryOptions {
        model,
        fallback_models,
        max_tokens,
        max_turns,
        provider_options,
        budget,
        retry,
        routing,
        compaction,
        permissions,
        default_mode,
        cancel_before_start,
    } = options;
    let mode = permission_mode(default_mode.as_deref().unwrap_or("allow"))?;
    let governance = Governance::new(build_permissions(permissions, mode)?, HookDispatcher::new());
    let executor: Arc<dyn ToolExecutor> = if tsfns.is_empty() {
        Arc::new(NoTools)
    } else {
        Arc::new(NodeToolExecutor {
            tools: RwLock::new(tsfns),
        })
    };

    let mut run_options = build_agent_options(
        Some(RunOptions {
            model,
            fallback_models,
            max_tokens,
            max_turns,
            provider_options,
            budget,
            retry,
            routing,
            compaction,
            cancel_before_start,
        }),
        governance,
    )?;
    run_options.tools = tool_specs;
    let messages = node_model_input(&env, input)?;
    let run = within_runtime_if_available(|| {
        CoreClient::new(CoreAgent::from_process_env()).query_cancellable_messages_with_executor(
            messages,
            run_options,
            executor,
        )
    })
    .map_err(|error| node_agent_error(&env, error))?;
    Ok(query_stream(run))
}

/// Async iterator over the agent loop's canonical stream deltas. The "streaming out" seam.
///
/// Single-consumer: `index.js` wraps this into a `for await` iterator by calling [`next`] until
/// it returns `null`.
///
/// [`next`]: QueryStream::next
#[napi]
pub struct QueryStream {
    inner: Arc<TokioMutex<Option<CancellableRun>>>,
    cancellation: CancellationHandle,
    recorder: RunRecorder,
}

struct QueryEventStreamState {
    encoder: StreamEventEncoder,
    pending: VecDeque<StreamEvent>,
}

/// Versioned start/delta/end view over a canonical query stream.
#[napi]
pub struct QueryEventStream {
    inner: Arc<TokioMutex<Option<CancellableRun>>>,
    state: Arc<TokioMutex<QueryEventStreamState>>,
    cancellation: CancellationHandle,
    recorder: RunRecorder,
}

fn query_stream(run: CancellableRun) -> QueryStream {
    let cancellation = run.cancellation_handle();
    let recorder = run.recorder();
    QueryStream {
        inner: Arc::new(TokioMutex::new(Some(run))),
        cancellation,
        recorder,
    }
}

#[napi]
impl QueryStream {
    /// The next stream delta as a plain object, or `null` when the stream is exhausted.
    #[napi(ts_return_type = "Promise<any | null>")]
    pub async fn next(&self) -> Result<Option<Value>> {
        let inner = self.inner.clone();
        let mut guard = inner.try_lock().map_err(|_| {
            Error::from_reason(
                "QueryStream is single-consumer; concurrent or re-entrant next() is not supported",
            )
        })?;
        let next = match guard.as_mut() {
            Some(run) => run.next().await,
            None => None,
        };
        match next {
            Some(delta) => Ok(Some(
                serde_json::to_value(delta).map_err(|e| Error::from_reason(e.to_string()))?,
            )),
            None => Ok(None),
        }
    }

    /// Consume this run through the versioned event lifecycle. Legacy and v2 views are alternate
    /// single-consumer surfaces and cannot be polled concurrently.
    #[napi]
    pub fn events(&self, response_id: String) -> Result<QueryEventStream> {
        if response_id.trim().is_empty() {
            return Err(Error::from_reason("responseId must not be empty"));
        }
        Ok(QueryEventStream {
            inner: self.inner.clone(),
            state: Arc::new(TokioMutex::new(QueryEventStreamState {
                encoder: StreamEventEncoder::new(response_id),
                pending: VecDeque::new(),
            })),
            cancellation: self.cancellation.clone(),
            recorder: self.recorder.clone(),
        })
    }

    /// Request cooperative cancellation immediately. Call `close()` to await all finalizers.
    #[napi]
    pub fn cancel(&self) {
        self.cancellation.cancel();
    }

    #[napi(getter)]
    pub fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }

    /// Cancel, drain the core driver, and return the terminal canonical RunOutcome.
    #[napi(ts_return_type = "Promise<any>")]
    pub async fn close(&self) -> Result<Value> {
        self.cancellation.cancel();
        let run = self.inner.lock().await.take();
        let outcome = match run {
            Some(run) => run.cancel().await,
            None => self.recorder.outcome(),
        };
        serde_json::to_value(outcome).map_err(|error| Error::from_reason(error.to_string()))
    }

    /// Current recorder snapshot. It is terminal after exhaustion or `close()`.
    #[napi(ts_return_type = "any")]
    pub fn outcome(&self) -> Result<Value> {
        serde_json::to_value(self.recorder.outcome())
            .map_err(|error| Error::from_reason(error.to_string()))
    }
}

#[napi]
impl QueryEventStream {
    #[napi(ts_return_type = "Promise<any | null>")]
    pub async fn next(&self) -> Result<Option<Value>> {
        let mut state = self.state.try_lock().map_err(|_| {
            Error::from_reason(
                "QueryEventStream is single-consumer; concurrent next() is unsupported",
            )
        })?;
        if let Some(event) = state.pending.pop_front() {
            return serde_json::to_value(event)
                .map(Some)
                .map_err(|error| Error::from_reason(error.to_string()));
        }

        let delta = {
            let mut run = self.inner.try_lock().map_err(|_| {
                Error::from_reason("legacy and event stream views cannot be consumed concurrently")
            })?;
            match run.as_mut() {
                Some(run) => run.next().await,
                None => None,
            }
        };
        let Some(delta) = delta else {
            return Ok(None);
        };
        let encoded = state.encoder.push(delta);
        state.pending.extend(encoded);
        state
            .pending
            .pop_front()
            .map(serde_json::to_value)
            .transpose()
            .map_err(|error| Error::from_reason(error.to_string()))
    }

    #[napi]
    pub fn cancel(&self) {
        self.cancellation.cancel();
    }

    #[napi(getter)]
    pub fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }

    #[napi(ts_return_type = "Promise<any>")]
    pub async fn close(&self) -> Result<Value> {
        self.cancellation.cancel();
        let run = self.inner.lock().await.take();
        let outcome = match run {
            Some(run) => run.cancel().await,
            None => self.recorder.outcome(),
        };
        serde_json::to_value(outcome).map_err(|error| Error::from_reason(error.to_string()))
    }

    #[napi(ts_return_type = "any")]
    pub fn outcome(&self) -> Result<Value> {
        serde_json::to_value(self.recorder.outcome())
            .map_err(|error| Error::from_reason(error.to_string()))
    }
}

/// Pull-based, single-consumer async iterator over serialized core `ObjectStreamEvent`s. There is
/// no background producer task: cancelling a pending `next()` drops that poll and releases the
/// mutex, matching [`QueryStream`]'s cancellation and re-entrancy behaviour.
#[napi]
pub struct ObjectStream {
    inner: Arc<TokioMutex<CoreObjectStream>>,
}

#[napi]
impl ObjectStream {
    /// The next structured-output event, or `null` when a validated completion ends the stream.
    #[napi(ts_return_type = "Promise<any | null>")]
    pub async fn next(&self) -> Result<Option<Value>> {
        let inner = self.inner.clone();
        let mut guard = inner.try_lock().map_err(|_| {
            Error::from_reason(
                "ObjectStream is single-consumer; concurrent or re-entrant next() is not supported",
            )
        })?;
        match guard.next().await {
            Some(Ok(event)) => Ok(Some(
                serde_json::to_value(event)
                    .map_err(|error| Error::from_reason(error.to_string()))?,
            )),
            Some(Err(error)) => Err(encoded_agent_error(AgentError::Core(error))),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn durable_binding_round_trips_replay_validated_state_and_evaluates_trace() {
        let mut run = DurableRun::new("session-sdk".into(), "run-sdk".into(), None).unwrap();
        run.replace_state("turn-1".into(), serde_json::json!({"answer": 42}))
            .unwrap();
        let state = run.snapshot().unwrap();
        let restored = DurableRun::from_state(state.clone()).unwrap();
        assert_eq!(restored.status().unwrap(), "running");
        assert_eq!(restored.snapshot().unwrap(), state);

        let result = evaluate_trace(
            serde_json::json!({
                "schema_version": 1,
                "name": "binding-durability",
                "assertions": [
                    {"type": "durable_sequence_monotonic"},
                    {"type": "run_status", "status": "running"}
                ]
            }),
            serde_json::json!({
                "durable_events": state["events"].clone(),
                "run_status": "running"
            }),
        )
        .unwrap();
        assert_eq!(result["passed"], true);

        let mut tampered = state;
        tampered["projection"]["status"] = serde_json::json!("completed");
        assert!(DurableRun::from_state(tampered).is_err());
    }

    #[test]
    fn mcp_tool_filter_options_fail_closed_and_share_core_validation() {
        let filter = node_mcp_tool_filter(Some(serde_json::json!({
            "allow": ["read_file", "write_file"],
            "deny": ["write_file"]
        })))
        .unwrap();
        assert!(filter.allows("read_file"));
        assert!(!filter.allows("write_file"));

        assert!(node_mcp_tool_filter(Some(serde_json::json!({
            "deny": ["duplicate", "duplicate"]
        })))
        .is_err());
        assert!(node_mcp_tool_filter(Some(serde_json::json!({
            "allow": ["read_file"],
            "unexpected": []
        })))
        .is_err());
    }

    struct ParsedApprover {
        decision: ApprovalDecision,
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl ToolApprover for ParsedApprover {
        async fn approve(&self, _request: ApprovalRequest) -> ApprovalDecision {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.decision.clone()
        }
    }

    async fn assert_reusable_update(scope: &str, second_input: Value) {
        let decision = parse_approval(serde_json::json!({
            "decision": "allow",
            "updated_permissions": [scope],
        }))
        .unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let governance = Governance::new(
            PermissionEngine::with_rules(
                PermissionMode::Allow,
                vec![
                    Rule::deny("search")
                        .matching("blocked")
                        .unwrap()
                        .named("static-deny"),
                    Rule::ask("search").named("ask-search"),
                ],
            ),
            HookDispatcher::new(),
        )
        .with_approver(Arc::new(ParsedApprover {
            decision,
            calls: calls.clone(),
        }));

        for (turn, input) in [(1, serde_json::json!({"q": "same"})), (2, second_input)] {
            let report = governance
                .authorize_detailed_with_context(aikit_core::AuthorizationContext {
                    run_id: "run".into(),
                    turn,
                    tool_use_id: format!("call-{turn}"),
                    tool: "search".into(),
                    input,
                })
                .await;
            assert!(matches!(
                report.authorization,
                aikit_core::Authorization::Allowed(_)
            ));
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let denied = governance
            .authorize_detailed_with_context(aikit_core::AuthorizationContext {
                run_id: "run".into(),
                turn: 3,
                tool_use_id: "call-3".into(),
                tool: "search".into(),
                input: serde_json::json!({"q": "blocked"}),
            })
            .await;
        assert!(matches!(
            denied.authorization,
            aikit_core::Authorization::Denied { .. }
        ));
        assert_eq!(denied.permission_source, "static-deny");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn approval_abi_reuses_only_safe_scopes_and_static_deny_wins() {
        assert!(matches!(
            parse_approval(Value::String("allow".into())),
            Ok(ApprovalDecision::Allow {
                updated_input: None,
                ref updated_permissions,
            }) if updated_permissions.is_empty()
        ));
        assert!(matches!(
            parse_approval(Value::String("deny".into())),
            Ok(ApprovalDecision::Deny {
                interrupt: false,
                ..
            })
        ));
        assert_reusable_update("allow_exact_input", serde_json::json!({"q": "same"})).await;
        assert_reusable_update("allow_tool", serde_json::json!({"q": "different"})).await;
        assert!(parse_approval(serde_json::json!({
            "decision": "allow",
            "updated_permissions": ["all_tools_forever"]
        }))
        .is_err());
        assert!(matches!(
            parse_approval(serde_json::json!({
                "decision": "deny",
                "interrupt": true
            })),
            Ok(ApprovalDecision::Deny {
                interrupt: true,
                ..
            })
        ));
    }

    #[test]
    fn run_options_map_every_shared_core_control() {
        let options = build_agent_options(
            Some(RunOptions {
                model: Some("primary".into()),
                fallback_models: Some(vec!["fallback-a".into(), "fallback-b".into()]),
                max_tokens: Some(321),
                max_turns: Some(7),
                provider_options: Some(serde_json::json!({
                    "openai": {"temperature": 0}
                })),
                budget: Some(serde_json::json!({
                    "maxTotalTokens": 999,
                    "maxCostUsd": 1.25,
                    "pricing": {
                        "inputPerMillionUsd": 2.0,
                        "outputPerMillionUsd": 4.0
                    }
                })),
                retry: Some(serde_json::json!({
                    "maxAttemptsPerModel": 3,
                    "baseDelayMs": 10,
                    "maxDelayMs": 20,
                    "perAttemptTimeoutMs": 30
                })),
                routing: None,
                compaction: Some(serde_json::json!({
                    "maxContextTokens": 2_000,
                    "keepRecentMessages": 6
                })),
                cancel_before_start: Some(true),
            }),
            Governance::default(),
        )
        .unwrap();
        assert_eq!(options.model, "primary");
        assert_eq!(options.fallback_models, ["fallback-a", "fallback-b"]);
        assert_eq!(options.max_tokens, 321);
        assert_eq!(options.max_turns, 7);
        assert_eq!(options.provider_options["openai"]["temperature"], 0);
        assert_eq!(options.budget.max_total_tokens, Some(999));
        assert_eq!(options.budget.max_cost_usd, Some(1.25));
        assert_eq!(options.budget.pricing.unwrap().output_per_million_usd, 4.0);
        assert_eq!(options.retry.max_attempts_per_model, 3);
        assert_eq!(options.retry.per_attempt_timeout_ms, 30);
        assert_eq!(options.compaction.max_context_tokens, 2_000);
        assert_eq!(options.compaction.keep_recent_messages, 6);
        assert!(options.cancellation.is_cancelled());
    }

    #[test]
    fn run_options_reject_unknown_nested_cost_and_reliability_fields() {
        for (budget, retry, compaction, field) in [
            (
                Some(serde_json::json!({"maxTotalTokenz": 0})),
                None,
                None,
                "maxTotalTokenz",
            ),
            (
                Some(serde_json::json!({"pricing": {
                    "inputPerMillionUsd": 1.0,
                    "outputPerMillionUsd": 2.0,
                    "cacheReadPerMillion": 0.5
                }})),
                None,
                None,
                "cacheReadPerMillion",
            ),
            (
                None,
                Some(serde_json::json!({"maxAttemptsPerModal": 1})),
                None,
                "maxAttemptsPerModal",
            ),
            (
                None,
                None,
                Some(serde_json::json!({
                    "maxContextTokens": 100,
                    "keepRecentMessagez": 2
                })),
                "keepRecentMessagez",
            ),
        ] {
            let error = build_agent_options(
                Some(RunOptions {
                    budget,
                    retry,
                    compaction,
                    ..RunOptions::default()
                }),
                Governance::default(),
            )
            .err()
            .expect("invalid options must fail closed");
            assert!(
                error.to_string().contains(field),
                "unexpected error: {error}"
            );
        }
    }

    #[test]
    fn run_options_reject_every_duplicate_nested_alias_even_when_values_match() {
        let cases = vec![
            (
                RunOptions {
                    budget: Some(serde_json::json!({
                        "max_total_tokens": 100,
                        "maxTotalTokens": 100
                    })),
                    ..RunOptions::default()
                },
                "max_total_tokens",
                "maxTotalTokens",
            ),
            (
                RunOptions {
                    budget: Some(serde_json::json!({
                        "max_cost_usd": 1.0,
                        "maxCostUsd": 1.0
                    })),
                    ..RunOptions::default()
                },
                "max_cost_usd",
                "maxCostUsd",
            ),
            (
                RunOptions {
                    budget: Some(serde_json::json!({"pricing": {
                        "input_per_million_usd": 1.0,
                        "inputPerMillionUsd": 1.0,
                        "outputPerMillionUsd": 2.0
                    }})),
                    ..RunOptions::default()
                },
                "input_per_million_usd",
                "inputPerMillionUsd",
            ),
            (
                RunOptions {
                    budget: Some(serde_json::json!({"pricing": {
                        "inputPerMillionUsd": 1.0,
                        "output_per_million_usd": 2.0,
                        "outputPerMillionUsd": 2.0
                    }})),
                    ..RunOptions::default()
                },
                "output_per_million_usd",
                "outputPerMillionUsd",
            ),
            (
                RunOptions {
                    budget: Some(serde_json::json!({"pricing": {
                        "inputPerMillionUsd": 1.0,
                        "outputPerMillionUsd": 2.0,
                        "cache_read_per_million_usd": 0.5,
                        "cacheReadPerMillionUsd": 0.5
                    }})),
                    ..RunOptions::default()
                },
                "cache_read_per_million_usd",
                "cacheReadPerMillionUsd",
            ),
            (
                RunOptions {
                    budget: Some(serde_json::json!({"pricing": {
                        "inputPerMillionUsd": 1.0,
                        "outputPerMillionUsd": 2.0,
                        "cache_write_per_million_usd": 0.5,
                        "cacheWritePerMillionUsd": 0.5
                    }})),
                    ..RunOptions::default()
                },
                "cache_write_per_million_usd",
                "cacheWritePerMillionUsd",
            ),
            (
                RunOptions {
                    retry: Some(serde_json::json!({
                        "max_attempts_per_model": 2,
                        "maxAttemptsPerModel": 2
                    })),
                    ..RunOptions::default()
                },
                "max_attempts_per_model",
                "maxAttemptsPerModel",
            ),
            (
                RunOptions {
                    retry: Some(serde_json::json!({
                        "base_delay_ms": 250,
                        "baseDelayMs": 250
                    })),
                    ..RunOptions::default()
                },
                "base_delay_ms",
                "baseDelayMs",
            ),
            (
                RunOptions {
                    retry: Some(serde_json::json!({
                        "max_delay_ms": 4_000,
                        "maxDelayMs": 4_000
                    })),
                    ..RunOptions::default()
                },
                "max_delay_ms",
                "maxDelayMs",
            ),
            (
                RunOptions {
                    retry: Some(serde_json::json!({
                        "per_attempt_timeout_ms": 30_000,
                        "perAttemptTimeoutMs": 30_000
                    })),
                    ..RunOptions::default()
                },
                "per_attempt_timeout_ms",
                "perAttemptTimeoutMs",
            ),
            (
                RunOptions {
                    compaction: Some(serde_json::json!({
                        "max_context_tokens": 4_096,
                        "maxContextTokens": 4_096
                    })),
                    ..RunOptions::default()
                },
                "max_context_tokens",
                "maxContextTokens",
            ),
            (
                RunOptions {
                    compaction: Some(serde_json::json!({
                        "maxContextTokens": 4_096,
                        "keep_recent_messages": 8,
                        "keepRecentMessages": 8
                    })),
                    ..RunOptions::default()
                },
                "keep_recent_messages",
                "keepRecentMessages",
            ),
        ];

        for (options, snake, camel) in cases {
            let error = build_agent_options(Some(options), Governance::default())
                .err()
                .expect("duplicate aliases must fail closed");
            let message = error.to_string();
            assert!(
                message.contains("duplicate aliases")
                    && message.contains(snake)
                    && message.contains(camel),
                "unexpected error for {snake}/{camel}: {message}"
            );
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn binding_builtin_suite_is_multi_root_jailed_and_bash_is_explicitly_contained() {
        use std::os::unix::fs::symlink;

        let primary = tempfile::tempdir().unwrap();
        let secondary = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::fs::write(outside.path().join("secret.txt"), "outside").unwrap();
        symlink(
            outside.path().join("secret.txt"),
            primary.path().join("escape-link"),
        )
        .unwrap();

        let sandbox = Sandbox::with_roots(vec![
            primary.path().to_path_buf(),
            secondary.path().to_path_buf(),
        ])
        .unwrap();
        let file_tools = BuiltinTools::new(sandbox.clone());
        assert_eq!(
            file_tools.tool_names(),
            ["Read", "Write", "Edit", "Grep", "Glob"]
        );
        file_tools
            .execute(
                "Write",
                serde_json::json!({"path": "roundtrip.txt", "content": "primary"}),
            )
            .await
            .unwrap();
        assert_eq!(
            file_tools
                .execute("Read", serde_json::json!({"path": "roundtrip.txt"}))
                .await
                .unwrap(),
            "primary"
        );
        let secondary_file = secondary.path().join("secondary.txt");
        file_tools
            .execute(
                "Write",
                serde_json::json!({"path": secondary_file, "content": "secondary"}),
            )
            .await
            .unwrap();
        assert_eq!(
            file_tools
                .execute("Read", serde_json::json!({"path": secondary_file}))
                .await
                .unwrap(),
            "secondary"
        );

        for forbidden in [
            outside.path().join("secret.txt"),
            primary.path().join("escape-link"),
        ] {
            assert!(matches!(
                file_tools
                    .execute("Read", serde_json::json!({"path": forbidden}))
                    .await,
                Err(aikit_core::AikitError::Sandbox(_))
            ));
        }
        assert!(file_tools
            .execute("Bash", serde_json::json!({"command": "echo hidden"}))
            .await
            .is_err());

        let contained =
            BuiltinTools::new(sandbox).with_containment_policy(ContainmentPolicy::required_auto());
        let report = contained.containment_capabilities().await;
        assert_eq!(
            report.requirement,
            aikit_core::ContainmentRequirement::Required(aikit_core::BackendSelector::Auto)
        );
        assert!(report.fail_closed);
        assert_ne!(
            report.selected_backend,
            Some(aikit_core::ActiveContainmentBackend::Uncontained)
        );
        let bash = contained
            .execute(
                "Bash",
                serde_json::json!({"command": "printf binding-contained"}),
            )
            .await;
        if report.selected_backend.is_some() {
            assert!(bash.unwrap().contains("binding-contained"));
        } else {
            assert!(bash.is_err(), "Bash ran without a required backend");
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn docker_fallback_options_preserve_safe_limits_and_core_rejects_unpinned_images() {
        let root = tempfile::tempdir().unwrap();
        let pinned = format!("example/aikit@sha256:{}", "a".repeat(64));
        let missing_docker = root.path().join("missing-docker");
        let policy = required_auto_containment(Some(DockerContainmentOptions {
            image: pinned,
            executable: Some(missing_docker.to_string_lossy().into_owned()),
            pids_limit: Some(17),
            memory_mib: Some(256),
            cpus: Some(2),
            tmpfs_mib: Some(32),
        }));
        assert_eq!(
            policy.requirement,
            aikit_core::ContainmentRequirement::Required(aikit_core::BackendSelector::Auto)
        );
        let config = policy.docker.as_ref().unwrap();
        assert_eq!(config.pids_limit, 17);
        assert_eq!(config.memory_bytes, 256 << 20);
        assert_eq!(config.cpus, 2);
        assert_eq!(config.tmpfs_bytes, 32 << 20);

        let pinned_report = BuiltinTools::new(Sandbox::jail(root.path()).unwrap())
            .with_containment_policy(policy)
            .containment_capabilities()
            .await;
        let docker = pinned_report
            .backends
            .iter()
            .find(|backend| backend.backend == aikit_core::ActiveContainmentBackend::Docker)
            .unwrap();
        assert!(!docker.available);
        assert!(!docker.detail.contains("must be pinned"));

        let unpinned_report = BuiltinTools::new(Sandbox::jail(root.path()).unwrap())
            .with_containment_policy(required_auto_containment(Some(DockerContainmentOptions {
                image: "alpine:latest".into(),
                executable: None,
                pids_limit: None,
                memory_mib: None,
                cpus: None,
                tmpfs_mib: None,
            })))
            .containment_capabilities()
            .await;
        let docker = unpinned_report
            .backends
            .iter()
            .find(|backend| backend.backend == aikit_core::ActiveContainmentBackend::Docker)
            .unwrap();
        assert!(!docker.available);
        assert!(docker.detail.contains("must be pinned"));
    }
}
