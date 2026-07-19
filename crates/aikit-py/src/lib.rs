//! Python (PyO3) binding over the shared `aikit-runtime-core` runtime.
//!
//! Keeps provider selection, routing, memory, orchestration, governance, and structured output in
//! Rust while exposing Python-native awaitables and async iterators. The two cross-language seams
//! remain explicit:
//!
//!   1. **Streaming out**: the Rust `tokio` agent loop's `StreamDelta`s surface in Python as a
//!      native `async for` iterator (`QueryStream`).
//!   2. **Tool callback in**: when the loop hits a tool call, it awaits a *Python* `async def`
//!      from Rust (`PyToolExecutor`), across the FFI boundary, and resumes with its result.
//!
//! No Python GIL is held while Rust awaits a provider or a Python coroutine.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use aikit_core::orchestration::{
    ExecutionContext, ModelRouteRequirements, Orchestrator, SubagentSpec,
};
use aikit_core::{
    evaluate_outcome as core_evaluate_outcome, request_capability_tool, tools::ToolExecutor, Agent,
    AgentError, AgentOptions as CoreAgentOptions, ApprovalDecision, ApprovalRequest,
    AuditFailureMode, AuditPayloadPolicy, AuditTrail, BrowserEgressPolicy, BrowserTools,
    BudgetLedger, BudgetLimits, BudgetPolicy, BuiltinTools, CancellableRun, CancellationHandle,
    CapabilityBroker, CapabilityGate, Client as CoreClient, CompactionPolicy, ContainmentPolicy,
    DockerConfig, EvalGate, FailureContext, FailureHookOutcome, GeneratedText, Governance,
    GuardedExecutor, GuardrailChain, HookDispatcher, HookMatcher, HookOutcome,
    InMemorySessionStore, JsonFileMemoryStore, JsonFileSessionStore, JsonlAuditSink, McpClient,
    McpToolExecutor, McpToolFilter, Message, ModelCatalog, ModelPricing, ModelProfile,
    ObjectOptions, ObjectStream as CoreObjectStream, ObjectStreamEvent, PermissionEngine,
    PermissionMode, PermissionUpdate, PiiRedactor, PostToolOutcome, PostToolUseContext,
    PreToolUseContext, PromptContext, PromptHookOutcome, ProviderOptions, RegexBlocklist,
    RetryPolicy, RouteRequest, RoutingOptions, Rule, RunOutcome, RunRecorder, RunTerminalStatus,
    Sandbox, SecretRedactor, SemanticValidation, SemanticValidator, Session, SessionStore,
    SessionStoreError, SqliteMemoryStore, SqliteSessionStore, StdioTransport, StopContext,
    StreamDelta, StreamableHttpTransport, ToolApprover, ToolRouter, ToolSpec, WebTools,
};
use async_trait::async_trait;
use futures::StreamExt;
use pyo3::exceptions::{PyRuntimeError, PyStopAsyncIteration, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use serde::Deserialize;
use serde_json::Value;
use tokio::sync::Mutex as TokioMutex;

pyo3::create_exception!(aikit, AikitError, PyRuntimeError);

type CallbackLocals = Arc<RwLock<Option<Arc<pyo3_async_runtimes::TaskLocals>>>>;

fn python_mcp_tool_filter(value: Option<Bound<'_, PyAny>>) -> PyResult<McpToolFilter> {
    let Some(value) = value else {
        return Ok(McpToolFilter::default());
    };
    let value: Value =
        pythonize::depythonize(&value).map_err(|error| PyValueError::new_err(error.to_string()))?;
    McpToolFilter::from_value(value).map_err(|error| PyValueError::new_err(error.to_string()))
}

/// The canonical core types intentionally remain backward-compatible for persisted sessions, so
/// their serde implementations accept some future fields. A public evaluation boundary must be
/// stricter: reject unknown host input before deserializing through those same core types.
fn validate_eval_outcome_shape(value: &Value) -> Result<(), String> {
    fn reject_unknown(value: &Value, allowed: &[&str], context: &str) -> Result<(), String> {
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

fn parse_eval_outcome(value: Value) -> PyResult<RunOutcome> {
    validate_eval_outcome_shape(&value).map_err(PyValueError::new_err)?;
    serde_json::from_value(value)
        .map_err(|_| PyValueError::new_err("invalid canonical RunOutcome structure"))
}

fn validate_eval_gate_shapes(value: &Value) -> PyResult<()> {
    let gates = value
        .as_array()
        .ok_or_else(|| PyValueError::new_err("eval gates must be a sequence"))?;
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
            return Err(PyValueError::new_err(format!(
                "eval gates[{index}] contains an unknown field"
            )));
        }
    }
    Ok(())
}

/// Evaluate a canonical recorded outcome with deterministic gates. This function performs no
/// provider, tool, filesystem, or network work.
#[pyfunction]
fn evaluate_outcome<'py>(
    py: Python<'py>,
    outcome: Bound<'_, PyAny>,
    gates: Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let outcome: Value = pythonize::depythonize(&outcome)
        .map_err(|_| PyValueError::new_err("invalid canonical RunOutcome structure"))?;
    let gates: Value = pythonize::depythonize(&gates)
        .map_err(|_| PyValueError::new_err("invalid eval gate sequence"))?;
    let outcome = parse_eval_outcome(outcome)?;
    validate_eval_gate_shapes(&gates)?;
    let gates: Vec<EvalGate> = serde_json::from_value(gates)
        .map_err(|_| PyValueError::new_err("invalid eval gate sequence"))?;
    let verdict = core_evaluate_outcome(&outcome, &gates)
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    pythonize::pythonize(py, &verdict)
        .map(Bound::unbind)
        .map_err(|_| PyRuntimeError::new_err("failed to encode EvalVerdict"))
}

#[pyclass(name = "McpServer")]
struct PyMcpServer {
    specs: Vec<ToolSpec>,
    executor: Arc<McpToolExecutor>,
    client: Arc<McpClient>,
}

#[pymethods]
impl PyMcpServer {
    fn list_resources<'py>(
        &self,
        py: Python<'py>,
        cursor: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let value = client
                .list_resources(cursor.as_deref())
                .await
                .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
            Python::attach(|py| {
                pythonize::pythonize(py, &value)
                    .map(|v| v.unbind())
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))
            })
        })
    }

    fn read_resource<'py>(&self, py: Python<'py>, uri: String) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let value = client
                .read_resource(&uri)
                .await
                .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
            Python::attach(|py| {
                pythonize::pythonize(py, &value)
                    .map(|v| v.unbind())
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))
            })
        })
    }

    fn list_prompts<'py>(
        &self,
        py: Python<'py>,
        cursor: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let value = client
                .list_prompts(cursor.as_deref())
                .await
                .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
            Python::attach(|py| {
                pythonize::pythonize(py, &value)
                    .map(|v| v.unbind())
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))
            })
        })
    }

    fn get_prompt<'py>(
        &self,
        py: Python<'py>,
        name: String,
        arguments: Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let arguments: Value =
            pythonize::depythonize(&arguments).map_err(|e| PyValueError::new_err(e.to_string()))?;
        let client = self.client.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let value = client
                .get_prompt(&name, arguments)
                .await
                .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
            Python::attach(|py| {
                pythonize::pythonize(py, &value)
                    .map(|v| v.unbind())
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))
            })
        })
    }
}

#[pyfunction]
#[pyo3(signature = (endpoint, name, bearer_token=None, tool_filter=None))]
fn connect_mcp_http<'py>(
    py: Python<'py>,
    endpoint: String,
    name: String,
    bearer_token: Option<String>,
    tool_filter: Option<Bound<'_, PyAny>>,
) -> PyResult<Bound<'py, PyAny>> {
    let tool_filter = python_mcp_tool_filter(tool_filter)?;
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let transport = Arc::new(
            StreamableHttpTransport::new(&endpoint, bearer_token)
                .map_err(|error| PyValueError::new_err(error.to_string()))?,
        );
        let mut client = McpClient::new_with_tool_filter(transport, name, tool_filter);
        client
            .initialize()
            .await
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
        let specs = client
            .list_tools()
            .await
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
        let client = Arc::new(client);
        Python::attach(|py| {
            Py::new(
                py,
                PyMcpServer {
                    specs,
                    executor: Arc::new(McpToolExecutor::new(vec![client.clone()])),
                    client,
                },
            )
        })
    })
}

#[pyfunction]
#[pyo3(signature = (program, args, name, env=None, inherit_env=false, tool_filter=None))]
fn connect_mcp_stdio<'py>(
    py: Python<'py>,
    program: String,
    args: Vec<String>,
    name: String,
    env: Option<HashMap<String, String>>,
    inherit_env: bool,
    tool_filter: Option<Bound<'_, PyAny>>,
) -> PyResult<Bound<'py, PyAny>> {
    let tool_filter = python_mcp_tool_filter(tool_filter)?;
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let env = env.unwrap_or_default().into_iter().collect();
        let transport = Arc::new(
            StdioTransport::spawn_with_env(&program, &args, &env, inherit_env)
                .await
                .map_err(|error| PyRuntimeError::new_err(error.to_string()))?,
        );
        let mut client = McpClient::new_with_tool_filter(transport, name, tool_filter);
        client
            .initialize()
            .await
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
        let specs = client
            .list_tools()
            .await
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
        let client = Arc::new(client);
        Python::attach(|py| {
            Py::new(
                py,
                PyMcpServer {
                    specs,
                    executor: Arc::new(McpToolExecutor::new(vec![client.clone()])),
                    client,
                },
            )
        })
    })
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PyDockerContainmentOptions {
    image: String,
    executable: Option<String>,
    pids_limit: Option<u32>,
    memory_mib: Option<u32>,
    cpus: Option<u32>,
    tmpfs_mib: Option<u32>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PyRoutingOptions {
    profiles: Vec<ModelProfile>,
    request: RouteRequest,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PyPermissionRuleSpec {
    id: Option<String>,
    effect: String,
    tool: String,
    pattern: Option<String>,
    field: Option<String>,
}

fn required_auto_containment(docker: Option<PyDockerContainmentOptions>) -> ContainmentPolicy {
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

struct PyHostCallback {
    callable: Arc<Py<PyAny>>,
    locals: CallbackLocals,
}

/// Private callable returned by `tool(...)` when used as a decorator factory. The decorated
/// Python function remains the actual definition object, preserving normal async-call behavior.
#[pyclass(name = "_ToolDecorator", module = "aikit")]
struct PyToolDecorator {
    name: String,
    description: String,
    input_schema: Value,
}

fn decorate_tool(
    py: Python<'_>,
    callback: &Bound<'_, PyAny>,
    name: &str,
    description: &str,
    input_schema: &Value,
) -> PyResult<()> {
    if !callback.is_callable() {
        return Err(PyValueError::new_err("tool callback must be callable"));
    }
    callback.setattr("name", name)?;
    callback.setattr("description", description)?;
    let schema = pythonize::pythonize(py, input_schema)
        .map_err(|error| PyValueError::new_err(format!("invalid tool input_schema: {error}")))?;
    callback.setattr("input_schema", schema)?;
    Ok(())
}

#[pymethods]
impl PyToolDecorator {
    fn __call__<'py>(
        &self,
        py: Python<'py>,
        callback: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        decorate_tool(
            py,
            &callback,
            &self.name,
            &self.description,
            &self.input_schema,
        )?;
        Ok(callback)
    }
}

/// A [`ToolExecutor`] that runs Python `async def` tools. This is the "tool callback in" seam.
#[derive(Default)]
struct PyToolExecutor {
    /// tool name -> Python async callable taking a dict and returning a str.
    tools: RwLock<HashMap<String, Arc<PyHostCallback>>>,
}

/// Binding-local composite: canonical built-ins dispatch to the exact registered core suite;
/// every other name dispatches to the Python host-callback registry. Registration rejects name
/// collisions, so this routing order can never shadow a host tool.
struct PyAgentToolExecutor {
    host: Arc<PyToolExecutor>,
    builtins: Option<Arc<BuiltinTools>>,
    external: Arc<ToolRouter>,
}

#[async_trait]
impl ToolExecutor for PyAgentToolExecutor {
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

/// Call one Python async callback without holding the GIL across its await and convert the result
/// through JSON-compatible values. This single seam drives tools, approvals, and all hooks.
async fn call_python(callback: Arc<PyHostCallback>, payload: Value) -> Result<Value, String> {
    let locals = callback
        .locals
        .read()
        .map_err(|_| "Python callback context lock poisoned".to_string())?
        .clone()
        .ok_or_else(|| {
            "Python callback requires Agent.run/query to start inside an active asyncio loop"
                .to_string()
        })?;
    let future = Python::attach(|py| -> PyResult<_> {
        let input = pythonize::pythonize(py, &payload)
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
        let coroutine = callback.callable.bind(py).call1((input,))?;
        pyo3_async_runtimes::into_future_with_locals(locals.as_ref(), coroutine)
    })
    .map_err(|error| error.to_string())?;

    let result = future.await.map_err(|error| error.to_string())?;
    Python::attach(|py| pythonize::depythonize(result.bind(py)).map_err(|error| error.to_string()))
}

#[async_trait]
impl ToolExecutor for PyToolExecutor {
    async fn execute(&self, name: &str, input: Value) -> aikit_core::Result<String> {
        let callback = self
            .tools
            .read()
            .map_err(|_| aikit_core::AikitError::ToolExecution("tool registry poisoned".into()))?
            .get(name)
            .cloned()
            .ok_or_else(|| {
                aikit_core::AikitError::ToolExecution(format!("unknown tool '{name}'"))
            })?;
        match call_python(callback, input).await {
            Ok(Value::String(output)) => Ok(output),
            Ok(_) => Err(aikit_core::AikitError::ToolExecution(format!(
                "Python tool '{name}' must resolve to str"
            ))),
            Err(error) => Err(aikit_core::AikitError::ToolExecution(error)),
        }
    }
}

struct PyToolApprover {
    callback: Arc<PyHostCallback>,
}

#[async_trait]
impl ToolApprover for PyToolApprover {
    async fn approve(&self, request: ApprovalRequest) -> ApprovalDecision {
        let payload = serde_json::json!({
            "run_id": request.run_id,
            "turn": request.turn,
            "tool_use_id": request.tool_use_id,
            "tool": request.tool,
            "input": request.input,
        });
        match call_python(self.callback.clone(), payload).await {
            Ok(Value::Bool(true)) => ApprovalDecision::Allow {
                updated_input: None,
                updated_permissions: Vec::new(),
            },
            Ok(Value::Bool(false)) => ApprovalDecision::Deny {
                message: "tool use denied by Python approver".into(),
                interrupt: false,
            },
            Ok(value) => parse_approval(value).unwrap_or_else(|message| ApprovalDecision::Deny {
                message,
                interrupt: false,
            }),
            Err(error) => ApprovalDecision::Deny {
                message: format!("Python approver failed: {error}"),
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

fn parse_semantic_validation(value: Value) -> Result<SemanticValidation, String> {
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
                "semantic validator must return 'accept' or an exact action object with retry/reject and reason"
                    .into(),
            ),
        },
        _ => Err(
            "semantic validator must return 'accept' or an exact action object with retry/reject and reason"
                .into(),
        ),
    }
}

struct PySemanticValidator {
    callback: Arc<PyHostCallback>,
}

#[async_trait]
impl SemanticValidator for PySemanticValidator {
    async fn validate(&self, value: Value) -> std::result::Result<SemanticValidation, String> {
        parse_semantic_validation(call_python(self.callback.clone(), value).await?)
    }
}

fn parse_approval(value: Value) -> Result<ApprovalDecision, String> {
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
                .unwrap_or("tool use denied by Python approver")
                .to_string(),
            interrupt: optional_bool(&value, "interrupt")?.unwrap_or(false),
        }),
        _ => Err(
            "Python approver returned an invalid decision; expected bool or {decision: allow|deny}"
                .into(),
        ),
    }
}

fn parse_permission_updates(value: &Value) -> Result<Vec<PermissionUpdate>, String> {
    let Some(updates) = value.get("updated_permissions") else {
        return Ok(Vec::new());
    };
    if updates.is_null() {
        return Ok(Vec::new());
    }
    let updates = updates.as_array().ok_or_else(|| {
        "Python approver updated_permissions must be an array of allow_exact_input|allow_tool"
            .to_string()
    })?;
    updates
        .iter()
        .map(|update| match update.as_str() {
            Some("allow_exact_input") => Ok(PermissionUpdate::AllowExactInput),
            Some("allow_tool") => Ok(PermissionUpdate::AllowTool),
            _ => Err(
                "Python approver updated_permissions contains an unsafe value; expected allow_exact_input|allow_tool"
                    .to_string(),
            ),
        })
        .collect()
}

fn optional_bool(value: &Value, field: &str) -> Result<Option<bool>, String> {
    match value.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Bool(value)) => Ok(Some(*value)),
        Some(_) => Err(format!("Python approver {field} must be a bool")),
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

fn host_callback(
    callback: Bound<'_, PyAny>,
    locals: CallbackLocals,
) -> PyResult<Arc<PyHostCallback>> {
    if !callback.is_callable() {
        return Err(PyValueError::new_err("callback must be callable"));
    }
    Ok(Arc::new(PyHostCallback {
        callable: Arc::new(callback.unbind()),
        locals,
    }))
}

fn capture_callback_locals(py: Python<'_>, locals: &CallbackLocals) -> PyResult<()> {
    let task_locals = pyo3_async_runtimes::tokio::get_current_locals(py).map_err(|error| {
        PyRuntimeError::new_err(format!(
            "cancellable runs must start inside an active asyncio loop: {error}"
        ))
    })?;
    *locals
        .write()
        .map_err(|_| PyRuntimeError::new_err("Python callback context lock poisoned"))? =
        Some(Arc::new(task_locals));
    Ok(())
}

fn callback_error(stage: &str, error: String) -> String {
    format!("Python {stage} callback failed: {error}")
}

/// Accept the compatibility string prompt or an exact canonical message sequence. Deserializing
/// through the shared core types keeps the Python boundary closed and typed: malformed roles,
/// content blocks, media sources, or unknown variants fail before any provider call starts.
fn python_messages(input: &Bound<'_, PyAny>) -> PyResult<Vec<Message>> {
    if let Ok(prompt) = input.extract::<String>() {
        return Ok(vec![Message::user(prompt)]);
    }

    let messages: Vec<Message> = pythonize::depythonize(input).map_err(|error| {
        PyValueError::new_err(format!(
            "prompt must be a string or a canonical message sequence: {error}"
        ))
    })?;
    if messages.is_empty() {
        return Err(PyValueError::new_err(
            "canonical message sequence must not be empty",
        ));
    }
    Ok(messages)
}

fn py_error_with_info(message: String, info: aikit_core::ErrorInfo) -> PyErr {
    Python::attach(|py| {
        let exception = AikitError::new_err(message);
        if let Ok(value) = pythonize::pythonize(py, &info) {
            let _ = exception.value(py).setattr("info", value);
        }
        if let Ok(Value::String(code)) = serde_json::to_value(info.code) {
            let _ = exception.value(py).setattr("code", code);
        }
        exception
    })
}

/// Preserve the core's stable, redacted classification on host exceptions. Callers can branch on
/// `exc.code` / `exc.info` without parsing the compatibility display message.
fn py_agent_error(error: AgentError) -> PyErr {
    let info = error.info();
    py_error_with_info(error.to_string(), info)
}

fn py_core_error(error: aikit_core::AikitError) -> PyErr {
    let info = error.info();
    py_error_with_info(error.to_string(), info)
}

/// Python-facing async iterator over the agent loop's stream. This is the "streaming out" seam.
///
/// Single-consumer: it is not safe to drive `__anext__` concurrently or re-entrantly. Doing so
/// raises a Python `RuntimeError` rather than deadlocking on the inner mutex.
#[pyclass]
struct QueryStream {
    inner: Arc<TokioMutex<Option<CancellableRun>>>,
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

async fn close_query_stream(
    inner: Arc<TokioMutex<Option<CancellableRun>>>,
    recorder: RunRecorder,
) -> aikit_core::RunOutcome {
    match inner.lock().await.take() {
        Some(run) => run.cancel().await,
        None => recorder.outcome(),
    }
}

/// Python-facing async iterator over incremental structured-output events. Like [`QueryStream`],
/// this is deliberately pull-based and single-consumer: cancelling an `__anext__` future drops the
/// in-flight poll without leaving a producer task or a permanently-held lock behind.
#[pyclass]
struct ObjectStream {
    inner: Arc<TokioMutex<CoreObjectStream>>,
    model_class: Option<Arc<Py<PyAny>>>,
}

fn object_event_to_python(
    py: Python<'_>,
    event: ObjectStreamEvent,
    model_class: Option<&Arc<Py<PyAny>>>,
) -> PyResult<Py<PyAny>> {
    match (event, model_class) {
        (ObjectStreamEvent::Completed { object }, Some(model_class)) => {
            let raw_value = pythonize::pythonize(py, &object.value)
                .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
            let typed_value = model_class
                .as_ref()
                .bind(py)
                .call_method1("model_validate", (raw_value,))?;
            let generated = PyDict::new(py);
            generated.set_item("value", typed_value)?;
            generated.set_item(
                "fidelity",
                pythonize::pythonize(py, &object.fidelity)
                    .map_err(|error| PyRuntimeError::new_err(error.to_string()))?,
            )?;
            generated.set_item("attempts", object.attempts)?;
            generated.set_item(
                "provider_metadata",
                pythonize::pythonize(py, &object.provider_metadata)
                    .map_err(|error| PyRuntimeError::new_err(error.to_string()))?,
            )?;
            let completed = PyDict::new(py);
            completed.set_item("type", "completed")?;
            completed.set_item("object", generated)?;
            Ok(completed.into_any().unbind())
        }
        (event, _) => pythonize::pythonize(py, &event)
            .map(Bound::unbind)
            .map_err(|error| PyRuntimeError::new_err(error.to_string())),
    }
}

#[pymethods]
impl ObjectStream {
    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        let model_class = self.model_class.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let next = {
                let mut guard = inner.try_lock().map_err(|_| {
                    PyRuntimeError::new_err(
                        "ObjectStream is single-consumer; concurrent or re-entrant __anext__ is not supported",
                    )
                })?;
                guard.next().await
            };
            match next {
                Some(Ok(event)) => {
                    Python::attach(|py| object_event_to_python(py, event, model_class.as_ref()))
                }
                Some(Err(error)) => Err(py_core_error(error)),
                None => Err(PyStopAsyncIteration::new_err("")),
            }
        })
    }
}

/// Accept either raw JSON Schema or a Pydantic v2 model class without taking a hard dependency on
/// Pydantic. The class is retained only so a final `completed.object.value` can be materialized;
/// all earlier stream events remain untouched and observable.
fn structured_schema(schema: Bound<'_, PyAny>) -> PyResult<(Value, Option<Arc<Py<PyAny>>>)> {
    let is_model = schema.hasattr("model_json_schema")? && schema.hasattr("model_validate")?;
    if is_model {
        let json_schema = schema.call_method0("model_json_schema")?;
        let schema_value = pythonize::depythonize(&json_schema).map_err(|error| {
            PyValueError::new_err(format!("invalid model JSON Schema: {error}"))
        })?;
        drop(json_schema);
        Ok((schema_value, Some(Arc::new(schema.unbind()))))
    } else {
        let schema_value = pythonize::depythonize(&schema)
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        Ok((schema_value, None))
    }
}

fn structured_provider_options(
    provider_options: Option<Bound<'_, PyAny>>,
) -> PyResult<ProviderOptions> {
    provider_options
        .map(|options| {
            pythonize::depythonize(&options).map_err(|error| {
                PyValueError::new_err(format!("invalid structured provider_options: {error}"))
            })
        })
        .transpose()
        .map(Option::unwrap_or_default)
}

fn optional_u64(
    object: &serde_json::Map<String, Value>,
    field: &str,
) -> Result<Option<u64>, String> {
    match object.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => value
            .as_u64()
            .map(Some)
            .ok_or_else(|| format!("RunOptions.{field} must be a non-negative integer")),
    }
}

fn optional_f64(
    object: &serde_json::Map<String, Value>,
    field: &str,
) -> Result<Option<f64>, String> {
    match object.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => value
            .as_f64()
            .filter(|value| value.is_finite())
            .map(Some)
            .ok_or_else(|| format!("RunOptions.{field} must be a finite number")),
    }
}

fn reject_unknown_fields(
    object: &serde_json::Map<String, Value>,
    context: &str,
    allowed: &[&str],
) -> Result<(), String> {
    if let Some(field) = object
        .keys()
        .find(|field| !allowed.contains(&field.as_str()))
    {
        return Err(format!("{context} contains unknown field '{field}'"));
    }
    Ok(())
}

fn parse_budget_policy(value: Option<&Value>) -> Result<BudgetPolicy, String> {
    let Some(value) = value.filter(|value| !value.is_null()) else {
        return Ok(BudgetPolicy::default());
    };
    let object = value
        .as_object()
        .ok_or_else(|| "RunOptions.budget must be a mapping".to_string())?;
    reject_unknown_fields(
        object,
        "RunOptions.budget",
        &["max_total_tokens", "max_cost_usd", "pricing"],
    )?;
    let pricing = match object.get("pricing").filter(|value| !value.is_null()) {
        None => None,
        Some(pricing) => {
            let pricing = pricing
                .as_object()
                .ok_or_else(|| "RunOptions.budget.pricing must be a mapping".to_string())?;
            reject_unknown_fields(
                pricing,
                "RunOptions.budget.pricing",
                &[
                    "input_per_million_usd",
                    "output_per_million_usd",
                    "cache_read_per_million_usd",
                    "cache_write_per_million_usd",
                ],
            )?;
            Some(ModelPricing {
                input_per_million_usd: optional_f64(pricing, "input_per_million_usd")?.ok_or_else(
                    || "RunOptions.budget.pricing.input_per_million_usd is required".to_string(),
                )?,
                output_per_million_usd: optional_f64(pricing, "output_per_million_usd")?
                    .ok_or_else(|| {
                        "RunOptions.budget.pricing.output_per_million_usd is required".to_string()
                    })?,
                cache_read_per_million_usd: optional_f64(pricing, "cache_read_per_million_usd")?,
                cache_write_per_million_usd: optional_f64(pricing, "cache_write_per_million_usd")?,
            })
        }
    };
    let policy = BudgetPolicy {
        max_total_tokens: optional_u64(object, "max_total_tokens")?,
        max_cost_usd: optional_f64(object, "max_cost_usd")?,
        pricing,
    };
    policy.validate().map_err(|error| error.to_string())?;
    Ok(policy)
}

fn parse_retry_policy(value: Option<&Value>) -> Result<RetryPolicy, String> {
    let Some(value) = value.filter(|value| !value.is_null()) else {
        return Ok(RetryPolicy::default());
    };
    let object = value
        .as_object()
        .ok_or_else(|| "RunOptions.retry must be a mapping".to_string())?;
    reject_unknown_fields(
        object,
        "RunOptions.retry",
        &[
            "max_attempts_per_model",
            "base_delay_ms",
            "max_delay_ms",
            "per_attempt_timeout_ms",
        ],
    )?;
    let mut retry = RetryPolicy::default();
    if let Some(value) = optional_u64(object, "max_attempts_per_model")? {
        retry.max_attempts_per_model = u32::try_from(value)
            .map_err(|_| "RunOptions.retry.max_attempts_per_model exceeds u32".to_string())?;
    }
    if let Some(value) = optional_u64(object, "base_delay_ms")? {
        retry.base_delay_ms = value;
    }
    if let Some(value) = optional_u64(object, "max_delay_ms")? {
        retry.max_delay_ms = value;
    }
    if let Some(value) = optional_u64(object, "per_attempt_timeout_ms")? {
        retry.per_attempt_timeout_ms = value;
    }
    Ok(retry)
}

fn parse_routing_options(value: Option<&Value>) -> Result<Option<RoutingOptions>, String> {
    let Some(value) = value.filter(|value| !value.is_null()) else {
        return Ok(None);
    };
    let routing: PyRoutingOptions = serde_json::from_value(value.clone())
        .map_err(|error| format!("invalid RunOptions.routing: {error}"))?;
    let catalog = ModelCatalog::new(routing.profiles)
        .map_err(|error| format!("invalid RunOptions.routing.profiles: {error}"))?;
    Ok(Some(RoutingOptions::new(catalog, routing.request)))
}

fn build_agent_options(
    value: Option<Value>,
    governance: Governance,
) -> Result<CoreAgentOptions, String> {
    let mut options = CoreAgentOptions {
        governance,
        ..CoreAgentOptions::default()
    };
    let Some(value) = value.filter(|value| !value.is_null()) else {
        return Ok(options);
    };
    let object = value
        .as_object()
        .ok_or_else(|| "RunOptions must be a mapping".to_string())?;
    reject_unknown_fields(
        object,
        "RunOptions",
        &[
            "model",
            "fallback_models",
            "max_tokens",
            "max_turns",
            "provider_options",
            "budget",
            "retry",
            "routing",
            "compaction",
        ],
    )?;
    if let Some(model) = object.get("model") {
        options.model = model
            .as_str()
            .ok_or_else(|| "RunOptions.model must be a string".to_string())?
            .to_string();
    }
    if let Some(fallbacks) = object.get("fallback_models") {
        options.fallback_models = fallbacks
            .as_array()
            .ok_or_else(|| "RunOptions.fallback_models must be a list".to_string())?
            .iter()
            .map(|model| {
                model
                    .as_str()
                    .map(str::to_string)
                    .ok_or_else(|| "RunOptions.fallback_models entries must be strings".to_string())
            })
            .collect::<Result<Vec<_>, _>>()?;
    }
    if let Some(max_tokens) = optional_u64(object, "max_tokens")? {
        options.max_tokens = max_tokens;
    }
    if let Some(max_turns) = optional_u64(object, "max_turns")? {
        options.max_turns = usize::try_from(max_turns)
            .map_err(|_| "RunOptions.max_turns exceeds usize".to_string())?;
    }
    if let Some(compaction) = object.get("compaction").filter(|value| !value.is_null()) {
        let compaction = compaction
            .as_object()
            .ok_or_else(|| "RunOptions.compaction must be a mapping".to_string())?;
        reject_unknown_fields(
            compaction,
            "RunOptions.compaction",
            &["max_context_tokens", "keep_recent_messages"],
        )?;
        let max_context_tokens = optional_u64(compaction, "max_context_tokens")?
            .ok_or_else(|| "RunOptions.compaction.max_context_tokens is required".to_string())?;
        let keep_recent_messages = optional_u64(compaction, "keep_recent_messages")?.unwrap_or(8);
        options.compaction = CompactionPolicy::new(
            max_context_tokens,
            usize::try_from(keep_recent_messages).map_err(|_| {
                "RunOptions.compaction.keep_recent_messages exceeds usize".to_string()
            })?,
        );
    }
    if let Some(provider_options) = object.get("provider_options") {
        options.provider_options = serde_json::from_value(provider_options.clone())
            .map_err(|error| format!("invalid RunOptions.provider_options: {error}"))?;
    }
    options.budget = parse_budget_policy(object.get("budget"))?;
    options.retry = parse_retry_policy(object.get("retry"))?;
    options.routing = parse_routing_options(object.get("routing"))?;
    Ok(options)
}

fn python_agent_options(
    options: Option<Bound<'_, PyAny>>,
    governance: Governance,
) -> PyResult<CoreAgentOptions> {
    let options = options
        .map(|options| {
            pythonize::depythonize(&options)
                .map_err(|error| PyValueError::new_err(format!("invalid RunOptions: {error}")))
        })
        .transpose()?;
    build_agent_options(options, governance).map_err(PyValueError::new_err)
}

#[pymethods]
impl QueryStream {
    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let next = {
                let mut guard = inner.try_lock().map_err(|_| {
                    PyRuntimeError::new_err(
                        "QueryStream is single-consumer; concurrent or re-entrant __anext__ is not supported",
                    )
                })?;
                match guard.as_mut() {
                    Some(run) => run.next().await,
                    None => None,
                }
            };
            match next {
                Some(delta) => Python::attach(|py| {
                    let obj = pythonize::pythonize(py, &delta)
                        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
                    Ok(obj.unbind())
                }),
                None => Err(PyStopAsyncIteration::new_err("")),
            }
        })
    }

    /// `async with` gives Python deterministic early-exit cleanup; plain `async for ... break`
    /// on a custom iterator does not call `aclose()` automatically.
    fn __aenter__<'py>(slf: PyRef<'_, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let stream: Py<QueryStream> = slf.into();
        pyo3_async_runtimes::tokio::future_into_py(py, async move { Ok(stream) })
    }

    fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        _exc_type: Bound<'_, PyAny>,
        _exc_value: Bound<'_, PyAny>,
        _traceback: Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.cancellation.cancel();
        let inner = self.inner.clone();
        let recorder = self.recorder.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            close_query_stream(inner, recorder).await;
            Ok(false)
        })
    }

    /// Request cooperative cancellation immediately. Use `await stream.aclose()` when the caller
    /// must wait for Stop hooks, audit emission, and recorder finalization before continuing.
    fn cancel(&self) {
        self.cancellation.cancel();
    }

    fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }

    /// Cancel, drain the core driver, and return the terminal canonical RunOutcome.
    fn aclose<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.cancellation.cancel();
        let inner = self.inner.clone();
        let recorder = self.recorder.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let outcome = close_query_stream(inner, recorder).await;
            Python::attach(|py| {
                pythonize::pythonize(py, &outcome)
                    .map(Bound::unbind)
                    .map_err(|error| PyRuntimeError::new_err(error.to_string()))
            })
        })
    }

    /// Current recorder snapshot. It is terminal after exhaustion or `await aclose()`.
    fn outcome<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        pythonize::pythonize(py, &self.recorder.outcome())
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))
    }
}

/// Parse a list of permission-rule dicts into the shared permission engine. Each
/// dict: `{"effect": "deny"|"allow"|"ask", "tool": str, "pattern": str?, "field": str?}`. The
/// pattern is a regex matched against the tool input's decoded string values.
fn build_permissions(
    rules: Option<Vec<Bound<'_, PyAny>>>,
    mode: PermissionMode,
) -> PyResult<PermissionEngine> {
    let mut parsed: Vec<Rule> = Vec::new();
    if let Some(rules) = rules {
        for r in rules {
            let spec: PyPermissionRuleSpec = pythonize::depythonize(&r).map_err(|error| {
                PyValueError::new_err(format!("invalid permission rule: {error}"))
            })?;
            let mut base = match spec.effect.as_str() {
                "allow" => Rule::allow(spec.tool),
                "deny" => Rule::deny(spec.tool),
                "ask" => Rule::ask(spec.tool),
                other => {
                    return Err(PyValueError::new_err(format!(
                        "unknown permission effect '{other}' (expected allow/deny/ask)"
                    )))
                }
            };
            if let Some(id) = spec.id {
                base = base.named(id);
            }
            let rule = match (spec.field, spec.pattern) {
                (Some(f), Some(p)) => base
                    .matching_field(f, &p)
                    .map_err(|e| PyValueError::new_err(e.to_string()))?,
                (None, Some(p)) => base
                    .matching(&p)
                    .map_err(|e| PyValueError::new_err(e.to_string()))?,
                (Some(_), None) => {
                    return Err(PyValueError::new_err(
                        "permission rule field requires pattern",
                    ))
                }
                _ => base,
            };
            parsed.push(rule);
        }
    }
    Ok(PermissionEngine::with_rules(mode, parsed))
}

fn permission_mode(mode: &str) -> PyResult<PermissionMode> {
    match mode {
        "allow" => Ok(PermissionMode::Allow),
        "deny" => Ok(PermissionMode::Deny),
        "ask" => Ok(PermissionMode::Ask),
        other => Err(PyValueError::new_err(format!(
            "unknown permission mode '{other}' (expected allow/deny/ask)"
        ))),
    }
}

fn audit_payload_policy(value: &str) -> PyResult<AuditPayloadPolicy> {
    match value {
        "metadata_only" => Ok(AuditPayloadPolicy::MetadataOnly),
        "full" => Ok(AuditPayloadPolicy::Full),
        other => Err(PyValueError::new_err(format!(
            "unknown audit payload policy '{other}' (expected metadata_only/full)"
        ))),
    }
}

fn audit_failure_mode(value: &str) -> PyResult<AuditFailureMode> {
    match value {
        "fail_closed" => Ok(AuditFailureMode::FailClosed),
        "best_effort" => Ok(AuditFailureMode::BestEffort),
        other => Err(PyValueError::new_err(format!(
            "unknown audit failure mode '{other}' (expected fail_closed/best_effort)"
        ))),
    }
}

fn jsonl_audit_trail(path: &str, payload_policy: &str, failure_mode: &str) -> PyResult<AuditTrail> {
    let payload_policy = audit_payload_policy(payload_policy)?;
    let failure_mode = audit_failure_mode(failure_mode)?;
    let sink = JsonlAuditSink::open(path)
        .map_err(|error| PyRuntimeError::new_err(format!("failed to open audit log: {error}")))?;
    Ok(AuditTrail::new()
        .with_sink(Arc::new(sink))
        .with_payload_policy(payload_policy)
        .with_failure_mode(failure_mode))
}

fn build_orchestrator(
    binding: &PyAgent,
    profiles: Vec<ModelProfile>,
    budget: BudgetLimits,
    max_parallelism: usize,
) -> PyResult<(Orchestrator, ExecutionContext)> {
    let catalog = ModelCatalog::new(profiles).map_err(|e| PyValueError::new_err(e.to_string()))?;
    let budget = BudgetLedger::new(budget).map_err(|e| PyValueError::new_err(e.to_string()))?;
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
        max_parallelism,
    );
    Ok((orchestrator, context))
}

/// Build a Python tool definition. With no callback this is a decorator factory; passing a
/// callback directly returns the same callable annotated with the canonical tool metadata.
#[pyfunction]
#[pyo3(signature = (name, description, input_schema, callback=None))]
fn tool<'py>(
    py: Python<'py>,
    name: String,
    description: String,
    input_schema: Bound<'_, PyAny>,
    callback: Option<Bound<'py, PyAny>>,
) -> PyResult<Bound<'py, PyAny>> {
    let input_schema: Value = pythonize::depythonize(&input_schema)
        .map_err(|error| PyValueError::new_err(format!("invalid tool input_schema: {error}")))?;
    if let Some(callback) = callback {
        decorate_tool(py, &callback, &name, &description, &input_schema)?;
        return Ok(callback);
    }

    let decorator = Py::new(
        py,
        PyToolDecorator {
            name,
            description,
            input_schema,
        },
    )?;
    Ok(decorator.into_bound(py).into_any())
}

/// `aikit.query(prompt, tools=None, model="mock-1", permissions=None)` → async iterator of
/// stream-delta dicts. A denied tool (per `permissions`) never runs; the model gets an error
/// tool-result instead.
///
/// A `tool` is any Python object with `.name` (str), `.description` (str), `.input_schema`
/// (dict), that is itself an `async` callable taking the input dict and returning a str.
#[pyfunction]
#[pyo3(signature = (prompt, tools=None, model=None, permissions=None, options=None))]
fn query(
    py: Python<'_>,
    prompt: Bound<'_, PyAny>,
    tools: Option<Vec<Bound<'_, PyAny>>>,
    model: Option<String>,
    permissions: Option<Vec<Bound<'_, PyAny>>>,
    options: Option<Bound<'_, PyAny>>,
) -> PyResult<QueryStream> {
    let messages = python_messages(&prompt)?;
    let mut tool_specs: Vec<ToolSpec> = Vec::new();
    let mut tool_map: HashMap<String, Arc<PyHostCallback>> = HashMap::new();
    let callback_locals = Arc::new(RwLock::new(None));
    capture_callback_locals(py, &callback_locals)?;

    if let Some(tools) = tools {
        for t in tools {
            let name: String = t.getattr("name")?.extract()?;
            let description: String = t.getattr("description")?.extract()?;
            let schema_obj = t.getattr("input_schema")?;
            let input_schema: Value = pythonize::depythonize(&schema_obj)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            tool_specs.push(ToolSpec {
                name: name.clone(),
                description,
                input_schema,
            });
            tool_map.insert(
                name,
                Arc::new(PyHostCallback {
                    callable: Arc::new(t.unbind()),
                    locals: callback_locals.clone(),
                }),
            );
        }
    }

    let governance = Governance::new(
        build_permissions(permissions, PermissionMode::Allow)?,
        HookDispatcher::new(),
    );
    let agent = Agent::from_process_env();
    let executor: Arc<dyn ToolExecutor> = Arc::new(PyToolExecutor {
        tools: RwLock::new(tool_map),
    });
    let mut run_options = python_agent_options(options, governance)?;
    if let Some(model) = model {
        run_options.model = model;
    }
    run_options.tools = tool_specs;
    let _runtime = pyo3_async_runtimes::tokio::get_runtime().enter();
    let run = CoreClient::new(agent)
        .query_cancellable_messages_with_executor(messages, run_options, executor)
        .map_err(py_agent_error)?;
    Ok(query_stream(run))
}

/// The agent-native `Agent` surface for Python — "drop in a key → get stronger."
#[pyclass(name = "Agent")]
struct PyAgent {
    inner: Agent,
    executor: Arc<PyToolExecutor>,
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
    callback_locals: CallbackLocals,
}

impl PyAgent {
    fn from_core(inner: Agent) -> Self {
        Self {
            inner,
            executor: Arc::new(PyToolExecutor::default()),
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
            callback_locals: Arc::new(RwLock::new(None)),
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
        let executor: Arc<dyn ToolExecutor> = Arc::new(PyAgentToolExecutor {
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
    ) -> PyResult<()> {
        if let Some(collision) = specs.iter().find(|spec| {
            self.inner
                .tool_specs()
                .iter()
                .any(|existing| existing.name == spec.name)
        }) {
            return Err(PyValueError::new_err(format!(
                "tool '{}' is already registered",
                collision.name
            )));
        }
        self.external_tools
            .register(&specs, executor)
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        for spec in specs {
            self.inner.add_tool(spec);
        }
        Ok(())
    }

    fn install_builtin_tools(
        &mut self,
        sandbox: Sandbox,
        tools: Arc<BuiltinTools>,
    ) -> PyResult<()> {
        let host_tools = self
            .executor
            .tools
            .read()
            .map_err(|_| PyRuntimeError::new_err("tool registry poisoned"))?;
        if let Some(spec) = tools
            .specs()
            .into_iter()
            .find(|spec| host_tools.contains_key(&spec.name))
        {
            return Err(PyValueError::new_err(format!(
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

    fn install_host_tool(
        &mut self,
        name: String,
        description: String,
        input_schema: Value,
        callback: Bound<'_, PyAny>,
    ) -> PyResult<()> {
        if self.inner.tool_specs().iter().any(|tool| tool.name == name) {
            return Err(PyValueError::new_err(format!(
                "tool '{name}' is already registered"
            )));
        }
        let callback = host_callback(callback, self.callback_locals.clone())?;
        self.executor
            .tools
            .write()
            .map_err(|_| PyRuntimeError::new_err("tool registry poisoned"))?
            .insert(name.clone(), callback);
        self.inner.add_tool(ToolSpec {
            name,
            description,
            input_schema,
        });
        Ok(())
    }

    fn start_run(
        &self,
        messages: Vec<Message>,
        mut options: CoreAgentOptions,
    ) -> PyResult<QueryStream> {
        options.tools = self.inner.tool_specs().to_vec();
        options.audit = self.audit.clone();
        let executor = self.tool_executor();
        let _runtime = pyo3_async_runtimes::tokio::get_runtime().enter();
        let run = CoreClient::new(self.inner.clone())
            .query_cancellable_messages_with_executor(messages, options, executor)
            .map_err(py_agent_error)?;
        Ok(query_stream(run))
    }
}

async fn generate_configured(
    agent: Agent,
    executor: Arc<dyn ToolExecutor>,
    governance: Governance,
    audit: AuditTrail,
    messages: Vec<Message>,
    model: String,
    max_tokens: u64,
) -> Result<GeneratedText, AgentError> {
    let tools = agent.tool_specs().to_vec();
    let options = CoreAgentOptions {
        model,
        max_tokens,
        tools,
        governance,
        audit,
        ..CoreAgentOptions::default()
    };
    let mut stream = CoreClient::new(agent)
        .query_cancellable_messages_with_executor(messages, options, executor)?;
    let recorder = stream.recorder();
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

#[pymethods]
impl PyAgent {
    #[new]
    fn new() -> Self {
        PyAgent::from_core(Agent::from_process_env())
    }

    /// Build an agent from an env dict `{VAR: value}`, activating providers by var name
    /// (ANTHROPIC_API_KEY, OPENAI_API_KEY, DEEPSEEK_API_KEY, GEMINI_API_KEY/GOOGLE_API_KEY).
    #[staticmethod]
    fn from_env(env: HashMap<String, String>) -> Self {
        PyAgent::from_core(Agent::from_env(env))
    }

    /// Persist structured audit records as owner-only JSONL. Configuration errors and unsafe
    /// targets fail immediately; configured audit defaults to metadata-only and fail-closed.
    #[pyo3(signature = (path, payload_policy="metadata_only", failure_mode="fail_closed"))]
    fn configure_jsonl_audit(
        &mut self,
        path: &str,
        payload_policy: &str,
        failure_mode: &str,
    ) -> PyResult<()> {
        self.audit = jsonl_audit_trail(path, payload_policy, failure_mode)?;
        Ok(())
    }

    /// Reopen a crash-safe local JSON memory store. Namespace selection is explicit so tenants
    /// sharing one file cannot recall each other's entries.
    #[pyo3(signature = (path, namespace="default"))]
    fn use_memory_file(&mut self, path: &str, namespace: &str) -> PyResult<()> {
        if namespace.trim().is_empty() {
            return Err(PyValueError::new_err("memory namespace must not be empty"));
        }
        let store = JsonFileMemoryStore::open(path).map_err(|error| {
            PyRuntimeError::new_err(format!("failed to open memory file: {error}"))
        })?;
        self.inner
            .set_memory_store(Arc::new(store), namespace.to_string());
        Ok(())
    }

    /// Use the process-local-CAS JSON session store for subagent execute/resume operations.
    fn use_session_file(&mut self, path: &str) -> PyResult<()> {
        if path.trim().is_empty() {
            return Err(PyValueError::new_err("session file path must not be empty"));
        }
        self.session_store = Arc::new(JsonFileSessionStore::new(path));
        Ok(())
    }

    #[pyo3(signature = (path, namespace="default"))]
    fn use_sqlite_memory(&mut self, path: &str, namespace: &str) -> PyResult<()> {
        if namespace.trim().is_empty() {
            return Err(PyValueError::new_err("memory namespace must not be empty"));
        }
        let store = SqliteMemoryStore::open(path)
            .map_err(|error| PyRuntimeError::new_err(format!("failed to open SQLite: {error}")))?;
        self.inner.set_memory_store(Arc::new(store), namespace);
        Ok(())
    }

    fn use_sqlite_sessions(&mut self, path: &str) -> PyResult<()> {
        self.session_store = Arc::new(
            SqliteSessionStore::open(path)
                .map_err(|error| PyRuntimeError::new_err(error.to_string()))?,
        );
        Ok(())
    }

    /// Clear one expired execution lease after the caller has reconciled every possibly completed
    /// external side effect. This never runs a provider or tool; retry/resume remains a separate
    /// explicit call.
    #[pyo3(signature = (session_id, *, side_effects_reconciled))]
    fn recover_expired_session(
        &self,
        session_id: &str,
        side_effects_reconciled: bool,
    ) -> PyResult<u64> {
        if !side_effects_reconciled {
            return Err(PyValueError::new_err(
                "expired session recovery requires side_effects_reconciled=True",
            ));
        }
        let base = match self.session_store.load_session(session_id) {
            Ok(session) => session,
            Err(SessionStoreError::NotFound { .. }) => Session::new(session_id, Vec::new()),
            Err(error) => return Err(py_core_error(error.into())),
        };
        self.session_store
            .clear_expired_execution_lease(base)
            .map(|session| session.revision)
            .map_err(|error| py_core_error(error.into()))
    }

    #[pyo3(signature = (allowed_hosts, search_endpoint=None, max_response_bytes=None))]
    fn register_web_tools(
        &mut self,
        allowed_hosts: Vec<String>,
        search_endpoint: Option<String>,
        max_response_bytes: Option<usize>,
    ) -> PyResult<()> {
        let mut tools = WebTools::new(allowed_hosts)
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        if let Some(endpoint) = search_endpoint {
            tools = tools
                .with_search_endpoint(endpoint)
                .map_err(|error| PyValueError::new_err(error.to_string()))?;
        }
        if let Some(bytes) = max_response_bytes {
            tools = tools.with_max_response_bytes(bytes);
        }
        let specs = tools.specs();
        self.install_external_tools(specs, Arc::new(tools))
    }

    #[pyo3(signature = (webdriver_endpoint, session_id, allowed_hosts, *, external_egress_enforced))]
    fn register_browser_tools(
        &mut self,
        webdriver_endpoint: &str,
        session_id: &str,
        allowed_hosts: Vec<String>,
        external_egress_enforced: bool,
    ) -> PyResult<()> {
        let policy = if external_egress_enforced {
            BrowserEgressPolicy::ExternallyEnforced
        } else {
            BrowserEgressPolicy::Deny
        };
        let tools = BrowserTools::new(webdriver_endpoint, session_id, allowed_hosts, policy)
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        let specs = tools.specs();
        self.install_external_tools(specs, Arc::new(tools))
    }

    fn register_mcp(&mut self, server: PyRef<'_, PyMcpServer>) -> PyResult<()> {
        self.install_external_tools(server.specs.clone(), server.executor.clone())
    }

    fn enable_capability_requests(&mut self, gated_tools: Vec<String>) -> PyResult<()> {
        if self.approver.is_none() {
            return Err(PyValueError::new_err(
                "configure can_use_tool before enabling capability requests",
            ));
        }
        if let Some(name) = gated_tools.iter().find(|name| {
            !self
                .inner
                .tool_specs()
                .iter()
                .any(|tool| tool.name == **name)
        }) {
            return Err(PyValueError::new_err(format!(
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

    #[pyo3(signature = (blocked_input_patterns=Vec::new()))]
    fn enable_default_guardrails(&mut self, blocked_input_patterns: Vec<String>) -> PyResult<()> {
        let pairs: Vec<_> = blocked_input_patterns
            .iter()
            .enumerate()
            .map(|(index, pattern)| (pattern.as_str(), format!("rule_{index}")))
            .collect();
        let blocklist = RegexBlocklist::new("blocked_input", pairs)
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        self.input_guardrails = Arc::new(GuardrailChain::new(vec![Arc::new(blocklist)]));
        self.output_guardrails = Arc::new(GuardrailChain::new(vec![
            Arc::new(SecretRedactor::default()),
            Arc::new(PiiRedactor::default()),
        ]));
        Ok(())
    }

    /// Add a credential; returns the activated provider name. `provider` disambiguates an `sk-`
    /// key that could be OpenAI or DeepSeek. Raises ValueError on an ambiguous/unknown key.
    #[pyo3(signature = (key, provider=None))]
    fn add_key(&mut self, key: &str, provider: Option<&str>) -> PyResult<String> {
        self.inner
            .add_key(key, provider, None)
            .map(|p| p.to_string())
            .map_err(|e| PyValueError::new_err(e.to_string()))
    }

    /// Register one advertised tool and its Python `async def` implementation.
    fn add_tool(
        &mut self,
        name: String,
        description: String,
        input_schema: Bound<'_, PyAny>,
        callback: Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let input_schema = pythonize::depythonize(&input_schema)
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        self.install_host_tool(name, description, input_schema, callback)
    }

    /// Register a definition created by `aikit.tool(...)` without unpacking its metadata.
    fn add_tool_definition(&mut self, definition: Bound<'_, PyAny>) -> PyResult<()> {
        let name: String = definition.getattr("name")?.extract()?;
        let description: String = definition.getattr("description")?.extract()?;
        let schema = definition.getattr("input_schema")?;
        let input_schema = pythonize::depythonize(&schema)
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        drop(schema);
        self.install_host_tool(name, description, input_schema, definition)
    }

    /// Register the canonical Read/Write/Edit/Glob/Grep suite inside one or more descriptor-
    /// relative filesystem jails. Bash is intentionally not part of this call.
    fn register_builtin_tools(&mut self, roots: Vec<String>) -> PyResult<()> {
        if roots.is_empty() || roots.iter().any(|root| root.trim().is_empty()) {
            return Err(PyValueError::new_err(
                "register_builtin_tools requires at least one non-empty jail root",
            ));
        }
        let sandbox =
            Sandbox::with_roots(roots.into_iter().map(PathBuf::from)).map_err(|error| {
                PyValueError::new_err(format!("invalid built-in jail roots: {error}"))
            })?;
        let tools = Arc::new(BuiltinTools::new(sandbox.clone()));
        self.install_builtin_tools(sandbox, tools)
    }

    /// Add Bash to an already registered built-in suite using the core's fail-closed
    /// `Required(Auto)` OS containment. An optional immutable Docker fallback makes the same
    /// contract usable off macOS. This binding exposes no uncontained Bash mode.
    #[pyo3(signature = (docker=None))]
    fn enable_bash_with_required_containment(
        &mut self,
        docker: Option<Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        let sandbox = self.builtin_sandbox.clone().ok_or_else(|| {
            PyValueError::new_err(
                "register_builtin_tools must be called before enabling contained Bash",
            )
        })?;
        let docker = docker
            .map(|docker| {
                let value = pythonize::depythonize(&docker).map_err(|error| {
                    PyValueError::new_err(format!("invalid Docker containment options: {error}"))
                })?;
                serde_json::from_value(value).map_err(|error| {
                    PyValueError::new_err(format!("invalid Docker containment options: {error}"))
                })
            })
            .transpose()?;
        let tools = Arc::new(
            BuiltinTools::new(sandbox.clone())
                .with_containment_policy(required_auto_containment(docker)),
        );
        self.install_builtin_tools(sandbox, tools)
    }

    /// Actively probe the required Bash containment backends. A missing backend is reported as
    /// unavailable and Bash execution remains fail-closed.
    fn builtin_containment_capabilities<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let tools = self.builtin_tools.clone().ok_or_else(|| {
            PyValueError::new_err("enable_bash_with_required_containment has not been called")
        })?;
        if !tools.tool_names().contains(&"Bash") {
            return Err(PyValueError::new_err(
                "enable_bash_with_required_containment has not been called",
            ));
        }
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let report = tools.containment_capabilities().await;
            Python::attach(|py| {
                pythonize::pythonize(py, &report)
                    .map(Bound::unbind)
                    .map_err(|error| PyRuntimeError::new_err(error.to_string()))
            })
        })
    }

    /// Replace the Agent's declarative permission policy. `ask` decisions flow to the callback
    /// registered with `can_use_tool`; without one they fail closed in core.
    #[pyo3(signature = (rules=None, default_mode="allow"))]
    fn set_permissions(
        &mut self,
        rules: Option<Vec<Bound<'_, PyAny>>>,
        default_mode: &str,
    ) -> PyResult<()> {
        self.permissions = build_permissions(rules, permission_mode(default_mode)?)?;
        Ok(())
    }

    /// Register an async human/host approval callback for `ask` permission decisions.
    fn can_use_tool(&mut self, callback_value: Bound<'_, PyAny>) -> PyResult<()> {
        self.approver = Some(Arc::new(PyToolApprover {
            callback: host_callback(callback_value, self.callback_locals.clone())?,
        }));
        Ok(())
    }

    fn on_user_prompt(&mut self, callback_value: Bound<'_, PyAny>) -> PyResult<()> {
        let callback = host_callback(callback_value, self.callback_locals.clone())?;
        self.hooks
            .on_user_prompt_submit_async(move |ctx: PromptContext| {
                let callback = callback.clone();
                async move {
                    let payload = serde_json::json!({
                        "run_id": ctx.run_id,
                        "prompt": ctx.prompt,
                    });
                    match call_python(callback, payload).await {
                        Ok(value) => parse_prompt_hook(value),
                        Err(error) => PromptHookOutcome::Block(callback_error("UserPrompt", error)),
                    }
                }
            });
        Ok(())
    }

    #[pyo3(signature = (callback_value, tool=None))]
    fn on_pre_tool_use(
        &mut self,
        callback_value: Bound<'_, PyAny>,
        tool: Option<String>,
    ) -> PyResult<()> {
        let callback = host_callback(callback_value, self.callback_locals.clone())?;
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
                    match call_python(callback, payload).await {
                        Ok(value) => parse_pre_hook(value),
                        Err(error) => HookOutcome::Block(callback_error("PreToolUse", error)),
                    }
                }
            });
        Ok(())
    }

    #[pyo3(signature = (callback_value, tool=None))]
    fn on_post_tool_use(
        &mut self,
        callback_value: Bound<'_, PyAny>,
        tool: Option<String>,
    ) -> PyResult<()> {
        let callback = host_callback(callback_value, self.callback_locals.clone())?;
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
                    match call_python(callback, payload).await {
                        Ok(value) => parse_post_hook(value),
                        Err(error) => {
                            PostToolOutcome::MarkError(callback_error("PostToolUse", error))
                        }
                    }
                }
            });
        Ok(())
    }

    /// Register an async hook for tool-scoped failures. This uses the core's dedicated
    /// PostToolFailure phase, so it runs before general Failure hooks and honors the same matcher.
    #[pyo3(signature = (callback_value, tool=None))]
    fn on_post_tool_failure(
        &mut self,
        callback_value: Bound<'_, PyAny>,
        tool: Option<String>,
    ) -> PyResult<()> {
        let callback = host_callback(callback_value, self.callback_locals.clone())?;
        let matcher = tool.map(HookMatcher::tool).unwrap_or_else(HookMatcher::any);
        self.hooks
            .on_post_tool_failure_async(matcher, move |ctx: FailureContext| {
                let callback = callback.clone();
                async move {
                    let stage = serde_json::to_value(ctx.stage).unwrap_or(Value::Null);
                    let payload = serde_json::json!({
                        "run_id": ctx.run_id,
                        "turn": ctx.turn,
                        "stage": stage,
                        "tool_use_id": ctx.tool_use_id,
                        "tool": ctx.tool,
                        "error": ctx.error,
                    });
                    match call_python(callback, payload).await {
                        Ok(value) => parse_failure_hook(value),
                        Err(_) => FailureHookOutcome::Continue,
                    }
                }
            });
        Ok(())
    }

    fn on_failure(&mut self, callback_value: Bound<'_, PyAny>) -> PyResult<()> {
        let callback = host_callback(callback_value, self.callback_locals.clone())?;
        self.hooks.on_failure_async(move |ctx: FailureContext| {
            let callback = callback.clone();
            async move {
                let stage = serde_json::to_value(ctx.stage).unwrap_or(Value::Null);
                let payload = serde_json::json!({
                    "run_id": ctx.run_id,
                    "turn": ctx.turn,
                    "stage": stage,
                    "tool_use_id": ctx.tool_use_id,
                    "tool": ctx.tool,
                    "error": ctx.error,
                });
                match call_python(callback, payload).await {
                    Ok(value) => parse_failure_hook(value),
                    Err(_) => FailureHookOutcome::Continue,
                }
            }
        });
        Ok(())
    }

    fn on_stop(&mut self, callback_value: Bound<'_, PyAny>) -> PyResult<()> {
        let callback = host_callback(callback_value, self.callback_locals.clone())?;
        self.hooks.on_stop_async(move |ctx: StopContext| {
            let callback = callback.clone();
            async move {
                let payload = serde_json::json!({
                    "run_id": ctx.run_id,
                    "turns": ctx.turns,
                    "reason": ctx.reason,
                    "usage": ctx.usage,
                });
                let _ = call_python(callback, payload).await;
            }
        });
        Ok(())
    }

    /// The active provider names.
    fn active_providers(&self) -> Vec<String> {
        self.inner
            .active_providers()
            .into_iter()
            .map(String::from)
            .collect()
    }

    fn has_provider(&self, provider: &str) -> bool {
        self.inner.has_provider(provider)
    }

    /// Introspect what the agent can do right now — providers (with fidelity grades) and tools.
    fn capabilities<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let caps = self.inner.capabilities();
        pythonize::pythonize(py, &caps).map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Generate one complete text response with the provider selected from `model`. The default
    /// mock model is deterministic and keyless; live models require `add_key` first.
    #[pyo3(signature = (prompt, model=None, max_tokens=1024))]
    fn generate_text<'py>(
        &self,
        py: Python<'py>,
        prompt: Bound<'_, PyAny>,
        model: Option<String>,
        max_tokens: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let messages = python_messages(&prompt)?;
        capture_callback_locals(py, &self.callback_locals)?;
        let agent = self.inner.clone();
        let executor = self.tool_executor();
        let governance = self.governance();
        let audit = self.audit.clone();
        let model = model.unwrap_or_else(|| "mock-1".into());
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let generated = generate_configured(
                agent, executor, governance, audit, messages, model, max_tokens,
            )
            .await
            .map_err(py_agent_error)?;
            Python::attach(|py| {
                let value = pythonize::pythonize(py, &generated)
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
                Ok(value.unbind())
            })
        })
    }

    /// Stream canonical deltas with the provider selected from `model`.
    #[pyo3(signature = (prompt, model=None, max_tokens=1024))]
    fn stream_text(
        &self,
        py: Python<'_>,
        prompt: Bound<'_, PyAny>,
        model: Option<String>,
        max_tokens: u64,
    ) -> PyResult<QueryStream> {
        let messages = python_messages(&prompt)?;
        capture_callback_locals(py, &self.callback_locals)?;
        let mut options = CoreAgentOptions {
            governance: self.governance(),
            ..CoreAgentOptions::default()
        };
        options.model = model.unwrap_or_else(|| "mock-1".into());
        options.max_tokens = max_tokens;
        self.start_run(messages, options)
    }

    /// Start a cancellable governed run using the full shared core RunOptions surface.
    #[pyo3(signature = (prompt, options=None))]
    fn run(
        &self,
        py: Python<'_>,
        prompt: Bound<'_, PyAny>,
        options: Option<Bound<'_, PyAny>>,
    ) -> PyResult<QueryStream> {
        let messages = python_messages(&prompt)?;
        capture_callback_locals(py, &self.callback_locals)?;
        let options = python_agent_options(options, self.governance())?;
        self.start_run(messages, options)
    }

    /// Snapshot this configured Agent into a reusable high-level Client.
    fn client(&self) -> PyClient {
        PyClient::from_agent(self)
    }

    /// Explicitly persist one JSON-compatible value. Model output is never remembered
    /// automatically.
    fn remember(&self, key: String, value: Bound<'_, PyAny>) -> PyResult<()> {
        let value =
            pythonize::depythonize(&value).map_err(|e| PyValueError::new_err(e.to_string()))?;
        self.inner
            .remember(key, value)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Search explicit memories in this agent's namespace.
    #[pyo3(signature = (query, limit=10))]
    fn recall<'py>(
        &self,
        py: Python<'py>,
        query: &str,
        limit: usize,
    ) -> PyResult<Bound<'py, PyAny>> {
        let entries = self
            .inner
            .recall(query, limit)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        // `MemoryEntry` uses u128 millisecond timestamps. serde_json safely narrows current
        // epoch values to JSON numbers before pythonize (which intentionally rejects raw u128).
        let entries =
            serde_json::to_value(entries).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        pythonize::pythonize(py, &entries).map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Deterministically route over caller-supplied model profiles. Credential values never
    /// cross into the catalog or result; the core injects only this agent's active providers.
    fn route<'py>(
        &self,
        py: Python<'py>,
        profiles: Bound<'py, PyAny>,
        request: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let profiles: Vec<ModelProfile> = pythonize::depythonize(&profiles)
            .map_err(|e| PyValueError::new_err(format!("invalid model profiles: {e}")))?;
        let request: RouteRequest = pythonize::depythonize(&request)
            .map_err(|e| PyValueError::new_err(format!("invalid route request: {e}")))?;
        let catalog =
            ModelCatalog::new(profiles).map_err(|e| PyValueError::new_err(e.to_string()))?;
        let decision = self
            .inner
            .route(&catalog, request)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        pythonize::pythonize(py, &decision).map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Ergonomic constructor for the exact canonical SubagentSpec consumed by run_subagent,
    /// fan_out, parallel, council, and resume_subagent.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (id, prompt, route, system=None, allowed_tools=None, max_turns=16, max_tokens=4096, estimated_input_tokens=1024))]
    fn subtask<'py>(
        &self,
        py: Python<'py>,
        id: String,
        prompt: String,
        route: Bound<'_, PyAny>,
        system: Option<String>,
        allowed_tools: Option<Vec<String>>,
        max_turns: usize,
        max_tokens: u64,
        estimated_input_tokens: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let route: ModelRouteRequirements = pythonize::depythonize(&route)
            .map_err(|error| PyValueError::new_err(format!("invalid subtask route: {error}")))?;
        let mut spec = SubagentSpec::new(id, prompt, route)
            .with_allowed_tools(allowed_tools.unwrap_or_default())
            .with_limits(max_turns, max_tokens, estimated_input_tokens);
        spec.system = system;
        pythonize::pythonize(py, &spec).map_err(|error| PyRuntimeError::new_err(error.to_string()))
    }

    /// Run one governed, budget-aware child agent. Registered host tools, hooks, approvals, and
    /// permission policy are inherited, then narrowed by the child's `allowed_tools` scope.
    #[pyo3(signature = (spec, profiles, budget=None, max_parallelism=4))]
    fn run_subagent<'py>(
        &self,
        py: Python<'py>,
        spec: Bound<'py, PyAny>,
        profiles: Bound<'py, PyAny>,
        budget: Option<Bound<'py, PyAny>>,
        max_parallelism: usize,
    ) -> PyResult<Bound<'py, PyAny>> {
        capture_callback_locals(py, &self.callback_locals)?;
        let spec: SubagentSpec = pythonize::depythonize(&spec)
            .map_err(|e| PyValueError::new_err(format!("invalid subagent spec: {e}")))?;
        let profiles: Vec<ModelProfile> = pythonize::depythonize(&profiles)
            .map_err(|e| PyValueError::new_err(format!("invalid model profiles: {e}")))?;
        let budget = match budget {
            Some(budget) => pythonize::depythonize(&budget)
                .map_err(|e| PyValueError::new_err(format!("invalid budget limits: {e}")))?,
            None => BudgetLimits::default(),
        };
        let (orchestrator, context) = build_orchestrator(self, profiles, budget, max_parallelism)?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = orchestrator.execute(spec, &context).await;
            let value =
                serde_json::to_value(result).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            Python::attach(|py| {
                let value = pythonize::pythonize(py, &value)
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
                Ok(value.unbind())
            })
        })
    }

    /// Run independent children with bounded concurrency while preserving input order.
    #[pyo3(signature = (specs, profiles, budget=None, max_parallelism=4))]
    fn fan_out<'py>(
        &self,
        py: Python<'py>,
        specs: Bound<'py, PyAny>,
        profiles: Bound<'py, PyAny>,
        budget: Option<Bound<'py, PyAny>>,
        max_parallelism: usize,
    ) -> PyResult<Bound<'py, PyAny>> {
        capture_callback_locals(py, &self.callback_locals)?;
        let specs: Vec<SubagentSpec> = pythonize::depythonize(&specs)
            .map_err(|e| PyValueError::new_err(format!("invalid subagent specs: {e}")))?;
        let profiles: Vec<ModelProfile> = pythonize::depythonize(&profiles)
            .map_err(|e| PyValueError::new_err(format!("invalid model profiles: {e}")))?;
        let budget = match budget {
            Some(budget) => pythonize::depythonize(&budget)
                .map_err(|e| PyValueError::new_err(format!("invalid budget limits: {e}")))?,
            None => BudgetLimits::default(),
        };
        let (orchestrator, context) = build_orchestrator(self, profiles, budget, max_parallelism)?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let results = orchestrator.fan_out(specs, &context).await;
            let value = serde_json::to_value(results)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            Python::attach(|py| {
                let value = pythonize::pythonize(py, &value)
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
                Ok(value.unbind())
            })
        })
    }

    /// Concise alias for fan_out; it preserves bounded concurrency, ordering, and governance.
    #[pyo3(signature = (specs, profiles, budget=None, max_parallelism=4))]
    fn parallel<'py>(
        &self,
        py: Python<'py>,
        specs: Bound<'py, PyAny>,
        profiles: Bound<'py, PyAny>,
        budget: Option<Bound<'py, PyAny>>,
        max_parallelism: usize,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.fan_out(py, specs, profiles, budget, max_parallelism)
    }

    /// Run a parallel council and synthesize only after `min_successes` members succeed.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (members, synthesizer, profiles, min_successes=1, budget=None, max_parallelism=4))]
    fn council<'py>(
        &self,
        py: Python<'py>,
        members: Bound<'py, PyAny>,
        synthesizer: Bound<'py, PyAny>,
        profiles: Bound<'py, PyAny>,
        min_successes: usize,
        budget: Option<Bound<'py, PyAny>>,
        max_parallelism: usize,
    ) -> PyResult<Bound<'py, PyAny>> {
        capture_callback_locals(py, &self.callback_locals)?;
        let members: Vec<SubagentSpec> = pythonize::depythonize(&members)
            .map_err(|e| PyValueError::new_err(format!("invalid council members: {e}")))?;
        let synthesizer: SubagentSpec = pythonize::depythonize(&synthesizer)
            .map_err(|e| PyValueError::new_err(format!("invalid synthesizer spec: {e}")))?;
        let profiles: Vec<ModelProfile> = pythonize::depythonize(&profiles)
            .map_err(|e| PyValueError::new_err(format!("invalid model profiles: {e}")))?;
        let budget = match budget {
            Some(budget) => pythonize::depythonize(&budget)
                .map_err(|e| PyValueError::new_err(format!("invalid budget limits: {e}")))?,
            None => BudgetLimits::default(),
        };
        let (orchestrator, context) = build_orchestrator(self, profiles, budget, max_parallelism)?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = orchestrator
                .council(members, synthesizer, min_successes, &context)
                .await;
            let value =
                serde_json::to_value(result).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            Python::attach(|py| {
                let value = pythonize::pythonize(py, &value)
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
                Ok(value.unbind())
            })
        })
    }

    /// Resume a previously persisted child session through the same per-Agent store and CAS
    /// contract used by the Rust core.
    #[pyo3(signature = (session_id, spec, profiles, budget=None, max_parallelism=4))]
    fn resume_subagent<'py>(
        &self,
        py: Python<'py>,
        session_id: String,
        spec: Bound<'py, PyAny>,
        profiles: Bound<'py, PyAny>,
        budget: Option<Bound<'py, PyAny>>,
        max_parallelism: usize,
    ) -> PyResult<Bound<'py, PyAny>> {
        capture_callback_locals(py, &self.callback_locals)?;
        let spec: SubagentSpec = pythonize::depythonize(&spec)
            .map_err(|e| PyValueError::new_err(format!("invalid subagent spec: {e}")))?;
        let profiles: Vec<ModelProfile> = pythonize::depythonize(&profiles)
            .map_err(|e| PyValueError::new_err(format!("invalid model profiles: {e}")))?;
        let budget = match budget {
            Some(budget) => pythonize::depythonize(&budget)
                .map_err(|e| PyValueError::new_err(format!("invalid budget limits: {e}")))?,
            None => BudgetLimits::default(),
        };
        let (orchestrator, context) = build_orchestrator(self, profiles, budget, max_parallelism)?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = orchestrator.resume(&session_id, spec, &context).await;
            let value =
                serde_json::to_value(result).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            Python::attach(|py| {
                let value = pythonize::pythonize(py, &value)
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
                Ok(value.unbind())
            })
        })
    }

    /// Generate a schema-validated object. Defaults to the deterministic keyless
    /// `mock-structured` model; pass a live model after activating its provider with `add_key`.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (prompt, schema, model=None, max_retries=2, max_tokens=1024, name=None, provider_options=None, validator=None))]
    fn generate_object<'py>(
        &self,
        py: Python<'py>,
        prompt: Bound<'_, PyAny>,
        schema: Bound<'py, PyAny>,
        model: Option<String>,
        max_retries: u32,
        max_tokens: u64,
        name: Option<String>,
        provider_options: Option<Bound<'_, PyAny>>,
        validator: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let messages = python_messages(&prompt)?;
        let (schema, model_class) = structured_schema(schema)?;
        let provider_options = structured_provider_options(provider_options)?;
        let semantic_validator = validator
            .map(|callback| {
                capture_callback_locals(py, &self.callback_locals)?;
                Ok::<Arc<dyn SemanticValidator>, PyErr>(Arc::new(PySemanticValidator {
                    callback: host_callback(callback, self.callback_locals.clone())?,
                })
                    as Arc<dyn SemanticValidator>)
            })
            .transpose()?;
        let agent = self.inner.clone();
        let options = ObjectOptions {
            max_retries,
            max_tokens,
            name: name.unwrap_or_else(|| "respond".into()),
            provider_options,
            semantic_validator,
        };
        let model = model.unwrap_or_else(|| "mock-structured".into());
        let audit = self.audit.fresh_run();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = agent
                .generate_object_messages_with_audit(
                    messages,
                    schema,
                    &model,
                    options,
                    Some(&audit),
                )
                .await
                .map_err(py_agent_error)?;
            Python::attach(|py| {
                let raw_value = pythonize::pythonize(py, &result.value)
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
                let value = match model_class {
                    Some(model_class) => model_class
                        .as_ref()
                        .bind(py)
                        .call_method1("model_validate", (raw_value,))?,
                    None => raw_value,
                };
                let output = PyDict::new(py);
                output.set_item("value", value)?;
                output.set_item(
                    "fidelity",
                    pythonize::pythonize(py, &result.fidelity)
                        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?,
                )?;
                output.set_item("attempts", result.attempts)?;
                output.set_item(
                    "provider_metadata",
                    pythonize::pythonize(py, &result.provider_metadata)
                        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?,
                )?;
                Ok(output.into_any().unbind())
            })
        })
    }

    /// Incrementally stream structured-output attempts, canonical provider deltas, validation
    /// failures/repairs, and finally one schema-validated `completed` event. Pydantic v2 classes
    /// materialize only the final `completed.object.value`; no intermediate event is hidden.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (prompt, schema, model=None, max_retries=2, max_tokens=1024, name=None, provider_options=None, validator=None))]
    fn stream_object(
        &self,
        py: Python<'_>,
        prompt: Bound<'_, PyAny>,
        schema: Bound<'_, PyAny>,
        model: Option<String>,
        max_retries: u32,
        max_tokens: u64,
        name: Option<String>,
        provider_options: Option<Bound<'_, PyAny>>,
        validator: Option<Bound<'_, PyAny>>,
    ) -> PyResult<ObjectStream> {
        let messages = python_messages(&prompt)?;
        let (schema, model_class) = structured_schema(schema)?;
        let provider_options = structured_provider_options(provider_options)?;
        let semantic_validator = validator
            .map(|callback| {
                capture_callback_locals(py, &self.callback_locals)?;
                Ok::<Arc<dyn SemanticValidator>, PyErr>(Arc::new(PySemanticValidator {
                    callback: host_callback(callback, self.callback_locals.clone())?,
                })
                    as Arc<dyn SemanticValidator>)
            })
            .transpose()?;
        let audit = self.audit.fresh_run();
        let stream = self
            .inner
            .stream_object_messages_with_audit(
                messages,
                schema,
                model.as_deref().unwrap_or("mock-structured"),
                ObjectOptions {
                    max_retries,
                    max_tokens,
                    name: name.unwrap_or_else(|| "respond".into()),
                    provider_options,
                    semantic_validator,
                },
                Some(&audit),
            )
            .map_err(py_agent_error)?;
        Ok(ObjectStream {
            inner: Arc::new(TokioMutex::new(stream)),
            model_class,
        })
    }

    fn __repr__(&self) -> String {
        format!("{:?}", self.inner)
    }
}

/// Reusable high-level client that snapshots one configured Agent while preserving its tools,
/// governance callbacks, and provider credentials across queries.
#[pyclass(name = "Client")]
struct PyClient {
    inner: CoreClient,
    executor: Arc<dyn ToolExecutor>,
    governance: Governance,
    audit: AuditTrail,
    callback_locals: CallbackLocals,
}

impl PyClient {
    fn from_agent(agent: &PyAgent) -> Self {
        Self {
            inner: CoreClient::new(agent.inner.clone()),
            executor: agent.tool_executor(),
            governance: agent.governance(),
            audit: agent.audit.clone(),
            callback_locals: agent.callback_locals.clone(),
        }
    }

    fn start_run(
        &self,
        messages: Vec<Message>,
        mut options: CoreAgentOptions,
    ) -> PyResult<QueryStream> {
        options.tools = self.inner.agent().tool_specs().to_vec();
        options.audit = self.audit.clone();
        let executor = self.executor.clone();
        let _runtime = pyo3_async_runtimes::tokio::get_runtime().enter();
        let run = self
            .inner
            .query_cancellable_messages_with_executor(messages, options, executor)
            .map_err(py_agent_error)?;
        Ok(query_stream(run))
    }
}

#[pymethods]
impl PyClient {
    #[new]
    fn new(agent: PyRef<'_, PyAgent>) -> Self {
        Self::from_agent(&agent)
    }

    #[pyo3(signature = (prompt, options=None))]
    fn query(
        &self,
        py: Python<'_>,
        prompt: Bound<'_, PyAny>,
        options: Option<Bound<'_, PyAny>>,
    ) -> PyResult<QueryStream> {
        let messages = python_messages(&prompt)?;
        capture_callback_locals(py, &self.callback_locals)?;
        let options = python_agent_options(options, self.governance.clone())?;
        self.start_run(messages, options)
    }
}

#[pymodule]
fn aikit(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("AikitError", m.py().get_type::<AikitError>())?;
    m.add_function(wrap_pyfunction!(tool, m)?)?;
    m.add_function(wrap_pyfunction!(query, m)?)?;
    m.add_function(wrap_pyfunction!(evaluate_outcome, m)?)?;
    m.add_function(wrap_pyfunction!(connect_mcp_http, m)?)?;
    m.add_function(wrap_pyfunction!(connect_mcp_stdio, m)?)?;
    m.add_class::<PyToolDecorator>()?;
    m.add_class::<QueryStream>()?;
    m.add_class::<ObjectStream>()?;
    m.add_class::<PyAgent>()?;
    m.add_class::<PyMcpServer>()?;
    m.add_class::<PyClient>()?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn mcp_tool_filter_options_reject_unknown_fields_and_invalid_names() {
        let filter = McpToolFilter::from_value(serde_json::json!({
            "allow": ["read_file"],
            "deny": ["write_file"]
        }))
        .unwrap();
        assert!(filter.allows("read_file"));
        assert!(!filter.allows("write_file"));

        assert!(McpToolFilter::from_value(serde_json::json!({
            "deny": ["write_file", "write_file"]
        }))
        .is_err());
        assert!(McpToolFilter::from_value(serde_json::json!({
            "allow": ["read_file"],
            "unexpected": []
        }))
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
            Some(serde_json::json!({
                "model": "primary",
                "fallback_models": ["fallback-a", "fallback-b"],
                "max_tokens": 321,
                "max_turns": 7,
                "provider_options": {"openai": {"temperature": 0}},
                "budget": {
                    "max_total_tokens": 999,
                    "max_cost_usd": 1.25,
                    "pricing": {
                        "input_per_million_usd": 2.0,
                        "output_per_million_usd": 4.0
                    }
                },
                "retry": {
                    "max_attempts_per_model": 3,
                    "base_delay_ms": 10,
                    "max_delay_ms": 20,
                    "per_attempt_timeout_ms": 30
                }
            })),
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
    }

    #[test]
    fn run_options_reject_unknown_fields_in_cost_and_reliability_controls() {
        for (value, field) in [
            (
                serde_json::json!({"budegt": {"max_total_tokens": 0}}),
                "budegt",
            ),
            (
                serde_json::json!({"budget": {"max_total_tokenz": 0}}),
                "max_total_tokenz",
            ),
            (
                serde_json::json!({"budget": {"pricing": {
                    "input_per_million_usd": 1.0,
                    "output_per_million_usd": 2.0,
                    "cache_read_per_million": 0.5
                }}}),
                "cache_read_per_million",
            ),
            (
                serde_json::json!({"retry": {"max_attempts_per_modal": 1}}),
                "max_attempts_per_modal",
            ),
            (
                serde_json::json!({"compaction": {
                    "max_context_tokens": 100,
                    "keep_recent_messagez": 2
                }}),
                "keep_recent_messagez",
            ),
        ] {
            let error = build_agent_options(Some(value), Governance::default())
                .err()
                .expect("invalid options must fail closed");
            assert!(error.contains(field), "unexpected error: {error}");
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
        let options: PyDockerContainmentOptions = serde_json::from_value(serde_json::json!({
            "image": pinned,
            "executable": missing_docker,
            "pids_limit": 17,
            "memory_mib": 256,
            "cpus": 2,
            "tmpfs_mib": 32
        }))
        .unwrap();
        let policy = required_auto_containment(Some(options));
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
            .with_containment_policy(required_auto_containment(Some(
                serde_json::from_value(serde_json::json!({"image": "alpine:latest"})).unwrap(),
            )))
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
