use aikit::{
    run_agent, ActiveContainmentBackend, Agent, AgentOptions, ApprovalDecision, ApprovalRequest,
    AuditEvent, AuditTrail, BackendSelector, BudgetLedger, BudgetLimits, BudgetPolicy,
    BuiltinTools, CancellationToken, Client, CompatibilityMode, ContainmentPolicy,
    ContainmentRequirement, ContentBlock, DockerConfig, ExecutionContext, FailureHookOutcome,
    Governance, HookDispatcher, HookMatcher, HookOutcome, InMemoryAuditSink, InMemorySessionStore,
    MediaSource, Message, MockProvider, ModelCatalog, ModelPricing, ModelProfile,
    ModelRouteRequirements, NoTools, ObjectOptions, ObjectStreamEvent, Orchestrator,
    PermissionEngine, PermissionMode, PermissionUpdate, PostToolOutcome, PromptHookOutcome,
    ProviderOptions, RetryPolicy, RouteObjective, RouteRequest, RoutingOptions, Rule, RunConfig,
    RunOutcome, RunRecorder, Sandbox, StreamDelta, SubagentSpec, ToolApprover, ToolExecutor,
    ToolSpec,
};
use async_trait::async_trait;
use futures::StreamExt;
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

const FILE_TOOL_NAMES: [&str; 5] = ["Read", "Write", "Edit", "Grep", "Glob"];

fn canonicalize(value: Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.into_iter().map(canonicalize).collect()),
        Value::Object(values) => {
            let sorted = values
                .into_iter()
                .map(|(key, value)| (key, canonicalize(value)))
                .collect::<BTreeMap<_, _>>();
            Value::Object(sorted.into_iter().collect::<Map<_, _>>())
        }
        primitive => primitive,
    }
}

fn emit(module: &str, value: Value) -> Result<(), Box<dyn std::error::Error>> {
    println!(
        "CONFORMANCE_{}_JSON={}",
        module.to_ascii_uppercase(),
        serde_json::to_string(&canonicalize(value))?
    );
    Ok(())
}

fn tool(name: &str, schema: Value) -> ToolSpec {
    ToolSpec {
        name: name.into(),
        description: name.into(),
        input_schema: schema,
    }
}

fn enum_name<T: serde::Serialize>(value: T) -> String {
    serde_json::to_value(value)
        .expect("enum must serialize")
        .as_str()
        .expect("enum must serialize to a string")
        .to_string()
}

#[derive(Clone)]
struct RecordingExecutor {
    calls: Arc<AtomicUsize>,
    events: Option<Arc<Mutex<Vec<String>>>>,
    inputs: Option<Arc<Mutex<Vec<String>>>>,
    prefix: &'static str,
}

#[async_trait]
impl ToolExecutor for RecordingExecutor {
    async fn execute(&self, _name: &str, input: Value) -> aikit::Result<String> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if let Some(events) = &self.events {
            events.lock().unwrap().push("tool".into());
        }
        let value = input.get("q").and_then(Value::as_str).unwrap_or("?");
        if let Some(inputs) = &self.inputs {
            inputs.lock().unwrap().push(value.into());
        }
        Ok(format!("{}{}", self.prefix, value))
    }
}

struct RewritingApprover {
    events: Arc<Mutex<Vec<String>>>,
    inputs: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl ToolApprover for RewritingApprover {
    async fn approve(&self, request: ApprovalRequest) -> ApprovalDecision {
        self.events.lock().unwrap().push("approve".into());
        self.inputs.lock().unwrap().push(
            request
                .input
                .get("q")
                .and_then(Value::as_str)
                .unwrap_or("?")
                .into(),
        );
        ApprovalDecision::Allow {
            updated_input: Some(json!({ "q": "approved" })),
            updated_permissions: vec![PermissionUpdate::AllowExactInput],
        }
    }
}

struct InterruptApprover;

#[async_trait]
impl ToolApprover for InterruptApprover {
    async fn approve(&self, _request: ApprovalRequest) -> ApprovalDecision {
        ApprovalDecision::Deny {
            message: "operator stopped".into(),
            interrupt: true,
        }
    }
}

struct ChildApprover {
    events: Arc<Mutex<Vec<String>>>,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl ToolApprover for ChildApprover {
    async fn approve(&self, _request: ApprovalRequest) -> ApprovalDecision {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.events.lock().unwrap().push("approve".into());
        ApprovalDecision::Allow {
            updated_input: None,
            updated_permissions: vec![PermissionUpdate::AllowTool],
        }
    }
}

async fn run_config(
    mut config: RunConfig,
    executor: Arc<dyn ToolExecutor>,
) -> (RunOutcome, Vec<StreamDelta>) {
    let recorder = RunRecorder::default();
    config.recorder = recorder.clone();
    let stream = run_agent(Arc::new(MockProvider), executor, config);
    let deltas = stream.collect::<Vec<_>>().await;
    (recorder.outcome(), deltas)
}

async fn governance_facts() -> Value {
    let events = Arc::new(Mutex::new(Vec::<String>::new()));
    let approval_inputs = Arc::new(Mutex::new(Vec::<String>::new()));
    let tool_inputs = Arc::new(Mutex::new(Vec::<String>::new()));
    let calls = Arc::new(AtomicUsize::new(0));
    let mut hooks = HookDispatcher::new();
    let prompt_events = events.clone();
    hooks.on_user_prompt_submit(move |prompt| {
        prompt_events.lock().unwrap().push("prompt".into());
        PromptHookOutcome::Rewrite(format!("{prompt} [checked]"))
    });
    let pre_events = events.clone();
    hooks.on_pre_tool_use(HookMatcher::tool("search"), move |_tool, _input| {
        pre_events.lock().unwrap().push("pre".into());
        HookOutcome::Rewrite(json!({ "q": "pre-approved" }))
    });
    let post_events = events.clone();
    hooks.on_post_tool_use(HookMatcher::tool("search"), move |_tool, _input, output| {
        post_events.lock().unwrap().push("post".into());
        PostToolOutcome::RewriteOutput(format!("post:{output}"))
    });
    let stop_events = events.clone();
    hooks.on_stop(move |_context| stop_events.lock().unwrap().push("stop".into()));
    let governance = Governance::new(
        PermissionEngine::with_rules(PermissionMode::Allow, vec![Rule::ask("search")]),
        hooks,
    )
    .with_approver(Arc::new(RewritingApprover {
        events: events.clone(),
        inputs: approval_inputs.clone(),
    }));
    let mut config = RunConfig::new("mock-1", vec![Message::user("governed")]);
    config.tools = vec![tool(
        "search",
        json!({
            "type": "object",
            "required": ["q"],
            "properties": { "q": { "type": "string" } }
        }),
    )];
    config.governance = governance;
    let (outcome, _) = run_config(
        config,
        Arc::new(RecordingExecutor {
            calls: calls.clone(),
            events: Some(events.clone()),
            inputs: Some(tool_inputs.clone()),
            prefix: "rows for ",
        }),
    )
    .await;
    let result = outcome
        .messages
        .iter()
        .flat_map(|message| &message.content)
        .find_map(|block| match block {
            ContentBlock::ToolResult {
                content,
                is_error: false,
                ..
            } => Some(content.clone()),
            _ => None,
        })
        .expect("approved tool result");

    let deny_calls = Arc::new(AtomicUsize::new(0));
    let deny_stages = Arc::new(Mutex::new(Vec::<String>::new()));
    let mut deny_hooks = HookDispatcher::new();
    let observed_deny_stages = deny_stages.clone();
    deny_hooks.on_failure(move |context| {
        observed_deny_stages
            .lock()
            .unwrap()
            .push(context.stage.as_str().into());
        FailureHookOutcome::Continue
    });
    let mut deny_config = RunConfig::new("mock-1", vec![Message::user("deny wins")]);
    deny_config.tools = vec![tool("guarded", json!({ "type": "object" }))];
    deny_config.governance = Governance::new(
        PermissionEngine::with_rules(
            PermissionMode::Allow,
            vec![Rule::allow("guarded"), Rule::deny("guarded")],
        ),
        deny_hooks,
    );
    let (_, deny_deltas) = run_config(
        deny_config,
        Arc::new(RecordingExecutor {
            calls: deny_calls.clone(),
            events: None,
            inputs: None,
            prefix: "",
        }),
    )
    .await;
    let deny_results = deny_deltas
        .iter()
        .filter(|delta| matches!(delta, StreamDelta::ToolResult { .. }))
        .collect::<Vec<_>>();

    let invalid_calls = Arc::new(AtomicUsize::new(0));
    let invalid_stages = Arc::new(Mutex::new(Vec::<String>::new()));
    let mut invalid_hooks = HookDispatcher::new();
    let observed_invalid_stages = invalid_stages.clone();
    invalid_hooks.on_failure(move |context| {
        observed_invalid_stages
            .lock()
            .unwrap()
            .push(context.stage.as_str().into());
        FailureHookOutcome::Continue
    });
    let mut invalid_config = RunConfig::new("mock-1", vec![Message::user("invalid tool input")]);
    invalid_config.tools = vec![tool(
        "typed",
        json!({
            "type": "object",
            "required": ["count"],
            "properties": { "count": { "type": "integer" } }
        }),
    )];
    invalid_config.governance = Governance::new(PermissionEngine::default(), invalid_hooks);
    let (_, invalid_deltas) = run_config(
        invalid_config,
        Arc::new(RecordingExecutor {
            calls: invalid_calls.clone(),
            events: None,
            inputs: None,
            prefix: "",
        }),
    )
    .await;
    let invalid_results = invalid_deltas
        .iter()
        .filter(|delta| matches!(delta, StreamDelta::ToolResult { .. }))
        .collect::<Vec<_>>();

    let interrupt_calls = Arc::new(AtomicUsize::new(0));
    let interrupt_stops = Arc::new(Mutex::new(Vec::<String>::new()));
    let mut interrupt_hooks = HookDispatcher::new();
    let observed_stops = interrupt_stops.clone();
    interrupt_hooks.on_stop(move |context| {
        observed_stops.lock().unwrap().push(context.reason.clone());
    });
    let mut interrupt_config = RunConfig::new("mock-1", vec![Message::user("interrupt")]);
    interrupt_config.tools = vec![tool("interrupt", json!({ "type": "object" }))];
    interrupt_config.governance = Governance::new(
        PermissionEngine::with_rules(PermissionMode::Allow, vec![Rule::ask("interrupt")]),
        interrupt_hooks,
    )
    .with_approver(Arc::new(InterruptApprover));
    let (_, interrupt_deltas) = run_config(
        interrupt_config,
        Arc::new(RecordingExecutor {
            calls: interrupt_calls.clone(),
            events: None,
            inputs: None,
            prefix: "",
        }),
    )
    .await;
    let interrupt_codes = interrupt_deltas
        .iter()
        .filter_map(|delta| match delta {
            StreamDelta::Error { info, .. } => Some(enum_name(info.code)),
            _ => None,
        })
        .collect::<Vec<_>>();

    json!({
        "approval": {
            "approval_inputs": approval_inputs.lock().unwrap().clone(),
            "events": events.lock().unwrap().clone(),
            "result": result,
            "tool_inputs": tool_inputs.lock().unwrap().clone(),
        },
        "authoritative_deny": {
            "failure_stages": deny_stages.lock().unwrap().clone(),
            "is_error": deny_results.len() == 1 && matches!(
                deny_results[0],
                StreamDelta::ToolResult { is_error: true, .. }
            ),
            "tool_calls": deny_calls.load(Ordering::SeqCst),
        },
        "interrupt": {
            "error_codes": interrupt_codes,
            "stop_reasons": interrupt_stops.lock().unwrap().clone(),
            "tool_calls": interrupt_calls.load(Ordering::SeqCst),
        },
        "schema_validation": {
            "failure_stages": invalid_stages.lock().unwrap().clone(),
            "is_error": invalid_results.len() == 1 && matches!(
                invalid_results[0],
                StreamDelta::ToolResult { is_error: true, .. }
            ),
            "tool_calls": invalid_calls.load(Ordering::SeqCst),
        },
    })
}

fn delta_name(delta: &StreamDelta) -> String {
    serde_json::to_value(delta)
        .expect("delta must serialize")
        .get("type")
        .and_then(Value::as_str)
        .expect("delta has a type")
        .to_string()
}

async fn structured_facts() -> Value {
    let agent = Agent::new();
    let schema = json!({
        "type": "object",
        "required": ["currency", "status"],
        "properties": {
            "currency": { "type": "string", "enum": ["EUR"] },
            "status": { "type": "string", "enum": ["ok"] }
        }
    });
    let mut provider_options = ProviderOptions::new();
    provider_options.insert(
        "mock".into(),
        serde_json::from_value(json!({ "temperature": 0, "tag": "parity" })).unwrap(),
    );
    let mut stream = agent
        .stream_object(
            "structured",
            schema,
            "mock-structured",
            ObjectOptions {
                provider_options,
                compatibility_mode: CompatibilityMode::Warn,
                ..ObjectOptions::default()
            },
        )
        .unwrap();
    let mut event_types = Vec::new();
    let mut delta_types = Vec::new();
    let mut completed = None;
    while let Some(event) = stream.next().await {
        match event.unwrap() {
            ObjectStreamEvent::AttemptStarted { .. } => event_types.push("attempt_started"),
            ObjectStreamEvent::Delta { delta, .. } => {
                event_types.push("delta");
                delta_types.push(delta_name(&delta));
            }
            ObjectStreamEvent::ValidationFailed { .. } => event_types.push("validation_failed"),
            ObjectStreamEvent::Completed { object } => {
                event_types.push("completed");
                completed = Some(object);
            }
        }
    }
    let completed = completed.expect("structured stream completed");

    let mut repair_stream = agent
        .stream_object(
            "repair",
            json!({
                "type": "object",
                "required": ["value"],
                "properties": { "value": { "type": "string", "minLength": 8 } }
            }),
            "mock-structured",
            ObjectOptions {
                max_retries: 1,
                ..ObjectOptions::default()
            },
        )
        .unwrap();
    let mut repair = Vec::new();
    let mut repair_failed = false;
    while let Some(event) = repair_stream.next().await {
        match event {
            Ok(ObjectStreamEvent::AttemptStarted { repair: flag, .. }) => {
                repair.push(json!(["attempt_started", flag]));
            }
            Ok(ObjectStreamEvent::ValidationFailed { will_retry, .. }) => {
                repair.push(json!(["validation_failed", will_retry]));
            }
            Ok(_) => {}
            Err(_) => repair_failed = true,
        }
    }
    json!({
        "attempts": completed.attempts,
        "delta_types": delta_types,
        "event_types": event_types,
        "fidelity": enum_name(completed.fidelity),
        "provider_metadata_empty": completed.provider_metadata.is_empty(),
        "repair": repair,
        "repair_failed": repair_failed,
        "value": [completed.value["currency"], completed.value["status"]],
    })
}

async fn finish_run(mut run: aikit::CancellableRun) -> (RunOutcome, Vec<String>) {
    let mut codes = Vec::new();
    while let Some(delta) = run.next().await {
        if let StreamDelta::Error { info, .. } = delta {
            codes.push(enum_name(info.code));
        }
    }
    (run.finish().await, codes)
}

async fn run_options_facts() -> Value {
    let client = Client::new(Agent::new());
    let mut provider_options = ProviderOptions::new();
    provider_options.insert(
        "mock".into(),
        serde_json::from_value(json!({ "tag": "parity" })).unwrap(),
    );
    let (client_outcome, client_codes) = finish_run(
        client
            .query_cancellable(
                "client",
                AgentOptions {
                    model: "mock-1".into(),
                    fallback_models: vec!["mock-2".into()],
                    max_tokens: 64,
                    max_turns: 2,
                    provider_options,
                    compatibility_mode: CompatibilityMode::Warn,
                    retry: RetryPolicy {
                        max_attempts_per_model: 2,
                        base_delay_ms: 0,
                        max_delay_ms: 0,
                        per_attempt_timeout_ms: 1_000,
                    },
                    ..AgentOptions::default()
                },
            )
            .unwrap(),
    )
    .await;
    let (priced, priced_codes) = finish_run(
        client
            .query_cancellable(
                "priced",
                AgentOptions {
                    budget: BudgetPolicy {
                        max_total_tokens: None,
                        max_cost_usd: Some(1.0),
                        pricing: Some(ModelPricing {
                            input_per_million_usd: 1.0,
                            output_per_million_usd: 2.0,
                            cache_read_per_million_usd: None,
                            cache_write_per_million_usd: None,
                        }),
                    },
                    ..AgentOptions::default()
                },
            )
            .unwrap(),
    )
    .await;
    let (limited, limited_codes) = finish_run(
        client
            .query_cancellable(
                "limited",
                AgentOptions {
                    max_turns: 0,
                    ..AgentOptions::default()
                },
            )
            .unwrap(),
    )
    .await;
    let (budget, budget_codes) = finish_run(
        client
            .query_cancellable(
                "budget",
                AgentOptions {
                    budget: BudgetPolicy::token_limit(0),
                    ..AgentOptions::default()
                },
            )
            .unwrap(),
    )
    .await;
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let (cancelled, cancel_codes) = finish_run(
        client
            .query_cancellable(
                "cancelled",
                AgentOptions {
                    cancellation,
                    ..AgentOptions::default()
                },
            )
            .unwrap(),
    )
    .await;
    json!({
        "budget": [enum_name(budget.terminal_status), budget_codes],
        "cancel": [enum_name(cancelled.terminal_status), cancel_codes],
        "client": [
            enum_name(client_outcome.terminal_status),
            client_outcome.model_attempts,
            client_codes,
        ],
        "max_turns": [enum_name(limited.terminal_status), limited_codes],
        "priced_budget": [enum_name(priced.terminal_status), priced_codes],
    })
}

async fn state_facts() -> Value {
    let agent = Agent::new();
    agent
        .remember("customer_note", json!("Ada prefers EUR"))
        .unwrap();
    let memory = agent
        .recall("EUR", 3)
        .unwrap()
        .into_iter()
        .map(|entry| json!([entry.key, entry.value]))
        .collect::<Vec<_>>();
    let stop_reasons = Arc::new(Mutex::new(Vec::<String>::new()));
    let mut hooks = HookDispatcher::new();
    let observed_stops = stop_reasons.clone();
    hooks.on_stop(move |context| {
        observed_stops.lock().unwrap().push(context.reason.clone());
    });
    let audit = Arc::new(InMemoryAuditSink::default());
    let mut config = RunConfig::new("mock-1", vec![Message::user("state")]);
    config.governance = Governance::new(PermissionEngine::default(), hooks);
    config.audit = AuditTrail::new().with_sink(audit.clone());
    let (outcome, _) = run_config(config, Arc::new(NoTools)).await;
    let records = audit.records();
    assert_eq!(
        records
            .iter()
            .filter(|record| matches!(record.event, AuditEvent::RunStarted { .. }))
            .count(),
        1
    );
    assert_eq!(
        records
            .iter()
            .filter(|record| matches!(record.event, AuditEvent::RunStopped { .. }))
            .count(),
        1
    );
    json!({
        "audit": {
            "advertised": agent.capabilities().runtime_features.iter().any(|item| item == "audit"),
            "stop_reasons": stop_reasons.lock().unwrap().clone(),
        },
        "memory": memory,
        "provider_metadata_empty": outcome.provider_metadata.is_empty(),
        "session": {
            "roles": outcome.messages.iter().map(|message| enum_name(message.role)).collect::<Vec<_>>(),
            "stop_reason": outcome.stop_reason,
        },
    })
}

fn catalog() -> ModelCatalog {
    ModelCatalog::new([ModelProfile::new("mock", "mock-1", 8_192, 1_024, 1)]).unwrap()
}

fn child_spec(
    id: &str,
    prompt: &str,
    tools: impl IntoIterator<Item = &'static str>,
) -> SubagentSpec {
    SubagentSpec::new(id, prompt, ModelRouteRequirements::explicit("mock-1"))
        .with_allowed_tools(tools)
        .with_limits(2, 64, 8)
}

async fn orchestration_facts() -> Value {
    let events = Arc::new(Mutex::new(Vec::<String>::new()));
    let approvals = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let mut hooks = HookDispatcher::new();
    let pre_events = events.clone();
    hooks.on_pre_tool_use(HookMatcher::tool("child_search"), move |_tool, _input| {
        pre_events.lock().unwrap().push("pre".into());
        HookOutcome::Rewrite(json!({ "q": "child-pre" }))
    });
    let post_events = events.clone();
    hooks.on_post_tool_use(
        HookMatcher::tool("child_search"),
        move |_tool, _input, output| {
            post_events.lock().unwrap().push("post".into());
            PostToolOutcome::RewriteOutput(format!("post:{output}"))
        },
    );
    let stop_events = events.clone();
    hooks.on_stop(move |_context| stop_events.lock().unwrap().push("stop".into()));
    let governance = Governance::new(
        PermissionEngine::with_rules(PermissionMode::Allow, vec![Rule::ask("child_search")]),
        hooks,
    )
    .with_approver(Arc::new(ChildApprover {
        events: events.clone(),
        calls: approvals.clone(),
    }));
    let mut agent = Agent::new();
    agent.add_tool(tool(
        "child_search",
        json!({
            "type": "object",
            "required": ["q"],
            "properties": { "q": { "type": "string" } }
        }),
    ));
    let executor = Arc::new(RecordingExecutor {
        calls: calls.clone(),
        events: Some(events.clone()),
        inputs: None,
        prefix: "child:",
    });
    let store = Arc::new(InMemorySessionStore::default());
    let orchestrator = Orchestrator::new(Arc::new(agent), catalog(), executor, store, 2);
    let context = ExecutionContext::new(
        governance,
        AuditTrail::new(),
        BudgetLedger::new(BudgetLimits {
            max_model_calls: Some(8),
            max_input_tokens: Some(8_192),
            max_output_tokens: Some(8_192),
            max_cost_micro_usd: None,
            wall_time_ms: Some(5_000),
        })
        .unwrap(),
        BTreeSet::from(["child_search".into()]),
    );
    let first = orchestrator
        .execute(child_spec("thread", "first", ["child_search"]), &context)
        .await;
    let resumed = orchestrator
        .resume(
            "thread",
            child_spec("thread-resume", "second", ["child_search"]),
            &context,
        )
        .await;
    let tool_result = first
        .outcome
        .messages
        .iter()
        .flat_map(|message| &message.content)
        .find_map(|block| match block {
            ContentBlock::ToolResult {
                content,
                is_error: false,
                ..
            } => Some(content.clone()),
            _ => None,
        })
        .expect("child tool result");

    let plain_orchestrator = Orchestrator::new(
        Arc::new(Agent::new()),
        catalog(),
        Arc::new(NoTools),
        Arc::new(InMemorySessionStore::default()),
        2,
    );
    let plain_context = ExecutionContext::new(
        Governance::default(),
        AuditTrail::new(),
        BudgetLedger::new(BudgetLimits {
            max_model_calls: Some(8),
            max_input_tokens: Some(8_192),
            max_output_tokens: Some(8_192),
            max_cost_micro_usd: None,
            wall_time_ms: Some(5_000),
        })
        .unwrap(),
        BTreeSet::new(),
    );
    let fan = plain_orchestrator
        .fan_out(
            vec![child_spec("fan-a", "A", []), child_spec("fan-b", "B", [])],
            &plain_context,
        )
        .await;
    let deadline_context = ExecutionContext::new(
        Governance::default(),
        AuditTrail::new(),
        BudgetLedger::new(BudgetLimits {
            wall_time_ms: Some(0),
            ..BudgetLimits::default()
        })
        .unwrap(),
        BTreeSet::new(),
    );
    let deadline = plain_orchestrator
        .execute(child_spec("expired", "expired", []), &deadline_context)
        .await;
    json!({
        "context": {
            "approval_calls": approvals.load(Ordering::SeqCst),
            "events": events.lock().unwrap().clone(),
            "status": enum_name(first.status),
            "tool_calls": calls.load(Ordering::SeqCst),
            "tool_result": tool_result,
        },
        "deadline": {
            "code": deadline.error_info.map(|info| enum_name(info.code)),
            "status": enum_name(deadline.status),
            "terminal": enum_name(deadline.outcome.terminal_status),
        },
        "fan_out": {
            "ids": fan.iter().map(|result| result.id.clone()).collect::<Vec<_>>(),
            "statuses": fan.iter().map(|result| enum_name(result.status)).collect::<Vec<_>>(),
        },
        "resume": {
            "message_counts": [first.outcome.messages.len(), resumed.outcome.messages.len()],
            "revisions": [first.session_revision, resumed.session_revision],
            "statuses": [enum_name(first.status), enum_name(resumed.status)],
        },
    })
}

fn outcome_tool_result(outcome: &RunOutcome) -> Option<(String, bool)> {
    outcome
        .messages
        .iter()
        .flat_map(|message| &message.content)
        .find_map(|block| match block {
            ContentBlock::ToolResult {
                content, is_error, ..
            } => Some((content.clone(), *is_error)),
            _ => None,
        })
}

fn outcome_used_tool(outcome: &RunOutcome, expected: &str) -> bool {
    outcome
        .messages
        .iter()
        .flat_map(|message| &message.content)
        .any(|block| matches!(block, ContentBlock::ToolUse { name, .. } if name == expected))
}

fn mock_tool_options(name: &str, input: Value) -> ProviderOptions {
    let mut provider_options = ProviderOptions::new();
    provider_options.insert(
        "mock".into(),
        serde_json::from_value(json!({
            "tool_name": name,
            "tool_input": input,
        }))
        .expect("mock fixture options are an object"),
    );
    provider_options
}

async fn force_builtin(client: &Client, name: &str, input: Value) -> RunOutcome {
    finish_run(
        client
            .query_cancellable(
                format!("deterministic built-in fixture: {name}"),
                AgentOptions {
                    provider_options: mock_tool_options(name, input),
                    ..AgentOptions::default()
                },
            )
            .expect("mock fixture run starts"),
    )
    .await
    .0
}

async fn builtins_facts() -> Result<Value, Box<dyn std::error::Error>> {
    let primary = tempfile::tempdir()?;
    let secondary = tempfile::tempdir()?;
    let outside = tempfile::tempdir()?;
    let secondary_file = secondary.path().join("secondary.txt");
    let outside_file = outside.path().join("outside.txt");
    std::fs::write(&secondary_file, "secondary-ok")?;
    std::fs::write(&outside_file, "outside-secret")?;

    #[cfg(unix)]
    std::os::unix::fs::symlink(&outside_file, primary.path().join("escape-link.txt"))?;

    let roots = vec![primary.path().to_path_buf(), secondary.path().to_path_buf()];
    let sandbox = Sandbox::with_roots(roots.clone())?;
    let tools = Arc::new(BuiltinTools::new(sandbox.clone()));
    let default_names = tools
        .tool_names()
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>();
    let expected_file_names = FILE_TOOL_NAMES
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>();
    let canonical_specs = tools.specs().iter().all(|spec| {
        spec.input_schema["type"] == "object" && spec.input_schema["additionalProperties"] == false
    });

    let mut client = Client::new(Agent::new());
    client.register_builtin_tools(tools.clone());
    let written = force_builtin(
        &client,
        "Write",
        json!({ "path": "roundtrip.txt", "content": "before needle" }),
    )
    .await;
    let read_before = force_builtin(&client, "Read", json!({ "path": "roundtrip.txt" })).await;
    let edited = force_builtin(
        &client,
        "Edit",
        json!({
            "path": "roundtrip.txt",
            "old_string": "before",
            "new_string": "after",
        }),
    )
    .await;
    let read_after = force_builtin(&client, "Read", json!({ "path": "roundtrip.txt" })).await;
    let grep = force_builtin(&client, "Grep", json!({ "pattern": "after", "path": "." })).await;
    let glob = force_builtin(&client, "Glob", json!({ "pattern": "*.txt" })).await;
    let multi_root = force_builtin(
        &client,
        "Read",
        json!({ "path": secondary_file.to_string_lossy() }),
    )
    .await;
    let outside_denial = force_builtin(
        &client,
        "Read",
        json!({ "path": outside_file.to_string_lossy() }),
    )
    .await;
    #[cfg(unix)]
    let symlink_denial = force_builtin(&client, "Read", json!({ "path": "escape-link.txt" })).await;
    let strict_schema = force_builtin(
        &client,
        "Read",
        json!({ "path": "roundtrip.txt", "unexpected": true }),
    )
    .await;

    let (write_text, write_error) = outcome_tool_result(&written).expect("Write result");
    let (before_text, before_error) = outcome_tool_result(&read_before).expect("Read result");
    let (_, edit_error) = outcome_tool_result(&edited).expect("Edit result");
    let (after_text, after_error) = outcome_tool_result(&read_after).expect("Read result");
    let (grep_text, grep_error) = outcome_tool_result(&grep).expect("Grep result");
    let (glob_text, glob_error) = outcome_tool_result(&glob).expect("Glob result");
    let (multi_root_text, multi_root_error) =
        outcome_tool_result(&multi_root).expect("multi-root Read result");
    let (_, outside_error) = outcome_tool_result(&outside_denial).expect("outside Read result");
    #[cfg(unix)]
    let (_, symlink_error) = outcome_tool_result(&symlink_denial).expect("symlink Read result");
    #[cfg(not(unix))]
    let symlink_error = false;
    let (_, strict_schema_error) =
        outcome_tool_result(&strict_schema).expect("strict schema result");

    let mut coexist = Agent::new();
    coexist.add_tool(tool("search", json!({ "type": "object" })));
    coexist.register_builtin_tools(BuiltinTools::new(Sandbox::with_roots(roots.clone())?));
    let coexist_names = coexist
        .tool_specs()
        .iter()
        .map(|spec| spec.name.clone())
        .collect::<Vec<_>>();
    let mut expected_coexist = vec!["search".to_string()];
    expected_coexist.extend(expected_file_names.clone());

    // Bindings reject this registration. Rust replaces the spoofed schema with its canonical
    // built-in. The byte-identical invariant is the security outcome: a host cannot spoof Read.
    let mut collision = Agent::new();
    collision.add_tool(ToolSpec {
        name: "Read".into(),
        description: "spoofed host Read".into(),
        input_schema: json!(true),
    });
    collision.register_builtin_tools(BuiltinTools::new(Sandbox::with_roots(roots.clone())?));
    let read_specs = collision
        .tool_specs()
        .iter()
        .filter(|spec| spec.name == "Read")
        .collect::<Vec<_>>();
    let host_before_builtin_spoof_blocked = read_specs.len() == 1
        && read_specs[0].description != "spoofed host Read"
        && read_specs[0].input_schema["additionalProperties"] == false;

    let bash_tools = BuiltinTools::new(Sandbox::with_roots(roots.clone())?).with_bash();
    let bash_names = bash_tools
        .tool_names()
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>();
    let containment = bash_tools.containment_capabilities().await;
    let mutable_docker = BuiltinTools::new(Sandbox::with_roots(roots.clone())?)
        .with_containment_policy(
            ContainmentPolicy::required_auto()
                .with_docker_fallback(DockerConfig::new("alpine:latest")),
        )
        .containment_capabilities()
        .await;
    let mutable_docker_rejected = mutable_docker
        .backends
        .iter()
        .any(|backend| backend.backend == ActiveContainmentBackend::Docker && !backend.available);

    let child_orchestrator = Orchestrator::new(
        Arc::new(client.agent().clone()),
        catalog(),
        tools,
        Arc::new(InMemorySessionStore::default()),
        1,
    );
    let child_context = ExecutionContext::new(
        Governance::default(),
        AuditTrail::new(),
        BudgetLedger::new(BudgetLimits::default()).expect("unlimited child budget"),
        BTreeSet::from(["Read".to_string()]),
    );
    let child = child_orchestrator
        .execute(
            child_spec("builtin-read", "use Read", ["Read"]),
            &child_context,
        )
        .await;

    Ok(json!({
        "containment": {
            "fail_closed": containment.fail_closed,
            "mutable_docker_rejected": mutable_docker_rejected,
            "required_auto": matches!(
                containment.requirement,
                ContainmentRequirement::Required(BackendSelector::Auto)
            ),
            "uncontained": containment.selected_backend
                == Some(ActiveContainmentBackend::Uncontained),
        },
        "filesystem": {
            "edit": !edit_error,
            "glob": !glob_error && glob_text.contains("roundtrip.txt"),
            "grep": !grep_error && grep_text.contains("after needle"),
            "multi_root_read": !multi_root_error && multi_root_text == "secondary-ok",
            "outside_denied": outside_error,
            "read_after": !after_error && after_text == "after needle",
            "read_before": !before_error && before_text == "before needle",
            "symlink_denied": symlink_error,
            "write": !write_error && write_text.contains("wrote"),
        },
        "registry": {
            "bash_absent_by_default": !default_names.iter().any(|name| name == "Bash"),
            "bash_tools": bash_names,
            "canonical_specs_strict": canonical_specs && strict_schema_error,
            "default_tools": default_names,
            "host_before_builtin_spoof_blocked": host_before_builtin_spoof_blocked,
            "host_builtin_coexist": coexist_names == expected_coexist,
        },
        "subagent": {
            "read_advertised": expected_file_names.iter().any(|name| name == "Read"),
            "read_inherited": outcome_used_tool(&child.outcome, "Read"),
            "status": enum_name(child.status),
        },
    }))
}

async fn input_facts() -> Value {
    let agent = Agent::new();
    let messages = vec![Message {
        role: aikit::Role::User,
        content: vec![
            ContentBlock::Text {
                text: "multimodal".into(),
            },
            ContentBlock::Media {
                media_type: "image/png".into(),
                source: MediaSource::Base64 {
                    data: "aGVsbG8=".into(),
                },
            },
        ],
    }];
    let generated = agent
        .generate_text_messages(messages.clone(), "mock-1", 64)
        .await
        .expect("multimodal text input");
    let object = agent
        .generate_object_messages(
            messages,
            json!({
                "type": "object",
                "required": ["status"],
                "properties": { "status": { "const": "ok" } }
            }),
            "mock-structured",
            ObjectOptions::default(),
        )
        .await
        .expect("multimodal structured input");

    let routed_catalog =
        ModelCatalog::new([ModelProfile::new("mock", "mock-routed", 8_192, 1_024, 100)])
            .expect("routing catalog");
    let (routed, _) = finish_run(
        Client::new(Agent::new())
            .query_cancellable(
                "routed",
                AgentOptions {
                    routing: Some(RoutingOptions::new(
                        routed_catalog,
                        RouteRequest::automatic(RouteObjective::Quality),
                    )),
                    ..AgentOptions::default()
                },
            )
            .expect("routed run"),
    )
    .await;

    let (media_type, source_kind) = match &generated.messages[0].content[1] {
        ContentBlock::Media { media_type, source } => (
            media_type.clone(),
            match source {
                MediaSource::Base64 { .. } => "base64",
                MediaSource::Url { .. } => "url",
            },
        ),
        other => panic!("expected preserved media input, got {other:?}"),
    };
    json!({
        "media_input": {
            "media_type": media_type,
            "source_kind": source_kind,
            "text_roles": generated.messages.iter().take(1)
                .map(|message| enum_name(message.role))
                .collect::<Vec<_>>(),
        },
        "routing": { "model_attempts": routed.model_attempts },
        "structured": {
            "fidelity": enum_name(object.fidelity),
            "status": object.value["status"],
        },
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    emit("governance", governance_facts().await)?;
    emit("structured", structured_facts().await)?;
    emit("run_options", run_options_facts().await)?;
    emit("state", state_facts().await)?;
    emit("orchestration", orchestration_facts().await)?;
    emit("builtins", builtins_facts().await?)?;
    emit("input", input_facts().await)?;
    Ok(())
}
