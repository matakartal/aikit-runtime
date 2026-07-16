//! Real-provider smoke test. It is ignored by default and requires explicit credentials *and*
//! model ids, so normal CI stays keyless and never pretends a mock server is a live proof.
//!
//! `AIKIT_LIVE_SMOKE=1` preserves the inexpensive, configured-provider text probe.
//! `AIKIT_LIVE_SMOKE_FULL=1` upgrades that probe to a fail-closed four-provider contract:
//! text, structured output, governance denial, and an allowed two-request tool round-trip.

use aikit::{
    run_agent, Agent, Governance, HookDispatcher, Message, ObjectOptions, PermissionEngine,
    PermissionMode, Provider, ProviderRequest, Rule, RunConfig, RunTerminalStatus, StreamDelta,
    ToolExecutor, ToolSpec,
};
use async_trait::async_trait;
use futures::{stream::BoxStream, StreamExt};
use serde_json::{json, Map, Value};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

const TOOL_NAME: &str = "aikit_live_probe";

struct SmokeTarget {
    provider: &'static str,
    key_vars: &'static [&'static str],
    model_var: &'static str,
}

struct ConfiguredTarget<'a> {
    target: &'a SmokeTarget,
    key: String,
    model: String,
}

const TARGETS: &[SmokeTarget] = &[
    SmokeTarget {
        provider: "anthropic",
        key_vars: &["ANTHROPIC_API_KEY"],
        model_var: "AIKIT_SMOKE_ANTHROPIC_MODEL",
    },
    SmokeTarget {
        provider: "openai",
        key_vars: &["OPENAI_API_KEY"],
        model_var: "AIKIT_SMOKE_OPENAI_MODEL",
    },
    SmokeTarget {
        provider: "deepseek",
        key_vars: &["DEEPSEEK_API_KEY"],
        model_var: "AIKIT_SMOKE_DEEPSEEK_MODEL",
    },
    SmokeTarget {
        provider: "google",
        key_vars: &["GEMINI_API_KEY", "GOOGLE_API_KEY"],
        model_var: "AIKIT_SMOKE_GOOGLE_MODEL",
    },
];

/// A live-provider wrapper used only by this ignored harness. It forces the advertised host tool
/// on the first request, then disables tools on later requests. That makes the second request
/// deterministic while preserving the exact assistant tool-call/reasoning + tool-result history
/// each provider must accept on replay.
struct FirstTurnToolChoiceProvider {
    inner: Arc<dyn Provider>,
    first_options: Map<String, Value>,
    later_options: Map<String, Value>,
    requests: AtomicUsize,
}

impl FirstTurnToolChoiceProvider {
    fn new(inner: Arc<dyn Provider>, provider: &str) -> Self {
        let (first_options, later_options) = tool_choice_options(provider);
        Self {
            inner,
            first_options,
            later_options,
            requests: AtomicUsize::new(0),
        }
    }

    fn request_count(&self) -> usize {
        self.requests.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl Provider for FirstTurnToolChoiceProvider {
    fn name(&self) -> &str {
        self.inner.name()
    }

    async fn stream(
        &self,
        mut request: ProviderRequest,
    ) -> aikit::Result<BoxStream<'static, StreamDelta>> {
        let request_index = self.requests.fetch_add(1, Ordering::SeqCst);
        let options = if request_index == 0 {
            &self.first_options
        } else {
            &self.later_options
        };
        request.options.extend(options.clone());
        self.inner.stream(request).await
    }
}

#[derive(Default)]
struct CountingExecutor {
    calls: AtomicUsize,
}

impl CountingExecutor {
    fn call_count(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl ToolExecutor for CountingExecutor {
    async fn execute(&self, name: &str, input: Value) -> aikit::Result<String> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(json!({ "tool": name, "input": input, "status": "ok" }).to_string())
    }
}

#[tokio::test]
#[ignore = "requires AIKIT_LIVE_SMOKE=1, real API keys, model ids, network, and billable calls"]
async fn configured_real_providers_complete_the_declared_live_contract() {
    assert_eq!(
        std::env::var("AIKIT_LIVE_SMOKE").as_deref(),
        Ok("1"),
        "set AIKIT_LIVE_SMOKE=1 to acknowledge real network/billable calls"
    );

    let full = std::env::var("AIKIT_LIVE_SMOKE_FULL").as_deref() == Ok("1");
    let configured = configured_targets(full);

    for configured_target in configured {
        let target = configured_target.target;
        let mut agent = Agent::new();
        agent
            .add_key(&configured_target.key, Some(target.provider), None)
            .unwrap_or_else(|error| panic!("{} credential setup failed: {error}", target.provider));

        assert_text(&agent, target, &configured_target.model).await;

        if full {
            assert_object(&agent, target, &configured_target.model).await;
            assert_tool_round_trip(&agent, target, &configured_target.model, true).await;
            assert_tool_round_trip(&agent, target, &configured_target.model, false).await;
        }
    }
}

/// Resolve every environment requirement before the first billable request. FULL mode refuses a
/// partial matrix; the legacy mode still exercises whichever providers are completely configured.
fn configured_targets(full: bool) -> Vec<ConfiguredTarget<'static>> {
    let mut configured = Vec::new();
    let mut missing = Vec::new();

    for target in TARGETS {
        let key = target.key_vars.iter().find_map(|name| non_empty_env(name));
        let model = non_empty_env(target.model_var);

        if full {
            if key.is_none() {
                missing.push(format!(
                    "{} key ({})",
                    target.provider,
                    target.key_vars.join(" or ")
                ));
            }
            if model.is_none() {
                missing.push(format!("{} model ({})", target.provider, target.model_var));
            }
        }

        match (key, model) {
            (Some(key), Some(model)) => configured.push(ConfiguredTarget { target, key, model }),
            (Some(_), None) if !full => panic!(
                "{} key is set, so {} must name the live model to smoke-test",
                target.provider, target.model_var
            ),
            _ => {}
        }
    }

    assert!(
        missing.is_empty(),
        "AIKIT_LIVE_SMOKE_FULL=1 requires the complete four-provider matrix before any live call; missing: {}",
        missing.join(", ")
    );
    assert!(
        !configured.is_empty(),
        "no real provider key+model pair was configured; refusing to report a fake live-smoke success"
    );
    if full {
        assert_eq!(
            configured.len(),
            TARGETS.len(),
            "FULL live smoke must resolve all four providers"
        );
    }
    configured
}

fn non_empty_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
}

async fn assert_text(agent: &Agent, target: &SmokeTarget, model: &str) {
    let generated = agent
        .generate_text("Reply briefly with the token AIKIT_SMOKE_OK.", model, 64)
        .await
        .unwrap_or_else(|error| panic!("{} text smoke failed: {error}", target.provider));
    assert!(
        !generated.text.trim().is_empty(),
        "{} returned an empty successful text response",
        target.provider
    );
}

async fn assert_object(agent: &Agent, target: &SmokeTarget, model: &str) {
    let schema = json!({
        "type": "object",
        "properties": {
            "status": { "type": "string", "enum": ["ok"] }
        },
        "required": ["status"],
        "additionalProperties": false
    });
    let options = ObjectOptions {
        max_retries: 1,
        max_tokens: 128,
        name: "aikit_live_object".into(),
        ..ObjectOptions::default()
    };
    let generated = agent
        .generate_object(
            "Return exactly one object whose status field is the string ok.",
            schema,
            model,
            options,
        )
        .await
        .unwrap_or_else(|error| panic!("{} object smoke failed: {error}", target.provider));
    assert_eq!(
        generated.value.get("status").and_then(Value::as_str),
        Some("ok"),
        "{} returned an unexpected structured value",
        target.provider
    );
}

async fn assert_tool_round_trip(agent: &Agent, target: &SmokeTarget, model: &str, deny: bool) {
    let live_provider = agent
        .provider_for(model)
        .unwrap_or_else(|error| panic!("{} provider setup failed: {error}", target.provider));
    let provider = Arc::new(FirstTurnToolChoiceProvider::new(
        live_provider,
        target.provider,
    ));
    let executor = Arc::new(CountingExecutor::default());
    let recorder = aikit::RunRecorder::default();
    let mut config = RunConfig::new(
        model,
        vec![Message::user(
            "Call aikit_live_probe with token AIKIT_LIVE_TOOL. After receiving its result, reply briefly.",
        )],
    );
    config.tools = vec![tool_spec()];
    config.max_tokens = 128;
    config.max_turns = 2;
    config.recorder = recorder.clone();
    if deny {
        config.governance = Governance::new(
            PermissionEngine::with_rules(
                PermissionMode::Allow,
                vec![Rule::deny(TOOL_NAME).named("live-smoke-deny")],
            ),
            HookDispatcher::new(),
        );
    }

    let stream = run_agent(provider.clone(), executor.clone(), config);
    futures::pin_mut!(stream);
    let mut saw_target_call = false;
    let mut saw_expected_result = false;
    let mut errors = Vec::new();
    while let Some(delta) = stream.next().await {
        match delta {
            StreamDelta::ToolCallStart { name, .. } if name == TOOL_NAME => {
                saw_target_call = true;
            }
            StreamDelta::ToolResult { is_error, .. } if is_error == deny => {
                saw_expected_result = true;
            }
            StreamDelta::Error { message, .. } => errors.push(message),
            _ => {}
        }
    }

    let mode = if deny { "denied" } else { "allowed" };
    assert!(
        errors.is_empty(),
        "{} {mode} tool round-trip failed: {}",
        target.provider,
        errors.join(" | ")
    );
    assert!(
        saw_target_call,
        "{} did not emit the forced {TOOL_NAME} call",
        target.provider
    );
    assert!(
        saw_expected_result,
        "{} did not emit the expected {mode} tool result",
        target.provider
    );
    assert_eq!(
        provider.request_count(),
        2,
        "{} did not complete the second-request replay contract",
        target.provider
    );
    assert_eq!(
        recorder.outcome().terminal_status,
        RunTerminalStatus::Completed,
        "{} {mode} tool round-trip did not complete cleanly",
        target.provider
    );
    if deny {
        assert_eq!(
            executor.call_count(),
            0,
            "{} governance denial reached the host executor",
            target.provider
        );
    } else {
        assert_eq!(
            executor.call_count(),
            1,
            "{} allowed tool should execute exactly once before replay",
            target.provider
        );
    }
}

fn tool_spec() -> ToolSpec {
    ToolSpec {
        name: TOOL_NAME.into(),
        description: "Return a deterministic live-smoke marker for the supplied token.".into(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "token": { "type": "string" }
            },
            "required": ["token"],
            "additionalProperties": false
        }),
    }
}

fn tool_choice_options(provider: &str) -> (Map<String, Value>, Map<String, Value>) {
    let (first, later) = match provider {
        "anthropic" => (
            json!({ "tool_choice": { "type": "tool", "name": TOOL_NAME } }),
            json!({ "tool_choice": { "type": "none" } }),
        ),
        "openai" => (
            json!({ "tool_choice": { "type": "function", "name": TOOL_NAME } }),
            json!({ "tool_choice": "none" }),
        ),
        "deepseek" => (
            json!({
                "tool_choice": { "type": "function", "function": { "name": TOOL_NAME } }
            }),
            json!({ "tool_choice": "none" }),
        ),
        "google" => (
            json!({
                "toolConfig": {
                    "functionCallingConfig": {
                        "mode": "ANY",
                        "allowedFunctionNames": [TOOL_NAME]
                    }
                }
            }),
            json!({
                "toolConfig": { "functionCallingConfig": { "mode": "NONE" } }
            }),
        ),
        other => panic!("no live tool-choice contract for provider '{other}'"),
    };
    (
        first.as_object().expect("first options object").clone(),
        later.as_object().expect("later options object").clone(),
    )
}
