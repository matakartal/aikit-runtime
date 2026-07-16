use aikit::{
    run_agent, Agent, Governance, Message, MockProvider, ModelCapability, ModelCatalog,
    ModelProfile, ObjectOptions, PermissionEngine, PermissionMode, RouteObjective, RouteRequest,
    Rule, RunConfig, StreamDelta, ToolExecutor, ToolSpec,
};
use async_trait::async_trait;
use futures::StreamExt;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

struct EchoTool {
    calls: AtomicUsize,
}

#[async_trait]
impl ToolExecutor for EchoTool {
    async fn execute(&self, _name: &str, input: Value) -> aikit::Result<String> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(format!(
            "rows for {}",
            input.get("q").and_then(Value::as_str).unwrap_or("?")
        ))
    }
}

async fn governed_query(executor: Arc<EchoTool>, effect: &str) -> (String, Option<String>) {
    let rule = match effect {
        "allow" => Rule::allow("search_db"),
        "deny" => Rule::deny("search_db"),
        _ => unreachable!(),
    };
    let mut config = RunConfig::new("mock-1", vec![Message::user("veritabanında ara")]);
    config.tools = vec![ToolSpec {
        name: "search_db".into(),
        description: "demo tool".into(),
        input_schema: json!({ "type": "object" }),
    }];
    config.governance = Governance::new(
        PermissionEngine::with_rules(PermissionMode::Allow, vec![rule]),
        Default::default(),
    );
    let stream = run_agent(Arc::new(MockProvider), executor, config);
    futures::pin_mut!(stream);
    let mut result = String::new();
    let mut error = None;
    while let Some(delta) = stream.next().await {
        if let StreamDelta::ToolResult {
            content, is_error, ..
        } = delta
        {
            if is_error {
                error = Some(content);
            } else {
                result = content;
            }
        }
    }
    (result, error)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Explicitly empty host environment keeps the golden transcript deterministic.
    let mut agent = Agent::from_env(Vec::<(String, String)>::new());
    let providers_fresh = agent
        .active_providers()
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>();
    agent.add_key("sk-ant-DEMOKEY", None, None).unwrap();
    agent.add_key("AIzaDEMOKEY", None, None).unwrap();
    let capabilities = agent
        .capabilities()
        .providers
        .into_iter()
        .map(|provider| {
            json!([
                provider.provider,
                serde_json::to_value(provider.structured_output).unwrap()
            ])
        })
        .collect::<Vec<_>>();
    let providers_after_keys = agent
        .active_providers()
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>();
    let ambiguous_rejected = agent.add_key("sk-proj-XXXX", None, None).is_err();
    agent
        .add_key("sk-proj-XXXX", Some("deepseek"), None)
        .unwrap();

    let executor = Arc::new(EchoTool {
        calls: AtomicUsize::new(0),
    });
    let (_, denial) = governed_query(executor.clone(), "deny").await;
    let (tool_echo, _) = governed_query(executor.clone(), "allow").await;
    let denial_message = denial.unwrap_or_default();

    let structured = agent
        .generate_object(
            "Return the invoice status",
            json!({
                "type": "object",
                "required": ["currency", "status"],
                "properties": {
                    "currency": { "type": "string", "enum": ["EUR"] },
                    "status": { "type": "string", "enum": ["ok"] }
                }
            }),
            "mock-structured",
            ObjectOptions::default(),
        )
        .await?;
    let structured_output = json!([
        structured.fidelity,
        structured.attempts,
        structured.value["currency"],
        structured.value["status"]
    ]);

    let generated = agent.generate_text("Say hello", "mock-1", 1024).await?;
    let generated_text = json!([
        generated.text,
        generated.usage.input_tokens,
        generated.usage.output_tokens,
        generated.stop_reason
    ]);
    let stream = agent.stream_text("Say hello", "mock-1", 1024)?;
    futures::pin_mut!(stream);
    let mut streamed = String::new();
    let mut streamed_output_tokens = 0;
    let mut streamed_stop = String::new();
    while let Some(delta) = stream.next().await {
        match delta {
            StreamDelta::TextDelta { text } => streamed.push_str(&text),
            StreamDelta::Usage(usage) => streamed_output_tokens += usage.output_tokens,
            StreamDelta::MessageStop { stop_reason } => streamed_stop = stop_reason,
            _ => {}
        }
    }
    let streamed_text = json!([streamed, streamed_output_tokens, streamed_stop]);

    agent.remember("customer_note", json!("Ada prefers EUR"))?;
    let memory_recall = agent
        .recall("EUR", 3)?
        .into_iter()
        .map(|entry| json!([entry.key, entry.value]))
        .collect::<Vec<_>>();

    let mut anthropic = ModelProfile::new("anthropic", "claude-demo", 100_000, 4_096, 80)
        .with_skill("general")
        .with_capability(ModelCapability::ToolUse);
    anthropic.pricing = None;
    let mut google = ModelProfile::new("google", "gemini-demo", 100_000, 4_096, 90)
        .with_skill("general")
        .with_capability(ModelCapability::ToolUse);
    google.pricing = None;
    let catalog = ModelCatalog::new(vec![anthropic, google])?;
    let mut route = RouteRequest::automatic(RouteObjective::Quality);
    route.estimated_input_tokens = 100;
    route.required_output_tokens = 64;
    route.required_skills.insert("general".into());
    route.required_capabilities.insert(ModelCapability::ToolUse);
    let decision = agent.route(&catalog, route)?;
    let route_decision = json!([
        decision.profile.provider,
        decision.profile.model,
        decision.eligible_models
    ]);

    let mut facts = BTreeMap::<String, Value>::new();
    facts.insert("ambiguous_rejected".into(), json!(ambiguous_rejected));
    facts.insert("capabilities".into(), json!(capabilities));
    facts.insert("denial_message".into(), json!(denial_message));
    facts.insert("denial_seen".into(), json!(!denial_message.is_empty()));
    facts.insert("generated_text".into(), generated_text);
    facts.insert("memory_recall".into(), json!(memory_recall));
    facts.insert("providers_after_keys".into(), json!(providers_after_keys));
    facts.insert("providers_fresh".into(), json!(providers_fresh));
    facts.insert("route_decision".into(), route_decision);
    facts.insert("streamed_text".into(), streamed_text);
    facts.insert("structured_output".into(), structured_output);
    facts.insert("tool_echo".into(), json!(tool_echo));
    facts.insert(
        "tool_ran".into(),
        json!(executor.calls.load(Ordering::SeqCst) == 1),
    );
    println!("PARITY_JSON={}", serde_json::to_string(&facts)?);
    Ok(())
}
