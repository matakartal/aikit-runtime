//! Defense in depth — aikit's governance layers, composed. Each layer is independent; stacked they
//! form the bundle no single multi-provider SDK ships. Deterministic and keyless (no API key).
//!
//! Run: cargo run -p aikit-runtime-core --example defense_in_depth

use aikit_core::{
    GuardedExecutor, GuardrailChain, HeuristicRiskScorer, OffPromptExecutor, OffPromptStore,
    PolicySpec, ReliabilityPolicy, RiskScorer, RunProgress, SecretRedactor, ToolExecutor,
    ToolRequirement,
};
use async_trait::async_trait;
use serde_json::{json, Value};
use std::sync::Arc;

/// A tool whose output both leaks a secret and is large — the worst case for a governed stack.
struct RawTool;
#[async_trait]
impl ToolExecutor for RawTool {
    async fn execute(&self, _name: &str, _input: Value) -> aikit_core::Result<String> {
        let mut out = String::from("ANTHROPIC_API_KEY=sk-ant-api03-Zx9_abcDEF0123456789ghijkl\n");
        out.push_str(&"telemetry line\n".repeat(400)); // large
        Ok(out)
    }
}

#[tokio::main]
async fn main() {
    // 1. DECISION LAYER — three independent judgments on a proposed `Bash(rm -rf /)` call.
    let engine = PolicySpec::from_json(r#"{ "deny": ["Bash(rm -rf *)"] }"#)
        .unwrap()
        .build()
        .unwrap();
    let scorer = HeuristicRiskScorer::default();
    let reliability = ReliabilityPolicy::new(vec![
        ToolRequirement::for_tool("deploy").only_after(["test"])
    ]);

    let call = json!({ "command": "rm -rf /" });
    println!("1. Decision layer — Bash(rm -rf /):");
    println!("     permission : {:?}", engine.evaluate("Bash", &call));
    println!("     risk       : {:?}", scorer.score("Bash", &call));
    println!(
        "     reliability(deploy before test): {:?}",
        reliability.check("deploy", &RunProgress::new())
    );

    // 2. EXECUTION LAYER — wrap the leaky tool: redact secrets, THEN off-prompt the bulk. Ordering
    //    matters: the guardrail runs inside, so what off-prompt stores is already redacted.
    let store = Arc::new(OffPromptStore::new());
    let guarded = Arc::new(GuardedExecutor::new(
        Arc::new(RawTool),
        Arc::new(GuardrailChain::default()),
        Arc::new(GuardrailChain::new(vec![Arc::new(
            SecretRedactor::default(),
        )])),
    ));
    let stack = OffPromptExecutor::new(guarded, store.clone(), 500);

    let result = stack.execute("Read", json!({})).await.unwrap();
    let shown: String = result.replace('\n', " ").chars().take(150).collect();
    println!("\n2. Execution layer (guardrail + off-prompt) returns:\n     {shown}");
    let id = result
        .split_once("id=")
        .and_then(|(_, rest)| rest.split_once(',').map(|(id, _)| id))
        .expect("off-prompt output must return its opaque reference id");
    let stored = store
        .retrieve(id)
        .expect("the returned off-prompt reference must resolve");
    println!(
        "     secret redacted before it was stored off-prompt: {}",
        !stored.contains("sk-ant-")
    );

    println!(
        "\n✅ Defense in depth: permission + risk + reliability DECIDE; guardrail + off-prompt \
         SANITIZE; capability requests + plan mode keep a human in the loop. Each layer is \
         independent, all provider-neutral, from one Rust core."
    );
}
