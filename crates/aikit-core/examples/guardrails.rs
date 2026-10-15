//! Content-safety guardrails at the tool boundary — a tool that returns secrets/PII must not hand
//! them to the model or the logs. Deterministic and keyless: no API key, no network.
//!
//! What it demonstrates:
//! 1. An *output* `GuardrailChain` (`SecretRedactor` + `PiiRedactor`) rewrites a tool result so
//!    an API key, an email address and a card number reach the model as `[REDACTED:<kind>]`.
//! 2. An *input* chain (`RegexBlocklist`) refuses a destructive command before the tool runs.
//!
//! `GuardedExecutor` wraps any `ToolExecutor`, so in a real agent you wrap your tool executor with
//! it and hand the result to the run. The built-in redactors are pattern-based: they catch known
//! formats, not every possible secret. For semantic detection, `McpGuardrail` can delegate to an
//! external MCP safety server and fails closed if that server errors.
//!
//! Run: `cargo run -p aikit-runtime-core --example guardrails`
//!
//! Expected: step 1 prints the line with three `[REDACTED:...]` markers; step 2 prints
//! `blocked: ... blocked by rule 'destructive'`.

use aikit_core::governance::guardrail::{
    GuardedExecutor, GuardrailChain, PiiRedactor, RegexBlocklist, SecretRedactor,
};
use aikit_core::tools::ToolExecutor;
use aikit_core::Result;
use async_trait::async_trait;
use serde_json::json;
use std::sync::Arc;

/// A stand-in for a real tool (e.g. `Read`) whose output happens to contain secrets + PII — exactly
/// the leak a guardrail must catch before it reaches the model or the transcript.
struct LeakyTool;
#[async_trait]
impl ToolExecutor for LeakyTool {
    async fn execute(&self, _name: &str, _input: serde_json::Value) -> Result<String> {
        Ok(
            "config loaded: ANTHROPIC_API_KEY=sk-ant-api03-Zx9_abcDEF0123456789ghijkl \
            owner=ada@finqt.com card=4111 1111 1111 1111"
                .to_string(),
        )
    }
}

#[tokio::main]
async fn main() {
    // Output guard: strip secrets AND PII from whatever a tool returns.
    let output_guard = Arc::new(GuardrailChain::new(vec![
        Arc::new(SecretRedactor::default()),
        Arc::new(PiiRedactor::default()),
    ]));
    // Input guard: block a destructive command before the tool ever runs.
    let input_guard = Arc::new(GuardrailChain::new(vec![Arc::new(
        RegexBlocklist::new("dangerous_input", [(r"(?i)rm\s+-rf", "destructive")]).unwrap(),
    )]));

    let guarded = GuardedExecutor::new(Arc::new(LeakyTool), input_guard, output_guard);

    println!(
        "1. A tool returns secrets + PII → the output guard redacts before the model sees it:"
    );
    let out = guarded
        .execute("Read", json!({ "path": "config.env" }))
        .await
        .unwrap();
    println!("   -> {out}");
    assert!(!out.contains("sk-ant-"), "API key leaked!");
    assert!(!out.contains("ada@finqt.com"), "email leaked!");
    assert!(!out.contains("4111 1111 1111 1111"), "card leaked!");

    println!("\n2. A dangerous tool input is blocked before execution:");
    match guarded
        .execute("Bash", json!({ "command": "rm -rf /" }))
        .await
    {
        Ok(o) => panic!("the destructive input should have been blocked, got: {o}"),
        Err(e) => println!("   -> blocked: {e}"),
    }

    println!(
        "\nOK: guardrails redacted the tool output and blocked the destructive input — deterministic,\n\
         keyless, and composable with the permission engine + sandbox. For semantic injection/PII\n\
         detection, plug an external MCP safety server in via McpGuardrail (fails closed on error)."
    );
}
