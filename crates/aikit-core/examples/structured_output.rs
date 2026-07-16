//! `generate_object` — typed structured output with an **honest** per-provider fidelity grade.
//! No API key required: two tiny mock providers stand in for a constrained-decoding provider and a
//! forced-tool-call provider, so you can see that aikit reports *which* mechanism produced the
//! object rather than pretending they are equivalent.
//!
//! Run: `cargo run -p aikit-core --example structured_output`

use aikit_core::capabilities::FidelityGrade;
use aikit_core::providers::{Provider, ProviderRequest};
use aikit_core::types::StreamDelta;
use aikit_core::{generate_object, ObjectOptions, Result};
use async_trait::async_trait;
use futures::stream::BoxStream;
use serde_json::{json, Value};
use std::sync::Arc;

/// Stands in for OpenAI/Gemini: returns the object as assistant text (constrained decoding on a
/// real provider guarantees it parses; here we just hand back valid JSON).
struct ConstrainedMock(String);
#[async_trait]
impl Provider for ConstrainedMock {
    fn name(&self) -> &str {
        "constrained-mock"
    }
    async fn stream(&self, _req: ProviderRequest) -> Result<BoxStream<'static, StreamDelta>> {
        let deltas = vec![
            StreamDelta::MessageStart { model: "m".into() },
            StreamDelta::TextDelta {
                text: self.0.clone(),
            },
            StreamDelta::MessageStop {
                stop_reason: "end_turn".into(),
            },
        ];
        Ok(Box::pin(futures::stream::iter(deltas)))
    }
}

/// Stands in for Anthropic: coerces the object through a forced tool call (schema-shaped, not
/// grammar-constrained — the honest grade is `ForcedToolCall`).
struct ForcedToolMock {
    name: String,
    input: Value,
}
#[async_trait]
impl Provider for ForcedToolMock {
    fn name(&self) -> &str {
        "forced-tool-mock"
    }
    async fn stream(&self, _req: ProviderRequest) -> Result<BoxStream<'static, StreamDelta>> {
        let deltas = vec![
            StreamDelta::MessageStart { model: "m".into() },
            StreamDelta::ToolCallStart {
                id: "c1".into(),
                name: self.name.clone(),
            },
            StreamDelta::ToolCallInput {
                id: "c1".into(),
                input: self.input.clone(),
            },
            StreamDelta::MessageStop {
                stop_reason: "tool_use".into(),
            },
        ];
        Ok(Box::pin(futures::stream::iter(deltas)))
    }
}

#[tokio::main]
async fn main() {
    // The target shape (Pydantic/Zod/serde would produce this JSON schema).
    let schema = json!({
        "type": "object",
        "required": ["total", "currency"],
        "properties": {
            "total":    { "type": "number" },
            "currency": { "type": "string" }
        }
    });
    let opts = ObjectOptions::default();

    // 1. A "constrained" provider (graded NativeConstrained). aikit asks via json_schema and reads
    //    the object from text.
    let openai: Arc<dyn Provider> = Arc::new(ConstrainedMock(
        r#"{"total": 42.50, "currency": "EUR"}"#.into(),
    ));
    let r = generate_object(
        openai,
        "openai",
        FidelityGrade::NativeConstrained,
        "gpt-x",
        "Extract the invoice total and currency.",
        &schema,
        &opts,
    )
    .await
    .unwrap();
    println!(
        "openai    -> value={}  fidelity={:?}  attempts={}",
        r.value, r.fidelity, r.attempts
    );

    // 2. A forced-tool-call provider (graded ForcedToolCall). aikit forces a tool whose input
    //    schema IS the target and reads the object from the tool call.
    let anthropic: Arc<dyn Provider> = Arc::new(ForcedToolMock {
        name: opts.name.clone(),
        input: json!({ "total": 42.50, "currency": "EUR" }),
    });
    let r = generate_object(
        anthropic,
        "anthropic",
        FidelityGrade::ForcedToolCall,
        "claude-opus-4-8",
        "Extract the invoice total and currency.",
        &schema,
        &opts,
    )
    .await
    .unwrap();
    println!(
        "anthropic -> value={}  fidelity={:?}  attempts={}",
        r.value, r.fidelity, r.attempts
    );

    println!(
        "\n✅ Same typed object from both — but the fidelity grade tells you HOW it was produced.\n\
         NativeConstrained (grammar-constrained) is a stronger guarantee than ForcedToolCall;\n\
         aikit reports the difference instead of silently pretending they are equal."
    );
}
