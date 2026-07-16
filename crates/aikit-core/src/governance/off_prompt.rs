//! Off-prompt tool output — keep bulky or sensitive tool results out of the model's context.
//!
//! Some tool outputs shouldn't sit in the transcript: a 2 MB file dump wastes the context window,
//! and a result full of secrets shouldn't be replayed to the model every turn. Off-prompt storage
//! (Griptape's "off-prompt by default" idea) stashes such output and hands the agent a compact
//! reference plus a short preview; the agent can pull the full content back with a `retrieve_output`
//! call only when it actually needs it.
//!
//! [`OffPromptExecutor`] wraps any [`ToolExecutor`], so it stacks with the guardrail and capability
//! layers. It stores an output only when it exceeds a size threshold; small outputs pass through
//! untouched.

use crate::error::{AikitError, Result};
use crate::tools::ToolExecutor;
use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// The tool the agent calls to pull a stored output back into context.
pub const RETRIEVE_TOOL: &str = "retrieve_output";

/// A keyed store of off-prompt tool outputs. Ids are deterministic (`out-1`, `out-2`, …).
#[derive(Default)]
pub struct OffPromptStore {
    entries: Mutex<HashMap<String, String>>,
    next_id: AtomicU64,
}

impl OffPromptStore {
    pub fn new() -> Self {
        OffPromptStore::default()
    }

    /// Store `content` and return its reference id.
    pub fn store(&self, content: String) -> String {
        let id = format!("out-{}", self.next_id.fetch_add(1, Ordering::SeqCst) + 1);
        self.entries
            .lock()
            .expect("off-prompt lock")
            .insert(id.clone(), content);
        id
    }

    /// Retrieve a previously stored output.
    pub fn retrieve(&self, id: &str) -> Option<String> {
        self.entries
            .lock()
            .expect("off-prompt lock")
            .get(id)
            .cloned()
    }

    /// Number of stored outputs.
    pub fn len(&self) -> usize {
        self.entries.lock().expect("off-prompt lock").len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// The first `n` characters of `s` (never splitting a multi-byte char).
fn preview(s: &str, n: usize) -> String {
    s.chars().take(n).collect()
}

/// Wraps a [`ToolExecutor`]: outputs larger than `max_inline_bytes` are stored off-prompt and
/// replaced with a reference + preview. Also answers the built-in `retrieve_output` tool.
pub struct OffPromptExecutor {
    inner: Arc<dyn ToolExecutor>,
    store: Arc<OffPromptStore>,
    max_inline_bytes: usize,
}

impl OffPromptExecutor {
    /// Wrap `inner`, storing any tool output longer than `max_inline_bytes`. The `store` is shared
    /// so the host can read stored outputs directly too.
    pub fn new(
        inner: Arc<dyn ToolExecutor>,
        store: Arc<OffPromptStore>,
        max_inline_bytes: usize,
    ) -> Self {
        OffPromptExecutor {
            inner,
            store,
            max_inline_bytes,
        }
    }

    pub fn store(&self) -> &Arc<OffPromptStore> {
        &self.store
    }
}

#[async_trait]
impl ToolExecutor for OffPromptExecutor {
    async fn execute(&self, name: &str, input: Value) -> Result<String> {
        // The agent pulling a stored output back into context.
        if name == RETRIEVE_TOOL {
            let id = input
                .get("id")
                .and_then(Value::as_str)
                .ok_or_else(|| AikitError::ToolExecution("retrieve_output needs an 'id'".into()))?;
            return self
                .store
                .retrieve(id)
                .ok_or_else(|| AikitError::ToolExecution(format!("no off-prompt output '{id}'")));
        }

        let output = self.inner.execute(name, input).await?;
        if output.len() <= self.max_inline_bytes {
            return Ok(output);
        }
        let bytes = output.len();
        let head = preview(&output, 200);
        let id = self.store.store(output);
        Ok(format!(
            "[off-prompt: id={id}, {bytes} bytes stored. Call {RETRIEVE_TOOL} with {{\"id\":\"{id}\"}} for the full content.\nPreview: {head}…]"
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    struct FixedOutput(String);
    #[async_trait]
    impl ToolExecutor for FixedOutput {
        async fn execute(&self, _name: &str, _input: Value) -> Result<String> {
            Ok(self.0.clone())
        }
    }

    fn executor(output: &str, max: usize) -> OffPromptExecutor {
        OffPromptExecutor::new(
            Arc::new(FixedOutput(output.to_string())),
            Arc::new(OffPromptStore::new()),
            max,
        )
    }

    #[tokio::test]
    async fn small_output_passes_through_inline() {
        let e = executor("short result", 1000);
        let out = e.execute("Read", json!({})).await.unwrap();
        assert_eq!(out, "short result");
        assert!(e.store().is_empty(), "small output must not be stored");
    }

    #[tokio::test]
    async fn large_output_is_stored_and_referenced() {
        let big = "x".repeat(5000);
        let e = executor(&big, 100);
        let out = e.execute("Read", json!({})).await.unwrap();
        // The full content is NOT in the returned string.
        assert!(!out.contains(&big));
        assert!(out.contains("off-prompt: id=out-1"));
        assert!(out.contains("5000 bytes"));
        assert!(out.contains("Preview:"));
        assert_eq!(e.store().len(), 1);
        // And the full content is retrievable from the store.
        assert_eq!(e.store().retrieve("out-1").as_deref(), Some(big.as_str()));
    }

    #[tokio::test]
    async fn retrieve_tool_returns_the_full_content() {
        let big = "y".repeat(2000);
        let e = executor(&big, 100);
        // First a large output gets stored under out-1.
        e.execute("Read", json!({})).await.unwrap();
        // The agent retrieves it.
        let full = e
            .execute(RETRIEVE_TOOL, json!({ "id": "out-1" }))
            .await
            .unwrap();
        assert_eq!(full, big);
    }

    #[tokio::test]
    async fn retrieving_an_unknown_id_errors() {
        let e = executor("x", 100);
        assert!(e
            .execute(RETRIEVE_TOOL, json!({ "id": "out-999" }))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn preview_does_not_split_multibyte_chars() {
        // A large output of multi-byte chars — the preview must be valid UTF-8 (no panic/garbage).
        let big = "ç".repeat(3000);
        let e = executor(&big, 100);
        let out = e.execute("Read", json!({})).await.unwrap();
        assert!(out.contains("Preview:"));
    }
}
