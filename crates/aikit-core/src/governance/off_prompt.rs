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
use std::collections::hash_map::RandomState;
use std::collections::{HashMap, VecDeque};
use std::hash::{BuildHasher, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// The tool the agent calls to pull a stored output back into context.
pub const RETRIEVE_TOOL: &str = "retrieve_output";

/// Default maximum number of retained off-prompt outputs.
pub const DEFAULT_MAX_STORED_OUTPUTS: usize = 128;
/// Default aggregate payload budget for retained off-prompt outputs (16 MiB).
pub const DEFAULT_MAX_STORED_BYTES: usize = 16 * 1024 * 1024;
/// Default lifetime of an off-prompt output before it is removed.
pub const DEFAULT_OFF_PROMPT_TTL: Duration = Duration::from_secs(60 * 60);

/// Canonical schema for the tool used to retrieve an off-prompt output. Hosts must advertise this
/// spec alongside the wrapped executor; the runtime intentionally rejects unadvertised tools.
pub fn retrieve_output_tool() -> crate::types::ToolSpec {
    crate::types::ToolSpec::new(
        RETRIEVE_TOOL,
        "Retrieve the complete content of a previously stored off-prompt tool output by id.",
        serde_json::json!({
            "type": "object",
            "properties": {
                "id": { "type": "string", "minLength": 1 }
            },
            "required": ["id"],
            "additionalProperties": false
        }),
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ScopeId([u64; 2]);

const HOST_SCOPE: ScopeId = ScopeId([0, 0]);

struct StoredOutput {
    content: String,
    scope: ScopeId,
    inserted_at: Instant,
}

#[derive(Default)]
struct StoreState {
    entries: HashMap<String, StoredOutput>,
    insertion_order: VecDeque<String>,
    stored_bytes: usize,
}

/// Generates opaque 128-bit values using two independently keyed standard-library hashers.
/// `RandomState` is process-randomized; the monotonic input prevents repeats within this store.
struct OpaqueIdGenerator {
    first: RandomState,
    second: RandomState,
    counter: AtomicU64,
}

impl Default for OpaqueIdGenerator {
    fn default() -> Self {
        Self {
            first: RandomState::new(),
            second: RandomState::new(),
            counter: AtomicU64::new(0),
        }
    }
}

impl OpaqueIdGenerator {
    fn next(&self) -> [u64; 2] {
        let counter = self.counter.fetch_add(1, Ordering::SeqCst);
        let hash = |state: &RandomState, domain: u64| {
            let mut hasher = state.build_hasher();
            hasher.write_u64(domain);
            hasher.write_u64(counter);
            hasher.finish()
        };
        [hash(&self.first, 0), hash(&self.second, 1)]
    }
}

fn secure_handle() -> Result<String> {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).map_err(|error| {
        AikitError::ToolExecution(format!(
            "could not generate an off-prompt output handle: {error}"
        ))
    })?;
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut handle = String::with_capacity(36);
    handle.push_str("out-");
    for byte in bytes {
        handle.push(HEX[(byte >> 4) as usize] as char);
        handle.push(HEX[(byte & 0x0f) as usize] as char);
    }
    Ok(handle)
}

/// A bounded keyed store of off-prompt tool outputs.
///
/// Handles are opaque and each [`OffPromptExecutor`] receives a private retrieval scope. The
/// direct [`retrieve`](Self::retrieve) method is intentionally a trusted-host inspection API;
/// model-triggered retrieval always goes through the executor's scope check.
pub struct OffPromptStore {
    state: Mutex<StoreState>,
    ids: OpaqueIdGenerator,
    max_outputs: usize,
    max_bytes: usize,
    ttl: Duration,
}

impl Default for OffPromptStore {
    fn default() -> Self {
        Self::with_retention(
            DEFAULT_MAX_STORED_OUTPUTS,
            DEFAULT_MAX_STORED_BYTES,
            DEFAULT_OFF_PROMPT_TTL,
        )
    }
}

impl OffPromptStore {
    pub fn new() -> Self {
        OffPromptStore::default()
    }

    /// Create a store with explicit item and aggregate byte limits, using the default TTL.
    pub fn with_limits(max_outputs: usize, max_bytes: usize) -> Self {
        Self::with_retention(max_outputs, max_bytes, DEFAULT_OFF_PROMPT_TTL)
    }

    /// Create a store with explicit retention limits. Zero item/byte limits are raised to one so
    /// the store remains usable; a zero TTL is allowed and expires entries immediately.
    pub fn with_retention(max_outputs: usize, max_bytes: usize, ttl: Duration) -> Self {
        Self {
            state: Mutex::new(StoreState::default()),
            ids: OpaqueIdGenerator::default(),
            max_outputs: max_outputs.max(1),
            max_bytes: max_bytes.max(1),
            ttl,
        }
    }

    /// Store `content` for trusted host use and return its opaque reference id.
    ///
    /// Content larger than the entire byte budget is rejected instead of briefly exceeding the
    /// configured memory boundary or returning a reference that can never be retrieved.
    pub fn store(&self, content: String) -> Result<String> {
        self.store_scoped(HOST_SCOPE, content)
    }

    /// Retrieve a previously stored output as a trusted host.
    pub fn retrieve(&self, id: &str) -> Option<String> {
        let mut state = self.state.lock().expect("off-prompt lock");
        self.purge_expired(&mut state, Instant::now());
        state.entries.get(id).map(|entry| entry.content.clone())
    }

    /// Number of stored outputs.
    pub fn len(&self) -> usize {
        let mut state = self.state.lock().expect("off-prompt lock");
        self.purge_expired(&mut state, Instant::now());
        state.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn new_scope(&self) -> ScopeId {
        ScopeId(self.ids.next())
    }

    fn store_scoped(&self, scope: ScopeId, content: String) -> Result<String> {
        let bytes = content.len();
        if bytes > self.max_bytes {
            return Err(AikitError::ToolExecution(format!(
                "off-prompt output is {bytes} bytes but the store limit is {} bytes",
                self.max_bytes
            )));
        }

        let now = Instant::now();
        let mut state = self.state.lock().expect("off-prompt lock");
        self.purge_expired(&mut state, now);
        while state.entries.len() >= self.max_outputs
            || state.stored_bytes.saturating_add(bytes) > self.max_bytes
        {
            let Some(oldest) = state.insertion_order.pop_front() else {
                break;
            };
            if let Some(removed) = state.entries.remove(&oldest) {
                state.stored_bytes = state.stored_bytes.saturating_sub(removed.content.len());
            }
        }

        let id = loop {
            let candidate = secure_handle()?;
            if !state.entries.contains_key(&candidate) {
                break candidate;
            }
        };
        state.stored_bytes = state.stored_bytes.saturating_add(bytes);
        state.insertion_order.push_back(id.clone());
        state.entries.insert(
            id.clone(),
            StoredOutput {
                content,
                scope,
                inserted_at: now,
            },
        );
        Ok(id)
    }

    fn retrieve_scoped(&self, scope: ScopeId, id: &str) -> Option<String> {
        let mut state = self.state.lock().expect("off-prompt lock");
        self.purge_expired(&mut state, Instant::now());
        state
            .entries
            .get(id)
            .filter(|entry| entry.scope == scope)
            .map(|entry| entry.content.clone())
    }

    fn purge_expired(&self, state: &mut StoreState, now: Instant) {
        while let Some(id) = state.insertion_order.front() {
            let expired = state
                .entries
                .get(id)
                .map(|entry| now.saturating_duration_since(entry.inserted_at) >= self.ttl)
                .unwrap_or(true);
            if !expired {
                break;
            }
            let id = state.insertion_order.pop_front().expect("front exists");
            if let Some(removed) = state.entries.remove(&id) {
                state.stored_bytes = state.stored_bytes.saturating_sub(removed.content.len());
            }
        }
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
    scope: ScopeId,
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
        let scope = store.new_scope();
        OffPromptExecutor {
            inner,
            store,
            scope,
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
                .retrieve_scoped(self.scope, id)
                .ok_or_else(|| AikitError::ToolExecution(format!("no off-prompt output '{id}'")));
        }

        let output = self.inner.execute(name, input).await?;
        if output.len() <= self.max_inline_bytes {
            return Ok(output);
        }
        let bytes = output.len();
        let head = preview(&output, 200);
        let id = self.store.store_scoped(self.scope, output)?;
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

    fn referenced_id(output: &str) -> &str {
        output
            .split_once("id=")
            .and_then(|(_, rest)| rest.split_once(',').map(|(id, _)| id))
            .expect("off-prompt reference id")
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
        let id = referenced_id(&out);
        assert!(id.starts_with("out-"));
        assert_eq!(id.len(), 36, "handle contains 128 opaque bits as hex");
        assert!(out.contains("5000 bytes"));
        assert!(out.contains("Preview:"));
        assert_eq!(e.store().len(), 1);
        // And the full content is retrievable from the store.
        assert_eq!(e.store().retrieve(id).as_deref(), Some(big.as_str()));
    }

    #[tokio::test]
    async fn retrieve_tool_returns_the_full_content() {
        let big = "y".repeat(2000);
        let e = executor(&big, 100);
        let reference = e.execute("Read", json!({})).await.unwrap();
        let id = referenced_id(&reference);
        // The agent retrieves it.
        let full = e.execute(RETRIEVE_TOOL, json!({ "id": id })).await.unwrap();
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

    #[test]
    fn retrieval_tool_has_a_canonical_strict_schema() {
        let spec = retrieve_output_tool();
        assert_eq!(spec.name, RETRIEVE_TOOL);
        assert_eq!(spec.input_schema["required"], json!(["id"]));
        assert_eq!(spec.input_schema["additionalProperties"], json!(false));
    }

    #[tokio::test]
    async fn preview_does_not_split_multibyte_chars() {
        // A large output of multi-byte chars — the preview must be valid UTF-8 (no panic/garbage).
        let big = "ç".repeat(3000);
        let e = executor(&big, 100);
        let out = e.execute("Read", json!({})).await.unwrap();
        assert!(out.contains("Preview:"));
    }

    #[tokio::test]
    async fn executors_sharing_a_store_cannot_retrieve_across_scopes() {
        let store = Arc::new(OffPromptStore::new());
        let first_secret = "first executor secret".repeat(100);
        let second_secret = "second executor secret".repeat(100);
        let first = OffPromptExecutor::new(
            Arc::new(FixedOutput(first_secret.clone())),
            store.clone(),
            1,
        );
        let second = OffPromptExecutor::new(Arc::new(FixedOutput(second_secret)), store, 1);

        let reference = first.execute("Read", json!({})).await.unwrap();
        let first_id = referenced_id(&reference);
        assert_eq!(
            first
                .execute(RETRIEVE_TOOL, json!({"id": first_id}))
                .await
                .unwrap(),
            first_secret
        );
        assert!(second
            .execute(RETRIEVE_TOOL, json!({"id": first_id}))
            .await
            .is_err());
    }

    #[test]
    fn store_evicts_oldest_entries_to_enforce_item_and_byte_limits() {
        let store = OffPromptStore::with_limits(2, 7);
        let first = store.store("aaa".into()).unwrap();
        let second = store.store("bbbb".into()).unwrap();
        let third = store.store("cc".into()).unwrap();

        assert_eq!(store.len(), 2);
        assert!(store.retrieve(&first).is_none());
        assert_eq!(store.retrieve(&second).as_deref(), Some("bbbb"));
        assert_eq!(store.retrieve(&third).as_deref(), Some("cc"));
    }

    #[test]
    fn store_rejects_an_item_larger_than_its_total_byte_budget() {
        let store = OffPromptStore::with_limits(2, 3);
        assert!(store.store("four".into()).is_err());
        assert!(store.is_empty());
    }

    #[test]
    fn expired_entries_are_removed_on_access() {
        let store = OffPromptStore::with_retention(2, 100, Duration::from_millis(1));
        let id = store.store("secret".into()).unwrap();
        std::thread::sleep(Duration::from_millis(5));
        assert!(store.retrieve(&id).is_none());
        assert!(store.is_empty());
    }
}
