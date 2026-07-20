//! The agent-native primary surface.
//!
//! The `Agent` is what makes "drop in a key → get stronger" real: it self-configures from the
//! credentials available to it, exposes what it can currently do via [`Agent::capabilities`],
//! and can be extended at runtime (`add_key`, `add_tool`). The core stays pure — it takes
//! env pairs as input rather than reading the process environment itself, so it is fully
//! testable and the same logic drives the Python/TS bindings.
//!
//! Secrets never leak: the `Debug` impl redacts keys, and `capabilities()` reports provider
//! *names*, never key material.

use crate::capabilities::{CapabilityRegistry, FidelityGrade};
use crate::credentials::{provider_from_env_var, resolve_provider, ResolveError};
use crate::providers::anthropic::AnthropicProvider;
use crate::providers::deepseek::DeepSeekProvider;
use crate::providers::google::GeminiProvider;
use crate::providers::groq::GroqProvider;
use crate::providers::mistral::MistralProvider;
use crate::providers::openai_responses::OpenAiResponsesProvider;
use crate::providers::openrouter::OpenRouterProvider;
use crate::providers::xai::XaiProvider;
use crate::providers::{MockProvider, Provider};
use crate::runtime::{run_agent, RunConfig};
use crate::tools::builtin::{BuiltinTools, ALL_TOOL_NAMES};
use crate::tools::ToolExecutor;
use crate::types::{Message, StreamDelta, ToolSpec, Usage};
use futures::{Stream, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;

/// A self-configuring, runtime-extensible agent handle.
#[derive(Clone)]
pub struct Agent {
    caps: CapabilityRegistry,
    /// provider name → api key (secret; redacted in Debug, never serialized).
    credentials: BTreeMap<String, String>,
    tools: Vec<ToolSpec>,
    memory: Arc<dyn crate::memory::MemoryStore>,
    memory_namespace: String,
}

impl Default for Agent {
    fn default() -> Self {
        Agent {
            caps: CapabilityRegistry::builtin(),
            credentials: BTreeMap::new(),
            tools: Vec::new(),
            memory: Arc::new(crate::memory::InMemoryMemoryStore::default()),
            memory_namespace: "default".into(),
        }
    }
}

impl Agent {
    /// A fresh agent with the built-in capability registry and no credentials yet.
    pub fn new() -> Self {
        Agent::default()
    }

    /// Self-configure from environment pairs (name, value). Every non-empty var that maps to a
    /// known provider activates it — the "key gir → güçlen" flow. Unknown vars and blank
    /// credentials are ignored. If both Google aliases are present, `GOOGLE_API_KEY` follows the
    /// official client-library precedence regardless of iterator order. The core stays pure by
    /// taking the pairs as input.
    pub fn from_env<I, S>(vars: I) -> Self
    where
        I: IntoIterator<Item = (S, S)>,
        S: AsRef<str>,
    {
        let mut agent = Agent::new();
        let mut discovered = BTreeMap::<String, (u8, String)>::new();
        for (name, value) in vars {
            let name = name.as_ref();
            let value = value.as_ref().trim();
            let Some(provider) = provider_from_env_var(name) else {
                continue;
            };
            if value.is_empty() {
                continue;
            }

            let priority = u8::from(name == "GOOGLE_API_KEY");
            let entry = discovered
                .entry(provider.to_string())
                .or_insert_with(|| (priority, value.to_string()));
            if priority >= entry.0 {
                *entry = (priority, value.to_string());
            }
        }
        agent.credentials = discovered
            .into_iter()
            .map(|(provider, (_, credential))| (provider, credential))
            .collect();
        agent
    }

    /// Convenience for native Rust applications. Bindings call [`Agent::from_env`] at their host
    /// boundary so tests can still inject a deterministic empty environment.
    pub fn from_process_env() -> Self {
        Agent::from_env(std::env::vars_os().filter_map(|(name, value)| {
            Some((name.into_string().ok()?, value.into_string().ok()?))
        }))
    }

    /// Add a credential at runtime, activating its provider. `explicit`/`env_var` disambiguate
    /// the shared `sk-` prefix (OpenAI vs DeepSeek); without either, a bare `sk-` key errors
    /// rather than being mis-routed. Returns the activated provider name.
    pub fn add_key(
        &mut self,
        key: &str,
        explicit: Option<&str>,
        env_var: Option<&str>,
    ) -> Result<&'static str, ResolveError> {
        let key = key.trim();
        let provider = resolve_provider(key, explicit, env_var)?;
        self.credentials
            .insert(provider.to_string(), key.to_string());
        Ok(provider)
    }

    /// Register a tool the agent can use.
    pub fn add_tool(&mut self, tool: ToolSpec) {
        self.tools.push(tool);
    }

    /// Register canonical schemas and return the exact same suite for use as the executor.
    /// Re-registering replaces every built-in schema (including removing a previously enabled
    /// Bash), so a stale or caller-fabricated schema can never be advertised under a built-in
    /// name. Pass the returned `Arc` to a lower-level `Agent` run method; the high-level
    /// [`crate::client::Client::register_builtin_tools`] helper owns that pairing automatically.
    pub fn register_builtin_tools(
        &mut self,
        tools: impl Into<Arc<BuiltinTools>>,
    ) -> Arc<BuiltinTools> {
        let tools = tools.into();
        self.tools.retain(|tool| {
            !ALL_TOOL_NAMES
                .iter()
                .any(|builtin_name| tool.name == *builtin_name)
        });
        self.tools.extend(tools.specs());
        tools
    }

    /// Canonical advertised tool schemas. Orchestrators may clone and narrow this list, never
    /// fabricate a broader child surface from names alone.
    pub fn tool_specs(&self) -> &[ToolSpec] {
        &self.tools
    }

    pub fn with_memory_store(
        mut self,
        store: Arc<dyn crate::memory::MemoryStore>,
        namespace: impl Into<String>,
    ) -> Self {
        self.set_memory_store(store, namespace);
        self
    }

    /// Replace the explicit memory backend and namespace on an existing agent.
    ///
    /// This is the mutable counterpart to [`Self::with_memory_store`]. It is primarily useful at
    /// host-language boundaries, where an already-constructed agent is configured after its
    /// credentials and callbacks have been registered.
    pub fn set_memory_store(
        &mut self,
        store: Arc<dyn crate::memory::MemoryStore>,
        namespace: impl Into<String>,
    ) {
        self.memory = store;
        self.memory_namespace = namespace.into();
    }

    pub fn remember(
        &self,
        key: impl Into<String>,
        value: serde_json::Value,
    ) -> Result<(), AgentError> {
        self.memory
            .put(crate::memory::MemoryEntry::new(
                self.memory_namespace.clone(),
                key,
                value,
            ))
            .map_err(|error| AgentError::Core(crate::error::AikitError::Session(error)))
    }

    /// Optimistic, plane-aware memory update. This is the safe path for concurrent agents;
    /// stale writers receive a typed session error instead of silently winning.
    pub fn remember_cas(
        &self,
        key: impl Into<String>,
        value: serde_json::Value,
        plane: crate::memory::MemoryPlane,
        provenance: crate::memory::MemoryProvenance,
        expected_revision: u64,
    ) -> Result<u64, AgentError> {
        self.memory
            .compare_and_swap(
                crate::memory::MemoryEntry::new(self.memory_namespace.clone(), key, value)
                    .with_plane(plane)
                    .with_provenance(provenance),
                expected_revision,
            )
            .map_err(|error| AgentError::Core(crate::error::AikitError::Conflict(error)))
    }

    pub fn recall(
        &self,
        query: &str,
        limit: usize,
    ) -> Result<Vec<crate::memory::MemoryEntry>, AgentError> {
        self.memory
            .search(&crate::memory::MemoryQuery::new(
                self.memory_namespace.clone(),
                query,
                limit,
            ))
            .map_err(|error| AgentError::Core(crate::error::AikitError::Session(error)))
    }

    /// The active provider names, sorted.
    pub fn active_providers(&self) -> Vec<&str> {
        self.credentials.keys().map(String::as_str).collect()
    }

    /// Whether a provider is currently activated (has a credential).
    pub fn has_provider(&self, provider: &str) -> bool {
        self.credentials.contains_key(provider)
    }

    /// Introspect what the agent can do *right now* — the agent-native "what am I capable of?"
    /// surface. Reports only activated providers, with their honest capability view.
    pub fn capabilities(&self) -> AgentCapabilities {
        let providers = self
            .credentials
            .keys()
            .filter_map(|p| self.caps.get(p))
            .map(|c| ProviderCapabilityView {
                provider: c.provider.clone(),
                supports_reasoning: c.supports_reasoning,
                supports_prompt_cache: c.supports_prompt_cache,
                supports_vision: c.supports_vision,
                supports_citations: c.supports_citations,
                structured_output: c.structured_output,
                structured_output_features: c.structured_output_capabilities(),
            })
            .collect();
        AgentCapabilities {
            providers,
            tools: self.tools.iter().map(|t| t.name.clone()).collect(),
            runtime_features: vec![
                "audit".into(),
                "budget_reservations".into(),
                "cancellation".into(),
                "compaction".into(),
                "durable_execution".into(),
                "eval_trace".into(),
                "human_governed_capabilities".into(),
                "mcp".into(),
                "model_capability_profiles".into(),
                "multimodal_contracts".into(),
                "multimodal_routing".into(),
                "governance_hooks".into(),
                "guardrails".into(),
                "memory".into(),
                "memory_cas".into(),
                "os_containment".into(),
                "protocol_governance".into(),
                "routing".into(),
                "sessions".into(),
                "skills".into(),
                "structured_output".into(),
                "stream_events_v2".into(),
                "subagents".into(),
            ],
        }
    }

    /// Resolve `model` to its provider and construct that provider's live HTTP adapter from the
    /// stored credential. Errors if no key is active for the provider, the model is unknown, or
    /// the provider has no adapter wired yet.
    pub fn provider_for(&self, model: &str) -> Result<Arc<dyn Provider>, AgentError> {
        let provider =
            provider_for_model(model).ok_or_else(|| AgentError::UnknownModel(model.to_string()))?;
        self.provider_for_name(provider)
    }

    /// Construct a provider by canonical name. Router/orchestrator code uses this after selecting
    /// a catalog profile, avoiding fragile model-prefix inference at that boundary.
    pub fn provider_for_name(&self, provider: &str) -> Result<Arc<dyn Provider>, AgentError> {
        if provider == "mock" {
            return Ok(Arc::new(MockProvider));
        }
        let canonical = match provider {
            "anthropic" => "anthropic",
            "deepseek" => "deepseek",
            "openai" => "openai",
            "google" => "google",
            "openrouter" => "openrouter",
            "groq" => "groq",
            "mistral" => "mistral",
            "xai" => "xai",
            _ => return Err(AgentError::NoAdapter("unknown")),
        };
        let key = self
            .credentials
            .get(canonical)
            .ok_or(AgentError::NoCredential(canonical))?
            .clone();
        match canonical {
            "anthropic" => Ok(Arc::new(AnthropicProvider::new(key))),
            "deepseek" => Ok(Arc::new(DeepSeekProvider::new(key))),
            "openai" => Ok(Arc::new(OpenAiResponsesProvider::new(key))),
            "google" => Ok(Arc::new(GeminiProvider::new(key))),
            "openrouter" => Ok(Arc::new(OpenRouterProvider::new(key))),
            "groq" => Ok(Arc::new(GroqProvider::new(key))),
            "mistral" => Ok(Arc::new(MistralProvider::new(key))),
            "xai" => Ok(Arc::new(XaiProvider::new(key))),
            other => Err(AgentError::NoAdapter(other)),
        }
    }

    /// Route using only this agent's currently active providers. Credential values never enter
    /// the catalog or route decision.
    pub fn route(
        &self,
        catalog: &crate::routing::ModelCatalog,
        mut request: crate::routing::RouteRequest,
    ) -> Result<crate::routing::RouteDecision, crate::routing::RouteError> {
        request.active_providers = self
            .active_providers()
            .into_iter()
            .map(str::to_string)
            .collect();
        request.active_providers.insert("mock".into());
        catalog.route(&request)
    }

    /// Build one governed child-agent specification with the same canonical shape bindings use.
    pub fn subtask(
        &self,
        id: impl Into<String>,
        prompt: impl Into<String>,
        route: crate::orchestration::ModelRouteRequirements,
    ) -> crate::orchestration::SubagentSpec {
        crate::orchestration::SubagentSpec::new(id, prompt, route)
    }

    /// Create an orchestrator whose children clone this agent's provider/tool-schema state.
    pub fn orchestrator(
        &self,
        catalog: crate::routing::ModelCatalog,
        executor: Arc<dyn ToolExecutor>,
        session_store: Arc<dyn crate::session::SessionStore>,
        max_parallelism: usize,
    ) -> crate::orchestration::Orchestrator {
        crate::orchestration::Orchestrator::new(
            Arc::new(self.clone()),
            catalog,
            executor,
            session_store,
            max_parallelism,
        )
    }

    /// Run independent governed child agents with bounded parallelism.
    #[allow(clippy::too_many_arguments)]
    pub async fn parallel(
        &self,
        specs: Vec<crate::orchestration::SubagentSpec>,
        catalog: crate::routing::ModelCatalog,
        executor: Arc<dyn ToolExecutor>,
        session_store: Arc<dyn crate::session::SessionStore>,
        max_parallelism: usize,
        parent: &crate::orchestration::ExecutionContext,
    ) -> Vec<crate::orchestration::SubagentResult> {
        self.orchestrator(catalog, executor, session_store, max_parallelism)
            .parallel(specs, parent)
            .await
    }

    /// Run one prompt to completion against `model`, driving the in-process agent loop with the
    /// caller-supplied tool executor (the FFI seam). Returns the canonical delta stream.
    pub fn run(
        &self,
        prompt: impl Into<String>,
        model: &str,
        max_tokens: u64,
        executor: Arc<dyn ToolExecutor>,
    ) -> Result<impl Stream<Item = StreamDelta>, AgentError> {
        self.run_messages(
            vec![Message::user(prompt.into())],
            model,
            max_tokens,
            executor,
        )
    }

    /// Run canonical message history, including multimodal [`crate::types::ContentBlock::Media`]
    /// input,
    /// through the same governed loop used by the string-prompt convenience API.
    pub fn run_messages(
        &self,
        messages: Vec<Message>,
        model: &str,
        max_tokens: u64,
        executor: Arc<dyn ToolExecutor>,
    ) -> Result<impl Stream<Item = StreamDelta>, AgentError> {
        let mut cfg = RunConfig::new(model, messages);
        cfg.max_tokens = max_tokens;
        cfg.tools = self.tools.clone();
        self.run_with_config(cfg, executor)
    }

    /// Ergonomic alias for a canonical delta stream. Uses no host tools; callers that registered
    /// tools should use [`Agent::stream_text_with_executor`] so callbacks can be executed.
    pub fn stream_text(
        &self,
        prompt: impl Into<String>,
        model: &str,
        max_tokens: u64,
    ) -> Result<impl Stream<Item = StreamDelta>, AgentError> {
        self.run(prompt, model, max_tokens, Arc::new(crate::tools::NoTools))
    }

    /// Multimodal/canonical-message form of [`Agent::stream_text`].
    pub fn stream_text_messages(
        &self,
        messages: Vec<Message>,
        model: &str,
        max_tokens: u64,
    ) -> Result<impl Stream<Item = StreamDelta>, AgentError> {
        self.run_messages(messages, model, max_tokens, Arc::new(crate::tools::NoTools))
    }

    pub fn stream_text_with_executor(
        &self,
        prompt: impl Into<String>,
        model: &str,
        max_tokens: u64,
        executor: Arc<dyn ToolExecutor>,
    ) -> Result<impl Stream<Item = StreamDelta>, AgentError> {
        self.run(prompt, model, max_tokens, executor)
    }

    pub fn stream_text_messages_with_executor(
        &self,
        messages: Vec<Message>,
        model: &str,
        max_tokens: u64,
        executor: Arc<dyn ToolExecutor>,
    ) -> Result<impl Stream<Item = StreamDelta>, AgentError> {
        self.run_messages(messages, model, max_tokens, executor)
    }

    /// Drain a complete run and return both its final text projection and canonical transcript.
    /// Streaming errors are reflected as a typed failed terminal outcome rather than a partial
    /// success. Uses no host tools; use `generate_text_with_executor` for registered tools.
    pub async fn generate_text(
        &self,
        prompt: impl Into<String>,
        model: &str,
        max_tokens: u64,
    ) -> Result<GeneratedText, AgentError> {
        self.generate_text_with_executor(prompt, model, max_tokens, Arc::new(crate::tools::NoTools))
            .await
    }

    pub async fn generate_text_with_executor(
        &self,
        prompt: impl Into<String>,
        model: &str,
        max_tokens: u64,
        executor: Arc<dyn ToolExecutor>,
    ) -> Result<GeneratedText, AgentError> {
        self.generate_text_messages_with_executor(
            vec![Message::user(prompt.into())],
            model,
            max_tokens,
            executor,
        )
        .await
    }

    /// Multimodal/canonical-message form of [`Agent::generate_text`].
    pub async fn generate_text_messages(
        &self,
        messages: Vec<Message>,
        model: &str,
        max_tokens: u64,
    ) -> Result<GeneratedText, AgentError> {
        self.generate_text_messages_with_executor(
            messages,
            model,
            max_tokens,
            Arc::new(crate::tools::NoTools),
        )
        .await
    }

    pub async fn generate_text_messages_with_executor(
        &self,
        messages: Vec<Message>,
        model: &str,
        max_tokens: u64,
        executor: Arc<dyn ToolExecutor>,
    ) -> Result<GeneratedText, AgentError> {
        let recorder = crate::session::RunRecorder::default();
        let mut config = RunConfig::new(model, messages);
        config.max_tokens = max_tokens;
        config.recorder = recorder.clone();
        let stream = self.run_with_config(config, executor)?;
        futures::pin_mut!(stream);
        let mut stream_error = None;
        while let Some(delta) = stream.next().await {
            if let StreamDelta::Error { message, info } = delta {
                stream_error = Some((message, info));
            }
        }
        let outcome = recorder.outcome();
        if outcome.terminal_status != crate::session::RunTerminalStatus::Completed {
            return Err(stream_error.map_or_else(
                || {
                    AgentError::Run(
                        outcome
                            .stop_reason
                            .clone()
                            .unwrap_or_else(|| "run failed".into()),
                    )
                },
                |(message, info)| AgentError::Stream {
                    message,
                    info: Box::new(info),
                },
            ));
        }
        Ok(GeneratedText {
            text: outcome.final_text.clone().unwrap_or_default(),
            usage: outcome.usage,
            provider_metadata: outcome.provider_metadata,
            warnings: outcome.warnings,
            stop_reason: outcome.stop_reason.clone(),
            messages: outcome.messages,
        })
    }

    /// Run a fully configured agent loop, preserving caller-supplied governance, audit, budgets,
    /// provider options, and messages while adding this agent's runtime tool registry.
    pub fn run_with_config(
        &self,
        config: RunConfig,
        executor: Arc<dyn ToolExecutor>,
    ) -> Result<impl Stream<Item = StreamDelta>, AgentError> {
        let provider = self.provider_for(&config.model)?;
        self.run_with_provider_config(provider, config, executor)
    }

    /// Run with pre-stream retry and an ordered model fallback chain. A provider becomes sticky
    /// after its first emitted delta, so text and tool side effects are never duplicated.
    pub fn run_with_fallback_config(
        &self,
        mut config: RunConfig,
        executor: Arc<dyn ToolExecutor>,
        fallback_models: &[String],
        retry: crate::resilience::RetryPolicy,
    ) -> Result<impl Stream<Item = StreamDelta>, AgentError> {
        // Resilience ProviderAttempt events and runtime lifecycle events must share one fresh
        // invocation identity and sequence, even when AgentOptions/AuditTrail were cloned.
        config.prepare_invocation();
        let mut models = vec![config.model.clone()];
        for model in fallback_models {
            if !models.contains(model) {
                models.push(model.clone());
            }
        }
        let targets = models
            .into_iter()
            .map(|model| {
                self.provider_for(&model)
                    .map(|provider| crate::resilience::ModelTarget::new(model, provider))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let provider = crate::resilience::ExecutionPlan::new(targets)
            .map_err(AgentError::Core)?
            .with_retry(retry)
            .with_audit(config.audit.clone())
            .into_provider();
        self.run_with_provider_config(Arc::new(provider), config, executor)
    }

    fn run_with_provider_config(
        &self,
        provider: Arc<dyn Provider>,
        mut config: RunConfig,
        executor: Arc<dyn ToolExecutor>,
    ) -> Result<impl Stream<Item = StreamDelta>, AgentError> {
        if let Some(prompt) = config.messages.iter().rev().find_map(|message| {
            if message.role != crate::types::Role::User {
                return None;
            }
            message.content.iter().find_map(|block| match block {
                crate::types::ContentBlock::Text { text } => Some(text.as_str()),
                _ => None,
            })
        }) {
            let memories = self.recall(prompt, 8)?;
            if !memories.is_empty() {
                let body = memories
                    .iter()
                    .map(|entry| format!("- {}: {}", entry.key, entry.value))
                    .collect::<Vec<_>>()
                    .join("\n");
                config.messages.insert(
                    0,
                    Message::system(format!(
                        "Explicit persistent memory for this agent namespace:\n{body}"
                    )),
                );
            }
        }
        if config.tools.is_empty() {
            config.tools = self.tools.clone();
        }
        Ok(run_agent(provider, executor, config))
    }

    /// Generate a schema-validated object against `model`, using the **strongest** structured-output
    /// mechanism that provider offers and reporting which one it used (the
    /// [`FidelityGrade`]) — degradation is never silent.
    /// Requires an active credential for the model's provider.
    pub async fn generate_object(
        &self,
        prompt: &str,
        schema: serde_json::Value,
        model: &str,
        options: crate::dx::ObjectOptions,
    ) -> Result<crate::dx::GeneratedObject, AgentError> {
        self.generate_object_messages_with_audit(
            vec![Message::user(prompt)],
            schema,
            model,
            options,
            None,
        )
        .await
    }

    /// Multimodal/canonical-message form of [`Agent::generate_object`].
    pub async fn generate_object_messages(
        &self,
        messages: Vec<Message>,
        schema: serde_json::Value,
        model: &str,
        options: crate::dx::ObjectOptions,
    ) -> Result<crate::dx::GeneratedObject, AgentError> {
        self.generate_object_messages_with_audit(messages, schema, model, options, None)
            .await
    }

    /// Stream structured-output provider deltas immediately, followed by validation/repair events
    /// and a final schema-validated object. Requires an active credential for the model's provider.
    pub fn stream_object(
        &self,
        prompt: &str,
        schema: serde_json::Value,
        model: &str,
        options: crate::dx::ObjectOptions,
    ) -> Result<crate::dx::ObjectStream, AgentError> {
        self.stream_object_messages_with_audit(
            vec![Message::user(prompt)],
            schema,
            model,
            options,
            None,
        )
    }

    /// Multimodal/canonical-message form of [`Agent::stream_object`].
    pub fn stream_object_messages(
        &self,
        messages: Vec<Message>,
        schema: serde_json::Value,
        model: &str,
        options: crate::dx::ObjectOptions,
    ) -> Result<crate::dx::ObjectStream, AgentError> {
        self.stream_object_messages_with_audit(messages, schema, model, options, None)
    }

    /// [`Agent::stream_object`] with structured audit events for every attempt and outcome.
    pub fn stream_object_with_audit(
        &self,
        prompt: &str,
        schema: serde_json::Value,
        model: &str,
        options: crate::dx::ObjectOptions,
        audit: Option<&crate::observability::AuditTrail>,
    ) -> Result<crate::dx::ObjectStream, AgentError> {
        self.stream_object_messages_with_audit(
            vec![Message::user(prompt)],
            schema,
            model,
            options,
            audit,
        )
    }

    /// [`Agent::stream_object_messages`] with structured audit events.
    pub fn stream_object_messages_with_audit(
        &self,
        messages: Vec<Message>,
        schema: serde_json::Value,
        model: &str,
        options: crate::dx::ObjectOptions,
        audit: Option<&crate::observability::AuditTrail>,
    ) -> Result<crate::dx::ObjectStream, AgentError> {
        let provider = self.provider_for(model)?;
        let name =
            provider_for_model(model).ok_or_else(|| AgentError::UnknownModel(model.to_string()))?;
        let grade = if name == "mock" {
            crate::capabilities::FidelityGrade::NativeConstrained
        } else {
            self.caps
                .get(name)
                .map(|capabilities| capabilities.structured_output)
                .unwrap_or(crate::capabilities::FidelityGrade::PromptedAndParsed)
        };
        Ok(crate::dx::stream_object_messages_observed(
            provider, name, grade, model, messages, &schema, &options, audit,
        ))
    }

    /// Rust-native typed structured output with a schema derived from `T`.
    pub async fn generate_object_typed<T>(
        &self,
        prompt: &str,
        model: &str,
        options: crate::dx::ObjectOptions,
    ) -> Result<crate::dx::TypedGeneratedObject<T>, AgentError>
    where
        T: serde::de::DeserializeOwned + schemars::JsonSchema,
    {
        self.generate_object_typed_messages::<T>(vec![Message::user(prompt)], model, options)
            .await
    }

    /// Rust-native typed structured output over canonical/multimodal messages.
    pub async fn generate_object_typed_messages<T>(
        &self,
        messages: Vec<Message>,
        model: &str,
        options: crate::dx::ObjectOptions,
    ) -> Result<crate::dx::TypedGeneratedObject<T>, AgentError>
    where
        T: serde::de::DeserializeOwned + schemars::JsonSchema,
    {
        let provider = self.provider_for(model)?;
        let name =
            provider_for_model(model).ok_or_else(|| AgentError::UnknownModel(model.to_string()))?;
        let grade = if name == "mock" {
            crate::capabilities::FidelityGrade::NativeConstrained
        } else {
            self.caps
                .get(name)
                .map(|capabilities| capabilities.structured_output)
                .unwrap_or(crate::capabilities::FidelityGrade::PromptedAndParsed)
        };
        crate::dx::generate_object_typed_messages(provider, name, grade, model, messages, &options)
            .await
            .map_err(AgentError::Core)
    }

    pub async fn generate_object_with_audit(
        &self,
        prompt: &str,
        schema: serde_json::Value,
        model: &str,
        options: crate::dx::ObjectOptions,
        audit: Option<&crate::observability::AuditTrail>,
    ) -> Result<crate::dx::GeneratedObject, AgentError> {
        self.generate_object_messages_with_audit(
            vec![Message::user(prompt)],
            schema,
            model,
            options,
            audit,
        )
        .await
    }

    /// [`Agent::generate_object_messages`] with structured audit events.
    pub async fn generate_object_messages_with_audit(
        &self,
        messages: Vec<Message>,
        schema: serde_json::Value,
        model: &str,
        options: crate::dx::ObjectOptions,
        audit: Option<&crate::observability::AuditTrail>,
    ) -> Result<crate::dx::GeneratedObject, AgentError> {
        let provider = self.provider_for(model)?;
        let name =
            provider_for_model(model).ok_or_else(|| AgentError::UnknownModel(model.to_string()))?;
        let grade = if name == "mock" {
            crate::capabilities::FidelityGrade::NativeConstrained
        } else {
            self.caps
                .get(name)
                .map(|c| c.structured_output)
                .unwrap_or(crate::capabilities::FidelityGrade::PromptedAndParsed)
        };
        crate::dx::generate_object_messages_observed(
            provider, name, grade, model, messages, &schema, &options, audit,
        )
        .await
        .map_err(AgentError::Core)
    }
}

/// Map a model id to its provider by conventional prefix.
fn provider_for_model(model: &str) -> Option<&'static str> {
    let m = model.to_ascii_lowercase();
    if m.starts_with("mock") {
        Some("mock")
    } else if m.starts_with("claude") {
        Some("anthropic")
    } else if m.starts_with("deepseek") {
        Some("deepseek")
    } else if m.starts_with("gpt")
        || m.starts_with("o1")
        || m.starts_with("o3")
        || m.starts_with("o4")
    {
        Some("openai")
    } else if m.starts_with("gemini") {
        Some("google")
    } else if m.starts_with("openrouter:") {
        Some("openrouter")
    } else if m.starts_with("groq:") {
        Some("groq")
    } else if m.starts_with("mistral:") {
        Some("mistral")
    } else if m.starts_with("xai:") || m.starts_with("grok-") {
        Some("xai")
    } else {
        None
    }
}

/// Why an [`Agent`] could not run a model.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentError {
    /// No credential is active for the provider this model belongs to.
    NoCredential(&'static str),
    /// The model id does not map to a known provider.
    UnknownModel(String),
    /// The provider is known but its live adapter is not wired yet.
    NoAdapter(&'static str),
    /// Structured-output generation failed (parse/validation after retries, or a provider error).
    Generate(String),
    Run(String),
    Memory(String),
    /// A typed failure produced directly by the core runtime.
    Core(crate::error::AikitError),
    /// A streamed terminal failure whose compatibility message and redacted classification were
    /// carried independently on the wire.
    Stream {
        message: String,
        info: Box<crate::error::ErrorInfo>,
    },
}

impl AgentError {
    /// Stable, redacted classification for native Rust callers and language bindings.
    pub fn info(&self) -> crate::error::ErrorInfo {
        use crate::error::{ErrorCode, ErrorInfo};
        match self {
            AgentError::NoCredential(provider) => {
                let mut info = ErrorInfo::new(ErrorCode::ProviderAuth);
                info.provider = Some((*provider).to_string());
                info
            }
            AgentError::UnknownModel(model) => {
                let mut info = ErrorInfo::new(ErrorCode::ProviderInvalidRequest);
                info.model = Some(model.clone());
                info
            }
            AgentError::NoAdapter(provider) => {
                let mut info = ErrorInfo::new(ErrorCode::ProviderProtocol);
                info.provider = Some((*provider).to_string());
                info
            }
            AgentError::Generate(_) => ErrorInfo::new(ErrorCode::StructuredOutput),
            AgentError::Run(_) => ErrorInfo::new(ErrorCode::Unknown),
            AgentError::Memory(_) => ErrorInfo::new(ErrorCode::Session),
            AgentError::Core(error) => error.info(),
            AgentError::Stream { info, .. } => info.as_ref().clone(),
        }
    }
}

impl From<crate::error::AikitError> for AgentError {
    fn from(error: crate::error::AikitError) -> Self {
        Self::Core(error)
    }
}

impl From<&AgentError> for crate::error::ErrorInfo {
    fn from(error: &AgentError) -> Self {
        error.info()
    }
}

impl From<AgentError> for crate::error::ErrorInfo {
    fn from(error: AgentError) -> Self {
        error.info()
    }
}

impl std::fmt::Display for AgentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AgentError::NoCredential(p) => {
                write!(
                    f,
                    "no credential active for provider '{p}' — add a key first"
                )
            }
            AgentError::UnknownModel(m) => write!(f, "unknown model '{m}' — no provider mapping"),
            AgentError::NoAdapter(p) => write!(f, "provider '{p}' has no live adapter yet"),
            AgentError::Generate(e) => write!(f, "structured-output generation failed: {e}"),
            AgentError::Run(e) => write!(f, "text generation failed: {e}"),
            AgentError::Memory(e) => write!(f, "memory error: {e}"),
            AgentError::Core(error) => std::fmt::Display::fmt(error, f),
            AgentError::Stream { message, .. } => write!(f, "text generation failed: {message}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GeneratedText {
    pub text: String,
    pub usage: Usage,
    #[serde(default)]
    pub provider_metadata: crate::types::ProviderMetadata,
    #[serde(default)]
    pub warnings: Vec<crate::contract::ProviderWarning>,
    pub stop_reason: Option<String>,
    pub messages: Vec<Message>,
}

impl std::error::Error for AgentError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            AgentError::Core(error) => Some(error),
            _ => None,
        }
    }
}

impl std::fmt::Debug for Agent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never print key material.
        f.debug_struct("Agent")
            .field("providers", &self.active_providers())
            .field("tools", &self.tools.len())
            .finish()
    }
}

/// A snapshot of an agent's current capabilities, safe to log/serialize (no secrets).
#[derive(Debug, Clone, Serialize)]
pub struct AgentCapabilities {
    pub providers: Vec<ProviderCapabilityView>,
    pub tools: Vec<String>,
    pub runtime_features: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProviderCapabilityView {
    pub provider: String,
    pub supports_reasoning: bool,
    pub supports_prompt_cache: bool,
    pub supports_vision: bool,
    pub supports_citations: bool,
    pub structured_output: FidelityGrade,
    pub structured_output_features: crate::capabilities::StructuredOutputCapabilities,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use serde_json::json;

    #[derive(Debug, Deserialize, schemars::JsonSchema)]
    struct TypedInvoice {
        total: f64,
        currency: String,
    }

    #[test]
    fn fresh_agent_has_no_capabilities() {
        let agent = Agent::new();
        assert!(agent.active_providers().is_empty());
        assert!(agent.capabilities().providers.is_empty());
    }

    #[tokio::test]
    async fn typed_structured_output_derives_schema_and_decodes_rust_value() {
        let generated = Agent::new()
            .generate_object_typed::<TypedInvoice>(
                "extract invoice",
                "mock-1",
                crate::dx::ObjectOptions::default(),
            )
            .await
            .unwrap();
        assert_eq!(generated.value.total, 0.0);
        assert_eq!(generated.value.currency, "mock");
        assert_eq!(
            generated.fidelity,
            crate::capabilities::FidelityGrade::NativeConstrained
        );
    }

    #[test]
    fn add_key_activates_and_capabilities_grows() {
        let mut agent = Agent::new();
        // "key gir → güçlen": an Anthropic key resolves with no hint at all.
        assert_eq!(
            agent.add_key("sk-ant-api03-xxxx", None, None),
            Ok("anthropic")
        );
        assert!(agent.has_provider("anthropic"));

        let caps = agent.capabilities();
        assert_eq!(caps.providers.len(), 1);
        assert_eq!(caps.providers[0].provider, "anthropic");
        assert!(caps.providers[0].supports_reasoning);

        // Add a second provider → the agent gets stronger.
        assert_eq!(agent.add_key("AIzaSyX", None, None), Ok("google"));
        assert_eq!(agent.capabilities().providers.len(), 2);
    }

    #[test]
    fn ambiguous_sk_key_is_rejected_not_misrouted() {
        let mut agent = Agent::new();
        assert_eq!(
            agent.add_key("sk-proj-xxxx", None, None),
            Err(ResolveError::Ambiguous)
        );
        // A hint disambiguates.
        assert_eq!(
            agent.add_key("sk-proj-xxxx", Some("deepseek"), None),
            Ok("deepseek")
        );
        assert!(agent.has_provider("deepseek"));
    }

    #[test]
    fn from_env_self_configures() {
        let agent = Agent::from_env([
            ("ANTHROPIC_API_KEY", "sk-ant-xxx"),
            ("DEEPSEEK_API_KEY", "sk-ds-xxx"),
            ("IRRELEVANT_VAR", "ignore-me"),
        ]);
        assert_eq!(agent.active_providers(), vec!["anthropic", "deepseek"]);
    }

    #[test]
    fn from_env_ignores_blank_credentials_and_trims_real_ones() {
        let agent = Agent::from_env([
            ("OPENAI_API_KEY", "   "),
            ("ANTHROPIC_API_KEY", "  sk-ant-xxx  "),
        ]);
        assert_eq!(agent.active_providers(), vec!["anthropic"]);
        assert_eq!(
            agent.credentials.get("anthropic").map(String::as_str),
            Some("sk-ant-xxx")
        );
    }

    #[test]
    fn google_env_alias_precedence_is_independent_of_iterator_order() {
        for vars in [
            [
                ("GOOGLE_API_KEY", "preferred"),
                ("GEMINI_API_KEY", "fallback"),
            ],
            [
                ("GEMINI_API_KEY", "fallback"),
                ("GOOGLE_API_KEY", "preferred"),
            ],
        ] {
            let agent = Agent::from_env(vars);
            assert_eq!(
                agent.credentials.get("google").map(String::as_str),
                Some("preferred")
            );
        }
    }

    #[test]
    fn add_key_rejects_blank_values_and_stores_trimmed_credentials() {
        let mut agent = Agent::new();
        assert_eq!(
            agent.add_key("   ", Some("openai"), None),
            Err(ResolveError::Empty)
        );
        assert_eq!(agent.add_key("  sk-ant-xxx  ", None, None), Ok("anthropic"));
        assert_eq!(
            agent.credentials.get("anthropic").map(String::as_str),
            Some("sk-ant-xxx")
        );
    }

    #[test]
    fn add_tool_shows_in_capabilities() {
        let mut agent = Agent::new();
        agent.add_tool(ToolSpec {
            name: "search_db".into(),
            description: "search".into(),
            input_schema: json!({ "type": "object" }),
        });
        assert_eq!(agent.capabilities().tools, vec!["search_db".to_string()]);
    }

    #[test]
    fn builtin_registration_replaces_spoofed_specs_and_removes_disabled_bash() {
        let mut agent = Agent::new();
        agent.add_tool(ToolSpec {
            name: "host_tool".into(),
            description: "preserve me".into(),
            input_schema: json!({ "type": "object" }),
        });
        for name in ["Read", "Bash"] {
            agent.add_tool(ToolSpec {
                name: name.into(),
                description: "stale or spoofed".into(),
                input_schema: json!(true),
            });
        }

        let dir = tempfile::tempdir().unwrap();
        let sandbox = crate::governance::sandbox::Sandbox::jail(dir.path()).unwrap();
        let executor = agent.register_builtin_tools(BuiltinTools::new(sandbox.clone()));
        assert_eq!(
            executor.tool_names(),
            vec!["Read", "Write", "Edit", "Grep", "Glob"]
        );

        assert_eq!(
            agent
                .tool_specs()
                .iter()
                .filter(|spec| spec.name == "Read")
                .count(),
            1
        );
        let read = agent
            .tool_specs()
            .iter()
            .find(|spec| spec.name == "Read")
            .unwrap();
        assert_eq!(read.input_schema["additionalProperties"], false);
        assert!(agent
            .tool_specs()
            .iter()
            .any(|spec| spec.name == "host_tool"));
        assert!(!agent.tool_specs().iter().any(|spec| spec.name == "Bash"));

        agent.register_builtin_tools(BuiltinTools::new(sandbox).with_bash());
        assert_eq!(
            agent
                .tool_specs()
                .iter()
                .filter(|spec| spec.name == "Bash")
                .count(),
            1
        );
    }

    #[test]
    fn debug_never_leaks_the_key() {
        let mut agent = Agent::new();
        agent.add_key("sk-ant-SUPERSECRET", None, None).unwrap();
        let dbg = format!("{agent:?}");
        assert!(!dbg.contains("SUPERSECRET"), "Debug leaked the key: {dbg}");
        assert!(dbg.contains("anthropic"));
    }

    #[test]
    fn provider_for_selects_by_model_and_requires_a_key() {
        let mut agent = Agent::new();
        // Model maps to anthropic, but no key is active yet.
        let error = agent.provider_for("claude-opus-4-8").err().unwrap();
        assert!(matches!(error, AgentError::NoCredential("anthropic")));
        let info = error.info();
        assert_eq!(info.code, crate::error::ErrorCode::ProviderAuth);
        assert_eq!(info.provider.as_deref(), Some("anthropic"));
        agent.add_key("sk-ant-xxx", None, None).unwrap();
        assert_eq!(
            agent.provider_for("claude-opus-4-8").unwrap().name(),
            "anthropic"
        );

        // DeepSeek needs its own credential.
        assert!(matches!(
            agent.provider_for("deepseek-reasoner"),
            Err(AgentError::NoCredential("deepseek"))
        ));
        agent.add_key("sk-ds", Some("deepseek"), None).unwrap();
        assert_eq!(
            agent.provider_for("deepseek-reasoner").unwrap().name(),
            "deepseek"
        );

        // Unknown model → no provider mapping.
        assert!(matches!(
            agent.provider_for("mystery-1"),
            Err(AgentError::UnknownModel(_))
        ));

        // All four flagship providers construct their adapter once a key is present.
        agent.add_key("sk-openai", Some("openai"), None).unwrap();
        assert_eq!(agent.provider_for("gpt-5").unwrap().name(), "openai");
        agent.add_key("AIzaGoogleKey", None, None).unwrap();
        assert_eq!(
            agent.provider_for("gemini-2.5-pro").unwrap().name(),
            "google"
        );
    }

    #[test]
    fn compatible_providers_keep_distinct_credentials_and_model_namespaces() {
        let agent = Agent::from_env([
            ("OPENROUTER_API_KEY", "or-key"),
            ("GROQ_API_KEY", "groq-key"),
            ("MISTRAL_API_KEY", "mistral-key"),
            ("XAI_API_KEY", "xai-key"),
        ]);
        for (model, provider) in [
            ("openrouter:openai/gpt-4o", "openrouter"),
            ("groq:llama-3.3-70b-versatile", "groq"),
            ("mistral:mistral-large-latest", "mistral"),
            ("xai:grok-3", "xai"),
            ("grok-4.5", "xai"),
        ] {
            assert_eq!(agent.provider_for(model).unwrap().name(), provider);
        }
    }

    #[tokio::test]
    async fn mock_model_exercises_structured_output_without_a_key() {
        let agent = Agent::new();
        let got = agent
            .generate_object(
                "Return an invoice status",
                json!({
                    "type": "object",
                    "required": ["currency", "status"],
                    "properties": {
                        "currency": { "type": "string", "enum": ["EUR"] },
                        "status": { "type": "string", "enum": ["ok"] }
                    }
                }),
                "mock-structured",
                crate::dx::ObjectOptions::default(),
            )
            .await
            .unwrap();

        assert_eq!(got.value, json!({ "currency": "EUR", "status": "ok" }));
        assert_eq!(
            got.fidelity,
            crate::capabilities::FidelityGrade::NativeConstrained
        );
        assert_eq!(got.attempts, 1);
    }

    #[tokio::test]
    async fn mock_model_exposes_incremental_structured_stream_on_agent() {
        let mut stream = Agent::new()
            .stream_object(
                "Return an invoice status",
                json!({
                    "type": "object",
                    "required": ["status"],
                    "properties": {
                        "status": { "type": "string", "enum": ["ok"] }
                    }
                }),
                "mock-structured",
                crate::dx::ObjectOptions::default(),
            )
            .unwrap();
        let mut saw_text_delta = false;
        let mut completed = None;
        while let Some(event) = stream.next().await {
            match event.unwrap() {
                crate::dx::ObjectStreamEvent::Delta {
                    delta: StreamDelta::TextDelta { .. },
                    ..
                } => saw_text_delta = true,
                crate::dx::ObjectStreamEvent::Completed { object } => completed = Some(object),
                _ => {}
            }
        }
        assert!(saw_text_delta);
        assert_eq!(completed.unwrap().value, json!({ "status": "ok" }));
    }

    #[tokio::test]
    async fn mock_generate_text_returns_terminal_projection_and_transcript() {
        let generated = Agent::new()
            .generate_text("hello", "mock-1", 64)
            .await
            .unwrap();
        assert!(generated.text.contains("görevi tamamladım"));
        assert_eq!(generated.usage.output_tokens, 9);
        assert_eq!(generated.stop_reason.as_deref(), Some("end_turn"));
        assert!(generated.messages.len() >= 2);
        assert!(generated.provider_metadata.is_empty());

        let mut legacy = serde_json::to_value(&generated).unwrap();
        legacy.as_object_mut().unwrap().remove("provider_metadata");
        let decoded: GeneratedText = serde_json::from_value(legacy).unwrap();
        assert!(decoded.provider_metadata.is_empty());
    }

    #[test]
    fn mutable_memory_configuration_preserves_store_and_namespace_isolation() {
        let store = Arc::new(crate::memory::InMemoryMemoryStore::default());
        let mut writer = Agent::new();
        writer.set_memory_store(store.clone(), "tenant-a");
        writer.remember("decision", json!("keep")).unwrap();

        let same_namespace = Agent::new().with_memory_store(store.clone(), "tenant-a");
        assert_eq!(same_namespace.recall("decision", 10).unwrap().len(), 1);

        let other_namespace = Agent::new().with_memory_store(store, "tenant-b");
        assert!(other_namespace.recall("decision", 10).unwrap().is_empty());
    }
}
