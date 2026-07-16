//! High-level Rust DX over the agent-native core.

use crate::agent::{Agent, AgentError};
use crate::budget::BudgetPolicy;
use crate::cancellation::{CancellationHandle, CancellationToken};
use crate::compaction::CompactionPolicy;
use crate::governance::Governance;
use crate::observability::AuditTrail;
use crate::resilience::RetryPolicy;
use crate::routing::{ModelCatalog, RouteRequest};
use crate::runtime::RunConfig;
use crate::tools::builtin::BuiltinTools;
use crate::tools::{NoTools, ToolExecutor};
use crate::types::{Message, ProviderOptions, StreamDelta, ToolSpec};
use futures::{Stream, StreamExt};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

pub type DeltaStream = Pin<Box<dyn Stream<Item = StreamDelta> + Send + 'static>>;

/// A high-level cancellable stream. A small owned driver drains the core stream so dropping this
/// handle still lets the runtime run its Stop hook, `RunStopped` audit, and recorder finalizer.
/// The driver is cancellation-bounded and never survives a completed/cancelled run.
pub struct CancellableRun {
    receiver: tokio::sync::mpsc::Receiver<StreamDelta>,
    cancellation: CancellationHandle,
    recorder: crate::session::RunRecorder,
    driver: Option<tokio::task::JoinHandle<()>>,
}

impl CancellableRun {
    pub fn cancellation_handle(&self) -> CancellationHandle {
        self.cancellation.clone()
    }

    pub fn outcome(&self) -> crate::session::RunOutcome {
        self.recorder.outcome()
    }

    /// Clone the completion recorder when another task needs to observe drop-triggered cleanup.
    pub fn recorder(&self) -> crate::session::RunRecorder {
        self.recorder.clone()
    }

    /// Request cancellation and wait for all deterministic runtime finalizers.
    pub async fn cancel(mut self) -> crate::session::RunOutcome {
        self.cancellation.cancel();
        self.wait_for_driver().await;
        self.recorder.outcome()
    }

    /// Wait for normal completion and return the canonical recorded outcome.
    pub async fn finish(mut self) -> crate::session::RunOutcome {
        self.wait_for_driver().await;
        self.recorder.outcome()
    }

    async fn wait_for_driver(&mut self) {
        while self.receiver.recv().await.is_some() {}
        if let Some(driver) = self.driver.take() {
            let _ = driver.await;
        }
    }
}

impl Stream for CancellableRun {
    type Item = StreamDelta;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.receiver).poll_recv(cx)
    }
}

impl Drop for CancellableRun {
    fn drop(&mut self) {
        if self.recorder.outcome().terminal_status == crate::session::RunTerminalStatus::Running {
            self.cancellation.cancel();
        }
        // Dropping the receiver wakes a driver blocked on backpressure. The driver deliberately
        // remains attached to the runtime stream long enough to execute its bounded finalizers.
    }
}

/// One coherent options object for the Rust `query`/`Client` layer. The lower-level `RunConfig`
/// remains available when a host needs direct control of session recorders or message history.
#[derive(Clone)]
pub struct AgentOptions {
    pub model: String,
    pub fallback_models: Vec<String>,
    pub max_tokens: u64,
    pub max_turns: usize,
    pub tools: Vec<ToolSpec>,
    pub provider_options: ProviderOptions,
    pub governance: Governance,
    pub audit: AuditTrail,
    pub budget: BudgetPolicy,
    /// Optional transcript bounding for long-running agents. Disabled by default.
    pub compaction: CompactionPolicy,
    pub retry: RetryPolicy,
    /// Optional caller-owned catalog/request. When present, routing selects `model` immediately
    /// before provider construction; the explicit `model` field becomes only the fallback default.
    pub routing: Option<RoutingOptions>,
    pub cancellation: CancellationToken,
}

/// Deterministic automatic/explicit routing attached to one normal query/run.
#[derive(Clone)]
pub struct RoutingOptions {
    pub catalog: ModelCatalog,
    pub request: RouteRequest,
}

impl RoutingOptions {
    pub fn new(catalog: ModelCatalog, request: RouteRequest) -> Self {
        Self { catalog, request }
    }
}

impl Default for AgentOptions {
    fn default() -> Self {
        Self {
            model: "mock-1".into(),
            fallback_models: Vec::new(),
            max_tokens: 4096,
            max_turns: 16,
            tools: Vec::new(),
            provider_options: ProviderOptions::new(),
            governance: Governance::default(),
            audit: AuditTrail::default(),
            budget: BudgetPolicy::default(),
            compaction: CompactionPolicy::default(),
            retry: RetryPolicy::default(),
            routing: None,
            cancellation: CancellationToken::new(),
        }
    }
}

/// Reusable high-level client. Credentials, registered tool schemas, and their executor stay
/// paired on the client; each call supplies its run policy through `AgentOptions`.
#[derive(Clone)]
pub struct Client {
    agent: Agent,
    executor: Arc<dyn ToolExecutor>,
}

impl Default for Client {
    fn default() -> Self {
        Self::new(Agent::default())
    }
}

impl Client {
    pub fn new(agent: Agent) -> Self {
        Self {
            agent,
            executor: Arc::new(NoTools),
        }
    }

    pub fn from_process_env() -> Self {
        Self::new(Agent::from_process_env())
    }

    pub fn agent(&self) -> &Agent {
        &self.agent
    }

    pub fn agent_mut(&mut self) -> &mut Agent {
        &mut self.agent
    }

    /// Register canonical built-in schemas and retain the same suite as this client's default
    /// executor. Accepts either an owned [`BuiltinTools`] value or an `Arc<BuiltinTools>`.
    pub fn register_builtin_tools(&mut self, tools: impl Into<Arc<BuiltinTools>>) {
        let tools = self.agent.register_builtin_tools(tools);
        self.executor = tools;
    }

    /// Builder form of [`Client::register_builtin_tools`].
    pub fn with_builtin_tools(mut self, tools: impl Into<Arc<BuiltinTools>>) -> Self {
        self.register_builtin_tools(tools);
        self
    }

    pub fn query(
        &self,
        prompt: impl Into<String>,
        options: AgentOptions,
    ) -> Result<DeltaStream, AgentError> {
        self.query_with_executor(prompt, options, self.executor.clone())
    }

    /// Query with canonical message history, including multimodal media blocks.
    pub fn query_messages(
        &self,
        messages: Vec<Message>,
        options: AgentOptions,
    ) -> Result<DeltaStream, AgentError> {
        self.query_messages_with_executor(messages, options, self.executor.clone())
    }

    pub fn query_with_executor(
        &self,
        prompt: impl Into<String>,
        options: AgentOptions,
        executor: Arc<dyn ToolExecutor>,
    ) -> Result<DeltaStream, AgentError> {
        self.query_messages_with_executor(vec![Message::user(prompt.into())], options, executor)
    }

    pub fn query_messages_with_executor(
        &self,
        messages: Vec<Message>,
        options: AgentOptions,
        executor: Arc<dyn ToolExecutor>,
    ) -> Result<DeltaStream, AgentError> {
        self.configured_stream_messages(messages, options, executor, None)
    }

    /// Start a cancellable run. Unlike the compatibility `query` stream, this handle owns a
    /// bounded driver so explicit cancellation or dropping the handle completes Stop/audit/session
    /// finalization even when the caller does not continue polling model deltas.
    pub fn query_cancellable(
        &self,
        prompt: impl Into<String>,
        options: AgentOptions,
    ) -> Result<CancellableRun, AgentError> {
        self.query_cancellable_with_executor(prompt, options, self.executor.clone())
    }

    /// Cancellable canonical-message form of [`Client::query_cancellable`].
    pub fn query_cancellable_messages(
        &self,
        messages: Vec<Message>,
        options: AgentOptions,
    ) -> Result<CancellableRun, AgentError> {
        self.query_cancellable_messages_with_executor(messages, options, self.executor.clone())
    }

    pub fn query_cancellable_with_executor(
        &self,
        prompt: impl Into<String>,
        options: AgentOptions,
        executor: Arc<dyn ToolExecutor>,
    ) -> Result<CancellableRun, AgentError> {
        self.query_cancellable_messages_with_executor(
            vec![Message::user(prompt.into())],
            options,
            executor,
        )
    }

    pub fn query_cancellable_messages_with_executor(
        &self,
        messages: Vec<Message>,
        options: AgentOptions,
        executor: Arc<dyn ToolExecutor>,
    ) -> Result<CancellableRun, AgentError> {
        let recorder = crate::session::RunRecorder::default();
        let cancellation = options.cancellation.handle();
        let mut stream =
            self.configured_stream_messages(messages, options, executor, Some(recorder.clone()))?;
        let (sender, receiver) = tokio::sync::mpsc::channel(32);
        let runtime = tokio::runtime::Handle::try_current().map_err(|_| {
            AgentError::Core(crate::error::AikitError::Session(
                "query_cancellable requires an active Tokio runtime".into(),
            ))
        })?;
        let driver = runtime.spawn(async move {
            while let Some(delta) = stream.next().await {
                if sender.send(delta).await.is_err() {
                    // The public handle was dropped. Keep draining: cancellation makes every
                    // provider/tool wait promptly resolve, then the runtime executes finalizers.
                    while stream.next().await.is_some() {}
                    break;
                }
            }
        });
        Ok(CancellableRun {
            receiver,
            cancellation,
            recorder,
            driver: Some(driver),
        })
    }

    /// Naming-compatible alias that keeps `query_messages*` methods grouped for discoverability.
    pub fn query_messages_cancellable_with_executor(
        &self,
        messages: Vec<Message>,
        options: AgentOptions,
        executor: Arc<dyn ToolExecutor>,
    ) -> Result<CancellableRun, AgentError> {
        self.query_cancellable_messages_with_executor(messages, options, executor)
    }

    fn configured_stream_messages(
        &self,
        messages: Vec<Message>,
        mut options: AgentOptions,
        executor: Arc<dyn ToolExecutor>,
        recorder: Option<crate::session::RunRecorder>,
    ) -> Result<DeltaStream, AgentError> {
        if let Some(routing) = options.routing.take() {
            let decision = self
                .agent
                .route(&routing.catalog, routing.request)
                .map_err(|error| {
                    AgentError::Core(crate::error::AikitError::Configuration(format!(
                        "routing failed: {error}"
                    )))
                })?;
            options.model = decision.profile.model;
        }
        let mut config = RunConfig::new(&options.model, messages);
        config.max_tokens = options.max_tokens;
        config.max_turns = options.max_turns;
        config.tools = options.tools;
        config.provider_options = options.provider_options;
        config.governance = options.governance;
        config.audit = options.audit;
        config.budget = options.budget;
        config.compaction = options.compaction;
        config.cancellation = options.cancellation;
        if let Some(recorder) = recorder {
            config.recorder = recorder;
        }
        let stream = self.agent.run_with_fallback_config(
            config,
            executor,
            &options.fallback_models,
            options.retry,
        )?;
        Ok(Box::pin(stream))
    }
}

/// One-shot Rust query using credentials from the process environment and no host tools.
pub fn query(prompt: impl Into<String>, options: AgentOptions) -> Result<DeltaStream, AgentError> {
    Client::from_process_env().query(prompt, options)
}

/// One-shot Rust query over canonical message history, including multimodal media blocks.
pub fn query_messages(
    messages: Vec<Message>,
    options: AgentOptions,
) -> Result<DeltaStream, AgentError> {
    Client::from_process_env().query_messages(messages, options)
}

/// One-shot cancellable query using credentials from the process environment.
pub fn query_cancellable(
    prompt: impl Into<String>,
    options: AgentOptions,
) -> Result<CancellableRun, AgentError> {
    Client::from_process_env().query_cancellable(prompt, options)
}

/// One-shot cancellable canonical-message query.
pub fn query_messages_cancellable(
    messages: Vec<Message>,
    options: AgentOptions,
) -> Result<CancellableRun, AgentError> {
    Client::from_process_env().query_cancellable_messages(messages, options)
}

/// One-shot Rust query with an explicit host tool executor.
pub fn query_with_executor(
    prompt: impl Into<String>,
    options: AgentOptions,
    executor: Arc<dyn ToolExecutor>,
) -> Result<DeltaStream, AgentError> {
    Client::from_process_env().query_with_executor(prompt, options, executor)
}

/// One-shot canonical-message query with an explicit host tool executor.
pub fn query_messages_with_executor(
    messages: Vec<Message>,
    options: AgentOptions,
    executor: Arc<dyn ToolExecutor>,
) -> Result<DeltaStream, AgentError> {
    Client::from_process_env().query_messages_with_executor(messages, options, executor)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::AikitError;
    use crate::governance::sandbox::Sandbox;
    use futures::StreamExt;

    #[tokio::test]
    async fn high_level_query_runs_the_keyless_mock_surface() {
        let mut stream = Client::default()
            .query("hello", AgentOptions::default())
            .unwrap();
        let mut text = String::new();
        while let Some(delta) = stream.next().await {
            if let StreamDelta::TextDelta { text: part } = delta {
                text.push_str(&part);
            }
        }
        assert!(text.contains("görevi tamamladım"));
    }

    #[tokio::test]
    async fn high_level_query_preserves_multimodal_input_in_the_outcome() {
        use crate::types::{ContentBlock, MediaSource, Role};

        let messages = vec![Message {
            role: Role::User,
            content: vec![
                ContentBlock::Text {
                    text: "Describe this image".into(),
                },
                ContentBlock::Media {
                    media_type: "image/png".into(),
                    source: MediaSource::Base64 {
                        data: "aGVsbG8=".into(),
                    },
                },
            ],
        }];
        let outcome = Client::default()
            .query_cancellable_messages(messages, AgentOptions::default())
            .unwrap()
            .finish()
            .await;

        assert!(matches!(
            outcome.messages[0].content[1],
            ContentBlock::Media { .. }
        ));
        assert_eq!(
            outcome.terminal_status,
            crate::session::RunTerminalStatus::Completed
        );
    }

    #[tokio::test]
    async fn normal_query_can_select_its_model_through_automatic_routing() {
        use crate::routing::{ModelProfile, RouteObjective, RouteRequest};

        let catalog = ModelCatalog::new(vec![ModelProfile::new(
            "mock",
            "mock-routed",
            8_192,
            1_024,
            100,
        )])
        .unwrap();
        let options = AgentOptions {
            routing: Some(RoutingOptions::new(
                catalog,
                RouteRequest::automatic(RouteObjective::Quality),
            )),
            ..AgentOptions::default()
        };
        let mut run = Client::default().query("route me", options).unwrap();
        let mut selected = None;
        while let Some(delta) = run.next().await {
            if let StreamDelta::MessageStart { model } = delta {
                selected = Some(model);
            }
        }
        assert_eq!(selected.as_deref(), Some("mock-routed"));
    }

    #[tokio::test]
    async fn concurrent_cloned_options_receive_distinct_audit_runs() {
        use crate::observability::{AuditEvent, AuditTrail, InMemoryAuditSink};

        let sink = Arc::new(InMemoryAuditSink::default());
        let options = AgentOptions {
            audit: AuditTrail::new().with_sink(sink.clone()),
            ..AgentOptions::default()
        };
        let client = Client::default();
        let mut first = client.query("first", options.clone()).unwrap();
        let mut second = client.query("second", options).unwrap();
        let ((), ()) = tokio::join!(async { while first.next().await.is_some() {} }, async {
            while second.next().await.is_some() {}
        },);

        let starts = sink
            .records()
            .into_iter()
            .filter(|record| matches!(record.event, AuditEvent::RunStarted { .. }))
            .collect::<Vec<_>>();
        assert_eq!(starts.len(), 2);
        assert_ne!(starts[0].run_id, starts[1].run_id);
        assert!(starts.iter().all(|record| record.sequence == 1));
    }

    #[test]
    fn options_default_is_safe_and_keyless() {
        let options = AgentOptions::default();
        assert_eq!(options.model, "mock-1");
        assert!(options.fallback_models.is_empty());
        assert!(options.tools.is_empty());
    }

    #[tokio::test]
    async fn builtin_registration_pairs_canonical_specs_with_the_executor() {
        let dir = tempfile::tempdir().unwrap();
        let tools = Arc::new(BuiltinTools::new(Sandbox::jail(dir.path()).unwrap()));
        let client = Client::default().with_builtin_tools(tools.clone());

        assert_eq!(
            client
                .agent()
                .tool_specs()
                .iter()
                .map(|spec| spec.name.as_str())
                .collect::<Vec<_>>(),
            tools.tool_names()
        );

        // MockProvider calls the first advertised tool with intentionally generic arguments.
        // Runtime schema validation must reject that input before the built-in executor; direct
        // execution below separately proves the registered executor remains paired and jailed.
        let mut stream = client
            .query("exercise tools", AgentOptions::default())
            .unwrap();
        let mut tool_result = None;
        while let Some(delta) = stream.next().await {
            if let StreamDelta::ToolResult { content, .. } = delta {
                tool_result = Some(content);
            }
        }
        let tool_result = tool_result.expect("mock run should reject the advertised Read input");
        assert!(tool_result.contains("JSON Schema validation"));
        assert!(!tool_result.contains("no tool executor registered"));

        // The same registered executor remains jailed to the declared root.
        let error = tools
            .execute("Read", serde_json::json!({ "path": "/etc/hostname" }))
            .await
            .unwrap_err();
        assert!(matches!(error, AikitError::Sandbox(_)));
        assert_eq!(error.info().code, crate::error::ErrorCode::Sandbox);
    }

    #[tokio::test]
    async fn cancellable_query_waits_for_cancelled_recorder_finalization() {
        let run = Client::default()
            .query_cancellable("hello", AgentOptions::default())
            .unwrap();
        let outcome = run.cancel().await;
        assert_eq!(
            outcome.terminal_status,
            crate::session::RunTerminalStatus::Cancelled
        );
        assert_eq!(outcome.stop_reason.as_deref(), Some("cancelled"));
    }

    #[tokio::test]
    async fn high_level_client_stream_exposes_typed_cancellation() {
        let token = CancellationToken::new();
        token.cancel();
        let options = AgentOptions {
            cancellation: token,
            ..AgentOptions::default()
        };
        let mut stream = Client::default().query("hello", options).unwrap();
        let mut info = None;
        while let Some(delta) = stream.next().await {
            if let StreamDelta::Error { info: error, .. } = delta {
                info = Some(error);
            }
        }
        assert_eq!(info.unwrap().code, crate::error::ErrorCode::Cancelled);
    }

    #[tokio::test]
    async fn dropping_cancellable_query_still_runs_stop_hook_and_audit() {
        use crate::governance::hooks::{HookDispatcher, PromptHookOutcome};
        use crate::observability::{AuditEvent, AuditTrail, InMemoryAuditSink};

        let prompt_entered = Arc::new(tokio::sync::Notify::new());
        let stop_reasons = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let audit = Arc::new(InMemoryAuditSink::default());
        let mut hooks = HookDispatcher::new();
        let entered = prompt_entered.clone();
        hooks.on_user_prompt_submit_async(move |_| {
            let entered = entered.clone();
            async move {
                entered.notify_one();
                std::future::pending::<PromptHookOutcome>().await
            }
        });
        let reasons = stop_reasons.clone();
        hooks.on_stop(move |ctx| reasons.lock().unwrap().push(ctx.reason.clone()));

        let options = AgentOptions {
            governance: Governance::new(Default::default(), hooks),
            audit: AuditTrail::new().with_sink(audit.clone()),
            ..AgentOptions::default()
        };
        let run = Client::default()
            .query_cancellable("hello", options)
            .unwrap();
        let recorder = run.recorder();
        prompt_entered.notified().await;
        drop(run);

        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                if audit.records().iter().any(|record| {
                    matches!(
                        &record.event,
                        AuditEvent::RunStopped { reason, .. } if reason == "cancelled"
                    )
                }) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("drop-triggered cancellation must finish its bounded driver");

        assert_eq!(stop_reasons.lock().unwrap().as_slice(), &["cancelled"]);
        assert_eq!(
            recorder.outcome().terminal_status,
            crate::session::RunTerminalStatus::Cancelled
        );
        assert_eq!(recorder.outcome().stop_reason.as_deref(), Some("cancelled"));
    }
}
