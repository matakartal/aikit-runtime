//! Codex app-server runtime adapter.

use super::{
    AgentProcessLaunch, ExecutionOwnership, ExternalAgentError, ExternalAgentErrorCode,
    ExternalAgentKind, ExternalAgentResult, ExternalAgentRuntime, ExternalApprovalDecision,
    ExternalPrompt, ExternalPromptBlock, JsonRpcConnection, JsonRpcDialect, JsonRpcProcess,
    NewRuntimeSession, RpcError, RpcInbound, RuntimeAuthState, RuntimeAuthStatus, RuntimeCandidate,
    RuntimeCatalogSnapshot, RuntimeDescriptor, RuntimeEvent, RuntimeEventKind, RuntimeHealthState,
    RuntimeInstanceId, RuntimeModel, RuntimeProfile, RuntimeSession, RuntimeSessionState,
    EXTERNAL_AGENT_CONTRACT_VERSION,
};
use async_trait::async_trait;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{broadcast, Mutex};

const RPC_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_MODEL_PAGES: usize = 128;
const MAX_PENDING_SERVER_REQUESTS: usize = 128;

#[derive(Clone)]
pub struct CodexAppServerConfig {
    pub instance_id: RuntimeInstanceId,
    pub candidate: RuntimeCandidate,
    pub profile: RuntimeProfile,
    pub environment: BTreeMap<String, String>,
    pub inherit_env: bool,
    /// Content hash of an AIKit-managed artifact. External installations leave this unset.
    pub artifact_hash: Option<String>,
}

impl std::fmt::Debug for CodexAppServerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CodexAppServerConfig")
            .field("instance_id", &self.instance_id)
            .field("candidate", &self.candidate)
            .field("profile", &self.profile)
            .field(
                "environment_keys",
                &self.environment.keys().collect::<Vec<_>>(),
            )
            .field("inherit_env", &self.inherit_env)
            .field("artifact_hash", &self.artifact_hash)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PendingKind {
    Approval,
    UserInput,
}

#[derive(Debug, Clone)]
struct PendingServerRequest {
    peer_id: Value,
    session_id: String,
    kind: PendingKind,
}

pub struct CodexAppServerRuntime {
    descriptor: Arc<RwLock<RuntimeDescriptor>>,
    connection: JsonRpcConnection,
    process: Option<Arc<JsonRpcProcess>>,
    events: broadcast::Sender<RuntimeEvent>,
    sequence: Arc<AtomicU64>,
    sessions: Arc<Mutex<HashMap<String, RuntimeSession>>>,
    pending: Arc<Mutex<HashMap<String, PendingServerRequest>>>,
    initialized: AtomicBool,
    initialize_lock: Mutex<()>,
}

impl CodexAppServerRuntime {
    pub async fn spawn(config: CodexAppServerConfig) -> ExternalAgentResult<Self> {
        if config.candidate.kind != ExternalAgentKind::Codex {
            return Err(ExternalAgentError::invalid_input(
                "Codex runtime requires a Codex executable candidate",
            ));
        }
        let descriptor = RuntimeDescriptor {
            schema_version: EXTERNAL_AGENT_CONTRACT_VERSION,
            instance_id: config.instance_id.clone(),
            kind: ExternalAgentKind::Codex,
            profile: config.profile,
            executable: config.candidate.executable.clone(),
            real_executable: config.candidate.real_executable.clone(),
            origin: config.candidate.origin,
            protocol: "codex-app-server".into(),
            version: None,
            auth: RuntimeAuthStatus::unknown(),
            health: RuntimeHealthState::Stopped,
            execution_ownership: ExecutionOwnership::VendorContained,
            artifact_hash: config.artifact_hash,
        };
        let process = Arc::new(
            JsonRpcProcess::spawn(AgentProcessLaunch {
                instance_id: config.instance_id,
                program: config.candidate.real_executable,
                args: vec!["app-server".into(), "--stdio".into()],
                cwd: None,
                env: config.environment,
                inherit_env: config.inherit_env,
                dialect: JsonRpcDialect::CodexAppServer,
            })
            .await?,
        );
        Ok(Self::from_connection(
            descriptor,
            process.connection().clone(),
            Some(process),
        ))
    }

    fn from_connection(
        descriptor: RuntimeDescriptor,
        connection: JsonRpcConnection,
        process: Option<Arc<JsonRpcProcess>>,
    ) -> Self {
        let (events, _) = broadcast::channel(256);
        let descriptor = Arc::new(RwLock::new(descriptor));
        let sequence = Arc::new(AtomicU64::new(1));
        let sessions = Arc::new(Mutex::new(HashMap::new()));
        let pending = Arc::new(Mutex::new(HashMap::new()));
        // Subscribe synchronously so no app-server request can arrive before the event pump owns
        // a receiver.
        let inbound = connection.subscribe();
        tokio::spawn(run_events(
            inbound,
            connection.clone(),
            descriptor.clone(),
            events.clone(),
            sequence.clone(),
            sessions.clone(),
            pending.clone(),
        ));
        Self {
            descriptor,
            connection,
            process,
            events,
            sequence,
            sessions,
            pending,
            initialized: AtomicBool::new(false),
            initialize_lock: Mutex::new(()),
        }
    }

    fn set_health(&self, health: RuntimeHealthState) {
        self.descriptor
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .health = health;
    }

    fn set_auth(&self, auth: RuntimeAuthStatus) {
        self.descriptor
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .auth = auth;
    }

    fn emit(&self, event: RuntimeEventKind) {
        emit_event(
            &self.connection,
            &self.events,
            &self.sequence,
            self.descriptor().instance_id,
            None,
            None,
            event,
        );
    }
}

#[async_trait]
impl ExternalAgentRuntime for CodexAppServerRuntime {
    fn descriptor(&self) -> RuntimeDescriptor {
        self.descriptor
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    fn subscribe(&self) -> broadcast::Receiver<RuntimeEvent> {
        self.events.subscribe()
    }

    async fn initialize(&self) -> ExternalAgentResult<RuntimeDescriptor> {
        if self.initialized.load(Ordering::Acquire) {
            return Ok(self.descriptor());
        }
        let _guard = self.initialize_lock.lock().await;
        if self.initialized.load(Ordering::Acquire) {
            return Ok(self.descriptor());
        }
        let response = self
            .connection
            .request(
                "initialize",
                json!({
                    "clientInfo": {
                        "name": "aikit",
                        "title": "AIKit",
                        "version": env!("CARGO_PKG_VERSION")
                    },
                    "capabilities": {"experimentalApi": false}
                }),
                RPC_TIMEOUT,
            )
            .await?;
        self.connection.notify("initialized", json!({})).await?;
        if let Some(version) = response
            .pointer("/serverInfo/version")
            .or_else(|| response.pointer("/serverInfo/codexVersion"))
            .and_then(Value::as_str)
        {
            self.descriptor
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .version = Some(version.into());
        }
        self.set_health(RuntimeHealthState::Healthy);
        self.initialized.store(true, Ordering::Release);
        Ok(self.descriptor())
    }

    async fn auth_status(&self, refresh: bool) -> ExternalAgentResult<RuntimeAuthStatus> {
        self.initialize().await?;
        let response = self
            .connection
            .request("account/read", json!({"refreshToken":refresh}), RPC_TIMEOUT)
            .await?;
        let auth = auth_status_from(&response);
        self.set_auth(auth.clone());
        self.set_health(if auth.state == RuntimeAuthState::Unauthenticated {
            RuntimeHealthState::AuthRequired
        } else {
            RuntimeHealthState::Healthy
        });
        Ok(auth)
    }

    async fn login(&self) -> ExternalAgentResult<RuntimeAuthStatus> {
        self.initialize().await?;
        let response = self
            .connection
            .request(
                "account/login/start",
                json!({"type":"chatgpt"}),
                RPC_TIMEOUT,
            )
            .await?;
        let auth = RuntimeAuthStatus {
            state: RuntimeAuthState::LoginPending,
            method: Some("chatgpt".into()),
            account_label: None,
        };
        self.set_auth(auth.clone());
        self.emit(RuntimeEventKind::LoginStarted {
            login_id: string_at(&response, &["loginId", "id"])
                .unwrap_or_else(|| "codex-login".into()),
            url: string_at(&response, &["authUrl", "url"]),
        });
        Ok(auth)
    }

    async fn logout(&self) -> ExternalAgentResult<()> {
        self.initialize().await?;
        self.connection
            .request("account/logout", json!({}), RPC_TIMEOUT)
            .await?;
        self.set_auth(RuntimeAuthStatus {
            state: RuntimeAuthState::Unauthenticated,
            method: None,
            account_label: None,
        });
        self.set_health(RuntimeHealthState::AuthRequired);
        Ok(())
    }

    async fn list_models(&self) -> ExternalAgentResult<RuntimeCatalogSnapshot> {
        self.initialize().await?;
        let mut cursor: Option<String> = None;
        let mut seen_cursors = BTreeSet::new();
        let mut seen_models = BTreeSet::new();
        let mut models = Vec::new();
        for _ in 0..MAX_MODEL_PAGES {
            let response = self
                .connection
                .request(
                    "model/list",
                    json!({"cursor":cursor.clone(),"includeHidden":false,"limit":100}),
                    RPC_TIMEOUT,
                )
                .await?;
            let data = response
                .get("data")
                .or_else(|| response.get("models"))
                .and_then(Value::as_array)
                .ok_or_else(|| protocol_error("Codex model/list omitted data"))?;
            for model in data {
                let id = string_at(model, &["id", "model", "slug"])
                    .ok_or_else(|| protocol_error("Codex model/list item omitted id"))?;
                if !seen_models.insert(id.clone()) {
                    continue;
                }
                let mut capabilities = BTreeMap::new();
                if let Some(efforts) = model.get("supportedReasoningEfforts") {
                    capabilities.insert("supported_reasoning_efforts".into(), efforts.clone());
                }
                if let Some(tiers) = model.get("additionalSpeedTiers") {
                    capabilities.insert("additional_speed_tiers".into(), tiers.clone());
                }
                models.push(RuntimeModel {
                    id,
                    display_name: string_at(model, &["displayName", "name"]),
                    capabilities,
                    available: !model
                        .get("hidden")
                        .and_then(Value::as_bool)
                        .unwrap_or(false),
                });
            }
            cursor = response
                .get("nextCursor")
                .and_then(Value::as_str)
                .map(str::to_string);
            let Some(next) = &cursor else {
                let observed = now_unix_ms();
                return Ok(RuntimeCatalogSnapshot {
                    schema_version: EXTERNAL_AGENT_CONTRACT_VERSION,
                    instance_id: self.descriptor().instance_id,
                    observed_unix_ms: observed,
                    stale_after_unix_ms: observed.checked_add(6 * 60 * 60 * 1_000),
                    models,
                });
            };
            if !seen_cursors.insert(next.clone()) {
                return Err(protocol_error(
                    "Codex model/list repeated a pagination cursor",
                ));
            }
        }
        Err(protocol_error("Codex model/list exceeded 128 pages"))
    }

    async fn new_session(&self, request: NewRuntimeSession) -> ExternalAgentResult<RuntimeSession> {
        self.initialize().await?;
        if !request.additional_directories.is_empty() {
            return Err(ExternalAgentError::invalid_input(
                "Codex additional directories require its experimental permission-profile API",
            ));
        }
        let cwd = canonical_workspace(&request.cwd)?;
        let (sandbox, approval_policy) = mode_settings(request.mode.as_deref())?;
        let requested_model = request.model.clone();
        let response = self
            .connection
            .request(
                "thread/start",
                json!({
                    "cwd":cwd,
                    "model":requested_model,
                    "sandbox":sandbox,
                    "approvalPolicy":approval_policy,
                    "ephemeral":false
                }),
                RPC_TIMEOUT,
            )
            .await?;
        let session_id = response
            .pointer("/thread/id")
            .and_then(Value::as_str)
            .ok_or_else(|| protocol_error("Codex thread/start omitted thread.id"))?
            .to_string();
        let session = RuntimeSession {
            instance_id: self.descriptor().instance_id,
            session_id: session_id.clone(),
            provider_session_id: Some(session_id.clone()),
            state: RuntimeSessionState::Ready,
            model: string_at(&response, &["model"]).or(request.model),
            artifact_hash: self.descriptor().artifact_hash,
        };
        self.sessions
            .lock()
            .await
            .insert(session_id, session.clone());
        Ok(session)
    }

    async fn load_session(&self, session_id: &str) -> ExternalAgentResult<RuntimeSession> {
        self.initialize().await?;
        validate_id("session id", session_id)?;
        let response = self
            .connection
            .request("thread/resume", json!({"threadId":session_id}), RPC_TIMEOUT)
            .await?;
        let resolved = response
            .pointer("/thread/id")
            .and_then(Value::as_str)
            .ok_or_else(|| protocol_error("Codex thread/resume omitted thread.id"))?
            .to_string();
        let session = RuntimeSession {
            instance_id: self.descriptor().instance_id,
            session_id: resolved.clone(),
            provider_session_id: Some(resolved.clone()),
            state: RuntimeSessionState::Ready,
            model: string_at(&response, &["model"]),
            artifact_hash: self.descriptor().artifact_hash,
        };
        self.sessions.lock().await.insert(resolved, session.clone());
        Ok(session)
    }

    async fn prompt(&self, prompt: ExternalPrompt) -> ExternalAgentResult<()> {
        self.initialize().await?;
        validate_id("session id", &prompt.session_id)?;
        validate_id("prompt id", &prompt.prompt_id)?;
        if prompt.blocks.is_empty() {
            return Err(ExternalAgentError::invalid_input(
                "external-agent prompt must contain at least one block",
            ));
        }
        if !self.sessions.lock().await.contains_key(&prompt.session_id) {
            return Err(ExternalAgentError::new(
                ExternalAgentErrorCode::NotFound,
                "Codex session is not loaded",
            ));
        }
        self.connection
            .request(
                "turn/start",
                json!({
                    "threadId":prompt.session_id,
                    "clientUserMessageId":prompt.prompt_id,
                    "input":prompt_blocks(prompt.blocks)?
                }),
                RPC_TIMEOUT,
            )
            .await?;
        Ok(())
    }

    async fn cancel(&self, session_id: &str, turn_id: Option<&str>) -> ExternalAgentResult<()> {
        self.initialize().await?;
        validate_id("session id", session_id)?;
        if let Some(turn_id) = turn_id {
            validate_id("turn id", turn_id)?;
        }
        self.connection
            .request(
                "turn/interrupt",
                json!({"threadId":session_id,"turnId":turn_id}),
                RPC_TIMEOUT,
            )
            .await?;
        Ok(())
    }

    async fn respond_approval(
        &self,
        session_id: &str,
        request_id: &str,
        decision: ExternalApprovalDecision,
    ) -> ExternalAgentResult<()> {
        let pending =
            take_pending(&self.pending, session_id, request_id, PendingKind::Approval).await?;
        let decision = match decision {
            ExternalApprovalDecision::Accept => "accept",
            ExternalApprovalDecision::AcceptForSession => "acceptForSession",
            ExternalApprovalDecision::Decline => "decline",
            ExternalApprovalDecision::Cancel => "cancel",
        };
        self.connection
            .respond_result(pending.peer_id, json!({"decision":decision}))
            .await
    }

    async fn respond_user_input(
        &self,
        session_id: &str,
        request_id: &str,
        answers: BTreeMap<String, String>,
    ) -> ExternalAgentResult<()> {
        let pending = take_pending(
            &self.pending,
            session_id,
            request_id,
            PendingKind::UserInput,
        )
        .await?;
        let answers = answers
            .into_iter()
            .map(|(id, answer)| (id, json!({"answers":[answer]})))
            .collect::<serde_json::Map<_, _>>();
        self.connection
            .respond_result(pending.peer_id, json!({"answers":answers}))
            .await
    }

    async fn shutdown(&self) -> ExternalAgentResult<()> {
        if let Some(process) = &self.process {
            process.shutdown(Duration::from_secs(1)).await?;
        } else {
            self.connection.close("Codex runtime shut down");
        }
        self.set_health(RuntimeHealthState::Stopped);
        Ok(())
    }
}

async fn take_pending(
    pending: &Mutex<HashMap<String, PendingServerRequest>>,
    session_id: &str,
    request_id: &str,
    kind: PendingKind,
) -> ExternalAgentResult<PendingServerRequest> {
    let mut pending = pending.lock().await;
    let current = pending.get(request_id).ok_or_else(|| {
        ExternalAgentError::new(
            ExternalAgentErrorCode::NotFound,
            "Codex server request is no longer pending",
        )
    })?;
    if current.session_id != session_id || current.kind != kind {
        return Err(ExternalAgentError::new(
            ExternalAgentErrorCode::Permission,
            "Codex server request belongs to another session or response type",
        ));
    }
    pending.remove(request_id).ok_or_else(|| {
        ExternalAgentError::new(
            ExternalAgentErrorCode::Conflict,
            "Codex server request was resolved concurrently",
        )
    })
}

async fn run_events(
    mut inbound: broadcast::Receiver<RpcInbound>,
    connection: JsonRpcConnection,
    descriptor: Arc<RwLock<RuntimeDescriptor>>,
    events: broadcast::Sender<RuntimeEvent>,
    sequence: Arc<AtomicU64>,
    sessions: Arc<Mutex<HashMap<String, RuntimeSession>>>,
    pending: Arc<Mutex<HashMap<String, PendingServerRequest>>>,
) {
    loop {
        match inbound.recv().await {
            Ok(RpcInbound::Closed { reason }) => {
                descriptor
                    .write()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .health = RuntimeHealthState::Stopped;
                if !reason.contains("shut down") && !reason.contains("shutting down") {
                    emit_event(
                        &connection,
                        &events,
                        &sequence,
                        descriptor_instance(&descriptor),
                        None,
                        None,
                        RuntimeEventKind::Failed { message: reason },
                    );
                }
                return;
            }
            Ok(RpcInbound::Notification { method, params }) => {
                map_notification(
                    &connection,
                    &descriptor,
                    &events,
                    &sequence,
                    &sessions,
                    method,
                    params,
                )
                .await;
            }
            Ok(RpcInbound::Request { id, method, params }) => {
                handle_server_request(
                    &connection,
                    &descriptor,
                    &events,
                    &sequence,
                    &pending,
                    id,
                    method,
                    params,
                )
                .await;
            }
            Err(broadcast::error::RecvError::Lagged(_)) => {
                emit_event(
                    &connection,
                    &events,
                    &sequence,
                    descriptor_instance(&descriptor),
                    None,
                    None,
                    RuntimeEventKind::Failed {
                        message: "Codex event pump exceeded its bounded queue".into(),
                    },
                );
                connection.close("Codex event pump exceeded its bounded queue");
                return;
            }
            Err(broadcast::error::RecvError::Closed) => return,
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_server_request(
    connection: &JsonRpcConnection,
    descriptor: &RwLock<RuntimeDescriptor>,
    events: &broadcast::Sender<RuntimeEvent>,
    sequence: &AtomicU64,
    pending: &Mutex<HashMap<String, PendingServerRequest>>,
    id: Value,
    method: String,
    params: Value,
) {
    let kind = if method == "item/tool/requestUserInput" {
        Some(PendingKind::UserInput)
    } else if method.contains("requestApproval") || method.ends_with("Approval") {
        Some(PendingKind::Approval)
    } else {
        None
    };
    let session_id = session_id_from(&params).unwrap_or_default();
    if kind.is_none() || session_id.is_empty() {
        let _ = connection
            .respond_error(
                id,
                RpcError {
                    code: Some(-32601),
                    message: "AIKit does not support this Codex server request".into(),
                    data: None,
                },
            )
            .await;
        return;
    }
    let kind = kind.expect("checked above");
    let request_id =
        string_at(&params, &["requestId", "approvalId"]).unwrap_or_else(|| peer_id_key(&id));
    let mut pending = pending.lock().await;
    if pending.len() >= MAX_PENDING_SERVER_REQUESTS {
        drop(pending);
        let _ = connection
            .respond_error(
                id,
                RpcError {
                    code: Some(-32000),
                    message: "AIKit pending Codex request capacity was reached".into(),
                    data: None,
                },
            )
            .await;
        return;
    }
    if pending.contains_key(&request_id) {
        drop(pending);
        let _ = connection
            .respond_error(
                id,
                RpcError {
                    code: Some(-32600),
                    message: "duplicate Codex server request id".into(),
                    data: None,
                },
            )
            .await;
        return;
    }
    pending.insert(
        request_id.clone(),
        PendingServerRequest {
            peer_id: id,
            session_id: session_id.clone(),
            kind,
        },
    );
    drop(pending);
    let event = match kind {
        PendingKind::Approval => RuntimeEventKind::ApprovalRequested {
            request_id,
            detail: params,
        },
        PendingKind::UserInput => RuntimeEventKind::UserInputRequested {
            request_id,
            detail: params,
        },
    };
    emit_event(
        connection,
        events,
        sequence,
        descriptor_instance(descriptor),
        Some(session_id),
        None,
        event,
    );
}

#[allow(clippy::too_many_arguments)]
async fn map_notification(
    connection: &JsonRpcConnection,
    descriptor: &RwLock<RuntimeDescriptor>,
    events: &broadcast::Sender<RuntimeEvent>,
    sequence: &AtomicU64,
    sessions: &Mutex<HashMap<String, RuntimeSession>>,
    method: String,
    params: Value,
) {
    let session_id = session_id_from(&params);
    let turn_id = turn_id_from(&params);
    let event = match method.as_str() {
        "thread/started" | "thread/resumed" => {
            set_session_state(sessions, session_id.as_deref(), RuntimeSessionState::Ready).await;
            RuntimeEventKind::SessionState {
                state: RuntimeSessionState::Ready,
            }
        }
        "turn/started" => {
            set_session_state(
                sessions,
                session_id.as_deref(),
                RuntimeSessionState::Running,
            )
            .await;
            RuntimeEventKind::SessionState {
                state: RuntimeSessionState::Running,
            }
        }
        "item/agentMessage/delta" => RuntimeEventKind::ContentDelta {
            stream: "assistant_text".into(),
            text: string_at(&params, &["delta", "text"]).unwrap_or_default(),
        },
        "item/reasoning/summaryTextDelta" | "item/reasoning/textDelta" => {
            RuntimeEventKind::ContentDelta {
                stream: "reasoning_text".into(),
                text: string_at(&params, &["delta", "text"]).unwrap_or_default(),
            }
        }
        "turn/completed" => {
            set_session_state(sessions, session_id.as_deref(), RuntimeSessionState::Ready).await;
            if params
                .pointer("/turn/status")
                .and_then(Value::as_str)
                .is_some_and(|status| matches!(status, "failed" | "error"))
            {
                RuntimeEventKind::Failed {
                    message: string_at(&params, &["error", "message"])
                        .unwrap_or_else(|| "Codex turn failed".into()),
                }
            } else {
                RuntimeEventKind::Completed
            }
        }
        "turn/interrupted" => RuntimeEventKind::Cancelled,
        "account/updated" | "account/login/completed" => {
            let auth = auth_status_from(&params);
            descriptor
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .auth = auth.clone();
            RuntimeEventKind::AuthUpdated { auth }
        }
        "warning" | "configWarning" => RuntimeEventKind::Warning {
            message: bounded_message(
                string_at(&params, &["message", "summary", "details"])
                    .unwrap_or_else(|| "Codex emitted a warning".into()),
            ),
        },
        _ => RuntimeEventKind::Unknown {
            method,
            // Unknown raw payloads can contain prompts, tool arguments, or credentials.
            payload: Value::Null,
        },
    };
    emit_event(
        connection,
        events,
        sequence,
        descriptor_instance(descriptor),
        session_id,
        turn_id,
        event,
    );
}

async fn set_session_state(
    sessions: &Mutex<HashMap<String, RuntimeSession>>,
    session_id: Option<&str>,
    state: RuntimeSessionState,
) {
    let Some(session_id) = session_id else {
        return;
    };
    if let Some(session) = sessions.lock().await.get_mut(session_id) {
        session.state = state;
    }
}

fn emit_event(
    connection: &JsonRpcConnection,
    events: &broadcast::Sender<RuntimeEvent>,
    sequence: &AtomicU64,
    instance_id: RuntimeInstanceId,
    session_id: Option<String>,
    turn_id: Option<String>,
    event: RuntimeEventKind,
) {
    let Some(number) = sequence
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
            value.checked_add(1)
        })
        .ok()
    else {
        connection.close("Codex runtime event sequence is exhausted");
        return;
    };
    let _ = events.send(RuntimeEvent {
        schema_version: EXTERNAL_AGENT_CONTRACT_VERSION,
        instance_id,
        session_id,
        turn_id,
        sequence: number,
        event,
    });
}

fn auth_status_from(value: &Value) -> RuntimeAuthStatus {
    let account = value.get("account").filter(|account| !account.is_null());
    let requires_auth = value
        .get("requiresOpenaiAuth")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    RuntimeAuthStatus {
        state: if account.is_some() || !requires_auth {
            RuntimeAuthState::Authenticated
        } else {
            RuntimeAuthState::Unauthenticated
        },
        method: account
            .and_then(|account| string_at(account, &["authMode", "auth_mode"]))
            .or_else(|| string_at(value, &["authMode", "auth_mode"]))
            .or_else(|| (!requires_auth).then(|| "not_required".into())),
        account_label: account.and_then(|account| {
            string_at(account, &["email", "planType"]).or_else(|| string_at(value, &["planType"]))
        }),
    }
}

fn mode_settings(mode: Option<&str>) -> ExternalAgentResult<(&'static str, &'static str)> {
    match mode.unwrap_or("workspace_write") {
        "analysis_only" => Ok(("read-only", "on-request")),
        "workspace_write" => Ok(("workspace-write", "on-request")),
        "full_access" => Ok(("danger-full-access", "on-request")),
        _ => Err(ExternalAgentError::invalid_input(
            "runtime mode must be analysis_only, workspace_write, or full_access",
        )),
    }
}

fn prompt_blocks(blocks: Vec<ExternalPromptBlock>) -> ExternalAgentResult<Vec<Value>> {
    blocks
        .into_iter()
        .map(|block| match block {
            ExternalPromptBlock::Text { text } => {
                Ok(json!({"type":"text","text":text,"text_elements":[]}))
            }
            ExternalPromptBlock::Image { uri, media_type } if media_type.starts_with("image/") => {
                Ok(json!({"type":"image","url":uri}))
            }
            ExternalPromptBlock::Image { .. } => Err(ExternalAgentError::invalid_input(
                "Codex image input requires an image media type",
            )),
            ExternalPromptBlock::Resource { .. } => Err(ExternalAgentError::invalid_input(
                "Codex app-server does not accept generic resource prompt blocks",
            )),
        })
        .collect()
}

fn canonical_workspace(path: &Path) -> ExternalAgentResult<PathBuf> {
    let canonical = std::fs::canonicalize(path).map_err(|error| {
        ExternalAgentError::invalid_input(format!(
            "failed to canonicalize runtime workspace {}: {error}",
            path.display()
        ))
    })?;
    if !canonical.is_dir() {
        return Err(ExternalAgentError::invalid_input(
            "runtime workspace must be a directory",
        ));
    }
    Ok(canonical)
}

fn validate_id(field: &str, value: &str) -> ExternalAgentResult<()> {
    if value.is_empty() || value.len() > 512 || value.chars().any(char::is_control) {
        Err(ExternalAgentError::invalid_input(format!(
            "Codex {field} is invalid"
        )))
    } else {
        Ok(())
    }
}

fn string_at(value: &Value, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(Value::as_str))
        .map(str::to_string)
}

fn session_id_from(value: &Value) -> Option<String> {
    string_at(value, &["threadId", "sessionId"]).or_else(|| {
        value
            .get("thread")
            .and_then(|thread| string_at(thread, &["id"]))
    })
}

fn turn_id_from(value: &Value) -> Option<String> {
    string_at(value, &["turnId"])
        .or_else(|| value.get("turn").and_then(|turn| string_at(turn, &["id"])))
}

fn descriptor_instance(descriptor: &RwLock<RuntimeDescriptor>) -> RuntimeInstanceId {
    descriptor
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .instance_id
        .clone()
}

fn peer_id_key(id: &Value) -> String {
    id.as_str()
        .map(str::to_string)
        .unwrap_or_else(|| id.to_string())
}

fn bounded_message(mut message: String) -> String {
    if let Some((index, _)) = message.char_indices().nth(4_096) {
        message.truncate(index);
        message.push('…');
    }
    message
}

fn protocol_error(message: impl Into<String>) -> ExternalAgentError {
    ExternalAgentError::new(ExternalAgentErrorCode::Protocol, message)
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    fn descriptor() -> RuntimeDescriptor {
        RuntimeDescriptor {
            schema_version: EXTERNAL_AGENT_CONTRACT_VERSION,
            instance_id: RuntimeInstanceId::default_for(ExternalAgentKind::Codex),
            kind: ExternalAgentKind::Codex,
            profile: RuntimeProfile::External,
            executable: "/codex".into(),
            real_executable: "/codex".into(),
            origin: super::super::RuntimeInstallOrigin::ExplicitPath,
            protocol: "codex-app-server".into(),
            version: None,
            auth: RuntimeAuthStatus::unknown(),
            health: RuntimeHealthState::Stopped,
            execution_ownership: ExecutionOwnership::VendorContained,
            artifact_hash: None,
        }
    }

    #[tokio::test]
    async fn initializes_and_reads_existing_chatgpt_auth_without_tokens() {
        let (client, server) = tokio::io::duplex(64 << 10);
        let (read, write) = tokio::io::split(client);
        let runtime = CodexAppServerRuntime::from_connection(
            descriptor(),
            JsonRpcConnection::new(read, write, JsonRpcDialect::CodexAppServer),
            None,
        );
        let peer = tokio::spawn(async move {
            let (read, mut write) = tokio::io::split(server);
            let mut read = BufReader::new(read);
            let mut line = String::new();
            read.read_line(&mut line).await.unwrap();
            let initialize: Value = serde_json::from_str(&line).unwrap();
            write
                .write_all(
                    format!(
                        "{}\n",
                        json!({"id":initialize["id"],"result":{"serverInfo":{"version":"0.144.5"}}})
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            line.clear();
            read.read_line(&mut line).await.unwrap();
            assert_eq!(
                serde_json::from_str::<Value>(&line).unwrap()["method"],
                "initialized"
            );
            line.clear();
            read.read_line(&mut line).await.unwrap();
            let request: Value = serde_json::from_str(&line).unwrap();
            write
                .write_all(
                    format!(
                        "{}\n",
                        json!({"id":request["id"],"result":{"account":{"authMode":"chatgpt","email":"user@example.com"},"requiresOpenaiAuth":true}})
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
        });
        assert_eq!(
            runtime.initialize().await.unwrap().version.as_deref(),
            Some("0.144.5")
        );
        let auth = runtime.auth_status(false).await.unwrap();
        assert_eq!(auth.state, RuntimeAuthState::Authenticated);
        assert_eq!(auth.method.as_deref(), Some("chatgpt"));
        assert_eq!(auth.account_label.as_deref(), Some("user@example.com"));
        peer.await.unwrap();
    }

    #[test]
    fn unsupported_resource_input_fails_instead_of_degrading() {
        let error = prompt_blocks(vec![ExternalPromptBlock::Resource {
            uri: "file:///tmp/a".into(),
            name: "a".into(),
        }])
        .unwrap_err();
        assert_eq!(error.code, ExternalAgentErrorCode::InvalidInput);
    }

    #[test]
    fn missing_account_never_becomes_authenticated() {
        let auth = auth_status_from(&json!({"requiresOpenaiAuth":true,"account":null}));
        assert_eq!(auth.state, RuntimeAuthState::Unauthenticated);
    }
}
