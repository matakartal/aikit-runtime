//! ACP runtime adapter for vendor-owned coding-agent CLI sessions.
//!
//! Gemini and Grok expose ACP directly. Claude requires an explicitly pinned bridge executable;
//! this module deliberately refuses `npx ...@latest` style launchers so a background session can
//! never change its protocol implementation midway through a run.

use super::{
    vendor_session_environment, ExecutionOwnership, ExternalAgentError, ExternalAgentErrorCode,
    ExternalAgentKind, ExternalAgentResult, ExternalAgentRuntime, ExternalApprovalDecision,
    ExternalPrompt, ExternalPromptBlock, NewRuntimeSession, RuntimeAuthState, RuntimeAuthStatus,
    RuntimeCandidate, RuntimeCatalogSnapshot, RuntimeDescriptor, RuntimeEvent, RuntimeEventKind,
    RuntimeHealthState, RuntimeInstanceId, RuntimeModel, RuntimeProfile, RuntimeSession,
    RuntimeSessionState, EXTERNAL_AGENT_CONTRACT_VERSION,
};
use agent_client_protocol::schema::v1::{
    AuthenticateRequest, CancelNotification, ContentBlock, ImageContent, Implementation,
    InitializeRequest, LoadSessionRequest, LogoutRequest, NewSessionRequest, PermissionOption,
    PermissionOptionKind, PromptRequest, RequestPermissionOutcome, RequestPermissionRequest,
    RequestPermissionResponse, ResourceLink, SelectedPermissionOutcome, SessionConfigKind,
    SessionConfigOption, SessionConfigOptionCategory, SessionConfigSelectOptions,
    SessionNotification, SessionUpdate, SetSessionConfigOptionRequest, SetSessionModeRequest,
    StopReason, TextContent,
};
use agent_client_protocol::schema::ProtocolVersion;
use agent_client_protocol::{AcpAgent, AcpAgentConfig, Agent, ConnectionTo};
use async_trait::async_trait;
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{broadcast, mpsc, oneshot, Mutex};

const COMMAND_QUEUE_CAPACITY: usize = 128;
const EVENT_QUEUE_CAPACITY: usize = 256;
pub const PINNED_CLAUDE_ACP_NPM_PACKAGE: &str = "@agentclientprotocol/claude-agent-acp@0.61.0";

#[derive(Clone)]
pub struct AcpRuntimeConfig {
    pub instance_id: RuntimeInstanceId,
    pub candidate: RuntimeCandidate,
    pub profile: RuntimeProfile,
    /// Exact executable that speaks ACP. For native implementations this equals the candidate.
    pub launch_program: PathBuf,
    pub launch_args: Vec<String>,
    /// Optional workspace used by `session/load`, whose ACP request requires a cwd.
    pub default_workspace: Option<PathBuf>,
    pub environment: BTreeMap<String, String>,
    /// Content hash of an AIKit-managed artifact. External installations leave this unset.
    pub artifact_hash: Option<String>,
}

impl std::fmt::Debug for AcpRuntimeConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AcpRuntimeConfig")
            .field("instance_id", &self.instance_id)
            .field("candidate", &self.candidate)
            .field("profile", &self.profile)
            .field("launch_program", &self.launch_program)
            .field("launch_args", &self.launch_args)
            .field("default_workspace", &self.default_workspace)
            .field("artifact_hash", &self.artifact_hash)
            .field(
                "environment_keys",
                &self.environment.keys().collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl AcpRuntimeConfig {
    pub fn native(
        instance_id: RuntimeInstanceId,
        candidate: RuntimeCandidate,
        profile: RuntimeProfile,
    ) -> ExternalAgentResult<Self> {
        let kind = candidate.kind;
        let launch_args = match kind {
            ExternalAgentKind::Gemini => vec!["--acp".into()],
            ExternalAgentKind::Grok => {
                vec!["--no-auto-update".into(), "agent".into(), "stdio".into()]
            }
            ExternalAgentKind::Claude => {
                return Err(ExternalAgentError::invalid_input(
                    "Claude CLI does not expose native ACP; configure an explicitly pinned ACP bridge",
                ));
            }
            ExternalAgentKind::Codex => {
                return Err(ExternalAgentError::invalid_input(
                    "Codex uses its app-server adapter, not ACP",
                ));
            }
        };
        Ok(Self {
            instance_id,
            launch_program: candidate.real_executable.clone(),
            candidate,
            profile,
            launch_args,
            default_workspace: None,
            environment: vendor_session_environment(kind),
            artifact_hash: None,
        })
    }

    pub fn pinned_bridge(
        instance_id: RuntimeInstanceId,
        candidate: RuntimeCandidate,
        profile: RuntimeProfile,
        bridge_executable: PathBuf,
        bridge_args: Vec<String>,
    ) -> ExternalAgentResult<Self> {
        if candidate.kind != ExternalAgentKind::Claude {
            return Err(ExternalAgentError::invalid_input(
                "pinned ACP bridges are currently reserved for Claude",
            ));
        }
        if bridge_args.iter().any(|arg| arg.contains("@latest")) {
            return Err(ExternalAgentError::invalid_input(
                "unpinned @latest ACP bridges are not allowed",
            ));
        }
        let mut environment = vendor_session_environment(ExternalAgentKind::Claude);
        environment.insert(
            "CLAUDE_CODE_EXECUTABLE".into(),
            candidate.real_executable.to_string_lossy().into_owned(),
        );
        Ok(Self {
            instance_id,
            candidate,
            profile,
            launch_program: bridge_executable,
            launch_args: bridge_args,
            default_workspace: None,
            environment,
            artifact_hash: None,
        })
    }

    pub fn pinned_claude_npx_bridge(
        instance_id: RuntimeInstanceId,
        candidate: RuntimeCandidate,
        profile: RuntimeProfile,
        npx_executable: PathBuf,
    ) -> ExternalAgentResult<Self> {
        if candidate.kind != ExternalAgentKind::Claude {
            return Err(ExternalAgentError::invalid_input(
                "the pinned Claude npx bridge requires a Claude candidate",
            ));
        }
        let mut environment = vendor_session_environment(ExternalAgentKind::Claude);
        environment.insert(
            "CLAUDE_CODE_EXECUTABLE".into(),
            candidate.real_executable.to_string_lossy().into_owned(),
        );
        Ok(Self {
            instance_id,
            candidate,
            profile,
            launch_program: npx_executable,
            launch_args: vec!["-y".into(), PINNED_CLAUDE_ACP_NPM_PACKAGE.into()],
            default_workspace: None,
            environment,
            artifact_hash: None,
        })
    }
}

struct PendingApproval {
    session_id: String,
    options: Vec<PermissionOption>,
    response: oneshot::Sender<RequestPermissionOutcome>,
}

struct AcpShared {
    descriptor: RwLock<RuntimeDescriptor>,
    events: broadcast::Sender<RuntimeEvent>,
    sequence: AtomicU64,
    request_sequence: AtomicU64,
    sessions: Mutex<HashMap<String, RuntimeSession>>,
    session_cwds: Mutex<HashMap<String, PathBuf>>,
    pending_approvals: Mutex<HashMap<String, PendingApproval>>,
    catalog: RwLock<Vec<RuntimeModel>>,
    initialized: AtomicBool,
    shutting_down: AtomicBool,
}

pub struct AcpExternalRuntime {
    shared: Arc<AcpShared>,
    commands: mpsc::Sender<AcpCommand>,
    initialize_lock: Mutex<()>,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    default_workspace: Option<PathBuf>,
}

enum AcpCommand {
    Initialize {
        reply: oneshot::Sender<ExternalAgentResult<AcpInitializeSnapshot>>,
    },
    Authenticate {
        method_id: String,
        reply: oneshot::Sender<ExternalAgentResult<()>>,
    },
    Logout {
        reply: oneshot::Sender<ExternalAgentResult<()>>,
    },
    NewSession {
        request: AcpNewSession,
        reply: oneshot::Sender<ExternalAgentResult<AcpSessionSnapshot>>,
    },
    LoadSession {
        session_id: String,
        cwd: PathBuf,
        reply: oneshot::Sender<ExternalAgentResult<AcpSessionSnapshot>>,
    },
    Prompt {
        prompt: ExternalPrompt,
        reply: oneshot::Sender<ExternalAgentResult<()>>,
    },
    Cancel {
        session_id: String,
        reply: oneshot::Sender<ExternalAgentResult<()>>,
    },
    Shutdown {
        reply: oneshot::Sender<ExternalAgentResult<()>>,
    },
}

#[derive(Clone)]
struct AcpInitializeSnapshot {
    version: Option<String>,
    auth_methods: Vec<(String, String)>,
    load_session: bool,
    additional_directories: bool,
}

struct AcpNewSession {
    cwd: PathBuf,
    additional_directories: Vec<PathBuf>,
    model: Option<String>,
    mode: Option<String>,
}

struct AcpSessionSnapshot {
    session_id: String,
    model: Option<String>,
    models: Vec<RuntimeModel>,
}

impl AcpExternalRuntime {
    pub async fn spawn(config: AcpRuntimeConfig) -> ExternalAgentResult<Self> {
        if matches!(config.candidate.kind, ExternalAgentKind::Codex) {
            return Err(ExternalAgentError::invalid_input(
                "Codex cannot be started through the ACP runtime",
            ));
        }
        validate_executable(&config.launch_program)?;
        if config.launch_args.iter().any(|arg| arg.contains("@latest")) {
            return Err(ExternalAgentError::invalid_input(
                "unpinned @latest ACP launch arguments are not allowed",
            ));
        }
        let default_workspace = config
            .default_workspace
            .as_deref()
            .map(canonical_workspace)
            .transpose()?;
        let descriptor = RuntimeDescriptor {
            schema_version: EXTERNAL_AGENT_CONTRACT_VERSION,
            instance_id: config.instance_id,
            kind: config.candidate.kind,
            profile: config.profile,
            executable: config.candidate.executable,
            real_executable: config.candidate.real_executable,
            origin: config.candidate.origin,
            protocol: "acp-v1".into(),
            version: None,
            auth: RuntimeAuthStatus::unknown(),
            health: RuntimeHealthState::Stopped,
            execution_ownership: ExecutionOwnership::VendorContained,
            artifact_hash: config.artifact_hash,
        };
        let (events, _) = broadcast::channel(EVENT_QUEUE_CAPACITY);
        let shared = Arc::new(AcpShared {
            descriptor: RwLock::new(descriptor),
            events,
            sequence: AtomicU64::new(1),
            request_sequence: AtomicU64::new(1),
            sessions: Mutex::new(HashMap::new()),
            session_cwds: Mutex::new(HashMap::new()),
            pending_approvals: Mutex::new(HashMap::new()),
            catalog: RwLock::new(Vec::new()),
            initialized: AtomicBool::new(false),
            shutting_down: AtomicBool::new(false),
        });
        let (commands, command_rx) = mpsc::channel(COMMAND_QUEUE_CAPACITY);
        let task_shared = shared.clone();
        let agent_config = AcpAgentConfig::new(config.launch_program)
            .args(config.launch_args)
            .envs(config.environment);
        let task = tokio::spawn(async move {
            run_acp(AcpAgent::new(agent_config), command_rx, task_shared.clone()).await;
            let mut descriptor = task_shared
                .descriptor
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            descriptor.health = RuntimeHealthState::Stopped;
        });
        Ok(Self {
            shared,
            commands,
            initialize_lock: Mutex::new(()),
            task: Mutex::new(Some(task)),
            default_workspace,
        })
    }

    fn set_health(&self, health: RuntimeHealthState) {
        self.shared
            .descriptor
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .health = health;
    }

    fn set_auth(&self, auth: RuntimeAuthStatus) {
        self.shared
            .descriptor
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .auth = auth;
    }

    async fn send_command<T>(
        &self,
        build: impl FnOnce(oneshot::Sender<ExternalAgentResult<T>>) -> AcpCommand,
    ) -> ExternalAgentResult<T> {
        let (reply, response) = oneshot::channel();
        self.commands.send(build(reply)).await.map_err(|_| {
            ExternalAgentError::new(
                ExternalAgentErrorCode::Protocol,
                "ACP runtime process is no longer available",
            )
        })?;
        response.await.map_err(|_| {
            ExternalAgentError::new(
                ExternalAgentErrorCode::Protocol,
                "ACP runtime command ended without a response",
            )
        })?
    }
}

#[async_trait]
impl ExternalAgentRuntime for AcpExternalRuntime {
    fn descriptor(&self) -> RuntimeDescriptor {
        self.shared
            .descriptor
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    fn subscribe(&self) -> broadcast::Receiver<RuntimeEvent> {
        self.shared.events.subscribe()
    }

    async fn initialize(&self) -> ExternalAgentResult<RuntimeDescriptor> {
        if self.shared.initialized.load(Ordering::Acquire) {
            return Ok(self.descriptor());
        }
        let _guard = self.initialize_lock.lock().await;
        if self.shared.initialized.load(Ordering::Acquire) {
            return Ok(self.descriptor());
        }
        let snapshot = self
            .send_command(|reply| AcpCommand::Initialize { reply })
            .await?;
        {
            let mut descriptor = self
                .shared
                .descriptor
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            descriptor.version = snapshot.version;
            descriptor.health = RuntimeHealthState::Healthy;
        }
        self.shared.initialized.store(true, Ordering::Release);
        Ok(self.descriptor())
    }

    async fn auth_status(&self, _refresh: bool) -> ExternalAgentResult<RuntimeAuthStatus> {
        self.initialize().await?;
        // ACP v1 intentionally has no "read current account" method. Do not infer auth by reading
        // vendor credential files or by issuing a potentially billable prompt.
        Ok(self.descriptor().auth)
    }

    async fn login(&self) -> ExternalAgentResult<RuntimeAuthStatus> {
        self.initialize().await?;
        let snapshot = self
            .send_command(|reply| AcpCommand::Initialize { reply })
            .await?;
        let (method_id, method_name) = snapshot.auth_methods.first().cloned().ok_or_else(|| {
            ExternalAgentError::new(
                ExternalAgentErrorCode::Authentication,
                "ACP agent did not advertise a vendor-owned login method",
            )
        })?;
        let pending = RuntimeAuthStatus {
            state: RuntimeAuthState::LoginPending,
            method: Some(method_name.clone()),
            account_label: None,
        };
        self.set_auth(pending.clone());
        emit(
            &self.shared,
            None,
            None,
            RuntimeEventKind::LoginStarted {
                login_id: method_id.clone(),
                url: None,
            },
        );
        self.send_command(|reply| AcpCommand::Authenticate {
            method_id: method_id.clone(),
            reply,
        })
        .await?;
        let authenticated = RuntimeAuthStatus {
            state: RuntimeAuthState::Authenticated,
            method: Some(method_name),
            account_label: None,
        };
        self.set_auth(authenticated.clone());
        self.set_health(RuntimeHealthState::Healthy);
        emit(
            &self.shared,
            None,
            None,
            RuntimeEventKind::AuthUpdated {
                auth: authenticated.clone(),
            },
        );
        Ok(authenticated)
    }

    async fn logout(&self) -> ExternalAgentResult<()> {
        self.initialize().await?;
        self.send_command(|reply| AcpCommand::Logout { reply })
            .await?;
        let status = RuntimeAuthStatus {
            state: RuntimeAuthState::Unauthenticated,
            method: None,
            account_label: None,
        };
        self.set_auth(status.clone());
        self.set_health(RuntimeHealthState::AuthRequired);
        emit(
            &self.shared,
            None,
            None,
            RuntimeEventKind::AuthUpdated { auth: status },
        );
        Ok(())
    }

    async fn list_models(&self) -> ExternalAgentResult<RuntimeCatalogSnapshot> {
        self.initialize().await?;
        let observed = now_unix_ms();
        Ok(RuntimeCatalogSnapshot {
            schema_version: EXTERNAL_AGENT_CONTRACT_VERSION,
            instance_id: self.descriptor().instance_id,
            observed_unix_ms: observed,
            stale_after_unix_ms: observed.checked_add(6 * 60 * 60 * 1_000),
            models: self
                .shared
                .catalog
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone(),
        })
    }

    async fn new_session(&self, request: NewRuntimeSession) -> ExternalAgentResult<RuntimeSession> {
        self.initialize().await?;
        let cwd = canonical_workspace(&request.cwd)?;
        let mut additional_directories = Vec::with_capacity(request.additional_directories.len());
        for directory in &request.additional_directories {
            additional_directories.push(canonical_workspace(directory)?);
        }
        let snapshot = self
            .send_command(|reply| AcpCommand::NewSession {
                request: AcpNewSession {
                    cwd: cwd.clone(),
                    additional_directories,
                    model: request.model.clone(),
                    mode: request.mode.clone(),
                },
                reply,
            })
            .await?;
        replace_catalog(&self.shared, snapshot.models);
        let session = RuntimeSession {
            instance_id: self.descriptor().instance_id,
            session_id: snapshot.session_id.clone(),
            provider_session_id: Some(snapshot.session_id.clone()),
            state: RuntimeSessionState::Ready,
            model: snapshot.model,
            artifact_hash: self.descriptor().artifact_hash,
        };
        self.shared
            .sessions
            .lock()
            .await
            .insert(snapshot.session_id.clone(), session.clone());
        self.shared
            .session_cwds
            .lock()
            .await
            .insert(snapshot.session_id, cwd);
        Ok(session)
    }

    async fn load_session(&self, session_id: &str) -> ExternalAgentResult<RuntimeSession> {
        self.initialize().await?;
        validate_id("session id", session_id)?;
        let cwd = self
            .shared
            .session_cwds
            .lock()
            .await
            .get(session_id)
            .cloned()
            .or_else(|| self.default_workspace.clone())
            .ok_or_else(|| {
                ExternalAgentError::invalid_input(
                    "ACP session/load requires a known session cwd or default_workspace",
                )
            })?;
        let session_id = session_id.to_string();
        let snapshot = self
            .send_command(|reply| AcpCommand::LoadSession {
                session_id: session_id.clone(),
                cwd,
                reply,
            })
            .await?;
        replace_catalog(&self.shared, snapshot.models);
        let session = RuntimeSession {
            instance_id: self.descriptor().instance_id,
            session_id: snapshot.session_id.clone(),
            provider_session_id: Some(snapshot.session_id.clone()),
            state: RuntimeSessionState::Ready,
            model: snapshot.model,
            artifact_hash: self.descriptor().artifact_hash,
        };
        self.shared
            .sessions
            .lock()
            .await
            .insert(snapshot.session_id, session.clone());
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
        if !self
            .shared
            .sessions
            .lock()
            .await
            .contains_key(&prompt.session_id)
        {
            return Err(ExternalAgentError::new(
                ExternalAgentErrorCode::NotFound,
                "ACP session is not loaded",
            ));
        }
        self.send_command(|reply| AcpCommand::Prompt { prompt, reply })
            .await
    }

    async fn cancel(&self, session_id: &str, _turn_id: Option<&str>) -> ExternalAgentResult<()> {
        self.initialize().await?;
        validate_id("session id", session_id)?;
        cancel_pending_approvals(&self.shared, session_id).await;
        let session_id = session_id.to_string();
        self.send_command(|reply| AcpCommand::Cancel { session_id, reply })
            .await
    }

    async fn respond_approval(
        &self,
        session_id: &str,
        request_id: &str,
        decision: ExternalApprovalDecision,
    ) -> ExternalAgentResult<()> {
        validate_id("session id", session_id)?;
        validate_id("request id", request_id)?;
        let mut pending = self.shared.pending_approvals.lock().await;
        let request = pending.get(request_id).ok_or_else(|| {
            ExternalAgentError::new(
                ExternalAgentErrorCode::NotFound,
                "ACP permission request is no longer pending",
            )
        })?;
        if request.session_id != session_id {
            return Err(ExternalAgentError::new(
                ExternalAgentErrorCode::Permission,
                "ACP permission request belongs to another session",
            ));
        }
        let outcome = approval_outcome(&request.options, decision)?;
        let request = pending.remove(request_id).ok_or_else(|| {
            ExternalAgentError::new(
                ExternalAgentErrorCode::Conflict,
                "ACP permission request was resolved concurrently",
            )
        })?;
        request.response.send(outcome).map_err(|_| {
            ExternalAgentError::new(
                ExternalAgentErrorCode::Conflict,
                "ACP permission responder has already closed",
            )
        })
    }

    async fn respond_user_input(
        &self,
        _session_id: &str,
        _request_id: &str,
        _answers: BTreeMap<String, String>,
    ) -> ExternalAgentResult<()> {
        Err(ExternalAgentError::invalid_input(
            "ACP v1 user-input elicitation is not enabled in this adapter",
        ))
    }

    async fn shutdown(&self) -> ExternalAgentResult<()> {
        if self.shared.shutting_down.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        let result = self
            .send_command(|reply| AcpCommand::Shutdown { reply })
            .await;
        if let Some(task) = self.task.lock().await.take() {
            let _ = task.await;
        }
        self.set_health(RuntimeHealthState::Stopped);
        result
    }
}

async fn run_acp(agent: AcpAgent, commands: mpsc::Receiver<AcpCommand>, shared: Arc<AcpShared>) {
    let notification_shared = shared.clone();
    let permission_shared = shared.clone();
    let command_shared = shared.clone();
    let result = agent_client_protocol::Client
        .builder()
        .name(format!("aikit-{}", descriptor_instance(&shared)))
        .on_receive_notification(
            async move |notification: SessionNotification, _connection| {
                handle_session_notification(&notification_shared, notification).await;
                Ok(())
            },
            agent_client_protocol::on_receive_notification!(),
        )
        .on_receive_request(
            async move |request: RequestPermissionRequest, responder, _connection| {
                let outcome = handle_permission_request(&permission_shared, request).await;
                responder.respond(RequestPermissionResponse::new(outcome))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .connect_with(agent, |connection: ConnectionTo<Agent>| async move {
            run_commands(connection, commands, command_shared).await
        })
        .await;
    if let Err(error) = result {
        let message = format!(
            "ACP connection failed: {}",
            safe_acp_error_message(error.to_string())
        );
        if !shared.shutting_down.load(Ordering::Acquire) {
            set_shared_health(&shared, RuntimeHealthState::Degraded);
            emit(&shared, None, None, RuntimeEventKind::Failed { message });
        }
    }
}

async fn run_commands(
    connection: ConnectionTo<Agent>,
    mut commands: mpsc::Receiver<AcpCommand>,
    shared: Arc<AcpShared>,
) -> Result<(), agent_client_protocol::Error> {
    let mut initialized: Option<AcpInitializeSnapshot> = None;
    while let Some(command) = commands.recv().await {
        match command {
            AcpCommand::Initialize { reply } => {
                if let Some(snapshot) = &initialized {
                    let _ = reply.send(Ok(snapshot.clone()));
                    continue;
                }
                let request = InitializeRequest::new(ProtocolVersion::V1).client_info(
                    Implementation::new("aikit", env!("CARGO_PKG_VERSION")).title("AIKit"),
                );
                let result = connection.send_request(request).block_task().await;
                match result {
                    Ok(response) if response.protocol_version == ProtocolVersion::V1 => {
                        let snapshot = AcpInitializeSnapshot {
                            version: response
                                .agent_info
                                .as_ref()
                                .map(|info| info.version.clone()),
                            auth_methods: response
                                .auth_methods
                                .iter()
                                .map(|method| (method.id().to_string(), method.name().to_string()))
                                .collect(),
                            load_session: response.agent_capabilities.load_session,
                            additional_directories: response
                                .agent_capabilities
                                .session_capabilities
                                .additional_directories
                                .is_some(),
                        };
                        initialized = Some(snapshot.clone());
                        let _ = reply.send(Ok(snapshot));
                    }
                    Ok(_) => {
                        let _ = reply.send(Err(protocol_error(
                            "ACP agent negotiated an unsupported protocol version",
                        )));
                    }
                    Err(error) => {
                        let _ = reply.send(Err(acp_error("initialize", error)));
                    }
                }
            }
            AcpCommand::Authenticate { method_id, reply } => {
                let result = connection
                    .send_request(AuthenticateRequest::new(method_id))
                    .block_task()
                    .await
                    .map(|_| ())
                    .map_err(|error| acp_error("authenticate", error));
                let _ = reply.send(result);
            }
            AcpCommand::Logout { reply } => {
                let result = connection
                    .send_request(LogoutRequest::new())
                    .block_task()
                    .await
                    .map(|_| ())
                    .map_err(|error| acp_error("logout", error));
                let _ = reply.send(result);
            }
            AcpCommand::NewSession { request, reply } => {
                let Some(capabilities) = initialized.as_ref() else {
                    let _ = reply.send(Err(protocol_error("ACP runtime is not initialized")));
                    continue;
                };
                if !request.additional_directories.is_empty()
                    && !capabilities.additional_directories
                {
                    let _ = reply.send(Err(ExternalAgentError::invalid_input(
                        "ACP agent did not advertise additionalDirectories support",
                    )));
                    continue;
                }
                let protocol_request = NewSessionRequest::new(request.cwd)
                    .additional_directories(request.additional_directories);
                let response = match connection.send_request(protocol_request).block_task().await {
                    Ok(response) => response,
                    Err(error) => {
                        let _ = reply.send(Err(acp_error("session/new", error)));
                        continue;
                    }
                };
                let session_id = response.session_id.to_string();
                let models = models_from_options(response.config_options.as_deref().unwrap_or(&[]));
                let selected_model = selected_model(response.config_options.as_deref());
                if let Some(mode) = request.mode {
                    if let Err(error) = connection
                        .send_request(SetSessionModeRequest::new(session_id.clone(), mode))
                        .block_task()
                        .await
                    {
                        let _ = reply.send(Err(acp_error("session/set_mode", error)));
                        continue;
                    }
                }
                let selected_model = if let Some(model) = request.model {
                    let config_id =
                        model_config_id(response.config_options.as_deref()).ok_or_else(|| {
                            ExternalAgentError::invalid_input(
                                "ACP agent did not advertise a model configuration option",
                            )
                        });
                    let config_id = match config_id {
                        Ok(id) => id,
                        Err(error) => {
                            let _ = reply.send(Err(error));
                            continue;
                        }
                    };
                    if !models.iter().any(|candidate| candidate.id == model) {
                        let _ = reply.send(Err(ExternalAgentError::invalid_input(format!(
                            "ACP agent did not advertise model `{model}`",
                        ))));
                        continue;
                    }
                    if let Err(error) = connection
                        .send_request(SetSessionConfigOptionRequest::new(
                            session_id.clone(),
                            config_id,
                            model.as_str(),
                        ))
                        .block_task()
                        .await
                    {
                        let _ = reply.send(Err(acp_error("session/set_config_option", error)));
                        continue;
                    }
                    Some(model)
                } else {
                    selected_model
                };
                let _ = reply.send(Ok(AcpSessionSnapshot {
                    session_id,
                    model: selected_model,
                    models,
                }));
            }
            AcpCommand::LoadSession {
                session_id,
                cwd,
                reply,
            } => {
                if !initialized
                    .as_ref()
                    .is_some_and(|capabilities| capabilities.load_session)
                {
                    let _ = reply.send(Err(ExternalAgentError::invalid_input(
                        "ACP agent did not advertise session/load support",
                    )));
                    continue;
                }
                match connection
                    .send_request(LoadSessionRequest::new(session_id.clone(), cwd))
                    .block_task()
                    .await
                {
                    Ok(response) => {
                        let models =
                            models_from_options(response.config_options.as_deref().unwrap_or(&[]));
                        let model = selected_model(response.config_options.as_deref());
                        let _ = reply.send(Ok(AcpSessionSnapshot {
                            session_id,
                            model,
                            models,
                        }));
                    }
                    Err(error) => {
                        let _ = reply.send(Err(acp_error("session/load", error)));
                    }
                }
            }
            AcpCommand::Prompt { prompt, reply } => {
                let blocks = match prompt_blocks(prompt.blocks) {
                    Ok(blocks) => blocks,
                    Err(error) => {
                        let _ = reply.send(Err(error));
                        continue;
                    }
                };
                let session_id = prompt.session_id;
                let prompt_id = prompt.prompt_id;
                let sent = connection.send_request(PromptRequest::new(session_id.clone(), blocks));
                let event_shared = shared.clone();
                tokio::spawn(async move {
                    emit(
                        &event_shared,
                        Some(session_id.clone()),
                        Some(prompt_id),
                        RuntimeEventKind::SessionState {
                            state: RuntimeSessionState::Running,
                        },
                    );
                    match sent.block_task().await {
                        Ok(response) => match response.stop_reason {
                            StopReason::EndTurn => emit(
                                &event_shared,
                                Some(session_id),
                                None,
                                RuntimeEventKind::Completed,
                            ),
                            StopReason::Cancelled => emit(
                                &event_shared,
                                Some(session_id),
                                None,
                                RuntimeEventKind::Cancelled,
                            ),
                            reason => {
                                emit(
                                    &event_shared,
                                    Some(session_id.clone()),
                                    None,
                                    RuntimeEventKind::Warning {
                                        message: format!("ACP prompt stopped: {reason:?}"),
                                    },
                                );
                                emit(
                                    &event_shared,
                                    Some(session_id),
                                    None,
                                    RuntimeEventKind::Completed,
                                )
                            }
                        },
                        Err(error) => emit(
                            &event_shared,
                            Some(session_id),
                            None,
                            RuntimeEventKind::Failed {
                                message: format!(
                                    "ACP prompt failed: {}",
                                    safe_acp_error_message(error.to_string())
                                ),
                            },
                        ),
                    }
                });
                let _ = reply.send(Ok(()));
            }
            AcpCommand::Cancel { session_id, reply } => {
                let result = connection
                    .send_notification(CancelNotification::new(session_id))
                    .map_err(|error| acp_error("session/cancel", error));
                let _ = reply.send(result);
            }
            AcpCommand::Shutdown { reply } => {
                let _ = reply.send(Ok(()));
                return Ok(());
            }
        }
    }
    Ok(())
}

async fn handle_session_notification(shared: &AcpShared, notification: SessionNotification) {
    let session_id = notification.session_id.to_string();
    let event = match notification.update {
        SessionUpdate::UserMessageChunk(chunk) => content_event("user", chunk.content),
        SessionUpdate::AgentMessageChunk(chunk) => content_event("assistant", chunk.content),
        SessionUpdate::AgentThoughtChunk(chunk) => content_event("reasoning", chunk.content),
        SessionUpdate::ToolCall(tool) => RuntimeEventKind::ToolStarted {
            tool_call_id: serialized_id(&tool),
            detail: safe_value(&tool),
        },
        SessionUpdate::ToolCallUpdate(tool) => RuntimeEventKind::ToolUpdated {
            tool_call_id: serialized_id(&tool),
            detail: safe_value(&tool),
        },
        SessionUpdate::Plan(plan) => RuntimeEventKind::PlanUpdated {
            entries: safe_value(&plan)
                .get("entries")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default(),
        },
        SessionUpdate::UsageUpdate(usage) => RuntimeEventKind::UsageUpdated {
            usage: safe_value(&usage),
        },
        SessionUpdate::ConfigOptionUpdate(update) => {
            replace_catalog(shared, models_from_options(&update.config_options));
            RuntimeEventKind::Unknown {
                method: "session/config_option_update".into(),
                payload: Value::Null,
            }
        }
        SessionUpdate::CurrentModeUpdate(_) => RuntimeEventKind::Unknown {
            method: "session/current_mode_update".into(),
            payload: Value::Null,
        },
        SessionUpdate::AvailableCommandsUpdate(_) => RuntimeEventKind::Unknown {
            method: "session/available_commands_update".into(),
            payload: Value::Null,
        },
        SessionUpdate::SessionInfoUpdate(_) => RuntimeEventKind::Unknown {
            method: "session/info_update".into(),
            payload: Value::Null,
        },
        _ => RuntimeEventKind::Unknown {
            method: "session/update".into(),
            payload: Value::Null,
        },
    };
    emit(shared, Some(session_id), None, event);
}

async fn handle_permission_request(
    shared: &AcpShared,
    request: RequestPermissionRequest,
) -> RequestPermissionOutcome {
    let session_id = request.session_id.to_string();
    let request_number = shared.request_sequence.fetch_add(1, Ordering::Relaxed);
    let request_id = format!("acp-permission-{request_number}");
    let detail = json!({
        "tool_call": safe_value(&request.tool_call),
        "options": request.options.iter().map(safe_value).collect::<Vec<_>>()
    });
    let (response, receiver) = oneshot::channel();
    {
        let mut pending = shared.pending_approvals.lock().await;
        if pending.contains_key(&request_id) {
            return RequestPermissionOutcome::Cancelled;
        }
        pending.insert(
            request_id.clone(),
            PendingApproval {
                session_id: session_id.clone(),
                options: request.options,
                response,
            },
        );
    }
    let delivered = emit(
        shared,
        Some(session_id),
        None,
        RuntimeEventKind::ApprovalRequested {
            request_id: request_id.clone(),
            detail,
        },
    );
    if !delivered {
        shared.pending_approvals.lock().await.remove(&request_id);
        return RequestPermissionOutcome::Cancelled;
    }
    receiver
        .await
        .unwrap_or(RequestPermissionOutcome::Cancelled)
}

async fn cancel_pending_approvals(shared: &AcpShared, session_id: &str) {
    let mut pending = shared.pending_approvals.lock().await;
    let ids = pending
        .iter()
        .filter_map(|(id, request)| (request.session_id == session_id).then_some(id.clone()))
        .collect::<Vec<_>>();
    for id in ids {
        if let Some(request) = pending.remove(&id) {
            let _ = request.response.send(RequestPermissionOutcome::Cancelled);
        }
    }
}

fn approval_outcome(
    options: &[PermissionOption],
    decision: ExternalApprovalDecision,
) -> ExternalAgentResult<RequestPermissionOutcome> {
    if decision == ExternalApprovalDecision::Cancel {
        return Ok(RequestPermissionOutcome::Cancelled);
    }
    // ACP v1 has `allow_always`, not a precisely session-scoped grant. Never widen a one-shot or
    // session request into a potentially persistent vendor permission. If the conservative option
    // is absent, cancel the turn rather than silently changing the user's intent.
    let preferred = match decision {
        ExternalApprovalDecision::Accept | ExternalApprovalDecision::AcceptForSession => {
            PermissionOptionKind::AllowOnce
        }
        ExternalApprovalDecision::Decline => PermissionOptionKind::RejectOnce,
        ExternalApprovalDecision::Cancel => unreachable!(),
    };
    Ok(options
        .iter()
        .find(|option| option.kind == preferred)
        .map(|option| {
            RequestPermissionOutcome::Selected(SelectedPermissionOutcome::new(
                option.option_id.clone(),
            ))
        })
        .unwrap_or(RequestPermissionOutcome::Cancelled))
}

fn prompt_blocks(blocks: Vec<ExternalPromptBlock>) -> ExternalAgentResult<Vec<ContentBlock>> {
    blocks
        .into_iter()
        .map(|block| match block {
            ExternalPromptBlock::Text { text } => Ok(ContentBlock::Text(TextContent::new(text))),
            ExternalPromptBlock::Resource { uri, name } => {
                Ok(ContentBlock::ResourceLink(ResourceLink::new(name, uri)))
            }
            ExternalPromptBlock::Image { uri, media_type } => {
                let (declared_type, data) = uri
                    .strip_prefix("data:")
                    .and_then(|value| value.split_once(";base64,"))
                    .ok_or_else(|| {
                        ExternalAgentError::invalid_input(
                            "ACP image blocks must use a base64 data URI",
                        )
                    })?;
                if declared_type != media_type {
                    return Err(ExternalAgentError::invalid_input(
                        "ACP image media_type does not match its data URI",
                    ));
                }
                Ok(ContentBlock::Image(
                    ImageContent::new(data, media_type).uri(uri),
                ))
            }
        })
        .collect()
}

fn content_event(stream: &str, content: ContentBlock) -> RuntimeEventKind {
    match content {
        ContentBlock::Text(text) => RuntimeEventKind::ContentDelta {
            stream: stream.into(),
            text: text.text,
        },
        _ => RuntimeEventKind::Unknown {
            method: format!("session/{stream}_content"),
            payload: Value::Null,
        },
    }
}

fn models_from_options(options: &[SessionConfigOption]) -> Vec<RuntimeModel> {
    let Some(option) = model_option(options) else {
        return Vec::new();
    };
    let SessionConfigKind::Select(select) = &option.kind else {
        return Vec::new();
    };
    let choices = match &select.options {
        SessionConfigSelectOptions::Ungrouped(options) => options
            .iter()
            .map(|option| (option.value.to_string(), option.name.clone()))
            .collect::<Vec<_>>(),
        SessionConfigSelectOptions::Grouped(groups) => groups
            .iter()
            .flat_map(|group| {
                group
                    .options
                    .iter()
                    .map(|option| (option.value.to_string(), option.name.clone()))
            })
            .collect::<Vec<_>>(),
        _ => Vec::new(),
    };
    choices
        .into_iter()
        .map(|(id, display_name)| RuntimeModel {
            id,
            display_name: Some(display_name),
            capabilities: BTreeMap::new(),
            available: true,
        })
        .collect()
}

fn model_option(options: &[SessionConfigOption]) -> Option<&SessionConfigOption> {
    options.iter().find(|option| {
        matches!(option.category, Some(SessionConfigOptionCategory::Model))
            || option.id.to_string().eq_ignore_ascii_case("model")
    })
}

fn model_config_id(options: Option<&[SessionConfigOption]>) -> Option<String> {
    model_option(options.unwrap_or_default()).map(|option| option.id.to_string())
}

fn selected_model(options: Option<&[SessionConfigOption]>) -> Option<String> {
    let option = model_option(options.unwrap_or_default())?;
    let SessionConfigKind::Select(select) = &option.kind else {
        return None;
    };
    Some(select.current_value.to_string())
}

fn replace_catalog(shared: &AcpShared, models: Vec<RuntimeModel>) {
    if models.is_empty() {
        return;
    }
    *shared
        .catalog
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = models;
}

fn safe_value(value: &impl serde::Serialize) -> Value {
    serde_json::to_value(value).unwrap_or(Value::Null)
}

fn serialized_id(value: &impl serde::Serialize) -> String {
    let value = safe_value(value);
    value
        .get("toolCallId")
        .or_else(|| value.get("tool_call_id"))
        .or_else(|| value.get("id"))
        .and_then(Value::as_str)
        .unwrap_or("unknown-tool-call")
        .to_string()
}

fn emit(
    shared: &AcpShared,
    session_id: Option<String>,
    turn_id: Option<String>,
    event: RuntimeEventKind,
) -> bool {
    let Some(sequence) = shared
        .sequence
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
            value.checked_add(1)
        })
        .ok()
    else {
        set_shared_health(shared, RuntimeHealthState::Blocked);
        return false;
    };
    shared
        .events
        .send(RuntimeEvent {
            schema_version: EXTERNAL_AGENT_CONTRACT_VERSION,
            instance_id: descriptor_instance(shared),
            session_id,
            turn_id,
            sequence,
            event,
        })
        .is_ok()
}

fn descriptor_instance(shared: &AcpShared) -> RuntimeInstanceId {
    shared
        .descriptor
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .instance_id
        .clone()
}

fn set_shared_health(shared: &AcpShared, health: RuntimeHealthState) {
    shared
        .descriptor
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .health = health;
}

fn validate_executable(path: &Path) -> ExternalAgentResult<()> {
    if !path.is_absolute() {
        return Err(ExternalAgentError::invalid_input(
            "ACP launch executable must be an absolute path",
        ));
    }
    let metadata = std::fs::metadata(path).map_err(|error| {
        ExternalAgentError::new(
            ExternalAgentErrorCode::NotFound,
            format!("ACP launch executable is unavailable: {error}"),
        )
    })?;
    if !metadata.is_file() {
        return Err(ExternalAgentError::new(
            ExternalAgentErrorCode::NotExecutable,
            "ACP launch path is not a regular file",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o111 == 0 {
            return Err(ExternalAgentError::new(
                ExternalAgentErrorCode::NotExecutable,
                "ACP launch path is not executable",
            ));
        }
    }
    Ok(())
}

fn canonical_workspace(path: &Path) -> ExternalAgentResult<PathBuf> {
    let canonical = std::fs::canonicalize(path).map_err(|error| {
        ExternalAgentError::io(format!("cannot resolve ACP workspace directory: {error}"))
    })?;
    if !canonical.is_dir() {
        return Err(ExternalAgentError::invalid_input(
            "ACP workspace path must be a directory",
        ));
    }
    Ok(canonical)
}

fn validate_id(label: &str, value: &str) -> ExternalAgentResult<()> {
    if value.is_empty() || value.len() > 512 || value.chars().any(char::is_control) {
        return Err(ExternalAgentError::invalid_input(format!(
            "{label} must contain 1-512 non-control characters",
        )));
    }
    Ok(())
}

fn protocol_error(message: impl Into<String>) -> ExternalAgentError {
    ExternalAgentError::new(ExternalAgentErrorCode::Protocol, message)
}

fn acp_error(operation: &str, error: agent_client_protocol::Error) -> ExternalAgentError {
    protocol_error(format!(
        "ACP {operation} failed: {}",
        safe_acp_error_message(error.to_string())
    ))
}

fn safe_acp_error_message(message: String) -> String {
    let lower = message.to_ascii_lowercase();
    if [
        "authorization",
        "api key",
        "api-key",
        "api_key",
        "bearer",
        "oauth",
        "password",
        "secret",
        "token",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
    {
        return "vendor ACP process failed; sensitive stderr was redacted".into();
    }
    message
        .chars()
        .filter(|character| !character.is_control() || matches!(character, '\n' | '\t'))
        .take(1_024)
        .collect()
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
    #[cfg(unix)]
    use crate::external_agent::{RuntimeDiscoverySource, RuntimeInstallOrigin};
    use agent_client_protocol::schema::v1::{
        PermissionOptionId, SessionConfigOption, SessionConfigOptionCategory,
        SessionConfigSelectOption,
    };

    #[test]
    fn approval_decisions_select_semantic_options() {
        let options = vec![
            PermissionOption::new(
                PermissionOptionId::new("no"),
                "Reject",
                PermissionOptionKind::RejectOnce,
            ),
            PermissionOption::new(
                PermissionOptionId::new("yes"),
                "Allow",
                PermissionOptionKind::AllowOnce,
            ),
        ];
        let outcome = approval_outcome(&options, ExternalApprovalDecision::Accept).unwrap();
        assert!(matches!(outcome, RequestPermissionOutcome::Selected(_)));
        assert_eq!(
            serde_json::to_value(outcome).unwrap(),
            json!({"outcome":"selected","optionId":"yes"})
        );
    }

    #[test]
    fn session_approval_never_widens_to_allow_always() {
        let options = vec![PermissionOption::new(
            PermissionOptionId::new("permanent"),
            "Always allow",
            PermissionOptionKind::AllowAlways,
        )];
        assert_eq!(
            approval_outcome(&options, ExternalApprovalDecision::AcceptForSession).unwrap(),
            RequestPermissionOutcome::Cancelled
        );
    }

    #[test]
    fn model_catalog_uses_semantic_category() {
        let option = SessionConfigOption::select(
            "runtime-model",
            "Model",
            "gemini-2.5-pro",
            vec![SessionConfigSelectOption::new(
                "gemini-2.5-pro",
                "Gemini 2.5 Pro",
            )],
        )
        .category(SessionConfigOptionCategory::Model);
        let models = models_from_options(&[option]);
        assert_eq!(models.len(), 1);
        assert_eq!(models[0].id, "gemini-2.5-pro");
    }

    #[test]
    fn image_prompt_requires_matching_base64_data_uri() {
        let result = prompt_blocks(vec![ExternalPromptBlock::Image {
            uri: "file:///tmp/image.png".into(),
            media_type: "image/png".into(),
        }]);
        assert!(result.is_err());
    }

    #[test]
    fn default_claude_bridge_is_exact_and_subscription_safe() {
        let candidate = RuntimeCandidate {
            kind: ExternalAgentKind::Claude,
            executable: PathBuf::from("/usr/local/bin/claude"),
            real_executable: PathBuf::from("/opt/claude/claude"),
            origin: crate::external_agent::RuntimeInstallOrigin::VendorNative,
            discovered_by: vec![crate::external_agent::RuntimeDiscoverySource::Explicit],
            aliases: Vec::new(),
        };
        let config = AcpRuntimeConfig::pinned_claude_npx_bridge(
            RuntimeInstanceId::new("claude:test").unwrap(),
            candidate,
            RuntimeProfile::External,
            PathBuf::from("/usr/local/bin/npx"),
        )
        .unwrap();
        assert_eq!(config.launch_args, ["-y", PINNED_CLAUDE_ACP_NPM_PACKAGE]);
        assert_eq!(
            config
                .environment
                .get("CLAUDE_CODE_EXECUTABLE")
                .map(String::as_str),
            Some("/opt/claude/claude")
        );
        assert_eq!(
            config
                .environment
                .get("ANTHROPIC_API_KEY")
                .map(String::as_str),
            Some("")
        );
        assert_eq!(
            config
                .environment
                .get("ANTHROPIC_AUTH_TOKEN")
                .map(String::as_str),
            Some("")
        );
    }

    #[test]
    fn acp_error_messages_are_redacted_and_bounded() {
        let secret = safe_acp_error_message("exit: Authorization: Bearer top-secret".into());
        assert_eq!(
            secret,
            "vendor ACP process failed; sensitive stderr was redacted"
        );
        assert!(!secret.contains("top-secret"));

        let ordinary = safe_acp_error_message("x".repeat(2_000));
        assert_eq!(ordinary.len(), 1_024);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn official_acp_sdk_runs_a_real_stdio_session_without_a_model_call() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap();
        let executable = directory.path().join("fake-gemini-acp");
        std::fs::write(
            &executable,
            r###"#!/bin/sh
while IFS= read -r line; do
  id=$(printf '%s' "$line" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
  case "$line" in
    *'"method":"initialize"'*)
      printf '{"jsonrpc":"2.0","id":"%s","result":{"protocolVersion":1,"agentCapabilities":{},"authMethods":[],"agentInfo":{"name":"fake-gemini","version":"1.2.3"}}}\n' "$id"
      ;;
    *'"method":"session/new"'*)
      printf '{"jsonrpc":"2.0","id":"%s","result":{"sessionId":"session-1"}}\n' "$id"
      ;;
    *'"method":"session/prompt"'*)
      printf '{"jsonrpc":"2.0","id":"%s","result":{"stopReason":"max_tokens"}}\n' "$id"
      ;;
  esac
done
"###,
        )
        .unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
        let candidate = RuntimeCandidate {
            kind: ExternalAgentKind::Gemini,
            executable: executable.clone(),
            real_executable: executable,
            origin: RuntimeInstallOrigin::ExplicitPath,
            discovered_by: vec![RuntimeDiscoverySource::Explicit],
            aliases: Vec::new(),
        };
        let mut config = AcpRuntimeConfig::native(
            RuntimeInstanceId::new("gemini:test").unwrap(),
            candidate,
            RuntimeProfile::External,
        )
        .unwrap();
        config.default_workspace = Some(directory.path().to_path_buf());
        let runtime = AcpExternalRuntime::spawn(config).await.unwrap();
        let mut events = runtime.subscribe();
        let descriptor = runtime.initialize().await.unwrap();
        assert_eq!(descriptor.version.as_deref(), Some("1.2.3"));
        let session = runtime
            .new_session(NewRuntimeSession {
                cwd: directory.path().to_path_buf(),
                additional_directories: Vec::new(),
                model: None,
                mode: None,
            })
            .await
            .unwrap();
        runtime
            .prompt(ExternalPrompt {
                session_id: session.session_id,
                prompt_id: "prompt-1".into(),
                blocks: vec![ExternalPromptBlock::Text {
                    text: "offline fixture".into(),
                }],
            })
            .await
            .unwrap();
        let completed = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            let mut warned = false;
            loop {
                match events.recv().await.unwrap().event {
                    RuntimeEventKind::Warning { .. } => warned = true,
                    RuntimeEventKind::Completed => return warned,
                    _ => {}
                }
            }
        })
        .await;
        assert_eq!(completed, Ok(true), "ACP non-success stop was not terminal");
        runtime.shutdown().await.unwrap();
    }
}
