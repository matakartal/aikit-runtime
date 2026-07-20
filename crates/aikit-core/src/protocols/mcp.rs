//! MCP server-side registry and durable task state machine.
//!
//! This is the server core, not a transport. Registry lookups produce governed intents for a host
//! to execute. The registry never calls a tool, reads a resource, or renders a prompt itself.

use super::common::{
    scopes, validate_identifier, validate_scope_set, CorrelationIdentity, GovernanceDenialCode,
    GovernanceEnvelope, GovernedAction, ProtocolError, ProtocolKind, ProtocolPrincipal,
    ProtocolResult,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

/// Base MCP revision whose task model these contracts follow.
pub const MCP_SERVER_CONTRACT_REVISION: &str = "2025-11-25";
/// Final MCP extension defining durable task get/update/cancel semantics.
pub const MCP_TASKS_EXTENSION: &str = "io.modelcontextprotocol/tasks";

const TOOLS_LIST_SCOPE: &str = "mcp:tools:list";
const TOOLS_CALL_SCOPE: &str = "mcp:tools:call";
const RESOURCES_LIST_SCOPE: &str = "mcp:resources:list";
const RESOURCES_READ_SCOPE: &str = "mcp:resources:read";
const PROMPTS_LIST_SCOPE: &str = "mcp:prompts:list";
const PROMPTS_GET_SCOPE: &str = "mcp:prompts:get";
const TASKS_READ_SCOPE: &str = "mcp:tasks:read";
const TASKS_CANCEL_SCOPE: &str = "mcp:tasks:cancel";

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpTaskSupport {
    #[default]
    Forbidden,
    Optional,
    Required,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpToolDefinition {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
    #[serde(default)]
    pub required_scopes: BTreeSet<String>,
    #[serde(default)]
    pub requires_approval: bool,
    #[serde(default)]
    pub task_support: McpTaskSupport,
}

impl McpToolDefinition {
    pub fn new(
        name: impl Into<String>,
        description: impl Into<String>,
        input_schema: Value,
    ) -> ProtocolResult<Self> {
        let definition = Self {
            name: name.into(),
            description: description.into(),
            input_schema,
            required_scopes: BTreeSet::new(),
            requires_approval: false,
            task_support: McpTaskSupport::Forbidden,
        };
        definition.validate()?;
        Ok(definition)
    }

    pub fn validate(&self) -> ProtocolResult<()> {
        validate_identifier("MCP tool name", &self.name)?;
        if !self.input_schema.is_object() {
            return Err(ProtocolError::invalid(
                "MCP tool input_schema must be a JSON object",
            ));
        }
        validate_scope_set(&self.required_scopes)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpResourceDefinition {
    pub uri: String,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
    #[serde(default)]
    pub required_scopes: BTreeSet<String>,
}

impl McpResourceDefinition {
    pub fn validate(&self) -> ProtocolResult<()> {
        validate_identifier("MCP resource URI", &self.uri)?;
        validate_identifier("MCP resource name", &self.name)?;
        validate_scope_set(&self.required_scopes)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpPromptArgument {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default)]
    pub required: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpPromptDefinition {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default)]
    pub arguments: Vec<McpPromptArgument>,
    #[serde(default)]
    pub required_scopes: BTreeSet<String>,
}

impl McpPromptDefinition {
    pub fn validate(&self) -> ProtocolResult<()> {
        validate_identifier("MCP prompt name", &self.name)?;
        let mut names = BTreeSet::new();
        for argument in &self.arguments {
            validate_identifier("MCP prompt argument name", &argument.name)?;
            if !names.insert(&argument.name) {
                return Err(ProtocolError::invalid(format!(
                    "duplicate MCP prompt argument: {}",
                    argument.name
                )));
            }
        }
        validate_scope_set(&self.required_scopes)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpToolExecutionMode {
    Direct,
    Task,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpToolCallRequest {
    pub name: String,
    #[serde(default)]
    pub arguments: Value,
    pub execution_mode: McpToolExecutionMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpTaskStatus {
    Working,
    InputRequired,
    Completed,
    Failed,
    Cancelled,
}

impl McpTaskStatus {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpProgress {
    pub progress: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

impl McpProgress {
    fn validate(&self) -> ProtocolResult<()> {
        if self.total.is_some_and(|total| self.progress > total) {
            return Err(ProtocolError::invalid(
                "MCP task progress cannot exceed total",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpApprovalChallenge {
    pub approval_id: String,
    pub prompt: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpApprovalResponse {
    pub approval_id: String,
    pub approved: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum McpTaskOperation {
    ToolCall { name: String, arguments: Value },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpTask {
    pub task_id: String,
    pub status: McpTaskStatus,
    pub operation: McpTaskOperation,
    pub correlation: CorrelationIdentity,
    pub owner_subject: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_tenant_id: Option<String>,
    #[serde(default)]
    pub required_scopes: BTreeSet<String>,
    pub advertised: bool,
    pub created_revision: u64,
    pub updated_revision: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status_message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub progress: Option<McpProgress>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_approval: Option<McpApprovalChallenge>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<ProtocolError>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum McpServerAction {
    ListTools {
        tools: Vec<McpToolDefinition>,
    },
    InvokeTool {
        task: McpTask,
    },
    AwaitApproval {
        task: McpTask,
        challenge: McpApprovalChallenge,
    },
    ListResources {
        resources: Vec<McpResourceDefinition>,
    },
    ReadResource {
        resource: McpResourceDefinition,
    },
    ListPrompts {
        prompts: Vec<McpPromptDefinition>,
    },
    RenderPrompt {
        prompt: McpPromptDefinition,
        arguments: BTreeMap<String, String>,
    },
    GetTask {
        task: McpTask,
    },
    ListTasks {
        tasks: Vec<McpTask>,
    },
    CancelTask {
        task: McpTask,
    },
    ApprovalDenied {
        task: McpTask,
    },
}

/// Serializable registry state. Serializing this value is sufficient to make every returned task
/// handle resolvable after a process restart.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpServerRegistry {
    tools: BTreeMap<String, McpToolDefinition>,
    resources: BTreeMap<String, McpResourceDefinition>,
    prompts: BTreeMap<String, McpPromptDefinition>,
    tasks: BTreeMap<String, McpTask>,
    next_task_sequence: u64,
    revision: u64,
}

impl Default for McpServerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl McpServerRegistry {
    pub fn new() -> Self {
        Self {
            tools: BTreeMap::new(),
            resources: BTreeMap::new(),
            prompts: BTreeMap::new(),
            tasks: BTreeMap::new(),
            next_task_sequence: 1,
            revision: 0,
        }
    }

    pub fn revision(&self) -> u64 {
        self.revision
    }

    pub fn tools(&self) -> &BTreeMap<String, McpToolDefinition> {
        &self.tools
    }

    pub fn resources(&self) -> &BTreeMap<String, McpResourceDefinition> {
        &self.resources
    }

    pub fn prompts(&self) -> &BTreeMap<String, McpPromptDefinition> {
        &self.prompts
    }

    pub fn tasks(&self) -> &BTreeMap<String, McpTask> {
        &self.tasks
    }

    pub fn register_tool(&mut self, definition: McpToolDefinition) -> ProtocolResult<()> {
        definition.validate()?;
        if self.tools.contains_key(&definition.name) {
            return Err(ProtocolError::conflict(format!(
                "MCP tool already registered: {}",
                definition.name
            )));
        }
        self.tools.insert(definition.name.clone(), definition);
        self.bump_revision();
        Ok(())
    }

    pub fn register_resource(&mut self, definition: McpResourceDefinition) -> ProtocolResult<()> {
        definition.validate()?;
        if self.resources.contains_key(&definition.uri) {
            return Err(ProtocolError::conflict(format!(
                "MCP resource already registered: {}",
                definition.uri
            )));
        }
        self.resources.insert(definition.uri.clone(), definition);
        self.bump_revision();
        Ok(())
    }

    pub fn register_prompt(&mut self, definition: McpPromptDefinition) -> ProtocolResult<()> {
        definition.validate()?;
        if self.prompts.contains_key(&definition.name) {
            return Err(ProtocolError::conflict(format!(
                "MCP prompt already registered: {}",
                definition.name
            )));
        }
        self.prompts.insert(definition.name.clone(), definition);
        self.bump_revision();
        Ok(())
    }

    pub fn prepare_list_tools(
        &self,
        correlation: CorrelationIdentity,
        principal: Option<&ProtocolPrincipal>,
    ) -> GovernedAction<McpServerAction> {
        let required = scopes(&[TOOLS_LIST_SCOPE]);
        let envelope = GovernanceEnvelope::evaluate(
            ProtocolKind::Mcp,
            correlation,
            principal,
            "tools/list",
            "tools",
            required,
        );
        let tools = principal.map_or_else(Vec::new, |principal| {
            self.tools
                .values()
                .filter(|tool| principal.allows(&tool.required_scopes))
                .cloned()
                .collect()
        });
        GovernedAction::from_envelope(envelope, McpServerAction::ListTools { tools })
    }

    pub fn prepare_tool_call(
        &mut self,
        request: McpToolCallRequest,
        correlation: CorrelationIdentity,
        principal: Option<&ProtocolPrincipal>,
    ) -> GovernedAction<McpServerAction> {
        let Some(tool) = self.tools.get(&request.name).cloned() else {
            let envelope = GovernanceEnvelope::evaluate(
                ProtocolKind::Mcp,
                correlation,
                principal,
                "tools/call",
                request.name,
                scopes(&[TOOLS_CALL_SCOPE]),
            )
            .deny(
                GovernanceDenialCode::UnknownTarget,
                "MCP tool is not registered",
            );
            return GovernedAction::denied(envelope);
        };

        let mut required = scopes(&[TOOLS_CALL_SCOPE]);
        required.extend(tool.required_scopes.iter().cloned());
        let mut envelope = GovernanceEnvelope::evaluate(
            ProtocolKind::Mcp,
            correlation,
            principal,
            "tools/call",
            tool.name.clone(),
            required.clone(),
        );
        if !envelope.authorization.is_allowed() {
            return GovernedAction::denied(envelope);
        }
        if !execution_mode_supported(tool.task_support, request.execution_mode) {
            envelope = envelope.deny(
                GovernanceDenialCode::StateConflict,
                "requested MCP task execution mode is not supported by this tool",
            );
            return GovernedAction::denied(envelope);
        }

        let principal = principal.expect("allowed envelope always has a principal");
        let task_id = self.next_task_id();
        let mut task_correlation = envelope.correlation.clone();
        if task_correlation.session_id.is_none() {
            task_correlation.session_id = Some(format!("mcp-session-{:016}", self.revision + 1));
        }
        task_correlation.run_id = Some(task_id.clone());
        envelope.correlation = task_correlation.clone();

        let pending_approval = tool.requires_approval.then(|| McpApprovalChallenge {
            approval_id: format!("{task_id}:approval"),
            prompt: format!("Approve MCP tool call `{}`", tool.name),
        });
        self.bump_revision();
        let status = if pending_approval.is_some() {
            McpTaskStatus::InputRequired
        } else {
            McpTaskStatus::Working
        };
        let task = McpTask {
            task_id: task_id.clone(),
            status,
            operation: McpTaskOperation::ToolCall {
                name: tool.name,
                arguments: request.arguments,
            },
            correlation: task_correlation,
            owner_subject: principal.subject.clone(),
            owner_tenant_id: principal.tenant_id.clone(),
            required_scopes: required,
            advertised: request.execution_mode == McpToolExecutionMode::Task,
            created_revision: self.revision,
            updated_revision: self.revision,
            status_message: None,
            progress: None,
            pending_approval: pending_approval.clone(),
            result: None,
            error: None,
        };
        // The task is inserted before its handle is returned: callers may persist this registry
        // immediately and a subsequent tasks/get can always resolve the advertised task id.
        self.tasks.insert(task_id, task.clone());

        let action = match pending_approval {
            Some(challenge) => McpServerAction::AwaitApproval { task, challenge },
            None => McpServerAction::InvokeTool { task },
        };
        GovernedAction::from_envelope(envelope, action)
    }

    pub fn prepare_resume_approval(
        &mut self,
        task_id: &str,
        response: McpApprovalResponse,
        correlation: CorrelationIdentity,
        principal: Option<&ProtocolPrincipal>,
    ) -> GovernedAction<McpServerAction> {
        let Some(existing) = self.tasks.get(task_id).cloned() else {
            return denied_unknown_task(
                correlation,
                principal,
                "tasks/update",
                task_id,
                scopes(&[TOOLS_CALL_SCOPE]),
            );
        };
        let mut envelope = GovernanceEnvelope::evaluate(
            ProtocolKind::Mcp,
            correlation_with_task(correlation, &existing),
            principal,
            "tasks/update",
            task_id,
            existing.required_scopes.clone(),
        );
        if !envelope.authorization.is_allowed() {
            return GovernedAction::denied(envelope);
        }
        if principal.is_none_or(|value| {
            !value.matches_identity(&existing.owner_subject, existing.owner_tenant_id.as_deref())
        }) {
            envelope = envelope.deny(
                GovernanceDenialCode::PrincipalMismatch,
                "MCP task is not accessible",
            );
            return GovernedAction::denied(envelope);
        }
        let Some(challenge) = existing.pending_approval.as_ref() else {
            envelope = envelope.deny(
                GovernanceDenialCode::InvalidApproval,
                "MCP task has no pending approval",
            );
            return GovernedAction::denied(envelope);
        };
        if existing.status != McpTaskStatus::InputRequired
            || challenge.approval_id != response.approval_id
        {
            envelope = envelope.deny(
                GovernanceDenialCode::InvalidApproval,
                "MCP approval does not match the pending challenge",
            );
            return GovernedAction::denied(envelope);
        }

        self.bump_revision();
        let task = self
            .tasks
            .get_mut(task_id)
            .expect("task was resolved before mutation");
        task.updated_revision = self.revision;
        task.pending_approval = None;
        task.status_message = None;
        if response.approved {
            task.status = McpTaskStatus::Working;
            GovernedAction::from_envelope(
                envelope,
                McpServerAction::InvokeTool { task: task.clone() },
            )
        } else {
            task.status = McpTaskStatus::Cancelled;
            task.status_message = Some("approval denied".into());
            GovernedAction::from_envelope(
                envelope,
                McpServerAction::ApprovalDenied { task: task.clone() },
            )
        }
    }

    pub fn prepare_list_resources(
        &self,
        correlation: CorrelationIdentity,
        principal: Option<&ProtocolPrincipal>,
    ) -> GovernedAction<McpServerAction> {
        let envelope = GovernanceEnvelope::evaluate(
            ProtocolKind::Mcp,
            correlation,
            principal,
            "resources/list",
            "resources",
            scopes(&[RESOURCES_LIST_SCOPE]),
        );
        let resources = principal.map_or_else(Vec::new, |principal| {
            self.resources
                .values()
                .filter(|resource| principal.allows(&resource.required_scopes))
                .cloned()
                .collect()
        });
        GovernedAction::from_envelope(envelope, McpServerAction::ListResources { resources })
    }

    pub fn prepare_read_resource(
        &self,
        uri: &str,
        correlation: CorrelationIdentity,
        principal: Option<&ProtocolPrincipal>,
    ) -> GovernedAction<McpServerAction> {
        let Some(resource) = self.resources.get(uri).cloned() else {
            let envelope = GovernanceEnvelope::evaluate(
                ProtocolKind::Mcp,
                correlation,
                principal,
                "resources/read",
                uri,
                scopes(&[RESOURCES_READ_SCOPE]),
            )
            .deny(
                GovernanceDenialCode::UnknownTarget,
                "MCP resource is not registered",
            );
            return GovernedAction::denied(envelope);
        };
        let mut required = scopes(&[RESOURCES_READ_SCOPE]);
        required.extend(resource.required_scopes.iter().cloned());
        let envelope = GovernanceEnvelope::evaluate(
            ProtocolKind::Mcp,
            correlation,
            principal,
            "resources/read",
            uri,
            required,
        );
        GovernedAction::from_envelope(envelope, McpServerAction::ReadResource { resource })
    }

    pub fn prepare_list_prompts(
        &self,
        correlation: CorrelationIdentity,
        principal: Option<&ProtocolPrincipal>,
    ) -> GovernedAction<McpServerAction> {
        let envelope = GovernanceEnvelope::evaluate(
            ProtocolKind::Mcp,
            correlation,
            principal,
            "prompts/list",
            "prompts",
            scopes(&[PROMPTS_LIST_SCOPE]),
        );
        let prompts = principal.map_or_else(Vec::new, |principal| {
            self.prompts
                .values()
                .filter(|prompt| principal.allows(&prompt.required_scopes))
                .cloned()
                .collect()
        });
        GovernedAction::from_envelope(envelope, McpServerAction::ListPrompts { prompts })
    }

    pub fn prepare_render_prompt(
        &self,
        name: &str,
        arguments: BTreeMap<String, String>,
        correlation: CorrelationIdentity,
        principal: Option<&ProtocolPrincipal>,
    ) -> GovernedAction<McpServerAction> {
        let Some(prompt) = self.prompts.get(name).cloned() else {
            let envelope = GovernanceEnvelope::evaluate(
                ProtocolKind::Mcp,
                correlation,
                principal,
                "prompts/get",
                name,
                scopes(&[PROMPTS_GET_SCOPE]),
            )
            .deny(
                GovernanceDenialCode::UnknownTarget,
                "MCP prompt is not registered",
            );
            return GovernedAction::denied(envelope);
        };
        let mut required = scopes(&[PROMPTS_GET_SCOPE]);
        required.extend(prompt.required_scopes.iter().cloned());
        let mut envelope = GovernanceEnvelope::evaluate(
            ProtocolKind::Mcp,
            correlation,
            principal,
            "prompts/get",
            name,
            required,
        );
        if envelope.authorization.is_allowed()
            && prompt
                .arguments
                .iter()
                .any(|argument| argument.required && !arguments.contains_key(&argument.name))
        {
            envelope = envelope.deny(
                GovernanceDenialCode::StateConflict,
                "MCP prompt is missing a required argument",
            );
        }
        GovernedAction::from_envelope(
            envelope,
            McpServerAction::RenderPrompt { prompt, arguments },
        )
    }

    pub fn prepare_get_task(
        &self,
        task_id: &str,
        correlation: CorrelationIdentity,
        principal: Option<&ProtocolPrincipal>,
    ) -> GovernedAction<McpServerAction> {
        let Some(task) = self.tasks.get(task_id).cloned() else {
            return denied_unknown_task(
                correlation,
                principal,
                "tasks/get",
                task_id,
                scopes(&[TASKS_READ_SCOPE]),
            );
        };
        let envelope =
            self.task_envelope(&task, correlation, principal, "tasks/get", TASKS_READ_SCOPE);
        GovernedAction::from_envelope(envelope, McpServerAction::GetTask { task })
    }

    pub fn prepare_list_tasks(
        &self,
        correlation: CorrelationIdentity,
        principal: Option<&ProtocolPrincipal>,
    ) -> GovernedAction<McpServerAction> {
        let envelope = GovernanceEnvelope::evaluate(
            ProtocolKind::Mcp,
            correlation,
            principal,
            "tasks/list",
            "tasks",
            scopes(&[TASKS_READ_SCOPE]),
        );
        let tasks = principal.map_or_else(Vec::new, |principal| {
            self.tasks
                .values()
                .filter(|task| {
                    principal.matches_identity(&task.owner_subject, task.owner_tenant_id.as_deref())
                })
                .cloned()
                .collect()
        });
        GovernedAction::from_envelope(envelope, McpServerAction::ListTasks { tasks })
    }

    pub fn prepare_cancel_task(
        &mut self,
        task_id: &str,
        correlation: CorrelationIdentity,
        principal: Option<&ProtocolPrincipal>,
    ) -> GovernedAction<McpServerAction> {
        let Some(existing) = self.tasks.get(task_id).cloned() else {
            return denied_unknown_task(
                correlation,
                principal,
                "tasks/cancel",
                task_id,
                scopes(&[TASKS_CANCEL_SCOPE]),
            );
        };
        let mut envelope = self.task_envelope(
            &existing,
            correlation,
            principal,
            "tasks/cancel",
            TASKS_CANCEL_SCOPE,
        );
        if !envelope.authorization.is_allowed() {
            return GovernedAction::denied(envelope);
        }
        if existing.status == McpTaskStatus::Cancelled {
            return GovernedAction::from_envelope(
                envelope,
                McpServerAction::CancelTask { task: existing },
            );
        }
        if existing.status.is_terminal() {
            envelope = envelope.deny(
                GovernanceDenialCode::StateConflict,
                "terminal MCP task cannot be cancelled",
            );
            return GovernedAction::denied(envelope);
        }

        self.bump_revision();
        let task = self
            .tasks
            .get_mut(task_id)
            .expect("task was resolved before cancellation");
        task.status = McpTaskStatus::Cancelled;
        task.status_message = Some("cancellation requested".into());
        task.pending_approval = None;
        task.updated_revision = self.revision;
        GovernedAction::from_envelope(envelope, McpServerAction::CancelTask { task: task.clone() })
    }

    /// Receiver-side progress update. This is not protocol ingress and does not execute work.
    pub fn record_progress(&mut self, task_id: &str, progress: McpProgress) -> ProtocolResult<()> {
        progress.validate()?;
        let existing = self
            .tasks
            .get(task_id)
            .ok_or_else(|| ProtocolError::not_found("MCP task is not registered"))?;
        if existing.status != McpTaskStatus::Working {
            return Err(ProtocolError::invalid_transition(
                "progress requires a working MCP task",
            ));
        }
        if existing
            .progress
            .as_ref()
            .is_some_and(|previous| progress.progress < previous.progress)
        {
            return Err(ProtocolError::invalid_transition(
                "MCP task progress must be monotonic",
            ));
        }
        self.bump_revision();
        let task = self.tasks.get_mut(task_id).expect("task exists");
        task.progress = Some(progress);
        task.updated_revision = self.revision;
        Ok(())
    }

    pub fn complete_task(&mut self, task_id: &str, result: Value) -> ProtocolResult<()> {
        self.finish_task(task_id, McpTaskStatus::Completed, Some(result), None)
    }

    pub fn fail_task(&mut self, task_id: &str, error: ProtocolError) -> ProtocolResult<()> {
        self.finish_task(task_id, McpTaskStatus::Failed, None, Some(error))
    }

    fn finish_task(
        &mut self,
        task_id: &str,
        status: McpTaskStatus,
        result: Option<Value>,
        error: Option<ProtocolError>,
    ) -> ProtocolResult<()> {
        let current = self
            .tasks
            .get(task_id)
            .ok_or_else(|| ProtocolError::not_found("MCP task is not registered"))?
            .status;
        if current != McpTaskStatus::Working {
            return Err(ProtocolError::invalid_transition(
                "only a working MCP task may finish",
            ));
        }
        self.bump_revision();
        let task = self.tasks.get_mut(task_id).expect("task exists");
        task.status = status;
        task.result = result;
        task.error = error;
        task.updated_revision = self.revision;
        Ok(())
    }

    fn task_envelope(
        &self,
        task: &McpTask,
        correlation: CorrelationIdentity,
        principal: Option<&ProtocolPrincipal>,
        operation: &str,
        scope: &str,
    ) -> GovernanceEnvelope {
        let mut envelope = GovernanceEnvelope::evaluate(
            ProtocolKind::Mcp,
            correlation_with_task(correlation, task),
            principal,
            operation,
            task.task_id.clone(),
            scopes(&[scope]),
        );
        if envelope.authorization.is_allowed()
            && principal.is_none_or(|value| {
                !value.matches_identity(&task.owner_subject, task.owner_tenant_id.as_deref())
            })
        {
            envelope = envelope.deny(
                GovernanceDenialCode::PrincipalMismatch,
                "MCP task is not accessible",
            );
        }
        envelope
    }

    fn next_task_id(&mut self) -> String {
        let sequence = self.next_task_sequence;
        self.next_task_sequence = self.next_task_sequence.saturating_add(1);
        format!("mcp-task-{sequence:016}")
    }

    fn bump_revision(&mut self) {
        self.revision = self.revision.saturating_add(1);
    }
}

fn execution_mode_supported(support: McpTaskSupport, mode: McpToolExecutionMode) -> bool {
    matches!(
        (support, mode),
        (McpTaskSupport::Forbidden, McpToolExecutionMode::Direct)
            | (McpTaskSupport::Optional, _)
            | (McpTaskSupport::Required, McpToolExecutionMode::Task)
    )
}

fn correlation_with_task(
    mut correlation: CorrelationIdentity,
    task: &McpTask,
) -> CorrelationIdentity {
    correlation.session_id = task.correlation.session_id.clone();
    correlation.run_id = task.correlation.run_id.clone();
    correlation
}

fn denied_unknown_task(
    correlation: CorrelationIdentity,
    principal: Option<&ProtocolPrincipal>,
    operation: &str,
    task_id: &str,
    required_scopes: BTreeSet<String>,
) -> GovernedAction<McpServerAction> {
    let envelope = GovernanceEnvelope::evaluate(
        ProtocolKind::Mcp,
        correlation,
        principal,
        operation,
        task_id,
        required_scopes,
    )
    .deny(
        GovernanceDenialCode::UnknownTarget,
        "MCP task is not accessible",
    );
    GovernedAction::denied(envelope)
}
