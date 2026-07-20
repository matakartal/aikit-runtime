use super::*;
use serde_json::json;

fn correlation(request_id: &str) -> CorrelationIdentity {
    CorrelationIdentity::new(format!("corr-{request_id}"), request_id).unwrap()
}

fn principal(scopes: &[&str]) -> ProtocolPrincipal {
    ProtocolPrincipal::new("user-1", scopes.iter().copied()).unwrap()
}

fn tenant_principal(tenant_id: &str, scopes: &[&str]) -> ProtocolPrincipal {
    principal(scopes).with_tenant(tenant_id).unwrap()
}

fn tool(name: &str) -> McpToolDefinition {
    McpToolDefinition::new(name, "deterministic test tool", json!({"type": "object"})).unwrap()
}

#[test]
fn mcp_authz_fails_closed_and_never_materializes_an_action() {
    let mut registry = McpServerRegistry::new();
    registry.register_tool(tool("write_file")).unwrap();

    let decision = registry.prepare_tool_call(
        McpToolCallRequest {
            name: "write_file".into(),
            arguments: json!({"path": "safe.txt"}),
            execution_mode: McpToolExecutionMode::Direct,
        },
        correlation("request-1"),
        None,
    );

    assert!(!decision.is_authorized());
    assert!(decision.action().is_none());
    assert!(matches!(
        decision.envelope.authorization,
        GovernanceAuthorization::Denied {
            code: GovernanceDenialCode::MissingPrincipal,
            ..
        }
    ));
    assert_eq!(decision.envelope.correlation.request_id, "request-1");
    assert!(registry.tasks().is_empty());
}

#[test]
fn mcp_cancel_is_authorized_owned_and_durable() {
    let mut registry = McpServerRegistry::new();
    let mut definition = tool("long_job");
    definition.task_support = McpTaskSupport::Optional;
    registry.register_tool(definition).unwrap();
    let actor = principal(&["mcp:tools:call", "mcp:tasks:cancel"]);

    let created = registry.prepare_tool_call(
        McpToolCallRequest {
            name: "long_job".into(),
            arguments: json!({"steps": 10}),
            execution_mode: McpToolExecutionMode::Task,
        },
        correlation("request-create"),
        Some(&actor),
    );
    let task_id = match created.action().unwrap() {
        McpServerAction::InvokeTool { task } => task.task_id.clone(),
        other => panic!("unexpected action: {other:?}"),
    };

    let cancelled =
        registry.prepare_cancel_task(&task_id, correlation("request-cancel"), Some(&actor));
    assert!(cancelled.is_authorized());
    assert_eq!(registry.tasks()[&task_id].status, McpTaskStatus::Working);
    assert_eq!(
        registry.tasks()[&task_id].cancellation,
        Some(McpCancellationState::Requested)
    );

    let encoded = serde_json::to_vec(&registry).unwrap();
    let mut restored: McpServerRegistry = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(
        restored.tasks()[&task_id].cancellation,
        Some(McpCancellationState::Requested)
    );
    restored.confirm_cancel_task(&task_id).unwrap();
    assert_eq!(restored.tasks()[&task_id].status, McpTaskStatus::Cancelled);
    assert!(restored
        .complete_task(&task_id, json!({"late": true}))
        .is_err());
}

#[test]
fn mcp_approval_resume_preserves_task_and_correlation_identity() {
    let mut registry = McpServerRegistry::new();
    let mut definition = tool("deploy");
    definition.requires_approval = true;
    registry.register_tool(definition).unwrap();
    let actor = principal(&["mcp:tools:call"]);

    let awaiting = registry.prepare_tool_call(
        McpToolCallRequest {
            name: "deploy".into(),
            arguments: json!({"environment": "staging"}),
            execution_mode: McpToolExecutionMode::Direct,
        },
        correlation("request-deploy"),
        Some(&actor),
    );
    let (task_id, approval_id, run_id) = match awaiting.action().unwrap() {
        McpServerAction::AwaitApproval { task, challenge } => (
            task.task_id.clone(),
            challenge.approval_id.clone(),
            awaiting.envelope.correlation.run_id.clone().unwrap(),
        ),
        other => panic!("unexpected action: {other:?}"),
    };
    assert_eq!(
        registry.tasks()[&task_id].status,
        McpTaskStatus::InputRequired
    );

    let resumed = registry.prepare_resume_approval(
        &task_id,
        McpApprovalResponse {
            approval_id,
            approved: true,
        },
        correlation("request-approve"),
        Some(&actor),
    );
    assert!(matches!(
        resumed.action(),
        Some(McpServerAction::InvokeTool { task })
            if task.task_id == task_id && task.status == McpTaskStatus::Working
    ));
    assert_eq!(
        resumed.envelope.correlation.run_id.as_deref(),
        Some(run_id.as_str())
    );
}

#[test]
fn a2a_duplicate_message_is_idempotent() {
    let mut mapper = A2aMapper::new();
    let actor = principal(&["a2a:message:send"]);
    let message = A2aMessage {
        message_id: "message-1".into(),
        context_id: Some("context-client".into()),
        task_id: None,
        role: A2aRole::User,
        parts: vec![A2aPart::Text {
            text: "do the work".into(),
        }],
        metadata: Default::default(),
    };

    let first =
        mapper.prepare_send_message(message.clone(), correlation("a2a-first"), Some(&actor));
    assert!(matches!(
        first.action(),
        Some(A2aAction::DispatchMessage { .. })
    ));
    let revision = mapper.revision();

    let duplicate = mapper.prepare_send_message(message, correlation("a2a-retry"), Some(&actor));
    assert!(matches!(
        duplicate.action(),
        Some(A2aAction::DuplicateMessage { .. })
    ));
    assert_eq!(mapper.tasks().len(), 1);
    assert_eq!(mapper.receipts().len(), 1);
    assert_eq!(mapper.revision(), revision);
}

#[test]
fn a2a_context_task_run_mapping_and_input_resume_are_stable() {
    let mut mapper = A2aMapper::new();
    let actor = principal(&["a2a:message:send"]);
    let first = mapper.prepare_send_message(
        A2aMessage {
            message_id: "message-initial".into(),
            context_id: Some("context-42".into()),
            task_id: None,
            role: A2aRole::User,
            parts: vec![A2aPart::Text {
                text: "book a flight".into(),
            }],
            metadata: Default::default(),
        },
        correlation("a2a-initial"),
        Some(&actor),
    );
    let mapping = match first.action().unwrap() {
        A2aAction::DispatchMessage { mapping, .. } => mapping.clone(),
        other => panic!("unexpected action: {other:?}"),
    };
    assert_eq!(mapper.contexts()["context-42"], mapping.session_id);
    assert_eq!(
        mapper.tasks()[&mapping.task_id].mapping.run_id,
        mapping.run_id
    );

    mapper
        .require_input(&mapping.task_id, "destination required")
        .unwrap();
    let follow_up = mapper.prepare_send_message(
        A2aMessage {
            message_id: "message-follow-up".into(),
            context_id: None,
            task_id: Some(mapping.task_id.clone()),
            role: A2aRole::User,
            parts: vec![A2aPart::Text {
                text: "London".into(),
            }],
            metadata: Default::default(),
        },
        correlation("a2a-follow-up"),
        Some(&actor),
    );
    match follow_up.action().unwrap() {
        A2aAction::DispatchMessage {
            mapping: resumed,
            resumed_from,
            ..
        } => {
            assert_eq!(resumed.context_id, mapping.context_id);
            assert_eq!(resumed.session_id, mapping.session_id);
            assert_eq!(resumed.task_id, mapping.task_id);
            assert_eq!(resumed.run_id, mapping.run_id);
            assert_eq!(*resumed_from, Some(A2aTaskState::InputRequired));
        }
        other => panic!("unexpected action: {other:?}"),
    }
    assert_eq!(
        mapper.tasks()[&mapping.task_id].state,
        A2aTaskState::Working
    );
}

#[test]
fn acp_session_and_runtime_events_map_without_provider_semantics() {
    let mut mapper = AcpSessionMapper::new();
    let actor = principal(&["acp:sessions:open", "acp:sessions:prompt"]);
    let opened =
        mapper.prepare_new_session(Some("zed-session-1"), correlation("acp-open"), Some(&actor));
    let session = match opened.action().unwrap() {
        AcpAction::OpenSession { mapping } => mapping.clone(),
        other => panic!("unexpected action: {other:?}"),
    };

    let prompted = mapper.prepare_prompt(
        AcpPromptRequest {
            acp_session_id: session.acp_session_id.clone(),
            prompt_id: "prompt-1".into(),
            blocks: vec![AcpPromptBlock::Text {
                text: "explain this file".into(),
            }],
            metadata: Default::default(),
        },
        correlation("acp-prompt"),
        Some(&actor),
    );
    let run = match prompted.action().unwrap() {
        AcpAction::Prompt { mapping, .. } => mapping.clone(),
        other => panic!("unexpected action: {other:?}"),
    };
    assert_eq!(run.session_id, session.session_id);
    assert_eq!(
        prompted.envelope.correlation.run_id,
        Some(run.run_id.clone())
    );

    let event = mapper
        .map_event(
            &run,
            AcpRuntimeEvent::AgentMessageChunk {
                text: "mapped".into(),
            },
        )
        .unwrap();
    assert_eq!(event.acp_session_id, "zed-session-1");
    assert_eq!(event.run_id, run.run_id);
    assert_eq!(
        event.update,
        AcpSessionUpdate::AgentMessageChunk {
            text: "mapped".into()
        }
    );
}

#[test]
fn mcp_same_subject_in_another_tenant_cannot_access_resume_or_cancel_task() {
    let mut registry = McpServerRegistry::new();
    let mut definition = tool("tenant_job");
    definition.requires_approval = true;
    definition.task_support = McpTaskSupport::Optional;
    registry.register_tool(definition).unwrap();
    let owner = tenant_principal(
        "tenant-a",
        &["mcp:tools:call", "mcp:tasks:read", "mcp:tasks:cancel"],
    );
    let attacker = tenant_principal(
        "tenant-b",
        &["mcp:tools:call", "mcp:tasks:read", "mcp:tasks:cancel"],
    );

    let created = registry.prepare_tool_call(
        McpToolCallRequest {
            name: "tenant_job".into(),
            arguments: json!({}),
            execution_mode: McpToolExecutionMode::Task,
        },
        correlation("mcp-tenant-create"),
        Some(&owner),
    );
    let (task_id, approval_id) = match created.action().unwrap() {
        McpServerAction::AwaitApproval { task, challenge } => {
            (task.task_id.clone(), challenge.approval_id.clone())
        }
        other => panic!("unexpected action: {other:?}"),
    };
    assert_eq!(
        registry.tasks()[&task_id].owner_tenant_id.as_deref(),
        Some("tenant-a")
    );

    assert!(!registry
        .prepare_get_task(
            &task_id,
            correlation("mcp-cross-tenant-get"),
            Some(&attacker),
        )
        .is_authorized());
    let listed = registry.prepare_list_tasks(correlation("mcp-cross-tenant-list"), Some(&attacker));
    assert!(matches!(
        listed.action(),
        Some(McpServerAction::ListTasks { tasks }) if tasks.is_empty()
    ));
    assert!(!registry
        .prepare_resume_approval(
            &task_id,
            McpApprovalResponse {
                approval_id: approval_id.clone(),
                approved: true,
            },
            correlation("mcp-cross-tenant-resume"),
            Some(&attacker),
        )
        .is_authorized());
    assert!(!registry
        .prepare_cancel_task(
            &task_id,
            correlation("mcp-cross-tenant-cancel"),
            Some(&attacker),
        )
        .is_authorized());
    assert_eq!(
        registry.tasks()[&task_id].status,
        McpTaskStatus::InputRequired
    );
    assert!(registry
        .prepare_get_task(&task_id, correlation("mcp-owner-get"), Some(&owner),)
        .is_authorized());
    assert!(registry
        .prepare_resume_approval(
            &task_id,
            McpApprovalResponse {
                approval_id,
                approved: true,
            },
            correlation("mcp-owner-resume"),
            Some(&owner),
        )
        .is_authorized());
    assert!(registry
        .prepare_cancel_task(&task_id, correlation("mcp-owner-cancel"), Some(&owner),)
        .is_authorized());
}

#[test]
fn a2a_same_subject_in_another_tenant_cannot_reuse_receipt_context_or_task() {
    let mut mapper = A2aMapper::new();
    let owner = tenant_principal(
        "tenant-a",
        &["a2a:message:send", "a2a:tasks:read", "a2a:tasks:cancel"],
    );
    let attacker = tenant_principal(
        "tenant-b",
        &["a2a:message:send", "a2a:tasks:read", "a2a:tasks:cancel"],
    );
    let initial = A2aMessage {
        message_id: "tenant-message-1".into(),
        context_id: Some("tenant-context-1".into()),
        task_id: None,
        role: A2aRole::User,
        parts: vec![A2aPart::Text {
            text: "start".into(),
        }],
        metadata: Default::default(),
    };
    let created = mapper.prepare_send_message(
        initial.clone(),
        correlation("a2a-tenant-create"),
        Some(&owner),
    );
    let mapping = match created.action().unwrap() {
        A2aAction::DispatchMessage { mapping, .. } => mapping.clone(),
        other => panic!("unexpected action: {other:?}"),
    };
    assert_eq!(
        mapper.tasks()[&mapping.task_id].owner_tenant_id.as_deref(),
        Some("tenant-a")
    );
    assert_eq!(
        mapper.receipts()["tenant-message-1"]
            .owner_tenant_id
            .as_deref(),
        Some("tenant-a")
    );

    assert!(!mapper
        .prepare_send_message(
            initial,
            correlation("a2a-cross-tenant-receipt"),
            Some(&attacker),
        )
        .is_authorized());
    assert!(!mapper
        .prepare_send_message(
            A2aMessage {
                message_id: "tenant-message-2".into(),
                context_id: Some(mapping.context_id.clone()),
                task_id: None,
                role: A2aRole::User,
                parts: vec![A2aPart::Text {
                    text: "reuse context".into(),
                }],
                metadata: Default::default(),
            },
            correlation("a2a-cross-tenant-context"),
            Some(&attacker),
        )
        .is_authorized());
    assert!(!mapper
        .prepare_send_message(
            A2aMessage {
                message_id: "tenant-message-3".into(),
                context_id: None,
                task_id: Some(mapping.task_id.clone()),
                role: A2aRole::User,
                parts: vec![A2aPart::Text {
                    text: "reuse task".into(),
                }],
                metadata: Default::default(),
            },
            correlation("a2a-cross-tenant-resume"),
            Some(&attacker),
        )
        .is_authorized());
    assert!(!mapper
        .prepare_get_task(
            &mapping.task_id,
            correlation("a2a-cross-tenant-get"),
            Some(&attacker),
        )
        .is_authorized());
    assert!(!mapper
        .prepare_cancel_task(
            &mapping.task_id,
            correlation("a2a-cross-tenant-cancel"),
            Some(&attacker),
        )
        .is_authorized());
    assert_eq!(
        mapper.tasks()[&mapping.task_id].state,
        A2aTaskState::Working
    );
    assert!(mapper
        .prepare_get_task(&mapping.task_id, correlation("a2a-owner-get"), Some(&owner),)
        .is_authorized());
    assert!(mapper
        .prepare_cancel_task(
            &mapping.task_id,
            correlation("a2a-owner-cancel"),
            Some(&owner),
        )
        .is_authorized());
}

#[test]
fn acp_same_subject_in_another_tenant_cannot_open_prompt_or_cancel_session() {
    let mut mapper = AcpSessionMapper::new();
    let owner = tenant_principal(
        "tenant-a",
        &[
            "acp:sessions:open",
            "acp:sessions:prompt",
            "acp:sessions:cancel",
        ],
    );
    let attacker = tenant_principal(
        "tenant-b",
        &[
            "acp:sessions:open",
            "acp:sessions:prompt",
            "acp:sessions:cancel",
        ],
    );
    let opened = mapper.prepare_new_session(
        Some("tenant-acp-session"),
        correlation("acp-tenant-open"),
        Some(&owner),
    );
    assert!(opened.is_authorized());
    assert_eq!(
        serde_json::to_value(&mapper).unwrap()["sessions"]["tenant-acp-session"]["owner_tenant_id"],
        "tenant-a"
    );

    assert!(!mapper
        .prepare_new_session(
            Some("tenant-acp-session"),
            correlation("acp-cross-tenant-open"),
            Some(&attacker),
        )
        .is_authorized());
    assert!(!mapper
        .prepare_prompt(
            AcpPromptRequest {
                acp_session_id: "tenant-acp-session".into(),
                prompt_id: "attacker-prompt".into(),
                blocks: vec![AcpPromptBlock::Text {
                    text: "cross tenant".into(),
                }],
                metadata: Default::default(),
            },
            correlation("acp-cross-tenant-prompt"),
            Some(&attacker),
        )
        .is_authorized());

    let prompted = mapper.prepare_prompt(
        AcpPromptRequest {
            acp_session_id: "tenant-acp-session".into(),
            prompt_id: "owner-prompt".into(),
            blocks: vec![AcpPromptBlock::Text {
                text: "owner run".into(),
            }],
            metadata: Default::default(),
        },
        correlation("acp-owner-prompt"),
        Some(&owner),
    );
    assert!(prompted.is_authorized());
    assert!(!mapper
        .prepare_cancel(
            "tenant-acp-session",
            correlation("acp-cross-tenant-cancel"),
            Some(&attacker),
        )
        .is_authorized());
    assert!(mapper.active_run("tenant-acp-session").is_some());
    assert!(mapper
        .prepare_cancel(
            "tenant-acp-session",
            correlation("acp-owner-cancel"),
            Some(&owner),
        )
        .is_authorized());
}
