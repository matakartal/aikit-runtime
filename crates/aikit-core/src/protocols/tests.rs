use super::*;
use serde_json::{json, Value};

fn correlation(request_id: &str) -> CorrelationIdentity {
    CorrelationIdentity::new(format!("corr-{request_id}"), request_id).unwrap()
}

fn principal(scopes: &[&str]) -> ProtocolPrincipal {
    ProtocolPrincipal::new("user-1", scopes.iter().copied()).unwrap()
}

fn tenant_principal(tenant_id: &str, scopes: &[&str]) -> ProtocolPrincipal {
    principal(scopes).with_tenant(tenant_id).unwrap()
}

fn a2a_text_message(
    message_id: &str,
    context_id: Option<&str>,
    task_id: Option<&str>,
) -> A2aMessage {
    A2aMessage {
        message_id: message_id.into(),
        context_id: context_id.map(str::to_owned),
        task_id: task_id.map(str::to_owned),
        role: A2aRole::User,
        parts: vec![A2aPart::Text {
            text: "work".into(),
        }],
        metadata: Default::default(),
    }
}

fn assert_a2a_not_found(decision: GovernedAction<A2aAction>) -> String {
    let reason = match &decision.envelope.authorization {
        GovernanceAuthorization::Denied {
            code: GovernanceDenialCode::UnknownTarget,
            reason,
        } => reason.clone(),
        other => panic!("expected an unknown-target denial, got {other:?}"),
    };
    assert!(decision.action().is_none());
    assert_eq!(decision.envelope.correlation.session_id, None);
    assert_eq!(decision.envelope.correlation.run_id, None);
    let error = decision.into_authorized().unwrap_err();
    assert_eq!(error.code, ProtocolErrorCode::NotFound);
    assert_eq!(error.message, reason);
    reason
}

fn assert_a2a_snapshot_rejected(snapshot: Value, case: &str) {
    if let Ok(mapper) = serde_json::from_value::<A2aMapper>(snapshot) {
        panic!("tampered A2A snapshot was accepted for {case}: {mapper:?}");
    }
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
fn a2a_common_ingress_rejects_invalid_identity_and_unknown_part_fields() {
    let mut mapper = A2aMapper::new();
    let actor = tenant_principal("tenant-a", &["a2a:message:send"]);
    let invalid_principal: ProtocolPrincipal = serde_json::from_value(json!({
        "subject": "",
        "tenant_id": "tenant-a",
        "scopes": ["a2a:message:send"]
    }))
    .unwrap();
    let invalid_correlation: CorrelationIdentity = serde_json::from_value(json!({
        "correlation_id": "bad\ncorrelation",
        "request_id": "request-invalid"
    }))
    .unwrap();

    for denied in [
        mapper.prepare_send_message(
            a2a_text_message("invalid-principal-message", None, None),
            correlation("invalid-principal"),
            Some(&invalid_principal),
        ),
        mapper.prepare_send_message(
            a2a_text_message("invalid-correlation-message", None, None),
            invalid_correlation,
            Some(&actor),
        ),
    ] {
        assert!(matches!(
            denied.envelope.authorization,
            GovernanceAuthorization::Denied {
                code: GovernanceDenialCode::InvalidRequest,
                ..
            }
        ));
        assert!(denied.action().is_none());
        assert_eq!(
            denied.into_authorized().unwrap_err().code,
            ProtocolErrorCode::InvalidRequest
        );
    }
    assert!(mapper.tasks().is_empty());
    assert!(mapper.receipts().is_empty());

    let unknown_text_field = serde_json::from_value::<A2aPart>(json!({
        "kind": "text",
        "text": "hello",
        "unexpected": true
    }));
    assert!(unknown_text_field.is_err());
}

#[test]
fn a2a_foreign_and_unknown_tasks_are_indistinguishable_and_do_not_leak_runtime_ids() {
    let mut mapper = A2aMapper::new();
    let scopes = ["a2a:message:send", "a2a:tasks:read", "a2a:tasks:cancel"];
    let owner = tenant_principal("tenant-a", &scopes);
    let foreign = tenant_principal("tenant-b", &scopes);
    let created = mapper.prepare_send_message(
        a2a_text_message("private-message", Some("private-context"), None),
        correlation("private-create"),
        Some(&owner),
    );
    let mapping = match created.action().unwrap() {
        A2aAction::DispatchMessage { mapping, .. } => mapping.clone(),
        other => panic!("unexpected action: {other:?}"),
    };
    let unknown_task_id = "a2a-task-9999999999999999";

    let foreign_get = assert_a2a_not_found(mapper.prepare_get_task(
        &mapping.task_id,
        correlation("foreign-get"),
        Some(&foreign),
    ));
    let unknown_get = assert_a2a_not_found(mapper.prepare_get_task(
        unknown_task_id,
        correlation("unknown-get"),
        Some(&foreign),
    ));
    let foreign_cancel = assert_a2a_not_found(mapper.prepare_cancel_task(
        &mapping.task_id,
        correlation("foreign-cancel"),
        Some(&foreign),
    ));
    let unknown_cancel = assert_a2a_not_found(mapper.prepare_cancel_task(
        unknown_task_id,
        correlation("unknown-cancel"),
        Some(&foreign),
    ));
    let foreign_resume = assert_a2a_not_found(mapper.prepare_send_message(
        a2a_text_message("foreign-resume-message", None, Some(&mapping.task_id)),
        correlation("foreign-resume"),
        Some(&foreign),
    ));
    let unknown_resume = assert_a2a_not_found(mapper.prepare_send_message(
        a2a_text_message("unknown-resume-message", None, Some(unknown_task_id)),
        correlation("unknown-resume"),
        Some(&foreign),
    ));

    for reason in [
        unknown_get,
        foreign_cancel,
        unknown_cancel,
        foreign_resume,
        unknown_resume,
    ] {
        assert_eq!(reason, foreign_get);
    }
    assert_eq!(
        mapper.tasks()[&mapping.task_id].state,
        A2aTaskState::Working
    );
    assert_eq!(mapper.receipts().len(), 1);
}

#[test]
fn a2a_list_tasks_is_governed_scoped_filtered_and_cursor_paginated() {
    let mut mapper = A2aMapper::new();
    let scopes = ["a2a:message:send", "a2a:tasks:read"];
    let owner_a = tenant_principal("tenant-a", &scopes);
    let owner_b = tenant_principal("tenant-b", &scopes);
    let mut owner_a_tasks = Vec::new();
    for (message_id, context_id, actor) in [
        ("message-a-1", "context-shared", &owner_a),
        ("message-a-2", "context-other", &owner_a),
        ("message-a-3", "context-shared", &owner_a),
        ("message-b-1", "context-shared", &owner_b),
    ] {
        let created = mapper.prepare_send_message(
            A2aMessage {
                message_id: message_id.into(),
                context_id: Some(context_id.into()),
                task_id: None,
                role: A2aRole::User,
                parts: vec![A2aPart::Text {
                    text: "work".into(),
                }],
                metadata: Default::default(),
            },
            correlation(message_id),
            Some(actor),
        );
        assert!(created.is_authorized());
        if actor.tenant_id.as_deref() == Some("tenant-a") {
            let Some(A2aAction::DispatchMessage { mapping, .. }) = created.action() else {
                panic!("expected an A2A dispatch action");
            };
            owner_a_tasks.push(mapping.task_id.clone());
        }
    }
    // The oldest task becomes the most recently updated one. Revision order is deterministic
    // across serialization and is the canonical mapper's stable ordering clock.
    mapper
        .transition_task(
            &owner_a_tasks[0],
            A2aTaskState::Completed,
            Some("done".into()),
        )
        .unwrap();

    let first = mapper.prepare_list_tasks(
        A2aListTasksRequest {
            tenant: Some("tenant-a".into()),
            page_size: Some(2),
            ..Default::default()
        },
        correlation("list-a-first"),
        Some(&owner_a),
    );
    assert_eq!(first.envelope.operation, "tasks/list");
    assert_eq!(first.envelope.target, "tenant-a");
    let Some(A2aAction::ListTasks { page: first_page }) = first.action() else {
        panic!("expected an A2A task page");
    };
    assert_eq!(first_page.total_size, 3);
    assert_eq!(first_page.page_size, 2);
    assert_eq!(first_page.tasks.len(), 2);
    assert_eq!(first_page.tasks[0].mapping.task_id, owner_a_tasks[0]);
    assert_eq!(first_page.tasks[1].mapping.task_id, owner_a_tasks[2]);
    assert!(!first_page.next_page_token.is_empty());
    assert!(first_page.tasks.iter().all(|task| {
        task.owner_subject == owner_a.subject && task.owner_tenant_id.as_deref() == Some("tenant-a")
    }));

    let mismatched_cursor_query = mapper.prepare_list_tasks(
        A2aListTasksRequest {
            tenant: Some("tenant-a".into()),
            context_id: Some("context-shared".into()),
            page_size: Some(2),
            page_token: Some(first_page.next_page_token.clone()),
            ..Default::default()
        },
        correlation("list-a-cursor-query-mismatch"),
        Some(&owner_a),
    );
    assert!(matches!(
        mismatched_cursor_query.envelope.authorization,
        GovernanceAuthorization::Denied {
            code: GovernanceDenialCode::InvalidRequest,
            ..
        }
    ));

    let second = mapper.prepare_list_tasks(
        A2aListTasksRequest {
            tenant: Some("tenant-a".into()),
            page_size: Some(2),
            page_token: Some(first_page.next_page_token.clone()),
            ..Default::default()
        },
        correlation("list-a-second"),
        Some(&owner_a),
    );
    let Some(A2aAction::ListTasks { page: second_page }) = second.action() else {
        panic!("expected a second A2A task page");
    };
    assert_eq!(second_page.total_size, 3);
    assert_eq!(second_page.tasks.len(), 1);
    assert_eq!(second_page.tasks[0].mapping.task_id, owner_a_tasks[1]);
    assert_eq!(second_page.next_page_token, "");

    let filtered = mapper.prepare_list_tasks(
        A2aListTasksRequest {
            tenant: Some("tenant-a".into()),
            context_id: Some("context-shared".into()),
            status: Some(A2aTaskState::Working),
            ..Default::default()
        },
        correlation("list-a-filtered"),
        Some(&owner_a),
    );
    assert!(matches!(
        filtered.action(),
        Some(A2aAction::ListTasks { page })
            if page.total_size == 1 && page.tasks[0].mapping.task_id == owner_a_tasks[2]
    ));

    let explicit_empty_cursor = mapper.prepare_list_tasks(
        A2aListTasksRequest {
            tenant: Some("tenant-a".into()),
            page_size: Some(2),
            page_token: Some(String::new()),
            ..Default::default()
        },
        correlation("list-a-empty-cursor"),
        Some(&owner_a),
    );
    assert_eq!(explicit_empty_cursor.action(), first.action());

    // Serialized mapper state must reproduce the same authorized ordering and cursor contract.
    let restored: A2aMapper =
        serde_json::from_slice(&serde_json::to_vec(&mapper).unwrap()).unwrap();
    let restored_page = restored.prepare_list_tasks(
        A2aListTasksRequest {
            tenant: Some("tenant-a".into()),
            page_size: Some(2),
            ..Default::default()
        },
        correlation("list-a-restored"),
        Some(&owner_a),
    );
    assert_eq!(restored_page.action(), first.action());

    let send_only = tenant_principal("tenant-a", &["a2a:message:send"]);
    let missing_scope = mapper.prepare_list_tasks(
        A2aListTasksRequest::default(),
        correlation("list-without-scope"),
        Some(&send_only),
    );
    assert!(matches!(
        missing_scope.envelope.authorization,
        GovernanceAuthorization::Denied {
            code: GovernanceDenialCode::MissingScope,
            ..
        }
    ));
    assert!(missing_scope.action().is_none());

    let anonymous = mapper.prepare_list_tasks(
        A2aListTasksRequest::default(),
        correlation("list-anonymous"),
        None,
    );
    assert!(matches!(
        anonymous.envelope.authorization,
        GovernanceAuthorization::Denied {
            code: GovernanceDenialCode::MissingPrincipal,
            ..
        }
    ));
    assert_eq!(
        anonymous.into_authorized().unwrap_err().code,
        ProtocolErrorCode::Unauthorized
    );

    // A same-owner task transition changes the authorized snapshot and invalidates the cursor
    // instead of silently duplicating or skipping work.
    mapper
        .transition_task(
            &owner_a_tasks[1],
            A2aTaskState::Completed,
            Some("completed after page one".into()),
        )
        .unwrap();
    let stale_cursor = mapper.prepare_list_tasks(
        A2aListTasksRequest {
            tenant: Some("tenant-a".into()),
            page_size: Some(2),
            page_token: Some(first_page.next_page_token.clone()),
            ..Default::default()
        },
        correlation("list-a-stale-cursor"),
        Some(&owner_a),
    );
    assert!(matches!(
        stale_cursor.envelope.authorization,
        GovernanceAuthorization::Denied {
            code: GovernanceDenialCode::InvalidRequest,
            ..
        }
    ));
}

#[test]
fn a2a_list_tasks_cursor_ignores_unrelated_tenant_mutations() {
    let mut mapper = A2aMapper::new();
    let scopes = ["a2a:message:send", "a2a:tasks:read"];
    let owner_a = tenant_principal("tenant-a", &scopes);
    let owner_b = tenant_principal("tenant-b", &scopes);

    for message_id in ["cursor-a-1", "cursor-a-2", "cursor-a-3"] {
        let created = mapper.prepare_send_message(
            a2a_text_message(message_id, Some("cursor-context-a"), None),
            correlation(message_id),
            Some(&owner_a),
        );
        assert!(created.is_authorized());
    }
    let first = mapper.prepare_list_tasks(
        A2aListTasksRequest {
            tenant: Some("tenant-a".into()),
            page_size: Some(2),
            ..Default::default()
        },
        correlation("cursor-a-first"),
        Some(&owner_a),
    );
    let Some(A2aAction::ListTasks { page: first_page }) = first.action() else {
        panic!("expected an A2A task page");
    };
    assert!(!first_page.next_page_token.is_empty());

    let unrelated = mapper.prepare_send_message(
        a2a_text_message("cursor-b-1", Some("cursor-context-b"), None),
        correlation("cursor-b-create"),
        Some(&owner_b),
    );
    assert!(unrelated.is_authorized());

    let second = mapper.prepare_list_tasks(
        A2aListTasksRequest {
            tenant: Some("tenant-a".into()),
            page_size: Some(2),
            page_token: Some(first_page.next_page_token.clone()),
            ..Default::default()
        },
        correlation("cursor-a-second-after-b"),
        Some(&owner_a),
    );
    assert!(matches!(
        second.action(),
        Some(A2aAction::ListTasks { page })
            if page.total_size == 3 && page.tasks.len() == 1 && page.next_page_token.is_empty()
    ));
}

#[test]
fn a2a_list_tasks_rejects_cross_identity_tenant_and_cursor_inputs() {
    let mut mapper = A2aMapper::new();
    let scopes = ["a2a:message:send", "a2a:tasks:read"];
    let owner = tenant_principal("tenant-a", &scopes);
    let same_tenant_other_subject = ProtocolPrincipal::new("user-2", scopes)
        .unwrap()
        .with_tenant("tenant-a")
        .unwrap();
    let created = mapper.prepare_send_message(
        A2aMessage {
            message_id: "private-message".into(),
            context_id: Some("private-context".into()),
            task_id: None,
            role: A2aRole::User,
            parts: vec![A2aPart::Text {
                text: "private work".into(),
            }],
            metadata: Default::default(),
        },
        correlation("private-create"),
        Some(&owner),
    );
    assert!(created.is_authorized());

    let other_subject = mapper.prepare_list_tasks(
        A2aListTasksRequest {
            tenant: Some("tenant-a".into()),
            ..Default::default()
        },
        correlation("list-other-subject"),
        Some(&same_tenant_other_subject),
    );
    assert!(matches!(
        other_subject.action(),
        Some(A2aAction::ListTasks { page })
            if page.tasks.is_empty() && page.total_size == 0 && page.next_page_token.is_empty()
    ));

    let mismatched_tenant = mapper.prepare_list_tasks(
        A2aListTasksRequest {
            tenant: Some("tenant-b".into()),
            ..Default::default()
        },
        correlation("list-cross-tenant"),
        Some(&owner),
    );
    assert!(matches!(
        mismatched_tenant.envelope.authorization,
        GovernanceAuthorization::Denied {
            code: GovernanceDenialCode::PrincipalMismatch,
            ..
        }
    ));
    assert_eq!(
        mismatched_tenant.into_authorized().unwrap_err().code,
        ProtocolErrorCode::Forbidden
    );

    for request in [
        A2aListTasksRequest {
            page_size: Some(0),
            ..Default::default()
        },
        A2aListTasksRequest {
            page_size: Some(A2A_MAX_LIST_TASKS_PAGE_SIZE + 1),
            ..Default::default()
        },
        A2aListTasksRequest {
            page_token: Some("forged".into()),
            ..Default::default()
        },
    ] {
        let denied =
            mapper.prepare_list_tasks(request, correlation("list-invalid-input"), Some(&owner));
        assert!(matches!(
            denied.envelope.authorization,
            GovernanceAuthorization::Denied {
                code: GovernanceDenialCode::InvalidRequest,
                ..
            }
        ));
        assert_eq!(
            denied.into_authorized().unwrap_err().code,
            ProtocolErrorCode::InvalidRequest
        );
    }
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
    assert_eq!(
        mapper.context_session("context-42", &actor),
        Some(mapping.session_id.as_str())
    );
    assert_eq!(
        mapper.tasks()[&mapping.task_id].mapping.run_id,
        mapping.run_id
    );

    for event in mapper.pending_events() {
        mapper.mark_event_settled(&event.event_id).unwrap();
    }
    mapper
        .require_input(&mapping.task_id, "destination required")
        .unwrap();
    for event in mapper.pending_events() {
        mapper.mark_event_settled(&event.event_id).unwrap();
    }
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
fn a2a_generated_context_cannot_reuse_a_precreated_predictable_context() {
    let mut mapper = A2aMapper::new();
    let actor = principal(&["a2a:message:send"]);
    let precreated_context = "a2a-context-0000000000000004";
    let precreated = mapper.prepare_send_message(
        a2a_text_message("precreated-context-message", Some(precreated_context), None),
        correlation("precreated-context"),
        Some(&actor),
    );
    let Some(A2aAction::DispatchMessage {
        mapping: precreated_mapping,
        ..
    }) = precreated.action()
    else {
        panic!("expected the explicit context dispatch");
    };

    let generated = mapper.prepare_send_message(
        a2a_text_message("contextless-message", None, None),
        correlation("generated-context"),
        Some(&actor),
    );
    let Some(A2aAction::DispatchMessage {
        mapping: generated_mapping,
        ..
    }) = generated.action()
    else {
        panic!("expected the contextless dispatch");
    };

    assert_ne!(generated_mapping.context_id, precreated_context);
    assert!(generated_mapping
        .context_id
        .starts_with("a2a-context-random-"));
    assert_ne!(generated_mapping.session_id, precreated_mapping.session_id);
    assert_eq!(
        mapper.context_session(precreated_context, &actor),
        Some(precreated_mapping.session_id.as_str())
    );
    assert_eq!(
        mapper.context_session(&generated_mapping.context_id, &actor),
        Some(generated_mapping.session_id.as_str())
    );
}

#[test]
fn a2a_restores_legacy_context_and_receipt_keys_without_cross_tenant_reuse() {
    let mut mapper = A2aMapper::new();
    let actor_a = tenant_principal("tenant-a", &["a2a:message:send"]);
    let actor_b = tenant_principal("tenant-b", &["a2a:message:send"]);
    let first = mapper.prepare_send_message(
        A2aMessage {
            message_id: "legacy-message-1".into(),
            context_id: Some("legacy-context".into()),
            task_id: None,
            role: A2aRole::User,
            parts: vec![A2aPart::Text {
                text: "first".into(),
            }],
            metadata: Default::default(),
        },
        correlation("legacy-first"),
        Some(&actor_a),
    );
    let Some(A2aAction::DispatchMessage { mapping, .. }) = first.action() else {
        panic!("expected an A2A dispatch action");
    };
    let original_session = mapping.session_id.clone();

    // Recreate the pre-owner-scoped alpha snapshot shape: context and receipt maps used raw ids.
    let mut snapshot = serde_json::to_value(&mapper).unwrap();
    for field in ["contexts", "context_owners"] {
        let entries = snapshot[field].as_object_mut().unwrap();
        let value = entries
            .values()
            .next()
            .cloned()
            .expect("snapshot contains the context entry");
        entries.clear();
        entries.insert("legacy-context".into(), value);
    }
    {
        let entries = snapshot["receipts"].as_object_mut().unwrap();
        let value = entries
            .values()
            .next()
            .cloned()
            .expect("snapshot contains the receipt entry");
        entries.clear();
        entries.insert("legacy-message-1".into(), value);
    }
    let mut restored: A2aMapper = serde_json::from_value(snapshot).unwrap();
    assert_eq!(
        restored.context_session("legacy-context", &actor_a),
        Some(original_session.as_str())
    );
    assert_eq!(restored.context_session("legacy-context", &actor_b), None);
    assert!(restored
        .message_receipt("legacy-message-1", &actor_a)
        .is_some());
    assert!(!restored.contexts().contains_key("legacy-context"));
    assert!(!restored.receipts().contains_key("legacy-message-1"));

    let same_owner = restored.prepare_send_message(
        A2aMessage {
            message_id: "legacy-message-2".into(),
            context_id: Some("legacy-context".into()),
            task_id: None,
            role: A2aRole::User,
            parts: vec![A2aPart::Text {
                text: "same owner".into(),
            }],
            metadata: Default::default(),
        },
        correlation("legacy-same-owner"),
        Some(&actor_a),
    );
    assert!(matches!(
        same_owner.action(),
        Some(A2aAction::DispatchMessage { mapping, .. })
            if mapping.session_id == original_session
    ));

    let other_tenant = restored.prepare_send_message(
        A2aMessage {
            message_id: "legacy-message-b".into(),
            context_id: Some("legacy-context".into()),
            task_id: None,
            role: A2aRole::User,
            parts: vec![A2aPart::Text {
                text: "other tenant".into(),
            }],
            metadata: Default::default(),
        },
        correlation("legacy-other-tenant"),
        Some(&actor_b),
    );
    assert!(matches!(
        other_tenant.action(),
        Some(A2aAction::DispatchMessage { mapping, .. })
            if mapping.session_id != original_session
    ));
}

#[test]
fn a2a_legacy_raw_context_that_looks_canonical_cannot_block_another_owner() {
    let actor_a = tenant_principal("tenant-a", &["a2a:message:send"]);
    let actor_b = tenant_principal("tenant-b", &["a2a:message:send"]);

    // Obtain a real canonical-looking string controlled by another owner, then use that opaque
    // string as tenant B's wire context id in a legacy raw-key snapshot.
    let mut shape_source = A2aMapper::new();
    assert!(shape_source
        .prepare_send_message(
            a2a_text_message("shape-message", Some("shape-context"), None),
            correlation("shape-create"),
            Some(&actor_a),
        )
        .is_authorized());
    let crafted_context_id = shape_source
        .contexts()
        .keys()
        .next()
        .expect("canonical context key exists")
        .clone();

    let mut mapper = A2aMapper::new();
    let created = mapper.prepare_send_message(
        a2a_text_message("crafted-message-b", Some(&crafted_context_id), None),
        correlation("crafted-create-b"),
        Some(&actor_b),
    );
    let mapping_b = match created.action().unwrap() {
        A2aAction::DispatchMessage { mapping, .. } => mapping.clone(),
        other => panic!("unexpected action: {other:?}"),
    };
    let mut snapshot = serde_json::to_value(&mapper).unwrap();
    for field in ["contexts", "context_owners"] {
        let entries = snapshot[field].as_object_mut().unwrap();
        let value = entries
            .values()
            .next()
            .cloned()
            .expect("snapshot contains the context entry");
        entries.clear();
        entries.insert(crafted_context_id.clone(), value);
    }

    let mut restored: A2aMapper = serde_json::from_value(snapshot).unwrap();
    assert_eq!(
        restored.context_session(&crafted_context_id, &actor_b),
        Some(mapping_b.session_id.as_str())
    );
    assert_eq!(
        restored.context_session(&crafted_context_id, &actor_a),
        None
    );

    let created_a = restored.prepare_send_message(
        a2a_text_message("crafted-message-a", Some(&crafted_context_id), None),
        correlation("crafted-create-a"),
        Some(&actor_a),
    );
    assert!(matches!(
        created_a.action(),
        Some(A2aAction::DispatchMessage { mapping, .. })
            if mapping.session_id != mapping_b.session_id
                && mapping.task_id != mapping_b.task_id
    ));
}

#[test]
fn a2a_snapshot_tampering_is_rejected_before_state_can_be_reused() {
    let mut mapper = A2aMapper::new();
    let actor = tenant_principal("tenant-a", &["a2a:message:send"]);
    let created = mapper.prepare_send_message(
        a2a_text_message("snapshot-message", Some("snapshot-context"), None),
        correlation("snapshot-create"),
        Some(&actor),
    );
    let mapping = match created.action().unwrap() {
        A2aAction::DispatchMessage { mapping, .. } => mapping.clone(),
        other => panic!("unexpected action: {other:?}"),
    };
    let base = serde_json::to_value(&mapper).unwrap();

    let mut schema_version = base.clone();
    schema_version["schema_version"] = json!(A2A_MAPPER_SCHEMA_VERSION + 1);
    assert_a2a_snapshot_rejected(schema_version, "unsupported schema version");

    let mut reused_sequence = base.clone();
    reused_sequence["next_sequence"] = json!(1);
    assert_a2a_snapshot_rejected(reused_sequence, "reused generated sequence");

    let mut mismatched_task_mapping = base.clone();
    mismatched_task_mapping["tasks"][&mapping.task_id]["mapping"]["task_id"] =
        json!("a2a-task-9999999999999999");
    assert_a2a_snapshot_rejected(mismatched_task_mapping, "task index and mapping mismatch");

    let mut mismatched_context_owner = base.clone();
    mismatched_context_owner["context_owners"]
        .as_object_mut()
        .unwrap()
        .values_mut()
        .next()
        .expect("context owner exists")["tenant_id"] = json!("tenant-b");
    assert_a2a_snapshot_rejected(mismatched_context_owner, "context owner mismatch");

    let mut dangling_receipt = base.clone();
    let mut receipt = dangling_receipt["receipts"]
        .as_object()
        .unwrap()
        .values()
        .next()
        .cloned()
        .expect("receipt exists");
    receipt["message"]["message_id"] = json!("dangling-message");
    receipt["message"]["task_id"] = json!("a2a-task-9999999999999999");
    receipt["mapping"]["message_id"] = json!("dangling-message");
    receipt["mapping"]["task_id"] = json!("a2a-task-9999999999999999");
    dangling_receipt["receipts"]
        .as_object_mut()
        .unwrap()
        .insert("dangling-message".into(), receipt);
    assert_a2a_snapshot_rejected(dangling_receipt, "dangling receipt task");

    let mut unrepresented_revision = base.clone();
    unrepresented_revision["revision"] = json!(base["revision"].as_u64().unwrap() + 1);
    assert_a2a_snapshot_rejected(unrepresented_revision, "unrepresented revision");

    let other = tenant_principal("tenant-b", &["a2a:message:send"]);
    let second = mapper.prepare_send_message(
        a2a_text_message("snapshot-message-b", Some("snapshot-context-b"), None),
        correlation("snapshot-create-b"),
        Some(&other),
    );
    let second_mapping = match second.action().unwrap() {
        A2aAction::DispatchMessage { mapping, .. } => mapping.clone(),
        other => panic!("unexpected action: {other:?}"),
    };
    let two_tenants = serde_json::to_value(&mapper).unwrap();

    let mut shared_session = two_tenants.clone();
    shared_session["tasks"][&second_mapping.task_id]["mapping"]["session_id"] =
        json!(mapping.session_id);
    for receipt in shared_session["receipts"]
        .as_object_mut()
        .unwrap()
        .values_mut()
    {
        if receipt["mapping"]["task_id"] == second_mapping.task_id {
            receipt["mapping"]["session_id"] = json!(mapping.session_id);
        }
    }
    for session_id in shared_session["contexts"]
        .as_object_mut()
        .unwrap()
        .values_mut()
    {
        if *session_id == second_mapping.session_id {
            *session_id = json!(mapping.session_id);
        }
    }
    assert_a2a_snapshot_rejected(shared_session, "cross-owner session collision");

    let mut shared_run = two_tenants;
    shared_run["tasks"][&second_mapping.task_id]["mapping"]["run_id"] = json!(mapping.run_id);
    for receipt in shared_run["receipts"].as_object_mut().unwrap().values_mut() {
        if receipt["mapping"]["task_id"] == second_mapping.task_id {
            receipt["mapping"]["run_id"] = json!(mapping.run_id);
        }
    }
    assert_a2a_snapshot_rejected(shared_run, "duplicate runtime run id");
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
fn a2a_same_subject_in_another_tenant_isolates_receipts_contexts_and_tasks() {
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
        mapper
            .message_receipt("tenant-message-1", &owner)
            .expect("owner receipt exists")
            .owner_tenant_id
            .as_deref(),
        Some("tenant-a")
    );

    let cross_tenant_same_message = mapper.prepare_send_message(
        initial.clone(),
        correlation("a2a-cross-tenant-receipt"),
        Some(&attacker),
    );
    let cross_tenant_mapping = match cross_tenant_same_message.action().unwrap() {
        A2aAction::DispatchMessage { mapping, .. } => mapping.clone(),
        other => panic!("unexpected action: {other:?}"),
    };
    assert_ne!(cross_tenant_mapping.session_id, mapping.session_id);
    assert_ne!(cross_tenant_mapping.task_id, mapping.task_id);
    assert_ne!(cross_tenant_mapping.run_id, mapping.run_id);
    assert_eq!(mapper.receipts().len(), 2);
    assert_eq!(
        mapper
            .message_receipt("tenant-message-1", &attacker)
            .expect("other tenant receipt exists")
            .mapping,
        cross_tenant_mapping
    );

    let owner_duplicate = mapper.prepare_send_message(
        initial.clone(),
        correlation("a2a-owner-duplicate"),
        Some(&owner),
    );
    assert!(matches!(
        owner_duplicate.action(),
        Some(A2aAction::DuplicateMessage { receipt }) if receipt.mapping == mapping
    ));
    let attacker_duplicate = mapper.prepare_send_message(
        initial,
        correlation("a2a-attacker-duplicate"),
        Some(&attacker),
    );
    assert!(matches!(
        attacker_duplicate.action(),
        Some(A2aAction::DuplicateMessage { receipt })
            if receipt.mapping == cross_tenant_mapping
    ));
    assert_eq!(mapper.receipts().len(), 2);
    // Context ids are opaque inside an authenticated tenant. Reusing the same wire id in another
    // tenant creates an isolated session/task instead of exposing or blocking the owner's one.
    let isolated_context = mapper.prepare_send_message(
        A2aMessage {
            message_id: "tenant-message-2".into(),
            context_id: Some(mapping.context_id.clone()),
            task_id: None,
            role: A2aRole::User,
            parts: vec![A2aPart::Text {
                text: "independent tenant context".into(),
            }],
            metadata: Default::default(),
        },
        correlation("a2a-cross-tenant-context"),
        Some(&attacker),
    );
    let isolated_mapping = match isolated_context.action().unwrap() {
        A2aAction::DispatchMessage { mapping, .. } => mapping,
        other => panic!("unexpected action: {other:?}"),
    };
    assert_eq!(isolated_mapping.context_id, mapping.context_id);
    assert_ne!(isolated_mapping.session_id, mapping.session_id);
    assert_ne!(isolated_mapping.task_id, mapping.task_id);
    assert_eq!(
        mapper.tasks()[&isolated_mapping.task_id]
            .owner_tenant_id
            .as_deref(),
        Some("tenant-b")
    );
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
