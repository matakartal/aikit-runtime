//! Credentials-free, deterministic localhost SUT for the official A2A TCK.
//!
//! This is a test fixture, not a production server. It intentionally advertises only the
//! JSON-RPC binding and SSE streaming implemented by `A2aHttpJsonRpcServer`.

use aikit_core::{
    A2aAction, A2aAgentCapabilities, A2aAgentCard, A2aAgentInterface, A2aAgentSkill, A2aArtifact,
    A2aContentPart, A2aDispatchAck, A2aDispatchContext, A2aDispatchHost, A2aHttpAuthError,
    A2aHttpAuthenticator, A2aHttpConfig, A2aHttpHeaders, A2aHttpJsonRpcServer, A2aMapper,
    A2aMessage, A2aPart, A2aRole, A2aRunMapping, A2aTaskRecord, A2aTaskState, CancellationToken,
    GovernanceEnvelope, InMemoryA2aEventStore, InMemoryA2aMapperSnapshotStore, ProtocolError,
    ProtocolPrincipal, ProtocolResult,
};
use async_trait::async_trait;
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;

const SUT_ADDRESS: &str = "127.0.0.1:9999";
const SUT_URL: &str = "http://127.0.0.1:9999/";

// This external-crate example intentionally compiles the exact public 0.2 shapes. Adding a new
// `A2aPart` variant or a required `A2aTaskRecord` field makes this compatibility sentinel fail.
#[allow(dead_code)]
fn legacy_public_api_compatibility_sentinel(part: A2aPart) -> A2aTaskRecord {
    match part {
        A2aPart::Text { text: _ } => {}
        A2aPart::Data { data: _ } => {}
        A2aPart::File {
            uri: _,
            media_type: _,
        } => {}
    }
    A2aTaskRecord {
        mapping: A2aRunMapping {
            context_id: "compat-context".into(),
            session_id: "compat-session".into(),
            task_id: "compat-task".into(),
            run_id: "compat-run".into(),
            message_id: "compat-message".into(),
        },
        state: A2aTaskState::Working,
        owner_subject: "compat-owner".into(),
        owner_tenant_id: None,
        created_revision: 1,
        updated_revision: 1,
        status_message: None,
    }
}

struct TckAuthenticator;

impl A2aHttpAuthenticator for TckAuthenticator {
    fn authenticate(
        &self,
        _headers: &A2aHttpHeaders,
    ) -> Result<ProtocolPrincipal, A2aHttpAuthError> {
        Ok(ProtocolPrincipal::new(
            "a2a-tck",
            ["a2a:message:send", "a2a:tasks:read", "a2a:tasks:cancel"],
        )
        .expect("the static TCK principal is valid"))
    }
}

struct TckDispatchHost;

#[async_trait]
impl A2aDispatchHost for TckDispatchHost {
    async fn handle(
        &self,
        server: Arc<A2aHttpJsonRpcServer>,
        context: &A2aDispatchContext,
        _envelope: &GovernanceEnvelope,
        action: &A2aAction,
    ) -> ProtocolResult<A2aDispatchAck> {
        if context.cancellation.is_cancelled() {
            return Ok(A2aDispatchAck::Stopped);
        }

        let (task_id, message_id, context_id) = match action {
            A2aAction::DispatchMessage {
                message, mapping, ..
            } => (&mapping.task_id, &message.message_id, &mapping.context_id),
            A2aAction::DuplicateMessage { receipt } => (
                &receipt.mapping.task_id,
                &receipt.message.message_id,
                &receipt.mapping.context_id,
            ),
            // This fixture starts no external work, so observing CancelTask is itself the
            // deterministic execution fence required before acknowledging cancellation.
            A2aAction::CancelTask { .. } => return Ok(A2aDispatchAck::Stopped),
            A2aAction::GetTask { .. } | A2aAction::ListTasks { .. } => {
                return Ok(A2aDispatchAck::Settled)
            }
        };

        let artifact = if message_id.starts_with("tck-artifact-file-url") {
            Some(A2aArtifact {
                artifact_id: format!("{message_id}-artifact"),
                name: None,
                description: None,
                parts: vec![A2aContentPart::File {
                    uri: "https://example.com/output.txt".into(),
                    media_type: "text/plain".into(),
                    filename: Some("output.txt".into()),
                }],
                metadata: BTreeMap::new(),
            })
        } else if message_id.starts_with("tck-artifact-file") {
            Some(A2aArtifact {
                artifact_id: format!("{message_id}-artifact"),
                name: None,
                description: None,
                parts: vec![A2aContentPart::Raw {
                    raw: b"tck".to_vec(),
                    media_type: "text/plain".into(),
                    filename: Some("output.txt".into()),
                }],
                metadata: BTreeMap::new(),
            })
        } else if message_id.starts_with("tck-artifact-text") {
            Some(A2aArtifact {
                artifact_id: format!("{message_id}-artifact"),
                name: None,
                description: None,
                parts: vec![A2aContentPart::Text {
                    text: "Generated text content".into(),
                    media_type: None,
                }],
                metadata: BTreeMap::new(),
            })
        } else if message_id.starts_with("tck-artifact-data") {
            Some(A2aArtifact {
                artifact_id: format!("{message_id}-artifact"),
                name: None,
                description: None,
                parts: vec![A2aContentPart::Data {
                    data: json!({"key": "value", "count": 42}),
                    media_type: None,
                }],
                metadata: BTreeMap::new(),
            })
        } else {
            None
        };
        if let Some(artifact) = artifact {
            server
                .complete_task_with_artifacts(context, vec![artifact])
                .await?;
            return Ok(A2aDispatchAck::Settled);
        }
        if message_id.starts_with("tck-message-response") {
            server
                .complete_with_direct_message(
                    context,
                    A2aMessage {
                        message_id: format!("{message_id}-response"),
                        context_id: Some(context_id.clone()),
                        task_id: None,
                        role: A2aRole::Agent,
                        parts: vec![A2aPart::Text {
                            text: "Direct message response".into(),
                        }],
                        metadata: BTreeMap::new(),
                    },
                )
                .await?;
            return Ok(A2aDispatchAck::Settled);
        }

        // These prefixes are the official TCK's in-band signals for a non-terminal task.
        // Every other accepted message completes synchronously and deterministically.
        let desired = if message_id.starts_with("tck-input-required")
            || message_id.starts_with("test-resubscribe-message-id")
        {
            A2aTaskState::InputRequired
        } else {
            A2aTaskState::Completed
        };
        let snapshot = server.mapper_snapshot().await;
        let current = snapshot
            .tasks()
            .get(task_id)
            .map(|task| task.state)
            .ok_or_else(|| ProtocolError::not_found("A2A TCK task is not registered"))?;
        if current != desired && !current.is_terminal() {
            if context.cancellation.is_cancelled() {
                return Ok(A2aDispatchAck::Stopped);
            }
            server
                .transition_task(
                    task_id,
                    desired,
                    Some("deterministic A2A TCK fixture".into()),
                )
                .await?;
        }
        // The desired non-active state, or an existing terminal state, is now durable.
        Ok(A2aDispatchAck::Settled)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let agent_card = A2aAgentCard {
        name: "AIKit A2A TCK SUT".into(),
        description: "Ephemeral, credentials-free A2A conformance fixture".into(),
        version: env!("CARGO_PKG_VERSION").into(),
        capabilities: A2aAgentCapabilities {
            streaming: true,
            push_notifications: false,
            extended_agent_card: false,
        },
        skills: vec![A2aAgentSkill {
            id: "deterministic-task-transition".into(),
            name: "Deterministic task transition".into(),
            description: "Accepts A2A TCK messages and deterministically settles their tasks"
                .into(),
            tags: vec!["a2a-tck".into(), "deterministic".into()],
            examples: vec!["Send a text message and observe its task state".into()],
            input_modes: vec!["text/plain".into(), "application/json".into()],
            output_modes: vec!["text/plain".into(), "application/json".into()],
            security_requirements: Vec::new(),
        }],
        supported_interfaces: vec![A2aAgentInterface {
            url: SUT_URL.into(),
            protocol_binding: "JSONRPC".into(),
            protocol_version: "1.0".into(),
            tenant: None,
        }],
        default_input_modes: vec!["text/plain".into(), "application/json".into()],
        default_output_modes: vec!["text/plain".into(), "application/json".into()],
        security_schemes: BTreeMap::new(),
        security_requirements: Vec::new(),
    };
    let config = A2aHttpConfig {
        path: "/".into(),
        allowed_hosts: ["127.0.0.1".into(), "localhost".into()]
            .into_iter()
            .collect::<BTreeSet<_>>(),
        request_timeout: Duration::from_secs(10),
        blocking_dispatch_timeout: Duration::from_secs(8),
        stream_idle_timeout: Duration::from_secs(2),
        ..A2aHttpConfig::default()
    };

    let server = Arc::new(A2aHttpJsonRpcServer::new_owned(
        A2aMapper::new(),
        Arc::new(InMemoryA2aMapperSnapshotStore::default()),
        Arc::new(InMemoryA2aEventStore::default()),
        Arc::new(TckAuthenticator),
        Arc::new(TckDispatchHost),
        agent_card,
        config,
    )?);
    let listener = TcpListener::bind(SUT_ADDRESS).await?;
    eprintln!("AIKit A2A TCK SUT listening at {SUT_URL}");
    server.serve(listener, CancellationToken::new()).await?;
    Ok(())
}
