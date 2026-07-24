//! Credentials-free, deterministic localhost SUT for the official A2A TCK.
//!
//! This is a test fixture, not a production server. It intentionally advertises only the
//! JSON-RPC binding and SSE streaming implemented by `A2aHttpJsonRpcServer`.

use aikit_core::{
    A2aAction, A2aAgentCapabilities, A2aAgentCard, A2aAgentInterface, A2aAgentSkill, A2aArtifact,
    A2aContentPart, A2aDispatchAck, A2aDispatchContext, A2aDispatchHost, A2aDispatchOutboxRecord,
    A2aDispatchOutboxState, A2aHttpAuthError, A2aHttpAuthenticator, A2aHttpConfig, A2aHttpHeaders,
    A2aHttpJsonRpcServer, A2aMapper, A2aMessage, A2aPart, A2aRole, A2aRunMapping, A2aTaskRecord,
    A2aTaskState, A2aUnknownDispatchDecision, CancellationToken, GovernanceEnvelope,
    InMemoryA2aEventStore, InMemoryA2aMapperSnapshotStore, ProtocolError, ProtocolPrincipal,
    ProtocolResult,
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
    async fn reconcile_unknown(
        &self,
        _server: Arc<A2aHttpJsonRpcServer>,
        _record: &A2aDispatchOutboxRecord,
    ) -> ProtocolResult<A2aUnknownDispatchDecision> {
        // This fixture performs no external work before its exact mapper transition. A recovery
        // scan that overlaps a live acceptance may therefore coalesce with, or safely retry, the
        // same durable dispatch without risking a duplicate side effect.
        Ok(A2aUnknownDispatchDecision::SafeToRetry)
    }

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
            A2aAction::DuplicateMessage { receipt } => {
                let snapshot = server.mapper_snapshot().await;
                let task = snapshot
                    .tasks()
                    .get(&receipt.mapping.task_id)
                    .ok_or_else(|| ProtocolError::not_found("A2A TCK task is not registered"))?;
                let settled = snapshot.dispatch_outbox().values().any(|dispatch| {
                    dispatch.task_id == receipt.mapping.task_id
                        && dispatch.message_id == receipt.message.message_id
                        && dispatch.state == A2aDispatchOutboxState::Settled
                });
                if settled && !matches!(task.state, A2aTaskState::Submitted | A2aTaskState::Working)
                {
                    // The transport projects the durable Task or direct Message response. The
                    // fixture must not execute or transition an already-settled idempotent retry.
                    return Ok(A2aDispatchAck::Settled);
                }
                return Err(ProtocolError::conflict(
                    "A2A TCK duplicate dispatch is not durably settled",
                ));
            }
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
                .transition_dispatch_task(
                    context,
                    desired,
                    Some("deterministic A2A TCK fixture".into()),
                )
                .await?;
        }
        // The desired non-active state, or an existing terminal state, is now durable.
        Ok(A2aDispatchAck::Settled)
    }
}

fn agent_card(url: &str) -> A2aAgentCard {
    A2aAgentCard {
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
            url: url.into(),
            protocol_binding: "JSONRPC".into(),
            protocol_version: "1.0".into(),
            tenant: None,
        }],
        default_input_modes: vec!["text/plain".into(), "application/json".into()],
        default_output_modes: vec!["text/plain".into(), "application/json".into()],
        security_schemes: BTreeMap::new(),
        security_requirements: Vec::new(),
    }
}

fn http_config() -> A2aHttpConfig {
    A2aHttpConfig {
        path: "/".into(),
        allowed_hosts: ["127.0.0.1".into(), "localhost".into()]
            .into_iter()
            .collect::<BTreeSet<_>>(),
        request_timeout: Duration::from_secs(10),
        blocking_dispatch_timeout: Duration::from_secs(8),
        stream_idle_timeout: Duration::from_secs(2),
        ..A2aHttpConfig::default()
    }
}

fn server(url: &str) -> ProtocolResult<Arc<A2aHttpJsonRpcServer>> {
    Ok(Arc::new(A2aHttpJsonRpcServer::new_owned(
        A2aMapper::new(),
        Arc::new(InMemoryA2aMapperSnapshotStore::default()),
        Arc::new(InMemoryA2aEventStore::default()),
        Arc::new(TckAuthenticator),
        Arc::new(TckDispatchHost),
        agent_card(url),
        http_config(),
    )?))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let server = server(SUT_URL)?;
    let listener = TcpListener::bind(SUT_ADDRESS).await?;
    eprintln!("AIKit A2A TCK SUT listening at {SUT_URL}");
    server.serve(listener, CancellationToken::new()).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use std::net::SocketAddr;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    use tokio::time::timeout;

    async fn post_jsonrpc(address: SocketAddr, accept: &str, payload: Value) -> String {
        timeout(Duration::from_secs(4), async {
            let body = payload.to_string();
            let request = format!(
                "POST / HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/json\r\nAccept: {accept}\r\nA2A-Version: 1.0\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let mut stream = TcpStream::connect(address).await.unwrap();
            stream.write_all(request.as_bytes()).await.unwrap();
            let mut response = Vec::new();
            stream.read_to_end(&mut response).await.unwrap();
            String::from_utf8(response).unwrap()
        })
        .await
        .expect("official-shaped A2A request timed out")
    }

    fn response_body(response: &str) -> Value {
        serde_json::from_str(response.split_once("\r\n\r\n").unwrap().1)
            .expect("A2A fixture returned invalid JSON")
    }

    #[tokio::test]
    async fn official_send_and_stream_shapes_settle_the_exact_dispatch() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let cancellation = CancellationToken::new();
        let handle = cancellation.handle();
        let server = server(&format!("http://{address}/")).unwrap();
        let task = tokio::spawn(server.serve(listener, cancellation));

        let send = post_jsonrpc(
            address,
            "application/json",
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "SendMessage",
                "params": {
                    "message": {
                        "role": "ROLE_USER",
                        "parts": [{"text": "Hello from TCK"}],
                        "messageId": "tck-complete-task-regression-jsonrpc"
                    }
                }
            }),
        )
        .await;
        assert!(send.starts_with("HTTP/1.1 200"), "{send}");
        let send_body = response_body(&send);
        assert_eq!(send_body["jsonrpc"], "2.0");
        assert_eq!(send_body["id"], 1);
        assert!(send_body.get("error").is_none(), "{send_body}");
        assert_eq!(
            send_body["result"]["task"]["status"]["state"],
            "TASK_STATE_COMPLETED"
        );

        let streaming = post_jsonrpc(
            address,
            "text/event-stream",
            json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "SendStreamingMessage",
                "params": {
                    "message": {
                        "role": "ROLE_USER",
                        "parts": [{"text": "Stream hello from TCK"}],
                        "messageId": "tck-stream-001-regression-jsonrpc"
                    }
                }
            }),
        )
        .await;
        let (stream_headers, stream_body) = streaming.split_once("\r\n\r\n").unwrap();
        assert!(stream_headers.contains("Content-Type: text/event-stream"));
        let events: Vec<Value> = stream_body
            .lines()
            .filter_map(|line| line.strip_prefix("data: "))
            .map(|event| serde_json::from_str(event).expect("SSE event was not JSON"))
            .collect();
        assert_eq!(events.len(), 2, "{streaming}");
        assert_eq!(events[0]["jsonrpc"], "2.0");
        assert_eq!(events[0]["id"], 2);
        assert_eq!(
            events[0]["result"]["task"]["status"]["state"],
            "TASK_STATE_WORKING"
        );
        assert_eq!(events[1]["jsonrpc"], "2.0");
        assert_eq!(events[1]["id"], 2);
        assert_eq!(
            events[1]["result"]["statusUpdate"]["status"]["state"],
            "TASK_STATE_COMPLETED"
        );

        handle.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn settled_retries_reuse_output_and_open_replacements_keep_their_dispatch_fence() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let cancellation = CancellationToken::new();
        let handle = cancellation.handle();
        let server = server(&format!("http://{address}/")).unwrap();
        let running_server = server.clone();
        let task = tokio::spawn(server.serve(listener, cancellation));

        let completed_message = json!({
            "role": "ROLE_USER",
            "parts": [{"text": "Blocking request"}],
            "messageId": "tck-complete-task-full-then-isolated-jsonrpc"
        });
        let completed = response_body(
            &post_jsonrpc(
                address,
                "application/json",
                json!({
                    "jsonrpc": "2.0",
                    "id": 10,
                    "method": "SendMessage",
                    "params": {"message": completed_message.clone()}
                }),
            )
            .await,
        );
        let completed_retry = response_body(
            &post_jsonrpc(
                address,
                "application/json",
                json!({
                    "jsonrpc": "2.0",
                    "id": 11,
                    "method": "SendMessage",
                    "params": {
                        "message": completed_message,
                        "configuration": {"returnImmediately": false}
                    }
                }),
            )
            .await,
        );
        assert!(completed.get("error").is_none(), "{completed}");
        assert!(completed_retry.get("error").is_none(), "{completed_retry}");
        assert_eq!(completed["result"], completed_retry["result"]);

        let direct_message = json!({
            "role": "ROLE_USER",
            "parts": [{"text": "Direct response"}],
            "messageId": "tck-message-response-full-then-isolated-jsonrpc"
        });
        let direct = response_body(
            &post_jsonrpc(
                address,
                "application/json",
                json!({
                    "jsonrpc": "2.0",
                    "id": 12,
                    "method": "SendMessage",
                    "params": {"message": direct_message.clone()}
                }),
            )
            .await,
        );
        let direct_retry = response_body(
            &post_jsonrpc(
                address,
                "application/json",
                json!({
                    "jsonrpc": "2.0",
                    "id": 13,
                    "method": "SendMessage",
                    "params": {
                        "message": direct_message,
                        "configuration": {"returnImmediately": false}
                    }
                }),
            )
            .await,
        );
        assert!(direct.get("error").is_none(), "{direct}");
        assert!(direct_retry.get("error").is_none(), "{direct_retry}");
        assert_eq!(direct["result"], direct_retry["result"]);
        assert_eq!(
            direct["result"]["message"]["messageId"],
            "tck-message-response-full-then-isolated-jsonrpc-response"
        );

        let interrupted = response_body(
            &post_jsonrpc(
                address,
                "application/json",
                json!({
                    "jsonrpc": "2.0",
                    "id": 14,
                    "method": "SendMessage",
                    "params": {"message": {
                        "role": "ROLE_USER",
                        "parts": [{"text": "Need input"}],
                        "messageId": "tck-input-required-open-replacement-jsonrpc"
                    }}
                }),
            )
            .await,
        );
        assert!(interrupted.get("error").is_none(), "{interrupted}");
        assert_eq!(
            interrupted["result"]["task"]["status"]["state"],
            "TASK_STATE_INPUT_REQUIRED"
        );
        let task_id = interrupted["result"]["task"]["id"]
            .as_str()
            .unwrap()
            .to_owned();
        let context_id = interrupted["result"]["task"]["contextId"]
            .as_str()
            .unwrap()
            .to_owned();
        let replacement_message_id = "tck-complete-task-open-replacement-jsonrpc";
        let replacement = response_body(
            &post_jsonrpc(
                address,
                "application/json",
                json!({
                    "jsonrpc": "2.0",
                    "id": 15,
                    "method": "SendMessage",
                    "params": {"message": {
                        "role": "ROLE_USER",
                        "parts": [{"text": "Replacement input"}],
                        "messageId": replacement_message_id,
                        "taskId": task_id,
                        "contextId": context_id
                    }}
                }),
            )
            .await,
        );
        assert!(replacement.get("error").is_none(), "{replacement}");
        assert_eq!(
            replacement["result"]["task"]["status"]["state"],
            "TASK_STATE_COMPLETED"
        );
        assert_eq!(replacement["result"]["task"]["id"], task_id);
        let snapshot = running_server.mapper_snapshot().await;
        let replacement_dispatch = snapshot
            .dispatch_outbox()
            .values()
            .find(|dispatch| dispatch.message_id == replacement_message_id)
            .expect("open replacement retained its durable dispatch");
        assert_eq!(replacement_dispatch.state, A2aDispatchOutboxState::Settled);
        assert!(replacement_dispatch.attempts > 0);

        handle.cancel();
        task.await.unwrap().unwrap();
    }
}
