//! Credentials-free, deterministic localhost SUT for official MCP SDK conformance.
//!
//! This is a test fixture, not a production server. It serves the implemented MCP 2025-11-25
//! Streamable HTTP transport with one echo tool, one static resource, and one prompt, so the
//! official `@modelcontextprotocol/sdk` client can exercise initialize, tools, resources, and
//! prompts end to end without provider credentials.

use aikit_core::{
    CancellationToken, InMemoryMcpHttpSessionStore, InMemoryMcpServerStateStore, McpDispatchHost,
    McpJsonRpcDispatch, McpJsonRpcServer, McpPromptArgument, McpPromptDefinition,
    McpProtectedResourceMetadata, McpResourceDefinition, McpServerAction, McpServerInfo,
    McpServerRegistry, McpStreamableHttpConfig, McpStreamableHttpServer, McpToolDefinition,
    ProtocolError, ProtocolResult, StaticBearerMcpAuthenticator,
};
use async_trait::async_trait;
use serde_json::json;
use std::sync::Arc;
use tokio::net::TcpListener;

const SUT_ADDRESS: &str = "127.0.0.1:9998";
const SUT_TOKEN: &str = "mcp-sdk-conformance";

#[derive(Debug)]
struct DeterministicHost;

#[async_trait]
impl McpDispatchHost for DeterministicHost {
    async fn handle(
        &self,
        server: &McpJsonRpcServer,
        dispatch: &McpJsonRpcDispatch,
    ) -> ProtocolResult<Vec<aikit_core::McpOutboundMessage>> {
        let action = dispatch
            .governed_action
            .as_ref()
            .and_then(|decision| decision.action());
        match action {
            Some(McpServerAction::InvokeTool { task }) => {
                let aikit_core::McpTaskOperation::ToolCall { arguments, .. } = &task.operation;
                let text = arguments
                    .get("text")
                    .and_then(|value| value.as_str())
                    .unwrap_or_default();
                server.complete_tool(
                    &task.task_id,
                    json!({
                        "content": [{"type": "text", "text": format!("echo: {text}")}],
                        "isError": false
                    }),
                )
            }
            Some(McpServerAction::ReadResource { resource }) => {
                let pending = dispatch.pending.as_ref().ok_or_else(|| {
                    ProtocolError::invalid("resource dispatch omitted pending handle")
                })?;
                server
                    .complete_pending(
                        pending,
                        Ok(json!({
                            "contents": [{
                                "uri": resource.uri,
                                "mimeType": "text/plain",
                                "text": "deterministic greeting body"
                            }]
                        })),
                    )
                    .map(|message| vec![message])
            }
            Some(McpServerAction::RenderPrompt { arguments, .. }) => {
                let pending = dispatch.pending.as_ref().ok_or_else(|| {
                    ProtocolError::invalid("prompt dispatch omitted pending handle")
                })?;
                let name = arguments.get("name").map(String::as_str).unwrap_or("world");
                server
                    .complete_pending(
                        pending,
                        Ok(json!({
                            "description": "deterministic greeting prompt",
                            "messages": [{
                                "role": "user",
                                "content": {"type": "text", "text": format!("Greet {name} politely.")}
                            }]
                        })),
                    )
                    .map(|message| vec![message])
            }
            _ => Ok(Vec::new()),
        }
    }
}

fn registry() -> ProtocolResult<McpServerRegistry> {
    let mut registry = McpServerRegistry::new();
    registry.register_tool(McpToolDefinition::new(
        "echo",
        "Echo the provided text back, prefixed deterministically.",
        json!({
            "type": "object",
            "properties": {"text": {"type": "string"}},
            "required": ["text"],
            "additionalProperties": false
        }),
    )?)?;
    registry.register_resource(McpResourceDefinition {
        uri: "demo://greeting".into(),
        name: "greeting".into(),
        description: Some("A deterministic text resource.".into()),
        mime_type: Some("text/plain".into()),
        required_scopes: Default::default(),
    })?;
    registry.register_prompt(McpPromptDefinition {
        name: "greet".into(),
        description: Some("Render a deterministic greeting request.".into()),
        arguments: vec![McpPromptArgument {
            name: "name".into(),
            description: Some("Who to greet.".into()),
            required: true,
        }],
        required_scopes: Default::default(),
    })?;
    Ok(registry)
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> ProtocolResult<()> {
    let principal = aikit_core::ProtocolPrincipal::new(
        "mcp-sdk-client",
        [
            "mcp:tools:list",
            "mcp:tools:call",
            "mcp:resources:list",
            "mcp:resources:read",
            "mcp:prompts:list",
            "mcp:prompts:get",
            "mcp:tasks:get",
            "mcp:tasks:cancel",
        ],
    )?;
    let server = Arc::new(McpJsonRpcServer::new(
        "mcp-sdk-sut",
        McpServerInfo::new("aikit-mcp-sdk-sut", env!("CARGO_PKG_VERSION"))?,
        Arc::new(InMemoryMcpServerStateStore::default()),
        registry()?,
    )?);
    let transport = Arc::new(McpStreamableHttpServer::new(
        server,
        Arc::new(InMemoryMcpHttpSessionStore::default()),
        Arc::new(StaticBearerMcpAuthenticator::new(
            SUT_TOKEN,
            principal,
            "Bearer realm=\"mcp-sdk-sut\"",
        )?),
        Arc::new(DeterministicHost),
        McpStreamableHttpConfig {
            oauth_protected_resource: Some(McpProtectedResourceMetadata {
                resource: format!("http://{SUT_ADDRESS}/mcp"),
                authorization_servers: vec!["https://auth.example.test".into()],
                scopes_supported: vec!["mcp:tools:call".into()],
                bearer_methods_supported: vec!["header".into()],
                resource_documentation: None,
            }),
            ..McpStreamableHttpConfig::default()
        },
    )?);
    let listener = TcpListener::bind(SUT_ADDRESS).await.map_err(|error| {
        ProtocolError::invalid(format!("bind MCP SDK SUT address {SUT_ADDRESS}: {error}"))
    })?;
    eprintln!("mcp-sdk-sut listening on http://{SUT_ADDRESS}/mcp");
    let cancellation = CancellationToken::new();
    transport.serve(listener, cancellation).await
}
