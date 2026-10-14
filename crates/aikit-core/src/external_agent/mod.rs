//! Governed external coding-agent runtimes.
//!
//! External agents are deliberately separate from [`crate::providers::Provider`]. A provider
//! streams model output into AIKit's tool loop; an external agent owns a session, its own tool
//! loop, and a bidirectional approval protocol. Keeping those contracts separate prevents tool
//! calls and side effects from being retried through the wrong runtime.

mod acp;
mod codex;
mod discovery;
mod host;
mod jsonrpc;
mod types;
mod update;

pub use acp::{AcpExternalRuntime, AcpRuntimeConfig};
pub use codex::{CodexAppServerConfig, CodexAppServerRuntime};
pub use discovery::{
    discover_runtime_candidates, discover_support_executable, runtime_install_origin,
};
pub use host::{
    ensure_vendor_terms_acknowledged, spawn_external_runtime_for_host, ExternalRuntimeSpawnOptions,
};
pub use jsonrpc::{
    AgentProcessLaunch, JsonRpcConnection, JsonRpcDialect, JsonRpcProcess, RpcError, RpcInbound,
    MAX_AGENT_RPC_FRAME_BYTES, MAX_AGENT_RPC_OUTBOUND_QUEUE, MAX_AGENT_RPC_PENDING_REQUESTS,
    MAX_AGENT_STDERR_TAIL_BYTES,
};
pub use types::*;
pub use update::*;
