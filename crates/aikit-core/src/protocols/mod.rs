//! Provider-neutral protocol conformance and runtime contracts.
//!
//! These modules map inbound MCP, A2A, and ACP operations into governed runtime intents with a
//! stable correlation identity. MCP includes a byte-level JSON-RPC dispatcher, durable state seam,
//! and concrete stdio/Streamable HTTP listeners. Hosts still execute authorized runtime actions;
//! no protocol adapter executes a provider or tool directly.

pub mod a2a;
pub mod acp;
pub mod common;
pub mod mcp;
pub mod mcp_io;
pub mod mcp_sqlite;
pub mod mcp_transport;

pub use a2a::*;
pub use acp::*;
pub use common::*;
pub use mcp::*;
pub use mcp_io::*;
pub use mcp_sqlite::*;
pub use mcp_transport::*;

#[cfg(test)]
mod tests;
