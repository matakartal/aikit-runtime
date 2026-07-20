//! Provider-neutral protocol conformance and runtime contracts.
//!
//! These modules contain no sockets, HTTP servers, JSON-RPC loops, provider calls, or tool
//! execution. They map inbound MCP, A2A, and ACP operations into governed runtime intents with a
//! stable correlation identity. Transports and hosts remain responsible for wire framing and for
//! executing an authorized intent through the normal aikit governance/runtime boundary.

pub mod a2a;
pub mod acp;
pub mod common;
pub mod mcp;

pub use a2a::*;
pub use acp::*;
pub use common::*;
pub use mcp::*;

#[cfg(test)]
mod tests;
