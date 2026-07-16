//! Tool execution boundary.
//!
//! `ToolExecutor` is THE FFI seam: the in-process agent loop, running on Rust's tokio
//! runtime, calls `execute` to run a user-defined `@tool`. The native Rust API implements
//! it with a closure; the PyO3 / napi bindings implement it by awaiting a host-language
//! coroutine across the FFI boundary. Everything above this trait is provider-agnostic and
//! language-agnostic.
//!
//! The [`builtin`] module ships a batteries-included `ToolExecutor` (Read/Write/Edit/Grep/Glob/
//! Bash) guarded by the filesystem [`sandbox`](crate::governance::sandbox) — the tool suite no
//! other multi-provider SDK bundles.

pub mod builtin;

use crate::error::{AikitError, Result};
use async_trait::async_trait;
use serde_json::Value;

/// Ergonomic canonical tool-schema constructor. Execution is attached separately through a
/// [`ToolExecutor`], keeping schema registration and side effects explicit.
pub fn tool(
    name: impl Into<String>,
    description: impl Into<String>,
    input_schema: Value,
) -> crate::types::ToolSpec {
    crate::types::ToolSpec::new(name, description, input_schema)
}

#[async_trait]
pub trait ToolExecutor: Send + Sync {
    /// Run the tool named `name` with the given JSON `input`, returning its result as a
    /// string (tool results are stringly-typed on the wire for every provider).
    async fn execute(&self, name: &str, input: Value) -> Result<String>;
}

/// Executor used when no tools are registered — any call is an error.
pub struct NoTools;

#[async_trait]
impl ToolExecutor for NoTools {
    async fn execute(&self, name: &str, _input: Value) -> Result<String> {
        Err(AikitError::ToolExecution(format!(
            "no tool executor registered for '{name}'"
        )))
    }
}
