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
pub mod web;

use crate::error::{AikitError, Result};
use async_trait::async_trait;
use serde_json::Value;
use std::collections::BTreeSet;
use std::sync::{Arc, RwLock};

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

struct ToolRoute {
    names: BTreeSet<String>,
    executor: Arc<dyn ToolExecutor>,
}

/// Thread-safe registry that composes built-in, MCP, web, browser, and host-owned executors
/// without allowing ambiguous tool-name shadowing.
#[derive(Default)]
pub struct ToolRouter {
    routes: RwLock<Vec<ToolRoute>>,
}

impl ToolRouter {
    pub fn register(
        &self,
        specs: &[crate::types::ToolSpec],
        executor: Arc<dyn ToolExecutor>,
    ) -> Result<()> {
        let names: BTreeSet<_> = specs.iter().map(|spec| spec.name.clone()).collect();
        if names.len() != specs.len() {
            return Err(AikitError::Configuration(
                "tool registration contains duplicate names".into(),
            ));
        }
        let mut routes = self
            .routes
            .write()
            .map_err(|_| AikitError::ToolExecution("tool router poisoned".into()))?;
        if let Some(collision) = routes
            .iter()
            .flat_map(|route| route.names.iter())
            .find(|name| names.contains(*name))
        {
            return Err(AikitError::Configuration(format!(
                "tool '{collision}' is already routed"
            )));
        }
        routes.push(ToolRoute { names, executor });
        Ok(())
    }

    pub fn contains(&self, name: &str) -> bool {
        self.routes
            .read()
            .map(|routes| routes.iter().any(|route| route.names.contains(name)))
            .unwrap_or(false)
    }
}

#[async_trait]
impl ToolExecutor for ToolRouter {
    async fn execute(&self, name: &str, input: Value) -> Result<String> {
        let executor = self
            .routes
            .read()
            .map_err(|_| AikitError::ToolExecution("tool router poisoned".into()))?
            .iter()
            .find(|route| route.names.contains(name))
            .map(|route| route.executor.clone())
            .ok_or_else(|| AikitError::ToolExecution(format!("unknown routed tool '{name}'")))?;
        executor.execute(name, input).await
    }
}
