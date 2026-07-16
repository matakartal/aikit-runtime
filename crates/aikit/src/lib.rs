//! The ergonomic Rust surface for `aikit`.
//!
//! All behavior lives in `aikit-runtime-core`; this crate is the stable package applications depend on.
//! Python and TypeScript bindings call the same core, so provider translation, governance,
//! routing, audit, and structured output are implemented once.

pub use aikit_core::*;

/// Common imports for a small agent application.
pub mod prelude {
    pub use aikit_core::{
        tool, Agent, AgentOptions, CancellableRun, CancellationHandle, CancellationToken, Client,
        ContentBlock, GeneratedObject, GeneratedText, JsonSchema, MediaSource, Message,
        ObjectOptions, ObjectStream, ObjectStreamEvent, RoutingOptions, StreamDelta, ToolExecutor,
        ToolSpec, TypedGeneratedObject,
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    #[tokio::test]
    async fn native_surface_runs_the_same_keyless_agent() {
        let generated = Agent::new()
            .generate_text("hello", "mock-1", 64)
            .await
            .unwrap();
        assert!(generated.text.contains("görevi tamamladım"));
    }

    #[tokio::test]
    async fn native_surface_exposes_incremental_structured_events() {
        let mut stream = Agent::new()
            .stream_object(
                "Return a status",
                serde_json::json!({
                    "type": "object",
                    "required": ["status"],
                    "properties": { "status": { "const": "ok" } }
                }),
                "mock-structured",
                ObjectOptions::default(),
            )
            .unwrap();
        let mut saw_delta = false;
        let mut completed = false;
        while let Some(event) = stream.next().await {
            match event.unwrap() {
                ObjectStreamEvent::Delta { .. } => saw_delta = true,
                ObjectStreamEvent::Completed { object } => {
                    assert_eq!(object.value["status"], "ok");
                    completed = true;
                }
                _ => {}
            }
        }
        assert!(saw_delta && completed);
    }

    #[test]
    fn native_surface_exposes_tool_and_subtask_ergonomics() {
        let spec = tool(
            "lookup",
            "Lookup one record",
            serde_json::json!({ "type": "object" }),
        );
        assert_eq!(spec.name, "lookup");

        let child = Agent::new().subtask(
            "child-1",
            "inspect",
            ModelRouteRequirements::explicit("mock-1"),
        );
        assert_eq!(child.id, "child-1");
        assert_eq!(child.prompt, "inspect");
    }
}
