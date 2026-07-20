//! Compatibility bridge from the legacy delta stream to the versioned block lifecycle.

use crate::contract::{StreamBlockKind, StreamEvent, StreamEventKind};
use crate::error::{ErrorCode, ErrorInfo};
use crate::types::StreamDelta;
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
use thiserror::Error;

pub type StreamEncodingResult<T> = Result<T, StreamEncodingError>;

/// Lifecycle violations rejected before any invalid canonical stream event is emitted.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum StreamEncodingError {
    #[error("stream is already terminal")]
    StreamTerminated,
    #[error("stream event was received before response start")]
    ResponseNotStarted,
    #[error("response was started more than once")]
    DuplicateResponseStart,
    #[error("block delta was received before block start: {block_id}")]
    DeltaBeforeBlockStart { block_id: String },
    #[error("block was started more than once: {block_id}")]
    DuplicateBlockStart { block_id: String },
}

/// Stateful encoder because one legacy delta can open a block and emit its first delta.
#[derive(Debug, Clone)]
pub struct StreamEventEncoder {
    response_id: String,
    next_sequence: u64,
    next_block: u64,
    active_text: Option<String>,
    active_reasoning: Option<String>,
    active_tools: BTreeMap<String, String>,
    started_blocks: BTreeSet<String>,
    response_started: bool,
    terminated: bool,
}

impl StreamEventEncoder {
    pub fn new(response_id: impl Into<String>) -> Self {
        Self {
            response_id: response_id.into(),
            next_sequence: 1,
            next_block: 1,
            active_text: None,
            active_reasoning: None,
            active_tools: BTreeMap::new(),
            started_blocks: BTreeSet::new(),
            response_started: false,
            terminated: false,
        }
    }

    fn emit(&mut self, kind: StreamEventKind) -> StreamEvent {
        let sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.saturating_add(1);
        StreamEvent {
            event_id: format!("{}-evt-{sequence}", self.response_id),
            sequence,
            kind,
        }
    }

    fn block_id(&mut self, prefix: &str) -> String {
        loop {
            let id = format!("{}-{prefix}-{}", self.response_id, self.next_block);
            self.next_block = self.next_block.saturating_add(1);
            if !self.started_blocks.contains(&id) {
                return id;
            }
        }
    }

    fn start_block(
        &mut self,
        block_id: String,
        block_kind: StreamBlockKind,
        name: Option<String>,
    ) -> StreamEncodingResult<StreamEvent> {
        if !self.started_blocks.insert(block_id.clone()) {
            return Err(StreamEncodingError::DuplicateBlockStart { block_id });
        }
        Ok(self.emit(StreamEventKind::BlockStart {
            block_id,
            block_kind,
            name,
        }))
    }

    fn ensure_text(&mut self, events: &mut Vec<StreamEvent>) -> StreamEncodingResult<String> {
        if let Some(id) = &self.active_text {
            return Ok(id.clone());
        }
        let id = self.block_id("text");
        events.push(self.start_block(id.clone(), StreamBlockKind::Text, None)?);
        self.active_text = Some(id.clone());
        Ok(id)
    }

    fn ensure_reasoning(&mut self, events: &mut Vec<StreamEvent>) -> StreamEncodingResult<String> {
        if let Some(id) = &self.active_reasoning {
            return Ok(id.clone());
        }
        let id = self.block_id("reasoning");
        events.push(self.start_block(id.clone(), StreamBlockKind::Reasoning, None)?);
        self.active_reasoning = Some(id.clone());
        Ok(id)
    }

    fn close_open_blocks(&mut self, events: &mut Vec<StreamEvent>) {
        if let Some(id) = self.active_text.take() {
            events.push(self.emit(StreamEventKind::BlockEnd {
                block_id: id,
                value: None,
            }));
        }
        if let Some(id) = self.active_reasoning.take() {
            events.push(self.emit(StreamEventKind::BlockEnd {
                block_id: id,
                value: None,
            }));
        }
        let tools = std::mem::take(&mut self.active_tools);
        for (_, block_id) in tools {
            events.push(self.emit(StreamEventKind::BlockEnd {
                block_id,
                value: None,
            }));
        }
    }

    /// Convert one legacy delta while enforcing the canonical block and terminal lifecycle.
    /// Successful events are never reordered and sequence numbers remain monotonic.
    pub fn try_push(&mut self, delta: StreamDelta) -> StreamEncodingResult<Vec<StreamEvent>> {
        if self.terminated {
            return Err(StreamEncodingError::StreamTerminated);
        }
        match &delta {
            StreamDelta::MessageStart { .. } if self.response_started => {
                return Err(StreamEncodingError::DuplicateResponseStart);
            }
            StreamDelta::MessageStart { .. }
            | StreamDelta::Warning { .. }
            | StreamDelta::Error { .. } => {}
            _ if !self.response_started => {
                return Err(StreamEncodingError::ResponseNotStarted);
            }
            _ => {}
        }

        let mut events = Vec::new();
        match delta {
            StreamDelta::MessageStart { model } => {
                self.response_started = true;
                let response_id = self.response_id.clone();
                events.push(self.emit(StreamEventKind::ResponseStart { response_id, model }));
            }
            StreamDelta::TextDelta { text } => {
                let block_id = self.ensure_text(&mut events)?;
                events.push(self.emit(StreamEventKind::BlockDelta {
                    block_id,
                    delta: json!({"text": text}),
                }));
            }
            StreamDelta::ReasoningDelta { text } => {
                let block_id = self.ensure_reasoning(&mut events)?;
                events.push(self.emit(StreamEventKind::BlockDelta {
                    block_id,
                    delta: json!({"text": text}),
                }));
            }
            StreamDelta::ReasoningComplete {
                text,
                signature,
                opaque,
            } => {
                let block_id = self.ensure_reasoning(&mut events)?;
                self.active_reasoning = None;
                events.push(self.emit(StreamEventKind::BlockEnd {
                    block_id,
                    value: Some(json!({
                        "text": text,
                        "signature": signature,
                        "opaque": opaque,
                    })),
                }));
            }
            StreamDelta::ToolCallStart { id, name } => {
                let block_id = format!("{}-tool-{id}", self.response_id);
                events.push(self.start_block(
                    block_id.clone(),
                    StreamBlockKind::ToolCall,
                    Some(name),
                )?);
                self.active_tools.insert(id, block_id);
            }
            StreamDelta::ToolCallInput { id, input } => {
                let block_id = self.active_tools.get(&id).cloned().ok_or_else(|| {
                    StreamEncodingError::DeltaBeforeBlockStart {
                        block_id: format!("{}-tool-{id}", self.response_id),
                    }
                })?;
                events.push(self.emit(StreamEventKind::BlockDelta {
                    block_id,
                    delta: json!({"input": input}),
                }));
            }
            StreamDelta::ToolResult {
                tool_use_id,
                content,
                is_error,
            } => {
                let block_id = self.block_id("tool-result");
                events.push(self.start_block(
                    block_id.clone(),
                    StreamBlockKind::ToolResult,
                    None,
                )?);
                events.push(self.emit(StreamEventKind::BlockEnd {
                    block_id,
                    value: Some(json!({
                        "tool_use_id": tool_use_id,
                        "content": content,
                        "is_error": is_error,
                    })),
                }));
            }
            StreamDelta::Citation {
                text,
                source,
                metadata,
            } => {
                let block_id = self.block_id("citation");
                events.push(self.start_block(block_id.clone(), StreamBlockKind::Citation, None)?);
                events.push(self.emit(StreamEventKind::BlockEnd {
                    block_id,
                    value: Some(json!({"text": text, "source": source, "metadata": metadata})),
                }));
            }
            StreamDelta::ProviderMetadata { provider, metadata } => {
                events.push(self.emit(StreamEventKind::ProviderMetadata { provider, metadata }));
            }
            StreamDelta::Warning { warning } => {
                events.push(self.emit(StreamEventKind::Warning { warning }));
            }
            StreamDelta::Usage(usage) => {
                events.push(self.emit(StreamEventKind::Usage { usage }));
            }
            StreamDelta::MessageStop { stop_reason } => {
                self.close_open_blocks(&mut events);
                let response_id = self.response_id.clone();
                events.push(self.emit(StreamEventKind::ResponseEnd {
                    response_id,
                    stop_reason,
                }));
                self.terminated = true;
            }
            StreamDelta::Error { message, info } => {
                self.close_open_blocks(&mut events);
                events.push(self.emit(StreamEventKind::Error { message, info }));
                self.terminated = true;
            }
        }
        Ok(events)
    }

    /// Compatibility adapter for callers that consume event vectors rather than typed results.
    /// Invalid input becomes one terminal error event; input after a terminal event is ignored.
    pub fn push(&mut self, delta: StreamDelta) -> Vec<StreamEvent> {
        match self.try_push(delta) {
            Ok(events) => events,
            Err(StreamEncodingError::StreamTerminated) => Vec::new(),
            Err(error) => {
                self.terminated = true;
                vec![self.emit(StreamEventKind::Error {
                    message: error.to_string(),
                    info: ErrorInfo::new(ErrorCode::Conflict),
                })]
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_has_start_delta_end_lifecycle_and_monotonic_sequence() {
        let mut encoder = StreamEventEncoder::new("response-1");
        let mut events = encoder.push(StreamDelta::MessageStart {
            model: "mock-1".into(),
        });
        events.extend(encoder.push(StreamDelta::TextDelta {
            text: "hello".into(),
        }));
        events.extend(encoder.push(StreamDelta::MessageStop {
            stop_reason: "stop".into(),
        }));

        assert_eq!(events.len(), 5);
        assert!(matches!(
            events[0].kind,
            StreamEventKind::ResponseStart { .. }
        ));
        assert!(matches!(events[1].kind, StreamEventKind::BlockStart { .. }));
        assert!(matches!(events[2].kind, StreamEventKind::BlockDelta { .. }));
        assert!(matches!(events[3].kind, StreamEventKind::BlockEnd { .. }));
        assert!(matches!(
            events[4].kind,
            StreamEventKind::ResponseEnd { .. }
        ));
        assert_eq!(
            events
                .iter()
                .map(|event| event.sequence)
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 5]
        );
    }

    #[test]
    fn response_lifecycle_rejects_delta_before_start_and_duplicate_start() {
        let mut encoder = StreamEventEncoder::new("response-envelope");
        assert_eq!(
            encoder
                .try_push(StreamDelta::TextDelta {
                    text: "orphan".into(),
                })
                .unwrap_err(),
            StreamEncodingError::ResponseNotStarted
        );
        encoder
            .try_push(StreamDelta::MessageStart {
                model: "mock-1".into(),
            })
            .unwrap();
        assert_eq!(
            encoder
                .try_push(StreamDelta::MessageStart {
                    model: "mock-2".into(),
                })
                .unwrap_err(),
            StreamEncodingError::DuplicateResponseStart
        );
    }

    #[test]
    fn compatibility_warning_can_precede_provider_response_start() {
        let mut encoder = StreamEventEncoder::new("response-warning");
        let warning = crate::contract::ProviderWarning {
            code: "unverified_provider_parameter".into(),
            message: "forwarded without semantic adaptation".into(),
            parameter: Some("future_option".into()),
            provider: Some("mock".into()),
            model: Some("mock-1".into()),
        };
        let events = encoder
            .try_push(StreamDelta::Warning {
                warning: warning.clone(),
            })
            .unwrap();
        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0].kind,
            StreamEventKind::Warning { warning: actual } if actual == &warning
        ));

        let start = encoder
            .try_push(StreamDelta::MessageStart {
                model: "mock-1".into(),
            })
            .unwrap();
        assert_eq!(start[0].sequence, 2);
    }

    #[test]
    fn parallel_tool_ids_remain_distinct() {
        let mut encoder = StreamEventEncoder::new("response-2");
        encoder.push(StreamDelta::MessageStart {
            model: "mock-1".into(),
        });
        let a = encoder.push(StreamDelta::ToolCallStart {
            id: "a".into(),
            name: "first".into(),
        });
        let b = encoder.push(StreamDelta::ToolCallStart {
            id: "b".into(),
            name: "second".into(),
        });
        let a_delta = encoder.push(StreamDelta::ToolCallInput {
            id: "a".into(),
            input: json!({"x": 1}),
        });

        let id = |event: &StreamEvent| match &event.kind {
            StreamEventKind::BlockStart { block_id, .. }
            | StreamEventKind::BlockDelta { block_id, .. } => block_id.clone(),
            _ => String::new(),
        };
        assert_ne!(id(&a[0]), id(&b[0]));
        assert_eq!(id(&a[0]), id(&a_delta[0]));
    }

    #[test]
    fn tool_delta_before_start_returns_typed_error_without_emitting_an_event() {
        let mut encoder = StreamEventEncoder::new("response-missing-start");
        let start = encoder
            .try_push(StreamDelta::MessageStart {
                model: "mock-1".into(),
            })
            .unwrap();
        assert_eq!(start[0].sequence, 1);

        let error = encoder
            .try_push(StreamDelta::ToolCallInput {
                id: "missing".into(),
                input: json!({"x": 1}),
            })
            .unwrap_err();
        assert!(matches!(
            error,
            StreamEncodingError::DeltaBeforeBlockStart { block_id }
                if block_id == "response-missing-start-tool-missing"
        ));

        let events = encoder
            .try_push(StreamDelta::Usage(Default::default()))
            .unwrap();
        assert_eq!(events[0].sequence, 2);
    }

    #[test]
    fn duplicate_tool_start_is_rejected_without_overwriting_the_active_block() {
        let mut encoder = StreamEventEncoder::new("response-duplicate");
        encoder
            .try_push(StreamDelta::MessageStart {
                model: "mock-1".into(),
            })
            .unwrap();
        let first = encoder
            .try_push(StreamDelta::ToolCallStart {
                id: "same".into(),
                name: "first".into(),
            })
            .unwrap();
        let error = encoder
            .try_push(StreamDelta::ToolCallStart {
                id: "same".into(),
                name: "second".into(),
            })
            .unwrap_err();
        assert!(matches!(
            error,
            StreamEncodingError::DuplicateBlockStart { block_id }
                if block_id == "response-duplicate-tool-same"
        ));

        let delta = encoder
            .try_push(StreamDelta::ToolCallInput {
                id: "same".into(),
                input: json!({"ok": true}),
            })
            .unwrap();
        let block_id = |event: &StreamEvent| match &event.kind {
            StreamEventKind::BlockStart { block_id, .. }
            | StreamEventKind::BlockDelta { block_id, .. } => block_id.clone(),
            _ => panic!("expected block event"),
        };
        assert_eq!(block_id(&first[0]), block_id(&delta[0]));
        assert_eq!(delta[0].sequence, 3);
    }

    #[test]
    fn generated_blocks_cannot_collide_with_provider_tool_ids() {
        let mut encoder = StreamEventEncoder::new("response-collision");
        encoder
            .try_push(StreamDelta::MessageStart {
                model: "mock-1".into(),
            })
            .unwrap();
        let tool = encoder
            .try_push(StreamDelta::ToolCallStart {
                id: "result-1".into(),
                name: "first".into(),
            })
            .unwrap();
        let result = encoder
            .try_push(StreamDelta::ToolResult {
                tool_use_id: "result-1".into(),
                content: "done".into(),
                is_error: false,
            })
            .unwrap();

        let started_id = |event: &StreamEvent| match &event.kind {
            StreamEventKind::BlockStart { block_id, .. } => block_id.clone(),
            _ => panic!("expected block start"),
        };
        assert_ne!(started_id(&tool[0]), started_id(&result[0]));
    }

    #[test]
    fn stop_and_error_are_terminal_for_typed_and_legacy_adapters() {
        for terminal in [
            StreamDelta::MessageStop {
                stop_reason: "stop".into(),
            },
            StreamDelta::Error {
                message: "failed".into(),
                info: ErrorInfo::new(ErrorCode::ProviderProtocol),
            },
        ] {
            let mut encoder = StreamEventEncoder::new("response-terminal");
            encoder
                .try_push(StreamDelta::MessageStart {
                    model: "mock-1".into(),
                })
                .unwrap();
            encoder.try_push(terminal).unwrap();
            assert_eq!(
                encoder
                    .try_push(StreamDelta::Usage(Default::default()))
                    .unwrap_err(),
                StreamEncodingError::StreamTerminated
            );
            assert!(encoder
                .push(StreamDelta::TextDelta {
                    text: "late".into(),
                })
                .is_empty());
        }
    }

    #[test]
    fn legacy_adapter_turns_a_lifecycle_violation_into_one_terminal_error() {
        let mut encoder = StreamEventEncoder::new("response-legacy-error");
        let events = encoder.push(StreamDelta::ToolCallInput {
            id: "missing".into(),
            input: json!({}),
        });
        assert!(matches!(events[0].kind, StreamEventKind::Error { .. }));
        assert!(encoder
            .push(StreamDelta::MessageStart {
                model: "mock-1".into(),
            })
            .is_empty());
    }
}
