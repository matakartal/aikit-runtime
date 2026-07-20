//! Deterministic trace assertions for stream, durability, approval, and recovery behavior.

use crate::contract::{StreamEvent, StreamEventKind};
use crate::durability::{DurableRunStatus, RunEvent, RunEventKind};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum TraceAssertion {
    StreamSequenceMonotonic,
    StreamBlocksBalanced,
    DurableSequenceMonotonic,
    NoDuplicateActivityCompletion,
    AllRequiredReconciliationsResolved,
    ApprovalResolved { approval_id: String, approved: bool },
    RunStatus { status: DurableRunStatus },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvalSuite {
    pub schema_version: u32,
    pub name: String,
    pub assertions: Vec<TraceAssertion>,
}

impl EvalSuite {
    pub fn new(name: impl Into<String>, assertions: Vec<TraceAssertion>) -> Self {
        Self {
            schema_version: 1,
            name: name.into(),
            assertions,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TraceInput {
    #[serde(default)]
    pub stream_events: Vec<StreamEvent>,
    #[serde(default)]
    pub durable_events: Vec<RunEvent>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_status: Option<DurableRunStatus>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceCheck {
    pub assertion: String,
    pub passed: bool,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EvalResult {
    pub suite: String,
    pub passed: bool,
    pub passed_checks: usize,
    pub total_checks: usize,
    pub checks: Vec<TraceCheck>,
}

pub fn evaluate_trace(suite: &EvalSuite, input: &TraceInput) -> EvalResult {
    let checks = suite
        .assertions
        .iter()
        .map(|assertion| evaluate_assertion(assertion, input))
        .collect::<Vec<_>>();
    let passed_checks = checks.iter().filter(|check| check.passed).count();
    EvalResult {
        suite: suite.name.clone(),
        passed: !checks.is_empty() && passed_checks == checks.len(),
        passed_checks,
        total_checks: checks.len(),
        checks,
    }
}

fn check(name: &str, passed: bool, message: impl Into<String>) -> TraceCheck {
    TraceCheck {
        assertion: name.into(),
        passed,
        message: message.into(),
    }
}

fn evaluate_assertion(assertion: &TraceAssertion, input: &TraceInput) -> TraceCheck {
    match assertion {
        TraceAssertion::StreamSequenceMonotonic => {
            let valid = input
                .stream_events
                .iter()
                .enumerate()
                .all(|(index, event)| event.sequence == index as u64 + 1);
            check(
                "stream_sequence_monotonic",
                valid,
                if valid {
                    "stream sequence is consecutive"
                } else {
                    "stream sequence contains a gap or duplicate"
                },
            )
        }
        TraceAssertion::StreamBlocksBalanced => {
            let mut open = BTreeSet::new();
            let mut valid = true;
            for event in &input.stream_events {
                match &event.kind {
                    StreamEventKind::BlockStart { block_id, .. } => {
                        valid &= open.insert(block_id.clone());
                    }
                    StreamEventKind::BlockDelta { block_id, .. } => {
                        valid &= open.contains(block_id);
                    }
                    StreamEventKind::BlockEnd { block_id, .. } => {
                        valid &= open.remove(block_id);
                    }
                    _ => {}
                }
            }
            valid &= open.is_empty();
            check(
                "stream_blocks_balanced",
                valid,
                if valid {
                    "every stream block has a valid lifecycle"
                } else {
                    "stream contains an orphan, duplicate, or unclosed block"
                },
            )
        }
        TraceAssertion::DurableSequenceMonotonic => {
            let valid = input
                .durable_events
                .iter()
                .enumerate()
                .all(|(index, event)| event.sequence == index as u64 + 1);
            check(
                "durable_sequence_monotonic",
                valid,
                if valid {
                    "durable sequence is consecutive"
                } else {
                    "durable sequence contains a gap or duplicate"
                },
            )
        }
        TraceAssertion::NoDuplicateActivityCompletion => {
            let mut completed = BTreeSet::new();
            let valid = input.durable_events.iter().all(|event| match &event.kind {
                RunEventKind::ActivityAttemptCompleted {
                    activity_id,
                    attempt,
                    ..
                } => completed.insert((activity_id.clone(), *attempt)),
                _ => true,
            });
            check(
                "no_duplicate_activity_completion",
                valid,
                if valid {
                    "activity completions are unique"
                } else {
                    "the same activity attempt completed more than once"
                },
            )
        }
        TraceAssertion::AllRequiredReconciliationsResolved => {
            let mut open = BTreeSet::new();
            let mut resolved = BTreeSet::new();
            let mut lifecycle_valid = true;
            for event in &input.durable_events {
                match &event.kind {
                    RunEventKind::ActivityReconciliationRequired {
                        activity_id,
                        attempt,
                        ..
                    } => {
                        let key = (activity_id.clone(), *attempt);
                        lifecycle_valid &= !resolved.contains(&key) && open.insert(key);
                    }
                    RunEventKind::ActivityReconciled {
                        activity_id,
                        attempt,
                        ..
                    } => {
                        let key = (activity_id.clone(), *attempt);
                        lifecycle_valid &= open.remove(&key) && resolved.insert(key);
                    }
                    _ => {}
                }
            }
            let unresolved = open.len();
            check(
                "all_required_reconciliations_resolved",
                lifecycle_valid && unresolved == 0,
                if lifecycle_valid {
                    format!("{unresolved} reconciliation requirement(s) remain unresolved")
                } else {
                    "reconciliation lifecycle is duplicate or out of order".into()
                },
            )
        }
        TraceAssertion::ApprovalResolved {
            approval_id,
            approved,
        } => {
            let mut requested = false;
            let mut resolution = None;
            let mut lifecycle_valid = true;
            for event in &input.durable_events {
                match &event.kind {
                    RunEventKind::ApprovalRequested {
                        approval_id: event_id,
                        ..
                    } if event_id == approval_id => {
                        lifecycle_valid &= !requested && resolution.is_none();
                        requested = true;
                    }
                    RunEventKind::ApprovalResolved {
                        approval_id: event_id,
                        approved,
                        ..
                    } if event_id == approval_id => {
                        lifecycle_valid &= requested && resolution.is_none();
                        if resolution.is_none() {
                            resolution = Some(*approved);
                        }
                    }
                    _ => {}
                }
            }
            let valid = lifecycle_valid && requested && resolution == Some(*approved);
            check(
                "approval_resolved",
                valid,
                if valid {
                    format!("approval '{approval_id}' has the expected resolution")
                } else if !lifecycle_valid {
                    format!("approval '{approval_id}' has a duplicate or out-of-order lifecycle")
                } else {
                    format!("approval '{approval_id}' is missing or has another resolution")
                },
            )
        }
        TraceAssertion::RunStatus { status } => {
            let valid = input.run_status.as_ref() == Some(status);
            check(
                "run_status",
                valid,
                format!("expected {status:?}, observed {:?}", input.run_status),
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::durability::{ActivityReconciliation, DurabilityMode, RunState};
    use crate::streaming::StreamEventEncoder;
    use crate::types::StreamDelta;

    #[test]
    fn deterministic_suite_accepts_valid_stream_and_run() {
        let mut encoder = StreamEventEncoder::new("response-1");
        let mut stream_events = encoder.push(StreamDelta::MessageStart {
            model: "mock-1".into(),
        });
        stream_events.extend(encoder.push(StreamDelta::TextDelta { text: "ok".into() }));
        stream_events.extend(encoder.push(StreamDelta::MessageStop {
            stop_reason: "stop".into(),
        }));
        let run = RunState::new("session-1", "run-1", DurabilityMode::Sync).unwrap();
        let suite = EvalSuite::new(
            "core",
            vec![
                TraceAssertion::StreamSequenceMonotonic,
                TraceAssertion::StreamBlocksBalanced,
                TraceAssertion::DurableSequenceMonotonic,
                TraceAssertion::NoDuplicateActivityCompletion,
            ],
        );
        let result = evaluate_trace(
            &suite,
            &TraceInput {
                stream_events,
                durable_events: run.events().to_vec(),
                run_status: Some(run.status()),
            },
        );
        assert!(result.passed, "{:?}", result.checks);
    }

    #[test]
    fn orphan_stream_delta_fails_deterministically() {
        let suite = EvalSuite::new("bad", vec![TraceAssertion::StreamBlocksBalanced]);
        let input = TraceInput {
            stream_events: vec![StreamEvent {
                event_id: "event-1".into(),
                sequence: 1,
                kind: StreamEventKind::BlockDelta {
                    block_id: "missing".into(),
                    delta: serde_json::json!({"text": "bad"}),
                },
            }],
            ..TraceInput::default()
        };
        assert!(!evaluate_trace(&suite, &input).passed);
    }

    #[test]
    fn approval_assertion_rejects_contradictory_resolutions() {
        let suite = EvalSuite::new(
            "approval",
            vec![TraceAssertion::ApprovalResolved {
                approval_id: "approval-1".into(),
                approved: true,
            }],
        );
        let input = TraceInput {
            durable_events: vec![
                run_event(
                    1,
                    RunEventKind::ApprovalRequested {
                        approval_id: "approval-1".into(),
                        logical_key: "deploy".into(),
                        activity_id: None,
                        prompt: "approve".into(),
                        payload: serde_json::json!({}),
                    },
                ),
                run_event(
                    2,
                    RunEventKind::ApprovalResolved {
                        approval_id: "approval-1".into(),
                        approved: false,
                        response: None,
                    },
                ),
                run_event(
                    3,
                    RunEventKind::ApprovalResolved {
                        approval_id: "approval-1".into(),
                        approved: true,
                        response: None,
                    },
                ),
            ],
            ..TraceInput::default()
        };
        assert!(!evaluate_trace(&suite, &input).passed);
    }

    #[test]
    fn reconciliation_assertion_rejects_resolution_before_requirement() {
        let suite = EvalSuite::new(
            "reconciliation",
            vec![TraceAssertion::AllRequiredReconciliationsResolved],
        );
        let input = TraceInput {
            durable_events: vec![
                run_event(
                    1,
                    RunEventKind::ActivityReconciled {
                        activity_id: "activity-1".into(),
                        attempt: 1,
                        resolution: ActivityReconciliation::Cancelled,
                    },
                ),
                run_event(
                    2,
                    RunEventKind::ActivityReconciliationRequired {
                        activity_id: "activity-1".into(),
                        attempt: 1,
                        reason: "ambiguous".into(),
                    },
                ),
            ],
            ..TraceInput::default()
        };
        assert!(!evaluate_trace(&suite, &input).passed);
    }

    fn run_event(sequence: u64, kind: RunEventKind) -> RunEvent {
        RunEvent {
            schema_version: crate::durability::DURABILITY_SCHEMA_VERSION,
            run_id: "run-1".into(),
            sequence,
            event_id: format!("event-{sequence}"),
            kind,
        }
    }
}
