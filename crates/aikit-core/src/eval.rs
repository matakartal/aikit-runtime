//! Deterministic, keyless evaluation gates for recorded agent outcomes.
//!
//! Evaluations deliberately consume [`RunOutcome`], the same
//! canonical record used by sessions and resume. They never ask another model to grade a model:
//! text, tool trajectory, terminal state, and usage checks therefore remain reproducible in CI.

use crate::error::{AikitError, Result};
use crate::session::{RunOutcome, RunTerminalStatus};
use crate::types::{ContentBlock, Role, Usage};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

const MAX_DATASET_NAME_BYTES: usize = 128;
const MAX_CASES: usize = 256;
const MAX_CASE_NAME_BYTES: usize = 128;
const MAX_PROMPT_BYTES: usize = 256 * 1024;
const MAX_MODEL_BYTES: usize = 256;
const MAX_GATES_PER_CASE: usize = 128;
const MAX_EXPECTED_TEXT_BYTES: usize = 256 * 1024;
const MAX_TOOL_NAME_BYTES: usize = 128;
const MAX_TOOL_SEQUENCE: usize = 256;
const MAX_EVAL_TOKENS: u64 = 10_000_000;

/// Current JSON dataset/report contract understood by this runtime.
pub const EVAL_SCHEMA_VERSION: u32 = 1;

fn default_model() -> String {
    "mock-1".into()
}

const fn default_max_tokens() -> u64 {
    1024
}

/// A version-control-friendly collection of deterministic evaluation cases.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvalDataset {
    pub schema_version: u32,
    pub name: String,
    #[serde(default = "default_model")]
    pub model: String,
    #[serde(default = "default_max_tokens")]
    pub max_tokens: u64,
    pub cases: Vec<EvalCase>,
}

impl EvalDataset {
    /// Validate all limits before any case is allowed to contact a provider.
    pub fn validate(&self) -> Result<()> {
        if self.schema_version != EVAL_SCHEMA_VERSION {
            return Err(configuration(format!(
                "unsupported eval schema_version {}; expected {EVAL_SCHEMA_VERSION}",
                self.schema_version
            )));
        }
        validate_display_text("dataset name", &self.name, MAX_DATASET_NAME_BYTES)?;
        validate_model(&self.model)?;
        validate_max_tokens("dataset max_tokens", self.max_tokens)?;
        if self.cases.is_empty() {
            return Err(configuration("eval dataset must contain at least one case"));
        }
        if self.cases.len() > MAX_CASES {
            return Err(configuration(format!(
                "eval dataset contains {} cases; maximum is {MAX_CASES}",
                self.cases.len()
            )));
        }

        let mut names = BTreeSet::new();
        for case in &self.cases {
            case.validate()?;
            if !names.insert(case.name.as_str()) {
                return Err(configuration(format!(
                    "eval dataset contains duplicate case name '{}'",
                    case.name
                )));
            }
            validate_model(case.model.as_deref().unwrap_or(&self.model))?;
            validate_max_tokens(
                "case max_tokens",
                case.max_tokens.unwrap_or(self.max_tokens),
            )?;
        }
        Ok(())
    }

    pub fn resolved_model<'a>(&'a self, case: &'a EvalCase) -> &'a str {
        case.model.as_deref().unwrap_or(&self.model)
    }

    pub fn resolved_max_tokens(&self, case: &EvalCase) -> u64 {
        case.max_tokens.unwrap_or(self.max_tokens)
    }
}

/// One prompt and its deterministic gates.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvalCase {
    pub name: String,
    pub prompt: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u64>,
    pub gates: Vec<EvalGate>,
}

impl EvalCase {
    fn validate(&self) -> Result<()> {
        validate_display_text("case name", &self.name, MAX_CASE_NAME_BYTES)?;
        validate_text("case prompt", &self.prompt, MAX_PROMPT_BYTES)?;
        if let Some(system) = &self.system {
            validate_sized_text("case system", system, MAX_PROMPT_BYTES)?;
        }
        if self.gates.is_empty() {
            return Err(configuration(format!(
                "eval case '{}' must contain at least one gate",
                self.name
            )));
        }
        if self.gates.len() > MAX_GATES_PER_CASE {
            return Err(configuration(format!(
                "eval case '{}' contains {} gates; maximum is {MAX_GATES_PER_CASE}",
                self.name,
                self.gates.len()
            )));
        }
        for gate in &self.gates {
            gate.validate()?;
        }
        Ok(())
    }
}

/// Built-in deterministic gates. Additional model-based judging stays outside the trusted core.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum EvalGate {
    OutputExact {
        value: String,
    },
    OutputContains {
        value: String,
    },
    OutputNotContains {
        value: String,
    },
    TerminalStatus {
        status: RunTerminalStatus,
    },
    CalledTool {
        name: String,
    },
    DidNotCallTool {
        name: String,
    },
    ToolSequence {
        names: Vec<String>,
        #[serde(default)]
        exact: bool,
    },
    NoToolErrors,
    MaxTurns {
        value: usize,
    },
    MaxInputTokens {
        value: u64,
    },
    MaxOutputTokens {
        value: u64,
    },
    MaxTotalTokens {
        value: u64,
    },
    MaxModelAttempts {
        value: usize,
    },
}

impl EvalGate {
    pub fn name(&self) -> &'static str {
        match self {
            Self::OutputExact { .. } => "output_exact",
            Self::OutputContains { .. } => "output_contains",
            Self::OutputNotContains { .. } => "output_not_contains",
            Self::TerminalStatus { .. } => "terminal_status",
            Self::CalledTool { .. } => "called_tool",
            Self::DidNotCallTool { .. } => "did_not_call_tool",
            Self::ToolSequence { .. } => "tool_sequence",
            Self::NoToolErrors => "no_tool_errors",
            Self::MaxTurns { .. } => "max_turns",
            Self::MaxInputTokens { .. } => "max_input_tokens",
            Self::MaxOutputTokens { .. } => "max_output_tokens",
            Self::MaxTotalTokens { .. } => "max_total_tokens",
            Self::MaxModelAttempts { .. } => "max_model_attempts",
        }
    }

    fn validate(&self) -> Result<()> {
        match self {
            Self::OutputExact { value } => {
                validate_sized_text("expected output text", value, MAX_EXPECTED_TEXT_BYTES)
            }
            Self::OutputContains { value } | Self::OutputNotContains { value } => {
                validate_text("expected output fragment", value, MAX_EXPECTED_TEXT_BYTES)
            }
            Self::TerminalStatus {
                status: RunTerminalStatus::Running,
            } => Err(configuration(
                "terminal_status cannot expect the non-terminal running state",
            )),
            Self::CalledTool { name } | Self::DidNotCallTool { name } => validate_tool_name(name),
            Self::ToolSequence { names, .. } => {
                if names.is_empty() {
                    return Err(configuration("tool_sequence names cannot be empty"));
                }
                if names.len() > MAX_TOOL_SEQUENCE {
                    return Err(configuration(format!(
                        "tool_sequence contains {} names; maximum is {MAX_TOOL_SEQUENCE}",
                        names.len()
                    )));
                }
                for name in names {
                    validate_tool_name(name)?;
                }
                Ok(())
            }
            Self::MaxTurns { value } | Self::MaxModelAttempts { value } if *value == 0 => Err(
                configuration(format!("{} value must be greater than zero", self.name())),
            ),
            Self::MaxInputTokens { value }
            | Self::MaxOutputTokens { value }
            | Self::MaxTotalTokens { value }
                if *value > MAX_EVAL_TOKENS =>
            {
                Err(configuration(format!(
                    "{} value exceeds {MAX_EVAL_TOKENS}",
                    self.name()
                )))
            }
            _ => Ok(()),
        }
    }
}

/// Result of one gate. Messages describe counts and state, but never echo model output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvalCheck {
    pub gate: String,
    pub passed: bool,
    pub message: String,
}

/// Aggregate deterministic verdict for one outcome.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EvalVerdict {
    pub passed: bool,
    pub passed_checks: usize,
    pub total_checks: usize,
    pub score: f64,
    pub checks: Vec<EvalCheck>,
}

impl EvalVerdict {
    pub fn from_checks(checks: Vec<EvalCheck>) -> Self {
        let total_checks = checks.len();
        let passed_checks = checks.iter().filter(|check| check.passed).count();
        let score = if total_checks == 0 {
            0.0
        } else {
            passed_checks as f64 / total_checks as f64
        };
        Self {
            passed: total_checks > 0 && passed_checks == total_checks,
            passed_checks,
            total_checks,
            score,
            checks,
        }
    }

    pub fn runtime_failure(message: impl Into<String>) -> Self {
        Self::from_checks(vec![EvalCheck {
            gate: "runtime".into(),
            passed: false,
            message: message.into(),
        }])
    }
}

/// One case in a serializable dataset report.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EvalCaseReport {
    pub name: String,
    pub model: String,
    pub verdict: EvalVerdict,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<Usage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_status: Option<RunTerminalStatus>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub model_attempts: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<crate::error::ErrorInfo>,
}

impl EvalCaseReport {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: impl Into<String>,
        model: impl Into<String>,
        verdict: EvalVerdict,
        usage: Option<Usage>,
        terminal_status: Option<RunTerminalStatus>,
        model_attempts: Vec<String>,
        error: Option<crate::error::ErrorInfo>,
    ) -> Self {
        Self {
            name: name.into(),
            model: model.into(),
            verdict,
            usage,
            terminal_status,
            model_attempts,
            error,
        }
    }

    pub fn passed(&self) -> bool {
        self.verdict.passed
    }
}

/// Dataset-level CI report. No timestamps are included, keeping keyless reports reproducible.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EvalReport {
    pub schema_version: u32,
    pub runtime_version: String,
    pub dataset_sha256: String,
    pub dataset: String,
    pub passed: bool,
    pub passed_cases: usize,
    pub total_cases: usize,
    pub cases: Vec<EvalCaseReport>,
}

impl EvalReport {
    pub fn new(
        dataset: &EvalDataset,
        dataset_sha256: impl Into<String>,
        cases: Vec<EvalCaseReport>,
    ) -> Self {
        let total_cases = cases.len();
        let passed_cases = cases.iter().filter(|case| case.passed()).count();
        Self {
            schema_version: dataset.schema_version,
            runtime_version: env!("CARGO_PKG_VERSION").into(),
            dataset_sha256: dataset_sha256.into(),
            dataset: dataset.name.clone(),
            passed: total_cases > 0 && passed_cases == total_cases,
            passed_cases,
            total_cases,
            cases,
        }
    }
}

/// Evaluate canonical output, tool trajectory, terminal state, and usage without another model.
pub fn evaluate_outcome(outcome: &RunOutcome, gates: &[EvalGate]) -> Result<EvalVerdict> {
    if gates.is_empty() {
        return Err(configuration("at least one eval gate is required"));
    }
    if gates.len() > MAX_GATES_PER_CASE {
        return Err(configuration(format!(
            "eval contains {} gates; maximum is {MAX_GATES_PER_CASE}",
            gates.len()
        )));
    }
    for gate in gates {
        gate.validate()?;
    }

    let requires_messages = gates.iter().any(|gate| {
        matches!(
            gate,
            EvalGate::OutputExact { .. }
                | EvalGate::OutputContains { .. }
                | EvalGate::OutputNotContains { .. }
                | EvalGate::CalledTool { .. }
                | EvalGate::DidNotCallTool { .. }
                | EvalGate::ToolSequence { .. }
                | EvalGate::NoToolErrors
                | EvalGate::MaxTurns { .. }
        )
    });
    let messages = invocation_messages(outcome, requires_messages)?;
    let output = final_text(messages);
    let tools = tool_calls(messages);
    let tool_errors = tool_error_count(messages);
    let turns = messages
        .iter()
        .filter(|message| message.role == Role::Assistant)
        .count();
    let total_tokens = outcome
        .usage
        .input_tokens
        .saturating_add(outcome.usage.output_tokens);

    let mut checks = gates
        .iter()
        .map(|gate| match gate {
            EvalGate::OutputExact { value } => check(
                gate,
                output == *value,
                format!(
                    "expected exact output of {} bytes; observed {} bytes",
                    value.len(),
                    output.len()
                ),
            ),
            EvalGate::OutputContains { value } => check(
                gate,
                output.contains(value),
                format!("expected output to contain a {} byte fragment", value.len()),
            ),
            EvalGate::OutputNotContains { value } => check(
                gate,
                !output.contains(value),
                format!("expected output to omit a {} byte fragment", value.len()),
            ),
            EvalGate::TerminalStatus { status } => check(
                gate,
                outcome.terminal_status == *status,
                format!(
                    "expected status {status:?}; observed {:?}",
                    outcome.terminal_status
                ),
            ),
            EvalGate::CalledTool { name } => check(
                gate,
                tools.iter().any(|tool| tool == name),
                format!(
                    "expected tool '{name}' to be called; observed {} call(s)",
                    tools.len()
                ),
            ),
            EvalGate::DidNotCallTool { name } => check(
                gate,
                tools.iter().all(|tool| tool != name),
                format!(
                    "expected tool '{name}' not to be called; observed {} call(s)",
                    tools.len()
                ),
            ),
            EvalGate::ToolSequence { names, exact } => {
                let passed = if *exact {
                    tools == *names
                } else {
                    is_subsequence(names, &tools)
                };
                check(
                    gate,
                    passed,
                    format!(
                        "expected {} tool sequence of {} call(s); observed {} call(s)",
                        if *exact { "exact" } else { "ordered" },
                        names.len(),
                        tools.len()
                    ),
                )
            }
            EvalGate::NoToolErrors => check(
                gate,
                tool_errors == 0,
                format!("observed {tool_errors} failed tool result(s)"),
            ),
            EvalGate::MaxTurns { value } => check(
                gate,
                turns <= *value,
                format!("maximum {value} turn(s); observed {turns}"),
            ),
            EvalGate::MaxInputTokens { value } => check(
                gate,
                outcome.usage.input_tokens <= *value,
                format!(
                    "maximum {value} input token(s); observed {}",
                    outcome.usage.input_tokens
                ),
            ),
            EvalGate::MaxOutputTokens { value } => check(
                gate,
                outcome.usage.output_tokens <= *value,
                format!(
                    "maximum {value} output token(s); observed {}",
                    outcome.usage.output_tokens
                ),
            ),
            EvalGate::MaxTotalTokens { value } => check(
                gate,
                total_tokens <= *value,
                format!("maximum {value} total token(s); observed {total_tokens}"),
            ),
            EvalGate::MaxModelAttempts { value } => check(
                gate,
                outcome.model_attempts.len() <= *value,
                format!(
                    "maximum {value} model attempt(s); observed {}",
                    outcome.model_attempts.len()
                ),
            ),
        })
        .collect::<Vec<_>>();

    // Permissive checks must not turn a crashed or unfinished run into a false green. Tests that
    // intentionally expect failure can opt into that behavior with an explicit terminal gate.
    let has_terminal_gate = gates
        .iter()
        .any(|gate| matches!(gate, EvalGate::TerminalStatus { .. }));
    if outcome.terminal_status != RunTerminalStatus::Completed && !has_terminal_gate {
        checks.push(EvalCheck {
            gate: "runtime_completed".into(),
            passed: false,
            message: format!(
                "expected a completed run; observed {:?}",
                outcome.terminal_status
            ),
        });
    }
    Ok(EvalVerdict::from_checks(checks))
}

fn check(gate: &EvalGate, passed: bool, message: String) -> EvalCheck {
    EvalCheck {
        gate: gate.name().into(),
        passed,
        message,
    }
}

fn invocation_messages(outcome: &RunOutcome, required: bool) -> Result<&[crate::types::Message]> {
    match outcome.invocation_start_message_index {
        Some(index) if index <= outcome.messages.len() => Ok(&outcome.messages[index..]),
        Some(_) => Err(configuration(
            "RunOutcome invocation_start_message_index exceeds its message count",
        )),
        None if required => Err(configuration(
            "message-derived eval gates require RunOutcome.invocation_start_message_index",
        )),
        None => Ok(&[]),
    }
}

fn final_text(messages: &[crate::types::Message]) -> String {
    messages
        .iter()
        .rev()
        .find(|message| message.role == Role::Assistant)
        .map(|message| {
            message
                .content
                .iter()
                .filter_map(|block| match block {
                    ContentBlock::Text { text } => Some(text.as_str()),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join("")
        })
        .unwrap_or_default()
}

fn tool_calls(messages: &[crate::types::Message]) -> Vec<String> {
    messages
        .iter()
        .filter(|message| message.role == Role::Assistant)
        .flat_map(|message| &message.content)
        .filter_map(|block| match block {
            ContentBlock::ToolUse { name, .. } => Some(name.clone()),
            _ => None,
        })
        .collect()
}

fn tool_error_count(messages: &[crate::types::Message]) -> usize {
    messages
        .iter()
        .filter(|message| message.role == Role::Tool)
        .flat_map(|message| &message.content)
        .filter(|block| matches!(block, ContentBlock::ToolResult { is_error: true, .. }))
        .count()
}

fn is_subsequence(expected: &[String], actual: &[String]) -> bool {
    let mut expected = expected.iter();
    let Some(mut wanted) = expected.next() else {
        return true;
    };
    for observed in actual {
        if observed == wanted {
            let Some(next) = expected.next() else {
                return true;
            };
            wanted = next;
        }
    }
    false
}

fn validate_model(model: &str) -> Result<()> {
    validate_display_text("model", model, MAX_MODEL_BYTES)
}

fn validate_max_tokens(label: &str, value: u64) -> Result<()> {
    if value == 0 || value > MAX_EVAL_TOKENS {
        return Err(configuration(format!(
            "{label} must be between 1 and {MAX_EVAL_TOKENS}"
        )));
    }
    Ok(())
}

fn validate_tool_name(name: &str) -> Result<()> {
    validate_display_text("tool name", name, MAX_TOOL_NAME_BYTES)
}

fn validate_display_text(label: &str, value: &str, max_bytes: usize) -> Result<()> {
    validate_text(label, value, max_bytes)?;
    if value.chars().any(|character| {
        character.is_control()
            || matches!(
                character,
                '\u{061c}'
                    | '\u{200e}'
                    | '\u{200f}'
                    | '\u{202a}'..='\u{202e}'
                    | '\u{2066}'..='\u{206f}'
            )
    }) {
        return Err(configuration(format!(
            "{label} cannot contain control or bidirectional formatting characters"
        )));
    }
    Ok(())
}

fn validate_text(label: &str, value: &str, max_bytes: usize) -> Result<()> {
    if value.trim().is_empty() {
        return Err(configuration(format!("{label} cannot be empty")));
    }
    validate_sized_text(label, value, max_bytes)
}

fn validate_sized_text(label: &str, value: &str, max_bytes: usize) -> Result<()> {
    if value.len() > max_bytes {
        return Err(configuration(format!(
            "{label} is {} bytes; maximum is {max_bytes}",
            value.len()
        )));
    }
    Ok(())
}

fn configuration(message: impl Into<String>) -> AikitError {
    AikitError::Configuration(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Message, Usage};
    use serde_json::json;

    fn outcome() -> RunOutcome {
        RunOutcome {
            messages: vec![
                Message::user("find it"),
                Message {
                    role: Role::Assistant,
                    content: vec![ContentBlock::ToolUse {
                        id: "call-1".into(),
                        name: "search".into(),
                        input: json!({"q":"rust"}),
                    }],
                },
                Message {
                    role: Role::Tool,
                    content: vec![ContentBlock::ToolResult {
                        tool_use_id: "call-1".into(),
                        content: "found".into(),
                        is_error: false,
                    }],
                },
                Message {
                    role: Role::Assistant,
                    content: vec![ContentBlock::Text {
                        text: "Rust result".into(),
                    }],
                },
            ],
            invocation_start_message_index: Some(1),
            usage: Usage {
                input_tokens: 20,
                output_tokens: 5,
                ..Usage::default()
            },
            terminal_status: RunTerminalStatus::Completed,
            stop_reason: Some("end_turn".into()),
            model_attempts: vec!["mock-1".into()],
            final_text: Some("Rust result".into()),
            ..RunOutcome::default()
        }
    }

    #[test]
    fn evaluates_text_tools_status_and_usage_deterministically() {
        let gates = vec![
            EvalGate::OutputContains {
                value: "Rust".into(),
            },
            EvalGate::CalledTool {
                name: "search".into(),
            },
            EvalGate::NoToolErrors,
            EvalGate::TerminalStatus {
                status: RunTerminalStatus::Completed,
            },
            EvalGate::MaxTurns { value: 2 },
            EvalGate::MaxTotalTokens { value: 25 },
        ];
        let verdict = evaluate_outcome(&outcome(), &gates).unwrap();
        assert!(verdict.passed);
        assert_eq!(verdict.passed_checks, 6);
        assert_eq!(verdict.score, 1.0);
    }

    #[test]
    fn failed_gate_reports_counts_without_echoing_model_output() {
        let verdict = evaluate_outcome(
            &outcome(),
            &[EvalGate::OutputExact {
                value: "different".into(),
            }],
        )
        .unwrap();
        assert!(!verdict.passed);
        assert_eq!(
            verdict.checks[0].message,
            "expected exact output of 9 bytes; observed 11 bytes"
        );
        assert!(!verdict.checks[0].message.contains("Rust result"));
    }

    #[test]
    fn tool_sequence_supports_exact_and_ordered_modes() {
        let mut outcome = outcome();
        outcome.messages.insert(
            3,
            Message {
                role: Role::Assistant,
                content: vec![ContentBlock::ToolUse {
                    id: "call-2".into(),
                    name: "read".into(),
                    input: json!({}),
                }],
            },
        );
        let ordered = evaluate_outcome(
            &outcome,
            &[EvalGate::ToolSequence {
                names: vec!["search".into(), "read".into()],
                exact: false,
            }],
        )
        .unwrap();
        assert!(ordered.passed);
        let wrong = evaluate_outcome(
            &outcome,
            &[EvalGate::ToolSequence {
                names: vec!["read".into(), "search".into()],
                exact: false,
            }],
        )
        .unwrap();
        assert!(!wrong.passed);
    }

    #[test]
    fn dataset_rejects_duplicates_and_unknown_fields() {
        let duplicate = EvalDataset {
            schema_version: EVAL_SCHEMA_VERSION,
            name: "suite".into(),
            model: "mock-1".into(),
            max_tokens: 32,
            cases: vec![
                EvalCase {
                    name: "same".into(),
                    prompt: "one".into(),
                    system: None,
                    model: None,
                    max_tokens: None,
                    gates: vec![EvalGate::NoToolErrors],
                },
                EvalCase {
                    name: "same".into(),
                    prompt: "two".into(),
                    system: None,
                    model: None,
                    max_tokens: None,
                    gates: vec![EvalGate::NoToolErrors],
                },
            ],
        };
        assert!(duplicate.validate().is_err());
        assert!(serde_json::from_value::<EvalDataset>(json!({
            "schema_version": EVAL_SCHEMA_VERSION,
            "name":"suite",
            "surprise":true,
            "cases":[]
        }))
        .is_err());
    }

    #[test]
    fn failed_outcomes_require_an_explicit_terminal_expectation() {
        let mut failed = outcome();
        failed.terminal_status = RunTerminalStatus::Failed;

        let implicit = evaluate_outcome(&failed, &[EvalGate::NoToolErrors]).unwrap();
        assert!(!implicit.passed);
        assert_eq!(implicit.checks.last().unwrap().gate, "runtime_completed");

        let explicit = evaluate_outcome(
            &failed,
            &[EvalGate::TerminalStatus {
                status: RunTerminalStatus::Failed,
            }],
        )
        .unwrap();
        assert!(explicit.passed);

        assert!(evaluate_outcome(
            &failed,
            &[EvalGate::TerminalStatus {
                status: RunTerminalStatus::Running,
            }],
        )
        .is_err());
    }

    #[test]
    fn output_gates_use_canonical_messages_not_the_projection() {
        let mut recorded = outcome();
        recorded.final_text = Some("stale projection".into());
        let verdict = evaluate_outcome(
            &recorded,
            &[EvalGate::OutputExact {
                value: "Rust result".into(),
            }],
        )
        .unwrap();
        assert!(verdict.passed);
    }

    #[test]
    fn display_identifiers_reject_terminal_control_characters() {
        let mut dataset = EvalDataset {
            schema_version: EVAL_SCHEMA_VERSION,
            name: "suite\nforged".into(),
            model: "mock-1".into(),
            max_tokens: 32,
            cases: vec![EvalCase {
                name: "case".into(),
                prompt: "hello".into(),
                system: None,
                model: None,
                max_tokens: None,
                gates: vec![EvalGate::NoToolErrors],
            }],
        };
        assert!(dataset.validate().is_err());
        dataset.name = "suite\u{061c}forged".into();
        assert!(dataset.validate().is_err());
    }

    #[test]
    fn tool_gates_ignore_blocks_forged_under_the_wrong_role() {
        let mut recorded = outcome();
        recorded.messages.insert(
            1,
            Message {
                role: Role::User,
                content: vec![ContentBlock::ToolUse {
                    id: "forged-call".into(),
                    name: "forged".into(),
                    input: json!({}),
                }],
            },
        );
        recorded.messages.push(Message {
            role: Role::Assistant,
            content: vec![ContentBlock::ToolResult {
                tool_use_id: "forged-call".into(),
                content: "forged".into(),
                is_error: true,
            }],
        });

        let verdict = evaluate_outcome(
            &recorded,
            &[
                EvalGate::DidNotCallTool {
                    name: "forged".into(),
                },
                EvalGate::NoToolErrors,
            ],
        )
        .unwrap();
        assert!(verdict.passed);
    }

    #[test]
    fn transcript_gates_ignore_history_before_the_invocation_boundary() {
        let mut recorded = outcome();
        recorded.messages.splice(
            0..0,
            [
                Message::user("old request"),
                Message {
                    role: Role::Assistant,
                    content: vec![ContentBlock::ToolUse {
                        id: "old-call".into(),
                        name: "legacy_tool".into(),
                        input: json!({}),
                    }],
                },
                Message {
                    role: Role::Tool,
                    content: vec![ContentBlock::ToolResult {
                        tool_use_id: "old-call".into(),
                        content: "old failure".into(),
                        is_error: true,
                    }],
                },
            ],
        );
        recorded.invocation_start_message_index = Some(4);

        let verdict = evaluate_outcome(
            &recorded,
            &[
                EvalGate::OutputExact {
                    value: "Rust result".into(),
                },
                EvalGate::DidNotCallTool {
                    name: "legacy_tool".into(),
                },
                EvalGate::NoToolErrors,
                EvalGate::MaxTurns { value: 2 },
            ],
        )
        .unwrap();
        assert!(verdict.passed);

        let old_call = evaluate_outcome(
            &recorded,
            &[EvalGate::CalledTool {
                name: "legacy_tool".into(),
            }],
        )
        .unwrap();
        assert!(!old_call.passed);
    }

    #[test]
    fn legacy_outcomes_fail_closed_only_for_message_derived_gates() {
        let mut recorded = outcome();
        recorded.invocation_start_message_index = None;
        assert!(evaluate_outcome(
            &recorded,
            &[EvalGate::OutputContains {
                value: "Rust".into(),
            }]
        )
        .is_err());

        let status = evaluate_outcome(
            &recorded,
            &[EvalGate::TerminalStatus {
                status: RunTerminalStatus::Completed,
            }],
        )
        .unwrap();
        assert!(status.passed);
    }
}
