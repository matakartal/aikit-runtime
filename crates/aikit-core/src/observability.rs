//! Structured audit events for the agent runtime.
//!
//! Audit is deliberately separate from [`StreamDelta`](crate::types::StreamDelta): stream deltas
//! are model/user output, while audit records are operational and governance evidence. One event
//! can be fanned out to memory, JSONL, or an OpenTelemetry bridge without changing agent output.

use crate::error::{AikitError, Result};
use crate::types::Usage;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{SystemTime, UNIX_EPOCH};

static NEXT_RUN_ID: AtomicU64 = AtomicU64::new(1);
static JSONL_APPEND_LOCKS: OnceLock<Mutex<HashMap<PathBuf, Weak<Mutex<()>>>>> = OnceLock::new();

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TraceSpanKind {
    Agent,
    Workflow,
    Model,
    Tool,
    Checkpoint,
    Activity,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TraceSpanStatus {
    #[default]
    Running,
    Ok,
    Error,
    Cancelled,
}

/// Sensitive prompt/tool/media payloads are excluded unless explicitly enabled.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TelemetryPolicy {
    #[serde(default)]
    pub include_sensitive_payloads: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TraceSpan {
    pub span_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_span_id: Option<String>,
    pub kind: TraceSpanKind,
    pub name: String,
    pub started_unix_ms: u128,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ended_unix_ms: Option<u128>,
    pub status: TraceSpanStatus,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub attributes: BTreeMap<String, Value>,
    /// Present only when [`TelemetryPolicy::include_sensitive_payloads`] is explicitly true.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sensitive_payload: Option<Value>,
}

/// In-process span builder used by the OpenTelemetry bridge and deterministic tests.
pub struct TraceCollector {
    policy: TelemetryPolicy,
    spans: Mutex<BTreeMap<String, TraceSpan>>,
}

impl TraceCollector {
    pub fn new(policy: TelemetryPolicy) -> Self {
        Self {
            policy,
            spans: Mutex::new(BTreeMap::new()),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn start_span(
        &self,
        span_id: impl Into<String>,
        parent_span_id: Option<String>,
        kind: TraceSpanKind,
        name: impl Into<String>,
        attributes: BTreeMap<String, Value>,
        sensitive_payload: Option<Value>,
    ) -> std::result::Result<(), String> {
        let span_id = span_id.into();
        let name = name.into();
        if span_id.trim().is_empty() || name.trim().is_empty() {
            return Err("span_id and name must not be empty".into());
        }
        if attributes.len() > 128 {
            return Err("trace attributes exceed the 128-item limit".into());
        }
        let mut spans = self
            .spans
            .lock()
            .map_err(|_| "trace collector mutex poisoned".to_string())?;
        if spans.contains_key(&span_id) {
            return Err(format!("duplicate span_id '{span_id}'"));
        }
        if parent_span_id
            .as_ref()
            .is_some_and(|parent| !spans.contains_key(parent))
        {
            return Err("parent span must exist before its child".into());
        }
        spans.insert(
            span_id.clone(),
            TraceSpan {
                span_id,
                parent_span_id,
                kind,
                name,
                started_unix_ms: now_unix_ms(),
                ended_unix_ms: None,
                status: TraceSpanStatus::Running,
                attributes,
                sensitive_payload: self
                    .policy
                    .include_sensitive_payloads
                    .then_some(sensitive_payload)
                    .flatten(),
            },
        );
        Ok(())
    }

    pub fn end_span(
        &self,
        span_id: &str,
        status: TraceSpanStatus,
    ) -> std::result::Result<(), String> {
        if status == TraceSpanStatus::Running {
            return Err("a completed span cannot retain running status".into());
        }
        let mut spans = self
            .spans
            .lock()
            .map_err(|_| "trace collector mutex poisoned".to_string())?;
        let span = spans
            .get_mut(span_id)
            .ok_or_else(|| format!("unknown span_id '{span_id}'"))?;
        if span.ended_unix_ms.is_some() {
            return Err(format!("span '{span_id}' already ended"));
        }
        span.status = status;
        span.ended_unix_ms = Some(now_unix_ms().max(span.started_unix_ms));
        Ok(())
    }

    pub fn spans(&self) -> Vec<TraceSpan> {
        self.spans
            .lock()
            .map(|spans| spans.values().cloned().collect())
            .unwrap_or_default()
    }
}

fn now_unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

/// Whether audit payloads contain tool inputs/results or metadata only.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuditPayloadPolicy {
    /// Safest default: record names, decisions, sizes, and errors but not arbitrary tool data.
    MetadataOnly,
    /// Record tool inputs and bounded result previews. Use only with an appropriately protected
    /// audit store because tool data can contain user secrets.
    Full,
}

/// What to do when an audit sink cannot persist a record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuditFailureMode {
    /// Continue the run. Appropriate for development telemetry.
    BestEffort,
    /// Abort the governed action. Appropriate when an audit trail is a compliance requirement.
    FailClosed,
}

/// One typed runtime/governance event.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AuditEvent {
    RunStarted {
        model: String,
    },
    RequestStarted {
        turn: usize,
        model: String,
        message_count: usize,
        tool_count: usize,
    },
    RouteSelected {
        provider: String,
        model: String,
        rationale: String,
    },
    ProviderAttempt {
        provider: String,
        model: String,
        attempt: u32,
        outcome: String,
    },
    SubagentStarted {
        subagent_id: String,
    },
    SubagentCompleted {
        subagent_id: String,
        status: String,
    },
    PermissionDecision {
        turn: usize,
        tool_use_id: String,
        tool: String,
        decision: String,
        source: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        input: Option<Value>,
    },
    HookCompleted {
        turn: usize,
        phase: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tool: Option<String>,
        outcome: String,
    },
    ToolStarted {
        turn: usize,
        tool_use_id: String,
        tool: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        input: Option<Value>,
    },
    ToolCompleted {
        turn: usize,
        tool_use_id: String,
        tool: String,
        is_error: bool,
        output_bytes: usize,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        output_preview: Option<String>,
    },
    Usage {
        turn: usize,
        usage: Usage,
    },
    BudgetUpdated {
        turn: usize,
        total_tokens: u64,
        estimated_cost_usd: f64,
    },
    StructuredOutputAttempt {
        attempt: u32,
        fidelity: String,
    },
    StructuredOutputValidationFailed {
        attempt: u32,
        error: String,
    },
    StructuredOutputCompleted {
        attempts: u32,
        fidelity: String,
    },
    Failure {
        turn: usize,
        stage: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tool: Option<String>,
        error: String,
        terminal: bool,
    },
    RunFailed {
        turn: usize,
        error: String,
    },
    RunStopped {
        turns: usize,
        reason: String,
    },
}

/// Event envelope persisted by sinks.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AuditRecord {
    pub run_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_label: Option<String>,
    pub sequence: u64,
    pub unix_ms: u128,
    #[serde(flatten)]
    pub event: AuditEvent,
}

/// Destination for audit records.
pub trait AuditSink: Send + Sync {
    fn record(&self, record: &AuditRecord) -> std::result::Result<(), String>;
}

/// Cloneable dispatcher configuration. A plain clone intentionally preserves identity for helper
/// components within one run; [`AuditTrail::fresh_run`] must be used at invocation boundaries.
#[derive(Clone)]
pub struct AuditTrail {
    run_id: String,
    parent_run_id: Option<String>,
    run_label: Option<String>,
    next_sequence: Arc<AtomicU64>,
    sinks: Vec<Arc<dyn AuditSink>>,
    payload_policy: AuditPayloadPolicy,
    failure_mode: AuditFailureMode,
    max_preview_bytes: usize,
}

impl Default for AuditTrail {
    fn default() -> Self {
        AuditTrail::new()
    }
}

impl AuditTrail {
    pub fn new() -> Self {
        let id = NEXT_RUN_ID.fetch_add(1, Ordering::Relaxed);
        AuditTrail {
            run_id: format!("run-{}-{id}", std::process::id()),
            parent_run_id: None,
            run_label: None,
            next_sequence: Arc::new(AtomicU64::new(1)),
            sinks: Vec::new(),
            payload_policy: AuditPayloadPolicy::MetadataOnly,
            failure_mode: AuditFailureMode::BestEffort,
            max_preview_bytes: 4096,
        }
    }

    pub fn with_sink(mut self, sink: Arc<dyn AuditSink>) -> Self {
        self.sinks.push(sink);
        self
    }

    pub fn with_payload_policy(mut self, policy: AuditPayloadPolicy) -> Self {
        self.payload_policy = policy;
        self
    }

    pub fn with_failure_mode(mut self, mode: AuditFailureMode) -> Self {
        self.failure_mode = mode;
        self
    }

    /// Create an independently sequenced child run that preserves the same sinks, payload policy,
    /// and failure mode while recording an explicit parent relationship.
    pub fn child(&self, label: impl Into<String>) -> Self {
        let id = NEXT_RUN_ID.fetch_add(1, Ordering::Relaxed);
        AuditTrail {
            run_id: format!("run-{}-{id}", std::process::id()),
            parent_run_id: Some(self.run_id.clone()),
            run_label: Some(label.into()),
            next_sequence: Arc::new(AtomicU64::new(1)),
            sinks: self.sinks.clone(),
            payload_policy: self.payload_policy,
            failure_mode: self.failure_mode,
            max_preview_bytes: self.max_preview_bytes,
        }
    }

    /// Allocate a new independently sequenced invocation while preserving sinks and policy.
    /// Existing parent correlation and label are retained, so a configured subagent remains a
    /// child of its parent even though each execution receives its own run id.
    pub fn fresh_run(&self) -> Self {
        let id = NEXT_RUN_ID.fetch_add(1, Ordering::Relaxed);
        AuditTrail {
            run_id: format!("run-{}-{id}", std::process::id()),
            parent_run_id: self.parent_run_id.clone(),
            run_label: self.run_label.clone(),
            next_sequence: Arc::new(AtomicU64::new(1)),
            sinks: self.sinks.clone(),
            payload_policy: self.payload_policy,
            failure_mode: self.failure_mode,
            max_preview_bytes: self.max_preview_bytes,
        }
    }

    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    pub fn captures_payloads(&self) -> bool {
        self.payload_policy == AuditPayloadPolicy::Full
    }

    pub fn capture_value(&self, value: &Value) -> Option<Value> {
        self.captures_payloads().then(|| value.clone())
    }

    pub fn capture_output(&self, output: &str) -> Option<String> {
        if !self.captures_payloads() {
            return None;
        }
        let mut end = output.len().min(self.max_preview_bytes);
        while !output.is_char_boundary(end) {
            end -= 1;
        }
        Some(output[..end].to_string())
    }

    pub fn emit(&self, event: AuditEvent) -> Result<()> {
        if self.sinks.is_empty() {
            if self.failure_mode == AuditFailureMode::FailClosed {
                return Err(AikitError::Audit(
                    "audit is fail-closed but no audit sink is configured".into(),
                ));
            }
            return Ok(());
        }
        let record = AuditRecord {
            run_id: self.run_id.clone(),
            parent_run_id: self.parent_run_id.clone(),
            run_label: self.run_label.clone(),
            sequence: self.next_sequence.fetch_add(1, Ordering::Relaxed),
            unix_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
            event,
        };
        for sink in &self.sinks {
            if let Err(error) = sink.record(&record) {
                if self.failure_mode == AuditFailureMode::FailClosed {
                    return Err(AikitError::Audit(format!(
                        "audit sink failed (fail-closed): {error}"
                    )));
                }
            }
        }
        Ok(())
    }
}

/// Thread-safe in-memory sink for tests, embedding, and host-language inspection.
#[derive(Default)]
pub struct InMemoryAuditSink {
    records: Mutex<Vec<AuditRecord>>,
}

impl InMemoryAuditSink {
    pub fn records(&self) -> Vec<AuditRecord> {
        self.records.lock().expect("audit mutex poisoned").clone()
    }
}

impl AuditSink for InMemoryAuditSink {
    fn record(&self, record: &AuditRecord) -> std::result::Result<(), String> {
        self.records
            .lock()
            .map_err(|_| "audit mutex poisoned".to_string())?
            .push(record.clone());
        Ok(())
    }
}

fn jsonl_append_lock(path: &Path) -> std::io::Result<Arc<Mutex<()>>> {
    let file_name = path.file_name().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "audit log path must include a file name",
        )
    })?;
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let key = std::fs::canonicalize(parent)?.join(file_name);

    let registry = JSONL_APPEND_LOCKS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut registry = registry
        .lock()
        .map_err(|_| std::io::Error::other("audit append-lock registry poisoned"))?;
    if let Some(lock) = registry.get(&key).and_then(Weak::upgrade) {
        return Ok(lock);
    }

    registry.retain(|_, lock| lock.strong_count() > 0);
    let lock = Arc::new(Mutex::new(()));
    registry.insert(key, Arc::downgrade(&lock));
    Ok(lock)
}

/// Append-only JSON Lines sink. Each line is a complete [`AuditRecord`].
///
/// Sink instances targeting the same canonical parent and file name serialize their appends
/// within this process. Writers in other processes require their own coordination.
pub struct JsonlAuditSink {
    writer: Mutex<File>,
    append_lock: Arc<Mutex<()>>,
}

impl JsonlAuditSink {
    pub fn open(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let path = path.as_ref();
        let append_lock = jsonl_append_lock(path)?;
        let file = {
            let _append_guard = append_lock
                .lock()
                .map_err(|_| std::io::Error::other("audit append lock poisoned"))?;
            // The Unix open below is race-safe through O_NOFOLLOW. This preflight also gives other
            // platforms a fail-closed guard against an already-present symlink.
            if std::fs::symlink_metadata(path)
                .is_ok_and(|metadata| metadata.file_type().is_symlink())
            {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "refusing to open an audit log through a symlink",
                ));
            }
            let mut options = OpenOptions::new();
            options.create(true).append(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
            }
            let file = options.open(path)?;
            if !file.metadata()?.is_file() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "audit log target is not a regular file",
                ));
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                // `OpenOptionsExt::mode` only affects newly created files. Tighten an existing file
                // through the opened descriptor as well, avoiding a pathname replacement race.
                file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
            }
            file
        };
        Ok(JsonlAuditSink {
            writer: Mutex::new(file),
            append_lock,
        })
    }
}

impl AuditSink for JsonlAuditSink {
    fn record(&self, record: &AuditRecord) -> std::result::Result<(), String> {
        let mut line = serde_json::to_vec(record).map_err(|error| error.to_string())?;
        line.push(b'\n');

        let _append_guard = self
            .append_lock
            .lock()
            .map_err(|_| "audit append lock poisoned".to_string())?;
        let mut writer = self
            .writer
            .lock()
            .map_err(|_| "audit writer mutex poisoned".to_string())?;
        writer.write_all(&line).map_err(|error| error.to_string())?;
        writer.flush().map_err(|error| error.to_string())
    }
}

/// Optional OpenTelemetry bridge. The host application remains responsible for installing and
/// shutting down its tracer provider/exporter; a library must never replace global telemetry.
#[cfg(feature = "opentelemetry")]
pub struct OpenTelemetryAuditSink {
    spans: Mutex<HashMap<String, opentelemetry::global::BoxedSpan>>,
}

#[cfg(feature = "opentelemetry")]
impl Default for OpenTelemetryAuditSink {
    fn default() -> Self {
        OpenTelemetryAuditSink {
            spans: Mutex::new(HashMap::new()),
        }
    }
}

#[cfg(feature = "opentelemetry")]
impl AuditSink for OpenTelemetryAuditSink {
    fn record(&self, record: &AuditRecord) -> std::result::Result<(), String> {
        use opentelemetry::trace::{Span, Tracer};
        use opentelemetry::{global, KeyValue};

        let mut spans = self
            .spans
            .lock()
            .map_err(|_| "OpenTelemetry span mutex poisoned".to_string())?;
        match &record.event {
            AuditEvent::RunStarted { model } => {
                let tracer = global::tracer("aikit");
                let mut span = tracer.start("aikit.agent.run");
                span.set_attribute(KeyValue::new("aikit.run_id", record.run_id.clone()));
                span.set_attribute(KeyValue::new("gen_ai.request.model", model.clone()));
                spans.insert(record.run_id.clone(), span);
            }
            AuditEvent::RunStopped { turns, reason } => {
                if let Some(mut span) = spans.remove(&record.run_id) {
                    span.set_attribute(KeyValue::new("aikit.turns", *turns as i64));
                    span.set_attribute(KeyValue::new("aikit.stop_reason", reason.clone()));
                    span.add_event(
                        "aikit.run_stopped",
                        vec![KeyValue::new(
                            "aikit.audit.sequence",
                            record.sequence as i64,
                        )],
                    );
                    span.end();
                }
            }
            event => {
                if let Some(span) = spans.get_mut(&record.run_id) {
                    let (name, attrs) = otel_event(event, record.sequence);
                    span.add_event(name, attrs);
                }
            }
        }
        Ok(())
    }
}

#[cfg(feature = "opentelemetry")]
fn otel_event(event: &AuditEvent, sequence: u64) -> (String, Vec<opentelemetry::KeyValue>) {
    use opentelemetry::KeyValue;

    let mut attrs = vec![KeyValue::new("aikit.audit.sequence", sequence as i64)];
    let name = match event {
        AuditEvent::RouteSelected {
            provider, model, ..
        } => {
            attrs.push(KeyValue::new("gen_ai.provider.name", provider.clone()));
            attrs.push(KeyValue::new("gen_ai.request.model", model.clone()));
            "aikit.route_selected"
        }
        AuditEvent::ProviderAttempt {
            provider,
            model,
            attempt,
            outcome,
        } => {
            attrs.push(KeyValue::new("gen_ai.provider.name", provider.clone()));
            attrs.push(KeyValue::new("gen_ai.request.model", model.clone()));
            attrs.push(KeyValue::new("aikit.provider.attempt", i64::from(*attempt)));
            attrs.push(KeyValue::new("aikit.provider.outcome", outcome.clone()));
            "aikit.provider_attempt"
        }
        AuditEvent::SubagentStarted { subagent_id } => {
            attrs.push(KeyValue::new("aikit.subagent.id", subagent_id.clone()));
            "aikit.subagent_started"
        }
        AuditEvent::SubagentCompleted {
            subagent_id,
            status,
        } => {
            attrs.push(KeyValue::new("aikit.subagent.id", subagent_id.clone()));
            attrs.push(KeyValue::new("aikit.subagent.status", status.clone()));
            "aikit.subagent_completed"
        }
        AuditEvent::RequestStarted { turn, model, .. } => {
            attrs.push(KeyValue::new("aikit.turn", *turn as i64));
            attrs.push(KeyValue::new("gen_ai.request.model", model.clone()));
            "aikit.request_started"
        }
        AuditEvent::PermissionDecision {
            turn,
            tool,
            decision,
            source,
            ..
        } => {
            attrs.push(KeyValue::new("aikit.turn", *turn as i64));
            attrs.push(KeyValue::new("aikit.tool", tool.clone()));
            attrs.push(KeyValue::new("aikit.permission.decision", decision.clone()));
            attrs.push(KeyValue::new("aikit.permission.source", source.clone()));
            "aikit.permission_decision"
        }
        AuditEvent::HookCompleted {
            turn,
            phase,
            outcome,
            ..
        } => {
            attrs.push(KeyValue::new("aikit.turn", *turn as i64));
            attrs.push(KeyValue::new("aikit.hook.phase", phase.clone()));
            attrs.push(KeyValue::new("aikit.hook.outcome", outcome.clone()));
            "aikit.hook_completed"
        }
        AuditEvent::ToolStarted { turn, tool, .. } => {
            attrs.push(KeyValue::new("aikit.turn", *turn as i64));
            attrs.push(KeyValue::new("aikit.tool", tool.clone()));
            "aikit.tool_started"
        }
        AuditEvent::ToolCompleted {
            turn,
            tool,
            is_error,
            output_bytes,
            ..
        } => {
            attrs.push(KeyValue::new("aikit.turn", *turn as i64));
            attrs.push(KeyValue::new("aikit.tool", tool.clone()));
            attrs.push(KeyValue::new("error.type", *is_error));
            attrs.push(KeyValue::new("aikit.output_bytes", *output_bytes as i64));
            "aikit.tool_completed"
        }
        AuditEvent::Usage { turn, usage } => {
            attrs.push(KeyValue::new("aikit.turn", *turn as i64));
            attrs.push(KeyValue::new(
                "gen_ai.usage.input_tokens",
                usage.input_tokens as i64,
            ));
            attrs.push(KeyValue::new(
                "gen_ai.usage.output_tokens",
                usage.output_tokens as i64,
            ));
            "aikit.usage"
        }
        AuditEvent::BudgetUpdated {
            turn,
            total_tokens,
            estimated_cost_usd,
        } => {
            attrs.push(KeyValue::new("aikit.turn", *turn as i64));
            attrs.push(KeyValue::new(
                "aikit.budget.total_tokens",
                *total_tokens as i64,
            ));
            attrs.push(KeyValue::new(
                "aikit.budget.estimated_cost_usd",
                *estimated_cost_usd,
            ));
            "aikit.budget_updated"
        }
        AuditEvent::StructuredOutputAttempt { attempt, fidelity } => {
            attrs.push(KeyValue::new(
                "aikit.structured.attempt",
                i64::from(*attempt),
            ));
            attrs.push(KeyValue::new("aikit.structured.fidelity", fidelity.clone()));
            "aikit.structured_output_attempt"
        }
        AuditEvent::StructuredOutputValidationFailed { attempt, .. } => {
            attrs.push(KeyValue::new(
                "aikit.structured.attempt",
                i64::from(*attempt),
            ));
            "aikit.structured_output_validation_failed"
        }
        AuditEvent::StructuredOutputCompleted { attempts, fidelity } => {
            attrs.push(KeyValue::new(
                "aikit.structured.attempts",
                i64::from(*attempts),
            ));
            attrs.push(KeyValue::new("aikit.structured.fidelity", fidelity.clone()));
            "aikit.structured_output_completed"
        }
        AuditEvent::Failure {
            turn,
            stage,
            terminal,
            ..
        } => {
            attrs.push(KeyValue::new("aikit.turn", *turn as i64));
            attrs.push(KeyValue::new("error.type", stage.clone()));
            attrs.push(KeyValue::new("aikit.failure.terminal", *terminal));
            "aikit.failure"
        }
        AuditEvent::RunFailed { turn, .. } => {
            attrs.push(KeyValue::new("aikit.turn", *turn as i64));
            "aikit.run_failed"
        }
        AuditEvent::RunStarted { .. } | AuditEvent::RunStopped { .. } => unreachable!(),
    };
    (name.into(), attrs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn trace_collector_redacts_payloads_and_enforces_parent_order() {
        let collector = TraceCollector::new(TelemetryPolicy::default());
        collector
            .start_span(
                "agent-1",
                None,
                TraceSpanKind::Agent,
                "agent",
                BTreeMap::new(),
                Some(json!({"prompt": "secret"})),
            )
            .unwrap();
        collector
            .start_span(
                "tool-1",
                Some("agent-1".into()),
                TraceSpanKind::Tool,
                "search",
                BTreeMap::new(),
                Some(json!({"input": "private"})),
            )
            .unwrap();
        collector.end_span("tool-1", TraceSpanStatus::Ok).unwrap();
        let spans = collector.spans();
        assert_eq!(spans.len(), 2);
        assert!(spans.iter().all(|span| span.sensitive_payload.is_none()));
        assert!(collector
            .start_span(
                "orphan",
                Some("missing".into()),
                TraceSpanKind::Activity,
                "orphan",
                BTreeMap::new(),
                None,
            )
            .is_err());
    }

    #[test]
    fn sensitive_trace_payloads_require_explicit_opt_in() {
        let collector = TraceCollector::new(TelemetryPolicy {
            include_sensitive_payloads: true,
        });
        collector
            .start_span(
                "model-1",
                None,
                TraceSpanKind::Model,
                "model",
                BTreeMap::new(),
                Some(json!({"prompt": "allowed"})),
            )
            .unwrap();
        assert_eq!(
            collector.spans()[0].sensitive_payload,
            Some(json!({"prompt": "allowed"}))
        );
    }

    #[test]
    fn in_memory_sink_preserves_order_and_redacts_payloads_by_default() {
        let sink = Arc::new(InMemoryAuditSink::default());
        let trail = AuditTrail::new().with_sink(sink.clone());
        assert_eq!(trail.capture_value(&json!({ "secret": "x" })), None);
        trail
            .emit(AuditEvent::RunStarted { model: "m".into() })
            .unwrap();
        trail
            .emit(AuditEvent::RunStopped {
                turns: 1,
                reason: "end_turn".into(),
            })
            .unwrap();
        let records = sink.records();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].sequence, 1);
        assert_eq!(records[1].sequence, 2);
        assert_eq!(records[0].run_id, records[1].run_id);
    }

    #[test]
    fn full_payload_mode_caps_utf8_preview_safely() {
        let mut trail = AuditTrail::new().with_payload_policy(AuditPayloadPolicy::Full);
        trail.max_preview_bytes = 5;
        assert_eq!(trail.capture_output("ağır-data"), Some("ağı".into()));
        assert_eq!(
            trail.capture_value(&json!({ "q": "x" })),
            Some(json!({ "q": "x" }))
        );
    }

    #[test]
    fn jsonl_sink_writes_parseable_records() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("audit.jsonl");
        let sink = Arc::new(JsonlAuditSink::open(&path).unwrap());
        let trail = AuditTrail::new().with_sink(sink);
        trail
            .emit(AuditEvent::RunStarted {
                model: "mock-1".into(),
            })
            .unwrap();
        let line = std::fs::read_to_string(&path).unwrap();
        let parsed: AuditRecord = serde_json::from_str(line.trim()).unwrap();
        assert!(matches!(parsed.event, AuditEvent::RunStarted { .. }));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
    }

    #[test]
    fn jsonl_sinks_serialize_concurrent_appends_across_path_aliases() {
        const SINK_COUNT: usize = 8;
        const RECORDS_PER_SINK: usize = 100;

        let dir = tempfile::tempdir().unwrap();
        let logs = dir.path().join("logs");
        std::fs::create_dir_all(logs.join("nested")).unwrap();
        let real_path = logs.join("audit.jsonl");
        let alias_path = logs.join("nested").join("..").join("audit.jsonl");
        let paths = [real_path.clone(), alias_path];
        let barrier = Arc::new(std::sync::Barrier::new(SINK_COUNT));
        let mut expected_sources = std::collections::HashMap::new();
        let mut handles = Vec::with_capacity(SINK_COUNT);
        let sinks = (0..SINK_COUNT)
            .map(|index| JsonlAuditSink::open(&paths[index % paths.len()]).unwrap())
            .collect::<Vec<_>>();
        assert!(Arc::ptr_eq(&sinks[0].append_lock, &sinks[1].append_lock));

        for (source, sink) in sinks.into_iter().enumerate() {
            let trail = AuditTrail::new()
                .with_sink(Arc::new(sink))
                .with_failure_mode(AuditFailureMode::FailClosed);
            expected_sources.insert(trail.run_id().to_string(), source);
            let barrier = barrier.clone();
            handles.push(std::thread::spawn(move || {
                let stage = format!("source-{source}");
                let error = format!("source-{source}:{}", "x".repeat(2048));
                barrier.wait();
                for turn in 0..RECORDS_PER_SINK {
                    trail
                        .emit(AuditEvent::Failure {
                            turn,
                            stage: stage.clone(),
                            tool: None,
                            error: error.clone(),
                            terminal: false,
                        })
                        .unwrap();
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let contents = std::fs::read_to_string(&real_path).unwrap();
        let lines = contents.lines().collect::<Vec<_>>();
        assert_eq!(lines.len(), SINK_COUNT * RECORDS_PER_SINK);
        let mut observed = std::collections::HashMap::<String, Vec<(u64, usize, String)>>::new();
        for line in lines {
            let AuditRecord {
                run_id,
                sequence,
                event,
                ..
            } = serde_json::from_str::<AuditRecord>(line).unwrap();
            let AuditEvent::Failure { turn, stage, .. } = event else {
                panic!("unexpected audit event in concurrent JSONL test");
            };
            observed
                .entry(run_id)
                .or_default()
                .push((sequence, turn, stage));
        }

        assert_eq!(observed.len(), SINK_COUNT);
        for (run_id, source) in expected_sources {
            let mut records = observed.remove(&run_id).unwrap();
            records.sort_unstable_by_key(|record| record.0);
            assert_eq!(records.len(), RECORDS_PER_SINK);
            for (index, (sequence, turn, stage)) in records.into_iter().enumerate() {
                assert_eq!(sequence, index as u64 + 1);
                assert_eq!(turn, index);
                assert_eq!(stage, format!("source-{source}"));
            }
        }
        assert!(observed.is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn jsonl_sink_tightens_existing_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("audit.jsonl");
        std::fs::write(&path, b"").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o666)).unwrap();

        let _sink = JsonlAuditSink::open(&path).unwrap();
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }

    #[cfg(unix)]
    #[test]
    fn jsonl_sink_refuses_symlink_targets() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("sensitive.txt");
        let link = dir.path().join("audit.jsonl");
        std::fs::write(&target, b"do not append").unwrap();
        symlink(&target, &link).unwrap();

        let error = JsonlAuditSink::open(&link).err().unwrap();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        assert_eq!(std::fs::read_to_string(&target).unwrap(), "do not append");
    }

    #[test]
    fn fail_closed_requires_at_least_one_sink() {
        let trail = AuditTrail::new().with_failure_mode(AuditFailureMode::FailClosed);
        let error = trail
            .emit(AuditEvent::RunStarted { model: "m".into() })
            .unwrap_err();
        assert!(error.to_string().contains("no audit sink"));
    }

    #[test]
    fn child_trail_has_independent_sequence_and_parent_correlation() {
        let sink = Arc::new(InMemoryAuditSink::default());
        let parent = AuditTrail::new().with_sink(sink.clone());
        let child = parent.child("researcher");
        parent
            .emit(AuditEvent::RunStarted { model: "p".into() })
            .unwrap();
        child
            .emit(AuditEvent::RunStarted { model: "c".into() })
            .unwrap();

        let records = sink.records();
        assert_eq!(records[0].sequence, 1);
        assert_eq!(records[1].sequence, 1);
        assert_eq!(records[1].parent_run_id.as_deref(), Some(parent.run_id()));
        assert_eq!(records[1].run_label.as_deref(), Some("researcher"));
    }

    #[test]
    fn concurrent_fresh_runs_have_distinct_ids_and_independent_sequences() {
        let sink = Arc::new(InMemoryAuditSink::default());
        let configured = AuditTrail::new().with_sink(sink.clone());
        let first = configured.clone().fresh_run();
        let second = configured.fresh_run();
        let first_id = first.run_id().to_string();
        let second_id = second.run_id().to_string();
        assert_ne!(first_id, second_id);

        let a = std::thread::spawn(move || {
            first
                .emit(AuditEvent::RunStarted { model: "a".into() })
                .unwrap();
            first
                .emit(AuditEvent::RunStopped {
                    turns: 1,
                    reason: "done".into(),
                })
                .unwrap();
        });
        let b = std::thread::spawn(move || {
            second
                .emit(AuditEvent::RunStarted { model: "b".into() })
                .unwrap();
            second
                .emit(AuditEvent::RunStopped {
                    turns: 1,
                    reason: "done".into(),
                })
                .unwrap();
        });
        a.join().unwrap();
        b.join().unwrap();

        let records = sink.records();
        for run_id in [first_id, second_id] {
            let sequences = records
                .iter()
                .filter(|record| record.run_id == run_id)
                .map(|record| record.sequence)
                .collect::<Vec<_>>();
            assert_eq!(sequences, vec![1, 2]);
        }
    }
}
