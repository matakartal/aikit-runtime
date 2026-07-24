//! Retry and provider fallback without duplicated streamed output or tool side effects.
//!
//! A target may be retried/fallen back only before its first delta is released. Once a provider
//! emits anything, that stream is sticky and errors are forwarded as-is. Tool execution sits
//! above this layer in the runtime and is therefore never replayed by resilience.

use crate::error::{AikitError, ProviderError, ProviderErrorKind, Result};
use crate::providers::{Provider, ProviderRequest};
use crate::types::StreamDelta;
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::{stream, StreamExt};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const MAX_SAFE_PREFIX_BYTES: usize = 1024 * 1024;
const MAX_SAFE_PREFIX_ITEMS: usize = 1024;

#[derive(Default)]
struct SafePrefixBudget {
    bytes: usize,
    items: usize,
}

impl SafePrefixBudget {
    fn retain(&mut self, delta: &StreamDelta) -> bool {
        let Some(bytes) = self.bytes.checked_add(safe_prefix_delta_bytes(delta)) else {
            return false;
        };
        let Some(items) = self.items.checked_add(1) else {
            return false;
        };
        if bytes > MAX_SAFE_PREFIX_BYTES || items > MAX_SAFE_PREFIX_ITEMS {
            return false;
        }
        self.bytes = bytes;
        self.items = items;
        true
    }
}

fn safe_prefix_delta_bytes(delta: &StreamDelta) -> usize {
    const STRUCTURAL_BYTES: usize = 32;
    let payload = match delta {
        StreamDelta::MessageStart { model } => model.len(),
        StreamDelta::ProviderMetadata { provider, metadata } => provider
            .len()
            .saturating_add(crate::providers::json_retained_bytes(metadata)),
        StreamDelta::Warning { warning } => warning
            .code
            .len()
            .saturating_add(warning.message.len())
            .saturating_add(warning.parameter.as_deref().map_or(0, str::len))
            .saturating_add(warning.provider.as_deref().map_or(0, str::len))
            .saturating_add(warning.model.as_deref().map_or(0, str::len)),
        StreamDelta::Usage(usage) => std::mem::size_of_val(usage),
        _ => 0,
    };
    payload.saturating_add(STRUCTURAL_BYTES)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryPolicy {
    /// Includes the first attempt. A value of zero is normalized to one.
    pub max_attempts_per_model: u32,
    pub base_delay_ms: u64,
    pub max_delay_ms: u64,
    pub per_attempt_timeout_ms: u64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        RetryPolicy {
            max_attempts_per_model: 2,
            base_delay_ms: 250,
            max_delay_ms: 4_000,
            per_attempt_timeout_ms: 30_000,
        }
    }
}

impl RetryPolicy {
    fn attempts(self) -> u32 {
        self.max_attempts_per_model.max(1)
    }

    fn timeout(self) -> Duration {
        Duration::from_millis(self.per_attempt_timeout_ms.max(1))
    }

    fn delay(self, attempt: u32, retry_after_ms: Option<u64>) -> Duration {
        let exponent = attempt.saturating_sub(1).min(31);
        let backoff = self
            .base_delay_ms
            .saturating_mul(1_u64 << exponent)
            .min(self.max_delay_ms);
        Duration::from_millis(retry_after_ms.unwrap_or(backoff).min(self.max_delay_ms))
    }
}

#[derive(Clone)]
pub struct ModelTarget {
    pub model: String,
    pub provider: Arc<dyn Provider>,
}

impl ModelTarget {
    pub fn new(model: impl Into<String>, provider: Arc<dyn Provider>) -> Self {
        ModelTarget {
            model: model.into(),
            provider,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttemptOutcome {
    Started,
    RetryableFailure(ProviderErrorKind),
    NonRetryableFailure(ProviderErrorKind),
    FirstDelta,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelAttemptRecord {
    pub provider: String,
    pub model: String,
    pub attempt: u32,
    pub outcome: AttemptOutcome,
}

#[derive(Clone)]
pub struct ExecutionPlan {
    pub targets: Vec<ModelTarget>,
    pub retry: RetryPolicy,
    pub audit: Option<crate::observability::AuditTrail>,
}

impl ExecutionPlan {
    pub fn new(targets: Vec<ModelTarget>) -> Result<Self> {
        if targets.is_empty() {
            return Err(AikitError::Other(
                "execution plan requires at least one model target".into(),
            ));
        }
        Ok(ExecutionPlan {
            targets,
            retry: RetryPolicy::default(),
            audit: None,
        })
    }

    pub fn with_retry(mut self, retry: RetryPolicy) -> Self {
        self.retry = retry;
        self
    }

    pub fn with_audit(mut self, audit: crate::observability::AuditTrail) -> Self {
        self.audit = Some(audit);
        self
    }

    pub fn into_provider(self) -> ResilientProvider {
        ResilientProvider {
            targets: self.targets,
            retry: self.retry,
            audit: self.audit,
            sticky_target: AtomicUsize::new(0),
            attempts: Mutex::new(Vec::new()),
        }
    }
}

pub struct ResilientProvider {
    targets: Vec<ModelTarget>,
    retry: RetryPolicy,
    audit: Option<crate::observability::AuditTrail>,
    sticky_target: AtomicUsize,
    attempts: Mutex<Vec<ModelAttemptRecord>>,
}

impl ResilientProvider {
    pub fn attempts(&self) -> Vec<ModelAttemptRecord> {
        self.attempts
            .lock()
            .expect("resilience attempt mutex poisoned")
            .clone()
    }

    pub fn selected_target(&self) -> usize {
        self.sticky_target.load(Ordering::Acquire)
    }

    fn record(&self, target: &ModelTarget, attempt: u32, outcome: AttemptOutcome) -> Result<()> {
        if let Some(audit) = &self.audit {
            audit.emit(crate::observability::AuditEvent::ProviderAttempt {
                provider: target.provider.name().to_string(),
                model: target.model.clone(),
                attempt,
                outcome: match &outcome {
                    AttemptOutcome::Started => "started".into(),
                    AttemptOutcome::RetryableFailure(kind) => {
                        format!("retryable_failure:{kind:?}").to_ascii_lowercase()
                    }
                    AttemptOutcome::NonRetryableFailure(kind) => {
                        format!("non_retryable_failure:{kind:?}").to_ascii_lowercase()
                    }
                    AttemptOutcome::FirstDelta => "first_delta".into(),
                },
            })?;
        }
        self.attempts
            .lock()
            .map_err(|_| AikitError::Conflict("resilience attempt state is unavailable".into()))?
            .push(ModelAttemptRecord {
                provider: target.provider.name().to_string(),
                model: target.model.clone(),
                attempt,
                outcome,
            });
        Ok(())
    }
}

fn retryable_error_before_commit(delta: &StreamDelta, target: &ModelTarget) -> Option<AikitError> {
    let StreamDelta::Error { message, info } = delta else {
        return None;
    };
    let kind = match info.code {
        crate::error::ErrorCode::ProviderRateLimit => ProviderErrorKind::RateLimited,
        crate::error::ErrorCode::ProviderTimeout => ProviderErrorKind::Timeout,
        crate::error::ErrorCode::ProviderTransport => ProviderErrorKind::Transport,
        crate::error::ErrorCode::ProviderServer => ProviderErrorKind::Server,
        _ => return None,
    };
    if !info.retryable {
        return None;
    }
    Some(
        ProviderError {
            provider: info
                .provider
                .clone()
                .unwrap_or_else(|| target.provider.name().to_string()),
            model: info.model.clone().unwrap_or_else(|| target.model.clone()),
            kind,
            status: info.status,
            retry_after_ms: info.retry_after_ms,
            message: message.clone(),
            warnings: info.warnings.clone(),
        }
        .into(),
    )
}

fn commits_provider_output(delta: &StreamDelta) -> bool {
    match delta {
        StreamDelta::MessageStart { .. }
        | StreamDelta::ProviderMetadata { .. }
        | StreamDelta::Warning { .. } => false,
        // Non-zero usage is a financial/accounting side effect. Once a provider reports it, the
        // attempt becomes sticky so the usage reaches the runtime budget/audit path exactly once
        // and an automatic retry cannot silently multiply billed work.
        StreamDelta::Usage(usage) => *usage != crate::types::Usage::default(),
        _ => true,
    }
}

#[async_trait]
impl Provider for ResilientProvider {
    fn name(&self) -> &str {
        self.targets[self.sticky_target.load(Ordering::Acquire)]
            .provider
            .name()
    }

    async fn stream(&self, request: ProviderRequest) -> Result<BoxStream<'static, StreamDelta>> {
        let start = self.sticky_target.load(Ordering::Acquire);
        let mut last_retryable: Option<AikitError> = None;

        for (target_index, target) in self.targets.iter().enumerate().skip(start) {
            'attempts: for attempt in 1..=self.retry.attempts() {
                self.record(target, attempt, AttemptOutcome::Started)?;
                let mut target_request = request.clone();
                target_request.model.clone_from(&target.model);

                // Opening the stream and consuming any non-content prefix share one absolute
                // deadline. A provider cannot keep resetting the clock with warnings/metadata.
                let first_output_deadline = tokio::time::Instant::now() + self.retry.timeout();

                let opened = tokio::time::timeout_at(
                    first_output_deadline,
                    target.provider.stream(target_request),
                )
                .await;
                let mut candidate = match opened {
                    Ok(Ok(stream)) => stream,
                    Ok(Err(error)) => {
                        let Some(provider_error) = error.provider_error() else {
                            return Err(error);
                        };
                        let retryable = provider_error.retryable();
                        self.record(
                            target,
                            attempt,
                            if retryable {
                                AttemptOutcome::RetryableFailure(provider_error.kind)
                            } else {
                                AttemptOutcome::NonRetryableFailure(provider_error.kind)
                            },
                        )?;
                        if !retryable {
                            return Err(error);
                        }
                        let delay = self.retry.delay(attempt, provider_error.retry_after_ms);
                        last_retryable = Some(error);
                        if attempt < self.retry.attempts() {
                            tokio::time::sleep(delay).await;
                        }
                        continue;
                    }
                    Err(_) => {
                        let error: AikitError = ProviderError::new(
                            target.provider.name(),
                            &target.model,
                            ProviderErrorKind::Timeout,
                            format!(
                                "provider did not open a stream within {} ms",
                                self.retry.per_attempt_timeout_ms
                            ),
                        )
                        .into();
                        self.record(
                            target,
                            attempt,
                            AttemptOutcome::RetryableFailure(ProviderErrorKind::Timeout),
                        )?;
                        let delay = self.retry.delay(attempt, None);
                        last_retryable = Some(error);
                        if attempt < self.retry.attempts() {
                            tokio::time::sleep(delay).await;
                        }
                        continue;
                    }
                };

                // Warning/start/metadata/usage deltas do not duplicate model content or tool
                // side effects. Buffer that prefix so a retryable stream error or timeout can
                // still retry/fallback before any meaningful provider output is released.
                let mut prefix = Vec::new();
                let mut prefix_budget = SafePrefixBudget::default();
                let first = loop {
                    match tokio::time::timeout_at(first_output_deadline, candidate.next()).await {
                        Ok(Some(delta)) => {
                            if let Some(error) = retryable_error_before_commit(&delta, target) {
                                let provider_error = error
                                    .provider_error()
                                    .expect("retryable stream errors are typed provider errors");
                                self.record(
                                    target,
                                    attempt,
                                    AttemptOutcome::RetryableFailure(provider_error.kind),
                                )?;
                                let delay =
                                    self.retry.delay(attempt, provider_error.retry_after_ms);
                                last_retryable = Some(error);
                                if attempt < self.retry.attempts() {
                                    tokio::time::sleep(delay).await;
                                }
                                continue 'attempts;
                            }
                            if commits_provider_output(&delta) {
                                break delta;
                            }
                            if !prefix_budget.retain(&delta) {
                                self.record(
                                    target,
                                    attempt,
                                    AttemptOutcome::NonRetryableFailure(
                                        ProviderErrorKind::Protocol,
                                    ),
                                )?;
                                return Err(ProviderError::new(
                                    target.provider.name(),
                                    &target.model,
                                    ProviderErrorKind::Protocol,
                                    "provider pre-content stream prefix exceeded the retained byte or item limit",
                                )
                                .into());
                            }
                            prefix.push(delta);
                        }
                        Ok(None) => {
                            self.record(
                                target,
                                attempt,
                                AttemptOutcome::NonRetryableFailure(ProviderErrorKind::Protocol),
                            )?;
                            return Err(ProviderError::new(
                                target.provider.name(),
                                &target.model,
                                ProviderErrorKind::Protocol,
                                "provider stream ended before meaningful output or a terminal delta",
                            )
                            .into());
                        }
                        Err(_) => {
                            let error: AikitError = ProviderError::new(
                                target.provider.name(),
                                &target.model,
                                ProviderErrorKind::Timeout,
                                format!(
                                    "provider emitted no meaningful output within {} ms",
                                    self.retry.per_attempt_timeout_ms
                                ),
                            )
                            .into();
                            self.record(
                                target,
                                attempt,
                                AttemptOutcome::RetryableFailure(ProviderErrorKind::Timeout),
                            )?;
                            last_retryable = Some(error);
                            if attempt < self.retry.attempts() {
                                tokio::time::sleep(self.retry.delay(attempt, None)).await;
                            }
                            continue 'attempts;
                        }
                    }
                };

                // A fail-closed audit sink gets the last word before any provider output is
                // released or the target becomes sticky.
                self.record(target, attempt, AttemptOutcome::FirstDelta)?;
                self.sticky_target.store(target_index, Ordering::Release);
                prefix.push(first);
                return Ok(Box::pin(stream::iter(prefix).chain(candidate)));
            }
        }

        Err(last_retryable.unwrap_or_else(|| {
            AikitError::Other("execution plan exhausted without a provider attempt".into())
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observability::{AuditEvent, AuditFailureMode, AuditRecord, AuditSink, AuditTrail};
    use crate::types::{Message, Usage};
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct SequenceProvider {
        name: &'static str,
        failures_left: AtomicUsize,
        calls: AtomicUsize,
        kind: ProviderErrorKind,
    }

    impl SequenceProvider {
        fn new(name: &'static str, failures: usize, kind: ProviderErrorKind) -> Self {
            SequenceProvider {
                name,
                failures_left: AtomicUsize::new(failures),
                calls: AtomicUsize::new(0),
                kind,
            }
        }
    }

    #[async_trait]
    impl Provider for SequenceProvider {
        fn name(&self) -> &str {
            self.name
        }

        async fn stream(&self, req: ProviderRequest) -> Result<BoxStream<'static, StreamDelta>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self
                .failures_left
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                    left.checked_sub(1)
                })
                .is_ok()
            {
                return Err(ProviderError::new(self.name, req.model, self.kind, "planned").into());
            }
            Ok(Box::pin(stream::iter(vec![
                StreamDelta::TextDelta {
                    text: self.name.into(),
                },
                StreamDelta::Usage(Usage::default()),
                StreamDelta::MessageStop {
                    stop_reason: "end_turn".into(),
                },
            ])))
        }
    }

    fn request() -> ProviderRequest {
        ProviderRequest {
            model: "primary-model".into(),
            messages: vec![Message::user("hi")],
            tools: Vec::new(),
            max_tokens: 10,
            options: Default::default(),
            provider_options: Default::default(),
            compatibility_mode: crate::contract::CompatibilityMode::Strict,
        }
    }

    fn no_wait_retry(attempts: u32) -> RetryPolicy {
        RetryPolicy {
            max_attempts_per_model: attempts,
            base_delay_ms: 0,
            max_delay_ms: 0,
            per_attempt_timeout_ms: 1_000,
        }
    }

    struct FailProviderAttemptAudit {
        outcome: &'static str,
    }

    impl AuditSink for FailProviderAttemptAudit {
        fn record(&self, record: &AuditRecord) -> std::result::Result<(), String> {
            if matches!(
                &record.event,
                AuditEvent::ProviderAttempt { outcome, .. } if outcome == self.outcome
            ) {
                Err(format!("planned {0} audit failure", self.outcome))
            } else {
                Ok(())
            }
        }
    }

    fn failing_attempt_audit(outcome: &'static str) -> AuditTrail {
        AuditTrail::new()
            .with_sink(Arc::new(FailProviderAttemptAudit { outcome }))
            .with_failure_mode(AuditFailureMode::FailClosed)
    }

    struct PrefixErrorThenSuccess {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl Provider for PrefixErrorThenSuccess {
        fn name(&self) -> &str {
            "prefix-error"
        }

        async fn stream(&self, req: ProviderRequest) -> Result<BoxStream<'static, StreamDelta>> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            let warning = StreamDelta::Warning {
                warning: crate::contract::ProviderWarning {
                    code: "unverified_provider_parameter".into(),
                    message: "test warning".into(),
                    parameter: Some("future_option".into()),
                    provider: Some(self.name().into()),
                    model: Some(req.model.clone()),
                },
            };
            if call == 0 {
                let mut info =
                    crate::error::ErrorInfo::new(crate::error::ErrorCode::ProviderRateLimit)
                        .with_provider(self.name(), req.model);
                info.retryable = true;
                return Ok(Box::pin(stream::iter(vec![
                    warning,
                    StreamDelta::MessageStart {
                        model: "primary".into(),
                    },
                    StreamDelta::Error {
                        message: "rate limited before content".into(),
                        info,
                    },
                ])));
            }
            Ok(Box::pin(stream::iter(vec![
                warning,
                StreamDelta::MessageStart {
                    model: "primary".into(),
                },
                StreamDelta::TextDelta {
                    text: "success".into(),
                },
                StreamDelta::MessageStop {
                    stop_reason: "end_turn".into(),
                },
            ])))
        }
    }

    struct EndlessPrefixProvider {
        calls: AtomicUsize,
        delta: StreamDelta,
    }

    struct BillablePrefixErrorThenSuccess {
        calls: AtomicUsize,
    }

    struct AnthropicBilledErrorThenSuccess {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl Provider for AnthropicBilledErrorThenSuccess {
        fn name(&self) -> &str {
            "anthropic"
        }

        async fn stream(&self, _req: ProviderRequest) -> Result<BoxStream<'static, StreamDelta>> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if call == 0 {
                let mut parser = crate::providers::anthropic::AnthropicStreamParser::new();
                let mut deltas = parser.push_event(&serde_json::json!({
                    "type": "message_start",
                    "message": {
                        "model": "claude-test",
                        "usage": {"input_tokens": 17, "output_tokens": 0}
                    }
                }));
                deltas.extend(parser.push_event(&serde_json::json!({
                    "type": "error",
                    "error": {"type": "overloaded_error", "message": "busy"}
                })));
                return Ok(Box::pin(stream::iter(deltas)));
            }
            Ok(Box::pin(stream::iter(vec![
                StreamDelta::TextDelta {
                    text: "must not retry".into(),
                },
                StreamDelta::MessageStop {
                    stop_reason: "end_turn".into(),
                },
            ])))
        }
    }

    #[async_trait]
    impl Provider for BillablePrefixErrorThenSuccess {
        fn name(&self) -> &str {
            "billable-prefix-error"
        }

        async fn stream(&self, req: ProviderRequest) -> Result<BoxStream<'static, StreamDelta>> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if call == 0 {
                let mut info =
                    crate::error::ErrorInfo::new(crate::error::ErrorCode::ProviderRateLimit)
                        .with_provider(self.name(), req.model);
                info.retryable = true;
                return Ok(Box::pin(stream::iter(vec![
                    StreamDelta::MessageStart {
                        model: "billable-primary".into(),
                    },
                    StreamDelta::Usage(Usage {
                        input_tokens: 11,
                        output_tokens: 3,
                        ..Usage::default()
                    }),
                    StreamDelta::Error {
                        message: "rate limited after billed work".into(),
                        info,
                    },
                ])));
            }
            Ok(Box::pin(stream::iter(vec![
                StreamDelta::MessageStart {
                    model: "unexpected-retry".into(),
                },
                StreamDelta::TextDelta {
                    text: "must not run".into(),
                },
                StreamDelta::MessageStop {
                    stop_reason: "end_turn".into(),
                },
            ])))
        }
    }

    #[async_trait]
    impl Provider for EndlessPrefixProvider {
        fn name(&self) -> &str {
            "endless-prefix"
        }

        async fn stream(&self, _req: ProviderRequest) -> Result<BoxStream<'static, StreamDelta>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(Box::pin(stream::repeat(self.delta.clone())))
        }
    }

    struct SlowPrefixThenText {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl Provider for SlowPrefixThenText {
        fn name(&self) -> &str {
            "slow-prefix"
        }

        async fn stream(&self, _req: ProviderRequest) -> Result<BoxStream<'static, StreamDelta>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(Box::pin(async_stream::stream! {
                tokio::time::sleep(Duration::from_millis(30)).await;
                yield warning_delta("first delayed warning");
                tokio::time::sleep(Duration::from_millis(30)).await;
                yield warning_delta("second delayed warning");
                yield StreamDelta::TextDelta { text: "too late".into() };
            }))
        }
    }

    fn warning_delta(message: impl Into<String>) -> StreamDelta {
        StreamDelta::Warning {
            warning: crate::contract::ProviderWarning {
                code: "provider_warning".into(),
                message: message.into(),
                parameter: None,
                provider: Some("test".into()),
                model: Some("test-model".into()),
            },
        }
    }

    #[tokio::test]
    async fn fail_closed_started_audit_prevents_provider_call() {
        let provider = Arc::new(SequenceProvider::new(
            "primary",
            0,
            ProviderErrorKind::Server,
        ));
        let resilient = ExecutionPlan::new(vec![ModelTarget::new("p", provider.clone())])
            .unwrap()
            .with_audit(failing_attempt_audit("started"))
            .into_provider();

        let error = resilient.stream(request()).await.err().unwrap();
        assert_eq!(error.info().code, crate::error::ErrorCode::Audit);
        assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
        assert!(resilient.attempts().is_empty());
    }

    #[tokio::test]
    async fn fail_closed_failure_audit_prevents_retry_and_fallback() {
        let primary = Arc::new(SequenceProvider::new(
            "primary",
            usize::MAX,
            ProviderErrorKind::RateLimited,
        ));
        let fallback = Arc::new(SequenceProvider::new(
            "fallback",
            0,
            ProviderErrorKind::Server,
        ));
        let resilient = ExecutionPlan::new(vec![
            ModelTarget::new("p", primary.clone()),
            ModelTarget::new("f", fallback.clone()),
        ])
        .unwrap()
        .with_retry(no_wait_retry(3))
        .with_audit(failing_attempt_audit("retryable_failure:ratelimited"))
        .into_provider();

        let error = resilient.stream(request()).await.err().unwrap();
        assert_eq!(error.info().code, crate::error::ErrorCode::Audit);
        assert_eq!(primary.calls.load(Ordering::SeqCst), 1);
        assert_eq!(fallback.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn fail_closed_first_delta_audit_releases_no_output_and_sets_no_sticky_target() {
        let provider = Arc::new(SequenceProvider::new(
            "primary",
            0,
            ProviderErrorKind::Server,
        ));
        let resilient = ExecutionPlan::new(vec![ModelTarget::new("p", provider.clone())])
            .unwrap()
            .with_audit(failing_attempt_audit("first_delta"))
            .into_provider();

        let error = resilient.stream(request()).await.err().unwrap();
        assert_eq!(error.info().code, crate::error::ErrorCode::Audit);
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
        assert_eq!(resilient.selected_target(), 0);
        assert_eq!(resilient.attempts().len(), 1); // only Started was durable
    }

    #[tokio::test]
    async fn retryable_failures_exhaust_primary_then_fallback_and_stay_sticky() {
        let primary = Arc::new(SequenceProvider::new(
            "primary",
            2,
            ProviderErrorKind::RateLimited,
        ));
        let fallback = Arc::new(SequenceProvider::new(
            "fallback",
            0,
            ProviderErrorKind::Server,
        ));
        let resilient = ExecutionPlan::new(vec![
            ModelTarget::new("p", primary.clone()),
            ModelTarget::new("f", fallback.clone()),
        ])
        .unwrap()
        .with_retry(no_wait_retry(2))
        .into_provider();

        let first: Vec<_> = resilient.stream(request()).await.unwrap().collect().await;
        assert!(matches!(&first[0], StreamDelta::TextDelta { text } if text == "fallback"));
        assert_eq!(resilient.selected_target(), 1);
        let _second: Vec<_> = resilient.stream(request()).await.unwrap().collect().await;
        assert_eq!(primary.calls.load(Ordering::SeqCst), 2);
        assert_eq!(fallback.calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn retryable_error_after_non_content_prefix_retries_without_leaking_failed_attempt() {
        let provider = Arc::new(PrefixErrorThenSuccess {
            calls: AtomicUsize::new(0),
        });
        let resilient = ExecutionPlan::new(vec![ModelTarget::new("p", provider.clone())])
            .unwrap()
            .with_retry(no_wait_retry(2))
            .into_provider();

        let deltas: Vec<_> = resilient.stream(request()).await.unwrap().collect().await;
        assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            deltas
                .iter()
                .filter(|delta| matches!(delta, StreamDelta::Warning { .. }))
                .count(),
            1,
            "only the successful attempt's buffered warning is released"
        );
        assert!(matches!(
            deltas.iter().find(|delta| matches!(delta, StreamDelta::TextDelta { .. })),
            Some(StreamDelta::TextDelta { text }) if text == "success"
        ));
        assert!(!deltas
            .iter()
            .any(|delta| matches!(delta, StreamDelta::Error { .. })));
        assert!(matches!(
            resilient.attempts().as_slice(),
            [
                ModelAttemptRecord {
                    outcome: AttemptOutcome::Started,
                    ..
                },
                ModelAttemptRecord {
                    outcome: AttemptOutcome::RetryableFailure(ProviderErrorKind::RateLimited),
                    ..
                },
                ModelAttemptRecord {
                    outcome: AttemptOutcome::Started,
                    ..
                },
                ModelAttemptRecord {
                    outcome: AttemptOutcome::FirstDelta,
                    ..
                },
            ]
        ));
    }

    #[tokio::test]
    async fn nonzero_usage_commits_attempt_and_prevents_automatic_retry() {
        let provider = Arc::new(BillablePrefixErrorThenSuccess {
            calls: AtomicUsize::new(0),
        });
        let resilient = ExecutionPlan::new(vec![ModelTarget::new("p", provider.clone())])
            .unwrap()
            .with_retry(no_wait_retry(2))
            .into_provider();

        let deltas: Vec<_> = resilient.stream(request()).await.unwrap().collect().await;
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            deltas
                .iter()
                .filter_map(|delta| match delta {
                    StreamDelta::Usage(usage) => Some(*usage),
                    _ => None,
                })
                .collect::<Vec<_>>(),
            vec![Usage {
                input_tokens: 11,
                output_tokens: 3,
                ..Usage::default()
            }]
        );
        assert!(deltas
            .iter()
            .any(|delta| matches!(delta, StreamDelta::Error { .. })));
        assert!(!deltas.iter().any(
            |delta| matches!(delta, StreamDelta::TextDelta { text } if text == "must not run")
        ));
        assert!(matches!(
            resilient.attempts().as_slice(),
            [
                ModelAttemptRecord {
                    outcome: AttemptOutcome::Started,
                    ..
                },
                ModelAttemptRecord {
                    outcome: AttemptOutcome::FirstDelta,
                    ..
                },
            ]
        ));
    }

    #[tokio::test]
    async fn anthropic_billed_terminal_error_reaches_budget_path_without_retry() {
        let provider = Arc::new(AnthropicBilledErrorThenSuccess {
            calls: AtomicUsize::new(0),
        });
        let resilient = ExecutionPlan::new(vec![ModelTarget::new("claude", provider.clone())])
            .unwrap()
            .with_retry(no_wait_retry(2))
            .into_provider();

        let deltas: Vec<_> = resilient.stream(request()).await.unwrap().collect().await;
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
        assert!(deltas.iter().any(|delta| matches!(
            delta,
            StreamDelta::Usage(Usage {
                input_tokens: 17,
                ..
            })
        )));
        assert!(deltas
            .iter()
            .any(|delta| matches!(delta, StreamDelta::Error { .. })));
        assert!(!deltas.iter().any(
            |delta| matches!(delta, StreamDelta::TextDelta { text } if text == "must not retry")
        ));
    }

    #[tokio::test]
    async fn endless_warning_and_metadata_prefixes_fail_closed_at_the_item_bound() {
        let cases = [
            warning_delta("never commits"),
            StreamDelta::ProviderMetadata {
                provider: "test".into(),
                metadata: serde_json::json!({"status": "still waiting"}),
            },
        ];

        for delta in cases {
            let provider = Arc::new(EndlessPrefixProvider {
                calls: AtomicUsize::new(0),
                delta,
            });
            let resilient = ExecutionPlan::new(vec![ModelTarget::new("p", provider.clone())])
                .unwrap()
                .with_retry(no_wait_retry(1))
                .into_provider();

            let error = resilient.stream(request()).await.err().unwrap();
            assert_eq!(error.info().code, crate::error::ErrorCode::ProviderProtocol);
            assert!(error.to_string().contains("prefix exceeded"));
            assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
            assert!(matches!(
                resilient.attempts().as_slice(),
                [
                    ModelAttemptRecord {
                        outcome: AttemptOutcome::Started,
                        ..
                    },
                    ModelAttemptRecord {
                        outcome: AttemptOutcome::NonRetryableFailure(ProviderErrorKind::Protocol),
                        ..
                    },
                ]
            ));
        }
    }

    #[tokio::test]
    async fn oversized_safe_prefix_item_fails_closed_at_the_byte_bound() {
        let provider = Arc::new(EndlessPrefixProvider {
            calls: AtomicUsize::new(0),
            delta: warning_delta("x".repeat(MAX_SAFE_PREFIX_BYTES + 1)),
        });
        let resilient = ExecutionPlan::new(vec![ModelTarget::new("p", provider.clone())])
            .unwrap()
            .with_retry(no_wait_retry(1))
            .into_provider();

        let error = resilient.stream(request()).await.err().unwrap();
        assert_eq!(error.info().code, crate::error::ErrorCode::ProviderProtocol);
        assert!(error.to_string().contains("prefix exceeded"));
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn non_content_prefix_reads_share_one_absolute_first_output_deadline() {
        let provider = Arc::new(SlowPrefixThenText {
            calls: AtomicUsize::new(0),
        });
        let retry = RetryPolicy {
            max_attempts_per_model: 2,
            base_delay_ms: 0,
            max_delay_ms: 0,
            per_attempt_timeout_ms: 50,
        };
        let resilient = ExecutionPlan::new(vec![ModelTarget::new("p", provider.clone())])
            .unwrap()
            .with_retry(retry)
            .into_provider();

        let error = resilient.stream(request()).await.err().unwrap();
        assert_eq!(error.info().code, crate::error::ErrorCode::ProviderTimeout);
        assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
        assert!(matches!(
            resilient.attempts().as_slice(),
            [
                ModelAttemptRecord {
                    outcome: AttemptOutcome::Started,
                    ..
                },
                ModelAttemptRecord {
                    outcome: AttemptOutcome::RetryableFailure(ProviderErrorKind::Timeout),
                    ..
                },
                ModelAttemptRecord {
                    outcome: AttemptOutcome::Started,
                    ..
                },
                ModelAttemptRecord {
                    outcome: AttemptOutcome::RetryableFailure(ProviderErrorKind::Timeout),
                    ..
                },
            ]
        ));
    }

    #[tokio::test]
    async fn authentication_is_not_retried_or_fallen_back() {
        let primary = Arc::new(SequenceProvider::new(
            "primary",
            usize::MAX,
            ProviderErrorKind::Authentication,
        ));
        let fallback = Arc::new(SequenceProvider::new(
            "fallback",
            0,
            ProviderErrorKind::Server,
        ));
        let resilient = ExecutionPlan::new(vec![
            ModelTarget::new("p", primary.clone()),
            ModelTarget::new("f", fallback.clone()),
        ])
        .unwrap()
        .with_retry(no_wait_retry(3))
        .into_provider();

        assert!(resilient.stream(request()).await.is_err());
        assert_eq!(primary.calls.load(Ordering::SeqCst), 1);
        assert_eq!(fallback.calls.load(Ordering::SeqCst), 0);
    }

    struct DeltaThenError;

    #[async_trait]
    impl Provider for DeltaThenError {
        fn name(&self) -> &str {
            "delta-error"
        }

        async fn stream(&self, _req: ProviderRequest) -> Result<BoxStream<'static, StreamDelta>> {
            let mut info = crate::error::ErrorInfo::new(crate::error::ErrorCode::ProviderRateLimit)
                .with_provider(self.name(), "p");
            info.retryable = true;
            Ok(Box::pin(stream::iter(vec![
                StreamDelta::TextDelta { text: "one".into() },
                StreamDelta::Error {
                    message: "midstream".into(),
                    info,
                },
            ])))
        }
    }

    #[tokio::test]
    async fn a_retryable_midstream_error_is_forwarded_without_retry_or_fallback() {
        let fallback = Arc::new(SequenceProvider::new(
            "fallback",
            0,
            ProviderErrorKind::Server,
        ));
        let resilient = ExecutionPlan::new(vec![
            ModelTarget::new("p", Arc::new(DeltaThenError)),
            ModelTarget::new("f", fallback.clone()),
        ])
        .unwrap()
        .with_retry(no_wait_retry(3))
        .into_provider();
        let deltas: Vec<_> = resilient.stream(request()).await.unwrap().collect().await;
        assert_eq!(deltas.len(), 2);
        assert!(matches!(deltas[1], StreamDelta::Error { .. }));
        assert_eq!(fallback.calls.load(Ordering::SeqCst), 0);
    }
}
