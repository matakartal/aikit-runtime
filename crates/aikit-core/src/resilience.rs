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
            for attempt in 1..=self.retry.attempts() {
                self.record(target, attempt, AttemptOutcome::Started)?;
                let mut target_request = request.clone();
                target_request.model.clone_from(&target.model);

                let opened = tokio::time::timeout(
                    self.retry.timeout(),
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

                // Do not expose a provider until it has produced its first delta. This is the
                // final safe point at which a timeout can retry/fallback without duplicate text.
                let first = match tokio::time::timeout(self.retry.timeout(), candidate.next()).await
                {
                    Ok(Some(delta)) => delta,
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
                            "provider stream ended before its first delta",
                        )
                        .into());
                    }
                    Err(_) => {
                        let error: AikitError = ProviderError::new(
                            target.provider.name(),
                            &target.model,
                            ProviderErrorKind::Timeout,
                            format!(
                                "provider emitted no first delta within {} ms",
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
                        continue;
                    }
                };

                // A fail-closed audit sink gets the last word before any provider output is
                // released or the target becomes sticky.
                self.record(target, attempt, AttemptOutcome::FirstDelta)?;
                self.sticky_target.store(target_index, Ordering::Release);
                return Ok(Box::pin(
                    stream::once(async move { first }).chain(candidate),
                ));
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
            Ok(Box::pin(stream::iter(vec![
                StreamDelta::TextDelta { text: "one".into() },
                StreamDelta::Error {
                    message: "midstream".into(),
                    info: crate::error::ErrorInfo::new(crate::error::ErrorCode::ProviderProtocol),
                },
            ])))
        }
    }

    #[tokio::test]
    async fn a_midstream_error_is_forwarded_without_retry_or_fallback() {
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
