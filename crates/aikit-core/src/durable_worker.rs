//! Async coordination for running the existing durable runtime from multiple worker processes.
//!
//! The worker does not execute providers or tools itself. It owns an expiring, fenced lease in the
//! existing [`RunState`] event log, then hands an owner-bound
//! [`DurableRunDriver`] to a caller that composes the normal runtime. This is at-least-once
//! coordination: a crash can leave an external effect ambiguous, and such work must be explicitly
//! reconciled before another worker proceeds.

use crate::cancellation::CancellationToken;
use crate::durability::{
    ActivityAttemptStatus, ActivityReconciliation, DurabilityError, DurableRunStatus, RunState,
    MAX_DURABLE_WORKER_LEASE_MS,
};
use crate::durable_runtime::{
    DurableInvocationDisposition, DurableRunDriver, DurableRunDriverError,
};
use crate::durable_store::{DurableStore, DurableStoreError, DurableStoreLeaseAuthority};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

const DEFAULT_LEASE_TTL: Duration = Duration::from_secs(30);
const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);
const DEFAULT_INITIAL_POLL_BACKOFF: Duration = Duration::from_millis(25);
const DEFAULT_MAX_POLL_BACKOFF: Duration = Duration::from_secs(1);
const DEFAULT_MAX_POLL_ATTEMPTS: u32 = 120;
const DEFAULT_CANCELLATION_GRACE: Duration = Duration::from_secs(5);

/// Bounded lease, heartbeat, polling, and cancellation policy for one worker identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableWorkerConfig {
    pub owner_id: String,
    pub lease_ttl: Duration,
    pub heartbeat_interval: Duration,
    pub initial_poll_backoff: Duration,
    pub max_poll_backoff: Duration,
    pub max_poll_attempts: u32,
    pub cancellation_grace: Duration,
}

impl DurableWorkerConfig {
    pub fn new(owner_id: impl Into<String>) -> Result<Self, DurableWorkerError> {
        let config = Self {
            owner_id: owner_id.into(),
            lease_ttl: DEFAULT_LEASE_TTL,
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
            initial_poll_backoff: DEFAULT_INITIAL_POLL_BACKOFF,
            max_poll_backoff: DEFAULT_MAX_POLL_BACKOFF,
            max_poll_attempts: DEFAULT_MAX_POLL_ATTEMPTS,
            cancellation_grace: DEFAULT_CANCELLATION_GRACE,
        };
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), DurableWorkerError> {
        validate_identifier("owner_id", &self.owner_id)?;
        let lease_ttl_ms = duration_ms("lease_ttl", self.lease_ttl)?;
        let heartbeat_interval_ms = duration_ms("heartbeat_interval", self.heartbeat_interval)?;
        if lease_ttl_ms > MAX_DURABLE_WORKER_LEASE_MS {
            return Err(DurableWorkerError::InvalidConfiguration(format!(
                "lease_ttl exceeds the {MAX_DURABLE_WORKER_LEASE_MS}ms durable limit"
            )));
        }
        if heartbeat_interval_ms > lease_ttl_ms / 2
            || self.initial_poll_backoff.is_zero()
            || self.max_poll_backoff < self.initial_poll_backoff
            || self.max_poll_attempts == 0
            || self.cancellation_grace.is_zero()
        {
            return Err(DurableWorkerError::InvalidConfiguration(
                "heartbeat must be no more than half the lease TTL; polling and cancellation bounds must be positive and ordered"
                    .into(),
            ));
        }
        duration_ms("initial_poll_backoff", self.initial_poll_backoff)?;
        duration_ms("max_poll_backoff", self.max_poll_backoff)?;
        duration_ms("cancellation_grace", self.cancellation_grace)?;
        Ok(())
    }
}

/// Result of one bounded worker run.
#[derive(Debug, Clone, PartialEq)]
pub enum DurableWorkerOutcome<T> {
    Executed {
        value: T,
        recovered_claim: bool,
    },
    Terminal {
        status: DurableRunStatus,
        reason: Option<String>,
    },
    AwaitingResume {
        reason: Option<String>,
    },
    ReconcileRequired {
        reason: String,
        activity_ids: Vec<String>,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum DurableWorkerError {
    #[error("invalid durable worker configuration: {0}")]
    InvalidConfiguration(String),
    #[error("durable worker claim for run `{run_id}` was cancelled")]
    Cancelled { run_id: String },
    #[error(
        "durable worker could not claim run `{run_id}` after {attempts} attempts; active owner: {owner_id:?}, expiry: {expires_at_unix_ms:?}"
    )]
    ClaimUnavailable {
        run_id: String,
        attempts: u32,
        owner_id: Option<String>,
        expires_at_unix_ms: Option<u64>,
    },
    #[error("durable worker lease for run `{run_id}` was lost: {reason}")]
    LeaseLost { run_id: String, reason: String },
    #[error("durable worker heartbeat for run `{run_id}` could not start: {reason}")]
    HeartbeatUnavailable { run_id: String, reason: String },
    #[error("{primary}; releasing the worker lease also failed: {cleanup}")]
    CleanupFailed {
        primary: Box<DurableWorkerError>,
        cleanup: Box<DurableWorkerError>,
    },
    #[error("terminal run `{run_id}` cannot be claimed for activity reconciliation ({status:?})")]
    TerminalReconciliation {
        run_id: String,
        status: DurableRunStatus,
    },
    #[error("durable run `{run_id}` does not currently require activity reconciliation")]
    ReconciliationUnavailable { run_id: String },
    #[error("secure durable worker lease identity is unavailable: {0}")]
    Entropy(String),
    #[error("durable worker lease clock is invalid: {0}")]
    Clock(String),
    #[error(transparent)]
    Store(#[from] DurableStoreError),
    #[error(transparent)]
    Driver(#[from] DurableRunDriverError),
}

/// Async lease coordinator over an existing append-only [`DurableStore`].
#[derive(Clone)]
pub struct DurableWorker {
    store: Arc<dyn DurableStore>,
    config: DurableWorkerConfig,
}

impl DurableWorker {
    pub fn new(
        store: Arc<dyn DurableStore>,
        config: DurableWorkerConfig,
    ) -> Result<Self, DurableWorkerError> {
        config.validate()?;
        Ok(Self { store, config })
    }

    pub fn config(&self) -> &DurableWorkerConfig {
        &self.config
    }

    /// Claim a run and execute the caller's existing runtime composition under heartbeats.
    ///
    /// The callback receives the same cooperative cancellation token and an owner-bound driver.
    /// It should attach both to the normal `RunConfig`/`run_agent` path rather than implementing
    /// provider or tool execution inside the worker. The worker proves the lease fence with an
    /// immediate renewal before invoking the callback. Because a lease can still be lost later,
    /// arbitrary callback effects must continue to commit through the supplied driver.
    pub async fn run<F, Fut, T>(
        &self,
        run_id: &str,
        cancellation: CancellationToken,
        execute: F,
    ) -> Result<DurableWorkerOutcome<T>, DurableWorkerError>
    where
        F: FnOnce(DurableRunDriver, CancellationToken) -> Fut,
        Fut: Future<Output = T>,
    {
        validate_identifier("run_id", run_id)?;
        let claim = self.acquire(run_id, &cancellation).await?;
        let driver = claim.driver.clone();
        let recovered_claim = claim.recovered_claim;

        if cancellation.is_cancelled() {
            return self.finish_claim(
                &claim,
                run_id,
                Err(DurableWorkerError::Cancelled {
                    run_id: run_id.to_string(),
                }),
            );
        }

        let disposition = match driver.invocation_disposition() {
            Ok(disposition) => disposition,
            Err(error) => return self.finish_claim(&claim, run_id, Err(error.into())),
        };
        if let Some(status) = claim.terminal_status {
            let outcome = match disposition {
                DurableInvocationDisposition::ReconcileRequired { reason } => {
                    let activity_ids = match driver.snapshot() {
                        Ok(state) => reconciliation_activity_ids(&state),
                        Err(error) => {
                            return self.finish_claim(&claim, run_id, Err(error.into()));
                        }
                    };
                    DurableWorkerOutcome::ReconcileRequired {
                        reason,
                        activity_ids,
                    }
                }
                DurableInvocationDisposition::AlreadyTerminal { status, reason } => {
                    DurableWorkerOutcome::Terminal { status, reason }
                }
                DurableInvocationDisposition::FinalizeTerminal(receipt) => {
                    if let Err(error) = driver.finalize_run_stopped_receipt(&receipt) {
                        return self.finish_claim(&claim, run_id, Err(error.into()));
                    }
                    let reason = match driver.snapshot() {
                        Ok(state) => state.projection().pause_reason.clone(),
                        Err(error) => {
                            return self.finish_claim(&claim, run_id, Err(error.into()));
                        }
                    };
                    DurableWorkerOutcome::Terminal { status, reason }
                }
                _ => {
                    let activity_ids = match driver.snapshot() {
                        Ok(state) => reconciliation_activity_ids(&state),
                        Err(error) => {
                            return self.finish_claim(&claim, run_id, Err(error.into()));
                        }
                    };
                    DurableWorkerOutcome::ReconcileRequired {
                        reason: "terminal durable run has a non-terminal invocation disposition; explicit reconciliation is required"
                            .into(),
                        activity_ids,
                    }
                }
            };
            return self.finish_claim(&claim, run_id, Ok(outcome));
        }
        match disposition {
            DurableInvocationDisposition::AwaitingResume { reason } => {
                return self.finish_claim(
                    &claim,
                    run_id,
                    Ok(DurableWorkerOutcome::AwaitingResume { reason }),
                );
            }
            DurableInvocationDisposition::ReconcileRequired { reason } => {
                let activity_ids = match driver.snapshot() {
                    Ok(state) => reconciliation_activity_ids(&state),
                    Err(error) => {
                        return self.finish_claim(&claim, run_id, Err(error.into()));
                    }
                };
                return self.finish_claim(
                    &claim,
                    run_id,
                    Ok(DurableWorkerOutcome::ReconcileRequired {
                        reason,
                        activity_ids,
                    }),
                );
            }
            DurableInvocationDisposition::AlreadyTerminal { status, reason } => {
                return self.finish_claim(
                    &claim,
                    run_id,
                    Ok(DurableWorkerOutcome::Terminal { status, reason }),
                );
            }
            DurableInvocationDisposition::Execute
            | DurableInvocationDisposition::FinalizeTerminal(_)
            | DurableInvocationDisposition::RetryTerminalAudit(_) => {}
        }

        let (mut heartbeat, mut heartbeat_failure, heartbeat_ready) =
            match self.start_heartbeat(driver.clone(), cancellation.clone(), run_id) {
                Ok(heartbeat) => heartbeat,
                Err(error) => return self.finish_claim(&claim, run_id, Err(error)),
            };
        let readiness = tokio::select! {
            readiness = heartbeat_ready => Some(readiness),
            _ = cancellation.cancelled() => None,
        };
        match readiness {
            Some(Ok(Ok(()))) => {}
            Some(Ok(Err(reason))) => {
                cancellation.cancel();
                let _ = heartbeat.stop(run_id, self.config.cancellation_grace).await;
                return Err(DurableWorkerError::LeaseLost {
                    run_id: run_id.to_string(),
                    reason,
                });
            }
            Some(Err(_)) => {
                cancellation.cancel();
                let _ = heartbeat.stop(run_id, self.config.cancellation_grace).await;
                return Err(DurableWorkerError::LeaseLost {
                    run_id: run_id.to_string(),
                    reason: "heartbeat thread stopped before proving the initial lease fence"
                        .into(),
                });
            }
            None => {
                heartbeat
                    .stop(run_id, self.config.cancellation_grace)
                    .await?;
                return self.finish_claim(
                    &claim,
                    run_id,
                    Err(DurableWorkerError::Cancelled {
                        run_id: run_id.to_string(),
                    }),
                );
            }
        }
        let mut execution = Box::pin(execute(driver, cancellation.clone()));
        let mut cancellation_deadline = None;

        loop {
            tokio::select! {
                value = execution.as_mut() => {
                    drop(execution);
                    heartbeat
                        .stop(run_id, self.config.cancellation_grace)
                        .await?;
                    return self.finish_claim(
                        &claim,
                        run_id,
                        Ok(DurableWorkerOutcome::Executed { value, recovered_claim }),
                    );
                }
                failure = &mut heartbeat_failure => {
                    drop(execution);
                    cancellation.cancel();
                    let reason = failure
                        .unwrap_or_else(|_| "heartbeat thread stopped unexpectedly".into());
                    let _ = heartbeat
                        .stop(run_id, self.config.cancellation_grace)
                        .await;
                    return Err(DurableWorkerError::LeaseLost {
                        run_id: run_id.to_string(),
                        reason,
                    });
                }
                _ = cancellation.cancelled(), if cancellation_deadline.is_none() => {
                    cancellation_deadline = Some(tokio::time::Instant::now() + self.config.cancellation_grace);
                }
                _ = async {
                    if let Some(deadline) = cancellation_deadline {
                        tokio::time::sleep_until(deadline).await;
                    }
                }, if cancellation_deadline.is_some() => {
                    drop(execution);
                    heartbeat
                        .stop(run_id, self.config.cancellation_grace)
                        .await?;
                    return self.finish_claim(
                        &claim,
                        run_id,
                        Err(DurableWorkerError::Cancelled {
                            run_id: run_id.to_string(),
                        }),
                    );
                }
            }
        }
    }

    /// Apply an operator-provided reconciliation while holding the same distributed lease fence.
    /// Reconciliation never invokes provider or tool work and leaves normal resume policy intact.
    pub async fn reconcile_activity(
        &self,
        run_id: &str,
        cancellation: CancellationToken,
        reconciliation_id: &str,
        activity_id: &str,
        resolution: ActivityReconciliation,
    ) -> Result<RunState, DurableWorkerError> {
        validate_identifier("run_id", run_id)?;
        validate_identifier("reconciliation_id", reconciliation_id)?;
        validate_identifier("activity_id", activity_id)?;
        let claim = self.acquire(run_id, &cancellation).await?;
        if cancellation.is_cancelled() {
            return self.finish_claim(
                &claim,
                run_id,
                Err(DurableWorkerError::Cancelled {
                    run_id: run_id.to_string(),
                }),
            );
        }
        let disposition = match claim.driver.invocation_disposition() {
            Ok(disposition) => disposition,
            Err(error) => return self.finish_claim(&claim, run_id, Err(error.into())),
        };
        if !matches!(
            disposition,
            DurableInvocationDisposition::ReconcileRequired { .. }
        ) {
            let error = claim.terminal_status.map_or_else(
                || DurableWorkerError::ReconciliationUnavailable {
                    run_id: run_id.to_string(),
                },
                |status| DurableWorkerError::TerminalReconciliation {
                    run_id: run_id.to_string(),
                    status,
                },
            );
            return self.finish_claim(&claim, run_id, Err(error));
        }
        let reconciliation = claim
            .driver
            .reconcile_worker_activity(reconciliation_id, activity_id, resolution)
            .map_err(DurableWorkerError::from);
        self.finish_claim(&claim, run_id, reconciliation)?;
        self.store.load(run_id).map_err(DurableWorkerError::from)
    }

    fn finish_claim<T>(
        &self,
        claim: &AcquiredWorker,
        run_id: &str,
        result: Result<T, DurableWorkerError>,
    ) -> Result<T, DurableWorkerError> {
        self.finish_authority(&claim.authority, run_id, result)
    }

    fn finish_authority<T>(
        &self,
        authority: &DurableStoreLeaseAuthority,
        run_id: &str,
        result: Result<T, DurableWorkerError>,
    ) -> Result<T, DurableWorkerError> {
        match (result, self.release_authority(authority, run_id)) {
            (Ok(value), Ok(())) => Ok(value),
            (Ok(_), Err(cleanup)) => Err(cleanup),
            (Err(primary), Ok(())) => Err(primary),
            (Err(primary), Err(cleanup)) => Err(DurableWorkerError::CleanupFailed {
                primary: Box::new(primary),
                cleanup: Box::new(cleanup),
            }),
        }
    }

    fn start_heartbeat(
        &self,
        driver: DurableRunDriver,
        cancellation: CancellationToken,
        run_id: &str,
    ) -> Result<HeartbeatStart, DurableWorkerError> {
        let (stop, stop_receiver) = std::sync::mpsc::channel();
        let (done, done_receiver) = tokio::sync::oneshot::channel();
        let (failure, failure_receiver) = tokio::sync::oneshot::channel();
        let (ready, ready_receiver) = tokio::sync::oneshot::channel();
        let worker = self.clone();
        let run_id = run_id.to_string();
        let thread = std::thread::Builder::new()
            .name("aikit-durable-heartbeat".into())
            .spawn(move || {
                if let Err(error) = worker.ready_heartbeat(&driver) {
                    let _ = ready.send(Err(error.to_string()));
                    let _ = done.send(());
                    return;
                }
                let _ = ready.send(Ok(()));
                loop {
                    match stop_receiver.recv_timeout(worker.config.heartbeat_interval) {
                        Ok(()) | Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                            if let Err(error) = worker.heartbeat(&driver) {
                                cancellation.cancel();
                                let _ = failure.send(error.to_string());
                                break;
                            }
                        }
                    }
                }
                let _ = done.send(());
            })
            .map_err(|error| DurableWorkerError::HeartbeatUnavailable {
                run_id,
                reason: error.to_string(),
            })?;
        Ok((
            HeartbeatThread {
                stop: Some(stop),
                done: Some(done_receiver),
                thread: Some(thread),
            },
            failure_receiver,
            ready_receiver,
        ))
    }

    fn heartbeat(&self, driver: &DurableRunDriver) -> Result<(), DurableWorkerError> {
        let now = self.store.worker_lease_clock_unix_ms()?;
        let expires_at = expires_at(now, self.config.lease_ttl)?;
        let snapshot = driver.snapshot()?;
        let current_expiry = snapshot
            .worker_lease()
            .map(|lease| lease.expires_at_unix_ms)
            .ok_or_else(|| DurableWorkerError::LeaseLost {
                run_id: snapshot.run_id().to_string(),
                reason: "active lease disappeared".into(),
            })?;
        if expires_at <= current_expiry {
            return Err(DurableWorkerError::Clock(
                "durable store clock did not advance across a worker heartbeat".into(),
            ));
        }
        driver.renew_worker_lease(now, expires_at)?;
        Ok(())
    }

    fn ready_heartbeat(&self, driver: &DurableRunDriver) -> Result<(), DurableWorkerError> {
        let wait_bound = self
            .config
            .heartbeat_interval
            .min(Duration::from_millis(50));
        let deadline = std::time::Instant::now() + wait_bound;
        loop {
            match self.heartbeat(driver) {
                Err(DurableWorkerError::Clock(message))
                    if message
                        == "durable store clock did not advance across a worker heartbeat"
                        && std::time::Instant::now() < deadline =>
                {
                    std::thread::sleep(Duration::from_millis(1));
                }
                result => return result,
            }
        }
    }

    /// Release through a freshly loaded driver so a post-claim load/CAS failure cannot strand a
    /// lease merely because the execution driver's fail-closed poison bit is set.
    fn release_authority(
        &self,
        authority: &DurableStoreLeaseAuthority,
        run_id: &str,
    ) -> Result<(), DurableWorkerError> {
        let state = self.store.load(run_id)?;
        let lease = state
            .worker_lease()
            .ok_or_else(|| DurableWorkerError::LeaseLost {
                run_id: run_id.to_string(),
                reason: "active lease disappeared before release".into(),
            })?;
        if lease.owner_id != authority.owner_id() || lease.lease_id != authority.lease_id() {
            return Err(DurableWorkerError::LeaseLost {
                run_id: run_id.to_string(),
                reason: "another worker owns the persisted lease".into(),
            });
        }
        let driver = DurableRunDriver::new(state, self.store.clone())?
            .bind_worker_lease(authority.owner_id(), authority.lease_id())?;
        let now = self.store.worker_lease_clock_unix_ms()?;
        driver
            .release_worker_lease(now)
            .map_err(|error| DurableWorkerError::LeaseLost {
                run_id: run_id.to_string(),
                reason: error.to_string(),
            })
    }

    async fn acquire(
        &self,
        run_id: &str,
        cancellation: &CancellationToken,
    ) -> Result<AcquiredWorker, DurableWorkerError> {
        let lease_id = secure_lease_id()?;
        let mut backoff = self.config.initial_poll_backoff;
        let mut last_holder = None;

        for attempt in 1..=self.config.max_poll_attempts {
            if cancellation.is_cancelled() {
                return Err(DurableWorkerError::Cancelled {
                    run_id: run_id.to_string(),
                });
            }
            let state = self.store.load(run_id)?;
            let terminal_status = state.status().is_terminal().then_some(state.status());
            let driver = match DurableRunDriver::new(state, self.store.clone()) {
                Ok(driver) => Some((driver, terminal_status)),
                Err(DurableRunDriverError::StateMismatch { .. })
                | Err(DurableRunDriverError::Store(DurableStoreError::Conflict { .. })) => None,
                Err(error) => return Err(error.into()),
            };
            if let Some((driver, terminal_status)) = driver {
                let now = self.store.worker_lease_clock_unix_ms()?;
                let expires_at = expires_at(now, self.config.lease_ttl)?;
                match driver.claim_worker_lease(&self.config.owner_id, &lease_id, now, expires_at) {
                    Ok(recovered_claim) => {
                        let authority =
                            DurableStoreLeaseAuthority::new(&self.config.owner_id, &lease_id);
                        let driver =
                            match driver.bind_worker_lease(&self.config.owner_id, &lease_id) {
                                Ok(driver) => driver,
                                Err(error) => {
                                    return self.finish_authority(
                                        &authority,
                                        run_id,
                                        Err(error.into()),
                                    );
                                }
                            };
                        return Ok(AcquiredWorker {
                            driver,
                            authority,
                            recovered_claim,
                            terminal_status,
                        });
                    }
                    Err(DurableRunDriverError::State(DurabilityError::WorkerLeaseHeld {
                        owner_id,
                        expires_at_unix_ms,
                    })) => {
                        last_holder = Some((owner_id, expires_at_unix_ms));
                    }
                    Err(DurableRunDriverError::StateMismatch { .. })
                    | Err(DurableRunDriverError::Store(DurableStoreError::Conflict { .. })) => {
                        // Another process won the same CAS window. Reload before deciding whether
                        // the new claim is active or already expired.
                    }
                    Err(error) => return Err(error.into()),
                }
            }

            if attempt == self.config.max_poll_attempts {
                break;
            }
            tokio::select! {
                _ = cancellation.cancelled() => {
                    return Err(DurableWorkerError::Cancelled { run_id: run_id.to_string() });
                }
                _ = tokio::time::sleep(backoff) => {}
            }
            backoff = backoff.saturating_mul(2).min(self.config.max_poll_backoff);
        }

        let (owner_id, expires_at_unix_ms) = last_holder
            .map(|(owner, expiry)| (Some(owner), Some(expiry)))
            .unwrap_or((None, None));
        Err(DurableWorkerError::ClaimUnavailable {
            run_id: run_id.to_string(),
            attempts: self.config.max_poll_attempts,
            owner_id,
            expires_at_unix_ms,
        })
    }
}

struct HeartbeatThread {
    stop: Option<std::sync::mpsc::Sender<()>>,
    done: Option<tokio::sync::oneshot::Receiver<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

type HeartbeatStart = (
    HeartbeatThread,
    tokio::sync::oneshot::Receiver<String>,
    tokio::sync::oneshot::Receiver<Result<(), String>>,
);

impl HeartbeatThread {
    async fn stop(&mut self, run_id: &str, timeout: Duration) -> Result<(), DurableWorkerError> {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        let acknowledgement = tokio::time::timeout(
            timeout,
            self.done
                .take()
                .ok_or_else(|| DurableWorkerError::LeaseLost {
                    run_id: run_id.to_string(),
                    reason: "heartbeat shutdown acknowledgement was already consumed".into(),
                })?,
        )
        .await;
        // Dropping a JoinHandle detaches it. This is intentional even after acknowledgement so
        // shutdown remains bounded if the OS delays the thread's final return. On timeout the
        // lease is deliberately retained because an in-flight heartbeat may still renew it.
        drop(self.thread.take());
        match acknowledgement {
            Ok(Ok(())) => Ok(()),
            Err(_) => {
                Err(DurableWorkerError::LeaseLost {
                    run_id: run_id.to_string(),
                    reason: format!(
                        "heartbeat did not acknowledge shutdown within {}ms; lease retained for fenced recovery",
                        timeout.as_millis()
                    ),
                })
            }
            Ok(Err(_)) => {
                Err(DurableWorkerError::LeaseLost {
                    run_id: run_id.to_string(),
                    reason: "heartbeat thread stopped without a shutdown acknowledgement".into(),
                })
            }
        }
    }
}

impl Drop for HeartbeatThread {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }
}

struct AcquiredWorker {
    driver: DurableRunDriver,
    authority: DurableStoreLeaseAuthority,
    recovered_claim: bool,
    terminal_status: Option<DurableRunStatus>,
}

fn reconciliation_activity_ids(state: &RunState) -> Vec<String> {
    state
        .projection()
        .activities
        .values()
        .filter(|record| {
            record
                .latest_attempt()
                .is_some_and(|attempt| attempt.status == ActivityAttemptStatus::ReconcileRequired)
        })
        .map(|record| record.definition.activity_id.clone())
        .collect()
}

fn secure_lease_id() -> Result<String, DurableWorkerError> {
    let mut random = [0_u8; 16];
    getrandom::fill(&mut random).map_err(|error| DurableWorkerError::Entropy(error.to_string()))?;
    let mut encoded = String::with_capacity(random.len() * 2);
    for byte in random {
        use std::fmt::Write as _;
        write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    Ok(format!("worker-lease-{encoded}"))
}

fn expires_at(now: u64, ttl: Duration) -> Result<u64, DurableWorkerError> {
    let ttl_ms = duration_ms("lease_ttl", ttl)?;
    now.checked_add(ttl_ms)
        .ok_or_else(|| DurableWorkerError::Clock("lease expiry overflowed u64".into()))
}

fn duration_ms(field: &str, duration: Duration) -> Result<u64, DurableWorkerError> {
    if duration.is_zero() || duration.as_millis() == 0 {
        return Err(DurableWorkerError::InvalidConfiguration(format!(
            "{field} must be positive"
        )));
    }
    duration.as_millis().try_into().map_err(|_| {
        DurableWorkerError::InvalidConfiguration(format!("{field} exceeds u64 milliseconds"))
    })
}

fn validate_identifier(field: &str, value: &str) -> Result<(), DurableWorkerError> {
    if value.is_empty() || value.chars().any(char::is_control) {
        return Err(DurableWorkerError::InvalidConfiguration(format!(
            "{field} cannot be empty or contain control characters"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::durability::{DurabilityMode, DurableWorkerLease, RunEventKind, SideEffectClass};
    use crate::durable_runtime::{
        DurableActivity, DurableLegacyRunStoppedResolutionEnvelope, DurableRunStoppedReceipt,
        LEGACY_RUN_STOPPED_RESOLUTION_KIND, LEGACY_RUN_STOPPED_RESOLUTION_SCHEMA_VERSION,
    };
    use crate::durable_store::{
        system_worker_lease_clock_unix_ms, validate_append_only, validate_worker_lease_fence,
        InMemoryDurableStore,
    };
    use serde_json::json;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Condvar, Mutex};
    use tokio::sync::Notify;

    struct ConstructorRaceStore {
        state: std::sync::Mutex<RunState>,
        loads: AtomicUsize,
    }

    impl ConstructorRaceStore {
        fn new(state: RunState) -> Self {
            Self {
                state: std::sync::Mutex::new(state),
                loads: AtomicUsize::new(0),
            }
        }
    }

    struct FailNthLoadStore {
        inner: InMemoryDurableStore,
        loads: AtomicUsize,
        fail_on_load: usize,
    }

    impl FailNthLoadStore {
        fn new(state: &RunState, fail_on_load: usize) -> Self {
            let inner = InMemoryDurableStore::default();
            inner.create(state).unwrap();
            Self {
                inner,
                loads: AtomicUsize::new(0),
                fail_on_load,
            }
        }
    }

    impl DurableStore for FailNthLoadStore {
        fn create(&self, state: &RunState) -> Result<(), DurableStoreError> {
            self.inner.create(state)
        }

        fn load(&self, run_id: &str) -> Result<RunState, DurableStoreError> {
            let load_number = self.loads.fetch_add(1, Ordering::SeqCst) + 1;
            if load_number == self.fail_on_load {
                return Err(DurableStoreError::Io(
                    "injected post-claim load failure".into(),
                ));
            }
            self.inner.load(run_id)
        }

        fn worker_lease_clock_unix_ms(&self) -> Result<u64, DurableStoreError> {
            self.inner.worker_lease_clock_unix_ms()
        }

        fn compare_and_swap(
            &self,
            expected_sequence: u64,
            replacement: &RunState,
        ) -> Result<(), DurableStoreError> {
            self.inner.compare_and_swap(expected_sequence, replacement)
        }

        fn compare_and_swap_fenced(
            &self,
            expected_sequence: u64,
            replacement: &RunState,
            authority: &DurableStoreLeaseAuthority,
        ) -> Result<(), DurableStoreError> {
            self.inner
                .compare_and_swap_fenced(expected_sequence, replacement, authority)
        }
    }

    struct SkewedClockStore {
        inner: InMemoryDurableStore,
        clock_offset_ms: u64,
        clock_calls: AtomicUsize,
    }

    impl SkewedClockStore {
        fn new(state: &RunState, clock_offset_ms: u64) -> Self {
            let inner = InMemoryDurableStore::default();
            inner.create(state).unwrap();
            Self {
                inner,
                clock_offset_ms,
                clock_calls: AtomicUsize::new(0),
            }
        }
    }

    impl DurableStore for SkewedClockStore {
        fn create(&self, state: &RunState) -> Result<(), DurableStoreError> {
            self.inner.create(state)
        }

        fn load(&self, run_id: &str) -> Result<RunState, DurableStoreError> {
            self.inner.load(run_id)
        }

        fn worker_lease_clock_unix_ms(&self) -> Result<u64, DurableStoreError> {
            self.clock_calls.fetch_add(1, Ordering::SeqCst);
            system_worker_lease_clock_unix_ms()?
                .checked_sub(self.clock_offset_ms)
                .ok_or_else(|| DurableStoreError::Io("test clock offset underflowed".into()))
        }

        fn compare_and_swap(
            &self,
            expected_sequence: u64,
            replacement: &RunState,
        ) -> Result<(), DurableStoreError> {
            self.inner.compare_and_swap(expected_sequence, replacement)
        }

        fn compare_and_swap_fenced(
            &self,
            expected_sequence: u64,
            replacement: &RunState,
            authority: &DurableStoreLeaseAuthority,
        ) -> Result<(), DurableStoreError> {
            self.inner
                .compare_and_swap_fenced(expected_sequence, replacement, authority)
        }
    }

    struct BlockingHeartbeatStore {
        inner: InMemoryDurableStore,
        heartbeat_blocked: Arc<Notify>,
        heartbeat_gate: Arc<(Mutex<bool>, Condvar)>,
        heartbeat_calls: AtomicUsize,
        block_on_heartbeat: usize,
    }

    impl BlockingHeartbeatStore {
        fn new(state: &RunState) -> Self {
            Self::blocking_on(state, 2)
        }

        fn blocking_initial(state: &RunState) -> Self {
            Self::blocking_on(state, 1)
        }

        fn blocking_on(state: &RunState, block_on_heartbeat: usize) -> Self {
            let inner = InMemoryDurableStore::default();
            inner.create(state).unwrap();
            Self {
                inner,
                heartbeat_blocked: Arc::new(Notify::new()),
                heartbeat_gate: Arc::new((Mutex::new(false), Condvar::new())),
                heartbeat_calls: AtomicUsize::new(0),
                block_on_heartbeat,
            }
        }

        fn unblock_heartbeat(&self) {
            let (unblocked, wake) = &*self.heartbeat_gate;
            *unblocked.lock().unwrap() = true;
            wake.notify_all();
        }
    }

    impl DurableStore for BlockingHeartbeatStore {
        fn create(&self, state: &RunState) -> Result<(), DurableStoreError> {
            self.inner.create(state)
        }

        fn load(&self, run_id: &str) -> Result<RunState, DurableStoreError> {
            self.inner.load(run_id)
        }

        fn worker_lease_clock_unix_ms(&self) -> Result<u64, DurableStoreError> {
            self.inner.worker_lease_clock_unix_ms()
        }

        fn compare_and_swap(
            &self,
            expected_sequence: u64,
            replacement: &RunState,
        ) -> Result<(), DurableStoreError> {
            self.inner.compare_and_swap(expected_sequence, replacement)
        }

        fn compare_and_swap_fenced(
            &self,
            expected_sequence: u64,
            replacement: &RunState,
            authority: &DurableStoreLeaseAuthority,
        ) -> Result<(), DurableStoreError> {
            let is_heartbeat = replacement.events().last().is_some_and(|event| {
                matches!(&event.kind, RunEventKind::WorkerLeaseRenewed { .. })
            });
            let heartbeat_number =
                is_heartbeat.then(|| self.heartbeat_calls.fetch_add(1, Ordering::SeqCst) + 1);
            if heartbeat_number == Some(self.block_on_heartbeat) {
                self.heartbeat_blocked.notify_one();
                let (unblocked, wake) = &*self.heartbeat_gate;
                let unblocked = unblocked.lock().unwrap();
                let _ = wake
                    .wait_timeout_while(unblocked, Duration::from_secs(2), |value| !*value)
                    .unwrap();
            }
            self.inner
                .compare_and_swap_fenced(expected_sequence, replacement, authority)
        }
    }

    struct BlockingDropFuture {
        started: Arc<Notify>,
        polled: bool,
        drop_started: Option<tokio::sync::oneshot::Sender<()>>,
        drop_gate: Arc<(Mutex<bool>, Condvar)>,
        drop_finished: Arc<AtomicBool>,
    }

    impl Future for BlockingDropFuture {
        type Output = ();

        fn poll(
            mut self: std::pin::Pin<&mut Self>,
            _context: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Self::Output> {
            if !self.polled {
                self.polled = true;
                self.started.notify_one();
            }
            std::task::Poll::Pending
        }
    }

    impl Drop for BlockingDropFuture {
        fn drop(&mut self) {
            if let Some(drop_started) = self.drop_started.take() {
                let _ = drop_started.send(());
            }
            let (released, wake) = &*self.drop_gate;
            let released = released.lock().unwrap();
            let _ = wake
                .wait_timeout_while(released, Duration::from_secs(2), |released| !*released)
                .unwrap();
            self.drop_finished.store(true, Ordering::SeqCst);
        }
    }

    async fn wait_for_notification(notify: &Notify, context: &str) {
        tokio::time::timeout(Duration::from_secs(2), notify.notified())
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for {context}"));
    }

    async fn wait_for_fresh_worker_lease(
        store: &dyn DurableStore,
        run_id: &str,
        heartbeat_after_unix_ms: u64,
        not_before_unix_ms: u64,
        minimum_headroom: Duration,
        context: &str,
    ) -> (DurableWorkerLease, u64) {
        let minimum_headroom_ms = u64::try_from(minimum_headroom.as_millis())
            .expect("test lease headroom fits in u64 milliseconds");
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let state = store.load(run_id).expect("load worker lease while polling");
                let now = store
                    .worker_lease_clock_unix_ms()
                    .expect("read worker lease clock while polling");
                if let Some(lease) = state.worker_lease() {
                    if now >= not_before_unix_ms
                        && lease.heartbeat_at_unix_ms > heartbeat_after_unix_ms
                        && lease.expires_at_unix_ms.saturating_sub(now) >= minimum_headroom_ms
                    {
                        return (lease.clone(), now);
                    }
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            let lease = store
                .load(run_id)
                .ok()
                .and_then(|state| state.worker_lease().cloned());
            let now = store.worker_lease_clock_unix_ms().ok();
            panic!(
                "timed out waiting for {context}; latest lease: {lease:?}, store clock: {now:?}"
            );
        })
    }

    impl DurableStore for ConstructorRaceStore {
        fn create(&self, state: &RunState) -> Result<(), DurableStoreError> {
            let current = self.state.lock().unwrap();
            if current.run_id() == state.run_id() {
                return Err(DurableStoreError::AlreadyExists {
                    run_id: state.run_id().to_string(),
                });
            }
            Err(DurableStoreError::Invalid(
                "constructor-race store owns exactly one run".into(),
            ))
        }

        fn load(&self, run_id: &str) -> Result<RunState, DurableStoreError> {
            let mut state = self.state.lock().unwrap();
            if state.run_id() != run_id {
                return Err(DurableStoreError::NotFound {
                    run_id: run_id.to_string(),
                });
            }
            if self.loads.fetch_add(1, Ordering::SeqCst) == 1 {
                state
                    .replace_state("constructor-race", json!({"revision": 1}))
                    .unwrap();
            }
            Ok(state.clone())
        }

        fn compare_and_swap(
            &self,
            expected_sequence: u64,
            replacement: &RunState,
        ) -> Result<(), DurableStoreError> {
            self.compare_and_swap_inner(expected_sequence, replacement, None)
        }

        fn worker_lease_clock_unix_ms(&self) -> Result<u64, DurableStoreError> {
            system_worker_lease_clock_unix_ms()
        }

        fn compare_and_swap_fenced(
            &self,
            expected_sequence: u64,
            replacement: &RunState,
            authority: &DurableStoreLeaseAuthority,
        ) -> Result<(), DurableStoreError> {
            self.compare_and_swap_inner(expected_sequence, replacement, Some(authority))
        }
    }

    impl ConstructorRaceStore {
        fn compare_and_swap_inner(
            &self,
            expected_sequence: u64,
            replacement: &RunState,
            authority: Option<&DurableStoreLeaseAuthority>,
        ) -> Result<(), DurableStoreError> {
            let mut current = self.state.lock().unwrap();
            let actual = current.events().last().map_or(0, |event| event.sequence);
            if actual != expected_sequence {
                return Err(DurableStoreError::Conflict {
                    run_id: replacement.run_id().to_string(),
                    expected: expected_sequence,
                    actual,
                });
            }
            validate_worker_lease_fence(
                &current,
                replacement,
                authority,
                system_worker_lease_clock_unix_ms()?,
            )?;
            validate_append_only(&current, replacement)?;
            *current = replacement.clone();
            Ok(())
        }
    }

    fn test_config(owner_id: &str) -> DurableWorkerConfig {
        DurableWorkerConfig {
            owner_id: owner_id.to_string(),
            lease_ttl: Duration::from_millis(180),
            heartbeat_interval: Duration::from_millis(30),
            initial_poll_backoff: Duration::from_millis(5),
            max_poll_backoff: Duration::from_millis(10),
            max_poll_attempts: 3,
            cancellation_grace: Duration::from_millis(40),
        }
    }

    fn seeded_store(run_id: &str) -> Arc<InMemoryDurableStore> {
        let store = Arc::new(InMemoryDurableStore::default());
        store
            .create(
                &RunState::new("durable-worker-session", run_id, DurabilityMode::Sync)
                    .expect("valid durable run"),
            )
            .expect("seed durable store");
        store
    }

    fn terminal_v1_zero_attempt_state(run_id: &str) -> (RunState, String) {
        let legacy_activity_id = crate::durability::stable_id(
            "activity",
            &[
                run_id,
                "root",
                crate::durability::RUNTIME_LEGACY_RUN_STOPPED_AUDIT_STEP_ID,
                "terminal",
            ],
        );
        let input = json!({"input_hash": "legacy"});
        let definition = crate::durability::ActivityDefinition {
            activity_id: legacy_activity_id.clone(),
            stable_step_id: crate::durability::RUNTIME_LEGACY_RUN_STOPPED_AUDIT_STEP_ID.into(),
            logical_key: "terminal".into(),
            input: input.clone(),
            input_hash: crate::durability::stable_input_hash(&input),
            side_effect_class: SideEffectClass::ReconcileRequired,
            idempotency_key: None,
        };
        let state = RunState::from_events(vec![
            crate::durability::RunEvent {
                schema_version: 1,
                run_id: run_id.into(),
                sequence: 1,
                event_id: "legacy-start".into(),
                kind: RunEventKind::RunStarted {
                    session_id: "terminal-worker-session".into(),
                    durability: DurabilityMode::Sync,
                    root_branch_id: "root".into(),
                },
            },
            crate::durability::RunEvent {
                schema_version: 1,
                run_id: run_id.into(),
                sequence: 2,
                event_id: "legacy-scheduled".into(),
                kind: RunEventKind::ActivityScheduled { definition },
            },
            crate::durability::RunEvent {
                schema_version: 1,
                run_id: run_id.into(),
                sequence: 3,
                event_id: "legacy-terminal".into(),
                kind: RunEventKind::RunCompleted,
            },
        ])
        .unwrap();
        (state, legacy_activity_id)
    }

    fn terminal_v1_unsafe_activity_state(
        run_id: &str,
        status: DurableRunStatus,
        reconcile_required: bool,
    ) -> (RunState, String) {
        let activity_id = crate::durability::stable_id(
            "activity",
            &[run_id, "root", "legacy-external-write-v1", "write-1"],
        );
        let input = json!({"value": 1});
        let definition = crate::durability::ActivityDefinition {
            activity_id: activity_id.clone(),
            stable_step_id: "legacy-external-write-v1".into(),
            logical_key: "write-1".into(),
            input: input.clone(),
            input_hash: crate::durability::stable_input_hash(&input),
            side_effect_class: SideEffectClass::ReconcileRequired,
            idempotency_key: None,
        };
        let mut events = vec![
            crate::durability::RunEvent {
                schema_version: 1,
                run_id: run_id.into(),
                sequence: 1,
                event_id: format!("{run_id}-start"),
                kind: RunEventKind::RunStarted {
                    session_id: "terminal-migration-session".into(),
                    durability: DurabilityMode::Sync,
                    root_branch_id: "root".into(),
                },
            },
            crate::durability::RunEvent {
                schema_version: 1,
                run_id: run_id.into(),
                sequence: 2,
                event_id: format!("{run_id}-schedule"),
                kind: RunEventKind::ActivityScheduled { definition },
            },
            crate::durability::RunEvent {
                schema_version: 1,
                run_id: run_id.into(),
                sequence: 3,
                event_id: format!("{run_id}-attempt"),
                kind: RunEventKind::ActivityAttemptStarted {
                    activity_id: activity_id.clone(),
                    attempt: 1,
                },
            },
        ];
        if reconcile_required {
            events.push(crate::durability::RunEvent {
                schema_version: 1,
                run_id: run_id.into(),
                sequence: 4,
                event_id: format!("{run_id}-reconcile-required"),
                kind: RunEventKind::ActivityReconciliationRequired {
                    activity_id: activity_id.clone(),
                    attempt: 1,
                    reason: "legacy outcome is unknown".into(),
                },
            });
        }
        let sequence = events.len() as u64 + 1;
        let terminal = match status {
            DurableRunStatus::Failed => RunEventKind::RunFailed {
                error: "legacy failure".into(),
            },
            DurableRunStatus::Cancelled => RunEventKind::RunCancelled {
                reason: Some("legacy cancellation".into()),
            },
            other => panic!("unsupported terminal migration status: {other:?}"),
        };
        events.push(crate::durability::RunEvent {
            schema_version: 1,
            run_id: run_id.into(),
            sequence,
            event_id: format!("{run_id}-terminal"),
            kind: terminal,
        });
        (RunState::from_events(events).unwrap(), activity_id)
    }

    #[test]
    fn configuration_rejects_sub_millisecond_and_unbounded_leases() {
        let mut config = test_config("worker-a");
        config.heartbeat_interval = Duration::from_nanos(1);
        assert!(matches!(
            config.validate(),
            Err(DurableWorkerError::InvalidConfiguration(_))
        ));

        config = test_config("worker-a");
        config.lease_ttl = Duration::from_millis(MAX_DURABLE_WORKER_LEASE_MS + 1);
        assert!(matches!(
            config.validate(),
            Err(DurableWorkerError::InvalidConfiguration(_))
        ));
    }

    #[test]
    fn lease_events_are_owner_bound_expiring_and_replayable() {
        let mut state = RunState::new(
            "lease-event-session",
            "lease-event-run",
            DurabilityMode::Sync,
        )
        .unwrap();
        assert!(!state
            .claim_worker_lease("worker-a", "lease-a", 1_000, 1_100)
            .unwrap());
        assert!(matches!(
            state.claim_worker_lease("worker-b", "lease-b", 1_050, 1_150),
            Err(DurabilityError::WorkerLeaseHeld {
                ref owner_id,
                expires_at_unix_ms: 1_100,
            }) if owner_id == "worker-a"
        ));
        assert!(matches!(
            state.renew_worker_lease("worker-b", "lease-b", 1_050, 1_150),
            Err(DurabilityError::WorkerLeaseLost { ref owner_id }) if owner_id == "worker-b"
        ));

        assert!(state
            .claim_worker_lease("worker-b", "lease-b", 1_100, 1_200)
            .unwrap());
        assert!(matches!(
            state.release_worker_lease("worker-a", "lease-a", 1_150),
            Err(DurabilityError::WorkerLeaseLost { ref owner_id }) if owner_id == "worker-a"
        ));
        state
            .renew_worker_lease("worker-b", "lease-b", 1_150, 1_250)
            .unwrap();
        assert!(matches!(
            state.release_worker_lease("worker-b", "lease-b", 1_250),
            Err(DurabilityError::WorkerLeaseLost { ref owner_id }) if owner_id == "worker-b"
        ));
        state
            .release_worker_lease("worker-b", "lease-b", 1_200)
            .unwrap();
        assert!(state.worker_lease().is_none());
        assert_eq!(
            RunState::from_events(state.events().to_vec()).unwrap(),
            state
        );
    }

    #[test]
    fn unbound_driver_cannot_mutate_a_worker_claimed_run() {
        let run_id = "owner-bound-driver-run";
        let store = seeded_store(run_id);
        let owner = DurableRunDriver::new(store.load(run_id).unwrap(), store.clone()).unwrap();
        let now = store.worker_lease_clock_unix_ms().unwrap();
        owner
            .claim_worker_lease("worker-a", "lease-a", now, now + 1_000)
            .unwrap();
        let owner = owner.bind_worker_lease("worker-a", "lease-a").unwrap();
        let unbound = DurableRunDriver::new(store.load(run_id).unwrap(), store.clone()).unwrap();

        assert!(matches!(
            unbound.begin_activity(
                "unbound-step",
                "unbound-logical-key",
                json!({}),
                SideEffectClass::Pure,
                None,
            ),
            Err(DurableRunDriverError::WorkerLeaseRequired { ref owner_id })
                if owner_id == "worker-a"
        ));
        assert!(store
            .load(run_id)
            .unwrap()
            .projection()
            .activities
            .is_empty());
        owner
            .release_worker_lease(store.worker_lease_clock_unix_ms().unwrap())
            .unwrap();
    }

    #[tokio::test]
    async fn acquire_reloads_when_the_revision_changes_during_driver_construction() {
        let run_id = "constructor-race-run";
        let store = Arc::new(ConstructorRaceStore::new(
            RunState::new("constructor-race-session", run_id, DurabilityMode::Sync).unwrap(),
        ));
        let mut config = test_config("worker-a");
        config.max_poll_attempts = 2;
        let worker = DurableWorker::new(store.clone(), config).unwrap();

        let outcome = worker
            .run(run_id, CancellationToken::new(), |_, _| async {
                "executed"
            })
            .await
            .unwrap();
        assert_eq!(
            outcome,
            DurableWorkerOutcome::Executed {
                value: "executed",
                recovered_claim: false,
            }
        );
        assert_eq!(
            store.load(run_id).unwrap().projection().state,
            json!({"revision": 1})
        );
    }

    #[tokio::test]
    async fn lease_timestamps_come_from_the_store_clock() {
        let run_id = "store-clock-run";
        let state = RunState::new("store-clock-session", run_id, DurabilityMode::Sync).unwrap();
        let store = Arc::new(SkewedClockStore::new(&state, 60_000));
        let local_before = system_worker_lease_clock_unix_ms().unwrap();
        let mut config = test_config("worker-a");
        config.lease_ttl = Duration::from_secs(120);
        config.heartbeat_interval = Duration::from_secs(30);
        let worker = DurableWorker::new(store.clone(), config).unwrap();

        assert!(matches!(
            worker
                .run(run_id, CancellationToken::new(), |_, _| async {})
                .await
                .unwrap(),
            DurableWorkerOutcome::Executed { .. }
        ));

        let persisted = store.load(run_id).unwrap();
        let (claimed_at, expires_at) = persisted
            .events()
            .iter()
            .find_map(|event| match &event.kind {
                RunEventKind::WorkerLeaseClaimed {
                    claimed_at_unix_ms,
                    expires_at_unix_ms,
                    ..
                } => Some((*claimed_at_unix_ms, *expires_at_unix_ms)),
                _ => None,
            })
            .expect("worker claim is durable");
        assert!(claimed_at <= local_before.saturating_sub(59_000));
        assert_eq!(expires_at - claimed_at, 120_000);
        assert!(store.clock_calls.load(Ordering::SeqCst) >= 4);
        assert!(persisted.worker_lease().is_none());
    }

    #[tokio::test]
    async fn poisoned_execution_driver_does_not_strand_the_lease() {
        let run_id = "poisoned-driver-cleanup-run";
        let state = RunState::new("poisoned-driver-session", run_id, DurabilityMode::Sync).unwrap();
        // Acquisition performs three loads; the fourth is the first post-claim disposition load.
        let store = Arc::new(FailNthLoadStore::new(&state, 4));
        let worker = DurableWorker::new(store.clone(), test_config("worker-a")).unwrap();
        let executions = Arc::new(AtomicUsize::new(0));

        let result = worker
            .run(run_id, CancellationToken::new(), {
                let executions = executions.clone();
                move |_, _| async move {
                    executions.fetch_add(1, Ordering::SeqCst);
                }
            })
            .await;

        assert!(matches!(
            result,
            Err(DurableWorkerError::Driver(DurableRunDriverError::Store(
                DurableStoreError::Io(ref message)
            ))) if message == "injected post-claim load failure"
        ));
        assert_eq!(executions.load(Ordering::SeqCst), 0);
        assert!(store.load(run_id).unwrap().worker_lease().is_none());
    }

    #[tokio::test]
    async fn terminal_legacy_audit_is_classified_and_reconciled_without_callback_execution() {
        let run_id = "terminal-legacy-worker-run";
        let (state, activity_id) = terminal_v1_zero_attempt_state(run_id);
        let store = Arc::new(InMemoryDurableStore::default());
        store.create(&state).unwrap();
        let worker = DurableWorker::new(store.clone(), test_config("worker-a")).unwrap();
        let executions = Arc::new(AtomicUsize::new(0));

        let outcome = worker
            .run(run_id, CancellationToken::new(), {
                let executions = executions.clone();
                move |_, _| async move {
                    executions.fetch_add(1, Ordering::SeqCst);
                }
            })
            .await
            .unwrap();
        assert!(matches!(
            outcome,
            DurableWorkerOutcome::ReconcileRequired {
                ref activity_ids,
                ..
            } if activity_ids == std::slice::from_ref(&activity_id)
        ));
        assert_eq!(executions.load(Ordering::SeqCst), 0);

        let quarantined = store.load(run_id).unwrap();
        let attempt = quarantined
            .activity(&activity_id)
            .unwrap()
            .latest_attempt()
            .unwrap();
        let output = serde_json::to_value(DurableLegacyRunStoppedResolutionEnvelope {
            schema_version: LEGACY_RUN_STOPPED_RESOLUTION_SCHEMA_VERSION,
            kind: LEGACY_RUN_STOPPED_RESOLUTION_KIND.into(),
            source_activity_id: activity_id.clone(),
            source_attempt: attempt.attempt,
            source_started_sequence: attempt.started_sequence,
            source_output_hash: None,
            terminal_receipt: DurableRunStoppedReceipt {
                turns: 0,
                reason: "end_turn".into(),
                usage: crate::types::Usage::default(),
            },
        })
        .unwrap();
        let reconciled = worker
            .reconcile_activity(
                run_id,
                CancellationToken::new(),
                "terminal-legacy-attestation",
                &activity_id,
                ActivityReconciliation::Completed { output },
            )
            .await
            .unwrap();
        assert_eq!(reconciled.status(), DurableRunStatus::Completed);
        assert!(reconciled.worker_lease().is_none());
    }

    #[tokio::test]
    async fn migrated_terminal_unsafe_activities_require_reconciliation_without_execution() {
        let cases = [
            (
                "terminal-v1-failed-running-worker",
                DurableRunStatus::Failed,
                false,
            ),
            (
                "terminal-v1-cancelled-running-worker",
                DurableRunStatus::Cancelled,
                false,
            ),
            (
                "terminal-v1-failed-already-reconciling-worker",
                DurableRunStatus::Failed,
                true,
            ),
        ];

        for (run_id, terminal_status, already_reconciling) in cases {
            let (state, activity_id) =
                terminal_v1_unsafe_activity_state(run_id, terminal_status, already_reconciling);
            let store = Arc::new(InMemoryDurableStore::default());
            store.create(&state).unwrap();
            let worker =
                DurableWorker::new(store.clone(), test_config("migration-worker")).unwrap();
            let executions = Arc::new(AtomicUsize::new(0));

            let outcome = worker
                .run(run_id, CancellationToken::new(), {
                    let executions = executions.clone();
                    move |_, _| async move {
                        executions.fetch_add(1, Ordering::SeqCst);
                    }
                })
                .await
                .unwrap();
            assert!(matches!(
                outcome,
                DurableWorkerOutcome::ReconcileRequired {
                    ref activity_ids,
                    ..
                } if activity_ids == std::slice::from_ref(&activity_id)
            ));
            assert_eq!(executions.load(Ordering::SeqCst), 0);

            let quarantined = store.load(run_id).unwrap();
            assert_eq!(quarantined.status(), terminal_status);
            assert_eq!(
                quarantined
                    .activity(&activity_id)
                    .unwrap()
                    .latest_attempt()
                    .unwrap()
                    .status,
                ActivityAttemptStatus::ReconcileRequired
            );
            assert!(quarantined.worker_lease().is_none());

            let reconciled = worker
                .reconcile_activity(
                    run_id,
                    CancellationToken::new(),
                    "terminal-v1-operator-resolution",
                    &activity_id,
                    ActivityReconciliation::Completed {
                        output: json!({"effect": "confirmed"}),
                    },
                )
                .await
                .unwrap();
            assert_eq!(reconciled.status(), terminal_status);
            assert_eq!(
                reconciled
                    .activity(&activity_id)
                    .unwrap()
                    .completed_output(),
                Some(&json!({"effect": "confirmed"}))
            );
            assert!(reconciled.worker_lease().is_none());
        }
    }

    #[tokio::test]
    async fn expired_terminal_ghost_lease_is_recovered_and_released_without_callback() {
        let run_id = "terminal-ghost-lease-run";
        let mut state =
            RunState::new("terminal-ghost-session", run_id, DurabilityMode::Sync).unwrap();
        state
            .claim_worker_lease("crashed-worker", "expired-lease", 1, 2)
            .unwrap();
        state.complete_run("terminal-before-crash").unwrap();
        let store = Arc::new(InMemoryDurableStore::default());
        store.create(&state).unwrap();
        let worker = DurableWorker::new(store.clone(), test_config("recovery-worker")).unwrap();
        let executions = Arc::new(AtomicUsize::new(0));

        let outcome = worker
            .run(run_id, CancellationToken::new(), {
                let executions = executions.clone();
                move |_, _| async move {
                    executions.fetch_add(1, Ordering::SeqCst);
                }
            })
            .await
            .unwrap();
        assert!(matches!(
            outcome,
            DurableWorkerOutcome::Terminal {
                status: DurableRunStatus::Completed,
                ..
            }
        ));
        assert_eq!(executions.load(Ordering::SeqCst), 0);

        let persisted = store.load(run_id).unwrap();
        assert!(persisted.worker_lease().is_none());
        assert_eq!(
            persisted
                .events()
                .iter()
                .filter(|event| matches!(&event.kind, RunEventKind::WorkerLeaseClaimed { .. }))
                .count(),
            2
        );
        assert!(persisted
            .events()
            .iter()
            .any(|event| matches!(&event.kind, RunEventKind::WorkerLeaseReleased { .. })));
    }

    #[tokio::test]
    async fn contradictory_terminal_receipt_fails_closed_without_callback_execution() {
        let run_id = "contradictory-terminal-receipt-run";
        let store = Arc::new(InMemoryDurableStore::default());
        let driver = DurableRunDriver::new(
            RunState::new(
                "contradictory-terminal-session",
                run_id,
                DurabilityMode::Sync,
            )
            .unwrap(),
            store.clone(),
        )
        .unwrap();
        let receipt = DurableRunStoppedReceipt {
            turns: 1,
            reason: "end_turn".into(),
            usage: crate::types::Usage::default(),
        };
        let DurableActivity::Execute {
            activity_id,
            attempt,
            ..
        } = driver.begin_run_stopped_audit(&receipt).unwrap()
        else {
            panic!("canonical terminal audit must start")
        };
        driver
            .complete_run_stopped_audit(&activity_id, attempt, receipt)
            .unwrap();
        let mut contradictory = store.load(run_id).unwrap();
        let expected_sequence = contradictory.events().last().unwrap().sequence;
        contradictory
            .fail_run(
                "contradictory-terminal-status",
                "failed after completed receipt",
            )
            .unwrap();
        store
            .compare_and_swap(expected_sequence, &contradictory)
            .unwrap();

        let worker = DurableWorker::new(store.clone(), test_config("worker-a")).unwrap();
        let executions = Arc::new(AtomicUsize::new(0));
        let result = worker
            .run(run_id, CancellationToken::new(), {
                let executions = executions.clone();
                move |_, _| async move {
                    executions.fetch_add(1, Ordering::SeqCst);
                }
            })
            .await;
        assert!(matches!(
            result,
            Err(DurableWorkerError::Driver(DurableRunDriverError::State(
                DurabilityError::InvalidEvent { .. }
            )))
        ));
        assert_eq!(executions.load(Ordering::SeqCst), 0);
        assert!(store.load(run_id).unwrap().worker_lease().is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn heartbeat_renews_the_owner_bound_lease_and_excludes_a_contender() {
        let run_id = "heartbeat-owner-run";
        let store = seeded_store(run_id);
        let first_config = test_config("worker-a");
        let minimum_headroom = first_config.lease_ttl / 2;
        let first = DurableWorker::new(store.clone(), first_config).unwrap();
        let started = Arc::new(Notify::new());
        let finish = Arc::new(Notify::new());
        let task = tokio::spawn({
            let started = started.clone();
            let finish = finish.clone();
            async move {
                first
                    .run(run_id, CancellationToken::new(), move |_, _| async move {
                        started.notify_one();
                        finish.notified().await;
                        "worker-a-result"
                    })
                    .await
            }
        });

        wait_for_notification(&started, "first worker callback").await;
        let initial_lease = store
            .load(run_id)
            .unwrap()
            .worker_lease()
            .expect("worker owns a lease")
            .clone();
        let (renewed_lease, observed_at_unix_ms) = wait_for_fresh_worker_lease(
            store.as_ref(),
            run_id,
            initial_lease.heartbeat_at_unix_ms,
            initial_lease.expires_at_unix_ms,
            minimum_headroom,
            "a renewal beyond the initial lease window",
        )
        .await;
        assert!(observed_at_unix_ms >= initial_lease.expires_at_unix_ms);
        assert_eq!(renewed_lease.owner_id, initial_lease.owner_id);
        assert_eq!(renewed_lease.lease_id, initial_lease.lease_id);
        assert!(renewed_lease.expires_at_unix_ms > initial_lease.expires_at_unix_ms);
        assert!(renewed_lease.heartbeat_at_unix_ms > initial_lease.heartbeat_at_unix_ms);

        let mut contender_config = test_config("worker-b");
        contender_config.max_poll_attempts = 1;
        let contender = DurableWorker::new(store.clone(), contender_config).unwrap();
        let executions = Arc::new(AtomicUsize::new(0));
        let result = contender
            .run(run_id, CancellationToken::new(), {
                let executions = executions.clone();
                move |_, _| async move {
                    executions.fetch_add(1, Ordering::SeqCst);
                }
            })
            .await;
        assert!(
            matches!(&result, Err(DurableWorkerError::ClaimUnavailable { .. })),
            "unexpected contender outcome: {result:?}"
        );
        assert_eq!(executions.load(Ordering::SeqCst), 0);
        let persisted_lease = store
            .load(run_id)
            .unwrap()
            .worker_lease()
            .expect("contender leaves the owner lease intact")
            .clone();
        assert_eq!(persisted_lease.owner_id, renewed_lease.owner_id);
        assert_eq!(persisted_lease.lease_id, renewed_lease.lease_id);

        finish.notify_one();
        assert!(matches!(
            task.await.unwrap().unwrap(),
            DurableWorkerOutcome::Executed {
                value: "worker-a-result",
                recovered_claim: false,
            }
        ));
        assert!(store.load(run_id).unwrap().worker_lease().is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn callback_waits_for_the_initial_fence_renewal() {
        let run_id = "initial-heartbeat-fence-run";
        let state =
            RunState::new("initial-heartbeat-session", run_id, DurabilityMode::Sync).unwrap();
        let store = Arc::new(BlockingHeartbeatStore::blocking_initial(&state));
        let first = DurableWorker::new(store.clone(), test_config("worker-a")).unwrap();
        let first_executions = Arc::new(AtomicUsize::new(0));
        let first_task = tokio::spawn({
            let first_executions = first_executions.clone();
            async move {
                first
                    .run(run_id, CancellationToken::new(), move |_, _| async move {
                        first_executions.fetch_add(1, Ordering::SeqCst);
                        "stale-worker"
                    })
                    .await
            }
        });

        wait_for_notification(&store.heartbeat_blocked, "initial heartbeat store I/O").await;
        assert_eq!(first_executions.load(Ordering::SeqCst), 0);
        tokio::time::sleep(Duration::from_millis(210)).await;

        let second = DurableWorker::new(store.clone(), test_config("worker-b")).unwrap();
        let second_executions = Arc::new(AtomicUsize::new(0));
        let second_outcome = second
            .run(run_id, CancellationToken::new(), {
                let second_executions = second_executions.clone();
                move |_, _| async move {
                    second_executions.fetch_add(1, Ordering::SeqCst);
                    "recovery-worker"
                }
            })
            .await
            .unwrap();
        assert!(matches!(
            second_outcome,
            DurableWorkerOutcome::Executed {
                value: "recovery-worker",
                recovered_claim: true,
            }
        ));

        store.unblock_heartbeat();
        let first_outcome = tokio::time::timeout(Duration::from_secs(1), first_task)
            .await
            .expect("stale worker observes the failed readiness renewal")
            .unwrap();
        assert!(
            matches!(first_outcome, Err(DurableWorkerError::LeaseLost { .. })),
            "unexpected stale-worker outcome: {first_outcome:?}"
        );
        assert_eq!(first_executions.load(Ordering::SeqCst), 0);
        assert_eq!(second_executions.load(Ordering::SeqCst), 1);
        assert!(store.load(run_id).unwrap().worker_lease().is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dedicated_heartbeat_survives_a_blocking_callback_poll() {
        let run_id = "blocking-callback-run";
        let store = seeded_store(run_id);
        let first_config = test_config("worker-a");
        let minimum_headroom = first_config.lease_ttl / 2;
        let first = DurableWorker::new(store.clone(), first_config).unwrap();
        let started = Arc::new(Notify::new());
        let first_task = tokio::spawn({
            let started = started.clone();
            async move {
                first
                    .run(run_id, CancellationToken::new(), move |_, _| async move {
                        started.notify_one();
                        std::thread::sleep(Duration::from_millis(300));
                        "blocking-result"
                    })
                    .await
            }
        });
        wait_for_notification(&started, "blocking callback").await;
        let initial_lease = store
            .load(run_id)
            .unwrap()
            .worker_lease()
            .expect("worker owns a lease before the blocking poll")
            .clone();
        let (renewed_lease, observed_at_unix_ms) = wait_for_fresh_worker_lease(
            store.as_ref(),
            run_id,
            initial_lease.heartbeat_at_unix_ms,
            initial_lease.expires_at_unix_ms,
            minimum_headroom,
            "a dedicated heartbeat beyond the initial lease window",
        )
        .await;
        assert!(observed_at_unix_ms >= initial_lease.expires_at_unix_ms);

        let mut contender_config = test_config("worker-b");
        contender_config.max_poll_attempts = 1;
        let contender = DurableWorker::new(store.clone(), contender_config).unwrap();
        let contender_executions = Arc::new(AtomicUsize::new(0));
        let contender_result = contender
            .run(run_id, CancellationToken::new(), {
                let contender_executions = contender_executions.clone();
                move |_, _| async move {
                    contender_executions.fetch_add(1, Ordering::SeqCst);
                }
            })
            .await;
        assert!(
            matches!(
                &contender_result,
                Err(DurableWorkerError::ClaimUnavailable { .. })
            ),
            "unexpected contender outcome: {contender_result:?}"
        );
        assert_eq!(contender_executions.load(Ordering::SeqCst), 0);
        let persisted_lease = store
            .load(run_id)
            .unwrap()
            .worker_lease()
            .expect("contender leaves the dedicated heartbeat lease intact")
            .clone();
        assert_eq!(persisted_lease.owner_id, renewed_lease.owner_id);
        assert_eq!(persisted_lease.lease_id, renewed_lease.lease_id);
        assert!(matches!(
            first_task.await.unwrap().unwrap(),
            DurableWorkerOutcome::Executed {
                value: "blocking-result",
                recovered_claim: false,
            }
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn polling_stops_promptly_when_cancelled() {
        let run_id = "cancelled-poll-run";
        let store = seeded_store(run_id);
        let owner = DurableWorker::new(store.clone(), test_config("worker-a")).unwrap();
        let owner_started = Arc::new(Notify::new());
        let owner_finish = Arc::new(Notify::new());
        let owner_task = tokio::spawn({
            let owner_started = owner_started.clone();
            let owner_finish = owner_finish.clone();
            async move {
                owner
                    .run(run_id, CancellationToken::new(), move |_, _| async move {
                        owner_started.notify_one();
                        owner_finish.notified().await;
                    })
                    .await
            }
        });
        wait_for_notification(&owner_started, "lease owner callback").await;

        let mut waiter_config = test_config("worker-b");
        waiter_config.initial_poll_backoff = Duration::from_millis(100);
        waiter_config.max_poll_backoff = Duration::from_millis(100);
        waiter_config.max_poll_attempts = 100;
        let waiter = DurableWorker::new(store, waiter_config).unwrap();
        let cancellation = CancellationToken::new();
        let waiter_task = tokio::spawn({
            let cancellation = cancellation.clone();
            async move {
                waiter
                    .run(run_id, cancellation, |_, _| async {
                        panic!("a polling worker must not execute without the claim")
                    })
                    .await
            }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        cancellation.cancel();
        let result = tokio::time::timeout(Duration::from_millis(250), waiter_task)
            .await
            .expect("cancellation interrupts bounded polling")
            .unwrap();
        assert!(matches!(
            result,
            Err(DurableWorkerError::Cancelled { ref run_id }) if run_id == "cancelled-poll-run"
        ));

        owner_finish.notify_one();
        assert!(owner_task.await.unwrap().is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn running_execution_observes_cancellation_and_releases_after_the_grace_bound() {
        let run_id = "cancelled-execution-run";
        let store = seeded_store(run_id);
        let worker = DurableWorker::new(store.clone(), test_config("worker-a")).unwrap();
        let execution_started = Arc::new(Notify::new());
        let cancellation_observed = Arc::new(Notify::new());
        let cancellation = CancellationToken::new();
        let task = tokio::spawn({
            let execution_started = execution_started.clone();
            let cancellation_observed = cancellation_observed.clone();
            let worker_cancellation = cancellation.clone();
            async move {
                worker
                    .run(
                        run_id,
                        worker_cancellation,
                        move |_, execution_cancellation| async move {
                            execution_started.notify_one();
                            execution_cancellation.cancelled().await;
                            cancellation_observed.notify_one();
                            std::future::pending::<()>().await;
                        },
                    )
                    .await
            }
        });
        wait_for_notification(&execution_started, "cancelled callback").await;
        cancellation.cancel();
        tokio::time::timeout(Duration::from_millis(100), cancellation_observed.notified())
            .await
            .expect("the runtime callback receives the worker cancellation token");
        let result = tokio::time::timeout(Duration::from_millis(200), task)
            .await
            .expect("cancellation grace is bounded")
            .unwrap();
        assert!(matches!(
            result,
            Err(DurableWorkerError::Cancelled { ref run_id }) if run_id == "cancelled-execution-run"
        ));
        assert!(store.load(run_id).unwrap().worker_lease().is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cancellation_drops_execution_before_releasing_the_lease() {
        let run_id = "cancel-drop-order-run";
        let store = seeded_store(run_id);
        let worker = DurableWorker::new(store.clone(), test_config("worker-a")).unwrap();
        let execution_started = Arc::new(Notify::new());
        let (drop_started, drop_started_receiver) = tokio::sync::oneshot::channel();
        let drop_gate = Arc::new((Mutex::new(false), Condvar::new()));
        let drop_finished = Arc::new(AtomicBool::new(false));
        let cancellation = CancellationToken::new();
        let task = tokio::spawn({
            let worker_cancellation = cancellation.clone();
            let execution_started = execution_started.clone();
            let future_drop_gate = drop_gate.clone();
            let future_drop_finished = drop_finished.clone();
            async move {
                worker
                    .run(run_id, worker_cancellation, move |_, _| {
                        BlockingDropFuture {
                            started: execution_started,
                            polled: false,
                            drop_started: Some(drop_started),
                            drop_gate: future_drop_gate,
                            drop_finished: future_drop_finished,
                        }
                    })
                    .await
            }
        });
        wait_for_notification(&execution_started, "drop-order callback").await;
        cancellation.cancel();
        tokio::time::timeout(Duration::from_secs(2), drop_started_receiver)
            .await
            .expect("cancellation reaches future Drop")
            .expect("future reports Drop start");
        let owner_lease = store
            .load(run_id)
            .unwrap()
            .worker_lease()
            .expect("worker lease remains held while the execution future is dropping")
            .clone();

        let mut contender_config = test_config("worker-b");
        contender_config.max_poll_attempts = 1;
        let contender = DurableWorker::new(store.clone(), contender_config).unwrap();
        let contender_executions = Arc::new(AtomicUsize::new(0));
        let contender_result = contender
            .run(run_id, CancellationToken::new(), {
                let contender_executions = contender_executions.clone();
                move |_, _| async move {
                    contender_executions.fetch_add(1, Ordering::SeqCst);
                }
            })
            .await;
        assert!(
            matches!(
                &contender_result,
                Err(DurableWorkerError::ClaimUnavailable { .. })
            ),
            "unexpected contender outcome: {contender_result:?}"
        );
        assert_eq!(contender_executions.load(Ordering::SeqCst), 0);
        let persisted_lease = store
            .load(run_id)
            .unwrap()
            .worker_lease()
            .expect("contender leaves the dropping worker lease intact")
            .clone();
        assert_eq!(persisted_lease.owner_id, owner_lease.owner_id);
        assert_eq!(persisted_lease.lease_id, owner_lease.lease_id);
        assert!(!drop_finished.load(Ordering::SeqCst));

        let (released, wake) = &*drop_gate;
        *released.lock().unwrap() = true;
        wake.notify_all();
        let result = tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("worker finishes after execution Drop")
            .unwrap();
        assert!(matches!(
            result,
            Err(DurableWorkerError::Cancelled { ref run_id }) if run_id == "cancel-drop-order-run"
        ));
        assert!(drop_finished.load(Ordering::SeqCst));
        assert!(store.load(run_id).unwrap().worker_lease().is_none());
    }

    #[tokio::test]
    async fn blocked_heartbeat_shutdown_is_bounded_and_retains_the_lease_fence() {
        let run_id = "blocked-heartbeat-shutdown-run";
        let state =
            RunState::new("blocked-heartbeat-session", run_id, DurabilityMode::Sync).unwrap();
        let store = Arc::new(BlockingHeartbeatStore::new(&state));
        let mut worker_config = test_config("worker-a");
        worker_config.lease_ttl = Duration::from_millis(400);
        worker_config.cancellation_grace = Duration::from_millis(120);
        let worker = DurableWorker::new(store.clone(), worker_config).unwrap();
        let callback_started = Arc::new(Notify::new());
        let callback_finish = Arc::new(Notify::new());
        let task = tokio::spawn({
            let callback_started = callback_started.clone();
            let callback_finish = callback_finish.clone();
            async move {
                worker
                    .run(run_id, CancellationToken::new(), move |_, _| async move {
                        callback_started.notify_one();
                        callback_finish.notified().await;
                        "completed-before-heartbeat-shutdown"
                    })
                    .await
            }
        });
        wait_for_notification(&callback_started, "blocked-heartbeat callback").await;
        wait_for_notification(&store.heartbeat_blocked, "heartbeat store I/O").await;
        let tick_started = std::time::Instant::now();
        callback_finish.notify_one();
        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(
            tick_started.elapsed() < Duration::from_millis(80),
            "heartbeat shutdown must not block the current-thread Tokio timer"
        );

        let result = tokio::time::timeout(Duration::from_millis(300), task)
            .await
            .expect("heartbeat shutdown is bounded")
            .unwrap();
        assert!(matches!(result, Err(DurableWorkerError::LeaseLost { .. })));
        assert_eq!(
            store.load(run_id).unwrap().worker_lease().unwrap().owner_id,
            "worker-a"
        );

        let mut contender_config = test_config("worker-b");
        contender_config.max_poll_attempts = 1;
        let contender = DurableWorker::new(store.clone(), contender_config).unwrap();
        let contender_executions = Arc::new(AtomicUsize::new(0));
        let contender_result = contender
            .run(run_id, CancellationToken::new(), {
                let contender_executions = contender_executions.clone();
                move |_, _| async move {
                    contender_executions.fetch_add(1, Ordering::SeqCst);
                }
            })
            .await;
        assert!(matches!(
            contender_result,
            Err(DurableWorkerError::ClaimUnavailable {
                owner_id: Some(ref owner_id),
                ..
            }) if owner_id == "worker-a"
        ));
        assert_eq!(contender_executions.load(Ordering::SeqCst), 0);

        store.unblock_heartbeat();
        tokio::time::sleep(Duration::from_millis(20)).await;
        let expiry = store
            .load(run_id)
            .unwrap()
            .worker_lease()
            .unwrap()
            .expires_at_unix_ms;
        let now = store.worker_lease_clock_unix_ms().unwrap();
        if expiry > now {
            tokio::time::sleep(Duration::from_millis(expiry - now + 10)).await;
        }

        let recovery = DurableWorker::new(store.clone(), test_config("worker-c")).unwrap();
        assert!(matches!(
            recovery
                .run(run_id, CancellationToken::new(), |_, _| async {
                    "recovered-after-heartbeat-timeout"
                })
                .await
                .unwrap(),
            DurableWorkerOutcome::Executed {
                value: "recovered-after-heartbeat-timeout",
                recovered_claim: true,
            }
        ));
        assert!(store.load(run_id).unwrap().worker_lease().is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn expired_claim_is_recovered_and_the_stale_driver_is_fenced() {
        let run_id = "expired-claim-run";
        let store = seeded_store(run_id);
        let first = DurableWorker::new(store.clone(), test_config("worker-a")).unwrap();
        let driver_ready = Arc::new(Notify::new());
        let stale_driver = Arc::new(std::sync::Mutex::new(None));
        let first_task = tokio::spawn({
            let driver_ready = driver_ready.clone();
            let stale_driver = stale_driver.clone();
            async move {
                first
                    .run(
                        run_id,
                        CancellationToken::new(),
                        move |driver, _| async move {
                            *stale_driver.lock().unwrap() = Some(driver);
                            driver_ready.notify_one();
                            std::future::pending::<()>().await;
                        },
                    )
                    .await
            }
        });
        wait_for_notification(&driver_ready, "stale driver capture").await;
        first_task.abort();
        let _ = first_task.await;
        tokio::time::sleep(Duration::from_millis(230)).await;

        let second = DurableWorker::new(store.clone(), test_config("worker-b")).unwrap();
        let second_started = Arc::new(Notify::new());
        let second_finish = Arc::new(Notify::new());
        let second_task = tokio::spawn({
            let second_started = second_started.clone();
            let second_finish = second_finish.clone();
            async move {
                second
                    .run(run_id, CancellationToken::new(), move |_, _| async move {
                        second_started.notify_one();
                        second_finish.notified().await;
                    })
                    .await
            }
        });
        wait_for_notification(&second_started, "recovery worker callback").await;

        let stale = stale_driver
            .lock()
            .unwrap()
            .clone()
            .expect("first worker exposed its bound driver");
        assert!(stale
            .begin_activity(
                "stale-step",
                "stale-logical-key",
                json!({}),
                SideEffectClass::Pure,
                None,
            )
            .is_err());
        assert!(store
            .load(run_id)
            .unwrap()
            .projection()
            .activities
            .values()
            .all(|record| record.definition.stable_step_id != "stale-step"));

        second_finish.notify_one();
        assert!(matches!(
            second_task.await.unwrap().unwrap(),
            DurableWorkerOutcome::Executed {
                recovered_claim: true,
                ..
            }
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ambiguous_activity_requires_explicit_reconciliation_before_resume() {
        let run_id = "ambiguous-activity-run";
        let store = seeded_store(run_id);
        let first = DurableWorker::new(store.clone(), test_config("worker-a")).unwrap();
        let activity_started = Arc::new(Notify::new());
        let first_task = tokio::spawn({
            let activity_started = activity_started.clone();
            async move {
                first
                    .run(
                        run_id,
                        CancellationToken::new(),
                        move |driver, _| async move {
                            let DurableActivity::Execute { .. } = driver
                                .begin_activity(
                                    "external-side-effect-v1",
                                    "effect-1",
                                    json!({"operation": "charge"}),
                                    SideEffectClass::ReconcileRequired,
                                    None,
                                )
                                .unwrap()
                            else {
                                panic!("first attempt must execute")
                            };
                            activity_started.notify_one();
                            std::future::pending::<()>().await;
                        },
                    )
                    .await
            }
        });
        wait_for_notification(&activity_started, "ambiguous activity start").await;
        first_task.abort();
        let _ = first_task.await;
        tokio::time::sleep(Duration::from_millis(230)).await;

        let second = DurableWorker::new(store.clone(), test_config("worker-b")).unwrap();
        let executions = Arc::new(AtomicUsize::new(0));
        let outcome = second
            .run(run_id, CancellationToken::new(), {
                let executions = executions.clone();
                move |_, _| async move {
                    executions.fetch_add(1, Ordering::SeqCst);
                }
            })
            .await
            .unwrap();
        let activity_id = match outcome {
            DurableWorkerOutcome::ReconcileRequired {
                activity_ids,
                reason,
            } => {
                assert!(!reason.is_empty());
                assert_eq!(activity_ids.len(), 1);
                activity_ids[0].clone()
            }
            other => panic!("expected reconciliation boundary, got {other:?}"),
        };
        assert_eq!(executions.load(Ordering::SeqCst), 0);

        let invalid = second
            .reconcile_activity(
                run_id,
                CancellationToken::new(),
                "operator-invalid-effect",
                "missing-activity",
                ActivityReconciliation::Completed {
                    output: json!({"charged": true}),
                },
            )
            .await;
        assert!(matches!(
            invalid,
            Err(DurableWorkerError::Driver(DurableRunDriverError::State(
                DurabilityError::ActivityNotFound { .. }
            )))
        ));
        assert!(store.load(run_id).unwrap().worker_lease().is_none());

        let reconciled = second
            .reconcile_activity(
                run_id,
                CancellationToken::new(),
                "operator-confirmed-effect-1",
                &activity_id,
                ActivityReconciliation::Completed {
                    output: json!({"charged": true}),
                },
            )
            .await
            .unwrap();
        assert_eq!(reconciled.status(), DurableRunStatus::Paused);
        assert_eq!(
            reconciled
                .activity(&activity_id)
                .unwrap()
                .completed_output(),
            Some(&json!({"charged": true}))
        );
        assert!(reconciled.worker_lease().is_none());
    }
}
