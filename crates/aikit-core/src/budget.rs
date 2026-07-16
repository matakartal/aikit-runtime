//! Turn/token/cost governor.
//!
//! USD budgets require explicit pricing. Model prices change, so the core never ships a stale
//! hard-coded table and never pretends an unknown model costs zero. A host/router can inject a
//! current [`ModelPricing`] snapshot and audit exactly which rates governed the run.

use crate::error::{AikitError, Result};
use crate::types::Usage;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct ModelPricing {
    pub input_per_million_usd: f64,
    pub output_per_million_usd: f64,
    pub cache_read_per_million_usd: Option<f64>,
    pub cache_write_per_million_usd: Option<f64>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct BudgetPolicy {
    pub max_total_tokens: Option<u64>,
    pub max_cost_usd: Option<f64>,
    pub pricing: Option<ModelPricing>,
}

impl BudgetPolicy {
    pub fn token_limit(max_total_tokens: u64) -> Self {
        BudgetPolicy {
            max_total_tokens: Some(max_total_tokens),
            ..BudgetPolicy::default()
        }
    }

    pub fn cost_limit(max_cost_usd: f64, pricing: ModelPricing) -> Self {
        BudgetPolicy {
            max_cost_usd: Some(max_cost_usd),
            pricing: Some(pricing),
            ..BudgetPolicy::default()
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.max_cost_usd.is_some() && self.pricing.is_none() {
            return Err(AikitError::BudgetExceeded);
        }
        if self
            .max_cost_usd
            .is_some_and(|limit| !limit.is_finite() || limit < 0.0)
        {
            return Err(AikitError::Other(
                "max_cost_usd must be finite and non-negative".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub struct BudgetSnapshot {
    pub usage: Usage,
    pub estimated_cost_usd: f64,
}

#[derive(Debug, Clone)]
pub struct BudgetTracker {
    policy: BudgetPolicy,
    snapshot: BudgetSnapshot,
}

impl BudgetTracker {
    pub fn new(policy: BudgetPolicy) -> Result<Self> {
        policy.validate()?;
        Ok(BudgetTracker {
            policy,
            snapshot: BudgetSnapshot::default(),
        })
    }

    pub fn snapshot(&self) -> BudgetSnapshot {
        self.snapshot
    }

    pub fn record(&mut self, usage: Usage) -> Result<BudgetSnapshot> {
        add_usage(&mut self.snapshot.usage, usage);
        self.snapshot.estimated_cost_usd = self.estimate_cost();

        let total_tokens = self
            .snapshot
            .usage
            .input_tokens
            .saturating_add(self.snapshot.usage.output_tokens);
        if self
            .policy
            .max_total_tokens
            .is_some_and(|limit| total_tokens > limit)
        {
            return Err(AikitError::BudgetExceeded);
        }
        if self
            .policy
            .max_cost_usd
            .is_some_and(|limit| self.snapshot.estimated_cost_usd > limit)
        {
            return Err(AikitError::BudgetExceeded);
        }
        Ok(self.snapshot)
    }

    fn estimate_cost(&self) -> f64 {
        let Some(pricing) = self.policy.pricing else {
            return 0.0;
        };
        let usage = self.snapshot.usage;
        let input = usage
            .input_tokens
            .saturating_sub(usage.cache_read_input_tokens);
        let mut cost = input as f64 * pricing.input_per_million_usd / 1_000_000.0;
        cost += usage.output_tokens as f64 * pricing.output_per_million_usd / 1_000_000.0;
        cost += usage.cache_read_input_tokens as f64
            * pricing
                .cache_read_per_million_usd
                .unwrap_or(pricing.input_per_million_usd)
            / 1_000_000.0;
        cost += usage.cache_creation_input_tokens as f64
            * pricing
                .cache_write_per_million_usd
                .unwrap_or(pricing.input_per_million_usd)
            / 1_000_000.0;
        cost
    }
}

fn add_usage(total: &mut Usage, part: Usage) {
    total.input_tokens = total.input_tokens.saturating_add(part.input_tokens);
    total.output_tokens = total.output_tokens.saturating_add(part.output_tokens);
    total.cache_creation_input_tokens = total
        .cache_creation_input_tokens
        .saturating_add(part.cache_creation_input_tokens);
    total.cache_read_input_tokens = total
        .cache_read_input_tokens
        .saturating_add(part.cache_read_input_tokens);
    total.reasoning_tokens = total.reasoning_tokens.saturating_add(part.reasoning_tokens);
}

/// Shared limits for parallel model calls. Unlike the compatibility [`BudgetTracker`], this
/// ledger reserves worst-case capacity *before* a call starts, so sibling agents cannot each see
/// the same remaining budget and overspend it concurrently.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BudgetLimits {
    pub max_model_calls: Option<u64>,
    pub max_input_tokens: Option<u64>,
    pub max_output_tokens: Option<u64>,
    pub max_cost_micro_usd: Option<u64>,
    pub wall_time_ms: Option<u64>,
}

#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum BudgetLedgerError {
    #[error("USD budget requires explicit model pricing")]
    UnknownPricing,
    #[error("invalid budget configuration: {0}")]
    Invalid(String),
    #[error("budget exceeded for {resource}: limit={limit}, requested_total={requested_total}")]
    Exceeded {
        resource: &'static str,
        limit: u64,
        requested_total: u64,
    },
    #[error("budget ledger lock poisoned")]
    LockPoisoned,
    #[error("budget reservation does not belong to this ledger or is no longer active")]
    InvalidReservation,
}

pub type BudgetLedgerResult<T> = std::result::Result<T, BudgetLedgerError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BillingDisposition {
    /// Commit reported usage and its calculated cost.
    ChargeUsage,
    /// The attempt is counted, but no token/cost usage is committed.
    NoCharge,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BudgetLedgerSnapshot {
    pub committed_model_calls: u64,
    pub committed_input_tokens: u64,
    pub committed_output_tokens: u64,
    pub committed_cost_micro_usd: u64,
    pub reserved_model_calls: u64,
    pub reserved_input_tokens: u64,
    pub reserved_output_tokens: u64,
    pub reserved_cost_micro_usd: u64,
    pub elapsed_ms: u64,
}

#[derive(Debug, Clone, Copy)]
struct ReservationValues {
    input_tokens: u64,
    output_tokens: u64,
    cost_micro_usd: u64,
    pricing: Option<ModelPricing>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReservationState {
    Reserved,
    Started,
    Reconciled,
}

#[derive(Debug, Clone, Copy)]
struct ReservationEntry {
    values: ReservationValues,
    state: ReservationState,
}

#[derive(Default)]
struct LedgerState {
    next_id: u64,
    committed_calls: u64,
    committed_input: u64,
    committed_output: u64,
    committed_cost: u64,
    reservations: HashMap<u64, ReservationEntry>,
}

struct BudgetLedgerInner {
    limits: BudgetLimits,
    started: Instant,
    deadline: Option<Instant>,
    state: Mutex<LedgerState>,
}

#[derive(Clone)]
pub struct BudgetLedger {
    inner: Arc<BudgetLedgerInner>,
}

pub struct BudgetReservation {
    id: u64,
    inner: Weak<BudgetLedgerInner>,
    state: ReservationState,
}

impl BudgetReservation {
    /// Mark the reserved model call as started immediately before its provider future is polled.
    ///
    /// Dropping a merely reserved call releases its capacity. Once started, dropping this handle
    /// conservatively commits the entire reservation so cancellation cannot turn an in-flight
    /// provider request into an unaccounted/free call.
    pub fn mark_started(&mut self) -> BudgetLedgerResult<()> {
        if self.state != ReservationState::Reserved {
            return Err(BudgetLedgerError::InvalidReservation);
        }
        let owner = self
            .inner
            .upgrade()
            .ok_or(BudgetLedgerError::InvalidReservation)?;
        let mut state = owner
            .state
            .lock()
            .map_err(|_| BudgetLedgerError::LockPoisoned)?;
        let entry = state
            .reservations
            .get_mut(&self.id)
            .ok_or(BudgetLedgerError::InvalidReservation)?;
        if entry.state != ReservationState::Reserved {
            return Err(BudgetLedgerError::InvalidReservation);
        }
        entry.state = ReservationState::Started;
        self.state = ReservationState::Started;
        Ok(())
    }
}

impl Drop for BudgetReservation {
    fn drop(&mut self) {
        if self.state == ReservationState::Reconciled {
            return;
        }
        if let Some(inner) = self.inner.upgrade() {
            if let Ok(mut state) = inner.state.lock() {
                if let Some(entry) = state.reservations.remove(&self.id) {
                    if entry.state == ReservationState::Started {
                        commit_reserved(&mut state, entry.values);
                    }
                }
            }
        }
    }
}

impl BudgetLedger {
    pub fn new(limits: BudgetLimits) -> BudgetLedgerResult<Self> {
        if limits.max_model_calls == Some(0) {
            return Err(BudgetLedgerError::Invalid(
                "max_model_calls must be greater than zero".into(),
            ));
        }
        let started = Instant::now();
        let deadline = limits
            .wall_time_ms
            .map(Duration::from_millis)
            .map(|duration| {
                started
                    .checked_add(duration)
                    .ok_or_else(|| BudgetLedgerError::Invalid("wall_time_ms is too large".into()))
            })
            .transpose()?;
        Ok(BudgetLedger {
            inner: Arc::new(BudgetLedgerInner {
                limits,
                started,
                deadline,
                state: Mutex::new(LedgerState::default()),
            }),
        })
    }

    pub fn limits(&self) -> &BudgetLimits {
        &self.inner.limits
    }

    /// The absolute wall-time deadline shared by every clone and child reservation.
    ///
    /// Runtime orchestration copies this instant into each child run instead of starting a new
    /// timer, so fan-out cannot multiply the parent's wall-time allowance.
    pub(crate) fn wall_time_deadline(&self) -> Option<Instant> {
        self.inner.deadline
    }

    pub fn reserve_model_call(
        &self,
        pricing: Option<ModelPricing>,
        estimated_input_tokens: u64,
        max_output_tokens: u64,
    ) -> BudgetLedgerResult<BudgetReservation> {
        self.check_wall_time()?;
        let reserved_cost = match (self.inner.limits.max_cost_micro_usd, pricing) {
            (Some(_), None) => return Err(BudgetLedgerError::UnknownPricing),
            (_, Some(pricing)) => cost_micro_usd(
                pricing,
                Usage {
                    input_tokens: estimated_input_tokens,
                    output_tokens: max_output_tokens,
                    ..Usage::default()
                },
            )?,
            (None, None) => 0,
        };

        let mut state = self
            .inner
            .state
            .lock()
            .map_err(|_| BudgetLedgerError::LockPoisoned)?;
        let reserved = reservation_totals(&state);
        check_limit(
            "model_calls",
            self.inner.limits.max_model_calls,
            state
                .committed_calls
                .saturating_add(reserved.0)
                .saturating_add(1),
        )?;
        check_limit(
            "input_tokens",
            self.inner.limits.max_input_tokens,
            state
                .committed_input
                .saturating_add(reserved.1)
                .saturating_add(estimated_input_tokens),
        )?;
        check_limit(
            "output_tokens",
            self.inner.limits.max_output_tokens,
            state
                .committed_output
                .saturating_add(reserved.2)
                .saturating_add(max_output_tokens),
        )?;
        check_limit(
            "cost_micro_usd",
            self.inner.limits.max_cost_micro_usd,
            state
                .committed_cost
                .saturating_add(reserved.3)
                .saturating_add(reserved_cost),
        )?;

        state.next_id = state.next_id.saturating_add(1);
        let id = state.next_id;
        state.reservations.insert(
            id,
            ReservationEntry {
                values: ReservationValues {
                    input_tokens: estimated_input_tokens,
                    output_tokens: max_output_tokens,
                    cost_micro_usd: reserved_cost,
                    pricing,
                },
                state: ReservationState::Reserved,
            },
        );
        Ok(BudgetReservation {
            id,
            inner: Arc::downgrade(&self.inner),
            state: ReservationState::Reserved,
        })
    }

    pub fn reconcile(
        &self,
        mut reservation: BudgetReservation,
        usage: Option<Usage>,
        disposition: BillingDisposition,
    ) -> BudgetLedgerResult<BudgetLedgerSnapshot> {
        let Some(owner) = reservation.inner.upgrade() else {
            return Err(BudgetLedgerError::InvalidReservation);
        };
        if !Arc::ptr_eq(&owner, &self.inner) || reservation.state != ReservationState::Started {
            return Err(BudgetLedgerError::InvalidReservation);
        }
        let mut state = self
            .inner
            .state
            .lock()
            .map_err(|_| BudgetLedgerError::LockPoisoned)?;
        let entry = state
            .reservations
            .get(&reservation.id)
            .copied()
            .ok_or(BudgetLedgerError::InvalidReservation)?;
        if entry.state != ReservationState::Started {
            return Err(BudgetLedgerError::InvalidReservation);
        }

        let usage = usage.unwrap_or_default();
        let exact_cost = if matches!(disposition, BillingDisposition::ChargeUsage) {
            entry
                .values
                .pricing
                .map(|pricing| cost_micro_usd(pricing, usage))
                .transpose()?
                .unwrap_or_default()
        } else {
            0
        };

        state.reservations.remove(&reservation.id);
        reservation.state = ReservationState::Reconciled;
        state.committed_calls = state.committed_calls.saturating_add(1);

        if matches!(disposition, BillingDisposition::ChargeUsage) {
            state.committed_input = state.committed_input.saturating_add(usage.input_tokens);
            state.committed_output = state.committed_output.saturating_add(usage.output_tokens);
            state.committed_cost = state.committed_cost.saturating_add(exact_cost);
        }

        // Report dishonest/over-limit provider usage after committing the real accounting data.
        check_limit(
            "model_calls",
            self.inner.limits.max_model_calls,
            state.committed_calls,
        )?;
        check_limit(
            "input_tokens",
            self.inner.limits.max_input_tokens,
            state.committed_input,
        )?;
        check_limit(
            "output_tokens",
            self.inner.limits.max_output_tokens,
            state.committed_output,
        )?;
        check_limit(
            "cost_micro_usd",
            self.inner.limits.max_cost_micro_usd,
            state.committed_cost,
        )?;
        Ok(snapshot(&self.inner, &state))
    }

    pub fn snapshot(&self) -> BudgetLedgerResult<BudgetLedgerSnapshot> {
        self.check_wall_time()?;
        let state = self
            .inner
            .state
            .lock()
            .map_err(|_| BudgetLedgerError::LockPoisoned)?;
        Ok(snapshot(&self.inner, &state))
    }

    fn check_wall_time(&self) -> BudgetLedgerResult<()> {
        let elapsed = self
            .inner
            .started
            .elapsed()
            .as_millis()
            .min(u64::MAX as u128) as u64;
        if self
            .inner
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            return Err(BudgetLedgerError::Exceeded {
                resource: "wall_time_ms",
                limit: self
                    .inner
                    .limits
                    .wall_time_ms
                    .expect("a deadline exists only when wall_time_ms is configured"),
                requested_total: elapsed,
            });
        }
        Ok(())
    }
}

fn reservation_totals(state: &LedgerState) -> (u64, u64, u64, u64) {
    state.reservations.values().fold(
        (0_u64, 0_u64, 0_u64, 0_u64),
        |(calls, input, output, cost), reservation| {
            (
                calls.saturating_add(1),
                input.saturating_add(reservation.values.input_tokens),
                output.saturating_add(reservation.values.output_tokens),
                cost.saturating_add(reservation.values.cost_micro_usd),
            )
        },
    )
}

fn commit_reserved(state: &mut LedgerState, values: ReservationValues) {
    state.committed_calls = state.committed_calls.saturating_add(1);
    state.committed_input = state.committed_input.saturating_add(values.input_tokens);
    state.committed_output = state.committed_output.saturating_add(values.output_tokens);
    state.committed_cost = state.committed_cost.saturating_add(values.cost_micro_usd);
}

fn snapshot(inner: &BudgetLedgerInner, state: &LedgerState) -> BudgetLedgerSnapshot {
    let reserved = reservation_totals(state);
    BudgetLedgerSnapshot {
        committed_model_calls: state.committed_calls,
        committed_input_tokens: state.committed_input,
        committed_output_tokens: state.committed_output,
        committed_cost_micro_usd: state.committed_cost,
        reserved_model_calls: reserved.0,
        reserved_input_tokens: reserved.1,
        reserved_output_tokens: reserved.2,
        reserved_cost_micro_usd: reserved.3,
        elapsed_ms: inner.started.elapsed().as_millis().min(u64::MAX as u128) as u64,
    }
}

fn check_limit(
    resource: &'static str,
    limit: Option<u64>,
    requested_total: u64,
) -> BudgetLedgerResult<()> {
    if let Some(limit) = limit {
        if requested_total > limit {
            return Err(BudgetLedgerError::Exceeded {
                resource,
                limit,
                requested_total,
            });
        }
    }
    Ok(())
}

fn cost_micro_usd(pricing: ModelPricing, usage: Usage) -> BudgetLedgerResult<u64> {
    for rate in [
        Some(pricing.input_per_million_usd),
        Some(pricing.output_per_million_usd),
        pricing.cache_read_per_million_usd,
        pricing.cache_write_per_million_usd,
    ]
    .into_iter()
    .flatten()
    {
        if !rate.is_finite() || rate < 0.0 {
            return Err(BudgetLedgerError::Invalid(
                "pricing rates must be finite and non-negative".into(),
            ));
        }
    }
    let uncached_input = usage
        .input_tokens
        .saturating_sub(usage.cache_read_input_tokens);
    let usd = uncached_input as f64 * pricing.input_per_million_usd / 1_000_000.0
        + usage.output_tokens as f64 * pricing.output_per_million_usd / 1_000_000.0
        + usage.cache_read_input_tokens as f64
            * pricing
                .cache_read_per_million_usd
                .unwrap_or(pricing.input_per_million_usd)
            / 1_000_000.0
        + usage.cache_creation_input_tokens as f64
            * pricing
                .cache_write_per_million_usd
                .unwrap_or(pricing.input_per_million_usd)
            / 1_000_000.0;
    let micro = (usd * 1_000_000.0).ceil();
    if !micro.is_finite() || micro > u64::MAX as f64 {
        return Err(BudgetLedgerError::Invalid(
            "estimated cost exceeds supported range".into(),
        ));
    }
    Ok(micro as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_limit_fails_after_usage_crosses_ceiling() {
        let mut tracker = BudgetTracker::new(BudgetPolicy::token_limit(10)).unwrap();
        tracker
            .record(Usage {
                input_tokens: 4,
                output_tokens: 5,
                ..Usage::default()
            })
            .unwrap();
        assert!(matches!(
            tracker.record(Usage {
                input_tokens: 2,
                ..Usage::default()
            }),
            Err(AikitError::BudgetExceeded)
        ));
    }

    #[test]
    fn cost_uses_explicit_rates_and_cache_discount() {
        let pricing = ModelPricing {
            input_per_million_usd: 10.0,
            output_per_million_usd: 20.0,
            cache_read_per_million_usd: Some(1.0),
            cache_write_per_million_usd: Some(12.5),
        };
        let mut tracker = BudgetTracker::new(BudgetPolicy::cost_limit(1.0, pricing)).unwrap();
        let snapshot = tracker
            .record(Usage {
                input_tokens: 10_000,
                output_tokens: 5_000,
                cache_read_input_tokens: 8_000,
                cache_creation_input_tokens: 1_000,
                ..Usage::default()
            })
            .unwrap();
        assert!((snapshot.estimated_cost_usd - 0.1405).abs() < 1e-9);
    }

    #[test]
    fn usd_limit_without_pricing_is_rejected_not_assumed_free() {
        let policy = BudgetPolicy {
            max_cost_usd: Some(1.0),
            ..BudgetPolicy::default()
        };
        assert!(matches!(
            BudgetTracker::new(policy),
            Err(AikitError::BudgetExceeded)
        ));
    }

    #[test]
    fn parallel_reservations_cannot_oversubscribe_shared_limit() {
        let ledger = BudgetLedger::new(BudgetLimits {
            max_model_calls: Some(1),
            ..BudgetLimits::default()
        })
        .unwrap();
        let first = ledger.reserve_model_call(None, 10, 10).unwrap();
        assert!(matches!(
            ledger.reserve_model_call(None, 10, 10),
            Err(BudgetLedgerError::Exceeded {
                resource: "model_calls",
                ..
            })
        ));
        drop(first);
        assert!(ledger.reserve_model_call(None, 10, 10).is_ok());
    }

    #[test]
    fn dropping_unstarted_reservation_releases_capacity_without_committing() {
        let ledger = BudgetLedger::new(BudgetLimits {
            max_model_calls: Some(1),
            max_input_tokens: Some(10),
            max_output_tokens: Some(20),
            ..BudgetLimits::default()
        })
        .unwrap();
        let reservation = ledger.reserve_model_call(None, 10, 20).unwrap();

        drop(reservation);

        let snapshot = ledger.snapshot().unwrap();
        assert_eq!(snapshot.committed_model_calls, 0);
        assert_eq!(snapshot.committed_input_tokens, 0);
        assert_eq!(snapshot.committed_output_tokens, 0);
        assert_eq!(snapshot.reserved_model_calls, 0);
        assert!(ledger.reserve_model_call(None, 10, 20).is_ok());
    }

    #[test]
    fn dropping_started_reservation_commits_worst_case_once() {
        let pricing = ModelPricing {
            input_per_million_usd: 1.0,
            output_per_million_usd: 2.0,
            cache_read_per_million_usd: None,
            cache_write_per_million_usd: None,
        };
        let ledger = BudgetLedger::new(BudgetLimits {
            max_model_calls: Some(2),
            max_input_tokens: Some(1_000),
            max_output_tokens: Some(1_000),
            max_cost_micro_usd: Some(10_000),
            wall_time_ms: None,
        })
        .unwrap();
        let mut reservation = ledger.reserve_model_call(Some(pricing), 100, 50).unwrap();
        reservation.mark_started().unwrap();

        drop(reservation);

        let snapshot = ledger.snapshot().unwrap();
        assert_eq!(snapshot.committed_model_calls, 1);
        assert_eq!(snapshot.committed_input_tokens, 100);
        assert_eq!(snapshot.committed_output_tokens, 50);
        assert_eq!(snapshot.committed_cost_micro_usd, 200);
        assert_eq!(snapshot.reserved_model_calls, 0);
    }

    #[test]
    fn usd_reservation_without_pricing_fails_closed() {
        let ledger = BudgetLedger::new(BudgetLimits {
            max_cost_micro_usd: Some(1_000_000),
            ..BudgetLimits::default()
        })
        .unwrap();
        assert!(matches!(
            ledger.reserve_model_call(None, 1, 1),
            Err(BudgetLedgerError::UnknownPricing)
        ));
    }

    #[test]
    fn reconcile_releases_reservation_and_commits_real_usage() {
        let pricing = ModelPricing {
            input_per_million_usd: 1.0,
            output_per_million_usd: 2.0,
            cache_read_per_million_usd: None,
            cache_write_per_million_usd: None,
        };
        let ledger = BudgetLedger::new(BudgetLimits {
            max_model_calls: Some(2),
            max_input_tokens: Some(1_000),
            max_output_tokens: Some(1_000),
            max_cost_micro_usd: Some(10_000),
            wall_time_ms: None,
        })
        .unwrap();
        let mut reservation = ledger.reserve_model_call(Some(pricing), 100, 100).unwrap();
        reservation.mark_started().unwrap();
        assert_eq!(ledger.snapshot().unwrap().reserved_model_calls, 1);
        let snapshot = ledger
            .reconcile(
                reservation,
                Some(Usage {
                    input_tokens: 20,
                    output_tokens: 10,
                    ..Usage::default()
                }),
                BillingDisposition::ChargeUsage,
            )
            .unwrap();
        assert_eq!(snapshot.reserved_model_calls, 0);
        assert_eq!(snapshot.committed_model_calls, 1);
        assert_eq!(snapshot.committed_input_tokens, 20);
        assert_eq!(snapshot.committed_output_tokens, 10);
        assert_eq!(snapshot.committed_cost_micro_usd, 40);
        assert_eq!(ledger.snapshot().unwrap(), snapshot);
    }
}
