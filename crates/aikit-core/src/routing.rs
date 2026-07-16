//! Deterministic model catalog and routing.
//!
//! Routing is intentionally pure: it receives only non-secret facts (which providers have an
//! active credential, the workload estimate, and hard requirements) and returns a reproducible
//! decision. Explicit routing never bypasses constraints. Automatic routing first applies every
//! hard constraint, then ranks the remaining models by cost, quality, or an equal-weight balanced
//! score. Model id is the final tie-breaker, so catalog insertion order can never change a route.

use crate::budget::ModelPricing;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use thiserror::Error;

/// A typed model feature that a route may require.
///
/// `Custom` keeps the catalog extensible without teaching the core every provider-specific
/// feature. Requirements use all-of semantics: a model must contain every requested capability.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelCapability {
    Reasoning,
    PromptCache,
    Citations,
    Vision,
    NativeStructuredOutput,
    ToolUse,
    ImageGeneration,
    Custom(String),
}

/// Static, caller-maintained facts about one routable model.
///
/// `quality_score` is an application-owned score in the inclusive `0..=100` range. The core does
/// not ship a possibly stale global ranking or price table. Pricing is similarly an explicit
/// snapshot supplied by the host.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelProfile {
    pub provider: String,
    pub model: String,
    /// Maximum combined prompt and generated-token context.
    pub context_window_tokens: u64,
    /// Maximum tokens the model may generate in one request.
    pub max_output_tokens: u64,
    /// Current pricing snapshot. `None` means genuinely unknown, never zero-cost.
    pub pricing: Option<ModelPricing>,
    /// Application-owned comparable quality score, `0..=100` (higher is better).
    pub quality_score: u8,
    /// Task-specialization labels such as `summary`, `coding`, or `hard_judgment`.
    pub skills: BTreeSet<String>,
    pub capabilities: BTreeSet<ModelCapability>,
}

impl ModelProfile {
    pub fn new(
        provider: impl Into<String>,
        model: impl Into<String>,
        context_window_tokens: u64,
        max_output_tokens: u64,
        quality_score: u8,
    ) -> Self {
        Self {
            provider: provider.into(),
            model: model.into(),
            context_window_tokens,
            max_output_tokens,
            pricing: None,
            quality_score,
            skills: BTreeSet::new(),
            capabilities: BTreeSet::new(),
        }
    }

    pub fn with_pricing(mut self, pricing: ModelPricing) -> Self {
        self.pricing = Some(pricing);
        self
    }

    pub fn with_skill(mut self, skill: impl Into<String>) -> Self {
        self.skills.insert(skill.into());
        self
    }

    pub fn with_capability(mut self, capability: ModelCapability) -> Self {
        self.capabilities.insert(capability);
        self
    }

    fn validate(&self) -> Result<(), RouteError> {
        if self.provider.trim().is_empty() {
            return Err(RouteError::InvalidProfile {
                model: self.model.clone(),
                reason: "provider must not be empty".into(),
            });
        }
        if self.model.trim().is_empty() {
            return Err(RouteError::InvalidProfile {
                model: self.model.clone(),
                reason: "model must not be empty".into(),
            });
        }
        if self.context_window_tokens == 0 {
            return Err(RouteError::InvalidProfile {
                model: self.model.clone(),
                reason: "context_window_tokens must be greater than zero".into(),
            });
        }
        if self.max_output_tokens == 0 || self.max_output_tokens > self.context_window_tokens {
            return Err(RouteError::InvalidProfile {
                model: self.model.clone(),
                reason: "max_output_tokens must be within the context window".into(),
            });
        }
        if self.quality_score > 100 {
            return Err(RouteError::InvalidProfile {
                model: self.model.clone(),
                reason: "quality_score must be in 0..=100".into(),
            });
        }
        if self.skills.iter().any(|skill| skill.trim().is_empty()) {
            return Err(RouteError::InvalidProfile {
                model: self.model.clone(),
                reason: "skills must not contain an empty value".into(),
            });
        }
        if let Some(pricing) = self.pricing {
            for (name, value) in [
                ("input_per_million_usd", pricing.input_per_million_usd),
                ("output_per_million_usd", pricing.output_per_million_usd),
            ] {
                if !value.is_finite() || value < 0.0 {
                    return Err(RouteError::InvalidProfile {
                        model: self.model.clone(),
                        reason: format!("{name} must be finite and non-negative"),
                    });
                }
            }
            for (name, value) in [
                (
                    "cache_read_per_million_usd",
                    pricing.cache_read_per_million_usd,
                ),
                (
                    "cache_write_per_million_usd",
                    pricing.cache_write_per_million_usd,
                ),
            ] {
                if value.is_some_and(|rate| !rate.is_finite() || rate < 0.0) {
                    return Err(RouteError::InvalidProfile {
                        model: self.model.clone(),
                        reason: format!("{name} must be finite and non-negative"),
                    });
                }
            }
        }
        Ok(())
    }
}

/// The objective used after all hard constraints have been applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RouteObjective {
    /// Lowest estimated request cost. Unknown-price models are ineligible.
    Cost,
    /// Highest caller-supplied quality score. Pricing may be unknown unless a USD cap is set.
    Quality,
    /// Equal-weight normalized quality and cost. Unknown-price models are ineligible.
    Balanced,
}

/// Select one named model, or let the router optimize across the eligible catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RoutePolicy {
    Explicit { model: String },
    Automatic { objective: RouteObjective },
}

/// Non-secret runtime facts and hard requirements for one route.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RouteRequest {
    pub policy: RoutePolicy,
    /// Providers for which the host currently has a usable credential/capability activation.
    pub active_providers: BTreeSet<String>,
    pub estimated_input_tokens: u64,
    pub required_output_tokens: u64,
    /// Maximum estimated request cost. Unknown prices fail closed whenever this is present.
    pub max_cost_usd: Option<f64>,
    /// All required skills must be present on the selected profile.
    pub required_skills: BTreeSet<String>,
    /// All required capabilities must be present on the selected profile.
    pub required_capabilities: BTreeSet<ModelCapability>,
}

impl RouteRequest {
    pub fn automatic(objective: RouteObjective) -> Self {
        Self {
            policy: RoutePolicy::Automatic { objective },
            active_providers: BTreeSet::new(),
            estimated_input_tokens: 0,
            required_output_tokens: 0,
            max_cost_usd: None,
            required_skills: BTreeSet::new(),
            required_capabilities: BTreeSet::new(),
        }
    }

    pub fn explicit(model: impl Into<String>) -> Self {
        Self {
            policy: RoutePolicy::Explicit {
                model: model.into(),
            },
            ..Self::automatic(RouteObjective::Balanced)
        }
    }

    fn validate(&self) -> Result<u64, RouteError> {
        if self
            .max_cost_usd
            .is_some_and(|limit| !limit.is_finite() || limit < 0.0)
        {
            return Err(RouteError::InvalidRequest(
                "max_cost_usd must be finite and non-negative".into(),
            ));
        }
        if self
            .active_providers
            .iter()
            .any(|provider| provider.trim().is_empty())
        {
            return Err(RouteError::InvalidRequest(
                "active_providers must not contain an empty value".into(),
            ));
        }
        if self
            .required_skills
            .iter()
            .any(|skill| skill.trim().is_empty())
        {
            return Err(RouteError::InvalidRequest(
                "required_skills must not contain an empty value".into(),
            ));
        }
        self.estimated_input_tokens
            .checked_add(self.required_output_tokens)
            .ok_or_else(|| RouteError::InvalidRequest("token estimate overflowed u64".into()))
    }
}

/// A reproducible route result. No credential value is ever stored in this structure.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RouteDecision {
    pub profile: ModelProfile,
    pub estimated_cost_usd: Option<f64>,
    pub policy: RoutePolicy,
    /// Number of models remaining after hard constraints (and objective pricing requirements).
    pub eligible_models: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelRejection {
    pub model: String,
    pub reasons: Vec<RejectionReason>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RejectionReason {
    MissingCredential { provider: String },
    MissingSkill { skill: String },
    MissingCapability { capability: ModelCapability },
    ContextWindowTooSmall { required: u64, available: u64 },
    OutputLimitTooSmall { required: u64, available: u64 },
    UnknownPricing,
    CostExceedsBudget { estimated: f64, limit: f64 },
}

#[derive(Debug, Clone, PartialEq, Error)]
pub enum RouteError {
    #[error("invalid model profile '{model}': {reason}")]
    InvalidProfile { model: String, reason: String },
    #[error("duplicate model id '{0}'")]
    DuplicateModel(String),
    #[error("invalid route request: {0}")]
    InvalidRequest(String),
    #[error("explicit model '{0}' is not in the catalog")]
    ExplicitModelNotFound(String),
    #[error("no model satisfies the route constraints")]
    NoEligibleModels { rejections: Vec<ModelRejection> },
}

/// A validated catalog. Profiles are stored by model id so iteration and diagnostics are stable.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ModelCatalog {
    profiles: Vec<ModelProfile>,
}

impl ModelCatalog {
    pub fn new(profiles: impl IntoIterator<Item = ModelProfile>) -> Result<Self, RouteError> {
        let mut profiles: Vec<_> = profiles.into_iter().collect();
        for profile in &profiles {
            profile.validate()?;
        }
        profiles.sort_by(|left, right| left.model.cmp(&right.model));
        if let Some(duplicate) = profiles
            .windows(2)
            .find(|pair| pair[0].model == pair[1].model)
        {
            return Err(RouteError::DuplicateModel(duplicate[0].model.clone()));
        }
        Ok(Self { profiles })
    }

    pub fn profiles(&self) -> impl ExactSizeIterator<Item = &ModelProfile> {
        self.profiles.iter()
    }

    /// Insert or replace one model profile while preserving canonical model-name order.
    pub fn upsert(&mut self, profile: ModelProfile) -> Result<(), RouteError> {
        profile.validate()?;
        match self
            .profiles
            .binary_search_by(|existing| existing.model.cmp(&profile.model))
        {
            Ok(index) => self.profiles[index] = profile,
            Err(index) => self.profiles.insert(index, profile),
        }
        Ok(())
    }

    pub fn route(&self, request: &RouteRequest) -> Result<RouteDecision, RouteError> {
        let required_context = request.validate()?;
        let candidates: Vec<&ModelProfile> = match &request.policy {
            RoutePolicy::Explicit { model } => vec![self
                .profiles
                .binary_search_by(|profile| profile.model.as_str().cmp(model.as_str()))
                .ok()
                .map(|index| &self.profiles[index])
                .ok_or_else(|| RouteError::ExplicitModelNotFound(model.clone()))?],
            RoutePolicy::Automatic { .. } => self.profiles.iter().collect(),
        };

        let objective = match &request.policy {
            RoutePolicy::Explicit { .. } => None,
            RoutePolicy::Automatic { objective } => Some(*objective),
        };
        let objective_needs_pricing = matches!(
            objective,
            Some(RouteObjective::Cost | RouteObjective::Balanced)
        );
        let mut eligible = Vec::new();
        let mut rejections = Vec::new();

        for profile in candidates {
            let cost = profile.pricing.map(|pricing| {
                estimate_cost_usd(
                    pricing,
                    request.estimated_input_tokens,
                    request.required_output_tokens,
                )
            });
            let mut reasons = Vec::new();

            if !request.active_providers.contains(&profile.provider) {
                reasons.push(RejectionReason::MissingCredential {
                    provider: profile.provider.clone(),
                });
            }
            reasons.extend(
                request
                    .required_skills
                    .difference(&profile.skills)
                    .cloned()
                    .map(|skill| RejectionReason::MissingSkill { skill }),
            );
            reasons.extend(
                request
                    .required_capabilities
                    .difference(&profile.capabilities)
                    .cloned()
                    .map(|capability| RejectionReason::MissingCapability { capability }),
            );
            if required_context > profile.context_window_tokens {
                reasons.push(RejectionReason::ContextWindowTooSmall {
                    required: required_context,
                    available: profile.context_window_tokens,
                });
            }
            if request.required_output_tokens > profile.max_output_tokens {
                reasons.push(RejectionReason::OutputLimitTooSmall {
                    required: request.required_output_tokens,
                    available: profile.max_output_tokens,
                });
            }
            if request.max_cost_usd.is_some() || objective_needs_pricing {
                match (cost, request.max_cost_usd) {
                    (None, _) => reasons.push(RejectionReason::UnknownPricing),
                    (Some(estimated), Some(limit)) if estimated > limit => {
                        reasons.push(RejectionReason::CostExceedsBudget { estimated, limit });
                    }
                    _ => {}
                }
            }

            if reasons.is_empty() {
                eligible.push(Candidate { profile, cost });
            } else {
                rejections.push(ModelRejection {
                    model: profile.model.clone(),
                    reasons,
                });
            }
        }

        if eligible.is_empty() {
            return Err(RouteError::NoEligibleModels { rejections });
        }

        let eligible_models = eligible.len();
        let selected = match objective {
            None => eligible.remove(0),
            Some(RouteObjective::Cost) => {
                eligible.sort_by(|left, right| {
                    left.cost
                        .expect("cost objective filtered unknown prices")
                        .total_cmp(&right.cost.expect("cost objective filtered unknown prices"))
                        .then_with(|| left.profile.model.cmp(&right.profile.model))
                });
                eligible.remove(0)
            }
            Some(RouteObjective::Quality) => {
                eligible.sort_by(|left, right| {
                    right
                        .profile
                        .quality_score
                        .cmp(&left.profile.quality_score)
                        .then_with(|| left.profile.model.cmp(&right.profile.model))
                });
                eligible.remove(0)
            }
            Some(RouteObjective::Balanced) => select_balanced(eligible),
        };

        Ok(RouteDecision {
            profile: selected.profile.clone(),
            estimated_cost_usd: selected.cost,
            policy: request.policy.clone(),
            eligible_models,
        })
    }
}

#[derive(Debug)]
struct Candidate<'a> {
    profile: &'a ModelProfile,
    cost: Option<f64>,
}

/// Equal-weight min/max normalization keeps quality and USD cost on comparable `0..=1` scales.
fn select_balanced(mut candidates: Vec<Candidate<'_>>) -> Candidate<'_> {
    let min_cost = candidates
        .iter()
        .map(|candidate| candidate.cost.expect("balanced filtered unknown prices"))
        .min_by(f64::total_cmp)
        .expect("balanced has at least one candidate");
    let max_cost = candidates
        .iter()
        .map(|candidate| candidate.cost.expect("balanced filtered unknown prices"))
        .max_by(f64::total_cmp)
        .expect("balanced has at least one candidate");
    let cost_span = max_cost - min_cost;

    candidates.sort_by(|left, right| {
        let score = |candidate: &Candidate<'_>| {
            let quality = f64::from(candidate.profile.quality_score) / 100.0;
            let cost = candidate.cost.expect("balanced filtered unknown prices");
            let cost_score = if cost_span == 0.0 {
                1.0
            } else {
                (max_cost - cost) / cost_span
            };
            (quality + cost_score) / 2.0
        };
        score(right)
            .total_cmp(&score(left))
            .then_with(|| left.profile.model.cmp(&right.profile.model))
    });
    candidates.remove(0)
}

/// Worst-case request estimate using uncached input rates. Cache rates are intentionally not used
/// because a router cannot assume a cache hit before a provider confirms one.
pub fn estimate_cost_usd(pricing: ModelPricing, input_tokens: u64, output_tokens: u64) -> f64 {
    input_tokens as f64 * pricing.input_per_million_usd / 1_000_000.0
        + output_tokens as f64 * pricing.output_per_million_usd / 1_000_000.0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pricing(input: f64, output: f64) -> ModelPricing {
        ModelPricing {
            input_per_million_usd: input,
            output_per_million_usd: output,
            cache_read_per_million_usd: None,
            cache_write_per_million_usd: None,
        }
    }

    fn model(provider: &str, name: &str, quality: u8, price: Option<f64>) -> ModelProfile {
        let mut profile = ModelProfile::new(provider, name, 100_000, 10_000, quality)
            .with_skill("general")
            .with_capability(ModelCapability::ToolUse);
        profile.pricing = price.map(|rate| pricing(rate, rate));
        profile
    }

    fn request(policy: RoutePolicy) -> RouteRequest {
        RouteRequest {
            policy,
            active_providers: ["anthropic", "deepseek", "google", "openai"]
                .into_iter()
                .map(str::to_string)
                .collect(),
            estimated_input_tokens: 1_000,
            required_output_tokens: 1_000,
            max_cost_usd: None,
            required_skills: BTreeSet::new(),
            required_capabilities: BTreeSet::new(),
        }
    }

    #[test]
    fn explicit_route_selects_the_named_model() {
        let catalog = ModelCatalog::new([
            model("deepseek", "deepseek-r1", 70, Some(1.0)),
            model("anthropic", "opus", 99, Some(20.0)),
        ])
        .unwrap();
        let decision = catalog
            .route(&request(RoutePolicy::Explicit {
                model: "deepseek-r1".into(),
            }))
            .unwrap();
        assert_eq!(decision.profile.model, "deepseek-r1");
        assert_eq!(decision.eligible_models, 1);
    }

    #[test]
    fn explicit_route_does_not_bypass_credentials_or_other_constraints() {
        let catalog = ModelCatalog::new([model("anthropic", "opus", 99, None)]).unwrap();
        let mut route = RouteRequest::explicit("opus");
        route.required_skills.insert("hard_judgment".into());
        let error = catalog.route(&route).unwrap_err();
        let RouteError::NoEligibleModels { rejections } = error else {
            panic!("unexpected error")
        };
        assert_eq!(rejections.len(), 1);
        assert_eq!(
            rejections[0].reasons,
            vec![
                RejectionReason::MissingCredential {
                    provider: "anthropic".into()
                },
                RejectionReason::MissingSkill {
                    skill: "hard_judgment".into()
                }
            ]
        );
    }

    #[test]
    fn unknown_explicit_model_is_distinct_from_ineligible() {
        let catalog = ModelCatalog::default();
        assert_eq!(
            catalog.route(&RouteRequest::explicit("missing")),
            Err(RouteError::ExplicitModelNotFound("missing".into()))
        );
    }

    #[test]
    fn automatic_route_filters_by_active_credentials() {
        let catalog = ModelCatalog::new([
            model("anthropic", "a-expensive", 90, Some(20.0)),
            model("deepseek", "d-cheap", 70, Some(1.0)),
        ])
        .unwrap();
        let mut route = RouteRequest::automatic(RouteObjective::Cost);
        route.active_providers.insert("anthropic".into());
        let decision = catalog.route(&route).unwrap();
        assert_eq!(decision.profile.model, "a-expensive");
    }

    #[test]
    fn required_skills_and_capabilities_use_all_of_semantics() {
        let capable = model("openai", "capable", 80, Some(2.0))
            .with_skill("coding")
            .with_capability(ModelCapability::Vision);
        let cheap = model("deepseek", "cheap", 80, Some(1.0)).with_skill("coding");
        let catalog = ModelCatalog::new([cheap, capable]).unwrap();
        let mut route = request(RoutePolicy::Automatic {
            objective: RouteObjective::Cost,
        });
        route.required_skills = ["coding", "general"]
            .into_iter()
            .map(str::to_string)
            .collect();
        route.required_capabilities = [ModelCapability::ToolUse, ModelCapability::Vision]
            .into_iter()
            .collect();
        assert_eq!(catalog.route(&route).unwrap().profile.model, "capable");
    }

    #[test]
    fn context_counts_input_plus_output_and_output_has_its_own_limit() {
        let context_too_small = ModelProfile::new("openai", "context-small", 5_000, 4_000, 90)
            .with_pricing(pricing(1.0, 1.0));
        let output_too_small = ModelProfile::new("google", "output-small", 20_000, 1_000, 90)
            .with_pricing(pricing(1.0, 1.0));
        let fits = ModelProfile::new("anthropic", "fits", 20_000, 4_000, 80)
            .with_pricing(pricing(2.0, 2.0));
        let catalog = ModelCatalog::new([context_too_small, output_too_small, fits]).unwrap();
        let mut route = request(RoutePolicy::Automatic {
            objective: RouteObjective::Cost,
        });
        route.estimated_input_tokens = 4_000;
        route.required_output_tokens = 2_000;
        assert_eq!(catalog.route(&route).unwrap().profile.model, "fits");
    }

    #[test]
    fn usd_budget_filters_expensive_models() {
        let catalog = ModelCatalog::new([
            model("anthropic", "expensive", 100, Some(100.0)),
            model("deepseek", "within-budget", 60, Some(1.0)),
        ])
        .unwrap();
        let mut route = request(RoutePolicy::Automatic {
            objective: RouteObjective::Quality,
        });
        route.max_cost_usd = Some(0.01);
        assert_eq!(
            catalog.route(&route).unwrap().profile.model,
            "within-budget"
        );
    }

    #[test]
    fn budget_boundary_is_inclusive() {
        let catalog = ModelCatalog::new([model("openai", "exact", 80, Some(5.0))]).unwrap();
        let mut route = request(RoutePolicy::Automatic {
            objective: RouteObjective::Quality,
        });
        route.max_cost_usd = Some(0.01);
        assert_eq!(
            catalog.route(&route).unwrap().estimated_cost_usd,
            Some(0.01)
        );
    }

    #[test]
    fn unknown_pricing_fails_closed_under_usd_budget_even_for_quality_and_explicit() {
        let catalog = ModelCatalog::new([model("anthropic", "unknown", 100, None)]).unwrap();
        for mut route in [
            request(RoutePolicy::Automatic {
                objective: RouteObjective::Quality,
            }),
            request(RoutePolicy::Explicit {
                model: "unknown".into(),
            }),
        ] {
            route.max_cost_usd = Some(10.0);
            let RouteError::NoEligibleModels { rejections } = catalog.route(&route).unwrap_err()
            else {
                panic!("unknown price must fail closed")
            };
            assert!(rejections[0]
                .reasons
                .contains(&RejectionReason::UnknownPricing));
        }
    }

    #[test]
    fn cost_and_balanced_objectives_also_require_known_pricing_without_a_budget() {
        let catalog = ModelCatalog::new([
            model("anthropic", "unknown", 100, None),
            model("deepseek", "known", 60, Some(1.0)),
        ])
        .unwrap();
        for objective in [RouteObjective::Cost, RouteObjective::Balanced] {
            assert_eq!(
                catalog
                    .route(&request(RoutePolicy::Automatic { objective }))
                    .unwrap()
                    .profile
                    .model,
                "known"
            );
        }
    }

    #[test]
    fn quality_objective_allows_unknown_pricing_when_no_usd_cap_exists() {
        let catalog = ModelCatalog::new([
            model("anthropic", "highest", 100, None),
            model("deepseek", "known", 60, Some(1.0)),
        ])
        .unwrap();
        let decision = catalog
            .route(&request(RoutePolicy::Automatic {
                objective: RouteObjective::Quality,
            }))
            .unwrap();
        assert_eq!(decision.profile.model, "highest");
        assert_eq!(decision.estimated_cost_usd, None);
    }

    #[test]
    fn cost_objective_selects_lowest_estimated_request_cost() {
        let catalog = ModelCatalog::new([
            model("anthropic", "costly", 100, Some(10.0)),
            model("deepseek", "cheap", 50, Some(0.5)),
        ])
        .unwrap();
        assert_eq!(
            catalog
                .route(&request(RoutePolicy::Automatic {
                    objective: RouteObjective::Cost
                }))
                .unwrap()
                .profile
                .model,
            "cheap"
        );
    }

    #[test]
    fn quality_objective_selects_highest_score() {
        let catalog = ModelCatalog::new([
            model("anthropic", "best", 99, Some(20.0)),
            model("deepseek", "cheap", 60, Some(0.5)),
        ])
        .unwrap();
        assert_eq!(
            catalog
                .route(&request(RoutePolicy::Automatic {
                    objective: RouteObjective::Quality
                }))
                .unwrap()
                .profile
                .model,
            "best"
        );
    }

    #[test]
    fn balanced_uses_equal_weight_normalized_cost_and_quality() {
        let catalog = ModelCatalog::new([
            model("anthropic", "premium", 100, Some(10.0)),
            model("deepseek", "budget", 30, Some(1.0)),
            model("openai", "middle", 90, Some(4.0)),
        ])
        .unwrap();
        let decision = catalog
            .route(&request(RoutePolicy::Automatic {
                objective: RouteObjective::Balanced,
            }))
            .unwrap();
        assert_eq!(decision.profile.model, "middle");
    }

    #[test]
    fn model_name_is_final_tie_breaker_regardless_of_insertion_order() {
        let a = model("openai", "alpha", 80, Some(2.0));
        let z = model("google", "zeta", 80, Some(2.0));
        for catalog in [
            ModelCatalog::new([z.clone(), a.clone()]).unwrap(),
            ModelCatalog::new([a.clone(), z.clone()]).unwrap(),
        ] {
            for objective in [
                RouteObjective::Cost,
                RouteObjective::Quality,
                RouteObjective::Balanced,
            ] {
                assert_eq!(
                    catalog
                        .route(&request(RoutePolicy::Automatic { objective }))
                        .unwrap()
                        .profile
                        .model,
                    "alpha"
                );
            }
        }
    }

    #[test]
    fn diagnostics_are_stably_sorted_by_model_and_requirement() {
        let catalog = ModelCatalog::new([
            model("openai", "zeta", 80, Some(1.0)),
            model("anthropic", "alpha", 80, Some(1.0)),
        ])
        .unwrap();
        let mut route = RouteRequest::automatic(RouteObjective::Quality);
        route.required_skills = ["z-skill", "a-skill"]
            .into_iter()
            .map(str::to_string)
            .collect();
        let RouteError::NoEligibleModels { rejections } = catalog.route(&route).unwrap_err() else {
            panic!("expected rejection diagnostics")
        };
        assert_eq!(
            rejections
                .iter()
                .map(|rejection| rejection.model.as_str())
                .collect::<Vec<_>>(),
            vec!["alpha", "zeta"]
        );
        assert!(matches!(
            rejections[0].reasons.as_slice(),
            [
                RejectionReason::MissingCredential { .. },
                RejectionReason::MissingSkill { skill: first },
                RejectionReason::MissingSkill { skill: second }
            ] if first == "a-skill" && second == "z-skill"
        ));
    }

    #[test]
    fn invalid_catalog_profiles_and_duplicate_ids_are_rejected() {
        let bad_quality = ModelProfile::new("openai", "bad-quality", 10, 1, 101);
        assert!(matches!(
            ModelCatalog::new([bad_quality]),
            Err(RouteError::InvalidProfile { .. })
        ));

        let bad_price = model("openai", "bad-price", 50, Some(f64::NAN));
        assert!(matches!(
            ModelCatalog::new([bad_price]),
            Err(RouteError::InvalidProfile { .. })
        ));

        let duplicate = model("openai", "same", 50, Some(1.0));
        assert_eq!(
            ModelCatalog::new([duplicate.clone(), duplicate]),
            Err(RouteError::DuplicateModel("same".into()))
        );
    }

    #[test]
    fn invalid_budget_and_context_overflow_are_rejected_before_selection() {
        let catalog = ModelCatalog::new([model("openai", "valid", 80, Some(1.0))]).unwrap();
        let mut bad_budget = request(RoutePolicy::Automatic {
            objective: RouteObjective::Quality,
        });
        bad_budget.max_cost_usd = Some(-1.0);
        assert!(matches!(
            catalog.route(&bad_budget),
            Err(RouteError::InvalidRequest(_))
        ));

        let mut overflow = bad_budget;
        overflow.max_cost_usd = None;
        overflow.estimated_input_tokens = u64::MAX;
        overflow.required_output_tokens = 1;
        assert!(matches!(
            catalog.route(&overflow),
            Err(RouteError::InvalidRequest(_))
        ));
    }

    #[test]
    fn upsert_replaces_without_changing_canonical_order() {
        let mut catalog = ModelCatalog::new([
            model("openai", "beta", 50, Some(2.0)),
            model("openai", "alpha", 50, Some(1.0)),
        ])
        .unwrap();
        catalog
            .upsert(model("openai", "alpha", 99, Some(1.0)))
            .unwrap();
        assert_eq!(
            catalog
                .profiles()
                .map(|profile| (profile.model.as_str(), profile.quality_score))
                .collect::<Vec<_>>(),
            vec![("alpha", 99), ("beta", 50)]
        );
    }

    #[test]
    fn estimate_uses_input_and_output_rates() {
        assert_eq!(estimate_cost_usd(pricing(2.0, 6.0), 500_000, 250_000), 2.5);
    }
}
