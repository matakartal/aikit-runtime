//! Versioned, offline model catalog with immutable shipped data and explicit override layering.
//!
//! The catalog is compiled into the crate. It never performs network I/O at runtime and missing
//! capability facts remain `unknown`. User overrides produce a separate resolved catalog instead
//! of mutating the shipped snapshot, so audits can always identify the original schema hash.

use crate::durability::stable_input_hash;
use crate::routing::{ModelCatalog, ModelProfile, RouteError};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use thiserror::Error;

pub const MODEL_CATALOG_SCHEMA_VERSION: u32 = 1;
pub const SHIPPED_MODEL_CATALOG_VERSION: &str = "2026-07-20.v1";
pub const SHIPPED_MODEL_CATALOG_JSON: &str =
    include_str!("../catalog/model-catalog-v0.3-alpha.json");

// Updated only when the reviewed, compiled-in snapshot changes. The hash is over canonical JSON,
// not file whitespace, and is also exported for Rust/Python/TypeScript release parity checks.
pub const SHIPPED_MODEL_CATALOG_HASH: &str =
    "sha256:5d99da41008d4eea60ca4e1bcaefd2deb72b7a9ab1f7ef6ee68bf6cd158e1fd1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogSource {
    pub provider: String,
    pub reference: String,
    pub url: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelCatalogSnapshot {
    pub schema_version: u32,
    pub catalog_version: String,
    pub verified_at: String,
    pub sources: Vec<CatalogSource>,
    pub profiles: Vec<ModelProfile>,
}

impl ModelCatalogSnapshot {
    pub fn from_json(json: &str) -> Result<Self, ModelCatalogError> {
        let snapshot: Self = serde_json::from_str(json)
            .map_err(|error| ModelCatalogError::InvalidJson(error.to_string()))?;
        snapshot.validate()?;
        Ok(snapshot)
    }

    /// Load the reviewed snapshot compiled into this exact crate version.
    pub fn shipped() -> Result<Self, ModelCatalogError> {
        let snapshot = Self::from_json(SHIPPED_MODEL_CATALOG_JSON)?;
        let actual = snapshot.snapshot_hash()?;
        if actual != SHIPPED_MODEL_CATALOG_HASH {
            return Err(ModelCatalogError::IntegrityMismatch {
                expected: SHIPPED_MODEL_CATALOG_HASH.into(),
                actual,
            });
        }
        Ok(snapshot)
    }

    pub fn snapshot_hash(&self) -> Result<String, ModelCatalogError> {
        let value = serde_json::to_value(self)
            .map_err(|error| ModelCatalogError::InvalidJson(error.to_string()))?;
        Ok(stable_input_hash(&value))
    }

    pub fn routing_catalog(&self) -> Result<ModelCatalog, ModelCatalogError> {
        ModelCatalog::new(self.profiles.clone()).map_err(ModelCatalogError::InvalidProfile)
    }

    pub fn source_for(&self, provider: &str) -> Option<&CatalogSource> {
        self.sources
            .iter()
            .find(|source| source.provider == provider)
    }

    fn validate(&self) -> Result<(), ModelCatalogError> {
        if self.schema_version != MODEL_CATALOG_SCHEMA_VERSION {
            return Err(ModelCatalogError::UnsupportedSchema {
                expected: MODEL_CATALOG_SCHEMA_VERSION,
                actual: self.schema_version,
            });
        }
        if self.catalog_version.trim().is_empty() || self.verified_at.trim().is_empty() {
            return Err(ModelCatalogError::InvalidMetadata);
        }
        self.routing_catalog()?;

        let mut providers = BTreeSet::new();
        for source in &self.sources {
            if source.provider.trim().is_empty()
                || source.reference.trim().is_empty()
                || !(source.url.starts_with("https://") || source.url.starts_with("http://"))
                || !providers.insert(source.provider.as_str())
            {
                return Err(ModelCatalogError::InvalidSource(source.provider.clone()));
            }
        }
        for profile in &self.profiles {
            if !providers.contains(profile.provider.as_str()) {
                return Err(ModelCatalogError::MissingSource(profile.provider.clone()));
            }
        }
        Ok(())
    }
}

/// A caller-owned layer. Overrides are validated but never written back into the shipped snapshot.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelCatalogOverrides {
    #[serde(default)]
    pub profiles: Vec<ModelProfile>,
}

impl ModelCatalogOverrides {
    pub fn resolve(
        &self,
        shipped: &ModelCatalogSnapshot,
    ) -> Result<ResolvedModelCatalog, ModelCatalogError> {
        let shipped_hash = shipped.snapshot_hash()?;
        let mut catalog = shipped.routing_catalog()?;
        let mut seen = BTreeSet::new();
        for profile in &self.profiles {
            if !seen.insert(profile.model.as_str()) {
                return Err(ModelCatalogError::DuplicateOverride(profile.model.clone()));
            }
            catalog
                .upsert(profile.clone())
                .map_err(ModelCatalogError::InvalidProfile)?;
        }
        let override_value = serde_json::to_value(self)
            .map_err(|error| ModelCatalogError::InvalidJson(error.to_string()))?;
        Ok(ResolvedModelCatalog {
            catalog,
            shipped_hash,
            overrides_hash: stable_input_hash(&override_value),
            override_count: self.profiles.len(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct ResolvedModelCatalog {
    catalog: ModelCatalog,
    pub shipped_hash: String,
    pub overrides_hash: String,
    pub override_count: usize,
}

impl ResolvedModelCatalog {
    pub fn catalog(&self) -> &ModelCatalog {
        &self.catalog
    }

    pub fn into_catalog(self) -> ModelCatalog {
        self.catalog
    }
}

#[derive(Debug, Error)]
pub enum ModelCatalogError {
    #[error("model catalog JSON is invalid: {0}")]
    InvalidJson(String),
    #[error("model catalog schema {actual} is unsupported; expected {expected}")]
    UnsupportedSchema { expected: u32, actual: u32 },
    #[error("model catalog metadata is incomplete")]
    InvalidMetadata,
    #[error("model catalog source for `{0}` is invalid or duplicated")]
    InvalidSource(String),
    #[error("model catalog profile provider `{0}` has no source record")]
    MissingSource(String),
    #[error("model catalog profile is invalid: {0}")]
    InvalidProfile(RouteError),
    #[error("model catalog integrity mismatch: expected {expected}, got {actual}")]
    IntegrityMismatch { expected: String, actual: String },
    #[error("model override `{0}` is duplicated")]
    DuplicateOverride(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CapabilityState, ModelCapability};

    #[test]
    fn shipped_catalog_is_offline_versioned_and_covers_exactly_eight_providers() {
        let snapshot = ModelCatalogSnapshot::from_json(SHIPPED_MODEL_CATALOG_JSON).unwrap();
        assert_eq!(snapshot.catalog_version, SHIPPED_MODEL_CATALOG_VERSION);
        assert_eq!(snapshot.sources.len(), 8);
        assert_eq!(snapshot.profiles.len(), 8);
        assert_eq!(
            snapshot.snapshot_hash().unwrap(),
            SHIPPED_MODEL_CATALOG_HASH
        );
    }

    #[test]
    fn unknown_and_unsupported_remain_distinct() {
        let snapshot = ModelCatalogSnapshot::from_json(SHIPPED_MODEL_CATALOG_JSON).unwrap();
        let mistral = snapshot
            .profiles
            .iter()
            .find(|profile| profile.provider == "mistral")
            .unwrap();
        assert_eq!(
            mistral.capability_state(&ModelCapability::Reasoning),
            CapabilityState::Unknown
        );
        assert_eq!(
            mistral.capability_state(&ModelCapability::ImageGeneration),
            CapabilityState::Unsupported
        );
    }

    #[test]
    fn overrides_are_separate_and_do_not_mutate_shipped_snapshot() {
        let snapshot = ModelCatalogSnapshot::from_json(SHIPPED_MODEL_CATALOG_JSON).unwrap();
        let before = snapshot.snapshot_hash().unwrap();
        let mut profile = snapshot.profiles[0].clone();
        profile.max_output_tokens -= 1;
        let resolved = ModelCatalogOverrides {
            profiles: vec![profile],
        }
        .resolve(&snapshot)
        .unwrap();
        assert_eq!(snapshot.snapshot_hash().unwrap(), before);
        assert_eq!(resolved.shipped_hash, before);
        assert_ne!(resolved.shipped_hash, resolved.overrides_hash);
        assert_eq!(resolved.override_count, 1);
    }

    #[test]
    fn duplicate_override_fails_closed() {
        let snapshot = ModelCatalogSnapshot::from_json(SHIPPED_MODEL_CATALOG_JSON).unwrap();
        let profile = snapshot.profiles[0].clone();
        let error = ModelCatalogOverrides {
            profiles: vec![profile.clone(), profile],
        }
        .resolve(&snapshot)
        .unwrap_err();
        assert!(matches!(error, ModelCatalogError::DuplicateOverride(_)));
    }
}
