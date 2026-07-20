//! Verified skill packages. Loading is data-only; executable hooks need a separate policy grant.

use super::contracts::{
    sha256_digest, GovernanceContractError, PolicyEffect, SandboxProfile, SkillManifest,
};
use super::sandbox::{Sandbox, SandboxError};
use crate::durability::stable_input_hash;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::io::Read;
use std::path::Path;
use thiserror::Error;

pub const MAX_SKILL_FILE_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_SKILL_TOTAL_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SkillExecutionMode {
    PromptData,
    ExecutableHook,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SkillSourcePin {
    pub source: String,
    pub revision: String,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SkillPackage {
    pub manifest: SkillManifest,
    pub source: SkillSourcePin,
    pub execution_mode: SkillExecutionMode,
    #[serde(default)]
    pub requested_permissions: BTreeSet<String>,
    pub package_hash: String,
}

#[derive(Serialize)]
struct SkillPackagePayload<'a> {
    manifest: &'a SkillManifest,
    source: &'a SkillSourcePin,
    execution_mode: SkillExecutionMode,
    requested_permissions: &'a BTreeSet<String>,
}

impl SkillPackage {
    pub fn seal(
        manifest: SkillManifest,
        source: SkillSourcePin,
        execution_mode: SkillExecutionMode,
        requested_permissions: BTreeSet<String>,
    ) -> Result<Self, SkillLoadError> {
        let mut package = Self {
            manifest,
            source,
            execution_mode,
            requested_permissions,
            package_hash: String::new(),
        };
        package.validate_shape()?;
        package.package_hash = package.computed_hash()?;
        Ok(package)
    }

    pub fn computed_hash(&self) -> Result<String, SkillLoadError> {
        let value = serde_json::to_value(SkillPackagePayload {
            manifest: &self.manifest,
            source: &self.source,
            execution_mode: self.execution_mode,
            requested_permissions: &self.requested_permissions,
        })
        .map_err(|error| SkillLoadError::InvalidPackage(error.to_string()))?;
        Ok(stable_input_hash(&value))
    }

    pub fn validate_integrity(&self) -> Result<(), SkillLoadError> {
        self.validate_shape()?;
        let actual = self.computed_hash()?;
        if actual != self.package_hash {
            return Err(SkillLoadError::PackageDrift {
                expected: self.package_hash.clone(),
                actual,
            });
        }
        Ok(())
    }

    fn validate_shape(&self) -> Result<(), SkillLoadError> {
        self.manifest.validate_integrity()?;
        if self.source.source.trim().is_empty()
            || self.source.revision.trim().is_empty()
            || !is_sha256(&self.source.sha256)
            || self
                .requested_permissions
                .iter()
                .any(|permission| permission.trim().is_empty())
        {
            return Err(SkillLoadError::InvalidPackage(
                "source, revision, digest, and permissions must be pinned".into(),
            ));
        }
        if self.execution_mode == SkillExecutionMode::PromptData
            && !self.requested_permissions.is_empty()
        {
            return Err(SkillLoadError::InvalidPackage(
                "prompt/data skills cannot request executable permissions".into(),
            ));
        }
        if self.execution_mode == SkillExecutionMode::ExecutableHook
            && self.requested_permissions.is_empty()
        {
            return Err(SkillLoadError::InvalidPackage(
                "executable hooks must declare at least one permission".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct SkillInspectionPolicy {
    /// Trusted canonical names. A near-match that is not exact is rejected as a likely typosquat.
    pub trusted_names: BTreeSet<String>,
    pub max_file_bytes: usize,
    pub max_total_bytes: usize,
}

impl Default for SkillInspectionPolicy {
    fn default() -> Self {
        Self {
            trusted_names: BTreeSet::new(),
            max_file_bytes: MAX_SKILL_FILE_BYTES,
            max_total_bytes: MAX_SKILL_TOTAL_BYTES,
        }
    }
}

#[derive(Debug, Clone)]
pub struct LoadedSkill {
    pub package: SkillPackage,
    pub entrypoint: Vec<u8>,
    pub artifacts: BTreeMap<String, Vec<u8>>,
}

impl LoadedSkill {
    pub fn prompt(&self) -> Result<&str, SkillLoadError> {
        std::str::from_utf8(&self.entrypoint)
            .map_err(|_| SkillLoadError::HiddenInstruction("entrypoint is not UTF-8".into()))
    }
}

#[derive(Debug, Clone)]
pub struct ExecutableSkillGrant {
    package_hash: String,
    permissions: BTreeSet<String>,
}

impl ExecutableSkillGrant {
    pub fn package_hash(&self) -> &str {
        &self.package_hash
    }

    pub fn permissions(&self) -> &BTreeSet<String> {
        &self.permissions
    }
}

/// Grant an executable hook only after the caller has evaluated the pinned policy snapshot.
pub fn authorize_executable_skill(
    package: &SkillPackage,
    policy_effect: PolicyEffect,
    granted_permissions: &BTreeSet<String>,
    sandbox: &SandboxProfile,
) -> Result<ExecutableSkillGrant, SkillLoadError> {
    package.validate_integrity()?;
    sandbox.validate()?;
    if package.execution_mode != SkillExecutionMode::ExecutableHook {
        return Err(SkillLoadError::ExecutionDenied("skill is data-only".into()));
    }
    if policy_effect != PolicyEffect::Allow {
        return Err(SkillLoadError::ExecutionDenied(
            "policy did not explicitly allow execution".into(),
        ));
    }
    if !sandbox.require_os_containment
        || sandbox.allow_unsandboxed_fallback
        || sandbox.allow_privilege_escalation
    {
        return Err(SkillLoadError::ExecutionDenied(
            "executable hooks require fail-closed OS containment".into(),
        ));
    }
    if !package.requested_permissions.is_subset(granted_permissions) {
        return Err(SkillLoadError::ExecutionDenied(
            "not all manifest permissions were granted".into(),
        ));
    }
    Ok(ExecutableSkillGrant {
        package_hash: package.package_hash.clone(),
        permissions: package.requested_permissions.clone(),
    })
}

pub struct SkillLoader {
    sandbox: Sandbox,
    policy: SkillInspectionPolicy,
}

impl SkillLoader {
    pub fn new(
        root: impl AsRef<Path>,
        policy: SkillInspectionPolicy,
    ) -> Result<Self, SkillLoadError> {
        if policy.max_file_bytes == 0
            || policy.max_total_bytes == 0
            || policy.max_file_bytes > policy.max_total_bytes
        {
            return Err(SkillLoadError::InvalidPackage(
                "skill byte limits are invalid".into(),
            ));
        }
        Ok(Self {
            sandbox: Sandbox::jail(root)?,
            policy,
        })
    }

    pub fn load(&self, package: SkillPackage) -> Result<LoadedSkill, SkillLoadError> {
        package.validate_integrity()?;
        self.reject_typosquat(&package.manifest.name)?;

        let entrypoint_digest = package
            .manifest
            .artifacts
            .get(&package.manifest.entrypoint)
            .ok_or_else(|| {
                SkillLoadError::UnexpectedArtifact(
                    "entrypoint must be present in the pinned artifacts map".into(),
                )
            })?;

        let mut total = 0usize;
        let mut artifacts = BTreeMap::new();
        for (path, expected_digest) in &package.manifest.artifacts {
            let bytes = self.read_bounded(path)?;
            total = total
                .checked_add(bytes.len())
                .ok_or(SkillLoadError::TooLarge)?;
            if total > self.policy.max_total_bytes {
                return Err(SkillLoadError::TooLarge);
            }
            let actual = sha256_digest(&bytes);
            if actual != *expected_digest {
                return Err(SkillLoadError::ArtifactDrift {
                    path: path.clone(),
                    expected: expected_digest.clone(),
                    actual,
                });
            }
            self.inspect_artifact(path, &bytes, package.execution_mode)?;
            artifacts.insert(path.clone(), bytes);
        }

        let entrypoint = artifacts
            .get(&package.manifest.entrypoint)
            .expect("entrypoint was required above")
            .clone();
        debug_assert_eq!(sha256_digest(&entrypoint), *entrypoint_digest);
        Ok(LoadedSkill {
            package,
            entrypoint,
            artifacts,
        })
    }

    fn read_bounded(&self, path: &str) -> Result<Vec<u8>, SkillLoadError> {
        let mut file = self.sandbox.open_read(path)?;
        let limit = self.policy.max_file_bytes.saturating_add(1) as u64;
        let mut bytes = Vec::new();
        file.by_ref()
            .take(limit)
            .read_to_end(&mut bytes)
            .map_err(|error| SkillLoadError::Io(error.to_string()))?;
        if bytes.len() > self.policy.max_file_bytes {
            return Err(SkillLoadError::TooLarge);
        }
        Ok(bytes)
    }

    fn reject_typosquat(&self, name: &str) -> Result<(), SkillLoadError> {
        let normalized = normalize_name(name);
        for trusted in &self.policy.trusted_names {
            let trusted_normalized = normalize_name(trusted);
            if normalized != trusted_normalized
                && levenshtein(&normalized, &trusted_normalized) <= 2
            {
                return Err(SkillLoadError::Typosquat {
                    requested: name.into(),
                    trusted: trusted.clone(),
                });
            }
        }
        Ok(())
    }

    fn inspect_artifact(
        &self,
        path: &str,
        bytes: &[u8],
        mode: SkillExecutionMode,
    ) -> Result<(), SkillLoadError> {
        if mode == SkillExecutionMode::PromptData && looks_executable(path, bytes) {
            return Err(SkillLoadError::UnexpectedExecutable(path.into()));
        }
        if let Ok(text) = std::str::from_utf8(bytes) {
            if let Some(character) = text.chars().find(|character| hidden_character(*character)) {
                return Err(SkillLoadError::HiddenInstruction(format!(
                    "{path} contains hidden/control character U+{:04X}",
                    character as u32
                )));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum SkillLoadError {
    #[error("invalid skill package: {0}")]
    InvalidPackage(String),
    #[error("skill package drift: expected {expected}, found {actual}")]
    PackageDrift { expected: String, actual: String },
    #[error("skill artifact `{path}` drift: expected {expected}, found {actual}")]
    ArtifactDrift {
        path: String,
        expected: String,
        actual: String,
    },
    #[error("skill artifact is too large")]
    TooLarge,
    #[error("unexpected skill artifact: {0}")]
    UnexpectedArtifact(String),
    #[error("unexpected executable artifact: {0}")]
    UnexpectedExecutable(String),
    #[error("hidden skill instruction rejected: {0}")]
    HiddenInstruction(String),
    #[error("possible skill typosquat `{requested}` near trusted `{trusted}`")]
    Typosquat { requested: String, trusted: String },
    #[error("skill execution denied: {0}")]
    ExecutionDenied(String),
    #[error("skill I/O failed: {0}")]
    Io(String),
    #[error(transparent)]
    Sandbox(#[from] SandboxError),
    #[error(transparent)]
    Governance(#[from] GovernanceContractError),
}

fn is_sha256(value: &str) -> bool {
    value.strip_prefix("sha256:").is_some_and(|digest| {
        digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
    })
}

fn normalize_name(value: &str) -> String {
    value
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect()
}

fn levenshtein(left: &str, right: &str) -> usize {
    let mut previous: Vec<usize> = (0..=right.chars().count()).collect();
    for (left_index, left_char) in left.chars().enumerate() {
        let mut current = vec![left_index + 1];
        for (right_index, right_char) in right.chars().enumerate() {
            let insertion = current[right_index] + 1;
            let deletion = previous[right_index + 1] + 1;
            let substitution = previous[right_index] + usize::from(left_char != right_char);
            current.push(insertion.min(deletion).min(substitution));
        }
        previous = current;
    }
    previous.last().copied().unwrap_or(left.len())
}

fn looks_executable(path: &str, bytes: &[u8]) -> bool {
    const EXECUTABLE_EXTENSIONS: &[&str] = &[
        "sh", "bash", "zsh", "py", "js", "mjs", "cjs", "exe", "dll", "so", "dylib", "bat", "cmd",
        "ps1",
    ];
    let extension_is_executable = Path::new(path)
        .extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| {
            EXECUTABLE_EXTENSIONS
                .iter()
                .any(|known| extension.eq_ignore_ascii_case(known))
        });
    extension_is_executable
        || bytes.starts_with(b"#!")
        || bytes.starts_with(b"\x7fELF")
        || bytes.starts_with(b"MZ")
        || bytes.starts_with(&[0xcf, 0xfa, 0xed, 0xfe])
        || bytes.starts_with(&[0xfe, 0xed, 0xfa, 0xcf])
}

fn hidden_character(character: char) -> bool {
    matches!(
        character,
        '\u{200b}'
            | '\u{200c}'
            | '\u{200d}'
            | '\u{202a}'..='\u{202e}'
            | '\u{2066}'..='\u{2069}'
            | '\u{feff}'
    ) || (character.is_control() && !matches!(character, '\n' | '\r' | '\t'))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn sealed_package(
        root: &Path,
        name: &str,
        bytes: &[u8],
        mode: SkillExecutionMode,
    ) -> SkillPackage {
        fs::write(root.join("SKILL.md"), bytes).unwrap();
        let mut artifacts = BTreeMap::new();
        artifacts.insert("SKILL.md".into(), sha256_digest(bytes));
        let manifest =
            SkillManifest::seal(name, "1.0.0", "test", "SKILL.md", vec![], artifacts).unwrap();
        let permissions = if mode == SkillExecutionMode::ExecutableHook {
            BTreeSet::from(["process:spawn".into()])
        } else {
            BTreeSet::new()
        };
        SkillPackage::seal(
            manifest,
            SkillSourcePin {
                source: "https://example.test/skill".into(),
                revision: "deadbeef".into(),
                sha256: sha256_digest(b"source archive"),
            },
            mode,
            permissions,
        )
        .unwrap()
    }

    #[test]
    fn prompt_skill_loads_only_when_every_byte_matches() {
        let root = tempfile::tempdir().unwrap();
        let package = sealed_package(
            root.path(),
            "reviewer",
            b"Review this input safely.",
            SkillExecutionMode::PromptData,
        );
        let loaded = SkillLoader::new(root.path(), SkillInspectionPolicy::default())
            .unwrap()
            .load(package)
            .unwrap();
        assert_eq!(loaded.prompt().unwrap(), "Review this input safely.");
    }

    #[test]
    fn artifact_drift_and_hidden_unicode_fail_closed() {
        let root = tempfile::tempdir().unwrap();
        let package = sealed_package(
            root.path(),
            "reviewer",
            b"safe",
            SkillExecutionMode::PromptData,
        );
        fs::write(root.path().join("SKILL.md"), b"changed").unwrap();
        assert!(matches!(
            SkillLoader::new(root.path(), SkillInspectionPolicy::default())
                .unwrap()
                .load(package),
            Err(SkillLoadError::ArtifactDrift { .. })
        ));

        let hidden = "visible\u{202e}hidden".as_bytes();
        let package = sealed_package(
            root.path(),
            "reviewer",
            hidden,
            SkillExecutionMode::PromptData,
        );
        assert!(matches!(
            SkillLoader::new(root.path(), SkillInspectionPolicy::default())
                .unwrap()
                .load(package),
            Err(SkillLoadError::HiddenInstruction(_))
        ));
    }

    #[test]
    fn prompt_skill_rejects_executable_and_typosquat() {
        let root = tempfile::tempdir().unwrap();
        let executable = sealed_package(
            root.path(),
            "code-revewer",
            b"#!/bin/sh\nexit 0\n",
            SkillExecutionMode::PromptData,
        );
        let policy = SkillInspectionPolicy {
            trusted_names: BTreeSet::from(["code-reviewer".into()]),
            ..SkillInspectionPolicy::default()
        };
        assert!(matches!(
            SkillLoader::new(root.path(), policy)
                .unwrap()
                .load(executable),
            Err(SkillLoadError::Typosquat { .. })
        ));
    }

    #[test]
    fn executable_needs_policy_permissions_and_fail_closed_containment() {
        let root = tempfile::tempdir().unwrap();
        let package = sealed_package(
            root.path(),
            "hook",
            b"#!/bin/sh\nexit 0\n",
            SkillExecutionMode::ExecutableHook,
        );
        let permissions = BTreeSet::from(["process:spawn".into()]);
        assert!(authorize_executable_skill(
            &package,
            PolicyEffect::Deny,
            &permissions,
            &SandboxProfile::hardened()
        )
        .is_err());
        let grant = authorize_executable_skill(
            &package,
            PolicyEffect::Allow,
            &permissions,
            &SandboxProfile::hardened(),
        )
        .unwrap();
        assert_eq!(grant.package_hash(), package.package_hash);
    }
}
