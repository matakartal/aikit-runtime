//! Signed compatibility manifests and side-by-side managed runtime artifacts.

use super::{
    ExternalAgentError, ExternalAgentErrorCode, ExternalAgentKind, ExternalAgentResult,
    RuntimeCandidate, RuntimeDiscoverySource, RuntimeInstallOrigin, RuntimeInstanceId,
    RuntimeProfile, EXTERNAL_AGENT_CONTRACT_VERSION,
};
use base64::Engine as _;
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use semver::Version;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};

pub const UPDATE_MANIFEST_SCHEMA_VERSION: u32 = 1;
pub const UPDATE_MANIFEST_MAX_BYTES: usize = 2 * 1024 * 1024;
pub const UPDATE_ARTIFACT_MAX_BYTES: u64 = 2 * 1024 * 1024 * 1024;
pub const UPDATE_CLOCK_SKEW_MS: u64 = 5 * 60 * 1_000;
pub const MANIFEST_REFRESH_TTL_MS: u64 = 24 * 60 * 60 * 1_000;
pub const MODEL_CATALOG_REFRESH_TTL_MS: u64 = 6 * 60 * 60 * 1_000;
pub const DISCOVERY_REFRESH_TTL_MS: u64 = 60 * 60 * 1_000;

const RETRY_DELAYS_MS: [u64; 3] = [60 * 60 * 1_000, 6 * 60 * 60 * 1_000, 24 * 60 * 60 * 1_000];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeUpdateChannel {
    Stable,
    Preview,
}

impl RuntimeUpdateChannel {
    fn rank(self) -> u8 {
        match self {
            Self::Stable => 0,
            Self::Preview => 1,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeUpdatePolicy {
    /// Discover and report versions without mutating the vendor installation.
    CheckOnly,
    /// The vendor's own updater may be invoked only through an explicit host action.
    VendorManaged,
    /// AIKit installs verified artifacts side-by-side and switches an activation pointer.
    AikitManaged,
}

impl RuntimeUpdatePolicy {
    pub const fn default_for(profile: RuntimeProfile) -> Self {
        match profile {
            RuntimeProfile::External => Self::CheckOnly,
            RuntimeProfile::Managed => Self::AikitManaged,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeUpdateManifest {
    pub schema_version: u32,
    pub generated_unix_ms: u64,
    pub expires_unix_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub minimum_aikit_version: Option<Version>,
    pub releases: Vec<RuntimeRelease>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeRelease {
    pub kind: ExternalAgentKind,
    pub channel: RuntimeUpdateChannel,
    pub version: Version,
    pub protocol: String,
    pub contract_version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub minimum_aikit_version: Option<Version>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub maximum_aikit_version: Option<Version>,
    pub artifacts: Vec<RuntimeArtifact>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeArtifact {
    pub target_os: String,
    pub target_arch: String,
    pub url: String,
    /// Lowercase hexadecimal SHA-256 without a prefix.
    pub sha256: String,
    pub size_bytes: u64,
    pub executable_name: String,
}

#[derive(Debug, Clone, Default)]
pub struct TrustedManifestKeys {
    keys: BTreeMap<String, VerifyingKey>,
}

impl TrustedManifestKeys {
    pub fn insert(
        &mut self,
        key_id: impl Into<String>,
        public_key: [u8; 32],
    ) -> ExternalAgentResult<()> {
        let key_id = key_id.into();
        validate_component("manifest key id", &key_id)?;
        let key = VerifyingKey::from_bytes(&public_key).map_err(|error| {
            update_error(format!("invalid Ed25519 manifest public key: {error}"))
        })?;
        match self.keys.entry(key_id) {
            Entry::Vacant(entry) => {
                entry.insert(key);
            }
            Entry::Occupied(_) => return Err(update_error("duplicate manifest key id")),
        }
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }
}

pub fn verify_update_manifest(
    manifest_bytes: &[u8],
    signature_base64: &str,
    key_id: &str,
    trusted_keys: &TrustedManifestKeys,
    now_unix_ms: u64,
) -> ExternalAgentResult<RuntimeUpdateManifest> {
    if manifest_bytes.is_empty() || manifest_bytes.len() > UPDATE_MANIFEST_MAX_BYTES {
        return Err(update_error(
            "update manifest size is outside the allowed range",
        ));
    }
    let key = trusted_keys
        .keys
        .get(key_id)
        .ok_or_else(|| update_error("update manifest was signed by an untrusted key id"))?;
    let signature_bytes = base64::engine::general_purpose::STANDARD
        .decode(signature_base64.trim())
        .map_err(|_| update_error("update manifest signature is not valid base64"))?;
    let signature = Signature::from_slice(&signature_bytes)
        .map_err(|_| update_error("update manifest signature must contain 64 bytes"))?;
    key.verify(manifest_bytes, &signature)
        .map_err(|_| update_error("update manifest signature verification failed"))?;

    // Parse only after detached signature verification. This keeps attacker-controlled structures
    // away from the serde layer until authenticity is established.
    let manifest: RuntimeUpdateManifest = serde_json::from_slice(manifest_bytes)
        .map_err(|error| update_error(format!("invalid update manifest JSON: {error}")))?;
    validate_manifest(&manifest, now_unix_ms)?;
    Ok(manifest)
}

fn validate_manifest(
    manifest: &RuntimeUpdateManifest,
    now_unix_ms: u64,
) -> ExternalAgentResult<()> {
    if manifest.schema_version != UPDATE_MANIFEST_SCHEMA_VERSION {
        return Err(update_error("unsupported update manifest schema version"));
    }
    if manifest.generated_unix_ms > now_unix_ms.saturating_add(UPDATE_CLOCK_SKEW_MS) {
        return Err(update_error(
            "update manifest generation time is in the future",
        ));
    }
    if manifest.expires_unix_ms <= now_unix_ms
        || manifest.expires_unix_ms <= manifest.generated_unix_ms
    {
        return Err(update_error(
            "update manifest is expired or has an invalid lifetime",
        ));
    }
    let mut release_ids = BTreeSet::new();
    for release in &manifest.releases {
        if release.contract_version != EXTERNAL_AGENT_CONTRACT_VERSION {
            return Err(update_error(format!(
                "{} {} targets unsupported external-agent contract {}",
                release.kind, release.version, release.contract_version
            )));
        }
        if release.protocol != release.kind.protocol_name() {
            return Err(update_error(format!(
                "{} {} declares protocol `{}` instead of `{}`",
                release.kind,
                release.version,
                release.protocol,
                release.kind.protocol_name()
            )));
        }
        let release_id = (
            release.kind,
            release.channel.rank(),
            release.version.clone(),
        );
        if !release_ids.insert(release_id) {
            return Err(update_error("update manifest contains a duplicate release"));
        }
        if release.artifacts.is_empty() {
            return Err(update_error("runtime release contains no artifacts"));
        }
        let mut targets = BTreeSet::new();
        for artifact in &release.artifacts {
            validate_artifact(artifact)?;
            if !targets.insert((&artifact.target_os, &artifact.target_arch)) {
                return Err(update_error(
                    "runtime release contains duplicate artifacts for a target",
                ));
            }
        }
    }
    Ok(())
}

fn validate_artifact(artifact: &RuntimeArtifact) -> ExternalAgentResult<()> {
    validate_component("target os", &artifact.target_os)?;
    validate_component("target architecture", &artifact.target_arch)?;
    validate_component("runtime executable name", &artifact.executable_name)?;
    validate_sha256(&artifact.sha256)?;
    if artifact.size_bytes == 0 || artifact.size_bytes > UPDATE_ARTIFACT_MAX_BYTES {
        return Err(update_error(
            "runtime artifact size is outside the allowed range",
        ));
    }
    let url = url::Url::parse(&artifact.url)
        .map_err(|error| update_error(format!("invalid runtime artifact URL: {error}")))?;
    if url.scheme() != "https" || url.host_str().is_none() || url.username() != "" {
        return Err(update_error(
            "runtime artifact URL must be credential-free HTTPS",
        ));
    }
    if url.password().is_some() || url.fragment().is_some() {
        return Err(update_error(
            "runtime artifact URL cannot contain credentials or a fragment",
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeUpdateSelection {
    pub kind: ExternalAgentKind,
    pub version: Version,
    pub channel: RuntimeUpdateChannel,
    pub protocol: String,
    pub artifact: RuntimeArtifact,
}

pub fn select_runtime_update(
    manifest: &RuntimeUpdateManifest,
    kind: ExternalAgentKind,
    channel: RuntimeUpdateChannel,
    current_runtime_version: Option<&Version>,
    aikit_version: &Version,
    target_os: &str,
    target_arch: &str,
) -> ExternalAgentResult<Option<RuntimeUpdateSelection>> {
    if manifest
        .minimum_aikit_version
        .as_ref()
        .is_some_and(|minimum| aikit_version < minimum)
    {
        return Err(update_error(
            "AIKit is older than the manifest's minimum compatible version",
        ));
    }
    let selected = manifest
        .releases
        .iter()
        .filter(|release| release.kind == kind && release.channel == channel)
        .filter(|release| {
            release
                .minimum_aikit_version
                .as_ref()
                .is_none_or(|minimum| aikit_version >= minimum)
                && release
                    .maximum_aikit_version
                    .as_ref()
                    .is_none_or(|maximum| aikit_version <= maximum)
        })
        .filter(|release| current_runtime_version.is_none_or(|current| release.version > *current))
        .filter_map(|release| {
            release
                .artifacts
                .iter()
                .find(|artifact| {
                    artifact.target_os == target_os && artifact.target_arch == target_arch
                })
                .map(|artifact| (release, artifact))
        })
        .max_by(|(left, _), (right, _)| left.version.cmp(&right.version));
    Ok(selected.map(|(release, artifact)| RuntimeUpdateSelection {
        kind,
        version: release.version.clone(),
        channel,
        protocol: release.protocol.clone(),
        artifact: artifact.clone(),
    }))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeUpdatePhase {
    Idle,
    Checking,
    Available,
    Downloading,
    Staged,
    Activating,
    Current,
    RolledBack,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeUpdateState {
    pub schema_version: u32,
    pub instance_id: RuntimeInstanceId,
    pub profile: RuntimeProfile,
    pub policy: RuntimeUpdatePolicy,
    pub channel: RuntimeUpdateChannel,
    pub phase: RuntimeUpdatePhase,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_version: Option<Version>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub available_version: Option<Version>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_manifest_check_unix_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_model_refresh_unix_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_discovery_unix_ms: Option<u64>,
    #[serde(default)]
    pub consecutive_failures: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_after_unix_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

impl RuntimeUpdateState {
    pub fn new(instance_id: RuntimeInstanceId, profile: RuntimeProfile) -> Self {
        Self {
            schema_version: EXTERNAL_AGENT_CONTRACT_VERSION,
            instance_id,
            profile,
            policy: RuntimeUpdatePolicy::default_for(profile),
            channel: RuntimeUpdateChannel::Stable,
            phase: RuntimeUpdatePhase::Idle,
            current_version: None,
            available_version: None,
            last_manifest_check_unix_ms: None,
            last_model_refresh_unix_ms: None,
            last_discovery_unix_ms: None,
            consecutive_failures: 0,
            retry_after_unix_ms: None,
            last_error: None,
        }
    }

    pub fn manifest_check_due(&self, now_unix_ms: u64) -> bool {
        self.retry_after_unix_ms
            .is_none_or(|retry| now_unix_ms >= retry)
            && self
                .last_manifest_check_unix_ms
                .is_none_or(|last| now_unix_ms.saturating_sub(last) >= MANIFEST_REFRESH_TTL_MS)
    }

    pub fn model_refresh_due(&self, now_unix_ms: u64) -> bool {
        self.last_model_refresh_unix_ms
            .is_none_or(|last| now_unix_ms.saturating_sub(last) >= MODEL_CATALOG_REFRESH_TTL_MS)
    }

    pub fn discovery_due(&self, now_unix_ms: u64) -> bool {
        self.last_discovery_unix_ms
            .is_none_or(|last| now_unix_ms.saturating_sub(last) >= DISCOVERY_REFRESH_TTL_MS)
    }

    pub fn record_check_success(
        &mut self,
        now_unix_ms: u64,
        selection: Option<&RuntimeUpdateSelection>,
    ) {
        self.last_manifest_check_unix_ms = Some(now_unix_ms);
        self.consecutive_failures = 0;
        self.retry_after_unix_ms = None;
        self.last_error = None;
        self.available_version = selection.map(|selection| selection.version.clone());
        self.phase = if selection.is_some() {
            RuntimeUpdatePhase::Available
        } else {
            RuntimeUpdatePhase::Current
        };
    }

    pub fn record_failure(&mut self, now_unix_ms: u64, error: impl Into<String>) {
        self.phase = RuntimeUpdatePhase::Failed;
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        let retry_index = self
            .consecutive_failures
            .saturating_sub(1)
            .min((RETRY_DELAYS_MS.len() - 1) as u32) as usize;
        self.retry_after_unix_ms = now_unix_ms.checked_add(RETRY_DELAYS_MS[retry_index]);
        self.last_error = Some(redact_update_error(error.into()));
    }

    pub fn record_discovery(&mut self, now_unix_ms: u64) {
        self.last_discovery_unix_ms = Some(now_unix_ms);
    }

    pub fn record_model_refresh(&mut self, now_unix_ms: u64) {
        self.last_model_refresh_unix_ms = Some(now_unix_ms);
    }

    pub fn begin_download(&mut self) -> ExternalAgentResult<()> {
        if !self.may_apply_binary_update() {
            return Err(update_error(
                "runtime update cannot download in the current profile, policy, or phase",
            ));
        }
        self.phase = RuntimeUpdatePhase::Downloading;
        Ok(())
    }

    pub fn record_staged(&mut self) -> ExternalAgentResult<()> {
        if self.phase != RuntimeUpdatePhase::Downloading {
            return Err(update_error(
                "runtime update can be staged only after downloading",
            ));
        }
        self.phase = RuntimeUpdatePhase::Staged;
        Ok(())
    }

    pub fn record_activated(&mut self) -> ExternalAgentResult<()> {
        if self.phase != RuntimeUpdatePhase::Staged {
            return Err(update_error(
                "runtime update can be activated only from a staged artifact",
            ));
        }
        self.current_version = self.available_version.take();
        self.phase = RuntimeUpdatePhase::Current;
        Ok(())
    }

    pub fn record_rollback(&mut self, restored_version: Version) {
        self.current_version = Some(restored_version);
        self.available_version = None;
        self.phase = RuntimeUpdatePhase::RolledBack;
    }

    pub fn may_apply_binary_update(&self) -> bool {
        self.profile == RuntimeProfile::Managed
            && self.policy == RuntimeUpdatePolicy::AikitManaged
            && self.phase == RuntimeUpdatePhase::Available
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeRefreshAction {
    Rediscover,
    RefreshModelCatalog,
    CheckCompatibilityManifest,
    ReportExternalUpdate,
    StageManagedUpdate,
}

pub fn due_refresh_actions(
    state: &RuntimeUpdateState,
    now_unix_ms: u64,
) -> Vec<RuntimeRefreshAction> {
    let mut actions = Vec::new();
    if state.discovery_due(now_unix_ms) {
        actions.push(RuntimeRefreshAction::Rediscover);
    }
    if state.model_refresh_due(now_unix_ms) {
        actions.push(RuntimeRefreshAction::RefreshModelCatalog);
    }
    if state.manifest_check_due(now_unix_ms) {
        actions.push(RuntimeRefreshAction::CheckCompatibilityManifest);
    }
    if state.phase == RuntimeUpdatePhase::Available {
        if state.may_apply_binary_update() {
            actions.push(RuntimeRefreshAction::StageManagedUpdate);
        } else {
            actions.push(RuntimeRefreshAction::ReportExternalUpdate);
        }
    }
    actions
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstalledRuntimeArtifact {
    pub schema_version: u32,
    pub sha256: String,
    pub version: Version,
    pub executable: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeActivation {
    pub schema_version: u32,
    pub instance_id: RuntimeInstanceId,
    pub active: InstalledRuntimeArtifact,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous: Option<InstalledRuntimeArtifact>,
    pub activated_unix_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedRuntimeLaunch {
    pub instance_id: RuntimeInstanceId,
    pub kind: ExternalAgentKind,
    pub candidate: RuntimeCandidate,
    pub version: Version,
    pub artifact_hash: String,
}

#[derive(Debug, Clone)]
pub struct ManagedRuntimeArtifactStore {
    root: PathBuf,
}

impl ManagedRuntimeArtifactStore {
    pub fn new(root: impl Into<PathBuf>) -> ExternalAgentResult<Self> {
        let root = root.into();
        if !root.is_absolute() {
            return Err(ExternalAgentError::invalid_input(
                "managed runtime artifact root must be absolute",
            ));
        }
        fs::create_dir_all(&root).map_err(store_io)?;
        let root = fs::canonicalize(root).map_err(store_io)?;
        Ok(Self { root })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn stage_bytes(
        &self,
        selection: &RuntimeUpdateSelection,
        bytes: &[u8],
    ) -> ExternalAgentResult<InstalledRuntimeArtifact> {
        validate_artifact(&selection.artifact)?;
        if bytes.len() as u64 != selection.artifact.size_bytes {
            return Err(update_error(
                "downloaded runtime artifact size does not match manifest",
            ));
        }
        let digest = hex_sha256(bytes);
        if digest != selection.artifact.sha256 {
            return Err(update_error(
                "downloaded runtime artifact hash does not match manifest",
            ));
        }
        let directory = self
            .root
            .join("artifacts")
            .join("sha256")
            .join(&selection.artifact.sha256);
        fs::create_dir_all(&directory).map_err(store_io)?;
        ensure_beneath(&self.root, &directory)?;
        let executable = directory.join(&selection.artifact.executable_name);
        if executable.exists() {
            verify_file(&executable, &selection.artifact)?;
        } else {
            let mut temporary = tempfile::NamedTempFile::new_in(&directory).map_err(store_io)?;
            temporary.write_all(bytes).map_err(store_io)?;
            temporary.as_file_mut().sync_all().map_err(store_io)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                temporary
                    .as_file_mut()
                    .set_permissions(fs::Permissions::from_mode(0o755))
                    .map_err(store_io)?;
            }
            match temporary.persist_noclobber(&executable) {
                Ok(_) => {}
                Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {
                    verify_file(&executable, &selection.artifact)?;
                }
                Err(error) => return Err(store_io(error.error)),
            }
        }
        let installed = InstalledRuntimeArtifact {
            schema_version: EXTERNAL_AGENT_CONTRACT_VERSION,
            sha256: selection.artifact.sha256.clone(),
            version: selection.version.clone(),
            executable,
        };
        write_json_atomic(&directory.join("artifact.json"), &installed)?;
        Ok(installed)
    }

    pub fn activate(
        &self,
        instance_id: &RuntimeInstanceId,
        artifact: InstalledRuntimeArtifact,
        now_unix_ms: u64,
    ) -> ExternalAgentResult<RuntimeActivation> {
        self.verify_installed(&artifact)?;
        let current = self.read_activation(instance_id)?;
        let activation = RuntimeActivation {
            schema_version: EXTERNAL_AGENT_CONTRACT_VERSION,
            instance_id: instance_id.clone(),
            previous: current.map(|current| current.active),
            active: artifact,
            activated_unix_ms: now_unix_ms,
        };
        write_json_atomic(&self.activation_path(instance_id)?, &activation)?;
        Ok(activation)
    }

    pub fn rollback(
        &self,
        instance_id: &RuntimeInstanceId,
        now_unix_ms: u64,
    ) -> ExternalAgentResult<RuntimeActivation> {
        let current = self
            .read_activation(instance_id)?
            .ok_or_else(|| update_error("managed runtime has no active artifact to roll back"))?;
        let previous = current.previous.ok_or_else(|| {
            update_error("managed runtime has no previous artifact to roll back to")
        })?;
        self.verify_installed(&previous)?;
        let rollback = RuntimeActivation {
            schema_version: EXTERNAL_AGENT_CONTRACT_VERSION,
            instance_id: instance_id.clone(),
            active: previous,
            previous: Some(current.active),
            activated_unix_ms: now_unix_ms,
        };
        write_json_atomic(&self.activation_path(instance_id)?, &rollback)?;
        Ok(rollback)
    }

    pub fn read_activation(
        &self,
        instance_id: &RuntimeInstanceId,
    ) -> ExternalAgentResult<Option<RuntimeActivation>> {
        let path = self.activation_path(instance_id)?;
        if !path.exists() {
            return Ok(None);
        }
        let bytes = fs::read(&path).map_err(store_io)?;
        let activation: RuntimeActivation = serde_json::from_slice(&bytes)
            .map_err(|error| update_error(format!("invalid runtime activation state: {error}")))?;
        if activation.schema_version != EXTERNAL_AGENT_CONTRACT_VERSION
            || activation.instance_id != *instance_id
        {
            return Err(update_error(
                "runtime activation state identity does not match",
            ));
        }
        self.verify_installed(&activation.active)?;
        if let Some(previous) = &activation.previous {
            self.verify_installed(previous)?;
        }
        Ok(Some(activation))
    }

    pub fn active_launch(
        &self,
        instance_id: &RuntimeInstanceId,
        kind: ExternalAgentKind,
    ) -> ExternalAgentResult<ManagedRuntimeLaunch> {
        let activation = self
            .read_activation(instance_id)?
            .ok_or_else(|| update_error("managed runtime has no active artifact"))?;
        let artifact = activation.active;
        Ok(ManagedRuntimeLaunch {
            instance_id: instance_id.clone(),
            kind,
            candidate: RuntimeCandidate {
                kind,
                executable: artifact.executable.clone(),
                real_executable: artifact.executable,
                origin: RuntimeInstallOrigin::AikitManaged,
                discovered_by: vec![RuntimeDiscoverySource::KnownLocation],
                aliases: Vec::new(),
            },
            version: artifact.version,
            artifact_hash: artifact.sha256,
        })
    }

    pub fn write_update_state(&self, state: &RuntimeUpdateState) -> ExternalAgentResult<()> {
        if state.schema_version != EXTERNAL_AGENT_CONTRACT_VERSION {
            return Err(update_error(
                "unsupported runtime update state schema version",
            ));
        }
        write_json_atomic(&self.update_state_path(&state.instance_id)?, state)
    }

    pub fn read_update_state(
        &self,
        instance_id: &RuntimeInstanceId,
    ) -> ExternalAgentResult<Option<RuntimeUpdateState>> {
        let path = self.update_state_path(instance_id)?;
        if !path.is_file() {
            return Ok(None);
        }
        let state: RuntimeUpdateState = serde_json::from_slice(&fs::read(path).map_err(store_io)?)
            .map_err(|error| update_error(format!("invalid runtime update state: {error}")))?;
        if state.schema_version != EXTERNAL_AGENT_CONTRACT_VERSION
            || state.instance_id != *instance_id
        {
            return Err(update_error("runtime update state identity does not match"));
        }
        Ok(Some(state))
    }

    pub fn garbage_collect(
        &self,
        session_pins: &BTreeSet<String>,
    ) -> ExternalAgentResult<Vec<String>> {
        let mut protected = session_pins.clone();
        let instances = self.root.join("instances");
        if instances.is_dir() {
            for entry in fs::read_dir(instances).map_err(store_io)? {
                let entry = entry.map_err(store_io)?;
                let state = entry.path().join("active.json");
                if !state.is_file() {
                    continue;
                }
                let bytes = fs::read(state).map_err(store_io)?;
                let activation: RuntimeActivation =
                    serde_json::from_slice(&bytes).map_err(|error| {
                        update_error(format!(
                            "invalid runtime activation state during GC: {error}"
                        ))
                    })?;
                protected.insert(activation.active.sha256);
                if let Some(previous) = activation.previous {
                    protected.insert(previous.sha256);
                }
            }
        }
        let artifacts = self.root.join("artifacts").join("sha256");
        let mut removed = Vec::new();
        if !artifacts.is_dir() {
            return Ok(removed);
        }
        for entry in fs::read_dir(&artifacts).map_err(store_io)? {
            let entry = entry.map_err(store_io)?;
            let hash = entry.file_name().to_string_lossy().to_string();
            if !is_sha256(&hash) || protected.contains(&hash) {
                continue;
            }
            let path = entry.path();
            ensure_beneath(&self.root, &path)?;
            if entry.file_type().map_err(store_io)?.is_dir() {
                fs::remove_dir_all(path).map_err(store_io)?;
                removed.push(hash);
            }
        }
        Ok(removed)
    }

    fn activation_path(&self, instance_id: &RuntimeInstanceId) -> ExternalAgentResult<PathBuf> {
        Ok(self.instance_directory(instance_id)?.join("active.json"))
    }

    fn update_state_path(&self, instance_id: &RuntimeInstanceId) -> ExternalAgentResult<PathBuf> {
        Ok(self
            .instance_directory(instance_id)?
            .join("update-state.json"))
    }

    fn instance_directory(&self, instance_id: &RuntimeInstanceId) -> ExternalAgentResult<PathBuf> {
        // Runtime IDs allow `:` for namespacing, but that is not a portable path character.
        // Hashing also avoids case-folding collisions on default macOS and Windows filesystems.
        let directory = self.root.join("instances").join(format!(
            "instance-{}",
            hex_sha256(instance_id.as_str().as_bytes())
        ));
        fs::create_dir_all(&directory).map_err(store_io)?;
        ensure_beneath(&self.root, &directory)?;
        Ok(directory)
    }

    fn verify_installed(&self, artifact: &InstalledRuntimeArtifact) -> ExternalAgentResult<()> {
        validate_sha256(&artifact.sha256)?;
        ensure_beneath(&self.root, &artifact.executable)?;
        let bytes = fs::read(&artifact.executable).map_err(store_io)?;
        if hex_sha256(&bytes) != artifact.sha256 {
            return Err(update_error(
                "installed runtime artifact failed hash verification",
            ));
        }
        Ok(())
    }
}

fn verify_file(path: &Path, artifact: &RuntimeArtifact) -> ExternalAgentResult<()> {
    let metadata = fs::symlink_metadata(path).map_err(store_io)?;
    if !metadata.file_type().is_file() || metadata.len() != artifact.size_bytes {
        return Err(update_error(
            "staged runtime artifact has unexpected file metadata",
        ));
    }
    let mut file = File::open(path).map_err(store_io)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer).map_err(store_io)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    if lowercase_hex(&hasher.finalize()) != artifact.sha256 {
        return Err(update_error(
            "staged runtime artifact failed hash verification",
        ));
    }
    Ok(())
}

fn write_json_atomic(path: &Path, value: &impl Serialize) -> ExternalAgentResult<()> {
    let parent = path
        .parent()
        .ok_or_else(|| update_error("atomic state path has no parent"))?;
    fs::create_dir_all(parent).map_err(store_io)?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent).map_err(store_io)?;
    serde_json::to_writer(temporary.as_file_mut(), value)
        .map_err(|error| update_error(format!("cannot serialize runtime update state: {error}")))?;
    temporary.as_file_mut().write_all(b"\n").map_err(store_io)?;
    temporary.as_file_mut().sync_all().map_err(store_io)?;
    temporary
        .persist(path)
        .map_err(|error| store_io(error.error))?;
    sync_directory(parent)?;
    Ok(())
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> ExternalAgentResult<()> {
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(store_io)
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> ExternalAgentResult<()> {
    Ok(())
}

fn ensure_beneath(root: &Path, path: &Path) -> ExternalAgentResult<()> {
    let canonical = if path.exists() {
        fs::canonicalize(path).map_err(store_io)?
    } else {
        let parent = path
            .parent()
            .ok_or_else(|| update_error("managed runtime path has no parent"))?;
        fs::canonicalize(parent).map_err(store_io)?.join(
            path.file_name()
                .ok_or_else(|| update_error("managed runtime path has no file name"))?,
        )
    };
    if !canonical.starts_with(root) {
        return Err(update_error(
            "managed runtime path escapes the artifact root",
        ));
    }
    Ok(())
}

fn validate_component(label: &str, value: &str) -> ExternalAgentResult<()> {
    let path = Path::new(value);
    let one_normal_component = path.components().count() == 1
        && matches!(path.components().next(), Some(Component::Normal(_)));
    if !one_normal_component
        || value.is_empty()
        || value.len() > 128
        || value.chars().any(char::is_control)
    {
        return Err(ExternalAgentError::invalid_input(format!(
            "{label} must be one safe path component",
        )));
    }
    Ok(())
}

fn validate_sha256(value: &str) -> ExternalAgentResult<()> {
    if !is_sha256(value) {
        return Err(update_error(
            "runtime artifact sha256 must be 64 lowercase hexadecimal characters",
        ));
    }
    Ok(())
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn hex_sha256(bytes: &[u8]) -> String {
    lowercase_hex(&Sha256::digest(bytes))
}

fn lowercase_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn redact_update_error(message: String) -> String {
    let lower = message.to_ascii_lowercase();
    if [
        "authorization",
        "api_key",
        "api-key",
        "token",
        "password",
        "secret",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
    {
        "runtime update failed; sensitive error details were redacted".into()
    } else {
        message.chars().take(512).collect()
    }
}

fn update_error(message: impl Into<String>) -> ExternalAgentError {
    ExternalAgentError::new(ExternalAgentErrorCode::Update, message)
}

fn store_io(error: std::io::Error) -> ExternalAgentError {
    ExternalAgentError::new(
        ExternalAgentErrorCode::Io,
        format!("managed runtime artifact store I/O failed: {error}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{Signer, SigningKey};

    fn artifact(bytes: &[u8]) -> RuntimeArtifact {
        RuntimeArtifact {
            target_os: "macos".into(),
            target_arch: "aarch64".into(),
            url: "https://updates.example.test/gemini".into(),
            sha256: hex_sha256(bytes),
            size_bytes: bytes.len() as u64,
            executable_name: "gemini".into(),
        }
    }

    fn manifest(bytes: &[u8]) -> RuntimeUpdateManifest {
        RuntimeUpdateManifest {
            schema_version: UPDATE_MANIFEST_SCHEMA_VERSION,
            generated_unix_ms: 1_000,
            expires_unix_ms: 10_000,
            minimum_aikit_version: None,
            releases: vec![RuntimeRelease {
                kind: ExternalAgentKind::Gemini,
                channel: RuntimeUpdateChannel::Stable,
                version: Version::new(1, 2, 3),
                protocol: "acp".into(),
                contract_version: EXTERNAL_AGENT_CONTRACT_VERSION,
                minimum_aikit_version: None,
                maximum_aikit_version: None,
                artifacts: vec![artifact(bytes)],
            }],
        }
    }

    #[test]
    fn signature_is_verified_before_manifest_is_accepted() {
        let signing = SigningKey::from_bytes(&[7_u8; 32]);
        let bytes = serde_json::to_vec(&manifest(b"runtime")).unwrap();
        let signature = signing.sign(&bytes);
        let mut keys = TrustedManifestKeys::default();
        keys.insert("stable-2026", signing.verifying_key().to_bytes())
            .unwrap();
        let encoded = base64::engine::general_purpose::STANDARD.encode(signature.to_bytes());
        assert!(verify_update_manifest(&bytes, &encoded, "stable-2026", &keys, 2_000).is_ok());
        let mut tampered = bytes;
        tampered.push(b' ');
        assert!(verify_update_manifest(&tampered, &encoded, "stable-2026", &keys, 2_000).is_err());
    }

    #[test]
    fn duplicate_key_ids_do_not_replace_the_trusted_key() {
        let first = SigningKey::from_bytes(&[7_u8; 32]);
        let replacement = SigningKey::from_bytes(&[8_u8; 32]);
        let mut keys = TrustedManifestKeys::default();
        keys.insert("stable-2026", first.verifying_key().to_bytes())
            .unwrap();
        assert!(keys
            .insert("stable-2026", replacement.verifying_key().to_bytes())
            .is_err());

        let bytes = serde_json::to_vec(&manifest(b"runtime")).unwrap();
        let signature = first.sign(&bytes);
        let encoded = base64::engine::general_purpose::STANDARD.encode(signature.to_bytes());
        assert!(verify_update_manifest(&bytes, &encoded, "stable-2026", &keys, 2_000).is_ok());
    }

    #[test]
    fn selection_respects_target_channel_and_current_version() {
        let manifest = manifest(b"runtime");
        let selected = select_runtime_update(
            &manifest,
            ExternalAgentKind::Gemini,
            RuntimeUpdateChannel::Stable,
            Some(&Version::new(1, 0, 0)),
            &Version::new(0, 3, 0),
            "macos",
            "aarch64",
        )
        .unwrap()
        .unwrap();
        assert_eq!(selected.version, Version::new(1, 2, 3));
        assert!(select_runtime_update(
            &manifest,
            ExternalAgentKind::Gemini,
            RuntimeUpdateChannel::Stable,
            Some(&Version::new(1, 2, 3)),
            &Version::new(0, 3, 0),
            "macos",
            "aarch64",
        )
        .unwrap()
        .is_none());
    }

    #[test]
    fn external_profiles_never_auto_stage_binary_updates() {
        let mut state = RuntimeUpdateState::new(
            RuntimeInstanceId::new("gemini:personal").unwrap(),
            RuntimeProfile::External,
        );
        state.phase = RuntimeUpdatePhase::Available;
        let actions = due_refresh_actions(&state, 1);
        assert!(actions.contains(&RuntimeRefreshAction::ReportExternalUpdate));
        assert!(!actions.contains(&RuntimeRefreshAction::StageManagedUpdate));
    }

    #[test]
    fn failed_checks_back_off_one_six_then_twenty_four_hours() {
        let mut state = RuntimeUpdateState::new(
            RuntimeInstanceId::new("grok:managed").unwrap(),
            RuntimeProfile::Managed,
        );
        state.record_failure(1_000, "network");
        assert_eq!(state.retry_after_unix_ms, Some(1_000 + RETRY_DELAYS_MS[0]));
        state.record_failure(2_000, "network");
        assert_eq!(state.retry_after_unix_ms, Some(2_000 + RETRY_DELAYS_MS[1]));
        state.record_failure(3_000, "network");
        assert_eq!(state.retry_after_unix_ms, Some(3_000 + RETRY_DELAYS_MS[2]));
        state.record_failure(4_000, "network");
        assert_eq!(state.retry_after_unix_ms, Some(4_000 + RETRY_DELAYS_MS[2]));
    }

    #[test]
    fn managed_update_phase_transitions_fail_closed() {
        let mut state = RuntimeUpdateState::new(
            RuntimeInstanceId::new("gemini:managed").unwrap(),
            RuntimeProfile::Managed,
        );
        assert!(state.record_staged().is_err());
        state.phase = RuntimeUpdatePhase::Available;
        state.available_version = Some(Version::new(2, 0, 0));
        state.begin_download().unwrap();
        state.record_staged().unwrap();
        state.record_activated().unwrap();
        assert_eq!(state.phase, RuntimeUpdatePhase::Current);
        assert_eq!(state.current_version, Some(Version::new(2, 0, 0)));
    }

    #[test]
    fn artifact_activation_and_rollback_are_side_by_side() {
        let root = tempfile::tempdir().unwrap();
        let store = ManagedRuntimeArtifactStore::new(root.path().to_path_buf()).unwrap();
        let instance = RuntimeInstanceId::new("gemini:managed").unwrap();

        let first_bytes = b"first runtime";
        let first = RuntimeUpdateSelection {
            kind: ExternalAgentKind::Gemini,
            version: Version::new(1, 0, 0),
            channel: RuntimeUpdateChannel::Stable,
            protocol: "acp".into(),
            artifact: artifact(first_bytes),
        };
        let first = store.stage_bytes(&first, first_bytes).unwrap();
        store.activate(&instance, first.clone(), 1_000).unwrap();

        let second_bytes = b"second runtime";
        let second = RuntimeUpdateSelection {
            kind: ExternalAgentKind::Gemini,
            version: Version::new(1, 1, 0),
            channel: RuntimeUpdateChannel::Stable,
            protocol: "acp".into(),
            artifact: artifact(second_bytes),
        };
        let second = store.stage_bytes(&second, second_bytes).unwrap();
        let active = store.activate(&instance, second.clone(), 2_000).unwrap();
        assert_eq!(active.active.sha256, second.sha256);
        assert_eq!(active.previous.as_ref().unwrap().sha256, first.sha256);

        let rolled_back = store.rollback(&instance, 3_000).unwrap();
        assert_eq!(rolled_back.active.sha256, first.sha256);
        assert_eq!(rolled_back.previous.unwrap().sha256, second.sha256);

        let launch = store
            .active_launch(&instance, ExternalAgentKind::Gemini)
            .unwrap();
        assert_eq!(launch.artifact_hash, first.sha256);
        assert_eq!(launch.candidate.origin, RuntimeInstallOrigin::AikitManaged);
        assert_eq!(launch.candidate.real_executable, first.executable);
    }

    #[test]
    fn update_state_round_trips_under_hashed_instance_path() {
        let root = tempfile::tempdir().unwrap();
        let store = ManagedRuntimeArtifactStore::new(root.path().to_path_buf()).unwrap();
        let instance = RuntimeInstanceId::new("gemini:personal").unwrap();
        let mut state = RuntimeUpdateState::new(instance.clone(), RuntimeProfile::External);
        state.record_discovery(123);
        state.record_model_refresh(456);
        store.write_update_state(&state).unwrap();
        assert_eq!(store.read_update_state(&instance).unwrap(), Some(state));
        assert!(!root.path().join("instances/gemini:personal").exists());
    }
}
