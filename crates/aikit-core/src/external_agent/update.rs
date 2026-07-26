//! Signed compatibility manifests and side-by-side managed runtime artifacts.

use super::{
    ExternalAgentError, ExternalAgentErrorCode, ExternalAgentKind, ExternalAgentResult,
    RuntimeCandidate, RuntimeDiscoverySource, RuntimeInstallOrigin, RuntimeInstanceId,
    RuntimeProfile, EXTERNAL_AGENT_CONTRACT_VERSION,
};
use base64::Engine as _;
use fs2::FileExt as _;
use futures::StreamExt as _;
use ring::signature::{UnparsedPublicKey, ED25519};
use semver::Version;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};
use std::time::Duration;

pub const UPDATE_MANIFEST_SCHEMA_VERSION: u32 = 1;
pub const UPDATE_MANIFEST_MAX_BYTES: usize = 2 * 1024 * 1024;
pub const UPDATE_SIGNATURE_MAX_BYTES: usize = 8 * 1024;
pub const UPDATE_STATE_MAX_BYTES: u64 = 256 * 1024;
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
pub struct RuntimeUpdateSource {
    pub manifest_url: String,
    pub signature_url: String,
    pub key_id: String,
    /// Base64-encoded raw 32-byte Ed25519 public key. This is public trust material, not a secret.
    pub public_key_base64: String,
}

impl RuntimeUpdateSource {
    pub fn validate(&self) -> ExternalAgentResult<()> {
        for (label, value) in [
            ("manifest", self.manifest_url.as_str()),
            ("signature", self.signature_url.as_str()),
        ] {
            let url = validate_update_url(label, value)?;
            if url.query().is_some() {
                return Err(update_error(format!(
                    "{label} URL cannot persist query credentials"
                )));
            }
        }
        validate_component("manifest key id", &self.key_id)?;
        self.public_key()?;
        Ok(())
    }

    pub fn trusted_keys(&self) -> ExternalAgentResult<TrustedManifestKeys> {
        let mut keys = TrustedManifestKeys::default();
        keys.insert(self.key_id.clone(), self.public_key()?)?;
        Ok(keys)
    }

    fn public_key(&self) -> ExternalAgentResult<[u8; 32]> {
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(self.public_key_base64.trim())
            .map_err(|_| update_error("manifest public key is not valid base64"))?;
        decoded
            .try_into()
            .map_err(|_| update_error("manifest public key must contain 32 bytes"))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeUpdateConfig {
    pub schema_version: u32,
    pub instance_id: RuntimeInstanceId,
    pub kind: ExternalAgentKind,
    pub profile: RuntimeProfile,
    pub policy: RuntimeUpdatePolicy,
    pub channel: RuntimeUpdateChannel,
    pub source: RuntimeUpdateSource,
}

impl RuntimeUpdateConfig {
    pub fn validate(&self) -> ExternalAgentResult<()> {
        if self.schema_version != EXTERNAL_AGENT_CONTRACT_VERSION {
            return Err(update_error(
                "unsupported runtime update configuration schema version",
            ));
        }
        if self.profile == RuntimeProfile::External
            && self.policy == RuntimeUpdatePolicy::AikitManaged
        {
            return Err(update_error(
                "external runtimes cannot use the AIKit-managed update policy",
            ));
        }
        self.source.validate()
    }
}

#[derive(Debug, Clone)]
pub struct RuntimeUpdateClient {
    client: reqwest::Client,
}

impl RuntimeUpdateClient {
    pub fn new() -> ExternalAgentResult<Self> {
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(10))
            .read_timeout(Duration::from_secs(30))
            .user_agent(concat!("aikit-runtime/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|_| update_error("cannot construct runtime update HTTP client"))?;
        Ok(Self { client })
    }

    pub async fn fetch_manifest(
        &self,
        source: &RuntimeUpdateSource,
        now_unix_ms: u64,
    ) -> ExternalAgentResult<RuntimeUpdateManifest> {
        source.validate()?;
        let manifest = self
            .fetch_limited(&source.manifest_url, UPDATE_MANIFEST_MAX_BYTES)
            .await?;
        let signature = self
            .fetch_limited(&source.signature_url, UPDATE_SIGNATURE_MAX_BYTES)
            .await?;
        let signature = std::str::from_utf8(&signature)
            .map_err(|_| update_error("update manifest signature response is not UTF-8"))?;
        verify_update_manifest(
            &manifest,
            signature,
            &source.key_id,
            &source.trusted_keys()?,
            now_unix_ms,
        )
    }

    pub async fn download_and_stage(
        &self,
        store: &ManagedRuntimeArtifactStore,
        selection: &RuntimeUpdateSelection,
    ) -> ExternalAgentResult<InstalledRuntimeArtifact> {
        validate_artifact(&selection.artifact)?;
        let response = self
            .client
            .get(&selection.artifact.url)
            .send()
            .await
            .map_err(|_| update_error("runtime artifact request failed"))?;
        store.stage_response(selection, response).await
    }

    async fn fetch_limited(&self, url: &str, maximum_bytes: usize) -> ExternalAgentResult<Vec<u8>> {
        validate_update_url("update metadata", url)?;
        let response = self
            .client
            .get(url)
            .send()
            .await
            .map_err(|_| update_error("runtime update metadata request failed"))?;
        if !response.status().is_success() {
            return Err(update_error(format!(
                "runtime update metadata returned HTTP {}",
                response.status().as_u16()
            )));
        }
        if response
            .content_length()
            .is_some_and(|length| length > maximum_bytes as u64)
        {
            return Err(update_error("runtime update metadata is too large"));
        }
        let mut bytes = Vec::new();
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk
                .map_err(|_| update_error("runtime update metadata response was interrupted"))?;
            if bytes.len().saturating_add(chunk.len()) > maximum_bytes {
                return Err(update_error("runtime update metadata is too large"));
            }
            bytes.extend_from_slice(&chunk);
        }
        if bytes.is_empty() {
            return Err(update_error("runtime update metadata response is empty"));
        }
        Ok(bytes)
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
    keys: BTreeMap<String, [u8; 32]>,
}

impl TrustedManifestKeys {
    pub fn insert(
        &mut self,
        key_id: impl Into<String>,
        public_key: [u8; 32],
    ) -> ExternalAgentResult<()> {
        let key_id = key_id.into();
        validate_component("manifest key id", &key_id)?;
        match self.keys.entry(key_id) {
            Entry::Vacant(entry) => {
                entry.insert(public_key);
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
    if signature_bytes.len() != 64 {
        return Err(update_error(
            "update manifest signature must contain 64 bytes",
        ));
    }
    UnparsedPublicKey::new(&ED25519, key)
        .verify(manifest_bytes, &signature_bytes)
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
    validate_update_url("runtime artifact", &artifact.url)?;
    Ok(())
}

fn validate_update_url(label: &str, value: &str) -> ExternalAgentResult<url::Url> {
    if value.len() > 4_096 || value.chars().any(char::is_control) {
        return Err(update_error(format!("{label} URL is too long or invalid")));
    }
    let url =
        url::Url::parse(value).map_err(|_| update_error(format!("{label} URL is invalid")))?;
    if url.scheme() != "https" || url.host_str().is_none() {
        return Err(update_error(format!("{label} URL must use HTTPS")));
    }
    if !url.username().is_empty() || url.password().is_some() || url.fragment().is_some() {
        return Err(update_error(format!(
            "{label} URL cannot contain userinfo credentials or a fragment"
        )));
    }
    Ok(url)
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
    /// A version whose staged health check failed or which was explicitly rolled back.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quarantined_version: Option<Version>,
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
            quarantined_version: None,
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

    pub fn begin_activation(&mut self) -> ExternalAgentResult<()> {
        if self.phase != RuntimeUpdatePhase::Staged {
            return Err(update_error(
                "runtime update can be activated only from a staged artifact",
            ));
        }
        self.phase = RuntimeUpdatePhase::Activating;
        Ok(())
    }

    pub fn record_activated(&mut self) -> ExternalAgentResult<()> {
        if self.phase != RuntimeUpdatePhase::Activating {
            return Err(update_error(
                "runtime update can be completed only after activation begins",
            ));
        }
        self.current_version = self.available_version.take();
        self.quarantined_version = None;
        self.phase = RuntimeUpdatePhase::Current;
        Ok(())
    }

    /// Finish the state write after a process crashed between atomic activation and persistence.
    pub fn recover_activated(&mut self, active_version: &Version) -> ExternalAgentResult<bool> {
        if !matches!(
            self.phase,
            RuntimeUpdatePhase::Staged | RuntimeUpdatePhase::Activating
        ) || self.available_version.as_ref() != Some(active_version)
        {
            return Ok(false);
        }
        if self.phase == RuntimeUpdatePhase::Staged {
            self.begin_activation()?;
        }
        self.record_activated()?;
        Ok(true)
    }

    pub fn quarantine_available(&mut self) {
        self.quarantined_version = self.available_version.clone();
    }

    pub fn record_rollback(
        &mut self,
        restored_version: Version,
        rejected_version: Option<Version>,
    ) {
        self.current_version = Some(restored_version);
        self.available_version = None;
        self.quarantined_version = rejected_version;
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

#[derive(Debug)]
pub struct ManagedRuntimeUpdateLock {
    file: File,
}

impl Drop for ManagedRuntimeUpdateLock {
    fn drop(&mut self) {
        let _ = fs2::FileExt::unlock(&self.file);
    }
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

    /// Prevent two host processes from racing update state or activation pointers.
    /// The advisory lock is released by the OS if the process exits unexpectedly.
    pub fn try_update_lock(&self) -> ExternalAgentResult<ManagedRuntimeUpdateLock> {
        let path = self.root.join("update.lock");
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)
            .map_err(store_io)?;
        file.try_lock_exclusive().map_err(|error| {
            if error.kind() == std::io::ErrorKind::WouldBlock {
                update_error("another runtime update is already in progress")
            } else {
                store_io(error)
            }
        })?;
        Ok(ManagedRuntimeUpdateLock { file })
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

    async fn stage_response(
        &self,
        selection: &RuntimeUpdateSelection,
        response: reqwest::Response,
    ) -> ExternalAgentResult<InstalledRuntimeArtifact> {
        if !response.status().is_success() {
            return Err(update_error(format!(
                "runtime artifact returned HTTP {}",
                response.status().as_u16()
            )));
        }
        if response
            .content_length()
            .is_some_and(|length| length != selection.artifact.size_bytes)
        {
            return Err(update_error(
                "runtime artifact Content-Length does not match manifest",
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
            let mut stream = response.bytes_stream();
            let mut size = 0_u64;
            let mut hasher = Sha256::new();
            while let Some(chunk) = stream.next().await {
                let chunk =
                    chunk.map_err(|_| update_error("runtime artifact response was interrupted"))?;
                size = size
                    .checked_add(chunk.len() as u64)
                    .ok_or_else(|| update_error("runtime artifact size overflow"))?;
                if size > selection.artifact.size_bytes {
                    return Err(update_error(
                        "downloaded runtime artifact exceeds manifest size",
                    ));
                }
                hasher.update(&chunk);
                temporary.write_all(&chunk).map_err(store_io)?;
            }
            if size != selection.artifact.size_bytes {
                return Err(update_error(
                    "downloaded runtime artifact size does not match manifest",
                ));
            }
            if lowercase_hex(&hasher.finalize()) != selection.artifact.sha256 {
                return Err(update_error(
                    "downloaded runtime artifact hash does not match manifest",
                ));
            }
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
        let bytes = read_state_file(&path, "runtime activation state")?;
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

    pub fn installed_launch(
        &self,
        instance_id: &RuntimeInstanceId,
        kind: ExternalAgentKind,
        artifact: &InstalledRuntimeArtifact,
    ) -> ExternalAgentResult<ManagedRuntimeLaunch> {
        self.verify_installed(artifact)?;
        Ok(ManagedRuntimeLaunch {
            instance_id: instance_id.clone(),
            kind,
            candidate: RuntimeCandidate {
                kind,
                executable: artifact.executable.clone(),
                real_executable: artifact.executable.clone(),
                origin: RuntimeInstallOrigin::AikitManaged,
                discovered_by: vec![RuntimeDiscoverySource::KnownLocation],
                aliases: Vec::new(),
            },
            version: artifact.version.clone(),
            artifact_hash: artifact.sha256.clone(),
        })
    }

    pub fn write_update_config(&self, config: &RuntimeUpdateConfig) -> ExternalAgentResult<()> {
        config.validate()?;
        write_json_atomic(&self.update_config_path(&config.instance_id)?, config)
    }

    pub fn read_update_config(
        &self,
        instance_id: &RuntimeInstanceId,
    ) -> ExternalAgentResult<Option<RuntimeUpdateConfig>> {
        let path = self.update_config_path(instance_id)?;
        if !path.is_file() {
            return Ok(None);
        }
        let config: RuntimeUpdateConfig =
            serde_json::from_slice(&read_state_file(&path, "runtime update configuration")?)
                .map_err(|error| {
                    update_error(format!("invalid runtime update configuration: {error}"))
                })?;
        if config.instance_id != *instance_id {
            return Err(update_error(
                "runtime update configuration identity does not match",
            ));
        }
        config.validate()?;
        Ok(Some(config))
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
        let state: RuntimeUpdateState =
            serde_json::from_slice(&read_state_file(&path, "runtime update state")?)
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
                let bytes = read_state_file(&state, "runtime activation state")?;
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

    fn update_config_path(&self, instance_id: &RuntimeInstanceId) -> ExternalAgentResult<PathBuf> {
        Ok(self
            .instance_directory(instance_id)?
            .join("update-config.json"))
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
        let metadata = fs::symlink_metadata(&artifact.executable).map_err(store_io)?;
        if !metadata.file_type().is_file()
            || metadata.len() == 0
            || metadata.len() > UPDATE_ARTIFACT_MAX_BYTES
        {
            return Err(update_error(
                "installed runtime artifact has unexpected file metadata",
            ));
        }
        if hash_file(&artifact.executable)? != artifact.sha256 {
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
    if hash_file(path)? != artifact.sha256 {
        return Err(update_error(
            "staged runtime artifact failed hash verification",
        ));
    }
    Ok(())
}

fn hash_file(path: &Path) -> ExternalAgentResult<String> {
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
    Ok(lowercase_hex(&hasher.finalize()))
}

fn read_state_file(path: &Path, label: &str) -> ExternalAgentResult<Vec<u8>> {
    let metadata = fs::symlink_metadata(path).map_err(store_io)?;
    if !metadata.file_type().is_file() || metadata.len() > UPDATE_STATE_MAX_BYTES {
        return Err(update_error(format!(
            "{label} is not a bounded regular file"
        )));
    }
    fs::read(path).map_err(store_io)
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
    use ring::signature::{Ed25519KeyPair, KeyPair};

    fn signing_key(seed: u8) -> Ed25519KeyPair {
        Ed25519KeyPair::from_seed_unchecked(&[seed; 32]).unwrap()
    }

    fn public_key(key: &Ed25519KeyPair) -> [u8; 32] {
        key.public_key().as_ref().try_into().unwrap()
    }

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
        let signing = signing_key(7);
        let bytes = serde_json::to_vec(&manifest(b"runtime")).unwrap();
        let signature = signing.sign(&bytes);
        let mut keys = TrustedManifestKeys::default();
        keys.insert("stable-2026", public_key(&signing)).unwrap();
        let encoded = base64::engine::general_purpose::STANDARD.encode(signature.as_ref());
        assert!(verify_update_manifest(&bytes, &encoded, "stable-2026", &keys, 2_000).is_ok());
        let mut tampered = bytes;
        tampered.push(b' ');
        assert!(verify_update_manifest(&tampered, &encoded, "stable-2026", &keys, 2_000).is_err());
    }

    #[test]
    fn duplicate_key_ids_do_not_replace_the_trusted_key() {
        let first = signing_key(7);
        let replacement = signing_key(8);
        let mut keys = TrustedManifestKeys::default();
        keys.insert("stable-2026", public_key(&first)).unwrap();
        assert!(keys
            .insert("stable-2026", public_key(&replacement))
            .is_err());

        let bytes = serde_json::to_vec(&manifest(b"runtime")).unwrap();
        let signature = first.sign(&bytes);
        let encoded = base64::engine::general_purpose::STANDARD.encode(signature.as_ref());
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
    fn update_source_rejects_downgrade_query_credentials_and_bad_keys() {
        let valid_key = base64::engine::general_purpose::STANDARD.encode([7_u8; 32]);
        let source = RuntimeUpdateSource {
            manifest_url: "http://updates.example.test/manifest.json".into(),
            signature_url: "https://updates.example.test/manifest.sig".into(),
            key_id: "stable-2026".into(),
            public_key_base64: valid_key.clone(),
        };
        assert!(source.validate().is_err());

        let source = RuntimeUpdateSource {
            manifest_url: "https://updates.example.test/manifest.json?token=secret".into(),
            signature_url: "https://updates.example.test/manifest.sig".into(),
            key_id: "stable-2026".into(),
            public_key_base64: valid_key,
        };
        assert!(source.validate().is_err());

        let source = RuntimeUpdateSource {
            manifest_url: "https://updates.example.test/manifest.json".into(),
            signature_url: "https://updates.example.test/manifest.sig".into(),
            key_id: "stable-2026".into(),
            public_key_base64: "too-short".into(),
        };
        assert!(source.validate().is_err());
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
        state.begin_activation().unwrap();
        state.record_activated().unwrap();
        assert_eq!(state.phase, RuntimeUpdatePhase::Current);
        assert_eq!(state.current_version, Some(Version::new(2, 0, 0)));
        assert_eq!(state.quarantined_version, None);
    }

    #[test]
    fn failed_or_rolled_back_versions_are_quarantined() {
        let mut state = RuntimeUpdateState::new(
            RuntimeInstanceId::new("gemini:managed").unwrap(),
            RuntimeProfile::Managed,
        );
        state.available_version = Some(Version::new(2, 0, 0));
        state.quarantine_available();
        assert_eq!(state.quarantined_version, Some(Version::new(2, 0, 0)));

        state.record_rollback(Version::new(1, 5, 0), Some(Version::new(2, 1, 0)));
        assert_eq!(state.current_version, Some(Version::new(1, 5, 0)));
        assert_eq!(state.quarantined_version, Some(Version::new(2, 1, 0)));
        assert_eq!(state.phase, RuntimeUpdatePhase::RolledBack);
    }

    #[test]
    fn interrupted_activation_is_reconciled_only_for_the_exact_active_version() {
        let mut state = RuntimeUpdateState::new(
            RuntimeInstanceId::new("gemini:managed").unwrap(),
            RuntimeProfile::Managed,
        );
        state.phase = RuntimeUpdatePhase::Staged;
        state.available_version = Some(Version::new(2, 0, 0));
        assert!(!state.recover_activated(&Version::new(1, 0, 0)).unwrap());
        assert_eq!(state.phase, RuntimeUpdatePhase::Staged);
        assert!(state.recover_activated(&Version::new(2, 0, 0)).unwrap());
        assert_eq!(state.current_version, Some(Version::new(2, 0, 0)));
        assert_eq!(state.phase, RuntimeUpdatePhase::Current);
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

    #[test]
    fn update_configuration_round_trips_and_lock_is_exclusive() {
        let root = tempfile::tempdir().unwrap();
        let store = ManagedRuntimeArtifactStore::new(root.path().to_path_buf()).unwrap();
        let instance = RuntimeInstanceId::new("gemini:managed").unwrap();
        let config = RuntimeUpdateConfig {
            schema_version: EXTERNAL_AGENT_CONTRACT_VERSION,
            instance_id: instance.clone(),
            kind: ExternalAgentKind::Gemini,
            profile: RuntimeProfile::Managed,
            policy: RuntimeUpdatePolicy::AikitManaged,
            channel: RuntimeUpdateChannel::Stable,
            source: RuntimeUpdateSource {
                manifest_url: "https://updates.example.test/manifest.json".into(),
                signature_url: "https://updates.example.test/manifest.sig".into(),
                key_id: "stable-2026".into(),
                public_key_base64: base64::engine::general_purpose::STANDARD.encode([7_u8; 32]),
            },
        };
        store.write_update_config(&config).unwrap();
        assert_eq!(store.read_update_config(&instance).unwrap(), Some(config));

        let first = store.try_update_lock().unwrap();
        assert!(store.try_update_lock().is_err());
        drop(first);
        assert!(store.try_update_lock().is_ok());
    }
}
