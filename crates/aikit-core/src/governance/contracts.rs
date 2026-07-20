//! Versioned governance contracts shared by agent definitions, policy snapshots, approvals,
//! information-flow controls, sandbox profiles, and skill manifests.
//!
//! Security-sensitive documents are deterministic and fail closed. Policy snapshots and skill
//! manifests are sealed with a canonical SHA-256 digest; deserializing a tampered policy snapshot
//! fails before callers can evaluate it. Deny decisions are monotonic across every policy scope.

use serde::{de, Deserialize, Deserializer, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::net::IpAddr;
use thiserror::Error;

pub const GOVERNANCE_CONTRACT_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum GovernanceContractError {
    #[error("cannot serialize governance contract: {0}")]
    Serialization(String),
    #[error("invalid governance contract: {0}")]
    Invalid(String),
    #[error("integrity mismatch: expected {expected}, computed {actual}")]
    IntegrityMismatch { expected: String, actual: String },
    #[error("skill drift detected: expected {expected}, found {actual}")]
    SkillDrift { expected: String, actual: String },
}

fn canonical_digest<T: Serialize>(value: &T) -> Result<String, GovernanceContractError> {
    // All maps in these contracts are BTreeMaps and serde_json's Value map is ordered in the
    // workspace configuration. Struct field order is declaration order, yielding stable bytes.
    let bytes = serde_json::to_vec(value)
        .map_err(|error| GovernanceContractError::Serialization(error.to_string()))?;
    Ok(format!("sha256:{}", hex_sha256(&bytes)))
}

fn valid_digest(value: &str) -> bool {
    value
        .strip_prefix("sha256:")
        .is_some_and(|hex| hex.len() == 64 && hex.bytes().all(|byte| byte.is_ascii_hexdigit()))
}

pub(crate) fn sha256_digest(input: &[u8]) -> String {
    format!("sha256:{}", hex_sha256(input))
}

fn hex_sha256(input: &[u8]) -> String {
    const INITIAL: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
        0x5be0cd19,
    ];
    const K: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
        0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
        0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
        0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
        0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
        0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
        0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
        0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
        0xc67178f2,
    ];

    let bit_len = (input.len() as u64).wrapping_mul(8);
    let mut padded = input.to_vec();
    padded.push(0x80);
    while padded.len() % 64 != 56 {
        padded.push(0);
    }
    padded.extend_from_slice(&bit_len.to_be_bytes());

    let mut state = INITIAL;
    for block in padded.chunks_exact(64) {
        let mut words = [0u32; 64];
        for (index, bytes) in block.chunks_exact(4).enumerate() {
            words[index] = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        }
        for index in 16..64 {
            let s0 = words[index - 15].rotate_right(7)
                ^ words[index - 15].rotate_right(18)
                ^ (words[index - 15] >> 3);
            let s1 = words[index - 2].rotate_right(17)
                ^ words[index - 2].rotate_right(19)
                ^ (words[index - 2] >> 10);
            words[index] = words[index - 16]
                .wrapping_add(s0)
                .wrapping_add(words[index - 7])
                .wrapping_add(s1);
        }

        let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut h] = state;
        for index in 0..64 {
            let upper = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let choice = (e & f) ^ ((!e) & g);
            let temp1 = h
                .wrapping_add(upper)
                .wrapping_add(choice)
                .wrapping_add(K[index])
                .wrapping_add(words[index]);
            let lower = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let majority = (a & b) ^ (a & c) ^ (b & c);
            let temp2 = lower.wrapping_add(majority);
            h = g;
            g = f;
            f = e;
            e = d.wrapping_add(temp1);
            d = c;
            c = b;
            b = a;
            a = temp1.wrapping_add(temp2);
        }
        for (slot, value) in state.iter_mut().zip([a, b, c, d, e, f, g, h]) {
            *slot = slot.wrapping_add(value);
        }
    }

    let mut result = String::with_capacity(64);
    for word in state {
        use std::fmt::Write as _;
        let _ = write!(result, "{word:08x}");
    }
    result
}

// -------------------------------------------------------------------------------------------------
// Agent and skill contracts
// -------------------------------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolDescriptor {
    pub name: String,
    pub description: String,
    #[serde(default)]
    pub input_schema: Value,
    #[serde(default)]
    pub read_only: bool,
    #[serde(default)]
    pub requires_approval: bool,
    #[serde(default)]
    pub capabilities: BTreeSet<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentDefinition {
    #[serde(default = "contract_version")]
    pub schema_version: u32,
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub tenant_id: Option<String>,
    pub instructions: String,
    #[serde(default)]
    pub tools: Vec<ToolDescriptor>,
    #[serde(default)]
    pub skills: Vec<String>,
    #[serde(default)]
    pub metadata: BTreeMap<String, Value>,
    #[serde(default)]
    pub policy_snapshot_hash: Option<String>,
    pub sandbox_profile: SandboxProfile,
}

const fn contract_version() -> u32 {
    GOVERNANCE_CONTRACT_VERSION
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SkillManifest {
    pub schema_version: u32,
    pub name: String,
    pub version: String,
    pub description: String,
    pub entrypoint: String,
    pub tools: Vec<ToolDescriptor>,
    /// Relative artifact path to a pinned `sha256:<hex>` digest.
    pub artifacts: BTreeMap<String, String>,
    integrity_hash: String,
}

#[derive(Serialize)]
struct SkillPayload<'a> {
    schema_version: u32,
    name: &'a str,
    version: &'a str,
    description: &'a str,
    entrypoint: &'a str,
    tools: &'a [ToolDescriptor],
    artifacts: &'a BTreeMap<String, String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SkillManifestWire {
    schema_version: u32,
    name: String,
    version: String,
    description: String,
    entrypoint: String,
    #[serde(default)]
    tools: Vec<ToolDescriptor>,
    #[serde(default)]
    artifacts: BTreeMap<String, String>,
    integrity_hash: String,
}

impl<'de> Deserialize<'de> for SkillManifest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = SkillManifestWire::deserialize(deserializer)?;
        let manifest = Self {
            schema_version: wire.schema_version,
            name: wire.name,
            version: wire.version,
            description: wire.description,
            entrypoint: wire.entrypoint,
            tools: wire.tools,
            artifacts: wire.artifacts,
            integrity_hash: wire.integrity_hash,
        };
        manifest.validate_integrity().map_err(de::Error::custom)?;
        Ok(manifest)
    }
}

impl SkillManifest {
    pub fn seal(
        name: impl Into<String>,
        version: impl Into<String>,
        description: impl Into<String>,
        entrypoint: impl Into<String>,
        tools: Vec<ToolDescriptor>,
        artifacts: BTreeMap<String, String>,
    ) -> Result<Self, GovernanceContractError> {
        let mut manifest = Self {
            schema_version: GOVERNANCE_CONTRACT_VERSION,
            name: name.into(),
            version: version.into(),
            description: description.into(),
            entrypoint: entrypoint.into(),
            tools,
            artifacts,
            integrity_hash: String::new(),
        };
        manifest.validate_shape()?;
        manifest.integrity_hash = manifest.computed_integrity_hash()?;
        Ok(manifest)
    }

    pub fn integrity_hash(&self) -> &str {
        &self.integrity_hash
    }

    pub fn computed_integrity_hash(&self) -> Result<String, GovernanceContractError> {
        canonical_digest(&SkillPayload {
            schema_version: self.schema_version,
            name: &self.name,
            version: &self.version,
            description: &self.description,
            entrypoint: &self.entrypoint,
            tools: &self.tools,
            artifacts: &self.artifacts,
        })
    }

    pub fn validate_integrity(&self) -> Result<(), GovernanceContractError> {
        self.validate_shape()?;
        let actual = self.computed_integrity_hash()?;
        if actual != self.integrity_hash {
            return Err(GovernanceContractError::IntegrityMismatch {
                expected: self.integrity_hash.clone(),
                actual,
            });
        }
        Ok(())
    }

    pub fn validate_no_drift(&self, expected: &Self) -> Result<(), GovernanceContractError> {
        self.validate_integrity()?;
        expected.validate_integrity()?;
        if self.integrity_hash != expected.integrity_hash {
            return Err(GovernanceContractError::SkillDrift {
                expected: expected.integrity_hash.clone(),
                actual: self.integrity_hash.clone(),
            });
        }
        Ok(())
    }

    fn validate_shape(&self) -> Result<(), GovernanceContractError> {
        if self.schema_version != GOVERNANCE_CONTRACT_VERSION {
            return Err(GovernanceContractError::Invalid(format!(
                "unsupported skill schema version {}",
                self.schema_version
            )));
        }
        if self.name.trim().is_empty()
            || self.version.trim().is_empty()
            || self.entrypoint.trim().is_empty()
        {
            return Err(GovernanceContractError::Invalid(
                "skill name, version, and entrypoint must be non-empty".into(),
            ));
        }
        let mut tool_names = BTreeSet::new();
        for tool in &self.tools {
            if tool.name.trim().is_empty() || !tool_names.insert(tool.name.as_str()) {
                return Err(GovernanceContractError::Invalid(format!(
                    "skill '{}' contains an empty or duplicate tool name",
                    self.name
                )));
            }
        }
        for (path, digest) in &self.artifacts {
            if path_is_unsafe(path) || !valid_digest(digest) {
                return Err(GovernanceContractError::Invalid(format!(
                    "skill '{}' contains an unsafe or unpinned artifact '{path}'",
                    self.name
                )));
            }
        }
        Ok(())
    }
}

fn path_is_unsafe(path: &str) -> bool {
    path.trim().is_empty()
        || path.starts_with('/')
        || path.starts_with('\\')
        || path.contains('\0')
        || path.split(['/', '\\']).any(|component| component == "..")
}

// -------------------------------------------------------------------------------------------------
// Immutable, deny-wins policy snapshots
// -------------------------------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PolicyEffect {
    Allow,
    Ask,
    Deny,
}

impl PolicyEffect {
    fn precedence(self) -> u8 {
        match self {
            Self::Allow => 0,
            Self::Ask => 1,
            Self::Deny => 2,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "scope", rename_all = "snake_case", deny_unknown_fields)]
pub enum PolicyScope {
    Global,
    Tenant { tenant_id: String },
    Agent { agent_id: String },
    Run { run_id: String },
    Tool { tool: String },
}

impl PolicyScope {
    fn rank(&self) -> u8 {
        match self {
            Self::Global => 0,
            Self::Tenant { .. } => 1,
            Self::Agent { .. } => 2,
            Self::Run { .. } => 3,
            Self::Tool { .. } => 4,
        }
    }

    fn matches(&self, context: &PolicyEvaluationContext) -> bool {
        match self {
            Self::Global => true,
            Self::Tenant { tenant_id } => context.tenant_id.as_ref() == Some(tenant_id),
            Self::Agent { agent_id } => context.agent_id.as_ref() == Some(agent_id),
            Self::Run { run_id } => context.run_id.as_ref() == Some(run_id),
            Self::Tool { tool } => context.tool == *tool,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScopedPolicyRule {
    pub id: String,
    pub scope: PolicyScope,
    pub effect: PolicyEffect,
    #[serde(default)]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyDocument {
    #[serde(default = "contract_version")]
    pub schema_version: u32,
    pub default_effect: PolicyEffect,
    #[serde(default)]
    pub rules: Vec<ScopedPolicyRule>,
}

impl Default for PolicyDocument {
    fn default() -> Self {
        Self {
            schema_version: GOVERNANCE_CONTRACT_VERSION,
            default_effect: PolicyEffect::Deny,
            rules: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PolicySnapshot {
    policy: PolicyDocument,
    hash: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PolicySnapshotWire {
    policy: PolicyDocument,
    hash: String,
}

impl<'de> Deserialize<'de> for PolicySnapshot {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = PolicySnapshotWire::deserialize(deserializer)?;
        let snapshot = Self {
            policy: wire.policy,
            hash: wire.hash,
        };
        snapshot.validate().map_err(de::Error::custom)?;
        Ok(snapshot)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyEvaluationContext {
    #[serde(default)]
    pub tenant_id: Option<String>,
    #[serde(default)]
    pub agent_id: Option<String>,
    #[serde(default)]
    pub run_id: Option<String>,
    pub tool: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScopedPolicyDecision {
    pub effect: PolicyEffect,
    #[serde(default)]
    pub deciding_rule_id: Option<String>,
    #[serde(default)]
    pub matched_rule_ids: Vec<String>,
}

impl PolicySnapshot {
    pub fn seal(policy: PolicyDocument) -> Result<Self, GovernanceContractError> {
        validate_policy(&policy)?;
        let hash = canonical_digest(&policy)?;
        Ok(Self { policy, hash })
    }

    pub fn policy(&self) -> &PolicyDocument {
        &self.policy
    }

    pub fn hash(&self) -> &str {
        &self.hash
    }

    pub fn validate(&self) -> Result<(), GovernanceContractError> {
        validate_policy(&self.policy)?;
        let actual = canonical_digest(&self.policy)?;
        if actual != self.hash {
            return Err(GovernanceContractError::IntegrityMismatch {
                expected: self.hash.clone(),
                actual,
            });
        }
        Ok(())
    }

    /// Evaluate all matching scopes in the fixed global → tenant → agent → run → tool order.
    /// Effect precedence is monotonic: deny > ask > allow. A narrower allow can never weaken a
    /// broader deny, and vector order cannot change the result.
    pub fn evaluate(&self, context: &PolicyEvaluationContext) -> ScopedPolicyDecision {
        let mut matches: Vec<(usize, &ScopedPolicyRule)> = self
            .policy
            .rules
            .iter()
            .enumerate()
            .filter(|(_, rule)| rule.scope.matches(context))
            .collect();
        matches.sort_by_key(|(index, rule)| (rule.scope.rank(), *index));

        let effect = matches
            .iter()
            .map(|(_, rule)| rule.effect)
            .max_by_key(|effect| effect.precedence())
            .unwrap_or(self.policy.default_effect);
        let deciding_rule_id = matches
            .iter()
            .rev()
            .find(|(_, rule)| rule.effect == effect)
            .map(|(_, rule)| rule.id.clone());
        ScopedPolicyDecision {
            effect,
            deciding_rule_id,
            matched_rule_ids: matches
                .into_iter()
                .map(|(_, rule)| rule.id.clone())
                .collect(),
        }
    }
}

fn validate_policy(policy: &PolicyDocument) -> Result<(), GovernanceContractError> {
    if policy.schema_version != GOVERNANCE_CONTRACT_VERSION {
        return Err(GovernanceContractError::Invalid(format!(
            "unsupported policy schema version {}",
            policy.schema_version
        )));
    }
    let mut ids = BTreeSet::new();
    for rule in &policy.rules {
        if rule.id.trim().is_empty() || !ids.insert(rule.id.as_str()) {
            return Err(GovernanceContractError::Invalid(
                "policy rule ids must be non-empty and unique".into(),
            ));
        }
    }
    Ok(())
}

// -------------------------------------------------------------------------------------------------
// Provenance and source-to-sink information flow
// -------------------------------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataLabel {
    Public,
    Internal,
    Confidential,
    Restricted,
    Pii,
    Secret,
    Credential,
    Untrusted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataSourceKind {
    User,
    Model,
    Tool,
    File,
    Network,
    System,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Provenance {
    pub source: DataSourceKind,
    pub origin: String,
    #[serde(default)]
    pub labels: BTreeSet<DataLabel>,
    #[serde(default)]
    pub parents: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataSinkKind {
    User,
    Model,
    Tool,
    File,
    Network,
    Log,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DataSink {
    pub kind: DataSinkKind,
    pub target: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FlowEffect {
    Allow,
    Redact,
    RequireApproval,
    Deny,
}

impl FlowEffect {
    fn precedence(self) -> u8 {
        match self {
            Self::Allow => 0,
            Self::Redact => 1,
            Self::RequireApproval => 2,
            Self::Deny => 3,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceToSinkRule {
    pub id: String,
    #[serde(default)]
    pub source: Option<DataSourceKind>,
    #[serde(default)]
    pub required_labels: BTreeSet<DataLabel>,
    pub sink: DataSinkKind,
    pub effect: FlowEffect,
}

impl SourceToSinkRule {
    fn matches(&self, provenance: &Provenance, sink: &DataSink) -> bool {
        self.sink == sink.kind
            && self.source.is_none_or(|source| source == provenance.source)
            && self
                .required_labels
                .iter()
                .all(|label| provenance.labels.contains(label))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DataFlowPolicy {
    pub default_effect: FlowEffect,
    #[serde(default)]
    pub rules: Vec<SourceToSinkRule>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DataFlowDecision {
    pub effect: FlowEffect,
    #[serde(default)]
    pub matched_rule_ids: Vec<String>,
}

impl Default for DataFlowPolicy {
    fn default() -> Self {
        Self::secure_defaults()
    }
}

impl DataFlowPolicy {
    pub fn secure_defaults() -> Self {
        let rules = [
            (
                "deny_secret_network",
                DataLabel::Secret,
                DataSinkKind::Network,
            ),
            (
                "deny_credential_network",
                DataLabel::Credential,
                DataSinkKind::Network,
            ),
            ("deny_secret_logs", DataLabel::Secret, DataSinkKind::Log),
            (
                "deny_credential_logs",
                DataLabel::Credential,
                DataSinkKind::Log,
            ),
            ("redact_pii_logs", DataLabel::Pii, DataSinkKind::Log),
        ]
        .into_iter()
        .map(|(id, label, sink)| SourceToSinkRule {
            id: id.into(),
            source: None,
            required_labels: BTreeSet::from([label]),
            sink,
            effect: if id == "redact_pii_logs" {
                FlowEffect::Redact
            } else {
                FlowEffect::Deny
            },
        })
        .collect();
        Self {
            default_effect: FlowEffect::Allow,
            rules,
        }
    }

    pub fn evaluate(&self, provenance: &Provenance, sink: &DataSink) -> DataFlowDecision {
        let matching: Vec<&SourceToSinkRule> = self
            .rules
            .iter()
            .filter(|rule| rule.matches(provenance, sink))
            .collect();
        let effect = matching
            .iter()
            .map(|rule| rule.effect)
            .max_by_key(|effect| effect.precedence())
            .unwrap_or(self.default_effect);
        DataFlowDecision {
            effect,
            matched_rule_ids: matching.into_iter().map(|rule| rule.id.clone()).collect(),
        }
    }
}

// -------------------------------------------------------------------------------------------------
// Action-bound approval evidence
// -------------------------------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "scope", rename_all = "snake_case", deny_unknown_fields)]
pub enum ApprovalScope {
    ExactAction { action_digest: String },
    ToolForRun { run_id: String, tool: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalEvidenceOutcome {
    Approve,
    Deny,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovalEvidence {
    pub request_id: String,
    pub approver_id: String,
    pub outcome: ApprovalEvidenceOutcome,
    pub scope: ApprovalScope,
    pub policy_snapshot_hash: String,
    pub issued_at_unix_ms: u64,
    pub expires_at_unix_ms: u64,
    #[serde(default)]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovalCheckContext {
    pub action_digest: String,
    pub run_id: String,
    pub tool: String,
    pub policy_snapshot_hash: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalDenyReason {
    ExplicitlyDenied,
    InvalidEvidence,
    NotYetValid,
    TimedOut,
    PolicyChanged,
    ScopeMismatch,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "decision", rename_all = "snake_case", deny_unknown_fields)]
pub enum ApprovalEvidenceDecision {
    Allow,
    Deny { reason: ApprovalDenyReason },
}

impl ApprovalEvidence {
    pub fn evaluate(
        &self,
        context: &ApprovalCheckContext,
        now_unix_ms: u64,
    ) -> ApprovalEvidenceDecision {
        if self.request_id.trim().is_empty()
            || self.approver_id.trim().is_empty()
            || !valid_digest(&self.policy_snapshot_hash)
            || self.expires_at_unix_ms <= self.issued_at_unix_ms
        {
            return ApprovalEvidenceDecision::Deny {
                reason: ApprovalDenyReason::InvalidEvidence,
            };
        }
        if self.outcome == ApprovalEvidenceOutcome::Deny {
            return ApprovalEvidenceDecision::Deny {
                reason: ApprovalDenyReason::ExplicitlyDenied,
            };
        }
        if now_unix_ms < self.issued_at_unix_ms {
            return ApprovalEvidenceDecision::Deny {
                reason: ApprovalDenyReason::NotYetValid,
            };
        }
        // Expiry is exclusive: at the deadline the evidence is already unusable.
        if now_unix_ms >= self.expires_at_unix_ms {
            return ApprovalEvidenceDecision::Deny {
                reason: ApprovalDenyReason::TimedOut,
            };
        }
        if self.policy_snapshot_hash != context.policy_snapshot_hash {
            return ApprovalEvidenceDecision::Deny {
                reason: ApprovalDenyReason::PolicyChanged,
            };
        }
        let scope_matches = match &self.scope {
            ApprovalScope::ExactAction { action_digest } => {
                valid_digest(action_digest) && action_digest == &context.action_digest
            }
            ApprovalScope::ToolForRun { run_id, tool } => {
                !run_id.is_empty()
                    && !tool.is_empty()
                    && run_id == &context.run_id
                    && tool == &context.tool
            }
        };
        if !scope_matches {
            return ApprovalEvidenceDecision::Deny {
                reason: ApprovalDenyReason::ScopeMismatch,
            };
        }
        ApprovalEvidenceDecision::Allow
    }
}

// -------------------------------------------------------------------------------------------------
// Sandbox and egress profiles
// -------------------------------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EgressDecision {
    Allow,
    Deny,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EgressPolicy {
    pub default_decision: EgressDecision,
    #[serde(default)]
    pub allowed_domains: BTreeSet<String>,
    #[serde(default)]
    pub denied_domains: BTreeSet<String>,
    #[serde(default)]
    pub allow_loopback: bool,
    #[serde(default)]
    pub allow_private_networks: bool,
    #[serde(default)]
    pub allowed_unix_sockets: BTreeSet<String>,
}

impl EgressPolicy {
    pub fn deny_all() -> Self {
        Self {
            default_decision: EgressDecision::Deny,
            allowed_domains: BTreeSet::new(),
            denied_domains: BTreeSet::new(),
            allow_loopback: false,
            allow_private_networks: false,
            allowed_unix_sockets: BTreeSet::new(),
        }
    }

    pub fn evaluate_destination(&self, destination: &str) -> EgressDecision {
        let normalized = destination
            .trim()
            .trim_end_matches('.')
            .to_ascii_lowercase();
        if normalized.is_empty() {
            return EgressDecision::Deny;
        }
        if let Ok(ip) = normalized.parse::<IpAddr>() {
            if ip.is_loopback() && !self.allow_loopback {
                return EgressDecision::Deny;
            }
            if is_private_or_local(ip) && !self.allow_private_networks {
                return EgressDecision::Deny;
            }
        }
        if self
            .denied_domains
            .iter()
            .any(|pattern| domain_matches(pattern, &normalized))
        {
            return EgressDecision::Deny;
        }
        if self
            .allowed_domains
            .iter()
            .any(|pattern| domain_matches(pattern, &normalized))
        {
            return EgressDecision::Allow;
        }
        self.default_decision
    }
}

impl Default for EgressPolicy {
    fn default() -> Self {
        Self::deny_all()
    }
}

fn domain_matches(pattern: &str, domain: &str) -> bool {
    let pattern = pattern.trim().trim_end_matches('.').to_ascii_lowercase();
    if pattern == "*" {
        return false;
    }
    match pattern.strip_prefix("*.") {
        Some(suffix) => domain.len() > suffix.len() && domain.ends_with(&format!(".{suffix}")),
        None => pattern == domain,
    }
}

fn is_private_or_local(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            ip.is_private()
                || ip.is_link_local()
                || ip.is_broadcast()
                || ip.is_unspecified()
                || ip.octets()[0] == 0
        }
        IpAddr::V6(ip) => ip.is_unique_local() || ip.is_unicast_link_local() || ip.is_unspecified(),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilesystemProfile {
    ReadOnly,
    WorkspaceWrite,
    Unrestricted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxProfile {
    pub name: String,
    pub require_os_containment: bool,
    pub allow_unsandboxed_fallback: bool,
    pub filesystem: FilesystemProfile,
    pub egress: EgressPolicy,
    pub max_processes: u32,
    pub max_memory_bytes: u64,
    pub max_output_bytes: usize,
    pub allow_privilege_escalation: bool,
}

impl SandboxProfile {
    pub fn hardened() -> Self {
        Self {
            name: "hardened".into(),
            require_os_containment: true,
            allow_unsandboxed_fallback: false,
            filesystem: FilesystemProfile::WorkspaceWrite,
            egress: EgressPolicy::deny_all(),
            max_processes: 128,
            max_memory_bytes: 512 * 1024 * 1024,
            max_output_bytes: 4 * 1024 * 1024,
            allow_privilege_escalation: false,
        }
    }

    pub fn validate(&self) -> Result<(), GovernanceContractError> {
        if self.name.trim().is_empty()
            || self.max_processes == 0
            || self.max_memory_bytes == 0
            || self.max_output_bytes == 0
        {
            return Err(GovernanceContractError::Invalid(
                "sandbox profile name and resource limits must be non-zero".into(),
            ));
        }
        if self.require_os_containment && self.allow_unsandboxed_fallback {
            return Err(GovernanceContractError::Invalid(
                "required containment cannot allow an unsandboxed fallback".into(),
            ));
        }
        Ok(())
    }
}

impl Default for SandboxProfile {
    fn default() -> Self {
        Self::hardened()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn tool(name: &str) -> ToolDescriptor {
        ToolDescriptor {
            name: name.into(),
            description: format!("{name} tool"),
            input_schema: json!({"type": "object"}),
            read_only: false,
            requires_approval: true,
            capabilities: BTreeSet::new(),
        }
    }

    #[test]
    fn sha256_matches_the_standard_vector() {
        assert_eq!(
            hex_sha256(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn secret_to_network_is_denied_even_with_a_matching_allow() {
        let mut policy = DataFlowPolicy::secure_defaults();
        policy.rules.push(SourceToSinkRule {
            id: "allow_tool_network".into(),
            source: Some(DataSourceKind::Tool),
            required_labels: BTreeSet::from([DataLabel::Secret]),
            sink: DataSinkKind::Network,
            effect: FlowEffect::Allow,
        });
        let provenance = Provenance {
            source: DataSourceKind::Tool,
            origin: "Read".into(),
            labels: BTreeSet::from([DataLabel::Secret]),
            parents: vec![],
        };
        let decision = policy.evaluate(
            &provenance,
            &DataSink {
                kind: DataSinkKind::Network,
                target: "attacker.example".into(),
            },
        );
        assert_eq!(decision.effect, FlowEffect::Deny);
        assert!(decision
            .matched_rule_ids
            .contains(&"deny_secret_network".to_string()));
    }

    #[test]
    fn policy_snapshot_hash_is_stable_and_tampering_fails_deserialization() {
        let policy = PolicyDocument {
            schema_version: GOVERNANCE_CONTRACT_VERSION,
            default_effect: PolicyEffect::Ask,
            rules: vec![ScopedPolicyRule {
                id: "global-deny".into(),
                scope: PolicyScope::Global,
                effect: PolicyEffect::Deny,
                reason: Some("blocked".into()),
            }],
        };
        let first = PolicySnapshot::seal(policy.clone()).unwrap();
        let second = PolicySnapshot::seal(policy).unwrap();
        assert_eq!(first.hash(), second.hash());

        let mut encoded = serde_json::to_value(&first).unwrap();
        encoded["policy"]["rules"][0]["effect"] = json!("allow");
        let error = serde_json::from_value::<PolicySnapshot>(encoded).unwrap_err();
        assert!(error.to_string().contains("integrity mismatch"));
    }

    #[test]
    fn global_deny_beats_more_specific_allows_regardless_of_vector_order() {
        let rules = vec![
            ScopedPolicyRule {
                id: "tool-allow".into(),
                scope: PolicyScope::Tool {
                    tool: "Bash".into(),
                },
                effect: PolicyEffect::Allow,
                reason: None,
            },
            ScopedPolicyRule {
                id: "run-allow".into(),
                scope: PolicyScope::Run {
                    run_id: "run-1".into(),
                },
                effect: PolicyEffect::Allow,
                reason: None,
            },
            ScopedPolicyRule {
                id: "global-deny".into(),
                scope: PolicyScope::Global,
                effect: PolicyEffect::Deny,
                reason: None,
            },
        ];
        let snapshot = PolicySnapshot::seal(PolicyDocument {
            schema_version: GOVERNANCE_CONTRACT_VERSION,
            default_effect: PolicyEffect::Allow,
            rules,
        })
        .unwrap();
        let decision = snapshot.evaluate(&PolicyEvaluationContext {
            tenant_id: Some("tenant-1".into()),
            agent_id: Some("agent-1".into()),
            run_id: Some("run-1".into()),
            tool: "Bash".into(),
        });
        assert_eq!(decision.effect, PolicyEffect::Deny);
        assert_eq!(decision.deciding_rule_id.as_deref(), Some("global-deny"));
        assert_eq!(
            decision.matched_rule_ids,
            vec!["global-deny", "run-allow", "tool-allow"]
        );
    }

    #[test]
    fn skill_mutation_is_detected_as_integrity_drift() {
        let expected = SkillManifest::seal(
            "filesystem",
            "1.0.0",
            "safe filesystem operations",
            "run",
            vec![tool("Read")],
            BTreeMap::from([("skill.rs".into(), format!("sha256:{}", "a".repeat(64)))]),
        )
        .unwrap();
        let mut changed = expected.clone();
        changed.tools[0].description = "silently expanded authority".into();

        assert!(matches!(
            changed.validate_integrity(),
            Err(GovernanceContractError::IntegrityMismatch { .. })
        ));
        assert!(changed.validate_no_drift(&expected).is_err());
    }

    #[test]
    fn expired_approval_fails_closed_at_the_deadline() {
        let digest = format!("sha256:{}", "b".repeat(64));
        let policy_hash = format!("sha256:{}", "c".repeat(64));
        let evidence = ApprovalEvidence {
            request_id: "approval-1".into(),
            approver_id: "user:alice".into(),
            outcome: ApprovalEvidenceOutcome::Approve,
            scope: ApprovalScope::ExactAction {
                action_digest: digest.clone(),
            },
            policy_snapshot_hash: policy_hash.clone(),
            issued_at_unix_ms: 1_000,
            expires_at_unix_ms: 2_000,
            reason: Some("reviewed".into()),
        };
        let context = ApprovalCheckContext {
            action_digest: digest,
            run_id: "run-1".into(),
            tool: "Bash".into(),
            policy_snapshot_hash: policy_hash,
        };
        assert_eq!(
            evidence.evaluate(&context, 1_999),
            ApprovalEvidenceDecision::Allow
        );
        assert_eq!(
            evidence.evaluate(&context, 2_000),
            ApprovalEvidenceDecision::Deny {
                reason: ApprovalDenyReason::TimedOut
            }
        );
    }

    #[test]
    fn egress_deny_overrides_allow_and_private_ips_fail_closed() {
        let policy = EgressPolicy {
            default_decision: EgressDecision::Deny,
            allowed_domains: BTreeSet::from(["*.example.com".into()]),
            denied_domains: BTreeSet::from(["api.example.com".into()]),
            allow_loopback: false,
            allow_private_networks: false,
            allowed_unix_sockets: BTreeSet::new(),
        };
        assert_eq!(
            policy.evaluate_destination("api.example.com"),
            EgressDecision::Deny
        );
        assert_eq!(
            policy.evaluate_destination("docs.example.com"),
            EgressDecision::Allow
        );
        assert_eq!(
            policy.evaluate_destination("169.254.169.254"),
            EgressDecision::Deny
        );
    }
}
