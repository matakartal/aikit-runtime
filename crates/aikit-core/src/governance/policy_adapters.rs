//! Fail-closed adapters for the native YAML policy format and external policy evaluators.
//!
//! OPA and Cedar policy languages are deliberately not evaluated here. Their adapters only
//! normalize a completed decision from an independently operated evaluator into auditable AIKit
//! evidence. Native and external evidence is then combined with monotonic deny-wins semantics.

use super::contracts::{
    GovernanceContractError, PolicyDocument, PolicyEffect, PolicyEvaluationContext, PolicyScope,
    PolicySnapshot, ScopedPolicyDecision, ScopedPolicyRule, GOVERNANCE_CONTRACT_VERSION,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use thiserror::Error;
use yaml_rust2::{
    parser::{Event, EventReceiver, Parser},
    Yaml, YamlLoader,
};

#[derive(Debug, Error)]
pub enum PolicyAdapterError {
    #[error("invalid native policy YAML: {0}")]
    InvalidYaml(String),
    #[error("invalid {engine} decision response: {message}")]
    InvalidExternalDecision {
        engine: &'static str,
        message: String,
    },
    #[error("{engine} returned no decision")]
    UndefinedDecision { engine: &'static str },
    #[error("{engine} returned a partial decision, which cannot authorize an action")]
    PartialDecision { engine: &'static str },
    #[error(transparent)]
    Governance(#[from] GovernanceContractError),
}

/// Strict parser for AIKit's native YAML policy document.
///
/// The accepted surface is intentionally smaller than YAML: exactly one document, no directives,
/// tags, anchors, aliases, merge keys, duplicate keys, implicit string coercion, or unknown fields.
pub struct NativeYamlPolicyAdapter;

impl NativeYamlPolicyAdapter {
    pub fn parse(input: &str) -> Result<PolicySnapshot, PolicyAdapterError> {
        reject_yaml_extensions(input)?;

        let mut guard = YamlEventGuard::default();
        Parser::new_from_str(input)
            .load(&mut guard, true)
            .map_err(|error| PolicyAdapterError::InvalidYaml(error.to_string()))?;
        guard.finish()?;

        let documents = YamlLoader::load_from_str(input)
            .map_err(|error| PolicyAdapterError::InvalidYaml(error.to_string()))?;
        if documents.len() != 1 {
            return Err(invalid_yaml("exactly one YAML document is required"));
        }

        let policy = parse_policy_document(&documents[0])?;
        PolicySnapshot::seal(policy).map_err(Into::into)
    }
}

fn reject_yaml_extensions(input: &str) -> Result<(), PolicyAdapterError> {
    if input.lines().any(|line| {
        let line = line.trim_start();
        line.starts_with("%YAML") || line.starts_with("%TAG")
    }) {
        return Err(invalid_yaml("YAML directives are not supported"));
    }
    Ok(())
}

#[derive(Default)]
struct YamlEventGuard {
    document_count: usize,
    violation: Option<String>,
}

impl YamlEventGuard {
    fn reject(&mut self, message: impl Into<String>) {
        if self.violation.is_none() {
            self.violation = Some(message.into());
        }
    }

    fn finish(self) -> Result<(), PolicyAdapterError> {
        if let Some(message) = self.violation {
            return Err(invalid_yaml(message));
        }
        if self.document_count != 1 {
            return Err(invalid_yaml("exactly one YAML document is required"));
        }
        Ok(())
    }
}

impl EventReceiver for YamlEventGuard {
    fn on_event(&mut self, event: Event) {
        match event {
            Event::DocumentStart => {
                self.document_count += 1;
                if self.document_count > 1 {
                    self.reject("multiple YAML documents are not supported");
                }
            }
            Event::Alias(_) => self.reject("YAML aliases are not supported"),
            Event::Scalar(_, _, anchor, tag) => {
                if anchor != 0 {
                    self.reject("YAML anchors are not supported");
                }
                if tag.is_some() {
                    self.reject("YAML tags are not supported");
                }
            }
            Event::SequenceStart(anchor, tag) | Event::MappingStart(anchor, tag) => {
                if anchor != 0 {
                    self.reject("YAML anchors are not supported");
                }
                if tag.is_some() {
                    self.reject("YAML tags are not supported");
                }
            }
            Event::Nothing
            | Event::StreamStart
            | Event::StreamEnd
            | Event::DocumentEnd
            | Event::SequenceEnd
            | Event::MappingEnd => {}
        }
    }
}

fn parse_policy_document(node: &Yaml) -> Result<PolicyDocument, PolicyAdapterError> {
    let fields = strict_mapping(node, "policy")?;
    ensure_only_fields(
        &fields,
        &["schema_version", "default_effect", "rules"],
        "policy",
    )?;

    let schema_version = match fields.get("schema_version") {
        Some(value) => strict_schema_version(value, "policy.schema_version")?,
        None => GOVERNANCE_CONTRACT_VERSION,
    };
    let default_effect = parse_effect(
        required(&fields, "default_effect", "policy")?,
        "policy.default_effect",
    )?;
    let rules = match fields.get("rules") {
        Some(value) => strict_sequence(value, "policy.rules")?
            .iter()
            .enumerate()
            .map(|(index, rule)| parse_rule(rule, index))
            .collect::<Result<Vec<_>, _>>()?,
        None => Vec::new(),
    };

    Ok(PolicyDocument {
        schema_version,
        default_effect,
        rules,
    })
}

fn parse_rule(node: &Yaml, index: usize) -> Result<ScopedPolicyRule, PolicyAdapterError> {
    let path = format!("policy.rules[{index}]");
    let fields = strict_mapping(node, &path)?;
    ensure_only_fields(
        &fields,
        &[
            "id",
            "scope",
            "tenant_id",
            "agent_id",
            "run_id",
            "tool",
            "effect",
            "reason",
        ],
        &path,
    )?;

    let id = strict_string(required(&fields, "id", &path)?, &format!("{path}.id"))?;
    let scope_name = strict_string(required(&fields, "scope", &path)?, &format!("{path}.scope"))?;
    let scope = match scope_name.as_str() {
        "global" => {
            reject_scope_fields(&fields, &path, &[])?;
            PolicyScope::Global
        }
        "tenant" => {
            reject_scope_fields(&fields, &path, &["tenant_id"])?;
            PolicyScope::Tenant {
                tenant_id: strict_string(
                    required(&fields, "tenant_id", &path)?,
                    &format!("{path}.tenant_id"),
                )?,
            }
        }
        "agent" => {
            reject_scope_fields(&fields, &path, &["agent_id"])?;
            PolicyScope::Agent {
                agent_id: strict_string(
                    required(&fields, "agent_id", &path)?,
                    &format!("{path}.agent_id"),
                )?,
            }
        }
        "run" => {
            reject_scope_fields(&fields, &path, &["run_id"])?;
            PolicyScope::Run {
                run_id: strict_string(
                    required(&fields, "run_id", &path)?,
                    &format!("{path}.run_id"),
                )?,
            }
        }
        "tool" => {
            reject_scope_fields(&fields, &path, &["tool"])?;
            PolicyScope::Tool {
                tool: strict_string(required(&fields, "tool", &path)?, &format!("{path}.tool"))?,
            }
        }
        other => {
            return Err(invalid_yaml(format!(
                "{path}.scope has unsupported value '{other}'"
            )))
        }
    };
    let effect = parse_effect(
        required(&fields, "effect", &path)?,
        &format!("{path}.effect"),
    )?;
    let reason = fields
        .get("reason")
        .map(|value| strict_string(value, &format!("{path}.reason")))
        .transpose()?;

    Ok(ScopedPolicyRule {
        id,
        scope,
        effect,
        reason,
    })
}

fn reject_scope_fields(
    fields: &BTreeMap<String, &Yaml>,
    path: &str,
    allowed: &[&str],
) -> Result<(), PolicyAdapterError> {
    for field in ["tenant_id", "agent_id", "run_id", "tool"] {
        if fields.contains_key(field) && !allowed.contains(&field) {
            return Err(invalid_yaml(format!(
                "{path}.{field} is invalid for the selected scope"
            )));
        }
    }
    Ok(())
}

fn strict_mapping<'a>(
    node: &'a Yaml,
    path: &str,
) -> Result<BTreeMap<String, &'a Yaml>, PolicyAdapterError> {
    let Yaml::Hash(mapping) = node else {
        return Err(invalid_yaml(format!("{path} must be a mapping")));
    };
    let mut result = BTreeMap::new();
    for (key, value) in mapping {
        let key = strict_string(key, &format!("{path} key"))?;
        if key == "<<" {
            return Err(invalid_yaml(format!(
                "{path} uses a YAML merge key, which is not supported"
            )));
        }
        if result.insert(key.clone(), value).is_some() {
            // YamlLoader also rejects duplicate keys. Keep this guard so the contract remains
            // fail-closed if its implementation changes.
            return Err(invalid_yaml(format!(
                "{path} contains duplicate key '{key}'"
            )));
        }
    }
    Ok(result)
}

fn strict_sequence<'a>(node: &'a Yaml, path: &str) -> Result<&'a [Yaml], PolicyAdapterError> {
    match node {
        Yaml::Array(values) => Ok(values),
        _ => Err(invalid_yaml(format!("{path} must be a sequence"))),
    }
}

fn strict_string(node: &Yaml, path: &str) -> Result<String, PolicyAdapterError> {
    match node {
        Yaml::String(value) if !value.trim().is_empty() => Ok(value.clone()),
        Yaml::String(_) => Err(invalid_yaml(format!("{path} must not be empty"))),
        Yaml::Integer(_) | Yaml::Real(_) | Yaml::Boolean(_) | Yaml::Null => Err(invalid_yaml(
            format!("{path} must be an unambiguous string; quote scalar-like values"),
        )),
        _ => Err(invalid_yaml(format!("{path} must be a string"))),
    }
}

fn strict_schema_version(node: &Yaml, path: &str) -> Result<u32, PolicyAdapterError> {
    match node {
        Yaml::Integer(value) if *value >= 0 => u32::try_from(*value)
            .map_err(|_| invalid_yaml(format!("{path} is outside the supported integer range"))),
        _ => Err(invalid_yaml(format!(
            "{path} must be an unquoted non-negative integer"
        ))),
    }
}

fn parse_effect(node: &Yaml, path: &str) -> Result<PolicyEffect, PolicyAdapterError> {
    match strict_string(node, path)?.as_str() {
        "allow" => Ok(PolicyEffect::Allow),
        "ask" => Ok(PolicyEffect::Ask),
        "deny" => Ok(PolicyEffect::Deny),
        value => Err(invalid_yaml(format!(
            "{path} has unsupported effect '{value}'"
        ))),
    }
}

fn required<'a>(
    fields: &BTreeMap<String, &'a Yaml>,
    field: &str,
    path: &str,
) -> Result<&'a Yaml, PolicyAdapterError> {
    fields
        .get(field)
        .copied()
        .ok_or_else(|| invalid_yaml(format!("{path}.{field} is required")))
}

fn ensure_only_fields(
    fields: &BTreeMap<String, &Yaml>,
    allowed: &[&str],
    path: &str,
) -> Result<(), PolicyAdapterError> {
    if let Some(field) = fields
        .keys()
        .find(|field| !allowed.contains(&field.as_str()))
    {
        return Err(invalid_yaml(format!("{path}.{field} is unknown")));
    }
    Ok(())
}

fn invalid_yaml(message: impl Into<String>) -> PolicyAdapterError {
    PolicyAdapterError::InvalidYaml(message.into())
}

// -------------------------------------------------------------------------------------------------
// External decision evidence
// -------------------------------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PolicyDecisionEngine {
    Native,
    Opa,
    Cedar,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExternalDecisionMetadata {
    /// Stable policy/package/query identifier requested from the external evaluator.
    pub policy_rule_id: String,
    pub input_summary: String,
    #[serde(default)]
    pub risk_evidence: Vec<String>,
    #[serde(default)]
    pub evaluator_revision: Option<String>,
}

impl ExternalDecisionMetadata {
    fn validate(&self, engine: &'static str) -> Result<(), PolicyAdapterError> {
        if self.input_summary.trim().is_empty() {
            return Err(invalid_external(engine, "input_summary must not be empty"));
        }
        if self.policy_rule_id.trim().is_empty() {
            return Err(invalid_external(engine, "policy_rule_id must not be empty"));
        }
        if self
            .risk_evidence
            .iter()
            .any(|evidence| evidence.trim().is_empty())
        {
            return Err(invalid_external(
                engine,
                "risk_evidence entries must not be empty",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuditablePolicyDecision {
    pub engine: PolicyDecisionEngine,
    pub effect: PolicyEffect,
    #[serde(default)]
    pub decision_id: Option<String>,
    #[serde(default)]
    pub deciding_rule_id: Option<String>,
    #[serde(default)]
    pub matched_rule_ids: Vec<String>,
    pub input_summary: String,
    #[serde(default)]
    pub risk_evidence: Vec<String>,
    #[serde(default)]
    pub evaluator_revision: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AggregatedPolicyDecision {
    pub effect: PolicyEffect,
    pub deciding_evidence_index: usize,
    pub evidence: Vec<AuditablePolicyDecision>,
}

/// Evaluate the immutable native snapshot and combine it with already-normalized external
/// decisions. Effect precedence is always deny > ask > allow.
pub fn evaluate_with_external(
    snapshot: &PolicySnapshot,
    context: &PolicyEvaluationContext,
    input_summary: impl Into<String>,
    risk_evidence: Vec<String>,
    external: impl IntoIterator<Item = AuditablePolicyDecision>,
) -> Result<AggregatedPolicyDecision, PolicyAdapterError> {
    let input_summary = input_summary.into();
    let metadata = ExternalDecisionMetadata {
        policy_rule_id: "aikit.native.snapshot".into(),
        input_summary: input_summary.clone(),
        risk_evidence: risk_evidence.clone(),
        evaluator_revision: Some(snapshot.hash().to_owned()),
    };
    metadata.validate("native")?;

    let native = snapshot.evaluate(context);
    let mut evidence = vec![native_evidence(
        native,
        input_summary,
        risk_evidence,
        snapshot.hash().to_owned(),
    )];
    for decision in external {
        validate_auditable_decision(&decision)?;
        evidence.push(decision);
    }

    let deciding_evidence_index = evidence
        .iter()
        .enumerate()
        .max_by_key(|(index, decision)| (effect_precedence(decision.effect), usize::MAX - *index))
        .map(|(index, _)| index)
        .expect("native evidence is always present");
    Ok(AggregatedPolicyDecision {
        effect: evidence[deciding_evidence_index].effect,
        deciding_evidence_index,
        evidence,
    })
}

fn native_evidence(
    decision: ScopedPolicyDecision,
    input_summary: String,
    risk_evidence: Vec<String>,
    policy_hash: String,
) -> AuditablePolicyDecision {
    AuditablePolicyDecision {
        engine: PolicyDecisionEngine::Native,
        effect: decision.effect,
        decision_id: Some(policy_hash.clone()),
        deciding_rule_id: decision.deciding_rule_id,
        matched_rule_ids: decision.matched_rule_ids,
        input_summary,
        risk_evidence,
        evaluator_revision: Some(policy_hash),
    }
}

fn validate_auditable_decision(
    decision: &AuditablePolicyDecision,
) -> Result<(), PolicyAdapterError> {
    let engine = match decision.engine {
        PolicyDecisionEngine::Native => "native",
        PolicyDecisionEngine::Opa => "opa",
        PolicyDecisionEngine::Cedar => "cedar",
    };
    if decision.input_summary.trim().is_empty() {
        return Err(invalid_external(engine, "input_summary must not be empty"));
    }
    if decision
        .matched_rule_ids
        .iter()
        .any(|rule| rule.trim().is_empty())
    {
        return Err(invalid_external(
            engine,
            "matched_rule_ids entries must not be empty",
        ));
    }
    Ok(())
}

fn effect_precedence(effect: PolicyEffect) -> u8 {
    match effect {
        PolicyEffect::Allow => 0,
        PolicyEffect::Ask => 1,
        PolicyEffect::Deny => 2,
    }
}

// -------------------------------------------------------------------------------------------------
// OPA REST adapter
// -------------------------------------------------------------------------------------------------

pub struct OpaDecisionAdapter;

impl OpaDecisionAdapter {
    /// Normalize a response from OPA's Data API. Missing/null results and partial decisions are
    /// errors, so callers cannot accidentally interpret an undefined policy as an allow.
    pub fn from_json(
        response: &str,
        metadata: ExternalDecisionMetadata,
    ) -> Result<AuditablePolicyDecision, PolicyAdapterError> {
        metadata.validate("opa")?;
        let envelope: OpaEnvelope = serde_json::from_str(response)
            .map_err(|error| invalid_external("opa", error.to_string()))?;
        let result = envelope
            .result
            .ok_or(PolicyAdapterError::UndefinedDecision { engine: "opa" })?;

        let (effect, deciding_rule_id, matched_rule_ids, mut result_risk) = match result {
            OpaResult::Boolean(allowed) => {
                let rule = metadata.policy_rule_id.clone();
                (
                    if allowed {
                        PolicyEffect::Allow
                    } else {
                        PolicyEffect::Deny
                    },
                    Some(rule.clone()),
                    vec![rule],
                    Vec::new(),
                )
            }
            OpaResult::Detailed(result) => {
                if result.partial {
                    return Err(PolicyAdapterError::PartialDecision { engine: "opa" });
                }
                let deciding_rule_id = result
                    .rule_id
                    .unwrap_or_else(|| metadata.policy_rule_id.clone());
                let mut matched = result.matched_rule_ids;
                if !matched.contains(&deciding_rule_id) {
                    matched.push(deciding_rule_id.clone());
                }
                (
                    result.effect,
                    Some(deciding_rule_id),
                    matched,
                    result.risk_evidence,
                )
            }
        };

        let mut risk_evidence = metadata.risk_evidence;
        risk_evidence.append(&mut result_risk);
        let decision = AuditablePolicyDecision {
            engine: PolicyDecisionEngine::Opa,
            effect,
            decision_id: envelope.decision_id,
            deciding_rule_id,
            matched_rule_ids,
            input_summary: metadata.input_summary,
            risk_evidence,
            evaluator_revision: metadata.evaluator_revision,
        };
        validate_auditable_decision(&decision)?;
        Ok(decision)
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OpaEnvelope {
    result: Option<OpaResult>,
    #[serde(default)]
    decision_id: Option<String>,
    #[serde(default, rename = "metrics")]
    _metrics: Option<Value>,
    #[serde(default, rename = "provenance")]
    _provenance: Option<Value>,
    #[serde(default, rename = "warning")]
    _warning: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum OpaResult {
    Boolean(bool),
    Detailed(OpaDetailedResult),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OpaDetailedResult {
    effect: PolicyEffect,
    #[serde(default)]
    rule_id: Option<String>,
    #[serde(default)]
    matched_rule_ids: Vec<String>,
    #[serde(default)]
    risk_evidence: Vec<String>,
    #[serde(default)]
    partial: bool,
}

// -------------------------------------------------------------------------------------------------
// Cedar authorization adapter
// -------------------------------------------------------------------------------------------------

pub struct CedarDecisionAdapter;

impl CedarDecisionAdapter {
    /// Normalize an authorization result produced by a Cedar evaluator. A matched forbid or an
    /// evaluator diagnostic error is a deny even if a buggy upstream wrapper labels it `Allow`.
    pub fn from_json(
        response: &str,
        metadata: ExternalDecisionMetadata,
    ) -> Result<AuditablePolicyDecision, PolicyAdapterError> {
        metadata.validate("cedar")?;
        let response: CedarAuthorizationResponse = serde_json::from_str(response)
            .map_err(|error| invalid_external("cedar", error.to_string()))?;

        let CedarAuthorizationResponse {
            decision,
            decision_id,
            permit_policy_ids,
            forbid_policy_ids,
            diagnostics,
            evaluator_revision: response_revision,
        } = response;

        let effect = if decision == CedarDecision::Deny
            || !forbid_policy_ids.is_empty()
            || !diagnostics.errors.is_empty()
        {
            PolicyEffect::Deny
        } else {
            PolicyEffect::Allow
        };

        let mut matched = BTreeSet::new();
        matched.extend(permit_policy_ids.iter().cloned());
        matched.extend(forbid_policy_ids.iter().cloned());
        matched.extend(diagnostics.reasons.iter().cloned());
        let deciding_rule_id = if effect == PolicyEffect::Deny {
            forbid_policy_ids
                .first()
                .cloned()
                .or_else(|| diagnostics.reasons.first().cloned())
                .or_else(|| Some(metadata.policy_rule_id.clone()))
        } else {
            permit_policy_ids
                .first()
                .cloned()
                .or_else(|| Some(metadata.policy_rule_id.clone()))
        };

        let mut risk_evidence = metadata.risk_evidence;
        risk_evidence.extend(
            diagnostics
                .errors
                .into_iter()
                .map(|error| format!("cedar_diagnostic:{error}")),
        );
        let decision = AuditablePolicyDecision {
            engine: PolicyDecisionEngine::Cedar,
            effect,
            decision_id,
            deciding_rule_id,
            matched_rule_ids: matched.into_iter().collect(),
            input_summary: metadata.input_summary,
            risk_evidence,
            evaluator_revision: response_revision.or(metadata.evaluator_revision),
        };
        validate_auditable_decision(&decision)?;
        Ok(decision)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
enum CedarDecision {
    Allow,
    Deny,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CedarAuthorizationResponse {
    decision: CedarDecision,
    #[serde(default)]
    decision_id: Option<String>,
    #[serde(default)]
    permit_policy_ids: Vec<String>,
    #[serde(default)]
    forbid_policy_ids: Vec<String>,
    #[serde(default)]
    diagnostics: CedarDiagnostics,
    #[serde(default)]
    evaluator_revision: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct CedarDiagnostics {
    #[serde(default)]
    reasons: Vec<String>,
    #[serde(default)]
    errors: Vec<String>,
}

fn invalid_external(engine: &'static str, message: impl Into<String>) -> PolicyAdapterError {
    PolicyAdapterError::InvalidExternalDecision {
        engine,
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn metadata() -> ExternalDecisionMetadata {
        ExternalDecisionMetadata {
            policy_rule_id: "external.default".into(),
            input_summary: "tool=network.fetch target=example.com".into(),
            risk_evidence: vec!["data_label=internal".into()],
            evaluator_revision: Some("bundle:42".into()),
        }
    }

    fn native_policy() -> PolicySnapshot {
        NativeYamlPolicyAdapter::parse(
            r#"
schema_version: 1
default_effect: allow
rules:
  - id: deny_network
    scope: tool
    tool: network.fetch
    effect: deny
    reason: network access is disabled
"#,
        )
        .expect("valid policy")
    }

    #[test]
    fn native_yaml_parses_and_seals_a_strict_policy() {
        let snapshot = native_policy();
        assert!(snapshot.hash().starts_with("sha256:"));
        assert_eq!(snapshot.policy().rules.len(), 1);
        assert_eq!(snapshot.policy().rules[0].effect, PolicyEffect::Deny);
    }

    #[test]
    fn malformed_duplicate_and_unknown_yaml_fail_closed() {
        for yaml in [
            "default_effect: [deny\n",
            "default_effect: deny\ndefault_effect: allow\n",
            "default_effect: deny\nunknown: true\n",
            "default_effect: deny\nrules: false\n",
        ] {
            assert!(
                NativeYamlPolicyAdapter::parse(yaml).is_err(),
                "unexpected success for {yaml:?}"
            );
        }
    }

    #[test]
    fn yaml_alias_tag_directive_multi_document_and_coercion_are_rejected() {
        for yaml in [
            "default_effect: &effect deny\nrules: []\n",
            "default_effect: !!str deny\nrules: []\n",
            "%YAML 1.2\n---\ndefault_effect: deny\n",
            "---\ndefault_effect: deny\n---\ndefault_effect: deny\n",
            "default_effect: deny\nrules:\n  - id: true\n    scope: global\n    effect: deny\n",
        ] {
            assert!(
                NativeYamlPolicyAdapter::parse(yaml).is_err(),
                "unexpected success for {yaml:?}"
            );
        }
    }

    #[test]
    fn opa_undefined_and_partial_decisions_fail_closed() {
        assert!(matches!(
            OpaDecisionAdapter::from_json("{}", metadata()),
            Err(PolicyAdapterError::UndefinedDecision { .. })
        ));
        assert!(matches!(
            OpaDecisionAdapter::from_json(
                r#"{"result":{"effect":"allow","partial":true}}"#,
                metadata()
            ),
            Err(PolicyAdapterError::PartialDecision { .. })
        ));
    }

    #[test]
    fn opa_decision_keeps_audit_evidence() {
        let decision = OpaDecisionAdapter::from_json(
            r#"{
                "decision_id":"opa-7",
                "result":{
                    "effect":"ask",
                    "rule_id":"rego.require_approval",
                    "matched_rule_ids":["rego.network"],
                    "risk_evidence":["destination=new"]
                }
            }"#,
            metadata(),
        )
        .expect("OPA decision");
        assert_eq!(decision.effect, PolicyEffect::Ask);
        assert_eq!(decision.decision_id.as_deref(), Some("opa-7"));
        assert!(decision
            .matched_rule_ids
            .contains(&"rego.require_approval".to_owned()));
        assert_eq!(decision.risk_evidence.len(), 2);
    }

    #[test]
    fn cedar_forbid_and_diagnostic_errors_override_permit() {
        let decision = CedarDecisionAdapter::from_json(
            r#"{
                "decision":"Allow",
                "permit_policy_ids":["permit.read"],
                "forbid_policy_ids":["forbid.secret_network"],
                "diagnostics":{"reasons":[],"errors":[]}
            }"#,
            metadata(),
        )
        .expect("Cedar decision");
        assert_eq!(decision.effect, PolicyEffect::Deny);
        assert_eq!(
            decision.deciding_rule_id.as_deref(),
            Some("forbid.secret_network")
        );

        let error_decision = CedarDecisionAdapter::from_json(
            r#"{
                "decision":"Allow",
                "permit_policy_ids":["permit.read"],
                "diagnostics":{"errors":["entity resolution failed"]}
            }"#,
            metadata(),
        )
        .expect("diagnostic errors normalize to deny");
        assert_eq!(error_decision.effect, PolicyEffect::Deny);
    }

    #[test]
    fn external_allow_cannot_override_native_deny() {
        let external =
            OpaDecisionAdapter::from_json(r#"{"result":true}"#, metadata()).expect("OPA allow");
        let combined = evaluate_with_external(
            &native_policy(),
            &PolicyEvaluationContext {
                tenant_id: None,
                agent_id: None,
                run_id: Some("run-1".into()),
                tool: "network.fetch".into(),
            },
            "network.fetch example.com",
            vec!["data_label=secret".into()],
            [external],
        )
        .expect("aggregate decision");

        assert_eq!(combined.effect, PolicyEffect::Deny);
        assert_eq!(combined.deciding_evidence_index, 0);
        assert_eq!(combined.evidence[0].engine, PolicyDecisionEngine::Native);
        assert_eq!(combined.evidence[1].effect, PolicyEffect::Allow);
    }
}
