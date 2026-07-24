# Regulatory evidence aids

**This page is not legal advice, and aikit is not a compliance product.** Using aikit does not
make an application compliant with any regulation, and nothing here claims conformity with the
EU AI Act or any other framework. What a governed runtime *can* honestly do is produce durable,
typed evidence of how an agent actually behaved — the raw material that compliance, audit, and
risk teams ask application owners for. This page maps aikit's existing artifacts to the kinds of
questions those teams ask.

## Context: obligations around 2 August 2026

As of this page's 24 July 2026 review, the EU AI Act's Article 50 transparency obligations are
scheduled to apply from 2 August 2026. General-purpose AI model-provider obligations have applied
since 2 August 2025, while the European Commission says its full enforcement powers for those
obligations apply from 2 August 2026. See the Commission's [Article 50
guidelines](https://digital-strategy.ec.europa.eu/en/library/guidelines-transparency-obligations-providers-and-deployers-ai-systems)
and the official [AI Act text](https://eur-lex.europa.eu/eli/reg/2024/1689/).

Application teams deploying agents in scope of these rules need to answer
questions like "what did the agent do, who approved it, and what evidence exists?" — with
records rather than recollection.

Whether a given application is in scope, what role it has (provider/deployer), and what its
obligations are remain determinations for the application owner and their counsel.

## What aikit records that evidence processes can use

| Question an assessor asks | aikit artifact | Where |
|---|---|---|
| What did the agent do, in order? | Typed audit lifecycle events (run, route, provider attempt, permission decision, hook, tool, usage, budget, subagent) with sequence numbers, metadata-only by default | `observability.rs`, JSONL sinks |
| Who authorized a side effect? | Permission decisions with source; async approval outcomes; smart-approval escalations | audit events, `governance/` |
| Did a human stay in the loop for privilege growth? | Capability-request grants: agent asks, human decides, the grant is recorded — no silent escalation | `governance/capability.rs` |
| How were secrets and personal data kept out of records? | Deterministic secret/PII redaction applied to audit and guardrail paths; fail-closed external safety-server interop | `governance/guardrail.rs` |
| Can behavior claims be re-verified? | Deterministic eval reports carrying the exact dataset SHA-256, per-case verdicts, and usage — same outcome, same verdict | `aikit eval`, `evals/`, the `ci.yml` eval job |
| What exactly shipped? | CycloneDX SBOM bound to the commit and `Cargo.lock` digest; SLSA build provenance on assembled artifacts; dependency advisory/license gates | `scripts/security-check.sh`, `security.yml`, `release.yml` |
| What are the isolation guarantees? | Honest per-backend containment guarantee tables (never a single "sandboxed" boolean) and a threat model that states limits | `containment_capabilities()`, [THREAT-MODEL.md](THREAT-MODEL.md) |

## What aikit deliberately does not do

- It does not mark or watermark model outputs as AI-generated (Article 50 output-marking is an
  application/model-provider concern; a runtime inserting marks could also corrupt tool traffic).
- It does not classify your system's risk tier, generate conformity documentation, or assess
  scope — those are legal determinations.
- It does not promise that audit records satisfy any specific evidentiary standard. Records are
  metadata-only by default; retaining payloads, retention windows, and access control are host
  decisions.
- A keyless test suite cannot prove live-provider behavior; live acceptance remains a separate,
  explicit, billable gate (see [LIVE-SMOKE.md](LIVE-SMOKE.md)).

If a claim on this page and the code disagree, the code is the truth and this page has a bug —
please report it.
