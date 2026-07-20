---
source_version: 0.3.0-alpha.1
source_status: draft
commit_sha:
source_remote: pending
security_contact: pending
live_matrix: optional
native_artifacts: pending
---

# Source-distribution evidence: v0.3.0-alpha.1

Copy this file to `docs/releases/vX.Y.Z.md` when recording a manual artifact assembly. Never
record API keys, access tokens, private prompts, or raw provider responses here.

## Exact source

- Commit SHA:
- Source remote URL:
- CI run URL:
- Assembly run URL:
- UTC evidence date:

## Keyless gates

| Gate | Version or result | Evidence |
|---|---|---|
| Rust primary toolchain 1.97.1 |  |  |
| Rust MSRV 1.88 |  |  |
| Python ABI3 wheel matrix |  |  |
| Node.js native matrix |  |  |
| Rust/Python/Node conformance |  |  |
| Deterministic eval dataset(s) |  |  |
| Containment capability review |  |  |
| CodeQL review (`codeql.yml` run URL) |  |  |
| Dependency advisory/license review (`security.yml` cargo-deny run URL) |  |  |
| Committed-secret review (`security.yml` gitleaks run URL) |  |  |

## Optional live-provider matrix

Use dedicated low-limit credentials only if the maintainer explicitly chooses this billable test.
Record model ids and outcomes, never secrets.

| Provider | Model id | Text | Object | Governed deny | Two-request replay |
|---|---|---|---|---|---|
| Anthropic |  |  |  |  |  |
| OpenAI |  |  |  |  |  |
| Google |  |  |  |  |  |
| DeepSeek |  |  |  |  |  |
| OpenRouter |  |  |  |  |  |
| Groq |  |  |  |  |  |
| Mistral |  |  |  |  |  |
| xAI |  |  |  |  |  |

## Local artifact inspection

| Target | Artifact | SHA-256 | Install/load result |
|---|---|---|---|
| Rust source package |  |  |  |
| Python Linux/macOS/Windows |  |  |  |
| Node Linux/macOS/Windows |  |  |  |

Confirm that `SHA256SUMS` verifies from the extracted bundle root and that GitHub provenance
attestations match the exact source repository and workflow run.

## Safety sign-off

- Private security-reporting route verified by:
- Threat model and known limitations reviewed by:
- Registry publication intentionally absent or separately authorized by:
- Live-provider result correctly marked optional/pending by:
- Unresolved high/critical security findings:
- Final source-distribution approval:
