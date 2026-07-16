---
source_version: 0.1.0
source_status: draft
commit_sha:
source_remote: pending
security_contact: pending
live_matrix: optional
native_artifacts: pending
---

# Source-distribution evidence: v0.1.0

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
| Rust stable |  |  |
| Rust MSRV 1.88 |  |  |
| Python ABI3 wheel matrix |  |  |
| Node.js native matrix |  |  |
| Rust/Python/Node conformance |  |  |
| Containment capability review |  |  |
| CodeQL review |  |  |
| Dependency advisory review |  |  |

## Optional live-provider matrix

Use dedicated low-limit credentials only if the maintainer explicitly chooses this billable test.
Record model ids and outcomes, never secrets.

| Provider | Model id | Text | Object | Governed deny | Two-request replay |
|---|---|---|---|---|---|
| Anthropic |  |  |  |  |  |
| OpenAI |  |  |  |  |  |
| DeepSeek |  |  |  |  |  |
| Google |  |  |  |  |  |

## Local artifact inspection

| Target | Artifact | SHA-256 | Install/load result |
|---|---|---|---|
| Rust source package |  |  |  |
| Python Linux/macOS/Windows |  |  |  |
| Node Linux/macOS/Windows |  |  |  |

## Safety sign-off

- Private security-reporting route verified by:
- Threat model and known limitations reviewed by:
- Unresolved high/critical security findings:
- Final source-distribution approval:
