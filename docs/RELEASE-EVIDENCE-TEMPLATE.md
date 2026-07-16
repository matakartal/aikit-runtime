---
release_version: 0.0.0
release_status: draft
commit_sha:
source_remote: pending
registry_identity: pending
security_contact: pending
live_matrix: pending
native_artifacts: pending
signing_authority: pending
---

# Release evidence: v0.0.0

Copy this file to `docs/releases/vX.Y.Z.md` and replace every placeholder. The release gate accepts
only `release_status: ready`, `verified` authority fields, and `passed` test/artifact fields. Never
record API keys, access tokens, private prompts, or raw provider responses here.

## Exact source

- Commit SHA:
- Source remote URL:
- CI run URL:
- Signed tag:
- UTC evidence date:

## Toolchain and clean-checkout gates

| Gate | Version or result | Evidence |
|---|---|---|
| Rust stable |  |  |
| Rust MSRV 1.88 |  |  |
| Python wheel matrix |  |  |
| Node.js native matrix |  |  |
| Rust/Python/Node conformance |  |  |
| Containment capability review |  |  |

## Live-provider matrix

Use dedicated low-limit credentials. Record model ids and outcomes only.

| Provider | Model id | Text | Object | Governed deny | Two-request replay |
|---|---|---|---|---|---|
| Anthropic |  |  |  |  |  |
| OpenAI |  |  |  |  |  |
| DeepSeek |  |  |  |  |  |
| Google |  |  |  |  |  |

## Artifact inspection

| Registry/target | Final package name | Artifact | SHA-256 | Install/load result |
|---|---|---|---|---|
| crates.io core |  |  |  |  |
| crates.io facade |  |  |  |  |
| PyPI Linux |  |  |  |  |
| PyPI macOS |  |  |  |  |
| PyPI Windows |  |  |  |  |
| npm Linux |  |  |  |  |
| npm macOS |  |  |  |  |
| npm Windows |  |  |  |  |

## Authority and safety sign-off

- Registry ownership or coordinated rename evidence:
- Private security-reporting route verified by:
- Publishing identities verified by:
- Signing authority verified by:
- Threat model and known limitations reviewed by:
- Final release approval:
