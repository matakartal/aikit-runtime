# Release evidence records

Each published (or release-candidate) version gets one immutable `vX.Y.Z.md` evidence record
copied from [`RELEASE-EVIDENCE-TEMPLATE.md`](../RELEASE-EVIDENCE-TEMPLATE.md). Keep credentials,
private model content, and raw provider responses out of these records.

## How evidence is committed

1. Freeze the exact source commit that will be tagged.
2. Copy the template to `docs/releases/vX.Y.Z.md` in a **follow-up** evidence commit.
3. Set `commit_sha` to the reachable source commit (not the evidence commit itself).
4. Fill toolchain, live-matrix, artifact, and authority fields without secrets.
5. The release tag targets the same source SHA the evidence record describes.

`release_status: ready` plus `verified` / `passed` fields are required before
`./scripts/release-check.sh --release` accepts the record.

## Records in this repository

| Version | Status | Notes |
|---|---|---|
| [v0.1.0](v0.1.0.md) | `draft` | Historical source/native snapshot; refresh, dependency clearance, live matrix, and registry authority still pending. |

Until a record reaches `ready` and a signed version tag is pushed, no registry publication is
claimed. See [`RELEASE.md`](../RELEASE.md) for the full checklist.
