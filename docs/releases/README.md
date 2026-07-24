# Release evidence records

Manual source-distribution assemblies may get an immutable `vX.Y.Z.md` evidence record copied from
[`RELEASE-EVIDENCE-TEMPLATE.md`](../RELEASE-EVIDENCE-TEMPLATE.md). Keep credentials, private model
content, and raw provider responses out of these records.

## How evidence is committed

1. Freeze the exact source commit that will be tagged.
2. Copy the template to `docs/releases/vX.Y.Z.md` in a **follow-up** evidence commit.
3. Set `commit_sha` to the reachable source commit (not the evidence commit itself).
4. Fill toolchain, live-matrix, artifact, and authority fields without secrets.
5. Keep the record tied to the exact source SHA it describes.

These records describe GitHub Actions artifacts only; they do not authorize external publication.

## Records in this repository

| Version | Status | Notes |
|---|---|---|
| [v0.1.0](v0.1.0.md) | `draft` | Historical source/native artifact snapshot. |

There is currently no `v0.2.0` or `v0.3.0-alpha.1` evidence record. The version in workspace
manifests and current source documentation must not be interpreted as a tag, registry release, or
completed artifact review.

A future 0.3 record must retain both the raw pinned official A2A TCK report and the exact-set
verified-waiver gate result, plus the overall required-workflow state for the recorded source SHA.

No registry publication is claimed for this candidate. See [`RELEASE.md`](../RELEASE.md) for the
current source-first distribution policy and future release gates.
