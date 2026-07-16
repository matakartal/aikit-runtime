# Release evidence records

Each published version gets one immutable `vX.Y.Z.md` evidence record copied from
[`RELEASE-EVIDENCE-TEMPLATE.md`](../RELEASE-EVIDENCE-TEMPLATE.md). Keep credentials, private model
content, and raw provider responses out of these records.

The evidence record is committed after the exact source candidate; its `commit_sha` points to that
reachable source commit and the release tag targets the same SHA.

The directory is intentionally empty until a real release candidate has a non-placeholder version,
an exact committed source revision, current live-provider evidence, inspected multi-platform native
artifacts, and verified publishing authority.
