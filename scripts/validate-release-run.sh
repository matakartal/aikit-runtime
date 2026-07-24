#!/usr/bin/env bash
# Bind an artifact run to the trusted release workflow, repository, source commit, and branch.
set -euo pipefail

RUN_ID="${1:-}"
EXPECTED_SHA="${2:-}"
EXPECTED_REF="${3:-}"
EXPECTED_REPOSITORY="${4:-${GITHUB_REPOSITORY:-}}"
test -n "$RUN_ID" && test -n "$EXPECTED_SHA" && test -n "$EXPECTED_REF" \
  && test -n "$EXPECTED_REPOSITORY" || {
  echo "usage: $0 <run-id> <source-sha> <refs/heads/source> <owner/repository>" >&2
  exit 2
}
case "$EXPECTED_REF" in refs/heads/*) ;; *) echo "release source must be a branch ref" >&2; exit 2 ;; esac

payload="$(gh api "repos/$EXPECTED_REPOSITORY/actions/runs/$RUN_ID")"
RUN_PAYLOAD="$payload" python3 - "$EXPECTED_SHA" "$EXPECTED_REF" "$EXPECTED_REPOSITORY" <<'PY'
import json
import os
import sys

expected_sha, expected_ref, expected_repository = sys.argv[1:]
payload = json.loads(os.environ["RUN_PAYLOAD"])
expected = {
    "path": ".github/workflows/release.yml",
    "conclusion": "success",
    "head_sha": expected_sha,
    "event": "workflow_dispatch",
    "head_branch": expected_ref.removeprefix("refs/heads/"),
}
for field, value in expected.items():
    if payload.get(field) != value:
        raise SystemExit(
            f"release run {field} mismatch: expected={value!r} actual={payload.get(field)!r}"
        )
repository = payload.get("head_repository")
actual_repository = repository.get("full_name") if isinstance(repository, dict) else None
if actual_repository != expected_repository:
    raise SystemExit(
        "release run repository mismatch: "
        f"expected={expected_repository!r} actual={actual_repository!r}"
    )
PY

printf 'PASS  release run %s matches trusted source %s at %s\n' "$RUN_ID" "$EXPECTED_REF" "$EXPECTED_SHA"
