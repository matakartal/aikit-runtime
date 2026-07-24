#!/usr/bin/env bash
# Derive one safe npm dist-tag; only stable SemVer releases may use latest.
set -euo pipefail

VERSION="${1:-}"
python3 - "$VERSION" <<'PY'
import re
import sys

version = sys.argv[1]
pattern = re.compile(
    r"^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)"
    r"(?:-([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?"
    r"(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$"
)
match = pattern.fullmatch(version)
if match is None:
    raise SystemExit(f"version is not npm-compatible SemVer: {version}")
prerelease = match.group(4)
if prerelease is None:
    print("latest")
    raise SystemExit(0)
identifiers = prerelease.split(".")
for identifier in identifiers:
    if identifier.isdigit() and len(identifier) > 1 and identifier.startswith("0"):
        raise SystemExit(f"numeric prerelease identifier has a leading zero: {version}")
tag = identifiers[0]
if re.fullmatch(r"[a-z][a-z0-9-]*", tag) is None:
    raise SystemExit(f"prerelease does not provide a safe npm dist-tag: {version}")
if tag not in {"alpha", "beta", "rc"}:
    raise SystemExit(
        f"unsupported npm prerelease channel {tag!r}; expected alpha, beta, or rc"
    )
print(tag)
PY
