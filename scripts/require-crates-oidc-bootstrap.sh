#!/usr/bin/env bash
# Trusted publishing can update an owned crate, but crates.io requires a manual first publish.
set -euo pipefail

cd "$(dirname "$0")/.."

if [ "$#" -eq 0 ]; then
  echo "usage: $0 <crate-name> [<crate-name> ...]" >&2
  exit 2
fi

for crate_name in "$@"; do
  set +e
  status="$(./scripts/registry-package-status.py crate-exists "$crate_name" 2>&1)"
  result=$?
  set -e
  case "$result" in
    0)
      printf 'PASS  crates.io owner bootstrap exists: %s\n' "$crate_name"
      ;;
    3)
      printf '%s\n' \
        "ERROR crates.io has no owner bootstrap for '$crate_name'." \
        "OIDC trusted publishing cannot create a crate's first release." \
        "Follow the one-time manual ownership bootstrap in docs/RELEASE.md, then configure the trusted publisher." >&2
      exit 1
      ;;
    *)
      printf 'ERROR cannot verify crates.io owner bootstrap for %s: %s\n' \
        "$crate_name" "$status" >&2
      exit "$result"
      ;;
  esac
done
