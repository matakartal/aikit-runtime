#!/usr/bin/env bash
# Publish one crate safely across registry propagation and post-upload client failures.
set -euo pipefail

cd "$(dirname "$0")/.."

CRATE_NAME="${1:-}"
CRATE_VERSION="${2:-}"
ARTIFACT="${3:-}"
test "${4:-}" = "--" || {
  echo "usage: $0 <crate> <version> <local-crate> -- <cargo-publish-args...>" >&2
  exit 2
}
shift 4
test "$#" -gt 0 || { echo "cargo publish arguments are required" >&2; exit 2; }
test -f "$ARTIFACT" || { echo "crate artifact not found: $ARTIFACT" >&2; exit 2; }

CARGO_BIN="${AIKIT_CARGO_BIN:-cargo}"
ATTEMPTS="${AIKIT_REGISTRY_ATTEMPTS:-30}"
DELAY="${AIKIT_REGISTRY_DELAY_SECONDS:-10}"
case "$ATTEMPTS" in ''|*[!0-9]*|0) echo "invalid registry attempt count" >&2; exit 2 ;; esac
case "$DELAY" in ''|*[!0-9]*) echo "invalid registry delay" >&2; exit 2 ;; esac

crate_status() {
  local status
  if ./scripts/registry-package-status.py crate "$CRATE_NAME" "$CRATE_VERSION" "$ARTIFACT"; then
    return 0
  else
    status=$?
  fi
  return "$status"
}

initial_status=5
for attempt in $(seq 1 "$ATTEMPTS"); do
  if crate_status; then
    printf 'PASS  crates.io already has the exact artifact: %s %s\n' "$CRATE_NAME" "$CRATE_VERSION"
    exit 0
  else
    initial_status=$?
  fi
  case "$initial_status" in
    3) break ;;
    5) ;;
    *) exit "$initial_status" ;;
  esac
  if test "$attempt" -lt "$ATTEMPTS"; then sleep "$DELAY"; fi
done
if test "$initial_status" = 5; then
  echo "crates.io stayed transiently unavailable; refusing to publish without a missing-version proof" >&2
  exit 1
fi

set +e
"$CARGO_BIN" publish "$@"
publish_status=$?
set -e

for attempt in $(seq 1 "$ATTEMPTS"); do
  if crate_status; then
    if test "$publish_status" -ne 0; then
      printf 'PASS  recovered a post-upload Cargo failure for %s %s\n' "$CRATE_NAME" "$CRATE_VERSION"
    fi
    exit 0
  else
    status=$?
  fi
  case "$status" in 3|5) ;; *) exit "$status" ;; esac
  if test "$attempt" -lt "$ATTEMPTS"; then sleep "$DELAY"; fi
done

if test "$publish_status" -ne 0; then
  echo "cargo publish failed and the exact crate never appeared in the registry" >&2
  exit "$publish_status"
fi
echo "cargo publish returned success but the exact crate never appeared in the registry" >&2
exit 1
