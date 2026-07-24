#!/usr/bin/env bash
# Require exact registry checksum plus a real Cargo index resolution before dependents publish.
set -euo pipefail

cd "$(dirname "$0")/.."

CRATE_NAME="${1:-}"
CRATE_VERSION="${2:-}"
ARTIFACT="${3:-}"
ATTEMPTS="${4:-30}"
DELAY_SECONDS="${5:-10}"
CARGO_BIN="${AIKIT_CARGO_BIN:-cargo}"

case "$CRATE_NAME" in ''|*[!A-Za-z0-9_-]*) echo "invalid crate name: $CRATE_NAME" >&2; exit 2 ;; esac
case "$CRATE_VERSION" in ''|*[!A-Za-z0-9.+-]*) echo "invalid crate version: $CRATE_VERSION" >&2; exit 2 ;; esac
test -f "$ARTIFACT" || { echo "crate artifact not found: $ARTIFACT" >&2; exit 2; }
case "$ATTEMPTS" in ''|*[!0-9]*|0) echo "attempts must be a positive integer" >&2; exit 2 ;; esac
case "$DELAY_SECONDS" in ''|*[!0-9]*) echo "delay must be a non-negative integer" >&2; exit 2 ;; esac

probe="$(mktemp -d)"
trap 'rm -rf "$probe"' EXIT
printf '%s\n' \
  '[package]' \
  'name = "aikit-registry-resolution-probe"' \
  'version = "0.0.0"' \
  'edition = "2021"' \
  '' \
  '[workspace]' \
  '' \
  '[dependencies]' \
  "probe-dependency = { package = \"$CRATE_NAME\", version = \"=$CRATE_VERSION\", registry = \"crates-io\" }" \
  > "$probe/Cargo.toml"
mkdir -p "$probe/src"
printf '%s\n' 'fn main() {}' > "$probe/src/main.rs"

for attempt in $(seq 1 "$ATTEMPTS"); do
  if ./scripts/registry-package-status.py crate "$CRATE_NAME" "$CRATE_VERSION" "$ARTIFACT"; then
    if CARGO_NET_OFFLINE=false "$CARGO_BIN" fetch --manifest-path "$probe/Cargo.toml"; then
      printf 'PASS  Cargo resolved %s =%s from crates-io\n' "$CRATE_NAME" "$CRATE_VERSION"
      exit 0
    fi
  else
    registry_status=$?
    case "$registry_status" in
      3|5) ;;
      *) exit "$registry_status" ;;
    esac
  fi
  if test "$attempt" -lt "$ATTEMPTS"; then sleep "$DELAY_SECONDS"; fi
done

printf 'Cargo could not resolve exact registry artifact %s %s after %s attempts\n' \
  "$CRATE_NAME" "$CRATE_VERSION" "$ATTEMPTS" >&2
exit 1
