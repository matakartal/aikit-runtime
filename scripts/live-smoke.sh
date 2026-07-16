#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

if [ "${AIKIT_LIVE_SMOKE:-}" != "1" ]; then
  echo "Refusing billable live calls: set AIKIT_LIVE_SMOKE=1 explicitly." >&2
  exit 2
fi

if [ "${AIKIT_LIVE_SMOKE_FULL:-}" = "1" ]; then
  echo "Running FULL live matrix: all four key+model pairs are required before any call."
fi

cargo test -p aikit --test live_smoke -- --ignored --nocapture
