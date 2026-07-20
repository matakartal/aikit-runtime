#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

if [ "${AIKIT_LIVE_SMOKE:-}" != "1" ]; then
  echo "Refusing billable live calls: set AIKIT_LIVE_SMOKE=1 explicitly." >&2
  exit 2
fi

if [ "${AIKIT_LIVE_SMOKE_FULL:-}" = "1" ]; then
  echo "Running FULL live matrix: all eight key+model pairs are required before any call."
fi

if [ -n "${AIKIT_LIVE_SMOKE_BIN:-}" ]; then
  if [ ! -x "$AIKIT_LIVE_SMOKE_BIN" ]; then
    echo "Configured live-smoke binary is not executable: $AIKIT_LIVE_SMOKE_BIN" >&2
    exit 2
  fi

  exec "$AIKIT_LIVE_SMOKE_BIN" --ignored --nocapture
fi

cargo test -p aikit-runtime --test live_smoke --locked -- --ignored --nocapture
