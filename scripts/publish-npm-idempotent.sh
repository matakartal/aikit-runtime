#!/usr/bin/env bash
# Publish one npm tarball with exact integrity and dist-tag reconciliation.
set -euo pipefail

cd "$(dirname "$0")/.."

ARTIFACT="${1:-}"
TAG="${2:-}"
test -f "$ARTIFACT" || { echo "npm tarball not found: $ARTIFACT" >&2; exit 2; }
test -n "$TAG" || { echo "npm dist-tag is required" >&2; exit 2; }

NPM_BIN="${AIKIT_NPM_BIN:-npm}"
ATTEMPTS="${AIKIT_REGISTRY_ATTEMPTS:-30}"
DELAY="${AIKIT_REGISTRY_DELAY_SECONDS:-10}"
case "$ATTEMPTS" in ''|*[!0-9]*|0) echo "invalid registry attempt count" >&2; exit 2 ;; esac
case "$DELAY" in ''|*[!0-9]*) echo "invalid registry delay" >&2; exit 2 ;; esac

metadata="$(./scripts/registry-package-status.py npm-metadata "$ARTIFACT")"
IFS=$'\t' read -r PACKAGE_NAME PACKAGE_VERSION <<<"$metadata"
EXPECTED_TAG="$(./scripts/npm-release-tag.sh "$PACKAGE_VERSION")"
test "$TAG" = "$EXPECTED_TAG" || {
  printf 'npm tag mismatch for %s@%s: expected=%s supplied=%s\n' \
    "$PACKAGE_NAME" "$PACKAGE_VERSION" "$EXPECTED_TAG" "$TAG" >&2
  exit 1
}

npm_status() {
  local status
  if ./scripts/registry-package-status.py npm "$ARTIFACT"; then return 0; else status=$?; fi
  return "$status"
}

npm_tag_status() {
  local status
  if ./scripts/registry-package-status.py npm-tag "$ARTIFACT" "$TAG"; then return 0; else status=$?; fi
  return "$status"
}

print_tag_recovery() {
  printf '%s\n' \
    "ERROR exact npm package bytes exist, but the required '$TAG' tag is missing or behind." \
    "Trusted-publisher OIDC cannot repair dist-tags. An authenticated maintainer must run:" \
    "  npm dist-tag add '$PACKAGE_NAME@$PACKAGE_VERSION' '$TAG'" \
    "Then verify the registry tag before rerunning publication." >&2
}

print_tag_conflict() {
  printf '%s\n' \
    "ERROR npm dist-tag '$TAG' is ahead of $PACKAGE_VERSION; refusing a backward move." \
    "Do not run npm dist-tag add for this older version. Investigate the stale release instead." >&2
}

ensure_tag() {
  local attempt status
  for attempt in $(seq 1 "$ATTEMPTS"); do
    if npm_tag_status; then return 0; else status=$?; fi
    case "$status" in
      3|4|5) ;;
      6) print_tag_conflict; return "$status" ;;
      *) return "$status" ;;
    esac
    if test "$attempt" -lt "$ATTEMPTS"; then sleep "$DELAY"; fi
  done
  print_tag_recovery
  return 1
}

initial_status=5
for attempt in $(seq 1 "$ATTEMPTS"); do
  if npm_status; then
    ensure_tag
    printf 'PASS  npm already has the exact artifact and tag: %s@%s (%s)\n' \
      "$PACKAGE_NAME" "$PACKAGE_VERSION" "$TAG"
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
  echo "npm stayed transiently unavailable; refusing to publish without a missing-version proof" >&2
  exit 1
fi

# A publish mutates the selected dist-tag atomically with the new version. Check its current
# target before uploading so an older/stale workflow can never move alpha/beta/rc/latest backward.
preflight_tag_status=5
for attempt in $(seq 1 "$ATTEMPTS"); do
  if npm_tag_status; then
    echo "npm dist-tag points at a version whose package metadata is missing" >&2
    exit 1
  else
    preflight_tag_status=$?
  fi
  case "$preflight_tag_status" in
    3|4) break ;;
    5) ;;
    *) exit "$preflight_tag_status" ;;
  esac
  if test "$attempt" -lt "$ATTEMPTS"; then sleep "$DELAY"; fi
done
if test "$preflight_tag_status" = 5; then
  echo "npm dist-tag lookup stayed transiently unavailable; refusing to publish" >&2
  exit 1
fi

set +e
"$NPM_BIN" publish "$ARTIFACT" --tag "$TAG"
publish_status=$?
set -e

for attempt in $(seq 1 "$ATTEMPTS"); do
  if npm_status; then
    ensure_tag
    if test "$publish_status" -ne 0; then
      printf 'PASS  recovered a post-upload npm failure for %s@%s\n' "$PACKAGE_NAME" "$PACKAGE_VERSION"
    fi
    exit 0
  else
    status=$?
  fi
  case "$status" in 3|5) ;; *) exit "$status" ;; esac
  if test "$attempt" -lt "$ATTEMPTS"; then sleep "$DELAY"; fi
done

if test "$publish_status" -ne 0; then
  echo "npm publish failed and the exact package never appeared in the registry" >&2
  exit "$publish_status"
fi
echo "npm publish returned success but the exact package never appeared in the registry" >&2
exit 1
