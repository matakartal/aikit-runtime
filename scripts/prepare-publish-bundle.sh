#!/usr/bin/env bash
# Verify one assembled release bundle and stage exactly one copy of each registry payload.
set -euo pipefail

cd "$(dirname "$0")/.."

REGISTRY="${1:-}"
BUNDLE_ROOT="${2:-}"
OUTPUT_DIR="${3:-}"

case "$REGISTRY" in
  pypi|npm) ;;
  *)
    echo "usage: $0 <pypi|npm> <assembled-bundle> <new-output-directory>" >&2
    exit 2
    ;;
esac

test -d "$BUNDLE_ROOT" || {
  printf 'assembled bundle not found: %s\n' "$BUNDLE_ROOT" >&2
  exit 1
}
test -n "$OUTPUT_DIR" || {
  echo "output directory cannot be empty" >&2
  exit 2
}
test ! -e "$OUTPUT_DIR" || {
  printf 'refusing to replace existing publish output: %s\n' "$OUTPUT_DIR" >&2
  exit 1
}

./scripts/release-check.sh --assert-bundle "$BUNDLE_ROOT"
./scripts/verify-checksum-manifest.py "$BUNDLE_ROOT"

files=()
case "$REGISTRY" in
  pypi)
    while IFS= read -r file; do files+=("$file"); done < <(
      find "$BUNDLE_ROOT" -mindepth 2 -maxdepth 2 -type f \
        -path "$BUNDLE_ROOT/python-*/*.whl" -print | sort
    )
    expected=5
    ;;
  npm)
    while IFS= read -r file; do files+=("$file"); done < <(
      find "$BUNDLE_ROOT" -mindepth 2 -maxdepth 2 -type f \
        -path "$BUNDLE_ROOT/node-*/*.tgz" -print | sort
    )
    expected=6
    ;;
esac

if test "${#files[@]}" -ne "$expected"; then
  printf 'expected %s %s payloads in the assembled bundle, found %s\n' \
    "$expected" "$REGISTRY" "${#files[@]}" >&2
  exit 1
fi

mkdir -p "$OUTPUT_DIR"
for file in "${files[@]}"; do
  cp "$file" "$OUTPUT_DIR/"
done

staged_count="$(find "$OUTPUT_DIR" -mindepth 1 -maxdepth 1 -type f | wc -l | tr -d ' ')"
test "$staged_count" = "$expected" || {
  echo "publish payload basenames collided while staging" >&2
  exit 1
}
printf 'PASS  staged %s verified %s payloads in %s\n' "$expected" "$REGISTRY" "$OUTPUT_DIR"
