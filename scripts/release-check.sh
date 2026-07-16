#!/usr/bin/env bash
# Validate source-distribution invariants without making live-provider or registry claims.
set -euo pipefail

cd "$(dirname "$0")/.."

MODE="${1:---candidate}"
case "$MODE" in
  --candidate) ;;
  *)
    echo "usage: $0 [--candidate]" >&2
    exit 2
    ;;
esac

failures=0

pass() { printf 'PASS  %s\n' "$1"; }
fail() { printf 'FAIL  %s\n' "$1" >&2; failures=$((failures + 1)); }
note() { printf 'NOTE  %s\n' "$1"; }

require_file() {
  if [ -f "$1" ]; then pass "$1 exists"; else fail "$1 is missing"; fi
}

for file in \
  CHANGELOG.md \
  SECURITY.md \
  CODE_OF_CONDUCT.md \
  docs/RELEASE.md \
  docs/RELEASE-EVIDENCE-TEMPLATE.md \
  docs/V1-COMPLETION-MATRIX.md \
  .github/allowed_signers \
  .github/workflows/release.yml \
  .github/workflows/live-smoke.yml \
  .env.example; do
  require_file "$file"
done

cargo_version="$(awk -F'"' '/^version = "/ { print $2; exit }' Cargo.toml)"
python_version="$(awk -F'"' '/^version = "/ { print $2; exit }' crates/aikit-py/pyproject.toml)"
node_version="$(node -p "require('./crates/aikit-node/package.json').version")"

if [ -n "$cargo_version" ] && [ "$cargo_version" = "$python_version" ] && [ "$cargo_version" = "$node_version" ]; then
  pass "Cargo, Python, and Node versions agree at $cargo_version"
else
  fail "version drift: Cargo=$cargo_version Python=$python_version Node=$node_version"
fi

for manifest in crates/aikit-node/npm/*/package.json; do
  platform_name="$(node -p "require('./$manifest').name")"
  platform_version="$(node -p "require('./$manifest').version")"
  pinned_version="$(node -p "require('./crates/aikit-node/package.json').optionalDependencies['$platform_name'] || ''")"
  if [ "$platform_version" = "$node_version" ] && [ "$pinned_version" = "$node_version" ]; then
    pass "$platform_name is exactly pinned at $node_version"
  else
    fail "$platform_name version drift: package=$platform_version optional=$pinned_version root=$node_version"
  fi
done

metadata_versions="$(cargo metadata --no-deps --format-version 1 --locked | node -e '
let input = "";
process.stdin.on("data", chunk => input += chunk);
process.stdin.on("end", () => {
  const versions = [...new Set(JSON.parse(input).packages.map(pkg => pkg.version))];
  process.stdout.write(versions.join(","));
});
')"
if [ "$metadata_versions" = "$cargo_version" ]; then
  pass "all Cargo workspace packages use $cargo_version"
else
  fail "Cargo workspace package versions drift: $metadata_versions"
fi

if grep -Fq '## [Unreleased]' CHANGELOG.md; then
  pass "CHANGELOG.md has an Unreleased section"
else
  fail "CHANGELOG.md lacks an Unreleased section"
fi

if grep -Eq '^[A-Z0-9_]*(API_KEY|TOKEN)=[^[:space:]#]+' .env.example; then
  fail ".env.example contains a non-empty credential-like value"
else
  pass ".env.example contains no credential values"
fi

if grep -Fq 'aikit-runtime' README.md && grep -Fq '## Use from source' docs/RELEASE.md; then
  pass "source-first aikit-runtime distribution is documented"
else
  fail "source-first distribution path is not documented"
fi

if [ "$cargo_version" = "0.0.0" ]; then
  note "0.0.0 is accepted for source development"
fi
note "source checks do not claim live-provider or remote multi-platform proof"

if [ "$failures" -ne 0 ]; then
  printf '\n%d release check(s) failed.\n' "$failures" >&2
  exit 1
fi

printf '\nSource %s checks passed.\n' "${MODE#--}"
