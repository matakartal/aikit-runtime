#!/usr/bin/env bash
# Validate release-candidate invariants without confusing them with live/provider publication proof.
set -euo pipefail

cd "$(dirname "$0")/.."

MODE="${1:---candidate}"
case "$MODE" in
  --candidate|--release) ;;
  *)
    echo "usage: $0 [--candidate|--release]" >&2
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

front_matter_value() {
  local file="$1" key="$2"
  awk -F': *' -v key="$key" '$1 == key { sub(/^[^:]*:[[:space:]]*/, ""); print; exit }' "$file"
}

for file in \
  CHANGELOG.md \
  SECURITY.md \
  CODE_OF_CONDUCT.md \
  docs/RELEASE.md \
  docs/RELEASE-EVIDENCE-TEMPLATE.md \
  docs/V1-COMPLETION-MATRIX.md \
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

if grep -Fq 'aikit-runtime' README.md && grep -Fq '## Registry identity' docs/RELEASE.md; then
  pass "coordinated aikit-runtime distribution identity is documented"
else
  fail "coordinated distribution identity is not documented"
fi

if [ "$MODE" = "--candidate" ]; then
  if [ "$cargo_version" = "0.0.0" ]; then
    note "0.0.0 is accepted only for the unpublished implementation candidate"
  fi
  note "candidate mode does not claim live-provider, registry, signing, or remote multi-platform proof"
else
  if [ "$cargo_version" = "0.0.0" ]; then
    fail "release mode rejects placeholder version 0.0.0"
  else
    pass "release version is non-placeholder"
  fi

  if git rev-parse --verify HEAD >/dev/null 2>&1; then
    pass "a committed source revision exists"
  else
    fail "no committed source revision exists"
  fi

  if [ -z "$(git status --porcelain)" ]; then
    pass "working tree is clean"
  else
    fail "working tree is not clean"
  fi

  if origin_url="$(git remote get-url origin 2>/dev/null)" && [ -n "$origin_url" ]; then
    pass "origin remote is configured: $origin_url"
  else
    fail "verified origin remote is not configured"
  fi

  if grep -Eq '^repository = "https://|^repository = "git\+https://' Cargo.toml; then
    pass "Cargo repository metadata is configured"
  else
    fail "Cargo repository metadata is missing"
  fi

  if grep -Eq 'TBD before (public )?release' SECURITY.md CODE_OF_CONDUCT.md; then
    fail "security or conduct contact is still marked TBD"
  else
    pass "public contact placeholders were removed"
  fi

  evidence="docs/releases/v${cargo_version}.md"
  if [ -f "$evidence" ]; then
    pass "$evidence exists"
    [ "$(front_matter_value "$evidence" release_version)" = "$cargo_version" ] \
      && pass "release evidence version matches" \
      || fail "release evidence version does not match $cargo_version"
    [ "$(front_matter_value "$evidence" release_status)" = "ready" ] \
      && pass "release evidence is marked ready" \
      || fail "release evidence is not marked ready"

    for field in source_remote registry_identity security_contact signing_authority; do
      [ "$(front_matter_value "$evidence" "$field")" = "verified" ] \
        && pass "$field is verified" \
        || fail "$field is not verified"
    done
    for field in live_matrix native_artifacts; do
      [ "$(front_matter_value "$evidence" "$field")" = "passed" ] \
        && pass "$field passed" \
        || fail "$field has not passed"
    done

    evidence_sha="$(front_matter_value "$evidence" commit_sha)"
    if [ -n "$evidence_sha" ] && git cat-file -e "$evidence_sha^{commit}" 2>/dev/null \
      && git merge-base --is-ancestor "$evidence_sha" HEAD; then
      pass "release evidence targets a committed source revision reachable from HEAD"
    else
      fail "release evidence commit_sha is missing, invalid, or not reachable from HEAD"
    fi
  else
    fail "$evidence is missing; copy and complete docs/RELEASE-EVIDENCE-TEMPLATE.md"
  fi
fi

if [ "$failures" -ne 0 ]; then
  printf '\n%d release check(s) failed.\n' "$failures" >&2
  exit 1
fi

printf '\nRelease %s checks passed.\n' "${MODE#--}"
