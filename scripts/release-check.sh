#!/usr/bin/env bash
# Validate source-distribution invariants without making live-provider or registry claims.
set -euo pipefail

cd "$(dirname "$0")/.."

usage() {
  printf '%s\n' \
    "usage: $0 [--candidate]" \
    "       $0 --assert-native <target> <native-addon>" \
    "       $0 --assert-wheel <target> <wheel-directory>" \
    "       $0 --assert-bundle <bundle-directory>" >&2
  exit 2
}

workspace_version() {
  awk -F'"' '/^version = "/ { print $2; exit }' Cargo.toml
}

assert_native() {
  local target="$1"
  local native_file="$2"
  [ -f "$native_file" ] || {
    printf 'native addon not found: %s\n' "$native_file" >&2
    exit 1
  }

  node - "$target" "$native_file" <<'NODE'
const fs = require("node:fs");

const [target, nativeFile] = process.argv.slice(2);
const specs = {
  "darwin-arm64": { runtime: "darwin-arm64", format: "mach-o", machine: 0x0100000c },
  "darwin-x64": { runtime: "darwin-x64", format: "mach-o", machine: 0x01000007 },
  "linux-arm64-gnu": { runtime: "linux-arm64", format: "elf", machine: 183 },
  "linux-x64-gnu": { runtime: "linux-x64", format: "elf", machine: 62 },
  "win32-x64-msvc": { runtime: "win32-x64", format: "pe", machine: 0x8664 },
};
const spec = specs[target];
if (spec == null) throw new Error(`unsupported native target: ${target}`);

const runtime = `${process.platform}-${process.arch}`;
if (runtime !== spec.runtime) {
  throw new Error(`target ${target} is running on ${runtime}, expected ${spec.runtime}`);
}

const bytes = fs.readFileSync(nativeFile);
let machine;
if (spec.format === "elf") {
  if (bytes.length < 20 || !bytes.subarray(0, 4).equals(Buffer.from([0x7f, 0x45, 0x4c, 0x46]))) {
    throw new Error(`${nativeFile} is not an ELF binary`);
  }
  if (bytes[4] !== 2 || bytes[5] !== 1) {
    throw new Error(`${nativeFile} is not a little-endian 64-bit ELF binary`);
  }
  machine = bytes.readUInt16LE(18);
} else if (spec.format === "mach-o") {
  if (bytes.length < 8 || bytes.readUInt32LE(0) !== 0xfeedfacf) {
    throw new Error(`${nativeFile} is not a little-endian 64-bit Mach-O binary`);
  }
  machine = bytes.readUInt32LE(4);
} else {
  if (bytes.length < 64 || bytes[0] !== 0x4d || bytes[1] !== 0x5a) {
    throw new Error(`${nativeFile} is not a PE binary`);
  }
  const peOffset = bytes.readUInt32LE(0x3c);
  if (peOffset + 6 > bytes.length || bytes.readUInt32LE(peOffset) !== 0x00004550) {
    throw new Error(`${nativeFile} has an invalid PE header`);
  }
  machine = bytes.readUInt16LE(peOffset + 4);
}

if (machine !== spec.machine) {
  throw new Error(
    `${nativeFile} machine 0x${machine.toString(16)} does not match ${target} ` +
      `(expected 0x${spec.machine.toString(16)})`,
  );
}
console.log(`PASS  ${target} runtime and native header agree`);
NODE
}

assert_wheel() {
  local target="$1"
  local wheel_directory="$2"
  local python_bin="${PYTHON:-python3}"
  local version
  version="$(workspace_version)"

  "$python_bin" - "$target" "$wheel_directory" "$version" <<'PYTHON'
import pathlib
import re
import sys

target, wheel_directory, version = sys.argv[1:]
root = pathlib.Path(wheel_directory)
wheels = sorted(path for path in root.glob("*.whl") if path.is_file())
if len(wheels) != 1:
    raise SystemExit(f"expected exactly one wheel in {root}, found {len(wheels)}")

prefix = rf"aikit_runtime-{re.escape(version)}-cp39-abi3-"
patterns = {
    "linux-x64": prefix + r"manylinux_2_28_x86_64\.whl",
    "linux-arm64": prefix + r"manylinux_2_28_aarch64\.whl",
    "macos-arm64": prefix + r"macosx_[0-9]+_[0-9]+_arm64\.whl",
    "macos-x64": prefix + r"macosx_[0-9]+_[0-9]+_x86_64\.whl",
    "windows-x64": prefix + r"win_amd64\.whl",
}
pattern = patterns.get(target)
if pattern is None:
    raise SystemExit(f"unsupported wheel target: {target}")
if re.fullmatch(pattern, wheels[0].name) is None:
    raise SystemExit(f"wheel {wheels[0].name} does not match target {target}")
print(f"PASS  {target} wheel tag agrees: {wheels[0].name}")
PYTHON
}

assert_bundle() {
  local bundle_directory="$1"
  local python_bin="${PYTHON:-python3}"
  local version
  version="$(workspace_version)"

  "$python_bin" - "$bundle_directory" "$version" <<'PYTHON'
import pathlib
import re
import sys

root = pathlib.Path(sys.argv[1])
version = sys.argv[2]
if not root.is_dir():
    raise SystemExit(f"bundle directory not found: {root}")

for path in root.rglob("*"):
    if path.is_symlink():
        raise SystemExit(f"bundle must not contain symlinks: {path.relative_to(root)}")

files = {
    path.relative_to(root).as_posix()
    for path in root.rglob("*")
    if path.is_file()
}
fixed = {
    f"source-packages/aikit-runtime-core-{version}.crate",
    "source-packages/aikit-runtime-files.txt",
    f"node-wrapper/aikit-runtime-{version}.tgz",
    f"node-darwin-arm64/aikit-runtime-darwin-arm64-{version}.tgz",
    f"node-darwin-x64/aikit-runtime-darwin-x64-{version}.tgz",
    f"node-linux-arm64-gnu/aikit-runtime-linux-arm64-gnu-{version}.tgz",
    f"node-linux-x64-gnu/aikit-runtime-linux-x64-gnu-{version}.tgz",
    f"node-win32-x64-msvc/aikit-runtime-win32-x64-msvc-{version}.tgz",
}
missing = sorted(fixed - files)
remaining = set(files - fixed)

wheel_prefix = rf"aikit_runtime-{re.escape(version)}-cp39-abi3-"
wheel_patterns = {
    "python-linux-x64": wheel_prefix + r"manylinux_2_28_x86_64\.whl",
    "python-linux-arm64": wheel_prefix + r"manylinux_2_28_aarch64\.whl",
    "python-macos-arm64": wheel_prefix + r"macosx_[0-9]+_[0-9]+_arm64\.whl",
    "python-macos-x64": wheel_prefix + r"macosx_[0-9]+_[0-9]+_x86_64\.whl",
    "python-windows-x64": wheel_prefix + r"win_amd64\.whl",
}
wheel_errors = []
for directory, filename_pattern in wheel_patterns.items():
    pattern = re.compile(rf"{re.escape(directory)}/{filename_pattern}")
    matches = sorted(path for path in remaining if pattern.fullmatch(path))
    if len(matches) != 1:
        wheel_errors.append(f"{directory}: expected one matching wheel, found {matches}")
    else:
        remaining.remove(matches[0])

if missing or wheel_errors or remaining:
    details = []
    if missing:
        details.append("missing: " + ", ".join(missing))
    details.extend(wheel_errors)
    if remaining:
        details.append("unexpected: " + ", ".join(sorted(remaining)))
    raise SystemExit("invalid release artifact set; " + "; ".join(details))

print(f"PASS  exact release artifact set verified ({len(files)} files)")
PYTHON
}

MODE="${1:---candidate}"
case "$MODE" in
  --candidate)
    [ "$#" -le 1 ] || usage
    ;;
  --assert-native)
    [ "$#" -eq 3 ] || usage
    assert_native "$2" "$3"
    exit 0
    ;;
  --assert-wheel)
    [ "$#" -eq 3 ] || usage
    assert_wheel "$2" "$3"
    exit 0
    ;;
  --assert-bundle)
    [ "$#" -eq 2 ] || usage
    assert_bundle "$2"
    exit 0
    ;;
  *) usage ;;
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

cargo_version="$(workspace_version)"
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

# Never reuse a recorded release version for different source bytes. Rebuilding the exact clean
# commit is allowed for reproducibility; a different commit or a dirty tree must advance SemVer.
release_evidence="docs/releases/v${cargo_version}.md"
release_tag="v${cargo_version}"
head_commit="$(git rev-parse HEAD 2>/dev/null || true)"
repository_is_shallow="$(git rev-parse --is-shallow-repository 2>/dev/null || true)"
source_is_clean=false
if [ -n "$head_commit" ] && [ -z "$(git status --porcelain --untracked-files=normal)" ]; then
  source_is_clean=true
fi

if [ "$repository_is_shallow" = false ]; then
  pass "complete Git history is available for release-version collision checks"
else
  fail "release-version collision checks require a non-shallow Git checkout with fetched tags"
fi

if [ -f "$release_evidence" ]; then
  evidence_commit="$(awk -F': *' '$1 == "commit_sha" { print $2; exit }' "$release_evidence")"
  if [ -n "$evidence_commit" ] && [ "$evidence_commit" = "$head_commit" ] \
    && [ "$source_is_clean" = true ]; then
    pass "version $cargo_version reproduces its exact recorded clean source"
  else
    fail "version $cargo_version already has artifact evidence for different source; advance SemVer"
  fi
else
  pass "version $cargo_version has no recorded artifact-version collision"
fi

tag_commit="$(git rev-list -n 1 "$release_tag" 2>/dev/null || true)"
if [ -n "$tag_commit" ]; then
  if [ "$tag_commit" = "$head_commit" ] && [ "$source_is_clean" = true ]; then
    pass "$release_tag reproduces its exact clean tagged source"
  else
    fail "$release_tag already identifies different source; advance SemVer"
  fi
else
  pass "$release_tag is not already assigned in this checkout"
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

unpinned_actions="$(
  grep -RhE '^[[:space:]]*-[[:space:]]*uses:' .github/workflows \
    | grep -Ev '@[0-9a-f]{40}([[:space:]]|$)' \
    || true
)"
if [ -z "$unpinned_actions" ]; then
  pass "GitHub Actions dependencies are pinned to immutable commits"
else
  fail "workflow actions must use full commit SHAs: $unpinned_actions"
fi

for architecture in x86_64 aarch64; do
  if grep -Eq "manylinux_2_28_${architecture}@sha256:[0-9a-f]{64}" .github/workflows/release.yml; then
    pass "manylinux_2_28 ${architecture} image is digest-pinned"
  else
    fail "manylinux_2_28 ${architecture} image is not digest-pinned"
  fi
done

if grep -Fq 'cd dist/release' .github/workflows/release.yml \
  && grep -Fq 'sha256sum -c SHA256SUMS' .github/workflows/release.yml; then
  pass "artifact checksums are relative to and verifiable from the bundle root"
else
  fail "artifact checksum manifest is not self-verifying from the bundle root"
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
