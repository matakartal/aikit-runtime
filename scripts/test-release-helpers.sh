#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

TMP="$(mktemp -d)"
HTTP_PID=""
cleanup() {
  if [ -n "${HTTP_PID:-}" ]; then
    kill "$HTTP_PID" 2>/dev/null || true
    wait "$HTTP_PID" 2>/dev/null || true
  fi
  rm -rf "$TMP"
}
trap cleanup EXIT

expect_failure() {
  local message="$1"
  shift
  if "$@" >"$TMP/expected-failure.out" 2>&1; then
    echo "$message" >&2
    exit 1
  fi
}

# The release candidate gate shares this exact clean-source predicate. Exercise it in an isolated
# Git fixture so a developer's unrelated dirty workspace cannot make this regression flaky.
source_fixture="$TMP/release-source"
mkdir -p "$source_fixture"
git -C "$source_fixture" init --quiet
printf 'committed source\n' > "$source_fixture/tracked.txt"
git -C "$source_fixture" add tracked.txt
git -C "$source_fixture" \
  -c user.name='AIKit release fixture' -c user.email='release-fixture@example.invalid' \
  -c commit.gpgsign=false \
  commit --quiet -m 'fixture'
./scripts/release-check.sh --assert-clean-source "$source_fixture" >/dev/null

printf 'dirty tracked source\n' >> "$source_fixture/tracked.txt"
expect_failure "dirty tracked release source unexpectedly passed" \
  ./scripts/release-check.sh --assert-clean-source "$source_fixture"
grep -Fq 'release candidate source must be a clean Git checkout' "$TMP/expected-failure.out"
git -C "$source_fixture" show HEAD:tracked.txt > "$source_fixture/tracked.txt"

printf 'untracked package source\n' > "$source_fixture/untracked.rs"
expect_failure "untracked release source unexpectedly passed" \
  ./scripts/release-check.sh --assert-clean-source "$source_fixture"
grep -Fq 'release candidate source must be a clean Git checkout' "$TMP/expected-failure.out"
rm "$source_fixture/untracked.rs"
./scripts/release-check.sh --assert-clean-source "$source_fixture" >/dev/null

# Semantic workflow parsing: quoted/flow values, aliases, merges, and local actions all work.
pins="$TMP/pins"
mkdir -p "$pins"
cat > "$pins/valid.yml" <<'YAML'
"x-ref": &pinned "owner/action@abcdef0123456789abcdef0123456789abcdef01"
x-shared: &shared
  "uses": "owner/repository/.github/workflows/check.yml@0123456789abcdef0123456789abcdef01234567"
jobs:
  inherited: { <<: *shared, "with": { "mode": "strict" } }
  alias:
    uses: *pinned
  local: { uses: "./actions/local" }
  container: { uses: "docker://example.invalid/tool@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef" }
YAML
./scripts/check-workflow-pins.sh "$pins" >/dev/null

cat > "$pins/invalid.yml" <<'YAML'
jobs: { unsafe: { "uses": "owner/action@main" } }
YAML
expect_failure "mutable quoted flow-style workflow reference unexpectedly passed" \
  ./scripts/check-workflow-pins.sh "$pins"
grep -Fq 'owner/action@main' "$TMP/expected-failure.out"
rm "$pins/invalid.yml"

cat > "$pins/invalid.yml" <<'YAML'
jobs:
  duplicate:
    "uses": owner/action@abcdef0123456789abcdef0123456789abcdef01
    uses: owner/action@abcdef0123456789abcdef0123456789abcdef01
YAML
expect_failure "duplicate YAML key unexpectedly passed" ./scripts/check-workflow-pins.sh "$pins"
grep -Fq 'duplicate YAML key "uses"' "$TMP/expected-failure.out"
rm "$pins/invalid.yml"

cat > "$pins/invalid.yml" <<'YAML'
x-ref: &mutable owner/action@main
jobs:
  unsafe: { uses: *mutable }
YAML
expect_failure "mutable aliased workflow reference unexpectedly passed" \
  ./scripts/check-workflow-pins.sh "$pins"
grep -Fq 'owner/action@main' "$TMP/expected-failure.out"
rm "$pins/invalid.yml"

cat > "$pins/invalid.yml" <<'YAML'
jobs:
  unsafe:
    uses: docker://example.invalid/tool@0123456789abcdef0123456789abcdef01234567
YAML
expect_failure "docker action without sha256 digest unexpectedly passed" \
  ./scripts/check-workflow-pins.sh "$pins"
grep -Fq 'docker://example.invalid/tool@0123456789abcdef' "$TMP/expected-failure.out"
rm "$pins/invalid.yml"

# npm SemVer and release-channel tags are explicit; latest remains stable-only.
test "$(./scripts/npm-release-tag.sh 0.3.0-alpha.1)" = alpha
test "$(./scripts/npm-release-tag.sh 1.2.3-beta.2)" = beta
test "$(./scripts/npm-release-tag.sh 1.2.3-rc.3)" = rc
test "$(./scripts/npm-release-tag.sh 1.2.3)" = latest
expect_failure "prerelease unexpectedly acquired npm latest" \
  ./scripts/npm-release-tag.sh 1.2.3-latest.1
expect_failure "invalid numeric prerelease unexpectedly passed npm SemVer" \
  ./scripts/npm-release-tag.sh 1.2.3-alpha.01
expect_failure "semver-range-like v1 npm tag unexpectedly passed" \
  ./scripts/npm-release-tag.sh 1.2.3-v1.0
expect_failure "semver wildcard x npm tag unexpectedly passed" \
  ./scripts/npm-release-tag.sh 1.2.3-x.1
expect_failure "semver wildcard star npm tag unexpectedly passed" \
  ./scripts/npm-release-tag.sh '1.2.3-*.1'

# Exact release bundle and strict checksum-manifest coverage.
version="$(awk -F'"' '/^version = "/ { print $2; exit }' Cargo.toml)"
wheel_version="$(python3 - "$version" <<'PY'
import re
import sys
version = re.sub(r"-alpha\.(\d+)$", r"a\1", sys.argv[1])
version = re.sub(r"-beta\.(\d+)$", r"b\1", version)
version = re.sub(r"-rc\.(\d+)$", r"rc\1", version)
print(version)
PY
)"
bundle="$TMP/bundle"
mkdir -p \
  "$bundle/source-packages" \
  "$bundle/node-wrapper" \
  "$bundle/node-darwin-arm64" \
  "$bundle/node-darwin-x64" \
  "$bundle/node-linux-arm64-gnu" \
  "$bundle/node-linux-x64-gnu" \
  "$bundle/node-win32-x64-msvc" \
  "$bundle/python-linux-x64" \
  "$bundle/python-linux-arm64" \
  "$bundle/python-macos-arm64" \
  "$bundle/python-macos-x64" \
  "$bundle/python-windows-x64"

files=(
  "source-packages/aikit-runtime-core-$version.crate"
  "source-packages/aikit-runtime-files.txt"
  "node-wrapper/aikit-runtime-$version.tgz"
  "node-darwin-arm64/aikit-runtime-darwin-arm64-$version.tgz"
  "node-darwin-x64/aikit-runtime-darwin-x64-$version.tgz"
  "node-linux-arm64-gnu/aikit-runtime-linux-arm64-gnu-$version.tgz"
  "node-linux-x64-gnu/aikit-runtime-linux-x64-gnu-$version.tgz"
  "node-win32-x64-msvc/aikit-runtime-win32-x64-msvc-$version.tgz"
  "python-linux-x64/aikit_runtime-$wheel_version-cp39-abi3-manylinux_2_28_x86_64.whl"
  "python-linux-arm64/aikit_runtime-$wheel_version-cp39-abi3-manylinux_2_28_aarch64.whl"
  "python-macos-arm64/aikit_runtime-$wheel_version-cp39-abi3-macosx_11_0_arm64.whl"
  "python-macos-x64/aikit_runtime-$wheel_version-cp39-abi3-macosx_10_12_x86_64.whl"
  "python-windows-x64/aikit_runtime-$wheel_version-cp39-abi3-win_amd64.whl"
)
for file in "${files[@]}"; do printf 'fixture:%s\n' "$file" > "$bundle/$file"; done

python3 - "$bundle" <<'PY'
import hashlib
import pathlib
import sys
root = pathlib.Path(sys.argv[1])
lines = []
for path in sorted(path for path in root.rglob("*") if path.is_file()):
    relative = path.relative_to(root).as_posix()
    lines.append(f"{hashlib.sha256(path.read_bytes()).hexdigest()}  ./{relative}")
(root / "SHA256SUMS").write_text("\n".join(lines) + "\n", encoding="utf-8")
PY

./scripts/verify-checksum-manifest.py "$bundle" >/dev/null
./scripts/prepare-publish-bundle.sh pypi "$bundle" "$TMP/pypi" >/dev/null
./scripts/prepare-publish-bundle.sh npm "$bundle" "$TMP/npm" >/dev/null
test "$(find "$TMP/pypi" -type f | wc -l | tr -d ' ')" = 5
test "$(find "$TMP/npm" -type f | wc -l | tr -d ' ')" = 6

for defect in omitted duplicate malformed traversal noncanonical extra; do
  defective="$TMP/checksum-$defect"
  cp -R "$bundle" "$defective"
  python3 - "$defective" "$defect" <<'PY'
import pathlib
import sys
root = pathlib.Path(sys.argv[1])
defect = sys.argv[2]
manifest = root / "SHA256SUMS"
lines = manifest.read_text(encoding="utf-8").splitlines()
if defect == "omitted":
    lines.pop()
elif defect == "duplicate":
    lines.append(lines[0])
elif defect == "malformed":
    lines[0] = "not-a-checksum  ./artifact"
elif defect == "traversal":
    checksum = lines[0].split()[0]
    lines[0] = f"{checksum}  ./../escape"
elif defect == "noncanonical":
    checksum, path = lines[0].split("  ./", 1)
    lines[0] = f"{checksum}  ./{path.replace('/', '//', 1)}"
elif defect == "extra":
    (root / "unlisted.bin").write_bytes(b"unlisted")
manifest.write_text("\n".join(lines) + "\n", encoding="utf-8")
PY
  expect_failure "checksum defect unexpectedly passed: $defect" \
    ./scripts/verify-checksum-manifest.py "$defective"
done

printf 'tampered\n' >> "$bundle/node-wrapper/aikit-runtime-$version.tgz"
expect_failure "tampered publish bundle unexpectedly passed" \
  ./scripts/prepare-publish-bundle.sh npm "$bundle" "$TMP/tampered"

# crates.io exact-checksum skip, mismatch refusal, post-upload recovery, and Cargo resolution.
crate_artifact="$TMP/aikit-runtime-core-9.8.7-test.1.crate"
printf 'deterministic crate fixture\n' > "$crate_artifact"
crate_registry="$TMP/crates-registry"
crate_path="$crate_registry/aikit-runtime-core/9.8.7-test.1"
mkdir -p "$(dirname "$crate_path")"
python3 - "$crate_artifact" "$crate_path" <<'PY'
import hashlib
import json
import pathlib
import sys
artifact, output = map(pathlib.Path, sys.argv[1:])
payload = {"version": {"crate": "aikit-runtime-core", "num": "9.8.7-test.1", "checksum": hashlib.sha256(artifact.read_bytes()).hexdigest()}}
output.write_text(json.dumps(payload), encoding="utf-8")
PY
exact_crate_json="$TMP/exact-crate.json"
cp "$crate_path" "$exact_crate_json"

mock_bin="$TMP/mock-bin"
mkdir -p "$mock_bin"
cat > "$mock_bin/cargo-never" <<'SH'
#!/usr/bin/env bash
echo "cargo must not run for an exact registry artifact" >&2
exit 99
SH
chmod +x "$mock_bin/cargo-never"
AIKIT_CRATES_API_BASE="file://$crate_registry" AIKIT_CARGO_BIN="$mock_bin/cargo-never" \
  ./scripts/publish-crate-idempotent.sh \
  aikit-runtime-core 9.8.7-test.1 "$crate_artifact" -- -p aikit-runtime-core --locked >/dev/null

python3 - "$crate_path" <<'PY'
import json
import pathlib
import sys
path = pathlib.Path(sys.argv[1])
payload = json.loads(path.read_text(encoding="utf-8"))
payload["version"]["checksum"] = "0" * 64
path.write_text(json.dumps(payload), encoding="utf-8")
PY
expect_failure "conflicting crates.io checksum unexpectedly passed" \
  env AIKIT_CRATES_API_BASE="file://$crate_registry" AIKIT_CARGO_BIN="$mock_bin/cargo-never" \
  ./scripts/publish-crate-idempotent.sh \
  aikit-runtime-core 9.8.7-test.1 "$crate_artifact" -- -p aikit-runtime-core --locked

rm "$crate_path"
cat > "$mock_bin/cargo-timeout" <<'SH'
#!/usr/bin/env bash
mkdir -p "$(dirname "$MOCK_CRATE_DEST")"
cp "$MOCK_CRATE_JSON" "$MOCK_CRATE_DEST"
exit 42
SH
chmod +x "$mock_bin/cargo-timeout"
AIKIT_CRATES_API_BASE="file://$crate_registry" \
AIKIT_CARGO_BIN="$mock_bin/cargo-timeout" \
AIKIT_REGISTRY_ATTEMPTS=1 AIKIT_REGISTRY_DELAY_SECONDS=0 \
MOCK_CRATE_JSON="$exact_crate_json" MOCK_CRATE_DEST="$crate_path" \
  ./scripts/publish-crate-idempotent.sh \
  aikit-runtime-core 9.8.7-test.1 "$crate_artifact" -- -p aikit-runtime-core --locked >/dev/null

cat > "$mock_bin/cargo-fetch" <<'SH'
#!/usr/bin/env bash
test "$1" = fetch
printf '%s\n' "$*" > "$MOCK_CARGO_LOG"
SH
chmod +x "$mock_bin/cargo-fetch"
AIKIT_CRATES_API_BASE="file://$crate_registry" AIKIT_CARGO_BIN="$mock_bin/cargo-fetch" \
AIKIT_REGISTRY_ATTEMPTS=1 AIKIT_REGISTRY_DELAY_SECONDS=0 MOCK_CARGO_LOG="$TMP/cargo-fetch.log" \
  ./scripts/wait-crate-version.sh \
  aikit-runtime-core 9.8.7-test.1 "$crate_artifact" 1 0 >/dev/null
grep -Fq 'fetch --manifest-path' "$TMP/cargo-fetch.log"

# OIDC publishing is refused until crates.io has both manually bootstrapped crate names.
bootstrap_registry="$TMP/crates-bootstrap"
mkdir -p "$bootstrap_registry/aikit-runtime-core"
printf '%s\n' '{"crate":{"id":"aikit-runtime-core"}}' \
  > "$bootstrap_registry/aikit-runtime-core/index.json"
expect_failure "missing facade crate owner bootstrap unexpectedly passed" \
  env AIKIT_CRATES_API_BASE="file://$bootstrap_registry" \
  ./scripts/require-crates-oidc-bootstrap.sh aikit-runtime-core aikit-runtime
grep -Fq 'OIDC trusted publishing cannot create' "$TMP/expected-failure.out"
grep -Fq 'docs/RELEASE.md' "$TMP/expected-failure.out"

mkdir -p "$bootstrap_registry/aikit-runtime"
printf '%s\n' '{"crate":{"id":"aikit-runtime"}}' \
  > "$bootstrap_registry/aikit-runtime/index.json"
AIKIT_CRATES_API_BASE="file://$bootstrap_registry" \
  ./scripts/require-crates-oidc-bootstrap.sh aikit-runtime-core aikit-runtime >/dev/null

printf '%s\n' '{"crate":{"id":"different-crate"}}' \
  > "$bootstrap_registry/aikit-runtime/index.json"
expect_failure "wrong crates.io bootstrap identity unexpectedly passed" \
  env AIKIT_CRATES_API_BASE="file://$bootstrap_registry" \
  ./scripts/require-crates-oidc-bootstrap.sh aikit-runtime-core aikit-runtime

# npm trusted publishing also requires each package to exist before its settings are available.
npm_bootstrap_registry="$TMP/npm-bootstrap"
npm_bootstrap_packages=(
  aikit-runtime
  aikit-runtime-darwin-arm64
  aikit-runtime-darwin-x64
  aikit-runtime-linux-arm64-gnu
  aikit-runtime-linux-x64-gnu
  aikit-runtime-win32-x64-msvc
)
for package_name in "${npm_bootstrap_packages[@]:0:5}"; do
  mkdir -p "$npm_bootstrap_registry/$package_name"
  printf '{"name":"%s"}\n' "$package_name" \
    > "$npm_bootstrap_registry/$package_name/index.json"
done
expect_failure "missing npm platform owner bootstrap unexpectedly passed" \
  env AIKIT_NPM_REGISTRY_BASE="file://$npm_bootstrap_registry" \
  ./scripts/require-npm-oidc-bootstrap.sh "${npm_bootstrap_packages[@]}"
grep -Fq 'Trusted publishing cannot be configured until the package exists' \
  "$TMP/expected-failure.out"
grep -Fq 'docs/RELEASE.md' "$TMP/expected-failure.out"

package_name="${npm_bootstrap_packages[5]}"
mkdir -p "$npm_bootstrap_registry/$package_name"
printf '{"name":"%s"}\n' "$package_name" \
  > "$npm_bootstrap_registry/$package_name/index.json"
AIKIT_NPM_REGISTRY_BASE="file://$npm_bootstrap_registry" \
  ./scripts/require-npm-oidc-bootstrap.sh "${npm_bootstrap_packages[@]}" >/dev/null

printf '%s\n' '{"name":"different-package"}' \
  > "$npm_bootstrap_registry/$package_name/index.json"
expect_failure "wrong npm bootstrap identity unexpectedly passed" \
  env AIKIT_NPM_REGISTRY_BASE="file://$npm_bootstrap_registry" \
  ./scripts/require-npm-oidc-bootstrap.sh "${npm_bootstrap_packages[@]}"

# npm exact-integrity retry with an explicit alpha tag.
npm_artifact="$TMP/aikit-runtime-test-0.3.0-alpha.1.tgz"
python3 - "$npm_artifact" <<'PY'
import io
import json
import pathlib
import tarfile
import sys
path = pathlib.Path(sys.argv[1])
payload = json.dumps({"name": "aikit-runtime-test", "version": "0.3.0-alpha.1"}).encode()
info = tarfile.TarInfo("package/package.json")
info.size = len(payload)
info.mtime = 0
with tarfile.open(path, "w:gz") as archive:
    archive.addfile(info, io.BytesIO(payload))
PY
npm_registry="$TMP/npm-registry"
npm_package_root="$npm_registry/aikit-runtime-test"
mkdir -p "$npm_package_root"
python3 - "$npm_artifact" "$npm_package_root" <<'PY'
import base64
import hashlib
import json
import pathlib
import sys
artifact = pathlib.Path(sys.argv[1])
root = pathlib.Path(sys.argv[2])
integrity = "sha512-" + base64.b64encode(hashlib.sha512(artifact.read_bytes()).digest()).decode()
(root / "0.3.0-alpha.1").write_text(json.dumps({"name": "aikit-runtime-test", "version": "0.3.0-alpha.1", "dist": {"integrity": integrity}}), encoding="utf-8")
(root / "index.json").write_text(json.dumps({"dist-tags": {"alpha": "0.3.0-alpha.1"}}), encoding="utf-8")
PY
exact_npm_version="$TMP/exact-npm-version.json"
exact_npm_root="$TMP/exact-npm-root.json"
cp "$npm_package_root/0.3.0-alpha.1" "$exact_npm_version"
cp "$npm_package_root/index.json" "$exact_npm_root"
cat > "$mock_bin/npm-never" <<'SH'
#!/usr/bin/env bash
echo "npm must not run for an exact registry artifact" >&2
exit 99
SH
chmod +x "$mock_bin/npm-never"

# Registry reads distinguish transient HTTP failures from hard integrity conflicts.
http_server="$TMP/registry-http.py"
cat > "$http_server" <<'PY'
import base64
import hashlib
import json
import pathlib
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

port_file, log_file, crate_artifact, npm_artifact = map(pathlib.Path, sys.argv[1:])
crate_checksum = hashlib.sha256(crate_artifact.read_bytes()).hexdigest()
npm_integrity = "sha512-" + base64.b64encode(
    hashlib.sha512(npm_artifact.read_bytes()).digest()
).decode()
counts = {}


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        counts[self.path] = counts.get(self.path, 0) + 1
        with log_file.open("a", encoding="utf-8") as handle:
            handle.write(self.path + "\n")
        if self.path == "/crates/transient-crate/9.8.7-test.1":
            if counts[self.path] == 1:
                return self.reply(503, {"error": "retry"})
            return self.reply(200, {"version": {"crate": "transient-crate", "num": "9.8.7-test.1", "checksum": crate_checksum}})
        if self.path == "/crates/conflict-crate/9.8.7-test.1":
            return self.reply(200, {"version": {"crate": "conflict-crate", "num": "9.8.7-test.1", "checksum": "0" * 64}})
        if self.path == "/npm/aikit-runtime-test/0.3.0-alpha.1":
            if counts[self.path] == 1:
                return self.reply(429, {"error": "retry"})
            return self.reply(200, {"name": "aikit-runtime-test", "version": "0.3.0-alpha.1", "dist": {"integrity": npm_integrity}})
        if self.path == "/npm/aikit-runtime-test":
            return self.reply(200, {"name": "aikit-runtime-test", "dist-tags": {"alpha": "0.3.0-alpha.1"}})
        return self.reply(404, {"error": "missing"})

    def reply(self, status, payload):
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, _format, *_args):
        pass


server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
port_file.write_text(str(server.server_port), encoding="utf-8")
server.serve_forever()
PY
http_port_file="$TMP/registry-http.port"
http_log="$TMP/registry-http.log"
python3 "$http_server" "$http_port_file" "$http_log" "$crate_artifact" "$npm_artifact" &
HTTP_PID=$!
for _ in $(seq 1 50); do
  [ -s "$http_port_file" ] && break
  sleep 0.1
done
test -s "$http_port_file"
http_port="$(cat "$http_port_file")"

AIKIT_CRATES_API_BASE="http://127.0.0.1:$http_port/crates" \
AIKIT_CARGO_BIN="$mock_bin/cargo-never" AIKIT_REGISTRY_ATTEMPTS=2 \
AIKIT_REGISTRY_DELAY_SECONDS=0 \
  ./scripts/publish-crate-idempotent.sh \
  transient-crate 9.8.7-test.1 "$crate_artifact" -- -p aikit-runtime-core --locked >/dev/null

AIKIT_NPM_REGISTRY_BASE="http://127.0.0.1:$http_port/npm" \
AIKIT_NPM_BIN="$mock_bin/npm-never" AIKIT_REGISTRY_ATTEMPTS=2 \
AIKIT_REGISTRY_DELAY_SECONDS=0 \
  ./scripts/publish-npm-idempotent.sh "$npm_artifact" alpha >/dev/null

rm -f "$TMP/conflict-cargo.log"
expect_failure "wait-crate-version unexpectedly retried a hard checksum conflict" \
  env AIKIT_CRATES_API_BASE="http://127.0.0.1:$http_port/crates" \
  AIKIT_CARGO_BIN="$mock_bin/cargo-fetch" MOCK_CARGO_LOG="$TMP/conflict-cargo.log" \
  ./scripts/wait-crate-version.sh conflict-crate 9.8.7-test.1 "$crate_artifact" 3 0
test ! -e "$TMP/conflict-cargo.log"
test "$(grep -Fc '/crates/conflict-crate/9.8.7-test.1' "$http_log")" = 1

AIKIT_NPM_REGISTRY_BASE="file://$npm_registry" AIKIT_NPM_BIN="$mock_bin/npm-never" \
AIKIT_REGISTRY_ATTEMPTS=1 AIKIT_REGISTRY_DELAY_SECONDS=0 \
  ./scripts/publish-npm-idempotent.sh "$npm_artifact" alpha >/dev/null

printf '%s\n' '{"dist-tags":{}}' > "$npm_package_root/index.json"
expect_failure "exact npm bytes with a missing tag unexpectedly passed" \
  env AIKIT_NPM_REGISTRY_BASE="file://$npm_registry" AIKIT_NPM_BIN="$mock_bin/npm-never" \
  AIKIT_REGISTRY_ATTEMPTS=1 AIKIT_REGISTRY_DELAY_SECONDS=0 \
  ./scripts/publish-npm-idempotent.sh "$npm_artifact" alpha
grep -Fq "npm dist-tag add 'aikit-runtime-test@0.3.0-alpha.1' 'alpha'" \
  "$TMP/expected-failure.out"
cp "$exact_npm_root" "$npm_package_root/index.json"

python3 - "$npm_package_root/0.3.0-alpha.1" <<'PY'
import json
import pathlib
import sys
path = pathlib.Path(sys.argv[1])
payload = json.loads(path.read_text(encoding="utf-8"))
payload["dist"]["integrity"] = "sha512-conflict"
path.write_text(json.dumps(payload), encoding="utf-8")
PY
expect_failure "conflicting npm integrity unexpectedly passed" \
  env AIKIT_NPM_REGISTRY_BASE="file://$npm_registry" AIKIT_NPM_BIN="$mock_bin/npm-never" \
  AIKIT_REGISTRY_ATTEMPTS=1 AIKIT_REGISTRY_DELAY_SECONDS=0 \
  ./scripts/publish-npm-idempotent.sh "$npm_artifact" alpha

rm -rf "$npm_package_root"
mkdir -p "$npm_package_root"
cat > "$mock_bin/npm-timeout" <<'SH'
#!/usr/bin/env bash
printf '%s\n' "$*" > "$MOCK_NPM_LOG"
cp "$MOCK_NPM_VERSION_JSON" "$MOCK_NPM_PACKAGE_ROOT/0.3.0-alpha.1"
cp "$MOCK_NPM_ROOT_JSON" "$MOCK_NPM_PACKAGE_ROOT/index.json"
exit 41
SH
chmod +x "$mock_bin/npm-timeout"
AIKIT_NPM_REGISTRY_BASE="file://$npm_registry" AIKIT_NPM_BIN="$mock_bin/npm-timeout" \
AIKIT_REGISTRY_ATTEMPTS=1 AIKIT_REGISTRY_DELAY_SECONDS=0 \
MOCK_NPM_LOG="$TMP/npm.log" MOCK_NPM_VERSION_JSON="$exact_npm_version" \
MOCK_NPM_ROOT_JSON="$exact_npm_root" MOCK_NPM_PACKAGE_ROOT="$npm_package_root" \
  ./scripts/publish-npm-idempotent.sh "$npm_artifact" alpha >/dev/null
grep -Fq "publish $npm_artifact --tag alpha" "$TMP/npm.log"

python3 - "$npm_package_root/index.json" <<'PY'
import json
import pathlib
import sys
path = pathlib.Path(sys.argv[1])
payload = json.loads(path.read_text(encoding="utf-8"))
payload["dist-tags"]["alpha"] = "0.3.0-alpha.2"
path.write_text(json.dumps(payload), encoding="utf-8")
PY
expect_failure "npm helper unexpectedly moved a dist-tag backward" \
  env AIKIT_NPM_REGISTRY_BASE="file://$npm_registry" AIKIT_NPM_BIN="$mock_bin/npm-never" \
  AIKIT_REGISTRY_ATTEMPTS=1 AIKIT_REGISTRY_DELAY_SECONDS=0 \
  ./scripts/publish-npm-idempotent.sh "$npm_artifact" alpha
grep -Fq 'refusing to move npm dist-tag alpha backward' "$TMP/expected-failure.out"
grep -Fq 'Do not run npm dist-tag add for this older version' "$TMP/expected-failure.out"
if grep -Fq "npm dist-tag add 'aikit-runtime-test@0.3.0-alpha.1' 'alpha'" \
  "$TMP/expected-failure.out"; then
  echo "ahead npm tag conflict suggested a backward repair" >&2
  exit 1
fi

stable_npm_artifact="$TMP/aikit-runtime-test-1.1.0.tgz"
python3 - "$stable_npm_artifact" <<'PY'
import io
import json
import pathlib
import tarfile
import sys
path = pathlib.Path(sys.argv[1])
payload = json.dumps({"name": "aikit-runtime-test", "version": "1.1.0"}).encode()
info = tarfile.TarInfo("package/package.json")
info.size = len(payload)
info.mtime = 0
with tarfile.open(path, "w:gz") as archive:
    archive.addfile(info, io.BytesIO(payload))
PY
rm -f "$npm_package_root/1.1.0"
python3 - "$npm_package_root/index.json" <<'PY'
import json
import pathlib
import sys
path = pathlib.Path(sys.argv[1])
path.write_text(json.dumps({"dist-tags": {"latest": "1.2.0"}}), encoding="utf-8")
PY
rm -f "$TMP/npm-must-not-publish.log"
cat > "$mock_bin/npm-log-and-fail" <<'SH'
#!/usr/bin/env bash
printf '%s\n' "$*" > "$MOCK_NPM_LOG"
exit 99
SH
chmod +x "$mock_bin/npm-log-and-fail"
expect_failure "missing older npm version unexpectedly moved latest backward" \
  env AIKIT_NPM_REGISTRY_BASE="file://$npm_registry" \
  AIKIT_NPM_BIN="$mock_bin/npm-log-and-fail" AIKIT_REGISTRY_ATTEMPTS=1 \
  AIKIT_REGISTRY_DELAY_SECONDS=0 MOCK_NPM_LOG="$TMP/npm-must-not-publish.log" \
  ./scripts/publish-npm-idempotent.sh "$stable_npm_artifact" latest
test ! -e "$TMP/npm-must-not-publish.log"

# PyPI plans only missing filenames, while exact files skip and checksum conflicts fail.
pypi_input="$TMP/pypi-input"
mkdir -p "$pypi_input"
python3 - "$pypi_input" <<'PY'
import pathlib
import sys
import zipfile
root = pathlib.Path(sys.argv[1])
for platform in ("manylinux_2_28_x86_64", "manylinux_2_28_aarch64"):
    path = root / f"aikit_runtime-0.3.0a1-cp39-abi3-{platform}.whl"
    with zipfile.ZipFile(path, "w") as wheel:
        wheel.writestr("aikit_runtime-0.3.0a1.dist-info/METADATA", "Metadata-Version: 2.1\nName: aikit-runtime\nVersion: 0.3.0a1\n")
PY
pypi_registry="$TMP/pypi-registry"
pypi_json="$pypi_registry/aikit-runtime/0.3.0a1/json"
mkdir -p "$(dirname "$pypi_json")"
python3 - "$pypi_input" "$pypi_json" <<'PY'
import hashlib
import json
import pathlib
import sys
root = pathlib.Path(sys.argv[1])
wheel = sorted(root.glob("*.whl"))[0]
payload = {"info": {"name": "aikit-runtime", "version": "0.3.0a1"}, "urls": [{"filename": wheel.name, "digests": {"sha256": hashlib.sha256(wheel.read_bytes()).hexdigest()}}]}
pathlib.Path(sys.argv[2]).write_text(json.dumps(payload), encoding="utf-8")
PY
AIKIT_PYPI_API_BASE="file://$pypi_registry" \
  ./scripts/prepare-pypi-publish.py "$pypi_input" "$TMP/pypi-plan" >/dev/null
test "$(find "$TMP/pypi-plan" -type f | wc -l | tr -d ' ')" = 1
python3 - "$pypi_json" <<'PY'
import json
import pathlib
import sys
path = pathlib.Path(sys.argv[1])
payload = json.loads(path.read_text(encoding="utf-8"))
payload["urls"][0]["digests"]["sha256"] = "0" * 64
path.write_text(json.dumps(payload), encoding="utf-8")
PY
expect_failure "conflicting PyPI filename checksum unexpectedly passed" \
  env AIKIT_PYPI_API_BASE="file://$pypi_registry" \
  ./scripts/prepare-pypi-publish.py "$pypi_input" "$TMP/pypi-conflict"

# Artifact runs are accepted only from the exact successful release workflow/source.
cat > "$mock_bin/gh" <<'SH'
#!/usr/bin/env bash
test "$1" = api
python3 - "$MOCK_GH_PAYLOAD" <<'PY'
import pathlib
import sys
sys.stdout.write(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))
PY
SH
chmod +x "$mock_bin/gh"
valid_run="$TMP/valid-run.json"
cat > "$valid_run" <<'JSON'
{"path":".github/workflows/release.yml","conclusion":"success","head_sha":"0123456789abcdef0123456789abcdef01234567","event":"workflow_dispatch","head_branch":"main","head_repository":{"full_name":"owner/repository"}}
JSON
PATH="$mock_bin:$PATH" MOCK_GH_PAYLOAD="$valid_run" \
  ./scripts/validate-release-run.sh 123 0123456789abcdef0123456789abcdef01234567 \
  refs/heads/main owner/repository >/dev/null
python3 - "$valid_run" <<'PY'
import json
import pathlib
import sys
path = pathlib.Path(sys.argv[1])
payload = json.loads(path.read_text(encoding="utf-8"))
payload["event"] = "push"
path.write_text(json.dumps(payload), encoding="utf-8")
PY
expect_failure "non-dispatch release artifact run unexpectedly passed" \
  env PATH="$mock_bin:$PATH" MOCK_GH_PAYLOAD="$valid_run" \
  ./scripts/validate-release-run.sh 123 0123456789abcdef0123456789abcdef01234567 \
  refs/heads/main owner/repository

echo "PASS  release helper regressions"
