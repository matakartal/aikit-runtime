#!/usr/bin/env bash
set -euo pipefail

# Official A2A Technology Compatibility Kit. Keep this immutable so a recorded result always
# identifies the exact upstream tests that ran.
readonly AIKIT_A2A_TCK_COMMIT="5996b79f9cefa6fc390980e383e358a66fb9e49e"
readonly AIKIT_A2A_TCK_REPOSITORY="https://github.com/a2aproject/a2a-tck.git"

fail() {
  printf 'a2a-conformance: %s\n' "$1" >&2
  exit 2
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || fail "required command is unavailable: $1"
}

sut_url="${AIKIT_A2A_SUT_URL:-}"
test -n "$sut_url" || fail "set AIKIT_A2A_SUT_URL to the running AIKit A2A server"

case "$sut_url" in
  *://*@*) fail "SUT URL must not contain credentials" ;;
esac

case "$sut_url" in
  http://127.0.0.1:* | http://localhost:* | http://\[::1\]:*) ;;
  *)
    test "${AIKIT_A2A_TCK_ALLOW_REMOTE:-0}" = "1" \
      || fail "remote SUTs require AIKIT_A2A_TCK_ALLOW_REMOTE=1"
    ;;
esac

level="${AIKIT_A2A_TCK_LEVEL:-must}"
case "$level" in
  must | should | may | all) ;;
  *) fail "AIKIT_A2A_TCK_LEVEL must be one of: must, should, may, all" ;;
esac

verified_waivers="${AIKIT_A2A_TCK_VERIFIED_WAIVERS:-0}"
case "$verified_waivers" in
  0 | 1) ;;
  *) fail "AIKIT_A2A_TCK_VERIFIED_WAIVERS must be 0 or 1" ;;
esac

require_command git
require_command uv

tck_root="$(mktemp -d)"
cleanup() {
  rm -rf -- "$tck_root"
}
trap cleanup EXIT

tck_checkout="$tck_root/a2a-tck"
git init --quiet "$tck_checkout"
git -C "$tck_checkout" remote add origin "$AIKIT_A2A_TCK_REPOSITORY"
git -C "$tck_checkout" fetch --quiet --depth 1 origin "$AIKIT_A2A_TCK_COMMIT"
git -C "$tck_checkout" checkout --quiet --detach FETCH_HEAD

resolved_commit="$(git -C "$tck_checkout" rev-parse HEAD)"
test "$resolved_commit" = "$AIKIT_A2A_TCK_COMMIT" \
  || fail "resolved TCK commit does not match the pinned commit"

printf 'A2A_TCK_COMMIT=%s\n' "$resolved_commit"
printf 'A2A_SUT_URL=%s\n' "$sut_url"
printf 'A2A_TCK_TRANSPORT=jsonrpc\n'
printf 'A2A_TCK_LEVEL=%s\n' "$level"

tck_args=(--sut-host "$sut_url" --transport jsonrpc)
if test "$level" != "all"; then
  tck_args+=(--level "$level")
fi

set +e
(
  cd "$tck_checkout"
  uv run --frozen ./run_tck.py "${tck_args[@]}" -- "$@"
)
tck_status=$?
set -e

waiver_status=0
if test "$tck_status" -ne 0 && test "$verified_waivers" = "1"; then
  require_command curl
  require_command python3
  junit_report="$tck_checkout/reports/junitreport.xml"
  test -f "$junit_report" || fail "verified waiver gate requires the raw JUnit report"

  python3 - "$junit_report" <<'PY' || fail "raw failures differ from the pinned verified-waiver set"
import sys
import xml.etree.ElementTree as ET

expected = {
    ("tests.compatibility.core_operations.test_requirements", "test_must_requirement[CORE-SEND-003-jsonrpc]"),
    ("tests.compatibility.core_operations.test_requirements", "test_must_requirement[CORE-EXECUTION-MODE-001-jsonrpc]"),
    ("tests.compatibility.core_operations.test_requirements", "test_must_requirement[CORE-EXECUTION-MODE-002-jsonrpc]"),
    ("tests.compatibility.core_operations.test_requirements", "test_must_requirement[CORE-MULTI-001a-jsonrpc]"),
    ("tests.compatibility.core_operations.test_requirements", "test_must_requirement[CORE-MULTI-002a-jsonrpc]"),
    ("tests.compatibility.core_operations.test_requirements", "test_must_requirement[CORE-MULTI-003-jsonrpc]"),
}
root = ET.parse(sys.argv[1]).getroot()
failed = [
    (case.get("classname", ""), case.get("name", ""))
    for case in root.iter("testcase")
    if case.find("failure") is not None or case.find("error") is not None
]
actual = set(failed)
if len(failed) != len(expected) or actual != expected:
    print("unexpected failures:", sorted(actual - expected), file=sys.stderr)
    print("missing pinned failures:", sorted(expected - actual), file=sys.stderr)
    raise SystemExit(1)
PY

  probe_id="aikit-send-003-probe-$$"
  probe_response="$tck_root/send-003-response.json"
  curl --silent --show-error \
    -H 'content-type: application/json' \
    -H 'A2A-Version: 1.0' \
    --data-binary "{\"jsonrpc\":\"2.0\",\"id\":\"$probe_id\",\"method\":\"SendMessage\",\"params\":{\"message\":{\"role\":\"user\",\"parts\":[{\"raw\":\"dGNr\",\"mediaType\":\"application/x-unsupported-tck-type\"}],\"messageId\":\"$probe_id\"}}}" \
    "$sut_url" >"$probe_response"
  python3 - "$probe_response" <<'PY' || fail "CORE-SEND-003 direct error probe failed"
import json
import sys

with open(sys.argv[1], encoding="utf-8") as response_file:
    response = json.load(response_file)
error = response.get("error", {})
if error.get("code") != -32005:
    raise SystemExit("expected JSON-RPC error -32005")
details = error.get("data", [])
if not any(
    isinstance(detail, dict) and detail.get("reason") == "CONTENT_TYPE_NOT_SUPPORTED"
    for detail in details
):
    raise SystemExit("expected CONTENT_TYPE_NOT_SUPPORTED reason")
PY

  collision_requirements=(
    CORE-EXECUTION-MODE-001
    CORE-EXECUTION-MODE-002
    CORE-MULTI-001a
    CORE-MULTI-002a
    CORE-MULTI-003
  )
  for requirement in "${collision_requirements[@]}"; do
    (
      cd "$tck_checkout"
      .venv/bin/python -m pytest \
        "tests/compatibility/core_operations/test_requirements.py::test_must_requirement[$requirement-jsonrpc]" \
        --sut-host="$sut_url" \
        --transport=jsonrpc \
        -m must \
        --tb=short \
        -q
    ) || fail "$requirement did not pass in an isolated TCK process"
  done
  waiver_status=1
fi

if test -n "${AIKIT_A2A_TCK_REPORT_DIR:-}"; then
  mkdir -p "$AIKIT_A2A_TCK_REPORT_DIR"
  if test -d "$tck_checkout/reports"; then
    cp -R "$tck_checkout/reports/." "$AIKIT_A2A_TCK_REPORT_DIR/"
  fi
fi

if test "$tck_status" -ne 0; then
  if test "$waiver_status" = "1"; then
    printf 'A2A raw TCK retained its six pinned upstream failures; direct and isolated verification gates passed.\n'
    exit 0
  fi
  printf 'A2A conformance failed; upstream reports were preserved when available.\n' >&2
  exit "$tck_status"
fi

printf 'A2A conformance passed against pinned official TCK.\n'
