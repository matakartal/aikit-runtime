# A2A conformance

AIKit includes a credentials-free localhost fixture for the official A2A Technology
Compatibility Kit (TCK). The fixture advertises only the implemented A2A 1.0 `JSONRPC`
interface and SSE streaming. It does not advertise REST, gRPC, push notifications, an extended
Agent Card, or authentication.

The CI workflow runs the official MUST-level JSON-RPC suite through
`scripts/a2a-conformance.sh`. That script fetches immutable TCK commit
`5996b79f9cefa6fc390980e383e358a66fb9e49e`, verifies the resolved commit, and uses the
upstream frozen dependency lock. The workflow uploads the compatibility, HTML, and JUnit reports
even when the suite fails. No provider credentials or billable model calls are used.

## Run locally

Prerequisites: Rust, `git`, `curl`, and `uv`.

In one terminal:

```bash
cargo run -p aikit-runtime-core --example a2a_tck_sut --locked
```

After `http://127.0.0.1:9999/.well-known/agent-card.json` responds, run this in another terminal:

```bash
AIKIT_A2A_SUT_URL=http://127.0.0.1:9999 \
AIKIT_A2A_TCK_LEVEL=must \
AIKIT_A2A_TCK_REPORT_DIR=target/a2a-tck-report \
./scripts/a2a-conformance.sh
```

CI additionally sets `AIKIT_A2A_TCK_VERIFIED_WAIVERS=1`. That mode does not discard or rewrite
the upstream result. It accepts only the exact six pinned false negatives, directly verifies the
`CORE-SEND-003` `-32005`/`CONTENT_TYPE_NOT_SUPPORTED` response, and reruns the five message-id
collision cases in separate TCK processes. Any missing or additional failure still fails the job.

The fixture is deliberately ephemeral and deterministic. Production hosts must provide durable
state, real authentication, authorization policy, and their own task executor.

## Current proof boundary

This fixture is not a full A2A implementation and the verified-waiver result is not an
unqualified official conformance claim. The 2026-07-24 raw JSON-RPC MUST run completed with 63
pytest cases passing, 6 failing, 166 skipped, and 30 deselected. The raw JUnit/HTML/compatibility
reports remain the authoritative upstream result.

The six failures have two verified false-negative patterns that must not be hidden by weakening
the runtime. Five requirements reuse one `messageId` for different message contents in the same
session, while AIKit correctly treats that as an idempotency conflict; every case passes in a
fresh TCK process. `CORE-SEND-003` sends an unsupported media type but does not declare its expected
error to the generic runner, so the correct `ContentTypeNotSupportedError` is recorded as a
failure. Keep the upstream commit and raw reports visible when evaluating future runs.
