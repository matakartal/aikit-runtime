# Migrating to 0.3 alpha

This guide covers the source upgrade from `0.2.x` to `0.3.0-alpha.1`. It does not imply that public
registry packages exist.

## Streaming

Existing `StreamDelta` consumers continue to work during the v0.x compatibility window. New code
should consume identified events:

- Python: `stream.events(response_id)`
- Node: `stream.events(responseId)`
- Rust: `StreamEventEncoder`

Do not consume both views for the same run: they share one underlying stream. The v2 view has
monotonic sequence numbers and explicit response/block lifecycle events.

## MCP naming

Python and Node client connections are now `McpConnection`. `McpServer` remains available only as
`legacy.McpServer` through the v0.6 compatibility window. Connections are factory-created through
the HTTP/stdio helpers; their public declarations no longer advertise a constructor. This rename
prevents a client connection from being confused with the new governed MCP server/task contracts.

## Model capabilities and routing

Required capabilities are no longer inferred from absence. Use `CapabilityState` explicitly:

- `supported`: routing may select the model;
- `unsupported`: the provider/model explicitly cannot perform it;
- `unknown`: there is not enough evidence, so strict routing fails closed.

Use `ModelCatalogSnapshot::shipped()` plus `ModelCatalogOverrides::resolve()` in Rust. Do not edit
the compiled snapshot or assume that an unknown price/capability is free/supported.

## Provider compatibility modes

New runs default to `strict`. Provider options are checked against the selected adapter's shipped
parameter catalog before network I/O. A protected or unknown field, a cataloged value-type mismatch,
or governed nested-field drift is a typed invalid-request error; it is never silently removed.

Use `warn` only when a provider has introduced a new wire parameter that AIKit has not cataloged
yet. AIKit forwards the value unchanged and emits a machine-readable `warning` stream item. If the
request fails before returning a stream, the same evidence remains in typed `info.warnings`.
`best_effort` has the same no-silent-drop rule and is reserved for documented semantic fallbacks;
it does not make an unknown model capability supported.

Cataloged object options are checked recursively on their governed fields. Complex vendor beta
objects without a complete nested catalog are not strict-known: strict mode rejects them, while
warn/best-effort can forward them only with explicit warning evidence. JSON Schema payloads inside
cataloged schema containers remain opaque and are validated by the structured-output boundary.

## Integrity-bound media

`MediaInput` requires a MIME type, byte size, and lowercase SHA-256. Inline bytes and base64 are
recomputed before provider dispatch. A strict URL or artifact reference is intentionally rejected
until the host resolves it through governed egress/artifact storage and supplies the verified
bytes. This prevents a mutable URL from changing after its declared hash was recorded.

The legacy source-only media block remains available during v0.x. For Google, its URL form accepts
only Google-managed `gs://` or Gemini Files API URIs; fetch ordinary web URLs yourself and send
inline bytes.

## Durable runs

Python and Node expose `DurableRun`; Rust uses `RunState`. Importing a snapshot replays and validates
its event log, so a caller-modified projection is rejected. Every external activity must declare a
side-effect class and idempotency key where required. AIKit does not promise exactly-once delivery.

If an external request may have completed before its checkpoint committed, reconcile it explicitly;
do not retry it blindly. Rewind does not reverse external systems, and fork creates a separate run
identity.

A durable governed run pins one complete binding: policy snapshot hash, tenant, agent, and run id.
Restart, approval resolution, and each authorization revalidate that binding. Changing any member
requires a new run rather than replacing policy or identity beneath an existing approval.

## Governed MCP server

The new MCP server is distinct from an outbound `McpConnection`. It persists Tasks, request
receipts, session state, and SSE replay state with compare-and-swap storage. Cancellation remains
pending/reconciliation-required until the host confirms the underlying operation stopped; a
transport timeout is not reported as a completed cancellation. Schema drift requires a fresh
approval and the original arguments are validated again against the approved schema.

Completed side-effect receipts now have bounded retention. Once replay proof for a connection
expires, that connection's request-id/dedupe namespace is permanently retired and further calls
return reconciliation-required. Reconnect with a new session identity after resolving any
ambiguous external effect; do not reuse old request identifiers.

## Skills and executable hooks

Skills default to prompt/data packages. Pin the manifest, source revision, source digest, and every
artifact digest. Executable hooks additionally require explicit manifest permissions, an allow
policy decision, and fail-closed OS containment. Loading a skill never executes it.

## Release checks

All Cargo, Python and npm manifests use the same SemVer source string. Python wheel filenames use
the PEP 440-normalized form (`0.3.0a1`); the release checker handles this deliberately. A live smoke
run remains opt-in and billable, and absence of provider keys is a failure rather than a skip.
