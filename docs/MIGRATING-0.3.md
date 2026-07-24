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

Durability schema version 3 adds the unambiguous `ActivityAttemptCancelled` event and retains
version 2's terminal-activity invariant. Version 1 and 2 snapshots and SQLite/PostgreSQL rows remain
readable: historical events replay under their original rules, then the in-memory state and next
persisted write use version 3. Do not rewrite old event versions in place; they identify the
validation rules under which those append-only events were originally accepted. Version 2 and
older readers cannot safely consume version 3 events: deploy new readers before enabling version 3
writes, and do not roll back to an older reader after any version 3 write. Cooperative cancellation
now writes `ActivityAttemptCancelled` only when no activity is running and the stop is unambiguous;
an ambiguous stop stays available for reconciliation instead.

If an external request may have completed before its checkpoint committed, reconcile it explicitly;
do not retry it blindly. Rewind does not reverse external systems, and fork creates a separate run
identity.

A durable governed run pins one complete binding: policy snapshot hash, tenant, agent, and run id.
Restart, approval resolution, and each authorization revalidate that binding. Changing any member
requires a new run rather than replacing policy or identity beneath an existing approval.

Rust callers can attach `RunState` plus a `DurableStore` with `RunConfig::with_durable_run`, or an
existing `DurableRunDriver` with `with_durable_driver`. Only `DurabilityMode::Sync` is accepted.
Provider and tool calls are treated conservatively as reconciliation-required external effects;
do not infer exactly-once delivery from a completed local test. The driver provides CAS-backed
in-process coordination and one executor invocation per validated in-process attempt; it is not a
distributed or exactly-once execution system. Ambiguous provider, tool, or audit effects must be
reconciled before retry.

Terminal audit delivery is now a persisted two-phase operation: the driver saves a `RunStopped`
delivery intent before calling the fail-closed audit sink, then saves acceptance after every sink
accepts. If the final terminal-state CAS fails after acceptance, restart reuses that acceptance and
retries only the terminal CAS; it does not rerun provider, tool, or audit effects. If delivery is
ambiguous (including a crash after intent or a sink failure), resolve it through reconciliation
rather than blindly retrying it.

An operator-approved terminal-audit retry now requires a persisted typed replay envelope. The
envelope binds the original run, audit invocation, sequence, terminal summary, fail-closed sink
configuration, payload policy, and host delivery identity. After explicit resume, `SafeToRetry`
replays only that exact `RunStopped`; it never reruns hooks, providers, tools, or a new audit
invocation. A partial replay returns to reconciliation. Legacy version 1 terminal records with no
attempt are not trusted as complete: they require a synthetic reconciliation attempt plus a typed
attestation that matches the persisted terminal state.

Activity schedule inputs now persist only a deterministic hash. Completed provider/tool results
remain verbatim replay data; configure their per-result bound with `DurablePayloadPolicy` and apply
transcript-grade access control, retention, and at-rest protection to the store. A result that
exceeds the configured limit is not persisted and leaves the effect reconciliation-required.

A durable driver accepts either no human approver or the exact adapter created with
`with_persisted_durable_driver_approver`; ordinary or separately constructed approvers fail closed
because they do not share the driver's state, store and CAS-poison authority. `AuditRecord` also
adds optional `invocation_id`: `(run_id, invocation_id, sequence)` distinguishes repeated attaches
to one logical run. Legacy serialized records without the field still deserialize, but Rust struct
literals or exhaustive destructuring must add/ignore the new field.

## Governed A2A mapper

Rust, Python and Node now expose the same canonical `A2aMapper`, including subject+tenant-scoped
task listing and bounded cursor pagination. Direct Rust callers must pass an `A2aListTasksRequest`;
Python uses `list_tasks`, and Node uses `listTasks`.

Separately from durability schema v3, governance envelopes use protocol contract version `2`. This
protocol version preserves
`invalid_request` as a first-class denial code instead of folding malformed identity/message input
into a generic conflict/forbidden result. Consumers that deserialize the denial enum exhaustively
must add this value before accepting version 2 envelopes.

Context storage keys are now owner-scoped so two tenants may safely use the same wire `context_id`.
Message receipts are likewise keyed by subject, tenant and `message_id`, so one tenant cannot
reserve another tenant's idempotency key. Pre-scoping alpha snapshots remain readable for their
original owner, but callers must stop treating `A2aMapper::contexts()` or `receipts()` keys as wire
ids; use `context_session` or `message_receipt` with an authenticated principal.

Mapper snapshots now include `schema_version` (currently `4`). Treat the exported runtime constant
as authoritative instead of hard-coding an older numeric value. Restore validates task/map keys, generated sequence,
owner/context relations, globally unique runtime ids, receipt references and revision bounds before
accepting state. List cursors are bound to one mapper revision and stale cursors are rejected rather
than producing duplicate/skipped tasks. Node's
`transitionTask` now returns the updated mapper snapshot, matching Python's `transition_task`.

The experimental Rust A2A network server now caps the complete canonical serialized mapper
snapshot at 32 MiB. Persisted raw bytes are rejected before decode when they exceed that ceiling.
Snapshot stores use compare-and-swap against the exact prior `(revision, SHA-256 digest)` and treat
only an identical revision+digest as already applied. Mapper serialization and store I/O run
outside the live mapper lock; an accepted commit finishes and installs its exact candidate even if
the originating request is dropped. Store errors must distinguish definitely-not-applied from an
unknown outcome. Unknown outcomes are resolved with a linearizable, post-CAS durable-head probe;
stale replica/cache reads are not valid implementations. An unresolved outcome fail-stops later
mapper writes and host effects. Use `A2aHttpJsonRpcServer::new_owned`; the
internal shared-Arc constructor is retained only for crate tests and requires exclusive ownership
of that Arc.

This full-snapshot server remains bounded and experimental, not production-scale persistence.
The new typed delta-journal API defines exact commit-token CAS, bounded/paged replay, validated
checkpoints, restore pins and integrity-bound garbage collection. It is additive and is not yet
wired into the HTTP transport mutation path, so operators must not treat it as production journal
persistence. The mapper itself remains transport-neutral and is not an official A2A wire DTO.

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

The Python and Node `A2aMapper` APIs remain transport-neutral mapping/persistence surfaces; they do
not start an A2A listener. The experimental Rust listener now projects artifacts and direct
Messages, supports SSE, and separates protected cancellation ingress. Its typed delta-journal API
is tested but not yet the transport's persistence hot path. See [A2A-CONFORMANCE.md](A2A-CONFORMANCE.md)
for the pinned official TCK result and exact verified-waiver boundary.

## Skills and executable hooks

Skills default to prompt/data packages. Pin the manifest, source revision, source digest, and every
artifact digest. Executable hooks additionally require explicit manifest permissions, an allow
policy decision, and fail-closed OS containment. Loading a skill never executes it.

## Release checks

All Cargo, Python and npm manifests use the same SemVer source string. Python wheel filenames use
the PEP 440-normalized form (`0.3.0a1`); the release checker handles this deliberately. A live smoke
run remains opt-in and billable, and absence of provider keys is a failure rather than a skip.
