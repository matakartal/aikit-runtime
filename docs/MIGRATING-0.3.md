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

## Durable runs

Python and Node expose `DurableRun`; Rust uses `RunState`. Importing a snapshot replays and validates
its event log, so a caller-modified projection is rejected. Every external activity must declare a
side-effect class and idempotency key where required. AIKit does not promise exactly-once delivery.

If an external request may have completed before its checkpoint committed, reconcile it explicitly;
do not retry it blindly. Rewind does not reverse external systems, and fork creates a separate run
identity.

## Skills and executable hooks

Skills default to prompt/data packages. Pin the manifest, source revision, source digest, and every
artifact digest. Executable hooks additionally require explicit manifest permissions, an allow
policy decision, and fail-closed OS containment. Loading a skill never executes it.

## Release checks

All Cargo, Python and npm manifests use the same SemVer source string. Python wheel filenames use
the PEP 440-normalized form (`0.3.0a1`); the release checker handles this deliberately. A live smoke
run remains opt-in and billable, and absence of provider keys is a failure rather than a skip.
