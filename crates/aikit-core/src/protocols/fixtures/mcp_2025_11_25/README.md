# MCP 2025-11-25 conformance fixtures

These request shapes track the official MCP lifecycle and experimental Tasks examples for the
`2025-11-25` protocol revision. They are intentionally local and credential-free so parser,
deduplication, restart and authorization behavior can be tested without a network dependency.
They are test inputs, not a vendored conformance certificate. The runtime also supports the
stateless `2026-07-28` dialect through version-scoped translation; do not silently rewrite these
historical fixtures to the newer shape.

Sources:

- <https://modelcontextprotocol.io/specification/2025-11-25/basic/lifecycle>
- <https://modelcontextprotocol.io/specification/2025-11-25/basic/utilities/tasks>
