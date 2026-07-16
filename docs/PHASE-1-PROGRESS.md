# Phase 1 retrospective — provider and reasoning spine

This is an archived milestone note. Phase 1 established the canonical schema, four provider
adapters, provider-specific reasoning replay, capability grades, and credential activation. Those
pieces are now integrated with governance, containment, audit, routing, sessions, memory, and the
three public language surfaces.

The important compatibility rules remain:

| Provider | Replay rule |
|---|---|
| Anthropic | Preserve signed thinking unchanged. |
| OpenAI Responses | Preserve only OpenAI-owned opaque reasoning items. |
| Google Gemini | For Gemini 3 function calling, replay the thought signature on the exact `functionCall` part that carried it. |
| DeepSeek | For a thinking assistant turn that called tools, replay its full `reasoning_content`; omitting it can produce a 400. |

Reasoning state is tagged by provider. Fallback must never send one provider's signature or opaque
payload to another provider. Structured output is similarly graded rather than flattened:
OpenAI, Google, and Anthropic use their current native JSON-schema constraints; DeepSeek uses JSON
mode plus local validation and bounded repair attempts. The generic `ForcedToolCall` grade remains
available for providers whose strongest mechanism is a forced schema-shaped tool.

Current native schema references are Anthropic
[`output_config.format`](https://platform.claude.com/docs/en/build-with-claude/structured-outputs)
and Gemini [`responseJsonSchema`](https://ai.google.dev/api/generate-content).

The conditional DeepSeek rule and Gemini function-call placement follow the current
[DeepSeek Thinking Mode](https://api-docs.deepseek.com/guides/thinking_mode) and
[Gemini thought-signature](https://ai.google.dev/gemini-api/docs/generate-content/thought-signatures)
contracts. They are covered by wire-format tests; changing live API acceptance still requires the
separate smoke test.

The HTTP adapters are tested end to end against local real-socket mock servers. That proves
request serialization, SSE framing, and canonical parsing, but not acceptance by a changing live
API. Live proof uses the separate opt-in contract in [`LIVE-SMOKE.md`](LIVE-SMOKE.md).

For current status and public usage, see the repository [`README`](../README.md) and
[`feature reference`](FEATURES.md).
