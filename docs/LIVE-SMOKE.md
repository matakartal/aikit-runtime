# Live-provider smoke contract

The normal suite is deterministic, keyless, and non-billable. It validates provider request and
stream handling with local real-socket mock servers; it does not prove that a changing live API
accepted the request.

**Status:** no live-provider run is claimed for the current draft `v0.2.0` candidate. The
[`v0.1.0` evidence](releases/v0.1.0.md) is a historical snapshot, not proof for this candidate.

Every live mode requires `AIKIT_LIVE_SMOKE=1` as an explicit acknowledgement of network and
billable calls. The wrapper exits before the test when that flag is absent.

## Provider configuration

| Provider | Credential | Model variable |
|---|---|---|
| Anthropic | `ANTHROPIC_API_KEY` | `AIKIT_SMOKE_ANTHROPIC_MODEL` |
| OpenAI | `OPENAI_API_KEY` | `AIKIT_SMOKE_OPENAI_MODEL` |
| DeepSeek | `DEEPSEEK_API_KEY` | `AIKIT_SMOKE_DEEPSEEK_MODEL` |
| Google | `GEMINI_API_KEY` or `GOOGLE_API_KEY` | `AIKIT_SMOKE_GOOGLE_MODEL` |

## Configured-provider text probe

The lighter mode runs one non-empty text response against each provider whose key **and** model
are configured. A key without its model is an error, and zero complete providers is an error.

```bash
export ANTHROPIC_API_KEY='...'
export AIKIT_SMOKE_ANTHROPIC_MODEL='your-current-model-id'
AIKIT_LIVE_SMOKE=1 ./scripts/live-smoke.sh
```

## Full four-provider contract

Release-level evidence uses both flags:

```bash
AIKIT_LIVE_SMOKE=1 AIKIT_LIVE_SMOKE_FULL=1 ./scripts/live-smoke.sh
```

Full mode resolves **all four** key/model pairs before the first billable request and fails closed
if any is missing. For each provider it verifies:

1. A non-empty text generation.
2. A schema-validated object with the provider's reported fidelity path.
3. A forced tool call denied by governance; the host executor must remain untouched.
4. A forced tool call allowed exactly once, followed by a second provider request that accepts the
   exact assistant reasoning/tool-call state and tool result.

The denied and allowed cases both require a clean two-request terminal outcome. This exercises the
provider-specific replay contracts that a single text request cannot prove. It can make several
billable calls per provider; use low limits and dedicated test credentials with appropriate
spending controls.

## Evidence handling

The harness never prints credential values. A maintainer should separately record the date,
commit SHA, provider/model ids, and pass/fail result when using a successful full run as release
evidence. Do not commit keys, raw sensitive prompts, or private response payloads. A keyless CI
skip is not a live pass.
