# Deterministic evaluations

`aikit eval` turns canonical agent outcomes into repeatable CI gates. It is inspired by dataset
and verdict patterns in other agent runtimes, but deliberately keeps LLM-as-judge calls outside
the trusted core: the same outcome always produces the same verdict.

## Run the keyless suite

```bash
cargo run -p aikit-cli -- eval evals/smoke.json
cargo run -p aikit-cli -- --format json eval evals/smoke.json
```

Datasets default to `mock-1`. A non-mock model is rejected before provider construction unless the
operator explicitly passes `--allow-live`; this prevents a checked-in dataset from silently making
network calls or spending money. Live mode is also bounded by default to 8 cases, 512 KiB of
prompt/system input, 32,768 requested output tokens, and 600 seconds total wall time. Deliberate
larger runs must raise `--max-live-cases`, `--max-live-input-bytes`,
`--max-live-output-tokens`, or `--max-live-wall-seconds` explicitly.

## Dataset format

```json
{
  "schema_version": 1,
  "name": "support-agent",
  "model": "mock-1",
  "max_tokens": 128,
  "cases": [
    {
      "name": "finishes",
      "system": "Be concise.",
      "prompt": "Complete the task.",
      "gates": [
        { "type": "output_contains", "value": "tamamladım" },
        { "type": "terminal_status", "status": "completed" },
        { "type": "max_total_tokens", "value": 256 },
        { "type": "no_tool_errors" }
      ]
    }
  ]
}
```

`schema_version` is required and currently must be `1`. Dataset and case objects reject unknown
fields. A case may override `model` and `max_tokens`. Names, prompts, gate counts, expected strings,
tool sequences, and the complete dataset file are bounded before execution. The CLI accepts only a
regular file, refuses symlinks/special files, and enforces its 4 MiB limit while reading.

## Gates

| Gate | Meaning |
|---|---|
| `output_exact` | Final text equals `value`. |
| `output_contains` / `output_not_contains` | Final text includes or omits a non-empty fragment. |
| `terminal_status` | Canonical terminal status equals `status`. |
| `called_tool` / `did_not_call_tool` | Canonical transcript contains or omits a tool name. |
| `tool_sequence` | Tool names occur in order; `exact: true` requires the complete trajectory. |
| `no_tool_errors` | No canonical tool result has `is_error: true`. |
| `max_turns` | Assistant-message count stays within the limit. |
| `max_input_tokens`, `max_output_tokens`, `max_total_tokens` | Reported token usage stays bounded. |
| `max_model_attempts` | Retry/fallback attempt count stays bounded. |

The reusable Rust function is `evaluate_outcome(&RunOutcome, &[EvalGate])`. It evaluates recorded
tool trajectories as well as text, so host applications can test governed tool runs even though
the source-first CLI intentionally registers no side-effecting tools. Message-derived gates use
`RunOutcome.invocation_start_message_index` and ignore older conversation history; legacy/manual
outcomes without a boundary can use status and usage gates but fail closed for text/tool/turn gates.

## Reports and exit codes

Text is intended for humans; JSON/JSONL contain `EvalReport`, schema/runtime versions, the exact
dataset SHA-256, model-attempt history, per-case verdicts, gate checks, usage, and typed redacted
runtime errors. Model output and provider metadata are not copied into reports.

The gate engine is deterministic: the same canonical `RunOutcome` and gates produce the same
verdict. A live provider response is not inherently reproducible, so its report is provenance for
the observed run rather than a promise that a later network call will return identical text.

- `0`: every case and gate passed;
- `2`: invalid dataset or live model without acknowledgement;
- `3`: provider/runtime infrastructure prevented a valid evaluation;
- `4`: the dataset ran but one or more cases failed.

Local I/O/serialization and normal runtime commands retain their existing exit codes.

## Design references

The implementation is native Rust code, but its public shape borrows proven ideas from
[Pydantic Evals datasets and evaluators](https://pydantic.dev/docs/ai/evals/evals/) and
[Mastra gates and verdicts](https://mastra.ai/blog/introducing-gates-and-verdicts). Aikit keeps
the trusted gate layer deterministic and keyless instead of silently adding an LLM judge.
