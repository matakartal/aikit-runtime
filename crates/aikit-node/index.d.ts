/**
 * TypeScript declarations for `aikit-runtime`, the Node (napi) binding of the aikit Rust core.
 *
 * Every behavior (provider dispatch, governance, budgets, structured output, durability) runs in
 * the shared Rust core; this package is a thin typed shell. The default `mock-1` /
 * `mock-structured` models are deterministic and keyless, so every example below runs offline.
 *
 * Wire-shaped records (outcomes, events, policies, durable state) keep the core's `snake_case`
 * field names; JavaScript option bags (`RunOptions`, `GenerateObjectOptions`, ...) use
 * `camelCase`. Unknown option fields are rejected rather than ignored.
 *
 * @example
 * ```ts
 * const { Agent } = require("aikit-runtime"); // or "./crates/aikit-node" from a checkout
 * const agent = Agent.fromEnv({}); // keyless: only the deterministic mock models are active
 * const { text } = await agent.generateText("Say hello");
 * ```
 *
 * @packageDocumentation
 */

/** A JSON scalar. */
export type JsonPrimitive = string | number | boolean | null;
/** Any JSON-serializable value; used wherever the core accepts or returns raw JSON. */
export type JsonValue = JsonPrimitive | JsonValue[] | { [key: string]: JsonValue };
/** Provider-keyed metadata observed during a run, in arrival order. */
export type ProviderMetadata = Record<string, JsonValue[]>;

/**
 * Stable, branchable error category shared by Rust, Python, and Node.
 *
 * `provider_*` codes describe upstream provider failures (`provider_auth` also covers a missing
 * credential, e.g. calling a live model before `addKey`). `configuration` covers invalid local
 * setup or options; `permission_denied`, `budget_exceeded`, `max_turns`, and `cancelled` are
 * governed terminal outcomes.
 */
export type ErrorCode =
  | "provider_auth"
  | "provider_rate_limit"
  | "provider_timeout"
  | "provider_transport"
  | "provider_server"
  | "provider_invalid_request"
  | "provider_protocol"
  | "provider_safety"
  | "configuration"
  | "permission_denied"
  | "sandbox"
  | "budget_exceeded"
  | "max_turns"
  | "tool_execution"
  | "structured_output"
  | "session"
  | "conflict"
  | "cancelled"
  | "audit"
  | "hook"
  | "unknown";

/** Redacted, serializable error envelope carried by {@link AikitError} and error deltas. */
export interface ErrorInfo {
  /** Stable category; branch on this instead of parsing `message`. */
  code: ErrorCode;
  /** Human-readable, redacted description. Wording may change between releases. */
  message: string;
  /** Provider involved in the failure, when known. */
  provider: string | null;
  /** Model involved in the failure, when known. */
  model: string | null;
  /** Upstream HTTP status, when the failure came from an HTTP response. */
  status: number | null;
  /** Provider-advised retry delay in milliseconds, when one was supplied. */
  retry_after_ms: number | null;
  /** Whether retrying the same request may succeed (for example a rate limit or timeout). */
  retryable: boolean;
  /** Preserved preflight warnings; omitted when no warning was observed. */
  warnings?: ProviderWarning[];
}

/**
 * Shape of errors thrown or rejected by aikit calls that surface a core failure: synchronous
 * `Agent.run` / `Client.query` / `query` startup, `generateText` / `generateObject` rejections,
 * terminal stream failures, `DurableRun` methods, and the validation/normalization helpers.
 *
 * This is a structural interface, not a runtime class: `instanceof AikitError` does not work, and
 * `error.name` is `"AikitError"` only for normalized asynchronous failures. Branch on
 * `error.code` (or `error.info`) instead. Plain JavaScript argument mistakes, such as an unknown
 * option field, are thrown as ordinary `TypeError`s without `code`.
 *
 * @example
 * ```ts
 * try {
 *   await agent.generateText("hello", { model: "gpt-5" });
 * } catch (error) {
 *   const { code, info } = error as AikitError;
 *   if (code === "provider_auth") console.error("Add a key first:", info.message);
 *   else if (info?.retryable) console.error("Transient; retry later");
 *   else throw error;
 * }
 * ```
 */
export interface AikitError extends Error {
  code: ErrorCode;
  info: ErrorInfo;
}

/**
 * One canonical event yielded by a {@link QueryStream} (`Agent.run`, `Agent.streamText`,
 * `Client.query`, `query`). Discriminate on `type`; concatenate `text_delta.text` for the answer.
 */
export type StreamDelta =
  | { type: "message_start"; model: string }
  | { type: "text_delta"; text: string }
  | { type: "reasoning_delta"; text: string }
  | {
      type: "reasoning_complete";
      text: string;
      signature?: string;
      opaque?: JsonValue;
    }
  | { type: "tool_call_start"; id: string; name: string }
  | { type: "tool_call_input"; id: string; input: JsonValue }
  | {
      type: "tool_result";
      tool_use_id: string;
      content: string;
      is_error: boolean;
    }
  | { type: "citation"; text: string; source?: string; metadata?: JsonValue }
  | { type: "provider_metadata"; provider: string; metadata: JsonValue }
  | { type: "warning"; warning: ProviderWarning }
  | ({ type: "usage" } & Usage)
  | { type: "message_stop"; stop_reason: string }
  | { type: "error"; message: string; info: ErrorInfo };

/** Tri-state capability fact: a missing fact is `unknown`, never assumed `supported`. */
export type CapabilityState = "supported" | "unsupported" | "unknown";
/**
 * How unsupported provider-specific options are handled. `strict` (the default) rejects them;
 * `warn` and `best_effort` are explicit opt-ins that keep going and surface `ProviderWarning`s.
 */
export type CompatibilityMode = "strict" | "warn" | "best_effort";

/** Content block kinds announced by `block_start` in the v2 event view. */
export type StreamBlockKind =
  | "text"
  | "reasoning"
  | "tool_call"
  | "tool_result"
  | "citation"
  | "image"
  | "audio"
  | "transcript"
  | "structured_data";

/** Payload variants of the v2 block-structured {@link StreamEvent} view. */
export type StreamEventKind =
  | { type: "response_start"; response_id: string; model: string }
  | { type: "block_start"; block_id: string; block_kind: StreamBlockKind; name?: string }
  | { type: "block_delta"; block_id: string; delta: JsonValue }
  | { type: "block_end"; block_id: string; value?: JsonValue }
  | { type: "provider_metadata"; provider: string; metadata: JsonValue }
  | { type: "usage"; usage: Usage }
  | { type: "warning"; warning: ProviderWarning }
  | { type: "response_end"; response_id: string; stop_reason: string }
  | { type: "error"; message: string; info: ErrorInfo }
  | { type: "raw_provider_event"; provider: string; event: JsonValue };

/** One v2 event with a stable id and monotonically increasing `sequence`. */
export type StreamEvent = {
  event_id: string;
  sequence: number;
} & StreamEventKind;

/**
 * Single-consumer v2 view returned by {@link QueryStream.events}. Shares cancellation, close, and
 * the outcome recorder with the parent stream.
 */
export interface QueryEventStream extends AsyncIterable<StreamEvent> {
  /**
   * Pull the next event.
   * @returns The next event, or `null` once the stream is exhausted or closed.
   * @throws If called again before the previous `next()` settled (single-consumer).
   */
  next(): Promise<StreamEvent | null>;
  /** Request cooperative cancellation without waiting for finalizers. */
  cancel(): void;
  /** `true` once cancellation was requested. */
  readonly isCancelled: boolean;
  /** Cancel and wait for Stop hooks, audit/session recording, and driver shutdown. */
  close(): Promise<RunOutcome>;
  /** Current recorder snapshot; terminal after exhaustion or close(). */
  outcome(): RunOutcome;
}

/**
 * Cancellable, single-consumer stream of {@link StreamDelta}s for one governed run.
 *
 * Prefer `for await`: breaking out of the loop (or aborting `RunOptions.signal`) automatically
 * calls {@link QueryStream.close}, so Stop hooks and audit/session recording always finish. If you
 * drive `next()` by hand, call `await stream.close()` in a `finally` block.
 *
 * @example
 * ```ts
 * const stream = agent.run("Summarize the report", { maxTurns: 4 });
 * let text = "";
 * for await (const delta of stream) {
 *   if (delta.type === "text_delta") text += delta.text;
 * }
 * console.log(text, stream.outcome().terminal_status);
 * ```
 */
export interface QueryStream extends AsyncIterable<StreamDelta> {
  /**
   * Pull the next delta.
   * @returns The next delta, or `null` once the run finished, was cancelled, or was closed.
   * @throws {@link AikitError} for terminal run failures; also throws if called again before the
   *   previous `next()` settled (single-consumer).
   */
  next(): Promise<StreamDelta | null>;
  /**
   * Alternate single-consumer v2 view with block start/delta/end events. Consume either this view
   * or the parent deltas, not both concurrently.
   * @param responseId Non-empty caller-chosen id stamped on `response_start`/`response_end`.
   */
  events(responseId: string): QueryEventStream;
  /** Request cooperative cancellation without waiting for finalizers. */
  cancel(): void;
  /** `true` once cancellation was requested. */
  readonly isCancelled: boolean;
  /** Cancel and wait for Stop hooks, audit/session recording, and driver shutdown. */
  close(): Promise<RunOutcome>;
  /** Current recorder snapshot; terminal after exhaustion or close(). */
  outcome(): RunOutcome;
}

/**
 * Event yielded by {@link ObjectStream}. Every attempt, provider delta, and validation failure is
 * visible; only a schema-valid (and semantically accepted) value produces `completed`.
 */
export type ObjectStreamEvent<T = JsonValue> =
  | {
      type: "attempt_started";
      attempt: number;
      total_attempts: number;
      fidelity: "native_constrained" | "forced_tool_call" | "prompted_and_parsed";
      repair: boolean;
    }
  | { type: "delta"; attempt: number; delta: StreamDelta }
  | { type: "validation_failed"; attempt: number; error: string; will_retry: boolean }
  | { type: "completed"; object: GeneratedObject<T> };

/** Single-consumer stream returned by `Agent.streamObject`; consume it with `for await`. */
export interface ObjectStream<T = JsonValue> extends AsyncIterable<ObjectStreamEvent<T>> {
  /**
   * Pull the next structured-output event.
   * @returns The next event, or `null` after `completed` or a terminal failure.
   * @throws {@link AikitError} with code `structured_output` when every attempt fails validation.
   */
  next(): Promise<ObjectStreamEvent<T> | null>;
}

/**
 * One block of canonical message content. Prefer `media_input` (hash- and size-pinned) over the
 * legacy source-only `media` block for new code.
 */
export type ContentBlock =
  | { type: "text"; text: string }
  | {
      type: "reasoning";
      text: string;
      signature?: string;
      provider?: string;
      opaque?: JsonValue;
    }
  | { type: "tool_use"; id: string; name: string; input: JsonValue }
  | { type: "tool_result"; tool_use_id: string; content: string; is_error: boolean }
  | {
      type: "media";
      media_type: string;
      source: { kind: "url"; url: string } | { kind: "base64"; data: string };
    }
  | { type: "media_input"; media: MediaInput }
  | { type: "citation"; text: string; source?: string; metadata?: JsonValue };

/** Canonical v0.3 name; ContentBlock remains the v0.x compatibility spelling. */
export type ContentPart = ContentBlock;

/**
 * Canonical validation gate; rejects malformed MIME, size, hash, and unknown fields. Runs locally
 * before any provider I/O.
 * @param media Candidate media descriptor.
 * @returns The validated, canonicalized descriptor.
 * @throws {@link AikitError} when the descriptor is invalid.
 */
export function validateMediaInput(media: MediaInput): MediaInput;

/** One canonical output part produced by a model response. */
export type OutputPart =
  | { type: "text"; text: string }
  | {
      type: "reasoning";
      text: string;
      signature?: string;
      provider?: string;
      opaque?: JsonValue;
    }
  | { type: "image"; media: MediaInput }
  | { type: "audio"; media: MediaInput }
  | { type: "file"; media: MediaInput; filename?: string }
  | { type: "transcript"; text: string; language?: string }
  | { type: "tool_call"; id: string; name: string; input: JsonValue }
  | { type: "tool_result"; tool_use_id: string; content: string; is_error: boolean }
  | { type: "structured_data"; value: JsonValue }
  | { type: "citation"; text: string; source?: string; metadata?: JsonValue };

/**
 * Canonical chat message. Pass `Message[]` anywhere a {@link ModelInput} is accepted to keep
 * media, reasoning, and tool blocks lossless.
 */
export interface Message {
  role: "system" | "user" | "assistant" | "tool";
  content: ContentBlock[];
}

/** String convenience input or lossless canonical history, including URL/base64 media blocks. */
export type ModelInput = string | Message[];

/** Result of `Agent.generateText`. */
export interface GeneratedText {
  /** Final assistant text. */
  text: string;
  /** Token usage for the call. */
  usage: Usage;
  /** Provider stop reason, when reported. */
  stop_reason: string | null;
  /** Full canonical transcript, including any tool calls the model made. */
  messages: Message[];
  provider_metadata: Record<string, JsonValue[]>;
  warnings: ProviderWarning[];
}

/** Result of `Agent.generateObject` and the `completed` event of `Agent.streamObject`. */
export interface GeneratedObject<T = JsonValue> {
  /** Schema-valid value (Zod-parsed when a Zod schema was supplied). */
  value: T;
  /** How the provider produced structured output, from strongest to weakest guarantee. */
  fidelity: "native_constrained" | "forced_tool_call" | "prompted_and_parsed";
  /** Number of attempts used, including repairs. */
  attempts: number;
  provider_metadata: Record<string, JsonValue[]>;
  warnings: ProviderWarning[];
}

/**
 * Verdict returned by a {@link SemanticValidator}. `retry` asks the model to repair (bounded by
 * `maxRetries`); `reject` fails immediately with a `structured_output` error.
 */
export type SemanticValidationDecision =
  | "accept"
  | { action: "accept" }
  | { action: "retry"; reason: string }
  | { action: "reject"; reason: string };

/**
 * Runs after JSON Schema succeeds and before Zod materialization. Keep it pure and idempotent.
 * A thrown error fails closed as a `structured_output` error. No timeout is applied around the
 * callback; wrap slow or remote checks in an application-owned timeout.
 *
 * @example
 * ```ts
 * const validator: SemanticValidator = async (value) =>
 *   (value as { total: number }).total >= 0
 *     ? "accept"
 *     : { action: "retry", reason: "total must be non-negative" };
 * ```
 */
export type SemanticValidator = (
  value: JsonValue,
) => Promise<SemanticValidationDecision>;

/** Options for `Agent.generateText` and `Agent.streamText`. */
export interface GenerateTextOptions {
  /** Model id; defaults to the keyless `mock-1`. Live models need `addKey` first. */
  model?: string;
  /** Output token cap; defaults to 1024. */
  maxTokens?: number;
}

/** Caller-supplied USD pricing used to enforce `RunBudgetPolicy.maxCostUsd`. */
export interface RunPricing {
  inputPerMillionUsd: number;
  outputPerMillionUsd: number;
  cacheReadPerMillionUsd?: number;
  cacheWritePerMillionUsd?: number;
}

/** Per-run budget. Exceeding it ends the run with `terminal_status: "budget_exceeded"`. */
export interface RunBudgetPolicy {
  /** Cap on input + output tokens across every turn of the run. */
  maxTotalTokens?: number;
  /** Cap on estimated spend in USD. Requires `pricing`; without it the run is rejected up front. */
  maxCostUsd?: number;
  /** Explicit pricing used for cost estimation. */
  pricing?: RunPricing;
}

/** Provider retry/backoff policy applied to each model before falling back to the next one. */
export interface RetryPolicy {
  /** Attempts per model, including the first; defaults to 2. */
  maxAttemptsPerModel?: number;
  /** Initial backoff in milliseconds; defaults to 250. */
  baseDelayMs?: number;
  /** Backoff ceiling in milliseconds; defaults to 4000. */
  maxDelayMs?: number;
  /** Timeout for one provider attempt in milliseconds; defaults to 30000. */
  perAttemptTimeoutMs?: number;
}

/**
 * Per-run controls for `Agent.run` and `Client.query`, mapped onto the shared Rust
 * `AgentOptions`. Unknown fields (including nested budget/retry/compaction fields) throw instead
 * of silently falling back to a default.
 *
 * @example
 * ```ts
 * agent.run("Plan the migration", {
 *   model: "mock-1",
 *   maxTurns: 6,
 *   budget: { maxTotalTokens: 20_000 },
 *   retry: { maxAttemptsPerModel: 3 },
 *   signal: AbortSignal.timeout(60_000),
 * });
 * ```
 */
export interface RunOptions {
  /** Primary model id; defaults to the keyless `mock-1`. */
  model?: string;
  /** Models tried in order after the primary model's retries are exhausted. */
  fallbackModels?: string[];
  /** Output token cap per model call; defaults to 4096. */
  maxTokens?: number;
  /** Maximum model turns before the run ends as `max_turns`; defaults to 16. */
  maxTurns?: number;
  /** Provider-keyed native parameters, e.g. `{ openai: { reasoning_effort: "low" } }`. */
  providerOptions?: Record<string, Record<string, JsonValue>>;
  /** Handling of unsupported provider options; defaults to `"strict"`. */
  compatibilityMode?: CompatibilityMode;
  /** Token/cost ceiling for the whole run. */
  budget?: RunBudgetPolicy;
  /** Retry/backoff policy per model. */
  retry?: RetryPolicy;
  /** Caller-owned catalog/request used to select the model immediately before provider startup. */
  routing?: RoutingConfig;
  /** Opt-in context compaction once the transcript approaches `maxContextTokens`. */
  compaction?: {
    maxContextTokens: number;
    /** Most recent messages kept verbatim; defaults to 8. */
    keepRecentMessages?: number;
    /** Model id for model-generated compaction summaries; failures fall back to the extractive note. */
    summaryModel?: string;
    /** Output cap for the summary call; defaults to 512. */
    summaryMaxTokens?: number;
  };
  /** Cancels and fully closes the run when aborted, including before the first delta. */
  signal?: AbortSignal;
}

/** Options for `Agent.generateObject` and `Agent.streamObject`. */
export interface GenerateObjectOptions {
  /** Model id; defaults to the keyless `mock-structured`. */
  model?: string;
  /** Repair attempts after a validation failure; defaults to 2 (the core rejects more than 32). */
  maxRetries?: number;
  /** Output token cap per attempt; defaults to 1024. */
  maxTokens?: number;
  /** Name of the structured response/tool presented to the model; defaults to `"respond"`. */
  name?: string;
  /** Provider-keyed native parameters. */
  providerOptions?: Record<string, Record<string, JsonValue>>;
  /** Handling of unsupported provider options; defaults to `"strict"`. */
  compatibilityMode?: CompatibilityMode;
  /** Optional async semantic check run after JSON Schema validation. */
  validator?: SemanticValidator;
}

/** A model capability; `{ custom }` names caller-defined capabilities. */
export type ModelCapability =
  | "reasoning"
  | "prompt_cache"
  | "citations"
  | "vision"
  | "native_structured_output"
  | "tool_use"
  | "image_generation"
  | "document_input"
  | "audio_input"
  | "transcription"
  | "speech_generation"
  | "realtime_duplex"
  | { custom: string };

/** Catalog pricing in USD per million tokens. */
export interface ModelPricing {
  input_per_million_usd: number;
  output_per_million_usd: number;
  cache_read_per_million_usd: number | null;
  cache_write_per_million_usd: number | null;
}

/** One routable model: limits, pricing, quality score, skills, and capability facts. */
export interface ModelProfile {
  provider: string;
  model: string;
  context_window_tokens: number;
  max_output_tokens: number;
  pricing: ModelPricing | null;
  quality_score: number;
  skills: string[];
  capabilities: ModelCapability[];
  /** Missing keys mean unknown; required unknown capabilities fail closed. */
  capability_states?: Record<string, CapabilityState>;
}

/**
 * Validate a profile with the exact invariant set used by the canonical router.
 * @returns The validated profile.
 * @throws {@link AikitError} describing the first violated invariant.
 */
export function validateModelProfile(profile: ModelProfile): ModelProfile;

/**
 * Resolve explicit tri-state data first; a missing fact remains `unknown`.
 * @example
 * ```ts
 * const [profile] = shippedModelCatalog().profiles;
 * modelCapabilityState(profile, "vision"); // "supported" | "unsupported" | "unknown"
 * ```
 */
export function modelCapabilityState(
  profile: ModelProfile,
  capability: ModelCapability,
): CapabilityState;

/** Public reference a catalog entry was reviewed against. */
export interface CatalogSource {
  provider: string;
  reference: string;
  url: string;
}

/** Versioned, reviewed model catalog. */
export interface ModelCatalogSnapshot {
  schema_version: number;
  catalog_version: string;
  verified_at: string;
  sources: CatalogSource[];
  profiles: ModelProfile[];
}

/** Shipped catalog plus caller overrides, with hashes that identify both layers. */
export interface ResolvedModelCatalog extends ModelCatalogSnapshot {
  shipped_hash: string;
  overrides_hash: string;
  override_count: number;
}

/** Reviewed offline catalog compiled into this exact package; performs no network I/O. */
export function shippedModelCatalog(): ModelCatalogSnapshot;

/**
 * Resolve caller-owned replacements in a separate layer; the shipped snapshot is immutable.
 * @param overrides Profiles that replace or extend shipped entries.
 * @throws {@link AikitError} when an override is invalid.
 */
export function resolveModelCatalog(overrides?: readonly ModelProfile[]): ResolvedModelCatalog;

/** Caller-supplied audit context attached when normalizing an external policy decision. */
export interface ExternalDecisionMetadata {
  policy_rule_id: string;
  input_summary: string;
  risk_evidence?: string[];
  evaluator_revision?: string | null;
}

/** Scope a {@link ScopedPolicyRule} applies to. */
export type PolicyScope =
  | { scope: "global" }
  | { scope: "tenant"; tenant_id: string }
  | { scope: "agent"; agent_id: string }
  | { scope: "run"; run_id: string }
  | { scope: "tool"; tool: string };

/** One allow/ask/deny rule inside a {@link PolicyDocument}. */
export interface ScopedPolicyRule {
  id: string;
  scope: PolicyScope;
  effect: "allow" | "ask" | "deny";
  reason?: string | null;
}

/** Declarative, scope-aware governance policy. */
export interface PolicyDocument {
  schema_version: number;
  default_effect: "allow" | "ask" | "deny";
  rules: ScopedPolicyRule[];
}

/** Integrity-sealed policy produced by {@link sealPolicySnapshot}. */
export interface PolicySnapshot {
  policy: PolicyDocument;
  hash: string;
}

/**
 * Validate and integrity-seal a policy before pinning it to a durable run.
 * @returns The policy together with its content hash.
 * @throws {@link AikitError} when the policy is invalid.
 */
export function sealPolicySnapshot(policy: PolicyDocument): PolicySnapshot;

/** Sealed tenant/agent/run scope produced by {@link sealGovernanceBinding}. */
export interface GovernanceBinding {
  schema_version: number;
  policy_snapshot_hash: string;
  tenant_id?: string;
  agent_id?: string;
  run_id: string;
  binding_hash: string;
}

/**
 * Seal the complete tenant/agent/run scope used by a governed durable run.
 * @param policySnapshot Output of {@link sealPolicySnapshot}.
 * @param runId Run the binding is valid for.
 * @param tenantId Optional tenant scope.
 * @param agentId Optional agent scope.
 * @throws {@link AikitError} when the snapshot or ids are invalid.
 */
export function sealGovernanceBinding(
  policySnapshot: PolicySnapshot,
  runId: string,
  tenantId?: string,
  agentId?: string,
): GovernanceBinding;

/** Engine-neutral, audit-ready policy decision. */
export interface AuditablePolicyDecision {
  engine: "opa" | "cedar" | "native";
  effect: "allow" | "ask" | "deny";
  decision_id: string | null;
  deciding_rule_id: string | null;
  matched_rule_ids: string[];
  input_summary: string;
  risk_evidence: string[];
  evaluator_revision: string | null;
}

/** Completed OPA Data API response body. */
export interface OpaDecisionResponse {
  result:
    | boolean
    | {
        effect: "allow" | "ask" | "deny";
        rule_id?: string;
        matched_rule_ids?: string[];
        risk_evidence?: string[];
        partial?: boolean;
      };
  decision_id?: string;
  metrics?: JsonValue;
  provenance?: JsonValue;
  warning?: string;
}

/** Completed Cedar authorization response. */
export interface CedarDecisionResponse {
  decision: "Allow" | "Deny";
  decision_id?: string;
  permit_policy_ids?: string[];
  forbid_policy_ids?: string[];
  diagnostics?: { reasons?: string[]; errors?: string[] };
  evaluator_revision?: string;
}

/**
 * Normalize a completed external decision; undefined/partial OPA responses fail closed. This
 * performs no network I/O: call OPA yourself and pass the response body.
 * @throws {@link AikitError} when the response or metadata is malformed.
 */
export function normalizeOpaDecision(
  response: OpaDecisionResponse,
  metadata: ExternalDecisionMetadata,
): AuditablePolicyDecision;

/**
 * Normalize Cedar evidence; matched forbids and evaluator diagnostics always deny. This performs
 * no evaluation itself: pass the result of your Cedar authorizer.
 * @throws {@link AikitError} when the response or metadata is malformed.
 */
export function normalizeCedarDecision(
  response: CedarDecisionResponse,
  metadata: ExternalDecisionMetadata,
): AuditablePolicyDecision;

/** Input/output modality a request may require. */
export type ModalityRequirement =
  | "text"
  | "reasoning"
  | "image_input"
  | "image_generation"
  | "document_input"
  | "audio_input"
  | "transcription"
  | "speech_generation"
  | "realtime_duplex"
  | "tool_use"
  | "structured_output";

/** Immutable, hash-pinned media artifact reference. */
export interface MediaArtifact {
  artifact_id: string;
  media_type: string;
  sha256: string;
  size_bytes: number;
  provider?: string;
  model?: string;
}

/** Canonical immutable-artifact validation gate, including a non-empty artifact id. */
export function validateMediaArtifact(artifact: MediaArtifact): MediaArtifact;

/** Generated image artifact. */
export interface GeneratedImage {
  artifact: MediaArtifact;
  revised_prompt?: string;
  provider_metadata?: JsonValue;
}

/** Generated speech/audio artifact. */
export interface GeneratedAudio {
  artifact: MediaArtifact;
  duration_ms?: number;
  voice?: string;
  provider_metadata?: JsonValue;
}

/** Timed transcript segment. */
export interface TranscriptSegment {
  start_ms: number;
  end_ms: number;
  text: string;
  speaker?: string;
  confidence?: number;
}

/** Transcription result. */
export interface Transcript {
  text: string;
  language?: string;
  segments: TranscriptSegment[];
  provider_metadata?: JsonValue;
}

/** Voice-activity detection settings for realtime audio. */
export interface VoiceActivityPolicy {
  enabled: boolean;
  threshold: number;
  prefix_padding_ms: number;
  silence_duration_ms: number;
  interrupt_response: boolean;
}

/** Explicit model choice, or automatic selection optimizing cost, quality, or both. */
export type RoutePolicy =
  | { kind: "explicit"; model: string }
  | { kind: "automatic"; objective: "cost" | "quality" | "balanced" };

/** Requirements a routed model must satisfy. Used by `Agent.route` and `RunOptions.routing`. */
export interface RouteRequest {
  policy: RoutePolicy;
  /** Ignored by Agent.route; active providers come from the Agent's credential state. */
  active_providers: string[];
  estimated_input_tokens: number;
  required_output_tokens: number;
  max_cost_usd: number | null;
  required_skills: string[];
  required_capabilities: ModelCapability[];
}

/** Router output: the selected profile and its estimated cost. */
export interface RouteDecision {
  profile: ModelProfile;
  estimated_cost_usd: number | null;
  policy: RoutePolicy;
  eligible_models: number;
}

/** Caller-owned catalog plus request; the router picks the model right before provider start. */
export interface RoutingConfig {
  profiles: ModelProfile[];
  request: RouteRequest;
}

/** One explicitly remembered value returned by `Agent.recall`. */
export interface MemoryEntry {
  namespace: string;
  key: string;
  value: JsonValue;
  plane: "working" | "episodic" | "semantic";
  revision: number;
  provenance: MemoryProvenance;
  tags: string[];
  importance: number;
  created_unix_ms: number;
  updated_unix_ms: number;
}

/** Optional origin metadata stored with a memory write. */
export interface MemoryProvenance {
  source_run_id?: string;
  source_event_sequence?: number;
  model_generated?: boolean;
}

/** Orchestration budget in the core's serialized shape (`snake_case`, micro-USD). */
export interface BudgetLimits {
  max_model_calls?: number | null;
  max_input_tokens?: number | null;
  max_output_tokens?: number | null;
  max_cost_micro_usd?: number | null;
  wall_time_ms?: number | null;
}

/** Required OS containment for built-in Bash. */
export type ContainmentRequirement =
  | { mode: "required"; backend: "auto" | "seatbelt" | "docker" }
  | { mode: "uncontained" };

/** Which boundaries a containment backend actually enforces on this host. */
export interface ContainmentGuarantees {
  filesystem_write_boundary: boolean;
  sensitive_home_read_boundary: boolean;
  network_boundary: boolean;
  descendant_inheritance: boolean;
  syscall_filter: boolean;
  resource_limits: boolean;
}

/** Probe result for one containment backend. */
export interface BackendCapability {
  backend: "seatbelt" | "docker" | "uncontained";
  available: boolean;
  guarantees: ContainmentGuarantees;
  detail: string;
}

/**
 * Result of `Agent.builtinContainmentCapabilities()`. `selected_backend: null` means Bash calls
 * will fail closed on this host.
 */
export interface ContainmentCapabilityReport {
  requirement: ContainmentRequirement;
  selected_backend: "seatbelt" | "docker" | "uncontained" | null;
  fail_closed: boolean;
  backends: BackendCapability[];
}

/** Optional Docker fallback for contained Bash. The image must already exist locally. */
export interface DockerContainmentOptions {
  /** Immutable name@sha256:<64 hex> or local sha256:<64 hex>; never pulled implicitly. */
  image: string;
  executable?: string;
  pidsLimit?: number;
  memoryMiB?: number;
  cpus?: number;
  tmpfsMiB?: number;
}

/** Required acknowledgement for `Agent.registerBrowserTools`. */
export interface BrowserToolsOptions {
  /**
   * Explicit caller assertion that an external proxy, BiDi interceptor, or equivalent boundary
   * already enforces the exact allowed hosts and public-IP policy before every browser request.
   * This flag does not install or verify that boundary.
   */
  externalEgressEnforced: true;
}

/** Routing requirements for one child agent. */
export interface ModelRouteRequirements {
  policy: RoutePolicy;
  max_cost_usd: number | null;
  required_skills: string[];
  required_capabilities: ModelCapability[];
}

/** Canonical child-agent specification. Build one with `Agent.subtask`. */
export interface SubagentSpec {
  id: string;
  prompt: string;
  system: string | null;
  route: ModelRouteRequirements;
  allowed_tools: string[];
  max_turns: number;
  max_tokens: number;
  estimated_input_tokens: number;
}

/** Optional fields for `Agent.subtask`. */
export interface SubtaskOptions {
  /** Child system prompt. */
  system?: string;
  /** Tools the child may call; defaults to none. Inherited tools are narrowed to this list. */
  allowedTools?: string[];
  /** Defaults to 16. */
  maxTurns?: number;
  /** Defaults to 4096. */
  maxTokens?: number;
  /** Routing/budget estimate; defaults to 1024. */
  estimatedInputTokens?: number;
}

/**
 * Canonical record of one run, returned by `QueryStream.outcome()` / `close()`. Feed it to
 * {@link evaluateOutcome} for deterministic CI gates.
 */
export interface RunOutcome {
  messages: Message[];
  usage: Usage;
  terminal_status:
    | "running"
    | "completed"
    | "failed"
    | "budget_exceeded"
    | "max_turns"
    | "cancelled";
  stop_reason: string | null;
  model_attempts: string[];
  final_text?: string;
  /** First message belonging to this invocation; omitted only by legacy persisted outcomes. */
  invocation_start_message_index?: number;
  /** Omitted by the canonical serializer when no provider metadata was observed. */
  provider_metadata?: Record<string, JsonValue[]>;
  /** Omitted by the canonical serializer when no compatibility warning was observed. */
  warnings?: ProviderWarning[];
}

/** Terminal statuses an {@link EvalGate} can assert. */
export type EvalTerminalStatus = Exclude<RunOutcome["terminal_status"], "running">;

/** One deterministic check; the same snake_case JSON contract as the `aikit eval` CLI. */
export type EvalGate =
  | { type: "output_exact"; value: string }
  | { type: "output_contains"; value: string }
  | { type: "output_not_contains"; value: string }
  | { type: "terminal_status"; status: EvalTerminalStatus }
  | { type: "called_tool"; name: string }
  | { type: "did_not_call_tool"; name: string }
  | { type: "tool_sequence"; names: string[]; exact?: boolean }
  | { type: "no_tool_errors" }
  | { type: "max_turns"; value: number }
  | { type: "max_input_tokens"; value: number }
  | { type: "max_output_tokens"; value: number }
  | { type: "max_total_tokens"; value: number }
  | { type: "max_model_attempts"; value: number };

/** Result of one gate. Messages report lengths/counts, never raw output. */
export interface EvalCheck {
  gate: string;
  passed: boolean;
  message: string;
}

/** Aggregate result of {@link evaluateOutcome}. */
export interface EvalVerdict {
  passed: boolean;
  passed_checks: number;
  total_checks: number;
  score: number;
  checks: EvalCheck[];
}

/**
 * Pure, deterministic evaluation over a canonical recorded outcome. Runs no model, tool, or
 * network work. Text, tool, and turn gates need `invocation_start_message_index`, which runtime
 * outcomes always record.
 * @throws When the outcome or a gate has unknown fields or an invalid shape.
 * @example
 * ```ts
 * const verdict = evaluateOutcome(await stream.close(), [
 *   { type: "terminal_status", status: "completed" },
 *   { type: "no_tool_errors" },
 * ]);
 * if (!verdict.passed) process.exitCode = 1;
 * ```
 */
export function evaluateOutcome(
  outcome: RunOutcome,
  gates: readonly EvalGate[],
): EvalVerdict;

/** When durable events are flushed: on every append (`sync`), in the background, or at exit. */
export type DurabilityMode = "sync" | "async" | "exit";
/** Lifecycle status of a {@link DurableRun}. */
export type DurableRunStatus =
  | "running"
  | "paused"
  | "reconcile_required"
  | "completed"
  | "failed"
  | "cancelled";

/** Serializable durable run state; persist it and restore with `DurableRun.fromState`. */
export interface DurableRunState {
  schema_version: number;
  session_id: string;
  run_id: string;
  durability: DurabilityMode;
  parent_run_id: string | null;
  policy_snapshot_hash: string | null;
  governance_binding?: GovernanceBinding;
  events: Array<Record<string, JsonValue>>;
  checkpoints: Record<string, JsonValue>;
  projection: DurableRunProjection;
}

/** Named point a durable run can fork or rewind to. */
export interface DurableCheckpoint {
  checkpoint_id: string;
  run_id: string;
  event_sequence: number;
  parent_checkpoint_id: string | null;
  label: string | null;
  projection: DurableRunProjection;
}

/** One approval decision supplied with a `resume` command. */
export interface ApprovalResolution {
  approval_id: string;
  approved: boolean;
  response?: JsonValue;
}

/** Human-in-the-loop request category. */
export type DurableApprovalKind =
  | "confirmation"
  | "missing_input"
  | "output_review"
  | "edit_retry";

/** Approval lifecycle state. */
export type DurableApprovalStatus = "pending" | "approved" | "rejected";

/** Restart-safe typed approval with explicit request/expiry timestamps. */
export interface DurableApprovalRequest {
  logical_key: string;
  activity_id?: string;
  kind: DurableApprovalKind;
  prompt: string;
  payload: JsonValue;
  policy_snapshot_hash?: string;
  governance_binding?: GovernanceBinding;
  requested_at_unix_ms: number;
  expires_at_unix_ms: number;
}

/** Recorded approval and its resolution, if any. */
export interface DurableApproval {
  approval_id: string;
  logical_key: string;
  activity_id: string | null;
  kind: DurableApprovalKind;
  prompt: string;
  payload: JsonValue;
  policy_snapshot_hash?: string;
  governance_binding?: GovernanceBinding;
  requested_at_unix_ms?: number;
  expires_at_unix_ms?: number;
  status: DurableApprovalStatus;
  response: JsonValue | null;
  resolved_at_unix_ms?: number;
  timed_out: boolean;
  requested_sequence: number;
  resolved_sequence: number | null;
}

/** Execution fence owned by the active durable worker. */
export interface DurableWorkerLease {
  owner_id: string;
  lease_id: string;
  acquired_at_unix_ms: number;
  heartbeat_at_unix_ms: number;
  expires_at_unix_ms: number;
}

/** Current materialized view of a durable run's event log. */
export interface DurableRunProjection {
  branch_id: string;
  status: DurableRunStatus;
  state: JsonValue;
  activities: Record<string, JsonValue>;
  approvals: Record<string, DurableApproval>;
  artifacts: Record<string, JsonValue[]>;
  current_checkpoint_id: string | null;
  pause_reason: string | null;
  /** Present only while a durable worker owns the active execution fence. */
  worker_lease?: DurableWorkerLease;
}

/**
 * Operator command for `DurableRun.applyCommand`. `fork` and `rewind` require
 * `side_effects_reconciled: true` as an explicit attestation.
 */
export type DurableCommand =
  | { command: "resume"; command_id: string; approvals?: ApprovalResolution[] }
  | {
      command: "fork";
      command_id: string;
      new_run_id: string;
      checkpoint_id: string;
      side_effects_reconciled: boolean;
    }
  | {
      command: "rewind";
      command_id: string;
      checkpoint_id: string;
      side_effects_reconciled: boolean;
    }
  | { command: "cancel"; command_id: string; reason?: string | null };

/** Outcome of an applied {@link DurableCommand}. */
export type DurableCommandResult =
  | { type: "resumed"; sequence: number }
  | { type: "forked"; run: DurableRunState }
  | { type: "rewound"; checkpoint_id: string; sequence: number }
  | { type: "cancelled"; sequence: number };

/** A2A task state names. */
export type A2aTaskState =
  | "TASK_STATE_SUBMITTED"
  | "TASK_STATE_WORKING"
  | "TASK_STATE_INPUT_REQUIRED"
  | "TASK_STATE_AUTH_REQUIRED"
  | "TASK_STATE_COMPLETED"
  | "TASK_STATE_FAILED"
  | "TASK_STATE_CANCELED"
  | "TASK_STATE_REJECTED";

/** A2A message author role. */
export type A2aRole = "ROLE_USER" | "ROLE_AGENT";

/** One A2A message part. */
export type A2aPart =
  | { kind: "text"; text: string }
  | { kind: "data"; data: JsonValue }
  | { kind: "file"; uri: string; media_type: string };

/** Inbound A2A message as accepted by `A2aMapper.sendMessage`. */
export interface A2aMessage {
  message_id: string;
  context_id?: string;
  task_id?: string;
  role: A2aRole;
  parts: A2aPart[];
  metadata?: Record<string, JsonValue>;
}

/** Correlation ids recorded on every governed A2A action. */
export interface A2aCorrelationIdentity {
  correlation_id: string;
  request_id: string;
  session_id?: string;
  run_id?: string;
}

/** Authenticated caller identity. Your transport authenticates it; the mapper enforces ownership. */
export interface A2aProtocolPrincipal {
  subject: string;
  tenant_id?: string;
  scopes?: string[];
}

/** Filters and pagination for `A2aMapper.listTasks`. */
export interface A2aListTasksRequest {
  tenant?: string;
  contextId?: string;
  status?: A2aTaskState;
  pageSize?: number;
  pageToken?: string;
}

/** Correlation between A2A ids and aikit session/run ids. */
export interface A2aRunMapping {
  context_id: string;
  session_id: string;
  task_id: string;
  run_id: string;
  message_id: string;
}

/** Internal canonical mapper record. This is not an official A2A wire Task DTO. */
export interface A2aTaskRecord {
  mapping: A2aRunMapping;
  state: A2aTaskState;
  owner_subject: string;
  owner_tenant_id?: string;
  /** Positive JavaScript safe integer, at most Number.MAX_SAFE_INTEGER. */
  created_revision: number;
  /** Positive JavaScript safe integer, at most Number.MAX_SAFE_INTEGER. */
  updated_revision: number;
  status_message?: string;
}

/** Internal canonical mapper receipt. This is not an official A2A wire DTO. */
export interface A2aMessageReceipt {
  message: A2aMessage;
  mapping: A2aRunMapping;
  owner_subject: string;
  owner_tenant_id?: string;
  /** Positive JavaScript safe integer, at most Number.MAX_SAFE_INTEGER. */
  accepted_revision: number;
}

/** Host-dispatch lifecycle state. */
export type A2aDispatchOutboxState =
  | "queued"
  | "running"
  | "reconcile_pending"
  | "settled";

/** How the host should answer the original send request. */
export type A2aSendResponsePolicy = "blocking" | "immediate" | "streaming";

/** Durable host-dispatch record. `attempts` is the exact stale-callback fence. */
export interface A2aDispatchOutboxRecord {
  dispatch_id: string;
  owner_subject: string;
  owner_tenant_id?: string;
  message_id: string;
  task_id: string;
  context_id: string;
  session_id: string;
  run_id: string;
  message: A2aMessage;
  resumed_from?: A2aTaskState;
  envelope: A2aGovernanceEnvelope;
  state: A2aDispatchOutboxState;
  response: JsonValue;
  response_policy: A2aSendResponsePolicy;
  immediate_response?: A2aTaskRecord;
  attempts: number;
  last_error?: string;
  created_revision: number;
  updated_revision: number;
}

/** One page of owner-scoped task records. */
export interface A2aTaskPage {
  tasks: A2aTaskRecord[];
  nextPageToken: string;
  pageSize: number;
  totalSize: number;
}

/** What the host should do next; the mapper itself performs no dispatch or I/O. */
export type A2aAction =
  | {
      kind: "dispatch_message";
      message: A2aMessage;
      mapping: A2aRunMapping;
      resumed_from: A2aTaskState | null;
    }
  | { kind: "duplicate_message"; receipt: A2aMessageReceipt }
  | { kind: "get_task"; task: A2aTaskRecord }
  | { kind: "list_tasks"; page: A2aTaskPage }
  | { kind: "cancel_task"; task: A2aTaskRecord };

/** Authorization verdict for one A2A operation. */
export type A2aGovernanceAuthorization =
  | { status: "allowed" }
  | {
      status: "denied";
      code:
        | "invalid_request"
        | "missing_principal"
        | "missing_scope"
        | "principal_mismatch"
        | "unknown_target"
        | "state_conflict"
        | "invalid_approval"
        | "duplicate_conflict";
      reason: string;
    };

/** Audit envelope recorded for every A2A operation. */
export interface A2aGovernanceEnvelope {
  schema_version: number;
  protocol: "a2a";
  correlation: A2aCorrelationIdentity;
  principal?: A2aProtocolPrincipal;
  operation: string;
  target: string;
  required_scopes: string[];
  authorization: A2aGovernanceAuthorization;
}

/** Mapper result: an allowed action, or a denial with no action. Check `envelope.authorization.status`. */
export type A2aGovernedAction =
  | {
      envelope: A2aGovernanceEnvelope & { authorization: { status: "allowed" } };
      action: A2aAction;
    }
  | {
      envelope: A2aGovernanceEnvelope & {
        authorization: Extract<A2aGovernanceAuthorization, { status: "denied" }>;
      };
      /** Omitted when authorization or mapper validation denies the request. */
      action?: never;
    };

/**
 * Serializable internal mapper state for local persistence and restore only.
 * Its owner indexes, task records, and receipts must never be used as A2A wire DTOs.
 */
export interface A2aMapperState {
  schema_version: number;
  contexts: Record<string, string>;
  context_owners: Record<string, { subject: string; tenant_id?: string }>;
  tasks: Record<string, A2aTaskRecord>;
  receipts: Record<string, A2aMessageReceipt>;
  /** Durable host-dispatch records; persist but do not project as A2A wire DTOs. */
  dispatch_outbox: Record<string, A2aDispatchOutboxRecord>;
  /** Opaque durable cancellation controls; persist but do not expose as protocol responses. */
  cancellation_outbox: Record<string, JsonValue>;
  /** Opaque durable event-delivery intents retained for snapshot/restore. */
  pending_events: Record<string, JsonValue>;
  /** Positive JavaScript safe integer, at most Number.MAX_SAFE_INTEGER. */
  next_sequence: number;
  /** Non-negative JavaScript safe integer, at most Number.MAX_SAFE_INTEGER. */
  revision: number;
}

/**
 * Canonical, transport-neutral mapping backed by the shared Rust core.
 * This class does not start an A2A HTTP/gRPC listener or project official wire responses.
 */
export class A2aMapper {
  /** Create an empty mapper. */
  constructor();
  /**
   * Restore from a previous {@link A2aMapper.snapshot}.
   * @throws When the state is malformed or exceeds safe-integer bounds.
   */
  static fromState(state: A2aMapperState): A2aMapper;
  /** Serialize the full mapper state for local persistence. */
  snapshot(): A2aMapperState;
  /** Map an inbound message to a new or resumed task; duplicate message ids are idempotent. */
  sendMessage(
    message: A2aMessage,
    correlation: A2aCorrelationIdentity,
    principal?: A2aProtocolPrincipal,
    nowUnixMs?: number,
  ): A2aGovernedAction;
  /** List tasks visible to `principal`. */
  listTasks(
    request: A2aListTasksRequest,
    correlation: A2aCorrelationIdentity,
    principal?: A2aProtocolPrincipal,
  ): A2aGovernedAction;
  /** Read one task owned by `principal`. */
  getTask(
    taskId: string,
    correlation: A2aCorrelationIdentity,
    principal?: A2aProtocolPrincipal,
  ): A2aGovernedAction;
  /** Decide whether a task owned by `principal` may be cancelled. */
  cancelTask(
    taskId: string,
    correlation: A2aCorrelationIdentity,
    principal?: A2aProtocolPrincipal,
  ): A2aGovernedAction;
  /** Claim a queued/reconcile-pending dispatch and read its incremented attempt fence. */
  markDispatchRunning(dispatchId: string): A2aMapperState;
  /** Mark an uncertain queued/running host dispatch for reconciliation; raw errors are sanitized. */
  markDispatchReconcilePending(dispatchId: string, error: string): A2aMapperState;
  /** Settle only the exact current dispatch attempt; stale attempts are rejected. */
  transitionDispatchTask(
    dispatchId: string,
    expectedAttempt: number,
    state: A2aTaskState,
    statusMessage?: string,
    nowUnixMs?: number,
  ): A2aMapperState;
  /** Admin-only transition for tasks without an open dispatch. */
  transitionTask(
    taskId: string,
    state: A2aTaskState,
    statusMessage?: string,
    nowUnixMs?: number,
  ): A2aMapperState;
}

// --- BEGIN GENERATED external-agent declarations (scripts/generate-declarations.sh) ---
export type ExternalAgentKind = "codex" | "gemini" | "claude" | "grok";
export type ExternalRuntimeProfile = "external" | "managed";
export type ExternalUpdatePolicy = "check_only" | "vendor_managed" | "aikit_managed";
export type ExternalUpdateChannel = "stable" | "preview";
export type ExternalUpdatePhase = "idle" | "checking" | "available" | "downloading" | "staged" | "activating" | "current" | "rolled_back" | "failed";
export type ExternalRefreshAction = "rediscover" | "refresh_model_catalog" | "check_compatibility_manifest" | "report_external_update" | "stage_managed_update";
export type ExternalRuntimeInstallOrigin = "aikit_managed" | "vendor_native" | "homebrew" | "npm" | "application_bundle" | "explicit_path" | "unknown";
export type ExternalRuntimeDiscoverySource = "explicit" | "path" | "known_location";

export type MediaInputSource =
  | { url: string; kind: "url" }
  | { data: string; kind: "base64" }
  | { data: number[]; kind: "bytes" }
  | { artifact_id: string; kind: "artifact" };

export interface ExternalRuntimeCandidate {
  kind: ExternalAgentKind;
  executable: string;
  real_executable: string;
  origin: ExternalRuntimeInstallOrigin;
  discovered_by: ExternalRuntimeDiscoverySource[];
  aliases?: string[];
}

export interface ExternalRuntimeArtifact {
  target_os: string;
  target_arch: string;
  url: string;
  sha256: string;
  size_bytes: number;
  executable_name: string;
}

export interface ExternalRuntimeRelease {
  kind: ExternalAgentKind;
  channel: ExternalUpdateChannel;
  version: string;
  protocol: string;
  contract_version: number;
  minimum_aikit_version?: string;
  maximum_aikit_version?: string;
  artifacts: ExternalRuntimeArtifact[];
}

export interface ExternalUpdateManifest {
  schema_version: number;
  generated_unix_ms: number;
  expires_unix_ms: number;
  minimum_aikit_version?: string;
  releases: ExternalRuntimeRelease[];
}

export interface ExternalUpdateSelection {
  kind: ExternalAgentKind;
  version: string;
  channel: ExternalUpdateChannel;
  protocol: string;
  artifact: ExternalRuntimeArtifact;
}

export interface ExternalUpdateSource {
  manifest_url: string;
  signature_url: string;
  key_id: string;
  public_key_base64: string;
}

export interface ExternalUpdateConfig {
  schema_version: number;
  instance_id: string;
  kind: ExternalAgentKind;
  profile: ExternalRuntimeProfile;
  policy: ExternalUpdatePolicy;
  channel: ExternalUpdateChannel;
  source: ExternalUpdateSource;
}

export interface ExternalUpdateStateSnapshot {
  schema_version: number;
  instance_id: string;
  profile: ExternalRuntimeProfile;
  policy: ExternalUpdatePolicy;
  channel: ExternalUpdateChannel;
  phase: ExternalUpdatePhase;
  current_version?: string;
  available_version?: string;
  quarantined_version?: string;
  last_manifest_check_unix_ms?: number;
  last_model_refresh_unix_ms?: number;
  last_discovery_unix_ms?: number;
  consecutive_failures: number;
  retry_after_unix_ms?: number;
  last_error?: string;
}

export interface Usage {
  input_tokens: number;
  output_tokens: number;
  cache_creation_input_tokens: number;
  cache_read_input_tokens: number;
  reasoning_tokens: number;
}

export interface ProviderWarning {
  code: string;
  message: string;
  parameter?: string;
  provider?: string;
  model?: string;
}

export interface MediaInput {
  media_type: string;
  source: MediaInputSource;
  sha256: string;
  size_bytes: number;
}
// --- END GENERATED external-agent declarations ---

/**
 * Deterministic, execution-free discovery of vendor CLI runtime candidates. Never runs the
 * discovered binaries.
 * @param kind External agent kind, e.g. `"codex"`, `"gemini"`, `"claude"`, or `"grok"`.
 * @param explicitPath Optional executable path checked first.
 */
export function discoverExternalRuntimeCandidates(
  kind: string,
  explicitPath?: string,
): ExternalRuntimeCandidate[];

/**
 * Verify a detached Ed25519 manifest signature before parsing, exactly like the Rust core.
 * @param trustedKeys Map of key id to base64 public key.
 * @param nowUnixMs Trusted current time, used for expiry checks.
 * @throws When the signature, key id, or manifest is invalid or expired.
 */
export function verifyExternalUpdateManifest(
  manifestBytes: Uint8Array,
  signatureBase64: string,
  keyId: string,
  trustedKeys: Record<string, string>,
  nowUnixMs: number,
): ExternalUpdateManifest;

/**
 * Pick the newest compatible release+artifact for one runtime kind, channel, and target.
 * @returns The selection, or `null` when nothing newer and compatible exists.
 */
export function selectExternalRuntimeUpdate(
  manifest: ExternalUpdateManifest,
  kind: string,
  channel: ExternalUpdateChannel,
  aikitVersion: string,
  targetOs: string,
  targetArch: string,
  currentVersion?: string,
): ExternalUpdateSelection | null;

/**
 * Fail-closed validation of a persisted signed-update configuration document.
 * @throws When the configuration is invalid.
 */
export function validateExternalUpdateConfig(config: ExternalUpdateConfig): void;

/** Due maintenance actions for one update state at an injected instant. */
export function externalUpdateRefreshActions(
  state: ExternalUpdateStateSnapshot,
  nowUnixMs: number,
): ExternalRefreshAction[];

/** Options for {@link spawnExternalAgentSession}. */
export interface ExternalAgentSpawnOptions {
  /** Use this vendor CLI executable instead of discovery. */
  explicitExecutable?: string;
  /** Stable runtime instance id. */
  instanceId?: string;
  /** Claude bridge executable. */
  claudeBridge?: string;
  /** Arguments passed to the Claude bridge executable. */
  claudeBridgeArgs?: string[];
  /** Gemini CLI OAuth reuse requires this explicit acknowledgement, like the CLI flag. */
  acknowledgeVendorTerms?: boolean;
}

/**
 * Spawn one live vendor-CLI session with the shared per-kind policy. The vendor CLI must already
 * be installed and authenticated; call `shutdown()` when done.
 * @param kind External agent kind, e.g. `"codex"`.
 * @param workspace Working directory handed to the vendor CLI.
 * @throws {@link AikitError} when the runtime cannot be found or started.
 */
export function spawnExternalAgentSession(
  kind: string,
  workspace: string,
  options?: ExternalAgentSpawnOptions,
): Promise<ExternalAgentSession>;

/** One live vendor-CLI session; `events()` fails closed when the subscriber lags. */
export class ExternalAgentSession {
  private constructor();
  /** Static description of the spawned runtime. */
  descriptor(): JsonValue;
  initialize(): Promise<JsonValue>;
  authStatus(refresh?: boolean): Promise<JsonValue>;
  listModels(): Promise<JsonValue>;
  newSession(request: JsonValue): Promise<JsonValue>;
  prompt(prompt: JsonValue): Promise<void>;
  cancel(sessionId: string, turnId?: string): Promise<void>;
  respondApproval(sessionId: string, requestId: string, decision: JsonValue): Promise<void>;
  respondUserInput(
    sessionId: string,
    requestId: string,
    answers: Record<string, string>,
  ): Promise<void>;
  /** Subscribe to normalized runtime events. */
  events(): ExternalAgentEventStream;
  /** Stop the vendor CLI process. */
  shutdown(): Promise<void>;
}

/** Async pull stream over normalized runtime events; `next()` is `null` after close. */
export class ExternalAgentEventStream {
  private constructor();
  next(): Promise<JsonValue | null>;
}

/** Typed projection of the fail-closed managed-update state machine. */
export class ExternalUpdateState {
  constructor(instanceId: string, profile: ExternalRuntimeProfile);
  static fromState(state: ExternalUpdateStateSnapshot): ExternalUpdateState;
  snapshot(): ExternalUpdateStateSnapshot;
  get phase(): ExternalUpdatePhase;
  get currentVersion(): string | null;
  get availableVersion(): string | null;
  get quarantinedVersion(): string | null;
  get consecutiveFailures(): number;
  get retryAfterUnixMs(): number | null;
  get lastError(): string | null;
  manifestCheckDue(nowUnixMs: number): boolean;
  modelRefreshDue(nowUnixMs: number): boolean;
  discoveryDue(nowUnixMs: number): boolean;
  mayApplyBinaryUpdate(): boolean;
  refreshActions(nowUnixMs: number): ExternalRefreshAction[];
  recordCheckSuccess(nowUnixMs: number, selection?: ExternalUpdateSelection): void;
  recordFailure(nowUnixMs: number, error: string): void;
  recordDiscovery(nowUnixMs: number): void;
  recordModelRefresh(nowUnixMs: number): void;
  beginDownload(): void;
  recordStaged(): void;
  beginActivation(): void;
  recordActivated(): void;
  recoverActivated(activeVersion: string): boolean;
  quarantineAvailable(): void;
  recordRollback(restoredVersion: string, rejectedVersion?: string): void;
}

/**
 * Stateful wrapper over the canonical append-only Rust durability engine. Every mutating method
 * takes a caller-chosen idempotency id, so replaying the same call after a crash is safe.
 * Durability-engine failures throw {@link AikitError}-shaped errors; malformed input throws a
 * plain `Error`.
 *
 * @example
 * ```ts
 * const run = new DurableRun("session-1", "run-1");
 * const approvalId = run.requestConfirmation("deploy", "Deploy to production?");
 * fs.writeFileSync("run.json", JSON.stringify(run.snapshot()));
 * // ...later, possibly in another process:
 * const restored = DurableRun.fromState(JSON.parse(fs.readFileSync("run.json", "utf8")));
 * restored.resolveApproval("resolve-1", approvalId, true);
 * ```
 */
export class DurableRun {
  /**
   * Start a new durable run.
   * @param durability Flush mode; defaults to `"sync"`.
   */
  constructor(sessionId: string, runId: string, durability?: DurabilityMode);
  /** Restore a run from a previous {@link DurableRun.snapshot}. */
  static fromState(state: DurableRunState): DurableRun;
  /** Start a run pinned to a sealed policy snapshot. */
  static withPolicySnapshot(
    sessionId: string,
    runId: string,
    policySnapshot: PolicySnapshot,
    durability?: DurabilityMode,
  ): DurableRun;
  /** Start a run pinned to a sealed tenant/agent/run governance binding. */
  static withGovernanceBinding(
    sessionId: string,
    runId: string,
    governanceBinding: GovernanceBinding,
    durability?: DurabilityMode,
  ): DurableRun;
  readonly schemaVersion: number;
  readonly sessionId: string;
  readonly runId: string;
  readonly durability: DurabilityMode;
  readonly policySnapshotHash: string | null;
  readonly governanceBinding: GovernanceBinding | null;
  readonly status: DurableRunStatus;
  /** Serializable state for persistence. */
  snapshot(): DurableRunState;
  /** Replace the application state (idempotent on `mutationId`). */
  replaceState(mutationId: string, state: JsonValue): DurableRunState;
  /** Record a checkpoint usable by `fork`/`rewind` (idempotent on `checkpointKey`). */
  checkpoint(checkpointKey: string, label?: string): DurableCheckpoint;
  /** Pause the run with a reason (idempotent on `pauseId`). */
  pause(pauseId: string, reason: string): void;
  /**
   * Request a generic approval. Non-expiring; use {@link DurableRun.requestTypedApproval} for
   * restart-safe expiry.
   * @returns The approval id.
   */
  requestApproval(
    logicalKey: string,
    prompt: string,
    payload: JsonValue,
    activityId?: string,
  ): string;
  /** Request a typed approval with explicit timestamps. @returns The approval id. */
  requestTypedApproval(request: DurableApprovalRequest): string;
  /**
   * Append idempotent timeout denials for approvals expired at `nowUnixMs`.
   * @param nowUnixMs Trusted current time as a `bigint` (e.g. `BigInt(Date.now())`).
   * @returns Ids of the approvals that expired.
   */
  expireApprovals(expirationId: string, nowUnixMs: bigint): string[];
  requestConfirmation(
    logicalKey: string,
    prompt: string,
    details?: JsonValue,
    activityId?: string,
  ): string;
  requestInput(
    logicalKey: string,
    prompt: string,
    inputSchema?: JsonValue,
    activityId?: string,
  ): string;
  requestOutputReview(
    logicalKey: string,
    prompt: string,
    output: JsonValue,
    activityId?: string,
  ): string;
  requestEditRetry(
    logicalKey: string,
    prompt: string,
    output: JsonValue,
    error?: string,
    activityId?: string,
  ): string;
  /** Approve or reject a pending approval and resume (idempotent on `commandId`). */
  resolveApproval(
    commandId: string,
    approvalId: string,
    approved: boolean,
    response?: JsonValue,
  ): DurableCommandResult;
  /** Like {@link DurableRun.resolveApproval}, but rejects approvals expired at `nowUnixMs`. */
  resolveApprovalAt(
    commandId: string,
    approvalId: string,
    approved: boolean,
    nowUnixMs: bigint,
    response?: JsonValue,
  ): DurableCommandResult;
  /** Mark the run completed (idempotent on `completionId`). */
  complete(completionId: string): void;
  /** Mark the run failed (idempotent on `failureId`). */
  fail(failureId: string, error: string): void;
  /** Apply a resume/fork/rewind/cancel command. */
  applyCommand(command: DurableCommand): DurableCommandResult;
  /** Apply a command, checking approval expiry at the trusted `nowUnixMs`. */
  applyCommandAt(command: DurableCommand, nowUnixMs: bigint): DurableCommandResult;
}

/** One deterministic trace assertion. */
export type TraceAssertion =
  | { type: "stream_sequence_monotonic" }
  | { type: "stream_blocks_balanced" }
  | { type: "durable_sequence_monotonic" }
  | { type: "no_duplicate_activity_completion" }
  | { type: "all_required_reconciliations_resolved" }
  | { type: "approval_resolved"; approval_id: string; approved: boolean }
  | { type: "run_status"; status: DurableRunStatus };

/** Named set of trace assertions for {@link evaluateTrace}. */
export interface TraceEvalSuite {
  schema_version: number;
  name: string;
  assertions: TraceAssertion[];
}

/** Recorded stream events and/or durable events to evaluate. */
export interface TraceInput {
  stream_events?: StreamEvent[];
  durable_events?: Array<Record<string, JsonValue>>;
  run_status?: DurableRunStatus | null;
}

/** Result of one trace assertion. */
export interface TraceCheck {
  assertion: string;
  passed: boolean;
  message: string;
}

/** Aggregate result of {@link evaluateTrace}. */
export interface TraceEvalResult {
  suite: string;
  passed: boolean;
  passed_checks: number;
  total_checks: number;
  checks: TraceCheck[];
}

/**
 * Pure deterministic evaluation; performs no provider, tool, filesystem, or network work.
 * @throws {@link AikitError} when the suite or trace is malformed.
 */
export function evaluateTrace(
  suite: TraceEvalSuite,
  trace: TraceInput,
): TraceEvalResult;

/** Result of one child run. Branch on `status`; `error_info` carries typed failures. */
export interface SubagentResult {
  id: string;
  status:
    | "succeeded"
    | "invalid_spec"
    | "route_rejected"
    | "budget_rejected"
    | "max_turns"
    | "failed"
    | "session_rejected"
    | "session_conflict"
    | "audit_rejected";
  model: string | null;
  final_text: string | null;
  outcome: RunOutcome;
  error: JsonValue | null;
  /** Present on typed child failures; omitted on successful runs. */
  error_info?: ErrorInfo;
  session_revision: number | null;
}

/** Result of `Agent.council`. */
export interface CouncilResult {
  status:
    | { kind: "succeeded" }
    | { kind: "insufficient_successes"; required: number; actual: number }
    | { kind: "synthesis_failed" };
  members: SubagentResult[];
  synthesis: SubagentResult | null;
}

/** Shared controls for `runSubagent`, `fanOut`/`parallel`, `council`, and `resumeSubagent`. */
export interface OrchestrationOptions {
  /** Maximum children running concurrently. */
  maxParallelism?: number;
  /** Shared budget across every child in the call. */
  budget?: BudgetLimits;
}

/** Capabilities of one active provider. */
export interface ProviderCapabilityView {
  provider: string;
  supports_reasoning: boolean;
  supports_prompt_cache: boolean;
  supports_vision: boolean;
  supports_citations: boolean;
  structured_output: "native_constrained" | "forced_tool_call" | "prompted_and_parsed";
  structured_output_features: StructuredOutputCapabilities;
}

/** Tri-state structured-output feature support for one provider. */
export interface StructuredOutputCapabilities {
  native_schema: CapabilityState;
  forced_tool: CapabilityState;
  prompted_parse: CapabilityState;
  schema_with_tools: CapabilityState;
  streaming_schema: CapabilityState;
  parallel_tools: CapabilityState;
}

/** Snapshot returned by `Agent.capabilities()`; grows as keys and tools are added. */
export interface AgentCapabilities {
  providers: ProviderCapabilityView[];
  tools: string[];
  runtime_features: string[];
}

/** Structural subset of a Zod v4 schema used by the dependency-free JS wrapper. */
export interface ZodSchemaLike<T> {
  readonly _zod: unknown;
  parse(input: unknown): T;
}

/** Context passed to the `Agent.canUseTool` approver for an `ask` decision. */
export interface ApprovalRequest {
  run_id: string;
  turn: number;
  tool_use_id: string;
  tool: string;
  input: JsonValue;
}

/**
 * Allow a tool call. `updated_input` replaces the input (rechecked against static deny rules);
 * `updated_permissions` reuses the approval for this run only: `allow_exact_input` for identical
 * input, `allow_tool` for any later call to the same tool.
 */
export type ApprovalAllowResponse =
  | {
      action: "allow";
      decision?: never;
      updated_input?: JsonValue;
      updated_permissions?: ("allow_exact_input" | "allow_tool")[];
      message?: never;
      interrupt?: never;
    }
  | {
      decision: "allow";
      action?: never;
      updated_input?: JsonValue;
      updated_permissions?: ("allow_exact_input" | "allow_tool")[];
      message?: never;
      interrupt?: never;
    };

/** Deny a tool call. `interrupt: true` stops the run before the tool or another model turn. */
export type ApprovalDenyResponse =
  | {
      action: "deny";
      decision?: never;
      message?: string;
      interrupt?: boolean;
      updated_input?: never;
      updated_permissions?: never;
    }
  | {
      decision: "deny";
      action?: never;
      message?: string;
      interrupt?: boolean;
      updated_input?: never;
      updated_permissions?: never;
    };

/** Approver return value: `true`/`"allow"`, `false`/`"deny"`, or a detailed object. */
export type ApprovalResponse =
  | boolean
  | "allow"
  | "deny"
  | ApprovalAllowResponse
  | ApprovalDenyResponse;

/** Context passed to `Agent.onUserPrompt` hooks. */
export interface PromptContext {
  run_id: string;
  prompt: string;
}

/** Context passed to `Agent.onPreToolUse` hooks. */
export interface PreToolUseContext {
  run_id: string;
  turn: number;
  tool_use_id: string;
  tool: string;
  input: JsonValue;
}

/** Context passed to `Agent.onPostToolUse` hooks. */
export interface PostToolUseContext extends PreToolUseContext {
  output: string;
  duration_ms: number;
}

/** Where in the run loop a failure occurred. */
export type FailureStage =
  | "configuration"
  | "provider_start"
  | "provider_stream"
  | "tool_not_advertised"
  | "pre_tool_use"
  | "permission"
  | "tool_execution"
  | "tool_input_validation"
  | "post_tool_use"
  | "max_turns"
  | "malformed_tool_call"
  | "budget"
  | "audit";

/** Context passed to `Agent.onFailure` and `Agent.onPostToolFailure` hooks. */
export interface FailureContext {
  run_id: string;
  turn: number;
  stage: FailureStage;
  tool_use_id: string | null;
  tool: string | null;
  error: string;
}

/** Context passed to `Agent.onStop` hooks when a run ends. */
export interface StopContext {
  run_id: string;
  turns: number;
  reason: string;
  usage: Usage;
}

/** Return nothing/`"continue"` to proceed, `rewrite` to replace the prompt, or `block`. */
export type PromptHookResponse =
  | null
  | undefined
  | "continue"
  | { action: "continue" }
  | { action: "rewrite"; prompt: string }
  | { action: "block"; message?: string };

/** Return nothing/`"continue"` to proceed, `rewrite` to replace the input, or `block`. */
export type PreToolHookResponse =
  | null
  | undefined
  | "continue"
  | { action: "continue" }
  | { action: "rewrite"; input: JsonValue }
  | { action: "block"; message?: string };

/** Return nothing/`"continue"`, `rewrite` to replace the output, or mark it as an error. */
export type PostToolHookResponse =
  | null
  | undefined
  | "continue"
  | { action: "continue" }
  | { action: "rewrite"; output: string }
  | { action: "error" | "mark_error"; message?: string };

/** Return nothing/`"continue"`, or `rewrite` to replace the error text. */
export type FailureHookResponse =
  | null
  | undefined
  | "continue"
  | { action: "continue" }
  | { action: "rewrite"; error: string };

/** Exact, case-sensitive MCP tool visibility policy. Deny entries always win. */
export interface McpToolFilter {
  /** Omit to allow every non-denied tool; an empty list exposes no tools. */
  allow?: readonly string[];
  deny?: readonly string[];
}

/**
 * Outbound client connection to an MCP server. Create one with a static factory, then expose its
 * tools to an agent with `Agent.registerMcp`.
 *
 * @example
 * ```ts
 * const server = await McpConnection.connectStdio("npx", ["-y", "my-mcp-server"], "local");
 * agent.registerMcp(server);
 * ```
 */
export class McpConnection {
  private constructor();
  /**
   * Connect over Streamable HTTP, initialize, and list tools.
   * @param endpoint MCP endpoint URL.
   * @param name Local connection name.
   * @param bearerToken Optional bearer token sent with every request.
   * @param toolFilter Exact allow/deny visibility applied before registration.
   * @throws When the endpoint is invalid, unreachable, or exceeds discovery bounds.
   */
  static connectHttp(endpoint: string, name: string, bearerToken?: string, toolFilter?: McpToolFilter): Promise<McpConnection>;
  /**
   * Spawn an MCP server over stdio, initialize, and list tools.
   * @param env Extra environment variables for the child process.
   * @param inheritEnv Inherit this process's environment; defaults to `false`.
   * @param toolFilter Exact allow/deny visibility applied before registration.
   * @throws When the process cannot be spawned or initialization fails.
   */
  static connectStdio(program: string, args: string[], name: string, env?: Record<string, string>, inheritEnv?: boolean, toolFilter?: McpToolFilter): Promise<McpConnection>;
  /** MCP `resources/list`; pass the previous `nextCursor` to page. */
  listResources(cursor?: string): Promise<JsonValue>;
  /** MCP `resources/read`. */
  readResource(uri: string): Promise<JsonValue>;
  /** MCP `prompts/list`; pass the previous `nextCursor` to page. */
  listPrompts(cursor?: string): Promise<JsonValue>;
  /** MCP `prompts/get`. */
  getPrompt(name: string, arguments: JsonValue): Promise<JsonValue>;
}

/** Deprecated v0.x compatibility namespace. Use McpConnection for new code. */
export const legacy: Readonly<{ McpServer: typeof McpConnection }>;

/**
 * The governed agent: credentials, tools, permission policy, hooks, memory, and audit in one
 * object, backed by the shared Rust core.
 *
 * Providers activate as keys are added ("add a key, gain a capability"). With no keys, only the
 * deterministic keyless mock models (`mock-1`, `mock-structured`) are usable; calling a live
 * model first throws an {@link AikitError} with code `provider_auth`.
 *
 * @example
 * ```ts
 * const { Agent, tool } = require("aikit-runtime");
 * const agent = new Agent(); // discovers supported provider keys from process.env
 * agent.addToolDefinition(
 *   tool("lookup", "Look up a symbol", { type: "object", properties: { symbol: { type: "string" } } },
 *     async (input) => `price:${(input as { symbol: string }).symbol}`),
 * );
 * agent.setPermissions([{ effect: "ask", tool: "lookup" }], "deny");
 * agent.canUseTool(async (request) => request.tool === "lookup");
 * for await (const delta of agent.run("Price of ACME?")) {
 *   if (delta.type === "text_delta") process.stdout.write(delta.text);
 * }
 * ```
 */
export class Agent {
  /** Create an agent and activate every supported provider key found in `process.env`. */
  constructor();
  /**
   * Create an agent from an explicit environment map instead of `process.env`. Pass `{}` for
   * deterministic, keyless tests.
   */
  static fromEnv(env: Record<string, string>): Agent;
  /**
   * Persist structured audit records as owner-only JSONL. The file is opened and validated now.
   * @param payloadPolicy Defaults to `"metadata_only"`; `"full"` also records payloads.
   * @param failureMode Defaults to `"fail_closed"` (an audit write failure fails the run).
   * @throws When the path cannot be opened.
   */
  configureJsonlAudit(
    path: string,
    payloadPolicy?: "metadata_only" | "full",
    failureMode?: "fail_closed" | "best_effort",
  ): void;
  /**
   * Use a crash-safe local JSON memory file. Coordination is process-local, not distributed.
   * @param namespace Tenant namespace; defaults to `"default"`.
   */
  useMemoryFile(path: string, namespace?: string): void;
  /** Use a local JSON session store for subagent execute/resume. Process-local coordination. */
  useSessionFile(path: string): void;
  /** Use a local SQLite memory store. @param namespace Defaults to `"default"`. */
  useSqliteMemory(path: string, namespace?: string): void;
  /** Use a local SQLite session store for subagent execute/resume. */
  useSqliteSessions(path: string): void;
  /**
   * Clear an expired lease after reconciliation; does not execute or resume work.
   * @param sideEffectsReconciled Must be `true`: your attestation that the prior worker is stopped
   *   and every possibly completed external side effect was reconciled.
   * @returns The new session revision.
   * @throws When the attestation is `false` or the lease is missing, active, or malformed.
   */
  recoverExpiredSession(sessionId: string, sideEffectsReconciled: true): number;
  /**
   * Register host-allowlisted web fetch (and optional search) tools.
   * @param allowedHosts Exact hosts the tools may contact.
   * @param searchEndpoint Optional search endpoint URL.
   * @param maxResponseBytes Optional response size cap.
   */
  registerWebTools(allowedHosts: string[], searchEndpoint?: string, maxResponseBytes?: number): void;
  /**
   * Register WebDriver browser tools for an existing session. `options.externalEgressEnforced`
   * is a caller assertion only; aikit does not install or verify the egress boundary.
   */
  registerBrowserTools(
    webdriverEndpoint: string,
    sessionId: string,
    allowedHosts: string[],
    options: BrowserToolsOptions,
  ): void;
  /** Expose an MCP connection's (filtered) tools to this agent. */
  registerMcp(server: McpConnection): void;
  /**
   * Refuse calls to `gatedTools` until the model requests the capability through the built-in
   * `request_capability` tool and the `canUseTool` approver grants it for the run.
   * @throws When no approver is configured or a gated tool is not registered.
   */
  enableCapabilityRequests(gatedTools: string[]): void;
  /**
   * Block prompts matching any regex in `blockedInputPatterns` and redact secrets/PII from output.
   * @throws When a pattern is not a valid regex.
   */
  enableDefaultGuardrails(blockedInputPatterns?: string[]): void;
  /**
   * Add an API key. The provider is inferred from the key format unless `provider` is given.
   * @returns The activated provider name.
   * @throws When the provider cannot be inferred (for example an ambiguous bare `sk-` key).
   * @example
   * ```ts
   * agent.addKey(process.env.ANTHROPIC_API_KEY!); // "anthropic"
   * agent.addKey("sk-...", "openai");             // disambiguate explicitly
   * ```
   */
  addKey(key: string, provider?: string): string;
  /** Names of providers that currently have a credential. */
  activeProviders(): string[];
  /** Whether `provider` currently has a credential. */
  hasProvider(provider: string): boolean;
  /** Introspect providers, tools, and runtime features available right now. */
  capabilities(): AgentCapabilities;
  /**
   * Register one tool and its async host implementation. See also {@link tool} with
   * {@link Agent.addToolDefinition}.
   * @param inputSchema JSON Schema for the tool input.
   * @param callback Receives the (permission-checked) input and resolves to the tool output text.
   */
  addTool(
    name: string,
    description: string,
    inputSchema: JsonValue,
    callback: (input: JsonValue) => Promise<string>,
  ): void;
  /** Register a definition created with {@link tool}. */
  addToolDefinition(definition: ToolDefinition): void;
  /**
   * Register jailed Read/Write/Edit/Glob/Grep tools; Bash stays disabled. The first root resolves
   * relative paths. Fails closed on Windows, which lacks the required file-jail implementation.
   * @throws When no non-empty root is given or a name collides with a registered tool.
   */
  registerBuiltinTools(roots: string[]): void;
  /**
   * Add Bash under Required(Auto), optionally with a digest-pinned Docker fallback. There is no
   * uncontained Bash mode in this binding. Call {@link Agent.registerBuiltinTools} first.
   */
  enableBashWithRequiredContainment(docker?: DockerContainmentOptions): void;
  /**
   * Probe the required Bash backends without weakening containment. Call after
   * {@link Agent.enableBashWithRequiredContainment}.
   */
  builtinContainmentCapabilities(): Promise<ContainmentCapabilityReport>;
  /**
   * Replace the declarative permission policy.
   * @param rules Ordered rules; see {@link RuleSpec}.
   * @param defaultMode Decision when no rule matches; defaults to `"allow"`.
   */
  setPermissions(
    rules?: RuleSpec[],
    defaultMode?: "allow" | "deny" | "ask",
  ): void;
  /** Register the async approver consulted for every `ask` decision. */
  canUseTool(callback: (request: ApprovalRequest) => Promise<ApprovalResponse>): void;
  /** Register a hook that can rewrite or block the user prompt before the first model call. */
  onUserPrompt(callback: (context: PromptContext) => Promise<PromptHookResponse>): void;
  /**
   * Register a hook that runs before each tool call; it can rewrite the input or block the call.
   * @param tool Restrict the hook to one tool name; omit to match every tool.
   */
  onPreToolUse(
    callback: (context: PreToolUseContext) => Promise<PreToolHookResponse>,
    tool?: string,
  ): void;
  /**
   * Register a hook that runs after each successful tool call; it can rewrite the output.
   * @param tool Restrict the hook to one tool name; omit to match every tool.
   */
  onPostToolUse(
    callback: (context: PostToolUseContext) => Promise<PostToolHookResponse>,
    tool?: string,
  ): void;
  /**
   * Register a failure hook limited to tool-use failures. Runs before global `onFailure` hooks.
   * @param tool Restrict the hook to one tool name; omit to match every tool.
   */
  onPostToolFailure(
    callback: (context: FailureContext) => Promise<FailureHookResponse>,
    tool?: string,
  ): void;
  /** Register a hook for every run failure. */
  onFailure(callback: (context: FailureContext) => Promise<FailureHookResponse>): void;
  /** Register a hook that runs once when a run ends, including on cancel and close. */
  onStop(callback: (context: StopContext) => Promise<void>): void;
  /**
   * Generate one complete text response.
   * @param input A prompt string or canonical `Message[]`.
   * @returns Final text, usage, and transcript.
   * @throws {@link AikitError}, e.g. `provider_auth` when the model's provider has no key.
   * @example
   * ```ts
   * const { text, usage } = await agent.generateText("Say hello"); // keyless mock-1
   * ```
   */
  generateText(input: ModelInput, options?: GenerateTextOptions): Promise<GeneratedText>;
  /**
   * Stream one text response as canonical deltas.
   * @throws {@link AikitError} synchronously when the run cannot start.
   */
  streamText(input: ModelInput, options?: GenerateTextOptions): QueryStream;
  /**
   * Start a cancellable governed run with tools, hooks, permissions, budgets, retries, and
   * fallbacks.
   * @throws {@link AikitError} synchronously when the run cannot start (for example a missing
   *   credential); `TypeError` for unknown option fields.
   */
  run(input: ModelInput, options?: RunOptions): QueryStream;
  /** Snapshot this configured agent into a reusable {@link Client}. */
  client(): Client;
  /**
   * Generate a value validated against a Zod v4 schema (requires the optional `zod` peer).
   * @returns The parsed value typed as the schema's output.
   * @throws {@link AikitError} with code `structured_output` when every attempt fails.
   * @example
   * ```ts
   * import { z } from "zod";
   * const { value } = await agent.generateObject("Return status", z.object({ status: z.enum(["ok"]) }));
   * ```
   */
  generateObject<T>(
    input: ModelInput,
    schema: ZodSchemaLike<T>,
    options?: GenerateObjectOptions,
  ): Promise<GeneratedObject<T>>;
  /**
   * Generate a value validated against a JSON Schema. Defaults to the keyless `mock-structured`.
   * @throws {@link AikitError} with code `structured_output` when every attempt fails.
   */
  generateObject<T = JsonValue>(
    input: ModelInput,
    schema: JsonValue,
    options?: GenerateObjectOptions,
  ): Promise<GeneratedObject<T>>;
  /** Stream structured-output attempts against a Zod v4 schema; only `completed` is parsed. */
  streamObject<T>(
    input: ModelInput,
    schema: ZodSchemaLike<T>,
    options?: GenerateObjectOptions,
  ): ObjectStream<T>;
  /**
   * Stream every structured-output attempt, provider delta, and validation failure.
   * @example
   * ```ts
   * for await (const event of agent.streamObject("Return status", schema)) {
   *   if (event.type === "completed") console.log(event.object.value);
   * }
   * ```
   */
  streamObject<T = JsonValue>(
    input: ModelInput,
    schema: JsonValue,
    options?: GenerateObjectOptions,
  ): ObjectStream<T>;
  /** Persist one value in this agent's memory namespace. Model output is never auto-remembered. */
  remember(key: string, value: JsonValue): void;
  /**
   * Compare-and-swap memory write for concurrent writers.
   * @param expectedRevision Current revision as a `bigint` (`0n` for a new key).
   * @param plane Memory plane; defaults to `"working"`.
   * @returns The new revision.
   * @throws When `expectedRevision` is stale (message contains `memory revision conflict`).
   */
  rememberCas(
    key: string,
    value: JsonValue,
    expectedRevision: bigint,
    plane?: "working" | "episodic" | "semantic",
    provenance?: MemoryProvenance,
  ): bigint;
  /** Search explicit memories in this agent's namespace. @param limit Defaults to 10. */
  recall(query: string, limit?: number): MemoryEntry[];
  /**
   * Deterministically route over caller-supplied profiles. `request.active_providers` is replaced
   * with this agent's active providers.
   * @throws When no profile satisfies the request.
   */
  route(profiles: ModelProfile[], request: RouteRequest): RouteDecision;
  /**
   * Run one governed, budget-aware child. Host tools, hooks, approvals, and permissions are
   * inherited, then narrowed to `spec.allowed_tools`.
   */
  runSubagent(
    spec: SubagentSpec,
    profiles: ModelProfile[],
    options?: OrchestrationOptions,
  ): Promise<SubagentResult>;
  /** Build the canonical child specification used by runSubagent/parallel/council. */
  subtask(
    id: string,
    prompt: string,
    route: ModelRouteRequirements,
    options?: SubtaskOptions,
  ): SubagentSpec;
  /** Resume a persisted child session through this agent's session store. */
  resumeSubagent(
    sessionId: string,
    spec: SubagentSpec,
    profiles: ModelProfile[],
    options?: OrchestrationOptions,
  ): Promise<SubagentResult>;
  /** Run independent children with bounded concurrency; results keep input order. */
  fanOut(
    specs: SubagentSpec[],
    profiles: ModelProfile[],
    options?: OrchestrationOptions,
  ): Promise<SubagentResult[]>;
  /** Ergonomic alias for fanOut; execution and ordering semantics are identical. */
  parallel(
    specs: SubagentSpec[],
    profiles: ModelProfile[],
    options?: OrchestrationOptions,
  ): Promise<SubagentResult[]>;
  /**
   * Run `members` in parallel, then the `synthesizer` once at least `minSuccesses` succeed.
   * @param minSuccesses Quorum; defaults to 1.
   */
  council(
    members: SubagentSpec[],
    synthesizer: SubagentSpec,
    profiles: ModelProfile[],
    minSuccesses?: number,
    options?: OrchestrationOptions,
  ): Promise<CouncilResult>;
}

/**
 * Reusable client that snapshots one configured {@link Agent} (credentials, tools, governance
 * callbacks, audit) and runs many queries against it.
 */
export class Client {
  /** Equivalent to `agent.client()`. */
  constructor(agent: Agent);
  /**
   * Start a governed run; same semantics as `Agent.run`.
   * @throws {@link AikitError} synchronously when the run cannot start.
   */
  query(input: ModelInput, options?: RunOptions): QueryStream;
}

/**
 * One declarative permission rule.
 * @example
 * ```ts
 * agent.setPermissions([
 *   { effect: "deny", tool: "Bash", pattern: "rm\\s+-rf" },
 *   { effect: "ask", tool: "Write" },
 * ], "allow");
 * ```
 */
export interface RuleSpec {
  /** Optional rule id recorded in audit and decisions. */
  id?: string;
  /** Decision when the rule matches. */
  effect: "allow" | "deny" | "ask";
  /** Tool name the rule applies to. */
  tool: string;
  /** Regex matched against the tool input's decoded string values. */
  pattern?: string;
  /** Restrict `pattern` to one input field; requires `pattern`. */
  field?: string;
}

/** Options for the top-level {@link query} helper. */
export interface QueryOptions extends RunOptions {
  /** Permission rules for this query. */
  permissions?: RuleSpec[];
  /** Decision when no rule matches; defaults to `"allow"`. */
  defaultMode?: "allow" | "deny" | "ask";
}

/** Tool name to async host implementation, used by {@link query}. */
export type ToolMap = Record<string, (input: JsonValue) => Promise<string>>;

/** Frozen tool definition returned by {@link tool}. */
export interface ToolDefinition {
  readonly name: string;
  readonly description: string;
  readonly inputSchema: JsonValue;
  readonly callback: (input: JsonValue) => Promise<string>;
}

/**
 * Pair a canonical tool schema with its host callback for Agent.addToolDefinition().
 * @throws {TypeError} When the name is empty, the schema is not an object, or the callback is not
 *   a function.
 * @example
 * ```ts
 * agent.addToolDefinition(tool(
 *   "lookup",
 *   "Look up one symbol",
 *   { type: "object", properties: { symbol: { type: "string" } }, required: ["symbol"] },
 *   async (input) => `price:${(input as { symbol: string }).symbol}`,
 * ));
 * ```
 */
export function tool(
  name: string,
  description: string,
  inputSchema: JsonValue,
  callback: (input: JsonValue) => Promise<string>,
): ToolDefinition;

/**
 * Backward-compatible deterministic mock query with host async-tool callbacks. Always uses the
 * in-process mock provider, so no API key is needed. Prefer `Agent.run` for new code.
 * @example
 * ```ts
 * for await (const delta of query("hi", { echo: async (input) => JSON.stringify(input) })) {
 *   console.log(delta.type);
 * }
 * ```
 */
export function query(
  input: ModelInput,
  tools?: ToolMap,
  options?: QueryOptions,
): QueryStream;
