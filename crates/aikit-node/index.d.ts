export type JsonPrimitive = string | number | boolean | null;
export type JsonValue = JsonPrimitive | JsonValue[] | { [key: string]: JsonValue };
export type ProviderMetadata = Record<string, JsonValue[]>;

export interface Usage {
  input_tokens: number;
  output_tokens: number;
  cache_creation_input_tokens: number;
  cache_read_input_tokens: number;
  reasoning_tokens: number;
}

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

export interface ErrorInfo {
  code: ErrorCode;
  message: string;
  provider: string | null;
  model: string | null;
  status: number | null;
  retry_after_ms: number | null;
  retryable: boolean;
  /** Preserved preflight warnings; omitted when no warning was observed. */
  warnings?: ProviderWarning[];
}

/** Shape attached to synchronous Agent.run/Client.query startup failures. */
export interface AikitError extends Error {
  code: ErrorCode;
  info: ErrorInfo;
}

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

export type CapabilityState = "supported" | "unsupported" | "unknown";
export type CompatibilityMode = "strict" | "warn" | "best_effort";

export interface ProviderWarning {
  code: string;
  message: string;
  parameter?: string;
  provider?: string;
  model?: string;
}

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

export type StreamEvent = {
  event_id: string;
  sequence: number;
} & StreamEventKind;

export interface QueryEventStream extends AsyncIterable<StreamEvent> {
  next(): Promise<StreamEvent | null>;
  cancel(): void;
  readonly isCancelled: boolean;
  close(): Promise<RunOutcome>;
  outcome(): RunOutcome;
}

export interface QueryStream extends AsyncIterable<StreamDelta> {
  next(): Promise<StreamDelta | null>;
  /** Alternate single-consumer v2 view with block start/delta/end events. */
  events(responseId: string): QueryEventStream;
  /** Request cooperative cancellation without waiting for finalizers. */
  cancel(): void;
  readonly isCancelled: boolean;
  /** Cancel and wait for Stop hooks, audit/session recording, and driver shutdown. */
  close(): Promise<RunOutcome>;
  /** Current recorder snapshot; terminal after exhaustion or close(). */
  outcome(): RunOutcome;
}

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

export interface ObjectStream<T = JsonValue> extends AsyncIterable<ObjectStreamEvent<T>> {
  next(): Promise<ObjectStreamEvent<T> | null>;
}

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

export type MediaInputSource =
  | { kind: "url"; url: string }
  | { kind: "base64"; data: string }
  | { kind: "bytes"; data: number[] }
  | { kind: "artifact"; artifact_id: string };

export interface MediaInput {
  media_type: string;
  source: MediaInputSource;
  sha256: string;
  size_bytes: number;
}

/** Canonical validation gate; rejects malformed MIME, size, hash, and unknown fields. */
export function validateMediaInput(media: MediaInput): MediaInput;

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

export interface Message {
  role: "system" | "user" | "assistant" | "tool";
  content: ContentBlock[];
}

/** String convenience input or lossless canonical history, including URL/base64 media blocks. */
export type ModelInput = string | Message[];

export interface GeneratedText {
  text: string;
  usage: Usage;
  stop_reason: string | null;
  messages: Message[];
  provider_metadata: Record<string, JsonValue[]>;
  warnings: ProviderWarning[];
}

export interface GeneratedObject<T = JsonValue> {
  value: T;
  fidelity: "native_constrained" | "forced_tool_call" | "prompted_and_parsed";
  attempts: number;
  provider_metadata: Record<string, JsonValue[]>;
  warnings: ProviderWarning[];
}

export type SemanticValidationDecision =
  | "accept"
  | { action: "accept" }
  | { action: "retry"; reason: string }
  | { action: "reject"; reason: string };

/** Runs after JSON Schema succeeds and before Zod materialization. Keep it pure and idempotent. */
export type SemanticValidator = (
  value: JsonValue,
) => Promise<SemanticValidationDecision>;

export interface GenerateTextOptions {
  model?: string;
  maxTokens?: number;
}

export interface RunPricing {
  inputPerMillionUsd: number;
  outputPerMillionUsd: number;
  cacheReadPerMillionUsd?: number;
  cacheWritePerMillionUsd?: number;
}

export interface RunBudgetPolicy {
  maxTotalTokens?: number;
  maxCostUsd?: number;
  pricing?: RunPricing;
}

export interface RetryPolicy {
  maxAttemptsPerModel?: number;
  baseDelayMs?: number;
  maxDelayMs?: number;
  perAttemptTimeoutMs?: number;
}

export interface RunOptions {
  model?: string;
  fallbackModels?: string[];
  maxTokens?: number;
  maxTurns?: number;
  providerOptions?: Record<string, Record<string, JsonValue>>;
  compatibilityMode?: CompatibilityMode;
  budget?: RunBudgetPolicy;
  retry?: RetryPolicy;
  /** Caller-owned catalog/request used to select the model immediately before provider startup. */
  routing?: RoutingConfig;
  compaction?: {
    maxContextTokens: number;
    keepRecentMessages?: number;
  };
  /** Cancels and fully closes the run when aborted, including before the first delta. */
  signal?: AbortSignal;
}

export interface GenerateObjectOptions {
  model?: string;
  maxRetries?: number;
  maxTokens?: number;
  name?: string;
  providerOptions?: Record<string, Record<string, JsonValue>>;
  compatibilityMode?: CompatibilityMode;
  validator?: SemanticValidator;
}

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

export interface ModelPricing {
  input_per_million_usd: number;
  output_per_million_usd: number;
  cache_read_per_million_usd: number | null;
  cache_write_per_million_usd: number | null;
}

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

/** Validate a profile with the exact invariant set used by the canonical router. */
export function validateModelProfile(profile: ModelProfile): ModelProfile;

/** Resolve explicit tri-state data first; a missing fact remains `unknown`. */
export function modelCapabilityState(
  profile: ModelProfile,
  capability: ModelCapability,
): CapabilityState;

export interface CatalogSource {
  provider: string;
  reference: string;
  url: string;
}

export interface ModelCatalogSnapshot {
  schema_version: number;
  catalog_version: string;
  verified_at: string;
  sources: CatalogSource[];
  profiles: ModelProfile[];
}

export interface ResolvedModelCatalog extends ModelCatalogSnapshot {
  shipped_hash: string;
  overrides_hash: string;
  override_count: number;
}

/** Reviewed offline catalog compiled into this exact package; performs no network I/O. */
export function shippedModelCatalog(): ModelCatalogSnapshot;

/** Resolve caller-owned replacements in a separate layer; the shipped snapshot is immutable. */
export function resolveModelCatalog(overrides?: readonly ModelProfile[]): ResolvedModelCatalog;

export interface ExternalDecisionMetadata {
  policy_rule_id: string;
  input_summary: string;
  risk_evidence?: string[];
  evaluator_revision?: string | null;
}

export type PolicyScope =
  | { scope: "global" }
  | { scope: "tenant"; tenant_id: string }
  | { scope: "agent"; agent_id: string }
  | { scope: "run"; run_id: string }
  | { scope: "tool"; tool: string };

export interface ScopedPolicyRule {
  id: string;
  scope: PolicyScope;
  effect: "allow" | "ask" | "deny";
  reason?: string | null;
}

export interface PolicyDocument {
  schema_version: number;
  default_effect: "allow" | "ask" | "deny";
  rules: ScopedPolicyRule[];
}

export interface PolicySnapshot {
  policy: PolicyDocument;
  hash: string;
}

/** Validate and integrity-seal a policy before pinning it to a durable run. */
export function sealPolicySnapshot(policy: PolicyDocument): PolicySnapshot;

export interface GovernanceBinding {
  schema_version: number;
  policy_snapshot_hash: string;
  tenant_id?: string;
  agent_id?: string;
  run_id: string;
  binding_hash: string;
}

/** Seal the complete tenant/agent/run scope used by a governed durable run. */
export function sealGovernanceBinding(
  policySnapshot: PolicySnapshot,
  runId: string,
  tenantId?: string,
  agentId?: string,
): GovernanceBinding;

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

export interface CedarDecisionResponse {
  decision: "Allow" | "Deny";
  decision_id?: string;
  permit_policy_ids?: string[];
  forbid_policy_ids?: string[];
  diagnostics?: { reasons?: string[]; errors?: string[] };
  evaluator_revision?: string;
}

/** Normalize a completed external decision; undefined/partial OPA responses fail closed. */
export function normalizeOpaDecision(
  response: OpaDecisionResponse,
  metadata: ExternalDecisionMetadata,
): AuditablePolicyDecision;

/** Normalize Cedar evidence; matched forbids and evaluator diagnostics always deny. */
export function normalizeCedarDecision(
  response: CedarDecisionResponse,
  metadata: ExternalDecisionMetadata,
): AuditablePolicyDecision;

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

export interface GeneratedImage {
  artifact: MediaArtifact;
  revised_prompt?: string;
  provider_metadata?: JsonValue;
}

export interface GeneratedAudio {
  artifact: MediaArtifact;
  duration_ms?: number;
  voice?: string;
  provider_metadata?: JsonValue;
}

export interface TranscriptSegment {
  start_ms: number;
  end_ms: number;
  text: string;
  speaker?: string;
  confidence?: number;
}

export interface Transcript {
  text: string;
  language?: string;
  segments: TranscriptSegment[];
  provider_metadata?: JsonValue;
}

export interface VoiceActivityPolicy {
  enabled: boolean;
  threshold: number;
  prefix_padding_ms: number;
  silence_duration_ms: number;
  interrupt_response: boolean;
}

export type RoutePolicy =
  | { kind: "explicit"; model: string }
  | { kind: "automatic"; objective: "cost" | "quality" | "balanced" };

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

export interface RouteDecision {
  profile: ModelProfile;
  estimated_cost_usd: number | null;
  policy: RoutePolicy;
  eligible_models: number;
}

export interface RoutingConfig {
  profiles: ModelProfile[];
  request: RouteRequest;
}

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

export interface MemoryProvenance {
  source_run_id?: string;
  source_event_sequence?: number;
  model_generated?: boolean;
}

export interface BudgetLimits {
  max_model_calls?: number | null;
  max_input_tokens?: number | null;
  max_output_tokens?: number | null;
  max_cost_micro_usd?: number | null;
  wall_time_ms?: number | null;
}

export type ContainmentRequirement =
  | { mode: "required"; backend: "auto" | "seatbelt" | "docker" }
  | { mode: "uncontained" };

export interface ContainmentGuarantees {
  filesystem_write_boundary: boolean;
  sensitive_home_read_boundary: boolean;
  network_boundary: boolean;
  descendant_inheritance: boolean;
  syscall_filter: boolean;
  resource_limits: boolean;
}

export interface BackendCapability {
  backend: "seatbelt" | "docker" | "uncontained";
  available: boolean;
  guarantees: ContainmentGuarantees;
  detail: string;
}

export interface ContainmentCapabilityReport {
  requirement: ContainmentRequirement;
  selected_backend: "seatbelt" | "docker" | "uncontained" | null;
  fail_closed: boolean;
  backends: BackendCapability[];
}

export interface DockerContainmentOptions {
  /** Immutable name@sha256:<64 hex> or local sha256:<64 hex>; never pulled implicitly. */
  image: string;
  executable?: string;
  pidsLimit?: number;
  memoryMiB?: number;
  cpus?: number;
  tmpfsMiB?: number;
}

export interface BrowserToolsOptions {
  /**
   * Explicit caller assertion that an external proxy, BiDi interceptor, or equivalent boundary
   * already enforces the exact allowed hosts and public-IP policy before every browser request.
   * This flag does not install or verify that boundary.
   */
  externalEgressEnforced: true;
}

export interface ModelRouteRequirements {
  policy: RoutePolicy;
  max_cost_usd: number | null;
  required_skills: string[];
  required_capabilities: ModelCapability[];
}

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

export interface SubtaskOptions {
  system?: string;
  allowedTools?: string[];
  maxTurns?: number;
  maxTokens?: number;
  estimatedInputTokens?: number;
}

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

export type EvalTerminalStatus = Exclude<RunOutcome["terminal_status"], "running">;

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

export interface EvalCheck {
  gate: string;
  passed: boolean;
  message: string;
}

export interface EvalVerdict {
  passed: boolean;
  passed_checks: number;
  total_checks: number;
  score: number;
  checks: EvalCheck[];
}

/** Pure, deterministic evaluation over a canonical recorded outcome. */
export function evaluateOutcome(
  outcome: RunOutcome,
  gates: readonly EvalGate[],
): EvalVerdict;

export type DurabilityMode = "sync" | "async" | "exit";
export type DurableRunStatus =
  | "running"
  | "paused"
  | "reconcile_required"
  | "completed"
  | "failed"
  | "cancelled";

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

export interface DurableCheckpoint {
  checkpoint_id: string;
  run_id: string;
  event_sequence: number;
  parent_checkpoint_id: string | null;
  label: string | null;
  projection: DurableRunProjection;
}

export interface ApprovalResolution {
  approval_id: string;
  approved: boolean;
  response?: JsonValue;
}

export type DurableApprovalKind =
  | "confirmation"
  | "missing_input"
  | "output_review"
  | "edit_retry";

export type DurableApprovalStatus = "pending" | "approved" | "rejected";

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

export interface DurableRunProjection {
  branch_id: string;
  status: DurableRunStatus;
  state: JsonValue;
  activities: Record<string, JsonValue>;
  approvals: Record<string, DurableApproval>;
  artifacts: Record<string, JsonValue[]>;
  current_checkpoint_id: string | null;
  pause_reason: string | null;
}

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

export type DurableCommandResult =
  | { type: "resumed"; sequence: number }
  | { type: "forked"; run: DurableRunState }
  | { type: "rewound"; checkpoint_id: string; sequence: number }
  | { type: "cancelled"; sequence: number };

export type A2aTaskState =
  | "TASK_STATE_SUBMITTED"
  | "TASK_STATE_WORKING"
  | "TASK_STATE_INPUT_REQUIRED"
  | "TASK_STATE_AUTH_REQUIRED"
  | "TASK_STATE_COMPLETED"
  | "TASK_STATE_FAILED"
  | "TASK_STATE_CANCELED"
  | "TASK_STATE_REJECTED";

export type A2aRole = "ROLE_USER" | "ROLE_AGENT";

export type A2aPart =
  | { kind: "text"; text: string }
  | { kind: "data"; data: JsonValue }
  | { kind: "file"; uri: string; media_type: string };

export interface A2aMessage {
  message_id: string;
  context_id?: string;
  task_id?: string;
  role: A2aRole;
  parts: A2aPart[];
  metadata?: Record<string, JsonValue>;
}

export interface A2aCorrelationIdentity {
  correlation_id: string;
  request_id: string;
  session_id?: string;
  run_id?: string;
}

export interface A2aProtocolPrincipal {
  subject: string;
  tenant_id?: string;
  scopes?: string[];
}

export interface A2aListTasksRequest {
  tenant?: string;
  contextId?: string;
  status?: A2aTaskState;
  pageSize?: number;
  pageToken?: string;
}

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

export interface A2aTaskPage {
  tasks: A2aTaskRecord[];
  nextPageToken: string;
  pageSize: number;
  totalSize: number;
}

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
  /** Opaque durable host-dispatch records; persist but do not project as A2A wire DTOs. */
  dispatch_outbox: Record<string, JsonValue>;
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
  constructor();
  static fromState(state: A2aMapperState): A2aMapper;
  snapshot(): A2aMapperState;
  sendMessage(
    message: A2aMessage,
    correlation: A2aCorrelationIdentity,
    principal?: A2aProtocolPrincipal,
  ): A2aGovernedAction;
  listTasks(
    request: A2aListTasksRequest,
    correlation: A2aCorrelationIdentity,
    principal?: A2aProtocolPrincipal,
  ): A2aGovernedAction;
  getTask(
    taskId: string,
    correlation: A2aCorrelationIdentity,
    principal?: A2aProtocolPrincipal,
  ): A2aGovernedAction;
  cancelTask(
    taskId: string,
    correlation: A2aCorrelationIdentity,
    principal?: A2aProtocolPrincipal,
  ): A2aGovernedAction;
  transitionTask(
    taskId: string,
    state: A2aTaskState,
    statusMessage?: string,
  ): A2aMapperState;
}

/** Stateful wrapper over the canonical append-only Rust durability engine. */
export class DurableRun {
  constructor(sessionId: string, runId: string, durability?: DurabilityMode);
  static fromState(state: DurableRunState): DurableRun;
  static withPolicySnapshot(
    sessionId: string,
    runId: string,
    policySnapshot: PolicySnapshot,
    durability?: DurabilityMode,
  ): DurableRun;
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
  snapshot(): DurableRunState;
  replaceState(mutationId: string, state: JsonValue): DurableRunState;
  checkpoint(checkpointKey: string, label?: string): DurableCheckpoint;
  pause(pauseId: string, reason: string): void;
  requestApproval(
    logicalKey: string,
    prompt: string,
    payload: JsonValue,
    activityId?: string,
  ): string;
  requestTypedApproval(request: DurableApprovalRequest): string;
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
  resolveApproval(
    commandId: string,
    approvalId: string,
    approved: boolean,
    response?: JsonValue,
  ): DurableCommandResult;
  resolveApprovalAt(
    commandId: string,
    approvalId: string,
    approved: boolean,
    nowUnixMs: bigint,
    response?: JsonValue,
  ): DurableCommandResult;
  complete(completionId: string): void;
  fail(failureId: string, error: string): void;
  applyCommand(command: DurableCommand): DurableCommandResult;
  applyCommandAt(command: DurableCommand, nowUnixMs: bigint): DurableCommandResult;
}

export type TraceAssertion =
  | { type: "stream_sequence_monotonic" }
  | { type: "stream_blocks_balanced" }
  | { type: "durable_sequence_monotonic" }
  | { type: "no_duplicate_activity_completion" }
  | { type: "all_required_reconciliations_resolved" }
  | { type: "approval_resolved"; approval_id: string; approved: boolean }
  | { type: "run_status"; status: DurableRunStatus };

export interface TraceEvalSuite {
  schema_version: number;
  name: string;
  assertions: TraceAssertion[];
}

export interface TraceInput {
  stream_events?: StreamEvent[];
  durable_events?: Array<Record<string, JsonValue>>;
  run_status?: DurableRunStatus | null;
}

export interface TraceCheck {
  assertion: string;
  passed: boolean;
  message: string;
}

export interface TraceEvalResult {
  suite: string;
  passed: boolean;
  passed_checks: number;
  total_checks: number;
  checks: TraceCheck[];
}

/** Pure deterministic evaluation; performs no provider, tool, filesystem, or network work. */
export function evaluateTrace(
  suite: TraceEvalSuite,
  trace: TraceInput,
): TraceEvalResult;

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

export interface CouncilResult {
  status:
    | { kind: "succeeded" }
    | { kind: "insufficient_successes"; required: number; actual: number }
    | { kind: "synthesis_failed" };
  members: SubagentResult[];
  synthesis: SubagentResult | null;
}

export interface OrchestrationOptions {
  maxParallelism?: number;
  budget?: BudgetLimits;
}

export interface ProviderCapabilityView {
  provider: string;
  supports_reasoning: boolean;
  supports_prompt_cache: boolean;
  supports_vision: boolean;
  supports_citations: boolean;
  structured_output: "native_constrained" | "forced_tool_call" | "prompted_and_parsed";
  structured_output_features: StructuredOutputCapabilities;
}

export interface StructuredOutputCapabilities {
  native_schema: CapabilityState;
  forced_tool: CapabilityState;
  prompted_parse: CapabilityState;
  schema_with_tools: CapabilityState;
  streaming_schema: CapabilityState;
  parallel_tools: CapabilityState;
}

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

export interface ApprovalRequest {
  run_id: string;
  turn: number;
  tool_use_id: string;
  tool: string;
  input: JsonValue;
}

export type ApprovalResponse =
  | boolean
  | "allow"
  | "deny"
  | {
      action?: "allow" | "deny";
      decision?: "allow" | "deny";
      updated_input?: JsonValue;
      updated_permissions?: ("allow_exact_input" | "allow_tool")[];
      message?: string;
      interrupt?: boolean;
    };

export interface PromptContext {
  run_id: string;
  prompt: string;
}

export interface PreToolUseContext {
  run_id: string;
  turn: number;
  tool_use_id: string;
  tool: string;
  input: JsonValue;
}

export interface PostToolUseContext extends PreToolUseContext {
  output: string;
  duration_ms: number;
}

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

export interface FailureContext {
  run_id: string;
  turn: number;
  stage: FailureStage;
  tool_use_id: string | null;
  tool: string | null;
  error: string;
}

export interface StopContext {
  run_id: string;
  turns: number;
  reason: string;
  usage: Usage;
}

export type PromptHookResponse =
  | null
  | undefined
  | "continue"
  | { action: "continue" }
  | { action: "rewrite"; prompt: string }
  | { action: "block"; message?: string };

export type PreToolHookResponse =
  | null
  | undefined
  | "continue"
  | { action: "continue" }
  | { action: "rewrite"; input: JsonValue }
  | { action: "block"; message?: string };

export type PostToolHookResponse =
  | null
  | undefined
  | "continue"
  | { action: "continue" }
  | { action: "rewrite"; output: string }
  | { action: "error" | "mark_error"; message?: string };

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

export class McpConnection {
  private constructor();
  static connectHttp(endpoint: string, name: string, bearerToken?: string, toolFilter?: McpToolFilter): Promise<McpConnection>;
  static connectStdio(program: string, args: string[], name: string, env?: Record<string, string>, inheritEnv?: boolean, toolFilter?: McpToolFilter): Promise<McpConnection>;
  listResources(cursor?: string): Promise<JsonValue>;
  readResource(uri: string): Promise<JsonValue>;
  listPrompts(cursor?: string): Promise<JsonValue>;
  getPrompt(name: string, arguments: JsonValue): Promise<JsonValue>;
}

/** Deprecated v0.x compatibility namespace. Use McpConnection for new code. */
export const legacy: Readonly<{ McpServer: typeof McpConnection }>;

export class Agent {
  constructor();
  static fromEnv(env: Record<string, string>): Agent;
  configureJsonlAudit(
    path: string,
    payloadPolicy?: "metadata_only" | "full",
    failureMode?: "fail_closed" | "best_effort",
  ): void;
  useMemoryFile(path: string, namespace?: string): void;
  useSessionFile(path: string): void;
  useSqliteMemory(path: string, namespace?: string): void;
  useSqliteSessions(path: string): void;
  /** Clear an expired lease after reconciliation; does not execute or resume work. */
  recoverExpiredSession(sessionId: string, sideEffectsReconciled: true): number;
  registerWebTools(allowedHosts: string[], searchEndpoint?: string, maxResponseBytes?: number): void;
  registerBrowserTools(
    webdriverEndpoint: string,
    sessionId: string,
    allowedHosts: string[],
    options: BrowserToolsOptions,
  ): void;
  registerMcp(server: McpConnection): void;
  enableCapabilityRequests(gatedTools: string[]): void;
  enableDefaultGuardrails(blockedInputPatterns?: string[]): void;
  addKey(key: string, provider?: string): string;
  activeProviders(): string[];
  hasProvider(provider: string): boolean;
  capabilities(): AgentCapabilities;
  addTool(
    name: string,
    description: string,
    inputSchema: JsonValue,
    callback: (input: JsonValue) => Promise<string>,
  ): void;
  addToolDefinition(definition: ToolDefinition): void;
  /** Register jailed Read/Write/Edit/Glob/Grep tools; Bash stays disabled. */
  registerBuiltinTools(roots: string[]): void;
  /** Add Bash under Required(Auto), optionally with a digest-pinned Docker fallback. */
  enableBashWithRequiredContainment(docker?: DockerContainmentOptions): void;
  /** Probe the required Bash backends without weakening containment. */
  builtinContainmentCapabilities(): Promise<ContainmentCapabilityReport>;
  setPermissions(
    rules?: RuleSpec[],
    defaultMode?: "allow" | "deny" | "ask",
  ): void;
  canUseTool(callback: (request: ApprovalRequest) => Promise<ApprovalResponse>): void;
  onUserPrompt(callback: (context: PromptContext) => Promise<PromptHookResponse>): void;
  onPreToolUse(
    callback: (context: PreToolUseContext) => Promise<PreToolHookResponse>,
    tool?: string,
  ): void;
  onPostToolUse(
    callback: (context: PostToolUseContext) => Promise<PostToolHookResponse>,
    tool?: string,
  ): void;
  onPostToolFailure(
    callback: (context: FailureContext) => Promise<FailureHookResponse>,
    tool?: string,
  ): void;
  onFailure(callback: (context: FailureContext) => Promise<FailureHookResponse>): void;
  onStop(callback: (context: StopContext) => Promise<void>): void;
  generateText(input: ModelInput, options?: GenerateTextOptions): Promise<GeneratedText>;
  streamText(input: ModelInput, options?: GenerateTextOptions): QueryStream;
  run(input: ModelInput, options?: RunOptions): QueryStream;
  client(): Client;
  generateObject<T>(
    input: ModelInput,
    schema: ZodSchemaLike<T>,
    options?: GenerateObjectOptions,
  ): Promise<GeneratedObject<T>>;
  generateObject<T = JsonValue>(
    input: ModelInput,
    schema: JsonValue,
    options?: GenerateObjectOptions,
  ): Promise<GeneratedObject<T>>;
  streamObject<T>(
    input: ModelInput,
    schema: ZodSchemaLike<T>,
    options?: GenerateObjectOptions,
  ): ObjectStream<T>;
  streamObject<T = JsonValue>(
    input: ModelInput,
    schema: JsonValue,
    options?: GenerateObjectOptions,
  ): ObjectStream<T>;
  remember(key: string, value: JsonValue): void;
  rememberCas(
    key: string,
    value: JsonValue,
    expectedRevision: bigint,
    plane?: "working" | "episodic" | "semantic",
    provenance?: MemoryProvenance,
  ): bigint;
  recall(query: string, limit?: number): MemoryEntry[];
  route(profiles: ModelProfile[], request: RouteRequest): RouteDecision;
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
  resumeSubagent(
    sessionId: string,
    spec: SubagentSpec,
    profiles: ModelProfile[],
    options?: OrchestrationOptions,
  ): Promise<SubagentResult>;
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
  council(
    members: SubagentSpec[],
    synthesizer: SubagentSpec,
    profiles: ModelProfile[],
    minSuccesses?: number,
    options?: OrchestrationOptions,
  ): Promise<CouncilResult>;
}

export class Client {
  constructor(agent: Agent);
  query(input: ModelInput, options?: RunOptions): QueryStream;
}

export interface RuleSpec {
  id?: string;
  effect: "allow" | "deny" | "ask";
  tool: string;
  pattern?: string;
  field?: string;
}

export interface QueryOptions extends RunOptions {
  permissions?: RuleSpec[];
  defaultMode?: "allow" | "deny" | "ask";
}

export type ToolMap = Record<string, (input: JsonValue) => Promise<string>>;

export interface ToolDefinition {
  readonly name: string;
  readonly description: string;
  readonly inputSchema: JsonValue;
  readonly callback: (input: JsonValue) => Promise<string>;
}

/** Pair a canonical tool schema with its host callback for Agent.addToolDefinition(). */
export function tool(
  name: string,
  description: string,
  inputSchema: JsonValue,
  callback: (input: JsonValue) => Promise<string>,
): ToolDefinition;

/** Backward-compatible deterministic mock query with host async-tool callbacks. */
export function query(
  input: ModelInput,
  tools?: ToolMap,
  options?: QueryOptions,
): QueryStream;
