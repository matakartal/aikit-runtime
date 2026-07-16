export type JsonPrimitive = string | number | boolean | null;
export type JsonValue = JsonPrimitive | JsonValue[] | { [key: string]: JsonValue };

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
  | ({ type: "usage" } & Usage)
  | { type: "message_stop"; stop_reason: string }
  | { type: "error"; message: string; info: ErrorInfo };

export interface QueryStream extends AsyncIterable<StreamDelta> {
  next(): Promise<StreamDelta | null>;
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
}

export interface GeneratedObject<T = JsonValue> {
  value: T;
  fidelity: "native_constrained" | "forced_tool_call" | "prompted_and_parsed";
  attempts: number;
  provider_metadata: Record<string, JsonValue[]>;
}

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
}

export type ModelCapability =
  | "reasoning"
  | "prompt_cache"
  | "citations"
  | "vision"
  | "native_structured_output"
  | "tool_use"
  | "image_generation"
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
  tags: string[];
  importance: number;
  created_unix_ms: number;
  updated_unix_ms: number;
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
  /** Omitted by the canonical serializer when no provider metadata was observed. */
  provider_metadata?: Record<string, JsonValue[]>;
}

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

export class McpServer {
  static connectHttp(endpoint: string, name: string, bearerToken?: string): Promise<McpServer>;
  static connectStdio(program: string, args: string[], name: string, env?: Record<string, string>, inheritEnv?: boolean): Promise<McpServer>;
  listResources(cursor?: string): Promise<JsonValue>;
  readResource(uri: string): Promise<JsonValue>;
  listPrompts(cursor?: string): Promise<JsonValue>;
  getPrompt(name: string, arguments: JsonValue): Promise<JsonValue>;
}

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
  registerWebTools(allowedHosts: string[], searchEndpoint?: string, maxResponseBytes?: number): void;
  registerBrowserTools(webdriverEndpoint: string, sessionId: string, allowedHosts: string[]): void;
  registerMcp(server: McpServer): void;
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
