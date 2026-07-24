import {
  A2aMapper,
  Agent,
  Client,
  DurableRun,
  McpConnection,
  legacy,
  evaluateOutcome,
  normalizeCedarDecision,
  normalizeOpaDecision,
  modelCapabilityState,
  resolveModelCatalog,
  sealGovernanceBinding,
  sealPolicySnapshot,
  shippedModelCatalog,
  tool,
  validateMediaArtifact,
  validateMediaInput,
  validateModelProfile,
  type AuditablePolicyDecision,
  type A2aDispatchOutboxRecord,
  type A2aGovernedAction,
  type A2aMapperState,
  type CapabilityState,
  type ApprovalResponse,
  type ContainmentCapabilityReport,
  type ContentPart,
  type FailureContext,
  type EvalGate,
  type EvalVerdict,
  type ModelInput,
  type MediaArtifact,
  type MediaInput,
  type PolicyDocument,
  type PolicySnapshot,
  type DurableApproval,
  type DurableApprovalRequest,
  type DurableWorkerLease,
  type GovernanceBinding,
  type JsonValue,
  type McpToolFilter,
  type ObjectStream,
  type OutputPart,
  type ProviderMetadata,
  type QueryStream,
  type RunOptions,
  type RunOutcome,
  type StreamDelta,
  type ModelCapability,
  type SemanticValidationDecision,
  type ModelProfile,
  type ModelCatalogSnapshot,
  type ResolvedModelCatalog,
  type SubagentResult,
  type SubagentSpec,
  type ZodSchemaLike,
} from "../index";

interface Invoice {
  currency: "EUR";
  status: "ok";
}

// The runtime parity test uses real Zod v4. This structural fixture keeps `tsc` dependency-free
// while proving the same inferred generic that a Zod v4 schema supplies.
const invoiceSchema: ZodSchemaLike<Invoice> = {
  _zod: {},
  parse(input: unknown): Invoice {
    return input as Invoice;
  },
};

const agent = Agent.fromEnv({});
// @ts-expect-error MCP connections are factory-only native handles
new McpConnection();
const semanticValidator = async (): Promise<SemanticValidationDecision> => ({
  action: "retry",
  reason: "business invariant not met",
});
agent.addToolDefinition(
  tool(
    "typed_search",
    "Search typed data",
    { type: "object", properties: { q: { type: "string" } } },
    async () => "typed result",
  ),
);
const messages: ModelInput = [
  {
    role: "system",
    content: [{ type: "text", text: "Describe the supplied media." }],
  },
  {
    role: "user",
    content: [
      { type: "text", text: "What is visible?" },
      {
        type: "media",
        media_type: "image/png",
        source: { kind: "url", url: "https://example.com/chart.png" },
      },
      {
        type: "media_input",
        media: {
          media_type: "application/octet-stream",
          source: { kind: "bytes", data: [97, 98, 99] },
          sha256: "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
          size_bytes: 3,
        },
      },
    ],
  },
];
const canonicalContent: ContentPart = { type: "text", text: "canonical" };
const materializedOutput: OutputPart = {
  type: "structured_data",
  value: { status: "ok" },
};
const providerMetadata: ProviderMetadata = {
  mock: [{ request_id: "fixture" }],
};
void canonicalContent;
void materializedOutput;
void providerMetadata;
const strictMedia: MediaInput = {
  media_type: "image/png",
  source: { kind: "artifact", artifact_id: "artifact-1" },
  sha256: "a".repeat(64),
  size_bytes: 12,
};
const strictArtifact: MediaArtifact = {
  artifact_id: "artifact-1",
  media_type: "image/png",
  sha256: "a".repeat(64),
  size_bytes: 12,
};
const checkedMedia: MediaInput = validateMediaInput(strictMedia);
const checkedArtifact: MediaArtifact = validateMediaArtifact(strictArtifact);
const shippedCatalog: ModelCatalogSnapshot = shippedModelCatalog();
const checkedProfile: ModelProfile = validateModelProfile(shippedCatalog.profiles[0]);
const capabilityState: CapabilityState = modelCapabilityState(checkedProfile, "tool_use");
const customCapability: ModelCapability = { custom: "acme_grounding" };
// @ts-expect-error custom capabilities require a string value
const invalidCustomCapability: ModelCapability = { custom: 1 };
const resolvedCatalog: ResolvedModelCatalog = resolveModelCatalog(shippedCatalog.profiles);
const opaEvidence: AuditablePolicyDecision = normalizeOpaDecision(
  { result: { effect: "allow", rule_id: "allow.read" } },
  { policy_rule_id: "package/aikit/read", input_summary: "tool=Read" },
);
const cedarEvidence: AuditablePolicyDecision = normalizeCedarDecision(
  { decision: "Deny", forbid_policy_ids: ["forbid.secret"] },
  { policy_rule_id: "package/aikit/read", input_summary: "tool=Read" },
);
void checkedMedia;
void checkedArtifact;
void capabilityState;
void customCapability;
void invalidCustomCapability;
void resolvedCatalog;
void opaEvidence;
void cedarEvidence;

const durable = new DurableRun("session-typed", "run-typed");
const confirmationId = durable.requestConfirmation("confirm", "Proceed?", { risk: "low" });
const confirmationOutcome = durable.resolveApproval(
  "resume-confirm",
  confirmationId,
  true,
  { accepted: true },
);
void confirmationOutcome;
durable.requestInput("missing", "Currency?", { type: "string" });
durable.requestOutputReview("review", "Review output", { status: "draft" });
durable.requestEditRetry("retry", "Edit or retry", { status: "invalid" }, "mismatch");

const policyDocument: PolicyDocument = {
  schema_version: 1,
  default_effect: "deny",
  rules: [{
    id: "allow.read",
    scope: { scope: "tool", tool: "Read" },
    effect: "allow",
  }],
};
const policySnapshot: PolicySnapshot = sealPolicySnapshot(policyDocument);
const governedDurable = DurableRun.withPolicySnapshot(
  "session-governed",
  "run-governed",
  policySnapshot,
);
const governanceBinding: GovernanceBinding = sealGovernanceBinding(
  policySnapshot,
  "run-scoped",
  "tenant-a",
  "agent-a",
);
const scopedDurable = DurableRun.withGovernanceBinding(
  "session-scoped",
  "run-scoped",
  governanceBinding,
);
void scopedDurable.governanceBinding;
const typedApprovalRequest: DurableApprovalRequest = {
  logical_key: "customer-id",
  kind: "missing_input",
  prompt: "Customer id?",
  payload: { field: "customer_id" },
  policy_snapshot_hash: policySnapshot.hash,
  requested_at_unix_ms: 100,
  expires_at_unix_ms: 200,
};
const typedApprovalId = governedDurable.requestTypedApproval(typedApprovalRequest);
const typedApproval: DurableApproval =
  governedDurable.snapshot().projection.approvals[typedApprovalId];
const typedWorkerLease: DurableWorkerLease | undefined =
  governedDurable.snapshot().projection.worker_lease;
const typedResume = governedDurable.resolveApprovalAt(
  "resume-typed",
  typedApprovalId,
  true,
  150n,
  "cust-1",
);
void governedDurable.policySnapshotHash;
void typedApproval;
void typedWorkerLease;
void typedResume;
void governedDurable.expireApprovals("sweep-typed", 200n);

const a2a = new A2aMapper();
const a2aPrincipal = {
  subject: "typed-owner",
  tenant_id: "typed-tenant",
  scopes: ["a2a:message:send", "a2a:tasks:read"],
};
const a2aCorrelation = { correlation_id: "typed-correlation", request_id: "typed-request" };
const governedA2a: A2aGovernedAction = a2a.sendMessage(
  {
    message_id: "typed-message",
    role: "ROLE_USER",
    parts: [{ kind: "text", text: "typed" }],
  },
  a2aCorrelation,
  a2aPrincipal,
);
if (governedA2a.action !== undefined) {
  void governedA2a.action.kind;
}
const a2aPage = a2a.listTasks(
  { tenant: "typed-tenant", pageSize: 10 },
  a2aCorrelation,
  a2aPrincipal,
);
const a2aState: A2aMapperState = a2a.snapshot();
const a2aRestored: A2aMapper = A2aMapper.fromState(a2aState);
const a2aNextSequence: number = a2aRestored.snapshot().next_sequence;
const a2aDispatchOutbox: Record<string, A2aDispatchOutboxRecord> =
  a2aState.dispatch_outbox;
const a2aCancellationOutbox: Record<string, JsonValue> = a2aState.cancellation_outbox;
const a2aPendingEvents: Record<string, JsonValue> = a2aState.pending_events;
const typedDispatch = Object.values(a2aDispatchOutbox)[0];
if (typedDispatch != null) {
  const dispatchId: string = typedDispatch.dispatch_id;
  const claimedState: A2aMapperState = a2a.markDispatchRunning(dispatchId);
  const reconcileState: A2aMapperState = a2a.markDispatchReconcilePending(
    dispatchId,
    "host outcome unknown",
  );
  const reclaimedState: A2aMapperState = a2a.markDispatchRunning(dispatchId);
  const expectedAttempt: number = reclaimedState.dispatch_outbox[dispatchId].attempts;
  const transitionedState: A2aMapperState = a2a.transitionDispatchTask(
    dispatchId,
    expectedAttempt,
    "TASK_STATE_INPUT_REQUIRED",
  );
  void claimedState;
  void reconcileState;
  void transitionedState;
}
void a2aPage;
void a2aState;
void a2aNextSequence;
void a2aDispatchOutbox;
void a2aCancellationOutbox;
void a2aPendingEvents;
// @ts-expect-error persisted A2A counters are JavaScript numbers, not bigint values
A2aMapper.fromState({ ...a2aState, next_sequence: 1n });
// @ts-expect-error A2A list request uses camelCase field names in Node
a2a.listTasks({ page_size: 10 }, a2aCorrelation, a2aPrincipal);
// @ts-expect-error A2A roles are a closed wire enum
a2a.sendMessage({ message_id: "bad-role", role: "user", parts: [] }, a2aCorrelation, a2aPrincipal);
const evalOutcome: RunOutcome = {
  messages,
  usage: {
    input_tokens: 8,
    output_tokens: 5,
    cache_creation_input_tokens: 0,
    cache_read_input_tokens: 0,
    reasoning_tokens: 0,
  },
  terminal_status: "completed",
  stop_reason: "stop",
  model_attempts: ["mock-1"],
  invocation_start_message_index: 0,
};
const evalGates: EvalGate[] = [
  { type: "output_contains", value: "chart" },
  { type: "terminal_status", status: "completed" },
  { type: "tool_sequence", names: ["search"], exact: false },
  { type: "no_tool_errors" },
  { type: "max_total_tokens", value: 32 },
];
const evalVerdict: EvalVerdict = evaluateOutcome(evalOutcome, evalGates);
void evalVerdict;
agent.configureJsonlAudit(
  "/tmp/aikit-audit.jsonl",
  "metadata_only",
  "fail_closed",
);
agent.useMemoryFile("/tmp/aikit-memory.json", "tenant-a");
agent.useSessionFile("/tmp/aikit-sessions.json");
const recoveredRevision: number = agent.recoverExpiredSession("typed-session", true);
void recoveredRevision;
// @ts-expect-error recovery requires an explicit compile-time reconciliation assertion
agent.recoverExpiredSession("typed-session", false);
agent.useSqliteMemory("/tmp/aikit-state.db", "tenant-a");
agent.useSqliteSessions("/tmp/aikit-state.db");
agent.registerWebTools(["example.com"], "https://example.com/search?q={query}");
agent.registerBrowserTools(
  "http://127.0.0.1:4444",
  "session",
  ["example.com"],
  { externalEgressEnforced: true },
);
const stream: ObjectStream<Invoice> = agent.streamObject(
  messages,
  invoiceSchema,
  {
    providerOptions: { openai: { temperature: 0 } },
    compatibilityMode: "best_effort",
    validator: semanticValidator,
  },
);

async function consume(): Promise<void> {
  for await (const event of stream) {
    if (event.type === "completed") {
      const invoice: Invoice = event.object.value;
      void invoice;
    }
  }
}

agent.canUseTool(async (): Promise<ApprovalResponse> => ({
  decision: "allow",
  updated_permissions: ["allow_exact_input", "allow_tool"],
}));
// @ts-expect-error approval aliases are mutually exclusive and cannot disagree
const conflictingApproval: ApprovalResponse = { action: "allow", decision: "deny" };
void conflictingApproval;
agent.onPostToolFailure(async (context: FailureContext) => {
  if (context.stage === "tool_input_validation") {
    return { action: "rewrite", error: "safe validation failure" };
  }
  return null;
}, "search");
agent.registerBuiltinTools(["/tmp/workspace", "/tmp/shared"]);
agent.enableBashWithRequiredContainment({
  image: `example/aikit@sha256:${"a".repeat(64)}`,
  pidsLimit: 64,
  memoryMiB: 512,
  cpus: 1,
  tmpfsMiB: 64,
});
agent.enableCapabilityRequests(["Bash"]);
agent.enableDefaultGuardrails(["ignore previous instructions"]);

async function configureMcp(): Promise<void> {
  const toolFilter: McpToolFilter = {
    allow: ["read_file", "search"],
    deny: ["write_file"],
  };
  const http = await McpConnection.connectHttp(
    "https://mcp.example.com",
    "remote",
    undefined,
    toolFilter,
  );
  const stdio = await McpConnection.connectStdio(
    "server",
    [],
    "local",
    {},
    false,
    { deny: ["Bash"] },
  );
  // @ts-expect-error MCP filters fail closed on unknown fields
  void McpConnection.connectHttp("https://mcp.example.com", "bad", undefined, { unknown: [] });

  const legacyHttp = await legacy.McpServer.connectHttp("https://mcp.example.com", "legacy");
  agent.registerMcp(legacyHttp);
  agent.registerMcp(http);
  await http.listResources();
  await http.listPrompts();
  await http.readResource("file:///guide");
  await http.getPrompt("review", {});
  void stdio;
}

async function inspectContainment(): Promise<void> {
  const report: ContainmentCapabilityReport =
    await agent.builtinContainmentCapabilities();
  void report;
}

const controller = new AbortController();
const runOptions: RunOptions = {
  model: "mock-1",
  fallbackModels: ["mock-2"],
  maxTokens: 128,
  maxTurns: 4,
  providerOptions: { openai: { temperature: 0 } },
  compatibilityMode: "warn",
  budget: { maxTotalTokens: 1000 },
  retry: { maxAttemptsPerModel: 2 },
  compaction: { maxContextTokens: 4096, keepRecentMessages: 8 },
  routing: {
    profiles: [
      {
        provider: "mock",
        model: "mock-routed",
        context_window_tokens: 8192,
        max_output_tokens: 1024,
        pricing: null,
        quality_score: 1,
        skills: [],
        capabilities: [],
      },
    ],
    request: {
      policy: { kind: "automatic", objective: "quality" },
      active_providers: ["mock"],
      estimated_input_tokens: 8,
      required_output_tokens: 64,
      max_cost_usd: null,
      required_skills: [],
      required_capabilities: [],
    },
  },
  signal: controller.signal,
};
// @ts-expect-error compatibility modes are closed and exact
const invalidCompatibilityOptions: RunOptions = { compatibilityMode: "loose" };
void invalidCompatibilityOptions;
const runStream: QueryStream = agent.run(messages, runOptions);
const client = new Client(agent);
const clientStream: QueryStream = client.query(messages, runOptions);
runStream.cancel();
void runStream.isCancelled;
void runStream.close();
void clientStream.outcome();

const profiles: ModelProfile[] = [
  {
    provider: "mock",
    model: "mock-1",
    context_window_tokens: 8192,
    max_output_tokens: 1024,
    pricing: null,
    quality_score: 1,
    skills: [],
    capabilities: [],
  },
];
const subagentSpec: SubagentSpec = {
  id: "typed-session",
  prompt: "typed child",
  system: null,
  route: {
    policy: { kind: "explicit", model: "mock-1" },
    max_cost_usd: null,
    required_skills: [],
    required_capabilities: [],
  },
  allowed_tools: [],
  max_turns: 2,
  max_tokens: 64,
  estimated_input_tokens: 8,
};
const ergonomicSubtask: SubagentSpec = agent.subtask(
  "typed-ergonomic",
  "typed child",
  subagentSpec.route,
  {
    allowedTools: ["typed_search"],
    maxTurns: 2,
    maxTokens: 64,
    estimatedInputTokens: 8,
  },
);
const parallelResult: Promise<SubagentResult[]> = agent.parallel(
  [ergonomicSubtask],
  profiles,
  { maxParallelism: 1 },
);
void parallelResult;

async function typedSubagentResume(): Promise<void> {
  const created: SubagentResult = await agent.runSubagent(subagentSpec, profiles);
  const resumed: SubagentResult = await agent.resumeSubagent(
    "typed-session",
    subagentSpec,
    profiles,
  );
  const createdErrorCode = created.error_info?.code;
  const resumedProviderMetadata = resumed.outcome.provider_metadata;
  const resumedFinalText = resumed.outcome.final_text;
  void createdErrorCode;
  void resumedProviderMetadata;
  void resumedFinalText;
}

async function typedCanonicalInputs(): Promise<void> {
  const generated = await agent.generateText(messages);
  void generated.messages;
  void generated.warnings[0]?.parameter;

  const warningDelta: StreamDelta = {
    type: "warning",
    warning: { code: "unsupported_parameter", message: "ignored", parameter: "future" },
  };
  if (warningDelta.type === "warning") void warningDelta.warning.parameter;
  const errorDelta: StreamDelta = {
    type: "error",
    message: "provider invalid request",
    info: {
      code: "provider_invalid_request",
      message: "provider invalid request",
      provider: "mock",
      model: "mock-1",
      status: null,
      retry_after_ms: null,
      retryable: false,
      warnings: [warningDelta.warning],
    },
  };
  if (errorDelta.type === "error") void errorDelta.info.warnings?.[0]?.parameter;

  const textStream = agent.streamText(messages);
  void textStream;

  const object = await agent.generateObject(messages, invoiceSchema, {
    compatibilityMode: "best_effort",
  });
  const invoice: Invoice = object.value;
  void object.warnings[0]?.parameter;
  void invoice;
}

void consume;
void inspectContainment;
void typedSubagentResume;
void typedCanonicalInputs;
