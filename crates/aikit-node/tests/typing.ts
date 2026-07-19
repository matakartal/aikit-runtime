import {
  Agent,
  Client,
  McpServer,
  evaluateOutcome,
  tool,
  type ApprovalResponse,
  type ContainmentCapabilityReport,
  type FailureContext,
  type EvalGate,
  type EvalVerdict,
  type ModelInput,
  type McpToolFilter,
  type ObjectStream,
  type QueryStream,
  type RunOptions,
  type RunOutcome,
  type SemanticValidationDecision,
  type ModelProfile,
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
    ],
  },
];
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
  const http = await McpServer.connectHttp(
    "https://mcp.example.com",
    "remote",
    undefined,
    toolFilter,
  );
  const stdio = await McpServer.connectStdio(
    "server",
    [],
    "local",
    {},
    false,
    { deny: ["Bash"] },
  );
  // @ts-expect-error MCP filters fail closed on unknown fields
  void McpServer.connectHttp("https://mcp.example.com", "bad", undefined, { unknown: [] });
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

  const textStream = agent.streamText(messages);
  void textStream;

  const object = await agent.generateObject(messages, invoiceSchema);
  const invoice: Invoice = object.value;
  void invoice;
}

void consume;
void inspectContainment;
void typedSubagentResume;
void typedCanonicalInputs;
