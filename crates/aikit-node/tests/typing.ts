import {
  Agent,
  Client,
  McpServer,
  tool,
  type ApprovalResponse,
  type ContainmentCapabilityReport,
  type FailureContext,
  type ModelInput,
  type ObjectStream,
  type QueryStream,
  type RunOptions,
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
agent.configureJsonlAudit(
  "/tmp/aikit-audit.jsonl",
  "metadata_only",
  "fail_closed",
);
agent.useMemoryFile("/tmp/aikit-memory.json", "tenant-a");
agent.useSessionFile("/tmp/aikit-sessions.json");
agent.useSqliteMemory("/tmp/aikit-state.db", "tenant-a");
agent.useSqliteSessions("/tmp/aikit-state.db");
agent.registerWebTools(["example.com"], "https://example.com/search?q={query}");
agent.registerBrowserTools("http://127.0.0.1:4444", "session", ["example.com"]);
const stream: ObjectStream<Invoice> = agent.streamObject(
  messages,
  invoiceSchema,
  {
    providerOptions: { openai: { temperature: 0 } },
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
  const http = await McpServer.connectHttp("https://mcp.example.com", "remote");
  const stdio = await McpServer.connectStdio("server", [], "local", {}, false);
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
