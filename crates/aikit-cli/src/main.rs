use aikit::{
    discover_runtime_candidates, discover_support_executable, evaluate_outcome,
    select_runtime_update, tool, vendor_session_environment, AcpExternalRuntime, AcpRuntimeConfig,
    Agent, BuiltinTools, CodexAppServerConfig, CodexAppServerRuntime, ErrorCode, ErrorInfo,
    EvalCaseReport, EvalDataset, EvalReport, EvalVerdict, ExternalAgentKind, ExternalAgentRuntime,
    ExternalApprovalDecision, ExternalPrompt, ExternalPromptBlock, Governance,
    ManagedRuntimeArtifactStore, Message, NewRuntimeSession, NoTools, PermissionEngine,
    PermissionMode, Rule, RunConfig, RunRecorder, RuntimeEvent, RuntimeEventKind,
    RuntimeInstanceId, RuntimeProfile, RuntimeUpdateChannel, RuntimeUpdateClient,
    RuntimeUpdateConfig, RuntimeUpdatePhase, RuntimeUpdatePolicy, RuntimeUpdateSource, Sandbox,
    StreamDelta, ToolExecutor, ToolSpec,
};
use clap::{Args, CommandFactory, Parser, Subcommand, ValueEnum};
use clap_complete::{generate, Shell};
use futures::StreamExt;
use semver::Version;
use serde::Serialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::io::{self, BufRead, IsTerminal, Read, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;

const MAX_EVAL_DATASET_BYTES: usize = 4 * 1024 * 1024;
const DEFAULT_MAX_LIVE_CASES: u64 = 8;
const DEFAULT_MAX_LIVE_INPUT_BYTES: u64 = 512 * 1024;
const DEFAULT_MAX_LIVE_OUTPUT_TOKENS: u64 = 32_768;
const DEFAULT_MAX_LIVE_WALL_SECONDS: u64 = 600;

const PROVIDERS: [(&str, &str, &str); 8] = [
    ("anthropic", "ANTHROPIC_API_KEY", "claude-*"),
    ("openai", "OPENAI_API_KEY", "gpt-* / o1-* / o3-* / o4-*"),
    ("google", "GEMINI_API_KEY or GOOGLE_API_KEY", "gemini-*"),
    ("deepseek", "DEEPSEEK_API_KEY", "deepseek-*"),
    ("xai", "XAI_API_KEY", "grok-* / xai:grok-*"),
    ("openrouter", "OPENROUTER_API_KEY", "openrouter:*"),
    ("groq", "GROQ_API_KEY", "groq:*"),
    ("mistral", "MISTRAL_API_KEY", "mistral:*"),
];

const ROOT_LONG_ABOUT: &str = "\
Governed, provider-aware AI agents from one Rust core.

This is the source-first terminal surface of aikit-runtime (pre-release). It calls the same core
used by the Rust, Python, and TypeScript SDKs. The default model is `mock-1`, which is keyless,
offline, deterministic, and free; no live provider, vendor CLI, or updater is selected implicitly.";

const ROOT_AFTER_HELP: &str = "\
Quick start:
  aikit run \"Say hello\"                        Keyless, offline mock-1 run
  aikit doctor                                 Check credentials, workspace, containment
  aikit providers                              Show which provider keys are active
  export ANTHROPIC_API_KEY=...                 Activate a live provider (never printed)
  aikit run --model claude-<id> \"Say hello\"    Use it explicitly

Exit codes:
  0  success
  1  local I/O or serialization failure
  2  invalid input, usage error, or incompatible CLI mode
  3  agent, provider, or runtime failure (including eval infrastructure)
  4  eval dataset ran but at least one case failed

Errors are printed to stderr as `aikit: <message>`, sometimes followed by a `hint:` line. Scripts
should pass --format json or --format jsonl and branch on the exit code, not on human text.

Run `aikit <COMMAND> --help` for command-specific examples.";

#[derive(Debug, Parser)]
#[command(
    name = "aikit",
    version,
    about = "Governed, provider-aware AI agents from one Rust core",
    long_about = ROOT_LONG_ABOUT,
    after_help = ROOT_AFTER_HELP
)]
#[command(propagate_version = true, arg_required_else_help = true)]
struct Cli {
    /// Output contract: `text` for humans, `json` for one document, `jsonl` for event streams.
    #[arg(long, global = true, value_enum, default_value_t = OutputFormat::Text)]
    format: OutputFormat,

    /// Suppress non-result informational output (banners, notices, hints); errors still print.
    #[arg(long, global = true)]
    quiet: bool,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
enum OutputFormat {
    /// Human-readable output (default).
    Text,
    /// One pretty-printed JSON document; rejected by multi-event commands.
    Json,
    /// One compact JSON value per line; required for automated chat and agent runs.
    Jsonl,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run one prompt and print the completed result.
    #[command(after_help = RUN_AFTER_HELP)]
    Run(RunArgs),
    /// Start a multi-turn terminal conversation.
    #[command(after_help = CHAT_AFTER_HELP)]
    Chat(ChatArgs),
    /// List supported providers and whether their key is set, without printing credentials.
    #[command(after_help = PROVIDERS_AFTER_HELP)]
    Providers,
    /// Discover and use vendor CLI sessions without copying their credentials.
    #[command(after_help = AGENTS_AFTER_HELP)]
    Agents(AgentsArgs),
    /// Show currently active provider and runtime capabilities as JSON.
    #[command(after_help = "Examples:\n  aikit capabilities\n  aikit --format jsonl capabilities")]
    Capabilities,
    /// Diagnose credentials, workspace access, and Bash containment.
    #[command(after_help = DOCTOR_AFTER_HELP)]
    Doctor(DoctorArgs),
    /// Run a deterministic JSON evaluation dataset; live models require --allow-live.
    #[command(after_help = EVAL_AFTER_HELP)]
    Eval(EvalArgs),
    /// Generate a shell completion script on stdout.
    #[command(after_help = COMPLETIONS_AFTER_HELP)]
    Completions(CompletionArgs),
}

const RUN_AFTER_HELP: &str = "\
Model routing (by id prefix):
  mock-1 (default, keyless)   claude-* -> ANTHROPIC_API_KEY   gpt-*/o1-*/o3-*/o4-* -> OPENAI_API_KEY
  gemini-* -> GEMINI_API_KEY or GOOGLE_API_KEY                deepseek-* -> DEEPSEEK_API_KEY
  grok-*/xai:* -> XAI_API_KEY   openrouter:* -> OPENROUTER_API_KEY
  groq:* -> GROQ_API_KEY        mistral:* -> MISTRAL_API_KEY

Examples:
  aikit run \"Say hello\"
  aikit run --system \"Be concise\" \"Explain Rust ownership\"
  printf 'Summarize this' | aikit run
  aikit --format json run --model grok-4.5 --max-tokens 256 \"Hello\"

A non-mock model makes a billable network call to its provider.";

const CHAT_AFTER_HELP: &str = "\
Commands inside chat:
  /help        show chat commands
  /providers   show provider activation (no secrets)
  /clear       clear history, keeping the --system instruction
  /exit        end the session (also /quit or Ctrl-D)

Examples:
  aikit chat
  aikit chat --model claude-<id> --system \"Answer in one paragraph\"

Text mode requires an interactive terminal; use `aikit run` for piped input. --format json is
rejected because chat emits one event per turn; use --format jsonl for automation.";

const PROVIDERS_AFTER_HELP: &str = "\
A provider is `active` when its conventional environment variable is set to a non-empty value.
Values are never printed. `mock-1` needs no key and is always available.

Examples:
  aikit providers
  aikit --format json providers";

const AGENTS_AFTER_HELP: &str = "\
External coding-agent runtimes are separate from API-key providers: the vendor CLI owns its account
session, tool loop, and approvals. aikit never reads, copies, or prints the CLI's tokens.

Examples:
  aikit agents discover
  aikit agents status codex
  aikit agents login codex
  aikit agents status gemini --acknowledge-vendor-terms
  aikit agents run codex --mode workspace_write \"Fix the focused test\"
  aikit --format jsonl agents run grok --approval deny \"Review the current diff\"

Guide: docs/EXTERNAL-AGENTS.md in the repository.";

const DOCTOR_AFTER_HELP: &str = "\
Statuses: PASS (ready), WARN (usable with a limitation), FAIL (blocking). A credentials warning
still leaves mock-1 available; a bash_containment warning means Bash tools fail closed while file
tools keep working.

Examples:
  aikit doctor
  aikit doctor --workspace ./my-project
  aikit --format json doctor";

const EVAL_AFTER_HELP: &str = "\
Datasets default to mock-1 and run keyless and offline. Any non-mock model is rejected before a
provider is built unless --allow-live is passed; live runs are then capped by the --max-live-*
limits. The dataset must be a regular JSON file of at most 4 MiB.

Examples:
  aikit eval evals/smoke.json
  aikit --format json eval evals/smoke.json
  aikit eval evals/governance-denied.json --demo-tools denied
  aikit eval my-live.json --allow-live --max-live-cases 2

Exit code 4 means the dataset ran and at least one case failed; 3 means provider or runtime
infrastructure failed. Guide: docs/EVALUATIONS.md in the repository.";

const COMPLETIONS_AFTER_HELP: &str = "\
Examples:
  aikit completions bash > ~/.local/share/bash-completion/completions/aikit
  aikit completions zsh  > ~/.zfunc/_aikit
  aikit completions fish > ~/.config/fish/completions/aikit.fish";

#[derive(Debug, Args)]
struct RunArgs {
    /// Prompt text. Use '-' or omit it to read stdin.
    #[arg(value_name = "PROMPT", trailing_var_arg = true)]
    prompt: Vec<String>,

    /// Model id; its prefix selects the provider (see below). Examples: mock-1, grok-4.5.
    #[arg(short, long, value_name = "MODEL", default_value = "mock-1")]
    model: String,

    /// Maximum output-token budget (at least 1).
    #[arg(long, value_name = "N", default_value_t = 1024, value_parser = clap::value_parser!(u64).range(1..))]
    max_tokens: u64,

    /// Optional system instruction prepended to the canonical message list.
    #[arg(long, value_name = "TEXT")]
    system: Option<String>,
}

#[derive(Debug, Args)]
struct ChatArgs {
    /// Model id used for every turn; its prefix selects the provider (see `aikit run --help`).
    #[arg(short, long, value_name = "MODEL", default_value = "mock-1")]
    model: String,

    /// Maximum output-token budget per turn (at least 1).
    #[arg(long, value_name = "N", default_value_t = 1024, value_parser = clap::value_parser!(u64).range(1..))]
    max_tokens: u64,

    /// Optional system instruction retained across the conversation (and after /clear).
    #[arg(long, value_name = "TEXT")]
    system: Option<String>,
}

#[derive(Debug, Args)]
struct AgentsArgs {
    #[command(subcommand)]
    command: AgentCommand,
}

#[derive(Debug, Subcommand)]
enum AgentCommand {
    /// Find installed Codex, Gemini, Claude, and Grok executables (inspected, never executed).
    #[command(
        after_help = "Examples:\n  aikit agents discover\n  aikit agents discover codex\n  aikit agents discover claude --path /usr/local/bin/claude"
    )]
    Discover(AgentDiscoverArgs),
    /// Initialize a runtime and report normalized health/auth status (no model prompt).
    #[command(
        after_help = "Examples:\n  aikit agents status codex\n  aikit agents status gemini --acknowledge-vendor-terms\n  aikit agents status claude --bridge /abs/path/to/pinned-claude-acp\n\nIf auth is not ready, run `aikit agents login KIND`."
    )]
    Status(AgentRuntimeArgs),
    /// Start the vendor-owned login flow without reading or copying tokens.
    #[command(
        after_help = "Examples:\n  aikit agents login codex\n  aikit agents login gemini --acknowledge-vendor-terms"
    )]
    Login(AgentRuntimeArgs),
    /// Ask the vendor runtime to invalidate its current login.
    #[command(after_help = "Example:\n  aikit agents logout codex")]
    Logout(AgentRuntimeArgs),
    /// Report models advertised by the runtime without issuing a billable prompt.
    #[command(
        after_help = "Examples:\n  aikit agents models codex\n  aikit --format json agents models grok\n\nACP runtimes may publish models only after a session starts, so an empty list is not an auth failure."
    )]
    Models(AgentRuntimeArgs),
    /// Run one coding-agent session through the vendor CLI (may be billed to the vendor account).
    #[command(after_help = AGENTS_RUN_AFTER_HELP)]
    Run(AgentRunArgs),
    /// Configure, check, apply, or roll back signed runtime updates.
    #[command(after_help = AGENTS_UPDATE_AFTER_HELP)]
    Update(AgentUpdateArgs),
}

const AGENTS_RUN_AFTER_HELP: &str = "\
Examples:
  aikit agents run codex \"Explain this workspace\"
  aikit agents run codex --mode workspace_write \"Fix the focused test\"
  aikit agents run gemini --acknowledge-vendor-terms \"Summarize the README\"
  printf 'Review the diff' | aikit --format jsonl agents run grok --approval deny

--approval prompt (default) needs an interactive terminal. Automation must choose `deny`, or opt
into `allow-once` explicitly, and use --format jsonl. Ctrl-C sends protocol cancellation first.";

const AGENTS_UPDATE_AFTER_HELP: &str = "\
Typical flow:
  1. aikit agents update configure KIND --manifest-url URL --signature-url URL \\
       --key-id ID --public-key-base64 KEY [--policy check-only|vendor-managed|aikit-managed]
  2. aikit agents update status KIND
  3. aikit agents update sync KIND [--force]
  4. aikit agents update rollback KIND        (aikit-managed only)

Nothing runs in the background; schedule `aikit agents update sync KIND` with the host scheduler
for unattended checks.";

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
enum AgentKindArg {
    Codex,
    Gemini,
    Claude,
    Grok,
}

impl From<AgentKindArg> for ExternalAgentKind {
    fn from(value: AgentKindArg) -> Self {
        match value {
            AgentKindArg::Codex => Self::Codex,
            AgentKindArg::Gemini => Self::Gemini,
            AgentKindArg::Claude => Self::Claude,
            AgentKindArg::Grok => Self::Grok,
        }
    }
}

#[derive(Debug, Args)]
struct AgentDiscoverArgs {
    /// Restrict discovery to one vendor runtime.
    #[arg(value_enum, value_name = "KIND")]
    kind: Option<AgentKindArg>,

    /// Exact executable path. Requires KIND and never falls back to PATH if invalid.
    #[arg(long, value_name = "EXECUTABLE", requires = "kind")]
    path: Option<PathBuf>,
}

#[derive(Debug, Clone, Args)]
struct AgentRuntimeArgs {
    /// Vendor coding-agent runtime.
    #[arg(value_enum, value_name = "KIND")]
    kind: AgentKindArg,

    /// Runtime instance id, for separate accounts. Defaults to the vendor name.
    #[arg(long, value_name = "ID")]
    instance: Option<String>,

    /// Exact vendor CLI executable. Invalid paths fail instead of falling back to PATH.
    #[arg(long, value_name = "EXECUTABLE")]
    path: Option<PathBuf>,

    /// Exact, pinned ACP bridge executable for Claude; otherwise AIKit uses its pinned npx bridge.
    #[arg(long, value_name = "EXECUTABLE")]
    bridge: Option<PathBuf>,

    /// Argument passed to a custom pinned Claude ACP bridge (repeatable). Requires --bridge; @latest is rejected.
    #[arg(long = "bridge-arg", value_name = "ARG", allow_hyphen_values = true)]
    bridge_args: Vec<String>,

    /// Workspace directory used as the session cwd. Must exist.
    #[arg(long, value_name = "DIR", default_value = ".")]
    workspace: PathBuf,

    /// Managed-runtime state root. Defaults to AIKIT_HOME/runtimes or the platform user-data path.
    #[arg(long, value_name = "DIR")]
    runtime_root: Option<PathBuf>,

    /// Acknowledge that vendor account terms govern third-party Gemini CLI OAuth integrations.
    #[arg(long, env = "AIKIT_ACKNOWLEDGE_GEMINI_CLI_TERMS")]
    acknowledge_vendor_terms: bool,
}

#[derive(Debug, Args)]
struct AgentUpdateArgs {
    #[command(subcommand)]
    command: AgentUpdateCommand,
}

#[derive(Debug, Subcommand)]
enum AgentUpdateCommand {
    /// Persist a signed manifest source and update policy for one runtime.
    #[command(
        after_help = "Example (placeholder values, not a production feed):\n  aikit agents update configure gemini \\\n    --manifest-url https://updates.example.com/aikit/gemini/manifest.json \\\n    --signature-url https://updates.example.com/aikit/gemini/manifest.sig \\\n    --key-id stable-2026 \\\n    --public-key-base64 BASE64_RAW_ED25519_PUBLIC_KEY \\\n    --policy aikit-managed"
    )]
    Configure(AgentUpdateConfigureArgs),
    /// Show persisted update configuration, state, and active artifact (offline).
    #[command(
        after_help = "Examples:\n  aikit agents update status gemini\n  aikit --format json agents update status codex --root ./runtimes"
    )]
    Status(AgentUpdateTargetArgs),
    /// Run a due check and automatically apply AIKit-managed updates.
    #[command(
        after_help = "Examples:\n  aikit agents update sync codex\n  aikit agents update sync gemini --force --acknowledge-vendor-terms\n\nChecks honor a 24-hour manifest TTL and failure backoff; --force overrides both once."
    )]
    Sync(AgentUpdateSyncArgs),
    /// Atomically restore the previous AIKit-managed artifact.
    #[command(after_help = "Example:\n  aikit agents update rollback codex")]
    Rollback(AgentUpdateRollbackArgs),
}

#[derive(Debug, Clone, Args)]
struct AgentUpdateTargetArgs {
    /// Vendor coding-agent runtime.
    #[arg(value_enum, value_name = "KIND")]
    kind: AgentKindArg,

    /// Runtime instance id, for separate accounts. Defaults to the vendor name.
    #[arg(long, value_name = "ID")]
    instance: Option<String>,

    /// Managed-runtime state root. Defaults to AIKIT_HOME/runtimes or the platform user-data path.
    #[arg(long, value_name = "DIR")]
    root: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
enum AgentUpdatePolicyArg {
    /// Verify and report a compatible version; never change the install.
    CheckOnly,
    /// Report that a vendor or package-manager action is required; never invoke it.
    VendorManaged,
    /// Download, verify, stage, health-check, then atomically activate the signed artifact.
    AikitManaged,
}

impl From<AgentUpdatePolicyArg> for RuntimeUpdatePolicy {
    fn from(value: AgentUpdatePolicyArg) -> Self {
        match value {
            AgentUpdatePolicyArg::CheckOnly => Self::CheckOnly,
            AgentUpdatePolicyArg::VendorManaged => Self::VendorManaged,
            AgentUpdatePolicyArg::AikitManaged => Self::AikitManaged,
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
enum AgentUpdateChannelArg {
    /// Stable releases only (default).
    Stable,
    /// Preview releases; only when selected explicitly.
    Preview,
}

impl From<AgentUpdateChannelArg> for RuntimeUpdateChannel {
    fn from(value: AgentUpdateChannelArg) -> Self {
        match value {
            AgentUpdateChannelArg::Stable => Self::Stable,
            AgentUpdateChannelArg::Preview => Self::Preview,
        }
    }
}

#[derive(Debug, Args)]
struct AgentUpdateConfigureArgs {
    #[command(flatten)]
    target: AgentUpdateTargetArgs,

    /// Credential-free HTTPS URL (no query string) for the strict JSON compatibility manifest.
    #[arg(long, value_name = "URL")]
    manifest_url: String,

    /// Credential-free HTTPS URL (no query string) of its detached base64 Ed25519 signature.
    #[arg(long, value_name = "URL")]
    signature_url: String,

    /// Stable identifier for the trusted signing key.
    #[arg(long, value_name = "ID")]
    key_id: String,

    /// Base64-encoded raw 32-byte Ed25519 public key.
    #[arg(long, value_name = "BASE64")]
    public_key_base64: String,

    /// Update behavior. Only aikit-managed installs binary artifacts automatically.
    #[arg(long, value_enum, default_value_t = AgentUpdatePolicyArg::CheckOnly)]
    policy: AgentUpdatePolicyArg,

    /// Release channel; preview is never selected implicitly.
    #[arg(long, value_enum, default_value_t = AgentUpdateChannelArg::Stable)]
    channel: AgentUpdateChannelArg,
}

#[derive(Debug, Args)]
struct AgentUpdateSyncArgs {
    #[command(flatten)]
    target: AgentUpdateTargetArgs,

    /// Ignore the 24-hour manifest TTL and retry backoff for this explicit sync.
    #[arg(long)]
    force: bool,

    /// Workspace directory used for the staged runtime health check. Must exist.
    #[arg(long, value_name = "DIR", default_value = ".")]
    workspace: PathBuf,

    /// Exact pinned Claude ACP bridge executable for staged health checks.
    #[arg(long, value_name = "EXECUTABLE")]
    bridge: Option<PathBuf>,

    /// Argument passed to a custom pinned Claude ACP bridge (repeatable).
    #[arg(long = "bridge-arg", value_name = "ARG", allow_hyphen_values = true)]
    bridge_args: Vec<String>,

    /// Acknowledge vendor terms before health-checking an AIKit-managed Gemini runtime.
    #[arg(long, env = "AIKIT_ACKNOWLEDGE_GEMINI_CLI_TERMS")]
    acknowledge_vendor_terms: bool,
}

#[derive(Debug, Args)]
struct AgentUpdateRollbackArgs {
    #[command(flatten)]
    target: AgentUpdateTargetArgs,

    /// Workspace directory used to verify the restored runtime after switching the activation pointer.
    #[arg(long, value_name = "DIR", default_value = ".")]
    workspace: PathBuf,

    /// Exact pinned Claude ACP bridge executable for the restored runtime health check.
    #[arg(long, value_name = "EXECUTABLE")]
    bridge: Option<PathBuf>,

    /// Argument passed to a custom pinned Claude ACP bridge (repeatable).
    #[arg(long = "bridge-arg", value_name = "ARG", allow_hyphen_values = true)]
    bridge_args: Vec<String>,

    /// Acknowledge vendor terms before health-checking a restored Gemini runtime.
    #[arg(long, env = "AIKIT_ACKNOWLEDGE_GEMINI_CLI_TERMS")]
    acknowledge_vendor_terms: bool,
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
enum AgentApprovalMode {
    /// Ask on the terminal for each vendor permission request.
    Prompt,
    /// Reject every permission request.
    Deny,
    /// Accept each request once. This must be selected explicitly.
    AllowOnce,
}

#[derive(Debug, Args)]
struct AgentRunArgs {
    #[command(flatten)]
    runtime: AgentRuntimeArgs,

    /// Prompt text. Use '-' or omit it to read stdin.
    #[arg(value_name = "PROMPT", trailing_var_arg = true)]
    prompt: Vec<String>,

    /// Runtime model id. ACP runtimes validate it against advertised session options.
    #[arg(long, value_name = "MODEL")]
    model: Option<String>,

    /// Runtime session mode. Codex supports analysis_only, workspace_write, or full_access.
    #[arg(long, value_name = "MODE")]
    mode: Option<String>,

    /// Permission behavior for vendor tool calls.
    #[arg(long, value_enum, default_value_t = AgentApprovalMode::Prompt)]
    approval: AgentApprovalMode,
}

#[derive(Debug, Args)]
struct DoctorArgs {
    /// Workspace directory used for filesystem jail and containment probing. Must exist.
    #[arg(long, value_name = "DIR", default_value = ".")]
    workspace: PathBuf,
}

#[derive(Debug, Args)]
struct EvalArgs {
    /// JSON dataset containing prompts and deterministic gates.
    #[arg(value_name = "DATASET")]
    dataset: PathBuf,

    /// Permit non-mock models. Without this flag evaluation is keyless and cannot spend money.
    #[arg(long)]
    allow_live: bool,

    /// Maximum number of paid/network cases permitted in one invocation (1-256).
    #[arg(long, value_name = "N", default_value_t = DEFAULT_MAX_LIVE_CASES, value_parser = clap::value_parser!(u64).range(1..=256))]
    max_live_cases: u64,

    /// Aggregate prompt and system-instruction byte ceiling across all live cases.
    #[arg(long, value_name = "BYTES", default_value_t = DEFAULT_MAX_LIVE_INPUT_BYTES, value_parser = clap::value_parser!(u64).range(1..))]
    max_live_input_bytes: u64,

    /// Aggregate requested output-token ceiling across all live cases.
    #[arg(long, value_name = "TOKENS", default_value_t = DEFAULT_MAX_LIVE_OUTPUT_TOKENS, value_parser = clap::value_parser!(u64).range(1..))]
    max_live_output_tokens: u64,

    /// Hard wall-time ceiling in seconds shared by all live cases in this invocation (1-86400).
    #[arg(long, value_name = "SECONDS", default_value_t = DEFAULT_MAX_LIVE_WALL_SECONDS, value_parser = clap::value_parser!(u64).range(1..=86_400))]
    max_live_wall_seconds: u64,

    /// Register a deterministic in-process demo probe tool so governance-trajectory datasets can
    /// exercise a real tool call. `denied` additionally installs a deny rule so the call is refused
    /// before execution. The probe has no side effects (it echoes its input).
    #[arg(long, value_enum, value_name = "MODE")]
    demo_tools: Option<DemoTools>,
}

/// Keyless demo-tool wiring for governance-trajectory eval datasets. Not a general tool surface.
#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
enum DemoTools {
    /// Advertise the probe and let the call execute.
    Allowed,
    /// Advertise the probe but deny it before execution.
    Denied,
}

/// The deterministic, side-effect-free tool advertised by `--demo-tools`. The mock provider calls
/// the first advertised tool with `{"q":"merhaba"}`, so the schema must accept that input.
fn demo_probe_spec() -> ToolSpec {
    tool(
        "demo_probe",
        "Deterministic keyless eval probe; echoes its input.",
        json!({
            "type": "object",
            "properties": { "q": { "type": "string" } },
            "additionalProperties": false
        }),
    )
}

struct DemoProbe;

#[async_trait::async_trait]
impl ToolExecutor for DemoProbe {
    async fn execute(&self, name: &str, input: Value) -> aikit::Result<String> {
        Ok(format!("{name}:{input}"))
    }
}

#[derive(Debug, Args)]
struct CompletionArgs {
    /// Target shell.
    #[arg(value_enum, value_name = "SHELL")]
    shell: Shell,
}

#[derive(Debug, thiserror::Error)]
enum CliError {
    #[error("input error: {0}")]
    Input(String),
    #[error("runtime error: {0}")]
    Runtime(String),
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("serialization error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("evaluation failed: {failed_cases} of {total_cases} case(s) failed")]
    EvalFailed {
        failed_cases: usize,
        total_cases: usize,
    },
}

#[derive(Debug, Serialize)]
struct RunView<'a> {
    model: &'a str,
    text: &'a str,
    stop_reason: &'a Option<String>,
    usage: &'a aikit::Usage,
}

#[derive(Debug, Serialize)]
struct ProviderView<'a> {
    provider: &'a str,
    credential: &'a str,
    models: &'a str,
    active: bool,
}

#[derive(Debug, Serialize)]
struct DoctorCheck {
    name: String,
    status: &'static str,
    detail: String,
}

#[derive(Debug, Serialize)]
struct DoctorReport {
    ok: bool,
    version: &'static str,
    checks: Vec<DoctorCheck>,
    containment: Value,
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    match execute(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("aikit: {error}");
            ExitCode::from(exit_code(&error))
        }
    }
}

async fn execute(cli: Cli) -> Result<(), CliError> {
    match cli.command {
        Command::Run(args) => run_once(&process_agent(), args, cli.format).await,
        Command::Chat(args) => chat(&process_agent(), args, cli.format, cli.quiet).await,
        Command::Providers => {
            let agent = process_agent();
            print_providers(&agent, cli.format)?;
            if cli.format == OutputFormat::Text && !cli.quiet && agent.active_providers().is_empty()
            {
                eprintln!(
                    "hint: no provider is active; export one CREDENTIAL variable above to enable it (mock-1 always works)"
                );
            }
            Ok(())
        }
        Command::Agents(args) => run_agent_command(args, cli.format, cli.quiet).await,
        Command::Capabilities => print_value(&process_agent().capabilities(), cli.format),
        Command::Doctor(args) => doctor(&process_agent(), args, cli.format).await,
        Command::Eval(args) => run_eval(&process_agent(), args, cli.format).await,
        Command::Completions(args) => {
            generate(args.shell, &mut Cli::command(), "aikit", &mut io::stdout());
            Ok(())
        }
    }
}

async fn run_agent_command(
    args: AgentsArgs,
    format: OutputFormat,
    quiet: bool,
) -> Result<(), CliError> {
    match args.command {
        AgentCommand::Discover(args) => discover_agents(args, format),
        AgentCommand::Status(args) => {
            let runtime = open_external_runtime(&args).await?;
            let operation = async {
                runtime.initialize().await.map_err(external_runtime_error)?;
                runtime
                    .auth_status(false)
                    .await
                    .map_err(external_runtime_error)?;
                print_runtime_descriptor(&runtime.descriptor(), format)
            }
            .await;
            finish_external_runtime(runtime.as_ref(), operation).await
        }
        AgentCommand::Login(args) => {
            let runtime = open_external_runtime(&args).await?;
            let mut events = runtime.subscribe();
            let operation = async {
                let auth = runtime.login().await.map_err(external_runtime_error)?;
                drain_login_events(&mut events, format)?;
                match format {
                    OutputFormat::Text => println!("auth: {:?}", auth.state),
                    OutputFormat::Json | OutputFormat::Jsonl => print_value(&auth, format)?,
                }
                Ok(())
            }
            .await;
            finish_external_runtime(runtime.as_ref(), operation).await
        }
        AgentCommand::Logout(args) => {
            let runtime = open_external_runtime(&args).await?;
            let operation = async {
                runtime.logout().await.map_err(external_runtime_error)?;
                if !quiet && format == OutputFormat::Text {
                    println!("vendor CLI session logged out");
                } else if format != OutputFormat::Text {
                    print_value(&json!({"logged_out":true}), format)?;
                }
                Ok(())
            }
            .await;
            finish_external_runtime(runtime.as_ref(), operation).await
        }
        AgentCommand::Models(args) => {
            let runtime = open_external_runtime(&args).await?;
            let operation = async {
                let models = runtime
                    .list_models()
                    .await
                    .map_err(external_runtime_error)?;
                match format {
                    OutputFormat::Text => {
                        if models.models.is_empty() {
                            println!(
                                "no non-billable model catalog advertised yet (ACP may publish it after session/new)"
                            );
                        } else {
                            for model in &models.models {
                                println!(
                                    "{:<32} {}",
                                    model.id,
                                    model.display_name.as_deref().unwrap_or("")
                                );
                            }
                        }
                    }
                    OutputFormat::Json | OutputFormat::Jsonl => print_value(&models, format)?,
                }
                Ok(())
            }
            .await;
            finish_external_runtime(runtime.as_ref(), operation).await
        }
        AgentCommand::Run(args) => run_external_agent(args, format).await,
        AgentCommand::Update(args) => run_agent_update_command(args, format, quiet).await,
    }
}

async fn finish_external_runtime(
    runtime: &dyn ExternalAgentRuntime,
    operation: Result<(), CliError>,
) -> Result<(), CliError> {
    let shutdown = runtime.shutdown().await.map_err(external_runtime_error);
    operation.and(shutdown)
}

fn discover_agents(args: AgentDiscoverArgs, format: OutputFormat) -> Result<(), CliError> {
    let kinds = args
        .kind
        .map(|kind| vec![ExternalAgentKind::from(kind)])
        .unwrap_or_else(|| ExternalAgentKind::ALL.to_vec());
    let mut candidates = Vec::new();
    for kind in kinds {
        candidates.extend(
            discover_runtime_candidates(kind, args.path.as_deref())
                .map_err(external_runtime_error)?,
        );
    }
    match format {
        OutputFormat::Text => {
            println!("AGENT   ORIGIN              EXECUTABLE");
            for candidate in candidates {
                println!(
                    "{:<7} {:<19?} {}",
                    candidate.kind,
                    candidate.origin,
                    candidate.executable.display()
                );
            }
            Ok(())
        }
        OutputFormat::Json | OutputFormat::Jsonl => print_value(&candidates, format),
    }
}

async fn run_agent_update_command(
    args: AgentUpdateArgs,
    format: OutputFormat,
    quiet: bool,
) -> Result<(), CliError> {
    match args.command {
        AgentUpdateCommand::Configure(args) => configure_agent_update(args, format),
        AgentUpdateCommand::Status(args) => print_agent_update_status(&args, format),
        AgentUpdateCommand::Sync(args) => sync_agent_update(args, format, quiet).await,
        AgentUpdateCommand::Rollback(args) => rollback_agent_update(args, format).await,
    }
}

fn configure_agent_update(
    args: AgentUpdateConfigureArgs,
    format: OutputFormat,
) -> Result<(), CliError> {
    let kind = ExternalAgentKind::from(args.target.kind);
    let instance_id = update_instance_id(&args.target, kind)?;
    let store = update_store(args.target.root.as_deref())?;
    let _lock = store.try_update_lock().map_err(external_runtime_error)?;
    let policy = RuntimeUpdatePolicy::from(args.policy);
    let profile = if policy == RuntimeUpdatePolicy::AikitManaged {
        RuntimeProfile::Managed
    } else {
        RuntimeProfile::External
    };
    let channel = RuntimeUpdateChannel::from(args.channel);
    let config = RuntimeUpdateConfig {
        schema_version: aikit::EXTERNAL_AGENT_CONTRACT_VERSION,
        instance_id: instance_id.clone(),
        kind,
        profile,
        policy,
        channel,
        source: RuntimeUpdateSource {
            manifest_url: args.manifest_url,
            signature_url: args.signature_url,
            key_id: args.key_id,
            public_key_base64: args.public_key_base64,
        },
    };
    config.validate().map_err(external_runtime_error)?;
    store
        .write_update_config(&config)
        .map_err(external_runtime_error)?;

    let mut state = aikit::RuntimeUpdateState::new(instance_id, profile);
    state.policy = policy;
    state.channel = channel;
    if let Some(activation) = store
        .read_activation(&config.instance_id)
        .map_err(external_runtime_error)?
    {
        state.current_version = Some(activation.active.version);
        state.phase = RuntimeUpdatePhase::Current;
    }
    store
        .write_update_state(&state)
        .map_err(external_runtime_error)?;
    print_update_snapshot(&store, &config.instance_id, "configured", format)
}

fn print_agent_update_status(
    target: &AgentUpdateTargetArgs,
    format: OutputFormat,
) -> Result<(), CliError> {
    let kind = ExternalAgentKind::from(target.kind);
    let instance_id = update_instance_id(target, kind)?;
    let store = update_store(target.root.as_deref())?;
    print_update_snapshot(&store, &instance_id, "status", format)
}

async fn sync_agent_update(
    args: AgentUpdateSyncArgs,
    format: OutputFormat,
    quiet: bool,
) -> Result<(), CliError> {
    let kind = ExternalAgentKind::from(args.target.kind);
    let instance_id = update_instance_id(&args.target, kind)?;
    let store = update_store(args.target.root.as_deref())?;
    let _lock = store.try_update_lock().map_err(external_runtime_error)?;
    let config = store
        .read_update_config(&instance_id)
        .map_err(external_runtime_error)?
        .ok_or_else(|| {
            CliError::Input(format!(
                "no update source configured for {instance_id}; run `aikit agents update configure {kind} ...`"
            ))
        })?;
    if config.kind != kind {
        return Err(CliError::Input(format!(
            "instance {instance_id} is configured for {}, not {kind}; pass the matching KIND or a different --instance",
            config.kind
        )));
    }
    let now = unix_millis();
    let activation = store
        .read_activation(&instance_id)
        .map_err(external_runtime_error)?;
    let mut state = store
        .read_update_state(&instance_id)
        .map_err(external_runtime_error)?
        .unwrap_or_else(|| aikit::RuntimeUpdateState::new(instance_id.clone(), config.profile));
    state.profile = config.profile;
    state.policy = config.policy;
    state.channel = config.channel;
    if let Some(active) = &activation {
        state.current_version = Some(active.active.version.clone());
    } else if state.current_version.is_none() && config.profile == RuntimeProfile::External {
        state.current_version = probe_external_runtime_version(
            kind,
            Some(instance_id.to_string()),
            args.target.root.clone(),
            &args.workspace,
            args.bridge.as_deref(),
            &args.bridge_args,
            args.acknowledge_vendor_terms,
        )
        .await;
    }

    let recovered_activation = match &activation {
        Some(active) => state
            .recover_activated(&active.active.version)
            .map_err(external_runtime_error)?,
        None => false,
    };
    if recovered_activation {
        store
            .write_update_state(&state)
            .map_err(external_runtime_error)?;
        return print_update_snapshot(&store, &instance_id, "activation_recovered", format);
    }

    let interrupted = matches!(
        state.phase,
        RuntimeUpdatePhase::Checking
            | RuntimeUpdatePhase::Downloading
            | RuntimeUpdatePhase::Staged
            | RuntimeUpdatePhase::Activating
    );
    if !args.force && !interrupted && !state.manifest_check_due(now) {
        store
            .write_update_state(&state)
            .map_err(external_runtime_error)?;
        if !quiet && format == OutputFormat::Text {
            println!("update check is not due; use --force for an explicit refresh");
        }
        return print_update_snapshot(&store, &instance_id, "not_due", format);
    }

    state.phase = RuntimeUpdatePhase::Checking;
    store
        .write_update_state(&state)
        .map_err(external_runtime_error)?;
    let client = RuntimeUpdateClient::new().map_err(external_runtime_error)?;
    let manifest = match client.fetch_manifest(&config.source, now).await {
        Ok(manifest) => manifest,
        Err(error) => {
            state.record_failure(now, error.to_string());
            store
                .write_update_state(&state)
                .map_err(external_runtime_error)?;
            return Err(external_runtime_error(error));
        }
    };
    let aikit_version = Version::parse(env!("CARGO_PKG_VERSION"))
        .map_err(|error| CliError::Runtime(format!("invalid AIKit package version: {error}")))?;
    let selection = match select_runtime_update(
        &manifest,
        kind,
        config.channel,
        state.current_version.as_ref(),
        &aikit_version,
        std::env::consts::OS,
        std::env::consts::ARCH,
    ) {
        Ok(selection) => selection,
        Err(error) => {
            state.record_failure(now, error.to_string());
            store
                .write_update_state(&state)
                .map_err(external_runtime_error)?;
            return Err(external_runtime_error(error));
        }
    };
    state.record_check_success(now, selection.as_ref());
    store
        .write_update_state(&state)
        .map_err(external_runtime_error)?;
    let Some(selection) = selection else {
        return print_update_snapshot(&store, &instance_id, "current", format);
    };

    if state.quarantined_version.as_ref() == Some(&selection.version) {
        state.phase = RuntimeUpdatePhase::RolledBack;
        store
            .write_update_state(&state)
            .map_err(external_runtime_error)?;
        return print_update_snapshot(&store, &instance_id, "quarantined", format);
    }

    if config.policy != RuntimeUpdatePolicy::AikitManaged {
        let outcome = if config.policy == RuntimeUpdatePolicy::VendorManaged {
            "vendor_action_required"
        } else {
            "available"
        };
        return print_update_snapshot(&store, &instance_id, outcome, format);
    }

    state.begin_download().map_err(external_runtime_error)?;
    store
        .write_update_state(&state)
        .map_err(external_runtime_error)?;
    let installed = match client.download_and_stage(&store, &selection).await {
        Ok(installed) => installed,
        Err(error) => {
            state.record_failure(now, error.to_string());
            store
                .write_update_state(&state)
                .map_err(external_runtime_error)?;
            return Err(external_runtime_error(error));
        }
    };
    state.record_staged().map_err(external_runtime_error)?;
    store
        .write_update_state(&state)
        .map_err(external_runtime_error)?;
    let launch = store
        .installed_launch(&instance_id, kind, &installed)
        .map_err(external_runtime_error)?;
    if let Err(error) = verify_managed_launch(
        launch,
        &args.workspace,
        args.bridge.as_deref(),
        &args.bridge_args,
        args.acknowledge_vendor_terms,
    )
    .await
    {
        state.quarantine_available();
        state.record_failure(now, error.to_string());
        store
            .write_update_state(&state)
            .map_err(external_runtime_error)?;
        return Err(error);
    }
    state.begin_activation().map_err(external_runtime_error)?;
    store
        .write_update_state(&state)
        .map_err(external_runtime_error)?;
    store
        .activate(&instance_id, installed, now)
        .map_err(external_runtime_error)?;
    state.record_activated().map_err(external_runtime_error)?;
    store
        .write_update_state(&state)
        .map_err(external_runtime_error)?;
    print_update_snapshot(&store, &instance_id, "activated", format)
}

async fn rollback_agent_update(
    args: AgentUpdateRollbackArgs,
    format: OutputFormat,
) -> Result<(), CliError> {
    let kind = ExternalAgentKind::from(args.target.kind);
    let instance_id = update_instance_id(&args.target, kind)?;
    let store = update_store(args.target.root.as_deref())?;
    let _lock = store.try_update_lock().map_err(external_runtime_error)?;
    let config = store
        .read_update_config(&instance_id)
        .map_err(external_runtime_error)?
        .ok_or_else(|| {
            CliError::Input(format!(
                "no update source configured for {instance_id}; run `aikit agents update configure {kind} ... --policy aikit-managed` first"
            ))
        })?;
    if config.kind != kind || config.policy != RuntimeUpdatePolicy::AikitManaged {
        return Err(CliError::Input(format!(
            "rollback is available only for the matching AIKit-managed runtime; `aikit agents update status {kind}` shows the configured kind and policy"
        )));
    }
    let activation = store
        .rollback(&instance_id, unix_millis())
        .map_err(external_runtime_error)?;
    let launch = store
        .active_launch(&instance_id, kind)
        .map_err(external_runtime_error)?;
    let mut state = store
        .read_update_state(&instance_id)
        .map_err(external_runtime_error)?
        .unwrap_or_else(|| aikit::RuntimeUpdateState::new(instance_id.clone(), config.profile));
    state.record_rollback(
        activation.active.version,
        activation.previous.map(|artifact| artifact.version),
    );
    if let Err(error) = verify_managed_launch(
        launch,
        &args.workspace,
        args.bridge.as_deref(),
        &args.bridge_args,
        args.acknowledge_vendor_terms,
    )
    .await
    {
        state.record_failure(unix_millis(), error.to_string());
        store
            .write_update_state(&state)
            .map_err(external_runtime_error)?;
        return Err(error);
    }
    store
        .write_update_state(&state)
        .map_err(external_runtime_error)?;
    print_update_snapshot(&store, &instance_id, "rolled_back", format)
}

fn update_instance_id(
    target: &AgentUpdateTargetArgs,
    kind: ExternalAgentKind,
) -> Result<RuntimeInstanceId, CliError> {
    target
        .instance
        .as_ref()
        .map_or_else(
            || Ok(RuntimeInstanceId::default_for(kind)),
            |value| RuntimeInstanceId::new(value.clone()),
        )
        .map_err(external_runtime_error)
}

fn update_store(explicit: Option<&Path>) -> Result<ManagedRuntimeArtifactStore, CliError> {
    ManagedRuntimeArtifactStore::new(runtime_root(explicit)?).map_err(external_runtime_error)
}

fn runtime_root(explicit: Option<&Path>) -> Result<PathBuf, CliError> {
    if let Some(path) = explicit {
        return absolute_path(path);
    }
    if let Some(home) = std::env::var_os("AIKIT_HOME") {
        return absolute_path(&PathBuf::from(home)).map(|path| path.join("runtimes"));
    }
    #[cfg(target_os = "windows")]
    if let Some(local) = std::env::var_os("LOCALAPPDATA") {
        return absolute_path(&PathBuf::from(local)).map(|path| path.join("aikit/runtimes"));
    }
    #[cfg(target_os = "macos")]
    if let Some(home) = std::env::var_os("HOME") {
        return absolute_path(&PathBuf::from(home))
            .map(|path| path.join("Library/Application Support/aikit/runtimes"));
    }
    #[cfg(all(unix, not(target_os = "macos")))]
    {
        if let Some(data) = std::env::var_os("XDG_DATA_HOME") {
            return absolute_path(&PathBuf::from(data)).map(|path| path.join("aikit/runtimes"));
        }
        if let Some(home) = std::env::var_os("HOME") {
            return absolute_path(&PathBuf::from(home))
                .map(|path| path.join(".local/share/aikit/runtimes"));
        }
    }
    Err(CliError::Input(
        "cannot determine runtime state root; set AIKIT_HOME or pass --root".into(),
    ))
}

fn absolute_path(path: &Path) -> Result<PathBuf, CliError> {
    if path.as_os_str().is_empty() {
        return Err(CliError::Input("runtime state root cannot be empty".into()));
    }
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(std::env::current_dir()?.join(path))
    }
}

fn print_update_snapshot(
    store: &ManagedRuntimeArtifactStore,
    instance_id: &RuntimeInstanceId,
    outcome: &str,
    format: OutputFormat,
) -> Result<(), CliError> {
    let config = store
        .read_update_config(instance_id)
        .map_err(external_runtime_error)?;
    let state = store
        .read_update_state(instance_id)
        .map_err(external_runtime_error)?;
    let activation = store
        .read_activation(instance_id)
        .map_err(external_runtime_error)?;
    match format {
        OutputFormat::Text => {
            println!("outcome:   {outcome}");
            println!("instance:  {instance_id}");
            println!("root:      {}", store.root().display());
            if let Some(config) = &config {
                println!("agent:     {}", config.kind);
                println!("policy:    {:?}", config.policy);
                println!("channel:   {:?}", config.channel);
            } else {
                println!("configured: no");
            }
            if let Some(state) = &state {
                println!("phase:     {:?}", state.phase);
                let current = state
                    .current_version
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_else(|| "unknown".into());
                let available = state
                    .available_version
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_else(|| "none".into());
                println!("current:   {current}");
                println!("available: {available}");
            }
            if let Some(activation) = &activation {
                println!("executable: {}", activation.active.executable.display());
                println!("sha256:     {}", activation.active.sha256);
            }
            Ok(())
        }
        OutputFormat::Json | OutputFormat::Jsonl => print_value(
            &json!({
                "outcome": outcome,
                "root": store.root(),
                "config": config,
                "state": state,
                "activation": activation,
            }),
            format,
        ),
    }
}

async fn probe_external_runtime_version(
    kind: ExternalAgentKind,
    instance: Option<String>,
    runtime_root: Option<PathBuf>,
    workspace: &Path,
    bridge: Option<&Path>,
    bridge_args: &[String],
    acknowledge_vendor_terms: bool,
) -> Option<Version> {
    let args = AgentRuntimeArgs {
        kind: match kind {
            ExternalAgentKind::Codex => AgentKindArg::Codex,
            ExternalAgentKind::Gemini => AgentKindArg::Gemini,
            ExternalAgentKind::Claude => AgentKindArg::Claude,
            ExternalAgentKind::Grok => AgentKindArg::Grok,
        },
        instance,
        path: None,
        bridge: bridge.map(Path::to_path_buf),
        bridge_args: bridge_args.to_vec(),
        workspace: workspace.to_path_buf(),
        runtime_root,
        acknowledge_vendor_terms,
    };
    let runtime = open_external_runtime(&args).await.ok()?;
    let operation = tokio::time::timeout(Duration::from_secs(20), runtime.initialize()).await;
    let version = match operation {
        Ok(Ok(_)) => parse_runtime_version(runtime.descriptor().version.as_deref()),
        _ => None,
    };
    let _ = runtime.shutdown().await;
    version
}

fn parse_runtime_version(value: Option<&str>) -> Option<Version> {
    value?.split_whitespace().find_map(|part| {
        Version::parse(part.trim_start_matches('v').trim_matches(|char: char| {
            !char.is_ascii_alphanumeric() && char != '.' && char != '-' && char != '+'
        }))
        .ok()
    })
}

async fn open_external_runtime(
    args: &AgentRuntimeArgs,
) -> Result<Box<dyn ExternalAgentRuntime>, CliError> {
    let kind = ExternalAgentKind::from(args.kind);
    ensure_vendor_terms_acknowledged(kind, args.acknowledge_vendor_terms)?;
    let workspace = canonical_workspace(&args.workspace)?;
    let instance_id = args
        .instance
        .as_ref()
        .map_or_else(
            || Ok(RuntimeInstanceId::default_for(kind)),
            |value| RuntimeInstanceId::new(value.clone()),
        )
        .map_err(external_runtime_error)?;

    if args.path.is_none() {
        let root = match args.runtime_root.as_deref() {
            Some(explicit) => Some(runtime_root(Some(explicit))?),
            None => runtime_root(None).ok(),
        };
        if let Some(root) = root.filter(|root| root.is_dir()) {
            let store = ManagedRuntimeArtifactStore::new(root).map_err(external_runtime_error)?;
            let config = store
                .read_update_config(&instance_id)
                .map_err(external_runtime_error)?;
            if config.as_ref().is_some_and(|config| {
                config.kind == kind && config.policy == RuntimeUpdatePolicy::AikitManaged
            }) && store
                .read_activation(&instance_id)
                .map_err(external_runtime_error)?
                .is_some()
            {
                let launch = store
                    .active_launch(&instance_id, kind)
                    .map_err(external_runtime_error)?;
                return spawn_external_runtime(
                    launch.instance_id,
                    launch.candidate,
                    RuntimeProfile::Managed,
                    Some(launch.artifact_hash),
                    &workspace,
                    args.bridge.as_deref(),
                    &args.bridge_args,
                )
                .await;
            }
        }
    }

    let candidate = discover_runtime_candidates(kind, args.path.as_deref())
        .map_err(external_runtime_error)?
        .into_iter()
        .next()
        .ok_or_else(|| {
            CliError::Input(format!(
                "no {} CLI executable found; install it, pass --path <EXECUTABLE>, or run `aikit agents discover` to see candidates",
                kind.command_name()
            ))
        })?;
    spawn_external_runtime(
        instance_id,
        candidate,
        RuntimeProfile::External,
        None,
        &workspace,
        args.bridge.as_deref(),
        &args.bridge_args,
    )
    .await
}

async fn spawn_external_runtime(
    instance_id: RuntimeInstanceId,
    candidate: aikit::RuntimeCandidate,
    profile: RuntimeProfile,
    artifact_hash: Option<String>,
    workspace: &Path,
    bridge: Option<&Path>,
    bridge_args: &[String],
) -> Result<Box<dyn ExternalAgentRuntime>, CliError> {
    let kind = candidate.kind;
    match kind {
        ExternalAgentKind::Codex => {
            if bridge.is_some() || !bridge_args.is_empty() {
                return Err(CliError::Input(
                    "--bridge and --bridge-arg apply only to Claude".into(),
                ));
            }
            let runtime = CodexAppServerRuntime::spawn(CodexAppServerConfig {
                instance_id,
                candidate,
                profile,
                environment: vendor_session_environment(ExternalAgentKind::Codex),
                inherit_env: true,
                artifact_hash,
            })
            .await
            .map_err(external_runtime_error)?;
            Ok(Box::new(runtime))
        }
        ExternalAgentKind::Gemini | ExternalAgentKind::Grok => {
            if bridge.is_some() || !bridge_args.is_empty() {
                return Err(CliError::Input(
                    "--bridge and --bridge-arg apply only to Claude".into(),
                ));
            }
            let mut config = AcpRuntimeConfig::native(instance_id, candidate, profile)
                .map_err(external_runtime_error)?;
            config.default_workspace = Some(workspace.to_path_buf());
            config.artifact_hash = artifact_hash;
            let runtime = AcpExternalRuntime::spawn(config)
                .await
                .map_err(external_runtime_error)?;
            Ok(Box::new(runtime))
        }
        ExternalAgentKind::Claude => {
            let mut config = if let Some(bridge) = bridge {
                let bridge = bridge.canonicalize().map_err(|error| {
                    CliError::Input(format!("Claude ACP bridge {}: {error}", bridge.display()))
                })?;
                AcpRuntimeConfig::pinned_bridge(
                    instance_id,
                    candidate,
                    profile,
                    bridge,
                    bridge_args.to_vec(),
                )
                .map_err(external_runtime_error)?
            } else {
                if !bridge_args.is_empty() {
                    return Err(CliError::Input(
                        "--bridge-arg requires an explicit --bridge".into(),
                    ));
                }
                let npx = discover_support_executable("npx").map_err(external_runtime_error)?;
                AcpRuntimeConfig::pinned_claude_npx_bridge(instance_id, candidate, profile, npx)
                    .map_err(external_runtime_error)?
            };
            config.default_workspace = Some(workspace.to_path_buf());
            config.artifact_hash = artifact_hash;
            let runtime = AcpExternalRuntime::spawn(config)
                .await
                .map_err(external_runtime_error)?;
            Ok(Box::new(runtime))
        }
    }
}

async fn verify_managed_launch(
    launch: aikit::ManagedRuntimeLaunch,
    workspace: &Path,
    bridge: Option<&Path>,
    bridge_args: &[String],
    acknowledge_vendor_terms: bool,
) -> Result<(), CliError> {
    ensure_vendor_terms_acknowledged(launch.kind, acknowledge_vendor_terms)?;
    let workspace = canonical_workspace(workspace)?;
    let runtime = spawn_external_runtime(
        launch.instance_id,
        launch.candidate,
        RuntimeProfile::Managed,
        Some(launch.artifact_hash),
        &workspace,
        bridge,
        bridge_args,
    )
    .await?;
    let operation = match tokio::time::timeout(Duration::from_secs(60), async {
        runtime.initialize().await.map_err(external_runtime_error)?;
        runtime
            .auth_status(false)
            .await
            .map_err(external_runtime_error)?;
        Ok(())
    })
    .await
    {
        Ok(result) => result,
        Err(_) => Err(CliError::Runtime(
            "staged managed runtime health check timed out after 60 seconds".into(),
        )),
    };
    finish_external_runtime(runtime.as_ref(), operation).await
}

fn ensure_vendor_terms_acknowledged(
    kind: ExternalAgentKind,
    acknowledged: bool,
) -> Result<(), CliError> {
    if kind == ExternalAgentKind::Gemini && !acknowledged {
        return Err(CliError::Input(
            "Gemini CLI OAuth use is governed by Google's account terms; pass --acknowledge-vendor-terms (or set AIKIT_ACKNOWLEDGE_GEMINI_CLI_TERMS=true), or use the normal Google provider with an API key"
                .into(),
        ));
    }
    Ok(())
}

fn print_runtime_descriptor(
    descriptor: &aikit::RuntimeDescriptor,
    format: OutputFormat,
) -> Result<(), CliError> {
    match format {
        OutputFormat::Text => {
            println!("agent:     {}", descriptor.kind);
            println!("health:    {:?}", descriptor.health);
            println!("auth:      {:?}", descriptor.auth.state);
            println!("protocol:  {}", descriptor.protocol);
            println!(
                "version:   {}",
                descriptor.version.as_deref().unwrap_or("unknown")
            );
            println!("executable: {}", descriptor.real_executable.display());
            Ok(())
        }
        OutputFormat::Json | OutputFormat::Jsonl => print_value(descriptor, format),
    }
}

fn drain_login_events(
    events: &mut tokio::sync::broadcast::Receiver<RuntimeEvent>,
    format: OutputFormat,
) -> Result<(), CliError> {
    loop {
        match events.try_recv() {
            Ok(event) => match (&event.event, format) {
                (RuntimeEventKind::LoginStarted { url, .. }, OutputFormat::Text) => {
                    if let Some(url) = url {
                        println!("open: {url}");
                    } else {
                        println!("complete login in the vendor flow");
                    }
                }
                (_, OutputFormat::Jsonl) => print_value(&event, format)?,
                _ => {}
            },
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
            | Err(tokio::sync::broadcast::error::TryRecvError::Closed) => return Ok(()),
            Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_)) => {
                return Err(CliError::Runtime(
                    "external-agent login event queue overflowed".into(),
                ));
            }
        }
    }
}

async fn run_external_agent(args: AgentRunArgs, format: OutputFormat) -> Result<(), CliError> {
    if format == OutputFormat::Json {
        return Err(CliError::Input(
            "agents run emits multiple events; use --format text or --format jsonl".into(),
        ));
    }
    if args.approval == AgentApprovalMode::Prompt && !io::stdin().is_terminal() {
        return Err(CliError::Input(
            "--approval prompt requires a terminal; use deny or allow-once for automation".into(),
        ));
    }
    let prompt_text = read_prompt(args.prompt)?;
    let workspace = canonical_workspace(&args.runtime.workspace)?;
    let runtime = open_external_runtime(&args.runtime).await?;
    let mut events = runtime.subscribe();
    let operation = async {
        runtime
            .initialize()
            .await
            .map_err(external_runtime_error)?;
        let session = runtime
            .new_session(NewRuntimeSession {
                cwd: workspace,
                additional_directories: Vec::new(),
                model: args.model,
                mode: args.mode,
            })
            .await
            .map_err(external_runtime_error)?;
        runtime
            .prompt(ExternalPrompt {
                session_id: session.session_id.clone(),
                prompt_id: format!("cli-{}", unix_millis()),
                blocks: vec![ExternalPromptBlock::Text { text: prompt_text }],
            })
            .await
            .map_err(external_runtime_error)?;

        let interrupt = tokio::signal::ctrl_c();
        tokio::pin!(interrupt);
        loop {
            tokio::select! {
                interrupt_result = &mut interrupt => {
                    interrupt_result.map_err(CliError::Io)?;
                    runtime.cancel(&session.session_id, None).await.map_err(external_runtime_error)?;
                    if format == OutputFormat::Text {
                        eprintln!("\nexternal-agent run cancelled");
                    }
                    return Ok(());
                }
                event = events.recv() => {
                    let event = match event {
                        Ok(event) => event,
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                            runtime.cancel(&session.session_id, None).await.map_err(external_runtime_error)?;
                            return Err(CliError::Runtime("external-agent event queue overflowed; run cancelled fail-closed".into()));
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            return Err(CliError::Runtime("external-agent event channel closed before completion".into()));
                        }
                    };
                    if format == OutputFormat::Jsonl {
                        print_value(&event, format)?;
                    }
                    match &event.event {
                        RuntimeEventKind::ContentDelta { stream, text }
                            if format == OutputFormat::Text && stream.contains("assistant") =>
                        {
                            print!("{text}");
                            io::stdout().flush()?;
                        }
                        RuntimeEventKind::ApprovalRequested { request_id, detail } => {
                            let decision = approval_decision(args.approval, detail.clone()).await?;
                            runtime.respond_approval(
                                event.session_id.as_deref().unwrap_or(&session.session_id),
                                request_id,
                                decision,
                            ).await.map_err(external_runtime_error)?;
                        }
                        RuntimeEventKind::UserInputRequested { request_id, detail } => {
                            if args.approval != AgentApprovalMode::Prompt {
                                runtime.cancel(&session.session_id, None).await.map_err(external_runtime_error)?;
                                return Err(CliError::Runtime("vendor requested user input in non-interactive mode; run cancelled".into()));
                            }
                            let answers = user_input_answers(detail.clone()).await?;
                            runtime.respond_user_input(
                                event.session_id.as_deref().unwrap_or(&session.session_id),
                                request_id,
                                answers,
                            ).await.map_err(external_runtime_error)?;
                        }
                        RuntimeEventKind::Warning { message } if format == OutputFormat::Text => {
                            eprintln!("\nwarning: {message}");
                        }
                        RuntimeEventKind::Completed => {
                            if format == OutputFormat::Text { println!(); }
                            return Ok(());
                        }
                        RuntimeEventKind::Cancelled => return Ok(()),
                        RuntimeEventKind::Failed { message } => {
                            return Err(CliError::Runtime(message.clone()));
                        }
                        _ => {}
                    }
                }
            }
        }
    }
    .await;
    let shutdown = runtime.shutdown().await.map_err(external_runtime_error);
    operation.and(shutdown)
}

async fn approval_decision(
    mode: AgentApprovalMode,
    detail: Value,
) -> Result<ExternalApprovalDecision, CliError> {
    match mode {
        AgentApprovalMode::Deny => Ok(ExternalApprovalDecision::Decline),
        AgentApprovalMode::AllowOnce => Ok(ExternalApprovalDecision::Accept),
        AgentApprovalMode::Prompt => tokio::task::spawn_blocking(move || {
            eprintln!(
                "\npermission requested:\n{}",
                serde_json::to_string_pretty(&detail).unwrap_or_else(|_| "{}".into())
            );
            eprint!("allow once [y], allow for session [a], deny [n], cancel [c]? ");
            io::stderr().flush()?;
            let mut answer = String::new();
            io::stdin().read_line(&mut answer)?;
            match answer.trim().to_ascii_lowercase().as_str() {
                "y" | "yes" => Ok(ExternalApprovalDecision::Accept),
                "a" | "always" => Ok(ExternalApprovalDecision::AcceptForSession),
                "n" | "no" | "" => Ok(ExternalApprovalDecision::Decline),
                "c" | "cancel" => Ok(ExternalApprovalDecision::Cancel),
                other => Err(CliError::Input(format!(
                    "unknown approval response '{other}'; expected y, a, n, or c"
                ))),
            }
        })
        .await
        .map_err(|error| CliError::Runtime(format!("approval prompt task failed: {error}")))?,
    }
}

async fn user_input_answers(detail: Value) -> Result<BTreeMap<String, String>, CliError> {
    let questions = detail
        .get("questions")
        .and_then(Value::as_array)
        .ok_or_else(|| CliError::Runtime("vendor user-input request omitted questions".into()))?
        .iter()
        .map(|question| {
            let id = question
                .get("id")
                .or_else(|| question.get("questionId"))
                .and_then(Value::as_str)
                .ok_or_else(|| CliError::Runtime("vendor question omitted id".into()))?;
            let prompt = question
                .get("prompt")
                .or_else(|| question.get("question"))
                .and_then(Value::as_str)
                .unwrap_or(id);
            Ok((id.to_string(), prompt.to_string()))
        })
        .collect::<Result<Vec<_>, CliError>>()?;
    tokio::task::spawn_blocking(move || {
        let mut answers = BTreeMap::new();
        for (id, prompt) in questions {
            eprint!("{prompt}: ");
            io::stderr().flush()?;
            let mut answer = String::new();
            io::stdin().read_line(&mut answer)?;
            answers.insert(id, answer.trim_end().to_string());
        }
        Ok(answers)
    })
    .await
    .map_err(|error| CliError::Runtime(format!("user-input prompt task failed: {error}")))?
}

/// Convert a provider-path failure into a runtime error that also tells the operator what to do
/// next. The message keeps the core's original wording; the hint is appended on its own line.
fn agent_error(error: &aikit::AgentError, model: &str) -> CliError {
    let hint = agent_error_hint(error, model);
    CliError::Runtime(match hint {
        Some(hint) => format!("{error}\nhint: {hint}"),
        None => error.to_string(),
    })
}

fn agent_error_hint(error: &aikit::AgentError, model: &str) -> Option<String> {
    match error {
        aikit::AgentError::NoCredential(provider) => Some(match provider_credential(provider) {
            Some(variable) => format!(
                "set {variable} in the environment (its value is never printed), then retry; `aikit providers` shows which keys are active, and --model mock-1 runs keyless"
            ),
            None => "run `aikit providers` to see which keys are active; --model mock-1 runs keyless".into(),
        }),
        aikit::AgentError::UnknownModel(_) => Some(format!(
            "the model id prefix selects the provider (claude-*, gpt-*, o1-*/o3-*/o4-*, gemini-*, deepseek-*, grok-*, xai:*, openrouter:*, groq:*, mistral:*); '{model}' matches none. See `aikit run --help`, or use --model mock-1"
        )),
        aikit::AgentError::NoAdapter(_) => {
            Some("choose a model from another provider; `aikit providers` lists them".into())
        }
        _ => {
            let info = error.info();
            let variable = info.provider.as_deref().and_then(provider_credential);
            match info.code {
                ErrorCode::ProviderAuth => Some(match variable {
                    Some(variable) => format!(
                        "the provider rejected the credential; check that {variable} holds a valid, unexpired key"
                    ),
                    None => "the provider rejected the credential; check the configured API key".into(),
                }),
                ErrorCode::ProviderRateLimit => Some(match info.retry_after_ms {
                    Some(ms) => format!("retry after about {} second(s), or lower --max-tokens", ms.div_ceil(1000)),
                    None => "wait and retry, or lower --max-tokens".into(),
                }),
                ErrorCode::ProviderTimeout | ErrorCode::ProviderTransport => {
                    Some("check network connectivity and any proxy settings, then retry".into())
                }
                ErrorCode::ProviderInvalidRequest => Some(format!(
                    "check that '{model}' is a model id your account can use and that --max-tokens is within its limit"
                )),
                _ => None,
            }
        }
    }
}

/// Conventional credential variable(s) for a provider name, from the same table `providers` prints.
fn provider_credential(provider: &str) -> Option<&'static str> {
    PROVIDERS
        .iter()
        .find(|(name, _, _)| *name == provider)
        .map(|(_, credential, _)| *credential)
}

/// Canonicalize a workspace directory with an actionable error.
fn canonical_workspace(workspace: &Path) -> Result<PathBuf, CliError> {
    workspace.canonicalize().map_err(|error| {
        CliError::Input(format!(
            "workspace {}: {error}; pass --workspace with an existing directory",
            workspace.display()
        ))
    })
}

fn external_runtime_error(error: impl std::fmt::Display) -> CliError {
    CliError::Runtime(error.to_string())
}

fn unix_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}

async fn run_eval(agent: &Agent, args: EvalArgs, format: OutputFormat) -> Result<(), CliError> {
    let loaded = load_eval_dataset(&args.dataset)?;
    let dataset = loaded.dataset;
    dataset
        .validate()
        .map_err(|error| CliError::Input(error.to_string()))?;

    let live_cases = dataset
        .cases
        .iter()
        .filter(|case| is_live_model(dataset.resolved_model(case)))
        .collect::<Vec<_>>();
    if !args.allow_live && !live_cases.is_empty() {
        let live_models = live_cases
            .iter()
            .map(|case| dataset.resolved_model(case))
            .collect::<std::collections::BTreeSet<_>>();
        return Err(CliError::Input(format!(
            "dataset requests live model(s) {}; pass --allow-live to acknowledge network use and possible cost",
            live_models.into_iter().collect::<Vec<_>>().join(", ")
        )));
    }
    if u64::try_from(live_cases.len()).unwrap_or(u64::MAX) > args.max_live_cases {
        return Err(CliError::Input(format!(
            "dataset requests {} live case(s); --max-live-cases is {} (raise it to at least {} to proceed)",
            live_cases.len(),
            args.max_live_cases,
            live_cases.len()
        )));
    }
    let requested_live_input_bytes = live_cases.iter().try_fold(0_u64, |total, case| {
        let case_bytes = case
            .prompt
            .len()
            .saturating_add(case.system.as_ref().map_or(0, String::len));
        total.checked_add(u64::try_from(case_bytes).unwrap_or(u64::MAX))
    });
    let Some(requested_live_input_bytes) = requested_live_input_bytes else {
        return Err(CliError::Input(
            "aggregate live input byte count overflowed".into(),
        ));
    };
    if requested_live_input_bytes > args.max_live_input_bytes {
        return Err(CliError::Input(format!(
            "dataset requests {requested_live_input_bytes} live input byte(s); --max-live-input-bytes is {} (raise it to at least {requested_live_input_bytes} to proceed)",
            args.max_live_input_bytes
        )));
    }
    let requested_live_output_tokens = live_cases.iter().try_fold(0_u64, |total, case| {
        total.checked_add(dataset.resolved_max_tokens(case))
    });
    let Some(requested_live_output_tokens) = requested_live_output_tokens else {
        return Err(CliError::Input(
            "aggregate live output-token request overflowed".into(),
        ));
    };
    if requested_live_output_tokens > args.max_live_output_tokens {
        return Err(CliError::Input(format!(
            "dataset requests {requested_live_output_tokens} live output token(s); --max-live-output-tokens is {} (raise it to at least {requested_live_output_tokens} to proceed)",
            args.max_live_output_tokens
        )));
    }
    let live_deadline = if live_cases.is_empty() {
        None
    } else {
        Instant::now().checked_add(Duration::from_secs(args.max_live_wall_seconds))
    };
    if !live_cases.is_empty() && live_deadline.is_none() {
        return Err(CliError::Input(
            "--max-live-wall-seconds is too large".into(),
        ));
    }

    // Deliberately sequential: an accidental dataset cannot fan out paid provider calls. Hosts
    // that need controlled parallel evaluation can compose evaluate_outcome over their own runs.
    let mut reports = Vec::with_capacity(dataset.cases.len());
    let mut infrastructure_failures = 0_usize;
    for case in &dataset.cases {
        let model = dataset.resolved_model(case).to_string();
        let is_live = is_live_model(&model);
        let max_tokens = dataset.resolved_max_tokens(case);
        let case_messages = messages(case.system.clone(), case.prompt.clone());
        let recorder = RunRecorder::default();
        let mut config = RunConfig::new(&model, case_messages);
        config.max_tokens = max_tokens;
        config.recorder = recorder.clone();
        let executor: Arc<dyn ToolExecutor> = match args.demo_tools {
            Some(demo) => {
                config.tools = vec![demo_probe_spec()];
                if demo == DemoTools::Denied {
                    config.governance = Governance::new(
                        PermissionEngine::with_rules(
                            PermissionMode::Allow,
                            vec![Rule::deny("demo_probe")],
                        ),
                        Default::default(),
                    );
                }
                Arc::new(DemoProbe)
            }
            None => Arc::new(NoTools),
        };
        let cancellation = config.cancellation.clone();
        match agent.run_with_config(config, executor) {
            Ok(stream) => {
                futures::pin_mut!(stream);
                let mut stream_error = None;
                let drain = async {
                    while let Some(delta) = stream.next().await {
                        if let StreamDelta::Error { info, .. } = delta {
                            stream_error = Some(info);
                        }
                    }
                };
                let timed_out = if is_live {
                    let remaining = live_deadline
                        .expect("live cases always have a validated deadline")
                        .saturating_duration_since(Instant::now());
                    remaining.is_zero() || tokio::time::timeout(remaining, drain).await.is_err()
                } else {
                    drain.await;
                    false
                };
                if timed_out {
                    cancellation.cancel();
                    infrastructure_failures = infrastructure_failures.saturating_add(1);
                    let outcome = recorder.outcome();
                    let mut info = ErrorInfo::new(ErrorCode::ProviderTimeout);
                    info.model = Some(model.clone());
                    reports.push(EvalCaseReport::new(
                        case.name.clone(),
                        model,
                        EvalVerdict::runtime_failure(info.message.clone()),
                        Some(outcome.usage),
                        Some(outcome.terminal_status),
                        outcome.model_attempts,
                        Some(info),
                    ));
                    continue;
                }
                let outcome = recorder.outcome();
                let mut verdict = evaluate_outcome(&outcome, &case.gates)
                    .map_err(|error| CliError::Input(error.to_string()))?;
                if let Some(info) = &stream_error {
                    if is_eval_infrastructure_error(info.code) {
                        infrastructure_failures = infrastructure_failures.saturating_add(1);
                        verdict = EvalVerdict::runtime_failure(info.message.clone());
                    }
                }
                reports.push(EvalCaseReport::new(
                    case.name.clone(),
                    model,
                    verdict,
                    Some(outcome.usage),
                    Some(outcome.terminal_status),
                    outcome.model_attempts,
                    stream_error,
                ));
            }
            Err(error) => {
                infrastructure_failures = infrastructure_failures.saturating_add(1);
                let info = error.info();
                reports.push(EvalCaseReport::new(
                    case.name.clone(),
                    model,
                    EvalVerdict::runtime_failure(info.message.clone()),
                    None,
                    None,
                    Vec::new(),
                    Some(info),
                ));
            }
        }
    }

    let report = EvalReport::new(&dataset, loaded.sha256, reports);
    print_eval_report(&report, format)?;
    if infrastructure_failures > 0 {
        Err(CliError::Runtime(format!(
            "evaluation infrastructure failed for {infrastructure_failures} case(s)"
        )))
    } else if report.passed {
        Ok(())
    } else {
        Err(CliError::EvalFailed {
            failed_cases: report.total_cases - report.passed_cases,
            total_cases: report.total_cases,
        })
    }
}

struct LoadedEvalDataset {
    dataset: EvalDataset,
    sha256: String,
}

fn is_live_model(model: &str) -> bool {
    !model.to_ascii_lowercase().starts_with("mock")
}

fn is_eval_infrastructure_error(code: ErrorCode) -> bool {
    matches!(
        code,
        ErrorCode::ProviderAuth
            | ErrorCode::ProviderRateLimit
            | ErrorCode::ProviderTimeout
            | ErrorCode::ProviderTransport
            | ErrorCode::ProviderServer
            | ErrorCode::ProviderInvalidRequest
            | ErrorCode::ProviderProtocol
            | ErrorCode::Configuration
            | ErrorCode::Session
            | ErrorCode::Conflict
            | ErrorCode::Audit
            | ErrorCode::Hook
            | ErrorCode::Unknown
    )
}

fn load_eval_dataset(path: &Path) -> Result<LoadedEvalDataset, CliError> {
    // Keep I/O failures in the I/O class (exit code 1) but name the dataset path in the message.
    let with_path = |error: io::Error| {
        io::Error::new(
            error.kind(),
            format!("eval dataset {}: {error}", path.display()),
        )
    };
    let path_metadata = std::fs::symlink_metadata(path).map_err(with_path)?;
    if !path_metadata.file_type().is_file() {
        return Err(CliError::Input(
            "eval dataset must be a regular file, not a symlink or special file".into(),
        ));
    }

    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    let file = options.open(path).map_err(with_path)?;
    let metadata = file.metadata().map_err(with_path)?;
    if !metadata.file_type().is_file() {
        return Err(CliError::Input(
            "eval dataset must remain a regular file while it is read".into(),
        ));
    }
    if metadata.len() > MAX_EVAL_DATASET_BYTES as u64 {
        return Err(CliError::Input(format!(
            "eval dataset {} exceeds the {} byte limit",
            path.display(),
            MAX_EVAL_DATASET_BYTES
        )));
    }

    let mut bytes = Vec::with_capacity(
        usize::try_from(metadata.len())
            .unwrap_or(MAX_EVAL_DATASET_BYTES)
            .min(MAX_EVAL_DATASET_BYTES),
    );
    file.take((MAX_EVAL_DATASET_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(with_path)?;
    if bytes.len() > MAX_EVAL_DATASET_BYTES {
        return Err(CliError::Input(format!(
            "eval dataset exceeds the {} byte limit",
            MAX_EVAL_DATASET_BYTES
        )));
    }
    let dataset = serde_json::from_slice(&bytes).map_err(|error| {
        CliError::Input(format!(
            "invalid eval dataset JSON in {}: {error}; see docs/EVALUATIONS.md for the schema",
            path.display()
        ))
    })?;
    let digest = Sha256::digest(&bytes);
    let mut sha256 = String::with_capacity(digest.len() * 2);
    for byte in digest {
        write!(&mut sha256, "{byte:02x}").expect("writing into String cannot fail");
    }
    Ok(LoadedEvalDataset { dataset, sha256 })
}

fn print_eval_report(report: &EvalReport, format: OutputFormat) -> Result<(), CliError> {
    match format {
        OutputFormat::Text => {
            println!(
                "{}  {}  {}/{} cases",
                if report.passed { "PASS" } else { "FAIL" },
                report.dataset,
                report.passed_cases,
                report.total_cases
            );
            for case in &report.cases {
                println!(
                    "{}  {} ({}/{})",
                    if case.passed() { "PASS" } else { "FAIL" },
                    case.name,
                    case.verdict.passed_checks,
                    case.verdict.total_checks
                );
                for check in &case.verdict.checks {
                    println!(
                        "  {}  {}: {}",
                        if check.passed { "PASS" } else { "FAIL" },
                        check.gate,
                        check.message
                    );
                }
            }
            Ok(())
        }
        OutputFormat::Json | OutputFormat::Jsonl => print_value(report, format),
    }
}

fn process_agent() -> Agent {
    Agent::from_process_env()
}

async fn run_once(agent: &Agent, args: RunArgs, format: OutputFormat) -> Result<(), CliError> {
    let prompt = read_prompt(args.prompt)?;
    let messages = messages(args.system, prompt);
    let result = agent
        .generate_text_messages(messages, &args.model, args.max_tokens)
        .await
        .map_err(|error| agent_error(&error, &args.model))?;
    let view = RunView {
        model: &args.model,
        text: &result.text,
        stop_reason: &result.stop_reason,
        usage: &result.usage,
    };
    match format {
        OutputFormat::Text => println!("{}", result.text),
        OutputFormat::Json | OutputFormat::Jsonl => print_value(&view, format)?,
    }
    Ok(())
}

fn read_prompt(parts: Vec<String>) -> Result<String, CliError> {
    if !parts.is_empty() && parts != ["-"] {
        let prompt = parts.join(" ");
        if !prompt.trim().is_empty() {
            return Ok(prompt);
        }
    }
    if io::stdin().is_terminal() {
        return Err(CliError::Input(
            "provide PROMPT or pipe input on stdin; see `aikit run --help`".into(),
        ));
    }
    let mut prompt = String::new();
    io::stdin().read_to_string(&mut prompt)?;
    if prompt.trim().is_empty() {
        return Err(CliError::Input("stdin contained no prompt".into()));
    }
    Ok(prompt.trim_end().to_string())
}

fn messages(system: Option<String>, prompt: String) -> Vec<Message> {
    let mut messages = Vec::new();
    if let Some(system) = system.filter(|value| !value.trim().is_empty()) {
        messages.push(Message::system(system));
    }
    messages.push(Message::user(prompt));
    messages
}

async fn chat(
    agent: &Agent,
    args: ChatArgs,
    format: OutputFormat,
    quiet: bool,
) -> Result<(), CliError> {
    if format == OutputFormat::Json {
        return Err(CliError::Input(
            "chat emits multiple events; use --format text or --format jsonl".into(),
        ));
    }
    if !io::stdin().is_terminal() && format == OutputFormat::Text {
        return Err(CliError::Input(
            "interactive chat requires a terminal; use `aikit run` for piped input".into(),
        ));
    }
    let mut history = args
        .system
        .clone()
        .map(Message::system)
        .into_iter()
        .collect::<Vec<_>>();
    if !quiet && format == OutputFormat::Text {
        eprintln!(
            "aikit chat · model={} · /help for commands · /exit or Ctrl-D to quit",
            args.model
        );
    }
    let stdin = io::stdin();
    let mut reader = stdin.lock();
    loop {
        if format == OutputFormat::Text {
            eprint!("you> ");
            io::stderr().flush()?;
        }
        let mut line = String::new();
        if reader.read_line(&mut line)? == 0 {
            break;
        }
        let input = line.trim();
        match input {
            "" => continue,
            "/exit" | "/quit" => break,
            "/help" => {
                eprintln!("/help       show this list");
                eprintln!("/providers  show provider activation (no secrets)");
                eprintln!("/clear      clear history, keeping the --system instruction");
                eprintln!("/exit       end the session (also /quit or Ctrl-D)");
                continue;
            }
            "/clear" => {
                history.clear();
                if let Some(system) = &args.system {
                    history.push(Message::system(system.clone()));
                }
                if !quiet {
                    eprintln!("conversation cleared");
                }
                continue;
            }
            "/providers" => {
                print_providers(agent, format)?;
                continue;
            }
            value if value.starts_with('/') => {
                eprintln!("unknown command: {value} (type /help for the list)");
                continue;
            }
            _ => {}
        }
        history.push(Message::user(input));
        let result = agent
            .generate_text_messages(history.clone(), &args.model, args.max_tokens)
            .await
            .map_err(|error| agent_error(&error, &args.model))?;
        match format {
            OutputFormat::Text => println!("assistant> {}", result.text),
            OutputFormat::Json | OutputFormat::Jsonl => print_value(
                &json!({"type":"message","model":args.model,"text":result.text,"usage":result.usage,"stop_reason":result.stop_reason}),
                format,
            )?,
        }
        history = result.messages;
    }
    Ok(())
}

fn print_providers(agent: &Agent, format: OutputFormat) -> Result<(), CliError> {
    let views = PROVIDERS
        .iter()
        .map(|(provider, credential, models)| ProviderView {
            provider,
            credential,
            models,
            active: agent.has_provider(provider),
        })
        .collect::<Vec<_>>();
    match format {
        OutputFormat::Text => {
            println!("PROVIDER    STATUS    CREDENTIAL                         MODELS");
            for view in views {
                println!(
                    "{:<11} {:<9} {:<34} {}",
                    view.provider,
                    if view.active { "active" } else { "inactive" },
                    view.credential,
                    view.models
                );
            }
            Ok(())
        }
        _ => print_value(&views, format),
    }
}

async fn doctor(agent: &Agent, args: DoctorArgs, format: OutputFormat) -> Result<(), CliError> {
    let mut checks = Vec::new();
    let workspace = canonical_workspace(&args.workspace)?;
    checks.push(DoctorCheck {
        name: "workspace".into(),
        status: "pass",
        detail: workspace.display().to_string(),
    });
    let active = agent.active_providers();
    checks.push(DoctorCheck {
        name: "credentials".into(),
        status: if active.is_empty() { "warn" } else { "pass" },
        detail: if active.is_empty() {
            "no provider keys found; mock-1 remains available (set e.g. ANTHROPIC_API_KEY or OPENAI_API_KEY; see `aikit providers`)".into()
        } else {
            format!("active providers: {}", active.join(", "))
        },
    });
    let tools = BuiltinTools::new(
        Sandbox::jail(&workspace).map_err(|error| CliError::Runtime(error.to_string()))?,
    )
    .with_bash();
    let containment = tools.containment_capabilities().await;
    checks.push(DoctorCheck {
        name: "bash_containment".into(),
        status: if containment.selected_backend.is_some() {
            "pass"
        } else {
            "warn"
        },
        detail: containment
            .selected_backend
            .map(|backend| format!("selected: {backend:?}"))
            .unwrap_or_else(|| {
                "no backend selected; Bash will fail closed (file tools still work; see --format json doctor for probe details)".into()
            }),
    });
    let report = DoctorReport {
        ok: checks.iter().all(|check| check.status != "fail"),
        version: env!("CARGO_PKG_VERSION"),
        checks,
        containment: serde_json::to_value(containment)?,
    };
    match format {
        OutputFormat::Text => {
            println!("aikit doctor {}", report.version);
            for check in &report.checks {
                println!(
                    "{:<5} {:<18} {}",
                    check.status.to_uppercase(),
                    check.name,
                    check.detail
                );
            }
            Ok(())
        }
        _ => print_value(&report, format),
    }
}

fn print_value(value: &impl Serialize, format: OutputFormat) -> Result<(), CliError> {
    match format {
        OutputFormat::Text | OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(value)?)
        }
        OutputFormat::Jsonl => println!("{}", serde_json::to_string(value)?),
    }
    Ok(())
}

fn exit_code(error: &CliError) -> u8 {
    match error {
        CliError::Input(_) => 2,
        CliError::Runtime(_) => 3,
        CliError::Io(_) | CliError::Json(_) => 1,
        CliError::EvalFailed { .. } => 4,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_contract_parses_grok_run() {
        let cli = Cli::try_parse_from([
            "aikit", "--format", "json", "run", "--model", "grok-4.5", "hello",
        ])
        .unwrap();
        assert_eq!(cli.format, OutputFormat::Json);
        let Command::Run(args) = cli.command else {
            panic!("expected run")
        };
        assert_eq!(args.model, "grok-4.5");
        assert_eq!(args.prompt, ["hello"]);
    }

    #[test]
    fn cli_contract_separates_external_agent_run_from_provider_run() {
        let cli = Cli::try_parse_from([
            "aikit",
            "agents",
            "run",
            "grok",
            "--approval",
            "deny",
            "inspect",
        ])
        .unwrap();
        let Command::Agents(AgentsArgs {
            command: AgentCommand::Run(args),
        }) = cli.command
        else {
            panic!("expected external-agent run")
        };
        assert_eq!(args.runtime.kind, AgentKindArg::Grok);
        assert_eq!(args.approval, AgentApprovalMode::Deny);
        assert_eq!(args.prompt, ["inspect"]);
    }

    #[test]
    fn canonical_messages_keep_system_first() {
        let got = messages(Some("be concise".into()), "hello".into());
        assert_eq!(got.len(), 2);
        assert!(matches!(got[0].role, aikit::Role::System));
        assert!(matches!(got[1].role, aikit::Role::User));
    }

    #[test]
    fn exit_codes_are_stable() {
        assert_eq!(exit_code(&CliError::Input("x".into())), 2);
        assert_eq!(exit_code(&CliError::Runtime("x".into())), 3);
        assert_eq!(
            exit_code(&CliError::EvalFailed {
                failed_cases: 1,
                total_cases: 1,
            }),
            4
        );
    }

    #[test]
    fn clap_definition_is_internally_consistent() {
        Cli::command().debug_assert();
    }

    #[test]
    fn missing_credential_hint_names_the_environment_variable() {
        let hint = agent_error_hint(&aikit::AgentError::NoCredential("anthropic"), "claude-x")
            .expect("missing credentials always carry a hint");
        assert!(hint.contains("ANTHROPIC_API_KEY"));
        assert!(hint.contains("mock-1"));
        let google = agent_error_hint(&aikit::AgentError::NoCredential("google"), "gemini-x")
            .expect("missing credentials always carry a hint");
        assert!(google.contains("GEMINI_API_KEY or GOOGLE_API_KEY"));
    }

    #[test]
    fn unknown_model_hint_lists_routing_prefixes() {
        let hint = agent_error_hint(&aikit::AgentError::UnknownModel("foo".into()), "foo")
            .expect("unknown models always carry a hint");
        assert!(hint.contains("claude-*"));
        assert!(hint.contains("'foo'"));
    }

    #[test]
    fn every_listed_provider_has_a_credential_variable() {
        for (provider, credential, _) in PROVIDERS {
            assert_eq!(provider_credential(provider), Some(credential));
        }
        assert_eq!(provider_credential("mock"), None);
    }

    #[test]
    fn eval_separates_provider_outages_from_expected_runtime_outcomes() {
        assert!(is_eval_infrastructure_error(ErrorCode::ProviderAuth));
        assert!(is_eval_infrastructure_error(ErrorCode::ProviderTimeout));
        assert!(!is_eval_infrastructure_error(ErrorCode::ProviderSafety));
        assert!(!is_eval_infrastructure_error(ErrorCode::BudgetExceeded));
        assert!(!is_eval_infrastructure_error(ErrorCode::Cancelled));
    }
}
