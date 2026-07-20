use aikit::{
    evaluate_outcome, tool, Agent, BuiltinTools, ErrorCode, ErrorInfo, EvalCaseReport, EvalDataset,
    EvalReport, EvalVerdict, Governance, Message, NoTools, PermissionEngine, PermissionMode, Rule,
    RunConfig, RunRecorder, Sandbox, StreamDelta, ToolExecutor, ToolSpec,
};
use clap::{Args, CommandFactory, Parser, Subcommand, ValueEnum};
use clap_complete::{generate, Shell};
use futures::StreamExt;
use serde::Serialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
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

#[derive(Debug, Parser)]
#[command(
    name = "aikit",
    version,
    about = "Governed, provider-aware AI agents from one Rust core"
)]
#[command(propagate_version = true, arg_required_else_help = true)]
struct Cli {
    /// Output contract for humans or automation.
    #[arg(long, global = true, value_enum, default_value_t = OutputFormat::Text)]
    format: OutputFormat,

    /// Suppress non-result informational output.
    #[arg(long, global = true)]
    quiet: bool,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
enum OutputFormat {
    Text,
    Json,
    Jsonl,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run one prompt and print the completed result.
    Run(RunArgs),
    /// Start a multi-turn terminal conversation.
    Chat(ChatArgs),
    /// List supported providers without exposing credentials.
    Providers,
    /// Show currently active provider and runtime capabilities.
    Capabilities,
    /// Diagnose credentials, workspace access, and containment.
    Doctor(DoctorArgs),
    /// Run a deterministic JSON evaluation dataset; live models require --allow-live.
    Eval(EvalArgs),
    /// Generate a shell completion script on stdout.
    Completions(CompletionArgs),
}

#[derive(Debug, Args)]
struct RunArgs {
    /// Prompt text. Use '-' or omit it to read stdin.
    #[arg(value_name = "PROMPT", trailing_var_arg = true)]
    prompt: Vec<String>,

    /// Model id. Examples: mock-1, claude-*, gpt-*, gemini-*, deepseek-*, grok-4.5.
    #[arg(short, long, default_value = "mock-1")]
    model: String,

    /// Maximum output-token budget.
    #[arg(long, default_value_t = 1024, value_parser = clap::value_parser!(u64).range(1..))]
    max_tokens: u64,

    /// Optional system instruction prepended to the canonical message list.
    #[arg(long)]
    system: Option<String>,
}

#[derive(Debug, Args)]
struct ChatArgs {
    /// Model id used for every turn.
    #[arg(short, long, default_value = "mock-1")]
    model: String,

    /// Maximum output-token budget per turn.
    #[arg(long, default_value_t = 1024, value_parser = clap::value_parser!(u64).range(1..))]
    max_tokens: u64,

    /// Optional system instruction retained across the conversation.
    #[arg(long)]
    system: Option<String>,
}

#[derive(Debug, Args)]
struct DoctorArgs {
    /// Workspace used for filesystem jail and containment probing.
    #[arg(long, default_value = ".")]
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

    /// Maximum number of paid/network cases permitted in one invocation.
    #[arg(long, default_value_t = DEFAULT_MAX_LIVE_CASES, value_parser = clap::value_parser!(u64).range(1..=256))]
    max_live_cases: u64,

    /// Aggregate prompt and system-instruction byte ceiling across all live cases.
    #[arg(long, default_value_t = DEFAULT_MAX_LIVE_INPUT_BYTES, value_parser = clap::value_parser!(u64).range(1..))]
    max_live_input_bytes: u64,

    /// Aggregate requested output-token ceiling across all live cases.
    #[arg(long, default_value_t = DEFAULT_MAX_LIVE_OUTPUT_TOKENS, value_parser = clap::value_parser!(u64).range(1..))]
    max_live_output_tokens: u64,

    /// Hard wall-time ceiling shared by all live cases in this invocation.
    #[arg(long, default_value_t = DEFAULT_MAX_LIVE_WALL_SECONDS, value_parser = clap::value_parser!(u64).range(1..=86_400))]
    max_live_wall_seconds: u64,

    /// Register a deterministic in-process demo probe tool so governance-trajectory datasets can
    /// exercise a real tool call. `denied` additionally installs a deny rule so the call is refused
    /// before execution. The probe has no side effects (it echoes its input).
    #[arg(long, value_enum)]
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
    #[arg(value_enum)]
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
    let agent = process_agent();
    match cli.command {
        Command::Run(args) => run_once(&agent, args, cli.format).await,
        Command::Chat(args) => chat(&agent, args, cli.format, cli.quiet).await,
        Command::Providers => print_providers(&agent, cli.format),
        Command::Capabilities => print_value(&agent.capabilities(), cli.format),
        Command::Doctor(args) => doctor(&agent, args, cli.format).await,
        Command::Eval(args) => run_eval(&agent, args, cli.format).await,
        Command::Completions(args) => {
            generate(args.shell, &mut Cli::command(), "aikit", &mut io::stdout());
            Ok(())
        }
    }
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
            "dataset requests {} live case(s); --max-live-cases is {}",
            live_cases.len(),
            args.max_live_cases
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
            "dataset requests {requested_live_input_bytes} live input byte(s); --max-live-input-bytes is {}",
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
            "dataset requests {requested_live_output_tokens} live output token(s); --max-live-output-tokens is {}",
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
    let path_metadata = std::fs::symlink_metadata(path)?;
    if !path_metadata.file_type().is_file() {
        return Err(CliError::Input(
            "eval dataset must be a regular file, not a symlink or special file".into(),
        ));
    }

    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    let file = options.open(path)?;
    let metadata = file.metadata()?;
    if !metadata.file_type().is_file() {
        return Err(CliError::Input(
            "eval dataset must remain a regular file while it is read".into(),
        ));
    }
    if metadata.len() > MAX_EVAL_DATASET_BYTES as u64 {
        return Err(CliError::Input(format!(
            "eval dataset exceeds the {} byte limit",
            MAX_EVAL_DATASET_BYTES
        )));
    }

    let mut bytes = Vec::with_capacity(
        usize::try_from(metadata.len())
            .unwrap_or(MAX_EVAL_DATASET_BYTES)
            .min(MAX_EVAL_DATASET_BYTES),
    );
    file.take((MAX_EVAL_DATASET_BYTES + 1) as u64)
        .read_to_end(&mut bytes)?;
    if bytes.len() > MAX_EVAL_DATASET_BYTES {
        return Err(CliError::Input(format!(
            "eval dataset exceeds the {} byte limit",
            MAX_EVAL_DATASET_BYTES
        )));
    }
    let dataset = serde_json::from_slice(&bytes)
        .map_err(|error| CliError::Input(format!("invalid eval dataset JSON: {error}")))?;
    let sha256 = format!("{:x}", Sha256::digest(&bytes));
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
        .map_err(|error| CliError::Runtime(error.to_string()))?;
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
        eprintln!("aikit chat · model={} · /help for commands", args.model);
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
                eprintln!("/help  /providers  /clear  /exit");
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
                eprintln!("unknown command: {value}");
                continue;
            }
            _ => {}
        }
        history.push(Message::user(input));
        let result = agent
            .generate_text_messages(history.clone(), &args.model, args.max_tokens)
            .await
            .map_err(|error| CliError::Runtime(error.to_string()))?;
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
    let workspace = args.workspace.canonicalize().map_err(|error| {
        CliError::Input(format!("workspace {}: {error}", args.workspace.display()))
    })?;
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
            "no provider keys found; mock-1 remains available".into()
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
            .unwrap_or_else(|| "no backend selected; Bash will fail closed".into()),
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
    fn eval_separates_provider_outages_from_expected_runtime_outcomes() {
        assert!(is_eval_infrastructure_error(ErrorCode::ProviderAuth));
        assert!(is_eval_infrastructure_error(ErrorCode::ProviderTimeout));
        assert!(!is_eval_infrastructure_error(ErrorCode::ProviderSafety));
        assert!(!is_eval_infrastructure_error(ErrorCode::BudgetExceeded));
        assert!(!is_eval_infrastructure_error(ErrorCode::Cancelled));
    }
}
