use aikit::{Agent, BuiltinTools, Message, Sandbox};
use clap::{Args, CommandFactory, Parser, Subcommand, ValueEnum};
use clap_complete::{generate, Shell};
use serde::Serialize;
use serde_json::{json, Value};
use std::env;
use std::io::{self, BufRead, IsTerminal, Read, Write};
use std::path::PathBuf;
use std::process::ExitCode;

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
        Command::Completions(args) => {
            generate(args.shell, &mut Cli::command(), "aikit", &mut io::stdout());
            Ok(())
        }
    }
}

fn process_agent() -> Agent {
    Agent::from_env(env::vars().filter(|(_, value)| !value.trim().is_empty()))
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
    }
}
