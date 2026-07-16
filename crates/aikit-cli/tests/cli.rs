use serde_json::Value;
use std::io::Write;
use std::process::{Command, Stdio};

fn aikit() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_aikit"));
    for key in [
        "ANTHROPIC_API_KEY",
        "OPENAI_API_KEY",
        "DEEPSEEK_API_KEY",
        "GEMINI_API_KEY",
        "GOOGLE_API_KEY",
        "XAI_API_KEY",
        "OPENROUTER_API_KEY",
        "GROQ_API_KEY",
        "MISTRAL_API_KEY",
    ] {
        command.env_remove(key);
    }
    command
}

#[test]
fn mock_run_is_keyless_and_machine_readable() {
    let output = aikit()
        .args(["--format", "json", "run", "hello"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let value: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["model"], "mock-1");
    assert!(value["text"].as_str().unwrap().contains("tamamladım"));
}

#[test]
fn run_accepts_stdin_without_a_prompt_argument() {
    let mut child = aikit()
        .args(["run", "--model", "mock-1"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(b"hello from stdin")
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success());
    assert!(String::from_utf8(output.stdout)
        .unwrap()
        .contains("tamamladım"));
}

#[test]
fn providers_never_print_secret_values() {
    let secret = "xai-secret-must-not-leak";
    let output = aikit()
        .env("XAI_API_KEY", secret)
        .args(["--format", "json", "providers"])
        .output()
        .unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(!stdout.contains(secret));
    let value: Value = serde_json::from_str(&stdout).unwrap();
    let xai = value
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["provider"] == "xai")
        .unwrap();
    assert_eq!(xai["active"], true);
}

#[test]
fn chat_rejects_ambiguous_json_document_mode() {
    let output = aikit().args(["--format", "json", "chat"]).output().unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert!(String::from_utf8(output.stderr).unwrap().contains("jsonl"));
}

#[test]
fn completions_emit_a_script() {
    let output = aikit().args(["completions", "bash"]).output().unwrap();
    assert!(output.status.success());
    assert!(String::from_utf8(output.stdout).unwrap().contains("_aikit"));
}
