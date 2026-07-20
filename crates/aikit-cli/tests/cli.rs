use serde_json::Value;
use std::io::Write;
use std::process::{Command, Stdio};

#[cfg(unix)]
use std::ffi::OsString;
#[cfg(unix)]
use std::os::unix::ffi::OsStringExt;

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
fn blank_credentials_do_not_activate_a_provider() {
    let output = aikit()
        .env("OPENAI_API_KEY", "   ")
        .args(["--format", "json", "providers"])
        .output()
        .unwrap();
    assert!(output.status.success());
    let providers: Value = serde_json::from_slice(&output.stdout).unwrap();
    let openai = providers
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["provider"] == "openai")
        .unwrap();
    assert_eq!(openai["active"], false);
}

#[cfg(unix)]
#[test]
fn unrelated_non_unicode_environment_values_do_not_crash_the_cli() {
    let output = aikit()
        .env("AIKIT_INVALID_UTF8", OsString::from_vec(vec![0xff, 0xfe]))
        .args(["--format", "json", "providers"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let providers: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(providers.is_array());
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

#[test]
fn eval_runs_keyless_dataset_and_emits_machine_report() {
    let mut dataset = tempfile::NamedTempFile::new().unwrap();
    write!(
        dataset,
        "{}",
        serde_json::json!({
            "schema_version": 1,
            "name": "offline-smoke",
            "model": "mock-1",
            "max_tokens": 64,
            "cases": [{
                "name": "completes",
                "prompt": "hello",
                "gates": [
                    {"type": "output_contains", "value": "tamamladım"},
                    {"type": "terminal_status", "status": "completed"},
                    {"type": "max_output_tokens", "value": 64}
                ]
            }]
        })
    )
    .unwrap();

    let output = aikit()
        .args(["--format", "json", "eval", dataset.path().to_str().unwrap()])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["dataset"], "offline-smoke");
    assert_eq!(report["schema_version"], 1);
    assert_eq!(report["runtime_version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(report["dataset_sha256"].as_str().unwrap().len(), 64);
    assert_eq!(report["passed"], true);
    assert_eq!(report["passed_cases"], 1);
    assert!(report["cases"][0].get("output").is_none());
    assert_eq!(report["cases"][0]["model_attempts"][0], "mock-1");
}

#[test]
fn eval_gate_failure_uses_dedicated_exit_code() {
    let mut dataset = tempfile::NamedTempFile::new().unwrap();
    write!(
        dataset,
        "{}",
        serde_json::json!({
            "schema_version": 1,
            "name": "failing-suite",
            "cases": [{
                "name": "wrong-expectation",
                "prompt": "hello",
                "gates": [{"type": "output_exact", "value": "not the mock output"}]
            }]
        })
    )
    .unwrap();
    let output = aikit()
        .args(["eval", dataset.path().to_str().unwrap()])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(4));
    assert!(String::from_utf8_lossy(&output.stdout).contains("FAIL"));
}

#[test]
fn eval_requires_explicit_live_provider_acknowledgement() {
    let mut dataset = tempfile::NamedTempFile::new().unwrap();
    write!(
        dataset,
        "{}",
        serde_json::json!({
            "schema_version": 1,
            "name": "live-suite",
            "model": "gpt-5",
            "cases": [{
                "name": "live",
                "prompt": "hello",
                "gates": [{"type": "no_tool_errors"}]
            }]
        })
    )
    .unwrap();
    let output = aikit()
        .args(["eval", dataset.path().to_str().unwrap()])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&output.stderr).contains("--allow-live"));
}

#[test]
fn demo_tools_allowed_passes_trajectory_gates() {
    let mut dataset = tempfile::NamedTempFile::new().unwrap();
    write!(
        dataset,
        "{}",
        serde_json::json!({
            "schema_version": 1,
            "name": "gov-allowed",
            "model": "mock-1",
            "max_tokens": 64,
            "cases": [{
                "name": "round-trip",
                "prompt": "use the probe tool",
                "gates": [
                    {"type": "called_tool", "name": "demo_probe"},
                    {"type": "tool_sequence", "names": ["demo_probe"], "exact": true},
                    {"type": "no_tool_errors"},
                    {"type": "max_turns", "value": 2},
                    {"type": "terminal_status", "status": "completed"}
                ]
            }]
        })
    )
    .unwrap();
    let output = aikit()
        .args([
            "eval",
            dataset.path().to_str().unwrap(),
            "--demo-tools",
            "allowed",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn demo_tools_denied_keeps_loop_alive() {
    let mut dataset = tempfile::NamedTempFile::new().unwrap();
    write!(
        dataset,
        "{}",
        serde_json::json!({
            "schema_version": 1,
            "name": "gov-denied",
            "model": "mock-1",
            "max_tokens": 64,
            "cases": [{
                "name": "denied-survives",
                "prompt": "attempt the probe tool",
                "gates": [
                    {"type": "called_tool", "name": "demo_probe"},
                    {"type": "max_turns", "value": 2},
                    {"type": "terminal_status", "status": "completed"}
                ]
            }]
        })
    )
    .unwrap();
    let output = aikit()
        .args([
            "eval",
            dataset.path().to_str().unwrap(),
            "--demo-tools",
            "denied",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn demo_tools_flag_is_required_for_governance_datasets() {
    // Proves the governance datasets truly depend on an advertised tool: without --demo-tools the
    // mock never emits a tool call, so the called_tool gate fails and the run exits with the
    // dedicated gate-failure code (4).
    let mut dataset = tempfile::NamedTempFile::new().unwrap();
    write!(
        dataset,
        "{}",
        serde_json::json!({
            "schema_version": 1,
            "name": "gov-needs-flag",
            "model": "mock-1",
            "max_tokens": 64,
            "cases": [{
                "name": "requires-tool",
                "prompt": "use the probe tool",
                "gates": [{"type": "called_tool", "name": "demo_probe"}]
            }]
        })
    )
    .unwrap();
    let output = aikit()
        .args(["eval", dataset.path().to_str().unwrap()])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(4));
}

#[test]
fn malformed_eval_dataset_is_an_input_error() {
    let mut dataset = tempfile::NamedTempFile::new().unwrap();
    dataset.write_all(b"{not-json").unwrap();
    let output = aikit()
        .args(["eval", dataset.path().to_str().unwrap()])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&output.stderr).contains("invalid eval dataset JSON"));
}

#[test]
fn eval_rejects_aggregate_live_token_budget_before_provider_use() {
    let mut dataset = tempfile::NamedTempFile::new().unwrap();
    write!(
        dataset,
        "{}",
        serde_json::json!({
            "schema_version": 1,
            "name": "bounded-live-suite",
            "model": "gpt-5",
            "max_tokens": 20_000,
            "cases": [
                {"name": "one", "prompt": "hello", "gates": [{"type": "no_tool_errors"}]},
                {"name": "two", "prompt": "hello", "gates": [{"type": "no_tool_errors"}]}
            ]
        })
    )
    .unwrap();
    let output = aikit()
        .args(["eval", dataset.path().to_str().unwrap(), "--allow-live"])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&output.stderr).contains("--max-live-output-tokens"));
}

#[test]
fn eval_infrastructure_failure_uses_runtime_exit_code() {
    let mut dataset = tempfile::NamedTempFile::new().unwrap();
    write!(
        dataset,
        "{}",
        serde_json::json!({
            "schema_version": 1,
            "name": "bad-runtime-suite",
            "model": "definitely-not-a-provider-model",
            "cases": [{
                "name": "cannot-start",
                "prompt": "hello",
                "gates": [{"type": "no_tool_errors"}]
            }]
        })
    )
    .unwrap();
    let output = aikit()
        .args([
            "--format",
            "json",
            "eval",
            dataset.path().to_str().unwrap(),
            "--allow-live",
        ])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(3));
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["passed"], false);
    assert!(report["cases"][0].get("error").is_some());
}

#[test]
fn eval_rejects_oversized_regular_files_without_reading_them_all() {
    let dataset = tempfile::NamedTempFile::new().unwrap();
    dataset.as_file().set_len(4 * 1024 * 1024 + 1).unwrap();
    let output = aikit()
        .args(["eval", dataset.path().to_str().unwrap()])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&output.stderr).contains("exceeds"));
}

#[cfg(unix)]
#[test]
fn eval_rejects_symlinks_before_opening_the_target() {
    let directory = tempfile::tempdir().unwrap();
    let target = directory.path().join("dataset.json");
    std::fs::write(&target, r#"{"schema_version":1,"name":"suite","cases":[]}"#).unwrap();
    let link = directory.path().join("dataset-link.json");
    std::os::unix::fs::symlink(&target, &link).unwrap();
    let output = aikit()
        .args(["eval", link.to_str().unwrap()])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&output.stderr).contains("regular file"));
}
