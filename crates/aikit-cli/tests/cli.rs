use serde_json::Value;
use std::io::Write;
use std::process::{Command, Stdio};

#[cfg(unix)]
use std::ffi::OsString;
#[cfg(unix)]
use std::os::unix::ffi::OsStringExt;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

#[cfg(unix)]
use aikit::{
    ExternalAgentKind, ManagedRuntimeArtifactStore, RuntimeArtifact, RuntimeInstanceId,
    RuntimeProfile, RuntimeUpdateChannel, RuntimeUpdateConfig, RuntimeUpdatePolicy,
    RuntimeUpdateSelection, RuntimeUpdateSource, EXTERNAL_AGENT_CONTRACT_VERSION,
};
#[cfg(unix)]
use semver::Version;
#[cfg(unix)]
use sha2::{Digest, Sha256};

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

#[cfg(unix)]
#[test]
fn agent_discovery_inspects_but_never_executes_explicit_binary() {
    let directory = tempfile::tempdir().unwrap();
    let executable = directory.path().join("gemini");
    let marker = directory.path().join("executed");
    std::fs::write(
        &executable,
        format!("#!/bin/sh\ntouch '{}'\n", marker.display()),
    )
    .unwrap();
    std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();

    let output = aikit()
        .args([
            "--format",
            "json",
            "agents",
            "discover",
            "gemini",
            "--path",
            executable.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let candidates: Value = serde_json::from_slice(&output.stdout).unwrap();
    let explicit = candidates
        .as_array()
        .unwrap()
        .iter()
        .find(|candidate| candidate["executable"] == executable.to_string_lossy().as_ref())
        .expect("explicit Gemini candidate must be present");
    assert_eq!(explicit["kind"], "gemini");
    assert!(!marker.exists(), "discovery executed the candidate binary");
}

#[test]
fn external_agent_run_rejects_single_json_document_mode_before_launch() {
    let output = aikit()
        .args(["--format", "json", "agents", "run", "codex", "hello"])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&output.stderr).contains("jsonl"));
}

#[test]
fn gemini_cli_account_use_requires_explicit_terms_acknowledgement() {
    let output = aikit()
        .args(["agents", "status", "gemini"])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&output.stderr).contains("acknowledge-vendor-terms"));
}

#[test]
fn signed_runtime_update_configuration_round_trips_without_network() {
    let root = tempfile::tempdir().unwrap();
    let root = root.path().to_str().unwrap();
    let key = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
    let configured = aikit()
        .args([
            "--format",
            "json",
            "agents",
            "update",
            "configure",
            "gemini",
            "--root",
            root,
            "--manifest-url",
            "https://updates.example.test/manifest.json",
            "--signature-url",
            "https://updates.example.test/manifest.sig",
            "--key-id",
            "stable-2026",
            "--public-key-base64",
            key,
            "--policy",
            "aikit-managed",
        ])
        .output()
        .unwrap();
    assert!(
        configured.status.success(),
        "{}",
        String::from_utf8_lossy(&configured.stderr)
    );
    let configured: Value = serde_json::from_slice(&configured.stdout).unwrap();
    assert_eq!(configured["outcome"], "configured");
    assert_eq!(configured["config"]["policy"], "aikit_managed");
    assert_eq!(configured["state"]["phase"], "idle");

    let status = aikit()
        .args([
            "--format", "json", "agents", "update", "status", "gemini", "--root", root,
        ])
        .output()
        .unwrap();
    assert!(status.status.success());
    let status: Value = serde_json::from_slice(&status.stdout).unwrap();
    assert_eq!(status["outcome"], "status");
    assert_eq!(status["config"]["kind"], "gemini");
}

#[cfg(unix)]
#[test]
fn normal_agent_commands_prefer_the_active_managed_artifact() {
    let root = tempfile::tempdir().unwrap();
    let marker = root.path().join("managed-runtime-executed");
    let script = format!(
        "#!/bin/sh\n\
         touch '{}'\n\
         IFS= read -r initialize\n\
         id=$(printf '%s' \"$initialize\" | sed -n 's/.*\"id\":\\([0-9]*\\).*/\\1/p')\n\
         printf '{{\"id\":%s,\"result\":{{\"serverInfo\":{{\"version\":\"9.9.9\"}}}}}}\\n' \"$id\"\n\
         IFS= read -r initialized\n\
         IFS= read -r account\n\
         id=$(printf '%s' \"$account\" | sed -n 's/.*\"id\":\\([0-9]*\\).*/\\1/p')\n\
         printf '{{\"id\":%s,\"result\":{{\"account\":{{\"authMode\":\"chatgpt\"}}}}}}\\n' \"$id\"\n",
        marker.display()
    );
    let bytes = script.as_bytes();
    let hash = Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let instance = RuntimeInstanceId::default_for(ExternalAgentKind::Codex);
    let store = ManagedRuntimeArtifactStore::new(root.path().to_path_buf()).unwrap();
    let selection = RuntimeUpdateSelection {
        kind: ExternalAgentKind::Codex,
        version: Version::new(9, 9, 9),
        channel: RuntimeUpdateChannel::Stable,
        protocol: "codex-app-server".into(),
        artifact: RuntimeArtifact {
            target_os: std::env::consts::OS.into(),
            target_arch: std::env::consts::ARCH.into(),
            url: "https://updates.example.test/codex".into(),
            sha256: hash,
            size_bytes: bytes.len() as u64,
            executable_name: "codex".into(),
        },
    };
    let installed = store.stage_bytes(&selection, bytes).unwrap();
    store.activate(&instance, installed, 1_000).unwrap();
    store
        .write_update_config(&RuntimeUpdateConfig {
            schema_version: EXTERNAL_AGENT_CONTRACT_VERSION,
            instance_id: instance,
            kind: ExternalAgentKind::Codex,
            profile: RuntimeProfile::Managed,
            policy: RuntimeUpdatePolicy::AikitManaged,
            channel: RuntimeUpdateChannel::Stable,
            source: RuntimeUpdateSource {
                manifest_url: "https://updates.example.test/manifest.json".into(),
                signature_url: "https://updates.example.test/manifest.sig".into(),
                key_id: "stable-2026".into(),
                public_key_base64: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".into(),
            },
        })
        .unwrap();

    let output = aikit()
        .args([
            "--format",
            "json",
            "agents",
            "status",
            "codex",
            "--runtime-root",
            root.path().to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let status: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(status["profile"], "managed");
    assert_eq!(status["version"], "9.9.9");
    assert!(marker.exists());
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
