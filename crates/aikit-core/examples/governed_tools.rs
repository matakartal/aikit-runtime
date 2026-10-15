//! The governed tool suite: a filesystem sandbox + built-in tools + the permission engine +
//! process hardening, composed without any model in the loop. Keyless and offline.
//!
//! What it demonstrates, step by step:
//! 1. `Sandbox::jail` confines the built-in file tools (`Read`, `Write`, ...) to one workspace.
//! 2. `Write`/`Read` work inside the jail.
//! 3. A read outside the jail (`/etc/hosts`) is refused by the sandbox.
//! 4. A `PermissionEngine` rule denies `rm -rf`, including a whitespace-obfuscated variant.
//! 5. An allowed `Bash` command runs. Bash is opt-in (`.with_bash()`) and is NOT jailed by the
//!    sandbox — the permission engine (and, if configured, OS containment) is its guard.
//! 6. The default `BashPolicy` scrubs the environment, so the shell cannot read API keys.
//!
//! In a real run you pass `tools` as the agent's executor and `gov` as `AgentOptions::governance`;
//! the runtime then calls `authorize` before every tool call, exactly as step 4 does by hand.
//!
//! Run: `cargo run -p aikit-runtime-core --example governed_tools`
//!
//! Expected: every escape attempt prints `DENIED`, `ls -la` prints `ALLOWED`, and the API-key
//! probe prints `key=CLEARED`. The workspace is a temp directory removed at the end.

use aikit_core::{
    Authorization, BuiltinTools, Governance, PermissionEngine, PermissionMode, Rule, Sandbox,
    ToolExecutor,
};
use serde_json::json;

#[tokio::main]
async fn main() {
    // 1. Jail the agent's file tools to a workspace directory.
    let workspace = std::env::temp_dir().join("aikit-governed-demo");
    std::fs::create_dir_all(&workspace).unwrap();
    let sandbox = Sandbox::jail(&workspace).unwrap();
    let tools = BuiltinTools::new(sandbox).with_bash();
    println!("workspace: {}\n", workspace.display());

    // 2. Sandbox-enforced file tools: write then read inside the jail.
    let w = tools
        .execute(
            "Write",
            json!({ "path": "hello.txt", "content": "merhaba dünya" }),
        )
        .await
        .unwrap();
    println!("Write hello.txt        -> {w}");
    let r = tools
        .execute("Read", json!({ "path": "hello.txt" }))
        .await
        .unwrap();
    println!("Read  hello.txt        -> {r:?}");

    // 3. The sandbox denies an escape out of the workspace.
    match tools.execute("Read", json!({ "path": "/etc/hosts" })).await {
        Ok(_) => println!("Read  /etc/hosts       -> !! ESCAPE NOT BLOCKED"),
        Err(e) => println!("Read  /etc/hosts       -> DENIED: {e}"),
    }

    // 4. The permission engine: deny the dangerous Bash, allow the rest. The third command hides
    //    the space as a TAB; the `\s+` in the rule matches it, so it is denied too.
    let gov = Governance::new(
        PermissionEngine::with_rules(
            PermissionMode::Allow,
            vec![Rule::deny("Bash").matching(r"rm\s+-rf").unwrap()],
        ),
        Default::default(),
    );
    println!();
    for (label, cmd) in [
        ("ls -la", "ls -la"),
        ("rm -rf /", "rm -rf /"),
        ("rm<TAB>-rf / (escape attempt)", "rm\t-rf /"),
    ] {
        match gov.authorize("Bash", &json!({ "command": cmd })).await {
            Authorization::Allowed(_) => println!("authorize Bash {label:32} -> ALLOWED"),
            Authorization::Denied { message, .. } => {
                println!("authorize Bash {label:32} -> DENIED ({message})")
            }
        }
    }

    // 5. An allowed Bash command actually runs through the suite (Bash is opt-in and NOT jailed —
    //    the permission engine is its guard, which is exactly why step 4 matters).
    let out = tools
        .execute("Bash", json!({ "command": "echo governed" }))
        .await
        .unwrap();
    println!("\nBash echo (allowed)    -> {}", out.replace('\n', " "));

    // 6. Process hardening: aikit's own secrets do NOT leak into the shell. `.with_bash()` applies
    //    the default BashPolicy, which scrubs the environment — a shell that could read the API key
    //    is a credential-exfiltration path the permission engine cannot see into.
    std::env::set_var("ANTHROPIC_API_KEY", "sk-ant-SUPERSECRET");
    let leak = tools
        .execute(
            "Bash",
            json!({ "command": "echo key=${ANTHROPIC_API_KEY:-CLEARED}" }),
        )
        .await
        .unwrap();
    std::env::remove_var("ANTHROPIC_API_KEY");
    println!("Bash echo $API_KEY     -> {}", leak.replace('\n', " "));
    assert!(
        !leak.contains("SUPERSECRET"),
        "the API key leaked into Bash!"
    );

    let _ = std::fs::remove_dir_all(&workspace);
    println!(
        "\nOK: governed tool suite demonstrated — sandbox + permissions + built-in tools + process \
         hardening (scrubbed env), all provider-agnostic.\n\
         Next: run `--example defense_in_depth` to see these layers stacked with guardrails."
    );
}
