//! A keyless, runnable demonstration of aikit's flagship: the governed tool suite — a filesystem
//! sandbox + built-in tools + the permission engine — the combination no other multi-provider SDK
//! ships in one package. No API key required.
//!
//! Run: `cargo run -p aikit-runtime-core --example governed_tools`

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

    // 4. The permission engine: deny the dangerous Bash, allow the rest. Note the third command
    //    hides the space as a TAB — a bypass the audit caught; it is denied correctly now.
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
    //    the permission engine is its guard, which is exactly why steps 4 matters).
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
        "\n✅ governed tool suite: sandbox + permissions + built-in tools + process hardening \
         (scrubbed env), all provider-agnostic."
    );
}
