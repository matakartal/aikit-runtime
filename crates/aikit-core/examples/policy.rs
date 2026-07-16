//! Declarative permission policy: JSON config compiled into an enforcing engine.
//!
//! Run: cargo run -p aikit-runtime-core --example policy

use aikit_core::PolicySpec;
use serde_json::json;

fn main() {
    let policy_json = r#"{
        "mode": "allow",
        "deny": ["Bash(rm -rf *)", "Read(*.env)"],
        "ask": ["Bash(git push *)"],
        "allow": ["Read(*)"]
    }"#;

    let spec = PolicySpec::from_json(policy_json).expect("parse policy JSON");
    let engine = spec.build().expect("build permission engine");

    let cases = [
        ("Bash", json!({"command": "rm -rf /"})),
        ("Bash", json!({"command": "git push origin main"})),
        ("Bash", json!({"command": "ls -la"})),
        ("Read", json!({"path": "secrets.env"})),
        ("Read", json!({"path": "notes.txt"})),
    ];

    for (tool, input) in &cases {
        let outcome = engine.evaluate(tool, input);
        println!("{tool} {input} -> {outcome:?}");
    }

    println!("Policy came from a declarative JSON config compiled into the enforcing engine.");
}
