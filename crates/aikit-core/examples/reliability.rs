//! Reliability rules: order, prerequisites, and use caps for tool calls.
//!
//! Run: cargo run -p aikit-runtime-core --example reliability

use aikit_core::{ReliabilityPolicy, ReliabilityVerdict, RunProgress, ToolRequirement};

fn main() {
    let policy = ReliabilityPolicy::new(vec![
        ToolRequirement::for_tool("deploy")
            .only_after(["test"])
            .max_uses(1),
        ToolRequirement::for_tool("drop_database").forbidden(),
    ]);

    let mut progress = RunProgress::new();

    let attempt = |tool: &str, progress: &mut RunProgress| {
        let verdict = policy.check(tool, progress);
        println!("attempt {tool} @step{} -> {verdict:?}", progress.step());
        if matches!(verdict, ReliabilityVerdict::Allow) {
            progress.record(tool);
        }
    };

    attempt("deploy", &mut progress); // before test → Forbid
    attempt("drop_database", &mut progress); // forbidden → Forbid
    attempt("test", &mut progress); // → Allow, recorded
    attempt("deploy", &mut progress); // after test → Allow, recorded
    attempt("deploy", &mut progress); // again → Forbid (max_uses 1)

    println!(
        "reliability rules make tool use predictable (ordering, prerequisites, caps) separate from security."
    );
}
