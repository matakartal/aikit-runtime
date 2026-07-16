//! The Bash tool. A shell can escape an in-process path jail, so its cwd is only a convenience;
//! security comes from three composable layers:
//!   - the **permission engine** (deny `rm -rf`, deny network, ask before `git push`, ...), and
//!   - a [`BashPolicy`] of **process hardening** (scrubbed env so secrets don't leak, a timeout,
//!     bounded output, and Unix rlimits), and
//!   - fail-closed OS [`ContainmentPolicy`] (Seatbelt or a hardened Docker container).
//!
//! It is opt-in on [`BuiltinTools`](super::BuiltinTools) for exactly this reason.

use crate::error::{AikitError, Result};
use crate::governance::containment::ContainmentPolicy;
use crate::governance::process::{run_bash_with_containment, BashPolicy};
use crate::governance::sandbox::Sandbox;
use serde_json::Value;

pub async fn run(
    sb: &Sandbox,
    policy: &BashPolicy,
    containment: &ContainmentPolicy,
    input: &Value,
) -> Result<String> {
    let command = input
        .get("command")
        .and_then(Value::as_str)
        .ok_or_else(|| AikitError::ToolExecution("missing 'command' argument".into()))?;
    run_bash_with_containment(command, sb.primary_root(), policy, containment).await
}
