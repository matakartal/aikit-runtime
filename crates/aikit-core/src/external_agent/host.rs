//! Host-facing live-session construction shared by the CLI and language bindings.
//!
//! Spawning a vendor runtime involves per-kind rules (Codex app-server vs ACP, the exact Claude
//! bridge pin, the Gemini vendor-terms gate). Centralizing them here keeps every host surface —
//! CLI, Python, Node — on one fail-closed policy instead of three drifting copies.

use super::acp::{AcpExternalRuntime, AcpRuntimeConfig};
use super::codex::{CodexAppServerConfig, CodexAppServerRuntime};
use super::discovery::{discover_runtime_candidates, discover_support_executable};
use super::types::{
    vendor_session_environment, ExternalAgentError, ExternalAgentKind, ExternalAgentResult,
    ExternalAgentRuntime, RuntimeInstanceId, RuntimeProfile,
};
use std::path::PathBuf;
use std::sync::Arc;

/// Options for spawning one live vendor-CLI session on behalf of a host application.
#[derive(Debug, Clone)]
pub struct ExternalRuntimeSpawnOptions {
    /// Instance identity; defaults to the kind's default instance.
    pub instance_id: Option<RuntimeInstanceId>,
    /// Exact executable to use. `None` discovers PATH/known locations and uses the first match.
    pub explicit_executable: Option<PathBuf>,
    /// Workspace the session operates in.
    pub workspace: PathBuf,
    pub profile: RuntimeProfile,
    /// Hash of a managed artifact when launching one; report-only otherwise.
    pub artifact_hash: Option<String>,
    /// Explicit Claude ACP bridge executable; `None` uses the pinned official npx bridge.
    pub claude_bridge: Option<PathBuf>,
    pub claude_bridge_args: Vec<String>,
    /// Gemini CLI OAuth reuse is governed by Google's account terms and requires an explicit
    /// acknowledgement, exactly like the CLI's `--acknowledge-vendor-terms`.
    pub acknowledge_vendor_terms: bool,
}

impl ExternalRuntimeSpawnOptions {
    pub fn new(workspace: impl Into<PathBuf>) -> Self {
        Self {
            instance_id: None,
            explicit_executable: None,
            workspace: workspace.into(),
            profile: RuntimeProfile::External,
            artifact_hash: None,
            claude_bridge: None,
            claude_bridge_args: Vec::new(),
            acknowledge_vendor_terms: false,
        }
    }
}

/// Enforce vendor-terms acknowledgements that must precede any account reuse.
pub fn ensure_vendor_terms_acknowledged(
    kind: ExternalAgentKind,
    acknowledged: bool,
) -> ExternalAgentResult<()> {
    if kind == ExternalAgentKind::Gemini && !acknowledged {
        return Err(ExternalAgentError::invalid_input(
            "Gemini CLI OAuth use is governed by Google's account terms; set \
             acknowledge_vendor_terms, or use the normal Google provider with an API key",
        ));
    }
    Ok(())
}

/// Discover, configure, and spawn one live vendor runtime with the shared per-kind policy.
pub async fn spawn_external_runtime_for_host(
    kind: ExternalAgentKind,
    options: ExternalRuntimeSpawnOptions,
) -> ExternalAgentResult<Arc<dyn ExternalAgentRuntime>> {
    ensure_vendor_terms_acknowledged(kind, options.acknowledge_vendor_terms)?;
    if kind != ExternalAgentKind::Claude
        && (options.claude_bridge.is_some() || !options.claude_bridge_args.is_empty())
    {
        return Err(ExternalAgentError::invalid_input(
            "a Claude ACP bridge applies only to the claude kind",
        ));
    }
    let candidate = discover_runtime_candidates(kind, options.explicit_executable.as_deref())?
        .into_iter()
        .next()
        .ok_or_else(|| {
            ExternalAgentError::invalid_input(format!(
                "no {kind} executable was found; install it or pass an explicit path"
            ))
        })?;
    let instance_id = match options.instance_id {
        Some(instance_id) => instance_id,
        None => RuntimeInstanceId::default_for(kind),
    };

    match kind {
        ExternalAgentKind::Codex => {
            let runtime = CodexAppServerRuntime::spawn(CodexAppServerConfig {
                instance_id,
                candidate,
                profile: options.profile,
                environment: vendor_session_environment(ExternalAgentKind::Codex),
                inherit_env: true,
                artifact_hash: options.artifact_hash,
            })
            .await?;
            Ok(Arc::new(runtime))
        }
        ExternalAgentKind::Gemini | ExternalAgentKind::Grok => {
            let mut config = AcpRuntimeConfig::native(instance_id, candidate, options.profile)?;
            config.default_workspace = Some(options.workspace);
            config.artifact_hash = options.artifact_hash;
            let runtime = AcpExternalRuntime::spawn(config).await?;
            Ok(Arc::new(runtime))
        }
        ExternalAgentKind::Claude => {
            let mut config = if let Some(bridge) = options.claude_bridge {
                let bridge = bridge.canonicalize().map_err(|error| {
                    ExternalAgentError::invalid_input(format!(
                        "Claude ACP bridge {}: {error}",
                        bridge.display()
                    ))
                })?;
                AcpRuntimeConfig::pinned_bridge(
                    instance_id,
                    candidate,
                    options.profile,
                    bridge,
                    options.claude_bridge_args,
                )?
            } else {
                if !options.claude_bridge_args.is_empty() {
                    return Err(ExternalAgentError::invalid_input(
                        "claude_bridge_args require an explicit claude_bridge",
                    ));
                }
                let npx = discover_support_executable("npx")?;
                AcpRuntimeConfig::pinned_claude_npx_bridge(
                    instance_id,
                    candidate,
                    options.profile,
                    npx,
                )?
            };
            config.default_workspace = Some(options.workspace);
            config.artifact_hash = options.artifact_hash;
            let runtime = AcpExternalRuntime::spawn(config).await?;
            Ok(Arc::new(runtime))
        }
    }
}
