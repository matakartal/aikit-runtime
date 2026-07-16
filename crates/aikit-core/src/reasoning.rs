//! Per-provider reasoning-state replay — proof-point #1.
//!
//! Reasoning models disagree, *by construction*, about what you must send back on the next
//! turn of a tool loop. Getting this wrong is not a soft error: it is a hard 400 (Anthropic)
//! or silently dropped/ignored state that corrupts the model's chain of thought (DeepSeek).
//! A lowest-common-denominator wrapper cannot satisfy all four at once — so aikit does not try
//! to *translate* reasoning across vendors. It captures each provider's opaque state on the
//! [`ContentBlock::Reasoning`] block and replays it verbatim according to that provider's own
//! rule, encoded here as a [`ReplayPolicy`].
//!
//! Verified rules (see docs/ for sources):
//!  - **Anthropic**: replay signed `thinking` blocks in the assistant turn preceding the
//!    `tool_use`, with `text` and `signature` **unchanged** — the API rejects tampering.
//!  - **DeepSeek** thinking mode: replay `reasoning_content` for assistant turns that performed
//!    tool calls; it may be omitted for assistant turns without tool calls.
//!  - **OpenAI**: carry the opaque reasoning item (id / encrypted content) forward.
//!  - **Google** (Gemini): preserve the `thought` signature.

use crate::types::ContentBlock;

/// How a provider wants reasoning state threaded back into the next request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReplayPolicy {
    /// Anthropic: keep the `thinking` block verbatim (text + signature). Required for a
    /// tool-use continuation with thinking enabled; dropping/editing it → 400.
    PreserveWithSignature,
    /// OpenAI: keep the reasoning item's opaque payload (id / encrypted content).
    OpaquePassthrough,
    /// Google: keep the thought signature.
    PreserveThoughtSignature,
    /// DeepSeek: replay reasoning only on assistant turns that contain tool calls. The serializer
    /// has the surrounding message needed to make that conditional decision.
    PreserveForToolCalls,
    /// Drop reasoning on replay. This is the safe default for unknown providers (never send back
    /// rejectable vendor-specific state).
    #[default]
    DropOnReplay,
}

impl ReplayPolicy {
    /// The policy for a canonical provider name. Unknown providers default to the safe
    /// `DropOnReplay` (never send back state a provider might reject).
    pub fn for_provider(provider: &str) -> ReplayPolicy {
        match provider {
            "anthropic" => ReplayPolicy::PreserveWithSignature,
            "openai" => ReplayPolicy::OpaquePassthrough,
            "google" => ReplayPolicy::PreserveThoughtSignature,
            "deepseek" => ReplayPolicy::PreserveForToolCalls,
            _ => ReplayPolicy::DropOnReplay,
        }
    }

    /// Whether the block's signature must survive replay untouched. When true, the loop must
    /// never strip or rewrite the signature (the classic Anthropic 400 trap).
    pub fn requires_signature(self) -> bool {
        matches!(
            self,
            ReplayPolicy::PreserveWithSignature | ReplayPolicy::PreserveThoughtSignature
        )
    }
}

/// Given the reasoning blocks captured from a provider's response, return the blocks that must
/// be included when re-sending that assistant turn under `policy`. For `DropOnReplay` this is
/// empty; for the others the blocks are returned unchanged (verbatim replay). For
/// `PreserveForToolCalls`, callers must only use the returned blocks when the same assistant
/// message also contains a tool call.
pub fn blocks_for_replay(policy: ReplayPolicy, reasoning: &[ContentBlock]) -> Vec<ContentBlock> {
    match policy {
        ReplayPolicy::DropOnReplay => Vec::new(),
        ReplayPolicy::PreserveWithSignature
        | ReplayPolicy::OpaquePassthrough
        | ReplayPolicy::PreserveThoughtSignature
        | ReplayPolicy::PreserveForToolCalls => reasoning
            .iter()
            .filter(|b| matches!(b, ContentBlock::Reasoning { .. }))
            .cloned()
            .collect(),
    }
}

/// Select replayable reasoning for one provider. Tagged state from another provider is always
/// dropped: opaque/signature formats are vendor-specific and must never cross a fallback
/// boundary. Untagged blocks remain accepted for backwards-compatible single-provider sessions;
/// new runtime records always carry the provider name.
pub fn blocks_for_provider_replay(
    provider: &str,
    policy: ReplayPolicy,
    reasoning: &[ContentBlock],
) -> Vec<ContentBlock> {
    let owned: Vec<ContentBlock> = reasoning
        .iter()
        .filter(|block| match block {
            ContentBlock::Reasoning {
                provider: source, ..
            } => source.as_deref().is_none_or(|source| source == provider),
            _ => false,
        })
        .cloned()
        .collect();
    blocks_for_replay(policy, &owned)
}

/// Validate that a set of reasoning blocks about to be replayed satisfies `policy`. Returns the
/// first violation, so a loop can fail loudly *before* sending a request that the provider would
/// reject — catching the "signature was stripped" bug at our layer, not the wire.
pub fn validate_replay(
    policy: ReplayPolicy,
    reasoning: &[ContentBlock],
) -> Result<(), ReplayError> {
    match policy {
        // No opaque integrity constraint is needed for dropped or plain-text conditional replay.
        ReplayPolicy::DropOnReplay | ReplayPolicy::PreserveForToolCalls => Ok(()),
        // OpenAI: the opaque reasoning item (id / encrypted content) MUST be present to carry
        // forward — a degenerate block with `opaque: None` would only 400 at the wire.
        ReplayPolicy::OpaquePassthrough => {
            for block in reasoning {
                if let ContentBlock::Reasoning { opaque, .. } = block {
                    if opaque.is_none() {
                        return Err(ReplayError::MissingOpaque);
                    }
                }
            }
            Ok(())
        }
        // Anthropic + Google require a present, non-empty signature. The empty-text guard is
        // Anthropic-SPECIFIC: Gemini attaches a thought signature to a function-call part with
        // no exposed thought text, so an empty-text Google block is legitimate.
        ReplayPolicy::PreserveWithSignature | ReplayPolicy::PreserveThoughtSignature => {
            for block in reasoning {
                if let ContentBlock::Reasoning {
                    signature, text, ..
                } = block
                {
                    match signature {
                        None => return Err(ReplayError::MissingSignature),
                        Some(sig) if sig.is_empty() => return Err(ReplayError::MissingSignature),
                        Some(_)
                            if text.is_empty()
                                && matches!(policy, ReplayPolicy::PreserveWithSignature) =>
                        {
                            return Err(ReplayError::EmptyReasoningText)
                        }
                        _ => {}
                    }
                }
            }
            Ok(())
        }
    }
}

/// Why a replay set is invalid for a provider's policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayError {
    /// A signature-requiring provider was given a reasoning block with no (or empty) signature.
    MissingSignature,
    /// A signature-requiring provider was given a signed block whose text was emptied out.
    EmptyReasoningText,
    /// An opaque-passthrough provider (OpenAI) was given a block with no opaque payload to carry.
    MissingOpaque,
}

impl std::fmt::Display for ReplayError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReplayError::MissingSignature => {
                write!(
                    f,
                    "reasoning block is missing its required signature (would 400)"
                )
            }
            ReplayError::EmptyReasoningText => {
                write!(
                    f,
                    "signed reasoning block has empty text (signature/text mismatch)"
                )
            }
            ReplayError::MissingOpaque => {
                write!(
                    f,
                    "reasoning block has no opaque payload to carry forward (would 400)"
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn anthropic_thinking() -> ContentBlock {
        ContentBlock::Reasoning {
            text: "Let me work through this...".into(),
            signature: Some("sig_abc123".into()),
            provider: Some("anthropic".into()),
            opaque: None,
        }
    }

    fn deepseek_reasoning() -> ContentBlock {
        ContentBlock::Reasoning {
            text: "首先，我需要...".into(),
            signature: None,
            provider: Some("deepseek".into()),
            opaque: None,
        }
    }

    fn openai_reasoning() -> ContentBlock {
        ContentBlock::Reasoning {
            text: String::new(),
            signature: None,
            provider: Some("openai".into()),
            opaque: Some(json!({ "reasoning_item_id": "rs_123", "encrypted": "..." })),
        }
    }

    #[test]
    fn policy_mapping_is_per_provider() {
        assert_eq!(
            ReplayPolicy::for_provider("anthropic"),
            ReplayPolicy::PreserveWithSignature
        );
        assert_eq!(
            ReplayPolicy::for_provider("deepseek"),
            ReplayPolicy::PreserveForToolCalls
        );
        assert_eq!(
            ReplayPolicy::for_provider("openai"),
            ReplayPolicy::OpaquePassthrough
        );
        assert_eq!(
            ReplayPolicy::for_provider("google"),
            ReplayPolicy::PreserveThoughtSignature
        );
        // Unknown providers default to the safe drop policy.
        assert_eq!(
            ReplayPolicy::for_provider("mystery"),
            ReplayPolicy::DropOnReplay
        );
    }

    #[test]
    fn anthropic_replays_thinking_with_signature_intact() {
        let blocks = [anthropic_thinking()];
        let replay = blocks_for_replay(ReplayPolicy::PreserveWithSignature, &blocks);
        assert_eq!(replay.len(), 1, "Anthropic must replay the thinking block");
        // Signature and text survive verbatim.
        match &replay[0] {
            ContentBlock::Reasoning {
                signature, text, ..
            } => {
                assert_eq!(signature.as_deref(), Some("sig_abc123"));
                assert!(text.contains("work through"));
            }
            _ => panic!("expected a reasoning block"),
        }
        assert!(validate_replay(ReplayPolicy::PreserveWithSignature, &replay).is_ok());
    }

    #[test]
    fn deepseek_drops_reasoning_on_replay() {
        let blocks = [deepseek_reasoning()];
        let replay = blocks_for_replay(ReplayPolicy::DropOnReplay, &blocks);
        assert!(
            replay.is_empty(),
            "DeepSeek reasoning_content must NOT be fed back — the inverse of Anthropic"
        );
        // Drop policy never validates signatures (there is nothing to send).
        assert!(validate_replay(ReplayPolicy::DropOnReplay, &blocks).is_ok());
    }

    #[test]
    fn cross_provider_reasoning_is_never_replayed() {
        let blocks = [anthropic_thinking(), openai_reasoning()];
        let anthropic =
            blocks_for_provider_replay("anthropic", ReplayPolicy::PreserveWithSignature, &blocks);
        let openai = blocks_for_provider_replay("openai", ReplayPolicy::OpaquePassthrough, &blocks);
        assert_eq!(anthropic, vec![blocks[0].clone()]);
        assert_eq!(openai, vec![blocks[1].clone()]);
    }

    #[test]
    fn openai_carries_opaque_item_forward() {
        let blocks = [openai_reasoning()];
        let replay = blocks_for_replay(ReplayPolicy::OpaquePassthrough, &blocks);
        assert_eq!(replay.len(), 1);
        match &replay[0] {
            ContentBlock::Reasoning { opaque, .. } => {
                assert_eq!(opaque.as_ref().unwrap()["reasoning_item_id"], "rs_123");
            }
            _ => panic!("expected a reasoning block"),
        }
    }

    #[test]
    fn stripping_anthropic_signature_is_caught_before_the_wire() {
        // Simulate the classic bug: a wrapper drops the signature before replay.
        let stripped = ContentBlock::Reasoning {
            text: "Let me work through this...".into(),
            signature: None, // ← the 400 trap
            provider: Some("anthropic".into()),
            opaque: None,
        };
        let err = validate_replay(ReplayPolicy::PreserveWithSignature, &[stripped]);
        assert_eq!(err, Err(ReplayError::MissingSignature));
    }

    #[test]
    fn emptying_signed_text_is_caught() {
        let hollow = ContentBlock::Reasoning {
            text: String::new(), // signed but text scrubbed
            signature: Some("sig_abc123".into()),
            provider: Some("anthropic".into()),
            opaque: None,
        };
        assert_eq!(
            validate_replay(ReplayPolicy::PreserveWithSignature, &[hollow]),
            Err(ReplayError::EmptyReasoningText)
        );
    }

    #[test]
    fn google_empty_text_signature_block_is_valid() {
        // Gemini attaches a thought signature to a function-call part with no exposed text.
        // The Anthropic-specific empty-text guard must NOT reject it.
        let block = ContentBlock::Reasoning {
            text: String::new(),
            signature: Some("thought_sig".into()),
            provider: Some("google".into()),
            opaque: None,
        };
        assert!(validate_replay(ReplayPolicy::PreserveThoughtSignature, &[block]).is_ok());
    }

    #[test]
    fn openai_missing_opaque_is_caught_before_the_wire() {
        // OpaquePassthrough with no opaque payload would 400 — catch it at our layer.
        let degenerate = ContentBlock::Reasoning {
            text: "some text".into(),
            signature: None,
            provider: Some("openai".into()),
            opaque: None,
        };
        assert_eq!(
            validate_replay(ReplayPolicy::OpaquePassthrough, &[degenerate]),
            Err(ReplayError::MissingOpaque)
        );
        // With the opaque item present it validates.
        assert!(validate_replay(ReplayPolicy::OpaquePassthrough, &[openai_reasoning()]).is_ok());
    }
}
