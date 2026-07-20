//! Honest capability model. aikit refuses to silently degrade: the caller can ask what a
//! provider actually supports and, for structured output, which fidelity tier it will get.
//! We do NOT claim grammar-constrained decoding on hosted APIs where it is impossible.

use crate::contract::CapabilityState;
use crate::reasoning::ReplayPolicy;
use serde::{Deserialize, Serialize};

/// The strength of a structured-output guarantee, surfaced to the caller so degradation is
/// never silent. Ordered strongest → weakest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FidelityGrade {
    /// Grammar-constrained decoding against the schema (e.g. OpenAI/Gemini json_schema mode).
    NativeConstrained,
    /// Coerced via a forced tool call — schema-shaped but not grammar-constrained.
    ForcedToolCall,
    /// Prompted for JSON and parsed with repair/retry — best effort, no wire guarantee.
    PromptedAndParsed,
}

/// Orthogonal structured-output facts. A provider can support native schema while still being
/// unknown for schema+tools composition or streaming validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct StructuredOutputCapabilities {
    pub native_schema: CapabilityState,
    pub forced_tool: CapabilityState,
    pub prompted_parse: CapabilityState,
    pub schema_with_tools: CapabilityState,
    pub streaming_schema: CapabilityState,
    pub parallel_tools: CapabilityState,
}

impl StructuredOutputCapabilities {
    pub fn from_fidelity(fidelity: FidelityGrade) -> Self {
        let mut profile = Self {
            native_schema: CapabilityState::Unsupported,
            forced_tool: CapabilityState::Unknown,
            prompted_parse: CapabilityState::Supported,
            schema_with_tools: CapabilityState::Unknown,
            streaming_schema: CapabilityState::Unknown,
            parallel_tools: CapabilityState::Unknown,
        };
        match fidelity {
            FidelityGrade::NativeConstrained => {
                profile.native_schema = CapabilityState::Supported;
            }
            FidelityGrade::ForcedToolCall => {
                profile.forced_tool = CapabilityState::Supported;
            }
            FidelityGrade::PromptedAndParsed => {}
        }
        profile
    }
}

/// What one provider can and cannot do. Descriptors are honest, not aspirational.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Capabilities {
    pub provider: String,
    pub supports_reasoning: bool,
    pub supports_prompt_cache: bool,
    pub supports_citations: bool,
    pub supports_vision: bool,
    /// The strongest structured-output tier this provider offers.
    pub structured_output: FidelityGrade,
    /// How reasoning state is replayed across a multi-turn tool loop.
    #[serde(skip)]
    pub reasoning_replay: ReplayPolicy,
}

impl Capabilities {
    pub fn structured_output_capabilities(&self) -> StructuredOutputCapabilities {
        StructuredOutputCapabilities::from_fidelity(self.structured_output)
    }
}

/// Registry of built-in provider capabilities. The `Agent` consults it to answer
/// `capabilities()` and to pick the right structured-output strategy per provider.
#[derive(Debug, Clone, Default)]
pub struct CapabilityRegistry {
    providers: Vec<Capabilities>,
}

impl CapabilityRegistry {
    /// The four co-equal flagship providers, described honestly.
    pub fn builtin() -> Self {
        CapabilityRegistry {
            providers: vec![
                Capabilities {
                    provider: "anthropic".into(),
                    supports_reasoning: true,
                    supports_prompt_cache: true,
                    supports_citations: true,
                    supports_vision: true,
                    // Current Messages API exposes constrained JSON via output_config.format.
                    structured_output: FidelityGrade::NativeConstrained,
                    reasoning_replay: ReplayPolicy::PreserveWithSignature,
                },
                Capabilities {
                    provider: "openai".into(),
                    supports_reasoning: true,
                    supports_prompt_cache: true,
                    supports_citations: false,
                    supports_vision: true,
                    structured_output: FidelityGrade::NativeConstrained,
                    reasoning_replay: ReplayPolicy::OpaquePassthrough,
                },
                Capabilities {
                    provider: "google".into(),
                    supports_reasoning: true,
                    supports_prompt_cache: true,
                    supports_citations: true,
                    supports_vision: true,
                    structured_output: FidelityGrade::NativeConstrained,
                    reasoning_replay: ReplayPolicy::PreserveThoughtSignature,
                },
                Capabilities {
                    provider: "deepseek".into(),
                    supports_reasoning: true,
                    supports_prompt_cache: true,
                    supports_citations: false,
                    supports_vision: false,
                    // DeepSeek offers json_object mode without a schema → best-effort parse.
                    structured_output: FidelityGrade::PromptedAndParsed,
                    reasoning_replay: ReplayPolicy::PreserveForToolCalls,
                },
                Capabilities {
                    provider: "openrouter".into(),
                    supports_reasoning: true,
                    supports_prompt_cache: false,
                    supports_citations: false,
                    supports_vision: true,
                    structured_output: FidelityGrade::PromptedAndParsed,
                    reasoning_replay: ReplayPolicy::DropOnReplay,
                },
                Capabilities {
                    provider: "groq".into(),
                    supports_reasoning: false,
                    supports_prompt_cache: false,
                    supports_citations: false,
                    supports_vision: false,
                    structured_output: FidelityGrade::PromptedAndParsed,
                    reasoning_replay: ReplayPolicy::DropOnReplay,
                },
                Capabilities {
                    provider: "mistral".into(),
                    supports_reasoning: false,
                    supports_prompt_cache: false,
                    supports_citations: false,
                    supports_vision: true,
                    structured_output: FidelityGrade::PromptedAndParsed,
                    reasoning_replay: ReplayPolicy::DropOnReplay,
                },
                Capabilities {
                    provider: "xai".into(),
                    supports_reasoning: true,
                    supports_prompt_cache: false,
                    supports_citations: false,
                    supports_vision: true,
                    structured_output: FidelityGrade::PromptedAndParsed,
                    reasoning_replay: ReplayPolicy::DropOnReplay,
                },
            ],
        }
    }

    pub fn get(&self, provider: &str) -> Option<&Capabilities> {
        self.providers.iter().find(|c| c.provider == provider)
    }

    pub fn providers(&self) -> impl Iterator<Item = &str> {
        self.providers.iter().map(|c| c.provider.as_str())
    }

    /// Register or replace a provider's capabilities (e.g. a runtime-added openai_compat host).
    pub fn upsert(&mut self, caps: Capabilities) {
        if let Some(existing) = self
            .providers
            .iter_mut()
            .find(|c| c.provider == caps.provider)
        {
            *existing = caps;
        } else {
            self.providers.push(caps);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builtin_has_all_four_co_equal_providers() {
        let reg = CapabilityRegistry::builtin();
        for p in ["anthropic", "openai", "google", "deepseek"] {
            assert!(reg.get(p).is_some(), "missing built-in provider {p}");
        }
    }

    #[test]
    fn structured_output_is_graded_honestly_not_uniformly() {
        let reg = CapabilityRegistry::builtin();
        // We do NOT pretend every provider gives the same guarantee.
        assert_eq!(
            reg.get("openai").unwrap().structured_output,
            FidelityGrade::NativeConstrained
        );
        assert_eq!(
            reg.get("anthropic").unwrap().structured_output,
            FidelityGrade::NativeConstrained
        );
        assert_eq!(
            reg.get("deepseek").unwrap().structured_output,
            FidelityGrade::PromptedAndParsed
        );
        // Grades are ordered strongest → weakest.
        assert!(FidelityGrade::NativeConstrained < FidelityGrade::PromptedAndParsed);
    }

    #[test]
    fn reasoning_replay_matches_the_reasoning_module() {
        let reg = CapabilityRegistry::builtin();
        assert_eq!(
            reg.get("anthropic").unwrap().reasoning_replay,
            ReplayPolicy::PreserveWithSignature
        );
        assert_eq!(
            reg.get("deepseek").unwrap().reasoning_replay,
            ReplayPolicy::PreserveForToolCalls
        );
    }

    #[test]
    fn upsert_adds_a_runtime_provider() {
        let mut reg = CapabilityRegistry::builtin();
        reg.upsert(Capabilities {
            provider: "openai_compat:together".into(),
            supports_reasoning: false,
            supports_prompt_cache: false,
            supports_citations: false,
            supports_vision: false,
            structured_output: FidelityGrade::PromptedAndParsed,
            reasoning_replay: ReplayPolicy::DropOnReplay,
        });
        assert!(reg.get("openai_compat:together").is_some());
        // Robust against the built-in provider set growing: upsert must add exactly one.
        let builtin_count = CapabilityRegistry::builtin().providers().count();
        assert_eq!(reg.providers().count(), builtin_count + 1);
    }
}
