//! Credential-driven capability activation — the agent-native "drop in a key → get stronger"
//! primitive. A credential is detected (by env-var name, or by key-format signature) and
//! resolved to a provider, which unlocks that provider's models and capabilities.
//!
//! Honesty about ambiguity is a feature: OpenAI and DeepSeek both mint `sk-...` keys, so the
//! key format alone cannot tell them apart. We say so ([`KeyGuess::AmbiguousSk`]) instead of
//! guessing wrong — the caller disambiguates with an explicit provider or the env-var name.

/// Canonical provider names aikit knows how to activate from a credential.
pub const KNOWN_PROVIDERS: &[&str] = &[
    "anthropic",
    "openai",
    "google",
    "deepseek",
    "openrouter",
    "groq",
    "mistral",
    "xai",
];

/// What a raw key's *format* tells us about its provider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyGuess {
    /// Format is unambiguous — this is the provider.
    Provider(&'static str),
    /// An `sk-...` key that could be OpenAI or DeepSeek — needs disambiguation.
    AmbiguousSk,
    /// No recognizable signature.
    Unknown,
}

/// Detect a provider from a key's format signature. Unambiguous prefixes resolve directly;
/// the shared `sk-` prefix is reported as ambiguous rather than guessed.
pub fn provider_from_key(key: &str) -> KeyGuess {
    let k = key.trim();
    if k.starts_with("sk-ant-admin") {
        // Anthropic admin/org keys are management credentials, not inference
        // credentials — they must not resolve to the "anthropic" inference provider.
        KeyGuess::Unknown
    } else if k.starts_with("sk-ant-") {
        KeyGuess::Provider("anthropic")
    } else if k.starts_with("AIza") {
        // Google API keys.
        KeyGuess::Provider("google")
    } else if k.starts_with("sk-or-") {
        // OpenRouter / aggregator keys (e.g. `sk-or-v1-...`) are neither OpenAI nor
        // DeepSeek, so reporting them as ambiguous `sk-` keys would mislabel them.
        KeyGuess::Unknown
    } else if k.starts_with("sk-") {
        // OpenAI and DeepSeek both use this prefix.
        KeyGuess::AmbiguousSk
    } else {
        KeyGuess::Unknown
    }
}

/// Map a conventional environment-variable name to its provider. This is the reliable
/// disambiguator: `OPENAI_API_KEY` vs `DEEPSEEK_API_KEY` resolve cleanly where the key format
/// cannot.
pub fn provider_from_env_var(name: &str) -> Option<&'static str> {
    match name {
        "ANTHROPIC_API_KEY" => Some("anthropic"),
        "OPENAI_API_KEY" => Some("openai"),
        "DEEPSEEK_API_KEY" => Some("deepseek"),
        "GEMINI_API_KEY" | "GOOGLE_API_KEY" => Some("google"),
        "OPENROUTER_API_KEY" => Some("openrouter"),
        "GROQ_API_KEY" => Some("groq"),
        "MISTRAL_API_KEY" => Some("mistral"),
        "XAI_API_KEY" => Some("xai"),
        _ => None,
    }
}

/// Resolve a credential to a provider using an optional explicit hint, then the env-var name,
/// then the key format. Returns `Err` when the result is genuinely ambiguous or unknown so the
/// agent can ask rather than mis-route the key.
pub fn resolve_provider(
    key: &str,
    explicit: Option<&str>,
    env_var: Option<&str>,
) -> Result<&'static str, ResolveError> {
    if key.trim().is_empty() {
        return Err(ResolveError::Empty);
    }
    if let Some(p) = explicit {
        return KNOWN_PROVIDERS
            .iter()
            .copied()
            .find(|&kp| kp == p)
            .ok_or_else(|| ResolveError::UnknownProvider(p.to_string()));
    }
    if let Some(p) = env_var.and_then(provider_from_env_var) {
        return Ok(p);
    }
    match provider_from_key(key) {
        KeyGuess::Provider(p) => Ok(p),
        KeyGuess::AmbiguousSk => Err(ResolveError::Ambiguous),
        KeyGuess::Unknown => Err(ResolveError::Unrecognized),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolveError {
    /// An empty or whitespace-only value can never authenticate an inference request.
    Empty,
    /// `sk-...` key with no env-var or explicit hint — could be OpenAI or DeepSeek.
    Ambiguous,
    /// No recognizable signature and no hint.
    Unrecognized,
    /// An explicit provider name that aikit does not know.
    UnknownProvider(String),
}

impl std::fmt::Display for ResolveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResolveError::Empty => write!(f, "credential cannot be empty"),
            ResolveError::Ambiguous => write!(
                f,
                "sk- key is ambiguous (OpenAI or DeepSeek); pass an explicit provider or use the env-var name"
            ),
            ResolveError::Unrecognized => write!(f, "unrecognized key format; pass an explicit provider"),
            ResolveError::UnknownProvider(p) => write!(f, "unknown provider '{p}'"),
        }
    }
}

impl std::error::Error for ResolveError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unambiguous_prefixes_resolve_by_format() {
        assert_eq!(
            provider_from_key("sk-ant-api03-xxxx"),
            KeyGuess::Provider("anthropic")
        );
        assert_eq!(
            provider_from_key("AIzaSyExample"),
            KeyGuess::Provider("google")
        );
    }

    #[test]
    fn sk_prefix_is_ambiguous_not_guessed() {
        // The honest answer: don't guess OpenAI-vs-DeepSeek from the format.
        assert_eq!(provider_from_key("sk-proj-xxxx"), KeyGuess::AmbiguousSk);
        assert_eq!(provider_from_key("garbage"), KeyGuess::Unknown);
    }

    #[test]
    fn env_var_name_disambiguates_sk_keys() {
        assert_eq!(
            resolve_provider("sk-xxxx", None, Some("DEEPSEEK_API_KEY")),
            Ok("deepseek")
        );
        assert_eq!(
            resolve_provider("sk-xxxx", None, Some("OPENAI_API_KEY")),
            Ok("openai")
        );
    }

    #[test]
    fn explicit_hint_wins() {
        assert_eq!(
            resolve_provider("sk-xxxx", Some("deepseek"), None),
            Ok("deepseek")
        );
        assert_eq!(
            resolve_provider("whatever", Some("nope"), None),
            Err(ResolveError::UnknownProvider("nope".into()))
        );
    }

    #[test]
    fn ambiguous_sk_without_hint_is_an_error_not_a_wrong_guess() {
        assert_eq!(
            resolve_provider("sk-xxxx", None, None),
            Err(ResolveError::Ambiguous)
        );
    }

    #[test]
    fn empty_key_is_rejected_even_with_an_explicit_provider() {
        assert_eq!(
            resolve_provider("   ", Some("openai"), None),
            Err(ResolveError::Empty)
        );
    }

    #[test]
    fn admin_key_is_not_an_inference_credential() {
        // Admin/org keys are management credentials, not inference credentials.
        assert_eq!(provider_from_key("sk-ant-admin01-xxxx"), KeyGuess::Unknown);
        assert_eq!(
            resolve_provider("sk-ant-admin01-xxxx", None, None),
            Err(ResolveError::Unrecognized)
        );
    }

    #[test]
    fn openrouter_key_is_unknown_not_ambiguous() {
        // Aggregator keys are neither OpenAI nor DeepSeek, so not `AmbiguousSk`.
        assert_eq!(provider_from_key("sk-or-v1-xxxx"), KeyGuess::Unknown);
    }

    #[test]
    fn key_gir_guclen_flow() {
        // "key gir → güçlen": Anthropic key resolves with no hint at all.
        assert_eq!(
            resolve_provider("sk-ant-api03-xxxx", None, None),
            Ok("anthropic")
        );
        // A Google key too.
        assert_eq!(resolve_provider("AIzaSyX", None, None), Ok("google"));
    }
}
