//! Credential-driven capability activation — the agent-native "drop in a key → get stronger"
//! primitive. A credential is detected (by env-var name, or by key-format signature) and
//! resolved to a provider, which unlocks that provider's models and capabilities.
//!
//! Honesty about ambiguity is a feature: OpenAI and DeepSeek both mint `sk-...` keys, so the
//! key format alone cannot tell them apart. We say so ([`KeyGuess::AmbiguousSk`]) instead of
//! guessing wrong — the caller disambiguates with an explicit provider or the env-var name.
//!
//! # Resolution order
//!
//! [`resolve_provider`] tries, in order:
//!
//! 1. an explicit provider name (`provider=` in the Python/Node `add_key`, `explicit` in Rust),
//!    which must be one of [`KNOWN_PROVIDERS`];
//! 2. the conventional environment-variable name the key came from (see
//!    [`provider_from_env_var`] and [`env_vars_for_provider`]);
//! 3. the key's format signature ([`provider_from_key`]).
//!
//! | Provider     | Environment variable(s)              | Detected from key format? |
//! |--------------|--------------------------------------|---------------------------|
//! | `anthropic`  | `ANTHROPIC_API_KEY`                  | yes (`sk-ant-`, not `sk-ant-admin`) |
//! | `openai`     | `OPENAI_API_KEY`                     | no (`sk-` is shared)      |
//! | `google`     | `GOOGLE_API_KEY`, `GEMINI_API_KEY`   | yes (`AIza`)              |
//! | `deepseek`   | `DEEPSEEK_API_KEY`                   | no (`sk-` is shared)      |
//! | `openrouter` | `OPENROUTER_API_KEY`                 | no                        |
//! | `groq`       | `GROQ_API_KEY`                       | no                        |
//! | `mistral`    | `MISTRAL_API_KEY`                    | no                        |
//! | `xai`        | `XAI_API_KEY`                        | no                        |
//!
//! When both Google variables are set, [`crate::agent::Agent::from_env`] prefers
//! `GOOGLE_API_KEY`.
//!
//! # Errors never echo the key
//!
//! [`ResolveError`] messages name the provider, the environment variables and the option to
//! set, but never include the credential value itself, so they are safe to log or show.

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

/// Map a conventional environment-variable name to its provider (exact, case-sensitive match). This is the reliable
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

/// The conventional environment variables aikit reads for `provider`, in the order they are
/// documented (for Google, `GOOGLE_API_KEY` takes precedence when both are set). Returns an
/// empty slice for a name outside [`KNOWN_PROVIDERS`]. This is the inverse of
/// [`provider_from_env_var`] and is what error messages use to tell the user what to set.
///
/// ```
/// use aikit_core::credentials::env_vars_for_provider;
/// assert_eq!(env_vars_for_provider("deepseek"), ["DEEPSEEK_API_KEY"]);
/// assert_eq!(env_vars_for_provider("google"), ["GOOGLE_API_KEY", "GEMINI_API_KEY"]);
/// assert!(env_vars_for_provider("nope").is_empty());
/// ```
pub fn env_vars_for_provider(provider: &str) -> &'static [&'static str] {
    match provider {
        "anthropic" => &["ANTHROPIC_API_KEY"],
        "openai" => &["OPENAI_API_KEY"],
        "deepseek" => &["DEEPSEEK_API_KEY"],
        "google" => &["GOOGLE_API_KEY", "GEMINI_API_KEY"],
        "openrouter" => &["OPENROUTER_API_KEY"],
        "groq" => &["GROQ_API_KEY"],
        "mistral" => &["MISTRAL_API_KEY"],
        "xai" => &["XAI_API_KEY"],
        _ => &[],
    }
}

/// Resolve a credential to a provider using an optional explicit hint, then the env-var name,
/// then the key format. Returns `Err` when the result is genuinely ambiguous or unknown so the
/// agent can ask rather than mis-route the key.
///
/// * `key` — the raw credential; surrounding whitespace is ignored, an empty value is
///   [`ResolveError::Empty`] even when a provider is given.
/// * `explicit` — a provider name from [`KNOWN_PROVIDERS`] (exact, lowercase). When present it
///   wins outright and the key format is not inspected.
/// * `env_var` — the variable the key was read from, e.g. `Some("DEEPSEEK_API_KEY")`. An
///   unrecognized name is ignored and resolution falls through to the key format.
///
/// ```
/// use aikit_core::credentials::{resolve_provider, ResolveError};
/// assert_eq!(resolve_provider("sk-ant-api03-example", None, None), Ok("anthropic"));
/// assert_eq!(resolve_provider("sk-example", None, Some("DEEPSEEK_API_KEY")), Ok("deepseek"));
/// assert_eq!(resolve_provider("sk-example", Some("openai"), None), Ok("openai"));
/// assert_eq!(resolve_provider("sk-example", None, None), Err(ResolveError::Ambiguous));
/// ```
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

/// Why a credential could not be routed to a provider.
///
/// The `Display` text is user-facing (Python raises it as `ValueError`, Node throws it): each
/// message says what to set — a provider name or an environment variable — and how to check the
/// result. No variant carries or prints the credential value.
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

/// How a user can confirm which providers are active after fixing a credential problem.
const VERIFY_HINT: &str =
    "run `aikit providers` (or call active_providers() in Python, activeProviders() in Node) to \
     confirm which providers are active";

impl std::fmt::Display for ResolveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResolveError::Empty => write!(
                f,
                "credential is empty or whitespace-only; pass the provider's API key value \
                 (for example the contents of ANTHROPIC_API_KEY or OPENAI_API_KEY), then {VERIFY_HINT}"
            ),
            ResolveError::Ambiguous => write!(
                f,
                "sk- key is ambiguous (OpenAI or DeepSeek both issue sk- keys); pass \
                 provider=\"openai\" or provider=\"deepseek\", or export it as OPENAI_API_KEY or \
                 DEEPSEEK_API_KEY, then {VERIFY_HINT}"
            ),
            ResolveError::Unrecognized => write!(
                f,
                "unrecognized key format (only Anthropic sk-ant- and Google AIza keys are detected \
                 from the key alone; Anthropic admin keys are not inference keys); pass \
                 provider=\"<name>\" with one of: {providers}, or export the key under its \
                 provider's variable ({vars}), then {VERIFY_HINT}",
                providers = KNOWN_PROVIDERS.join(", "),
                vars = all_env_vars().join(", "),
            ),
            ResolveError::UnknownProvider(p) if looks_like_credential(p) => write!(
                f,
                "unknown provider '<redacted>': the provider argument looks like an API key \
                 (were the key and provider arguments swapped?); expected one of: {}",
                KNOWN_PROVIDERS.join(", ")
            ),
            ResolveError::UnknownProvider(p) => write!(
                f,
                "unknown provider '{p}'; expected one of: {} (exact, lowercase)",
                KNOWN_PROVIDERS.join(", ")
            ),
        }
    }
}

/// A provider name is short and lowercase-ish; anything key-shaped is redacted from messages so a
/// swapped `add_key(provider, key)` call cannot echo the secret.
fn looks_like_credential(value: &str) -> bool {
    let value = value.trim();
    value.len() > 32
        || provider_from_key(value) != KeyGuess::Unknown
        || value.starts_with("sk-")
        || value.starts_with("gsk_")
        || !value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.')
}

fn all_env_vars() -> Vec<&'static str> {
    KNOWN_PROVIDERS
        .iter()
        .flat_map(|provider| env_vars_for_provider(provider).iter().copied())
        .collect()
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
    fn env_var_table_round_trips_for_every_known_provider() {
        for &provider in KNOWN_PROVIDERS {
            let vars = env_vars_for_provider(provider);
            assert!(!vars.is_empty(), "{provider} has no documented env var");
            for var in vars {
                assert_eq!(provider_from_env_var(var), Some(provider), "{var}");
            }
        }
        assert!(env_vars_for_provider("unknown").is_empty());
    }

    #[test]
    fn error_messages_name_the_fix_and_never_echo_the_key() {
        let secret = "sk-proj-SECRETVALUE123";
        let ambiguous = resolve_provider(secret, None, None)
            .unwrap_err()
            .to_string();
        assert!(ambiguous.contains("provider=\"openai\""), "{ambiguous}");
        assert!(ambiguous.contains("DEEPSEEK_API_KEY"), "{ambiguous}");
        assert!(ambiguous.contains("aikit providers"), "{ambiguous}");
        assert!(!ambiguous.contains("SECRETVALUE"), "{ambiguous}");

        let unrecognized = resolve_provider("opaque-SECRETVALUE", None, None)
            .unwrap_err()
            .to_string();
        assert!(unrecognized.contains("openrouter"), "{unrecognized}");
        assert!(unrecognized.contains("GROQ_API_KEY"), "{unrecognized}");
        assert!(!unrecognized.contains("SECRETVALUE"), "{unrecognized}");

        let unknown = resolve_provider(secret, Some("OpenAI"), None)
            .unwrap_err()
            .to_string();
        assert!(unknown.contains("'OpenAI'"), "{unknown}");
        assert!(unknown.contains("openai"), "{unknown}");
        assert!(!unknown.contains("SECRETVALUE"), "{unknown}");

        assert!(ResolveError::Empty.to_string().contains("empty"));

        // Swapped arguments: the "provider" is actually a key and must not be echoed.
        let swapped = resolve_provider("openai", Some(secret), None)
            .unwrap_err()
            .to_string();
        assert!(swapped.contains("<redacted>"), "{swapped}");
        assert!(swapped.contains("swapped"), "{swapped}");
        assert!(!swapped.contains("SECRETVALUE"), "{swapped}");
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
