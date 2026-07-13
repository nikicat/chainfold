//! The redacting [`ApiKey`] newtype and the `--api-key <provider>:<KEY>`
//! funnel ([`ProviderKey`]).

use std::fmt;

use super::ProviderKind;

/// A provider API key. The secret never renders: `Debug` prints
/// `ApiKey(…)`, and there is deliberately **no `Display` impl** — a
/// `%key` tracing field or a stray `format!` is a compile error, which is
/// stronger than redaction. The only way out is the loudly named
/// [`Self::expose`], called where the key becomes a URL (adaptor
/// `endpoint` templates) and where the bin masks `config show` output.
///
/// No serde impls either: the config file speaks raw strings at its own
/// boundary and funnels through [`Self::new`] (a `Serialize` impl would be
/// a standing leak).
#[derive(Clone, PartialEq, Eq)]
pub struct ApiKey(String);

impl ApiKey {
    /// `None` on empty input — an empty key means "unset", matching the
    /// config template's `${ALCHEMY_KEY:-}` semantics.
    pub fn new(raw: impl Into<String>) -> Option<Self> {
        let raw = raw.into();
        if raw.is_empty() {
            None
        } else {
            Some(Self(raw))
        }
    }

    /// The secret itself. Every call site is a deliberate exfiltration
    /// point — keep them countable.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for ApiKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ApiKey(…)")
    }
}

/// One `--api-key` value, funneled at the clap boundary:
/// `<provider>:<key>` → `{ provider, key }`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderKey {
    pub provider: ProviderKind,
    pub key: ApiKey,
}

impl ProviderKey {
    /// The clap `value_parser`. Errors never echo the key — only the
    /// provider token before the colon.
    pub fn parse(raw: &str) -> Result<Self, ParseProviderKeyError> {
        let (provider, key) = raw
            .split_once(':')
            .ok_or(ParseProviderKeyError::MissingSeparator)?;
        let provider: ProviderKind = provider.parse()?;
        let key = ApiKey::new(key).ok_or(ParseProviderKeyError::EmptyKey(provider))?;
        Ok(Self { provider, key })
    }
}

/// A provider name outside the [`ProviderKind`] vocabulary.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unknown provider '{name}' (known: {known})", name = .0, known = known_providers())]
pub struct UnknownProvider(pub(super) String);

/// A malformed `--api-key` value.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ParseProviderKeyError {
    #[error(
        "expected <provider>:<KEY>, e.g. alchemy:YOURKEY (known: {})",
        known_providers()
    )]
    MissingSeparator,
    #[error(transparent)]
    UnknownProvider(#[from] UnknownProvider),
    #[error("empty key for provider '{0}'")]
    EmptyKey(ProviderKind),
}

/// The vocabulary, derived from [`ProviderKind::ALL`] so error messages
/// can't drift from the enum.
fn known_providers() -> String {
    let names: Vec<String> = ProviderKind::ALL.iter().map(ToString::to_string).collect();
    names.join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_never_prints_secret() {
        let key = ApiKey::new("sekrit123").unwrap();
        assert_eq!(format!("{key:?}"), "ApiKey(…)");
        let pk = ProviderKey {
            provider: ProviderKind::Alchemy,
            key,
        };
        assert!(!format!("{pk:?}").contains("sekrit123"));
        // No Display impl exists to test — that absence is the guarantee.
    }

    #[test]
    fn empty_key_means_unset() {
        assert_eq!(ApiKey::new(""), None);
        assert!(ApiKey::new("k").is_some());
    }

    #[test]
    fn provider_key_parses_the_funnel_shape() {
        let pk = ProviderKey::parse("alchemy:abc123").unwrap();
        assert_eq!(pk.provider, ProviderKind::Alchemy);
        assert_eq!(pk.key.expose(), "abc123");
        // Keys may themselves contain colons — only the first splits.
        let pk = ProviderKey::parse("infura:a:b").unwrap();
        assert_eq!(pk.key.expose(), "a:b");
    }

    #[test]
    fn provider_key_parse_errors_do_not_echo_key() {
        let e = ProviderKey::parse("no-separator-sekrit").unwrap_err();
        assert!(matches!(e, ParseProviderKeyError::MissingSeparator));

        let e = ProviderKey::parse("quicknode:sekrit").unwrap_err();
        let rendered = e.to_string();
        assert!(rendered.contains("quicknode"), "{rendered}");
        assert!(!rendered.contains("sekrit"), "{rendered}");
        assert!(
            rendered.contains("alchemy, infura"),
            "the vocabulary rides the error: {rendered}"
        );

        let e = ProviderKey::parse("alchemy:").unwrap_err();
        assert!(matches!(
            e,
            ParseProviderKeyError::EmptyKey(ProviderKind::Alchemy)
        ));
    }
}
