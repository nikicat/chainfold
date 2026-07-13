//! The redacting [`ApiKey`] newtype (and the [`ProviderKind`] name-parse
//! error, kept beside the vocabulary helper it renders).

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

/// A provider name outside the [`ProviderKind`] vocabulary — the
/// `FromStr` error behind `chains list --provider` and any other place a
/// provider is named by string.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unknown provider '{name}' (known: {known})", name = .0, known = known_providers())]
pub struct UnknownProvider(pub(super) String);

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
        // No Display impl exists to test — that absence is the guarantee.
    }

    #[test]
    fn empty_key_means_unset() {
        assert_eq!(ApiKey::new(""), None);
        assert!(ApiKey::new("k").is_some());
    }

    #[test]
    fn unknown_provider_error_carries_the_vocabulary() {
        let e = "quicknode".parse::<ProviderKind>().unwrap_err();
        let rendered = e.to_string();
        assert!(rendered.contains("quicknode"), "{rendered}");
        assert!(
            rendered.contains("alchemy, infura"),
            "the vocabulary rides the error: {rendered}"
        );
    }
}
