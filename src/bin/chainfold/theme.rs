//! Terminal themes — the one place SGR escapes are spelled.
//!
//! [`TermTheme`] is a renderer: one method per renderable entity, whose
//! *default* bodies are the plain (escape-free) rendering. A theme
//! overrides the subset it dresses; anything it doesn't know keeps the
//! default — so a new entity kind extends the trait with one defaulted
//! method without touching existing themes, and a new theme overrides
//! without touching entities. [`PlainTheme`] (piped output) is the empty
//! override set.
//!
//! The one rule: overrides *dress*, never reformat — stripping a themed
//! rendering must yield the plain rendering byte-for-byte. Width math and
//! piped output rely on it; `themed_rendering_strips_to_plain` pins it
//! for every theme × entity kind.
//!
//! [`Content`] is what cells, titles, and prose lines hold until render
//! time: a value still typed enough to dispatch to its theme method.

use chainfold::arch::Timestamp;
use chainfold::chain::{BlockNumber, BlockRange};

use crate::table::RenderMode;

/// A terminal rendering of each entity kind the CLI prints. Method
/// defaults are the plain rendering; composite defaults (ranges) call
/// through `self`, so a theme overriding the parts dresses the whole.
pub trait TermTheme {
    /// Bare digits; themes may group thousands.
    fn block_number(&self, n: BlockNumber) -> String {
        n.to_string()
    }

    /// Inclusive `from..=to`, matching Rust range syntax.
    fn block_range(&self, r: BlockRange) -> String {
        format!(
            "{}{}{}",
            self.block_number(r.from),
            self.structural("..="),
            self.block_number(r.to)
        )
    }

    /// ISO-8601 UTC (`2026-07-12T23:41:11Z`).
    fn timestamp(&self, ts: Timestamp) -> String {
        format_timestamp(ts)
    }

    /// Text carrying `0x`-prefixed hex values — tx hashes, addresses,
    /// topics, calldata.
    fn hex(&self, s: &str) -> String {
        s.to_string()
    }

    /// Structural punctuation between values (a range's `..=`).
    fn structural(&self, s: &str) -> String {
        s.to_string()
    }

    // ── Table chrome ──────────────────────────────────────────────

    /// A table title, drawn inline in the top rule.
    fn title(&self, s: &str) -> String {
        s.to_string()
    }

    /// Rule dashes framing a title or heading.
    fn rule(&self, s: &str) -> String {
        s.to_string()
    }

    /// A header row cell.
    fn header(&self, s: &str) -> String {
        s.to_string()
    }

    /// Free-form meta text (decoded event params).
    fn muted(&self, s: &str) -> String {
        s.to_string()
    }

    /// A positive/affirming cell (`IN`).
    fn good(&self, s: &str) -> String {
        s.to_string()
    }

    /// A negative/warning cell (`OUT`).
    fn bad(&self, s: &str) -> String {
        s.to_string()
    }
}

/// Escape-free rendering for pipes — every method keeps its default.
pub struct PlainTheme;

impl TermTheme for PlainTheme {}

/// The default TTY dressing: structural noise (thousands-group
/// alternates, `0x` prefixes, timestamp `T`/`Z`, `..=`) renders dim so
/// the payload stands out; titles italic + cyan, headers bold, good/bad
/// green/red.
pub struct PrettyTheme;

impl TermTheme for PrettyTheme {
    fn block_number(&self, n: BlockNumber) -> String {
        dim_alt_groups(&n.to_string())
    }

    fn timestamp(&self, ts: Timestamp) -> String {
        dim_chars(&format_timestamp(ts), &['T', 'Z'])
    }

    fn hex(&self, s: &str) -> String {
        dim_matches(s, "0x")
    }

    fn structural(&self, s: &str) -> String {
        dim_span(s)
    }

    fn title(&self, s: &str) -> String {
        sgr(s, &format!("{ITALIC}{CYAN}"))
    }

    fn rule(&self, s: &str) -> String {
        sgr(s, DIM)
    }

    fn header(&self, s: &str) -> String {
        sgr(s, BOLD)
    }

    fn muted(&self, s: &str) -> String {
        sgr(s, DIM)
    }

    fn good(&self, s: &str) -> String {
        sgr(s, GREEN)
    }

    fn bad(&self, s: &str) -> String {
        sgr(s, RED)
    }
}

/// The default theme for a render mode. A `--theme` flag or `NO_COLOR`
/// support would hook in here (and thread a theme through `Out`).
pub fn theme_for(mode: RenderMode) -> &'static dyn TermTheme {
    match mode {
        RenderMode::Pretty => &PrettyTheme,
        RenderMode::Plain => &PlainTheme,
    }
}

/// A deferred whole-line composition mixing themed values and plain text.
type ComposeFn = Box<dyn Fn(&dyn TermTheme) -> String>;

/// A renderable value — what cells, titles, and prose lines hold until a
/// theme renders them. `From` impls keep call sites bare: strings pass
/// through untouched, domain values dispatch to their [`TermTheme`]
/// method, closures compose mixed-content lines lazily.
pub enum Content {
    Text(String),
    /// Text carrying `0x` hex values (an abbreviated hash, a topics/data
    /// suffix) — already shaped by `HexDetail`, dressed by the theme.
    Hex(String),
    Number(BlockNumber),
    Range(BlockRange),
    Time(Timestamp),
    Composed(ComposeFn),
}

impl Content {
    pub fn render(&self, theme: &dyn TermTheme) -> String {
        match self {
            Self::Text(s) => s.clone(),
            Self::Hex(s) => theme.hex(s),
            Self::Number(n) => theme.block_number(*n),
            Self::Range(r) => theme.block_range(*r),
            Self::Time(ts) => theme.timestamp(*ts),
            Self::Composed(f) => f(theme),
        }
    }

    /// The escape-free text — what width math measures and pipes carry.
    /// Equal to stripping any theme's rendering (the dress-don't-reformat
    /// rule).
    pub fn plain(&self) -> String {
        self.render(&PlainTheme)
    }
}

impl From<String> for Content {
    fn from(s: String) -> Self {
        Self::Text(s)
    }
}

impl From<&str> for Content {
    fn from(s: &str) -> Self {
        Self::Text(s.to_string())
    }
}

impl From<BlockNumber> for Content {
    fn from(n: BlockNumber) -> Self {
        Self::Number(n)
    }
}

impl From<BlockRange> for Content {
    fn from(r: BlockRange) -> Self {
        Self::Range(r)
    }
}

impl From<Timestamp> for Content {
    fn from(ts: Timestamp) -> Self {
        Self::Time(ts)
    }
}

impl<F: Fn(&dyn TermTheme) -> String + 'static> From<F> for Content {
    fn from(f: F) -> Self {
        Self::Composed(Box::new(f))
    }
}

pub(crate) const BOLD: &str = "\x1b[1m";
pub(crate) const DIM: &str = "\x1b[2m";
/// Normal intensity — ends a [`DIM`] span without clobbering enclosing
/// SGR state, so dim fragments survive inside an italic + cyan title.
pub(crate) const DIM_OFF: &str = "\x1b[22m";
pub(crate) const ITALIC: &str = "\x1b[3m";
pub(crate) const GREEN: &str = "\x1b[32m";
pub(crate) const RED: &str = "\x1b[31m";
pub(crate) const CYAN: &str = "\x1b[36m";
pub(crate) const RESET: &str = "\x1b[0m";

fn sgr(text: &str, code: &str) -> String {
    format!("{code}{text}{RESET}")
}

/// One dim fragment, [`DIM_OFF`]-terminated (see the const's note).
fn dim_span(text: &str) -> String {
    format!("{DIM}{text}{DIM_OFF}")
}

/// Wrap every occurrence of `pat` in a dim span — `0x` prefixes (`x` is
/// not a hex digit, so the pair only ever occurs as a prefix).
fn dim_matches(text: &str, pat: &str) -> String {
    text.split(pat).collect::<Vec<_>>().join(&dim_span(pat))
}

/// Dim each occurrence of the given marker characters — ISO-8601's
/// structural `T` and `Z`.
fn dim_chars(text: &str, marks: &[char]) -> String {
    let mut out = String::new();
    for c in text.chars() {
        if marks.contains(&c) {
            out.push_str(DIM);
            out.push(c);
            out.push_str(DIM_OFF);
        } else {
            out.push(c);
        }
    }
    out
}

/// Dim every other 3-digit group, counted from the right, so the digits
/// read as grouped without separator characters: `22123456` renders as
/// `22` dim(`123`) `456`. The rightmost group is always normal, anchoring
/// the pattern as numbers grow.
fn dim_alt_groups(digits: &str) -> String {
    let chars: Vec<char> = digits.chars().collect();
    let mut out = String::new();
    let mut start = 0;
    while start < chars.len() {
        let rem = chars.len() - start;
        let len = if rem.is_multiple_of(3) { 3 } else { rem % 3 };
        let group: String = chars[start..start + len].iter().collect();
        if ((rem - 1) / 3) % 2 == 1 {
            out.push_str(&dim_span(&group));
        } else {
            out.push_str(&group);
        }
        start += len;
    }
    out
}

pub(crate) fn format_timestamp(ts: Timestamp) -> String {
    chrono::DateTime::from_timestamp(ts.get() as i64, 0).map_or_else(
        || "----------T--:--:--Z".into(),
        |dt| dt.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::strip_ansi;

    #[test]
    fn pretty_groups_thousands() {
        let n = |v: u64| PrettyTheme.block_number(BlockNumber::new(v));
        assert_eq!(n(999), "999");
        assert_eq!(n(1_000), format!("{DIM}1{DIM_OFF}000"));
        assert_eq!(n(22_123_456), format!("22{DIM}123{DIM_OFF}456"));
        assert_eq!(
            n(18_446_744_073_709_551_615),
            format!("18{DIM}446{DIM_OFF}744{DIM}073{DIM_OFF}709{DIM}551{DIM_OFF}615")
        );
    }

    #[test]
    fn pretty_dims_structure() {
        // Range: grouped endpoints joined by a dim `..=`.
        assert_eq!(
            PrettyTheme.block_range(BlockRange::from_u64(1_000, 2_999)),
            format!("{DIM}1{DIM_OFF}000{DIM}..={DIM_OFF}{DIM}2{DIM_OFF}999")
        );
        // Hex: every 0x prefix dims — details cells carry several values.
        assert_eq!(
            PrettyTheme.hex("topics=[0xaa], data=0xbb"),
            format!("topics=[{DIM}0x{DIM_OFF}aa], data={DIM}0x{DIM_OFF}bb")
        );
        // Timestamp: the ISO-8601 T and Z dim.
        assert_eq!(
            PrettyTheme.timestamp(Timestamp::new(1_752_364_871)),
            format!("2025-07-13{DIM}T{DIM_OFF}00:01:11{DIM}Z{DIM_OFF}")
        );
    }

    /// The dress-don't-reformat rule: for every theme and entity kind,
    /// stripping the themed rendering yields the plain rendering.
    #[test]
    fn themed_rendering_strips_to_plain() {
        let samples = [
            Content::Text("plain text".into()),
            Content::Hex("0xdead…beef".into()),
            Content::Number(BlockNumber::new(25_218_798)),
            Content::Range(BlockRange::from_u64(25_218_798, 25_519_987)),
            Content::Time(Timestamp::new(1_752_364_871)),
            Content::Composed(Box::new(|theme| {
                format!("blocks {}", theme.block_number(BlockNumber::new(1_000)))
            })),
        ];
        for theme in [&PlainTheme as &dyn TermTheme, &PrettyTheme] {
            for content in &samples {
                assert_eq!(strip_ansi(&content.render(theme)), content.plain());
            }
        }
    }
}
