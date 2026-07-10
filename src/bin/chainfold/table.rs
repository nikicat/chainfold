//! Table rendering funnel — the only module that imports `tabled` or emits
//! raw ANSI (SGR styling, OSC-8 hyperlinks).
//!
//! The boundary-funnel: [`Out::stdout`] / [`Out::stderr`] sniff "is this
//! stream a terminal?" once at the edge (in `main`) and bind the resolved
//! [`RenderMode`] to that stream in one value — a table can never render
//! with one stream's mode and land on the other. All printing goes through
//! [`Out`]; [`TableBuf`] itself is pure data plus `render(mode)`, the seam
//! unit tests use to force either mode.
//!
//! Two render paths share one [`TableSpec`] per table:
//! - [`TableBuf`] buffers rows and renders through `tabled` (one-shot
//!   commands, which are bounded and already collect their rows).
//! - [`RowWriter`] (from [`Out::rows`]) prints one row at a time with fixed
//!   widths for `--follow` streams, which must print append-only as items
//!   arrive. It pads by `char` count of the plain text (pre-ANSI), so
//!   exotic double-width glyphs can misalign a column — same flaw as the
//!   `format!` padding it replaces.

use std::io::IsTerminal;

use tabled::builder::Builder;
use tabled::settings::object::Columns;
use tabled::settings::{Alignment, Padding, Style};
use url::Url;

/// How to render for the target stream: `Pretty` (dimmed rules, italic
/// colored titles, bold headers, colors, OSC-8 links) on terminals, `Plain`
/// (the same layout with no escape bytes at all) when piped.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RenderMode {
    Pretty,
    Plain,
}

impl RenderMode {
    fn for_terminal(is_terminal: bool) -> Self {
        if is_terminal {
            Self::Pretty
        } else {
            Self::Plain
        }
    }
}

/// A destination stream with the render mode resolved *for it*. The only
/// constructors sniff the stream they name, binding mode and fd together;
/// producers take an `Out` and never choose file descriptors themselves.
#[derive(Clone, Copy)]
pub struct Out {
    mode: RenderMode,
    target: Target,
}

#[derive(Clone, Copy)]
enum Target {
    Stdout,
    Stderr,
}

impl Out {
    pub fn stdout() -> Self {
        Self {
            mode: RenderMode::for_terminal(std::io::stdout().is_terminal()),
            target: Target::Stdout,
        }
    }

    pub fn stderr() -> Self {
        Self {
            mode: RenderMode::for_terminal(std::io::stderr().is_terminal()),
            target: Target::Stderr,
        }
    }

    fn write_line(self, line: &str) {
        match self.target {
            Target::Stdout => println!("{line}"),
            Target::Stderr => eprintln!("{line}"),
        }
    }

    /// Render and print a buffered table.
    pub fn table(self, table: &TableBuf) {
        self.write_line(&table.render(self.mode));
    }

    /// A `--follow` row writer bound to this stream and mode.
    pub fn rows(self, spec: TableSpec) -> RowWriter {
        RowWriter { spec, out: self }
    }

    /// Print free-form meta text (lines interleaved with rows, e.g. decoded
    /// event params) — dimmed in `Pretty`, bare in `Plain`.
    pub fn dim_line(self, text: &str) {
        self.write_line(&dim(text, self.mode));
    }

    /// A standalone heading — the same dressing table titles get (a short
    /// rule with the text inline, `── text ──`); for headlines whose table
    /// has no rows.
    pub fn heading(self, text: &str) {
        self.write_line(&top_rule(Some(text), 0, self.mode));
    }

    /// A blank separator line on this stream.
    pub fn blank_line(self) {
        self.write_line("");
    }
}

#[derive(Clone, Copy)]
pub enum Align {
    Left,
    Right,
}

/// Column width policy. `Min(n)` is a floor (content may overflow, never
/// truncates); in follow mode it doubles as the fixed pad width, so specs
/// used with [`RowWriter`] should give every column but the last a `Min`.
#[derive(Clone, Copy)]
pub enum WidthHint {
    Auto,
    Min(usize),
}

/// How column names are cased at render time — presentation, not content:
/// specs declare natural names and this option decides the look, the same
/// in both modes so piped headers stay stable.
#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub enum HeaderCase {
    /// Upper-case headers (`gas used` → `GAS USED`) — the CLI default.
    #[default]
    Upper,
    /// Names as declared — for headers that are identifiers with meaningful
    /// casing (e.g. chainlist's `chainId` / `shortName`).
    AsIs,
}

pub struct Column {
    name: &'static str,
    align: Align,
    width: WidthHint,
}

impl Column {
    pub const fn left(name: &'static str) -> Self {
        Self {
            name,
            align: Align::Left,
            width: WidthHint::Auto,
        }
    }

    pub const fn right(name: &'static str) -> Self {
        Self {
            name,
            align: Align::Right,
            width: WidthHint::Auto,
        }
    }

    pub const fn min(mut self, n: usize) -> Self {
        self.width = WidthHint::Min(n);
        self
    }

    fn cased_name(&self, case: HeaderCase) -> String {
        match case {
            HeaderCase::Upper => self.name.to_uppercase(),
            HeaderCase::AsIs => self.name.to_string(),
        }
    }

    /// Fixed width on the follow path: the `Min` floor, but never narrower
    /// than the (cased) header name.
    fn follow_width(&self, case: HeaderCase) -> usize {
        match self.width {
            WidthHint::Auto => 0,
            WidthHint::Min(n) => n.max(self.cased_name(case).chars().count()),
        }
    }

    /// Header text padded to the `Min` floor (respecting alignment), so the
    /// column can never render narrower than its hint.
    fn header_text(&self, case: HeaderCase) -> String {
        let name = self.cased_name(case);
        let WidthHint::Min(n) = self.width else {
            return name;
        };
        pad(&name, n, self.align)
    }
}

/// Column layout of one table — the single source of truth both render
/// paths consume. Static for fixed tables, runtime-built for erc20's
/// dynamic symbol/decimal widths.
pub struct TableSpec {
    columns: Vec<Column>,
    header_case: HeaderCase,
    header: bool,
}

impl TableSpec {
    pub fn new(columns: Vec<Column>) -> Self {
        Self {
            columns,
            header_case: HeaderCase::default(),
            header: true,
        }
    }

    pub fn header_case(mut self, case: HeaderCase) -> Self {
        self.header_case = case;
        self
    }

    /// Skip the header row — for label/value summaries where column names
    /// would be noise. `Min` width hints are enforced by padding the header
    /// cell, so a headerless [`TableBuf`] ignores them (content width).
    pub fn no_header(mut self) -> Self {
        self.header = false;
        self
    }
}

/// Semantic cell styling; mapped to SGR only in [`RenderMode::Pretty`].
/// (Free-form dim text goes through [`Out::dim_line`], not a cell style.)
#[derive(Clone, Copy, Default)]
pub enum CellStyle {
    #[default]
    Default,
    Good,
    Bad,
}

pub struct Cell {
    text: String,
    url: Option<Url>,
    style: CellStyle,
}

impl Cell {
    pub fn new(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            url: None,
            style: CellStyle::Default,
        }
    }

    /// Attach a hyperlink; rendered as OSC-8 in `Pretty`, dropped in `Plain`.
    pub fn link(mut self, url: Url) -> Self {
        self.url = Some(url);
        self
    }

    pub fn style(mut self, style: CellStyle) -> Self {
        self.style = style;
        self
    }
}

/// One-shot buffered table, rendered through `tabled`.
pub struct TableBuf {
    spec: TableSpec,
    title: Option<String>,
    rows: Vec<Vec<Cell>>,
}

impl TableBuf {
    pub fn new(spec: TableSpec) -> Self {
        Self {
            spec,
            title: None,
            rows: Vec::new(),
        }
    }

    /// Table title: drawn inline in the table's top rule
    /// (`── title ────`) — italic and colored in `Pretty`, bare in `Plain`.
    pub fn title(&mut self, title: impl Into<String>) {
        self.title = Some(title.into());
    }

    pub fn push(&mut self, row: Vec<Cell>) {
        self.rows.push(row);
    }

    pub fn render(&self, mode: RenderMode) -> String {
        let mut builder = Builder::default();
        if self.spec.header {
            builder.push_record(self.spec.columns.iter().map(|c| {
                styled(
                    &c.header_text(self.spec.header_case),
                    CellStyle::Default,
                    mode,
                    BOLD,
                )
            }));
        }
        for row in &self.rows {
            builder.push_record(row.iter().map(|c| materialize(c, mode)));
        }

        let mut table = builder.build();
        for (i, col) in self.spec.columns.iter().enumerate() {
            table.modify(Columns::one(i), alignment(col.align));
        }
        // One layout for both modes, whose only frame is the top rule below —
        // an append-only `--follow` stream can never close a frame, so no
        // table gets more; `Pretty` is dressing (italics, colors, links),
        // never geometry. Stripping the ANSI from a Pretty render yields the
        // Plain render exactly.
        table.with(Style::empty());
        table.with(Padding::new(0, 2, 0, 0));
        table.modify(Columns::last(), Padding::zero());

        // tabled pads every cell to its column width; strip the resulting
        // trailing spaces so plain output stays clean for pipes.
        let rendered = table.to_string();
        let lines: Vec<&str> = rendered.lines().map(str::trim_end).collect();
        let width = lines.iter().copied().map(visible_width).max().unwrap_or(0);
        let mut out = top_rule(self.title.as_deref(), width, mode);
        for line in lines {
            out.push('\n');
            out.push_str(line);
        }
        out
    }
}

/// Append-only row printer for `--follow` streams: fixed widths from the
/// spec, borderless by construction (an unbounded stream has no closing
/// frame). "Pretty follow" keeps the bold header, colors, and links.
/// Created via [`Out::rows`] and prints to that stream itself, so streamed
/// rows can't drift to another fd.
pub struct RowWriter {
    spec: TableSpec,
    out: Out,
}

impl RowWriter {
    pub fn print_header(&self) {
        self.out.write_line(&self.rule());
        self.out.write_line(&self.header());
    }

    /// The stream's top border: a bare rule as wide as the header (titles
    /// don't exist on the follow path).
    fn rule(&self) -> String {
        top_rule(None, visible_width(&self.header()), self.out.mode)
    }

    pub fn print_row(&self, cells: &[Cell]) {
        self.out.write_line(&self.row(cells));
    }

    fn header(&self) -> String {
        let case = self.spec.header_case;
        let cells = self.spec.columns.iter().map(|col| {
            let text = pad(&col.cased_name(case), col.follow_width(case), col.align);
            styled(&text, CellStyle::Default, self.out.mode, BOLD)
        });
        join(cells)
    }

    fn row(&self, cells: &[Cell]) -> String {
        let parts = self.spec.columns.iter().zip(cells).map(|(col, cell)| {
            // Pad outside the ANSI wrapping so link targets and SGR spans
            // cover the text only, and width never counts escape bytes.
            let width = col.follow_width(self.spec.header_case);
            let fill = width.saturating_sub(cell.text.chars().count());
            match col.align {
                Align::Left => format!("{}{}", materialize(cell, self.out.mode), " ".repeat(fill)),
                Align::Right => format!("{}{}", " ".repeat(fill), materialize(cell, self.out.mode)),
            }
        });
        join(parts)
    }
}

/// A table's single border: the top rule, with the title (when present)
/// drawn inline — `── title ────`. `width` is the table's visible width;
/// the trailing run never drops below two dashes, so a title wider than its
/// table (or a bare [`Out::heading`], which passes 0) still reads as a
/// rule. In `Pretty` the dashes are dimmed and the title italic + colored;
/// `Plain` is the same text bare, keeping the strip-ANSI parity.
fn top_rule(title: Option<&str>, width: usize, mode: RenderMode) -> String {
    let Some(title) = title else {
        return dim(&"─".repeat(width), mode);
    };
    let fill = width.saturating_sub(title.chars().count() + 4).max(2);
    format!(
        "{}{}{}",
        dim("── ", mode),
        title_text(title, mode),
        dim(&format!(" {}", "─".repeat(fill)), mode),
    )
}

/// Title dressing: italic + cyan in `Pretty`, bare in `Plain`.
fn title_text(text: &str, mode: RenderMode) -> String {
    match mode {
        RenderMode::Pretty => sgr(text, &format!("{ITALIC}{CYAN}")),
        RenderMode::Plain => text.to_string(),
    }
}

/// Dim free-form text in `Pretty`; pass-through in `Plain`.
fn dim(text: &str, mode: RenderMode) -> String {
    match mode {
        RenderMode::Pretty => sgr(text, DIM),
        RenderMode::Plain => text.to_string(),
    }
}

const BOLD: &str = "\x1b[1m";
const DIM: &str = "\x1b[2m";
const ITALIC: &str = "\x1b[3m";
const GREEN: &str = "\x1b[32m";
const RED: &str = "\x1b[31m";
const CYAN: &str = "\x1b[36m";
const RESET: &str = "\x1b[0m";

fn sgr(text: &str, code: &str) -> String {
    format!("{code}{text}{RESET}")
}

/// Drop ANSI escape sequences (SGR `ESC [ … m` and OSC `ESC ] … BEL`/
/// `ESC \`), keeping the visible text. This module writes every escape it
/// ever needs to measure, so the two forms above are the whole vocabulary.
fn strip_ansi(s: &str) -> String {
    let mut out = String::new();
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '\x1b' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('[') => while chars.next().is_some_and(|c| c != 'm') {},
            Some(']') => {
                while let Some(c) = chars.next() {
                    if c == '\x07' {
                        break;
                    }
                    if c == '\x1b' && chars.peek() == Some(&'\\') {
                        chars.next();
                        break;
                    }
                }
            }
            _ => {}
        }
    }
    out
}

/// Width as the terminal shows it: `char` count of the ANSI-stripped text —
/// same double-width-glyph caveat as the rest of this module's padding.
fn visible_width(line: &str) -> usize {
    strip_ansi(line).chars().count()
}

fn osc8_link(url: &Url, text: &str) -> String {
    format!("\x1b]8;;{url}\x07{text}\x1b]8;;\x07")
}

fn alignment(align: Align) -> Alignment {
    match align {
        Align::Left => Alignment::left(),
        Align::Right => Alignment::right(),
    }
}

fn pad(text: &str, width: usize, align: Align) -> String {
    match align {
        Align::Left => format!("{text:<width$}"),
        Align::Right => format!("{text:>width$}"),
    }
}

fn join(cells: impl Iterator<Item = String>) -> String {
    let line = cells.collect::<Vec<_>>().join("  ");
    line.trim_end().to_string()
}

/// The one place cell text turns into output text: `Plain` is the bare
/// text; `Pretty` wraps it in SGR for its style, then in an OSC-8 link.
/// Both render paths style through here, so one-shot and follow output of
/// the same cell always match.
fn materialize(cell: &Cell, mode: RenderMode) -> String {
    let text = styled(&cell.text, cell.style, mode, "");
    match (&cell.url, mode) {
        (Some(url), RenderMode::Pretty) => osc8_link(url, &text),
        _ => text,
    }
}

fn styled(text: &str, style: CellStyle, mode: RenderMode, extra: &str) -> String {
    if mode == RenderMode::Plain {
        return text.to_string();
    }
    let code = match style {
        CellStyle::Default => "",
        CellStyle::Good => GREEN,
        CellStyle::Bad => RED,
    };
    if code.is_empty() && extra.is_empty() {
        text.to_string()
    } else {
        sgr(text, &format!("{extra}{code}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> TableSpec {
        TableSpec::new(vec![
            Column::right("ID").min(4),
            Column::left("NAME").min(6),
            Column::left("NOTE"),
        ])
    }

    fn url(s: &str) -> Url {
        Url::parse(s).unwrap()
    }

    /// A stdout-bound writer with a forced mode (tests never sniff TTYs).
    fn writer(mode: RenderMode) -> RowWriter {
        Out {
            mode,
            target: Target::Stdout,
        }
        .rows(spec())
    }

    fn sample() -> TableBuf {
        let mut t = TableBuf::new(spec());
        t.push(vec![
            Cell::new("7"),
            Cell::new("alpha").link(url("https://example.com/a")),
            Cell::new("x"),
        ]);
        t.push(vec![
            Cell::new("42"),
            Cell::new("beta").style(CellStyle::Good),
            Cell::new("y"),
        ]);
        t
    }

    #[test]
    fn plain_render_is_exact_and_ansi_free() {
        let mut t = sample();
        t.title("Things");
        let out = t.render(RenderMode::Plain);
        assert!(
            !out.contains('\x1b'),
            "plain output must carry no ANSI: {out:?}"
        );
        assert_eq!(
            out,
            "── Things ────────\n\
             \x20 ID  NAME    NOTE\n\
             \x20  7  alpha   x\n\
             \x20 42  beta    y"
        );
    }

    /// Pretty is dressing over the identical layout — stripping its ANSI
    /// yields the Plain render byte-for-byte. This also pins tabled's
    /// `ansi` feature measuring OSC-8 links and SGR as zero-width: were
    /// they counted, the linked column's padding would differ.
    #[test]
    fn pretty_is_plain_plus_dressing() {
        let mut t = sample();
        t.title("Things");
        let pretty = t.render(RenderMode::Pretty);
        assert_eq!(strip_ansi(&pretty), t.render(RenderMode::Plain));
        assert!(!pretty.contains('│'), "no side borders expected: {pretty}");
        assert!(pretty.contains(&format!(
            "{DIM}── {RESET}{ITALIC}{CYAN}Things{RESET}{DIM} ────────{RESET}"
        )));
        assert!(
            pretty.contains("\x1b]8;;https://example.com/a\x07alpha\x1b]8;;\x07"),
            "expected OSC-8 wrapped cell: {pretty:?}"
        );
        assert!(pretty.contains(&format!("{GREEN}beta{RESET}")));
    }

    #[test]
    fn row_writer_plain_is_exact() {
        let w = writer(RenderMode::Plain);
        assert_eq!(w.rule(), "─".repeat(18));
        assert_eq!(w.header(), "  ID  NAME    NOTE");
        let row = w.row(&[Cell::new("7"), Cell::new("alpha"), Cell::new("x")]);
        assert_eq!(row, "   7  alpha   x");
    }

    #[test]
    fn row_writer_pretty_pads_pre_ansi() {
        let plain = writer(RenderMode::Plain);
        let pretty = writer(RenderMode::Pretty);
        let cells = [
            Cell::new("7").style(CellStyle::Bad),
            Cell::new("alpha").link(url("https://example.com/a")),
            Cell::new("x"),
        ];
        assert_eq!(
            visible_width(&pretty.row(&cells)),
            visible_width(&plain.row(&cells)),
        );
        assert_eq!(visible_width(&pretty.header()), plain.header().len());
        assert_eq!(strip_ansi(&pretty.rule()), plain.rule());
    }

    #[test]
    fn no_header_renders_rule_and_rows() {
        let mut t =
            TableBuf::new(TableSpec::new(vec![Column::left("a"), Column::right("b")]).no_header());
        t.push(vec![Cell::new("x"), Cell::new("1")]);
        assert_eq!(t.render(RenderMode::Plain), "────\nx  1");
        // No header, no title, no styled cells → the dimmed rule is Pretty's
        // only dressing.
        assert_eq!(
            t.render(RenderMode::Pretty),
            format!("{DIM}────{RESET}\nx  1")
        );
    }

    /// The rule's trailing run tracks the table width but never drops below
    /// two dashes — a title wider than its table (and the width-0 heading
    /// path) still reads as a rule.
    #[test]
    fn top_rule_fill_floors_at_two() {
        assert_eq!(
            top_rule(Some("Things"), 18, RenderMode::Plain),
            "── Things ────────"
        );
        assert_eq!(
            top_rule(Some("rpc calls: 0"), 0, RenderMode::Plain),
            "── rpc calls: 0 ──"
        );
    }

    #[test]
    fn header_case_is_style_not_content() {
        let cols = || vec![Column::right("id").min(4), Column::left("name")];
        let out = Out {
            mode: RenderMode::Plain,
            target: Target::Stdout,
        };
        assert_eq!(out.rows(TableSpec::new(cols())).header(), "  ID  NAME");
        assert_eq!(
            out.rows(TableSpec::new(cols()).header_case(HeaderCase::AsIs))
                .header(),
            "  id  name"
        );
    }

    #[test]
    fn materialize_styles_per_mode() {
        let cell = Cell::new("hi")
            .style(CellStyle::Bad)
            .link(url("https://e.com/x"));
        assert_eq!(materialize(&cell, RenderMode::Plain), "hi");
        assert_eq!(
            materialize(&cell, RenderMode::Pretty),
            format!("\x1b]8;;https://e.com/x\x07{RED}hi{RESET}\x1b]8;;\x07")
        );
        assert_eq!(dim("meta", RenderMode::Plain), "meta");
        assert_eq!(dim("meta", RenderMode::Pretty), format!("{DIM}meta{RESET}"));
    }
}
