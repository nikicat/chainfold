//! `chainfold cache` — manage the local `SQLite` cache: per-table entry
//! counts + on-disk size (`size`), and deletion by section (`purge`).
//!
//! Needs no chain, RPC, or config — only the DB path — which is why it is a
//! top-level command and not part of the `evm` subtree.

use std::fs;
use std::path::{Path, PathBuf};

use bytesize::ByteSize;
use chainfold::evm::{CACHE_VERSION, CacheDb, CacheSection, versioned_path};

use crate::cli::{CacheArgs, CacheCommand, CachePurgeArgs};
use crate::util::db_base_path;

pub fn run(args: &CacheArgs) -> Result<(), Box<dyn std::error::Error>> {
    let base = db_base_path(args.db.as_deref());
    let db_path = versioned_path(&base);
    match &args.command {
        CacheCommand::Size => size(&base, &db_path),
        CacheCommand::Purge(purge_args) => purge(&base, &db_path, purge_args),
    }
}

/// The sections selected by the purge flags, funneled once from clap's raw
/// `bool`s (the [`Direction`](crate::cli::Direction) pattern). No flags
/// selects everything — a CLI-policy default the library deliberately
/// doesn't know about.
fn purge_sections(args: &CachePurgeArgs) -> Vec<CacheSection> {
    let flag = |section: CacheSection| match section {
        CacheSection::History => args.history,
        CacheSection::Erc20 => args.erc20,
        CacheSection::Signatures => args.signatures,
    };
    let picked: Vec<_> = CacheSection::ALL.into_iter().filter(|&s| flag(s)).collect();
    if picked.is_empty() {
        CacheSection::ALL.to_vec()
    } else {
        picked
    }
}

/// Whether the purge is a full wipe (no narrowing flags) — full purges also
/// remove stale older-version cache files.
fn is_full_purge(args: &CachePurgeArgs) -> bool {
    !(args.history || args.erc20 || args.signatures)
}

// ============================================================
// On-disk footprint
// ============================================================

/// On-disk footprint of the cache: the main DB file plus the `-wal`/`-shm`
/// sidecars WAL mode keeps next to it.
struct DbFootprint {
    main: ByteSize,
    sidecars: ByteSize,
}

/// Size of the file at `path`, zero if absent (sidecars come and go).
fn file_size(path: &Path) -> ByteSize {
    ByteSize::b(fs::metadata(path).map_or(0, |m| m.len()))
}

/// `path` with `suffix` appended to the file name (`x.db` → `x.db-wal`).
fn with_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(suffix);
    path.with_file_name(name)
}

impl DbFootprint {
    fn measure(db_path: &Path) -> Self {
        Self {
            main: file_size(db_path),
            sidecars: file_size(&with_suffix(db_path, "-wal"))
                + file_size(&with_suffix(db_path, "-shm")),
        }
    }

    fn total(&self) -> ByteSize {
        self.main + self.sidecars
    }
}

// ============================================================
// Stale older-version files
// ============================================================

/// Cache files of *other* schema versions next to the current one — orphaned
/// whenever [`CACHE_VERSION`] bumps (`versioned_path` switches to a new file
/// name) and never cleaned up by anything else.
fn stale_version_files(base: &Path) -> Vec<(PathBuf, ByteSize)> {
    let dir = match base.parent() {
        Some(p) if !p.as_os_str().is_empty() => p,
        _ => Path::new("."),
    };
    let Some((stem, ext)) = base
        .file_name()
        .and_then(|f| f.to_str())
        .map(|f| f.rsplit_once('.').unwrap_or((f, "")))
    else {
        return Vec::new();
    };
    let Ok(entries) = fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut stale: Vec<_> = entries
        .flatten()
        .filter(|e| {
            e.file_name()
                .to_str()
                .is_some_and(|name| is_stale_version(name, stem, ext))
        })
        .map(|e| (e.path(), file_size(&e.path())))
        .collect();
    stale.sort();
    stale
}

/// Matches `{stem}.v{N}.{ext}` (or the same with a `-wal`/`-shm` sidecar
/// suffix) where `N` is a version other than [`CACHE_VERSION`]. `ext` is
/// `""` for extension-less base paths (`{stem}.v{N}`).
fn is_stale_version(name: &str, stem: &str, ext: &str) -> bool {
    let name = name
        .strip_suffix("-wal")
        .or_else(|| name.strip_suffix("-shm"))
        .unwrap_or(name);
    let Some(rest) = name.strip_prefix(stem).and_then(|r| r.strip_prefix(".v")) else {
        return false;
    };
    let version = if ext.is_empty() {
        rest
    } else {
        match rest.strip_suffix(ext).and_then(|r| r.strip_suffix('.')) {
            Some(v) => v,
            None => return false,
        }
    };
    version.parse::<u32>().is_ok_and(|v| v != CACHE_VERSION)
}

// ============================================================
// Subcommands
// ============================================================

fn size(base: &Path, db_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    if !db_path.exists() {
        println!("no cache at {}", db_path.display());
        return Ok(());
    }
    // Measure before opening — opening (re)creates the WAL sidecars.
    let footprint = DbFootprint::measure(db_path);
    let stats = CacheDb::open(db_path)?.table_stats()?;

    println!("Cache: {}", db_path.display());
    for section in CacheSection::ALL {
        println!();
        println!("  {}", section.name());
        for stat in stats.iter().filter(|s| s.table.section() == section) {
            println!(
                "    {:<22} {:>10} {:>12}",
                stat.table.name(),
                stat.rows,
                stat.bytes
            );
        }
    }
    println!();
    if footprint.sidecars == ByteSize::b(0) {
        println!("  Size:  {}", footprint.total());
    } else {
        println!(
            "  Size:  {}  ({} + {} WAL/SHM)",
            footprint.total(),
            footprint.main,
            footprint.sidecars
        );
    }

    let stale = stale_version_files(base);
    if !stale.is_empty() {
        println!();
        println!("  stale caches (older versions, removed by a full `cache purge`):");
        for (path, size) in &stale {
            println!("    {:<40} {:>10}", path.display(), size);
        }
    }
    Ok(())
}

fn purge(
    base: &Path,
    db_path: &Path,
    args: &CachePurgeArgs,
) -> Result<(), Box<dyn std::error::Error>> {
    if !db_path.exists() {
        println!("no cache at {} — nothing to purge", db_path.display());
        return Ok(());
    }
    let before = DbFootprint::measure(db_path);

    let db = CacheDb::open(db_path)?;
    let report = db.purge(&purge_sections(args))?;
    db.vacuum()?;
    drop(db);

    println!("Purged {}", db_path.display());
    println!();
    // Every in-scope table, zero-row ones included — the scope stays visible.
    for count in &report {
        println!("    {:<22} {:>10} removed", count.table.name(), count.rows);
    }

    if is_full_purge(args) {
        for (path, size) in stale_version_files(base) {
            fs::remove_file(&path)?;
            println!("    removed stale {} ({size})", path.display());
        }
    }

    let after = DbFootprint::measure(db_path);
    println!();
    println!("  Size:  {}  (was {})", after.total(), before.total());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stale_version_matching() {
        // Other versions of the same base match, sidecars included.
        assert!(is_stale_version("chainfold.v2.db", "chainfold", "db"));
        assert!(is_stale_version("chainfold.v2.db-wal", "chainfold", "db"));
        assert!(is_stale_version("chainfold.v2.db-shm", "chainfold", "db"));
        // The current version does not.
        assert!(!is_stale_version("chainfold.v3.db", "chainfold", "db"));
        assert!(!is_stale_version("chainfold.v3.db-wal", "chainfold", "db"));
        // Nor unrelated files, other stems/extensions, or non-numeric tags.
        assert!(!is_stale_version("chainlist.json", "chainfold", "db"));
        assert!(!is_stale_version("other.v2.db", "chainfold", "db"));
        assert!(!is_stale_version("chainfold.v2.sqlite", "chainfold", "db"));
        assert!(!is_stale_version("chainfold.vX.db", "chainfold", "db"));
        // Extension-less base paths ({stem}.v{N}).
        assert!(is_stale_version("chainfold.v1", "chainfold", ""));
        assert!(!is_stale_version("chainfold.v3", "chainfold", ""));
    }

    #[test]
    fn purge_flags_funnel_to_sections() {
        let all = CachePurgeArgs {
            history: false,
            erc20: false,
            signatures: false,
        };
        assert_eq!(purge_sections(&all), CacheSection::ALL.to_vec());

        let narrowed = CachePurgeArgs {
            history: true,
            erc20: false,
            signatures: true,
        };
        assert_eq!(
            purge_sections(&narrowed),
            vec![CacheSection::History, CacheSection::Signatures]
        );
    }
}
