/// SQL DDL migrations for the chainfold storage schema.
///
/// Tables:
/// - `events`: persisted event payloads, deduplicated by (source_id, cursor_major, cursor_minor)
/// - `snapshots`: state machine snapshots for fast recovery
/// - `anchors`: named cursor bookmarks, upserted by (source_id, name)
/// - `coverage`: fetched cursor ranges, merged on insert
///
/// Index design:
/// - events: B-tree on (source_id, cursor_major, cursor_minor) for range queries + UNIQUE for dedup
/// - snapshots: on (source_id, cursor_major DESC, cursor_minor DESC) for latest-first queries
/// - coverage: on (source_id, range_start_major, range_start_minor) for overlap queries
use rusqlite_migration::{Migrations, M};

pub fn migrations() -> Migrations<'static> {
    Migrations::new(vec![M::up(
        "
        CREATE TABLE IF NOT EXISTS events (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            source_id     TEXT    NOT NULL,
            cursor_major  INTEGER NOT NULL,
            cursor_minor  INTEGER NOT NULL,
            payload       BLOB    NOT NULL,
            UNIQUE(source_id, cursor_major, cursor_minor)
        );

        CREATE INDEX IF NOT EXISTS idx_events_source_cursor
            ON events(source_id, cursor_major, cursor_minor);

        CREATE TABLE IF NOT EXISTS snapshots (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            source_id     TEXT    NOT NULL,
            cursor_major  INTEGER NOT NULL,
            cursor_minor  INTEGER NOT NULL,
            payload       BLOB    NOT NULL,
            created_at    INTEGER NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_snapshots_source_cursor
            ON snapshots(source_id, cursor_major DESC, cursor_minor DESC);

        CREATE TABLE IF NOT EXISTS anchors (
            source_id     TEXT    NOT NULL,
            name          TEXT    NOT NULL,
            cursor_major  INTEGER NOT NULL,
            cursor_minor  INTEGER NOT NULL,
            PRIMARY KEY(source_id, name)
        );

        CREATE TABLE IF NOT EXISTS coverage (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            source_id           TEXT    NOT NULL,
            range_start_major   INTEGER NOT NULL,
            range_start_minor   INTEGER NOT NULL,
            range_end_major     INTEGER NOT NULL,
            range_end_minor     INTEGER NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_coverage_source_start
            ON coverage(source_id, range_start_major, range_start_minor);
        ",
    )])
}
