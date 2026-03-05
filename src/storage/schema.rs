/// SQL DDL migrations for the chainfold storage schema.
///
/// Migration chain:
/// - v1: original linear schema (events, snapshots, anchors, coverage tables)
/// - v2: block DAG schema (blocks, events, snapshots, canonical_tip tables)
///
/// v1 is preserved for migration chain continuity — rusqlite_migration requires
/// an unbroken sequence of migrations. v2 drops all v1 tables and creates the
/// new block DAG schema.
use rusqlite_migration::{Migrations, M};

pub fn migrations() -> Migrations<'static> {
    Migrations::new(vec![
        // v1 — original linear schema (kept for migration chain continuity)
        M::up(
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
        ),
        // v2 — block DAG schema (replaces all v1 tables)
        M::up(
            "
        DROP TABLE IF EXISTS coverage;
        DROP TABLE IF EXISTS anchors;
        DROP INDEX IF EXISTS idx_events_source_cursor;
        DROP TABLE IF EXISTS events;
        DROP INDEX IF EXISTS idx_snapshots_source_cursor;
        DROP TABLE IF EXISTS snapshots;

        CREATE TABLE blocks (
            hash        BLOB    NOT NULL PRIMARY KEY,
            parent_hash BLOB    NOT NULL,
            height      INTEGER NOT NULL,
            source_id   TEXT    NOT NULL
        );
        CREATE INDEX idx_blocks_source_height ON blocks(source_id, height);
        CREATE INDEX idx_blocks_source_parent ON blocks(source_id, parent_hash);

        CREATE TABLE events (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            block_hash  BLOB    NOT NULL REFERENCES blocks(hash),
            cursor      BLOB    NOT NULL,
            payload     BLOB    NOT NULL
        );
        CREATE INDEX idx_events_block ON events(block_hash);

        CREATE TABLE snapshots (
            source_id   TEXT    NOT NULL,
            block_hash  BLOB    NOT NULL REFERENCES blocks(hash),
            payload     BLOB    NOT NULL,
            PRIMARY KEY (source_id, block_hash)
        );

        CREATE TABLE canonical_tip (
            source_id   TEXT    NOT NULL PRIMARY KEY,
            block_hash  BLOB    NOT NULL REFERENCES blocks(hash)
        );
        ",
        ),
    ])
}
