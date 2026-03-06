use anyhow::Result;
use rusqlite::Connection;

use super::Database;

/// Create all database tables and indexes
pub fn create_tables(db: &Database) -> Result<()> {
    db.with_conn(|conn| {
        create_runs_table(conn)?;
        create_records_table(conn)?;
        create_mapping_profiles_table(conn)?;
        create_sync_logs_table(conn)?;
        create_config_table(conn)?;
        Ok(())
    })
}

/// Table: runs - metadata for each parsing execution
fn create_runs_table(conn: &mut Connection) -> Result<()> {
    conn.execute(
        r#"
        CREATE TABLE IF NOT EXISTS runs (
            id TEXT PRIMARY KEY,
            source_url TEXT NOT NULL,
            limit_count INTEGER DEFAULT 0,
            workers INTEGER NOT NULL,
            requests_per_sec REAL NOT NULL,
            max_sitemaps INTEGER DEFAULT 120,
            discovered_urls INTEGER DEFAULT 0,
            parsed_products INTEGER DEFAULT 0,
            rate_limit_retries INTEGER DEFAULT 0,
            status TEXT NOT NULL CHECK(status IN ('running', 'finished', 'failed')),
            error TEXT,
            detected_fields TEXT,
            created_at DATETIME NOT NULL,
            finished_at DATETIME
        )
        "#,
        [],
    ).map(|_| ())?;

    // Index for querying recent runs
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_runs_created_at ON runs(created_at DESC)",
        [],
    ).map(|_| ())?;

    // Index for filtering by status
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status, created_at DESC)",
        [],
    ).map(|_| ())?;

    Ok(())
}

/// Table: records - individual parsed product records
fn create_records_table(conn: &mut Connection) -> Result<()> {
    conn.execute(
        r#"
        CREATE TABLE IF NOT EXISTS records (
            id TEXT PRIMARY KEY,
            run_id TEXT NOT NULL,
            source_url TEXT NOT NULL,
            data TEXT NOT NULL,
            created_at DATETIME NOT NULL,
            FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE
        )
        "#,
        [],
    ).map(|_| ())?;

    // Index for querying by run_id
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_records_run_id ON records(run_id, created_at)",
        [],
    ).map(|_| ())?;

    // Index for querying by source_url
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_records_source_url ON records(run_id, source_url)",
        [],
    ).map(|_| ())?;

    Ok(())
}

/// Table: mapping_profiles - saved field mapping configurations
fn create_mapping_profiles_table(conn: &mut Connection) -> Result<()> {
    conn.execute(
        r#"
        CREATE TABLE IF NOT EXISTS mapping_profiles (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            rules TEXT NOT NULL,
            created_at DATETIME NOT NULL,
            updated_at DATETIME NOT NULL
        )
        "#,
        [],
    ).map(|_| ())?;

    // Index for querying recent mappings
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_mapping_created_at ON mapping_profiles(created_at DESC)",
        [],
    ).map(|_| ())?;

    Ok(())
}

/// Table: sync_logs - history of database sync operations
fn create_sync_logs_table(conn: &mut Connection) -> Result<()> {
    conn.execute(
        r#"
        CREATE TABLE IF NOT EXISTS sync_logs (
            id TEXT PRIMARY KEY,
            run_id TEXT NOT NULL,
            total_records INTEGER NOT NULL,
            new_count INTEGER DEFAULT 0,
            updated_count INTEGER DEFAULT 0,
            unchanged_count INTEGER DEFAULT 0,
            failed_count INTEGER DEFAULT 0,
            sync_to_eksmo INTEGER NOT NULL CHECK(sync_to_eksmo IN (0, 1)),
            sync_to_main INTEGER NOT NULL CHECK(sync_to_main IN (0, 1)),
            started_at DATETIME NOT NULL,
            finished_at DATETIME,
            error TEXT,
            details TEXT,
            FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE
        )
        "#,
        [],
    ).map(|_| ())?;

    // Index for querying sync history
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_sync_logs_started_at ON sync_logs(started_at DESC)",
        [],
    ).map(|_| ())?;

    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_sync_logs_run_id ON sync_logs(run_id)",
        [],
    ).map(|_| ())?;

    Ok(())
}

/// Table: config - application configuration key-value store
fn create_config_table(conn: &mut Connection) -> Result<()> {
    conn.execute(
        r#"
        CREATE TABLE IF NOT EXISTS config (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at DATETIME NOT NULL
        )
        "#,
        [],
    ).map(|_| ())?;

    Ok(())
}
