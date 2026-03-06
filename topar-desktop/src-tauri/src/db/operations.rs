use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, Row, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::HashMap;

use super::Database;

// ============================================================================
// Data Models
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Run {
    pub id: String,
    pub source_url: String,
    pub limit_count: i64,
    pub workers: i64,
    pub requests_per_sec: f64,
    pub max_sitemaps: i64,
    pub discovered_urls: i64,
    pub parsed_products: i64,
    pub rate_limit_retries: i64,
    pub status: String, // "running" | "finished" | "failed"
    pub error: Option<String>,
    pub detected_fields: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    pub id: String,
    pub run_id: String,
    pub source_url: String,
    pub data: HashMap<String, JsonValue>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MappingProfile {
    pub id: String,
    pub name: String,
    pub rules: HashMap<String, MappingRule>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MappingRule {
    #[serde(default)]
    pub source: String,
    #[serde(default)]
    pub constant: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncLog {
    pub id: String,
    pub run_id: String,
    pub total_records: i64,
    pub new_count: i64,
    pub updated_count: i64,
    pub unchanged_count: i64,
    pub failed_count: i64,
    pub sync_to_eksmo: bool,
    pub sync_to_main: bool,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub error: Option<String>,
    pub details: Option<String>,
}

// ============================================================================
// Run Operations
// ============================================================================

impl Database {
    /// Insert a new parsing run
    pub fn insert_run(&self, run: &Run) -> Result<()> {
        self.with_conn(|conn| {
            let detected_fields_json = serde_json::to_string(&run.detected_fields)?;

            conn.execute(
                r#"
                INSERT INTO runs (
                    id, source_url, limit_count, workers, requests_per_sec, max_sitemaps,
                    discovered_urls, parsed_products, rate_limit_retries, status, error,
                    detected_fields, created_at, finished_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
                "#,
                params![
                    run.id,
                    run.source_url,
                    run.limit_count,
                    run.workers,
                    run.requests_per_sec,
                    run.max_sitemaps,
                    run.discovered_urls,
                    run.parsed_products,
                    run.rate_limit_retries,
                    run.status,
                    run.error,
                    detected_fields_json,
                    run.created_at.to_rfc3339(),
                    run.finished_at.map(|dt| dt.to_rfc3339()),
                ],
            ).map(|_| ())?;

            Ok(())
        })
    }

    /// Update run status and metrics
    pub fn update_run(&self, run: &Run) -> Result<()> {
        self.with_conn(|conn| {
            let detected_fields_json = serde_json::to_string(&run.detected_fields)?;

            conn.execute(
                r#"
                UPDATE runs SET
                    discovered_urls = ?1,
                    parsed_products = ?2,
                    rate_limit_retries = ?3,
                    status = ?4,
                    error = ?5,
                    detected_fields = ?6,
                    finished_at = ?7
                WHERE id = ?8
                "#,
                params![
                    run.discovered_urls,
                    run.parsed_products,
                    run.rate_limit_retries,
                    run.status,
                    run.error,
                    detected_fields_json,
                    run.finished_at.map(|dt| dt.to_rfc3339()),
                    run.id,
                ],
            ).map(|_| ())?;

            Ok(())
        })
    }

    /// Get a run by ID
    pub fn get_run(&self, id: &str) -> Result<Option<Run>> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(
                r#"
                SELECT id, source_url, limit_count, workers, requests_per_sec, max_sitemaps,
                       discovered_urls, parsed_products, rate_limit_retries, status, error,
                       detected_fields, created_at, finished_at
                FROM runs WHERE id = ?1
                "#,
            )?;

            let run = stmt
                .query_row(params![id], |row| parse_run_row(row).map_err(|_| rusqlite::Error::InvalidQuery))
                .optional()?;

            Ok(run)
        })
    }

    /// Get all runs ordered by created_at DESC
    pub fn get_all_runs(&self, limit: usize) -> Result<Vec<Run>> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(
                r#"
                SELECT id, source_url, limit_count, workers, requests_per_sec, max_sitemaps,
                       discovered_urls, parsed_products, rate_limit_retries, status, error,
                       detected_fields, created_at, finished_at
                FROM runs
                ORDER BY created_at DESC
                LIMIT ?1
                "#,
            )?;

            let runs = stmt
                .query_map(params![limit], |row| parse_run_row(row).map_err(|_| rusqlite::Error::InvalidQuery))?
                .collect::<std::result::Result<Vec<_>, _>>()?;

            Ok(runs)
        })
    }

    /// Delete a run and all its records (CASCADE)
    pub fn delete_run(&self, id: &str) -> Result<()> {
        self.with_conn(|conn| {
            conn.execute("DELETE FROM runs WHERE id = ?1", params![id]).map(|_| ())?;
            Ok(())
        })
    }
}

fn parse_run_row(row: &Row) -> Result<Run> {
    let detected_fields_json: String = row.get(11)?;
    let detected_fields: Vec<String> = serde_json::from_str(&detected_fields_json)
        .unwrap_or_default();

    let created_at_str: String = row.get(12)?;
    let created_at = DateTime::parse_from_rfc3339(&created_at_str)
        .context("Invalid created_at timestamp")?
        .with_timezone(&Utc);

    let finished_at = row
        .get::<_, Option<String>>(13)?
        .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
        .map(|dt| dt.with_timezone(&Utc));

    Ok(Run {
        id: row.get(0)?,
        source_url: row.get(1)?,
        limit_count: row.get(2)?,
        workers: row.get(3)?,
        requests_per_sec: row.get(4)?,
        max_sitemaps: row.get(5)?,
        discovered_urls: row.get(6)?,
        parsed_products: row.get(7)?,
        rate_limit_retries: row.get(8)?,
        status: row.get(9)?,
        error: row.get(10)?,
        detected_fields,
        created_at,
        finished_at,
    })
}

// ============================================================================
// Record Operations
// ============================================================================

impl Database {
    /// Insert a parsed record
    pub fn insert_record(&self, record: &Record) -> Result<()> {
        self.with_conn(|conn| {
            let data_json = serde_json::to_string(&record.data)?;

            conn.execute(
                r#"
                INSERT INTO records (id, run_id, source_url, data, created_at)
                VALUES (?1, ?2, ?3, ?4, ?5)
                "#,
                params![
                    record.id,
                    record.run_id,
                    record.source_url,
                    data_json,
                    record.created_at.to_rfc3339(),
                ],
            ).map(|_| ())?;

            Ok(())
        })
    }

    /// Insert multiple records in a transaction (batch)
    pub fn insert_records_batch(&self, records: &[Record]) -> Result<()> {
        self.with_conn(|conn| {
            let tx = conn.transaction()?;

            for record in records {
                let data_json = serde_json::to_string(&record.data)?;

                tx.execute(
                    r#"
                    INSERT INTO records (id, run_id, source_url, data, created_at)
                    VALUES (?1, ?2, ?3, ?4, ?5)
                    "#,
                    params![
                        record.id,
                        record.run_id,
                        record.source_url,
                        data_json,
                        record.created_at.to_rfc3339(),
                    ],
                ).map(|_| ())?;
            }

            tx.commit()?;
            Ok(())
        })
    }

    /// Get records for a specific run with pagination
    pub fn get_records_by_run(
        &self,
        run_id: &str,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<Record>> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(
                r#"
                SELECT id, run_id, source_url, data, created_at
                FROM records
                WHERE run_id = ?1
                ORDER BY created_at ASC
                LIMIT ?2 OFFSET ?3
                "#,
            )?;

            let records = stmt
                .query_map(params![run_id, limit, offset], |row| {
                    parse_record_row(row).map_err(|_| rusqlite::Error::InvalidQuery)
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;

            Ok(records)
        })
    }

    /// Get all records for a run (use carefully for large datasets)
    pub fn get_all_records_by_run(&self, run_id: &str) -> Result<Vec<Record>> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(
                r#"
                SELECT id, run_id, source_url, data, created_at
                FROM records
                WHERE run_id = ?1
                ORDER BY created_at ASC
                "#,
            )?;

            let records = stmt
                .query_map(params![run_id], |row| parse_record_row(row).map_err(|_| rusqlite::Error::InvalidQuery))?
                .collect::<std::result::Result<Vec<_>, _>>()?;

            Ok(records)
        })
    }

    /// Count records for a run
    pub fn count_records_by_run(&self, run_id: &str) -> Result<usize> {
        self.with_conn(|conn| {
            let count: i64 = conn.query_row(
                "SELECT COUNT(*) FROM records WHERE run_id = ?1",
                params![run_id],
                |row| row.get(0),
            )?;

            Ok(count as usize)
        })
    }
}

fn parse_record_row(row: &Row) -> Result<Record> {
    let data_json: String = row.get(3)?;
    let data: HashMap<String, JsonValue> = serde_json::from_str(&data_json)
        .context("Failed to parse record data JSON")?;

    let created_at_str: String = row.get(4)?;
    let created_at = DateTime::parse_from_rfc3339(&created_at_str)
        .context("Invalid created_at timestamp")?
        .with_timezone(&Utc);

    Ok(Record {
        id: row.get(0)?,
        run_id: row.get(1)?,
        source_url: row.get(2)?,
        data,
        created_at,
    })
}

// ============================================================================
// Mapping Profile Operations
// ============================================================================

impl Database {
    /// Save a mapping profile
    pub fn insert_mapping_profile(&self, profile: &MappingProfile) -> Result<()> {
        self.with_conn(|conn| {
            let rules_json = serde_json::to_string(&profile.rules)?;

            conn.execute(
                r#"
                INSERT INTO mapping_profiles (id, name, rules, created_at, updated_at)
                VALUES (?1, ?2, ?3, ?4, ?5)
                "#,
                params![
                    profile.id,
                    profile.name,
                    rules_json,
                    profile.created_at.to_rfc3339(),
                    profile.updated_at.to_rfc3339(),
                ],
            ).map(|_| ())?;

            Ok(())
        })
    }

    /// Update a mapping profile
    pub fn update_mapping_profile(&self, profile: &MappingProfile) -> Result<()> {
        self.with_conn(|conn| {
            let rules_json = serde_json::to_string(&profile.rules)?;

            conn.execute(
                r#"
                UPDATE mapping_profiles
                SET name = ?1, rules = ?2, updated_at = ?3
                WHERE id = ?4
                "#,
                params![
                    profile.name,
                    rules_json,
                    profile.updated_at.to_rfc3339(),
                    profile.id,
                ],
            ).map(|_| ())?;

            Ok(())
        })
    }

    /// Get a mapping profile by ID
    pub fn get_mapping_profile(&self, id: &str) -> Result<Option<MappingProfile>> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(
                r#"
                SELECT id, name, rules, created_at, updated_at
                FROM mapping_profiles
                WHERE id = ?1
                "#,
            )?;

            let profile = stmt
                .query_row(params![id], |row| parse_mapping_profile_row(row).map_err(|_| rusqlite::Error::InvalidQuery))
                .optional()?;

            Ok(profile)
        })
    }

    /// Get all mapping profiles ordered by created_at DESC
    pub fn get_all_mapping_profiles(&self, limit: usize) -> Result<Vec<MappingProfile>> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(
                r#"
                SELECT id, name, rules, created_at, updated_at
                FROM mapping_profiles
                ORDER BY created_at DESC
                LIMIT ?1
                "#,
            )?;

            let profiles = stmt
                .query_map(params![limit], |row| parse_mapping_profile_row(row).map_err(|_| rusqlite::Error::InvalidQuery))?
                .collect::<std::result::Result<Vec<_>, _>>()?;

            Ok(profiles)
        })
    }

    /// Delete a mapping profile
    pub fn delete_mapping_profile(&self, id: &str) -> Result<()> {
        self.with_conn(|conn| {
            conn.execute("DELETE FROM mapping_profiles WHERE id = ?1", params![id]).map(|_| ())?;
            Ok(())
        })
    }
}

fn parse_mapping_profile_row(row: &Row) -> Result<MappingProfile> {
    let rules_json: String = row.get(2)?;
    let rules: HashMap<String, MappingRule> = serde_json::from_str(&rules_json)
        .context("Failed to parse mapping rules JSON")?;

    let created_at_str: String = row.get(3)?;
    let created_at = DateTime::parse_from_rfc3339(&created_at_str)
        .context("Invalid created_at timestamp")?
        .with_timezone(&Utc);

    let updated_at_str: String = row.get(4)?;
    let updated_at = DateTime::parse_from_rfc3339(&updated_at_str)
        .context("Invalid updated_at timestamp")?
        .with_timezone(&Utc);

    Ok(MappingProfile {
        id: row.get(0)?,
        name: row.get(1)?,
        rules,
        created_at,
        updated_at,
    })
}

// ============================================================================
// Sync Log Operations
// ============================================================================

impl Database {
    /// Insert a sync log entry
    pub fn insert_sync_log(&self, log: &SyncLog) -> Result<()> {
        self.with_conn(|conn| {
            conn.execute(
                r#"
                INSERT INTO sync_logs (
                    id, run_id, total_records, new_count, updated_count, unchanged_count,
                    failed_count, sync_to_eksmo, sync_to_main, started_at, finished_at,
                    error, details
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
                "#,
                params![
                    log.id,
                    log.run_id,
                    log.total_records,
                    log.new_count,
                    log.updated_count,
                    log.unchanged_count,
                    log.failed_count,
                    log.sync_to_eksmo as i32,
                    log.sync_to_main as i32,
                    log.started_at.to_rfc3339(),
                    log.finished_at.map(|dt| dt.to_rfc3339()),
                    log.error,
                    log.details,
                ],
            ).map(|_| ())?;

            Ok(())
        })
    }

    /// Update sync log when finished
    pub fn update_sync_log(&self, log: &SyncLog) -> Result<()> {
        self.with_conn(|conn| {
            conn.execute(
                r#"
                UPDATE sync_logs SET
                    new_count = ?1,
                    updated_count = ?2,
                    unchanged_count = ?3,
                    failed_count = ?4,
                    finished_at = ?5,
                    error = ?6,
                    details = ?7
                WHERE id = ?8
                "#,
                params![
                    log.new_count,
                    log.updated_count,
                    log.unchanged_count,
                    log.failed_count,
                    log.finished_at.map(|dt| dt.to_rfc3339()),
                    log.error,
                    log.details,
                    log.id,
                ],
            ).map(|_| ())?;

            Ok(())
        })
    }

    /// Get all sync logs ordered by started_at DESC
    pub fn get_all_sync_logs(&self, limit: usize) -> Result<Vec<SyncLog>> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(
                r#"
                SELECT id, run_id, total_records, new_count, updated_count, unchanged_count,
                       failed_count, sync_to_eksmo, sync_to_main, started_at, finished_at,
                       error, details
                FROM sync_logs
                ORDER BY started_at DESC
                LIMIT ?1
                "#,
            )?;

            let logs = stmt
                .query_map(params![limit], |row| parse_sync_log_row(row).map_err(|_| rusqlite::Error::InvalidQuery))?
                .collect::<std::result::Result<Vec<_>, _>>()?;

            Ok(logs)
        })
    }

    /// Get sync logs for a specific run
    pub fn get_sync_logs_by_run(&self, run_id: &str) -> Result<Vec<SyncLog>> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(
                r#"
                SELECT id, run_id, total_records, new_count, updated_count, unchanged_count,
                       failed_count, sync_to_eksmo, sync_to_main, started_at, finished_at,
                       error, details
                FROM sync_logs
                WHERE run_id = ?1
                ORDER BY started_at DESC
                "#,
            )?;

            let logs = stmt
                .query_map(params![run_id], |row| parse_sync_log_row(row).map_err(|_| rusqlite::Error::InvalidQuery))?
                .collect::<std::result::Result<Vec<_>, _>>()?;

            Ok(logs)
        })
    }
}

fn parse_sync_log_row(row: &Row) -> Result<SyncLog> {
    let started_at_str: String = row.get(9)?;
    let started_at = DateTime::parse_from_rfc3339(&started_at_str)
        .context("Invalid started_at timestamp")?
        .with_timezone(&Utc);

    let finished_at = row
        .get::<_, Option<String>>(10)?
        .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
        .map(|dt| dt.with_timezone(&Utc));

    Ok(SyncLog {
        id: row.get(0)?,
        run_id: row.get(1)?,
        total_records: row.get(2)?,
        new_count: row.get(3)?,
        updated_count: row.get(4)?,
        unchanged_count: row.get(5)?,
        failed_count: row.get(6)?,
        sync_to_eksmo: row.get::<_, i32>(7)? != 0,
        sync_to_main: row.get::<_, i32>(8)? != 0,
        started_at,
        finished_at,
        error: row.get(11)?,
        details: row.get(12)?,
    })
}

// ============================================================================
// Config Operations
// ============================================================================

impl Database {
    /// Set a config value
    pub fn set_config(&self, key: &str, value: &str) -> Result<()> {
        self.with_conn(|conn| {
            let now = Utc::now().to_rfc3339();

            conn.execute(
                r#"
                INSERT INTO config (key, value, updated_at)
                VALUES (?1, ?2, ?3)
                ON CONFLICT(key) DO UPDATE SET value = ?2, updated_at = ?3
                "#,
                params![key, value, now],
            )?;

            Ok(())
        })
    }

    /// Get a config value
    pub fn get_config(&self, key: &str) -> Result<Option<String>> {
        self.with_conn(|conn| {
            let value = conn
                .query_row(
                    "SELECT value FROM config WHERE key = ?1",
                    params![key],
                    |row| row.get::<_, String>(0),
                )
                .optional()?;

            Ok(value)
        })
    }

    /// Get all config as a map
    pub fn get_all_config(&self) -> Result<HashMap<String, String>> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare("SELECT key, value FROM config")?;

            let config_map = stmt
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })?
                .collect::<Result<HashMap<_, _>, _>>()?;

            Ok(config_map)
        })
    }

    /// Delete a config key
    pub fn delete_config(&self, key: &str) -> Result<()> {
        self.with_conn(|conn| {
            conn.execute("DELETE FROM config WHERE key = ?1", params![key]).map(|_| ())?;
            Ok(())
        })
    }
}
