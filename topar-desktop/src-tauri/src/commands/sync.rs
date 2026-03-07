use crate::db::operations::{MappingRule, SyncLog};
use crate::sync::client::LocalRecordPayload;
use crate::sync::SyncClient;
use crate::AppState;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tauri::State;
use tokio::sync::watch;

lazy_static::lazy_static! {
    static ref ACTIVE_SYNC_CANCEL: Mutex<Option<watch::Sender<bool>>> = Mutex::new(None);
}

struct ActiveSyncGuard;

impl Drop for ActiveSyncGuard {
    fn drop(&mut self) {
        let mut guard = ACTIVE_SYNC_CANCEL.lock();
        *guard = None;
    }
}

fn start_sync_session() -> Result<(watch::Receiver<bool>, ActiveSyncGuard), String> {
    let mut guard = ACTIVE_SYNC_CANCEL.lock();
    if guard.is_some() {
        return Err("Sync is already running".to_string());
    }

    let (tx, rx) = watch::channel(false);
    *guard = Some(tx);

    Ok((rx, ActiveSyncGuard))
}

fn stop_sync_session() {
    let sender = { ACTIVE_SYNC_CANCEL.lock().clone() };
    if let Some(sender) = sender {
        let _ = sender.send(true);
    }
}

fn is_cancelled(cancel_rx: &watch::Receiver<bool>) -> bool {
    *cancel_rx.borrow()
}

#[derive(Debug, Deserialize)]
pub struct CompareRequest {
    pub run_id: String,
    pub rules: HashMap<String, MappingRule>,
}

#[derive(Debug, Serialize)]
pub struct CompareResponse {
    pub total: usize,
    pub new_count: usize,
    pub changed_count: usize,
    pub unchanged_count: usize,
}

#[tauri::command]
pub async fn compare_with_remote(
    state: State<'_, AppState>,
    request: CompareRequest,
) -> Result<CompareResponse, String> {
    // Get local records from current run
    let records = state
        .db
        .get_all_records_by_run(&request.run_id)
        .map_err(|e| format!("Failed to get records: {}", e))?;

    let total = records.len();

    // For local mode: compare with previous runs
    // Get all previous runs
    let all_runs = state
        .db
        .get_all_runs(10)
        .map_err(|e| format!("Failed to get previous runs: {}", e))?;

    let mut remote_products = HashMap::new();

    // Get records from the most recent previous run
    for run in all_runs.iter() {
        if run.id != request.run_id && run.status == "finished" {
            let prev_records = state
                .db
                .get_all_records_by_run(&run.id)
                .unwrap_or_default();

            for record in prev_records {
                // Use source_url as unique identifier
                remote_products.insert(record.source_url.clone(), record.data);
            }

            // Only use the most recent finished run
            if !remote_products.is_empty() {
                break;
            }
        }
    }

    // If no previous data, all products are new
    if remote_products.is_empty() {
        return Ok(CompareResponse {
            total,
            new_count: total,
            changed_count: 0,
            unchanged_count: 0,
        });
    }

    // Compare current records with previous run
    let mut new_count = 0;
    let mut unchanged_count = 0;
    let mut changed_count = 0;

    for record in records.iter() {
        if let Some(prev_data) = remote_products.get(&record.source_url) {
            // Product exists in previous run - check if changed
            if &record.data == prev_data {
                unchanged_count += 1;
            } else {
                changed_count += 1;
            }
        } else {
            // New product
            new_count += 1;
        }
    }

    Ok(CompareResponse {
        total,
        new_count,
        changed_count,
        unchanged_count,
    })
}

#[derive(Debug, Deserialize)]
pub struct SyncRequest {
    pub run_id: String,
    pub rules: HashMap<String, MappingRule>,
    pub mapping_name: String,
    pub save_mapping: bool,
    pub sync_to_eksmo: bool,
    pub sync_to_main: bool,
}

#[derive(Debug, Serialize)]
pub struct SyncResponse {
    pub sync_log_id: String,
    pub total_records: usize,
    pub new_count: usize,
    pub updated_count: usize,
    pub unchanged_count: usize,
    pub failed_count: usize,
}

#[tauri::command]
pub async fn sync_to_database(
    state: State<'_, AppState>,
    request: SyncRequest,
) -> Result<SyncResponse, String> {
    let (mut cancel_rx, _session_guard) = start_sync_session()?;

    let sync_to_eksmo = true;
    let sync_to_main = true;
    let sync_log_id = uuid::Uuid::new_v4().to_string();
    let started_at = chrono::Utc::now();

    if is_cancelled(&cancel_rx) {
        return Err("Sync cancelled".to_string());
    }

    let records = state
        .db
        .get_all_records_by_run(&request.run_id)
        .map_err(|e| format!("Failed to get records: {}", e))?;

    if records.is_empty() {
        return Err("No records found for selected run".to_string());
    }

    const DEFAULT_BACKEND_URL: &str = "https://klasstovar.uz/api";

    let configured_backend_url = state
        .db
        .get_config("backend_url")
        .map_err(|e| format!("Failed to load backend URL: {}", e))?
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());

    let backend_url = match configured_backend_url.as_deref() {
        Some("http://localhost:8090")
        | Some("http://127.0.0.1:8090")
        | Some("http://localhost:8080")
        | Some("http://127.0.0.1:8080") => DEFAULT_BACKEND_URL.to_string(),
        Some(value) => value.to_string(),
        None => DEFAULT_BACKEND_URL.to_string(),
    };

    if configured_backend_url.as_deref() != Some(backend_url.as_str()) {
        let _ = state.db.set_config("backend_url", &backend_url);
    }

    let client = SyncClient::new(backend_url.clone())
        .map_err(|e| format!("Failed to initialize sync client: {}", e))?;

    let mapped_rules: HashMap<String, crate::sync::client::MappingRule> = request
        .rules
        .iter()
        .map(|(target, rule)| {
            (
                target.clone(),
                crate::sync::client::MappingRule {
                    source: rule.source.clone(),
                    constant: rule.constant.clone(),
                },
            )
        })
        .collect();

    let payload_records: Vec<LocalRecordPayload> = records
        .iter()
        .map(|record| LocalRecordPayload {
            source_url: record.source_url.clone(),
            data: record.data.clone(),
        })
        .collect();

    if is_cancelled(&cancel_rx) {
        return Err("Sync cancelled".to_string());
    }

    let remote_result = tokio::select! {
        _ = cancel_rx.changed() => {
            Err(anyhow::anyhow!("Sync cancelled by user"))
        }
        result = client.sync_local_records(
            &request.run_id,
            payload_records,
            mapped_rules,
            request.mapping_name.clone(),
            request.save_mapping,
            sync_to_eksmo,
            sync_to_main,
        ) => result
    };

    let remote_result = match remote_result {
        Ok(result) => result,
        Err(error) => {
            let cancelled = error.to_string().to_lowercase().contains("cancel");
            let failed_log = SyncLog {
                id: sync_log_id.clone(),
                run_id: request.run_id.clone(),
                total_records: if cancelled { 0 } else { records.len() as i64 },
                new_count: 0,
                updated_count: 0,
                unchanged_count: 0,
                failed_count: if cancelled { 0 } else { records.len() as i64 },
                sync_to_eksmo,
                sync_to_main,
                started_at,
                finished_at: Some(chrono::Utc::now()),
                error: Some(error.to_string()),
                details: Some(format!("Backend URL: {}", backend_url)),
            };

            state
                .db
                .insert_sync_log(&failed_log)
                .map_err(|e| format!("Failed to write sync log: {}", e))?;

            if cancelled {
                return Err("Sync cancelled".to_string());
            }
            return Err(format!("Failed to sync to backend: {}", error));
        }
    };

    let total_records = remote_result.total_records;
    let new_count = remote_result.eksmo_upserted + remote_result.main_inserted;
    let updated_count = remote_result.eksmo_modified + remote_result.main_modified;
    let unchanged_count = remote_result.eksmo_skipped + remote_result.main_skipped;
    let failed_count = 0usize;

    let sync_log = SyncLog {
        id: sync_log_id.clone(),
        run_id: request.run_id.clone(),
        total_records: total_records as i64,
        new_count: new_count as i64,
        updated_count: updated_count as i64,
        unchanged_count: unchanged_count as i64,
        failed_count: failed_count as i64,
        sync_to_eksmo,
        sync_to_main,
        started_at,
        finished_at: Some(chrono::Utc::now()),
        error: None,
        details: Some(format!("Synced via backend: {}", backend_url)),
    };

    state
        .db
        .insert_sync_log(&sync_log)
        .map_err(|e| format!("Failed to create sync log: {}", e))?;

    Ok(SyncResponse {
        sync_log_id,
        total_records,
        new_count,
        updated_count,
        unchanged_count,
        failed_count,
    })
}

#[tauri::command]
pub async fn stop_sync() -> Result<(), String> {
    stop_sync_session();
    Ok(())
}
