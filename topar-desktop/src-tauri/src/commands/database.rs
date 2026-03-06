use crate::db::operations::*;
use crate::AppState;
use serde::{Deserialize, Serialize};
use tauri::State;

// ============================================================================
// Run Commands
// ============================================================================

#[derive(Debug, Serialize)]
pub struct RunWithRecords {
    pub run: Run,
    pub records: Vec<Record>,
    pub total_records: usize,
}

#[tauri::command]
pub async fn get_all_runs(
    state: State<'_, AppState>,
    limit: Option<usize>,
) -> Result<Vec<Run>, String> {
    let limit = limit.unwrap_or(50).min(200);

    state
        .db
        .get_all_runs(limit)
        .map_err(|e| format!("Failed to get runs: {}", e))
}

#[tauri::command]
pub async fn get_run_with_records(
    state: State<'_, AppState>,
    run_id: String,
    limit: Option<usize>,
    offset: Option<usize>,
) -> Result<RunWithRecords, String> {
    let limit = limit.unwrap_or(20).min(100);
    let offset = offset.unwrap_or(0);

    let run = state
        .db
        .get_run(&run_id)
        .map_err(|e| format!("Failed to get run: {}", e))?
        .ok_or_else(|| "Run not found".to_string())?;

    let records = state
        .db
        .get_records_by_run(&run_id, limit, offset)
        .map_err(|e| format!("Failed to get records: {}", e))?;

    let total_records = state
        .db
        .count_records_by_run(&run_id)
        .map_err(|e| format!("Failed to count records: {}", e))?;

    Ok(RunWithRecords {
        run,
        records,
        total_records,
    })
}

#[tauri::command]
pub async fn delete_run(
    state: State<'_, AppState>,
    run_id: String,
) -> Result<(), String> {
    state
        .db
        .delete_run(&run_id)
        .map_err(|e| format!("Failed to delete run: {}", e))
}

// ============================================================================
// Mapping Profile Commands
// ============================================================================

#[tauri::command]
pub async fn get_all_mapping_profiles(
    state: State<'_, AppState>,
    limit: Option<usize>,
) -> Result<Vec<MappingProfile>, String> {
    let limit = limit.unwrap_or(50).min(200);

    state
        .db
        .get_all_mapping_profiles(limit)
        .map_err(|e| format!("Failed to get mapping profiles: {}", e))
}

#[derive(Debug, Deserialize)]
pub struct SaveMappingProfileRequest {
    pub name: String,
    pub rules: std::collections::HashMap<String, MappingRule>,
}

#[tauri::command]
pub async fn save_mapping_profile(
    state: State<'_, AppState>,
    request: SaveMappingProfileRequest,
) -> Result<MappingProfile, String> {
    let now = chrono::Utc::now();

    let profile = MappingProfile {
        id: uuid::Uuid::new_v4().to_string(),
        name: request.name,
        rules: request.rules,
        created_at: now,
        updated_at: now,
    };

    state
        .db
        .insert_mapping_profile(&profile)
        .map_err(|e| format!("Failed to save mapping profile: {}", e))?;

    Ok(profile)
}

#[tauri::command]
pub async fn get_mapping_profile(
    state: State<'_, AppState>,
    profile_id: String,
) -> Result<MappingProfile, String> {
    state
        .db
        .get_mapping_profile(&profile_id)
        .map_err(|e| format!("Failed to get mapping profile: {}", e))?
        .ok_or_else(|| "Mapping profile not found".to_string())
}

#[tauri::command]
pub async fn delete_mapping_profile(
    state: State<'_, AppState>,
    profile_id: String,
) -> Result<(), String> {
    state
        .db
        .delete_mapping_profile(&profile_id)
        .map_err(|e| format!("Failed to delete mapping profile: {}", e))
}

// ============================================================================
// Sync Log Commands
// ============================================================================

#[tauri::command]
pub async fn get_all_sync_logs(
    state: State<'_, AppState>,
    limit: Option<usize>,
) -> Result<Vec<SyncLog>, String> {
    let limit = limit.unwrap_or(50).min(200);

    state
        .db
        .get_all_sync_logs(limit)
        .map_err(|e| format!("Failed to get sync logs: {}", e))
}

// ============================================================================
// Config Commands
// ============================================================================

#[tauri::command]
pub async fn get_config(
    state: State<'_, AppState>,
    key: String,
) -> Result<Option<String>, String> {
    state
        .db
        .get_config(&key)
        .map_err(|e| format!("Failed to get config: {}", e))
}

#[tauri::command]
pub async fn set_config(
    state: State<'_, AppState>,
    key: String,
    value: String,
) -> Result<(), String> {
    state
        .db
        .set_config(&key, &value)
        .map_err(|e| format!("Failed to set config: {}", e))
}
