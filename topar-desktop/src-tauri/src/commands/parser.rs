use crate::parser::{ParserEngine, ParserProgress, ParserStatus, progress::ProgressTracker};
use crate::AppState;
use parking_lot::Mutex;
use serde::Deserialize;
use std::sync::Arc;
use tauri::State;

// Global parser state
lazy_static::lazy_static! {
    static ref PARSER_STATE: Arc<Mutex<Option<ParserEngine>>> = Arc::new(Mutex::new(None));
    static ref PROGRESS_TRACKER: ProgressTracker = ProgressTracker::new();
}

#[derive(Debug, Deserialize)]
pub struct StartParsingRequest {
    pub source_url: String,
    #[serde(default)]
    pub limit: usize,
    #[serde(default = "default_workers")]
    pub workers: usize,
    #[serde(default = "default_rps")]
    pub requests_per_sec: f64,
    #[serde(default = "default_max_sitemaps")]
    pub max_sitemaps: usize,
}

fn default_workers() -> usize {
    1
}

fn default_rps() -> f64 {
    3.0
}

fn default_max_sitemaps() -> usize {
    120
}

#[tauri::command]
pub async fn start_parsing(
    state: State<'_, AppState>,
    request: StartParsingRequest,
) -> Result<crate::db::operations::Run, String> {
    // Check if parser is already running
    {
        let mut parser_lock = PARSER_STATE.lock();
        if parser_lock.is_some() {
            let progress = PROGRESS_TRACKER.get();
            if progress.status == ParserStatus::Running {
                return Err("Parser is already running".to_string());
            }
            // Previous parser finished/failed but state wasn't cleared.
            *parser_lock = None;
        }
    }

    // Validate request
    if request.source_url.trim().is_empty() {
        return Err("Source URL is required".to_string());
    }

    let workers = request.workers.clamp(1, 4);
    let rps = request.requests_per_sec.clamp(1.0, 20.0);

    // Create parser engine
    let mut engine = ParserEngine::new(state.db.clone(), PROGRESS_TRACKER.clone());

    // Start parsing
    let parse_request = crate::parser::engine::ParseRequest {
        source_url: request.source_url,
        limit: request.limit,
        workers,
        requests_per_sec: rps,
        max_sitemaps: request.max_sitemaps,
    };

    let run = engine
        .start(parse_request)
        .await
        .map_err(|e| format!("Failed to start parsing: {}", e))?;

    // Store parser engine
    {
        let mut parser_lock = PARSER_STATE.lock();
        *parser_lock = Some(engine);
    }

    Ok(run)
}

#[tauri::command]
pub async fn get_parser_status() -> Result<ParserProgress, String> {
    let progress = PROGRESS_TRACKER.get();

    // Release parser engine once it is no longer running.
    if progress.status != ParserStatus::Running {
        let mut parser_lock = PARSER_STATE.lock();
        *parser_lock = None;
    }

    Ok(progress)
}

#[tauri::command]
pub async fn stop_parsing() -> Result<(), String> {
    let mut parser_lock = PARSER_STATE.lock();
    if let Some(engine) = parser_lock.as_mut() {
        engine.stop()
            .map_err(|e| format!("Failed to stop parser: {}", e))?;
    }
    *parser_lock = None;

    Ok(())
}
