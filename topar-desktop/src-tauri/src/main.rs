// Prevents additional console window on Windows in release, DO NOT REMOVE!!
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod commands;
mod db;
mod parser;
mod sync;

use std::path::PathBuf;
use std::sync::Arc;
use tauri::Manager;

use db::Database;

/// Application state shared across all commands
pub struct AppState {
    pub db: Arc<Database>,
    pub parser_engine_dir: PathBuf,
}

fn main() {
    tauri::Builder::default()
        .setup(|app| {
            // Get app data directory
            let app_data_dir = app
                .path_resolver()
                .app_data_dir()
                .expect("Failed to resolve app data directory");

            // Create directory if it doesn't exist
            std::fs::create_dir_all(&app_data_dir).expect("Failed to create app data directory");

            // Initialize database
            let db_path = app_data_dir.join("topar.db");
            let db = Database::new(db_path)
                .map_err(|e| format!("Failed to initialize database: {:?}", e))
                .expect("Database initialization failed");

            // Store database in app state
            let parser_engine_dir = resolve_parser_engine_dir(app);
            app.manage(AppState {
                db: Arc::new(db),
                parser_engine_dir,
            });

            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            // Database commands
            commands::database::get_all_runs,
            commands::database::get_run_with_records,
            commands::database::delete_run,
            commands::database::get_all_mapping_profiles,
            commands::database::get_mapping_profile,
            commands::database::save_mapping_profile,
            commands::database::delete_mapping_profile,
            commands::database::get_all_sync_logs,
            commands::database::get_config,
            commands::database::set_config,
            // Parser commands
            commands::parser::start_parsing,
            commands::parser::get_parser_status,
            commands::parser::stop_parsing,
            // Sync commands
            commands::sync::compare_with_remote,
            commands::sync::sync_to_database,
            commands::sync::stop_sync,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}

fn resolve_parser_engine_dir(app: &tauri::App) -> PathBuf {
    let resolver = app.path_resolver();

    if let Some(resource_main) = resolver.resolve_resource("parser_engine/main.py") {
        if let Some(dir) = resource_main.parent() {
            if dir.join("main.py").exists() {
                return dir.to_path_buf();
            }
        }
    }

    let dev_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("parser_engine");
    if dev_dir.join("main.py").exists() {
        return dev_dir;
    }

    if let Ok(exe_path) = std::env::current_exe() {
        if let Some(exe_dir) = exe_path.parent() {
            let direct = exe_dir.join("parser_engine");
            if direct.join("main.py").exists() {
                return direct;
            }
            let mac_resources = exe_dir.join("../Resources/parser_engine");
            if mac_resources.join("main.py").exists() {
                return mac_resources;
            }
        }
    }

    panic!("Cannot locate parser_engine directory (main.py not found)")
}
