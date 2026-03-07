use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use super::progress::{ParserStatus, ProgressTracker};
use crate::db::operations::{Record, Run};
use crate::db::Database;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParseRequest {
    pub source_url: String,
    pub limit: usize,
    pub workers: usize,
    pub requests_per_sec: f64,
    pub max_sitemaps: usize,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum ParserMessage {
    Progress {
        event: String,
        data: HashMap<String, JsonValue>,
    },
    Error {
        error: String,
        details: Option<HashMap<String, JsonValue>>,
    },
    Result {
        data: ParserResult,
    },
}

#[derive(Debug, Deserialize)]
struct ParserResult {
    discovered_urls: usize,
    parsed_products: usize,
    rate_limit_retries: usize,
    detected_fields: Vec<String>,
    records: Vec<ParsedRecord>,
    #[serde(default = "default_true")]
    completed: bool,
    error: Option<String>,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Deserialize)]
struct ParsedRecord {
    source_url: String,
    #[serde(flatten)]
    data: HashMap<String, JsonValue>,
}

pub struct ParserEngine {
    db: Arc<Database>,
    progress: ProgressTracker,
    parser_engine_dir: PathBuf,
    parser_runtime_dir: PathBuf,
    python_process: Option<Child>,
    current_run_id: Option<String>,
}

impl ParserEngine {
    pub fn new(
        db: Arc<Database>,
        progress: ProgressTracker,
        parser_engine_dir: PathBuf,
        parser_runtime_dir: PathBuf,
    ) -> Self {
        Self {
            db,
            progress,
            parser_engine_dir,
            parser_runtime_dir,
            python_process: None,
            current_run_id: None,
        }
    }

    /// Start parsing asynchronously
    pub async fn start(&mut self, request: ParseRequest) -> Result<Run> {
        // Reset progress
        self.progress.reset();
        self.progress.update(|p| {
            p.status = ParserStatus::Running;
        });

        // Create run record
        let run_id = uuid::Uuid::new_v4().to_string();
        let run = Run {
            id: run_id.clone(),
            source_url: request.source_url.clone(),
            limit_count: request.limit as i64,
            workers: request.workers as i64,
            requests_per_sec: request.requests_per_sec,
            max_sitemaps: request.max_sitemaps as i64,
            discovered_urls: 0,
            parsed_products: 0,
            rate_limit_retries: 0,
            status: "running".to_string(),
            error: None,
            detected_fields: Vec::new(),
            created_at: chrono::Utc::now(),
            finished_at: None,
        };

        // Save to database
        self.db
            .insert_run(&run)
            .context("Failed to save run to database")?;
        self.current_run_id = Some(run_id.clone());

        // Spawn Python parser process
        let parser_dir = self.parser_engine_dir.clone();
        let parser_script = parser_dir.join("main.py");
        if !parser_script.exists() {
            return Err(anyhow::anyhow!(
                "Parser script not found at {}",
                parser_script.display()
            ));
        }

        // Prefer persistent runtime venv in app data; bootstrap it if deps are missing.
        let python_cmd = self.prepare_python_runtime(&parser_dir)?;
        let browsers_path = self.parser_runtime_dir.join("playwright");

        let mut child = Command::new(python_cmd)
            .arg(&parser_script)
            .arg("parse")
            .arg("--source-url")
            .arg(&request.source_url)
            .arg("--limit")
            .arg(request.limit.to_string())
            .arg("--workers")
            .arg(request.workers.to_string())
            .arg("--requests-per-sec")
            .arg(request.requests_per_sec.to_string())
            .arg("--max-sitemaps")
            .arg(request.max_sitemaps.to_string())
            .current_dir(&parser_dir)
            .env("PYTHONPATH", &parser_dir)
            .env("PLAYWRIGHT_BROWSERS_PATH", &browsers_path)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .context("Failed to spawn Python parser process")?;

        // Read output in background thread
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow::anyhow!("Failed to capture stdout"))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| anyhow::anyhow!("Failed to capture stderr"))?;

        let db = self.db.clone();
        let progress = self.progress.clone();
        let run_id_clone = run_id.clone();
        let db_err = self.db.clone();
        let progress_err = self.progress.clone();
        let run_id_err = run_id.clone();

        tokio::spawn(async move {
            let reader = BufReader::new(stderr);
            let mut lines: Vec<String> = Vec::new();

            for line in reader.lines() {
                let line = match line {
                    Ok(l) => l.trim().to_string(),
                    Err(_) => break,
                };
                if line.is_empty() {
                    continue;
                }
                lines.push(line);
                if lines.len() > 20 {
                    lines.remove(0);
                }
            }

            if lines.is_empty() {
                return;
            }

            let snapshot = progress_err.get();
            if snapshot.status != ParserStatus::Running {
                return;
            }

            let message = lines
                .last()
                .cloned()
                .unwrap_or_else(|| "Parser process failed".to_string());
            progress_err.update(|p| {
                if p.status == ParserStatus::Running {
                    p.status = ParserStatus::Failed;
                    p.error = Some(message.clone());
                }
            });

            if let Ok(Some(mut run)) = db_err.get_run(&run_id_err) {
                if run.status == "running" {
                    run.status = "failed".to_string();
                    run.error = Some(lines.join(" | "));
                    run.finished_at = Some(chrono::Utc::now());
                    let _ = db_err.update_run(&run);
                }
            }
        });

        tokio::spawn(async move {
            let reader = BufReader::new(stdout);

            for line in reader.lines() {
                let line = match line {
                    Ok(l) => l,
                    Err(_) => break,
                };

                // Parse JSON message
                if let Ok(msg) = serde_json::from_str::<ParserMessage>(&line) {
                    match msg {
                        ParserMessage::Progress { event, data } => {
                            Self::handle_progress_event(&progress, &event, &data);
                        }
                        ParserMessage::Error { error, .. } => {
                            progress.update(|p| {
                                p.status = ParserStatus::Failed;
                                p.error = Some(error.clone());
                            });

                            // Update run in database
                            if let Ok(Some(mut run)) = db.get_run(&run_id_clone) {
                                run.status = "failed".to_string();
                                run.error = Some(error);
                                run.finished_at = Some(chrono::Utc::now());
                                let _ = db.update_run(&run);
                            }
                        }
                        ParserMessage::Result { data } => {
                            let completed = data.completed;
                            let error_message = data.error.clone();

                            // Save records to database
                            let _ = Self::save_results(&db, &run_id_clone, data).await;

                            progress.update(|p| {
                                if completed {
                                    p.status = ParserStatus::Finished;
                                    p.error = None;
                                } else {
                                    p.status = ParserStatus::Failed;
                                    p.error = Some(error_message.unwrap_or_else(|| {
                                        "Parsing cancelled by user".to_string()
                                    }));
                                }
                            });
                        }
                    }
                }
            }
        });

        self.python_process = Some(child);

        Ok(run)
    }

    /// Stop running parser process and mark run as cancelled.
    pub fn stop(&mut self) -> Result<()> {
        let Some(mut child) = self.python_process.take() else {
            self.current_run_id = None;
            return Ok(());
        };

        let run_id = self.current_run_id.clone();
        let already_exited = matches!(child.try_wait(), Ok(Some(_)));
        let interrupt_attempted = if already_exited {
            true
        } else {
            Self::interrupt_process(&mut child)
        };
        let exited = already_exited || Self::wait_for_exit(&mut child, Duration::from_secs(3));

        if !exited {
            let _ = child.kill();
            let _ = child.wait();
        }

        let snapshot = self.progress.get();
        if let Some(run_id) = run_id.as_deref() {
            if let Ok(Some(mut run)) = self.db.get_run(run_id) {
                if run.status != "running" {
                    self.current_run_id = None;
                    return Ok(());
                }

                run.discovered_urls = snapshot.discovered_urls as i64;
                run.parsed_products = snapshot.parsed_products as i64;
                run.rate_limit_retries = snapshot.rate_limit_retries as i64;
                run.status = "failed".to_string();
                run.error = Some(if interrupt_attempted {
                    "Parsing cancelled by user".to_string()
                } else {
                    "Failed to interrupt parser process".to_string()
                });
                run.finished_at = Some(chrono::Utc::now());
                self.db
                    .update_run(&run)
                    .context("Failed to update cancelled run")?;
            }
        }

        self.progress.update(|p| {
            p.status = ParserStatus::Failed;
            p.error = Some(if interrupt_attempted {
                "Parsing cancelled by user".to_string()
            } else {
                "Failed to interrupt parser process".to_string()
            });
        });

        self.current_run_id = None;

        Ok(())
    }

    fn wait_for_exit(child: &mut Child, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            match child.try_wait() {
                Ok(Some(_)) => return true,
                Ok(None) => {
                    if Instant::now() >= deadline {
                        return false;
                    }
                    thread::sleep(Duration::from_millis(80));
                }
                Err(_) => return false,
            }
        }
    }

    #[cfg(unix)]
    fn interrupt_process(child: &mut Child) -> bool {
        let pid = child.id().to_string();
        Command::new("kill")
            .args(["-s", "INT", &pid])
            .status()
            .map(|status| status.success())
            .unwrap_or(false)
    }

    #[cfg(not(unix))]
    fn interrupt_process(child: &mut Child) -> bool {
        child.kill().is_ok()
    }

    fn handle_progress_event(
        progress: &ProgressTracker,
        event: &str,
        data: &HashMap<String, JsonValue>,
    ) {
        match event {
            "discovering_urls" | "checking_sitemap" | "crawling_started" => {
                progress.update(|p| {
                    if let Some(url) = data.get("source").or_else(|| data.get("url")) {
                        p.current_url = Some(url.as_str().unwrap_or("").to_string());
                    }
                });
            }
            "urls_discovered" => {
                if let Some(count) = data.get("count").and_then(|v| v.as_u64()) {
                    progress.update(|p| {
                        p.discovered_urls = count as usize;
                    });
                }
            }
            "parsing_started" => {
                if let Some(total) = data.get("total_urls").and_then(|v| v.as_u64()) {
                    progress.update(|p| {
                        p.discovered_urls = total as usize;
                    });
                }
            }
            "product_parsed" => {
                if let Some(total) = data.get("total").and_then(|v| v.as_u64()) {
                    progress.update(|p| {
                        p.parsed_products = total as usize;
                        if let Some(url) = data.get("url").and_then(|v| v.as_str()) {
                            p.current_url = Some(url.to_string());
                        }
                    });
                }
            }
            "rate_limited" => {
                progress.update(|p| {
                    p.rate_limit_retries += 1;
                });
            }
            _ => {}
        }
    }

    async fn save_results(db: &Arc<Database>, run_id: &str, result: ParserResult) -> Result<()> {
        // Convert parsed records to Record model
        let records: Vec<Record> = result
            .records
            .into_iter()
            .map(|r| Record {
                id: uuid::Uuid::new_v4().to_string(),
                run_id: run_id.to_string(),
                source_url: r.source_url,
                data: r.data,
                created_at: chrono::Utc::now(),
            })
            .collect();

        // Save records in batches
        const BATCH_SIZE: usize = 250;
        for chunk in records.chunks(BATCH_SIZE) {
            db.insert_records_batch(chunk)
                .context("Failed to insert records batch")?;
        }

        // Update run with final stats
        if let Ok(Some(mut run)) = db.get_run(run_id) {
            run.discovered_urls = result.discovered_urls as i64;
            run.parsed_products = result.parsed_products as i64;
            run.rate_limit_retries = result.rate_limit_retries as i64;
            run.detected_fields = result.detected_fields;
            if result.completed {
                run.status = "finished".to_string();
                run.error = None;
            } else {
                run.status = "failed".to_string();
                run.error = Some(
                    result
                        .error
                        .unwrap_or_else(|| "Parsing cancelled by user".to_string()),
                );
            }
            run.finished_at = Some(chrono::Utc::now());

            db.update_run(&run).context("Failed to update run")?;
        }

        Ok(())
    }

    fn find_python_command(&self, parser_dir: &Path) -> Result<String> {
        let runtime_candidates = vec![
            self.parser_runtime_dir.join("venv").join("bin").join("python"),
            self.parser_runtime_dir.join("venv").join("bin").join("python3"),
            self.parser_runtime_dir
                .join("venv")
                .join("Scripts")
                .join("python.exe"),
        ];
        for candidate in runtime_candidates {
            if candidate.exists()
                && Command::new(&candidate)
                    .arg("--version")
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status()
                    .map(|status| status.success())
                    .unwrap_or(false)
            {
                return Ok(candidate.to_string_lossy().to_string());
            }
        }

        let venv_candidates = vec![
            parser_dir.join("venv").join("bin").join("python"),
            parser_dir.join("venv").join("bin").join("python3"),
            parser_dir.join("venv").join("Scripts").join("python.exe"),
        ];

        for candidate in venv_candidates {
            if candidate.exists()
                && Command::new(&candidate)
                    .arg("--version")
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status()
                    .map(|status| status.success())
                    .unwrap_or(false)
            {
                return Ok(candidate.to_string_lossy().to_string());
            }
        }

        // Try common Python commands
        let candidates = vec!["python3", "python"];

        for cmd in candidates {
            if Command::new(cmd).arg("--version").output().is_ok() {
                return Ok(cmd.to_string());
            }
        }

        Err(anyhow::anyhow!(
            "Python 3 not found. Please install Python 3.11 or later."
        ))
    }

    fn find_system_python_command(&self) -> Result<String> {
        let candidates = vec!["python3", "python"];
        for cmd in candidates {
            if Command::new(cmd)
                .arg("--version")
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .map(|status| status.success())
                .unwrap_or(false)
            {
                return Ok(cmd.to_string());
            }
        }
        Err(anyhow::anyhow!(
            "Python 3 not found. Please install Python 3.11 or later."
        ))
    }

    fn prepare_python_runtime(&self, parser_dir: &Path) -> Result<String> {
        let python_cmd = self.find_python_command(parser_dir)?;
        if self.verify_python_runtime(&python_cmd, parser_dir).is_ok() {
            return Ok(python_cmd);
        }

        self.bootstrap_parser_runtime(parser_dir)?;
        let runtime_python = self.find_python_command(parser_dir)?;
        self.verify_python_runtime(&runtime_python, parser_dir)?;
        Ok(runtime_python)
    }

    fn bootstrap_parser_runtime(&self, parser_dir: &Path) -> Result<()> {
        std::fs::create_dir_all(&self.parser_runtime_dir)
            .context("Failed to create parser runtime directory")?;

        let system_python = self.find_system_python_command()?;
        let runtime_venv = self.parser_runtime_dir.join("venv");
        let runtime_python = if cfg!(windows) {
            runtime_venv.join("Scripts").join("python.exe")
        } else {
            runtime_venv.join("bin").join("python")
        };

        if !runtime_python.exists() {
            Command::new(&system_python)
                .arg("-m")
                .arg("venv")
                .arg("--copies")
                .arg(&runtime_venv)
                .status()
                .context("Failed to create parser runtime venv")?
                .success()
                .then_some(())
                .ok_or_else(|| anyhow::anyhow!("Failed to create parser runtime venv"))?;
        }

        let requirements_path = parser_dir.join("requirements.txt");
        if !requirements_path.exists() {
            return Err(anyhow::anyhow!(
                "Parser requirements.txt not found at {}",
                requirements_path.display()
            ));
        }

        Command::new(&runtime_python)
            .arg("-m")
            .arg("pip")
            .arg("install")
            .arg("--upgrade")
            .arg("pip")
            .status()
            .context("Failed to upgrade pip in parser runtime")?
            .success()
            .then_some(())
            .ok_or_else(|| anyhow::anyhow!("Failed to upgrade pip in parser runtime"))?;

        Command::new(&runtime_python)
            .arg("-m")
            .arg("pip")
            .arg("install")
            .arg("-r")
            .arg(&requirements_path)
            .status()
            .context("Failed to install parser requirements")?
            .success()
            .then_some(())
            .ok_or_else(|| anyhow::anyhow!("Failed to install parser requirements"))?;

        let browsers_path = self.parser_runtime_dir.join("playwright");
        std::fs::create_dir_all(&browsers_path)
            .context("Failed to create Playwright browser cache directory")?;
        Command::new(&runtime_python)
            .arg("-m")
            .arg("playwright")
            .arg("install")
            .arg("chromium")
            .env("PLAYWRIGHT_BROWSERS_PATH", &browsers_path)
            .status()
            .context("Failed to install Playwright Chromium runtime")?
            .success()
            .then_some(())
            .ok_or_else(|| anyhow::anyhow!("Failed to install Playwright Chromium runtime"))?;

        Ok(())
    }

    fn verify_python_runtime(&self, python_cmd: &str, parser_dir: &Path) -> Result<()> {
        let output = Command::new(python_cmd)
            .arg("-c")
            .arg("import bs4, lxml, requests, playwright.async_api")
            .current_dir(parser_dir)
            .env("PYTHONPATH", parser_dir)
            .output()
            .context("Failed to execute Python runtime check")?;

        if output.status.success() {
            return Ok(());
        }

        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        let details = if !stderr.is_empty() {
            stderr
        } else if !stdout.is_empty() {
            stdout
        } else {
            "required Python packages are missing".to_string()
        };

        Err(anyhow::anyhow!(
            "Python parser runtime is not ready: {}. Install parser dependencies or rebuild app bundle with parser runtime included.",
            details
        ))
    }

    pub fn get_progress(&self) -> super::progress::ParserProgress {
        self.progress.get()
    }
}

impl Drop for ParserEngine {
    fn drop(&mut self) {
        if let Some(mut child) = self.python_process.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}
