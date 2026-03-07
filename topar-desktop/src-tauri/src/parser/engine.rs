use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::io::{BufRead, BufReader};
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
    python_process: Option<Child>,
    current_run_id: Option<String>,
}

impl ParserEngine {
    pub fn new(db: Arc<Database>, progress: ProgressTracker) -> Self {
        Self {
            db,
            progress,
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
        self.db.insert_run(&run)
            .context("Failed to save run to database")?;
        self.current_run_id = Some(run_id.clone());

        // Spawn Python parser process
        let app_dir = std::env::current_exe()?
            .parent()
            .ok_or_else(|| anyhow::anyhow!("Cannot find app directory"))?
            .to_path_buf();

        let parser_dir = app_dir.join("parser_engine");
        let parser_script = parser_dir.join("main.py");

        // Use venv Python if available, otherwise system Python
        let python_cmd = parser_dir.join("venv").join("bin").join("python");
        let python_cmd = if python_cmd.exists() {
            python_cmd.to_string_lossy().to_string()
        } else {
            self.find_python_command()?
        };

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
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .context("Failed to spawn Python parser process")?;

        // Read output in background thread
        let stdout = child.stdout.take()
            .ok_or_else(|| anyhow::anyhow!("Failed to capture stdout"))?;

        let db = self.db.clone();
        let progress = self.progress.clone();
        let run_id_clone = run_id.clone();

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
                                    p.error = Some(
                                        error_message.unwrap_or_else(|| "Parsing cancelled by user".to_string())
                                    );
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
                self.db.update_run(&run)
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

    async fn save_results(
        db: &Arc<Database>,
        run_id: &str,
        result: ParserResult,
    ) -> Result<()> {
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
                run.error = Some(result.error.unwrap_or_else(|| "Parsing cancelled by user".to_string()));
            }
            run.finished_at = Some(chrono::Utc::now());

            db.update_run(&run)
                .context("Failed to update run")?;
        }

        Ok(())
    }

    fn find_python_command(&self) -> Result<String> {
        // Try common Python commands
        let candidates = vec!["python3", "python"];

        for cmd in candidates {
            if Command::new(cmd)
                .arg("--version")
                .output()
                .is_ok()
            {
                return Ok(cmd.to_string());
            }
        }

        Err(anyhow::anyhow!(
            "Python 3 not found. Please install Python 3.11 or later."
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
