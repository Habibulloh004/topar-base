use serde::{Deserialize, Serialize};
use std::sync::Arc;
use parking_lot::RwLock;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum ParserStatus {
    Idle,
    Running,
    Finished,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParserProgress {
    pub status: ParserStatus,
    pub current_url: Option<String>,
    pub discovered_urls: usize,
    pub parsed_products: usize,
    pub rate_limit_retries: usize,
    pub error: Option<String>,
    pub progress_percent: f64,
}

impl Default for ParserProgress {
    fn default() -> Self {
        Self {
            status: ParserStatus::Idle,
            current_url: None,
            discovered_urls: 0,
            parsed_products: 0,
            rate_limit_retries: 0,
            error: None,
            progress_percent: 0.0,
        }
    }
}

/// Thread-safe progress tracker
#[derive(Clone)]
pub struct ProgressTracker {
    inner: Arc<RwLock<ParserProgress>>,
}

impl ProgressTracker {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(ParserProgress::default())),
        }
    }

    pub fn update<F>(&self, f: F)
    where
        F: FnOnce(&mut ParserProgress),
    {
        let mut progress = self.inner.write();
        f(&mut progress);

        // Calculate progress percentage
        if progress.discovered_urls > 0 {
            progress.progress_percent = (progress.parsed_products as f64
                / progress.discovered_urls as f64 * 100.0).min(100.0);
        }
    }

    pub fn get(&self) -> ParserProgress {
        self.inner.read().clone()
    }

    pub fn reset(&self) {
        let mut progress = self.inner.write();
        *progress = ParserProgress::default();
    }
}
