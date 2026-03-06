use anyhow::{Context, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct SyncClient {
    client: Client,
    base_url: String,
}

#[derive(Debug, Serialize)]
struct LocalSyncRequest {
    #[serde(rename = "runId")]
    run_id: String,
    records: Vec<LocalRecordPayload>,
    rules: HashMap<String, MappingRule>,
    #[serde(rename = "saveMapping")]
    save_mapping: bool,
    #[serde(rename = "mappingName")]
    mapping_name: String,
    #[serde(rename = "syncEksmo")]
    sync_eksmo: bool,
    #[serde(rename = "syncMain")]
    sync_main: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MappingRule {
    #[serde(default)]
    pub source: String,
    #[serde(default)]
    pub constant: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct LocalRecordPayload {
    #[serde(rename = "sourceUrl")]
    pub source_url: String,
    pub data: HashMap<String, JsonValue>,
}

#[derive(Debug, Deserialize)]
pub struct SyncResponse {
    #[serde(rename = "totalRecords")]
    pub total_records: usize,
    #[serde(rename = "eksmoUpserted")]
    pub eksmo_upserted: usize,
    #[serde(rename = "eksmoModified")]
    pub eksmo_modified: usize,
    #[serde(rename = "eksmoSkipped")]
    pub eksmo_skipped: usize,
    #[serde(rename = "mainInserted")]
    pub main_inserted: usize,
    #[serde(rename = "mainModified")]
    pub main_modified: usize,
    #[serde(rename = "mainSkipped")]
    pub main_skipped: usize,
}

impl SyncClient {
    pub fn new(base_url: String) -> Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(300))
            .build()
            .context("Failed to create HTTP client")?;

        Ok(Self {
            client,
            base_url: base_url.trim_end_matches('/').to_string(),
        })
    }

    /// Fetch remote products for comparison
    pub async fn fetch_remote_products(
        &self,
        _identifiers: &[String],
    ) -> Result<HashMap<String, HashMap<String, JsonValue>>> {
        // This would call the backend API to fetch products by identifiers
        // For now, return empty (to be implemented based on backend API)
        Ok(HashMap::new())
    }

    /// Sync local desktop records to backend database
    pub async fn sync_local_records(
        &self,
        run_id: &str,
        records: Vec<LocalRecordPayload>,
        rules: HashMap<String, MappingRule>,
        mapping_name: String,
        save_mapping: bool,
        sync_eksmo: bool,
        sync_main: bool,
    ) -> Result<SyncResponse> {
        let url = format!("{}/parser-app/sync-local", self.base_url);

        let request = LocalSyncRequest {
            run_id: run_id.to_string(),
            records,
            rules,
            save_mapping,
            mapping_name,
            sync_eksmo,
            sync_main,
        };

        let response = self
            .client
            .post(&url)
            .json(&request)
            .send()
            .await
            .context("Failed to send sync request")?;

        if !response.status().is_success() {
            let error_text = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!("Sync failed: {}", error_text));
        }

        let sync_response: SyncResponse = response
            .json()
            .await
            .context("Failed to parse sync response")?;

        Ok(sync_response)
    }
}
