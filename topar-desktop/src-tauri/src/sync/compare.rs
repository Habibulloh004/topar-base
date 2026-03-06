use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum DiffStatus {
    New,
    Changed,
    Unchanged,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldDiff {
    pub field: String,
    pub local_value: Option<JsonValue>,
    pub remote_value: Option<JsonValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProductDiff {
    pub status: DiffStatus,
    pub local_data: HashMap<String, JsonValue>,
    pub remote_data: Option<HashMap<String, JsonValue>>,
    pub changed_fields: Vec<FieldDiff>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ComparisonResult {
    pub total: usize,
    pub new_count: usize,
    pub changed_count: usize,
    pub unchanged_count: usize,
    pub diffs: Vec<ProductDiff>,
}

/// Compare local records with remote products
pub fn compare_products(
    local_records: &[HashMap<String, JsonValue>],
    remote_products: &HashMap<String, HashMap<String, JsonValue>>,
    mapping_rules: &HashMap<String, MappingRule>,
) -> ComparisonResult {
    let mut new_count = 0;
    let mut changed_count = 0;
    let mut unchanged_count = 0;
    let mut diffs = Vec::new();

    for local_data in local_records {
        // Apply mapping to get the product keys
        let mapped_data = apply_mapping_rules(local_data, mapping_rules);

        // Try to find remote product by identifier
        let identifier = extract_identifier(&mapped_data);
        let remote_data = identifier.and_then(|id| remote_products.get(&id));

        let (status, changed_fields) = if let Some(remote) = remote_data {
            // Product exists, compare fields
            let changes = compare_fields(&mapped_data, remote);

            if changes.is_empty() {
                unchanged_count += 1;
                (DiffStatus::Unchanged, Vec::new())
            } else {
                changed_count += 1;
                (DiffStatus::Changed, changes)
            }
        } else {
            // New product
            new_count += 1;
            (DiffStatus::New, Vec::new())
        };

        diffs.push(ProductDiff {
            status,
            local_data: mapped_data,
            remote_data: remote_data.cloned(),
            changed_fields,
        });
    }

    ComparisonResult {
        total: local_records.len(),
        new_count,
        changed_count,
        unchanged_count,
        diffs,
    }
}

fn apply_mapping_rules(
    data: &HashMap<String, JsonValue>,
    rules: &HashMap<String, MappingRule>,
) -> HashMap<String, JsonValue> {
    let mut mapped = HashMap::new();

    for (target, rule) in rules {
        let value = if !rule.source.is_empty() {
            data.get(&rule.source).cloned()
        } else if !rule.constant.is_empty() {
            Some(JsonValue::String(rule.constant.clone()))
        } else {
            None
        };

        if let Some(v) = value {
            mapped.insert(target.clone(), v);
        }
    }

    mapped
}

fn extract_identifier(data: &HashMap<String, JsonValue>) -> Option<String> {
    // Try multiple identifier fields in order of preference
    let id_fields = [
        "eksmo.guidNom",
        "main.sourceGuidNom",
        "eksmo.guid",
        "main.sourceGuid",
        "eksmo.nomcode",
        "main.sourceNomcode",
        "eksmo.isbn",
        "main.isbn",
    ];

    for field in &id_fields {
        if let Some(value) = data.get(*field) {
            if let Some(s) = value.as_str() {
                if !s.trim().is_empty() {
                    return Some(s.to_string());
                }
            }
        }
    }

    None
}

fn compare_fields(
    local: &HashMap<String, JsonValue>,
    remote: &HashMap<String, JsonValue>,
) -> Vec<FieldDiff> {
    let mut diffs = Vec::new();

    // Check all local fields
    for (field, local_value) in local {
        if let Some(remote_value) = remote.get(field) {
            if !values_equal(local_value, remote_value) {
                diffs.push(FieldDiff {
                    field: field.clone(),
                    local_value: Some(local_value.clone()),
                    remote_value: Some(remote_value.clone()),
                });
            }
        } else {
            // Field exists locally but not remotely
            diffs.push(FieldDiff {
                field: field.clone(),
                local_value: Some(local_value.clone()),
                remote_value: None,
            });
        }
    }

    diffs
}

fn values_equal(a: &JsonValue, b: &JsonValue) -> bool {
    // Normalize values before comparison
    let a_normalized = normalize_value(a);
    let b_normalized = normalize_value(b);

    a_normalized == b_normalized
}

fn normalize_value(value: &JsonValue) -> JsonValue {
    match value {
        JsonValue::String(s) => {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                JsonValue::Null
            } else {
                JsonValue::String(trimmed.to_string())
            }
        }
        JsonValue::Number(n) => {
            if let Some(f) = n.as_f64() {
                if f == 0.0 {
                    JsonValue::Null
                } else {
                    value.clone()
                }
            } else {
                value.clone()
            }
        }
        JsonValue::Array(arr) => {
            if arr.is_empty() {
                JsonValue::Null
            } else {
                value.clone()
            }
        }
        _ => value.clone(),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MappingRule {
    #[serde(default)]
    pub source: String,
    #[serde(default)]
    pub constant: String,
}
