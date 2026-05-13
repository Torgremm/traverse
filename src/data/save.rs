use std::collections::BTreeMap;
use std::fs::File;
use std::path::Path;

use anyhow::{Result, anyhow};
use serde_json::{Map, Value};

use crate::{data::Storage, load::parse_tables::SchemaConfig};

impl Storage {
    pub async fn save(schema: &SchemaConfig, path: &Path) -> Result<()> {
        let graph = Storage::read_graph(path)?;
        let data_dir = path.join("data");
        std::fs::create_dir_all(&data_dir)?;

        let mut by_label: BTreeMap<String, Vec<Map<String, Value>>> = BTreeMap::new();
        for node in graph.nodes {
            by_label
                .entry(node.label)
                .or_default()
                .push(node.properties.into_iter().collect());
        }

        for table in &schema.tables {
            let table_file = data_dir.join(format!("{}.json", table.name));
            let mut rows = by_label.remove(&table.name).unwrap_or_default();
            rows.sort_by(|a, b| {
                let left = a
                    .get(&table.primary_key)
                    .map(stable_value)
                    .unwrap_or_default();
                let right = b
                    .get(&table.primary_key)
                    .map(stable_value)
                    .unwrap_or_default();
                left.cmp(&right)
            });

            let mut top_level = Map::new();
            top_level.insert(
                table.name.clone(),
                Value::Array(rows.into_iter().map(Value::Object).collect()),
            );

            let mut file = File::create(table_file)?;
            serde_json::to_writer_pretty(&mut file, &top_level)?;
        }

        if let Some((unknown, _)) = by_label.into_iter().next() {
            return Err(anyhow!("Graph contains unknown node label `{}`", unknown));
        }

        Ok(())
    }
}

fn stable_value(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        Value::Number(value) => value.to_string(),
        Value::Bool(value) => value.to_string(),
        Value::Null => "null".to_string(),
        other => other.to_string(),
    }
}
