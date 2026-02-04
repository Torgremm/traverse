use crate::{
    data::Storage,
    load::{
        parse_data::{DataFile, Row},
        parse_tables::{ColumnConfig, TableConfig},
    },
};
use anyhow::{Result, anyhow};
use sqlx::Row as SqlxRow;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

impl Storage {
    pub async fn init_globals(&self, path: &Path) -> sqlx::Result<DataFile> {
        log::info!("Loading globals");

        let globals_dir = path.join("globals");

        let table_cfg = globals_table_config();
        self.create_table(&table_cfg).await?;

        let mut rows: Vec<Row> = Vec::new();

        if !globals_dir.exists() {
            return Ok(HashMap::from([("_GLOBALS_".to_string(), rows)]));
        }

        let entries = std::fs::read_dir(&globals_dir)?;

        for entry in entries {
            let entry = entry?;
            let path = entry.path();

            if !path.is_file() {
                continue;
            }

            let name = path
                .file_stem()
                .and_then(|s| s.to_str())
                .ok_or_else(|| sqlx::Error::Protocol("Invalid filename".into()))?
                .to_string();

            let value = std::fs::read_to_string(&path)?;

            let mut row = Row::new();
            row.insert("name".to_string(), serde_json::Value::String(name));
            row.insert("value".to_string(), serde_json::Value::String(value));

            rows.push(row);
        }

        let mut datafile = DataFile::new();
        datafile.insert("_GLOBALS_".to_string(), rows);

        Ok(datafile)
    }
    pub async fn fetch_scoped_globals(
        globals: &[String],
        path: &PathBuf,
    ) -> Result<HashMap<String, String>> {
        if globals.is_empty() {
            return Ok(HashMap::new());
        }

        let placeholders = globals.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
        let sql = format!(
            "SELECT name, value FROM _GLOBALS_ WHERE name IN ({})",
            placeholders
        );

        let rows = Storage::query(&sql, path).await?;

        let map: HashMap<String, String> = rows
            .into_iter()
            .map(|row| {
                let name: String = row.try_get("name")?;
                let value: String = row.try_get("value")?;
                Ok((name, value))
            })
            .collect::<anyhow::Result<_>>()?;

        Ok(map)
    }
}
fn globals_table_config() -> TableConfig {
    TableConfig {
        name: "_GLOBALS_".to_string(),
        primary_key: "name".to_string(),
        columns: vec![
            ColumnConfig {
                name: "name".to_string(),
                col_type: "text".to_string(),
            },
            ColumnConfig {
                name: "value".to_string(),
                col_type: "text".to_string(),
            },
        ],
        foreign_keys: vec![],
    }
}
