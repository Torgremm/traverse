use crate::{data::Storage, load::parse_tables::SchemaConfig};
use anyhow::Result;
use serde_json::Map;
use serde_json::Value;
use sqlx::Column;
use sqlx::Row as SqlxRow;
use sqlx::sqlite::SqliteTypeInfo;
use sqlx::sqlite::SqliteValueRef;
use std::path::Path;

impl Storage {
    pub async fn save(schema: &SchemaConfig, path: &Path) -> Result<()> {
        let data_dir = path.join("data");
        std::fs::create_dir_all(&data_dir)?;

        for table in &schema.tables {
            let table_file = data_dir.join(format!("{}.json", table.name));
            let mut file = std::fs::File::create(table_file)?;

            let rows = Storage::query(
                &format!("SELECT * FROM {}", table.name),
                &path.to_path_buf(),
            )
            .await?;

            let mut rows_array = Vec::new();
            for row in rows.iter() {
                let mut json_row = Map::new();
                for col in row.columns() {
                    let val = Storage::parse_col(row, col)?;
                    json_row.insert(col.name().to_string(), val);
                }
                rows_array.push(Value::Object(json_row));
            }

            let mut top_level = Map::new();
            top_level.insert(table.name.clone(), Value::Array(rows_array));

            serde_json::to_writer_pretty(&mut file, &top_level)?;
        }

        Ok(())
    }

    pub fn parse_col(
        row: &sqlx::sqlite::SqliteRow,
        col: &sqlx::sqlite::SqliteColumn,
    ) -> Result<Value> {
        use serde_json::Value::String as Out;
        if let Ok(v) = row.try_get::<String, _>(col.name()) {
            return Ok(Out(v));
        }
        if let Ok(v) = row.try_get::<i64, _>(col.name()) {
            return Ok(Out(v.to_string()));
        }
        if let Ok(v) = row.try_get::<f64, _>(col.name()) {
            return Ok(Out(v.to_string()));
        }
        Err(anyhow::anyhow!(
            "Failed to match value in column: {}",
            col.name()
        ))
    }
}
