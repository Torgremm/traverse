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
        use sqlx::TypeInfo;

        if col.type_info().is_null() {
            return Ok(Value::String("NULL".to_string()));
        }

        match col.type_info().name() {
            "INTEGER" | "BOOLEAN" | "INT" | "INT4" => {
                let i: i64 = row.try_get(col.ordinal())?;
                Ok(Value::from(i))
            }
            "REAL" | "FLOAT" | "DOUBLE" => {
                let f: f64 = row.try_get(col.ordinal())?;
                Ok(Value::from(f))
            }
            "TEXT" => {
                let s: String = row.try_get(col.ordinal())?;
                Ok(Value::from(s))
            }
            "BLOB" => {
                let b: Vec<u8> = row.try_get(col.ordinal())?;
                // Try to interpret as UTF-8 string first
                match String::from_utf8(b) {
                    Ok(s) => Ok(Value::from(s)),
                    Err(_) => {
                        todo!()
                    }
                }
            }
            _ => {
                let s: Result<String, _> = row.try_get(col.ordinal());
                match s {
                    Ok(s) => Ok(Value::from(s)),
                    Err(_) => Err(anyhow::anyhow!(
                        "Unsupported column type: {}",
                        col.type_info().name()
                    )),
                }
            }
        }
    }
}
