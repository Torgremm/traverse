use std::path::{Path, PathBuf};

use crate::load::parse_data::DataFile;
use crate::load::parse_tables::{SchemaConfig, TableConfig};
use fxhash::FxHasher;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Result, SqlitePool};
use std::hash::Hasher;

pub struct Storage {
    pub pool: SqlitePool,
}
impl Storage {
    pub fn db_file_for_project(project_path: &Path) -> PathBuf {
        let mut hasher = FxHasher::default();
        hasher.write(project_path.to_string_lossy().as_bytes());
        let hash = hasher.finish() & 0xFFFF_FFFF;

        let db_file = std::env::temp_dir().join(format!("traverse_{:08x}.db", hash));

        if let Some(parent) = db_file.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }

        db_file
    }
    pub async fn init(schema: SchemaConfig, data: DataFile, path: &Path) -> Result<PathBuf> {
        log::info!("Loaded file, creating SQLite database");
        let db_file = Storage::db_file_for_project(path);
        log::debug!("{:?}", db_file);
        std::fs::remove_file(&db_file)?;
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(
                SqliteConnectOptions::new()
                    .filename(&db_file)
                    .create_if_missing(true),
            )
            .await?;

        let s = Self { pool };
        for table in schema.tables {
            s.create_table(&table).await?;
        }

        s.init_data(&data).await?;
        Ok(db_file)
    }

    pub async fn create_table(&self, table: &TableConfig) -> Result<(), sqlx::Error> {
        let mut qb = sqlx::QueryBuilder::new("CREATE TABLE ");

        qb.push(&table.name);
        qb.push(" (");

        let mut separated = qb.separated(", ");

        for col in &table.columns {
            separated.push(format!("{} {}", col.name, col.col_type));
        }

        separated.push(format!("PRIMARY KEY ({})", table.primary_key));

        for fk in &table.foreign_keys {
            separated.push(format!(
                "FOREIGN KEY ({}) REFERENCES {}({}) DEFERRABLE INITIALLY DEFERRED",
                fk.column, fk.references.table, fk.references.column
            ));
        }

        drop(separated);
        qb.push(")");

        let query = qb.build();
        query.execute(&self.pool).await?;

        log::info!("Successfuly created table: {}", &table.name);
        Ok(())
    }
    async fn init_data(&self, data: &DataFile) -> Result<(), sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        const BATCH_SIZE: usize = 100;

        for (table_name, rows) in data {
            let mut batch: Vec<&serde_json::Map<String, serde_json::Value>> = Vec::new();

            for row in rows {
                batch.push(row);

                if batch.len() >= BATCH_SIZE {
                    self.execute_batch(&mut tx, table_name, &batch).await?;
                    batch.clear();
                }
            }

            if !batch.is_empty() {
                self.execute_batch(&mut tx, table_name, &batch).await?;
            }
        }

        tx.commit().await?;
        Ok(())
    }
}

impl Storage {
    async fn execute_batch<'a>(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        table_name: &str,
        batch: &[&serde_json::Map<String, serde_json::Value>],
    ) -> Result<(), sqlx::Error> {
        if batch.is_empty() {
            return Ok(());
        }

        let columns: Vec<&String> = batch[0].keys().collect();

        let mut qb = sqlx::QueryBuilder::new(format!(
            "INSERT INTO {} ({})",
            table_name,
            columns
                .iter()
                .map(|c| c.as_str().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        ));

        qb.push_values(batch.iter(), |mut row_builder, row| {
            for col in &columns {
                bind_json_value(&mut row_builder, row.get(*col).unwrap());
            }
        });

        qb.build().execute(&mut **tx).await?;
        Ok(())
    }
}

fn bind_json_value<'q>(
    row_builder: &mut sqlx::query_builder::Separated<'q, '_, sqlx::Sqlite, &'static str>,
    value: &serde_json::Value,
) {
    match value {
        serde_json::Value::String(s) => {
            row_builder.push_bind(s.clone());
        }
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                row_builder.push_bind(i);
            } else if let Some(f) = n.as_f64() {
                row_builder.push_bind(f);
            } else {
                row_builder.push_bind(n.to_string());
            }
        }
        serde_json::Value::Bool(b) => {
            row_builder.push_bind(*b);
        }
        serde_json::Value::Null => {
            row_builder.push_bind(None::<String>);
        }
        _ => {
            row_builder.push_bind(value.to_string());
        }
    }
}
