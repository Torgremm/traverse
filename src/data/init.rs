use crate::load::parse_data::DataFile;
use crate::load::parse_tables::{SchemaConfig, TableConfig};
use sqlx::sqlite::SqliteQueryResult;
use sqlx::{Result, SqlitePool};
use sqlx::{Row, sqlite::SqlitePoolOptions};

pub struct Storage {
    pub pool: SqlitePool,
}

const MEM: &str = "sqlite::memory:";

impl Storage {
    pub async fn new(schema: SchemaConfig, data: DataFile) -> Result<Self> {
        let pool = SqlitePool::connect(MEM).await?;
        let s = Self { pool };

        for table in schema.tables {
            s.create_table(&table).await?;
        }

        s.init_data(&data).await?;
        Ok(s)
    }

    async fn create_table(&self, table: &TableConfig) -> Result<(), sqlx::Error> {
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
                "FOREIGN KEY ({}) REFERENCES {}({})",
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
            "INSERT INTO {} ({}) VALUES ",
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
                // fallback for weird numbers (rare)
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
            // everything else as string
            row_builder.push_bind(value.to_string());
        }
    }
}
