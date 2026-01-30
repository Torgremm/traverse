use std::path::Path;

use crate::data::Storage;
use anyhow::Result;
use serde::Deserialize;
use sqlx::Column;
use sqlx::Row;
use sqlx::sqlite::SqliteColumn;
use sqlx::sqlite::SqliteRow;
use sqlx::{Arguments, sqlite::SqliteArguments};
use tera::{Context, Tera};

#[derive(Debug, Deserialize)]
pub struct Script {
    fetch: String,
    act: String,
}

impl Script {
    pub fn load(path: &Path) -> Option<Self> {
        let raw_text = match std::fs::read_to_string(path) {
            Ok(v) => v,
            Err(e) => {
                log::error!("Failed to load script file: {:?} \n {}", path, e);
                return None;
            }
        };

        let s: Script = match serde_json::from_str(&raw_text) {
            Ok(v) => v,
            Err(e) => {
                log::error!("Failed to serialize script: {:?} \n {}", path, e);
                return None;
            }
        };

        Some(s)
    }
    pub async fn run(&self, storage: &Storage) -> Result<()> {
        let data: Vec<SqliteRow> = storage.query(&self.fetch).await?;

        let first = match data.first() {
            Some(v) => v,
            None => {
                return Err(anyhow::anyhow!(
                    "Query returned 0 rows, check your FETCH section"
                ));
            }
        };
        let tokens: Vec<String> = first
            .columns()
            .iter()
            .map(|col| col.name().to_string())
            .collect();

        for row in data {
            let std_out = self.fill_data(row, &tokens);
            println!("{std_out}");
        }

        Ok(())
    }

    fn fill_data(&self, data: SqliteRow, tokens: &Vec<String>) -> String {
        let mut tera = Tera::default();

        match tera.add_raw_template("script", &self.act) {
            Ok(_) => {}
            Err(e) => log::error!("{e}"),
        }

        let mut context = Context::new();

        for name in tokens {
            if let Ok(v) = data.try_get::<String, _>(name.as_str()) {
                context.insert(name, &v);
            } else if let Ok(v) = data.try_get::<i64, _>(name.as_str()) {
                context.insert(name, &v);
            } else if let Ok(v) = data.try_get::<f64, _>(name.as_str()) {
                context.insert(name, &v);
            }
        }

        tera.render("script", &context)
            .unwrap_or_else(|e| format!("TEMPLATE ERROR: {e}"))
    }
}
