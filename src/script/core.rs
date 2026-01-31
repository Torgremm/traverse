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
    #[serde(default)]
    mode: FetchMode,
    act: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FetchMode {
    Raw,
    Scope,
}

impl Default for FetchMode {
    fn default() -> Self {
        FetchMode::Scope
    }
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
        let sql = match self.mode {
            FetchMode::Raw => self.fetch.clone(),
            FetchMode::Scope => storage.build_scope_query(&self.fetch)?,
        };
        let rows: Vec<SqliteRow> = storage.query(&sql).await?;
        log::debug!("Query returned {} rows, rendering script:", rows.len());

        if rows.is_empty() {
            return Err(anyhow::anyhow!(
                "Query returned 0 rows, check your FETCH section"
            ));
        }

        match self.mode {
            FetchMode::Raw => {
                for row in rows {
                    let mut tera = Tera::default();
                    tera.add_raw_template("script", &self.act)?;

                    let mut context = Context::new();

                    for col in row.columns() {
                        let name = col.name();

                        if let Ok(v) = row.try_get::<String, _>(name) {
                            context.insert(name, &v);
                        } else if let Ok(v) = row.try_get::<i64, _>(name) {
                            context.insert(name, &v);
                        } else if let Ok(v) = row.try_get::<f64, _>(name) {
                            context.insert(name, &v);
                        }
                    }
                    let out = tera.render("script", &context)?;
                    println!("{out}");
                }
            }

            FetchMode::Scope => {
                for row in rows {
                    print_sqlite_row(&row);
                    let object_id: String = row.try_get("root_id")?;
                    let scope_json: String = row.try_get("scope_json")?;

                    let scope: std::collections::HashMap<String, serde_json::Value> =
                        serde_json::from_str(&scope_json)?;

                    let mut tera = Tera::default();
                    tera.add_raw_template("script", &self.act)?;

                    let mut context = Context::new();
                    context.insert("object_id", &object_id);

                    for (k, v) in scope {
                        context.insert(&k, &v);
                    }

                    let out = tera.render("script", &context)?;
                    println!("{out}");
                }
            }
        }
        Ok(())
    }
}
pub fn print_sqlite_row(row: &SqliteRow) {
    let mut output = Vec::new();

    for col in row.columns() {
        let name = col.name();
        let value: Result<String, _> = row.try_get(name);
        match value {
            Ok(v) => output.push(format!("{}={}", name, v)),
            Err(_) => output.push(format!("{}=<non-string>", name)),
        }
    }

    log::debug!("SqliteRow {{ {} }}", output.join(", "));
}
