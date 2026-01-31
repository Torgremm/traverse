use std::path::Path;

use crate::data::Storage;
use anyhow::Result;
use serde::Deserialize;
use serde_json::{Map, Value};
use sqlx::Column;
use sqlx::Row;
use sqlx::sqlite::SqliteColumn;
use sqlx::sqlite::SqliteRow;
use sqlx::{Arguments, sqlite::SqliteArguments};
use std::collections::HashMap;
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
                let mut grouped: HashMap<String, Vec<SqliteRow>> = HashMap::new();
                for row in rows {
                    let root_id: String = row.try_get("root_id")?;
                    grouped.entry(root_id.clone()).or_default().push(row);
                }

                for (object_id, rows_for_object) in grouped {
                    let mut nested_scope: Map<String, Value> = Map::new();

                    for row in rows_for_object {
                        let path: String = row.try_get("path")?;
                        let value: String = row.try_get("value")?;
                        nested_scope.insert(path, Value::String(value));
                    }
                    let mut context = Context::new();
                    context.insert("object_id", &object_id);
                    for (k, v) in nested_scope {
                        context.insert(&k, &v);
                    }
                    log::debug!("{:?}", context);

                    let mut tera = Tera::default();
                    tera.add_raw_template("script", &self.act)?;
                    let out = tera.render("script", &context)?;
                    println!("{out}");
                }
            }
        }
        Ok(())
    }
}
