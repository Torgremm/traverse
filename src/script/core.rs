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
                    let object_id: String = row.try_get("root_id")?;
                    let scope_json: String = row.try_get("scope_json")?;

                    let flat_scope: std::collections::HashMap<String, serde_json::Value> =
                        serde_json::from_str(&scope_json)?;

                    let mut nested_scope: serde_json::Map<String, serde_json::Value> =
                        serde_json::Map::new();

                    fn insert_nested_value(
                        map: &mut serde_json::Map<String, serde_json::Value>,
                        key: &str,
                        value: serde_json::Value,
                    ) {
                        let parts: Vec<&str> = key.split('.').collect();
                        let last = parts.last().unwrap();
                        let mut current = map;

                        for part in &parts[..parts.len() - 1] {
                            current = current
                                .entry(part.to_string())
                                .or_insert_with(|| Value::Object(serde_json::Map::new()))
                                .as_object_mut()
                                .expect("All intermediate values must be objects");
                        }

                        match current.get_mut(*last) {
                            Some(Value::Object(obj)) => {
                                obj.insert("value".to_string(), value);
                            }
                            _ => {
                                let mut obj = serde_json::Map::new();
                                obj.insert("value".to_string(), value);
                                current.insert(last.to_string(), Value::Object(obj));
                            }
                        }
                    }

                    for (flat_key, val) in flat_scope {
                        insert_nested_value(&mut nested_scope, &flat_key, val);
                    }

                    prune_leaves_to_values(&mut nested_scope);
                    let mut context = Context::new();
                    context.insert("object_id", &object_id);

                    for (k, v) in nested_scope {
                        context.insert(&k, &v);
                    }

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

fn insert_nested_value(map: &mut serde_json::Map<String, Value>, key: &str, value: Value) {
    let parts: Vec<&str> = key.split('.').collect();
    let last = parts.last().unwrap();
    let mut current = map;

    for part in &parts[..parts.len() - 1] {
        current = current
            .entry(part.to_string())
            .or_insert_with(|| Value::Object(serde_json::Map::new()))
            .as_object_mut()
            .expect("All intermediate values must be objects");
    }

    current.insert(last.to_string(), value);
}
fn prune_leaves_to_values(map: &mut serde_json::Map<String, Value>) {
    for (_k, v) in map.iter_mut() {
        match v {
            Value::Object(obj) => {
                if obj.len() == 1 && obj.contains_key("value") {
                    *v = obj.remove("value").unwrap();
                } else {
                    prune_leaves_to_values(obj);
                }
            }
            _ => {}
        }
    }
}
