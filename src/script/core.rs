use std::path::Path;
use std::path::PathBuf;

use crate::data::Storage;
use crate::load::parse_tables::SchemaConfig;
use anyhow::Result;
use indexmap::IndexMap;
use serde::Deserialize;
use serde_json::{Map, Value};
use sqlx::Column;
use sqlx::Row;
use sqlx::sqlite::SqliteColumn;
use sqlx::sqlite::SqliteRow;
use sqlx::{Arguments, sqlite::SqliteArguments};
use std::collections::HashMap;
use tera::{Context, Tera};

pub struct Script {
    data: UserScript,
    output: PathBuf,
    project_dir: PathBuf,
}

#[derive(Debug, Deserialize)]
pub struct UserScript {
    fetch: String,
    #[serde(default)]
    mode: FetchMode,
    act: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FetchMode {
    Raw,
    Scope(Option<String>),
}

impl Default for FetchMode {
    fn default() -> Self {
        FetchMode::Scope(None)
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

        let s: UserScript = match serde_json::from_str(&raw_text) {
            Ok(v) => v,
            Err(e) => {
                log::error!("Failed to serialize script: {:?} \n {}", path, e);
                return None;
            }
        };
        let filename = path.file_name()?;
        let output = path.parent()?.parent()?.join("output").join(filename);
        let parent = Script::get_root_dir(path)?;

        Some(Self {
            data: s,
            output,
            project_dir: parent.to_path_buf(),
        })
    }
    pub async fn run(&self, schema: &SchemaConfig) -> Result<()> {
        let sql = match &self.data.mode {
            FetchMode::Raw => self.data.fetch.clone(),
            FetchMode::Scope(key) => Storage::build_scope_query(schema, &self.data.fetch, key)?,
        };

        let rows: Vec<SqliteRow> = Storage::query(&sql, &self.project_dir).await?;
        log::debug!("Query returned {} rows, rendering script:", rows.len());

        if rows.is_empty() {
            return Err(anyhow::anyhow!(
                "Query returned 0 rows, check your FETCH section"
            ));
        }

        let mut stdout = String::new();
        let mut tera = Tera::default();
        tera.add_raw_template("script", &self.data.act)?;

        if let Ok(entries) = std::fs::read_dir(self.project_dir.join("templates")) {
            let files: Vec<(std::path::PathBuf, Option<String>)> = entries
                .filter_map(Result::ok)
                .map(|e| {
                    let path = e.path();
                    let name = path.file_name().unwrap().to_string_lossy().to_string();
                    (path, Some(name))
                })
                .collect();
            tera.add_template_files(files)?;
        }
        log::debug!("{:#?}", tera.get_template_names().collect::<String>());

        match &self.data.mode {
            FetchMode::Raw => {
                for row in rows {
                    let mut context = Context::new();

                    for col in row.columns() {
                        let name = col.name();
                        let val = Storage::parse_col(&row, col)?;

                        context.insert(name, &val);
                    }
                    let out = tera.render("script", &context)?;
                    log::debug!("{out}");
                    stdout.push_str(&out);
                }
            }
            FetchMode::Scope(key) => {
                let mut grouped: IndexMap<String, Vec<SqliteRow>> = IndexMap::new();
                for row in rows {
                    let root_id: String = row.try_get("root_id")?;
                    grouped.entry(root_id.clone()).or_default().push(row);
                }
                log::debug!("Grouped {} objects", grouped.len());

                for (object_id, rows_for_object) in grouped {
                    let mut nested_scope: Map<String, Value> = Map::new();

                    for row in rows_for_object {
                        let path: String = row.try_get("path")?;
                        let value = Storage::parse_col(&row, row.column("value"))?;
                        nested_scope.insert(path, value);
                    }
                    let mut context = Context::new();
                    context.insert("object_id", &object_id);
                    for (k, v) in nested_scope {
                        context.insert(&k, &v);
                    }
                    log::debug!("{:?}", context);

                    let out = tera.render("script", &context)?;
                    log::debug!("{out}");
                    stdout.push_str(&out);
                }
            }
        }
        self.write(stdout)?;
        Ok(())
    }
    fn write(&self, s: String) -> Result<()> {
        std::fs::create_dir_all(&self.output.parent().expect("Impossible"))?;

        std::fs::write(&self.output, s)
            .map_err(|e| anyhow::anyhow!("Failed to write to file: {}", e))
    }
    pub fn get_root_dir(path: &Path) -> Option<&Path> {
        let mut parent = path;
        loop {
            if std::fs::exists(parent.join("schema.json")).unwrap() {
                break;
            }
            parent = parent.parent()?;
        }
        Some(parent)
    }
}
