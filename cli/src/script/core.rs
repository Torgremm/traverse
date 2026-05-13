use std::path::Path;
use std::path::PathBuf;

use crate::data::{QueryRow, Storage};
use crate::load::parse_tables::SchemaConfig;
use anyhow::Result;
use indexmap::IndexMap;
use serde::Deserialize;
use serde_json::{Map, Value};
use tera::{Context, Tera};

pub struct Script {
    data: UserScript,
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
        let parent = Script::get_root_dir(path)?;

        Some(Self {
            data: s,
            project_dir: parent.to_path_buf(),
        })
    }
    pub async fn run(&self, schema: &SchemaConfig) -> Result<()> {
        print!("{}", self.render(schema).await?);
        Ok(())
    }

    pub async fn render(&self, schema: &SchemaConfig) -> Result<String> {
        let rows = match &self.data.mode {
            FetchMode::Raw => Storage::query(&self.data.fetch, &self.project_dir).await?,
            FetchMode::Scope(key) => {
                Storage::scope(schema, &self.data.fetch, key, &self.project_dir).await?
            }
        };
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

                    for (name, val) in row.entries() {
                        context.insert(name, val);
                    }
                    let out = tera.render("script", &context)?;
                    log::debug!("{out}");
                    stdout.push_str(&out);
                }
            }
            FetchMode::Scope(_) => {
                let mut grouped: IndexMap<String, Vec<QueryRow>> = IndexMap::new();
                for row in rows {
                    let root_id = row_value_as_string(&row, "root_id")?;
                    grouped.entry(root_id.clone()).or_default().push(row);
                }
                log::debug!("Grouped {} objects", grouped.len());

                for (object_id, rows_for_object) in grouped {
                    let mut nested_scope: Map<String, Value> = Map::new();

                    for row in rows_for_object {
                        let path = row_value_as_string(&row, "path")?;
                        let value = row
                            .get("value")
                            .ok_or_else(|| anyhow::anyhow!("Scoped row has no value field"))?
                            .clone();
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
        Ok(stdout)
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

fn row_value_as_string(row: &QueryRow, key: &str) -> Result<String> {
    match row.get(key) {
        Some(Value::String(value)) => Ok(value.clone()),
        Some(Value::Number(value)) => Ok(value.to_string()),
        Some(Value::Bool(value)) => Ok(value.to_string()),
        Some(Value::Null) => Ok("null".to_string()),
        Some(value) => Ok(value.to_string()),
        None => Err(anyhow::anyhow!("Query row has no `{}` field", key)),
    }
}
