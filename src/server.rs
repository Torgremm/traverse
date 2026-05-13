use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};

use anyhow::{Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::data::validate_property_value;
use crate::data::{self, QueryRow, Storage};
use crate::load::{self, parse_tables::SchemaConfig};
use crate::script::Script;

#[derive(Debug, Deserialize)]
pub struct ApiRequest {
    #[serde(default)]
    pub id: Value,
    pub method: String,
    #[serde(default)]
    pub params: Value,
}

#[derive(Debug, Serialize)]
pub struct ApiResponse {
    #[serde(default)]
    pub id: Value,
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ApiError>,
}

#[derive(Debug, Serialize)]
pub struct ApiError {
    pub message: String,
}

#[derive(Debug)]
pub struct ServerState {
    project_dir: Option<PathBuf>,
    schema: Option<SchemaConfig>,
    graph: Option<crate::data::Graph>,
}

impl ServerState {
    pub fn new() -> Self {
        Self {
            project_dir: None,
            schema: None,
            graph: None,
        }
    }

    pub async fn handle_request(&mut self, request: ApiRequest) -> ApiResponse {
        let id = request.id.clone();
        match self.handle_method(request).await {
            Ok(result) => ApiResponse {
                id,
                ok: true,
                result: Some(result),
                error: None,
            },
            Err(error) => ApiResponse {
                id,
                ok: false,
                result: None,
                error: Some(ApiError {
                    message: error.to_string(),
                }),
            },
        }
    }

    async fn handle_method(&mut self, request: ApiRequest) -> Result<Value> {
        match request.method.as_str() {
            "project.open" => self.open_project(&request.params).await,
            "project.status" => Ok(json!({
                "open": self.project_dir.is_some(),
                "path": self.project_dir.as_ref().map(|path| path.display().to_string()),
            })),
            "project.save" => {
                let project_dir = self.project_dir()?;
                let schema = self.schema()?;
                data::save(schema.clone(), project_dir).await?;
                Ok(json!({ "saved": true }))
            }
            "types.list" => {
                let schema = self.schema()?;
                let graph = self.graph()?;
                Ok(Storage::type_view_for_graph(schema, graph))
            }
            "table.get" => {
                let label = required_string(&request.params, "type")?;
                let schema = self.schema()?;
                let graph = self.graph()?;
                Storage::table_view_for_graph(schema, graph, &label)
            }
            "graph.get" => {
                let schema = self.schema()?;
                let graph = self.graph()?;
                Ok(Storage::graph_view_for_graph(schema, graph))
            }
            "query.run" => {
                let query = required_string(&request.params, "query")?;
                let rows = Storage::query_graph(&query, self.graph()?)?;
                Ok(json!({
                    "rows": query_rows_to_json(rows),
                }))
            }
            "node.update" => self.update_node(&request.params),
            "script.run" => {
                let script_name = required_string(&request.params, "script")?;
                let script_path = self.project_dir()?.join("scripts").join(script_name);
                let script = Script::load(&script_path)
                    .ok_or_else(|| anyhow!("Failed to load script `{}`", script_path.display()))?;
                let output = script.render(self.schema()?).await?;
                Ok(json!({ "output": output }))
            }
            "shutdown" => Ok(json!({ "shutdown": true })),
            method => bail!("Unknown method `{}`", method),
        }
    }

    async fn open_project(&mut self, params: &Value) -> Result<Value> {
        let project_dir = PathBuf::from(required_string(params, "path")?);
        let schema = load::load_config(&project_dir)?;

        if !Storage::nodes_file_for_project(&project_dir).exists()
            || !Storage::edges_file_for_project(&project_dir).exists()
        {
            let data = load::load_data(&project_dir.join("data"), &schema)?;
            data::init(schema.clone(), data, &project_dir).await?;
        }

        let graph = Storage::read_graph(&project_dir)?;
        let result = json!({
            "path": project_dir.display().to_string(),
            "types": Storage::type_view_for_graph(&schema, &graph)["types"].clone(),
        });

        self.project_dir = Some(project_dir);
        self.schema = Some(schema);
        self.graph = Some(graph);
        Ok(result)
    }

    fn update_node(&mut self, params: &Value) -> Result<Value> {
        let id = required_string(params, "id")?;
        let properties = params
            .get("properties")
            .and_then(Value::as_object)
            .ok_or_else(|| anyhow!("Missing object parameter `properties`"))?;

        let schema = self.schema()?.clone();
        let project_dir = self.project_dir()?.to_path_buf();
        let graph = self.graph()?;
        let node = graph
            .nodes
            .iter()
            .find(|node| node.id == id)
            .ok_or_else(|| anyhow!("Unknown node id `{}`", id))?;
        let table = schema
            .tables
            .iter()
            .find(|table| table.name == node.label)
            .ok_or_else(|| anyhow!("Unknown node type `{}`", node.label))?;

        for (field, value) in properties {
            if field == &table.primary_key {
                bail!("Primary key field `{}` cannot be changed", field);
            }
            let column = table
                .columns
                .iter()
                .find(|column| column.name == *field)
                .ok_or_else(|| anyhow!("Unknown field `{}` for type `{}`", field, table.name))?;
            validate_property_value(&table.name, field, &column.col_type, value)?;
        }

        let mut candidate = graph.clone();
        let node = candidate
            .nodes
            .iter_mut()
            .find(|node| node.id == id)
            .ok_or_else(|| anyhow!("Unknown node id `{}`", id))?;

        for (field, value) in properties {
            node.properties.insert(field.clone(), value.clone());
        }

        let rebuilt = Storage::rebuild_graph_from_nodes(&schema, &candidate)?;
        Storage::write_graph(&project_dir, &rebuilt)?;
        let updated = rebuilt
            .nodes
            .iter()
            .find(|node| node.id == id)
            .ok_or_else(|| anyhow!("Updated node `{}` no longer exists", id))?;
        let result = json!({
            "changed": 1,
            "node": {
                "id": updated.id,
                "type": updated.label,
                "properties": updated.properties,
            }
        });
        self.graph = Some(rebuilt);
        Ok(result)
    }

    fn project_dir(&self) -> Result<&Path> {
        self.project_dir
            .as_deref()
            .ok_or_else(|| anyhow!("No project is open"))
    }

    fn schema(&self) -> Result<&SchemaConfig> {
        self.schema
            .as_ref()
            .ok_or_else(|| anyhow!("No project is open"))
    }

    fn graph(&self) -> Result<&crate::data::Graph> {
        self.graph
            .as_ref()
            .ok_or_else(|| anyhow!("No project is open"))
    }
}

pub async fn serve_stdio() -> Result<()> {
    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout();
    let mut state = ServerState::new();

    for line in stdin.lock().lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }

        let response = match serde_json::from_str::<ApiRequest>(&line) {
            Ok(request) => {
                let shutdown = request.method == "shutdown";
                let response = state.handle_request(request).await;
                write_response(&mut stdout, &response)?;
                if shutdown && response.ok {
                    break;
                }
                continue;
            }
            Err(error) => ApiResponse {
                id: Value::Null,
                ok: false,
                result: None,
                error: Some(ApiError {
                    message: format!("Invalid request JSON: {}", error),
                }),
            },
        };
        write_response(&mut stdout, &response)?;
    }

    Ok(())
}

fn write_response(stdout: &mut impl Write, response: &ApiResponse) -> Result<()> {
    serde_json::to_writer(&mut *stdout, response)?;
    stdout.write_all(b"\n")?;
    stdout.flush()?;
    Ok(())
}

fn required_string(params: &Value, key: &str) -> Result<String> {
    params
        .get(key)
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .ok_or_else(|| anyhow!("Missing string parameter `{}`", key))
}

fn query_rows_to_json(rows: Vec<QueryRow>) -> Vec<Value> {
    rows.into_iter()
        .map(|row| {
            let values = row
                .entries()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect();
            Value::Object(values)
        })
        .collect()
}
