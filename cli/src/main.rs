use anyhow::Result;

use crate::arg_handler::CommandHandler;

mod arg_handler;
mod data;
mod load;
mod script;
mod server;

#[tokio::main]
async fn main() -> Result<()> {
    //env_logger::Builder::from_default_env().init();
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Error)
        .init();

    let cwd = std::env::current_dir()?;
    let handler = CommandHandler::new(cwd);

    let args = std::env::args().skip(1);
    handler.accept(args).await;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    #[tokio::test]
    async fn graph_storage_is_deterministic_after_load_run_save() {
        env_logger::Builder::new()
            .filter_level(log::LevelFilter::Error)
            .try_init()
            .ok();

        let test_dir = temp_project();

        let mut args = Vec::new();
        args.push("load".to_string());
        let handler = CommandHandler::new(test_dir.clone());
        handler.accept(args.iter().map(|s| s.to_string())).await;

        let script_path = "valve_io.json".to_string();

        args.clear();
        args.push("run".to_string());
        args.push(script_path.clone());
        handler.accept(args.iter().map(|s| s.to_string())).await;

        args.clear();
        args.push("save".to_string());
        handler.accept(args.iter().map(|s| s.to_string())).await;

        let first = list_dir_recursive(&test_dir);

        args.clear();
        args.push("load".to_string());
        handler.accept(args.iter().map(|s| s.to_string())).await;

        args.clear();
        args.push("run".to_string());
        args.push(script_path.clone());
        handler.accept(args.iter().map(|s| s.to_string())).await;

        args.clear();
        args.push("save".to_string());
        handler.accept(args.iter().map(|s| s.to_string())).await;

        let second = list_dir_recursive(&test_dir);
        assert_eq!(first, second);

        std::fs::remove_dir_all(&test_dir).unwrap();
    }

    #[tokio::test]
    async fn run_from_project_dir_auto_loads_graph_without_output_file() {
        let test_dir = temp_project();
        let output_dir = test_dir.join("output");
        if output_dir.exists() {
            std::fs::remove_dir_all(&output_dir).unwrap();
        }
        let handler = CommandHandler::new(test_dir.clone());

        handler
            .accept(["run".to_string(), "valve_io.json".to_string()].into_iter())
            .await;

        assert!(test_dir.join("graph").join("nodes.jsonl").exists());
        assert!(test_dir.join("graph").join("edges.jsonl").exists());
        assert!(!test_dir.join("output").join("valve_io.json").exists());

        std::fs::remove_dir_all(&test_dir).unwrap();
    }

    #[tokio::test]
    async fn script_render_uses_scoped_graph_values() {
        let test_dir = temp_project();
        let handler = CommandHandler::new(test_dir.clone());
        handler.accept(["load".to_string()].into_iter()).await;

        let schema = load::load_config(&test_dir).unwrap();
        let script = script::Script::load(&test_dir.join("scripts").join("valve_io.json")).unwrap();
        let output = script.render(&schema).await.unwrap();

        assert!(output.contains("Valve V001:"));
        assert!(output.contains("OPEN FB  => Rack 0, DB 1, Addr 0"));
        assert!(output.contains("CLOSED FB=> Rack 0, DB 1, Addr 1"));

        std::fs::remove_dir_all(&test_dir).unwrap();
    }

    #[tokio::test]
    async fn frontend_views_expose_types_tables_and_graph() {
        let test_dir = temp_project();
        let handler = CommandHandler::new(test_dir.clone());
        handler.accept(["load".to_string()].into_iter()).await;

        let schema = load::load_config(&test_dir).unwrap();
        let types = data::Storage::type_view(&schema, &test_dir).unwrap();
        assert_eq!(types["types"][0]["name"], "io");
        assert_eq!(types["types"][1]["name"], "valves");
        assert_eq!(types["types"][1]["edges"][0]["label"], "open_feedback");
        assert!(
            types["types"][1]["color"]
                .as_str()
                .unwrap()
                .starts_with('#')
        );

        let table = data::Storage::table_view(&schema, &test_dir, "valves").unwrap();
        assert_eq!(table["type"], "valves");
        assert_eq!(table["primary_key"], "name");
        assert_eq!(table["rows"][0]["name"], "V001");

        let graph = data::Storage::graph_view(&schema, &test_dir).unwrap();
        assert!(graph["nodes"].as_array().unwrap().len() >= 3000);
        assert!(graph["edges"].as_array().unwrap().len() >= 2000);
        assert_eq!(graph["nodes"][0]["type"], "io");

        std::fs::remove_dir_all(&test_dir).unwrap();
    }

    #[test]
    fn graph_view_labels_nodes_by_schema_primary_key_not_fixture_field_names() {
        let schema = load::parse_tables::SchemaConfig {
            tables: vec![load::parse_tables::TableConfig {
                name: "assets".to_string(),
                primary_key: "code".to_string(),
                columns: vec![
                    load::parse_tables::ColumnConfig {
                        name: "code".to_string(),
                        col_type: "text".to_string(),
                    },
                    load::parse_tables::ColumnConfig {
                        name: "description".to_string(),
                        col_type: "text".to_string(),
                    },
                ],
                foreign_keys: vec![],
            }],
        };
        let graph = data::Graph {
            nodes: vec![data::GraphNode {
                id: "assets:A-1".to_string(),
                label: "assets".to_string(),
                properties: [
                    ("code".to_string(), serde_json::json!("A-1")),
                    ("description".to_string(), serde_json::json!("Pump")),
                ]
                .into_iter()
                .collect(),
            }],
            edges: vec![],
        };

        let view = data::Storage::graph_view_for_graph(&schema, &graph);
        assert_eq!(view["nodes"][0]["label"], "A-1");
    }

    #[tokio::test]
    async fn stdio_api_opens_project_reads_and_mutates_state() {
        let test_dir = temp_project();
        let mut state = server::ServerState::new();

        let open = state
            .handle_request(server::ApiRequest {
                id: 1.into(),
                method: "project.open".to_string(),
                params: serde_json::json!({ "path": test_dir.display().to_string() }),
            })
            .await;
        assert!(open.ok);
        assert_eq!(open.result.unwrap()["types"][1]["name"], "valves");

        let table = state
            .handle_request(server::ApiRequest {
                id: 2.into(),
                method: "table.get".to_string(),
                params: serde_json::json!({ "type": "valves" }),
            })
            .await;
        assert!(table.ok);
        assert_eq!(table.result.unwrap()["rows"][0]["name"], "V001");

        let update = state
            .handle_request(server::ApiRequest {
                id: 3.into(),
                method: "node.update".to_string(),
                params: serde_json::json!({
                    "id": "valves:V001",
                    "properties": { "open_feedback": "IO_OPEN_2" }
                }),
            })
            .await;
        assert!(update.ok);
        assert_eq!(
            update.result.unwrap()["node"]["properties"]["open_feedback"],
            "IO_OPEN_2"
        );

        let graph = data::Storage::read_graph(&test_dir).unwrap();
        let edge = graph
            .edges
            .iter()
            .find(|edge| edge.from == "valves:V001" && edge.label == "open_feedback")
            .unwrap();
        assert_eq!(edge.to, "io:IO_OPEN_1");

        let status = state
            .handle_request(server::ApiRequest {
                id: 4.into(),
                method: "project.status".to_string(),
                params: serde_json::json!({}),
            })
            .await;
        assert_eq!(status.result.unwrap()["dirty"], true);

        let save = state
            .handle_request(server::ApiRequest {
                id: 5.into(),
                method: "project.save".to_string(),
                params: serde_json::json!({}),
            })
            .await;
        assert!(save.ok);
        assert_eq!(save.result.unwrap()["dirty"], false);

        let graph = data::Storage::read_graph(&test_dir).unwrap();
        let edge = graph
            .edges
            .iter()
            .find(|edge| edge.from == "valves:V001" && edge.label == "open_feedback")
            .unwrap();
        assert_eq!(edge.to, "io:IO_OPEN_2");

        std::fs::remove_dir_all(&test_dir).unwrap();
    }

    #[tokio::test]
    async fn stdio_api_flushes_dirty_graph_on_shutdown() {
        let test_dir = temp_project();
        let mut state = server::ServerState::new();

        state
            .handle_request(server::ApiRequest {
                id: 1.into(),
                method: "project.open".to_string(),
                params: serde_json::json!({ "path": test_dir.display().to_string() }),
            })
            .await;

        state
            .handle_request(server::ApiRequest {
                id: 2.into(),
                method: "node.update".to_string(),
                params: serde_json::json!({
                    "id": "valves:V001",
                    "properties": { "open_feedback": "IO_OPEN_2" }
                }),
            })
            .await;

        let shutdown = state
            .handle_request(server::ApiRequest {
                id: 3.into(),
                method: "shutdown".to_string(),
                params: serde_json::json!({}),
            })
            .await;
        assert!(shutdown.ok);
        assert_eq!(shutdown.result.unwrap()["dirty"], false);

        let graph = data::Storage::read_graph(&test_dir).unwrap();
        let edge = graph
            .edges
            .iter()
            .find(|edge| edge.from == "valves:V001" && edge.label == "open_feedback")
            .unwrap();
        assert_eq!(edge.to, "io:IO_OPEN_2");

        std::fs::remove_dir_all(&test_dir).unwrap();
    }

    #[tokio::test]
    async fn stdio_api_rejects_invalid_mutations_and_runs_scripts() {
        let test_dir = temp_project();
        let mut state = server::ServerState::new();

        state
            .handle_request(server::ApiRequest {
                id: 1.into(),
                method: "project.open".to_string(),
                params: serde_json::json!({ "path": test_dir.display().to_string() }),
            })
            .await;

        let invalid = state
            .handle_request(server::ApiRequest {
                id: 2.into(),
                method: "node.update".to_string(),
                params: serde_json::json!({
                    "id": "io:IO_OPEN_1",
                    "properties": { "rack": "not-an-int" }
                }),
            })
            .await;
        assert!(!invalid.ok);
        assert!(invalid.error.unwrap().message.contains("expected int"));

        let script = state
            .handle_request(server::ApiRequest {
                id: 3.into(),
                method: "script.run".to_string(),
                params: serde_json::json!({ "script": "valve_io.json" }),
            })
            .await;
        assert!(script.ok);
        assert!(
            script.result.unwrap()["output"]
                .as_str()
                .unwrap()
                .contains("Valve V001:")
        );

        std::fs::remove_dir_all(&test_dir).unwrap();
    }

    #[tokio::test]
    async fn stdio_api_covers_error_paths_without_project_or_known_method() {
        let mut state = server::ServerState::new();

        let no_project = state
            .handle_request(server::ApiRequest {
                id: 1.into(),
                method: "types.list".to_string(),
                params: serde_json::json!({}),
            })
            .await;
        assert!(!no_project.ok);
        assert!(
            no_project
                .error
                .unwrap()
                .message
                .contains("No project is open")
        );

        let unknown = state
            .handle_request(server::ApiRequest {
                id: 2.into(),
                method: "does.not.exist".to_string(),
                params: serde_json::json!({}),
            })
            .await;
        assert!(!unknown.ok);
        assert!(unknown.error.unwrap().message.contains("Unknown method"));
    }

    #[tokio::test]
    async fn stdio_api_rejects_unknown_table_primary_key_and_bad_fk_without_mutating() {
        let test_dir = temp_project();
        let mut state = server::ServerState::new();

        state
            .handle_request(server::ApiRequest {
                id: 1.into(),
                method: "project.open".to_string(),
                params: serde_json::json!({ "path": test_dir.display().to_string() }),
            })
            .await;

        let unknown_table = state
            .handle_request(server::ApiRequest {
                id: 2.into(),
                method: "table.get".to_string(),
                params: serde_json::json!({ "type": "missing" }),
            })
            .await;
        assert!(!unknown_table.ok);

        let primary_key = state
            .handle_request(server::ApiRequest {
                id: 3.into(),
                method: "node.update".to_string(),
                params: serde_json::json!({
                    "id": "valves:V001",
                    "properties": { "name": "V001-renamed" }
                }),
            })
            .await;
        assert!(!primary_key.ok);
        assert!(primary_key.error.unwrap().message.contains("Primary key"));

        let bad_fk = state
            .handle_request(server::ApiRequest {
                id: 4.into(),
                method: "node.update".to_string(),
                params: serde_json::json!({
                    "id": "valves:V001",
                    "properties": { "open_feedback": "IO_OPEN_DOES_NOT_EXIST" }
                }),
            })
            .await;
        assert!(!bad_fk.ok);

        let table = state
            .handle_request(server::ApiRequest {
                id: 5.into(),
                method: "table.get".to_string(),
                params: serde_json::json!({ "type": "valves" }),
            })
            .await;
        let rows = table.result.unwrap()["rows"].as_array().unwrap().clone();
        let v001 = rows.iter().find(|row| row["_id"] == "valves:V001").unwrap();
        assert_eq!(v001["name"], "V001");
        assert_eq!(v001["open_feedback"], "IO_OPEN_1");

        std::fs::remove_dir_all(&test_dir).unwrap();
    }

    #[tokio::test]
    async fn file_backed_mutation_rebuilds_edges_and_rejects_invalid_values() {
        let test_dir = temp_project();
        let handler = CommandHandler::new(test_dir.clone());
        handler.accept(["load".to_string()].into_iter()).await;
        let schema = load::load_config(&test_dir).unwrap();

        let changed = data::Storage::mutate_with_schema(
            "MATCH valves WHERE name = V001 SET open_feedback = IO_OPEN_2",
            &schema,
            &test_dir,
        )
        .await
        .unwrap();
        assert_eq!(changed, 1);

        let graph = data::Storage::read_graph(&test_dir).unwrap();
        let edge = graph
            .edges
            .iter()
            .find(|edge| edge.from == "valves:V001" && edge.label == "open_feedback")
            .unwrap();
        assert_eq!(edge.to, "io:IO_OPEN_2");

        let invalid = data::Storage::mutate_with_schema(
            "MATCH valves WHERE name = V001 SET open_feedback = IO_OPEN_DOES_NOT_EXIST",
            &schema,
            &test_dir,
        )
        .await;
        assert!(invalid.is_err());

        let graph = data::Storage::read_graph(&test_dir).unwrap();
        let edge = graph
            .edges
            .iter()
            .find(|edge| edge.from == "valves:V001" && edge.label == "open_feedback")
            .unwrap();
        assert_eq!(edge.to, "io:IO_OPEN_2");

        std::fs::remove_dir_all(&test_dir).unwrap();
    }

    fn list_dir_recursive(path: &PathBuf) -> Vec<String> {
        let mut entries = Vec::new();
        for entry in std::fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.is_dir() {
                entries.extend(list_dir_recursive(&path));
            } else {
                entries.push(std::fs::read_to_string(path).unwrap());
            }
        }
        entries.sort();
        entries
    }

    fn temp_project() -> PathBuf {
        static NEXT_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let source_dir = root.join("tests").join("test_dir");
        let id = NEXT_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let test_dir = std::env::temp_dir().join(format!(
            "traverse_test_{}_{}_{}",
            std::process::id(),
            id,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        copy_dir_recursive(&source_dir, &test_dir);
        test_dir
    }

    fn copy_dir_recursive(from: &PathBuf, to: &PathBuf) {
        std::fs::create_dir_all(to).unwrap();
        for entry in std::fs::read_dir(from).unwrap() {
            let entry = entry.unwrap();
            let source = entry.path();
            let target = to.join(entry.file_name());
            if source.is_dir() {
                copy_dir_recursive(&source, &target);
            } else {
                std::fs::copy(source, target).unwrap();
            }
        }
    }
}
