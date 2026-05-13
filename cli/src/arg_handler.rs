use std::path::{Path, PathBuf};

use crate::{
    data::{self, Storage},
    load::{self, parse_tables::SchemaConfig},
    script,
};

pub struct CommandHandler {
    directory: PathBuf,
}

pub enum Command<'a> {
    Load,
    Run(&'a Path),
    Save,
    Query(String),
    Types,
    Table(String),
    Graph,
    Serve,
}
impl CommandHandler {
    pub fn new(directory: PathBuf) -> Self {
        Self { directory }
    }
}

impl CommandHandler {
    fn get_schema(&self) -> Option<SchemaConfig> {
        let schema = match load::load_config(&self.directory) {
            Ok(v) => v,
            Err(e) => {
                log::error!("Could not load config: {}", e);
                return None;
            }
        };
        Some(schema)
    }

    async fn save_project(&self) {
        let dir = &self.directory;
        log::debug!("Saving project to: {:?}", dir);

        let Some(schema) = self.get_schema() else {
            return;
        };
        match data::save(schema, dir).await {
            Ok(_) => log::info!("Successfully saved project"),
            Err(e) => log::error!("Failed to save project: {}", e),
        }
    }

    async fn run_script(&self, dir: &Path) {
        let Some(schema) = self.get_schema() else {
            return;
        };

        if let Err(e) = self.ensure_graph_loaded(&schema).await {
            log::error!("Failed to load graph data: {}", e);
            return;
        }

        let Some(script) = script::Script::load(dir) else {
            log::error!("Failed to load script at given directory");
            return;
        };
        if let Err(e) = script.run(&schema).await {
            log::error!("Failed to run script: {}", e);
        }
    }

    async fn ensure_graph_loaded(&self, schema: &SchemaConfig) -> anyhow::Result<()> {
        if Storage::nodes_file_for_project(&self.directory).exists()
            && Storage::edges_file_for_project(&self.directory).exists()
        {
            return Ok(());
        }

        let data = load::load_data(&self.directory.join("data"), schema)?;
        data::init(schema.clone(), data, &self.directory).await?;
        Ok(())
    }
    pub async fn accept(&self, mut args: impl Iterator<Item = String>) {
        let Some(cmd) = args.next() else {
            log::info!("Invalid command, valid commands are: load <dir>\nrun <path>");
            return;
        };
        match cmd.as_str() {
            "load" => {
                if let None = args.next() {
                    self.handle(Command::Load).await
                } else {
                    log::error!("Extraneous argument given, load uses current directory")
                }
            }
            "save" => {
                if let None = args.next() {
                    self.handle(Command::Save).await
                } else {
                    log::error!("Extraneous argument given, load uses current directory")
                }
            }
            "run" => {
                if let Some(p) = args.next() {
                    self.handle(Command::Run(&self.directory.join("scripts").join(p)))
                        .await
                } else {
                    log::error!(
                        "No script path given, path given is appended from working_directory/scripts/"
                    )
                }
            }
            "query" => {
                if let Some(q) = args.next() {
                    self.handle(Command::Query(q)).await
                } else {
                    log::error!("No query given.")
                }
            }
            "types" => {
                if let None = args.next() {
                    self.handle(Command::Types).await
                } else {
                    log::error!("Extraneous argument given, types uses current directory")
                }
            }
            "table" => {
                if let Some(label) = args.next() {
                    self.handle(Command::Table(label)).await
                } else {
                    log::error!("No type given.")
                }
            }
            "graph" => {
                if let None = args.next() {
                    self.handle(Command::Graph).await
                } else {
                    log::error!("Extraneous argument given, graph uses current directory")
                }
            }
            "serve" => {
                if let None = args.next() {
                    self.handle(Command::Serve).await
                } else {
                    log::error!("Extraneous argument given, serve uses stdio")
                }
            }
            _ => log::info!(
                "\nInvalid command, valid commands are:\nload\nrun <path>\nsave\nquery <graph query>\ntypes\ntable <type>\ngraph\nserve"
            ),
        }
    }
    async fn handle(&self, c: Command<'_>) {
        match c {
            Command::Load => self.load_project().await,
            Command::Run(p) => self.run_script(p).await,
            Command::Save => self.save_project().await,
            Command::Query(q) => {
                let Some(schema) = self.get_schema() else {
                    return;
                };
                if let Err(e) = self.ensure_graph_loaded(&schema).await {
                    log::error!("Failed to load graph data: {}", e);
                    return;
                }
                let result = if q.to_ascii_uppercase().contains(" SET ") {
                    match Storage::mutate_with_schema(&q, &schema, &self.directory).await {
                        Ok(rows) => {
                            println!("{rows}");
                            Ok(())
                        }
                        Err(e) => Err(e),
                    }
                } else {
                    match Storage::query(&q, &self.directory).await {
                        Ok(rows) => {
                            for row in rows {
                                let values = row
                                    .entries()
                                    .map(|(key, value)| (key.clone(), value.clone()))
                                    .collect();
                                println!("{}", serde_json::Value::Object(values));
                            }
                            Ok(())
                        }
                        Err(e) => Err(e),
                    }
                };

                if let Err(e) = result {
                    log::error!("Query failed: {}", e);
                }
            }
            Command::Types => {
                self.print_frontend_json(|schema, directory| Storage::type_view(schema, directory))
                    .await
            }
            Command::Table(label) => {
                self.print_frontend_json(|schema, directory| {
                    Storage::table_view(schema, directory, &label)
                })
                .await
            }
            Command::Graph => {
                self.print_frontend_json(|schema, directory| Storage::graph_view(schema, directory))
                    .await
            }
            Command::Serve => {
                if let Err(e) = crate::server::serve_stdio().await {
                    log::error!("Server failed: {}", e);
                }
            }
        }
    }

    async fn print_frontend_json(
        &self,
        build: impl FnOnce(&SchemaConfig, &Path) -> anyhow::Result<serde_json::Value>,
    ) {
        let Some(schema) = self.get_schema() else {
            return;
        };
        if let Err(e) = self.ensure_graph_loaded(&schema).await {
            log::error!("Failed to load graph data: {}", e);
            return;
        }
        match build(&schema, &self.directory) {
            Ok(value) => println!("{}", value),
            Err(e) => log::error!("Command failed: {}", e),
        }
    }
    async fn load_project(&self) {
        let dir = &self.directory;
        log::info!("Loading project at: {:?}", dir);
        let config = match load::load_config(&Path::new(dir)) {
            Ok(v) => v,
            Err(e) => {
                log::error!("Failed to load schema config: {e}");
                return;
            }
        };
        let data = match load::load_data(&Path::new(dir).join("data"), &config) {
            Ok(v) => v,
            Err(e) => {
                log::error!("Failed to load data: {e}");
                return;
            }
        };
        let graph_dir = match data::init(config, data, dir).await {
            Ok(v) => v,
            Err(e) => {
                log::error!("Failed to initialize data: {e}");
                return;
            }
        };
        println!("{}", graph_dir.display());
        log::info!("Successfully loaded project");
    }
}
