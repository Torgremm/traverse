use std::path::{Path, PathBuf};

use crate::{
    data,
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

        let Some(script) = script::Script::load(dir) else {
            log::error!("Failed to load script at given directory");
            return;
        };
        if let Err(e) = script.run(&schema).await {
            log::error!("Failed to run script: {}", e);
        }
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
            _ => log::info!("\nInvalid command, valid commands are:\nload <dir>\nrun <path>"),
        }
    }
    async fn handle(&self, c: Command<'_>) {
        match c {
            Command::Load => self.load_project().await,
            Command::Run(p) => self.run_script(p).await,
            Command::Save => self.save_project().await,
        }
    }
    async fn load_project(&self) {
        let dir = &self.directory;
        log::debug!("Loading project at: {:?}", dir);
        let config = match load::load_config(&Path::new(dir)) {
            Ok(v) => v,
            Err(e) => {
                log::error!("{e}");
                return;
            }
        };
        let data = match load::load_data(&Path::new(dir).join("data"), &config) {
            Ok(v) => v,
            Err(e) => {
                log::error!("{e}");
                return;
            }
        };
        let db_file = match data::init(config, data, dir).await {
            Ok(v) => v,
            Err(e) => {
                log::error!("{e}");
                return;
            }
        };
        println!("{}", db_file.display());
        log::info!("Successfully loaded project");
    }
}
