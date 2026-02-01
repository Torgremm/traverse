use std::path::{Path, PathBuf};

use crate::{data, load, script};

pub struct CommandHandler {}

pub enum Command<'a> {
    Load(&'a Path),
    Run(&'a Path),
}

impl CommandHandler {
    pub async fn accept(mut args: impl Iterator<Item = String>) {
        let Some(cmd) = args.next() else {
            log::info!("Invalid command, valid commands are: load <dir>\nrun <path>");
            return;
        };
        match cmd.as_str() {
            "load" => {
                if let Some(p) = args.next() {
                    CommandHandler::handle(Command::Load(Path::new(&p))).await
                } else {
                    log::error!("No path in argument")
                }
            }
            "run" => {
                if let Some(p) = args.next() {
                    CommandHandler::handle(Command::Run(Path::new(&p))).await
                } else {
                    log::error!("No path in argument")
                }
            }
            _ => log::info!("Invalid command, valid commands are:\n load <dir>\nrun <path>"),
        }
    }
    async fn handle(c: Command<'_>) {
        match c {
            Command::Load(p) => load_project(p).await,
            Command::Run(p) => run_script(p).await,
        }
    }
}
async fn load_project(dir: &Path) {
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
    match data::init(config, data, dir).await {
        Ok(_) => {}
        Err(e) => {
            log::error!("{e}");
            return;
        }
    }
    log::info!("Successfully loaded project");
}

async fn run_script(dir: &Path) {
    let root_dir = script::Script::get_root_dir(dir).unwrap();
    let schema = load::load_config(root_dir).unwrap();
    let script = script::Script::load(dir).unwrap();
    script.run(&schema).await.unwrap();
}
