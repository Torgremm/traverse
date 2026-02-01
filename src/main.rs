use anyhow::Result;
use std::io::{self, Write};
use std::path::Path;

use crate::arg_handler::CommandHandler;

mod arg_handler;
mod data;
mod load;
mod script;

#[tokio::main]
async fn main() -> Result<()> {
    //env_logger::Builder::from_default_env().init();
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Trace)
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
    async fn should_not_crash() {
        env_logger::Builder::new()
            .filter_level(log::LevelFilter::Info)
            .init();

        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let test_dir = root.join("tests").join("test_dir");

        let mut args = Vec::new();
        args.push("load".to_string());
        let handler = CommandHandler::new(test_dir);
        handler.accept(args.iter().map(|s| s.to_string())).await;

        let script_path = "valve_io.json".to_string();

        let mut args = Vec::new();
        args.push("run".to_string());
        args.push(script_path);
        handler.accept(args.iter().map(|s| s.to_string())).await;
    }
}
