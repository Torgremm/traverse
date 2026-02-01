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

    let args = std::env::args().skip(1);
    CommandHandler::accept(args).await;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    #[tokio::test]
    async fn should_not_crash() {
        env_logger::Builder::new()
            .filter_level(log::LevelFilter::Trace)
            .is_test(true)
            .init();

        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let test_dir = root.join("tests").join("test_dir");

        let mut args = Vec::new();
        args.push("load".to_string());
        args.push(test_dir.to_string_lossy().to_string());

        CommandHandler::accept(args.clone().into_iter()).await;

        let script_path = test_dir.join("scripts").join("valve_io.json");

        let mut args = Vec::new();
        args.push("run".to_string());
        args.push(script_path.to_string_lossy().to_string());

        CommandHandler::accept(args.into_iter()).await;
    }
}
