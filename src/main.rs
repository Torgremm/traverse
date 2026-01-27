use anyhow::Result;
use std::path::Path;

mod data;
mod load;
mod script;

#[tokio::main()]
async fn main() -> Result<()> {
    env_logger::Builder::from_default_env();
    let dir = "placeholder";
    let dir = Path::new(dir);
    loop {
        if load_project(dir).await.is_none() {
            continue;
        }
        //Load config and data into SQLite in memory
        log::info!("Successfully loaded project");
    }
    Ok(())
}

async fn load_project(dir: &Path) -> Option<()> {
    let config = match load::load_config(&Path::new(dir)) {
        Ok(v) => v,
        Err(e) => {
            log::error!("{e}");
            return None;
        }
    };
    let _data = match load::load_data(&Path::new(dir), config) {
        Ok(v) => Some(v),
        Err(e) => {
            log::error!("{e}");
            None
        }
    };
    None
    // let _data = load::load_data(&Path::new(dir))?;
}
