use anyhow::Result;
use std::path::Path;

mod data;
mod load;
mod script;

#[tokio::main()]
async fn main() -> Result<()> {
    let dir = "placeholder";
    let _config = load::load_config(&Path::new(dir))?;
    let _data = load::load_data(&Path::new(dir))?;
    Ok(())
}
