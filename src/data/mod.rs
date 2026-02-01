use std::path::{Path, PathBuf};

use crate::load::{parse_data::DataFile, parse_tables::SchemaConfig};
use anyhow::Result;
pub use init::Storage;

mod init;
mod query;
mod save;

pub async fn init(schema: SchemaConfig, data: DataFile, path: &Path) -> Result<PathBuf> {
    let db_path = Storage::init(schema, data, path).await?;
    Ok(db_path)
}
pub async fn save(schema: SchemaConfig, path: &Path) -> Result<()> {
    Storage::save(schema, path)
}
