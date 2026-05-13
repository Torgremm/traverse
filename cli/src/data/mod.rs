use std::path::{Path, PathBuf};

use crate::load::{parse_data::DataFile, parse_tables::SchemaConfig};
use anyhow::Result;
#[cfg(test)]
pub use init::GraphNode;
pub(crate) use init::validate_property_value;
pub use init::{Graph, QueryRow, Storage};

mod init;
mod query;
mod save;

pub async fn init(schema: SchemaConfig, data: DataFile, path: &Path) -> Result<PathBuf> {
    let graph_path = Storage::init(schema, data, path).await?;
    Ok(graph_path)
}
pub async fn save(schema: SchemaConfig, path: &Path) -> Result<()> {
    Storage::save(&schema, path).await
}
