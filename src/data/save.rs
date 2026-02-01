use std::path::Path;

use crate::{data::Storage, load::parse_tables::SchemaConfig};
use anyhow::Result;

impl Storage {
    pub fn save(schema: SchemaConfig, path: &Path) -> Result<()> {
        let db_file = Storage::db_file_for_project(path);
        todo!("Implement saving");
    }
}
