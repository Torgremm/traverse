use crate::load::{parse_data::DataFile, parse_tables::SchemaConfig};
use anyhow::Result;
pub use init::Storage;
use once_cell::sync::OnceCell;

mod init;
mod query;

static STORAGE: OnceCell<Storage> = OnceCell::new();

pub async fn init(schema: SchemaConfig, data: DataFile) -> Result<()> {
    let storage = Storage::new(schema, data).await?;
    STORAGE
        .set(storage)
        .map_err(|_| anyhow::anyhow!("Storage already initialized"))?;
    Ok(())
}
pub fn get_storage() -> &'static Storage {
    STORAGE.get().expect("Storage not initialized")
}
