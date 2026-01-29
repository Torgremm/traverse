use crate::load::{parse_data::DataFile, parse_tables::SchemaConfig};
pub use init::Storage;
use sqlx::Result;

mod init;
mod query;

pub async fn init(schema: SchemaConfig, data: DataFile) -> Result<Storage> {
    Storage::new(schema, data).await
}
