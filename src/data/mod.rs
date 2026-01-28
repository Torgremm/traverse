use crate::{
    data::init::Storage,
    load::{parse_data::DataFile, parse_tables::SchemaConfig},
};
use sqlx::Result;

mod init;
mod query;

pub async fn init(schema: SchemaConfig, data: DataFile) -> Result<Storage> {
    Storage::new(schema, data).await
}
