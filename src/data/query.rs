use sqlx::{
    Result,
    sqlite::{SqliteQueryResult, SqliteRow},
};

use crate::data::init::Storage;

impl Storage {
    pub async fn query(&self, q: &String) -> Result<Vec<SqliteRow>> {
        let mut qb = sqlx::QueryBuilder::new(q);
        let result = qb.build().fetch_all(&self.pool).await?;
        Ok(result)
    }
}
