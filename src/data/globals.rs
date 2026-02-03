use crate::{
    data::Storage,
    load::parse_tables::{ColumnConfig, TableConfig},
};
use serde_json::Value;
use sqlx::Column;
use sqlx::Row;
use std::path::{Path, PathBuf};
pub struct Global {
    name: String,
    value: String,
    children: Vec<String>,
}
use tera::Tera;

impl Global {
    pub async fn resolve_with_tera(&self, tera: &mut Tera, depth: usize) -> anyhow::Result<String> {
        const MAX_DEPTH: usize = 10;

        if depth > MAX_DEPTH {
            return Err(anyhow::anyhow!(
                "Maximum recursion depth of {} exceeded at global '{}'",
                MAX_DEPTH,
                self.name
            ));
        }

        if self.children.is_empty() {
            let mut context = tera::Context::new();
            context.insert("object_id", &self.name);
            return tera.render_str(&self.value, &context).map_err(Into::into);
        }

        let placeholders = self
            .children
            .iter()
            .map(|_| "?")
            .collect::<Vec<_>>()
            .join(", ");
        let query = format!("SELECT * FROM _GLOBALS_ WHERE name IN ({})", placeholders);
        let rows = Storage::query(&query, todo!()).await?;

        let mut scope = tera::Map::new();
        for row in rows {
            let name: String = row.try_get("name")?;
            let value: String = row.try_get("value")?;

            let children: Vec<String> = {
                let col = row
                    .columns()
                    .iter()
                    .find(|&c| c.name() == "children")
                    .expect("children column missing");
                match Storage::parse_col(&row, col)? {
                    Value::String(s) => serde_json::from_str(&s)?,
                    _ => vec![],
                }
            };
            let child_global = Global {
                name: name.clone(),
                value,
                children,
            };
            let rendered_child = child_global.resolve_with_tera(tera, depth + 1).await?;
            scope.insert(name, tera::Value::String(rendered_child));
        }

        let mut context = tera::Context::new();
        context.insert("object_id", &self.name);
        for (k, v) in scope {
            context.insert(&k, &v);
        }

        tera.render_str(&self.value, &context).map_err(Into::into)
    }
}

impl Storage {}
