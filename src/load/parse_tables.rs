use anyhow::{Result, anyhow};
use serde::Deserialize;
use std::collections::HashSet;
use std::path::Path;

#[derive(Debug, Deserialize, Clone)]
pub struct SchemaConfig {
    pub tables: Vec<TableConfig>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct TableConfig {
    pub name: String,
    pub primary_key: String,
    pub columns: Vec<ColumnConfig>,
    #[serde(default)]
    pub foreign_keys: Vec<ForeignKeyConfig>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ColumnConfig {
    pub name: String,
    #[serde(rename = "type")]
    pub col_type: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ForeignKeyConfig {
    pub column: String,
    pub references: ReferenceConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ReferenceConfig {
    pub table: String,
    pub column: String,
}

pub fn load_config(dir: &Path) -> Result<SchemaConfig> {
    let json = std::fs::read_to_string(dir).expect("Failed to read config file");
    let schema: SchemaConfig =
        serde_json::from_str(&json).expect("Failed to serialize the config, unexpected pattern");

    match validate(&schema) {
        Ok(_) => Ok(schema),
        Err(e) => Err(anyhow!(e)),
    }
}
fn validate(schema: &SchemaConfig) -> Result<(), String> {
    let table_names: HashSet<_> = schema.tables.iter().map(|t| t.name.as_str()).collect();

    for table in &schema.tables {
        let col_names: HashSet<_> = table.columns.iter().map(|c| c.name.as_str()).collect();

        if !col_names.contains(table.primary_key.as_str()) {
            return Err(format!(
                "Table {}: primary key {} not in columns",
                table.name, table.primary_key
            ));
        }

        for fk in &table.foreign_keys {
            if !col_names.contains(fk.column.as_str()) {
                return Err(format!(
                    "Table {}: FK column {} not found",
                    table.name, fk.column
                ));
            }
            if !table_names.contains(fk.references.table.as_str()) {
                return Err(format!(
                    "Table {}: referenced table {} not found",
                    table.name, fk.references.table
                ));
            }
        }
    }
    Ok(())
}
