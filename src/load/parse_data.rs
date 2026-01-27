use anyhow::{Result, anyhow};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::path::Path;

use crate::load::parse_tables::SchemaConfig;

type Row = HashMap<String, Value>;
type DataFile = HashMap<String, Vec<Row>>;

pub fn load_data(dir: &Path, schema: SchemaConfig) -> Result<DataFile> {
    let json = std::fs::read_to_string(dir).map_err(|e| anyhow::anyhow!("{e}"))?;
    let data: DataFile = serde_json::from_str(&json)?;
    validate(&data, &schema)?;
    Ok(data)
}

fn validate(data: &DataFile, schema: &SchemaConfig) -> Result<()> {
    let table_map: HashMap<_, _> = schema.tables.iter().map(|t| (&t.name, t)).collect();

    for (table_name, rows) in data {
        let table = table_map
            .get(table_name)
            .ok_or_else(|| anyhow!("Data contains unknown table `{}`", table_name))?;

        let column_map: HashMap<_, _> = table
            .columns
            .iter()
            .map(|c| (&c.name, &c.col_type))
            .collect();

        let mut seen_pks = HashSet::new();

        for (row_index, row) in rows.iter().enumerate() {
            for (col_name, value) in row {
                let col_type = column_map.get(col_name).ok_or_else(|| {
                    anyhow!(
                        "Table `{}` row {}: unknown column `{}`",
                        table_name,
                        row_index,
                        col_name
                    )
                })?;

                validate_type(table_name, row_index, col_name, col_type, value)?;
            }

            let pk_value = row.get(&table.primary_key).ok_or_else(|| {
                anyhow!(
                    "Table `{}` row {}: missing primary key `{}`",
                    table_name,
                    row_index,
                    table.primary_key
                )
            })?;

            if !seen_pks.insert(pk_value.clone()) {
                return Err(anyhow!(
                    "Table `{}`: duplicate primary key value `{}`",
                    table_name,
                    pk_value
                ));
            }
        }
    }

    for table in &schema.tables {
        if let Some(rows) = data.get(&table.name) {
            for fk in &table.foreign_keys {
                let target_rows = data.get(&fk.references.table).ok_or_else(|| {
                    anyhow!(
                        "Table `{}` references missing table `{}`",
                        table.name,
                        fk.references.table
                    )
                })?;

                let mut target_values = HashSet::new();
                for r in target_rows {
                    if let Some(v) = r.get(&fk.references.column) {
                        target_values.insert(v.clone());
                    }
                }

                for (i, row) in rows.iter().enumerate() {
                    if let Some(v) = row.get(&fk.column) {
                        if !target_values.contains(v) {
                            return Err(anyhow!(
                                "FK violation: `{}`.`{}` = {} (row {}) does not exist in `{}`.`{}`",
                                table.name,
                                fk.column,
                                v,
                                i,
                                fk.references.table,
                                fk.references.column
                            ));
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

fn validate_type(table: &str, row: usize, col: &str, col_type: &str, value: &Value) -> Result<()> {
    let ok = match col_type {
        "int" => value.is_i64() || value.is_u64(),
        "float" => value.is_f64(),
        "text" => value.is_string(),
        "bool" => value.is_boolean(),
        _ => false,
    };

    if ok {
        Ok(())
    } else {
        Err(anyhow!(
            "Type error: `{}` row {} column `{}` expected {}, got {}",
            table,
            row,
            col,
            col_type,
            value
        ))
    }
}
