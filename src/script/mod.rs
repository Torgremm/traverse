use std::path::Path;

use crate::data::Storage;
use anyhow::Result;
use serde::Deserialize;
use sqlx::Column;
use sqlx::Row;
use sqlx::sqlite::SqliteColumn;
use sqlx::sqlite::SqliteRow;
use sqlx::{Arguments, sqlite::SqliteArguments};

#[derive(Debug, Deserialize)]
pub struct Script {
    fetch: String,
    act: String,
    #[serde(default = "default_char")]
    escape_char: char,
}

fn default_char() -> char {
    '|'
}

impl Script {
    pub fn load(path: &Path) -> Option<Self> {
        let raw_text = match std::fs::read_to_string(path) {
            Ok(v) => v,
            Err(e) => {
                log::error!("Failed to load script file: {:?} \n {}", path, e);
                return None;
            }
        };

        let s: Script = match serde_json::from_str(&raw_text) {
            Ok(v) => v,
            Err(e) => {
                log::error!("Failed to serialize script: {:?} \n {}", path, e);
                return None;
            }
        };

        Some(s)
    }
    pub async fn run(&self, storage: Storage) -> Result<()> {
        let data: Vec<SqliteRow> = storage.query(&self.fetch).await?;

        let first = match data.first() {
            Some(v) => v,
            None => {
                return Err(anyhow::anyhow!(
                    "Query returned 0 rows, check your FETCH section"
                ));
            }
        };
        let tokens: Vec<String> = first
            .columns()
            .iter()
            .map(|col| col.name().to_string())
            .collect();

        for row in data {
            let std_out = self.fill_data(row, &tokens);
            println!("{std_out}");
        }

        Ok(())
    }

    fn fill_data(&self, data: SqliteRow, tokens: &Vec<String>) -> String {
        let mut result = String::with_capacity(self.act.len() + 64);
        let act = &self.act;
        let mut i = 0;

        while i < act.len() {
            let mut matched = false;

            for name in tokens {
                let token_str = format!("{}{}{}", self.escape_char, name, self.escape_char);

                if act[i..].starts_with(&token_str) {
                    let value = data
                        .try_get::<String, _>(name.as_str())
                        .or_else(|_| data.try_get::<i64, _>(name.as_str()).map(|v| v.to_string()))
                        .unwrap_or_else(|_| name.clone());

                    result.push_str(&value);
                    i += token_str.len();
                    matched = true;
                    break;
                }
            }

            if !matched {
                let c = act[i..].chars().next().unwrap();
                result.push(c);
                i += c.len_utf8();
            }
        }

        result
    }
}
