use std::path::Path;

use crate::load::{
    parse_data::{DataObject, RawData},
    parse_tables::{Config, Table},
};

mod parse_data;
mod parse_tables;

pub fn load_config(_dir: &Path) -> Result<Config, std::io::Error> {
    let mut vec = Vec::new();
    vec.push(Table::new("t", "t"));

    Ok(Config { tables: vec })
}
pub fn load_data(_dir: &Path) -> Result<RawData, std::io::Error> {
    let mut vec = Vec::new();
    vec.push(DataObject::new("t", "t"));

    Ok(RawData { d: vec })
}
