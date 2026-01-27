pub struct Config {
    pub tables: Vec<Table>,
}

pub struct Table {
    name: String,
    columns: Vec<String>,
    key: String,
}

impl Table {
    pub fn new(name: &str, content: &str) -> Self {
        let columns = Table::get_columns(content);
        let key = Table::get_key(content);
        Self {
            name: name.to_string(),
            columns,
            key,
        }
    }

    fn get_columns(content: &str) -> Vec<String> {
        Vec::new()
    }

    fn get_key(contrent: &str) -> String {
        String::new()
    }
}
