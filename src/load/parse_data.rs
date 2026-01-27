use serde_json::Value;
pub struct RawData {
    pub d: Vec<DataObject>,
}

pub struct DataObject {
    key: String,
    data: String,
}

impl DataObject {
    pub fn new(key: &str, data: &str) -> Self {
        Self {
            key: key.to_string(),
            data: serde_json::from_str(data)
                .expect("Failed to serialize object: {key}, Corrupt data"),
        }
    }
}
