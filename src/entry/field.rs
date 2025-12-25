pub enum FieldData {
    String(String),
    Bool(bool),
    F64(f64),
    I64(i64),
    U64(u64),
}

impl FieldData {
    pub fn len(&self) -> usize {
        match self {
            FieldData::String(str) => str.len(),
            FieldData::Bool(_) => 1,
            FieldData::F64(_) => 8,
            FieldData::I64(_) => 8,
            FieldData::U64(_) => 8,
        }
    }
}