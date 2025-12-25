pub struct SSTable {
    id: u64,
}

impl SSTable {
    pub fn new(id: u64) -> Self {
        Self {id}
    }
}