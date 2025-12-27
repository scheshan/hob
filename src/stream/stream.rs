use crate::stream::ss_table::SSTable;
use std::sync::Arc;

#[derive(Clone)]
pub struct Stream {
    name: Arc<String>,
    ss_table_list: Vec<Arc<SSTable>>,
}

impl Stream {
    pub fn new(name: String) -> Self {
        Self {
            name: Arc::new(name),
            ss_table_list: Vec::new()
        }
    }

    pub fn name(&self) -> &String {
        &self.name
    }

    pub fn ss_table_list(&self) -> Vec<Arc<SSTable>> {
        self.ss_table_list.clone()
    }

    pub fn extend_ss_table(&mut self, ss_table_list: Vec<Arc<SSTable>>) {
        self.ss_table_list.extend(ss_table_list)
    }
}
