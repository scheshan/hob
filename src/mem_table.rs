use arrow_array::RecordBatch;
use std::sync::Arc;
use arrow::datatypes::SchemaRef;
use crate::entry::EntryBatch;

#[derive(Clone)]
pub struct MemTable {
    data: Vec<Arc<RecordBatch>>,
    approximate_size: usize,
}

impl MemTable {
    pub fn add(&mut self, schema: SchemaRef, batch: EntryBatch) {
        let rb = batch.to_record_batch(schema);
        self.approximate_size += rb.get_array_memory_size();
        self.data.push(Arc::new(rb));
    }
}
