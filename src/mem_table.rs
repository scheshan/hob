use crate::entry::EntryBatch;
use crate::record::RecordBatchBuilder;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use std::sync::Arc;

#[derive(Clone)]
pub struct MemTable {
    data: Vec<Arc<RecordBatch>>,
    approximate_size: usize,
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            data: Vec::new(),
            approximate_size: 0,
        }
    }

    pub fn add(&mut self, schema: SchemaRef, batch: EntryBatch) {
        let mut record_builder = RecordBatchBuilder::new(schema);
        for entry in batch.entries {
            record_builder.add_record(entry);
        }

        let rb = record_builder.build();
        self.approximate_size += rb.get_array_memory_size();
        self.data.push(Arc::new(rb));
    }

    pub fn approximate_size(&self) -> usize {
        self.approximate_size
    }
}
