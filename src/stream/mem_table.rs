use crate::arrow::{ArrowRecordBatch, ArrowSchema, RecordBatchBuilder};
use crate::entry::EntryBatch;
use std::sync::Arc;

#[derive(Clone)]
pub struct MemTable {
    id: u64,
    data: Vec<Arc<ArrowRecordBatch>>,
    approximate_size: usize,
}

impl MemTable {
    pub fn new(id: u64) -> Self {
        Self {
            id,
            data: Vec::new(),
            approximate_size: 0,
        }
    }

    pub fn add(&mut self, arrow_schema: ArrowSchema, batch: EntryBatch) {
        let mut record_builder = RecordBatchBuilder::new(arrow_schema);
        for entry in batch.entries {
            record_builder.add_record(entry);
        }

        let rb = record_builder.build();
        self.approximate_size += rb.memory_size();
        self.data.push(Arc::new(rb));
    }

    pub fn approximate_size(&self) -> usize {
        self.approximate_size
    }
    
    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn data(&self) -> &Vec<Arc<ArrowRecordBatch>> {
        &self.data
    }
}
