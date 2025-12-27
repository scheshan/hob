use crate::arrow::{ArrowRecordBatch, ArrowSchema};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone)]
pub struct MemTable {
    id: u64,
    streams: HashMap<String, HashMap<u64, Partition>>,
    approximate_size: usize,
}

impl MemTable {
    pub fn new(id: u64) -> Self {
        Self {
            id,
            streams: HashMap::new(),
            approximate_size: 0,
        }
    }

    pub fn add(
        &mut self,
        stream_name: &String,
        arrow_schema: ArrowSchema,
        batch: ArrowRecordBatch,
    ) {
        let partitions = self
            .streams
            .entry(stream_name.clone())
            .or_insert_with(HashMap::new);
        let partition = partitions
            .entry(arrow_schema.version())
            .or_insert_with(|| Partition::new(arrow_schema));
        partition.add_batch(batch);
    }

    pub fn approximate_size(&self) -> usize {
        self.approximate_size
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn streams(&self) -> &HashMap<String, HashMap<u64, Partition>> {
        &self.streams
    }
}

#[derive(Clone)]
pub struct Partition {
    schema: ArrowSchema,
    batches: Vec<Arc<ArrowRecordBatch>>,
}

impl Partition {
    pub fn new(schema: ArrowSchema) -> Self {
        Self {
            schema,
            batches: Vec::new(),
        }
    }

    pub fn schema(&self) -> &ArrowSchema {
        &self.schema
    }

    pub fn add_batch(&mut self, batch: ArrowRecordBatch) {
        self.batches.push(Arc::new(batch));
    }

    pub fn batches(&self) -> &Vec<Arc<ArrowRecordBatch>> {
        &self.batches
    }
}
