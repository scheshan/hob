use std::collections::HashMap;
use crate::arrow::{ArrowRecordBatch, ArrowSchema};
use std::sync::Arc;

type Streams = HashMap<String, Partitions>;
type Partitions = HashMap<u64, Partition>;
type Partition = Vec<Arc<ArrowRecordBatch>>;

#[derive(Clone)]
pub struct MemTable {
    id: u64,
    streams: Streams,
    approximate_size: usize,
}

impl MemTable {
    pub fn new(id: u64) -> Self {
        Self {
            id,
            streams: Streams::new(),
            approximate_size: 0,
        }
    }

    pub fn add(&mut self, stream_name: &String, arrow_schema: ArrowSchema, batch: ArrowRecordBatch) {
        let partitions = self.streams.entry(stream_name.clone()).or_insert_with(Partitions::new);
        let partition = partitions.entry(arrow_schema.version()).or_insert_with(Partition::new);
        partition.push(Arc::new(batch));
    }

    pub fn approximate_size(&self) -> usize {
        self.approximate_size
    }
    
    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn data(&self) -> &Vec<Arc<ArrowRecordBatch>> {
        todo!()
    }
}

