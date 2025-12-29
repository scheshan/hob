use crate::arrow::ArrowSchema;
use crate::entry::EntryBatch;
use crate::storage::partition::Partitions;
use crate::storage::{PartitionData, PartitionKey};
use std::collections::HashMap;

pub struct MemTable {
    id: u64,
    approximate_size: usize,

    //The mem_table data is grouped first by stream_name, then by day, then by schema version.
    data: HashMap<String, Partitions>,
}

impl MemTable {
    pub fn new(id: u64) -> Self {
        Self {
            id,
            approximate_size: 0,
            data: HashMap::new(),
        }
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn approximate_size(&self) -> usize {
        self.approximate_size
    }

    pub fn add(
        &mut self,
        stream_name: &String,
        schema: ArrowSchema,
        daily_batches: HashMap<u64, EntryBatch>,
    ) {
        if !self.data.contains_key(stream_name) {
            self.data.insert(stream_name.clone(), Partitions::new());
        }
        let partitions = self.data.get_mut(stream_name).unwrap();

        for (day, batch) in daily_batches {
            match partitions.binary_search(stream_name, day, schema.version()) {
                Some(data) => {
                    self.approximate_size += data.add(batch);
                    continue;
                }
                None => {}
            }
            let mut data = PartitionData::new(schema.clone(), PartitionKey::new(stream_name.clone(), day, schema.version()));
            self.approximate_size += data.add(batch);
            partitions.insert(data);
        }
    }

    ///The mem_table data is grouped first by stream_name, then by day, then by schema version.
    pub fn partitions(&self) -> &HashMap<String, Partitions> {
        &self.data
    }
}
