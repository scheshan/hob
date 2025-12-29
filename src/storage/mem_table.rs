use crate::arrow::ArrowSchema;
use crate::entry::EntryBatch;
use crate::storage::partition::Partitions;
use crate::storage::{PartitionData, PartitionKey};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use chrono::{DateTime, Datelike};
use crate::storage::wal::WALWriter;
use crate::Result;

pub struct MemTable {
    id: u64,
    approximate_size: usize,

    wal_writer: WALWriter,

    //The mem_table data is grouped first by stream_name, then by day, then by schema version.
    data: HashMap<String, Partitions>,
}

impl MemTable {
    pub fn new(root_dir: Arc<PathBuf>, id: u64) -> Result<Self> {
        let wal_writer = WALWriter::new(root_dir, id)?;

        Ok(Self {
            id,
            approximate_size: 0,
            wal_writer,
            data: HashMap::new(),
        })
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
        batch: EntryBatch,
    ) -> Result<()> {
        if !self.data.contains_key(stream_name) {
            self.data.insert(stream_name.clone(), Partitions::new());
        }

        self.wal_writer.write(&batch)?;

        let daily_batches = self.group_entry_batch_by_day(batch);
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

        Ok(())
    }

    ///The mem_table data is grouped first by stream_name, then by day, then by schema version.
    pub fn partitions(&self) -> &HashMap<String, Partitions> {
        &self.data
    }

    fn group_entry_batch_by_day(&self, mut batch: EntryBatch) -> HashMap<u64, EntryBatch> {
        let mut res = HashMap::new();

        batch.sort();

        let mut last_key = self.get_day(batch.entries[0].time);
        let mut last_batch = EntryBatch::new();

        for entry in batch.entries {
            let day_key = self.get_day(entry.time);
            if day_key != last_key {
                res.insert(last_key, last_batch);
                last_batch = EntryBatch::new();
                last_key = day_key;
            }

            last_batch.add(entry);
        }

        if !last_batch.entries.is_empty() {
            res.insert(last_key, last_batch);
        }

        res
    }

    fn get_day(&self, entry_time: u64) -> u64 {
        let time = DateTime::from_timestamp_millis(entry_time as i64).unwrap();
        time.year() as u64 * 10000 + time.month() as u64 * 100 + time.day() as u64
    }
}
