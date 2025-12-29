use crate::arrow::{ArrowRecordBatch, ArrowRecordBatchStream, ArrowSchema};
use crate::entry::EntryBatch;
use std::cmp::Ordering;
use std::sync::Arc;

#[derive(Clone, Hash, Eq, PartialEq, Ord, PartialOrd, Debug)]
pub struct PartitionKey {
    stream_name: String,
    day: u64,
    version: u64,
}

impl PartitionKey {
    pub fn new(stream_name: String, day: u64, version: u64) -> Self {
        Self {
            stream_name,
            day,
            version,
        }
    }

    pub fn stream_name(&self) -> &str {
        &self.stream_name
    }

    pub fn day(&self) -> u64 {
        self.day
    }

    pub fn version(&self) -> u64 {
        self.version
    }

    ///Compare without construct a PartitionKey, this can avoid a string copy
    pub fn compare(&self, stream_name: &String, day: u64, version: u64) -> Ordering {
        match self.stream_name.cmp(stream_name) {
            Ordering::Less => Ordering::Less,
            Ordering::Greater => Ordering::Greater,
            Ordering::Equal => match self.day.cmp(&day) {
                Ordering::Less => Ordering::Less,
                Ordering::Greater => Ordering::Greater,
                Ordering::Equal => self.version.cmp(&version),
            },
        }
    }
}

pub struct PartitionData {
    key: PartitionKey,
    schema: ArrowSchema,
    builder: ArrowRecordBatchStream,
    data: Vec<Arc<ArrowRecordBatch>>,
}

impl PartitionData {
    pub fn new(schema: ArrowSchema, key: PartitionKey) -> Self {
        Self {
            key,
            schema: schema.clone(),
            data: Vec::new(),
            builder: ArrowRecordBatchStream::new(schema),
        }
    }

    pub fn key(&self) -> &PartitionKey {
        &self.key
    }

    pub fn add(&mut self, batch: EntryBatch) -> usize {
        let mut size = 0;
        self.builder.add_entry_batch(Arc::new(batch));
        loop {
            match self.builder.next_record_batch() {
                None => break,
                Some(rb) => {
                    size += rb.memory_size();
                    self.data.push(Arc::new(rb));
                }
            }
        }

        size
    }

    pub fn schema(&self) -> ArrowSchema {
        self.schema.clone()
    }

    pub fn data(&self) -> Vec<Arc<ArrowRecordBatch>> {
        self.data.clone()
    }
}

pub struct Partitions {
    data: Vec<PartitionData>,
}

impl Partitions {
    pub fn new() -> Self {
        Self { data: Vec::new() }
    }

    pub fn binary_search(
        &mut self,
        stream_name: &String,
        day: u64,
        version: u64,
    ) -> Option<&mut PartitionData> {
        let mut l = 0;
        let mut r = self.data.len();

        while l < r {
            let mid = (l + r) >> 1;
            let pd = &self.data[mid];
            match pd.key().compare(stream_name, day, version) {
                Ordering::Less => l = mid + 1,
                Ordering::Greater => r = mid,
                _ => return Some(self.data.get_mut(mid).unwrap()),
            }
        }

        None
    }

    pub fn insert(&mut self, data: PartitionData) {
        self.data.push(data);
        self.data.sort_by(|l, r| l.key.cmp(r.key()));
    }

    pub fn data(&self) -> &Vec<PartitionData> {
        &self.data
    }
}
