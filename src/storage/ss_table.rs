use crate::Result;
use crate::arrow::{ArrowRecordBatch, ArrowSchema};
use crate::storage::partition::PartitionKey;
use parquet::arrow::AsyncArrowWriter;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::{File, OpenOptions, create_dir_all};

pub struct SSTable {
    key: SSTableKey,
}

impl SSTable {
    pub fn new(key: SSTableKey) -> Self {
        Self { key }
    }
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct SSTableKey {
    prefix: PartitionKey,
    id: u64,
}

impl SSTableKey {
    pub fn new(prefix: PartitionKey, id: u64) -> Self {
        Self { prefix, id }
    }

    pub fn new_raw(stream_name: String, day: u64, version: u64, id: u64) -> Self {
        let prefix = PartitionKey::new(stream_name, day, version);
        Self::new(prefix, id)
    }

    pub fn stream_name(&self) -> &str {
        self.prefix.stream_name()
    }

    pub fn day(&self) -> u64 {
        self.prefix.day()
    }

    pub fn version(&self) -> u64 {
        self.prefix.version()
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn prefix(&self) -> &PartitionKey {
        &self.prefix
    }
}

pub struct SSTableWriter {
    root_dir: Arc<String>,
    key: SSTableKey,
    schema: ArrowSchema,
    writer: AsyncArrowWriter<File>,
}

impl SSTableWriter {
    pub async fn try_new(
        root_dir: Arc<String>,
        key: SSTableKey,
        schema: ArrowSchema,
    ) -> Result<Self> {
        let dir = PathBuf::from(root_dir.as_str())
            .join("data")
            .join(&key.stream_name())
            .join(key.day().to_string());
        create_dir_all(&dir).await?;

        let path = dir.join(format!("{}_{}.bin", key.id(), key.version()));

        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)
            .await?;

        let writer = AsyncArrowWriter::try_new(file, schema.schema(), None)?;

        Ok(Self {
            root_dir,
            key,
            schema,
            writer,
        })
    }

    pub async fn write(&mut self, rb: ArrowRecordBatch) -> Result<()> {
        self.writer.write(rb.record_ref()).await?;
        Ok(())
    }

    pub async fn close(self) -> Result<()> {
        self.writer.close().await?;
        Ok(())
    }
}
