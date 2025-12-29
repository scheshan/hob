use std::fs::{File, OpenOptions};
use std::io::Write;
use crate::Result;
use std::path::PathBuf;
use std::sync::Arc;
use bytes::BytesMut;
use crate::entry::EntryBatch;

pub struct WALWriter {
    file: File,
    buf: BytesMut,
}

impl WALWriter {
    pub fn new(root_dir: Arc<PathBuf>, id: u64) -> Result<Self> {
        let path = root_dir.join("wal").join(format!("{}.bin", id));
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)?;

        Ok(Self { file, buf: BytesMut::new() })
    }

    pub fn write(&mut self, batch: &EntryBatch) -> Result<()> {
        self.buf.clear();
        batch.encode_to_bytes(&mut self.buf);

        self.file.write_all(&self.buf)?;
        self.file.sync_all()?;

        Ok(())
    }
}
