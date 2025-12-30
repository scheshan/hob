use crate::entry::EntryBatch;
use crate::{entry, Result};
use anyhow::anyhow;
use bytes::Bytes;
use std::fs::{remove_file, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::Arc;

pub struct WALWriter {
    file: File,
}

impl WALWriter {
    pub fn new(root_dir: Arc<PathBuf>, id: u64) -> Result<Self> {
        let path = wal_file_path(root_dir, id);
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(path)?;
        file.seek(SeekFrom::End(0))?;

        Ok(Self { file })
    }

    pub fn write(&mut self, buf: impl AsRef<[u8]>) -> Result<()> {
        self.file.write_all(buf.as_ref())?;
        self.file.sync_all()?;

        Ok(())
    }
}

pub struct WALReader {
    id: u64,
    file: File,
    len_buf: [u8; 8]
}

impl WALReader {
    pub fn new(root_dir: Arc<PathBuf>, id: u64) -> Result<Self> {
        let path = wal_file_path(root_dir, id);
        let file = OpenOptions::new()
            .read(true)
            .open(path)?;

        Ok(Self { id, file, len_buf: [0u8; 8] })
    }

    pub fn next_batch(&mut self) -> Result<Option<EntryBatch>> {
        let size = self.file.read(&mut self.len_buf)?;
        if size == 0 {
            return Ok(None);
        } else if size < 8 {
            return Err(anyhow!("Invalid wal file: {}", self.id));
        }

        let len = u64::from_be_bytes(self.len_buf) as usize;
        let mut buf = vec![0; len];
        self.file.read_exact(&mut buf[..])?;
        let batch = entry::decode_from_bytes(&mut Bytes::from(buf))?;

        Ok(Some(batch))
    }
}

pub fn remove_wal_file(root_dir: Arc<PathBuf>, id: u64) -> Result<()> {
    let path = wal_file_path(root_dir, id);
    remove_file(path)?;
    Ok(())
}

fn wal_file_path(root_dir: Arc<PathBuf>, id: u64) -> PathBuf {
    root_dir.join("wal").join(format!("{}.bin", id))
}