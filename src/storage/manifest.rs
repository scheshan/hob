use crate::storage::SSTableKey;
use crate::Result;
use anyhow::anyhow;
use bytes::{Buf, Bytes};
use parquet::file::reader::ChunkReader;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tokio_util::bytes::{BufMut, BytesMut};

const MANIFEST_FILE_NAME: &str = "MANIFEST";

#[derive(Debug)]
pub enum ManifestRecord {
    NewMemTable(u64),
    FlushMemTable(u64, Vec<SSTableKey>),
}

#[derive(Clone)]
pub struct ManifestWriter {
    inner: Arc<Mutex<File>>,
}

impl ManifestWriter {
    pub fn new(dir: Arc<PathBuf>) -> Result<Self> {
        let path = dir.join(MANIFEST_FILE_NAME);
        let file = OpenOptions::new().create(true).append(true).open(&path)?;

        Ok(Self {
            inner: Arc::new(Mutex::new(file)),
        })
    }

    pub fn write(&self, record: ManifestRecord) -> Result<()> {
        let mut buf = BytesMut::new();
        buf.put_u64(0); //The frame length, we will calculate it later.
        match record {
            ManifestRecord::NewMemTable(id) => {
                buf.put_u8(1);
                buf.put_u64(id);
            }
            ManifestRecord::FlushMemTable(mem_table_id, list) => {
                buf.put_u8(2);
                buf.put_u64(mem_table_id);
                buf.put_u64(list.len() as u64);
                for ss_table_key in list {
                    buf.put_u64(ss_table_key.stream_name().len() as u64);
                    buf.put_slice(ss_table_key.stream_name().as_bytes());
                    buf.put_u64(ss_table_key.day());
                    buf.put_u64(ss_table_key.version());
                    buf.put_u64(ss_table_key.id())
                }
            }
        }

        //Set the frame length
        let len = buf.len() - 8;
        let len_bytes = (len as u64).to_be_bytes();
        for i in 0..8 {
            buf[i] = len_bytes[i];
        }

        let mut guard = self.inner.lock().unwrap();
        guard.write(&buf)?;
        guard.sync_all()?;

        Ok(())
    }
}

pub struct ManifestReader {
    file: File,
}

impl ManifestReader {
    pub fn new(dir: Arc<PathBuf>) -> Result<Self> {
        let path = dir.join(MANIFEST_FILE_NAME);
        let file = OpenOptions::new().read(true).open(&path)?;
        Ok(Self { file })
    }

    pub fn read(mut self) -> Result<Vec<ManifestRecord>> {
        let mut vec = Vec::new();
        let mut len_buf = [0u8; 8];

        loop {
            let size = self.file.read(&mut len_buf)?;
            if size == 0 {
                break;
            }
            if size < 8 {
                return Err(anyhow!("Invalid manifest file"));
            }

            let len = u64::from_be_bytes(len_buf) as usize;
            let mut data_buf = vec![0u8; len];
            let size = self.file.read(&mut data_buf)?;
            if size < len {
                return Err(anyhow!("Invalid manifest file"));
            }

            let mut data = Bytes::from(data_buf);
            let typ = data.get_u8();
            match typ {
                1 => {
                    let id = data.get_u64();
                    let record = ManifestRecord::NewMemTable(id);
                    vec.push(record);
                }
                2 => {
                    let mem_table_id = data.get_u64();
                    let list_len = data.get_u64() as usize;
                    let mut list = Vec::with_capacity(list_len);
                    for i in 0..list_len {
                        let stream_name_length = data.get_u64() as usize;
                        let stream_name = data.get_bytes(0, stream_name_length)?;
                        let stream_name = String::from_utf8_lossy(&stream_name).to_string();
                        data.advance(stream_name_length);
                        let day = data.get_u64();
                        let version = data.get_u64();
                        let ss_table_id = data.get_u64();

                        list.push(SSTableKey::new_raw(stream_name, day, version, ss_table_id));
                    }
                    let record = ManifestRecord::FlushMemTable(mem_table_id, list);
                    vec.push(record);
                }
                _ => {
                    return Err(anyhow!("Invalid manifest file"));
                }
            }
        }

        Ok(vec)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use tempfile::tempdir;
    use crate::storage::{ManifestReader, ManifestRecord, ManifestWriter, SSTableKey};

    #[test]
    fn test_write_and_read() -> crate::Result<()> {
        let dir = tempdir()?;
        let writer = ManifestWriter::new(Arc::new(dir.path().to_path_buf()))?;

        writer.write(ManifestRecord::NewMemTable(1000))?;
        writer.write(ManifestRecord::NewMemTable(2000))?;
        writer.write(ManifestRecord::FlushMemTable(100, vec![
            SSTableKey::new_raw("test1".to_string(), 20251230, 1, 1),
            SSTableKey::new_raw("test1".to_string(), 20251231, 1, 2),
            SSTableKey::new_raw("test1".to_string(), 20251232, 1, 3),
        ]))?;
        writer.write(ManifestRecord::NewMemTable(3000))?;

        drop(writer);

        let reader = ManifestReader::new(Arc::new(dir.path().to_path_buf()))?;

        let vec = reader.read()?;
        let ManifestRecord::NewMemTable(num) = &vec[0] else {
            panic!("Read failed");
        };
        assert_eq!(*num, 1000);
        let ManifestRecord::NewMemTable(num) = &vec[1] else {
            panic!("Read failed");
        };
        assert_eq!(*num, 2000);
        let ManifestRecord::FlushMemTable(id, ss_table_list) = &vec[2] else {
            panic!("Read failed");
        };
        assert_eq!(*id, 100);
        assert_eq!(ss_table_list.len(), 3);
        assert_eq!(ss_table_list[0].stream_name(), "test1");
        assert_eq!(ss_table_list[0].day(), 20251230);

        Ok(())
    }
}