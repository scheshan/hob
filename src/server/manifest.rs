use crate::Result;
use std::fs::{File, OpenOptions};
use std::io::{Error, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use anyhow::anyhow;
use tokio_util::bytes::{BufMut, BytesMut};
use bytes::{Buf, Bytes};
use parquet::file::reader::ChunkReader;
use crate::storage::SSTableKey;

pub enum ManifestRecord {
    NewMemTable(u64),
    FlushMemTable(u64, Vec<SSTableKey>),
}

#[derive(Clone)]
pub struct Manifest {
    dir: Arc<PathBuf>,
    inner: Arc<Mutex<ManifestInner>>,
}

impl Manifest {
    pub fn new(dir: PathBuf) -> Result<Self> {
        let path = dir.join("MANIFEST");
        let file = OpenOptions::new().create(true).write(true).open(&path)?;

        Ok(Self {
            dir: Arc::new(dir),
            inner: Arc::new(Mutex::new(ManifestInner::new(file))),
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
        guard.file.write(&buf)?;
        guard.file.sync_all()?;

        Ok(())
    }

    pub async fn load(&self) -> Result<Vec<ManifestRecord>> {
        let mut vec = Vec::new();
        let mut len_buf = [0u8; 8];
        let mut guard = self.inner.lock().unwrap();
        guard.file.seek(SeekFrom::Start(0))?;

        loop {
            let size = guard.file.read(&mut len_buf)?;
            if size == 0 {
                break;
            }
            if size < 8 {
                return Err(anyhow!("Invalid manifest file"));
            }

            let len = u64::from_be_bytes(len_buf) as usize;
            let mut data_buf = vec![0u8; len];
            let size = guard.file.read(&mut data_buf)?;
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

struct ManifestInner {
    file: File,
}

impl ManifestInner {
    pub fn new(file: File) -> Self {
        Self { file }
    }
}
