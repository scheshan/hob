use crate::arg::Args;
use crate::arrow::{ArrowRecordBatchStream, ArrowSchema};
use crate::entry::EntryBatch;
use crate::server::id::IdGenerator;
use crate::schema::{infer_schema, need_evolve_schema, SchemaStore};
use crate::stream::{MemTable, SSTable, Stream};
use crate::Result;
use parquet::arrow::AsyncArrowWriter;
use std::cmp::max;
use std::collections::HashMap;
use std::mem;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use tokio::fs::OpenOptions;
use crate::server::manifest::{Manifest, ManifestRecord};

#[derive(Clone)]
pub struct Server {
    args: Args,
    id_generator: IdGenerator,
    schema_store: SchemaStore,
    mem_table_id: Arc<AtomicU64>,
    ss_table_id: Arc<AtomicU64>,
    inner: Arc<RwLock<ServerInner>>,
    manifest: Manifest,
}

impl Server {
    pub fn new(id_generator: IdGenerator, schema_store: SchemaStore, manifest: Manifest, args: Args) -> Self {
        Self {
            id_generator,
            schema_store,
            args,
            mem_table_id: Arc::new(AtomicU64::new(1)),
            ss_table_id: Arc::new(AtomicU64::new(1)),
            inner: Arc::new(RwLock::new(ServerInner::new())),
            manifest,
        }
    }

    pub fn args_ref(&self) -> &Args {
        &self.args
    }

    pub fn ingest(&self, stream_name: &String, mut batch: EntryBatch) -> Result<()> {
        if batch.entries.is_empty() {
            return Ok(());
        }

        self.populate_id(&mut batch);

        let arrow_schema = self.generate_schema(stream_name, &batch)?;

        let mut stream =
            ArrowRecordBatchStream::new_with_batch_size(arrow_schema.clone(), batch.entries.len());
        stream.add_entry_batch(Arc::new(batch));
        let arrow_record_batch = stream.next_record_batch().unwrap();

        let mut inner = self.inner.write().unwrap();
        if !inner.streams.contains_key(stream_name) {
            inner.streams.insert(stream_name.clone(), Stream::new(stream_name.clone()));
        }

        inner
            .mem_table
            .add(stream_name, arrow_schema, arrow_record_batch);

        if inner.mem_table.approximate_size() > self.args.mem_table_size {
            log::info!("Generate new mem_table for stream: {}", stream_name);
            let mem_table_id = self.next_mem_table_id();
            let new_mem_table = MemTable::new(mem_table_id);
            let old_mem_table = mem::replace(&mut inner.mem_table, new_mem_table);
            inner.mem_table_list.push(Arc::new(old_mem_table));
            self.manifest.write(ManifestRecord::NewMemTable(mem_table_id))?;
        }

        Ok(())
    }

    pub async fn flush_mem_table(&self) -> Result<()> {
        let mem_table_list = self.im_mem_table_list();

        if mem_table_list.is_empty() {
            return Ok(());
        }

        //flush ss_table
        let mut max_mem_table_id = 0;
        let mut all_stream_builder: HashMap<String, HashMap<u64, ArrowRecordBatchStream>> =
            HashMap::new();
        for mem_table in mem_table_list {
            for (stream_name, partitions) in mem_table.streams() {
                let stream_builder = all_stream_builder
                    .entry(stream_name.clone())
                    .or_insert_with(HashMap::new);

                for (par_key, par) in partitions {
                    let partition_builder = stream_builder
                        .entry(*par_key)
                        .or_insert_with(|| ArrowRecordBatchStream::new(par.schema().clone()));

                    for batch in par.batches() {
                        match partition_builder.add_record_batch(batch.clone()) {
                            Err(e) => log::error!("Add record_batch failed: {}", e),
                            Ok(_) => {}
                        }
                    }
                }
            }
            max_mem_table_id = max(max_mem_table_id, mem_table.id());
        }

        let mut stream_ss_table: HashMap<String, Vec<Arc<SSTable>>> = HashMap::new();
        let mut manifest_record_vec = Vec::new();
        for (stream_name, partitions) in all_stream_builder {
            let mut ss_table_list = Vec::new();

            for (par_key, builder) in partitions {
                let ss_table_id = self.next_ss_table_id();
                let ss_table = self
                    .generate_ss_table(ss_table_id, &stream_name, par_key, builder)
                    .await?;
                ss_table_list.push(Arc::new(ss_table));
                manifest_record_vec.push((stream_name.clone(), par_key, ss_table_id));
            }
            stream_ss_table.insert(stream_name, ss_table_list);
        }
        self.manifest.write(ManifestRecord::FlushMemTable(max_mem_table_id, manifest_record_vec))?;

        //replace memory data
        let mut guard = self.inner.write().unwrap();
        guard.mem_table_list = guard
            .mem_table_list
            .iter()
            .filter(|m| m.id() > max_mem_table_id)
            .map(|m| m.clone())
            .collect();

        for (stream_name, ss_table_list) in stream_ss_table {
            let stream = guard.streams.get_mut(&stream_name).unwrap();
            stream.extend_ss_table(ss_table_list);
        }

        Ok(())
    }

    fn populate_id(&self, batch: &mut EntryBatch) {
        let mut id_range = self.id_generator.generate_n(batch.entries.len());
        for entry in &mut batch.entries {
            entry.id = id_range.next().unwrap()
        }
    }

    fn generate_schema(&self, stream_name: &String, batch: &EntryBatch) -> Result<ArrowSchema> {
        loop {
            let infer_schema = infer_schema(&batch.entries[0]);
            match self.schema_store.get(stream_name) {
                None => {
                    //if schema not exists, try to store the inferred schema to store
                    let arrow_schema = ArrowSchema::new(infer_schema.clone(), 1);
                    if self.schema_store.set(stream_name, 0, arrow_schema.clone()) {
                        return Ok(arrow_schema);
                    }
                }
                Some(exist_schema) => {
                    match need_evolve_schema(exist_schema.schema(), infer_schema) {
                        None => {
                            //if no need to evolve, return the exist schema
                            return Ok(exist_schema);
                        }
                        Some(new_schema) => {
                            //try to store the combined schema
                            let arrow_schema =
                                ArrowSchema::new(new_schema, exist_schema.version() + 1);
                            if self.schema_store.set(
                                stream_name,
                                exist_schema.version(),
                                arrow_schema.clone(),
                            ) {
                                return Ok(arrow_schema);
                            }
                        }
                    }
                }
            }
        }
    }

    fn next_mem_table_id(&self) -> u64 {
        self.mem_table_id.fetch_add(1, Ordering::Relaxed)
    }

    fn next_ss_table_id(&self) -> u64 {
        self.ss_table_id.fetch_add(1, Ordering::Relaxed)
    }

    async fn generate_ss_table(
        &self,
        id: u64,
        stream_name: &String,
        par_key: u64,
        mut stream: ArrowRecordBatchStream,
    ) -> Result<SSTable> {
        let mut path: PathBuf = PathBuf::from(self.args.root_dir.as_str());
        path.push("data");
        path.push(stream_name);
        path.push(par_key.to_string());
        path.push(format!("{}.bin", id));

        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(path)
            .await?;
        let mut arrow_writer = AsyncArrowWriter::try_new(file, stream.schema(), None)?;
        loop {
            match stream.next_record_batch() {
                Some(rb) => {
                    arrow_writer.write(rb.record_ref()).await?;
                }
                None => break,
            }
        }
        arrow_writer.flush().await?;

        Ok(SSTable::new(id))
    }

    fn im_mem_table_list(&self) -> Vec<Arc<MemTable>> {
        let guard = self.inner.read().unwrap();
        guard.mem_table_list.clone()
    }
}

struct ServerInner {
    mem_table: MemTable,
    mem_table_list: Vec<Arc<MemTable>>,
    streams: HashMap<String, Stream>,
}

impl ServerInner {
    pub fn new() -> Self {
        Self {
            mem_table: MemTable::new(0),
            mem_table_list: Vec::new(),
            streams: HashMap::new(),
        }
    }
}
