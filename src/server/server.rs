use crate::Result;
use crate::arg::Args;
use crate::arrow::{ArrowRecordBatchStream, ArrowSchema};
use crate::entry::EntryBatch;
use crate::id::IdGenerator;
use crate::schema::{SchemaStore, infer_schema, need_evolve_schema};
use crate::stream::{MemTable, Stream};
use std::collections::HashMap;
use std::mem;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

#[derive(Clone)]
pub struct Server {
    args: Args,
    id_generator: IdGenerator,
    schema_store: SchemaStore,
    streams: Arc<RwLock<HashMap<String, Stream>>>,
    mem_table_id: Arc<AtomicU64>,
    inner: Arc<RwLock<ServerInner>>,
}

impl Server {
    pub fn new(id_generator: IdGenerator, schema_store: SchemaStore, args: Args) -> Self {
        Self {
            id_generator,
            schema_store,
            streams: Arc::new(RwLock::new(HashMap::new())),
            args,
            mem_table_id: Arc::new(AtomicU64::new(1)),
            inner: Arc::new(RwLock::new(ServerInner::new())),
        }
    }

    pub fn get_stream(&self, name: String) -> Stream {
        let guard = self.streams.read().unwrap();
        let stream = guard.get(&name).map(|s| s.clone());
        drop(guard);
        if stream.is_some() {
            return stream.unwrap();
        }

        let mut guard = self.streams.write().unwrap();
        let stream = guard.entry(name.clone()).or_insert_with(|| {
            Stream::new(
                name,
                self.args.clone(),
                self.schema_store.clone(),
                self.id_generator.clone(),
            )
        });
        stream.clone()
    }

    pub fn all_streams(&self) -> Vec<Stream> {
        let guard = self.streams.read().unwrap();
        guard.iter().map(|e| e.1.clone()).collect()
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
        inner
            .mem_table
            .add(stream_name, arrow_schema, arrow_record_batch);

        if inner.mem_table.approximate_size() > self.args.mem_table_size {
            log::info!("Generate new mem_table for stream: {}", stream_name);
            let new_mem_table = MemTable::new(self.next_mem_table_id());
            let old_mem_table = mem::replace(&mut inner.mem_table, new_mem_table);
            inner.mem_table_list.push(Arc::new(old_mem_table));
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

    pub fn next_mem_table_id(&self) -> u64 {
        self.mem_table_id.fetch_add(1, Ordering::Relaxed)
    }
}

struct ServerInner {
    mem_table: MemTable,
    mem_table_list: Vec<Arc<MemTable>>,
}

impl ServerInner {
    pub fn new() -> Self {
        Self {
            mem_table: MemTable::new(0),
            mem_table_list: Vec::new(),
        }
    }
}
