use crate::Result;
use crate::arrow::{ArrowRecordBatchStream, ArrowSchema};
use crate::entry::EntryBatch;
use crate::id::IdGenerator;
use crate::schema::{SchemaStore, infer_schema, need_evolve_schema};
use crate::stream::MemTable;
use crate::stream::ss_table::SSTable;
use std::{cmp, mem};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use crate::arg::Args;

#[derive(Clone)]
pub struct Stream {
    name: Arc<String>,
    args: Args,
    schema_store: SchemaStore,
    id_generator: IdGenerator,
    inner: Arc<RwLock<StreamInner>>,
    mem_table_id: Arc<AtomicU64>,
    ss_table_id: Arc<AtomicU64>,
}

impl Stream {
    pub fn new(name: String, args: Args, schema_store: SchemaStore, id_generator: IdGenerator) -> Self {
        Self {
            name: Arc::new(name),
            args,
            schema_store,
            id_generator,
            inner: Arc::new(RwLock::new(StreamInner {
                mem_table: MemTable::new(0),
                mem_table_list: Vec::new(),
                ss_table_list: Vec::new(),
            })),
            mem_table_id: Arc::new(AtomicU64::new(1)),
            ss_table_id: Arc::new(AtomicU64::new(1)),
        }
    }

    pub fn add(&self, mut batch: EntryBatch) -> Result<()> {
        if batch.entries.is_empty() {
            return Ok(());
        }

        self.populate_id(&mut batch);

        let arrow_schema = self.generate_schema(&batch)?;

        let mut stream = ArrowRecordBatchStream::new(arrow_schema.clone());
        let mut arrow_record_batch_list = Vec::new();
        loop {
            match stream.next_record_batch() {
                Some(record) => arrow_record_batch_list.push(record),
                None => break,
            }
        }

        let mut inner = self.inner.write().unwrap();
        inner.mem_table.add(arrow_schema, arrow_record_batch_list);

        if inner.mem_table.approximate_size() > self.args.mem_table_size {
            log::info!("Generate new mem_table for stream: {}", self.name);
            let new_mem_table = MemTable::new(self.next_mem_table_id());
            let old_mem_table = mem::replace(&mut inner.mem_table, new_mem_table);
            inner.mem_table_list.push(Arc::new(old_mem_table));
        }

        Ok(())
    }

    pub fn name(&self) -> &String {
        &self.name
    }

    pub fn clone_mem_table_list(&self) -> Vec<Arc<MemTable>> {
        let guard = self.inner.read().unwrap();
        guard.mem_table_list.clone()
    }

    pub fn next_mem_table_id(&self) -> u64 {
        self.mem_table_id.fetch_add(1, Ordering::Relaxed)
    }

    pub fn next_ss_table_id(&self) -> u64 {
        self.ss_table_id.fetch_add(1, Ordering::Relaxed)
    }

    pub fn flush_completed(&self, ss_table_list: Vec<SSTable>, max_mem_table_id: u64) {
        let mut guard = self.inner.write().unwrap();
        let mem_table_list = guard
            .mem_table_list
            .iter()
            .filter(|m| m.id() > max_mem_table_id)
            .map(|m| m.clone())
            .collect();
        guard.mem_table_list = mem_table_list;

        for ss_table in ss_table_list {
            guard.ss_table_list.push(Arc::new(ss_table));
        }
    }

    fn populate_id(&self, batch: &mut EntryBatch) {
        let mut id_range = self.id_generator.generate_n(batch.entries.len());
        for entry in &mut batch.entries {
            entry.id = id_range.next().unwrap()
        }
    }

    fn generate_schema(&self, batch: &EntryBatch) -> Result<ArrowSchema> {
        loop {
            let infer_schema = infer_schema(&batch.entries[0]);
            match self.schema_store.get(&self.name) {
                None => {
                    //if schema not exists, try to store the inferred schema to store
                    let arrow_schema = ArrowSchema::new(infer_schema.clone(), 1);
                    if self.schema_store.set(&self.name, 0, arrow_schema.clone()) {
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
                                &self.name,
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
}

#[derive(Clone)]
struct StreamInner {
    mem_table: MemTable,
    mem_table_list: Vec<Arc<MemTable>>,
    ss_table_list: Vec<Arc<SSTable>>,
}
