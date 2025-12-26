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
            ss_table_id: Arc::new(AtomicU64::new(1)),
        }
    }

    pub fn name(&self) -> &String {
        &self.name
    }

    pub fn clone_mem_table_list(&self) -> Vec<Arc<MemTable>> {
        let guard = self.inner.read().unwrap();
        guard.mem_table_list.clone()
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
}

#[derive(Clone)]
struct StreamInner {
    mem_table: MemTable,
    mem_table_list: Vec<Arc<MemTable>>,
    ss_table_list: Vec<Arc<SSTable>>,
}
