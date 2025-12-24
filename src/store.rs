use crate::entry::EntryBatch;
use crate::id::IdGenerator;
use crate::mem_table::MemTable;
use crate::schema::SchemaResolver;
use crate::Result;
use arrow::datatypes::SchemaRef;
use std::mem;
use std::sync::{Arc, RwLock};

#[derive(Clone)]
pub struct Store {
    schema_resolver: Arc<SchemaResolver>,
    id_generator: Arc<IdGenerator>,
    schema: SchemaRef,
    inner: Arc<RwLock<StoreInner>>
}

impl Store {
    pub fn add(&self, mut batch: EntryBatch) -> Result<()> {
        if batch.entries.is_empty() {
            return Ok(())
        }

        let mut id_range = self.id_generator.generate_n(batch.entries.len());
        for entry in &mut batch.entries {
            entry.id = id_range.next().unwrap()
        }

        let mut inner = self.inner.write().unwrap();
        inner.mem_table.add(self.schema.clone(), batch);

        if inner.mem_table.approximate_size() > 8 * 1024 * 1024 {
            let new_mem_table = MemTable::new();
            let old_mem_table = mem::replace(&mut inner.mem_table, new_mem_table);
            inner.mem_table_list.push(Arc::new(old_mem_table));
        }

        Ok(())
    }
}

struct StoreInner {
    mem_table: MemTable,
    mem_table_list: Vec<Arc<MemTable>>
}