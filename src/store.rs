use crate::arrow::ArrowSchema;
use crate::entry::EntryBatch;
use crate::id::IdGenerator;
use crate::mem_table::MemTable;
use crate::schema::{infer_schema, need_evolve_schema, SchemaStore};
use crate::Result;
use std::mem;
use std::sync::{Arc, RwLock};

#[derive(Clone)]
pub struct Store {
    name: String,
    schema_store: Arc<SchemaStore>,
    id_generator: Arc<IdGenerator>,
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

        let arrow_schema = self.generate_schema(&batch)?;

        let mut inner = self.inner.write().unwrap();
        inner.mem_table.add(arrow_schema, batch);

        if inner.mem_table.approximate_size() > 8 * 1024 * 1024 {
            let new_mem_table = MemTable::new();
            let old_mem_table = mem::replace(&mut inner.mem_table, new_mem_table);
            inner.mem_table_list.push(Arc::new(old_mem_table));
        }

        Ok(())
    }

    fn generate_schema(&self, batch: &EntryBatch) -> Result<ArrowSchema> {
        loop {
            let infer_schema = infer_schema(&batch.entries[0]);
            match self.schema_store.get(&self.name) {
                None => {
                    //if schema not exists, try to store the inferred schema to store
                    let arrow_schema = ArrowSchema::new(infer_schema.clone(), 1);
                    if self.schema_store.set(&self.name, 0, arrow_schema.clone()) {
                        return Ok(arrow_schema)
                    }
                }
                Some(exist_schema) => {
                    match need_evolve_schema(exist_schema.schema(), infer_schema) {
                        None => {
                            //if no need to evolve, return the exist schema
                            return Ok(exist_schema);
                        },
                        Some(new_schema) => {
                            //try to store the combined schema
                            let arrow_schema = ArrowSchema::new(new_schema, exist_schema.version() + 1);
                            if self.schema_store.set(&self.name, exist_schema.version(), arrow_schema.clone()) {
                                return Ok(arrow_schema);
                            }
                        }
                    }
                }
            }
        }
    }
}

struct StoreInner {
    mem_table: MemTable,
    mem_table_list: Vec<Arc<MemTable>>
}