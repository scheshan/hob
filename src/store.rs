use std::sync::Arc;
use arrow::datatypes::SchemaRef;
use crate::mem_table::MemTable;
use crate::schema::SchemaResolver;

pub struct Store {
    schema_resolver: Arc<SchemaResolver>,
    schema: SchemaRef,
    mem_table: MemTable,
    mem_table_list: Vec<Arc<MemTable>>
}

impl Store {

}
