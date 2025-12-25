use arrow_schema::SchemaRef;
use std::ops::Deref;

#[derive(Clone)]
pub struct ArrowSchema {
    schema: SchemaRef,
    version: u64,
}

impl ArrowSchema {
    pub fn new(schema: SchemaRef, version: u64) -> Self {
        Self { schema, version }
    }

    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Deref for ArrowSchema {
    type Target = SchemaRef;

    fn deref(&self) -> &Self::Target {
        &self.schema
    }
}
