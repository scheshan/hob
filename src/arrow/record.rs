use crate::arrow::schema::ArrowSchema;
use arrow_array::RecordBatch;

pub struct ArrowRecordBatch {
    record: RecordBatch,
    schema: ArrowSchema,
}

impl ArrowRecordBatch {
    pub fn new(schema: ArrowSchema, record: RecordBatch) -> Self {
        Self { schema, record }
    }

    pub fn arrow_schema_ref(&self) -> &ArrowSchema {
        &self.schema
    }

    pub fn arrow_schema(&self) -> ArrowSchema {
        self.schema.clone()
    }

    pub fn record_ref(&self) -> &RecordBatch {
        &self.record
    }

    pub fn record(&self) -> RecordBatch {
        self.record.clone()
    }

    pub fn memory_size(&self) -> usize {
        self.record.get_array_memory_size()
    }

    pub fn schema_version(&self) -> u64 {
        self.schema.version()
    }
}
