use crate::arrow::array_builder::ArrowArrayBuilder;
use crate::arrow::{ArrowRecordBatch, ArrowSchema};
use crate::entry::{EntryBatch, FieldData};
use anyhow::anyhow;
use arrow_array::{Array, ArrayRef, BinaryArray, BooleanArray, Float64Array, Int64Array, RecordBatch, UInt64Array};
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Arc;
use arrow_schema::DataType;

pub struct ArrowRecordBatchStream {
    schema: ArrowSchema,
    array_builders: Vec<ArrowArrayBuilder>,
    heap: BinaryHeap<HeapWrapper>,
    batch_size: usize,
    cnt: usize,
}

impl ArrowRecordBatchStream {
    pub fn new(schema: ArrowSchema) -> Self {
        Self::new_with_batch_size(schema, 819200)
    }

    pub fn new_with_batch_size(schema: ArrowSchema, batch_size: usize) -> Self {
        let mut array_builders = Vec::new();
        for field in &schema.fields {
            array_builders.push(ArrowArrayBuilder::new(field));
        }

        Self {
            schema,
            array_builders,
            heap: BinaryHeap::new(),
            batch_size,
            cnt: 0,
        }
    }

    pub fn next_record_batch(&mut self) -> Option<ArrowRecordBatch> {
        while self.cnt < self.batch_size && !self.heap.is_empty() {
            let mut wrapper = self.heap.pop().unwrap();

            self.append(&wrapper);

            self.cnt += 1;

            wrapper.next();
            if wrapper.has_next() {
                wrapper.next();
                self.heap.push(wrapper);
            }
        }

        if self.cnt == 0 {
            return None;
        }
        self.cnt = 0;

        Some(self.build_record_batch())
    }

    pub fn add_entry_batch(&mut self, batch: Arc<EntryBatch>) {
        let wrapper = HeapWrapper::entry_batch(batch);
        self.heap.push(wrapper);
    }

    pub fn add_record_batch(&mut self, batch: Arc<ArrowRecordBatch>) -> crate::Result<()> {
        let wrapper = HeapWrapper::record_batch(batch)?;
        self.heap.push(wrapper);

        Ok(())
    }

    fn append(&mut self, wrapper: &HeapWrapper) {
        match wrapper {
            HeapWrapper::EntryBatch(b) => self.append_entry_batch(b),
            HeapWrapper::RecordBatch(b) => self.append_record_batch(b),
        }
    }

    fn append_entry_batch(&mut self, batch: &EntryBatchWrapper) {
        let entry = &batch.batch.entries[batch.ind];

        for i in 0..self.schema.fields.len() {
            let field = self.schema.field(i);
            let array_builder = self.array_builders.get_mut(i).unwrap();

            if field.name() == "__id__" {
                array_builder.add_record_value(Some(&FieldData::U64(entry.id)));
            } else if field.name() == "__time__" {
                array_builder.add_record_value(Some(&FieldData::U64(entry.time)));
            } else {
                let data = entry.fields.get(field.name());
                array_builder.add_record_value(data);
            }
        }
    }

    fn append_record_batch(&mut self, batch: &RecordBatchWrapper) {
        for i in 0..self.schema.fields.len() {
            let field = self.schema.field(i);
            let array_builder = self.array_builders.get_mut(i).unwrap();
            let array = batch.batch.record_ref().column(i);

            match field.data_type() {
                DataType::Boolean => {
                    let array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
                    if array.is_null(batch.ind) {
                        array_builder.add_record_value(None);
                    } else {
                        array_builder.add_record_value(Some(&FieldData::Bool(array.value(batch.ind))));
                    }
                }
                DataType::Int64 => {
                    let array = array.as_any().downcast_ref::<Int64Array>().unwrap();
                    if array.is_null(batch.ind) {
                        array_builder.add_record_value(None);
                    } else {
                        array_builder.add_record_value(Some(&FieldData::I64(array.value(batch.ind))));
                    }
                }
                DataType::UInt64 => {
                    let array = array.as_any().downcast_ref::<UInt64Array>().unwrap();
                    if array.is_null(batch.ind) {
                        array_builder.add_record_value(None);
                    } else {
                        array_builder.add_record_value(Some(&FieldData::U64(array.value(batch.ind))));
                    }
                }
                DataType::Float64 => {
                    let array = array.as_any().downcast_ref::<Float64Array>().unwrap();
                    if array.is_null(batch.ind) {
                        array_builder.add_record_value(None);
                    } else {
                        array_builder.add_record_value(Some(&FieldData::F64(array.value(batch.ind))));
                    }
                }
                _ => {
                    let array = array.as_any().downcast_ref::<BinaryArray>().unwrap();
                    if array.is_null(batch.ind) {
                        array_builder.add_record_value(None);
                    } else {
                        array_builder.add_record_value(Some(&FieldData::String(String::from_utf8_lossy(array.value(batch.ind)).to_string())));
                    }
                }
            }
        }
    }

    fn build_record_batch(&mut self) -> ArrowRecordBatch {
        let mut arrays = Vec::with_capacity(self.array_builders.len());
        for builder in &mut self.array_builders {
            arrays.push(builder.build());
        }

        let record = RecordBatch::try_new(self.schema.schema(), arrays).unwrap();
        ArrowRecordBatch::new(self.schema.clone(), record)
    }
}

struct EntryBatchWrapper {
    batch: Arc<EntryBatch>,
    ind: usize,
}

struct RecordBatchWrapper {
    batch: Arc<ArrowRecordBatch>,
    ind: usize,
    time_column: ArrayRef,
    id_column: ArrayRef,
}

enum HeapWrapper {
    EntryBatch(EntryBatchWrapper),
    RecordBatch(RecordBatchWrapper),
}

impl HeapWrapper {
    pub fn entry_batch(batch: Arc<EntryBatch>) -> Self {
        let wrapper = EntryBatchWrapper { batch, ind: 0 };
        Self::EntryBatch(wrapper)
    }

    pub fn record_batch(batch: Arc<ArrowRecordBatch>) -> crate::Result<Self> {
        let Some(time_column) = batch.record_ref().column_by_name("__time__") else {
            return Err(anyhow!("Cannot find __time__ column"));
        };
        let time_column = time_column.clone();
        let Some(id_column) = batch.record_ref().column_by_name("__id__") else {
            return Err(anyhow!("Cannot find __id__ column"));
        };
        let id_column = id_column.clone();

        let wrapper = RecordBatchWrapper {
            batch,
            ind: 0,
            time_column,
            id_column,
        };
        Ok(Self::RecordBatch(wrapper))
    }

    pub fn has_next(&self) -> bool {
        match self {
            HeapWrapper::EntryBatch(b) => b.ind < b.batch.entries.len() - 1,
            HeapWrapper::RecordBatch(b) => b.ind < b.batch.num_rows() - 1,
        }
    }

    pub fn current_key(&self) -> (u64, u64) {
        match self {
            HeapWrapper::EntryBatch(b) => {
                let e = b.batch.entries.get(b.ind).unwrap();
                (e.time, e.id)
            }
            HeapWrapper::RecordBatch(b) => {
                let time_column = b
                    .time_column
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap();
                let id_column = b.id_column.as_any().downcast_ref::<UInt64Array>().unwrap();

                (time_column.value(b.ind), id_column.value(b.ind))
            }
        }
    }

    pub fn next(&mut self) {
        match self {
            HeapWrapper::EntryBatch(b) => b.ind += 1,
            HeapWrapper::RecordBatch(b) => b.ind += 1,
        }
    }
}

impl Eq for HeapWrapper {}

impl PartialEq<Self> for HeapWrapper {
    fn eq(&self, other: &Self) -> bool {
        let left_key = self.current_key();
        let right_key = other.current_key();

        left_key.0 == right_key.0 && left_key.1 == right_key.1
    }
}

impl PartialOrd<Self> for HeapWrapper {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapWrapper {
    fn cmp(&self, other: &Self) -> Ordering {
        let left_key = self.current_key();
        let right_key = other.current_key();

        if left_key.0 > right_key.0 {
            Ordering::Less
        } else if left_key.0 < right_key.0 {
            Ordering::Greater
        } else {
            right_key.1.cmp(&left_key.1)
        }
    }
}
