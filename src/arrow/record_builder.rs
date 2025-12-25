use crate::arrow::record::ArrowRecordBatch;
use crate::arrow::ArrowSchema;
use crate::entry::{Entry, EntryBatch, FieldData};
use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Float64Builder, Int64Builder, UInt64Builder,
};
use arrow_array::{ArrayRef, RecordBatch, RecordBatchOptions};
use arrow_schema::{DataType, FieldRef};
use std::cmp;
use std::str::FromStr;
use std::sync::Arc;

pub struct RecordBatchBuilder {
    schema: ArrowSchema,
    array_builders: Vec<ArrayBuilder>,
}

impl RecordBatchBuilder {
    pub fn new(schema: ArrowSchema) -> Self {
        let mut array_builders = Vec::new();
        for field in &schema.fields {
            let array_builder = ArrayBuilder::new(field);
            array_builders.push(array_builder);
        }

        Self {
            schema,
            array_builders,
        }
    }

    pub fn build_with_entry_batch(mut self, mut batch: EntryBatch) -> ArrowRecordBatch {
        batch.entries.sort_by(|l, r| {
            if l.time > r.time {
                cmp::Ordering::Less
            } else if l.time < r.time {
                cmp::Ordering::Greater
            } else {
                r.id.cmp(&l.id)
            }
        });

        for entry in batch.entries {
            if !self.record_is_valid(&entry) {
                continue;
            } else {
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
        }

        self.build()
    }

    pub fn build_with_record_batch(mut self, batches: Vec<ArrowRecordBatch>) -> ArrowRecordBatch {
        todo!()
    }

    fn record_is_valid(&self, entry: &Entry) -> bool {
        for i in 0..self.schema.fields.len() {
            let field = self.schema.field(i);
            let array_builder = self.array_builders.get(i).unwrap();

            if field.name() == "__id__" {
                continue;
            } else if field.name() == "__time__" {
                continue;
            } else {
                if let Some(data) = entry.fields.get(field.name()) {
                    if !array_builder.is_compatible(data) {
                        return false;
                    }
                }
            }
        }

        true
    }

    fn build(self) -> ArrowRecordBatch {
        let mut arrays = Vec::with_capacity(self.array_builders.len());
        for builder in self.array_builders {
            arrays.push(builder.build());
        }

        let opt = RecordBatchOptions::new();

        let record = RecordBatch::try_new(self.schema.schema(), arrays).unwrap();
        ArrowRecordBatch::new(self.schema, record)
    }
}

enum ArrayBuilder {
    String(BinaryBuilder),
    Boolean(BooleanBuilder),
    Int64(Int64Builder),
    UInt64(UInt64Builder),
    Float64(Float64Builder),
}

impl ArrayBuilder {
    pub fn new(field: &FieldRef) -> Self {
        match field.data_type() {
            DataType::Boolean => ArrayBuilder::Boolean(BooleanBuilder::new()),
            DataType::Int64 => ArrayBuilder::Int64(Int64Builder::new()),
            DataType::UInt64 => ArrayBuilder::UInt64(UInt64Builder::new()),
            DataType::Float64 => ArrayBuilder::Float64(Float64Builder::new()),
            _ => ArrayBuilder::String(BinaryBuilder::new()),
        }
    }

    pub fn add_record_value(&mut self, data: Option<&FieldData>) {
        match self {
            ArrayBuilder::String(builder) => {
                if data.is_none() {
                    builder.append_null();
                    return;
                }

                let data = data.unwrap();
                match data {
                    FieldData::String(v) => builder.append_value(v),
                    FieldData::Bool(v) => builder.append_value(v.to_string()),
                    FieldData::I64(v) => builder.append_value(v.to_string()),
                    FieldData::U64(v) => builder.append_value(v.to_string()),
                    FieldData::F64(v) => builder.append_value(v.to_string()),
                }
            }
            ArrayBuilder::Boolean(builder) => {
                if data.is_none() {
                    builder.append_null();
                    return;
                }

                let data = data.unwrap();
                match data {
                    FieldData::String(v) => builder.append_value(v.eq_ignore_ascii_case("true")),
                    FieldData::Bool(v) => builder.append_value(*v),
                    FieldData::I64(v) => builder.append_value(*v != 0),
                    FieldData::U64(v) => builder.append_value(*v != 0),
                    FieldData::F64(v) => builder.append_value(*v != 0.0),
                }
            }
            ArrayBuilder::Int64(builder) => {
                if data.is_none() {
                    builder.append_null();
                    return;
                }

                let data = data.unwrap();
                match data {
                    FieldData::String(v) => builder.append_value(i64::from_str(v).unwrap()),
                    FieldData::Bool(v) => builder.append_value(match v {
                        true => 1,
                        false => 0,
                    }),
                    FieldData::I64(v) => builder.append_value(*v),
                    FieldData::U64(v) => builder.append_value(*v as i64),
                    FieldData::F64(v) => builder.append_value(*v as i64),
                }
            }
            ArrayBuilder::UInt64(builder) => {
                if data.is_none() {
                    builder.append_null();
                    return;
                }

                let data = data.unwrap();
                match data {
                    FieldData::String(v) => builder.append_value(u64::from_str(v).unwrap()),
                    FieldData::Bool(v) => builder.append_value(match v {
                        true => 1,
                        false => 0,
                    }),
                    FieldData::I64(v) => builder.append_value(*v as u64),
                    FieldData::U64(v) => builder.append_value(*v),
                    FieldData::F64(v) => builder.append_value(*v as u64),
                }
            }
            ArrayBuilder::Float64(builder) => {
                if data.is_none() {
                    builder.append_null();
                    return;
                }

                let data = data.unwrap();
                match data {
                    FieldData::String(v) => builder.append_value(f64::from_str(v).unwrap()),
                    FieldData::Bool(v) => builder.append_value(match v {
                        true => 1f64,
                        false => 0f64,
                    }),
                    FieldData::I64(v) => builder.append_value(*v as f64),
                    FieldData::U64(v) => builder.append_value(*v as f64),
                    FieldData::F64(v) => builder.append_value(*v),
                }
            }
        }
    }

    fn is_compatible(&self, field: &FieldData) -> bool {
        match (self, field) {
            (ArrayBuilder::Boolean(_), FieldData::String(s)) => {
                s.eq_ignore_ascii_case("true") || s.eq_ignore_ascii_case("false")
            }
            (ArrayBuilder::Int64(_), FieldData::String(s)) => i64::from_str(s).is_ok(),
            (ArrayBuilder::UInt64(_), FieldData::String(s)) => u64::from_str(s).is_ok(),
            (ArrayBuilder::Float64(_), FieldData::String(s)) => f64::from_str(s).is_ok(),

            (_, _) => true,
        }
    }

    pub fn build(self) -> ArrayRef {
        match self {
            ArrayBuilder::String(mut builder) => Arc::new(builder.finish()),
            ArrayBuilder::Boolean(mut builder) => Arc::new(builder.finish()),
            ArrayBuilder::Int64(mut builder) => Arc::new(builder.finish()),
            ArrayBuilder::UInt64(mut builder) => Arc::new(builder.finish()),
            ArrayBuilder::Float64(mut builder) => Arc::new(builder.finish()),
        }
    }
}