use crate::entry::FieldData;
use arrow_array::ArrayRef;
use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Float64Builder, Int64Builder, UInt64Builder,
};
use arrow_schema::{DataType, FieldRef};
use std::str::FromStr;
use std::sync::Arc;

pub enum ArrowArrayBuilder {
    String(BinaryBuilder),
    Boolean(BooleanBuilder),
    Int64(Int64Builder),
    UInt64(UInt64Builder),
    Float64(Float64Builder),
}

impl ArrowArrayBuilder {
    pub fn new(field: &FieldRef) -> Self {
        match field.data_type() {
            DataType::Boolean => ArrowArrayBuilder::Boolean(BooleanBuilder::new()),
            DataType::Int64 => ArrowArrayBuilder::Int64(Int64Builder::new()),
            DataType::UInt64 => ArrowArrayBuilder::UInt64(UInt64Builder::new()),
            DataType::Float64 => ArrowArrayBuilder::Float64(Float64Builder::new()),
            _ => ArrowArrayBuilder::String(BinaryBuilder::new()),
        }
    }

    pub fn add_record_value(&mut self, data: Option<&FieldData>) {
        match self {
            ArrowArrayBuilder::String(builder) => {
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
            ArrowArrayBuilder::Boolean(builder) => {
                if data.is_none() {
                    builder.append_null();
                    return;
                }

                let data = data.unwrap();
                match data {
                    FieldData::String(v) => {
                        if v.eq_ignore_ascii_case("true") {
                            builder.append_value(true)
                        } else if v.eq_ignore_ascii_case("false") {
                            builder.append_value(false)
                        } else {
                            builder.append_null()
                        }
                    }
                    FieldData::Bool(v) => builder.append_value(*v),
                    FieldData::I64(v) => builder.append_value(*v != 0),
                    FieldData::U64(v) => builder.append_value(*v != 0),
                    FieldData::F64(v) => builder.append_value(*v != 0.0),
                }
            }
            ArrowArrayBuilder::Int64(builder) => {
                if data.is_none() {
                    builder.append_null();
                    return;
                }

                let data = data.unwrap();
                match data {
                    FieldData::String(v) => match i64::from_str(v) {
                        Ok(num) => builder.append_value(num),
                        Err(e) => builder.append_null(),
                    },
                    FieldData::Bool(v) => builder.append_value(match v {
                        true => 1,
                        false => 0,
                    }),
                    FieldData::I64(v) => builder.append_value(*v),
                    FieldData::U64(v) => builder.append_value(*v as i64),
                    FieldData::F64(v) => builder.append_value(*v as i64),
                }
            }
            ArrowArrayBuilder::UInt64(builder) => {
                if data.is_none() {
                    builder.append_null();
                    return;
                }

                let data = data.unwrap();
                match data {
                    FieldData::String(v) => match u64::from_str(v) {
                        Ok(num) => builder.append_value(num),
                        Err(_) => builder.append_null(),
                    },
                    FieldData::Bool(v) => builder.append_value(match v {
                        true => 1,
                        false => 0,
                    }),
                    FieldData::I64(v) => builder.append_value(*v as u64),
                    FieldData::U64(v) => builder.append_value(*v),
                    FieldData::F64(v) => builder.append_value(*v as u64),
                }
            }
            ArrowArrayBuilder::Float64(builder) => {
                if data.is_none() {
                    builder.append_null();
                    return;
                }

                let data = data.unwrap();
                match data {
                    FieldData::String(v) => match f64::from_str(v) {
                        Ok(num) => builder.append_value(num),
                        Err(_) => builder.append_null(),
                    },
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

    pub fn build(&mut self) -> ArrayRef {
        match self {
            ArrowArrayBuilder::String(builder) => Arc::new(builder.finish()),
            ArrowArrayBuilder::Boolean(builder) => Arc::new(builder.finish()),
            ArrowArrayBuilder::Int64(builder) => Arc::new(builder.finish()),
            ArrowArrayBuilder::UInt64(builder) => Arc::new(builder.finish()),
            ArrowArrayBuilder::Float64(builder) => Arc::new(builder.finish()),
        }
    }
}
