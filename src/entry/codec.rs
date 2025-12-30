use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use arrow_schema::{DataType, Field, Schema};
use bytes::{Buf, BufMut, Bytes, BytesMut, TryGetError};
use crate::arrow::ArrowSchema;
use crate::entry::{Entry, EntryBatch, FieldData};

pub fn encode_to_bytes(buf: &mut BytesMut, batch: &EntryBatch) {
    //todo add checksum

    //8 byte for frame length
    buf.put_u64(0);

    //region stream_name
    encode_string_to_bytes(buf, &batch.stream_name);
    //endregion

    //region schema

    //schema version
    buf.put_u64(batch.schema.version());

    //schema metadata
    buf.put_u64(batch.schema.metadata.len() as u64);
    for (k, v) in &batch.schema.metadata {
        encode_string_to_bytes(buf, k);
        encode_string_to_bytes(buf, v);
    }

    //schema fields
    buf.put_u64(batch.schema.fields.len() as u64);
    for field in &batch.schema.fields {
        encode_string_to_bytes(buf, field.name());
        encode_string_to_bytes(buf, &format!("{}", field.data_type()));
        if field.is_nullable() {
            buf.put_u8(1);
        } else {
            buf.put_u8(0);
        }
    }

    //endregion

    //region entries

    //8 byte for length
    buf.put_u64(batch.entries.len() as u64);

    for entry in &batch.entries {
        buf.put_u64(entry.time); //8 byte for time
        buf.put_u64(entry.id); //8 byte for id
        buf.put_u64(entry.fields.len() as u64); //8 byte for fields length

        for (field_name, field_data) in &entry.fields {
            encode_string_to_bytes(buf, field_name);
            match field_data {
                FieldData::String(str) => {
                    buf.put_u8(1);
                    encode_string_to_bytes(buf, str);
                }
                FieldData::Bool(b) => {
                    buf.put_u8(2);
                    if *b {
                        buf.put_u8(1);
                    } else {
                        buf.put_u8(0);
                    }
                }
                FieldData::I64(num) => {
                    buf.put_u8(3);
                    buf.put_i64(*num);
                }
                FieldData::U64(num) => {
                    buf.put_u8(3);
                    buf.put_u64(*num);
                }
                FieldData::F64(num) => {
                    buf.put_u8(3);
                    buf.put_f64(*num);
                }
            }
        }
    }
    //endregion

    //set the frame length
    let frame_length = (buf.len() as u64 - 8).to_be_bytes();
    for i in 0..8 {
        buf[i] = frame_length[i];
    }
}

pub fn decode_from_bytes(buf: &mut BytesMut) -> crate::Result<EntryBatch> {
    //region stream_name
    let stream_name = try_get_string_from_bytes(buf)?;
    //endregion

    //region schema
    let schema_version = buf.try_get_u64()?;

    let schema_metadata_len = buf.try_get_u64()? as usize;
    let mut schema_metadata = HashMap::with_capacity(schema_metadata_len);
    for i in 0..schema_metadata_len {
        let metadata_key = try_get_string_from_bytes(buf)?;
        let metadata_value = try_get_string_from_bytes(buf)?;
        schema_metadata.insert(metadata_key, metadata_value);
    }

    let schema_fields_len = buf.try_get_u64()? as usize;
    let mut schema_fields = Vec::with_capacity(schema_fields_len);
    for i in 0..schema_fields_len {
        let field_name = try_get_string_from_bytes(buf)?;
        let field_type = DataType::from_str(try_get_string_from_bytes(buf)?.as_str())?;
        let field_nullable = buf.try_get_u8()? == 1;

        schema_fields.push(Arc::new(Field::new(field_name, field_type, field_nullable)));
    }

    let schema = ArrowSchema::new(
        Arc::new(
            Schema::new_with_metadata(schema_fields, schema_metadata)
        ),
        schema_version
    );
    //endregion

    //region batches
    let len = buf.try_get_u64()? as usize;
    let mut batch = EntryBatch::new_with_capacity(stream_name, schema, len);

    for i in 0..len {
        let time = buf.try_get_u64()?;
        let id = buf.try_get_u64()?;
        let field_len = buf.try_get_u64()? as usize;
        let mut fields = HashMap::new();

        for j in 0..field_len {
            let key = try_get_string_from_bytes(buf)?;
            let typ = buf.try_get_u8()?;
            match typ {
                1 => {
                    let str = try_get_string_from_bytes(buf)?;
                    fields.insert(key, FieldData::String(str));
                }
                2 => {
                    let num = buf.try_get_u8()?;
                    fields.insert(key, FieldData::Bool(num == 1));
                }
                3 => {
                    let num = buf.try_get_i64()?;
                    fields.insert(key, FieldData::I64(num));
                }
                4 => {
                    let num = buf.try_get_u64()?;
                    fields.insert(key, FieldData::U64(num));
                }
                _ => {
                    let num = buf.try_get_f64()?;
                    fields.insert(key, FieldData::F64(num));
                }
            }
        }

        let entry = Entry {
            time,
            id,
            fields
        };
        batch.entries.push(entry);
    }

    //endregion

    Ok(batch)
}

fn encode_string_to_bytes(buf: &mut BytesMut, str: &String) {
    buf.put_u64(str.len() as u64);
    buf.put_slice(str.as_bytes());
}

fn try_get_string_from_bytes(buf: &mut BytesMut) -> Result<String, TryGetError> {
    let len = buf.try_get_u64()? as usize;
    if buf.len() < len {
        return Err(TryGetError {
            requested: len,
            available: buf.len(),
        });
    }

    let str = String::from_utf8_lossy(&buf[..len]).to_string();
    buf.advance(len);
    Ok(str)
}