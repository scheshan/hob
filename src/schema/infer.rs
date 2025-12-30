use crate::entry::{Entry, FieldData};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use std::sync::Arc;

pub fn infer_schema(entry: &Entry) -> SchemaRef {
    let mut fields = Vec::new();
    //add reserved fields
    fields.push(Field::new("__id__", DataType::UInt64, false));
    fields.push(Field::new("__time__", DataType::UInt64, false));

    for (key, value) in &entry.fields {
        let typ = match value {
            FieldData::String(_) => DataType::Binary,
            FieldData::Bool(_) => DataType::Boolean,
            FieldData::I64(_) => DataType::Int64,
            FieldData::U64(_) => DataType::UInt64,
            FieldData::F64(_) => DataType::Float64
        };
        fields.push(Field::new(key, typ, true));
    }

    fields.sort_by(|l, r| l.name().partial_cmp(r.name()).unwrap());

    Arc::new(Schema::new(fields))
}

#[cfg(test)]
mod tests {
    use crate::entry::Entry;
    use crate::schema::infer_schema;
    use arrow_schema::DataType;
    use serde_json::Value;

    #[test]
    fn test_infer_schema() -> crate::Result<()> {
        let json = "{\
  \"string\": \"这是一个字符串\",\
  \"u64\": 18446744073709551615,\
  \"i64\": -123,\
  \"f64\": -1.23,\
  \"boolean\": true,\
  \"null\": null,\
  \"array\": [1, 2, 3],\
  \"object\": {\
    \"key1\": \"value1\"\
  }\
}";

        let value = serde_json::from_str::<Value>(json)?;
        let entry = Entry::try_from(&value)?;
        let schema = infer_schema(&entry);

        assert_eq!(*schema.field_with_name("string")?.data_type(), DataType::Binary);
        assert_eq!(*schema.field_with_name("u64")?.data_type(), DataType::UInt64);
        assert_eq!(*schema.field_with_name("i64")?.data_type(), DataType::Int64);
        assert_eq!(*schema.field_with_name("f64")?.data_type(), DataType::Float64);
        assert_eq!(*schema.field_with_name("boolean")?.data_type(), DataType::Boolean);
        assert!(schema.field_with_name("null").is_err());
        assert!(schema.field_with_name("array").is_err());
        assert_eq!(*schema.field_with_name("object.key1")?.data_type(), DataType::Binary);

        Ok(())
    }
}