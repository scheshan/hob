use crate::entry::field::FieldData;
use anyhow::anyhow;
use bytes::{BufMut, BytesMut};
use serde_json::Value;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::time;
use std::time::SystemTime;

pub struct Entry {
    pub(crate) time: u64,
    pub(crate) id: u64,
    pub(crate) fields: HashMap<String, FieldData>,
}

impl TryFrom<Value> for Entry {
    type Error = anyhow::Error;

    fn try_from(value: Value) -> std::result::Result<Self, Self::Error> {
        if !value.is_object() {
            return Err(anyhow!("cannot parse json string, not a valid object."));
        }

        let mut fields: HashMap<String, FieldData> = HashMap::new();

        for (key, value) in value.as_object().unwrap() {
            populate_fields(&mut fields, None, key, value);
        }

        Ok(Self {
            id: 0,
            time: parse_time(&value),
            fields,
        })
    }
}

pub struct EntryBatch {
    pub(crate) entries: Vec<Entry>,
}

impl EntryBatch {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    pub fn from_entries(entries: Vec<Entry>) -> Self {
        Self { entries }
    }

    pub fn add(&mut self, entry: Entry) {
        self.entries.push(entry);
    }

    //Sort EntryBatch by time desc, then by id desc
    pub fn sort(&mut self) {
        if self.entries.is_empty() {
            return;
        }

        self.entries.sort_by(|l, r| {
            return if l.time > r.time {
                Ordering::Less
            } else if l.time < r.time {
                Ordering::Greater
            } else {
                r.id.cmp(&l.id)
            };
        })
    }

    pub fn encode_to_bytes(&self, buf: &mut BytesMut) {
        //8 byte for length
        buf.put_u64(self.entries.len() as u64);

        for entry in &self.entries {
            buf.put_u64(entry.time); //8 byte for time
            buf.put_u64(entry.id); //8 byte for id
            buf.put_u64(entry.fields.len() as u64); //8 byte for fields length

            for (field_name, field_data) in &entry.fields {
                buf.put_u64(field_name.len() as u64); //8 byte for field name's length
                buf.put_slice(field_name.as_bytes());
                match field_data {
                    FieldData::String(str) => {
                        buf.put_u8(1);
                        buf.put_u64(str.len() as u64); //8 byte for field name's length
                        buf.put_slice(str.as_bytes());
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
    }
}

fn parse_time(value: &Value) -> u64 {
    //todo: support various time format
    if let Some(v) = value.get("__time__") {
        if v.is_i64() {
            return v.as_i64().unwrap() as u64;
        } else if v.is_u64() {
            return v.as_u64().unwrap();
        }
    }

    SystemTime::now()
        .duration_since(time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn populate_fields(
    fields: &mut HashMap<String, FieldData>,
    prefix: Option<&str>,
    name: &str,
    value: &Value,
) {
    if value.is_array() || value.is_null() {
        //DO NOT support array or null
        return;
    }
    if name.starts_with("__") && name.ends_with("__") {
        //skip reserved keys, such as __time__ and others
        return;
    }

    let key = match prefix {
        None => name.to_string(),
        Some(p) => format!("{}.{}", p, name).to_string(),
    };

    if value.is_u64() {
        fields.insert(key, FieldData::U64(value.as_u64().unwrap()));
    } else if value.is_i64() {
        fields.insert(key, FieldData::I64(value.as_i64().unwrap()));
    } else if value.is_boolean() {
        fields.insert(key, FieldData::Bool(value.as_bool().unwrap()));
    } else if value.is_f64() {
        fields.insert(key, FieldData::F64(value.as_f64().unwrap()));
    } else if value.is_string() {
        fields.insert(key, FieldData::String(value.as_str().unwrap().to_string()));
    } else if value.is_object() {
        let children = value.as_object().unwrap();
        for (child_key, child_value) in children.iter() {
            populate_fields(fields, Some(key.as_str()), child_key, child_value);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::entry::{Entry, FieldData};
    use serde_json::Value;

    #[test]
    fn test_from_json() {
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
        let value = serde_json::from_str::<Value>(json).unwrap();
        let entry = Entry::try_from(value).unwrap();

        let FieldData::String(s) = &entry.fields["string"] else {
            panic!("parse failed")
        };
        let FieldData::U64(s) = &entry.fields["u64"] else {
            panic!("parse failed")
        };
        let FieldData::I64(s) = &entry.fields["i64"] else {
            panic!("parse failed")
        };
        let FieldData::F64(s) = &entry.fields["f64"] else {
            panic!("parse failed")
        };
        let FieldData::Bool(s) = &entry.fields["boolean"] else {
            panic!("parse failed")
        };
        assert!(!entry.fields.contains_key("null"));
        assert!(!entry.fields.contains_key("array"));
        let FieldData::String(s) = &entry.fields["object.key1"] else {
            panic!("parse failed")
        };
    }
}
