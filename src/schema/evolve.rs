use arrow_schema::{FieldRef, Schema, SchemaRef};
use std::collections::HashMap;
use std::sync::Arc;

///Determine whether the schema need to evolve. This method return the evolved result
///only if new schema contain more fields than the old,
pub fn need_evolve_schema(old: SchemaRef, new: SchemaRef) -> Option<SchemaRef> {
    let mut l = 0;
    let mut r = 0;

    while l < old.fields.len() && r < new.fields.len() {
        let left = &old.fields[l];
        let right = &new.fields[r];

        if left.name() == right.name() {
            l += 1;
            r += 1;
        } else if left.name() < right.name() {
            l += 1;
        } else {
            //the field does not exist in old schema, break here and go to combine schema
            break;
        }
    }

    if r == new.fields.len() {
        return None;
    }

    Some(combine_schema(old, new))
}

//combine the two schema, keep all old's fields, add new's fields that don't exist in old.
fn combine_schema(old: SchemaRef, new: SchemaRef) -> SchemaRef {
    let mut m = HashMap::new();
    for field in &old.fields {
        m.insert(field.name(), field.clone());
    }

    for field in &new.fields {
        if !m.contains_key(field.name()) {
            m.insert(field.name(), field.clone());
        }
    }

    let mut fields: Vec<FieldRef> = m.iter().map(|e| e.1.clone()).collect();
    fields.sort_by(|l, r| l.name().cmp(r.name()));

    Arc::new(Schema::new(fields))
}

#[cfg(test)]
mod tests{
    use std::sync::Arc;
    use arrow_schema::{DataType, Field, Schema, SchemaRef};

    fn check_combined_schema(s: SchemaRef) {
        assert!(s.field_with_name("id").is_ok());
        assert!(s.field_with_name("name").is_ok());
        assert!(s.field_with_name("name2").is_ok());
    }

    #[test]
    fn need_evolve() {
        let old = Arc::new(
            Schema::new(
                vec![
                    Field::new("id", DataType::UInt64, false),
                    Field::new("name", DataType::UInt64, false)
                ]
            )
        );

        let new1 = Arc::new(
            Schema::new(
                vec![
                    Field::new("name2", DataType::UInt64, false)
                ]
            )
        );
        let opt = super::need_evolve_schema(old.clone(), new1);
        assert!(opt.is_some());
        check_combined_schema(opt.unwrap());

        let new2 = Arc::new(
            Schema::new(
                vec![
                    Field::new("name2", DataType::UInt64, false),
                    Field::new("id", DataType::UInt64, false),
                    Field::new("name", DataType::UInt64, false),
                ]
            )
        );
        let opt = super::need_evolve_schema(old.clone(), new2);
        assert!(opt.is_some());
        check_combined_schema(opt.unwrap());

        let new2 = Arc::new(
            Schema::new(
                vec![
                    Field::new("id", DataType::UInt64, false),
                    Field::new("name2", DataType::UInt64, false),
                    Field::new("name", DataType::UInt64, false),
                ]
            )
        );
        let opt = super::need_evolve_schema(old.clone(), new2);
        assert!(opt.is_some());
        check_combined_schema(opt.unwrap());

        let new3 = Arc::new(
            Schema::new(
                vec![
                    Field::new("id", DataType::UInt64, false),
                    Field::new("name", DataType::UInt64, false),
                    Field::new("name2", DataType::UInt64, false),
                ]
            )
        );
        let opt = super::need_evolve_schema(old.clone(), new3);
        assert!(opt.is_some());
        check_combined_schema(opt.unwrap());
    }
}