use crate::arrow::ArrowSchema;
use crate::Result;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

#[derive(Clone)]
pub struct SchemaStore {
    lock: Arc<RwLock<HashMap<String, ArrowSchema>>>,
}

impl SchemaStore {
    pub fn new() -> Result<Self> {
        let resolver = Self {
            lock: Arc::new(RwLock::new(HashMap::new())),
        };
        //resolver.load_schema()?;
        Ok(resolver)
    }

    pub fn get(&self, name: &String) -> Option<ArrowSchema> {
        let lock = self.lock.read().unwrap();
        lock.get(name).map(|e| e.clone())
    }

    pub fn set(&self, name: &String, version: u64, schema: ArrowSchema) -> bool {
        let mut lock = self.lock.write().unwrap();

        match lock.get(name) {
            Some(old_schema) => {
                if old_schema.version() != version {
                    return false;
                }
            }
            _ => {}
        }

        lock.insert(name.clone(), schema);
        true
    }

    // pub fn load_schema(&self) -> Result<()> {
    //     let schema_ref = Arc::new(Schema::new(vec![
    //         Field::new("__id__", DataType::UInt64, false),
    //         Field::new("__time__", DataType::UInt64, false),
    //         Field::new("message", DataType::Utf8, true),
    //         Field::new("log_level", DataType::Utf8, true),
    //         Field::new("app_name", DataType::Utf8, true),
    //     ]));
    //     let mut lock = self.lock.write().unwrap();
    //     let mut hm = HashMap::new();
    //     hm.insert("test1".to_string(), schema_ref.clone());
    //     hm.insert("test2".to_string(), schema_ref.clone());
    //     hm.insert("test3".to_string(), schema_ref.clone());
    //     *lock = hm;
    //     Ok(())
    // }
    //
    // pub async fn run(self, ct: CancellationToken) {
    //     let period = Duration::from_secs(30);
    //     let mut interval = interval_at(Instant::now().add(period), period);
    //
    //     loop {
    //         tokio::select! {
    //             _ = ct.cancelled() => {
    //                 log::info!("receive shutdown signal, exit SchemaResolver loop");
    //             }
    //             _ = interval.tick() => {
    //                 log::info!("start to load schema");
    //                 match self.load_schema() {
    //                     Ok(_) => {
    //                         log::info!("successfully load schema");
    //                     },
    //                     Err(e) => {
    //                         log::error!("load schema failed: {}, will retry", e);
    //                     }
    //                 }
    //             }
    //         }
    //     }
    // }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use arrow_schema::{DataType, Field, Schema};
    use crate::arrow::ArrowSchema;
    use crate::schema::{SchemaStore};

    #[test]
    fn test_set() {
        let store = SchemaStore::new().unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt64, false)
        ]));
        let name = "test".to_string();
        assert!(store.set(&name, 0, ArrowSchema::new(schema.clone(), 1)));
        assert!(!store.set(&name, 0, ArrowSchema::new(schema.clone(), 2)));
        assert!(store.set(&name, 1, ArrowSchema::new(schema.clone(), 2)));
    }
}