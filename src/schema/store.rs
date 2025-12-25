use crate::arrow::ArrowSchema;
use crate::Result;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::time::{interval_at, Instant};
use tokio_util::sync::CancellationToken;

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

    pub async fn run(self, ct: CancellationToken) {
        let period = Duration::from_secs(30);
        let mut interval = interval_at(Instant::now() + period, period);

        loop {
            tokio::select! {
                _ = ct.cancelled() => {
                    log::info!("Exit SchemaAutoLoadJob");
                    break;
                }
                _ = interval.tick() => {
                    log::info!("start to load schema");
                }
            }
        }
    }
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