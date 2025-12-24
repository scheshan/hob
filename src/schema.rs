use crate::Result;
use arrow::datatypes::SchemaRef;
use arrow_schema::{DataType, Field, Schema};
use std::collections::HashMap;
use std::ops::Add;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::time::{Instant, interval_at};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct SchemaResolver {
    lock: Arc<RwLock<HashMap<String, SchemaRef>>>,
}

impl SchemaResolver {
    pub fn new() -> Result<Self> {
        let resolver = Self {
            lock: Arc::new(RwLock::new(HashMap::new())),
        };
        resolver.load_schema()?;
        Ok(resolver)
    }

    pub fn resolve(&self, name: &String) -> Option<SchemaRef> {
        let lock = self.lock.read().unwrap();
        lock.get(name).map(|e| e.clone())
    }

    pub fn load_schema(&self) -> Result<()> {
        let schema_ref = Arc::new(Schema::new(vec![
            Field::new("__id__", DataType::UInt64, false),
            Field::new("__time__", DataType::UInt64, false),
            Field::new("message", DataType::Utf8, true),
            Field::new("log_level", DataType::Utf8, true),
            Field::new("app_name", DataType::Utf8, true),
        ]));
        let mut lock = self.lock.write().unwrap();
        let mut hm = HashMap::new();
        hm.insert("test1".to_string(), schema_ref.clone());
        hm.insert("test2".to_string(), schema_ref.clone());
        hm.insert("test3".to_string(), schema_ref.clone());
        *lock = hm;
        Ok(())
    }

    pub async fn run(self, ct: CancellationToken) {
        let period = Duration::from_secs(30);
        let mut interval = interval_at(Instant::now().add(period), period);

        loop {
            tokio::select! {
                _ = ct.cancelled() => {
                    log::info!("receive shutdown signal, exit SchemaResolver loop");
                }
                _ = interval.tick() => {
                    log::info!("start to load schema");
                    match self.load_schema() {
                        Ok(_) => {
                            log::info!("successfully load schema");
                        },
                        Err(e) => {
                            log::error!("load schema failed: {}, will retry", e);
                        }
                    }
                }
            }
        }
    }
}
