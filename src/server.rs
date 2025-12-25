use crate::id::IdGenerator;
use crate::schema::{SchemaStore, refresh_schema_job};
use crate::stream::{Stream, flush_mem_table_job};
use crate::{Result, schema};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tokio::signal::ctrl_c;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

#[derive(Clone)]
pub struct Server {
    id_generator: IdGenerator,
    schema_store: SchemaStore,
    streams: Arc<RwLock<HashMap<String, Stream>>>,
}

impl Server {
    pub fn new(id_generator: IdGenerator, schema_store: SchemaStore) -> Self {
        Self {
            id_generator,
            schema_store,
            streams: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn add_stream(&self, name: String, store: Stream) {
        let mut guard = self.streams.write().unwrap();
        guard.insert(name, store);
    }

    pub fn all_streams(&self) -> Vec<Stream> {
        let guard = self.streams.read().unwrap();
        guard.iter().map(|e| e.1.clone()).collect()
    }
}

pub async fn run() {
    env_logger::init();

    let ct = CancellationToken::new();
    let main_tracker = TaskTracker::new();

    match run_0(&main_tracker, ct).await {
        Ok(_) => {}
        Err(e) => log::error!("start server failed: {}", e),
    }

    main_tracker.close();

    main_tracker.wait().await;
}

async fn run_0(main_tracker: &TaskTracker, ct: CancellationToken) -> Result<()> {
    let id_generator = IdGenerator::new();

    let schema_store = init_schema_store(&main_tracker, ct.clone())?;

    let server = Server::new(id_generator, schema_store);

    init_flush_mem_table_job(main_tracker, server.clone(), ct.clone());

    ctrl_c().await?;

    log::info!("Shutting down server");

    ct.cancel();

    drop(server);

    Ok(())
}

fn init_schema_store(tracker: &TaskTracker, ct: CancellationToken) -> Result<SchemaStore> {
    let store = SchemaStore::new()?;

    let store_copy = store.clone();
    tracker.spawn(async move { refresh_schema_job(store_copy, ct).await });

    Ok(store)
}

fn init_flush_mem_table_job(tracker: &TaskTracker, server: Server, ct: CancellationToken) {
    tracker.spawn(async move { flush_mem_table_job(server, ct).await });
}
