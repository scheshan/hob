use crate::Result;
use crate::arg::Args;
use crate::entry::{Entry, EntryBatch};
use crate::id::IdGenerator;
use crate::schema::{SchemaStore, refresh_schema_job};
use crate::stream::{Stream, flush_mem_table_job};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::signal::ctrl_c;
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

#[derive(Clone)]
pub struct Server {
    args: Args,
    id_generator: IdGenerator,
    schema_store: SchemaStore,
    streams: Arc<RwLock<HashMap<String, Stream>>>,
}

impl Server {
    pub fn new(id_generator: IdGenerator, schema_store: SchemaStore, args: Args) -> Self {
        Self {
            id_generator,
            schema_store,
            streams: Arc::new(RwLock::new(HashMap::new())),
            args,
        }
    }

    pub fn get_stream(&self, name: String) -> Stream {
        let guard = self.streams.read().unwrap();
        let stream = guard.get(&name).map(|s| s.clone());
        drop(guard);
        if stream.is_some() {
            return stream.unwrap();
        }

        let mut guard = self.streams.write().unwrap();
        let stream = guard.entry(name.clone()).or_insert_with(|| {
            Stream::new(
                name,
                self.args.clone(),
                self.schema_store.clone(),
                self.id_generator.clone(),
            )
        });
        stream.clone()
    }

    pub fn all_streams(&self) -> Vec<Stream> {
        let guard = self.streams.read().unwrap();
        guard.iter().map(|e| e.1.clone()).collect()
    }

    pub fn args_ref(&self) -> &Args {
        &self.args
    }
}

pub async fn run() {
    let args = Args::default();
    args.init_logger();
    args.init_directories();

    let ct = CancellationToken::new();
    let main_tracker = TaskTracker::new();

    match run_0(&main_tracker, ct, args).await {
        Ok(_) => {}
        Err(e) => log::error!("start server failed: {}", e),
    }

    main_tracker.close();

    main_tracker.wait().await;
}

async fn run_0(main_tracker: &TaskTracker, ct: CancellationToken, args: Args) -> Result<()> {
    let id_generator = IdGenerator::new();

    let schema_store = init_schema_store(&main_tracker, ct.clone())?;

    let server = Server::new(id_generator, schema_store, args);

    init_flush_mem_table_job(main_tracker, server.clone(), ct.clone());

    add_test_data(main_tracker, server.clone(), ct.clone());

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

fn add_test_data(tracker: &TaskTracker, server: Server, ct: CancellationToken) {
    tracker.spawn(async move {
        let mut interval = interval(Duration::from_secs(1));

        loop {
            tokio::select! {
                _ = ct.cancelled() => {
                    log::info!("Exit add testing data");
                    break;
                }
                _ = interval.tick() =>{
                    log::info!("Add testing data");
                    let stream = server.get_stream("test1".to_string());
                    let batch = generate_test_data();
                    stream.add(batch).unwrap();
                }
            }
        }
    });
}

fn generate_test_data() -> EntryBatch {
    use serde_json::Value;

    let json_str = "{\
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
    let json = serde_json::from_str::<Value>(json_str).unwrap();

    let mut entries = Vec::new();
    for i in 0..100 {
        let entry = Entry::try_from(json.clone()).unwrap();
        entries.push(entry);
    }

    EntryBatch { entries }
}
