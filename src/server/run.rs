use std::path::PathBuf;
use std::time::Duration;
use tokio::signal::ctrl_c;
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use crate::arg::Args;
use crate::entry::{Entry, EntryBatch};
use crate::server::id::IdGenerator;
use crate::Result;
use crate::schema::{refresh_schema_job, SchemaStore};
use crate::server::manifest::Manifest;
use crate::server::server::Server;
use crate::stream::flush_mem_table_job;

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

    let manifest = Manifest::new(PathBuf::from(args.root_dir.as_str()))?;

    let server = Server::new(id_generator, schema_store, manifest, args);

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
                    let batch = generate_test_data();
                    server.ingest(&"test1".to_string(), batch).unwrap();
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