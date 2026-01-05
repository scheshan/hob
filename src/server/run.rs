use crate::Result;
use crate::arg::Args;
use crate::schema::{SchemaStore, refresh_schema_job};
use crate::server::id::IdGenerator;
use crate::server::server::{Server, ServerRecoveryState};
use crate::storage::{
    ManifestReader, ManifestRecord, ManifestWriter, SSTableKey, flush_mem_table_job,
};
use serde_json::Value;
use std::cmp::max;
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use crate::server::http;

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

    let server = init_server(id_generator, schema_store, args)?;

    //init flush_mem_table_job
    main_tracker.spawn({
        let server = server.clone();
        let ct = ct.clone();
        async move { flush_mem_table_job(server, ct).await }
    });

    //add test data
    main_tracker.spawn({
        let server = server.clone();
        let ct = ct.clone();
        async move { add_test_data(server, ct).await }
    });

    //run http server
    http::run_http_server(server.clone()).await?;

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

fn init_server(id_generator: IdGenerator, schema_store: SchemaStore, args: Args) -> Result<Server> {
    //Create the writer first, this will help create the manifest file while it doesn't exist
    let manifest_writer = ManifestWriter::new(args.root_dir.clone())?;

    let manifest_reader = ManifestReader::new(args.root_dir.clone())?;
    let manifest_records = manifest_reader.read()?;
    if manifest_records.is_empty() {
        log::info!("No recovery data, create a new server");
        let server = Server::new(id_generator, schema_store, manifest_writer, args, None)?;
        return Ok(server);
    }

    log::info!("Recovering server from manifest: {:?}", manifest_records);
    let mut mem_table_ids: Vec<u64> = Vec::new();
    let mut flush_mem_table_id: Option<u64> = None;
    let mut stream_ss_table_keys: HashMap<String, Vec<SSTableKey>> = HashMap::new();

    for record in manifest_records {
        match record {
            ManifestRecord::NewMemTable(id) => {
                mem_table_ids.push(id);
            }
            ManifestRecord::FlushMemTable(id, ss_table_key_list) => {
                match flush_mem_table_id {
                    None => flush_mem_table_id = Some(id),
                    Some(prev_id) => flush_mem_table_id = Some(max(prev_id, id)),
                }

                for ss_table_key in ss_table_key_list {
                    let list = stream_ss_table_keys
                        .entry(ss_table_key.stream_name().to_string())
                        .or_insert_with(Vec::new);
                    list.push(ss_table_key);
                }
            }
        }
    }

    let recovery_state =
        ServerRecoveryState::new(mem_table_ids, flush_mem_table_id, stream_ss_table_keys);
    Server::new(
        id_generator,
        schema_store,
        manifest_writer,
        args,
        Some(recovery_state),
    )
}

async fn add_test_data(server: Server, ct: CancellationToken) {
    let mut interval = interval(Duration::from_secs(1));

    let mut x = 0;

    loop {
        tokio::select! {
            _ = ct.cancelled() => {
                log::info!("Exit add testing data");
                break;
            }
            _ = interval.tick() =>{
                log::info!("Add testing data");
                let json = generate_test_data();
                server.ingest(&"test1".to_string(), json).unwrap();

                x += 1;
                if x == 20 {
                    log::info!("Add 20 batches of testing data, now exit");
                    break;
                }
            }
        }
    }
}

fn generate_test_data() -> Value {
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
    let mut v = Vec::new();

    for i in 0..100 {
        v.push(json.clone());
    }

    Value::Array(v)
}
