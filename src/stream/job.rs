use std::cmp::max;
use crate::Result;
use crate::arrow::{ArrowRecordBatch, ArrowSchema};
use crate::server::Server;
use crate::stream::Stream;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use parquet::arrow::{ArrowWriter, AsyncArrowWriter};
use tokio::fs::OpenOptions;
use tokio::time::{Instant, interval_at};
use tokio_util::sync::CancellationToken;
use crate::stream::ss_table::SSTable;

pub async fn flush_mem_table_job(server: Server, ct: CancellationToken) {
    let period = Duration::from_secs(server.args_ref().mem_table_flush_seconds);
    let mut interval = interval_at(Instant::now() + period, period);

    loop {
        tokio::select! {
            _ = ct.cancelled() => {
                log::info!("Exit flush_mem_table_job");
                break;
            }
            _ = interval.tick() => {
                let streams = server.all_streams();
                for stream in streams {
                    log::info!("Start to flush stream[{}]'s mem_table", stream.name());
                    match flush_stream_mem_table(&stream).await {
                        Ok(_) => {
                            log::info!("Successfully flushed stream[{}]'s mem_table", stream.name())
                        }
                        Err(e) => {
                            log::error!("Flushing stream mem_table failed: {}", e);
                        }
                    }
                }
            }
        }
    }
}

async fn flush_stream_mem_table(stream: &Stream) -> Result<()> {
    //collect all mem_table list
    let mem_table_list = stream.clone_mem_table_list();

    //group all record_batch by schema version
    let mut version_schema_map: HashMap<u64, ArrowSchema> = HashMap::new();
    let mut version_record_map: HashMap<u64, Vec<Arc<ArrowRecordBatch>>> = HashMap::new();
    let mut max_mem_table_id = 0u64;

    for mem_table in mem_table_list {
        for record_batch in mem_table.data() {
            let version = record_batch.schema_version();
            version_schema_map
                .entry(version)
                .or_insert(record_batch.arrow_schema());
            let record_vec = version_record_map.entry(version).or_insert_with(Vec::new);
            record_vec.push(record_batch.clone());
        }
        max_mem_table_id = max(max_mem_table_id, mem_table.id());
    }

    //iterate all versions and flush to disk
    let mut versions: Vec<u64> = version_schema_map.keys().map(|v| *v).collect();
    versions.sort();
    let mut ss_tables = Vec::new();
    for version in versions {
        let ss_table_id = stream.next_ss_table_id();
        let schema = version_schema_map.get(&version).unwrap();
        let records = version_record_map.get(&version).unwrap();
        let ss_table = generate_ss_table(ss_table_id, schema, records).await?;
        ss_tables.push(ss_table);
    }

    stream.flush_completed(ss_tables, max_mem_table_id);

    Ok(())
}

async fn generate_ss_table(id: u64, arrow_schema: &ArrowSchema, records: &Vec<Arc<ArrowRecordBatch>>) -> Result<SSTable> {
    let root_dir : PathBuf = "D:\\data\\hob".into();
    let path = root_dir.join("data").join(format!("{}.bin", id));

    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .open(path).await?;
    let mut arrow_writer = AsyncArrowWriter::try_new(file, arrow_schema.schema(), None)?;
    for record in records {
        arrow_writer.write(record.record_ref()).await?;
    }
    arrow_writer.flush().await?;

    Ok(SSTable::new(id))
}
