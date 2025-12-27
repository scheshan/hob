use std::cmp::max;
use crate::Result;
use crate::arrow::{ArrowRecordBatch, ArrowRecordBatchStream, ArrowSchema};
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
                log::info!("Start to flush mem_table");
                let now = Instant::now();
                match server.flush_mem_table().await {
                    Ok(_) => {
                        log::info!("Flush mem_table completed, cost: {} ms", now.elapsed().as_millis());
                    },
                    Err(e) => log::error!("Flush mem_table failed: {}", e),
                }
            }
        }
    }
}