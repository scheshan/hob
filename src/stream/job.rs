use std::time::Duration;
use tokio::time::{interval_at, Instant};
use tokio_util::sync::CancellationToken;
use crate::server::Server;

pub async fn flush_mem_table_job(server: Server, ct: CancellationToken) {
    let period = Duration::from_secs(30);
    let mut interval = interval_at(Instant::now() + period, period);

    loop {
        tokio::select! {
            _ = ct.cancelled() => {
                log::info!("Exit flush_mem_table_job");
                break;
            }
            _ = interval.tick() => {
                log::info!("Start flushing mem table");
            }
        }
    }
}