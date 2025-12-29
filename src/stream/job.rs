use crate::server::Server;
use std::time::Duration;
use tokio::time::{Instant, interval_at};
use tokio_util::sync::CancellationToken;

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