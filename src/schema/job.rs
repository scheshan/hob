use std::time::Duration;
use tokio::time::{interval_at, Instant};
use tokio_util::sync::CancellationToken;
use crate::schema::SchemaStore;

pub async fn refresh_schema_job(store: SchemaStore, ct: CancellationToken) {
    let period = Duration::from_secs(30);
    let mut interval = interval_at(Instant::now() + period, period);

    loop {
        tokio::select! {
                _ = ct.cancelled() => {
                    log::info!("Exit refresh_schema_job");
                    break;
                }
                _ = interval.tick() => {
                    log::info!("Start to refresh schema");
                }
            }
    }
}