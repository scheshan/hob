use datafusion::prelude::*;
use crate::server::Server;
use crate::Result;

//todo: testing for just now, complete the logic later!!!
pub async fn search(server: Server, stream_name: &str, sql: &str) -> Result<()> {
    let ctx = SessionContext::new();
    let mut df = ctx.sql(sql).await?;
    df = df.sort(vec![
        col("__time__").sort(false, true),
        col("__id__").sort(false, true)
    ])?;
    df = df.limit(0, Some(50))?;

    df.show().await?;

    Ok(())
}