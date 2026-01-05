mod entry;
mod schema;
mod arrow;
mod server;
mod arg;
mod storage;
mod search;

pub type Result<T> = anyhow::Result<T>;
pub use server::run;