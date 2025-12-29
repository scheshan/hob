mod entry;
mod schema;
mod arrow;
mod stream;
mod server;
mod arg;
mod storage;

pub type Result<T> = anyhow::Result<T>;
pub use server::run;