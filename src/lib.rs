mod id;
mod entry;
mod schema;
mod arrow;
mod stream;
mod server;
mod arg;

pub type Result<T> = anyhow::Result<T>;
pub use server::run;