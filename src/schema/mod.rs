mod store;
mod infer;
mod evolve;
mod job;

pub use store::SchemaStore;
pub use infer::infer_schema;
pub use evolve::need_evolve_schema;
pub use job::refresh_schema_job;

pub const SCHEMA_VERSION_KEY: &str = "__SCHEMA_VERSION__";