mod store;
mod infer;
mod evolve;

pub use store::SchemaStore;
pub use infer::infer_schema;
pub use evolve::need_evolve_schema;

pub const SCHEMA_VERSION_KEY: &str = "__SCHEMA_VERSION__";