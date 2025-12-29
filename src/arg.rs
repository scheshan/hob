use std::fs::create_dir_all;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use log::LevelFilter;

#[derive(Clone)]
pub struct Args {
    pub root_dir: Arc<PathBuf>,
    pub log_level: Arc<String>,
    pub mem_table_size: usize,
    pub mem_table_flush_seconds: u64,
    pub ss_table_compact_seconds: u64,
}

impl Args {
    pub fn init_logger(&self) {
        let level_filter = LevelFilter::from_str(self.log_level.as_str());
        let filter = level_filter.unwrap();
        env_logger::builder()
            .filter(None, filter)
            .init();
    }

    pub fn init_directories(&self) {
        create_dir_all(self.root_dir.join("data")).unwrap();
        create_dir_all(self.root_dir.join("wal")).unwrap();
    }
}

//todo: parse arguments from command line args
impl Default for Args {
    fn default() -> Self {
        Self {
            root_dir: Arc::new(PathBuf::from("D:\\data\\hob")),
            log_level: Arc::new(String::from("info")),
            mem_table_size: 100 * 1024,
            mem_table_flush_seconds: 30,
            ss_table_compact_seconds: 30,
        }
    }
}
