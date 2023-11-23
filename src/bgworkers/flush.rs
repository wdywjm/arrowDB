use std::path::PathBuf;


use crate::{index::Record, fileio::FileManager, enums, errors::DbError};
use super::bgworker::BgWorker;

pub struct FlushWorker {
    bg_worker: BgWorker<Record>,
    flush_worker_idx: usize,
}

impl FlushWorker {
    pub fn new(flush_worker_idx: usize, flush_mode: enums::RWMode, dir: PathBuf, file_size_mb: u64) -> Result<Self, DbError> {
        let data_file = dir.join(format!("{}.dat", flush_worker_idx));
        let bg_worker = BgWorker::new(format!("flush-worker-{}", flush_worker_idx).as_str(), move|record: Record| {
            let mut file_manager = FileManager::new(flush_mode.clone());
            let data_file_manager = file_manager.get_fileio_manager(data_file.to_str().unwrap(), file_size_mb)?;
            let mut file = data_file_manager.write();
            file.write(&record.encode(), record.hint.offset)?;
            let record_key = record.hint.key.to_owned();
            Ok(record_key)
        });
        Ok(FlushWorker { bg_worker, flush_worker_idx })
    }
    
    pub fn send(&self, record: Record) {
        self.bg_worker.send(record)
    }

    pub fn stop(&self) {
        self.bg_worker.stop()
    }

}

