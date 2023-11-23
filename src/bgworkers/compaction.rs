
use crate::bgworkers::bgworker::BgWorker;
use crate::index::Record;

pub struct CompactionWorker {
    bg_worker: BgWorker<Record>,
    compaction_worker_idx: usize,
}

impl CompactionWorker {
    
    pub fn send(&self, record: Record) {
        self.bg_worker.send(record)
    }

    pub fn stop(&self) {
        self.bg_worker.stop()
    }

}
