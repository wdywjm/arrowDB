use crate::enums;
use crate::errors::DbError;
use crate::fileio::{FDManager, FileIOManager};
use parking_lot::Mutex;
use std::fs::OpenOptions;
use std::os::unix::prelude::FileExt;
use std::sync::Arc;

pub struct StdFile {
    pub file_path: String,
    pub file_size_mb: u64,
    pub fd_manager: Arc<Mutex<FDManager>>,
}

impl FileIOManager for StdFile {
    fn write(&mut self, b: &[u8], offset: u64) -> Result<usize, DbError> {
        if offset >= self.file_size_mb * enums::MB {
            return Err(DbError::OffsetOutOfRange {
                method: "write".to_owned(),
                offset,
            });
        }
        let mut fd_manager = self.fd_manager.lock();
        if let Some(fd) = fd_manager.fds_cache.get_mut(&self.file_path) {
            let size = fd.write_at(b, offset)?;
            Ok(size)
        } else {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&self.file_path)?;
            file.set_len(self.file_size_mb * enums::MB)?;
            let size = file.write_at(b, offset)?;
            fd_manager.fds_cache.push(self.file_path.to_owned(), file);
            Ok(size)
        }
    }

    fn read(&self, b: &mut [u8], offset: u64) -> Result<usize, DbError> {
        let mut fd_manager = self.fd_manager.lock();
        if let Some(fd) = fd_manager.fds_cache.get(&self.file_path) {
            let size = fd.read_at(b, offset)?;
            Ok(size)
        } else {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&self.file_path)?;
            file.set_len(self.file_size_mb * enums::MB)?;
            let size = file.read_at(b, offset)?;
            Ok(size)
        }
    }

    fn sync(&mut self) -> Result<bool, DbError> {
        if let Some(fd) = self.fd_manager.lock().fds_cache.get(&self.file_path) {
            fd.sync_all()?;
            return Ok(true);
        }
        return Ok(false);
    }

    fn release(&mut self) -> bool {
        self.fd_manager
            .lock()
            .fds_cache
            .pop(&self.file_path)
            .is_some()
    }
}
