use crate::enums;
use crate::errors::DbError;
use crate::fileio::{FDManager, FileIOManager};
use memmap::MmapMut;
use parking_lot::Mutex;
use std::io::Write;
use std::sync::Arc;

pub struct MMapFile {
    pub file_path: String,
    pub file_size_mb: u64,
    pub fd_manager: Arc<Mutex<FDManager>>,
    pub mmap: Option<MmapMut>,
}

impl FileIOManager for MMapFile {
    fn write(&mut self, b: &[u8], offset: u64) -> Result<usize, DbError> {
        if offset >= self.file_size_mb * enums::MB {
            return Err(DbError::OffsetOutOfRange {
                method: "write".to_owned(),
                offset,
            });
        }
        if let Some(mmap) = self.mmap.as_mut() {
            (&mut mmap[offset as usize..]).write_all(b)?;
            return Ok(b.len());
        }
        Err(DbError::IOError(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "mmap file not found",
        )))
    }

    fn read(&self, b: &mut [u8], offset: u64) -> Result<usize, DbError> {
        if offset >= self.file_size_mb * enums::MB {
            return Err(DbError::OffsetOutOfRange {
                method: "read".to_owned(),
                offset,
            });
        }
        if let Some(mmap) = self.mmap.as_ref() {
            let end = if offset as usize + b.len() <= mmap.len() {
                offset as usize + b.len()
            } else {
                mmap.len()
            };

            b.copy_from_slice(&mmap[offset as usize..end]);
            return Ok(b.len());
        }
        Err(DbError::IOError(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "mmap file not found",
        )))
    }

    fn sync(&mut self) -> Result<bool, DbError> {
        if let Some(mmap) = self.mmap.as_mut() {
            mmap.flush()?;
            return Ok(true);
        }
        Ok(false)
    }

    fn release(&mut self) -> bool {
        self.mmap = None;
        self.fd_manager
            .lock()
            .fds_cache
            .pop(&self.file_path)
            .is_some()
    }
}
