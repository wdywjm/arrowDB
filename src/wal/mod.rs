use std::sync::Arc;

use crate::{
    enums,
    errors::DbError,
    fileio::{self, FileIOManagerObject},
};

#[derive(Clone)]
pub struct Wal {
    pub file_id: u64,
    pub write_at: u64,
    pub file_io: FileIOManagerObject,
}

impl Wal {
    pub fn new(
        file_id: u64,
        path: &str,
        file_size_mb: u64,
        rw_mode: enums::RWMode,
    ) -> Result<Wal, DbError> {
        match fileio::FileManager::new(rw_mode).get_fileio_manager(path, file_size_mb) {
            Ok(file_manager) => Ok(Wal {
                file_id,
                write_at: 0,
                file_io: Arc::clone(&file_manager),
            }),
            Err(err) => Err(err),
        }
    }

    pub fn write(&mut self, b: &[u8]) -> Result<usize, DbError> {
        let mut wal = self.file_io.write();
        let len = wal.write(b, self.write_at)?;
        Ok(len)
    }
}
