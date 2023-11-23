mod mmap;
mod std_file;

use crate::enums;
use crate::errors::DbError;
use memmap::MmapMut;
use mmap::MMapFile;
use once_cell::sync::OnceCell;
use parking_lot::{Mutex, RwLock};
use std::fs::OpenOptions;
use std::{fs::File, num::NonZeroUsize, sync::Arc};
use std_file::StdFile;

pub trait FileIOManager {
    fn write(&mut self, b: &[u8], offset: u64) -> Result<usize, DbError>;
    fn read(&self, b: &mut [u8], offset: u64) -> Result<usize, DbError>;
    fn sync(&mut self) -> Result<bool, DbError>;
    fn release(&mut self) -> bool;
}

pub type FileIOManagerObject = Arc<RwLock<Box<dyn FileIOManager>>>;

#[derive(Debug)]
pub struct FDManager {
    fds_cache: lru::LruCache<String, File>,
}

static GLOBALFDMANAGER: OnceCell<Arc<Mutex<FDManager>>> = OnceCell::new();

impl FDManager {
    pub fn set_fd_manager(fds_cache_cap: NonZeroUsize) {
        GLOBALFDMANAGER
            .set(Arc::new(Mutex::new(FDManager {
                fds_cache: lru::LruCache::new(fds_cache_cap),
            })))
            .unwrap();
    }

    pub fn get_fd_manager() -> Arc<Mutex<FDManager>> {
        let fd_manager = GLOBALFDMANAGER.get().unwrap();
        Arc::clone(fd_manager)
    }
}

pub struct FileManager {
    rw_mode: enums::RWMode,
    fd_manager: Arc<Mutex<FDManager>>,
}

impl FileManager {
    pub fn new(rw_mode: enums::RWMode) -> Self {
        FileManager {
            rw_mode,
            fd_manager: FDManager::get_fd_manager(),
        }
    }

    pub fn get_fileio_manager(
        &mut self,
        path: &str,
        file_size_mb: u64,
    ) -> Result<FileIOManagerObject, DbError> {
        let mut fd_manager = self.fd_manager.lock();
        match self.rw_mode {
            enums::RWMode::StdIO => {
                if let Some(cache_fd) = fd_manager.fds_cache.get(path) {
                    cache_fd.set_len(file_size_mb * enums::MB)?;
                } else {
                    let file = OpenOptions::new()
                        .read(true)
                        .write(true)
                        .create(true)
                        .open(&path)?;
                    file.set_len(file_size_mb * enums::MB)?;
                    fd_manager.fds_cache.put(path.to_owned(), file);
                }
                Ok(Arc::new(RwLock::new(Box::new(StdFile {
                    file_path: path.to_owned(),
                    file_size_mb,
                    fd_manager: self.fd_manager.clone(),
                }))))
            }
            enums::RWMode::MMap => {
                let mmap: MmapMut;
                if let Some(file) = fd_manager.fds_cache.get(path) {
                    file.set_len(file_size_mb * enums::MB)?;
                    mmap = unsafe { MmapMut::map_mut(file)? };
                } else {
                    let file = OpenOptions::new()
                        .read(true)
                        .write(true)
                        .create(true)
                        .open(&path)?;
                    file.set_len(file_size_mb * enums::MB)?;
                    mmap = unsafe { MmapMut::map_mut(&file)? };
                    fd_manager.fds_cache.push(path.to_owned(), file);
                }
                Ok(Arc::new(RwLock::new(Box::new(MMapFile {
                    file_path: path.to_owned(),
                    file_size_mb,
                    fd_manager: self.fd_manager.clone(),
                    mmap: Some(mmap),
                }))))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_file() {
        FDManager::set_fd_manager(NonZeroUsize::new(10).unwrap());

        let mut std_file_manager = FileManager::new(enums::RWMode::StdIO);

        let mut temp = project_root::get_project_root().unwrap();
        temp = temp.join("tempdata/std");

        let std_file_io_manger_ob = std_file_manager
            .get_fileio_manager(temp.to_str().unwrap(), 10)
            .unwrap();

        let mut file = std_file_io_manger_ob.write();
        let size = file.write(b"std", 0).unwrap();
        file.sync().unwrap();
        assert_eq!(size, 3);

        let mut buf = vec![0u8; size];
        let size = file.read(&mut buf, 0).unwrap();
        assert_eq!(size, 3);
        assert_eq!(buf.len(), 3);

        assert_eq!(file.release(), true);
        assert_eq!(file.release(), false);

        let mut mmap_file_manager = FileManager::new(enums::RWMode::MMap);

        let mut temp = project_root::get_project_root().unwrap();
        temp = temp.join("tempdata/mmap");

        let mmap_file_io_manger_ob = mmap_file_manager
            .get_fileio_manager(temp.to_str().unwrap(), 10)
            .unwrap();

        let mut file = mmap_file_io_manger_ob.write();
        let size = file.write(b"mmap", 0).unwrap();
        file.sync().unwrap();
        assert_eq!(size, 4);

        let mut buf = vec![0u8; size];
        let size = file.read(&mut buf, 0).unwrap();
        assert_eq!(size, 4);
        assert_eq!(buf.len(), 4);

        assert_eq!(file.release(), true);
        assert_eq!(file.release(), false);
    }
}
