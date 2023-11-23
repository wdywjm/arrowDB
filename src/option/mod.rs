use crate::enums;
use derivative::Derivative;

#[derive(Debug, Clone, Default, Derivative)]
pub struct Option {
    file_option: FileOption,
    index_mode: enums::IndexMode,

    #[derivative(Default(value = "5"))]
    max_memtable_nums: usize,
    #[derivative(Default(value = "1024"))]
    memtable_size_mb: usize,
    compaction: CompactionOption,
}

impl Option {
    pub fn with_dir(&mut self, dir: &str) -> Self {
        self.file_option.dir = dir.to_owned();
        self.to_owned()
    }

    pub fn with_dat_file_size(&mut self, size_mb: usize) -> Self {
        self.file_option.dat_file_size_mb = size_mb;
        self.to_owned()
    }

    pub fn with_rw_mode(&mut self, rw_mode: enums::RWMode) -> Self {
        self.file_option.rw_mode = rw_mode;
        self.to_owned()
    }

    pub fn with_write_sync_immediately(&mut self, write_sync_immediately: bool) -> Self {
        self.file_option.write_sync_immediately = write_sync_immediately;
        self.to_owned()
    }

    pub fn with_fd_cache_size(&mut self, fd_cache_size: usize) -> Self {
        self.file_option.fd_cache_size = fd_cache_size;
        self.to_owned()
    }

    pub fn whth_index_mode(&mut self, index_mode: enums::IndexMode) -> Self {
        self.index_mode = index_mode;
        self.to_owned()
    }

    pub fn with_max_memtable_nums(&mut self, nums: usize) -> Self {
        self.max_memtable_nums = nums;
        self.to_owned()
    }

    pub fn with_memtable_size_mb(&mut self, size_mb: usize) -> Self {
        self.memtable_size_mb = size_mb;
        self.to_owned()
    }

    pub fn with_candidate_live_key_ratio(&mut self, candidate_live_key_ratio: f32) -> Self {
        self.compaction.candidate_live_key_ratio = candidate_live_key_ratio;
        self.to_owned()
    }

    pub fn with_merge_overlapping_ratio(&mut self, merge_overlapping_ratio: f32) -> Self {
        self.compaction.merge_overlapping_ratio = merge_overlapping_ratio;
        self.to_owned()
    }

    pub fn with_candidate_ratio_everytime(&mut self, candidate_ratio_everytime: f32) -> Self {
        self.compaction.candidate_ratio_everytime = candidate_ratio_everytime;
        self.to_owned()
    }
}

#[derive(Debug, Clone, Default, Derivative)]
pub struct FileOption {
    dir: String,
    #[derivative(Default(value = "256"))]
    dat_file_size_mb: usize,
    rw_mode: enums::RWMode,
    write_sync_immediately: bool,
    fd_cache_size: usize,
}

#[derive(Debug, Clone, Default, Derivative)]
pub struct CompactionOption {
    #[derivative(Default(value = "0.1"))]
    candidate_live_key_ratio: f32,
    #[derivative(Default(value = "0.1"))]
    merge_overlapping_ratio: f32,
    #[derivative(Default(value = "0.5"))]
    candidate_ratio_everytime: f32,
}
