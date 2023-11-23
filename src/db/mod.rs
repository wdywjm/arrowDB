use std::{collections::HashMap, sync::Arc};

use parking_lot::RwLock;

use crate::{index::Index, memtable, option, bgworkers::{flush::FlushWorker, index::IndexWorker, compaction::CompactionWorker}};

pub struct DB {
    opt: option::Option,
    index: HashMap<String, Arc<Index>>,
    mem_tables: Vec<memtable::Memtable>,
    background_workers: (FlushWorker, IndexWorker, CompactionWorker)
}
