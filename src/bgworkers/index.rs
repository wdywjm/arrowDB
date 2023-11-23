use std::{path::PathBuf, sync::Arc};

use anyhow::anyhow;
use crossbeam_channel::{Sender, Receiver};
use parking_lot::{RwLock, Mutex};
use super::bgworker::BgWorker;
use crate::{index::{Record, Index}, fileio::FileManager, enums::{self, DataTypes, EntryOperate}, errors::{DbError, self}};

pub struct IndexWorker {
    bg_worker: BgWorker<(EntryOperate, Record, usize)>,
    index_worker_idx: usize,
}

impl IndexWorker {
    pub fn new(index_worker_idx: usize, index: Arc<RwLock<Index>>) -> Result<Self, DbError> {
        let bg_worker = BgWorker::new(format!("index-worker-{}", index_worker_idx).as_str(), move|record: (EntryOperate, Record, usize)| {
            // index.write().get(std::str::from_utf8(&record.1.hint.key).unwrap());
            let record_key = record.1.hint.key.to_owned();
            let record_key_str = String::from_utf8(record.1.hint.key.to_vec()).map_err(|_| errors::DbError::OtherError(anyhow!("key covert to string error")))?;
            match record.0 {
                EntryOperate::Put => index.write().put(record_key_str, record.1)?,
                EntryOperate::Del => index.write().del(&record_key_str)?,
                EntryOperate::Ttl => unimplemented!(),
                EntryOperate::LLpush => index.write().lpush(&record_key_str, record.1)?,
                EntryOperate::LLpop => index.write().lpop(&record_key_str).map(|_| 1)?,
                EntryOperate::LRpush => index.write().rpush(&record_key_str, record.1)?,
                EntryOperate::LRpop => index.write().rpop(&record_key_str).map(|_| 1)?,
                EntryOperate::LLpushx => index.write().lpushx(&record_key_str, record.1)?,
                EntryOperate::LRpushx => index.write().rpushx(&record_key_str, record.1)?,
                EntryOperate::LRem => unimplemented!(),
                EntryOperate::LSet => index.write().lset(&record_key_str, record.2, record.1).unwrap_or(0),
                EntryOperate::SAdd => index.write().sadd(&record_key_str, vec![record.1]).unwrap_or(0),
                EntryOperate::SRem => index.write().srem(&record_key_str, vec![record.1]).unwrap_or(0),
                EntryOperate::ZPut => unimplemented!(),
                EntryOperate::ZRem => unimplemented!(),
                _ => 0,

            };
            Ok(record_key)
        });
        Ok(IndexWorker { bg_worker, index_worker_idx })
    }

    fn send(&self, record:(EntryOperate, Record, usize) ) {
        self.bg_worker.send(record);
    }

    fn stop(&self) {
        self.bg_worker.stop()
    }
}