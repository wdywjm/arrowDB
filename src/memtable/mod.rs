use std::collections::{BTreeMap, HashMap};

use crate::{
    consts::ZESTKEYVALSPLITCHAR,
    datatypes::{
        list::List,
        set::Set,
        sortedset::{ArcNode, SortedSet},
    },
    errors::DbError, index::Record,
};
use bytes::Bytes;
use num_enum::TryFromPrimitive;

use crate::{
    data::entry::Entry,
    enums::{self, DataTypes, EntryOperate},
    errors,
    wal::Wal,
};
use std::ops::Bound::Included;

pub struct Memtable {
    active: bool,
    kvs: HashMap<String, BTreeMap<String, Bytes>>,
    list: HashMap<String, List>,
    set: HashMap<String, Set>,
    sorted_set: HashMap<String, SortedSet>,
    wal: Wal,
    live_key_ratio: f64,
}

impl Memtable {
    pub fn new(
        file_id: u64,
        wal_path: &str,
        file_size_mb: u64,
        rw_mode: enums::RWMode,
    ) -> Result<Self, errors::DbError> {
        Ok(Self {
            active: false,
            kvs: HashMap::new(),
            list: HashMap::new(),
            set: HashMap::new(),
            sorted_set: HashMap::new(),
            wal: Wal::new(file_id, wal_path, file_size_mb, rw_mode)?,
            live_key_ratio: 1.0,
        })
    }

    pub fn active(&self) -> bool {
        self.active
    }

    pub fn set_active(&mut self, active: bool) {
        self.active = active
    }

    pub fn get(&self, bucket: &str, key: &str) -> Result<Option<Entry>, DbError> {
        if let Some(bucket) = self.kvs.get(bucket) {
            if let Some(entry_bytes) = bucket.get(key) {
                let entry = Entry::decode(entry_bytes)?;
                if entry.is_expired() {
                    return Ok(None);
                }
                return Ok(Some(entry));
            }
        }
        Ok(None)
    }

    pub fn range_scan(&self, bucket: &str, start: &str, end: &str) -> Result<Vec<Entry>, DbError> {
        let mut res = vec![];
        if let Some(bucket) = self.kvs.get(bucket) {
            for (_, value) in bucket.range((Included(start.to_owned()), Included(end.to_owned()))) {
                let entry = Entry::decode(value)?;
                res.push(entry);
            }
        }
        Ok(res)
    }

    pub fn put(&mut self, entry: Entry) -> Result<&str, DbError> {
        let entry_bytes = entry.encode();
        self.wal.write(entry_bytes.as_ref())?;

        let bucket_name = String::from_utf8(entry.meta.bucket.to_vec()).unwrap_or("".to_owned());

        match TryFromPrimitive::try_from_primitive(entry.meta.data_type as usize)
            .map_or(enums::DataTypes::String, |data_type| data_type)
        {
            DataTypes::String => {
                let bucket = self
                    .kvs
                    .entry(bucket_name.clone())
                    .or_insert(BTreeMap::new());
                if entry.meta.operate == EntryOperate::Del as u16 {
                    bucket.remove(&bucket_name);
                    return Ok("ok");
                }
                bucket.insert(
                    String::from_utf8(entry.key.to_vec()).unwrap_or("".to_owned()),
                    Bytes::from(entry_bytes),
                );
            }
            _ => {
                return Err(DbError::EntryDataTypeOpInvalid {
                    bucket: bucket_name,
                    key: String::from_utf8(entry.meta.bucket.to_vec()).unwrap_or("".to_owned()),
                    op: entry.meta.operate,
                    data_type: entry.meta.data_type,
                })
            }
        }

        Ok("ok")
    }

    pub fn lpush(&mut self, entry: Entry) -> Result<usize, DbError> {
        let entry_bytes = entry.encode();
        self.wal.write(entry_bytes.as_ref())?;

        let bucket_name = String::from_utf8(entry.meta.bucket.to_vec()).unwrap_or("".to_owned());
        let entry_key_name = String::from_utf8(entry.key.to_vec()).unwrap_or("".to_owned());
        let bucket = self.list.entry(bucket_name).or_insert(List::new());
        bucket.lpush(&entry_key_name, vec![entry_bytes.into()]);
        Ok(1)
    }

    pub fn lpushx(&mut self, entry: Entry) -> Result<usize, DbError> {
        let entry_bytes = entry.encode();
        self.wal.write(entry_bytes.as_ref())?;

        let bucket_name = String::from_utf8(entry.meta.bucket.to_vec()).unwrap_or("".to_owned());
        let entry_key_name = String::from_utf8(entry.key.to_vec()).unwrap_or("".to_owned());
        let bucket = self.list.entry(bucket_name).or_insert(List::new());
        Ok(bucket
            .rpushx(&entry_key_name, vec![entry_bytes.into()])
            .unwrap_or(0))
    }

    pub fn rpush(&mut self, entry: Entry) -> Result<usize, DbError> {
        let entry_bytes = entry.encode();
        self.wal.write(entry_bytes.as_ref())?;

        let bucket_name = String::from_utf8(entry.meta.bucket.to_vec()).unwrap_or("".to_owned());
        let entry_key_name = String::from_utf8(entry.key.to_vec()).unwrap_or("".to_owned());
        let bucket = self.list.entry(bucket_name).or_insert(List::new());
        bucket.rpush(&entry_key_name, vec![entry_bytes.into()]);

        Ok(1)
    }

    pub fn rpushx(&mut self, entry: Entry) -> Result<usize, DbError> {
        let entry_bytes = entry.encode();
        self.wal.write(entry_bytes.as_ref())?;

        let bucket_name = String::from_utf8(entry.meta.bucket.to_vec()).unwrap_or("".to_owned());
        let entry_key_name = String::from_utf8(entry.key.to_vec()).unwrap_or("".to_owned());
        let bucket = self.list.entry(bucket_name).or_insert(List::new());
        Ok(bucket
            .rpushx(&entry_key_name, vec![entry_bytes.into()])
            .unwrap_or(0))
    }

    pub fn lpop(&mut self, entry: Entry) -> Result<Option<Entry>, DbError> {
        let entry_bytes = entry.encode();
        self.wal.write(entry_bytes.as_ref())?;

        let bucket_name = String::from_utf8(entry.meta.bucket.to_vec()).unwrap_or("".to_owned());
        let entry_key_name = String::from_utf8(entry.key.to_vec()).unwrap_or("".to_owned());
        let bucket = self.list.entry(bucket_name).or_insert(List::new());

        match bucket.lpop(&entry_key_name) {
            Some(entry_bytes) => Ok(Some(Entry::decode(entry_bytes.as_ref())?)),
            None => Ok(None),
        }
    }

    pub fn rpop(&mut self, entry: Entry) -> Result<Option<Entry>, DbError> {
        let entry_bytes = entry.encode();
        self.wal.write(entry_bytes.as_ref())?;

        let bucket_name = String::from_utf8(entry.meta.bucket.to_vec()).unwrap_or("".to_owned());
        let entry_key_name = String::from_utf8(entry.key.to_vec()).unwrap_or("".to_owned());
        let bucket = self.list.entry(bucket_name).or_insert(List::new());

        match bucket.rpop(&entry_key_name) {
            Some(entry_bytes) => Ok(Some(Entry::decode(entry_bytes.as_ref())?)),
            None => Ok(None),
        }
    }

    pub fn lset(&mut self, index: usize, entry: Entry) -> Result<usize, DbError> {
        let entry_bytes = entry.encode();
        self.wal.write(entry_bytes.as_ref())?;

        let bucket_name = String::from_utf8(entry.meta.bucket.to_vec()).unwrap_or("".to_owned());
        let entry_key_name = String::from_utf8(entry.key.to_vec()).unwrap_or("".to_owned());
        let bucket = self.list.entry(bucket_name).or_insert(List::new());
        bucket.lset(&entry_key_name, index, Bytes::from(entry_bytes));

        Ok(1)
    }

    pub fn llen(&self, bucket: &str, key: &str) -> Result<usize, DbError> {
        if let Some(bucket) = self.list.get(bucket) {
            return Ok(bucket.llen(key).unwrap_or(0));
        }

        Ok(0)
    }

    pub fn lindex(&self, bucket: &str, key: &str, index: usize) -> Result<Option<Entry>, DbError> {
        if let Some(bucket) = self.list.get(bucket) {
            return match bucket.lindex(key, index) {
                Some(entry_bytes) => Ok(Some(Entry::decode(entry_bytes.as_ref())?)),
                None => Ok(None),
            };
        }

        Ok(None)
    }

    pub fn lrange(
        &self,
        bucket: &str,
        key: &str,
        start: usize,
        end: usize,
    ) -> Result<Vec<Bytes>, DbError> {
        if let Some(bucket) = self.list.get(bucket) {
            return Ok(bucket.lrange(key, start, end).unwrap_or(vec![]));
        }
        Ok(vec![])
    }

    pub fn sadd(&mut self, entry: Entry) -> Result<usize, DbError> {
        let entry_bytes = entry.encode();
        self.wal.write(entry_bytes.as_ref())?;

        let bucket_name = String::from_utf8(entry.meta.bucket.to_vec()).unwrap_or("".to_owned());
        let entry_key_name = String::from_utf8(entry.key.to_vec()).unwrap_or("".to_owned());
        let bucket = self.set.entry(bucket_name).or_insert(Set::new());
        bucket.sadd(&entry_key_name, vec![Bytes::from(entry_bytes)]);

        Ok(1)
    }

    pub fn srem(&mut self, entry: Entry) -> Result<usize, DbError> {
        let entry_bytes = entry.encode();
        self.wal.write(entry_bytes.as_ref())?;

        let bucket_name = String::from_utf8(entry.meta.bucket.to_vec()).unwrap_or("".to_owned());
        let entry_key_name = String::from_utf8(entry.key.to_vec()).unwrap_or("".to_owned());
        let bucket = self.set.entry(bucket_name).or_insert(Set::new());
        bucket.srem(&entry_key_name, vec![Bytes::from(entry_bytes)]);

        Ok(1)
    }

    pub fn suion(&self, bucket: &str, key: &str, keys: Vec<&str>) -> Result<Vec<Bytes>, DbError> {
        if let Some(bucket) = self.set.get(bucket) {
            return Ok(bucket.suion(key, keys).unwrap_or(vec![]));
        }

        Ok(vec![])
    }

    pub fn sdiff(&self, bucket: &str, key: &str, keys: Vec<&str>) -> Result<Vec<Bytes>, DbError> {
        if let Some(bucket) = self.set.get(bucket) {
            return Ok(bucket.sdiff(key, keys).unwrap_or(vec![]));
        }
        Ok(vec![])
    }

    pub fn sinter(&self, bucket: &str, key: &str, keys: Vec<&str>) -> Result<Vec<Bytes>, DbError> {
        if let Some(bucket) = self.set.get(bucket) {
            return Ok(bucket.sinter(key, keys).unwrap_or(vec![]));
        }
        Ok(vec![])
    }

    pub fn sismember(&self, entry: Entry) -> Result<bool, DbError> {
        let entry_bytes = Bytes::from(entry.encode());
        let bucket_name = std::str::from_utf8(entry.meta.bucket.as_ref()).unwrap_or("");
        let entry_key_name = std::str::from_utf8(entry.key.as_ref()).unwrap_or("");
        if let Some(bucket) = self.set.get(bucket_name) {
            return Ok(bucket
                .sismember(entry_key_name, entry_bytes)
                .unwrap_or(false));
        }
        Ok(false)
    }

    pub fn smembers(&self, bucket: &str, key: &str) -> Result<Vec<Bytes>, DbError> {
        if let Some(bucket) = self.set.get(bucket) {
            return Ok(bucket.smembers(key).unwrap_or(vec![]));
        }
        Ok(vec![])
    }

    pub fn scard(&self, bucket: &str, key: &str) -> Result<usize, DbError> {
        if let Some(bucket) = self.set.get(bucket) {
            return Ok(bucket.scard(key).unwrap_or(0));
        }

        Ok(0)
    }

    pub fn zadd(&mut self, entry: Entry) -> Result<usize, DbError> {
        let entry_key_name = String::from_utf8(entry.key.to_vec()).unwrap_or("".to_owned());
        let key_score: Vec<&str> = entry_key_name.split(ZESTKEYVALSPLITCHAR).collect();
        let key = key_score[0];
        let score = key_score[1].parse::<f64>().unwrap_or(0.0);
        let entry_bytes = entry.encode();
        self.wal.write(entry_bytes.as_ref())?;

        let bucket_name = String::from_utf8(entry.meta.bucket.to_vec()).unwrap_or("".to_owned());
        let bucket = self
            .sorted_set
            .entry(bucket_name)
            .or_insert(SortedSet::new());
        Ok(bucket.put(key, Bytes::from(entry_bytes), score))
    }

    pub fn zrem(&mut self, entry: Entry) -> Result<Option<ArcNode>, DbError> {
        let entry_key_name = String::from_utf8(entry.key.to_vec()).unwrap_or("".to_owned());
        let key_score: Vec<&str> = entry_key_name.split(ZESTKEYVALSPLITCHAR).collect();
        let key = key_score[0];
        let entry_bytes = entry.encode();
        self.wal.write(entry_bytes.as_ref())?;

        let bucket_name = std::str::from_utf8(entry.meta.bucket.as_ref()).unwrap_or("");
        if let Some(bucket) = self.sorted_set.get_mut(bucket_name) {
            return Ok(bucket.remove(key));
        }

        Ok(None)
    }

    pub fn get_by_rank_range(
        &mut self,
        bucket: &str,
        start: usize,
        end: usize,
        remove: bool,
    ) -> Result<Vec<ArcNode>, DbError> {
        if let Some(bucket) = self.sorted_set.get_mut(bucket) {
            let rank_items = bucket.get_by_rank_range(start, end, remove);
            for node in &rank_items {
                if remove {
                    self.wal.write(node.borrow().value.as_ref())?;
                }
            }
            return Ok(rank_items);
        }
        Ok(vec![])
    }

    pub fn get_by_rank(
        &mut self,
        bucket: &str,
        rank: usize,
        remove: bool,
    ) -> Result<Option<ArcNode>, DbError> {
        if let Some(bucket) = self.sorted_set.get_mut(bucket) {
            if let Some(node) = bucket.get_by_rank(rank, remove) {
                if remove {
                    self.wal.write(node.borrow().value.as_ref())?;
                }
                return Ok(Some(node));
            }
        }

        Ok(None)
    }

    pub fn get_by_key(&self, bucket: &str, key: &str) -> Result<Option<ArcNode>, DbError> {
        if let Some(bucket) = self.sorted_set.get(bucket) {
            return Ok(bucket.get_by_key(key));
        }
        Ok(None)
    }

    pub fn get_by_score_range(
        &self,
        bucket: &str,
        start: f64,
        end: f64,
        limit: usize,
        exclude_start: bool,
        exclude_end: bool,
    ) -> Result<Vec<ArcNode>, DbError> {
        if let Some(bucket) = self.sorted_set.get(bucket) {
            return Ok(bucket.get_by_score_range(start, end, limit, exclude_start, exclude_end));
        }
        Ok(vec![])
    }
}

impl Iterator for Memtable {
    type Item = Record;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}
