use std::collections::BTreeMap;

use bytes::{BufMut, Bytes};

use crate::{data::entry::Entry, datatypes::{list::List, set::Set, sortedset::SortedSet}, errors::DbError};
use self::hint::Hint;
use std::ops::Bound::Included;

mod hint;

#[derive(Debug, Clone, Default)]
pub struct Record {
    pub hint: Hint,
    pub entry: Entry,
}

#[derive(Debug, Default)]
pub struct Index {
    kvs: BTreeMap<String, Record>,
    lists: List,
    sets: Set,
    sorted_sets: SortedSet
}

impl Record {
    pub fn encode(&self) -> Vec<u8> {
        let mut hint_b = self.hint.encode();
        let mut entry_b = self.entry.encode();
        hint_b.put_slice(&entry_b);
        let mut res = vec![0; 8];
        let value_start_index = hint_b.len() as u64;
        res[0..8].copy_from_slice(&value_start_index.to_le_bytes());
        res.append(&mut hint_b);
        res.append(&mut entry_b);
        res
    }
}

impl Index {
    pub fn get(&self, key: &str) -> Option<&Record> {
        self.kvs.get(key)
    }
    
    pub fn put(&mut self, key: String, record: Record) -> Result<usize, DbError>{
        // if key.contains(enums::SEPARATOR as char) {
        //     return Err(DbError::ContainSeparatorChar { separator: enums::SEPARATOR as char});
        // }
        let put_num = match self.kvs.insert(key, record) {
            Some(_) => 1,
            None => 0,
        };
        Ok(put_num as usize) 
    }

    pub fn del(&mut self, key: &str) -> Result<usize, DbError> {
        if self.kvs.remove(key).is_none() {
            return Ok(0);
        }
        return Ok(1);
    }

    pub fn range_scan(&self, start: &str, end: &str) -> Option<Vec<&Record>>{
        let range: Vec<&Record> = self.kvs.range((Included(start.to_owned()), Included(end.to_owned()))).map(|(_, v)| v).collect();
        Some(range)
    }

    pub fn lpush(&mut self, key: &str, record: Record) -> Result<usize, DbError>{
        // if key.contains(enums::SEPARATOR as char) {
        //     return Err(DbError::ContainSeparatorChar { separator: enums::SEPARATOR as char});
        // }
        Ok(self.lists.lpush(key, vec![record.encode().into()]).unwrap_or(0))
        
    }

    pub fn lpushx(&mut self, key: &str, record: Record) -> Result<usize, DbError>{
        // if key.contains(enums::SEPARATOR as char) {
        //     return Err(DbError::ContainSeparatorChar { separator: enums::SEPARATOR as char});
        // }
        Ok(self.lists.lpushx(key, vec![record.encode().into()]).unwrap_or(0))
    }

    pub fn rpush(&mut self, key: &str, record: Record) -> Result<usize, DbError>{
        // if key.contains(enums::SEPARATOR as char) {
        //     return Err(DbError::ContainSeparatorChar { separator: enums::SEPARATOR as char});
        // }
        Ok(self.lists.rpush(key, vec![record.encode().into()]).unwrap_or(0))
    }

    pub fn rpushx(&mut self, key: &str, record: Record) -> Result<usize, DbError>{
        // if key.contains(enums::SEPARATOR as char) {
        //     return Err(DbError::ContainSeparatorChar { separator: enums::SEPARATOR as char});
        // }
        Ok(self.lists.rpushx(key, vec![record.encode().into()]).unwrap_or(0))
    }

    pub fn lpop(&mut self, key: &str) -> Result<Option<Record>, DbError>{
        // if key.contains(enums::SEPARATOR as char) {
        //     return Err(DbError::ContainSeparatorChar { separator: enums::SEPARATOR as char});
        // }
        let value = self.lists.lpop(key);
        if value.is_none() {
            return Ok(None);
        }
        let value = value.unwrap();
        let value_start_index = u64::from_le_bytes(value[0..8].try_into().unwrap());
        let mut record = Record { hint: Hint::decode(&value[0..value_start_index as usize])?, entry: Entry::default()};
        record.hint = Hint::decode(&value[0..value_start_index as usize])?;
        if value_start_index < value.len() as u64 {
            record.entry = Entry::decode(&value[value_start_index as usize..])?;
        }
        Ok(Some(record))
    }

    pub fn rpop(&mut self, key: &str) -> Result<Option<Record>, DbError>{
        let value = self.lists.rpop(key);
        if value.is_none() {
            return Ok(None);
        }
        let value = value.unwrap();
        let value_start_index = u64::from_le_bytes(value[0..8].try_into().unwrap());
        let mut record = Record { hint: Hint::decode(&value[0..value_start_index as usize])?, entry: Entry::default()};
        record.hint = Hint::decode(&value[0..value_start_index as usize])?;
        if value_start_index < value.len() as u64 {
            record.entry = Entry::decode(&value[value_start_index as usize..])?;
        }
        Ok(Some(record))
    }

    pub fn lset(&mut self, key: &str, index: usize, record: Record) -> Option<usize>{
        self.lists.lset(key, index, record.encode().into())
    }

    pub fn llen(&self, key: &str) -> Option<usize>{
        self.lists.llen(key)
    }

    pub fn lindex(&self, key: &str, index: usize) -> Result<Option<Record>, DbError>{
        if let Some(b) = self.lists.lindex(key, index) {
            let value_start_index = u64::from_le_bytes(b[0..8].try_into().unwrap());
            let mut record = Record { hint: Hint::decode(&b[0..value_start_index as usize])?, entry: Entry::default()};
            record.hint = Hint::decode(&b[0..value_start_index as usize])?;
            if value_start_index < b.len() as u64 {
                record.entry = Entry::decode(&b[value_start_index as usize..])?;
            }
            return Ok(Some(record))
        }
        Ok(None)
    }

    pub fn lrange(
        &self,
        key: &str,
        start: usize,
        end: usize,
    ) -> Result<Option<Vec<Record>>, DbError>{
        if let Some(bs) = self.lists.lrange(key, start, end) {
            let records: Result<Option<Vec<Record>>, DbError> = bs.iter().map(|value| {
                let value_start_index = u64::from_le_bytes(value[0..8].try_into().unwrap());
                let mut record = Record { hint:  Hint::decode(&value[0..value_start_index as usize])?, entry: Entry::default()};
                record.hint = Hint::decode(&value[0..value_start_index as usize])?;
                if value_start_index < value.len() as u64 {
                    record.entry = Entry::decode(&value[value_start_index as usize..])?;
                }
                Ok(Some(record))
            }).collect();
            return records;
        }
        Ok(None)
    }

    pub fn sadd(&mut self, key: &str, members: Vec<Record>)->Option<usize> {
        let records = members.iter().map(|member| member.encode().into()).collect::<Vec<Bytes>>();
        self.sets.sadd(key, records)
    }

    pub fn srem(&mut self, key: &str, members: Vec<Record>) -> Option<usize>{
        let records = members.iter().map(|member| member.encode().into()).collect::<Vec<Bytes>>();
        self.sets.srem(key, records)
    }

    pub fn suion(&self, key: &str, keys: Vec<&str>) -> Result<Option<Vec<Record>>, DbError>{
        let sets = self.sets.suion(key, keys);
        if sets.is_none() {
            return Ok(None)
        }
        let sets = sets.unwrap();
        let records: Result<Option<Vec<Record>>, DbError> = sets.iter().map(|value| {
            let value_start_index = u64::from_le_bytes(value[0..8].try_into().unwrap());
                let mut record = Record { hint:  Hint::decode(&value[0..value_start_index as usize])?, entry: Entry::default()};
                record.hint = Hint::decode(&value[0..value_start_index as usize])?;
                if value_start_index < value.len() as u64 {
                    record.entry = Entry::decode(&value[value_start_index as usize..])?;
                }
                Ok(Some(record))
        }).collect();
        return records;
    }

    pub fn sdiff(&self, key: &str, keys: Vec<&str>) -> Result<Option<Vec<Record>>, DbError>{
        let sets = self.sets.sdiff(key, keys);
        if sets.is_none() {
            return Ok(None)
        }
        let sets = sets.unwrap();
        let records: Result<Option<Vec<Record>>, DbError> = sets.iter().map(|value| {
            let value_start_index = u64::from_le_bytes(value[0..8].try_into().unwrap());
                let mut record = Record { hint:  Hint::decode(&value[0..value_start_index as usize])?, entry: Entry::default()};
                record.hint = Hint::decode(&value[0..value_start_index as usize])?;
                if value_start_index < value.len() as u64 {
                    record.entry = Entry::decode(&value[value_start_index as usize..])?;
                }
                Ok(Some(record))
        }).collect();
        return records;
    }

    pub fn sinter(&self, key: &str, keys: Vec<&str>) -> Result<Option<Vec<Record>>, DbError>{
        let sets = self.sets.sinter(key, keys);
        if sets.is_none() {
            return Ok(None)
        }
        let sets = sets.unwrap();
        let records: Result<Option<Vec<Record>>, DbError> = sets.iter().map(|value| {
            let value_start_index = u64::from_le_bytes(value[0..8].try_into().unwrap());
                let mut record = Record { hint:  Hint::decode(&value[0..value_start_index as usize])?, entry: Entry::default()};
                record.hint = Hint::decode(&value[0..value_start_index as usize])?;
                if value_start_index < value.len() as u64 {
                    record.entry = Entry::decode(&value[value_start_index as usize..])?;
                }
                Ok(Some(record))
        }).collect();
        return records;
    }

    pub fn sismember(&self, record: &Record) -> Option<bool>{
        let key_str = std::str::from_utf8(&record.hint.key).unwrap();
        let member = Bytes::from(record.encode());
        self.sets.sismember(key_str, member)
    }

    pub fn smembers(&self, key: &str) -> Result<Option<Vec<Record>>, DbError>{
        let members = self.sets.smembers(key);
        if members.is_none() {
            return Ok(None)
        }
        let members = members.unwrap();
        let records: Result<Option<Vec<Record>>, DbError> = members.iter().map(|value| {
            let value_start_index = u64::from_le_bytes(value[0..8].try_into().unwrap());
                let mut record = Record { hint:  Hint::decode(&value[0..value_start_index as usize])?, entry: Entry::default()};
                record.hint = Hint::decode(&value[0..value_start_index as usize])?;
                if value_start_index < value.len() as u64 {
                    record.entry = Entry::decode(&value[value_start_index as usize..])?;
                }
                Ok(Some(record))
        }).collect();
        return records;
    }

    pub fn scard(&self, key: &str) -> Option<usize>{
        self.sets.scard(key)
    }

    pub fn zadd(&mut self, record:Record, score: f64) -> Option<usize>{
        Some(self.sorted_sets.put(std::str::from_utf8(&record.hint.key).unwrap(), Bytes::from(record.encode()), score))
    }

    pub fn zrem(&mut self, key: &str) -> Result<Option<Record>, DbError>{
        let node = self.sorted_sets.remove(key);
        if node.is_none() {
            return Ok(None);
        }
        let value = node.unwrap().borrow().value.clone();
        let value_start_index = u64::from_le_bytes(value[0..8].try_into().unwrap());
        let mut record = Record { hint: Hint::decode(&value[0..value_start_index as usize])?, entry: Entry::default()};
        if value_start_index < value.len() as u64 {
            record.entry = Entry::decode(&value[value_start_index as usize..])?;
        }
        Ok(Some(record))
    }

    pub fn get_by_rank_range(
        &mut self,
        start: usize,
        end: usize,
        remove: bool,
    ) -> Result<Vec<Record>, DbError>{
        let records: Result<Vec<Record>, DbError> = self.sorted_sets.get_by_rank_range(start, end, remove).iter().map(|node| {
            let value = node.borrow().value.clone();
            let value_start_index = u64::from_le_bytes(value[0..8].try_into().unwrap());
            let mut record = Record { hint: Hint::decode(&value[0..value_start_index as usize])?, entry: Entry::default()};
            if value_start_index < value.len() as u64 {
                record.entry = Entry::decode(&value[value_start_index as usize..])?;
            }
            Ok(record)
        }).collect();
        records
    }

    pub fn get_by_rank(
        &mut self,
        rank: usize,
        remove: bool,
    ) -> Result<Option<Record>, DbError>{
        let node = self.sorted_sets.get_by_rank(rank, remove);
        if node.is_none() {
            return Ok(None);
        }
        let value = node.unwrap().borrow().value.clone();
        let value_start_index = u64::from_le_bytes(value[0..8].try_into().unwrap());
        let mut record = Record { hint: Hint::decode(&value[0..value_start_index as usize])?, entry: Entry::default()};
        if value_start_index < value.len() as u64 {
            record.entry = Entry::decode(&value[value_start_index as usize..])?;
        }
        Ok(Some(record))
    }

    pub fn get_by_key(&self, key: &str) -> Result<Option<Record>, DbError>{
        let node = self.sorted_sets.get_by_key(key);
        if node.is_none() {
            return Ok(None);
        }
        let value = node.unwrap().borrow().value.clone();
        let value_start_index = u64::from_le_bytes(value[0..8].try_into().unwrap());
        let mut record = Record { hint: Hint::decode(&value[0..value_start_index as usize])?, entry: Entry::default()};
        if value_start_index < value.len() as u64 {
            record.entry = Entry::decode(&value[value_start_index as usize..])?;
        }
        Ok(Some(record))
    }

    pub fn get_by_score_range(
        &self,
        start: f64,
        end: f64,
        limit: usize,
        exclude_start: bool,
        exclude_end: bool,
    ) -> Result<Vec<Record>, DbError>{
        let records: Result<Vec<Record>, DbError> = self.sorted_sets.get_by_score_range(start, end, limit, exclude_start, exclude_end).iter().map(|node| {
            let value = node.borrow().value.clone();
            let value_start_index = u64::from_le_bytes(value[0..8].try_into().unwrap());
            let mut record = Record { hint: Hint::decode(&value[0..value_start_index as usize])?, entry: Entry::default()};
            if value_start_index < value.len() as u64 {
                record.entry = Entry::decode(&value[value_start_index as usize..])?;
            }
            Ok(record)
        }).collect();
        records
    }
}
