use super::{meta::Meta, ENTRYHEADERSIZE};
use crate::errors::DbError;
use bytes::{Bytes, BytesMut};
use chrono::Local;
use crc::{Crc, CRC_32_ISCSI};

#[derive(Debug, Clone, Default)]
pub struct Entry {
    pub key: Bytes,
    pub value: Bytes,
    pub meta: Meta,
    pub crc: u32,
}

// Encode returns the slice after the entry be encoded.
//
//  the entry stored format:
//  |----------------------------------------------------------------------------------------------------------------------|
//  |  crc  | timestamp | ksz | valueSize | op  | TTL  |bucketSize| status | datatype   | txId |  bucket |  key  | value   |
//  |----------------------------------------------------------------------------------------------------------------------|
//  | uint32| uint64  |uint32 |  uint32 | uint16  | uint32| uint32 | uint16 | uint16    |uint64|[]byte|[]byte | []byte     |
//  |----------------------------------------------------------------------------------------------------------------------|
//

impl Entry {
    pub fn size(&self) -> usize {
        ENTRYHEADERSIZE
            + self.meta.key_size as usize
            + self.meta.value_size as usize
            + self.meta.bucket_size as usize
    }

    pub fn is_expired(&self) -> bool {
        if self.meta.ttl == 0 {
            return false;
        }
        let ex = if self.meta.ttl + self.meta.timestamp as u32 > Local::now().timestamp() as u32 {
            false
        } else {
            true
        };
        ex
    }

    pub fn encode(&self) -> Vec<u8> {
        let key_size = self.meta.key_size;
        let value_size = self.meta.value_size;
        let bucket_size = self.meta.bucket_size;

        // set DataItemHeader buf
        let mut buf = vec![0; self.size()];
        self.meta.set_entry_header_buf(&mut buf);
        // set bucket\key\value
        buf[ENTRYHEADERSIZE..(ENTRYHEADERSIZE + bucket_size as usize)]
            .copy_from_slice(&self.meta.bucket);
        buf[(ENTRYHEADERSIZE + bucket_size as usize)
            ..(ENTRYHEADERSIZE + bucket_size as usize + key_size as usize)]
            .copy_from_slice(&self.key);
        buf[(ENTRYHEADERSIZE + bucket_size as usize + key_size as usize)
            ..(ENTRYHEADERSIZE + bucket_size as usize + key_size as usize + value_size as usize)]
            .copy_from_slice(&self.value);

        let crc = Crc::<u32>::new(&CRC_32_ISCSI);
        let c32 = crc.checksum(&buf[4..]);
        buf[0..4].copy_from_slice(&c32.to_le_bytes());

        buf
    }

    pub fn decode(buf: &[u8]) -> Result<Self, DbError> {
        let timestamp = i64::from_le_bytes(buf[4..12].try_into().unwrap());
        let key_size = u32::from_le_bytes(buf[12..16].try_into().unwrap());
        let value_size = u32::from_le_bytes(buf[16..20].try_into().unwrap());
        let op = u16::from_le_bytes(buf[20..22].try_into().unwrap());
        let ttl = u32::from_le_bytes(buf[22..26].try_into().unwrap());
        let bucket_size = u32::from_le_bytes(buf[26..30].try_into().unwrap());
        let status = u16::from_le_bytes(buf[30..32].try_into().unwrap());
        let data_type = u16::from_le_bytes(buf[32..34].try_into().unwrap());
        let tx_id = u64::from_le_bytes(buf[34..42].try_into().unwrap());
        let bucket = buf[42..(42 + bucket_size as usize)].to_vec();
        let key = buf[(42 + bucket_size as usize)..(42 + bucket_size as usize + key_size as usize)]
            .to_vec();
        let value = buf[(42 + bucket_size as usize + key_size as usize)
            ..(42 + bucket_size as usize + key_size as usize + value_size as usize)]
            .to_vec();

        let crc = Crc::<u32>::new(&CRC_32_ISCSI);
        let expected_crc = crc.checksum(&buf[4..]);
        let actual_crc = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        if actual_crc != expected_crc {
            return Err(DbError::EntryCRCInvalid {
                bucket: String::from_utf8(bucket).unwrap_or("".to_owned()),
                key: String::from_utf8(key).unwrap_or("".to_owned()),
            });
        }

        Ok(Entry {
            meta: Meta {
                timestamp,
                key_size,
                value_size,
                operate: op,
                ttl,
                bucket_size,
                status,
                data_type,
                tx_id,
                bucket: Bytes::from(bucket),
            },
            key: Bytes::from(key),
            value: Bytes::from(value),
            crc: expected_crc,
        })
    }
}
