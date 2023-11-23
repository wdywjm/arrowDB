use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct Meta {
    pub bucket: Bytes,
    pub bucket_size: u32,
    pub key_size: u32,
    pub value_size: u32,
    pub timestamp: i64,
    pub ttl: u32,
    pub operate: u16,
    pub data_type: u16,
    pub tx_id: u64,
    pub status: u16,
}

impl Meta {
    pub fn new(
        bucket: Bytes,
        key_size: u32,
        value_size: u32,
        timestamp: i64,
        ttl: u32,
        operate: u16,
        data_type: u16,
        tx_id: u64,
        status: u16,
    ) -> Self {
        let bucket_len = bucket.len() as u32;
        Meta {
            bucket,
            bucket_size: bucket_len,
            key_size,
            value_size,
            timestamp,
            ttl,
            operate,
            data_type,
            tx_id,
            status,
        }
    }

    pub fn set_entry_header_buf<'a>(&self, buf: &'a mut [u8]) -> &'a mut [u8] {
        let timestamp_bytes = self.timestamp.to_le_bytes();
        buf[4..12].copy_from_slice(&timestamp_bytes);
        let key_size_bytes = self.key_size.to_le_bytes();
        buf[12..16].copy_from_slice(&key_size_bytes);
        let value_size_bytes = self.value_size.to_le_bytes();
        buf[16..20].copy_from_slice(&value_size_bytes);
        let op_bytes = self.operate.to_le_bytes();
        buf[20..22].copy_from_slice(&op_bytes);
        let ttl_bytes = self.ttl.to_le_bytes();
        buf[22..26].copy_from_slice(&ttl_bytes);
        let bucket_size_bytes = self.bucket_size.to_le_bytes();
        buf[26..30].copy_from_slice(&bucket_size_bytes);
        let status_bytes = self.status.to_le_bytes();
        buf[30..32].copy_from_slice(&status_bytes);
        let ds_bytes = self.data_type.to_le_bytes();
        buf[32..34].copy_from_slice(&ds_bytes);
        let txid_bytes = self.tx_id.to_le_bytes();
        buf[34..42].copy_from_slice(&txid_bytes);

        buf
    }

    pub fn parse_entry_header_buf(buf: &[u8]) -> Self {
        let timestamp_bytes = <[u8; 8]>::try_from(&buf[4..12]).unwrap();
        let timestamp = i64::from_le_bytes(timestamp_bytes);
        let key_size_bytes = <[u8; 4]>::try_from(&buf[12..16]).unwrap();
        let key_size = u32::from_le_bytes(key_size_bytes);
        let value_size_bytes = <[u8; 4]>::try_from(&buf[16..20]).unwrap();
        let value_size = u32::from_le_bytes(value_size_bytes);
        let op_bytes = <[u8; 2]>::try_from(&buf[20..22]).unwrap();
        let operate = u16::from_le_bytes(op_bytes);
        let ttl_bytes = <[u8; 4]>::try_from(&buf[22..26]).unwrap();
        let ttl = u32::from_le_bytes(ttl_bytes);
        let bucket_size_bytes = <[u8; 4]>::try_from(&buf[26..30]).unwrap();
        let bucket_size = u32::from_le_bytes(bucket_size_bytes);
        let status_bytes = <[u8; 2]>::try_from(&buf[30..32]).unwrap();
        let status = u16::from_le_bytes(status_bytes);
        let ds_bytes = <[u8; 2]>::try_from(&buf[32..34]).unwrap();
        let data_type = u16::from_le_bytes(ds_bytes);
        let txid_bytes = <[u8; 8]>::try_from(&buf[34..42]).unwrap();
        let tx_id = u64::from_le_bytes(txid_bytes);
        let bucket = Bytes::copy_from_slice(&buf[42..42 + bucket_size as usize]);

        Meta {
            bucket,
            bucket_size,
            key_size,
            value_size,
            timestamp,
            ttl,
            operate,
            data_type,
            tx_id,
            status,
        }
    }
}
