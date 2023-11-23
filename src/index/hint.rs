use crate::data::meta::Meta;
use crate::data::ENTRYHEADERSIZE;
use crate::errors::DbError;
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct Hint {
    pub key: Bytes,
    pub file_id: u32,
    pub offset: u64,
    pub meta: Meta,
}

impl Hint {
    pub fn new(key: Bytes, file_id: u32, offset: u64, meta: Meta) -> Self {
        Hint {
            key,
            file_id,
            offset,
            meta,
        }
    }

    pub fn size(&self) -> usize {
        ENTRYHEADERSIZE - 4 + self.meta.bucket_size as usize + self.key.len() as usize + 12
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = vec![0; self.size()];
        self.meta.set_entry_header_buf(&mut buf);
        buf[0..4].copy_from_slice(&self.file_id.to_le_bytes());
        buf[ENTRYHEADERSIZE..(ENTRYHEADERSIZE + self.meta.bucket_size as usize)]
            .copy_from_slice(&self.meta.bucket);
        buf[(ENTRYHEADERSIZE + self.meta.bucket_size as usize)
            ..(ENTRYHEADERSIZE + self.meta.bucket_size as usize + self.meta.key_size as usize)]
            .copy_from_slice(&self.key);
        buf[(ENTRYHEADERSIZE + self.meta.bucket_size as usize)
            ..(ENTRYHEADERSIZE + self.meta.bucket_size as usize + self.meta.key_size as usize)]
            .copy_from_slice(&self.key);
        buf[(ENTRYHEADERSIZE + self.meta.bucket_size as usize + self.meta.key_size as usize)
            ..(ENTRYHEADERSIZE + self.meta.bucket_size as usize + self.meta.key_size as usize + 8)]
            .copy_from_slice(&self.offset.to_le_bytes());

        buf
    }

    pub fn decode(buf: &[u8]) -> Result<Self, DbError> {
        let meta = Meta::parse_entry_header_buf(&buf[..ENTRYHEADERSIZE]);

        let file_id = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        let key = buf[(ENTRYHEADERSIZE + meta.bucket_size as usize)
            ..(ENTRYHEADERSIZE + meta.bucket_size as usize + meta.key_size as usize)]
            .to_vec();
        let offset = u64::from_le_bytes(
            buf[(ENTRYHEADERSIZE + meta.bucket_size as usize + meta.key_size as usize)
                ..(ENTRYHEADERSIZE + meta.bucket_size as usize + meta.key_size as usize + 8)]
                .try_into()
                .unwrap(),
        );

        Ok(Hint {
            key: Bytes::from(key),
            file_id,
            offset,
            meta,
        })
    }
}
