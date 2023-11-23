use crossbeam_channel::SendError;
use thiserror::Error;

use crate::index::Record;

#[derive(Debug, Error)]
pub enum DbError {
    #[error("{method} offset {offset} out of range")]
    OffsetOutOfRange { method: String, offset: u64 },
    #[error(transparent)]
    IOError(#[from] std::io::Error),
    #[error(transparent)]
    OtherError(#[from] anyhow::Error),

    #[error("bucket:{bucket} key:{key} decode error, msg:{msg}")]
    EntryDecodeError {
        bucket: String,
        key: String,
        msg: String,
    },

    #[error("bucket:{bucket} key:{key} decode error")]
    EntryCRCInvalid { bucket: String, key: String },

    #[error("bucket:{bucket} key:{key} data_type {data_type} not support op {op}")]
    EntryDataTypeOpInvalid {
        bucket: String,
        key: String,
        op: u16,
        data_type: u16,
    },

    #[error("bucket {bucket} not exist")]
    BucketNotExist { bucket: String },

    #[error("key contains separator char {separator}")]
    ContainSeparatorChar { separator: char },

    #[error("sender send record error")]
    BackgroundWorkerSendError(#[from] SendError<Record>)
}
