use std::sync::Arc;

use crate::{db::DB, data::entry::Entry};

pub struct Tx {
    db: Arc<DB>,
    writable: bool,
    pending_writes: Vec<Entry>
}