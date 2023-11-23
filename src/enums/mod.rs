use num_enum::{FromPrimitive, IntoPrimitive, TryFromPrimitive};

pub static B: u64 = 1;
pub static KB: u64 = 1024 * B;
pub static MB: u64 = 1024 * KB;
pub static GB: u64 = 1024 * MB;

pub static SEPARATOR: u8 = b'#';

#[derive(Debug, Clone, IntoPrimitive, TryFromPrimitive, Default)]
#[repr(usize)]
pub enum DataTypes {
    #[default]
    String = 1,
    List = 2,
    Set = 3,
    SortedSet = 4,
}

#[derive(Debug, Clone, IntoPrimitive, TryFromPrimitive, Default)]
#[repr(usize)]
pub enum EntryStatus {
    #[default]
    Uncommited = 1,
    Commited = 2,
}

#[derive(Debug, Clone, IntoPrimitive, TryFromPrimitive, Default, PartialEq, Eq)]
#[repr(usize)]
pub enum EntryOperate {
    #[default]
    Put = 1,
    Del = 2,
    Ttl = 3,
    LLpush = 4,
    LLpop = 5,
    LRpush = 6,
    LRpop = 7,
    LLpushx = 8,
    LRpushx = 9,
    LRem = 10,
    LLen = 11,
    LIndex = 12,
    LPos = 13,
    LSet = 14,
    LRange = 15,
    SAdd = 16,
    SScard = 17,
    SDiff = 18,
    SUnion = 19,
    SInter = 20,
    SIsmember = 21,
    SMembers = 22,
    SRem = 23,
    ZPut = 24,
    ZRem = 25,
    ZGetByRankRange = 26,
    ZGetByRank = 27,
    ZGetByKey = 28,
    ZFindRank = 29,
    ZFindRevRank = 30,
    ZGetByScoreRange = 31,
}

#[derive(Debug, Clone, IntoPrimitive, TryFromPrimitive, Default)]
#[repr(usize)]
pub enum IndexMode {
    #[default]
    KeysInRAM = 1,
    KeysValuesInAam = 2,
    SparseKeysInRAM = 3,
}

#[derive(Debug, Clone, IntoPrimitive, TryFromPrimitive, Default)]
#[repr(usize)]
pub enum RWMode {
    #[default]
    StdIO = 1,
    MMap = 2,
}
