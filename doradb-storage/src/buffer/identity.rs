use serde::{Deserialize, Serialize};

/// Stable identity assigned to one predefined buffer pool.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PoolIdentity {
    #[default]
    Invalid = 0,
    Meta = 1,
    Index = 2,
    Mem = 3,
    Disk = 4,
}

impl PoolIdentity {
    #[inline]
    pub(crate) fn assert_valid(self, context: &'static str) {
        if matches!(self, Self::Invalid) {
            panic!("invalid pool identity in {context}");
        }
    }
}

/// Identity of the row-page pool assigned to one table runtime.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum RowPoolIdentity {
    Meta = 1,
    Mem = 2,
}

impl From<RowPoolIdentity> for PoolIdentity {
    #[inline]
    fn from(value: RowPoolIdentity) -> Self {
        match value {
            RowPoolIdentity::Meta => PoolIdentity::Meta,
            RowPoolIdentity::Mem => PoolIdentity::Mem,
        }
    }
}
