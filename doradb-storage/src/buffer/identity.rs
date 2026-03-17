use serde::{Deserialize, Serialize};
use std::fmt;

/// Predefined selector of one logical buffer-pool role.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PoolRole {
    #[default]
    Invalid = 0,
    Meta = 1,
    Index = 2,
    Mem = 3,
    Disk = 4,
}

impl PoolRole {
    #[inline]
    pub(crate) fn assert_valid(self, context: &'static str) {
        if matches!(self, Self::Invalid) {
            panic!("invalid pool role in {context}");
        }
    }

    #[inline]
    pub(crate) fn row_pool_role(self) -> RowPoolRole {
        match self {
            Self::Meta => RowPoolRole::Meta,
            Self::Mem => RowPoolRole::Mem,
            _ => panic!("pool role {:?} is not a row pool", self),
        }
    }
}

/// Runtime-only provenance token for one exact buffer-pool instance.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct PoolIdentity(usize);

impl PoolIdentity {
    #[inline]
    pub(crate) fn from_owner_addr(owner_addr: usize) -> Self {
        debug_assert_ne!(owner_addr, 0);
        Self(owner_addr)
    }
}

impl fmt::Debug for PoolIdentity {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PoolIdentity({:#x})", self.0)
    }
}

/// Selector of the row-page pool assigned to one table runtime.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum RowPoolRole {
    Meta = 1,
    Mem = 2,
}

impl From<RowPoolRole> for PoolRole {
    #[inline]
    fn from(value: RowPoolRole) -> Self {
        match value {
            RowPoolRole::Meta => PoolRole::Meta,
            RowPoolRole::Mem => PoolRole::Mem,
        }
    }
}
