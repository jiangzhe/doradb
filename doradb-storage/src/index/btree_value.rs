use crate::buffer::page::INVALID_PAGE_ID;
use crate::index::util::Maskable;
use crate::row::INVALID_ROW_ID;
use std::ops::Deref;

/// BTreeValue is the value type stored in leaf node.
/// In branch node, the value type is always page id,
/// which is a logical pointer to child node.
///
/// There are two implementations of BTreeValue.
/// 1. BTreeU64(u64).
///    a) PageID(u64): value of branch node, represents
///    logical pointer to child node.
///    b) RowID(u64), which supports unique index.
/// 3. Single byte(u8), which supports non-unique index.
///    Non-unique index is a bit complicated.
///    The key of non-unique index is user-defined
///    key followed by RowID, to make it unique for
///    B-tree operations(e.g. deletion).
///    As a consequence, if we still use RowID as
///    value type, it's waste of space.
///    Instead, we can only store single byte as its
///    value, in order to represent delete bit.
///    If we want to retrieve value, we can always
///    extract last 8 byte from key and convert it
///    to RowID.
pub trait BTreeValue: Maskable {}

/// U64 value type to support both page id in branch node
/// and row id in leaf node.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct BTreeU64(u64);

impl Deref for BTreeU64 {
    type Target = u64;
    #[inline]
    fn deref(&self) -> &u64 {
        &self.0
    }
}

impl From<u64> for BTreeU64 {
    #[inline]
    fn from(value: u64) -> Self {
        BTreeU64(value)
    }
}

impl BTreeU64 {
    #[inline]
    pub fn to_u64(self) -> u64 {
        self.0
    }
}

const BTREE_VALUE_U64_DELETE_BIT: u64 = 1u64 << 63;

const _: () = assert!(BTreeU64::INVALID_VALUE.0 == INVALID_ROW_ID);
const _: () = assert!(BTreeU64::INVALID_VALUE.0 == INVALID_PAGE_ID);

impl Maskable for BTreeU64 {
    const INVALID_VALUE: Self = BTreeU64(!0);

    #[inline]
    fn deleted(self) -> Self {
        BTreeU64(*self | BTREE_VALUE_U64_DELETE_BIT)
    }

    #[inline]
    fn value(self) -> Self {
        BTreeU64(*self & !BTREE_VALUE_U64_DELETE_BIT)
    }

    #[inline]
    fn is_deleted(self) -> bool {
        *self & BTREE_VALUE_U64_DELETE_BIT != 0
    }
}

impl BTreeValue for BTreeU64 {}
