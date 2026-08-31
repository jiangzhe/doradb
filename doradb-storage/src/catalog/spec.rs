use super::index_ref::ColumnOrdinal;
#[cfg(test)]
use super::index_ref::IndexRef;
use crate::value::ValKind;
use bitflags::bitflags;

/// User-facing name-free storage table definition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageTableSpec {
    /// Ordered physical column definitions for the table.
    pub columns: Vec<StorageColumnSpec>,
}

impl StorageTableSpec {
    /// Creates a storage table specification from ordered columns.
    #[inline]
    pub fn new(columns: Vec<StorageColumnSpec>) -> Self {
        Self { columns }
    }
}

/// Logical value definition for one physical storage column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageColumnSpec {
    /// Logical value kind stored by the column.
    pub value_kind: ValKind,
    /// Column-level storage flags.
    pub flags: StorageColumnFlags,
}

impl StorageColumnSpec {
    /// Creates one storage column specification.
    #[inline]
    pub const fn new(value_kind: ValKind, flags: StorageColumnFlags) -> Self {
        Self { value_kind, flags }
    }
}

/// Name-free storage index definition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageIndexSpec {
    /// Ordered index key columns.
    pub keys: Vec<StorageIndexKey>,
    /// Index-level storage flags.
    pub flags: StorageIndexFlags,
}

impl StorageIndexSpec {
    /// Creates one storage index specification.
    #[inline]
    pub fn new(keys: Vec<StorageIndexKey>, flags: StorageIndexFlags) -> Self {
        Self { keys, flags }
    }

    /// Return whether this index enforces uniqueness.
    #[inline]
    pub fn unique(&self) -> bool {
        self.flags.contains(StorageIndexFlags::PK) || self.flags.contains(StorageIndexFlags::UK)
    }

    /// Return whether this index is the table primary key.
    #[inline]
    pub fn primary_key(&self) -> bool {
        self.flags.contains(StorageIndexFlags::PK)
    }
}

bitflags! {
    /// Column-level flags for storage schema definition.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct StorageColumnFlags: u32 {
        /// The column accepts `NULL` values.
        const NULLABLE = 0x01;
    }
}

bitflags! {
    /// Index-level flags for storage schema definition.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct StorageIndexFlags: u32 {
        /// The index is a primary key.
        const PK = 0x01;
        /// The index enforces uniqueness.
        const UK = 0x02;
    }
}

/// One indexed column descriptor inside an index definition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageIndexKey {
    /// Physical storage-column ordinal included in this key position.
    pub column_ordinal: ColumnOrdinal,
    /// Sort direction for this key column.
    pub order: IndexOrder,
}

impl StorageIndexKey {
    /// Creates an ascending index key on one storage column.
    #[inline]
    pub const fn new(column_ordinal: u16) -> Self {
        StorageIndexKey {
            column_ordinal: ColumnOrdinal::new(column_ordinal),
            order: IndexOrder::Asc,
        }
    }
}

/// Sort direction of one column in an index key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum IndexOrder {
    Asc = 0,
    Desc = 1,
}

impl TryFrom<u8> for IndexOrder {
    type Error = ();

    #[inline]
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(IndexOrder::Asc),
            1 => Ok(IndexOrder::Desc),
            _ => Err(()),
        }
    }
}

/// One active internal index definition paired with its exact generation.
#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ActiveIndexSpec {
    /// Exact table-local index generation and physical slot.
    pub(crate) index: IndexRef,
    /// Logical definition stored in this slot.
    pub(crate) spec: StorageIndexSpec,
}

#[cfg(test)]
impl ActiveIndexSpec {
    /// Creates one active exact-generation index specification.
    #[inline]
    pub(crate) fn new(index: IndexRef, spec: StorageIndexSpec) -> Self {
        Self { index, spec }
    }
}
