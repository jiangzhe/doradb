use crate::value::ValKind;
use bitflags::bitflags;
use semistr::SemiStr;

/// Stable table-local secondary-index number.
pub type IndexNo = u16;

/// User-facing table definition used by DDL/create-table paths.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableSpec {
    /// Ordered user column definitions for the table.
    pub columns: Vec<ColumnSpec>,
}

impl TableSpec {
    /// Create a table spec from ordered column definitions.
    #[inline]
    pub fn new(columns: Vec<ColumnSpec>) -> Self {
        Self { columns }
    }
}

/// Logical column definition in a table schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnSpec {
    /// User-visible column name.
    pub column_name: SemiStr,
    /// Logical value kind stored by the column.
    pub column_type: ValKind,
    /// Column-level schema attributes.
    pub column_attributes: ColumnAttributes,
}

impl ColumnSpec {
    /// Create one column specification.
    #[inline]
    pub fn new(
        column_name: &str,
        column_type: ValKind,
        column_attributes: ColumnAttributes,
    ) -> Self {
        Self {
            column_name: SemiStr::new(column_name),
            column_type,
            column_attributes,
        }
    }
}

/// Logical index definition in a table schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexSpec {
    /// Ordered index key columns.
    pub cols: Vec<IndexKey>,
    /// Index-level schema attributes.
    pub attributes: IndexAttributes,
}

impl IndexSpec {
    /// Create one index specification.
    #[inline]
    pub fn new(cols: Vec<IndexKey>, attributes: IndexAttributes) -> Self {
        Self { cols, attributes }
    }

    /// Return whether this index enforces uniqueness.
    #[inline]
    pub fn unique(&self) -> bool {
        self.attributes.contains(IndexAttributes::PK)
            || self.attributes.contains(IndexAttributes::UK)
    }
}

/// One active index definition paired with its allocated stable table-local
/// index number.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ActiveIndexSpec {
    /// Stable table-local index number.
    pub(crate) index_no: IndexNo,
    /// Logical index definition stored in this slot.
    pub(crate) spec: IndexSpec,
}

impl ActiveIndexSpec {
    /// Create one active index specification.
    #[inline]
    pub(crate) fn new(index_no: IndexNo, spec: IndexSpec) -> Self {
        Self { index_no, spec }
    }
}

bitflags! {
    /// Column-level attributes for schema definition.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct ColumnAttributes: u32 {
        // whether value can be null.
        const NULLABLE = 0x01;
        // whether it belongs to any index.
        const INDEX = 0x02;
    }
}

bitflags! {
    /// Index-level attributes for schema definition.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct IndexAttributes: u32 {
        const PK = 0x01;
        const UK = 0x02;
    }
}

/// One indexed column descriptor inside an index definition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexKey {
    /// User-column number included in this key position.
    pub col_no: u16,
    /// Sort direction for this key column.
    pub order: IndexOrder,
}

impl IndexKey {
    /// Create an index key on one user column with ascending order.
    #[inline]
    pub fn new(col_no: u16) -> Self {
        IndexKey {
            col_no,
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

impl From<u8> for IndexOrder {
    #[inline]
    fn from(value: u8) -> Self {
        match value {
            0 => IndexOrder::Asc,
            1 => IndexOrder::Desc,
            _ => panic!("unexpected index order"),
        }
    }
}
