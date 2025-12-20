use crate::value::ValKind;
use bitflags::bitflags;
use semistr::SemiStr;

#[derive(Debug, Clone)]
pub struct SchemaSpec {
    pub schema_name: SemiStr,
}

impl SchemaSpec {
    #[inline]
    pub fn new(schema_name: &str) -> Self {
        Self {
            schema_name: SemiStr::new(schema_name),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableSpec {
    pub table_name: SemiStr,
    pub columns: Vec<ColumnSpec>,
}

impl TableSpec {
    #[inline]
    pub fn new(table_name: &str, columns: Vec<ColumnSpec>) -> Self {
        Self {
            table_name: SemiStr::new(table_name),
            columns,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnSpec {
    pub column_name: SemiStr,
    pub column_type: ValKind,
    pub column_attributes: ColumnAttributes,
}

impl ColumnSpec {
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexSpec {
    pub index_name: SemiStr,
    pub index_cols: Vec<IndexKey>,
    pub index_attributes: IndexAttributes,
}

impl IndexSpec {
    #[inline]
    pub fn new(name: &str, index_cols: Vec<IndexKey>, index_attributes: IndexAttributes) -> Self {
        let index_name = SemiStr::new(name);
        Self {
            index_name,
            index_cols,
            index_attributes,
        }
    }

    #[inline]
    pub fn unique(&self) -> bool {
        self.index_attributes.contains(IndexAttributes::PK)
            || self.index_attributes.contains(IndexAttributes::UK)
    }
}

bitflags! {
    pub struct ColumnAttributes: u32 {
        // whether value can be null.
        const NULLABLE = 0x01;
        // whether it belongs to any index.
        const INDEX = 0x02;
    }
}

bitflags! {
    pub struct IndexAttributes: u32 {
        const PK = 0x01;
        const UK = 0x02;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexKey {
    // This is user_col_idx. RowID is not included.
    pub col_no: u16,
    pub order: IndexOrder,
}

impl IndexKey {
    #[inline]
    pub fn new(col_no: u16) -> Self {
        IndexKey {
            col_no,
            order: IndexOrder::Asc,
        }
    }
}

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
