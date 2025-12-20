use crate::{ColumnAttributes, IndexAttributes, IndexKey};
use doradb_datatype::PreciseType;
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
    pub column_type: PreciseType,
    pub column_attributes: ColumnAttributes,
}

impl ColumnSpec {
    #[inline]
    pub fn new(
        column_name: &str,
        column_type: PreciseType,
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
