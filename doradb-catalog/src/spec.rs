use crate::{ColumnAttributes, IndexAttributes};
use doradb_datatype::PreciseType;
use semistr::SemiStr;

#[derive(Debug)]
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
#[derive(Debug)]
pub struct TableSpec {
    pub table_name: SemiStr,
    pub columns: Vec<ColumnSpec>,
}

#[derive(Debug)]
pub struct ColumnSpec {
    pub column_name: SemiStr,
    pub column_type: PreciseType,
    pub column_attributes: ColumnAttributes,
}

#[derive(Debug)]
pub struct IndexSpec {
    pub index_name: SemiStr,
    pub index_cols: Vec<u16>,
    pub index_attributes: IndexAttributes,
}
