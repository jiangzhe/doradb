use semistr::SemiStr;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Schema '{0}' already exists")]
    SchemaAlreadyExists(SemiStr),
    #[error("Schema '{0}' not exists")]
    SchemaNotExists(SemiStr),
    #[error("Table '{0}' already exists")]
    TableAlreadyExists(SemiStr),
    #[error("Table '{0}' not exists")]
    TableNotExists(SemiStr),
    #[error("Index '{0}' not exists")]
    IndexNotExists(SemiStr),
    #[error("Column name '{0}' is not unique")]
    ColumnNameNotUnique(SemiStr),
}
