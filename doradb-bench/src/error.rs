use std::io;
use std::result::Result as StdResult;
use thiserror::Error;
use toml::de::Error as TomlDecodeError;
use toml::ser::Error as TomlEncodeError;

/// Benchmark crate result type.
pub type Result<T> = StdResult<T, BenchError>;

/// Error type used by the benchmark binary.
#[derive(Debug, Error)]
pub enum BenchError {
    #[error("{0}")]
    Message(String),
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("storage error: {0}")]
    Storage(#[from] doradb_storage::Error),
    #[error("manifest decode error: {0}")]
    TomlDecode(#[from] TomlDecodeError),
    #[error("manifest encode error: {0}")]
    TomlEncode(#[from] TomlEncodeError),
}

impl BenchError {
    pub(super) fn message(message: impl Into<String>) -> Self {
        Self::Message(message.into())
    }
}
