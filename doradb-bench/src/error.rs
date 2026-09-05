use doradb_storage::CallbackError;
use std::convert::Infallible;
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
    #[error("TOML decode error: {0}")]
    TomlDecode(#[from] TomlDecodeError),
    #[error("TOML encode error: {0}")]
    TomlEncode(#[from] TomlEncodeError),
}

impl BenchError {
    pub(super) fn message(message: impl Into<String>) -> Self {
        Self::Message(message.into())
    }
}

/// Preserves application errors and classifies engine failures as storage errors.
impl From<CallbackError<BenchError>> for BenchError {
    fn from(error: CallbackError<BenchError>) -> Self {
        match error {
            CallbackError::Engine(error) => Self::Storage(error),
            CallbackError::User(error) => error,
        }
    }
}

/// Converts callbacks that can fail only through engine operations.
impl From<CallbackError<Infallible>> for BenchError {
    fn from(error: CallbackError<Infallible>) -> Self {
        Self::Storage(error.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn callback_user_error_preserves_owned_payload() {
        let message = String::from("application marker");
        let address = message.as_ptr();
        let error = BenchError::from(CallbackError::User(BenchError::Message(message)));
        let BenchError::Message(message) = error else {
            panic!("callback user error must preserve its variant")
        };
        assert_eq!(message, "application marker");
        assert_eq!(message.as_ptr(), address);
    }
}
