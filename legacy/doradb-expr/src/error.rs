use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, Error)]
pub enum Error {
    #[error("Value out of range")]
    ValueOutOfRange,
    #[error("Invalid Value format")]
    InvalidValueFormat,
    #[error("Unknown column type")]
    UnknownColumnType,
    #[error("Unknown argument type")]
    UnknownArgumentType,
    #[error("Type inference of subquery not supported")]
    InferSubqueryNotSupport,
    #[error("Invalid type to compare")]
    InvalidTypeToCompare,
}

impl From<std::num::ParseFloatError> for Error {
    fn from(_: std::num::ParseFloatError) -> Self {
        Error::InvalidValueFormat
    }
}
