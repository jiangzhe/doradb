use doradb_storage::error::Error as StorageError;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, Error)]
pub enum Error {
    #[error("Row number mismtach")]
    RowNumberMismatch,
    #[error("Missing attribute")]
    MissingAttr,
    #[error("unsupported evaluation")]
    UnsupportedEval,
    #[error("Invalid evaluation plan")]
    InvalidEvalPlan,
    #[error("Failed to build eval arguments")]
    FailToBuildEvalArgs,
    #[error("Failed to build eval output")]
    FailToBuildEvalOutput,
    #[error("Failed to fetch attribute")]
    FailToFetchAttr,
    #[error("Failed to fetch eval cache")]
    FailToFetchEvalCache,
    #[error("Index out of bound")]
    IndexOutOfBound,
    #[error("Invalid codec for selection")]
    InvalidCodecForSel,
    #[error("{0}")]
    ExpressionError(#[from] doradb_expr::error::Error),
}

impl From<StorageError> for Error {
    #[inline]
    fn from(src: StorageError) -> Self {
        match src {
            StorageError::IndexOutOfBound => Error::IndexOutOfBound,
            StorageError::InvalidCodecForSel => Error::InvalidCodecForSel,
            _ => todo!(),
        }
    }
}
