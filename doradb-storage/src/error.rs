use crate::io::AIOError;
use doradb_datatype::error::Error as DataTypeError;
use std::array::TryFromSliceError;
use std::ops::ControlFlow;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, Error)]
pub enum Error {
    #[error("invalid argument")]
    InvalidArgument,
    #[error("internal error")]
    InternalError,
    #[error("invalid state")]
    InvalidState,
    #[error("Invalid format")]
    InvalidFormat,
    #[error("Checksum mismatch")]
    ChecksumMismatch,
    #[error("IO Error")]
    IOError,
    #[error("Data type not supported")]
    DataTypeNotSupported,
    #[error("Index out of bound")]
    IndexOutOfBound,
    #[error("Invalid codec for selection")]
    InvalidCodecForSel,
    #[error("Value count mismatch")]
    ValueCountMismatch,
    #[error("Invalid datatype")]
    InvalidDatatype,
    // buffer pool errors
    #[error("insufficient memory({0})")]
    InsufficientMemory(usize),
    #[error("buffer page already allocated")]
    BufferPageAlreadyAllocated,
    #[error("empty free list of buffer pool")]
    EmptyFreeListOfBufferPool,
    #[error("buffer pool size is too small")]
    BufferPoolSizeTooSmall,
    // row action errors
    #[error("row not found")]
    RowNotFound,
    #[error("insufficient free space for in-place update")]
    InsufficientFreeSpaceForInplaceUpdate,
    #[error("Transaction system shutdown")]
    TransactionSystemShutdown,
    #[error("{0}")]
    AIOError(#[from] AIOError),
    #[error("schema not found")]
    SchemaNotFound,
    #[error("schema not deleted")]
    SchemaNotDeleted,
    #[error("schema already exists")]
    SchemaAlreadyExists,
    #[error("table not found")]
    TableNotFound,
    #[error("table not deleted")]
    TableNotDeleted,
    #[error("table already exists")]
    TableAlreadyExists,
    #[error("glob error")]
    GlobError,
    #[error("log file corrupted")]
    LogFileCorrupted,
    #[error("channel send error")]
    SendError,
    #[error("torn write on file")]
    TornWrite,
    #[error("{0} not supported")]
    NotSupported(&'static str),
}

impl From<TryFromSliceError> for Error {
    #[inline]
    fn from(_src: TryFromSliceError) -> Error {
        Error::InvalidFormat
    }
}

impl From<DataTypeError> for Error {
    #[inline]
    fn from(src: DataTypeError) -> Self {
        match src {
            DataTypeError::InvalidFormat => Error::InvalidFormat,
            DataTypeError::IOError => Error::IOError,
        }
    }
}

impl From<std::io::Error> for Error {
    #[inline]
    fn from(_src: std::io::Error) -> Self {
        Error::IOError
    }
}

impl From<std::str::Utf8Error> for Error {
    #[inline]
    fn from(_src: std::str::Utf8Error) -> Error {
        Error::InvalidFormat
    }
}

impl From<std::num::ParseIntError> for Error {
    #[inline]
    fn from(_src: std::num::ParseIntError) -> Error {
        Error::InvalidFormat
    }
}

impl From<glob::GlobError> for Error {
    #[inline]
    fn from(_src: glob::GlobError) -> Self {
        Error::GlobError
    }
}

/// Validation of optimistic lock
pub enum Validation<T> {
    Valid(T),
    Invalid,
}

impl<T> Validation<T> {
    #[inline]
    pub fn branch(self) -> ControlFlow<(), T> {
        match self {
            Validation::Valid(v) => ControlFlow::Continue(v),
            Validation::Invalid => ControlFlow::Break(()),
        }
    }

    #[inline]
    pub fn map<U, F: FnOnce(T) -> U>(self, f: F) -> Validation<U> {
        match self {
            Validation::Valid(v) => Validation::Valid(f(v)),
            Validation::Invalid => Validation::Invalid,
        }
    }

    #[inline]
    pub fn and_then<U, F>(self, f: F) -> Validation<U>
    where
        F: FnOnce(T) -> Validation<U>,
    {
        match self {
            Validation::Valid(v) => f(v),
            Validation::Invalid => Validation::Invalid,
        }
    }

    #[inline]
    pub fn expect(self, msg: &str) -> T {
        match self {
            Validation::Valid(v) => v,
            Validation::Invalid => unwrap_failed(msg),
        }
    }

    #[inline]
    pub fn unwrap(self) -> T {
        match self {
            Validation::Valid(v) => v,
            Validation::Invalid => unwrap_failed_no_message(),
        }
    }

    #[inline]
    pub fn is_valid(&self) -> bool {
        matches!(self, Validation::Valid(_))
    }

    #[inline]
    pub fn is_invalid(&self) -> bool {
        matches!(self, Validation::Invalid)
    }
}

#[cold]
#[inline(never)]
fn unwrap_failed(msg: &str) -> ! {
    panic!("{msg}")
}

#[cold]
#[inline(never)]
const fn unwrap_failed_no_message() -> ! {
    panic!("called `Validation::unwrap()` on a `Invalid` value")
}

macro_rules! verify {
    ($exp:expr) => {
        match $exp {
            Validation::Valid(v) => v,
            Validation::Invalid => return Validation::Invalid,
        }
    };
}

macro_rules! verify_continue {
    ($exp:expr) => {
        match $exp {
            Validation::Invalid => continue,
            Validation::Valid(v) => v,
        }
    };
}
