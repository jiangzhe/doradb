use crate::buffer::page::PageID;
use crate::io::AIOError;
use crate::row::RowID;
use std::array::TryFromSliceError;
use std::fmt;
use std::ops::ControlFlow;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

/// Identifies which persisted CoW file surfaced a corruption failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PersistedFileKind {
    TableFile,
    CatalogMultiTableFile,
}

impl fmt::Display for PersistedFileKind {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            PersistedFileKind::TableFile => "table-file",
            PersistedFileKind::CatalogMultiTableFile => "catalog.mtb",
        })
    }
}

/// Identifies which persisted page kind failed integrity or root validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PersistedPageKind {
    TableMeta,
    MultiTableMeta,
    LwcPage,
    ColumnBlockIndex,
    ColumnDeletionBlob,
}

impl fmt::Display for PersistedPageKind {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            PersistedPageKind::TableMeta => "table-meta",
            PersistedPageKind::MultiTableMeta => "multi-table-meta",
            PersistedPageKind::LwcPage => "lwc-page",
            PersistedPageKind::ColumnBlockIndex => "column-block-index",
            PersistedPageKind::ColumnDeletionBlob => "column-deletion-blob",
        })
    }
}

/// Classifies why a persisted page was rejected during startup or checkpoint reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PersistedPageCorruptionCause {
    InvalidMagic,
    InvalidVersion,
    ChecksumMismatch,
    InvalidPayload,
    InvalidRootInvariant,
}

impl fmt::Display for PersistedPageCorruptionCause {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            PersistedPageCorruptionCause::InvalidMagic => "invalid magic",
            PersistedPageCorruptionCause::InvalidVersion => "invalid version",
            PersistedPageCorruptionCause::ChecksumMismatch => "checksum mismatch",
            PersistedPageCorruptionCause::InvalidPayload => "invalid payload",
            PersistedPageCorruptionCause::InvalidRootInvariant => "invalid root invariant",
        })
    }
}

/// Classifies which fatal storage path poisoned runtime admission.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoragePoisonSource {
    RedoSubmit,
    RedoWrite,
    RedoSync,
    CheckpointWrite,
    CheckpointSync,
    PurgeDeallocate,
}

impl fmt::Display for StoragePoisonSource {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            StoragePoisonSource::RedoSubmit => "redo submit",
            StoragePoisonSource::RedoWrite => "redo write",
            StoragePoisonSource::RedoSync => "redo sync",
            StoragePoisonSource::CheckpointWrite => "checkpoint write",
            StoragePoisonSource::CheckpointSync => "checkpoint sync",
            StoragePoisonSource::PurgeDeallocate => "purge deallocate",
        })
    }
}

#[derive(Debug, Clone, Error)]
pub enum Error {
    #[error("invalid argument")]
    InvalidArgument,
    #[error("invalid storage path: {0}")]
    InvalidStoragePath(String),
    #[error("storage layout mismatch: {0}")]
    StorageLayoutMismatch(String),
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
    #[error("storage engine shutdown")]
    StorageEngineShutdown,
    #[error("storage engine shutdown is waiting for {0} extra engine refs to drop")]
    StorageEngineShutdownBusy(usize),
    #[error("storage engine runtime is poisoned by fatal {0} failure")]
    StorageEnginePoisoned(StoragePoisonSource),
    #[error("{0}")]
    AIOError(#[from] AIOError),
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
    #[error("invalid compressed data")]
    InvalidCompressedData,
    #[error("column cannot be null")]
    ColumnNeverNull,
    #[error("invalid column scan")]
    InvalidColumnScan,
    #[error("column storage is required for column-path row lookup")]
    ColumnStorageMissing,
    #[error(
        "persisted page corrupted: file={file_kind}, page={page_kind}, page_id={page_id}, cause={cause}"
    )]
    PersistedPageCorrupted {
        file_kind: PersistedFileKind,
        page_kind: PersistedPageKind,
        page_id: PageID,
        cause: PersistedPageCorruptionCause,
    },
    #[error(
        "unexpected duplicate key during recovery index rebuild for index {index_no}: row_id={row_id}, deleted={deleted}"
    )]
    UnexpectedRecoveryDuplicateKey {
        index_no: usize,
        row_id: RowID,
        deleted: bool,
    },
    #[error("engine component already registered")]
    EngineComponentAlreadyRegistered,
    #[error("engine component missing dependency")]
    EngineComponentMissingDependency,
}

impl From<TryFromSliceError> for Error {
    #[inline]
    fn from(_src: TryFromSliceError) -> Error {
        Error::InvalidFormat
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

impl Error {
    /// Constructs a contextual corruption error for one persisted CoW page.
    #[inline]
    pub fn persisted_page_corrupted(
        file_kind: PersistedFileKind,
        page_kind: PersistedPageKind,
        page_id: PageID,
        cause: PersistedPageCorruptionCause,
    ) -> Self {
        Error::PersistedPageCorrupted {
            file_kind,
            page_kind,
            page_id,
            cause,
        }
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
