use crate::row::RowID;
use error_stack::Report;
use std::array::TryFromSliceError;
use std::fmt;
use std::ops::ControlFlow;
use thiserror::Error as ThisError;

pub type Result<T> = std::result::Result<T, Error>;
pub(crate) type ConfigResult<T> = std::result::Result<T, Report<ConfigError>>;
pub(crate) type DataIntegrityResult<T> = std::result::Result<T, Report<DataIntegrityError>>;

/// Public storage error boundary classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ThisError)]
pub enum ErrorKind {
    /// Invalid static or startup configuration.
    #[error("configuration error")]
    Config,
    /// Caller-provided arguments or shapes are invalid.
    #[error("invalid input")]
    InvalidInput,
    /// A requested storage operation cannot complete in the current logical state.
    #[error("operation error")]
    Operation,
    /// Storage memory, buffer, or file capacity was exhausted.
    #[error("resource exhausted")]
    Resource,
    /// An operating-system or async-channel IO boundary failed.
    #[error("io error")]
    Io,
    /// Persisted bytes or recovery invariants failed integrity checks.
    #[error("data integrity error")]
    DataIntegrity,
    /// Storage lifecycle state rejected the request.
    #[error("storage lifecycle error")]
    Lifecycle,
    /// An internal invariant or component construction contract was violated.
    #[error("internal storage error")]
    Internal,
    /// A fatal runtime failure poisoned future storage admission.
    #[error("fatal storage error")]
    Fatal,
}

/// Fieldless config-domain errors carried underneath `ErrorKind::Config`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ThisError)]
pub(crate) enum ConfigError {
    #[error("invalid catalog file name")]
    InvalidCatalogFileName,
    #[error("invalid log file stem")]
    InvalidLogFileStem,
    #[error("invalid log partition count")]
    InvalidLogPartitions,
    #[error("path must not be empty")]
    PathMustNotBeEmpty,
    #[error("path must be valid UTF-8")]
    PathMustBeUtf8,
    #[error("path must use required suffix")]
    PathMustUseRequiredSuffix,
    #[error("path must resolve to a file path")]
    PathMustResolveToFile,
    #[error("path must be relative to storage_root")]
    PathMustBeRelativeToStorageRoot,
    #[error("path must not escape storage_root")]
    PathMustNotEscapeStorageRoot,
    #[error("path must not contain parent traversal")]
    PathMustNotContainParentTraversal,
    #[error("paths must not overlap")]
    PathsMustNotOverlap,
    #[error("path must not overlap reserved storage location")]
    PathMustNotOverlapReservedLocation,
    #[error("path must not use reserved parent directory")]
    PathMustNotUseReservedParentDirectory,
    #[error("path must not use durable storage suffix")]
    PathMustNotUseDurableStorageSuffix,
    #[error("invalid storage layout marker")]
    InvalidStorageLayoutMarker,
    #[error("storage layout mismatch")]
    StorageLayoutMismatch,
}

/// Fieldless data-integrity-domain errors carried underneath `ErrorKind::DataIntegrity`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ThisError)]
pub(crate) enum DataIntegrityError {
    #[error("invalid magic")]
    InvalidMagic,
    #[error("invalid version")]
    InvalidVersion,
    #[error("checksum mismatch")]
    ChecksumMismatch,
    #[error("torn write")]
    TornWrite,
    #[error("invalid payload")]
    InvalidPayload,
    #[error("invalid root invariant")]
    InvalidRootInvariant,
    #[error("log file corrupted")]
    LogFileCorrupted,
    #[error("unexpected recovery duplicate key")]
    UnexpectedRecoveryDuplicateKey,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ErrorCode {
    InvalidArgument,
    InternalError,
    InvalidState,
    WrongSecondaryIndexBinding,
    OldTableRootAlreadyRetained,
    IOError,
    StorageIOError,
    ShortIO,
    StorageFileCapacityExceeded,
    DataTypeNotSupported,
    IndexOutOfBound,
    InvalidCodecForSel,
    ValueCountMismatch,
    InvalidDatatype,
    InsufficientMemory,
    BufferPageAlreadyAllocated,
    BufferPoolFull,
    EmptyFreeListOfBufferPool,
    BufferPoolSizeTooSmall,
    RowNotFound,
    InsufficientFreeSpaceForInplaceUpdate,
    StorageEngineShutdown,
    StorageEngineShutdownBusy,
    StorageEnginePoisoned,
    TableNotFound,
    TableNotDeleted,
    TableAlreadyExists,
    GlobError,
    SendError,
    NotSupported,
    ColumnNeverNull,
    InvalidColumnScan,
    ColumnStorageMissing,
    EngineComponentAlreadyRegistered,
    EngineComponentMissingDependency,
}

/// Identifies which persisted CoW file surfaced a corruption failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileKind {
    TableFile,
    CatalogMultiTableFile,
}

impl fmt::Display for FileKind {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            FileKind::TableFile => "table-file",
            FileKind::CatalogMultiTableFile => "catalog.mtb",
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
    RollbackAccess,
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
            StoragePoisonSource::RollbackAccess => "rollback access",
        })
    }
}

/// Classifies which storage setup/file operation surfaced one OS-backed IO failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageOp {
    BackendSetup,
    FileCreate,
    FileOpen,
    FileResize,
    FileStat,
}

impl fmt::Display for StorageOp {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            StorageOp::BackendSetup => "backend setup",
            StorageOp::FileCreate => "file create",
            StorageOp::FileOpen => "file open",
            StorageOp::FileResize => "file resize",
            StorageOp::FileStat => "file stat",
        })
    }
}

/// Printable secondary-index binding mismatch context.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SecondaryIndexBinding {
    /// Expected secondary-index physical kind.
    pub expected: &'static str,
    /// Actual secondary-index physical kind.
    pub actual: &'static str,
}

impl fmt::Display for SecondaryIndexBinding {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "expected {}, found {}", self.expected, self.actual)
    }
}

/// Printable recovery duplicate-key context.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecoveryDuplicateKey {
    /// Table index number being rebuilt.
    pub index_no: usize,
    /// Duplicate row id reported by the index insert.
    pub row_id: RowID,
    /// Whether the duplicate row id was already marked deleted.
    pub deleted: bool,
}

impl fmt::Display for RecoveryDuplicateKey {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "index_no={}, row_id={}, deleted={}",
            self.index_no, self.row_id, self.deleted
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct IoFailure {
    pub(crate) kind: std::io::ErrorKind,
    pub(crate) raw_os_error: Option<i32>,
}

impl fmt::Display for IoFailure {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "kind={:?}, raw_os_error={:?}",
            self.kind, self.raw_os_error
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StorageIoFailure {
    pub(crate) op: StorageOp,
    pub(crate) failure: IoFailure,
}

impl fmt::Display for StorageIoFailure {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "op={}, {}", self.op, self.failure)
    }
}

/// Public storage error report.
pub struct Error(Report<ErrorKind>);

#[allow(dead_code)]
impl Error {
    #[inline]
    fn new(kind: ErrorKind, code: ErrorCode) -> Self {
        Error(Report::new(kind).attach_opaque(code))
    }

    #[inline]
    fn new_with_attachment<A>(kind: ErrorKind, code: ErrorCode, attachment: A) -> Self
    where
        A: fmt::Debug + fmt::Display + Send + Sync + 'static,
    {
        Error(Report::new(kind).attach_opaque(code).attach(attachment))
    }

    /// Return the boundary classification for this error.
    #[inline]
    pub fn kind(&self) -> ErrorKind {
        *self.0.current_context()
    }

    /// Return true when this error belongs to `kind`.
    #[inline]
    pub fn is_kind(&self, kind: ErrorKind) -> bool {
        self.kind() == kind
    }

    /// Return the underlying `error-stack` report.
    #[inline]
    pub fn report(&self) -> &Report<ErrorKind> {
        &self.0
    }

    /// Consume this error and return the underlying `error-stack` report.
    #[inline]
    pub fn into_report(self) -> Report<ErrorKind> {
        self.0
    }

    #[inline]
    pub(crate) fn duplicate(&self) -> Self {
        if let Some(config) = self.config_error() {
            let report = match self.text_detail() {
                Some(detail) => Report::new(config).attach(detail.to_owned()),
                None => Report::new(config),
            };
            return report.into();
        }
        if let Some(data_integrity) = self.data_integrity_error() {
            if let Some(detail) = self.text_detail() {
                return Report::new(data_integrity).attach(detail.to_owned()).into();
            }
            if let Some(duplicate) = self.downcast_ref::<RecoveryDuplicateKey>().copied() {
                return Report::new(data_integrity).attach(duplicate).into();
            }
            return Report::new(data_integrity).into();
        }
        self.0
            .downcast_ref::<ErrorCode>()
            .copied()
            .map_or_else(Self::internal, |code| self.duplicate_from_code(code))
    }

    #[inline]
    pub(crate) fn is_code(&self, code: ErrorCode) -> bool {
        self.0.downcast_ref::<ErrorCode>().copied() == Some(code)
    }

    #[inline]
    pub(crate) fn downcast_ref<T: Send + Sync + 'static>(&self) -> Option<&T> {
        self.0.downcast_ref::<T>()
    }

    #[inline]
    fn text_detail(&self) -> Option<&str> {
        self.downcast_ref::<String>().map(String::as_str)
    }

    #[inline]
    fn duplicate_from_code(&self, code: ErrorCode) -> Self {
        match code {
            ErrorCode::InvalidArgument => self
                .text_detail()
                .map_or_else(Self::invalid_argument, |message| {
                    Self::invalid_argument_message(message.to_owned())
                }),
            ErrorCode::InternalError => Self::internal(),
            ErrorCode::InvalidState => Self::invalid_state(),
            ErrorCode::WrongSecondaryIndexBinding => self
                .downcast_ref::<SecondaryIndexBinding>()
                .copied()
                .map_or_else(Self::internal, |binding| {
                    Self::wrong_secondary_index_binding(binding.expected, binding.actual)
                }),
            ErrorCode::OldTableRootAlreadyRetained => Self::old_table_root_already_retained(),
            ErrorCode::IOError => self.io_failure().map_or_else(Self::internal, |failure| {
                Self::io_error_parts(failure.kind, failure.raw_os_error)
            }),
            ErrorCode::StorageIOError => {
                self.storage_io_failure()
                    .map_or_else(Self::internal, |failure| {
                        Self::storage_io_error_parts(
                            failure.op,
                            failure.failure.kind,
                            failure.failure.raw_os_error,
                        )
                    })
            }
            ErrorCode::ShortIO => Self::short_io(),
            ErrorCode::StorageFileCapacityExceeded => Self::storage_file_capacity_exceeded(),
            ErrorCode::DataTypeNotSupported => Self::data_type_not_supported(),
            ErrorCode::IndexOutOfBound => Self::index_out_of_bound(),
            ErrorCode::InvalidCodecForSel => Self::invalid_codec_for_sel(),
            ErrorCode::ValueCountMismatch => Self::value_count_mismatch(),
            ErrorCode::InvalidDatatype => Self::invalid_datatype(),
            ErrorCode::InsufficientMemory => self
                .downcast_ref::<usize>()
                .copied()
                .map_or_else(Self::internal, Self::insufficient_memory),
            ErrorCode::BufferPageAlreadyAllocated => Self::buffer_page_already_allocated(),
            ErrorCode::BufferPoolFull => Self::buffer_pool_full(),
            ErrorCode::EmptyFreeListOfBufferPool => Self::empty_free_list_of_buffer_pool(),
            ErrorCode::BufferPoolSizeTooSmall => Self::buffer_pool_size_too_small(),
            ErrorCode::RowNotFound => Self::row_not_found(),
            ErrorCode::InsufficientFreeSpaceForInplaceUpdate => {
                Self::insufficient_free_space_for_inplace_update()
            }
            ErrorCode::StorageEngineShutdown => Self::storage_engine_shutdown(),
            ErrorCode::StorageEngineShutdownBusy => self
                .downcast_ref::<usize>()
                .copied()
                .map_or_else(Self::internal, Self::storage_engine_shutdown_busy),
            ErrorCode::StorageEnginePoisoned => self
                .storage_poison_source()
                .map_or_else(Self::internal, Self::storage_engine_poisoned),
            ErrorCode::TableNotFound => Self::table_not_found(),
            ErrorCode::TableNotDeleted => Self::table_not_deleted(),
            ErrorCode::TableAlreadyExists => Self::table_already_exists(),
            ErrorCode::GlobError => Self::glob_error(),
            ErrorCode::SendError => Self::send_error(),
            ErrorCode::NotSupported => self
                .downcast_ref::<&'static str>()
                .copied()
                .map_or_else(|| Self::not_supported("<unknown>"), Self::not_supported),
            ErrorCode::ColumnNeverNull => Self::column_never_null(),
            ErrorCode::InvalidColumnScan => Self::invalid_column_scan(),
            ErrorCode::ColumnStorageMissing => Self::column_storage_missing(),
            ErrorCode::EngineComponentAlreadyRegistered => {
                Self::engine_component_already_registered()
            }
            ErrorCode::EngineComponentMissingDependency => {
                Self::engine_component_missing_dependency()
            }
        }
    }

    #[inline]
    pub(crate) fn config_error(&self) -> Option<ConfigError> {
        self.downcast_ref::<ConfigError>().copied()
    }

    #[inline]
    pub(crate) fn data_integrity_error(&self) -> Option<DataIntegrityError> {
        self.downcast_ref::<DataIntegrityError>().copied()
    }

    #[inline]
    pub(crate) fn invalid_argument() -> Self {
        Self::new(ErrorKind::InvalidInput, ErrorCode::InvalidArgument)
    }

    #[inline]
    pub(crate) fn invalid_argument_message(message: impl Into<String>) -> Self {
        Self::new_with_attachment(
            ErrorKind::InvalidInput,
            ErrorCode::InvalidArgument,
            message.into(),
        )
    }

    #[inline]
    pub(crate) fn internal() -> Self {
        Self::new(ErrorKind::Internal, ErrorCode::InternalError)
    }

    #[inline]
    pub(crate) fn invalid_state() -> Self {
        Self::new(ErrorKind::Internal, ErrorCode::InvalidState)
    }

    #[inline]
    pub(crate) fn wrong_secondary_index_binding(
        expected: &'static str,
        actual: &'static str,
    ) -> Self {
        Self::new_with_attachment(
            ErrorKind::Internal,
            ErrorCode::WrongSecondaryIndexBinding,
            SecondaryIndexBinding { expected, actual },
        )
    }

    #[inline]
    pub(crate) fn old_table_root_already_retained() -> Self {
        Self::new(ErrorKind::Operation, ErrorCode::OldTableRootAlreadyRetained)
    }

    #[inline]
    pub(crate) fn io_error(err: std::io::Error) -> Self {
        Self::io_error_parts(err.kind(), err.raw_os_error())
    }

    #[inline]
    pub(crate) fn io_error_parts(kind: std::io::ErrorKind, raw_os_error: Option<i32>) -> Self {
        Self::new_with_attachment(
            ErrorKind::Io,
            ErrorCode::IOError,
            IoFailure { kind, raw_os_error },
        )
    }

    #[inline]
    pub(crate) fn storage_io_error(op: StorageOp, err: std::io::Error) -> Self {
        Self::storage_io_error_parts(op, err.kind(), err.raw_os_error())
    }

    #[inline]
    pub(crate) fn storage_io_error_parts(
        op: StorageOp,
        kind: std::io::ErrorKind,
        raw_os_error: Option<i32>,
    ) -> Self {
        Self::new_with_attachment(
            ErrorKind::Io,
            ErrorCode::StorageIOError,
            StorageIoFailure {
                op,
                failure: IoFailure { kind, raw_os_error },
            },
        )
    }

    #[inline]
    pub(crate) fn short_io() -> Self {
        Self::new(ErrorKind::Io, ErrorCode::ShortIO)
    }

    #[inline]
    pub(crate) fn storage_file_capacity_exceeded() -> Self {
        Self::new(ErrorKind::Resource, ErrorCode::StorageFileCapacityExceeded)
    }

    #[inline]
    pub(crate) fn data_type_not_supported() -> Self {
        Self::new(ErrorKind::Operation, ErrorCode::DataTypeNotSupported)
    }

    #[inline]
    pub(crate) fn index_out_of_bound() -> Self {
        Self::new(ErrorKind::InvalidInput, ErrorCode::IndexOutOfBound)
    }

    #[inline]
    pub(crate) fn invalid_codec_for_sel() -> Self {
        Self::new(ErrorKind::InvalidInput, ErrorCode::InvalidCodecForSel)
    }

    #[inline]
    pub(crate) fn value_count_mismatch() -> Self {
        Self::new(ErrorKind::InvalidInput, ErrorCode::ValueCountMismatch)
    }

    #[inline]
    pub(crate) fn invalid_datatype() -> Self {
        Self::new(ErrorKind::InvalidInput, ErrorCode::InvalidDatatype)
    }

    #[inline]
    pub(crate) fn insufficient_memory(total_bytes: usize) -> Self {
        Self::new_with_attachment(
            ErrorKind::Resource,
            ErrorCode::InsufficientMemory,
            total_bytes,
        )
    }

    #[inline]
    pub(crate) fn buffer_page_already_allocated() -> Self {
        Self::new(ErrorKind::Internal, ErrorCode::BufferPageAlreadyAllocated)
    }

    #[inline]
    pub(crate) fn buffer_pool_full() -> Self {
        Self::new(ErrorKind::Resource, ErrorCode::BufferPoolFull)
    }

    #[inline]
    pub(crate) fn empty_free_list_of_buffer_pool() -> Self {
        Self::new(ErrorKind::Resource, ErrorCode::EmptyFreeListOfBufferPool)
    }

    #[inline]
    pub(crate) fn buffer_pool_size_too_small() -> Self {
        Self::new(ErrorKind::Resource, ErrorCode::BufferPoolSizeTooSmall)
    }

    #[inline]
    pub(crate) fn row_not_found() -> Self {
        Self::new(ErrorKind::Operation, ErrorCode::RowNotFound)
    }

    #[inline]
    pub(crate) fn insufficient_free_space_for_inplace_update() -> Self {
        Self::new(
            ErrorKind::Operation,
            ErrorCode::InsufficientFreeSpaceForInplaceUpdate,
        )
    }

    #[inline]
    pub(crate) fn storage_engine_shutdown() -> Self {
        Self::new(ErrorKind::Lifecycle, ErrorCode::StorageEngineShutdown)
    }

    #[inline]
    pub(crate) fn storage_engine_shutdown_busy(extra_refs: usize) -> Self {
        Self::new_with_attachment(
            ErrorKind::Lifecycle,
            ErrorCode::StorageEngineShutdownBusy,
            extra_refs,
        )
    }

    #[inline]
    pub(crate) fn storage_engine_poisoned(source: StoragePoisonSource) -> Self {
        Self::new_with_attachment(ErrorKind::Fatal, ErrorCode::StorageEnginePoisoned, source)
    }

    #[inline]
    pub(crate) fn storage_poison_source(&self) -> Option<StoragePoisonSource> {
        self.downcast_ref::<StoragePoisonSource>().copied()
    }

    #[inline]
    pub(crate) fn is_storage_poisoned_by(&self, source: StoragePoisonSource) -> bool {
        self.storage_poison_source() == Some(source)
    }

    #[inline]
    pub(crate) fn table_not_found() -> Self {
        Self::new(ErrorKind::Operation, ErrorCode::TableNotFound)
    }

    #[inline]
    pub(crate) fn table_not_deleted() -> Self {
        Self::new(ErrorKind::Operation, ErrorCode::TableNotDeleted)
    }

    #[inline]
    pub(crate) fn table_already_exists() -> Self {
        Self::new(ErrorKind::Operation, ErrorCode::TableAlreadyExists)
    }

    #[inline]
    pub(crate) fn glob_error() -> Self {
        Self::new(ErrorKind::Io, ErrorCode::GlobError)
    }

    #[inline]
    pub(crate) fn send_error() -> Self {
        Self::new(ErrorKind::Io, ErrorCode::SendError)
    }

    #[inline]
    pub(crate) fn not_supported(feature: &'static str) -> Self {
        Self::new_with_attachment(ErrorKind::Operation, ErrorCode::NotSupported, feature)
    }

    #[inline]
    pub(crate) fn column_never_null() -> Self {
        Self::new(ErrorKind::InvalidInput, ErrorCode::ColumnNeverNull)
    }

    #[inline]
    pub(crate) fn invalid_column_scan() -> Self {
        Self::new(ErrorKind::InvalidInput, ErrorCode::InvalidColumnScan)
    }

    #[inline]
    pub(crate) fn column_storage_missing() -> Self {
        Self::new(ErrorKind::Internal, ErrorCode::ColumnStorageMissing)
    }

    #[inline]
    pub(crate) fn engine_component_already_registered() -> Self {
        Self::new(
            ErrorKind::Internal,
            ErrorCode::EngineComponentAlreadyRegistered,
        )
    }

    #[inline]
    pub(crate) fn engine_component_missing_dependency() -> Self {
        Self::new(
            ErrorKind::Internal,
            ErrorCode::EngineComponentMissingDependency,
        )
    }

    #[inline]
    pub(crate) fn is_storage_file_capacity_exceeded(&self) -> bool {
        self.is_code(ErrorCode::StorageFileCapacityExceeded)
    }

    #[inline]
    pub(crate) fn is_storage_io_failure(&self) -> bool {
        self.is_code(ErrorCode::IOError)
            || self.is_code(ErrorCode::StorageIOError)
            || self.is_code(ErrorCode::ShortIO)
    }

    #[inline]
    pub(crate) fn io_failure(&self) -> Option<IoFailure> {
        self.downcast_ref::<IoFailure>().copied()
    }

    #[inline]
    pub(crate) fn is_io_error_kind(&self, kind: std::io::ErrorKind) -> bool {
        self.io_failure()
            .is_some_and(|failure| failure.kind == kind)
    }

    #[inline]
    pub(crate) fn storage_io_failure(&self) -> Option<StorageIoFailure> {
        self.downcast_ref::<StorageIoFailure>().copied()
    }

    #[inline]
    pub(crate) fn is_storage_io_error(&self, op: StorageOp, kind: std::io::ErrorKind) -> bool {
        self.downcast_ref::<StorageIoFailure>()
            .is_some_and(|failure| failure.op == op && failure.failure.kind == kind)
    }

    #[inline]
    pub(crate) fn is_send_error(&self) -> bool {
        self.is_code(ErrorCode::SendError)
    }

    #[inline]
    pub(crate) fn is_not_supported(&self, feature: &'static str) -> bool {
        self.is_code(ErrorCode::NotSupported)
            && self.downcast_ref::<&'static str>().copied() == Some(feature)
    }
}

impl From<Report<ConfigError>> for Error {
    #[inline]
    fn from(report: Report<ConfigError>) -> Self {
        Error(report.change_context(ErrorKind::Config))
    }
}

impl From<Report<DataIntegrityError>> for Error {
    #[inline]
    fn from(report: Report<DataIntegrityError>) -> Self {
        Error(report.change_context(ErrorKind::DataIntegrity))
    }
}

impl From<TryFromSliceError> for Error {
    #[inline]
    fn from(_src: TryFromSliceError) -> Error {
        Report::new(DataIntegrityError::InvalidPayload).into()
    }
}

impl From<std::io::Error> for Error {
    #[inline]
    fn from(src: std::io::Error) -> Self {
        match (src.kind(), src.raw_os_error()) {
            (std::io::ErrorKind::UnexpectedEof, None) => Error::short_io(),
            _ => Error::io_error(src),
        }
    }
}

impl From<std::str::Utf8Error> for Error {
    #[inline]
    fn from(_src: std::str::Utf8Error) -> Error {
        Report::new(DataIntegrityError::InvalidPayload).into()
    }
}

impl From<std::num::ParseIntError> for Error {
    #[inline]
    fn from(_src: std::num::ParseIntError) -> Error {
        Report::new(DataIntegrityError::InvalidPayload).into()
    }
}

impl From<glob::GlobError> for Error {
    #[inline]
    fn from(_src: glob::GlobError) -> Self {
        Error::glob_error()
    }
}

impl fmt::Display for Error {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl fmt::Debug for Error {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.0, f)
    }
}

impl std::error::Error for Error {}

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
