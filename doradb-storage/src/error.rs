use crate::id::{BlockID, RowID};
use crate::io::{
    IOBackendFailure, IOBackendOperationKind, backend_failure, backend_operation_kind,
};
use error_stack::{AttachmentKind, FrameKind, Report};
use std::array::TryFromSliceError;
use std::error::Error as StdError;
use std::fmt::{self, Debug, Display};
use std::io::{self, ErrorKind as IoErrorKind};
use std::num::ParseIntError;
use std::ops::ControlFlow;
use std::result;
use std::str::Utf8Error;
use thiserror::Error as ThisError;

/// Storage result using the public storage error wrapper.
pub type Result<T> = result::Result<T, Error>;
/// Result carrying configuration-domain reports.
pub(crate) type ConfigResult<T> = result::Result<T, Report<ConfigError>>;
/// Result carrying operation-domain reports.
pub(crate) type OperationResult<T> = result::Result<T, Report<OperationError>>;

/// Fluent conversion from a data-integrity report to the storage error boundary.
pub(crate) trait DataIntegrityResultExt<T> {
    /// Attaches persisted-block identity and converts into the storage result type.
    fn with_block_context(
        self,
        file_kind: FileKind,
        block_kind: &str,
        block_id: BlockID,
    ) -> Result<T>;
}

/// Result carrying data-integrity-domain reports.
pub(crate) type DataIntegrityResult<T> = result::Result<T, Report<DataIntegrityError>>;

impl<T> DataIntegrityResultExt<T> for DataIntegrityResult<T> {
    #[inline]
    fn with_block_context(
        self,
        file_kind: FileKind,
        block_kind: &str,
        block_id: BlockID,
    ) -> Result<T> {
        self.map_err(|report| {
            Error::from(report.attach(format!(
                "file={file_kind}, block={block_kind}, block_id={block_id}"
            )))
        })
    }
}

/// Result carrying lifecycle-domain reports.
pub(crate) type LifecycleResult<T> = result::Result<T, Report<LifecycleError>>;
/// Result carrying fatal-domain reports.
pub(crate) type FatalResult<T> = result::Result<T, Report<FatalError>>;
/// Result carrying completion transport reports.
pub(crate) type CompletionResult<T> = result::Result<T, Report<CompletionErrorKind>>;

/// Public storage error boundary classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ThisError)]
pub enum ErrorKind {
    /// Invalid static or startup configuration.
    #[error("configuration error")]
    Config,
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
    #[error("invalid io depth")]
    InvalidIoDepth,
    #[error("invalid purge thread count")]
    InvalidPurgeThreads,
    #[error("invalid transaction GC bucket count")]
    InvalidGcBuckets,
    #[error("invalid log block size")]
    InvalidLogBlockSize,
    #[error("invalid log file max size")]
    InvalidLogFileMaxSize,
    #[error("invalid log sync")]
    InvalidLogSync,
    #[error("invalid latch fallback mode")]
    InvalidLatchFallbackMode,
    #[error("path must not contain NUL")]
    PathMustNotContainNul,
    #[error("invalid B-tree compact ratio")]
    InvalidBTreeCompactRatio,
    #[error("invalid index spec")]
    InvalidIndexSpec,
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
    #[error("redo log sequence gap")]
    RedoLogSequenceGap,
    #[error("invalid root invariant")]
    InvalidRootInvariant,
    #[error("log file corrupted")]
    LogFileCorrupted,
    #[error("unexpected recovery duplicate key")]
    UnexpectedRecoveryDuplicateKey,
}

/// Fieldless lifecycle-domain errors carried underneath `ErrorKind::Lifecycle`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ThisError)]
pub(crate) enum LifecycleError {
    #[error("storage engine is shut down")]
    Shutdown,
    #[error("storage engine shutdown is busy")]
    ShutdownBusy,
}

/// Fieldless resource-domain errors carried underneath `ErrorKind::Resource`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ThisError)]
pub(crate) enum ResourceError {
    #[error("storage file capacity exceeded")]
    StorageFileCapacityExceeded,
    #[error("insufficient memory")]
    InsufficientMemory,
    #[error("buffer pool full")]
    BufferPoolFull,
    #[error("buffer pool size too small")]
    BufferPoolSizeTooSmall,
}

/// Fieldless operation-domain errors carried underneath `ErrorKind::Operation`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ThisError)]
pub(crate) enum OperationError {
    #[error("table not found")]
    TableNotFound,
    #[error("table is dropping")]
    TableDropping,
    #[error("table already exists")]
    TableAlreadyExists,
    #[error("index not found")]
    IndexNotFound,
    #[error("index mutation failed")]
    IndexMutation,
    #[error("existing transaction")]
    ExistingTransaction,
    #[error("not supported")]
    NotSupported,
    #[error("duplicate key")]
    DuplicateKey,
    #[error("write conflict")]
    WriteConflict,
    #[error("invalid DML input")]
    InvalidDmlInput,
    #[error("invalid lock mode")]
    InvalidLockMode,
    #[error("lock upgrade would block")]
    LockUpgradeWouldBlock,
    #[error("lock conversion is not supported")]
    LockConversionNotSupported,
    #[error("lock owner group conflict")]
    LockOwnerGroupConflict,
    #[error("lock waiter released")]
    LockWaiterReleased,
}

/// Fieldless fatal-domain errors carried underneath `ErrorKind::Fatal`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ThisError)]
pub(crate) enum FatalError {
    #[error("storage engine poisoned")]
    Poisoned,
    #[error("redo write failed")]
    RedoWrite,
    #[error("redo sync failed")]
    RedoSync,
    #[error("storage io failed")]
    StorageIo,
    #[error("checkpoint write failed")]
    CheckpointWrite,
    #[error("catalog write failed")]
    CatalogWrite,
    #[error("purge deallocate failed")]
    PurgeDeallocate,
    #[error("purge access failed")]
    PurgeAccess,
    #[error("rollback access failed")]
    RollbackAccess,
}

/// Fieldless internal-domain errors carried underneath `ErrorKind::Internal`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ThisError)]
pub(crate) enum InternalError {
    #[error("internal storage error")]
    Generic,
    #[error("component shelf duplicate provision")]
    ComponentShelfDuplicateProvision,
    #[error("component shelf not empty")]
    ComponentShelfNotEmpty,
    #[error("component registry missing")]
    ComponentRegistryMissing,
    #[error("component provision missing")]
    ComponentProvisionMissing,
    #[error("engine component already registered")]
    EngineComponentAlreadyRegistered,
    #[error("engine component missing dependency")]
    EngineComponentMissingDependency,
    #[error("secondary index binding mismatch")]
    SecondaryIndexBindingMismatch,
    #[error("buffer page already allocated")]
    BufferPageAlreadyAllocated,
    #[error("buffer page kind mismatch")]
    BufferPageKindMismatch,
    #[error("column storage missing")]
    ColumnStorageMissing,
    #[error("mutable block view mismatch")]
    MutableBlockViewMismatch,
    #[error("completion dropped")]
    CompletionDropped,
    #[error("readonly buffer mapping conflict")]
    ReadonlyMappingConflict,
    #[error("readonly write barrier encountered an in-flight load")]
    ReadonlyWriteInflight,
    #[error("readonly block is blocked by a write barrier")]
    ReadonlyWriteBlocked,
    #[error("readonly frame lock missing")]
    ReadonlyFrameLockMissing,
    #[error("row page missing")]
    RowPageMissing,
    #[error("full-table update row-page identity mismatch")]
    FullTableUpdatePageIdentityMismatch,
    #[error("full-table update encountered transition page")]
    FullTableUpdateTransitionPage,
    #[error("pool guard missing")]
    PoolGuardMissing,
    #[error("disk pool guard missing")]
    DiskPoolGuardMissing,
    #[error("block-index leaf stale")]
    BlockIndexLeafStale,
    #[error("indexed value missing")]
    IndexedValueMissing,
    #[error("index key missing")]
    IndexKeyMissing,
    #[error("lwc builder misuse")]
    LwcBuilderMisuse,
    #[error("mem index key malformed")]
    MemIndexKeyMalformed,
    #[error("column deletion blob writer state mismatch")]
    ColumnDeletionBlobWriterStateMismatch,
    #[error("cow file allocation invariant violated")]
    CowFileAllocationInvariant,
    #[error("column index rewrite did not touch target")]
    ColumnIndexRewriteMiss,
    #[error("column index path invariant violated")]
    ColumnIndexPathInvariant,
    #[error("column index search type missing")]
    ColumnIndexSearchTypeMissing,
    #[error("column index out of bounds")]
    ColumnIndexOutOfBounds,
    #[error("secondary index out of bounds")]
    SecondaryIndexOutOfBounds,
    #[error("secondary index kind mismatch")]
    SecondaryIndexKindMismatch,
    #[error("secondary index root count mismatch")]
    SecondaryIndexRootCountMismatch,
    #[error("mutable root metadata regression")]
    MutableRootMetadataRegression,
    #[error("catalog root descriptor invariant violated")]
    CatalogRootDescriptorInvariant,
    #[error("catalog primary key missing")]
    CatalogPrimaryKeyMissing,
    #[error("column scan shape mismatch")]
    ColumnScanShapeMismatch,
    #[error("LWC block encoding invariant violated")]
    LwcBlockEncodingInvariant,
    #[error("readonly frame index out of bounds")]
    ReadonlyFrameIndexOutOfBounds,
    #[error("readonly frame guard mismatch")]
    ReadonlyFrameGuardMismatch,
    #[error("B-tree pack invariant violated")]
    BTreePackInvariant,
    #[error("DiskTree rewrite invariant violated")]
    DiskTreeRewriteInvariant,
    #[error("DiskTree batch order invariant violated")]
    DiskTreeBatchOrderInvariant,
    #[error("column block index invariant violated")]
    ColumnBlockIndexInvariant,
    #[error("column deletion blob invariant violated")]
    ColumnDeletionBlobInvariant,
    #[error("active transaction is discarded")]
    ActiveTransactionDiscarded,
    #[error("system transaction redo missing")]
    SystemTransactionRedoMissing,
    #[error("statement number overflow")]
    StatementNumberOverflow,
    #[cfg(test)]
    #[error("injected test failure")]
    InjectedTestFailure,
}

/// IO-domain errors carried underneath `ErrorKind::Io`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ThisError)]
#[error("{0}")]
pub(crate) struct IoError(IoErrorKind);

impl IoError {
    /// Returns the underlying IO error kind.
    #[inline]
    pub(crate) fn kind(self) -> IoErrorKind {
        self.0
    }

    /// Builds an IO-domain report from a standard IO error.
    #[inline]
    pub(crate) fn report(err: io::Error) -> Report<Self> {
        Report::new(Self::from(err.kind())).attach(format!("{}", err))
    }

    /// Builds an IO-domain report annotated with the storage operation.
    #[inline]
    pub(crate) fn report_with_op(op: StorageOp, err: io::Error) -> Report<Self> {
        Report::new(Self::from(err.kind())).attach(format!("op={op}, {err}"))
    }

    /// Builds an IO-domain report from a backend progress error.
    #[inline]
    pub(crate) fn report_backend(
        err: &Report<IoError>,
        message: impl Into<String>,
    ) -> Report<Self> {
        copy_backend_io_report(err).attach(message.into())
    }

    /// Builds an unexpected-EOF report with byte counts.
    #[inline]
    pub(crate) fn report_unexpected_eof(
        actual_bytes: usize,
        expected_bytes: usize,
    ) -> Report<Self> {
        Report::new(Self::from(IoErrorKind::UnexpectedEof)).attach(format!(
            "unexpected eof: actual_bytes={actual_bytes}, expected_bytes={expected_bytes}"
        ))
    }

    /// Builds a send-failure report with additional context.
    #[inline]
    pub(crate) fn report_send(message: impl Into<String>) -> Report<Self> {
        Report::new(Self::from(IoErrorKind::BrokenPipe)).attach(message.into())
    }
}

impl From<IoErrorKind> for IoError {
    #[inline]
    fn from(kind: IoErrorKind) -> Self {
        IoError(kind)
    }
}

/// Cross-thread completion transport errors preserving their exact cause.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ThisError)]
pub(crate) enum CompletionErrorKind {
    #[error("io error: {0:?}")]
    Io(IoErrorKind),
    #[error("send error")]
    Send,
    #[error("configuration error: {0}")]
    Config(ConfigError),
    #[error("operation error: {0}")]
    Operation(OperationError),
    #[error("resource exhausted: {0}")]
    Resource(ResourceError),
    #[error("data integrity error: {0}")]
    DataIntegrity(DataIntegrityError),
    #[error("storage lifecycle error: {0}")]
    Lifecycle(LifecycleError),
    #[error("fatal storage error: {0}")]
    Fatal(FatalError),
    #[error("internal completion error: {0}")]
    Internal(InternalError),
}

impl CompletionErrorKind {
    /// Builds a completion report from a standard IO error.
    #[inline]
    pub(crate) fn report_io(err: io::Error, message: impl Into<String>) -> Report<Self> {
        let kind = err.kind();
        Report::new(IoError::from(kind))
            .change_context(Self::Io(kind))
            .attach(format!("{err}"))
            .attach(message.into())
    }

    /// Builds a completion report from a backend progress error.
    #[inline]
    pub(crate) fn report_backend_io(
        err: &Report<IoError>,
        message: impl Into<String>,
    ) -> Report<Self> {
        let kind = err.current_context().kind();
        copy_backend_io_report(err)
            .change_context(Self::Io(kind))
            .attach(message.into())
    }

    /// Builds a completion report for a short read.
    #[inline]
    pub(crate) fn report_unexpected_eof(
        actual_bytes: usize,
        expected_bytes: usize,
        message: impl Into<String>,
    ) -> Report<Self> {
        Report::new(IoError::from(IoErrorKind::UnexpectedEof))
            .change_context(Self::Io(IoErrorKind::UnexpectedEof))
            .attach(format!(
                "unexpected eof: actual_bytes={actual_bytes}, expected_bytes={expected_bytes}"
            ))
            .attach(message.into())
    }

    /// Builds a completion report for an unexpected successful IO result.
    #[inline]
    pub(crate) fn report_unexpected_result(
        actual_result: usize,
        expected_result: usize,
        message: impl Into<String>,
    ) -> Report<Self> {
        Report::new(IoError::from(IoErrorKind::Other))
            .change_context(Self::Io(IoErrorKind::Other))
            .attach(format!(
                "unexpected io completion result: actual_result={actual_result}, expected_result={expected_result}"
            ))
            .attach(message.into())
    }

    /// Builds a completion report for a send failure.
    #[inline]
    pub(crate) fn report_send(message: impl Into<String>) -> Report<Self> {
        Report::new(IoError::from(IoErrorKind::BrokenPipe))
            .change_context(Self::Send)
            .attach(message.into())
    }

    /// Builds a completion report for a fatal storage error.
    #[inline]
    pub(crate) fn report_fatal(reason: FatalError, message: impl Into<String>) -> Report<Self> {
        Report::new(reason)
            .change_context(Self::Fatal(reason))
            .attach(message.into())
    }

    /// Builds a completion report for an internal storage error.
    #[inline]
    pub(crate) fn report_internal(
        reason: InternalError,
        message: impl Into<String>,
    ) -> Report<Self> {
        Report::new(reason)
            .change_context(Self::Internal(reason))
            .attach(message.into())
    }

    /// Converts a storage error into a completion-domain report.
    #[inline]
    pub(crate) fn report_error(err: Error, message: impl Into<String>) -> Report<Self> {
        let kind = Self::from_error(&err);
        err.into_report()
            .change_context(kind)
            .attach(message.into())
    }

    #[inline]
    fn from_error(err: &Error) -> Self {
        if let Some(kind) = err.completion_error() {
            return kind;
        }
        if let Some(reason) = err.downcast_ref::<IoError>().copied() {
            return Self::Io(reason.kind());
        }
        if let Some(reason) = err.downcast_ref::<ConfigError>().copied() {
            return Self::Config(reason);
        }
        if let Some(reason) = err.downcast_ref::<OperationError>().copied() {
            return Self::Operation(reason);
        }
        if let Some(reason) = err.downcast_ref::<ResourceError>().copied() {
            return Self::Resource(reason);
        }
        if let Some(reason) = err.downcast_ref::<DataIntegrityError>().copied() {
            return Self::DataIntegrity(reason);
        }
        if let Some(reason) = err.downcast_ref::<LifecycleError>().copied() {
            return Self::Lifecycle(reason);
        }
        if let Some(reason) = err.downcast_ref::<FatalError>().copied() {
            return Self::Fatal(reason);
        }
        if let Some(reason) = err.downcast_ref::<InternalError>().copied() {
            return Self::Internal(reason);
        }
        Self::Internal(InternalError::Generic)
    }

    #[inline]
    fn error_kind(self) -> ErrorKind {
        match self {
            CompletionErrorKind::Io(_) | CompletionErrorKind::Send => ErrorKind::Io,
            CompletionErrorKind::Config(_) => ErrorKind::Config,
            CompletionErrorKind::Operation(_) => ErrorKind::Operation,
            CompletionErrorKind::Resource(_) => ErrorKind::Resource,
            CompletionErrorKind::DataIntegrity(_) => ErrorKind::DataIntegrity,
            CompletionErrorKind::Lifecycle(_) => ErrorKind::Lifecycle,
            CompletionErrorKind::Fatal(_) => ErrorKind::Fatal,
            CompletionErrorKind::Internal(_) => ErrorKind::Internal,
        }
    }
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BufferPageKindMismatch {
    expected: &'static str,
    actual: &'static str,
}

impl fmt::Display for BufferPageKindMismatch {
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

/// Public storage error report.
pub struct Error(Report<ErrorKind>);

impl Error {
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

    /// Returns an attached report frame of type `T`, when present.
    #[inline]
    pub(crate) fn downcast_ref<T: Send + Sync + 'static>(&self) -> Option<&T> {
        self.0.downcast_ref::<T>()
    }

    /// Returns the attached data-integrity reason, when present.
    #[inline]
    pub(crate) fn data_integrity_error(&self) -> Option<DataIntegrityError> {
        self.downcast_ref::<DataIntegrityError>().copied()
    }

    /// Returns the attached lifecycle reason, when present.
    #[inline]
    pub(crate) fn lifecycle_error(&self) -> Option<LifecycleError> {
        self.downcast_ref::<LifecycleError>().copied()
    }

    /// Returns the attached resource reason, when present.
    #[inline]
    pub(crate) fn resource_error(&self) -> Option<ResourceError> {
        self.downcast_ref::<ResourceError>().copied()
    }

    /// Returns the attached operation reason, when present.
    #[inline]
    pub(crate) fn operation_error(&self) -> Option<OperationError> {
        self.downcast_ref::<OperationError>().copied()
    }

    /// Returns the attached completion reason, when present.
    #[inline]
    pub(crate) fn completion_error(&self) -> Option<CompletionErrorKind> {
        self.downcast_ref::<CompletionErrorKind>().copied()
    }

    /// Converts a completion-domain report into the public storage error.
    #[inline]
    pub(crate) fn from_completion_report(
        report: Report<CompletionErrorKind>,
        message: impl Into<String>,
    ) -> Self {
        let kind = report.current_context().error_kind();
        Error(report.change_context(kind).attach(message.into()))
    }

    /// Builds a generic internal storage error.
    #[inline]
    pub(crate) fn internal() -> Self {
        Report::new(InternalError::Generic).into()
    }

    /// Builds an error for a secondary-index binding mismatch.
    #[inline]
    pub(crate) fn wrong_secondary_index_binding(
        expected: &'static str,
        actual: &'static str,
    ) -> Self {
        Report::new(InternalError::SecondaryIndexBindingMismatch)
            .attach(SecondaryIndexBinding { expected, actual })
            .into()
    }

    /// Builds an error for double allocation of a buffer page.
    #[inline]
    pub(crate) fn buffer_page_already_allocated() -> Self {
        Report::new(InternalError::BufferPageAlreadyAllocated).into()
    }

    /// Builds an internal error for typed buffer-page access with the wrong page kind.
    #[inline]
    pub(crate) fn buffer_page_kind_mismatch(expected: &'static str, actual: &'static str) -> Self {
        Report::new(InternalError::BufferPageKindMismatch)
            .attach(BufferPageKindMismatch { expected, actual })
            .into()
    }

    /// Builds an error for missing column storage.
    #[inline]
    pub(crate) fn column_storage_missing() -> Self {
        Report::new(InternalError::ColumnStorageMissing).into()
    }

    /// Builds an error for duplicate engine component registration.
    #[inline]
    pub(crate) fn engine_component_already_registered() -> Self {
        Report::new(InternalError::EngineComponentAlreadyRegistered).into()
    }

    /// Builds an error for a missing component dependency.
    #[inline]
    pub(crate) fn engine_component_missing_dependency() -> Self {
        Report::new(InternalError::EngineComponentMissingDependency).into()
    }

    #[inline]
    fn fmt_report_line(report: &Report<ErrorKind>, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut first = true;
        for frame in report.frames() {
            if let FrameKind::Context(context) = frame.kind() {
                if first {
                    first = false;
                } else {
                    f.write_str(": ")?;
                }
                Display::fmt(context, f)?;
            }
        }
        for frame in report.frames() {
            if let FrameKind::Attachment(AttachmentKind::Printable(attachment)) = frame.kind() {
                if first {
                    first = false;
                } else {
                    f.write_str(": ")?;
                }
                Display::fmt(attachment, f)?;
            }
        }
        Ok(())
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

impl From<Report<LifecycleError>> for Error {
    #[inline]
    fn from(report: Report<LifecycleError>) -> Self {
        Error(report.change_context(ErrorKind::Lifecycle))
    }
}

impl From<Report<FatalError>> for Error {
    #[inline]
    fn from(report: Report<FatalError>) -> Self {
        Error(report.change_context(ErrorKind::Fatal))
    }
}

impl From<Report<ResourceError>> for Error {
    #[inline]
    fn from(report: Report<ResourceError>) -> Self {
        Error(report.change_context(ErrorKind::Resource))
    }
}

impl From<Report<OperationError>> for Error {
    #[inline]
    fn from(report: Report<OperationError>) -> Self {
        Error(report.change_context(ErrorKind::Operation))
    }
}

impl From<Report<IoError>> for Error {
    #[inline]
    fn from(report: Report<IoError>) -> Self {
        Error(report.change_context(ErrorKind::Io))
    }
}

impl From<Report<InternalError>> for Error {
    #[inline]
    fn from(report: Report<InternalError>) -> Self {
        Error(report.change_context(ErrorKind::Internal))
    }
}

impl From<TryFromSliceError> for Error {
    #[inline]
    fn from(_src: TryFromSliceError) -> Error {
        Report::new(DataIntegrityError::InvalidPayload).into()
    }
}

impl From<io::Error> for Error {
    #[inline]
    fn from(src: io::Error) -> Self {
        IoError::report(src).into()
    }
}

impl From<Utf8Error> for Error {
    #[inline]
    fn from(_src: Utf8Error) -> Error {
        Report::new(DataIntegrityError::InvalidPayload).into()
    }
}

impl From<ParseIntError> for Error {
    #[inline]
    fn from(_src: ParseIntError) -> Error {
        Report::new(DataIntegrityError::InvalidPayload).into()
    }
}

impl From<glob::GlobError> for Error {
    #[inline]
    fn from(src: glob::GlobError) -> Self {
        Report::new(IoError::from(src.error().kind()))
            .attach(format!("{}", src))
            .into()
    }
}

impl fmt::Display for Error {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Self::fmt_report_line(&self.0, f)
    }
}

impl fmt::Debug for Error {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Debug::fmt(&self.0, f)
    }
}

impl StdError for Error {}

/// Validation of optimistic lock
pub enum Validation<T> {
    Valid(T),
    Invalid,
}

impl<T> Validation<T> {
    /// Converts validation into `ControlFlow`.
    #[inline]
    pub fn branch(self) -> ControlFlow<(), T> {
        match self {
            Validation::Valid(v) => ControlFlow::Continue(v),
            Validation::Invalid => ControlFlow::Break(()),
        }
    }

    /// Maps the valid value while preserving invalid state.
    #[inline]
    pub fn map<U, F: FnOnce(T) -> U>(self, f: F) -> Validation<U> {
        match self {
            Validation::Valid(v) => Validation::Valid(f(v)),
            Validation::Invalid => Validation::Invalid,
        }
    }

    /// Chains validation-producing functions.
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

    /// Returns the valid value or panics with `msg`.
    #[inline]
    pub fn expect(self, msg: &str) -> T {
        match self {
            Validation::Valid(v) => v,
            Validation::Invalid => unwrap_failed(msg),
        }
    }

    /// Returns the valid value or panics with a default message.
    #[inline]
    pub fn unwrap(self) -> T {
        match self {
            Validation::Valid(v) => v,
            Validation::Invalid => unwrap_failed_no_message(),
        }
    }

    /// Returns true when this validation is valid.
    #[inline]
    pub fn is_valid(&self) -> bool {
        matches!(self, Validation::Valid(_))
    }

    /// Returns true when this validation is invalid.
    #[inline]
    pub fn is_invalid(&self) -> bool {
        matches!(self, Validation::Invalid)
    }
}

/// Propagate a completion report while preserving structured backend context.
#[inline]
pub(crate) fn propagate_completion_report(
    report: &Report<CompletionErrorKind>,
    message: impl Into<String>,
) -> Report<CompletionErrorKind> {
    let mut propagated = match report.downcast_ref::<IoError>().copied() {
        Some(io) => Report::new(io).change_context(*report.current_context()),
        None => Report::new(*report.current_context()),
    };
    if let Some(failure) = report.downcast_ref::<IOBackendFailure>() {
        propagated = propagated.attach(failure.clone());
    }
    if let Some(operation_kind) = report.downcast_ref::<IOBackendOperationKind>() {
        propagated = propagated.attach(*operation_kind);
    }
    propagated.attach(message.into())
}

#[inline]
fn copy_backend_io_report(err: &Report<IoError>) -> Report<IoError> {
    let mut report = Report::new(*err.current_context());
    if let Some(failure) = backend_failure(err) {
        report = report.attach(failure.clone());
    }
    if let Some(operation_kind) = backend_operation_kind(err) {
        report = report.attach(operation_kind);
    }
    report
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::{
        IOBackendErrorPhase, IOBackendFailure, IOBackendOperationKind, IOBackendQueueState, IOKind,
        attach_backend_operation_kind,
    };
    use error_stack::ResultExt;
    use std::io::Error as StdIoError;

    #[test]
    fn test_io_report_with_op_attaches_formatted_context() {
        let report = IoError::report_with_op(
            StorageOp::FileOpen,
            StdIoError::new(IoErrorKind::PermissionDenied, "open denied"),
        );

        assert_eq!(
            report.current_context().kind(),
            IoErrorKind::PermissionDenied
        );
        let output = format!("{report:?}");
        assert!(output.contains("op=file open"));
        assert!(output.contains("open denied"));
    }

    #[test]
    fn test_io_report_converts_to_top_level_io() {
        let err = Error::from(IoError::report(StdIoError::new(
            IoErrorKind::WouldBlock,
            "not ready",
        )));

        assert_eq!(err.kind(), ErrorKind::Io);
        assert_eq!(
            err.report()
                .downcast_ref::<IoError>()
                .copied()
                .map(IoError::kind),
            Some(IoErrorKind::WouldBlock)
        );
        assert!(format!("{err:?}").contains("not ready"));
    }

    #[test]
    fn test_storage_error_display_includes_config_detail() {
        let err =
            Error::from(Report::new(ConfigError::InvalidIoDepth).attach("recovery_io_depth=0"));

        assert_eq!(
            format!("{err}"),
            "configuration error: invalid io depth: recovery_io_depth=0"
        );
    }

    #[test]
    fn test_storage_error_display_includes_io_detail() {
        let err = Error::from(IoError::report_with_op(
            StorageOp::FileOpen,
            StdIoError::new(IoErrorKind::PermissionDenied, "open denied"),
        ));

        let output = format!("{err}");
        assert!(output.contains("io error"), "{output}");
        assert!(output.contains("permission denied"), "{output}");
        assert!(output.contains("op=file open"), "{output}");
        assert!(output.contains("open denied"), "{output}");
    }

    #[test]
    fn test_index_mutation_context_preserves_lower_error() {
        let lower: Result<()> = Err(Error::column_storage_missing());
        let report = lower
            .change_context(OperationError::IndexMutation)
            .unwrap_err();

        assert_eq!(report.current_context(), &OperationError::IndexMutation);
        assert_eq!(
            report.downcast_ref::<Error>().map(Error::kind),
            Some(ErrorKind::Internal)
        );

        let err: Error = report.attach("secondary index claim").into();
        assert_eq!(err.kind(), ErrorKind::Operation);
        assert_eq!(err.operation_error(), Some(OperationError::IndexMutation));
        assert!(format!("{err}").contains("secondary index claim"));
    }

    #[test]
    fn test_completion_backend_report_preserves_typed_attachments() {
        let backend_report = IOBackendFailure::report(
            "test_backend",
            IOBackendErrorPhase::Wait,
            StdIoError::new(IoErrorKind::TimedOut, "wait timed out"),
            3,
            IOBackendQueueState::wait_with_completions(0),
        );
        let backend_report = attach_backend_operation_kind(backend_report, Some(IOKind::Read));
        let completion =
            CompletionErrorKind::report_backend_io(&backend_report, "completion context");

        assert_eq!(
            *completion.current_context(),
            CompletionErrorKind::Io(IoErrorKind::TimedOut)
        );
        assert!(completion.downcast_ref::<IOBackendFailure>().is_some());
        assert!(
            completion
                .downcast_ref::<IOBackendOperationKind>()
                .is_some()
        );
        let output = format!("{completion:?}");
        assert!(output.contains("backend=test_backend"), "{output}");
        assert!(output.contains("operation_kind=Read"), "{output}");
        assert!(output.contains("completion context"), "{output}");
    }
}
