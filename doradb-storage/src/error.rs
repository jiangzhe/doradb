use crate::id::RowID;
use crate::io::{
    IOBackendFailure, IOBackendOperationKind, backend_failure, backend_operation_kind,
};
use error_stack::{AttachmentKind, FrameKind, Report};
use std::array::TryFromSliceError;
use std::convert::Infallible;
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
/// Result carrying resource-domain reports.
pub(crate) type ResourceResult<T> = result::Result<T, Report<ResourceError>>;
/// Result carrying IO-domain reports.
pub(crate) type IoResult<T> = result::Result<T, Report<IoError>>;
/// Result carrying internal-invariant reports.
pub(crate) type InternalResult<T> = result::Result<T, Report<InternalError>>;

/// Result carrying data-integrity-domain reports.
pub(crate) type DataIntegrityResult<T> = result::Result<T, Report<DataIntegrityError>>;

/// Result carrying lifecycle-domain reports.
pub(crate) type LifecycleResult<T> = result::Result<T, Report<LifecycleError>>;
/// Result carrying engine-owned runtime-operation reports.
pub(crate) type RuntimeResult<T> = result::Result<T, Report<RuntimeError>>;
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
    /// A recoverable engine-owned runtime operation could not complete.
    #[error("storage runtime error")]
    Runtime,
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
    #[error("storage layout marker read failed")]
    StorageLayoutMarkerRead,
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
    #[error("invalid redo log file name")]
    InvalidRedoLogFileName,
    #[error("duplicate redo log sequence")]
    DuplicateRedoLogSequence,
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
    #[error("runtime is unavailable")]
    RuntimeUnavailable,
    #[error("storage engine is shut down")]
    Shutdown,
    #[error("storage engine shutdown is busy")]
    ShutdownBusy,
    #[error("session is unavailable")]
    SessionUnavailable,
    #[error("existing transaction")]
    ExistingTransaction,
    #[error("transaction is discarded")]
    TransactionDiscarded,
}

/// Recoverable failures of engine-owned internal operations and runtime infrastructure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ThisError)]
pub(crate) enum RuntimeError {
    /// The operating system rejected creation of a required background thread.
    #[error("background thread spawn failed")]
    BackgroundSpawn,
    /// Construction of an engine-owned buffer pool could not complete.
    #[error("buffer pool initialization failed")]
    BufferPoolInit,
    /// An engine-owned buffer pool could not allocate a page.
    #[error("buffer page allocation failed")]
    BufferPageAllocation,
    /// An engine-owned buffer pool could not access a page.
    #[error("buffer page access failed")]
    BufferPageAccess,
    /// An engine-owned file root could not be loaded or published.
    #[error("file root access failed")]
    FileRootAccess,
    /// Discovery of the configured redo log file family could not complete.
    #[error("redo log discovery failed")]
    RedoLogDiscovery,
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
    #[error("lock is unavailable")]
    LockUnavailable,
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
    #[error("secondary index binding mismatch")]
    SecondaryIndexBindingMismatch,
    #[error("buffer page already allocated")]
    BufferPageAlreadyAllocated,
    #[error("buffer page kind mismatch")]
    BufferPageKindMismatch,
    #[error("column storage missing")]
    ColumnStorageMissing,
    #[error("completion dropped")]
    CompletionDropped,
    #[error("readonly write barrier encountered an in-flight load")]
    ReadonlyWriteInflight,
    #[error("readonly block is blocked by a write barrier")]
    ReadonlyWriteBlocked,
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
    #[error("LWC block encoding contract is unsatisfied")]
    LwcBlockEncodingContract,
    #[error("readonly frame index out of bounds")]
    ReadonlyFrameIndexOutOfBounds,
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
    #[error("current redo log file is missing")]
    CurrentRedoLogFileMissing,
    #[error("redo writer format encoding failed")]
    RedoFormatEncoding,
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

    /// Builds an IO-domain report from a backend progress error.
    #[inline]
    pub(crate) fn report_backend(
        err: &Report<IoError>,
        message: impl Into<String>,
    ) -> Report<Self> {
        let mut report = Report::new(*err.current_context());
        if let Some(failure) = backend_failure(err) {
            report = report.attach(failure.clone());
        }
        if let Some(operation_kind) = backend_operation_kind(err) {
            report = report.attach(operation_kind);
        }
        report.attach(message.into())
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
    /// Temporarily adapts an owned IO report for completion transport.
    #[inline]
    pub(crate) fn from_io(report: Report<IoError>, message: impl Into<String>) -> Report<Self> {
        let kind = report.current_context().kind();
        report.change_context(Self::Io(kind)).attach(message.into())
    }

    /// Temporarily adapts an owned send-failure IO report for completion transport.
    #[inline]
    pub(crate) fn from_send(report: Report<IoError>, message: impl Into<String>) -> Report<Self> {
        report.change_context(Self::Send).attach(message.into())
    }

    /// Temporarily adapts an owned resource report for completion transport.
    #[inline]
    pub(crate) fn from_resource(
        report: Report<ResourceError>,
        message: impl Into<String>,
    ) -> Report<Self> {
        let reason = *report.current_context();
        report
            .change_context(Self::Resource(reason))
            .attach(message.into())
    }

    /// Temporarily adapts an owned data-integrity report for completion transport.
    #[inline]
    pub(crate) fn from_data_integrity(
        report: Report<DataIntegrityError>,
        message: impl Into<String>,
    ) -> Report<Self> {
        let reason = *report.current_context();
        report
            .change_context(Self::DataIntegrity(reason))
            .attach(message.into())
    }

    /// Temporarily adapts an owned lifecycle report for completion transport.
    #[inline]
    pub(crate) fn from_lifecycle(
        report: Report<LifecycleError>,
        message: impl Into<String>,
    ) -> Report<Self> {
        let reason = *report.current_context();
        report
            .change_context(Self::Lifecycle(reason))
            .attach(message.into())
    }

    /// Temporarily adapts an owned fatal report for completion transport.
    #[inline]
    pub(crate) fn from_fatal(
        report: Report<FatalError>,
        message: impl Into<String>,
    ) -> Report<Self> {
        let reason = *report.current_context();
        report
            .change_context(Self::Fatal(reason))
            .attach(message.into())
    }

    /// Temporarily adapts an owned internal report for completion transport.
    #[inline]
    pub(crate) fn from_internal(
        report: Report<InternalError>,
        message: impl Into<String>,
    ) -> Report<Self> {
        let reason = *report.current_context();
        report
            .change_context(Self::Internal(reason))
            .attach(message.into())
    }

    #[inline]
    fn error_kind(self) -> ErrorKind {
        match self {
            CompletionErrorKind::Io(_) | CompletionErrorKind::Send => ErrorKind::Io,
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
            FileKind::TableFile => "table_file",
            FileKind::CatalogMultiTableFile => "catalog.mtb",
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

    /// Adds boundary context without changing the existing error classification.
    #[inline]
    pub(crate) fn attach(self, attachment: impl Into<String>) -> Self {
        Error(self.0.attach(attachment.into()))
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

    /// Builds an error for missing column storage.
    #[inline]
    pub(crate) fn column_storage_missing() -> Self {
        Report::new(InternalError::ColumnStorageMissing).into()
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
        // This structural public classification adds no caller-owned diagnostic.
        Error(report.change_context(ErrorKind::Config))
    }
}

impl From<Report<DataIntegrityError>> for Error {
    #[inline]
    fn from(report: Report<DataIntegrityError>) -> Self {
        // This structural public classification adds no caller-owned diagnostic.
        Error(report.change_context(ErrorKind::DataIntegrity))
    }
}

impl From<Report<LifecycleError>> for Error {
    #[inline]
    fn from(report: Report<LifecycleError>) -> Self {
        // This structural public classification adds no caller-owned diagnostic.
        Error(report.change_context(ErrorKind::Lifecycle))
    }
}

impl From<Report<FatalError>> for Error {
    #[inline]
    fn from(report: Report<FatalError>) -> Self {
        // This structural public classification adds no caller-owned diagnostic.
        Error(report.change_context(ErrorKind::Fatal))
    }
}

impl From<Report<ResourceError>> for Error {
    #[inline]
    fn from(report: Report<ResourceError>) -> Self {
        // This structural public classification adds no caller-owned diagnostic.
        Error(report.change_context(ErrorKind::Resource))
    }
}

impl From<Report<OperationError>> for Error {
    #[inline]
    fn from(report: Report<OperationError>) -> Self {
        // This structural public classification adds no caller-owned diagnostic.
        Error(report.change_context(ErrorKind::Operation))
    }
}

impl From<Report<IoError>> for Error {
    #[inline]
    fn from(report: Report<IoError>) -> Self {
        // This structural public classification adds no caller-owned diagnostic.
        Error(report.change_context(ErrorKind::Io))
    }
}

impl From<Report<RuntimeError>> for Error {
    #[inline]
    fn from(report: Report<RuntimeError>) -> Self {
        // This structural public classification adds no caller-owned diagnostic.
        Error(report.change_context(ErrorKind::Runtime))
    }
}

impl From<Report<InternalError>> for Error {
    #[inline]
    fn from(report: Report<InternalError>) -> Self {
        // This structural public classification adds no caller-owned diagnostic.
        Error(report.change_context(ErrorKind::Internal))
    }
}

impl From<Infallible> for Error {
    #[inline]
    fn from(src: Infallible) -> Self {
        match src {}
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
        let kind = src.kind();
        Report::new(IoError::from(kind)).attach(src).into()
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
    fn test_io_report_with_caller_attachment_preserves_detail() {
        let source = StdIoError::new(IoErrorKind::PermissionDenied, "open denied");
        let report =
            Report::new(IoError::from(source.kind())).attach(format!("op=file_open, {source}"));

        assert_eq!(
            report.current_context().kind(),
            IoErrorKind::PermissionDenied
        );
        let output = format!("{report:?}");
        assert!(output.contains("op=file_open"));
        assert!(output.contains("open denied"));
    }

    #[test]
    fn test_io_report_converts_to_top_level_io() {
        let source = StdIoError::new(IoErrorKind::WouldBlock, "not ready");
        let err =
            Error::from(Report::new(IoError::from(source.kind())).attach(format!("{source}")));

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
    fn test_std_io_error_conversion_preserves_owned_source() {
        let err = Error::from(StdIoError::from_raw_os_error(libc::EIO));

        assert_eq!(err.kind(), ErrorKind::Io);
        assert_eq!(
            err.report()
                .downcast_ref::<StdIoError>()
                .and_then(StdIoError::raw_os_error),
            Some(libc::EIO)
        );
    }

    #[test]
    fn test_runtime_report_converts_losslessly_to_public_runtime() {
        let source = StdIoError::other("spawn unavailable");
        let report = Report::new(IoError::from(source.kind()))
            .attach(format!("{source}"))
            .change_context(RuntimeError::BackgroundSpawn)
            .attach("thread_name=Runtime-Conversion-Test");

        let err = Error::from(report);

        assert_eq!(err.kind(), ErrorKind::Runtime);
        assert_eq!(
            err.report().downcast_ref::<RuntimeError>().copied(),
            Some(RuntimeError::BackgroundSpawn)
        );
        assert_eq!(
            err.report()
                .downcast_ref::<IoError>()
                .copied()
                .map(IoError::kind),
            Some(IoErrorKind::Other)
        );
        assert!(format!("{err:?}").contains("thread_name=Runtime-Conversion-Test"));
    }

    #[test]
    fn test_buffer_pool_init_report_converts_losslessly_to_public_runtime() {
        let report = Report::new(ResourceError::BufferPoolSizeTooSmall)
            .attach("configured pool cannot hold the minimum resident pages")
            .change_context(RuntimeError::BufferPoolInit)
            .attach("buffer_pool_type=fixed, buffer_pool_role=meta");

        let err = Error::from(report);

        assert_eq!(err.kind(), ErrorKind::Runtime);
        assert_eq!(
            err.report().downcast_ref::<RuntimeError>().copied(),
            Some(RuntimeError::BufferPoolInit)
        );
        assert_eq!(
            err.report().downcast_ref::<ResourceError>().copied(),
            Some(ResourceError::BufferPoolSizeTooSmall)
        );
        assert!(format!("{err:?}").contains("buffer_pool_type=fixed, buffer_pool_role=meta"));
    }

    #[test]
    fn test_buffer_page_allocation_report_converts_losslessly_to_public_runtime() {
        let report = Report::new(ResourceError::BufferPoolFull)
            .attach("capacity=1, allocated=1")
            .change_context(RuntimeError::BufferPageAllocation)
            .attach("buffer_pool_type=fixed, buffer_pool_role=meta, operation=allocate_page");

        let err = Error::from(report);

        assert_eq!(err.kind(), ErrorKind::Runtime);
        assert_eq!(
            err.report().downcast_ref::<RuntimeError>().copied(),
            Some(RuntimeError::BufferPageAllocation)
        );
        assert_eq!(
            err.report().downcast_ref::<ResourceError>().copied(),
            Some(ResourceError::BufferPoolFull)
        );
        assert!(format!("{err:?}").contains("operation=allocate_page"));
    }

    #[test]
    fn test_buffer_page_access_report_converts_losslessly_to_public_runtime() {
        let report = Report::new(IoError::from(IoErrorKind::Other))
            .attach("injected page read failure")
            .change_context(RuntimeError::BufferPageAccess)
            .attach(
                "buffer_pool_type=evictable, buffer_pool_role=mem, operation=get_page, page_id=7",
            );

        let err = Error::from(report);

        assert_eq!(err.kind(), ErrorKind::Runtime);
        assert_eq!(
            err.report().downcast_ref::<RuntimeError>().copied(),
            Some(RuntimeError::BufferPageAccess)
        );
        assert_eq!(
            err.report()
                .downcast_ref::<IoError>()
                .copied()
                .map(IoError::kind),
            Some(IoErrorKind::Other)
        );
        assert!(format!("{err:?}").contains("operation=get_page, page_id=7"));
    }

    #[test]
    fn test_file_root_access_report_converts_losslessly_to_public_runtime() {
        let report = Report::new(DataIntegrityError::InvalidRootInvariant)
            .attach("block_id=3")
            .change_context(RuntimeError::FileRootAccess)
            .attach(
                "operation=load_file_root, file_kind=table_file, file_id=42, phase=validate_root",
            );

        let err = Error::from(report);

        assert_eq!(err.kind(), ErrorKind::Runtime);
        assert_eq!(
            err.report().downcast_ref::<RuntimeError>().copied(),
            Some(RuntimeError::FileRootAccess)
        );
        assert_eq!(
            err.report().downcast_ref::<DataIntegrityError>().copied(),
            Some(DataIntegrityError::InvalidRootInvariant)
        );
        assert!(format!("{err:?}").contains("operation=load_file_root"));
    }

    #[test]
    fn test_redo_log_discovery_report_converts_losslessly_to_public_runtime() {
        let report = Report::new(DataIntegrityError::InvalidRedoLogFileName)
            .attach("path=redo.log.invalid")
            .change_context(RuntimeError::RedoLogDiscovery)
            .attach("phase=enumerate_redo_log_family, file_prefix=redo.log");

        let err = Error::from(report);

        assert_eq!(err.kind(), ErrorKind::Runtime);
        assert_eq!(
            err.report().downcast_ref::<RuntimeError>().copied(),
            Some(RuntimeError::RedoLogDiscovery)
        );
        assert_eq!(
            err.report().downcast_ref::<DataIntegrityError>().copied(),
            Some(DataIntegrityError::InvalidRedoLogFileName)
        );
        assert!(format!("{err:?}").contains("file_prefix=redo.log"));
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
        let source = StdIoError::new(IoErrorKind::PermissionDenied, "open denied");
        let err = Error::from(
            Report::new(IoError::from(source.kind())).attach(format!("op=file_open, {source}")),
        );

        let output = format!("{err}");
        assert!(output.contains("io error"), "{output}");
        assert!(output.contains("permission denied"), "{output}");
        assert!(output.contains("op=file_open"), "{output}");
        assert!(output.contains("open denied"), "{output}");
    }

    #[test]
    fn test_typed_index_mutation_context_preserves_lower_error() {
        let lower: InternalResult<()> = Err(Report::new(InternalError::ColumnStorageMissing)
            .attach("secondary index storage is unavailable"));
        let report = lower
            .change_context(OperationError::IndexMutation)
            .attach_with(|| "phase=claim_secondary_index_key")
            .unwrap_err();

        assert_eq!(report.current_context(), &OperationError::IndexMutation);
        assert_eq!(
            report.downcast_ref::<InternalError>().copied(),
            Some(InternalError::ColumnStorageMissing)
        );

        let err: Error = report.attach("secondary index claim").into();
        assert_eq!(err.kind(), ErrorKind::Operation);
        assert_eq!(
            err.report().downcast_ref::<OperationError>().copied(),
            Some(OperationError::IndexMutation)
        );
        assert_eq!(
            err.report().downcast_ref::<InternalError>().copied(),
            Some(InternalError::ColumnStorageMissing)
        );
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
        let completion = CompletionErrorKind::from_io(
            IoError::report_backend(&backend_report, "backend progress source"),
            "completion context",
        );

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

    #[test]
    fn test_completion_adapters_preserve_each_typed_source() {
        let resource = CompletionErrorKind::from_resource(
            Report::new(ResourceError::BufferPoolFull).attach("resource source"),
            "resource completion",
        );
        assert_eq!(
            resource.current_context(),
            &CompletionErrorKind::Resource(ResourceError::BufferPoolFull)
        );
        assert_eq!(
            resource.downcast_ref::<ResourceError>().copied(),
            Some(ResourceError::BufferPoolFull)
        );
        assert_eq!(
            resource.downcast_ref::<&'static str>().copied(),
            Some("resource source")
        );

        let data_integrity = CompletionErrorKind::from_data_integrity(
            Report::new(DataIntegrityError::ChecksumMismatch).attach("checksum source"),
            "read completion",
        );
        assert_eq!(
            data_integrity.downcast_ref::<DataIntegrityError>().copied(),
            Some(DataIntegrityError::ChecksumMismatch)
        );

        let lifecycle = CompletionErrorKind::from_lifecycle(
            Report::new(LifecycleError::Shutdown).attach("shutdown source"),
            "reservation completion",
        );
        assert_eq!(
            lifecycle.downcast_ref::<LifecycleError>().copied(),
            Some(LifecycleError::Shutdown)
        );

        let source = StdIoError::other("durability IO source");
        let fatal = CompletionErrorKind::from_fatal(
            Report::new(IoError::from(source.kind()))
                .attach(format!("{source}"))
                .change_context(FatalError::RedoWrite),
            "redo completion",
        );
        assert_eq!(
            fatal.downcast_ref::<FatalError>().copied(),
            Some(FatalError::RedoWrite)
        );
        assert_eq!(
            fatal.downcast_ref::<IoError>().copied().map(IoError::kind),
            Some(IoErrorKind::Other)
        );

        let internal = CompletionErrorKind::from_internal(
            Report::new(InternalError::CompletionDropped).attach("internal source"),
            "completion owner",
        );
        assert_eq!(
            internal.downcast_ref::<InternalError>().copied(),
            Some(InternalError::CompletionDropped)
        );

        let send = CompletionErrorKind::from_send(
            Report::new(IoError::from(IoErrorKind::BrokenPipe)).attach("channel source"),
            "send completion",
        );
        assert_eq!(send.current_context(), &CompletionErrorKind::Send);
        assert_eq!(
            send.downcast_ref::<IoError>().copied().map(IoError::kind),
            Some(IoErrorKind::BrokenPipe)
        );

        for report in [resource, data_integrity, lifecycle, fatal, internal, send] {
            assert!(report.downcast_ref::<String>().is_some());
        }
    }
}
