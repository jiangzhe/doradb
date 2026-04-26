use crate::row::RowID;
use error_stack::Report;
use std::array::TryFromSliceError;
use std::fmt;
use std::io::{self, ErrorKind as IoErrorKind};
use std::ops::ControlFlow;
use thiserror::Error as ThisError;

pub type Result<T> = std::result::Result<T, Error>;
pub(crate) type ConfigResult<T> = std::result::Result<T, Report<ConfigError>>;
pub(crate) type DataIntegrityResult<T> = std::result::Result<T, Report<DataIntegrityError>>;
pub(crate) type LifecycleResult<T> = std::result::Result<T, Report<LifecycleError>>;
pub(crate) type FatalResult<T> = std::result::Result<T, Report<FatalError>>;
pub(crate) type CompletionResult<T> = std::result::Result<T, Report<CompletionErrorKind>>;

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
    #[error("invalid io depth")]
    InvalidIoDepth,
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
    #[error("old table root already retained")]
    OldTableRootAlreadyRetained,
    #[error("table not found")]
    TableNotFound,
    #[error("table already exists")]
    TableAlreadyExists,
    #[error("not supported")]
    NotSupported,
    #[error("duplicate key")]
    DuplicateKey,
    #[error("write conflict")]
    WriteConflict,
}

/// Fieldless fatal-domain errors carried underneath `ErrorKind::Fatal`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ThisError)]
pub(crate) enum FatalError {
    #[error("storage engine poisoned")]
    Poisoned,
    #[error("redo submit failed")]
    RedoSubmit,
    #[error("redo write failed")]
    RedoWrite,
    #[error("redo sync failed")]
    RedoSync,
    #[error("checkpoint write failed")]
    CheckpointWrite,
    #[error("purge deallocate failed")]
    PurgeDeallocate,
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
    #[error("column storage missing")]
    ColumnStorageMissing,
    #[error("mutable block view mismatch")]
    MutableBlockViewMismatch,
    #[error("completion dropped")]
    CompletionDropped,
    #[error("readonly buffer mapping conflict")]
    ReadonlyMappingConflict,
    #[error("readonly frame lock missing")]
    ReadonlyFrameLockMissing,
    #[error("row page missing")]
    RowPageMissing,
    #[error("table storage missing")]
    TableStorageMissing,
    #[error("disk pool guard missing")]
    DiskPoolGuardMissing,
    #[error("user secondary index missing")]
    UserSecondaryIndexMissing,
    #[error("secondary index view mismatch")]
    SecondaryIndexViewMismatch,
    #[error("deletion buffer missing")]
    DeletionBufferMissing,
    #[error("indexed value missing")]
    IndexedValueMissing,
    #[error("index key missing")]
    IndexKeyMissing,
    #[error("lwc builder misuse")]
    LwcBuilderMisuse,
    #[error("latch guard state mismatch")]
    LatchGuardStateMismatch,
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
    #[error("column index gc page invariant violated")]
    ColumnIndexGcPageInvariant,
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
    #[cfg(test)]
    #[error("injected test failure")]
    InjectedTestFailure,
}

/// IO-domain errors carried underneath `ErrorKind::Io`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ThisError)]
#[error("{0}")]
pub(crate) struct IoError(IoErrorKind);

impl IoError {
    #[inline]
    pub(crate) fn kind(self) -> IoErrorKind {
        self.0
    }

    #[inline]
    pub(crate) fn report(err: io::Error) -> Report<Self> {
        Report::new(Self::from(err.kind())).attach(format!("{}", err))
    }

    #[inline]
    pub(crate) fn report_with_op(op: StorageOp, err: io::Error) -> Report<Self> {
        Report::new(Self::from(err.kind())).attach(format!("op={op}, {err}"))
    }

    #[inline]
    pub(crate) fn report_unexpected_eof(
        actual_bytes: usize,
        expected_bytes: usize,
    ) -> Report<Self> {
        Report::new(Self::from(IoErrorKind::UnexpectedEof)).attach(format!(
            "unexpected eof: actual_bytes={actual_bytes}, expected_bytes={expected_bytes}"
        ))
    }

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
    #[inline]
    pub(crate) fn report_io(err: io::Error, message: impl Into<String>) -> Report<Self> {
        let kind = err.kind();
        Report::new(IoError::from(kind))
            .change_context(Self::Io(kind))
            .attach(format!("{err}"))
            .attach(message.into())
    }

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

    #[inline]
    pub(crate) fn report_send(message: impl Into<String>) -> Report<Self> {
        Report::new(IoError::from(IoErrorKind::BrokenPipe))
            .change_context(Self::Send)
            .attach(message.into())
    }

    #[inline]
    pub(crate) fn report_fatal(reason: FatalError, message: impl Into<String>) -> Report<Self> {
        Report::new(reason)
            .change_context(Self::Fatal(reason))
            .attach(message.into())
    }

    #[inline]
    pub(crate) fn report_internal(
        reason: InternalError,
        message: impl Into<String>,
    ) -> Report<Self> {
        Report::new(reason)
            .change_context(Self::Internal(reason))
            .attach(message.into())
    }

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

    #[inline]
    pub(crate) fn downcast_ref<T: Send + Sync + 'static>(&self) -> Option<&T> {
        self.0.downcast_ref::<T>()
    }

    #[inline]
    pub(crate) fn data_integrity_error(&self) -> Option<DataIntegrityError> {
        self.downcast_ref::<DataIntegrityError>().copied()
    }

    #[inline]
    pub(crate) fn lifecycle_error(&self) -> Option<LifecycleError> {
        self.downcast_ref::<LifecycleError>().copied()
    }

    #[inline]
    pub(crate) fn resource_error(&self) -> Option<ResourceError> {
        self.downcast_ref::<ResourceError>().copied()
    }

    #[inline]
    pub(crate) fn operation_error(&self) -> Option<OperationError> {
        self.downcast_ref::<OperationError>().copied()
    }

    #[inline]
    pub(crate) fn completion_error(&self) -> Option<CompletionErrorKind> {
        self.downcast_ref::<CompletionErrorKind>().copied()
    }

    #[inline]
    pub(crate) fn from_completion_report(
        report: Report<CompletionErrorKind>,
        message: impl Into<String>,
    ) -> Self {
        let kind = report.current_context().error_kind();
        Error(report.change_context(kind).attach(message.into()))
    }

    #[inline]
    pub(crate) fn internal() -> Self {
        Report::new(InternalError::Generic).into()
    }

    #[inline]
    pub(crate) fn wrong_secondary_index_binding(
        expected: &'static str,
        actual: &'static str,
    ) -> Self {
        Report::new(InternalError::SecondaryIndexBindingMismatch)
            .attach(SecondaryIndexBinding { expected, actual })
            .into()
    }

    #[inline]
    pub(crate) fn buffer_page_already_allocated() -> Self {
        Report::new(InternalError::BufferPageAlreadyAllocated).into()
    }

    #[inline]
    pub(crate) fn column_storage_missing() -> Self {
        Report::new(InternalError::ColumnStorageMissing).into()
    }

    #[inline]
    pub(crate) fn engine_component_already_registered() -> Self {
        Report::new(InternalError::EngineComponentAlreadyRegistered).into()
    }

    #[inline]
    pub(crate) fn engine_component_missing_dependency() -> Self {
        Report::new(InternalError::EngineComponentMissingDependency).into()
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
    fn from(src: glob::GlobError) -> Self {
        Report::new(IoError::from(src.error().kind()))
            .attach(format!("{}", src))
            .into()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_io_report_with_op_attaches_formatted_context() {
        let report = IoError::report_with_op(
            StorageOp::FileOpen,
            io::Error::new(IoErrorKind::PermissionDenied, "open denied"),
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
        let err = Error::from(IoError::report(io::Error::new(
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
}
