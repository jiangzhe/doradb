use crate::id::RowID;
use crate::io::BackendError;
use error_stack::{AttachmentKind, Frame, FrameKind, Report};
use std::array::TryFromSliceError;
use std::backtrace::Backtrace;
use std::convert::Infallible;
use std::error::Error as StdError;
use std::fmt::{self, Debug, Display};
use std::io::{self, ErrorKind as IoErrorKind};
use std::num::ParseIntError;
use std::ops::ControlFlow;
use std::panic::Location;
use std::result;
use std::str::Utf8Error;
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
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
/// Result carrying cloneable completion error bridges.
pub(crate) type CompletionResult<T> = result::Result<T, CompletionErrorBridge>;

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
}

impl From<IoErrorKind> for IoError {
    #[inline]
    fn from(kind: IoErrorKind) -> Self {
        IoError(kind)
    }
}

/// Closed registry of typed report roots permitted to cross a completion handoff.
pub(crate) enum CompletionSourceReport {
    /// IO-domain completion source.
    Io(Report<IoError>),
    /// Resource-domain completion source.
    Resource(Report<ResourceError>),
    /// Data-integrity-domain completion source.
    DataIntegrity(Report<DataIntegrityError>),
    /// Lifecycle-domain completion source.
    Lifecycle(Report<LifecycleError>),
    /// Fatal-domain completion source.
    Fatal(Report<FatalError>),
    /// Internal-domain completion source.
    Internal(Report<InternalError>),
}

impl From<Report<IoError>> for CompletionSourceReport {
    #[inline]
    fn from(report: Report<IoError>) -> Self {
        Self::Io(report)
    }
}

impl From<Report<ResourceError>> for CompletionSourceReport {
    #[inline]
    fn from(report: Report<ResourceError>) -> Self {
        Self::Resource(report)
    }
}

impl From<Report<DataIntegrityError>> for CompletionSourceReport {
    #[inline]
    fn from(report: Report<DataIntegrityError>) -> Self {
        Self::DataIntegrity(report)
    }
}

impl From<Report<LifecycleError>> for CompletionSourceReport {
    #[inline]
    fn from(report: Report<LifecycleError>) -> Self {
        Self::Lifecycle(report)
    }
}

impl From<Report<FatalError>> for CompletionSourceReport {
    #[inline]
    fn from(report: Report<FatalError>) -> Self {
        Self::Fatal(report)
    }
}

impl From<Report<InternalError>> for CompletionSourceReport {
    #[inline]
    fn from(report: Report<InternalError>) -> Self {
        Self::Internal(report)
    }
}

impl Debug for CompletionSourceReport {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(report) => Debug::fmt(report, f),
            Self::Resource(report) => Debug::fmt(report, f),
            Self::DataIntegrity(report) => Debug::fmt(report, f),
            Self::Lifecycle(report) => Debug::fmt(report, f),
            Self::Fatal(report) => Debug::fmt(report, f),
            Self::Internal(report) => Debug::fmt(report, f),
        }
    }
}

impl CompletionSourceReport {
    #[inline]
    #[cfg(test)]
    fn downcast_ref<T>(&self) -> Option<&T>
    where
        T: Send + Sync + 'static,
    {
        match self {
            Self::Io(report) => report.downcast_ref(),
            Self::Resource(report) => report.downcast_ref(),
            Self::DataIntegrity(report) => report.downcast_ref(),
            Self::Lifecycle(report) => report.downcast_ref(),
            Self::Fatal(report) => report.downcast_ref(),
            Self::Internal(report) => report.downcast_ref(),
        }
    }

    #[inline]
    fn fatal_context(&self) -> Option<FatalError> {
        match self {
            Self::Fatal(report) => Some(*report.current_context()),
            Self::Io(_)
            | Self::Resource(_)
            | Self::DataIntegrity(_)
            | Self::Lifecycle(_)
            | Self::Internal(_) => None,
        }
    }
}

struct BridgeInner {
    canonical: CompletionSourceReport,
    replay: Box<[ReplayFrame]>,
    #[cfg(test)]
    reconstructions: AtomicUsize,
}

enum ReplayFrame {
    Context(ReplayContext),
    Attachment(ReplayAttachment),
}

/// Arc-backed printable diagnostic replayed into every reconstructed report.
#[derive(Clone)]
struct SharedDiagnostic(Arc<str>);

impl Display for SharedDiagnostic {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Debug for SharedDiagnostic {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Debug::fmt(&self.0, f)
    }
}

/// Cloneable cross-thread transport for one canonical typed completion report.
///
/// Cloning this wrapper increments only the inner `Arc`. Physical reports are
/// rebuilt lazily and independently with the final owner's context when that
/// owner consumes the bridge. The transport itself is never a report context.
#[derive(Clone)]
pub(crate) struct CompletionErrorBridge(Arc<BridgeInner>);

impl CompletionErrorBridge {
    /// Captures one owned canonical typed report and validates its replay plan.
    ///
    /// # Panics
    ///
    /// Panics when the report is branched or contains a context or attachment
    /// outside the closed completion replay registry. Such a frame violates
    /// the crate-private completion producer contract; `Report` erases
    /// attachment types, so this invariant is validated during capture.
    #[inline]
    pub(crate) fn capture(report: impl Into<CompletionSourceReport>) -> Self {
        let canonical = report.into();
        let replay = Self::capture_replay(&canonical);
        Self(Arc::new(BridgeInner {
            canonical,
            replay,
            #[cfg(test)]
            reconstructions: AtomicUsize::new(0),
        }))
    }

    /// Reconstructs the physical report and installs the caller-owned context
    /// instead of retaining the completion transport as a report frame.
    #[inline]
    pub(crate) fn replace_context<C>(self, context: C) -> Report<C>
    where
        C: StdError + Send + Sync + 'static,
    {
        #[cfg(test)]
        self.0.reconstructions.fetch_add(1, Ordering::Relaxed);

        self.replay_builder().finish(context)
    }

    /// Inspects the canonical physical report without reconstructing a new stack.
    #[inline]
    #[cfg(test)]
    pub(crate) fn downcast_ref<T>(&self) -> Option<&T>
    where
        T: Send + Sync + 'static,
    {
        self.0.canonical.downcast_ref()
    }

    fn replay_builder(&self) -> ReplayReportBuilder {
        let mut replay = self.0.replay.iter();
        let Some(ReplayFrame::Context(context)) = replay.next() else {
            unreachable!("validated completion replay must start with a real context");
        };
        let mut builder = context.start_builder();
        for frame in replay {
            builder = match frame {
                ReplayFrame::Context(context) => builder.change_context(*context),
                ReplayFrame::Attachment(attachment) => builder.attach(attachment),
            };
        }
        builder
    }

    fn public_error_kind(&self) -> ErrorKind {
        self.0
            .replay
            .iter()
            .rev()
            .find_map(|frame| match frame {
                ReplayFrame::Context(context) => Some(context.error_kind()),
                ReplayFrame::Attachment(_) => None,
            })
            .expect("validated completion bridge must contain a real context")
    }

    #[inline]
    fn fatal_context(&self) -> Option<FatalError> {
        self.0.canonical.fatal_context()
    }

    #[inline]
    fn reconstruct_fatal(self) -> Option<Report<FatalError>> {
        #[cfg(test)]
        self.0.reconstructions.fetch_add(1, Ordering::Relaxed);

        self.replay_builder().into_fatal()
    }

    fn capture_replay(report: &CompletionSourceReport) -> Box<[ReplayFrame]> {
        match report {
            CompletionSourceReport::Io(report) => Self::capture_typed_replay(report, false),
            CompletionSourceReport::Resource(report) => Self::capture_typed_replay(report, false),
            CompletionSourceReport::DataIntegrity(report) => {
                Self::capture_typed_replay(report, false)
            }
            CompletionSourceReport::Lifecycle(report) => Self::capture_typed_replay(report, false),
            CompletionSourceReport::Fatal(report) => Self::capture_typed_replay(report, true),
            CompletionSourceReport::Internal(report) => Self::capture_typed_replay(report, false),
        }
    }

    fn capture_typed_replay<E>(report: &Report<E>, fatal_root: bool) -> Box<[ReplayFrame]>
    where
        E: StdError + Send + Sync + 'static,
    {
        let expected_frames = report.frames().count();
        let mut replay = Vec::with_capacity(expected_frames);
        let mut frame = report.current_frame();
        let mut visited = 0;
        let mut seen_fatal = false;

        loop {
            let position = visited;
            visited += 1;
            assert!(
                frame.sources().len() <= 1,
                "completion report must be linear: root_type={}, frame_position={position}, source_count={}",
                std::any::type_name::<E>(),
                frame.sources().len()
            );
            if let Some(replay_frame) =
                Self::capture_frame(frame, position, fatal_root, &mut seen_fatal)
            {
                replay.push(replay_frame);
            }
            let Some(source) = frame.sources().first() else {
                break;
            };
            frame = source;
        }

        assert_eq!(
            visited,
            expected_frames,
            "completion report must contain one linear root: root_type={}, visited_frames={visited}, total_frames={expected_frames}",
            std::any::type_name::<E>()
        );
        replay.reverse();
        assert!(
            matches!(replay.first(), Some(ReplayFrame::Context(_))),
            "completion report replay must start with a real context: root_type={}",
            std::any::type_name::<E>()
        );
        replay.into_boxed_slice()
    }

    fn capture_frame(
        frame: &Frame,
        position: usize,
        fatal_root: bool,
        seen_fatal: &mut bool,
    ) -> Option<ReplayFrame> {
        match frame.kind() {
            FrameKind::Context(_) => {
                if let Some(context) = ReplayContext::capture(frame) {
                    *seen_fatal |= matches!(context, ReplayContext::Fatal(_));
                    return Some(ReplayFrame::Context(context));
                }
                // TODO(error-boundary): remove this compatibility exception with backlog 000161.
                if fatal_root && *seen_fatal && frame.is::<ErrorKind>() {
                    return None;
                }
                panic!(
                    "unregistered completion context: frame_position={position}, type_id={:?}",
                    frame.type_id()
                );
            }
            FrameKind::Attachment(AttachmentKind::Printable(_)) => Some(ReplayFrame::Attachment(
                ReplayAttachment::capture(frame, position),
            )),
            FrameKind::Attachment(AttachmentKind::Opaque(_)) => {
                if frame.is::<Location<'static>>() || frame.is::<Backtrace>() {
                    None
                } else {
                    panic!(
                        "unregistered opaque completion attachment: frame_position={position}, type_id={:?}",
                        frame.type_id()
                    );
                }
            }
            FrameKind::Attachment(_) => {
                panic!(
                    "unregistered completion attachment kind: frame_position={position}, type_id={:?}",
                    frame.type_id()
                );
            }
        }
    }

    /// Returns the stable address identifying the shared canonical report.
    #[cfg(test)]
    pub(crate) fn test_identity(&self) -> *const () {
        Arc::as_ptr(&self.0).cast()
    }

    /// Returns how many physical reports have been reconstructed from this bridge.
    #[cfg(test)]
    pub(crate) fn test_reconstructions(&self) -> usize {
        self.0.reconstructions.load(Ordering::Relaxed)
    }
}

impl Display for CompletionErrorBridge {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("completion error bridge")
    }
}

impl Debug for CompletionErrorBridge {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Debug::fmt(&self.0.canonical, f)
    }
}

/// Cloneable source-bearing failure whose current context is always Fatal.
///
/// This wrapper carries fatal policy state through redo, transaction cleanup,
/// and engine poison without exposing the generic completion transport at
/// those boundaries. It is converted back to a completion bridge only when a
/// generic completion cell must carry the failure.
#[derive(Clone)]
pub(crate) struct SharedFatalError(CompletionErrorBridge);

impl SharedFatalError {
    /// Captures one owned Fatal report for shared propagation.
    #[inline]
    pub(crate) fn capture(report: Report<FatalError>) -> Self {
        Self(CompletionErrorBridge::capture(report))
    }

    /// Returns the current Fatal context guaranteed by this wrapper.
    #[inline]
    pub(crate) fn reason(&self) -> FatalError {
        self.0
            .fatal_context()
            .expect("shared fatal error must contain FatalError as its current context")
    }

    /// Reconstructs the exact source-bearing Fatal report without adding a
    /// duplicate Fatal context.
    #[inline]
    pub(crate) fn into_report(self) -> Report<FatalError> {
        let reason = self.reason();
        let report = self
            .0
            .reconstruct_fatal()
            .expect("shared fatal error replay must end with FatalError");
        debug_assert_eq!(*report.current_context(), reason);
        report
    }

    /// Converts this Fatal carrier for publication through a generic
    /// completion cell.
    #[inline]
    pub(crate) fn into_completion_bridge(self) -> CompletionErrorBridge {
        self.0
    }

    /// Returns the stable address identifying the shared canonical report.
    #[cfg(test)]
    pub(crate) fn test_identity(&self) -> *const () {
        self.0.test_identity()
    }
}

impl Debug for SharedFatalError {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Debug::fmt(&self.0, f)
    }
}

#[derive(Clone, Copy)]
enum ReplayContext {
    Config(ConfigError),
    Operation(OperationError),
    Resource(ResourceError),
    Io(IoError),
    DataIntegrity(DataIntegrityError),
    Lifecycle(LifecycleError),
    Runtime(RuntimeError),
    Fatal(FatalError),
    Internal(InternalError),
}

impl ReplayContext {
    fn capture(frame: &Frame) -> Option<Self> {
        if let Some(context) = frame.downcast_ref::<ConfigError>() {
            return Some(Self::Config(*context));
        }
        if let Some(context) = frame.downcast_ref::<OperationError>() {
            return Some(Self::Operation(*context));
        }
        if let Some(context) = frame.downcast_ref::<ResourceError>() {
            return Some(Self::Resource(*context));
        }
        if let Some(context) = frame.downcast_ref::<IoError>() {
            return Some(Self::Io(*context));
        }
        if let Some(context) = frame.downcast_ref::<DataIntegrityError>() {
            return Some(Self::DataIntegrity(*context));
        }
        if let Some(context) = frame.downcast_ref::<LifecycleError>() {
            return Some(Self::Lifecycle(*context));
        }
        if let Some(context) = frame.downcast_ref::<RuntimeError>() {
            return Some(Self::Runtime(*context));
        }
        if let Some(context) = frame.downcast_ref::<FatalError>() {
            return Some(Self::Fatal(*context));
        }
        if let Some(context) = frame.downcast_ref::<InternalError>() {
            return Some(Self::Internal(*context));
        }
        None
    }

    fn start_builder(self) -> ReplayReportBuilder {
        match self {
            Self::Config(context) => ReplayReportBuilder::Config(Report::new(context)),
            Self::Operation(context) => ReplayReportBuilder::Operation(Report::new(context)),
            Self::Resource(context) => ReplayReportBuilder::Resource(Report::new(context)),
            Self::Io(context) => ReplayReportBuilder::Io(Report::new(context)),
            Self::DataIntegrity(context) => {
                ReplayReportBuilder::DataIntegrity(Report::new(context))
            }
            Self::Lifecycle(context) => ReplayReportBuilder::Lifecycle(Report::new(context)),
            Self::Runtime(context) => ReplayReportBuilder::Runtime(Report::new(context)),
            Self::Fatal(context) => ReplayReportBuilder::Fatal(Report::new(context)),
            Self::Internal(context) => ReplayReportBuilder::Internal(Report::new(context)),
        }
    }

    fn change_report<C>(self, report: Report<C>) -> ReplayReportBuilder
    where
        C: StdError + Send + Sync + 'static,
    {
        match self {
            Self::Config(context) => ReplayReportBuilder::Config(report.change_context(context)),
            Self::Operation(context) => {
                ReplayReportBuilder::Operation(report.change_context(context))
            }
            Self::Resource(context) => {
                ReplayReportBuilder::Resource(report.change_context(context))
            }
            Self::Io(context) => ReplayReportBuilder::Io(report.change_context(context)),
            Self::DataIntegrity(context) => {
                ReplayReportBuilder::DataIntegrity(report.change_context(context))
            }
            Self::Lifecycle(context) => {
                ReplayReportBuilder::Lifecycle(report.change_context(context))
            }
            Self::Runtime(context) => ReplayReportBuilder::Runtime(report.change_context(context)),
            Self::Fatal(context) => ReplayReportBuilder::Fatal(report.change_context(context)),
            Self::Internal(context) => {
                ReplayReportBuilder::Internal(report.change_context(context))
            }
        }
    }

    #[inline]
    const fn error_kind(self) -> ErrorKind {
        match self {
            ReplayContext::Config(_) => ErrorKind::Config,
            ReplayContext::Operation(_) => ErrorKind::Operation,
            ReplayContext::Resource(_) => ErrorKind::Resource,
            ReplayContext::Io(_) => ErrorKind::Io,
            ReplayContext::DataIntegrity(_) => ErrorKind::DataIntegrity,
            ReplayContext::Lifecycle(_) => ErrorKind::Lifecycle,
            ReplayContext::Runtime(_) => ErrorKind::Runtime,
            ReplayContext::Fatal(_) => ErrorKind::Fatal,
            ReplayContext::Internal(_) => ErrorKind::Internal,
        }
    }
}

enum ReplayAttachment {
    Diagnostic(SharedDiagnostic),
    BackendError(BackendError),
}

impl ReplayAttachment {
    fn capture(frame: &Frame, position: usize) -> Self {
        if let Some(value) = frame.downcast_ref::<SharedDiagnostic>() {
            return Self::Diagnostic(value.clone());
        }
        if let Some(value) = frame.downcast_ref::<String>() {
            return Self::Diagnostic(SharedDiagnostic(Arc::from(value.as_str())));
        }
        if let Some(value) = frame.downcast_ref::<&'static str>() {
            return Self::Diagnostic(SharedDiagnostic(Arc::from(*value)));
        }
        if let Some(value) = frame.downcast_ref::<BackendError>() {
            return Self::BackendError(value.clone());
        }
        panic!(
            "unregistered printable completion attachment: frame_position={position}, type_id={:?}",
            frame.type_id()
        );
    }

    fn attach_to<C>(&self, report: Report<C>) -> Report<C>
    where
        C: StdError + Send + Sync + 'static,
    {
        match self {
            Self::Diagnostic(value) => report.attach(value.clone()),
            Self::BackendError(value) => report.attach(value.clone()),
        }
    }
}

enum ReplayReportBuilder {
    Config(Report<ConfigError>),
    Operation(Report<OperationError>),
    Resource(Report<ResourceError>),
    Io(Report<IoError>),
    DataIntegrity(Report<DataIntegrityError>),
    Lifecycle(Report<LifecycleError>),
    Runtime(Report<RuntimeError>),
    Fatal(Report<FatalError>),
    Internal(Report<InternalError>),
}

impl ReplayReportBuilder {
    fn attach(self, attachment: &ReplayAttachment) -> Self {
        match self {
            Self::Config(report) => Self::Config(attachment.attach_to(report)),
            Self::Operation(report) => Self::Operation(attachment.attach_to(report)),
            Self::Resource(report) => Self::Resource(attachment.attach_to(report)),
            Self::Io(report) => Self::Io(attachment.attach_to(report)),
            Self::DataIntegrity(report) => Self::DataIntegrity(attachment.attach_to(report)),
            Self::Lifecycle(report) => Self::Lifecycle(attachment.attach_to(report)),
            Self::Runtime(report) => Self::Runtime(attachment.attach_to(report)),
            Self::Fatal(report) => Self::Fatal(attachment.attach_to(report)),
            Self::Internal(report) => Self::Internal(attachment.attach_to(report)),
        }
    }

    fn change_context(self, context: ReplayContext) -> Self {
        match self {
            Self::Config(report) => context.change_report(report),
            Self::Operation(report) => context.change_report(report),
            Self::Resource(report) => context.change_report(report),
            Self::Io(report) => context.change_report(report),
            Self::DataIntegrity(report) => context.change_report(report),
            Self::Lifecycle(report) => context.change_report(report),
            Self::Runtime(report) => context.change_report(report),
            Self::Fatal(report) => context.change_report(report),
            Self::Internal(report) => context.change_report(report),
        }
    }

    fn finish<C>(self, context: C) -> Report<C>
    where
        C: StdError + Send + Sync + 'static,
    {
        match self {
            Self::Config(report) => report.change_context(context),
            Self::Operation(report) => report.change_context(context),
            Self::Resource(report) => report.change_context(context),
            Self::Io(report) => report.change_context(context),
            Self::DataIntegrity(report) => report.change_context(context),
            Self::Lifecycle(report) => report.change_context(context),
            Self::Runtime(report) => report.change_context(context),
            Self::Fatal(report) => report.change_context(context),
            Self::Internal(report) => report.change_context(context),
        }
    }

    #[inline]
    fn into_fatal(self) -> Option<Report<FatalError>> {
        match self {
            Self::Fatal(report) => Some(report),
            Self::Config(_)
            | Self::Operation(_)
            | Self::Resource(_)
            | Self::Io(_)
            | Self::DataIntegrity(_)
            | Self::Lifecycle(_)
            | Self::Runtime(_)
            | Self::Internal(_) => None,
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

    /// Reclassifies this public error as Fatal while retaining its physical sources.
    ///
    /// An existing Fatal frame owns the policy reason. `fallback_reason` is
    /// used only when the source has not already crossed a Fatal boundary.
    #[inline]
    pub(crate) fn into_fatal_report(self, fallback_reason: FatalError) -> Report<FatalError> {
        let reason = self
            .report()
            .downcast_ref::<FatalError>()
            .copied()
            .unwrap_or(fallback_reason);
        self.into_report().change_context(reason)
    }

    /// Adds boundary context without changing the existing error classification.
    #[inline]
    pub(crate) fn attach(self, attachment: impl Into<String>) -> Self {
        Error(self.0.attach(attachment.into()))
    }

    /// Converts a completion bridge into the public storage error.
    #[inline]
    pub(crate) fn from_completion_bridge(
        bridge: CompletionErrorBridge,
        message: impl Into<String>,
    ) -> Self {
        let kind = bridge.public_error_kind();
        Error(bridge.replace_context(kind).attach(message.into()))
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

impl From<SharedFatalError> for Error {
    #[inline]
    fn from(error: SharedFatalError) -> Self {
        error.into_report().into()
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
    use crate::io::BackendError;
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
    fn test_public_error_fatalization_preserves_existing_reason() {
        let existing = Error::from(
            Report::new(IoError::from(IoErrorKind::BrokenPipe))
                .change_context(FatalError::RedoSync),
        )
        .into_fatal_report(FatalError::CheckpointWrite);
        assert_eq!(*existing.current_context(), FatalError::RedoSync);
        assert_eq!(
            existing
                .downcast_ref::<IoError>()
                .copied()
                .map(IoError::kind),
            Some(IoErrorKind::BrokenPipe)
        );

        let fallback = Error::from(Report::new(IoError::from(IoErrorKind::Other)))
            .into_fatal_report(FatalError::CheckpointWrite);
        assert_eq!(*fallback.current_context(), FatalError::CheckpointWrite);
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
        let lower: InternalResult<()> = Err(Report::new(InternalError::IndexKeyMissing)
            .attach("secondary index key is unavailable"));
        let report = lower
            .change_context(OperationError::IndexMutation)
            .attach_with(|| "phase=claim_secondary_index_key")
            .unwrap_err();

        assert_eq!(report.current_context(), &OperationError::IndexMutation);
        assert_eq!(
            report.downcast_ref::<InternalError>().copied(),
            Some(InternalError::IndexKeyMissing)
        );

        let err: Error = report.attach("secondary index claim").into();
        assert_eq!(err.kind(), ErrorKind::Operation);
        assert_eq!(
            err.report().downcast_ref::<OperationError>().copied(),
            Some(OperationError::IndexMutation)
        );
        assert_eq!(
            err.report().downcast_ref::<InternalError>().copied(),
            Some(InternalError::IndexKeyMissing)
        );
        assert!(format!("{err}").contains("secondary index claim"));
    }

    #[test]
    fn test_completion_bridge_debug_delegates_to_canonical_report() {
        let report = Report::new(IoError::from(IoErrorKind::BrokenPipe))
            .attach("canonical completion detail");
        let expected = format!("{report:?}");
        let bridge = CompletionErrorBridge::capture(report);

        assert_eq!(format!("{bridge:?}"), expected);
    }

    #[test]
    fn test_completion_bridge_preserves_backend_report_and_public_classification() {
        let backend_report = BackendError::wait(
            "test_backend",
            StdIoError::new(IoErrorKind::TimedOut, "wait timed out"),
            3,
        )
        .into_report();
        // Keep two frames deliberately: replay must preserve repeated printable
        // attachments, even though production call sites combine one boundary's facts.
        let backend_report = backend_report.attach("op_kind=read");
        let bridge =
            CompletionErrorBridge::capture(backend_report.attach("complete test backend read"));
        assert_eq!(std::mem::size_of_val(&bridge), std::mem::size_of::<usize>());
        assert!(bridge.downcast_ref::<BackendError>().is_some());
        let completion = bridge
            .clone()
            .replace_context(RuntimeError::BufferPageAccess);
        let second_completion = bridge
            .clone()
            .replace_context(RuntimeError::BufferPageAccess);

        assert_eq!(
            completion
                .downcast_ref::<IoError>()
                .copied()
                .map(IoError::kind),
            Some(IoErrorKind::TimedOut)
        );
        assert!(completion.downcast_ref::<BackendError>().is_some());
        let output = format!("{completion:?}");
        assert!(output.contains("backend=test_backend"), "{output}");
        assert!(output.contains("op_kind=read"), "{output}");
        assert!(output.contains("complete test backend read"), "{output}");
        assert_eq!(
            completion
                .frames()
                .filter(|frame| frame.downcast_ref::<SharedDiagnostic>().is_some())
                .count(),
            2
        );
        let first_text = completion.downcast_ref::<SharedDiagnostic>().unwrap();
        let second_text = second_completion
            .downcast_ref::<SharedDiagnostic>()
            .unwrap();
        assert!(!std::ptr::eq(first_text, second_text));
        assert!(Arc::ptr_eq(&first_text.0, &second_text.0));
        let second_output = format!("{second_completion:?}");
        assert!(second_output.contains("backend=test_backend"));
        assert!(second_output.contains("op_kind=read"));
        assert!(second_output.contains("complete test backend read"));

        let err = Error::from_completion_bridge(bridge, "public completion boundary");
        assert_eq!(err.kind(), ErrorKind::Io);
        assert!(err.report().downcast_ref::<BackendError>().is_some());
        assert!(
            err.report()
                .downcast_ref::<CompletionErrorBridge>()
                .is_none()
        );
        assert!(!format!("{err}").contains("completion error bridge"));
    }

    #[test]
    fn test_completion_bridge_captures_permitted_roots() {
        let resource = CompletionErrorBridge::capture(
            Report::new(ResourceError::BufferPoolFull)
                .attach("resource source, resource completion"),
        )
        .replace_context(RuntimeError::BufferPageAccess);
        assert_eq!(
            resource.downcast_ref::<ResourceError>().copied(),
            Some(ResourceError::BufferPoolFull)
        );
        assert_eq!(
            resource
                .downcast_ref::<SharedDiagnostic>()
                .unwrap()
                .0
                .as_ref(),
            "resource source, resource completion"
        );

        let data_integrity = CompletionErrorBridge::capture(
            Report::new(DataIntegrityError::ChecksumMismatch)
                .attach("checksum source, read completion"),
        )
        .replace_context(RuntimeError::BufferPageAccess);
        assert_eq!(
            data_integrity.downcast_ref::<DataIntegrityError>().copied(),
            Some(DataIntegrityError::ChecksumMismatch)
        );

        let lifecycle = CompletionErrorBridge::capture(
            Report::new(LifecycleError::Shutdown).attach("shutdown source, reservation completion"),
        )
        .replace_context(RuntimeError::BufferPageAccess);
        assert_eq!(
            lifecycle.downcast_ref::<LifecycleError>().copied(),
            Some(LifecycleError::Shutdown)
        );

        let source = StdIoError::other("durability IO source");
        let fatal = CompletionErrorBridge::capture(
            Report::new(IoError::from(source.kind()))
                .attach(format!("{source}"))
                .change_context(FatalError::RedoWrite)
                .attach("redo completion"),
        )
        .replace_context(RuntimeError::BufferPageAccess);
        assert_eq!(
            fatal.downcast_ref::<FatalError>().copied(),
            Some(FatalError::RedoWrite)
        );
        assert_eq!(
            fatal.downcast_ref::<IoError>().copied().map(IoError::kind),
            Some(IoErrorKind::Other)
        );

        let internal = CompletionErrorBridge::capture(
            Report::new(InternalError::CompletionDropped)
                .attach("internal source, completion owner"),
        )
        .replace_context(RuntimeError::BufferPageAccess);
        assert_eq!(
            internal.downcast_ref::<InternalError>().copied(),
            Some(InternalError::CompletionDropped)
        );

        let send = CompletionErrorBridge::capture(
            Report::new(IoError::from(IoErrorKind::BrokenPipe))
                .attach("channel source, send completion"),
        )
        .replace_context(RuntimeError::BufferPageAccess);
        assert_eq!(
            send.downcast_ref::<IoError>().copied().map(IoError::kind),
            Some(IoErrorKind::BrokenPipe)
        );
    }

    #[test]
    fn test_completion_bridge_replays_real_context_order() {
        let report = Report::new(ConfigError::InvalidIoDepth)
            .attach("recovery_io_depth=0")
            .change_context(IoError::from(IoErrorKind::InvalidInput))
            .change_context(FatalError::RedoWrite);
        let reconstructed =
            CompletionErrorBridge::capture(report).replace_context(RuntimeError::FileRootAccess);
        let contexts = reconstructed
            .frames()
            .filter_map(|frame| {
                ReplayContext::capture(frame).map(|context| match context {
                    ReplayContext::Config(_) => "config",
                    ReplayContext::Io(_) => "io",
                    ReplayContext::Fatal(_) => "fatal",
                    ReplayContext::Runtime(_) => "runtime",
                    _ => "unexpected",
                })
            })
            .collect::<Vec<_>>();
        assert_eq!(contexts, ["runtime", "fatal", "io", "config"]);
    }

    #[test]
    fn test_completion_bridge_skips_lower_public_frame_below_fatal() {
        let report = Report::new(IoError::from(IoErrorKind::Other))
            .attach("terminal rollback IO source")
            .change_context(ErrorKind::Io)
            .change_context(FatalError::RollbackAccess);
        let reconstructed =
            CompletionErrorBridge::capture(report).replace_context(RuntimeError::BufferPageAccess);
        assert_eq!(
            reconstructed.downcast_ref::<FatalError>().copied(),
            Some(FatalError::RollbackAccess)
        );
        assert_eq!(
            reconstructed
                .downcast_ref::<IoError>()
                .copied()
                .map(IoError::kind),
            Some(IoErrorKind::Other)
        );
        assert!(reconstructed.downcast_ref::<ErrorKind>().is_none());
    }

    #[test]
    fn test_shared_fatal_error_reconstructs_exact_fatal_chain() {
        let shared = SharedFatalError::capture(
            Report::new(IoError::from(IoErrorKind::BrokenPipe))
                .attach("redo write source")
                .change_context(FatalError::RedoWrite)
                .attach("redo write policy"),
        );
        let identity = shared.test_identity();
        assert_eq!(std::mem::size_of_val(&shared), std::mem::size_of::<usize>());
        assert_eq!(shared.reason(), FatalError::RedoWrite);
        assert_eq!(shared.clone().test_identity(), identity);

        let fatal = shared.clone().into_report();
        assert_eq!(*fatal.current_context(), FatalError::RedoWrite);
        assert_eq!(
            fatal
                .frames()
                .filter(|frame| frame.is::<FatalError>())
                .count(),
            1
        );
        assert_eq!(
            fatal.downcast_ref::<IoError>().copied().map(IoError::kind),
            Some(IoErrorKind::BrokenPipe)
        );
        assert!(fatal.downcast_ref::<CompletionErrorBridge>().is_none());
        let output = format!("{fatal:?}");
        assert!(output.contains("redo write source"), "{output}");
        assert!(output.contains("redo write policy"), "{output}");

        let public = Error::from_completion_bridge(
            shared.into_completion_bridge(),
            "wait for shared fatal completion",
        );
        assert_eq!(public.kind(), ErrorKind::Fatal);
        assert_eq!(
            public.report().downcast_ref::<FatalError>().copied(),
            Some(FatalError::RedoWrite)
        );
        assert!(public.report().downcast_ref::<IoError>().is_some());
        assert!(
            public
                .report()
                .downcast_ref::<CompletionErrorBridge>()
                .is_none()
        );
    }

    #[test]
    fn test_completion_bridge_final_fatal_policy_recaptures_without_prior_transport() {
        let original = CompletionErrorBridge::capture(
            Report::new(IoError::from(IoErrorKind::TimedOut)).attach("checkpoint write source"),
        );
        let original_identity = original.test_identity();
        let public: Error = original
            .clone()
            .replace_context(RuntimeError::FileRootAccess)
            .into();
        assert!(
            public
                .report()
                .downcast_ref::<CompletionErrorBridge>()
                .is_none()
        );
        let fatal_error = SharedFatalError::capture(
            public
                .into_report()
                .change_context(FatalError::CheckpointWrite),
        );
        assert_ne!(fatal_error.test_identity(), original_identity);
        let fatal = fatal_error.into_report();

        assert_eq!(
            fatal.downcast_ref::<FatalError>().copied(),
            Some(FatalError::CheckpointWrite)
        );
        assert_eq!(
            fatal.downcast_ref::<RuntimeError>().copied(),
            Some(RuntimeError::FileRootAccess)
        );
        assert_eq!(
            fatal.downcast_ref::<IoError>().copied().map(IoError::kind),
            Some(IoErrorKind::TimedOut)
        );
        assert_eq!(
            fatal
                .frames()
                .filter(|frame| frame.is::<CompletionErrorBridge>())
                .count(),
            0
        );
        assert_eq!(
            fatal
                .frames()
                .filter(|frame| frame.is::<FatalError>())
                .count(),
            1
        );
        assert!(format!("{fatal:?}").contains("checkpoint write source"));
    }

    #[test]
    #[should_panic(expected = "completion report must be linear")]
    fn test_completion_bridge_rejects_branched_report() {
        let mut report = Report::new(FatalError::RedoWrite).expand();
        report.push(Report::new(FatalError::RedoSync));
        let _ = CompletionErrorBridge::capture(report.change_context(FatalError::Poisoned));
    }

    #[derive(Debug)]
    struct UnknownAttachment;

    impl Display for UnknownAttachment {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("unknown attachment")
        }
    }

    #[test]
    #[should_panic(expected = "unregistered printable completion attachment")]
    fn test_completion_bridge_rejects_unknown_attachment() {
        let report = Report::new(IoError::from(IoErrorKind::Other)).attach(UnknownAttachment);
        let _ = CompletionErrorBridge::capture(report);
    }
}
