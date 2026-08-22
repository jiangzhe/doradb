use crate::id::RowID;
use crate::io::BackendError;
use error_stack::{AttachmentKind, Frame, FrameKind, Report};
use std::backtrace::Backtrace;
use std::error::Error as StdError;
use std::fmt::{self, Debug, Display};
use std::io::ErrorKind as IoErrorKind;
use std::ops::ControlFlow;
use std::panic::Location;
use std::result;
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use thiserror::Error as ThisError;

/// Explicitly converges one audited typed error into the public wrapper.
pub(crate) trait DiscloseError {
    /// Applies the audited public classification without replacing source frames.
    fn disclose(self) -> Error;
}

/// Explicitly converges an audited typed result into the public result.
pub(crate) trait DiscloseResultExt<T> {
    /// Discloses only the error arm and preserves a successful value unchanged.
    fn disclose(self) -> Result<T>;
}

/// Storage result using the public storage error wrapper.
pub type Result<T> = result::Result<T, Error>;

impl<T, E: DiscloseError> DiscloseResultExt<T> for result::Result<T, E> {
    #[inline]
    fn disclose(self) -> Result<T> {
        self.map_err(DiscloseError::disclose)
    }
}
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

/// Minimal result extensions for carriers that preserve multiple error domains.
///
/// Unlike [`error_stack::ResultExt`], this trait keeps the carrier error type
/// unchanged instead of collapsing it into one `Report` context.
pub(crate) trait MultiDomainResultExt: Sized {
    /// Adds static caller-owned diagnostic context to the carrier error.
    fn attach(self, attachment: &'static str) -> Self;

    /// Lazily adds caller-owned diagnostic context to the carrier error.
    fn attach_with<F>(self, attachment: F) -> Self
    where
        F: FnOnce() -> String;
}

/// Result extensions specific to the Runtime-or-Fatal carrier.
pub(crate) trait RuntimeOrFatalResultExt: MultiDomainResultExt {
    /// Replaces only an ordinary Runtime context and leaves Fatal unchanged.
    ///
    /// This carrier primitive owns no operation identity. Semantic callers
    /// must chain [`MultiDomainResultExt::attach_with`] at the conversion site.
    fn change_runtime_context(self, context: RuntimeError) -> Self;
}

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
    #[error("invalid thread pool worker thread count")]
    InvalidThreadPoolWorkerThreads,
    #[error("invalid mandatory runtime concurrency limit")]
    InvalidMandatoryConcurrencyLimit,
    #[error("invalid buffer pool configuration")]
    InvalidBufferPoolConfig,
    #[error("invalid fixed buffer pool size")]
    InvalidFixedBufferPoolSize,
    #[error("invalid CoW file size")]
    InvalidCowFileSize,
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
    #[error("storage root is already in use")]
    StorageRootInUse,
    #[error("storage engine is shut down")]
    Shutdown,
    #[error("storage engine shutdown is busy")]
    ShutdownBusy,
    #[error("session is unavailable")]
    SessionUnavailable,
    #[error("existing transaction")]
    ExistingTransaction,
    #[error("existing session operation")]
    ExistingOperation,
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
    /// Redo discovery, recovery planning, or stream access could not complete.
    #[error("redo log access failed")]
    RedoLogAccess,
    /// Startup recovery could not complete because IO or durable state was invalid.
    #[error("startup recovery failed")]
    Recovery,
    /// Lookup, traversal, mutation, binding, or stream integration failed.
    #[error("index access failed")]
    IndexAccess,
    /// Row, index, or table runtime integration failed.
    #[error("table access failed")]
    TableAccess,
    /// Catalog storage or runtime integration failed.
    #[error("catalog access failed")]
    CatalogAccess,
    /// Reversible checkpoint orchestration failed.
    #[error("checkpoint execution failed")]
    CheckpointExecution,
    /// User- or system-transaction preparation or commit integration failed.
    #[error("transaction commit failed")]
    TransactionCommit,
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

/// Specific logical failures carried underneath [`ErrorKind::Operation`].
///
/// Public callers can retrieve this typed context through
/// [`Error::operation_error`] while the storage engine retains ownership of
/// error construction and disclosure.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, ThisError)]
pub enum OperationError {
    /// The requested table does not exist.
    #[error("table not found")]
    TableNotFound,
    /// The requested table is being dropped.
    #[error("table is dropping")]
    TableDropping,
    /// The admitted schema no longer matches the request.
    #[error("schema changed")]
    SchemaChanged,
    /// The requested index does not exist.
    #[error("index not found")]
    IndexNotFound,
    /// A unique key is already present.
    #[error("duplicate key")]
    DuplicateKey,
    /// A concurrent writer owns the requested row or key.
    #[error("write conflict")]
    WriteConflict,
    /// The DML request is structurally invalid.
    #[error("invalid DML input")]
    InvalidDmlInput,
    /// Catalog metadata is invalid for the requested operation.
    #[error("invalid metadata")]
    InvalidMetadata,
    /// A lock upgrade would have to wait.
    #[error("lock upgrade would block")]
    LockUpgradeWouldBlock,
    /// The requested lock conversion is unsupported.
    #[error("lock conversion is not supported")]
    LockConversionNotSupported,
    /// The requested lock conflicts with another lock family.
    #[error("lock family conflict")]
    LockFamilyConflict,
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
    #[error("mandatory task panicked")]
    MandatoryTaskPanic,
    #[error("thread pool task panicked")]
    ThreadPoolTaskPanic,
    #[error("thread pool is unavailable")]
    ThreadPoolUnavailable,
}

/// Fieldless internal-domain errors used beneath typed crate-private owners.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ThisError)]
pub(crate) enum InternalError {
    #[error("secondary index binding mismatch")]
    SecondaryIndexBindingMismatch,
    #[error("buffer page already allocated")]
    BufferPageAlreadyAllocated,
    #[error("readonly write barrier encountered an in-flight load")]
    ReadonlyWriteInflight,
    #[error("readonly block is blocked by a write barrier")]
    ReadonlyWriteBlocked,
    #[error("row page scan start is not a page boundary")]
    RowPageScanStartInvalid,
    #[error("lwc builder misuse")]
    LwcBuilderMisuse,
    #[error("secondary index out of bounds")]
    SecondaryIndexOutOfBounds,
    #[error("LWC block encoding contract is unsatisfied")]
    LwcBlockEncodingContract,
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
    /// Operation-domain completion source.
    Operation(Report<OperationError>),
    /// IO-domain completion source.
    Io(Report<IoError>),
    /// Resource-domain completion source.
    Resource(Report<ResourceError>),
    /// Data-integrity-domain completion source.
    DataIntegrity(Report<DataIntegrityError>),
    /// Lifecycle-domain completion source.
    Lifecycle(Report<LifecycleError>),
    /// Runtime-domain completion source.
    Runtime(Report<RuntimeError>),
    /// Fatal-domain completion source.
    Fatal(Report<FatalError>),
}

impl From<Report<OperationError>> for CompletionSourceReport {
    #[inline]
    fn from(report: Report<OperationError>) -> Self {
        Self::Operation(report)
    }
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

impl From<Report<RuntimeError>> for CompletionSourceReport {
    #[inline]
    fn from(report: Report<RuntimeError>) -> Self {
        Self::Runtime(report)
    }
}

impl From<Report<FatalError>> for CompletionSourceReport {
    #[inline]
    fn from(report: Report<FatalError>) -> Self {
        Self::Fatal(report)
    }
}

impl Debug for CompletionSourceReport {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Operation(report) => Debug::fmt(report, f),
            Self::Io(report) => Debug::fmt(report, f),
            Self::Resource(report) => Debug::fmt(report, f),
            Self::DataIntegrity(report) => Debug::fmt(report, f),
            Self::Lifecycle(report) => Debug::fmt(report, f),
            Self::Runtime(report) => Debug::fmt(report, f),
            Self::Fatal(report) => Debug::fmt(report, f),
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
            Self::Operation(report) => report.downcast_ref(),
            Self::Io(report) => report.downcast_ref(),
            Self::Resource(report) => report.downcast_ref(),
            Self::DataIntegrity(report) => report.downcast_ref(),
            Self::Lifecycle(report) => report.downcast_ref(),
            Self::Runtime(report) => report.downcast_ref(),
            Self::Fatal(report) => report.downcast_ref(),
        }
    }

    #[inline]
    fn fatal_context(&self) -> Option<FatalError> {
        match self {
            Self::Fatal(report) => Some(*report.current_context()),
            Self::Operation(_)
            | Self::Io(_)
            | Self::Resource(_)
            | Self::DataIntegrity(_)
            | Self::Lifecycle(_)
            | Self::Runtime(_) => None,
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

    /// Captures either native report in an Operation-or-Runtime carrier.
    #[inline]
    pub(crate) fn capture_operation_or_runtime(error: OperationOrRuntimeError) -> Self {
        match error {
            OperationOrRuntimeError::Operation(report) => Self::capture(report),
            OperationOrRuntimeError::Runtime(report) => Self::capture(report),
        }
    }

    /// Captures either native report in a Runtime-or-Fatal carrier.
    #[inline]
    pub(crate) fn capture_runtime_or_fatal(error: RuntimeOrFatalError) -> Self {
        match error {
            RuntimeOrFatalError::Runtime(report) => Self::capture(report),
            RuntimeOrFatalError::Fatal(report) => Self::capture(report),
        }
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

    /// Splits an audited completion into a peer Runtime-or-Fatal error.
    ///
    /// An existing Fatal source is reconstructed without adding Runtime. Every
    /// other registered physical source is reconstructed beneath the supplied
    /// Runtime context. The caller owns any diagnostic attached after this
    /// structural conversion.
    #[inline]
    pub(crate) fn into_runtime_or_fatal(
        self,
        runtime_context: RuntimeError,
    ) -> RuntimeOrFatalError {
        if self.fatal_context().is_some() {
            let report = self
                .reconstruct_fatal()
                .expect("Fatal completion source must reconstruct as Fatal");
            RuntimeOrFatalError::Fatal(report)
        } else {
            RuntimeOrFatalError::Runtime(self.replace_context(runtime_context))
        }
    }

    /// Replays a completion into the common four-domain integration carrier.
    ///
    /// Operation, Runtime, Lifecycle, and Fatal roots retain their native
    /// outer domain. Lower physical roots are stacked beneath the
    /// caller-supplied Runtime context.
    #[inline]
    pub(crate) fn into_quad(self, runtime_context: RuntimeError) -> QuadError {
        enum RootDomain {
            Operation,
            Runtime,
            Lifecycle,
            Fatal,
            Physical,
        }

        let root_domain = match &self.0.canonical {
            CompletionSourceReport::Operation(_) => RootDomain::Operation,
            CompletionSourceReport::Runtime(_) => RootDomain::Runtime,
            CompletionSourceReport::Lifecycle(_) => RootDomain::Lifecycle,
            CompletionSourceReport::Fatal(_) => RootDomain::Fatal,
            CompletionSourceReport::Io(_)
            | CompletionSourceReport::Resource(_)
            | CompletionSourceReport::DataIntegrity(_) => RootDomain::Physical,
        };

        #[cfg(test)]
        self.0.reconstructions.fetch_add(1, Ordering::Relaxed);

        let builder = self.replay_builder();
        match root_domain {
            RootDomain::Operation => QuadError::Operation(
                builder
                    .into_operation()
                    .expect("Operation completion source must reconstruct as Operation"),
            ),
            RootDomain::Runtime => QuadError::Runtime(
                builder
                    .into_runtime()
                    .expect("Runtime completion source must reconstruct as Runtime"),
            ),
            RootDomain::Lifecycle => QuadError::Lifecycle(
                builder
                    .into_lifecycle()
                    .expect("Lifecycle completion source must reconstruct as Lifecycle"),
            ),
            RootDomain::Fatal => QuadError::Fatal(
                builder
                    .into_fatal()
                    .expect("Fatal completion source must reconstruct as Fatal"),
            ),
            RootDomain::Physical => QuadError::Runtime(builder.finish(runtime_context)),
        }
    }

    /// Reconstructs a completion whose producer contract guarantees Fatal.
    ///
    /// # Panics
    ///
    /// Panics when the canonical completion source is not Fatal. Callers may
    /// use this only at a handoff whose completion type is generic but whose
    /// sole error producer is statically audited as [`FatalError`].
    #[inline]
    pub(crate) fn into_fatal_report(self) -> Report<FatalError> {
        self.reconstruct_fatal()
            .expect("Fatal-only completion must retain a canonical Fatal source")
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
            CompletionSourceReport::Operation(report) => Self::capture_typed_replay(report),
            CompletionSourceReport::Io(report) => Self::capture_typed_replay(report),
            CompletionSourceReport::Resource(report) => Self::capture_typed_replay(report),
            CompletionSourceReport::DataIntegrity(report) => Self::capture_typed_replay(report),
            CompletionSourceReport::Lifecycle(report) => Self::capture_typed_replay(report),
            CompletionSourceReport::Runtime(report) => Self::capture_typed_replay(report),
            CompletionSourceReport::Fatal(report) => Self::capture_typed_replay(report),
        }
    }

    fn capture_typed_replay<E>(report: &Report<E>) -> Box<[ReplayFrame]>
    where
        E: StdError + Send + Sync + 'static,
    {
        let expected_frames = report.frames().count();
        let mut replay = Vec::with_capacity(expected_frames);
        let mut frame = report.current_frame();
        let mut visited = 0;

        loop {
            let position = visited;
            visited += 1;
            assert!(
                frame.sources().len() <= 1,
                "completion report must be linear: root_type={}, frame_position={position}, source_count={}",
                std::any::type_name::<E>(),
                frame.sources().len()
            );
            if let Some(replay_frame) = Self::capture_frame(frame, position) {
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

    fn capture_frame(frame: &Frame, position: usize) -> Option<ReplayFrame> {
        match frame.kind() {
            FrameKind::Context(_) => {
                if let Some(context) = ReplayContext::capture(frame) {
                    return Some(ReplayFrame::Context(context));
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

impl DiscloseError for SharedFatalError {
    #[inline]
    fn disclose(self) -> Error {
        self.into_report().disclose()
    }
}

/// Constrained carrier for internal operations with Operation and Runtime exits.
///
/// The reports remain in their native domains until an outward public boundary.
/// This carrier is deliberately not an `error-stack` context and never accepts
/// a public [`Error`].
pub(crate) enum OperationOrRuntimeError {
    /// A terminal semantic operation failure.
    Operation(Report<OperationError>),
    /// A recoverable runtime-integration failure.
    Runtime(Report<RuntimeError>),
}

impl Debug for OperationOrRuntimeError {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Operation(report) => Debug::fmt(report, f),
            Self::Runtime(report) => Debug::fmt(report, f),
        }
    }
}

impl OperationOrRuntimeError {
    /// Adds static caller-owned diagnostic context without changing either domain.
    #[inline]
    pub(crate) fn attach(self, attachment: &'static str) -> Self {
        match self {
            Self::Operation(report) => Self::Operation(report.attach(attachment)),
            Self::Runtime(report) => Self::Runtime(report.attach(attachment)),
        }
    }

    /// Adds caller-owned diagnostic context without changing either domain.
    #[inline]
    pub(crate) fn attach_with<F>(self, attachment: F) -> Self
    where
        F: FnOnce() -> String,
    {
        match self {
            Self::Operation(report) => Self::Operation(report.attach(attachment())),
            Self::Runtime(report) => Self::Runtime(report.attach(attachment())),
        }
    }
}

impl From<Report<OperationError>> for OperationOrRuntimeError {
    #[inline]
    fn from(report: Report<OperationError>) -> Self {
        Self::Operation(report)
    }
}

impl From<Report<RuntimeError>> for OperationOrRuntimeError {
    #[inline]
    fn from(report: Report<RuntimeError>) -> Self {
        Self::Runtime(report)
    }
}

impl DiscloseError for OperationOrRuntimeError {
    #[inline]
    fn disclose(self) -> Error {
        match self {
            OperationOrRuntimeError::Operation(report) => report.disclose(),
            OperationOrRuntimeError::Runtime(report) => report.disclose(),
        }
    }
}

/// Result carrying either a terminal Operation report or a Runtime report.
pub(crate) type OperationOrRuntimeResult<T> = result::Result<T, OperationOrRuntimeError>;

impl<T> MultiDomainResultExt for OperationOrRuntimeResult<T> {
    #[inline]
    fn attach(self, attachment: &'static str) -> Self {
        self.map_err(|error| error.attach(attachment))
    }

    #[inline]
    fn attach_with<F>(self, attachment: F) -> Self
    where
        F: FnOnce() -> String,
    {
        self.map_err(|error| error.attach_with(attachment))
    }
}

/// Constrained carrier for foreground operations with Operation and Fatal exits.
///
/// The reports remain in their native domains until an outward public boundary.
/// This carrier is deliberately not an `error-stack` context and never accepts
/// a public [`Error`].
pub(crate) enum OperationOrFatalError {
    /// A terminal semantic operation failure.
    Operation(Report<OperationError>),
    /// A failure that already crossed a Fatal policy boundary.
    Fatal(Report<FatalError>),
}

impl Debug for OperationOrFatalError {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Operation(report) => Debug::fmt(report, f),
            Self::Fatal(report) => Debug::fmt(report, f),
        }
    }
}

impl OperationOrFatalError {
    /// Adds static caller-owned diagnostic context without changing either domain.
    #[inline]
    pub(crate) fn attach(self, attachment: &'static str) -> Self {
        match self {
            Self::Operation(report) => Self::Operation(report.attach(attachment)),
            Self::Fatal(report) => Self::Fatal(report.attach(attachment)),
        }
    }

    /// Adds caller-owned diagnostic context without changing either domain.
    #[inline]
    pub(crate) fn attach_with<F>(self, attachment: F) -> Self
    where
        F: FnOnce() -> String,
    {
        match self {
            Self::Operation(report) => Self::Operation(report.attach(attachment())),
            Self::Fatal(report) => Self::Fatal(report.attach(attachment())),
        }
    }
}

impl From<Report<OperationError>> for OperationOrFatalError {
    #[inline]
    fn from(report: Report<OperationError>) -> Self {
        Self::Operation(report)
    }
}

impl From<Report<FatalError>> for OperationOrFatalError {
    #[inline]
    fn from(report: Report<FatalError>) -> Self {
        Self::Fatal(report)
    }
}

impl DiscloseError for OperationOrFatalError {
    #[inline]
    fn disclose(self) -> Error {
        match self {
            OperationOrFatalError::Operation(report) => report.disclose(),
            OperationOrFatalError::Fatal(report) => report.disclose(),
        }
    }
}

/// Result carrying either a terminal Operation report or a Fatal report.
pub(crate) type OperationOrFatalResult<T> = result::Result<T, OperationOrFatalError>;

impl<T> MultiDomainResultExt for OperationOrFatalResult<T> {
    #[inline]
    fn attach(self, attachment: &'static str) -> Self {
        self.map_err(|error| error.attach(attachment))
    }

    #[inline]
    fn attach_with<F>(self, attachment: F) -> Self
    where
        F: FnOnce() -> String,
    {
        self.map_err(|error| error.attach_with(attachment))
    }
}

struct RuntimeOrFatalAttachment {
    relationship: &'static str,
    error: RuntimeOrFatalError,
}

impl Debug for RuntimeOrFatalAttachment {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Display::fmt(self, f)
    }
}

impl Display for RuntimeOrFatalAttachment {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {:?}", self.relationship, self.error)
    }
}

/// Constrained carrier for integration operations with Runtime and Fatal exits.
///
/// This enum owns the two report types directly and is deliberately not an
/// `error-stack` context. In particular, it never accepts a public [`Error`].
pub(crate) enum RuntimeOrFatalError {
    /// An ordinary recoverable integration failure.
    Runtime(Report<RuntimeError>),
    /// A failure that already crossed a Fatal policy boundary.
    Fatal(Report<FatalError>),
}

impl Debug for RuntimeOrFatalError {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Runtime(report) => Debug::fmt(report, f),
            Self::Fatal(report) => Debug::fmt(report, f),
        }
    }
}

impl Display for RuntimeOrFatalError {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Runtime(report) => Display::fmt(report.current_context(), f),
            Self::Fatal(report) => Display::fmt(report.current_context(), f),
        }
    }
}

impl RuntimeOrFatalError {
    /// Adds static caller-owned diagnostic context to either report arm.
    #[inline]
    pub(crate) fn attach(self, attachment: &'static str) -> Self {
        match self {
            Self::Runtime(report) => Self::Runtime(report.attach(attachment)),
            Self::Fatal(report) => Self::Fatal(report.attach(attachment)),
        }
    }

    /// Adds caller-owned diagnostic context to either report arm.
    #[inline]
    pub(crate) fn attach_with<F>(self, attachment: F) -> Self
    where
        F: FnOnce() -> String,
    {
        match self {
            Self::Runtime(report) => Self::Runtime(report.attach(attachment())),
            Self::Fatal(report) => Self::Fatal(report.attach(attachment())),
        }
    }

    /// Replaces only an ordinary Runtime context and leaves Fatal unchanged.
    ///
    /// This is the error-arm implementation behind
    /// [`RuntimeOrFatalResultExt::change_runtime_context`]. It deliberately
    /// does not attach caller facts; the result-level caller owns them.
    #[inline]
    fn change_runtime_context(self, context: RuntimeError) -> Self {
        match self {
            Self::Runtime(report) => Self::Runtime(report.change_context(context)),
            Self::Fatal(report) => Self::Fatal(report),
        }
    }

    /// Crosses an irreversible boundary without replacing an existing Fatal reason.
    ///
    /// The fallback selects policy only. Callers must attach the irreversible
    /// operation or phase where they invoke this generic carrier conversion.
    #[inline]
    pub(crate) fn into_fatal_report(self, fallback_reason: FatalError) -> Report<FatalError> {
        match self {
            Self::Runtime(report) => report.change_context(fallback_reason),
            Self::Fatal(report) => report,
        }
    }

    /// Merges a cleanup failure without losing the most important typed source.
    ///
    /// Fatal outranks Runtime, while the original operation source wins when
    /// both errors have the same domain. The non-selected carrier remains
    /// attached to the selected report as diagnostic evidence.
    #[inline]
    pub(crate) fn merge_cleanup(self, cleanup: Self) -> Self {
        match (self, cleanup) {
            (Self::Fatal(source), cleanup) => {
                Self::Fatal(source.attach(RuntimeOrFatalAttachment {
                    relationship: "secondary cleanup failure",
                    error: cleanup,
                }))
            }
            (source, Self::Fatal(cleanup)) => {
                Self::Fatal(cleanup.attach(RuntimeOrFatalAttachment {
                    relationship: "primary operation failure before fatal cleanup",
                    error: source,
                }))
            }
            (Self::Runtime(source), cleanup) => {
                Self::Runtime(source.attach(RuntimeOrFatalAttachment {
                    relationship: "secondary cleanup failure",
                    error: cleanup,
                }))
            }
        }
    }
}

impl From<Report<RuntimeError>> for RuntimeOrFatalError {
    #[inline]
    fn from(report: Report<RuntimeError>) -> Self {
        Self::Runtime(report)
    }
}

impl From<Report<FatalError>> for RuntimeOrFatalError {
    #[inline]
    fn from(report: Report<FatalError>) -> Self {
        Self::Fatal(report)
    }
}

impl From<SharedFatalError> for RuntimeOrFatalError {
    #[inline]
    fn from(error: SharedFatalError) -> Self {
        Self::Fatal(error.into_report())
    }
}

impl DiscloseError for RuntimeOrFatalError {
    #[inline]
    fn disclose(self) -> Error {
        match self {
            RuntimeOrFatalError::Runtime(report) => report.disclose(),
            RuntimeOrFatalError::Fatal(report) => report.disclose(),
        }
    }
}

/// Result carrying either an ordinary Runtime report or an already-Fatal report.
pub(crate) type RuntimeOrFatalResult<T> = result::Result<T, RuntimeOrFatalError>;

impl<T> MultiDomainResultExt for RuntimeOrFatalResult<T> {
    #[inline]
    fn attach(self, attachment: &'static str) -> Self {
        self.map_err(|error| error.attach(attachment))
    }

    #[inline]
    fn attach_with<F>(self, attachment: F) -> Self
    where
        F: FnOnce() -> String,
    {
        self.map_err(|error| error.attach_with(attachment))
    }
}

impl<T> RuntimeOrFatalResultExt for RuntimeOrFatalResult<T> {
    #[inline]
    fn change_runtime_context(self, context: RuntimeError) -> Self {
        self.map_err(|error| error.change_runtime_context(context))
    }
}

/// Constrained carrier for lifecycle rejection and fatal engine health exits.
///
/// The reports remain in their native domains until an outward public
/// boundary. This carrier is deliberately not an `error-stack` context.
pub(crate) enum LifecycleOrFatalError {
    /// A request rejected by ordinary lifecycle state.
    Lifecycle(Report<LifecycleError>),
    /// A request rejected by the engine's one-way fatal state.
    Fatal(Report<FatalError>),
}

impl Debug for LifecycleOrFatalError {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Lifecycle(report) => Debug::fmt(report, f),
            Self::Fatal(report) => Debug::fmt(report, f),
        }
    }
}

impl Display for LifecycleOrFatalError {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Lifecycle(report) => Display::fmt(report, f),
            Self::Fatal(report) => Display::fmt(report, f),
        }
    }
}

impl LifecycleOrFatalError {
    #[inline]
    fn attach(self, attachment: &'static str) -> Self {
        match self {
            Self::Lifecycle(report) => Self::Lifecycle(report.attach(attachment)),
            Self::Fatal(report) => Self::Fatal(report.attach(attachment)),
        }
    }

    #[inline]
    fn attach_with<F>(self, attachment: F) -> Self
    where
        F: FnOnce() -> String,
    {
        match self {
            Self::Lifecycle(report) => Self::Lifecycle(report.attach(attachment())),
            Self::Fatal(report) => Self::Fatal(report.attach(attachment())),
        }
    }
}

impl From<Report<LifecycleError>> for LifecycleOrFatalError {
    #[inline]
    fn from(report: Report<LifecycleError>) -> Self {
        Self::Lifecycle(report)
    }
}

impl From<Report<FatalError>> for LifecycleOrFatalError {
    #[inline]
    fn from(report: Report<FatalError>) -> Self {
        Self::Fatal(report)
    }
}

impl From<SharedFatalError> for LifecycleOrFatalError {
    #[inline]
    fn from(error: SharedFatalError) -> Self {
        Self::Fatal(error.into_report())
    }
}

impl DiscloseError for LifecycleOrFatalError {
    #[inline]
    fn disclose(self) -> Error {
        match self {
            Self::Lifecycle(report) => report.disclose(),
            Self::Fatal(report) => report.disclose(),
        }
    }
}

/// Result carrying either ordinary lifecycle rejection or a Fatal report.
pub(crate) type LifecycleOrFatalResult<T> = result::Result<T, LifecycleOrFatalError>;

impl<T> MultiDomainResultExt for LifecycleOrFatalResult<T> {
    #[inline]
    fn attach(self, attachment: &'static str) -> Self {
        self.map_err(|error| error.attach(attachment))
    }

    #[inline]
    fn attach_with<F>(self, attachment: F) -> Self
    where
        F: FnOnce() -> String,
    {
        self.map_err(|error| error.attach_with(attachment))
    }
}

/// Closed four-domain carrier for final internal integration owners.
///
/// The fixed membership is Operation, Runtime, Lifecycle, and Fatal. Lower
/// physical domains require an explicit Runtime owner before entering this
/// carrier. This carrier is deliberately not an `error-stack` context.
pub(crate) enum QuadError {
    /// A terminal semantic operation failure.
    Operation(Report<OperationError>),
    /// A recoverable runtime-integration failure.
    Runtime(Report<RuntimeError>),
    /// An ordinary lifecycle rejection.
    Lifecycle(Report<LifecycleError>),
    /// A failure that already crossed a Fatal policy boundary.
    Fatal(Report<FatalError>),
}

impl Debug for QuadError {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Operation(report) => Debug::fmt(report, f),
            Self::Runtime(report) => Debug::fmt(report, f),
            Self::Lifecycle(report) => Debug::fmt(report, f),
            Self::Fatal(report) => Debug::fmt(report, f),
        }
    }
}

impl Display for QuadError {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Operation(report) => Display::fmt(report, f),
            Self::Runtime(report) => Display::fmt(report, f),
            Self::Lifecycle(report) => Display::fmt(report, f),
            Self::Fatal(report) => Display::fmt(report, f),
        }
    }
}

impl QuadError {
    #[inline]
    fn attach(self, attachment: &'static str) -> Self {
        match self {
            Self::Operation(report) => Self::Operation(report.attach(attachment)),
            Self::Runtime(report) => Self::Runtime(report.attach(attachment)),
            Self::Lifecycle(report) => Self::Lifecycle(report.attach(attachment)),
            Self::Fatal(report) => Self::Fatal(report.attach(attachment)),
        }
    }

    #[inline]
    fn attach_with<F>(self, attachment: F) -> Self
    where
        F: FnOnce() -> String,
    {
        match self {
            Self::Operation(report) => Self::Operation(report.attach(attachment())),
            Self::Runtime(report) => Self::Runtime(report.attach(attachment())),
            Self::Lifecycle(report) => Self::Lifecycle(report.attach(attachment())),
            Self::Fatal(report) => Self::Fatal(report.attach(attachment())),
        }
    }
}

impl From<Report<OperationError>> for QuadError {
    #[inline]
    fn from(report: Report<OperationError>) -> Self {
        Self::Operation(report)
    }
}

impl From<Report<RuntimeError>> for QuadError {
    #[inline]
    fn from(report: Report<RuntimeError>) -> Self {
        Self::Runtime(report)
    }
}

impl From<Report<LifecycleError>> for QuadError {
    #[inline]
    fn from(report: Report<LifecycleError>) -> Self {
        Self::Lifecycle(report)
    }
}

impl From<Report<FatalError>> for QuadError {
    #[inline]
    fn from(report: Report<FatalError>) -> Self {
        Self::Fatal(report)
    }
}

impl From<SharedFatalError> for QuadError {
    #[inline]
    fn from(error: SharedFatalError) -> Self {
        Self::Fatal(error.into_report())
    }
}

impl From<OperationOrRuntimeError> for QuadError {
    #[inline]
    fn from(error: OperationOrRuntimeError) -> Self {
        match error {
            OperationOrRuntimeError::Operation(report) => Self::Operation(report),
            OperationOrRuntimeError::Runtime(report) => Self::Runtime(report),
        }
    }
}

impl From<OperationOrFatalError> for QuadError {
    #[inline]
    fn from(error: OperationOrFatalError) -> Self {
        match error {
            OperationOrFatalError::Operation(report) => Self::Operation(report),
            OperationOrFatalError::Fatal(report) => Self::Fatal(report),
        }
    }
}

impl From<RuntimeOrFatalError> for QuadError {
    #[inline]
    fn from(error: RuntimeOrFatalError) -> Self {
        match error {
            RuntimeOrFatalError::Runtime(report) => Self::Runtime(report),
            RuntimeOrFatalError::Fatal(report) => Self::Fatal(report),
        }
    }
}

impl From<LifecycleOrFatalError> for QuadError {
    #[inline]
    fn from(error: LifecycleOrFatalError) -> Self {
        match error {
            LifecycleOrFatalError::Lifecycle(report) => Self::Lifecycle(report),
            LifecycleOrFatalError::Fatal(report) => Self::Fatal(report),
        }
    }
}

impl DiscloseError for QuadError {
    #[inline]
    fn disclose(self) -> Error {
        match self {
            Self::Operation(report) => report.disclose(),
            Self::Runtime(report) => report.disclose(),
            Self::Lifecycle(report) => report.disclose(),
            Self::Fatal(report) => report.disclose(),
        }
    }
}

/// Result carrying the fixed Operation/Runtime/Lifecycle/Fatal integration set.
pub(crate) type QuadResult<T> = result::Result<T, QuadError>;

impl<T> MultiDomainResultExt for QuadResult<T> {
    #[inline]
    fn attach(self, attachment: &'static str) -> Self {
        self.map_err(|error| error.attach(attachment))
    }

    #[inline]
    fn attach_with<F>(self, attachment: F) -> Self
    where
        F: FnOnce() -> String,
    {
        self.map_err(|error| error.attach_with(attachment))
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

    #[inline]
    fn into_operation(self) -> Option<Report<OperationError>> {
        match self {
            Self::Operation(report) => Some(report),
            Self::Config(_)
            | Self::Resource(_)
            | Self::Io(_)
            | Self::DataIntegrity(_)
            | Self::Lifecycle(_)
            | Self::Runtime(_)
            | Self::Fatal(_)
            | Self::Internal(_) => None,
        }
    }

    #[inline]
    fn into_runtime(self) -> Option<Report<RuntimeError>> {
        match self {
            Self::Runtime(report) => Some(report),
            Self::Config(_)
            | Self::Operation(_)
            | Self::Resource(_)
            | Self::Io(_)
            | Self::DataIntegrity(_)
            | Self::Lifecycle(_)
            | Self::Fatal(_)
            | Self::Internal(_) => None,
        }
    }

    #[inline]
    fn into_lifecycle(self) -> Option<Report<LifecycleError>> {
        match self {
            Self::Lifecycle(report) => Some(report),
            Self::Config(_)
            | Self::Operation(_)
            | Self::Resource(_)
            | Self::Io(_)
            | Self::DataIntegrity(_)
            | Self::Runtime(_)
            | Self::Fatal(_)
            | Self::Internal(_) => None,
        }
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

    /// Return the specific operation failure when this is an operation error.
    #[inline]
    pub fn operation_error(&self) -> Option<OperationError> {
        if self.kind() != ErrorKind::Operation {
            return None;
        }
        self.0.downcast_ref::<OperationError>().copied()
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

impl DiscloseError for Report<ConfigError> {
    #[inline]
    fn disclose(self) -> Error {
        // This structural public classification adds no caller-owned diagnostic.
        Error(self.change_context(ErrorKind::Config))
    }
}

impl DiscloseError for Report<DataIntegrityError> {
    #[inline]
    fn disclose(self) -> Error {
        // This structural public classification adds no caller-owned diagnostic.
        Error(self.change_context(ErrorKind::DataIntegrity))
    }
}

impl DiscloseError for Report<LifecycleError> {
    #[inline]
    fn disclose(self) -> Error {
        // This structural public classification adds no caller-owned diagnostic.
        Error(self.change_context(ErrorKind::Lifecycle))
    }
}

impl DiscloseError for Report<FatalError> {
    #[inline]
    fn disclose(self) -> Error {
        // This structural public classification adds no caller-owned diagnostic.
        Error(self.change_context(ErrorKind::Fatal))
    }
}

impl DiscloseError for Report<ResourceError> {
    #[inline]
    fn disclose(self) -> Error {
        // This structural public classification adds no caller-owned diagnostic.
        Error(self.change_context(ErrorKind::Resource))
    }
}

impl DiscloseError for Report<OperationError> {
    #[inline]
    fn disclose(self) -> Error {
        // This structural public classification adds no caller-owned diagnostic.
        Error(self.change_context(ErrorKind::Operation))
    }
}

impl DiscloseError for Report<IoError> {
    #[inline]
    fn disclose(self) -> Error {
        // This structural public classification adds no caller-owned diagnostic.
        Error(self.change_context(ErrorKind::Io))
    }
}

impl DiscloseError for Report<RuntimeError> {
    #[inline]
    fn disclose(self) -> Error {
        // This structural public classification adds no caller-owned diagnostic.
        Error(self.change_context(ErrorKind::Runtime))
    }
}

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
    use std::cell::Cell;
    use std::io::Error as StdIoError;

    #[derive(Debug)]
    struct UnknownAttachment;

    impl Display for UnknownAttachment {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("unknown attachment")
        }
    }

    #[test]
    fn test_public_operation_error_returns_every_variant() {
        let cases = [
            OperationError::TableNotFound,
            OperationError::TableDropping,
            OperationError::SchemaChanged,
            OperationError::IndexNotFound,
            OperationError::DuplicateKey,
            OperationError::WriteConflict,
            OperationError::InvalidDmlInput,
            OperationError::InvalidMetadata,
            OperationError::LockUpgradeWouldBlock,
            OperationError::LockConversionNotSupported,
            OperationError::LockFamilyConflict,
        ];
        for operation_error in cases {
            let error = Report::new(operation_error).disclose();
            assert_eq!(error.operation_error(), Some(operation_error));
        }

        let config = Report::new(ConfigError::InvalidIoDepth).disclose();
        assert_eq!(config.operation_error(), None);
    }

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
        let err = Report::new(IoError::from(source.kind()))
            .attach(format!("{source}"))
            .disclose();

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
    fn test_std_io_error_disclosure_preserves_owned_source() {
        let source = StdIoError::from_raw_os_error(libc::EIO);
        let err = Report::new(IoError::from(source.kind()))
            .attach(source)
            .disclose();

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

        let err = report.disclose();

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
    fn test_operation_or_runtime_operation_arm_stays_operation() {
        let carrier = OperationOrRuntimeError::from(
            Report::new(OperationError::DuplicateKey).attach("operation=insert_unique_index"),
        )
        .attach("table_id=42");

        let err = carrier.disclose();

        assert_eq!(err.kind(), ErrorKind::Operation);
        assert_eq!(
            err.report().downcast_ref::<OperationError>().copied(),
            Some(OperationError::DuplicateKey)
        );
        let output = format!("{err:?}");
        assert!(output.contains("operation=insert_unique_index"));
        assert!(output.contains("table_id=42"));
    }

    #[test]
    fn test_operation_or_runtime_runtime_arm_preserves_native_source() {
        let carrier = OperationOrRuntimeError::from(
            Report::new(InternalError::SecondaryIndexOutOfBounds)
                .attach("index_no=4, index_count=2")
                .change_context(RuntimeError::TableAccess),
        )
        .attach("operation=insert_index");

        let err = carrier.disclose();

        assert_eq!(err.kind(), ErrorKind::Runtime);
        assert_eq!(
            err.report().downcast_ref::<RuntimeError>().copied(),
            Some(RuntimeError::TableAccess)
        );
        assert_eq!(
            err.report().downcast_ref::<InternalError>().copied(),
            Some(InternalError::SecondaryIndexOutOfBounds)
        );
        let output = format!("{err:?}");
        assert!(output.contains("index_no=4, index_count=2"));
        assert!(output.contains("operation=insert_index"));
    }

    #[test]
    fn test_operation_or_runtime_result_attachment_preserves_operation_arm() {
        let result: OperationOrRuntimeResult<()> = Err(OperationOrRuntimeError::from(Report::new(
            OperationError::DuplicateKey,
        )));

        let carrier = result
            .attach("operation=insert_unique_index")
            .expect_err("operation failure must remain terminal");
        let OperationOrRuntimeError::Operation(report) = carrier else {
            panic!("operation attachment must preserve the Operation arm")
        };

        assert_eq!(
            report.downcast_ref::<OperationError>().copied(),
            Some(OperationError::DuplicateKey)
        );
        assert!(format!("{report:?}").contains("operation=insert_unique_index"));
    }

    #[test]
    fn test_operation_or_fatal_operation_arm_stays_operation() {
        let carrier = OperationOrFatalError::from(
            Report::new(OperationError::SchemaChanged).attach("table_id=42"),
        )
        .attach("operation=table_insert_mvcc");

        let err = carrier.disclose();

        assert_eq!(err.kind(), ErrorKind::Operation);
        assert_eq!(
            err.report().downcast_ref::<OperationError>().copied(),
            Some(OperationError::SchemaChanged)
        );
        let output = format!("{err:?}");
        assert!(output.contains("table_id=42"));
        assert!(output.contains("operation=table_insert_mvcc"));
    }

    #[test]
    fn test_operation_or_fatal_result_attachment_preserves_fatal_arm() {
        let result: OperationOrFatalResult<()> = Err(OperationOrFatalError::from(Report::new(
            FatalError::Poisoned,
        )));

        let carrier = result
            .attach("operation=admit_user_table")
            .expect_err("fatal failure must remain terminal");
        let OperationOrFatalError::Fatal(report) = carrier else {
            panic!("fatal attachment must preserve the Fatal arm")
        };

        assert_eq!(
            report.downcast_ref::<FatalError>().copied(),
            Some(FatalError::Poisoned)
        );
        assert!(format!("{report:?}").contains("operation=admit_user_table"));
        assert_eq!(
            OperationOrFatalError::Fatal(report).disclose().kind(),
            ErrorKind::Fatal
        );
    }

    #[test]
    fn test_runtime_or_fatal_result_attachment_preserves_fatal_arm() {
        let result: RuntimeOrFatalResult<()> = Err(RuntimeOrFatalError::Fatal(Report::new(
            FatalError::CheckpointWrite,
        )));

        let carrier = result
            .attach("operation=publish_checkpoint")
            .expect_err("fatal failure must remain terminal");
        let RuntimeOrFatalError::Fatal(report) = carrier else {
            panic!("fatal attachment must preserve the Fatal arm")
        };

        assert_eq!(
            report.downcast_ref::<FatalError>().copied(),
            Some(FatalError::CheckpointWrite)
        );
        assert!(format!("{report:?}").contains("operation=publish_checkpoint"));
    }

    #[test]
    fn test_runtime_or_fatal_result_changes_only_runtime_context() {
        let result: RuntimeOrFatalResult<()> = Err(RuntimeOrFatalError::Runtime(
            Report::new(InternalError::SecondaryIndexOutOfBounds)
                .attach("index_no=4, index_count=2")
                .change_context(RuntimeError::IndexAccess),
        ));

        let carrier = result
            .change_runtime_context(RuntimeError::CheckpointExecution)
            .attach("operation=checkpoint_table")
            .expect_err("runtime failure must remain an error");
        let RuntimeOrFatalError::Runtime(report) = carrier else {
            panic!("Runtime context replacement must preserve the Runtime arm")
        };

        assert_eq!(report.current_context(), &RuntimeError::CheckpointExecution);
        assert_eq!(
            report.downcast_ref::<InternalError>().copied(),
            Some(InternalError::SecondaryIndexOutOfBounds)
        );
        let output = format!("{report:?}");
        assert!(output.contains("index_no=4, index_count=2"));
        assert!(output.contains("operation=checkpoint_table"));
    }

    #[test]
    fn test_runtime_or_fatal_result_context_change_preserves_fatal_arm() {
        let result: RuntimeOrFatalResult<()> = Err(RuntimeOrFatalError::Fatal(
            Report::new(IoError::from(IoErrorKind::BrokenPipe))
                .attach("checkpoint write failed")
                .change_context(FatalError::CheckpointWrite),
        ));

        let carrier = result
            .change_runtime_context(RuntimeError::CheckpointExecution)
            .expect_err("fatal failure must remain an error");
        let RuntimeOrFatalError::Fatal(report) = carrier else {
            panic!("Runtime context replacement must not replace Fatal")
        };

        assert_eq!(report.current_context(), &FatalError::CheckpointWrite);
        assert_eq!(
            report.downcast_ref::<IoError>().copied().map(IoError::kind),
            Some(IoErrorKind::BrokenPipe)
        );
        assert!(format!("{report:?}").contains("checkpoint write failed"));
    }

    #[test]
    fn test_runtime_or_fatal_cleanup_precedence_preserves_typed_sources() {
        let source_fatal = RuntimeOrFatalError::Fatal(
            Report::new(FatalError::RedoWrite).attach("fatal operation source"),
        );
        let cleanup_runtime = RuntimeOrFatalError::Runtime(
            Report::new(RuntimeError::CatalogAccess).attach("runtime cleanup source"),
        );
        let RuntimeOrFatalError::Fatal(report) = source_fatal.merge_cleanup(cleanup_runtime) else {
            panic!("fatal operation source must outrank runtime cleanup")
        };
        assert_eq!(*report.current_context(), FatalError::RedoWrite);
        let output = format!("{report:?}");
        assert!(output.contains("fatal operation source"));
        assert!(output.contains("runtime cleanup source"));
        assert!(output.contains("secondary cleanup failure"));

        let source_runtime = RuntimeOrFatalError::Runtime(
            Report::new(RuntimeError::IndexAccess).attach("runtime operation source"),
        );
        let cleanup_fatal = RuntimeOrFatalError::Fatal(
            Report::new(FatalError::RollbackAccess).attach("fatal cleanup source"),
        );
        let RuntimeOrFatalError::Fatal(report) = source_runtime.merge_cleanup(cleanup_fatal) else {
            panic!("fatal cleanup must outrank runtime operation source")
        };
        assert_eq!(*report.current_context(), FatalError::RollbackAccess);
        let output = format!("{report:?}");
        assert!(output.contains("runtime operation source"));
        assert!(output.contains("fatal cleanup source"));
        assert!(output.contains("primary operation failure before fatal cleanup"));

        let source_fatal = RuntimeOrFatalError::Fatal(
            Report::new(FatalError::RedoWrite).attach("first fatal source"),
        );
        let cleanup_fatal = RuntimeOrFatalError::Fatal(
            Report::new(FatalError::RollbackAccess).attach("later fatal cleanup"),
        );
        let RuntimeOrFatalError::Fatal(report) = source_fatal.merge_cleanup(cleanup_fatal) else {
            panic!("first fatal source must retain equal-domain precedence")
        };
        assert_eq!(*report.current_context(), FatalError::RedoWrite);
        assert!(format!("{report:?}").contains("later fatal cleanup"));

        let source_runtime = RuntimeOrFatalError::Runtime(
            Report::new(RuntimeError::IndexAccess).attach("first runtime source"),
        );
        let cleanup_runtime = RuntimeOrFatalError::Runtime(
            Report::new(RuntimeError::CatalogAccess).attach("later runtime cleanup"),
        );
        let RuntimeOrFatalError::Runtime(report) = source_runtime.merge_cleanup(cleanup_runtime)
        else {
            panic!("runtime operation source must retain equal-domain precedence")
        };
        assert_eq!(*report.current_context(), RuntimeError::IndexAccess);
        assert!(format!("{report:?}").contains("later runtime cleanup"));
    }

    #[test]
    fn test_multi_domain_result_attachment_is_lazy_on_success() {
        let called = Cell::new(false);
        let result: OperationOrRuntimeResult<u8> = Ok(7);

        let value = result
            .attach_with(|| {
                called.set(true);
                "must not be evaluated".to_string()
            })
            .expect("successful result must remain successful");

        assert_eq!(value, 7);
        assert!(!called.get());
    }

    #[test]
    fn test_runtime_or_fatal_runtime_arm_converts_losslessly() {
        let carrier = RuntimeOrFatalError::Runtime(
            Report::new(InternalError::SecondaryIndexOutOfBounds)
                .attach("index_no=4, index_count=2")
                .change_context(RuntimeError::CheckpointExecution),
        );

        let err = carrier.disclose();

        assert_eq!(err.kind(), ErrorKind::Runtime);
        assert_eq!(
            err.report().downcast_ref::<RuntimeError>().copied(),
            Some(RuntimeError::CheckpointExecution)
        );
        assert_eq!(
            err.report().downcast_ref::<InternalError>().copied(),
            Some(InternalError::SecondaryIndexOutOfBounds)
        );
        assert!(format!("{err:?}").contains("index_no=4, index_count=2"));
    }

    #[test]
    fn test_runtime_or_fatal_fatal_arm_converts_losslessly() {
        let carrier = RuntimeOrFatalError::Fatal(
            Report::new(IoError::from(IoErrorKind::BrokenPipe))
                .attach("checkpoint write failed")
                .change_context(FatalError::CheckpointWrite),
        );

        let err = carrier.disclose();

        assert_eq!(err.kind(), ErrorKind::Fatal);
        assert_eq!(
            err.report().downcast_ref::<FatalError>().copied(),
            Some(FatalError::CheckpointWrite)
        );
        assert_eq!(
            err.report()
                .downcast_ref::<IoError>()
                .copied()
                .map(IoError::kind),
            Some(IoErrorKind::BrokenPipe)
        );
        assert!(format!("{err:?}").contains("checkpoint write failed"));
    }

    #[test]
    fn lifecycle_or_fatal_preserves_domain_and_attachments() {
        let lifecycle: LifecycleOrFatalResult<()> =
            Err(Report::new(LifecycleError::Shutdown).into());
        let error = lifecycle
            .attach("operation=admit")
            .expect_err("shutdown must reject admission")
            .disclose();
        assert_eq!(error.kind(), ErrorKind::Lifecycle);
        assert_eq!(
            error.report().downcast_ref::<LifecycleError>().copied(),
            Some(LifecycleError::Shutdown)
        );
        assert!(format!("{error:?}").contains("operation=admit"));

        let fatal: LifecycleOrFatalResult<()> = Err(Report::new(FatalError::Poisoned).into());
        let error = fatal
            .attach_with(|| "phase=health_check".to_owned())
            .expect_err("poison must reject admission")
            .disclose();
        assert_eq!(error.kind(), ErrorKind::Fatal);
        assert_eq!(
            error.report().downcast_ref::<FatalError>().copied(),
            Some(FatalError::Poisoned)
        );
        assert!(error.report().downcast_ref::<LifecycleError>().is_none());
        assert!(format!("{error:?}").contains("phase=health_check"));
    }

    #[test]
    fn quad_native_arms_disclose_without_carrier_context() {
        let cases = [
            (
                QuadError::from(Report::new(OperationError::DuplicateKey)),
                ErrorKind::Operation,
            ),
            (
                QuadError::from(Report::new(RuntimeError::TableAccess)),
                ErrorKind::Runtime,
            ),
            (
                QuadError::from(Report::new(LifecycleError::Shutdown)),
                ErrorKind::Lifecycle,
            ),
            (
                QuadError::from(Report::new(FatalError::Poisoned)),
                ErrorKind::Fatal,
            ),
        ];

        for (carrier, expected_kind) in cases {
            let error = carrier.attach("operation=quad_test").disclose();
            assert_eq!(error.kind(), expected_kind);
            assert!(error.report().downcast_ref::<QuadError>().is_none());
            assert!(format!("{error:?}").contains("operation=quad_test"));
        }
    }

    #[test]
    fn quad_flattens_pairwise_carriers_without_losing_reports() {
        let operation = QuadError::from(OperationOrRuntimeError::Operation(
            Report::new(OperationError::IndexNotFound).attach("pair=operation_runtime"),
        ));
        let runtime = QuadError::from(RuntimeOrFatalError::Runtime(
            Report::new(RuntimeError::IndexAccess).attach("pair=runtime_fatal"),
        ));
        let fatal = QuadError::from(OperationOrFatalError::Fatal(
            Report::new(FatalError::RedoWrite).attach("pair=operation_fatal"),
        ));
        let lifecycle = QuadError::from(LifecycleOrFatalError::Lifecycle(
            Report::new(LifecycleError::Shutdown).attach("pair=lifecycle_fatal"),
        ));

        assert!(matches!(&operation, QuadError::Operation(_)));
        assert!(matches!(&runtime, QuadError::Runtime(_)));
        assert!(matches!(&fatal, QuadError::Fatal(_)));
        assert!(matches!(&lifecycle, QuadError::Lifecycle(_)));
        for (carrier, attachment) in [
            (operation, "pair=operation_runtime"),
            (runtime, "pair=runtime_fatal"),
            (fatal, "pair=operation_fatal"),
            (lifecycle, "pair=lifecycle_fatal"),
        ] {
            let error = carrier.disclose();
            assert!(format!("{error:?}").contains(attachment));
            assert!(error.report().downcast_ref::<QuadError>().is_none());
        }
    }

    #[test]
    fn test_buffer_pool_init_report_converts_losslessly_to_public_runtime() {
        let report = Report::new(ResourceError::BufferPoolSizeTooSmall)
            .attach("configured pool cannot hold the minimum resident pages")
            .change_context(RuntimeError::BufferPoolInit)
            .attach("buffer_pool_type=fixed, buffer_pool_role=meta");

        let err = report.disclose();

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

        let err = report.disclose();

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

        let err = report.disclose();

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

        let err = report.disclose();

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
            .change_context(RuntimeError::RedoLogAccess)
            .attach("phase=enumerate_redo_log_family, file_prefix=redo.log");

        let err = report.disclose();

        assert_eq!(err.kind(), ErrorKind::Runtime);
        assert_eq!(
            err.report().downcast_ref::<RuntimeError>().copied(),
            Some(RuntimeError::RedoLogAccess)
        );
        assert_eq!(
            err.report().downcast_ref::<DataIntegrityError>().copied(),
            Some(DataIntegrityError::InvalidRedoLogFileName)
        );
        assert!(format!("{err:?}").contains("file_prefix=redo.log"));
    }

    #[test]
    fn test_recovery_io_report_converts_losslessly_to_public_runtime() {
        let report = Report::new(IoError::from(IoErrorKind::PermissionDenied))
            .attach("operation=open_recovery_file")
            .change_context(RuntimeError::Recovery);

        let err = report.disclose();

        assert_eq!(err.kind(), ErrorKind::Runtime);
        assert_eq!(
            err.report().downcast_ref::<RuntimeError>().copied(),
            Some(RuntimeError::Recovery)
        );
        assert_eq!(
            err.report()
                .downcast_ref::<IoError>()
                .copied()
                .map(IoError::kind),
            Some(IoErrorKind::PermissionDenied)
        );
        assert!(format!("{err:?}").contains("operation=open_recovery_file"));
    }

    #[test]
    fn test_recovery_integrity_report_converts_losslessly_to_public_runtime() {
        let report = Report::new(DataIntegrityError::LogFileCorrupted)
            .attach("phase=replay_redo")
            .change_context(RuntimeError::Recovery);

        let err = report.disclose();

        assert_eq!(err.kind(), ErrorKind::Runtime);
        assert_eq!(
            err.report().downcast_ref::<RuntimeError>().copied(),
            Some(RuntimeError::Recovery)
        );
        assert_eq!(
            err.report().downcast_ref::<DataIntegrityError>().copied(),
            Some(DataIntegrityError::LogFileCorrupted)
        );
        assert!(format!("{err:?}").contains("phase=replay_redo"));
    }

    #[test]
    fn test_storage_error_display_includes_config_detail() {
        let err = Report::new(ConfigError::InvalidIoDepth)
            .attach("recovery_io_depth=0")
            .disclose();

        assert_eq!(
            format!("{err}"),
            "configuration error: invalid io depth: recovery_io_depth=0"
        );
    }

    #[test]
    fn test_storage_error_display_includes_io_detail() {
        let source = StdIoError::new(IoErrorKind::PermissionDenied, "open denied");
        let err = Report::new(IoError::from(source.kind()))
            .attach(format!("op=file_open, {source}"))
            .disclose();

        let output = format!("{err}");
        assert!(output.contains("io error"), "{output}");
        assert!(output.contains("permission denied"), "{output}");
        assert!(output.contains("op=file_open"), "{output}");
        assert!(output.contains("open denied"), "{output}");
    }

    #[test]
    fn test_typed_index_access_context_preserves_lower_error() {
        let lower: InternalResult<()> = Err(Report::new(InternalError::SecondaryIndexOutOfBounds)
            .attach("secondary index key is unavailable"));
        let report = lower
            .change_context(RuntimeError::IndexAccess)
            .attach_with(|| "operation=insert_if_not_exists")
            .unwrap_err();

        assert_eq!(report.current_context(), &RuntimeError::IndexAccess);
        assert_eq!(
            report.downcast_ref::<InternalError>().copied(),
            Some(InternalError::SecondaryIndexOutOfBounds)
        );

        let err = report.attach("secondary index claim").disclose();
        assert_eq!(err.kind(), ErrorKind::Runtime);
        assert_eq!(
            err.report().downcast_ref::<RuntimeError>().copied(),
            Some(RuntimeError::IndexAccess)
        );
        assert_eq!(
            err.report().downcast_ref::<InternalError>().copied(),
            Some(InternalError::SecondaryIndexOutOfBounds)
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

        let err = bridge
            .into_quad(RuntimeError::FileRootAccess)
            .attach("public completion boundary")
            .disclose();
        assert_eq!(err.kind(), ErrorKind::Runtime);
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
            Report::new(InternalError::SecondaryIndexOutOfBounds)
                .attach("internal source, completion owner")
                .change_context(RuntimeError::IndexAccess),
        )
        .replace_context(RuntimeError::BufferPageAccess);
        assert_eq!(
            internal.downcast_ref::<InternalError>().copied(),
            Some(InternalError::SecondaryIndexOutOfBounds)
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
    fn test_completion_bridge_captures_multi_domain_carriers() {
        let operation = CompletionErrorBridge::capture_operation_or_runtime(
            OperationOrRuntimeError::Operation(
                Report::new(OperationError::IndexNotFound).attach("operation carrier"),
            ),
        );
        assert_eq!(
            operation.downcast_ref::<OperationError>().copied(),
            Some(OperationError::IndexNotFound)
        );
        assert!(format!("{operation:?}").contains("operation carrier"));

        let operation_runtime =
            CompletionErrorBridge::capture_operation_or_runtime(OperationOrRuntimeError::Runtime(
                Report::new(RuntimeError::IndexAccess).attach("operation runtime carrier"),
            ));
        assert_eq!(
            operation_runtime.downcast_ref::<RuntimeError>().copied(),
            Some(RuntimeError::IndexAccess)
        );
        assert!(format!("{operation_runtime:?}").contains("operation runtime carrier"));

        let runtime =
            CompletionErrorBridge::capture_runtime_or_fatal(RuntimeOrFatalError::Runtime(
                Report::new(RuntimeError::CatalogAccess).attach("runtime carrier"),
            ));
        assert_eq!(
            runtime.downcast_ref::<RuntimeError>().copied(),
            Some(RuntimeError::CatalogAccess)
        );
        assert!(format!("{runtime:?}").contains("runtime carrier"));

        let fatal = CompletionErrorBridge::capture_runtime_or_fatal(RuntimeOrFatalError::Fatal(
            Report::new(FatalError::Poisoned).attach("fatal carrier"),
        ));
        assert_eq!(
            fatal.downcast_ref::<FatalError>().copied(),
            Some(FatalError::Poisoned)
        );
        assert!(format!("{fatal:?}").contains("fatal carrier"));
    }

    #[test]
    fn completion_bridge_into_quad_preserves_common_outer_domains() {
        let operation = CompletionErrorBridge::capture(
            Report::new(OperationError::IndexNotFound).attach("operation source"),
        )
        .into_quad(RuntimeError::CatalogAccess)
        .attach("operation=create_index, phase=wait_mandatory_completion");
        let QuadError::Operation(operation) = operation else {
            panic!("Operation completion must remain Operation")
        };
        assert_eq!(
            operation.downcast_ref::<OperationError>().copied(),
            Some(OperationError::IndexNotFound)
        );
        assert!(format!("{operation:?}").contains("operation source"));
        assert!(
            format!("{operation:?}")
                .contains("operation=create_index, phase=wait_mandatory_completion")
        );
        assert!(operation.downcast_ref::<QuadError>().is_none());
        assert!(operation.downcast_ref::<CompletionErrorBridge>().is_none());

        let runtime = CompletionErrorBridge::capture(
            Report::new(RuntimeError::IndexAccess).attach("runtime source"),
        )
        .into_quad(RuntimeError::CatalogAccess)
        .attach("operation=create_index, phase=wait_mandatory_completion");
        let QuadError::Runtime(runtime) = runtime else {
            panic!("Runtime completion must remain Runtime")
        };
        assert_eq!(runtime.current_context(), &RuntimeError::IndexAccess);
        assert!(format!("{runtime:?}").contains("runtime source"));
        assert!(
            format!("{runtime:?}")
                .contains("operation=create_index, phase=wait_mandatory_completion")
        );
        assert!(runtime.downcast_ref::<QuadError>().is_none());
        assert!(runtime.downcast_ref::<CompletionErrorBridge>().is_none());

        let lifecycle = CompletionErrorBridge::capture(
            Report::new(LifecycleError::Shutdown).attach("lifecycle source"),
        )
        .into_quad(RuntimeError::CatalogAccess)
        .attach("operation=create_index, phase=wait_mandatory_completion");
        let QuadError::Lifecycle(lifecycle) = lifecycle else {
            panic!("Lifecycle completion must remain Lifecycle")
        };
        assert!(
            format!("{lifecycle:?}")
                .contains("operation=create_index, phase=wait_mandatory_completion")
        );
        assert!(lifecycle.downcast_ref::<QuadError>().is_none());
        assert!(lifecycle.downcast_ref::<CompletionErrorBridge>().is_none());

        let fatal = CompletionErrorBridge::capture(
            Report::new(FatalError::RedoWrite).attach("fatal source"),
        )
        .into_quad(RuntimeError::CatalogAccess)
        .attach("operation=create_index, phase=wait_mandatory_completion");
        let QuadError::Fatal(fatal) = fatal else {
            panic!("Fatal completion must remain Fatal")
        };
        assert!(fatal.downcast_ref::<RuntimeError>().is_none());
        assert!(format!("{fatal:?}").contains("fatal source"));
        assert!(
            format!("{fatal:?}")
                .contains("operation=create_index, phase=wait_mandatory_completion")
        );
        assert!(fatal.downcast_ref::<QuadError>().is_none());
        assert!(fatal.downcast_ref::<CompletionErrorBridge>().is_none());
    }

    #[test]
    fn completion_bridge_into_quad_stacks_physical_roots_under_runtime() {
        let resource = CompletionErrorBridge::capture(
            Report::new(ResourceError::BufferPoolFull).attach("resource source"),
        )
        .into_quad(RuntimeError::TransactionCommit)
        .attach("operation=commit_transaction, phase=wait_redo_group_commit");
        let QuadError::Runtime(resource) = resource else {
            panic!("Resource completion must enter Quad through Runtime")
        };
        assert_eq!(resource.current_context(), &RuntimeError::TransactionCommit);
        assert_eq!(
            resource.downcast_ref::<ResourceError>().copied(),
            Some(ResourceError::BufferPoolFull)
        );
        assert!(
            format!("{resource:?}")
                .contains("operation=commit_transaction, phase=wait_redo_group_commit")
        );
        assert!(resource.downcast_ref::<QuadError>().is_none());
        assert!(resource.downcast_ref::<CompletionErrorBridge>().is_none());

        let io = CompletionErrorBridge::capture(
            Report::new(IoError::from(IoErrorKind::BrokenPipe)).attach("io source"),
        )
        .into_quad(RuntimeError::RedoLogAccess)
        .attach("operation=truncate_redo_log, phase=wait_mandatory_completion");
        let QuadError::Runtime(io) = io else {
            panic!("IO completion must enter Quad through Runtime")
        };
        assert_eq!(io.current_context(), &RuntimeError::RedoLogAccess);
        assert_eq!(
            io.downcast_ref::<IoError>().copied().map(IoError::kind),
            Some(IoErrorKind::BrokenPipe)
        );
        assert!(
            format!("{io:?}")
                .contains("operation=truncate_redo_log, phase=wait_mandatory_completion")
        );
        assert!(io.downcast_ref::<QuadError>().is_none());
        assert!(io.downcast_ref::<CompletionErrorBridge>().is_none());

        let integrity = CompletionErrorBridge::capture(
            Report::new(DataIntegrityError::ChecksumMismatch).attach("integrity source"),
        )
        .into_quad(RuntimeError::Recovery)
        .attach("operation=recover_transaction_system, phase=wait_completion");
        let QuadError::Runtime(integrity) = integrity else {
            panic!("Data-integrity completion must enter Quad through Runtime")
        };
        assert_eq!(integrity.current_context(), &RuntimeError::Recovery);
        assert_eq!(
            integrity.downcast_ref::<DataIntegrityError>().copied(),
            Some(DataIntegrityError::ChecksumMismatch)
        );
        assert!(
            format!("{integrity:?}")
                .contains("operation=recover_transaction_system, phase=wait_completion")
        );
        assert!(integrity.downcast_ref::<QuadError>().is_none());
        assert!(integrity.downcast_ref::<CompletionErrorBridge>().is_none());
    }

    #[test]
    fn test_completion_bridge_runtime_conversion_composes_static_attachment() {
        let bridge = CompletionErrorBridge::capture(
            Report::new(InternalError::SecondaryIndexOutOfBounds)
                .attach("completion source")
                .change_context(RuntimeError::IndexAccess),
        );
        let result: RuntimeOrFatalResult<()> =
            Err(bridge.into_runtime_or_fatal(RuntimeError::TransactionCommit));

        let error = result
            .attach("operation=commit_system_transaction")
            .expect_err("completion failure must remain an error");
        let RuntimeOrFatalError::Runtime(report) = error else {
            panic!("non-Fatal completion must reconstruct as Runtime")
        };

        assert_eq!(report.current_context(), &RuntimeError::TransactionCommit);
        assert_eq!(
            report.downcast_ref::<InternalError>().copied(),
            Some(InternalError::SecondaryIndexOutOfBounds)
        );
        let output = format!("{report:?}");
        assert!(output.contains("completion source"), "{output}");
        assert!(
            output.contains("operation=commit_system_transaction"),
            "{output}"
        );
    }

    #[test]
    fn test_completion_bridge_fatal_conversion_composes_static_attachment() {
        let bridge = CompletionErrorBridge::capture(
            Report::new(IoError::from(IoErrorKind::BrokenPipe))
                .attach("redo completion source")
                .change_context(FatalError::RedoWrite),
        );
        let result: RuntimeOrFatalResult<()> =
            Err(bridge.into_runtime_or_fatal(RuntimeError::TransactionCommit));

        let error = result
            .attach("operation=commit_system_transaction")
            .expect_err("Fatal completion must remain an error");
        let RuntimeOrFatalError::Fatal(report) = error else {
            panic!("Fatal completion must remain Fatal")
        };

        assert_eq!(report.current_context(), &FatalError::RedoWrite);
        assert!(report.downcast_ref::<RuntimeError>().is_none());
        assert_eq!(
            report.downcast_ref::<IoError>().copied().map(IoError::kind),
            Some(IoErrorKind::BrokenPipe)
        );
        let output = format!("{report:?}");
        assert!(output.contains("redo completion source"), "{output}");
        assert!(
            output.contains("operation=commit_system_transaction"),
            "{output}"
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
    fn test_completion_bridge_preserves_fatal_runtime_io_stack() {
        let io_kind = StdIoError::from_raw_os_error(libc::EIO).kind();
        let report = Report::new(IoError::from(io_kind))
            .attach("terminal rollback IO source")
            .change_context(RuntimeError::TableAccess)
            .attach("operation=rollback_row_undo")
            .change_context(FatalError::RollbackAccess)
            .attach("terminal rollback cleanup failed");
        let bridge = CompletionErrorBridge::capture(report);
        let reconstructed = bridge
            .clone()
            .reconstruct_fatal()
            .expect("Fatal completion must reconstruct as Fatal");

        assert_eq!(reconstructed.current_context(), &FatalError::RollbackAccess);
        assert_eq!(
            reconstructed.downcast_ref::<RuntimeError>().copied(),
            Some(RuntimeError::TableAccess)
        );
        assert_eq!(
            reconstructed
                .downcast_ref::<IoError>()
                .copied()
                .map(IoError::kind),
            Some(io_kind)
        );
        assert!(reconstructed.downcast_ref::<ErrorKind>().is_none());

        let public = bridge.into_quad(RuntimeError::TableAccess).disclose();
        assert_eq!(public.kind(), ErrorKind::Fatal);
        assert_eq!(
            public
                .report()
                .frames()
                .filter(|frame| frame.is::<ErrorKind>())
                .count(),
            1
        );
        assert_eq!(
            public.report().downcast_ref::<RuntimeError>().copied(),
            Some(RuntimeError::TableAccess)
        );
        assert_eq!(
            public
                .report()
                .downcast_ref::<IoError>()
                .copied()
                .map(IoError::kind),
            Some(io_kind)
        );
    }

    #[test]
    #[should_panic(expected = "unregistered completion context")]
    fn test_completion_bridge_rejects_public_frame_below_fatal() {
        let report = Report::new(IoError::from(IoErrorKind::Other))
            .attach("terminal rollback IO source")
            .change_context(ErrorKind::Io)
            .change_context(FatalError::RollbackAccess);
        let _ = CompletionErrorBridge::capture(report);
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

        let public = shared
            .into_report()
            .attach("wait for shared fatal completion")
            .disclose();
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
    #[should_panic(expected = "completion report must be linear")]
    fn test_completion_bridge_rejects_branched_report() {
        let mut report = Report::new(FatalError::RedoWrite).expand();
        report.push(Report::new(FatalError::RedoSync));
        let _ = CompletionErrorBridge::capture(report.change_context(FatalError::Poisoned));
    }

    #[test]
    #[should_panic(expected = "unregistered printable completion attachment")]
    fn test_completion_bridge_rejects_unknown_attachment() {
        let report = Report::new(IoError::from(IoErrorKind::Other)).attach(UnknownAttachment);
        let _ = CompletionErrorBridge::capture(report);
    }
}
