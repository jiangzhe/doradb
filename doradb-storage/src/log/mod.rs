pub(crate) mod block_group;
pub(crate) mod format;
mod prefix;
pub(crate) mod redo;
mod seal;

use self::prefix::{LogPrefixEntry, LogPrefixId, LogPrefixKind, LogPrefixTracker};
pub(crate) use self::seal::LogFileSealer;
use crate::conf::TrxSysConfig;
use crate::error::{
    CompletionErrorKind, ConfigError, DataIntegrityError, Error, FatalError, InternalError,
    ResourceError, Result,
};
use crate::file::{SparseFile, UNTRACKED_FILE_ID};
use crate::free_list::FreeList;
use crate::id::TrxID;
use crate::io::{
    CompletedSubmission, Completion, DirectBuf, IOBackend, IOBackendStats, IOBackendStatsHandle,
    IOBuf, IOKind, IOSubmission, Operation, StorageBackend, SubmissionDriver,
};
use crate::log::block_group::{LogBlockGroup, TrxLog};
use crate::log::format::{
    REDO_DEFAULT_DATA_START_OFFSET, REDO_SUPER_BLOCK_SLOT_SIZE, RedoSuperBlock,
    serialize_redo_super_block,
};
use crate::map::FastHashMap;
use crate::trx::MIN_SNAPSHOT_TS;
use crate::trx::group::{
    Commit, CommitGroup, CommitGroupLog, CommitJoin, GroupCommit, MutexGroupCommit,
};
use crate::trx::purge::Purge;
use crate::trx::sys::TransactionSystem;
use crate::trx::{CommittedTrx, FailedPrecommitCleanupJob, FailedPrecommitReason, PrecommitTrx};
use crossbeam_utils::CachePadded;
use error_stack::Report;
use flume::Sender;
use glob::{Pattern, glob};
use parking_lot::{Mutex, MutexGuard};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::io::Result as IoResult;
use std::mem;
use std::os::fd::{AsRawFd, RawFd};
use std::path::{Path, PathBuf};
use std::result::Result as StdResult;
use std::str::{FromStr, from_utf8};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

/// Redo-local owner for a sparse log file and its selected super-block metadata.
pub(crate) struct RedoLogFile {
    file: SparseFile,
    super_block: RedoSuperBlock,
    /// Recovered durable-prefix metadata for a file that must be sealed by the
    /// normal prefix-owned seal path before startup publishes the next header.
    seal_metadata: Option<Box<RedoLogSealMetadata>>,
}

impl RedoLogFile {
    /// Build a redo log file wrapper from an owned sparse file and open header.
    #[inline]
    fn new(file: SparseFile, super_block: RedoSuperBlock) -> Self {
        RedoLogFile {
            file,
            super_block,
            seal_metadata: None,
        }
    }

    /// Attach recovered durable-prefix metadata used by the normal seal path.
    #[inline]
    fn with_seal_metadata(mut self, seal_metadata: RedoLogSealMetadata) -> Self {
        self.seal_metadata = Some(Box::new(seal_metadata));
        self
    }

    /// Allocate an append range in this redo file.
    #[inline]
    pub(crate) fn alloc(&self, len: usize) -> Result<(usize, usize)> {
        self.file.alloc(len)
    }

    /// Return the file sequence encoded in this file's super-block.
    #[inline]
    pub(crate) fn file_seq(&self) -> u32 {
        self.super_block.file_seq
    }

    /// Return a copy of the selected super-block used for inactive-slot sealing.
    #[inline]
    fn super_block(&self) -> RedoSuperBlock {
        self.super_block.clone()
    }
}

impl AsRawFd for RedoLogFile {
    #[inline]
    fn as_raw_fd(&self) -> RawFd {
        self.file.as_raw_fd()
    }
}

/// Target file allocation for one queued redo group write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RedoGroupWriteAlloc {
    /// Redo log file sequence receiving this group.
    pub(crate) file_seq: u32,
    /// File descriptor used for the physical write.
    pub(crate) fd: RawFd,
    /// Starting byte offset of the group write.
    pub(crate) offset: usize,
    /// Ending byte offset after the group write.
    pub(crate) end_offset: usize,
}

/// Allocation and real redo CTS range for one durable redo group write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RedoGroupWriteMeta {
    /// Redo log file sequence receiving this group.
    pub(crate) file_seq: u32,
    /// File descriptor used for the physical write.
    pub(crate) fd: RawFd,
    /// Starting byte offset of the group write.
    pub(crate) offset: usize,
    /// Ending byte offset after the group write.
    pub(crate) end_offset: usize,
    /// Minimum redo CTS serialized in the group.
    pub(crate) min_redo_cts: TrxID,
    /// Maximum redo CTS serialized in the group.
    pub(crate) max_redo_cts: TrxID,
}

/// Durable prefix metadata used to seal a redo file without replaying groups in this process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RedoLogSealMetadata {
    /// Redo log file sequence being sealed.
    pub(crate) file_seq: u32,
    /// Accepted durable end offset written into the sealed super-block.
    pub(crate) durable_end_offset: usize,
    /// Real redo CTS range from accepted complete groups, if any.
    pub(crate) redo_range: Option<(TrxID, TrxID)>,
}

/// Non-queued precommit transaction returned from redo group admission.
pub(crate) struct EnqueuePrecommitError {
    /// Transaction whose commit handoff could not be queued.
    pub(crate) trx: Box<PrecommitTrx>,
    /// Reason reported to the rejected transaction.
    pub(crate) reason: FailedPrecommitReason,
    /// Whether group-commit admission must be closed after this rejection.
    pub(crate) close_admission: bool,
}

/// Writer-assigned identity for one physical redo IO request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LogRequestId(u64);

impl LogRequestId {
    #[inline]
    fn new(raw: u64) -> Self {
        Self(raw)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum LogRequestKind {
    Group,
    Header,
    CommitSync,
    SealWrite,
    SealSync,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LogRequestOwner {
    request_id: LogRequestId,
    prefix_id: Option<LogPrefixId>,
    kind: LogRequestKind,
    group_write_idx: Option<usize>,
}

impl LogRequestOwner {
    #[inline]
    fn header(request_id: LogRequestId, prefix_id: LogPrefixId) -> Self {
        Self {
            request_id,
            prefix_id: Some(prefix_id),
            kind: LogRequestKind::Header,
            group_write_idx: None,
        }
    }

    #[inline]
    fn group(request_id: LogRequestId, prefix_id: LogPrefixId, group_write_idx: usize) -> Self {
        Self {
            request_id,
            prefix_id: Some(prefix_id),
            kind: LogRequestKind::Group,
            group_write_idx: Some(group_write_idx),
        }
    }

    #[inline]
    fn commit_sync(request_id: LogRequestId, prefix_id: LogPrefixId) -> Self {
        Self {
            request_id,
            prefix_id: Some(prefix_id),
            kind: LogRequestKind::CommitSync,
            group_write_idx: None,
        }
    }

    #[inline]
    fn seal_write(request_id: LogRequestId, prefix_id: LogPrefixId) -> Self {
        Self {
            request_id,
            prefix_id: Some(prefix_id),
            kind: LogRequestKind::SealWrite,
            group_write_idx: None,
        }
    }

    #[inline]
    fn seal_sync(request_id: LogRequestId, prefix_id: LogPrefixId) -> Self {
        Self {
            request_id,
            prefix_id: Some(prefix_id),
            kind: LogRequestKind::SealSync,
            group_write_idx: None,
        }
    }
}

struct CreatedLogFile {
    log_file: RedoLogFile,
    header_write: LogWriteSubmission,
}

/// File-system descriptor for one discovered redo log file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RedoLogFileDescriptor {
    /// Sequence number parsed from the 8-hex file suffix.
    pub(crate) file_seq: u32,
    /// Full path to the discovered redo log file.
    pub(crate) path: PathBuf,
}

/// File creation mode for redo startup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RedoLogCreateMode {
    /// Create a new file and fail if it already exists.
    CreateOrFail,
    /// Create or truncate an existing tail file.
    CreateOrTrunc,
}

struct RecoveredRedoSeal {
    path: PathBuf,
    super_block: RedoSuperBlock,
    metadata: RedoLogSealMetadata,
}

/// Value-only finalizer for redo startup repair and the initial writable file.
pub(crate) struct RedoLogFinalizer {
    /// Full file prefix for redo log file names.
    pub(crate) file_prefix: String,
    /// Maximum size of each redo log file.
    pub(crate) file_max_size: usize,
    /// Direct-I/O block size used for redo group writes.
    pub(crate) log_block_size: usize,
    /// Maximum in-flight I/O depth for redo writes.
    pub(crate) log_write_io_depth: usize,
    /// Sequence for the next writable redo file.
    pub(crate) next_file_seq: u32,
    /// How the startup file should be opened.
    create_mode: RedoLogCreateMode,
    /// Optional recovered prefix to seal before the startup header is published.
    recovered_seal: Option<RecoveredRedoSeal>,
}

impl RedoLogFinalizer {
    /// Create a value-only redo log finalizer.
    #[inline]
    pub(crate) fn new(
        file_prefix: String,
        log_write_io_depth: usize,
        file_max_size: usize,
        log_block_size: usize,
        next_file_seq: u32,
    ) -> Self {
        Self {
            file_prefix,
            file_max_size,
            log_block_size,
            log_write_io_depth,
            next_file_seq,
            create_mode: RedoLogCreateMode::CreateOrFail,
            recovered_seal: None,
        }
    }

    /// Select the runtime active file sequence and creation mode.
    #[inline]
    pub(crate) fn set_startup_file(&mut self, file_seq: u32, create_mode: RedoLogCreateMode) {
        self.next_file_seq = file_seq;
        self.create_mode = create_mode;
    }

    /// Queue a recovered redo file for normal prefix-owned sealing during startup.
    #[inline]
    pub(crate) fn set_recovered_seal(
        &mut self,
        path: PathBuf,
        super_block: RedoSuperBlock,
        durable_end_offset: usize,
        redo_range: Option<(TrxID, TrxID)>,
    ) {
        debug_assert!(self.recovered_seal.is_none());
        let file_seq = super_block.file_seq;
        self.recovered_seal = Some(RecoveredRedoSeal {
            path,
            super_block,
            metadata: RedoLogSealMetadata {
                file_seq,
                durable_end_offset,
                redo_range,
            },
        });
    }

    /// Finalize startup redo state and return the live redo log.
    #[inline]
    pub(crate) fn finalize(
        self,
        purge_tx: Sender<Purge>,
    ) -> Result<(RedoLog, Arc<Completion<()>>)> {
        let ctx = StorageBackend::new(self.log_write_io_depth)?;
        let mut file_seq = self.next_file_seq;
        let (
            CreatedLogFile {
                log_file,
                header_write,
            },
            header_completion,
        ) = create_log_file_with_header_completion_with_mode(
            &self.file_prefix,
            file_seq,
            self.file_max_size,
            self.log_block_size,
            self.create_mode,
        )?;
        file_seq = next_redo_file_seq(file_seq)?;

        let ended_log_file = self
            .recovered_seal
            .map(open_recovered_seal_file)
            .transpose()?;
        let mut queue = VecDeque::new();
        queue.push_back(Commit::LogFileBoundary {
            ended_log_file,
            header_write,
        });
        let group_commit = GroupCommit {
            queue,
            closed: None,
            log_file: Some(log_file),
        };
        let io_backend_stats = ctx.stats_handle();
        Ok((
            RedoLog {
                group_commit: CachePadded::new(MutexGroupCommit::new(group_commit)),
                persisted_cts: CachePadded::new(AtomicU64::new(MIN_SNAPSHOT_TS.as_u64())),
                stats: Arc::new(CachePadded::new(RedoLogStats::default())),
                purge_tx,
                log_write_backend: CachePadded::new(Mutex::new(Some(ctx))),
                io_backend_stats,
                log_block_size: self.log_block_size,
                file_prefix: self.file_prefix,
                file_seq: AtomicU32::new(file_seq),
                file_max_size: self.file_max_size,
                buf_free_list: FreeList::new(
                    self.log_write_io_depth,
                    self.log_write_io_depth * 2,
                    move || DirectBuf::zeroed(self.log_block_size),
                ),
            },
            header_completion,
        ))
    }
}

/// Pending redo write submitted to the storage backend.
pub(crate) struct LogWriteSubmission {
    owner: Option<LogRequestOwner>,
    kind: LogWriteKind,
    operation: Operation,
}

impl LogWriteSubmission {
    /// Create one fixed-block group write submission.
    #[inline]
    pub(crate) fn group(fd: RawFd, offset: usize, buf: DirectBuf, group_write_idx: usize) -> Self {
        LogWriteSubmission {
            owner: None,
            kind: LogWriteKind::Group { group_write_idx },
            operation: Operation::pwrite_owned(fd, offset, buf),
        }
    }

    #[inline]
    fn header(fd: RawFd, offset: usize, buf: DirectBuf) -> (Self, Arc<Completion<()>>) {
        let completion = Arc::new(Completion::new());
        let submission = LogWriteSubmission {
            owner: None,
            kind: LogWriteKind::Header {
                completion: Arc::clone(&completion),
            },
            operation: Operation::pwrite_owned(fd, offset, buf),
        };
        (submission, completion)
    }

    #[inline]
    fn seal(log_file: RedoLogFile, offset: usize, buf: DirectBuf) -> Self {
        let fd = log_file.as_raw_fd();
        LogWriteSubmission {
            owner: None,
            kind: LogWriteKind::SealWrite {
                log_file: Box::new(log_file),
            },
            operation: Operation::pwrite_owned(fd, offset, buf),
        }
    }

    #[inline]
    fn commit_sync(fd: RawFd, log_sync: LogSync) -> Self {
        LogWriteSubmission {
            owner: None,
            kind: LogWriteKind::CommitSync,
            operation: sync_operation(fd, log_sync),
        }
    }

    #[inline]
    fn seal_sync(log_file: RedoLogFile, log_sync: LogSync) -> Self {
        let fd = log_file.as_raw_fd();
        LogWriteSubmission {
            owner: None,
            kind: LogWriteKind::SealSync {
                log_file: Box::new(log_file),
            },
            operation: sync_operation(fd, log_sync),
        }
    }

    #[inline]
    fn standalone_sync(fd: RawFd, log_sync: LogSync) -> Option<Self> {
        match log_sync {
            LogSync::None => None,
            LogSync::Fsync | LogSync::Fdatasync => Some(LogWriteSubmission {
                owner: None,
                kind: LogWriteKind::StandaloneSync,
                operation: sync_operation(fd, log_sync),
            }),
        }
    }

    #[inline]
    fn owner(&self) -> Option<LogRequestOwner> {
        self.owner
    }

    #[inline]
    fn attach_owner(&mut self, owner: LogRequestOwner) {
        debug_assert!(self.owner.is_none() || self.owner == Some(owner));
        self.owner = Some(owner);
    }

    #[inline]
    fn header_completion(&self) -> Option<Arc<Completion<()>>> {
        match &self.kind {
            LogWriteKind::Header { completion } => Some(Arc::clone(completion)),
            LogWriteKind::Group { .. }
            | LogWriteKind::CommitSync
            | LogWriteKind::SealWrite { .. }
            | LogWriteKind::SealSync { .. }
            | LogWriteKind::StandaloneSync => None,
        }
    }

    #[inline]
    fn fail_unsubmitted_header(mut self, reason: FatalError) {
        let _ = self.operation.take_buf();
        if let LogWriteKind::Header { completion } = self.kind {
            completion.complete(Err(CompletionErrorKind::report_fatal(
                reason,
                "redo header write was not submitted",
            )));
        }
    }
}

impl IOSubmission for LogWriteSubmission {
    #[inline]
    fn operation(&mut self) -> &mut Operation {
        &mut self.operation
    }
}

enum LogWriteKind {
    Group { group_write_idx: usize },
    Header { completion: Arc<Completion<()>> },
    CommitSync,
    SealWrite { log_file: Box<RedoLogFile> },
    SealSync { log_file: Box<RedoLogFile> },
    StandaloneSync,
}

struct LogWriteCompletion {
    owner: Option<LogRequestOwner>,
    kind: LogWriteKind,
    buf: Option<DirectBuf>,
    poison: Option<FatalError>,
}

/// Driver wrapper for redo log write submissions.
pub(crate) struct LogWriteDriver<B = StorageBackend>
where
    B: IOBackend,
{
    driver: SubmissionDriver<LogWriteSubmission, B>,
}

impl<B> LogWriteDriver<B>
where
    B: IOBackend,
{
    #[inline]
    fn new(backend: B) -> Self {
        LogWriteDriver {
            driver: SubmissionDriver::new(backend),
        }
    }

    #[inline]
    fn capacity(&self) -> usize {
        self.driver.capacity()
    }

    #[inline]
    fn available_capacity(&self) -> usize {
        self.driver.available_capacity()
    }

    #[inline]
    fn pending_len(&self) -> usize {
        self.driver.pending_len()
    }

    #[inline]
    fn submitted_len(&self) -> usize {
        self.driver.submitted_len()
    }

    #[inline]
    fn push_write(&mut self, submission: LogWriteSubmission) -> StdResult<(), LogWriteSubmission> {
        self.driver.push(submission)
    }

    #[inline]
    fn submit_ready(&mut self) -> usize {
        self.driver.submit_ready()
    }

    #[inline]
    fn wait_at_least_one(&mut self) -> LogWriteCompletion {
        log_write_completion_from_completed(self.driver.wait_at_least_one())
    }

    #[inline]
    fn try_pop_buffered_completion(&mut self) -> Option<LogWriteCompletion> {
        self.driver
            .try_pop_completed()
            .map(log_write_completion_from_completed)
    }
}

/// Shared redo log state used by commit admission and the log writer.
pub(crate) struct RedoLog {
    /// Group commit state for the redo log.
    pub(crate) group_commit: CachePadded<MutexGroupCommit>,
    /// Maximum ordered-completed CTS of this redo log.
    ///
    /// Durability-required groups advance this after redo write/sync completes.
    /// No-log groups also advance it after ordered completion, but recovery
    /// still seeds timestamps only from checkpoint metadata, table roots, and
    /// redo headers.
    pub(crate) persisted_cts: CachePadded<AtomicU64>,
    /// Stats of transaction system.
    pub(crate) stats: Arc<CachePadded<RedoLogStats>>,
    /// Purge coordinator channel used for committed transaction GC handoff.
    pub(crate) purge_tx: Sender<Purge>,
    /// Backend for redo writes, taken exactly once by the log thread.
    log_write_backend: CachePadded<Mutex<Option<StorageBackend>>>,
    /// Backend-owned submit/wait statistics for redo writes.
    io_backend_stats: IOBackendStatsHandle,
    /// Fixed byte size of every redo data-block write.
    pub(crate) log_block_size: usize,
    /// Log file prefix for the single redo file family.
    pub(crate) file_prefix: String,
    /// Sequence of current file in this redo log, starting from 0.
    pub(crate) file_seq: AtomicU32,
    /// Maximum size of single log file.
    pub(crate) file_max_size: usize,
    /// Free list of reusable fixed-block write buffers returned by completed I/O.
    pub(crate) buf_free_list: FreeList<DirectBuf>,
}

impl RedoLog {
    /// Create a logical fixed-block group for one transaction's redo log.
    #[inline]
    fn new_log_group(&self, data: TrxLog) -> Result<LogBlockGroup> {
        LogBlockGroup::new(self.log_block_size, data)
    }

    #[inline]
    fn rotate_log_file(&self, group_commit_g: &mut MutexGuard<'_, GroupCommit>) -> Result<()> {
        let Some(old_log_file) = group_commit_g.log_file.take() else {
            return Err(Error::from(
                Report::new(InternalError::Generic)
                    .attach("redo log rotation requires current log file"),
            ));
        };
        let CreatedLogFile {
            log_file: new_log_file,
            header_write,
        } = match self.create_log_file() {
            Ok(created) => created,
            Err(err) => {
                group_commit_g.log_file = Some(old_log_file);
                return Err(err);
            }
        };
        group_commit_g.log_file = Some(new_log_file);
        group_commit_g.queue.push_back(Commit::LogFileBoundary {
            ended_log_file: Some(old_log_file),
            header_write,
        });
        Ok(())
    }

    #[inline]
    fn create_log_file(&self) -> Result<CreatedLogFile> {
        let file_seq = self.file_seq.fetch_add(1, Ordering::SeqCst);
        create_log_file(
            &self.file_prefix,
            file_seq,
            self.file_max_size,
            self.log_block_size,
        )
    }

    /// Enqueue a prepared transaction into group commit and return its optional sync waiter.
    #[inline]
    pub(crate) fn enqueue_precommit_group(
        &self,
        mut trx: PrecommitTrx,
        group_commit_g: &mut MutexGuard<'_, GroupCommit>,
        wait_sync: bool,
    ) -> StdResult<CommitJoin, EnqueuePrecommitError> {
        let cts = trx.cts;
        let log = if let Some(redo_bin) = trx.take_log() {
            let log_group = match self.new_log_group(redo_bin) {
                Ok(log_group) => log_group,
                Err(_) => {
                    return Err(EnqueuePrecommitError {
                        trx: Box::new(trx),
                        reason: FailedPrecommitReason::Fatal(FatalError::RedoWrite),
                        close_admission: true,
                    });
                }
            };
            let Some(log_file) = group_commit_g.log_file.as_ref() else {
                return Err(EnqueuePrecommitError {
                    trx: Box::new(trx),
                    reason: FailedPrecommitReason::Fatal(FatalError::RedoWrite),
                    close_admission: true,
                });
            };
            let group_physical_len = log_group.physical_len();
            let (file_seq, fd, offset, end_offset) = match log_file.alloc(group_physical_len) {
                Ok((offset, end_offset)) => (
                    log_file.file_seq(),
                    log_file.as_raw_fd(),
                    offset,
                    end_offset,
                ),
                Err(err)
                    if err.resource_error() == Some(ResourceError::StorageFileCapacityExceeded) =>
                {
                    // Rotate log file and try again.
                    if self.rotate_log_file(group_commit_g).is_err() {
                        return Err(EnqueuePrecommitError {
                            trx: Box::new(trx),
                            reason: FailedPrecommitReason::Fatal(FatalError::RedoWrite),
                            close_admission: true,
                        });
                    }

                    let Some(new_log_file) = group_commit_g.log_file.as_ref() else {
                        return Err(EnqueuePrecommitError {
                            trx: Box::new(trx),
                            reason: FailedPrecommitReason::Fatal(FatalError::RedoWrite),
                            close_admission: true,
                        });
                    };
                    match new_log_file.alloc(group_physical_len) {
                        Ok((offset, end_offset)) => (
                            new_log_file.file_seq(),
                            new_log_file.as_raw_fd(),
                            offset,
                            end_offset,
                        ),
                        Err(err)
                            if err.resource_error()
                                == Some(ResourceError::StorageFileCapacityExceeded) =>
                        {
                            return Err(EnqueuePrecommitError {
                                trx: Box::new(trx),
                                reason: FailedPrecommitReason::Resource(
                                    ResourceError::StorageFileCapacityExceeded,
                                ),
                                close_admission: false,
                            });
                        }
                        Err(_) => {
                            return Err(EnqueuePrecommitError {
                                trx: Box::new(trx),
                                reason: FailedPrecommitReason::Fatal(FatalError::RedoWrite),
                                close_admission: true,
                            });
                        }
                    }
                }
                Err(_) => {
                    return Err(EnqueuePrecommitError {
                        trx: Box::new(trx),
                        reason: FailedPrecommitReason::Fatal(FatalError::RedoWrite),
                        close_admission: true,
                    });
                }
            };
            Some(CommitGroupLog {
                alloc: RedoGroupWriteAlloc {
                    file_seq,
                    fd,
                    offset,
                    end_offset,
                },
                group: log_group,
            })
        } else {
            None
        };
        // This is the commit handoff boundary for user transactions. Once the
        // precommit transaction is queued, the log thread owns session
        // commit/rollback completion through PrecommitTrx::commit or failed
        // precommit cleanup. The
        // user future may wait for the result, but dropping it must not make
        // rollback a competing terminal outcome or leave the session active.
        let completion = Arc::new(Completion::new());
        let waiter = wait_sync.then(|| Arc::clone(&completion));
        let new_group = CommitGroup {
            trx_list: vec![trx],
            max_cts: cts,
            log,
            completion,
        };
        group_commit_g.queue.push_back(Commit::Group(new_group));
        Ok(waiter)
    }

    #[inline]
    fn update_stats(
        &self,
        trx_count: usize,
        commit_count: usize,
        log_bytes: usize,
        sync_count: usize,
        sync_nanos: usize,
    ) {
        self.stats.trx_count.fetch_add(trx_count, Ordering::Relaxed);
        self.stats
            .commit_count
            .fetch_add(commit_count, Ordering::Relaxed);
        self.stats.log_bytes.fetch_add(log_bytes, Ordering::Relaxed);
        self.stats
            .sync_count
            .fetch_add(sync_count, Ordering::Relaxed);
        self.stats
            .sync_nanos
            .fetch_add(sync_nanos, Ordering::Relaxed);
    }

    /// Take ownership of the redo write driver backend for the log thread.
    #[inline]
    pub(crate) fn take_log_write_driver(&self) -> LogWriteDriver {
        let backend = self
            .log_write_backend
            .lock()
            .take()
            .expect("redo log write backend must exist before startup");
        LogWriteDriver::new(backend)
    }

    /// Return a snapshot of redo write backend statistics.
    #[inline]
    pub(crate) fn io_backend_stats(&self) -> IOBackendStats {
        self.io_backend_stats.snapshot()
    }
}

/// Logical commit group state while redo requests and sync are being processed.
pub(crate) struct SyncGroup {
    /// Transactions covered by this ordered commit group.
    pub(crate) trx_list: Vec<PrecommitTrx>,
    /// Maximum commit timestamp in the group.
    ///
    /// This is transaction publication metadata, not physical IO identity.
    pub(crate) max_cts: TrxID,
    /// Serialized redo byte count for the group.
    pub(crate) log_bytes: usize,
    /// Redo-bearing groups keep the borrowed log fd for the final fsync. No-log
    /// groups have no file allocation and therefore no sync target.
    pub(crate) log_fd: Option<RawFd>,
    /// Physical write metadata recorded after the group becomes durable.
    pub(crate) write_meta: Option<RedoGroupWriteMeta>,
    /// Pending fixed-block write submissions for this logical group.
    pub(crate) writes: VecDeque<LogWriteSubmission>,
    /// Buffers returned by completed redo write I/O.
    pub(crate) returned_bufs: Vec<DirectBuf>,
    /// Completion notified when commit or cleanup finishes.
    pub(crate) completion: Arc<Completion<()>>,
    /// Number of physical write requests still owned by the backend driver.
    pub(crate) outstanding_requests: usize,
    /// Redo logging is sequential: once one group fails, this group and every
    /// later group cannot be part of the durable prefix. Submitted IO may still
    /// complete later, but completion only returns buffers for recycling.
    pub(crate) failure_reason: Option<FailedPrecommitReason>,
}

impl SyncGroup {
    #[inline]
    fn take_submission(&mut self) -> Option<LogWriteSubmission> {
        self.writes.pop_front()
    }

    #[inline]
    fn restore_submission(&mut self, submission: LogWriteSubmission) {
        self.writes.push_front(submission);
    }

    #[inline]
    fn mark_request_submitted(&mut self) {
        self.outstanding_requests += 1;
    }

    #[inline]
    fn finish_request(&mut self, buf: DirectBuf) {
        self.outstanding_requests = self
            .outstanding_requests
            .checked_sub(1)
            .expect("redo group completion must match outstanding request count");
        self.returned_bufs.push(buf);
    }

    #[inline]
    fn ready(&self) -> bool {
        self.writes.is_empty() && self.outstanding_requests == 0
    }

    #[inline]
    fn drain_buffers(&mut self) -> Vec<DirectBuf> {
        let mut bufs = mem::take(&mut self.returned_bufs);
        for submission in &mut self.writes {
            if let Some(buf) = submission.operation.take_buf() {
                bufs.push(buf);
            }
        }
        self.writes.clear();
        bufs
    }

    #[inline]
    fn fail_waiters(&mut self, trx_sys: &TransactionSystem, reason: FailedPrecommitReason) {
        if self.trx_list.is_empty() {
            self.failure_reason.get_or_insert(reason);
            return;
        }
        self.failure_reason.get_or_insert(reason);
        let trx_list = mem::take(&mut self.trx_list);
        let completion = Arc::clone(&self.completion);
        trx_sys.request_failed_precommit_cleanup(FailedPrecommitCleanupJob::new(
            trx_list, completion, reason,
        ));
    }
}

/// Atomic counters maintained by the redo log writer.
#[derive(Default)]
pub(crate) struct RedoLogStats {
    /// Number of commit groups completed.
    pub(crate) commit_count: AtomicUsize,
    /// Number of transactions completed through redo.
    pub(crate) trx_count: AtomicUsize,
    /// Total redo bytes written.
    pub(crate) log_bytes: AtomicUsize,
    /// Number of redo file sync calls.
    pub(crate) sync_count: AtomicUsize,
    /// Total nanoseconds spent in redo file sync calls.
    pub(crate) sync_nanos: AtomicUsize,
    /// Number of best-effort redo file seal failures.
    pub(crate) seal_failure_count: AtomicUsize,
    /// Number of transactions handed to purge.
    pub(crate) purge_trx_count: AtomicUsize,
    /// Number of row versions purged.
    pub(crate) purge_row_count: AtomicUsize,
    /// Number of index entries purged.
    pub(crate) purge_index_count: AtomicUsize,
}

#[derive(Default)]
struct ReadyGroupPrefix {
    /// Groups whose redo bytes form one durable publication prefix.
    written: Vec<SyncGroup>,
    /// Ready groups after the first failed group; these are cleanup-only.
    failed: Vec<SyncGroup>,
    /// Last prefix entry id drained into this batch. A front sync barrier
    /// reuses this id to keep live prefix ids contiguous for O(1) lookup.
    sync_barrier_id: Option<LogPrefixId>,
    trx_count: usize,
    commit_count: usize,
    log_bytes: usize,
    /// Redo fd for `written`. A publish batch cannot span log files.
    log_fd: Option<RawFd>,
    failure_reason: Option<FailedPrecommitReason>,
}

/// Durability mode for syncing redo log writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum LogSync {
    #[default]
    #[serde(rename = "none")]
    None,
    #[serde(rename = "fsync")]
    Fsync,
    #[serde(rename = "fdatasync")]
    Fdatasync,
}

impl FromStr for LogSync {
    type Err = Error;

    #[inline]
    fn from_str(s: &str) -> StdResult<Self, Self::Err> {
        if s.eq_ignore_ascii_case("fsync") {
            Ok(LogSync::Fsync)
        } else if s.eq_ignore_ascii_case("fdatasync") {
            Ok(LogSync::Fdatasync)
        } else if s.eq_ignore_ascii_case("none") {
            Ok(LogSync::None)
        } else {
            Err(Report::new(ConfigError::InvalidLogSync)
                .attach(format!("value={s}"))
                .into())
        }
    }
}

/// Processes redo write and sync work for commit groups.
pub(crate) struct RedoLogWriter<'a, B = StorageBackend>
where
    B: IOBackend,
{
    trx_sys: &'a TransactionSystem,
    write_driver: &'a mut LogWriteDriver<B>,
    log_block_size: usize,
    prefix: LogPrefixTracker,
    next_request_id: u64,
    log_sync: LogSync,
    shutdown: bool,
}

impl<'a, B> RedoLogWriter<'a, B>
where
    B: IOBackend,
{
    /// Create a redo log writer bound to a transaction system and write driver.
    #[inline]
    pub(crate) fn new(
        trx_sys: &'a TransactionSystem,
        config: &TrxSysConfig,
        write_driver: &'a mut LogWriteDriver<B>,
    ) -> Self {
        RedoLogWriter {
            trx_sys,
            write_driver,
            log_block_size: config.log_block_size.as_u64() as usize,
            prefix: LogPrefixTracker::new(),
            next_request_id: 0,
            log_sync: config.log_sync,
            shutdown: false,
        }
    }

    #[inline]
    fn next_request_id(&mut self) -> LogRequestId {
        let request_id = LogRequestId::new(self.next_request_id);
        self.next_request_id = self
            .next_request_id
            .checked_add(1)
            .expect("redo request id overflow");
        request_id
    }

    #[inline]
    fn take_reusable_bufs(&self, count: usize) -> Vec<DirectBuf> {
        self.trx_sys.redo_log.buf_free_list.pop_batch(count)
    }

    #[inline]
    fn recycle_bufs(&self, bufs: Vec<DirectBuf>) {
        let mut reusable = Vec::with_capacity(bufs.len());
        for mut buf in bufs {
            if buf.capacity() != self.log_block_size {
                continue;
            }
            buf.reset();
            reusable.push(buf);
        }
        if !reusable.is_empty() {
            self.trx_sys.redo_log.buf_free_list.push_batch(reusable);
        }
    }

    #[inline]
    fn fail_queued_group(&self, mut group: CommitGroup, reason: FailedPrecommitReason) {
        if group.trx_list.is_empty() {
            return;
        }
        let trx_list = mem::take(&mut group.trx_list);
        let completion = Arc::clone(&group.completion);
        self.trx_sys
            .request_failed_precommit_cleanup(FailedPrecommitCleanupJob::new(
                trx_list, completion, reason,
            ));
    }

    #[inline]
    fn fail_sync_group(&self, sync_group: &mut SyncGroup, reason: FailedPrecommitReason) {
        sync_group.fail_waiters(self.trx_sys, reason);
        self.recycle_bufs(sync_group.drain_buffers());
    }

    /// Fail all pending redo work after a fatal storage error.
    #[inline]
    pub(crate) fn fail_pending(&mut self, _sealer: &mut LogFileSealer, err: Report<FatalError>) {
        self.shutdown = true;
        let fatal = *err.current_context();
        let reason = FailedPrecommitReason::Fatal(fatal);
        self.fail_prefix_entries(fatal);

        let mut queued = Vec::new();
        {
            let mut group_commit_g = self.trx_sys.redo_log.group_commit.lock();
            group_commit_g.close(reason);
            while let Some(commit) = group_commit_g.queue.pop_front() {
                match commit {
                    Commit::Group(group) => queued.push(group),
                    Commit::LogFileBoundary {
                        ended_log_file,
                        header_write,
                    } => {
                        header_write.fail_unsubmitted_header(fatal);
                        drop(ended_log_file);
                    }
                    Commit::Shutdown => {}
                }
            }
        }
        for group in queued {
            self.fail_queued_group(group, reason);
        }
    }

    #[inline]
    fn fail_prefix_entries(&mut self, fatal: FatalError) {
        let reason = FailedPrecommitReason::Fatal(fatal);
        let mut recycle = Vec::new();
        for entry in &mut self.prefix.entries {
            match &mut entry.kind {
                LogPrefixKind::Header {
                    write,
                    ready,
                    failure,
                    ..
                } => {
                    if let Some(mut submission) = write.take() {
                        let _ = submission.operation.take_buf();
                        *ready = true;
                    }
                    if !*ready || failure.is_none() {
                        *failure = Some(fatal);
                    }
                }
                LogPrefixKind::Group { group } => {
                    group.fail_waiters(self.trx_sys, reason);
                    recycle.extend(group.drain_buffers());
                    group.failure_reason.get_or_insert(reason);
                }
                LogPrefixKind::Sync {
                    ready_prefix,
                    sync,
                    ready,
                    failure,
                    ..
                } => {
                    if sync.take().is_some() {
                        *ready = true;
                    }
                    fail_ready_prefix_waiters(self.trx_sys, ready_prefix, reason, &mut recycle);
                    if !*ready || failure.is_none() {
                        *failure = Some(fatal);
                    }
                }
                LogPrefixKind::Seal {
                    log_file,
                    write,
                    sync,
                    ready,
                    failure,
                } => {
                    if log_file.take().is_some() {
                        *ready = true;
                    }
                    if let Some(mut submission) = write.take() {
                        let _ = submission.operation.take_buf();
                        *ready = true;
                    }
                    if sync.take().is_some() {
                        *ready = true;
                    }
                    if !*ready || failure.is_none() {
                        *failure = Some(fatal);
                    }
                }
            }
        }
        self.recycle_bufs(recycle);
    }

    #[inline]
    fn shrink_prefix_if_sparse(&mut self) {
        self.prefix.shrink_if_sparse(self.write_driver.capacity());
    }

    /// Process redo prefix work until shutdown.
    #[inline]
    pub(crate) fn process_until_shutdown(&mut self, sealer: &mut LogFileSealer) {
        loop {
            debug_assert!(
                self.prefix.len() + self.prefix.driver_owned_len()
                    >= self.write_driver.pending_len(),
                "queued and inflight redo work should cover all driver-owned work"
            );
            // If shutdown flag is set, we still submit and finish all pending IOs,
            // but do not accept any new IO requests.
            if !self.shutdown {
                self.fetch_io_reqs();
                // Newly fetched prefix entries may already be publishable, or
                // may need to prepare front seal/sync submissions before
                // `submit_io` can make progress.
                self.advance_ordered_prefix(sealer);
            }
            self.submit_io();
            // This may wait for rotated-file seal I/O even when no prefix
            // group/header write is submitted. Rotated-file sealing is required
            // maintenance for ended files: completion performs the configured
            // seal sync and reports fatal errors. Only active-file sealing during
            // clean shutdown is best-effort.
            self.wait_and_drain_io_if_submitted(sealer);
            // Completion handling only marks prefix entries ready; advance the
            // ordered prefix again to publish completed work or prepare the
            // next barrier made reachable by completion.
            self.advance_ordered_prefix(sealer);
            self.shrink_prefix_if_sparse();

            if self.shutdown && self.prefix.is_empty() {
                return;
            }
        }
    }

    /// Return whether this writer still owns pending group or header I/O.
    #[inline]
    pub(crate) fn has_pending_io(&self) -> bool {
        !self.prefix.is_empty()
    }

    /// Return whether the writer has entered shutdown drain mode.
    #[inline]
    pub(crate) fn shutdown(&self) -> bool {
        self.shutdown
    }

    /// Fetch queued commit work into the logical redo prefix.
    #[inline]
    fn fetch_io_reqs(&mut self) {
        if self.write_driver.pending_len() == 0 && self.prefix.is_empty() {
            // there is no processing IO, so we can block on waiting for next request.
            self.fetch_io_reqs_internal()
        } else {
            // only try non-blocking way to fetch incoming requests, because we also
            // need to finish previous IO.
            self.try_fetch_io_reqs_internal()
        }
    }

    #[inline]
    fn fetch_io_reqs_internal(&mut self) {
        loop {
            let mut group_commit_g = self.trx_sys.redo_log.group_commit.lock();
            let mut fetched = false;
            loop {
                match group_commit_g.queue.pop_front() {
                    None => break,
                    Some(Commit::Shutdown) => {
                        self.shutdown = true;
                        return;
                    }
                    Some(Commit::LogFileBoundary {
                        ended_log_file,
                        header_write,
                    }) => {
                        if let Some(log_file) = ended_log_file {
                            self.prefix.push_seal(log_file);
                        }
                        self.prefix.push_header(header_write);
                        fetched = true;
                    }
                    Some(Commit::Group(cg)) => {
                        let sync_group = cg.into_sync_group(|count| self.take_reusable_bufs(count));
                        self.prefix.push_group(sync_group);
                        fetched = true;
                    }
                }
            }
            if fetched {
                return;
            }
            self.trx_sys
                .redo_log
                .group_commit
                .wait_for(&mut group_commit_g, Duration::from_secs(1));
        }
    }

    #[inline]
    fn try_fetch_io_reqs_internal(&mut self) {
        // Single thread perform IO so here we only need to use sync mutex.
        let mut group_commit_g = self.trx_sys.redo_log.group_commit.lock();
        loop {
            match group_commit_g.queue.pop_front() {
                None => {
                    return;
                }
                Some(Commit::Shutdown) => {
                    self.shutdown = true;
                    return;
                }
                Some(Commit::LogFileBoundary {
                    ended_log_file,
                    header_write,
                }) => {
                    if let Some(log_file) = ended_log_file {
                        self.prefix.push_seal(log_file);
                    }
                    self.prefix.push_header(header_write);
                }
                Some(Commit::Group(cg)) => {
                    let sync_group = cg.into_sync_group(|count| self.take_reusable_bufs(count));
                    self.prefix.push_group(sync_group);
                }
            }
        }
    }

    /// Advance the ordered prefix until the front entry blocks.
    ///
    /// This consumes ready barriers, publishes ready no-sync group prefixes,
    /// prepares front seal work, or turns a ready redo-bearing group prefix into
    /// a sync barrier. It returns when the front entry has no immediate progress.
    ///
    /// Redo durability is sequential: this stops at the first unfinished group,
    /// and a failed group makes every later group cleanup-only even if its
    /// individual write has completed.
    #[inline]
    fn advance_ordered_prefix(&mut self, sealer: &mut LogFileSealer) {
        loop {
            let Some(front) = self.prefix.entries.front() else {
                return;
            };
            match &front.kind {
                LogPrefixKind::Header { ready, .. } => {
                    if !ready {
                        return;
                    }
                    // Header writes are barriers between files. Complete the
                    // header before any later group can be published.
                    let entry = self
                        .prefix
                        .pop_front()
                        .expect("front redo header prefix entry must exist");
                    finish_header_prefix_entry(entry);
                }
                LogPrefixKind::Seal { ready, .. } => {
                    if !ready {
                        // A rotated-file seal is prepared only after it reaches
                        // the front of the ordered prefix. Until the old file's
                        // seal write and configured sync complete, later header
                        // and group entries must stay unpublished.
                        self.prepare_front_seal_barrier(sealer);
                        return;
                    }
                    let entry = self
                        .prefix
                        .pop_front()
                        .expect("front redo seal prefix entry must exist");
                    finish_seal_prefix_entry(entry);
                }
                LogPrefixKind::Group { group } => {
                    if !group.ready() {
                        return;
                    }
                    // A ready group prefix is the only point where transaction
                    // waiters can be completed.
                    self.advance_ready_group_prefix(sealer);
                }
                LogPrefixKind::Sync { ready, .. } => {
                    if !ready {
                        return;
                    }
                    let entry = self
                        .prefix
                        .pop_front()
                        .expect("front redo sync prefix entry must exist");
                    self.advance_sync_prefix_entry(sealer, entry);
                }
            }
        }
    }

    /// Prepare the front rotated-file seal barrier for submission.
    ///
    /// This is invoked from `advance_ordered_prefix` only when a seal entry is
    /// the first prefix entry and is not ready yet. Preparing it at the front,
    /// instead of when it is enqueued, keeps seal metadata aligned with the
    /// already-durable group prefix and makes the seal an ordered publication
    /// barrier before the next file's header/groups can complete.
    #[inline]
    fn prepare_front_seal_barrier(&mut self, sealer: &mut LogFileSealer) {
        let Some(entry) = self.prefix.entries.front_mut() else {
            return;
        };
        let LogPrefixKind::Seal {
            log_file,
            write,
            sync,
            ready,
            failure,
        } = &mut entry.kind
        else {
            unreachable!("front prefix entry must be a seal")
        };
        if *ready || write.is_some() || sync.is_some() || failure.is_some() {
            return;
        }
        // The log file is consumed exactly once: after the header write is
        // built, the in-flight submission owns it until completion marks this
        // prefix entry ready.
        let Some(log_file) = log_file.take() else {
            return;
        };
        match sealer.prepare_prefix_seal(log_file) {
            Ok(submission) => {
                *write = Some(submission);
            }
            Err(reason) => {
                *failure = Some(reason);
                *ready = true;
                let err = self.trx_sys.poison_storage(reason);
                self.fail_pending(sealer, err);
            }
        }
    }

    #[inline]
    fn advance_ready_group_prefix(&mut self, sealer: &mut LogFileSealer) {
        let mut ready = self.prefix.drain_ready_group_prefix();

        if let Some(reason) = ready.failure_reason {
            // Redo is sequential: the first failed group turns every later
            // queued group into cleanup-only work even if its own write is
            // already complete.
            for entry in &mut self.prefix.entries {
                if let LogPrefixKind::Group { group } = &mut entry.kind {
                    group.fail_waiters(self.trx_sys, reason);
                    group.failure_reason.get_or_insert(reason);
                }
            }
            for sync_group in &mut ready.failed {
                self.fail_sync_group(sync_group, reason);
            }
        }

        if ready.written.is_empty() {
            return;
        }

        if ready.log_bytes == 0 || self.log_sync == LogSync::None {
            self.publish_ready_group_prefix(sealer, ready, 0);
            return;
        }

        let log_fd = ready
            .log_fd
            .expect("redo-bearing finished prefix must include a log file fd");
        let sync = LogWriteSubmission::commit_sync(log_fd, self.log_sync);
        self.prefix.push_front_sync(ready, sync);
    }

    #[inline]
    fn advance_sync_prefix_entry(&mut self, sealer: &mut LogFileSealer, entry: LogPrefixEntry) {
        let LogPrefixKind::Sync {
            mut ready_prefix,
            sync,
            ready,
            failure,
            sync_nanos,
            ..
        } = entry.kind
        else {
            unreachable!("sync prefix completion requires a sync entry")
        };
        debug_assert!(sync.is_none());
        debug_assert!(ready);
        if let Some(reason) = failure {
            let failed_reason = FailedPrecommitReason::Fatal(reason);
            self.fail_ready_prefix_waiters(&mut ready_prefix, failed_reason);
            return;
        }
        self.publish_ready_group_prefix(sealer, ready_prefix, sync_nanos);
    }

    #[inline]
    fn publish_ready_group_prefix(
        &self,
        sealer: &mut LogFileSealer,
        ready: ReadyGroupPrefix,
        sync_nanos: usize,
    ) {
        let max_cts = ready.written.last().unwrap().max_cts;
        for sync_group in &ready.written {
            if let Some(write_meta) = sync_group.write_meta {
                // Seal metadata is file-local, so only record groups after the
                // corresponding file prefix has become durable.
                sealer.record_group(write_meta);
            }
        }
        self.trx_sys
            .redo_log
            .persisted_cts
            .store(max_cts.as_u64(), Ordering::SeqCst);

        for mut sync_group in ready.written {
            debug_assert!(sync_group.failure_reason.is_none());
            self.recycle_bufs(sync_group.drain_buffers());
            // commit transactions to let waiting read operations to continue
            let mut committed_trx_list: FastHashMap<usize, Vec<CommittedTrx>> =
                FastHashMap::default();
            for trx in mem::take(&mut sync_group.trx_list) {
                let trx = trx.commit();
                if let Some(gc_no) = trx.gc_no() {
                    // Only user transaction is involved in GC process.
                    committed_trx_list.entry(gc_no).or_default().push(trx);
                }
            }
            // Handoff committed transaction payloads to the purge coordinator
            // before waking commit waiters. The purge receiver must stay alive
            // until redo has joined during worker shutdown.
            if !committed_trx_list.is_empty() {
                self.trx_sys
                    .redo_log
                    .purge_tx
                    .send(Purge::Committed(committed_trx_list))
                    .expect("purge coordinator receiver must stay alive until log thread stopped");
            }
            sync_group.completion.complete(Ok(()));
        }

        self.trx_sys.redo_log.update_stats(
            ready.trx_count,
            ready.commit_count,
            ready.log_bytes,
            usize::from(ready.log_bytes > 0 && self.log_sync != LogSync::None),
            sync_nanos,
        );
    }

    #[inline]
    fn fail_ready_prefix_waiters(
        &self,
        ready: &mut ReadyGroupPrefix,
        reason: FailedPrecommitReason,
    ) {
        let mut recycle = Vec::new();
        fail_ready_prefix_waiters(self.trx_sys, ready, reason, &mut recycle);
        self.recycle_bufs(recycle);
    }

    /// Submit queued redo writes.
    ///
    /// Header, group, and seal writes share the same async write driver
    /// capacity. Groups without redo bytes are ready as soon as they enter the
    /// prefix and are published by `advance_ordered_prefix`.
    #[inline]
    fn submit_io(&mut self) {
        let mut idx = 0;
        while idx < self.prefix.entries.len() {
            if self.write_driver.available_capacity() == 0 {
                break;
            }
            let prefix_id = self.prefix.entry_id(idx);
            let Some(mut submission) = self.prefix.take_submission(idx) else {
                idx += 1;
                continue;
            };
            let owner = self.ensure_submission_owner(&mut submission, prefix_id);
            if let Err(submission) = self.write_driver.push_write(submission) {
                self.prefix.restore_submission(idx, submission);
                break;
            }
            self.prefix.mark_submission_driver_owned(idx, owner.kind);
            if !self.prefix.entry_has_pending_submission(idx) {
                idx += 1;
            }
        }
        self.write_driver.submit_ready();
    }

    #[inline]
    fn ensure_submission_owner(
        &mut self,
        submission: &mut LogWriteSubmission,
        prefix_id: LogPrefixId,
    ) -> LogRequestOwner {
        if let Some(owner) = submission.owner() {
            return owner;
        }
        let request_id = self.next_request_id();
        let owner = match &submission.kind {
            LogWriteKind::Header { .. } => LogRequestOwner::header(request_id, prefix_id),
            LogWriteKind::Group { group_write_idx } => {
                LogRequestOwner::group(request_id, prefix_id, *group_write_idx)
            }
            LogWriteKind::CommitSync => LogRequestOwner::commit_sync(request_id, prefix_id),
            LogWriteKind::SealWrite { .. } => LogRequestOwner::seal_write(request_id, prefix_id),
            LogWriteKind::SealSync { .. } => LogRequestOwner::seal_sync(request_id, prefix_id),
            LogWriteKind::StandaloneSync => {
                panic!("standalone sync submissions are not prefix-owned")
            }
        };
        submission.attach_owner(owner);
        owner
    }

    /// Wait for one submitted redo write, then drain already-buffered completions.
    #[inline]
    fn wait_and_drain_io_if_submitted(&mut self, sealer: &mut LogFileSealer) {
        if self.write_driver.submitted_len() == 0 {
            return;
        }
        let completion = self.write_driver.wait_at_least_one();
        let entered_fatal = self.handle_write_completion(sealer, completion);
        if entered_fatal {
            return;
        }
        while let Some(completion) = self.write_driver.try_pop_buffered_completion() {
            if self.handle_write_completion(sealer, completion) {
                return;
            }
        }
    }

    #[inline]
    fn handle_write_completion(
        &mut self,
        sealer: &mut LogFileSealer,
        completion: LogWriteCompletion,
    ) -> bool {
        let LogWriteCompletion {
            owner,
            kind,
            buf,
            poison,
        } = completion;
        match kind {
            LogWriteKind::Header { completion } => {
                drop(buf.expect("redo header write completion must return a buffer"));
                let Some(owner) = owner else {
                    if let Some(source) = poison {
                        completion.complete(Err(CompletionErrorKind::report_fatal(
                            source,
                            "redo header write failed",
                        )));
                    } else {
                        completion.complete(Ok(()));
                    }
                    return false;
                };
                debug_assert_eq!(owner.kind, LogRequestKind::Header);
                let prefix_id = owner
                    .prefix_id
                    .expect("redo header request must carry a prefix entry id");
                self.complete_header_request(prefix_id, poison);
                if let Some(source) = poison {
                    let err = self.trx_sys.poison_storage(source);
                    self.fail_pending(sealer, err);
                    return true;
                }
            }
            LogWriteKind::Group { .. } => {
                let owner = owner.expect("redo group completion must carry request owner");
                debug_assert_eq!(owner.kind, LogRequestKind::Group);
                let prefix_id = owner
                    .prefix_id
                    .expect("redo group request must carry a prefix entry id");
                self.complete_group_request(
                    prefix_id,
                    owner.group_write_idx,
                    buf.expect("redo group write completion must return a buffer"),
                    poison,
                );
                if let Some(source) = poison {
                    let err = self.trx_sys.poison_storage(source);
                    self.fail_pending(sealer, err);
                    return true;
                }
            }
            LogWriteKind::CommitSync => {
                debug_assert!(buf.is_none());
                let owner = owner.expect("redo sync completion must carry request owner");
                debug_assert_eq!(owner.kind, LogRequestKind::CommitSync);
                let prefix_id = owner
                    .prefix_id
                    .expect("redo sync request must carry a prefix entry id");
                self.complete_sync_request(prefix_id, poison);
                if let Some(source) = poison {
                    let err = self.trx_sys.poison_storage(source);
                    self.fail_pending(sealer, err);
                    return true;
                }
            }
            LogWriteKind::SealWrite { log_file } => {
                drop(buf.expect("redo seal write completion must return a buffer"));
                let owner = owner.expect("redo seal write completion must carry request owner");
                debug_assert_eq!(owner.kind, LogRequestKind::SealWrite);
                let prefix_id = owner
                    .prefix_id
                    .expect("redo seal write request must carry a prefix entry id");
                if let Some(reason) = self.complete_seal_write_request(prefix_id, log_file, poison)
                {
                    let err = self.trx_sys.poison_storage(reason);
                    self.fail_pending(sealer, err);
                    return true;
                }
            }
            LogWriteKind::SealSync { log_file } => {
                debug_assert!(buf.is_none());
                let owner = owner.expect("redo seal sync completion must carry request owner");
                debug_assert_eq!(owner.kind, LogRequestKind::SealSync);
                let prefix_id = owner
                    .prefix_id
                    .expect("redo seal sync request must carry a prefix entry id");
                if let Some(reason) = self.complete_seal_sync_request(prefix_id, log_file, poison) {
                    let err = self.trx_sys.poison_storage(reason);
                    self.fail_pending(sealer, err);
                    return true;
                }
            }
            LogWriteKind::StandaloneSync => {
                panic!("standalone redo sync completion reached prefix writer")
            }
        }
        false
    }

    #[inline]
    fn complete_header_request(&mut self, prefix_id: LogPrefixId, poison: Option<FatalError>) {
        let entry = self
            .prefix
            .entry_mut(prefix_id)
            .expect("redo header completion must match one prefix entry");
        let LogPrefixKind::Header { ready, failure, .. } = &mut entry.kind else {
            panic!("redo header completion matched non-header prefix entry");
        };
        if failure.is_none() {
            *failure = poison;
        }
        *ready = true;
    }

    #[inline]
    fn complete_group_request(
        &mut self,
        prefix_id: LogPrefixId,
        group_write_idx: Option<usize>,
        buf: DirectBuf,
        poison: Option<FatalError>,
    ) {
        let entry = self
            .prefix
            .entry_mut(prefix_id)
            .expect("redo group completion must match one prefix entry");
        let LogPrefixKind::Group { group } = &mut entry.kind else {
            panic!("redo group completion matched non-group prefix entry");
        };
        debug_assert!(
            group_write_idx.is_some(),
            "redo group completion must carry a physical block index"
        );
        group.finish_request(buf);
        if let Some(source) = poison {
            group
                .failure_reason
                .get_or_insert(FailedPrecommitReason::Fatal(source));
        }
    }

    #[inline]
    fn complete_sync_request(&mut self, prefix_id: LogPrefixId, poison: Option<FatalError>) {
        let entry = self
            .prefix
            .entry_mut(prefix_id)
            .expect("redo sync completion must match one prefix entry");
        let LogPrefixKind::Sync {
            ready,
            failure,
            started_at,
            sync_nanos,
            ..
        } = &mut entry.kind
        else {
            panic!("redo sync completion matched non-sync prefix entry");
        };
        if failure.is_none() {
            *failure = poison;
        }
        *sync_nanos = started_at
            .take()
            .map_or(0, |started_at| started_at.elapsed().as_nanos() as usize);
        *ready = true;
    }

    #[inline]
    fn complete_seal_write_request(
        &mut self,
        prefix_id: LogPrefixId,
        log_file: Box<RedoLogFile>,
        poison: Option<FatalError>,
    ) -> Option<FatalError> {
        let reason = poison;
        let entry = self
            .prefix
            .entry_mut(prefix_id)
            .expect("redo seal write completion must match one prefix entry");
        let LogPrefixKind::Seal {
            sync: pending_sync,
            ready,
            failure,
            ..
        } = &mut entry.kind
        else {
            panic!("redo seal write completion matched non-seal prefix entry");
        };
        let already_failed = failure.is_some();
        if !already_failed {
            *failure = reason;
        }
        if !already_failed && reason.is_none() && self.log_sync != LogSync::None {
            debug_assert!(pending_sync.is_none());
            *pending_sync = Some(LogWriteSubmission::seal_sync(*log_file, self.log_sync));
        } else {
            drop(log_file);
            *ready = true;
        }
        reason
    }

    #[inline]
    fn complete_seal_sync_request(
        &mut self,
        prefix_id: LogPrefixId,
        log_file: Box<RedoLogFile>,
        poison: Option<FatalError>,
    ) -> Option<FatalError> {
        drop(log_file);
        let entry = self
            .prefix
            .entry_mut(prefix_id)
            .expect("redo seal sync completion must match one prefix entry");
        let LogPrefixKind::Seal { ready, failure, .. } = &mut entry.kind else {
            panic!("redo seal sync completion matched non-seal prefix entry");
        };
        if failure.is_none() {
            *failure = poison;
        }
        *ready = true;
        poison
    }
}

/// Return the next redo log file sequence.
#[inline]
pub(crate) fn next_redo_file_seq(file_seq: u32) -> Result<u32> {
    file_seq.checked_add(1).ok_or_else(|| {
        Error::from(
            Report::new(DataIntegrityError::RedoLogSequenceGap).attach(format!(
                "redo log file family has terminal sequence {file_seq:08x}; cannot create next file"
            )),
        )
    })
}

/// Discover redo log files in the configured single-stream file family.
#[inline]
pub(crate) fn discover_redo_log_files(
    file_prefix: &str,
    first_retained_file_seq: u32,
    desc: bool,
) -> Result<Vec<RedoLogFileDescriptor>> {
    let pattern = format!("{}.*", Pattern::escape(file_prefix));
    let mut files = vec![];
    for entry in glob(&pattern).unwrap() {
        let path = entry?;
        let Some(suffix) = log_family_suffix(file_prefix, &path)? else {
            continue;
        };
        if is_log_file_seq(suffix) {
            let file_seq = parse_file_seq(&path)?;
            files.push((file_seq, path));
            continue;
        }
        if is_legacy_partitioned_log_suffix(suffix) {
            return Err(Report::new(DataIntegrityError::InvalidPayload)
                .attach(format!(
                    "unsupported legacy partitioned redo log file: {}",
                    path.display()
                ))
                .into());
        }
        return Err(Report::new(DataIntegrityError::InvalidPayload)
            .attach(format!(
                "invalid redo log file name for single-stream layout: {}",
                path.display()
            ))
            .into());
    }
    files.sort_by_key(|(seq, _)| *seq);
    let files = files
        .into_iter()
        .filter(|(seq, _)| *seq >= first_retained_file_seq)
        .collect::<Vec<_>>();
    validate_redo_log_file_sequences(file_prefix, first_retained_file_seq, &files)?;
    let mut res = files
        .into_iter()
        .map(|(file_seq, path)| RedoLogFileDescriptor { file_seq, path })
        .collect::<Vec<_>>();
    if desc {
        res.reverse();
    }
    Ok(res)
}

/// Parse the eight-hex redo file sequence suffix from a redo log path.
#[inline]
pub(crate) fn parse_file_seq(file_path: &Path) -> Result<u32> {
    let file_name = file_path
        .file_name()
        .ok_or_else(|| {
            Error::from(
                Report::new(DataIntegrityError::InvalidPayload)
                    .attach(format!("missing log file name: {}", file_path.display())),
            )
        })?
        .to_str()
        .ok_or_else(|| {
            Error::from(
                Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                    "log file name must be valid UTF-8: {}",
                    file_path.display()
                )),
            )
        })?;
    if file_name.len() < 9 {
        return Err(Report::new(DataIntegrityError::InvalidPayload)
            .attach(format!("log file name is too short: {file_name}"))
            .into());
    }
    // last 8 bytes are hex encoded.
    let suffix = from_utf8(&file_name.as_bytes()[file_name.len() - 8..]).map_err(|_| {
        Error::from(
            Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                "log file sequence suffix must be UTF-8: {file_name}"
            )),
        )
    })?;
    let file_seq = u32::from_str_radix(suffix, 16).map_err(|_| {
        Error::from(
            Report::new(DataIntegrityError::InvalidPayload)
                .attach(format!("log file sequence suffix must be hex: {file_name}")),
        )
    })?;
    Ok(file_seq)
}

#[inline]
fn finish_header_prefix_entry(entry: LogPrefixEntry) {
    let LogPrefixKind::Header {
        completion,
        failure,
        ..
    } = entry.kind
    else {
        unreachable!("header prefix completion requires a header entry")
    };
    let Some(completion) = completion else {
        return;
    };
    if let Some(reason) = failure {
        completion.complete(Err(CompletionErrorKind::report_fatal(
            reason,
            "redo header write failed",
        )));
    } else {
        completion.complete(Ok(()));
    }
}

#[inline]
fn finish_seal_prefix_entry(entry: LogPrefixEntry) {
    let LogPrefixKind::Seal {
        log_file,
        write,
        sync,
        ready,
        ..
    } = entry.kind
    else {
        unreachable!("seal prefix completion requires a seal entry")
    };
    debug_assert!(log_file.is_none());
    debug_assert!(write.is_none());
    debug_assert!(sync.is_none());
    debug_assert!(ready);
}

#[inline]
fn sync_operation(fd: RawFd, log_sync: LogSync) -> Operation {
    match log_sync {
        LogSync::Fsync => Operation::fsync(fd),
        LogSync::Fdatasync => Operation::fdatasync(fd),
        LogSync::None => {
            panic!("log_sync=none must not submit a backend sync operation")
        }
    }
}

#[inline]
fn log_write_completion_from_completed(
    completed: CompletedSubmission<LogWriteSubmission>,
) -> LogWriteCompletion {
    let mut submission = completed.submission;
    let kind = submission.operation.kind();
    let expected_len = submission.operation.len();
    let buf = match kind {
        IOKind::Read | IOKind::Write => Some(
            submission
                .operation
                .take_buf()
                .expect("redo write submission must still own its direct buffer"),
        ),
        IOKind::Fsync | IOKind::Fdatasync => None,
    };
    LogWriteCompletion {
        owner: submission.owner,
        kind: submission.kind,
        buf,
        poison: redo_io_poison(kind, completed.result, expected_len),
    }
}

#[inline]
fn redo_io_poison(
    kind: IOKind,
    result: IoResult<usize>,
    expected_len: usize,
) -> Option<FatalError> {
    match kind {
        IOKind::Read | IOKind::Write => match result {
            Ok(len) if len == expected_len => None,
            Ok(_) | Err(_) => Some(FatalError::RedoWrite),
        },
        IOKind::Fsync | IOKind::Fdatasync => match result {
            Ok(0) => None,
            Ok(_) | Err(_) => Some(FatalError::RedoSync),
        },
    }
}

#[inline]
fn fail_ready_prefix_waiters(
    trx_sys: &TransactionSystem,
    ready: &mut ReadyGroupPrefix,
    reason: FailedPrecommitReason,
    recycle: &mut Vec<DirectBuf>,
) {
    for sync_group in &mut ready.written {
        sync_group.fail_waiters(trx_sys, reason);
        recycle.extend(sync_group.drain_buffers());
    }
    for sync_group in &mut ready.failed {
        sync_group.fail_waiters(trx_sys, reason);
        recycle.extend(sync_group.drain_buffers());
    }
}

#[inline]
fn log_file_name(file_prefix: &str, file_seq: u32) -> String {
    format!("{file_prefix}.{file_seq:08x}")
}

/// Create a new log file and prepare its initial super-block write.
#[inline]
fn create_log_file(
    file_prefix: &str,
    file_seq: u32,
    file_max_size: usize,
    log_block_size: usize,
) -> Result<CreatedLogFile> {
    let (created, _) = create_log_file_with_header_completion(
        file_prefix,
        file_seq,
        file_max_size,
        log_block_size,
    )?;
    Ok(created)
}

#[inline]
fn create_log_file_with_header_completion(
    file_prefix: &str,
    file_seq: u32,
    file_max_size: usize,
    log_block_size: usize,
) -> Result<(CreatedLogFile, Arc<Completion<()>>)> {
    create_log_file_with_header_completion_with_mode(
        file_prefix,
        file_seq,
        file_max_size,
        log_block_size,
        RedoLogCreateMode::CreateOrFail,
    )
}

#[inline]
fn create_log_file_with_header_completion_with_mode(
    file_prefix: &str,
    file_seq: u32,
    file_max_size: usize,
    log_block_size: usize,
    create_mode: RedoLogCreateMode,
) -> Result<(CreatedLogFile, Arc<Completion<()>>)> {
    let file_name = log_file_name(file_prefix, file_seq);
    let sparse_file = match create_mode {
        RedoLogCreateMode::CreateOrFail => {
            SparseFile::create_or_fail(&file_name, file_max_size, UNTRACKED_FILE_ID)?
        }
        RedoLogCreateMode::CreateOrTrunc => {
            SparseFile::create_or_trunc(&file_name, file_max_size, UNTRACKED_FILE_ID)?
        }
    };
    let super_block = RedoSuperBlock::initial(file_seq, log_block_size, file_max_size);
    let (header_write, header_completion) =
        prepare_initial_redo_super_block(&sparse_file, &super_block)?;
    let _ = sparse_file.alloc(REDO_DEFAULT_DATA_START_OFFSET)?;
    let log_file = RedoLogFile::new(sparse_file, super_block);
    Ok((
        CreatedLogFile {
            log_file,
            header_write,
        },
        header_completion,
    ))
}

#[inline]
fn prepare_initial_redo_super_block(
    log_file: &SparseFile,
    super_block: &RedoSuperBlock,
) -> Result<(LogWriteSubmission, Arc<Completion<()>>)> {
    let mut buf = DirectBuf::zeroed(REDO_SUPER_BLOCK_SLOT_SIZE);
    serialize_redo_super_block(buf.as_bytes_mut(), super_block)?;
    Ok(LogWriteSubmission::header(log_file.as_raw_fd(), 0, buf))
}

#[inline]
fn open_recovered_seal_file(recovered: RecoveredRedoSeal) -> Result<RedoLogFile> {
    let path = recovered.path.to_str().ok_or_else(|| {
        Error::from(
            Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                "redo log path is not valid UTF-8: path={}",
                recovered.path.display()
            )),
        )
    })?;
    let sparse_file = SparseFile::open(path, UNTRACKED_FILE_ID)?;
    Ok(RedoLogFile::new(sparse_file, recovered.super_block).with_seal_metadata(recovered.metadata))
}

#[inline]
fn validate_redo_log_file_sequences(
    file_prefix: &str,
    first_retained_file_seq: u32,
    files: &[(u32, PathBuf)],
) -> Result<()> {
    if files.is_empty() {
        if first_retained_file_seq > 0 {
            return Err(Report::new(DataIntegrityError::RedoLogSequenceGap)
                .attach(format!(
                    "no redo log files at or above first retained sequence {first_retained_file_seq:08x} in family {file_prefix}"
                ))
                .into());
        }
        return Ok(());
    }
    if let Some((first, _)) = files.first()
        && *first != first_retained_file_seq
    {
        let missing_end = first.saturating_sub(1);
        let missing = format_redo_sequence_range(first_retained_file_seq, missing_end);
        let missing_kind = if first_retained_file_seq == 0 {
            "redo log file prefix"
        } else {
            "first retained redo log file"
        };
        return Err(Report::new(DataIntegrityError::RedoLogSequenceGap)
            .attach(format!(
                "missing {missing_kind} sequence(s) {missing} in family {file_prefix}"
            ))
            .into());
    }
    for window in files.windows(2) {
        let prev = window[0].0;
        let next = window[1].0;
        if next <= prev {
            return Err(Report::new(DataIntegrityError::InvalidPayload)
                .attach(format!(
                    "duplicate redo log file sequence {next:08x} in family {file_prefix}"
                ))
                .into());
        }
        let expected = prev.checked_add(1).ok_or_else(|| {
            Error::from(
                Report::new(DataIntegrityError::RedoLogSequenceGap).attach(format!(
                    "redo log file family {file_prefix} has file after terminal sequence {prev:08x}"
                )),
            )
        })?;
        if next != expected {
            let missing_end = next - 1;
            let missing = if expected == missing_end {
                format!("{expected:08x}")
            } else {
                format!("{expected:08x}..={missing_end:08x}")
            };
            return Err(Report::new(DataIntegrityError::RedoLogSequenceGap)
                .attach(format!(
                    "missing redo log file sequence(s) {missing} in family {file_prefix}"
                ))
                .into());
        }
    }
    Ok(())
}

#[inline]
fn format_redo_sequence_range(first: u32, last: u32) -> String {
    if first == last {
        format!("{first:08x}")
    } else {
        format!("{first:08x}..={last:08x}")
    }
}

#[inline]
fn log_family_suffix<'a>(file_prefix: &str, file_path: &'a Path) -> Result<Option<&'a str>> {
    let path = file_path.to_str().ok_or_else(|| {
        Error::from(
            Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                "log file path must be valid UTF-8: {}",
                file_path.display()
            )),
        )
    })?;
    let Some(suffix) = path.strip_prefix(file_prefix) else {
        return Ok(None);
    };
    Ok(suffix.strip_prefix('.'))
}

#[inline]
fn is_log_file_seq(value: &str) -> bool {
    value.len() == 8 && value.as_bytes().iter().all(u8::is_ascii_hexdigit)
}

#[inline]
fn is_legacy_partitioned_log_suffix(value: &str) -> bool {
    let mut parts = value.split('.');
    let Some(partition) = parts.next() else {
        return false;
    };
    let Some(seq) = parts.next() else {
        return false;
    };
    parts.next().is_none()
        && !partition.is_empty()
        && partition.as_bytes().iter().all(u8::is_ascii_digit)
        && is_log_file_seq(seq)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::{PoolRole, test_page_id};
    use crate::catalog::tests::table2;
    use crate::conf::{EngineConfig, EvictableBufferPoolConfig, TrxSysConfig};
    use crate::engine::{Engine, EngineRef};
    use crate::error::{
        CompletionErrorKind, DataIntegrityError, FatalError, InternalError, LifecycleError,
    };
    use crate::id::{PageID, RowID, TableID};
    use crate::io::{
        BackendToken, IOBackend, IOKind, StdIoResult, StorageBackendOp, StorageBackendTestHook,
        install_storage_backend_test_hook,
    };
    use crate::log::format::{
        REDO_DEFAULT_DATA_START_OFFSET, REDO_SUPER_BLOCK_SLOT_SIZE, parse_redo_super_block,
        slot_offset,
    };
    use crate::log::redo::{RedoHeader, RedoLogs, RedoTrxKind, RowRedo, RowRedoKind, TableDML};
    use crate::recovery::stream::{RedoLogSegment, RedoReplayPlanner};
    use crate::session::tests::SessionTestExt;
    use crate::trx::MAX_SNAPSHOT_TS;
    use crate::trx::sys::{TransactionSystemQueues, TrxCleanupMessage};
    use crate::value::Val;
    use event_listener::Event;
    use futures::task::noop_waker;
    use smol::Timer;
    use std::collections::BTreeMap;
    use std::fmt::Debug;
    use std::fs::{self, File, OpenOptions};
    use std::future::Future;
    use std::io::{Error as IoError, Seek, SeekFrom, Write};
    use std::iter::repeat_n;
    use std::os::fd::{AsRawFd, RawFd};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex, mpsc};
    use std::task::{Context, Poll};
    use std::thread::{self, JoinHandle};
    use std::time::Duration;
    use tempfile::TempDir;

    const TEST_WAIT_RETRIES: usize = 100;
    const TEST_WAIT_INTERVAL: Duration = Duration::from_millis(10);

    async fn wait_for<F>(mut predicate: F)
    where
        F: FnMut() -> bool,
    {
        for _ in 0..TEST_WAIT_RETRIES {
            if predicate() {
                return;
            }
            Timer::after(TEST_WAIT_INTERVAL).await;
        }
        panic!("condition was not satisfied before timeout");
    }

    fn oversized_redo_log_for_test(cts: TrxID) -> TrxLog {
        let mut rows = BTreeMap::new();
        let text: String = repeat_n('a', 8000).collect();
        rows.insert(
            RowID::new(100),
            RowRedo {
                page_id: test_page_id(5),
                row_id: RowID::new(100),
                kind: RowRedoKind::Insert(vec![Val::from(1u32), Val::from(&text[..])]),
            },
        );
        let mut dml = BTreeMap::new();
        dml.insert(TableID::new(5), TableDML { rows });
        TrxLog::new(
            RedoHeader {
                cts,
                trx_kind: RedoTrxKind::User,
            },
            RedoLogs { ddl: None, dml },
        )
    }

    fn create_log_file_for_test(
        file_prefix: &str,
        file_seq: u32,
        file_max_size: usize,
        log_block_size: usize,
    ) -> RedoLogFile {
        let (
            CreatedLogFile {
                log_file,
                header_write,
            },
            header_completion,
        ) = create_log_file_with_header_completion(
            file_prefix,
            file_seq,
            file_max_size,
            log_block_size,
        )
        .unwrap();
        submit_header_write_for_test(header_write, &header_completion);
        log_file
    }

    fn redo_planner_and_finalizer_for_test(
        file_prefix: &str,
        recovery_io_depth: usize,
        log_write_io_depth: usize,
        file_max_size: usize,
        log_block_size: usize,
        logs: Vec<RedoLogFileDescriptor>,
    ) -> (RedoLogFinalizer, RedoReplayPlanner, usize) {
        let next_file_seq = logs
            .last()
            .map(|descriptor| next_redo_file_seq(descriptor.file_seq))
            .transpose()
            .unwrap()
            .unwrap_or(0);
        let planner = RedoReplayPlanner::new(logs);
        let finalizer = RedoLogFinalizer::new(
            file_prefix.to_owned(),
            log_write_io_depth,
            file_max_size,
            log_block_size,
            next_file_seq,
        );
        (finalizer, planner, recovery_io_depth)
    }

    fn create_sealed_log_file_for_test(
        file_prefix: &str,
        file_seq: u32,
        durable_end_offset: usize,
        redo_range: Option<(TrxID, TrxID)>,
    ) -> PathBuf {
        let log_file = create_log_file_for_test(file_prefix, file_seq, 128 * 1024, 4096);
        let open = log_file.super_block();
        let slot_no = if open.slot_no == 0 { 1 } else { 0 };
        let sealed =
            RedoSuperBlock::sealed_from_open(&open, slot_no, durable_end_offset, redo_range)
                .unwrap();
        let mut buf = vec![0; REDO_SUPER_BLOCK_SLOT_SIZE];
        serialize_redo_super_block(&mut buf, &sealed).unwrap();
        let path = PathBuf::from(log_file_name(file_prefix, file_seq));
        let mut file = OpenOptions::new().write(true).open(&path).unwrap();
        file.seek(SeekFrom::Start(slot_offset(sealed.slot_no) as u64))
            .unwrap();
        file.write_all(&buf).unwrap();
        file.flush().unwrap();
        path
    }

    fn finish_redo_log_for_test(
        file_prefix: String,
        log_write_io_depth: usize,
    ) -> (RedoLog, Arc<Completion<()>>) {
        let (purge_tx, _purge_rx) = flume::unbounded();
        RedoLogFinalizer::new(file_prefix, log_write_io_depth, 128 * 1024, 4096, 0)
            .finalize(purge_tx)
            .unwrap()
    }

    fn submit_header_write_for_test(
        header_write: LogWriteSubmission,
        header_completion: &Completion<()>,
    ) {
        let mut write_driver = LogWriteDriver::new(StorageBackend::new(1).unwrap());
        assert!(write_driver.push_write(header_write).is_ok());
        assert_eq!(write_driver.submit_ready(), 1);
        let LogWriteCompletion {
            owner,
            kind,
            buf,
            poison,
        } = write_driver.wait_at_least_one();
        match kind {
            LogWriteKind::Header { completion } => {
                drop(buf.expect("redo header write completion must return a buffer"));
                assert!(owner.is_none());
                assert_eq!(poison, None);
                completion.complete(Ok(()));
            }
            LogWriteKind::Group { .. } => panic!("expected redo header write completion"),
            LogWriteKind::SealWrite { .. }
            | LogWriteKind::SealSync { .. }
            | LogWriteKind::CommitSync
            | LogWriteKind::StandaloneSync => panic!("expected redo header write completion"),
        }
        assert!(header_completion.completed_result().unwrap().is_ok());
    }

    #[derive(Clone, Default)]
    struct RecordingRedoWriteSubmitHook {
        submits: Arc<Mutex<Vec<StorageBackendOp>>>,
    }

    impl RecordingRedoWriteSubmitHook {
        fn submits(&self) -> Vec<StorageBackendOp> {
            self.submits.lock().unwrap().clone()
        }
    }

    impl StorageBackendTestHook for RecordingRedoWriteSubmitHook {
        fn on_submit(&self, op: StorageBackendOp) {
            if op.kind() == IOKind::Write {
                self.submits.lock().unwrap().push(op);
            }
        }
    }

    #[derive(Clone)]
    struct ControlledRedoWriteHook {
        inner: Arc<ControlledRedoWriteHookInner>,
    }

    struct ControlledRedoWriteHookInner {
        fd: RawFd,
        errno: Option<i32>,
        calls: AtomicUsize,
        started: Event,
        released: AtomicBool,
        release: Event,
    }

    impl ControlledRedoWriteHook {
        fn new(fd: RawFd, errno: i32) -> Self {
            Self::with_error(fd, Some(errno))
        }

        fn success(fd: RawFd) -> Self {
            Self::with_error(fd, None)
        }

        fn with_error(fd: RawFd, errno: Option<i32>) -> Self {
            Self {
                inner: Arc::new(ControlledRedoWriteHookInner {
                    fd,
                    errno,
                    calls: AtomicUsize::new(0),
                    started: Event::new(),
                    released: AtomicBool::new(false),
                    release: Event::new(),
                }),
            }
        }

        fn matches(&self, op: StorageBackendOp) -> bool {
            op.kind() == IOKind::Write && op.fd() == self.inner.fd
        }

        async fn wait_started(&self, expected_calls: usize) {
            loop {
                if self.inner.calls.load(Ordering::SeqCst) >= expected_calls {
                    return;
                }
                event_listener::listener!(self.inner.started => listener);
                if self.inner.calls.load(Ordering::SeqCst) >= expected_calls {
                    return;
                }
                listener.await;
            }
        }

        fn release(&self) {
            self.inner.released.store(true, Ordering::SeqCst);
            self.inner.release.notify(usize::MAX);
        }
    }

    impl StorageBackendTestHook for ControlledRedoWriteHook {
        fn on_submit(&self, op: StorageBackendOp) {
            if self.matches(op) {
                self.inner.calls.fetch_add(1, Ordering::SeqCst);
                self.inner.started.notify(usize::MAX);
            }
        }

        fn on_complete(&self, op: StorageBackendOp, res: &mut StdIoResult<usize>) {
            if !self.matches(op) {
                return;
            }
            loop {
                if self.inner.released.load(Ordering::SeqCst) {
                    break;
                }
                event_listener::listener!(self.inner.release => listener);
                if self.inner.released.load(Ordering::SeqCst) {
                    break;
                }
                smol::block_on(listener);
            }
            if let Some(errno) = self.inner.errno {
                *res = Err(IoError::from_raw_os_error(errno));
            }
        }
    }

    #[derive(Clone)]
    struct ControlledRedoSyncHook {
        inner: Arc<ControlledRedoSyncHookInner>,
    }

    struct ControlledRedoSyncHookInner {
        fd: RawFd,
        kind: IOKind,
        errno: i32,
        calls: AtomicUsize,
        started: Event,
        released: AtomicBool,
        release: Event,
    }

    impl ControlledRedoSyncHook {
        fn new(fd: RawFd, kind: IOKind, errno: i32) -> Self {
            debug_assert!(matches!(kind, IOKind::Fsync | IOKind::Fdatasync));
            Self {
                inner: Arc::new(ControlledRedoSyncHookInner {
                    fd,
                    kind,
                    errno,
                    calls: AtomicUsize::new(0),
                    started: Event::new(),
                    released: AtomicBool::new(false),
                    release: Event::new(),
                }),
            }
        }

        fn matches(&self, op: StorageBackendOp) -> bool {
            op.fd() == self.inner.fd && op.kind() == self.inner.kind
        }

        async fn wait_started(&self, expected_calls: usize) {
            loop {
                if self.inner.calls.load(Ordering::SeqCst) >= expected_calls {
                    return;
                }
                event_listener::listener!(self.inner.started => listener);
                if self.inner.calls.load(Ordering::SeqCst) >= expected_calls {
                    return;
                }
                listener.await;
            }
        }

        fn release(&self) {
            self.inner.released.store(true, Ordering::SeqCst);
            self.inner.release.notify(usize::MAX);
        }
    }

    impl StorageBackendTestHook for ControlledRedoSyncHook {
        fn on_submit(&self, op: StorageBackendOp) {
            if self.matches(op) {
                self.inner.calls.fetch_add(1, Ordering::SeqCst);
                self.inner.started.notify(usize::MAX);
            }
        }

        fn on_complete(&self, op: StorageBackendOp, res: &mut StdIoResult<usize>) {
            if !self.matches(op) {
                return;
            }
            loop {
                if self.inner.released.load(Ordering::SeqCst) {
                    break;
                }
                event_listener::listener!(self.inner.release => listener);
                if self.inner.released.load(Ordering::SeqCst) {
                    break;
                }
                smol::block_on(listener);
            }
            *res = Err(IoError::from_raw_os_error(self.inner.errno));
        }
    }

    #[derive(Clone)]
    struct RecordingRedoSyncHook {
        calls: Arc<Mutex<Vec<StorageBackendOp>>>,
    }

    impl RecordingRedoSyncHook {
        fn new() -> Self {
            Self {
                calls: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn calls(&self) -> Vec<StorageBackendOp> {
            self.calls.lock().unwrap().clone()
        }
    }

    impl StorageBackendTestHook for RecordingRedoSyncHook {
        fn on_submit(&self, op: StorageBackendOp) {
            if matches!(op.kind(), IOKind::Fsync | IOKind::Fdatasync) {
                self.calls.lock().unwrap().push(op);
            }
        }
    }

    fn spawn_sys_commit_wait(engine: EngineRef, marker: u64) -> JoinHandle<Result<TrxID>> {
        thread::spawn(move || {
            smol::block_on(async move {
                let mut sys_trx = engine.trx_sys.begin_sys_trx();
                sys_trx.create_row_page(
                    TableID::from(marker),
                    PageID::from(marker),
                    RowID::new(0),
                    RowID::new(1),
                );
                let prepared = sys_trx.prepare();
                engine.trx_sys.commit_prepared(prepared).await
            })
        })
    }

    fn assert_direct_fatal<T: Debug>(res: &Result<T>, expected: FatalError) {
        let err = match res {
            Ok(value) => panic!("expected fatal error, got {value:?}"),
            Err(err) => err,
        };
        assert_eq!(
            err.downcast_ref::<FatalError>().copied(),
            Some(expected),
            "{err:?}"
        );
    }

    fn assert_propagated_completion_fatal<T: Debug>(res: &Result<T>, expected: FatalError) {
        let err = match res {
            Ok(value) => panic!("expected propagated completion failure, got {value:?}"),
            Err(err) => err,
        };
        assert_eq!(
            err.completion_error(),
            Some(CompletionErrorKind::Fatal(expected)),
            "{err:?}"
        );
        let report = format!("{err:?}");
        assert!(report.contains("propagate from other threads"), "{report}");
        assert!(report.contains("wait for redo group commit"), "{report}");
    }

    #[test]
    fn test_redo_sync_completion_accepts_only_zero_success_result() {
        assert_eq!(redo_io_poison(IOKind::Fsync, Ok(0), 0), None);
        assert_eq!(redo_io_poison(IOKind::Fdatasync, Ok(0), 0), None);
        assert_eq!(
            redo_io_poison(IOKind::Fsync, Ok(1), 0),
            Some(FatalError::RedoSync)
        );
        assert_eq!(
            redo_io_poison(
                IOKind::Fdatasync,
                Err(IoError::from_raw_os_error(libc::EIO)),
                0
            ),
            Some(FatalError::RedoSync)
        );
    }

    fn record_seal_group(
        sealer: &mut LogFileSealer,
        log_file: &RedoLogFile,
        min_redo_cts: TrxID,
        max_redo_cts: TrxID,
        end_offset: usize,
    ) {
        sealer.record_group(RedoGroupWriteMeta {
            file_seq: log_file.file_seq(),
            fd: log_file.as_raw_fd(),
            offset: REDO_DEFAULT_DATA_START_OFFSET,
            end_offset,
            min_redo_cts,
            max_redo_cts,
        });
    }

    fn manual_redo_log_writer_for_seal(
        engine: &Engine,
        log_sync: LogSync,
        file_prefix: String,
    ) -> (ManualLogProcessorHarness, LogWriteDriver) {
        let (redo_log, _initial_header) = finish_redo_log_for_test(file_prefix, 1);
        let config = TrxSysConfig::default()
            .log_block_size(4096usize)
            .log_sync(log_sync);
        (
            manual_log_processor_harness(engine, config, redo_log),
            LogWriteDriver::new(StorageBackend::new(1).unwrap()),
        )
    }

    fn finish_prefix_seal_for_test(
        harness: &ManualLogProcessorHarness,
        write_driver: &mut LogWriteDriver,
        sealer: &mut LogFileSealer,
        log_file: RedoLogFile,
    ) {
        let mut writer =
            RedoLogWriter::new(&harness.trx_sys, &harness.trx_sys.config, write_driver);
        writer.prefix.push_seal(log_file);
        drain_pending_prefix_for_test(&mut writer, sealer);
        assert!(!writer.has_pending_io());
        assert_eq!(writer.write_driver.pending_len(), 0);
        assert_eq!(writer.write_driver.submitted_len(), 0);
    }

    fn drain_pending_prefix_for_test<B>(
        writer: &mut RedoLogWriter<'_, B>,
        sealer: &mut LogFileSealer,
    ) where
        B: IOBackend,
    {
        while !writer.prefix.is_empty() {
            writer.advance_ordered_prefix(sealer);
            writer.shrink_prefix_if_sparse();
            writer.submit_io();
            writer.wait_and_drain_io_if_submitted(sealer);
            writer.advance_ordered_prefix(sealer);
            writer.shrink_prefix_if_sparse();
        }
    }

    async fn build_redo_test_engine(log_file_stem: &str, log_sync: LogSync) -> (TempDir, Engine) {
        build_redo_test_engine_with_log_file_max_size(
            log_file_stem,
            log_sync,
            128usize * 1024 * 1024,
        )
        .await
    }

    async fn build_redo_test_engine_with_log_file_max_size(
        log_file_stem: &str,
        log_sync: LogSync,
        log_file_max_size: usize,
    ) -> (TempDir, Engine) {
        let temp_dir = TempDir::new().unwrap();
        let engine = EngineConfig::default()
            .storage_root(temp_dir.path().to_path_buf())
            .trx(
                TrxSysConfig::default()
                    .log_file_stem(log_file_stem)
                    .log_write_io_depth(1)
                    .recovery_io_depth(1)
                    .catalog_checkpoint_scan_io_depth(1)
                    .log_sync(log_sync)
                    .log_file_max_size(log_file_max_size),
            )
            .data_buffer(
                EvictableBufferPoolConfig::default()
                    .role(PoolRole::Mem)
                    .max_mem_size(64u64 * 1024 * 1024)
                    .max_file_size(128u64 * 1024 * 1024),
            )
            .build()
            .await
            .unwrap();
        (temp_dir, engine)
    }

    struct ManualLogProcessorHarness {
        trx_sys: TransactionSystem,
        _purge_rx: flume::Receiver<Purge>,
        _cleanup_rx: flume::Receiver<TrxCleanupMessage>,
    }

    fn manual_log_processor_harness(
        engine: &Engine,
        config: TrxSysConfig,
        redo_log: RedoLog,
    ) -> ManualLogProcessorHarness {
        let (purge_tx, purge_rx) = flume::unbounded();
        let (cleanup_tx, cleanup_rx) = flume::unbounded();
        let trx_sys = TransactionSystem::new(
            config,
            engine.inner().catalog.clone(),
            engine.inner().table_fs.clone(),
            CachePadded::new(redo_log),
            MIN_SNAPSHOT_TS,
            TransactionSystemQueues {
                purge_tx,
                cleanup_tx,
            },
            Vec::new(),
        );
        ManualLogProcessorHarness {
            trx_sys,
            _purge_rx: purge_rx,
            _cleanup_rx: cleanup_rx,
        }
    }

    #[test]
    fn test_rotate_log_file_missing_current_file_returns_error() {
        let temp_dir = TempDir::new().unwrap();
        let file_prefix = temp_dir
            .path()
            .join("standalone_missing_current_redo.log")
            .to_str()
            .unwrap()
            .to_owned();
        let (redo_log, _initial_header) = finish_redo_log_for_test(file_prefix, 1);

        let err = {
            let mut group_commit_g = redo_log.group_commit.lock();
            drop(group_commit_g.log_file.take().unwrap());
            redo_log.rotate_log_file(&mut group_commit_g).unwrap_err()
        };

        assert_eq!(
            err.downcast_ref::<InternalError>().copied(),
            Some(InternalError::Generic),
            "{err:?}"
        );
    }

    #[test]
    fn test_commit_sys_missing_current_log_file_rejects_without_panic() {
        smol::block_on(async {
            let (_temp_dir, engine) =
                build_redo_test_engine("commit_missing_current_log_file", LogSync::None).await;
            {
                let mut group_commit_g = engine.inner().trx_sys.redo_log.group_commit.lock();
                drop(group_commit_g.log_file.take().unwrap());
            }

            let mut sys_trx = engine.inner().trx_sys.begin_sys_trx();
            sys_trx.create_row_page(
                TableID::from(1u64),
                PageID::from(1u64),
                RowID::new(0),
                RowID::new(1),
            );
            let res = engine.inner().trx_sys.commit_sys(sys_trx);

            assert_direct_fatal(&res, FatalError::RedoWrite);
            assert!(
                engine
                    .inner()
                    .trx_sys
                    .ensure_runtime_healthy()
                    .as_ref()
                    .is_err_and(|err| *err.current_context() == FatalError::RedoWrite)
            );
        });
    }

    #[test]
    fn test_user_commit_rotation_create_failure_rejects_without_panic() {
        smol::block_on(async {
            let (_temp_dir, engine) = build_redo_test_engine_with_log_file_max_size(
                "user_rotation_create_failure",
                LogSync::None,
                64usize * 1024,
            )
            .await;
            let table_id = table2(&engine).await;
            let mut session = engine.new_session().unwrap();
            let mut warmup = session.begin_trx().unwrap();
            warmup
                .exec(async |stmt| {
                    stmt.table_insert_mvcc(table_id, vec![Val::from(0), Val::from("warmup")])
                        .await?;
                    Ok(())
                })
                .await
                .unwrap();
            warmup.commit().await.unwrap();

            let file_prefix = engine.inner().trx_sys.config.file_prefix().unwrap();
            {
                let group_commit_g = engine.inner().trx_sys.redo_log.group_commit.lock();
                let log_file = group_commit_g.log_file.as_ref().unwrap();
                while log_file.alloc(REDO_SUPER_BLOCK_SLOT_SIZE).is_ok() {}
            }
            let next_seq = engine
                .inner()
                .trx_sys
                .redo_log
                .file_seq
                .load(Ordering::SeqCst);
            fs::create_dir(log_file_name(&file_prefix, next_seq)).unwrap();

            let mut trx = session.begin_trx().unwrap();
            trx.exec(async |stmt| {
                stmt.table_insert_mvcc(
                    table_id,
                    vec![Val::from(1), Val::from("rotation-create-failure")],
                )
                .await?;
                Ok(())
            })
            .await
            .unwrap();

            let err = trx.commit().await.unwrap_err();

            assert_eq!(
                err.completion_error(),
                Some(CompletionErrorKind::Fatal(FatalError::RedoWrite)),
                "{err:?}"
            );
            assert!(!session.in_trx().unwrap());
            assert!(
                engine
                    .inner()
                    .trx_sys
                    .ensure_runtime_healthy()
                    .as_ref()
                    .is_err_and(|err| *err.current_context() == FatalError::RedoWrite)
            );
            drop(session);
        });
    }

    #[test]
    fn test_dropped_user_commit_future_after_handoff_finishes_session() {
        smol::block_on(async {
            let (_temp_dir, engine) =
                build_redo_test_engine("commit_handoff_drop", LogSync::None).await;
            let table_id = table2(&engine).await;
            let redo_fd = {
                engine
                    .inner()
                    .trx_sys
                    .redo_log
                    .group_commit
                    .lock()
                    .log_file
                    .as_ref()
                    .unwrap()
                    .as_raw_fd()
            };
            let hook = ControlledRedoWriteHook::success(redo_fd);
            let _install = install_storage_backend_test_hook(Arc::new(hook.clone()));

            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();
            trx.exec(async |stmt| {
                stmt.table_insert_mvcc(table_id, vec![Val::from(1), Val::from("handoff")])
                    .await?;
                Ok(())
            })
            .await
            .unwrap();
            assert!(session.in_trx().unwrap());

            let mut commit_fut = Box::pin(trx.commit());
            let waker = noop_waker();
            let mut cx = Context::from_waker(&waker);
            match commit_fut.as_mut().poll(&mut cx) {
                Poll::Pending => {}
                Poll::Ready(res) => panic!("commit should wait after handoff, got {res:?}"),
            }

            hook.wait_started(1).await;
            assert!(session.in_trx().unwrap());
            drop(commit_fut);

            hook.release();
            wait_for(|| session.in_trx().is_ok_and(|active| !active)).await;

            drop(session);
            engine.shutdown().unwrap();
        });
    }

    #[test]
    fn test_shutdown_drains_committed_handoff_after_dropped_commit_waiter() {
        smol::block_on(async {
            let (_temp_dir, engine) =
                build_redo_test_engine("commit_handoff_shutdown", LogSync::None).await;
            let table_id = table2(&engine).await;
            // Table setup commits through the same asynchronous purge handoff as
            // user transactions. Wait until those setup commits leave the active
            // horizon before this test opens the transaction it wants to track.
            wait_for(|| engine.inner().trx_sys.min_active_sts() == MAX_SNAPSHOT_TS).await;
            let redo_fd = {
                engine
                    .inner()
                    .trx_sys
                    .redo_log
                    .group_commit
                    .lock()
                    .log_file
                    .as_ref()
                    .unwrap()
                    .as_raw_fd()
            };
            let hook = ControlledRedoWriteHook::success(redo_fd);
            let _install = install_storage_backend_test_hook(Arc::new(hook.clone()));

            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();
            let sts = trx.sts();
            assert_eq!(engine.inner().trx_sys.min_active_sts(), sts);
            trx.exec(async |stmt| {
                stmt.table_insert_mvcc(table_id, vec![Val::from(2), Val::from("shutdown")])
                    .await?;
                Ok(())
            })
            .await
            .unwrap();

            let mut commit_fut = Box::pin(trx.commit());
            let waker = noop_waker();
            let mut cx = Context::from_waker(&waker);
            match commit_fut.as_mut().poll(&mut cx) {
                Poll::Pending => {}
                Poll::Ready(res) => panic!("commit should wait after handoff, got {res:?}"),
            }
            hook.wait_started(1).await;
            drop(commit_fut);

            thread::scope(|scope| {
                let (started_tx, started_rx) = mpsc::channel();
                let (done_tx, done_rx) = mpsc::channel();
                let shutdown_engine = &engine;
                let shutdown = scope.spawn(move || {
                    started_tx
                        .send(())
                        .expect("shutdown thread should report start");
                    shutdown_engine.shutdown().unwrap();
                    done_tx.send(()).expect("shutdown should report completion");
                });

                started_rx
                    .recv_timeout(Duration::from_secs(5))
                    .expect("shutdown thread should start");
                assert!(
                    done_rx.recv_timeout(Duration::from_millis(20)).is_err(),
                    "shutdown must wait while redo final commit is blocked"
                );
                hook.release();
                done_rx
                    .recv_timeout(Duration::from_secs(5))
                    .expect("shutdown should finish after redo completion");
                shutdown.join().unwrap();
            });

            assert!(
                engine.inner().trx_sys.min_active_sts() > sts,
                "shutdown must drain committed purge handoff before purge exits"
            );
            drop(session);
        });
    }

    #[test]
    fn test_closed_group_commit_rejects_after_shutdown_message_consumed() {
        smol::block_on(async {
            let (_temp_dir, engine) =
                build_redo_test_engine("commit_closed_after_shutdown_consumed", LogSync::None)
                    .await;
            let table_id = table2(&engine).await;
            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();
            trx.exec(async |stmt| {
                stmt.table_insert_mvcc(table_id, vec![Val::from(1), Val::from("closed")])
                    .await?;
                Ok(())
            })
            .await
            .unwrap();

            {
                let mut group_commit_g = engine.inner().trx_sys.redo_log.group_commit.lock();
                // Simulate the state after the log thread has consumed any
                // shutdown wakeup message: admission is closed, but rejection
                // must not depend on a `Commit::Shutdown` queue tail.
                group_commit_g.close(FailedPrecommitReason::Shutdown);
            }

            let err = trx.commit().await.unwrap_err();
            assert_eq!(
                err.completion_error(),
                Some(CompletionErrorKind::Lifecycle(LifecycleError::Shutdown)),
                "{err:?}"
            );
            assert!(!session.in_trx().unwrap());

            drop(session);
            engine.shutdown().unwrap();
        });
    }

    #[test]
    fn test_user_redo_fsync_failure_rolls_back_session_before_return() {
        smol::block_on(async {
            let (_temp_dir, engine) =
                build_redo_test_engine("user_redo_fsync_failure", LogSync::Fsync).await;
            let table_id = table2(&engine).await;
            let redo_fd = {
                engine
                    .inner()
                    .trx_sys
                    .redo_log
                    .group_commit
                    .lock()
                    .log_file
                    .as_ref()
                    .unwrap()
                    .as_raw_fd()
            };
            let hook = ControlledRedoSyncHook::new(redo_fd, IOKind::Fsync, libc::EIO);
            let _install = install_storage_backend_test_hook(Arc::new(hook.clone()));

            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();
            trx.exec(async |stmt| {
                stmt.table_insert_mvcc(table_id, vec![Val::from(1), Val::from("sync-fail")])
                    .await?;
                Ok(())
            })
            .await
            .unwrap();

            let mut commit_fut = Box::pin(trx.commit());
            let waker = noop_waker();
            let mut cx = Context::from_waker(&waker);
            match commit_fut.as_mut().poll(&mut cx) {
                Poll::Pending => {}
                Poll::Ready(res) => panic!("commit should wait for blocked fsync, got {res:?}"),
            }

            hook.wait_started(1).await;
            assert!(session.in_trx().unwrap());
            hook.release();

            let err = commit_fut.await.unwrap_err();
            assert_eq!(
                err.completion_error(),
                Some(CompletionErrorKind::Fatal(FatalError::RedoSync)),
                "{err:?}"
            );
            assert!(!session.in_trx().unwrap());
            assert!(
                engine
                    .inner()
                    .trx_sys
                    .storage_poison_error()
                    .as_ref()
                    .is_some_and(|err| *err.current_context() == FatalError::RedoSync)
            );

            drop(session);
        });
    }

    fn sync_group_for_order_test(cts: TrxID, ready: bool, log_bytes: usize) -> SyncGroup {
        sync_group_for_order_test_with_log_fd(cts, ready, log_bytes, (log_bytes > 0).then_some(0))
    }

    fn sync_group_for_order_test_with_log_fd(
        cts: TrxID,
        ready: bool,
        log_bytes: usize,
        log_fd: Option<RawFd>,
    ) -> SyncGroup {
        SyncGroup {
            trx_list: vec![PrecommitTrx {
                cts,
                redo_bin: None,
                payload: None,
                attachment: None,
                lock_manager: None,
                lock_state: None,
            }],
            max_cts: cts,
            log_bytes,
            log_fd,
            write_meta: (log_bytes > 0)
                .then_some(log_fd)
                .flatten()
                .map(|fd| RedoGroupWriteMeta {
                    file_seq: 0,
                    fd,
                    offset: REDO_DEFAULT_DATA_START_OFFSET,
                    end_offset: REDO_DEFAULT_DATA_START_OFFSET + log_bytes,
                    min_redo_cts: cts,
                    max_redo_cts: cts,
                }),
            writes: VecDeque::new(),
            returned_bufs: Vec::new(),
            completion: Arc::new(Completion::new()),
            outstanding_requests: usize::from(!ready),
            failure_reason: None,
        }
    }

    fn sync_group_with_pending_write_for_test(cts: TrxID, fd: RawFd, offset: usize) -> SyncGroup {
        let mut group = sync_group_for_order_test_with_log_fd(cts, true, 4096, Some(fd));
        group.write_meta = Some(RedoGroupWriteMeta {
            file_seq: 0,
            fd,
            offset,
            end_offset: offset + 4096,
            min_redo_cts: cts,
            max_redo_cts: cts,
        });
        group.writes.push_back(LogWriteSubmission::group(
            fd,
            offset,
            DirectBuf::zeroed(4096),
            0,
        ));
        group
    }

    #[derive(Clone, Copy)]
    enum LogTestCompletionBatch {
        AllFront,
        OneBack,
    }

    struct LogTestBackend {
        max_events: usize,
        inflight: VecDeque<(BackendToken, IOKind)>,
        wait_batches: VecDeque<LogTestCompletionBatch>,
    }

    impl LogTestBackend {
        fn complete_all(max_events: usize) -> Self {
            Self {
                max_events,
                inflight: VecDeque::new(),
                wait_batches: VecDeque::new(),
            }
        }

        fn complete_one_back(max_events: usize) -> Self {
            Self {
                max_events,
                inflight: VecDeque::new(),
                wait_batches: VecDeque::from([LogTestCompletionBatch::OneBack]),
            }
        }
    }

    impl IOBackend for LogTestBackend {
        type Prepared = (BackendToken, IOKind);
        type SubmitBatch = VecDeque<(BackendToken, IOKind)>;
        type Events = ();

        fn max_events(&self) -> usize {
            self.max_events
        }

        fn new_submit_batch(&self) -> Self::SubmitBatch {
            VecDeque::with_capacity(self.max_events)
        }

        fn new_events(&self) -> Self::Events {}

        fn prepare(&mut self, token: BackendToken, operation: &mut Operation) -> Self::Prepared {
            (token, operation.kind())
        }

        fn push_prepared(&mut self, batch: &mut Self::SubmitBatch, prepared: &mut Self::Prepared) {
            batch.push_back(*prepared);
        }

        fn submit_batch(&mut self, batch: &mut Self::SubmitBatch, limit: usize) -> usize {
            let submit_count = limit.min(batch.len());
            for _ in 0..submit_count {
                let token = batch
                    .pop_front()
                    .expect("submit batch length must match queued tokens");
                self.inflight.push_back(token);
            }
            submit_count
        }

        fn wait_at_least(
            &mut self,
            _events: &mut Self::Events,
            min_nr: usize,
        ) -> Vec<(BackendToken, StdIoResult<usize>)> {
            assert!(
                self.inflight.len() >= min_nr,
                "test backend requires enough inflight work to satisfy wait"
            );
            match self
                .wait_batches
                .pop_front()
                .unwrap_or(LogTestCompletionBatch::AllFront)
            {
                LogTestCompletionBatch::AllFront => self
                    .inflight
                    .drain(..)
                    .map(|(token, kind)| (token, Ok(log_test_completion_len(kind))))
                    .collect(),
                LogTestCompletionBatch::OneBack => {
                    let (token, kind) = self
                        .inflight
                        .pop_back()
                        .expect("test backend must have one back completion");
                    vec![(token, Ok(log_test_completion_len(kind)))]
                }
            }
        }
    }

    fn log_test_completion_len(kind: IOKind) -> usize {
        match kind {
            IOKind::Read | IOKind::Write => 4096,
            IOKind::Fsync | IOKind::Fdatasync => 0,
        }
    }

    #[test]
    fn test_wait_and_drain_buffered_completions_batches_same_file_sync() {
        smol::block_on(async {
            let (_engine_temp_dir, engine) =
                build_redo_test_engine("buffered_completion_batch", LogSync::None).await;
            let temp_dir = TempDir::new().unwrap();
            let file_prefix = temp_dir
                .path()
                .join("standalone_buffered_batch_redo.log")
                .to_str()
                .unwrap()
                .to_owned();
            let (redo_log, _initial_header) = finish_redo_log_for_test(file_prefix, 2);
            let log_fd = redo_log
                .group_commit
                .lock()
                .log_file
                .as_ref()
                .unwrap()
                .as_raw_fd();
            let config = TrxSysConfig::default()
                .log_block_size(4096usize)
                .log_sync(LogSync::Fsync);
            let harness = manual_log_processor_harness(&engine, config, redo_log);
            let hook = RecordingRedoSyncHook::new();
            let _install = install_storage_backend_test_hook(Arc::new(hook.clone()));
            let mut write_driver = LogWriteDriver::new(LogTestBackend::complete_all(2));
            let mut sealer = LogFileSealer::new(&harness.trx_sys.config);

            {
                let mut writer = RedoLogWriter::new(
                    &harness.trx_sys,
                    &harness.trx_sys.config,
                    &mut write_driver,
                );
                writer
                    .prefix
                    .push_group(sync_group_with_pending_write_for_test(
                        TrxID::new(90),
                        log_fd,
                        REDO_DEFAULT_DATA_START_OFFSET,
                    ));
                writer
                    .prefix
                    .push_group(sync_group_with_pending_write_for_test(
                        TrxID::new(91),
                        log_fd,
                        REDO_DEFAULT_DATA_START_OFFSET + 4096,
                    ));

                writer.submit_io();
                writer.wait_and_drain_io_if_submitted(&mut sealer);
                writer.advance_ordered_prefix(&mut sealer);
                writer.submit_io();
                writer.wait_and_drain_io_if_submitted(&mut sealer);
                writer.advance_ordered_prefix(&mut sealer);

                assert!(writer.prefix.is_empty());
                assert_eq!(writer.write_driver.submitted_len(), 0);
            }

            let calls = hook.calls();
            assert_eq!(calls.len(), 1);
            assert_eq!(calls[0].kind(), IOKind::Fsync);
            assert_eq!(calls[0].fd(), log_fd);
            assert_eq!(
                harness
                    .trx_sys
                    .redo_log
                    .persisted_cts
                    .load(Ordering::SeqCst),
                91
            );
            assert_eq!(
                harness
                    .trx_sys
                    .redo_log
                    .stats
                    .commit_count
                    .load(Ordering::Relaxed),
                2
            );
            assert_eq!(
                harness
                    .trx_sys
                    .redo_log
                    .stats
                    .sync_count
                    .load(Ordering::Relaxed),
                1
            );

            drop(harness);
            engine.shutdown().unwrap();
        });
    }

    #[test]
    fn test_wait_and_drain_keeps_later_ready_group_behind_unfinished_prefix() {
        smol::block_on(async {
            let (_engine_temp_dir, engine) =
                build_redo_test_engine("buffered_completion_order", LogSync::None).await;
            let temp_dir = TempDir::new().unwrap();
            let file_prefix = temp_dir
                .path()
                .join("standalone_buffered_order_redo.log")
                .to_str()
                .unwrap()
                .to_owned();
            let (redo_log, _initial_header) = finish_redo_log_for_test(file_prefix, 2);
            let log_fd = redo_log
                .group_commit
                .lock()
                .log_file
                .as_ref()
                .unwrap()
                .as_raw_fd();
            let config = TrxSysConfig::default()
                .log_block_size(4096usize)
                .log_sync(LogSync::Fsync);
            let harness = manual_log_processor_harness(&engine, config, redo_log);
            let hook = RecordingRedoSyncHook::new();
            let _install = install_storage_backend_test_hook(Arc::new(hook.clone()));
            let mut write_driver = LogWriteDriver::new(LogTestBackend::complete_one_back(2));
            let mut sealer = LogFileSealer::new(&harness.trx_sys.config);

            {
                let mut writer = RedoLogWriter::new(
                    &harness.trx_sys,
                    &harness.trx_sys.config,
                    &mut write_driver,
                );
                writer
                    .prefix
                    .push_group(sync_group_with_pending_write_for_test(
                        TrxID::new(92),
                        log_fd,
                        REDO_DEFAULT_DATA_START_OFFSET,
                    ));
                writer
                    .prefix
                    .push_group(sync_group_with_pending_write_for_test(
                        TrxID::new(93),
                        log_fd,
                        REDO_DEFAULT_DATA_START_OFFSET + 4096,
                    ));

                writer.submit_io();
                writer.wait_and_drain_io_if_submitted(&mut sealer);
                writer.advance_ordered_prefix(&mut sealer);

                assert_eq!(writer.prefix.len(), 2);
                let LogPrefixKind::Group { group } = &writer.prefix.entries[0].kind else {
                    panic!("expected first group")
                };
                assert!(!group.ready());
                let LogPrefixKind::Group { group } = &writer.prefix.entries[1].kind else {
                    panic!("expected second group")
                };
                assert!(group.ready());
                assert_eq!(writer.write_driver.submitted_len(), 1);
            }

            assert!(hook.calls().is_empty());
            assert_eq!(
                harness
                    .trx_sys
                    .redo_log
                    .persisted_cts
                    .load(Ordering::SeqCst),
                MIN_SNAPSHOT_TS.as_u64()
            );

            drop(harness);
            engine.shutdown().unwrap();
        });
    }

    #[test]
    fn test_drain_pending_prefix_finalizes_finished_prefix_without_submitted_write() {
        smol::block_on(async {
            let (_engine_temp_dir, engine) =
                build_redo_test_engine("finish_pending_no_submitted_write", LogSync::None).await;
            let temp_dir = TempDir::new().unwrap();
            let file_prefix = temp_dir
                .path()
                .join("standalone_redo.log")
                .to_str()
                .unwrap()
                .to_owned();
            let (redo_log, _initial_header) = finish_redo_log_for_test(file_prefix, 1);
            let config = TrxSysConfig::default()
                .log_block_size(4096usize)
                .log_sync(LogSync::None);
            let harness = manual_log_processor_harness(&engine, config, redo_log);
            let mut write_driver = LogWriteDriver::new(StorageBackend::new(1).unwrap());
            let mut sealer = LogFileSealer::new(&harness.trx_sys.config);
            let cts = TrxID::new(50);

            {
                let mut fp = RedoLogWriter::new(
                    &harness.trx_sys,
                    &harness.trx_sys.config,
                    &mut write_driver,
                );
                fp.prefix
                    .push_group(sync_group_for_order_test(cts, true, 0));

                drain_pending_prefix_for_test(&mut fp, &mut sealer);

                assert!(fp.prefix.is_empty());
                assert_eq!(fp.write_driver.pending_len(), 0);
                assert_eq!(fp.write_driver.submitted_len(), 0);
            }

            assert_eq!(
                harness
                    .trx_sys
                    .redo_log
                    .persisted_cts
                    .load(Ordering::SeqCst),
                cts.as_u64()
            );
            drop(harness);
            engine.shutdown().unwrap();
        });
    }

    #[test]
    fn test_log_file_boundary_header_request_precedes_following_group_request() {
        smol::block_on(async {
            let (_engine_temp_dir, engine) =
                build_redo_test_engine("header_and_group_submit", LogSync::None).await;
            let temp_dir = TempDir::new().unwrap();
            let file_prefix = temp_dir
                .path()
                .join("standalone_header_group_redo.log")
                .to_str()
                .unwrap()
                .to_owned();
            let (redo_log, _initial_header) = finish_redo_log_for_test(file_prefix, 2);
            let config = TrxSysConfig::default()
                .log_block_size(4096usize)
                .log_sync(LogSync::None);
            let harness = manual_log_processor_harness(&engine, config, redo_log);
            let redo_log = &*harness.trx_sys.redo_log;
            let cts = TrxID::new(55);
            let (log_fd, group_offset) = {
                let mut group_commit_g = redo_log.group_commit.lock();
                let log_file = group_commit_g.log_file.as_ref().unwrap();
                let log_fd = log_file.as_raw_fd();
                let file_seq = log_file.file_seq();
                let log_group = LogBlockGroup::new(
                    4096,
                    TrxLog::new(
                        RedoHeader {
                            cts,
                            trx_kind: RedoTrxKind::System,
                        },
                        RedoLogs::default(),
                    ),
                )
                .unwrap();
                let physical_len = log_group.physical_len();
                let (offset, _) = log_file.alloc(physical_len).unwrap();
                group_commit_g.queue.push_back(Commit::Group(CommitGroup {
                    trx_list: vec![PrecommitTrx {
                        cts,
                        redo_bin: None,
                        payload: None,
                        attachment: None,
                        lock_manager: None,
                        lock_state: None,
                    }],
                    max_cts: cts,
                    log: Some(CommitGroupLog {
                        alloc: RedoGroupWriteAlloc {
                            file_seq,
                            fd: log_fd,
                            offset,
                            end_offset: offset + physical_len,
                        },
                        group: log_group,
                    }),
                    completion: Arc::new(Completion::new()),
                }));
                (log_fd, offset)
            };
            assert_eq!(group_offset, REDO_DEFAULT_DATA_START_OFFSET);

            let hook = RecordingRedoWriteSubmitHook::default();
            let _install = install_storage_backend_test_hook(Arc::new(hook.clone()));
            let mut write_driver = LogWriteDriver::new(StorageBackend::new(2).unwrap());
            let mut sealer = LogFileSealer::new(&harness.trx_sys.config);
            {
                let mut fp = RedoLogWriter::new(
                    &harness.trx_sys,
                    &harness.trx_sys.config,
                    &mut write_driver,
                );
                fp.fetch_io_reqs();
                fp.submit_io();

                assert_eq!(
                    hook.submits(),
                    vec![
                        StorageBackendOp::new(IOKind::Write, log_fd, 0),
                        StorageBackendOp::new(IOKind::Write, log_fd, group_offset),
                    ]
                );
                assert_eq!(fp.next_request_id, 2);
                assert_eq!(fp.prefix.driver_owned_len(), 2);

                drain_pending_prefix_for_test(&mut fp, &mut sealer);
                assert!(fp.prefix.is_empty());
                assert_eq!(fp.write_driver.pending_len(), 0);
            }

            assert_eq!(
                harness
                    .trx_sys
                    .redo_log
                    .persisted_cts
                    .load(Ordering::SeqCst),
                cts.as_u64()
            );
            drop(harness);
            engine.shutdown().unwrap();
        });
    }

    #[test]
    fn test_fetch_io_reqs_materializes_group_with_recycled_redo_buffers() {
        smol::block_on(async {
            let (_engine_temp_dir, engine) =
                build_redo_test_engine("fetch_reuses_redo_buffer", LogSync::None).await;
            let temp_dir = TempDir::new().unwrap();
            let file_prefix = temp_dir
                .path()
                .join("standalone_reuse_redo.log")
                .to_str()
                .unwrap()
                .to_owned();
            let (redo_log, _initial_header) = finish_redo_log_for_test(file_prefix, 2);
            let config = TrxSysConfig::default()
                .log_block_size(4096usize)
                .log_sync(LogSync::None);
            let harness = manual_log_processor_harness(&engine, config, redo_log);
            let redo_log = &*harness.trx_sys.redo_log;
            let cts = TrxID::new(58);

            {
                let mut group_commit_g = redo_log.group_commit.lock();
                let log_file = group_commit_g.log_file.as_ref().unwrap();
                let file_seq = log_file.file_seq();
                let log_fd = log_file.as_raw_fd();
                let log_group = LogBlockGroup::new(4096, oversized_redo_log_for_test(cts)).unwrap();
                let physical_len = log_group.physical_len();
                assert!(physical_len > 4096);
                let (offset, _) = log_file.alloc(physical_len).unwrap();
                group_commit_g.queue.push_back(Commit::Group(CommitGroup {
                    trx_list: vec![PrecommitTrx {
                        cts,
                        redo_bin: None,
                        payload: None,
                        attachment: None,
                        lock_manager: None,
                        lock_state: None,
                    }],
                    max_cts: cts,
                    log: Some(CommitGroupLog {
                        alloc: RedoGroupWriteAlloc {
                            file_seq,
                            fd: log_fd,
                            offset,
                            end_offset: offset + physical_len,
                        },
                        group: log_group,
                    }),
                    completion: Arc::new(Completion::new()),
                }));
            }

            let mut write_driver = LogWriteDriver::new(StorageBackend::new(2).unwrap());
            {
                let mut fp = RedoLogWriter::new(
                    &harness.trx_sys,
                    &harness.trx_sys.config,
                    &mut write_driver,
                );
                let reusable1 = DirectBuf::zeroed(4096);
                let reusable1_ptr = reusable1.as_bytes().as_ptr() as usize;
                let reusable2 = DirectBuf::zeroed(4096);
                let reusable2_ptr = reusable2.as_bytes().as_ptr() as usize;
                fp.recycle_bufs(vec![reusable1, reusable2]);

                fp.fetch_io_reqs();

                let group = fp
                    .prefix
                    .entries
                    .iter()
                    .find_map(|entry| match &entry.kind {
                        LogPrefixKind::Group { group } => Some(group),
                        _ => None,
                    })
                    .expect("queued redo group must enter prefix");
                let buf_ptrs = group
                    .writes
                    .iter()
                    .map(|submission| {
                        let buf = submission
                            .operation
                            .buf()
                            .expect("group write submission must own a buffer");
                        buf.as_bytes().as_ptr() as usize
                    })
                    .collect::<Vec<_>>();
                assert!(buf_ptrs.contains(&reusable1_ptr));
                assert!(buf_ptrs.contains(&reusable2_ptr));
            }

            drop(harness);
            engine.shutdown().unwrap();
        });
    }

    #[test]
    fn test_drain_pending_prefix_syncs_boundary_ended_log_file() {
        smol::block_on(async {
            let (_engine_temp_dir, engine) =
                build_redo_test_engine("finish_pending_switch_syncer", LogSync::None).await;
            let temp_dir = TempDir::new().unwrap();
            let file_prefix = temp_dir
                .path()
                .join("standalone_switch_redo.log")
                .to_str()
                .unwrap()
                .to_owned();
            let (redo_log, _initial_header) = finish_redo_log_for_test(file_prefix, 1);

            let (ended_fd, current_fd) = {
                let mut group_commit_g = redo_log.group_commit.lock();
                let ended_fd = group_commit_g.log_file.as_ref().unwrap().as_raw_fd();
                redo_log.rotate_log_file(&mut group_commit_g).unwrap();
                let current_fd = group_commit_g.log_file.as_ref().unwrap().as_raw_fd();
                (ended_fd, current_fd)
            };
            assert_ne!(ended_fd, current_fd);

            let hook = RecordingRedoSyncHook::new();
            let _install = install_storage_backend_test_hook(Arc::new(hook.clone()));
            let config = TrxSysConfig::default()
                .log_block_size(4096usize)
                .log_sync(LogSync::Fsync);
            let harness = manual_log_processor_harness(&engine, config, redo_log);
            let mut write_driver = LogWriteDriver::new(StorageBackend::new(1).unwrap());
            let mut sealer = LogFileSealer::new(&harness.trx_sys.config);
            let cts = TrxID::new(60);

            {
                let mut fp = RedoLogWriter::new(
                    &harness.trx_sys,
                    &harness.trx_sys.config,
                    &mut write_driver,
                );
                fp.prefix.push_group(sync_group_for_order_test_with_log_fd(
                    cts,
                    true,
                    4096,
                    Some(ended_fd),
                ));
                fp.fetch_io_reqs();

                drain_pending_prefix_for_test(&mut fp, &mut sealer);

                assert!(fp.prefix.is_empty());
                assert_eq!(fp.write_driver.pending_len(), 0);
                assert_eq!(fp.write_driver.submitted_len(), 0);
            }

            assert_eq!(
                harness
                    .trx_sys
                    .redo_log
                    .persisted_cts
                    .load(Ordering::SeqCst),
                cts.as_u64()
            );
            let calls = hook.calls();
            assert_eq!(calls.len(), 2);
            for call in calls {
                assert_eq!(call.kind(), IOKind::Fsync);
                assert_eq!(call.fd(), ended_fd);
                assert_ne!(call.fd(), current_fd);
            }

            drop(harness);
            engine.shutdown().unwrap();
        });
    }

    #[test]
    fn test_drain_pending_prefix_drains_prefix_owned_seal() {
        smol::block_on(async {
            let (_engine_temp_dir, engine) =
                build_redo_test_engine("finish_pending_with_prefix_seal", LogSync::None).await;
            let temp_dir = TempDir::new().unwrap();
            let ended_prefix = temp_dir
                .path()
                .join("standalone_prefix_seal_redo.log")
                .to_str()
                .unwrap()
                .to_owned();
            let active_prefix = temp_dir
                .path()
                .join("standalone_pending_seal_active_redo.log")
                .to_str()
                .unwrap()
                .to_owned();
            let ended_log_file = create_log_file_for_test(&ended_prefix, 0, 128 * 1024, 4096);
            let (redo_log, _initial_header) = finish_redo_log_for_test(active_prefix, 1);
            let config = TrxSysConfig::default()
                .log_block_size(4096usize)
                .log_sync(LogSync::None);
            let harness = manual_log_processor_harness(&engine, config, redo_log);
            let mut write_driver = LogWriteDriver::new(StorageBackend::new(1).unwrap());
            let mut sealer = LogFileSealer::new(&harness.trx_sys.config);

            finish_prefix_seal_for_test(&harness, &mut write_driver, &mut sealer, ended_log_file);

            drop(harness);
            engine.shutdown().unwrap();
        });
    }

    fn assert_rotated_file_seal_sync_policy(
        log_sync: LogSync,
        expected_sync_kind: Option<IOKind>,
        log_file_stem: &str,
    ) {
        smol::block_on(async {
            let (_engine_temp_dir, engine) =
                build_redo_test_engine(log_file_stem, LogSync::None).await;
            let temp_dir = TempDir::new().unwrap();
            let file_prefix = temp_dir
                .path()
                .join("standalone_seal_redo.log")
                .to_str()
                .unwrap()
                .to_owned();
            let harness_prefix = temp_dir
                .path()
                .join("standalone_seal_harness_redo.log")
                .to_str()
                .unwrap()
                .to_owned();
            let ended_log_file = create_log_file_for_test(&file_prefix, 0, 128 * 1024, 4096);
            let ended_fd = ended_log_file.as_raw_fd();
            let hook = RecordingRedoSyncHook::new();
            let _install = install_storage_backend_test_hook(Arc::new(hook.clone()));
            let (harness, mut write_driver) =
                manual_redo_log_writer_for_seal(&engine, log_sync, harness_prefix);
            let end_offset = REDO_DEFAULT_DATA_START_OFFSET + 4096;

            let mut sealer = LogFileSealer::new(&harness.trx_sys.config);
            record_seal_group(
                &mut sealer,
                &ended_log_file,
                TrxID::new(70),
                TrxID::new(72),
                end_offset,
            );
            finish_prefix_seal_for_test(&harness, &mut write_driver, &mut sealer, ended_log_file);

            let bytes = fs::read(format!("{file_prefix}.00000000")).unwrap();
            let slot1 = parse_redo_super_block(
                &bytes[REDO_SUPER_BLOCK_SLOT_SIZE..][..REDO_SUPER_BLOCK_SLOT_SIZE],
                0,
                1,
            )
            .unwrap();
            assert_eq!(slot1.slot_no, 1);
            assert_eq!(slot1.generation, 1);
            assert_eq!(slot1.durable_end_offset, end_offset as u64);
            assert_eq!(slot1.min_redo_cts, 70);
            assert_eq!(slot1.max_redo_cts, 72);

            let calls = hook.calls();
            match expected_sync_kind {
                Some(kind) => {
                    assert_eq!(calls.len(), 1);
                    assert_eq!(calls[0].kind(), kind);
                    assert_eq!(calls[0].fd(), ended_fd);
                }
                None => assert!(calls.is_empty()),
            }

            drop(harness);
            engine.shutdown().unwrap();
        });
    }

    #[test]
    fn test_rotated_file_seal_writes_inactive_slot_and_fsyncs_ended_fd() {
        assert_rotated_file_seal_sync_policy(
            LogSync::Fsync,
            Some(IOKind::Fsync),
            "rotated_seal_fsync",
        );
    }

    #[test]
    fn test_rotated_file_seal_fdatasyncs_ended_fd() {
        assert_rotated_file_seal_sync_policy(
            LogSync::Fdatasync,
            Some(IOKind::Fdatasync),
            "rotated_seal_fdatasync",
        );
    }

    #[test]
    fn test_rotated_file_seal_with_log_sync_none_skips_sync_syscall() {
        assert_rotated_file_seal_sync_policy(LogSync::None, None, "rotated_seal_no_sync");
    }

    #[test]
    fn test_rotated_file_seal_write_failure_poisons_storage() {
        smol::block_on(async {
            let (_engine_temp_dir, engine) =
                build_redo_test_engine("rotated_seal_write_failure", LogSync::None).await;
            let temp_dir = TempDir::new().unwrap();
            let file_prefix = temp_dir
                .path()
                .join("standalone_seal_write_fail_redo.log")
                .to_str()
                .unwrap()
                .to_owned();
            let harness_prefix = temp_dir
                .path()
                .join("standalone_seal_write_fail_harness_redo.log")
                .to_str()
                .unwrap()
                .to_owned();
            let ended_log_file = create_log_file_for_test(&file_prefix, 0, 128 * 1024, 4096);
            let hook = ControlledRedoWriteHook::new(ended_log_file.as_raw_fd(), libc::EIO);
            hook.release();
            let _install = install_storage_backend_test_hook(Arc::new(hook));
            let (harness, mut write_driver) =
                manual_redo_log_writer_for_seal(&engine, LogSync::None, harness_prefix);

            let mut sealer = LogFileSealer::new(&harness.trx_sys.config);
            finish_prefix_seal_for_test(&harness, &mut write_driver, &mut sealer, ended_log_file);

            assert!(
                harness
                    .trx_sys
                    .storage_poison_error()
                    .as_ref()
                    .is_some_and(|err| *err.current_context() == FatalError::RedoWrite)
            );

            drop(harness);
            engine.shutdown().unwrap();
        });
    }

    #[test]
    fn test_rotated_file_seal_sync_failure_poisons_storage() {
        smol::block_on(async {
            let (_engine_temp_dir, engine) =
                build_redo_test_engine("rotated_seal_sync_failure", LogSync::None).await;
            let temp_dir = TempDir::new().unwrap();
            let file_prefix = temp_dir
                .path()
                .join("standalone_seal_sync_fail_redo.log")
                .to_str()
                .unwrap()
                .to_owned();
            let harness_prefix = temp_dir
                .path()
                .join("standalone_seal_sync_fail_harness_redo.log")
                .to_str()
                .unwrap()
                .to_owned();
            let ended_log_file = create_log_file_for_test(&file_prefix, 0, 128 * 1024, 4096);
            let hook =
                ControlledRedoSyncHook::new(ended_log_file.as_raw_fd(), IOKind::Fsync, libc::EIO);
            hook.release();
            let _install = install_storage_backend_test_hook(Arc::new(hook));
            let (harness, mut write_driver) =
                manual_redo_log_writer_for_seal(&engine, LogSync::Fsync, harness_prefix);

            let mut sealer = LogFileSealer::new(&harness.trx_sys.config);
            finish_prefix_seal_for_test(&harness, &mut write_driver, &mut sealer, ended_log_file);

            assert!(
                harness
                    .trx_sys
                    .storage_poison_error()
                    .as_ref()
                    .is_some_and(|err| *err.current_context() == FatalError::RedoSync)
            );

            drop(harness);
            engine.shutdown().unwrap();
        });
    }

    #[test]
    fn test_clean_shutdown_active_seal_failure_is_best_effort() {
        smol::block_on(async {
            let (_engine_temp_dir, engine) =
                build_redo_test_engine("active_seal_best_effort", LogSync::None).await;
            let temp_dir = TempDir::new().unwrap();
            let file_prefix = temp_dir
                .path()
                .join("standalone_active_seal_fail_redo.log")
                .to_str()
                .unwrap()
                .to_owned();
            let (harness, mut write_driver) =
                manual_redo_log_writer_for_seal(&engine, LogSync::None, file_prefix);
            let active_fd = {
                harness
                    .trx_sys
                    .redo_log
                    .group_commit
                    .lock()
                    .log_file
                    .as_ref()
                    .unwrap()
                    .as_raw_fd()
            };
            let hook = ControlledRedoWriteHook::new(active_fd, libc::EIO);
            hook.release();
            let _install = install_storage_backend_test_hook(Arc::new(hook));

            let mut sealer = LogFileSealer::new(&harness.trx_sys.config);
            sealer.seal_active_file_best_effort(&harness.trx_sys, &mut write_driver);

            assert!(harness.trx_sys.storage_poison_error().is_none());
            assert_eq!(
                harness
                    .trx_sys
                    .redo_log
                    .stats
                    .seal_failure_count
                    .load(Ordering::Relaxed),
                1
            );

            drop(harness);
            engine.shutdown().unwrap();
        });
    }

    #[test]
    fn test_clean_shutdown_seals_active_file_after_pending_work_drains() {
        smol::block_on(async {
            let (_temp_dir, engine) =
                build_redo_test_engine("active_seal_success", LogSync::None).await;
            let file_prefix = engine.inner().trx_sys.config.file_prefix().unwrap();
            let mut sys_trx = engine.inner().trx_sys.begin_sys_trx();
            sys_trx.create_row_page(
                TableID::from(1000u64),
                PageID::from(1000u64),
                RowID::new(0),
                RowID::new(1),
            );
            let cts = engine.inner().trx_sys.commit_sys(sys_trx).unwrap();

            engine.shutdown().unwrap();

            let bytes = fs::read(format!("{file_prefix}.00000000")).unwrap();
            let sealed = parse_redo_super_block(
                &bytes[REDO_SUPER_BLOCK_SLOT_SIZE..][..REDO_SUPER_BLOCK_SLOT_SIZE],
                0,
                1,
            )
            .unwrap();
            assert!(sealed.is_sealed());
            assert_eq!(sealed.generation, 1);
            assert_eq!(sealed.min_redo_cts, cts.as_u64());
            assert_eq!(sealed.max_redo_cts, cts.as_u64());
            assert!(sealed.durable_end_offset > REDO_DEFAULT_DATA_START_OFFSET as u64);
        });
    }

    #[test]
    fn test_fail_prefix_entries_marks_unprepared_seal_ready() {
        smol::block_on(async {
            let (_engine_temp_dir, engine) =
                build_redo_test_engine("prefix_failed_unprepared_seal", LogSync::None).await;
            let temp_dir = TempDir::new().unwrap();
            let ended_prefix = temp_dir
                .path()
                .join("standalone_prefix_failed_seal_ended_redo.log")
                .to_str()
                .unwrap()
                .to_owned();
            let active_prefix = temp_dir
                .path()
                .join("standalone_prefix_failed_seal_active_redo.log")
                .to_str()
                .unwrap()
                .to_owned();
            let ended_log_file = create_log_file_for_test(&ended_prefix, 0, 128 * 1024, 4096);
            let (header_write, header_completion) = LogWriteSubmission::header(
                ended_log_file.as_raw_fd(),
                0,
                DirectBuf::zeroed(REDO_SUPER_BLOCK_SLOT_SIZE),
            );
            let (redo_log, _initial_header) = finish_redo_log_for_test(active_prefix, 1);
            let config = TrxSysConfig::default()
                .log_block_size(4096usize)
                .log_sync(LogSync::None);
            let harness = manual_log_processor_harness(&engine, config, redo_log);
            let mut write_driver = LogWriteDriver::new(StorageBackend::new(1).unwrap());
            let mut writer =
                RedoLogWriter::new(&harness.trx_sys, &harness.trx_sys.config, &mut write_driver);
            let mut sealer = LogFileSealer::new(&harness.trx_sys.config);

            writer.prefix.push_header(header_write);
            writer.prefix.push_seal(ended_log_file);
            writer.fail_prefix_entries(FatalError::RedoWrite);
            writer.advance_ordered_prefix(&mut sealer);

            assert!(writer.prefix.is_empty());
            assert!(header_completion.completed_result().unwrap().is_err());

            drop(harness);
            engine.shutdown().unwrap();
        });
    }

    #[test]
    fn test_discover_redo_log_files_escapes_directory_metacharacters() {
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().join("redo[dir]");
        fs::create_dir(&log_dir).unwrap();

        let file_prefix = log_dir.join("redo.log");
        let file_prefix = file_prefix.to_str().unwrap();
        let expected = [
            PathBuf::from(format!("{file_prefix}.00000000")),
            PathBuf::from(format!("{file_prefix}.00000001")),
            PathBuf::from(format!("{file_prefix}.00000002")),
        ];
        for file_seq in 0..expected.len() as u32 {
            drop(create_log_file_for_test(
                file_prefix,
                file_seq,
                128 * 1024,
                4096,
            ));
        }
        File::create(log_dir.join("redo.logx.00000000")).unwrap();

        let asc = discover_redo_log_files(file_prefix, 0, false).unwrap();
        assert_eq!(
            expected.to_vec(),
            asc.iter()
                .map(|descriptor| descriptor.path.clone())
                .collect::<Vec<_>>()
        );
        assert_eq!(
            vec![0, 1, 2],
            asc.iter()
                .map(|descriptor| descriptor.file_seq)
                .collect::<Vec<_>>()
        );

        let desc = discover_redo_log_files(file_prefix, 0, true).unwrap();
        assert_eq!(
            expected.iter().rev().cloned().collect::<Vec<_>>(),
            desc.iter()
                .map(|descriptor| descriptor.path.clone())
                .collect::<Vec<_>>()
        );
        assert_eq!(
            vec![2, 1, 0],
            desc.iter()
                .map(|descriptor| descriptor.file_seq)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_discover_redo_log_files_rejects_missing_prefix() {
        let temp_dir = TempDir::new().unwrap();
        let file_prefix = temp_dir.path().join("redo.log");
        let file_prefix = file_prefix.to_str().unwrap();
        drop(create_log_file_for_test(file_prefix, 1, 128 * 1024, 4096));

        let err = discover_redo_log_files(file_prefix, 0, false).unwrap_err();
        assert_eq!(
            err.data_integrity_error(),
            Some(DataIntegrityError::RedoLogSequenceGap)
        );
        let report = format!("{err:?}");
        assert!(report.contains("00000000"), "{report}");
        assert!(report.contains("prefix"), "{report}");
        assert!(report.contains(file_prefix), "{report}");
    }

    #[test]
    fn test_discover_redo_log_files_rejects_sequence_gap() {
        let temp_dir = TempDir::new().unwrap();
        let file_prefix = temp_dir.path().join("redo.log");
        let file_prefix = file_prefix.to_str().unwrap();
        drop(create_log_file_for_test(file_prefix, 0, 128 * 1024, 4096));
        drop(create_log_file_for_test(file_prefix, 2, 128 * 1024, 4096));

        let err = discover_redo_log_files(file_prefix, 0, false).unwrap_err();
        assert_eq!(
            err.data_integrity_error(),
            Some(DataIntegrityError::RedoLogSequenceGap)
        );
        let report = format!("{err:?}");
        assert!(report.contains("00000001"), "{report}");
        assert!(report.contains(file_prefix), "{report}");
    }

    #[test]
    fn test_discover_redo_log_files_accepts_retained_suffix_without_prefix() {
        let temp_dir = TempDir::new().unwrap();
        let file_prefix = temp_dir.path().join("redo.log");
        let file_prefix = file_prefix.to_str().unwrap();
        drop(create_log_file_for_test(file_prefix, 2, 128 * 1024, 4096));
        drop(create_log_file_for_test(file_prefix, 3, 128 * 1024, 4096));

        let descriptors = discover_redo_log_files(file_prefix, 2, false).unwrap();
        assert_eq!(
            descriptors
                .iter()
                .map(|descriptor| descriptor.file_seq)
                .collect::<Vec<_>>(),
            vec![2, 3]
        );
    }

    #[test]
    fn test_discover_redo_log_files_excludes_obsolete_prefix_below_marker() {
        let temp_dir = TempDir::new().unwrap();
        let file_prefix = temp_dir.path().join("redo.log");
        let file_prefix = file_prefix.to_str().unwrap();
        for file_seq in 0..4 {
            drop(create_log_file_for_test(
                file_prefix,
                file_seq,
                128 * 1024,
                4096,
            ));
        }

        let descriptors = discover_redo_log_files(file_prefix, 2, true).unwrap();
        assert_eq!(
            descriptors
                .iter()
                .map(|descriptor| descriptor.file_seq)
                .collect::<Vec<_>>(),
            vec![3, 2]
        );
    }

    #[test]
    fn test_discover_redo_log_files_rejects_empty_retained_suffix() {
        let temp_dir = TempDir::new().unwrap();
        let file_prefix = temp_dir.path().join("redo.log");
        let file_prefix = file_prefix.to_str().unwrap();
        drop(create_log_file_for_test(file_prefix, 0, 128 * 1024, 4096));
        drop(create_log_file_for_test(file_prefix, 1, 128 * 1024, 4096));

        let err = discover_redo_log_files(file_prefix, 2, false).unwrap_err();
        assert_eq!(
            err.data_integrity_error(),
            Some(DataIntegrityError::RedoLogSequenceGap)
        );
        let report = format!("{err:?}");
        assert!(
            report.contains("first retained sequence 00000002"),
            "{report}"
        );
    }

    #[test]
    fn test_discover_redo_log_files_rejects_missing_marker_file() {
        let temp_dir = TempDir::new().unwrap();
        let file_prefix = temp_dir.path().join("redo.log");
        let file_prefix = file_prefix.to_str().unwrap();
        drop(create_log_file_for_test(file_prefix, 3, 128 * 1024, 4096));

        let err = discover_redo_log_files(file_prefix, 2, false).unwrap_err();
        assert_eq!(
            err.data_integrity_error(),
            Some(DataIntegrityError::RedoLogSequenceGap)
        );
        let report = format!("{err:?}");
        assert!(report.contains("00000002"), "{report}");
        assert!(report.contains(file_prefix), "{report}");
    }

    #[test]
    fn test_discover_redo_log_files_rejects_gap_above_marker() {
        let temp_dir = TempDir::new().unwrap();
        let file_prefix = temp_dir.path().join("redo.log");
        let file_prefix = file_prefix.to_str().unwrap();
        drop(create_log_file_for_test(file_prefix, 2, 128 * 1024, 4096));
        drop(create_log_file_for_test(file_prefix, 4, 128 * 1024, 4096));

        let err = discover_redo_log_files(file_prefix, 2, false).unwrap_err();
        assert_eq!(
            err.data_integrity_error(),
            Some(DataIntegrityError::RedoLogSequenceGap)
        );
        let report = format!("{err:?}");
        assert!(report.contains("00000003"), "{report}");
        assert!(report.contains(file_prefix), "{report}");
    }

    #[test]
    fn test_discover_redo_log_files_rejects_legacy_partitioned_files() {
        let temp_dir = TempDir::new().unwrap();
        let file_prefix = temp_dir.path().join("redo.log");
        let file_prefix = file_prefix.to_str().unwrap();
        File::create(format!("{file_prefix}.0.00000000")).unwrap();

        let err = discover_redo_log_files(file_prefix, 0, false).unwrap_err();
        let report = format!("{err:?}");
        assert!(report.contains("unsupported legacy partitioned redo log file"));
    }

    #[test]
    fn test_redo_segment_metadata_reads_valid_super_block_without_reader() {
        let temp_dir = TempDir::new().unwrap();
        let file_prefix = temp_dir.path().join("redo.log");
        let file_prefix = file_prefix.to_str().unwrap();
        let expected_path =
            create_sealed_log_file_for_test(file_prefix, 0, REDO_DEFAULT_DATA_START_OFFSET, None);

        let descriptors = discover_redo_log_files(file_prefix, 0, false).unwrap();
        assert_eq!(descriptors.len(), 1);
        let segment = RedoLogSegment::from_descriptor(descriptors[0].clone()).unwrap();
        assert_eq!(segment.path, expected_path);
        assert_eq!(segment.file_seq, 0);
        assert!(segment.super_block.is_sealed());
        assert!(segment.sealed_empty());
    }

    #[test]
    fn test_redo_replay_plan_stops_before_invalid_obsolete_prefix() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let file_prefix = temp_dir.path().join("redo.log");
            let file_prefix = file_prefix.to_str().unwrap();
            let invalid_prefix = File::create(format!("{file_prefix}.00000000")).unwrap();
            invalid_prefix.set_len(128 * 1024).unwrap();
            drop(invalid_prefix);
            create_sealed_log_file_for_test(
                file_prefix,
                1,
                REDO_DEFAULT_DATA_START_OFFSET + 4096,
                Some((TrxID::new(10), TrxID::new(20))),
            );
            create_sealed_log_file_for_test(file_prefix, 2, REDO_DEFAULT_DATA_START_OFFSET, None);
            let descriptors = discover_redo_log_files(file_prefix, 0, false).unwrap();
            let planner = RedoReplayPlanner::new(descriptors);

            let planned = planner.plan_recovery(TrxID::new(15), 1).unwrap();
            assert_eq!(planned.skipped_max_recovered_cts, None);
            let mut stream = planned.stream;
            let err = stream.try_next().await.unwrap_err();
            assert_eq!(
                err.data_integrity_error(),
                Some(DataIntegrityError::LogFileCorrupted)
            );
        });
    }

    #[test]
    fn test_redo_replay_planner_can_build_independent_empty_streams() {
        smol::block_on(async {
            let planner = RedoReplayPlanner::new(Vec::new());
            let mut stream1 = planner.plan_recovery(TrxID::new(10), 1).unwrap().stream;
            let mut stream2 = planner.plan_recovery(TrxID::new(11), 1).unwrap().stream;

            assert!(stream1.try_next().await.unwrap().is_none());
            assert!(stream2.try_next().await.unwrap().is_none());
        });
    }

    #[test]
    fn test_redo_replay_plan_skips_obsolete_sealed_segment_and_seeds_cts() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let file_prefix = temp_dir.path().join("redo.log");
            let file_prefix = file_prefix.to_str().unwrap();
            create_sealed_log_file_for_test(
                file_prefix,
                0,
                REDO_DEFAULT_DATA_START_OFFSET + 4096,
                Some((TrxID::new(10), TrxID::new(20))),
            );
            let logs = discover_redo_log_files(file_prefix, 0, false).unwrap();
            let (finalizer, planner, read_depth) =
                redo_planner_and_finalizer_for_test(file_prefix, 1, 1, 128 * 1024, 4096, logs);

            let planned = planner.plan_recovery(TrxID::new(21), read_depth).unwrap();
            assert_eq!(planned.skipped_max_recovered_cts, Some(TrxID::new(20)));
            let mut stream = planned.stream;
            assert!(stream.try_next().await.unwrap().is_none());
            assert_eq!(finalizer.next_file_seq, 1);
        });
    }

    #[test]
    fn test_redo_replay_plan_skips_sealed_empty_without_cts_seed() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let file_prefix = temp_dir.path().join("redo.log");
            let file_prefix = file_prefix.to_str().unwrap();
            create_sealed_log_file_for_test(file_prefix, 0, REDO_DEFAULT_DATA_START_OFFSET, None);
            let logs = discover_redo_log_files(file_prefix, 0, false).unwrap();
            let (finalizer, planner, read_depth) =
                redo_planner_and_finalizer_for_test(file_prefix, 1, 1, 128 * 1024, 4096, logs);

            let planned = planner.plan_recovery(TrxID::new(100), read_depth).unwrap();
            assert_eq!(planned.skipped_max_recovered_cts, None);
            let mut stream = planned.stream;
            assert!(stream.try_next().await.unwrap().is_none());
            assert_eq!(finalizer.next_file_seq, 1);
        });
    }

    #[test]
    fn test_redo_replay_plan_does_not_skip_boundary_equality() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let file_prefix = temp_dir.path().join("redo.log");
            let file_prefix = file_prefix.to_str().unwrap();
            create_sealed_log_file_for_test(
                file_prefix,
                0,
                REDO_DEFAULT_DATA_START_OFFSET + 4096,
                Some((TrxID::new(10), TrxID::new(20))),
            );
            let logs = discover_redo_log_files(file_prefix, 0, false).unwrap();
            let planner = RedoReplayPlanner::new(logs);

            let planned = planner.plan_recovery(TrxID::new(20), 1).unwrap();
            assert_eq!(planned.skipped_max_recovered_cts, None);
            let mut stream = planned.stream;
            let err = stream.try_next().await.unwrap_err();
            assert_eq!(
                err.data_integrity_error(),
                Some(DataIntegrityError::LogFileCorrupted)
            );
        });
    }

    #[test]
    fn test_create_log_file_writes_open_super_block() {
        let temp_dir = TempDir::new().unwrap();
        let file_prefix = temp_dir.path().join("redo.log");
        let file_prefix = file_prefix.to_str().unwrap();
        let log_file = create_log_file_for_test(file_prefix, 3, 128 * 1024, 4096);

        let (offset, _) = log_file.alloc(4096).unwrap();
        assert_eq!(offset, REDO_DEFAULT_DATA_START_OFFSET);
        drop(log_file);

        let bytes = fs::read(format!("{file_prefix}.00000003")).unwrap();
        let slot0 = parse_redo_super_block(&bytes[..REDO_SUPER_BLOCK_SLOT_SIZE], 3, 0).unwrap();
        assert_eq!(slot0.slot_no, 0);
        assert_eq!(slot0.generation, 0);
        assert_eq!(slot0.log_block_size, 4096);
        assert_eq!(slot0.file_max_size, 128 * 1024);

        let slot1 = &bytes[REDO_SUPER_BLOCK_SLOT_SIZE..REDO_DEFAULT_DATA_START_OFFSET];
        let err = parse_redo_super_block(slot1, 3, 1).unwrap_err();
        assert_eq!(
            err.data_integrity_error(),
            Some(DataIntegrityError::InvalidMagic)
        );
    }

    #[test]
    fn test_recovery_finalizer_seals_recovered_redo_file_in_normal_prefix() {
        smol::block_on(async {
            let (_engine_temp_dir, engine) =
                build_redo_test_engine("recovered_seal_prefix", LogSync::None).await;
            let temp_dir = TempDir::new().unwrap();
            let file_prefix = temp_dir.path().join("redo.log");
            let file_prefix = file_prefix.to_str().unwrap();
            let log_file = create_log_file_for_test(file_prefix, 0, 128 * 1024, 4096);
            let open = log_file.super_block();
            drop(log_file);
            let path = PathBuf::from(log_file_name(file_prefix, 0));
            let durable_end_offset = REDO_DEFAULT_DATA_START_OFFSET + 4096;
            let (purge_tx, _purge_rx) = flume::unbounded();
            let mut finalizer =
                RedoLogFinalizer::new(file_prefix.to_owned(), 1, 128 * 1024, 4096, 1);

            finalizer.set_recovered_seal(
                path.clone(),
                open,
                durable_end_offset,
                Some((TrxID::new(70), TrxID::new(72))),
            );
            let (redo_log, header_completion) = finalizer.finalize(purge_tx).unwrap();
            let config = TrxSysConfig::default()
                .log_block_size(4096usize)
                .log_sync(LogSync::None);
            let harness = manual_log_processor_harness(&engine, config, redo_log);
            let mut write_driver = LogWriteDriver::new(StorageBackend::new(1).unwrap());
            let mut sealer = LogFileSealer::new(&harness.trx_sys.config);
            let mut writer =
                RedoLogWriter::new(&harness.trx_sys, &harness.trx_sys.config, &mut write_driver);

            writer.fetch_io_reqs();
            drain_pending_prefix_for_test(&mut writer, &mut sealer);

            assert!(!writer.has_pending_io());
            assert_eq!(writer.write_driver.pending_len(), 0);
            assert_eq!(writer.write_driver.submitted_len(), 0);
            assert!(header_completion.completed_result().unwrap().is_ok());

            let bytes = fs::read(path).unwrap();
            let sealed = parse_redo_super_block(
                &bytes[REDO_SUPER_BLOCK_SLOT_SIZE..][..REDO_SUPER_BLOCK_SLOT_SIZE],
                0,
                1,
            )
            .unwrap();
            assert!(sealed.is_sealed());
            assert_eq!(sealed.generation, 1);
            assert_eq!(sealed.durable_end_offset, durable_end_offset as u64);
            assert_eq!(sealed.min_redo_cts, 70);
            assert_eq!(sealed.max_redo_cts, 72);

            drop(harness);
            engine.shutdown().unwrap();
        });
    }

    #[test]
    fn test_redo_finalizer_recreates_existing_tail_file() {
        let temp_dir = TempDir::new().unwrap();
        let file_prefix = temp_dir.path().join("redo.log");
        let file_prefix = file_prefix.to_str().unwrap();
        create_sealed_log_file_for_test(
            file_prefix,
            0,
            REDO_DEFAULT_DATA_START_OFFSET + 4096,
            Some((TrxID::new(1), TrxID::new(1))),
        );
        let (purge_tx, _purge_rx) = flume::unbounded();
        let mut finalizer = RedoLogFinalizer::new(file_prefix.to_owned(), 1, 128 * 1024, 4096, 0);
        finalizer.set_startup_file(0, RedoLogCreateMode::CreateOrTrunc);
        let (redo_log, header_completion) = finalizer.finalize(purge_tx).unwrap();
        let header_write = {
            let mut group_commit_g = redo_log.group_commit.lock();
            match group_commit_g.queue.pop_front().unwrap() {
                Commit::LogFileBoundary {
                    ended_log_file,
                    header_write,
                } => {
                    assert!(ended_log_file.is_none());
                    header_write
                }
                Commit::Group(_) | Commit::Shutdown => {
                    panic!("expected initial redo super-block write")
                }
            }
        };
        submit_header_write_for_test(header_write, &header_completion);
        drop(redo_log);

        let bytes = fs::read(format!("{file_prefix}.00000000")).unwrap();
        let open = parse_redo_super_block(&bytes[..REDO_SUPER_BLOCK_SLOT_SIZE], 0, 0).unwrap();
        assert!(!open.is_sealed());
        let slot1 = &bytes[REDO_SUPER_BLOCK_SLOT_SIZE..REDO_DEFAULT_DATA_START_OFFSET];
        let err = parse_redo_super_block(slot1, 0, 1).unwrap_err();
        assert_eq!(
            err.data_integrity_error(),
            Some(DataIntegrityError::InvalidMagic)
        );
    }

    #[test]
    fn test_restart_replays_old_log_with_persisted_config_and_creates_new_log_with_current_config()
    {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let file_prefix = temp_dir.path().join("mixed_config_redo.log");
            let file_prefix = file_prefix.to_str().unwrap();
            let old_log_block_size = 8192;
            let old_file_max_size = REDO_DEFAULT_DATA_START_OFFSET + old_log_block_size * 2;
            let new_log_block_size = 4096;
            let new_file_max_size = REDO_DEFAULT_DATA_START_OFFSET + new_log_block_size * 3;

            let old_log_file =
                create_log_file_for_test(file_prefix, 0, old_file_max_size, old_log_block_size);
            let cts = TrxID::new(17);
            let log_group = LogBlockGroup::new(
                old_log_block_size,
                TrxLog::new(
                    RedoHeader {
                        cts,
                        trx_kind: RedoTrxKind::System,
                    },
                    RedoLogs::default(),
                ),
            )
            .unwrap();
            let blocks = log_group
                .finish_with(|count| {
                    (0..count)
                        .map(|_| DirectBuf::zeroed(old_log_block_size))
                        .collect()
                })
                .unwrap();
            let group_len = blocks.iter().map(DirectBuf::capacity).sum();
            let (group_offset, _) = old_log_file.alloc(group_len).unwrap();
            assert_eq!(group_offset, REDO_DEFAULT_DATA_START_OFFSET);
            let mut write_driver = LogWriteDriver::new(StorageBackend::new(1).unwrap());
            for (idx, direct_buf) in blocks.into_iter().enumerate() {
                assert!(
                    write_driver
                        .push_write(LogWriteSubmission::group(
                            old_log_file.as_raw_fd(),
                            group_offset + idx * old_log_block_size,
                            direct_buf,
                            idx,
                        ))
                        .is_ok()
                );
            }
            assert_eq!(write_driver.submit_ready(), 1);
            let LogWriteCompletion {
                owner,
                kind,
                buf,
                poison,
            } = write_driver.wait_at_least_one();
            match kind {
                LogWriteKind::Group { .. } => {
                    drop(buf.expect("redo group write completion must return a buffer"));
                    assert!(owner.is_none());
                    assert_eq!(poison, None);
                }
                LogWriteKind::Header { .. } => panic!("expected redo group write completion"),
                LogWriteKind::SealWrite { .. }
                | LogWriteKind::SealSync { .. }
                | LogWriteKind::CommitSync
                | LogWriteKind::StandaloneSync => panic!("expected redo group write completion"),
            }
            drop(old_log_file);

            let config = TrxSysConfig::default()
                .log_dir(temp_dir.path())
                .log_file_stem("mixed_config_redo.log")
                .log_write_io_depth(7)
                .recovery_io_depth(5)
                .catalog_checkpoint_scan_io_depth(3)
                .log_block_size(new_log_block_size)
                .log_file_max_size(new_file_max_size);
            let file_prefix = config.file_prefix().unwrap();
            let logs = discover_redo_log_files(&file_prefix, 0, false).unwrap();
            let (finalizer, planner, read_depth) = redo_planner_and_finalizer_for_test(
                &file_prefix,
                config.recovery_io_depth,
                config.log_write_io_depth,
                new_file_max_size,
                new_log_block_size,
                logs,
            );
            let planned = planner.plan_recovery(MIN_SNAPSHOT_TS, read_depth).unwrap();
            assert_eq!(planned.skipped_max_recovered_cts, None);
            let mut stream = planned.stream;
            let recovered = stream.try_next().await.unwrap().unwrap();
            assert_eq!(recovered.header.cts, cts);
            assert_eq!(recovered.header.trx_kind, RedoTrxKind::System);
            assert!(stream.try_next().await.unwrap().is_none());

            assert_eq!(finalizer.next_file_seq, 1);
            assert_eq!(finalizer.log_write_io_depth, 7);
            assert_eq!(finalizer.log_block_size, new_log_block_size);
            assert_eq!(finalizer.file_max_size, new_file_max_size);

            let (purge_tx, _purge_rx) = flume::unbounded();
            let (redo_log, initial_header_completion) = finalizer.finalize(purge_tx).unwrap();
            let header_write = {
                let mut group_commit_g = redo_log.group_commit.lock();
                match group_commit_g.queue.pop_front().unwrap() {
                    Commit::LogFileBoundary {
                        ended_log_file,
                        header_write,
                    } => {
                        assert!(ended_log_file.is_none());
                        header_write
                    }
                    Commit::Group(_) | Commit::Shutdown => {
                        panic!("expected initial redo super-block write")
                    }
                }
            };
            submit_header_write_for_test(header_write, &initial_header_completion);
            drop(redo_log);

            let new_bytes = fs::read(format!("{file_prefix}.00000001")).unwrap();
            let new_slot0 =
                parse_redo_super_block(&new_bytes[..REDO_SUPER_BLOCK_SLOT_SIZE], 1, 0).unwrap();
            assert_eq!(new_slot0.log_block_size, new_log_block_size as u64);
            assert_eq!(new_slot0.file_max_size, new_file_max_size as u64);
        });
    }

    #[test]
    fn test_engine_startup_rejects_legacy_partitioned_redo_file() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            File::create(temp_dir.path().join("legacy_redo.0.00000000")).unwrap();

            let err = match EngineConfig::default()
                .storage_root(temp_dir.path())
                .trx(TrxSysConfig::default().log_file_stem("legacy_redo"))
                .build()
                .await
            {
                Ok(_) => panic!("engine startup should reject legacy partitioned redo files"),
                Err(err) => err,
            };
            let report = format!("{err:?}");
            assert!(report.contains("unsupported legacy partitioned redo log file"));
        });
    }

    #[test]
    fn test_engine_startup_rejects_legacy_zero_header_redo_file() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let legacy = File::create(temp_dir.path().join("legacy_redo.00000000")).unwrap();
            legacy.set_len(128 * 1024).unwrap();
            drop(legacy);

            let err = match EngineConfig::default()
                .storage_root(temp_dir.path())
                .trx(TrxSysConfig::default().log_file_stem("legacy_redo"))
                .build()
                .await
            {
                Ok(_) => panic!("engine startup should reject legacy zero-header redo files"),
                Err(err) => err,
            };
            assert_eq!(
                err.data_integrity_error(),
                Some(DataIntegrityError::InvalidMagic)
            );
        });
    }

    #[test]
    fn test_direct_redo_log_stream_reader() {
        smol::block_on(async {
            const SIZE: i32 = 100;

            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = EngineConfig::default()
                .storage_root(main_dir)
                .trx(
                    TrxSysConfig::default()
                        .log_file_stem(String::from("direct_redo_stream_reader.log")),
                )
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .role(PoolRole::Mem)
                        .max_mem_size(64u64 * 1024 * 1024)
                        .max_file_size(128u64 * 1024 * 1024),
                )
                .build()
                .await
                .unwrap();
            let table_id = table2(&engine).await;

            let mut session = engine.new_session().unwrap();
            {
                for i in 0..SIZE {
                    let mut trx = session.begin_trx().unwrap();
                    trx.exec(async |stmt| {
                        let s = format!("{}", i);
                        let insert = vec![Val::from(i), Val::from(&s[..])];
                        stmt.table_insert_mvcc(table_id, insert).await?;
                        Ok(())
                    })
                    .await
                    .unwrap();
                    trx.commit().await.unwrap();
                }
            }
            drop(session);

            let mut log_recs = 0usize;
            let file_prefix = engine.inner().trx_sys.config.file_prefix().unwrap();
            let logs = discover_redo_log_files(&file_prefix, 0, false).unwrap();
            let planner = RedoReplayPlanner::new(logs);
            let mut stream = planner.plan_recovery(TrxID::new(0), 1).unwrap().stream;
            while let Some(pod) = stream.try_next().await.unwrap() {
                println!(
                    "log {}, header={:?}, payload={:?}",
                    log_recs, pod.header, pod.payload
                );
                log_recs += 1;
            }
            println!("total log records {}", log_recs);
            assert!(
                log_recs > 0,
                "direct redo stream should produce at least one record"
            );

            drop(engine);
        });
    }

    #[test]
    fn test_redo_write_failure_poison_runtime_and_fail_waiters() {
        smol::block_on(async {
            let (_temp_dir, engine) =
                build_redo_test_engine("redo_write_failure", LogSync::None).await;
            let redo_fd = {
                engine
                    .inner()
                    .trx_sys
                    .redo_log
                    .group_commit
                    .lock()
                    .log_file
                    .as_ref()
                    .unwrap()
                    .as_raw_fd()
            };
            let hook = ControlledRedoWriteHook::new(redo_fd, libc::EIO);
            let _install = install_storage_backend_test_hook(Arc::new(hook.clone()));

            let commit1 = spawn_sys_commit_wait(engine.new_ref().unwrap(), 1);
            hook.wait_started(1).await;

            let commit2 = spawn_sys_commit_wait(engine.new_ref().unwrap(), 2);
            wait_for(|| {
                !engine
                    .inner()
                    .trx_sys
                    .redo_log
                    .group_commit
                    .lock()
                    .queue
                    .is_empty()
            })
            .await;

            hook.release();

            let res1 = commit1.join().unwrap();
            let res2 = commit2.join().unwrap();
            assert_propagated_completion_fatal(&res1, FatalError::RedoWrite);
            assert_propagated_completion_fatal(&res2, FatalError::RedoWrite);
            assert!(
                engine
                    .inner()
                    .trx_sys
                    .ensure_runtime_healthy()
                    .as_ref()
                    .is_err_and(|err| *err.current_context() == FatalError::RedoWrite)
            );
        });
    }

    async fn assert_redo_sync_failure_poison_runtime_and_fail_waiters(
        log_sync: LogSync,
        sync_kind: IOKind,
        log_file_stem: &str,
    ) {
        let (_temp_dir, engine) = build_redo_test_engine(log_file_stem, log_sync).await;
        let redo_fd = {
            engine
                .inner()
                .trx_sys
                .redo_log
                .group_commit
                .lock()
                .log_file
                .as_ref()
                .unwrap()
                .as_raw_fd()
        };
        let hook = ControlledRedoSyncHook::new(redo_fd, sync_kind, libc::EIO);
        let _install = install_storage_backend_test_hook(Arc::new(hook.clone()));

        let commit1 = spawn_sys_commit_wait(engine.new_ref().unwrap(), 10);
        hook.wait_started(1).await;

        let commit2 = spawn_sys_commit_wait(engine.new_ref().unwrap(), 11);
        wait_for(|| {
            !engine
                .inner()
                .trx_sys
                .redo_log
                .group_commit
                .lock()
                .queue
                .is_empty()
        })
        .await;

        hook.release();

        let res1 = commit1.join().unwrap();
        let res2 = commit2.join().unwrap();
        assert_propagated_completion_fatal(&res1, FatalError::RedoSync);
        assert_propagated_completion_fatal(&res2, FatalError::RedoSync);
        assert!(
            engine
                .inner()
                .trx_sys
                .ensure_runtime_healthy()
                .as_ref()
                .is_err_and(|err| *err.current_context() == FatalError::RedoSync)
        );
    }

    #[test]
    fn test_redo_fsync_failure_poison_runtime_and_fail_waiters() {
        smol::block_on(async {
            assert_redo_sync_failure_poison_runtime_and_fail_waiters(
                LogSync::Fsync,
                IOKind::Fsync,
                "redo_fsync_failure",
            )
            .await;
        });
    }

    #[test]
    fn test_redo_fdatasync_failure_poison_runtime_and_fail_waiters() {
        smol::block_on(async {
            assert_redo_sync_failure_poison_runtime_and_fail_waiters(
                LogSync::Fdatasync,
                IOKind::Fdatasync,
                "redo_fdatasync_failure",
            )
            .await;
        });
    }

    #[test]
    fn test_commit_sys_returns_cts() {
        smol::block_on(async {
            let (_temp_dir, engine) = build_redo_test_engine("redo_no_wait", LogSync::None).await;
            let mut sys_trx = engine.inner().trx_sys.begin_sys_trx();
            sys_trx.create_row_page(
                TableID::new(1),
                test_page_id(1),
                RowID::new(0),
                RowID::new(1),
            );
            let cts = engine.inner().trx_sys.commit_sys(sys_trx).unwrap();
            assert!(cts >= MIN_SNAPSHOT_TS);
        });
    }
}
