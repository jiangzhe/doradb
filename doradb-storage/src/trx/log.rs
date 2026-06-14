use crate::conf::TrxSysConfig;
use crate::error::{ConfigError, DataIntegrityError, Error, FatalError, ResourceError, Result};
use crate::file::{FileSyncer, SparseFile, UNTRACKED_FILE_ID};
use crate::free_list::FreeList;
use crate::id::TrxID;
use crate::io::{
    Completion, DirectBuf, IOBackendStats, IOBackendStatsHandle, IOSubmission, Operation,
    StorageBackend, SubmissionDriver,
};
use crate::map::FastHashMap;
use crate::serde::Ser;
use crate::trx::MIN_SNAPSHOT_TS;
use crate::trx::group::{
    Commit, CommitGroup, CommitGroupLog, CommitJoin, CommitWaiter, GroupCommit, MutexGroupCommit,
};
use crate::trx::log_replay::{LogBuf, MmapLogReader, RedoLogStream, TrxLog};
use crate::trx::purge::{GCBucket, Purge};
use crate::trx::sys::GC_BUCKETS;
use crate::trx::sys::TransactionSystem;
use crate::trx::{
    CommittedTrx, FailedPrecommitCleanupJob, FailedPrecommitReason, MAX_COMMIT_TS, MAX_SNAPSHOT_TS,
    PrecommitTrx, PreparedTrx,
};
use crossbeam_utils::CachePadded;
use error_stack::Report;
use flume::Sender;
use glob::{Pattern, glob};
use parking_lot::{Mutex, MutexGuard};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, VecDeque};
use std::mem;
use std::os::fd::{AsRawFd, RawFd};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

pub(crate) const LOG_HEADER_PAGES: usize = 2;
enum EnqueuedCommit {
    Queued {
        cts: TrxID,
        waiter: Option<CommitWaiter>,
    },
    Rejected {
        cts: TrxID,
        trx: Box<PrecommitTrx>,
        reason: FailedPrecommitReason,
    },
}

pub(crate) struct RedoLogInitializer {
    pub(crate) ctx: StorageBackend,
    pub(crate) mode: RedoLogMode,
    pub(crate) file_prefix: String,
    pub(crate) file_max_size: usize,
    pub(crate) file_header_size: usize,
    pub(crate) page_size: usize,
    pub(crate) io_depth_per_log: usize,
    // sequence of last log file.
    pub(crate) file_seq: Option<u32>,
}

impl RedoLogInitializer {
    #[inline]
    pub(crate) fn recovery(
        file_prefix: String,
        io_depth_per_log: usize,
        file_max_size: usize,
        max_io_size: usize,
        logs: Vec<PathBuf>,
    ) -> Result<Self> {
        Ok(Self {
            ctx: StorageBackend::new(io_depth_per_log)?,
            mode: RedoLogMode::Recovery(VecDeque::from(logs)),
            file_prefix,
            file_max_size,
            file_header_size: max_io_size * LOG_HEADER_PAGES,
            page_size: max_io_size,
            io_depth_per_log,
            file_seq: None,
        })
    }

    #[inline]
    pub(crate) fn stream(self) -> RedoLogStream {
        RedoLogStream {
            initializer: self,
            reader: None,
            buffer: VecDeque::new(),
        }
    }

    #[inline]
    pub(crate) fn next_reader(&mut self) -> Result<Option<MmapLogReader>> {
        match &mut self.mode {
            RedoLogMode::Done => Ok(None),
            RedoLogMode::Recovery(logs) => {
                let log = logs.pop_front().unwrap();
                self.file_seq.replace(parse_file_seq(log.as_path())?);
                let reader = MmapLogReader::new(
                    &log,
                    self.page_size,
                    self.file_max_size,
                    self.file_header_size,
                )?;
                if logs.is_empty() {
                    // add file seq so we always open a new log file.
                    *self.file_seq.as_mut().unwrap() += 1;
                    self.mode = RedoLogMode::Done;
                }
                Ok(Some(reader))
            }
        }
    }

    #[inline]
    pub(super) fn finish(self, purge_tx: Sender<Purge>) -> Result<RedoLog> {
        let mut file_seq = self.file_seq.unwrap_or(0);
        let log_file = create_log_file(
            &self.file_prefix,
            file_seq,
            self.file_max_size,
            self.file_header_size,
        )?;
        file_seq += 1;

        let group_commit = GroupCommit {
            queue: VecDeque::new(),
            closed: None,
            log_file: Some(log_file),
        };
        let gc_info: Vec<_> = (0..GC_BUCKETS).map(|_| GCBucket::new()).collect();
        let io_backend_stats = self.ctx.stats_handle();
        Ok(RedoLog {
            group_commit: CachePadded::new(MutexGroupCommit::new(group_commit)),
            persisted_cts: CachePadded::new(AtomicU64::new(MIN_SNAPSHOT_TS.as_u64())),
            rr_gc_no: CachePadded::new(AtomicUsize::new(0)),
            stats: Arc::new(CachePadded::new(RedoLogStats::default())),
            purge_tx,
            gc_buckets: gc_info.into_boxed_slice(),
            log_write_backend: CachePadded::new(Mutex::new(Some(self.ctx))),
            io_backend_stats,
            max_io_size: self.page_size,
            file_prefix: self.file_prefix,
            file_seq: AtomicU32::new(file_seq),
            file_max_size: self.file_max_size,
            buf_free_list: FreeList::new(
                self.io_depth_per_log,
                self.io_depth_per_log * 2,
                move || DirectBuf::zeroed(self.page_size),
            ),
        })
    }
}

pub(crate) enum RedoLogMode {
    /// Previous log should be analyzed and replayed
    /// for data recovery.
    Recovery(VecDeque<PathBuf>),
    /// Recovery is done or there is no existing log files.
    Done,
}

pub(super) struct LogWriteSubmission {
    cts: TrxID,
    operation: Operation,
}

impl LogWriteSubmission {
    #[inline]
    pub(super) fn new(cts: TrxID, fd: std::os::fd::RawFd, offset: usize, buf: DirectBuf) -> Self {
        LogWriteSubmission {
            cts,
            operation: Operation::pwrite_owned(fd, offset, buf),
        }
    }
}

impl IOSubmission for LogWriteSubmission {
    #[inline]
    fn operation(&mut self) -> &mut Operation {
        &mut self.operation
    }
}

struct LogWriteCompletion {
    cts: TrxID,
    buf: DirectBuf,
    poison: Option<FatalError>,
}

struct LogWriteDriver {
    driver: SubmissionDriver<LogWriteSubmission>,
}

impl LogWriteDriver {
    #[inline]
    fn new(backend: StorageBackend) -> Self {
        LogWriteDriver {
            driver: SubmissionDriver::new(backend),
        }
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
    fn push_write(
        &mut self,
        submission: LogWriteSubmission,
    ) -> std::result::Result<(), LogWriteSubmission> {
        self.driver.push(submission)
    }

    #[inline]
    fn submit_ready(&mut self) -> usize {
        self.driver.submit_ready()
    }

    #[inline]
    fn wait_one(&mut self) -> LogWriteCompletion {
        let completed = self.driver.wait_one();
        let mut submission = completed.submission;
        let expected_len = submission.operation.len();
        let buf = submission
            .operation
            .take_buf()
            .expect("redo write submission must still own its direct buffer");
        let result = match completed.result {
            Ok(len) if len == expected_len => None,
            Ok(_len) => Some(FatalError::RedoWrite),
            Err(_err) => Some(FatalError::RedoWrite),
        };
        LogWriteCompletion {
            cts: submission.cts,
            buf,
            poison: result,
        }
    }
}

pub(crate) struct RedoLog {
    /// Group commit state for the redo log.
    pub(super) group_commit: CachePadded<MutexGroupCommit>,
    /// Maximum ordered-completed CTS of this redo log.
    ///
    /// Durability-required groups advance this after redo write/sync completes.
    /// No-log groups also advance it after ordered completion, but recovery
    /// still seeds timestamps only from checkpoint metadata, table roots, and
    /// redo headers.
    pub(super) persisted_cts: CachePadded<AtomicU64>,
    /// Round-robin GC number generator.
    rr_gc_no: CachePadded<AtomicUsize>,
    /// Stats of transaction system.
    pub(super) stats: Arc<CachePadded<RedoLogStats>>,
    /// Purge coordinator channel used for committed transaction GC handoff.
    pub(super) purge_tx: Sender<Purge>,
    /// Each GC bucket contains active sts list, committed transaction list and
    /// old transaction list.
    /// Split into multiple buckets in order to avoid bottleneck of global synchronization.
    pub(super) gc_buckets: Box<[GCBucket]>,
    /// Backend for redo writes, taken exactly once by the log thread.
    log_write_backend: CachePadded<Mutex<Option<StorageBackend>>>,
    /// Backend-owned submit/wait statistics for redo writes.
    io_backend_stats: IOBackendStatsHandle,
    /// Maximum IO size of each group.
    pub(super) max_io_size: usize,
    /// Log file prefix for the single redo file family.
    pub(super) file_prefix: String,
    /// Sequence of current file in this redo log, starting from 0.
    pub(super) file_seq: AtomicU32,
    /// Maximum size of single log file.
    pub(super) file_max_size: usize,
    /// Free list of page buffer, which is used by commit group to concat
    /// redo logs.
    pub(super) buf_free_list: FreeList<DirectBuf>,
}

impl RedoLog {
    /// Returns next GC number.
    /// It is used to evenly dispatch transactions to all GC buckets.
    pub(crate) fn next_gc_no(&self) -> usize {
        self.rr_gc_no.fetch_add(1, Ordering::Relaxed) % GC_BUCKETS
    }

    /// Create a new log buffer to hold one transaction's redo log.
    #[inline]
    fn new_buf(&self, data: TrxLog) -> LogBuf {
        let ser_len = data.ser_len();
        let buf_len = LogBuf::actual_len(ser_len);
        if ser_len > self.max_io_size {
            // data is longer than single page, we need to
            let mut buf = LogBuf::new(buf_len);
            buf.ser(&data);
            return buf;
        }
        if let Some(mut buf) = self.try_buf_in_free_list(ser_len) {
            buf.ser(&data);
            return buf;
        }
        let mut buf = LogBuf::new(buf_len);
        buf.ser(&data);
        buf
    }

    #[inline]
    fn try_buf_in_free_list(&self, ser_len: usize) -> Option<LogBuf> {
        if LogBuf::actual_len(ser_len) <= self.max_io_size {
            // fit buffer size in free list.
            return self.buf_free_list.try_pop(true).map(LogBuf::with_buffer);
        }
        None
    }

    #[inline]
    pub(super) fn min_active_sts(&self) -> TrxID {
        let mut min = MAX_SNAPSHOT_TS;
        for gc_bucket in &self.gc_buckets {
            let ts = TrxID::new(gc_bucket.min_active_sts.load(Ordering::Relaxed));
            if ts < min {
                min = ts;
            }
        }
        min
    }

    #[inline]
    fn rotate_log_file(&self, group_commit_g: &mut MutexGuard<'_, GroupCommit>) -> Result<()> {
        let new_log_file = self.create_log_file()?;
        let old_log_file = group_commit_g.log_file.replace(new_log_file).unwrap();
        group_commit_g.queue.push_back(Commit::Switch(old_log_file));
        Ok(())
    }

    #[inline]
    fn create_log_file(&self) -> Result<SparseFile> {
        let file_seq = self.file_seq.fetch_add(1, Ordering::SeqCst);
        create_log_file(
            &self.file_prefix,
            file_seq,
            self.file_max_size,
            // we use two pages as file header to maintain metadata of log file.
            self.max_io_size * LOG_HEADER_PAGES,
        )
    }

    #[inline]
    fn create_new_group(
        &self,
        mut trx: PrecommitTrx,
        group_commit_g: &mut MutexGuard<'_, GroupCommit>,
        wait_sync: bool,
    ) -> CommitJoin {
        let cts = trx.cts;
        let log = if let Some(redo_bin) = trx.take_log() {
            // Serialize redo log to buffer.
            let log_buf = self.new_buf(redo_bin);
            let log_file = group_commit_g.log_file.as_ref().unwrap();
            // Allocate space of log file.
            let (fd, offset) = match log_file.alloc(log_buf.capacity()) {
                Ok((offset, _)) => (log_file.as_raw_fd(), offset),
                Err(err)
                    if err.resource_error() == Some(ResourceError::StorageFileCapacityExceeded) =>
                {
                    // Rotate log file and try again.
                    self.rotate_log_file(group_commit_g)
                        .expect("rotate log file");

                    let new_log_file = group_commit_g.log_file.as_ref().unwrap();
                    let (offset, _) = new_log_file
                        .alloc(log_buf.capacity())
                        .expect("alloc on new log file");
                    (new_log_file.as_raw_fd(), offset)
                }
                Err(_) => unreachable!(),
            };
            Some(CommitGroupLog {
                fd,
                offset,
                log_buf,
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
        waiter
    }

    #[inline]
    fn enqueue_commit(
        &self,
        trx: PreparedTrx,
        global_ts: &AtomicU64,
        wait_sync: bool,
    ) -> EnqueuedCommit {
        let mut group_commit_g = self.group_commit.lock();
        let cts = TrxID::new(global_ts.fetch_add(1, Ordering::SeqCst));
        debug_assert!(cts < MAX_COMMIT_TS);
        let precommit_trx = trx.fill_cts(cts);
        if let Some(reason) = group_commit_g.closed {
            drop(group_commit_g);
            return EnqueuedCommit::Rejected {
                cts,
                trx: Box::new(precommit_trx),
                reason,
            };
        }
        if group_commit_g.queue.is_empty() {
            let waiter = self.create_new_group(precommit_trx, &mut group_commit_g, wait_sync);
            self.group_commit.notify_one(); // notify sync thread to work.
            drop(group_commit_g);
            return EnqueuedCommit::Queued { cts, waiter };
        }
        let last_group = match group_commit_g.queue.back_mut().unwrap() {
            Commit::Shutdown => unreachable!("shutdown queue tail requires closed admission"),
            Commit::Group(group) => group,
            Commit::Switch(_) => {
                // Impossible, switch always has one group followed.
                unreachable!()
            }
        };
        if last_group.can_join(&precommit_trx) {
            let waiter = last_group.join(precommit_trx, wait_sync);
            drop(group_commit_g); // unlock to let other transactions to enter commit phase.
            return EnqueuedCommit::Queued { cts, waiter };
        }
        let waiter = self.create_new_group(precommit_trx, &mut group_commit_g, wait_sync);
        self.group_commit.notify_one(); // notify sync thread to work.
        drop(group_commit_g);
        EnqueuedCommit::Queued { cts, waiter }
    }

    #[inline]
    pub(super) fn commit_no_wait(&self, trx: PreparedTrx, global_ts: &AtomicU64) -> Result<TrxID> {
        debug_assert!(trx.attachment.is_none());
        // This API is for sessionless system transactions only.
        //
        // System transactions are used to durably piggyback internal state
        // changes that become globally visible once the redo group commits. They
        // do not participate in user-session rollback semantics, so this path
        // intentionally returns immediately without a waiter or a detached
        // transaction attachment.
        //
        // Do not route a session-bound user transaction here. The user path must
        // wait for durability so the caller can commit/rollback the session
        // explicitly.
        match self.enqueue_commit(trx, global_ts, false) {
            EnqueuedCommit::Queued { cts, waiter } => {
                debug_assert!(waiter.is_none());
                Ok(cts)
            }
            EnqueuedCommit::Rejected { cts, trx, reason } => {
                (*trx).discard_rejected();
                Err(reason.into_error(format!("redo group commit is closed: commit_ts={cts}")))
            }
        }
    }

    #[inline]
    pub(super) async fn commit(
        &self,
        trx: PreparedTrx,
        trx_sys: &TransactionSystem,
        global_ts: &AtomicU64,
    ) -> Result<TrxID> {
        let (cts, waiter) = match self.enqueue_commit(trx, global_ts, true) {
            EnqueuedCommit::Queued { cts, waiter } => (cts, waiter),
            EnqueuedCommit::Rejected { cts, trx, reason } => {
                let completion = Arc::new(Completion::new());
                let waiter = Arc::clone(&completion);
                trx_sys.request_failed_precommit_cleanup(FailedPrecommitCleanupJob::new(
                    vec![*trx],
                    completion,
                    reason,
                ));
                (cts, Some(waiter))
            }
        };
        let Some(waiter) = waiter else {
            unreachable!("async commit must enqueue a completion waiter");
        };
        waiter.wait_result().await.map_err(|report| {
            Error::from_completion_report(
                report,
                format!("wait for redo group commit: commit_ts={cts}"),
            )
        })?;
        assert!(TrxID::new(self.persisted_cts.load(Ordering::Relaxed)) >= cts);
        Ok(cts)
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

    #[inline]
    fn take_log_write_driver(&self) -> LogWriteDriver {
        let backend = self
            .log_write_backend
            .lock()
            .take()
            .expect("redo log write backend must exist before startup");
        LogWriteDriver::new(backend)
    }

    #[inline]
    pub(crate) fn io_backend_stats(&self) -> IOBackendStats {
        self.io_backend_stats.snapshot()
    }

    #[inline]
    fn file_processor<'a>(
        &'a self,
        trx_sys: &'a TransactionSystem,
        config: &TrxSysConfig,
        write_driver: &'a mut LogWriteDriver,
    ) -> FileProcessor<'a> {
        FileProcessor::new(self, trx_sys, config, write_driver)
    }

    #[inline]
    pub(super) fn log_loop(&self, trx_sys: &TransactionSystem, config: &TrxSysConfig) {
        let mut write_driver = self.take_log_write_driver();
        loop {
            let mut fp = self.file_processor(trx_sys, config, &mut write_driver);
            if let Some(ended_log_file) = fp.process_single_file() {
                // Finished groups carry borrowed raw fds for sync. Keep the
                // switched-out file alive until every old-file group is
                // finalized.
                fp.finish_pending_io();

                debug_assert!(fp.inflight.is_empty());
                debug_assert!(fp.sync_groups.is_empty());
                debug_assert_eq!(fp.write_driver.pending_len(), 0);

                // todo: update file header.
                // this may include information which can speed up recovery.
                // e.g. DDL start/end pages, min/max transaction CTS.

                // Close file explicitly.
                drop(ended_log_file);
            }
            if fp.shutdown {
                return;
            }
        }
    }
}

pub(super) struct SyncGroup {
    pub(super) trx_list: Vec<PrecommitTrx>,
    pub(super) max_cts: TrxID,
    pub(super) log_bytes: usize,
    // Redo-bearing groups keep the borrowed log fd for the final fsync. No-log
    // groups have no file allocation and therefore no sync target.
    pub(super) log_fd: Option<RawFd>,
    pub(super) write: Option<LogWriteSubmission>,
    pub(super) returned_buf: Option<DirectBuf>,
    pub(super) completion: Arc<Completion<()>>,
    pub(super) finished: bool,
    // Redo logging is sequential: once one group fails, this group and every
    // later group cannot be part of the durable prefix. Submitted IO may still
    // complete later, but completion only returns buffers for recycling.
    pub(super) failure_reason: Option<FailedPrecommitReason>,
}

impl SyncGroup {
    #[inline]
    fn take_submission(&mut self) -> Option<LogWriteSubmission> {
        self.write.take()
    }

    #[inline]
    fn finish_write(&mut self, buf: DirectBuf) {
        self.returned_buf = Some(buf);
        self.finished = true;
    }

    #[inline]
    fn take_any_buf(&mut self) -> Option<DirectBuf> {
        if let Some(buf) = self.returned_buf.take() {
            return Some(buf);
        }
        self.write
            .as_mut()
            .and_then(|submission| submission.operation.take_buf())
    }

    #[inline]
    fn fail_waiters(&mut self, trx_sys: &TransactionSystem, reason: FailedPrecommitReason) {
        if self.failure_reason.is_some() {
            return;
        }
        self.failure_reason = Some(reason);
        let trx_list = mem::take(&mut self.trx_list);
        let completion = Arc::clone(&self.completion);
        trx_sys.request_failed_precommit_cleanup(FailedPrecommitCleanupJob::new(
            trx_list, completion, reason,
        ));
    }
}

#[derive(Default)]
pub(crate) struct RedoLogStats {
    pub(crate) commit_count: AtomicUsize,
    pub(crate) trx_count: AtomicUsize,
    pub(crate) log_bytes: AtomicUsize,
    pub(crate) sync_count: AtomicUsize,
    pub(crate) sync_nanos: AtomicUsize,
    pub(crate) purge_trx_count: AtomicUsize,
    pub(crate) purge_row_count: AtomicUsize,
    pub(crate) purge_index_count: AtomicUsize,
}

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
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
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

struct FileProcessor<'a> {
    redo_log: &'a RedoLog,
    trx_sys: &'a TransactionSystem,
    write_driver: &'a mut LogWriteDriver,
    max_io_size: usize,
    inflight: BTreeMap<TrxID, SyncGroup>,
    sync_groups: VecDeque<SyncGroup>,
    written: Vec<SyncGroup>,
    failed_written: Vec<SyncGroup>,
    log_sync: LogSync,
    shutdown: bool,
}

impl<'a> FileProcessor<'a> {
    #[inline]
    fn new(
        redo_log: &'a RedoLog,
        trx_sys: &'a TransactionSystem,
        config: &TrxSysConfig,
        write_driver: &'a mut LogWriteDriver,
    ) -> Self {
        FileProcessor {
            redo_log,
            trx_sys,
            write_driver,
            max_io_size: config.max_io_size.as_u64() as usize,
            inflight: BTreeMap::new(),
            sync_groups: VecDeque::new(),
            written: vec![],
            failed_written: vec![],
            log_sync: config.log_sync,
            shutdown: false,
        }
    }

    #[inline]
    fn recycle_buf(&self, mut buf: DirectBuf) {
        if buf.capacity() == self.max_io_size {
            buf.reset();
            self.redo_log.buf_free_list.push(buf);
        }
    }

    #[inline]
    fn fail_sync_group(&self, sync_group: &mut SyncGroup, reason: FailedPrecommitReason) {
        sync_group.fail_waiters(self.trx_sys, reason);
        if let Some(buf) = sync_group.take_any_buf() {
            self.recycle_buf(buf);
        }
    }

    #[inline]
    fn fail_pending(&mut self, err: Report<FatalError>) {
        self.shutdown = true;
        let reason = FailedPrecommitReason::Fatal(*err.current_context());
        let drained_sync_groups: Vec<_> = self.sync_groups.drain(..).collect();
        for mut sync_group in drained_sync_groups {
            self.fail_sync_group(&mut sync_group, reason);
        }

        let mut queued = Vec::new();
        {
            let mut group_commit_g = self.redo_log.group_commit.lock();
            group_commit_g.close(reason);
            while let Some(commit) = group_commit_g.queue.pop_front() {
                match commit {
                    Commit::Group(group) => queued.push(group),
                    Commit::Switch(log_file) => drop(log_file),
                    Commit::Shutdown => {}
                }
            }
        }
        for group in queued {
            let mut sync_group = group.into_sync_group();
            self.fail_sync_group(&mut sync_group, reason);
        }

        for sync_group in self.inflight.values_mut() {
            sync_group.fail_waiters(self.trx_sys, reason);
        }
    }

    /// Process single log file until it is full or shutdown.
    #[inline]
    fn process_single_file(&mut self) -> Option<SparseFile> {
        loop {
            debug_assert!(
                self.sync_groups.len() + self.inflight.len() >= self.write_driver.pending_len(),
                "queued and inflight groups should cover all driver-owned work"
            );
            // If shutdown flag is set, we still submit and finish all pending IOs,
            // but do not accept any new IO requests.
            if !self.shutdown {
                let ended_log_file = self.fetch_io_reqs();
                // End this loop if log file is full.
                if ended_log_file.is_some() {
                    return ended_log_file;
                }
            }
            self.submit_io();
            // Always try to publish the ordered prefix after submit. Some groups
            // are already finished when they enter inflight, and a log rotation
            // can leave finished inflight groups behind before the usual wait
            // path gets another chance to run.
            self.finalize_finished_prefix();
            self.wait_one_io_if_submitted();
            // The wait only marks a write finished; this publishes the durable
            // ordered prefix made ready by that completion.
            self.finalize_finished_prefix();

            if self.shutdown
                && self.sync_groups.is_empty()
                && self.inflight.is_empty()
                && self.write_driver.pending_len() == 0
            {
                return None;
            }
        }
    }

    /// Finish pending IOs.
    #[inline]
    fn finish_pending_io(&mut self) {
        while !self.sync_groups.is_empty()
            || !self.inflight.is_empty()
            || self.write_driver.pending_len() > 0
        {
            self.submit_io();
            // Rotation can return from `process_single_file` before its
            // post-wait finalization step. Drain any finished prefix even when
            // there is no backend-submitted write to wait for.
            self.finalize_finished_prefix();
            self.wait_one_io_if_submitted();
            self.finalize_finished_prefix();
        }
    }

    /// Fetch IO requests, returns ended log file if any.
    #[inline]
    fn fetch_io_reqs(&mut self) -> Option<SparseFile> {
        if self.write_driver.pending_len() == 0 {
            // there is no processing IO, so we can block on waiting for next request.
            self.fetch_io_reqs_internal()
        } else {
            // only try non-blocking way to fetch incoming requests, because we also
            // need to finish previous IO.
            self.try_fetch_io_reqs_internal()
        }
    }

    #[inline]
    fn fetch_io_reqs_internal(&mut self) -> Option<SparseFile> {
        loop {
            let mut group_commit_g = self.redo_log.group_commit.lock();
            loop {
                match group_commit_g.queue.pop_front() {
                    None => break,
                    Some(Commit::Shutdown) => {
                        self.shutdown = true;
                        return None;
                    }
                    Some(Commit::Switch(log_file)) => return Some(log_file),
                    Some(Commit::Group(cg)) => {
                        self.sync_groups.push_back(cg.into_sync_group());
                    }
                }
            }
            if !self.sync_groups.is_empty() {
                return None;
            }
            self.redo_log
                .group_commit
                .wait_for(&mut group_commit_g, Duration::from_secs(1));
        }
    }

    #[inline]
    fn try_fetch_io_reqs_internal(&mut self) -> Option<SparseFile> {
        // Single thread perform IO so here we only need to use sync mutex.
        let mut group_commit_g = self.redo_log.group_commit.lock();
        loop {
            match group_commit_g.queue.pop_front() {
                None => {
                    return None;
                }
                Some(Commit::Shutdown) => {
                    self.shutdown = true;
                    return None;
                }
                Some(Commit::Switch(log_file)) => return Some(log_file),
                Some(Commit::Group(cg)) => {
                    self.sync_groups.push_back(cg.into_sync_group());
                }
            }
        }
    }

    #[inline]
    fn written_log_fd(&self) -> RawFd {
        let mut log_fd = None;
        for sync_group in &self.written {
            if sync_group.log_bytes == 0 {
                continue;
            }
            let sync_group_fd = sync_group
                .log_fd
                .expect("redo-bearing sync group must carry its log file fd");
            if let Some(fd) = log_fd {
                debug_assert_eq!(
                    fd, sync_group_fd,
                    "finished redo prefix must not cross log file boundaries"
                );
            } else {
                log_fd = Some(sync_group_fd);
            }
        }
        log_fd.expect("redo-bearing finished prefix must include a log file fd")
    }

    #[inline]
    fn sync_written_prefix(&self, log_bytes: usize) -> Result<()> {
        if log_bytes == 0 {
            return Ok(());
        }
        let log_fd = self.written_log_fd();
        let syncer = FileSyncer::from_borrowed_fd(log_fd);
        match self.log_sync {
            LogSync::Fsync => syncer.fsync(),
            LogSync::Fdatasync => syncer.fdatasync(),
            LogSync::None => Ok(()),
        }
    }

    /// Finalizes the ordered prefix whose redo writes are already finished.
    ///
    /// This is a no-op when the first inflight group is not finished. Otherwise
    /// it removes the finished CTS prefix, performs the configured file sync
    /// when that prefix contains redo bytes, advances persisted CTS, commits
    /// transactions, wakes waiters, recycles write buffers, and updates stats.
    ///
    /// Redo durability is sequential: this stops at the first unfinished group,
    /// and a failed group makes every later group cleanup-only even if its
    /// individual write has completed.
    #[inline]
    fn finalize_finished_prefix(&mut self) {
        self.written.clear();
        self.failed_written.clear();
        let (trx_count, commit_count, log_bytes, failure_reason) = shrink_inflight(
            &mut self.inflight,
            &mut self.written,
            &mut self.failed_written,
        );

        if let Some(reason) = failure_reason {
            // The first failed group ends the sequential redo prefix. Any
            // later inflight group that has not finished yet must be failed
            // now; it stays in `inflight` only until its IO completion returns
            // the backend-owned buffer.
            for sync_group in self.inflight.values_mut() {
                sync_group.fail_waiters(self.trx_sys, reason);
            }
            let drained_failed: Vec<_> = self.failed_written.drain(..).collect();
            for mut sync_group in drained_failed {
                self.fail_sync_group(&mut sync_group, reason);
            }
        }

        if !self.written.is_empty() {
            let max_cts = self.written.last().unwrap().max_cts;

            let start = Instant::now();
            let sync_res = self.sync_written_prefix(log_bytes);
            let sync_dur = start.elapsed();
            if sync_res.is_err() {
                let err = self.trx_sys.poison_storage(FatalError::RedoSync);
                let reason = FailedPrecommitReason::Fatal(*err.current_context());
                let drained_written: Vec<_> = self.written.drain(..).collect();
                for mut sync_group in drained_written {
                    self.fail_sync_group(&mut sync_group, reason);
                }
                self.fail_pending(err);
                return;
            }

            self.redo_log
                .persisted_cts
                .store(max_cts.as_u64(), Ordering::SeqCst);

            // Put IO buffer back into free list.
            let drained_written: Vec<_> = self.written.drain(..).collect();
            for mut sync_group in drained_written {
                debug_assert!(sync_group.failure_reason.is_none());
                if let Some(buf) = sync_group.take_any_buf() {
                    self.recycle_buf(buf);
                }
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
                    self.redo_log
                        .purge_tx
                        .send(Purge::Committed(committed_trx_list))
                        .expect(
                            "purge coordinator receiver must stay alive until log thread stopped",
                        );
                }
                sync_group.completion.complete(Ok(()));
            }

            self.redo_log.update_stats(
                trx_count,
                commit_count,
                log_bytes,
                usize::from(log_bytes > 0),
                if log_bytes > 0 {
                    sync_dur.as_nanos() as usize
                } else {
                    0
                },
            );
        }
    }

    /// Submit queued redo writes.
    ///
    /// Groups without redo bytes are already finished when they enter the
    /// scheduler. This only admits them into `inflight`; callers must still run
    /// `finalize_finished_prefix` after submit so those groups can unblock the
    /// ordered prefix without a backend completion.
    #[inline]
    fn submit_io(&mut self) {
        while !self.sync_groups.is_empty() {
            let mut sync_group = self
                .sync_groups
                .pop_front()
                .expect("redo sync group queue length was checked");
            let Some(submission) = sync_group.take_submission() else {
                debug_assert!(sync_group.finished);
                let res = self.inflight.insert(sync_group.max_cts, sync_group);
                debug_assert!(res.is_none());
                continue;
            };
            if self.write_driver.available_capacity() == 0 {
                sync_group.write = Some(submission);
                self.sync_groups.push_front(sync_group);
                break;
            }
            if let Err(submission) = self.write_driver.push_write(submission) {
                sync_group.write = Some(submission);
                self.sync_groups.push_front(sync_group);
                break;
            }
            let res = self.inflight.insert(sync_group.max_cts, sync_group);
            debug_assert!(res.is_none());
        }
        self.write_driver.submit_ready();
    }

    /// Wait for one submitted redo write, if any, and mark its sync group finished.
    #[inline]
    fn wait_one_io_if_submitted(&mut self) {
        if self.write_driver.submitted_len() == 0 {
            return;
        }
        let completion = self.write_driver.wait_one();
        if let Some(source) = completion.poison {
            let mut failed_group = self
                .inflight
                .remove(&completion.cts)
                .expect("redo completion must match one inflight sync group");
            failed_group.finish_write(completion.buf);
            let err = self.trx_sys.poison_storage(source);
            let reason = FailedPrecommitReason::Fatal(*err.current_context());
            self.fail_sync_group(&mut failed_group, reason);
            self.fail_pending(err);
            return;
        }
        let sync_group = self
            .inflight
            .get_mut(&completion.cts)
            .expect("redo completion must match one inflight sync group");
        sync_group.finish_write(completion.buf);
    }
}

#[inline]
fn shrink_inflight(
    tree: &mut BTreeMap<TrxID, SyncGroup>,
    buffer: &mut Vec<SyncGroup>,
    failed_buffer: &mut Vec<SyncGroup>,
) -> (usize, usize, usize, Option<FailedPrecommitReason>) {
    let mut trx_count = 0;
    let mut commit_count = 0;
    let mut log_bytes = 0;
    let mut failure_reason = None;
    while let Some(entry) = tree.first_entry() {
        let task = entry.get();
        if !task.finished {
            break; // stop at the transaction which is not persisted.
        }
        if let Some(reason) = failure_reason.or(task.failure_reason) {
            // Redo records are sequential. The first failed group ends the
            // durable prefix, so every finished group after it is cleanup-only
            // even if its individual write completed successfully.
            failure_reason = Some(reason);
            failed_buffer.push(entry.remove());
            continue;
        }
        trx_count += task.trx_list.len();
        commit_count += 1;
        log_bytes += task.log_bytes;
        buffer.push(entry.remove());
    }
    (trx_count, commit_count, log_bytes, failure_reason)
}

#[inline]
fn log_file_name(file_prefix: &str, file_seq: u32) -> String {
    format!("{file_prefix}.{file_seq:08x}")
}

/// Create a new log file.
#[inline]
pub(super) fn create_log_file(
    file_prefix: &str,
    file_seq: u32,
    file_max_size: usize,
    file_header_size: usize,
) -> Result<SparseFile> {
    let file_name = log_file_name(file_prefix, file_seq);
    let log_file = SparseFile::create_or_fail(&file_name, file_max_size, UNTRACKED_FILE_ID)?;
    // todo: Add two pages as file header.
    let _ = log_file.alloc(file_header_size)?;
    Ok(log_file)
}

#[inline]
pub(crate) fn discover_redo_log_files(file_prefix: &str, desc: bool) -> Result<Vec<PathBuf>> {
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
    validate_redo_log_file_sequences(file_prefix, &files)?;
    let mut res: Vec<_> = files.into_iter().map(|(_, path)| path).collect();
    if desc {
        res.reverse();
    }
    Ok(res)
}

#[inline]
fn validate_redo_log_file_sequences(file_prefix: &str, files: &[(u32, PathBuf)]) -> Result<()> {
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
    let suffix =
        std::str::from_utf8(&file_name.as_bytes()[file_name.len() - 8..]).map_err(|_| {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::{PoolRole, test_page_id};
    use crate::catalog::tests::table2;
    use crate::conf::{EngineConfig, EvictableBufferPoolConfig, TrxSysConfig};
    use crate::engine::{Engine, EngineRef};
    use crate::error::{CompletionErrorKind, DataIntegrityError, FatalError, LifecycleError};
    use crate::file::{FileSyncKind, FileSyncOp, FileSyncTestHook, set_file_sync_test_hook};
    use crate::id::{PageID, RowID, TableID};
    use crate::io::{
        IOKind, StdIoResult, StorageBackendOp, StorageBackendTestHook,
        install_storage_backend_test_hook,
    };
    use crate::trx::log_replay::ReadLog;
    use crate::value::Val;
    use futures::task::noop_waker;
    use std::fs::{self, File};
    use std::future::Future;
    use std::os::fd::AsRawFd;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, LazyLock, Mutex, MutexGuard, mpsc};
    use std::task::{Context, Poll};
    use std::thread::JoinHandle;
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
            smol::Timer::after(TEST_WAIT_INTERVAL).await;
        }
        panic!("condition was not satisfied before timeout");
    }

    static FILE_SYNC_TEST_HOOK_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

    struct InstalledFileSyncTestHook {
        previous: Option<Arc<dyn FileSyncTestHook>>,
        guard: Option<MutexGuard<'static, ()>>,
    }

    impl Drop for InstalledFileSyncTestHook {
        #[inline]
        fn drop(&mut self) {
            let _ = set_file_sync_test_hook(self.previous.take());
            drop(self.guard.take());
        }
    }

    fn install_file_sync_test_hook(hook: Arc<dyn FileSyncTestHook>) -> InstalledFileSyncTestHook {
        let guard = FILE_SYNC_TEST_HOOK_LOCK.lock().unwrap();
        InstalledFileSyncTestHook {
            previous: set_file_sync_test_hook(Some(hook)),
            guard: Some(guard),
        }
    }

    #[derive(Clone)]
    struct ControlledRedoWriteHook {
        inner: Arc<ControlledRedoWriteHookInner>,
    }

    struct ControlledRedoWriteHookInner {
        fd: std::os::fd::RawFd,
        errno: Option<i32>,
        calls: AtomicUsize,
        started: event_listener::Event,
        released: AtomicBool,
        release: event_listener::Event,
    }

    impl ControlledRedoWriteHook {
        fn new(fd: std::os::fd::RawFd, errno: i32) -> Self {
            Self::with_error(fd, Some(errno))
        }

        fn success(fd: std::os::fd::RawFd) -> Self {
            Self::with_error(fd, None)
        }

        fn with_error(fd: std::os::fd::RawFd, errno: Option<i32>) -> Self {
            Self {
                inner: Arc::new(ControlledRedoWriteHookInner {
                    fd,
                    errno,
                    calls: AtomicUsize::new(0),
                    started: event_listener::Event::new(),
                    released: AtomicBool::new(false),
                    release: event_listener::Event::new(),
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
                *res = Err(std::io::Error::from_raw_os_error(errno));
            }
        }
    }

    #[derive(Clone)]
    struct ControlledFileSyncHook {
        inner: Arc<ControlledFileSyncHookInner>,
    }

    struct ControlledFileSyncHookInner {
        fd: std::os::fd::RawFd,
        kind: FileSyncKind,
        errno: i32,
        calls: AtomicUsize,
        started: event_listener::Event,
        released: AtomicBool,
        release: event_listener::Event,
    }

    impl ControlledFileSyncHook {
        fn new(fd: std::os::fd::RawFd, kind: FileSyncKind, errno: i32) -> Self {
            Self {
                inner: Arc::new(ControlledFileSyncHookInner {
                    fd,
                    kind,
                    errno,
                    calls: AtomicUsize::new(0),
                    started: event_listener::Event::new(),
                    released: AtomicBool::new(false),
                    release: event_listener::Event::new(),
                }),
            }
        }

        fn matches(&self, op: FileSyncOp) -> bool {
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

    impl FileSyncTestHook for ControlledFileSyncHook {
        fn on_sync(&self, op: FileSyncOp, override_res: &mut Option<Result<()>>) {
            if !self.matches(op) {
                return;
            }
            self.inner.calls.fetch_add(1, Ordering::SeqCst);
            self.inner.started.notify(usize::MAX);
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
            *override_res = Some(Err(
                std::io::Error::from_raw_os_error(self.inner.errno).into()
            ));
        }
    }

    #[derive(Clone)]
    struct RecordingFileSyncHook {
        calls: Arc<Mutex<Vec<FileSyncOp>>>,
    }

    impl RecordingFileSyncHook {
        fn new() -> Self {
            Self {
                calls: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn calls(&self) -> Vec<FileSyncOp> {
            self.calls.lock().unwrap().clone()
        }
    }

    impl FileSyncTestHook for RecordingFileSyncHook {
        fn on_sync(&self, op: FileSyncOp, override_res: &mut Option<Result<()>>) {
            self.calls.lock().unwrap().push(op);
            *override_res = Some(Ok(()));
        }
    }

    fn spawn_sys_commit_wait(engine: EngineRef, marker: u64) -> JoinHandle<Result<TrxID>> {
        std::thread::spawn(move || {
            smol::block_on(async move {
                let mut sys_trx = engine.trx_sys.begin_sys_trx();
                sys_trx.create_row_page(
                    TableID::from(marker),
                    PageID::from(marker),
                    RowID::new(0),
                    RowID::new(1),
                );
                let prepared = sys_trx.prepare();
                engine
                    .trx_sys
                    .redo_log
                    .commit(prepared, &engine.trx_sys, &engine.trx_sys.ts)
                    .await
            })
        })
    }

    fn assert_propagated_completion_fatal<T: std::fmt::Debug>(
        res: &Result<T>,
        expected: FatalError,
    ) {
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

    async fn build_redo_test_engine(log_file_stem: &str, log_sync: LogSync) -> (TempDir, Engine) {
        let temp_dir = TempDir::new().unwrap();
        let engine = EngineConfig::default()
            .storage_root(temp_dir.path().to_path_buf())
            .trx(
                TrxSysConfig::default()
                    .log_file_stem(log_file_stem)
                    .io_depth_per_log(1)
                    .log_sync(log_sync),
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
            wait_for(|| engine.inner().trx_sys.redo_log.min_active_sts() == MAX_SNAPSHOT_TS).await;
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
            assert_eq!(engine.inner().trx_sys.redo_log.min_active_sts(), sts);
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

            std::thread::scope(|scope| {
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
                engine.inner().trx_sys.redo_log.min_active_sts() > sts,
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
            let hook = ControlledFileSyncHook::new(redo_fd, FileSyncKind::Fsync, libc::EIO);
            let _install = install_file_sync_test_hook(Arc::new(hook.clone()));

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

    fn sync_group_for_order_test(cts: TrxID, finished: bool, log_bytes: usize) -> SyncGroup {
        sync_group_for_order_test_with_log_fd(cts, finished, log_bytes, None)
    }

    fn sync_group_for_order_test_with_log_fd(
        cts: TrxID,
        finished: bool,
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
            write: None,
            returned_buf: None,
            completion: Arc::new(Completion::new()),
            finished,
            failure_reason: None,
        }
    }

    #[test]
    fn test_shrink_inflight_preserves_order_with_no_log_groups() {
        let mut inflight = BTreeMap::new();
        let mut written = Vec::new();
        let mut failed_written = Vec::new();

        inflight.insert(
            TrxID::new(10),
            sync_group_for_order_test(TrxID::new(10), false, 4096),
        );
        inflight.insert(
            TrxID::new(11),
            sync_group_for_order_test(TrxID::new(11), true, 0),
        );
        assert_eq!(
            shrink_inflight(&mut inflight, &mut written, &mut failed_written),
            (0, 0, 0, None)
        );
        assert!(written.is_empty());
        assert!(failed_written.is_empty());
        assert_eq!(inflight.len(), 2);

        inflight.get_mut(&TrxID::new(10)).unwrap().finished = true;
        assert_eq!(
            shrink_inflight(&mut inflight, &mut written, &mut failed_written),
            (2, 2, 4096, None)
        );
        assert_eq!(written.len(), 2);
        assert_eq!(written[0].max_cts, TrxID::new(10));
        assert_eq!(written[1].max_cts, TrxID::new(11));
        assert!(failed_written.is_empty());
    }

    #[test]
    fn test_shrink_inflight_releases_no_log_prefix_without_later_log() {
        let mut inflight = BTreeMap::new();
        let mut written = Vec::new();
        let mut failed_written = Vec::new();

        inflight.insert(
            TrxID::new(20),
            sync_group_for_order_test(TrxID::new(20), true, 0),
        );
        inflight.insert(
            TrxID::new(21),
            sync_group_for_order_test(TrxID::new(21), false, 4096),
        );
        assert_eq!(
            shrink_inflight(&mut inflight, &mut written, &mut failed_written),
            (1, 1, 0, None)
        );
        assert_eq!(written.len(), 1);
        assert_eq!(written[0].max_cts, TrxID::new(20));
        assert!(failed_written.is_empty());
        assert!(inflight.contains_key(&TrxID::new(21)));
    }

    #[test]
    fn test_finish_pending_io_finalizes_finished_prefix_without_submitted_write() {
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
            let (purge_tx, _purge_rx) = flume::unbounded();
            let redo_log = RedoLogInitializer {
                ctx: StorageBackend::new(1).unwrap(),
                mode: RedoLogMode::Done,
                file_prefix,
                file_max_size: 128 * 1024,
                file_header_size: 4096 * LOG_HEADER_PAGES,
                page_size: 4096,
                io_depth_per_log: 1,
                file_seq: None,
            }
            .finish(purge_tx)
            .unwrap();
            let config = TrxSysConfig::default()
                .max_io_size(4096usize)
                .log_sync(LogSync::None);
            let mut write_driver = LogWriteDriver::new(StorageBackend::new(1).unwrap());
            let cts = TrxID::new(50);

            {
                let mut fp = FileProcessor::new(
                    &redo_log,
                    &engine.inner().trx_sys,
                    &config,
                    &mut write_driver,
                );
                fp.inflight
                    .insert(cts, sync_group_for_order_test(cts, true, 0));

                fp.finish_pending_io();

                assert!(fp.inflight.is_empty());
                assert!(fp.sync_groups.is_empty());
                assert_eq!(fp.write_driver.pending_len(), 0);
                assert_eq!(fp.write_driver.submitted_len(), 0);
            }

            assert_eq!(redo_log.persisted_cts.load(Ordering::SeqCst), cts.as_u64());
            engine.shutdown().unwrap();
        });
    }

    #[test]
    fn test_finish_pending_io_syncs_switch_ended_log_file() {
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
            let (purge_tx, _purge_rx) = flume::unbounded();
            let redo_log = RedoLogInitializer {
                ctx: StorageBackend::new(1).unwrap(),
                mode: RedoLogMode::Done,
                file_prefix,
                file_max_size: 128 * 1024,
                file_header_size: 4096 * LOG_HEADER_PAGES,
                page_size: 4096,
                io_depth_per_log: 1,
                file_seq: None,
            }
            .finish(purge_tx)
            .unwrap();

            let (ended_fd, current_fd) = {
                let mut group_commit_g = redo_log.group_commit.lock();
                let ended_fd = group_commit_g.log_file.as_ref().unwrap().as_raw_fd();
                redo_log.rotate_log_file(&mut group_commit_g).unwrap();
                let current_fd = group_commit_g.log_file.as_ref().unwrap().as_raw_fd();
                (ended_fd, current_fd)
            };
            assert_ne!(ended_fd, current_fd);

            let hook = RecordingFileSyncHook::new();
            let _install = install_file_sync_test_hook(Arc::new(hook.clone()));
            let config = TrxSysConfig::default()
                .max_io_size(4096usize)
                .log_sync(LogSync::Fsync);
            let mut write_driver = LogWriteDriver::new(StorageBackend::new(1).unwrap());
            let cts = TrxID::new(60);

            let ended_log_file = {
                let mut fp =
                    redo_log.file_processor(&engine.inner().trx_sys, &config, &mut write_driver);
                fp.inflight.insert(
                    cts,
                    sync_group_for_order_test_with_log_fd(cts, true, 4096, Some(ended_fd)),
                );

                let ended_log_file = fp
                    .process_single_file()
                    .expect("queued rotation should yield ended log file");
                assert_eq!(ended_log_file.as_raw_fd(), ended_fd);

                fp.finish_pending_io();

                assert!(fp.inflight.is_empty());
                assert!(fp.sync_groups.is_empty());
                assert_eq!(fp.write_driver.pending_len(), 0);
                assert_eq!(fp.write_driver.submitted_len(), 0);
                ended_log_file
            };

            assert_eq!(redo_log.persisted_cts.load(Ordering::SeqCst), cts.as_u64());
            let calls = hook.calls();
            assert_eq!(calls.len(), 1);
            assert_eq!(calls[0].kind(), FileSyncKind::Fsync);
            assert_eq!(calls[0].fd(), ended_fd);
            assert_ne!(calls[0].fd(), current_fd);

            drop(ended_log_file);
            engine.shutdown().unwrap();
        });
    }

    #[test]
    fn test_shrink_inflight_stops_at_failed_redo_boundary() {
        let mut inflight = BTreeMap::new();
        let mut written = Vec::new();
        let mut failed_written = Vec::new();
        let reason = FailedPrecommitReason::Fatal(FatalError::RedoWrite);

        inflight.insert(
            TrxID::new(30),
            sync_group_for_order_test(TrxID::new(30), true, 1024),
        );
        inflight.insert(
            TrxID::new(31),
            sync_group_for_order_test(TrxID::new(31), true, 2048),
        );
        inflight.insert(
            TrxID::new(32),
            sync_group_for_order_test(TrxID::new(32), true, 4096),
        );
        inflight.get_mut(&TrxID::new(31)).unwrap().failure_reason = Some(reason);

        assert_eq!(
            shrink_inflight(&mut inflight, &mut written, &mut failed_written),
            (1, 1, 1024, Some(reason))
        );
        assert!(inflight.is_empty());
        assert_eq!(written.len(), 1);
        assert_eq!(written[0].max_cts, TrxID::new(30));
        assert_eq!(failed_written.len(), 2);
        assert_eq!(failed_written[0].max_cts, TrxID::new(31));
        assert_eq!(failed_written[1].max_cts, TrxID::new(32));
    }

    #[test]
    fn test_shrink_inflight_keeps_unfinished_groups_after_failed_boundary() {
        let mut inflight = BTreeMap::new();
        let mut written = Vec::new();
        let mut failed_written = Vec::new();
        let reason = FailedPrecommitReason::Fatal(FatalError::RedoWrite);

        inflight.insert(
            TrxID::new(40),
            sync_group_for_order_test(TrxID::new(40), true, 1024),
        );
        inflight.insert(
            TrxID::new(41),
            sync_group_for_order_test(TrxID::new(41), false, 2048),
        );
        inflight.get_mut(&TrxID::new(40)).unwrap().failure_reason = Some(reason);

        assert_eq!(
            shrink_inflight(&mut inflight, &mut written, &mut failed_written),
            (0, 0, 0, Some(reason))
        );
        assert!(written.is_empty());
        assert_eq!(failed_written.len(), 1);
        assert_eq!(failed_written[0].max_cts, TrxID::new(40));
        assert!(inflight.contains_key(&TrxID::new(41)));
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
        for path in &expected {
            File::create(path).unwrap();
        }
        File::create(log_dir.join("redo.logx.00000000")).unwrap();

        let asc = discover_redo_log_files(file_prefix, false).unwrap();
        assert_eq!(expected.to_vec(), asc);

        let desc = discover_redo_log_files(file_prefix, true).unwrap();
        assert_eq!(expected.iter().rev().cloned().collect::<Vec<_>>(), desc);
    }

    #[test]
    fn test_discover_redo_log_files_rejects_sequence_gap() {
        let temp_dir = TempDir::new().unwrap();
        let file_prefix = temp_dir.path().join("redo.log");
        let file_prefix = file_prefix.to_str().unwrap();
        File::create(format!("{file_prefix}.00000000")).unwrap();
        File::create(format!("{file_prefix}.00000002")).unwrap();

        let err = discover_redo_log_files(file_prefix, false).unwrap_err();
        assert_eq!(
            err.data_integrity_error(),
            Some(DataIntegrityError::RedoLogSequenceGap)
        );
        let report = format!("{err:?}");
        assert!(report.contains("00000001"), "{report}");
        assert!(report.contains(file_prefix), "{report}");
    }

    #[test]
    fn test_discover_redo_log_files_rejects_legacy_partitioned_files() {
        let temp_dir = TempDir::new().unwrap();
        let file_prefix = temp_dir.path().join("redo.log");
        let file_prefix = file_prefix.to_str().unwrap();
        File::create(format!("{file_prefix}.0.00000000")).unwrap();

        let err = discover_redo_log_files(file_prefix, false).unwrap_err();
        let report = format!("{err:?}");
        assert!(report.contains("unsupported legacy partitioned redo log file"));
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
    fn test_mmap_log_reader() {
        smol::block_on(async {
            const SIZE: i32 = 100;

            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = EngineConfig::default()
                .storage_root(main_dir)
                .trx(
                    TrxSysConfig::default().log_file_stem(String::from("mmap_log_reader_redo.log")),
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
            let logs = discover_redo_log_files(&file_prefix, false).unwrap();
            for log in logs {
                println!("log file {:?}", log.file_name());
                let mut reader = engine.inner().trx_sys.log_reader(&log).unwrap();
                loop {
                    match reader.read() {
                        ReadLog::SizeLimit => unreachable!(),
                        ReadLog::DataCorrupted => unreachable!(),
                        ReadLog::DataEnd => break,
                        ReadLog::Some(mut group) => {
                            let data_len = group.data().len();
                            while let Some(pod) = group.try_next().unwrap() {
                                println!(
                                    "log {}, len={}, header={:?}, payload={:?}",
                                    log_recs, data_len, pod.header, pod.payload
                                );
                                log_recs += 1;
                            }
                        }
                    }
                }
            }
            println!("total log records {}", log_recs);

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
        sync_kind: FileSyncKind,
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
        let hook = ControlledFileSyncHook::new(redo_fd, sync_kind, libc::EIO);
        let _install = install_file_sync_test_hook(Arc::new(hook.clone()));

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
                FileSyncKind::Fsync,
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
                FileSyncKind::Fdatasync,
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
