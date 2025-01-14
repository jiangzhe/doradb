use crate::error::{Error, Result};
use crate::io::{
    align_to_sector_size, pwrite, AIOError, AIOManager, AIOManagerConfig, Buf, DirectBuf,
    FreeListWithFactory, IocbRawPtr, PageBuf, SparseFile, AIO,
};
use crate::session::{InternalSession, IntoSession, Session};
use crate::trx::{
    ActiveTrx, CommittedTrx, PrecommitTrx, PreparedTrx, TrxID, MAX_COMMIT_TS, MAX_SNAPSHOT_TS,
    MIN_ACTIVE_TRX_ID, MIN_SNAPSHOT_TS,
};
use crossbeam_utils::CachePadded;
use flume::{Receiver, Sender};
use parking_lot::{Condvar, Mutex, MutexGuard};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::mem;
use std::os::fd::{AsRawFd, RawFd};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

/// TransactionSystem controls lifecycle of all transactions.
///
/// 1. Transaction begin:
/// a) Generate STS and TrxID.
/// b) Put it into active transaction list.
///
/// 2. Transaction pre-commmit:
/// a) Generate CTS.
/// b) Put it into precommit transaction list.
///
/// Note 1: Before pre-commit, the transaction should serialize its redo log to binary
/// because group commit is single-threaded and the serialization may require
/// much CPU and slow down the log writer. So each transaction
///
/// Note 2: In this phase, transaction is still in active transaction list.
/// Only when redo log is persisted, we can move it from active list to committed list.
/// One optimization is Early Lock Release, which unlock all row-locks(backfill CTS to undo)
/// and move it to committed list. This can improve performance because it does not wait
/// log writer to fsync. But final-commit step must wait for additional transaction dependencies,
/// to ensure any previous dependent transaction's log are already persisted.
/// Currently, we do NOT apply this optimization.  
///
/// 3. Transaction group-commit:
///
/// A single-threaded log writer is responsible for persisting redo logs.
/// It also notify all transactions in group commit to check if log has been persisted.
///
/// 4. Transaction final-commit:
///
/// TrxID in all undo log entries of current transaction should be updated to CTS after log
/// is persisted.
/// As undo logs are maintained purely in memory, we can use shared pointer with atomic variable
/// to perform very fast CTS backfill.
pub struct TransactionSystem {
    /// A sequence to generate snapshot timestamp(abbr. sts) and commit timestamp(abbr. cts).
    /// They share the same sequence and start from 1.
    /// The two timestamps are used to identify which version of data a transaction should see.
    /// Transaction id is derived from snapshot timestamp with highest bit set to 1.
    ///
    /// trx_id range: (1<<63)+1 to uint::MAX-1
    /// sts range: 1 to 1<<63
    /// cts range: 1 to 1<<63
    ts: CachePadded<AtomicU64>,
    /// Minimum active snapshot timestamp.
    /// It's updated by group committer thread, and is used by query/GC thread to clean
    /// out-of-date version chains.
    ///
    /// Note: this field may not reflect the latest value, but is enough for GC purpose.
    min_active_sts: CachePadded<AtomicU64>,
    /// Round-robin partition id generator.
    partition_id: CachePadded<AtomicUsize>,
    /// Multiple log partitions.
    log_partitions: CachePadded<Box<[CachePadded<LogPartition>]>>,
    /// Persisted commit timestamp is the maximum commit timestamp of all persisted redo
    /// logs. Precommit transactions are already notified by group committer. This global
    /// atomic variable is used by query or GC thread to perform GC.
    persisted_cts: CachePadded<AtomicU64>,
    /// Committed transaction list.
    /// When a transaction is committed, it will be put into this queue in sequence.
    /// Head is always oldest and tail is newest.
    gc_info: CachePadded<Mutex<GCInfo>>,
    /// Transaction system configuration.
    config: CachePadded<TrxSysConfig>,
}

impl TransactionSystem {
    /// Drop static transaction system.
    ///
    /// # Safety
    ///
    /// Caller must ensure no further use on it.
    pub unsafe fn drop_static(this: &'static Self) {
        // notify and wait for group commit to quit.
        // this.stop_group_committer_and_wait();
        drop(Box::from_raw(this as *const Self as *mut Self));
    }

    /// Create a new transaction.
    #[inline]
    pub fn new_trx<'a>(&self, session: Option<Box<InternalSession>>) -> ActiveTrx {
        // active transaction list is calculated by group committer thread
        // so here we just generate STS and TrxID.
        let sts = self.ts.fetch_add(1, Ordering::SeqCst);
        let trx_id = sts | (1 << 63);
        debug_assert!(sts < MAX_SNAPSHOT_TS);
        debug_assert!(trx_id >= MIN_ACTIVE_TRX_ID);
        // assign log partition index so current transaction will stick
        // to certain log partititon for commit.
        let log_partition_idx = if self.config.log_partitions == 1 {
            0
        } else {
            self.partition_id.fetch_add(1, Ordering::Relaxed) % self.config.log_partitions
        };
        let mut trx = ActiveTrx::new(session, trx_id, sts);
        trx.set_log_partition(log_partition_idx);
        trx
    }

    /// Commit an active transaction.
    /// The commit process is implemented as group commit.
    /// If multiple transactions are being committed at the same time, one of them
    /// will become leader of the commit group. Others become followers waiting for
    /// leader to persist log and backfill CTS.
    /// This strategy can largely reduce logging IO, therefore improve throughput.
    #[inline]
    pub async fn commit(&self, trx: ActiveTrx) -> Result<Session> {
        // Prepare redo log first, this may take some time,
        // so keep it out of lock scope, and we can fill cts after the lock is held.
        let partition = &*self.log_partitions[trx.log_partition_idx];
        let prepared_trx = trx.prepare();
        if prepared_trx.redo_bin.is_none() {
            if prepared_trx.undo.is_empty() {
                // This is a read-only transaction, drop it is safe.
                debug_assert!(prepared_trx.readonly());
                return Ok(prepared_trx.into_session());
            }
            // There might be scenario that the transaction does not change anything
            // logically, but have undo logs.
            // For example, a transaction
            // 1) insert one row.
            // 2) delete it.
            // 3) commit.
            //
            // The preparation should shrink redo logs on row level and finally there
            // is no redo entry. But we have undo logs and already put them into
            // page-level undo maps.
            // In such case, we pass this transaction to group commit, and it will
            // directly succeeds if it's group leader.
            // Otherwise, it always joins last group successfully.
        }
        // start group commit
        partition.commit(prepared_trx, &self.ts).await
    }

    /// Rollback active transaction.
    #[inline]
    pub fn rollback(&self, trx: ActiveTrx) {
        // let sts = trx.sts;
        // let log_partition_idx = trx.log_partition_idx;
        trx.rollback();
        // self.log_partitions[log_partition_idx].end_active_sts(sts);
    }

    /// Returns statistics of group commit.
    #[inline]
    pub fn trx_sys_stats(&self) -> TrxSysStats {
        let mut stats = TrxSysStats::default();
        for partition in &*self.log_partitions {
            stats.trx_count += partition.stats.trx_count.load(Ordering::Relaxed);
            stats.commit_count += partition.stats.commit_count.load(Ordering::Relaxed);
            stats.log_bytes += partition.stats.log_bytes.load(Ordering::Relaxed);
            stats.sync_count += partition.stats.sync_count.load(Ordering::Relaxed);
            stats.sync_nanos += partition.stats.sync_nanos.load(Ordering::Relaxed);
            stats.io_submit_count += partition.stats.io_submit_count.load(Ordering::Relaxed);
            stats.io_submit_nanos += partition.stats.io_submit_nanos.load(Ordering::Relaxed);
            stats.io_wait_count += partition.stats.io_wait_count.load(Ordering::Relaxed);
            stats.io_wait_nanos += partition.stats.io_wait_nanos.load(Ordering::Relaxed);
        }
        stats
    }

    /// Start background sync thread.
    /// This method should be called once transaction system is initialized,
    /// and should after start_gc_thread().
    #[inline]
    fn start_sync_threads(&'static self) {
        // Start threads for all log partitions
        for (idx, partition) in self.log_partitions.iter().enumerate() {
            let thread_name = format!("Sync-Thread-{}", idx);
            let partition = &**partition;

            let builder = thread::Builder::new().name(thread_name);
            let handle = if self.config.log_drop {
                builder.spawn(move || partition.io_loop_noop(&self.config))
            } else {
                builder.spawn(move || partition.io_loop(&self.config))
            }
            .unwrap();
            *partition.sync_thread.lock() = Some(handle);
        }
    }
}

unsafe impl Sync for TransactionSystem {}

impl Drop for TransactionSystem {
    #[inline]
    fn drop(&mut self) {
        let log_partitions = &*self.log_partitions;
        // notify sync thread to quit for each log partition.
        for partition in log_partitions {
            let mut group_commit_g = partition.group_commit.0.lock();
            group_commit_g.queue.push_back(CommitMessage::Shutdown);
            if group_commit_g.queue.len() == 1 {
                partition.group_commit.1.notify_one(); // notify sync thread to quit.
            }
        }
        for partition in log_partitions {
            let sync_thread = { partition.sync_thread.lock().take().unwrap() };
            sync_thread.join().unwrap();
        }
        // finally close log files
        for partition in log_partitions {
            let mut group_commit_g = partition.group_commit.0.lock();
            let log_file = group_commit_g.log_file.take().unwrap();
            partition.aio_mgr.drop_sparse_file(log_file);
        }
    }
}

pub const DEFAULT_LOG_IO_DEPTH: usize = 32;
pub const DEFAULT_LOG_IO_MAX_SIZE: usize = 8192;
pub const DEFAULT_LOG_FILE_PREFIX: &'static str = "redo.log";
pub const DEFAULT_LOG_PARTITIONS: usize = 1;
pub const MAX_LOG_PARTITIONS: usize = 99; // big enough for log partitions, so fix two digits in file name.
pub const DEFAULT_LOG_FILE_MAX_SIZE: usize = 1024 * 1024 * 1024; // 1GB, sparse file will not occupy space until actual write.
pub const DEFAULT_LOG_SYNC: LogSync = LogSync::Fsync;
pub const DEFAULT_LOG_DROP: bool = false;
pub const DEFAULT_GC: bool = false;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogSync {
    None,
    Fsync,
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
            Err(Error::InvalidArgument)
        }
    }
}

pub struct TrxSysConfig {
    // Controls infight IO request number of each log file.
    io_depth_per_log: usize,
    // Controls maximum IO size of each IO request.
    // This only limit the combination of multiple transactions.
    // If single transaction has very large redo log. It is kept
    // what it is and send to AIO manager as one IO request.
    max_io_size: usize,
    // Prefix of log file.
    // the complete file name pattern is:
    // <file-prefix>.<partition_idx>.<file-sequence>
    // e.g. redo.log.0.00000001
    log_file_prefix: String,
    // Log partition number.
    log_partitions: usize,
    // Controls the maximum size of each log file.
    // Log file will be rotated once the size limit is reached.
    // a u32 suffix is appended at end of the file name in
    // hexdigit format.
    log_file_max_size: usize,
    // Controls which method to sync data on disk.
    // By default, use fsync(),
    // Can be switched to fdatasync() or not sync.
    log_sync: LogSync,
    // Drop log directly. If this parameter is set to true,
    // log_sync parameter will be discarded.
    log_drop: bool,
    gc: bool,
}

impl TrxSysConfig {
    /// How many commits can be issued concurrently.
    #[inline]
    pub fn io_depth_per_log(mut self, io_depth_per_log: usize) -> Self {
        self.io_depth_per_log = io_depth_per_log;
        self
    }

    /// How large single IO operation can be.
    #[inline]
    pub fn max_io_size(mut self, max_io_size: usize) -> Self {
        self.max_io_size = max_io_size;
        self
    }

    /// Whether to enable GC.
    #[inline]
    pub fn gc(mut self, gc: bool) -> Self {
        self.gc = gc;
        self
    }

    /// Log file name.
    #[inline]
    pub fn log_file_prefix(mut self, log_file_prefix: String) -> Self {
        self.log_file_prefix = log_file_prefix;
        self
    }

    /// Maximum size of single log file.
    #[inline]
    pub fn log_file_max_size(mut self, log_file_max_size: usize) -> Self {
        self.log_file_max_size = align_to_sector_size(log_file_max_size);
        self
    }

    /// Controls how many files can be used for concurrent logging.
    #[inline]
    pub fn log_partitions(mut self, log_partitions: usize) -> Self {
        assert!(log_partitions > 0 && log_partitions <= MAX_LOG_PARTITIONS);
        self.log_partitions = log_partitions;
        self
    }

    /// Sync method of log files.
    #[inline]
    pub fn log_sync(mut self, log_sync: LogSync) -> Self {
        self.log_sync = log_sync;
        self
    }

    #[inline]
    pub fn log_drop(mut self, log_drop: bool) -> Self {
        self.log_drop = log_drop;
        self
    }

    fn setup_log_partition(&self, idx: usize) -> LogPartition {
        let aio_mgr = AIOManagerConfig::default()
            .max_events(self.io_depth_per_log)
            .build()
            .expect("create AIO manager");

        let file_seq = 0;
        let file_name = log_file_name(&self.log_file_prefix, idx, file_seq);
        let log_file = aio_mgr
            .create_sparse_file(&file_name, self.log_file_max_size)
            .expect("create log file");

        let group_commit = GroupCommit {
            queue: VecDeque::new(),
            log_file: Some(log_file),
            file_seq,
        };
        let max_io_size = self.max_io_size;
        LogPartition {
            group_commit: CachePadded::new((Mutex::new(group_commit), Condvar::new())),
            persisted_cts: CachePadded::new(AtomicU64::new(MIN_SNAPSHOT_TS)),
            aio_mgr,
            idx,
            max_io_size,
            file_prefix: log_file_prefix(&self.log_file_prefix, idx),
            buf_free_list: FreeListWithFactory::prefill(self.io_depth_per_log, move || {
                PageBuf::uninit(max_io_size)
            }),
            sync_thread: Mutex::new(None),
            stats: CachePadded::new(LogPartitionStats::default()),
        }
    }

    /// Build transaction system with logging and GC, leak it to heap
    /// for the convenience to share the singleton among multiple threads.
    #[inline]
    pub fn build_static(self) -> &'static TransactionSystem {
        let mut log_partitions = Vec::with_capacity(self.log_partitions);
        for idx in 0..self.log_partitions {
            let partition = self.setup_log_partition(idx);
            log_partitions.push(CachePadded::new(partition));
        }
        let trx_sys = TransactionSystem {
            ts: CachePadded::new(AtomicU64::new(MIN_SNAPSHOT_TS)),
            min_active_sts: CachePadded::new(AtomicU64::new(MIN_SNAPSHOT_TS)),
            partition_id: CachePadded::new(AtomicUsize::new(0)),
            log_partitions: CachePadded::new(log_partitions.into_boxed_slice()),
            persisted_cts: CachePadded::new(AtomicU64::new(MIN_SNAPSHOT_TS)),
            gc_info: CachePadded::new(Mutex::new(GCInfo::new())),
            config: CachePadded::new(self),
        };
        let trx_sys = Box::leak(Box::new(trx_sys));
        trx_sys.start_sync_threads();
        trx_sys
    }
}

impl Default for TrxSysConfig {
    #[inline]
    fn default() -> Self {
        TrxSysConfig {
            io_depth_per_log: DEFAULT_LOG_IO_DEPTH,
            max_io_size: DEFAULT_LOG_IO_MAX_SIZE,
            log_file_prefix: String::from(DEFAULT_LOG_FILE_PREFIX),
            log_file_max_size: DEFAULT_LOG_FILE_MAX_SIZE,
            log_partitions: DEFAULT_LOG_PARTITIONS,
            log_sync: DEFAULT_LOG_SYNC,
            log_drop: DEFAULT_LOG_DROP,
            gc: DEFAULT_GC,
        }
    }
}

struct LogPartition {
    // Group commit of this partition.
    group_commit: CachePadded<(Mutex<GroupCommit>, Condvar)>,
    // Maximum persisted CTS of this partition.
    persisted_cts: CachePadded<AtomicU64>,
    /// AIO manager to handle async IO with libaio.
    aio_mgr: AIOManager,
    // Index of log partition in total partitions, starts from 0.
    idx: usize,
    // Maximum IO size of each group.
    max_io_size: usize,
    // Log file prefix, including partition number.
    file_prefix: String,
    // Free list of page buffer, which is used by commit group to concat
    // redo logs.
    buf_free_list: FreeListWithFactory<PageBuf>,
    // Standalone thread to handle transaction commit.
    // Including submit IO requests, wait IO responses
    // and do fsync().
    sync_thread: Mutex<Option<JoinHandle<()>>>,
    /// Stats of transaction system.
    stats: CachePadded<LogPartitionStats>,
}

impl LogPartition {
    #[inline]
    fn buf(&self, data: &[u8]) -> Buf {
        if data.len() > self.max_io_size {
            let buf = DirectBuf::uninit(data.len());
            Buf::Direct(buf)
        } else {
            let mut buf = self.buf_free_list.pop_elem_or_new();
            buf.set_len(data.len());
            buf.clone_from_slice(0, data);
            Buf::Reuse(buf)
        }
    }

    #[inline]
    fn create_new_group(
        &self,
        mut trx: PrecommitTrx,
        mut group_commit_g: MutexGuard<'_, GroupCommit>,
    ) -> (Session, Receiver<()>) {
        let cts = trx.cts;
        let redo_bin = trx.redo_bin.take().unwrap();
        debug_assert!(!redo_bin.is_empty());
        // inside the lock, we only need to determine which range of the log file this transaction
        // should write to.
        let log_file = group_commit_g.log_file.as_ref().unwrap();
        let offset = match log_file.alloc(redo_bin.len()) {
            Ok((offset, _)) => offset,
            Err(AIOError::OutOfRange) => {
                // todo: rotate if log file is full.
                todo!();
            }
            Err(_) => unreachable!(),
        };
        let fd = log_file.as_raw_fd();
        let log_buf = self.buf(&redo_bin);
        let (sync_signal, sync_notifier) = flume::unbounded();
        let session = trx.split_session();
        let new_group = CommitGroup {
            trx_list: vec![trx],
            max_cts: cts,
            fd,
            offset,
            log_buf,
            sync_signal,
            sync_notifier: sync_notifier.clone(),
        };
        group_commit_g
            .queue
            .push_back(CommitMessage::Group(new_group));
        drop(group_commit_g);

        (session, sync_notifier)
    }

    /// Transaction has no redo log, so we can just acquire CTS and finish it immediately.
    #[inline]
    fn commit_without_redo(&self, trx: PreparedTrx, ts: &AtomicU64) -> Session {
        let cts = ts.fetch_add(1, Ordering::SeqCst);
        let committed_trx = trx.fill_cts(cts).commit();
        // todo: GC
        committed_trx.into_session()
    }

    #[inline]
    async fn commit(&self, trx: PreparedTrx, ts: &AtomicU64) -> Result<Session> {
        if trx.redo_bin.is_none() {
            let session = self.commit_without_redo(trx, ts);
            return Ok(session);
        }
        let mut group_commit_g = self.group_commit.0.lock();
        let cts = ts.fetch_add(1, Ordering::SeqCst);
        debug_assert!(cts < MAX_COMMIT_TS);
        let precommit_trx = trx.fill_cts(cts);
        if group_commit_g.queue.is_empty() {
            let (session, sync_notifier) = self.create_new_group(precommit_trx, group_commit_g);
            self.group_commit.1.notify_one(); // notify sync thread to work.

            let _ = sync_notifier.recv_async().await; // wait for fsync
            assert!(self.persisted_cts.load(Ordering::Relaxed) >= cts);
            return Ok(session);
        }
        let last_group = match group_commit_g.queue.back_mut().unwrap() {
            CommitMessage::Shutdown => return Err(Error::TransactionSystemShutdown),
            CommitMessage::Group(group) => group,
        };
        if last_group.can_join(&precommit_trx) {
            let (session, sync_notifier) = last_group.join(precommit_trx);
            drop(group_commit_g); // unlock to let other transactions to enter commit phase.

            let _ = sync_notifier.recv_async().await; // wait for fsync
            assert!(self.persisted_cts.load(Ordering::Relaxed) >= cts);
            return Ok(session);
        }

        let (session, sync_notifier) = self.create_new_group(precommit_trx, group_commit_g);

        let _ = sync_notifier.recv_async().await; // wait for fsync
        assert!(self.persisted_cts.load(Ordering::Relaxed) >= cts);
        Ok(session)
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
    fn try_fetch_io_reqs(
        &self,
        io_reqs: &mut Vec<IocbRawPtr>,
        sync_groups: &mut VecDeque<SyncGroup>,
    ) -> bool {
        let mut group_commit_g = self.group_commit.0.lock();
        loop {
            match group_commit_g.queue.pop_front() {
                None => return false,
                Some(CommitMessage::Shutdown) => return true,
                Some(CommitMessage::Group(cg)) => {
                    let (iocb_ptr, sg) = cg.split();
                    io_reqs.push(iocb_ptr);
                    sync_groups.push_back(sg);
                }
            }
        }
    }

    #[inline]
    fn fetch_io_reqs(
        &self,
        io_reqs: &mut Vec<IocbRawPtr>,
        sync_groups: &mut VecDeque<SyncGroup>,
    ) -> bool {
        let mut group_commit_g = self.group_commit.0.lock();
        loop {
            loop {
                match group_commit_g.queue.pop_front() {
                    None => break,
                    Some(CommitMessage::Shutdown) => {
                        return true;
                    }
                    Some(CommitMessage::Group(cg)) => {
                        let (iocb_ptr, sg) = cg.split();
                        io_reqs.push(iocb_ptr);
                        sync_groups.push_back(sg);
                    }
                }
            }
            if !io_reqs.is_empty() {
                return false;
            }
            self.group_commit
                .1
                .wait_for(&mut group_commit_g, Duration::from_secs(1));
        }
    }

    #[inline]
    fn io_loop_noop(&self, config: &TrxSysConfig) {
        let io_depth = config.io_depth_per_log;
        let mut io_reqs = Vec::with_capacity(io_depth * 2);
        let mut sync_groups = VecDeque::with_capacity(io_depth * 2);
        let mut shutdown = false;
        loop {
            if !shutdown {
                shutdown |= self.fetch_io_reqs(&mut io_reqs, &mut sync_groups);
            }
            if !io_reqs.is_empty() {
                let mut trx_count = 0;
                let mut commit_count = 0;
                let mut log_bytes = 0;
                for sg in &sync_groups {
                    trx_count += sg.trx_list.len();
                    commit_count += 1;
                    log_bytes += sg.log_bytes;
                }

                let max_cts = sync_groups.back().as_ref().unwrap().max_cts;

                self.persisted_cts.store(max_cts, Ordering::SeqCst);

                io_reqs.clear();

                for mut sync_group in sync_groups.drain(..) {
                    mem::take(&mut sync_group.trx_list)
                        .into_iter()
                        .for_each(|trx| {
                            trx.commit();
                        });
                }

                self.update_stats(trx_count, commit_count, log_bytes, 1, 0);
            }
            if shutdown {
                return;
            }
        }
    }

    #[inline]
    fn io_loop(&self, config: &TrxSysConfig) {
        let syncer = {
            self.group_commit
                .0
                .lock()
                .log_file
                .as_ref()
                .unwrap()
                .syncer()
        };
        let io_depth = config.io_depth_per_log;
        let mut inflight = BTreeMap::new();
        let mut in_progress = 0;
        let mut io_reqs = Vec::with_capacity(io_depth * 2);
        let mut sync_groups = VecDeque::with_capacity(io_depth * 2);
        let mut events = self.aio_mgr.events();
        let mut written = vec![];
        let mut shutdown = false;
        loop {
            debug_assert!(
                io_reqs.len() == sync_groups.len(),
                "pending IO number equals to pending group number"
            );
            if !shutdown {
                if in_progress == 0 {
                    // there is no processing AIO, so we can block on waiting for next request.
                    shutdown |= self.fetch_io_reqs(&mut io_reqs, &mut sync_groups);
                } else {
                    // only try non-blocking way to fetch incoming requests, because we also
                    // need to finish previous IO.
                    shutdown |= self.try_fetch_io_reqs(&mut io_reqs, &mut sync_groups);
                }
            }
            let (io_submit_count, io_submit_nanos) = if !io_reqs.is_empty() {
                let start = Instant::now();
                // try to submit as many IO requests as possible
                let limit = io_depth - in_progress;
                let submit_count = self.aio_mgr.submit_limit(&mut io_reqs, limit);
                // add sync groups to inflight tree.
                for sync_group in sync_groups.drain(..submit_count) {
                    let res = inflight.insert(sync_group.max_cts, sync_group);
                    debug_assert!(res.is_none());
                }
                in_progress += submit_count;
                debug_assert!(in_progress <= io_depth);
                (1, start.elapsed().as_nanos() as usize)
            } else {
                (0, 0)
            };

            // wait for any request to be done.
            let (io_wait_count, io_wait_nanos) = if in_progress != 0 {
                let start = Instant::now();
                let finish_count =
                    self.aio_mgr
                        .wait_at_least(&mut events, 1, |cts, res| match res {
                            Ok(len) => {
                                let sg = inflight.get_mut(&cts).expect("finish inflight IO");
                                debug_assert!(sg.aio.buf.as_ref().unwrap().aligned_len() == len);
                                sg.finished = true;
                            }
                            Err(err) => {
                                let sg = inflight.remove(&cts).unwrap();
                                unimplemented!(
                                    "AIO error: task.cts={}, task.log_bytes={}, {}",
                                    sg.max_cts,
                                    sg.log_bytes,
                                    err
                                )
                            }
                        });
                in_progress -= finish_count;
                (1, start.elapsed().as_nanos() as usize)
            } else {
                (0, 0)
            };

            // after logs are written to disk, we need to do fsync to make sure its durablity.
            // Also, we need to check if any previous transaction is also done and notify them.
            written.clear();
            let (trx_count, commit_count, log_bytes) = shrink_inflight(&mut inflight, &mut written);
            if !written.is_empty() {
                let max_cts = written.last().unwrap().max_cts;

                let start = Instant::now();
                match config.log_sync {
                    LogSync::Fsync => syncer.fsync(),
                    LogSync::Fdatasync => syncer.fdatasync(),
                    LogSync::None => (),
                }
                let sync_dur = start.elapsed();

                self.persisted_cts.store(max_cts, Ordering::SeqCst);

                // put IO buffer back into free list.
                for mut sync_group in written.drain(..) {
                    if let Some(Buf::Reuse(elem)) = sync_group.aio.take_buf() {
                        self.buf_free_list.push_elem(elem); // return buf to free list for future reuse
                    }
                    // commit transactions to let waiting read operations to continue
                    // todo: send to GC thread.
                    mem::take(&mut sync_group.trx_list)
                        .into_iter()
                        .for_each(|trx| {
                            trx.commit();
                        });
                    drop(sync_group); // notify transaction thread to continue.
                }

                self.update_stats(
                    trx_count,
                    commit_count,
                    log_bytes,
                    1,
                    sync_dur.as_nanos() as usize,
                );
            }
            if io_submit_count != 0 {
                self.stats
                    .io_submit_count
                    .fetch_add(io_submit_count, Ordering::Relaxed);
                self.stats
                    .io_submit_nanos
                    .fetch_add(io_submit_nanos, Ordering::Relaxed);
            }
            if io_wait_count != 0 {
                self.stats
                    .io_wait_count
                    .fetch_add(io_wait_count, Ordering::Relaxed);
                self.stats
                    .io_wait_nanos
                    .fetch_add(io_wait_nanos, Ordering::Relaxed);
            }

            if shutdown && inflight.is_empty() {
                return;
            }
        }
    }
}

struct GroupCommit {
    // Commit group queue, there can be multiple groups in commit phase.
    // Each of them submit IO request to AIO manager and then wait for
    // pwrite & fsync done.
    queue: VecDeque<CommitMessage>,
    // Current log file.
    log_file: Option<SparseFile>,
    // sequence of current file in this partition, starts from 0.
    file_seq: u32,
}

/// GCInfo is only used for GroupCommitter to store and analyze GC related information,
/// including committed transaction list, old transaction list, active snapshot timestamp
/// list, etc.
pub struct GCInfo {
    /// Committed transaction list.
    /// When a transaction is committed, it will be put into this queue in sequence.
    /// Head is always oldest and tail is newest.
    committed_trx_list: VecDeque<CommittedTrx>,
    /// Old transaction list.
    /// If a transaction's committed timestamp is less than the smallest
    /// snapshot timestamp of all active transactions, it means this transction's
    /// data vesion is latest and all its undo log can be purged.
    /// So we move such transactions from commited list to old list.
    old_trx_list: Vec<CommittedTrx>,
    /// Active snapshot timestamp list.
    /// The smallest value equals to min_active_sts.
    active_sts_list: BTreeSet<TrxID>,
}

impl GCInfo {
    #[inline]
    pub fn new() -> Self {
        GCInfo {
            committed_trx_list: VecDeque::new(),
            old_trx_list: Vec::new(),
            active_sts_list: BTreeSet::new(),
        }
    }
}

enum CommitMessage {
    Group(CommitGroup),
    Shutdown,
}

/// CommitGroup groups multiple transactions with only
/// one log IO and at most one fsync() call.
/// It is controlled by two parameters:
/// 1. Maximum IO size, e.g. 16KB.
/// 2. Timeout to wait for next transaction to join.
struct CommitGroup {
    trx_list: Vec<PrecommitTrx>,
    max_cts: TrxID,
    fd: RawFd,
    offset: usize,
    log_buf: Buf,
    sync_signal: Sender<()>,
    sync_notifier: Receiver<()>,
}

impl CommitGroup {
    #[inline]
    fn can_join(&self, trx: &PrecommitTrx) -> bool {
        if let Some(redo_bin) = trx.redo_bin.as_ref() {
            return redo_bin.len() <= self.log_buf.remaining_capacity();
        }
        true
    }

    #[inline]
    fn join(&mut self, mut trx: PrecommitTrx) -> (Session, Receiver<()>) {
        debug_assert!(self.max_cts < trx.cts);
        if let Some(redo_bin) = trx.redo_bin.take() {
            self.log_buf.clone_from_slice(&redo_bin);
        }
        self.max_cts = trx.cts;
        let session = trx.split_session();
        self.trx_list.push(trx);
        (session, self.sync_notifier.clone())
    }

    #[inline]
    fn split(self) -> (IocbRawPtr, SyncGroup) {
        let log_bytes = self.log_buf.aligned_len();
        let aio = pwrite(self.max_cts, self.fd, self.offset, self.log_buf);
        let iocb_ptr = aio.iocb.load(Ordering::Relaxed);
        let sync_group = SyncGroup {
            trx_list: self.trx_list,
            max_cts: self.max_cts,
            log_bytes,
            aio,
            sync_signal: self.sync_signal,
            finished: false,
        };
        (iocb_ptr, sync_group)
    }
}

struct SyncGroup {
    trx_list: Vec<PrecommitTrx>,
    max_cts: TrxID,
    log_bytes: usize,
    aio: AIO,
    // Signal to notify transaction threads.
    // This field won't be used, until the group is dropped.
    #[allow(dead_code)]
    sync_signal: Sender<()>,
    finished: bool,
}

#[derive(Default)]
pub struct TrxSysStats {
    pub commit_count: usize,
    pub trx_count: usize,
    pub log_bytes: usize,
    pub sync_count: usize,
    pub sync_nanos: usize,
    pub io_submit_count: usize,
    pub io_submit_nanos: usize,
    pub io_wait_count: usize,
    pub io_wait_nanos: usize,
}

#[derive(Default)]
pub struct LogPartitionStats {
    pub commit_count: AtomicUsize,
    pub trx_count: AtomicUsize,
    pub log_bytes: AtomicUsize,
    pub sync_count: AtomicUsize,
    pub sync_nanos: AtomicUsize,
    pub io_submit_count: AtomicUsize,
    pub io_submit_nanos: AtomicUsize,
    pub io_wait_count: AtomicUsize,
    pub io_wait_nanos: AtomicUsize,
}

#[inline]
fn log_file_name(file_prefix: &str, idx: usize, file_seq: u32) -> String {
    format!("{}.{}.{:08x}", file_prefix, idx, file_seq)
}

#[inline]
fn log_file_prefix(file_prefix: &str, idx: usize) -> String {
    format!("{}.{}", file_prefix, idx)
}

#[inline]
fn shrink_inflight(
    tree: &mut BTreeMap<TrxID, SyncGroup>,
    buffer: &mut Vec<SyncGroup>,
) -> (usize, usize, usize) {
    let mut trx_count = 0;
    let mut commit_count = 0;
    let mut log_bytes = 0;
    while let Some(entry) = tree.first_entry() {
        let task = entry.get();
        if task.finished {
            trx_count += task.trx_list.len();
            commit_count += 1;
            log_bytes += task.log_bytes;
            buffer.push(entry.remove());
        } else {
            break; // stop at the transaction which is not persisted.
        }
    }
    (trx_count, commit_count, log_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::Session;
    use crossbeam_utils::CachePadded;
    use parking_lot::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::Arc;
    use std::time::Instant;

    #[test]
    fn test_transaction_system() {
        let trx_sys = TrxSysConfig::default().build_static();
        let session = Session::new();
        {
            let trx = session.begin_trx(trx_sys);
            let _ = smol::block_on(trx_sys.commit(trx));
        }
        std::thread::spawn(|| {
            let session = Session::new();
            let trx = session.begin_trx(trx_sys);
            let _ = smol::block_on(trx_sys.commit(trx));
        })
        .join()
        .unwrap();
        unsafe {
            TransactionSystem::drop_static(trx_sys);
        }
    }

    #[test]
    fn test_single_thread_mutex_trx_id_generate() {
        const COUNT: usize = 1000000;
        let mu = Mutex::new(1u64);
        let start = Instant::now();
        for _ in 0..COUNT {
            let mut g = mu.lock();
            let _ = *g;
            *g += 1;
        }
        let dur = start.elapsed();
        println!(
            "{:?} transaction id generation cost {:?} microseconds, avg {:?} op/s",
            COUNT,
            dur.as_micros(),
            COUNT as f64 * 1_000_000_000f64 / dur.as_nanos() as f64
        );
    }

    #[test]
    fn test_multi_threads_mutex_trx_id_generate() {
        const COUNT: usize = 1000000;
        const THREADS: usize = 4;
        let mu = Arc::new(Mutex::new(1u64));
        let stop = Arc::new(AtomicBool::new(false));
        let start = Instant::now();
        let mut handles = vec![];
        for _ in 1..THREADS {
            let mu = Arc::clone(&mu);
            let stop = Arc::clone(&stop);
            let handle =
                std::thread::spawn(move || worker_thread_mutex_trx_id_generate(&mu, &stop));
            handles.push(handle);
        }
        let mut count = 0usize;
        for _ in 0..COUNT {
            let mut g = mu.lock();
            let _ = *g;
            *g += 1;
            count += 1;
        }
        stop.store(true, Ordering::SeqCst);
        for handle in handles {
            count += handle.join().unwrap();
        }
        let dur = start.elapsed();
        println!(
            "{:?} threads generate {:?} transaction ids in {:?} microseconds, avg {:?} op/s",
            THREADS,
            count,
            dur.as_micros(),
            count as f64 * 1_000_000_000f64 / dur.as_nanos() as f64
        );
    }

    #[inline]
    fn worker_thread_mutex_trx_id_generate(mu: &Mutex<u64>, stop: &AtomicBool) -> usize {
        let mut count = 0usize;
        while !stop.load(Ordering::Relaxed) {
            let mut g = mu.lock();
            let _ = *g;
            *g += 1;
            count += 1;
        }
        count
    }

    #[test]
    fn test_single_thread_atomic_trx_id_generate() {
        const COUNT: usize = 1000000;
        let atom = AtomicU64::new(1u64);
        let start = Instant::now();
        for _ in 0..COUNT {
            let _ = atom.fetch_add(1, Ordering::SeqCst);
        }
        let dur = start.elapsed();
        println!(
            "{:?} transaction id generation cost {:?} microseconds, avg {:?} op/s",
            COUNT,
            dur.as_micros(),
            COUNT as f64 * 1_000_000_000f64 / dur.as_nanos() as f64
        );
    }

    #[test]
    fn test_multi_threads_atomic_trx_id_generate() {
        const COUNT: usize = 1000000;
        const THREADS: usize = 4;
        let atom = Arc::new(CachePadded::new(AtomicU64::new(1u64)));
        let stop = Arc::new(AtomicBool::new(false));
        let start = Instant::now();
        let mut handles = vec![];
        for _ in 1..THREADS {
            let atom = Arc::clone(&atom);
            let stop = Arc::clone(&stop);
            let handle =
                std::thread::spawn(move || worker_thread_atomic_trx_id_generate(&atom, &stop));
            handles.push(handle);
        }
        let mut count = 0usize;
        for _ in 0..COUNT {
            let _ = atom.fetch_add(1, Ordering::SeqCst);
            count += 1;
        }
        stop.store(true, Ordering::SeqCst);
        for handle in handles {
            count += handle.join().unwrap();
        }
        let dur = start.elapsed();
        println!(
            "{:?} threads generate {:?} transaction ids in {:?} microseconds, avg {:?} op/s",
            THREADS,
            count,
            dur.as_micros(),
            count as f64 * 1_000_000_000f64 / dur.as_nanos() as f64
        );
    }

    #[inline]
    fn worker_thread_atomic_trx_id_generate(atom: &AtomicU64, stop: &AtomicBool) -> usize {
        let mut count = 0usize;
        while !stop.load(Ordering::Relaxed) {
            let _ = atom.fetch_add(1, Ordering::SeqCst);
            count += 1;
        }
        count
    }

    #[test]
    fn test_single_thread_trx_begin_and_commit() {
        const COUNT: usize = 1000000;
        let trx_sys = TrxSysConfig::default().build_static();
        let mut session = Session::new();
        {
            // hook persisted_ts to u64::MAX to allaw all transactions immediately finish.
            trx_sys.persisted_cts.store(u64::MAX, Ordering::SeqCst);
        }

        {
            let start = Instant::now();
            for _ in 0..COUNT {
                let trx = session.begin_trx(trx_sys);
                let res = smol::block_on(trx_sys.commit(trx));
                assert!(res.is_ok());
                session = res.unwrap();
            }
            let dur = start.elapsed();
            println!(
                "{:?} transaction begin and commit cost {:?} microseconds, avg {:?} trx/s",
                COUNT,
                dur.as_micros(),
                COUNT as f64 * 1_000_000_000f64 / dur.as_nanos() as f64
            );
        }
        unsafe {
            TransactionSystem::drop_static(trx_sys);
        }
    }
}
