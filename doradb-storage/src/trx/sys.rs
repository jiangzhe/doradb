use crate::buffer::BufferPool;
use crate::catalog::Catalog;
use crate::error::Result;
use crate::io::{align_to_sector_size, AIOManagerConfig, FreeListWithFactory, PageBuf};
use crate::lifetime::StaticLifetime;
use crate::session::{InternalSession, IntoSession, Session};
use crate::trx::group::{Commit, GroupCommit};
use crate::trx::log::{LogPartition, LogPartitionStats, LogSync};
use crate::trx::purge::{GCBucket, Purge, GC};
use crate::trx::{
    ActiveTrx, PreparedTrx, TrxID, MAX_SNAPSHOT_TS, MIN_ACTIVE_TRX_ID, MIN_SNAPSHOT_TS,
};
use crossbeam_utils::CachePadded;
use flume::{Receiver, Sender};
use parking_lot::{Condvar, Mutex};
use std::collections::VecDeque;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::mem;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::thread::{self, JoinHandle};

pub const GC_BUCKETS: usize = 64;

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
    pub(super) ts: CachePadded<AtomicU64>,
    /// Minimum active snapshot timestamp.
    /// It's updated by group committer thread, and is used by query/GC thread to clean
    /// out-of-date version chains.
    ///
    /// Note: this field may not reflect the latest value, but is enough for GC purpose.
    pub(super) min_active_sts: CachePadded<AtomicU64>,
    /// Round-robin partition id generator.
    partition_id: CachePadded<AtomicUsize>,
    /// Multiple log partitions.
    pub(super) log_partitions: CachePadded<Box<[CachePadded<LogPartition>]>>,
    /// Transaction system configuration.
    pub(super) config: CachePadded<TrxSysConfig>,
    /// Channel to send message to purge threads.
    purge_chan: Sender<Purge>,
    /// Purge threads purge unused undo logs, row pages and index entries.
    pub(super) purge_threads: Mutex<Vec<JoinHandle<()>>>,
}

impl TransactionSystem {
    /// Create a new transaction.
    #[inline]
    pub fn new_trx<'a>(&self, session: Option<Box<InternalSession>>) -> ActiveTrx {
        // Active transaction list is calculated by group committer thread
        // so here we just generate STS and TrxID.
        let sts = self.ts.fetch_add(1, Ordering::SeqCst);
        let trx_id = sts | (1 << 63);
        debug_assert!(sts < MAX_SNAPSHOT_TS);
        debug_assert!(trx_id >= MIN_ACTIVE_TRX_ID);
        // Assign log partition index so current transaction will stick
        // to certain log partititon for commit.
        let log_no = if self.config.log_partitions == 1 {
            0
        } else {
            self.partition_id.fetch_add(1, Ordering::Relaxed) % self.config.log_partitions
        };
        // Add to active sts list.
        let gc_no = gc_no(sts);
        {
            let gc_bucket = &self.log_partitions[log_no].gc_buckets[gc_no];
            let mut g = gc_bucket.active_sts_list.lock();
            g.insert(sts);
            if g.len() == 1 {
                // Only when the previous list is empty, we should update min_active_sts
                // as STS of current transaction.
                // In this case, current value of min_active_sts should be MAX.
                debug_assert!(gc_bucket.min_active_sts.load(Ordering::Relaxed) == MAX_SNAPSHOT_TS);
                gc_bucket.min_active_sts.store(sts, Ordering::Relaxed);
            }
        }
        ActiveTrx::new(session, trx_id, sts, log_no, gc_no)
    }

    /// Commit an active transaction.
    /// The commit process is implemented as group commit.
    /// If multiple transactions are being committed at the same time, one of them
    /// will become leader of the commit group. Others become followers waiting for
    /// leader to persist log and backfill CTS.
    /// This strategy can largely reduce logging IO, therefore improve throughput.
    #[inline]
    pub async fn commit<P: BufferPool>(
        &self,
        trx: ActiveTrx,
        buf_pool: P,
        catalog: &Catalog<P>,
    ) -> Result<Session> {
        // Prepare redo log first, this may take some time,
        // so keep it out of lock scope, and we can fill cts after the lock is held.
        let partition = &*self.log_partitions[trx.log_no];
        let prepared_trx = trx.prepare();
        if prepared_trx.redo_bin.is_none() {
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
            // In such case, we can just rollback this transaction because it actually
            // do nothing.
            return Ok(self.rollback_prepared(prepared_trx, buf_pool, catalog));
        }
        // start group commit
        partition.commit(prepared_trx, &self.ts).await
    }

    /// Rollback active transaction.
    #[inline]
    pub fn rollback<P: BufferPool>(
        &self,
        mut trx: ActiveTrx,
        buf_pool: P,
        catalog: &Catalog<P>,
    ) -> Session {
        trx.row_undo.rollback(buf_pool);
        trx.index_undo.rollback(catalog);
        self.log_partitions[trx.log_no].gc_buckets[trx.gc_no].gc_analyze_rollback(trx.sts);
        trx.into_session()
    }

    /// Rollback prepared transaction.
    /// This is special case of transaction commit without redo log.
    /// In such case, we do not need to go through entire commit process but just
    /// rollback the transaction, because it actually do nothing.
    #[inline]
    fn rollback_prepared<P: BufferPool>(
        &self,
        mut trx: PreparedTrx,
        buf_pool: P,
        catalog: &Catalog<P>,
    ) -> Session {
        debug_assert!(trx.redo_bin.is_none());
        trx.row_undo.rollback(buf_pool);
        trx.index_undo.rollback(catalog);
        self.log_partitions[trx.log_no].gc_buckets[trx.gc_no].gc_analyze_rollback(trx.sts);
        trx.into_session()
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
            stats.purge_trx_count += partition.stats.purge_trx_count.load(Ordering::Relaxed);
            stats.purge_row_count += partition.stats.purge_row_count.load(Ordering::Relaxed);
            stats.purge_index_count += partition.stats.purge_index_count.load(Ordering::Relaxed);
        }
        stats
    }

    /// Start background GC threads.
    #[inline]
    fn start_gc_threads(&'static self, gc_chans: Vec<Receiver<GC>>) {
        for ((idx, partition), gc_rx) in self.log_partitions.iter().enumerate().zip(gc_chans) {
            let thread_name = format!("GC-Thread-{}", idx);
            let partition = &**partition;
            let purge_chan = self.purge_chan.clone();
            let handle = thread::Builder::new()
                .name(thread_name)
                .spawn(move || partition.gc_loop(gc_rx, purge_chan, self.config.gc))
                .unwrap();
            *partition.gc_thread.lock() = Some(handle);
        }
    }

    /// Start background sync threads.
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

unsafe impl StaticLifetime for TransactionSystem {}

impl Drop for TransactionSystem {
    #[inline]
    fn drop(&mut self) {
        let log_partitions = &*self.log_partitions;
        for partition in log_partitions {
            // notify sync thread to quit.
            {
                let mut group_commit_g = partition.group_commit.0.lock();
                group_commit_g.queue.push_back(Commit::Shutdown);
                if group_commit_g.queue.len() == 1 {
                    partition.group_commit.1.notify_one(); // notify sync thread to quit.
                }
            }
            // notify gc thread to quit.
            let _ = partition.gc_chan.send(GC::Stop);
        }
        // wait for sync thread and GC thread to quit.
        for partition in log_partitions {
            let sync_thread = { partition.sync_thread.lock().take().unwrap() };
            sync_thread.join().unwrap();
            let gc_thread = { partition.gc_thread.lock().take().unwrap() };
            gc_thread.join().unwrap();
        }
        // notify purge threads and wait for them to quit.
        {
            let _ = self.purge_chan.send(Purge::Stop);
            let purge_threads = { mem::take(&mut *self.purge_threads.lock()) };
            for handle in purge_threads {
                handle.join().unwrap();
            }
        }
        // finally close log files
        for partition in log_partitions {
            let mut group_commit_g = partition.group_commit.0.lock();
            let log_file = group_commit_g.log_file.take().unwrap();
            partition.aio_mgr.drop_sparse_file(log_file);
        }
    }
}

#[inline]
fn gc_no(sts: TrxID) -> usize {
    let mut hasher = DefaultHasher::default();
    sts.hash(&mut hasher);
    let value = hasher.finish();
    value as usize % GC_BUCKETS
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
pub const DEFAULT_PURGE_THREADS: usize = 2;

pub struct TrxSysConfig {
    // Controls infight IO request number of each log file.
    pub io_depth_per_log: usize,
    // Controls maximum IO size of each IO request.
    // This only limit the combination of multiple transactions.
    // If single transaction has very large redo log. It is kept
    // what it is and send to AIO manager as one IO request.
    pub max_io_size: usize,
    // Prefix of log file.
    // the complete file name pattern is:
    // <file-prefix>.<partition_idx>.<file-sequence>
    // e.g. redo.log.0.00000001
    pub log_file_prefix: String,
    // Log partition number.
    pub log_partitions: usize,
    // Controls the maximum size of each log file.
    // Log file will be rotated once the size limit is reached.
    // a u32 suffix is appended at end of the file name in
    // hexdigit format.
    pub log_file_max_size: usize,
    // Controls which method to sync data on disk.
    // By default, use fsync(),
    // Can be switched to fdatasync() or not sync.
    pub log_sync: LogSync,
    // Drop log directly. If this parameter is set to true,
    // log_sync parameter will be discarded.
    pub log_drop: bool,
    // Whether enable GC or not.
    pub gc: bool,
    // Threads for purging undo logs.
    pub purge_threads: usize,
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
    /// Note:
    #[inline]
    pub fn gc(mut self, gc: bool) -> Self {
        self.gc = gc;
        self
    }

    /// How many threads to execute for purge undo logs and remove
    /// unused index and row pages.
    #[inline]
    pub fn purge_threads(mut self, purge_threads: usize) -> Self {
        self.purge_threads = purge_threads;
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

    fn setup_log_partition(&self, idx: usize) -> (LogPartition, Receiver<GC>) {
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
        let gc_info: Vec<_> = (0..GC_BUCKETS).map(|_| GCBucket::new()).collect();
        let (gc_chan, gc_rx) = flume::unbounded();
        (
            LogPartition {
                group_commit: CachePadded::new((Mutex::new(group_commit), Condvar::new())),
                persisted_cts: CachePadded::new(AtomicU64::new(MIN_SNAPSHOT_TS)),
                stats: CachePadded::new(LogPartitionStats::default()),
                gc_chan,
                gc_buckets: gc_info.into_boxed_slice(),
                aio_mgr,
                log_no: idx,
                max_io_size,
                file_prefix: log_file_prefix(&self.log_file_prefix, idx),
                buf_free_list: FreeListWithFactory::prefill(self.io_depth_per_log, move || {
                    PageBuf::uninit(max_io_size)
                }),
                sync_thread: Mutex::new(None),
                gc_thread: Mutex::new(None),
            },
            gc_rx,
        )
    }

    /// Build transaction system with logging and GC, leak it to heap
    /// for the convenience to share the singleton among multiple threads.
    #[inline]
    pub fn build_static<P: BufferPool>(
        self,
        buf_pool: P,
        catalog: &'static Catalog<P>,
    ) -> &'static TransactionSystem {
        let mut log_partitions = Vec::with_capacity(self.log_partitions);
        let mut gc_chans = Vec::with_capacity(self.log_partitions);
        for idx in 0..self.log_partitions {
            let (partition, gc_rx) = self.setup_log_partition(idx);
            log_partitions.push(CachePadded::new(partition));
            gc_chans.push(gc_rx);
        }
        let (purge_chan, purge_rx) = flume::unbounded();
        let trx_sys = TransactionSystem {
            ts: CachePadded::new(AtomicU64::new(MIN_SNAPSHOT_TS)),
            min_active_sts: CachePadded::new(AtomicU64::new(MIN_SNAPSHOT_TS)),
            partition_id: CachePadded::new(AtomicUsize::new(0)),
            log_partitions: CachePadded::new(log_partitions.into_boxed_slice()),
            config: CachePadded::new(self),
            purge_chan,
            purge_threads: Mutex::new(vec![]),
        };
        let trx_sys = StaticLifetime::new_static(trx_sys);
        trx_sys.start_sync_threads();
        trx_sys.start_gc_threads(gc_chans);
        trx_sys.start_purge_threads(buf_pool, catalog, purge_rx);
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
            purge_threads: DEFAULT_PURGE_THREADS,
        }
    }
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
    pub purge_trx_count: usize,
    pub purge_row_count: usize,
    pub purge_index_count: usize,
}

#[inline]
fn log_file_name(file_prefix: &str, idx: usize, file_seq: u32) -> String {
    format!("{}.{}.{:08x}", file_prefix, idx, file_seq)
}

#[inline]
fn log_file_prefix(file_prefix: &str, idx: usize) -> String {
    format!("{}.{}", file_prefix, idx)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::FixedBufferPool;
    use crate::catalog::Catalog;
    use crate::session::Session;
    use crossbeam_utils::CachePadded;
    use parking_lot::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::Arc;
    use std::time::Instant;

    #[test]
    fn test_transaction_system() {
        let buf_pool = FixedBufferPool::with_capacity_static(128 * 1024 * 1024).unwrap();
        let catalog = Catalog::empty_static();
        let trx_sys = TrxSysConfig::default().build_static(buf_pool, catalog);
        let session = Session::new();
        {
            let trx = session.begin_trx(trx_sys);
            let _ = smol::block_on(trx_sys.commit(trx, buf_pool, catalog));
        }
        std::thread::spawn(move || {
            let session = Session::new();
            let trx = session.begin_trx(trx_sys);
            let _ = smol::block_on(trx_sys.commit(trx, buf_pool, catalog));
        })
        .join()
        .unwrap();
        unsafe {
            TransactionSystem::drop_static(trx_sys);
            Catalog::drop_static(catalog);
            FixedBufferPool::drop_static(buf_pool);
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
        smol::block_on(async {
            let buf_pool = FixedBufferPool::with_capacity_static(128 * 1024 * 1024).unwrap();
            let catalog = Catalog::empty_static();
            let trx_sys = TrxSysConfig::default().build_static(buf_pool, catalog);
            let mut session = Session::new();
            let start = Instant::now();
            for _ in 0..COUNT {
                let trx = session.begin_trx(trx_sys);
                let res = trx_sys.commit(trx, buf_pool, catalog).await;
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
            unsafe {
                TransactionSystem::drop_static(trx_sys);
                Catalog::drop_static(catalog);
                FixedBufferPool::drop_static(buf_pool);
            }
        });
    }
}
