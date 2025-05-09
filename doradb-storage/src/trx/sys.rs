use crate::buffer::BufferPool;
use crate::catalog::Catalog;
use crate::error::{Error, Result};
use crate::io::{align_to_sector_size, AIOManagerConfig, FreeListWithFactory, PageBuf};
use crate::latch::Mutex;
use crate::lifetime::StaticLifetime;
use crate::notify::Signal;
use crate::session::{IntoSession, Session};
use crate::thread;
use crate::trx::group::{Commit, GroupCommit};
use crate::trx::log::{create_log_file, LogPartition, LogPartitionStats, LogSync};
use crate::trx::purge::{GCBucket, Purge, GC};
use crate::trx::redo::RedoLogs;
use crate::trx::sys_trx::SysTrx;
use crate::trx::{
    ActiveTrx, PreparedTrx, TrxID, MAX_SNAPSHOT_TS, MIN_ACTIVE_TRX_ID, MIN_SNAPSHOT_TS,
};
use crossbeam_utils::CachePadded;
use flume::{Receiver, Sender};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::mem;
use std::panic::{RefUnwindSafe, UnwindSafe};
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::thread::JoinHandle;
pub const GC_BUCKETS: usize = 64;
use byte_unit::Byte;

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
pub struct TransactionSystem<P: BufferPool> {
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
    pub(super) log_partitions: CachePadded<Box<[CachePadded<LogPartition<P>>]>>,
    /// Transaction system configuration.
    pub(super) config: CachePadded<TrxSysConfig>,
    /// Catalog of the database.
    pub(crate) catalog: CachePadded<Catalog<P>>,
    /// Channel to send message to purge threads.
    purge_chan: Sender<Purge>,
    /// Purge threads purge unused undo logs, row pages and index entries.
    pub(super) purge_threads: Mutex<Vec<JoinHandle<()>>>,
}

impl<P: BufferPool> TransactionSystem<P> {
    #[inline]
    pub async fn start(&'static self, buf_pool: &'static P, start_ctx: TrxSysStartContext<P>) {
        self.start_sync_threads();
        self.start_gc_threads(start_ctx.gc_chans);
        self.start_purge_threads(buf_pool, start_ctx.purge_rx);

        // todo: recover catalog.
        unsafe {
            self.catalog.init(buf_pool, self).await;
        }
        // todo: recover data from log file.
    }

    /// Create a new transaction.
    #[inline]
    pub fn begin_trx(&self, session: Session<P>) -> ActiveTrx<P> {
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

    #[inline]
    pub fn begin_sys_trx(&self) -> SysTrx {
        SysTrx {
            redo: RedoLogs::default(),
        }
    }

    /// Commit an active transaction.
    /// The commit process is implemented as group commit.
    /// If multiple transactions are being committed at the same time, one of them
    /// will become leader of the commit group. Others become followers waiting for
    /// leader to persist log and backfill CTS.
    /// This strategy can largely reduce logging IO, therefore improve throughput.
    #[inline]
    pub async fn commit(&self, trx: ActiveTrx<P>, buf_pool: &'static P) -> Result<Session<P>> {
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
            let session = self.rollback_prepared(prepared_trx, buf_pool).await;
            return Ok(session);
        }
        // start group commit
        partition
            .commit(prepared_trx, &self.ts)
            .await
            .and_then(|res| res.ok_or(Error::UserSessionMissing))
    }

    #[inline]
    pub async fn commit_sys(&self, trx: SysTrx) -> Result<()> {
        if trx.redo.is_empty() {
            // System transaction does not hold any active start timestamp
            // so we can just drop it if there is no change to replay.
            return Ok(());
        }
        // system transactions are always submitted to first log partition.
        const LOG_NO: usize = 0;
        let partition = &*self.log_partitions[LOG_NO];
        let prepared_trx = trx.prepare::<P>();
        partition.commit(prepared_trx, &self.ts).await.map(|_| ())
    }

    /// Rollback active transaction.
    #[inline]
    pub async fn rollback(&self, mut trx: ActiveTrx<P>, buf_pool: &'static P) -> Session<P> {
        trx.row_undo.rollback(buf_pool).await;
        trx.index_undo.rollback(&self.catalog);
        trx.redo.clear();
        self.log_partitions[trx.log_no].gc_buckets[trx.gc_no].gc_analyze_rollback(trx.sts);
        trx.into_session().unwrap()
    }

    /// Rollback prepared transaction.
    /// This is special case of transaction commit without redo log.
    /// In such case, we do not need to go through entire commit process but just
    /// rollback the transaction, because it actually do nothing.
    #[inline]
    async fn rollback_prepared(&self, mut trx: PreparedTrx<P>, buf_pool: &'static P) -> Session<P> {
        debug_assert!(trx.redo_bin.is_none());
        // Note: rollback can only happens to user transaction, so payload is always non-empty.
        let mut payload = trx.payload.take().unwrap();
        payload.row_undo.rollback(buf_pool).await;
        payload.index_undo.rollback(&self.catalog);
        trx.redo_bin.take();
        self.log_partitions[payload.log_no].gc_buckets[payload.gc_no]
            .gc_analyze_rollback(payload.sts);
        trx.into_session().unwrap()
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
    fn start_gc_threads(&'static self, gc_chans: Vec<Receiver<GC<P>>>) {
        for ((idx, partition), gc_rx) in self.log_partitions.iter().enumerate().zip(gc_chans) {
            let thread_name = format!("GC-Thread-{}", idx);
            let partition = &**partition;
            let purge_chan = self.purge_chan.clone();
            let handle =
                thread::spawn_named(thread_name, move || partition.gc_loop(gc_rx, purge_chan));
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
            let handle = thread::spawn_named(thread_name, move || partition.io_loop(&self.config));
            *partition.sync_thread.lock() = Some(handle);
        }
    }
}

unsafe impl<P: BufferPool> StaticLifetime for TransactionSystem<P> {}

impl<P: BufferPool> UnwindSafe for TransactionSystem<P> {}

impl<P: BufferPool> RefUnwindSafe for TransactionSystem<P> {}

impl<P: BufferPool> Drop for TransactionSystem<P> {
    #[inline]
    fn drop(&mut self) {
        let log_partitions = &*self.log_partitions;
        for partition in log_partitions {
            // notify sync thread to quit.
            {
                let mut group_commit_g = partition.group_commit.0.lock();
                group_commit_g.queue.push_back(Commit::Shutdown);
                if group_commit_g.queue.len() == 1 {
                    Signal::set_and_notify(&partition.group_commit.1, 1); // notify sync thread to quit.
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
pub const DEFAULT_LOG_IO_MAX_SIZE: Byte = Byte::from_u64(8192);
pub const DEFAULT_LOG_FILE_PREFIX: &'static str = "redo.log";
pub const DEFAULT_LOG_PARTITIONS: usize = 1;
pub const MAX_LOG_PARTITIONS: usize = 99; // big enough for log partitions, so fix two digits in file name.
pub const DEFAULT_LOG_FILE_MAX_SIZE: Byte = Byte::from_u64(1024 * 1024 * 1024); // 1GB, sparse file will not occupy space until actual write.
pub const DEFAULT_LOG_SYNC: LogSync = LogSync::Fsync;
pub const DEFAULT_PURGE_THREADS: usize = 2;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrxSysConfig {
    // Controls infight IO request number of each log file.
    pub io_depth_per_log: usize,
    // Controls maximum IO size of each IO request.
    // This only limit the combination of multiple transactions.
    // If single transaction has very large redo log. It is kept
    // what it is and send to AIO manager as one IO request.
    pub max_io_size: Byte,
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
    pub log_file_max_size: Byte,
    // Controls which method to sync data on disk.
    // By default, use fsync(),
    // Can be switched to fdatasync() or not sync.
    pub log_sync: LogSync,
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
    pub fn max_io_size<T>(mut self, max_io_size: T) -> Self
    where
        Byte: From<T>,
    {
        self.max_io_size = Byte::from(max_io_size);
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
    pub fn log_file_max_size<T>(mut self, log_file_max_size: T) -> Self
    where
        Byte: From<T>,
    {
        let size = Byte::from(log_file_max_size);
        let aligned_size = align_to_sector_size(size.as_u64() as usize);
        self.log_file_max_size = <Byte as From<usize>>::from(aligned_size);
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

    fn setup_log_partition<P: BufferPool>(
        &self,
        log_no: usize,
    ) -> (LogPartition<P>, Receiver<GC<P>>) {
        let aio_mgr = AIOManagerConfig::default()
            .max_events(self.io_depth_per_log)
            .build()
            .expect("create AIO manager");

        // todo: handle recovery from previous log file.
        let mut file_seq = 0;
        let log_file = create_log_file(
            &aio_mgr,
            &self.log_file_prefix,
            log_no,
            file_seq,
            self.log_file_max_size.as_u64() as usize,
        )
        .expect("create log file");
        file_seq += 1;

        let group_commit = GroupCommit {
            queue: VecDeque::new(),
            log_file: Some(log_file),
        };
        let max_io_size = self.max_io_size.as_u64() as usize;
        let gc_info: Vec<_> = (0..GC_BUCKETS).map(|_| GCBucket::new()).collect();
        let (gc_chan, gc_rx) = flume::unbounded();
        (
            LogPartition {
                group_commit: CachePadded::new((Mutex::new(group_commit), Signal::default())),
                persisted_cts: CachePadded::new(AtomicU64::new(MIN_SNAPSHOT_TS)),
                stats: CachePadded::new(LogPartitionStats::default()),
                gc_chan,
                gc_buckets: gc_info.into_boxed_slice(),
                aio_mgr,
                log_no,
                max_io_size,
                file_prefix: self.log_file_prefix.clone(),
                file_seq: AtomicU32::new(file_seq),
                file_max_size: self.log_file_max_size.as_u64() as usize,
                buf_free_list: FreeListWithFactory::prefill(self.io_depth_per_log, move || {
                    PageBuf::zeroed(max_io_size)
                }),
                sync_thread: Mutex::new(None),
                gc_thread: Mutex::new(None),
            },
            gc_rx,
        )
    }

    #[inline]
    pub fn build<P: BufferPool>(self) -> (TransactionSystem<P>, TrxSysStartContext<P>) {
        let (purge_chan, purge_rx) = flume::unbounded();

        let mut log_partitions = Vec::with_capacity(self.log_partitions);
        let mut gc_chans = Vec::with_capacity(self.log_partitions);
        for idx in 0..self.log_partitions {
            let (partition, gc_rx) = self.setup_log_partition(idx);
            log_partitions.push(CachePadded::new(partition));
            gc_chans.push(gc_rx);
        }
        let trx_sys = TransactionSystem {
            ts: CachePadded::new(AtomicU64::new(MIN_SNAPSHOT_TS)),
            min_active_sts: CachePadded::new(AtomicU64::new(MIN_SNAPSHOT_TS)),
            partition_id: CachePadded::new(AtomicUsize::new(0)),
            log_partitions: CachePadded::new(log_partitions.into_boxed_slice()),
            config: CachePadded::new(self),
            catalog: CachePadded::new(Catalog::empty()),
            purge_chan,
            purge_threads: Mutex::new(vec![]),
        };
        (trx_sys, TrxSysStartContext { purge_rx, gc_chans })
    }

    /// Build transaction system with logging and GC, leak it to heap
    /// for the convenience to share the singleton among multiple threads.
    #[inline]
    pub async fn build_static<P: BufferPool>(
        self,
        buf_pool: &'static P,
    ) -> &'static TransactionSystem<P> {
        let (trx_sys, start_ctx) = self.build();
        let trx_sys = StaticLifetime::new_static(trx_sys);
        trx_sys.start(buf_pool, start_ctx).await;
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

pub struct TrxSysStartContext<P: BufferPool> {
    // Receiver side of purge requests, used by dispatcher/purge thread.
    pub purge_rx: Receiver<Purge>,
    // Receiver side of GC requests, used by purge thread.
    pub gc_chans: Vec<Receiver<GC<P>>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::Engine;
    use crate::value::Val;
    use crossbeam_utils::CachePadded;
    use parking_lot::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::Arc;
    use std::time::Instant;

    #[test]
    fn test_transaction_system() {
        smol::block_on(async {
            let engine = Engine::new_fixed(128 * 1024 * 1024, TrxSysConfig::default())
                .await
                .unwrap();
            let session = engine.new_session();
            {
                let trx = session.begin_trx();
                let _ = smol::block_on(trx.commit());
            }
            std::thread::spawn(move || {
                let session = engine.new_session();
                let trx = session.begin_trx();
                let _ = smol::block_on(trx.commit());
            })
            .join()
            .unwrap();
            unsafe {
                StaticLifetime::drop_static(engine);
            }
        })
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
            let engine = Engine::new_fixed(128 * 1024 * 1024, TrxSysConfig::default())
                .await
                .unwrap();
            let mut session = engine.new_session();
            let start = Instant::now();
            for _ in 0..COUNT {
                let trx = session.begin_trx();
                let res = trx.commit().await;
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
                StaticLifetime::drop_static(engine);
            }
        });
    }

    #[test]
    fn test_log_rotate() {
        use crate::catalog::tests::table2;
        // 10000 rows, 200 bytes each row, 20M log file size.
        // log file is 5MB, so it will rotate at least 4 times.
        // Due to alignment of direct IO, the write amplification might
        // be higher and produce more files.
        const COUNT: usize = 10000;
        smol::block_on(async {
            let engine = Engine::new_fixed(
                128 * 1024 * 1024,
                TrxSysConfig::default()
                    .log_partitions(1)
                    .log_file_max_size(1024u64 * 1024 * 5),
            )
            .await
            .unwrap();
            let table_id = table2(engine).await;
            let table = engine.catalog().get_table(table_id).unwrap();

            let mut session = engine.new_session();
            let s = vec![1u8; 200];
            for i in 0..COUNT {
                let trx = session.begin_trx();
                let mut stmt = trx.start_stmt();
                let insert = vec![Val::from(i as i32), Val::from(&s[..])];
                let res = stmt.insert_row(&table, insert).await;
                assert!(res.is_ok());
                let trx = stmt.succeed();
                session = trx.commit().await.unwrap();
            }

            unsafe {
                StaticLifetime::drop_static(engine);
            }
        });
    }
}
