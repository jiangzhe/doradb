use crate::io::{
    align_to_sector_size, pwrite_direct, AIOError, AIOManager, AIOManagerConfig, DirectBuf,
    FileSyncer, IocbPtr, SparseFile, AIO,
};
use crate::trx::redo::RedoBin;
use crate::trx::{
    ActiveTrx, CommittedTrx, PrecommitTrx, TrxID, MAX_COMMIT_TS, MAX_SNAPSHOT_TS,
    MIN_ACTIVE_TRX_ID, MIN_SNAPSHOT_TS,
};
use crossbeam_utils::CachePadded;
use flume::{Receiver, RecvTimeoutError, Sender};
use parking_lot::{Mutex, MutexGuard};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::mem;
use std::ops::Deref;
use std::os::fd::{AsRawFd, RawFd};
use std::sync::atomic::{AtomicPtr, AtomicU64, AtomicUsize, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;

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
    /// Group commit implementation.
    /// This implementation limit the group size to dispatch more IO operations instead
    /// of combining them into single large operation.
    group_commit: CachePadded<Mutex<GroupCommit>>,
    /// Status map of inflight commits.
    inflight_status: CachePadded<InflightStatus>,
    /// list of precommitted transactions.
    /// Once user sends COMMIT command or statement is auto-committed. The transaction
    /// will be assign CTS and put into this list, waiting for log writer thread to
    /// persist its
    // precommit_trx_list: CachePadded<(Mutex<Vec<PrecommitTrx>>, Condvar)>,
    /// Rollbacked transaction snapshot timestamp list.
    /// This list is used to calculate active sts list.
    rollback_sts_list: CachePadded<Mutex<BTreeSet<TrxID>>>,
    /// Persisted commit timestamp is the maximum commit timestamp of all persisted redo
    /// logs. Precommit transactions are already notified by group committer. This global
    /// atomic variable is used by query or GC thread to perform GC.
    persisted_cts: CachePadded<AtomicU64>,
    /// Committed transaction list.
    /// When a transaction is committed, it will be put into this queue in sequence.
    /// Head is always oldest and tail is newest.
    gc_info: CachePadded<Mutex<GCInfo>>,
    /// Configuration of transaction system.
    config: CachePadded<TrxSysConfig>,
    /// Stats of transaction system.
    stats: CachePadded<TrxSysStats>,
    /// AIO Manager to control logging operation.
    aio_mgr: CachePadded<AIOManager>,
    /// Channel to send IO task to IO thread.
    io_chan: Sender<IOTask>,
    /// Sync log to disk.
    sync_thread: Mutex<Option<JoinHandle<()>>>,
    /// Background GC thread identify which transactions can be garbage collected and
    /// calculate watermark for other thread to cooperate.
    gc_thread: Mutex<Option<JoinHandle<()>>>,
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
    pub fn new_trx(&self) -> ActiveTrx {
        // active transaction list is calculated by group committer thread
        // so here we just generate STS and TrxID.
        let sts = self.ts.fetch_add(1, Ordering::SeqCst);
        let trx_id = sts | (1 << 63);
        debug_assert!(sts < MAX_SNAPSHOT_TS);
        debug_assert!(trx_id >= MIN_ACTIVE_TRX_ID);
        ActiveTrx::new(trx_id, sts)
    }

    /// Commit an active transaction.
    /// The commit process is implemented as group commit.
    /// If multiple transactions are being committed at the same time, one of them
    /// will become leader of the commit group. Others become followers waiting for
    /// leader to persist log and backfill CTS.
    /// This strategy can largely reduce logging IO, therefore improve throughput.
    #[inline]
    pub async fn commit(&self, trx: ActiveTrx) {
        let max_io_size = self.config.max_io_size;
        let log_io_depth = self.config.log_io_depth;

        // Prepare redo log first, this may take some time,
        // so keep it out of lock scope, and only fill cts then.
        let prepared_trx = trx.prepare();

        // acquire lock and start group commit
        let mut group_commit_g = self.group_commit.lock();
        let cts = self.ts.fetch_add(1, Ordering::SeqCst);
        debug_assert!(cts < MAX_COMMIT_TS);
        let mut precommit_trx = prepared_trx.fill_cts(cts);
        if !group_commit_g.backlog.is_empty() || self.inflight_status.count() >= log_io_depth {
            // backlog is not empty or inflight commit number reach the limitation.
            // This means inflight IO number reach the limit.
            // Then let current transaction join commit group in backlog
            let sync_notifier = group_commit_g.backlog.join(precommit_trx, max_io_size);
            drop(group_commit_g); // unlock to let other transactions process.
                                  // other thread will push the progress of backlog, so just wait for fsync notification.
            let _ = sync_notifier.recv_async().await;

            // assert!(self.persisted_cts.load(Ordering::Relaxed) >= cts);
            return;
        }

        // inflight commit number is less than threshold, current thread do single transaction commit.
        if precommit_trx.redo_bin.is_none() {
            // no redo log, that mean no data change.
            return;
        }

        let redo_bin = precommit_trx.redo_bin.take().unwrap();
        debug_assert!(!redo_bin.is_empty());

        // inside the lock, we only need to determine which range of the log file this transaction
        // should write to.
        let log_file = group_commit_g.logger.as_ref().unwrap();
        let offset = match log_file.alloc(redo_bin.len()) {
            Ok((offset, _)) => offset,
            Err(AIOError::OutOfRange) => {
                // todo: if log file is full. open a new file and update logger.
                todo!();
            }
            Err(_) => unreachable!(),
        };
        let fd = log_file.as_raw_fd();
        // increment inflight commit number here to make it precise.
        self.inflight_status.increase_count(1);
        drop(group_commit_g); // unlock to let other transactions enter commit phase

        let (sync_signal, sync_notifier) = flume::unbounded();
        // put current commit into inflight status tree.
        let redo_bin = DirectBuf::with_data(&redo_bin);
        // length of DirectIO must be aligned.
        let log_bytes = redo_bin.capacity();
        // Construct AIO request.
        let aio = pwrite_direct(cts, fd, offset, redo_bin);
        // let iocb_ptr = AtomicPtr::new(aio.iocb.load(Ordering::Relaxed));
        let task = CommitTask {
            trx_list: vec![precommit_trx],
            cts,
            log_bytes,
            aio,
            sync_finished: false,
            sync_signal,
        };
        // self.inflight_status.insert_pending(task);

        // Send to sync thread to execute.
        // this may yield because the channel is bounded.
        let _ = self.io_chan.send_async(IOTask::AIO(task)).await;

        // wait for sync thread to notify fsync is done.
        let _ = sync_notifier.recv_async().await;

        // assert!(self.persisted_cts.load(Ordering::Relaxed) >= cts);
    }

    /// Rollback active transaction.
    #[inline]
    pub fn rollback(&self, trx: ActiveTrx) {
        let sts = trx.sts;
        trx.rollback();
        let mut g = self.rollback_sts_list.lock();
        let rollback_inserted = g.insert(sts);
        debug_assert!(rollback_inserted);
    }

    /// Returns statistics of group commit.
    #[inline]
    pub fn trx_sys_stats(&self) -> &TrxSysStats {
        &self.stats
    }

    #[inline]
    fn gc(&self, trx_list: Vec<CommittedTrx>) {
        if trx_list.is_empty() {
            return;
        }
        let persisted_cts = trx_list
            .last()
            .expect("committed transaction list is not empty")
            .cts;

        // Re-calculate GC info, including active_sts_list, committed_trx_list, old_trx_list
        // min_active_sts.
        {
            let mut gc_info_g = self.gc_info.lock();

            let GCInfo {
                committed_trx_list,
                old_trx_list,
                active_sts_list,
            } = &mut *gc_info_g;

            // swap out active sts list for update.
            let mut next_active_sts_list = mem::take(active_sts_list);

            // add all potential active sts
            let next_min_active_sts = if let Some(max) = next_active_sts_list.last() {
                *max + 1
            } else {
                MIN_SNAPSHOT_TS
            };
            let next_max_active_sts = self.ts.load(Ordering::Relaxed);
            for ts in next_min_active_sts..next_max_active_sts {
                let sts_inserted = next_active_sts_list.insert(ts);
                debug_assert!(sts_inserted);
            }

            // remove sts and cts of committed transactions
            for trx in &trx_list {
                let sts_removed = next_active_sts_list.remove(&trx.sts);
                debug_assert!(sts_removed);
                let cts_removed = next_active_sts_list.remove(&trx.cts);
                debug_assert!(cts_removed);
            }

            // remove rollback sts
            {
                let mut removed_rb_sts = vec![];
                let mut rb_g = self.rollback_sts_list.lock();
                for rb_sts in (&rb_g).iter() {
                    if next_active_sts_list.remove(rb_sts) {
                        removed_rb_sts.push(*rb_sts);
                    } // otherwise, rollback trx is added after latest sts is acquired, which should be very rare.
                }
                for rb_sts in removed_rb_sts {
                    let rb_sts_removed = rb_g.remove(&rb_sts);
                    debug_assert!(rb_sts_removed);
                }
            }
            // calculate smallest active sts
            // 1. if active_sts_list is empty, means there is no new transaction after group commit.
            // so maximum persisted cts + 1 can be used.
            // 2. otherwise, use minimum value in active_sts_list.
            let min_active_sts = if let Some(min) = next_active_sts_list.first() {
                *min
            } else {
                persisted_cts + 1
            };

            // update smallest active sts
            self.min_active_sts.store(min_active_sts, Ordering::Relaxed);

            // update active_sts_list
            *active_sts_list = next_active_sts_list;

            // populate committed transaction list
            committed_trx_list.extend(trx_list);

            // move committed transactions to old transaction list, waiting for GC.
            while let Some(trx) = committed_trx_list.front() {
                if trx.cts < min_active_sts {
                    old_trx_list.push(committed_trx_list.pop_front().unwrap());
                } else {
                    break;
                }
            }

            // todo: implement undo cleaner based on old transaction list.
            // currently just ignore.
            old_trx_list.clear();
        }
    }

    /// loop to handle sync task.
    /// It will try to batch and submit IOs, wait and notify once request is done.
    fn io_loop(&self, syncer: FileSyncer, sync_rx: Receiver<IOTask>) {
        let mut reqs = vec![];
        let mut events = self.aio_mgr.events();
        // let mut written = vec![];
        loop {
            let mut trx_count = 0;
            let mut commit_count = 0;
            let mut log_bytes = 0;
            let task = if self.inflight_status.count() == 0 {
                // there is no ongoing IO, so we can block on wait for new requests.
                match sync_rx.recv_timeout(Duration::from_millis(100)) {
                    Ok(task) => Some(task),
                    Err(RecvTimeoutError::Timeout) => None,
                    Err(RecvTimeoutError::Disconnected) => return,
                }
            } else {
                // here we do not block on waiting for new request because we may have ongoing ones.
                None
            };
            if let Some(task) = task {
                match task {
                    IOTask::Shutdown => return,
                    IOTask::Open(_) => todo!(),
                    IOTask::Switch(_) => todo!(),
                    IOTask::AIO(task) => {
                        let iocb_ptr = task.aio.iocb.load(Ordering::Relaxed);
                        // debug_assert!(self.inflight_status.exists(unsafe { (*iocb_ptr).data }));
                        reqs.push(iocb_ptr);
                        trx_count += task.trx_list.len();
                        commit_count += 1;
                        log_bytes += task.log_bytes;
                    }
                }
            }

            // submit IO requests as many as possible.
            while let Ok(task) = sync_rx.try_recv() {
                match task {
                    IOTask::Shutdown => return,
                    IOTask::Open(_) => todo!(),
                    IOTask::Switch(_) => todo!(),
                    IOTask::AIO(task) => {
                        let iocb_ptr = task.aio.iocb.load(Ordering::Relaxed);
                        // debug_assert!(self.inflight_status.exists(unsafe { (*iocb_ptr).data }));
                        reqs.push(iocb_ptr);
                        trx_count += task.trx_list.len();
                        commit_count += 1;
                        log_bytes += task.log_bytes;
                    }
                }
            }

            if reqs.is_empty() {
                continue;
            }

            let finish_count = reqs.len();
            self.stats.trx_count.fetch_add(trx_count, Ordering::Relaxed);
            self.stats
                .commit_count
                .fetch_add(commit_count, Ordering::Relaxed);
            self.stats.log_bytes.fetch_add(log_bytes, Ordering::Relaxed);
            self.stats.sync_count.fetch_add(1, Ordering::Relaxed);

            reqs.clear();

            // self.aio_mgr.submit(&mut reqs);
            // if self.inflight_status.count() == 0 {
            //     continue;
            // }

            // // wait for any request to be done.
            // let finish_count = self.aio_mgr.wait_at_least(&mut events, 1, |cts, res| {
            //     match res {
            //         Ok(_len) => {
            //             self.inflight_status.mark_finished(cts);
            //         }
            //         Err(err) => {
            //             let task = self.inflight_status.remove_failed(cts);
            //             unimplemented!("AIO error: task.cts={}, task.log_bytes={}, {}", task.cts, task.log_bytes, err)
            //         }
            //     }
            // });

            // update inflight count, and bring more transactions into commit phase
            let inflight_count = self.inflight_status.decrease_count(finish_count);
            if inflight_count < self.config.log_io_depth {
                self.consume_backlog(inflight_count);
            }

            // // after logs are written to disk, we need to do fsync to make sure its durablity.
            // // Also, we need to check if any previous transaction is also done and notify them.
            // written.clear();
            // self.inflight_status.shrink(&mut written);
            // if written.is_empty() {
            //     continue; // no group committed
            // }
            // let max_cts = written.last().unwrap().cts;

            // // syncer.fdatasync();

            // self.persisted_cts.store(max_cts, Ordering::SeqCst);

            // // update statistics
            // self.update_stats(&written);

            // // todo: GC
            // written.clear(); // also notify transactions to continue.
        }
    }

    #[inline]
    fn update_stats(&self, tasks: &[CommitTask]) {
        let mut trx_count = 0;
        let mut log_bytes = 0;
        for task in tasks {
            trx_count += task.trx_list.len();
            log_bytes += task.log_bytes;
        }
        self.stats.trx_count.fetch_add(trx_count, Ordering::Relaxed);
        self.stats
            .commit_count
            .fetch_add(tasks.len(), Ordering::Relaxed);
        self.stats.log_bytes.fetch_add(log_bytes, Ordering::Relaxed);
        self.stats.sync_count.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    fn consume_backlog(&self, inflight_count: usize) {
        let count = self.config.log_io_depth - inflight_count;
        let mut group_commit_g = self.group_commit.lock();
        if group_commit_g.backlog.is_empty() {
            return;
        }
        let inflight_count = self.inflight_status.count();
        if inflight_count >= self.config.log_io_depth {
            return;
        }
        let count = count.min(group_commit_g.backlog.len());
        let groups = group_commit_g.backlog.pop_many(count);
        // determine log fd and calculate log offsets.
        let log_file = group_commit_g.logger.as_ref().unwrap();
        let fd = log_file.as_raw_fd();
        let offsets: Vec<usize> = groups
            .iter()
            .map(|group| match log_file.alloc(group.redo_bin.len()) {
                Ok((offset, _)) => offset,
                Err(AIOError::OutOfRange) => {
                    // todo: if log file is full. open a new file and update logger.
                    todo!();
                }
                Err(_) => unreachable!(),
            })
            .collect();
        // increment inflight commit number here to make it precise.
        self.inflight_status.increase_count(groups.len());
        drop(group_commit_g); // unlock to let other transactions enter commit phase.

        for (mut group, offset) in groups.into_iter().zip(offsets) {
            let trx_count = group.trx_list.len();
            let redo_bin = DirectBuf::with_data(&group.redo_bin);
            let log_bytes = redo_bin.capacity();
            let aio = pwrite_direct(group.max_cts, fd, offset, redo_bin);
            let iocb_ptr = AtomicPtr::new(aio.iocb.load(Ordering::Relaxed));
            let task = CommitTask {
                trx_list: group.trx_list,
                cts: group.max_cts,
                log_bytes,
                aio,
                sync_finished: false,
                sync_signal: group.sync_signal.take().unwrap(),
            };
            self.io_chan.try_send(IOTask::AIO(task)).unwrap(); // won't fail and won't block
        }
    }

    /// Start background GC thread.
    /// This method should be called once transaction system is initialized.
    #[inline]
    fn start_gc_thread(&'static self) {
        let (tx, rx) = flume::unbounded();

        // Pass GC channel to group commit
        {
            self.group_commit.lock().gc_chan = Some(tx);
        }

        // Start thread
        let handle = thread::Builder::new()
            .name("GC-Thread".to_string())
            .spawn(move || {
                while let Ok(committed_trx_list) = rx.recv() {
                    self.gc(committed_trx_list);
                }
            })
            .unwrap();
        *self.gc_thread.lock() = Some(handle);
    }

    /// Start background sync thread.
    /// This method should be called once transaction system is initialized,
    /// and should after start_gc_thread().
    #[inline]
    fn start_sync_thread(&'static self, syncer: FileSyncer, sync_rx: Receiver<IOTask>) {
        // Start thread
        let handle = thread::Builder::new()
            .name("Sync-Thread".to_string())
            .spawn(move || {
                self.io_loop(syncer, sync_rx);
            })
            .unwrap();

        *self.sync_thread.lock() = Some(handle);
    }
}

unsafe impl Sync for TransactionSystem {}

impl Drop for TransactionSystem {
    #[inline]
    fn drop(&mut self) {
        // notify sync thread and gc thread
        let _ = self.io_chan.send(IOTask::Shutdown).unwrap();
        {
            let mut group_commit_g = self.group_commit.lock();
            drop(group_commit_g.gc_chan.take());
        }
        // wait for sync thread and gc thread to exit
        {
            if let Some(sync_thread) = self.sync_thread.lock().take() {
                sync_thread.join().unwrap();
            }
            if let Some(gc_thread) = self.gc_thread.lock().take() {
                gc_thread.join().unwrap();
            }
        }
        // drop log file manually
        let log_file = self.group_commit.lock().logger.take().unwrap();
        self.aio_mgr.drop_sparse_file(log_file);
    }
}

pub const DEFAULT_IODEPTH: usize = 32;
pub const DEFAULT_LOG_IODEPTH: usize = 16;
pub const DEFAULT_LOG_IO_MAX_SIZE: usize = 8192;
pub const DEFAULT_LOG_FILE: &'static str = "redo.log";
pub const DEFAULT_LOG_FILE_MAX_SIZE: usize = 256 * 1024 * 1024; // 256MB, sparse file will not occupy space until actual write.
pub const DEFAULT_LOG_WAIT_INTERVAL: Duration = Duration::from_millis(100);
pub const DEFAULT_GC: bool = false;

pub struct TrxSysConfig {
    io_depth: usize,
    log_io_depth: usize,
    max_io_size: usize,
    // here we do not allow disabling logging.
    log_file: String,
    log_file_max_size: usize,
    // log wait interval.
    log_wait_interval: Duration,
    gc: bool,
}

impl TrxSysConfig {
    /// How many commits can be issued concurrently.
    #[inline]
    pub fn io_depth(mut self, io_depth: usize) -> Self {
        self.io_depth = io_depth;
        self
    }

    /// How many commits can be issued concurrently.
    #[inline]
    pub fn log_io_depth(mut self, log_io_depth: usize) -> Self {
        self.log_io_depth = log_io_depth;
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
    pub fn log_file(mut self, log_file: String) -> Self {
        self.log_file = log_file;
        self
    }

    /// Maximum size of single log file.
    #[inline]
    pub fn log_file_max_size(mut self, log_file_max_size: usize) -> Self {
        self.log_file_max_size = align_to_sector_size(log_file_max_size);
        self
    }

    /// Log wait interval, if only one transaction is committing, and IO request
    /// is submitted to linux kernel, this parameter determines how long to check
    /// if IO is done in loop.
    #[inline]
    pub fn log_wait_interval(mut self, log_wait_interval: Duration) -> Self {
        self.log_wait_interval = log_wait_interval;
        self
    }

    /// Build transaction system with logging and GC, leak it to heap
    /// for the convenience to share the singleton among multiple threads.
    #[inline]
    pub fn build_static(self) -> &'static TransactionSystem {
        debug_assert!(self.io_depth > self.log_io_depth);
        let aio_mgr = AIOManagerConfig::default()
            .max_events(self.io_depth)
            .build()
            .expect("create AIO manager");
        let log_file = aio_mgr
            .create_sparse_file(&self.log_file, self.log_file_max_size)
            .expect("create sparse log file");
        let syncer = log_file.syncer();

        let gc = self.gc;
        // use channel to limit concurrent IO.
        let (sync_chan, sync_rx) = flume::unbounded();
        // use channel to

        let logger = TrxSysLogger {
            log_file: Some(log_file),
        };

        let group_commit = GroupCommit {
            backlog: Backlog::default(),
            logger,
            gc_chan: None,
        };

        let trx_sys = TransactionSystem {
            ts: CachePadded::new(AtomicU64::new(MIN_SNAPSHOT_TS)),
            min_active_sts: CachePadded::new(AtomicU64::new(MIN_SNAPSHOT_TS)),
            group_commit: CachePadded::new(Mutex::new(group_commit)),
            inflight_status: CachePadded::new(InflightStatus::default()),
            rollback_sts_list: CachePadded::new(Mutex::new(BTreeSet::new())),
            persisted_cts: CachePadded::new(AtomicU64::new(MIN_SNAPSHOT_TS)),
            gc_info: CachePadded::new(Mutex::new(GCInfo::new())),
            config: CachePadded::new(self),
            stats: CachePadded::new(TrxSysStats::default()),
            aio_mgr: CachePadded::new(aio_mgr),
            io_chan: sync_chan,
            sync_thread: Mutex::new(None),
            gc_thread: Mutex::new(None),
        };
        let trx_sys = Box::leak(Box::new(trx_sys));
        if gc {
            trx_sys.start_gc_thread();
        }
        trx_sys.start_sync_thread(syncer, sync_rx);
        trx_sys
    }
}

impl Default for TrxSysConfig {
    #[inline]
    fn default() -> Self {
        TrxSysConfig {
            io_depth: DEFAULT_IODEPTH,
            log_io_depth: DEFAULT_LOG_IODEPTH,
            max_io_size: DEFAULT_LOG_IO_MAX_SIZE,
            log_file: String::from(DEFAULT_LOG_FILE),
            log_file_max_size: DEFAULT_LOG_FILE_MAX_SIZE,
            log_wait_interval: DEFAULT_LOG_WAIT_INTERVAL,
            gc: DEFAULT_GC,
        }
    }
}

struct TrxSysLogger {
    // current log file.
    log_file: Option<SparseFile>,
}

impl Deref for TrxSysLogger {
    type Target = Option<SparseFile>;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.log_file
    }
}

impl TrxSysLogger {
    #[inline]
    fn take(&mut self) -> Option<SparseFile> {
        self.log_file.take()
    }
}

#[derive(Default)]
struct InflightStatus {
    tree: Mutex<BTreeMap<TrxID, CommitTask>>,
    count: AtomicUsize,
}

impl InflightStatus {
    #[inline]
    fn count(&self) -> usize {
        self.count.load(Ordering::Relaxed)
    }

    #[inline]
    fn exists(&self, cts: TrxID) -> bool {
        let g = self.tree.lock();
        g.contains_key(&cts)
    }

    #[inline]
    fn increase_count(&self, count: usize) -> usize {
        self.count.fetch_add(count, Ordering::Relaxed) + count
    }

    #[inline]
    fn decrease_count(&self, count: usize) -> usize {
        self.count.fetch_sub(count, Ordering::Relaxed) - count
    }

    #[inline]
    fn insert_pending(&self, task: CommitTask) {
        let mut g = self.tree.lock();
        let res = g.insert(task.cts, task);
        debug_assert!(res.is_none());
    }

    #[inline]
    fn remove_failed(&self, cts: TrxID) -> CommitTask {
        let mut g = self.tree.lock();
        g.remove(&cts).unwrap()
    }

    #[inline]
    fn mark_finished(&self, cts: TrxID) {
        let mut g = self.tree.lock();
        let task = g.get_mut(&cts).expect("sync task status");
        task.sync_finished = true;
    }

    #[inline]
    pub fn shrink(&self, buffer: &mut Vec<CommitTask>) {
        let mut g = self.tree.lock();
        while let Some(entry) = g.first_entry() {
            let task = entry.get();
            if task.sync_finished {
                buffer.push(entry.remove());
            } else {
                break; // stop at the transaction which is not persisted.
            }
        }
    }

    #[inline]
    pub fn clear(&self) {
        self.tree.lock().clear();
    }
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

struct GroupCommit {
    backlog: Backlog,
    /// Redo logger of transaction system.
    logger: TrxSysLogger,
    /// Channel to send committed transactions for GC.
    gc_chan: Option<Sender<Vec<CommittedTrx>>>,
}

#[derive(Default)]
struct Backlog(VecDeque<CommitGroup>);

impl Backlog {
    #[inline]
    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    #[inline]
    fn len(&self) -> usize {
        self.0.len()
    }

    #[inline]
    fn join(&mut self, trx: PrecommitTrx, max_io_size: usize) -> Receiver<()> {
        if let Some(group) = self.0.back_mut() {
            if group.can_join(&trx, max_io_size) {
                return group.join(trx);
            }
            return self.add_new_group(trx);
        }
        self.add_new_group(trx)
    }

    #[inline]
    fn add_new_group(&mut self, mut trx: PrecommitTrx) -> Receiver<()> {
        // cannot join, form a new group
        let (fsync_signal, sync_notifier) = flume::unbounded();
        let max_cts = trx.cts;
        let redo_bin = trx.redo_bin.take().unwrap_or_default();
        let group = CommitGroup {
            trx_list: vec![trx],
            max_cts,
            redo_bin,
            sync_signal: Some(fsync_signal),
            sync_notifier: sync_notifier.clone(),
        };
        self.0.push_back(group);
        sync_notifier
    }

    #[inline]
    fn pop_many(&mut self, count: usize) -> Vec<CommitGroup> {
        self.0.drain(..count).collect()
    }
}

/// GroupCommit is an abstraction to group multiple transactions
/// and write and sync logs in batch mode.
/// The first transaction thread arrived in the group becomes the
/// group leader, and take care of all transactions log persistence.
/// It's used to improve logging performance.
struct CommitGroup {
    trx_list: Vec<PrecommitTrx>,
    max_cts: TrxID,
    redo_bin: RedoBin,
    sync_signal: Option<Sender<()>>,
    // every transaction in this group should wait for this notification
    // to make sure logs of itself are done(fsync succeeds).
    // if single trx group, this field can be None, because that trx can hold
    // the notification locally.
    sync_notifier: Receiver<()>,
}

impl CommitGroup {
    #[inline]
    fn can_join(&self, trx: &PrecommitTrx, max_io_size: usize) -> bool {
        if let Some(redo_bin) = trx.redo_bin.as_ref() {
            return self.redo_bin.len() + redo_bin.len() <= max_io_size;
        }
        true
    }

    #[inline]
    fn join(&mut self, mut trx: PrecommitTrx) -> Receiver<()> {
        debug_assert!(self.max_cts < trx.cts);
        if let Some(redo_bin) = trx.redo_bin.take() {
            self.redo_bin.extend(redo_bin);
        }
        self.max_cts = trx.cts;
        self.trx_list.push(trx);
        self.sync_notifier.clone()
    }
}

enum IOTask {
    // if log file is full, we should open a new file and switch to it.
    Switch(Sender<()>),
    Open(SparseFile),
    // shutdown the sync thread.
    Shutdown,
    AIO(CommitTask),
}

struct CommitTask {
    trx_list: Vec<PrecommitTrx>,
    cts: TrxID,
    log_bytes: usize,
    // used to hold AIO buffer, make the iocb pointer be valid during AIO processing.
    aio: AIO,
    sync_finished: bool,
    sync_signal: Sender<()>,
}

#[derive(Default)]
pub struct TrxSysStats {
    pub commit_count: AtomicUsize,
    pub trx_count: AtomicUsize,
    pub log_bytes: AtomicUsize,
    pub sync_count: AtomicUsize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossbeam_utils::CachePadded;
    use parking_lot::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::Arc;
    use std::time::Instant;

    #[test]
    fn test_transaction_system() {
        let trx_sys = TrxSysConfig::default().build_static();
        {
            let trx = trx_sys.new_trx();
            smol::block_on(trx_sys.commit(trx));
        }
        std::thread::spawn(|| {
            let trx = trx_sys.new_trx();
            smol::block_on(trx_sys.commit(trx));
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
        {
            // hook persisted_ts to u64::MAX to allaw all transactions immediately finish.
            trx_sys.persisted_cts.store(u64::MAX, Ordering::SeqCst);
        }

        {
            let start = Instant::now();
            for _ in 0..COUNT {
                let trx = trx_sys.new_trx();
                smol::block_on(trx_sys.commit(trx));
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

    // #[test]
    // fn test_trx_sys_group_commit() {
    //     let trx_sys = TrxSysConfig::default().build_static();
    //     {
    //         let new_trx = trx_sys.new_trx();
    //         smol::block_on(trx_sys.commit(new_trx));
    //     }
    //     // sts=1, cts=2, next_ts=3
    //     for _ in 0..20 {
    //         // 2 seconds should be enough
    //         if trx_sys.ts.load(Ordering::Relaxed) == 3
    //             && trx_sys.min_active_sts.load(Ordering::Relaxed) == 3
    //         {
    //             break;
    //         }
    //         thread::sleep(std::time::Duration::from_millis(100));
    //     }
    //     assert!(trx_sys.ts.load(Ordering::Relaxed) == 3);
    //     assert!(trx_sys.min_active_sts.load(Ordering::Relaxed) == 3);
    //     unsafe {
    //         TransactionSystem::drop_static(trx_sys);
    //     }
    // }
}
