use crate::trx::redo::RedoBin;
use crate::trx::{
    ActiveTrx, CommittedTrx, PrecommitTrx, TrxID, MAX_COMMIT_TS, MAX_SNAPSHOT_TS,
    MIN_ACTIVE_TRX_ID, MIN_SNAPSHOT_TS,
};
use crossbeam_utils::CachePadded;
use flume::{Receiver, Sender};
use parking_lot::{Mutex, MutexGuard};
use std::collections::{BTreeSet, VecDeque};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::mem;
use std::os::fd::RawFd;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread::{self, JoinHandle};

pub const DEFAULT_LOG_LEN_THRESHOLD: usize = 4096;

/// Logger should be thread safe.
pub trait RedoLogger {
    /// Write redo binary logs to disk.
    fn write(&self, cts: TrxID, redo_bin: &RedoBin) -> usize;

    /// Wait for previous written logs to be persisted.
    fn flush(&self);
}

pub trait RedoSyncer {
    /// Sync data to storage device.
    fn sync(&self);
}

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
    /// Log writer controls how to write redo log buffer to disk.
    logger: Box<dyn RedoLogger>,
    /// Log syncer to perform fsync for redo log.
    syncer: Box<dyn RedoSyncer + Send>,
    /// Sync counter
    stats: TrxSysStats,
    /// Sync log to disk.
    sync_thread: Mutex<Option<JoinHandle<()>>>,
    /// Background GC thread identify which transactions can be garbage collected and
    /// calculate watermark for other thread to cooperate.
    gc_thread: Mutex<Option<JoinHandle<()>>>,
}

impl TransactionSystem {
    /// Setup a builder to initialize transaction system.
    #[inline]
    pub fn builder() -> TrxSysBuilder {
        TrxSysBuilder::default()
    }

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
        // Prepare redo log first, this may take some time,
        // so keep it out of lock scope, and only fill cts then.
        let prepared_trx = trx.prepare();
        // start group commit
        let mut group_commit_g = self.group_commit.lock();
        let cts = self.ts.fetch_add(1, Ordering::SeqCst);
        debug_assert!(cts < MAX_COMMIT_TS);
        let precommit_trx = prepared_trx.fill_cts(cts);
        let log_len_threshold = group_commit_g.log_len_threshold;
        match group_commit_g.groups.len() {
            // single tranasaction won't yield.
            0 => self.act_as_single_trx_leader(precommit_trx, group_commit_g),
            1 => {
                let curr_group = group_commit_g.groups.front_mut().unwrap();
                match &mut curr_group.kind {
                    // There is only one commit group and it's processing, so current thread
                    // becomes leader of next group, and waits for previous one to finish,
                    // and then start processing.
                    CommitGroupKind::Processing => {
                        self.act_as_next_group_leader(precommit_trx, group_commit_g)
                            .await
                    }
                    CommitGroupKind::Sequential(_) => {
                        // This is at least the second transaction entering a non-started commit group,
                        // it should add itself to this group and setup notifier.
                        debug_assert!(curr_group.next_leader_notifier.is_some());
                        // Here we check if current group is full because log length exceeds threshold.
                        // If yes, create a new group instead of joining current one.
                        if curr_group.log_len >= log_len_threshold {
                            return self
                                .act_as_next_group_leader(precommit_trx, group_commit_g)
                                .await;
                        }
                        curr_group.add_trx(precommit_trx);
                        let notify = curr_group
                            .follower_notifier
                            .get_or_insert_with(flume::unbounded)
                            .1
                            .clone();
                        drop(group_commit_g); // release lock to let other transactions enter or leader start.

                        // wait for leader to finish group commit
                        let _ = notify.recv_async().await;

                        // log persisted max CTS must be greater than or equal to current trx CTS.
                        assert!(self.persisted_cts.load(Ordering::Relaxed) >= cts);
                    }
                }
            }
            _ => {
                // This is common scenario: previous group commit is running, and next group is already established.
                // Just join it.
                debug_assert!(matches!(
                    group_commit_g.groups.front().unwrap().kind,
                    CommitGroupKind::Processing
                ));
                debug_assert!(!matches!(
                    group_commit_g.groups.back().unwrap().kind,
                    CommitGroupKind::Processing
                ));
                let curr_group = group_commit_g.groups.back_mut().unwrap();
                debug_assert!({
                    matches!(&curr_group.kind, CommitGroupKind::Sequential(_))
                        && curr_group.next_leader_notifier.is_some()
                });
                if curr_group.log_len >= log_len_threshold {
                    return self
                        .act_as_next_group_leader(precommit_trx, group_commit_g)
                        .await;
                }
                // join existing group, there must be one leader already.
                curr_group.add_trx(precommit_trx);
                let notify = curr_group
                    .follower_notifier
                    .get_or_insert_with(flume::unbounded)
                    .1
                    .clone();
                drop(group_commit_g); // release lock to let other transactions enter or leader start.

                // wait for leader to finish group commit
                let _ = notify.recv_async().await;

                // log persisted max CTS must be greater than or equal to current trx CTS.
                // assert!(self.persisted_cts.load(Ordering::SeqCst) >= cts);
                assert!(self.persisted_cts.load(Ordering::Relaxed) >= cts);
            }
        }
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
    fn commit_internal(&self, trx_list: &mut [PrecommitTrx]) -> (TrxID, usize) {
        debug_assert!(trx_list.len() > 1);
        debug_assert!({
            trx_list
                .iter()
                .zip(trx_list.iter().skip(1))
                .all(|(l, r)| l.cts < r.cts)
        });
        let max_cts = trx_list.last().unwrap().cts;
        let mut log_bytes = 0;
        // persist log
        for trx in trx_list {
            if let Some(redo_bin) = trx.redo_bin.take() {
                log_bytes += self.logger.write(trx.cts, &redo_bin);
            }
        }
        self.logger.flush();

        (max_cts, log_bytes)
    }

    #[inline]
    fn act_as_single_trx_leader(
        &self,
        mut precommit_trx: PrecommitTrx,
        mut group_commit_g: MutexGuard<'_, GroupCommit>,
    ) {
        // no group commit running, current thread is just leader and do single transaction commit.
        let log_len = precommit_trx.redo_bin_len();
        let new_group = CommitGroup {
            kind: CommitGroupKind::Processing,
            log_len,
            follower_notifier: None,
            next_leader_notifier: None,
        };
        group_commit_g.groups.push_back(new_group);
        drop(group_commit_g);

        let (cts, log_bytes) = self.commit_internal(std::slice::from_mut(&mut precommit_trx));

        // Here we remove the finished group and let other transactions to enter commit phase.
        // fsync() will be called in a separate thread.
        let mut group_commit_g = self.group_commit.lock();
        debug_assert!(group_commit_g.groups.len() >= 1);
        debug_assert!(matches!(
            group_commit_g.groups.front().unwrap().kind,
            CommitGroupKind::Processing
        ));

        // Since this group contains only one transaction, we'd like to just do fsync() in current thread.
        // Even if we dispatch fsync() task to the sync thread, we still need to wait for it to finish.
        let curr_group = group_commit_g.groups.pop_front().unwrap();
        let gc_chan = group_commit_g.gc_chan.clone();

        drop(group_commit_g); // unlock to let other transaction enter commit phase.

        debug_assert!(curr_group.follower_notifier.is_none());
        drop(curr_group); // notify leader of next group if any.

        let mut sync_group = SyncGroup::new(vec![precommit_trx], None, cts, log_bytes);
        let committed_trx_list = self.sync(sync_group);

        assert!(self.persisted_cts.load(Ordering::Relaxed) >= cts);

        // update stats
        self.stats.trx_count.fetch_add(1, Ordering::Relaxed);
        self.stats.commit_count.fetch_add(1, Ordering::Relaxed);
        self.stats.sync_count.fetch_add(1, Ordering::Relaxed);
        self.stats
            .log_bytes
            .fetch_add(log_bytes as u64, Ordering::Relaxed);

        // GC if necessary
        if let Some(gc_chan) = gc_chan {
            let _ = gc_chan.send(committed_trx_list);
        }
    }

    #[inline]
    async fn act_as_next_group_leader(
        &self,
        precommit_trx: PrecommitTrx,
        mut group_commit_g: MutexGuard<'_, GroupCommit>,
    ) {
        let this_cts = precommit_trx.cts;
        // First group is processing, so become leader and create a new group.
        let log_len = precommit_trx.redo_bin_len();
        let new_group = CommitGroup {
            kind: CommitGroupKind::Sequential(vec![precommit_trx]),
            log_len,
            follower_notifier: None,
            next_leader_notifier: None,
        };

        // Register next leader notifier of last group.
        let notify = {
            group_commit_g
                .groups
                .back_mut()
                .unwrap()
                .next_leader_notifier
                .get_or_insert_with(flume::unbounded)
                .1
                .clone()
        };

        // Add this group into queue.
        group_commit_g.groups.push_back(new_group);

        drop(group_commit_g); // release lock to let other transactions join the new group.

        // Wait until previous group to finish.
        let _ = notify.recv_async().await;

        // Now previous group finishes(writing log), start current group.
        let kind = {
            let mut group_commit_g = self.group_commit.lock();
            // Previous leader removed its group before notifying next leader.
            // So current group must be at head of the queue.
            let curr_group = group_commit_g.groups.front_mut().unwrap();
            let kind = mem::replace(&mut curr_group.kind, CommitGroupKind::Processing);
            kind
        }; // Here release the lock so other transactions can form new group.

        // Persist redo log, backfill cts for each transaction, and get back maximum persisted cts.
        let (cts, trx_list, log_bytes) = match kind {
            CommitGroupKind::Sequential(mut trx_list) => {
                let (cts, log_bytes) = self.commit_internal(&mut trx_list);
                (cts, trx_list, log_bytes)
            }
            _ => unreachable!("invalid group commit kind"),
        };

        let mut group_commit_g = self.group_commit.lock();
        debug_assert!(group_commit_g.groups.len() >= 1);
        debug_assert!(matches!(
            group_commit_g.groups.front().unwrap().kind,
            CommitGroupKind::Processing
        ));

        assert!(!trx_list.is_empty());
        let mut curr_group = group_commit_g.groups.pop_front().unwrap();
        let gc_chan = group_commit_g.gc_chan.clone();
        let sync_chan = group_commit_g.sync_chan.clone();

        drop(group_commit_g); // unlock to let other transaction enter commit phase.

        drop(curr_group.next_leader_notifier.take()); // notify leader of next group.

        // If we do not have followers, we can perform fsync in current thread.
        // Otherwise, delegate fsync() to sync thread.
        let follower_notifier = curr_group.follower_notifier.take();
        drop(curr_group);
        if follower_notifier.is_none() {
            let sync_group = SyncGroup::new(trx_list, None, cts, log_bytes);
            let committed_trx_list = self.sync(sync_group);

            assert!(self.persisted_cts.load(Ordering::Relaxed) >= this_cts);

            // update stats
            self.stats.trx_count.fetch_add(1, Ordering::Relaxed);
            self.stats.commit_count.fetch_add(1, Ordering::Relaxed);
            self.stats.sync_count.fetch_add(1, Ordering::Relaxed);
            self.stats
                .log_bytes
                .fetch_add(log_bytes as u64, Ordering::Relaxed);

            // gc if necessary
            if let Some(gc_chan) = gc_chan {
                let _ = gc_chan.send(committed_trx_list);
            }
            return;
        }

        let (tx, rx) = follower_notifier.unwrap();
        if let Some(sync_chan) = sync_chan {
            let sync_group = SyncGroup::with_notifiers(trx_list, vec![tx], cts, log_bytes);
            let _ = sync_chan.send(sync_group);
        }
        // GC will be triggered by sync thread so we only wait for sync to be done.
        // let leader become also follower to wait.
        let _ = rx.recv_async().await;

        assert!(self.persisted_cts.load(Ordering::Relaxed) >= this_cts);
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
            self.min_active_sts.store(min_active_sts, Ordering::SeqCst);

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

    #[inline]
    fn sync(&self, sync_group: SyncGroup) -> Vec<CommittedTrx> {
        debug_assert!(!sync_group.trx_list.is_empty());

        self.syncer.sync(); // fsync here.

        // Instead of letting each thread backfill its CTS in undo logs,
        // we delegate this action to group commit leader because it's
        // a very cheap operation via Arc<AtomicU64>::store().
        for trx in &sync_group.trx_list {
            trx.trx_id.store(trx.cts, Ordering::SeqCst);
        }

        let trx_list = sync_group
            .trx_list
            .into_iter()
            .map(PrecommitTrx::commit)
            .collect();

        // update global persisted cts.
        // we have to make sure the update of cts is monotonously increasing
        // use CAS operation, because the order of fsync might vary in different threads.
        loop {
            let curr_cts = self.persisted_cts.load(Ordering::Relaxed);
            if curr_cts >= sync_group.cts {
                // other thread as leader of later group might already update this value.
                break;
            }
            if self
                .persisted_cts
                .compare_exchange_weak(
                    curr_cts,
                    sync_group.cts,
                    Ordering::SeqCst,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                break;
            }
        }

        // notify followers once sync group is dropped
        trx_list
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
    fn start_sync_thread(&'static self) {
        let (sync_chan, rx) = flume::unbounded::<SyncGroup>();

        // Pass sync channel to group commit, and get GC channel if exists.
        let gc_chan = {
            let mut group_commit_g = self.group_commit.lock();
            group_commit_g.sync_chan = Some(sync_chan);
            group_commit_g.gc_chan.clone()
        };

        // Start thread
        let handle = thread::Builder::new()
            .name("Sync-Thread".to_string())
            .spawn(move || {
                let gc_chan = gc_chan.as_ref();
                while let Ok(mut sync_group) = rx.recv() {
                    // batch groups if possible
                    while let Ok(additional) = rx.try_recv() {
                        sync_group.merge(additional);
                    }
                    let trx_count = sync_group.trx_list.len();
                    let commit_count = sync_group.commits;
                    let log_bytes = sync_group.log_bytes;

                    let committed_trx_list = self.sync(sync_group);

                    // update stats
                    self.stats
                        .trx_count
                        .fetch_add(trx_count as u64, Ordering::Relaxed);
                    self.stats
                        .commit_count
                        .fetch_add(commit_count as u64, Ordering::Relaxed);
                    self.stats.sync_count.fetch_add(1, Ordering::Relaxed);
                    self.stats
                        .log_bytes
                        .fetch_add(log_bytes as u64, Ordering::Relaxed);

                    if let Some(gc_chan) = gc_chan {
                        let _ = gc_chan.send(committed_trx_list);
                    }
                }
            })
            .unwrap();

        *self.sync_thread.lock() = Some(handle);
    }
}

unsafe impl Sync for TransactionSystem {}

impl Drop for TransactionSystem {
    #[inline]
    fn drop(&mut self) {
        // close sync channel and gc channel
        {
            let mut group_commit_g = self.group_commit.lock();
            drop(group_commit_g.sync_chan.take());
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
    }
}

pub type LoggerFactory = Box<dyn FnOnce() -> (Box<dyn RedoLogger>, Box<dyn RedoSyncer + Send>)>;

pub struct TrxSysBuilder {
    log_len_threshold: usize,
    logger_factory: LoggerFactory,
    gc: bool,
}

impl TrxSysBuilder {
    #[inline]
    pub fn log_len_threshold(mut self, log_len_threshold: usize) -> Self {
        self.log_len_threshold = log_len_threshold;
        self
    }

    #[inline]
    pub fn gc(mut self, gc: bool) -> Self {
        self.gc = gc;
        self
    }

    #[inline]
    pub fn logger_factory(mut self, logger_factory: LoggerFactory) -> Self {
        self.logger_factory = logger_factory;
        self
    }

    #[inline]
    pub fn build_static(self) -> &'static TransactionSystem {
        let (logger, syncer) = (self.logger_factory)();

        let trx_sys = TransactionSystem {
            ts: CachePadded::new(AtomicU64::new(MIN_SNAPSHOT_TS)),
            min_active_sts: CachePadded::new(AtomicU64::new(MIN_SNAPSHOT_TS)),
            group_commit: CachePadded::new(Mutex::new(GroupCommit::new(self.log_len_threshold))),
            rollback_sts_list: CachePadded::new(Mutex::new(BTreeSet::new())),
            persisted_cts: CachePadded::new(AtomicU64::new(MIN_SNAPSHOT_TS)),
            gc_info: CachePadded::new(Mutex::new(GCInfo::new())),
            logger,
            syncer,
            stats: TrxSysStats::default(),
            sync_thread: Mutex::new(None),
            gc_thread: Mutex::new(None),
        };

        let trx_sys = Box::leak(Box::new(trx_sys));
        if self.gc {
            trx_sys.start_gc_thread();
        }
        trx_sys.start_sync_thread();
        trx_sys
    }
}

impl Default for TrxSysBuilder {
    #[inline]
    fn default() -> Self {
        TrxSysBuilder {
            log_len_threshold: DEFAULT_LOG_LEN_THRESHOLD,
            logger_factory: noop_logger(),
            gc: false,
        }
    }
}

fn noop_logger() -> LoggerFactory {
    Box::new(|| (Box::new(NoopLogger), Box::new(NoopLogger)))
}

struct NoopLogger;

impl RedoLogger for NoopLogger {
    fn write(&self, cts: TrxID, redo_bin: &RedoBin) -> usize {
        0
    }

    fn flush(&self) {}
}

impl RedoSyncer for NoopLogger {
    fn sync(&self) {}
}

pub struct BufferedSingleFileLogger {
    writer: Mutex<BufWriter<File>>,
}

impl BufferedSingleFileLogger {
    #[inline]
    pub fn new(f: File, buf_size: usize) -> Self {
        let writer = BufWriter::with_capacity(buf_size, f);
        BufferedSingleFileLogger {
            writer: Mutex::new(writer),
        }
    }
}

impl RedoLogger for BufferedSingleFileLogger {
    #[inline]
    fn write(&self, _cts: TrxID, redo_bin: &RedoBin) -> usize {
        let mut writer_g = self.writer.lock();
        writer_g.write_all(&redo_bin).unwrap();
        redo_bin.len()
    }

    #[inline]
    fn flush(&self) {
        let mut writer_g = self.writer.lock();
        writer_g.flush().unwrap();
    }
}

pub struct Fsync(pub RawFd);

impl RedoSyncer for Fsync {
    #[inline]
    fn sync(&self) {
        unsafe {
            libc::fsync(self.0);
        }
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
    /// Groups of committing transactions.
    /// At most 2.
    groups: VecDeque<CommitGroup>,
    /// Channel to send committed transactions for GC.
    gc_chan: Option<Sender<Vec<CommittedTrx>>>,
    /// Channel to send group to sync thread.
    sync_chan: Option<Sender<SyncGroup>>,
    /// Controls log size of each group.
    /// If transactions has more data, a new group is established instead of
    /// joining current group.
    log_len_threshold: usize,
}

impl GroupCommit {
    #[inline]
    fn new(log_len_threshold: usize) -> Self {
        GroupCommit {
            groups: VecDeque::new(),
            gc_chan: None,
            sync_chan: None,
            log_len_threshold,
        }
    }
}

/// GroupCommit is an abstraction to group multiple transactions
/// and write and sync logs in batch mode.
/// The first transaction thread arrived in the group becomes the
/// group leader, and take care of all transactions log persistence.
/// It's used to improve logging performance.
struct CommitGroup {
    kind: CommitGroupKind,
    log_len: usize,
    follower_notifier: Option<CommitGroupNotify>,
    // notify leader of next group.
    next_leader_notifier: Option<CommitGroupNotify>,
}

impl CommitGroup {
    #[inline]
    fn add_trx(&mut self, trx: PrecommitTrx) {
        self.log_len += trx.redo_bin_len();
        self.kind.add_trx(trx);
    }
}

enum CommitGroupKind {
    Processing,
    Sequential(Vec<PrecommitTrx>),
}

impl CommitGroupKind {
    #[inline]
    fn add_trx(&mut self, trx: PrecommitTrx) {
        match self {
            CommitGroupKind::Processing => {
                unreachable!("Transaction cannot be added to processing commit group")
            }
            CommitGroupKind::Sequential(trx_list) => trx_list.push(trx),
        }
    }
}

type CommitGroupNotify = (Sender<()>, Receiver<()>);

struct SyncGroup {
    trx_list: Vec<PrecommitTrx>,
    notifiers: Vec<Sender<()>>,
    cts: TrxID,
    log_bytes: usize,
    commits: usize,
}

impl SyncGroup {
    #[inline]
    fn new(
        trx_list: Vec<PrecommitTrx>,
        notifier: Option<Sender<()>>,
        cts: TrxID,
        log_bytes: usize,
    ) -> Self {
        if let Some(notifier) = notifier {
            Self::with_notifiers(trx_list, vec![notifier], cts, log_bytes)
        } else {
            Self::with_notifiers(trx_list, vec![], cts, log_bytes)
        }
    }

    #[inline]
    fn with_notifiers(
        trx_list: Vec<PrecommitTrx>,
        notifiers: Vec<Sender<()>>,
        cts: TrxID,
        log_bytes: usize,
    ) -> Self {
        SyncGroup {
            trx_list,
            notifiers,
            cts,
            log_bytes,
            commits: 1,
        }
    }

    #[inline]
    fn merge(&mut self, other: Self) {
        self.trx_list.extend(other.trx_list);
        self.notifiers.extend(other.notifiers);
        if self.cts < other.cts {
            self.cts = other.cts;
        }
        self.log_bytes += other.log_bytes;
        self.commits += other.commits;
    }
}

#[derive(Default)]
pub struct TrxSysStats {
    pub commit_count: AtomicU64,
    pub trx_count: AtomicU64,
    pub log_bytes: AtomicU64,
    pub sync_count: AtomicU64,
}
