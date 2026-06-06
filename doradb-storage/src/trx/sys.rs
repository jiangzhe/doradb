use crate::DiskPool;
use crate::catalog::{Catalog, CatalogCheckpointScanConfig, TableCache};
use crate::component::{Component, ComponentRegistry, IndexPool, MemPool, MetaPool, ShelfScope};
use crate::conf::TrxSysConfig;
use crate::engine::EngineRef;
use crate::error::{Error, FatalError, FatalResult, InternalError, Result};
use crate::file::fs::FileSystem;
use crate::file::table_file::{MutableTableFile, OldRoot, TableFile};
use crate::id::TrxID;
use crate::quiescent::{QuiescentBox, QuiescentGuard, SyncQuiescentGuard};
use crate::session::{SessionState, TrxAttachment};
use crate::thread;
use crate::trx::group::Commit;
use crate::trx::log::{LOG_HEADER_PAGES, LogPartition};
use crate::trx::log_replay::MmapLogReader;
use crate::trx::purge::{DroppedTableFileDeleteItem, DroppedTableQueue, GC, Purge, TableRootQueue};
use crate::trx::redo::RedoLogs;
use crate::trx::sys_trx::SysTrx;
use crate::trx::{
    MAX_SNAPSHOT_TS, MIN_ACTIVE_TRX_ID, MIN_SNAPSHOT_TS, PreparedTrx, StartedTransaction,
    Transaction, TrxCleanupJob, TrxCleanupReason, TrxCompletionClaim, TrxEntry, TrxEntryState,
    TrxInner,
};
use crossbeam_utils::CachePadded;
use error_stack::Report;
use flume::{Receiver, Sender};
use parking_lot::Mutex;
use std::mem;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::thread::JoinHandle;

pub(crate) const GC_BUCKETS: usize = 64;

/// TransactionSystem controls lifecycle of all transactions.
///
/// 1. Transaction begin:
///    a) Generate STS and TrxID.
///    b) Put it into active transaction list.
///
/// 2. Transaction pre-commmit:
///    a) Generate CTS.
///    b) Put it into precommit transaction list.
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
/// TrxID in all undo log entries of current transaction should be updated to CTS after the
/// transaction reaches its ordered commit barrier. Durability-required transactions reach that
/// barrier after redo persistence; no-log runtime-effect transactions reach it through the same
/// group ordering without publishing a recovery-visible timestamp carrier.
/// As undo logs are maintained purely in memory, we can use shared pointer with atomic variable
/// to perform very fast CTS backfill.
pub(crate) struct TransactionSystem {
    /// A sequence to generate snapshot timestamp(abbr. sts) and commit timestamp(abbr. cts).
    /// They share the same sequence and start from 1.
    /// The two timestamps are used to identify which version of data a transaction should see.
    /// Transaction id is derived from snapshot timestamp with highest bit set to 1.
    ///
    /// trx_id range: (1<<63)+1 to uint::MAX-1
    /// sts range: 1 to 1<<63
    /// cts range: 1 to 1<<63
    pub(super) ts: CachePadded<AtomicU64>,
    /// Global visible snapshot timestamp.
    /// It's updated by query/GC thread after cleaning out-of-date version chains.
    ///
    /// Data associated with smaller timestamp will be always visible to all transactions.
    global_visible_sts: CachePadded<AtomicU64>,
    /// Round-robin partition id generator.
    rr_partition_id: CachePadded<AtomicUsize>,
    /// Multiple log partitions.
    pub(super) log_partitions: CachePadded<Box<[CachePadded<LogPartition>]>>,
    /// Transaction system configuration.
    pub(super) config: CachePadded<TrxSysConfig>,
    /// Catalog of the database.
    pub(crate) catalog: CachePadded<QuiescentGuard<Catalog>>,
    /// Table file facade used by background dropped-table cleanup.
    pub(super) table_fs: CachePadded<QuiescentGuard<FileSystem>>,
    /// Wakeup channel for purge coordination.
    pub(super) purge_tx: CachePadded<Sender<Purge>>,
    /// Best-effort abandoned transaction cleanup queue.
    cleanup_tx: CachePadded<Sender<TrxCleanupMessage>>,
    /// Swapped table roots retained until post-publish readers drain.
    pub(super) table_roots: CachePadded<Mutex<TableRootQueue>>,
    /// Dropped table runtime and file cleanup queues.
    pub(super) dropped_tables: CachePadded<Mutex<DroppedTableQueue>>,
    /// Storage-runtime poison flag for fatal storage background or durability failures.
    storage_poisoned: CachePadded<AtomicBool>,
    /// First fatal storage reason that poisoned runtime admission.
    storage_poison_err: CachePadded<Mutex<Option<FatalError>>>,
}

pub(crate) struct TransactionSystemWorkers;

pub(crate) struct TransactionSystemWorkersOwned {
    pub(crate) trx_sys: SyncQuiescentGuard<TransactionSystem>,
    pub(crate) purge_tx: Sender<Purge>,
    pub(crate) cleanup_tx: Sender<TrxCleanupMessage>,
    pub(crate) io_threads: Mutex<Vec<JoinHandle<()>>>,
    pub(crate) gc_threads: Mutex<Vec<JoinHandle<()>>>,
    pub(crate) purge_threads: Mutex<Vec<JoinHandle<()>>>,
    pub(crate) cleanup_thread: Mutex<Option<JoinHandle<()>>>,
    pub(crate) shutdown_started: AtomicBool,
}

pub(crate) enum TrxCleanupMessage {
    Job(TrxCleanupJob),
    Stop,
}

pub(crate) struct TransactionSystemQueues {
    pub(crate) purge_tx: Sender<Purge>,
    pub(crate) cleanup_tx: Sender<TrxCleanupMessage>,
}

impl TransactionSystem {
    #[inline]
    pub(crate) fn new(
        config: TrxSysConfig,
        catalog: QuiescentGuard<Catalog>,
        table_fs: QuiescentGuard<FileSystem>,
        log_partitions: Vec<CachePadded<LogPartition>>,
        initial_ts: TrxID,
        queues: TransactionSystemQueues,
        initial_file_deletes: Vec<DroppedTableFileDeleteItem>,
    ) -> Self {
        debug_assert!((MIN_SNAPSHOT_TS..MAX_SNAPSHOT_TS).contains(&initial_ts));
        TransactionSystem {
            ts: CachePadded::new(AtomicU64::new(initial_ts.as_u64())),
            global_visible_sts: CachePadded::new(AtomicU64::new(initial_ts.as_u64())),
            rr_partition_id: CachePadded::new(AtomicUsize::new(0)),
            log_partitions: CachePadded::new(log_partitions.into_boxed_slice()),
            config: CachePadded::new(config),
            catalog: CachePadded::new(catalog),
            table_fs: CachePadded::new(table_fs),
            purge_tx: CachePadded::new(queues.purge_tx),
            cleanup_tx: CachePadded::new(queues.cleanup_tx),
            table_roots: CachePadded::new(Mutex::new(TableRootQueue::default())),
            dropped_tables: CachePadded::new(Mutex::new(DroppedTableQueue::from_file_deletes(
                initial_file_deletes,
            ))),
            storage_poisoned: CachePadded::new(AtomicBool::new(false)),
            storage_poison_err: CachePadded::new(Mutex::new(None)),
        }
    }

    /// Returns the first fatal storage poison error, if runtime admission has been poisoned.
    ///
    /// Poison is an admission barrier, not a shutdown mechanism. It prevents new
    /// foreground/system work from entering paths that depend on durable
    /// consistency, while the engine owner remains responsible for the normal
    /// explicit shutdown sequence.
    #[inline]
    pub(crate) fn storage_poison_error(&self) -> Option<Report<FatalError>> {
        if !self.storage_poisoned.load(Ordering::Acquire) {
            return None;
        }
        let guard = self.storage_poison_err.lock();
        debug_assert!(
            guard.is_some(),
            "storage poison flag published before poison error was recorded"
        );
        guard.as_ref().copied().map(Report::new)
    }

    /// Returns `Err` once a fatal storage failure poisoned runtime admission.
    ///
    /// Call this at work-admission boundaries. Background worker shutdown must
    /// not call it, because shutdown must remain available after the runtime has
    /// already been poisoned.
    #[inline]
    pub(crate) fn ensure_runtime_healthy(&self) -> FatalResult<()> {
        match self.storage_poison_error() {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    /// Records the first fatal storage poison reason and returns a fresh poison error.
    ///
    /// The first caller wins: later poison attempts keep returning the already
    /// recorded reason. The reason is stored before the atomic flag is published
    /// so a thread that observes `storage_poisoned == true` can immediately load
    /// a meaningful error. This method intentionally does not wake or stop worker
    /// threads; callers that hit an unrecoverable background failure should
    /// return from their worker loop after poisoning.
    #[inline]
    pub(crate) fn poison_storage(&self, reason: FatalError) -> Report<FatalError> {
        {
            let mut guard = self.storage_poison_err.lock();
            if guard.is_none() {
                *guard = Some(reason);
            }
        }
        self.storage_poisoned.swap(true, Ordering::AcqRel);
        self.storage_poison_error().unwrap_or_else(|| {
            Report::new(FatalError::Poisoned).attach(format!("poisoned_by={reason}"))
        })
    }

    /// Mark a user-table root publication effective and retain the swapped root.
    ///
    /// The fence timestamp is allocated after the active-root pointer has
    /// already been swapped. It is installed on the new active root as the
    /// runtime-only reclamation boundary and also attached to the replaced root
    /// guard. Any transaction that could have observed the old root must have
    /// started before this fence, so purge can release the root once the global
    /// active-snapshot horizon crosses it.
    #[inline]
    fn mark_published_table_root(
        &self,
        table_file: &Arc<TableFile>,
        old_root: Option<OldRoot>,
    ) -> TrxID {
        let effective_ts = TrxID::new(self.ts.fetch_add(1, Ordering::SeqCst));
        debug_assert!(effective_ts < MAX_SNAPSHOT_TS);
        table_file.install_active_root_effective_ts(effective_ts);
        if let Some(old_root) = old_root {
            self.table_roots
                .lock()
                .push_retained(effective_ts, old_root);
            self.request_table_root_retention_purge();
        }
        effective_ts
    }

    /// Publish one user-table file root and install its runtime effective fence.
    #[inline]
    pub(crate) async fn publish_table_file_root(
        &self,
        mutable_file: MutableTableFile,
        root_ts: TrxID,
        try_delete_if_fail: bool,
    ) -> Result<Arc<TableFile>> {
        let (table_file, old_root) = mutable_file.commit(root_ts, try_delete_if_fail).await?;
        self.mark_published_table_root(&table_file, old_root);
        Ok(table_file)
    }

    /// Create a new transaction.
    #[inline]
    pub(crate) fn begin_trx(
        &self,
        engine: EngineRef,
        session_state: &Arc<SessionState>,
    ) -> StartedTransaction {
        // Assign log partition index so current transaction will stick
        // to certain log partition for commit.
        let log_no = self.next_log_no();
        let partition = &*self.log_partitions[log_no];
        let gc_no = partition.next_gc_no();
        let gc_bucket = &partition.gc_buckets[gc_no];
        // Add to active sts list.
        let mut g = gc_bucket.active_sts_list.lock();
        // With bucket lock, we can make sure all transactions are ordered by STS.
        let sts = TrxID::new(self.ts.fetch_add(1, Ordering::SeqCst));
        let trx_id = TrxID::new(sts.as_u64() | (1 << 63));
        debug_assert!(sts < MAX_SNAPSHOT_TS);
        debug_assert!(trx_id >= MIN_ACTIVE_TRX_ID);
        g.insert(sts);
        if g.len() == 1 {
            // Only when the previous list is empty, we should update min_active_sts
            // as STS of current transaction.
            // In this case, current value of min_active_sts should be MAX.
            debug_assert!(
                TrxID::new(gc_bucket.min_active_sts.load(Ordering::Relaxed)) == MAX_SNAPSHOT_TS
            );
            gc_bucket
                .min_active_sts
                .store(sts.as_u64(), Ordering::Relaxed);
        }
        drop(g); // release bucket lock.
        let inner = TrxInner::new(trx_id, sts, log_no, gc_no, session_state.id());
        let entry = TrxEntry::new(inner);
        let handle = Transaction::new(engine.downgrade(), session_state.id(), trx_id, sts);
        StartedTransaction { handle, entry }
    }

    /// Returns next log(partition) number.
    #[inline]
    fn next_log_no(&self) -> usize {
        if self.config.log_partitions == 1 {
            0
        } else {
            self.rr_partition_id.fetch_add(1, Ordering::Relaxed) % self.config.log_partitions
        }
    }

    #[inline]
    pub(crate) fn begin_sys_trx(&self) -> SysTrx {
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
    pub(crate) async fn commit_transaction(&self, claim: TrxCompletionClaim) -> Result<TrxID> {
        if let Err(err) = self.ensure_runtime_healthy() {
            self.rollback_claim(claim, "rollback poisoned commit")
                .await?;
            return Err(err.into());
        }
        // Prepare redo log first, this may take some time,
        // so keep it out of lock scope, and we can fill cts after the lock is held.
        let (_entry, inner, attachment) = claim.into_parts();
        inner.debug_assert_redo_invariants();
        let partition = &*self.log_partitions[inner.log_no()];
        let prepared_trx = inner.prepare(attachment)?;
        if !prepared_trx.require_ordered_commit() {
            // No runtime effects means there is no CTS to publish and no commit
            // order to preserve. Effects without redo still enter group commit
            // because readers, sessions, and GC depend on ordered CTS backfill.
            self.discard_unordered_prepared(prepared_trx);
            return Ok(TrxID::new(0));
        }
        // start group commit
        partition.commit(prepared_trx, &self.ts, true).await
    }

    #[inline]
    pub(crate) fn commit_sys(&self, trx: SysTrx) -> Result<TrxID> {
        self.ensure_runtime_healthy()?;
        if trx.redo.is_empty() {
            // System transaction does not hold any active start timestamp
            // so we can just drop it if there is no change to replay.
            return Ok(TrxID::new(0));
        }
        // System transactions are always submitted to first log partition and
        // use the no-wait piggyback flow intentionally. They publish internal
        // state that becomes globally visible once the redo group commits, and
        // they are not modeled as session-bound transactions that can later be
        // rolled back through the normal user transaction API.
        const LOG_NO: usize = 0;
        let partition = &*self.log_partitions[LOG_NO];
        let prepared_trx = trx.prepare();
        partition.commit_no_wait(prepared_trx, &self.ts)
    }

    /// Rollback active transaction.
    #[inline]
    pub(crate) async fn rollback_transaction(&self, claim: TrxCompletionClaim) -> Result<()> {
        self.rollback_claim(claim, "rollback active transaction")
            .await
    }

    /// Rollback an abandoned transaction claimed by cleanup.
    #[inline]
    pub(crate) async fn cleanup_abandoned_transaction(
        &self,
        claim: TrxCompletionClaim,
    ) -> Result<()> {
        self.rollback_claim(claim, "cleanup abandoned transaction")
            .await
    }

    #[inline]
    async fn rollback_claim(
        &self,
        claim: TrxCompletionClaim,
        operation: &'static str,
    ) -> Result<()> {
        let (entry, mut inner, attachment) = claim.into_parts();
        self.rollback_inner(entry.as_ref(), &mut inner, &attachment, operation)
            .await
    }

    #[inline]
    async fn rollback_inner(
        &self,
        entry: &TrxEntry,
        inner: &mut TrxInner,
        attachment: &TrxAttachment,
        _operation: &'static str,
    ) -> Result<()> {
        let sts = inner.sts();
        let log_no = inner.log_no();
        let gc_no = inner.gc_no();
        let pool_guards = attachment.pool_guards().clone();
        let mut table_cache = TableCache::new(&self.catalog);
        if inner
            .index_undo_mut()
            .rollback(&mut table_cache, &pool_guards, sts)
            .await
            .is_err()
        {
            entry.publish_state(TrxEntryState::Failed);
            inner.discard_after_fatal_rollback(attachment);
            entry.finish(TrxEntryState::Failed);
            return Err(self.poison_storage(FatalError::RollbackAccess).into());
        }
        if inner
            .row_undo_mut()
            .rollback(&mut table_cache, &pool_guards, sts)
            .await
            .is_err()
        {
            entry.publish_state(TrxEntryState::Failed);
            inner.discard_after_fatal_rollback(attachment);
            entry.finish(TrxEntryState::Failed);
            return Err(self.poison_storage(FatalError::RollbackAccess).into());
        }
        inner.effects_mut().clear_for_rollback();
        self.log_partitions[log_no].gc_buckets[gc_no].gc_analyze_rollback(sts);
        inner.release_transaction_locks(attachment);
        inner.finish_session_rollback(attachment);
        entry.finish(TrxEntryState::Terminal);
        Ok(())
    }

    /// Discard a prepared transaction that has no runtime effects.
    ///
    /// This path does not assign a CTS. Transactions with volatile effects but
    /// no log still require ordered commit and must not route here.
    #[inline]
    fn discard_unordered_prepared(&self, mut trx: PreparedTrx) {
        debug_assert!(!trx.require_durability());
        debug_assert!(!trx.require_ordered_commit());
        let payload = trx
            .payload
            .take()
            .expect("prepared no-op cleanup requires user transaction payload");
        self.log_partitions[payload.log_no].gc_buckets[payload.gc_no]
            .gc_analyze_rollback(payload.sts);
        if let Some(s) = trx.attachment.take() {
            s.rollback();
        }
        trx.release_transaction_locks();
    }

    /// Returns statistics of group commit.
    #[inline]
    #[cfg_attr(not(test), expect(dead_code, reason = "internal trx sys stats"))]
    pub(crate) fn trx_sys_stats(&self) -> TrxSysStats {
        let mut stats = TrxSysStats::default();
        for partition in &*self.log_partitions {
            stats.trx_count += partition.stats.trx_count.load(Ordering::Relaxed);
            stats.commit_count += partition.stats.commit_count.load(Ordering::Relaxed);
            stats.log_bytes += partition.stats.log_bytes.load(Ordering::Relaxed);
            stats.sync_count += partition.stats.sync_count.load(Ordering::Relaxed);
            stats.sync_nanos += partition.stats.sync_nanos.load(Ordering::Relaxed);
            let io_stats = partition.io_backend_stats();
            stats.io_submit_and_wait_count += io_stats.submit_and_wait_calls;
            stats.io_submit_and_wait_nanos += io_stats.submit_and_wait_nanos;
            stats.purge_trx_count += partition.stats.purge_trx_count.load(Ordering::Relaxed);
            stats.purge_row_count += partition.stats.purge_row_count.load(Ordering::Relaxed);
            stats.purge_index_count += partition.stats.purge_index_count.load(Ordering::Relaxed);
        }
        stats
    }

    /// Returns global visible snapshot timestamp.
    #[inline]
    pub(crate) fn global_visible_sts(&self) -> TrxID {
        TrxID::new(self.global_visible_sts.load(Ordering::Relaxed))
    }

    /// Update global visible snapshot timestamp.
    #[inline]
    pub(crate) fn update_global_visible_sts(&self, sts: TrxID) {
        debug_assert!({
            let curr_sts = TrxID::new(self.global_visible_sts.load(Ordering::Relaxed));
            sts >= curr_sts
        });
        self.global_visible_sts
            .store(sts.as_u64(), Ordering::SeqCst)
    }

    /// Start background GC threads.
    #[inline]
    pub(crate) fn start_gc_threads(
        trx_sys: QuiescentGuard<Self>,
        gc_rxs: Vec<Receiver<GC>>,
        purge_chan: Sender<Purge>,
    ) -> Vec<JoinHandle<()>> {
        let trx_sys = trx_sys.into_sync();
        let mut handles = Vec::with_capacity(gc_rxs.len());
        for (idx, gc_rx) in gc_rxs.into_iter().enumerate() {
            let thread_name = format!("GC-Thread-{idx}");
            let task_trx_sys = trx_sys.clone();
            let task_purge_chan = purge_chan.clone();
            let handle = thread::spawn_named(thread_name, move || {
                let partition = &*task_trx_sys.log_partitions[idx];
                partition.gc_loop(gc_rx, task_purge_chan);
            });
            handles.push(handle);
        }
        handles
    }

    /// Start background IO threads.
    #[inline]
    pub(crate) fn start_io_threads(trx_sys: QuiescentGuard<Self>) -> Vec<JoinHandle<()>> {
        let trx_sys = trx_sys.into_sync();
        let mut handles = Vec::with_capacity(trx_sys.log_partitions.len());
        for idx in 0..trx_sys.log_partitions.len() {
            let thread_name = format!("IO-Thread-{idx}");
            let task_trx_sys = trx_sys.clone();
            let handle = thread::spawn_named(thread_name, move || {
                let partition = &*task_trx_sys.log_partitions[idx];
                partition.io_loop(&task_trx_sys, &task_trx_sys.config);
            });
            handles.push(handle);
        }
        handles
    }

    /// Start the abandoned transaction cleanup worker.
    #[inline]
    pub(crate) fn start_cleanup_thread(cleanup_rx: Receiver<TrxCleanupMessage>) -> JoinHandle<()> {
        thread::spawn_named("Trx-Cleanup-Thread", move || {
            while let Ok(message) = cleanup_rx.recv() {
                match message {
                    TrxCleanupMessage::Job(job) => run_trx_cleanup_job(job),
                    TrxCleanupMessage::Stop => return,
                }
            }
        })
    }

    /// Queue abandoned transaction rollback cleanup.
    #[inline]
    pub(crate) fn request_abandoned_trx_cleanup(
        &self,
        engine: EngineRef,
        session_id: crate::id::SessionID,
        trx_id: TrxID,
        reason: TrxCleanupReason,
    ) {
        let _ = self.cleanup_tx.send(TrxCleanupMessage::Job(TrxCleanupJob {
            engine,
            session_id,
            trx_id,
            reason,
        }));
    }

    #[inline]
    #[cfg_attr(not(test), expect(dead_code, reason = "reserved log_reader"))]
    pub(crate) fn log_reader(&self, log_file_path: impl AsRef<Path>) -> Result<MmapLogReader> {
        MmapLogReader::new(
            log_file_path,
            self.config.max_io_size.as_u64() as usize,
            self.config.log_file_max_size.as_u64() as usize,
            self.config.max_io_size.as_u64() as usize * LOG_HEADER_PAGES,
        )
    }

    /// Returns the global ordered-completion watermark `W` from all partitions.
    ///
    /// This is used as the upper bound for scanning durable redo. No-log
    /// commit barriers can advance this runtime watermark, but recovery still
    /// seeds future timestamps only from checkpoint metadata, table roots, and
    /// real redo headers.
    #[inline]
    pub(crate) fn persisted_watermark_cts(&self) -> TrxID {
        self.log_partitions
            .iter()
            .map(|partition| TrxID::new(partition.persisted_cts.load(Ordering::Acquire)))
            .min()
            .unwrap_or(MIN_SNAPSHOT_TS)
    }

    #[inline]
    pub(crate) fn catalog_checkpoint_scan_config(&self) -> Result<CatalogCheckpointScanConfig> {
        Ok(CatalogCheckpointScanConfig {
            file_prefix: self.config.file_prefix()?,
            log_partitions: self.config.log_partitions,
            io_depth_per_log: self.config.io_depth_per_log,
            log_file_max_size: self.config.log_file_max_size.as_u64() as usize,
            max_io_size: self.config.max_io_size.as_u64() as usize,
        })
    }
}

#[derive(Default)]
pub(crate) struct TrxSysStats {
    pub(crate) commit_count: usize,
    pub(crate) trx_count: usize,
    pub(crate) log_bytes: usize,
    pub(crate) sync_count: usize,
    pub(crate) sync_nanos: usize,
    /// Number of backend submit-or-wait calls observed across redo workers.
    ///
    /// On `libaio`, one logical IO commonly contributes separate submit and
    /// wait syscalls, so this count can be roughly doubled compared with
    /// `io_uring` for serialized workloads.
    pub(crate) io_submit_and_wait_count: usize,
    /// Total non-overlapping nanoseconds spent in backend submit-or-wait calls.
    pub(crate) io_submit_and_wait_nanos: usize,
    pub(crate) purge_trx_count: usize,
    pub(crate) purge_row_count: usize,
    pub(crate) purge_index_count: usize,
}

#[inline]
fn run_trx_cleanup_job(job: TrxCleanupJob) {
    let TrxCleanupJob {
        engine,
        session_id,
        trx_id,
        reason,
    } = job;
    let operation = match reason {
        TrxCleanupReason::HandleDrop => "cleanup dropped transaction handle",
        TrxCleanupReason::ShutdownDrain => "cleanup shutdown abandoned transaction",
    };
    let (entry, session) = match engine
        .session_registry
        .resolve_trx(session_id, trx_id, operation)
    {
        Ok(parts) => parts,
        Err(_) => return,
    };
    match entry.inspect_state() {
        TrxEntryState::Abandoned => {
            let trx_sys = engine.trx_sys.clone();
            let attachment = TrxAttachment::new(engine, session, trx_id);
            let claim = match TrxCompletionClaim::cleanup(entry, attachment, operation) {
                Ok(claim) => claim,
                Err(_) => return,
            };
            let _ = smol::block_on(trx_sys.cleanup_abandoned_transaction(claim));
        }
        TrxEntryState::CheckedOutAbandoned => {
            std::thread::sleep(std::time::Duration::from_millis(1));
            let trx_sys = engine.trx_sys.clone();
            trx_sys.request_abandoned_trx_cleanup(engine, session_id, trx_id, reason);
        }
        TrxEntryState::Active
        | TrxEntryState::CheckedOut
        | TrxEntryState::Committing
        | TrxEntryState::RollingBack
        | TrxEntryState::CleanupRunning
        | TrxEntryState::Terminal
        | TrxEntryState::Failed => {}
    }
}

impl Component for TransactionSystem {
    type Config = TrxSysConfig;
    type Owned = Self;
    type Access = QuiescentGuard<Self>;

    const NAME: &'static str = "trx_sys";

    #[inline]
    async fn build(
        config: Self::Config,
        registry: &mut ComponentRegistry,
        mut shelf: ShelfScope<'_, Self>,
    ) -> Result<()> {
        let meta_pool = registry.dependency::<MetaPool>()?;
        let index_pool = registry.dependency::<IndexPool>()?;
        let mem_pool = registry.dependency::<MemPool>()?;
        let table_fs = registry.dependency::<FileSystem>()?;
        let disk_pool = registry.dependency::<DiskPool>()?;
        let catalog = registry.dependency::<Catalog>()?;

        let (trx_sys, startup) = config
            .prepare(
                meta_pool.clone_inner(),
                index_pool.clone_inner(),
                mem_pool.clone_inner(),
                table_fs,
                disk_pool.clone_inner(),
                catalog,
            )
            .await?;
        registry.register::<Self>(trx_sys)?;
        shelf.put::<TransactionSystemWorkers>(startup)?;
        TransactionSystemWorkers::build((), registry, shelf.scope::<TransactionSystemWorkers>())
            .await
    }

    #[inline]
    fn access(owner: &QuiescentBox<Self::Owned>) -> Self::Access {
        owner.guard()
    }

    #[inline]
    fn shutdown(_component: &Self::Owned) {}
}

impl Component for TransactionSystemWorkers {
    type Config = ();
    type Owned = TransactionSystemWorkersOwned;
    type Access = ();

    const NAME: &'static str = "trx_sys_workers";

    #[inline]
    async fn build(
        _config: Self::Config,
        registry: &mut ComponentRegistry,
        mut shelf: ShelfScope<'_, Self>,
    ) -> Result<()> {
        let trx_sys = registry.dependency::<TransactionSystem>()?;
        let startup = shelf.take::<TransactionSystem>().ok_or_else(|| {
            Error::from(
                Report::new(InternalError::ComponentProvisionMissing)
                    .attach("provider=TransactionSystem, consumer=TransactionRuntime"),
            )
        })?;
        registry.register::<Self>(startup.start(trx_sys))
    }

    #[inline]
    fn access(_owner: &QuiescentBox<Self::Owned>) -> Self::Access {}

    #[inline]
    fn shutdown(component: &Self::Owned) {
        if component.shutdown_started.swap(true, Ordering::AcqRel) {
            return;
        }

        // Shutdown is independent from storage poison. A poisoned runtime may
        // have already caused one worker to exit early, but owner-side teardown
        // still has to signal every channel and join every worker handle.
        let log_partitions = &*component.trx_sys.log_partitions;
        for partition in log_partitions {
            {
                let mut group_commit_g = partition.group_commit.lock();
                group_commit_g.queue.push_back(Commit::Shutdown);
                if group_commit_g.queue.len() == 1 {
                    partition.group_commit.notify_one();
                }
            }
            let _ = partition.gc_chan.send(GC::Stop);
        }

        let io_threads = { mem::take(&mut *component.io_threads.lock()) };
        for handle in io_threads {
            handle.join().unwrap();
        }
        let gc_threads = { mem::take(&mut *component.gc_threads.lock()) };
        for handle in gc_threads {
            handle.join().unwrap();
        }

        let _ = component.cleanup_tx.send(TrxCleanupMessage::Stop);
        let cleanup_thread = { component.cleanup_thread.lock().take() };
        if let Some(handle) = cleanup_thread {
            handle.join().unwrap();
        }

        let _ = component.purge_tx.send(Purge::Stop);
        let purge_threads = { mem::take(&mut *component.purge_threads.lock()) };
        for handle in purge_threads {
            handle.join().unwrap();
        }

        for partition in log_partitions {
            let mut group_commit_g = partition.group_commit.lock();
            let Some(log_file) = group_commit_g.log_file.take() else {
                continue;
            };
            drop(log_file);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::PoolRole;
    use crate::catalog::tests::table2;
    use crate::conf::{EngineConfig, EvictableBufferPoolConfig};
    use crate::value::Val;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Barrier};
    use std::time::Duration;
    use tempfile::TempDir;

    #[test]
    fn test_transaction_system() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = EngineConfig::default()
                .storage_root(main_dir)
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .role(PoolRole::Mem)
                        .max_mem_size(128usize * 1024 * 1024)
                        .max_file_size(256usize * 1024 * 1024),
                )
                .trx(TrxSysConfig::default().log_file_stem("redo_trx"))
                .build()
                .await
                .unwrap();
            let mut session = engine.new_session().unwrap();
            {
                let trx = session.begin_trx().unwrap();
                let _ = smol::block_on(trx.commit());
            }
            {
                let engine = engine.new_ref().unwrap();
                std::thread::spawn(move || {
                    let mut session = engine.new_session().unwrap();
                    let trx = session.begin_trx().unwrap();
                    let _ = smol::block_on(trx.commit());
                })
                .join()
                .unwrap();
            }

            drop(session);
            drop(engine);
        })
    }

    #[test]
    fn test_poison_storage_records_error_before_publishing_flag() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = EngineConfig::default()
                .storage_root(main_dir)
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .role(PoolRole::Mem)
                        .max_mem_size(128usize * 1024 * 1024)
                        .max_file_size(256usize * 1024 * 1024),
                )
                .trx(TrxSysConfig::default().log_file_stem("redo_poison_lock_order"))
                .build()
                .await
                .unwrap();

            let trx_sys = engine.trx_sys.clone();
            let blocked = trx_sys.storage_poison_err.lock();
            let started = Arc::new(AtomicBool::new(false));
            let finished = Arc::new(AtomicBool::new(false));

            let worker_started = Arc::clone(&started);
            let worker_finished = Arc::clone(&finished);
            let worker_trx_sys = trx_sys.clone();
            let handle = std::thread::spawn(move || {
                worker_started.store(true, Ordering::Release);
                let err = worker_trx_sys.poison_storage(FatalError::RedoSubmit);
                worker_finished.store(true, Ordering::Release);
                err
            });

            while !started.load(Ordering::Acquire) {
                std::thread::yield_now();
            }
            for _ in 0..20 {
                assert!(
                    !trx_sys.storage_poisoned.load(Ordering::Acquire),
                    "poison flag must not publish before poison error is recorded"
                );
                assert!(
                    !finished.load(Ordering::Acquire),
                    "poison call should remain blocked while poison error lock is held"
                );
                std::thread::sleep(Duration::from_millis(1));
            }
            assert!(trx_sys.storage_poison_error().is_none());

            drop(blocked);

            let err = handle.join().unwrap();
            assert_eq!(*err.current_context(), FatalError::RedoSubmit);
            assert!(trx_sys.storage_poisoned.load(Ordering::Acquire));
            assert!(
                trx_sys
                    .storage_poison_error()
                    .as_ref()
                    .is_some_and(|err| { *err.current_context() == FatalError::RedoSubmit })
            );
            assert!(
                trx_sys
                    .ensure_runtime_healthy()
                    .as_ref()
                    .is_err_and(|err| { *err.current_context() == FatalError::RedoSubmit })
            );

            drop(trx_sys);
            drop(engine);
        });
    }

    #[test]
    fn test_poison_storage_concurrent_callers_share_first_error() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = EngineConfig::default()
                .storage_root(main_dir)
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .role(PoolRole::Mem)
                        .max_mem_size(128usize * 1024 * 1024)
                        .max_file_size(256usize * 1024 * 1024),
                )
                .trx(TrxSysConfig::default().log_file_stem("redo_poison_concurrent"))
                .build()
                .await
                .unwrap();

            let trx_sys = engine.trx_sys.clone();
            let barrier = Arc::new(Barrier::new(3));

            let worker_a_barrier = Arc::clone(&barrier);
            let worker_a_trx_sys = trx_sys.clone();
            let worker_a = std::thread::spawn(move || {
                worker_a_barrier.wait();
                worker_a_trx_sys.poison_storage(FatalError::RedoSubmit)
            });

            let worker_b_barrier = Arc::clone(&barrier);
            let worker_b_trx_sys = trx_sys.clone();
            let worker_b = std::thread::spawn(move || {
                worker_b_barrier.wait();
                worker_b_trx_sys.poison_storage(FatalError::RedoSync)
            });

            barrier.wait();

            let err_a = worker_a.join().unwrap();
            let err_b = worker_b.join().unwrap();
            let stored = trx_sys.storage_poison_error().unwrap();
            let err_a_reason = *err_a.current_context();
            let err_b_reason = *err_b.current_context();
            let stored_reason = *stored.current_context();

            assert!(trx_sys.storage_poisoned.load(Ordering::Acquire));
            assert_eq!(err_a_reason, err_b_reason);
            assert_eq!(stored_reason, err_a_reason);
            assert!(
                trx_sys
                    .ensure_runtime_healthy()
                    .as_ref()
                    .is_err_and(|err| *err.current_context() == stored_reason)
            );
            assert!(matches!(
                stored_reason,
                FatalError::RedoSubmit | FatalError::RedoSync
            ));

            drop(trx_sys);
            drop(engine);
        });
    }

    #[test]
    fn test_log_rotate() {
        // 2000 rows, 200 bytes each row, 4M log file size.
        // log file is 1MB, so it will rotate at least 4 times.
        // Due to alignment of direct IO, the write amplification might
        // be higher and produce more files.
        const COUNT: usize = 2000;
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = EngineConfig::default()
                .storage_root(main_dir)
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .role(PoolRole::Mem)
                        .max_mem_size(128usize * 1024 * 1024)
                        .max_file_size(256usize * 1024 * 1024),
                )
                .trx(
                    TrxSysConfig::default()
                        .log_partitions(1)
                        .log_file_stem("redo_rotate")
                        .log_file_max_size(1024u64 * 1024),
                )
                .build()
                .await
                .unwrap();
            let table_id = table2(&engine).await;
            let table = engine.catalog().get_table(table_id).await.unwrap();

            let mut session = engine.new_session().unwrap();
            let s = [1u8; 196];
            for i in 0..COUNT {
                let mut trx = session.begin_trx().unwrap();
                trx.exec(async |stmt| {
                    let insert = vec![Val::from(i as i32), Val::from(&s[..])];
                    stmt.table_insert_mvcc(&table, insert).await?;
                    Ok(())
                })
                .await
                .unwrap();
                trx.commit().await.unwrap();
            }
            drop(session);
            drop(table);
            drop(engine);
        });
    }
}
