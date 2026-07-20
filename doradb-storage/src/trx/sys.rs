use crate::DiskPool;
use crate::buffer::PoolGuards;
use crate::catalog::{Catalog, CatalogCheckpointScanConfig, TableCache};
use crate::component::{
    Component, ComponentRegistry, EnginePools, IndexPool, MemPool, MetaPool, ShelfScope, Supplier,
};
use crate::conf::TrxSysConfig;
use crate::engine::EngineRef;
use crate::error::{
    CompletionErrorBridge, DataIntegrityError, Error, FatalError, FatalResult, InternalError,
    Result, RuntimeResult,
};
use crate::file::fs::FileSystem;
use crate::file::table_file::{MutableTableFile, OldRoot, TableFile};
use crate::id::{SessionID, TrxID};
use crate::io::Completion;
use crate::log::redo::RedoLogs;
use crate::log::{EnqueuePrecommitError, LogFileSealer, LogWriteDriver, RedoLog, RedoLogWriter};
use crate::notify::MonotonicU64;
use crate::obs;
use crate::poison::EnginePoisoner;
use crate::quiescent::{QuiescentBox, QuiescentGuard, SyncQuiescentGuard};
use crate::recovery::RecoveryResources;
use crate::recovery::stream::CatalogSafeRedoSegment;
use crate::runtime;
use crate::session::{SessionState, TrxAttachment};
use crate::thread;
use crate::trx::group::{Commit, CommitJoin, GroupCommit};
#[cfg(test)]
use crate::trx::purge::PurgeTestHook;
use crate::trx::purge::{DroppedTableFileCleanupQueue, GCBucket, Purge, TableRootQueue};
use crate::trx::sys_trx::SysTrx;
use crate::trx::{
    FailedPrecommitCleanupJob, FailedPrecommitReason, FatalRollbackRetention, MAX_COMMIT_TS,
    MAX_SNAPSHOT_TS, MIN_ACTIVE_TRX_ID, MIN_SNAPSHOT_TS, PrecommitTrx, PreparedTrx,
    PreparedTrxPayload, StartedTransaction, Transaction, TrxCleanupJob, TrxCleanupReason,
    TrxCompletionClaim, TrxEntry, TrxEntryState, TrxInner,
};
use crossbeam_utils::CachePadded;
use either::Either::{Left, Right};
use error_stack::{Report, ResultExt};
use event_listener::{Event, EventListener, listener};
use flume::{Receiver, Sender};
use parking_lot::{Mutex, MutexGuard};
use std::collections::BTreeMap;
use std::mem::{forget, take};
use std::panic::resume_unwind;
use std::result::Result as StdResult;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::thread::JoinHandle;

/// In-memory catalog-safe redo segment progress from a published catalog checkpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CatalogRedoRetentionProgress {
    /// First retained redo file sequence from the durable catalog marker.
    pub(crate) first_retained_file_seq: u32,
    /// Catalog replay boundary published by the checkpoint.
    pub(crate) catalog_replay_start_ts: TrxID,
    /// Sealed retained segments proven safe for catalog recovery below the boundary.
    pub(crate) segments: Vec<CatalogSafeRedoSegment>,
}

impl CatalogRedoRetentionProgress {
    #[inline]
    fn merge_equal_boundary(&mut self, progress: Self) {
        debug_assert_eq!(
            self.catalog_replay_start_ts,
            progress.catalog_replay_start_ts
        );
        self.first_retained_file_seq = self
            .first_retained_file_seq
            .max(progress.first_retained_file_seq);
        let mut segments = BTreeMap::new();
        for segment in take(&mut self.segments)
            .into_iter()
            .chain(progress.segments)
            .filter(|segment| segment.file_seq >= self.first_retained_file_seq)
        {
            segments.insert(segment.file_seq, segment);
        }
        self.segments = segments.into_values().collect();
    }
}

#[derive(Debug, Default)]
struct RedoRetentionGateState {
    active: bool,
}

/// Transaction-system-wide gate for marker-based redo retention work.
///
/// Catalog checkpoint and redo truncation both reason about the durable
/// first-retained redo marker, the retained redo suffix on disk, and the
/// in-memory catalog-safe segment progress cache. The gate serializes those
/// sections so a checkpoint cannot scan one marker/suffix while truncation
/// publishes another marker or unlinks files below it.
///
/// This intentionally remains separate from `CatalogCheckpointGate`. The
/// catalog gate excludes `catalog.mtb` root writers and metadata DDL, but it is
/// released before redo truncation performs filesystem cleanup; this gate stays
/// held through cleanup so retained-redo scans never race disappearing files.
struct RedoRetentionGate {
    state: Mutex<RedoRetentionGateState>,
    changed: Event,
}

impl RedoRetentionGate {
    #[inline]
    fn new() -> Self {
        Self {
            state: Mutex::new(RedoRetentionGateState::default()),
            changed: Event::new(),
        }
    }

    /// Acquire exclusive access to redo-retention planning or publication.
    async fn acquire(&self) -> RedoRetentionLease<'_> {
        loop {
            {
                let mut state = self.state.lock();
                if !state.active {
                    state.active = true;
                    return RedoRetentionLease { gate: self };
                }
            }
            listener!(self.changed => listener);
            {
                let state = self.state.lock();
                if !state.active {
                    continue;
                }
            }
            listener.await;
        }
    }

    #[inline]
    fn release(&self) {
        let mut state = self.state.lock();
        debug_assert!(state.active);
        state.active = false;
        drop(state);
        self.changed.notify(usize::MAX);
    }
}

/// RAII lease for a redo-retention planning/publication/cleanup section.
///
/// While held, catalog checkpoint retained-redo scans, catalog-safe progress
/// publication, redo truncation planning, marker publication, and obsolete-file
/// cleanup run as one serialized retention observation.
pub(crate) struct RedoRetentionLease<'a> {
    gate: &'a RedoRetentionGate,
}

impl Drop for RedoRetentionLease<'_> {
    #[inline]
    fn drop(&mut self) {
        self.gate.release();
    }
}

struct QueuedCommit {
    cts: TrxID,
    waiter: CommitJoin,
}

struct CommitRejection {
    cts: TrxID,
    trx: Box<PrecommitTrx>,
    reason: FailedPrecommitReason,
}

/// Marker component that owns transaction-system background workers.
pub(crate) struct TransactionSystemWorkers;

impl Component for TransactionSystemWorkers {
    type Config = ();
    type Owned = TransactionSystemWorkersOwned;
    type Access = ();
    type Error = Error;

    const NAME: &'static str = "trx_sys_workers";

    #[inline]
    async fn build(
        _config: Self::Config,
        registry: &mut ComponentRegistry,
        mut shelf: ShelfScope<'_, Self>,
    ) -> Result<()> {
        let trx_sys = registry.dependency::<TransactionSystem>();
        let startup = shelf.take::<TransactionSystem>();
        registry.register::<Self>(startup.start(trx_sys).await?);
        Ok(())
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
        //
        // Cleanup lifetime invariant:
        // 1. Close group-commit admission and wake the log thread.
        // 2. Join the log thread, so all queued/inflight failed-precommit groups
        //    have either handed committed payloads to purge or rollback jobs to cleanup.
        // 3. Send cleanup Stop; the cleanup worker drains already queued
        //    abandoned, terminal rollback, and failed-precommit jobs before it exits.
        // 4. Send purge Stop; purge drains committed payloads accepted before
        //    the stop marker and then exits.
        //
        // This keeps rollback-capable terminal and precommit payloads alive
        // until cleanup resolves them, keeps committed payload analysis ahead
        // of purge shutdown, and keeps cleanup/purge ahead of Catalog/pool/file
        // teardown because TransactionSystemWorkers shuts down before those
        // components.
        component.workers.shutdown(&component.trx_sys);
    }
}

/// Deferred transaction-system startup resources produced after recovery.
pub(crate) struct PendingTransactionSystemStartup {
    initial_redo_header: Arc<Completion<()>>,
    purge_tx: Sender<Purge>,
    purge_rx: Receiver<Purge>,
    cleanup_tx: Sender<TrxCleanupMessage>,
    cleanup_rx: Receiver<TrxCleanupMessage>,
    pool_guards: PoolGuards,
}

impl PendingTransactionSystemStartup {
    /// Start transaction-system background workers after recovery is durable.
    #[inline]
    pub(crate) async fn start(
        self,
        trx_sys: QuiescentGuard<TransactionSystem>,
    ) -> Result<TransactionSystemWorkersOwned> {
        // Start the log thread first because it owns the redo write driver and
        // must process the initial redo header write submitted during recovery.
        let log_thread = TransactionSystem::start_log_thread(trx_sys.clone())
            .attach("phase=start_transaction_log_worker")?;
        // Wait until the initial header is durable before exposing transaction
        // workers; later redo records must not be accepted until the active log
        // file has a valid super-block.
        if let Err(report) = self.initial_redo_header.wait_result().await {
            let mut err =
                Error::from_completion_bridge(report, "wait for initial redo super-block write");
            if !trx_sys.rollback_log_thread_startup(log_thread) {
                err = err.attach(
                    "phase=rollback_initial_redo_header_failure, cleanup=join_log_worker, result=panic",
                );
            }
            return Err(err);
        }
        let purge_threads = match TransactionSystem::start_purge_threads(
            trx_sys.clone(),
            self.pool_guards,
            self.purge_rx,
        ) {
            Ok(handles) => handles,
            Err(mut report) => {
                if !trx_sys.rollback_log_thread_startup(log_thread) {
                    report = report.attach(
                        "phase=rollback_purge_spawn_failure, cleanup=join_log_worker, result=panic",
                    );
                }
                return Err(report
                    .attach("phase=start_transaction_purge_workers")
                    .into());
            }
        };
        let cleanup_thread = match TransactionSystem::start_cleanup_thread(self.cleanup_rx) {
            Ok(handle) => handle,
            Err(mut report) => {
                if !trx_sys.rollback_log_thread_startup(log_thread) {
                    report = report.attach(
                        "phase=rollback_cleanup_spawn_failure, cleanup=join_log_worker, result=panic",
                    );
                }
                if self.purge_tx.send(Purge::Stop).is_err() {
                    report = report.attach(
                        "phase=rollback_cleanup_spawn_failure, cleanup=signal_purge_stop, result=receiver_closed",
                    );
                }
                let mut join_panics = 0usize;
                for handle in purge_threads {
                    if handle.join().is_err() {
                        join_panics += 1;
                    }
                }
                if join_panics != 0 {
                    report = report.attach(format!(
                        "phase=rollback_cleanup_spawn_failure, cleanup=join_purge_workers, join_panics={join_panics}"
                    ));
                }
                return Err(report
                    .attach("phase=start_transaction_cleanup_worker")
                    .into());
            }
        };
        Ok(TransactionSystemWorkersOwned::new(
            trx_sys.into_sync(),
            self.purge_tx,
            self.cleanup_tx,
            log_thread,
            purge_threads,
            cleanup_thread,
        ))
    }
}

/// Owned transaction-system workers retained by the component registry.
pub(crate) struct TransactionSystemWorkersOwned {
    /// Transaction system guarded for worker shutdown.
    pub(crate) trx_sys: SyncQuiescentGuard<TransactionSystem>,
    workers: TransactionBackgroundWorkers,
    /// Flag that makes component shutdown idempotent.
    pub(crate) shutdown_started: AtomicBool,
}

impl TransactionSystemWorkersOwned {
    /// Create owned worker handles after transaction-system startup.
    #[inline]
    pub(crate) fn new(
        trx_sys: SyncQuiescentGuard<TransactionSystem>,
        purge_tx: Sender<Purge>,
        cleanup_tx: Sender<TrxCleanupMessage>,
        log_thread: JoinHandle<()>,
        purge_threads: Vec<JoinHandle<()>>,
        cleanup_thread: JoinHandle<()>,
    ) -> Self {
        Self {
            trx_sys,
            workers: TransactionBackgroundWorkers {
                purge_tx,
                cleanup_tx,
                log_thread: Mutex::new(Some(log_thread)),
                purge_threads: Mutex::new(purge_threads),
                cleanup_thread: Mutex::new(Some(cleanup_thread)),
            },
            shutdown_started: AtomicBool::new(false),
        }
    }
}

/// Background worker handles and stop channels for transaction services.
pub(crate) struct TransactionBackgroundWorkers {
    purge_tx: Sender<Purge>,
    cleanup_tx: Sender<TrxCleanupMessage>,
    log_thread: Mutex<Option<JoinHandle<()>>>,
    purge_threads: Mutex<Vec<JoinHandle<()>>>,
    cleanup_thread: Mutex<Option<JoinHandle<()>>>,
}

impl TransactionBackgroundWorkers {
    #[inline]
    fn shutdown(&self, trx_sys: &TransactionSystem) {
        let redo_log = &*trx_sys.redo_log;
        obs::info!("event=worker_shutdown component=trx action=start result=ok");

        // This shutdown order is a correctness contract, not only a resource
        // cleanup preference. Redo owns the final commit/fail-precommit outcome
        // after group-commit handoff, cleanup owns rollback-capable failed
        // precommit payloads, and purge owns committed-payload GC analysis.
        // Changing this sequence can drop or strand transaction-owned undo that
        // MVCC structures may still reference.

        // First close group-commit admission and wake redo. Any transaction
        // already accepted by group commit must still reach one terminal owner:
        // successful ordered completion hands committed payloads to purge, and
        // failed ordered completion hands rollback payloads to cleanup.
        {
            let mut group_commit_g = redo_log.group_commit.lock();
            group_commit_g.close(FailedPrecommitReason::Shutdown);
            group_commit_g.queue.push_back(Commit::Shutdown);
            redo_log.group_commit.notify_one();
        }

        // Join redo before stopping cleanup or purge. After this point no new
        // committed-payload handoff and no new failed-precommit cleanup job can
        // be produced by redo.
        if let Some(handle) = self.log_thread.lock().take() {
            match handle.join().inspect_err(|_| {
                obs::error!(
                    "event=worker_shutdown component=trx worker=Log-Thread action=join result=error reason=panic"
                );
            }) {
                Ok(()) => {}
                Err(payload) => {
                    // Known redo failures poison storage before thread exit. A
                    // join panic is an invariant failure; do not downgrade it
                    // to a successful shutdown.
                    resume_unwind(payload);
                }
            }
        }

        // Cleanup can update GC bucket state while rolling back failed,
        // terminal, or abandoned transactions. Stop it only after redo has
        // finished producing failed-precommit jobs, and join it before purge
        // shutdown so purge does not exit while cleanup can still move the GC
        // horizon.
        if self.cleanup_tx.send(TrxCleanupMessage::Stop).is_err() {
            obs::warn!(
                "event=worker_shutdown component=trx worker=Trx-Cleanup-Thread action=signal_stop result=ignored reason=receiver_closed"
            );
        }
        if let Some(handle) = self.cleanup_thread.lock().take() {
            match handle.join().inspect_err(|_| {
                obs::error!(
                    "event=worker_shutdown component=trx worker=Trx-Cleanup-Thread action=join result=error reason=panic"
                );
            }) {
                Ok(()) => {}
                Err(payload) => {
                    // Cleanup handles known messages explicitly. A thread panic
                    // means its ownership invariants may be broken.
                    resume_unwind(payload);
                }
            }
        }

        // Purge receives non-lossy committed-payload batches from redo. Its
        // stop marker is sent after redo and cleanup have joined, so messages
        // accepted before shutdown are recorded before the coordinator exits.
        // The purge coalescer treats Stop as terminal and may skip a final scan,
        // so this ordering is not optional: there must be no later committed
        // handoff behind Stop.
        if self.purge_tx.send(Purge::Stop).is_err() {
            obs::warn!(
                "event=worker_shutdown component=trx worker=purge action=signal_stop result=ignored reason=receiver_closed"
            );
        }
        let purge_threads = { take(&mut *self.purge_threads.lock()) };
        for handle in purge_threads {
            if let Err(payload) = handle.join().inspect_err(|_| {
                obs::error!(
                    "event=worker_shutdown component=trx worker=purge action=join result=error reason=panic"
                );
            }) {
                // Purge known failures should be represented before thread
                // exit. A join panic is an invariant failure that must remain
                // visible to the owner.
                resume_unwind(payload);
            }
        }

        // Close the active redo file last. Redo has stopped using it, and
        // cleanup/purge have finished the in-memory ownership transitions that
        // depend on ordered completion.
        let mut group_commit_g = redo_log.group_commit.lock();
        if let Some(log_file) = group_commit_g.log_file.take() {
            drop(log_file);
        }
        obs::info!("event=worker_shutdown component=trx action=finish result=ok");
    }
}

/// Message consumed by the single transaction cleanup worker.
///
/// The worker serializes cleanup that cannot be performed from `Drop`, from
/// user-cancellable terminal futures, or from the log thread. `Stop` is a
/// drain barrier, not an immediate cancellation: after it is received, the
/// worker runs any messages already pending behind the marker before exiting.
pub(crate) enum TrxCleanupMessage {
    /// Best-effort rollback for a dropped or shutdown-discovered active transaction.
    Job(TrxCleanupJob),
    /// Mandatory rollback for an explicit terminal rollback claim.
    TerminalRollback(Box<TerminalRollbackCleanupJob>),
    /// Mandatory rollback for transactions that reached precommit but cannot commit.
    FailedPrecommit(FailedPrecommitCleanupJob),
    /// Shutdown marker consumed after the log thread can no longer enqueue cleanup.
    Stop,
}

/// Startup queues consumed by the transaction system and worker owner.
struct TransactionSystemQueues {
    /// Wakeup channel for purge coordination.
    purge_tx: Sender<Purge>,
    /// Transaction cleanup queue for rollback work.
    cleanup_tx: Sender<TrxCleanupMessage>,
}

/// Terminal rollback job owned by the transaction cleanup worker.
///
/// Once a terminal rollback claim is created, the claimed mutable transaction
/// core owns rollback-capable undo and session cleanup obligations. This job
/// transfers that ownership to the cleanup worker before any rollback await
/// point, so dropping the public rollback waiter cannot cancel cleanup.
pub(crate) struct TerminalRollbackCleanupJob {
    claim: TrxCompletionClaim,
    completion: Arc<Completion<()>>,
    operation: &'static str,
}

impl TerminalRollbackCleanupJob {
    #[inline]
    async fn run(self) {
        #[cfg(test)]
        let trx_id = self.claim.entry.trx_id();
        let trx_sys = self.claim.engine().trx_sys.clone();
        #[cfg(test)]
        tests::run_terminal_rollback_test_hook(trx_id, self.operation);
        let result = trx_sys.rollback_claim(self.claim, self.operation).await;
        match result {
            Ok(()) => self.completion.complete(Ok(())),
            Err(err) => self
                .completion
                .complete(Err(CompletionErrorBridge::capture(err.attach(format!(
                    "terminal rollback cleanup failed: {}",
                    self.operation
                ))))),
        }
    }
}

/// Aggregated transaction-system and redo worker statistics.
#[derive(Default)]
pub(crate) struct TrxSysStats {
    /// Number of transactions durably or logically committed.
    pub(crate) commit_count: usize,
    /// Number of transactions processed by the log thread.
    pub(crate) trx_count: usize,
    /// Total redo log bytes written.
    pub(crate) log_bytes: usize,
    /// Number of log sync operations.
    pub(crate) sync_count: usize,
    /// Nanoseconds spent syncing redo.
    pub(crate) sync_nanos: usize,
    /// Number of redo file seal failures observed.
    pub(crate) seal_failure_count: usize,
    /// Number of backend submit-or-wait calls observed by the log thread.
    ///
    /// On `libaio`, one logical IO commonly contributes separate submit and
    /// wait syscalls, so this count can be roughly doubled compared with
    /// `io_uring` for serialized workloads.
    pub(crate) io_submit_and_wait_count: usize,
    /// Total non-overlapping nanoseconds spent in backend submit-or-wait calls.
    pub(crate) io_submit_and_wait_nanos: usize,
    /// Number of committed transactions processed by purge.
    pub(crate) purge_trx_count: usize,
    /// Number of row undo entries processed by purge.
    pub(crate) purge_row_count: usize,
    /// Number of index entries processed by purge.
    pub(crate) purge_index_count: usize,
}

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
    pub(crate) ts: CachePadded<AtomicU64>,
    /// Global visible snapshot timestamp.
    ///
    /// Purge coordination updates it only after a complete, genuinely newer
    /// horizon cycle succeeds.
    ///
    /// Data associated with smaller timestamp will be always visible to all transactions.
    global_visible_sts: CachePadded<MonotonicU64>,
    /// Purge-published active transaction horizon.
    ///
    /// This advances as soon as purge freshly scans the authoritative active
    /// buckets. Unlike `global_visible_sts`, it does not imply physical purge
    /// work has finished or that the scan scheduled transaction GC.
    published_gc_horizon: CachePadded<MonotonicU64>,
    /// Highest ordered CTS whose committed payload has reached the purge queue.
    #[cfg(test)]
    purge_handoff_cts: CachePadded<MonotonicU64>,
    /// Round-robin user-transaction GC number generator.
    rr_gc_no: CachePadded<AtomicUsize>,
    /// Active and completed transaction buckets used by purge.
    ///
    /// Split into multiple buckets to avoid a single global synchronization
    /// point on transaction begin/finish.
    pub(super) gc_buckets: Box<[GCBucket]>,
    /// Single canonical redo log stream.
    pub(crate) redo_log: CachePadded<RedoLog>,
    /// Transaction system configuration.
    pub(crate) config: CachePadded<TrxSysConfig>,
    /// Catalog of the database.
    pub(crate) catalog: QuiescentGuard<Catalog>,
    /// Table file facade used by background dropped-table cleanup.
    pub(super) table_fs: QuiescentGuard<FileSystem>,
    /// Engine-level fatal runtime poison state.
    pub(super) poisoner: QuiescentGuard<EnginePoisoner>,
    /// Wakeup channel for purge coordination.
    pub(super) purge_tx: CachePadded<Sender<Purge>>,
    /// Narrow per-engine purge scheduling hook for deterministic tests.
    #[cfg(test)]
    pub(super) purge_test_hook: CachePadded<Mutex<Option<PurgeTestHook>>>,
    /// Transaction cleanup queue for abandoned and mandatory rollback work.
    cleanup_tx: CachePadded<Sender<TrxCleanupMessage>>,
    /// Swapped table roots retained until post-publish readers drain.
    pub(super) table_roots: CachePadded<Mutex<TableRootQueue>>,
    /// Advisory queue for dropped table files waiting on catalog checkpoint safety.
    ///
    /// The catalog's retained dropped-floor entries remain authoritative; this
    /// queue only avoids full catalog-map scans during purge wakeups.
    pub(super) dropped_table_files: CachePadded<Mutex<DroppedTableFileCleanupQueue>>,
    /// Rollback payloads retained after fatal rollback cleanup failure.
    ///
    /// Poisoning stops future admitted work, but row-version maps can already
    /// contain raw references into row undo owned by a transaction payload.
    /// Fatal rollback failures move those payloads here so reachable
    /// `RowUndoRef`s remain valid until component teardown drops the engine.
    fatal_rollback_retention: CachePadded<Mutex<Vec<FatalRollbackRetention>>>,
    /// Latest in-memory catalog-safe redo segment progress.
    catalog_redo_retention: CachePadded<Mutex<Option<CatalogRedoRetentionProgress>>>,
    /// Exclusive async gate for marker-based redo retention observations.
    ///
    /// This is separate from the catalog checkpoint gate: it protects redo
    /// marker/suffix/progress consistency, not catalog metadata DDL ordering.
    redo_retention_gate: CachePadded<RedoRetentionGate>,
}

impl TransactionSystem {
    /// Recover durable state and bootstrap transaction-system startup resources.
    pub(crate) async fn bootstrap(
        config: TrxSysConfig,
        poisoner: QuiescentGuard<EnginePoisoner>,
        pools: EnginePools,
        table_fs: QuiescentGuard<FileSystem>,
        catalog: QuiescentGuard<Catalog>,
    ) -> Result<(Self, PendingTransactionSystemStartup)> {
        debug_assert!(config.purge_threads != 0);
        debug_assert!(
            (1..=256).contains(&config.gc_buckets) && config.gc_buckets.is_power_of_two()
        );
        debug_assert!(config.log_write_io_depth != 0);
        debug_assert!(config.recovery_io_depth != 0);
        debug_assert!(config.catalog_checkpoint_scan_io_depth != 0);

        let pool_guards = pools.pool_guards();
        let (purge_tx, purge_rx) = flume::unbounded();
        let (cleanup_tx, cleanup_rx) = flume::unbounded();
        let recovery_resources = RecoveryResources::new(pools, table_fs.clone(), &catalog);
        let coordinator = recovery_resources.prepare(&config)?;
        let (max_recovered_cts, finalizer) = coordinator.recover_all().await?;
        let initial_trx_ts = recovery_initial_trx_ts(max_recovered_cts)?;
        let (redo_log, initial_redo_header) = finalizer.finalize(purge_tx.clone())?;
        let redo_log = CachePadded::new(redo_log);

        let trx_sys = Self::new(
            config,
            poisoner,
            catalog,
            table_fs,
            redo_log,
            initial_trx_ts,
            TransactionSystemQueues {
                purge_tx: purge_tx.clone(),
                cleanup_tx: cleanup_tx.clone(),
            },
        );
        Ok((
            trx_sys,
            PendingTransactionSystemStartup {
                initial_redo_header,
                purge_tx,
                purge_rx,
                cleanup_tx,
                cleanup_rx,
                pool_guards,
            },
        ))
    }

    /// Create a transaction system with redo, catalog, and background queues.
    #[inline]
    fn new(
        config: TrxSysConfig,
        poisoner: QuiescentGuard<EnginePoisoner>,
        catalog: QuiescentGuard<Catalog>,
        table_fs: QuiescentGuard<FileSystem>,
        redo_log: CachePadded<RedoLog>,
        initial_ts: TrxID,
        queues: TransactionSystemQueues,
    ) -> Self {
        debug_assert!((MIN_SNAPSHOT_TS..MAX_SNAPSHOT_TS).contains(&initial_ts));
        let gc_buckets: Vec<_> = (0..config.gc_buckets).map(|_| GCBucket::new()).collect();
        // Recovery can insert dropped-floor entries before purge queues exist.
        // Seed the advisory queue once so checkpoint-gated file cleanup resumes
        // without scanning the catalog map on every later purge wake.
        let dropped_table_files = DroppedTableFileCleanupQueue::from_items(
            catalog.snapshot_dropped_table_file_cleanups(),
        );
        TransactionSystem {
            ts: CachePadded::new(AtomicU64::new(initial_ts.as_u64())),
            global_visible_sts: CachePadded::new(MonotonicU64::new(initial_ts.as_u64())),
            published_gc_horizon: CachePadded::new(MonotonicU64::new(initial_ts.as_u64())),
            #[cfg(test)]
            purge_handoff_cts: CachePadded::new(MonotonicU64::new(initial_ts.as_u64())),
            rr_gc_no: CachePadded::new(AtomicUsize::new(0)),
            gc_buckets: gc_buckets.into_boxed_slice(),
            redo_log,
            config: CachePadded::new(config),
            catalog,
            table_fs,
            poisoner,
            purge_tx: CachePadded::new(queues.purge_tx),
            #[cfg(test)]
            purge_test_hook: CachePadded::new(Mutex::new(None)),
            cleanup_tx: CachePadded::new(queues.cleanup_tx),
            table_roots: CachePadded::new(Mutex::new(TableRootQueue::default())),
            dropped_table_files: CachePadded::new(Mutex::new(dropped_table_files)),
            fatal_rollback_retention: CachePadded::new(Mutex::new(Vec::new())),
            catalog_redo_retention: CachePadded::new(Mutex::new(None)),
            redo_retention_gate: CachePadded::new(RedoRetentionGate::new()),
        }
    }

    /// Record catalog-safe redo retention progress after a catalog checkpoint publish.
    #[inline]
    pub(crate) fn record_catalog_redo_retention_progress(
        &self,
        progress: CatalogRedoRetentionProgress,
    ) {
        let mut current = self.catalog_redo_retention.lock();
        match current.as_mut() {
            Some(stored) if progress.catalog_replay_start_ts < stored.catalog_replay_start_ts => {}
            Some(stored) if progress.catalog_replay_start_ts == stored.catalog_replay_start_ts => {
                stored.merge_equal_boundary(progress);
            }
            _ => *current = Some(progress),
        }
    }

    /// Return the latest in-memory catalog-safe redo retention progress.
    #[inline]
    pub(crate) fn catalog_redo_retention_progress(&self) -> Option<CatalogRedoRetentionProgress> {
        self.catalog_redo_retention.lock().clone()
    }

    /// Acquire the exclusive redo-retention gate.
    ///
    /// Use this around code that observes or changes the durable first-retained
    /// redo marker together with retained redo files or catalog-safe segment
    /// progress.
    #[inline]
    pub(crate) async fn begin_redo_retention(&self) -> RedoRetentionLease<'_> {
        self.redo_retention_gate.acquire().await
    }

    /// Retain undo/effect ownership after rollback can no longer finish safely.
    ///
    /// This is only for fatal rollback access failures after storage has been
    /// poisoned. It is not a retry queue: retention is the memory-lifetime
    /// guarantee for raw row-undo pointers that may still be reachable from
    /// in-memory row-version chains.
    #[inline]
    pub(super) fn retain_fatal_rollback(&self, retention: FatalRollbackRetention) {
        if retention.is_empty() {
            return;
        }
        self.fatal_rollback_retention.lock().push(retention);
    }

    /// Returns next GC number.
    ///
    /// It is used to evenly dispatch transactions to all GC buckets.
    #[inline]
    fn next_gc_no(&self) -> usize {
        self.rr_gc_no.fetch_add(1, Ordering::Relaxed) % self.gc_buckets.len()
    }

    /// Returns the minimum active snapshot timestamp across all GC buckets.
    #[inline]
    pub(crate) fn min_active_sts(&self) -> TrxID {
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
    fn reject_group_creation_failure(
        &self,
        redo_log: &RedoLog,
        group_commit_g: &mut MutexGuard<'_, GroupCommit>,
        error: EnqueuePrecommitError,
    ) -> CommitRejection {
        let EnqueuePrecommitError {
            trx,
            reason,
            close_admission,
        } = error;
        let cts = trx.cts;
        if close_admission {
            let error = match reason {
                FailedPrecommitReason::Fatal(error) => {
                    obs::error!(
                        "event=engine_poison component=trx action=poison result=error error={:?}",
                        error
                    );
                    self.poisoner.poison_shared(error)
                }
                FailedPrecommitReason::Resource(resource) => {
                    let report = Report::new(resource)
                        .change_context(FatalError::RedoWrite)
                        .attach("redo group creation resource failure");
                    obs::error!(
                        "event=engine_poison component=trx action=poison result=error error={:?}",
                        report
                    );
                    self.poisoner.poison(report)
                }
                FailedPrecommitReason::Shutdown => {
                    let report = Report::new(FatalError::RedoWrite)
                        .attach("redo group creation failed during shutdown");
                    obs::error!(
                        "event=engine_poison component=trx action=poison result=error error={:?}",
                        report
                    );
                    self.poisoner.poison(report)
                }
            };
            let reason = FailedPrecommitReason::Fatal(error);
            group_commit_g.close(reason.clone());
            redo_log.group_commit.notify_one();
            return CommitRejection { cts, trx, reason };
        }
        CommitRejection { cts, trx, reason }
    }

    #[inline]
    fn try_enqueue_new_group(
        &self,
        precommit_trx: PrecommitTrx,
        group_commit_g: &mut MutexGuard<'_, GroupCommit>,
        wait_sync: bool,
    ) -> StdResult<CommitJoin, CommitRejection> {
        let redo_log = &*self.redo_log;
        match redo_log.enqueue_precommit_group(precommit_trx, group_commit_g, wait_sync) {
            Ok(waiter) => {
                redo_log.group_commit.notify_one();
                Ok(waiter)
            }
            Err(error) => Err(self.reject_group_creation_failure(redo_log, group_commit_g, error)),
        }
    }

    #[inline]
    fn enqueue_commit(
        &self,
        trx: PreparedTrx,
        wait_sync: bool,
    ) -> StdResult<QueuedCommit, CommitRejection> {
        let redo_log = &*self.redo_log;
        let mut group_commit_g = redo_log.group_commit.lock();
        let cts = TrxID::new(self.ts.fetch_add(1, Ordering::SeqCst));
        debug_assert!(cts < MAX_COMMIT_TS);
        let precommit_trx = trx.fill_cts(cts);
        if let Some(reason) = group_commit_g.closed.clone() {
            drop(group_commit_g);
            return Err(CommitRejection {
                cts,
                trx: Box::new(precommit_trx),
                reason,
            });
        }
        if group_commit_g.queue.is_empty() {
            let waiter =
                match self.try_enqueue_new_group(precommit_trx, &mut group_commit_g, wait_sync) {
                    Ok(waiter) => waiter,
                    Err(rejected) => {
                        drop(group_commit_g);
                        return Err(rejected);
                    }
                };
            drop(group_commit_g);
            return Ok(QueuedCommit { cts, waiter });
        }
        let Some(last_entry) = group_commit_g.queue.back_mut() else {
            drop(group_commit_g);
            return Err(CommitRejection {
                cts,
                trx: Box::new(precommit_trx),
                reason: FailedPrecommitReason::Shutdown,
            });
        };
        let last_group = match last_entry {
            Commit::Shutdown => {
                drop(group_commit_g);
                return Err(CommitRejection {
                    cts,
                    trx: Box::new(precommit_trx),
                    reason: FailedPrecommitReason::Shutdown,
                });
            }
            Commit::Group(group) => group,
            Commit::LogFileBoundary { .. } => {
                let waiter =
                    match self.try_enqueue_new_group(precommit_trx, &mut group_commit_g, wait_sync)
                    {
                        Ok(waiter) => waiter,
                        Err(rejected) => {
                            drop(group_commit_g);
                            return Err(rejected);
                        }
                    };
                drop(group_commit_g);
                return Ok(QueuedCommit { cts, waiter });
            }
        };
        let trx = match last_group.try_join(precommit_trx, wait_sync) {
            Left(waiter) => {
                drop(group_commit_g);
                return Ok(QueuedCommit { cts, waiter });
            }
            Right(rejected) => rejected,
        };
        let waiter = match self.try_enqueue_new_group(trx, &mut group_commit_g, wait_sync) {
            Ok(waiter) => waiter,
            Err(rejected) => {
                drop(group_commit_g);
                return Err(rejected);
            }
        };
        drop(group_commit_g);
        Ok(QueuedCommit { cts, waiter })
    }

    #[inline]
    fn commit_prepared_no_wait(&self, trx: PreparedTrx) -> Result<TrxID> {
        debug_assert!(trx.attachment.is_none());
        // This API is for sessionless system transactions only.
        //
        // System transactions are used to durably piggyback internal state
        // changes that become globally visible once the redo group commits.
        // They do not participate in user-session rollback semantics, so this
        // path intentionally returns immediately without a waiter or a detached
        // transaction attachment.
        //
        // Do not route a session-bound user transaction here. The user path
        // must wait for durability so the caller can commit/rollback the
        // session explicitly.
        match self.enqueue_commit(trx, false) {
            Ok(QueuedCommit { cts, waiter }) => {
                debug_assert!(waiter.is_none());
                Ok(cts)
            }
            Err(CommitRejection { cts, trx, reason }) => {
                (*trx).discard_rejected();
                Err(reason.into_error(format!("redo group commit is closed: commit_ts={cts}")))
            }
        }
    }

    /// Enqueue a prepared transaction and wait for ordered commit completion.
    #[inline]
    pub(crate) async fn commit_prepared(&self, trx: PreparedTrx) -> Result<TrxID> {
        let (cts, waiter) = match self.enqueue_commit(trx, true) {
            Ok(QueuedCommit { cts, waiter }) => (cts, waiter),
            Err(CommitRejection { cts, trx, reason }) => {
                let completion = Arc::new(Completion::new());
                let waiter = Arc::clone(&completion);
                self.request_failed_precommit_cleanup(FailedPrecommitCleanupJob::new(
                    vec![*trx],
                    completion,
                    reason,
                ));
                (cts, Some(waiter))
            }
        };
        // `wait_sync=true` is passed to both enqueue outcomes above, and both
        // constructors create the waiter before returning ownership here.
        let waiter = waiter.expect(
            "async prepared commit requested a completion waiter but enqueue returned none",
        );
        waiter.wait_result().await.map_err(|report| {
            Error::from_completion_bridge(
                report,
                format!("wait for redo group commit: commit_ts={cts}"),
            )
        })?;
        assert!(TrxID::new(self.redo_log.persisted_cts.load(Ordering::Relaxed)) >= cts);
        Ok(cts)
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
        engine: &EngineRef,
        session_state: &Arc<SessionState>,
    ) -> StartedTransaction {
        let gc_no = self.next_gc_no();
        let gc_bucket = &self.gc_buckets[gc_no];
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
        let inner = TrxInner::new(trx_id, sts, gc_no, session_state.id());
        let entry = TrxEntry::new(inner);
        let handle = Transaction::new(engine.downgrade(), session_state.id(), trx_id, sts);
        StartedTransaction { handle, entry }
    }

    /// Allocate a timestamp fence for a runtime state transition.
    ///
    /// Callers publish the protected state before allocating the fence. Any
    /// transaction whose STS is at or after the returned value must therefore
    /// observe that state before it can enter the protected operation.
    #[inline]
    pub(crate) fn allocate_snapshot_fence(&self) -> TrxID {
        let fence = TrxID::new(self.ts.fetch_add(1, Ordering::SeqCst));
        debug_assert!(fence < MAX_SNAPSHOT_TS);
        fence
    }

    /// Allocate a non-active timestamp for table checkpoint construction/publication.
    ///
    /// Unlike a transaction STS, this timestamp is not registered in an active
    /// GC bucket and therefore has no rollback or active-snapshot ownership.
    #[inline]
    pub(crate) fn allocate_checkpoint_ts(&self) -> TrxID {
        let checkpoint_ts = TrxID::new(self.ts.fetch_add(1, Ordering::SeqCst));
        debug_assert!(checkpoint_ts < MAX_SNAPSHOT_TS);
        checkpoint_ts
    }

    /// Begin a sessionless system transaction.
    #[inline]
    pub(crate) fn begin_sys_trx(&self) -> SysTrx {
        SysTrx {
            redo: RedoLogs::default(),
            retired_row_pages: None,
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
        if let Err(err) = self.poisoner.ensure_healthy() {
            let completion = self.enqueue_terminal_rollback(claim, "rollback poisoned commit");
            Self::wait_terminal_rollback(completion, "wait for poisoned commit rollback cleanup")
                .await?;
            return Err(err.into());
        }
        // Prepare redo log first, this may take some time,
        // so keep it out of lock scope, and we can fill cts after the lock is held.
        let (_entry, inner, attachment) = claim.into_parts();
        inner.debug_assert_redo_invariants();
        let prepared_trx = inner.prepare(attachment)?;
        if !prepared_trx.require_ordered_commit() {
            // No runtime effects means there is no CTS to publish and no commit
            // order to preserve. Effects without redo still enter group commit
            // because readers, sessions, and GC depend on ordered CTS backfill.
            self.discard_unordered_prepared(prepared_trx);
            return Ok(TrxID::new(0));
        }
        // start group commit
        self.commit_prepared(prepared_trx).await
    }

    /// Commit a system transaction through the no-wait group-commit path.
    #[inline]
    pub(crate) fn commit_sys(&self, trx: SysTrx) -> Result<TrxID> {
        self.poisoner.ensure_healthy()?;
        if trx.redo.is_empty() {
            if trx.retired_row_pages.is_some() {
                return Err(Error::from(
                    Report::new(InternalError::SystemTransactionRedoMissing)
                        .attach("system row-page retirement requires recovery-visible redo"),
                ));
            }
            // System transaction does not hold any active start timestamp
            // so we can just drop it if there is no change to replay.
            return Ok(TrxID::new(0));
        }
        // System transactions use the no-wait piggyback flow intentionally.
        // They publish internal
        // state that becomes globally visible once the redo group commits, and
        // they are not modeled as session-bound transactions that can later be
        // rolled back through the normal user transaction API.
        let prepared_trx = trx.prepare();
        self.commit_prepared_no_wait(prepared_trx)
    }

    /// Rollback active transaction.
    #[inline]
    pub(crate) async fn rollback_transaction(&self, claim: TrxCompletionClaim) -> Result<()> {
        let completion = self.enqueue_terminal_rollback(claim, "rollback active transaction");
        Self::wait_terminal_rollback(completion, "wait for terminal rollback cleanup").await
    }

    /// Queue terminal rollback cleanup and return the observer completion.
    #[inline]
    fn enqueue_terminal_rollback(
        &self,
        claim: TrxCompletionClaim,
        operation: &'static str,
    ) -> Arc<Completion<()>> {
        let completion = Arc::new(Completion::new());
        let job = TerminalRollbackCleanupJob {
            claim,
            completion: Arc::clone(&completion),
            operation,
        };
        if let Err(err) = self
            .cleanup_tx
            .send(TrxCleanupMessage::TerminalRollback(Box::new(job)))
        {
            // The returned job owns an already-claimed terminal transaction.
            // Do not drop, discard, or run it from this caller. A closed
            // receiver means the cleanup-worker lifetime invariant is broken,
            // so leak the payload and fail at that boundary.
            forget(err.0);
            panic!("terminal rollback cleanup receiver closed before transaction-system shutdown");
        }
        completion
    }

    #[inline]
    async fn wait_terminal_rollback(
        completion: Arc<Completion<()>>,
        operation: &'static str,
    ) -> Result<()> {
        match completion.wait_result().await {
            Ok(()) => Ok(()),
            Err(bridge) => Err(Error::from_completion_bridge(
                bridge,
                format!("{operation}: terminal rollback cleanup failed"),
            )),
        }
    }

    /// Rollback an abandoned transaction claimed by cleanup.
    #[inline]
    pub(crate) async fn cleanup_abandoned_transaction(
        &self,
        claim: TrxCompletionClaim,
    ) -> Result<()> {
        Ok(self
            .rollback_claim(claim, "cleanup abandoned transaction")
            .await?)
    }

    #[inline]
    async fn rollback_claim(
        &self,
        claim: TrxCompletionClaim,
        operation: &'static str,
    ) -> FatalResult<()> {
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
        operation: &'static str,
    ) -> FatalResult<()> {
        let sts = inner.sts();
        let gc_no = inner.gc_no();
        let status = inner.ctx().status();
        let pool_guards = attachment.pool_guards().clone();
        let mut table_cache = TableCache::new(&self.catalog);
        if let Err(err) = inner
            .index_undo_mut()
            .rollback(&mut table_cache, &pool_guards, sts)
            .await
        {
            entry.publish_state(TrxEntryState::Failed);
            let retention = inner.retain_and_discard_after_fatal_rollback(attachment);
            self.retain_fatal_rollback(retention);
            entry.finish(TrxEntryState::Failed);
            let report = err
                .into_report()
                .change_context(FatalError::RollbackAccess)
                .attach(format!("{operation}: index undo rollback failed"));
            obs::error!(
                "event=engine_poison component=trx action=poison result=error error={:?}",
                report
            );
            let error = self.poisoner.poison(report);
            // TODO(error-boundary): blocked=index undo rollback still returns public Error;
            // owner=typed index rollback source in RFC-0023 Phase 3;
            // tracking=docs/backlogs/000161-narrow-terminal-rollback-undo-error-boundaries.md.
            return Err(error.into_report());
        }
        if let Err(err) = inner
            .row_undo_mut()
            .rollback(&mut table_cache, &pool_guards)
            .await
        {
            entry.publish_state(TrxEntryState::Failed);
            let retention = inner.retain_and_discard_after_fatal_rollback(attachment);
            self.retain_fatal_rollback(retention);
            entry.finish(TrxEntryState::Failed);
            let report = err
                .into_report()
                .change_context(FatalError::RollbackAccess)
                .attach(format!("{operation}: row undo rollback failed"));
            obs::error!(
                "event=engine_poison component=trx action=poison result=error error={:?}",
                report
            );
            let error = self.poisoner.poison(report);
            // TODO(error-boundary): blocked=row undo rollback still returns public Error;
            // owner=typed row rollback source in RFC-0023 Phase 3;
            // tracking=docs/backlogs/000161-narrow-terminal-rollback-undo-error-boundaries.md.
            return Err(error.into_report());
        }
        inner.effects_mut().clear_for_rollback();
        self.record_rollback_for_purge(gc_no, sts);
        inner.release_transaction_locks(attachment);
        inner.finish_session_rollback(attachment);
        entry.finish(TrxEntryState::Terminal);
        status.finish_terminal();
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
        let PreparedTrxPayload::User {
            status, gc_no, sts, ..
        } = payload
        else {
            panic!("prepared no-op cleanup requires user transaction state")
        };
        self.record_rollback_for_purge(gc_no, sts);
        if let Some(s) = trx.attachment.take() {
            s.rollback();
        }
        trx.release_transaction_locks();
        status.finish_terminal();
    }

    /// Returns statistics of group commit.
    #[inline]
    pub(crate) fn trx_sys_stats(&self) -> TrxSysStats {
        let mut stats = TrxSysStats::default();
        let redo_log = &*self.redo_log;
        stats.trx_count += redo_log.stats.trx_count.load(Ordering::Relaxed);
        stats.commit_count += redo_log.stats.commit_count.load(Ordering::Relaxed);
        stats.log_bytes += redo_log.stats.log_bytes.load(Ordering::Relaxed);
        stats.sync_count += redo_log.stats.sync_count.load(Ordering::Relaxed);
        stats.sync_nanos += redo_log.stats.sync_nanos.load(Ordering::Relaxed);
        stats.seal_failure_count += redo_log.stats.seal_failure_count.load(Ordering::Relaxed);
        let io_stats = redo_log.io_backend_stats();
        stats.io_submit_and_wait_count += io_stats.submit_and_wait_calls;
        stats.io_submit_and_wait_nanos += io_stats.submit_and_wait_nanos;
        stats.purge_trx_count += redo_log.stats.purge_trx_count.load(Ordering::Relaxed);
        stats.purge_row_count += redo_log.stats.purge_row_count.load(Ordering::Relaxed);
        stats.purge_index_count += redo_log.stats.purge_index_count.load(Ordering::Relaxed);
        stats
    }

    /// Returns global visible snapshot timestamp.
    #[inline]
    pub(crate) fn global_visible_sts(&self) -> TrxID {
        TrxID::new(self.global_visible_sts.load())
    }

    /// Returns the purge-published active transaction GC horizon.
    #[inline]
    pub(crate) fn published_gc_horizon(&self) -> TrxID {
        TrxID::new(self.published_gc_horizon.load())
    }

    /// Registers for purge-published horizon advancement.
    #[inline]
    pub(crate) fn gc_horizon_listener(&self) -> EventListener {
        self.published_gc_horizon.listen()
    }

    /// Registers for completed purge-horizon-cycle advancement.
    #[inline]
    pub(crate) fn purge_completion_listener(&self) -> EventListener {
        self.global_visible_sts.listen()
    }

    /// Returns the highest ordered CTS handed to purge coordination.
    #[cfg(test)]
    #[inline]
    pub(crate) fn purge_handoff_cts(&self) -> TrxID {
        TrxID::new(self.purge_handoff_cts.load())
    }

    /// Registers for ordered purge-handoff advancement.
    #[cfg(test)]
    #[inline]
    pub(crate) fn purge_handoff_listener(&self) -> EventListener {
        self.purge_handoff_cts.listen()
    }

    /// Publish that ordered commit payloads through `cts` reached purge coordination.
    #[cfg(test)]
    #[inline]
    pub(crate) fn publish_purge_handoff(&self, cts: TrxID) {
        let previous = self.purge_handoff_cts.load();
        debug_assert!(cts.as_u64() >= previous);
        self.purge_handoff_cts.advance(cts.as_u64());
    }

    /// Publish active transaction horizon progress observed by purge.
    ///
    /// This is intentionally separate from `global_visible_sts`: frozen-page
    /// checkpoint stabilization only needs to know relevant transactions are no
    /// longer active, while visibility acceleration waits until purge work
    /// completes and updates `global_visible_sts`.
    #[inline]
    pub(crate) fn publish_gc_horizon(&self, sts: TrxID) -> bool {
        self.published_gc_horizon.advance(sts.as_u64())
    }

    /// Update global visible snapshot timestamp.
    #[inline]
    pub(crate) fn update_global_visible_sts(&self, sts: TrxID) -> bool {
        self.global_visible_sts.advance(sts.as_u64())
    }

    #[inline]
    fn redo_log_writer<'a>(
        &'a self,
        config: &TrxSysConfig,
        write_driver: &'a mut LogWriteDriver,
    ) -> RedoLogWriter<'a> {
        RedoLogWriter::new(self, &self.poisoner, config, write_driver)
    }

    /// Run the single-threaded redo log loop until shutdown.
    #[inline]
    pub(crate) fn log_loop(&self) {
        let redo_log = &*self.redo_log;
        let mut write_driver = redo_log.take_log_write_driver();
        let mut sealer = LogFileSealer::new(&self.config);
        {
            let mut writer = self.redo_log_writer(&self.config, &mut write_driver);
            writer.process_until_shutdown(&mut sealer);
            debug_assert!(writer.shutdown());
            debug_assert!(!writer.has_pending_io());
        }
        if self.poisoner.poison_error().is_none() {
            sealer.seal_active_file_best_effort(self, &mut write_driver);
        }
    }

    /// Start the background log thread.
    #[inline]
    pub(crate) fn start_log_thread(trx_sys: QuiescentGuard<Self>) -> RuntimeResult<JoinHandle<()>> {
        thread::spawn_named("Log-Thread", move || trx_sys.log_loop())
    }

    /// Stop and reclaim a log worker owned by an uncommitted startup sequence.
    #[inline]
    pub(crate) fn rollback_log_thread_startup(&self, handle: JoinHandle<()>) -> bool {
        let redo_log = &*self.redo_log;
        {
            let mut group_commit_g = redo_log.group_commit.lock();
            group_commit_g.close(FailedPrecommitReason::Shutdown);
            group_commit_g.queue.push_back(Commit::Shutdown);
            redo_log.group_commit.notify_one();
        }
        handle
            .join()
            .inspect_err(|_| {
                obs::error!(
                    "event=worker_startup_rollback component=trx worker=Log-Thread action=join result=error reason=panic"
                );
            })
            .is_ok()
    }

    /// Start the transaction cleanup worker.
    ///
    /// The thread blocks once at the top level, then handles cleanup messages
    /// through async rollback code. It exits only when the sender side is gone
    /// or when it receives `Stop`; in the `Stop` case it drains any messages
    /// already queued behind the marker. Worker shutdown relies on this drain
    /// behavior to let failed-precommit handoffs finish before lower-level
    /// table, pool, and file components are torn down.
    #[inline]
    pub(crate) fn start_cleanup_thread(
        cleanup_rx: Receiver<TrxCleanupMessage>,
    ) -> RuntimeResult<JoinHandle<()>> {
        thread::spawn_named("Trx-Cleanup-Thread", move || {
            runtime::block_on(async move {
                while let Ok(message) = cleanup_rx.recv_async().await {
                    if run_trx_cleanup_message(message).await {
                        continue;
                    }
                    while let Ok(message) = cleanup_rx.try_recv() {
                        let _ = run_trx_cleanup_message(message).await;
                    }
                    return;
                }
            })
        })
    }

    /// Queue abandoned transaction rollback cleanup.
    ///
    /// This is a best-effort signal from public-handle drop or owner shutdown
    /// scanning. During normal operation the receiver is alive; during owner
    /// shutdown, the engine keeps scanning and waiting for active transaction
    /// state to become terminal before component teardown starts.
    #[inline]
    pub(crate) fn request_abandoned_trx_cleanup(
        &self,
        engine: EngineRef,
        session_id: SessionID,
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

    /// Queue failed-precommit rollback cleanup.
    ///
    /// Unlike abandoned cleanup, this is not best effort. A user precommit
    /// transaction owns undo memory that in-memory MVCC structures can still
    /// reference until rollback runs. Therefore every failed-precommit producer
    /// must enqueue cleanup before waiters are completed.
    ///
    /// Send failure means the cleanup receiver is gone. That should be
    /// unreachable for normal shutdown: transaction-system worker shutdown
    /// first closes group-commit admission, wakes and joins the log thread,
    /// and only then sends cleanup `Stop` and joins the cleanup worker.
    #[inline]
    pub(crate) fn request_failed_precommit_cleanup(&self, job: FailedPrecommitCleanupJob) {
        if let Err(err) = self
            .cleanup_tx
            .send(TrxCleanupMessage::FailedPrecommit(job))
        {
            // The returned job may own rollback-capable `PrecommitTrx` payloads.
            // Do not drop, discard, or run it from this synchronous caller. The
            // closed receiver means the worker-lifetime invariant is already
            // broken, so leak the payload and fail at the invariant boundary.
            forget(err.0);
            panic!("failed-precommit cleanup receiver closed before log thread stopped");
        }
    }

    /// Returns the current ordered durable-prefix watermark `W` from the redo log.
    ///
    /// Catalog maintenance uses this conservative snapshot as its redo scan
    /// upper bound. Accepted work that has not reached the durable prefix is
    /// left for a later checkpoint. Ordered no-log transactions can advance
    /// this runtime watermark, but recovery still seeds future timestamps only
    /// from checkpoint metadata, table roots, and real redo headers.
    #[inline]
    pub(crate) fn persisted_watermark_cts(&self) -> TrxID {
        TrxID::new(self.redo_log.persisted_cts.load(Ordering::Acquire))
    }

    /// Build the catalog checkpoint scan configuration from the transaction config.
    #[inline]
    pub(crate) fn catalog_checkpoint_scan_config(&self) -> Result<CatalogCheckpointScanConfig> {
        Ok(CatalogCheckpointScanConfig {
            file_prefix: self.config.file_prefix()?,
            read_ahead_depth: self.config.catalog_checkpoint_scan_io_depth,
        })
    }
}

impl Supplier<TransactionSystemWorkers> for TransactionSystem {
    type Provision = PendingTransactionSystemStartup;
}

impl Component for TransactionSystem {
    type Config = TrxSysConfig;
    type Owned = Self;
    type Access = QuiescentGuard<Self>;
    type Error = Error;

    const NAME: &'static str = "trx_sys";

    #[inline]
    async fn build(
        mut config: Self::Config,
        registry: &mut ComponentRegistry,
        mut shelf: ShelfScope<'_, Self>,
    ) -> Result<()> {
        config.validate()?;
        let meta_pool = registry.dependency::<MetaPool>();
        let index_pool = registry.dependency::<IndexPool>();
        let mem_pool = registry.dependency::<MemPool>();
        let table_fs = registry.dependency::<FileSystem>();
        let disk_pool = registry.dependency::<DiskPool>();
        let catalog = registry.dependency::<Catalog>();
        let poisoner = registry.dependency::<EnginePoisoner>();

        let (trx_sys, startup) = TransactionSystem::bootstrap(
            config,
            poisoner,
            EnginePools::new(
                meta_pool.clone_inner(),
                index_pool.clone_inner(),
                mem_pool.clone_inner(),
                disk_pool.clone_inner(),
            ),
            table_fs,
            catalog,
        )
        .await?;
        registry.register::<Self>(trx_sys);
        shelf.put::<TransactionSystemWorkers>(startup);
        Ok(())
    }

    #[inline]
    fn access(owner: &QuiescentBox<Self::Owned>) -> Self::Access {
        owner.guard()
    }

    #[inline]
    fn shutdown(_component: &Self::Owned) {}
}

#[inline]
fn recovery_initial_trx_ts(max_recovered_cts: TrxID) -> Result<TrxID> {
    max_recovered_cts
        .checked_add(1)
        .filter(|ts| *ts < MAX_SNAPSHOT_TS)
        .ok_or_else(|| {
            Error::from(
                Report::new(DataIntegrityError::LogFileCorrupted).attach(format!(
                    "recovered commit timestamp out of range: max_recovered_cts={max_recovered_cts}"
                )),
            )
        })
}

#[inline]
async fn run_trx_cleanup_job(job: TrxCleanupJob) {
    let TrxCleanupJob {
        engine,
        session_id,
        trx_id,
        reason,
    } = job;
    let _ = reason;
    let (entry, session) = match engine.session_registry.resolve_trx(session_id, trx_id) {
        Ok(parts) => parts,
        Err(_) => return,
    };
    match entry.inspect_state() {
        TrxEntryState::Abandoned => {
            let trx_sys = engine.trx_sys.clone();
            let attachment = TrxAttachment::new(engine, session, trx_id);
            let claim = match TrxCompletionClaim::cleanup(entry, attachment) {
                Ok(claim) => claim,
                Err(_) => return,
            };
            let _ = trx_sys.cleanup_abandoned_transaction(claim).await;
        }
        TrxEntryState::Active
        | TrxEntryState::CheckedOut
        | TrxEntryState::CheckedOutAbandoned
        | TrxEntryState::Committing
        | TrxEntryState::RollingBack
        | TrxEntryState::CleanupRunning
        | TrxEntryState::Terminal
        | TrxEntryState::Failed => {}
    }
}

#[inline]
async fn run_trx_cleanup_message(message: TrxCleanupMessage) -> bool {
    match message {
        TrxCleanupMessage::Job(job) => {
            run_trx_cleanup_job(job).await;
            true
        }
        TrxCleanupMessage::TerminalRollback(job) => {
            job.run().await;
            true
        }
        TrxCleanupMessage::FailedPrecommit(job) => {
            job.run().await;
            true
        }
        TrxCleanupMessage::Stop => false,
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::buffer::PoolRole;
    use crate::catalog::tests::table2;
    use crate::conf::{EngineConfig, EvictableBufferPoolConfig};
    use crate::engine::Engine;
    use crate::error::ResourceError;
    use crate::id::{PageID, RowID, TableID};
    use crate::log::LogSync;
    use crate::log::format::{REDO_DEFAULT_DATA_START_OFFSET, REDO_SUPER_BLOCK_SLOT_SIZE};
    use crate::log::redo::{RowRedo, RowRedoKind};
    use crate::recovery::stream::RedoSegmentCtsRange;
    use crate::session::tests::SessionTestExt;
    use crate::trx::{RetiredRowPageBatch, SharedTrxStatus};
    use crate::value::Val;
    use std::sync::{Arc, Barrier, OnceLock};
    use std::thread::spawn;
    use tempfile::TempDir;

    /// Build a transaction system around a caller-provided redo log for log tests.
    pub(crate) fn manual_log_processor_transaction_system(
        engine: &Engine,
        config: TrxSysConfig,
        redo_log: RedoLog,
    ) -> (
        TransactionSystem,
        Receiver<Purge>,
        Receiver<TrxCleanupMessage>,
    ) {
        let (purge_tx, purge_rx) = flume::unbounded();
        let (cleanup_tx, cleanup_rx) = flume::unbounded();
        let trx_sys = TransactionSystem::new(
            config,
            engine.inner().poisoner.clone(),
            engine.inner().catalog.clone(),
            engine.inner().table_fs.clone(),
            CachePadded::new(redo_log),
            MIN_SNAPSHOT_TS,
            TransactionSystemQueues {
                purge_tx,
                cleanup_tx,
            },
        );
        (trx_sys, purge_rx, cleanup_rx)
    }

    type TerminalRollbackTestHook = Arc<dyn Fn(TrxID, &'static str) + Send + Sync + 'static>;

    fn terminal_rollback_test_hook_slot() -> &'static Mutex<Option<TerminalRollbackTestHook>> {
        static HOOK: OnceLock<Mutex<Option<TerminalRollbackTestHook>>> = OnceLock::new();
        HOOK.get_or_init(|| Mutex::new(None))
    }

    /// Guard that restores the previous terminal rollback test hook on drop.
    pub(crate) struct TerminalRollbackTestHookGuard {
        previous: Option<TerminalRollbackTestHook>,
    }

    impl Drop for TerminalRollbackTestHookGuard {
        #[inline]
        fn drop(&mut self) {
            *terminal_rollback_test_hook_slot().lock() = self.previous.take();
        }
    }

    /// Install a test-only hook invoked after terminal rollback worker ownership.
    #[inline]
    pub(crate) fn install_terminal_rollback_test_hook(
        hook: TerminalRollbackTestHook,
    ) -> TerminalRollbackTestHookGuard {
        let mut slot = terminal_rollback_test_hook_slot().lock();
        let previous = slot.replace(hook);
        TerminalRollbackTestHookGuard { previous }
    }

    #[inline]
    pub(crate) fn run_terminal_rollback_test_hook(trx_id: TrxID, operation: &'static str) {
        let hook = terminal_rollback_test_hook_slot().lock().clone();
        if let Some(hook) = hook {
            hook(trx_id, operation);
        }
    }

    /// Returns retained fatal rollback payload count for tests.
    pub(crate) fn fatal_rollback_retention_count(trx_sys: &TransactionSystem) -> usize {
        trx_sys.fatal_rollback_retention.lock().len()
    }

    pub(crate) fn retains_statement_row_undo(
        trx_sys: &TransactionSystem,
        table_id: TableID,
        row_id: RowID,
    ) -> bool {
        trx_sys
            .fatal_rollback_retention
            .lock()
            .iter()
            .any(|retention| match retention {
                FatalRollbackRetention::Statement { row_undo, .. } => row_undo
                    .iter()
                    .any(|undo| undo.table_id == table_id && undo.row_id == row_id),
                _ => false,
            })
    }

    async fn build_trx_sys_redo_test_engine_with_log_file_max_size(
        log_file_stem: &str,
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
                    .log_sync(LogSync::None)
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

    fn add_large_system_redo(sys_trx: &mut SysTrx, value_count: usize) {
        let values = (0..value_count as u64).map(Val::from).collect();
        sys_trx.redo.insert_dml(
            TableID::from(1u64),
            RowRedo {
                page_id: PageID::from(1u64),
                row_id: RowID::new(0),
                kind: RowRedoKind::Insert(values),
            },
        );
    }

    fn catalog_safe_segment(
        file_seq: u32,
        redo_range: Option<(TrxID, TrxID)>,
    ) -> CatalogSafeRedoSegment {
        CatalogSafeRedoSegment {
            file_seq,
            redo_range: redo_range
                .map(|(min_cts, max_cts)| RedoSegmentCtsRange { min_cts, max_cts }),
        }
    }

    fn capture_transaction_cleanup_state(
        trx: &Transaction,
    ) -> (Arc<TrxEntry>, Arc<SharedTrxStatus>) {
        let engine = trx.engine().expect("test transaction must have engine");
        let (entry, _session) = engine
            .session_registry
            .resolve_trx(trx.session_id, trx.trx_id())
            .expect("test transaction must resolve");
        let status = {
            let inner_slot = entry.inner.lock();
            inner_slot
                .as_ref()
                .expect("test transaction must be checked in")
                .ctx()
                .status()
        };
        (entry, status)
    }

    #[test]
    fn test_catalog_redo_retention_progress_records_monotonic_merge() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = EngineConfig::default()
                .storage_root(temp_dir.path().to_path_buf())
                .trx(TrxSysConfig::default().log_file_stem("redo_catalog_retention"))
                .build()
                .await
                .unwrap();
            let trx_sys = &engine.inner().trx_sys;

            assert_eq!(trx_sys.catalog_redo_retention_progress(), None);

            trx_sys.record_catalog_redo_retention_progress(CatalogRedoRetentionProgress {
                first_retained_file_seq: 0,
                catalog_replay_start_ts: TrxID::new(10),
                segments: vec![catalog_safe_segment(
                    1,
                    Some((TrxID::new(2), TrxID::new(4))),
                )],
            });
            trx_sys.record_catalog_redo_retention_progress(CatalogRedoRetentionProgress {
                first_retained_file_seq: 0,
                catalog_replay_start_ts: TrxID::new(9),
                segments: vec![catalog_safe_segment(0, None)],
            });
            assert_eq!(
                trx_sys.catalog_redo_retention_progress().unwrap().segments,
                vec![catalog_safe_segment(
                    1,
                    Some((TrxID::new(2), TrxID::new(4)))
                )]
            );

            trx_sys.record_catalog_redo_retention_progress(CatalogRedoRetentionProgress {
                first_retained_file_seq: 0,
                catalog_replay_start_ts: TrxID::new(10),
                segments: vec![catalog_safe_segment(0, None), catalog_safe_segment(2, None)],
            });
            assert_eq!(
                trx_sys.catalog_redo_retention_progress(),
                Some(CatalogRedoRetentionProgress {
                    first_retained_file_seq: 0,
                    catalog_replay_start_ts: TrxID::new(10),
                    segments: vec![
                        catalog_safe_segment(0, None),
                        catalog_safe_segment(1, Some((TrxID::new(2), TrxID::new(4)))),
                        catalog_safe_segment(2, None),
                    ],
                })
            );
        });
    }

    #[test]
    fn test_redo_retention_gate_serializes_leases() {
        smol::block_on(async {
            let gate = RedoRetentionGate::new();
            let lease = gate.acquire().await;
            let mut waiter = Box::pin(gate.acquire());

            assert!(matches!(
                futures::poll!(waiter.as_mut()),
                std::task::Poll::Pending
            ));

            drop(lease);
            let _next_lease = waiter.await;
        });
    }

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
                spawn(move || {
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
    fn test_poison_engine_concurrent_callers_share_first_error() {
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

            let trx_sys = engine.inner().trx_sys.clone();
            let barrier = Arc::new(Barrier::new(3));

            let worker_a_barrier = Arc::clone(&barrier);
            let worker_a_trx_sys = trx_sys.clone();
            let worker_a = spawn(move || {
                worker_a_barrier.wait();
                worker_a_trx_sys
                    .poisoner
                    .poison(Report::new(FatalError::RedoWrite).attach("test writer failure"))
            });

            let worker_b_barrier = Arc::clone(&barrier);
            let worker_b_trx_sys = trx_sys.clone();
            let worker_b = spawn(move || {
                worker_b_barrier.wait();
                worker_b_trx_sys
                    .poisoner
                    .poison(Report::new(FatalError::RedoSync).attach("test sync failure"))
            });

            barrier.wait();

            let err_a = worker_a.join().unwrap();
            let err_b = worker_b.join().unwrap();
            let stored = trx_sys.poisoner.poison_error().unwrap();
            let stored_reason = *stored.current_context();

            assert!(trx_sys.poisoner.poison_error().is_some());
            assert_eq!(err_a.reason(), FatalError::RedoWrite);
            assert_eq!(err_b.reason(), FatalError::RedoSync);
            assert!(
                trx_sys
                    .poisoner
                    .ensure_healthy()
                    .as_ref()
                    .is_err_and(|err| *err.current_context() == stored_reason)
            );
            assert!(matches!(
                stored_reason,
                FatalError::RedoWrite | FatalError::RedoSync
            ));

            drop(trx_sys);
            drop(engine);
        });
    }

    #[test]
    fn test_poison_engine_listener_wakes_first_waiters() {
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
                .trx(TrxSysConfig::default().log_file_stem("redo_poison_listener"))
                .build()
                .await
                .unwrap();

            let trx_sys = engine.inner().trx_sys.clone();
            let listener = trx_sys.poisoner.listener();
            let waiter = smol::spawn(async move {
                listener.await;
            });

            trx_sys.poisoner.ensure_healthy().unwrap();
            let err = trx_sys
                .poisoner
                .poison(Report::new(FatalError::CheckpointWrite).attach("test checkpoint failure"));
            assert_eq!(err.reason(), FatalError::CheckpointWrite);
            waiter.await;
            assert!(
                trx_sys
                    .poisoner
                    .ensure_healthy()
                    .as_ref()
                    .is_err_and(|err| *err.current_context() == FatalError::CheckpointWrite)
            );

            let late_listener = trx_sys.poisoner.listener();
            let err = trx_sys
                .poisoner
                .poison(Report::new(FatalError::RedoSync).attach("test sync failure"));
            assert_eq!(err.reason(), FatalError::RedoSync);
            assert_eq!(
                trx_sys
                    .poisoner
                    .poison_error()
                    .as_ref()
                    .map(|report| *report.current_context()),
                Some(FatalError::CheckpointWrite)
            );
            drop(late_listener);
            assert!(
                trx_sys
                    .poisoner
                    .ensure_healthy()
                    .as_ref()
                    .is_err_and(|err| *err.current_context() == FatalError::CheckpointWrite)
            );

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
                        .log_file_stem("redo_rotate")
                        .log_file_max_size(1024u64 * 1024),
                )
                .build()
                .await
                .unwrap();
            let table_id = table2(&engine).await;

            let mut session = engine.new_session().unwrap();
            let s = [1u8; 196];
            for i in 0..COUNT {
                let mut trx = session.begin_trx().unwrap();
                trx.exec(async |stmt| {
                    let insert = vec![Val::from(i as i32), Val::from(&s[..])];
                    stmt.table_insert_mvcc(table_id, insert).await?;
                    Ok(())
                })
                .await
                .unwrap();
                trx.commit().await.unwrap();
            }
            drop(session);
            drop(engine);
        });
    }

    #[test]
    fn test_commit_sys_new_log_allocation_failure_rejects_without_panic() {
        smol::block_on(async {
            let (_temp_dir, engine) = build_trx_sys_redo_test_engine_with_log_file_max_size(
                "sys_new_log_alloc_failure",
                REDO_DEFAULT_DATA_START_OFFSET + REDO_SUPER_BLOCK_SLOT_SIZE,
            )
            .await;
            let mut sys_trx = engine.inner().trx_sys.begin_sys_trx();
            add_large_system_redo(&mut sys_trx, 1024);
            let res = engine.inner().trx_sys.commit_sys(sys_trx);

            let err = res.unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<ResourceError>().copied(),
                Some(ResourceError::StorageFileCapacityExceeded)
            );
            engine.inner().poisoner.ensure_healthy().unwrap();
        });
    }

    #[test]
    fn test_checkpoint_timestamp_is_not_registered_as_active() {
        smol::block_on(async {
            let (_temp_dir, engine) = build_trx_sys_redo_test_engine_with_log_file_max_size(
                "checkpoint_non_active_ts",
                64usize * 1024,
            )
            .await;
            let trx_sys = &engine.inner().trx_sys;
            let checkpoint_ts = trx_sys.allocate_checkpoint_ts();
            assert!(checkpoint_ts >= MIN_SNAPSHOT_TS);
            assert_eq!(trx_sys.min_active_sts(), MAX_SNAPSHOT_TS);
            assert!(
                trx_sys.gc_buckets.iter().all(|bucket| bucket
                    .active_sts_list
                    .lock()
                    .active
                    .is_empty())
            );
        });
    }

    #[test]
    fn test_sys_trx_without_retirement_does_not_perturb_user_gc_sequence() {
        smol::block_on(async {
            let (_temp_dir, engine) = build_trx_sys_redo_test_engine_with_log_file_max_size(
                "sys_trx_gc_sequence",
                64usize * 1024,
            )
            .await;
            let trx_sys = &engine.inner().trx_sys;
            let user_gc_no = trx_sys.rr_gc_no.load(Ordering::Relaxed);
            let _first = trx_sys.begin_sys_trx();
            let _second = trx_sys.begin_sys_trx();

            assert_eq!(trx_sys.rr_gc_no.load(Ordering::Relaxed), user_gc_no);
        });
    }

    #[test]
    fn test_commit_sys_rejects_gc_pages_without_redo() {
        smol::block_on(async {
            let (_temp_dir, engine) = build_trx_sys_redo_test_engine_with_log_file_max_size(
                "sys_gc_requires_redo",
                64usize * 1024,
            )
            .await;
            let mut sys_trx = engine.inner().trx_sys.begin_sys_trx();
            sys_trx.retire_row_pages(RetiredRowPageBatch::new(
                TableID::new(3),
                RowID::new(0),
                RowID::new(1),
                vec![PageID::new(46)].into_boxed_slice(),
            ));
            let err = engine.inner().trx_sys.commit_sys(sys_trx).unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<InternalError>().copied(),
                Some(InternalError::SystemTransactionRedoMissing)
            );
            engine.inner().poisoner.ensure_healthy().unwrap();
        });
    }

    #[test]
    fn test_user_commit_new_log_allocation_failure_cleans_failed_precommit() {
        smol::block_on(async {
            let (_temp_dir, engine) = build_trx_sys_redo_test_engine_with_log_file_max_size(
                "user_new_log_alloc_failure",
                64usize * 1024,
            )
            .await;
            let table_id = table2(&engine).await;
            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();
            let value = [7u8; 196];
            trx.exec(async |stmt| {
                for i in 0..384 {
                    let insert = vec![Val::from(i), Val::from(&value[..])];
                    stmt.table_insert_mvcc(table_id, insert).await?;
                }
                Ok(())
            })
            .await
            .unwrap();
            let (entry, status) = capture_transaction_cleanup_state(&trx);

            let err = trx.commit().await.unwrap_err();

            assert_eq!(
                err.report().downcast_ref::<ResourceError>().copied(),
                Some(ResourceError::StorageFileCapacityExceeded),
                "{err:?}"
            );
            assert_eq!(entry.inspect_state(), TrxEntryState::Terminal);
            assert!(!session.in_trx().unwrap());
            assert!(!status.preparing());
            assert!(status.prepare_listener().is_none());
            engine.inner().poisoner.ensure_healthy().unwrap();
        });
    }
}
