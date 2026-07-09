use crate::buffer::guard::PageSharedGuard;
use crate::buffer::{BufferPool, EvictableBufferPool, PoolGuards};
use crate::catalog::{
    Catalog, DroppedTableFileCleanup, DroppedTableRuntime, TableCache, is_catalog_table,
};
use crate::error::{FatalError, InternalError, Result};
use crate::file::table_file::OldRoot;
use crate::id::{PageID, TrxID};
use crate::latch::LatchFallbackMode;
use crate::map::{FastHashMap, FastHashSet};
use crate::quiescent::{QuiescentGuard, SyncQuiescentGuard};
use crate::row::RowPage;
use crate::runtime;
use crate::table::Table;
use crate::thread;
use crate::trx::row::RowWriteAccess;
use crate::trx::sys::TransactionSystem;
use crate::trx::undo::{OwnedRowUndo, RowUndoKind};
use crate::trx::{CommittedTrx, MAX_SNAPSHOT_TS};
use crossbeam_utils::CachePadded;
use error_stack::Report;
use flume::{Receiver, Sender};
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread::JoinHandle;

/// Swapped user-table active root retained until post-publish readers drain.
struct RetainedTableRoot {
    fence_ts: TrxID,
    _old_root: OldRoot,
}

/// Purge-owned queue for swapped table roots awaiting post-publish reader drain.
#[derive(Default)]
pub(crate) struct TableRootQueue {
    roots: VecDeque<RetainedTableRoot>,
}

impl TableRootQueue {
    /// Retain an old table root until the fence timestamp is below the active horizon.
    #[inline]
    pub(crate) fn push_retained(&mut self, fence_ts: TrxID, old_root: OldRoot) {
        self.roots.push_back(RetainedTableRoot {
            fence_ts,
            _old_root: old_root,
        });
    }

    #[inline]
    fn release_ready(&mut self, min_active_sts: TrxID) {
        let mut retained = VecDeque::new();
        while let Some(item) = self.roots.pop_front() {
            if item.fence_ts < min_active_sts {
                drop(item);
            } else {
                retained.push_back(item);
            }
        }
        self.roots = retained;
    }
}

/// Purge-owned worklist for dropped table files waiting on catalog checkpoint safety.
///
/// This queue is only an optimization to avoid scanning the whole catalog map on
/// every dropped-table purge wake. The catalog's `DroppedFloor` entry remains
/// authoritative for redo retention and for validating that a queued file
/// cleanup item is still current.
#[derive(Default)]
pub(crate) struct DroppedTableFileCleanupQueue {
    files: VecDeque<DroppedTableFileCleanup>,
}

impl DroppedTableFileCleanupQueue {
    /// Seed the queue from catalog-retained dropped floors after recovery.
    ///
    /// This restores pending file cleanup work that may have been waiting on a
    /// future catalog checkpoint when the previous engine instance stopped.
    #[inline]
    pub(crate) fn from_items(mut items: Vec<DroppedTableFileCleanup>) -> Self {
        items.sort_by_key(Self::cleanup_key);
        Self {
            files: VecDeque::from(items),
        }
    }

    #[inline]
    fn push_ordered(&mut self, item: DroppedTableFileCleanup) {
        let key = Self::cleanup_key(&item);
        let idx = self
            .files
            .iter()
            .position(|existing| Self::cleanup_key(existing) > key)
            .unwrap_or(self.files.len());
        self.files.insert(idx, item);
    }

    #[inline]
    fn drain_ready(&mut self, catalog_replay_start_ts: TrxID) -> Vec<DroppedTableFileCleanup> {
        let mut ready = Vec::new();
        while self
            .files
            .front()
            .is_some_and(|item| item.drop_cts < catalog_replay_start_ts)
        {
            let Some(item) = self.files.pop_front() else {
                break;
            };
            ready.push(item);
        }
        ready
    }

    #[inline]
    fn prepend_failed_ready(&mut self, items: Vec<DroppedTableFileCleanup>) {
        debug_assert!(Self::cleanups_ordered(&items));
        if let (Some(last_item), Some(front_item)) = (items.last(), self.files.front()) {
            debug_assert!(Self::cleanup_key(last_item) <= Self::cleanup_key(front_item));
        }
        for item in items.into_iter().rev() {
            self.files.push_front(item);
        }
    }

    #[inline]
    fn cleanup_key(item: &DroppedTableFileCleanup) -> (u64, u64) {
        (item.drop_cts.as_u64(), item.table_id.as_u64())
    }

    #[inline]
    fn cleanups_ordered(items: &[DroppedTableFileCleanup]) -> bool {
        items
            .windows(2)
            .all(|window| Self::cleanup_key(&window[0]) <= Self::cleanup_key(&window[1]))
    }
}

impl TransactionSystem {
    /// Wake the purge coordinator for table-root retention cleanup only.
    ///
    /// This is best-effort. Dropping the purge receiver is the normal shutdown
    /// signal path, so callers must not treat a failed send as a storage error.
    #[inline]
    pub(crate) fn request_table_root_retention_purge(&self) {
        let _ = self.purge_tx.send(Purge::TableRootRetention);
    }

    /// Wake the purge coordinator for dropped-table cleanup only.
    ///
    /// This is best-effort. Dropping the purge receiver is the normal shutdown
    /// signal path, so callers must not treat a failed send as a storage error.
    #[inline]
    pub(crate) fn request_dropped_table_purge(&self) {
        let _ = self.purge_tx.send(Purge::DroppedTable);
    }

    /// Start the purge coordinator and optional executor threads.
    #[inline]
    pub(crate) fn start_purge_threads(
        trx_sys: QuiescentGuard<Self>,
        mem_pool: QuiescentGuard<EvictableBufferPool>,
        pool_guards: PoolGuards,
        purge_chan: Receiver<Purge>,
    ) -> Vec<JoinHandle<()>> {
        let trx_sys = trx_sys.into_sync();
        let mem_pool = mem_pool.into_sync();
        if trx_sys.config.purge_threads == 1 {
            // single-threaded purger
            let task_trx_sys = trx_sys.clone();
            let task_mem_pool = mem_pool.clone();
            let handle = thread::spawn_named("Purge-Thread", move || {
                let mut purger = PurgeSingleThreaded;
                runtime::block_on(purger.purge_loop(
                    &task_mem_pool,
                    &task_trx_sys.catalog,
                    &task_trx_sys,
                    pool_guards,
                    purge_chan,
                ))
            });
            vec![handle]
        } else {
            // multi-threaded purger
            let dispatcher_guards = pool_guards.clone();
            let (mut dispatcher, executors) = Self::dispatch_purge(trx_sys.clone(), pool_guards);
            let task_trx_sys = trx_sys.clone();
            let task_mem_pool = mem_pool.clone();
            let handle = thread::spawn_named("Purge-Dispatcher", move || {
                runtime::block_on(dispatcher.purge_loop(
                    &task_mem_pool,
                    &task_trx_sys.catalog,
                    &task_trx_sys,
                    dispatcher_guards,
                    purge_chan,
                ));
            });
            let mut handles = Vec::with_capacity(executors.len() + 1);
            handles.push(handle);
            handles.extend(executors);
            handles
        }
    }

    /// Calculate the purge horizon from active transaction buckets.
    #[inline]
    pub(crate) fn calc_min_active_sts_for_gc(&self) -> TrxID {
        // first, we load current STS as upperbound.
        // There might be case a transaction begins and commits
        // when we refresh min_active_sts, if we do not hold this
        // upperbound, we may clear the new transaction incorrectly.
        let max_active_sts = TrxID::new(self.ts.load(Ordering::SeqCst));
        // then, load actual minimum active STS from all GC buckets.
        self.min_active_sts().min(max_active_sts)
    }

    /// Record committed transaction handoffs accepted from redo completion.
    ///
    /// Purge coordination owns this step so committed payload batches cannot be
    /// overtaken by a separate stop marker. Returns whether the
    /// global minimum active STS may have advanced and a full purge cycle should
    /// be requested when the engine is not already tearing down.
    #[inline]
    pub(super) fn record_committed_for_purge(
        &self,
        trx_list: FastHashMap<usize, Vec<CommittedTrx>>,
    ) -> bool {
        let mut changed = false;
        for (gc_no, trx_list) in trx_list {
            let gc_bucket = &self.gc_buckets[gc_no];
            changed |= gc_bucket.record_committed_for_purge(trx_list);
        }
        changed
    }

    /// Record rollback progress and wake purge if the active horizon advanced.
    ///
    /// Commit handoffs are non-lossy through `Purge::Committed` because purge
    /// must retain every committed payload batch. Rollback horizon wakeups are
    /// lossy: bucket state is already authoritative, so the message only asks
    /// purge to recalculate the horizon and run one full GC cycle.
    #[inline]
    pub(crate) fn record_rollback_for_purge(&self, gc_no: usize, sts: TrxID) -> bool {
        let advanced = self.gc_buckets[gc_no].record_rollback_for_purge(sts);
        if advanced {
            let _ = self.purge_tx.send(Purge::HorizonAdvanced);
        }
        advanced
    }

    #[inline]
    pub(super) fn dispatch_purge(
        trx_sys: SyncQuiescentGuard<Self>,
        pool_guards: PoolGuards,
    ) -> (PurgeDispatcher, Vec<JoinHandle<()>>) {
        let mut handles = vec![];
        let mut chans = vec![];
        for i in 0..trx_sys.config.purge_threads {
            let (tx, rx) = flume::unbounded();
            chans.push(tx);
            let thread_name = format!("Purge-Executor-{i}");
            let pool_guards = pool_guards.clone();
            let task_trx_sys = trx_sys.clone();
            let handle = thread::spawn_named(thread_name, move || {
                let mut purger = PurgeExecutor;
                runtime::block_on(purger.purge_task_loop(
                    &task_trx_sys.catalog,
                    &task_trx_sys,
                    pool_guards,
                    rx,
                ));
            });
            handles.push(handle);
        }
        (PurgeDispatcher(chans), handles)
    }

    /// Purge row undo logs and index entries according to given transaction
    /// list and minimum active STS.
    ///
    /// This method is the purge access-failure boundary. If row-page access or
    /// secondary-index cleanup fails, purge cannot replay the same in-memory
    /// mutation safely, so it poisons runtime admission before returning the
    /// fatal purge error.
    #[inline]
    pub(super) async fn purge_trx_list(
        &self,
        catalog: &Catalog,
        guards: &PoolGuards,
        trx_list: Vec<CommittedTrx>,
        min_active_sts: TrxID,
    ) -> Result<FastHashSet<PageID>> {
        let res: Result<FastHashSet<PageID>> = async {
            let mut table_cache = TableCache::new(catalog);
            let purge_trx_count = trx_list.len();
            let mut purge_row_count = 0;
            let mut purge_index_count = 0;
            // First, purge row undo logs by versioned page identity.
            for trx in &trx_list {
                if let Some(row_undo) = trx.row_undo() {
                    purge_row_count += row_undo.len();
                    for undo in &**row_undo {
                        if is_catalog_table(undo.table_id) {
                            let Some(table) = table_cache.get_catalog_table(undo.table_id) else {
                                continue;
                            };
                            let page_guard = if let Some(page_id) = undo.page_id {
                                table.get_row_page_versioned_shared(guards, page_id).await?
                            } else {
                                None
                            };
                            let Some(page_guard) = page_guard else {
                                continue;
                            };
                            purge_undo_chain_from_page(page_guard, undo, min_active_sts);
                        } else {
                            let Some(table) = table_cache.get_user_table(undo.table_id).await
                            else {
                                continue;
                            };
                            let page_guard = if let Some(page_id) = undo.page_id {
                                table
                                    .mem
                                    .get_row_page_versioned_shared(guards, page_id)
                                    .await?
                            } else {
                                None
                            };
                            let Some(page_guard) = page_guard else {
                                promote_delete_marker_if_needed(table, undo);
                                continue;
                            };
                            purge_undo_chain_from_page(page_guard, undo, min_active_sts);
                        }
                    }
                }
            }

            // Second, we purge index.
            for trx in &trx_list {
                if let Some(index_gc) = trx.index_gc() {
                    for ip in index_gc {
                        if is_catalog_table(ip.table_id) {
                            let Some(table) = table_cache.get_catalog_table(ip.table_id) else {
                                continue;
                            };
                            if table
                                .delete_index(
                                    guards,
                                    ip.key.index_no,
                                    &ip.key.vals,
                                    ip.row_id,
                                    ip.unique,
                                    min_active_sts,
                                )
                                .await?
                            {
                                purge_index_count += 1;
                            }
                        } else {
                            let Some(table) = table_cache.get_user_entry_mut(ip.table_id).await
                            else {
                                continue;
                            };
                            if table
                                .delete_index(guards, &ip.key, ip.row_id, ip.unique, min_active_sts)
                                .await?
                            {
                                purge_index_count += 1;
                            }
                        }
                    }
                }
            }
            let mut gc_row_pages = FastHashSet::default();
            for trx in &trx_list {
                if let Some(pages) = trx.gc_row_pages() {
                    gc_row_pages.extend(pages.iter().copied());
                }
            }

            drop(trx_list);

            self.redo_log
                .stats
                .purge_trx_count
                .fetch_add(purge_trx_count, Ordering::Relaxed);
            self.redo_log
                .stats
                .purge_row_count
                .fetch_add(purge_row_count, Ordering::Relaxed);
            self.redo_log
                .stats
                .purge_index_count
                .fetch_add(purge_index_count, Ordering::Relaxed);
            Ok(gc_row_pages)
        }
        .await;
        match res {
            Ok(gc_row_pages) => Ok(gc_row_pages),
            Err(_) => Err(self.poison_engine(FatalError::PurgeAccess).into()),
        }
    }

    #[inline]
    async fn deallocate_gc_row_pages(
        &self,
        mem_pool: &EvictableBufferPool,
        guards: &PoolGuards,
        gc_row_pages: FastHashSet<PageID>,
    ) -> Result<()> {
        for page_id in gc_row_pages {
            let page_guard = mem_pool
                .get_page::<RowPage>(guards.mem_guard(), page_id, LatchFallbackMode::Exclusive)
                .await?
                .lock_exclusive_async()
                .await
                .unwrap();
            mem_pool.deallocate_page(page_guard);
        }
        Ok(())
    }

    /// Deallocate retired row pages, poisoning storage on failure.
    ///
    /// Returns `false` only after publishing `FatalError::PurgeDeallocate`.
    #[inline]
    async fn deallocate_gc_row_pages_or_poison(
        &self,
        mem_pool: &EvictableBufferPool,
        guards: &PoolGuards,
        gc_row_pages: FastHashSet<PageID>,
    ) -> bool {
        match self
            .deallocate_gc_row_pages(mem_pool, guards, gc_row_pages)
            .await
        {
            Ok(()) => true,
            Err(_) => {
                let _ = self.poison_engine(FatalError::PurgeDeallocate);
                false
            }
        }
    }

    #[inline]
    fn process_retained_table_roots(&self, min_active_sts: TrxID) {
        self.table_roots.lock().release_ready(min_active_sts);
    }

    #[inline]
    async fn process_dropped_table_gc(
        &self,
        guards: &PoolGuards,
        min_active_sts: TrxID,
    ) -> Result<()> {
        let mut stale_handles = Vec::new();
        for DroppedTableRuntime {
            table_id,
            drop_cts,
            replay_floor,
            table,
        } in self.catalog.take_dropped_runtime_candidates(min_active_sts)
        {
            match Arc::try_unwrap(table) {
                Ok(table) => {
                    // At this point purge has exclusive ownership of the dropped
                    // runtime. Any destroy error is fatal to the storage runtime
                    // and is converted to poison by the caller.
                    table.destroy_dropped_runtime(guards).await?;
                    // Runtime destruction leaves only the catalog `DroppedFloor`.
                    // The queued item schedules checkpoint-gated file unlink;
                    // redo retention still reads the catalog floor as truth.
                    self.dropped_table_files
                        .lock()
                        .push_ordered(DroppedTableFileCleanup::new(
                            table_id,
                            drop_cts,
                            replay_floor,
                        ));
                }
                Err(table) => {
                    // Stale external table handles are not fatal. Keep retrying
                    // on future purge wakes until the last handle is released.
                    stale_handles.push(DroppedTableRuntime {
                        table_id,
                        drop_cts,
                        replay_floor,
                        table,
                    })
                }
            }
        }

        let stale_handle_count = stale_handles.len();
        for (idx, item) in stale_handles.into_iter().enumerate() {
            let table_id = item.table_id;
            let drop_cts = item.drop_cts;
            let replay_floor = item.replay_floor;
            let strong_count = Arc::strong_count(&item.table);
            let remaining_stale_handles = stale_handle_count - idx - 1;
            if !self.catalog.restore_dropped_runtime(item) {
                return Err(Report::new(InternalError::Generic)
                    .attach(format!(
                        "restore dropped table runtime after stale handle: table_id={table_id}, drop_cts={drop_cts}, replay_floor={replay_floor:?}, strong_count={strong_count}, remaining_stale_handles={remaining_stale_handles}"
                    ))
                    .into());
            }
        }

        self.process_dropped_table_file_deletes();
        Ok(())
    }

    /// Destroy purge-ready dropped table runtimes, poisoning storage on failure.
    ///
    /// Returns `false` only after publishing `FatalError::PurgeDeallocate`.
    #[inline]
    async fn process_dropped_table_gc_or_poison(
        &self,
        guards: &PoolGuards,
        min_active_sts: TrxID,
    ) -> bool {
        match self.process_dropped_table_gc(guards, min_active_sts).await {
            Ok(()) => true,
            Err(_) => {
                let _ = self.poison_engine(FatalError::PurgeDeallocate);
                false
            }
        }
    }

    #[inline]
    fn process_dropped_table_file_deletes(&self) {
        // File deletion is a checkpoint-gated housekeeping step. Unlike runtime
        // destroy, unlink failure is retryable and does not poison the engine:
        // retain the catalog floor entry and requeue failed ready items so a
        // later purge wake can retry without scanning every catalog table.
        let catalog_replay_start_ts = match self.catalog.storage.checkpoint_snapshot() {
            Ok(snapshot) => snapshot.catalog_replay_start_ts,
            Err(_) => return,
        };
        // Drain only the checkpoint-ready prefix. Newer entries stay queued,
        // preserving order without a full drain-and-requeue pass.
        let file_deletes = self
            .dropped_table_files
            .lock()
            .drain_ready(catalog_replay_start_ts);
        if file_deletes.is_empty() {
            return;
        }

        let mut failed = Vec::new();
        for item in file_deletes {
            if self.table_fs.delete_user_table_file(item.table_id).is_err() {
                failed.push(item);
                continue;
            }
            // Successful unlink is followed by a catalog-validated floor
            // removal. A mismatch is self-healing in release builds because
            // startup can rediscover the retained floor and retry the
            // idempotent file delete, but debug builds should surface the
            // invariant violation immediately.
            let removed = self.catalog.remove_dropped_floor(item);
            debug_assert!(
                removed,
                "remove dropped floor after file delete: item={item:?}"
            );
        }
        if !failed.is_empty() {
            self.dropped_table_files.lock().prepend_failed_ready(failed);
        }
    }
}

/// ActiveStsList maintains snapshot timestamps of active transactions
/// in order.
/// It is used to calculate min_active_sts for garbage collection.
#[derive(Default)]
pub(super) struct ActiveStsList {
    // ordered snapshot timestamps.
    pub(super) active: VecDeque<TrxID>,
    // cached deleted snapshot timestamps if it's not smallest.
    pub(super) deleted: FastHashSet<TrxID>,
}

impl ActiveStsList {
    /// Insert a new snapshot timestamp.
    /// The value should be larger than any one stored in the list.
    #[inline]
    pub(super) fn insert(&mut self, value: TrxID) {
        debug_assert!(self.active.is_empty() || self.active.back().unwrap() < &value);
        self.active.push_back(value);
    }

    /// Returns length of this list.
    #[inline]
    pub(super) fn len(&self) -> usize {
        self.active.len()
    }

    /// Remove a snapshot timestamp from the list.
    /// Returns the smallest snapshot timestamp in the list.
    /// If list is empty, returns MAX_SNAPSHOT_TS.
    /// If it's not the smallest one, cache it in deleted set.
    /// This is an optimization to speed up retrieval of smallest snapshot timestamp.
    #[inline]
    pub(super) fn remove(&mut self, value: TrxID) -> TrxID {
        debug_assert!(!self.active.is_empty());
        let first = self.active.front().copied().unwrap();
        if first == value {
            let _ = self.active.pop_front();
            while let Some(first) = self.active.front() {
                if !self.deleted.remove(first) {
                    return *first; // smallest STS not deleted.
                }
                self.active.pop_front();
            }
            debug_assert!(self.active.is_empty());
            debug_assert!(self.deleted.is_empty());
            return MAX_SNAPSHOT_TS;
        }
        // cache the deletion.
        let res = self.deleted.insert(value);
        debug_assert!(res);
        first
    }
}

/// GCBucket stores and records transaction GC information for purge,
/// including committed transaction list, old transaction list, active snapshot timestamp
/// list, etc.
pub(crate) struct GCBucket {
    /// Committed transaction list.
    /// When a transaction is committed, it will be put into this queue in sequence.
    /// Head is always oldest and tail is newest.
    pub(super) committed_trx_list: CachePadded<Mutex<VecDeque<CommittedTrx>>>,
    /// Active snapshot timestamp list.
    /// The smallest value equals to min_active_sts.
    pub(super) active_sts_list: CachePadded<Mutex<ActiveStsList>>,
    /// Minimum active snapshot sts of this bucket.
    pub(crate) min_active_sts: CachePadded<AtomicU64>,
}

impl GCBucket {
    /// Create a new GC bucket.
    #[inline]
    pub(crate) fn new() -> Self {
        GCBucket {
            committed_trx_list: CachePadded::new(Mutex::new(VecDeque::new())),
            active_sts_list: CachePadded::new(Mutex::new(ActiveStsList::default())),
            min_active_sts: CachePadded::new(AtomicU64::new(MAX_SNAPSHOT_TS.as_u64())),
        }
    }

    /// Get committed transaction list to purge.
    #[inline]
    pub(super) fn get_purge_list(&self, min_active_sts: TrxID, trx_list: &mut Vec<CommittedTrx>) {
        // If a transaction's committed timestamp is less than the smallest
        // snapshot timestamp of all active transactions, it means this transction's
        // data vesion is latest and all its undo log can be purged.
        // So we move such transactions from commited list to old list.
        let mut g = self.committed_trx_list.lock();
        while let Some(trx) = g.front() {
            if trx.cts < min_active_sts {
                trx_list.push(g.pop_front().unwrap());
            } else {
                break;
            }
        }
    }

    /// Record a rolled-back transaction for purge.
    #[inline]
    pub(super) fn record_rollback_for_purge(&self, sts: TrxID) -> bool {
        debug_assert!(TrxID::new(self.min_active_sts.load(Ordering::Relaxed)) != MAX_SNAPSHOT_TS);
        let mut active_sts_list = self.active_sts_list.lock();
        let min_sts = active_sts_list.remove(sts);
        self.update_min_active_sts(min_sts)
    }

    /// Record committed transactions for purge.
    #[inline]
    pub(super) fn record_committed_for_purge(&self, trx_list: Vec<CommittedTrx>) -> bool {
        // Update both active sts list and committed transaction list
        let mut active_sts_list = self.active_sts_list.lock();
        let mut min_sts = MAX_SNAPSHOT_TS;
        {
            let mut committed_trx_list = self.committed_trx_list.lock();
            for trx in trx_list {
                if let Some(sts) = trx.sts() {
                    min_sts = active_sts_list.remove(sts);
                    committed_trx_list.push_back(trx);
                }
            }
        }
        // Update minimum active STS
        // separate load and store is safe because this update will only happen when lock of
        // active_sts_list is acquired.
        self.update_min_active_sts(min_sts)
    }

    #[inline]
    fn update_min_active_sts(&self, min_sts: TrxID) -> bool {
        // Because we just commit/rollback at least one transaction in this bucket, that means there must be
        // some transaction in the list before, so current value of min_active_sts must not be MAX.
        debug_assert!(TrxID::new(self.min_active_sts.load(Ordering::Relaxed)) != MAX_SNAPSHOT_TS);

        // There is no active transaction. We should update min_active_sts.
        if min_sts == MAX_SNAPSHOT_TS {
            self.min_active_sts
                .store(min_sts.as_u64(), Ordering::Relaxed);
            return true;
        }

        // There are active transactions. We should compare them and update only if
        // latest value is larger.
        let curr_sts = TrxID::new(self.min_active_sts.load(Ordering::Relaxed));
        if min_sts > curr_sts {
            self.min_active_sts
                .store(min_sts.as_u64(), Ordering::Relaxed);
            return true;
        }
        false
    }
}

/// Messages sent to the purge coordinator.
pub(crate) enum Purge {
    /// Stop the purge coordinator and let worker shutdown join the thread.
    ///
    /// This is a shutdown-only barrier. It may make the coordinator exit
    /// without running another purge scan, so worker shutdown must enqueue it
    /// only after redo has joined and no further non-lossy `Committed` handoffs
    /// can be produced.
    Stop,
    /// Non-lossy committed transaction payload handoff from redo completion.
    ///
    /// The payload map is grouped by GC bucket. The purge coordinator must
    /// record every accepted batch before it exits.
    Committed(FastHashMap<usize, Vec<CommittedTrx>>),
    /// Lossy rollback-driven active-horizon wakeup.
    HorizonAdvanced,
    /// Release retained table roots whose post-publish readers have drained.
    TableRootRetention,
    /// Run only dropped-table runtime/file cleanup.
    DroppedTable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PurgeWork {
    full_gc: bool,
    table_root_retention: bool,
    dropped_table: bool,
    /// Terminal shutdown marker. When set, the purge loop returns without
    /// running cleanup work collected before the marker.
    stop_after: bool,
}

impl PurgeWork {
    #[inline]
    const fn none() -> Self {
        Self {
            full_gc: false,
            table_root_retention: false,
            dropped_table: false,
            stop_after: false,
        }
    }

    #[inline]
    const fn full() -> Self {
        Self {
            full_gc: true,
            table_root_retention: true,
            dropped_table: true,
            stop_after: false,
        }
    }

    #[inline]
    #[cfg(test)]
    const fn table_root_retention() -> Self {
        Self {
            full_gc: false,
            table_root_retention: true,
            dropped_table: false,
            stop_after: false,
        }
    }

    #[inline]
    const fn stop() -> Self {
        Self {
            full_gc: false,
            table_root_retention: false,
            dropped_table: false,
            stop_after: true,
        }
    }

    #[inline]
    fn absorb<F>(&mut self, purge: Purge, analyze_committed: &mut F) -> bool
    where
        F: FnMut(FastHashMap<usize, Vec<CommittedTrx>>) -> bool,
    {
        match purge {
            Purge::Stop => {
                // This early stop is safe only because shutdown sends `Stop`
                // after redo has joined, so all non-lossy committed handoffs
                // have already been queued before the marker. Do not move
                // `Purge::Stop` earlier in shutdown without changing this
                // coalescing contract.
                *self = PurgeWork::stop();
                return false;
            }
            Purge::Committed(trx_list) => {
                if analyze_committed(trx_list) {
                    *self = PurgeWork::full();
                }
            }
            Purge::HorizonAdvanced => *self = PurgeWork::full(),
            Purge::TableRootRetention => self.table_root_retention = true,
            Purge::DroppedTable => self.dropped_table = true,
        }
        true
    }
}

struct PurgeTask {
    gc_no: usize,
    min_active_sts: TrxID,
    done: Sender<Result<()>>,
    gc_row_pages: Arc<Mutex<Vec<PageID>>>,
}

trait PurgeLoop {
    async fn purge_loop(
        &mut self,
        mem_pool: &EvictableBufferPool,
        catalog: &Catalog,
        trx_sys: &TransactionSystem,
        pool_guards: PoolGuards,
        purge_chan: Receiver<Purge>,
    );
}

/// Single-threaded purge-loop implementation.
#[derive(Default)]
pub(crate) struct PurgeSingleThreaded;

impl PurgeLoop for PurgeSingleThreaded {
    #[inline]
    async fn purge_loop(
        &mut self,
        mem_pool: &EvictableBufferPool,
        catalog: &Catalog,
        trx_sys: &TransactionSystem,
        pool_guards: PoolGuards,
        purge_chan: Receiver<Purge>,
    ) {
        // initialize min_active_sts.
        let mut min_sts = trx_sys.global_visible_sts();
        while let Ok(purge) = purge_chan.recv() {
            let work = coalesce_purge_work(&purge_chan, purge, |trx_list| {
                trx_sys.record_committed_for_purge(trx_list)
            });
            if work.stop_after {
                return;
            }
            let curr_sts = trx_sys.calc_min_active_sts_for_gc();
            trx_sys.publish_gc_horizon(curr_sts);
            if work.full_gc && curr_sts > min_sts {
                // Start GC. Purge undo/index first, then deallocate retired
                // row pages once all bucket lists have been collected.
                let mut gc_row_pages = FastHashSet::default();
                let mut trx_list = vec![];
                for gc_bucket in &trx_sys.gc_buckets {
                    gc_bucket.get_purge_list(curr_sts, &mut trx_list);
                }
                let bucket_gc_pages = match trx_sys
                    .purge_trx_list(catalog, &pool_guards, trx_list, curr_sts)
                    .await
                {
                    Ok(gc_pages) => gc_pages,
                    Err(_) => return,
                };
                gc_row_pages.extend(bucket_gc_pages);
                let deallocated = trx_sys
                    .deallocate_gc_row_pages_or_poison(mem_pool, &pool_guards, gc_row_pages)
                    .await;
                if !deallocated {
                    // Runtime admission is already poisoned. Exiting the loop
                    // leaves shutdown to join this already-finished worker.
                    return;
                }
            }
            if work.table_root_retention {
                trx_sys.process_retained_table_roots(curr_sts);
            }
            if work.dropped_table {
                let dropped_tables_processed = trx_sys
                    .process_dropped_table_gc_or_poison(&pool_guards, curr_sts)
                    .await;
                if !dropped_tables_processed {
                    // Dropped-table runtime destruction failed after purge took
                    // ownership. Do not retry in-place against a poisoned runtime.
                    return;
                }
            }
            if work.full_gc {
                // Once GC is finished, update global_visible_sts so other threads can use it to
                // speed up visibility check.
                trx_sys.update_global_visible_sts(curr_sts);
                min_sts = curr_sts;
            }
        }
    }
}

/// Dispatcher that fans purge tasks out to executor threads.
pub(crate) struct PurgeDispatcher(Vec<Sender<PurgeTask>>);

impl PurgeLoop for PurgeDispatcher {
    #[inline]
    async fn purge_loop(
        &mut self,
        mem_pool: &EvictableBufferPool,
        _catalog: &Catalog,
        trx_sys: &TransactionSystem,
        pool_guards: PoolGuards,
        purge_chan: Receiver<Purge>,
    ) {
        let mut min_sts = trx_sys.global_visible_sts();
        let mut dispatch_no: usize = 0;
        'DISPATCH_LOOP: while let Ok(purge) = purge_chan.recv_async().await {
            let work = coalesce_purge_work(&purge_chan, purge, |trx_list| {
                trx_sys.record_committed_for_purge(trx_list)
            });
            if work.stop_after {
                break 'DISPATCH_LOOP;
            }
            let curr_sts = trx_sys.calc_min_active_sts_for_gc();
            trx_sys.publish_gc_horizon(curr_sts);
            if work.full_gc && curr_sts > min_sts {
                // dispatch tasks to executors
                let (done_tx, done_rx) = flume::unbounded();
                let mut expected_tasks = 0usize;
                let gc_row_pages = Arc::new(Mutex::new(vec![]));
                for gc_no in 0..trx_sys.gc_buckets.len() {
                    let task = PurgeTask {
                        gc_no,
                        min_active_sts: curr_sts,
                        done: done_tx.clone(),
                        gc_row_pages: Arc::clone(&gc_row_pages),
                    };
                    self.0[dispatch_no % self.0.len()].send(task).expect(
                        "purge executor receiver must stay alive while dispatcher owns sender",
                    );
                    expected_tasks += 1;
                    dispatch_no += 1;
                }
                drop(done_tx);
                // wait for all executors to finish their tasks in this cycle.
                for _ in 0..expected_tasks {
                    match done_rx.recv_async().await {
                        Ok(Ok(())) => (),
                        Ok(Err(_)) => return,
                        Err(_) => break 'DISPATCH_LOOP,
                    }
                }
                let gc_row_pages = {
                    let mut g = gc_row_pages.lock();
                    g.drain(..).collect::<FastHashSet<PageID>>()
                };
                let deallocated = trx_sys
                    .deallocate_gc_row_pages_or_poison(mem_pool, &pool_guards, gc_row_pages)
                    .await;
                if !deallocated {
                    // Poison is the durable-consistency boundary here. The
                    // dispatcher exits; once the worker closure drops the
                    // dispatcher value, executor task channels close and
                    // executor threads can exit before shutdown joins them.
                    return;
                }
            }
            if work.table_root_retention {
                trx_sys.process_retained_table_roots(curr_sts);
            }
            if work.dropped_table {
                let dropped_tables_processed = trx_sys
                    .process_dropped_table_gc_or_poison(&pool_guards, curr_sts)
                    .await;
                if !dropped_tables_processed {
                    // Dropped-table destroy failure is fatal; exiting also closes
                    // the executor task channels when this dispatcher is dropped.
                    return;
                }
            }
            if work.full_gc && curr_sts > min_sts {
                // Once GC is finished, update global_visible_sts so other threads can use it to
                // speed up visibility check.
                trx_sys.update_global_visible_sts(curr_sts);
                min_sts = curr_sts;
            }
        }

        // Notify executors to quit after a normal Stop or channel close. Fatal
        // poison returns above; then the worker closure drops the dispatcher and
        // closes these same task channels.
        self.0.clear();
    }
}

/// Worker that executes dispatched purge tasks.
#[derive(Default)]
pub(crate) struct PurgeExecutor;

impl PurgeExecutor {
    #[inline]
    async fn purge_task_loop(
        &mut self,
        catalog: &Catalog,
        trx_sys: &TransactionSystem,
        pool_guards: PoolGuards,
        purge_chan: Receiver<PurgeTask>,
    ) {
        while let Ok(PurgeTask {
            gc_no,
            min_active_sts,
            done,
            gc_row_pages,
        }) = purge_chan.recv()
        {
            let mut trx_list = vec![];
            trx_sys.gc_buckets[gc_no].get_purge_list(min_active_sts, &mut trx_list);
            // actual purge here
            let res = trx_sys
                .purge_trx_list(catalog, &pool_guards, trx_list, min_active_sts)
                .await;
            match res {
                Ok(gc_pages) => {
                    if !gc_pages.is_empty() {
                        gc_row_pages.lock().extend(gc_pages);
                    }
                    let _ = done.send(Ok(()));
                }
                Err(err) => {
                    let _ = done.send(Err(err));
                }
            }
        }
    }
}

#[inline]
fn coalesce_purge_work<F>(
    purge_chan: &Receiver<Purge>,
    initial: Purge,
    mut analyze_committed: F,
) -> PurgeWork
where
    F: FnMut(FastHashMap<usize, Vec<CommittedTrx>>) -> bool,
{
    // Collapse queued lossy wakeups so bursts do not trigger repeated scans,
    // while committed payload batches are recorded one by one and never
    // coalesced away. `Stop` is a terminal shutdown barrier: messages before it
    // have been absorbed, messages after it are intentionally ignored.
    let mut work = PurgeWork::none();
    if !work.absorb(initial, &mut analyze_committed) {
        return work;
    }
    while let Ok(purge) = purge_chan.try_recv() {
        if !work.absorb(purge, &mut analyze_committed) {
            return work;
        }
    }
    work
}

#[inline]
fn promote_delete_marker_if_needed(table: &Table, undo: &OwnedRowUndo) {
    if matches!(&undo.kind, RowUndoKind::Delete) {
        table
            .deletion_buffer()
            .promote_delete_marker_if_committed(undo.row_id);
    }
}

#[inline]
fn purge_undo_chain_from_page(
    page_guard: PageSharedGuard<RowPage>,
    undo: &OwnedRowUndo,
    min_active_sts: TrxID,
) {
    let (ctx, page) = page_guard.ctx_and_page();
    if !page.row_id_in_valid_range(undo.row_id) {
        return;
    }
    let row_idx = page.row_idx(undo.row_id);
    let mut access = RowWriteAccess::new(page, ctx, row_idx, None, false);
    access.purge_undo_chain(min_active_sts);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::guard::PageSharedGuard;
    use crate::buffer::page::{Page, VersionedPageID};
    use crate::buffer::{BufferPool, PoolGuards, PoolRole};
    use crate::catalog::tests::table1;
    use crate::conf::{EngineConfig, EvictableBufferPoolConfig, TrxSysConfig};
    use crate::engine::Engine;
    use crate::error::{FatalError, Result};
    use crate::id::{RowID, TableID};
    use crate::index::{RowLocation, UniqueIndex};
    use crate::latch::LatchFallbackMode;
    use crate::row::RowPage;
    use crate::row::ops::{DeleteMvcc, SelectKey};
    use crate::table::tests::bound_unique_index;
    use crate::table::{DeleteMarker, TableRedoReplayFloor};
    use crate::trx::row::RowReadAccess;
    use crate::trx::stmt::Statement;
    use crate::trx::undo::{OwnedRowUndo, RowUndoKind, RowUndoLogs};
    use crate::trx::{CommittedTrxPayload, MIN_ACTIVE_TRX_ID, SharedTrxStatus};
    use crate::value::Val;
    use smol::Timer;
    use std::sync::Arc;
    use std::thread::sleep;
    use std::time::{Duration, Instant};
    use tempfile::TempDir;

    #[inline]
    fn full_pool_guards(engine: &Engine) -> PoolGuards {
        PoolGuards::builder()
            .push(PoolRole::Meta, engine.inner().meta_pool.pool_guard())
            .push(PoolRole::Index, engine.inner().index_pool.pool_guard())
            .push(PoolRole::Mem, engine.inner().mem_pool.pool_guard())
            .push(PoolRole::Disk, engine.inner().disk_pool.pool_guard())
            .build()
    }

    async fn stmt_insert_row(
        stmt: &mut Statement<'_>,
        table_id: TableID,
        cols: Vec<Val>,
    ) -> Result<RowID> {
        stmt.table_insert_mvcc(table_id, cols).await
    }

    async fn stmt_delete_row(
        stmt: &mut Statement<'_>,
        table_id: TableID,
        key: &SelectKey,
    ) -> Result<DeleteMvcc> {
        stmt.table_delete_unique_mvcc(table_id, key.index_no, &key.vals, false)
            .await
    }

    #[inline]
    fn dropped_file_cleanup(table_id: u64, drop_cts: u64) -> DroppedTableFileCleanup {
        DroppedTableFileCleanup::new(
            TableID::new(table_id),
            TrxID::new(drop_cts),
            TableRedoReplayFloor {
                heap_redo_start_ts: TrxID::new(1),
                deletion_cutoff_ts: TrxID::new(1),
            },
        )
    }

    #[inline]
    fn cleanup_pairs(items: &[DroppedTableFileCleanup]) -> Vec<(u64, u64)> {
        items
            .iter()
            .map(|item| (item.drop_cts.as_u64(), item.table_id.as_u64()))
            .collect()
    }

    #[inline]
    fn queued_cleanup_pairs(queue: &DroppedTableFileCleanupQueue) -> Vec<(u64, u64)> {
        queue
            .files
            .iter()
            .map(|item| (item.drop_cts.as_u64(), item.table_id.as_u64()))
            .collect()
    }

    #[test]
    fn test_dropped_table_file_cleanup_queue_sorts_seed_and_insert() {
        let mut queue = DroppedTableFileCleanupQueue::from_items(vec![
            dropped_file_cleanup(104, 20),
            dropped_file_cleanup(103, 10),
            dropped_file_cleanup(102, 10),
        ]);
        assert_eq!(
            queued_cleanup_pairs(&queue),
            vec![(10, 102), (10, 103), (20, 104)]
        );

        queue.push_ordered(dropped_file_cleanup(101, 15));
        queue.push_ordered(dropped_file_cleanup(105, 10));
        assert_eq!(
            queued_cleanup_pairs(&queue),
            vec![(10, 102), (10, 103), (10, 105), (15, 101), (20, 104)]
        );
    }

    #[test]
    fn test_dropped_table_file_cleanup_queue_drains_ready_prefix_only() {
        let mut queue = DroppedTableFileCleanupQueue::from_items(vec![
            dropped_file_cleanup(101, 10),
            dropped_file_cleanup(102, 20),
            dropped_file_cleanup(103, 30),
        ]);

        let ready = queue.drain_ready(TrxID::new(20));
        assert_eq!(cleanup_pairs(&ready), vec![(10, 101)]);
        assert_eq!(queued_cleanup_pairs(&queue), vec![(20, 102), (30, 103)]);

        let ready = queue.drain_ready(TrxID::new(31));
        assert_eq!(cleanup_pairs(&ready), vec![(20, 102), (30, 103)]);
        assert!(queue.files.is_empty());
    }

    #[test]
    fn test_dropped_table_file_cleanup_queue_prepends_failed_ready_items() {
        let mut queue = DroppedTableFileCleanupQueue::from_items(vec![
            dropped_file_cleanup(101, 10),
            dropped_file_cleanup(102, 20),
            dropped_file_cleanup(103, 30),
            dropped_file_cleanup(104, 40),
        ]);
        let ready = queue.drain_ready(TrxID::new(35));
        let failed = ready
            .into_iter()
            .filter(|item| item.drop_cts >= TrxID::new(20))
            .collect::<Vec<_>>();

        queue.prepend_failed_ready(failed);
        assert_eq!(
            queued_cleanup_pairs(&queue),
            vec![(20, 102), (30, 103), (40, 104)]
        );
    }

    #[test]
    fn test_coalesce_purge_work_preserves_strongest_request() {
        let (_tx, rx) = flume::unbounded();
        assert_eq!(
            coalesce_purge_work(&rx, Purge::TableRootRetention, |_| {
                panic!("no committed payload expected")
            }),
            PurgeWork::table_root_retention()
        );

        let (tx, rx) = flume::unbounded();
        tx.send(Purge::DroppedTable).unwrap();
        assert_eq!(
            coalesce_purge_work(&rx, Purge::TableRootRetention, |_| {
                panic!("no committed payload expected")
            }),
            PurgeWork {
                full_gc: false,
                table_root_retention: true,
                dropped_table: true,
                stop_after: false,
            }
        );

        let (tx, rx) = flume::unbounded();
        tx.send(Purge::TableRootRetention).unwrap();
        tx.send(Purge::Committed(FastHashMap::default())).unwrap();
        assert_eq!(
            coalesce_purge_work(&rx, Purge::DroppedTable, |_| true),
            PurgeWork::full()
        );

        let (tx, rx) = flume::unbounded();
        tx.send(Purge::DroppedTable).unwrap();
        assert_eq!(
            coalesce_purge_work(&rx, Purge::Committed(FastHashMap::default()), |_| true),
            PurgeWork::full()
        );

        let (tx, rx) = flume::unbounded();
        tx.send(Purge::HorizonAdvanced).unwrap();
        tx.send(Purge::HorizonAdvanced).unwrap();
        assert_eq!(
            coalesce_purge_work(&rx, Purge::HorizonAdvanced, |_| {
                panic!("rollback horizon wakeups carry no committed payload")
            }),
            PurgeWork::full()
        );
    }

    #[test]
    fn test_coalesce_purge_work_stop_wins() {
        let (tx, rx) = flume::unbounded();
        tx.send(Purge::Committed(FastHashMap::default())).unwrap();
        tx.send(Purge::Stop).unwrap();
        tx.send(Purge::TableRootRetention).unwrap();
        assert_eq!(
            coalesce_purge_work(&rx, Purge::DroppedTable, |_| true),
            PurgeWork::stop()
        );
        assert_eq!(
            coalesce_purge_work(&rx, Purge::Stop, |_| {
                panic!("no committed payload expected")
            }),
            PurgeWork::stop()
        );
    }

    #[test]
    fn test_coalesce_purge_work_analyzes_committed_before_stop() {
        let (tx, rx) = flume::unbounded();
        tx.send(Purge::TableRootRetention).unwrap();
        tx.send(Purge::Committed(FastHashMap::default())).unwrap();
        tx.send(Purge::DroppedTable).unwrap();
        tx.send(Purge::Committed(FastHashMap::default())).unwrap();
        tx.send(Purge::Stop).unwrap();
        tx.send(Purge::Committed(FastHashMap::default())).unwrap();

        let mut analyzed = 0usize;
        let work = coalesce_purge_work(&rx, Purge::Committed(FastHashMap::default()), |_| {
            analyzed += 1;
            true
        });

        assert_eq!(work, PurgeWork::stop());
        assert_eq!(analyzed, 3);
    }

    #[test]
    fn test_coalesce_purge_work_committed_can_request_full_gc() {
        let (_tx, rx) = flume::unbounded();

        let work = coalesce_purge_work(&rx, Purge::Committed(FastHashMap::default()), |_| true);

        assert_eq!(work, PurgeWork::full());
    }

    #[test]
    fn test_active_sts_list() {
        let mut active_sts_list = ActiveStsList::default();
        for (val, expected, delete) in vec![
            (1, TrxID::new(1), false),
            (1, MAX_SNAPSHOT_TS, true),
            (2, TrxID::new(2), false),
            (3, TrxID::new(2), false),
            (4, TrxID::new(2), false),
            (3, TrxID::new(2), true),
            (2, TrxID::new(4), true),
            (5, TrxID::new(4), false),
            (6, TrxID::new(4), false),
            (6, TrxID::new(4), true),
            (5, TrxID::new(4), true),
            (4, MAX_SNAPSHOT_TS, true),
        ] {
            let res = if delete {
                active_sts_list.remove(TrxID::new(val))
            } else {
                active_sts_list.insert(TrxID::new(val));
                active_sts_list.active.front().copied().unwrap()
            };
            assert_eq!(res, expected);
        }
    }

    #[test]
    fn test_deallocate_gc_row_pages_or_poison_poisons_runtime() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = EngineConfig::default()
                .storage_root(main_dir)
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .role(PoolRole::Mem)
                        .max_mem_size(64usize * 1024 * 1024)
                        .max_file_size(128usize * 1024 * 1024),
                )
                .trx(
                    TrxSysConfig::default()
                        .purge_threads(1)
                        .log_file_stem("redo_purge_poison"),
                )
                .build()
                .await
                .unwrap();

            let guards = full_pool_guards(&engine);
            assert!(
                engine
                    .inner()
                    .trx_sys
                    .deallocate_gc_row_pages_or_poison(
                        &engine.inner().mem_pool,
                        &guards,
                        FastHashSet::default(),
                    )
                    .await
            );
            assert!(engine.inner().trx_sys.poison_error().is_none());

            let raw_page = engine
                .inner()
                .mem_pool
                .allocate_page::<Page>(guards.mem_guard())
                .await
                .unwrap();
            let raw_page_id = raw_page.page_id();
            drop(raw_page);
            assert!(
                !engine
                    .inner()
                    .trx_sys
                    .deallocate_gc_row_pages_or_poison(
                        &engine.inner().mem_pool,
                        &guards,
                        FastHashSet::from_iter([raw_page_id]),
                    )
                    .await
            );
            assert!(
                engine
                    .inner()
                    .trx_sys
                    .poison_error()
                    .as_ref()
                    .is_some_and(|err| *err.current_context() == FatalError::PurgeDeallocate)
            );
            assert!(
                engine
                    .inner()
                    .trx_sys
                    .ensure_runtime_healthy()
                    .as_ref()
                    .is_err_and(|err| *err.current_context() == FatalError::PurgeDeallocate)
            );
        });
    }

    #[test]
    fn test_purge_trx_list_returns_error_on_row_page_access_failure() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = EngineConfig::default()
                .storage_root(main_dir)
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .role(PoolRole::Mem)
                        .max_mem_size(64usize * 1024 * 1024)
                        .max_file_size(128usize * 1024 * 1024),
                )
                .trx(
                    TrxSysConfig::default()
                        .purge_threads(1)
                        .log_file_stem("redo_purge_access_error"),
                )
                .build()
                .await
                .unwrap();

            let table_id = table1(&engine).await;
            let mut row_undo = RowUndoLogs::empty();
            row_undo.push(OwnedRowUndo::new(
                table_id,
                Some(VersionedPageID {
                    page_id: PageID::from(0u64),
                    generation: 0,
                }),
                RowID::new(0),
                RowUndoKind::Delete,
            ));
            let trx = CommittedTrx {
                cts: TrxID::new(100),
                payload: Some(CommittedTrxPayload {
                    sts: TrxID::new(1),
                    gc_no: 0,
                    row_undo,
                    index_gc: vec![],
                    gc_row_pages: vec![],
                }),
            };
            let guards = PoolGuards::builder()
                .push(PoolRole::Index, engine.inner().index_pool.pool_guard())
                .build();

            let err = engine
                .inner()
                .trx_sys
                .purge_trx_list(engine.catalog(), &guards, vec![trx], MAX_SNAPSHOT_TS)
                .await
                .unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<FatalError>().copied(),
                Some(FatalError::PurgeAccess)
            );
            assert!(
                engine
                    .inner()
                    .trx_sys
                    .poison_error()
                    .as_ref()
                    .is_some_and(|err| *err.current_context() == FatalError::PurgeAccess)
            );
        });
    }

    #[test]
    fn test_purge_promote_delete_marker_if_committed_for_delete_without_page_id() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = EngineConfig::default()
                .storage_root(main_dir)
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .role(PoolRole::Mem)
                        .max_mem_size(64usize * 1024 * 1024)
                        .max_file_size(128usize * 1024 * 1024),
                )
                .trx(
                    TrxSysConfig::default()
                        .purge_threads(1)
                        .log_file_stem("redo_purge_promote"),
                )
                .build()
                .await
                .unwrap();

            let table_id = table1(&engine).await;
            let table = engine.catalog().get_table(table_id).await.unwrap();
            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();
            trx.exec(async |stmt| {
                stmt_insert_row(stmt, table_id, vec![Val::from(1001i32)]).await?;
                Ok(())
            })
            .await
            .unwrap();
            trx.commit().await.unwrap();
            drop(session);
            let pool_guards = full_pool_guards(&engine);
            let key = vec![Val::from(1001i32)];
            let Some((row_id, _)) = bound_unique_index(&table, &pool_guards, 0)
                .lookup(&key, MAX_SNAPSHOT_TS)
                .await
                .unwrap()
            else {
                panic!("row should exist");
            };
            let status = Arc::new(SharedTrxStatus::new(TrxID::new(100)));
            table
                .deletion_buffer()
                .put_ref(row_id, status.clone(), MAX_SNAPSHOT_TS)
                .unwrap();

            let mut row_undo = RowUndoLogs::empty();
            row_undo.push(OwnedRowUndo::new(
                table.table_id(),
                None,
                row_id,
                RowUndoKind::Delete,
            ));
            let trx = CommittedTrx {
                cts: TrxID::new(100),
                payload: Some(CommittedTrxPayload {
                    sts: TrxID::new(1),
                    gc_no: 0,
                    row_undo,
                    index_gc: vec![],
                    gc_row_pages: vec![],
                }),
            };
            {
                let pool_guards = full_pool_guards(&engine);
                engine
                    .inner()
                    .trx_sys
                    .purge_trx_list(engine.catalog(), &pool_guards, vec![trx], MAX_SNAPSHOT_TS)
                    .await
                    .unwrap();
            }

            match table.deletion_buffer().get(row_id) {
                Some(DeleteMarker::Committed(ts)) => assert_eq!(ts, status.ts()),
                Some(DeleteMarker::Ref(_)) => panic!("delete marker should be promoted"),
                None => panic!("delete marker should exist"),
            }
        });
    }

    #[test]
    fn test_purge_skip_promote_delete_marker_if_uncommitted_for_delete_without_page_id() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = EngineConfig::default()
                .storage_root(main_dir)
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .role(PoolRole::Mem)
                        .max_mem_size(64usize * 1024 * 1024)
                        .max_file_size(128usize * 1024 * 1024),
                )
                .trx(
                    TrxSysConfig::default()
                        .purge_threads(1)
                        .log_file_stem("redo_purge_no_promote"),
                )
                .build()
                .await
                .unwrap();

            let table_id = table1(&engine).await;
            let table = engine.catalog().get_table(table_id).await.unwrap();
            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();
            trx.exec(async |stmt| {
                stmt_insert_row(stmt, table_id, vec![Val::from(1002i32)]).await?;
                Ok(())
            })
            .await
            .unwrap();
            trx.commit().await.unwrap();
            drop(session);
            let pool_guards = full_pool_guards(&engine);
            let key = vec![Val::from(1002i32)];
            let Some((row_id, _)) = bound_unique_index(&table, &pool_guards, 0)
                .lookup(&key, MAX_SNAPSHOT_TS)
                .await
                .unwrap()
            else {
                panic!("row should exist");
            };
            let status = Arc::new(SharedTrxStatus::new(MIN_ACTIVE_TRX_ID + 1));
            table
                .deletion_buffer()
                .put_ref(row_id, status.clone(), MAX_SNAPSHOT_TS)
                .unwrap();

            let mut row_undo = RowUndoLogs::empty();
            row_undo.push(OwnedRowUndo::new(
                table.table_id(),
                None,
                row_id,
                RowUndoKind::Delete,
            ));
            let trx = CommittedTrx {
                cts: TrxID::new(100),
                payload: Some(CommittedTrxPayload {
                    sts: TrxID::new(1),
                    gc_no: 0,
                    row_undo,
                    index_gc: vec![],
                    gc_row_pages: vec![],
                }),
            };
            {
                let pool_guards = full_pool_guards(&engine);
                engine
                    .inner()
                    .trx_sys
                    .purge_trx_list(engine.catalog(), &pool_guards, vec![trx], MAX_SNAPSHOT_TS)
                    .await
                    .unwrap();
            }

            match table.deletion_buffer().get(row_id) {
                Some(DeleteMarker::Ref(actual)) => {
                    assert!(Arc::ptr_eq(&actual, &status));
                }
                Some(DeleteMarker::Committed(_)) => {
                    panic!("uncommitted delete marker should remain as ref")
                }
                None => panic!("delete marker should exist"),
            }
        });
    }

    #[test]
    fn test_purge_promote_delete_marker_if_committed_for_delete_with_missing_page_id() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = EngineConfig::default()
                .storage_root(main_dir)
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .role(PoolRole::Mem)
                        .max_mem_size(64usize * 1024 * 1024)
                        .max_file_size(128usize * 1024 * 1024),
                )
                .trx(
                    TrxSysConfig::default()
                        .purge_threads(1)
                        .log_file_stem("redo_purge_promote_missing_page"),
                )
                .build()
                .await
                .unwrap();

            let table_id = table1(&engine).await;
            let table = engine.catalog().get_table(table_id).await.unwrap();
            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();
            trx.exec(async |stmt| {
                stmt_insert_row(stmt, table_id, vec![Val::from(1003i32)]).await?;
                Ok(())
            })
            .await
            .unwrap();
            trx.commit().await.unwrap();
            drop(session);
            let pool_guards = full_pool_guards(&engine);
            let key = vec![Val::from(1003i32)];
            let Some((row_id, _)) = bound_unique_index(&table, &pool_guards, 0)
                .lookup(&key, MAX_SNAPSHOT_TS)
                .await
                .unwrap()
            else {
                panic!("row should exist");
            };
            let page_id = match table
                .find_row(&pool_guards, row_id)
                .await
                .expect("test row lookup should succeed")
            {
                RowLocation::RowPage(page_id) => page_id,
                RowLocation::LwcBlock { .. } | RowLocation::NotFound => unreachable!(),
            };
            let page_guard = table
                .mem
                .mem_pool()
                .get_page::<RowPage>(
                    &table.mem.mem_pool().pool_guard(),
                    page_id,
                    LatchFallbackMode::Shared,
                )
                .await
                .expect("buffer-pool read failed in test");
            let stale_page_id = VersionedPageID {
                page_id,
                generation: page_guard.bf().generation().saturating_add(1),
            };
            drop(page_guard);
            let status = Arc::new(SharedTrxStatus::new(TrxID::new(100)));
            table
                .deletion_buffer()
                .put_ref(row_id, status.clone(), MAX_SNAPSHOT_TS)
                .unwrap();

            let mut row_undo = RowUndoLogs::empty();
            row_undo.push(OwnedRowUndo::new(
                table.table_id(),
                Some(stale_page_id),
                row_id,
                RowUndoKind::Delete,
            ));
            let trx = CommittedTrx {
                cts: TrxID::new(100),
                payload: Some(CommittedTrxPayload {
                    sts: TrxID::new(1),
                    gc_no: 0,
                    row_undo,
                    index_gc: vec![],
                    gc_row_pages: vec![],
                }),
            };
            {
                let pool_guards = full_pool_guards(&engine);
                engine
                    .inner()
                    .trx_sys
                    .purge_trx_list(engine.catalog(), &pool_guards, vec![trx], MAX_SNAPSHOT_TS)
                    .await
                    .unwrap();
            }

            match table.deletion_buffer().get(row_id) {
                Some(DeleteMarker::Committed(ts)) => assert_eq!(ts, status.ts()),
                Some(DeleteMarker::Ref(_)) => panic!("delete marker should be promoted"),
                None => panic!("delete marker should exist"),
            }
        });
    }

    #[test]
    fn test_purge_skip_promote_delete_marker_if_uncommitted_for_delete_with_missing_page_id() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = EngineConfig::default()
                .storage_root(main_dir)
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .role(PoolRole::Mem)
                        .max_mem_size(64usize * 1024 * 1024)
                        .max_file_size(128usize * 1024 * 1024),
                )
                .trx(
                    TrxSysConfig::default()
                        .purge_threads(1)
                        .log_file_stem("redo_purge_no_promote_missing_page"),
                )
                .build()
                .await
                .unwrap();

            let table_id = table1(&engine).await;
            let table = engine.catalog().get_table(table_id).await.unwrap();
            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();
            trx.exec(async |stmt| {
                stmt_insert_row(stmt, table_id, vec![Val::from(1004i32)]).await?;
                Ok(())
            })
            .await
            .unwrap();
            trx.commit().await.unwrap();
            drop(session);
            let pool_guards = full_pool_guards(&engine);
            let key = vec![Val::from(1004i32)];
            let Some((row_id, _)) = bound_unique_index(&table, &pool_guards, 0)
                .lookup(&key, MAX_SNAPSHOT_TS)
                .await
                .unwrap()
            else {
                panic!("row should exist");
            };
            let page_id = match table
                .find_row(&pool_guards, row_id)
                .await
                .expect("test row lookup should succeed")
            {
                RowLocation::RowPage(page_id) => page_id,
                RowLocation::LwcBlock { .. } | RowLocation::NotFound => unreachable!(),
            };
            let page_guard = table
                .mem
                .mem_pool()
                .get_page::<RowPage>(
                    &table.mem.mem_pool().pool_guard(),
                    page_id,
                    LatchFallbackMode::Shared,
                )
                .await
                .expect("buffer-pool read failed in test");
            let stale_page_id = VersionedPageID {
                page_id,
                generation: page_guard.bf().generation().saturating_add(1),
            };
            drop(page_guard);
            let status = Arc::new(SharedTrxStatus::new(MIN_ACTIVE_TRX_ID + 1));
            table
                .deletion_buffer()
                .put_ref(row_id, status.clone(), MAX_SNAPSHOT_TS)
                .unwrap();

            let mut row_undo = RowUndoLogs::empty();
            row_undo.push(OwnedRowUndo::new(
                table.table_id(),
                Some(stale_page_id),
                row_id,
                RowUndoKind::Delete,
            ));
            let trx = CommittedTrx {
                cts: TrxID::new(100),
                payload: Some(CommittedTrxPayload {
                    sts: TrxID::new(1),
                    gc_no: 0,
                    row_undo,
                    index_gc: vec![],
                    gc_row_pages: vec![],
                }),
            };
            {
                let pool_guards = full_pool_guards(&engine);
                engine
                    .inner()
                    .trx_sys
                    .purge_trx_list(engine.catalog(), &pool_guards, vec![trx], MAX_SNAPSHOT_TS)
                    .await
                    .unwrap();
            }

            match table.deletion_buffer().get(row_id) {
                Some(DeleteMarker::Ref(actual)) => {
                    assert!(Arc::ptr_eq(&actual, &status));
                }
                Some(DeleteMarker::Committed(_)) => {
                    panic!("uncommitted delete marker should remain as ref")
                }
                None => panic!("delete marker should exist"),
            }
        });
    }

    #[test]
    fn test_trx_purge_single_thread() {
        use crate::catalog::tests::table1;

        const PURGE_SIZE: usize = 100;
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = EngineConfig::default()
                .storage_root(main_dir)
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .role(PoolRole::Mem)
                        .max_mem_size(64usize * 1024 * 1024)
                        .max_file_size(128usize * 1024 * 1024),
                )
                .trx(
                    TrxSysConfig::default()
                        .purge_threads(1)
                        .log_file_stem("redo_purge"),
                )
                .build()
                .await
                .unwrap();

            let table_id = table1(&engine).await;

            // Since we populate metadata table, we need to count those purge transactions and rows.
            // 100ms should be enough.
            Timer::after(Duration::from_secs(1)).await;
            let init_stats = engine.inner().trx_sys.trx_sys_stats();

            let mut session = engine.new_session().unwrap();
            // insert
            for i in 0..PURGE_SIZE {
                let mut trx = session.begin_trx().unwrap();
                let res = trx
                    .exec(async |stmt| {
                        stmt_insert_row(stmt, table_id, vec![Val::from(i as i32)]).await?;
                        Ok(())
                    })
                    .await;
                assert!(res.is_ok());
                let res = trx.commit().await;
                assert!(res.is_ok());
            }
            // delete
            for i in 0..PURGE_SIZE {
                let mut trx = session.begin_trx().unwrap();
                let key = SelectKey::new(0, vec![Val::from(i as i32)]);
                let res = trx
                    .exec(async |stmt| {
                        stmt_delete_row(stmt, table_id, &key).await?;
                        Ok(())
                    })
                    .await;
                assert!(res.is_ok());
                let res = trx.commit().await;
                assert!(res.is_ok());
            }

            // wait for GC.
            let start = Instant::now();
            loop {
                let stats = engine.inner().trx_sys.trx_sys_stats();
                assert!(stats.purge_trx_count <= init_stats.purge_trx_count + PURGE_SIZE * 2);
                assert!(stats.purge_row_count <= init_stats.purge_row_count + PURGE_SIZE * 2);
                assert!(stats.purge_index_count <= init_stats.purge_index_count + PURGE_SIZE);
                println!(
                    "purge_trx={},purge_row={},purge_index={}",
                    stats.purge_trx_count, stats.purge_row_count, stats.purge_index_count
                );
                if stats.purge_trx_count >= init_stats.purge_trx_count + PURGE_SIZE * 2
                    && stats.purge_row_count >= init_stats.purge_row_count + PURGE_SIZE * 2
                    && stats.purge_index_count >= init_stats.purge_index_count + PURGE_SIZE
                {
                    break;
                }
                if start.elapsed() >= Duration::from_secs(1) {
                    panic!("gc timeout");
                } else {
                    sleep(Duration::from_millis(100));
                }
            }
            drop(session);
        });
    }

    #[test]
    fn test_trx_purge_multi_threads() {
        use crate::catalog::tests::table1;

        smol::block_on(async {
            const PURGE_SIZE: usize = 100;
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = EngineConfig::default()
                .storage_root(main_dir)
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .role(PoolRole::Mem)
                        .max_mem_size(64usize * 1024 * 1024)
                        .max_file_size(128usize * 1024 * 1024),
                )
                .trx(
                    TrxSysConfig::default()
                        .purge_threads(2)
                        .log_file_stem("redo_purge"),
                )
                .build()
                .await
                .unwrap();

            let table_id = table1(&engine).await;

            // Since we populate metadata table, we need to count those purge transactions and rows.
            // 100ms should be enough.
            Timer::after(Duration::from_millis(100)).await;
            let init_stats = engine.inner().trx_sys.trx_sys_stats();

            let mut session = engine.new_session().unwrap();
            // insert
            for i in 0..PURGE_SIZE {
                let mut trx = session.begin_trx().unwrap();
                let res = trx
                    .exec(async |stmt| {
                        stmt_insert_row(stmt, table_id, vec![Val::from(i as i32)]).await?;
                        Ok(())
                    })
                    .await;
                assert!(res.is_ok());
                let res = trx.commit().await;
                assert!(res.is_ok());
            }
            // delete
            for i in 0..PURGE_SIZE {
                let mut trx = session.begin_trx().unwrap();
                let key = SelectKey::new(0, vec![Val::from(i as i32)]);
                let res = trx
                    .exec(async |stmt| {
                        stmt_delete_row(stmt, table_id, &key).await?;
                        Ok(())
                    })
                    .await;
                assert!(res.is_ok());
                let res = trx.commit().await;
                assert!(res.is_ok());
            }

            // wait for GC.
            let start = Instant::now();
            let mut gc_timeout = false;
            loop {
                let stats = engine.inner().trx_sys.trx_sys_stats();
                assert!(stats.purge_trx_count <= init_stats.purge_trx_count + PURGE_SIZE * 2);
                assert!(stats.purge_row_count <= init_stats.purge_row_count + PURGE_SIZE * 2);
                assert!(stats.purge_index_count <= init_stats.purge_index_count + PURGE_SIZE);
                println!(
                    "purge_trx={},purge_row={},purge_index={}",
                    stats.purge_trx_count, stats.purge_row_count, stats.purge_index_count
                );
                if stats.purge_trx_count == init_stats.purge_trx_count + PURGE_SIZE * 2
                    && stats.purge_row_count == init_stats.purge_row_count + PURGE_SIZE * 2
                    && stats.purge_index_count == init_stats.purge_index_count + PURGE_SIZE
                {
                    break;
                }
                if start.elapsed() >= Duration::from_secs(1) {
                    // panic!("gc timeout");
                    gc_timeout = true;
                    break;
                } else {
                    sleep(Duration::from_millis(100));
                }
            }
            if gc_timeout {
                // see which one is not purged, and its cts.
                let table = engine.catalog().get_table(table_id).await.unwrap();
                let pool_guards = full_pool_guards(&engine);
                let index = bound_unique_index(&table, &pool_guards, 0);
                let mut remained_row_ids = vec![];
                index
                    .scan_values(&mut remained_row_ids, TrxID::new(100))
                    .await
                    .unwrap();
                println!("gc timeout, remained_row_ids={:?}", remained_row_ids);
                let row_id = remained_row_ids[0];
                let location = table
                    .find_row(&pool_guards, row_id)
                    .await
                    .expect("test row lookup should succeed");
                let page_id = match location {
                    RowLocation::RowPage(page_id) => page_id,
                    _ => unreachable!(),
                };
                let mem_guard = engine.inner().mem_pool.pool_guard();
                let page_guard: PageSharedGuard<RowPage> = engine
                    .inner()
                    .mem_pool
                    .get_page(&mem_guard, page_id, LatchFallbackMode::Shared)
                    .await
                    .expect("buffer-pool read failed in test")
                    .lock_shared_async()
                    .await
                    .unwrap();
                let (ctx, page) = page_guard.ctx_and_page();
                let access = RowReadAccess::new(page, ctx, page.row_idx(row_id));
                let ts = access.ts();
                println!("row ts={:?}", ts);
            }
            println!(
                "final min_active_sts={}",
                engine.inner().trx_sys.global_visible_sts()
            );
            drop(session);
        });
    }
}
