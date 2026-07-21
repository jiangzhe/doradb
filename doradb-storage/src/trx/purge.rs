use crate::buffer::PoolGuards;
use crate::buffer::guard::{PageGuard, PageSharedGuard};
use crate::catalog::{
    Catalog, DroppedTableFileCleanup, DroppedTableRuntime, TableCache, is_catalog_table,
};
use crate::error::{FatalError, InternalError, Result, RuntimeError, RuntimeResult};
use crate::file::table_file::OldRoot;
use crate::id::TrxID;
use crate::map::{FastHashMap, FastHashSet};
use crate::obs;
use crate::quiescent::{QuiescentGuard, SyncQuiescentGuard};
use crate::row::RowPage;
use crate::runtime;
use crate::table::Table;
use crate::thread;
use crate::trx::sys::TransactionSystem;
use crate::trx::undo::{OwnedRowUndo, RowUndoKind};
use crate::trx::{CommittedTrx, MAX_SNAPSHOT_TS, RetiredRowPageBatch};
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
    /// Install a per-engine purge scheduling observer for deterministic tests.
    #[cfg(test)]
    #[inline]
    pub(crate) fn set_purge_test_observer(&self, observer: Sender<PurgeTestEvent>) {
        self.set_purge_test_hook(Arc::new(move |event| {
            let _ = observer.send(event);
            PurgeTestAction::Continue
        }));
    }

    /// Install a per-engine purge scheduling control hook for deterministic tests.
    #[cfg(test)]
    #[inline]
    pub(crate) fn set_purge_test_hook(&self, hook: PurgeTestHook) {
        self.purge_test_hook.lock().replace(hook);
    }

    #[cfg(test)]
    #[inline]
    fn run_purge_test_hook(&self, event: PurgeTestEvent) -> PurgeTestAction {
        let hook = self.purge_test_hook.lock().clone();
        hook.map_or(PurgeTestAction::Continue, |hook| hook(event))
    }

    #[cfg(test)]
    #[inline]
    fn observe_purge_test_event(&self, event: PurgeTestEvent) {
        let _ = self.run_purge_test_hook(event);
    }

    /// Request one coalescible full-purge observation.
    ///
    /// `Purge::FullObservation` is the payload-free signal for observing all
    /// authoritative purge state and scheduling every eligible cleanup class.
    /// Bucket and retained-work state remain authoritative, so this best-effort
    /// wake can be dropped or coalesced during shutdown without losing payload.
    #[inline]
    pub(crate) fn request_purge_observation(&self) {
        let _ = self.purge_tx.send(Purge::FullObservation);
    }

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

    /// Start exactly the configured number of purge-bucket worker threads.
    ///
    /// The dispatcher is worker slot zero and the remaining configured slots
    /// are executor threads.
    #[inline]
    pub(crate) fn start_purge_threads(
        trx_sys: QuiescentGuard<Self>,
        pool_guards: PoolGuards,
        purge_chan: Receiver<Purge>,
    ) -> RuntimeResult<Vec<JoinHandle<()>>> {
        let trx_sys = trx_sys.into_sync();
        let (mut dispatcher, executors) = Self::dispatch_purge(trx_sys.clone(), &pool_guards)?;
        let task_trx_sys = trx_sys.clone();
        let handle = match thread::spawn_named("Purge-Dispatcher", move || {
            runtime::block_on(dispatcher.purge_loop(&task_trx_sys, pool_guards, purge_chan));
        }) {
            Ok(handle) => handle,
            Err(report) => {
                // PurgeDispatcher has been dropped so executors should quit quickly.
                return Err(reclaim_partial_purge_workers(
                    report,
                    executors,
                    "rollback_purge_dispatcher_spawn",
                ));
            }
        };
        let mut handles = Vec::with_capacity(executors.len() + 1);
        handles.push(handle);
        handles.extend(executors);
        Ok(handles)
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
    /// overtaken by a separate stop marker. The returned progress keeps active
    /// horizon movement independent from system-payload eligibility.
    #[inline]
    fn record_committed_for_purge(
        &self,
        trx_list: FastHashMap<usize, Vec<CommittedTrx>>,
    ) -> CommittedPurgeProgress {
        let mut progress = CommittedPurgeProgress::default();
        for (gc_no, trx_list) in trx_list {
            let gc_bucket = &self.gc_buckets[gc_no];
            let bucket_progress = gc_bucket.record_committed_for_purge(trx_list);
            progress.merge_bucket(bucket_progress);
        }
        #[cfg(test)]
        self.observe_purge_test_event(PurgeTestEvent::CommittedRecorded {
            min_original_sts: progress.min_original_sts,
            min_system_cts: progress.min_system_cts,
        });
        progress
    }

    /// Record rollback progress and report the causal active-STS transition.
    ///
    /// Commit handoffs are non-lossy through `Purge::Committed` because purge
    /// must retain every committed payload batch. Rollback observations are
    /// lossy: bucket state is already authoritative, and the transition is only
    /// a coalescible scheduling observation.
    #[inline]
    pub(crate) fn record_rollback_for_purge(&self, gc_no: usize, sts: TrxID) -> bool {
        let progress = self.gc_buckets[gc_no].record_rollback_for_purge(sts);
        if let Some(progress) = progress {
            let _ = self.purge_tx.send(Purge::ActiveSts(progress));
        }
        progress.is_some()
    }

    #[inline]
    pub(super) fn dispatch_purge(
        trx_sys: SyncQuiescentGuard<Self>,
        pool_guards: &PoolGuards,
    ) -> RuntimeResult<(PurgeDispatcher, Vec<JoinHandle<()>>)> {
        let mut handles = vec![];
        let mut chans = vec![];
        for worker_slot in 1..trx_sys.config.purge_threads {
            let (tx, rx) = flume::unbounded();
            chans.push(tx);
            let thread_name = format!("Purge-Executor-{worker_slot}");
            let pool_guards = pool_guards.clone();
            let task_trx_sys = trx_sys.clone();
            let handle = match thread::spawn_named(thread_name, move || {
                runtime::block_on(purge_executor(
                    &task_trx_sys.catalog,
                    &task_trx_sys,
                    pool_guards,
                    rx,
                ));
            }) {
                Ok(handle) => handle,
                Err(report) => {
                    drop(chans); // drop channels to let all executors quit.
                    return Err(reclaim_partial_purge_workers(
                        report,
                        handles,
                        "rollback_purge_executor_spawn",
                    ));
                }
            };
            handles.push(handle);
        }
        Ok((PurgeDispatcher(chans), handles))
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
    ) -> Result<Vec<RetiredRowPageBatch>> {
        match self
            .purge_trx_list_inner(catalog, guards, trx_list, min_active_sts)
            .await
        {
            Ok(retired_row_pages) => Ok(retired_row_pages),
            Err(err) => {
                let report = err
                    .change_context(FatalError::PurgeAccess)
                    .attach("purge transaction-list access failed");
                obs::error!(
                    "event=engine_poison component=purge action=poison result=error error={:?}",
                    report
                );
                Err(self.poisoner.poison(report).into())
            }
        }
    }

    /// Execute one GC bucket through the common purge access boundary.
    #[inline]
    async fn purge_gc_bucket(
        &self,
        catalog: &Catalog,
        guards: &PoolGuards,
        gc_no: usize,
        min_active_sts: TrxID,
    ) -> Result<Vec<RetiredRowPageBatch>> {
        #[cfg(test)]
        if self.run_purge_test_hook(PurgeTestEvent::BucketStarted { gc_no })
            == PurgeTestAction::Fail
        {
            self.observe_purge_test_event(PurgeTestEvent::BucketFailed { gc_no });
            let report = Report::new(FatalError::PurgeAccess)
                .attach(format!("purge test hook failed bucket: gc_no={gc_no}"));
            return Err(self.poisoner.poison(report).into());
        }
        let mut trx_list = Vec::new();
        self.gc_buckets[gc_no].get_purge_list(min_active_sts, &mut trx_list);
        let result = self
            .purge_trx_list(catalog, guards, trx_list, min_active_sts)
            .await;
        #[cfg(test)]
        match &result {
            Ok(_) => self.observe_purge_test_event(PurgeTestEvent::BucketCompleted { gc_no }),
            Err(_) => self.observe_purge_test_event(PurgeTestEvent::BucketFailed { gc_no }),
        }
        result
    }

    async fn purge_trx_list_inner(
        &self,
        catalog: &Catalog,
        guards: &PoolGuards,
        trx_list: Vec<CommittedTrx>,
        min_active_sts: TrxID,
    ) -> RuntimeResult<Vec<RetiredRowPageBatch>> {
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
                        let Some(table) = table_cache.get_user_table(undo.table_id).await else {
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
                        let Some(table) = table_cache.get_user_entry_mut(ip.table_id).await else {
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
        let retired_row_pages = trx_list
            .into_iter()
            .filter_map(CommittedTrx::into_retired_row_pages)
            .collect();

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
        Ok(retired_row_pages)
    }

    #[inline]
    async fn process_retired_row_pages(
        &self,
        catalog: &Catalog,
        guards: &PoolGuards,
        retired_row_pages: Vec<RetiredRowPageBatch>,
    ) -> bool {
        for batch in retired_row_pages {
            let Some(table) = catalog.pin_user_table_for_purge(batch.table_id) else {
                let report = Report::new(FatalError::PurgeAccess).attach(format!(
                    "checkpoint retirement runtime missing: table_id={}, start_row_id={}, end_row_id={}",
                    batch.table_id, batch.start_row_id, batch.end_row_id
                ));
                obs::error!(
                    "event=engine_poison component=purge action=poison result=error error={:?}",
                    report
                );
                let _ = self.poisoner.poison(report);
                return false;
            };
            let page_ids = match table.mem.unlink_retired_row_pages(guards, &batch).await {
                Ok(page_ids) if page_ids.as_ref() == batch.page_ids.as_ref() => page_ids,
                Ok(_) | Err(_) => {
                    let report = Report::new(FatalError::PurgeAccess).attach(format!(
                        "checkpoint retirement prefix unlink failed: table_id={}, start_row_id={}, end_row_id={}",
                        batch.table_id, batch.start_row_id, batch.end_row_id
                    ));
                    obs::error!(
                        "event=engine_poison component=purge action=poison result=error error={:?}",
                        report
                    );
                    let _ = self.poisoner.poison(report);
                    return false;
                }
            };
            if table
                .mem
                .deallocate_retired_row_pages(guards, &page_ids)
                .await
                .is_err()
            {
                let report = Report::new(FatalError::PurgeDeallocate).attach(format!(
                    "checkpoint retirement row-page deallocation failed: table_id={}, start_row_id={}, end_row_id={}",
                    batch.table_id, batch.start_row_id, batch.end_row_id
                ));
                obs::error!(
                    "event=engine_poison component=purge action=poison result=error error={:?}",
                    report
                );
                let _ = self.poisoner.poison(report);
                return false;
            }
        }
        true
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

    #[inline]
    fn process_dropped_table_file_deletes(&self) {
        // File deletion is a checkpoint-gated housekeeping step. Unlike runtime
        // destroy, unlink failure is retryable and does not poison the engine:
        // retain the catalog floor entry and requeue failed ready items so a
        // later purge wake can retry without scanning every catalog table.
        let catalog_replay_start_ts = self
            .catalog
            .storage
            .checkpoint_snapshot()
            .catalog_replay_start_ts;
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

/// One causal change to a GC bucket's cached active-STS minimum.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ActiveStsProgress {
    /// Removing the old minimum left the bucket without an active transaction.
    NoActive { original_sts: TrxID },
    /// Removing the old minimum exposed a later active transaction.
    Advance { original_sts: TrxID, new_sts: TrxID },
}

impl ActiveStsProgress {
    #[inline]
    const fn original_sts(self) -> TrxID {
        match self {
            ActiveStsProgress::NoActive { original_sts }
            | ActiveStsProgress::Advance { original_sts, .. } => original_sts,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct GCBucketPurgeProgress {
    active_sts: Option<ActiveStsProgress>,
    min_system_cts: Option<TrxID>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct CommittedPurgeProgress {
    min_original_sts: Option<TrxID>,
    min_system_cts: Option<TrxID>,
}

impl CommittedPurgeProgress {
    #[inline]
    fn merge_bucket(&mut self, progress: GCBucketPurgeProgress) {
        if let Some(active_sts) = progress.active_sts {
            self.merge_original_sts(active_sts.original_sts());
        }
        if let Some(system_cts) = progress.min_system_cts {
            self.merge_system_cts(system_cts);
        }
    }

    #[inline]
    fn merge_original_sts(&mut self, sts: TrxID) {
        self.min_original_sts = Some(
            self.min_original_sts
                .map_or(sts, |current| current.min(sts)),
        );
    }

    #[inline]
    fn merge_system_cts(&mut self, cts: TrxID) {
        self.min_system_cts = Some(self.min_system_cts.map_or(cts, |current| current.min(cts)));
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
    pub(super) fn record_rollback_for_purge(&self, sts: TrxID) -> Option<ActiveStsProgress> {
        let mut active_sts_list = self.active_sts_list.lock();
        let min_sts = active_sts_list.remove(sts);
        self.update_min_active_sts(min_sts)
    }

    /// Record committed transactions for purge.
    #[inline]
    pub(super) fn record_committed_for_purge(
        &self,
        trx_list: Vec<CommittedTrx>,
    ) -> GCBucketPurgeProgress {
        // Update both active sts list and committed transaction list
        let mut active_sts_list = self.active_sts_list.lock();
        let mut min_sts = MAX_SNAPSHOT_TS;
        let mut removed_active_sts = false;
        let mut min_system_cts: Option<TrxID> = None;
        {
            let mut committed_trx_list = self.committed_trx_list.lock();
            for trx in trx_list {
                if let Some(sts) = trx.sts() {
                    min_sts = active_sts_list.remove(sts);
                    removed_active_sts = true;
                } else {
                    min_system_cts =
                        Some(min_system_cts.map_or(trx.cts, |current| current.min(trx.cts)));
                }
                committed_trx_list.push_back(trx);
            }
        }
        let active_sts = removed_active_sts
            .then(|| self.update_min_active_sts(min_sts))
            .flatten();
        GCBucketPurgeProgress {
            active_sts,
            min_system_cts,
        }
    }

    #[inline]
    fn update_min_active_sts(&self, min_sts: TrxID) -> Option<ActiveStsProgress> {
        // Because we just commit/rollback at least one transaction in this bucket, that means there must be
        // some transaction in the list before, so current value of min_active_sts must not be MAX.
        let original_sts = TrxID::new(self.min_active_sts.load(Ordering::Relaxed));
        debug_assert_ne!(original_sts, MAX_SNAPSHOT_TS);

        if min_sts == original_sts {
            return None;
        }
        debug_assert!(original_sts < min_sts);

        // There is no active transaction. We should update min_active_sts.
        if min_sts == MAX_SNAPSHOT_TS {
            self.min_active_sts
                .store(min_sts.as_u64(), Ordering::Relaxed);
            return Some(ActiveStsProgress::NoActive { original_sts });
        }

        debug_assert!(min_sts < MAX_SNAPSHOT_TS);
        self.min_active_sts
            .store(min_sts.as_u64(), Ordering::Relaxed);
        Some(ActiveStsProgress::Advance {
            original_sts,
            new_sts: min_sts,
        })
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
    /// Lossy, payload-free observation of one changed bucket minimum.
    ///
    /// The bucket atomics remain authoritative. This transition only preserves
    /// the original blocker needed to decide whether fresh global progress
    /// crossed it.
    ActiveSts(ActiveStsProgress),
    /// Lossy, payload-free request to observe all purge state and run full cleanup.
    FullObservation,
    /// Release retained table roots whose post-publish readers have drained.
    TableRootRetention,
    /// Run only dropped-table runtime/file cleanup.
    DroppedTable,
    /// Test-only system-CTS scheduling input without a retirement payload.
    #[cfg(test)]
    TestSystemCts(TrxID),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PurgeWork {
    min_original_sts: Option<TrxID>,
    min_system_cts: Option<TrxID>,
    full_observation: bool,
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
            min_original_sts: None,
            min_system_cts: None,
            full_observation: false,
            table_root_retention: false,
            dropped_table: false,
            stop_after: false,
        }
    }

    #[inline]
    #[cfg(test)]
    const fn table_root_retention() -> Self {
        Self {
            min_original_sts: None,
            min_system_cts: None,
            full_observation: false,
            table_root_retention: true,
            dropped_table: false,
            stop_after: false,
        }
    }

    #[inline]
    const fn stop() -> Self {
        Self {
            min_original_sts: None,
            min_system_cts: None,
            full_observation: false,
            table_root_retention: false,
            dropped_table: false,
            stop_after: true,
        }
    }

    #[inline]
    fn merge_original_sts(&mut self, sts: TrxID) {
        self.min_original_sts = Some(
            self.min_original_sts
                .map_or(sts, |current| current.min(sts)),
        );
    }

    #[inline]
    fn merge_system_cts(&mut self, cts: TrxID) {
        self.min_system_cts = Some(self.min_system_cts.map_or(cts, |current| current.min(cts)));
    }

    #[inline]
    const fn needs_observation(self) -> bool {
        self.min_original_sts.is_some()
            || self.min_system_cts.is_some()
            || self.full_observation
            || self.table_root_retention
            || self.dropped_table
    }

    #[inline]
    fn absorb<F>(&mut self, purge: Purge, analyze_committed: &mut F) -> bool
    where
        F: FnMut(FastHashMap<usize, Vec<CommittedTrx>>) -> CommittedPurgeProgress,
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
                let progress = analyze_committed(trx_list);
                if let Some(original_sts) = progress.min_original_sts {
                    self.merge_original_sts(original_sts);
                }
                if let Some(system_cts) = progress.min_system_cts {
                    self.merge_system_cts(system_cts);
                }
            }
            Purge::ActiveSts(progress) => self.merge_original_sts(progress.original_sts()),
            Purge::FullObservation => {
                self.full_observation = true;
                self.table_root_retention = true;
                self.dropped_table = true;
            }
            Purge::TableRootRetention => self.table_root_retention = true,
            Purge::DroppedTable => self.dropped_table = true,
            #[cfg(test)]
            Purge::TestSystemCts(cts) => self.merge_system_cts(cts),
        }
        true
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PurgeCyclePlan {
    transaction_gc: bool,
    table_root_retention: bool,
    dropped_table: bool,
    advance_completed_horizon: bool,
}

/// Test-only action returned by a purge scheduling hook.
#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PurgeTestAction {
    /// Continue normal purge execution.
    Continue,
    /// Fail the selected bucket before its committed prefix is drained.
    Fail,
}

/// Test-only purge scheduling callback.
#[cfg(test)]
pub(crate) type PurgeTestHook = Arc<dyn Fn(PurgeTestEvent) -> PurgeTestAction + Send + Sync>;

/// Narrow purge scheduling events used by deterministic concurrency tests.
#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PurgeTestEvent {
    /// The coordinator is about to scan authoritative bucket minima.
    PreScan,
    /// One committed handoff was fully recorded in authoritative buckets.
    CommittedRecorded {
        min_original_sts: Option<TrxID>,
        min_system_cts: Option<TrxID>,
    },
    /// One coalesced work item was planned.
    Planned {
        /// Whether all GC buckets were selected.
        transaction_gc: bool,
        /// Whether success may advance completed-horizon progress.
        advance_completed_horizon: bool,
    },
    /// One remote bucket task was successfully enqueued.
    RemoteEnqueued { gc_no: usize, worker_slot: usize },
    /// One bucket is about to drain its eligible committed prefix.
    BucketStarted { gc_no: usize },
    /// One bucket completed successfully.
    BucketCompleted { gc_no: usize },
    /// One bucket failed before the cycle barrier completed.
    BucketFailed { gc_no: usize },
    /// The dispatcher started one local bucket.
    LocalStarted { gc_no: usize },
    /// The dispatcher completed one local bucket successfully.
    LocalCompleted { gc_no: usize },
    /// The dispatcher received one successful remote bucket result.
    RemoteCompleted { gc_no: usize },
    /// All selected bucket results passed and retirement processing is starting.
    RetirementStarted,
    /// Retained-root housekeeping is starting.
    TableRootRetentionStarted,
    /// Dropped-table housekeeping is starting.
    DroppedTableStarted,
    /// The completed purge horizon was published after cycle success.
    CompletedHorizonPublished { sts: TrxID },
    /// One planned cycle finished all selected work.
    CycleCompleted,
}

struct PurgeTask {
    gc_no: usize,
    min_active_sts: TrxID,
    done: Sender<PurgeTaskResult>,
}

struct PurgeTaskResult {
    gc_no: usize,
    result: Result<Vec<RetiredRowPageBatch>>,
}

/// Purge coordinator and worker-slot-zero executor.
pub(crate) struct PurgeDispatcher(Vec<Sender<PurgeTask>>);

impl PurgeDispatcher {
    #[inline]
    async fn purge_loop(
        &mut self,
        trx_sys: &TransactionSystem,
        pool_guards: PoolGuards,
        purge_chan: Receiver<Purge>,
    ) {
        let mut completed_horizon = trx_sys.global_visible_sts();
        'DISPATCH_LOOP: while let Ok(purge) = purge_chan.recv_async().await {
            let work = coalesce_purge_work(&purge_chan, purge, |trx_list| {
                trx_sys.record_committed_for_purge(trx_list)
            });
            if work.stop_after {
                break 'DISPATCH_LOOP;
            }
            if !work.needs_observation() {
                continue;
            }
            #[cfg(test)]
            trx_sys.observe_purge_test_event(PurgeTestEvent::PreScan);
            let curr_sts = trx_sys.calc_min_active_sts_for_gc();
            trx_sys.publish_gc_horizon(curr_sts);
            let plan = plan_purge_cycle(work, completed_horizon, curr_sts);
            #[cfg(test)]
            trx_sys.observe_purge_test_event(PurgeTestEvent::Planned {
                transaction_gc: plan.transaction_gc,
                advance_completed_horizon: plan.advance_completed_horizon,
            });
            if plan.transaction_gc {
                let worker_count = self.0.len() + 1;
                // Enqueue every remote bucket before the dispatcher starts its
                // own deterministic worker-slot share. With no executors this
                // skips completion-channel creation entirely.
                let remote_results = if self.0.is_empty() {
                    None
                } else {
                    let (done_tx, done_rx) = flume::unbounded();
                    let mut expected_remote_tasks = 0usize;
                    for gc_no in 0..trx_sys.gc_buckets.len() {
                        let worker_slot = purge_worker_slot(gc_no, worker_count);
                        if worker_slot == 0 {
                            continue;
                        }
                        let task = PurgeTask {
                            gc_no,
                            min_active_sts: curr_sts,
                            done: done_tx.clone(),
                        };
                        self.0[worker_slot - 1].send(task).expect(
                            "purge executor receiver must stay alive while dispatcher owns sender",
                        );
                        #[cfg(test)]
                        trx_sys.observe_purge_test_event(PurgeTestEvent::RemoteEnqueued {
                            gc_no,
                            worker_slot,
                        });
                        expected_remote_tasks += 1;
                    }
                    drop(done_tx);
                    Some((done_rx, expected_remote_tasks))
                };
                let mut bucket_results = Vec::with_capacity(trx_sys.gc_buckets.len());
                for gc_no in (0..trx_sys.gc_buckets.len()).step_by(worker_count) {
                    #[cfg(test)]
                    trx_sys.observe_purge_test_event(PurgeTestEvent::LocalStarted { gc_no });
                    let result = trx_sys
                        .purge_gc_bucket(&trx_sys.catalog, &pool_guards, gc_no, curr_sts)
                        .await;
                    match result {
                        Ok(retired_row_pages) => {
                            bucket_results.push((gc_no, retired_row_pages));
                            #[cfg(test)]
                            trx_sys
                                .observe_purge_test_event(PurgeTestEvent::LocalCompleted { gc_no });
                        }
                        Err(_) => return,
                    }
                }
                // Wait for every remote bucket's undo/index work before the
                // dispatcher deallocates any retired page.
                if let Some((done_rx, expected_remote_tasks)) = remote_results {
                    for _ in 0..expected_remote_tasks {
                        match done_rx.recv_async().await {
                            Ok(PurgeTaskResult {
                                gc_no,
                                result: Ok(retired_row_pages),
                            }) => {
                                bucket_results.push((gc_no, retired_row_pages));
                                #[cfg(test)]
                                trx_sys.observe_purge_test_event(PurgeTestEvent::RemoteCompleted {
                                    gc_no,
                                });
                            }
                            Ok(PurgeTaskResult { result: Err(_), .. }) => return,
                            Err(_) => break 'DISPATCH_LOOP,
                        }
                    }
                }
                let retired_row_pages =
                    merge_bucket_results(bucket_results, trx_sys.gc_buckets.len());
                #[cfg(test)]
                trx_sys.observe_purge_test_event(PurgeTestEvent::RetirementStarted);
                if !trx_sys
                    .process_retired_row_pages(&trx_sys.catalog, &pool_guards, retired_row_pages)
                    .await
                {
                    // Poison is the durable-consistency boundary here. The
                    // dispatcher exits; once the worker closure drops the
                    // dispatcher value, executor task channels close and
                    // executor threads can exit before shutdown joins them.
                    return;
                }
            }
            if plan.table_root_retention {
                #[cfg(test)]
                trx_sys.observe_purge_test_event(PurgeTestEvent::TableRootRetentionStarted);
                trx_sys.process_retained_table_roots(curr_sts);
            }
            if plan.dropped_table {
                #[cfg(test)]
                trx_sys.observe_purge_test_event(PurgeTestEvent::DroppedTableStarted);
                if trx_sys
                    .process_dropped_table_gc(&pool_guards, curr_sts)
                    .await
                    .is_err()
                {
                    let report = Report::new(FatalError::PurgeDeallocate)
                        .attach("dropped-table garbage collection failed");
                    obs::error!(
                        "event=engine_poison component=purge action=poison result=error error={:?}",
                        report
                    );
                    let _ = trx_sys.poisoner.poison(report);
                    // Dropped-table destroy failure is fatal; exiting also closes
                    // the executor task channels when this dispatcher is dropped.
                    return;
                }
            }
            if plan.advance_completed_horizon {
                // Once GC is finished, update global_visible_sts so other threads can use it to
                // speed up visibility check.
                trx_sys.update_global_visible_sts(curr_sts);
                completed_horizon = curr_sts;
                #[cfg(test)]
                trx_sys.observe_purge_test_event(PurgeTestEvent::CompletedHorizonPublished {
                    sts: curr_sts,
                });
            }
            #[cfg(test)]
            trx_sys.observe_purge_test_event(PurgeTestEvent::CycleCompleted);
        }

        // Notify executors to quit after a normal Stop or channel close. Fatal
        // poison returns above; then the worker closure drops the dispatcher and
        // closes these same task channels.
        self.0.clear();
    }
}

/// Executes dispatched purge tasks until the task channel closes.
#[inline]
async fn purge_executor(
    catalog: &Catalog,
    trx_sys: &TransactionSystem,
    pool_guards: PoolGuards,
    purge_chan: Receiver<PurgeTask>,
) {
    while let Ok(PurgeTask {
        gc_no,
        min_active_sts,
        done,
    }) = purge_chan.recv_async().await
    {
        let result = trx_sys
            .purge_gc_bucket(catalog, &pool_guards, gc_no, min_active_sts)
            .await;
        let _ = done.send(PurgeTaskResult { gc_no, result });
    }
}

#[inline]
fn reclaim_partial_purge_workers(
    mut report: Report<RuntimeError>,
    handles: Vec<JoinHandle<()>>,
    phase: &'static str,
) -> Report<RuntimeError> {
    let mut join_panics = 0usize;
    for handle in handles {
        if handle.join().is_err() {
            join_panics += 1;
        }
    }
    if join_panics != 0 {
        report = report.attach(format!(
            "phase={phase}, cleanup=join_partial_purge_workers, join_panics={join_panics}"
        ));
    }
    report
}

#[inline]
fn plan_purge_cycle(
    work: PurgeWork,
    completed_horizon: TrxID,
    current_horizon: TrxID,
) -> PurgeCyclePlan {
    let horizon_newer = current_horizon > completed_horizon;
    let active_crossed = horizon_newer
        && work
            .min_original_sts
            .is_some_and(|original_sts| original_sts < current_horizon);
    let explicit_observation_advanced = horizon_newer && work.full_observation;
    let system_eligible = work
        .min_system_cts
        .is_some_and(|system_cts| system_cts < current_horizon);
    let horizon_cycle =
        active_crossed || explicit_observation_advanced || (horizon_newer && system_eligible);
    PurgeCyclePlan {
        transaction_gc: horizon_cycle || system_eligible,
        table_root_retention: work.table_root_retention || horizon_cycle,
        dropped_table: work.dropped_table || horizon_cycle,
        advance_completed_horizon: horizon_cycle,
    }
}

#[inline]
const fn purge_worker_slot(gc_no: usize, worker_count: usize) -> usize {
    gc_no % worker_count
}

#[inline]
fn merge_bucket_results(
    mut bucket_results: Vec<(usize, Vec<RetiredRowPageBatch>)>,
    expected_buckets: usize,
) -> Vec<RetiredRowPageBatch> {
    debug_assert_eq!(bucket_results.len(), expected_buckets);
    bucket_results.sort_unstable_by_key(|(gc_no, _)| *gc_no);
    bucket_results
        .into_iter()
        .flat_map(|(_, retired_row_pages)| retired_row_pages)
        .collect()
}

#[inline]
fn coalesce_purge_work<F>(
    purge_chan: &Receiver<Purge>,
    initial: Purge,
    mut analyze_committed: F,
) -> PurgeWork
where
    F: FnMut(FastHashMap<usize, Vec<CommittedTrx>>) -> CommittedPurgeProgress,
{
    // Collapse queued lossy observations and targeted wakeups so bursts do not
    // trigger repeated scans, while committed payload batches are recorded one
    // by one and never coalesced away. `Stop` is a terminal shutdown barrier:
    // messages before it have been absorbed, messages after it are intentionally
    // ignored.
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
    let page = page_guard.page();
    if !page.row_id_in_valid_range(undo.row_id) {
        return;
    }
    let mut access = page_guard.write_row_by_id(undo.row_id);
    access.purge_undo_chain(min_active_sts);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::page::VersionedPageID;
    use crate::buffer::{BufferPool, PoolGuards, PoolRole};
    use crate::catalog::tests::table1;
    use crate::conf::{DEFAULT_GC_BUCKETS, EngineConfig, EvictableBufferPoolConfig, TrxSysConfig};
    use crate::engine::Engine;
    use crate::error::{FatalError, Result};
    use crate::id::{PageID, RowID, TableID};
    use crate::index::{RowLocation, UniqueIndex};
    use crate::latch::LatchFallbackMode;
    use crate::row::RowPage;
    use crate::row::ops::{DeleteMvcc, SelectKey};
    use crate::table::tests::bound_unique_index;
    use crate::table::{DeleteMarker, TableRedoReplayFloor};
    use crate::trx::stmt::Statement;
    use crate::trx::undo::{OwnedRowUndo, RowUndoKind, RowUndoLogs};
    use crate::trx::{CommittedTrxPayload, MIN_ACTIVE_TRX_ID, SharedTrxStatus, SysTrxPayload};
    use crate::value::Val;
    use std::path::Path;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
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

    fn purge_test_engine_config(
        storage_root: &Path,
        log_file_stem: &str,
        gc_buckets: usize,
        purge_threads: usize,
    ) -> EngineConfig {
        EngineConfig::default()
            .storage_root(storage_root.to_path_buf())
            .data_buffer(
                EvictableBufferPoolConfig::default()
                    .role(PoolRole::Mem)
                    .max_mem_size(64usize * 1024 * 1024)
                    .max_file_size(128usize * 1024 * 1024),
            )
            .trx(
                TrxSysConfig::default()
                    .gc_buckets(gc_buckets)
                    .purge_threads(purge_threads)
                    .log_file_stem(log_file_stem),
            )
    }

    async fn purge_test_engine(
        log_file_stem: &str,
        gc_buckets: usize,
        purge_threads: usize,
    ) -> (TempDir, Engine) {
        let temp_dir = TempDir::new().unwrap();
        let engine =
            purge_test_engine_config(temp_dir.path(), log_file_stem, gc_buckets, purge_threads)
                .build()
                .await
                .unwrap();
        (temp_dir, engine)
    }

    async fn recv_purge_event(
        event_rx: &Receiver<PurgeTestEvent>,
        mut predicate: impl FnMut(&PurgeTestEvent) -> bool,
    ) -> PurgeTestEvent {
        loop {
            let event = event_rx.recv_async().await.unwrap();
            if predicate(&event) {
                return event;
            }
        }
    }

    async fn recv_purge_plan(event_rx: &Receiver<PurgeTestEvent>) -> PurgeTestEvent {
        recv_purge_event(event_rx, |event| {
            matches!(event, PurgeTestEvent::Planned { .. })
        })
        .await
    }

    async fn collect_purge_cycle(event_rx: &Receiver<PurgeTestEvent>) -> Vec<PurgeTestEvent> {
        let mut events = Vec::new();
        loop {
            let event = event_rx.recv_async().await.unwrap();
            events.push(event);
            if event == PurgeTestEvent::CycleCompleted {
                return events;
            }
        }
    }

    async fn wait_for_purge_poison(trx_sys: &TransactionSystem) {
        let listener = trx_sys.poisoner.listener();
        if trx_sys.poisoner.poison_error().is_none() {
            listener.await;
        }
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

    #[inline]
    fn activate_bucket(bucket: &GCBucket, active_sts: &[u64]) {
        let mut active = bucket.active_sts_list.lock();
        for sts in active_sts {
            active.insert(TrxID::new(*sts));
        }
        if let Some(first) = active_sts.first() {
            bucket.min_active_sts.store(*first, Ordering::Relaxed);
        }
    }

    #[inline]
    fn committed_user(sts: u64, cts: u64, gc_no: usize) -> CommittedTrx {
        CommittedTrx {
            cts: TrxID::new(cts),
            payload: Some(CommittedTrxPayload::User {
                sts: TrxID::new(sts),
                gc_no,
                row_undo: RowUndoLogs::default(),
                index_gc: Vec::new(),
            }),
        }
    }

    #[inline]
    fn committed_system(cts: u64, table_id: u64, page_id: u64) -> CommittedTrx {
        let table_id = TableID::new(table_id);
        CommittedTrx {
            cts: TrxID::new(cts),
            payload: Some(CommittedTrxPayload::System(SysTrxPayload {
                retired_row_pages: RetiredRowPageBatch::new(
                    table_id,
                    RowID::new(0),
                    RowID::new(10),
                    vec![PageID::new(page_id)].into_boxed_slice(),
                ),
            })),
        }
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
    fn test_coalesce_purge_work_preserves_causal_minima_and_requests() {
        let (tx, rx) = flume::unbounded();
        tx.send(Purge::ActiveSts(ActiveStsProgress::NoActive {
            original_sts: TrxID::new(10),
        }))
        .unwrap();
        tx.send(Purge::Committed(FastHashMap::default())).unwrap();
        tx.send(Purge::FullObservation).unwrap();

        let work = coalesce_purge_work(
            &rx,
            Purge::ActiveSts(ActiveStsProgress::Advance {
                original_sts: TrxID::new(20),
                new_sts: TrxID::new(30),
            }),
            |_| CommittedPurgeProgress {
                min_original_sts: Some(TrxID::new(15)),
                min_system_cts: Some(TrxID::new(40)),
            },
        );

        assert_eq!(
            work,
            PurgeWork {
                min_original_sts: Some(TrxID::new(10)),
                min_system_cts: Some(TrxID::new(40)),
                full_observation: true,
                table_root_retention: true,
                dropped_table: true,
                stop_after: false,
            }
        );
    }

    #[test]
    fn test_coalesce_purge_work_keeps_targeted_housekeeping_independent() {
        let (tx, rx) = flume::unbounded();
        tx.send(Purge::DroppedTable).unwrap();
        assert_eq!(
            coalesce_purge_work(&rx, Purge::TableRootRetention, |_| {
                panic!("no committed payload expected")
            }),
            PurgeWork {
                dropped_table: true,
                ..PurgeWork::table_root_retention()
            }
        );
    }

    #[test]
    fn test_plan_purge_cycle_distinguishes_horizon_and_system_work() {
        let completed = TrxID::new(10);
        let current = TrxID::new(20);
        let no_plan = PurgeCyclePlan {
            transaction_gc: false,
            table_root_retention: false,
            dropped_table: false,
            advance_completed_horizon: false,
        };
        let full_plan = PurgeCyclePlan {
            transaction_gc: true,
            table_root_retention: true,
            dropped_table: true,
            advance_completed_horizon: true,
        };

        assert_eq!(
            plan_purge_cycle(
                PurgeWork {
                    min_original_sts: Some(TrxID::new(30)),
                    ..PurgeWork::none()
                },
                completed,
                current,
            ),
            no_plan
        );
        assert_eq!(
            plan_purge_cycle(
                PurgeWork {
                    min_original_sts: Some(TrxID::new(9)),
                    ..PurgeWork::none()
                },
                completed,
                current,
            ),
            full_plan
        );
        assert_eq!(
            plan_purge_cycle(
                PurgeWork {
                    min_original_sts: Some(TrxID::new(9)),
                    ..PurgeWork::none()
                },
                current,
                current,
            ),
            no_plan
        );

        let full_observation = PurgeWork {
            full_observation: true,
            table_root_retention: true,
            dropped_table: true,
            ..PurgeWork::none()
        };
        assert_eq!(
            plan_purge_cycle(full_observation, current, current),
            PurgeCyclePlan {
                transaction_gc: false,
                table_root_retention: true,
                dropped_table: true,
                advance_completed_horizon: false,
            }
        );
        assert_eq!(
            plan_purge_cycle(full_observation, completed, current),
            full_plan
        );

        let system_eligible = PurgeWork {
            min_system_cts: Some(TrxID::new(19)),
            ..PurgeWork::none()
        };
        assert_eq!(
            plan_purge_cycle(system_eligible, current, current),
            PurgeCyclePlan {
                transaction_gc: true,
                table_root_retention: false,
                dropped_table: false,
                advance_completed_horizon: false,
            }
        );
        assert_eq!(
            plan_purge_cycle(system_eligible, completed, current),
            full_plan
        );
        assert_eq!(
            plan_purge_cycle(
                PurgeWork {
                    min_system_cts: Some(current),
                    ..PurgeWork::none()
                },
                current,
                current,
            ),
            no_plan
        );
    }

    #[test]
    fn test_plan_targeted_housekeeping_does_not_advance_completed_horizon() {
        assert_eq!(
            plan_purge_cycle(
                PurgeWork::table_root_retention(),
                TrxID::new(10),
                TrxID::new(20),
            ),
            PurgeCyclePlan {
                transaction_gc: false,
                table_root_retention: true,
                dropped_table: false,
                advance_completed_horizon: false,
            }
        );
    }

    #[test]
    fn test_gc_bucket_records_system_payload_without_active_sts() {
        let bucket = GCBucket::new();
        let committed = committed_system(10, 7, 19);
        assert_eq!(
            bucket.record_committed_for_purge(vec![committed]),
            GCBucketPurgeProgress {
                active_sts: None,
                min_system_cts: Some(TrxID::new(10)),
            }
        );
        assert!(bucket.active_sts_list.lock().active.is_empty());
        assert_eq!(
            TrxID::new(bucket.min_active_sts.load(Ordering::Relaxed)),
            MAX_SNAPSHOT_TS
        );
        let mut purge = Vec::new();
        bucket.get_purge_list(TrxID::new(11), &mut purge);
        assert_eq!(purge.len(), 1);
        assert_eq!(purge[0].sts(), None);
        assert_eq!(
            purge[0]
                .retired_row_pages()
                .map(|batch| batch.page_ids.as_ref()),
            Some(&[PageID::new(19)][..])
        );
    }

    #[test]
    fn test_gc_bucket_preserves_same_table_retirement_order() {
        let bucket = GCBucket::new();
        let table_id = TableID::new(7);
        let committed = [(10, 0, 10, 19), (11, 10, 20, 23)]
            .into_iter()
            .map(|(cts, start, end, page_id)| CommittedTrx {
                cts: TrxID::new(cts),
                payload: Some(CommittedTrxPayload::System(SysTrxPayload {
                    retired_row_pages: RetiredRowPageBatch::new(
                        table_id,
                        RowID::new(start),
                        RowID::new(end),
                        vec![PageID::new(page_id)].into_boxed_slice(),
                    ),
                })),
            })
            .collect();
        assert_eq!(
            bucket.record_committed_for_purge(committed),
            GCBucketPurgeProgress {
                active_sts: None,
                min_system_cts: Some(TrxID::new(10)),
            }
        );

        let mut purge = Vec::new();
        bucket.get_purge_list(TrxID::new(12), &mut purge);
        let ranges = purge
            .into_iter()
            .map(|trx| {
                let batch = trx.into_retired_row_pages().unwrap();
                (batch.start_row_id, batch.end_row_id)
            })
            .collect::<Vec<_>>();
        assert_eq!(
            ranges,
            vec![
                (RowID::new(0), RowID::new(10)),
                (RowID::new(10), RowID::new(20)),
            ]
        );
    }

    #[test]
    fn test_coalesce_purge_work_stop_wins() {
        let (tx, rx) = flume::unbounded();
        tx.send(Purge::Committed(FastHashMap::default())).unwrap();
        tx.send(Purge::Stop).unwrap();
        tx.send(Purge::TableRootRetention).unwrap();
        assert_eq!(
            coalesce_purge_work(&rx, Purge::DroppedTable, |_| {
                CommittedPurgeProgress::default()
            }),
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
            CommittedPurgeProgress::default()
        });

        assert_eq!(work, PurgeWork::stop());
        assert_eq!(analyzed, 3);
    }

    #[test]
    fn test_coalesce_purge_work_committed_preserves_both_progress_dimensions() {
        let (_tx, rx) = flume::unbounded();

        let work = coalesce_purge_work(&rx, Purge::Committed(FastHashMap::default()), |_| {
            CommittedPurgeProgress {
                min_original_sts: Some(TrxID::new(10)),
                min_system_cts: Some(TrxID::new(20)),
            }
        });

        assert_eq!(
            work,
            PurgeWork {
                min_original_sts: Some(TrxID::new(10)),
                min_system_cts: Some(TrxID::new(20)),
                ..PurgeWork::none()
            }
        );
    }

    #[test]
    fn test_gc_bucket_reports_only_changed_active_minimum() {
        let bucket = GCBucket::new();
        activate_bucket(&bucket, &[10, 20, 30]);

        assert_eq!(bucket.record_rollback_for_purge(TrxID::new(20)), None);
        assert_eq!(
            TrxID::new(bucket.min_active_sts.load(Ordering::Relaxed)),
            TrxID::new(10)
        );
        assert_eq!(
            bucket.record_rollback_for_purge(TrxID::new(10)),
            Some(ActiveStsProgress::Advance {
                original_sts: TrxID::new(10),
                new_sts: TrxID::new(30),
            })
        );
        assert_eq!(
            TrxID::new(bucket.min_active_sts.load(Ordering::Relaxed)),
            TrxID::new(30)
        );
        assert_eq!(
            bucket.record_rollback_for_purge(TrxID::new(30)),
            Some(ActiveStsProgress::NoActive {
                original_sts: TrxID::new(30),
            })
        );
        assert_eq!(
            TrxID::new(bucket.min_active_sts.load(Ordering::Relaxed)),
            MAX_SNAPSHOT_TS
        );
    }

    #[test]
    fn test_gc_bucket_committed_batch_reports_one_original_to_final_transition() {
        let bucket = GCBucket::new();
        activate_bucket(&bucket, &[10, 20, 30, 40]);

        let progress = bucket.record_committed_for_purge(vec![
            committed_user(30, 50, 0),
            committed_user(10, 51, 0),
            committed_user(20, 52, 0),
        ]);

        assert_eq!(
            progress,
            GCBucketPurgeProgress {
                active_sts: Some(ActiveStsProgress::Advance {
                    original_sts: TrxID::new(10),
                    new_sts: TrxID::new(40),
                }),
                min_system_cts: None,
            }
        );
        assert_eq!(
            TrxID::new(bucket.min_active_sts.load(Ordering::Relaxed)),
            TrxID::new(40)
        );
        assert_eq!(bucket.committed_trx_list.lock().len(), 3);
    }

    #[test]
    fn test_gc_bucket_mixed_commit_preserves_active_and_system_progress() {
        let bucket = GCBucket::new();
        activate_bucket(&bucket, &[10, 20]);

        let progress = bucket.record_committed_for_purge(vec![
            committed_user(10, 30, 0),
            committed_system(31, 7, 19),
            committed_system(32, 7, 23),
        ]);

        assert_eq!(
            progress,
            GCBucketPurgeProgress {
                active_sts: Some(ActiveStsProgress::Advance {
                    original_sts: TrxID::new(10),
                    new_sts: TrxID::new(20),
                }),
                min_system_cts: Some(TrxID::new(31)),
            }
        );
        assert_eq!(bucket.committed_trx_list.lock().len(), 3);
    }

    #[test]
    fn test_committed_progress_merges_minima_across_buckets() {
        let mut progress = CommittedPurgeProgress::default();
        progress.merge_bucket(GCBucketPurgeProgress {
            active_sts: Some(ActiveStsProgress::Advance {
                original_sts: TrxID::new(30),
                new_sts: TrxID::new(40),
            }),
            min_system_cts: Some(TrxID::new(70)),
        });
        progress.merge_bucket(GCBucketPurgeProgress {
            active_sts: Some(ActiveStsProgress::NoActive {
                original_sts: TrxID::new(10),
            }),
            min_system_cts: Some(TrxID::new(50)),
        });

        assert_eq!(
            progress,
            CommittedPurgeProgress {
                min_original_sts: Some(TrxID::new(10)),
                min_system_cts: Some(TrxID::new(50)),
            }
        );
    }

    #[test]
    fn test_system_bucket_routing_uses_runtime_bucket_count() {
        let committed = committed_system(10, 39, 19);
        assert_eq!(committed.gc_no(2), Some(1));
        assert_eq!(committed.gc_no(32), Some(7));

        let committed = committed_user(10, 20, 1);
        assert_eq!(committed.gc_no(2), Some(1));
        assert_eq!(committed.gc_no(256), Some(1));
    }

    #[test]
    fn test_merge_bucket_results_orders_buckets_and_preserves_bucket_fifo() {
        let batch = |table_id, page_id| {
            RetiredRowPageBatch::new(
                TableID::new(table_id),
                RowID::new(0),
                RowID::new(1),
                vec![PageID::new(page_id)].into_boxed_slice(),
            )
        };
        let merged = merge_bucket_results(
            vec![
                (1, vec![batch(11, 21), batch(11, 22)]),
                (0, vec![batch(10, 20)]),
            ],
            2,
        );
        let pages = merged
            .into_iter()
            .map(|batch| batch.page_ids[0])
            .collect::<Vec<_>>();
        assert_eq!(
            pages,
            vec![PageID::new(20), PageID::new(21), PageID::new(22)]
        );
    }

    #[test]
    fn test_static_purge_worker_slots_cover_each_bucket_once() {
        for gc_buckets in [1, 2, DEFAULT_GC_BUCKETS, 256] {
            for worker_count in [1, 2, 3, 7, 32, 33, 257] {
                let mut bucket_counts = vec![0usize; worker_count];
                for gc_no in 0..gc_buckets {
                    bucket_counts[purge_worker_slot(gc_no, worker_count)] += 1;
                }
                assert_eq!(bucket_counts.iter().sum::<usize>(), gc_buckets);
                if worker_count <= gc_buckets {
                    let min = bucket_counts.iter().copied().min().unwrap();
                    let max = bucket_counts.iter().copied().max().unwrap();
                    assert!(
                        max - min <= 1,
                        "gc_buckets={gc_buckets}, worker_count={worker_count}"
                    );
                }
            }
        }

        for gc_no in 0..DEFAULT_GC_BUCKETS {
            assert_eq!(purge_worker_slot(gc_no, 2), gc_no % 2);
        }
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
    fn test_user_transactions_round_robin_over_configured_buckets() {
        smol::block_on(async {
            let (_temp_dir, engine) = purge_test_engine("redo_gc_routing", 2, 1).await;
            let mut sessions = Vec::new();
            let mut transactions = Vec::new();
            for _ in 0..5 {
                let mut session = engine.new_session().unwrap();
                transactions.push(session.begin_trx().unwrap());
                sessions.push(session);
            }

            let active_counts = engine
                .inner()
                .trx_sys
                .gc_buckets
                .iter()
                .map(|bucket| bucket.active_sts_list.lock().len())
                .collect::<Vec<_>>();
            assert_eq!(active_counts, vec![3, 2]);

            for trx in transactions {
                trx.rollback().await.unwrap();
            }
            drop(sessions);
            engine.shutdown().unwrap();
        });
    }

    #[test]
    fn test_gc_bucket_count_can_change_across_restart_without_migration() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = purge_test_engine_config(temp_dir.path(), "redo_gc_restart", 2, 1)
                .build()
                .await
                .unwrap();
            assert_eq!(engine.inner().trx_sys.gc_buckets.len(), 2);
            engine.shutdown().unwrap();

            let engine = purge_test_engine_config(temp_dir.path(), "redo_gc_restart", 4, 1)
                .build()
                .await
                .unwrap();
            assert_eq!(engine.inner().trx_sys.gc_buckets.len(), 4);
            engine.shutdown().unwrap();
        });
    }

    #[test]
    fn test_process_retired_row_pages_poisons_on_missing_runtime() {
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
                    .process_retired_row_pages(engine.catalog(), &guards, Vec::new())
                    .await
            );
            assert!(engine.inner().poisoner.poison_error().is_none());

            assert!(
                !engine
                    .inner()
                    .trx_sys
                    .process_retired_row_pages(
                        engine.catalog(),
                        &guards,
                        vec![RetiredRowPageBatch::new(
                            TableID::new(999_999),
                            RowID::new(0),
                            RowID::new(1),
                            vec![PageID::new(1)].into_boxed_slice(),
                        )],
                    )
                    .await
            );
            assert!(
                engine
                    .inner()
                    .poisoner
                    .poison_error()
                    .as_ref()
                    .is_some_and(|err| *err.current_context() == FatalError::PurgeAccess)
            );
            assert!(
                engine
                    .inner()
                    .poisoner
                    .ensure_healthy()
                    .as_ref()
                    .is_err_and(|err| *err.current_context() == FatalError::PurgeAccess)
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
                payload: Some(CommittedTrxPayload::User {
                    sts: TrxID::new(1),
                    gc_no: 0,
                    row_undo,
                    index_gc: vec![],
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
                    .poisoner
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
                payload: Some(CommittedTrxPayload::User {
                    sts: TrxID::new(1),
                    gc_no: 0,
                    row_undo,
                    index_gc: vec![],
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
                payload: Some(CommittedTrxPayload::User {
                    sts: TrxID::new(1),
                    gc_no: 0,
                    row_undo,
                    index_gc: vec![],
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
                payload: Some(CommittedTrxPayload::User {
                    sts: TrxID::new(1),
                    gc_no: 0,
                    row_undo,
                    index_gc: vec![],
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
                payload: Some(CommittedTrxPayload::User {
                    sts: TrxID::new(1),
                    gc_no: 0,
                    row_undo,
                    index_gc: vec![],
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
    fn test_full_observation_at_unchanged_horizon_runs_only_requested_housekeeping() {
        smol::block_on(async {
            let (_temp_dir, engine) = purge_test_engine("redo_unchanged_observation", 2, 1).await;
            let completed = engine.inner().trx_sys.global_visible_sts();
            let (event_tx, event_rx) = flume::unbounded();
            engine.inner().trx_sys.set_purge_test_observer(event_tx);

            engine.inner().trx_sys.request_purge_observation();
            let events = collect_purge_cycle(&event_rx).await;
            assert!(events.contains(&PurgeTestEvent::Planned {
                transaction_gc: false,
                advance_completed_horizon: false,
            }));
            assert!(events.contains(&PurgeTestEvent::TableRootRetentionStarted));
            assert!(events.contains(&PurgeTestEvent::DroppedTableStarted));
            assert!(
                events
                    .iter()
                    .all(|event| !matches!(event, PurgeTestEvent::BucketStarted { .. }))
            );
            assert!(
                events.iter().all(|event| !matches!(
                    event,
                    PurgeTestEvent::CompletedHorizonPublished { .. }
                ))
            );
            assert_eq!(engine.inner().trx_sys.global_visible_sts(), completed);
            engine.shutdown().unwrap();
        });
    }

    #[test]
    fn test_system_cts_plans_execute_with_strict_eligibility_and_housekeeping_scope() {
        smol::block_on(async {
            let (_temp_dir, engine) = purge_test_engine("redo_system_plan_execution", 2, 1).await;
            let (event_tx, event_rx) = flume::unbounded();
            engine.inner().trx_sys.set_purge_test_observer(event_tx);

            engine.inner().trx_sys.allocate_snapshot_fence();
            engine.inner().trx_sys.request_purge_observation();
            let priming = collect_purge_cycle(&event_rx).await;
            assert!(
                priming
                    .iter()
                    .any(|event| matches!(event, PurgeTestEvent::CompletedHorizonPublished { .. }))
            );
            let completed = engine.inner().trx_sys.global_visible_sts();

            engine
                .inner()
                .trx_sys
                .purge_tx
                .send(Purge::TestSystemCts(completed))
                .unwrap();
            let equal = collect_purge_cycle(&event_rx).await;
            assert!(equal.contains(&PurgeTestEvent::Planned {
                transaction_gc: false,
                advance_completed_horizon: false,
            }));
            assert!(
                equal
                    .iter()
                    .all(|event| !matches!(event, PurgeTestEvent::BucketStarted { .. }))
            );

            let eligible_cts = TrxID::new(completed.as_u64() - 1);
            engine
                .inner()
                .trx_sys
                .purge_tx
                .send(Purge::TestSystemCts(eligible_cts))
                .unwrap();
            let unchanged = collect_purge_cycle(&event_rx).await;
            assert!(unchanged.contains(&PurgeTestEvent::Planned {
                transaction_gc: true,
                advance_completed_horizon: false,
            }));
            assert_eq!(
                unchanged
                    .iter()
                    .filter(|event| matches!(event, PurgeTestEvent::BucketStarted { .. }))
                    .count(),
                2
            );
            assert!(unchanged.contains(&PurgeTestEvent::RetirementStarted));
            assert!(unchanged.iter().all(|event| !matches!(
                event,
                PurgeTestEvent::TableRootRetentionStarted
                    | PurgeTestEvent::DroppedTableStarted
                    | PurgeTestEvent::CompletedHorizonPublished { .. }
            )));

            engine.inner().trx_sys.allocate_snapshot_fence();
            engine
                .inner()
                .trx_sys
                .purge_tx
                .send(Purge::TestSystemCts(eligible_cts))
                .unwrap();
            let newer = collect_purge_cycle(&event_rx).await;
            assert!(newer.contains(&PurgeTestEvent::Planned {
                transaction_gc: true,
                advance_completed_horizon: true,
            }));
            assert!(newer.contains(&PurgeTestEvent::TableRootRetentionStarted));
            assert!(newer.contains(&PurgeTestEvent::DroppedTableStarted));
            assert!(
                newer
                    .iter()
                    .any(|event| matches!(event, PurgeTestEvent::CompletedHorizonPublished { .. }))
            );
            engine.shutdown().unwrap();
        });
    }

    #[test]
    fn test_global_blocker_rollback_runs_the_deferred_horizon_cycle() {
        smol::block_on(async {
            let (_temp_dir, engine) = purge_test_engine("redo_rollback_blocker", 2, 2).await;
            let mut oldest_session = engine.new_session().unwrap();
            let mut later_session = engine.new_session().unwrap();
            let oldest = oldest_session.begin_trx().unwrap();
            let later = later_session.begin_trx().unwrap();
            assert!(oldest.sts() < later.sts());
            let (event_tx, event_rx) = flume::unbounded();
            engine.inner().trx_sys.set_purge_test_observer(event_tx);

            later.commit().await.unwrap();
            assert_eq!(
                recv_purge_plan(&event_rx).await,
                PurgeTestEvent::Planned {
                    transaction_gc: false,
                    advance_completed_horizon: false,
                }
            );
            assert!(
                event_rx
                    .try_iter()
                    .all(|event| !matches!(event, PurgeTestEvent::BucketStarted { .. }))
            );

            oldest.rollback().await.unwrap();
            assert_eq!(
                recv_purge_plan(&event_rx).await,
                PurgeTestEvent::Planned {
                    transaction_gc: true,
                    advance_completed_horizon: true,
                }
            );
            recv_purge_event(&event_rx, |event| {
                matches!(event, PurgeTestEvent::CompletedHorizonPublished { .. })
            })
            .await;

            drop(oldest_session);
            drop(later_session);
            engine.shutdown().unwrap();
        });
    }

    #[test]
    fn test_stale_no_active_observation_uses_fresh_bucket_minimum() {
        smol::block_on(async {
            let (_temp_dir, engine) = purge_test_engine("redo_stale_no_active", 1, 1).await;
            let mut oldest_session = engine.new_session().unwrap();
            let oldest = oldest_session.begin_trx().unwrap();
            let (event_tx, event_rx) = flume::unbounded();
            let (scan_tx, scan_rx) = flume::bounded(1);
            let (release_tx, release_rx) = flume::bounded(1);
            let blocked = Arc::new(AtomicBool::new(false));
            let hook_blocked = Arc::clone(&blocked);
            engine
                .inner()
                .trx_sys
                .set_purge_test_hook(Arc::new(move |event| {
                    let _ = event_tx.send(event);
                    if event == PurgeTestEvent::PreScan
                        && hook_blocked
                            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                            .is_ok()
                    {
                        scan_tx.send(()).unwrap();
                        release_rx.recv().unwrap();
                    }
                    PurgeTestAction::Continue
                }));

            oldest.rollback().await.unwrap();
            scan_rx.recv_async().await.unwrap();
            let mut replacement_session = engine.new_session().unwrap();
            let replacement = replacement_session.begin_trx().unwrap();
            let replacement_sts = replacement.sts();
            release_tx.send(()).unwrap();

            assert_eq!(
                recv_purge_plan(&event_rx).await,
                PurgeTestEvent::Planned {
                    transaction_gc: true,
                    advance_completed_horizon: true,
                }
            );
            assert_eq!(
                recv_purge_event(&event_rx, |event| {
                    matches!(event, PurgeTestEvent::CompletedHorizonPublished { .. })
                })
                .await,
                PurgeTestEvent::CompletedHorizonPublished {
                    sts: replacement_sts,
                }
            );

            replacement.rollback().await.unwrap();
            drop(oldest_session);
            drop(replacement_session);
            engine.shutdown().unwrap();
        });
    }

    #[test]
    fn test_dispatcher_waits_for_blocked_remote_bucket_before_retirement_and_publication() {
        smol::block_on(async {
            let (_temp_dir, engine) = purge_test_engine("redo_remote_barrier", 2, 2).await;
            let initial_completed = engine.inner().trx_sys.global_visible_sts();
            let (event_tx, event_rx) = flume::unbounded();
            let (blocked_tx, blocked_rx) = flume::bounded(1);
            let (release_tx, release_rx) = flume::bounded(1);
            let blocked = Arc::new(AtomicBool::new(false));
            let hook_blocked = Arc::clone(&blocked);
            engine
                .inner()
                .trx_sys
                .set_purge_test_hook(Arc::new(move |event| {
                    let _ = event_tx.send(event);
                    if event == (PurgeTestEvent::BucketStarted { gc_no: 1 })
                        && hook_blocked
                            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                            .is_ok()
                    {
                        blocked_tx.send(()).unwrap();
                        release_rx.recv().unwrap();
                    }
                    PurgeTestAction::Continue
                }));

            engine.inner().trx_sys.allocate_snapshot_fence();
            engine.inner().trx_sys.request_purge_observation();
            blocked_rx.recv_async().await.unwrap();
            recv_purge_event(&event_rx, |event| {
                *event == (PurgeTestEvent::BucketCompleted { gc_no: 0 })
            })
            .await;
            assert_eq!(
                engine.inner().trx_sys.global_visible_sts(),
                initial_completed
            );
            let blocked_events = event_rx.try_iter().collect::<Vec<_>>();
            assert!(blocked_events.iter().all(|event| !matches!(
                event,
                PurgeTestEvent::RetirementStarted
                    | PurgeTestEvent::CompletedHorizonPublished { .. }
            )));

            release_tx.send(()).unwrap();
            recv_purge_event(&event_rx, |event| {
                *event == PurgeTestEvent::RetirementStarted
            })
            .await;
            recv_purge_event(&event_rx, |event| {
                matches!(event, PurgeTestEvent::CompletedHorizonPublished { .. })
            })
            .await;
            assert!(engine.inner().trx_sys.global_visible_sts() > initial_completed);
            engine.shutdown().unwrap();
        });
    }

    #[test]
    fn test_dispatcher_bucket_failures_poison_without_retirement_or_publication() {
        smol::block_on(async {
            for fail_gc_no in [0, 1] {
                let stem = format!("redo_bucket_failure_{fail_gc_no}");
                let (_temp_dir, engine) = purge_test_engine(&stem, 2, 2).await;
                let initial_completed = engine.inner().trx_sys.global_visible_sts();
                let (event_tx, event_rx) = flume::unbounded();
                let failed = Arc::new(AtomicBool::new(false));
                let hook_failed = Arc::clone(&failed);
                engine
                    .inner()
                    .trx_sys
                    .set_purge_test_hook(Arc::new(move |event| {
                        let _ = event_tx.send(event);
                        if event == (PurgeTestEvent::BucketStarted { gc_no: fail_gc_no })
                            && hook_failed
                                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                                .is_ok()
                        {
                            return PurgeTestAction::Fail;
                        }
                        PurgeTestAction::Continue
                    }));

                engine.inner().trx_sys.allocate_snapshot_fence();
                engine.inner().trx_sys.request_purge_observation();
                recv_purge_event(&event_rx, |event| {
                    *event == (PurgeTestEvent::BucketFailed { gc_no: fail_gc_no })
                })
                .await;
                wait_for_purge_poison(&engine.inner().trx_sys).await;
                assert!(
                    engine
                        .inner()
                        .poisoner
                        .poison_error()
                        .as_ref()
                        .is_some_and(|err| *err.current_context() == FatalError::PurgeAccess)
                );
                assert_eq!(
                    engine.inner().trx_sys.global_visible_sts(),
                    initial_completed
                );
                engine.shutdown().unwrap();

                let events = event_rx.try_iter().collect::<Vec<_>>();
                assert!(events.iter().all(|event| !matches!(
                    event,
                    PurgeTestEvent::RetirementStarted
                        | PurgeTestEvent::CompletedHorizonPublished { .. }
                )));
            }
        });
    }

    #[test]
    fn test_dispatcher_without_executors_runs_every_bucket_locally_and_joins() {
        smol::block_on(async {
            let (_temp_dir, engine) = purge_test_engine("redo_dispatcher_only", 2, 1).await;
            let (event_tx, event_rx) = flume::unbounded();
            engine.inner().trx_sys.set_purge_test_observer(event_tx);
            engine.inner().trx_sys.allocate_snapshot_fence();
            engine.inner().trx_sys.request_purge_observation();

            let mut events = Vec::new();
            loop {
                let event = event_rx.recv_async().await.unwrap();
                events.push(event);
                if event == PurgeTestEvent::CycleCompleted {
                    break;
                }
            }

            let bucket_nos = |select: fn(&PurgeTestEvent) -> Option<usize>| {
                let mut gc_nos = events.iter().filter_map(select).collect::<Vec<_>>();
                gc_nos.sort_unstable();
                gc_nos
            };
            assert_eq!(
                bucket_nos(|event| match event {
                    PurgeTestEvent::BucketStarted { gc_no } => Some(*gc_no),
                    _ => None,
                }),
                vec![0, 1]
            );
            assert_eq!(
                bucket_nos(|event| match event {
                    PurgeTestEvent::BucketCompleted { gc_no } => Some(*gc_no),
                    _ => None,
                }),
                vec![0, 1]
            );
            assert_eq!(
                bucket_nos(|event| match event {
                    PurgeTestEvent::LocalStarted { gc_no } => Some(*gc_no),
                    _ => None,
                }),
                vec![0, 1]
            );
            assert_eq!(
                bucket_nos(|event| match event {
                    PurgeTestEvent::LocalCompleted { gc_no } => Some(*gc_no),
                    _ => None,
                }),
                vec![0, 1]
            );
            assert!(events.iter().all(|event| !matches!(
                event,
                PurgeTestEvent::RemoteEnqueued { .. } | PurgeTestEvent::RemoteCompleted { .. }
            )));
            assert!(
                events
                    .iter()
                    .any(|event| matches!(event, PurgeTestEvent::CompletedHorizonPublished { .. }))
            );
            engine.shutdown().unwrap();
        });
    }

    #[test]
    fn test_more_purge_workers_than_buckets_execute_each_bucket_once_and_join() {
        smol::block_on(async {
            let (_temp_dir, engine) = purge_test_engine("redo_excess_workers", 2, 3).await;
            let (event_tx, event_rx) = flume::unbounded();
            engine.inner().trx_sys.set_purge_test_observer(event_tx);
            engine.inner().trx_sys.allocate_snapshot_fence();
            engine.inner().trx_sys.request_purge_observation();

            let mut events = Vec::new();
            loop {
                let event = event_rx.recv_async().await.unwrap();
                events.push(event);
                if matches!(event, PurgeTestEvent::CompletedHorizonPublished { .. }) {
                    break;
                }
            }
            let mut started = events
                .iter()
                .filter_map(|event| match event {
                    PurgeTestEvent::BucketStarted { gc_no } => Some(*gc_no),
                    _ => None,
                })
                .collect::<Vec<_>>();
            let mut completed = events
                .iter()
                .filter_map(|event| match event {
                    PurgeTestEvent::BucketCompleted { gc_no } => Some(*gc_no),
                    _ => None,
                })
                .collect::<Vec<_>>();
            started.sort_unstable();
            completed.sort_unstable();
            assert_eq!(started, vec![0, 1]);
            assert_eq!(completed, vec![0, 1]);
            engine.shutdown().unwrap();
        });
    }

    #[test]
    fn test_dispatcher_skips_non_global_progress_and_enqueues_remote_work_first() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = EngineConfig::default()
                .storage_root(temp_dir.path().to_path_buf())
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .role(PoolRole::Mem)
                        .max_mem_size(64usize * 1024 * 1024)
                        .max_file_size(128usize * 1024 * 1024),
                )
                .trx(
                    TrxSysConfig::default()
                        .purge_threads(2)
                        .log_file_stem("redo_purge_schedule"),
                )
                .build()
                .await
                .unwrap();
            let mut setup = engine.new_session().unwrap();
            let initial_target = engine.inner().trx_sys.purge_handoff_cts();
            setup.begin_trx().unwrap().commit().await.unwrap();
            setup
                .wait_for_purge_completion_after(initial_target)
                .await
                .unwrap();

            let (event_tx, event_rx) = flume::unbounded();
            engine.inner().trx_sys.set_purge_test_observer(event_tx);
            let mut oldest_session = engine.new_session().unwrap();
            let mut later_session = engine.new_session().unwrap();
            let oldest = oldest_session.begin_trx().unwrap();
            let later = later_session.begin_trx().unwrap();
            assert!(oldest.sts() < later.sts());

            later.commit().await.unwrap();
            assert_eq!(
                recv_purge_plan(&event_rx).await,
                PurgeTestEvent::Planned {
                    transaction_gc: false,
                    advance_completed_horizon: false,
                }
            );
            assert_eq!(
                recv_purge_event(&event_rx, |event| *event == PurgeTestEvent::CycleCompleted).await,
                PurgeTestEvent::CycleCompleted
            );
            assert!(event_rx.try_recv().is_err());

            oldest.commit().await.unwrap();
            assert_eq!(
                recv_purge_plan(&event_rx).await,
                PurgeTestEvent::Planned {
                    transaction_gc: true,
                    advance_completed_horizon: true,
                }
            );
            let mut events = Vec::new();
            loop {
                let event = event_rx.recv_async().await.unwrap();
                events.push(event);
                if event == PurgeTestEvent::RetirementStarted {
                    break;
                }
            }

            let remote_enqueued = events
                .iter()
                .filter_map(|event| match event {
                    PurgeTestEvent::RemoteEnqueued { gc_no, worker_slot } => {
                        assert_eq!(*worker_slot, 1);
                        Some(*gc_no)
                    }
                    _ => None,
                })
                .collect::<Vec<_>>();
            let local_started = events
                .iter()
                .filter_map(|event| match event {
                    PurgeTestEvent::LocalStarted { gc_no } => Some(*gc_no),
                    _ => None,
                })
                .collect::<Vec<_>>();
            let local_completed = events
                .iter()
                .filter(|event| matches!(event, PurgeTestEvent::LocalCompleted { .. }))
                .count();
            let remote_completed = events
                .iter()
                .filter(|event| matches!(event, PurgeTestEvent::RemoteCompleted { .. }))
                .count();

            assert_eq!(remote_enqueued.len(), DEFAULT_GC_BUCKETS / 2);
            assert!(remote_enqueued.iter().all(|gc_no| gc_no % 2 == 1));
            assert_eq!(local_started.len(), DEFAULT_GC_BUCKETS / 2);
            assert!(local_started.iter().all(|gc_no| gc_no % 2 == 0));
            assert_eq!(local_completed, DEFAULT_GC_BUCKETS / 2);
            assert_eq!(remote_completed, DEFAULT_GC_BUCKETS / 2);
            let first_local = events
                .iter()
                .position(|event| matches!(event, PurgeTestEvent::LocalStarted { .. }))
                .unwrap();
            let last_remote_enqueue = events
                .iter()
                .rposition(|event| matches!(event, PurgeTestEvent::RemoteEnqueued { .. }))
                .unwrap();
            assert!(last_remote_enqueue < first_local);
            assert_eq!(events.last(), Some(&PurgeTestEvent::RetirementStarted));
        });
    }

    #[test]
    fn test_trx_purge_one_worker() {
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
            let mut session = engine.new_session().unwrap();
            let initial_target = engine.inner().trx_sys.purge_handoff_cts();
            session
                .wait_for_purge_completion_after(initial_target)
                .await
                .unwrap();
            let init_stats = engine.inner().trx_sys.trx_sys_stats();
            let mut purge_target = initial_target;
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
                purge_target = trx.commit().await.unwrap();
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
                purge_target = trx.commit().await.unwrap();
            }

            session
                .wait_for_purge_completion_after(purge_target)
                .await
                .unwrap();
            let stats = engine.inner().trx_sys.trx_sys_stats();
            assert_eq!(
                stats.purge_trx_count,
                init_stats.purge_trx_count + PURGE_SIZE * 2
            );
            assert_eq!(
                stats.purge_row_count,
                init_stats.purge_row_count + PURGE_SIZE * 2
            );
            assert_eq!(
                stats.purge_index_count,
                init_stats.purge_index_count + PURGE_SIZE
            );
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
            let mut session = engine.new_session().unwrap();
            let initial_target = engine.inner().trx_sys.purge_handoff_cts();
            session
                .wait_for_purge_completion_after(initial_target)
                .await
                .unwrap();
            let init_stats = engine.inner().trx_sys.trx_sys_stats();
            let mut purge_target = initial_target;
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
                purge_target = trx.commit().await.unwrap();
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
                purge_target = trx.commit().await.unwrap();
            }

            session
                .wait_for_purge_completion_after(purge_target)
                .await
                .unwrap();
            let stats = engine.inner().trx_sys.trx_sys_stats();
            assert_eq!(
                stats.purge_trx_count,
                init_stats.purge_trx_count + PURGE_SIZE * 2
            );
            assert_eq!(
                stats.purge_row_count,
                init_stats.purge_row_count + PURGE_SIZE * 2
            );
            assert_eq!(
                stats.purge_index_count,
                init_stats.purge_index_count + PURGE_SIZE
            );
            drop(session);
        });
    }
}
