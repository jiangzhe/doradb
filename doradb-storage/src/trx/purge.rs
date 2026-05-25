use crate::buffer::PageID;
use crate::buffer::guard::PageSharedGuard;
use crate::buffer::{BufferPool, EvictableBufferPool, PoolGuards};
use crate::catalog::{Catalog, TableCache, TableID, is_catalog_obj_id};
use crate::error::{FatalError, Result};
use crate::file::table_file::OldRoot;
use crate::latch::LatchFallbackMode;
use crate::quiescent::{QuiescentGuard, SyncQuiescentGuard};
use crate::row::RowPage;
use crate::table::Table;
use crate::thread;
use crate::trx::log::LogPartition;
use crate::trx::row::RowWriteAccess;
use crate::trx::sys::TransactionSystem;
use crate::trx::undo::{OwnedRowUndo, RowUndoKind};
use crate::trx::{CommittedTrx, MAX_SNAPSHOT_TS, TrxID};
use async_executor::LocalExecutor;
use crossbeam_utils::CachePadded;
use flume::{Receiver, Sender};
use parking_lot::Mutex;
use std::collections::HashSet;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread::JoinHandle;

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

#[inline]
fn handle_gc_row_page_deallocation_result(trx_sys: &TransactionSystem, res: Result<()>) -> bool {
    match res {
        Ok(()) => true,
        Err(_) => {
            // Runtime resource destruction is not replayable once purge has
            // started mutating in-memory ownership. Treat any error here as a
            // fatal storage failure: reject new work, let explicit shutdown join
            // the worker later, and stop this purge cycle immediately.
            let _ = trx_sys.poison_storage(FatalError::PurgeDeallocate);
            false
        }
    }
}

/// Runtime table handle waiting for purge-horizon destruction after DROP TABLE.
///
/// The catalog no longer exposes this table once foreground DROP TABLE commits,
/// but stale `Arc<Table>` handles may still exist in old sessions or other
/// runtime holders. Purge owns the final runtime destroy step so it can wait
/// until the drop commit is older than every active snapshot.
pub(crate) struct DroppedTableGcItem {
    pub(crate) table_id: TableID,
    pub(crate) drop_cts: TrxID,
    pub(crate) table: Arc<Table>,
}

/// User table file waiting for checkpoint-safe deletion after runtime destroy.
///
/// Physical file unlink must lag runtime destroy until a catalog checkpoint has
/// made the table absence durable. Before that point recovery may still need the
/// file to replay the committed drop.
#[derive(Clone, Copy)]
pub(crate) struct DroppedTableFileDeleteItem {
    pub(crate) table_id: TableID,
    pub(crate) drop_cts: TrxID,
}

impl DroppedTableFileDeleteItem {
    #[inline]
    pub(crate) fn new(table_id: TableID, drop_cts: TrxID) -> Self {
        Self { table_id, drop_cts }
    }
}

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

/// Purge-owned queues for GC-managed dropped-table cleanup.
///
/// `runtime` entries wait for both the purge horizon and exclusive ownership of
/// the table `Arc`; `files` entries wait for the catalog checkpoint replay
/// floor. These queues are deliberately purge-owned so foreground DROP TABLE can
/// finish after logical removal without synchronously reclaiming memory or
/// unlinking files.
#[derive(Default)]
pub(crate) struct DroppedTableQueue {
    runtime: VecDeque<DroppedTableGcItem>,
    files: VecDeque<DroppedTableFileDeleteItem>,
}

impl DroppedTableQueue {
    #[inline]
    pub(crate) fn from_file_deletes(file_deletes: Vec<DroppedTableFileDeleteItem>) -> Self {
        Self {
            runtime: VecDeque::new(),
            files: VecDeque::from(file_deletes),
        }
    }

    #[inline]
    fn push_runtime(&mut self, item: DroppedTableGcItem) {
        self.runtime.push_back(item);
    }
}

impl TransactionSystem {
    /// Enqueue a logically dropped table runtime for purge-horizon destruction.
    ///
    /// The wake requests only dropped-table cleanup, so foreground DROP TABLE
    /// does not trigger a full undo/index/page GC cycle.
    ///
    /// A send failure is harmless during shutdown: the engine owner will join the
    /// purge worker, and any remaining queue entries become irrelevant once the
    /// owner tears down the whole runtime.
    #[inline]
    pub(crate) fn enqueue_dropped_table(
        &self,
        table_id: TableID,
        drop_cts: TrxID,
        table: Arc<Table>,
    ) {
        self.dropped_tables.lock().push_runtime(DroppedTableGcItem {
            table_id,
            drop_cts,
            table,
        });
        self.request_dropped_table_purge();
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
                let ex = LocalExecutor::new();
                let mut purger = PurgeSingleThreaded;
                smol::block_on(ex.run(purger.purge_loop(
                    &task_mem_pool,
                    &task_trx_sys.catalog,
                    &task_trx_sys,
                    pool_guards,
                    purge_chan,
                )))
            });
            vec![handle]
        } else {
            // multi-threaded purger
            let dispatcher_guards = pool_guards.clone();
            let (mut dispatcher, executors) = Self::dispatch_purge(trx_sys.clone(), pool_guards);
            let task_trx_sys = trx_sys.clone();
            let task_mem_pool = mem_pool.clone();
            let handle = thread::spawn_named("Purge-Dispatcher", move || {
                let ex = LocalExecutor::new();
                smol::block_on(ex.run(dispatcher.purge_loop(
                    &task_mem_pool,
                    &task_trx_sys.catalog,
                    &task_trx_sys,
                    dispatcher_guards,
                    purge_chan,
                )));
            });
            let mut handles = Vec::with_capacity(executors.len() + 1);
            handles.push(handle);
            handles.extend(executors);
            handles
        }
    }

    #[inline]
    pub(crate) fn calc_min_active_sts_for_gc(&self) -> TrxID {
        // first, we load current STS as upperbound.
        // There might be case a transaction begins and commits
        // when we refresh min_active_sts, if we do not hold this
        // upperbound, we may clear the new transaction incorrectly.
        let max_active_sts = self.ts.load(Ordering::SeqCst);
        // then, load actual minimum active STS from all GC buckets.
        let mut min_ts = MAX_SNAPSHOT_TS;
        for partition in &*self.log_partitions {
            let ts = partition.min_active_sts();
            min_ts = min_ts.min(ts);
        }
        min_ts.min(max_active_sts)
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
                let ex = LocalExecutor::new();
                smol::block_on(ex.run(purger.purge_task_loop(
                    &task_trx_sys.catalog,
                    &task_trx_sys,
                    pool_guards,
                    rx,
                )));
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
        log_no: usize,
        trx_list: Vec<CommittedTrx>,
        min_active_sts: TrxID,
    ) -> Result<HashSet<PageID>> {
        let res: Result<HashSet<PageID>> = async {
            let partition = &self.log_partitions[log_no];
            let mut table_cache = TableCache::new(catalog);
            let purge_trx_count = trx_list.len();
            let mut purge_row_count = 0;
            let mut purge_index_count = 0;
            // First, purge row undo logs by versioned page identity.
            for trx in &trx_list {
                if let Some(row_undo) = trx.row_undo() {
                    purge_row_count += row_undo.len();
                    for undo in &**row_undo {
                        if is_catalog_obj_id(undo.table_id) {
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
                        if is_catalog_obj_id(ip.table_id) {
                            let Some(table) = table_cache.get_catalog_table(ip.table_id) else {
                                continue;
                            };
                            if table
                                .delete_index(guards, &ip.key, ip.row_id, ip.unique, min_active_sts)
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
            let mut gc_row_pages = HashSet::new();
            for trx in &trx_list {
                if let Some(pages) = trx.gc_row_pages() {
                    gc_row_pages.extend(pages.iter().copied());
                }
            }

            drop(trx_list);

            partition
                .stats
                .purge_trx_count
                .fetch_add(purge_trx_count, Ordering::Relaxed);
            partition
                .stats
                .purge_row_count
                .fetch_add(purge_row_count, Ordering::Relaxed);
            partition
                .stats
                .purge_index_count
                .fetch_add(purge_index_count, Ordering::Relaxed);
            Ok(gc_row_pages)
        }
        .await;
        match res {
            Ok(gc_row_pages) => Ok(gc_row_pages),
            Err(_) => Err(self.poison_storage(FatalError::PurgeAccess).into()),
        }
    }

    #[inline]
    async fn deallocate_gc_row_pages(
        &self,
        mem_pool: &EvictableBufferPool,
        guards: &PoolGuards,
        gc_row_pages: HashSet<PageID>,
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
        // First detach eligible runtime entries under the queue lock, then do
        // async destruction without holding the lock. Entries that are not past
        // the purge horizon stay queued for a later wake.
        let eligible = {
            let mut queues = self.dropped_tables.lock();
            let mut eligible = Vec::new();
            let mut retained = VecDeque::new();
            while let Some(item) = queues.runtime.pop_front() {
                if item.drop_cts < min_active_sts {
                    eligible.push(item);
                } else {
                    retained.push_back(item);
                }
            }
            queues.runtime = retained;
            eligible
        };

        let mut stale_handles = Vec::new();
        let mut file_deletes = Vec::new();
        for DroppedTableGcItem {
            table_id,
            drop_cts,
            table,
        } in eligible
        {
            match Arc::try_unwrap(table) {
                Ok(table) => {
                    // At this point purge has exclusive ownership of the dropped
                    // runtime. Any destroy error is fatal to the storage runtime
                    // and is converted to poison by the caller.
                    table.destroy_dropped_runtime(guards).await?;
                    file_deletes.push(DroppedTableFileDeleteItem::new(table_id, drop_cts));
                }
                Err(table) => {
                    // Stale external table handles are not fatal. Keep retrying
                    // on future purge wakes until the last handle is released.
                    stale_handles.push(DroppedTableGcItem {
                        table_id,
                        drop_cts,
                        table,
                    })
                }
            }
        }

        {
            let mut queues = self.dropped_tables.lock();
            queues.runtime.extend(stale_handles);
            queues.files.extend(file_deletes);
        }

        self.process_dropped_table_file_deletes();
        Ok(())
    }

    #[inline]
    fn process_dropped_table_file_deletes(&self) {
        // File deletion is a checkpoint-gated housekeeping step. Unlike runtime
        // destroy, unlink failure is retryable and does not poison the engine:
        // retain the item and let a later purge wake try again.
        let catalog_replay_start_ts = match self.catalog.storage.checkpoint_snapshot() {
            Ok(snapshot) => snapshot.catalog_replay_start_ts,
            Err(_) => return,
        };
        let file_deletes = {
            let mut queues = self.dropped_tables.lock();
            queues.files.drain(..).collect::<Vec<_>>()
        };
        if file_deletes.is_empty() {
            return;
        }

        let mut retained = Vec::new();
        for item in file_deletes {
            if catalog_replay_start_ts <= item.drop_cts {
                retained.push(item);
                continue;
            }
            if self.table_fs.delete_user_table_file(item.table_id).is_err() {
                retained.push(item);
            }
        }
        if !retained.is_empty() {
            self.dropped_tables.lock().files.extend(retained);
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
    pub(super) deleted: HashSet<TrxID>,
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
        let first = self.active.front().cloned().unwrap();
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

impl LogPartition {
    /// Execute GC analysis in loop.
    /// This method is used for a separate GC analyzer thread for each log partition.
    #[inline]
    pub(crate) fn gc_loop(&self, gc_rx: Receiver<GC>, purge_chan: Sender<Purge>) {
        while let Ok(msg) = gc_rx.recv() {
            match msg {
                GC::Stop => return,
                GC::Commit(trx_list) => {
                    let min_active_sts_may_change = self.gc_analyze(trx_list);
                    if min_active_sts_may_change {
                        let _ = purge_chan.send(Purge::Next);
                    }
                }
            }
        }
    }

    /// Analyze committed transaction and modify active sts list and committed transaction list.
    /// Returns whether min_active_sts may change.
    #[inline]
    fn gc_analyze(&self, trx_list: HashMap<usize, Vec<CommittedTrx>>) -> bool {
        let mut changed = false;
        for (gc_no, trx_list) in trx_list {
            let gc_bucket = &self.gc_buckets[gc_no];
            changed |= gc_bucket.gc_analyze_commit(trx_list);
        }
        changed
    }
}

/// GCBucket is used for GC analyzer to store and analyze GC related information,
/// including committed transaction list, old transaction list, active snapshot timestamp
/// list, etc.
pub(super) struct GCBucket {
    /// Committed transaction list.
    /// When a transaction is committed, it will be put into this queue in sequence.
    /// Head is always oldest and tail is newest.
    pub(super) committed_trx_list: CachePadded<Mutex<VecDeque<CommittedTrx>>>,
    /// Active snapshot timestamp list.
    /// The smallest value equals to min_active_sts.
    pub(super) active_sts_list: CachePadded<Mutex<ActiveStsList>>,
    /// Minimum active snapshot sts of this bucket.
    pub(super) min_active_sts: CachePadded<AtomicU64>,
}

impl GCBucket {
    /// Create a new GC bucket.
    #[inline]
    pub(super) fn new() -> Self {
        GCBucket {
            committed_trx_list: CachePadded::new(Mutex::new(VecDeque::new())),
            active_sts_list: CachePadded::new(Mutex::new(ActiveStsList::default())),
            min_active_sts: CachePadded::new(AtomicU64::new(MAX_SNAPSHOT_TS)),
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

    /// Analyze rollbacked transactions for GC.
    #[inline]
    pub(super) fn gc_analyze_rollback(&self, sts: TrxID) -> bool {
        debug_assert!(self.min_active_sts.load(Ordering::Relaxed) != MAX_SNAPSHOT_TS);
        let mut active_sts_list = self.active_sts_list.lock();
        let min_sts = active_sts_list.remove(sts);
        self.update_min_active_sts(min_sts)
    }

    /// Analyze committed transactions for GC.
    #[inline]
    pub(super) fn gc_analyze_commit(&self, trx_list: Vec<CommittedTrx>) -> bool {
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
        debug_assert!(self.min_active_sts.load(Ordering::Relaxed) != MAX_SNAPSHOT_TS);

        // There is no active transaction. We should update min_active_sts.
        if min_sts == MAX_SNAPSHOT_TS {
            self.min_active_sts.store(min_sts, Ordering::Relaxed);
            return true;
        }

        // There are active transactions. We should compare them and update only if
        // latest value is larger.
        let curr_sts = self.min_active_sts.load(Ordering::Relaxed);
        if min_sts > curr_sts {
            self.min_active_sts.store(min_sts, Ordering::Relaxed);
            return true;
        }
        false
    }
}

/// Messages sent from commit processing to GC analyzers.
pub(crate) enum GC {
    Stop,
    // transaction list per gc bucket.
    Commit(HashMap<usize, Vec<CommittedTrx>>),
}

/// Commands sent to purge workers.
pub(crate) enum Purge {
    /// Stop the purge coordinator and let worker shutdown join the thread.
    Stop,
    /// Run a full transaction/index/row-page purge cycle.
    Next,
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
}

impl PurgeWork {
    #[inline]
    const fn full() -> Self {
        Self {
            full_gc: true,
            table_root_retention: true,
            dropped_table: true,
        }
    }

    #[inline]
    const fn table_root_retention() -> Self {
        Self {
            full_gc: false,
            table_root_retention: true,
            dropped_table: false,
        }
    }

    #[inline]
    const fn dropped_table() -> Self {
        Self {
            full_gc: false,
            table_root_retention: false,
            dropped_table: true,
        }
    }

    #[inline]
    fn absorb(&mut self, purge: Purge) -> bool {
        match purge {
            Purge::Stop => return false,
            Purge::Next => *self = PurgeWork::full(),
            Purge::TableRootRetention => self.table_root_retention = true,
            Purge::DroppedTable => self.dropped_table = true,
        }
        true
    }
}

#[inline]
fn coalesce_purge_work(purge_chan: &Receiver<Purge>, initial: Purge) -> Option<PurgeWork> {
    // Collapse queued wakeups so a burst of lightweight cleanup requests does
    // not trigger repeated scans, while any full GC request upgrades the cycle.
    // `Stop` always wins because shutdown must not be delayed behind cleanup.
    let mut work = match initial {
        Purge::Stop => return None,
        Purge::Next => PurgeWork::full(),
        Purge::TableRootRetention => PurgeWork::table_root_retention(),
        Purge::DroppedTable => PurgeWork::dropped_table(),
    };
    while let Ok(purge) = purge_chan.try_recv() {
        if !work.absorb(purge) {
            return None;
        }
    }
    Some(work)
}

struct PurgeTask {
    log_no: usize,
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
            let Some(work) = coalesce_purge_work(&purge_chan, purge) else {
                return;
            };
            let curr_sts = trx_sys.calc_min_active_sts_for_gc();
            if work.full_gc && curr_sts > min_sts {
                // Start GC. Purge undo/index for all partitions first, then
                // deallocate retired row pages once to avoid cross-partition ordering issue.
                let mut gc_row_pages = HashSet::new();
                for partition in &*trx_sys.log_partitions {
                    let mut trx_list = vec![];
                    for gc_bucket in &partition.gc_buckets {
                        gc_bucket.get_purge_list(curr_sts, &mut trx_list);
                    }
                    let log_no = partition.log_no;
                    let partition_gc_pages = match trx_sys
                        .purge_trx_list(catalog, &pool_guards, log_no, trx_list, curr_sts)
                        .await
                    {
                        Ok(gc_pages) => gc_pages,
                        Err(_) => return,
                    };
                    gc_row_pages.extend(partition_gc_pages);
                }
                if !handle_gc_row_page_deallocation_result(
                    trx_sys,
                    trx_sys
                        .deallocate_gc_row_pages(mem_pool, &pool_guards, gc_row_pages)
                        .await,
                ) {
                    // The helper already poisoned runtime admission. Exiting the
                    // loop leaves shutdown to join this already-finished worker.
                    return;
                }
            }
            if work.table_root_retention {
                trx_sys.process_retained_table_roots(curr_sts);
            }
            if work.dropped_table
                && !handle_gc_row_page_deallocation_result(
                    trx_sys,
                    trx_sys
                        .process_dropped_table_gc(&pool_guards, curr_sts)
                        .await,
                )
            {
                // Dropped-table runtime destruction failed after purge took
                // ownership. Do not retry in-place against a poisoned runtime.
                return;
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
            let Some(work) = coalesce_purge_work(&purge_chan, purge) else {
                break 'DISPATCH_LOOP;
            };
            let curr_sts = trx_sys.calc_min_active_sts_for_gc();
            if work.full_gc && curr_sts > min_sts {
                // dispatch tasks to executors
                let (done_tx, done_rx) = flume::unbounded();
                let mut expected_tasks = 0usize;
                let gc_row_pages = Arc::new(Mutex::new(vec![]));
                for partition in &*trx_sys.log_partitions {
                    let log_no = partition.log_no;
                    for gc_no in 0..partition.gc_buckets.len() {
                        let task = PurgeTask {
                            log_no,
                            gc_no,
                            min_active_sts: curr_sts,
                            done: done_tx.clone(),
                            gc_row_pages: Arc::clone(&gc_row_pages),
                        };
                        if self.0[dispatch_no % self.0.len()].send(task).is_ok() {
                            expected_tasks += 1;
                        }
                        dispatch_no += 1;
                    }
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
                    g.drain(..).collect::<HashSet<PageID>>()
                };
                if !handle_gc_row_page_deallocation_result(
                    trx_sys,
                    trx_sys
                        .deallocate_gc_row_pages(mem_pool, &pool_guards, gc_row_pages)
                        .await,
                ) {
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
            if work.dropped_table
                && !handle_gc_row_page_deallocation_result(
                    trx_sys,
                    trx_sys
                        .process_dropped_table_gc(&pool_guards, curr_sts)
                        .await,
                )
            {
                // Dropped-table destroy failure is fatal; exiting also closes
                // the executor task channels when this dispatcher is dropped.
                return;
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
            log_no,
            gc_no,
            min_active_sts,
            done,
            gc_row_pages,
        }) = purge_chan.recv()
        {
            let mut trx_list = vec![];
            let partition = &trx_sys.log_partitions[log_no];
            partition.gc_buckets[gc_no].get_purge_list(min_active_sts, &mut trx_list);
            // actual purge here
            let res = trx_sys
                .purge_trx_list(catalog, &pool_guards, log_no, trx_list, min_active_sts)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::guard::PageSharedGuard;
    use crate::buffer::page::VersionedPageID;
    use crate::buffer::{BufferPool, PoolGuard, PoolGuards, PoolRole};
    use crate::catalog::tests::table1;
    use crate::conf::{EngineConfig, EvictableBufferPoolConfig, TrxSysConfig};
    use crate::error::FatalError;
    use crate::file::cow_file::BlockID;
    use crate::index::{IndexCompareExchange, IndexInsert, RowLocation, UniqueIndex};
    use crate::latch::LatchFallbackMode;
    use crate::row::ops::{DeleteMvcc, SelectKey};
    use crate::row::{RowID, RowPage};
    use crate::table::{DeleteMarker, Table};
    use crate::trx::row::RowReadAccess;
    use crate::trx::stmt::Statement;
    use crate::trx::undo::{OwnedRowUndo, RowUndoKind, RowUndoLogs};
    use crate::trx::{CommittedTrxPayload, MIN_ACTIVE_TRX_ID, SharedTrxStatus};
    use crate::value::Val;
    use std::sync::Arc;
    use std::time::{Duration, Instant};
    use tempfile::TempDir;

    #[inline]
    fn full_pool_guards(engine: &crate::engine::Engine) -> PoolGuards {
        PoolGuards::builder()
            .push(PoolRole::Meta, engine.meta_pool.pool_guard())
            .push(PoolRole::Index, engine.index_pool.pool_guard())
            .push(PoolRole::Mem, engine.mem_pool.pool_guard())
            .push(PoolRole::Disk, engine.disk_pool.pool_guard())
            .build()
    }

    #[inline]
    fn active_secondary_root(table: &Table, index_no: usize) -> BlockID {
        table.file().active_root_unchecked().secondary_index_roots[index_no]
    }

    struct BoundUniqueIndexNo<'a> {
        table: &'a Table,
        index_no: usize,
        root: BlockID,
    }

    impl UniqueIndex for BoundUniqueIndexNo<'_> {
        #[inline]
        async fn lookup(
            &self,
            pool_guard: &PoolGuard,
            key: &[Val],
            ts: TrxID,
        ) -> crate::error::Result<Option<(RowID, bool)>> {
            let layout = self.table.layout_snapshot();
            let index = layout.secondary_index(self.index_no)?;
            index
                .bind_unique(self.root)?
                .lookup(pool_guard, key, ts)
                .await
        }

        #[inline]
        async fn insert_if_not_exists(
            &self,
            pool_guard: &PoolGuard,
            key: &[Val],
            row_id: RowID,
            merge_if_match_deleted: bool,
            ts: TrxID,
        ) -> crate::error::Result<IndexInsert> {
            let layout = self.table.layout_snapshot();
            let index = layout.secondary_index(self.index_no)?;
            index
                .bind_unique(self.root)?
                .insert_if_not_exists(pool_guard, key, row_id, merge_if_match_deleted, ts)
                .await
        }

        #[inline]
        async fn compare_delete(
            &self,
            pool_guard: &PoolGuard,
            key: &[Val],
            old_row_id: RowID,
            ignore_del_mask: bool,
            ts: TrxID,
        ) -> crate::error::Result<bool> {
            let layout = self.table.layout_snapshot();
            let index = layout.secondary_index(self.index_no)?;
            index
                .bind_unique(self.root)?
                .compare_delete(pool_guard, key, old_row_id, ignore_del_mask, ts)
                .await
        }

        #[inline]
        async fn compare_exchange(
            &self,
            pool_guard: &PoolGuard,
            key: &[Val],
            old_row_id: RowID,
            new_row_id: RowID,
            ts: TrxID,
        ) -> crate::error::Result<IndexCompareExchange> {
            let layout = self.table.layout_snapshot();
            let index = layout.secondary_index(self.index_no)?;
            index
                .bind_unique(self.root)?
                .compare_exchange(pool_guard, key, old_row_id, new_row_id, ts)
                .await
        }

        #[inline]
        async fn scan_values(
            &self,
            pool_guard: &PoolGuard,
            values: &mut Vec<RowID>,
            ts: TrxID,
        ) -> crate::error::Result<()> {
            let layout = self.table.layout_snapshot();
            let index = layout.secondary_index(self.index_no)?;
            index
                .bind_unique(self.root)?
                .scan_values(pool_guard, values, ts)
                .await
        }
    }

    #[inline]
    fn bound_unique_index_no(table: &Table, index_no: usize) -> BoundUniqueIndexNo<'_> {
        BoundUniqueIndexNo {
            table,
            index_no,
            root: active_secondary_root(table, index_no),
        }
    }

    async fn stmt_insert_row(
        stmt: &mut Statement<'_>,
        table: &Table,
        cols: Vec<Val>,
    ) -> crate::error::Result<RowID> {
        stmt.table_insert_mvcc(table, cols).await
    }

    async fn stmt_delete_row(
        stmt: &mut Statement<'_>,
        table: &Table,
        key: &SelectKey,
    ) -> crate::error::Result<DeleteMvcc> {
        stmt.table_delete_unique_mvcc(table, key, false).await
    }

    #[test]
    fn test_coalesce_purge_work_preserves_strongest_request() {
        let (_tx, rx) = flume::unbounded();
        assert_eq!(
            coalesce_purge_work(&rx, Purge::TableRootRetention),
            Some(PurgeWork::table_root_retention())
        );

        let (tx, rx) = flume::unbounded();
        tx.send(Purge::DroppedTable).unwrap();
        assert_eq!(
            coalesce_purge_work(&rx, Purge::TableRootRetention),
            Some(PurgeWork {
                full_gc: false,
                table_root_retention: true,
                dropped_table: true,
            })
        );

        let (tx, rx) = flume::unbounded();
        tx.send(Purge::TableRootRetention).unwrap();
        tx.send(Purge::Next).unwrap();
        assert_eq!(
            coalesce_purge_work(&rx, Purge::DroppedTable),
            Some(PurgeWork::full())
        );

        let (tx, rx) = flume::unbounded();
        tx.send(Purge::DroppedTable).unwrap();
        assert_eq!(
            coalesce_purge_work(&rx, Purge::Next),
            Some(PurgeWork::full())
        );
    }

    #[test]
    fn test_coalesce_purge_work_stop_wins() {
        let (tx, rx) = flume::unbounded();
        tx.send(Purge::Next).unwrap();
        tx.send(Purge::Stop).unwrap();
        tx.send(Purge::TableRootRetention).unwrap();
        assert_eq!(coalesce_purge_work(&rx, Purge::DroppedTable), None);
        assert_eq!(coalesce_purge_work(&rx, Purge::Stop), None);
    }

    #[test]
    fn test_active_sts_list() {
        let mut active_sts_list = ActiveStsList::default();
        for (val, expected, delete) in vec![
            (1, 1, false),
            (1, MAX_SNAPSHOT_TS, true),
            (2, 2, false),
            (3, 2, false),
            (4, 2, false),
            (3, 2, true),
            (2, 4, true),
            (5, 4, false),
            (6, 4, false),
            (6, 4, true),
            (5, 4, true),
            (4, MAX_SNAPSHOT_TS, true),
        ] {
            let res = if delete {
                active_sts_list.remove(val)
            } else {
                active_sts_list.insert(val);
                active_sts_list.active.front().cloned().unwrap()
            };
            assert!(res == expected)
        }
    }

    #[test]
    fn test_handle_gc_row_page_deallocation_result_poisons_runtime() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = EngineConfig::default()
                .storage_root(main_dir)
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .role(crate::buffer::PoolRole::Mem)
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

            assert!(handle_gc_row_page_deallocation_result(
                &engine.trx_sys,
                Ok(())
            ));
            assert!(engine.trx_sys.storage_poison_error().is_none());

            assert!(!handle_gc_row_page_deallocation_result(
                &engine.trx_sys,
                Err(std::io::Error::from_raw_os_error(libc::EIO).into())
            ));
            assert!(
                engine
                    .trx_sys
                    .storage_poison_error()
                    .as_ref()
                    .is_some_and(|err| *err.current_context() == FatalError::PurgeDeallocate)
            );
            assert!(
                engine
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
                        .role(crate::buffer::PoolRole::Mem)
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
                0,
                RowUndoKind::Delete,
            ));
            let trx = CommittedTrx {
                cts: 100,
                payload: Some(CommittedTrxPayload {
                    sts: 1,
                    gc_no: 0,
                    row_undo,
                    index_gc: vec![],
                    gc_row_pages: vec![],
                }),
            };
            let guards = PoolGuards::builder()
                .push(PoolRole::Index, engine.index_pool.pool_guard())
                .build();

            let err = engine
                .trx_sys
                .purge_trx_list(engine.catalog(), &guards, 0, vec![trx], MAX_SNAPSHOT_TS)
                .await
                .unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<FatalError>().copied(),
                Some(FatalError::PurgeAccess)
            );
            assert!(
                engine
                    .trx_sys
                    .storage_poison_error()
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
                        .role(crate::buffer::PoolRole::Mem)
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
                stmt_insert_row(stmt, &table, vec![Val::from(1001i32)]).await?;
                Ok(())
            })
            .await
            .unwrap();
            trx.commit().await.unwrap();
            drop(session);
            let pool_guards = full_pool_guards(&engine);
            let key = vec![Val::from(1001i32)];
            let Some((row_id, _)) = bound_unique_index_no(&table, 0)
                .lookup(pool_guards.index_guard(), &key, MAX_SNAPSHOT_TS)
                .await
                .unwrap()
            else {
                panic!("row should exist");
            };
            let status = Arc::new(SharedTrxStatus::new(100));
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
                cts: 100,
                payload: Some(CommittedTrxPayload {
                    sts: 1,
                    gc_no: 0,
                    row_undo,
                    index_gc: vec![],
                    gc_row_pages: vec![],
                }),
            };
            {
                let pool_guards = full_pool_guards(&engine);
                engine
                    .trx_sys
                    .purge_trx_list(
                        engine.catalog(),
                        &pool_guards,
                        0,
                        vec![trx],
                        MAX_SNAPSHOT_TS,
                    )
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
                        .role(crate::buffer::PoolRole::Mem)
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
                stmt_insert_row(stmt, &table, vec![Val::from(1002i32)]).await?;
                Ok(())
            })
            .await
            .unwrap();
            trx.commit().await.unwrap();
            drop(session);
            let pool_guards = full_pool_guards(&engine);
            let key = vec![Val::from(1002i32)];
            let Some((row_id, _)) = bound_unique_index_no(&table, 0)
                .lookup(pool_guards.index_guard(), &key, MAX_SNAPSHOT_TS)
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
                cts: 100,
                payload: Some(CommittedTrxPayload {
                    sts: 1,
                    gc_no: 0,
                    row_undo,
                    index_gc: vec![],
                    gc_row_pages: vec![],
                }),
            };
            {
                let pool_guards = full_pool_guards(&engine);
                engine
                    .trx_sys
                    .purge_trx_list(
                        engine.catalog(),
                        &pool_guards,
                        0,
                        vec![trx],
                        MAX_SNAPSHOT_TS,
                    )
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
                        .role(crate::buffer::PoolRole::Mem)
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
                stmt_insert_row(stmt, &table, vec![Val::from(1003i32)]).await?;
                Ok(())
            })
            .await
            .unwrap();
            trx.commit().await.unwrap();
            drop(session);
            let pool_guards = full_pool_guards(&engine);
            let key = vec![Val::from(1003i32)];
            let Some((row_id, _)) = bound_unique_index_no(&table, 0)
                .lookup(pool_guards.index_guard(), &key, MAX_SNAPSHOT_TS)
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
            let status = Arc::new(SharedTrxStatus::new(100));
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
                cts: 100,
                payload: Some(CommittedTrxPayload {
                    sts: 1,
                    gc_no: 0,
                    row_undo,
                    index_gc: vec![],
                    gc_row_pages: vec![],
                }),
            };
            {
                let pool_guards = full_pool_guards(&engine);
                engine
                    .trx_sys
                    .purge_trx_list(
                        engine.catalog(),
                        &pool_guards,
                        0,
                        vec![trx],
                        MAX_SNAPSHOT_TS,
                    )
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
                        .role(crate::buffer::PoolRole::Mem)
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
                stmt_insert_row(stmt, &table, vec![Val::from(1004i32)]).await?;
                Ok(())
            })
            .await
            .unwrap();
            trx.commit().await.unwrap();
            drop(session);
            let pool_guards = full_pool_guards(&engine);
            let key = vec![Val::from(1004i32)];
            let Some((row_id, _)) = bound_unique_index_no(&table, 0)
                .lookup(pool_guards.index_guard(), &key, MAX_SNAPSHOT_TS)
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
                cts: 100,
                payload: Some(CommittedTrxPayload {
                    sts: 1,
                    gc_no: 0,
                    row_undo,
                    index_gc: vec![],
                    gc_row_pages: vec![],
                }),
            };
            {
                let pool_guards = full_pool_guards(&engine);
                engine
                    .trx_sys
                    .purge_trx_list(
                        engine.catalog(),
                        &pool_guards,
                        0,
                        vec![trx],
                        MAX_SNAPSHOT_TS,
                    )
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
                        .role(crate::buffer::PoolRole::Mem)
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
            let table = engine.catalog().get_table(table_id).await.unwrap();

            // Since we populate metadata table, we need to count those purge transactions and rows.
            // 100ms should be enough.
            smol::Timer::after(Duration::from_millis(1000)).await;
            let init_stats = engine.trx_sys.trx_sys_stats();

            let mut session = engine.new_session().unwrap();
            // insert
            for i in 0..PURGE_SIZE {
                let mut trx = session.begin_trx().unwrap();
                let res = trx
                    .exec(async |stmt| {
                        stmt_insert_row(stmt, &table, vec![Val::from(i as i32)]).await?;
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
                        stmt_delete_row(stmt, &table, &key).await?;
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
                let stats = engine.trx_sys.trx_sys_stats();
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
                    std::thread::sleep(Duration::from_millis(100));
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
                        .role(crate::buffer::PoolRole::Mem)
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
            let table = engine.catalog().get_table(table_id).await.unwrap();

            // Since we populate metadata table, we need to count those purge transactions and rows.
            // 100ms should be enough.
            smol::Timer::after(Duration::from_millis(100)).await;
            let init_stats = engine.trx_sys.trx_sys_stats();

            let mut session = engine.new_session().unwrap();
            // insert
            for i in 0..PURGE_SIZE {
                let mut trx = session.begin_trx().unwrap();
                let res = trx
                    .exec(async |stmt| {
                        stmt_insert_row(stmt, &table, vec![Val::from(i as i32)]).await?;
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
                        stmt_delete_row(stmt, &table, &key).await?;
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
                let stats = engine.trx_sys.trx_sys_stats();
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
                    std::thread::sleep(Duration::from_millis(100));
                }
            }
            if gc_timeout {
                // see which one is not purged, and its cts.
                let pool_guards = full_pool_guards(&engine);
                let index = bound_unique_index_no(&table, 0);
                let mut remained_row_ids = vec![];
                index
                    .scan_values(pool_guards.index_guard(), &mut remained_row_ids, 100)
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
                let mem_guard = engine.mem_pool.pool_guard();
                let page_guard: PageSharedGuard<RowPage> = engine
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
                engine.trx_sys.global_visible_sts()
            );
            drop(session);
        });
    }
}
