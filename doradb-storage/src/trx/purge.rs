use crate::buffer::page::PageID;
use crate::buffer::BufferPool;
use crate::catalog::{Catalog, TableCache};
use crate::latch::LatchFallbackMode;
use crate::row::{RowID, RowPage};
use crate::thread;
use crate::trx::log::LogPartition;
use crate::trx::sys::TransactionSystem;
use crate::trx::{CommittedTrx, TrxID, MAX_SNAPSHOT_TS};
use async_executor::LocalExecutor;
use crossbeam_utils::CachePadded;
use flume::{Receiver, Sender};
use parking_lot::Mutex;
use std::collections::HashSet;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread::JoinHandle;

impl<P: BufferPool> TransactionSystem<P> {
    #[inline]
    pub(super) fn start_purge_threads(
        &'static self,
        buf_pool: &'static P,
        catalog: &'static Catalog<P>,
        purge_chan: Receiver<Purge>,
    ) {
        if self.config.purge_threads == 1 {
            // single-threaded purger
            let handle = thread::spawn_named("Purge-Thread", move || {
                let ex = LocalExecutor::new();
                let mut purger = PurgeSingleThreaded::default();
                smol::block_on(ex.run(purger.purge_loop(buf_pool, catalog, self, purge_chan)))
            });
            let mut g = self.purge_threads.lock();
            g.push(handle);
        } else {
            // multi-threaded purger
            let (mut dispatcher, executors) = self.dispatch_purge(buf_pool, catalog);
            let handle = thread::spawn_named("Purge-Dispatcher", move || {
                let ex = LocalExecutor::new();
                smol::block_on(ex.run(dispatcher.purge_loop(buf_pool, catalog, self, purge_chan)));
            });
            let mut g = self.purge_threads.lock();
            g.push(handle);
            g.extend(executors);
        }
    }

    #[inline]
    pub(super) fn refresh_min_active_sts(&self) -> Option<TrxID> {
        // Refresh minimum active STS.
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
        min_ts = min_ts.min(max_active_sts);

        // update global min_active_sts
        let old_ts = self.min_active_sts.load(Ordering::Relaxed);
        if min_ts > old_ts {
            // Only single thread will update this watermark, so it's safe to overwrite
            // current value.
            self.min_active_sts.store(min_ts, Ordering::SeqCst);
            return Some(min_ts);
        }
        None
    }

    #[inline]
    pub(super) fn dispatch_purge(
        &'static self,
        buf_pool: &'static P,
        catalog: &'static Catalog<P>,
    ) -> (PurgeDispatcher, Vec<JoinHandle<()>>) {
        let mut handles = vec![];
        let mut chans = vec![];
        for i in 0..self.config.purge_threads {
            let (tx, rx) = flume::unbounded();
            chans.push(tx);
            let thread_name = format!("Purge-Executor-{}", i);
            let handle = thread::spawn_named(thread_name, move || {
                let mut purger = PurgeExecutor::default();
                let ex = LocalExecutor::new();
                smol::block_on(ex.run(purger.purge_task_loop(buf_pool, catalog, self, rx)));
            });
            handles.push(handle);
        }
        (PurgeDispatcher(chans), handles)
    }

    /// Purge row undo logs and index entries according to given transaction
    /// list and minimum active STS.
    #[inline]
    pub(super) async fn purge_trx_list(
        &self,
        buf_pool: &'static P,
        catalog: &Catalog<P>,
        log_no: usize,
        trx_list: Vec<CommittedTrx<P>>,
        min_active_sts: TrxID,
    ) {
        let partition = &self.log_partitions[log_no];
        let mut table_cache = TableCache::new(catalog);
        let purge_trx_count = trx_list.len();
        let mut purge_row_count = 0;
        let mut purge_index_count = 0;
        // First, we collect pages and row ids to purge row undo.
        let mut target: HashMap<PageID, HashSet<RowID>> = HashMap::new();
        for trx in &trx_list {
            purge_row_count += trx.row_undo.len();
            for undo in &*trx.row_undo {
                target.entry(undo.page_id).or_default().insert(undo.row_id);
            }
        }
        for (page_id, row_ids) in target {
            let page_guard = buf_pool
                .get_page::<RowPage>(page_id, LatchFallbackMode::Shared)
                .await
                .shared_async()
                .await;
            for row_id in row_ids {
                let mut access = page_guard.write_row_by_id(row_id);
                access.purge_undo_chain(min_active_sts);
            }
        }

        // Second, we purge index.
        for trx in &trx_list {
            for ip in &trx.index_gc {
                if let Some(table) = table_cache.get_table(ip.table_id) {
                    if table.delete_index(buf_pool, &ip.key, ip.row_id).await {
                        purge_index_count += 1;
                    }
                }
            }
        }
        // Finally, delete all transactions
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
    pub fn insert(&mut self, value: TrxID) {
        debug_assert!(self.active.is_empty() || self.active.back().unwrap() < &value);
        self.active.push_back(value);
    }

    /// Returns length of this list.
    #[inline]
    pub fn len(&self) -> usize {
        self.active.len()
    }

    /// Remove a snapshot timestamp from the list.
    /// Returns the smallest snapshot timestamp in the list.
    /// If list is empty, returns MAX_SNAPSHOT_TS.
    /// If it's not the smallest one, cache it in deleted set.
    /// This is an optimization to speed up retrieval of smallest snapshot timestamp.
    #[inline]
    pub fn remove(&mut self, value: TrxID) -> TrxID {
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

impl<P: BufferPool> LogPartition<P> {
    /// Execute GC analysis in loop.
    /// This method is used for a separate GC analyzer thread for each log partition.
    #[inline]
    pub fn gc_loop(&self, gc_rx: Receiver<GC<P>>, purge_chan: Sender<Purge>) {
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
    fn gc_analyze(&self, trx_list: HashMap<usize, Vec<CommittedTrx<P>>>) -> bool {
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
pub(super) struct GCBucket<P: BufferPool> {
    /// Committed transaction list.
    /// When a transaction is committed, it will be put into this queue in sequence.
    /// Head is always oldest and tail is newest.
    pub(super) committed_trx_list: CachePadded<Mutex<VecDeque<CommittedTrx<P>>>>,
    /// Active snapshot timestamp list.
    /// The smallest value equals to min_active_sts.
    pub(super) active_sts_list: CachePadded<Mutex<ActiveStsList>>,
    /// Minimum active snapshot sts of this bucket.
    pub(super) min_active_sts: CachePadded<AtomicU64>,
}

impl<P: BufferPool> GCBucket<P> {
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
    pub(super) fn get_purge_list(
        &self,
        min_active_sts: TrxID,
        trx_list: &mut Vec<CommittedTrx<P>>,
    ) {
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
    pub fn gc_analyze_rollback(&self, sts: TrxID) -> bool {
        debug_assert!(self.min_active_sts.load(Ordering::Relaxed) != MAX_SNAPSHOT_TS);
        let mut active_sts_list = self.active_sts_list.lock();
        let min_sts = active_sts_list.remove(sts);
        self.update_min_active_sts(min_sts)
    }

    /// Analyze committed transactions for GC.
    #[inline]
    pub fn gc_analyze_commit(&self, trx_list: Vec<CommittedTrx<P>>) -> bool {
        // Update both active sts list and committed transaction list
        let mut active_sts_list = self.active_sts_list.lock();
        let mut min_sts = MAX_SNAPSHOT_TS;
        {
            let mut committed_trx_list = self.committed_trx_list.lock();
            for trx in trx_list {
                min_sts = active_sts_list.remove(trx.sts);
                committed_trx_list.push_back(trx);
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

pub enum GC<P: BufferPool> {
    Stop,
    // transaction list per gc bucket.
    Commit(HashMap<usize, Vec<CommittedTrx<P>>>),
}

pub enum Purge {
    Stop,
    Next,
}

struct PurgeTask {
    log_no: usize,
    gc_no: usize,
    min_active_sts: TrxID,
    signal: Sender<()>,
}

trait PurgeLoop {
    async fn purge_loop<P: BufferPool>(
        &mut self,
        buf_pool: &'static P,
        catalog: &Catalog<P>,
        trx_sys: &TransactionSystem<P>,
        purge_chan: Receiver<Purge>,
    );
}

#[derive(Default)]
pub struct PurgeSingleThreaded;

impl PurgeLoop for PurgeSingleThreaded {
    #[inline]
    async fn purge_loop<P: BufferPool>(
        &mut self,
        buf_pool: &'static P,
        catalog: &Catalog<P>,
        trx_sys: &TransactionSystem<P>,
        purge_chan: Receiver<Purge>,
    ) {
        while let Ok(purge) = purge_chan.recv() {
            match purge {
                Purge::Stop => return,
                Purge::Next => {
                    // Cascade multiple Next message to avoid unnecessary work.
                    while let Ok(p) = purge_chan.try_recv() {
                        match p {
                            Purge::Stop => return,
                            Purge::Next => (),
                        }
                    }
                    if let Some(min_active_sts) = trx_sys.refresh_min_active_sts() {
                        for partition in &*trx_sys.log_partitions {
                            let mut trx_list = vec![];
                            for gc_bucket in &partition.gc_buckets {
                                gc_bucket.get_purge_list(min_active_sts, &mut trx_list);
                            }
                            let log_no = partition.log_no;
                            trx_sys
                                .purge_trx_list(buf_pool, catalog, log_no, trx_list, min_active_sts)
                                .await;
                        }
                    }
                }
            }
        }
    }
}

pub struct PurgeDispatcher(Vec<Sender<PurgeTask>>);

impl PurgeLoop for PurgeDispatcher {
    #[inline]
    async fn purge_loop<P: BufferPool>(
        &mut self,
        _buf_pool: &'static P,
        _catalog: &Catalog<P>,
        trx_sys: &TransactionSystem<P>,
        purge_chan: Receiver<Purge>,
    ) {
        // let chans = self.init(trx_sys);
        let mut dispatch_no: usize = 0;
        'DISPATCH_LOOP: while let Ok(purge) = purge_chan.recv_async().await {
            match purge {
                Purge::Stop => break 'DISPATCH_LOOP,
                Purge::Next => {
                    // Cascade multiple Next message to avoid unnecessary work.
                    while let Ok(p) = purge_chan.try_recv() {
                        match p {
                            Purge::Stop => break 'DISPATCH_LOOP,
                            Purge::Next => (),
                        }
                    }
                    if let Some(min_active_sts) = trx_sys.refresh_min_active_sts() {
                        // dispatch tasks to executors
                        let (signal, notify) = flume::unbounded();
                        for partition in &*trx_sys.log_partitions {
                            let log_no = partition.log_no;
                            for gc_no in 0..partition.gc_buckets.len() {
                                let task = PurgeTask {
                                    log_no,
                                    gc_no,
                                    min_active_sts,
                                    signal: signal.clone(),
                                };
                                let _ = self.0[dispatch_no % self.0.len()].send(task);
                                dispatch_no += 1;
                            }
                        }
                        drop(signal);
                        // wait for all executors finish their tasks.
                        let _ = notify.recv_async().await;
                    }
                }
            }
        }

        // notify executors to quit
        self.0.clear();
    }
}

#[derive(Default)]
pub struct PurgeExecutor;

impl PurgeExecutor {
    #[inline]
    async fn purge_task_loop<P: BufferPool>(
        &mut self,
        buf_pool: &'static P,
        catalog: &Catalog<P>,
        trx_sys: &TransactionSystem<P>,
        purge_chan: Receiver<PurgeTask>,
    ) {
        while let Ok(PurgeTask {
            log_no,
            gc_no,
            min_active_sts,
            signal,
        }) = purge_chan.recv()
        {
            let mut trx_list = vec![];
            let partition = &trx_sys.log_partitions[log_no];
            partition.gc_buckets[gc_no].get_purge_list(min_active_sts, &mut trx_list);
            // actual purge here
            trx_sys
                .purge_trx_list(buf_pool, catalog, log_no, trx_list, min_active_sts)
                .await;
            drop(signal); // notify dispatcher
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::guard::PageSharedGuard;
    use crate::engine::Engine;
    use crate::index::RowLocation;
    use crate::latch::LatchFallbackMode;
    use crate::lifetime::StaticLifetime;
    use crate::row::ops::SelectKey;
    use crate::row::RowPage;
    use crate::trx::sys::TrxSysConfig;
    use crate::value::Val;
    use std::time::{Duration, Instant};

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
    fn test_trx_purge_single_thread() {
        use crate::catalog::tests::table1;

        const PURGE_SIZE: usize = 1000;
        smol::block_on(async {
            let engine =
                Engine::new_fixed(16 * 1024 * 1024, TrxSysConfig::default().purge_threads(1))
                    .await
                    .unwrap();

            let table_id = table1(engine).await;
            let table = engine.catalog.get_table(table_id).unwrap();

            // Since we populate metadata table, we need to count those purge transactions and rows.
            // 100ms should be enough.
            smol::Timer::after(Duration::from_millis(100)).await;
            let init_stats = engine.trx_sys.trx_sys_stats();

            let mut session = engine.new_session();
            // insert
            for i in 0..PURGE_SIZE {
                let mut trx = session.begin_trx();
                let mut stmt = trx.start_stmt();
                let res = stmt.insert_row(&table, vec![Val::from(i as i32)]).await;
                assert!(res.is_ok());
                trx = stmt.succeed();
                let res = trx.commit().await;
                assert!(res.is_ok());
                session = res.unwrap();
            }
            // delete
            for i in 0..PURGE_SIZE {
                let mut trx = session.begin_trx();
                let mut stmt = trx.start_stmt();
                let key = SelectKey::new(0, vec![Val::from(i as i32)]);
                let res = stmt.delete_row(&table, &key).await;
                assert!(res.is_ok());
                trx = stmt.succeed();
                let res = trx.commit().await;
                assert!(res.is_ok());
                session = res.unwrap();
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
                if stats.purge_trx_count == init_stats.purge_trx_count + PURGE_SIZE * 2
                    && stats.purge_row_count == init_stats.purge_row_count + PURGE_SIZE * 2
                    && stats.purge_index_count == init_stats.purge_index_count + PURGE_SIZE
                {
                    break;
                }
                if start.elapsed() >= Duration::from_secs(3) {
                    panic!("gc timeout");
                } else {
                    std::thread::sleep(Duration::from_millis(100));
                }
            }
            unsafe {
                StaticLifetime::drop_static(engine);
            }
        });
    }

    #[test]
    fn test_trx_purge_multi_threads() {
        use crate::catalog::tests::table1;

        smol::block_on(async {
            const PURGE_SIZE: usize = 1000;
            let engine =
                Engine::new_fixed(16 * 1024 * 1024, TrxSysConfig::default().purge_threads(2))
                    .await
                    .unwrap();

            let table_id = table1(engine).await;
            let table = engine.catalog.get_table(table_id).unwrap();

            // Since we populate metadata table, we need to count those purge transactions and rows.
            // 100ms should be enough.
            smol::Timer::after(Duration::from_millis(100)).await;
            let init_stats = engine.trx_sys.trx_sys_stats();

            let mut session = engine.new_session();
            // insert
            for i in 0..PURGE_SIZE {
                let mut trx = session.begin_trx();
                let mut stmt = trx.start_stmt();
                let res = stmt.insert_row(&table, vec![Val::from(i as i32)]).await;
                assert!(res.is_ok());
                trx = stmt.succeed();
                let res = trx.commit().await;
                assert!(res.is_ok());
                session = res.unwrap();
            }
            // delete
            for i in 0..PURGE_SIZE {
                let mut trx = session.begin_trx();
                let mut stmt = trx.start_stmt();
                let key = SelectKey::new(0, vec![Val::from(i as i32)]);
                let res = stmt.delete_row(&table, &key).await;
                assert!(res.is_ok());
                trx = stmt.succeed();
                let res = trx.commit().await;
                assert!(res.is_ok());
                session = res.unwrap();
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
                if start.elapsed() >= Duration::from_secs(3) {
                    // panic!("gc timeout");
                    gc_timeout = true;
                    break;
                } else {
                    std::thread::sleep(Duration::from_millis(100));
                }
            }
            if gc_timeout {
                // see which one is not purged, and its cts.
                let index = table.sec_idx[0].unique().unwrap();
                let mut remained_row_ids = vec![];
                index.scan_values(&mut remained_row_ids);
                println!("gc timeout, remained_row_ids={:?}", remained_row_ids);
                let row_id = remained_row_ids[0];
                let location = table.blk_idx.find_row_id(&engine.buf_pool, row_id).await;
                let page_id = match location {
                    RowLocation::RowPage(page_id) => page_id,
                    _ => unreachable!(),
                };
                let page_guard: PageSharedGuard<RowPage> = engine
                    .buf_pool
                    .get_page(page_id, LatchFallbackMode::Shared)
                    .await
                    .shared_async()
                    .await;
                let access = page_guard.read_row_by_id(row_id);
                let status = access.latest_status();
                println!("row status={:?}", status);
            }
            println!(
                "final min_active_sts={}",
                engine.trx_sys.min_active_sts.load(Ordering::Relaxed)
            );
            unsafe {
                StaticLifetime::drop_static(engine);
            }
        });
    }
}
