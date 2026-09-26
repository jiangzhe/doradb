//! Bounded page-local replay on the existing finite-job pool.
//!
//! Pool acceptance transfers each batch and bitmap to the job. Completion is
//! authoritative even after poison: accepted jobs and their live I/O/eviction
//! dependencies produce progress. The coordinator owns settlement; cancellation
//! drops its observers and bootstrap rollback drains the pool before storage.

use super::RowReplayState;
use super::packed::{BatchPool, PackedPageBatch, ReplayOp};
use crate::buffer::PoolGuards;
use crate::completion::Completion;
use crate::conf::RecoveryConfig;
use crate::error::{
    CompletionResult, DataIntegrityError, RuntimeError, RuntimeOrFatalError, RuntimeOrFatalResult,
    RuntimeResult,
};
use crate::id::{PageID, TableID};
use crate::map::FastHashMap;
use crate::quiescent::QuiescentGuard;
use crate::runtime::thread_pool::ThreadPool;
use crate::stats::{RecoveryReport, recovery_add_count};
use crate::table::Table;
use error_stack::Report;
use futures::future::{BoxFuture, Either, select};
use futures::stream::FuturesUnordered;
use futures::{Future, FutureExt, StreamExt};
use std::collections::VecDeque;
use std::collections::hash_map::Entry;
use std::mem;
use std::sync::Arc;
#[cfg(test)]
pub(super) use tests::recycled_snapshot;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct PageKey {
    table_id: TableID,
    page_id: PageID,
}

type BatchCompletionFuture = BoxFuture<'static, BatchCompletion>;

/// Terminal batch result with the page identity retained by its observer.
struct BatchCompletion {
    page_key: PageKey,
    result: CompletionResult<RuntimeOrFatalResult<BatchOutput>>,
}

/// Successful hot-row mutations in a replay batch or collected recovery work.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct RowReplayCounts {
    /// Successfully applied inserts.
    pub(crate) inserts: u64,
    /// Successfully applied updates.
    pub(crate) updates: u64,
    /// Successfully applied deletes.
    pub(crate) deletes: u64,
}

impl RowReplayCounts {
    /// Returns whether no successful mutations have been recorded.
    pub(crate) fn is_empty(&self) -> bool {
        self.inserts == 0 && self.updates == 0 && self.deletes == 0
    }
}

struct ActivePage {
    table: Arc<Table>,
    // None while the outstanding job/completion owns the bitmap.
    state: Option<RowReplayState>,
    pending: Option<PackedPageBatch>,
    ready_queued: bool,
}

#[derive(Default)]
struct TableWork {
    submitted_batches: usize,
    pending_ops: usize,
}

struct BatchOutput {
    state: RowReplayState,
    counts: RowReplayCounts,
    batch: PackedPageBatch,
}

struct ReplayLimits {
    tasks: usize,
    pages: usize,
    batch_ops: usize,
    batch_bytes: usize,
}

impl ReplayLimits {
    fn new(config: &RecoveryConfig) -> Self {
        // Engine validation resolves automatic limits before recovery starts.
        Self {
            tasks: config
                .max_in_flight_batches
                .expect("validated recovery batch limit"),
            pages: config
                .max_active_pages
                .expect("validated recovery active-page limit"),
            batch_ops: config.max_batch_ops,
            batch_bytes: config.target_batch_bytes,
        }
    }
}

/// Recovery-owned scheduling, retained history, and exclusive result collection.
pub(super) struct ReplayDispatcher {
    /// Surviving page histories, including empty creations, outside active scheduling.
    pub(super) page_history: FastHashMap<TableID, FastHashMap<PageID, RowReplayState>>,
    active_pages: FastHashMap<PageKey, ActivePage>,
    active_tables: FastHashMap<TableID, TableWork>,
    ready: VecDeque<PageKey>,
    in_flight: FuturesUnordered<BatchCompletionFuture>,
    limits: ReplayLimits,
    recycled: BatchPool,
    pool: QuiescentGuard<ThreadPool>,
    guards: PoolGuards,
    disable_validation: bool,
    counts: RowReplayCounts,
    saturated: bool,
    #[cfg(test)]
    test_hook: Option<tests::BatchHook>,
}

impl ReplayDispatcher {
    /// Bind the already-running pool and validated recovery admission limits.
    pub(super) fn new(
        pool: QuiescentGuard<ThreadPool>,
        guards: PoolGuards,
        config: &RecoveryConfig,
    ) -> Self {
        let limits = ReplayLimits::new(config);
        let recycled = BatchPool::new(
            config.target_batch_bytes,
            config.max_recycled_bytes,
            limits.pages.saturating_add(limits.tasks),
        );
        Self {
            page_history: FastHashMap::default(),
            active_pages: FastHashMap::default(),
            active_tables: FastHashMap::default(),
            ready: VecDeque::new(),
            in_flight: FuturesUnordered::new(),
            limits,
            recycled,
            pool,
            guards,
            disable_validation: config.disable_dml_validation,
            counts: RowReplayCounts::default(),
            saturated: false,
            #[cfg(test)]
            test_hook: tests::installed_hook(),
        }
    }

    /// Admit one eligible operation, stopping at this input on scheduling pressure.
    pub(super) async fn admit(
        &mut self,
        table: &Arc<Table>,
        page_id: PageID,
        op: ReplayOp<'_>,
    ) -> RuntimeOrFatalResult<()> {
        let key = PageKey {
            table_id: table.table_id(),
            page_id,
        };
        let op_bytes = op.used_bytes();
        loop {
            self.reap()?;
            let can_activate = self.active_pages.len() < self.limits.pages;
            let page = match self.active_pages.entry(key) {
                Entry::Occupied(entry)
                    if match &entry.get().pending {
                        None => true,
                        Some(batch) => {
                            batch.len() < self.limits.batch_ops
                                && batch.used_bytes() + op_bytes <= self.limits.batch_bytes
                        }
                    } =>
                {
                    Some(entry.into_mut())
                }
                Entry::Vacant(entry) if can_activate => {
                    // Removing page history also validates page registration.
                    let state = self
                        .page_history
                        .get_mut(&key.table_id)
                        .and_then(|pages| pages.remove(&key.page_id))
                        .ok_or_else(|| {
                            Report::new(DataIntegrityError::InvalidRootInvariant)
                                .attach(format!("missing row replay state: table_id={}, page_id={page_id}, row_id={}, cts={}", key.table_id, op.row_id, op.cts))
                                .change_context(RuntimeError::Recovery)
                        })?;
                    Some(entry.insert(ActivePage {
                        table: Arc::clone(table),
                        state: Some(state),
                        pending: None,
                        ready_queued: false,
                    }))
                }
                _ => None,
            };
            if let Some(page) = page {
                let batch = page.pending.get_or_insert_with(|| self.recycled.acquire());
                batch.append(&op, self.limits.batch_bytes);
                let flush = batch.len() == self.limits.batch_ops
                    || batch.used_bytes() >= self.limits.batch_bytes;
                self.active_tables
                    .entry(key.table_id)
                    .or_default()
                    .pending_ops += 1;
                if page.state.is_some() && !page.ready_queued {
                    page.ready_queued = true;
                    self.ready.push_back(key);
                }
                if flush {
                    self.progress()?;
                }
                break;
            }
            // Pressure flushes partial batches too. Recheck admission after
            // immediate completions before parking on an accepted job.
            if self.progress()? {
                continue;
            }
            self.wait_one().await?;
        }
        Ok(())
    }

    fn pump(&mut self) -> bool {
        let mut submitted = false;
        // Only idle-with-pending pages enter ready; collection retires a page
        // only with an empty pending slot. Thus ready keys and table counters
        // below must exist, and taking state excludes overlapping page jobs.
        while self.in_flight.len() < self.limits.tasks {
            let Some(key) = self.ready.pop_front() else {
                break;
            };
            let page = self
                .active_pages
                .get_mut(&key)
                .expect("recovery ready page exists");
            page.ready_queued = false;
            let state = page.state.take().expect("recovery ready page owns history");
            let batch = page
                .pending
                .take()
                .expect("recovery ready page owns pending batch");
            let work = self
                .active_tables
                .get_mut(&key.table_id)
                .expect("recovery active table exists");
            work.pending_ops -= batch.len();
            work.submitted_batches += 1;
            let table = Arc::clone(&page.table);
            let guards = self.guards.clone();
            let disable_validation = self.disable_validation;
            let completion = self.pool.submit_async(replay_page_batch(
                table,
                guards,
                state,
                batch,
                disable_validation,
                #[cfg(test)]
                self.test_hook.clone(),
            ));
            submitted = true;
            self.in_flight
                .push(wait_batch_completion(key, completion).boxed());
        }
        submitted
    }

    fn collect(&mut self, completion: BatchCompletion) -> RuntimeOrFatalResult<()> {
        let BatchCompletion {
            page_key: key,
            result,
        } = completion;
        // Each waiter is consumed once; submission retains its admission slot
        // and table/page entries until this terminal result is collected.
        let work = self
            .active_tables
            .get_mut(&key.table_id)
            .expect("recovery completion table exists");
        work.submitted_batches -= 1;
        if work.submitted_batches == 0 && work.pending_ops == 0 {
            self.active_tables.remove(&key.table_id);
        }
        let output = result.map_err(|bridge| {
            bridge
                .into_runtime_or_fatal(RuntimeError::Recovery)
                .attach_with(|| {
                    format!(
                        "operation=recovery_batch_completion, table_id={}, page_id={}",
                        key.table_id, key.page_id
                    )
                })
        })??;
        recovery_add_count(
            &mut self.counts.inserts,
            output.counts.inserts,
            &mut self.saturated,
        );
        recovery_add_count(
            &mut self.counts.updates,
            output.counts.updates,
            &mut self.saturated,
        );
        recovery_add_count(
            &mut self.counts.deletes,
            output.counts.deletes,
            &mut self.saturated,
        );
        let page = self
            .active_pages
            .get_mut(&key)
            .expect("recovery completion page exists");
        // This completion exclusively returns the state taken at submission.
        if page.pending.is_none() {
            self.active_pages.remove(&key);
            self.page_history
                .entry(key.table_id)
                .or_default()
                .insert(key.page_id, output.state);
        } else {
            page.state = Some(output.state);
            page.ready_queued = true;
            self.ready.push_back(key);
        }
        self.recycled.recycle(output.batch);
        #[cfg(test)]
        if let Some(hook) = &self.test_hook {
            hook.collected.send(key).unwrap();
        }
        Ok(())
    }

    fn reap(&mut self) -> RuntimeOrFatalResult<bool> {
        let mut collected = false;
        while let Some(Some(result)) = self.in_flight.next().now_or_never() {
            self.collect(result)?;
            collected = true;
        }
        Ok(collected)
    }

    /// Flush eligible FIFO work and register/reap its completion observers.
    pub(super) fn progress(&mut self) -> RuntimeOrFatalResult<bool> {
        let mut progressed = self.reap()?;
        loop {
            progressed |= self.pump();
            if !self.reap()? {
                return Ok(progressed);
            }
            progressed = true;
        }
    }

    // Accepted jobs, including I/O/latch waiters, own progress. Terminal results
    // and the counters are authoritative, never a notification or poison alone.
    // Cancellation leaves captures pool-owned; RegistryBuilder rollback drains.
    async fn wait_one(&mut self) -> RuntimeOrFatalResult<()> {
        let result = self
            .in_flight
            .next()
            .await
            .expect("recovery pressure must have a progress producer");
        self.collect(result)
    }

    /// Complete earlier parsed work for one table.
    pub(super) async fn drain(&mut self, table_id: TableID) -> RuntimeOrFatalResult<()> {
        loop {
            self.progress()?;
            if !self.active_tables.contains_key(&table_id) {
                return Ok(());
            }
            self.wait_one().await?;
        }
    }

    /// Complete earlier parsed work for all tables at EOF.
    pub(super) async fn drain_all(&mut self) -> RuntimeOrFatalResult<()> {
        loop {
            self.progress()?;
            if self.active_tables.is_empty() {
                self.recycled.clear();
                return Ok(());
            }
            self.wait_one().await?;
        }
    }

    /// Wait for input while collecting completions and scheduling pending replay.
    ///
    /// The same input future stays pinned across worker completions; collecting a
    /// completion never cancels or restarts the input operation. Unless a worker
    /// error or cancellation ends the wait, input is driven to completion.
    ///
    /// A successful return means the input operation completed. Processing or
    /// replaying its returned value belongs to the caller, as do draining at EOF
    /// and settling accepted work after an error.
    pub(super) async fn read_next<T>(
        &mut self,
        input: impl Future<Output = RuntimeResult<T>>,
    ) -> RuntimeOrFatalResult<T> {
        futures::pin_mut!(input);
        loop {
            self.progress()?;
            if self.in_flight.is_empty() {
                return input.await.map_err(Into::into);
            }
            match select(input.as_mut(), self.in_flight.next()).await {
                Either::Left((result, _)) => return result.map_err(Into::into),
                Either::Right((Some(result), _)) => self.collect(result)?,
                Either::Right((None, _)) => unreachable!("recovery input race lost all jobs"),
            }
        }
    }

    /// Abandon pending work, then drain accepted jobs without any new submission.
    pub(super) async fn settle(&mut self, mut error: RuntimeOrFatalError) -> RuntimeOrFatalError {
        self.ready.clear();
        for page in self.active_pages.values_mut() {
            page.pending = None;
            page.ready_queued = false;
        }
        for work in self.active_tables.values_mut() {
            work.pending_ops = 0;
        }
        while let Some(result) = self.in_flight.next().await {
            if let Err(cleanup) = self.collect(result) {
                error = error.merge_cleanup(cleanup);
            }
        }
        self.active_pages.clear();
        self.active_tables.clear();
        self.recycled.clear();
        error
    }

    /// Merge successfully collected hot work once, retaining report saturation.
    pub(super) fn merge_counts(&mut self, report: &mut RecoveryReport) {
        let counts = mem::take(&mut self.counts);
        recovery_add_count(
            &mut report.work.hot_inserts,
            counts.inserts,
            &mut report.saturated,
        );
        recovery_add_count(
            &mut report.work.hot_updates,
            counts.updates,
            &mut report.saturated,
        );
        recovery_add_count(
            &mut report.work.hot_deletes,
            counts.deletes,
            &mut report.saturated,
        );
        report.saturated |= self.saturated;
    }
}

/// Run a finite page job with owned captures that the pool drops before completion.
async fn replay_page_batch(
    table: Arc<Table>,
    guards: PoolGuards,
    mut state: RowReplayState,
    batch: PackedPageBatch,
    disable_validation: bool,
    #[cfg(test)] test_hook: Option<tests::BatchHook>,
) -> RuntimeOrFatalResult<BatchOutput> {
    #[cfg(test)]
    let key = PageKey {
        table_id: table.table_id(),
        page_id: state.page_id(),
    };
    #[cfg(test)]
    if let Some(hook) = &test_hook {
        hook.before(key).await;
    }
    let counts = table
        .recover_row_batch(&guards, &mut state, &batch, disable_validation)
        .await?;
    #[cfg(test)]
    if let Some(hook) = &test_hook {
        hook.finished.send(key).unwrap();
    }
    Ok(BatchOutput {
        state,
        counts,
        batch,
    })
}

/// Keep the page identity available even when the job fails.
async fn wait_batch_completion(
    key: PageKey,
    completion: Arc<Completion<RuntimeOrFatalResult<BatchOutput>>>,
) -> BatchCompletion {
    BatchCompletion {
        page_key: key,
        result: completion.wait_take_result().await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::guard::{PageExclusiveGuard, PageGuard};
    use crate::conf::{EngineConfig, ThreadPoolConfig};
    use crate::engine::Engine;
    use crate::error::FatalError;
    use crate::id::RowID;
    use crate::id::TrxID;
    use crate::log::redo::RowRedo;
    use crate::log::redo::RowRedoKind;
    use crate::recovery::packed::{OwnedReplayOp, decode_test_op, pool_snapshot};
    use crate::row::ops::UpdateCol;
    use crate::row::{RowPage, RowRead};
    use crate::table::RowPageDescriptor;
    use crate::table::tests::{create_table2_for_test, lightweight_test_engine_config};
    use crate::value::Val;
    use futures::future::poll_fn;
    use std::cell::RefCell;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll};
    use std::thread;
    use tempfile::TempDir;

    thread_local! {
        static BATCH_HOOK: RefCell<Option<BatchHook>> = const { RefCell::new(None) };
    }

    /// Per-dispatcher gates for accepted execution and terminal collection.
    #[derive(Clone)]
    pub(super) struct BatchHook {
        started: flume::Sender<PageKey>,
        release: flume::Receiver<()>,
        /// Signals successful page application, before job capture release.
        pub(super) finished: flume::Sender<PageKey>,
        /// Signals consumed completion and scheduler retirement or requeue.
        pub(super) collected: flume::Sender<PageKey>,
        observer_dropped: flume::Sender<()>,
        panic: bool,
    }

    impl BatchHook {
        /// Park the accepted batch before page acquisition.
        pub(super) async fn before(&self, key: PageKey) {
            self.started.send(key).unwrap();
            self.release.recv_async().await.unwrap();
            assert!(!self.panic, "injected replay worker panic");
        }
    }

    struct Gate {
        hook: BatchHook,
        started: flume::Receiver<PageKey>,
        release: flume::Sender<()>,
        finished: flume::Receiver<PageKey>,
        collected: flume::Receiver<PageKey>,
        observer_dropped: flume::Receiver<()>,
    }

    impl Gate {
        fn new() -> Self {
            let (started_tx, started) = flume::unbounded();
            let (release, release_rx) = flume::unbounded();
            let (finished_tx, finished) = flume::unbounded();
            let (collected_tx, collected) = flume::unbounded();
            let (observer_tx, observer_dropped) = flume::unbounded();
            Self {
                hook: BatchHook {
                    started: started_tx,
                    release: release_rx,
                    finished: finished_tx,
                    collected: collected_tx,
                    observer_dropped: observer_tx,
                    panic: false,
                },
                started,
                release,
                finished,
                collected,
                observer_dropped,
            }
        }

        fn poll_collection<F: Future>(&self, input: Pin<&mut F>, cx: &mut Context<'_>) -> Poll<()> {
            assert!(input.poll(cx).is_pending());
            // This hook runs only after the authoritative completion was consumed.
            if self.collected.try_recv().is_ok() {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }

        fn run_cancelled_bootstrap(&self, config: &EngineConfig) {
            BATCH_HOOK.with(|hook| *hook.borrow_mut() = Some(self.hook.clone()));
            let _installed = InstalledHook;
            smol::block_on(self.cancel_bootstrap_after_batch_start(config));
            assert_eq!(
                self.finished.len(),
                1,
                "bootstrap teardown lost accepted replay"
            );
        }

        async fn cancel_bootstrap_after_batch_start(&self, config: &EngineConfig) {
            let mut bootstrap = Box::pin(Engine::bootstrap(config.clone()));
            let started = Box::pin(self.started.recv_async());
            match select(bootstrap.as_mut(), started).await {
                Either::Left(_) => panic!("bootstrap returned before blocked replay"),
                Either::Right((started, _)) => {
                    started.unwrap();
                }
            }
            drop(bootstrap); // RegistryBuilder rollback waits for the accepted job.
        }

        fn release_after_observer_drop(&self) {
            // Dispatcher Drop proves cancellation happened while the accepted
            // job still owned table/pool captures; no elapsed-time gate.
            self.observer_dropped.recv().unwrap();
            assert!(self.finished.is_empty());
            self.release.send(()).unwrap();
        }
    }

    impl Drop for ReplayDispatcher {
        fn drop(&mut self) {
            if let Some(hook) = &self.test_hook {
                let _ = hook.observer_dropped.send(());
            }
        }
    }

    struct InstalledHook;

    impl Drop for InstalledHook {
        fn drop(&mut self) {
            BATCH_HOOK.with(|hook| *hook.borrow_mut() = None);
        }
    }

    struct ReadGuard(Arc<AtomicUsize>);

    impl Drop for ReadGuard {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct Fixture {
        dispatch: ReplayDispatcher,
        table: Arc<Table>,
        pages: Vec<(PageID, RowID)>,
        engine: Engine,
        _temp: TempDir,
    }

    impl Fixture {
        async fn new(pages: usize, workers: usize) -> Self {
            Self::with_config(pages, workers, RecoveryConfig::default()).await
        }

        async fn with_config(pages: usize, workers: usize, recovery: RecoveryConfig) -> Self {
            let temp = TempDir::new().unwrap();
            let config = lightweight_test_engine_config(temp.path().to_path_buf(), "dispatch")
                .thread_pool(ThreadPoolConfig::default().worker_threads(workers))
                .recovery(recovery)
                .validate()
                .unwrap();
            let recovery = config.recovery.clone();
            let engine = Engine::bootstrap(config).await.unwrap();
            let table_id = create_table2_for_test(&engine).await;
            let table = engine.inner().core.catalog().get_table(table_id).unwrap();
            let guards = engine.inner().core.pools.pool_guards().clone();
            let mut dispatch =
                ReplayDispatcher::new(engine.inner().thread_pool.clone(), guards, &recovery);
            let mut allocated = Vec::new();
            for index in 0..pages {
                let page_id = PageID::new(100 + index as u64);
                let page = table
                    .row_store
                    .allocate_row_page_at(&dispatch.guards, 128, page_id)
                    .await
                    .unwrap();
                let start = page.page().header.start_row_id;
                dispatch.page_history.entry(table_id).or_default().insert(
                    page_id,
                    RowReplayState::new(RowPageDescriptor {
                        page_id,
                        start_row_id: start,
                        end_row_id: start + 128,
                    }),
                );
                allocated.push((page_id, start));
            }
            Self {
                dispatch,
                table,
                pages: allocated,
                engine,
                _temp: temp,
            }
        }

        async fn lock(&self, index: usize) -> PageExclusiveGuard<RowPage> {
            self.table
                .row_store
                .must_get_row_page_exclusive(&self.dispatch.guards, self.pages[index].0)
                .await
                .unwrap()
        }

        async fn clear_dirty(&self, index: usize) {
            let guard = self.lock(index).await;
            guard.bf().set_dirty(false);
            assert!(!guard.is_dirty());
        }

        async fn insert(&mut self, index: usize, slot: u64) {
            let (page, first) = self.pages[index];
            admit_test_op(
                &mut self.dispatch,
                &self.table,
                page,
                insert(page, first + slot, "initial"),
            )
            .await
            .unwrap();
        }

        fn assert_idle(&self) {
            assert!(self.dispatch.active_pages.is_empty());
            assert!(self.dispatch.active_tables.is_empty());
            assert!(self.dispatch.ready.is_empty());
            assert!(self.dispatch.in_flight.is_empty());
        }
    }

    /// Copy the test-thread bootstrap hook into the new dispatcher.
    pub(super) fn installed_hook() -> Option<BatchHook> {
        BATCH_HOOK.with(|hook| hook.borrow().clone())
    }

    /// Observe idle storage independently of page histories in lifecycle tests.
    pub(in crate::recovery) fn recycled_snapshot(dispatch: &ReplayDispatcher) -> (usize, usize) {
        pool_snapshot(&dispatch.recycled)
    }

    async fn admit_test_op(
        dispatch: &mut ReplayDispatcher,
        table: &Arc<Table>,
        page: PageID,
        op: OwnedReplayOp,
    ) -> RuntimeOrFatalResult<()> {
        let cts = op.cts;
        let (group, row) = decode_test_op(op);
        dispatch
            .admit(table, page, group.operation(&row, cts))
            .await
    }

    async fn observed_input(
        receive: &flume::Receiver<()>,
        polls: &AtomicUsize,
        drops: &Arc<AtomicUsize>,
    ) -> RuntimeResult<i32> {
        let _guard = ReadGuard(Arc::clone(drops));
        polls.fetch_add(1, Ordering::SeqCst);
        receive.recv_async().await.unwrap();
        Ok(17)
    }

    async fn prepare_cancelled_replay(config: &EngineConfig) {
        let engine = Engine::bootstrap(config.clone()).await.unwrap();
        let table = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        let mut trx = session.begin_trx().unwrap();
        trx.table_insert_mvcc(table, vec![Val::from(7i32), Val::from("committed")])
            .await
            .unwrap();
        trx.commit().await.unwrap();
    }

    async fn verify_replay_after_cancellation(config: EngineConfig) {
        let engine = Engine::bootstrap(config).await.unwrap();
        assert_eq!(engine.recovery_report().work.hot_inserts, 1);
    }

    fn insert(page: PageID, row_id: RowID, value: &str) -> OwnedReplayOp {
        OwnedReplayOp {
            cts: TrxID::new(10),
            row: RowRedo {
                row_id,
                kind: RowRedoKind::Insert(
                    page,
                    vec![Val::from(row_id.as_u64() as i32), Val::from(value)],
                ),
            },
        }
    }

    fn update(page: PageID, row_id: RowID, value: &str) -> OwnedReplayOp {
        OwnedReplayOp {
            cts: TrxID::new(11),
            row: RowRedo {
                row_id,
                kind: RowRedoKind::Update(
                    page,
                    vec![UpdateCol {
                        idx: 1,
                        val: Val::from(value),
                    }],
                ),
            },
        }
    }

    fn runtime_error() -> RuntimeOrFatalError {
        Report::new(DataIntegrityError::InvalidPayload)
            .attach("injected parser/DDL failure")
            .change_context(RuntimeError::Recovery)
            .into()
    }

    fn insert_bytes(value: &str) -> usize {
        let op = insert(PageID::new(100), RowID::new(0), value);
        let cts = op.cts;
        let (group, row) = decode_test_op(op);
        group.operation(&row, cts).used_bytes()
    }

    /// Purpose: Exercise restart with minimal replay capacity and either DML validation setting.
    /// Expected: Bootstrap replays committed user and catalog operations successfully under both settings.
    #[test]
    fn test_restart_with_single_operation_replay_limits() {
        smol::block_on(async {
            for disable in [false, true] {
                let temp = TempDir::new().unwrap();
                let config =
                    lightweight_test_engine_config(temp.path().to_path_buf(), "tiny-replay")
                        .recovery(
                            RecoveryConfig::default()
                                .io_depth(1)
                                .disable_dml_validation(disable)
                                .max_in_flight_batches(Some(1))
                                .max_active_pages(Some(1))
                                .max_batch_ops(1),
                        );
                prepare_cancelled_replay(&config).await;
                let engine = Engine::bootstrap(config).await.unwrap();
                assert_eq!(engine.recovery_report().work.hot_inserts, 1);
                assert!(engine.recovery_report().work.catalog_row_ops_applied > 0);
            }
        });
    }

    /// Purpose: Preserve per-page replay ordering while independent pages progress and retired pages reactivate.
    /// Expected: Ordered mutations retain insertion history, and reinserting a deleted slot is rejected.
    #[test]
    fn test_page_order_concurrency_and_retained_history() {
        smol::block_on(async {
            let mut f =
                Fixture::with_config(3, 2, RecoveryConfig::default().max_batch_ops(1)).await;
            let held = f.lock(0).await;
            f.insert(0, 69).await;
            f.insert(1, 3).await;
            let (page, first) = f.pages[0];
            let key = PageKey {
                table_id: f.table.table_id(),
                page_id: page,
            };
            while !f.dispatch.page_history[&key.table_id].contains_key(&f.pages[1].0) {
                f.dispatch.wait_one().await.unwrap();
            }
            assert!(f.dispatch.active_pages[&key].state.is_none());
            // A successor remains pending while its predecessor owns the bitmap.
            admit_test_op(
                &mut f.dispatch,
                &f.table,
                page,
                update(page, first + 69, "a larger replacement value"),
            )
            .await
            .unwrap();
            assert_eq!(f.dispatch.in_flight.len(), 1);
            let deletion = OwnedReplayOp {
                cts: TrxID::new(12),
                row: RowRedo {
                    row_id: first + 69,
                    kind: RowRedoKind::Delete(Some(page)),
                },
            };
            let mut admission = Box::pin(admit_test_op(&mut f.dispatch, &f.table, page, deletion));
            assert!(admission.as_mut().now_or_never().is_none());
            drop(held);
            admission.await.unwrap();
            f.dispatch.drain_all().await.unwrap();
            f.assert_idle();
            assert!(f.dispatch.page_history[&key.table_id][&page].is_inserted(69));
            assert!(f.lock(0).await.page().is_deleted(69));
            // Empty page 2 survives and a fresh slot reactivates page 0.
            assert_eq!(f.dispatch.page_history[&key.table_id].len(), 3);
            f.insert(0, 1).await;
            f.dispatch.drain_all().await.unwrap();
            f.assert_idle();
            assert_eq!(
                f.dispatch.counts,
                RowReplayCounts {
                    inserts: 3,
                    updates: 1,
                    deletes: 1
                }
            );
            let result = admit_test_op(
                &mut f.dispatch,
                &f.table,
                page,
                insert(page, first + 69, "duplicate after deletion"),
            )
            .await;
            let error = match result {
                Err(error) => error,
                Ok(()) => f.dispatch.drain_all().await.unwrap_err(),
            };
            assert!(format!("{error:?}").contains("row slot was already inserted"));
            f.dispatch.settle(error).await;
            f.assert_idle();
        });
    }

    /// Purpose: Exercise replay admission under batch and active-page pressure.
    /// Expected: Partial batches are submitted, excess admission waits, and completion permits replay to drain.
    #[test]
    fn test_admission_bounds_flush_partial_batches_before_waiting() {
        smol::block_on(async {
            for pressure in ["batch", "page"] {
                let mut config = RecoveryConfig::default()
                    .max_in_flight_batches(Some(1))
                    .max_batch_ops(2);
                if pressure == "page" {
                    config = config.max_active_pages(Some(1));
                }
                let mut f = Fixture::with_config(3, 1, config).await;
                let held = f.lock(0).await;
                f.insert(0, 0).await;
                // This transaction-end flush must submit an underfilled batch.
                f.dispatch.progress().unwrap();
                assert_eq!(f.dispatch.in_flight.len(), 1);
                if pressure == "batch" {
                    f.insert(0, 1).await;
                    f.insert(0, 2).await;
                    assert_eq!(
                        f.dispatch.active_pages[&PageKey {
                            table_id: f.table.table_id(),
                            page_id: f.pages[0].0,
                        }]
                            .pending
                            .as_ref()
                            .map_or(0, PackedPageBatch::len),
                        2
                    );
                    assert_eq!(f.dispatch.in_flight.len(), 1);
                }
                let index = usize::from(pressure != "batch");
                let (page, first) = f.pages[index];
                let mut admission = Box::pin(admit_test_op(
                    &mut f.dispatch,
                    &f.table,
                    page,
                    insert(page, first + 3, "initial"),
                ));
                assert!(admission.as_mut().now_or_never().is_none(), "{pressure}");
                drop(held);
                admission.await.unwrap();
                f.dispatch.drain_all().await.unwrap();
                f.assert_idle();
            }
        });
    }

    /// Purpose: Preserve ready-page order when partial and full batches compete for a submission slot.
    /// Expected: The older partial batch runs first and its collected completion releases the slot.
    #[test]
    fn test_fifo_partial_batches_and_completion_credits() {
        smol::block_on(async {
            let mut f = Fixture::with_config(
                3,
                1,
                RecoveryConfig::default()
                    .max_in_flight_batches(Some(1))
                    .max_batch_ops(2),
            )
            .await;
            let held = f.lock(0).await;
            f.insert(0, 0).await;
            f.dispatch.progress().unwrap();
            f.insert(1, 0).await; // older partial batch
            f.insert(2, 0).await;
            f.insert(2, 1).await; // later full batch
            let key = |idx: usize| PageKey {
                table_id: f.table.table_id(),
                page_id: f.pages[idx].0,
            };
            assert_eq!(
                f.dispatch.ready.iter().copied().collect::<Vec<_>>(),
                vec![key(1), key(2)]
            );
            assert_eq!(f.dispatch.in_flight.len(), 1);
            let held1 = f.lock(1).await;
            drop(held);
            f.dispatch.wait_one().await.unwrap();
            f.dispatch.pump();
            assert!(f.dispatch.active_pages[&key(1)].state.is_none());
            assert!(f.dispatch.active_pages[&key(2)].state.is_some());
            assert_eq!(f.dispatch.in_flight.len(), 1);
            drop(held1);
            // Collection releases the submission slot before the next page is pumped.
            f.dispatch.wait_one().await.unwrap();
            assert!(f.dispatch.in_flight.is_empty());
            assert!(!f.dispatch.active_pages.contains_key(&key(1)));
            f.dispatch.drain_all().await.unwrap();
            f.assert_idle();
        });
    }

    /// Purpose: Scope replay barriers to their table with multiple jobs on a single worker.
    /// Expected: Unrelated table barriers return immediately, while the affected barrier drains jobs and releases captures.
    #[test]
    fn test_multiple_outstanding_jobs_with_one_worker_and_table_barrier() {
        smol::block_on(async {
            let mut f = Fixture::with_config(
                3,
                1,
                RecoveryConfig::default()
                    .max_in_flight_batches(Some(2))
                    .max_batch_ops(2),
            )
            .await;
            let held0 = f.lock(0).await;
            let held1 = f.lock(1).await;
            f.insert(0, 0).await;
            f.dispatch.progress().unwrap();
            f.insert(1, 0).await;
            f.dispatch.progress().unwrap();
            f.insert(2, 0).await;
            f.dispatch.progress().unwrap();
            assert_eq!(f.dispatch.in_flight.len(), 2);
            assert_eq!(f.dispatch.ready.len(), 1);
            assert_eq!(f.dispatch.active_tables[&f.table.table_id()].pending_ops, 1);
            // Waiting on unrelated table U does not wait for T's blocked jobs.
            let other = create_table2_for_test(&f.engine).await;
            f.dispatch.drain(other).await.unwrap();
            assert_eq!(f.dispatch.in_flight.len(), 2);
            let mut drain = Box::pin(f.dispatch.drain(f.table.table_id()));
            assert!(drain.as_mut().now_or_never().is_none());
            drop(held0);
            drop(held1);
            drain.await.unwrap();
            f.assert_idle();
            assert_eq!(Arc::strong_count(&f.table), 2); // fixture + catalog, no worker handle
        });
    }

    /// Purpose: Allow a mixed-size payload batch to progress independently of a blocked page.
    /// Expected: The independent batch applies its values correctly before the blocked page is released.
    #[test]
    fn test_mixed_payload_batch_progresses_while_another_page_is_blocked() {
        smol::block_on(async {
            let mut f = Fixture::with_config(
                2,
                1,
                RecoveryConfig::default()
                    .max_in_flight_batches(Some(2))
                    .max_batch_ops(2),
            )
            .await;
            let held0 = f.lock(0).await;
            f.insert(0, 0).await;
            f.dispatch.progress().unwrap();
            let held1 = f.lock(1).await;
            let (page, first) = f.pages[1];
            let key = PageKey {
                table_id: f.table.table_id(),
                page_id: page,
            };
            let large_value = "x".repeat(2048);
            admit_test_op(
                &mut f.dispatch,
                &f.table,
                page,
                insert(page, first, &large_value),
            )
            .now_or_never()
            .expect("payload size must not wait for the blocked page")
            .unwrap();
            assert_eq!(
                f.dispatch.active_pages[&key]
                    .pending
                    .as_ref()
                    .map_or(0, PackedPageBatch::len),
                1
            );
            assert_eq!(f.dispatch.in_flight.len(), 1);
            // The second operation fills the batch despite its smaller payload.
            f.insert(1, 1).await;
            assert!(f.dispatch.active_pages[&key].pending.is_none());
            assert_eq!(f.dispatch.in_flight.len(), 2);
            assert!(f.dispatch.active_pages[&key].state.is_none());
            drop(held1);
            f.dispatch.wait_one().await.unwrap();
            assert_eq!(f.dispatch.counts.inserts, 2);
            assert_eq!(f.dispatch.in_flight.len(), 1);
            let page = f.lock(1).await;
            let layout = f.table.layout_snapshot();
            let metadata = layout.metadata();
            assert_eq!(
                page.page().row(0).val(&metadata.col, 1),
                Val::from(large_value.as_str())
            );
            assert_eq!(
                page.page().row(1).val(&metadata.col, 1),
                Val::from("initial")
            );
            drop(page);
            drop(held0);
            f.dispatch.drain_all().await.unwrap();
            f.assert_idle();
            assert_eq!(f.dispatch.counts.inserts, 3);
        });
    }

    /// Purpose: Settle a replay failure with both accepted and unsubmitted work.
    /// Expected: Accepted jobs complete, pending mutations are discarded, and the original failure survives.
    #[test]
    fn test_failure_discards_pending_and_drains_accepted_jobs() {
        smol::block_on(async {
            let mut f = Fixture::new(2, 1).await;
            let held = f.lock(0).await;
            f.insert(0, 0).await;
            f.dispatch.progress().unwrap();
            f.insert(1, 0).await; // unsubmitted work must be discarded
            let mut settlement = Box::pin(f.dispatch.settle(runtime_error()));
            assert!(settlement.as_mut().now_or_never().is_none());
            drop(held);
            let error = settlement.await;
            assert!(format!("{error:?}").contains("injected parser/DDL failure"));
            f.assert_idle();
            assert_eq!(
                f.dispatch.counts,
                RowReplayCounts {
                    inserts: 1,
                    updates: 0,
                    deletes: 0
                }
            );
            assert!(f.lock(1).await.page().is_deleted(0));
        });
    }

    /// Purpose: Collect replay completions while a source read remains pending.
    /// Expected: Completion processing preserves the same input future until its result arrives.
    #[test]
    fn test_completion_while_waiting_keeps_same_input_future() {
        smol::block_on(async {
            let mut f = Fixture::new(2, 1).await;
            let gate = Gate::new();
            f.dispatch.test_hook = Some(gate.hook.clone());
            f.insert(0, 0).await;
            f.dispatch.progress().unwrap();
            let (release, receive) = flume::bounded::<()>(1);
            let polls = Arc::new(AtomicUsize::new(0));
            let drops = Arc::new(AtomicUsize::new(0));
            let input = observed_input(&receive, &polls, &drops);
            let mut read = Box::pin(f.dispatch.read_next(input));
            assert!(read.as_mut().now_or_never().is_none());
            gate.release.send(()).unwrap();
            // Poll until completion processing retired the page, while input is
            // still parked. A test wake ensures we observe the worker's result.
            poll_fn(|cx| gate.poll_collection(read.as_mut(), cx)).await;
            assert_eq!(polls.load(Ordering::SeqCst), 1);
            assert_eq!(drops.load(Ordering::SeqCst), 0);
            release.send(()).unwrap();
            assert_eq!(read.await.unwrap(), 17);
            assert_eq!(drops.load(Ordering::SeqCst), 1);
            f.assert_idle();
        });
    }

    /// Purpose: Preserve page mutations when a later operation in the same replay batch fails.
    /// Expected: Earlier inserted data remains visible and its page stays dirty after settlement.
    #[test]
    fn test_batch_failure_marks_prior_mutations_dirty() {
        smol::block_on(async {
            let mut f = Fixture::new(1, 1).await;
            let (page, first) = f.pages[0];
            f.clear_dirty(0).await;
            f.insert(0, 0).await;
            admit_test_op(
                &mut f.dispatch,
                &f.table,
                page,
                update(page, first + 1, "never inserted"),
            )
            .await
            .unwrap();
            let error = f.dispatch.drain_all().await.unwrap_err();
            assert!(format!("{error:?}").contains("missing inserted state"));
            f.dispatch.settle(error).await;
            let guard = f.lock(0).await;
            assert!(guard.is_dirty());
            assert!(!guard.page().is_deleted(0));
            assert_eq!(
                guard.page().row(0).val(&f.table.metadata().col, 1),
                Val::from("initial")
            );
        });
    }

    /// Purpose: Settle replay after the worker pool rejects submission due to engine poison.
    /// Expected: The original fatal poison takes precedence while retaining the ordinary failure context.
    #[test]
    fn test_pool_rejection_preserves_fatal_during_settlement() {
        smol::block_on(async {
            let mut f = Fixture::new(1, 1).await;
            f.insert(0, 0).await;
            f.engine
                .inner()
                .poisoner
                .poison(Report::new(FatalError::ThreadPoolTaskPanic).attach("original poison"));
            // Submit after poison and leave the rejected completion uncollected.
            f.dispatch.pump();
            let error = f.dispatch.settle(runtime_error()).await;
            let RuntimeOrFatalError::Fatal(report) = error else {
                panic!("expected Fatal: {error:?}")
            };
            assert_eq!(*report.current_context(), FatalError::ThreadPoolTaskPanic);
            let debug = format!("{report:?}");
            assert!(debug.contains("original poison"), "{debug}");
            assert!(debug.contains("injected parser/DDL failure"), "{debug}");
            f.assert_idle();
        });
    }

    /// Purpose: Distinguish worker completion from dispatcher collection of a replay batch.
    /// Expected: Finished work retains its submission slot and page ownership until collection retires it.
    #[test]
    fn test_finished_batch_keeps_submission_slot_until_completion_collection() {
        smol::block_on(async {
            let mut f = Fixture::new(1, 1).await;
            let gate = Gate::new();
            f.dispatch.test_hook = Some(gate.hook.clone());
            f.insert(0, 0).await;
            f.dispatch.pump(); // deliberately do not register completion waiters yet
            gate.started.recv_async().await.unwrap();
            gate.release.send(()).unwrap();
            gate.finished.recv_async().await.unwrap();
            assert_eq!(f.dispatch.in_flight.len(), 1);
            assert_eq!(
                f.dispatch.active_tables[&f.table.table_id()].submitted_batches,
                1
            );
            assert!(
                f.dispatch.active_pages[&PageKey {
                    table_id: f.table.table_id(),
                    page_id: f.pages[0].0,
                }]
                    .state
                    .is_none()
            );
            assert!(gate.collected.is_empty());
            f.dispatch.drain_all().await.unwrap();
            assert_eq!(gate.collected.len(), 1);
            f.assert_idle();
        });
    }

    /// Purpose: Settle an ordinary recovery error while an accepted replay worker panics.
    /// Expected: Settlement returns the fatal worker panic and clears active dispatcher bookkeeping.
    #[test]
    fn test_worker_panic_outranks_ordinary_failure() {
        smol::block_on(async {
            let mut f = Fixture::new(1, 1).await;
            let mut gate = Gate::new();
            gate.hook.panic = true;
            f.dispatch.test_hook = Some(gate.hook.clone());
            f.insert(0, 0).await;
            f.dispatch.pump();
            gate.started.recv_async().await.unwrap();
            gate.release.send(()).unwrap();
            let error = f.dispatch.settle(runtime_error()).await;
            let RuntimeOrFatalError::Fatal(report) = error else {
                panic!("expected Fatal: {error:?}")
            };
            assert_eq!(*report.current_context(), FatalError::ThreadPoolTaskPanic);
            assert!(format!("{report:?}").contains("injected replay worker panic"));
            f.assert_idle();
        });
    }

    /// Purpose: Cancel bootstrap while an accepted replay job still owns storage resources.
    /// Expected: Rollback waits for accepted replay and releases ownership so another bootstrap succeeds.
    #[test]
    fn test_cancelled_bootstrap_drains_replay_before_storage_teardown() {
        let temp = TempDir::new().unwrap();
        let config = lightweight_test_engine_config(temp.path(), "cancelled-replay")
            .thread_pool(ThreadPoolConfig::default().worker_threads(1));
        smol::block_on(prepare_cancelled_replay(&config));
        let gate = Gate::new();
        thread::scope(|scope| {
            let cancelled = scope.spawn(|| gate.run_cancelled_bootstrap(&config));
            gate.release_after_observer_drop();
            cancelled.join().unwrap();
        });
        // Root lease and all storage/pool owners were released by rollback.
        smol::block_on(verify_replay_after_cancellation(config));
    }

    /// Purpose: Replay many underfilled pages under tight scheduling limits, then revisit them.
    /// Expected: Active bookkeeping remains bounded and retained histories allow all later updates to complete.
    #[test]
    fn test_many_partial_pages_retire_and_reactivate_with_bounded_bookkeeping() {
        smol::block_on(async {
            let mut f = Fixture::with_config(
                64,
                1,
                RecoveryConfig::default()
                    .max_in_flight_batches(Some(1))
                    .max_active_pages(Some(2)),
            )
            .await;
            for index in 0..f.pages.len() {
                f.insert(index, 0).await;
                assert!(f.dispatch.active_pages.len() <= 2);
                assert!(f.dispatch.ready.len() <= 2);
                assert!(f.dispatch.in_flight.len() <= 1);
                assert!(
                    f.dispatch.active_pages.values().all(|page| page
                        .pending
                        .as_ref()
                        .map_or(0, PackedPageBatch::len)
                        <= f.dispatch.limits.batch_ops)
                );
            }
            // Admission pressure made progress long before EOF, despite every
            // page containing fewer rows than its batch target.
            assert!(f.dispatch.counts.inserts >= 62);
            f.dispatch.drain_all().await.unwrap();
            f.assert_idle();
            for &(page, row) in &f.pages {
                admit_test_op(
                    &mut f.dispatch,
                    &f.table,
                    page,
                    update(page, row, "later transaction"),
                )
                .await
                .unwrap();
            }
            f.dispatch.drain_all().await.unwrap();
            f.assert_idle();
            assert_eq!(
                f.dispatch.counts,
                RowReplayCounts {
                    inserts: 64,
                    updates: 64,
                    deletes: 0
                }
            );
            assert_eq!(f.dispatch.page_history[&f.table.table_id()].len(), 64);
        });
    }

    /// Purpose: Admit new page work when submission of a queued batch frees pending capacity.
    /// Expected: Admission resumes before the newly submitted batch finishes and all operations eventually drain.
    #[test]
    fn test_submission_rechecks_newly_available_pending_capacity_before_wait() {
        smol::block_on(async {
            let mut f = Fixture::with_config(
                1,
                1,
                RecoveryConfig::default()
                    .max_in_flight_batches(Some(1))
                    .max_batch_ops(2),
            )
            .await;
            let gate = Gate::new();
            f.dispatch.test_hook = Some(gate.hook.clone());
            for slot in 0..4 {
                f.insert(0, slot).await;
            }
            gate.started.recv_async().await.unwrap();
            let (page, first) = f.pages[0];
            let mut next = Box::pin(admit_test_op(
                &mut f.dispatch,
                &f.table,
                page,
                insert(page, first + 4, "next batch"),
            ));
            assert!(next.as_mut().now_or_never().is_none());
            gate.release.send(()).unwrap();
            // Collection submits the full successor. That frees the pending
            // slot immediately, even while the successor is held at its gate.
            next.await.unwrap();
            gate.started.recv_async().await.unwrap();
            assert_eq!(f.dispatch.in_flight.len(), 1);
            assert_eq!(
                f.dispatch.active_pages[&PageKey {
                    table_id: f.table.table_id(),
                    page_id: page,
                }]
                    .pending
                    .as_ref()
                    .map_or(0, PackedPageBatch::len),
                1
            );
            gate.release.send(()).unwrap();
            gate.release.send(()).unwrap();
            f.dispatch.drain_all().await.unwrap();
            f.assert_idle();
            assert_eq!(
                f.dispatch.counts,
                RowReplayCounts {
                    inserts: 5,
                    updates: 0,
                    deletes: 0
                }
            );
        });
    }

    /// Purpose: Split a partial replay batch when the next row would exceed its byte target.
    /// Expected: The existing batch is submitted and the next row remains pending until both are replayed.
    #[test]
    fn byte_target_flushes_partial_batch_before_next_row() {
        smol::block_on(async {
            let mut f = Fixture::with_config(
                1,
                1,
                RecoveryConfig::default().target_batch_bytes(insert_bytes("initial") * 2 - 1),
            )
            .await;
            let gate = Gate::new();
            f.dispatch.test_hook = Some(gate.hook.clone());
            f.insert(0, 0).await;
            assert_eq!(f.dispatch.in_flight.len(), 0);
            f.insert(0, 1).await;
            gate.started.recv_async().await.unwrap();
            let page = &f.dispatch.active_pages[&PageKey {
                table_id: f.table.table_id(),
                page_id: f.pages[0].0,
            }];
            assert_eq!(page.pending.as_ref().unwrap().len(), 1);
            assert_eq!(f.dispatch.in_flight.len(), 1);
            gate.release.send(()).unwrap();
            gate.release.send(()).unwrap();
            f.dispatch.drain_all().await.unwrap();
            assert_eq!(f.dispatch.counts.inserts, 2);
            assert_eq!(pool_snapshot(&f.dispatch.recycled), (0, 0));
        });
    }

    /// Purpose: Admit individual operations that meet or exceed the batch byte target.
    /// Expected: Each operation is submitted immediately and completes without waiting for byte credits.
    #[test]
    fn exact_fit_and_oversized_operations_dispatch_without_byte_credit_waits() {
        smol::block_on(async {
            for target in [insert_bytes("initial"), insert_bytes("initial") - 1] {
                let mut f = Fixture::with_config(
                    1,
                    1,
                    RecoveryConfig::default().target_batch_bytes(target),
                )
                .await;
                let gate = Gate::new();
                f.dispatch.test_hook = Some(gate.hook.clone());
                f.insert(0, 0).await;
                gate.started.recv_async().await.unwrap();
                assert_eq!(f.dispatch.in_flight.len(), 1);
                assert!(
                    f.dispatch
                        .active_pages
                        .values()
                        .all(|page| page.pending.is_none())
                );
                gate.release.send(()).unwrap();
                f.dispatch.drain_all().await.unwrap();
                assert_eq!(f.dispatch.counts.inserts, 1);
            }
        });
    }

    /// Purpose: Reuse collected batch storage across pages after source groups have been released.
    /// Expected: Recycling begins only after collection, preserves both pages' values, and releases storage at final drain.
    #[test]
    fn collected_storage_recycles_across_pages_without_borrowed_group_or_page_bytes() {
        smol::block_on(async {
            let mut f = Fixture::new(2, 1).await;
            let gate = Gate::new();
            f.dispatch.test_hook = Some(gate.hook.clone());
            let (page, first) = f.pages[0];
            let initial = "a".repeat(512);
            // This helper releases its source group before the accepted job runs.
            admit_test_op(
                &mut f.dispatch,
                &f.table,
                page,
                insert(page, first, &initial),
            )
            .await
            .unwrap();
            f.dispatch.pump();
            gate.started.recv_async().await.unwrap();
            assert_eq!(pool_snapshot(&f.dispatch.recycled), (0, 0));
            gate.release.send(()).unwrap();
            gate.finished.recv_async().await.unwrap();
            assert_eq!(pool_snapshot(&f.dispatch.recycled), (0, 0));
            f.dispatch.drain(f.table.table_id()).await.unwrap();
            let retained = pool_snapshot(&f.dispatch.recycled);
            assert_eq!(retained.0, 1);
            assert!(retained.1 > 512);
            f.dispatch.test_hook = None;
            let (page, first) = f.pages[1];
            admit_test_op(
                &mut f.dispatch,
                &f.table,
                page,
                insert(page, first, &"b".repeat(512)),
            )
            .await
            .unwrap();
            assert_eq!(pool_snapshot(&f.dispatch.recycled), (0, 0));
            f.dispatch.drain(f.table.table_id()).await.unwrap();
            assert_eq!(pool_snapshot(&f.dispatch.recycled), retained);
            assert_eq!(
                f.lock(0)
                    .await
                    .page()
                    .row(0)
                    .val(&f.table.metadata().col, 1),
                Val::from(initial.as_str())
            );
            assert_eq!(
                f.lock(1)
                    .await
                    .page()
                    .row(0)
                    .val(&f.table.metadata().col, 1),
                Val::from("b".repeat(512).as_str())
            );
            f.dispatch.drain_all().await.unwrap();
            assert_eq!(pool_snapshot(&f.dispatch.recycled), (0, 0));
        });
    }
}
