use crate::bitmap::AllocMap;
use crate::buffer::arena::{ArenaGuard, QuiescentArena};
use crate::buffer::evictor::{
    ClockHand, EvictionArbiter, EvictionRuntime, Evictor, FailureRateTracker,
    PressureDeltaClockPolicy, clock_collect_batch, clock_sweep_candidate,
};
use crate::buffer::frame::{BufferFrame, FrameKind};
use crate::buffer::guard::{FacadePageGuard, PageExclusiveGuard, PageGuard, PageLatchGuard};
use crate::buffer::load::{PageReservation, PageReservationGuard};
use crate::buffer::page::{BufferPage, IOKind, PAGE_SIZE, Page, PageID, PageIO, VersionedPageID};
use crate::buffer::util::{frame_total_bytes, madvise_dontneed};
use crate::buffer::{
    BufferPool, BufferPoolStats, BufferPoolStatsHandle, PageIOCompletion, PoolGuard, PoolIdentity,
    PoolRole, RowPoolRole,
};
use crate::component::{Component, ComponentRegistry, ShelfScope, Supplier};
use crate::conf::EvictableBufferPoolConfig;
use crate::conf::path::{path_to_utf8, validate_swap_file_path_candidate};
use crate::error::Validation::Valid;
use crate::error::{Error, Result, Validation};
use crate::file::SparseFile;
use crate::file::fs::{FileSystem, FileSystemWorkers};
use crate::io::{AIOKind, IOBackendStats, IOQueue, IOStateMachine, IOSubmission, Operation};
use crate::latch::{GuardState, LatchFallbackMode};
use crate::notify::EventNotifyOnDrop;
use crate::quiescent::{QuiescentBox, QuiescentGuard, SyncQuiescentGuard};
use crate::{IndexPool, MemPool};
use event_listener::{Event, EventListener, listener};
use parking_lot::Mutex;
use std::collections::hash_map::Entry;
use std::collections::{BTreeSet, HashMap};
use std::mem;
use std::os::fd::{AsRawFd, RawFd};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread::JoinHandle;
// min buffer pool size is 64KB * 128 = 8MB.
const MIN_IN_MEM_PAGES: usize = 128;

/// EvictableBufferPool is a buffer pool which can evict
/// pages to disk.
///
/// The pool owns page/frame state and its eviction thread, while all actual file
/// IO is routed through the shared storage worker exposed by [`FileSystem`].
pub struct EvictableBufferPool {
    // Takes care of page allocation and deallocation.
    alloc_map: AllocMap,
    // Event to notify allocating new page is available.
    alloc_ev: Event,
    // Shared filesystem guard used to route IO through the storage worker.
    fs: QuiescentGuard<FileSystem>,
    // shutdown flag.
    shutdown_flag: Arc<AtomicBool>,
    // In-memory page set.
    in_mem: Arc<InMemPageSet>,
    // Borrowed file descriptor used to describe worker-owned read completions.
    raw_fd: RawFd,
    // Inflight IO map.
    inflight_io: Arc<InflightIO>,
    // Pool-owned access and IO lifecycle counters.
    stats: BufferPoolStatsHandle,
    role: PoolRole,
    arena: QuiescentArena,
}

/// Lifecycle component that owns the mem-pool evictor thread.
pub(crate) struct MemPoolWorkers;

/// Registered shutdown state for the mem-pool evictor thread.
pub(crate) struct MemPoolWorkersOwned {
    pool: SyncQuiescentGuard<EvictableBufferPool>,
    evict_thread: Mutex<Option<JoinHandle<()>>>,
}

/// Lifecycle component that owns the index-pool evictor thread.
pub(crate) struct IndexPoolWorkers;

/// Registered shutdown state for the index-pool evictor thread.
pub(crate) struct IndexPoolWorkersOwned {
    pool: SyncQuiescentGuard<EvictableBufferPool>,
    evict_thread: Mutex<Option<JoinHandle<()>>>,
}

/// Shelf marker proving that a pool still needs its evictor thread started.
pub(crate) struct PendingEvictorThread;

impl EvictableBufferPool {
    #[inline]
    pub(crate) fn identity(&self) -> PoolIdentity {
        self.arena.identity()
    }

    #[inline]
    pub(crate) fn row_pool_role(&self) -> RowPoolRole {
        self.role.row_pool_role()
    }

    #[inline]
    fn try_lock_page_exclusive(
        &self,
        guard: &PoolGuard,
        page_id: PageID,
    ) -> Option<PageExclusiveGuard<Page>> {
        self.arena.try_lock_page_exclusive(guard, page_id)
    }

    /// Returns one snapshot of evictable-pool access and IO lifecycle counters.
    #[inline]
    pub fn stats(&self) -> BufferPoolStats {
        self.stats.snapshot()
    }

    /// Returns one snapshot of backend-owned submit/wait activity for this pool.
    #[inline]
    pub fn io_backend_stats(&self) -> IOBackendStats {
        self.fs.io_backend_stats()
    }

    #[inline]
    async fn try_wait_for_io_write(&self, page_id: PageID) -> Result<()> {
        self.inflight_io
            .wait_for_write(page_id, self.arena.frame(page_id))
            .await
    }

    /// Try to dispatch read IO on given page.
    /// This method may not succeed, and client should retry.
    #[inline]
    async fn try_dispatch_io_read(&self, guard: &PoolGuard, page_id: PageID) -> Result<()> {
        guard.assert_matches(self.identity(), "evictable buffer pool");
        self.stats.record_cache_miss();
        enum DispatchAction {
            RetryYield,
            Wait(Arc<PageIOCompletion>),
            WaitForLoad(EventListener),
            SendRead {
                req: EvictReadSubmission,
                completion: Arc<PageIOCompletion>,
            },
        }

        // Prepare state under lock, then perform async operations after lock release.
        let action = {
            let mut g = self.inflight_io.map.lock();
            match g.entry(page_id) {
                Entry::Vacant(vac) => {
                    // First thread to initialize IO read.
                    match self.try_lock_page_exclusive(guard, page_id) {
                        // Retry path. Yield after lock release to avoid tight spin.
                        None => DispatchAction::RetryYield,
                        Some(page_guard) => {
                            // Reserve one in-memory slot for this load.
                            if !self.in_mem.try_inc() {
                                let listener = self.in_mem.load_ev.listen();
                                if !self.in_mem.try_inc() {
                                    self.in_mem.record_alloc_failure();
                                    // Still no budget. Ask evictor to work and wait for progress.
                                    self.in_mem.evict_ev.notify(1);
                                    drop(page_guard);
                                    DispatchAction::WaitForLoad(listener)
                                } else {
                                    self.in_mem.record_alloc_success();
                                    let completion = Arc::new(PageIOCompletion::new());
                                    vac.insert(IOStatus {
                                        kind: IOKind::Read,
                                        completion: Some(Arc::clone(&completion)),
                                    });
                                    self.inflight_io.reads.fetch_add(1, Ordering::AcqRel);
                                    let reservation =
                                        PageReservationGuard::new(EvictPageReservation::new(
                                            page_guard,
                                            Arc::clone(&self.in_mem),
                                        ));
                                    let req = EvictReadSubmission::new(
                                        page_id,
                                        self.raw_fd,
                                        Arc::clone(&self.inflight_io),
                                        self.stats.clone(),
                                        reservation,
                                    );
                                    DispatchAction::SendRead { req, completion }
                                }
                            } else {
                                self.in_mem.record_alloc_success();
                                let completion = Arc::new(PageIOCompletion::new());
                                vac.insert(IOStatus {
                                    kind: IOKind::Read,
                                    completion: Some(Arc::clone(&completion)),
                                });
                                self.inflight_io.reads.fetch_add(1, Ordering::AcqRel);
                                let reservation = PageReservationGuard::new(
                                    EvictPageReservation::new(page_guard, Arc::clone(&self.in_mem)),
                                );
                                let req = EvictReadSubmission::new(
                                    page_id,
                                    self.raw_fd,
                                    Arc::clone(&self.inflight_io),
                                    self.stats.clone(),
                                    reservation,
                                );
                                DispatchAction::SendRead { req, completion }
                            }
                        }
                    }
                }
                Entry::Occupied(mut occ) => {
                    self.stats.record_miss_join();
                    let status = occ.get_mut();
                    let completion = status
                        .completion
                        .get_or_insert_with(|| Arc::new(PageIOCompletion::new()))
                        .clone();
                    DispatchAction::Wait(completion)
                }
            }
        };

        match action {
            DispatchAction::RetryYield => {
                smol::future::yield_now().await;
            }
            DispatchAction::Wait(completion) => {
                completion.wait_result().await?;
            }
            DispatchAction::WaitForLoad(listener) => {
                listener.await;
                self.in_mem.load_ev.notify(1);
            }
            DispatchAction::SendRead { req, completion } => {
                if let Err(send_err) = self.fs.send_pool_read_async(self.role, req).await {
                    let failed_req = send_err.0;
                    failed_req.fail(Error::SendError);
                    self.in_mem.load_ev.notify(1);
                    return Err(Error::SendError);
                }
                completion.wait_result().await?;
            }
        }
        Ok(())
    }

    /// Start the dedicated eviction thread for one registered pool.
    #[inline]
    fn spawn_evict_thread_guarded(pool: SyncQuiescentGuard<Self>) -> JoinHandle<()> {
        let runtime = EvictableRuntime {
            arena: pool.arena.arena_guard(pool.pool_guard()),
            in_mem: Arc::clone(&pool.in_mem),
            fs: pool.fs.clone(),
            inflight_io: Arc::clone(&pool.inflight_io),
            role: pool.role,
            stats: pool.stats.clone(),
            _pool: Some(pool.clone()),
        };
        let policy =
            PressureDeltaClockPolicy::new(pool.in_mem.eviction_arbiter, MIN_IN_MEM_PAGES / 2);
        Evictor::new(runtime, policy, Arc::clone(&pool.shutdown_flag))
            .start_thread("EvictableBufferPoolEvictor")
    }

    /// Signal the pool-local eviction thread to stop.
    #[inline]
    fn signal_shutdown(&self) {
        self.shutdown_flag.store(true, Ordering::SeqCst);
        self.in_mem.evict_ev.notify(1);
    }

    /// Reserve a page in memory.
    /// If this function returns true, in-mem page counter is incremented.
    /// Caller should handle the failure of add a page into memory and decrease this number.
    #[inline]
    async fn reserve_page(&self) {
        if self.in_mem.try_inc() {
            self.in_mem.record_alloc_success();
            return;
        }
        // Slow path. Wait for other to release memory.
        loop {
            listener!(self.in_mem.load_ev => listener);
            if self.in_mem.try_inc() {
                self.in_mem.record_alloc_success();
                self.in_mem.load_ev.notify(1);
                return;
            }
            self.in_mem.record_alloc_failure();
            // Notify evictor thread to work.
            self.in_mem.evict_ev.notify(1);
            listener.await;
        }
    }
}

impl BufferPool for EvictableBufferPool {
    #[inline]
    fn capacity(&self) -> usize {
        self.alloc_map.len()
    }

    #[inline]
    fn allocated(&self) -> usize {
        self.alloc_map.allocated()
    }

    #[inline]
    fn pool_guard(&self) -> PoolGuard {
        self.arena.guard()
    }

    #[inline]
    async fn allocate_page<T: BufferPage>(&self, guard: &PoolGuard) -> PageExclusiveGuard<T> {
        loop {
            self.reserve_page().await;

            // Now we have memory budget to allocate new page.
            match self.alloc_map.try_allocate() {
                Some(page_id) => {
                    let page_id = PageID::from(page_id);
                    self.in_mem.pin(page_id);
                    return self.arena.init_page(guard, page_id);
                }
                None => {
                    listener!(self.alloc_ev => listener);
                    // re-check
                    if let Some(page_id) = self.alloc_map.try_allocate() {
                        let page_id = PageID::from(page_id);
                        self.in_mem.pin(page_id);
                        return self.arena.init_page(guard, page_id);
                    }

                    // Here we cannot find a free page to load, we should cancel reservation of a page
                    // and retry.
                    self.in_mem.dec();
                    listener.await;
                }
            }
        }
    }

    #[inline]
    async fn allocate_page_at<T: BufferPage>(
        &self,
        guard: &PoolGuard,
        page_id: PageID,
    ) -> Result<PageExclusiveGuard<T>> {
        self.reserve_page().await;

        if self.alloc_map.allocate_at(usize::from(page_id)) {
            self.in_mem.pin(page_id);
            Ok(self.arena.init_page(guard, page_id))
        } else {
            self.in_mem.dec();
            Err(Error::BufferPageAlreadyAllocated)
        }
    }

    #[inline]
    async fn get_page<T: BufferPage>(
        &self,
        guard: &PoolGuard,
        page_id: PageID,
        mode: LatchFallbackMode,
    ) -> Result<FacadePageGuard<T>> {
        guard.assert_matches(self.identity(), "evictable buffer pool");
        loop {
            let bf = self.arena.frame_ptr(page_id);
            let frame = self.arena.frame(page_id);
            match frame.kind() {
                FrameKind::Uninitialized => {
                    panic!("get an uninitialized page");
                }
                FrameKind::Fixed | FrameKind::Hot => {
                    let g = frame.latch.optimistic_fallback_raw(mode).await;
                    self.stats.record_cache_hit();
                    return Ok(FacadePageGuard::new(
                        PageLatchGuard::new(guard.clone(), g),
                        bf,
                    ));
                }
                FrameKind::EvictionFailed => return Err(Error::IOError),
                FrameKind::Cool => {
                    // Try to mark this page as HOT.
                    if frame.compare_exchange_kind(FrameKind::Cool, FrameKind::Hot)
                        != FrameKind::Cool
                    {
                        // This page is going to be evicted. we have to retry and probably wait.
                        continue;
                    }
                    let g = frame.latch.optimistic_fallback_raw(mode).await;
                    self.stats.record_cache_hit();
                    return Ok(FacadePageGuard::new(
                        PageLatchGuard::new(guard.clone(), g),
                        bf,
                    ));
                }
                FrameKind::Evicting => {
                    // The page is marked evicting in order to be evicted to disk in near future.
                    // Here we do not break the write operation,
                    // Instead, we wait for it to complete.
                    // And then, reload it in memory.
                    self.try_wait_for_io_write(page_id).await?;
                }
                FrameKind::Evicted => {
                    // The page is already on disk.
                    // This means we should let one thread to exclusively lock this page
                    // and initiate a IO read.
                    // Other threads can wait for the initiator to finish.
                    self.try_dispatch_io_read(guard, page_id).await?;
                }
            }
        }
    }

    #[inline]
    async fn get_page_versioned<T: BufferPage>(
        &self,
        guard: &PoolGuard,
        id: VersionedPageID,
        mode: LatchFallbackMode,
    ) -> Result<Option<FacadePageGuard<T>>> {
        guard.assert_matches(self.identity(), "evictable buffer pool");
        loop {
            let bf = self.arena.frame_ptr(id.page_id);
            let frame = self.arena.frame(id.page_id);
            if frame.generation() != id.generation {
                return Ok(None);
            }
            match frame.kind() {
                FrameKind::Uninitialized => return Ok(None),
                FrameKind::Fixed | FrameKind::Hot => {
                    let g = frame.latch.optimistic_fallback_raw(mode).await;
                    let g = FacadePageGuard::new(PageLatchGuard::new(guard.clone(), g), bf);
                    let bf = g.bf();
                    if bf.kind() == FrameKind::Uninitialized || bf.generation() != id.generation {
                        if g.is_exclusive() {
                            g.rollback_exclusive_version_change();
                        }
                        return Ok(None);
                    }
                    self.stats.record_cache_hit();
                    return Ok(Some(g));
                }
                FrameKind::EvictionFailed => return Err(Error::IOError),
                FrameKind::Cool => {
                    let g = frame.latch.optimistic_fallback_raw(mode).await;
                    if frame.compare_exchange_kind(FrameKind::Cool, FrameKind::Hot)
                        != FrameKind::Cool
                    {
                        continue;
                    }
                    let g = FacadePageGuard::new(PageLatchGuard::new(guard.clone(), g), bf);
                    let bf = g.bf();
                    if bf.kind() == FrameKind::Uninitialized || bf.generation() != id.generation {
                        if g.is_exclusive() {
                            g.rollback_exclusive_version_change();
                        }
                        return Ok(None);
                    }
                    self.stats.record_cache_hit();
                    return Ok(Some(g));
                }
                FrameKind::Evicting => {
                    self.try_wait_for_io_write(id.page_id).await?;
                }
                FrameKind::Evicted => {
                    self.try_dispatch_io_read(guard, id.page_id).await?;
                }
            }
        }
    }

    #[inline]
    fn deallocate_page<T: BufferPage>(&self, mut g: PageExclusiveGuard<T>) {
        let page_id = g.page_id();
        g.page_mut().zero(); // zero the page
        g.bf_mut().ctx = None;
        T::deinit_frame(g.bf_mut());
        g.bf_mut().bump_generation();
        self.in_mem.unpin(page_id);
        self.in_mem.mark_page_dontneed(g);
        self.alloc_map.deallocate(usize::from(page_id));
        self.alloc_ev.notify(usize::MAX);
    }

    #[inline]
    async fn get_child_page<T: BufferPage>(
        &self,
        guard: &PoolGuard,
        p_guard: &FacadePageGuard<T>,
        page_id: PageID,
        mode: LatchFallbackMode,
    ) -> Result<Validation<FacadePageGuard<T>>> {
        guard.assert_matches(self.identity(), "evictable buffer pool");
        loop {
            let bf = self.arena.frame_ptr(page_id);
            let frame = self.arena.frame(page_id);
            match frame.kind() {
                FrameKind::Uninitialized => {
                    panic!("get an uninitialized page");
                }
                FrameKind::Fixed | FrameKind::Hot => {
                    let g = frame.latch.optimistic_fallback_raw(mode).await;
                    // apply lock coupling.
                    // the validation make sure parent page does not change until child
                    // page is acquired.
                    if p_guard.validate_bool() {
                        self.stats.record_cache_hit();
                        return Ok(Valid(FacadePageGuard::new(
                            PageLatchGuard::new(guard.clone(), g),
                            bf,
                        )));
                    }
                    if g.state() == GuardState::Exclusive {
                        g.rollback_exclusive_bit();
                    }
                    return Ok(Validation::Invalid);
                }
                FrameKind::EvictionFailed => return Err(Error::IOError),
                FrameKind::Cool => {
                    let g = frame.latch.optimistic_fallback_raw(mode).await;
                    // Try to mark this page as HOT.
                    if frame.compare_exchange_kind(FrameKind::Cool, FrameKind::Hot)
                        != FrameKind::Cool
                    {
                        // This page is going to be evicted.
                        continue;
                    }
                    // apply lock coupling.
                    // the validation make sure parent page does not change until child
                    // page is acquired.
                    let validated = p_guard.validate().and_then(|_| {
                        Valid(FacadePageGuard::new(
                            PageLatchGuard::new(guard.clone(), g),
                            bf,
                        ))
                    });
                    if matches!(validated, Validation::Valid(_)) {
                        self.stats.record_cache_hit();
                    }
                    return Ok(validated);
                }
                FrameKind::Evicting => {
                    // The page is being evicted to disk.
                    self.try_wait_for_io_write(page_id).await?;
                }
                FrameKind::Evicted => {
                    // The page is already on disk.
                    // This means we should let one thread to exclusively lock this page
                    // and initiate a IO read.
                    // Other threads can wait for the initiator to finish.
                    self.try_dispatch_io_read(guard, page_id).await?;
                }
            }
        }
    }
}

// SAFETY: the pool only exposes concurrent access through thread-safe
// primitives (atomics, events, mutexes, quiescent guards, and page latches).
unsafe impl Send for EvictableBufferPool {}

// SAFETY: shared references coordinate all mutable state through the same
// thread-safe primitives and stable arena-owned frame/page memory.
unsafe impl Sync for EvictableBufferPool {}

impl Component for MemPoolWorkers {
    type Config = ();
    type Owned = MemPoolWorkersOwned;
    type Access = ();

    const NAME: &'static str = "mem_pool_workers";

    /// Start the mem-pool evictor after the pool component has registered.
    #[inline]
    async fn build(
        _config: Self::Config,
        registry: &mut ComponentRegistry,
        mut shelf: ShelfScope<'_, Self>,
    ) -> Result<()> {
        let pool = registry.dependency::<MemPool>()?;
        let _pending = shelf.take::<MemPool>().ok_or(Error::InvalidState)?;
        let sync_pool = pool.clone_inner().into_sync();
        let evict_thread = EvictableBufferPool::spawn_evict_thread_guarded(sync_pool.clone());
        registry.register::<Self>(MemPoolWorkersOwned {
            pool: sync_pool,
            evict_thread: Mutex::new(Some(evict_thread)),
        })
    }

    #[inline]
    fn access(_owner: &QuiescentBox<Self::Owned>) -> Self::Access {}

    /// Stop the mem-pool evictor thread after signaling shutdown.
    #[inline]
    fn shutdown(component: &Self::Owned) {
        component.pool.signal_shutdown();
        if let Some(handle) = component.evict_thread.lock().take() {
            handle.join().unwrap();
        }
    }
}

impl Supplier<MemPoolWorkers> for MemPool {
    type Provision = PendingEvictorThread;
}

impl Supplier<FileSystemWorkers> for MemPool {
    type Provision = PoolStorageProvision;
}

impl Component for IndexPoolWorkers {
    type Config = ();
    type Owned = IndexPoolWorkersOwned;
    type Access = ();

    const NAME: &'static str = "index_pool_workers";

    /// Start the index-pool evictor after the pool component has registered.
    #[inline]
    async fn build(
        _config: Self::Config,
        registry: &mut ComponentRegistry,
        mut shelf: ShelfScope<'_, Self>,
    ) -> Result<()> {
        let pool = registry.dependency::<IndexPool>()?;
        let _pending = shelf.take::<IndexPool>().ok_or(Error::InvalidState)?;
        let sync_pool = pool.clone_inner().into_sync();
        let evict_thread = EvictableBufferPool::spawn_evict_thread_guarded(sync_pool.clone());
        registry.register::<Self>(IndexPoolWorkersOwned {
            pool: sync_pool,
            evict_thread: Mutex::new(Some(evict_thread)),
        })
    }

    #[inline]
    fn access(_owner: &QuiescentBox<Self::Owned>) -> Self::Access {}

    /// Stop the index-pool evictor thread after signaling shutdown.
    #[inline]
    fn shutdown(component: &Self::Owned) {
        component.pool.signal_shutdown();
        if let Some(handle) = component.evict_thread.lock().take() {
            handle.join().unwrap();
        }
    }
}

impl Supplier<IndexPoolWorkers> for IndexPool {
    type Provision = PendingEvictorThread;
}

impl Supplier<FileSystemWorkers> for IndexPool {
    type Provision = PoolStorageProvision;
}

/// IO worker state machine for [`EvictableBufferPool`].
///
/// It translates high-level pool requests into concrete page IO submissions and
/// reconciles frame/inflight state on completion.
pub(crate) struct EvictablePoolStateMachine {
    pool: SyncQuiescentGuard<EvictableBufferPool>,
    file_io: SingleFileIO,
}

impl EvictablePoolStateMachine {
    /// Bind one pool runtime plus its worker-owned sparse-file handle.
    #[inline]
    pub(crate) fn new(
        pool: SyncQuiescentGuard<EvictableBufferPool>,
        file_io: SingleFileIO,
    ) -> Self {
        Self { pool, file_io }
    }
}

/// Backend-facing pool submission emitted by `EvictablePoolStateMachine`.
pub(crate) enum EvictSubmission {
    Read(EvictReadSubmission),
    Write(PageIO),
}

impl IOSubmission for EvictSubmission {
    type Key = PageID;

    #[inline]
    fn key(&self) -> &Self::Key {
        match self {
            EvictSubmission::Read(sub) => sub.key(),
            EvictSubmission::Write(sub) => sub.key(),
        }
    }

    #[inline]
    fn operation(&mut self) -> &mut Operation {
        match self {
            EvictSubmission::Read(sub) => sub.operation(),
            EvictSubmission::Write(sub) => sub.operation(),
        }
    }
}

impl IOStateMachine for EvictablePoolStateMachine {
    type Request = Box<PoolRequest>;
    type Key = PageID;
    type Submission = EvictSubmission;

    /// Expand one pool request into backend-facing read or write submissions.
    #[inline]
    fn prepare_request(
        &mut self,
        req: Box<PoolRequest>,
        max_new: usize,
        queue: &mut IOQueue<EvictSubmission>,
    ) -> Option<Box<PoolRequest>> {
        if max_new == 0 {
            return Some(req);
        }
        match *req {
            PoolRequest::Read(req) => {
                let page_id = *req.key();
                debug_assert!(self.pool.inflight_io.contains(page_id));
                queue.push(EvictSubmission::Read(req));
                None
            }
            PoolRequest::BatchWrite(mut page_guards, done_ev) => {
                let remainder = if page_guards.len() > max_new {
                    Some(page_guards.split_off(max_new))
                } else {
                    None
                };
                for mut page_guard in page_guards {
                    let page_id = page_guard.page_id();
                    debug_assert!(self.pool.inflight_io.contains(page_id));
                    debug_assert!(page_guard.bf().kind() == FrameKind::Evicting);
                    let operation = self.file_io.prepare_write(page_id, page_guard.page_mut());
                    let req = PageIO {
                        key: page_id,
                        operation,
                        page_guard,
                        batch_done: Some(Arc::clone(&done_ev)),
                    };
                    queue.push(EvictSubmission::Write(req));
                }
                remainder.map(|page_guards| Box::new(PoolRequest::BatchWrite(page_guards, done_ev)))
            }
        }
    }

    /// Record submit-side pool bookkeeping once the backend accepts a submission.
    #[inline]
    fn on_submit(&mut self, sub: &EvictSubmission) {
        match sub {
            EvictSubmission::Read(sub) => {
                debug_assert!(self.pool.inflight_io.contains(*sub.key()));
                sub.record_running();
            }
            EvictSubmission::Write(sub) => {
                debug_assert!(self.pool.inflight_io.contains(sub.page_id()));
                self.pool.stats.add_running_writes(1);
            }
        }
    }

    /// Reconcile inflight/page state after one backend completion.
    #[inline]
    fn on_complete(&mut self, sub: EvictSubmission, res: std::io::Result<usize>) -> AIOKind {
        match sub {
            EvictSubmission::Read(sub) => sub.complete(res),
            EvictSubmission::Write(sub) => {
                let page_id = sub.page_id();
                let mut g = self.pool.inflight_io.map.lock();
                let mut status = g.remove(&page_id).unwrap();
                match status.kind {
                    IOKind::Write => {
                        let completion = status.completion.take();
                        self.pool.inflight_io.writes.fetch_sub(1, Ordering::Relaxed);
                        let mut page_guard = sub.page_guard;
                        let bf = page_guard.bf();
                        debug_assert!(bf.kind() == FrameKind::Evicting);
                        // Writeback either finishes eviction or leaves the
                        // frame marked for retry on the next access.
                        let result = match res {
                            Ok(len) if len == PAGE_SIZE => {
                                self.pool.in_mem.evict_page(page_guard);
                                Ok(page_id)
                            }
                            Ok(_) | Err(_) => {
                                page_guard.bf_mut().set_kind(FrameKind::EvictionFailed);
                                drop(page_guard);
                                Err(Error::IOError)
                            }
                        };
                        self.pool.stats.add_completed_writes(1);
                        if result.is_err() {
                            self.pool.stats.add_write_errors(1);
                        }
                        drop(g);
                        if let Some(completion) = completion {
                            completion.complete(result);
                        }
                        drop(sub.batch_done);
                        AIOKind::Write
                    }
                    IOKind::ReadWaitForWrite => {
                        // Write request will overwrite it to IOKind::Write.
                        unreachable!()
                    }
                    IOKind::Read => unreachable!(),
                }
            }
        }
    }

    #[inline]
    fn end_loop(self) {
        // do nothing
    }
}

/// Evictor-facing runtime that routes selected writeback through `FileSystem`.
struct EvictableRuntime {
    arena: ArenaGuard,
    in_mem: Arc<InMemPageSet>,
    inflight_io: Arc<InflightIO>,
    fs: QuiescentGuard<FileSystem>,
    role: PoolRole,
    stats: BufferPoolStatsHandle,
    _pool: Option<SyncQuiescentGuard<EvictableBufferPool>>,
}

impl EvictableRuntime {
    /// Send one eviction writeback batch to the shared storage worker.
    #[inline]
    fn dispatch_io_writes(&self, page_guards: Vec<PageExclusiveGuard<Page>>) -> EventListener {
        self.stats.add_queued_writes(page_guards.len());
        self.inflight_io.batch_writes(&page_guards);
        let done_ev = Arc::new(EventNotifyOnDrop::new());
        let listener = done_ev.listen();
        let _ = self
            .fs
            .send_pool_batch_write(self.role, page_guards, done_ev);
        listener
    }
}

impl EvictionRuntime for EvictableRuntime {
    #[inline]
    fn resident_count(&self) -> usize {
        self.in_mem.count.load(Ordering::Acquire)
    }

    #[inline]
    fn capacity(&self) -> usize {
        self.in_mem.max_count
    }

    #[inline]
    fn inflight_evicts(&self) -> usize {
        self.inflight_io.writes.load(Ordering::Relaxed)
    }

    #[inline]
    fn allocation_failure_rate(&self) -> f64 {
        self.in_mem.alloc_failure_rate()
    }

    #[inline]
    fn work_listener(&self) -> EventListener {
        self.in_mem.evict_ev.listen()
    }

    #[inline]
    fn notify_progress(&self) {
        let in_mem_count = self.in_mem.count.load(Ordering::Acquire);
        if in_mem_count < self.in_mem.max_count {
            self.in_mem.load_ev.notify(1);
        }
    }

    #[inline]
    fn collect_batch_ids(
        &self,
        hand: ClockHand,
        limit: usize,
        out: &mut Vec<PageID>,
    ) -> Option<ClockHand> {
        self.in_mem.collect_batch_ids(hand, limit, out)
    }

    #[inline]
    fn try_mark_evicting(&self, page_id: PageID) -> Option<PageExclusiveGuard<Page>> {
        clock_sweep_candidate(&self.arena, page_id)
    }

    #[inline]
    fn execute(&self, pages: Vec<PageExclusiveGuard<Page>>) -> Option<EventListener> {
        let mut dirty_pages = vec![];
        for page_guard in pages {
            if page_guard.is_dirty() {
                dirty_pages.push(page_guard);
            } else {
                // Page is a read-only copy of disk content.
                // Keep inflight map ordering consistent with write path.
                let page_id = page_guard.page_id();
                let waiter_completion = {
                    let mut g = self.inflight_io.map.lock();
                    let waiter_completion = if matches!(
                        g.get(&page_id).map(|status| status.kind),
                        Some(IOKind::ReadWaitForWrite)
                    ) {
                        let mut status = g.remove(&page_id).unwrap();
                        status.completion.take()
                    } else {
                        if let Some(status) = g.get(&page_id) {
                            debug_assert!(status.kind != IOKind::Write);
                        }
                        None
                    };
                    debug_assert!(page_guard.bf().kind() == FrameKind::Evicting);
                    self.in_mem.evict_page(page_guard);
                    waiter_completion
                };
                if let Some(completion) = waiter_completion {
                    completion.complete(Ok(page_id));
                }
            }
        }

        if dirty_pages.is_empty() {
            return None;
        }

        Some(self.dispatch_io_writes(dirty_pages))
    }
}

struct InMemPageSet {
    // Current page number held in memory.
    count: AtomicUsize,
    // Maximum page number held in memory.
    max_count: usize,
    // Shared pressure-delta eviction arbiter.
    eviction_arbiter: EvictionArbiter,
    // Sliding-window allocation failure tracker.
    alloc_failures: FailureRateTracker,
    // Ordered page id set, used for evict thread.
    set: Mutex<BTreeSet<PageID>>,
    // Event to notify thread that loading a page is available.
    load_ev: Event,
    // Event to notify evictor thread to choose page candidates,
    // write to disk and release memory.
    evict_ev: Event,
}

impl InMemPageSet {
    #[inline]
    fn new(max_count: usize, eviction_arbiter: EvictionArbiter) -> Self {
        InMemPageSet {
            count: AtomicUsize::new(0),
            max_count,
            alloc_failures: FailureRateTracker::new(eviction_arbiter.failure_window()),
            eviction_arbiter,
            set: Mutex::new(BTreeSet::new()),
            load_ev: Event::new(),
            evict_ev: Event::new(),
        }
    }

    // Try to increment in-mem page count.
    // fail if exceeds limit.
    #[inline]
    fn try_inc(&self) -> bool {
        let mut curr_count = self.count.load(Ordering::Acquire);
        loop {
            if curr_count >= self.max_count {
                return false;
            }
            match self.count.compare_exchange(
                curr_count,
                curr_count + 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return true,
                Err(n) => curr_count = n,
            }
        }
    }

    /// Decrement in-mem page count.
    #[inline]
    fn dec(&self) {
        let count = self.count.fetch_sub(1, Ordering::AcqRel);
        if count == self.max_count {
            self.load_ev.notify(1);
        }
    }

    /// Returns current failure rate in allocation attempts.
    #[inline]
    fn alloc_failure_rate(&self) -> f64 {
        self.alloc_failures.failure_rate()
    }

    #[inline]
    fn record_alloc_success(&self) {
        self.alloc_failures.record_success();
    }

    #[inline]
    fn record_alloc_failure(&self) {
        self.alloc_failures.record_failure();
    }

    /// Pin given page in memory.
    #[inline]
    fn pin(&self, page_id: PageID) {
        let mut g = self.set.lock();
        g.insert(page_id);
    }

    #[inline]
    fn unpin(&self, page_id: PageID) {
        let mut g = self.set.lock();
        g.remove(&page_id);
    }

    #[inline]
    fn evict_page(&self, mut g: PageExclusiveGuard<Page>) {
        let page_id = g.page_id();
        g.page_mut().zero(); // zero the page
        let old_kind = g
            .bf_mut()
            .compare_exchange_kind(FrameKind::Evicting, FrameKind::Evicted);
        debug_assert!(old_kind == FrameKind::Evicting);
        self.unpin(page_id);
        self.mark_page_dontneed(g);
    }

    /// Reclaim memory for given page.
    /// This method is invoked after page eviction.
    #[inline]
    fn mark_page_dontneed<T: BufferPage>(&self, mut page_guard: PageExclusiveGuard<T>) {
        // SAFETY: the exclusive page guard yields a live page pointer owned by
        // the arena, and `PAGE_SIZE` matches the mapped page allocation size.
        unsafe {
            assert!(madvise_dontneed(
                page_guard.page_mut() as *mut T as *mut u8,
                PAGE_SIZE
            ));
        }
        drop(page_guard);
        self.dec();
    }

    #[inline]
    fn collect_batch_ids(
        &self,
        clock_hand: ClockHand,
        limit: usize,
        out: &mut Vec<PageID>,
    ) -> Option<ClockHand> {
        let g = self.set.lock();
        clock_collect_batch(&g, clock_hand, limit, out)
    }
}

/// Reserved evictable-page slot waiting to be refilled from disk.
///
/// The reservation owns the exclusive page guard and the in-memory budget that
/// was reserved for the reload. Publishing revives the page as `Hot`; rollback
/// releases the reserved memory budget.
pub(crate) struct EvictPageReservation {
    page_id: PageID,
    page_guard: PageExclusiveGuard<Page>,
    in_mem: Arc<InMemPageSet>,
}

impl EvictPageReservation {
    #[inline]
    /// Creates one reservation from an already locked evicted page slot.
    fn new(page_guard: PageExclusiveGuard<Page>, in_mem: Arc<InMemPageSet>) -> Self {
        let page_id = page_guard.page_id();
        EvictPageReservation {
            page_id,
            page_guard,
            in_mem,
        }
    }
}

impl PageReservation for EvictPageReservation {
    #[inline]
    fn page(&self) -> &Page {
        self.page_guard.page()
    }

    #[inline]
    fn page_mut(&mut self) -> &mut Page {
        self.page_guard.page_mut()
    }

    #[inline]
    fn publish(self) -> Result<PageID> {
        let EvictPageReservation {
            page_id,
            mut page_guard,
            in_mem,
        } = self;
        {
            let bf = page_guard.bf_mut();
            bf.set_dirty(false);
            let old_kind = bf.compare_exchange_kind(FrameKind::Evicted, FrameKind::Hot);
            debug_assert!(old_kind == FrameKind::Evicted);
        }
        drop(page_guard);
        in_mem.pin(page_id);
        Ok(page_id)
    }

    #[inline]
    /// Reclaims the reserved page memory and releases the reserved memory slot.
    fn rollback(self) {
        let EvictPageReservation {
            page_guard, in_mem, ..
        } = self;
        in_mem.mark_page_dontneed(page_guard);
    }
}

/// Worker-owned evictable-pool read reload.
///
/// The submission keeps the reserved page slot and the pool inflight entry
/// together until the read either publishes the page or completes waiters with
/// an error.
pub(crate) struct EvictReadSubmission {
    key: PageID,
    operation: Operation,
    inflight_io: Arc<InflightIO>,
    stats: BufferPoolStatsHandle,
    reservation: Option<PageReservationGuard<EvictPageReservation>>,
    completed: bool,
}

impl EvictReadSubmission {
    #[inline]
    /// Builds one evictable reload submission from an already reserved page.
    fn new(
        page_id: PageID,
        raw_fd: RawFd,
        inflight_io: Arc<InflightIO>,
        stats: BufferPoolStatsHandle,
        mut reservation: PageReservationGuard<EvictPageReservation>,
    ) -> Self {
        let ptr = reservation.page_mut() as *mut Page as *mut u8;
        // SAFETY: the reservation keeps the page slot alive and exclusively
        // owned until the read completes or rolls back.
        let operation = unsafe {
            Operation::pread_borrowed(raw_fd, usize::from(page_id) * PAGE_SIZE, ptr, PAGE_SIZE)
        };
        stats.add_queued_reads(1);
        EvictReadSubmission {
            key: page_id,
            operation,
            inflight_io,
            stats,
            reservation: Some(reservation),
            completed: false,
        }
    }

    #[inline]
    /// Removes the pool inflight entry and wakes all waiters exactly once.
    fn complete_waiters(&mut self, result: Result<PageID>) {
        if self.completed {
            return;
        }
        self.completed = true;
        let completion = {
            let mut g = self.inflight_io.map.lock();
            let mut status = g.remove(&self.key).unwrap_or_else(|| {
                panic!(
                    "inflight read entry missing during completion: page_id={}",
                    self.key
                )
            });
            debug_assert!(status.kind == IOKind::Read);
            self.inflight_io.reads.fetch_sub(1, Ordering::Relaxed);
            status.completion.take()
        };
        if let Some(completion) = completion {
            completion.complete(result);
        }
    }

    #[inline]
    /// Fails the reload before worker completion and wakes joined readers.
    pub(crate) fn fail(mut self, err: Error) {
        drop(self.reservation.take());
        self.stats.add_completed_reads(1);
        self.stats.add_read_errors(1);
        self.complete_waiters(Err(err));
    }

    #[inline]
    pub(crate) fn record_running(&self) {
        self.stats.add_running_reads(1);
    }

    #[inline]
    /// Finalizes one worker-side evictable read completion.
    ///
    /// Successful full-page reads publish the reserved page back to `Hot`.
    /// Short reads and IO errors drop the reservation so memory accounting is
    /// rolled back before waiters are released.
    pub(crate) fn complete(mut self, res: std::io::Result<usize>) -> AIOKind {
        let result = match res {
            Ok(len) if len == PAGE_SIZE => {
                let reservation = self.reservation.take().expect(
                    "evict read submission must still own its page reservation before publish",
                );
                reservation.publish()
            }
            Ok(_) => {
                drop(self.reservation.take());
                Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof).into())
            }
            Err(err) => {
                drop(self.reservation.take());
                Err(err.into())
            }
        };
        self.stats.add_completed_reads(1);
        if result.is_err() {
            self.stats.add_read_errors(1);
        }
        self.complete_waiters(result);
        AIOKind::Read
    }
}

impl IOSubmission for EvictReadSubmission {
    type Key = PageID;

    #[inline]
    fn key(&self) -> &Self::Key {
        &self.key
    }

    #[inline]
    fn operation(&mut self) -> &mut Operation {
        &mut self.operation
    }
}

impl Drop for EvictReadSubmission {
    #[inline]
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        drop(self.reservation.take());
        self.stats.add_completed_reads(1);
        self.stats.add_read_errors(1);
        self.complete_waiters(Err(Error::InternalError));
    }
}

/// Thin wrapper for sparse-file page IO used by the evictable pool.
pub(crate) struct SingleFileIO {
    file: SparseFile,
}

impl SingleFileIO {
    #[inline]
    fn new(file: SparseFile) -> Self {
        SingleFileIO { file }
    }

    /// Prepare one borrowed page write against the pool swap file.
    #[inline]
    pub(crate) fn prepare_write(&self, page_id: PageID, ptr: *mut Page) -> Operation {
        // SAFETY: evict writes borrow one live page-sized arena allocation that
        // stays owned by the page guard until IO completion.
        unsafe {
            Operation::pwrite_borrowed(
                self.file.as_raw_fd(),
                usize::from(page_id) * PAGE_SIZE,
                ptr as *mut u8,
                PAGE_SIZE,
            )
        }
    }
}

/// Shelf provision that hands the shared worker a pool-owned swap-file handle.
pub(crate) struct PoolStorageProvision {
    pub(crate) file_io: SingleFileIO,
}

/// Build one evictable pool together with the storage-worker provision it needs.
pub(crate) fn build_pool_with_swap_file_field(
    config: EvictableBufferPoolConfig,
    swap_file_field_name: &'static str,
    fs: QuiescentGuard<FileSystem>,
) -> Result<(EvictableBufferPool, PoolStorageProvision)> {
    config.role.assert_valid("evictable buffer pool");
    validate_swap_file_path_candidate(swap_file_field_name, &config.data_swap_file)?;
    // 1. Calculate memory usage.
    let max_file_size = config.max_file_size.as_u64() as usize;
    let max_mem_size = config.max_mem_size.as_u64() as usize;
    let max_nbr = max_file_size / mem::size_of::<Page>();
    // We need to hold all frames in-memory, even if many pages
    // can not be loaded into memory at the same time.
    let frame_total_bytes = frame_total_bytes(max_nbr);
    assert!(
        max_mem_size <= max_file_size,
        "max mem size of buffer pool should be no more than max file size"
    );
    assert!(
        max_mem_size > frame_total_bytes,
        "max mem size of buffer pool can not hold all buffer frames"
    );
    let max_nbr_in_mem = (max_mem_size - frame_total_bytes) / mem::size_of::<Page>();
    if max_nbr_in_mem < MIN_IN_MEM_PAGES {
        return Err(Error::BufferPoolSizeTooSmall);
    }
    let eviction_arbiter = config.eviction_arbiter_builder.build(max_nbr_in_mem);

    // 2. Initialize memory of frames and pages.
    let arena = QuiescentArena::new(max_nbr)?;

    // 3. Create the swap file and retain the opened descriptor for worker-owned IO.
    let swap_file_path = path_to_utf8(&config.data_swap_file, swap_file_field_name)?;
    let file = SparseFile::create_or_trunc(swap_file_path, max_file_size)?;
    let raw_fd = file.as_raw_fd();
    let file_io = SingleFileIO::new(file);

    let pool = EvictableBufferPool {
        alloc_map: AllocMap::new(max_nbr),
        alloc_ev: Event::new(),
        fs,
        shutdown_flag: Arc::new(AtomicBool::new(false)),
        in_mem: Arc::new(InMemPageSet::new(max_nbr_in_mem, eviction_arbiter)),
        raw_fd,
        inflight_io: Arc::new(InflightIO::default()),
        stats: BufferPoolStatsHandle::default(),
        role: config.role,
        arena,
    };
    Ok((pool, PoolStorageProvision { file_io }))
}

struct IOStatus {
    kind: IOKind,
    // Shared page-IO completion cell for this specific page state transition.
    completion: Option<Arc<PageIOCompletion>>,
}

struct InflightIO {
    map: Mutex<HashMap<PageID, IOStatus>>,
    reads: AtomicUsize,
    writes: AtomicUsize,
}

impl Default for InflightIO {
    #[inline]
    fn default() -> Self {
        InflightIO {
            map: Mutex::new(HashMap::new()),
            reads: AtomicUsize::new(0),
            writes: AtomicUsize::new(0),
        }
    }
}

impl InflightIO {
    #[allow(clippy::await_holding_lock)]
    #[inline]
    async fn wait_for_write(&self, page_id: PageID, frame: &BufferFrame) -> Result<()> {
        let mut g = self.map.lock();
        match g.entry(page_id) {
            Entry::Vacant(vac) => {
                // Check whether the page is done.
                match frame.kind() {
                    FrameKind::Evicting => {
                        // Evict thread marked it as evicting, but for some reason, does
                        // not process it immediately.
                        // Here we insert a ReadWaitForWrite entry.
                        // And wait for the event
                        let completion = Arc::new(PageIOCompletion::new());
                        vac.insert(IOStatus {
                            kind: IOKind::ReadWaitForWrite,
                            completion: Some(Arc::clone(&completion)),
                        });
                        drop(g); // explicit drop guard before await.
                        completion.wait_result().await.map(|_| ())?;
                    }
                    FrameKind::EvictionFailed => {
                        drop(g);
                        return Err(Error::IOError);
                    }
                    // In any other kind, we let caller retry.
                    FrameKind::Cool
                    | FrameKind::Hot
                    | FrameKind::Fixed
                    | FrameKind::Uninitialized
                    | FrameKind::Evicted => {}
                }
            }
            Entry::Occupied(mut occ) => {
                // There is a write in progress, or a preceding read, waiting for write.
                // In both cases, we can just wait for the event.
                debug_assert!({
                    let kind = occ.get().kind;
                    kind == IOKind::ReadWaitForWrite || kind == IOKind::Write
                });
                let completion = occ
                    .get_mut()
                    .completion
                    .get_or_insert_with(|| Arc::new(PageIOCompletion::new()))
                    .clone();
                drop(g); // explicitly drop guard before await.
                completion.wait_result().await.map(|_| ())?;
            }
        }
        Ok(())
    }

    #[inline]
    fn batch_writes(&self, page_guards: &[PageExclusiveGuard<Page>]) {
        let mut g = self.map.lock();
        let count = page_guards.len();
        for page_guard in page_guards {
            let page_id = page_guard.page_id();
            match g.entry(page_id) {
                Entry::Vacant(vac) => {
                    vac.insert(IOStatus {
                        kind: IOKind::Write,
                        completion: None,
                    });
                }
                Entry::Occupied(mut occ) => {
                    // There could be concurrent read requests.
                    let status = occ.get_mut();
                    debug_assert!(status.kind == IOKind::ReadWaitForWrite);
                    status.kind = IOKind::Write;
                }
            }
        }
        self.writes.fetch_add(count, Ordering::AcqRel);
    }

    #[inline]
    fn contains(&self, page_id: PageID) -> bool {
        let g = self.map.lock();
        g.contains_key(&page_id)
    }
}

/// Internal request channel payload consumed by the IO event loop.
///
/// `Read` schedules a single-page load; `BatchWrite` schedules writeback for a
/// batch of dirty pages selected by eviction.
pub(crate) enum PoolRequest {
    Read(EvictReadSubmission),
    BatchWrite(Vec<PageExclusiveGuard<Page>>, Arc<EventNotifyOnDrop>),
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::buffer::EvictionArbiterBuilder;
    use crate::buffer::guard::PageGuard;
    use crate::buffer::test_page_id;
    use crate::conf::{EngineConfig, FileSystemConfig, TrxSysConfig};
    use crate::engine::Engine;
    use crate::file::fs::FileSystem;
    use crate::file::fs::tests::{
        build_test_fs_owner_in, io_backend_stats_handle_identity as fs_stats_handle_identity,
    };
    use crate::quiescent::{QuiescentBox, QuiescentGuard};
    use crate::row::RowPage;
    use std::ops::Deref;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread;
    use std::time::Duration;
    use tempfile::TempDir;

    const TEST_META_POOL_BYTES: usize = 32 * 1024 * 1024;
    const TEST_INDEX_POOL_BYTES: usize = 64 * 1024 * 1024;
    const TEST_INDEX_MAX_FILE_BYTES: usize = 128 * 1024 * 1024;
    const TEST_READONLY_BUFFER_BYTES: usize = 32 * 1024 * 1024;
    const TEST_WAIT_RETRIES: usize = 100;
    const TEST_WAIT_INTERVAL: Duration = Duration::from_millis(10);

    #[inline]
    pub(crate) fn io_backend_stats_handle_identity(pool: &EvictableBufferPool) -> usize {
        fs_stats_handle_identity(&pool.fs)
    }

    #[inline]
    pub(crate) fn frame_kind(pool: &EvictableBufferPool, page_id: PageID) -> FrameKind {
        pool.arena.frame(page_id).kind()
    }

    #[inline]
    pub(crate) fn raw_fd(pool: &EvictableBufferPool) -> RawFd {
        pool.raw_fd
    }

    fn wait_for(mut predicate: impl FnMut() -> bool) {
        for _ in 0..TEST_WAIT_RETRIES {
            if predicate() {
                return;
            }
            thread::sleep(TEST_WAIT_INTERVAL);
        }
        panic!("condition was not satisfied before timeout");
    }

    fn build_raw_pool_for_test(
        config: EvictableBufferPoolConfig,
    ) -> Result<(
        QuiescentBox<FileSystem>,
        EvictableBufferPool,
        PoolStorageProvision,
    )> {
        let swap_file = config.data_swap_file_ref();
        let storage_root = if swap_file.is_absolute() {
            swap_file
                .parent()
                .expect("absolute test swap file must have a parent")
                .to_path_buf()
        } else {
            std::env::current_dir().unwrap()
        };
        let fs_owner = build_test_fs_owner_in(&storage_root)?;
        let fs = fs_owner.guard();
        let (pool, storage) = build_pool_with_swap_file_field(config, "data_swap_file", fs)?;
        Ok((fs_owner, pool, storage))
    }

    struct StartedEvictPool {
        engine: Engine,
    }

    impl StartedEvictPool {
        fn new(config: EvictableBufferPoolConfig) -> Self {
            let swap_file = config.data_swap_file_ref();
            let (storage_root, data_swap_file) = if swap_file.is_absolute() {
                (
                    swap_file
                        .parent()
                        .expect("absolute test swap file must have a parent")
                        .to_path_buf(),
                    PathBuf::from(
                        swap_file
                            .file_name()
                            .expect("absolute test swap file must have a file name"),
                    ),
                )
            } else {
                (std::env::current_dir().unwrap(), swap_file.to_path_buf())
            };
            let config = config.data_swap_file(data_swap_file);
            let engine = smol::block_on(async {
                EngineConfig::default()
                    .storage_root(storage_root)
                    .meta_buffer(TEST_META_POOL_BYTES)
                    .index_buffer(TEST_INDEX_POOL_BYTES)
                    .index_max_file_size(TEST_INDEX_MAX_FILE_BYTES)
                    .data_buffer(config)
                    .file(
                        FileSystemConfig::default()
                            .data_dir(".")
                            .readonly_buffer_size(TEST_READONLY_BUFFER_BYTES),
                    )
                    .trx(TrxSysConfig::default().skip_recovery(true))
                    .build()
                    .await
                    .unwrap()
            });
            Self { engine }
        }

        fn owner_guard(&self) -> QuiescentGuard<EvictableBufferPool> {
            self.engine.mem_pool.clone_inner()
        }

        fn shutdown(&self) {
            self.engine.shutdown().unwrap();
        }
    }

    impl Deref for StartedEvictPool {
        type Target = EvictableBufferPool;

        fn deref(&self) -> &Self::Target {
            &self.engine.mem_pool
        }
    }

    impl Drop for StartedEvictPool {
        fn drop(&mut self) {
            let _ = self.engine.shutdown();
        }
    }

    #[test]
    fn test_evictable_buffer_pool_shutdown_is_idempotent_before_workers_start() {
        let temp_dir = TempDir::new().unwrap();
        let (_fs_owner, pool, _storage) = build_raw_pool_for_test(
            EvictableBufferPoolConfig::default()
                .role(PoolRole::Mem)
                .data_swap_file(temp_dir.path().join("data.swp"))
                .max_mem_size(64u64 * 1024 * 1024)
                .max_file_size(128u64 * 1024 * 1024),
        )
        .unwrap();

        pool.signal_shutdown();
        pool.signal_shutdown();
    }

    #[test]
    fn test_evict_buffer_pool_simple() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let pool = StartedEvictPool::new(
                EvictableBufferPoolConfig::default()
                    .role(PoolRole::Mem)
                    .data_swap_file(temp_dir.path().join("data.swp"))
                    .max_mem_size(1024u64 * 1024 * 128)
                    .max_file_size(1024u64 * 1024 * 256),
            );
            let pool_guard = pool.pool_guard();
            {
                let g = pool.allocate_page::<RowPage>(&pool_guard).await;
                assert_eq!(g.page_id(), 0);
            }
            {
                let g = pool.allocate_page::<RowPage>(&pool_guard).await;
                assert_eq!(g.page_id(), 1);
                let stale_versioned = g.versioned_page_id();
                pool.deallocate_page(g);
                let g = pool.allocate_page::<RowPage>(&pool_guard).await;
                assert_eq!(g.page_id(), 1);
                assert_eq!(g.bf().generation(), stale_versioned.generation + 2);
                let current_versioned = g.versioned_page_id();
                drop(g);

                let g = pool
                    .get_page_versioned::<RowPage>(
                        &pool_guard,
                        current_versioned,
                        LatchFallbackMode::Shared,
                    )
                    .await
                    .unwrap();
                assert!(g.is_some());

                let g = pool
                    .get_page_versioned::<RowPage>(
                        &pool_guard,
                        stale_versioned,
                        LatchFallbackMode::Shared,
                    )
                    .await
                    .unwrap();
                assert!(g.is_none());
            }
            {
                let g = pool.allocate_page::<RowPage>(&pool_guard).await;
                let page_id = g.page_id();
                let versioned = g.versioned_page_id();
                drop(g);

                // Keep an optimistic guard, then reuse the page slot.
                let stale_guard = pool
                    .get_page_versioned::<RowPage>(
                        &pool_guard,
                        versioned,
                        LatchFallbackMode::Shared,
                    )
                    .await
                    .unwrap()
                    .unwrap();
                let g = pool
                    .get_page::<RowPage>(&pool_guard, page_id, LatchFallbackMode::Exclusive)
                    .await
                    .expect("buffer-pool read failed in test")
                    .lock_exclusive_async()
                    .await
                    .unwrap();
                pool.deallocate_page(g);
                let g = pool.allocate_page::<RowPage>(&pool_guard).await;
                assert_eq!(g.page_id(), page_id);
                drop(g);

                assert!(stale_guard.lock_shared_async().await.is_none());
            }
            {
                let g = pool
                    .get_page::<RowPage>(&pool_guard, test_page_id(0), LatchFallbackMode::Spin)
                    .await
                    .expect("buffer-pool read failed in test")
                    .downgrade();
                assert_eq!(g.page_id(), 0);
                let p = g.facade();
                // test coupling.
                let c = pool
                    .get_child_page::<RowPage>(
                        &pool_guard,
                        &p,
                        test_page_id(1),
                        LatchFallbackMode::Exclusive,
                    )
                    .await;
                let c = c.unwrap();
                drop(c);
            }
            {
                let g = pool
                    .get_page::<RowPage>(&pool_guard, test_page_id(0), LatchFallbackMode::Spin)
                    .await
                    .expect("buffer-pool read failed in test")
                    .downgrade();
                assert_eq!(g.page_id(), 0);
                let p = g.facade();

                // modify page 0.
                let g = pool
                    .get_page::<RowPage>(&pool_guard, test_page_id(0), LatchFallbackMode::Exclusive)
                    .await
                    .expect("buffer-pool read failed in test")
                    .verify_exclusive_async::<false>()
                    .await;
                drop(g.unwrap());

                // test coupling fail.
                let c = pool
                    .get_child_page::<RowPage>(
                        &pool_guard,
                        &p,
                        test_page_id(1),
                        LatchFallbackMode::Exclusive,
                    )
                    .await;
                assert!(c.unwrap().is_invalid());
            }
        })
    }

    #[test]
    fn test_evict_page_reservation_rollback_reclaims_page_memory() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let (_fs_owner, pool, _storage) = build_raw_pool_for_test(
                EvictableBufferPoolConfig::default()
                    .role(PoolRole::Mem)
                    .data_swap_file(temp_dir.path().join("data.swp"))
                    .max_mem_size(64u64 * 1024 * 1024)
                    .max_file_size(128u64 * 1024 * 1024),
            )
            .unwrap();
            let pool_guard = pool.pool_guard();

            let mut page_guard = pool.allocate_page::<Page>(&pool_guard).await;
            let page_id = page_guard.page_id();
            page_guard.bf_mut().set_kind(FrameKind::Evicting);
            pool.in_mem.evict_page(page_guard);

            assert_eq!(pool.in_mem.count.load(Ordering::Acquire), 0);
            assert_eq!(pool.arena.frame(page_id).kind(), FrameKind::Evicted);

            assert!(pool.in_mem.try_inc());
            let mut page_guard = pool.try_lock_page_exclusive(&pool_guard, page_id).unwrap();
            page_guard.page_mut()[0] = 0xAB;
            drop(PageReservationGuard::new(EvictPageReservation::new(
                page_guard,
                Arc::clone(&pool.in_mem),
            )));

            assert_eq!(pool.in_mem.count.load(Ordering::Acquire), 0);
            assert_eq!(pool.arena.frame(page_id).kind(), FrameKind::Evicted);

            let page_guard = pool.try_lock_page_exclusive(&pool_guard, page_id).unwrap();
            assert_eq!(page_guard.page()[0], 0);
        });
    }

    #[test]
    fn test_writeback_failure_preserves_failed_frame_kind_and_surfaces_error() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let (_fs_owner, pool, storage) = build_raw_pool_for_test(
                EvictableBufferPoolConfig::default()
                    .role(PoolRole::Mem)
                    .data_swap_file(temp_dir.path().join("data.swp"))
                    .max_mem_size(64u64 * 1024 * 1024)
                    .max_file_size(128u64 * 1024 * 1024),
            )
            .unwrap();
            let owner = QuiescentBox::new(pool);
            let pool_guard = owner.pool_guard();
            let sync_pool = owner.guard().into_sync();
            let mut state_machine = EvictablePoolStateMachine {
                pool: sync_pool,
                file_io: storage.file_io,
            };

            let mut page_guard = owner.allocate_page::<Page>(&pool_guard).await;
            let page_id = page_guard.page_id();
            page_guard.page_mut()[0] = 0xAB;
            page_guard.bf_mut().set_kind(FrameKind::Evicting);

            let completion = Arc::new(PageIOCompletion::new());
            {
                let mut g = owner.inflight_io.map.lock();
                g.insert(
                    page_id,
                    IOStatus {
                        kind: IOKind::Write,
                        completion: Some(Arc::clone(&completion)),
                    },
                );
            }
            owner.inflight_io.writes.store(1, Ordering::Relaxed);

            let operation = state_machine
                .file_io
                .prepare_write(page_id, page_guard.page_mut());
            let kind = state_machine.on_complete(
                EvictSubmission::Write(PageIO {
                    key: page_id,
                    operation,
                    page_guard,
                    batch_done: None,
                }),
                Err(std::io::Error::other("writeback failed")),
            );

            assert_eq!(kind, AIOKind::Write);
            assert_eq!(owner.arena.frame(page_id).kind(), FrameKind::EvictionFailed);
            assert!(matches!(
                completion.wait_result().await,
                Err(Error::IOError)
            ));
            assert_eq!(owner.inflight_io.writes.load(Ordering::Relaxed), 0);
            assert!(!owner.inflight_io.map.lock().contains_key(&page_id));

            drop(state_machine);
            drop(pool_guard);
            drop(owner);
        });
    }

    #[test]
    fn test_failed_eviction_frame_returns_error_to_readers() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let (_fs_owner, pool, _storage) = build_raw_pool_for_test(
                EvictableBufferPoolConfig::default()
                    .role(PoolRole::Mem)
                    .data_swap_file(temp_dir.path().join("data.swp"))
                    .max_mem_size(64u64 * 1024 * 1024)
                    .max_file_size(128u64 * 1024 * 1024),
            )
            .unwrap();
            let pool_guard = pool.pool_guard();
            let mut page_guard = pool.allocate_page::<Page>(&pool_guard).await;
            let page_id = page_guard.page_id();
            let versioned = page_guard.versioned_page_id();
            page_guard.bf_mut().set_kind(FrameKind::EvictionFailed);
            drop(page_guard);

            assert!(matches!(
                pool.inflight_io
                    .wait_for_write(page_id, pool.arena.frame(page_id))
                    .await,
                Err(Error::IOError)
            ));
            assert!(matches!(
                pool.get_page::<Page>(&pool_guard, page_id, LatchFallbackMode::Shared)
                    .await,
                Err(Error::IOError)
            ));
            assert!(matches!(
                pool.get_page_versioned::<Page>(&pool_guard, versioned, LatchFallbackMode::Shared,)
                    .await,
                Err(Error::IOError)
            ));
        });
    }

    #[test]
    fn test_failed_eviction_frame_can_retry_and_clear_on_success() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let (_fs_owner, pool, _storage) = build_raw_pool_for_test(
                EvictableBufferPoolConfig::default()
                    .role(PoolRole::Mem)
                    .data_swap_file(temp_dir.path().join("data.swp"))
                    .max_mem_size(64u64 * 1024 * 1024)
                    .max_file_size(128u64 * 1024 * 1024),
            )
            .unwrap();
            let pool_guard = pool.pool_guard();
            let mut page_guard = pool.allocate_page::<Page>(&pool_guard).await;
            let page_id = page_guard.page_id();
            page_guard.bf_mut().set_kind(FrameKind::EvictionFailed);
            drop(page_guard);

            let arena_guard = pool.arena.arena_guard(pool_guard.clone());
            let page_guard = clock_sweep_candidate(&arena_guard, page_id)
                .expect("failed eviction should remain retryable");
            assert_eq!(page_guard.bf().kind(), FrameKind::Evicting);

            pool.in_mem.evict_page(page_guard);
            assert_eq!(pool.arena.frame(page_id).kind(), FrameKind::Evicted);
            assert_eq!(pool.in_mem.count.load(Ordering::Acquire), 0);

            assert!(pool.in_mem.try_inc());
            let page_guard = pool.try_lock_page_exclusive(&pool_guard, page_id).unwrap();
            let reservation = PageReservationGuard::new(EvictPageReservation::new(
                page_guard,
                Arc::clone(&pool.in_mem),
            ));
            assert_eq!(reservation.publish().unwrap(), page_id);
            assert_eq!(pool.arena.frame(page_id).kind(), FrameKind::Hot);
            assert_eq!(pool.in_mem.count.load(Ordering::Acquire), 1);
        });
    }

    #[test]
    fn test_evict_buffer_pool_full() {
        // 100 in-mem pages and 200 total pages.
        let temp_dir = TempDir::new().unwrap();
        let pool = StartedEvictPool::new(
            EvictableBufferPoolConfig::default()
                .role(PoolRole::Mem)
                .data_swap_file(temp_dir.path().join("data.swp"))
                .max_mem_size(64u64 * 1024 * 130)
                .max_file_size(128u64 * 1024 * 130),
        );
        let pool_ref = pool.owner_guard();
        let pool_guard = pool.pool_guard();

        let (tx, rx) = flume::unbounded();
        let handle1 = {
            let pool_guard = pool_guard.clone();
            let pool_ref = pool_ref.clone();
            thread::spawn(move || {
                smol::block_on(async move {
                    // allocate more pages than memory limit.
                    for i in 0..160 {
                        let g = pool_ref.allocate_page::<RowPage>(&pool_guard).await;
                        let _ = tx.send(g.page_id());
                        println!("allocated page {}", i);
                    }
                    drop(tx);
                })
            })
        };

        thread::sleep(Duration::from_millis(50));
        println!("wait sometime");
        smol::block_on(async {
            while let Ok(page_id) = rx.recv() {
                let g = pool
                    .get_page::<RowPage>(&pool_guard, page_id, LatchFallbackMode::Exclusive)
                    .await
                    .expect("buffer-pool read failed in test")
                    .lock_exclusive_async()
                    .await
                    .unwrap();
                pool.deallocate_page(g);
                println!("deallocated page {}", page_id);
            }
        });

        handle1.join().unwrap();
    }

    #[test]
    fn test_evict_buffer_pool_alloc() {
        // max pages 16k, max in-mem 1k
        let temp_dir = TempDir::new().unwrap();
        let pool = StartedEvictPool::new(
            EvictableBufferPoolConfig::default()
                .role(PoolRole::Mem)
                .data_swap_file(temp_dir.path().join("data.swp"))
                .max_mem_size(1024u64 * 1024 * 64)
                .max_file_size(1024u64 * 1024 * 128),
        );
        let pool_guard = pool.pool_guard();

        println!(
            "max_nbr={}, max_nbr_in_mem={}",
            pool.capacity(),
            pool.in_mem.max_count
        );
        smol::block_on(async {
            let mut pages = vec![];
            for _ in 0..2048 {
                let g = pool.allocate_page::<RowPage>(&pool_guard).await;
                pages.push(g.page_id());
            }
            debug_assert!(pages.len() == 2048);
        });
    }

    #[test]
    fn test_evictable_buffer_pool_started_scope_starts_workers_for_eviction_and_reload() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let pool = StartedEvictPool::new(
                EvictableBufferPoolConfig::default()
                    .role(PoolRole::Mem)
                    .data_swap_file(temp_dir.path().join("data.swp"))
                    .max_mem_size(64u64 * 1024 * 130)
                    .max_file_size(128u64 * 1024 * 130),
            );
            let pool_guard = EvictableBufferPool::pool_guard(&pool);
            let total_pages = pool.in_mem.max_count + 64;

            for i in 0..total_pages {
                let mut g = pool.allocate_page::<Page>(&pool_guard).await;
                let payload = format!("page-{i}");
                g.page_mut()[..payload.len()].copy_from_slice(payload.as_bytes());
                drop(g);
            }

            wait_for(|| pool.arena.frame(test_page_id(0)).kind() == FrameKind::Evicted);

            let g = pool
                .get_page::<Page>(&pool_guard, test_page_id(0), LatchFallbackMode::Shared)
                .await
                .expect("buffer-pool read failed in test");
            let g = g.lock_shared_async().await.unwrap();
            assert_eq!(&g.page()[..6], b"page-0");
            drop(g);

            pool.shutdown();
            drop(pool_guard);
            drop(pool);
        });
    }

    #[test]
    fn test_evictable_buffer_pool_drop_waits_for_outstanding_guard() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let (_fs_owner, pool, _storage) = build_raw_pool_for_test(
                EvictableBufferPoolConfig::default()
                    .role(PoolRole::Mem)
                    .data_swap_file(temp_dir.path().join("data.swp"))
                    .max_mem_size(1024u64 * 1024 * 32)
                    .max_file_size(1024u64 * 1024 * 64),
            )
            .unwrap();
            let pool = QuiescentBox::new(pool);
            let guard = {
                let pool_guard = EvictableBufferPool::pool_guard(&pool);
                pool.allocate_page::<RowPage>(&pool_guard).await
            };
            let dropped = Arc::new(AtomicBool::new(false));
            let dropped_flag = Arc::clone(&dropped);

            let handle = thread::spawn(move || {
                drop(pool);
                dropped_flag.store(true, Ordering::SeqCst);
            });

            thread::sleep(Duration::from_millis(50));
            assert!(!dropped.load(Ordering::SeqCst));
            assert_eq!(guard.page_id(), 0);

            drop(guard);
            handle.join().unwrap();
            assert!(dropped.load(Ordering::SeqCst));
        });
    }

    #[test]
    fn test_evict_buffer_pool_multi_threads() {
        use rand::{Rng, prelude::IndexedRandom};
        // max pages 2k, max in-mem 1k
        let temp_dir = TempDir::new().unwrap();
        let pool = StartedEvictPool::new(
            EvictableBufferPoolConfig::default()
                .role(PoolRole::Mem)
                .data_swap_file(temp_dir.path().join("data.swp"))
                .max_mem_size(64u64 * 1024 * 1024)
                .max_file_size(64u64 * 1024 * 2048),
        );
        let pool_guard = pool.pool_guard();

        println!(
            "max_nbr={}, max_nbr_in_mem={}",
            pool.capacity(),
            pool.in_mem.max_count
        );
        let mut handles = vec![];
        for thread_id in 0..10 {
            let pool_guard = pool_guard.clone();
            let pool_ref = pool.owner_guard();
            let handle = thread::spawn(move || {
                smol::block_on(async {
                    let mut rng = rand::rng();

                    let mut pages = vec![];
                    for _ in 0..200 {
                        // allocate a new page.
                        println!("thread {} alloc page start", thread_id);
                        let g = pool_ref.allocate_page::<RowPage>(&pool_guard).await;
                        pages.push(g.page_id());
                        println!(
                            "thread {} alloc page end page_id {}, allocated {}, in-mem {}, target_free {}, reads {}, writes {}",
                            thread_id,
                            g.page_id(),
                            pool_ref.allocated(),
                            pool_ref.in_mem.count.load(Ordering::Relaxed),
                            pool_ref.in_mem.eviction_arbiter.target_free(),
                            pool_ref.inflight_io.reads.load(Ordering::Relaxed),
                            pool_ref.inflight_io.writes.load(Ordering::Relaxed),
                        );
                        // unlock the page.
                        drop(g);
                        if rng.random_bool(0.3) {
                            // choose one page and try to lock it.
                            let page_id = pages.choose(&mut rng).cloned().unwrap();
                            println!(
                                "thread {} read page {}, allocated {}, in-mem {}, target_free {}, reads {}, writes {}",
                                thread_id,
                                page_id,
                                pool_ref.allocated(),
                                pool_ref.in_mem.count.load(Ordering::Relaxed),
                                pool_ref.in_mem.eviction_arbiter.target_free(),
                                pool_ref.inflight_io.reads.load(Ordering::Relaxed),
                                pool_ref.inflight_io.writes.load(Ordering::Relaxed),
                            );
                            let g = pool_ref
                                .get_page::<RowPage>(
                                    &pool_guard,
                                    page_id,
                                    LatchFallbackMode::Shared,
                                )
                                .await;
                            println!("thread {} read page {} end", thread_id, page_id);
                            drop(g);
                        }
                    }
                    println!("thread {} done", thread_id);
                })
            });
            handles.push(handle);
        }
        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn test_evict_buffer_pool_uses_custom_arbiter_builder() {
        let temp_dir = TempDir::new().unwrap();
        let pool = StartedEvictPool::new(
            EvictableBufferPoolConfig::default()
                .role(PoolRole::Mem)
                .data_swap_file(temp_dir.path().join("data.swp"))
                .max_mem_size(1024u64 * 1024 * 10)
                .max_file_size(1024u64 * 1024 * 16)
                .eviction_arbiter_builder(
                    EvictionArbiterBuilder::new()
                        .target_free(2)
                        .hysteresis(1)
                        .failure_rate_threshold(0.05)
                        .failure_window(5)
                        .dynamic_batch_bounds(3, 3),
                ),
        );
        let _pool_guard = pool.pool_guard();
        let arbiter = pool.in_mem.eviction_arbiter;

        assert_eq!(arbiter.target_free(), 2);
        assert_eq!(arbiter.hysteresis(), 1);
        assert_eq!(arbiter.failure_window(), 5);

        // Verify failure-window wiring through the in-mem tracker.
        for _ in 0..5 {
            pool.in_mem.record_alloc_failure();
        }
        for _ in 0..2 {
            pool.in_mem.record_alloc_success();
        }
        let failure_rate = pool.in_mem.alloc_failure_rate();
        assert!((failure_rate - (3.0 / 5.0)).abs() < 1e-9);

        // Verify batch bounds/threshold from builder influence decision output.
        let decision = arbiter
            .decide(pool.in_mem.max_count / 2, pool.in_mem.max_count, 0, 0.10, 1)
            .unwrap();
        assert_eq!(decision.batch_size, 3);
    }

    #[test]
    #[should_panic(expected = "pool guard identity mismatch")]
    fn test_evictable_buffer_pool_panics_on_foreign_guard() {
        smol::block_on(async {
            let temp_dir1 = TempDir::new().unwrap();
            let temp_dir2 = TempDir::new().unwrap();
            let pool1 = StartedEvictPool::new(
                EvictableBufferPoolConfig::default()
                    .role(PoolRole::Mem)
                    .data_swap_file(temp_dir1.path().join("data1.swp"))
                    .max_mem_size(1024u64 * 1024 * 32)
                    .max_file_size(1024u64 * 1024 * 64),
            );
            let pool2 = StartedEvictPool::new(
                EvictableBufferPoolConfig::default()
                    .role(PoolRole::Mem)
                    .data_swap_file(temp_dir2.path().join("data2.swp"))
                    .max_mem_size(1024u64 * 1024 * 32)
                    .max_file_size(1024u64 * 1024 * 64),
            );
            let pool1_guard = pool1.pool_guard();
            let pool2_guard = pool2.pool_guard();

            let page = pool1.allocate_page::<RowPage>(&pool1_guard).await;
            let page_id = page.page_id();
            drop(page);

            let _ = pool1
                .get_page::<RowPage>(&pool2_guard, page_id, LatchFallbackMode::Shared)
                .await;
        });
    }

    #[test]
    fn test_evictable_buffer_pool_build_reports_data_swap_file_label() {
        let temp_dir = TempDir::new().unwrap();
        let fs_owner = build_test_fs_owner_in(temp_dir.path()).unwrap();
        let err = match EvictableBufferPoolConfig::default()
            .role(PoolRole::Mem)
            .data_swap_file("data.bin")
            .build_for_engine(fs_owner.guard())
        {
            Ok(_) => panic!("expected invalid storage path"),
            Err(err) => err,
        };
        match err {
            Error::InvalidStoragePath(msg) => {
                assert!(msg.contains("data_swap_file"));
                assert!(msg.contains(".swp"));
            }
            err => panic!("expected invalid storage path, got {err:?}"),
        }
    }

    #[test]
    fn test_evictable_buffer_pool_build_reports_index_swap_file_label() {
        let temp_dir = TempDir::new().unwrap();
        let fs_owner = build_test_fs_owner_in(temp_dir.path()).unwrap();
        let err = match EvictableBufferPoolConfig::default()
            .role(PoolRole::Index)
            .data_swap_file("index.bin")
            .build_index_for_engine(fs_owner.guard())
        {
            Ok(_) => panic!("expected invalid storage path"),
            Err(err) => err,
        };
        match err {
            Error::InvalidStoragePath(msg) => {
                assert!(msg.contains("index_swap_file"));
                assert!(msg.contains(".swp"));
            }
            err => panic!("expected invalid storage path, got {err:?}"),
        }
    }
}
