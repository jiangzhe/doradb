use crate::buffer::PersistedFileID;
use crate::buffer::arena::{ArenaGuard, QuiescentArena};
use crate::buffer::evictor::{
    ClockHand, EvictionArbiter, EvictionArbiterBuilder, EvictionRuntime, Evictor,
    FailureRateTracker, PressureDeltaClockPolicy, clock_collect_batch, clock_sweep_candidate,
};
use crate::buffer::frame::{BufferFrame, FrameKind};
use crate::buffer::guard::{
    FacadePageGuard, PageExclusiveGuard, PageGuard, PageLatchGuard, PageSharedGuard,
};
use crate::buffer::load::{PageReservation, PageReservationGuard};
use crate::buffer::page::{BufferPage, PAGE_SIZE, Page, PageID, VersionedPageID};
use crate::buffer::util::madvise_dontneed;
use crate::buffer::{
    BufferPool, BufferPoolStats, BufferPoolStatsHandle, PageIOCompletion, PoolGuard, PoolIdentity,
    PoolRole, ReadonlyPageValidator,
};
use crate::component::{Component, ComponentRegistry, ShelfScope};
use crate::error::Validation::Valid;
use crate::error::{Error, PersistedFileKind, Result, Validation};
use crate::file::multi_table_file::MultiTableFile;
use crate::file::table_file::TableFile;
use crate::io::{AIOKind, IOSubmission, Operation};
use crate::latch::LatchFallbackMode;
use crate::quiescent::{QuiescentBox, QuiescentGuard, SyncQuiescentGuard};
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use event_listener::{Event, EventListener, listener};
use parking_lot::Mutex;
use std::collections::BTreeSet;
use std::mem;
use std::os::fd::RawFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;

/// Minimum number of frames required for global readonly pool.
///
/// Very small pools provide little practical value and can stall eviction/load flow.
const MIN_READONLY_POOL_PAGES: usize = 256;

/// Physical persisted-block identity for cache lookup and invalidation.
///
/// This intentionally excludes root version to preserve cache hits across
/// root swaps when physical blocks are unchanged.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PersistedBlockKey {
    /// Persisted file identity owning the block.
    pub file_id: PersistedFileID,
    /// Physical page/block id in the backing file.
    pub block_id: PageID,
}

impl PersistedBlockKey {
    /// Builds a key from file id and physical block id.
    #[inline]
    pub fn new(file_id: PersistedFileID, block_id: PageID) -> Self {
        PersistedBlockKey { file_id, block_id }
    }
}

/// Keepalive handle for one real readonly backing file.
///
/// This stays on the production path only: readonly-cache misses are always
/// satisfied by queueing one real read against either a table file or a
/// multi-table file.
#[derive(Clone)]
pub(crate) enum ReadonlyBackingFile {
    Table(Arc<TableFile>),
    Multi(Arc<MultiTableFile>),
}

impl ReadonlyBackingFile {
    #[inline]
    async fn queue_read(&self, req: ReadSubmission) -> Result<()> {
        match self {
            ReadonlyBackingFile::Table(file) => file.queue_read(req).await,
            ReadonlyBackingFile::Multi(file) => file.queue_read(req).await,
        }
    }

    #[inline]
    fn raw_fd(&self) -> RawFd {
        match self {
            ReadonlyBackingFile::Table(file) => file.raw_fd(),
            ReadonlyBackingFile::Multi(file) => file.raw_fd(),
        }
    }

    #[inline]
    fn read_operation(&self, key: PersistedBlockKey, ptr: *mut u8, len: usize) -> Operation {
        let offset = key.block_id as usize * PAGE_SIZE;
        // SAFETY: readonly loads borrow frame-owned page memory that remains
        // live until IO completion, and offsets are page-aligned by construction.
        unsafe { Operation::pread_borrowed(self.raw_fd(), offset, ptr, len) }
    }
}

impl From<Arc<TableFile>> for ReadonlyBackingFile {
    #[inline]
    fn from(value: Arc<TableFile>) -> Self {
        ReadonlyBackingFile::Table(value)
    }
}

impl From<Arc<MultiTableFile>> for ReadonlyBackingFile {
    #[inline]
    fn from(value: Arc<MultiTableFile>) -> Self {
        ReadonlyBackingFile::Multi(value)
    }
}

/// Global readonly cache owner shared across files.
///
/// This type owns the frame/page arena and maintains a forward mapping
/// from physical on-disk identity to in-memory frame id.
///
/// Reverse lookup is stored inline in `BufferFrame` as persisted-block metadata.
pub struct GlobalReadonlyBufferPool {
    size: usize,
    mappings: Arc<DashMap<PersistedBlockKey, PageID>>,
    inflight_loads: Arc<DashMap<PersistedBlockKey, Arc<PageIOCompletion>>>,
    residency: Arc<ReadonlyResidency>,
    eviction_arbiter: EvictionArbiter,
    shutdown_flag: Arc<AtomicBool>,
    role: PoolRole,
    stats: BufferPoolStatsHandle,
    arena: QuiescentArena,
}

pub(crate) struct DiskPoolWorkers;

pub(crate) struct DiskPoolWorkersOwned {
    pool: SyncQuiescentGuard<GlobalReadonlyBufferPool>,
    evict_thread: Mutex<Option<JoinHandle<()>>>,
}

impl GlobalReadonlyBufferPool {
    /// Creates a raw global readonly pool with a target memory budget in bytes.
    ///
    /// This constructor intentionally does not start the eviction worker.
    /// Callers must first place the pool into a stable owner such as
    /// [`QuiescentBox`] before starting guarded worker threads.
    #[inline]
    pub fn with_capacity(role: PoolRole, pool_size: usize) -> Result<Self> {
        Self::with_capacity_and_arbiter_builder(role, pool_size, EvictionArbiter::builder())
    }

    /// Creates a raw global readonly pool with explicit eviction arbiter builder.
    ///
    /// This constructor intentionally does not start the eviction worker.
    #[inline]
    pub fn with_capacity_and_arbiter_builder(
        role: PoolRole,
        pool_size: usize,
        eviction_arbiter_builder: EvictionArbiterBuilder,
    ) -> Result<Self> {
        role.assert_valid("global readonly buffer pool");
        let frame_plus_page = mem::size_of::<BufferFrame>() + mem::size_of::<Page>();
        let size = pool_size / frame_plus_page;
        if size < MIN_READONLY_POOL_PAGES {
            return Err(Error::BufferPoolSizeTooSmall);
        }
        let eviction_arbiter = eviction_arbiter_builder.build(size);
        let arena = QuiescentArena::new(size)?;
        let pool = GlobalReadonlyBufferPool {
            size,
            mappings: Arc::new(DashMap::new()),
            inflight_loads: Arc::new(DashMap::new()),
            residency: Arc::new(ReadonlyResidency::new(size, eviction_arbiter)),
            eviction_arbiter,
            shutdown_flag: Arc::new(AtomicBool::new(false)),
            role,
            stats: BufferPoolStatsHandle::default(),
            arena,
        };
        Ok(pool)
    }

    /// Returns total number of frame slots in this pool.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.size
    }

    /// Returns number of currently mapped cache entries.
    #[inline]
    pub fn allocated(&self) -> usize {
        self.mappings.len()
    }

    /// Returns one snapshot of shared readonly-pool access and load counters.
    #[inline]
    pub fn stats(&self) -> BufferPoolStats {
        self.stats.snapshot()
    }

    #[inline]
    pub fn pool_guard(&self) -> PoolGuard {
        debug_assert!(!matches!(self.role, PoolRole::Invalid));
        self.arena.guard()
    }

    #[inline]
    pub(crate) fn identity(&self) -> PoolIdentity {
        self.arena.identity()
    }

    #[inline]
    fn try_lock_page_exclusive(
        &self,
        guard: &PoolGuard,
        frame_id: PageID,
    ) -> Option<PageExclusiveGuard<Page>> {
        self.arena.try_lock_page_exclusive(guard, frame_id)
    }

    #[inline]
    fn validate_guard(&self, guard: &PoolGuard) {
        guard.assert_matches(self.identity(), "global readonly buffer pool");
    }

    #[inline]
    async fn reserve_frame_id_for_load(&self) -> Result<PageID> {
        loop {
            if self.shutdown_flag.load(Ordering::Acquire) {
                return Err(Error::InvalidState);
            }
            if let Some(frame_id) = self.residency.try_reserve_frame() {
                if self.shutdown_flag.load(Ordering::Acquire) {
                    self.residency.release_free(frame_id);
                    return Err(Error::InvalidState);
                }
                self.residency.record_alloc_success();
                return Ok(frame_id);
            }
            self.residency.record_alloc_failure();
            listener!(self.residency.free_ev => listener);
            if self.shutdown_flag.load(Ordering::Acquire) {
                return Err(Error::InvalidState);
            }
            if let Some(frame_id) = self.residency.try_reserve_frame() {
                if self.shutdown_flag.load(Ordering::Acquire) {
                    self.residency.release_free(frame_id);
                    return Err(Error::InvalidState);
                }
                self.residency.record_alloc_success();
                return Ok(frame_id);
            }
            self.residency.record_alloc_failure();
            self.residency.evict_ev.notify(1);
            listener.await;
            if self.shutdown_flag.load(Ordering::Acquire) {
                return Err(Error::InvalidState);
            }
        }
    }

    #[inline]
    fn complete_inflight_load(
        &self,
        key: PersistedBlockKey,
        inflight: &Arc<PageIOCompletion>,
        result: Result<PageID>,
    ) {
        inflight.complete(result);
        match self.inflight_loads.entry(key) {
            Entry::Occupied(occ) if Arc::ptr_eq(occ.get(), inflight) => {
                occ.remove();
            }
            _ => {}
        }
    }

    /// Looks up mapped frame id for a given physical cache key.
    #[inline]
    pub fn try_get_frame_id(&self, key: &PersistedBlockKey) -> Option<PageID> {
        self.mappings.get(key).map(|v| *v)
    }

    /// Looks up persisted-block identity by frame id.
    #[inline]
    pub fn try_get_block_key(&self, frame_id: PageID) -> Option<PersistedBlockKey> {
        if frame_id as usize >= self.size {
            return None;
        }
        let frame = self.arena.frame(frame_id);
        frame
            .persisted_block_key()
            .map(|(file_id, block_id)| PersistedBlockKey::new(file_id, block_id))
    }

    /// Binds one persisted-block key to an exclusively locked frame.
    ///
    /// Binding is idempotent for the same key/frame pair and returns
    /// `Error::InvalidState` for conflicting mapping attempts.
    #[inline]
    pub fn bind_frame(
        &self,
        key: PersistedBlockKey,
        frame_guard: &mut PageExclusiveGuard<Page>,
    ) -> Result<()> {
        let frame_id = frame_guard.page_id();
        if frame_id as usize >= self.size {
            return Err(Error::InvalidArgument);
        }
        let frame = frame_guard.bf_mut();
        let expected_frame = self.arena.frame(frame_id) as *const BufferFrame;
        if !std::ptr::eq(frame as *const BufferFrame, expected_frame) {
            return Err(Error::InvalidArgument);
        }
        let inserted = match self.mappings.entry(key) {
            Entry::Occupied(occ) => {
                let existing = *occ.get();
                if existing != frame_id {
                    return Err(Error::InvalidState);
                }
                return match frame.persisted_block_key() {
                    Some((file_id, block_id))
                        if file_id == key.file_id && block_id == key.block_id =>
                    {
                        Ok(())
                    }
                    _ => Err(Error::InvalidState),
                };
            }
            Entry::Vacant(vac) => {
                if let Some((file_id, block_id)) = frame.persisted_block_key() {
                    if file_id != key.file_id || block_id != key.block_id {
                        return Err(Error::InvalidState);
                    }
                    return Err(Error::InvalidState);
                }
                vac.insert(frame_id);
                true
            }
        };
        if inserted {
            frame.set_persisted_block_key(key.file_id, key.block_id);
            frame.set_dirty(false);
            frame.bump_generation();
            frame.set_kind(FrameKind::Hot);
            self.residency.remove_from_free(frame_id);
            self.residency.mark_resident(frame_id);
        }
        Ok(())
    }

    /// Invalidates a specific cache key and returns its old frame id.
    #[inline]
    pub fn invalidate_key(&self, key: &PersistedBlockKey) -> Option<PageID> {
        let frame_id = match self.mappings.remove(key) {
            Some((_, frame_id)) => frame_id,
            None => return None,
        };
        self.invalidate_frame_retry(frame_id, Some(*key));
        Some(frame_id)
    }

    /// Invalidates a specific cache key using strict GC-ordering preconditions.
    ///
    /// This method expects no holder on the target frame latch. If exclusive
    /// lock cannot be acquired immediately, it panics to surface protocol bugs.
    #[inline]
    pub fn invalidate_key_strict(&self, key: &PersistedBlockKey) -> Option<PageID> {
        let frame_id = match self.mappings.remove(key) {
            Some((_, frame_id)) => frame_id,
            None => return None,
        };
        self.invalidate_frame_strict(frame_id, Some(*key));
        Some(frame_id)
    }

    /// Invalidates one physical block from one file.
    #[inline]
    pub fn invalidate_file_block(
        &self,
        file_id: PersistedFileID,
        block_id: PageID,
    ) -> Option<PageID> {
        self.invalidate_key(&PersistedBlockKey::new(file_id, block_id))
    }

    /// Invalidates one physical block from one file using strict GC ordering.
    #[inline]
    pub fn invalidate_file_block_strict(
        &self,
        file_id: PersistedFileID,
        block_id: PageID,
    ) -> Option<PageID> {
        self.invalidate_key_strict(&PersistedBlockKey::new(file_id, block_id))
    }

    /// Invalidates all cache entries belonging to one file.
    ///
    /// Returns the number of invalidated mappings.
    #[inline]
    pub fn invalidate_file(&self, file_id: PersistedFileID) -> usize {
        let keys = self
            .mappings
            .iter()
            .filter_map(|entry| {
                if entry.key().file_id == file_id {
                    Some(*entry.key())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        let mut count = 0usize;
        for key in keys {
            if self.invalidate_key(&key).is_some() {
                count += 1;
            }
        }
        count
    }

    /// Invalidates all cache entries of one file using strict GC ordering.
    ///
    /// This method panics if any target frame is still latch-held.
    #[inline]
    pub fn invalidate_file_strict(&self, file_id: PersistedFileID) -> usize {
        let keys = self
            .mappings
            .iter()
            .filter_map(|entry| {
                if entry.key().file_id == file_id {
                    Some(*entry.key())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        let mut count = 0usize;
        for key in keys {
            if self.invalidate_key_strict(&key).is_some() {
                count += 1;
            }
        }
        count
    }

    #[inline]
    pub(crate) fn spawn_evictor_thread_guarded(pool: QuiescentGuard<Self>) -> JoinHandle<()> {
        // Acquire one direct guard from the stable owner, convert it once to
        // `SyncQuiescentGuard`, then clone that wrapper into the spawned
        // thread/runtime. This keeps worker startup on the guard side without
        // rebuilding a static-ownership compatibility layer.
        let pool = pool.into_sync();
        let runtime = ReadonlyRuntime {
            arena: pool.arena.arena_guard(pool.pool_guard()),
            mappings: Arc::clone(&pool.mappings),
            residency: Arc::clone(&pool.residency),
            _pool: Some(pool.clone()),
        };
        let policy = PressureDeltaClockPolicy::new(pool.eviction_arbiter, 1);
        let evictor = Evictor::new(runtime, policy, Arc::clone(&pool.shutdown_flag));
        evictor.start_thread("ReadonlyBufferPoolEvictor")
    }

    #[inline]
    fn signal_shutdown(&self) {
        self.shutdown_flag.store(true, Ordering::SeqCst);
        self.residency.free_ev.notify(usize::MAX);
        self.residency.evict_ev.notify(1);
    }

    #[inline]
    fn invalidate_frame_with_guard(
        &self,
        mut page_guard: PageExclusiveGuard<Page>,
        expected_key: Option<PersistedBlockKey>,
    ) {
        let frame = page_guard.bf_mut();
        if frame.kind() == FrameKind::Uninitialized {
            return;
        }
        if let Some(key) = expected_key
            && let Some((file_id, block_id)) = frame.persisted_block_key()
        {
            debug_assert_eq!(file_id, key.file_id);
            debug_assert_eq!(block_id, key.block_id);
        }
        frame.clear_persisted_block_key();
        frame.set_dirty(false);
        frame.bump_generation();
        frame.set_kind(FrameKind::Uninitialized);
        page_guard.page_mut().zero();
        // SAFETY: page pointer belongs to frame arena and has page-sized length.
        unsafe {
            let _ = madvise_dontneed(page_guard.page_mut() as *mut Page as *mut u8, PAGE_SIZE);
        }
        drop(page_guard);
    }

    #[inline]
    fn invalidate_frame_retry(&self, frame_id: PageID, expected_key: Option<PersistedBlockKey>) {
        loop {
            let guard = self.pool_guard();
            if let Some(page_guard) = self.try_lock_page_exclusive(&guard, frame_id) {
                self.invalidate_frame_with_guard(page_guard, expected_key);
                let _ = self.residency.move_resident_to_free(frame_id);
                return;
            }
            std::hint::spin_loop();
        }
    }

    #[inline]
    fn invalidate_frame_strict(&self, frame_id: PageID, expected_key: Option<PersistedBlockKey>) {
        let guard = self.pool_guard();
        let page_guard = self
            .try_lock_page_exclusive(&guard, frame_id)
            .unwrap_or_else(|| {
                panic!(
                    "strict invalidation lock acquisition failed: frame_id={}",
                    frame_id
                )
            });
        self.invalidate_frame_with_guard(page_guard, expected_key);
        let _ = self.residency.move_resident_to_free(frame_id);
    }

    #[inline]
    async fn get_page_internal<T: 'static>(
        &self,
        guard: &PoolGuard,
        frame_id: PageID,
        mode: LatchFallbackMode,
    ) -> Result<FacadePageGuard<T>> {
        self.validate_guard(guard);
        if frame_id as usize >= self.size {
            return Err(Error::InvalidArgument);
        }
        let keepalive = guard.clone();
        let bf = self.arena.frame_ptr(frame_id);
        let g = self
            .arena
            .frame(frame_id)
            .latch
            .optimistic_fallback_raw(mode)
            .await;
        Ok(FacadePageGuard::new(PageLatchGuard::new(keepalive, g), bf))
    }

    #[inline]
    fn validate_guarded_frame_key<T: 'static>(
        &self,
        guard: &FacadePageGuard<T>,
        expected_key: PersistedBlockKey,
    ) -> bool {
        let frame = guard.bf();
        frame.kind() != FrameKind::Uninitialized
            && frame.persisted_block_key() == Some((expected_key.file_id, expected_key.block_id))
    }

    #[inline]
    fn invalidate_stale_mapping_if_same_frame(&self, key: PersistedBlockKey, frame_id: PageID) {
        if let Entry::Occupied(occ) = self.mappings.entry(key)
            && *occ.get() == frame_id
        {
            occ.remove();
        }
    }
}

impl Drop for GlobalReadonlyBufferPool {
    #[inline]
    fn drop(&mut self) {
        self.signal_shutdown();
    }
}

// SAFETY: the global pool coordinates shared state with atomics, dashmaps, and
// page latches, while the arena keeps frame/page memory stable.
unsafe impl Send for GlobalReadonlyBufferPool {}
// SAFETY: shared references only access thread-safe coordination state and
// latch-protected frames.
unsafe impl Sync for GlobalReadonlyBufferPool {}

impl Component for DiskPoolWorkers {
    type Config = ();
    type Owned = DiskPoolWorkersOwned;
    type Access = ();

    const NAME: &'static str = "disk_pool_workers";

    #[inline]
    async fn build(
        _config: Self::Config,
        registry: &mut ComponentRegistry,
        _shelf: ShelfScope<'_, Self>,
    ) -> Result<()> {
        let pool = registry.dependency::<crate::DiskPool>()?;
        let sync_pool = pool.clone_inner().into_sync();
        let evict_thread =
            GlobalReadonlyBufferPool::spawn_evictor_thread_guarded(pool.into_inner());
        registry.register::<Self>(DiskPoolWorkersOwned {
            pool: sync_pool,
            evict_thread: Mutex::new(Some(evict_thread)),
        })
    }

    #[inline]
    fn access(_owner: &QuiescentBox<Self::Owned>) -> Self::Access {}

    #[inline]
    fn shutdown(component: &Self::Owned) {
        component.pool.signal_shutdown();
        if let Some(handle) = component.evict_thread.lock().take() {
            handle.join().unwrap();
        }
    }
}

#[derive(Clone, Copy)]
struct InflightLoadValidation {
    file_kind: PersistedFileKind,
    validator: ReadonlyPageValidator,
}

type ReadonlyReservedPage = PageReservationGuard<ReadonlyPageReservation>;

/// Reserved readonly-cache frame waiting to be filled by one miss load.
///
/// The reservation owns the exclusive frame guard and the global-pool context
/// needed to either publish the loaded frame into the mapping table or roll it
/// back to the free list.
pub(crate) struct ReadonlyPageReservation {
    pool: QuiescentGuard<GlobalReadonlyBufferPool>,
    key: PersistedBlockKey,
    frame_id: PageID,
    page_guard: PageExclusiveGuard<Page>,
}

impl ReadonlyPageReservation {
    #[inline]
    /// Reserves one free readonly frame and locks it exclusively for one load.
    ///
    /// The returned guard is not yet visible through the persisted-block map.
    async fn reserve_page(
        pool: &GlobalReadonlyBufferPool,
        _key: PersistedBlockKey,
        arena: ArenaGuard,
    ) -> Result<(PageID, PageExclusiveGuard<Page>)> {
        let frame_id = pool.reserve_frame_id_for_load().await?;
        let mut page_guard = match arena.try_lock_page_exclusive(frame_id) {
            Some(page_guard) => page_guard,
            None => {
                pool.residency.release_free(frame_id);
                return Err(Error::InvalidState);
            }
        };
        {
            let frame = page_guard.bf_mut();
            frame.clear_persisted_block_key();
            frame.set_dirty(false);
            frame.set_kind(FrameKind::Evicting);
        }
        Ok((frame_id, page_guard))
    }

    #[inline]
    /// Wraps one reserved frame in the shared reservation guard.
    fn from_reserved_page(
        pool: QuiescentGuard<GlobalReadonlyBufferPool>,
        key: PersistedBlockKey,
        frame_id: PageID,
        page_guard: PageExclusiveGuard<Page>,
    ) -> ReadonlyReservedPage {
        PageReservationGuard::new(ReadonlyPageReservation {
            pool,
            key,
            frame_id,
            page_guard,
        })
    }
}

impl PageReservation for ReadonlyPageReservation {
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
        let ReadonlyPageReservation {
            pool,
            key,
            frame_id,
            mut page_guard,
        } = self;
        {
            let frame = page_guard.bf_mut();
            frame.set_persisted_block_key(key.file_id, key.block_id);
            frame.set_dirty(false);
            frame.bump_generation();
            frame.set_kind(FrameKind::Hot);
        }
        match pool.mappings.entry(key) {
            Entry::Vacant(vac) => {
                vac.insert(frame_id);
            }
            Entry::Occupied(_) => return Err(Error::InvalidState),
        }
        drop(page_guard);
        pool.residency.mark_resident(frame_id);
        if pool.residency.no_free_frame() {
            pool.residency.evict_ev.notify(1);
        }
        Ok(frame_id)
    }

    #[inline]
    /// Returns the frame to the free list and clears all persisted-block state.
    fn rollback(self) {
        let ReadonlyPageReservation {
            pool,
            frame_id,
            mut page_guard,
            ..
        } = self;
        page_guard.page_mut().zero();
        let frame = page_guard.bf_mut();
        frame.clear_persisted_block_key();
        frame.set_dirty(false);
        frame.bump_generation();
        frame.set_kind(FrameKind::Uninitialized);
        drop(page_guard);
        pool.residency.release_free(frame_id);
    }
}

/// Worker-owned readonly cache miss load.
///
/// The submission keeps the reserved frame, the inflight dedupe cell, and the
/// IO operation together until the worker either publishes a successful load or
/// completes all waiters with an error. Exactly one terminal result is
/// published; drop only reports `InternalError` if no earlier terminal path
/// completed the miss.
pub(crate) struct ReadSubmission {
    key: PersistedBlockKey,
    operation: Operation,
    pool: QuiescentGuard<GlobalReadonlyBufferPool>,
    inflight: Arc<PageIOCompletion>,
    reservation: Option<ReadonlyReservedPage>,
    validation: Option<InflightLoadValidation>,
    completed: bool,
}

impl ReadSubmission {
    #[inline]
    /// Builds one readonly miss-load submission from an already reserved frame.
    fn new(
        backing: ReadonlyBackingFile,
        pool: QuiescentGuard<GlobalReadonlyBufferPool>,
        key: PersistedBlockKey,
        inflight: Arc<PageIOCompletion>,
        validation: Option<InflightLoadValidation>,
        mut reservation: ReadonlyReservedPage,
    ) -> Self {
        let ptr = reservation.page_mut() as *mut Page as *mut u8;
        pool.stats.add_queued_reads(1);
        ReadSubmission {
            key,
            operation: backing.read_operation(key, ptr, PAGE_SIZE),
            pool,
            inflight,
            reservation: Some(reservation),
            validation,
            completed: false,
        }
    }

    #[inline]
    /// Publishes the terminal miss result at most once.
    ///
    /// `ReadSubmission` also completes in `Drop` as a last-resort rollback
    /// path, so every explicit terminal path must mark the submission as
    /// completed to avoid a redundant second completion and inflight-map check.
    fn complete_inflight_once(&mut self, result: Result<PageID>) {
        if self.completed {
            return;
        }
        self.pool
            .complete_inflight_load(self.key, &self.inflight, result);
        self.completed = true;
    }

    #[inline]
    /// Fails the miss before worker completion and wakes all joined waiters.
    pub(crate) fn fail(mut self, err: Error) {
        drop(self.reservation.take());
        self.pool.stats.add_completed_reads(1);
        self.pool.stats.add_read_errors(1);
        self.complete_inflight_once(Err(err));
    }

    #[inline]
    pub(crate) fn record_running(&self) {
        self.pool.stats.add_running_reads(1);
    }

    #[inline]
    /// Finalizes one worker-side readonly read completion.
    ///
    /// Exact-page reads publish the reserved frame; short reads and IO errors
    /// drop the reservation so rollback returns the frame to the free list.
    pub(crate) fn complete(mut self, res: std::io::Result<usize>) -> AIOKind {
        let result = match res {
            Ok(len) if len == PAGE_SIZE => {
                if let Some(validation) = self.validation {
                    let reservation = self.reservation.as_ref().expect(
                        "readonly read submission must still own its page reservation before publish",
                    );
                    if let Err(err) = (validation.validator)(
                        reservation.page(),
                        validation.file_kind,
                        self.key.block_id,
                    ) {
                        drop(self.reservation.take());
                        self.complete_inflight_once(Err(err));
                        return AIOKind::Read;
                    }
                }
                let reservation = self.reservation.take().expect(
                    "readonly read submission must still own its page reservation before publish",
                );
                match reservation.publish() {
                    Ok(page_id) => Ok(page_id),
                    Err(err) => Err(err),
                }
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
        self.pool.stats.add_completed_reads(1);
        if result.is_err() {
            self.pool.stats.add_read_errors(1);
        }
        self.complete_inflight_once(result);
        AIOKind::Read
    }
}

impl IOSubmission for ReadSubmission {
    type Key = PersistedBlockKey;

    #[inline]
    fn key(&self) -> &Self::Key {
        &self.key
    }

    #[inline]
    fn operation(&mut self) -> &mut Operation {
        &mut self.operation
    }
}

impl Drop for ReadSubmission {
    #[inline]
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        drop(self.reservation.take());
        self.pool.stats.add_completed_reads(1);
        self.pool.stats.add_read_errors(1);
        self.complete_inflight_once(Err(Error::InternalError));
    }
}

struct ReadonlyResidency {
    capacity: usize,
    set: Mutex<BTreeSet<PageID>>,
    free: Mutex<Vec<PageID>>,
    alloc_failures: FailureRateTracker,
    free_ev: Event,
    evict_ev: Event,
}

impl ReadonlyResidency {
    #[inline]
    fn new(capacity: usize, eviction_arbiter: EvictionArbiter) -> Self {
        let mut free = Vec::with_capacity(capacity);
        for frame_id in 0..capacity {
            free.push(frame_id as PageID);
        }
        ReadonlyResidency {
            capacity,
            set: Mutex::new(BTreeSet::new()),
            free: Mutex::new(free),
            alloc_failures: FailureRateTracker::new(eviction_arbiter.failure_window()),
            free_ev: Event::new(),
            evict_ev: Event::new(),
        }
    }

    #[inline]
    fn try_reserve_frame(&self) -> Option<PageID> {
        let mut g = self.free.lock();
        g.pop()
    }

    #[inline]
    fn release_free(&self, frame_id: PageID) {
        let mut g = self.free.lock();
        g.push(frame_id);
        drop(g);
        self.free_ev.notify(1);
    }

    #[inline]
    fn remove_from_free(&self, frame_id: PageID) -> bool {
        let mut g = self.free.lock();
        if let Some(idx) = g.iter().position(|id| *id == frame_id) {
            g.swap_remove(idx);
            return true;
        }
        false
    }

    #[inline]
    fn no_free_frame(&self) -> bool {
        let g = self.free.lock();
        g.is_empty()
    }

    #[inline]
    fn mark_resident(&self, frame_id: PageID) {
        let mut g = self.set.lock();
        g.insert(frame_id);
    }

    #[inline]
    fn move_resident_to_free(&self, frame_id: PageID) -> bool {
        let removed = {
            let mut g = self.set.lock();
            g.remove(&frame_id)
        };
        if removed {
            self.release_free(frame_id);
        }
        removed
    }

    #[inline]
    fn resident_len(&self) -> usize {
        let g = self.set.lock();
        g.len()
    }

    #[inline]
    fn capacity(&self) -> usize {
        self.capacity
    }

    #[inline]
    fn record_alloc_success(&self) {
        self.alloc_failures.record_success();
    }

    #[inline]
    fn record_alloc_failure(&self) {
        self.alloc_failures.record_failure();
    }

    #[inline]
    fn alloc_failure_rate(&self) -> f64 {
        self.alloc_failures.failure_rate()
    }

    #[inline]
    fn collect_batch_ids(
        &self,
        hand: ClockHand,
        limit: usize,
        out: &mut Vec<PageID>,
    ) -> Option<ClockHand> {
        let g = self.set.lock();
        clock_collect_batch(&g, hand, limit, out)
    }
}

struct ReadonlyRuntime {
    arena: ArenaGuard,
    mappings: Arc<DashMap<PersistedBlockKey, PageID>>,
    residency: Arc<ReadonlyResidency>,
    _pool: Option<SyncQuiescentGuard<GlobalReadonlyBufferPool>>,
}

impl ReadonlyRuntime {
    #[inline]
    fn drop_resident_page(&self, mut page_guard: PageExclusiveGuard<Page>) {
        let frame_id = page_guard.page_id();
        let frame = page_guard.bf_mut();
        if frame.kind() != FrameKind::Evicting {
            return;
        }

        if let Some((file_id, block_id)) = frame.persisted_block_key() {
            let key = PersistedBlockKey::new(file_id, block_id);
            if let Some((_, mapped_frame_id)) = self.mappings.remove(&key) {
                debug_assert_eq!(mapped_frame_id, frame_id);
            }
        }

        frame.clear_persisted_block_key();
        frame.set_dirty(false);
        frame.bump_generation();
        let prev = frame.compare_exchange_kind(FrameKind::Evicting, FrameKind::Uninitialized);
        debug_assert_eq!(prev, FrameKind::Evicting);
        page_guard.page_mut().zero();
        // SAFETY: page pointer belongs to frame arena and has page-sized length.
        unsafe {
            let _ = madvise_dontneed(page_guard.page_mut() as *mut Page as *mut u8, PAGE_SIZE);
        }
        drop(page_guard);
        let _ = self.residency.move_resident_to_free(frame_id);
    }
}

impl EvictionRuntime for ReadonlyRuntime {
    #[inline]
    fn resident_count(&self) -> usize {
        self.residency.resident_len()
    }

    #[inline]
    fn capacity(&self) -> usize {
        self.residency.capacity()
    }

    #[inline]
    fn inflight_evicts(&self) -> usize {
        0
    }

    #[inline]
    fn allocation_failure_rate(&self) -> f64 {
        self.residency.alloc_failure_rate()
    }

    #[inline]
    fn work_listener(&self) -> EventListener {
        self.residency.evict_ev.listen()
    }

    #[inline]
    fn notify_progress(&self) {
        // no-op for readonly drop-only path.
    }

    #[inline]
    fn collect_batch_ids(
        &self,
        hand: ClockHand,
        limit: usize,
        out: &mut Vec<PageID>,
    ) -> Option<ClockHand> {
        self.residency.collect_batch_ids(hand, limit, out)
    }

    #[inline]
    fn try_mark_evicting(&self, page_id: PageID) -> Option<PageExclusiveGuard<Page>> {
        clock_sweep_candidate(&self.arena, page_id)
    }

    #[inline]
    fn execute(&self, pages: Vec<PageExclusiveGuard<Page>>) -> Option<EventListener> {
        for page_guard in pages {
            self.drop_resident_page(page_guard);
        }
        None
    }
}

/// Per-file readonly wrapper implementing the `BufferPool` contract.
///
/// This wrapper translates file-local `PageID` into global physical cache keys
/// and delegates to `GlobalReadonlyBufferPool`.
#[derive(Clone)]
pub struct ReadonlyBufferPool {
    file_id: PersistedFileID,
    file_kind: PersistedFileKind,
    backing: ReadonlyBackingFile,
    global: QuiescentGuard<GlobalReadonlyBufferPool>,
}

impl ReadonlyBufferPool {
    /// Creates a per-file readonly pool wrapper.
    #[inline]
    pub(crate) fn new<O>(
        file_id: PersistedFileID,
        file_kind: PersistedFileKind,
        backing: O,
        global: QuiescentGuard<GlobalReadonlyBufferPool>,
    ) -> Self
    where
        O: Into<ReadonlyBackingFile>,
    {
        ReadonlyBufferPool {
            file_id,
            file_kind,
            backing: backing.into(),
            global,
        }
    }

    /// Creates a readonly pool backed by one table file.
    #[inline]
    pub fn from_table_file(
        file_id: PersistedFileID,
        file_kind: PersistedFileKind,
        table_file: Arc<TableFile>,
        global: QuiescentGuard<GlobalReadonlyBufferPool>,
    ) -> Self {
        Self::new(file_id, file_kind, table_file, global)
    }

    /// Creates a readonly pool backed by one multi-table file.
    #[inline]
    pub fn from_multi_table_file(
        file_id: PersistedFileID,
        file_kind: PersistedFileKind,
        mtb: Arc<MultiTableFile>,
        global: QuiescentGuard<GlobalReadonlyBufferPool>,
    ) -> Self {
        Self::new(file_id, file_kind, mtb, global)
    }

    /// Returns which persisted file format this pool reads from.
    #[inline]
    pub fn persisted_file_kind(&self) -> PersistedFileKind {
        self.file_kind
    }

    /// Returns one snapshot of the shared readonly-pool access and load counters.
    #[inline]
    pub fn global_stats(&self) -> BufferPoolStats {
        self.global.stats()
    }

    #[inline]
    fn block_key(&self, block_id: PageID) -> PersistedBlockKey {
        PersistedBlockKey::new(self.file_id, block_id)
    }

    #[inline]
    async fn join_or_start_inflight_load(
        &self,
        key: PersistedBlockKey,
        validation: Option<InflightLoadValidation>,
    ) -> Arc<PageIOCompletion> {
        let global = &self.global;
        global.stats.record_cache_miss();
        match global.inflight_loads.entry(key) {
            Entry::Vacant(vac) => {
                let inflight = Arc::new(PageIOCompletion::new());
                vac.insert(Arc::clone(&inflight));
                let task_arena = global.arena.arena_guard(global.pool_guard());
                match ReadonlyPageReservation::reserve_page(global, key, task_arena).await {
                    Ok((frame_id, page_guard)) => {
                        let reservation = ReadonlyPageReservation::from_reserved_page(
                            self.global.clone(),
                            key,
                            frame_id,
                            page_guard,
                        );
                        let req = ReadSubmission::new(
                            self.backing.clone(),
                            self.global.clone(),
                            key,
                            Arc::clone(&inflight),
                            validation,
                            reservation,
                        );
                        let _ = self.backing.queue_read(req).await;
                    }
                    Err(err) => {
                        global.complete_inflight_load(key, &inflight, Err(err));
                    }
                }
                inflight
            }
            Entry::Occupied(occ) => {
                global.stats.record_miss_join();
                Arc::clone(occ.get())
            }
        }
    }

    #[inline]
    async fn get_or_load_frame_id(&self, key: PersistedBlockKey) -> Result<(PageID, bool)> {
        if let Some(frame_id) = self.global.try_get_frame_id(&key) {
            return Ok((frame_id, true));
        }
        let inflight = self.join_or_start_inflight_load(key, None).await;
        if let Some(frame_id) = self.global.try_get_frame_id(&key) {
            return Ok((frame_id, false));
        }
        inflight
            .wait_result()
            .await
            .map(|frame_id| (frame_id, false))
    }

    #[inline]
    async fn get_or_load_frame_id_validated(
        &self,
        key: PersistedBlockKey,
        validator: ReadonlyPageValidator,
    ) -> Result<(PageID, bool)> {
        if let Some(frame_id) = self.global.try_get_frame_id(&key) {
            return Ok((frame_id, true));
        }
        let inflight = self
            .join_or_start_inflight_load(
                key,
                Some(InflightLoadValidation {
                    file_kind: self.file_kind,
                    validator,
                }),
            )
            .await;
        if let Some(frame_id) = self.global.try_get_frame_id(&key) {
            return Ok((frame_id, false));
        }
        inflight
            .wait_result()
            .await
            .map(|frame_id| (frame_id, false))
    }

    #[inline]
    async fn get_page_facade<T: BufferPage>(
        &self,
        guard: &PoolGuard,
        page_id: PageID,
        mode: LatchFallbackMode,
    ) -> Result<FacadePageGuard<T>> {
        let key = self.block_key(page_id);
        let global = &self.global;
        global.validate_guard(guard);
        loop {
            let (frame_id, resident_hit) = self.get_or_load_frame_id(key).await?;
            let page_guard = global.get_page_internal(guard, frame_id, mode).await?;
            if global.validate_guarded_frame_key(&page_guard, key) {
                if resident_hit {
                    global.stats.record_cache_hit();
                }
                return Ok(page_guard);
            }
            global.invalidate_stale_mapping_if_same_frame(key, frame_id);
        }
    }

    /// Invalidates one block for this file from the global readonly cache.
    #[inline]
    pub fn invalidate_block_id(&self, block_id: PageID) -> Option<PageID> {
        self.global.invalidate_file_block(self.file_id, block_id)
    }

    /// Invalidates one block for this file with strict GC ordering preconditions.
    #[inline]
    pub fn invalidate_block_id_strict(&self, block_id: PageID) -> Option<PageID> {
        self.global
            .invalidate_file_block_strict(self.file_id, block_id)
    }

    /// Returns a shared guard for one persisted page after validating its page-kind contract.
    ///
    /// On cache miss, validation runs before the new frame becomes resident.
    /// On cached hits, validation is re-run against the resident bytes and a
    /// failed validation invalidates the mapping before returning the error.
    #[inline]
    pub(crate) async fn get_validated_page_shared(
        &self,
        page_id: PageID,
        validator: ReadonlyPageValidator,
    ) -> Result<PageSharedGuard<Page>> {
        let key = self.block_key(page_id);
        let global = &self.global;
        loop {
            let (frame_id, resident_hit) =
                self.get_or_load_frame_id_validated(key, validator).await?;
            let guard = global.pool_guard();
            let guard = global
                .get_page_internal::<Page>(&guard, frame_id, LatchFallbackMode::Shared)
                .await?;
            if !global.validate_guarded_frame_key(&guard, key) {
                global.invalidate_stale_mapping_if_same_frame(key, frame_id);
                continue;
            }
            if let Some(shared) = guard.lock_shared_async().await {
                if let Err(err) = validator(shared.page(), self.file_kind, page_id) {
                    drop(shared);
                    let _ = self.invalidate_block_id(page_id);
                    return Err(err);
                }
                if resident_hit {
                    global.stats.record_cache_hit();
                }
                return Ok(shared);
            }
        }
    }
}

impl BufferPool for ReadonlyBufferPool {
    #[inline]
    fn capacity(&self) -> usize {
        self.global.capacity()
    }

    #[inline]
    fn allocated(&self) -> usize {
        self.global.allocated()
    }

    #[inline]
    fn pool_guard(&self) -> PoolGuard {
        self.global.pool_guard()
    }

    #[inline]
    async fn allocate_page<T: BufferPage>(&self, _guard: &PoolGuard) -> PageExclusiveGuard<T> {
        panic!("readonly buffer pool does not support page allocation")
    }

    #[inline]
    async fn allocate_page_at<T: BufferPage>(
        &self,
        _guard: &PoolGuard,
        _page_id: PageID,
    ) -> Result<PageExclusiveGuard<T>> {
        panic!("readonly buffer pool does not support page allocation")
    }

    #[inline]
    async fn get_page<T: BufferPage>(
        &self,
        guard: &PoolGuard,
        page_id: PageID,
        mode: LatchFallbackMode,
    ) -> Result<FacadePageGuard<T>> {
        self.get_page_facade(guard, page_id, mode).await
    }

    #[inline]
    async fn get_page_versioned<T: BufferPage>(
        &self,
        guard: &PoolGuard,
        _id: VersionedPageID,
        _mode: LatchFallbackMode,
    ) -> Result<Option<FacadePageGuard<T>>> {
        self.global.validate_guard(guard);
        Ok(None)
    }

    #[inline]
    fn deallocate_page<T: BufferPage>(&self, _g: PageExclusiveGuard<T>) {
        panic!("readonly buffer pool does not support page deallocation")
    }

    #[inline]
    async fn get_child_page<T: BufferPage>(
        &self,
        guard: &PoolGuard,
        p_guard: &FacadePageGuard<T>,
        page_id: PageID,
        mode: LatchFallbackMode,
    ) -> Result<Validation<FacadePageGuard<T>>> {
        let g = self.get_page(guard, page_id, mode).await?;
        if p_guard.validate_bool() {
            return Ok(Valid(g));
        }
        if g.is_exclusive() {
            g.rollback_exclusive_version_change();
        }
        Ok(Validation::Invalid)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::buffer::guard::{FacadePageGuard, PageGuard};
    use crate::buffer::page::Page;
    use crate::catalog::TableID;
    use crate::catalog::{ColumnAttributes, ColumnSpec, TableMetadata, USER_OBJ_ID_START};
    use crate::error::{PersistedPageCorruptionCause, PersistedPageKind};
    use crate::file::build_test_fs;
    use crate::file::cow_file::COW_FILE_PAGE_SIZE;
    use crate::file::page_integrity::{
        COLUMN_BLOCK_INDEX_PAGE_SPEC, COLUMN_DELETION_BLOB_PAGE_SPEC, LWC_PAGE_SPEC,
        max_payload_len, write_page_checksum, write_page_header,
    };
    use crate::file::table_file::TableFile;
    use crate::index::{
        COLUMN_BLOCK_HEADER_SIZE, COLUMN_BLOCK_NODE_PAYLOAD_SIZE,
        COLUMN_DELETION_BLOB_PAGE_HEADER_SIZE, ColumnBlockNodeHeader, validate_persisted_blob_page,
        validate_persisted_column_block_index_page,
    };
    use crate::io::{
        AIOBuf, AIOKind, DirectBuf, StorageBackendOp, StorageBackendTestHook,
        set_storage_backend_test_hook,
    };
    use crate::lwc::{LWC_PAGE_PAYLOAD_SIZE, LwcPage, LwcPageHeader, validate_persisted_lwc_page};
    use crate::ptr::UnsafePtr;
    use crate::quiescent::QuiescentBox;
    use crate::thread::join_worker;
    use crate::value::ValKind;
    use std::ops::Deref;
    use std::os::fd::RawFd;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, LazyLock};
    use std::thread;
    use std::time::Duration;

    const TEST_WAIT_RETRIES: usize = 100;
    const TEST_WAIT_INTERVAL: Duration = Duration::from_millis(10);

    async fn wait_for<F>(mut predicate: F)
    where
        F: FnMut() -> bool,
    {
        for _ in 0..TEST_WAIT_RETRIES {
            if predicate() {
                return;
            }
            smol::Timer::after(TEST_WAIT_INTERVAL).await;
        }
        panic!("condition was not satisfied before timeout");
    }

    struct StartedGlobalReadonlyBufferPool {
        owner: QuiescentBox<GlobalReadonlyBufferPool>,
        evict_thread: Mutex<Option<JoinHandle<()>>>,
    }

    impl StartedGlobalReadonlyBufferPool {
        #[inline]
        fn new(pool_size: usize) -> Self {
            let owner = QuiescentBox::new(
                GlobalReadonlyBufferPool::with_capacity(PoolRole::Disk, pool_size).unwrap(),
            );
            let evict_thread =
                GlobalReadonlyBufferPool::spawn_evictor_thread_guarded(owner.guard());
            Self {
                owner,
                evict_thread: Mutex::new(Some(evict_thread)),
            }
        }

        #[inline]
        fn guard(&self) -> crate::quiescent::QuiescentGuard<GlobalReadonlyBufferPool> {
            self.owner.guard()
        }

        #[inline]
        fn shutdown(&self) {
            self.owner.signal_shutdown();
            join_worker(&self.evict_thread);
        }
    }

    impl Deref for StartedGlobalReadonlyBufferPool {
        type Target = GlobalReadonlyBufferPool;

        #[inline]
        fn deref(&self) -> &Self::Target {
            &self.owner
        }
    }

    impl Drop for StartedGlobalReadonlyBufferPool {
        #[inline]
        fn drop(&mut self) {
            self.shutdown();
        }
    }

    fn frame_page_bytes(cap: usize) -> usize {
        cap.max(MIN_READONLY_POOL_PAGES) * (mem::size_of::<BufferFrame>() + mem::size_of::<Page>())
    }

    fn owned_global_pool(pool_size: usize) -> QuiescentBox<GlobalReadonlyBufferPool> {
        QuiescentBox::new(
            GlobalReadonlyBufferPool::with_capacity(PoolRole::Disk, pool_size).unwrap(),
        )
    }

    fn started_global_pool(pool_size: usize) -> StartedGlobalReadonlyBufferPool {
        StartedGlobalReadonlyBufferPool::new(pool_size)
    }

    /// Test-only owner wrapper for a started global readonly pool.
    ///
    /// Local started pools must shut down their evictor thread before owner
    /// drop, otherwise `QuiescentBox` waits forever on the worker keepalive
    /// guard. This scope centralizes that shutdown ordering for other test
    /// modules that need a reusable readonly cache.
    pub(crate) struct GlobalReadOnlyPoolScope {
        owner: StartedGlobalReadonlyBufferPool,
    }

    impl Deref for GlobalReadOnlyPoolScope {
        type Target = GlobalReadonlyBufferPool;

        #[inline]
        fn deref(&self) -> &Self::Target {
            &self.owner
        }
    }

    impl Drop for GlobalReadOnlyPoolScope {
        #[inline]
        fn drop(&mut self) {
            self.owner.shutdown();
        }
    }

    impl GlobalReadOnlyPoolScope {
        #[inline]
        pub(crate) fn guard(&self) -> crate::quiescent::QuiescentGuard<GlobalReadonlyBufferPool> {
            self.owner.guard()
        }
    }

    #[inline]
    pub(crate) fn global_readonly_pool_scope(pool_size: usize) -> GlobalReadOnlyPoolScope {
        GlobalReadOnlyPoolScope {
            owner: started_global_pool(pool_size),
        }
    }

    #[inline]
    pub(crate) fn table_readonly_pool(
        scope: &GlobalReadOnlyPoolScope,
        table_id: TableID,
        table_file: &Arc<TableFile>,
    ) -> ReadonlyBufferPool {
        ReadonlyBufferPool::new(
            table_id,
            PersistedFileKind::TableFile,
            Arc::clone(table_file),
            scope.owner.guard(),
        )
    }

    #[allow(dead_code)]
    trait GlobalReadonlyPoolHandle {
        fn guard(&self) -> crate::quiescent::QuiescentGuard<GlobalReadonlyBufferPool>;
    }

    impl GlobalReadonlyPoolHandle for QuiescentBox<GlobalReadonlyBufferPool> {
        fn guard(&self) -> crate::quiescent::QuiescentGuard<GlobalReadonlyBufferPool> {
            QuiescentBox::guard(self)
        }
    }

    impl GlobalReadonlyPoolHandle for StartedGlobalReadonlyBufferPool {
        fn guard(&self) -> crate::quiescent::QuiescentGuard<GlobalReadonlyBufferPool> {
            StartedGlobalReadonlyBufferPool::guard(self)
        }
    }

    fn owned_readonly_pool<O, G>(
        file_id: PersistedFileID,
        file_kind: PersistedFileKind,
        backing: O,
        global: &G,
    ) -> QuiescentBox<ReadonlyBufferPool>
    where
        O: Into<ReadonlyBackingFile>,
        G: GlobalReadonlyPoolHandle,
    {
        QuiescentBox::new(ReadonlyBufferPool::new(
            file_id,
            file_kind,
            backing,
            global.guard(),
        ))
    }

    #[test]
    fn test_global_readonly_pool_shutdown_is_idempotent_before_worker_start() {
        let pool =
            GlobalReadonlyBufferPool::with_capacity(PoolRole::Disk, frame_page_bytes(2)).unwrap();

        pool.signal_shutdown();
        pool.signal_shutdown();
    }

    fn make_metadata() -> Arc<TableMetadata> {
        Arc::new(TableMetadata::new(
            vec![ColumnSpec::new(
                "c0",
                ValKind::U32,
                ColumnAttributes::empty(),
            )],
            vec![],
        ))
    }

    async fn write_payload(table_file: &Arc<TableFile>, page_id: PageID, payload: &[u8]) {
        let mut buf = DirectBuf::zeroed(COW_FILE_PAGE_SIZE);
        let bytes = buf.as_bytes_mut();
        bytes[..payload.len()].copy_from_slice(payload);
        table_file.write_page(page_id, buf).await.unwrap();
    }

    async fn write_page_bytes(table_file: &Arc<TableFile>, page_id: PageID, bytes: &[u8]) {
        let mut buf = DirectBuf::zeroed(COW_FILE_PAGE_SIZE);
        buf.as_bytes_mut().copy_from_slice(bytes);
        table_file.write_page(page_id, buf).await.unwrap();
    }

    #[test]
    fn test_readonly_pool_global_stats_track_single_miss_then_warm_hit() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let table_file = fs.create_table_file(120, make_metadata(), false).unwrap();
            let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
            drop(old_root);
            write_payload(&table_file, 0, b"readonly-stats").await;

            let scope = global_readonly_pool_scope(frame_page_bytes(4));
            let pool = table_readonly_pool(&scope, 120, &table_file);
            let pool_guard = pool.pool_guard();

            let cold_start = pool.global_stats();
            let cold_guard = pool
                .get_page::<Page>(&pool_guard, 0, crate::latch::LatchFallbackMode::Shared)
                .await
                .expect("readonly cold read failed in test")
                .lock_shared_async()
                .await
                .unwrap();
            assert_eq!(&cold_guard.page()[..14], b"readonly-stats");
            drop(cold_guard);

            let cold_delta = pool.global_stats().delta_since(cold_start);
            assert_eq!(cold_delta.cache_hits, 0);
            assert_eq!(cold_delta.cache_misses, 1);
            assert_eq!(cold_delta.miss_joins, 0);
            assert_eq!(cold_delta.queued_reads, 1);
            assert_eq!(cold_delta.running_reads, 1);
            assert_eq!(cold_delta.completed_reads, 1);
            assert_eq!(cold_delta.read_errors, 0);

            let warm_start = pool.global_stats();
            let warm_guard = pool
                .get_page::<Page>(&pool_guard, 0, crate::latch::LatchFallbackMode::Shared)
                .await
                .expect("readonly warm read failed in test")
                .lock_shared_async()
                .await
                .unwrap();
            assert_eq!(&warm_guard.page()[..14], b"readonly-stats");
            drop(warm_guard);

            let warm_delta = pool.global_stats().delta_since(warm_start);
            assert_eq!(warm_delta.cache_hits, 1);
            assert_eq!(warm_delta.cache_misses, 0);
            assert_eq!(warm_delta.queued_reads, 0);
            assert_eq!(warm_delta.running_reads, 0);
            assert_eq!(warm_delta.completed_reads, 0);
        });
    }

    #[test]
    fn test_readonly_pool_global_stats_are_shared_across_file_wrappers() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let table_file_a = fs.create_table_file(121, make_metadata(), false).unwrap();
            let (table_file_a, old_root_a) = table_file_a.commit(1, false).await.unwrap();
            drop(old_root_a);
            write_payload(&table_file_a, 0, b"readonly-shared-a").await;

            let table_file_b = fs.create_table_file(122, make_metadata(), false).unwrap();
            let (table_file_b, old_root_b) = table_file_b.commit(1, false).await.unwrap();
            drop(old_root_b);
            write_payload(&table_file_b, 0, b"readonly-shared-b").await;

            let scope = global_readonly_pool_scope(frame_page_bytes(4));
            let pool_a = table_readonly_pool(&scope, 121, &table_file_a);
            let pool_b = table_readonly_pool(&scope, 122, &table_file_b);
            let pool_guard_a = pool_a.pool_guard();

            let start_a = pool_a.global_stats();
            let start_b = pool_b.global_stats();
            assert_eq!(start_a, start_b);

            let guard_a = pool_a
                .get_page::<Page>(&pool_guard_a, 0, crate::latch::LatchFallbackMode::Shared)
                .await
                .expect("readonly shared global read failed in test")
                .lock_shared_async()
                .await
                .unwrap();
            assert_eq!(&guard_a.page()[..17], b"readonly-shared-a");
            drop(guard_a);

            let delta_a = pool_a.global_stats().delta_since(start_a);
            let delta_b = pool_b.global_stats().delta_since(start_b);
            assert_eq!(delta_a, delta_b);
            assert_eq!(delta_a.cache_hits, 0);
            assert_eq!(delta_a.cache_misses, 1);
            assert_eq!(delta_a.miss_joins, 0);
            assert_eq!(delta_a.queued_reads, 1);
            assert_eq!(delta_a.running_reads, 1);
            assert_eq!(delta_a.completed_reads, 1);
            assert_eq!(delta_a.read_errors, 0);
        });
    }

    fn build_valid_persisted_lwc_page() -> Vec<u8> {
        let mut buf = vec![0u8; COW_FILE_PAGE_SIZE];
        let payload_start = write_page_header(&mut buf, LWC_PAGE_SPEC);
        let payload_end = payload_start + LWC_PAGE_PAYLOAD_SIZE;
        let page = LwcPage::try_from_bytes_mut(&mut buf[payload_start..payload_end]).unwrap();
        page.header = LwcPageHeader::new(1, 1, 0, 0, 0);
        write_page_checksum(&mut buf);
        buf
    }

    fn build_valid_persisted_column_block_page() -> Vec<u8> {
        let mut buf = vec![0u8; COW_FILE_PAGE_SIZE];
        let payload_start = write_page_header(&mut buf, COLUMN_BLOCK_INDEX_PAGE_SPEC);
        let payload_end = payload_start + COLUMN_BLOCK_NODE_PAYLOAD_SIZE;
        let header = ColumnBlockNodeHeader {
            height: 0,
            count: 0,
            start_row_id: 0,
            create_ts: 1,
        };
        let header_bytes = bytemuck::bytes_of(&header);
        buf[payload_start..payload_start + COLUMN_BLOCK_HEADER_SIZE].copy_from_slice(header_bytes);
        buf[payload_start + COLUMN_BLOCK_HEADER_SIZE..payload_end].fill(0);
        write_page_checksum(&mut buf);
        buf
    }

    fn build_valid_persisted_blob_page() -> Vec<u8> {
        let mut buf = vec![0u8; COW_FILE_PAGE_SIZE];
        let payload_start = write_page_header(&mut buf, COLUMN_DELETION_BLOB_PAGE_SPEC);
        let payload_end = payload_start + max_payload_len(COW_FILE_PAGE_SIZE);
        let payload = &mut buf[payload_start..payload_end];
        payload[..COLUMN_DELETION_BLOB_PAGE_HEADER_SIZE].fill(0);
        payload[8..10].copy_from_slice(&(1u16).to_le_bytes());
        payload[COLUMN_DELETION_BLOB_PAGE_HEADER_SIZE] = 7;
        write_page_checksum(&mut buf);
        buf
    }

    /// Serializes ownership of the process-global storage-backend test hook.
    ///
    /// The hook itself is stored in `io` as one process-global
    /// slot, so readonly tests must not interleave install/restore across
    /// different test cases.
    static STORAGE_BACKEND_TEST_HOOK_LOCK: LazyLock<parking_lot::Mutex<()>> =
        LazyLock::new(|| parking_lot::Mutex::new(()));

    struct InstalledStorageBackendTestHook {
        previous: Option<Arc<dyn StorageBackendTestHook>>,
        guard: Option<parking_lot::MutexGuard<'static, ()>>,
    }

    impl Drop for InstalledStorageBackendTestHook {
        #[inline]
        fn drop(&mut self) {
            let _ = set_storage_backend_test_hook(self.previous.take());
            drop(self.guard.take());
        }
    }

    fn install_storage_backend_test_hook(
        hook: Arc<dyn StorageBackendTestHook>,
    ) -> InstalledStorageBackendTestHook {
        let guard = STORAGE_BACKEND_TEST_HOOK_LOCK.lock();
        InstalledStorageBackendTestHook {
            previous: set_storage_backend_test_hook(Some(hook)),
            guard: Some(guard),
        }
    }

    #[derive(Clone)]
    enum ControlledReadResult {
        Success,
        Errno(i32),
    }

    // Test-only backend hook that controls one real persisted-page read by
    // `(fd, offset)`. Tests use this to wait until a miss read has been
    // submitted, then cancel or attach additional waiters before releasing the
    // completion.
    #[derive(Clone)]
    struct ControlledReadHook {
        inner: Arc<ControlledReadHookInner>,
    }

    struct ControlledReadHookInner {
        fd: RawFd,
        offset: usize,
        result: ControlledReadResult,
        calls: AtomicUsize,
        start_ev: Event,
        released: AtomicBool,
        release_ev: Event,
    }

    impl ControlledReadHook {
        fn for_page(fd: RawFd, page_id: PageID) -> Self {
            ControlledReadHook {
                inner: Arc::new(ControlledReadHookInner {
                    fd,
                    offset: page_id as usize * PAGE_SIZE,
                    result: ControlledReadResult::Success,
                    calls: AtomicUsize::new(0),
                    start_ev: Event::new(),
                    released: AtomicBool::new(false),
                    release_ev: Event::new(),
                }),
            }
        }

        fn with_errno(fd: RawFd, page_id: PageID, errno: i32) -> Self {
            ControlledReadHook {
                inner: Arc::new(ControlledReadHookInner {
                    fd,
                    offset: page_id as usize * PAGE_SIZE,
                    result: ControlledReadResult::Errno(errno),
                    calls: AtomicUsize::new(0),
                    start_ev: Event::new(),
                    released: AtomicBool::new(false),
                    release_ev: Event::new(),
                }),
            }
        }

        fn call_count(&self) -> usize {
            self.inner.calls.load(Ordering::SeqCst)
        }

        // Unblocks the in-flight read so the test can deterministically decide
        // when the mock IO completes.
        fn release(&self) {
            self.inner.released.store(true, Ordering::SeqCst);
            self.inner.release_ev.notify(usize::MAX);
        }

        // Waits until the mock source has observed the requested number of read
        // attempts. Tests use this to synchronize on "IO started" before
        // canceling the initiating future or attaching followers.
        async fn wait_started(&self, expected_calls: usize) {
            loop {
                if self.call_count() >= expected_calls {
                    return;
                }
                listener!(self.inner.start_ev => listener);
                if self.call_count() >= expected_calls {
                    return;
                }
                listener.await;
            }
        }

        fn matches(&self, op: StorageBackendOp) -> bool {
            op.kind() == AIOKind::Read
                && op.fd() == self.inner.fd
                && op.offset() == self.inner.offset
        }
    }

    impl StorageBackendTestHook for ControlledReadHook {
        fn on_submit(&self, op: StorageBackendOp) {
            if self.matches(op) {
                self.inner.calls.fetch_add(1, Ordering::SeqCst);
                self.inner.start_ev.notify(usize::MAX);
            }
        }

        fn on_complete(&self, op: StorageBackendOp, res: &mut std::io::Result<usize>) {
            if !self.matches(op) {
                return;
            }
            loop {
                if self.inner.released.load(Ordering::SeqCst) {
                    break;
                }
                listener!(self.inner.release_ev => listener);
                if self.inner.released.load(Ordering::SeqCst) {
                    break;
                }
                smol::block_on(listener);
            }
            if let ControlledReadResult::Errno(errno) = self.inner.result {
                *res = Err(std::io::Error::from_raw_os_error(errno));
            }
        }
    }

    #[test]
    fn test_global_readonly_mapping_and_invalidation() {
        let global = owned_global_pool(64 * 1024 * 1024);
        let global_guard = (*global).pool_guard();
        let key = PersistedBlockKey::new(7, 11);

        assert_eq!(global.allocated(), 0);
        let mut g3 = global.try_lock_page_exclusive(&global_guard, 3).unwrap();
        global.bind_frame(key, &mut g3).unwrap();
        assert_eq!(global.allocated(), 1);
        assert_eq!(global.try_get_frame_id(&key), Some(3));
        assert_eq!(global.try_get_block_key(3), Some(key));

        assert!(global.bind_frame(key, &mut g3).is_ok());

        let err = global
            .bind_frame(PersistedBlockKey::new(7, 12), &mut g3)
            .unwrap_err();
        assert!(matches!(err, Error::InvalidState));

        drop(g3);
        assert_eq!(global.invalidate_key(&key), Some(3));
        assert_eq!(global.allocated(), 0);
    }

    #[test]
    fn test_read_submission_terminal_completion_is_one_shot() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let table_file = fs.create_table_file(118, make_metadata(), false).unwrap();
            let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
            drop(old_root);

            let global = owned_global_pool(frame_page_bytes(2));
            let pool = owned_readonly_pool(
                118,
                PersistedFileKind::TableFile,
                Arc::clone(&table_file),
                &global,
            );
            let key = PersistedBlockKey::new(118, 14);
            let inflight = Arc::new(PageIOCompletion::new());
            let task_arena = global.arena.arena_guard(global.pool_guard());
            let (frame_id, page_guard) =
                ReadonlyPageReservation::reserve_page(&global, key, task_arena)
                    .await
                    .unwrap();
            let reservation = ReadonlyPageReservation::from_reserved_page(
                pool.global.clone(),
                key,
                frame_id,
                page_guard,
            );
            let mut submission = ReadSubmission::new(
                pool.backing.clone(),
                pool.global.clone(),
                key,
                Arc::clone(&inflight),
                None,
                reservation,
            );

            submission.complete_inflight_once(Err(Error::SendError));
            assert!(submission.completed);
            assert!(matches!(
                inflight.completed_result(),
                Some(Err(Error::SendError))
            ));

            drop(submission);
            assert!(matches!(
                inflight.completed_result(),
                Some(Err(Error::SendError))
            ));

            drop(pool);
            drop(table_file);
            drop(fs);
        });
    }

    #[test]
    fn test_global_readonly_pool_rejects_too_small_capacity() {
        let bytes = (MIN_READONLY_POOL_PAGES - 1)
            * (mem::size_of::<BufferFrame>() + mem::size_of::<Page>());
        let res = GlobalReadonlyBufferPool::with_capacity(PoolRole::Disk, bytes);
        assert!(matches!(res, Err(Error::BufferPoolSizeTooSmall)));
    }

    #[test]
    fn test_global_invalidate_file() {
        let global = owned_global_pool(64 * 1024 * 1024);
        let global_guard = (*global).pool_guard();
        let k1 = PersistedBlockKey::new(1, 10);
        let k2 = PersistedBlockKey::new(1, 11);
        let k3 = PersistedBlockKey::new(2, 20);

        let mut g1 = global.try_lock_page_exclusive(&global_guard, 1).unwrap();
        let mut g2 = global.try_lock_page_exclusive(&global_guard, 2).unwrap();
        let mut g3 = global.try_lock_page_exclusive(&global_guard, 3).unwrap();
        global.bind_frame(k1, &mut g1).unwrap();
        global.bind_frame(k2, &mut g2).unwrap();
        global.bind_frame(k3, &mut g3).unwrap();
        drop(g1);
        drop(g2);
        drop(g3);

        assert_eq!(global.invalidate_file(1), 2);
        assert_eq!(global.try_get_frame_id(&k1), None);
        assert_eq!(global.try_get_frame_id(&k2), None);
        assert_eq!(global.try_get_frame_id(&k3), Some(3));
    }

    #[test]
    fn test_readonly_cache_file_ids_keep_catalog_and_user_pages_isolated() {
        let global = owned_global_pool(64 * 1024 * 1024);
        let global_guard = (*global).pool_guard();
        let catalog_key = PersistedBlockKey::new(USER_OBJ_ID_START - 1, 42);
        let user_key = PersistedBlockKey::new(USER_OBJ_ID_START, 42);

        let mut catalog_frame = global.try_lock_page_exclusive(&global_guard, 1).unwrap();
        let mut user_frame = global.try_lock_page_exclusive(&global_guard, 2).unwrap();
        global.bind_frame(catalog_key, &mut catalog_frame).unwrap();
        global.bind_frame(user_key, &mut user_frame).unwrap();
        drop(catalog_frame);
        drop(user_frame);

        assert_eq!(global.try_get_frame_id(&catalog_key), Some(1));
        assert_eq!(global.try_get_frame_id(&user_key), Some(2));
    }

    #[test]
    fn test_global_invalidate_key_strict() {
        let global = owned_global_pool(64 * 1024 * 1024);
        let global_guard = (*global).pool_guard();
        let key = PersistedBlockKey::new(9, 77);

        let mut g = global.try_lock_page_exclusive(&global_guard, 5).unwrap();
        global.bind_frame(key, &mut g).unwrap();
        drop(g);

        assert_eq!(global.invalidate_key_strict(&key), Some(5));
        assert_eq!(global.try_get_frame_id(&key), None);
    }

    #[test]
    #[should_panic(expected = "strict invalidation lock acquisition failed")]
    fn test_global_invalidate_key_strict_panics_when_latch_held() {
        smol::block_on(async {
            let global = owned_global_pool(64 * 1024 * 1024);
            let global_guard = (*global).pool_guard();
            let key = PersistedBlockKey::new(10, 99);

            let mut g = global.try_lock_page_exclusive(&global_guard, 6).unwrap();
            global.bind_frame(key, &mut g).unwrap();
            let shared = g.downgrade_shared();
            let _ = global.invalidate_key_strict(&key);
            drop(shared);
        });
    }

    #[test]
    #[should_panic(expected = "pool guard identity mismatch")]
    fn test_global_readonly_pool_panics_on_foreign_guard() {
        smol::block_on(async {
            let global1 = owned_global_pool(64 * 1024 * 1024);
            let global2 = owned_global_pool(64 * 1024 * 1024);
            let foreign_guard = (*global2).pool_guard();
            let _ = global1
                .get_page_internal::<Page>(&foreign_guard, 0, LatchFallbackMode::Shared)
                .await
                .unwrap();
        });
    }

    #[test]
    fn test_readonly_pool_reloads_when_mapping_points_to_uninitialized_frame() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let table_file = fs.create_table_file(111, make_metadata(), false).unwrap();
            let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
            drop(old_root);
            write_payload(&table_file, 9, b"reload").await;

            let global = owned_global_pool(frame_page_bytes(2));
            let global_guard = (*global).pool_guard();
            let key = PersistedBlockKey::new(111, 9);

            let mut g0 = global.try_lock_page_exclusive(&global_guard, 0).unwrap();
            global.bind_frame(key, &mut g0).unwrap();
            g0.page_mut().zero();
            let frame = g0.bf_mut();
            frame.clear_persisted_block_key();
            frame.bump_generation();
            frame.set_kind(FrameKind::Uninitialized);
            drop(g0);

            let pool = owned_readonly_pool(
                111,
                PersistedFileKind::TableFile,
                Arc::clone(&table_file),
                &global,
            );
            let pool_guard = (*pool).pool_guard();
            let reload_start = pool.global_stats();
            let page: FacadePageGuard<Page> = pool
                .get_page::<Page>(&pool_guard, 9, LatchFallbackMode::Shared)
                .await
                .expect("buffer-pool read failed in test");
            let page = page.lock_shared_async().await.unwrap();
            assert_eq!(&page.page()[..6], b"reload");
            drop(page);

            let reload_delta = pool.global_stats().delta_since(reload_start);
            assert_eq!(reload_delta.cache_hits, 0);
            assert_eq!(reload_delta.cache_misses, 1);
            assert_eq!(reload_delta.miss_joins, 0);
            assert_eq!(reload_delta.queued_reads, 1);
            assert_eq!(reload_delta.running_reads, 1);
            assert_eq!(reload_delta.completed_reads, 1);
            assert_eq!(reload_delta.read_errors, 0);

            let reloaded_frame_id = global.try_get_frame_id(&key).unwrap();
            assert_ne!(reloaded_frame_id, 0);
            drop(table_file);
            drop(fs);
        });
    }

    #[test]
    fn test_readonly_pool_validated_reload_counts_miss_then_warm_hit() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let table_file = fs.create_table_file(123, make_metadata(), false).unwrap();
            let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
            drop(old_root);

            let persisted_page = build_valid_persisted_lwc_page();
            write_page_bytes(&table_file, 12, &persisted_page).await;

            let global = owned_global_pool(frame_page_bytes(2));
            let global_guard = (*global).pool_guard();
            let key = PersistedBlockKey::new(123, 12);

            let mut g0 = global.try_lock_page_exclusive(&global_guard, 0).unwrap();
            global.bind_frame(key, &mut g0).unwrap();
            g0.page_mut().zero();
            let frame = g0.bf_mut();
            frame.clear_persisted_block_key();
            frame.bump_generation();
            frame.set_kind(FrameKind::Uninitialized);
            drop(g0);

            let pool = owned_readonly_pool(
                123,
                PersistedFileKind::TableFile,
                Arc::clone(&table_file),
                &global,
            );

            let reload_start = pool.global_stats();
            let page = pool
                .get_validated_page_shared(12, validate_persisted_lwc_page)
                .await
                .expect("validated readonly reload failed in test");
            drop(page);

            let reload_delta = pool.global_stats().delta_since(reload_start);
            assert_eq!(reload_delta.cache_hits, 0);
            assert_eq!(reload_delta.cache_misses, 1);
            assert_eq!(reload_delta.miss_joins, 0);
            assert_eq!(reload_delta.queued_reads, 1);
            assert_eq!(reload_delta.running_reads, 1);
            assert_eq!(reload_delta.completed_reads, 1);
            assert_eq!(reload_delta.read_errors, 0);

            let warm_start = pool.global_stats();
            let page = pool
                .get_validated_page_shared(12, validate_persisted_lwc_page)
                .await
                .expect("validated readonly warm hit failed in test");
            drop(page);

            let warm_delta = pool.global_stats().delta_since(warm_start);
            assert_eq!(warm_delta.cache_hits, 1);
            assert_eq!(warm_delta.cache_misses, 0);
            assert_eq!(warm_delta.miss_joins, 0);
            assert_eq!(warm_delta.queued_reads, 0);
            assert_eq!(warm_delta.running_reads, 0);
            assert_eq!(warm_delta.completed_reads, 0);
            assert_eq!(warm_delta.read_errors, 0);
        });
    }

    #[test]
    fn test_readonly_pool_miss_load_and_hit() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let table_file = fs.create_table_file(101, make_metadata(), false).unwrap();
            let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
            drop(old_root);
            write_payload(&table_file, 3, b"hello").await;

            let global = owned_global_pool(frame_page_bytes(4));
            let pool = owned_readonly_pool(
                101,
                PersistedFileKind::TableFile,
                Arc::clone(&table_file),
                &global,
            );
            let pool_guard = (*pool).pool_guard();
            let page: FacadePageGuard<Page> = pool
                .get_page::<Page>(&pool_guard, 3, LatchFallbackMode::Shared)
                .await
                .expect("buffer-pool read failed in test");
            let page = page.lock_shared_async().await.unwrap();
            assert_eq!(&page.page()[..5], b"hello");
            drop(page);

            let page: FacadePageGuard<Page> = pool
                .get_page::<Page>(&pool_guard, 3, LatchFallbackMode::Shared)
                .await
                .expect("buffer-pool read failed in test");
            let page = page.lock_shared_async().await.unwrap();
            assert_eq!(&page.page()[..5], b"hello");
            assert_eq!(global.allocated(), 1);
            drop(page);
            drop(table_file);
            drop(fs);
        });
    }

    #[test]
    fn test_readonly_pool_dedup_concurrent_miss() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let table_file = fs.create_table_file(102, make_metadata(), false).unwrap();
            let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
            drop(old_root);
            write_payload(&table_file, 5, b"world").await;

            let global = owned_global_pool(frame_page_bytes(8));
            let pool = owned_readonly_pool(
                102,
                PersistedFileKind::TableFile,
                Arc::clone(&table_file),
                &global,
            );
            let pool_guard = (*pool).pool_guard();

            let mut tasks = vec![];
            for _ in 0..16 {
                let pool = (*pool).clone();
                let pool_guard = pool_guard.clone();
                tasks.push(smol::spawn(async move {
                    let g: FacadePageGuard<Page> = pool
                        .get_page::<Page>(&pool_guard, 5, LatchFallbackMode::Shared)
                        .await
                        .expect("buffer-pool read failed in test");
                    g.lock_shared_async().await.unwrap().page()[0]
                }));
            }
            for task in tasks {
                assert_eq!(task.await, b'w');
            }
            assert_eq!(global.allocated(), 1);
            drop(table_file);
            drop(fs);
        });
    }

    #[test]
    fn test_readonly_pool_cancelled_loader_keeps_shared_miss_attempt_alive() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let table_file = fs.create_table_file(112, make_metadata(), false).unwrap();
            let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
            drop(old_root);
            write_payload(&table_file, 5, b"hello").await;
            let read_hook = Arc::new(ControlledReadHook::for_page(table_file.raw_fd(), 5));
            let _hook = install_storage_backend_test_hook(read_hook.clone());

            let global = owned_global_pool(frame_page_bytes(2));
            let pool = owned_readonly_pool(
                112,
                PersistedFileKind::TableFile,
                Arc::clone(&table_file),
                &global,
            );
            let pool_guard = (*pool).pool_guard();
            let key = PersistedBlockKey::new(112, 5);

            let pool_for_loader = (*pool).clone();
            let loader = smol::spawn(async move {
                let _ = pool_for_loader
                    .get_page_facade::<Page>(&pool_guard, 5, LatchFallbackMode::Shared)
                    .await
                    .unwrap();
            });
            read_hook.wait_started(1).await;
            assert!(global.inflight_loads.contains_key(&key));

            drop(loader);

            let pool_guard = (*pool).pool_guard();
            let pool_for_waiter = (*pool).clone();
            let waiter = smol::spawn(async move {
                let g = pool_for_waiter
                    .get_page_facade::<Page>(&pool_guard, 5, LatchFallbackMode::Shared)
                    .await
                    .unwrap();
                let g = g.lock_shared_async().await.unwrap();
                g.page()[..5].to_vec()
            });
            smol::future::yield_now().await;

            read_hook.release();

            assert_eq!(waiter.await, b"hello");
            wait_for(|| !global.inflight_loads.contains_key(&key)).await;
            assert_eq!(read_hook.call_count(), 1);
            assert_eq!(global.allocated(), 1);
            assert!(global.try_get_frame_id(&key).is_some());
            drop(table_file);
            drop(fs);
        });
    }

    #[test]
    fn test_readonly_pool_cancelled_single_loader_does_not_leak_completed_inflight() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let table_file = fs.create_table_file(113, make_metadata(), false).unwrap();
            let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
            drop(old_root);
            write_payload(&table_file, 9, b"solo").await;
            let read_hook = Arc::new(ControlledReadHook::for_page(table_file.raw_fd(), 9));
            let _hook = install_storage_backend_test_hook(read_hook.clone());

            let global = owned_global_pool(frame_page_bytes(2));
            let pool = owned_readonly_pool(
                113,
                PersistedFileKind::TableFile,
                Arc::clone(&table_file),
                &global,
            );
            let pool_guard = (*pool).pool_guard();
            let key = PersistedBlockKey::new(113, 9);

            let pool_for_loader = (*pool).clone();
            let loader = smol::spawn(async move {
                let _ = pool_for_loader
                    .get_page_facade::<Page>(&pool_guard, 9, LatchFallbackMode::Shared)
                    .await
                    .unwrap();
            });
            read_hook.wait_started(1).await;
            assert!(global.inflight_loads.contains_key(&key));

            drop(loader);
            read_hook.release();

            let pool_guard = (*pool).pool_guard();
            wait_for(|| {
                !global.inflight_loads.contains_key(&key) && global.try_get_frame_id(&key).is_some()
            })
            .await;

            assert_eq!(read_hook.call_count(), 1);
            assert_eq!(global.allocated(), 1);
            let g = pool
                .get_page_facade::<Page>(&pool_guard, 9, LatchFallbackMode::Shared)
                .await
                .unwrap();
            let g = g.lock_shared_async().await.unwrap();
            assert_eq!(&g.page()[..4], b"solo");
            drop(table_file);
            drop(fs);
        });
    }

    #[test]
    fn test_readonly_pool_detached_miss_load_survives_pool_drop() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let table_file = fs.create_table_file(116, make_metadata(), false).unwrap();
            let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
            drop(old_root);
            write_payload(&table_file, 12, b"drop").await;
            let read_hook = Arc::new(ControlledReadHook::for_page(table_file.raw_fd(), 12));
            let _hook = install_storage_backend_test_hook(read_hook.clone());
            let key = PersistedBlockKey::new(116, 12);

            let global = owned_global_pool(frame_page_bytes(2));
            let pool = owned_readonly_pool(
                116,
                PersistedFileKind::TableFile,
                Arc::clone(&table_file),
                &global,
            );
            let pool_guard = (*pool).pool_guard();
            let inflight_loads = Arc::clone(&global.inflight_loads);
            let mappings = Arc::clone(&global.mappings);

            let pool_for_loader = (*pool).clone();
            let loader = smol::spawn(async move {
                let _ = pool_for_loader
                    .get_page_facade::<Page>(&pool_guard, 12, LatchFallbackMode::Shared)
                    .await
                    .unwrap();
            });
            read_hook.wait_started(1).await;
            assert!(inflight_loads.contains_key(&key));
            drop(loader);

            let dropped = Arc::new(AtomicBool::new(false));
            let dropped_flag = Arc::clone(&dropped);
            let teardown = thread::spawn(move || {
                drop(pool);
                drop(global);
                dropped_flag.store(true, Ordering::SeqCst);
            });

            thread::sleep(Duration::from_millis(50));
            assert!(!dropped.load(Ordering::SeqCst));

            read_hook.release();
            wait_for(|| !inflight_loads.contains_key(&key) && mappings.contains_key(&key)).await;
            teardown.join().unwrap();
            assert_eq!(read_hook.call_count(), 1);
            assert!(mappings.contains_key(&key));
            assert!(dropped.load(Ordering::SeqCst));
            drop(table_file);
            drop(fs);
        });
    }

    #[test]
    fn test_readonly_pool_drop_unblocks_detached_reserve_waiter() {
        smol::block_on(async {
            let key = PersistedBlockKey::new(117, 13);
            let global = owned_global_pool(frame_page_bytes(1));
            let inflight_loads = Arc::clone(&global.inflight_loads);

            {
                let mut free = global.residency.free.lock();
                free.clear();
            }

            let inflight = Arc::new(PageIOCompletion::new());
            global.inflight_loads.insert(key, Arc::clone(&inflight));
            let task_arena = global.arena.arena_guard(global.pool_guard());
            let global_ptr =
                UnsafePtr(std::ptr::from_ref::<GlobalReadonlyBufferPool>(&global) as *mut _);
            let reserve_waiter = {
                listener!(global.residency.evict_ev => evict_listener);
                let reserve_waiter = smol::spawn(async move {
                    let global_ptr = global_ptr;
                    // SAFETY: the spawned task holds `task_arena`, which keeps
                    // the pool allocation alive until the waiter finishes.
                    let global = unsafe { &*global_ptr.0 };
                    match ReadonlyPageReservation::reserve_page(global, key, task_arena).await {
                        Ok((_frame_id, _page_guard)) => {
                            panic!("reserve waiter unexpectedly acquired a frame");
                        }
                        Err(err) => global.complete_inflight_load(key, &inflight, Err(err)),
                    }
                });
                evict_listener.await;
                reserve_waiter
            };
            assert!(inflight_loads.contains_key(&key));
            assert!(global.residency.alloc_failure_rate() > 0.0);

            let dropped = Arc::new(AtomicBool::new(false));
            let dropped_flag = Arc::clone(&dropped);
            let teardown = thread::spawn(move || {
                drop(global);
                dropped_flag.store(true, Ordering::SeqCst);
            });

            wait_for(|| dropped.load(Ordering::SeqCst)).await;
            reserve_waiter.await;
            teardown.join().unwrap();

            assert!(!inflight_loads.contains_key(&key));
        });
    }

    #[test]
    fn test_readonly_pool_shared_io_failure_propagates_to_all_waiters() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let table_file = fs.create_table_file(114, make_metadata(), false).unwrap();
            let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
            drop(old_root);
            let read_hook = Arc::new(ControlledReadHook::with_errno(
                table_file.raw_fd(),
                7,
                libc::EIO,
            ));
            let _hook = install_storage_backend_test_hook(read_hook.clone());

            let global = owned_global_pool(frame_page_bytes(2));
            let pool = owned_readonly_pool(
                114,
                PersistedFileKind::TableFile,
                Arc::clone(&table_file),
                &global,
            );
            let pool_guard = (*pool).pool_guard();
            let key = PersistedBlockKey::new(114, 7);

            let pool_guard_1 = pool_guard.clone();
            let pool_1 = (*pool).clone();
            let waiter1 = smol::spawn(async move {
                pool_1
                    .get_page_facade::<Page>(&pool_guard_1, 7, LatchFallbackMode::Shared)
                    .await
            });
            let pool_guard_2 = pool_guard.clone();
            let pool_2 = (*pool).clone();
            let waiter2 = smol::spawn(async move {
                pool_2
                    .get_page_facade::<Page>(&pool_guard_2, 7, LatchFallbackMode::Shared)
                    .await
            });

            read_hook.wait_started(1).await;
            smol::Timer::after(Duration::from_millis(10)).await;
            read_hook.release();

            assert!(matches!(waiter1.await, Err(Error::IOError)));
            assert!(matches!(waiter2.await, Err(Error::IOError)));
            assert_eq!(read_hook.call_count(), 1);
            assert_eq!(global.allocated(), 0);
            assert_eq!(global.try_get_frame_id(&key), None);
            wait_for(|| !global.inflight_loads.contains_key(&key)).await;
            drop(table_file);
            drop(fs);
        });
    }

    #[test]
    fn test_readonly_pool_shared_validated_load_propagates_validation_failure() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let table_file = fs.create_table_file(115, make_metadata(), false).unwrap();
            let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
            drop(old_root);
            let mut page = build_valid_persisted_lwc_page();
            let last_idx = page.len() - 1;
            page[last_idx] ^= 0xFF;
            write_page_bytes(&table_file, 8, &page).await;
            let read_hook = Arc::new(ControlledReadHook::for_page(table_file.raw_fd(), 8));
            let _hook = install_storage_backend_test_hook(read_hook.clone());

            let global = owned_global_pool(frame_page_bytes(2));
            let pool = owned_readonly_pool(
                115,
                PersistedFileKind::TableFile,
                Arc::clone(&table_file),
                &global,
            );
            let _pool_guard = (*pool).pool_guard();
            let key = PersistedBlockKey::new(115, 8);

            let pool_1 = (*pool).clone();
            let waiter1 = smol::spawn(async move {
                pool_1
                    .get_validated_page_shared(8, validate_persisted_lwc_page)
                    .await
            });
            let pool_2 = (*pool).clone();
            let waiter2 = smol::spawn(async move {
                pool_2
                    .get_validated_page_shared(8, validate_persisted_lwc_page)
                    .await
            });

            read_hook.wait_started(1).await;
            smol::Timer::after(Duration::from_millis(10)).await;
            read_hook.release();

            let err1 = match waiter1.await {
                Ok(_) => panic!("expected persisted LWC corruption"),
                Err(err) => err,
            };
            let err2 = match waiter2.await {
                Ok(_) => panic!("expected persisted LWC corruption"),
                Err(err) => err,
            };
            assert!(matches!(
                err1,
                Error::PersistedPageCorrupted {
                    file_kind: PersistedFileKind::TableFile,
                    page_kind: PersistedPageKind::LwcPage,
                    page_id: 8,
                    cause: PersistedPageCorruptionCause::ChecksumMismatch,
                }
            ));
            assert!(matches!(
                err2,
                Error::PersistedPageCorrupted {
                    file_kind: PersistedFileKind::TableFile,
                    page_kind: PersistedPageKind::LwcPage,
                    page_id: 8,
                    cause: PersistedPageCorruptionCause::ChecksumMismatch,
                }
            ));
            assert_eq!(read_hook.call_count(), 1);
            assert_eq!(global.allocated(), 0);
            assert_eq!(global.try_get_frame_id(&key), None);
            wait_for(|| !global.inflight_loads.contains_key(&key)).await;
            drop(table_file);
            drop(fs);
        });
    }

    #[test]
    fn test_readonly_pool_validated_lwc_miss_rejects_corruption_without_mapping() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let table_file = fs.create_table_file(107, make_metadata(), false).unwrap();
            let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
            drop(old_root);

            let mut page = build_valid_persisted_lwc_page();
            let last_idx = page.len() - 1;
            page[last_idx] ^= 0xFF;
            write_page_bytes(&table_file, 9, &page).await;

            let global = owned_global_pool(frame_page_bytes(4));
            let pool = owned_readonly_pool(
                107,
                PersistedFileKind::TableFile,
                Arc::clone(&table_file),
                &global,
            );
            let key = PersistedBlockKey::new(107, 9);

            let err = match pool
                .get_validated_page_shared(9, validate_persisted_lwc_page)
                .await
            {
                Ok(_) => panic!("expected persisted LWC corruption"),
                Err(err) => err,
            };
            assert!(matches!(
                err,
                Error::PersistedPageCorrupted {
                    file_kind: PersistedFileKind::TableFile,
                    page_kind: PersistedPageKind::LwcPage,
                    page_id: 9,
                    cause: PersistedPageCorruptionCause::ChecksumMismatch,
                }
            ));
            assert_eq!(global.try_get_frame_id(&key), None);
            assert_eq!(global.allocated(), 0);
        });
    }

    #[test]
    fn test_readonly_pool_validated_column_index_miss_rejects_corruption_without_mapping() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let table_file = fs.create_table_file(108, make_metadata(), false).unwrap();
            let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
            drop(old_root);

            let mut page = build_valid_persisted_column_block_page();
            let last_idx = page.len() - 1;
            page[last_idx] ^= 0xFF;
            write_page_bytes(&table_file, 10, &page).await;

            let global = owned_global_pool(frame_page_bytes(4));
            let pool = owned_readonly_pool(
                108,
                PersistedFileKind::TableFile,
                Arc::clone(&table_file),
                &global,
            );
            let key = PersistedBlockKey::new(108, 10);

            let err = match pool
                .get_validated_page_shared(10, validate_persisted_column_block_index_page)
                .await
            {
                Ok(_) => panic!("expected persisted column-block corruption"),
                Err(err) => err,
            };
            assert!(matches!(
                err,
                Error::PersistedPageCorrupted {
                    file_kind: PersistedFileKind::TableFile,
                    page_kind: PersistedPageKind::ColumnBlockIndex,
                    page_id: 10,
                    cause: PersistedPageCorruptionCause::ChecksumMismatch,
                }
            ));
            assert_eq!(global.try_get_frame_id(&key), None);
            assert_eq!(global.allocated(), 0);
        });
    }

    #[test]
    fn test_readonly_pool_validated_blob_miss_rejects_corruption_without_mapping() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let table_file = fs.create_table_file(109, make_metadata(), false).unwrap();
            let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
            drop(old_root);

            let mut page = build_valid_persisted_blob_page();
            let last_idx = page.len() - 1;
            page[last_idx] ^= 0xFF;
            write_page_bytes(&table_file, 11, &page).await;

            let global = owned_global_pool(frame_page_bytes(4));
            let pool = owned_readonly_pool(
                109,
                PersistedFileKind::TableFile,
                Arc::clone(&table_file),
                &global,
            );
            let key = PersistedBlockKey::new(109, 11);

            let err = match pool
                .get_validated_page_shared(11, validate_persisted_blob_page)
                .await
            {
                Ok(_) => panic!("expected persisted deletion-blob corruption"),
                Err(err) => err,
            };
            assert!(matches!(
                err,
                Error::PersistedPageCorrupted {
                    file_kind: PersistedFileKind::TableFile,
                    page_kind: PersistedPageKind::ColumnDeletionBlob,
                    page_id: 11,
                    cause: PersistedPageCorruptionCause::ChecksumMismatch,
                }
            ));
            assert_eq!(global.try_get_frame_id(&key), None);
            assert_eq!(global.allocated(), 0);
        });
    }

    #[test]
    fn test_readonly_pool_drop_only_eviction_and_reload() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let table_file = fs.create_table_file(103, make_metadata(), false).unwrap();
            let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
            drop(old_root);

            let global = started_global_pool(frame_page_bytes(1));
            let pool = owned_readonly_pool(
                103,
                PersistedFileKind::TableFile,
                Arc::clone(&table_file),
                &global,
            );
            let pool_guard = (*pool).pool_guard();
            let capacity = global.capacity();
            let base_page_id = 7u64;

            // Prepare one more block than cache capacity to force drop-only eviction.
            for i in 0..=capacity {
                let page_id = base_page_id + i as u64;
                let payload = format!("page-{i}");
                write_payload(&table_file, page_id, payload.as_bytes()).await;
            }

            for i in 0..=capacity {
                let page_id = base_page_id + i as u64;
                let expected = format!("page-{i}");
                let g: FacadePageGuard<Page> = pool
                    .get_page::<Page>(&pool_guard, page_id, LatchFallbackMode::Shared)
                    .await
                    .expect("buffer-pool read failed in test");
                let g = g.lock_shared_async().await.unwrap();
                assert_eq!(&g.page()[..expected.len()], expected.as_bytes());
                drop(g);
            }

            let loaded_count = capacity + 1;
            let mapped_count = (0..=capacity)
                .filter(|i| {
                    let key = PersistedBlockKey::new(103, base_page_id + *i as u64);
                    global.try_get_frame_id(&key).is_some()
                })
                .count();
            assert!(mapped_count < loaded_count);

            // Reload the first page after cache pressure; this should still return correct data.
            let g: FacadePageGuard<Page> = pool
                .get_page::<Page>(&pool_guard, base_page_id, LatchFallbackMode::Shared)
                .await
                .expect("buffer-pool read failed in test");
            let g = g.lock_shared_async().await.unwrap();
            assert_eq!(&g.page()[..6], b"page-0");
            drop(g);
            drop(pool_guard);
            drop(pool);
            global.shutdown();
            drop(global);
            drop(table_file);
            drop(fs);
        });
    }

    #[test]
    fn test_readonly_pool_lifecycle_drop_order_with_table_fs() {
        smol::block_on(async {
            let global = owned_global_pool(frame_page_bytes(2));
            let (_temp_dir, fs) = build_test_fs();
            let table_file = fs.create_table_file(105, make_metadata(), false).unwrap();
            let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
            drop(old_root);
            write_payload(&table_file, 9, b"drop-order").await;

            let pool = owned_readonly_pool(
                105,
                PersistedFileKind::TableFile,
                Arc::clone(&table_file),
                &global,
            );
            let pool_guard = (*pool).pool_guard();

            let g: FacadePageGuard<Page> = pool
                .get_page::<Page>(&pool_guard, 9, LatchFallbackMode::Shared)
                .await
                .expect("buffer-pool read failed in test");
            let g = g.lock_shared_async().await.unwrap();
            assert_eq!(&g.page()[..10], b"drop-order");
            drop(g);
            drop(table_file);
        });
    }

    #[test]
    #[should_panic(expected = "readonly buffer pool does not support page allocation")]
    fn test_readonly_pool_allocate_page_panics() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let table_file = fs.create_table_file(104, make_metadata(), false).unwrap();
            let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
            drop(old_root);

            let global = owned_global_pool(64 * 1024 * 1024);
            let pool = owned_readonly_pool(104, PersistedFileKind::TableFile, table_file, &global);
            let pool_guard = (*pool).pool_guard();
            let _ = pool.allocate_page::<Page>(&pool_guard).await;
        });
    }

    #[test]
    fn test_global_readonly_pool_uses_custom_arbiter_builder() {
        let global = QuiescentBox::new(
            GlobalReadonlyBufferPool::with_capacity_and_arbiter_builder(
                PoolRole::Disk,
                frame_page_bytes(16),
                EvictionArbiterBuilder::new()
                    .target_free(4)
                    .hysteresis(2)
                    .failure_rate_threshold(0.01)
                    .failure_window(7)
                    .dynamic_batch_bounds(5, 5),
            )
            .unwrap(),
        );
        let arbiter = global.eviction_arbiter;

        assert_eq!(arbiter.target_free(), 4);
        assert_eq!(arbiter.hysteresis(), 2);
        assert_eq!(arbiter.failure_window(), 7);

        // Verify failure-window wiring through readonly residency tracker.
        for _ in 0..7 {
            global.residency.record_alloc_failure();
        }
        for _ in 0..3 {
            global.residency.record_alloc_success();
        }
        let failure_rate = global.residency.alloc_failure_rate();
        assert!((failure_rate - (4.0 / 7.0)).abs() < 1e-9);

        // Verify batch bounds/threshold from builder influence decision output.
        let decision = arbiter.decide(8, global.capacity(), 0, 0.02, 1).unwrap();
        assert_eq!(decision.batch_size, 5);
    }
}
