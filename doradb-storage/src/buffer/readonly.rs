use crate::buffer::ReadonlyFileID;
use crate::buffer::arena::{ArenaGuard, QuiescentArena};
use crate::buffer::evictor::{
    ClockHand, EvictionArbiter, EvictionArbiterBuilder, EvictionRuntime, Evictor,
    FailureRateTracker, PressureDeltaClockPolicy, clock_collect_batch, clock_sweep_candidate,
};
use crate::buffer::frame::{BufferFrame, FrameKind};
use crate::buffer::guard::{FacadePageGuard, PageExclusiveGuard, PageGuard, PageSharedGuard};
use crate::buffer::page::{BufferPage, PAGE_SIZE, Page, PageID, VersionedPageID};
use crate::buffer::util::madvise_dontneed;
use crate::buffer::{BufferPool, PoolGuard, PoolIdentity, PoolRole};
use crate::error::Validation::Valid;
use crate::error::{Error, PersistedFileKind, Result, Validation};
use crate::file::multi_table_file::MultiTableFile;
use crate::file::table_file::TableFile;
use crate::latch::LatchFallbackMode;
use crate::ptr::UnsafePtr;
use crate::quiescent::{QuiescentBox, QuiescentGuard, SyncQuiescentGuard};
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use event_listener::{Event, EventListener, listener};
use parking_lot::Mutex;
use std::collections::BTreeSet;
use std::future::Future;
use std::mem;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;

/// Minimum number of frames required for global readonly pool.
///
/// Very small pools provide little practical value and can stall eviction/load flow.
const MIN_READONLY_POOL_PAGES: usize = 256;

/// Physical cache identity for readonly file pages.
///
/// This intentionally excludes root version to preserve cache hits across
/// root swaps when physical blocks are unchanged.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ReadonlyCacheKey {
    /// Readonly file identity owning the block.
    pub file_id: ReadonlyFileID,
    /// Physical page/block id in the backing file.
    pub block_id: PageID,
}

impl ReadonlyCacheKey {
    /// Builds a key from file id and physical block id.
    #[inline]
    pub fn new(file_id: ReadonlyFileID, block_id: PageID) -> Self {
        ReadonlyCacheKey { file_id, block_id }
    }
}

/// Async direct-read source usable by the shared readonly cache.
pub trait ReadonlyPageSource: Send + Sync {
    fn read_page_into_ptr<'a>(
        &'a self,
        page_id: PageID,
        ptr: UnsafePtr<u8>,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;
}

type ReadonlyPageValidator = fn(&[u8], PersistedFileKind, PageID) -> Result<()>;

impl ReadonlyPageSource for TableFile {
    #[inline]
    fn read_page_into_ptr<'a>(
        &'a self,
        page_id: PageID,
        ptr: UnsafePtr<u8>,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            // SAFETY: caller upholds destination pointer validity for the full async read.
            unsafe { TableFile::read_page_into_ptr(self, page_id, ptr).await }
        })
    }
}

impl ReadonlyPageSource for MultiTableFile {
    #[inline]
    fn read_page_into_ptr<'a>(
        &'a self,
        page_id: PageID,
        ptr: UnsafePtr<u8>,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            let cow_file = std::ops::Deref::deref(self);
            // SAFETY: caller upholds destination pointer validity for the full async read.
            unsafe { cow_file.read_page_into_ptr(page_id, ptr).await }
        })
    }
}

/// Global readonly cache owner shared across files.
///
/// This type owns the frame/page arena and maintains a forward mapping
/// from physical on-disk identity to in-memory frame id.
///
/// Reverse lookup is stored inline in `BufferFrame` as readonly key metadata.
pub struct GlobalReadonlyBufferPool {
    size: usize,
    mappings: Arc<DashMap<ReadonlyCacheKey, PageID>>,
    inflight_loads: Arc<DashMap<ReadonlyCacheKey, Arc<InflightLoad>>>,
    residency: Arc<ReadonlyResidency>,
    eviction_arbiter: EvictionArbiter,
    shutdown_flag: Arc<AtomicBool>,
    evict_thread: Mutex<Option<JoinHandle<()>>>,
    role: PoolRole,
    arena: QuiescentArena,
}

impl GlobalReadonlyBufferPool {
    /// Creates a raw global readonly pool with a target memory budget in bytes.
    ///
    /// This constructor intentionally does not start the eviction worker.
    /// Callers must first place the pool into a stable owner such as
    /// [`QuiescentBox`] or [`crate::quiescent::QuiDAG`] before starting guarded
    /// worker threads.
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
            evict_thread: Mutex::new(None),
            role,
            arena,
        };
        Ok(pool)
    }

    /// Creates a started quiescent owner for standalone readonly-pool usage.
    ///
    /// The returned owner already has its eviction worker running through the
    /// guarded startup path. Call [`Self::shutdown`] before dropping
    /// non-process-lifetime owners, otherwise [`QuiescentBox`] teardown will
    /// wait forever on the worker's keepalive guard.
    #[inline]
    pub fn with_capacity_owned(role: PoolRole, pool_size: usize) -> Result<QuiescentBox<Self>> {
        Self::with_capacity_and_arbiter_builder_owned(role, pool_size, EvictionArbiter::builder())
    }

    /// Creates a started quiescent owner with explicit eviction arbiter builder.
    #[inline]
    pub fn with_capacity_and_arbiter_builder_owned(
        role: PoolRole,
        pool_size: usize,
        eviction_arbiter_builder: EvictionArbiterBuilder,
    ) -> Result<QuiescentBox<Self>> {
        let pool = QuiescentBox::new(Self::with_capacity_and_arbiter_builder(
            role,
            pool_size,
            eviction_arbiter_builder,
        )?);
        Self::start_evictor_thread_guarded(pool.guard());
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

    #[inline]
    pub fn guard(&self) -> PoolGuard {
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
    fn load_task_state(&self) -> ReadonlyLoadTaskState {
        ReadonlyLoadTaskState {
            mappings: Arc::clone(&self.mappings),
            inflight_loads: Arc::clone(&self.inflight_loads),
            residency: Arc::clone(&self.residency),
            shutdown_flag: Arc::clone(&self.shutdown_flag),
        }
    }

    /// Looks up mapped frame id for a given physical cache key.
    #[inline]
    pub fn try_get_frame_id(&self, key: &ReadonlyCacheKey) -> Option<PageID> {
        self.mappings.get(key).map(|v| *v)
    }

    /// Looks up physical cache key by frame id.
    #[inline]
    pub fn try_get_key(&self, frame_id: PageID) -> Option<ReadonlyCacheKey> {
        if frame_id as usize >= self.size {
            return None;
        }
        let frame = self.arena.frame(frame_id);
        frame
            .readonly_key()
            .map(|(file_id, block_id)| ReadonlyCacheKey::new(file_id, block_id))
    }

    /// Binds a physical key to an exclusively locked frame.
    ///
    /// Binding is idempotent for the same key/frame pair and returns
    /// `Error::InvalidState` for conflicting mapping attempts.
    #[inline]
    pub fn bind_frame(
        &self,
        key: ReadonlyCacheKey,
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
                return match frame.readonly_key() {
                    Some((file_id, block_id))
                        if file_id == key.file_id && block_id == key.block_id =>
                    {
                        Ok(())
                    }
                    _ => Err(Error::InvalidState),
                };
            }
            Entry::Vacant(vac) => {
                if let Some((file_id, block_id)) = frame.readonly_key() {
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
            frame.set_readonly_key(key.file_id, key.block_id);
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
    pub fn invalidate_key(&self, key: &ReadonlyCacheKey) -> Option<PageID> {
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
    pub fn invalidate_key_strict(&self, key: &ReadonlyCacheKey) -> Option<PageID> {
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
        file_id: ReadonlyFileID,
        block_id: PageID,
    ) -> Option<PageID> {
        self.invalidate_key(&ReadonlyCacheKey::new(file_id, block_id))
    }

    /// Invalidates one physical block from one file using strict GC ordering.
    #[inline]
    pub fn invalidate_file_block_strict(
        &self,
        file_id: ReadonlyFileID,
        block_id: PageID,
    ) -> Option<PageID> {
        self.invalidate_key_strict(&ReadonlyCacheKey::new(file_id, block_id))
    }

    /// Invalidates all cache entries belonging to one file.
    ///
    /// Returns the number of invalidated mappings.
    #[inline]
    pub fn invalidate_file(&self, file_id: ReadonlyFileID) -> usize {
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
    pub fn invalidate_file_strict(&self, file_id: ReadonlyFileID) -> usize {
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
    pub(crate) fn start_evictor_thread_guarded(pool: QuiescentGuard<Self>) {
        // Acquire one direct guard from the stable owner, convert it once to
        // `SyncQuiescentGuard`, then clone that wrapper into the spawned
        // thread/runtime. This keeps worker startup on the guard side without
        // rebuilding a static-ownership compatibility layer.
        let pool = pool.into_sync();
        let runtime = ReadonlyRuntime {
            arena: pool.arena.arena_guard(pool.guard()),
            mappings: Arc::clone(&pool.mappings),
            residency: Arc::clone(&pool.residency),
            _pool: Some(pool.clone()),
        };
        let policy = PressureDeltaClockPolicy::new(pool.eviction_arbiter, 1);
        let evictor = Evictor::new(runtime, policy, Arc::clone(&pool.shutdown_flag));
        let handle = evictor.start_thread("ReadonlyBufferPoolEvictor");
        let mut g = pool.evict_thread.lock();
        *g = Some(handle);
    }

    /// Stops the eviction worker for a started standalone readonly pool.
    ///
    /// This method is idempotent and is required before dropping owners
    /// created by [`Self::with_capacity_owned`] or
    /// [`Self::with_capacity_and_arbiter_builder_owned`].
    #[inline]
    pub fn shutdown(&self) {
        self.shutdown_flag.store(true, Ordering::SeqCst);
        self.residency.free_ev.notify(usize::MAX);
        self.residency.evict_ev.notify(1);
        {
            let mut g = self.evict_thread.lock();
            if let Some(handle) = g.take() {
                handle.join().unwrap();
            }
        }
    }

    #[inline]
    fn invalidate_frame_with_guard(
        &self,
        mut page_guard: PageExclusiveGuard<Page>,
        expected_key: Option<ReadonlyCacheKey>,
    ) {
        let frame = page_guard.bf_mut();
        if frame.kind() == FrameKind::Uninitialized {
            return;
        }
        if let Some(key) = expected_key
            && let Some((file_id, block_id)) = frame.readonly_key()
        {
            debug_assert_eq!(file_id, key.file_id);
            debug_assert_eq!(block_id, key.block_id);
        }
        frame.clear_readonly_key();
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
    fn invalidate_frame_retry(&self, frame_id: PageID, expected_key: Option<ReadonlyCacheKey>) {
        loop {
            let guard = self.guard();
            if let Some(page_guard) = self.try_lock_page_exclusive(&guard, frame_id) {
                self.invalidate_frame_with_guard(page_guard, expected_key);
                let _ = self.residency.move_resident_to_free(frame_id);
                return;
            }
            std::hint::spin_loop();
        }
    }

    #[inline]
    fn invalidate_frame_strict(&self, frame_id: PageID, expected_key: Option<ReadonlyCacheKey>) {
        let guard = self.guard();
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
    fn join_or_start_inflight_load(
        &self,
        key: ReadonlyCacheKey,
        page_source: Arc<dyn ReadonlyPageSource>,
        validation: Option<InflightLoadValidation>,
    ) -> Arc<InflightLoad> {
        match self.inflight_loads.entry(key) {
            Entry::Vacant(vac) => {
                let inflight = Arc::new(InflightLoad::new());
                vac.insert(Arc::clone(&inflight));
                let task_state = self.load_task_state();
                let task_inflight = Arc::clone(&inflight);
                let task_arena = self.arena.arena_guard(self.guard());
                smol::spawn(async move {
                    let mut completion =
                        InflightLoadCompletion::new(task_state.clone(), key, task_inflight);
                    let load_res = task_state
                        .run_inflight_load(key, page_source, validation, task_arena)
                        .await;
                    completion.complete(load_res);
                })
                .detach();
                inflight
            }
            // The first miss attempt for one key owns the shared validation policy.
            // Followers join that attempt and observe its terminal result.
            Entry::Occupied(occ) => Arc::clone(occ.get()),
        }
    }

    #[inline]
    async fn get_or_load_frame_id(
        &self,
        key: ReadonlyCacheKey,
        page_source: Arc<dyn ReadonlyPageSource>,
    ) -> Result<PageID> {
        if let Some(frame_id) = self.try_get_frame_id(&key) {
            return Ok(frame_id);
        }
        let inflight = self.join_or_start_inflight_load(key, page_source, None);
        if let Some(frame_id) = self.try_get_frame_id(&key) {
            return Ok(frame_id);
        }
        inflight.wait_result().await
    }

    #[inline]
    async fn get_or_load_frame_id_validated(
        &self,
        key: ReadonlyCacheKey,
        page_source: Arc<dyn ReadonlyPageSource>,
        file_kind: PersistedFileKind,
        validator: ReadonlyPageValidator,
    ) -> Result<PageID> {
        if let Some(frame_id) = self.try_get_frame_id(&key) {
            return Ok(frame_id);
        }
        let inflight = self.join_or_start_inflight_load(
            key,
            page_source,
            Some(InflightLoadValidation {
                file_kind,
                validator,
            }),
        );
        if let Some(frame_id) = self.try_get_frame_id(&key) {
            return Ok(frame_id);
        }
        inflight.wait_result().await
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
        Ok(FacadePageGuard::new(keepalive, bf, g))
    }

    #[inline]
    fn validate_guarded_frame_key<T: 'static>(
        &self,
        guard: &FacadePageGuard<T>,
        expected_key: ReadonlyCacheKey,
    ) -> bool {
        let frame = guard.bf();
        frame.kind() != FrameKind::Uninitialized
            && frame.readonly_key() == Some((expected_key.file_id, expected_key.block_id))
    }

    #[inline]
    fn invalidate_stale_mapping_if_same_frame(&self, key: ReadonlyCacheKey, frame_id: PageID) {
        if let Entry::Occupied(occ) = self.mappings.entry(key)
            && *occ.get() == frame_id
        {
            occ.remove();
        }
    }
}

impl Drop for GlobalReadonlyBufferPool {
    fn drop(&mut self) {
        self.shutdown();
    }
}

unsafe impl Send for GlobalReadonlyBufferPool {}
unsafe impl Sync for GlobalReadonlyBufferPool {}

#[derive(Clone, Copy)]
struct InflightLoadValidation {
    file_kind: PersistedFileKind,
    validator: ReadonlyPageValidator,
}

enum InflightState {
    Running,
    Completed(Result<PageID>),
}

struct InflightLoad {
    state: Mutex<InflightState>,
    ev: Event,
}

#[derive(Clone)]
struct ReadonlyLoadTaskState {
    mappings: Arc<DashMap<ReadonlyCacheKey, PageID>>,
    inflight_loads: Arc<DashMap<ReadonlyCacheKey, Arc<InflightLoad>>>,
    residency: Arc<ReadonlyResidency>,
    shutdown_flag: Arc<AtomicBool>,
}

impl ReadonlyLoadTaskState {
    #[inline]
    async fn reserve_frame_id(&self) -> Result<PageID> {
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
    async fn run_inflight_load(
        &self,
        key: ReadonlyCacheKey,
        page_source: Arc<dyn ReadonlyPageSource>,
        validation: Option<InflightLoadValidation>,
        arena: ArenaGuard,
    ) -> Result<PageID> {
        let mut reserved = ReservedMissFrameGuard::acquire(self.clone(), arena).await?;
        // SAFETY: destination memory is page-sized, aligned, and exclusively owned
        // by the reserved miss-frame guard for the full background miss attempt.
        let dst = UnsafePtr(reserved.page_mut() as *mut Page as *mut u8);
        page_source.read_page_into_ptr(key.block_id, dst).await?;
        if let Some(validation) = validation {
            (validation.validator)(reserved.page(), validation.file_kind, key.block_id)?;
        }
        reserved.publish(key)
    }

    #[inline]
    fn complete_inflight_load(
        &self,
        key: ReadonlyCacheKey,
        inflight: &Arc<InflightLoad>,
        result: Result<PageID>,
    ) {
        inflight.complete(result);
        match self.inflight_loads.entry(key) {
            Entry::Occupied(occ) if Arc::ptr_eq(occ.get(), inflight) => {
                occ.remove();
            }
            _ => {}
        }
        inflight.notify();
    }
}

struct ReservedMissFrameGuard {
    state: ReadonlyLoadTaskState,
    frame_id: PageID,
    page_guard: Option<PageExclusiveGuard<Page>>,
    published: bool,
}

impl ReservedMissFrameGuard {
    #[inline]
    async fn acquire(state: ReadonlyLoadTaskState, arena: ArenaGuard) -> Result<Self> {
        let frame_id = state.reserve_frame_id().await?;
        let mut page_guard = match arena.try_lock_page_exclusive(frame_id) {
            Some(page_guard) => page_guard,
            None => {
                state.residency.release_free(frame_id);
                return Err(Error::InvalidState);
            }
        };
        {
            let frame = page_guard.bf_mut();
            frame.clear_readonly_key();
            frame.set_dirty(false);
            frame.set_kind(FrameKind::Evicting);
        }
        Ok(ReservedMissFrameGuard {
            state,
            frame_id,
            page_guard: Some(page_guard),
            published: false,
        })
    }

    #[inline]
    fn page(&self) -> &Page {
        self.page_guard
            .as_ref()
            .expect("reserved miss frame must hold an exclusive page guard before publish")
            .page()
    }

    #[inline]
    fn page_mut(&mut self) -> &mut Page {
        self.page_guard
            .as_mut()
            .expect("reserved miss frame must hold an exclusive page guard before publish")
            .page_mut()
    }

    #[inline]
    fn publish(&mut self, key: ReadonlyCacheKey) -> Result<PageID> {
        {
            let frame = self
                .page_guard
                .as_mut()
                .expect("reserved miss frame must hold an exclusive page guard before publish")
                .bf_mut();
            frame.set_readonly_key(key.file_id, key.block_id);
            frame.set_dirty(false);
            frame.bump_generation();
            frame.set_kind(FrameKind::Hot);
        }
        match self.state.mappings.entry(key) {
            Entry::Vacant(vac) => {
                vac.insert(self.frame_id);
            }
            Entry::Occupied(_) => return Err(Error::InvalidState),
        }
        self.published = true;
        drop(self.page_guard.take());
        self.state.residency.mark_resident(self.frame_id);
        if self.state.residency.no_free_frame() {
            self.state.residency.evict_ev.notify(1);
        }
        Ok(self.frame_id)
    }
}

impl Drop for ReservedMissFrameGuard {
    #[inline]
    fn drop(&mut self) {
        if self.published {
            return;
        }
        let Some(mut page_guard) = self.page_guard.take() else {
            return;
        };
        page_guard.page_mut().zero();
        let frame = page_guard.bf_mut();
        frame.clear_readonly_key();
        frame.set_dirty(false);
        frame.bump_generation();
        frame.set_kind(FrameKind::Uninitialized);
        drop(page_guard);
        self.state.residency.release_free(self.frame_id);
    }
}

impl InflightLoad {
    #[inline]
    fn new() -> Self {
        InflightLoad {
            state: Mutex::new(InflightState::Running),
            ev: Event::new(),
        }
    }

    #[inline]
    fn complete(&self, result: Result<PageID>) {
        let mut state = self.state.lock();
        if matches!(&*state, InflightState::Running) {
            *state = InflightState::Completed(result);
        }
    }

    #[inline]
    fn notify(&self) {
        self.ev.notify(usize::MAX);
    }

    #[inline]
    fn completed_result(&self) -> Option<Result<PageID>> {
        let state = self.state.lock();
        match &*state {
            InflightState::Running => None,
            InflightState::Completed(result) => Some(result.clone()),
        }
    }

    #[inline]
    async fn wait_result(&self) -> Result<PageID> {
        loop {
            listener!(self.ev => listener);
            if let Some(result) = self.completed_result() {
                return result;
            }
            listener.await;
        }
    }
}

struct InflightLoadCompletion {
    state: ReadonlyLoadTaskState,
    key: ReadonlyCacheKey,
    inflight: Arc<InflightLoad>,
    completed: bool,
}

impl InflightLoadCompletion {
    #[inline]
    fn new(
        state: ReadonlyLoadTaskState,
        key: ReadonlyCacheKey,
        inflight: Arc<InflightLoad>,
    ) -> Self {
        InflightLoadCompletion {
            state,
            key,
            inflight,
            completed: false,
        }
    }

    #[inline]
    fn complete(&mut self, result: Result<PageID>) {
        if self.completed {
            return;
        }
        self.completed = true;
        self.state
            .complete_inflight_load(self.key, &self.inflight, result);
    }
}

impl Drop for InflightLoadCompletion {
    #[inline]
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        self.completed = true;
        self.state
            .complete_inflight_load(self.key, &self.inflight, Err(Error::InternalError));
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
    mappings: Arc<DashMap<ReadonlyCacheKey, PageID>>,
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

        if let Some((file_id, block_id)) = frame.readonly_key() {
            let key = ReadonlyCacheKey::new(file_id, block_id);
            if let Some((_, mapped_frame_id)) = self.mappings.remove(&key) {
                debug_assert_eq!(mapped_frame_id, frame_id);
            }
        }

        frame.clear_readonly_key();
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

unsafe impl Send for ReadonlyRuntime {}

/// Per-file readonly wrapper implementing the `BufferPool` contract.
///
/// This wrapper translates file-local `PageID` into global physical cache keys
/// and delegates to `GlobalReadonlyBufferPool`.
#[derive(Clone)]
pub struct ReadonlyBufferPool {
    file_id: ReadonlyFileID,
    file_kind: PersistedFileKind,
    page_source: Arc<dyn ReadonlyPageSource>,
    global: QuiescentGuard<GlobalReadonlyBufferPool>,
}

impl ReadonlyBufferPool {
    /// Creates a per-file readonly pool wrapper.
    #[inline]
    pub fn new<S>(
        file_id: ReadonlyFileID,
        file_kind: PersistedFileKind,
        page_source: Arc<S>,
        global: QuiescentGuard<GlobalReadonlyBufferPool>,
    ) -> Self
    where
        S: ReadonlyPageSource + 'static,
    {
        ReadonlyBufferPool {
            file_id,
            file_kind,
            page_source,
            global,
        }
    }

    /// Returns which persisted file format this pool reads from.
    #[inline]
    pub fn persisted_file_kind(&self) -> PersistedFileKind {
        self.file_kind
    }

    #[inline]
    fn cache_key(&self, block_id: PageID) -> ReadonlyCacheKey {
        ReadonlyCacheKey::new(self.file_id, block_id)
    }

    #[inline]
    async fn get_page_facade<T: BufferPage>(
        &self,
        guard: &PoolGuard,
        page_id: PageID,
        mode: LatchFallbackMode,
    ) -> Result<FacadePageGuard<T>> {
        let key = self.cache_key(page_id);
        let global = &self.global;
        global.validate_guard(guard);
        loop {
            let frame_id = global
                .get_or_load_frame_id(key, Arc::clone(&self.page_source))
                .await?;
            let page_guard = global.get_page_internal(guard, frame_id, mode).await?;
            if global.validate_guarded_frame_key(&page_guard, key) {
                return Ok(page_guard);
            }
            global.invalidate_stale_mapping_if_same_frame(key, frame_id);
        }
    }

    /// Returns a shared guard for one persisted page after validating its page-kind contract.
    ///
    /// On cache miss, validation runs before the new frame becomes resident.
    /// On cached hits, validation is re-run against the resident bytes and a
    /// failed validation invalidates the mapping before returning the error.
    #[inline]
    pub(crate) async fn try_get_validated_page_shared(
        &self,
        page_id: PageID,
        validator: ReadonlyPageValidator,
    ) -> Result<PageSharedGuard<Page>> {
        let key = self.cache_key(page_id);
        let global = &self.global;
        loop {
            let frame_id = global
                .get_or_load_frame_id_validated(
                    key,
                    Arc::clone(&self.page_source),
                    self.file_kind,
                    validator,
                )
                .await?;
            let guard = global.guard();
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
                return Ok(shared);
            }
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
    fn guard(&self) -> PoolGuard {
        self.global.guard()
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
    ) -> FacadePageGuard<T> {
        match self.get_page_facade(guard, page_id, mode).await {
            Ok(g) => g,
            Err(err) => {
                todo!("readonly BufferPool::get_page error policy is deferred: {err}");
            }
        }
    }

    #[inline]
    async fn try_get_page_versioned<T: BufferPage>(
        &self,
        guard: &PoolGuard,
        _id: VersionedPageID,
        _mode: LatchFallbackMode,
    ) -> Option<FacadePageGuard<T>> {
        self.global.validate_guard(guard);
        None
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
    ) -> Validation<FacadePageGuard<T>> {
        let g = <Self as BufferPool>::get_page(self, guard, page_id, mode).await;
        if p_guard.validate_bool() {
            return Valid(g);
        }
        if g.is_exclusive() {
            g.rollback_exclusive_version_change();
        }
        Validation::Invalid
    }
}

unsafe impl Send for ReadonlyBufferPool {}
unsafe impl Sync for ReadonlyBufferPool {}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::buffer::guard::{FacadePageGuard, PageGuard};
    use crate::buffer::page::Page;
    use crate::catalog::TableID;
    use crate::catalog::{ColumnAttributes, ColumnSpec, TableMetadata, USER_OBJ_ID_START};
    use crate::error::{PersistedPageCorruptionCause, PersistedPageKind};
    use crate::file::cow_file::COW_FILE_PAGE_SIZE;
    use crate::file::page_integrity::{
        COLUMN_BLOCK_INDEX_PAGE_SPEC, COLUMN_DELETION_BLOB_PAGE_SPEC, LWC_PAGE_SPEC,
        max_payload_len, write_page_checksum, write_page_header,
    };
    use crate::file::table_file::TableFile;
    use crate::file::table_fs::TableFileSystemConfig;
    use crate::index::{
        COLUMN_BLOCK_HEADER_SIZE, COLUMN_BLOCK_NODE_PAYLOAD_SIZE,
        COLUMN_DELETION_BLOB_PAGE_HEADER_SIZE, ColumnBlockNodeHeader, validate_persisted_blob_page,
        validate_persisted_column_block_index_page,
    };
    use crate::io::AIOBuf;
    use crate::lwc::{LWC_PAGE_PAYLOAD_SIZE, LwcPage, LwcPageHeader, validate_persisted_lwc_page};
    use crate::quiescent::QuiescentBox;
    use crate::value::ValKind;
    use std::ops::Deref;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::thread;
    use std::time::Duration;
    use tempfile::TempDir;

    fn frame_page_bytes(cap: usize) -> usize {
        cap.max(MIN_READONLY_POOL_PAGES) * (mem::size_of::<BufferFrame>() + mem::size_of::<Page>())
    }

    fn owned_global_pool(pool_size: usize) -> QuiescentBox<GlobalReadonlyBufferPool> {
        QuiescentBox::new(
            GlobalReadonlyBufferPool::with_capacity(PoolRole::Disk, pool_size).unwrap(),
        )
    }

    fn started_global_pool(pool_size: usize) -> QuiescentBox<GlobalReadonlyBufferPool> {
        GlobalReadonlyBufferPool::with_capacity_owned(PoolRole::Disk, pool_size).unwrap()
    }

    /// Test-only owner wrapper for a started global readonly pool.
    ///
    /// Local started pools must shut down their evictor thread before owner
    /// drop, otherwise `QuiescentBox` waits forever on the worker keepalive
    /// guard. This scope centralizes that shutdown ordering for other test
    /// modules that need a reusable readonly cache.
    pub(crate) struct GlobalReadOnlyPoolScope {
        owner: QuiescentBox<GlobalReadonlyBufferPool>,
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

    fn owned_readonly_pool<S>(
        file_id: ReadonlyFileID,
        file_kind: PersistedFileKind,
        page_source: Arc<S>,
        global: &QuiescentBox<GlobalReadonlyBufferPool>,
    ) -> QuiescentBox<ReadonlyBufferPool>
    where
        S: ReadonlyPageSource + 'static,
    {
        QuiescentBox::new(ReadonlyBufferPool::new(
            file_id,
            file_kind,
            page_source,
            global.guard(),
        ))
    }

    #[test]
    fn test_global_readonly_pool_shutdown_is_idempotent_before_worker_start() {
        let pool =
            GlobalReadonlyBufferPool::with_capacity(PoolRole::Disk, frame_page_bytes(2)).unwrap();

        pool.shutdown();
        pool.shutdown();
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
        let mut buf = table_file.buf_list().pop_async(true).await;
        let bytes = buf.as_bytes_mut();
        bytes[..payload.len()].copy_from_slice(payload);
        table_file.write_page(page_id, buf).await.unwrap();
    }

    async fn write_page_bytes(table_file: &Arc<TableFile>, page_id: PageID, bytes: &[u8]) {
        let mut buf = table_file.buf_list().pop_async(true).await;
        buf.as_bytes_mut().copy_from_slice(bytes);
        table_file.write_page(page_id, buf).await.unwrap();
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

    fn page_from_bytes(bytes: &[u8]) -> Page {
        assert_eq!(bytes.len(), PAGE_SIZE);
        let mut page = [0u8; PAGE_SIZE];
        page.copy_from_slice(bytes);
        page
    }

    #[derive(Clone)]
    enum ControlledReadOutcome {
        Page(Arc<Page>),
        Error(Error),
    }

    // Test-only page source that lets readonly miss-load tests control both
    // outcome and timing. Tests use `wait_started()` to observe that the miss
    // load has submitted into the shared inflight path, then cancel or attach
    // other waiters before calling `release()` to let the read complete.
    struct ControlledPageSource {
        outcome: ControlledReadOutcome,
        calls: AtomicUsize,
        start_ev: Event,
        released: AtomicBool,
        release_ev: Event,
    }

    impl ControlledPageSource {
        fn with_page(page: Page) -> Self {
            ControlledPageSource {
                outcome: ControlledReadOutcome::Page(Arc::new(page)),
                calls: AtomicUsize::new(0),
                start_ev: Event::new(),
                released: AtomicBool::new(false),
                release_ev: Event::new(),
            }
        }

        fn with_error(err: Error) -> Self {
            ControlledPageSource {
                outcome: ControlledReadOutcome::Error(err),
                calls: AtomicUsize::new(0),
                start_ev: Event::new(),
                released: AtomicBool::new(false),
                release_ev: Event::new(),
            }
        }

        fn call_count(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }

        // Unblocks the in-flight read so the test can deterministically decide
        // when the mock IO completes.
        fn release(&self) {
            self.released.store(true, Ordering::SeqCst);
            self.release_ev.notify(usize::MAX);
        }

        // Waits until the mock source has observed the requested number of read
        // attempts. Tests use this to synchronize on "IO started" before
        // canceling the initiating future or attaching followers.
        async fn wait_started(&self, expected_calls: usize) {
            loop {
                if self.call_count() >= expected_calls {
                    return;
                }
                listener!(self.start_ev => listener);
                if self.call_count() >= expected_calls {
                    return;
                }
                listener.await;
            }
        }
    }

    impl ReadonlyPageSource for ControlledPageSource {
        fn read_page_into_ptr<'a>(
            &'a self,
            _page_id: PageID,
            ptr: UnsafePtr<u8>,
        ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::SeqCst);
                self.start_ev.notify(usize::MAX);
                // Keep the read artificially in-flight until the test releases it.
                loop {
                    if self.released.load(Ordering::SeqCst) {
                        break;
                    }
                    listener!(self.release_ev => listener);
                    if self.released.load(Ordering::SeqCst) {
                        break;
                    }
                    listener.await;
                }
                match &self.outcome {
                    ControlledReadOutcome::Page(page) => {
                        // SAFETY: readonly miss tests reserve page-sized aligned frame memory.
                        unsafe { copy_page_into_ptr(page.as_ref(), ptr) }
                        Ok(())
                    }
                    ControlledReadOutcome::Error(err) => Err(err.clone()),
                }
            })
        }
    }

    async fn wait_for<F>(mut predicate: F)
    where
        F: FnMut() -> bool,
    {
        for _ in 0..100 {
            if predicate() {
                return;
            }
            smol::Timer::after(Duration::from_millis(10)).await;
        }
        panic!("condition was not satisfied before timeout");
    }

    unsafe fn copy_page_into_ptr(src: &Page, dst: UnsafePtr<u8>) {
        unsafe {
            std::ptr::copy_nonoverlapping(src.as_ptr(), dst.0, PAGE_SIZE);
        }
    }

    #[test]
    fn test_global_readonly_mapping_and_invalidation() {
        let global = owned_global_pool(64 * 1024 * 1024);
        let global_guard = (*global).guard();
        let key = ReadonlyCacheKey::new(7, 11);

        assert_eq!(global.allocated(), 0);
        let mut g3 = global.try_lock_page_exclusive(&global_guard, 3).unwrap();
        global.bind_frame(key, &mut g3).unwrap();
        assert_eq!(global.allocated(), 1);
        assert_eq!(global.try_get_frame_id(&key), Some(3));
        assert_eq!(global.try_get_key(3), Some(key));

        assert!(global.bind_frame(key, &mut g3).is_ok());

        let err = global
            .bind_frame(ReadonlyCacheKey::new(7, 12), &mut g3)
            .unwrap_err();
        assert!(matches!(err, Error::InvalidState));

        drop(g3);
        assert_eq!(global.invalidate_key(&key), Some(3));
        assert_eq!(global.allocated(), 0);
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
        let global_guard = (*global).guard();
        let k1 = ReadonlyCacheKey::new(1, 10);
        let k2 = ReadonlyCacheKey::new(1, 11);
        let k3 = ReadonlyCacheKey::new(2, 20);

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
        let global_guard = (*global).guard();
        let catalog_key = ReadonlyCacheKey::new(USER_OBJ_ID_START - 1, 42);
        let user_key = ReadonlyCacheKey::new(USER_OBJ_ID_START, 42);

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
        let global_guard = (*global).guard();
        let key = ReadonlyCacheKey::new(9, 77);

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
            let global_guard = (*global).guard();
            let key = ReadonlyCacheKey::new(10, 99);

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
            let foreign_guard = (*global2).guard();
            let _ = global1
                .get_page_internal::<Page>(&foreign_guard, 0, LatchFallbackMode::Shared)
                .await
                .unwrap();
        });
    }

    #[test]
    fn test_readonly_pool_reloads_when_mapping_points_to_uninitialized_frame() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let fs = TableFileSystemConfig::default()
                .data_dir(temp_dir.path())
                .build()
                .unwrap();
            let table_file = fs.create_table_file(111, make_metadata(), false).unwrap();
            let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
            drop(old_root);
            write_payload(&table_file, 9, b"reload").await;

            let global = owned_global_pool(frame_page_bytes(2));
            let global_guard = (*global).guard();
            let key = ReadonlyCacheKey::new(111, 9);

            let mut g0 = global.try_lock_page_exclusive(&global_guard, 0).unwrap();
            global.bind_frame(key, &mut g0).unwrap();
            g0.page_mut().zero();
            let frame = g0.bf_mut();
            frame.clear_readonly_key();
            frame.bump_generation();
            frame.set_kind(FrameKind::Uninitialized);
            drop(g0);

            let pool = owned_readonly_pool(
                111,
                PersistedFileKind::TableFile,
                Arc::clone(&table_file),
                &global,
            );
            let pool_guard = (*pool).guard();
            let page: FacadePageGuard<Page> = pool
                .get_page::<Page>(&pool_guard, 9, LatchFallbackMode::Shared)
                .await;
            let page = page.lock_shared_async().await.unwrap();
            assert_eq!(&page.page()[..6], b"reload");
            drop(page);

            let reloaded_frame_id = global.try_get_frame_id(&key).unwrap();
            assert_ne!(reloaded_frame_id, 0);
            drop(table_file);
            drop(fs);
        });
    }

    #[test]
    fn test_readonly_pool_miss_load_and_hit() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let fs = TableFileSystemConfig::default()
                .data_dir(temp_dir.path())
                .build()
                .unwrap();
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
            let pool_guard = (*pool).guard();
            let page: FacadePageGuard<Page> = pool
                .get_page::<Page>(&pool_guard, 3, LatchFallbackMode::Shared)
                .await;
            let page = page.lock_shared_async().await.unwrap();
            assert_eq!(&page.page()[..5], b"hello");
            drop(page);

            let page: FacadePageGuard<Page> = pool
                .get_page::<Page>(&pool_guard, 3, LatchFallbackMode::Shared)
                .await;
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
            let temp_dir = TempDir::new().unwrap();
            let fs = TableFileSystemConfig::default()
                .data_dir(temp_dir.path())
                .build()
                .unwrap();
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
            let pool_guard = (*pool).guard();

            let mut tasks = vec![];
            for _ in 0..16 {
                let pool = (*pool).clone();
                let pool_guard = pool_guard.clone();
                tasks.push(smol::spawn(async move {
                    let g: FacadePageGuard<Page> = pool
                        .get_page::<Page>(&pool_guard, 5, LatchFallbackMode::Shared)
                        .await;
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
            let mut page = [0u8; PAGE_SIZE];
            page[..5].copy_from_slice(b"hello");
            let page_source = Arc::new(ControlledPageSource::with_page(page));

            let global = owned_global_pool(frame_page_bytes(2));
            let pool = owned_readonly_pool(
                112,
                PersistedFileKind::TableFile,
                Arc::clone(&page_source),
                &global,
            );
            let pool_guard = (*pool).guard();
            let key = ReadonlyCacheKey::new(112, 5);

            let pool_for_loader = (*pool).clone();
            let loader = smol::spawn(async move {
                let _ = pool_for_loader
                    .get_page_facade::<Page>(&pool_guard, 5, LatchFallbackMode::Shared)
                    .await
                    .unwrap();
            });
            page_source.wait_started(1).await;
            assert!(global.inflight_loads.contains_key(&key));

            drop(loader);

            let pool_guard = (*pool).guard();
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

            page_source.release();

            assert_eq!(waiter.await, b"hello");
            assert_eq!(page_source.call_count(), 1);
            assert_eq!(global.allocated(), 1);
            assert!(global.try_get_frame_id(&key).is_some());
            assert!(!global.inflight_loads.contains_key(&key));
        });
    }

    #[test]
    fn test_readonly_pool_cancelled_single_loader_does_not_leak_completed_inflight() {
        smol::block_on(async {
            let mut page = [0u8; PAGE_SIZE];
            page[..4].copy_from_slice(b"solo");
            let page_source = Arc::new(ControlledPageSource::with_page(page));

            let global = owned_global_pool(frame_page_bytes(2));
            let pool = owned_readonly_pool(
                113,
                PersistedFileKind::TableFile,
                Arc::clone(&page_source),
                &global,
            );
            let pool_guard = (*pool).guard();
            let key = ReadonlyCacheKey::new(113, 9);

            let pool_for_loader = (*pool).clone();
            let loader = smol::spawn(async move {
                let _ = pool_for_loader
                    .get_page_facade::<Page>(&pool_guard, 9, LatchFallbackMode::Shared)
                    .await
                    .unwrap();
            });
            page_source.wait_started(1).await;
            assert!(global.inflight_loads.contains_key(&key));

            drop(loader);
            page_source.release();

            let pool_guard = (*pool).guard();
            wait_for(|| {
                !global.inflight_loads.contains_key(&key) && global.try_get_frame_id(&key).is_some()
            })
            .await;

            assert_eq!(page_source.call_count(), 1);
            assert_eq!(global.allocated(), 1);
            let g = pool
                .get_page_facade::<Page>(&pool_guard, 9, LatchFallbackMode::Shared)
                .await
                .unwrap();
            let g = g.lock_shared_async().await.unwrap();
            assert_eq!(&g.page()[..4], b"solo");
        });
    }

    #[test]
    fn test_readonly_pool_detached_miss_load_survives_pool_drop() {
        smol::block_on(async {
            let mut page = [0u8; PAGE_SIZE];
            page[..4].copy_from_slice(b"drop");
            let page_source = Arc::new(ControlledPageSource::with_page(page));
            let key = ReadonlyCacheKey::new(116, 12);

            let global = owned_global_pool(frame_page_bytes(2));
            let pool = owned_readonly_pool(
                116,
                PersistedFileKind::TableFile,
                Arc::clone(&page_source),
                &global,
            );
            let pool_guard = (*pool).guard();
            let inflight_loads = Arc::clone(&global.inflight_loads);
            let mappings = Arc::clone(&global.mappings);

            let pool_for_loader = (*pool).clone();
            let loader = smol::spawn(async move {
                let _ = pool_for_loader
                    .get_page_facade::<Page>(&pool_guard, 12, LatchFallbackMode::Shared)
                    .await
                    .unwrap();
            });
            page_source.wait_started(1).await;
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

            page_source.release();
            wait_for(|| !inflight_loads.contains_key(&key) && mappings.contains_key(&key)).await;
            teardown.join().unwrap();
            assert_eq!(page_source.call_count(), 1);
            assert!(mappings.contains_key(&key));
            assert!(dropped.load(Ordering::SeqCst));
        });
    }

    #[test]
    fn test_readonly_pool_drop_unblocks_detached_reserve_waiter() {
        smol::block_on(async {
            let page_source = Arc::new(ControlledPageSource::with_page([0u8; PAGE_SIZE]));
            let key = ReadonlyCacheKey::new(117, 13);
            let global = owned_global_pool(frame_page_bytes(1));
            let inflight_loads = Arc::clone(&global.inflight_loads);

            {
                let mut free = global.residency.free.lock();
                free.clear();
            }

            let page_source_dyn: Arc<dyn ReadonlyPageSource> = page_source.clone();
            let _inflight = global.join_or_start_inflight_load(key, page_source_dyn, None);
            wait_for(|| {
                inflight_loads.contains_key(&key)
                    && page_source.call_count() == 0
                    && global.residency.alloc_failure_rate() > 0.0
            })
            .await;

            let dropped = Arc::new(AtomicBool::new(false));
            let dropped_flag = Arc::clone(&dropped);
            let teardown = thread::spawn(move || {
                drop(global);
                dropped_flag.store(true, Ordering::SeqCst);
            });

            wait_for(|| dropped.load(Ordering::SeqCst)).await;
            teardown.join().unwrap();

            assert_eq!(page_source.call_count(), 0);
            assert!(!inflight_loads.contains_key(&key));
        });
    }

    #[test]
    fn test_readonly_pool_shared_io_failure_propagates_to_all_waiters() {
        smol::block_on(async {
            let page_source = Arc::new(ControlledPageSource::with_error(Error::IOError));

            let global = owned_global_pool(frame_page_bytes(2));
            let pool = owned_readonly_pool(
                114,
                PersistedFileKind::TableFile,
                Arc::clone(&page_source),
                &global,
            );
            let pool_guard = (*pool).guard();
            let key = ReadonlyCacheKey::new(114, 7);

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

            page_source.wait_started(1).await;
            smol::Timer::after(Duration::from_millis(10)).await;
            page_source.release();

            assert!(matches!(waiter1.await, Err(Error::IOError)));
            assert!(matches!(waiter2.await, Err(Error::IOError)));
            assert_eq!(page_source.call_count(), 1);
            assert_eq!(global.allocated(), 0);
            assert_eq!(global.try_get_frame_id(&key), None);
            assert!(!global.inflight_loads.contains_key(&key));
        });
    }

    #[test]
    fn test_readonly_pool_shared_validated_load_propagates_validation_failure() {
        smol::block_on(async {
            let mut page = build_valid_persisted_lwc_page();
            let last_idx = page.len() - 1;
            page[last_idx] ^= 0xFF;
            let page_source = Arc::new(ControlledPageSource::with_page(page_from_bytes(&page)));

            let global = owned_global_pool(frame_page_bytes(2));
            let pool = owned_readonly_pool(
                115,
                PersistedFileKind::TableFile,
                Arc::clone(&page_source),
                &global,
            );
            let _pool_guard = (*pool).guard();
            let key = ReadonlyCacheKey::new(115, 8);

            let pool_1 = (*pool).clone();
            let waiter1 = smol::spawn(async move {
                pool_1
                    .try_get_validated_page_shared(8, validate_persisted_lwc_page)
                    .await
            });
            let pool_2 = (*pool).clone();
            let waiter2 = smol::spawn(async move {
                pool_2
                    .try_get_validated_page_shared(8, validate_persisted_lwc_page)
                    .await
            });

            page_source.wait_started(1).await;
            smol::Timer::after(Duration::from_millis(10)).await;
            page_source.release();

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
            assert_eq!(page_source.call_count(), 1);
            assert_eq!(global.allocated(), 0);
            assert_eq!(global.try_get_frame_id(&key), None);
            assert!(!global.inflight_loads.contains_key(&key));
        });
    }

    #[test]
    fn test_readonly_pool_validated_lwc_miss_rejects_corruption_without_mapping() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let fs = TableFileSystemConfig::default()
                .data_dir(temp_dir.path())
                .build()
                .unwrap();
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
            let key = ReadonlyCacheKey::new(107, 9);

            let err = match pool
                .try_get_validated_page_shared(9, validate_persisted_lwc_page)
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
            let temp_dir = TempDir::new().unwrap();
            let fs = TableFileSystemConfig::default()
                .data_dir(temp_dir.path())
                .build()
                .unwrap();
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
            let key = ReadonlyCacheKey::new(108, 10);

            let err = match pool
                .try_get_validated_page_shared(10, validate_persisted_column_block_index_page)
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
            let temp_dir = TempDir::new().unwrap();
            let fs = TableFileSystemConfig::default()
                .data_dir(temp_dir.path())
                .build()
                .unwrap();
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
            let key = ReadonlyCacheKey::new(109, 11);

            let err = match pool
                .try_get_validated_page_shared(11, validate_persisted_blob_page)
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
            let temp_dir = TempDir::new().unwrap();
            let fs = TableFileSystemConfig::default()
                .data_dir(temp_dir.path())
                .build()
                .unwrap();
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
            let pool_guard = (*pool).guard();
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
                    .await;
                let g = g.lock_shared_async().await.unwrap();
                assert_eq!(&g.page()[..expected.len()], expected.as_bytes());
                drop(g);
            }

            let loaded_count = capacity + 1;
            let mapped_count = (0..=capacity)
                .filter(|i| {
                    let key = ReadonlyCacheKey::new(103, base_page_id + *i as u64);
                    global.try_get_frame_id(&key).is_some()
                })
                .count();
            assert!(mapped_count < loaded_count);

            // Reload the first page after cache pressure; this should still return correct data.
            let g: FacadePageGuard<Page> = pool
                .get_page::<Page>(&pool_guard, base_page_id, LatchFallbackMode::Shared)
                .await;
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
            let temp_dir = TempDir::new().unwrap();
            let global = owned_global_pool(frame_page_bytes(2));
            let fs = TableFileSystemConfig::default()
                .data_dir(temp_dir.path())
                .build()
                .unwrap();
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
            let pool_guard = (*pool).guard();

            let g: FacadePageGuard<Page> = pool
                .get_page::<Page>(&pool_guard, 9, LatchFallbackMode::Shared)
                .await;
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
            let temp_dir = TempDir::new().unwrap();
            let fs = TableFileSystemConfig::default()
                .data_dir(temp_dir.path())
                .build()
                .unwrap();
            let table_file = fs.create_table_file(104, make_metadata(), false).unwrap();
            let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
            drop(old_root);

            let global = owned_global_pool(64 * 1024 * 1024);
            let pool = owned_readonly_pool(104, PersistedFileKind::TableFile, table_file, &global);
            let pool_guard = (*pool).guard();
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
