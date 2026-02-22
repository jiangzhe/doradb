use crate::buffer::BufferPool;
use crate::buffer::frame::{BufferFrame, BufferFrames, FrameKind};
use crate::buffer::guard::{FacadePageGuard, PageExclusiveGuard, PageSharedGuard};
use crate::buffer::page::{BufferPage, PAGE_SIZE, Page, PageID, VersionedPageID};
use crate::buffer::util::{
    ClockHand, clock_collect, clock_sweep_candidate, deallocate_frame_and_page_arrays,
    initialize_frame_and_page_arrays, madvise_dontneed,
};
use crate::catalog::TableID;
use crate::error::Validation::Valid;
use crate::error::{Error, Result, Validation};
use crate::file::table_file::TableFile;
use crate::latch::LatchFallbackMode;
use crate::lifetime::StaticLifetime;
use crate::ptr::UnsafePtr;
use crate::thread;
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use event_listener::{Event, Listener, listener};
use parking_lot::Mutex;
use std::collections::BTreeSet;
use std::mem;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

const EVICT_CHECK_INTERVAL: Duration = Duration::from_secs(1);
const EVICT_BATCH_SIZE: usize = 64;

/// Physical cache identity for readonly table-file pages.
///
/// This intentionally excludes root version to preserve cache hits across
/// root swaps when physical blocks are unchanged.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ReadonlyCacheKey {
    /// Table identity owning the block.
    pub table_id: TableID,
    /// Physical page/block id in the table file.
    pub block_id: PageID,
}

impl ReadonlyCacheKey {
    /// Builds a key from table id and physical block id.
    #[inline]
    pub fn new(table_id: TableID, block_id: PageID) -> Self {
        ReadonlyCacheKey { table_id, block_id }
    }
}

/// Global readonly cache owner shared across tables.
///
/// This type owns the frame/page arena and maintains a forward mapping
/// from physical on-disk identity to in-memory frame id.
///
/// Reverse lookup is stored inline in `BufferFrame` as readonly key metadata.
pub struct GlobalReadonlyBufferPool {
    frames: BufferFrames,
    pages: *mut Page,
    size: usize,
    mappings: Arc<DashMap<ReadonlyCacheKey, PageID>>,
    inflight_loads: Arc<DashMap<ReadonlyCacheKey, Arc<InflightLoad>>>,
    residency: Arc<ReadonlyResidency>,
    shutdown_flag: Arc<AtomicBool>,
    evict_thread: Mutex<Option<JoinHandle<()>>>,
}

impl GlobalReadonlyBufferPool {
    /// Creates a global readonly pool with a target memory budget in bytes.
    #[inline]
    pub fn with_capacity(pool_size: usize) -> Result<Self> {
        let frame_plus_page = mem::size_of::<BufferFrame>() + mem::size_of::<Page>();
        let size = pool_size / frame_plus_page;
        if size == 0 {
            return Err(Error::BufferPoolSizeTooSmall);
        }
        // SAFETY: memory regions are released in `Drop`, and frame/page pointers are
        // initialized before exposure.
        let (frames, pages) = unsafe { initialize_frame_and_page_arrays(size)? };
        let pool = GlobalReadonlyBufferPool {
            frames: BufferFrames(frames),
            pages,
            size,
            mappings: Arc::new(DashMap::new()),
            inflight_loads: Arc::new(DashMap::new()),
            residency: Arc::new(ReadonlyResidency::new(size)),
            shutdown_flag: Arc::new(AtomicBool::new(false)),
            evict_thread: Mutex::new(None),
        };
        pool.start_evictor_thread();
        Ok(pool)
    }

    /// Creates and leaks a global readonly pool for static-lifetime usage.
    #[inline]
    pub fn with_capacity_static(pool_size: usize) -> Result<&'static Self> {
        let pool = Self::with_capacity(pool_size)?;
        Ok(StaticLifetime::new_static(pool))
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
        let frame = self.frames.frame(frame_id);
        frame
            .readonly_key()
            .map(|(table_id, block_id)| ReadonlyCacheKey::new(table_id, block_id))
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
        let expected_frame = self.frames.frame(frame_id) as *const BufferFrame;
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
                    Some((table_id, block_id))
                        if table_id == key.table_id && block_id == key.block_id =>
                    {
                        Ok(())
                    }
                    _ => Err(Error::InvalidState),
                };
            }
            Entry::Vacant(vac) => {
                if let Some((table_id, block_id)) = frame.readonly_key() {
                    if table_id != key.table_id || block_id != key.block_id {
                        return Err(Error::InvalidState);
                    }
                    return Err(Error::InvalidState);
                }
                vac.insert(frame_id);
                true
            }
        };
        if inserted {
            frame.set_readonly_key(key.table_id, key.block_id);
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

    /// Invalidates one physical block from one table.
    #[inline]
    pub fn invalidate_block(&self, table_id: TableID, block_id: PageID) -> Option<PageID> {
        self.invalidate_key(&ReadonlyCacheKey::new(table_id, block_id))
    }

    /// Invalidates one physical block from one table using strict GC ordering.
    #[inline]
    pub fn invalidate_block_strict(&self, table_id: TableID, block_id: PageID) -> Option<PageID> {
        self.invalidate_key_strict(&ReadonlyCacheKey::new(table_id, block_id))
    }

    /// Invalidates all cache entries belonging to one table.
    ///
    /// Returns the number of invalidated mappings.
    #[inline]
    pub fn invalidate_table(&self, table_id: TableID) -> usize {
        let keys = self
            .mappings
            .iter()
            .filter_map(|entry| {
                if entry.key().table_id == table_id {
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

    /// Invalidates all cache entries of one table using strict GC ordering.
    ///
    /// This method panics if any target frame is still latch-held.
    #[inline]
    pub fn invalidate_table_strict(&self, table_id: TableID) -> usize {
        let keys = self
            .mappings
            .iter()
            .filter_map(|entry| {
                if entry.key().table_id == table_id {
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
    fn start_evictor_thread(&self) {
        let evictor = ReadonlyBufferPoolEvictor {
            frames: BufferFrames(self.frames.0),
            mappings: Arc::clone(&self.mappings),
            residency: Arc::clone(&self.residency),
            shutdown_flag: Arc::clone(&self.shutdown_flag),
        };
        let handle = evictor.start_thread();
        let mut g = self.evict_thread.lock();
        *g = Some(handle);
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
            && let Some((table_id, block_id)) = frame.readonly_key()
        {
            debug_assert_eq!(table_id, key.table_id);
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
            if let Some(page_guard) = self.frames.try_lock_page_exclusive(frame_id) {
                self.invalidate_frame_with_guard(page_guard, expected_key);
                let _ = self.residency.move_resident_to_free(frame_id);
                return;
            }
            std::hint::spin_loop();
        }
    }

    #[inline]
    fn invalidate_frame_strict(&self, frame_id: PageID, expected_key: Option<ReadonlyCacheKey>) {
        let page_guard = self
            .frames
            .try_lock_page_exclusive(frame_id)
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
    async fn reserve_frame_id(&self) -> PageID {
        loop {
            if let Some(frame_id) = self.residency.try_reserve_frame() {
                return frame_id;
            }
            listener!(self.residency.free_ev => listener);
            if let Some(frame_id) = self.residency.try_reserve_frame() {
                return frame_id;
            }
            self.residency.evict_ev.notify(1);
            listener.await;
        }
    }

    #[inline]
    async fn load_cache_miss(
        &'static self,
        key: ReadonlyCacheKey,
        table_file: &TableFile,
    ) -> Result<PageID> {
        let frame_id = self.reserve_frame_id().await;
        let mut page_guard = self
            .frames
            .try_lock_page_exclusive(frame_id)
            .expect("reserved frame must be lockable");
        {
            let frame = page_guard.bf_mut();
            frame.clear_readonly_key();
            frame.set_dirty(false);
            frame.set_kind(FrameKind::Evicting);
        }

        // SAFETY: frame page memory is exclusively held by `page_guard`, aligned to page size,
        // and remains valid until IO completion because the guard is held for the full await.
        let read_res = unsafe {
            table_file
                .read_page_into_ptr(
                    key.block_id,
                    UnsafePtr(page_guard.page_mut() as *mut Page as *mut u8),
                )
                .await
        };

        if let Err(err) = read_res {
            page_guard.page_mut().zero();
            let frame = page_guard.bf_mut();
            frame.clear_readonly_key();
            frame.bump_generation();
            frame.set_kind(FrameKind::Uninitialized);
            drop(page_guard);
            self.residency.release_free(frame_id);
            return Err(err);
        }

        {
            let frame = page_guard.bf_mut();
            frame.set_readonly_key(key.table_id, key.block_id);
            frame.set_dirty(false);
            frame.bump_generation();
            frame.set_kind(FrameKind::Hot);
        }
        match self.mappings.entry(key) {
            Entry::Vacant(vac) => {
                vac.insert(frame_id);
            }
            Entry::Occupied(_) => {
                panic!(
                    "readonly cache key already loaded unexpectedly: table_id={}, block_id={}",
                    key.table_id, key.block_id
                );
            }
        }
        drop(page_guard);
        self.residency.mark_resident(frame_id);
        if self.residency.no_free_frame() {
            self.residency.evict_ev.notify(1);
        }
        Ok(frame_id)
    }

    #[inline]
    async fn get_or_load_frame_id(
        &'static self,
        key: ReadonlyCacheKey,
        table_file: &Arc<TableFile>,
    ) -> PageID {
        loop {
            if let Some(frame_id) = self.try_get_frame_id(&key) {
                return frame_id;
            }

            let (is_loader, inflight) = match self.inflight_loads.entry(key) {
                Entry::Vacant(vac) => {
                    let inflight = Arc::new(InflightLoad::new());
                    vac.insert(Arc::clone(&inflight));
                    (true, inflight)
                }
                Entry::Occupied(occ) => (false, Arc::clone(occ.get())),
            };

            if is_loader {
                let load_res = self.load_cache_miss(key, table_file).await;
                if let Some((_, inflight)) = self.inflight_loads.remove(&key) {
                    inflight.ev.notify(usize::MAX);
                }
                match load_res {
                    Ok(frame_id) => return frame_id,
                    Err(err) => {
                        panic!(
                            "readonly cache miss load failed: table_id={}, block_id={}, err={}",
                            key.table_id, key.block_id, err
                        );
                    }
                }
            } else {
                let listener = inflight.ev.listen();
                if self.try_get_frame_id(&key).is_some() || !self.inflight_loads.contains_key(&key)
                {
                    continue;
                }
                listener.await;
            }
        }
    }

    #[inline]
    async fn get_page_internal<T: 'static>(
        &'static self,
        frame_id: PageID,
        mode: LatchFallbackMode,
    ) -> FacadePageGuard<T> {
        let bf = self.frames.frame_ptr(frame_id);
        let g = BufferFrames::frame_ref(bf.clone())
            .latch
            .optimistic_fallback(mode)
            .await;
        FacadePageGuard::new(bf, g)
    }

    #[inline]
    fn validate_guarded_frame_key<T: 'static>(
        &self,
        guard: &FacadePageGuard<T>,
        expected_key: ReadonlyCacheKey,
    ) -> bool {
        let frame = guard.bf();
        frame.kind() != FrameKind::Uninitialized
            && frame.readonly_key() == Some((expected_key.table_id, expected_key.block_id))
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
        self.shutdown_flag.store(true, Ordering::SeqCst);
        self.residency.evict_ev.notify(1);
        {
            let mut g = self.evict_thread.lock();
            if let Some(handle) = g.take() {
                handle.join().unwrap();
            }
        }

        // SAFETY: thread shutdown above guarantees no background worker can access
        // frame/page memory while it is being reclaimed.
        unsafe {
            for frame_id in 0..self.size {
                let frame_ptr = self.frames.0.add(frame_id);
                std::ptr::drop_in_place(frame_ptr);
            }
            deallocate_frame_and_page_arrays(self.frames.0, self.pages, self.size);
        }
    }
}

unsafe impl Send for GlobalReadonlyBufferPool {}
unsafe impl Sync for GlobalReadonlyBufferPool {}
unsafe impl StaticLifetime for GlobalReadonlyBufferPool {}

struct InflightLoad {
    ev: Event,
}

impl InflightLoad {
    #[inline]
    fn new() -> Self {
        InflightLoad { ev: Event::new() }
    }
}

struct ReadonlyResidency {
    set: Mutex<BTreeSet<PageID>>,
    free: Mutex<Vec<PageID>>,
    free_ev: Event,
    evict_ev: Event,
}

impl ReadonlyResidency {
    #[inline]
    fn new(capacity: usize) -> Self {
        let mut free = Vec::with_capacity(capacity);
        for frame_id in 0..capacity {
            free.push(frame_id as PageID);
        }
        ReadonlyResidency {
            set: Mutex::new(BTreeSet::new()),
            free: Mutex::new(free),
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
    fn collect<F: FnMut(PageID) -> bool>(
        &self,
        hand: ClockHand,
        mut callback: F,
    ) -> Option<ClockHand> {
        let g = self.set.lock();
        clock_collect(&g, hand, &mut callback)
    }
}

struct ReadonlyBufferPoolEvictor {
    frames: BufferFrames,
    mappings: Arc<DashMap<ReadonlyCacheKey, PageID>>,
    residency: Arc<ReadonlyResidency>,
    shutdown_flag: Arc<AtomicBool>,
}

impl ReadonlyBufferPoolEvictor {
    #[inline]
    fn start_thread(self) -> JoinHandle<()> {
        thread::spawn_named("ReadonlyBufferPoolEvictor", move || self.run())
    }

    #[inline]
    fn run(self) {
        let mut clock_hand = ClockHand::default();
        let mut candidates = vec![];
        loop {
            if self.shutdown_flag.load(Ordering::Acquire) {
                return;
            }
            if !self.residency.no_free_frame() {
                listener!(self.residency.evict_ev => listener);
                if !self.residency.no_free_frame() {
                    listener.wait_timeout(EVICT_CHECK_INTERVAL);
                    continue;
                }
            }

            let target = self.residency.resident_len().min(EVICT_BATCH_SIZE);
            if target == 0 {
                listener!(self.residency.evict_ev => listener);
                listener.wait_timeout(EVICT_CHECK_INTERVAL);
                continue;
            }

            let mut next_ch = Some(clock_hand.clone());
            while candidates.len() < target {
                let Some(ch) = next_ch.take() else {
                    break;
                };
                next_ch = self.residency.collect(ch, |frame_id| {
                    if let Some(page_guard) = clock_sweep_candidate(&self.frames, frame_id) {
                        candidates.push(page_guard);
                        candidates.len() == target
                    } else {
                        false
                    }
                });
                if next_ch.is_none() {
                    break;
                }
            }

            if candidates.is_empty() {
                listener!(self.residency.evict_ev => listener);
                listener.wait_timeout(EVICT_CHECK_INTERVAL);
                continue;
            }

            for page_guard in candidates.drain(..) {
                self.drop_resident_page(page_guard);
            }

            if let Some(mut ch) = next_ch {
                ch.reset();
                clock_hand = ch;
            } else {
                clock_hand.reset();
            }
        }
    }

    #[inline]
    fn drop_resident_page(&self, mut page_guard: PageExclusiveGuard<Page>) {
        let frame_id = page_guard.page_id();
        let frame = page_guard.bf_mut();
        if frame.kind() != FrameKind::Evicting {
            return;
        }

        if let Some((table_id, block_id)) = frame.readonly_key() {
            let key = ReadonlyCacheKey::new(table_id, block_id);
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

unsafe impl Send for ReadonlyBufferPoolEvictor {}

/// Per-table readonly wrapper implementing the `BufferPool` contract.
///
/// This wrapper translates table-local `PageID` into global physical cache key
/// and delegates to `GlobalReadonlyBufferPool`.
#[derive(Clone)]
pub struct ReadonlyBufferPool {
    table_id: TableID,
    table_file: Arc<TableFile>,
    global: &'static GlobalReadonlyBufferPool,
}

impl ReadonlyBufferPool {
    /// Creates a per-table readonly pool wrapper.
    #[inline]
    pub fn new(
        table_id: TableID,
        table_file: Arc<TableFile>,
        global: &'static GlobalReadonlyBufferPool,
    ) -> Self {
        ReadonlyBufferPool {
            table_id,
            table_file,
            global,
        }
    }

    #[inline]
    fn cache_key(&self, block_id: PageID) -> ReadonlyCacheKey {
        ReadonlyCacheKey::new(self.table_id, block_id)
    }

    #[inline]
    async fn get_page_facade<T: BufferPage>(
        &self,
        page_id: PageID,
        mode: LatchFallbackMode,
    ) -> FacadePageGuard<T> {
        let key = self.cache_key(page_id);
        loop {
            let frame_id = self
                .global
                .get_or_load_frame_id(key, &self.table_file)
                .await;
            let guard = self.global.get_page_internal(frame_id, mode).await;
            if self.global.validate_guarded_frame_key(&guard, key) {
                return guard;
            }
            self.global
                .invalidate_stale_mapping_if_same_frame(key, frame_id);
        }
    }

    /// Returns a shared lock guard for a cached page.
    ///
    /// This helper is intended for runtime call sites that hold `&self`
    /// instead of `&'static self`.
    #[inline]
    pub(crate) async fn get_page_shared<T: BufferPage>(
        &self,
        page_id: PageID,
    ) -> PageSharedGuard<T> {
        loop {
            let guard = self
                .get_page_facade::<T>(page_id, LatchFallbackMode::Shared)
                .await;
            if let Some(shared) = guard.lock_shared_async().await {
                return shared;
            }
        }
    }

    /// Invalidates one block for this table from the global readonly cache.
    #[inline]
    pub fn invalidate_block_id(&self, block_id: PageID) -> Option<PageID> {
        self.global.invalidate_block(self.table_id, block_id)
    }

    /// Invalidates one block for this table with strict GC ordering preconditions.
    #[inline]
    pub fn invalidate_block_id_strict(&self, block_id: PageID) -> Option<PageID> {
        self.global.invalidate_block_strict(self.table_id, block_id)
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
    async fn allocate_page<T: BufferPage>(&'static self) -> PageExclusiveGuard<T> {
        panic!("readonly buffer pool does not support page allocation")
    }

    #[inline]
    async fn allocate_page_at<T: BufferPage>(
        &'static self,
        _page_id: PageID,
    ) -> Result<PageExclusiveGuard<T>> {
        panic!("readonly buffer pool does not support page allocation")
    }

    #[inline]
    async fn get_page<T: BufferPage>(
        &'static self,
        page_id: PageID,
        mode: LatchFallbackMode,
    ) -> FacadePageGuard<T> {
        self.get_page_facade(page_id, mode).await
    }

    #[inline]
    async fn try_get_page_versioned<T: BufferPage>(
        &'static self,
        _id: VersionedPageID,
        _mode: LatchFallbackMode,
    ) -> Option<FacadePageGuard<T>> {
        None
    }

    #[inline]
    fn deallocate_page<T: BufferPage>(&'static self, _g: PageExclusiveGuard<T>) {
        panic!("readonly buffer pool does not support page deallocation")
    }

    #[inline]
    async fn get_child_page<T: BufferPage>(
        &'static self,
        p_guard: &FacadePageGuard<T>,
        page_id: PageID,
        mode: LatchFallbackMode,
    ) -> Validation<FacadePageGuard<T>> {
        let g = self.get_page::<T>(page_id, mode).await;
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
unsafe impl StaticLifetime for ReadonlyBufferPool {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::guard::{FacadePageGuard, PageGuard};
    use crate::buffer::page::Page;
    use crate::catalog::{ColumnAttributes, ColumnSpec, TableMetadata};
    use crate::file::table_fs::TableFileSystemConfig;
    use crate::io::AIOBuf;
    use crate::lifetime::{StaticLifetime, StaticLifetimeScope};
    use crate::value::ValKind;
    use tempfile::TempDir;

    fn frame_page_bytes(cap: usize) -> usize {
        cap * (mem::size_of::<BufferFrame>() + mem::size_of::<Page>())
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

    #[test]
    fn test_global_readonly_mapping_and_invalidation() {
        let scope = StaticLifetimeScope::new();
        let global =
            scope.adopt(GlobalReadonlyBufferPool::with_capacity_static(64 * 1024 * 1024).unwrap());
        let global = global.as_static();
        let key = ReadonlyCacheKey::new(7, 11);

        assert_eq!(global.allocated(), 0);
        let mut g3 = global.frames.try_lock_page_exclusive(3).unwrap();
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
    fn test_global_invalidate_table() {
        let scope = StaticLifetimeScope::new();
        let global =
            scope.adopt(GlobalReadonlyBufferPool::with_capacity_static(64 * 1024 * 1024).unwrap());
        let global = global.as_static();
        let k1 = ReadonlyCacheKey::new(1, 10);
        let k2 = ReadonlyCacheKey::new(1, 11);
        let k3 = ReadonlyCacheKey::new(2, 20);

        let mut g1 = global.frames.try_lock_page_exclusive(1).unwrap();
        let mut g2 = global.frames.try_lock_page_exclusive(2).unwrap();
        let mut g3 = global.frames.try_lock_page_exclusive(3).unwrap();
        global.bind_frame(k1, &mut g1).unwrap();
        global.bind_frame(k2, &mut g2).unwrap();
        global.bind_frame(k3, &mut g3).unwrap();
        drop(g1);
        drop(g2);
        drop(g3);

        assert_eq!(global.invalidate_table(1), 2);
        assert_eq!(global.try_get_frame_id(&k1), None);
        assert_eq!(global.try_get_frame_id(&k2), None);
        assert_eq!(global.try_get_frame_id(&k3), Some(3));
    }

    #[test]
    fn test_global_invalidate_key_strict() {
        let scope = StaticLifetimeScope::new();
        let global =
            scope.adopt(GlobalReadonlyBufferPool::with_capacity_static(64 * 1024 * 1024).unwrap());
        let global = global.as_static();
        let key = ReadonlyCacheKey::new(9, 77);

        let mut g = global.frames.try_lock_page_exclusive(5).unwrap();
        global.bind_frame(key, &mut g).unwrap();
        drop(g);

        assert_eq!(global.invalidate_key_strict(&key), Some(5));
        assert_eq!(global.try_get_frame_id(&key), None);
    }

    #[test]
    #[should_panic(expected = "strict invalidation lock acquisition failed")]
    fn test_global_invalidate_key_strict_panics_when_latch_held() {
        smol::block_on(async {
            let scope = StaticLifetimeScope::new();
            let global = scope
                .adopt(GlobalReadonlyBufferPool::with_capacity_static(64 * 1024 * 1024).unwrap());
            let global = global.as_static();
            let key = ReadonlyCacheKey::new(10, 99);

            let mut g = global.frames.try_lock_page_exclusive(6).unwrap();
            global.bind_frame(key, &mut g).unwrap();
            let shared = g.downgrade_shared();
            let _ = global.invalidate_key_strict(&key);
            drop(shared);
        });
    }

    #[test]
    fn test_readonly_pool_reloads_when_mapping_points_to_uninitialized_frame() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let fs = TableFileSystemConfig::default()
                .with_main_dir(temp_dir.path())
                .build()
                .unwrap();
            let table_file = fs.create_table_file(111, make_metadata(), false).unwrap();
            let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
            drop(old_root);
            write_payload(&table_file, 9, b"reload").await;

            let scope = StaticLifetimeScope::new();
            let global = scope.adopt(
                GlobalReadonlyBufferPool::with_capacity_static(frame_page_bytes(2)).unwrap(),
            );
            let global = global.as_static();
            let key = ReadonlyCacheKey::new(111, 9);

            let mut g0 = global.frames.try_lock_page_exclusive(0).unwrap();
            global.bind_frame(key, &mut g0).unwrap();
            g0.page_mut().zero();
            let frame = g0.bf_mut();
            frame.clear_readonly_key();
            frame.bump_generation();
            frame.set_kind(FrameKind::Uninitialized);
            drop(g0);

            let pool = scope.adopt(StaticLifetime::new_static(ReadonlyBufferPool::new(
                111,
                Arc::clone(&table_file),
                global,
            )));
            let page: FacadePageGuard<Page> = pool
                .as_static()
                .get_page::<Page>(9, LatchFallbackMode::Shared)
                .await;
            let page = page.lock_shared_async().await.unwrap();
            assert_eq!(&page.page()[..6], b"reload");
            drop(page);

            assert_eq!(global.try_get_frame_id(&key), Some(1));
            drop(table_file);
            drop(fs);
        });
    }

    #[test]
    fn test_readonly_pool_miss_load_and_hit() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let fs = TableFileSystemConfig::default()
                .with_main_dir(temp_dir.path())
                .build()
                .unwrap();
            let table_file = fs.create_table_file(101, make_metadata(), false).unwrap();
            let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
            drop(old_root);
            write_payload(&table_file, 3, b"hello").await;

            let scope = StaticLifetimeScope::new();
            let global = scope.adopt(
                GlobalReadonlyBufferPool::with_capacity_static(frame_page_bytes(4)).unwrap(),
            );
            let pool = scope.adopt(StaticLifetime::new_static(ReadonlyBufferPool::new(
                101,
                Arc::clone(&table_file),
                global.as_static(),
            )));
            let page: FacadePageGuard<Page> = pool
                .as_static()
                .get_page::<Page>(3, LatchFallbackMode::Shared)
                .await;
            let page = page.lock_shared_async().await.unwrap();
            assert_eq!(&page.page()[..5], b"hello");
            drop(page);

            let page: FacadePageGuard<Page> = pool
                .as_static()
                .get_page::<Page>(3, LatchFallbackMode::Shared)
                .await;
            let page = page.lock_shared_async().await.unwrap();
            assert_eq!(&page.page()[..5], b"hello");
            assert_eq!(global.as_static().allocated(), 1);
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
                .with_main_dir(temp_dir.path())
                .build()
                .unwrap();
            let table_file = fs.create_table_file(102, make_metadata(), false).unwrap();
            let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
            drop(old_root);
            write_payload(&table_file, 5, b"world").await;

            let scope = StaticLifetimeScope::new();
            let global = scope.adopt(
                GlobalReadonlyBufferPool::with_capacity_static(frame_page_bytes(8)).unwrap(),
            );
            let pool = scope.adopt(StaticLifetime::new_static(ReadonlyBufferPool::new(
                102,
                Arc::clone(&table_file),
                global.as_static(),
            )));
            let pool = pool.as_static();

            let mut tasks = vec![];
            for _ in 0..16 {
                tasks.push(smol::spawn(async move {
                    let g: FacadePageGuard<Page> =
                        pool.get_page::<Page>(5, LatchFallbackMode::Shared).await;
                    g.lock_shared_async().await.unwrap().page()[0]
                }));
            }
            for task in tasks {
                assert_eq!(task.await, b'w');
            }
            assert_eq!(global.as_static().allocated(), 1);
            drop(table_file);
            drop(fs);
        });
    }

    #[test]
    fn test_readonly_pool_drop_only_eviction_and_reload() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let fs = TableFileSystemConfig::default()
                .with_main_dir(temp_dir.path())
                .build()
                .unwrap();
            let table_file = fs.create_table_file(103, make_metadata(), false).unwrap();
            let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
            drop(old_root);
            write_payload(&table_file, 7, b"page-a").await;
            write_payload(&table_file, 8, b"page-b").await;

            let scope = StaticLifetimeScope::new();
            let global = scope.adopt(
                GlobalReadonlyBufferPool::with_capacity_static(frame_page_bytes(1)).unwrap(),
            );
            let pool = scope.adopt(StaticLifetime::new_static(ReadonlyBufferPool::new(
                103,
                Arc::clone(&table_file),
                global.as_static(),
            )));
            let pool = pool.as_static();

            let g1: FacadePageGuard<Page> =
                pool.get_page::<Page>(7, LatchFallbackMode::Shared).await;
            let g1 = g1.lock_shared_async().await.unwrap();
            assert_eq!(&g1.page()[..6], b"page-a");
            drop(g1);

            let g2: FacadePageGuard<Page> =
                pool.get_page::<Page>(8, LatchFallbackMode::Shared).await;
            let g2 = g2.lock_shared_async().await.unwrap();
            assert_eq!(&g2.page()[..6], b"page-b");
            drop(g2);

            let g3: FacadePageGuard<Page> =
                pool.get_page::<Page>(7, LatchFallbackMode::Shared).await;
            let g3 = g3.lock_shared_async().await.unwrap();
            assert_eq!(&g3.page()[..6], b"page-a");
            drop(g3);

            assert_eq!(global.as_static().allocated(), 1);
            drop(table_file);
            drop(fs);
        });
    }

    #[test]
    fn test_readonly_pool_lifecycle_drop_order_with_table_fs() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let fs = StaticLifetime::new_static(
                TableFileSystemConfig::default()
                    .with_main_dir(temp_dir.path())
                    .build()
                    .unwrap(),
            );
            let table_file = fs.create_table_file(105, make_metadata(), false).unwrap();
            let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
            drop(old_root);
            write_payload(&table_file, 9, b"drop-order").await;

            let scope = StaticLifetimeScope::new();
            let global = scope.adopt(
                GlobalReadonlyBufferPool::with_capacity_static(frame_page_bytes(2)).unwrap(),
            );
            // Adopt filesystem after global, so scope teardown stops file IO thread first.
            let _fs = scope.adopt(fs);
            let pool = scope.adopt(StaticLifetime::new_static(ReadonlyBufferPool::new(
                105,
                Arc::clone(&table_file),
                global.as_static(),
            )));

            let g: FacadePageGuard<Page> = pool
                .as_static()
                .get_page::<Page>(9, LatchFallbackMode::Shared)
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
                .with_main_dir(temp_dir.path())
                .build()
                .unwrap();
            let table_file = fs.create_table_file(104, make_metadata(), false).unwrap();
            let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
            drop(old_root);

            let scope = StaticLifetimeScope::new();
            let global = scope
                .adopt(GlobalReadonlyBufferPool::with_capacity_static(64 * 1024 * 1024).unwrap());
            let pool = scope.adopt(StaticLifetime::new_static(ReadonlyBufferPool::new(
                104,
                table_file,
                global.as_static(),
            )));
            let _ = pool.as_static().allocate_page::<Page>().await;
        });
    }
}
