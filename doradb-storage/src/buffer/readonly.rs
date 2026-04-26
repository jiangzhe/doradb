use crate::buffer::arena::{ArenaGuard, QuiescentArena};
use crate::buffer::evictor::{
    ClockHand, EvictionArbiter, EvictionArbiterBuilder, EvictionRuntime, FailureRateTracker,
    PressureDeltaClockPolicy, SharedEvictionDomain, SharedEvictionDomainId, clock_collect_batch,
    clock_sweep_candidate,
};
use crate::buffer::frame::{BufferFrame, FrameKind};
use crate::buffer::guard::{
    FacadePageGuard, PageExclusiveGuard, PageGuard, PageLatchGuard, PageSharedGuard,
};
use crate::buffer::load::{PageReservation, PageReservationGuard};
use crate::buffer::page::{BufferPage, PAGE_SIZE, Page, PageID};
use crate::buffer::util::madvise_dontneed;
use crate::buffer::{
    BufferPoolStats, BufferPoolStatsHandle, PageIOCompletion, PoolGuard, PoolIdentity, PoolRole,
    ReadonlyBlockValidator,
};
use crate::error::{
    CompletionErrorKind, CompletionResult, Error, FileKind, InternalError, LifecycleError,
    ResourceError, Result,
};
use crate::file::fs::FileSystem;
use crate::file::{BlockID, BlockKey, FileID, SparseFile};
use crate::io::{IOKind, IOSubmission, Operation, StdIoResult};
use crate::latch::LatchFallbackMode;
use crate::quiescent::{QuiescentGuard, SyncQuiescentGuard};
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use error_stack::Report;
use event_listener::{Event, EventListener, listener};
use parking_lot::Mutex;
use std::collections::BTreeSet;
use std::mem;
use std::os::fd::AsRawFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// Minimum number of frames required for global readonly pool.
///
/// Very small pools provide little practical value and can stall eviction/load flow.
const MIN_READONLY_POOL_PAGES: usize = 256;

/// Global readonly cache owner shared across files.
///
/// This type owns the frame/page arena and maintains a forward mapping
/// from physical on-disk identity to in-memory frame id.
///
/// Reverse lookup is stored inline in `BufferFrame` as persisted-block metadata.
pub struct ReadonlyBufferPool {
    size: usize,
    mappings: DashMap<BlockKey, PageID>,
    inflight_loads: DashMap<BlockKey, Arc<PageIOCompletion>>,
    residency: ReadonlyResidency,
    eviction_arbiter: EvictionArbiter,
    fs: QuiescentGuard<FileSystem>,
    shutdown_flag: Arc<AtomicBool>,
    role: PoolRole,
    stats: BufferPoolStatsHandle,
    arena: QuiescentArena,
}

impl ReadonlyBufferPool {
    /// Creates a raw global readonly pool with a target memory budget in bytes.
    ///
    /// This constructor intentionally does not start the eviction worker.
    /// Callers must first place the pool into a stable owner such as
    /// [`QuiescentBox`] before starting guarded worker threads.
    #[inline]
    pub fn with_capacity(
        role: PoolRole,
        pool_size: usize,
        fs: QuiescentGuard<FileSystem>,
    ) -> Result<Self> {
        Self::with_capacity_and_arbiter_builder(role, pool_size, fs, EvictionArbiter::builder())
    }

    /// Creates a raw global readonly pool with explicit eviction arbiter builder.
    ///
    /// This constructor intentionally does not start the eviction worker.
    #[inline]
    pub fn with_capacity_and_arbiter_builder(
        role: PoolRole,
        pool_size: usize,
        fs: QuiescentGuard<FileSystem>,
        eviction_arbiter_builder: EvictionArbiterBuilder,
    ) -> Result<Self> {
        role.assert_valid("global readonly buffer pool");
        let frame_plus_page = mem::size_of::<BufferFrame>() + mem::size_of::<Page>();
        let size = pool_size / frame_plus_page;
        if size < MIN_READONLY_POOL_PAGES {
            return Err(Report::new(ResourceError::BufferPoolSizeTooSmall)
                .attach(format!(
                    "global readonly buffer pool sizing: role={role:?}, pool_size={pool_size}, frame_plus_page={frame_plus_page}, pages={size}, min_pages={MIN_READONLY_POOL_PAGES}"
                ))
                .into());
        }
        let eviction_arbiter = eviction_arbiter_builder.build(size);
        let arena = QuiescentArena::new(size)?;
        let pool = ReadonlyBufferPool {
            size,
            mappings: DashMap::new(),
            inflight_loads: DashMap::new(),
            residency: ReadonlyResidency::new(size, eviction_arbiter),
            eviction_arbiter,
            fs,
            shutdown_flag: Arc::new(AtomicBool::new(false)),
            role,
            stats: BufferPoolStatsHandle::default(),
            arena,
        };
        Ok(pool)
    }

    #[inline]
    async fn send_read_async(
        &self,
        req: ReadSubmission,
    ) -> std::result::Result<(), flume::SendError<ReadSubmission>> {
        self.fs.send_table_read_async(req).await
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
                return Err(Report::new(LifecycleError::Shutdown).into());
            }
            if let Some(frame_id) = self.residency.try_reserve_frame() {
                // Like the mutable pool, terminal shutdown does not try to reclaim
                // successful reservations. Only blocking wait paths must exit.
                self.residency.record_alloc_success();
                return Ok(frame_id);
            }
            self.residency.record_alloc_failure();
            listener!(self.residency.free_ev => listener);
            // Check after listener registration so shutdown cannot strand this waiter.
            if self.shutdown_flag.load(Ordering::Acquire) {
                return Err(Report::new(LifecycleError::Shutdown).into());
            }
            if let Some(frame_id) = self.residency.try_reserve_frame() {
                self.residency.record_alloc_success();
                return Ok(frame_id);
            }
            self.residency.record_alloc_failure();
            self.residency.notify_evictor();
            listener.await;
            // Wakeups can come from shutdown as well as real frame reclamation.
            if self.shutdown_flag.load(Ordering::Acquire) {
                return Err(Report::new(LifecycleError::Shutdown).into());
            }
        }
    }

    #[inline]
    fn complete_inflight_load(
        &self,
        key: BlockKey,
        inflight: &Arc<PageIOCompletion>,
        result: CompletionResult<PageID>,
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
    pub fn try_get_frame_id(&self, key: &BlockKey) -> Option<PageID> {
        self.mappings.get(key).map(|v| *v)
    }

    /// Looks up persisted-block identity by frame id.
    #[inline]
    pub fn try_get_block_key(&self, frame_id: PageID) -> Option<BlockKey> {
        if usize::from(frame_id) >= self.size {
            return None;
        }
        let frame = self.arena.frame(frame_id);
        frame
            .persisted_block_key()
            .map(|(file_id, block_id)| BlockKey::new(file_id, block_id))
    }

    /// Binds one persisted-block key to an exclusively locked frame.
    ///
    /// Binding is idempotent for the same key/frame pair and returns
    /// Returns an internal mapping-conflict error for conflicting mapping attempts.
    #[inline]
    pub fn bind_frame(
        &self,
        key: BlockKey,
        frame_guard: &mut PageExclusiveGuard<Page>,
    ) -> Result<()> {
        let frame_id = frame_guard.page_id();
        if usize::from(frame_id) >= self.size {
            return Err(Report::new(InternalError::ReadonlyFrameIndexOutOfBounds)
                .attach(format!("frame_id={frame_id}, size={}", self.size))
                .into());
        }
        let frame = frame_guard.bf_mut();
        let expected_frame = self.arena.frame(frame_id) as *const BufferFrame;
        if !std::ptr::eq(frame as *const BufferFrame, expected_frame) {
            return Err(Report::new(InternalError::ReadonlyFrameGuardMismatch)
                .attach(format!("frame_id={frame_id}"))
                .into());
        }
        let inserted = match self.mappings.entry(key) {
            Entry::Occupied(occ) => {
                let existing = *occ.get();
                if existing != frame_id {
                    return Err(Report::new(InternalError::ReadonlyMappingConflict)
                        .attach(format!(
                            "key={:?}, existing_frame={}, new_frame={frame_id}",
                            key, existing
                        ))
                        .into());
                }
                return match frame.persisted_block_key() {
                    Some((file_id, block_id))
                        if file_id == key.file_id && block_id == key.block_id =>
                    {
                        Ok(())
                    }
                    _ => Err(Report::new(InternalError::ReadonlyMappingConflict)
                        .attach(format!("key={key:?}, frame_id={frame_id}"))
                        .into()),
                };
            }
            Entry::Vacant(vac) => {
                if let Some((file_id, block_id)) = frame.persisted_block_key() {
                    if file_id != key.file_id || block_id != key.block_id {
                        return Err(Report::new(InternalError::ReadonlyMappingConflict)
                            .attach(format!(
                                "key={key:?}, frame_key={:?}",
                                BlockKey::new(file_id, block_id)
                            ))
                            .into());
                    }
                    return Err(Report::new(InternalError::ReadonlyMappingConflict)
                        .attach(format!(
                            "duplicate frame binding: key={key:?}, frame_id={frame_id}"
                        ))
                        .into());
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
    pub fn invalidate_key(&self, key: &BlockKey) -> Option<PageID> {
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
    pub fn invalidate_key_strict(&self, key: &BlockKey) -> Option<PageID> {
        let frame_id = match self.mappings.remove(key) {
            Some((_, frame_id)) => frame_id,
            None => return None,
        };
        self.invalidate_frame_strict(frame_id, Some(*key));
        Some(frame_id)
    }

    /// Invalidates one physical block from one file.
    #[inline]
    pub fn invalidate_file_block(&self, file_id: FileID, block_id: BlockID) -> Option<PageID> {
        self.invalidate_key(&BlockKey::new(file_id, block_id))
    }

    /// Invalidates one physical block from one file using strict GC ordering.
    #[inline]
    pub fn invalidate_file_block_strict(
        &self,
        file_id: FileID,
        block_id: BlockID,
    ) -> Option<PageID> {
        self.invalidate_key_strict(&BlockKey::new(file_id, block_id))
    }

    /// Invalidates all cache entries belonging to one file.
    ///
    /// Returns the number of invalidated mappings.
    #[inline]
    pub fn invalidate_file(&self, file_id: FileID) -> usize {
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
    pub fn invalidate_file_strict(&self, file_id: FileID) -> usize {
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
    fn evictor_parts(
        pool: SyncQuiescentGuard<Self>,
    ) -> (ReadonlyRuntime, PressureDeltaClockPolicy) {
        let policy = PressureDeltaClockPolicy::new(pool.eviction_arbiter, 1);
        let runtime = ReadonlyRuntime {
            arena: pool.arena.arena_guard(pool.pool_guard()),
            pool,
        };
        (runtime, policy)
    }

    #[inline]
    pub(super) fn signal_shutdown(&self) {
        self.shutdown_flag.store(true, Ordering::SeqCst);
        self.residency.free_ev.notify(usize::MAX);
        self.residency.notify_evictor();
    }

    #[inline]
    pub(super) fn install_shared_evictor_wake(&self, wake_event: Arc<Event>) {
        self.residency.install_shared_wake(wake_event);
    }

    #[inline]
    pub(super) fn shared_evictor_domain(pool: SyncQuiescentGuard<Self>) -> SharedEvictionDomain {
        let (runtime, policy) = Self::evictor_parts(pool);
        SharedEvictionDomain::new(SharedEvictionDomainId::Readonly, runtime, policy)
    }

    #[inline]
    fn invalidate_frame_with_guard(
        &self,
        mut page_guard: PageExclusiveGuard<Page>,
        expected_key: Option<BlockKey>,
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
    fn invalidate_frame_retry(&self, frame_id: PageID, expected_key: Option<BlockKey>) {
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
    fn invalidate_frame_strict(&self, frame_id: PageID, expected_key: Option<BlockKey>) {
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
        if usize::from(frame_id) >= self.size {
            return Err(Report::new(InternalError::ReadonlyFrameIndexOutOfBounds)
                .attach(format!("frame_id={frame_id}, size={}", self.size))
                .into());
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
        expected_key: BlockKey,
    ) -> bool {
        let frame = guard.bf();
        frame.kind() != FrameKind::Uninitialized
            && frame.persisted_block_key() == Some((expected_key.file_id, expected_key.block_id))
    }

    #[inline]
    fn invalidate_stale_mapping_if_same_frame(&self, key: BlockKey, frame_id: PageID) {
        if let Entry::Occupied(occ) = self.mappings.entry(key)
            && *occ.get() == frame_id
        {
            occ.remove();
        }
    }
}

impl Drop for ReadonlyBufferPool {
    #[inline]
    fn drop(&mut self) {
        self.signal_shutdown();
    }
}

// SAFETY: the global pool coordinates shared state with atomics, dashmaps, and
// page latches, while the arena keeps frame/page memory stable.
unsafe impl Send for ReadonlyBufferPool {}
// SAFETY: shared references only access thread-safe coordination state and
// latch-protected frames.
unsafe impl Sync for ReadonlyBufferPool {}

#[derive(Clone, Copy)]
struct InflightLoadValidation {
    file_kind: FileKind,
    validator: ReadonlyBlockValidator,
}

type ReadonlyReservedPage = PageReservationGuard<ReadonlyPageReservation>;

/// Reserved readonly-cache frame waiting to be filled by one miss load.
///
/// The reservation owns the exclusive frame guard and the global-pool context
/// needed to either publish the loaded frame into the mapping table or roll it
/// back to the free list.
pub(crate) struct ReadonlyPageReservation {
    pool: QuiescentGuard<ReadonlyBufferPool>,
    key: BlockKey,
    frame_id: PageID,
    page_guard: PageExclusiveGuard<Page>,
}

impl ReadonlyPageReservation {
    #[inline]
    /// Reserves one free readonly frame and locks it exclusively for one load.
    ///
    /// The returned guard is not yet visible through the persisted-block map.
    async fn reserve_page(
        pool: &ReadonlyBufferPool,
        _key: BlockKey,
        arena: ArenaGuard,
    ) -> Result<(PageID, PageExclusiveGuard<Page>)> {
        let frame_id = pool.reserve_frame_id_for_load().await?;
        let mut page_guard = match arena.try_lock_page_exclusive(frame_id) {
            Some(page_guard) => page_guard,
            None => {
                pool.residency.release_free(frame_id);
                return Err(Report::new(InternalError::ReadonlyFrameLockMissing)
                    .attach(format!("frame_id={frame_id}"))
                    .into());
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
        pool: QuiescentGuard<ReadonlyBufferPool>,
        key: BlockKey,
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
            Entry::Occupied(_) => {
                return Err(Report::new(InternalError::ReadonlyMappingConflict)
                    .attach(format!("publish key={key:?}, frame_id={frame_id}"))
                    .into());
            }
        }
        drop(page_guard);
        pool.residency.mark_resident(frame_id);
        if pool.residency.no_free_frame() {
            pool.residency.notify_evictor();
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
    key: BlockKey,
    _file: Arc<SparseFile>,
    operation: Operation,
    pool: QuiescentGuard<ReadonlyBufferPool>,
    inflight: Arc<PageIOCompletion>,
    reservation: Option<ReadonlyReservedPage>,
    validation: Option<InflightLoadValidation>,
    completed: bool,
}

impl ReadSubmission {
    #[inline]
    /// Builds one readonly miss-load submission from an already reserved frame.
    fn new(
        file: Arc<SparseFile>,
        pool: QuiescentGuard<ReadonlyBufferPool>,
        key: BlockKey,
        inflight: Arc<PageIOCompletion>,
        validation: Option<InflightLoadValidation>,
        mut reservation: ReadonlyReservedPage,
    ) -> Self {
        let ptr = reservation.page_mut() as *mut Page as *mut u8;
        let offset = usize::from(key.block_id) * PAGE_SIZE;
        // SAFETY: readonly loads borrow frame-owned page memory that remains
        // live until IO completion, and offsets are page-aligned by construction.
        let operation =
            unsafe { Operation::pread_borrowed(file.as_raw_fd(), offset, ptr, PAGE_SIZE) };
        pool.stats.add_queued_reads(1);
        ReadSubmission {
            key,
            _file: file,
            operation,
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
    fn complete_inflight_once(&mut self, result: CompletionResult<PageID>) {
        if self.completed {
            return;
        }
        self.pool
            .complete_inflight_load(self.key, &self.inflight, result);
        self.completed = true;
    }

    #[inline]
    /// Fails the miss before worker completion and wakes all joined waiters.
    pub(crate) fn fail(mut self, err: Report<CompletionErrorKind>) {
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
    pub(crate) fn complete(mut self, res: StdIoResult<usize>) -> IOKind {
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
                        self.complete_inflight_once(Err(CompletionErrorKind::report_error(
                            err,
                            format!(
                                "validate readonly block load: file={}, key={:?}",
                                validation.file_kind, self.key
                            ),
                        )));
                        return IOKind::Read;
                    }
                }
                let reservation = self.reservation.take().expect(
                    "readonly read submission must still own its page reservation before publish",
                );
                match reservation.publish() {
                    Ok(page_id) => Ok(page_id),
                    Err(err) => Err(CompletionErrorKind::report_error(
                        err,
                        format!("publish readonly block load: key={:?}", self.key),
                    )),
                }
            }
            Ok(len) => {
                drop(self.reservation.take());
                Err(CompletionErrorKind::report_unexpected_eof(
                    len,
                    PAGE_SIZE,
                    format!("complete readonly block load: key={:?}", self.key),
                ))
            }
            Err(err) => {
                drop(self.reservation.take());
                Err(CompletionErrorKind::report_io(
                    err,
                    format!("complete readonly block load: key={:?}", self.key),
                ))
            }
        };
        self.pool.stats.add_completed_reads(1);
        if result.is_err() {
            self.pool.stats.add_read_errors(1);
        }
        self.complete_inflight_once(result);
        IOKind::Read
    }
}

impl IOSubmission for ReadSubmission {
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
        self.complete_inflight_once(Err(CompletionErrorKind::report_internal(
            InternalError::CompletionDropped,
            format!(
                "drop readonly read submission before completion: key={:?}",
                self.key
            ),
        )));
    }
}

struct ReadonlyResidency {
    capacity: usize,
    set: Mutex<BTreeSet<PageID>>,
    free: Mutex<Vec<PageID>>,
    alloc_failures: FailureRateTracker,
    free_ev: Event,
    evict_ev: Event,
    shared_evict_wake: Mutex<Option<Arc<Event>>>,
}

impl ReadonlyResidency {
    #[inline]
    fn new(capacity: usize, eviction_arbiter: EvictionArbiter) -> Self {
        let mut free = Vec::with_capacity(capacity);
        for frame_id in 0..capacity {
            free.push(PageID::from(frame_id));
        }
        ReadonlyResidency {
            capacity,
            set: Mutex::new(BTreeSet::new()),
            free: Mutex::new(free),
            alloc_failures: FailureRateTracker::new(eviction_arbiter.failure_window()),
            free_ev: Event::new(),
            evict_ev: Event::new(),
            shared_evict_wake: Mutex::new(None),
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
    fn install_shared_wake(&self, wake_event: Arc<Event>) {
        *self.shared_evict_wake.lock() = Some(wake_event);
    }

    #[inline]
    fn notify_evictor(&self) {
        let shared_wake = self.shared_evict_wake.lock().clone();
        self.evict_ev.notify(1);
        if let Some(shared_wake) = shared_wake {
            shared_wake.notify(1);
        }
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
    pool: SyncQuiescentGuard<ReadonlyBufferPool>,
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
            let key = BlockKey::new(file_id, block_id);
            if let Some((_, mapped_frame_id)) = self.pool.mappings.remove(&key) {
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
        let _ = self.pool.residency.move_resident_to_free(frame_id);
    }
}

impl EvictionRuntime for ReadonlyRuntime {
    #[inline]
    fn resident_count(&self) -> usize {
        self.pool.residency.resident_len()
    }

    #[inline]
    fn capacity(&self) -> usize {
        self.pool.residency.capacity()
    }

    #[inline]
    fn inflight_evicts(&self) -> usize {
        0
    }

    #[inline]
    fn allocation_failure_rate(&self) -> f64 {
        self.pool.residency.alloc_failure_rate()
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
        self.pool.residency.collect_batch_ids(hand, limit, out)
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

/// Immutable persisted-block view returned by the shared readonly cache.
///
/// The guard retains the readonly-cache residency/lifetime of one loaded block
/// without exposing pool guards or latch types to callers.
pub struct ReadonlyBlockGuard {
    block_id: BlockID,
    guard: PageSharedGuard<Page>,
}

impl ReadonlyBlockGuard {
    #[inline]
    fn new(block_id: BlockID, guard: PageSharedGuard<Page>) -> Self {
        Self { block_id, guard }
    }

    /// Returns the persisted bytes of the loaded block.
    #[inline]
    pub fn page(&self) -> &Page {
        self.guard.page()
    }

    /// Returns the physical block id in the backing file.
    #[inline]
    pub fn block_id(&self) -> BlockID {
        self.block_id
    }
}

impl ReadonlyBufferPool {
    #[inline]
    /// Invalidates one block for the provided file id from the shared readonly cache.
    pub(crate) fn invalidate_block_id(&self, file_id: FileID, block_id: BlockID) -> Option<PageID> {
        self.invalidate_file_block(file_id, block_id)
    }

    #[inline]
    fn block_key(&self, file: &Arc<SparseFile>, block_id: BlockID) -> BlockKey {
        BlockKey::new(file.file_id(), block_id)
    }
}

impl QuiescentGuard<ReadonlyBufferPool> {
    #[inline]
    /// Reads one persisted block through the shared readonly cache.
    ///
    /// Add future callers with caution. This API does not provide the
    /// pre-publication validation contract of [`Self::read_validated_block`],
    /// so pages that require page-kind validation should use the validated
    /// entrypoint instead. If a new raw-read caller could overlap with a
    /// validated read for the same page, revisit the inflight-load semantics
    /// before expanding this API's usage.
    pub(crate) async fn read_block(
        &self,
        file_kind: FileKind,
        file: &Arc<SparseFile>,
        guard: &PoolGuard,
        block_id: BlockID,
    ) -> Result<ReadonlyBlockGuard> {
        self.read_shared_block(file_kind, file, guard, block_id, None)
            .await
    }

    #[inline]
    /// Reads and validates one persisted block before returning immutable bytes.
    ///
    /// On cache miss, validation runs before the new frame becomes resident.
    /// On cache hit, validation is re-run against the resident bytes and a
    /// failed validation invalidates the mapping before returning the error.
    pub(crate) async fn read_validated_block(
        &self,
        file_kind: FileKind,
        file: &Arc<SparseFile>,
        guard: &PoolGuard,
        block_id: BlockID,
        validator: ReadonlyBlockValidator,
    ) -> Result<ReadonlyBlockGuard> {
        self.read_shared_block(file_kind, file, guard, block_id, Some(validator))
            .await
    }

    #[inline]
    async fn join_or_start_inflight_load(
        &self,
        file: &Arc<SparseFile>,
        key: BlockKey,
        validation: Option<InflightLoadValidation>,
    ) -> Arc<PageIOCompletion> {
        self.stats.record_cache_miss();
        match self.inflight_loads.entry(key) {
            Entry::Vacant(vac) => {
                let inflight = Arc::new(PageIOCompletion::new());
                vac.insert(Arc::clone(&inflight));
                let task_arena = self.arena.arena_guard(self.pool_guard());
                match ReadonlyPageReservation::reserve_page(self, key, task_arena).await {
                    Ok((frame_id, page_guard)) => {
                        let reservation = ReadonlyPageReservation::from_reserved_page(
                            self.clone(),
                            key,
                            frame_id,
                            page_guard,
                        );
                        let req = ReadSubmission::new(
                            Arc::clone(file),
                            self.clone(),
                            key,
                            Arc::clone(&inflight),
                            validation,
                            reservation,
                        );
                        if let Err(err) = self.send_read_async(req).await {
                            err.into_inner().fail(CompletionErrorKind::report_send(
                                "send readonly pool read request",
                            ));
                        }
                    }
                    Err(err) => {
                        self.complete_inflight_load(
                            key,
                            &inflight,
                            Err(CompletionErrorKind::report_error(
                                err,
                                format!("reserve readonly block load frame: key={key:?}"),
                            )),
                        );
                    }
                }
                inflight
            }
            Entry::Occupied(occ) => {
                self.stats.record_miss_join();
                Arc::clone(occ.get())
            }
        }
    }

    #[inline]
    async fn get_or_load_frame_id(
        &self,
        file: &Arc<SparseFile>,
        key: BlockKey,
    ) -> Result<(PageID, bool)> {
        if let Some(frame_id) = self.try_get_frame_id(&key) {
            return Ok((frame_id, true));
        }
        let inflight = self.join_or_start_inflight_load(file, key, None).await;
        if let Some(frame_id) = self.try_get_frame_id(&key) {
            return Ok((frame_id, false));
        }
        inflight
            .wait_result()
            .await
            .map_err(|report| {
                Error::from_completion_report(
                    report,
                    format!("wait for readonly block load: key={key:?}"),
                )
            })
            .map(|frame_id| (frame_id, false))
    }

    #[inline]
    async fn get_or_load_frame_id_validated(
        &self,
        file_kind: FileKind,
        file: &Arc<SparseFile>,
        key: BlockKey,
        validator: ReadonlyBlockValidator,
    ) -> Result<(PageID, bool)> {
        if let Some(frame_id) = self.try_get_frame_id(&key) {
            return Ok((frame_id, true));
        }
        let inflight = self
            .join_or_start_inflight_load(
                file,
                key,
                Some(InflightLoadValidation {
                    file_kind,
                    validator,
                }),
            )
            .await;
        if let Some(frame_id) = self.try_get_frame_id(&key) {
            return Ok((frame_id, false));
        }
        inflight
            .wait_result()
            .await
            .map_err(|report| {
                Error::from_completion_report(
                    report,
                    format!(
                        "wait for validated readonly block load: file={file_kind}, key={key:?}"
                    ),
                )
            })
            .map(|frame_id| (frame_id, false))
    }

    #[inline]
    async fn read_shared_block(
        &self,
        file_kind: FileKind,
        file: &Arc<SparseFile>,
        guard: &PoolGuard,
        block_id: BlockID,
        validation: Option<ReadonlyBlockValidator>,
    ) -> Result<ReadonlyBlockGuard> {
        let key = self.block_key(file, block_id);
        self.validate_guard(guard);
        loop {
            let (frame_id, resident_hit) = match validation {
                Some(validator) => {
                    self.get_or_load_frame_id_validated(file_kind, file, key, validator)
                        .await?
                }
                None => self.get_or_load_frame_id(file, key).await?,
            };
            let guard = self
                .get_page_internal::<Page>(guard, frame_id, LatchFallbackMode::Shared)
                .await?;
            if !self.validate_guarded_frame_key(&guard, key) {
                self.invalidate_stale_mapping_if_same_frame(key, frame_id);
                continue;
            }
            if let Some(shared) = guard.lock_shared_async().await {
                let block = ReadonlyBlockGuard::new(block_id, shared);
                if let Some(validator) = validation
                    && let Err(err) = validator(block.page(), file_kind, block_id)
                {
                    drop(block);
                    // This resident-hit cleanup is only expected when a page
                    // became resident without miss-time validation. In current
                    // runtime usage that raw-read path is limited to CowFile
                    // super/meta-block loading, which does not overlap with the
                    // validated page kinds that reach this branch. Drop the
                    // shared guard before invalidation so the retry loop does
                    // not contend with our own latch; revisit this synchronous
                    // path if raw readonly usage expands in the future.
                    let _ = self.invalidate_block_id(file.file_id(), block_id);
                    return Err(err);
                }
                if resident_hit {
                    self.stats.record_cache_hit();
                }
                return Ok(block);
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::buffer::page::Page;
    use crate::buffer::test_page_id;
    use crate::catalog::TableID;
    use crate::catalog::{ColumnAttributes, ColumnSpec, TableMetadata, USER_OBJ_ID_START};
    use crate::conf::{EngineConfig, EvictableBufferPoolConfig, FileSystemConfig, TrxSysConfig};
    use crate::error::{CompletionErrorKind, Error, FileKind, LifecycleError, ResourceError};
    use crate::file::block_integrity::{
        BLOCK_INTEGRITY_HEADER_SIZE, COLUMN_BLOCK_INDEX_BLOCK_SPEC,
        COLUMN_DELETION_BLOB_BLOCK_SPEC, LWC_BLOCK_SPEC, max_payload_len, write_block_checksum,
        write_block_header,
    };
    use crate::file::build_test_fs;
    use crate::file::build_test_fs_in;
    use crate::file::cow_file::{COW_FILE_PAGE_SIZE, SUPER_BLOCK_ID};
    use crate::file::fs::FileSystem;
    use crate::file::fs::tests::{TestFileSystem, build_test_fs_owner_in};
    use crate::file::table_file::{MutableTableFile, TableFile};
    use crate::file::{CATALOG_MTB_FILE_ID, test_block_id, test_file_id};
    use crate::index::{
        COLUMN_BLOCK_HEADER_SIZE, COLUMN_BLOCK_NODE_PAYLOAD_SIZE,
        COLUMN_DELETION_BLOB_PAGE_HEADER_SIZE, ColumnBlockNodeHeader, validate_persisted_blob_page,
        validate_persisted_column_block_index_page,
    };
    use crate::io::{
        DirectBuf, IOBuf, IOKind, StorageBackendOp, StorageBackendTestHook,
        install_storage_backend_test_hook,
    };
    use crate::lwc::{
        LWC_BLOCK_PAYLOAD_SIZE, LwcBlock, LwcBlockHeader, validate_persisted_lwc_block,
    };
    use crate::quiescent::{QuiescentBox, QuiescentGuard};
    use crate::value::ValKind;
    use std::ops::Deref;
    use std::os::fd::{AsRawFd, RawFd};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::thread;
    use std::time::Duration;
    use tempfile::TempDir;

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

    fn assert_completion_data_integrity(err: Error) {
        assert!(matches!(
            err.completion_error(),
            Some(CompletionErrorKind::DataIntegrity(_))
        ));
        let report = format!("{err:?}");
        assert!(report.contains("propagate from other threads"), "{report}");
        assert!(report.contains("wait for"), "{report}");
    }

    fn frame_page_bytes(cap: usize) -> usize {
        cap.max(MIN_READONLY_POOL_PAGES) * (mem::size_of::<BufferFrame>() + mem::size_of::<Page>())
    }

    /// Test-only owner wrapper for one shared readonly pool.
    pub(crate) struct GlobalReadOnlyPoolScope {
        _temp_dir: TempDir,
        fs: Option<TestFileSystem>,
        owner: Option<QuiescentBox<ReadonlyBufferPool>>,
    }

    fn owned_global_pool(pool_size: usize) -> GlobalReadOnlyPoolScope {
        let temp_dir = TempDir::new().unwrap();
        let fs = build_test_fs_in(temp_dir.path());
        let owner = QuiescentBox::new(
            ReadonlyBufferPool::with_capacity(PoolRole::Disk, pool_size, fs.guard()).unwrap(),
        );
        GlobalReadOnlyPoolScope {
            _temp_dir: temp_dir,
            fs: Some(fs),
            owner: Some(owner),
        }
    }

    impl Deref for GlobalReadOnlyPoolScope {
        type Target = ReadonlyBufferPool;

        #[inline]
        fn deref(&self) -> &Self::Target {
            self.owner
                .as_ref()
                .expect("test readonly pool owner is live")
        }
    }

    impl GlobalReadOnlyPoolScope {
        #[inline]
        pub(crate) fn guard(&self) -> QuiescentGuard<ReadonlyBufferPool> {
            self.owner
                .as_ref()
                .expect("test readonly pool owner is live")
                .guard()
        }
    }

    impl Drop for GlobalReadOnlyPoolScope {
        #[inline]
        fn drop(&mut self) {
            drop(self.owner.take());
            drop(self.fs.take());
        }
    }

    #[inline]
    pub(crate) fn global_readonly_pool_scope(pool_size: usize) -> GlobalReadOnlyPoolScope {
        owned_global_pool(pool_size)
    }

    #[derive(Clone)]
    pub(crate) struct TestReadonlyPool {
        file_kind: FileKind,
        file: Arc<SparseFile>,
        global: QuiescentGuard<ReadonlyBufferPool>,
    }

    impl TestReadonlyPool {
        #[inline]
        pub(crate) fn file_kind(&self) -> FileKind {
            self.file_kind
        }

        #[inline]
        pub(crate) fn sparse_file(&self) -> &Arc<SparseFile> {
            &self.file
        }

        #[inline]
        pub(crate) fn global_pool(&self) -> &QuiescentGuard<ReadonlyBufferPool> {
            &self.global
        }

        #[inline]
        pub(crate) fn global_stats(&self) -> BufferPoolStats {
            self.global.stats()
        }

        #[inline]
        pub(crate) fn pool_guard(&self) -> PoolGuard {
            self.global.pool_guard()
        }

        #[inline]
        pub(crate) async fn read_block(
            &self,
            guard: &PoolGuard,
            block_id: BlockID,
        ) -> Result<ReadonlyBlockGuard> {
            self.global
                .read_block(self.file_kind, &self.file, guard, block_id)
                .await
        }

        #[inline]
        pub(crate) async fn read_validated_block(
            &self,
            guard: &PoolGuard,
            block_id: BlockID,
            validator: ReadonlyBlockValidator,
        ) -> Result<ReadonlyBlockGuard> {
            self.global
                .read_validated_block(self.file_kind, &self.file, guard, block_id, validator)
                .await
        }
    }

    #[inline]
    pub(crate) fn table_readonly_pool(
        scope: &GlobalReadOnlyPoolScope,
        _table_id: TableID,
        table_file: &Arc<TableFile>,
    ) -> TestReadonlyPool {
        TestReadonlyPool {
            file_kind: FileKind::TableFile,
            file: Arc::clone(table_file.sparse_file()),
            global: scope.guard(),
        }
    }

    fn owned_readonly_pool(
        _file_id: FileID,
        file_kind: FileKind,
        file: Arc<SparseFile>,
        global: &GlobalReadOnlyPoolScope,
    ) -> QuiescentBox<TestReadonlyPool> {
        QuiescentBox::new(TestReadonlyPool {
            file_kind,
            file,
            global: global.guard(),
        })
    }

    #[test]
    fn test_global_readonly_pool_shutdown_is_idempotent_before_worker_start() {
        let pool = owned_global_pool(frame_page_bytes(2));

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

    async fn commit_table_file(_fs: &FileSystem, table_file: MutableTableFile) -> Arc<TableFile> {
        let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
        drop(old_root);
        table_file
    }

    async fn write_payload(
        fs: &FileSystem,
        table_file: &Arc<TableFile>,
        page_id: BlockID,
        payload: &[u8],
    ) {
        let mut buf = DirectBuf::zeroed(COW_FILE_PAGE_SIZE);
        let bytes = buf.as_bytes_mut();
        bytes[..payload.len()].copy_from_slice(payload);
        let mutable = MutableTableFile::fork(table_file, fs.background_writes());
        mutable.write_block(page_id, buf).await.unwrap();
        drop(mutable);
    }

    async fn write_page_bytes(
        fs: &FileSystem,
        table_file: &Arc<TableFile>,
        page_id: BlockID,
        bytes: &[u8],
    ) {
        let mut buf = DirectBuf::zeroed(COW_FILE_PAGE_SIZE);
        buf.as_bytes_mut().copy_from_slice(bytes);
        let mutable = MutableTableFile::fork(table_file, fs.background_writes());
        mutable.write_block(page_id, buf).await.unwrap();
        drop(mutable);
    }

    #[test]
    fn test_readonly_pool_global_stats_track_single_miss_then_warm_hit() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let table_file = fs.create_table_file(120, make_metadata(), false).unwrap();
            let table_file = commit_table_file(&fs, table_file).await;
            write_payload(&fs, &table_file, test_block_id(0), b"readonly-stats").await;

            let scope = global_readonly_pool_scope(frame_page_bytes(4));
            let pool = table_readonly_pool(&scope, 120, &table_file);
            let pool_guard = pool.pool_guard();

            let cold_start = pool.global_stats();
            let cold_guard = pool
                .read_block(&pool_guard, SUPER_BLOCK_ID)
                .await
                .expect("readonly cold read failed in test");
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
                .read_block(&pool_guard, SUPER_BLOCK_ID)
                .await
                .expect("readonly warm read failed in test");
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
            let table_file_a = commit_table_file(&fs, table_file_a).await;
            write_payload(&fs, &table_file_a, test_block_id(0), b"readonly-shared-a").await;

            let table_file_b = fs.create_table_file(122, make_metadata(), false).unwrap();
            let table_file_b = commit_table_file(&fs, table_file_b).await;
            write_payload(&fs, &table_file_b, test_block_id(0), b"readonly-shared-b").await;

            let scope = global_readonly_pool_scope(frame_page_bytes(4));
            let pool_a = table_readonly_pool(&scope, 121, &table_file_a);
            let pool_b = table_readonly_pool(&scope, 122, &table_file_b);
            let pool_a_guard = pool_a.pool_guard();

            let start_a = pool_a.global_stats();
            let start_b = pool_b.global_stats();
            assert_eq!(start_a, start_b);

            let guard_a = pool_a
                .read_block(&pool_a_guard, test_block_id(0))
                .await
                .expect("readonly shared global read failed in test");
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

    fn build_valid_persisted_lwc_block() -> Vec<u8> {
        let mut buf = vec![0u8; COW_FILE_PAGE_SIZE];
        let payload_start = write_block_header(&mut buf, LWC_BLOCK_SPEC);
        let payload_end = payload_start + LWC_BLOCK_PAYLOAD_SIZE;
        let page = LwcBlock::try_from_bytes_mut(&mut buf[payload_start..payload_end]).unwrap();
        page.header = LwcBlockHeader::new(1, 0, 0, 0);
        write_block_checksum(&mut buf);
        buf
    }

    fn build_valid_persisted_column_block_page() -> Vec<u8> {
        let mut buf = vec![0u8; COW_FILE_PAGE_SIZE];
        let payload_start = write_block_header(&mut buf, COLUMN_BLOCK_INDEX_BLOCK_SPEC);
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
        write_block_checksum(&mut buf);
        buf
    }

    fn build_valid_persisted_blob_page() -> Vec<u8> {
        let mut buf = vec![0u8; COW_FILE_PAGE_SIZE];
        let payload_start = write_block_header(&mut buf, COLUMN_DELETION_BLOB_BLOCK_SPEC);
        let payload_end = payload_start + max_payload_len(COW_FILE_PAGE_SIZE);
        let payload = &mut buf[payload_start..payload_end];
        payload[..COLUMN_DELETION_BLOB_PAGE_HEADER_SIZE].fill(0);
        payload[8..10].copy_from_slice(&(1u16).to_le_bytes());
        payload[COLUMN_DELETION_BLOB_PAGE_HEADER_SIZE] = 7;
        write_block_checksum(&mut buf);
        buf
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
        fn for_page(fd: RawFd, page_id: BlockID) -> Self {
            ControlledReadHook {
                inner: Arc::new(ControlledReadHookInner {
                    fd,
                    offset: usize::from(page_id) * PAGE_SIZE,
                    result: ControlledReadResult::Success,
                    calls: AtomicUsize::new(0),
                    start_ev: Event::new(),
                    released: AtomicBool::new(false),
                    release_ev: Event::new(),
                }),
            }
        }

        fn with_errno(fd: RawFd, page_id: BlockID, errno: i32) -> Self {
            ControlledReadHook {
                inner: Arc::new(ControlledReadHookInner {
                    fd,
                    offset: usize::from(page_id) * PAGE_SIZE,
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
            op.kind() == IOKind::Read
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

        fn on_complete(&self, op: StorageBackendOp, res: &mut StdIoResult<usize>) {
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
        let key = BlockKey::new(test_file_id(7), test_block_id(11));

        assert_eq!(global.allocated(), 0);
        let mut g3 = global
            .try_lock_page_exclusive(&global_guard, test_page_id(3))
            .unwrap();
        global.bind_frame(key, &mut g3).unwrap();
        assert_eq!(global.allocated(), 1);
        assert_eq!(global.try_get_frame_id(&key), Some(test_page_id(3)));
        assert_eq!(global.try_get_block_key(test_page_id(3)), Some(key));

        assert!(global.bind_frame(key, &mut g3).is_ok());

        let err = global
            .bind_frame(BlockKey::new(test_file_id(7), test_block_id(12)), &mut g3)
            .unwrap_err();
        assert_eq!(
            err.report().downcast_ref::<InternalError>().copied(),
            Some(InternalError::ReadonlyMappingConflict)
        );

        drop(g3);
        assert_eq!(global.invalidate_key(&key), Some(test_page_id(3)));
        assert_eq!(global.allocated(), 0);
    }

    #[test]
    fn test_read_submission_terminal_completion_is_one_shot() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let table_file = fs.create_table_file(118, make_metadata(), false).unwrap();
            let table_file = commit_table_file(&fs, table_file).await;

            let global = owned_global_pool(frame_page_bytes(2));
            let pool = owned_readonly_pool(
                test_file_id(118),
                FileKind::TableFile,
                Arc::clone(table_file.sparse_file()),
                &global,
            );
            let key = BlockKey::new(test_file_id(118), test_block_id(14));
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
                Arc::clone(pool.sparse_file()),
                pool.global.clone(),
                key,
                Arc::clone(&inflight),
                None,
                reservation,
            );

            submission.complete_inflight_once(Err(CompletionErrorKind::report_send(
                "complete readonly inflight test send failure",
            )));
            assert!(submission.completed);
            let report = inflight.completed_result().unwrap().unwrap_err();
            assert_eq!(*report.current_context(), CompletionErrorKind::Send);

            drop(submission);
            let report = inflight.completed_result().unwrap().unwrap_err();
            assert_eq!(*report.current_context(), CompletionErrorKind::Send);

            drop(pool);
            drop(table_file);
            drop(fs);
        });
    }

    #[test]
    fn test_global_readonly_pool_rejects_too_small_capacity() {
        let bytes = (MIN_READONLY_POOL_PAGES - 1)
            * (mem::size_of::<BufferFrame>() + mem::size_of::<Page>());
        let temp_dir = TempDir::new().unwrap();
        let fs_owner = build_test_fs_owner_in(temp_dir.path()).unwrap();
        let res = ReadonlyBufferPool::with_capacity(PoolRole::Disk, bytes, fs_owner.guard());
        assert!(
            res.as_ref().is_err_and(
                |err| err.resource_error() == Some(ResourceError::BufferPoolSizeTooSmall)
            )
        );
    }

    #[test]
    fn test_global_invalidate_file() {
        let global = owned_global_pool(64 * 1024 * 1024);
        let global_guard = (*global).pool_guard();
        let k1 = BlockKey::new(test_file_id(1), test_block_id(10));
        let k2 = BlockKey::new(test_file_id(1), test_block_id(11));
        let k3 = BlockKey::new(test_file_id(2), test_block_id(20));

        let mut g1 = global
            .try_lock_page_exclusive(&global_guard, test_page_id(1))
            .unwrap();
        let mut g2 = global
            .try_lock_page_exclusive(&global_guard, test_page_id(2))
            .unwrap();
        let mut g3 = global
            .try_lock_page_exclusive(&global_guard, test_page_id(3))
            .unwrap();
        global.bind_frame(k1, &mut g1).unwrap();
        global.bind_frame(k2, &mut g2).unwrap();
        global.bind_frame(k3, &mut g3).unwrap();
        drop(g1);
        drop(g2);
        drop(g3);

        assert_eq!(global.invalidate_file(test_file_id(1)), 2);
        assert_eq!(global.try_get_frame_id(&k1), None);
        assert_eq!(global.try_get_frame_id(&k2), None);
        assert_eq!(global.try_get_frame_id(&k3), Some(test_page_id(3)));
    }

    #[test]
    fn test_readonly_cache_file_ids_keep_catalog_and_user_pages_isolated() {
        let global = owned_global_pool(64 * 1024 * 1024);
        let global_guard = (*global).pool_guard();
        let catalog_key = BlockKey::new(CATALOG_MTB_FILE_ID, test_block_id(42));
        let user_key = BlockKey::new(FileID::new(USER_OBJ_ID_START), test_block_id(42));

        let mut catalog_frame = global
            .try_lock_page_exclusive(&global_guard, test_page_id(1))
            .unwrap();
        let mut user_frame = global
            .try_lock_page_exclusive(&global_guard, test_page_id(2))
            .unwrap();
        global.bind_frame(catalog_key, &mut catalog_frame).unwrap();
        global.bind_frame(user_key, &mut user_frame).unwrap();
        drop(catalog_frame);
        drop(user_frame);

        assert_eq!(global.try_get_frame_id(&catalog_key), Some(test_page_id(1)));
        assert_eq!(global.try_get_frame_id(&user_key), Some(test_page_id(2)));
    }

    #[test]
    fn test_global_invalidate_key_strict() {
        let global = owned_global_pool(64 * 1024 * 1024);
        let global_guard = (*global).pool_guard();
        let key = BlockKey::new(test_file_id(9), test_block_id(77));

        let mut g = global
            .try_lock_page_exclusive(&global_guard, test_page_id(5))
            .unwrap();
        global.bind_frame(key, &mut g).unwrap();
        drop(g);

        assert_eq!(global.invalidate_key_strict(&key), Some(test_page_id(5)));
        assert_eq!(global.try_get_frame_id(&key), None);
    }

    #[test]
    #[should_panic(expected = "strict invalidation lock acquisition failed")]
    fn test_global_invalidate_key_strict_panics_when_latch_held() {
        smol::block_on(async {
            let global = owned_global_pool(64 * 1024 * 1024);
            let global_guard = (*global).pool_guard();
            let key = BlockKey::new(test_file_id(10), test_block_id(99));

            let mut g = global
                .try_lock_page_exclusive(&global_guard, test_page_id(6))
                .unwrap();
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
                .get_page_internal::<Page>(
                    &foreign_guard,
                    test_page_id(0),
                    LatchFallbackMode::Shared,
                )
                .await
                .unwrap();
        });
    }

    #[test]
    fn test_readonly_pool_reloads_when_mapping_points_to_uninitialized_frame() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let table_file = fs.create_table_file(111, make_metadata(), false).unwrap();
            let table_file = commit_table_file(&fs, table_file).await;
            write_payload(&fs, &table_file, test_block_id(9), b"reload").await;

            let global = owned_global_pool(frame_page_bytes(2));
            let global_guard = (*global).pool_guard();
            let key = BlockKey::new(test_file_id(111), test_block_id(9));

            let mut g0 = global
                .try_lock_page_exclusive(&global_guard, test_page_id(0))
                .unwrap();
            global.bind_frame(key, &mut g0).unwrap();
            g0.page_mut().zero();
            let frame = g0.bf_mut();
            frame.clear_persisted_block_key();
            frame.bump_generation();
            frame.set_kind(FrameKind::Uninitialized);
            drop(g0);

            let pool = owned_readonly_pool(
                test_file_id(111),
                FileKind::TableFile,
                Arc::clone(table_file.sparse_file()),
                &global,
            );
            let pool_guard = pool.pool_guard();
            let reload_start = pool.global_stats();
            let page = pool
                .read_block(&pool_guard, test_block_id(9))
                .await
                .expect("buffer-pool read failed in test");
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
            let table_file = commit_table_file(&fs, table_file).await;

            let persisted_page = build_valid_persisted_lwc_block();
            write_page_bytes(&fs, &table_file, test_block_id(12), &persisted_page).await;

            let global = owned_global_pool(frame_page_bytes(2));
            let global_guard = (*global).pool_guard();
            let key = BlockKey::new(test_file_id(123), test_block_id(12));

            let mut g0 = global
                .try_lock_page_exclusive(&global_guard, test_page_id(0))
                .unwrap();
            global.bind_frame(key, &mut g0).unwrap();
            g0.page_mut().zero();
            let frame = g0.bf_mut();
            frame.clear_persisted_block_key();
            frame.bump_generation();
            frame.set_kind(FrameKind::Uninitialized);
            drop(g0);

            let pool = owned_readonly_pool(
                test_file_id(123),
                FileKind::TableFile,
                Arc::clone(table_file.sparse_file()),
                &global,
            );
            let pool_guard = pool.pool_guard();

            let reload_start = pool.global_stats();
            let page = pool
                .read_validated_block(&pool_guard, test_block_id(12), validate_persisted_lwc_block)
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
                .read_validated_block(&pool_guard, test_block_id(12), validate_persisted_lwc_block)
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
            let table_file = commit_table_file(&fs, table_file).await;
            write_payload(&fs, &table_file, test_block_id(3), b"hello").await;

            let global = owned_global_pool(frame_page_bytes(4));
            let pool = owned_readonly_pool(
                test_file_id(101),
                FileKind::TableFile,
                Arc::clone(table_file.sparse_file()),
                &global,
            );
            let pool_guard = pool.pool_guard();
            let page = pool
                .read_block(&pool_guard, test_block_id(3))
                .await
                .expect("buffer-pool read failed in test");
            assert_eq!(&page.page()[..5], b"hello");
            drop(page);

            let page = pool
                .read_block(&pool_guard, test_block_id(3))
                .await
                .expect("buffer-pool read failed in test");
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
            let table_file = commit_table_file(&fs, table_file).await;
            write_payload(&fs, &table_file, test_block_id(5), b"world").await;

            let global = owned_global_pool(frame_page_bytes(8));
            let pool = owned_readonly_pool(
                test_file_id(102),
                FileKind::TableFile,
                Arc::clone(table_file.sparse_file()),
                &global,
            );
            let pool_guard = pool.pool_guard();

            let mut tasks = vec![];
            for _ in 0..16 {
                let pool = (*pool).clone();
                let pool_guard = pool_guard.clone();
                tasks.push(smol::spawn(async move {
                    let g = pool
                        .read_block(&pool_guard, test_block_id(5))
                        .await
                        .expect("buffer-pool read failed in test");
                    g.page()[0]
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
            let table_file = commit_table_file(&fs, table_file).await;
            write_payload(&fs, &table_file, test_block_id(5), b"hello").await;
            let read_hook = Arc::new(ControlledReadHook::for_page(
                table_file.sparse_file().as_raw_fd(),
                test_block_id(5),
            ));
            let _hook = install_storage_backend_test_hook(read_hook.clone());

            let global = owned_global_pool(frame_page_bytes(2));
            let pool = owned_readonly_pool(
                test_file_id(112),
                FileKind::TableFile,
                Arc::clone(table_file.sparse_file()),
                &global,
            );
            let pool_guard = pool.pool_guard();
            let key = BlockKey::new(test_file_id(112), test_block_id(5));

            let pool_for_loader = (*pool).clone();
            let loader_guard = pool_guard.clone();
            let loader = smol::spawn(async move {
                let _ = pool_for_loader
                    .read_block(&loader_guard, test_block_id(5))
                    .await
                    .unwrap();
            });
            read_hook.wait_started(1).await;
            assert!(global.inflight_loads.contains_key(&key));

            drop(loader);

            let pool_for_waiter = (*pool).clone();
            let waiter_guard = pool_guard.clone();
            let waiter = smol::spawn(async move {
                let g = pool_for_waiter
                    .read_block(&waiter_guard, test_block_id(5))
                    .await
                    .unwrap();
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
            let table_file = commit_table_file(&fs, table_file).await;
            write_payload(&fs, &table_file, test_block_id(9), b"solo").await;
            let read_hook = Arc::new(ControlledReadHook::for_page(
                table_file.sparse_file().as_raw_fd(),
                test_block_id(9),
            ));
            let _hook = install_storage_backend_test_hook(read_hook.clone());

            let global = owned_global_pool(frame_page_bytes(2));
            let pool = owned_readonly_pool(
                test_file_id(113),
                FileKind::TableFile,
                Arc::clone(table_file.sparse_file()),
                &global,
            );
            let pool_guard = pool.pool_guard();
            let key = BlockKey::new(test_file_id(113), test_block_id(9));

            let pool_for_loader = (*pool).clone();
            let loader_guard = pool_guard.clone();
            let loader = smol::spawn(async move {
                let _ = pool_for_loader
                    .read_block(&loader_guard, test_block_id(9))
                    .await
                    .unwrap();
            });
            read_hook.wait_started(1).await;
            assert!(global.inflight_loads.contains_key(&key));

            drop(loader);
            read_hook.release();

            wait_for(|| {
                !global.inflight_loads.contains_key(&key) && global.try_get_frame_id(&key).is_some()
            })
            .await;

            assert_eq!(read_hook.call_count(), 1);
            assert_eq!(global.allocated(), 1);
            let g = pool
                .read_block(&pool_guard, test_block_id(9))
                .await
                .unwrap();
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
            let table_file = commit_table_file(&fs, table_file).await;
            write_payload(&fs, &table_file, test_block_id(12), b"drop").await;
            let read_hook = Arc::new(ControlledReadHook::for_page(
                table_file.sparse_file().as_raw_fd(),
                test_block_id(12),
            ));
            let _hook = install_storage_backend_test_hook(read_hook.clone());
            let key = BlockKey::new(test_file_id(116), test_block_id(12));

            let global = owned_global_pool(frame_page_bytes(2));
            let pool = owned_readonly_pool(
                test_file_id(116),
                FileKind::TableFile,
                Arc::clone(table_file.sparse_file()),
                &global,
            );
            let pool_guard = pool.pool_guard();
            let observe = global.guard();

            let pool_for_loader = (*pool).clone();
            let loader_guard = pool_guard.clone();
            let loader = smol::spawn(async move {
                let _ = pool_for_loader
                    .read_block(&loader_guard, test_block_id(12))
                    .await
                    .unwrap();
            });
            read_hook.wait_started(1).await;
            assert!(observe.inflight_loads.contains_key(&key));
            drop(loader);
            drop(pool_guard);

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
            wait_for(|| {
                !observe.inflight_loads.contains_key(&key) && observe.mappings.contains_key(&key)
            })
            .await;
            assert!(observe.mappings.contains_key(&key));
            drop(observe);
            teardown.join().unwrap();
            assert_eq!(read_hook.call_count(), 1);
            assert!(dropped.load(Ordering::SeqCst));
            drop(table_file);
            drop(fs);
        });
    }

    #[test]
    fn test_readonly_pool_drop_unblocks_detached_reserve_waiter() {
        smol::block_on(async {
            let key = BlockKey::new(test_file_id(117), test_block_id(13));
            let global = owned_global_pool(frame_page_bytes(1));

            {
                let mut free = global.residency.free.lock();
                free.clear();
            }

            let inflight = Arc::new(PageIOCompletion::new());
            global.inflight_loads.insert(key, Arc::clone(&inflight));
            let global_guard = global.guard();
            let task_arena = global_guard.arena.arena_guard(global_guard.pool_guard());
            let inflight_for_waiter = Arc::clone(&inflight);
            let reserve_waiter = {
                listener!(global.residency.evict_ev => evict_listener);
                let reserve_waiter = smol::spawn(async move {
                    let global = global_guard;
                    match ReadonlyPageReservation::reserve_page(&global, key, task_arena).await {
                        Ok((_frame_id, _page_guard)) => {
                            panic!("reserve waiter unexpectedly acquired a frame");
                        }
                        Err(err) => global.complete_inflight_load(
                            key,
                            &inflight_for_waiter,
                            Err(CompletionErrorKind::report_error(
                                err,
                                format!("reserve readonly test block load frame: key={key:?}"),
                            )),
                        ),
                    }
                });
                evict_listener.await;
                reserve_waiter
            };
            assert!(global.inflight_loads.contains_key(&key));
            assert!(global.residency.alloc_failure_rate() > 0.0);

            let dropped = Arc::new(AtomicBool::new(false));
            let dropped_flag = Arc::clone(&dropped);
            let shutdown = global.guard();
            let teardown = thread::spawn(move || {
                shutdown.signal_shutdown();
                drop(shutdown);
                drop(global);
                dropped_flag.store(true, Ordering::SeqCst);
            });

            reserve_waiter.await;
            teardown.join().unwrap();
            assert!(dropped.load(Ordering::SeqCst));
            let report = inflight.wait_result().await.unwrap_err();
            assert_eq!(
                *report.current_context(),
                CompletionErrorKind::Lifecycle(LifecycleError::Shutdown)
            );
        });
    }

    #[test]
    fn test_readonly_pool_shared_io_failure_propagates_to_all_waiters() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let table_file = fs.create_table_file(114, make_metadata(), false).unwrap();
            let table_file = commit_table_file(&fs, table_file).await;
            let read_hook = Arc::new(ControlledReadHook::with_errno(
                table_file.sparse_file().as_raw_fd(),
                test_block_id(7),
                libc::EIO,
            ));
            let _hook = install_storage_backend_test_hook(read_hook.clone());

            let global = owned_global_pool(frame_page_bytes(2));
            let pool = owned_readonly_pool(
                test_file_id(114),
                FileKind::TableFile,
                Arc::clone(table_file.sparse_file()),
                &global,
            );
            let pool_guard = pool.pool_guard();
            let key = BlockKey::new(test_file_id(114), test_block_id(7));

            let pool_1 = (*pool).clone();
            let waiter1_guard = pool_guard.clone();
            let waiter1 =
                smol::spawn(
                    async move { pool_1.read_block(&waiter1_guard, test_block_id(7)).await },
                );
            let pool_2 = (*pool).clone();
            let waiter2_guard = pool_guard.clone();
            let waiter2 =
                smol::spawn(
                    async move { pool_2.read_block(&waiter2_guard, test_block_id(7)).await },
                );

            read_hook.wait_started(1).await;
            smol::Timer::after(Duration::from_millis(10)).await;
            read_hook.release();

            assert!(waiter1.await.as_ref().is_err_and(|err| matches!(
                err.completion_error(),
                Some(CompletionErrorKind::Io(_))
            )));
            assert!(waiter2.await.as_ref().is_err_and(|err| matches!(
                err.completion_error(),
                Some(CompletionErrorKind::Io(_))
            )));
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
            let table_file = commit_table_file(&fs, table_file).await;
            let mut page = build_valid_persisted_lwc_block();
            let last_idx = page.len() - 1;
            page[last_idx] ^= 0xFF;
            write_page_bytes(&fs, &table_file, test_block_id(8), &page).await;
            let read_hook = Arc::new(ControlledReadHook::for_page(
                table_file.sparse_file().as_raw_fd(),
                test_block_id(8),
            ));
            let _hook = install_storage_backend_test_hook(read_hook.clone());

            let global = owned_global_pool(frame_page_bytes(2));
            let pool = owned_readonly_pool(
                test_file_id(115),
                FileKind::TableFile,
                Arc::clone(table_file.sparse_file()),
                &global,
            );
            let pool_guard = pool.pool_guard();
            let key = BlockKey::new(test_file_id(115), test_block_id(8));

            let pool_1 = (*pool).clone();
            let waiter1_guard = pool_guard.clone();
            let waiter1 = smol::spawn(async move {
                pool_1
                    .read_validated_block(
                        &waiter1_guard,
                        test_block_id(8),
                        validate_persisted_lwc_block,
                    )
                    .await
            });
            let pool_2 = (*pool).clone();
            let waiter2_guard = pool_guard.clone();
            let waiter2 = smol::spawn(async move {
                pool_2
                    .read_validated_block(
                        &waiter2_guard,
                        test_block_id(8),
                        validate_persisted_lwc_block,
                    )
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
            assert_completion_data_integrity(err1);
            assert_completion_data_integrity(err2);
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
            let table_file = commit_table_file(&fs, table_file).await;

            let mut page = build_valid_persisted_lwc_block();
            let last_idx = page.len() - 1;
            page[last_idx] ^= 0xFF;
            write_page_bytes(&fs, &table_file, test_block_id(9), &page).await;

            let global = owned_global_pool(frame_page_bytes(4));
            let pool = owned_readonly_pool(
                test_file_id(107),
                FileKind::TableFile,
                Arc::clone(table_file.sparse_file()),
                &global,
            );
            let pool_guard = pool.pool_guard();
            let key = BlockKey::new(test_file_id(107), test_block_id(9));

            let err = match pool
                .read_validated_block(&pool_guard, test_block_id(9), validate_persisted_lwc_block)
                .await
            {
                Ok(_) => panic!("expected persisted LWC corruption"),
                Err(err) => err,
            };
            assert_completion_data_integrity(err);
            assert_eq!(global.try_get_frame_id(&key), None);
            assert_eq!(global.allocated(), 0);
        });
    }

    #[test]
    fn test_readonly_pool_validated_lwc_miss_rejects_invalid_offsets_without_mapping() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let table_file = fs.create_table_file(116, make_metadata(), false).unwrap();
            let table_file = commit_table_file(&fs, table_file).await;

            let mut page = build_valid_persisted_lwc_block();
            {
                let payload_start = BLOCK_INTEGRITY_HEADER_SIZE;
                let payload_end = payload_start + LWC_BLOCK_PAYLOAD_SIZE;
                let page_view =
                    LwcBlock::try_from_bytes_mut(&mut page[payload_start..payload_end]).unwrap();
                let invalid_end = (page_view.body.len() as u16).saturating_add(1);
                page_view.header = LwcBlockHeader::new(1, 1, 1, 0);
                page_view.body[..2].copy_from_slice(&invalid_end.to_le_bytes());
            }
            write_block_checksum(&mut page);
            write_page_bytes(&fs, &table_file, test_block_id(10), &page).await;

            let global = owned_global_pool(frame_page_bytes(4));
            let pool = owned_readonly_pool(
                test_file_id(116),
                FileKind::TableFile,
                Arc::clone(table_file.sparse_file()),
                &global,
            );
            let pool_guard = pool.pool_guard();
            let key = BlockKey::new(test_file_id(116), test_block_id(10));

            let err = match pool
                .read_validated_block(&pool_guard, test_block_id(10), validate_persisted_lwc_block)
                .await
            {
                Ok(_) => panic!("expected persisted LWC invalid-payload corruption"),
                Err(err) => err,
            };
            assert_completion_data_integrity(err);
            assert_eq!(global.try_get_frame_id(&key), None);
            assert_eq!(global.allocated(), 0);
        });
    }

    #[test]
    fn test_readonly_pool_validated_column_index_miss_rejects_corruption_without_mapping() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let table_file = fs.create_table_file(108, make_metadata(), false).unwrap();
            let table_file = commit_table_file(&fs, table_file).await;

            let mut page = build_valid_persisted_column_block_page();
            let last_idx = page.len() - 1;
            page[last_idx] ^= 0xFF;
            write_page_bytes(&fs, &table_file, test_block_id(10), &page).await;

            let global = owned_global_pool(frame_page_bytes(4));
            let pool = owned_readonly_pool(
                test_file_id(108),
                FileKind::TableFile,
                Arc::clone(table_file.sparse_file()),
                &global,
            );
            let pool_guard = pool.pool_guard();
            let key = BlockKey::new(test_file_id(108), test_block_id(10));

            let err = match pool
                .read_validated_block(
                    &pool_guard,
                    test_block_id(10),
                    validate_persisted_column_block_index_page,
                )
                .await
            {
                Ok(_) => panic!("expected persisted column-block corruption"),
                Err(err) => err,
            };
            assert_completion_data_integrity(err);
            assert_eq!(global.try_get_frame_id(&key), None);
            assert_eq!(global.allocated(), 0);
        });
    }

    #[test]
    fn test_readonly_pool_validated_blob_miss_rejects_corruption_without_mapping() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let table_file = fs.create_table_file(109, make_metadata(), false).unwrap();
            let table_file = commit_table_file(&fs, table_file).await;

            let mut page = build_valid_persisted_blob_page();
            let last_idx = page.len() - 1;
            page[last_idx] ^= 0xFF;
            write_page_bytes(&fs, &table_file, test_block_id(11), &page).await;

            let global = owned_global_pool(frame_page_bytes(4));
            let pool = owned_readonly_pool(
                test_file_id(109),
                FileKind::TableFile,
                Arc::clone(table_file.sparse_file()),
                &global,
            );
            let pool_guard = pool.pool_guard();
            let key = BlockKey::new(test_file_id(109), test_block_id(11));

            let err = match pool
                .read_validated_block(&pool_guard, test_block_id(11), validate_persisted_blob_page)
                .await
            {
                Ok(_) => panic!("expected persisted deletion-blob corruption"),
                Err(err) => err,
            };
            assert_completion_data_integrity(err);
            assert_eq!(global.try_get_frame_id(&key), None);
            assert_eq!(global.allocated(), 0);
        });
    }

    #[test]
    fn test_readonly_pool_drop_only_eviction_and_reload() {
        smol::block_on(async {
            const TEST_POOL_BYTES: usize = 64 * 1024 * 1024;

            let root = TempDir::new().unwrap();
            let engine = EngineConfig::default()
                .storage_root(root.path())
                .meta_buffer(TEST_POOL_BYTES)
                .index_buffer(TEST_POOL_BYTES)
                .index_max_file_size(128usize * 1024 * 1024)
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .role(PoolRole::Mem)
                        .max_mem_size(TEST_POOL_BYTES)
                        .max_file_size(128usize * 1024 * 1024),
                )
                .file(FileSystemConfig::default().readonly_buffer_size(frame_page_bytes(1)))
                .trx(TrxSysConfig::default())
                .build()
                .await
                .unwrap();

            let table_file = engine
                .table_fs
                .create_table_file(103, make_metadata(), false)
                .unwrap();
            let table_file = commit_table_file(&engine.table_fs, table_file).await;

            let capacity = engine.disk_pool.capacity();
            let base_page_id = 7u64;

            // Prepare one more block than cache capacity to force drop-only eviction.
            for i in 0..=capacity {
                let block_id = BlockID::from(base_page_id + i as u64);
                let payload = format!("page-{i}");
                write_payload(&engine.table_fs, &table_file, block_id, payload.as_bytes()).await;
            }
            drop(table_file);

            let table_file = engine
                .table_fs
                .open_table_file(103, engine.disk_pool.clone_inner())
                .await
                .unwrap();
            let pool = engine.disk_pool.clone_inner();
            let pool_guard = pool.pool_guard();

            for i in 0..=capacity {
                let block_id = BlockID::from(base_page_id + i as u64);
                let expected = format!("page-{i}");
                let g = pool
                    .read_block(
                        table_file.file_kind(),
                        table_file.sparse_file(),
                        &pool_guard,
                        block_id,
                    )
                    .await
                    .expect("buffer-pool read failed in test");
                assert_eq!(&g.page()[..expected.len()], expected.as_bytes());
                drop(g);
            }

            let loaded_count = capacity + 1;
            let mapped_count = (0..=capacity)
                .filter(|i| {
                    let key =
                        BlockKey::new(test_file_id(103), BlockID::from(base_page_id + *i as u64));
                    engine.disk_pool.try_get_frame_id(&key).is_some()
                })
                .count();
            assert!(mapped_count < loaded_count);

            // Reload the first page after cache pressure; this should still return correct data.
            let g = pool
                .read_block(
                    table_file.file_kind(),
                    table_file.sparse_file(),
                    &pool_guard,
                    BlockID::from(base_page_id),
                )
                .await
                .expect("buffer-pool read failed in test");
            assert_eq!(&g.page()[..6], b"page-0");
            drop(g);
            drop(pool_guard);
            drop(pool);
            drop(table_file);
            engine.shutdown().unwrap();
        });
    }

    #[test]
    fn test_readonly_pool_lifecycle_drop_order_with_table_fs() {
        smol::block_on(async {
            let global = owned_global_pool(frame_page_bytes(2));
            let (_temp_dir, fs) = build_test_fs();
            let table_file = fs.create_table_file(105, make_metadata(), false).unwrap();
            let table_file = commit_table_file(&fs, table_file).await;
            write_payload(&fs, &table_file, test_block_id(9), b"drop-order").await;

            let pool = owned_readonly_pool(
                test_file_id(105),
                FileKind::TableFile,
                Arc::clone(table_file.sparse_file()),
                &global,
            );
            let pool_guard = pool.pool_guard();

            let g = pool
                .read_block(&pool_guard, test_block_id(9))
                .await
                .expect("buffer-pool read failed in test");
            assert_eq!(&g.page()[..10], b"drop-order");
            drop(g);
            drop(table_file);
        });
    }

    #[test]
    fn test_readonly_pool_read_block_returns_immutable_guard_type() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let table_file = fs.create_table_file(104, make_metadata(), false).unwrap();
            let table_file = commit_table_file(&fs, table_file).await;
            write_payload(&fs, &table_file, test_block_id(4), b"guard").await;

            let global = owned_global_pool(64 * 1024 * 1024);
            let pool = owned_readonly_pool(
                test_file_id(104),
                FileKind::TableFile,
                Arc::clone(table_file.sparse_file()),
                &global,
            );
            let pool_guard = pool.pool_guard();
            let guard: ReadonlyBlockGuard = pool
                .read_block(&pool_guard, test_block_id(4))
                .await
                .unwrap();
            assert_eq!(guard.block_id(), 4);
            assert_eq!(&guard.page()[..5], b"guard");
        });
    }

    #[test]
    fn test_global_readonly_pool_uses_custom_arbiter_builder() {
        let temp_dir = TempDir::new().unwrap();
        let fs_owner = build_test_fs_owner_in(temp_dir.path()).unwrap();
        let global = QuiescentBox::new(
            ReadonlyBufferPool::with_capacity_and_arbiter_builder(
                PoolRole::Disk,
                frame_page_bytes(16),
                fs_owner.guard(),
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
