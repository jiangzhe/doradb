use crate::buffer::frame::{BufferFrame, FrameKind};
use crate::buffer::guard::{
    FacadePageGuard, PageExclusiveGuard, PageLatchGuard, RowVersionMapGuard,
};
use crate::buffer::page::{BufferPage, Page, VersionedPageID};
use crate::buffer::util::{deallocate_frame_and_page_arrays, initialize_frame_and_page_arrays};
use crate::buffer::{PoolGuard, PoolIdentity};
use crate::error::ResourceResult;
use crate::id::PageID;
use crate::ptr::UnsafePtr;
use crate::quiescent::{QuiescentBox, QuiescentGuard};
use std::ptr::{drop_in_place, from_ref};

#[cfg(test)]
pub(crate) use self::tests::outstanding_base_guard_count;

/// Cloneable guard that keeps arena frame/page mappings alive while accessed.
#[derive(Clone)]
pub(crate) struct ArenaGuard {
    inner: UnsafePtr<ArenaInner>,
    keepalive: PoolGuard,
}

impl ArenaGuard {
    #[inline]
    fn inner(&self) -> &ArenaInner {
        // SAFETY: `ArenaGuard` is created only from a stable `QuiescentArena`
        // address and retains one pool keepalive for the full guard lifetime.
        unsafe { &*self.inner.0 }
    }

    /// Returns the current frame state for `page_id`.
    #[inline]
    pub(crate) fn frame_kind(&self, page_id: PageID) -> FrameKind {
        self.inner().frame_kind(page_id)
    }

    /// Atomically swaps the frame state when the observed state matches `old_kind`.
    #[inline]
    pub(crate) fn compare_exchange_frame_kind(
        &self,
        page_id: PageID,
        old_kind: FrameKind,
        new_kind: FrameKind,
    ) -> FrameKind {
        self.inner()
            .compare_exchange_frame_kind(page_id, old_kind, new_kind)
    }

    /// Tries to acquire exclusive access to the raw page at `page_id`.
    #[inline]
    pub(crate) fn try_lock_page_exclusive(
        &self,
        page_id: PageID,
    ) -> Option<PageExclusiveGuard<Page>> {
        self.inner()
            .try_lock_page_exclusive_with(&self.keepalive, page_id)
    }
}

// SAFETY: `ArenaGuard` only shares a stable raw pointer to `ArenaInner` plus
// one retained sync keepalive guard. Callers still need external latch/pool
// synchronization before dereferencing frame/page memory across threads.
unsafe impl Send for ArenaGuard {}

// SAFETY: see `Send` above.
unsafe impl Sync for ArenaGuard {}

/// One-shot owner of the mmap-backed frame/page arrays.
///
/// The frame/page mappings are installed exactly once during construction and
/// must never be reallocated, remapped, or replaced for the full lifetime of
/// this owner. That stable-address invariant is what allows raw frame/page
/// pointers to remain valid while a paired quiescent guard is retained.
pub(crate) struct ArenaInner {
    frames: *mut BufferFrame,
    pages: *mut Page,
    capacity: usize,
}

impl ArenaInner {
    #[inline]
    fn new(capacity: usize) -> ResourceResult<Self> {
        // SAFETY: `ArenaInner::drop` destroys initialized frames and unmaps
        // both regions exactly once after the leading keepalive box drains.
        let (frames, pages) = unsafe { initialize_frame_and_page_arrays(capacity)? };
        Ok(Self {
            frames,
            pages,
            capacity,
        })
    }

    /// Returns a raw pointer to the frame header for `page_id`.
    #[inline]
    pub(crate) fn frame_ptr(&self, page_id: PageID) -> UnsafePtr<BufferFrame> {
        debug_assert!(usize::from(page_id) < self.capacity);
        // SAFETY: frame memory is one contiguous mmap region indexed by page id.
        unsafe { UnsafePtr(self.frames.add(usize::from(page_id))) }
    }

    /// Returns the frame header for `page_id`.
    #[inline]
    pub(crate) fn frame(&self, page_id: PageID) -> &BufferFrame {
        let ptr = self.frame_ptr(page_id);
        // SAFETY: `ptr` indexes the stable frame mmap owned by this arena.
        unsafe { &*ptr.0 }
    }

    /// Returns the current frame state for `page_id`.
    #[inline]
    pub(crate) fn frame_kind(&self, page_id: PageID) -> FrameKind {
        self.frame(page_id).kind()
    }

    /// Atomically swaps the frame state when the observed state matches `old_kind`.
    #[inline]
    pub(crate) fn compare_exchange_frame_kind(
        &self,
        page_id: PageID,
        old_kind: FrameKind,
        new_kind: FrameKind,
    ) -> FrameKind {
        self.frame(page_id)
            .compare_exchange_kind(old_kind, new_kind)
    }

    /// Tries to lock `page_id` exclusively using the supplied pool keepalive.
    #[inline]
    pub(crate) fn try_lock_page_exclusive_with(
        &self,
        keepalive: &PoolGuard,
        page_id: PageID,
    ) -> Option<PageExclusiveGuard<Page>> {
        let bf = self.frame_ptr(page_id);
        let g = self.frame(page_id).latch.try_exclusive_raw();
        g.map(|g| {
            FacadePageGuard::new(PageLatchGuard::new(keepalive.clone(), g), bf).must_exclusive()
        })
    }
}

impl Drop for ArenaInner {
    #[inline]
    fn drop(&mut self) {
        // SAFETY: `QuiescentArena` declares `keepalive` before `state`, so the
        // keepalive owner waits for all guards before `ArenaInner::drop`
        // destroys frames and unmaps the backing regions.
        unsafe {
            for frame_id in 0..self.capacity {
                drop_in_place(self.frames.add(frame_id));
            }
            deallocate_frame_and_page_arrays(self.frames, self.pages, self.capacity);
        }
    }
}

// SAFETY: moving this owner transfers the mapping pointers without moving the
// frames or pages. BufferFrame is Send, and mapping allocation/destruction has
// no thread affinity. QuiescentArena drains keepalives before dropping us.
unsafe impl Send for ArenaInner {}

// SAFETY: mapping pointers and capacity are immutable after construction.
// Shared frame access uses BufferFrame's atomics and latches; mutable page and
// context access requires the frame latch and a matching pool keepalive.
unsafe impl Sync for ArenaInner {}

/// Quiescent owner for one stable frame/page arena.
pub(crate) struct QuiescentArena {
    // Field order is part of the safety contract. `keepalive` must drop before
    // `state` so owner teardown waits for all outstanding quiescent guards
    // before the frame/page mappings are reclaimed.
    keepalive: QuiescentBox<()>,
    identity: PoolIdentity,
    state: ArenaInner,
}

impl QuiescentArena {
    /// Allocates one arena with `capacity` frame/page slots.
    #[inline]
    pub(crate) fn new(capacity: usize) -> ResourceResult<Self> {
        let keepalive = QuiescentBox::new(());
        let identity = keepalive.owner_identity();
        Ok(Self {
            identity,
            keepalive,
            state: ArenaInner::new(capacity)?,
        })
    }

    /// Returns the runtime identity of this arena owner.
    #[inline]
    pub(crate) fn identity(&self) -> PoolIdentity {
        self.identity
    }

    #[inline]
    fn quiescent_guard(&self) -> QuiescentGuard<()> {
        self.keepalive.guard()
    }

    /// Creates one independent clone root for accesses into this arena.
    ///
    /// Each call acquires the arena's pool-global quiescent counter once and
    /// wraps that direct guard in a new `Arc`. Callers must keep the resulting
    /// base guard at a natural ownership boundary and clone it for individual
    /// page or task lifetimes.
    #[inline]
    pub(crate) fn create_base_guard(&self) -> PoolGuard {
        PoolGuard::new(self.identity, self.quiescent_guard().into_sync())
    }

    /// Converts a matching pool guard into an arena guard.
    /// The containing owner must remain at a stable address while arena guards
    /// exist; production pools establish this through their QuiescentBox owner.
    #[inline]
    pub(crate) fn arena_guard(&self, guard: PoolGuard) -> ArenaGuard {
        guard.assert_matches(self.identity, "arena guard");
        let inner = UnsafePtr(from_ref(&self.state).cast_mut());
        ArenaGuard {
            inner,
            keepalive: guard,
        }
    }

    /// Returns a raw pointer to the frame header for `page_id`.
    #[inline]
    pub(crate) fn frame_ptr(&self, page_id: PageID) -> UnsafePtr<BufferFrame> {
        self.state.frame_ptr(page_id)
    }

    /// Returns the frame header for `page_id`.
    #[inline]
    pub(crate) fn frame(&self, page_id: PageID) -> &BufferFrame {
        self.state.frame(page_id)
    }

    /// Pins resident row metadata without consulting or loading the page image.
    /// Returns `None` for an out-of-range, stale, or uninitialized identity.
    /// Purge can encounter a stale identity when checkpoint retirement precedes
    /// a later-committing writer's undo eligibility. Eviction alone preserves
    /// both the identity and its resident version map.
    ///
    /// A matching initialized identity must have runtime row-version context;
    /// absent or recovery context is an invariant violation, not missing undo.
    pub(crate) async fn get_row_version_map(
        &self,
        guard: &PoolGuard,
        id: VersionedPageID,
    ) -> Option<RowVersionMapGuard> {
        guard.assert_matches(self.identity, "arena row-version map");
        let offset = usize::try_from(id.page_id.as_u64()).ok()?;
        if offset >= self.state.capacity {
            return None;
        }
        let frame = self.frame(id.page_id);
        if frame.generation() != id.generation {
            return None;
        }
        // Bind the keepalive before raw latch state, preserving reverse local
        // drop order even if acquisition is cancelled while suspended.
        let keepalive = guard.clone();
        // Existing generic-latch wait: the exclusive holder produces progress.
        // Poison/shutdown do not cancel it; the acquisition future owns wait
        // cleanup and the pool keepalive survives until after latch release.
        let raw = frame.latch.shared_async_raw().await;
        let latch = PageLatchGuard::new(keepalive, raw);
        if frame.generation() != id.generation || frame.kind() == FrameKind::Uninitialized {
            return None;
        }
        // Successful under-latch validation is the access linearization point.
        // Only now may non-atomic context be inspected. Never inspect page bytes.
        frame.unwrap_vmap();
        Some(RowVersionMapGuard::new(latch, self.frame_ptr(id.page_id)))
    }

    /// Initializes an allocated frame/page slot for logical page type `T`.
    #[inline]
    pub(crate) fn init_page<T: BufferPage>(
        &self,
        keepalive: &PoolGuard,
        page_id: PageID,
    ) -> PageExclusiveGuard<T> {
        keepalive.assert_matches(self.identity, "arena init page");
        let keepalive = keepalive.clone();
        let bf = self.frame_ptr(page_id);
        let mut guard = {
            let frame = self.frame(page_id);
            let g = frame.latch.try_exclusive_raw().unwrap();
            frame.bump_generation();
            FacadePageGuard::<T>::new(PageLatchGuard::new(keepalive, g), bf.clone())
                .must_exclusive()
        };
        {
            let frame = guard.bf_mut();
            debug_assert_eq!(frame as *mut BufferFrame, bf.0);
            frame.page_id = page_id;
            frame.ctx = None;
            debug_assert_eq!(frame.kind(), FrameKind::Uninitialized);
            frame.set_kind(FrameKind::Hot);
            frame.set_dirty(true);
            frame.clear_persisted_block_key();
        }
        guard.page_mut().zero();
        guard
    }

    /// Tries to acquire exclusive access to the raw page at `page_id`.
    #[inline]
    pub(crate) fn try_lock_page_exclusive(
        &self,
        keepalive: &PoolGuard,
        page_id: PageID,
    ) -> Option<PageExclusiveGuard<Page>> {
        keepalive.assert_matches(self.identity, "arena try_lock_page_exclusive");
        self.state.try_lock_page_exclusive_with(keepalive, page_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::test_pool_guards_share_keepalive_root;
    use crate::catalog::{StorageColumnFlags, StorageColumnSpec, TableMetadata};
    use crate::id::{RowID, TrxID};
    use crate::row::RowPage;
    use crate::value::ValKind;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::sync::Arc;
    use std::thread;

    /// Returns the number of independently created base guards.
    #[inline]
    pub(crate) fn outstanding_base_guard_count(arena: &QuiescentArena) -> usize {
        arena.keepalive.outstanding_guard_count()
    }

    fn init_row_page(arena: &QuiescentArena, guard: &PoolGuard) -> PageExclusiveGuard<RowPage> {
        let metadata = TableMetadata::try_new(
            vec![StorageColumnSpec::new(
                ValKind::I32,
                StorageColumnFlags::empty(),
            )],
            vec![],
        )
        .unwrap();
        let mut page = arena.init_page::<RowPage>(guard, PageID::new(0));
        page.page_mut().init(RowID::new(100), 4, &metadata.col);
        page.bf_mut()
            .init_undo_map(Arc::clone(&metadata.col), RowID::new(100), 4);
        page
    }

    #[test]
    fn test_arena_shared_metadata_future_and_owner_cross_threads() {
        let arena = QuiescentBox::new(QuiescentArena::new(1).unwrap());
        let root = arena.create_base_guard();
        let page = init_row_page(&arena, &root);
        let id = page.versioned_page_id();
        page.unwrap_vmap().set_create_cts(TrxID::new(42));
        drop(page);

        // Sending the borrowed future requires QuiescentArena: Sync and proves
        // that the direct async accessor still meets BufferPool's Send contract.
        let future = arena.get_row_version_map(&root, id);
        let map = thread::scope(|scope| {
            scope
                .spawn(move || smol::block_on(future))
                .join()
                .unwrap()
                .unwrap()
        });
        assert_eq!(map.version_map().create_cts(), TrxID::new(42));
        drop(map);

        // Moving the pinned owner requires QuiescentArena: Send. Its mappings
        // and inline ArenaInner stay stable, and teardown runs on this worker.
        thread::spawn(move || {
            let map = smol::block_on(arena.get_row_version_map(&root, id)).unwrap();
            assert_eq!(map.version_map().create_cts(), TrxID::new(42));
            drop(map);
            drop(root);
            assert_eq!(outstanding_base_guard_count(&arena), 0);
            drop(arena);
        })
        .join()
        .unwrap();
    }

    #[test]
    fn test_row_metadata_latch_pins_context_and_rejects_reuse() {
        smol::block_on(async {
            let arena = QuiescentArena::new(1).unwrap();
            let root = arena.create_base_guard();
            let page = init_row_page(&arena, &root);
            let id = page.versioned_page_id();
            drop(page);
            let map = arena.get_row_version_map(&root, id).await.unwrap();
            assert!(
                map.version_map()
                    .try_write_row(RowID::new(100))
                    .unwrap()
                    .is_none()
            );
            assert!(arena.try_lock_page_exclusive(&root, id.page_id).is_none());
            drop(root);
            assert_eq!(outstanding_base_guard_count(&arena), 1);
            drop(map);
            assert_eq!(outstanding_base_guard_count(&arena), 0);

            let root = arena.create_base_guard();
            let mut old = arena.try_lock_page_exclusive(&root, id.page_id).unwrap();
            old.bf_mut().ctx = None;
            old.bf_mut().set_kind(FrameKind::Uninitialized);
            drop(old);
            assert!(arena.get_row_version_map(&root, id).await.is_none());
            let new = init_row_page(&arena, &root);
            let replacement = new.versioned_page_id();
            new.unwrap_vmap().set_create_cts(TrxID::new(42));
            drop(new);
            assert!(arena.get_row_version_map(&root, id).await.is_none());
            let map = arena.get_row_version_map(&root, replacement).await.unwrap();
            assert_eq!(map.version_map().create_cts(), TrxID::new(42));
            for page_id in [PageID::new(1), PageID::new(u64::MAX)] {
                assert!(
                    arena
                        .get_row_version_map(&root, VersionedPageID { page_id, ..id })
                        .await
                        .is_none()
                );
            }
        });
    }

    #[test]
    fn test_row_metadata_wait_revalidates_identity_and_cancellation_drains() {
        smol::block_on(async {
            for replace in [false, true] {
                let arena = QuiescentArena::new(1).unwrap();
                let root = arena.create_base_guard();
                let mut page = init_row_page(&arena, &root);
                let id = page.versioned_page_id();
                let mut pending = Box::pin(arena.get_row_version_map(&root, id));
                assert!(futures::poll!(pending.as_mut()).is_pending());
                page.bf_mut().ctx = None;
                page.bf_mut().set_kind(FrameKind::Uninitialized);
                drop(page);
                if replace {
                    drop(init_row_page(&arena, &root));
                }
                assert!(pending.await.is_none());
            }

            let arena = QuiescentArena::new(1).unwrap();
            let root = arena.create_base_guard();
            let page = init_row_page(&arena, &root);
            let id = page.versioned_page_id();
            let mut pending = Box::pin(arena.get_row_version_map(&root, id));
            assert!(futures::poll!(pending.as_mut()).is_pending());
            drop(page);
            assert_eq!(outstanding_base_guard_count(&arena), 1);
            drop(pending);
            drop(root);
            assert_eq!(outstanding_base_guard_count(&arena), 0);
            let root = arena.create_base_guard();
            assert!(arena.try_lock_page_exclusive(&root, id.page_id).is_some());
        });
    }

    #[test]
    #[should_panic(expected = "pool guard identity mismatch")]
    fn test_row_metadata_rejects_foreign_pool_guard() {
        let arena = QuiescentArena::new(1).unwrap();
        let foreign = QuiescentArena::new(1).unwrap();
        smol::block_on(arena.get_row_version_map(
            &foreign.create_base_guard(),
            VersionedPageID {
                page_id: PageID::new(0),
                generation: 0,
            },
        ));
    }

    #[test]
    fn test_row_metadata_matching_identity_requires_runtime_context() {
        for recovery in [false, true] {
            let arena = QuiescentArena::new(1).unwrap();
            let root = arena.create_base_guard();
            let mut page = arena.init_page::<RowPage>(&root, PageID::new(0));
            let id = page.versioned_page_id();
            if recovery {
                page.bf_mut().init_recover_map(TrxID::new(42));
            }
            drop(page);
            assert!(
                catch_unwind(AssertUnwindSafe(|| {
                    smol::block_on(arena.get_row_version_map(&root, id))
                }))
                .is_err()
            );
            assert!(arena.try_lock_page_exclusive(&root, id.page_id).is_some());
        }
    }

    #[test]
    #[should_panic(expected = "pool guard identity mismatch")]
    fn test_arena_guard_panics_on_foreign_guard() {
        let arena1 = Box::leak(Box::new(QuiescentArena::new(1).unwrap()));
        let arena2 = Box::leak(Box::new(QuiescentArena::new(1).unwrap()));
        let foreign_guard = arena2.create_base_guard();
        let _ = arena1.arena_guard(foreign_guard);
    }

    #[test]
    fn test_base_guard_creation_and_clone_lifecycle() {
        let arena = QuiescentArena::new(1).unwrap();
        assert_eq!(outstanding_base_guard_count(&arena), 0);

        let first = arena.create_base_guard();
        let first_clone = first.clone();
        assert_eq!(outstanding_base_guard_count(&arena), 1);
        assert!(test_pool_guards_share_keepalive_root(&first, &first_clone));

        let second = arena.create_base_guard();
        assert_eq!(outstanding_base_guard_count(&arena), 2);
        assert!(!test_pool_guards_share_keepalive_root(&first, &second));
        assert_eq!(first.identity(), second.identity());

        drop(first);
        drop(first_clone);
        assert_eq!(outstanding_base_guard_count(&arena), 1);
        drop(second);
        assert_eq!(outstanding_base_guard_count(&arena), 0);
    }
}
