use crate::bitmap::AllocMap;
use crate::buffer::arena::QuiescentArena;
use crate::buffer::frame::{BufferFrame, FrameKind};
use crate::buffer::guard::{FacadePageGuard, PageExclusiveGuard};
use crate::buffer::page::{BufferPage, Page, PageID, VersionedPageID};
use crate::buffer::{
    BufferPool, BufferPoolStats, BufferPoolStatsHandle, PoolGuard, PoolIdentity, PoolRole,
    RowPoolRole,
};
use crate::error::Validation::Valid;
use crate::error::{Error, Result, Validation};
use crate::latch::LatchFallbackMode;
use std::mem;

/// A simple buffer pool with fixed size pre-allocated using mmap() and
/// does not support swap/evict.
pub struct FixedBufferPool {
    size: usize,
    // free_list: Mutex<PageID>,
    alloc_map: AllocMap,
    role: PoolRole,
    stats: BufferPoolStatsHandle,
    arena: QuiescentArena,
}

impl FixedBufferPool {
    /// Create a buffer pool with given capacity.
    ///
    /// Pool size if total available bytes of this buffer pool.
    /// We will determine the number of pages accordingly.
    /// We separate pages and frames so that pages are always aligned
    /// to the unit of direct IO and can be flushed via libaio.
    #[inline]
    pub fn with_capacity(role: PoolRole, pool_size: usize) -> Result<Self> {
        role.assert_valid("fixed buffer pool");
        let size = pool_size / (mem::size_of::<BufferFrame>() + mem::size_of::<Page>());
        let arena = QuiescentArena::new(size)?;
        Ok(FixedBufferPool {
            size,
            alloc_map: AllocMap::new(size),
            role,
            stats: BufferPoolStatsHandle::default(),
            arena,
        })
    }

    /// Returns the maximum page number of this pool.
    #[inline]
    pub fn size(&self) -> usize {
        self.size
    }

    #[inline]
    pub(crate) fn identity(&self) -> PoolIdentity {
        self.arena.identity()
    }

    #[inline]
    pub(crate) fn row_pool_role(&self) -> RowPoolRole {
        self.role.row_pool_role()
    }

    /// Returns one snapshot of fixed-pool access counters.
    #[inline]
    pub fn stats(&self) -> BufferPoolStats {
        self.stats.snapshot()
    }

    #[inline]
    async fn get_page_internal<T: 'static>(
        &self,
        guard: &PoolGuard,
        page_id: PageID,
        mode: LatchFallbackMode,
    ) -> FacadePageGuard<T> {
        guard.assert_matches(self.identity(), "fixed buffer pool");
        let keepalive = guard.clone();
        let bf = self.arena.frame_ptr(page_id);
        let g = self
            .arena
            .frame(page_id)
            .latch
            .optimistic_fallback_raw(mode)
            .await;
        FacadePageGuard::new(keepalive, bf, g)
    }

    /// Since all pages are kept in memory, we can use spin mode to eliminate
    /// the cost of async/await calls.
    #[inline]
    pub fn get_page_spin<T: BufferPage>(
        &self,
        guard: &PoolGuard,
        page_id: PageID,
    ) -> FacadePageGuard<T> {
        debug_assert!(
            self.alloc_map.is_allocated(page_id as usize),
            "page not allocated"
        );
        let guard = self.get_page_spin_internal(guard, page_id);
        self.stats.record_cache_hit();
        guard
    }

    #[inline]
    fn get_page_spin_internal<T: 'static>(
        &self,
        guard: &PoolGuard,
        page_id: PageID,
    ) -> FacadePageGuard<T> {
        guard.assert_matches(self.identity(), "fixed buffer pool");
        let keepalive = guard.clone();
        let bf = self.arena.frame_ptr(page_id);
        let g = self.arena.frame(page_id).latch.optimistic_spin_raw();
        FacadePageGuard::new(keepalive, bf, g)
    }
}

impl BufferPool for FixedBufferPool {
    #[inline]
    fn capacity(&self) -> usize {
        self.size
    }

    #[inline]
    fn allocated(&self) -> usize {
        self.alloc_map.allocated()
    }

    #[inline]
    fn pool_guard(&self) -> PoolGuard {
        self.arena.guard()
    }

    // allocate a new page with exclusive lock.
    #[inline]
    async fn allocate_page<T: BufferPage>(&self, guard: &PoolGuard) -> PageExclusiveGuard<T> {
        match self.alloc_map.try_allocate() {
            Some(page_id) => self.arena.init_page(guard, page_id as PageID),
            None => {
                panic!("buffer pool full");
            }
        }
    }

    #[inline]
    async fn allocate_page_at<T: BufferPage>(
        &self,
        guard: &PoolGuard,
        page_id: PageID,
    ) -> Result<PageExclusiveGuard<T>> {
        if self.alloc_map.allocate_at(page_id as usize) {
            Ok(self.arena.init_page(guard, page_id as PageID))
        } else {
            Err(Error::BufferPageAlreadyAllocated)
        }
    }

    /// Returns the page guard with given page id.
    /// Caller should make sure page id is valid.
    #[inline]
    async fn get_page<T: BufferPage>(
        &self,
        guard: &PoolGuard,
        page_id: PageID,
        mode: LatchFallbackMode,
    ) -> Result<FacadePageGuard<T>> {
        debug_assert!(
            self.alloc_map.is_allocated(page_id as usize),
            "page not allocated"
        );
        let guard = self.get_page_internal(guard, page_id, mode).await;
        self.stats.record_cache_hit();
        Ok(guard)
    }

    #[inline]
    async fn get_page_versioned<T: BufferPage>(
        &self,
        guard: &PoolGuard,
        id: VersionedPageID,
        mode: LatchFallbackMode,
    ) -> Result<Option<FacadePageGuard<T>>> {
        if !self.alloc_map.is_allocated(id.page_id as usize) {
            return Ok(None);
        }
        let g = self.get_page_internal(guard, id.page_id, mode).await;
        let bf = g.bf();
        if bf.kind() == FrameKind::Uninitialized || bf.generation() != id.generation {
            if g.is_exclusive() {
                g.rollback_exclusive_version_change();
            }
            return Ok(None);
        }
        self.stats.record_cache_hit();
        Ok(Some(g))
    }

    /// Deallocate page.
    #[inline]
    fn deallocate_page<T: BufferPage>(&self, mut g: PageExclusiveGuard<T>) {
        let page_id = g.page_id();
        g.page_mut().zero();
        g.bf_mut().ctx = None;
        T::deinit_frame(g.bf_mut());
        g.bf_mut().bump_generation();
        let res = self.alloc_map.deallocate(page_id as usize);
        debug_assert!(res);
    }

    /// Get child page by page id provided by parent page.
    /// The parent page guard should be provided because other thread may change page
    /// id concurrently, and the input page id may not be valid through the function
    /// call. So version must be validated before returning the buffer frame.
    #[inline]
    async fn get_child_page<T: BufferPage>(
        &self,
        guard: &PoolGuard,
        p_guard: &FacadePageGuard<T>,
        page_id: PageID,
        mode: LatchFallbackMode,
    ) -> Result<Validation<FacadePageGuard<T>>> {
        debug_assert!(
            self.alloc_map.is_allocated(page_id as usize),
            "page not allocated"
        );
        let g = self.get_page_internal::<T>(guard, page_id, mode).await;
        // apply lock coupling.
        // the validation make sure parent page does not change until child
        // page is acquired.
        if p_guard.validate_bool() {
            self.stats.record_cache_hit();
            return Ok(Valid(g));
        }
        if g.is_exclusive() {
            g.rollback_exclusive_version_change();
        }
        Ok(Validation::Invalid)
    }
}

// SAFETY: `FixedBufferPool` shares access through atomics, `AllocMap`, and the
// quiescent arena; it does not hand out thread-affine resources.
unsafe impl Send for FixedBufferPool {}

// SAFETY: shared references coordinate mutable page access through page latches
// and allocation state, so `&FixedBufferPool` is safe to share between threads.
unsafe impl Sync for FixedBufferPool {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::BlockNode;
    use crate::quiescent::QuiescentBox;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread;
    use std::time::Duration;

    fn test_pool() -> QuiescentBox<FixedBufferPool> {
        QuiescentBox::new(FixedBufferPool::with_capacity(PoolRole::Meta, 64 * 1024 * 1024).unwrap())
    }

    #[test]
    fn test_fixed_buffer_pool() {
        smol::block_on(async {
            let pool = test_pool();
            let pool_guard = FixedBufferPool::pool_guard(&pool);
            {
                let g = pool.allocate_page::<BlockNode>(&pool_guard).await;
                assert_eq!(g.page_id(), 0);
            }
            {
                let g = pool.allocate_page::<BlockNode>(&pool_guard).await;
                assert_eq!(g.page_id(), 1);
                pool.deallocate_page(g);
                let g = pool.allocate_page::<BlockNode>(&pool_guard).await;
                assert_eq!(g.page_id(), 1);
            }
            {
                let g = pool
                    .get_page::<BlockNode>(&pool_guard, 0, LatchFallbackMode::Spin)
                    .await
                    .expect("buffer-pool read failed in test")
                    .downgrade();
                assert_eq!(g.page_id(), 0);
            }
            {
                let g = pool.allocate_page::<BlockNode>(&pool_guard).await;
                let page_id = g.page_id();
                drop(g);
                let g = pool.get_page_spin::<BlockNode>(&pool_guard, page_id);
                assert!(g.page_id() == page_id);
                drop(g);

                let p = pool.allocate_page::<BlockNode>(&pool_guard).await;
                let p = p.downgrade().facade();
                let c = pool
                    .get_child_page::<BlockNode>(
                        &pool_guard,
                        &p,
                        page_id,
                        LatchFallbackMode::Shared,
                    )
                    .await;
                let c = c.unwrap();
                drop(c);
            }
            {
                let g = pool.allocate_page::<BlockNode>(&pool_guard).await;
                let page_id = g.page_id();
                let stale_versioned = g.versioned_page_id();
                let first_generation = stale_versioned.generation;
                pool.deallocate_page(g);

                let g = pool.allocate_page::<BlockNode>(&pool_guard).await;
                assert_eq!(g.page_id(), page_id);
                assert_eq!(g.bf().generation(), first_generation + 2);
                let current_versioned = g.versioned_page_id();
                drop(g);

                let g = pool
                    .get_page_versioned::<BlockNode>(
                        &pool_guard,
                        current_versioned,
                        LatchFallbackMode::Shared,
                    )
                    .await
                    .unwrap();
                assert!(g.is_some());

                let g = pool
                    .get_page_versioned::<BlockNode>(
                        &pool_guard,
                        stale_versioned,
                        LatchFallbackMode::Shared,
                    )
                    .await
                    .unwrap();
                assert!(g.is_none());
            }
            {
                let g = pool.allocate_page::<BlockNode>(&pool_guard).await;
                let page_id = g.page_id();
                let versioned = g.versioned_page_id();
                drop(g);

                // Keep an optimistic guard, then reuse the page slot.
                let stale_guard = pool
                    .get_page_versioned::<BlockNode>(
                        &pool_guard,
                        versioned,
                        LatchFallbackMode::Shared,
                    )
                    .await
                    .unwrap()
                    .unwrap();
                let g = pool
                    .get_page::<BlockNode>(&pool_guard, page_id, LatchFallbackMode::Exclusive)
                    .await
                    .expect("buffer-pool read failed in test")
                    .lock_exclusive_async()
                    .await
                    .unwrap();
                pool.deallocate_page(g);
                let g = pool.allocate_page::<BlockNode>(&pool_guard).await;
                assert_eq!(g.page_id(), page_id);
                drop(g);

                assert!(stale_guard.lock_shared_async().await.is_none());
            }
        })
    }

    #[test]
    fn test_fixed_buffer_pool_stats_track_resident_hits_only() {
        smol::block_on(async {
            let pool = test_pool();
            let pool_guard = FixedBufferPool::pool_guard(&pool);
            let page = pool.allocate_page::<BlockNode>(&pool_guard).await;
            let page_id = page.page_id();
            drop(page);

            let baseline = pool.stats();
            let guard = pool
                .get_page::<BlockNode>(&pool_guard, page_id, LatchFallbackMode::Shared)
                .await
                .expect("fixed-pool read failed in test");
            drop(guard);

            let delta = pool.stats().delta_since(baseline);
            assert_eq!(delta.cache_hits, 1);
            assert_eq!(delta.cache_misses, 0);
            assert_eq!(delta.queued_reads, 0);
            assert_eq!(delta.running_reads, 0);
            assert_eq!(delta.completed_reads, 0);
        });
    }

    #[test]
    fn test_facade_page_guard_lock_shared_and_try_into_shared() {
        smol::block_on(async {
            let pool = test_pool();
            let pool_guard = FixedBufferPool::pool_guard(&pool);
            let g = pool.allocate_page::<BlockNode>(&pool_guard).await;
            let page_id = g.page_id();
            drop(g);

            let g = pool
                .get_page::<BlockNode>(&pool_guard, page_id, LatchFallbackMode::Spin)
                .await
                .expect("buffer-pool read failed in test");
            let g = g.lock_shared_async().await;
            assert!(g.is_some());
            let g = g.unwrap();
            assert_eq!(g.page_id(), page_id);

            let g = g.facade(false);
            let g = g.try_into_shared();
            assert!(g.is_some());
            let g = g.unwrap();
            assert_eq!(g.page_id(), page_id);
            drop(g);

            let g = pool
                .get_page::<BlockNode>(&pool_guard, page_id, LatchFallbackMode::Spin)
                .await
                .expect("buffer-pool read failed in test");
            assert!(g.try_into_shared().is_none());

            let g = pool
                .get_page::<BlockNode>(&pool_guard, page_id, LatchFallbackMode::Spin)
                .await
                .expect("buffer-pool read failed in test")
                .lock_exclusive_async()
                .await
                .unwrap();
            let versioned = g.versioned_page_id();
            drop(g);

            let stale_guard = pool
                .get_page_versioned::<BlockNode>(&pool_guard, versioned, LatchFallbackMode::Shared)
                .await
                .unwrap()
                .unwrap();

            let g = pool
                .get_page::<BlockNode>(&pool_guard, page_id, LatchFallbackMode::Spin)
                .await
                .expect("buffer-pool read failed in test")
                .lock_exclusive_async()
                .await
                .unwrap();
            pool.deallocate_page(g);
            let g = pool.allocate_page::<BlockNode>(&pool_guard).await;
            assert_eq!(g.page_id(), page_id);
            drop(g);

            assert!(stale_guard.lock_shared_async().await.is_none());
        })
    }

    #[test]
    fn test_facade_page_guard_lock_exclusive_and_try_into_exclusive() {
        smol::block_on(async {
            let pool = test_pool();
            let pool_guard = FixedBufferPool::pool_guard(&pool);
            let g = pool.allocate_page::<BlockNode>(&pool_guard).await;
            let page_id = g.page_id();
            drop(g);

            let g = pool
                .get_page::<BlockNode>(&pool_guard, page_id, LatchFallbackMode::Spin)
                .await
                .expect("buffer-pool read failed in test");
            let g = g.lock_exclusive_async().await;
            assert!(g.is_some());
            let g = g.unwrap();
            assert_eq!(g.page_id(), page_id);

            let g = g.facade(false);
            let g = g.try_into_exclusive();
            assert!(g.is_some());
            let g = g.unwrap();
            assert_eq!(g.page_id(), page_id);

            let g = g.facade(false);
            let g = g.lock_exclusive_async().await;
            assert!(g.is_some());
            drop(g);

            let g = pool
                .get_page::<BlockNode>(&pool_guard, page_id, LatchFallbackMode::Spin)
                .await
                .expect("buffer-pool read failed in test");
            assert!(g.try_into_exclusive().is_none());

            let g = pool
                .get_page::<BlockNode>(&pool_guard, page_id, LatchFallbackMode::Spin)
                .await
                .expect("buffer-pool read failed in test")
                .lock_exclusive_async()
                .await
                .unwrap();
            let versioned = g.versioned_page_id();
            drop(g);

            let stale_guard = pool
                .get_page_versioned::<BlockNode>(&pool_guard, versioned, LatchFallbackMode::Shared)
                .await
                .unwrap()
                .unwrap();

            let g = pool
                .get_page::<BlockNode>(&pool_guard, page_id, LatchFallbackMode::Spin)
                .await
                .expect("buffer-pool read failed in test")
                .lock_exclusive_async()
                .await
                .unwrap();
            pool.deallocate_page(g);
            let g = pool.allocate_page::<BlockNode>(&pool_guard).await;
            assert_eq!(g.page_id(), page_id);
            drop(g);

            assert!(stale_guard.lock_exclusive_async().await.is_none());
        })
    }

    #[test]
    #[should_panic(expected = "block until exclusive by shared lock is not allowed")]
    fn test_facade_page_guard_lock_exclusive_async_panics_on_shared_state() {
        smol::block_on(async {
            let pool = test_pool();
            let pool_guard = FixedBufferPool::pool_guard(&pool);
            let g = pool.allocate_page::<BlockNode>(&pool_guard).await;
            let page_id = g.page_id();
            drop(g);

            let g = pool
                .get_page::<BlockNode>(&pool_guard, page_id, LatchFallbackMode::Spin)
                .await
                .expect("buffer-pool read failed in test")
                .lock_shared_async()
                .await
                .unwrap();
            let g = g.facade(false);

            let _ = g.lock_exclusive_async().await;
        })
    }

    #[test]
    #[should_panic(expected = "block until exclusive by shared lock is not allowed")]
    fn test_facade_page_guard_lock_shared_async_panics_on_exclusive_state() {
        smol::block_on(async {
            let pool = test_pool();
            let pool_guard = FixedBufferPool::pool_guard(&pool);
            let g = pool.allocate_page::<BlockNode>(&pool_guard).await;
            let page_id = g.page_id();
            drop(g);

            let g = pool
                .get_page::<BlockNode>(&pool_guard, page_id, LatchFallbackMode::Spin)
                .await
                .expect("buffer-pool read failed in test")
                .lock_exclusive_async()
                .await
                .unwrap();
            let g = g.facade(false);

            let _ = g.lock_shared_async().await;
        })
    }

    #[test]
    fn test_fixed_buffer_pool_drop_waits_for_outstanding_guard() {
        smol::block_on(async {
            let pool = QuiescentBox::new(
                FixedBufferPool::with_capacity(PoolRole::Meta, 8 * 1024 * 1024).unwrap(),
            );
            let guard = {
                let pool_guard = FixedBufferPool::pool_guard(&pool);
                pool.allocate_page::<BlockNode>(&pool_guard).await
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
    #[should_panic(expected = "pool guard identity mismatch")]
    fn test_fixed_buffer_pool_panics_on_foreign_guard() {
        smol::block_on(async {
            let pool1 = test_pool();
            let pool2 = test_pool();
            let pool1_guard = FixedBufferPool::pool_guard(&pool1);
            let pool2_guard = FixedBufferPool::pool_guard(&pool2);

            let page = pool1.allocate_page::<BlockNode>(&pool1_guard).await;
            let page_id = page.page_id();
            drop(page);

            let _ = pool1
                .get_page::<BlockNode>(&pool2_guard, page_id, LatchFallbackMode::Shared)
                .await;
        });
    }
}
