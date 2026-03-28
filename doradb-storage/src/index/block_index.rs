use crate::buffer::guard::{PageExclusiveGuard, PageSharedGuard};
use crate::buffer::page::PageID;
use crate::buffer::{BufferPool, FixedBufferPool, PoolGuard};
use crate::catalog::TableMetadata;
use crate::error::{Error, Result};
use crate::index::block_index_root::{BlockIndexRoot, BlockIndexRoute};
use crate::index::column_block_index::ColumnBlockIndex;
use crate::index::row_block_index::{
    GenericRowBlockIndex, GenericRowBlockIndexMemCursor, RowLocation,
};
use crate::index::util::{Maskable, RowPageCreateRedoCtx};
use crate::quiescent::QuiescentGuard;
use crate::row::{RowID, RowPage};
use crate::table::ColumnStorage;
use std::sync::Arc;

/// Facade of the hybrid block index.
///
/// This type routes lookups between:
/// - the in-memory row-store index (`RowBlockIndex`)
/// - the on-disk column-store index (`ColumnBlockIndex`)
///
/// Routing decisions are made by `BlockIndexRoot`.
pub struct GenericBlockIndex<P: 'static> {
    root: BlockIndexRoot,
    row: GenericRowBlockIndex<P>,
}

/// Compatibility alias for runtime block index backed by `FixedBufferPool`.
pub type BlockIndex = GenericBlockIndex<FixedBufferPool>;

impl<P: BufferPool> GenericBlockIndex<P> {
    /// Creates a block-index facade for one table.
    ///
    /// `pivot_row_id` and `column_root_page_id` define the boundary and root of
    /// persisted columnar data at startup.
    #[inline]
    pub async fn new(
        pool: QuiescentGuard<P>,
        meta_pool_guard: &PoolGuard,
        pivot_row_id: RowID,
        column_root_page_id: PageID,
    ) -> Self {
        let row = GenericRowBlockIndex::new(pool, meta_pool_guard, pivot_row_id).await;
        let root = BlockIndexRoot::new(pivot_row_id, column_root_page_id);
        GenericBlockIndex { root, row }
    }

    /// Creates block index for catalog-table runtime without table-file backing.
    #[inline]
    pub async fn new_catalog(pool: QuiescentGuard<P>, meta_pool_guard: &PoolGuard) -> Self {
        Self::new(pool, meta_pool_guard, 0, 0).await
    }

    /// Returns the in-memory row index height.
    #[inline]
    pub fn height(&self) -> usize {
        self.row.height()
    }

    /// Atomically updates the persisted column index boundary and root page.
    ///
    /// Called after checkpoint/persist updates the column block index.
    #[inline]
    pub async fn update_column_root(&self, pivot_row_id: RowID, column_root_page_id: PageID) {
        self.root
            .update_column_root(pivot_row_id, column_root_page_id)
            .await;
    }

    /// Returns current row/column route boundary metadata.
    #[inline]
    pub fn root_snapshot(&self) -> (RowID, PageID) {
        self.root.snapshot()
    }

    /// Returns current pivot row id.
    #[inline]
    pub fn pivot_row_id(&self) -> RowID {
        self.root.pivot_row_id()
    }

    /// Returns current column-root page id.
    #[inline]
    pub fn column_root_page_id(&self) -> PageID {
        self.root.column_root_page_id()
    }

    /// Returns a shared row page suitable for insert operations.
    #[inline]
    pub async fn get_insert_page<B: BufferPool>(
        &self,
        meta_pool_guard: &PoolGuard,
        mem_pool: &B,
        mem_pool_guard: &PoolGuard,
        metadata: &Arc<TableMetadata>,
        count: usize,
    ) -> PageSharedGuard<RowPage> {
        self.try_get_insert_page_with_redo(
            meta_pool_guard,
            mem_pool,
            mem_pool_guard,
            metadata,
            count,
            None,
        )
        .await
        .expect("block-index get_insert_page should not ignore row-page I/O failures")
    }

    #[inline]
    pub(crate) async fn try_get_insert_page_with_redo<B: BufferPool>(
        &self,
        meta_pool_guard: &PoolGuard,
        mem_pool: &B,
        mem_pool_guard: &PoolGuard,
        metadata: &Arc<TableMetadata>,
        count: usize,
        redo_ctx: Option<RowPageCreateRedoCtx<'_>>,
    ) -> Result<PageSharedGuard<RowPage>> {
        self.row
            .try_get_insert_page_with_redo(
                meta_pool_guard,
                mem_pool,
                mem_pool_guard,
                metadata,
                count,
                redo_ctx,
            )
            .await
    }

    /// Returns an exclusive row page suitable for insert operations.
    #[inline]
    pub async fn get_insert_page_exclusive<B: BufferPool>(
        &self,
        meta_pool_guard: &PoolGuard,
        mem_pool: &B,
        mem_pool_guard: &PoolGuard,
        metadata: &Arc<TableMetadata>,
        count: usize,
    ) -> PageExclusiveGuard<RowPage> {
        self.get_insert_page_exclusive_with_redo(
            meta_pool_guard,
            mem_pool,
            mem_pool_guard,
            metadata,
            count,
            None,
        )
        .await
    }

    #[inline]
    pub(crate) async fn get_insert_page_exclusive_with_redo<B: BufferPool>(
        &self,
        meta_pool_guard: &PoolGuard,
        mem_pool: &B,
        mem_pool_guard: &PoolGuard,
        metadata: &Arc<TableMetadata>,
        count: usize,
        redo_ctx: Option<RowPageCreateRedoCtx<'_>>,
    ) -> PageExclusiveGuard<RowPage> {
        self.row
            .get_insert_page_exclusive_with_redo(
                meta_pool_guard,
                mem_pool,
                mem_pool_guard,
                metadata,
                count,
                redo_ctx,
            )
            .await
    }

    /// Allocates a row page at a specific page id.
    ///
    /// This is primarily used by recovery replay.
    #[inline]
    pub async fn allocate_row_page_at<B: BufferPool>(
        &self,
        meta_pool_guard: &PoolGuard,
        mem_pool: &B,
        mem_pool_guard: &PoolGuard,
        metadata: &Arc<TableMetadata>,
        count: usize,
        page_id: PageID,
    ) -> PageExclusiveGuard<RowPage> {
        self.row
            .allocate_row_page_at(
                meta_pool_guard,
                mem_pool,
                mem_pool_guard,
                metadata,
                count,
                page_id,
            )
            .await
    }

    /// Returns an exclusive insert page back to the in-memory free list cache.
    #[inline]
    pub fn cache_exclusive_insert_page(&self, guard: PageExclusiveGuard<RowPage>) {
        self.row.cache_exclusive_insert_page(guard)
    }

    /// Creates a cursor to scan in-memory block-index leaves.
    #[inline]
    pub fn mem_cursor<'a>(
        &'a self,
        meta_pool_guard: &'a PoolGuard,
    ) -> GenericRowBlockIndexMemCursor<'a, P> {
        self.row.mem_cursor(meta_pool_guard)
    }

    /// Finds the physical location of one row id.
    ///
    /// It first follows the root route decision and may fallback to the column
    /// path when the row lookup misses due to concurrent boundary movement.
    #[inline]
    pub(crate) async fn find_row(
        &self,
        meta_pool_guard: &PoolGuard,
        row_id: RowID,
        storage: Option<&ColumnStorage>,
    ) -> RowLocation {
        self.try_find_row(meta_pool_guard, row_id, storage)
            .await
            .expect("block-index find_row should not ignore persisted lookup I/O failures")
    }

    /// Finds the physical location of one row id with persisted column-path errors surfaced.
    #[inline]
    pub(crate) async fn try_find_row(
        &self,
        meta_pool_guard: &PoolGuard,
        row_id: RowID,
        storage: Option<&ColumnStorage>,
    ) -> Result<RowLocation> {
        debug_assert!(!row_id.is_deleted());
        match self.root.guide(row_id) {
            BlockIndexRoute::Column {
                pivot_row_id,
                root_page_id,
            } => {
                self.find_row_in_column(storage, row_id, pivot_row_id, root_page_id)
                    .await
            }
            BlockIndexRoute::Row => {
                let found = self.row.find_row(meta_pool_guard, row_id).await;
                if !matches!(found, RowLocation::NotFound) {
                    return Ok(found);
                }
                match self.root.try_column(row_id) {
                    Some((pivot_row_id, root_page_id)) => {
                        self.find_row_in_column(storage, row_id, pivot_row_id, root_page_id)
                            .await
                    }
                    None => Ok(RowLocation::NotFound),
                }
            }
        }
    }

    #[inline]
    async fn find_row_in_column(
        &self,
        storage: Option<&ColumnStorage>,
        row_id: RowID,
        pivot_row_id: RowID,
        root_page_id: PageID,
    ) -> Result<RowLocation> {
        let Some(storage) = storage else {
            return Err(Error::ColumnStorageMissing);
        };
        let index = ColumnBlockIndex::new(root_page_id, pivot_row_id, storage.disk_pool());
        match index.find(row_id).await {
            Ok(Some(payload)) => Ok(RowLocation::LwcPage(payload.block_id as PageID)),
            Ok(None) => Ok(RowLocation::NotFound),
            Err(err) => Err(err),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::guard::{FacadePageGuard, PageExclusiveGuard};
    use crate::buffer::page::{BufferPage, INVALID_PAGE_ID, VersionedPageID};
    use crate::buffer::{BufferPool, FixedBufferPool, PoolGuard};
    use crate::error::Validation;
    use crate::latch::LatchFallbackMode;
    use crate::quiescent::{QuiescentBox, QuiescentGuard};
    use std::future::Future;
    use std::sync::Arc;
    use std::sync::Barrier;
    use std::sync::atomic::{AtomicU64, Ordering};

    // Test helper that lets us pause one row-index page fetch at a precise point.
    // The fallback test uses this to hold `try_find_row()` after the initial route
    // decision but before the row-store lookup completes, so we can move the pivot
    // and force the fallback-to-column path deterministically.
    struct StallingBufferPool {
        inner: QuiescentGuard<FixedBufferPool>,
        stall_page_id: AtomicU64,
        entered: Arc<Barrier>,
        release: Arc<Barrier>,
    }

    impl StallingBufferPool {
        #[inline]
        fn new(
            inner: QuiescentGuard<FixedBufferPool>,
            entered: Arc<Barrier>,
            release: Arc<Barrier>,
        ) -> Self {
            StallingBufferPool {
                inner,
                stall_page_id: AtomicU64::new(INVALID_PAGE_ID),
                entered,
                release,
            }
        }

        #[inline]
        fn set_stall_page_id(&self, page_id: PageID) {
            self.stall_page_id.store(page_id, Ordering::Release);
        }
    }

    impl BufferPool for StallingBufferPool {
        #[inline]
        fn capacity(&self) -> usize {
            self.inner.capacity()
        }

        #[inline]
        fn allocated(&self) -> usize {
            self.inner.allocated()
        }

        #[inline]
        fn pool_guard(&self) -> PoolGuard {
            self.inner.pool_guard()
        }

        #[inline]
        fn allocate_page<T: BufferPage>(
            &self,
            guard: &PoolGuard,
        ) -> impl Future<Output = PageExclusiveGuard<T>> + Send {
            self.inner.allocate_page(guard)
        }

        #[inline]
        fn allocate_page_at<T: BufferPage>(
            &self,
            guard: &PoolGuard,
            page_id: PageID,
        ) -> impl Future<Output = Result<PageExclusiveGuard<T>>> + Send {
            self.inner.allocate_page_at(guard, page_id)
        }

        #[inline]
        async fn get_page<T: BufferPage>(
            &self,
            guard: &PoolGuard,
            page_id: PageID,
            mode: LatchFallbackMode,
        ) -> Result<FacadePageGuard<T>> {
            // Only stall the specific spin-mode read we care about; everything
            // else should behave exactly like the wrapped fixed buffer pool.
            if mode == LatchFallbackMode::Spin
                && page_id == self.stall_page_id.load(Ordering::Acquire)
            {
                self.entered.wait();
                self.release.wait();
            }
            self.inner.get_page(guard, page_id, mode).await
        }

        #[inline]
        fn get_page_versioned<T: BufferPage>(
            &self,
            guard: &PoolGuard,
            id: VersionedPageID,
            mode: LatchFallbackMode,
        ) -> impl Future<Output = Result<Option<FacadePageGuard<T>>>> + Send {
            self.inner.get_page_versioned(guard, id, mode)
        }

        #[inline]
        fn deallocate_page<T: BufferPage>(&self, g: PageExclusiveGuard<T>) {
            self.inner.deallocate_page(g)
        }

        #[inline]
        fn get_child_page<T: BufferPage>(
            &self,
            guard: &PoolGuard,
            p_guard: &FacadePageGuard<T>,
            page_id: PageID,
            mode: LatchFallbackMode,
        ) -> impl Future<Output = Result<Validation<FacadePageGuard<T>>>> + Send {
            self.inner.get_child_page(guard, p_guard, page_id, mode)
        }
    }

    #[test]
    fn test_try_find_row_returns_error_when_column_route_has_no_storage() {
        let pool = QuiescentBox::new(
            FixedBufferPool::with_capacity(crate::buffer::PoolRole::Index, 64 * 1024 * 1024)
                .unwrap(),
        );
        let meta_guard = (*pool).pool_guard();
        let blk_idx = smol::block_on(BlockIndex::new(pool.guard(), &meta_guard, 10, 77));

        // Row id 9 is below the pivot, so lookup goes straight to the column path.
        // Without column storage this must surface as an error, not as "not found".
        let err = match smol::block_on(blk_idx.try_find_row(&meta_guard, 9, None)) {
            Ok(_location) => panic!("expected missing-column-storage error, got row location"),
            Err(err) => err,
        };
        assert!(matches!(err, Error::ColumnStorageMissing));
    }

    #[test]
    fn test_try_find_row_returns_error_when_column_fallback_has_no_storage() {
        let inner = QuiescentBox::new(
            FixedBufferPool::with_capacity(crate::buffer::PoolRole::Index, 64 * 1024 * 1024)
                .unwrap(),
        );
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let pool = QuiescentBox::new(StallingBufferPool::new(
            inner.guard(),
            Arc::clone(&entered),
            Arc::clone(&release),
        ));
        let meta_guard = (*pool).pool_guard();
        let blk_idx = smol::block_on(GenericBlockIndex::new(pool.guard(), &meta_guard, 10, 77));
        pool.set_stall_page_id(blk_idx.row.root_page_id());

        std::thread::scope(|s| {
            // Start with row_id == pivot so the first route decision picks the row path.
            let blk_idx = &blk_idx;
            let meta_guard = meta_guard.clone();
            let handle = s.spawn(move || {
                smol::block_on(async { blk_idx.try_find_row(&meta_guard, 10, None).await })
            });

            // Wait until the row-store root fetch is paused, then move the pivot past
            // row_id so the subsequent `try_column()` fallback becomes eligible.
            // This avoids relying on timing-sensitive races to exercise the fallback.
            entered.wait();
            smol::block_on(blk_idx.update_column_root(11, 88));
            release.wait();

            let res = handle.join().unwrap();
            assert!(matches!(res, Err(Error::ColumnStorageMissing)));
        });
    }
}
