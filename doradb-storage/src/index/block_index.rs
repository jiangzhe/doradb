use crate::buffer::guard::{PageExclusiveGuard, PageSharedGuard};
use crate::buffer::{BufferPool, FixedBufferPool, PoolGuard};
use crate::catalog::TableColumnLayout;
use crate::error::{Error, InternalError, Result};
use crate::file::cow_file::SUPER_BLOCK_ID;
use crate::id::{BlockID, PageID, RowID};
use crate::index::block_index_root::{BlockIndexRoot, BlockIndexRoute};
use crate::index::column_block_index::ColumnBlockIndex;
use crate::index::row_page_index::{RowLocation, RowPageIndex, RowPageIndexMemCursor};
use crate::index::util::{Maskable, RowPageCreateRedoCtx};
use crate::quiescent::QuiescentGuard;
use crate::row::RowPage;
use crate::table::ColumnStorage;
use error_stack::Report;
use std::sync::Arc;

/// Facade of the hybrid block index.
///
/// This type routes lookups between:
/// - the in-memory row-store index (`RowPageIndex`)
/// - the on-disk column-store index (`ColumnBlockIndex`)
///
/// Routing decisions are made by `BlockIndexRoot`.
pub(crate) struct BlockIndex<P: 'static = FixedBufferPool> {
    root: BlockIndexRoot,
    row: RowPageIndex<P>,
}

impl<P: BufferPool> BlockIndex<P> {
    /// Creates a block-index facade for one table.
    ///
    /// `pivot_row_id` and `column_root_block_id` define the boundary and root of
    /// persisted columnar data at startup.
    #[inline]
    pub(crate) async fn new(
        pool: QuiescentGuard<P>,
        meta_pool_guard: &PoolGuard,
        pivot_row_id: RowID,
        column_root_block_id: BlockID,
    ) -> Result<Self> {
        let row = RowPageIndex::new(pool, meta_pool_guard, pivot_row_id).await?;
        let root = BlockIndexRoot::new(pivot_row_id, column_root_block_id);
        Ok(BlockIndex { root, row })
    }

    /// Creates block index for catalog-table runtime without table-file backing.
    #[inline]
    pub(crate) async fn new_catalog(
        pool: QuiescentGuard<P>,
        meta_pool_guard: &PoolGuard,
    ) -> Result<Self> {
        Self::new(pool, meta_pool_guard, RowID::new(0), SUPER_BLOCK_ID).await
    }

    /// Returns the in-memory row index height.
    #[inline]
    #[cfg_attr(not(test), expect(dead_code, reason = "reserved index height"))]
    pub(crate) fn height(&self) -> usize {
        self.row.height()
    }

    /// Atomically updates the persisted column index boundary and root block.
    ///
    /// Called after checkpoint/persist updates the column block index.
    #[inline]
    pub(crate) async fn update_column_root(
        &self,
        pivot_row_id: RowID,
        column_root_block_id: BlockID,
    ) {
        self.root
            .update_column_root(pivot_row_id, column_root_block_id)
            .await;
    }

    /// Returns current pivot row id.
    #[inline]
    pub(crate) fn pivot_row_id(&self) -> RowID {
        self.root.pivot_row_id()
    }

    /// Returns the route-publication notification epoch.
    #[inline]
    pub(crate) fn route_epoch(&self) -> u64 {
        self.root.route_epoch()
    }

    /// Wait asynchronously until checkpoint publishes route progress.
    #[inline]
    pub(crate) async fn wait_route_since(&self, observed_epoch: u64) {
        self.root.wait_route_since(observed_epoch).await;
    }

    /// Destroy the in-memory row-page index owned by this facade.
    #[inline]
    pub(crate) async fn destroy<B: BufferPool>(
        self,
        meta_pool_guard: &PoolGuard,
        mem_pool: &B,
        mem_pool_guard: &PoolGuard,
    ) -> Result<()> {
        self.row
            .destroy(meta_pool_guard, mem_pool, mem_pool_guard)
            .await
    }

    /// Returns a shared row page suitable for appending rows, recording redo when requested.
    #[inline]
    pub(crate) async fn try_get_insert_page_with_redo<B: BufferPool>(
        &self,
        meta_pool_guard: &PoolGuard,
        mem_pool: &B,
        mem_pool_guard: &PoolGuard,
        col_layout: &Arc<TableColumnLayout>,
        count: usize,
        redo_ctx: Option<RowPageCreateRedoCtx<'_>>,
    ) -> Result<PageSharedGuard<RowPage>> {
        self.row
            .get_insert_page(
                meta_pool_guard,
                mem_pool,
                mem_pool_guard,
                col_layout,
                count,
                redo_ctx,
            )
            .await
    }

    /// Returns an exclusive row page suitable for appending rows, recording redo when requested.
    #[inline]
    pub(crate) async fn get_insert_page_exclusive_with_redo<B: BufferPool>(
        &self,
        meta_pool_guard: &PoolGuard,
        mem_pool: &B,
        mem_pool_guard: &PoolGuard,
        col_layout: &Arc<TableColumnLayout>,
        count: usize,
        redo_ctx: Option<RowPageCreateRedoCtx<'_>>,
    ) -> Result<PageExclusiveGuard<RowPage>> {
        self.row
            .get_insert_page_exclusive(
                meta_pool_guard,
                mem_pool,
                mem_pool_guard,
                col_layout,
                count,
                redo_ctx,
            )
            .await
    }

    /// Allocates a row page at a specific page id.
    ///
    /// This is primarily used by recovery replay.
    #[inline]
    pub(crate) async fn allocate_row_page_at<B: BufferPool>(
        &self,
        meta_pool_guard: &PoolGuard,
        mem_pool: &B,
        mem_pool_guard: &PoolGuard,
        col_layout: &Arc<TableColumnLayout>,
        count: usize,
        page_id: PageID,
    ) -> Result<PageExclusiveGuard<RowPage>> {
        self.row
            .allocate_row_page_at(
                meta_pool_guard,
                mem_pool,
                mem_pool_guard,
                col_layout,
                count,
                page_id,
            )
            .await
    }

    /// Returns an exclusive insert page back to the in-memory free list cache.
    #[inline]
    pub(crate) fn cache_exclusive_insert_page(&self, guard: PageExclusiveGuard<RowPage>) {
        self.row.cache_exclusive_insert_page(guard)
    }

    /// Creates a cursor to scan in-memory row-page-index leaves.
    #[inline]
    pub(crate) fn mem_cursor<'a>(
        &'a self,
        meta_pool_guard: &'a PoolGuard,
    ) -> RowPageIndexMemCursor<'a, P> {
        self.row.mem_cursor(meta_pool_guard)
    }

    /// Finds one in-memory row-page location without consulting column storage.
    #[inline]
    pub(crate) async fn find_mem_row(
        &self,
        meta_pool_guard: &PoolGuard,
        row_id: RowID,
    ) -> Result<RowLocation> {
        debug_assert!(!row_id.is_deleted());
        self.row.find_row(meta_pool_guard, row_id).await
    }

    /// Finds the physical location of one row id with persisted column-path errors surfaced.
    #[inline]
    pub(crate) async fn find_row(
        &self,
        meta_pool_guard: &PoolGuard,
        disk_pool_guard: Option<&PoolGuard>,
        row_id: RowID,
        storage: Option<&ColumnStorage>,
    ) -> Result<RowLocation> {
        debug_assert!(!row_id.is_deleted());
        match self.root.guide(row_id) {
            BlockIndexRoute::Column {
                pivot_row_id,
                root_block_id,
            } => {
                self.find_row_in_column(
                    storage,
                    disk_pool_guard,
                    row_id,
                    pivot_row_id,
                    root_block_id,
                )
                .await
            }
            BlockIndexRoute::Row => {
                let found = self.row.find_row(meta_pool_guard, row_id).await?;
                if !matches!(found, RowLocation::NotFound) {
                    return Ok(found);
                }
                match self.root.try_column(row_id) {
                    Some((pivot_row_id, root_block_id)) => {
                        self.find_row_in_column(
                            storage,
                            disk_pool_guard,
                            row_id,
                            pivot_row_id,
                            root_block_id,
                        )
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
        disk_pool_guard: Option<&PoolGuard>,
        row_id: RowID,
        pivot_row_id: RowID,
        root_block_id: BlockID,
    ) -> Result<RowLocation> {
        let Some(storage) = storage else {
            return Err(Error::column_storage_missing());
        };
        let Some(disk_pool_guard) = disk_pool_guard else {
            return Err(Report::new(InternalError::DiskPoolGuardMissing)
                .attach(format!(
                    "row_id={row_id}, pivot_row_id={pivot_row_id}, root_block_id={root_block_id}"
                ))
                .into());
        };
        let index = ColumnBlockIndex::new(
            root_block_id,
            pivot_row_id,
            storage.file().file_kind(),
            storage.file().sparse_file(),
            storage.disk_pool(),
            disk_pool_guard,
        );
        match index.locate_and_resolve_row(row_id).await {
            Ok(Some(resolved)) => Ok(RowLocation::LwcBlock {
                block_id: resolved.block_id(),
                row_idx: resolved.row_idx(),
                row_shape_fingerprint: resolved.row_shape_fingerprint(),
            }),
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
    use crate::buffer::{BufferPool, FixedBufferPool, PoolGuard, PoolRole};
    use crate::catalog::{
        ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexSpec, TableMetadata,
    };
    use crate::error::{IoError, Validation};
    use crate::file::test_block_id;
    use crate::latch::LatchFallbackMode;
    use crate::quiescent::{QuiescentBox, QuiescentGuard};
    use crate::value::ValKind;
    use semistr::SemiStr;
    use std::future::Future;
    use std::io::Error as StdIoError;
    use std::sync::Arc;
    use std::sync::Barrier;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::thread::scope;

    // Test helper that lets us pause one row-index page fetch at a precise point.
    // The fallback test uses this to hold `find_row()` after the initial route
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
                stall_page_id: AtomicU64::new(u64::from(INVALID_PAGE_ID)),
                entered,
                release,
            }
        }

        #[inline]
        fn set_stall_page_id(&self, page_id: PageID) {
            self.stall_page_id
                .store(u64::from(page_id), Ordering::Release);
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
        ) -> impl Future<Output = Result<PageExclusiveGuard<T>>> + Send {
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

    struct FailingInsertPagePool {
        inner: QuiescentGuard<FixedBufferPool>,
        fail_page_id: AtomicU64,
    }

    impl FailingInsertPagePool {
        #[inline]
        fn new(inner: QuiescentGuard<FixedBufferPool>, fail_page_id: PageID) -> Self {
            Self {
                inner,
                fail_page_id: AtomicU64::new(u64::from(fail_page_id)),
            }
        }
    }

    impl BufferPool for FailingInsertPagePool {
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
        ) -> impl Future<Output = Result<PageExclusiveGuard<T>>> + Send {
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
            if page_id == self.fail_page_id.load(Ordering::Acquire) {
                return Err(StdIoError::from_raw_os_error(libc::EIO).into());
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

    fn owned_index_pool(pool_size: usize) -> QuiescentBox<FixedBufferPool> {
        QuiescentBox::new(FixedBufferPool::with_capacity(PoolRole::Index, pool_size).unwrap())
    }

    fn owned_mem_pool(pool_size: usize) -> QuiescentBox<FixedBufferPool> {
        QuiescentBox::new(FixedBufferPool::with_capacity(PoolRole::Mem, pool_size).unwrap())
    }

    #[test]
    fn test_block_index_root_accessors_and_update() {
        smol::block_on(async {
            let meta_pool = owned_index_pool(64 * 1024 * 1024);
            let meta_guard = (*meta_pool).pool_guard();
            let blk_idx = BlockIndex::new(
                meta_pool.guard(),
                &meta_guard,
                RowID::new(7),
                test_block_id(11),
            )
            .await
            .expect("test block-index construction should succeed");

            assert_eq!(blk_idx.height(), 0);
            assert_eq!(blk_idx.pivot_row_id(), RowID::new(7));

            blk_idx
                .update_column_root(RowID::new(9), test_block_id(12))
                .await;
            assert_eq!(blk_idx.pivot_row_id(), RowID::new(9));

            let catalog_idx = BlockIndex::new_catalog(meta_pool.guard(), &meta_guard)
                .await
                .expect("test catalog block-index construction should succeed");
            assert_eq!(catalog_idx.pivot_row_id(), RowID::new(0));
        });
    }

    fn make_test_metadata() -> Arc<TableMetadata> {
        Arc::new(
            TableMetadata::try_new(
                vec![ColumnSpec {
                    column_name: SemiStr::new("id"),
                    column_type: ValKind::I32,
                    column_attributes: ColumnAttributes::empty(),
                }],
                vec![IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::UK)],
            )
            .expect("valid table metadata"),
        )
    }

    #[test]
    fn test_find_row_returns_error_when_column_route_has_no_storage() {
        let pool = QuiescentBox::new(
            FixedBufferPool::with_capacity(PoolRole::Index, 64 * 1024 * 1024).unwrap(),
        );
        let meta_guard = (*pool).pool_guard();
        let blk_idx = smol::block_on(BlockIndex::new(
            pool.guard(),
            &meta_guard,
            RowID::new(10),
            test_block_id(77),
        ))
        .expect("test block-index construction should succeed");

        // Row id 9 is below the pivot, so lookup goes straight to the column path.
        // Without column storage this must surface as an error, not as "not found".
        let err = match smol::block_on(blk_idx.find_row(&meta_guard, None, RowID::new(9), None)) {
            Ok(_location) => panic!("expected missing-column-storage error, got row location"),
            Err(err) => err,
        };
        assert_eq!(
            err.report().downcast_ref::<InternalError>().copied(),
            Some(InternalError::ColumnStorageMissing)
        );
    }

    #[test]
    fn test_find_row_returns_error_when_column_fallback_has_no_storage() {
        let inner = QuiescentBox::new(
            FixedBufferPool::with_capacity(PoolRole::Index, 64 * 1024 * 1024).unwrap(),
        );
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let pool = QuiescentBox::new(StallingBufferPool::new(
            inner.guard(),
            Arc::clone(&entered),
            Arc::clone(&release),
        ));
        let meta_guard = (*pool).pool_guard();
        let blk_idx = smol::block_on(BlockIndex::new(
            pool.guard(),
            &meta_guard,
            RowID::new(10),
            test_block_id(77),
        ))
        .expect("test block-index construction should succeed");
        pool.set_stall_page_id(blk_idx.row.root_page_id());

        scope(|s| {
            // Start with row_id == pivot so the first route decision picks the row path.
            let blk_idx = &blk_idx;
            let meta_guard = meta_guard.clone();
            let handle = s.spawn(move || {
                smol::block_on(async {
                    blk_idx
                        .find_row(&meta_guard, None, RowID::new(10), None)
                        .await
                })
            });

            // Wait until the row-store root fetch is paused, then move the pivot past
            // row_id so the subsequent `try_column()` fallback becomes eligible.
            // This avoids relying on timing-sensitive races to exercise the fallback.
            entered.wait();
            smol::block_on(blk_idx.update_column_root(RowID::new(11), test_block_id(88)));
            release.wait();

            let res = handle.join().unwrap();
            assert!(
                res.as_ref().is_err_and(|err| err
                    .report()
                    .downcast_ref::<InternalError>()
                    .copied()
                    == Some(InternalError::ColumnStorageMissing))
            );
        });
    }

    #[test]
    fn test_get_insert_page_returns_error_on_free_list_io_failure() {
        smol::block_on(async {
            let meta_pool = owned_index_pool(64 * 1024 * 1024);
            let mem_pool = owned_mem_pool(64 * 1024 * 1024);
            let meta_guard = (*meta_pool).pool_guard();
            let mem_guard = (*mem_pool).pool_guard();
            let metadata = make_test_metadata();
            let blk_idx = BlockIndex::new(
                meta_pool.guard(),
                &meta_guard,
                RowID::new(0),
                test_block_id(77),
            )
            .await
            .expect("test block-index construction should succeed");

            let page_guard = blk_idx
                .get_insert_page_exclusive_with_redo(
                    &meta_guard,
                    &*mem_pool,
                    &mem_guard,
                    &metadata.col,
                    100,
                    None,
                )
                .await
                .expect("test insert-page allocation should succeed");
            let page_id = page_guard.page_id();
            blk_idx.cache_exclusive_insert_page(page_guard);

            let failing_pool = FailingInsertPagePool::new(mem_pool.guard(), page_id);
            let res = blk_idx
                .try_get_insert_page_with_redo(
                    &meta_guard,
                    &failing_pool,
                    &mem_guard,
                    &metadata.col,
                    100,
                    None,
                )
                .await;
            let err = match res {
                Ok(_) => panic!("expected cached insert-page reload failure"),
                Err(err) => err,
            };
            assert!(err.report().downcast_ref::<IoError>().is_some());
        });
    }
}
