use crate::buffer::guard::{PageExclusiveGuard, PageSharedGuard};
use crate::buffer::page::PageID;
use crate::buffer::{BufferPool, FixedBufferPool};
use crate::catalog::TableMetadata;
use crate::error::Result;
use crate::index::block_index_root::{BlockIndexRoot, BlockIndexRoute};
use crate::index::column_block_index::ColumnBlockIndex;
use crate::index::row_block_index::{
    GenericRowBlockIndex, GenericRowBlockIndexMemCursor, RowLocation,
};
use crate::index::util::Maskable;
use crate::row::{RowID, RowPage};
use crate::table::ColumnStorage;
use crate::trx::sys::TransactionSystem;
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
    pub async fn new(pool: &'static P, pivot_row_id: RowID, column_root_page_id: PageID) -> Self {
        let row = GenericRowBlockIndex::new(pool, pivot_row_id).await;
        let root = BlockIndexRoot::new(pivot_row_id, column_root_page_id);
        GenericBlockIndex { root, row }
    }

    /// Creates block index for catalog-table runtime without table-file backing.
    #[inline]
    pub async fn new_catalog(pool: &'static P) -> Self {
        Self::new(pool, 0, 0).await
    }

    /// Returns the in-memory row index height.
    #[inline]
    pub fn height(&self) -> usize {
        self.row.height()
    }

    /// Enables redo logging for newly allocated row pages.
    #[inline]
    pub(crate) fn enable_page_committer(
        &self,
        table_id: crate::catalog::TableID,
        trx_sys: &'static TransactionSystem,
    ) {
        self.row.enable_page_committer(table_id, trx_sys)
    }

    /// Returns whether row-page redo logging is enabled.
    #[inline]
    pub fn is_page_committer_enabled(&self) -> bool {
        self.row.is_page_committer_enabled()
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
        mem_pool: &'static B,
        metadata: &Arc<TableMetadata>,
        count: usize,
    ) -> PageSharedGuard<RowPage> {
        self.row.get_insert_page(mem_pool, metadata, count).await
    }

    /// Returns an exclusive row page suitable for insert operations.
    #[inline]
    pub async fn get_insert_page_exclusive<B: BufferPool>(
        &self,
        mem_pool: &'static B,
        metadata: &Arc<TableMetadata>,
        count: usize,
    ) -> PageExclusiveGuard<RowPage> {
        self.row
            .get_insert_page_exclusive(mem_pool, metadata, count)
            .await
    }

    /// Allocates a row page at a specific page id.
    ///
    /// This is primarily used by recovery replay.
    #[inline]
    pub async fn allocate_row_page_at<B: BufferPool>(
        &self,
        mem_pool: &'static B,
        metadata: &Arc<TableMetadata>,
        count: usize,
        page_id: PageID,
    ) -> PageExclusiveGuard<RowPage> {
        self.row
            .allocate_row_page_at(mem_pool, metadata, count, page_id)
            .await
    }

    /// Returns an exclusive insert page back to the in-memory free list cache.
    #[inline]
    pub fn cache_exclusive_insert_page(&self, guard: PageExclusiveGuard<RowPage>) {
        self.row.cache_exclusive_insert_page(guard)
    }

    /// Creates a cursor to scan in-memory block-index leaves.
    #[inline]
    pub fn mem_cursor(&self) -> GenericRowBlockIndexMemCursor<'_, P> {
        self.row.mem_cursor()
    }

    /// Finds the physical location of one row id.
    ///
    /// It first follows the root route decision and may fallback to the column
    /// path when the row lookup misses due to concurrent boundary movement.
    #[inline]
    pub(crate) async fn find_row(
        &self,
        row_id: RowID,
        storage: Option<&ColumnStorage>,
    ) -> RowLocation {
        match self.try_find_row(row_id, storage).await {
            Ok(location) => location,
            Err(err) => todo!(
                "block-index column-path error policy is deferred (row_id={}, err={})",
                row_id,
                err
            ),
        }
    }

    /// Finds the physical location of one row id with persisted column-path errors surfaced.
    #[inline]
    pub(crate) async fn try_find_row(
        &self,
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
                let found = self.row.find_row(row_id).await;
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
            return Ok(RowLocation::NotFound);
        };
        let index = ColumnBlockIndex::new(root_page_id, pivot_row_id, storage.disk_pool());
        match index.find(row_id).await {
            Ok(Some(payload)) => Ok(RowLocation::LwcPage(payload.block_id as PageID)),
            Ok(None) => Ok(RowLocation::NotFound),
            Err(err) => Err(err),
        }
    }
}

unsafe impl<P: BufferPool> Send for GenericBlockIndex<P> {}
unsafe impl<P: BufferPool> Sync for GenericBlockIndex<P> {}
