use crate::buffer::guard::{PageExclusiveGuard, PageSharedGuard};
use crate::buffer::page::PageID;
use crate::buffer::{BufferPool, FixedBufferPool, GlobalReadonlyBufferPool, ReadonlyBufferPool};
use crate::catalog::{TableID, TableMetadata};
use crate::file::table_file::TableFile;
use crate::index::block_index_root::{BlockIndexRoot, BlockIndexRoute};
use crate::index::column_block_index::ColumnBlockIndex;
use crate::index::row_block_index::{
    GenericRowBlockIndex, GenericRowBlockIndexMemCursor, RowLocation,
};
use crate::index::util::Maskable;
use crate::row::{RowID, RowPage};
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
    /// Owning table id.
    pub table_id: TableID,
    root: BlockIndexRoot,
    row: GenericRowBlockIndex<P>,
    disk_pool: Option<ReadonlyBufferPool>,
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
        pool: &'static P,
        table_id: TableID,
        pivot_row_id: RowID,
        column_root_page_id: PageID,
        table_file: Arc<TableFile>,
        global_disk_pool: &'static GlobalReadonlyBufferPool,
    ) -> Self {
        let metadata = Arc::clone(&table_file.active_root().metadata);
        let row = GenericRowBlockIndex::new(pool, table_id, metadata).await;
        let root = BlockIndexRoot::new(pivot_row_id, column_root_page_id);
        let disk_pool =
            ReadonlyBufferPool::new(table_id, Arc::clone(&table_file), global_disk_pool);
        GenericBlockIndex {
            table_id,
            root,
            row,
            disk_pool: Some(disk_pool),
        }
    }

    /// Creates block index for catalog-table runtime without table-file backing.
    #[inline]
    pub async fn new_catalog(
        pool: &'static P,
        table_id: TableID,
        metadata: Arc<TableMetadata>,
    ) -> Self {
        let row = GenericRowBlockIndex::new(pool, table_id, metadata).await;
        let root = BlockIndexRoot::new(0, 0);
        GenericBlockIndex {
            table_id,
            root,
            row,
            disk_pool: None,
        }
    }

    /// Returns the in-memory row index height.
    #[inline]
    pub fn height(&self) -> usize {
        self.row.height()
    }

    /// Enables redo logging for newly allocated row pages.
    #[inline]
    pub fn enable_page_committer(&self, trx_sys: &'static TransactionSystem) {
        self.row.enable_page_committer(trx_sys)
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
        count: usize,
    ) -> PageSharedGuard<RowPage> {
        self.row.get_insert_page(mem_pool, count).await
    }

    /// Returns an exclusive row page suitable for insert operations.
    #[inline]
    pub async fn get_insert_page_exclusive<B: BufferPool>(
        &self,
        mem_pool: &'static B,
        count: usize,
    ) -> PageExclusiveGuard<RowPage> {
        self.row.get_insert_page_exclusive(mem_pool, count).await
    }

    /// Allocates a row page at a specific page id.
    ///
    /// This is primarily used by recovery replay.
    #[inline]
    pub async fn allocate_row_page_at<B: BufferPool>(
        &self,
        mem_pool: &'static B,
        count: usize,
        page_id: PageID,
    ) -> PageExclusiveGuard<RowPage> {
        self.row
            .allocate_row_page_at(mem_pool, count, page_id)
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
    pub async fn find_row(&self, row_id: RowID) -> RowLocation {
        debug_assert!(!row_id.is_deleted());
        match self.root.guide(row_id) {
            BlockIndexRoute::Column {
                pivot_row_id,
                root_page_id,
            } => {
                self.find_row_in_column(row_id, pivot_row_id, root_page_id)
                    .await
            }
            BlockIndexRoute::Row => {
                let found = self.row.find_row(row_id).await;
                if !matches!(found, RowLocation::NotFound) {
                    return found;
                }
                match self.root.try_column(row_id) {
                    Some((pivot_row_id, root_page_id)) => {
                        self.find_row_in_column(row_id, pivot_row_id, root_page_id)
                            .await
                    }
                    None => RowLocation::NotFound,
                }
            }
        }
    }

    #[inline]
    async fn find_row_in_column(
        &self,
        row_id: RowID,
        pivot_row_id: RowID,
        root_page_id: PageID,
    ) -> RowLocation {
        let Some(disk_pool) = &self.disk_pool else {
            return RowLocation::NotFound;
        };
        let index = ColumnBlockIndex::new(root_page_id, pivot_row_id, disk_pool);
        match index.find(row_id).await {
            Ok(Some(payload)) => RowLocation::LwcPage(payload.block_id as PageID),
            Ok(None) => RowLocation::NotFound,
            Err(err) => todo!(
                "block-index column-path error policy is deferred (row_id={}, err={})",
                row_id,
                err
            ),
        }
    }
}

unsafe impl<P: BufferPool> Send for GenericBlockIndex<P> {}
unsafe impl<P: BufferPool> Sync for GenericBlockIndex<P> {}
