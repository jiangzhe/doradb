mod access;
mod deletion_buffer;
mod gc;
mod persistence;
mod recover;
mod rollback;
#[cfg(test)]
mod tests;

pub use access::*;
pub use deletion_buffer::*;
pub use gc::{SecondaryMemIndexCleanupIndexStats, SecondaryMemIndexCleanupStats};
pub use persistence::*;
pub use recover::*;
pub(crate) use rollback::IndexRollback;

use crate::buffer::guard::{PageExclusiveGuard, PageGuard, PageSharedGuard};
use crate::buffer::page::{PageID, VersionedPageID};
use crate::buffer::{
    BufferPool, EvictableBufferPool, PoolGuard, PoolGuards, PoolRole, ReadonlyBufferPool,
    RowPoolRole,
};
use crate::catalog::{IndexSpec, TableID, TableMetadata};
use crate::error::{DataIntegrityError, Error, InternalError, Result};
use crate::file::BlockID;
use crate::file::table_file::{ActiveRoot, LwcBlockPersist, TableFile};
use crate::index::util::{Maskable, RowPageCreateRedoCtx};
use crate::index::{
    BlockIndex, ColumnBlockEntryShape, InMemorySecondaryIndex, IndexCompareExchange, IndexInsert,
    NonUniqueIndex, NonUniqueMemIndex, RowLocation, SecondaryDiskTreeRuntime, SecondaryIndex,
    UniqueIndex, UniqueMemIndex,
};
use crate::latch::LatchFallbackMode;
use crate::lwc::LwcBuilder;
use crate::quiescent::QuiescentGuard;
use crate::row::ops::{Recover, RecoverIndex, SelectKey, UpdateCol};
use crate::row::{RowID, RowPage, RowRead, var_len_for_insert};
use crate::trx::row::RowReadAccess;
use crate::trx::sys::TransactionSystem;
use crate::trx::undo::{IndexBranch, RowUndoKind, UndoStatus};
use crate::trx::ver_map::RowPageState;
use crate::trx::{
    MAX_SNAPSHOT_TS, MIN_SNAPSHOT_TS, TrxContext, TrxID, TrxReadProof, trx_is_committed,
};
use crate::value::{PAGE_VAR_LEN_INLINE, Val};
use error_stack::Report;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

#[inline]
fn missing_secondary_index(index_no: usize, index_count: usize) -> Error {
    Report::new(InternalError::SecondaryIndexOutOfBounds)
        .attach(format!("index_no={index_no}, index_count={index_count}"))
        .into()
}

/// Table is a logical data set of rows.
/// It combines components such as row page, undo map, block index, secondary
/// index to provide full-featured CRUD and MVCC functionalities.
///
/// The basic flow is:
///
/// secondary index -> block index -> buffer pool -> row page -> undo map.
///
/// 1. secondary index stores mapping from key to row id.
///
/// 2. block index stores mapping from row id to page.
///
/// 3. Buffer pool takes care of creating and fetching pages.
///
/// 4. Row page stores latest version fo row data.
///
/// 5. Undo map stores old versions of row data.
///
/// We have a separate undo array associated to each row in row page.
///
/// The undo head also acts as *physical* row lock, so that threads need to
/// synchronize the row access.
///
/// Undo entry with uncommitted timestamp represents *logical* row lock and
/// only released once transaction commits.
///
/// Insert/update/delete operation will add one or more undo entry to the
/// chain linked to undo head.
///
/// Select operation will traverse undo chain to find visible version.
///
/// Additional key validation is performed if index lookup is used, because
/// index does not contain version information, and out-of-date index entry
/// should ignored if visible data version does not match index key.
pub struct GenericMemTable<D: 'static, I: 'static> {
    pub(crate) table_id: TableID,
    pub(crate) metadata: Arc<TableMetadata>,
    pub(crate) mem_pool: QuiescentGuard<D>,
    pub(crate) row_pool_role: RowPoolRole,
    pub(crate) index_pool_role: PoolRole,
    pub(crate) blk_idx: BlockIndex,
    pub(crate) sec_idx: Box<[InMemorySecondaryIndex<I>]>,
}

/// Persisted column-store attachments associated with a user table runtime.
pub struct ColumnStorage {
    pub(crate) file: Arc<TableFile>,
    pub(crate) disk_pool: QuiescentGuard<ReadonlyBufferPool>,
    pub(crate) deletion_buffer: ColumnDeletionBuffer,
    secondary_indexes: Box<[SecondaryDiskTreeRuntime]>,
}

/// Runtime handle for a user table, combining in-memory and persisted storage.
pub struct Table {
    pub(crate) mem: GenericMemTable<EvictableBufferPool, EvictableBufferPool>,
    pub(crate) storage: ColumnStorage,
    pub(crate) sec_idx: Box<[SecondaryIndex<EvictableBufferPool>]>,
}

/// Owned projection of one proof-gated user-table active root.
///
/// The snapshot contains only runtime read contract fields copied from a
/// single active-root observation. Publication and allocation internals remain
/// behind the table-file boundary.
pub(crate) struct TableRootSnapshot<'ctx> {
    root_trx_id: TrxID,
    pivot_row_id: RowID,
    column_block_index_root: BlockID,
    secondary_index_roots: Vec<BlockID>,
    deletion_cutoff_ts: TrxID,
    _proof: PhantomData<&'ctx TrxContext>,
}

impl<'ctx> TableRootSnapshot<'ctx> {
    #[inline]
    fn from_active_root(root: &ActiveRoot, _proof: &TrxReadProof<'ctx>) -> Self {
        Self {
            root_trx_id: root.trx_id,
            pivot_row_id: root.pivot_row_id,
            column_block_index_root: root.column_block_index_root,
            secondary_index_roots: root.secondary_index_roots.clone(),
            deletion_cutoff_ts: root.deletion_cutoff_ts,
            _proof: PhantomData,
        }
    }

    /// Returns the checkpoint timestamp carried by the captured root.
    #[inline]
    pub(crate) fn root_trx_id(&self) -> TrxID {
        self.root_trx_id
    }

    /// Returns the row-id boundary between persisted and in-memory rows.
    #[inline]
    pub(crate) fn pivot_row_id(&self) -> RowID {
        self.pivot_row_id
    }

    /// Returns the persisted column-block-index root from the captured root.
    #[inline]
    pub(crate) fn column_block_index_root(&self) -> BlockID {
        self.column_block_index_root
    }

    /// Returns the cold-row deletion replay cutoff from the captured root.
    #[inline]
    pub(crate) fn deletion_cutoff_ts(&self) -> TrxID {
        self.deletion_cutoff_ts
    }

    /// Returns the captured DiskTree root for one secondary index.
    #[inline]
    pub(crate) fn secondary_index_root(&self, index_no: usize) -> Result<BlockID> {
        self.secondary_index_roots
            .get(index_no)
            .copied()
            .ok_or_else(|| missing_secondary_index(index_no, self.secondary_index_roots.len()))
    }

    /// Returns whether the captured root predates the supplied snapshot time.
    #[inline]
    pub(crate) fn root_is_visible_to(&self, sts: TrxID) -> bool {
        self.root_trx_id < sts
    }
}

struct FrozenPage {
    page_id: PageID,
    start_row_id: RowID,
    end_row_id: RowID,
}

type VisibleRowCollector<'a> = &'a mut dyn FnMut(&RowPage, usize, RowID) -> Result<()>;

/// Stages newly built secondary indexes until the caller publishes them.
///
/// This keeps the build flow linear: construct each index, then either publish
/// the whole batch on success or explicitly destroy already-built trees before
/// returning the original build error.
struct InMemorySecondaryIndexScopedBuilder<P: 'static> {
    staged: Vec<InMemorySecondaryIndex<P>>,
}

/// Stages newly built dual-tree secondary indexes until the caller publishes them.
struct SecondaryIndexScopedBuilder {
    staged: Vec<SecondaryIndex<EvictableBufferPool>>,
}

impl<P: BufferPool> InMemorySecondaryIndexScopedBuilder<P> {
    #[inline]
    fn new(capacity: usize) -> Self {
        Self {
            staged: Vec::with_capacity(capacity),
        }
    }

    #[inline]
    async fn push_or_rollback(
        &mut self,
        built: Result<InMemorySecondaryIndex<P>>,
        pool_guard: &PoolGuard,
    ) -> Result<()> {
        match built {
            Ok(index) => {
                self.staged.push(index);
                Ok(())
            }
            Err(err) => {
                self.rollback(pool_guard).await;
                Err(err)
            }
        }
    }

    #[inline]
    async fn rollback(&mut self, pool_guard: &PoolGuard) {
        for index in std::mem::take(&mut self.staged).into_iter().rev() {
            // Keep the original construction error as the function result.
            let _ = index.destroy(pool_guard).await;
        }
    }

    #[inline]
    fn publish(self) -> Box<[InMemorySecondaryIndex<P>]> {
        self.staged.into_boxed_slice()
    }
}

impl SecondaryIndexScopedBuilder {
    #[inline]
    fn new(capacity: usize) -> Self {
        Self {
            staged: Vec::with_capacity(capacity),
        }
    }

    #[inline]
    fn push(&mut self, index: SecondaryIndex<EvictableBufferPool>) {
        self.staged.push(index);
    }

    #[inline]
    async fn rollback(&mut self, pool_guard: &PoolGuard) {
        for index in std::mem::take(&mut self.staged).into_iter().rev() {
            // Keep the original construction error as the function result.
            let _ = index.destroy(pool_guard).await;
        }
    }

    #[inline]
    fn publish(self) -> Box<[SecondaryIndex<EvictableBufferPool>]> {
        self.staged.into_boxed_slice()
    }
}

#[inline]
pub(crate) async fn build_in_memory_secondary_indexes<I: BufferPool + 'static>(
    index_pool: QuiescentGuard<I>,
    index_pool_guard: &PoolGuard,
    metadata: &TableMetadata,
    index_ts: TrxID,
) -> Result<Box<[InMemorySecondaryIndex<I>]>> {
    let mut builder = InMemorySecondaryIndexScopedBuilder::new(metadata.index_specs.len());
    for index_spec in &metadata.index_specs {
        let ty_infer = |col_no: usize| metadata.col_type(col_no);
        builder
            .push_or_rollback(
                InMemorySecondaryIndex::new(
                    index_pool.clone(),
                    index_pool_guard,
                    index_spec,
                    ty_infer,
                    index_ts,
                )
                .await,
                index_pool_guard,
            )
            .await?;
    }
    Ok(builder.publish())
}

/// Build user-table dual-tree secondary indexes from fresh MemIndex backends
/// paired with the table file's checkpointed DiskTree runtimes.
#[inline]
pub(crate) async fn build_dual_tree_secondary_indexes(
    index_pool: QuiescentGuard<EvictableBufferPool>,
    index_pool_guard: &PoolGuard,
    metadata: Arc<TableMetadata>,
    file: Arc<TableFile>,
    disk_pool: QuiescentGuard<ReadonlyBufferPool>,
    index_ts: TrxID,
) -> Result<Box<[SecondaryIndex<EvictableBufferPool>]>> {
    let mut builder = SecondaryIndexScopedBuilder::new(metadata.index_specs.len());
    for (index_no, index_spec) in metadata.index_specs.iter().enumerate() {
        let runtime = match SecondaryDiskTreeRuntime::new(
            index_no,
            Arc::clone(&metadata),
            Arc::clone(&file),
            disk_pool.clone(),
        ) {
            Ok(runtime) => runtime,
            Err(err) => {
                builder.rollback(index_pool_guard).await;
                return Err(err);
            }
        };
        let ty_infer = |col_no: usize| metadata.col_type(col_no);
        let index = if index_spec.unique() {
            let mem = match UniqueMemIndex::new(
                index_pool.clone(),
                index_pool_guard,
                index_spec,
                ty_infer,
                index_ts,
            )
            .await
            {
                Ok(mem) => mem,
                Err(err) => {
                    builder.rollback(index_pool_guard).await;
                    return Err(err);
                }
            };
            SecondaryIndex::Unique { mem, disk: runtime }
        } else {
            let mem = match NonUniqueMemIndex::new(
                index_pool.clone(),
                index_pool_guard,
                index_spec,
                ty_infer,
                index_ts,
            )
            .await
            {
                Ok(mem) => mem,
                Err(err) => {
                    builder.rollback(index_pool_guard).await;
                    return Err(err);
                }
            };
            SecondaryIndex::NonUnique { mem, disk: runtime }
        };
        builder.push(index);
    }
    Ok(builder.publish())
}

impl<D: BufferPool, I: BufferPool> GenericMemTable<D, I> {
    #[inline]
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn new(
        mem_pool: QuiescentGuard<D>,
        row_pool_role: RowPoolRole,
        index_pool: QuiescentGuard<I>,
        index_pool_role: PoolRole,
        index_pool_guard: &crate::buffer::PoolGuard,
        table_id: TableID,
        metadata: Arc<TableMetadata>,
        blk_idx: BlockIndex,
        index_ts: TrxID,
    ) -> Result<Self> {
        let sec_idx =
            build_in_memory_secondary_indexes(index_pool, index_pool_guard, &metadata, index_ts)
                .await?;
        Ok(GenericMemTable {
            table_id,
            metadata,
            mem_pool,
            row_pool_role,
            index_pool_role,
            blk_idx,
            sec_idx,
        })
    }

    /// Returns the logical table id of this runtime.
    #[inline]
    pub fn table_id(&self) -> TableID {
        self.table_id
    }

    /// Returns the immutable metadata for this table.
    #[inline]
    pub fn metadata(&self) -> &TableMetadata {
        &self.metadata
    }

    /// Returns the buffer pool used for in-memory row pages.
    #[inline]
    pub fn mem_pool(&self) -> &D {
        &self.mem_pool
    }

    /// Returns the row page index used by this table.
    #[inline]
    pub fn blk_idx(&self) -> &BlockIndex {
        &self.blk_idx
    }

    /// Returns the secondary-index array owned by this table.
    #[inline]
    pub(crate) fn sec_idx(&self) -> &[InMemorySecondaryIndex<I>] {
        &self.sec_idx
    }

    /// Returns the row-id boundary between persisted and in-memory rows.
    #[inline]
    pub fn pivot_row_id(&self) -> RowID {
        self.blk_idx.pivot_row_id()
    }

    #[inline]
    fn row_pool_guard<'a>(&self, guards: &'a PoolGuards) -> &'a PoolGuard {
        guards
            .try_row_guard(self.row_pool_role)
            .expect("missing row-page pool guard")
    }

    #[inline]
    pub(crate) fn index_pool_guard<'a>(&self, guards: &'a PoolGuards) -> &'a PoolGuard {
        guards
            .try_guard(self.index_pool_role)
            .expect("missing secondary-index pool guard")
    }

    #[inline]
    pub(crate) async fn get_row_page_shared(
        &self,
        guards: &PoolGuards,
        page_id: PageID,
    ) -> Result<Option<PageSharedGuard<RowPage>>> {
        Ok(self
            .mem_pool()
            .get_page::<RowPage>(
                self.row_pool_guard(guards),
                page_id,
                LatchFallbackMode::Shared,
            )
            .await?
            .lock_shared_async()
            .await)
    }

    #[inline]
    pub(crate) async fn get_row_page_versioned_shared(
        &self,
        guards: &PoolGuards,
        page_id: VersionedPageID,
    ) -> Result<Option<PageSharedGuard<RowPage>>> {
        let guard = self
            .mem_pool()
            .get_page_versioned::<RowPage>(
                self.row_pool_guard(guards),
                page_id,
                LatchFallbackMode::Shared,
            )
            .await?;
        Ok(match guard {
            Some(guard) => guard.lock_shared_async().await,
            None => None,
        })
    }

    #[inline]
    pub(crate) async fn get_row_page_exclusive(
        &self,
        guards: &PoolGuards,
        page_id: PageID,
    ) -> Result<Option<PageExclusiveGuard<RowPage>>> {
        Ok(self
            .mem_pool()
            .get_page::<RowPage>(
                self.row_pool_guard(guards),
                page_id,
                LatchFallbackMode::Exclusive,
            )
            .await?
            .lock_exclusive_async()
            .await)
    }

    #[inline]
    async fn must_get_row_page_shared(
        &self,
        guards: &PoolGuards,
        page_id: PageID,
    ) -> Result<PageSharedGuard<RowPage>> {
        self.get_row_page_shared(guards, page_id)
            .await
            .and_then(|guard| {
                guard.ok_or_else(|| {
                    Report::new(InternalError::RowPageMissing)
                        .attach(format!("page_id={page_id}"))
                        .into()
                })
            })
    }

    #[inline]
    async fn must_get_row_page_exclusive(
        &self,
        guards: &PoolGuards,
        page_id: PageID,
    ) -> Result<PageExclusiveGuard<RowPage>> {
        self.get_row_page_exclusive(guards, page_id)
            .await
            .and_then(|guard| {
                guard.ok_or_else(|| {
                    Report::new(InternalError::RowPageMissing)
                        .attach(format!("page_id={page_id}"))
                        .into()
                })
            })
    }

    #[inline]
    pub(crate) async fn try_get_insert_page(
        &self,
        guards: &PoolGuards,
        count: usize,
        redo_ctx: Option<RowPageCreateRedoCtx<'_>>,
    ) -> Result<PageSharedGuard<RowPage>> {
        let meta_pool_guard = guards.meta_guard();
        let row_pool_guard = self.row_pool_guard(guards);
        self.blk_idx
            .try_get_insert_page_with_redo(
                meta_pool_guard,
                self.mem_pool(),
                row_pool_guard,
                &self.metadata,
                count,
                redo_ctx,
            )
            .await
    }

    #[inline]
    pub(crate) async fn get_insert_page_exclusive(
        &self,
        guards: &PoolGuards,
        count: usize,
        redo_ctx: Option<RowPageCreateRedoCtx<'_>>,
    ) -> Result<PageExclusiveGuard<RowPage>> {
        let meta_pool_guard = guards.meta_guard();
        let row_pool_guard = self.row_pool_guard(guards);
        self.blk_idx
            .get_insert_page_exclusive_with_redo(
                meta_pool_guard,
                self.mem_pool(),
                row_pool_guard,
                &self.metadata,
                count,
                redo_ctx,
            )
            .await
    }

    #[inline]
    pub(crate) async fn allocate_row_page_at(
        &self,
        guards: &PoolGuards,
        count: usize,
        page_id: PageID,
    ) -> Result<PageExclusiveGuard<RowPage>> {
        let meta_pool_guard = guards.meta_guard();
        let row_pool_guard = self.row_pool_guard(guards);
        self.blk_idx
            .allocate_row_page_at(
                meta_pool_guard,
                self.mem_pool(),
                row_pool_guard,
                &self.metadata,
                count,
                page_id,
            )
            .await
    }

    #[inline]
    pub(crate) fn cache_exclusive_insert_page(&self, guard: PageExclusiveGuard<RowPage>) {
        self.blk_idx.cache_exclusive_insert_page(guard)
    }

    pub(crate) async fn mem_scan<F>(&self, guards: &PoolGuards, mut page_action: F)
    where
        F: FnMut(PageSharedGuard<RowPage>) -> bool,
    {
        let meta_pool_guard = guards.meta_guard();
        let mut cursor = self.blk_idx.mem_cursor(meta_pool_guard);
        cursor.seek(0).await;
        while let Some(leaf) = cursor.next().await {
            let g = leaf.lock_shared_async().await.unwrap();
            debug_assert!(g.page().is_leaf());
            let entries = g.page().leaf_entries();
            for page_entry in entries {
                let page_guard = self
                    .must_get_row_page_shared(guards, page_entry.page_id)
                    .await
                    .expect("table mem scan should not ignore row-page access failures");
                if !page_action(page_guard) {
                    return;
                }
            }
        }
    }

    #[inline]
    pub(crate) async fn find_row(
        &self,
        guards: &PoolGuards,
        row_id: RowID,
        storage: Option<&ColumnStorage>,
    ) -> RowLocation {
        let meta_pool_guard = guards.meta_guard();
        let disk_pool_guard = storage.map(|_| guards.disk_guard());
        self.blk_idx
            .find_row(meta_pool_guard, disk_pool_guard, row_id, storage)
            .await
    }

    #[inline]
    pub(crate) async fn try_find_row(
        &self,
        guards: &PoolGuards,
        row_id: RowID,
        storage: Option<&ColumnStorage>,
    ) -> Result<RowLocation> {
        let meta_pool_guard = guards.meta_guard();
        let disk_pool_guard = storage.map(|_| guards.disk_guard());
        self.blk_idx
            .try_find_row(meta_pool_guard, disk_pool_guard, row_id, storage)
            .await
    }
}

impl ColumnStorage {
    #[inline]
    pub(crate) fn new(
        file: Arc<TableFile>,
        disk_pool: QuiescentGuard<ReadonlyBufferPool>,
    ) -> Result<Self> {
        // `catalog_load_boundary`: table construction binds the loaded root to
        // initialize column storage and validate secondary root layout.
        let active_root = file.active_root_unchecked();
        let metadata = Arc::clone(&active_root.metadata);
        if active_root.secondary_index_roots.len() != metadata.index_specs.len() {
            return Err(Report::new(DataIntegrityError::InvalidRootInvariant)
                .attach(format!(
                    "secondary root count mismatch: root_count={}, index_count={}",
                    active_root.secondary_index_roots.len(),
                    metadata.index_specs.len()
                ))
                .into());
        }
        let secondary_indexes = (0..metadata.index_specs.len())
            .map(|index_no| {
                SecondaryDiskTreeRuntime::new(
                    index_no,
                    Arc::clone(&metadata),
                    Arc::clone(&file),
                    disk_pool.clone(),
                )
            })
            .collect::<Result<Vec<_>>>()?
            .into_boxed_slice();
        Ok(ColumnStorage {
            file,
            disk_pool,
            deletion_buffer: ColumnDeletionBuffer::new(),
            secondary_indexes,
        })
    }

    /// Returns the underlying table file for persisted column data.
    #[inline]
    pub(crate) fn file(&self) -> &Arc<TableFile> {
        &self.file
    }

    /// Bind one active root observation under a transaction read proof.
    #[inline]
    pub(crate) fn with_active_root<'ctx, R, F>(&self, _proof: &TrxReadProof<'ctx>, f: F) -> R
    where
        F: for<'root> FnOnce(&'root ActiveRoot) -> R,
    {
        let root = self.file().active_root_unchecked();
        f(root)
    }

    /// Returns the read-only buffer pool used for persisted blocks.
    #[inline]
    pub fn disk_pool(&self) -> &QuiescentGuard<ReadonlyBufferPool> {
        &self.disk_pool
    }

    /// Returns the deletion buffer tracking persisted-row tombstones.
    #[inline]
    pub fn deletion_buffer(&self) -> &ColumnDeletionBuffer {
        &self.deletion_buffer
    }

    /// Returns one reusable secondary DiskTree runtime by index number.
    #[inline]
    pub(crate) fn secondary_index_runtime(
        &self,
        index_no: usize,
    ) -> Result<&SecondaryDiskTreeRuntime> {
        self.secondary_indexes
            .get(index_no)
            .ok_or_else(|| missing_secondary_index(index_no, self.secondary_indexes.len()))
    }

    /// Returns the reusable secondary DiskTree runtimes owned by this table.
    #[inline]
    pub(crate) fn secondary_index_runtimes(&self) -> &[SecondaryDiskTreeRuntime] {
        &self.secondary_indexes
    }
}

impl Table {
    /// Create a new table.
    #[inline]
    pub(crate) async fn new(
        mem_pool: QuiescentGuard<EvictableBufferPool>,
        index_pool: QuiescentGuard<EvictableBufferPool>,
        index_pool_guard: &crate::buffer::PoolGuard,
        table_id: TableID,
        blk_idx: BlockIndex,
        file: Arc<TableFile>,
        disk_pool: QuiescentGuard<ReadonlyBufferPool>,
    ) -> Result<Self> {
        // `catalog_load_boundary`: runtime table construction uses the loaded
        // root to seed metadata and hot/cold secondary-index state.
        let active_root = file.active_root_unchecked();
        let metadata = Arc::clone(&active_root.metadata);
        let secondary_index_count = metadata.index_specs.len();
        let sec_idx = build_dual_tree_secondary_indexes(
            index_pool,
            index_pool_guard,
            Arc::clone(&metadata),
            Arc::clone(&file),
            disk_pool.clone(),
            active_root.trx_id,
        )
        .await?;
        let mem = GenericMemTable {
            table_id,
            metadata,
            mem_pool: mem_pool.clone(),
            row_pool_role: mem_pool.row_pool_role(),
            index_pool_role: PoolRole::Index,
            blk_idx,
            sec_idx: Box::new([]),
        };
        let storage = ColumnStorage::new(file, disk_pool)?;
        debug_assert_eq!(
            storage.secondary_index_runtimes().len(),
            secondary_index_count
        );
        debug_assert_eq!(sec_idx.len(), secondary_index_count);
        Ok(Table {
            mem,
            storage,
            sec_idx,
        })
    }

    /// Build a lightweight operation accessor over this table runtime.
    #[inline]
    pub fn accessor(&self) -> HybridTableAccessor<'_> {
        HybridTableAccessor::from(self)
    }

    /// Bind one active root observation under a transaction read proof.
    #[inline]
    pub(crate) fn with_active_root<'ctx, R, F>(&self, proof: &TrxReadProof<'ctx>, f: F) -> R
    where
        F: for<'root> FnOnce(&'root ActiveRoot) -> R,
    {
        self.storage.with_active_root(proof, f)
    }

    /// Capture an owned table-root snapshot for this table.
    #[inline]
    pub(crate) fn root_snapshot<'ctx>(
        &self,
        proof: &TrxReadProof<'ctx>,
    ) -> Result<TableRootSnapshot<'ctx>> {
        Ok(self.with_active_root(proof, |root| {
            TableRootSnapshot::from_active_root(root, proof)
        }))
    }

    /// Returns the deletion buffer tracking persisted-row tombstones.
    #[inline]
    pub fn deletion_buffer(&self) -> &ColumnDeletionBuffer {
        self.storage.deletion_buffer()
    }

    /// Returns the user-table composite secondary-index array.
    #[inline]
    pub(crate) fn sec_idx(&self) -> &[SecondaryIndex<EvictableBufferPool>] {
        &self.sec_idx
    }

    /// Returns the readonly disk buffer pool used by persisted table data.
    #[inline]
    pub(crate) fn disk_pool(&self) -> &QuiescentGuard<ReadonlyBufferPool> {
        self.storage.disk_pool()
    }

    /// Returns the backing table file for this user table.
    #[inline]
    pub(crate) fn file(&self) -> &Arc<TableFile> {
        self.storage.file()
    }

    #[inline]
    pub(crate) async fn find_row(&self, guards: &PoolGuards, row_id: RowID) -> RowLocation {
        GenericMemTable::find_row(self, guards, row_id, Some(&self.storage)).await
    }

    async fn collect_frozen_pages(&self, guards: &PoolGuards) -> (Vec<FrozenPage>, Option<TrxID>) {
        let mut frozen_pages = Vec::new();
        let pivot_row_id = self.pivot_row_id();
        let mut expected_row_id = pivot_row_id;
        let mut heap_redo_start_ts = None;
        let mut seen_first_page = false;
        self.mem_scan(guards, |page_guard| {
            let page = page_guard.page();
            if !seen_first_page {
                seen_first_page = true;
                debug_assert_eq!(
                    page.header.start_row_id, pivot_row_id,
                    "first in-memory row page must start from pivot_row_id"
                );
            }
            if page.header.start_row_id != expected_row_id {
                return false;
            }
            let (ctx, _) = page_guard.ctx_and_page();
            let row_ver = ctx.row_ver().unwrap();
            if row_ver.state() != RowPageState::Frozen {
                if row_ver.state() == RowPageState::Active {
                    // heap redo start ts is creation cts of first remaining active page.
                    heap_redo_start_ts = Some(row_ver.create_cts());
                }
                return false;
            }
            let end_row_id = page.header.start_row_id + page.header.max_row_count as u64;
            frozen_pages.push(FrozenPage {
                page_id: page_guard.page_id(),
                start_row_id: page.header.start_row_id,
                end_row_id,
            });
            expected_row_id = end_row_id;
            true
        })
        .await;
        (frozen_pages, heap_redo_start_ts)
    }

    async fn wait_for_frozen_pages_stable(
        &self,
        guards: &PoolGuards,
        trx_sys: &TransactionSystem,
        frozen_pages: &[FrozenPage],
    ) -> Result<()> {
        loop {
            let min_active_sts = trx_sys.calc_min_active_sts_for_gc();
            let mut stabilized = true;
            for page_info in frozen_pages {
                // A potential optimization is to check row version map without loading
                // row page back. This requires interface change of buffer pool.
                let page_guard = self
                    .must_get_row_page_shared(guards, page_info.page_id)
                    .await?;
                let (ctx, _) = page_guard.ctx_and_page();
                let row_ver = ctx.row_ver().unwrap();
                // Check whether all insert and updates on this page are committed.
                // This may be blocked by a long-running irrelevant transaction
                // but we accept it now.
                if row_ver.max_ins_sts() >= min_active_sts {
                    stabilized = false;
                    break;
                }
            }
            if stabilized {
                break;
            }
            smol::Timer::after(Duration::from_secs(1)).await;
        }
        Ok(())
    }

    async fn set_frozen_pages_to_transition(
        &self,
        guards: &PoolGuards,
        frozen_pages: &[FrozenPage],
        cutoff_ts: TrxID,
    ) -> Result<()> {
        for page_info in frozen_pages {
            let page_guard = self
                .must_get_row_page_shared(guards, page_info.page_id)
                .await?;
            let (ctx, page) = page_guard.ctx_and_page();
            ctx.row_ver().unwrap().set_transition();
            self.capture_delete_markers_for_transition(page, ctx, cutoff_ts);
        }
        Ok(())
    }

    async fn build_lwc_blocks(
        &self,
        guards: &PoolGuards,
        cutoff_ts: TrxID,
        frozen_pages: &[FrozenPage],
        mut collect_visible_row: Option<VisibleRowCollector<'_>>,
    ) -> Result<Vec<LwcBlockPersist>> {
        #[cfg(test)]
        {
            if tests::test_force_lwc_build_error_enabled() {
                return Err(Report::new(InternalError::InjectedTestFailure).into());
            }
        }
        let mut lwc_blocks = Vec::new();
        if !frozen_pages.is_empty() {
            let metadata = self.metadata();
            let mut builder = LwcBuilder::new(metadata);
            let mut current_start: RowID = 0;
            let mut current_end: RowID = 0;
            for page_info in frozen_pages {
                let page_guard = self
                    .must_get_row_page_shared(guards, page_info.page_id)
                    .await?;
                let (ctx, page) = page_guard.ctx_and_page();
                let view = page.vector_view_in_transition(metadata, ctx, cutoff_ts, cutoff_ts);
                if view.rows_non_deleted() == 0 {
                    continue;
                }
                if let Some(collect_visible_row) = collect_visible_row.as_mut() {
                    for (start_idx, end_idx) in view.range_non_deleted() {
                        for row_idx in start_idx..end_idx {
                            collect_visible_row(page, row_idx, page.row_id(row_idx))?;
                        }
                    }
                }
                if builder.is_empty() {
                    current_start = page_info.start_row_id;
                    current_end = page_info.end_row_id;
                }
                if !builder.append_view(page, view)? {
                    let shape = ColumnBlockEntryShape::new(
                        current_start,
                        current_end,
                        builder.row_ids().to_vec(),
                        Vec::new(),
                    )?;
                    let buf = builder.build(shape.row_shape_fingerprint())?;
                    lwc_blocks.push(LwcBlockPersist { shape, buf });
                    builder = LwcBuilder::new(metadata);
                    current_start = page_info.start_row_id;
                    current_end = page_info.end_row_id;
                    let view = page.vector_view_in_transition(metadata, ctx, cutoff_ts, cutoff_ts);
                    if !builder.append_view(page, view)? {
                        return Err(Report::new(InternalError::LwcBuilderMisuse)
                            .attach(format!(
                                "single row page does not fit in LWC block: page_id={}",
                                page_info.page_id
                            ))
                            .into());
                    }
                } else {
                    current_end = page_info.end_row_id;
                }
            }
            if !builder.is_empty() {
                let shape = ColumnBlockEntryShape::new(
                    current_start,
                    current_end,
                    builder.row_ids().to_vec(),
                    Vec::new(),
                )?;
                let buf = builder.build(shape.row_shape_fingerprint())?;
                lwc_blocks.push(LwcBlockPersist { shape, buf });
            }
        }
        Ok(lwc_blocks)
    }

    fn capture_delete_markers_for_transition(
        &self,
        page: &RowPage,
        ctx: &crate::buffer::frame::FrameContext,
        cutoff_ts: TrxID,
    ) {
        let Some(map) = ctx.row_ver() else {
            return;
        };
        let row_count = page.header.row_count();
        for row_idx in 0..row_count {
            let undo_guard = map.read_latch(row_idx);
            let Some(head) = undo_guard.as_ref() else {
                continue;
            };
            let mut ts = head.next.main.status.ts();
            let mut status = match &head.next.main.status {
                UndoStatus::Ref(status) => Some(status.clone()),
                UndoStatus::CTS(_) => None,
            };
            let mut entry = head.next.main.entry.clone();
            loop {
                let row_id = page.row_id(row_idx);
                match entry.as_ref().kind {
                    RowUndoKind::Lock => {
                        if let Some(trx_status) = status.as_ref()
                            && !trx_is_committed(trx_status.ts())
                        {
                            let _ = self.deletion_buffer().put_ref(
                                row_id,
                                trx_status.clone(),
                                MAX_SNAPSHOT_TS,
                            );
                        }
                    }
                    RowUndoKind::Delete => {
                        match status.as_ref() {
                            Some(trx_status) => {
                                let status_ts = trx_status.ts();
                                if trx_is_committed(status_ts) {
                                    if status_ts >= cutoff_ts {
                                        let _ =
                                            self.deletion_buffer().put_committed(row_id, status_ts);
                                    }
                                } else {
                                    let _ = self.deletion_buffer().put_ref(
                                        row_id,
                                        trx_status.clone(),
                                        MAX_SNAPSHOT_TS,
                                    );
                                }
                            }
                            None => {
                                if ts >= cutoff_ts {
                                    let _ = self.deletion_buffer().put_committed(row_id, ts);
                                }
                            }
                        };
                        break;
                    }
                    RowUndoKind::Insert | RowUndoKind::Update(_) => {
                        break;
                    }
                }
                let next = entry.as_ref().next.as_ref().map(|next| {
                    let ts = next.main.status.ts();
                    let status = match &next.main.status {
                        UndoStatus::Ref(status) => Some(status.clone()),
                        UndoStatus::CTS(_) => None,
                    };
                    (ts, status, next.main.entry.clone())
                });
                let Some((next_ts, next_status, next_entry)) = next else {
                    break;
                };
                ts = next_ts;
                status = next_status;
                entry = next_entry;
            }
        }
    }

    /// Returns total number of row pages.
    #[inline]
    pub async fn total_row_pages(&self, guards: &PoolGuards) -> usize {
        let mut res = 0usize;
        let pivot_row_id = self.pivot_row_id();
        let meta_pool_guard = guards.meta_guard();
        let mut cursor = self.blk_idx().mem_cursor(meta_pool_guard);
        cursor.seek(pivot_row_id).await;
        while let Some(leaf) = cursor.next().await {
            let g = leaf.lock_shared_async().await.unwrap();
            debug_assert!(g.page().is_leaf());
            res += g
                .page()
                .leaf_entries()
                .iter()
                .filter(|entry| entry.row_id >= pivot_row_id)
                .count();
        }
        res
    }

    async fn mem_scan<F>(&self, guards: &PoolGuards, mut page_action: F)
    where
        F: FnMut(PageSharedGuard<RowPage>) -> bool,
    {
        // With cursor, we lock two pages in block index and one row page
        // when scanning rows.
        let meta_pool_guard = guards.meta_guard();
        let pivot_row_id = self.pivot_row_id();
        let mut cursor = self.blk_idx().mem_cursor(meta_pool_guard);
        cursor.seek(pivot_row_id).await;
        while let Some(leaf) = cursor.next().await {
            let g = leaf.lock_shared_async().await.unwrap();
            debug_assert!(g.page().is_leaf());
            let entries = g.page().leaf_entries();
            for page_entry in entries {
                if page_entry.row_id < pivot_row_id {
                    continue;
                }
                let page_guard = self
                    .must_get_row_page_shared(guards, page_entry.page_id)
                    .await
                    .expect("table mem scan should not ignore row-page access failures");
                if !page_action(page_guard) {
                    return;
                }
            }
        }
    }

    #[inline]
    fn recover_row_insert_to_page(
        &self,
        page_guard: &mut PageExclusiveGuard<RowPage>,
        row_id: RowID,
        cols: &[Val],
        cts: TrxID,
    ) -> Recover {
        let (ctx, page) = page_guard.ctx_and_page_mut();
        debug_assert!(self.metadata().col_count() == page.header.col_count as usize);
        debug_assert!(cols.len() == page.header.col_count as usize);
        let row_idx = page.row_idx(row_id);
        // Insert log should always be located to an empty slot.
        debug_assert!(ctx.recover().unwrap().is_vacant(row_idx));
        let var_len = var_len_for_insert(self.metadata(), cols);
        let (var_offset, var_end) = if let Some(var_offset) = page.request_free_space(var_len) {
            (var_offset, var_offset + var_len)
        } else {
            return Recover::NoSpace;
        };
        // update count field to include current row id.
        page.update_count_to_include_row_id(row_id);
        // insert CTS.
        ctx.recover_mut().unwrap().insert_at(row_idx, cts);
        let row_idx = page.row_idx(row_id);
        let mut row = page.row_mut_exclusive(row_idx, var_offset, var_end);
        debug_assert!(row.is_deleted()); // before recovery, this row should be initialized as deleted.
        let metadata = self.metadata();
        for (user_col_idx, user_col) in cols.iter().enumerate() {
            row.update_col(metadata, user_col_idx, user_col, false);
        }
        row.finish_insert()
    }

    #[inline]
    async fn recover_index_insert(
        &self,
        guards: &PoolGuards,
        key: SelectKey,
        row_id: RowID,
        cts: TrxID,
    ) -> Result<RecoverIndex> {
        if self.metadata().index_specs[key.index_no].unique() {
            self.recover_unique_index_insert(guards, key, row_id, cts)
                .await
        } else {
            self.recover_non_unique_index_insert(guards, key, row_id)
                .await
        }
    }

    #[inline]
    async fn recover_index_delete(
        &self,
        guards: &PoolGuards,
        key: SelectKey,
        row_id: RowID,
    ) -> Result<RecoverIndex> {
        if self.metadata().index_specs[key.index_no].unique() {
            self.recover_unique_index_delete(guards, key, row_id).await
        } else {
            self.recover_non_unique_index_delete(guards, key, row_id)
                .await
        }
    }

    #[inline]
    fn recover_row_update_to_page(
        &self,
        page_guard: &mut PageExclusiveGuard<RowPage>,
        row_id: RowID,
        cols: &[UpdateCol],
        cts: TrxID,
        index_change_cols: Option<&mut HashMap<usize, Val>>,
    ) -> Recover {
        let (ctx, page) = page_guard.ctx_and_page_mut();
        // column indexes must be in range
        debug_assert!(
            {
                cols.iter()
                    .all(|uc| uc.idx < page.header.col_count as usize)
            },
            "update column indexes must be in range"
        );
        // column indexes should be in order.
        debug_assert!(
            {
                cols.is_empty()
                    || cols
                        .iter()
                        .zip(cols.iter().skip(1))
                        .all(|(l, r)| l.idx < r.idx)
            },
            "update columns should be in order"
        );
        if !page.row_id_in_valid_range(row_id) {
            return Recover::NotFound;
        }
        let row_idx = page.row_idx(row_id);
        if page.row(row_idx).is_deleted() {
            return Recover::AlreadyDeleted;
        }
        let var_len = page.var_len_for_update(row_idx, cols);
        let (var_offset, var_end) = if let Some(var_offset) = page.request_free_space(var_len) {
            (var_offset, var_offset + var_len)
        } else {
            return Recover::NoSpace;
        };
        // update CTS.
        ctx.recover_mut().unwrap().update_at(row_idx, cts);
        let mut row = page.row_mut_exclusive(row_idx, var_offset, var_end);
        debug_assert_eq!(row_id, row.row_id());

        let metadata = self.metadata();
        let disable_index = index_change_cols.is_none();
        if disable_index {
            for uc in cols {
                row.update_col(metadata, uc.idx, &uc.val, true);
            }
            row.finish_update()
        } else {
            // collect index change columns.
            let index_change_cols = index_change_cols.unwrap();
            for uc in cols {
                if let Some((old_val, _)) = row.different(metadata, uc.idx, &uc.val) {
                    // we also check whether the value change is related to any index,
                    // so we can update index later.
                    if metadata.index_cols.contains(&uc.idx) {
                        index_change_cols.insert(uc.idx, old_val);
                    }
                    // actual update
                    row.update_col(metadata, uc.idx, &uc.val, true);
                }
            }
            row.finish_update()
        }
    }

    #[inline]
    fn recover_row_delete_to_page(
        &self,
        page_guard: &mut PageExclusiveGuard<RowPage>,
        row_id: RowID,
        cts: TrxID,
        index_cols: Option<&mut HashMap<usize, Val>>,
    ) -> Recover {
        let (ctx, page) = page_guard.ctx_and_page_mut();
        if !page.row_id_in_valid_range(row_id) {
            return Recover::NotFound;
        }
        let row_idx = page.row_idx(row_id);
        if page.row(row_idx).is_deleted() {
            return Recover::AlreadyDeleted;
        }
        ctx.recover_mut().unwrap().update_at(row_idx, cts);
        page.set_deleted_exclusive(row_idx, true);
        let metadata = self.metadata();
        if let Some(index_cols) = index_cols {
            // save index columns for index update.
            let row = page.row(row_idx);
            for idx_col_no in &metadata.index_cols {
                let val = row.val(metadata, *idx_col_no);
                index_cols.insert(*idx_col_no, val);
            }
        }
        Recover::Ok
    }

    #[inline]
    async fn recover_unique_index_insert(
        &self,
        guards: &PoolGuards,
        key: SelectKey,
        row_id: RowID,
        cts: TrxID,
    ) -> Result<RecoverIndex> {
        let index = self.sec_idx()[key.index_no].unique_mem()?;
        let index_pool_guard = self.index_pool_guard(guards);
        loop {
            match index
                .insert_if_not_exists(index_pool_guard, &key.vals, row_id, true, MIN_SNAPSHOT_TS)
                .await?
            {
                IndexInsert::Ok(_) => {
                    // insert index success.
                    return Ok(RecoverIndex::Ok);
                }
                IndexInsert::DuplicateKey(old_row_id, deleted) => {
                    debug_assert!(old_row_id != row_id);
                    // Find CTS of old row.
                    match self.find_recover_cts_for_row_id(guards, old_row_id).await? {
                        Some(old_cts) => {
                            if cts < old_cts {
                                // Current row has smaller CTS, that means this insert
                                // can be skipped, and probably there is a followed DELETE
                                // operation on it.
                                return Ok(RecoverIndex::InsertOutdated);
                            }
                            // Current row is newer, we should update the index entry.
                            let old_row_id = if deleted {
                                old_row_id.deleted()
                            } else {
                                old_row_id
                            };
                            match index
                                .compare_exchange(
                                    index_pool_guard,
                                    &key.vals,
                                    old_row_id,
                                    row_id,
                                    MIN_SNAPSHOT_TS,
                                )
                                .await?
                            {
                                IndexCompareExchange::Ok => {
                                    return Ok(RecoverIndex::Ok);
                                }
                                // retry the insert.
                                IndexCompareExchange::Mismatch
                                | IndexCompareExchange::NotExists => {}
                            }
                        }
                        None => {
                            unreachable!()
                        }
                    }
                }
            }
        }
    }

    #[inline]
    async fn recover_non_unique_index_insert(
        &self,
        guards: &PoolGuards,
        key: SelectKey,
        row_id: RowID,
    ) -> Result<RecoverIndex> {
        let index = self.sec_idx()[key.index_no].non_unique_mem()?;
        let index_pool_guard = self.index_pool_guard(guards);
        let res = index
            .insert_if_not_exists(index_pool_guard, &key.vals, row_id, true, MIN_SNAPSHOT_TS)
            .await?;
        recover::ensure_recovery_index_insert(key.index_no, res)?;
        Ok(RecoverIndex::Ok)
    }

    #[inline]
    async fn recover_unique_index_delete(
        &self,
        guards: &PoolGuards,
        key: SelectKey,
        row_id: RowID,
    ) -> Result<RecoverIndex> {
        let index = self.sec_idx()[key.index_no].unique_mem()?;
        let index_pool_guard = self.index_pool_guard(guards);
        if !index
            .compare_delete(index_pool_guard, &key.vals, row_id, true, MIN_SNAPSHOT_TS)
            .await?
        {
            // Another recover thread concurrently insert index entry with same key, probably with greater CTS.
            // We just skip this deletion.
            return Ok(RecoverIndex::DeleteOutdated);
        }
        Ok(RecoverIndex::Ok)
    }

    #[inline]
    async fn recover_non_unique_index_delete(
        &self,
        guards: &PoolGuards,
        key: SelectKey,
        row_id: RowID,
    ) -> Result<RecoverIndex> {
        let index = self.sec_idx()[key.index_no].non_unique_mem()?;
        let index_pool_guard = self.index_pool_guard(guards);
        if !index
            .compare_delete(index_pool_guard, &key.vals, row_id, true, MIN_SNAPSHOT_TS)
            .await?
        {
            return Ok(RecoverIndex::DeleteOutdated);
        }
        Ok(RecoverIndex::Ok)
    }

    #[inline]
    async fn find_recover_cts_for_row_id(
        &self,
        guards: &PoolGuards,
        row_id: RowID,
    ) -> Result<Option<TrxID>> {
        Ok(match self.find_row(guards, row_id).await {
            RowLocation::NotFound => None,
            RowLocation::LwcBlock { .. } => todo!("lwc block"),
            RowLocation::RowPage(page_id) => {
                let page_guard = self.must_get_row_page_shared(guards, page_id).await?;
                debug_assert!(validate_page_row_range(&page_guard, page_id, row_id));
                let (ctx, page) = page_guard.ctx_and_page();
                let row_idx = page.row_idx(row_id);
                let access = RowReadAccess::new(page, ctx, row_idx);
                access.ts()
            }
        })
    }
}

impl Deref for Table {
    type Target = GenericMemTable<EvictableBufferPool, EvictableBufferPool>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.mem
    }
}

#[inline]
fn validate_page_row_range(
    page_guard: &PageSharedGuard<RowPage>,
    page_id: PageID,
    row_id: RowID,
) -> bool {
    if page_guard.page_id() != page_id {
        return false;
    }
    page_guard.page().row_id_in_valid_range(row_id)
}

#[inline]
fn row_len(metadata: &TableMetadata, cols: &[Val]) -> usize {
    let var_len = metadata
        .var_cols
        .iter()
        .map(|idx| {
            let val = &cols[*idx];
            match val {
                Val::Null => 0,
                Val::VarByte(var) => {
                    if var.len() <= PAGE_VAR_LEN_INLINE {
                        0
                    } else {
                        var.len()
                    }
                }
                _ => unreachable!(),
            }
        })
        .sum::<usize>();
    metadata.fix_len + var_len
}

enum InsertRowIntoPage {
    Ok(RowID, PageSharedGuard<RowPage>),
    NoSpaceOrFrozen(Vec<Val>, RowUndoKind, Vec<IndexBranch>),
}

enum UpdateRowInplace {
    // We keep row page lock if there is any index change,
    // so we can read latest values from page.
    // The hash map stores the changed column number and its old value.
    // for other columns in the changed index, we can read value(old and new are same)
    // from current page.
    Ok(RowID, HashMap<usize, Val>, PageSharedGuard<RowPage>),
    RowNotFound,
    RowDeleted,
    WriteConflict,
    RetryInTransition,
    NoFreeSpace(
        RowID,
        Vec<(Val, Option<u16>)>,
        Vec<UpdateCol>,
        PageSharedGuard<RowPage>,
    ),
}

enum DeleteInternal {
    Ok(PageSharedGuard<RowPage>),
    NotFound,
    WriteConflict,
    RetryInTransition,
}

#[inline]
fn index_key_is_changed(index_spec: &IndexSpec, index_change_cols: &HashMap<usize, Val>) -> bool {
    index_spec
        .index_cols
        .iter()
        .any(|key| index_change_cols.contains_key(&(key.col_no as usize)))
}

#[inline]
fn index_key_replace(
    index_spec: &IndexSpec,
    key: &SelectKey,
    updates: &HashMap<usize, Val>,
) -> SelectKey {
    let vals: Vec<Val> = index_spec
        .index_cols
        .iter()
        .zip(&key.vals)
        .map(|(ik, val)| {
            let col_no = ik.col_no as usize;
            updates.get(&col_no).cloned().unwrap_or_else(|| val.clone())
        })
        .collect();
    SelectKey::new(key.index_no, vals)
}

#[inline]
fn read_latest_index_key(
    metadata: &TableMetadata,
    index_no: usize,
    page_guard: &PageSharedGuard<RowPage>,
    row_id: RowID,
) -> SelectKey {
    let index_spec = &metadata.index_specs[index_no];
    let mut new_key = SelectKey::null(index_no, index_spec.index_cols.len());
    for (pos, key) in index_spec.index_cols.iter().enumerate() {
        let (ctx, page) = page_guard.ctx_and_page();
        let access = RowReadAccess::new(page, ctx, page.row_idx(row_id));
        let val = access.row().val(metadata, key.col_no as usize);
        new_key.vals[pos] = val;
    }
    new_key
}
