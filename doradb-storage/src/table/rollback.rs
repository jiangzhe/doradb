use super::{GenericMemTable, Table};
use crate::buffer::{BufferPool, EvictableBufferPool, FixedBufferPool, PoolGuard, PoolGuards};
use crate::catalog::CatalogTable;
use crate::error::{Error, InternalError, Result};
use crate::index::util::Maskable;
use crate::index::{IndexCompareExchange, NonUniqueIndex, UniqueIndex};
use crate::row::RowID;
use crate::row::ops::SelectKey;
use crate::trx::TrxID;
use crate::trx::undo::{IndexUndo, IndexUndoKind};
use error_stack::Report;

#[inline]
fn secondary_index_kind_mismatch(operation: &'static str, expected: &'static str) -> Error {
    Report::new(InternalError::SecondaryIndexKindMismatch)
        .attach(format!("operation={operation}, expected={expected}"))
        .into()
}

/// Rollback adapter for table-specific secondary-index runtimes.
///
/// User tables route rollback through dual-tree secondary indexes, while
/// catalog tables keep using their in-memory generic indexes. Implementors
/// provide the primitive index operations; the shared rollback body applies
/// undo entries in reverse order and preserves the exact old index value
/// recorded in the undo log.
pub(crate) trait IndexRollback {
    /// Row buffer pool type owned by the table runtime.
    type RowPool: BufferPool + 'static;
    /// Secondary-index buffer pool type owned by the table runtime.
    type IndexPool: BufferPool + 'static;

    /// Returns the shared MemTable metadata and index-pool binding.
    fn mem_table(&self) -> &GenericMemTable<Self::RowPool, Self::IndexPool>;

    /// Marks an existing unique entry as deleted.
    async fn unique_mask_as_deleted(
        &self,
        index_pool_guard: &PoolGuard,
        key: &SelectKey,
        row_id: RowID,
        ts: TrxID,
    ) -> Result<bool>;

    /// Removes a unique entry when the current value matches `row_id`.
    async fn unique_compare_delete(
        &self,
        index_pool_guard: &PoolGuard,
        key: &SelectKey,
        row_id: RowID,
        ignore_del_mask: bool,
        ts: TrxID,
    ) -> Result<bool>;

    /// Atomically replaces a unique entry when the current value matches.
    async fn unique_compare_exchange(
        &self,
        index_pool_guard: &PoolGuard,
        key: &SelectKey,
        old_row_id: RowID,
        new_row_id: RowID,
        ts: TrxID,
    ) -> Result<IndexCompareExchange>;

    /// Marks an existing non-unique exact entry as deleted.
    async fn non_unique_mask_as_deleted(
        &self,
        index_pool_guard: &PoolGuard,
        key: &SelectKey,
        row_id: RowID,
        ts: TrxID,
    ) -> Result<bool>;

    /// Marks an existing non-unique exact entry as active.
    async fn non_unique_mask_as_active(
        &self,
        index_pool_guard: &PoolGuard,
        key: &SelectKey,
        row_id: RowID,
        ts: TrxID,
    ) -> Result<bool>;

    /// Removes a non-unique exact entry when the current value matches.
    async fn non_unique_compare_delete(
        &self,
        index_pool_guard: &PoolGuard,
        key: &SelectKey,
        row_id: RowID,
        ignore_del_mask: bool,
        ts: TrxID,
    ) -> Result<bool>;

    /// Rolls back one secondary-index undo entry against this table runtime.
    async fn rollback_index_entry(
        &self,
        entry: IndexUndo,
        guards: &PoolGuards,
        ts: TrxID,
    ) -> Result<()> {
        let table = self.mem_table();
        let index_pool_guard = table.index_pool_guard(guards);
        match entry.kind {
            IndexUndoKind::InsertUnique(key, merge_old_deleted) => {
                if merge_old_deleted {
                    // This insert reused a delete-masked unique entry for the
                    // same RowID, usually from repeated hot key changes in one
                    // transaction. Rollback restores the masked old owner
                    // instead of removing the entry.
                    let res = self
                        .unique_mask_as_deleted(index_pool_guard, &key, entry.row_id, ts)
                        .await?;
                    assert!(res);
                } else {
                    // The transaction created a new latest unique owner. If it
                    // aborts, remove only that claim; older owners are restored
                    // by separate update/defer-delete undo entries.
                    let res = self
                        .unique_compare_delete(index_pool_guard, &key, entry.row_id, true, ts)
                        .await?;
                    assert!(res);
                }
            }
            IndexUndoKind::InsertNonUnique(key, merge_old_deleted) => {
                if merge_old_deleted {
                    // Same exact `(key, row_id)` was unmasked during a hot
                    // key-change cycle. Rollback masks it back to deleted.
                    let res = self
                        .non_unique_mask_as_deleted(index_pool_guard, &key, entry.row_id, ts)
                        .await?;
                    assert!(res);
                } else {
                    // Remove the exact non-unique claim inserted by the
                    // aborted transaction.
                    let res = self
                        .non_unique_compare_delete(index_pool_guard, &key, entry.row_id, true, ts)
                        .await?;
                    assert!(res);
                }
            }
            IndexUndoKind::UpdateUnique(key, old_row_id, deleted) => {
                // Rollback restores the exact unique-index value that existed
                // before this transaction claimed the key. A delete-masked old
                // owner may already be stale, but rollback must not race purge by
                // inspecting row-page undo chains to prove that. Leaving the
                // stale marker is safe: it remains invisible to reads and normal
                // index purge or a later foreground update can remove it.
                let new_row_id = entry.row_id;
                let restored_row_id = if deleted {
                    old_row_id.deleted()
                } else {
                    old_row_id
                };
                let res = self
                    .unique_compare_exchange(
                        index_pool_guard,
                        &key,
                        new_row_id,
                        restored_row_id,
                        ts,
                    )
                    .await?;
                debug_assert!(res.is_ok());
            }
            IndexUndoKind::DeferDelete(key, unique) => {
                // Foreground hot/cold delete and update paths mask index
                // entries but leave them physically present for rollback and
                // old snapshots. Aborting the transaction unmasks the exact
                // owner that was deferred for deletion.
                if unique {
                    let res = self
                        .unique_compare_exchange(
                            index_pool_guard,
                            &key,
                            entry.row_id.deleted(),
                            entry.row_id,
                            ts,
                        )
                        .await?;
                    assert!(res.is_ok());
                } else {
                    let res = self
                        .non_unique_mask_as_active(index_pool_guard, &key, entry.row_id, ts)
                        .await?;
                    assert!(res);
                }
            }
        }
        Ok(())
    }
}

impl IndexRollback for Table {
    type RowPool = EvictableBufferPool;
    type IndexPool = EvictableBufferPool;

    #[inline]
    fn mem_table(&self) -> &GenericMemTable<Self::RowPool, Self::IndexPool> {
        &self.mem
    }

    #[inline]
    async fn unique_mask_as_deleted(
        &self,
        index_pool_guard: &PoolGuard,
        key: &SelectKey,
        row_id: RowID,
        ts: TrxID,
    ) -> Result<bool> {
        self.sec_idx()[key.index_no]
            .unique_mem()?
            .mask_as_deleted(index_pool_guard, &key.vals, row_id, ts)
            .await
    }

    #[inline]
    async fn unique_compare_delete(
        &self,
        index_pool_guard: &PoolGuard,
        key: &SelectKey,
        row_id: RowID,
        ignore_del_mask: bool,
        ts: TrxID,
    ) -> Result<bool> {
        self.sec_idx()[key.index_no]
            .unique_mem()?
            .compare_delete(index_pool_guard, &key.vals, row_id, ignore_del_mask, ts)
            .await
    }

    #[inline]
    async fn unique_compare_exchange(
        &self,
        index_pool_guard: &PoolGuard,
        key: &SelectKey,
        old_row_id: RowID,
        new_row_id: RowID,
        ts: TrxID,
    ) -> Result<IndexCompareExchange> {
        self.sec_idx()[key.index_no]
            .unique_mem()?
            .compare_exchange(index_pool_guard, &key.vals, old_row_id, new_row_id, ts)
            .await
    }

    #[inline]
    async fn non_unique_mask_as_deleted(
        &self,
        index_pool_guard: &PoolGuard,
        key: &SelectKey,
        row_id: RowID,
        ts: TrxID,
    ) -> Result<bool> {
        self.sec_idx()[key.index_no]
            .non_unique_mem()?
            .mask_as_deleted(index_pool_guard, &key.vals, row_id, ts)
            .await
    }

    #[inline]
    async fn non_unique_mask_as_active(
        &self,
        index_pool_guard: &PoolGuard,
        key: &SelectKey,
        row_id: RowID,
        ts: TrxID,
    ) -> Result<bool> {
        self.sec_idx()[key.index_no]
            .non_unique_mem()?
            .mask_as_active(index_pool_guard, &key.vals, row_id, ts)
            .await
    }

    #[inline]
    async fn non_unique_compare_delete(
        &self,
        index_pool_guard: &PoolGuard,
        key: &SelectKey,
        row_id: RowID,
        ignore_del_mask: bool,
        ts: TrxID,
    ) -> Result<bool> {
        self.sec_idx()[key.index_no]
            .non_unique_mem()?
            .compare_delete(index_pool_guard, &key.vals, row_id, ignore_del_mask, ts)
            .await
    }
}

impl IndexRollback for CatalogTable {
    type RowPool = FixedBufferPool;
    type IndexPool = FixedBufferPool;

    #[inline]
    fn mem_table(&self) -> &GenericMemTable<Self::RowPool, Self::IndexPool> {
        &self.mem
    }

    #[inline]
    async fn unique_mask_as_deleted(
        &self,
        index_pool_guard: &PoolGuard,
        key: &SelectKey,
        row_id: RowID,
        ts: TrxID,
    ) -> Result<bool> {
        self.mem.sec_idx()[key.index_no]
            .unique()
            .ok_or_else(|| secondary_index_kind_mismatch("rollback unique mask deleted", "unique"))?
            .mask_as_deleted(index_pool_guard, &key.vals, row_id, ts)
            .await
    }

    #[inline]
    async fn unique_compare_delete(
        &self,
        index_pool_guard: &PoolGuard,
        key: &SelectKey,
        row_id: RowID,
        ignore_del_mask: bool,
        ts: TrxID,
    ) -> Result<bool> {
        self.mem.sec_idx()[key.index_no]
            .unique()
            .ok_or_else(|| {
                secondary_index_kind_mismatch("rollback unique compare delete", "unique")
            })?
            .compare_delete(index_pool_guard, &key.vals, row_id, ignore_del_mask, ts)
            .await
    }

    #[inline]
    async fn unique_compare_exchange(
        &self,
        index_pool_guard: &PoolGuard,
        key: &SelectKey,
        old_row_id: RowID,
        new_row_id: RowID,
        ts: TrxID,
    ) -> Result<IndexCompareExchange> {
        self.mem.sec_idx()[key.index_no]
            .unique()
            .ok_or_else(|| {
                secondary_index_kind_mismatch("rollback unique compare exchange", "unique")
            })?
            .compare_exchange(index_pool_guard, &key.vals, old_row_id, new_row_id, ts)
            .await
    }

    #[inline]
    async fn non_unique_mask_as_deleted(
        &self,
        index_pool_guard: &PoolGuard,
        key: &SelectKey,
        row_id: RowID,
        ts: TrxID,
    ) -> Result<bool> {
        self.mem.sec_idx()[key.index_no]
            .non_unique()
            .ok_or_else(|| {
                secondary_index_kind_mismatch("rollback non-unique mask deleted", "non-unique")
            })?
            .mask_as_deleted(index_pool_guard, &key.vals, row_id, ts)
            .await
    }

    #[inline]
    async fn non_unique_mask_as_active(
        &self,
        index_pool_guard: &PoolGuard,
        key: &SelectKey,
        row_id: RowID,
        ts: TrxID,
    ) -> Result<bool> {
        self.mem.sec_idx()[key.index_no]
            .non_unique()
            .ok_or_else(|| {
                secondary_index_kind_mismatch("rollback non-unique mask active", "non-unique")
            })?
            .mask_as_active(index_pool_guard, &key.vals, row_id, ts)
            .await
    }

    #[inline]
    async fn non_unique_compare_delete(
        &self,
        index_pool_guard: &PoolGuard,
        key: &SelectKey,
        row_id: RowID,
        ignore_del_mask: bool,
        ts: TrxID,
    ) -> Result<bool> {
        self.mem.sec_idx()[key.index_no]
            .non_unique()
            .ok_or_else(|| {
                secondary_index_kind_mismatch("rollback non-unique compare delete", "non-unique")
            })?
            .compare_delete(index_pool_guard, &key.vals, row_id, ignore_del_mask, ts)
            .await
    }
}
