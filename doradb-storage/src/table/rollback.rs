use super::{ColumnStorage, GenericMemTable, Table};
use crate::buffer::{BufferPool, EvictableBufferPool, FixedBufferPool, PoolGuard, PoolGuards};
use crate::catalog::CatalogTable;
use crate::error::{BlockCorruptionCause, BlockKind, Error, FileKind, Result};
use crate::file::BlockID;
use crate::index::util::Maskable;
use crate::index::{IndexCompareExchange, NonUniqueIndex, RowLocation, UniqueIndex};
use crate::lwc::PersistedLwcBlock;
use crate::row::ops::SelectKey;
use crate::row::{RowID, RowRead};
use crate::trx::TrxID;
use crate::trx::row::RowReadAccess;
use crate::trx::undo::{IndexUndo, IndexUndoKind};

pub(crate) trait IndexRollback {
    type RowPool: BufferPool + 'static;
    type IndexPool: BufferPool + 'static;

    fn mem_table(&self) -> &GenericMemTable<Self::RowPool, Self::IndexPool>;

    fn storage(&self) -> Option<&ColumnStorage>;

    async fn unique_mask_as_deleted(
        &self,
        index_pool_guard: &PoolGuard,
        key: &SelectKey,
        row_id: RowID,
        ts: TrxID,
    ) -> Result<bool>;

    async fn unique_compare_delete(
        &self,
        index_pool_guard: &PoolGuard,
        key: &SelectKey,
        row_id: RowID,
        ignore_del_mask: bool,
        ts: TrxID,
    ) -> Result<bool>;

    async fn unique_compare_exchange(
        &self,
        index_pool_guard: &PoolGuard,
        key: &SelectKey,
        old_row_id: RowID,
        new_row_id: RowID,
        ts: TrxID,
    ) -> Result<IndexCompareExchange>;

    async fn non_unique_mask_as_deleted(
        &self,
        index_pool_guard: &PoolGuard,
        key: &SelectKey,
        row_id: RowID,
        ts: TrxID,
    ) -> Result<bool>;

    async fn non_unique_mask_as_active(
        &self,
        index_pool_guard: &PoolGuard,
        key: &SelectKey,
        row_id: RowID,
        ts: TrxID,
    ) -> Result<bool>;

    async fn non_unique_compare_delete(
        &self,
        index_pool_guard: &PoolGuard,
        key: &SelectKey,
        row_id: RowID,
        ignore_del_mask: bool,
        ts: TrxID,
    ) -> Result<bool>;

    async fn rollback_index_entry<F>(
        &self,
        entry: IndexUndo,
        guards: &PoolGuards,
        ts: TrxID,
        min_active_sts: &mut Option<TrxID>,
        calc_min_active_sts: &mut F,
    ) -> Result<()>
    where
        F: FnMut() -> TrxID,
    {
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
                if !deleted {
                    // The version of previous row is not deleted, means current transaction
                    // must own the lock of old row.
                    // So directly rollback index should be fine.
                    let new_row_id = entry.row_id;
                    let res = self
                        .unique_compare_exchange(index_pool_guard, &key, new_row_id, old_row_id, ts)
                        .await?;
                    debug_assert!(res.is_ok());
                } else {
                    // Roll back a claim over a delete-masked unique owner.
                    // The replacement row may have taken over an index entry
                    // that previously pointed at a deleted hot or cold owner.
                    // If rollback blindly restores that owner after GC has
                    // already proven it obsolete, the index can leak a stale
                    // entry that no later GC pass owns. Re-check the old owner
                    // location and decide whether to restore the masked owner
                    // or delete only the failed replacement claim.
                    match table.find_row(guards, old_row_id, self.storage()).await {
                        RowLocation::NotFound => {
                            // The delete-masked owner may already have been
                            // purged while its stale unique-index entry was
                            // still present. There is no old owner to restore,
                            // so rollback only removes the new claim.
                            let new_row_id = entry.row_id;
                            let res = self
                                .unique_compare_delete(index_pool_guard, &key, new_row_id, true, ts)
                                .await?;
                            assert!(res);
                            return Ok(());
                        }
                        RowLocation::LwcBlock {
                            block_id,
                            row_idx,
                            row_shape_fingerprint,
                        } => {
                            // Rollback of a claimed cold unique owner cannot
                            // latch a row page or inspect a row undo chain.
                            // Use the same cold-delete purgeability rule as
                            // index GC: when no live snapshot can need the
                            // deleted owner, remove only the replacement claim
                            // instead of recreating a masked owner that GC may
                            // already have skipped.
                            let new_row_id = entry.row_id;
                            if self.storage().is_some_and(|storage| {
                                storage
                                    .deletion_buffer()
                                    .delete_marker_is_globally_purgeable_with(old_row_id, || {
                                        match *min_active_sts {
                                            Some(min_active_sts) => min_active_sts,
                                            None => {
                                                let calculated = calc_min_active_sts();
                                                *min_active_sts = Some(calculated);
                                                calculated
                                            }
                                        }
                                    })
                            }) {
                                let res = self
                                    .unique_compare_delete(
                                        index_pool_guard,
                                        &key,
                                        new_row_id,
                                        true,
                                        ts,
                                    )
                                    .await?;
                                assert!(res);
                                return Ok(());
                            }
                            // If the persisted LWC row no longer matches the
                            // key, the unique entry we claimed was stale.
                            // Removing the replacement claim is enough; there
                            // is no matching old owner to restore for this key.
                            if !self
                                .lwc_row_matches_key(
                                    guards,
                                    &key,
                                    block_id,
                                    row_idx,
                                    row_shape_fingerprint,
                                )
                                .await?
                            {
                                let res = self
                                    .unique_compare_delete(
                                        index_pool_guard,
                                        &key,
                                        new_row_id,
                                        true,
                                        ts,
                                    )
                                    .await?;
                                assert!(res);
                                return Ok(());
                            }
                            // Restore the masked old owner first. If this
                            // transaction also recorded a deferred delete,
                            // that undo entry will unmask it next.
                            let old_row_id = old_row_id.deleted();
                            let res = self
                                .unique_compare_exchange(
                                    index_pool_guard,
                                    &key,
                                    new_row_id,
                                    old_row_id,
                                    ts,
                                )
                                .await?;
                            assert!(res.is_ok());
                        }
                        RowLocation::RowPage(page_id) => {
                            let Some(page_guard) =
                                table.get_row_page_shared(guards, page_id).await?
                            else {
                                return Ok(());
                            };
                            // acquire row latch to avoid race condition.
                            let (ctx, page) = page_guard.ctx_and_page();
                            let access = RowReadAccess::new(page, ctx, page.row_idx(old_row_id));
                            if access.row().is_deleted() && !access.any_old_version_exists() {
                                // The old hot owner is now globally invisible:
                                // its page image is deleted and no undo chain
                                // can reconstruct an older visible version.
                                // Rollback removes the failed replacement
                                // claim instead of resurrecting a stale owner.
                                let new_row_id = entry.row_id;
                                let res = self
                                    .unique_compare_delete(
                                        index_pool_guard,
                                        &key,
                                        new_row_id,
                                        true,
                                        ts,
                                    )
                                    .await?;
                                assert!(res);
                            } else {
                                // Some active or future snapshot may still
                                // need the delete-masked hot owner. Restore
                                // that masked owner and let normal defer-delete
                                // undo or GC finish cleanup.
                                let new_row_id = entry.row_id;
                                let res = self
                                    .unique_compare_exchange(
                                        index_pool_guard,
                                        &key,
                                        new_row_id,
                                        old_row_id.deleted(),
                                        ts,
                                    )
                                    .await?;
                                assert!(res.is_ok());
                            }
                        }
                    }
                }
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

    async fn lwc_row_matches_key(
        &self,
        guards: &PoolGuards,
        key: &SelectKey,
        block_id: BlockID,
        row_idx: usize,
        row_shape_fingerprint: u128,
    ) -> Result<bool> {
        let Some(storage) = self.storage() else {
            return Ok(false);
        };
        let table = self.mem_table();
        let block = PersistedLwcBlock::load(
            storage.file().file_kind(),
            storage.file().sparse_file(),
            storage.disk_pool(),
            guards.disk_guard(),
            block_id,
        )
        .await?;
        if block.row_shape_fingerprint() != row_shape_fingerprint {
            return Err(Error::block_corrupted(
                FileKind::TableFile,
                BlockKind::LwcBlock,
                block_id,
                BlockCorruptionCause::InvalidPayload,
            ));
        }
        let read_set = table.metadata().index_specs[key.index_no]
            .index_cols
            .iter()
            .map(|key| key.col_no as usize)
            .collect::<Vec<_>>();
        let vals = block.decode_row_values(table.metadata(), row_idx, &read_set)?;
        Ok(vals.as_slice() == key.vals.as_slice())
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
    fn storage(&self) -> Option<&ColumnStorage> {
        Some(&self.storage)
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
            .unique()
            .ok_or(Error::InvalidArgument)?
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
            .unique()
            .ok_or(Error::InvalidArgument)?
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
            .unique()
            .ok_or(Error::InvalidArgument)?
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
            .non_unique()
            .ok_or(Error::InvalidArgument)?
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
            .non_unique()
            .ok_or(Error::InvalidArgument)?
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
            .non_unique()
            .ok_or(Error::InvalidArgument)?
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
    fn storage(&self) -> Option<&ColumnStorage> {
        None
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
            .ok_or(Error::InvalidArgument)?
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
            .ok_or(Error::InvalidArgument)?
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
            .ok_or(Error::InvalidArgument)?
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
            .ok_or(Error::InvalidArgument)?
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
            .ok_or(Error::InvalidArgument)?
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
            .ok_or(Error::InvalidArgument)?
            .compare_delete(index_pool_guard, &key.vals, row_id, ignore_del_mask, ts)
            .await
    }
}
