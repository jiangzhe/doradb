use crate::buffer::PoolGuards;
use crate::error::{DataIntegrityError, RecoveryDuplicateKey, Result};
use crate::id::{PageID, RowID, TrxID};
use crate::index::IndexInsert;
use crate::index::{NonUniqueIndex, UniqueIndex};
use crate::row::RowRead;
use crate::row::ops::{ReadRow, UpdateCol};
use crate::table::{DeletionError, Table};
use crate::trx::MIN_SNAPSHOT_TS;
use crate::trx::row::ReadAllRows;
use crate::value::Val;
use error_stack::Report;

impl Table {
    /// Recover row insert from redo log.
    pub(crate) async fn recover_row_insert(
        &self,
        guards: &PoolGuards,
        page_id: PageID,
        row_id: RowID,
        cols: &[Val],
        cts: TrxID,
    ) -> Result<()> {
        let layout = self.layout_snapshot();
        let metadata = layout.metadata();
        debug_assert!(cols.len() == metadata.col.col_count());
        debug_assert!({
            cols.iter()
                .enumerate()
                .all(|(idx, val)| metadata.col.col_type_match(idx, val))
        });
        // Since we always dispatch rows of one page to same thread,
        // we can just hold exclusive lock on this page and process all rows in it.
        let mut page_guard = self
            .mem
            .must_get_row_page_exclusive(guards, page_id)
            .await?;

        self.recover_row_insert_to_page(metadata, &mut page_guard, row_id, cols, cts)?;
        page_guard.set_dirty(); // mark as dirty page.
        Ok(())
    }

    /// Recover row update from redo log.
    pub(crate) async fn recover_row_update(
        &self,
        guards: &PoolGuards,
        page_id: PageID,
        row_id: RowID,
        update: &[UpdateCol],
        cts: TrxID,
    ) -> Result<()> {
        let layout = self.layout_snapshot();
        let metadata = layout.metadata();
        let mut page_guard = self
            .mem
            .must_get_row_page_exclusive(guards, page_id)
            .await?;

        self.recover_row_update_to_page(metadata, &mut page_guard, row_id, update, cts)?;
        page_guard.set_dirty(); // mark as dirty page.
        Ok(())
    }

    /// Recover row delete from redo log.
    pub(crate) async fn recover_row_delete(
        &self,
        guards: &PoolGuards,
        page_id: PageID,
        row_id: RowID,
        cts: TrxID,
    ) -> Result<()> {
        // `recovery_bootstrap_unchecked`: restart recovery runs without
        // surviving user transactions, so it binds the current loaded root
        // directly for cold-row delete replay predicates.
        let active_root = self.file().active_root_unchecked();
        if row_id < active_root.pivot_row_id {
            if cts < active_root.deletion_cutoff_ts {
                return Ok(());
            }
            match self.deletion_buffer().put_committed(row_id, cts) {
                Ok(()) => return Ok(()),
                Err(DeletionError::AlreadyDeleted | DeletionError::WriteConflict) => {
                    return Err(Report::new(DataIntegrityError::InvalidRootInvariant)
                        .attach(format!("row_id={row_id}, cts={cts}"))
                        .into());
                }
            }
        }

        let mut page_guard = self
            .mem
            .must_get_row_page_exclusive(guards, page_id)
            .await?;

        self.recover_row_delete_to_page(&mut page_guard, row_id, cts)?;
        page_guard.set_dirty(); // mark as dirty page.
        Ok(())
    }

    /// Populate index using data on row page.
    pub(crate) async fn populate_index_via_row_page(
        &self,
        guards: &PoolGuards,
        page_id: PageID,
    ) -> Result<()> {
        let page_guard = self.mem.must_get_row_page_shared(guards, page_id).await?;
        let layout = self.layout_snapshot();
        let metadata = layout.metadata();
        let index_pool_guard = self.mem.index_pool_guard(guards)?;
        let (ctx, page) = page_guard.ctx_and_page();
        for (index_no, index_spec) in metadata.idx.active_indexes() {
            let sec_idx = layout.secondary_index(index_no)?;
            let read_set: Vec<_> = index_spec.cols.iter().map(|c| c.col_no as usize).collect();
            for row_access in ReadAllRows::new(page, ctx) {
                let row_id = row_access.row().row_id();
                match row_access.read_row_latest(metadata, &read_set, None) {
                    ReadRow::Ok(vals) => {
                        if index_spec.unique() {
                            let index = sec_idx.unique_mem()?;
                            let res = index
                                .insert_if_not_exists(
                                    index_pool_guard,
                                    &vals,
                                    row_id,
                                    false,
                                    MIN_SNAPSHOT_TS,
                                )
                                .await?;
                            ensure_recovery_index_insert(sec_idx.index_no(), res)?;
                        } else {
                            let index = sec_idx.non_unique_mem()?;
                            let res = index
                                .insert_if_not_exists(
                                    index_pool_guard,
                                    &vals,
                                    row_id,
                                    false,
                                    MIN_SNAPSHOT_TS,
                                )
                                .await?;
                            ensure_recovery_index_insert(sec_idx.index_no(), res)?;
                        }
                    }
                    ReadRow::NotFound => (),
                    ReadRow::InvalidIndex => unreachable!(),
                }
            }
        }
        Ok(())
    }
}

/// Reject duplicate secondary-index entries during recovery rebuild.
#[inline]
pub(super) fn ensure_recovery_index_insert(index_no: usize, res: IndexInsert) -> Result<()> {
    match res {
        IndexInsert::Ok(_) => Ok(()),
        IndexInsert::DuplicateKey(row_id, deleted) => Err(Report::new(
            DataIntegrityError::UnexpectedRecoveryDuplicateKey,
        )
        .attach(RecoveryDuplicateKey {
            index_no,
            row_id,
            deleted,
        })
        .into()),
    }
}

#[cfg(test)]
mod tests {
    use super::ensure_recovery_index_insert;
    use crate::id::RowID;
    use crate::index::IndexInsert;

    #[test]
    fn test_ensure_recovery_index_insert_accepts_ok_variants() {
        assert!(ensure_recovery_index_insert(1, IndexInsert::Ok(false)).is_ok());
        assert!(ensure_recovery_index_insert(1, IndexInsert::Ok(true)).is_ok());
    }

    #[test]
    fn test_ensure_recovery_index_insert_rejects_duplicate_key() {
        let err = ensure_recovery_index_insert(3, IndexInsert::DuplicateKey(RowID::new(42), false))
            .unwrap_err();
        let duplicate = err
            .downcast_ref::<crate::error::RecoveryDuplicateKey>()
            .unwrap_or_else(|| panic!("unexpected error: {err:?}"));
        assert_eq!(duplicate.index_no, 3);
        assert_eq!(duplicate.row_id, RowID::new(42));
        assert!(!duplicate.deleted);
    }
}
