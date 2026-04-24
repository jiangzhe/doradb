use crate::buffer::PageID;
use crate::buffer::PoolGuards;
use crate::error::{DataIntegrityError, Error, RecoveryDuplicateKey, Result};
use crate::index::IndexInsert;
use crate::index::{NonUniqueIndex, UniqueIndex};
use crate::row::ops::{ReadRow, SelectKey, UpdateCol};
use crate::row::{RowID, RowRead};
use crate::table::{
    DeletionError, RecoverIndex, Table, index_key_is_changed, index_key_replace,
    read_latest_index_key,
};
use crate::trx::MIN_SNAPSHOT_TS;
use crate::trx::TrxID;
use crate::trx::row::ReadAllRows;
use crate::value::Val;
use error_stack::Report;
use std::collections::HashMap;
use std::future::Future;

/// Redo-recovery hooks implemented by table runtimes.
pub trait TableRecover {
    /// Recover row insert from redo log.
    fn recover_row_insert(
        &self,
        guards: &PoolGuards,
        page_id: PageID,
        row_id: RowID,
        cols: &[Val],
        cts: TrxID,
        disable_index: bool,
    ) -> impl Future<Output = Result<()>>;

    /// Recover row update from redo log.
    fn recover_row_update(
        &self,
        guards: &PoolGuards,
        page_id: PageID,
        row_id: RowID,
        update: &[UpdateCol],
        cts: TrxID,
        disable_index: bool,
    ) -> impl Future<Output = Result<()>>;

    /// Recover row delete from redo log.
    fn recover_row_delete(
        &self,
        guards: &PoolGuards,
        page_id: PageID,
        row_id: RowID,
        cts: TrxID,
        disable_index: bool,
    ) -> impl Future<Output = Result<()>>;

    /// Populate index using data on row page.
    fn populate_index_via_row_page(
        &self,
        guards: &PoolGuards,
        page_id: PageID,
    ) -> impl Future<Output = Result<()>>;
}

impl TableRecover for Table {
    async fn recover_row_insert(
        &self,
        guards: &PoolGuards,
        page_id: PageID,
        row_id: RowID,
        cols: &[Val],
        cts: TrxID,
        disable_index: bool,
    ) -> Result<()> {
        debug_assert!(cols.len() == self.metadata().col_count());
        debug_assert!({
            cols.iter()
                .enumerate()
                .all(|(idx, val)| self.metadata().col_type_match(idx, val))
        });
        // Since we always dispatch rows of one page to same thread,
        // we can just hold exclusive lock on this page and process all rows in it.
        let mut page_guard = self.must_get_row_page_exclusive(guards, page_id).await?;

        let res = self.recover_row_insert_to_page(&mut page_guard, row_id, cols, cts);
        assert!(res.is_ok());
        page_guard.set_dirty(); // mark as dirty page.

        if !disable_index {
            let keys = self.metadata().keys_for_insert(cols);
            for key in keys {
                match self.recover_index_insert(guards, key, row_id, cts).await? {
                    RecoverIndex::Ok | RecoverIndex::InsertOutdated => (),
                    RecoverIndex::DeleteOutdated => unreachable!(),
                }
            }
        }
        Ok(())
    }

    async fn recover_row_update(
        &self,
        guards: &PoolGuards,
        page_id: PageID,
        row_id: RowID,
        update: &[UpdateCol],
        cts: TrxID,
        disable_index: bool,
    ) -> Result<()> {
        let mut page_guard = self.must_get_row_page_exclusive(guards, page_id).await?;

        if disable_index {
            let res = self.recover_row_update_to_page(&mut page_guard, row_id, update, cts, None);
            assert!(res.is_ok());
            page_guard.set_dirty(); // mark as dirty page.
        } else {
            let mut index_change_cols = HashMap::new();
            let res = self.recover_row_update_to_page(
                &mut page_guard,
                row_id,
                update,
                cts,
                Some(&mut index_change_cols),
            );
            assert!(res.is_ok());
            page_guard.set_dirty(); // mark as dirty page.

            if !index_change_cols.is_empty() {
                // There is index change, we need to update index.
                let page_guard = self.must_get_row_page_shared(guards, page_id).await?;

                let metadata = self.metadata();
                for (index_no, index_schema) in metadata.index_specs.iter().enumerate() {
                    debug_assert_eq!(self.sec_idx()[index_no].is_unique(), index_schema.unique());
                    if index_key_is_changed(index_schema, &index_change_cols) {
                        let new_key =
                            read_latest_index_key(metadata, index_no, &page_guard, row_id);
                        let old_key = index_key_replace(index_schema, &new_key, &index_change_cols);
                        // insert new index entry.
                        match self
                            .recover_index_insert(guards, new_key, row_id, cts)
                            .await?
                        {
                            RecoverIndex::Ok | RecoverIndex::InsertOutdated => (),
                            RecoverIndex::DeleteOutdated => unreachable!(),
                        }
                        // delete old index entry.
                        match self.recover_index_delete(guards, old_key, row_id).await? {
                            RecoverIndex::Ok | RecoverIndex::DeleteOutdated => (),
                            RecoverIndex::InsertOutdated => unreachable!(),
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn recover_row_delete(
        &self,
        guards: &PoolGuards,
        page_id: PageID,
        row_id: RowID,
        cts: TrxID,
        disable_index: bool,
    ) -> Result<()> {
        // `recovery_bootstrap_unchecked`: restart recovery runs without
        // surviving user transactions, so it binds the current loaded root
        // directly for cold-row delete replay predicates.
        let active_root = self.file().active_root_unchecked();
        if row_id < active_root.pivot_row_id {
            if cts < active_root.deletion_cutoff_ts {
                return Ok(());
            }
            if !disable_index {
                return Err(Error::not_supported(
                    "cold-row delete recovery requires deferred index rebuild",
                ));
            }
            match self.deletion_buffer().put_committed(row_id, cts) {
                Ok(()) => return Ok(()),
                Err(DeletionError::AlreadyDeleted | DeletionError::WriteConflict) => {
                    return Err(Error::invalid_state());
                }
            }
        }

        let mut page_guard = self.must_get_row_page_exclusive(guards, page_id).await?;

        if disable_index {
            let res = self.recover_row_delete_to_page(&mut page_guard, row_id, cts, None);
            assert!(res.is_ok());
            page_guard.set_dirty(); // mark as dirty page.
        } else {
            let mut index_cols = HashMap::new();
            let res = self.recover_row_delete_to_page(
                &mut page_guard,
                row_id,
                cts,
                Some(&mut index_cols),
            );
            assert!(res.is_ok());
            page_guard.set_dirty(); // mark as dirty page.
            for (index_no, index_schema) in self.metadata().index_specs.iter().enumerate() {
                debug_assert_eq!(self.sec_idx()[index_no].is_unique(), index_schema.unique());
                let vals: Vec<Val> = index_schema
                    .index_cols
                    .iter()
                    .map(|ik| index_cols[&(ik.col_no as usize)].clone())
                    .collect();
                let key = SelectKey::new(index_no, vals);
                match self.recover_index_delete(guards, key, row_id).await? {
                    RecoverIndex::Ok | RecoverIndex::DeleteOutdated => (),
                    RecoverIndex::InsertOutdated => unreachable!(),
                }
            }
        }
        Ok(())
    }

    async fn populate_index_via_row_page(
        &self,
        guards: &PoolGuards,
        page_id: PageID,
    ) -> Result<()> {
        let page_guard = self.must_get_row_page_shared(guards, page_id).await?;
        let metadata = self.metadata();
        let index_pool_guard = self.index_pool_guard(guards);
        let (ctx, page) = page_guard.ctx_and_page();
        for (index_no, index_spec) in metadata.index_specs.iter().enumerate() {
            let sec_idx = &self.sec_idx()[index_no];
            let read_set: Vec<_> = index_spec
                .index_cols
                .iter()
                .map(|c| c.col_no as usize)
                .collect();
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
    use crate::index::IndexInsert;

    #[test]
    fn test_ensure_recovery_index_insert_accepts_ok_variants() {
        assert!(ensure_recovery_index_insert(1, IndexInsert::Ok(false)).is_ok());
        assert!(ensure_recovery_index_insert(1, IndexInsert::Ok(true)).is_ok());
    }

    #[test]
    fn test_ensure_recovery_index_insert_rejects_duplicate_key() {
        let err =
            ensure_recovery_index_insert(3, IndexInsert::DuplicateKey(42, false)).unwrap_err();
        let duplicate = err
            .downcast_ref::<crate::error::RecoveryDuplicateKey>()
            .unwrap_or_else(|| panic!("unexpected error: {err:?}"));
        assert_eq!(duplicate.index_no, 3);
        assert_eq!(duplicate.row_id, 42);
        assert!(!duplicate.deleted);
    }
}
