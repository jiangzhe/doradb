use crate::buffer::BufferPool;
use crate::buffer::guard::PageGuard;
use crate::buffer::page::{Page, PageID};
use crate::error::{Error, Result};
use crate::index::{
    ColumnBlockIndex, IndexInsert, NonUniqueIndex, UniqueIndex, load_payload_deletion_deltas,
};
use crate::latch::LatchFallbackMode;
use crate::lwc::{LwcData, LwcPage};
use crate::row::ops::{ReadRow, SelectKey, UpdateCol};
use crate::row::{RowID, RowPage, RowRead};
use crate::table::{
    RecoverIndex, Table, index_key_is_changed, index_key_replace, read_latest_index_key,
};
use crate::trx::MIN_SNAPSHOT_TS;
use crate::trx::TrxID;
use crate::trx::row::ReadAllRows;
use crate::value::{Val, ValKind};
use std::collections::HashMap;
use std::future::Future;
use std::mem;

pub trait TableRecover {
    /// Recover row insert from redo log.
    fn recover_row_insert(
        &self,
        page_id: PageID,
        row_id: RowID,
        cols: &[Val],
        cts: TrxID,
        disable_index: bool,
    ) -> impl Future<Output = ()>;

    /// Recover row update from redo log.
    fn recover_row_update(
        &self,
        page_id: PageID,
        row_id: RowID,
        update: &[UpdateCol],
        cts: TrxID,
        disable_index: bool,
    ) -> impl Future<Output = ()>;

    /// Recover row delete from redo log.
    fn recover_row_delete(
        &self,
        page_id: PageID,
        row_id: RowID,
        cts: TrxID,
        disable_index: bool,
    ) -> impl Future<Output = ()>;

    /// Populate index using data on row page.
    fn populate_index_via_row_page(&self, page_id: PageID) -> impl Future<Output = Result<()>>;

    /// Populate indexes using persisted LWC pages below the current pivot boundary.
    fn populate_index_via_persisted_data(&self) -> impl Future<Output = Result<()>>;
}

impl TableRecover for Table {
    async fn recover_row_insert(
        &self,
        page_id: PageID,
        row_id: RowID,
        cols: &[Val],
        cts: TrxID,
        disable_index: bool,
    ) {
        debug_assert!(cols.len() == self.metadata().col_count());
        debug_assert!({
            cols.iter()
                .enumerate()
                .all(|(idx, val)| self.metadata().col_type_match(idx, val))
        });
        // Since we always dispatch rows of one page to same thread,
        // we can just hold exclusive lock on this page and process all rows in it.
        let mut page_guard = self
            .mem_pool
            .get_page::<RowPage>(page_id, LatchFallbackMode::Exclusive)
            .await
            .lock_exclusive_async()
            .await
            .unwrap();

        let res = self.recover_row_insert_to_page(&mut page_guard, row_id, cols, cts);
        assert!(res.is_ok());
        page_guard.set_dirty(); // mark as dirty page.

        if !disable_index {
            let keys = self.metadata().keys_for_insert(cols);
            for key in keys {
                match self.recover_index_insert(key, row_id, cts).await {
                    RecoverIndex::Ok | RecoverIndex::InsertOutdated => (),
                    RecoverIndex::DeleteOutdated => unreachable!(),
                }
            }
        }
    }

    async fn recover_row_update(
        &self,
        page_id: PageID,
        row_id: RowID,
        update: &[UpdateCol],
        cts: TrxID,
        disable_index: bool,
    ) {
        let mut page_guard = self
            .mem_pool
            .get_page::<RowPage>(page_id, LatchFallbackMode::Exclusive)
            .await
            .lock_exclusive_async()
            .await
            .unwrap();

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
                let page_guard = self
                    .mem_pool
                    .get_page::<RowPage>(page_id, LatchFallbackMode::Shared)
                    .await
                    .lock_shared_async()
                    .await
                    .unwrap();

                let metadata = self.metadata();
                for (index, index_schema) in self.sec_idx.iter().zip(&metadata.index_specs) {
                    debug_assert!(index.is_unique() == index_schema.unique());
                    if index_key_is_changed(index_schema, &index_change_cols) {
                        let new_key =
                            read_latest_index_key(metadata, index.index_no, &page_guard, row_id);
                        let old_key = index_key_replace(index_schema, &new_key, &index_change_cols);
                        // insert new index entry.
                        match self.recover_index_insert(new_key, row_id, cts).await {
                            RecoverIndex::Ok | RecoverIndex::InsertOutdated => (),
                            RecoverIndex::DeleteOutdated => unreachable!(),
                        }
                        // delete old index entry.
                        match self.recover_index_delete(old_key, row_id).await {
                            RecoverIndex::Ok | RecoverIndex::DeleteOutdated => (),
                            RecoverIndex::InsertOutdated => unreachable!(),
                        }
                    }
                }
            }
        }
    }

    async fn recover_row_delete(
        &self,
        page_id: PageID,
        row_id: RowID,
        cts: TrxID,
        disable_index: bool,
    ) {
        let mut page_guard = self
            .mem_pool
            .get_page::<RowPage>(page_id, LatchFallbackMode::Exclusive)
            .await
            .lock_exclusive_async()
            .await
            .unwrap();

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
            for (index, index_schema) in self.sec_idx.iter().zip(&self.metadata().index_specs) {
                debug_assert!(index.is_unique() == index_schema.unique());
                let vals: Vec<Val> = index_schema
                    .index_cols
                    .iter()
                    .map(|ik| index_cols[&(ik.col_no as usize)].clone())
                    .collect();
                let key = SelectKey::new(index.index_no, vals);
                match self.recover_index_delete(key, row_id).await {
                    RecoverIndex::Ok | RecoverIndex::DeleteOutdated => (),
                    RecoverIndex::InsertOutdated => unreachable!(),
                }
            }
        }
    }

    async fn populate_index_via_row_page(&self, page_id: PageID) -> Result<()> {
        let page_guard = self
            .mem_pool
            .get_page::<RowPage>(page_id, LatchFallbackMode::Shared)
            .await
            .lock_shared_async()
            .await
            .unwrap();
        let metadata = self.metadata();
        let (ctx, page) = page_guard.ctx_and_page();
        for (index_spec, sec_idx) in metadata.index_specs.iter().zip(&*self.sec_idx) {
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
                            let index = sec_idx.unique().unwrap();
                            let res = index
                                .insert_if_not_exists(&vals, row_id, false, MIN_SNAPSHOT_TS)
                                .await;
                            ensure_recovery_index_insert(sec_idx.index_no, res)?;
                        } else {
                            let index = sec_idx.non_unique().unwrap();
                            let res = index
                                .insert_if_not_exists(&vals, row_id, false, MIN_SNAPSHOT_TS)
                                .await;
                            ensure_recovery_index_insert(sec_idx.index_no, res)?;
                        }
                    }
                    ReadRow::NotFound => (),
                    ReadRow::InvalidIndex => unreachable!(),
                }
            }
        }
        Ok(())
    }

    async fn populate_index_via_persisted_data(&self) -> Result<()> {
        if self.sec_idx.is_empty() {
            return Ok(());
        }
        let active_root = self.file.active_root();
        if active_root.column_block_index_root == 0 || active_root.pivot_row_id == 0 {
            return Ok(());
        }

        let index = ColumnBlockIndex::new(
            active_root.column_block_index_root,
            active_root.pivot_row_id,
            &self.disk_pool,
        );
        for (start_row_id, payload) in index.collect_leaf_entries().await? {
            let deleted = load_payload_deletion_deltas(&index, payload).await?;
            let page_guard = self
                .disk_pool
                .try_get_page_shared::<Page>(payload.block_id)
                .await?;
            let page = LwcPage::try_from_bytes(page_guard.page())?;
            let row_ids = decode_lwc_row_ids(page)?;
            if row_ids.len() != page.header.row_count() as usize {
                return Err(Error::InvalidCompressedData);
            }

            for (row_idx, row_id) in row_ids.into_iter().enumerate() {
                let delta = row_id
                    .checked_sub(start_row_id)
                    .ok_or(Error::InvalidState)?;
                if delta > u32::MAX as u64 {
                    return Err(Error::InvalidState);
                }
                if deleted.contains(&(delta as u32)) {
                    continue;
                }

                let mut vals = Vec::with_capacity(self.metadata().col_count());
                for col_idx in 0..self.metadata().col_count() {
                    let column = page.column(self.metadata(), col_idx)?;
                    if column.is_null(row_idx) {
                        vals.push(Val::Null);
                        continue;
                    }
                    let data = column.data()?;
                    let val = data.value(row_idx).ok_or(Error::InvalidCompressedData)?;
                    vals.push(val);
                }

                for key in self.metadata().keys_for_insert(&vals) {
                    if self.metadata().index_specs[key.index_no].unique() {
                        let res = self.sec_idx[key.index_no]
                            .unique()
                            .unwrap()
                            .insert_if_not_exists(&key.vals, row_id, false, MIN_SNAPSHOT_TS)
                            .await;
                        ensure_recovery_index_insert(key.index_no, res)?;
                    } else {
                        let res = self.sec_idx[key.index_no]
                            .non_unique()
                            .unwrap()
                            .insert_if_not_exists(&key.vals, row_id, false, MIN_SNAPSHOT_TS)
                            .await;
                        ensure_recovery_index_insert(key.index_no, res)?;
                    }
                }
            }
        }
        Ok(())
    }
}

#[inline]
fn ensure_recovery_index_insert(index_no: usize, res: IndexInsert) -> Result<()> {
    match res {
        IndexInsert::Ok(_) => Ok(()),
        IndexInsert::DuplicateKey(row_id, deleted) => Err(Error::UnexpectedRecoveryDuplicateKey {
            index_no,
            row_id,
            deleted,
        }),
    }
}

fn decode_lwc_row_ids(page: &LwcPage) -> Result<Vec<RowID>> {
    let row_count = page.header.row_count() as usize;
    let col_count = page.header.col_count() as usize;
    let start_idx = col_count * mem::size_of::<u16>();
    let end_idx = page.header.first_col_offset() as usize;
    if end_idx > page.body.len() || start_idx > end_idx {
        return Err(Error::InvalidCompressedData);
    }
    let row_id_data = LwcData::from_bytes(ValKind::U64, &page.body[start_idx..end_idx])?;
    let mut row_ids = Vec::with_capacity(row_count);
    for row_idx in 0..row_count {
        let row_id = row_id_data
            .value(row_idx)
            .and_then(|v| v.as_u64())
            .ok_or(Error::InvalidCompressedData)?;
        row_ids.push(row_id);
    }
    Ok(row_ids)
}

#[cfg(test)]
mod tests {
    use super::ensure_recovery_index_insert;
    use crate::error::Error;
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
        match err {
            Error::UnexpectedRecoveryDuplicateKey {
                index_no,
                row_id,
                deleted,
            } => {
                assert_eq!(index_no, 3);
                assert_eq!(row_id, 42);
                assert!(!deleted);
            }
            _ => panic!("unexpected error: {err:?}"),
        }
    }
}
