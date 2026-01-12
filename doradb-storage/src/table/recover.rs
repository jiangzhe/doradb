use crate::buffer::page::PageID;
use crate::index::{IndexInsert, NonUniqueIndex, UniqueIndex};
use crate::latch::LatchFallbackMode;
use crate::row::ops::{ReadRow, SelectKey, UpdateCol};
use crate::row::{RowID, RowPage, RowRead};
use crate::table::{
    RecoverIndex, Table, index_key_is_changed, index_key_replace, read_latest_index_key,
};
use crate::trx::MIN_SNAPSHOT_TS;
use crate::trx::TrxID;
use crate::trx::row::ReadAllRows;
use crate::value::Val;
use std::collections::HashMap;
use std::future::Future;

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
    fn populate_index_via_row_page(
        &self,
        page_id: PageID,
    ) -> impl Future<Output = ()>;
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
        let mut page_guard = self.mem_pool
            .get_page::<RowPage>(page_id, LatchFallbackMode::Exclusive)
            .await
            .exclusive_async()
            .await;

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
        let mut page_guard = self.mem_pool
            .get_page::<RowPage>(page_id, LatchFallbackMode::Exclusive)
            .await
            .exclusive_async()
            .await;

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
                let page_guard = self.mem_pool
                    .get_page::<RowPage>(page_id, LatchFallbackMode::Shared)
                    .await
                    .shared_async()
                    .await;

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
        let mut page_guard = self.mem_pool
            .get_page::<RowPage>(page_id, LatchFallbackMode::Exclusive)
            .await
            .exclusive_async()
            .await;

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

    async fn populate_index_via_row_page(
        &self,
        page_id: PageID,
    ) {
        let page_guard = self.mem_pool
            .get_page::<RowPage>(page_id, LatchFallbackMode::Shared)
            .await
            .shared_async()
            .await;
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
                            debug_assert!(matches!(res, IndexInsert::Ok(_)));
                        } else {
                            let index = sec_idx.non_unique().unwrap();
                            let res = index
                                .insert_if_not_exists(&vals, row_id, false, MIN_SNAPSHOT_TS)
                                .await;
                            debug_assert!(matches!(res, IndexInsert::Ok(_)));
                        }
                    }
                    ReadRow::NotFound => (),
                    ReadRow::InvalidIndex => unreachable!(),
                }
            }
        }
    }
}
