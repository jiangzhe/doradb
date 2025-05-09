// pub mod schema;
#[cfg(test)]
mod tests;

use crate::buffer::guard::PageSharedGuard;
use crate::buffer::page::PageID;
use crate::buffer::BufferPool;
use crate::catalog::TableMetadata;
use crate::index::{BlockIndex, RowLocation, SecondaryIndex, UniqueIndex};
use crate::latch::LatchFallbackMode;
use crate::row::ops::{
    DeleteMvcc, InsertIndex, InsertMvcc, LinkForUniqueIndex, ReadRow, SelectKey, SelectMvcc,
    UndoCol, UpdateCol, UpdateIndex, UpdateMvcc, UpdateRow,
};
use crate::row::{estimate_max_row_count, var_len_for_insert, Row, RowID, RowPage, RowRead};
use crate::stmt::Statement;
use crate::trx::redo::{RowRedo, RowRedoKind};
use crate::trx::row::{FindOldVersion, LockRowForWrite, LockUndo};
use crate::trx::undo::{
    IndexUndo, IndexUndoKind, MainBranch, NextRowUndo, RowUndoKind, UndoStatus,
};
use crate::value::{Val, PAGE_VAR_LEN_INLINE};
use doradb_catalog::IndexSpec;
use std::collections::HashSet;
use std::mem;
use std::sync::Arc;

pub use doradb_catalog::TableID;

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
pub struct Table<P: BufferPool> {
    pub metadata: Arc<TableMetadata>,
    pub blk_idx: Arc<BlockIndex<P>>,
    pub sec_idx: Arc<[SecondaryIndex]>,
}

impl<P: BufferPool> Table<P> {
    /// Create a new table.
    #[inline]
    pub fn new(blk_idx: BlockIndex<P>, metadata: TableMetadata) -> Self {
        let sec_idx: Vec<_> = metadata
            .index_specs
            .iter()
            .enumerate()
            .map(|(index_no, index_spec)| {
                let ty_infer = |col_no: usize| metadata.user_col_type(col_no);
                SecondaryIndex::new(index_no, index_spec, ty_infer)
            })
            .collect();
        Table {
            metadata: Arc::new(metadata),
            blk_idx: Arc::new(blk_idx),
            sec_idx: Arc::from(sec_idx.into_boxed_slice()),
        }
    }

    #[inline]
    pub fn table_id(&self) -> TableID {
        self.blk_idx.table_id
    }

    #[inline]
    pub async fn scan_rows(&self) {
        todo!()
    }

    /// Select row with unique index with MVCC.
    /// Result should be no more than one row.
    #[inline]
    pub async fn select_row_mvcc(
        &self,
        buf_pool: &'static P,
        stmt: &Statement<P>,
        key: &SelectKey,
        user_read_set: &[usize],
    ) -> SelectMvcc {
        debug_assert!(key.index_no < self.sec_idx.len());
        debug_assert!(self.metadata.index_specs[key.index_no].unique());
        debug_assert!(self.metadata.index_layout_match(key.index_no, &key.vals));
        debug_assert!({
            !user_read_set.is_empty()
                && user_read_set
                    .iter()
                    .zip(user_read_set.iter().skip(1))
                    .all(|(l, r)| l < r)
        });
        let (page_guard, row_id) = loop {
            match self.sec_idx[key.index_no]
                .unique()
                .unwrap()
                .lookup(&key.vals)
            {
                None => return SelectMvcc::NotFound,
                Some(row_id) => match self.blk_idx.find_row_id(buf_pool, row_id).await {
                    RowLocation::NotFound => return SelectMvcc::NotFound,
                    RowLocation::ColSegment(..) => todo!(),
                    RowLocation::RowPage(page_id) => {
                        let page_guard = buf_pool
                            .get_page::<RowPage>(page_id, LatchFallbackMode::Shared)
                            .await
                            .shared_async()
                            .await;
                        if validate_page_id(&page_guard, page_id) {
                            break (page_guard, row_id);
                        }
                    }
                },
            }
        };
        let page = page_guard.page();
        if !page.row_id_in_valid_range(row_id) {
            return SelectMvcc::NotFound;
        }
        // MVCC read does not require row lock.
        let access = page_guard.read_row_by_id(row_id);
        match access.read_row_mvcc(&stmt.trx, &self.metadata, user_read_set, key) {
            ReadRow::Ok(vals) => SelectMvcc::Ok(vals),
            ReadRow::InvalidIndex | ReadRow::NotFound => SelectMvcc::NotFound,
        }
    }

    /// Select row with unique index in uncommitted mode.
    #[inline]
    pub async fn select_row_uncommitted<R, F>(
        &self,
        buf_pool: &'static P,
        key: &SelectKey,
        row_action: F,
    ) -> Option<R>
    where
        for<'a> F: FnOnce(Row<'a>) -> R,
    {
        debug_assert!(key.index_no < self.sec_idx.len());
        debug_assert!(self.metadata.index_specs[key.index_no].unique());
        debug_assert!(self.metadata.index_layout_match(key.index_no, &key.vals));
        let (page_guard, row_id) = loop {
            match self.sec_idx[key.index_no]
                .unique()
                .unwrap()
                .lookup(&key.vals)
            {
                None => return None,
                Some(row_id) => match self.blk_idx.find_row_id(buf_pool, row_id).await {
                    RowLocation::NotFound => return None,
                    RowLocation::ColSegment(..) => todo!(),
                    RowLocation::RowPage(page_id) => {
                        let page_guard = buf_pool
                            .get_page::<RowPage>(page_id, LatchFallbackMode::Shared)
                            .await
                            .shared_async()
                            .await;
                        if validate_page_id(&page_guard, page_id) {
                            break (page_guard, row_id);
                        }
                    }
                },
            }
        };
        let page = page_guard.page();
        if !page.row_id_in_valid_range(row_id) {
            return None;
        }
        let access = page_guard.read_row_by_id(row_id);
        let row = access.row();
        // latest version in row page.
        if row.is_deleted() {
            return None;
        }
        if row.is_key_different(&self.metadata, key) {
            return None;
        }
        Some(row_action(row))
    }

    /// Insert row with MVCC.
    /// This method will also take care of index update.
    #[inline]
    pub async fn insert_row(
        &self,
        buf_pool: &'static P,
        stmt: &mut Statement<P>,
        cols: Vec<Val>,
    ) -> InsertMvcc {
        debug_assert!(cols.len() + 1 == self.metadata.col_count());
        debug_assert!({
            cols.iter()
                .enumerate()
                .all(|(idx, val)| self.metadata.user_col_type_match(idx, val))
        });
        let keys = self.metadata.keys_for_insert(&cols);
        // insert row into page with undo log linked.
        let (row_id, page_guard) = self
            .insert_row_internal(buf_pool, stmt, cols, RowUndoKind::Insert, None)
            .await;
        // insert index
        for key in keys {
            if self.metadata.index_specs[key.index_no].unique() {
                let res = self
                    .insert_unique_index(buf_pool, stmt, key, row_id, &page_guard)
                    .await;
                match res {
                    InsertIndex::Ok => (),
                    InsertIndex::DuplicateKey => {
                        return InsertMvcc::DuplicateKey;
                    }
                    InsertIndex::WriteConflict => {
                        return InsertMvcc::WriteConflict;
                    }
                }
            } else {
                todo!()
            }
        }
        page_guard.set_dirty(); // mark as dirty page.
        InsertMvcc::Ok(row_id)
    }

    /// Update row with MVCC.
    /// This method is for update based on unique index lookup.
    /// It also takes care of index change.
    #[inline]
    pub async fn update_row(
        &self,
        buf_pool: &'static P,
        stmt: &mut Statement<P>,
        key: &SelectKey,
        update: Vec<UpdateCol>,
    ) -> UpdateMvcc {
        debug_assert!(key.index_no < self.sec_idx.len());
        debug_assert!(self.metadata.index_specs[key.index_no].unique());
        debug_assert!(self.metadata.index_layout_match(key.index_no, &key.vals));
        let index = self.sec_idx[key.index_no].unique().unwrap();
        let (page_guard, row_id) = loop {
            match index.lookup(&key.vals) {
                None => return UpdateMvcc::NotFound,
                Some(row_id) => match self.blk_idx.find_row_id(buf_pool, row_id).await {
                    RowLocation::NotFound => return UpdateMvcc::NotFound,
                    RowLocation::ColSegment(..) => todo!(),
                    RowLocation::RowPage(page_id) => {
                        let page_guard = buf_pool
                            .get_page::<RowPage>(page_id, LatchFallbackMode::Shared)
                            .await
                            .shared_async()
                            .await;
                        if validate_page_id(&page_guard, page_id) {
                            break (page_guard, row_id);
                        }
                    }
                },
            }
        };
        let res = self
            .update_row_inplace(stmt, page_guard, key, row_id, update)
            .await;
        match res {
            UpdateRowInplace::Ok(new_row_id, index_change_cols, page_guard) => {
                debug_assert!(row_id == new_row_id);
                if !index_change_cols.is_empty() {
                    // Index may change, we should check whether each index key change and update correspondingly.
                    let res = self
                        .update_indexes_only_key_change(
                            buf_pool,
                            stmt,
                            row_id,
                            &page_guard,
                            &index_change_cols,
                        )
                        .await;
                    page_guard.set_dirty(); // mark as dirty page.
                    return match res {
                        UpdateIndex::Ok => UpdateMvcc::Ok(new_row_id),
                        UpdateIndex::DuplicateKey => UpdateMvcc::DuplicateKey,
                        UpdateIndex::WriteConflict => UpdateMvcc::WriteConflict,
                    };
                } // otherwise, do nothing
                page_guard.set_dirty(); // mark as dirty page.
                UpdateMvcc::Ok(row_id)
            }
            UpdateRowInplace::RowDeleted | UpdateRowInplace::RowNotFound => UpdateMvcc::NotFound,
            UpdateRowInplace::WriteConflict => UpdateMvcc::WriteConflict,
            UpdateRowInplace::NoFreeSpace(old_row_id, old_row, update, old_guard) => {
                // in-place update failed, we transfer update into
                // move+update.
                let (new_row_id, index_change_cols, new_guard) = self
                    .move_update_for_space(buf_pool, stmt, old_row, update, old_row_id, old_guard)
                    .await;
                if !index_change_cols.is_empty() {
                    let res = self
                        .update_indexes_may_both_change(
                            buf_pool,
                            stmt,
                            old_row_id,
                            new_row_id,
                            &index_change_cols,
                            &new_guard,
                        )
                        .await;
                    // old guard is already marked inside.
                    new_guard.set_dirty(); // mark as dirty page.
                    match res {
                        UpdateIndex::Ok => UpdateMvcc::Ok(new_row_id),
                        UpdateIndex::DuplicateKey => UpdateMvcc::DuplicateKey,
                        UpdateIndex::WriteConflict => UpdateMvcc::WriteConflict,
                    }
                } else {
                    let res = self.update_indexes_only_row_id_change(
                        stmt, old_row_id, new_row_id, &new_guard,
                    );
                    new_guard.set_dirty(); // mark as dirty page.
                    match res {
                        UpdateIndex::Ok => UpdateMvcc::Ok(new_row_id),
                        UpdateIndex::DuplicateKey => UpdateMvcc::DuplicateKey,
                        UpdateIndex::WriteConflict => UpdateMvcc::WriteConflict,
                    }
                }
            }
        }
    }

    /// Delete row with MVCC.
    /// This method is for delete based on unique index lookup.
    #[inline]
    pub async fn delete_row(
        &self,
        buf_pool: &'static P,
        stmt: &mut Statement<P>,
        key: &SelectKey,
    ) -> DeleteMvcc {
        debug_assert!(key.index_no < self.sec_idx.len());
        debug_assert!(self.metadata.index_specs[key.index_no].unique());
        debug_assert!(self.metadata.index_layout_match(key.index_no, &key.vals));
        let index = self.sec_idx[key.index_no].unique().unwrap();
        let (page_guard, row_id) = loop {
            match index.lookup(&key.vals) {
                None => return DeleteMvcc::NotFound,
                Some(row_id) => match self.blk_idx.find_row_id(buf_pool, row_id).await {
                    RowLocation::NotFound => return DeleteMvcc::NotFound,
                    RowLocation::ColSegment(..) => todo!(),
                    RowLocation::RowPage(page_id) => {
                        let page_guard = buf_pool
                            .get_page::<RowPage>(page_id, LatchFallbackMode::Shared)
                            .await
                            .shared_async()
                            .await;
                        if validate_page_id(&page_guard, page_id) {
                            break (page_guard, row_id);
                        }
                    }
                },
            }
        };
        match self
            .delete_row_internal(stmt, page_guard, row_id, key)
            .await
        {
            DeleteInternal::NotFound => DeleteMvcc::NotFound,
            DeleteInternal::WriteConflict => DeleteMvcc::WriteConflict,
            DeleteInternal::Ok(page_guard) => {
                // defer index deletion with index undo log.
                self.defer_delete_indexes(stmt, row_id, &page_guard);
                page_guard.set_dirty(); // mark as dirty.
                DeleteMvcc::Ok
            }
        }
    }

    /// Delete index by purge threads.
    /// This method will be only called by internal threads and don't maintain
    /// transaction properties.
    ///
    /// It checks whether the index entry still points to valid row, and if not,
    /// remove the entry.
    ///
    /// The validation is based on MVCC with minimum active STS. If the input
    /// key is not found on the path of undo chain, it means the index entry can be
    /// removed.
    #[inline]
    pub async fn delete_index(&self, buf_pool: &'static P, key: &SelectKey, row_id: RowID) -> bool {
        // todo: consider index drop.
        let index_schema = &self.metadata.index_specs[key.index_no];
        if index_schema.unique() {
            let index = self.sec_idx[key.index_no].unique().unwrap();
            return self.delete_unique_index(buf_pool, index, key, row_id).await;
        }
        todo!()
    }

    #[inline]
    async fn delete_unique_index(
        &self,
        buf_pool: &'static P,
        index: &dyn UniqueIndex,
        key: &SelectKey,
        row_id: RowID,
    ) -> bool {
        let (page_guard, row_id) = loop {
            match index.lookup(&key.vals) {
                None => return false, // Another thread deleted this entry.
                Some(index_row_id) => {
                    if index_row_id != row_id {
                        // Row id changed, means another transaction inserted
                        // new row with same key and reused this index entry.
                        // So we skip to delete it.
                        return false;
                    }
                    match self.blk_idx.find_row_id(buf_pool, row_id).await {
                        RowLocation::NotFound => {
                            return index.compare_delete(&key.vals, row_id);
                        }
                        RowLocation::ColSegment(..) => todo!(),
                        RowLocation::RowPage(page_id) => {
                            let page_guard = buf_pool
                                .get_page::<RowPage>(page_id, LatchFallbackMode::Shared)
                                .await
                                .shared_async()
                                .await;
                            if validate_page_row_range(&page_guard, page_id, row_id) {
                                break (page_guard, row_id);
                            }
                        }
                    }
                }
            }
        };
        let access = page_guard.read_row_by_id(row_id);
        // To safely delete an index entry, we need to make sure
        // no version with matched keys can be found in either page
        // data or version chain.
        if !access.any_version_matches_key(&self.metadata, key) {
            return index.compare_delete(&key.vals, row_id);
        }
        false
    }

    /// Move update is similar to a delete+insert.
    /// It's caused by no more space on current row page.
    #[inline]
    async fn move_update_for_space(
        &self,
        buf_pool: &'static P,
        stmt: &mut Statement<P>,
        old_row: Vec<(Val, Option<u16>)>,
        update: Vec<UpdateCol>,
        old_id: RowID,
        old_guard: PageSharedGuard<RowPage>,
    ) -> (RowID, HashSet<usize>, PageSharedGuard<RowPage>) {
        // calculate new row and undo entry.
        let (new_row, undo_kind, index_change_cols) = {
            let mut index_change_cols = HashSet::new();
            let mut row = Vec::with_capacity(old_row.len());
            let mut var_offsets = Vec::with_capacity(old_row.len());
            for (v, var_offset) in old_row {
                row.push(v);
                var_offsets.push(var_offset);
            }
            let mut undo_cols = vec![];
            for mut uc in update {
                let old_val = &mut row[uc.idx];
                if old_val != &uc.val {
                    if self.metadata.user_index_cols.contains(&uc.idx) {
                        index_change_cols.insert(uc.idx);
                    }
                    // swap old value and new value, then put into undo columns
                    mem::swap(&mut uc.val, old_val);
                    undo_cols.push(UndoCol {
                        idx: uc.idx,
                        val: uc.val,
                        var_offset: var_offsets[uc.idx],
                    });
                }
            }
            (row, RowUndoKind::Update(undo_cols), index_change_cols)
        };
        let (new_row_id, new_guard) = self
            .insert_row_internal(
                buf_pool,
                stmt,
                new_row,
                undo_kind,
                Some((old_id, old_guard)),
            )
            .await;
        // do not unlock the page because we may need to update index
        (new_row_id, index_change_cols, new_guard)
    }

    /// Link old version for index.
    /// This is a special operation for unique index maintainance.
    /// It's triggered by duplicate key finding when updating index.
    ///
    /// There are two cases:
    /// 1. The old row is deleted.
    /// In this case, the old row must be committed and the DELETE undo
    /// entry is not purged so some other transaction is still able to
    /// see a non-deleted version.
    /// We need to link new row(via undo head) to the DELETE entry.
    ///
    /// 2. The old row is updated with another key but one of its
    /// old versions matches the key.
    /// In this case, the old row must be committed.
    /// We need to find the key-match version, and can link new row
    /// to it.
    #[inline]
    async fn link_for_unique_index(
        &self,
        buf_pool: &'static P,
        stmt: &Statement<P>,
        old_id: RowID,
        key: &SelectKey,
        new_id: RowID,
        new_guard: &PageSharedGuard<RowPage>,
    ) -> LinkForUniqueIndex {
        debug_assert!(old_id != new_id);
        let (old_guard, old_id) = loop {
            match self.blk_idx.find_row_id(buf_pool, old_id).await {
                RowLocation::NotFound => return LinkForUniqueIndex::None,
                RowLocation::ColSegment(..) => todo!(),
                RowLocation::RowPage(page_id) => {
                    let old_guard = buf_pool
                        .get_page::<RowPage>(page_id, LatchFallbackMode::Shared)
                        .await
                        .shared_async()
                        .await;
                    if validate_page_row_range(&old_guard, page_id, old_id) {
                        break (old_guard, old_id);
                    }
                }
            }
        };
        // The link process is to find one version of the old row that matches
        // given key and then link new row to it.
        let old_access = old_guard.write_row_by_id(old_id);
        match old_access.find_old_version_for_unique_key(&self.metadata, key, &stmt.trx) {
            FindOldVersion::None => LinkForUniqueIndex::Ok,
            FindOldVersion::DuplicateKey => LinkForUniqueIndex::DuplicateKey,
            FindOldVersion::WriteConflict => LinkForUniqueIndex::WriteConflict,
            FindOldVersion::Ok(old_row, cts, old_entry) => {
                // row latch is enough, because row lock is already acquired.
                let mut new_access = new_guard.write_row_by_id(new_id);
                let undo_vals = new_access.row().calc_delta(&self.metadata, &old_row);
                new_access.link_for_unique_index(key.clone(), cts, old_entry, undo_vals);
                LinkForUniqueIndex::Ok
            }
        }
    }

    #[inline]
    async fn insert_row_internal(
        &self,
        buf_pool: &'static P,
        stmt: &mut Statement<P>,
        mut insert: Vec<Val>,
        mut undo_kind: RowUndoKind,
        mut move_entry: Option<(RowID, PageSharedGuard<RowPage>)>,
    ) -> (RowID, PageSharedGuard<RowPage>) {
        let row_len = row_len(&self.metadata, &insert);
        let row_count = estimate_max_row_count(row_len, self.metadata.col_count());
        loop {
            let page_guard = self.get_insert_page(buf_pool, stmt, row_count).await;
            let page_id = page_guard.page_id();
            match self.insert_row_to_page(stmt, page_guard, insert, undo_kind, move_entry) {
                InsertRowIntoPage::Ok(row_id, page_guard) => {
                    stmt.save_active_insert_page(self.table_id(), page_id, row_id);
                    return (row_id, page_guard);
                }
                // this page cannot be inserted any more, just leave it and retry another page.
                InsertRowIntoPage::NoSpaceOrRowID(ins, uk, me) => {
                    insert = ins;
                    undo_kind = uk;
                    move_entry = me;
                }
            }
        }
    }

    /// Insert row into given page.
    /// There might be move+update call this method, in such case, undo_kind will be
    /// set to UndoKind::Update.
    #[inline]
    fn insert_row_to_page(
        &self,
        stmt: &mut Statement<P>,
        page_guard: PageSharedGuard<RowPage>,
        insert: Vec<Val>,
        undo_kind: RowUndoKind,
        move_entry: Option<(RowID, PageSharedGuard<RowPage>)>,
    ) -> InsertRowIntoPage {
        debug_assert!({
            (matches!(undo_kind, RowUndoKind::Insert) && move_entry.is_none())
                || (matches!(undo_kind, RowUndoKind::Update(_)) && move_entry.is_some())
        });

        let page_id = page_guard.page_id();
        let page = page_guard.page();
        debug_assert!(self.metadata.col_count() == page.header.col_count as usize);
        // insert row does not include RowID, as RowID is auto-generated.
        debug_assert!(insert.len() + 1 == page.header.col_count as usize);

        let var_len = var_len_for_insert(&self.metadata, &insert);
        let (row_idx, var_offset) =
            if let Some((row_idx, var_offset)) = page.request_row_idx_and_free_space(var_len) {
                (row_idx, var_offset)
            } else {
                return InsertRowIntoPage::NoSpaceOrRowID(insert, undo_kind, move_entry);
            };
        // Before real insert, we need to lock the row.
        let row_id = page.header.start_row_id + row_idx as u64;
        let mut access = page_guard.write_row(row_idx);
        access.lock_undo(stmt, &self.metadata, self.table_id(), page_id, row_id, None);
        // Apply insert
        let mut new_row = page.new_row(row_idx as usize, var_offset);
        for v in &insert {
            match v {
                Val::Null => new_row.add_null(),
                Val::Byte1(v1) => new_row.add_val(*v1),
                Val::Byte2(v2) => new_row.add_val(*v2),
                Val::Byte4(v4) => new_row.add_val(*v4),
                Val::Byte8(v8) => new_row.add_val(*v8),
                Val::VarByte(var) => new_row.add_var(var.as_bytes()),
            }
        }
        let new_row_id = new_row.finish();
        debug_assert!(new_row_id == row_id);
        stmt.update_last_undo(undo_kind);
        // Here we do not release row latch because we may need to link MOVE entry.

        // The MOVE undo entry is for MOVE+UPDATE.
        // Once update in-place fails, we convert the update operation to insert.
        // and link them together.
        if let Some((old_id, old_guard)) = move_entry {
            // Here we actually lock both new row and old row,
            // not very sure if this will cause dead-lock.
            let old_access = old_guard.write_row_by_id(old_id);
            debug_assert!({ old_access.undo_head().is_some() });
            debug_assert!(stmt
                .trx
                .is_same_trx(&old_access.undo_head().as_ref().unwrap()));
            // re-lock moved row and link new entry to it.
            let move_entry = old_access.first_undo_entry().unwrap();
            let new_entry = stmt.row_undo.last_mut().unwrap();
            debug_assert!(matches!(move_entry.as_ref().kind, RowUndoKind::Move(_)));
            debug_assert!(matches!(new_entry.kind, RowUndoKind::Update(_)));
            debug_assert!(new_entry.next.is_none());
            new_entry.next.replace(NextRowUndo::new(MainBranch {
                entry: move_entry,
                status: UndoStatus::Ref(stmt.trx.status()),
            }));
            drop(old_access);
            old_guard.set_dirty(); // mark as dirty page.
        }
        drop(access);
        // Here we do not unlock the page because we need to verify validity of unique index update
        // according to this insert.
        // There might be scenario that a deleted row or old version of updated row shares the same
        // key with this insert.
        // Then we have to link insert's undo head to that version via *INDEX* branch.
        // Hold the page guard in order to re-lock the undo head fast.
        //
        // create redo log.
        // even if the operation is move+update, we still treat it as insert redo log.
        // because redo is only useful when recovering and no version chain is required
        // during recovery.
        let redo_entry = RowRedo {
            page_id,
            row_id,
            kind: RowRedoKind::Insert(insert),
        };
        // store redo log into transaction redo buffer.
        stmt.redo.insert_dml(self.table_id(), redo_entry);
        InsertRowIntoPage::Ok(row_id, page_guard)
    }

    #[inline]
    async fn update_row_inplace(
        &self,
        stmt: &mut Statement<P>,
        page_guard: PageSharedGuard<RowPage>,
        key: &SelectKey,
        row_id: RowID,
        mut update: Vec<UpdateCol>,
    ) -> UpdateRowInplace {
        let page_id = page_guard.page_id();
        let page = page_guard.page();
        // column indexes must be in range
        debug_assert!(
            {
                update
                    .iter()
                    .all(|uc| uc.idx < page_guard.page().header.col_count as usize)
            },
            "update column indexes must be in range"
        );
        // column indexes should be in order.
        debug_assert!(
            {
                update.is_empty()
                    || update
                        .iter()
                        .zip(update.iter().skip(1))
                        .all(|(l, r)| l.idx < r.idx)
            },
            "update columns should be in order"
        );
        if row_id < page.header.start_row_id
            || row_id >= page.header.start_row_id + page.header.max_row_count as u64
        {
            return UpdateRowInplace::RowNotFound;
        }
        let mut lock_row = self
            .lock_row_for_write(stmt, &page_guard, row_id, Some(key))
            .await;
        match &mut lock_row {
            LockRowForWrite::InvalidIndex => return UpdateRowInplace::RowNotFound,
            LockRowForWrite::WriteConflict => return UpdateRowInplace::WriteConflict,
            LockRowForWrite::Ok(access) => {
                let mut access = access.take().unwrap();
                if access.row().is_deleted() {
                    return UpdateRowInplace::RowDeleted;
                }
                match access.update_row(&self.metadata, &update) {
                    UpdateRow::NoFreeSpace(old_row) => {
                        // Page does not have enough space for update, we need to switch
                        // to out-of-place update mode, which will add a MOVE undo entry
                        // to end original row and perform a INSERT into new page, and
                        // link the two versions.
                        //
                        // Mark page data as deleted.
                        access.delete_row();
                        // Update LOCK entry to MOVE entry.
                        stmt.update_last_undo(RowUndoKind::Move(false));
                        drop(access); // unlock row
                        drop(lock_row);
                        // Here we do not unlock page because we need to perform MOVE+UPDATE
                        // and link undo entries of two rows.
                        // The re-lock of current undo is required.
                        let redo_entry = RowRedo {
                            page_id,
                            row_id,
                            // use DELETE for redo is ok, no version chain should be maintained if recovering from redo.
                            kind: RowRedoKind::Delete,
                        };
                        stmt.redo.insert_dml(self.table_id(), redo_entry);
                        UpdateRowInplace::NoFreeSpace(row_id, old_row, update, page_guard)
                    }
                    UpdateRow::Ok(mut row) => {
                        let mut index_change_cols = HashSet::new();
                        // perform in-place update.
                        let (mut undo_cols, mut redo_cols) = (vec![], vec![]);
                        for uc in &mut update {
                            if let Some((old, var_offset)) =
                                row.user_different(&self.metadata, uc.idx, &uc.val)
                            {
                                let old_val = Val::from(old);
                                let new_val = mem::take(&mut uc.val);
                                // we also check whether the value change is related to any index,
                                // so we can update index later.
                                if self.metadata.user_index_cols.contains(&uc.idx) {
                                    index_change_cols.insert(uc.idx);
                                }
                                // actual update
                                row.update_user_col(uc.idx, &new_val);
                                // record undo and redo
                                undo_cols.push(UndoCol {
                                    idx: uc.idx,
                                    val: old_val,
                                    var_offset,
                                });
                                redo_cols.push(UpdateCol {
                                    idx: uc.idx,
                                    // new value no longer needed, so safe to take it here.
                                    val: new_val,
                                });
                            }
                        }
                        // Update LOCK entry to UPDATE entry.
                        stmt.update_last_undo(RowUndoKind::Update(undo_cols));
                        drop(access); // unlock the row.
                        drop(lock_row);
                        // we may still need this page if we'd like to update index.
                        if !redo_cols.is_empty() {
                            // there might be nothing to update, so we do not need to add redo log.
                            // but undo is required because we need to properly lock the row.
                            let redo_entry = RowRedo {
                                page_id,
                                row_id,
                                kind: RowRedoKind::Update(redo_cols),
                            };
                            stmt.redo.insert_dml(self.table_id(), redo_entry);
                        }
                        UpdateRowInplace::Ok(row_id, index_change_cols, page_guard)
                    }
                }
            }
        }
    }

    #[inline]
    async fn delete_row_internal(
        &self,
        stmt: &mut Statement<P>,
        page_guard: PageSharedGuard<RowPage>,
        row_id: RowID,
        key: &SelectKey,
    ) -> DeleteInternal {
        let page_id = page_guard.page_id();
        let page = page_guard.page();
        if !page.row_id_in_valid_range(row_id) {
            return DeleteInternal::NotFound;
        }
        let mut lock_row = self
            .lock_row_for_write(stmt, &page_guard, row_id, Some(key))
            .await;
        match &mut lock_row {
            LockRowForWrite::InvalidIndex => return DeleteInternal::NotFound,
            LockRowForWrite::WriteConflict => return DeleteInternal::WriteConflict,
            LockRowForWrite::Ok(access) => {
                let mut access = access.take().unwrap();
                if access.row().is_deleted() {
                    return DeleteInternal::NotFound;
                }
                access.delete_row();
                // update LOCK entry to DELETE entry.
                stmt.update_last_undo(RowUndoKind::Delete);
                drop(access); // unlock row
                drop(lock_row);
                // hold page lock in order to update index later.
                // create redo log
                let redo_entry = RowRedo {
                    page_id,
                    row_id,
                    kind: RowRedoKind::Delete,
                };
                stmt.redo.insert_dml(self.table_id(), redo_entry);
                DeleteInternal::Ok(page_guard)
            }
        }
    }

    #[inline]
    async fn get_insert_page(
        &self,
        buf_pool: &'static P,
        stmt: &mut Statement<P>,
        row_count: usize,
    ) -> PageSharedGuard<RowPage> {
        if let Some((page_id, row_id)) = stmt.load_active_insert_page(self.table_id()) {
            let page_guard = buf_pool
                .get_page(page_id, LatchFallbackMode::Shared)
                .await
                .shared_async()
                .await;
            // because we save last insert page in session and meanwhile other thread may access this page
            // and do some modification, even worse, buffer pool may evict it and reload other data into
            // this page. so here, we do not require that no change should happen, but if something change,
            // we validate that page id and row id range is still valid.
            if validate_page_row_range(&page_guard, page_id, row_id) {
                return page_guard;
            }
        }
        self.blk_idx
            .get_insert_page(buf_pool, row_count, &self.metadata)
            .await
    }

    // lock row will check write conflict on given row and lock it.
    #[inline]
    async fn lock_row_for_write<'a>(
        &self,
        stmt: &mut Statement<P>,
        page_guard: &'a PageSharedGuard<RowPage>,
        row_id: RowID,
        key: Option<&SelectKey>,
    ) -> LockRowForWrite<'a> {
        loop {
            let mut access = page_guard.write_row_by_id(row_id);
            let lock_undo = access.lock_undo(
                stmt,
                &self.metadata,
                self.table_id(),
                page_guard.page_id(),
                row_id,
                key,
            );
            match lock_undo {
                LockUndo::Ok => {
                    return LockRowForWrite::Ok(Some(access));
                }
                LockUndo::InvalidIndex => {
                    return LockRowForWrite::InvalidIndex;
                }
                LockUndo::WriteConflict => {
                    return LockRowForWrite::WriteConflict;
                }
                LockUndo::Preparing(notify) => {
                    if let Some(notify) = notify {
                        drop(access);

                        // Here we do not unlock the page, because the preparation time of commit is supposed
                        // to be short.
                        // And as active transaction is using this page, we don't want page evictor swap it onto
                        // disk.
                        // Other transactions can still access this page and modify other rows.

                        let _ = notify.wait_async(false).await; // wait for that transaction to be committed.

                        // now we get back on current page.
                        // maybe another thread modify our row before the lock acquisition,
                        // so we need to recheck.
                    } // there might be progress on preparation, so recheck.
                }
            }
        }
    }

    #[inline]
    fn index_undo(&self, row_id: RowID, kind: IndexUndoKind) -> IndexUndo {
        IndexUndo {
            table_id: self.table_id(),
            row_id,
            kind,
        }
    }

    #[inline]
    async fn insert_unique_index(
        &self,
        buf_pool: &'static P,
        stmt: &mut Statement<P>,
        key: SelectKey,
        row_id: RowID,
        page_guard: &PageSharedGuard<RowPage>,
    ) -> InsertIndex {
        let index = self.sec_idx[key.index_no].unique().unwrap();
        match index.insert_if_not_exists(&key.vals, row_id) {
            None => {
                // insert index success.
                let index_undo = self.index_undo(row_id, IndexUndoKind::InsertUnique(key));
                stmt.index_undo.push(index_undo);
                InsertIndex::Ok
            }
            Some(old_row_id) => {
                // we found there is already one existing row with same key.
                // so perform move+link.
                debug_assert!(old_row_id != row_id);
                match self
                    .link_for_unique_index(buf_pool, stmt, old_row_id, &key, row_id, page_guard)
                    .await
                {
                    LinkForUniqueIndex::DuplicateKey => InsertIndex::DuplicateKey,
                    LinkForUniqueIndex::WriteConflict => InsertIndex::WriteConflict,
                    LinkForUniqueIndex::None => {
                        // No old row found, so we can update index to point to self.
                        // This case can happen if purge thread removed row data,
                        // but index is not purged.
                        if !index.compare_exchange(&key.vals, old_row_id, row_id) {
                            // there is another transaction update the unique index concurrently,
                            // we can directly fail.
                            return InsertIndex::WriteConflict;
                        }
                        let index_undo =
                            self.index_undo(row_id, IndexUndoKind::UpdateUnique(key, old_row_id));
                        stmt.index_undo.push(index_undo);
                        InsertIndex::Ok
                    }
                    LinkForUniqueIndex::Ok => {
                        // There is scenario that two transactions update different rows to the same
                        // key. Because we only search the matched version of old row but not add
                        // logical lock on it, it's possible that both transactions are trying to
                        // update the index. Only one should succeed and the other will fail.
                        if !index.compare_exchange(&key.vals, old_row_id, row_id) {
                            return InsertIndex::WriteConflict;
                        }
                        let index_undo =
                            self.index_undo(row_id, IndexUndoKind::UpdateUnique(key, old_row_id));
                        stmt.index_undo.push(index_undo);
                        InsertIndex::Ok
                    }
                }
            }
        }
    }

    #[inline]
    async fn update_indexes_only_key_change(
        &self,
        buf_pool: &'static P,
        stmt: &mut Statement<P>,
        row_id: RowID,
        page_guard: &PageSharedGuard<RowPage>,
        index_change_cols: &HashSet<usize>,
    ) -> UpdateIndex {
        for (index, index_schema) in self.sec_idx.iter().zip(&self.metadata.index_specs) {
            debug_assert!(index.is_unique() == index_schema.unique());
            if index_schema.unique() {
                if index_key_is_changed(index_schema, index_change_cols) {
                    let new_key =
                        read_latest_index_key(&self.metadata, index.index_no, page_guard, row_id);
                    match self
                        .update_unique_index_only_key_change(
                            buf_pool,
                            stmt,
                            index.unique().unwrap(),
                            new_key,
                            row_id,
                            page_guard,
                        )
                        .await
                    {
                        UpdateIndex::Ok => (),
                        UpdateIndex::DuplicateKey => return UpdateIndex::DuplicateKey,
                        UpdateIndex::WriteConflict => return UpdateIndex::WriteConflict,
                    };
                } // otherwise, in-place update do not change row id, so we do nothing
            } else {
                todo!();
            }
        }
        UpdateIndex::Ok
    }

    #[inline]
    fn update_indexes_only_row_id_change<'a>(
        &self,
        stmt: &mut Statement<P>,
        old_row_id: RowID,
        new_row_id: RowID,
        page_guard: &PageSharedGuard<RowPage>,
    ) -> UpdateIndex {
        debug_assert!(old_row_id != new_row_id);
        for (index, index_schema) in self.sec_idx.iter().zip(&self.metadata.index_specs) {
            debug_assert!(index.is_unique() == index_schema.unique());
            if index_schema.unique() {
                let key =
                    read_latest_index_key(&self.metadata, index.index_no, page_guard, new_row_id);
                match self.update_unique_index_only_row_id_change(
                    stmt,
                    index.unique().unwrap(),
                    key,
                    old_row_id,
                    new_row_id,
                ) {
                    UpdateIndex::Ok => (),
                    UpdateIndex::DuplicateKey => return UpdateIndex::DuplicateKey,
                    UpdateIndex::WriteConflict => return UpdateIndex::WriteConflict,
                }
            } else {
                todo!();
            }
        }
        UpdateIndex::Ok
    }

    #[inline]
    async fn update_indexes_may_both_change(
        &self,
        buf_pool: &'static P,
        stmt: &mut Statement<P>,
        old_row_id: RowID,
        new_row_id: RowID,
        index_change_cols: &HashSet<usize>,
        page_guard: &PageSharedGuard<RowPage>,
    ) -> UpdateIndex {
        debug_assert!(old_row_id != new_row_id);
        for (index, index_schema) in self.sec_idx.iter().zip(&self.metadata.index_specs) {
            debug_assert!(index.is_unique() == index_schema.unique());
            if index_schema.unique() {
                let key =
                    read_latest_index_key(&self.metadata, index.index_no, &page_guard, new_row_id);
                if index_key_is_changed(index_schema, index_change_cols) {
                    // key change and row id change.
                    match self
                        .update_unique_index_key_and_row_id_change(
                            buf_pool,
                            stmt,
                            index.unique().unwrap(),
                            key,
                            old_row_id,
                            new_row_id,
                            page_guard,
                        )
                        .await
                    {
                        UpdateIndex::DuplicateKey => return UpdateIndex::DuplicateKey,
                        UpdateIndex::WriteConflict => return UpdateIndex::WriteConflict,
                        UpdateIndex::Ok => (),
                    }
                } else {
                    // only row id change.
                    match self.update_unique_index_only_row_id_change(
                        stmt,
                        index.unique().unwrap(),
                        key,
                        old_row_id,
                        new_row_id,
                    ) {
                        UpdateIndex::DuplicateKey => return UpdateIndex::DuplicateKey,
                        UpdateIndex::WriteConflict => return UpdateIndex::WriteConflict,
                        UpdateIndex::Ok => (),
                    }
                }
            } else {
                todo!();
            }
        }
        UpdateIndex::Ok
    }

    #[inline]
    fn defer_delete_indexes<'a>(
        &self,
        stmt: &mut Statement<P>,
        row_id: RowID,
        page_guard: &PageSharedGuard<RowPage>,
    ) {
        for (index, index_schema) in self.sec_idx.iter().zip(&self.metadata.index_specs) {
            debug_assert!(index.is_unique() == index_schema.unique());
            if index_schema.unique() {
                let key = read_latest_index_key(&self.metadata, index.index_no, page_guard, row_id);
                let index_undo = self.index_undo(row_id, IndexUndoKind::DeferDelete(key));
                stmt.index_undo.push(index_undo);
            } else {
                todo!();
            }
        }
    }

    async fn update_unique_index_key_and_row_id_change(
        &self,
        buf_pool: &'static P,
        stmt: &mut Statement<P>,
        index: &dyn UniqueIndex,
        key: SelectKey,
        old_row_id: RowID,
        new_row_id: RowID,
        new_guard: &PageSharedGuard<RowPage>,
    ) -> UpdateIndex {
        debug_assert!(old_row_id != new_row_id);
        match index.insert_if_not_exists(&key.vals, new_row_id) {
            None => {
                let index_undo = self.index_undo(new_row_id, IndexUndoKind::InsertUnique(key));
                stmt.index_undo.push(index_undo);
                UpdateIndex::Ok
            }
            Some(index_row_id) => {
                // new row id is the insert id so index value must not be the same.
                debug_assert!(index_row_id != new_row_id);
                if index_row_id == old_row_id {
                    // This is possible.
                    // For example, transaction update row(RowID=100) key=1 to key=2.
                    //
                    // Then index has following entries:
                    // key=1 -> RowID=100 (old version)
                    // key=2 -> RowID=100 (latest version)
                    //
                    // Then we update key=2 to key=1 again.
                    // And page does not have enough space, so move+update with RowID=200.
                    // Now we should have:
                    // key=1 -> RowID=200 (latest version)
                    // key=2 -> RowID=100 (old version)
                    //
                    // In this case, we can just update index to point to new version.
                    let res = index.compare_exchange(&key.vals, old_row_id, new_row_id);
                    assert!(res);
                    let index_undo =
                        self.index_undo(new_row_id, IndexUndoKind::UpdateUnique(key, old_row_id));
                    stmt.index_undo.push(index_undo);
                    return UpdateIndex::Ok;
                }
                match self
                    .link_for_unique_index(
                        buf_pool,
                        stmt,
                        index_row_id,
                        &key,
                        new_row_id,
                        new_guard,
                    )
                    .await
                {
                    LinkForUniqueIndex::DuplicateKey => UpdateIndex::DuplicateKey,
                    LinkForUniqueIndex::WriteConflict => UpdateIndex::WriteConflict,
                    LinkForUniqueIndex::None => {
                        // move+insert does not find old row.
                        // so we can update index to point to self
                        if !index.compare_exchange(&key.vals, index_row_id, new_row_id) {
                            // there is another transaction update the unique index concurrently,
                            // we can directly fail.
                            return UpdateIndex::WriteConflict;
                        }
                        let index_undo = self
                            .index_undo(new_row_id, IndexUndoKind::UpdateUnique(key, index_row_id));
                        stmt.index_undo.push(index_undo);
                        UpdateIndex::Ok
                    }
                    LinkForUniqueIndex::Ok => {
                        // Once move+insert is done,
                        // we already locked both old and new row, and make undo chain linked.
                        // So any other transaction that want to modify the index with same key
                        // should fail because lock can not be acquired by them.
                        let res = index.compare_exchange(&key.vals, index_row_id, new_row_id);
                        assert!(res);
                        let index_undo = self
                            .index_undo(new_row_id, IndexUndoKind::UpdateUnique(key, index_row_id));
                        stmt.index_undo.push(index_undo);
                        UpdateIndex::Ok
                    }
                }
            }
        }
    }

    #[inline]
    fn update_unique_index_only_row_id_change<'a>(
        &self,
        stmt: &mut Statement<P>,
        index: &dyn UniqueIndex,
        key: SelectKey,
        old_row_id: RowID,
        new_row_id: RowID,
    ) -> UpdateIndex {
        debug_assert!(old_row_id != new_row_id);
        let res = index.compare_exchange(&key.vals, old_row_id, new_row_id);
        assert!(res);
        let index_undo = self.index_undo(new_row_id, IndexUndoKind::UpdateUnique(key, old_row_id));
        stmt.index_undo.push(index_undo);
        UpdateIndex::Ok
    }

    /// Update unique index due to key change.
    /// In this scenario, we only need to insert pair of new key and row id
    /// into index. Keep old index entry as is.
    #[inline]
    async fn update_unique_index_only_key_change(
        &self,
        buf_pool: &'static P,
        stmt: &mut Statement<P>,
        index: &dyn UniqueIndex,
        new_key: SelectKey,
        row_id: RowID,
        page_guard: &PageSharedGuard<RowPage>,
    ) -> UpdateIndex {
        match index.insert_if_not_exists(&new_key.vals, row_id) {
            None => {
                let index_undo = self.index_undo(row_id, IndexUndoKind::InsertUnique(new_key));
                stmt.index_undo.push(index_undo);
                UpdateIndex::Ok
            }
            Some(index_row_id) => {
                // There is already a row with same new key.
                // We have to check its status.
                if index_row_id == row_id {
                    // This is possible.
                    // For example, transaction update row(RowID=100) key=1 to key=2.
                    //
                    // Then index has following entries:
                    // key=1 -> RowID=100 (old version)
                    // key=2 -> RowID=100 (latest version)
                    //
                    // Then we update key=2 to key=1 again.
                    // Now we should have:
                    // key=1 -> RowID=100 (latest version)
                    // key=2 -> RowID=100 (old version)
                    //
                    // nothing to do in this case.
                    return UpdateIndex::Ok;
                }
                match self
                    .link_for_unique_index(
                        buf_pool,
                        stmt,
                        index_row_id,
                        &new_key,
                        row_id,
                        page_guard,
                    )
                    .await
                {
                    LinkForUniqueIndex::DuplicateKey => UpdateIndex::DuplicateKey,
                    LinkForUniqueIndex::WriteConflict => UpdateIndex::WriteConflict,
                    LinkForUniqueIndex::None => {
                        // no old row found.
                        if !index.compare_exchange(&new_key.vals, index_row_id, row_id) {
                            // there is another transaction updating the unique index concurrently,
                            // we can directly fail.
                            return UpdateIndex::WriteConflict;
                        }
                        let index_undo = self
                            .index_undo(row_id, IndexUndoKind::UpdateUnique(new_key, index_row_id));
                        stmt.index_undo.push(index_undo);
                        UpdateIndex::Ok
                    }
                    LinkForUniqueIndex::Ok => {
                        // Both old row(index points to) and new row are locked.
                        // we must succeed on updateing index.
                        let res = index.compare_exchange(&new_key.vals, index_row_id, row_id);
                        assert!(res);
                        let index_undo = self
                            .index_undo(row_id, IndexUndoKind::UpdateUnique(new_key, index_row_id));
                        stmt.index_undo.push(index_undo);
                        UpdateIndex::Ok
                    }
                }
            }
        }
    }
}

impl<P: BufferPool> Clone for Table<P> {
    #[inline]
    fn clone(&self) -> Self {
        Table {
            metadata: Arc::clone(&self.metadata),
            blk_idx: Arc::clone(&self.blk_idx),
            sec_idx: Arc::clone(&self.sec_idx),
        }
    }
}

#[inline]
fn validate_page_id(page_guard: &PageSharedGuard<RowPage>, page_id: PageID) -> bool {
    if page_guard.page_id() != page_id {
        return false;
    }
    true
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
fn row_len(schema: &TableMetadata, user_cols: &[Val]) -> usize {
    let var_len = schema
        .var_cols
        .iter()
        .map(|idx| {
            let val = &user_cols[*idx - 1];
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
    schema.fix_len + var_len
}

enum InsertRowIntoPage {
    Ok(RowID, PageSharedGuard<RowPage>),
    NoSpaceOrRowID(
        Vec<Val>,
        RowUndoKind,
        Option<(RowID, PageSharedGuard<RowPage>)>,
    ),
}

enum UpdateRowInplace {
    // We keep row page lock if there is any index change,
    // so we can read latest values from page.
    Ok(RowID, HashSet<usize>, PageSharedGuard<RowPage>),
    RowNotFound,
    RowDeleted,
    WriteConflict,
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
}

#[inline]
fn index_key_is_changed(index_schema: &IndexSpec, index_change_cols: &HashSet<usize>) -> bool {
    index_schema
        .index_cols
        .iter()
        .any(|key| index_change_cols.contains(&(key.col_no as usize)))
}

#[inline]
fn read_latest_index_key(
    schema: &TableMetadata,
    index_no: usize,
    page_guard: &PageSharedGuard<RowPage>,
    row_id: RowID,
) -> SelectKey {
    let index_spec = &schema.index_specs[index_no];
    let mut new_key = SelectKey::null(index_no, index_spec.index_cols.len());
    for (pos, key) in index_spec.index_cols.iter().enumerate() {
        let access = page_guard.read_row_by_id(row_id);
        let val = access.row().clone_user_val(schema, key.col_no as usize);
        new_key.vals[pos] = val;
    }
    new_key
}
