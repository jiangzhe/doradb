use crate::buffer::guard::PageExclusiveGuard;
use crate::row::ops::{InsertResult, InsertRow};
use crate::row::RowPage;
use crate::table::{SchemaRef, Table, TableID};
use crate::trx::redo::RedoEntry;
use crate::trx::undo::{UndoChain, UndoEntry, UndoKind};
use crate::trx::ActiveTrx;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;

/// MvccTable is a thin wrapper on table to extend MVCC behaviors.
pub struct MvccTable<'a>(pub(super) &'a Table<'a>);

impl<'a> Deref for MvccTable<'a> {
    type Target = Table<'a>;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a> MvccTable<'a> {
    #[inline]
    fn insert_row_into_page(
        &self,
        trx: &mut ActiveTrx,
        page_guard: &mut PageExclusiveGuard<'_, RowPage>,
        insert: &InsertRow,
    ) -> InsertResult {
        match page_guard.page_mut().insert(&self.schema, &insert) {
            InsertResult::Ok(row_id) => {
                let page_id = page_guard.page_id();
                // create undo log.
                let undo_entry = Arc::new(UndoEntry {
                    ts: Arc::clone(&trx.trx_id),
                    table_id: self.table_id,
                    page_id,
                    row_id,
                    kind: UndoKind::Insert,
                    chain: Mutex::new(UndoChain::default()),
                });
                // store undo log in undo map.
                let undo_map = page_guard
                    .bf_mut()
                    .undo_map
                    .get_or_insert_with(|| Box::new(HashMap::new()))
                    .as_mut();
                let res = undo_map.insert(row_id, Arc::clone(&undo_entry));
                debug_assert!(res.is_none()); // insert must not have old version.
                                              // store undo log into transaction undo buffer.
                trx.stmt_undo.push(undo_entry);
                // create redo log.
                let redo_entry = RedoEntry {
                    page_id,
                    row_id,
                    kind: insert.create_redo(),
                };
                // store redo log into transaction redo buffer.
                trx.stmt_redo.push(redo_entry);
                InsertResult::Ok(row_id)
            }
            err => err,
        }
    }
}
