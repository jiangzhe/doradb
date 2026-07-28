mod index;
mod row;

pub(super) use index::IndexPurgeEntry;
pub(crate) use index::{IndexUndo, IndexUndoKind, IndexUndoLogs};
pub(crate) use row::*;

#[cfg(test)]
pub(crate) mod test_hooks {
    use std::cell::Cell;
    use std::future::pending;

    thread_local! {
        static PAUSE_INDEX_ROLLBACK: Cell<bool> = const { Cell::new(false) };
        static PAUSE_ROW_ROLLBACK: Cell<bool> = const { Cell::new(false) };
    }

    #[inline]
    pub(crate) fn pause_next_index_rollback() {
        PAUSE_INDEX_ROLLBACK.set(true);
    }

    #[inline]
    pub(crate) fn pause_next_row_rollback() {
        PAUSE_ROW_ROLLBACK.set(true);
    }

    #[inline]
    pub(super) async fn maybe_pause_index_rollback() {
        if PAUSE_INDEX_ROLLBACK.replace(false) {
            pending::<()>().await;
        }
    }

    #[inline]
    pub(super) async fn maybe_pause_row_rollback() {
        if PAUSE_ROW_ROLLBACK.replace(false) {
            pending::<()>().await;
        }
    }
}
