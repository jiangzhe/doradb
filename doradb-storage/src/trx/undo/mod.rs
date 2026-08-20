mod index;
mod row;

pub(super) use index::IndexPurgeEntry;
pub(crate) use index::{IndexUndo, IndexUndoKind, IndexUndoLogs};
pub(crate) use row::*;

#[cfg(test)]
pub(crate) mod tests {
    use std::cell::Cell;
    use std::future::pending;

    thread_local! {
        static PAUSE_INDEX_ROLLBACK: Cell<bool> = const { Cell::new(false) };
        static PAUSE_ROW_ROLLBACK: Cell<bool> = const { Cell::new(false) };
        static INDEX_ROLLBACK_PAUSED: Cell<bool> = const { Cell::new(false) };
        static ROW_ROLLBACK_PAUSED: Cell<bool> = const { Cell::new(false) };
    }

    /// Pauses next index rollback for tests.
    #[inline]
    pub(crate) fn pause_next_index_rollback() {
        INDEX_ROLLBACK_PAUSED.set(false);
        PAUSE_INDEX_ROLLBACK.set(true);
    }

    /// Pauses next row rollback for tests.
    #[inline]
    pub(crate) fn pause_next_row_rollback() {
        ROW_ROLLBACK_PAUSED.set(false);
        PAUSE_ROW_ROLLBACK.set(true);
    }

    /// Provides test-only access to `index_rollback_paused`.
    #[inline]
    pub(crate) fn index_rollback_paused() -> bool {
        INDEX_ROLLBACK_PAUSED.get()
    }

    /// Provides test-only access to `row_rollback_paused`.
    #[inline]
    pub(crate) fn row_rollback_paused() -> bool {
        ROW_ROLLBACK_PAUSED.get()
    }

    #[inline]
    pub(super) async fn maybe_pause_index_rollback() {
        if PAUSE_INDEX_ROLLBACK.replace(false) {
            INDEX_ROLLBACK_PAUSED.set(true);
            pending::<()>().await;
        }
    }

    #[inline]
    pub(super) async fn maybe_pause_row_rollback() {
        if PAUSE_ROW_ROLLBACK.replace(false) {
            ROW_ROLLBACK_PAUSED.set(true);
            pending::<()>().await;
        }
    }
}
