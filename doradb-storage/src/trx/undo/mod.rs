mod index;
mod row;

pub(super) use index::IndexPurgeEntry;
#[cfg(test)]
pub(super) use index::take_index_undo;
pub(crate) use index::{IndexUndo, IndexUndoKind, IndexUndoLogs};
pub(crate) use row::*;

#[cfg(test)]
pub(crate) mod tests {
    use super::{OwnedRowUndo, RowUndoKind};
    use crate::catalog::{IndexID, IndexRef, IndexSlot, user_key_from_index_ref};
    use crate::error::{
        Error, FatalError, IoError, RuntimeError, RuntimeOrFatalError, RuntimeOrFatalResult,
    };
    use crate::id::{RowID, TableID};
    use crate::io::BackendError;
    use crate::trx::stmt::StmtEffects;
    use error_stack::Report;
    use std::cell::{Cell, RefCell};
    use std::future::pending;
    use std::io::Error as StdIoError;

    /// Undo family that receives the injected access failure.
    #[derive(Clone, Copy, PartialEq, Eq)]
    pub(crate) enum RollbackTarget {
        Index,
        Row,
    }

    thread_local! {
        static NEXT_ROLLBACK_ERROR: RefCell<Option<(RollbackTarget, RuntimeOrFatalError)>> = const { RefCell::new(None) };
    }

    /// Injects one error before the next undo entry is accessed on this thread.
    pub(crate) fn force_next_rollback_error(target: RollbackTarget, error: RuntimeOrFatalError) {
        NEXT_ROLLBACK_ERROR.set(Some((target, error)));
    }

    /// Builds the source chain produced by a backend progress failure.
    pub(crate) fn storage_io_failure() -> Report<FatalError> {
        BackendError::wait(
            "rollback_test_backend",
            StdIoError::from_raw_os_error(libc::EIO),
            1,
        )
        .to_report()
        .change_context(FatalError::StorageIo)
    }

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

    /// Stages unreachable undo so the injected error must run before table access.
    pub(crate) fn stage_rollback_test_effects(effects: &mut StmtEffects, target: RollbackTarget) {
        effects.push_row_undo(OwnedRowUndo::new(
            effects.stmt_no(),
            TableID::new(99_999_999),
            None,
            RowID::new(24),
            RowUndoKind::delete(),
        ));
        if target == RollbackTarget::Index {
            effects.push_delete_index_undo(
                TableID::new(12),
                RowID::new(23),
                user_key_from_index_ref(IndexRef::new(IndexID::new(0), IndexSlot::new(0)), vec![]),
                true,
            );
        }
    }

    /// Checks the original backend failure survived rollback and public disclosure.
    pub(crate) fn assert_storage_io_failure(error: &Error) {
        assert_eq!(
            error.report().downcast_ref::<FatalError>(),
            Some(&FatalError::StorageIo)
        );
        assert!(error.report().downcast_ref::<RuntimeError>().is_none());
        assert!(error.report().downcast_ref::<IoError>().is_some());
        let backend = error.report().downcast_ref::<BackendError>().unwrap();
        assert_eq!(backend.backend(), "rollback_test_backend");
        assert_eq!(backend.op(), "wait");
        assert_eq!(backend.raw_errno(), Some(libc::EIO));
    }

    pub(super) fn maybe_fail_rollback(target: RollbackTarget) -> RuntimeOrFatalResult<()> {
        NEXT_ROLLBACK_ERROR.with_borrow_mut(|next| {
            if next
                .as_ref()
                .is_some_and(|(expected, _)| *expected == target)
            {
                return Err(next.take().unwrap().1);
            }
            Ok(())
        })
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
