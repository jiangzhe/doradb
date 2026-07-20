use crate::error::{LifecycleError, Result};
use crate::id::{PageID, RowID, TableID, TrxID};
use crate::poison::EnginePoisoner;
use crate::row::INVALID_ROW_ID;
use crate::trx::sys::TransactionSystem;

const U64_DELETE_BIT: u64 = 1u64 << 63;

/// Value that can be masked as deleted.
pub(crate) trait Maskable: Copy + PartialEq + Eq {
    const INVALID_VALUE: Self;

    /// Mask given value as deleted.
    fn deleted(self) -> Self;

    /// Returns value without delete mask.
    fn value(self) -> Self;

    /// Returns whether this value is masked as deleted.
    fn is_deleted(self) -> bool;
}

impl Maskable for RowID {
    const INVALID_VALUE: Self = INVALID_ROW_ID;

    #[inline]
    fn deleted(self) -> Self {
        RowID::new(self.as_u64() | U64_DELETE_BIT)
    }

    #[inline]
    fn value(self) -> Self {
        RowID::new(self.as_u64() & !U64_DELETE_BIT)
    }

    #[inline]
    fn is_deleted(self) -> bool {
        self.as_u64() & U64_DELETE_BIT != 0
    }
}

/// Statistics of space used by nodes.
#[derive(Debug, Default)]
pub(crate) struct SpaceStatistics {
    /// Number of nodes included in the statistic.
    pub(crate) nodes: usize,
    /// Total addressable bytes across all included nodes.
    pub(crate) total_space: usize,
    /// Bytes currently occupied by encoded entries.
    pub(crate) used_space: usize,
    /// Bytes counted as effective payload after format overhead.
    pub(crate) effective_space: usize,
}

pub(super) struct ParentPosition<G> {
    pub(super) g: G,
    // -1 means lower fence in btree node.
    pub(super) idx: isize,
}

/// Redo context used when a row page is created through the index.
#[derive(Clone, Copy)]
pub(crate) struct RowPageCreateRedoCtx<'a> {
    trx_sys: &'a TransactionSystem,
    poisoner: &'a EnginePoisoner,
    table_id: TableID,
}

impl RowPageCreateRedoCtx<'_> {
    /// Creates a redo context for row-page creation records.
    #[inline]
    pub(crate) fn new<'a>(
        trx_sys: &'a TransactionSystem,
        poisoner: &'a EnginePoisoner,
        table_id: TableID,
    ) -> RowPageCreateRedoCtx<'a> {
        RowPageCreateRedoCtx {
            trx_sys,
            poisoner,
            table_id,
        }
    }

    /// Commits the system redo record for a newly allocated row page.
    #[inline]
    pub(crate) fn commit_row_page(
        &self,
        page_id: PageID,
        start_row_id: RowID,
        end_row_id: RowID,
    ) -> Result<TrxID> {
        let mut trx = self.trx_sys.begin_sys_trx();
        let table_id = self.table_id;
        // Safety relies on callers serializing this no-wait commit with the
        // row-page-index append path. Later row-page creation and user redo
        // must not persist ahead of this CreateRowPage record.
        trx.create_row_page(table_id, page_id, start_row_id, end_row_id);
        let res = self.trx_sys.commit_sys(trx);
        if let Err(err) = &res {
            debug_assert!(
                self.poisoner.poison_error().is_some()
                    || err.report().downcast_ref::<LifecycleError>().copied()
                        == Some(LifecycleError::Shutdown),
                "row-page create redo failed while transaction system is still healthy: {err:?}"
            );
        }
        res
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::super::index_stream::{IndexBatchStream, IndexLookupCandidate};
    use crate::id::RowID;

    pub(crate) async fn drain_candidates<S: IndexBatchStream<IndexLookupCandidate>>(
        stream: &mut S,
    ) -> Vec<IndexLookupCandidate> {
        let mut candidates = Vec::new();
        while let Some(batch) = stream.next_batch().await.unwrap() {
            candidates.extend(batch);
        }
        candidates
    }

    pub(crate) async fn drain_row_ids<S: IndexBatchStream<IndexLookupCandidate>>(
        stream: &mut S,
    ) -> Vec<RowID> {
        drain_candidates(stream)
            .await
            .into_iter()
            .map(|candidate| candidate.row_id)
            .collect()
    }
}
