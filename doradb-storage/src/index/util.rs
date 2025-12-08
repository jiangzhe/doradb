use crate::buffer::page::PageID;
use crate::row::{INVALID_ROW_ID, RowID};
use crate::trx::sys::TransactionSystem;
use doradb_catalog::TableID;

/// Value that can be masked as deleted.
pub trait Maskable: Copy + PartialEq + Eq {
    const INVALID_VALUE: Self;

    /// Mask given value as deleted.
    fn deleted(self) -> Self;

    /// Returns value without delete mask.
    fn value(self) -> Self;

    /// Returns whether this value is masked as deleted.
    fn is_deleted(self) -> bool;
}

const U64_DELETE_BIT: u64 = 1u64 << 63;

impl Maskable for RowID {
    const INVALID_VALUE: Self = INVALID_ROW_ID;

    #[inline]
    fn deleted(self) -> Self {
        self | U64_DELETE_BIT
    }

    #[inline]
    fn value(self) -> Self {
        self & !U64_DELETE_BIT
    }

    #[inline]
    fn is_deleted(self) -> bool {
        self & U64_DELETE_BIT != 0
    }
}

/// Statistics of space used by nodes.
#[derive(Debug, Default)]
pub struct SpaceStatistics {
    pub nodes: usize,
    pub total_space: usize,
    pub used_space: usize,
    pub effective_space: usize,
}

pub(super) struct ParentPosition<G> {
    pub(super) g: G,
    // -1 means lower fence in btree node.
    pub(super) idx: isize,
}

pub(super) struct RedoLogPageCommitter {
    trx_sys: &'static TransactionSystem,
    table_id: TableID,
}

impl Clone for RedoLogPageCommitter {
    #[inline]
    fn clone(&self) -> Self {
        RedoLogPageCommitter {
            trx_sys: self.trx_sys,
            table_id: self.table_id,
        }
    }
}

impl RedoLogPageCommitter {
    #[inline]
    pub fn new(trx_sys: &'static TransactionSystem, table_id: TableID) -> Self {
        RedoLogPageCommitter { trx_sys, table_id }
    }

    pub async fn commit_row_page(&self, page_id: PageID, start_row_id: RowID, end_row_id: RowID) {
        let mut trx = self.trx_sys.begin_sys_trx();
        let table_id = self.table_id;
        // Once a row page is added to block index, we start
        // a new internal transaction and log its information.
        trx.create_row_page(table_id, page_id, start_row_id, end_row_id);
        let res = self.trx_sys.commit_sys(trx).await;
        assert!(res.is_ok());
    }
}
