use crate::buffer::page::PageID;
use crate::latch::{HybridGuard, HybridLatch};
use crate::row::RowID;
use std::cell::UnsafeCell;

/// Route returned by `BlockIndexRoot::guide`.
pub enum BlockIndexRoute {
    /// Lookup should continue in persisted column-store index.
    Column {
        /// First row id stored in in-memory row store.
        pivot_row_id: RowID,
        /// Root page id of current column block index.
        root_page_id: PageID,
    },
    /// Lookup should continue in in-memory row-store index.
    Row,
}

/// Root routing metadata for hybrid row/column block index.
///
/// This structure stores:
/// - current `pivot_row_id` (minimum row id in row store)
/// - current root page id of column block index
///
/// Both values are protected by a `HybridLatch` so readers can do optimistic
/// lock-free routing while checkpoint updates can atomically swap both values.
pub struct BlockIndexRoot {
    // Latch protecting pivot and column-root updates.
    latch: HybridLatch,
    // Minimum row id of row-store pages.
    pivot_row_id: UnsafeCell<RowID>,
    // Root page id of column block index.
    column_root_page_id: UnsafeCell<PageID>,
}

impl BlockIndexRoot {
    /// Creates a new root router with initial pivot and column root page id.
    #[inline]
    pub fn new(pivot_row_id: RowID, column_root_page_id: PageID) -> Self {
        BlockIndexRoot {
            latch: HybridLatch::new(),
            pivot_row_id: UnsafeCell::new(pivot_row_id),
            column_root_page_id: UnsafeCell::new(column_root_page_id),
        }
    }

    /// Guides one row-id lookup to row-store or column-store path.
    #[inline]
    pub fn guide(&self, row_id: RowID) -> BlockIndexRoute {
        loop {
            let g = self.optimistic_guard();
            let pivot_row_id = self.pivot_row_id_value();
            if row_id < pivot_row_id {
                let root_page_id = self.column_root_page_id_value();
                if g.validate() {
                    return BlockIndexRoute::Column {
                        pivot_row_id,
                        root_page_id,
                    };
                }
                continue;
            }
            if g.validate() {
                return BlockIndexRoute::Row;
            }
        }
    }

    /// Returns current column route metadata only if `row_id` is below pivot.
    ///
    /// This is used as a fallback path when row-store lookup misses.
    #[inline]
    pub fn try_column(&self, row_id: RowID) -> Option<(RowID, PageID)> {
        loop {
            let g = self.optimistic_guard();
            let pivot_row_id = self.pivot_row_id_value();
            if row_id < pivot_row_id {
                let root_page_id = self.column_root_page_id_value();
                if g.validate() {
                    return Some((pivot_row_id, root_page_id));
                }
                continue;
            }
            return None;
        }
    }

    /// Atomically updates pivot row id and column root page id.
    ///
    /// Called after checkpoint/persist updates on-disk column index state.
    #[inline]
    pub async fn update_column_root(&self, pivot_row_id: RowID, column_root_page_id: PageID) {
        let _g = self.latch.exclusive_async().await;
        // SAFETY: protected by exclusive latch.
        unsafe {
            *self.pivot_row_id.get() = pivot_row_id;
            *self.column_root_page_id.get() = column_root_page_id;
        }
    }

    #[inline]
    fn optimistic_guard(&self) -> HybridGuard<'_> {
        self.latch.optimistic_spin()
    }

    #[inline]
    fn pivot_row_id_value(&self) -> RowID {
        // SAFETY: reads validated by optimistic latch version checks.
        unsafe { *self.pivot_row_id.get() }
    }

    #[inline]
    fn column_root_page_id_value(&self) -> PageID {
        // SAFETY: reads validated by optimistic latch version checks.
        unsafe { *self.column_root_page_id.get() }
    }
}

unsafe impl Send for BlockIndexRoot {}
unsafe impl Sync for BlockIndexRoot {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_root_guide_and_try_column() {
        let root = BlockIndexRoot::new(1000, 77);
        match root.guide(999) {
            BlockIndexRoute::Column {
                pivot_row_id,
                root_page_id,
            } => {
                assert_eq!(pivot_row_id, 1000);
                assert_eq!(root_page_id, 77);
            }
            BlockIndexRoute::Row => panic!("unexpected row route"),
        }
        match root.guide(1000) {
            BlockIndexRoute::Row => {}
            BlockIndexRoute::Column { .. } => panic!("unexpected column route"),
        }
        assert_eq!(root.try_column(10), Some((1000, 77)));
        assert_eq!(root.try_column(1000), None);
    }

    #[test]
    fn test_root_update_column_root() {
        smol::block_on(async {
            let root = BlockIndexRoot::new(1000, 77);
            root.update_column_root(2000, 88).await;

            match root.guide(1999) {
                BlockIndexRoute::Column {
                    pivot_row_id,
                    root_page_id,
                } => {
                    assert_eq!(pivot_row_id, 2000);
                    assert_eq!(root_page_id, 88);
                }
                BlockIndexRoute::Row => panic!("unexpected row route"),
            }
            assert_eq!(root.try_column(10), Some((2000, 88)));
            assert_eq!(root.try_column(2000), None);
        });
    }
}
