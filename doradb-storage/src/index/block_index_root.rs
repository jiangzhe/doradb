use crate::buffer::page::PageID;
use crate::latch::HybridLatch;
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
        let (pivot_row_id, root_page_id) = self.snapshot();
        if row_id < pivot_row_id {
            BlockIndexRoute::Column {
                pivot_row_id,
                root_page_id,
            }
        } else {
            BlockIndexRoute::Row
        }
    }

    /// Returns current column route metadata only if `row_id` is below pivot.
    ///
    /// This is used as a fallback path when row-store lookup misses.
    #[inline]
    pub fn try_column(&self, row_id: RowID) -> Option<(RowID, PageID)> {
        let (pivot_row_id, root_page_id) = self.snapshot();
        (row_id < pivot_row_id).then_some((pivot_row_id, root_page_id))
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
    fn snapshot(&self) -> (RowID, PageID) {
        self.latch.optimistic_read(|| {
            // SAFETY: values are read under optimistic latch and validated
            // before being returned from `optimistic_read`.
            unsafe { (*self.pivot_row_id.get(), *self.column_root_page_id.get()) }
        })
    }
}

unsafe impl Send for BlockIndexRoot {}
unsafe impl Sync for BlockIndexRoot {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

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

    #[test]
    fn test_root_concurrent_guide_and_update() {
        smol::block_on(async {
            let root = Arc::new(BlockIndexRoot::new(1000, 77));
            let reader_root = Arc::clone(&root);
            let writer_root = Arc::clone(&root);

            let reader = smol::spawn(async move {
                for i in 0..20_000u64 {
                    let row_id = i % 2_000;
                    match reader_root.guide(row_id) {
                        BlockIndexRoute::Column {
                            pivot_row_id,
                            root_page_id: _,
                        } => assert!(row_id < pivot_row_id),
                        BlockIndexRoute::Row => {
                            if let Some((pivot_row_id, _)) = reader_root.try_column(row_id) {
                                assert!(row_id < pivot_row_id);
                            }
                        }
                    }
                }
            });

            let writer = smol::spawn(async move {
                for i in 0..2_000u64 {
                    let pivot_row_id = 600 + (i % 800);
                    let root_page_id = 80 + i;
                    writer_root
                        .update_column_root(pivot_row_id, root_page_id)
                        .await;
                    let snapshot = writer_root.try_column(pivot_row_id - 1).unwrap();
                    assert_eq!(snapshot.0, pivot_row_id);
                    assert_eq!(snapshot.1, root_page_id);
                }
            });

            reader.await;
            writer.await;
        });
    }
}
