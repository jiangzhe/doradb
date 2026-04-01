use crate::file::BlockID;
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
        root_block_id: BlockID,
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
    column_root_block_id: UnsafeCell<BlockID>,
}

impl BlockIndexRoot {
    /// Creates a new root router with initial pivot and column root page id.
    #[inline]
    pub fn new(pivot_row_id: RowID, column_root_block_id: BlockID) -> Self {
        BlockIndexRoot {
            latch: HybridLatch::new(),
            pivot_row_id: UnsafeCell::new(pivot_row_id),
            column_root_block_id: UnsafeCell::new(column_root_block_id),
        }
    }

    /// Guides one row-id lookup to row-store or column-store path.
    #[inline]
    pub fn guide(&self, row_id: RowID) -> BlockIndexRoute {
        let (pivot_row_id, root_block_id) = self.snapshot();
        if row_id < pivot_row_id {
            BlockIndexRoute::Column {
                pivot_row_id,
                root_block_id,
            }
        } else {
            BlockIndexRoute::Row
        }
    }

    /// Returns current column route metadata only if `row_id` is below pivot.
    ///
    /// This is used as a fallback path when row-store lookup misses.
    #[inline]
    pub fn try_column(&self, row_id: RowID) -> Option<(RowID, BlockID)> {
        let (pivot_row_id, root_block_id) = self.snapshot();
        (row_id < pivot_row_id).then_some((pivot_row_id, root_block_id))
    }

    /// Atomically updates pivot row id and column root page id.
    ///
    /// Called after checkpoint/persist updates on-disk column index state.
    #[inline]
    pub async fn update_column_root(&self, pivot_row_id: RowID, column_root_block_id: BlockID) {
        let _g = self.latch.exclusive_async().await;
        // SAFETY: protected by exclusive latch.
        unsafe {
            *self.pivot_row_id.get() = pivot_row_id;
            *self.column_root_block_id.get() = column_root_block_id;
        }
    }

    #[inline]
    pub fn snapshot(&self) -> (RowID, BlockID) {
        self.latch.optimistic_read(|| {
            // SAFETY: values are read under optimistic latch and validated
            // before being returned from `optimistic_read`.
            unsafe { (*self.pivot_row_id.get(), *self.column_root_block_id.get()) }
        })
    }

    #[inline]
    pub fn pivot_row_id(&self) -> RowID {
        self.snapshot().0
    }

    #[inline]
    pub fn column_root_block_id(&self) -> BlockID {
        self.snapshot().1
    }
}

// SAFETY: shared references only observe values through latch-validated reads
// or exclusive-latch-protected updates.
unsafe impl Sync for BlockIndexRoot {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_root_guide_and_try_column() {
        let root = BlockIndexRoot::new(1000, BlockID::from(77));
        match root.guide(999) {
            BlockIndexRoute::Column {
                pivot_row_id,
                root_block_id,
            } => {
                assert_eq!(pivot_row_id, 1000);
                assert_eq!(root_block_id, 77);
            }
            BlockIndexRoute::Row => panic!("unexpected row route"),
        }
        match root.guide(1000) {
            BlockIndexRoute::Row => {}
            BlockIndexRoute::Column { .. } => panic!("unexpected column route"),
        }
        assert_eq!(root.try_column(10), Some((1000, BlockID::from(77))));
        assert_eq!(root.try_column(1000), None);
    }

    #[test]
    fn test_root_update_column_root() {
        smol::block_on(async {
            let root = BlockIndexRoot::new(1000, BlockID::from(77));
            root.update_column_root(2000, BlockID::from(88)).await;

            match root.guide(1999) {
                BlockIndexRoute::Column {
                    pivot_row_id,
                    root_block_id,
                } => {
                    assert_eq!(pivot_row_id, 2000);
                    assert_eq!(root_block_id, 88);
                }
                BlockIndexRoute::Row => panic!("unexpected row route"),
            }
            assert_eq!(root.try_column(10), Some((2000, BlockID::from(88))));
            assert_eq!(root.try_column(2000), None);
        });
    }

    #[test]
    fn test_root_concurrent_guide_and_update() {
        smol::block_on(async {
            let root = Arc::new(BlockIndexRoot::new(1000, BlockID::from(77)));
            let reader_root = Arc::clone(&root);
            let writer_root = Arc::clone(&root);

            let reader = smol::spawn(async move {
                for i in 0..20_000u64 {
                    let row_id = i % 2_000;
                    match reader_root.guide(row_id) {
                        BlockIndexRoute::Column {
                            pivot_row_id,
                            root_block_id: _,
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
                    let root_block_id = BlockID::from(80 + i);
                    writer_root
                        .update_column_root(pivot_row_id, root_block_id)
                        .await;
                    let snapshot = writer_root.try_column(pivot_row_id - 1).unwrap();
                    assert_eq!(snapshot.0, pivot_row_id);
                    assert_eq!(snapshot.1, root_block_id);
                }
            });

            reader.await;
            writer.await;
        });
    }
}
