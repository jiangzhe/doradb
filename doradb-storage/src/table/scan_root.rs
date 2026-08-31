use super::TableRootSnapshot;
use crate::file::table_file::ActiveRoot;
use crate::id::{BlockID, RowID};

/// Statically dispatched table-scan access to an authority-bound root.
pub(crate) trait TableScanRootView {
    /// Returns the captured boundary between persisted and hot rows.
    fn pivot_row_id(&self) -> RowID;

    /// Returns the captured persisted column block-index root.
    fn column_block_index_root(&self) -> BlockID;
}

/// Lifetime-free scan-only fields copied from one table active root.
///
/// The stored artifact intentionally exposes no root fields by itself. A
/// registered-snapshot checkout borrows it as an exact
/// [`CheckedOutTableScanRoot`] before scan planning can use those fields.
pub(crate) struct OwnedTableScanRoot {
    pivot_row_id: RowID,
    column_block_index_root: BlockID,
}

impl OwnedTableScanRoot {
    /// Copy the scan-only projection from one active-root observation.
    #[inline]
    pub(super) fn from_active_root(root: &ActiveRoot) -> Self {
        Self {
            pivot_row_id: root.pivot_row_id,
            column_block_index_root: root.column_block_index_root,
        }
    }
}

/// Usable scan-root view borrowed from the checkout that pins its owner.
pub(crate) struct CheckedOutTableScanRoot<'checkout> {
    root: &'checkout OwnedTableScanRoot,
}

impl CheckedOutTableScanRoot<'_> {
    /// Borrow one stored root through the checkout that pins its owner.
    #[inline]
    pub(crate) fn new(root: &OwnedTableScanRoot) -> CheckedOutTableScanRoot<'_> {
        CheckedOutTableScanRoot { root }
    }
}

impl TableScanRootView for CheckedOutTableScanRoot<'_> {
    #[inline]
    fn pivot_row_id(&self) -> RowID {
        self.root.pivot_row_id
    }

    #[inline]
    fn column_block_index_root(&self) -> BlockID {
        self.root.column_block_index_root
    }
}

impl TableScanRootView for TableRootSnapshot<'_> {
    #[inline]
    fn pivot_row_id(&self) -> RowID {
        self.pivot_row_id()
    }

    #[inline]
    fn column_block_index_root(&self) -> BlockID {
        self.column_block_index_root()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{StorageColumnFlags, StorageColumnSpec, TableMetadata};
    use crate::file::cow_file::SUPER_BLOCK_ID;
    use crate::id::TrxID;
    use crate::value::ValKind;
    use std::sync::Arc;

    macro_rules! assert_not_impl {
        ($ty:ty: $trait:path) => {
            const _: fn() = || {
                trait AmbiguousIfImpl<A> {
                    fn check() {}
                }
                impl<T: ?Sized> AmbiguousIfImpl<()> for T {}
                struct Invalid;
                impl<T: ?Sized + $trait> AmbiguousIfImpl<Invalid> for T {}
                <$ty as AmbiguousIfImpl<_>>::check();
            };
        };
    }

    assert_not_impl!(OwnedTableScanRoot: TableScanRootView);

    const _: fn() = || {
        fn assert_impl<T: TableScanRootView>() {}
        assert_impl::<CheckedOutTableScanRoot<'_>>();
    };

    #[test]
    fn checked_out_root_exposes_exact_scan_projection() {
        let metadata = Arc::new(
            TableMetadata::try_new(
                vec![StorageColumnSpec::new(
                    ValKind::U64,
                    StorageColumnFlags::empty(),
                )],
                vec![],
            )
            .unwrap(),
        );
        let mut active = ActiveRoot::new(TrxID::new(7), 256, metadata);
        active.pivot_row_id = RowID::new(123);
        active.column_block_index_root = BlockID::new(17);

        let owned = OwnedTableScanRoot::from_active_root(&active);
        let checked_out = CheckedOutTableScanRoot { root: &owned };

        assert_eq!(checked_out.pivot_row_id(), RowID::new(123));
        assert_eq!(checked_out.column_block_index_root(), BlockID::new(17));
        assert_ne!(checked_out.column_block_index_root(), SUPER_BLOCK_ID);
    }
}
