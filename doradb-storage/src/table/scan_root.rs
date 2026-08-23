use super::TableRootSnapshot;
use crate::file::table_file::ActiveRoot;
use crate::id::{BlockID, RowID, TrxID};

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
/// future registered-snapshot checkout will borrow it as an exact
/// [`CheckedOutTableScanRoot`] before scan planning can use those fields.
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "Phase 2 will store owned scan roots in the registered snapshot core"
    )
)]
pub(crate) struct OwnedTableScanRoot {
    root_ts: TrxID,
    effective_ts: TrxID,
    pivot_row_id: RowID,
    column_block_index_root: BlockID,
}

impl OwnedTableScanRoot {
    /// Copy the scan-only projection from one active-root observation.
    #[inline]
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Phase 2 will capture owned roots during snapshot preparation"
        )
    )]
    pub(crate) fn from_active_root(root: &ActiveRoot) -> Self {
        Self {
            root_ts: root.root_ts,
            effective_ts: root.effective_ts(),
            pivot_row_id: root.pivot_row_id,
            column_block_index_root: root.column_block_index_root,
        }
    }
}

/// Usable scan-root view borrowed from the checkout that pins its owner.
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "Phase 2 will return checked-out roots from the registered snapshot checkout"
    )
)]
pub(crate) struct CheckedOutTableScanRoot<'checkout> {
    root: &'checkout OwnedTableScanRoot,
}

impl CheckedOutTableScanRoot<'_> {
    /// Returns the durable timestamp carried by the captured root.
    #[inline]
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Phase 2 will use checked-out root timestamps for diagnostics"
        )
    )]
    pub(crate) fn root_ts(&self) -> TrxID {
        self.root.root_ts
    }

    /// Returns when the captured root became observable at runtime.
    #[inline]
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Phase 2 will use checked-out root timestamps for diagnostics"
        )
    )]
    pub(crate) fn effective_ts(&self) -> TrxID {
        self.root.effective_ts
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
    use crate::catalog::{ColumnAttributes, ColumnSpec, TableMetadata};
    use crate::file::cow_file::SUPER_BLOCK_ID;
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
    fn checked_out_root_exposes_exact_owned_projection() {
        let metadata = Arc::new(
            TableMetadata::try_new(
                vec![ColumnSpec::new(
                    "c0",
                    ValKind::U64,
                    ColumnAttributes::empty(),
                )],
                vec![],
            )
            .unwrap(),
        );
        let mut active = ActiveRoot::new(TrxID::new(7), 256, metadata);
        active.install_effective_ts(TrxID::new(11));
        active.pivot_row_id = RowID::new(123);
        active.column_block_index_root = BlockID::new(17);

        let owned = OwnedTableScanRoot::from_active_root(&active);
        let checked_out = CheckedOutTableScanRoot { root: &owned };

        assert_eq!(checked_out.root_ts(), TrxID::new(7));
        assert_eq!(checked_out.effective_ts(), TrxID::new(11));
        assert_eq!(checked_out.pivot_row_id(), RowID::new(123));
        assert_eq!(checked_out.column_block_index_root(), BlockID::new(17));
        assert_ne!(checked_out.column_block_index_root(), SUPER_BLOCK_ID);
    }
}
