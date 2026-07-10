use crate::catalog::TableColumnLayout;
use crate::id::TrxID;
use crate::trx::undo::RowUndoHead;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Write state of a row page tracked by its version map.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RowPageState {
    /// Page accepts foreground row writes.
    Active = 0,
    /// Page is frozen and no longer accepts ordinary row growth.
    Frozen = 1,
    /// Page is being converted by checkpoint.
    Transition = 2,
}

/// RowVersionMap is a page-level hash map to store
/// old versions of rows in row page.
/// It also contains modification counter and max STS
/// to speed up table scan with MVCC.
pub(crate) struct RowVersionMap {
    // Fixed size array to store undo chains.
    // It wastes 16 bytes(one lock and one pointer)
    // for each row if no undo associated.
    entries: Box<[RwLock<Option<Box<RowUndoHead>>>]>,
    /// Column layout fixed when the version map is created.
    ///
    /// Currently it should be same as the table-file column layout because we
    /// do not support physical column-layout changes.
    pub(crate) column_layout: Arc<TableColumnLayout>,
    // Commit timestamp when this row page is created.
    create_cts: AtomicU64,
    // Row page state indicates the write status of the page.
    // It is guarded by rwlock so that checkpointer can block incoming
    // writers when switching frozen page to transition state.
    state: RwLock<RowPageState>,
}

impl RowVersionMap {
    /// Create a new version map.
    #[inline]
    pub(crate) fn new(column_layout: Arc<TableColumnLayout>, max_size: usize) -> Self {
        let vec: Vec<_> = (0..max_size).map(|_| RwLock::new(None)).collect();
        RowVersionMap {
            entries: vec.into_boxed_slice(),
            column_layout,
            create_cts: AtomicU64::new(0),
            state: RwLock::new(RowPageState::Active),
        }
    }

    /// Inspects the current row page state.
    #[inline]
    pub(crate) fn inspect_state(&self) -> RowPageState {
        *self.state.read()
    }

    /// Acquire shared lock of page state.
    #[inline]
    pub(crate) fn read_state(&self) -> RwLockReadGuard<'_, RowPageState> {
        self.state.read()
    }

    /// Acquire exclusive access to the page state.
    #[inline]
    pub(crate) fn write_state(&self) -> RwLockWriteGuard<'_, RowPageState> {
        self.state.write()
    }

    /// Set commit timestamp of page creation.
    #[inline]
    pub(crate) fn set_create_cts(&self, cts: TrxID) {
        self.create_cts.store(cts.as_u64(), Ordering::Release);
    }

    /// Returns commit timestamp of page creation.
    #[inline]
    pub(crate) fn create_cts(&self) -> TrxID {
        TrxID::new(self.create_cts.load(Ordering::Acquire))
    }

    /// Acquire a read latch on given row.
    #[inline]
    pub(crate) fn read_latch(&self, row_idx: usize) -> RowVersionReadGuard<'_> {
        let g = self.entries[row_idx].read();
        RowVersionReadGuard { g }
    }

    /// Acquire a write latch on given row.
    #[inline]
    pub(crate) fn write_latch(&self, row_idx: usize) -> RowVersionWriteGuard<'_> {
        let g = self.entries[row_idx].write();
        RowVersionWriteGuard { g }
    }
}

/// Shared guard over a row's undo head in the page version map.
pub(crate) struct RowVersionReadGuard<'a> {
    g: RwLockReadGuard<'a, Option<Box<RowUndoHead>>>,
}

impl<'a> Deref for RowVersionReadGuard<'a> {
    type Target = Option<Box<RowUndoHead>>;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.g
    }
}

/// Exclusive guard over a row's undo head in the page version map.
pub(crate) struct RowVersionWriteGuard<'a> {
    g: RwLockWriteGuard<'a, Option<Box<RowUndoHead>>>,
}

impl<'a> Deref for RowVersionWriteGuard<'a> {
    type Target = Option<Box<RowUndoHead>>;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.g
    }
}

impl<'a> DerefMut for RowVersionWriteGuard<'a> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.g
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::TableMetadata;
    use crate::catalog::spec::{
        ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexSpec,
    };
    use crate::value::ValKind;

    #[test]
    fn test_row_version_map_create_cts() {
        let metadata = TableMetadata::try_new(
            vec![ColumnSpec::new(
                "id",
                ValKind::I64,
                ColumnAttributes::empty(),
            )],
            Vec::<IndexSpec>::new(),
        )
        .expect("valid table metadata");
        let map = RowVersionMap::new(Arc::clone(&metadata.col), 1);
        assert_eq!(map.create_cts(), TrxID::new(0));
        map.set_create_cts(TrxID::new(42));
        assert_eq!(map.create_cts(), TrxID::new(42));
    }

    #[test]
    fn test_row_version_map_state_transitions() {
        let metadata = TableMetadata::try_new(
            vec![ColumnSpec::new(
                "id",
                ValKind::I64,
                ColumnAttributes::empty(),
            )],
            Vec::<IndexSpec>::new(),
        )
        .expect("valid table metadata");
        let map = RowVersionMap::new(Arc::clone(&metadata.col), 1);
        assert_eq!(map.inspect_state(), RowPageState::Active);

        *map.write_state() = RowPageState::Frozen;
        assert_eq!(map.inspect_state(), RowPageState::Frozen);

        *map.write_state() = RowPageState::Transition;
        assert_eq!(map.inspect_state(), RowPageState::Transition);
    }

    #[test]
    fn test_row_version_map_stores_column_layout_arc_only() {
        let metadata = TableMetadata::try_new(
            vec![ColumnSpec::new(
                "id",
                ValKind::I64,
                ColumnAttributes::empty(),
            )],
            Vec::<IndexSpec>::new(),
        )
        .expect("valid table metadata");
        let (index_no, indexed_metadata) = metadata
            .try_with_created_index(IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::UK))
            .unwrap();
        assert_eq!(index_no, 0);
        assert!(Arc::ptr_eq(&metadata.col, &indexed_metadata.col));
        assert_ne!(
            metadata.idx.active_index_count(),
            indexed_metadata.idx.active_index_count()
        );

        let map = RowVersionMap::new(Arc::clone(&metadata.col), 1);
        assert!(Arc::ptr_eq(&map.column_layout, &metadata.col));
        assert_eq!(
            map.column_layout.col_count(),
            indexed_metadata.col.col_count()
        );
    }
}
