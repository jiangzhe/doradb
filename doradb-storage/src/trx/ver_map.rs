use crate::catalog::TableColumnLayout;
use crate::id::{RowID, TrxID};
use crate::trx::trx_is_committed;
use crate::trx::undo::RowUndoHead;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::mem;
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
    /// Checkpoint fixes columns and its prepared bitmap. Only exact-owner
    /// Lock/Delete completion, rollback, and recorded forward-slot restoration
    /// may change live row metadata; ordinary foreground writes must reroute.
    Transition = 2,
}

/// RowVersionMap is a page-level hash map to store
/// old versions of rows in row page.
/// It also contains modification counter and max STS
/// to speed up table scan with MVCC.
pub(crate) struct RowVersionMap {
    // Immutable reserved-slot identity survives eviction of the page body.
    start_row_id: RowID,
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
    // Equality-only version for optimistic frozen-page checkpoint plans.
    // Writers publish paired bumps around every mutation. The value is not a
    // seqlock: overlapping writers can leave either parity while still active.
    frozen_mutation_version: AtomicU64,
}

impl RowVersionMap {
    /// Create a new version map.
    #[inline]
    pub(crate) fn new(
        column_layout: Arc<TableColumnLayout>,
        start_row_id: RowID,
        max_size: usize,
    ) -> Self {
        let vec: Vec<_> = (0..max_size).map(|_| RwLock::new(None)).collect();
        RowVersionMap {
            entries: vec.into_boxed_slice(),
            start_row_id,
            column_layout,
            create_cts: AtomicU64::new(0),
            state: RwLock::new(RowPageState::Active),
            frozen_mutation_version: AtomicU64::new(0),
        }
    }

    /// Acquires version mutation access for a reserved row slot, including empty slots.
    #[inline]
    pub(crate) fn try_write_row(&self, row_id: RowID) -> Option<RowVersionWriteAccess<'_>> {
        let row_idx = usize::try_from(row_id.checked_sub(self.start_row_id)?).ok()?;
        if row_idx >= self.entries.len() {
            return None;
        }
        Some(RowVersionWriteAccess::with_state_guard(
            self,
            row_idx,
            self.read_state(),
        ))
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

    /// Loads the equality-only frozen-page mutation version.
    #[inline]
    pub(crate) fn frozen_mutation_version(&self) -> u64 {
        self.frozen_mutation_version.load(Ordering::Acquire)
    }

    /// Publishes the opening bump before a frozen-page mutation.
    #[inline]
    pub(crate) fn begin_frozen_mutation(&self) {
        let bumped = self.frozen_mutation_version.fetch_update(
            Ordering::AcqRel,
            Ordering::Acquire,
            |version| version.checked_add(1),
        );
        assert!(
            bumped.is_ok(),
            "frozen-page mutation version wrapped before opening bump"
        );
    }

    /// Publishes the closing bump before the mutation guards are released.
    #[inline]
    pub(crate) fn finish_frozen_mutation(&self) {
        let bumped = self.frozen_mutation_version.fetch_update(
            Ordering::Release,
            Ordering::Relaxed,
            |version| version.checked_add(1),
        );
        assert!(
            bumped.is_ok(),
            "frozen-page mutation version wrapped before closing bump"
        );
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

/// Version mutation access shared by physical row writes and metadata-only purge.
/// Locks are acquired in page-state then row order; both Frozen bumps occur
/// while both locks are held. No await, page acquisition, or callback belongs here.
pub(crate) struct RowVersionWriteAccess<'a> {
    // Field order releases the row latch before the page-state lock.
    guard: RowVersionWriteGuard<'a>,
    state_guard: RwLockReadGuard<'a, RowPageState>,
    frozen_version_map: Option<&'a RowVersionMap>,
}

impl<'a> RowVersionWriteAccess<'a> {
    /// Acquires the row latch using a state guard from the same version map.
    #[inline]
    pub(crate) fn with_state_guard(
        ver_map: &'a RowVersionMap,
        row_idx: usize,
        state_guard: RwLockReadGuard<'a, RowPageState>,
    ) -> Self {
        assert!(
            std::ptr::eq(RwLockReadGuard::rwlock(&state_guard), &ver_map.state),
            "row-version mutation requires the same map's state lock: row_idx={row_idx}"
        );
        let guard = ver_map.write_latch(row_idx);
        let frozen_version_map = if *state_guard == RowPageState::Frozen {
            // Final checkpoint state locking must drain this modifier through
            // its closing bump before comparing the optimistic plan version.
            ver_map.begin_frozen_mutation();
            Some(ver_map)
        } else {
            None
        };
        Self {
            guard,
            state_guard,
            frozen_version_map,
        }
    }

    /// Returns the page state retained for this mutation.
    #[inline]
    pub(crate) fn page_state(&self) -> RowPageState {
        *self.state_guard
    }

    /// Purge undo chain according to minimum active STS.
    /// This method removes out-of-date versions from the next list.
    /// The real deletion of undo logs is performed later.
    #[inline]
    pub(crate) fn purge_undo_chain(&mut self, min_active_sts: TrxID) {
        match &mut *self.guard {
            None => (),
            Some(undo_head) => {
                if undo_head.purge_ts >= min_active_sts {
                    // Another thread already prune this version chain.
                    return;
                }
                undo_head.purge_ts = min_active_sts;

                // Check whether the head can be purged.
                let ts = undo_head.ts();
                if trx_is_committed(ts) && ts < min_active_sts {
                    // The newest hot row-page image is older than every active
                    // snapshot. No reader can need older main or unique-index
                    // branches, so the whole undo head can be detached.
                    self.guard.take();
                    return;
                }
                let mut entry = undo_head.next.main.entry.as_mut();
                loop {
                    let mut entry_next = mem::take(&mut entry.next);
                    if entry_next.is_none() {
                        return;
                    }
                    let next = entry_next.as_mut().unwrap();
                    // purge main branch
                    if next.main.status.can_purge(min_active_sts) {
                        // main branch can be purged means index branches can also
                        // be purged, because index branches have smaller timestamps.
                        entry.next.take();
                        return;
                    }
                    // purge index branches
                    let mut idx = next.indexes.len();
                    // remove old links.
                    while idx > 0 {
                        idx -= 1;
                        if next.indexes[idx]
                            .purge_cts()
                            .is_some_and(|cts| cts < min_active_sts)
                        {
                            // This runtime unique branch only preserves an
                            // older owner for snapshots at or before its CTS.
                            // Once that CTS is below the oldest active
                            // snapshot, the latest mapping alone is enough.
                            next.indexes.swap_remove(idx);
                        }
                    }
                    // update back
                    entry.next = entry_next;
                    // go to next version, which should be main branch.
                    entry = entry.next.as_mut().unwrap().main.entry.as_mut();
                }
            }
        }
    }
}

impl Deref for RowVersionWriteAccess<'_> {
    type Target = Option<Box<RowUndoHead>>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl DerefMut for RowVersionWriteAccess<'_> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard
    }
}

impl Drop for RowVersionWriteAccess<'_> {
    #[inline]
    fn drop(&mut self) {
        if let Some(ver_map) = self.frozen_version_map {
            // Drop runs before either lock field is released.
            ver_map.finish_frozen_mutation();
        }
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
        StorageColumnFlags, StorageColumnSpec, StorageIndexFlags, StorageIndexKey, StorageIndexSpec,
    };
    use crate::catalog::{IndexSlot, ResolvedIndexKey, catalog_index_ref};
    use crate::id::TableID;
    use crate::trx::tests::shared_trx_status;
    use crate::trx::undo::{
        IndexBranch, IndexBranchTarget, MainBranch, NextRowUndo, OwnedRowUndo, RowUndoKind,
        UndoStatus,
    };
    use crate::trx::{MIN_ACTIVE_TRX_ID, NON_FOREGROUND_STMT_NO};
    use crate::value::{Val, ValKind};

    fn metadata() -> TableMetadata {
        TableMetadata::try_new(
            vec![StorageColumnSpec::new(
                ValKind::I64,
                StorageColumnFlags::empty(),
            )],
            Vec::<StorageIndexSpec>::new(),
        )
        .unwrap()
    }

    fn undo() -> OwnedRowUndo {
        OwnedRowUndo::new(
            NON_FOREGROUND_STMT_NO,
            TableID::new(1),
            None,
            RowID::new(100),
            RowUndoKind::Lock,
        )
    }

    #[test]
    fn test_row_slot_lookup_checked_bounds() {
        for start in [100, u64::MAX - 3] {
            let map = RowVersionMap::new(Arc::clone(&metadata().col), RowID::new(start), 4);
            for row_id in [start, start + 3] {
                assert!(map.try_write_row(RowID::new(row_id)).unwrap().is_none());
            }
            assert!(map.try_write_row(RowID::new(start - 1)).is_none());
            if let Some(end) = start.checked_add(4) {
                assert!(map.try_write_row(RowID::new(end)).is_none());
                assert!(map.try_write_row(RowID::new(u64::MAX)).is_none());
            }
        }
        let empty = RowVersionMap::new(Arc::clone(&metadata().col), RowID::new(100), 0);
        assert!(empty.try_write_row(RowID::new(100)).is_none());
    }

    #[test]
    fn test_version_purge_strict_horizon_and_paired_mutations() {
        for state in [
            RowPageState::Active,
            RowPageState::Frozen,
            RowPageState::Transition,
        ] {
            let map = RowVersionMap::new(Arc::clone(&metadata().col), RowID::new(100), 4);
            *map.write_state() = state;
            let owner = undo();
            *map.write_latch(0) = Some(Box::new(RowUndoHead::new(
                Arc::new(shared_trx_status(TrxID::new(20))),
                owner.leak(),
            )));
            for (round, horizon) in [20, 20, 21, 22].into_iter().enumerate() {
                let before = map.frozen_mutation_version();
                let mut access = map.try_write_row(RowID::new(100)).unwrap();
                assert_eq!(access.page_state(), state);
                assert_eq!(
                    map.frozen_mutation_version(),
                    before + u64::from(state == RowPageState::Frozen)
                );
                access.purge_undo_chain(TrxID::new(horizon));
                assert_eq!(access.is_none(), horizon > 20);
                if let Some(head) = access.as_ref() {
                    assert_eq!(head.purge_ts, TrxID::new(horizon));
                }
                // Neither a row reader nor final checkpoint state locking can pass.
                assert!(map.entries[0].try_read().is_none());
                assert!(map.state.try_write().is_none());
                drop(access);
                assert_eq!(
                    map.frozen_mutation_version(),
                    if state == RowPageState::Frozen {
                        (round as u64 + 1) * 2
                    } else {
                        0
                    }
                );
            }
        }
    }

    #[test]
    fn test_version_purge_main_suffix_status_compaction_and_index_branches() {
        let map = RowVersionMap::new(Arc::clone(&metadata().col), RowID::new(100), 4);
        let mut newest = undo();
        let mut middle = undo();
        let oldest = undo();
        middle.next = Some(NextRowUndo::new(MainBranch {
            entry: oldest.leak(),
            status: UndoStatus::Committed(TrxID::new(5)),
        }));
        let mut next = NextRowUndo::new(MainBranch {
            entry: middle.leak(),
            status: UndoStatus::Ref(Arc::new(shared_trx_status(TrxID::new(20)))),
        });
        for target in [
            IndexBranchTarget::Hot {
                cts: TrxID::new(9),
                entry: oldest.leak(),
            },
            IndexBranchTarget::ColdTerminal {
                delete_cts: Some(TrxID::new(9)),
            },
            IndexBranchTarget::Hot {
                cts: TrxID::new(10),
                entry: middle.leak(),
            },
            IndexBranchTarget::ColdTerminal {
                delete_cts: Some(TrxID::new(10)),
            },
            IndexBranchTarget::ColdTerminal { delete_cts: None },
        ] {
            next.indexes.push(IndexBranch::new(
                ResolvedIndexKey::new(catalog_index_ref(IndexSlot::new(0)), vec![Val::from(1i64)]),
                target,
                vec![],
            ));
        }
        newest.next = Some(next);
        *map.write_latch(0) = Some(Box::new(RowUndoHead::new(
            Arc::new(shared_trx_status(MIN_ACTIVE_TRX_ID + 1)),
            newest.leak(),
        )));
        map.try_write_row(RowID::new(100))
            .unwrap()
            .purge_undo_chain(TrxID::new(10));
        let next = newest.next.as_ref().unwrap();
        assert!(matches!(next.main.status, UndoStatus::Committed(cts) if cts == TrxID::new(20)));
        assert_eq!(next.indexes.len(), 3);
        assert!(
            next.indexes
                .iter()
                .all(|branch| branch.purge_cts().is_none_or(|cts| cts >= TrxID::new(10)))
        );
        assert!(middle.next.is_none());
        assert!(map.read_latch(0).is_some());
        map.try_write_row(RowID::new(100))
            .unwrap()
            .purge_undo_chain(TrxID::new(21));
        assert!(newest.next.is_none());
        assert!(map.read_latch(0).is_some());
        // Undo allocations remain owned here after their non-owning links detach.
        assert_eq!(oldest.row_id, RowID::new(100));
    }

    #[test]
    #[should_panic(expected = "same map's state lock")]
    fn test_version_mutation_rejects_foreign_state_guard() {
        let first = RowVersionMap::new(Arc::clone(&metadata().col), RowID::new(100), 1);
        let second = RowVersionMap::new(Arc::clone(&metadata().col), RowID::new(100), 1);
        let _access = RowVersionWriteAccess::with_state_guard(&first, 0, second.read_state());
    }

    #[test]
    fn test_row_version_map_create_cts() {
        let metadata = metadata();
        let map = RowVersionMap::new(Arc::clone(&metadata.col), RowID::new(100), 1);
        assert_eq!(map.create_cts(), TrxID::new(0));
        map.set_create_cts(TrxID::new(42));
        assert_eq!(map.create_cts(), TrxID::new(42));
    }

    #[test]
    fn test_row_version_map_state_transitions() {
        let metadata = metadata();
        let map = RowVersionMap::new(Arc::clone(&metadata.col), RowID::new(100), 1);
        assert_eq!(map.inspect_state(), RowPageState::Active);

        *map.write_state() = RowPageState::Frozen;
        assert_eq!(map.inspect_state(), RowPageState::Frozen);

        *map.write_state() = RowPageState::Transition;
        assert_eq!(map.inspect_state(), RowPageState::Transition);
    }

    #[test]
    fn test_row_version_map_stores_column_layout_arc_only() {
        let metadata = metadata();
        let (index_slot, indexed_metadata) = metadata
            .try_with_created_index(StorageIndexSpec::new(
                vec![StorageIndexKey::new(0)],
                StorageIndexFlags::UK,
            ))
            .unwrap();
        assert_eq!(index_slot.id(), crate::catalog::IndexID::new(0));
        assert_eq!(index_slot.slot(), crate::catalog::IndexSlot::new(0));
        assert!(Arc::ptr_eq(&metadata.col, &indexed_metadata.col));
        assert_ne!(
            metadata.idx.active_index_count(),
            indexed_metadata.idx.active_index_count()
        );

        let map = RowVersionMap::new(Arc::clone(&metadata.col), RowID::new(100), 1);
        assert!(Arc::ptr_eq(&map.column_layout, &metadata.col));
        assert_eq!(
            map.column_layout.col_count(),
            indexed_metadata.col.col_count()
        );
    }
}
