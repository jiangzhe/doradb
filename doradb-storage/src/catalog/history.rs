use super::TableMetadata;
use crate::id::TrxID;
use crate::table::{Table, TableRedoReplayFloor};
use std::sync::Arc;

/// One superseded live logical metadata version.
pub(crate) struct TableMetadataVersion {
    effective_cts: TrxID,
    metadata: Arc<TableMetadata>,
}

impl TableMetadataVersion {
    #[inline]
    fn new(effective_cts: TrxID, metadata: Arc<TableMetadata>) -> Self {
        Self {
            effective_cts,
            metadata,
        }
    }
}

/// Current authoritative logical state for one user table.
#[derive(Clone)]
pub(crate) enum CurrentTableState {
    /// Current live metadata and its executable runtime.
    Live {
        /// Commit timestamp at which this metadata became effective.
        effective_cts: TrxID,
        /// Metadata pointer-identical to the table's current runtime layout.
        metadata: Arc<TableMetadata>,
        /// Current foreground table runtime.
        table: Arc<Table>,
    },
    /// Terminal logical DROP TABLE tombstone.
    Dropped {
        /// Commit timestamp of the DROP TABLE operation.
        effective_cts: TrxID,
    },
}

impl CurrentTableState {
    /// Returns the current state's effective commit timestamp.
    #[inline]
    pub(crate) fn effective_cts(&self) -> TrxID {
        match self {
            CurrentTableState::Live { effective_cts, .. }
            | CurrentTableState::Dropped { effective_cts } => *effective_cts,
        }
    }

    /// Returns the current foreground table runtime when live.
    #[inline]
    pub(crate) fn live_table(&self) -> Option<&Arc<Table>> {
        match self {
            CurrentTableState::Live { table, .. } => Some(table),
            CurrentTableState::Dropped { .. } => None,
        }
    }
}

/// A live logical metadata result selected for one transaction snapshot.
pub(crate) struct ResolvedLiveMetadata {
    effective_cts: TrxID,
    metadata: Arc<TableMetadata>,
}

impl ResolvedLiveMetadata {
    /// Returns the commit timestamp at which the selected metadata became effective.
    #[inline]
    pub(crate) fn effective_cts(&self) -> TrxID {
        self.effective_cts
    }

    /// Returns the selected logical table metadata.
    #[inline]
    pub(crate) fn metadata(&self) -> &Arc<TableMetadata> {
        &self.metadata
    }
}

/// Transaction-snapshot-visible logical state for one user table.
pub(crate) enum ResolvedVisibleTableMetadata {
    /// The selected state is live logical metadata.
    Live(ResolvedLiveMetadata),
    /// The selected state is the terminal DROP TABLE tombstone.
    Tombstone {
        /// Commit timestamp of the DROP TABLE operation.
        effective_cts: TrxID,
    },
}

/// Short volatile metadata history for one user table.
pub(crate) struct TableHistoryEntry {
    /// Superseded live versions, oldest to newest.
    versions: Vec<TableMetadataVersion>,
    current: CurrentTableState,
}

impl TableHistoryEntry {
    #[inline]
    fn new_live(effective_cts: TrxID, metadata: Arc<TableMetadata>, table: Arc<Table>) -> Self {
        assert_current_layout_metadata(&table, &metadata);
        Self {
            versions: Vec::new(),
            current: CurrentTableState::Live {
                effective_cts,
                metadata,
                table,
            },
        }
    }

    #[inline]
    fn resolve_visible(&self, sts: TrxID) -> Option<ResolvedVisibleTableMetadata> {
        if self.current.effective_cts() < sts {
            return Some(match &self.current {
                CurrentTableState::Live {
                    effective_cts,
                    metadata,
                    ..
                } => ResolvedVisibleTableMetadata::Live(ResolvedLiveMetadata {
                    effective_cts: *effective_cts,
                    metadata: Arc::clone(metadata),
                }),
                CurrentTableState::Dropped { effective_cts } => {
                    ResolvedVisibleTableMetadata::Tombstone {
                        effective_cts: *effective_cts,
                    }
                }
            });
        }

        self.versions
            .iter()
            .rev()
            .find(|version| version.effective_cts < sts)
            .map(|version| {
                ResolvedVisibleTableMetadata::Live(ResolvedLiveMetadata {
                    effective_cts: version.effective_cts,
                    metadata: Arc::clone(&version.metadata),
                })
            })
    }

    #[inline]
    fn resolve_current(&self) -> CurrentTableState {
        self.current.clone()
    }

    #[inline]
    fn publish_live(
        &mut self,
        effective_cts: TrxID,
        expected_table: &Arc<Table>,
        expected_metadata: &Arc<TableMetadata>,
        new_metadata: Arc<TableMetadata>,
    ) -> bool {
        let CurrentTableState::Live {
            effective_cts: current_cts,
            metadata: current_metadata,
            table,
        } = &self.current
        else {
            return false;
        };
        if effective_cts <= *current_cts
            || !Arc::ptr_eq(table, expected_table)
            || !Arc::ptr_eq(current_metadata, expected_metadata)
        {
            return false;
        }
        let installed_layout = expected_table.layout_snapshot();
        if !Arc::ptr_eq(installed_layout.metadata_arc(), &new_metadata) {
            return false;
        }

        let old_cts = *current_cts;
        let old_metadata = Arc::clone(current_metadata);
        self.versions
            .push(TableMetadataVersion::new(old_cts, old_metadata));
        self.current = CurrentTableState::Live {
            effective_cts,
            metadata: new_metadata,
            table: Arc::clone(expected_table),
        };
        self.assert_valid();
        true
    }

    #[inline]
    fn publish_drop(&mut self, effective_cts: TrxID, expected_table: &Arc<Table>) -> bool {
        let CurrentTableState::Live {
            effective_cts: current_cts,
            metadata,
            table,
        } = &self.current
        else {
            return false;
        };
        if effective_cts <= *current_cts || !Arc::ptr_eq(table, expected_table) {
            return false;
        }
        let installed_layout = expected_table.layout_snapshot();
        if !Arc::ptr_eq(installed_layout.metadata_arc(), metadata) {
            return false;
        }

        self.versions.push(TableMetadataVersion::new(
            *current_cts,
            Arc::clone(metadata),
        ));
        self.current = CurrentTableState::Dropped { effective_cts };
        self.assert_valid();
        true
    }

    /// Purges obsolete history and returns whether a dropped history can be removed.
    #[inline]
    fn purge(&mut self, min_active_sts: TrxID) -> bool {
        match &self.current {
            CurrentTableState::Dropped { effective_cts } => *effective_cts < min_active_sts,
            CurrentTableState::Live { effective_cts, .. } => {
                let obsolete_prefix_len = if *effective_cts < min_active_sts {
                    self.versions.len()
                } else {
                    // The newest version below the horizon is the predecessor
                    // visible to the oldest active snapshot, so retain it.
                    self.versions
                        .iter()
                        .rposition(|version| version.effective_cts < min_active_sts)
                        .unwrap_or(0)
                };
                self.versions.drain(..obsolete_prefix_len);
                self.assert_valid();
                false
            }
        }
    }

    #[inline]
    fn assert_valid(&self) {
        assert!(
            self.versions
                .windows(2)
                .all(|pair| pair[0].effective_cts < pair[1].effective_cts),
            "table metadata history invariant violated: historical versions are not strictly ordered"
        );
        assert!(
            self.versions
                .iter()
                .all(|version| version.effective_cts < self.current.effective_cts()),
            "table metadata history invariant violated: historical version is not older than current state"
        );
        if let CurrentTableState::Live {
            metadata, table, ..
        } = &self.current
        {
            assert_current_layout_metadata(table, metadata);
        } else {
            assert!(
                !self.versions.is_empty(),
                "table metadata history invariant violated: online tombstone has no live predecessor"
            );
        }
    }

    /// Returns the number of superseded metadata versions for tests.
    #[cfg(test)]
    #[inline]
    pub(crate) fn version_count(&self) -> usize {
        self.versions.len()
    }
}

/// Operational state retained independently from authoritative logical history.
pub(crate) enum DroppedTableOperationalState {
    /// Runtime retained until the strict active-STS horizon permits one-way detach.
    Runtime {
        table: Arc<Table>,
        drop_cts: TrxID,
        replay_floor: TableRedoReplayFloor,
    },
    /// Lightweight replay/file-cleanup floor retained after runtime destruction.
    Floor {
        drop_cts: TrxID,
        replay_floor: TableRedoReplayFloor,
    },
}

/// Orthogonal logical-history and dropped-runtime registry slots.
pub(crate) struct UserTableEntry {
    history: Option<TableHistoryEntry>,
    dropped: Option<DroppedTableOperationalState>,
}

impl UserTableEntry {
    /// Creates one live current entry.
    #[inline]
    pub(crate) fn new_live(
        effective_cts: TrxID,
        metadata: Arc<TableMetadata>,
        table: Arc<Table>,
    ) -> Self {
        let entry = Self {
            history: Some(TableHistoryEntry::new_live(effective_cts, metadata, table)),
            dropped: None,
        };
        entry.assert_valid();
        entry
    }

    /// Creates one recovery-retained dropped replay floor without logical history.
    #[inline]
    pub(crate) fn new_dropped_floor(drop_cts: TrxID, replay_floor: TableRedoReplayFloor) -> Self {
        let entry = Self {
            history: None,
            dropped: Some(DroppedTableOperationalState::Floor {
                drop_cts,
                replay_floor,
            }),
        };
        entry.assert_valid();
        entry
    }

    /// Resolves logical metadata using strict row-MVCC visibility.
    #[inline]
    pub(crate) fn resolve_visible(&self, sts: TrxID) -> Option<ResolvedVisibleTableMetadata> {
        self.history
            .as_ref()
            .and_then(|history| history.resolve_visible(sts))
    }

    /// Clones the direct current logical state without consulting history.
    #[inline]
    pub(crate) fn resolve_current(&self) -> Option<CurrentTableState> {
        self.history
            .as_ref()
            .map(TableHistoryEntry::resolve_current)
    }

    /// Returns the current foreground runtime without consulting operational state.
    #[inline]
    pub(crate) fn current_live_table(&self) -> Option<Arc<Table>> {
        self.resolve_current()
            .and_then(|current| current.live_table().map(Arc::clone))
    }

    /// Borrows the current foreground runtime while its catalog entry is guarded.
    #[inline]
    pub(crate) fn current_live_table_ref(&self) -> Option<&Table> {
        self.history
            .as_ref()
            .and_then(|history| history.current.live_table())
            .map(Arc::as_ref)
    }

    /// Returns a live or retained dropped runtime for purge-owned physical work.
    #[inline]
    pub(crate) fn runtime_for_purge(&self) -> Option<Arc<Table>> {
        if let Some(table) = self.current_live_table() {
            return Some(table);
        }
        match &self.dropped {
            Some(DroppedTableOperationalState::Runtime { table, .. }) => Some(Arc::clone(table)),
            Some(DroppedTableOperationalState::Floor { .. }) | None => None,
        }
    }

    /// Publishes a new current live metadata version after layout installation.
    #[inline]
    pub(crate) fn publish_live(
        &mut self,
        effective_cts: TrxID,
        expected_table: &Arc<Table>,
        expected_metadata: &Arc<TableMetadata>,
        new_metadata: Arc<TableMetadata>,
    ) -> bool {
        if self.dropped.is_some() {
            return false;
        }
        let published = self.history.as_mut().is_some_and(|history| {
            history.publish_live(
                effective_cts,
                expected_table,
                expected_metadata,
                new_metadata,
            )
        });
        if published {
            self.assert_valid();
        }
        published
    }

    /// Publishes a terminal tombstone and installs the sibling retained runtime.
    #[inline]
    pub(crate) fn publish_drop(
        &mut self,
        effective_cts: TrxID,
        table: Arc<Table>,
        replay_floor: TableRedoReplayFloor,
    ) -> bool {
        if self.dropped.is_some() {
            return false;
        }
        let Some(history) = &mut self.history else {
            return false;
        };
        if !history.publish_drop(effective_cts, &table) {
            return false;
        }
        self.dropped = Some(DroppedTableOperationalState::Runtime {
            table,
            drop_cts: effective_cts,
            replay_floor,
        });
        self.assert_valid();
        true
    }

    /// Copies the current live runtime's replay floor.
    #[inline]
    pub(crate) fn live_replay_floor(
        &self,
        checkpointed_silent: Option<TableRedoReplayFloor>,
    ) -> Option<TableRedoReplayFloor> {
        self.current_live_table_ref().map(|table| {
            let root_floor = table.redo_replay_floor_snapshot();
            super::effective_table_redo_replay_floor(root_floor, checkpointed_silent)
        })
    }

    /// Copies a retained dropped operational replay floor.
    #[inline]
    pub(crate) fn dropped_replay_floor(&self) -> Option<(TrxID, TableRedoReplayFloor)> {
        match &self.dropped {
            Some(
                DroppedTableOperationalState::Runtime {
                    drop_cts,
                    replay_floor,
                    ..
                }
                | DroppedTableOperationalState::Floor {
                    drop_cts,
                    replay_floor,
                },
            ) => Some((*drop_cts, *replay_floor)),
            None => None,
        }
    }

    /// Converts one eligible retained runtime into a lightweight floor candidate.
    #[inline]
    pub(crate) fn take_dropped_runtime(
        &mut self,
        min_active_sts: TrxID,
    ) -> Option<(Arc<Table>, TrxID, TableRedoReplayFloor)> {
        let Some(DroppedTableOperationalState::Runtime {
            table,
            drop_cts,
            replay_floor,
        }) = &self.dropped
        else {
            return None;
        };
        if *drop_cts >= min_active_sts {
            return None;
        }
        let result = (Arc::clone(table), *drop_cts, *replay_floor);
        self.dropped = Some(DroppedTableOperationalState::Floor {
            drop_cts: result.1,
            replay_floor: result.2,
        });
        self.assert_valid();
        Some(result)
    }

    /// Returns a retained floor eligible for file-cleanup queue seeding.
    #[inline]
    pub(crate) fn dropped_floor(&self) -> Option<(TrxID, TableRedoReplayFloor)> {
        match &self.dropped {
            Some(DroppedTableOperationalState::Floor {
                drop_cts,
                replay_floor,
            }) => Some((*drop_cts, *replay_floor)),
            Some(DroppedTableOperationalState::Runtime { .. }) | None => None,
        }
    }

    /// Clears one exact dropped floor.
    #[inline]
    pub(crate) fn remove_dropped_floor(
        &mut self,
        drop_cts: TrxID,
        replay_floor: TableRedoReplayFloor,
    ) -> bool {
        let matches = matches!(
            &self.dropped,
            Some(DroppedTableOperationalState::Floor {
                drop_cts: observed_drop_cts,
                replay_floor: observed_replay_floor,
            }) if *observed_drop_cts == drop_cts && *observed_replay_floor == replay_floor
        );
        if matches {
            self.dropped = None;
            self.assert_valid();
        }
        matches
    }

    /// Returns whether operational cleanup state remains.
    #[inline]
    pub(crate) fn has_dropped_operational_state(&self) -> bool {
        self.dropped.is_some()
    }

    /// Purges logical metadata against the shared transaction horizon.
    #[inline]
    pub(crate) fn purge_history(&mut self, min_active_sts: TrxID) {
        let remove_history = self
            .history
            .as_mut()
            .is_some_and(|history| history.purge(min_active_sts));
        if remove_history {
            self.history = None;
        }
        self.assert_valid();
    }

    /// Returns whether both logical and operational slots are absent.
    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.history.is_none() && self.dropped.is_none()
    }

    /// Consumes an offline recovery live entry and returns its sole runtime.
    #[inline]
    pub(crate) fn into_recovery_live_table(self) -> Arc<Table> {
        assert!(
            self.dropped.is_none(),
            "recovery live removal invariant violated: dropped operational state is present"
        );
        let history = self.history.unwrap_or_else(|| {
            panic!("recovery live removal invariant violated: logical history is absent")
        });
        assert!(
            history.versions.is_empty(),
            "recovery live removal invariant violated: superseded metadata history is present"
        );
        match history.current {
            CurrentTableState::Live { table, .. } => table,
            CurrentTableState::Dropped { .. } => {
                panic!("recovery live removal invariant violated: current state is dropped")
            }
        }
    }

    /// Returns the logical history's superseded-version count for tests.
    #[cfg(test)]
    #[inline]
    pub(crate) fn history_version_count(&self) -> Option<usize> {
        self.history.as_ref().map(TableHistoryEntry::version_count)
    }

    #[inline]
    fn assert_valid(&self) {
        let Some(history) = &self.history else {
            return;
        };
        history.assert_valid();
        if matches!(history.current, CurrentTableState::Live { .. }) {
            assert!(
                self.dropped.is_none(),
                "user-table registry invariant violated: live current state has dropped operational state"
            );
        }
    }
}

#[inline]
fn assert_current_layout_metadata(table: &Arc<Table>, metadata: &Arc<TableMetadata>) {
    let layout = table.layout_snapshot();
    assert!(
        Arc::ptr_eq(layout.metadata_arc(), metadata),
        "table metadata history invariant violated: current metadata is not pointer-identical to installed runtime layout, table_id={}",
        table.table_id()
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{IndexAttributes, IndexKey, IndexSpec};
    use crate::engine::Engine;
    use crate::file::cow_file::SUPER_BLOCK_ID;
    use crate::id::TableID;
    use crate::table::tests::{create_table2_for_test, lightweight_test_engine};
    use crate::trx::MAX_SNAPSHOT_TS;
    use tempfile::TempDir;

    trait CurrentTableStateTestExt {
        fn live_metadata(&self) -> Option<&Arc<TableMetadata>>;
        fn is_dropped(&self) -> bool;
    }

    impl CurrentTableStateTestExt for CurrentTableState {
        #[inline]
        fn live_metadata(&self) -> Option<&Arc<TableMetadata>> {
            match self {
                CurrentTableState::Live { metadata, .. } => Some(metadata),
                CurrentTableState::Dropped { .. } => None,
            }
        }

        #[inline]
        fn is_dropped(&self) -> bool {
            matches!(self, CurrentTableState::Dropped { .. })
        }
    }

    trait ResolvedVisibleTableMetadataTestExt {
        fn effective_cts(&self) -> TrxID;
        fn live(&self) -> Option<&ResolvedLiveMetadata>;
        fn is_tombstone(&self) -> bool;
    }

    impl ResolvedVisibleTableMetadataTestExt for ResolvedVisibleTableMetadata {
        #[inline]
        fn effective_cts(&self) -> TrxID {
            match self {
                ResolvedVisibleTableMetadata::Live(live) => live.effective_cts(),
                ResolvedVisibleTableMetadata::Tombstone { effective_cts } => *effective_cts,
            }
        }

        #[inline]
        fn live(&self) -> Option<&ResolvedLiveMetadata> {
            match self {
                ResolvedVisibleTableMetadata::Live(live) => Some(live),
                ResolvedVisibleTableMetadata::Tombstone { .. } => None,
            }
        }

        #[inline]
        fn is_tombstone(&self) -> bool {
            matches!(self, ResolvedVisibleTableMetadata::Tombstone { .. })
        }
    }

    #[inline]
    fn after(ts: TrxID) -> TrxID {
        TrxID::new(ts.as_u64() + 1)
    }

    #[test]
    fn metadata_history_resolves_strict_boundaries_and_tombstones() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "metadata_history").await;
            let table_id = create_table2_for_test(&engine).await;
            let catalog = engine.catalog();

            let initial = catalog.resolve_user_table_current(table_id).unwrap();
            let initial_cts = initial.effective_cts();
            let initial_metadata = Arc::clone(initial.live_metadata().unwrap());
            let table = Arc::clone(initial.live_table().unwrap());
            assert!(Arc::ptr_eq(
                &initial_metadata,
                table.layout_snapshot().metadata_arc()
            ));
            assert!(!initial.is_dropped());
            assert!(
                catalog
                    .resolve_user_table_visible(table_id, initial_cts)
                    .is_none()
            );
            let initial_visible = catalog
                .resolve_user_table_visible(table_id, after(initial_cts))
                .unwrap();
            let initial_live = initial_visible.live().unwrap();
            assert_eq!(initial_live.effective_cts(), initial_cts);
            assert!(Arc::ptr_eq(initial_live.metadata(), &initial_metadata));

            let mut reader_session = engine.new_session().unwrap();
            let reader = reader_session.begin_trx().unwrap();
            let reader_sts = reader.sts();
            assert!(initial_cts < reader_sts);

            let mut ddl_session = engine.new_session().unwrap();
            let index_no = ddl_session
                .create_index(
                    table_id,
                    IndexSpec::new(vec![IndexKey::new(1)], IndexAttributes::empty()),
                )
                .await
                .unwrap();
            let after_create = catalog.resolve_user_table_current(table_id).unwrap();
            let create_index_cts = after_create.effective_cts();
            assert!(reader_sts < create_index_cts);
            assert_eq!(
                after_create
                    .live_metadata()
                    .unwrap()
                    .idx
                    .active_index_count(),
                2
            );

            ddl_session.drop_index(table_id, index_no).await.unwrap();
            let after_drop_index = catalog.resolve_user_table_current(table_id).unwrap();
            let drop_index_cts = after_drop_index.effective_cts();
            assert!(create_index_cts < drop_index_cts);
            assert_eq!(
                after_drop_index
                    .live_metadata()
                    .unwrap()
                    .idx
                    .active_index_count(),
                1
            );
            assert_eq!(catalog.user_table_history_version_count(table_id), Some(2));

            let at_create_equality = catalog
                .resolve_user_table_visible(table_id, create_index_cts)
                .unwrap();
            let create_predecessor = at_create_equality.live().unwrap();
            assert_eq!(create_predecessor.effective_cts(), initial_cts);
            assert_eq!(create_predecessor.metadata().idx.active_index_count(), 1);

            let at_drop_equality = catalog
                .resolve_user_table_visible(table_id, drop_index_cts)
                .unwrap();
            let drop_predecessor = at_drop_equality.live().unwrap();
            assert_eq!(drop_predecessor.effective_cts(), create_index_cts);
            assert_eq!(drop_predecessor.metadata().idx.active_index_count(), 2);
            assert_ne!(
                create_predecessor.effective_cts(),
                drop_predecessor.effective_cts()
            );

            let layout = table.layout_snapshot();
            let index = Arc::clone(layout.secondary_indexes()[0].as_ref().unwrap());
            let table_owners = Arc::strong_count(&table);
            let layout_owners = Arc::strong_count(&layout);
            let index_owners = Arc::strong_count(&index);
            let resolved_visible = catalog
                .resolve_user_table_visible(table_id, reader_sts)
                .unwrap();
            let resolved_live = resolved_visible.live().unwrap();
            assert_eq!(resolved_live.effective_cts(), initial_cts);
            assert!(Arc::ptr_eq(resolved_live.metadata(), &initial_metadata));
            assert_eq!(Arc::strong_count(&table), table_owners);
            assert_eq!(Arc::strong_count(&layout), layout_owners);
            assert_eq!(Arc::strong_count(&index), index_owners);

            drop(at_create_equality);
            drop(at_drop_equality);
            reader.rollback().await.unwrap();
            catalog.purge_user_table_history(MAX_SNAPSHOT_TS);
            assert_eq!(catalog.user_table_history_version_count(table_id), Some(0));
            assert_eq!(
                resolved_visible.live().unwrap().effective_cts(),
                initial_cts
            );
            assert!(Arc::ptr_eq(
                resolved_visible.live().unwrap().metadata(),
                &initial_metadata
            ));
            assert_eq!(Arc::strong_count(&table), table_owners);
            assert_eq!(Arc::strong_count(&layout), layout_owners);
            assert_eq!(Arc::strong_count(&index), index_owners);

            drop(initial);
            drop(after_create);
            drop(after_drop_index);
            drop(table);
            let mut drop_reader_session = engine.new_session().unwrap();
            let drop_reader = drop_reader_session.begin_trx().unwrap();
            let drop_reader_sts = drop_reader.sts();
            ddl_session.drop_table(table_id).await.unwrap();

            let dropped = catalog.resolve_user_table_current(table_id).unwrap();
            let drop_table_cts = dropped.effective_cts();
            assert!(dropped.is_dropped());
            assert!(dropped.live_table().is_none());
            assert!(catalog.get_table_now(table_id).is_none());

            let at_table_drop_equality = catalog
                .resolve_user_table_visible(table_id, drop_table_cts)
                .unwrap();
            assert_eq!(
                at_table_drop_equality.live().unwrap().effective_cts(),
                drop_index_cts
            );
            let after_table_drop = catalog
                .resolve_user_table_visible(table_id, after(drop_table_cts))
                .unwrap();
            assert!(after_table_drop.is_tombstone());
            assert_eq!(after_table_drop.effective_cts(), drop_table_cts);

            let reader_visible = catalog
                .resolve_user_table_visible(table_id, drop_reader_sts)
                .unwrap();
            let reader_visible_cts = reader_visible.live().unwrap().effective_cts();
            let reader_visible_metadata = Arc::clone(reader_visible.live().unwrap().metadata());
            drop_reader.rollback().await.unwrap();
            drop(after_table_drop);
            catalog.purge_user_table_history(MAX_SNAPSHOT_TS);
            assert!(catalog.resolve_user_table_current(table_id).is_none());
            assert_eq!(
                at_table_drop_equality.live().unwrap().effective_cts(),
                drop_index_cts
            );
            assert_eq!(
                reader_visible.live().unwrap().effective_cts(),
                reader_visible_cts
            );
            assert!(Arc::ptr_eq(
                reader_visible.live().unwrap().metadata(),
                &reader_visible_metadata
            ));
            assert_eq!(catalog.retained_dropped_table_ids_now(), vec![table_id]);
            assert!(
                catalog
                    .resolve_user_table_visible(table_id, MAX_SNAPSHOT_TS)
                    .is_none()
            );
        });
    }

    fn assert_dropped_runtime(entry: &UserTableEntry) {
        assert!(matches!(
            entry.dropped.as_ref(),
            Some(DroppedTableOperationalState::Runtime { .. })
        ));
    }

    fn assert_dropped_floor(entry: &UserTableEntry) {
        assert!(matches!(
            entry.dropped.as_ref(),
            Some(DroppedTableOperationalState::Floor { .. })
        ));
    }

    fn dropped_entry_fixture(
        engine: &Engine,
        table_id: TableID,
    ) -> (
        UserTableEntry,
        Arc<Table>,
        TrxID,
        TrxID,
        TableRedoReplayFloor,
    ) {
        let current = engine
            .catalog()
            .resolve_user_table_current(table_id)
            .unwrap();
        let initial_cts = current.effective_cts();
        let table = Arc::clone(current.live_table().unwrap());
        let metadata = Arc::clone(current.live_metadata().unwrap());
        let drop_cts = after(initial_cts);
        let replay_floor = TableRedoReplayFloor {
            heap_redo_start_ts: TrxID::new(7),
            deletion_cutoff_ts: TrxID::new(9),
        };
        let mut entry = UserTableEntry::new_live(initial_cts, metadata, Arc::clone(&table));
        assert!(entry.publish_drop(drop_cts, Arc::clone(&table), replay_floor));
        (entry, table, initial_cts, drop_cts, replay_floor)
    }

    #[test]
    fn dropped_runtime_can_become_floor_before_tombstone_history_purge() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "drop_runtime_before_history").await;
            let table_id = create_table2_for_test(&engine).await;
            let (mut entry, _table, initial_cts, drop_cts, replay_floor) =
                dropped_entry_fixture(&engine, table_id);

            entry.purge_history(drop_cts);
            assert!(entry.take_dropped_runtime(drop_cts).is_none());
            assert_dropped_runtime(&entry);
            let at_equality = entry.resolve_visible(drop_cts).unwrap();
            assert_eq!(at_equality.live().unwrap().effective_cts(), initial_cts);

            let (detached, observed_drop_cts, observed_floor) =
                entry.take_dropped_runtime(after(drop_cts)).unwrap();
            assert_eq!(observed_drop_cts, drop_cts);
            assert_eq!(observed_floor, replay_floor);
            assert_dropped_floor(&entry);
            assert!(entry.resolve_current().unwrap().is_dropped());
            assert!(
                entry
                    .resolve_visible(after(drop_cts))
                    .unwrap()
                    .is_tombstone()
            );
            assert!(!entry.is_empty());

            entry.purge_history(after(drop_cts));
            assert!(entry.resolve_current().is_none());
            assert!(!entry.is_empty());
            assert!(entry.remove_dropped_floor(drop_cts, replay_floor));
            assert!(entry.is_empty());
            drop(detached);
        });
    }

    #[test]
    fn tombstone_history_can_purge_before_dropped_runtime_becomes_floor() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "drop_history_before_runtime").await;
            let table_id = create_table2_for_test(&engine).await;
            let (mut entry, _table, _initial_cts, drop_cts, replay_floor) =
                dropped_entry_fixture(&engine, table_id);

            entry.purge_history(drop_cts);
            assert!(entry.resolve_current().unwrap().is_dropped());
            assert!(entry.take_dropped_runtime(drop_cts).is_none());
            assert_dropped_runtime(&entry);

            entry.purge_history(after(drop_cts));
            assert!(entry.resolve_current().is_none());
            assert_eq!(entry.history_version_count(), None);
            assert_dropped_runtime(&entry);
            assert!(!entry.is_empty());

            let (detached, observed_drop_cts, observed_floor) =
                entry.take_dropped_runtime(after(drop_cts)).unwrap();
            assert_eq!(observed_drop_cts, drop_cts);
            assert_eq!(observed_floor, replay_floor);
            assert_dropped_floor(&entry);
            assert!(entry.remove_dropped_floor(drop_cts, replay_floor));
            assert!(entry.is_empty());
            drop(detached);
        });
    }

    #[test]
    fn metadata_history_purge_retains_active_horizon_predecessor() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "metadata_history_purge").await;
            let table_id = create_table2_for_test(&engine).await;
            let catalog = engine.catalog();

            let mut ddl_session = engine.new_session().unwrap();
            let index_no = ddl_session
                .create_index(
                    table_id,
                    IndexSpec::new(vec![IndexKey::new(1)], IndexAttributes::empty()),
                )
                .await
                .unwrap();
            let create_index_cts = catalog
                .resolve_user_table_current(table_id)
                .unwrap()
                .effective_cts();

            let mut reader_session = engine.new_session().unwrap();
            let reader = reader_session.begin_trx().unwrap();
            let reader_sts = reader.sts();
            assert!(create_index_cts < reader_sts);

            ddl_session.drop_index(table_id, index_no).await.unwrap();
            let drop_index_cts = catalog
                .resolve_user_table_current(table_id)
                .unwrap()
                .effective_cts();
            assert!(reader_sts < drop_index_cts);

            catalog.purge_user_table_history(reader_sts);
            assert_eq!(catalog.user_table_history_version_count(table_id), Some(1));
            let visible = catalog
                .resolve_user_table_visible(table_id, reader_sts)
                .unwrap();
            let predecessor = visible.live().unwrap();
            assert_eq!(predecessor.effective_cts(), create_index_cts);
            assert_eq!(predecessor.metadata().idx.active_index_count(), 2);

            reader.rollback().await.unwrap();
            drop(visible);
            catalog.purge_user_table_history(after(drop_index_cts));
            assert_eq!(catalog.user_table_history_version_count(table_id), Some(0));
        });
    }

    #[test]
    fn recovery_builds_one_zero_cts_current_baseline() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let (table_id, retained_pre_crash_metadata) = {
                let engine = lightweight_test_engine(&temp_dir, "metadata_history_recovery").await;
                let table_id = create_table2_for_test(&engine).await;
                let mut session = engine.new_session().unwrap();
                let index_no = session
                    .create_index(
                        table_id,
                        IndexSpec::new(vec![IndexKey::new(1)], IndexAttributes::empty()),
                    )
                    .await
                    .unwrap();
                assert_eq!(usize::from(index_no), 1);
                let visible = engine
                    .catalog()
                    .resolve_user_table_visible(table_id, MAX_SNAPSHOT_TS)
                    .unwrap();
                let retained_metadata = Arc::clone(visible.live().unwrap().metadata());
                assert!(retained_metadata.idx.index_spec(1).is_some());

                session.drop_index(table_id, index_no).await.unwrap();
                let table = engine.catalog().get_table_now(table_id).unwrap();
                assert!(table.layout_snapshot().secondary_indexes()[1].is_none());
                assert!(!table.has_retired_secondary_indexes());
                assert_eq!(
                    table.file().active_root_unchecked().secondary_index_roots[1],
                    SUPER_BLOCK_ID
                );
                (table_id, retained_metadata)
            };

            let recovered = lightweight_test_engine(&temp_dir, "metadata_history_recovery").await;
            let current = recovered
                .catalog()
                .resolve_user_table_current(table_id)
                .unwrap();
            assert_eq!(current.effective_cts(), TrxID::new(0));
            let table = current.live_table().unwrap();
            assert!(Arc::ptr_eq(
                current.live_metadata().unwrap(),
                table.layout_snapshot().metadata_arc()
            ));
            assert!(!Arc::ptr_eq(
                current.live_metadata().unwrap(),
                &retained_pre_crash_metadata
            ));
            assert!(retained_pre_crash_metadata.idx.index_spec(1).is_some());
            assert!(current.live_metadata().unwrap().idx.index_spec(1).is_none());
            assert_eq!(current.live_metadata().unwrap().idx.index_slot_count(), 2);
            assert!(table.layout_snapshot().secondary_indexes()[1].is_none());
            assert_eq!(
                table.file().active_root_unchecked().secondary_index_roots[1],
                SUPER_BLOCK_ID
            );
            assert!(!table.has_retired_secondary_indexes());
            assert_eq!(
                recovered
                    .catalog()
                    .user_table_history_version_count(table_id),
                Some(0)
            );
            assert_eq!(
                recovered
                    .inner()
                    .session_registry
                    .active_transaction_count(),
                0
            );
        });
    }
}
