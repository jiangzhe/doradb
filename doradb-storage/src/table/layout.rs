use crate::buffer::EvictableBufferPool;
use crate::catalog::TableMetadata;
use crate::error::{InternalError, RuntimeError, RuntimeResult};
use crate::index::SecondaryIndex;
use error_stack::Report;
use std::sync::Arc;

/// Immutable metadata and secondary-index runtime snapshot for a user table.
pub(crate) struct TableRuntimeLayout {
    generation: u64,
    metadata: Arc<TableMetadata>,
    secondary_indexes: Box<[Option<Arc<SecondaryIndex<EvictableBufferPool>>>]>,
}

impl TableRuntimeLayout {
    /// Create a validated user-table runtime layout snapshot.
    #[inline]
    pub(crate) fn new(
        generation: u64,
        metadata: Arc<TableMetadata>,
        secondary_indexes: Box<[Option<Arc<SecondaryIndex<EvictableBufferPool>>>]>,
    ) -> Self {
        let layout = Self {
            generation,
            metadata,
            secondary_indexes,
        };
        layout.assert_valid();
        layout
    }

    /// Assert layout shape against metadata and index runtime identity.
    #[inline]
    pub(crate) fn assert_valid(&self) {
        assert_eq!(
            self.secondary_indexes.len(),
            self.metadata.idx.index_slot_count(),
            "table runtime layout invariant violated: runtime_slots={}, metadata_slots={}",
            self.secondary_indexes.len(),
            self.metadata.idx.index_slot_count()
        );

        for (index_no, _) in self.metadata.idx.active_indexes() {
            assert!(
                self.secondary_indexes
                    .get(index_no)
                    .and_then(Option::as_ref)
                    .is_some(),
                "table runtime layout invariant violated: active metadata index missing runtime slot, index_no={index_no}"
            );
        }

        for (index_no, index) in self.secondary_indexes.iter().enumerate() {
            let Some(index) = index else {
                continue;
            };
            assert!(
                self.metadata.idx.index_spec(index_no).is_some(),
                "table runtime layout invariant violated: runtime slot has no active metadata spec, index_no={index_no}"
            );
            assert_eq!(
                index.index_no(),
                index_no,
                "table runtime layout invariant violated: runtime index number mismatch, slot={index_no}, runtime={}",
                index.index_no()
            );
            let index_spec = self
                .metadata
                .idx
                .index_spec(index_no)
                .expect("runtime slot was already proven active");
            assert_eq!(
                index.is_unique(),
                index_spec.unique(),
                "table runtime layout invariant violated: runtime index kind mismatch, index_no={index_no}, runtime_unique={}, metadata_unique={}",
                index.is_unique(),
                index_spec.unique()
            );
        }
    }

    /// Returns the monotonic runtime layout generation.
    #[inline]
    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }

    /// Returns this layout's table metadata.
    #[inline]
    pub(crate) fn metadata(&self) -> &TableMetadata {
        &self.metadata
    }

    /// Returns this layout's table metadata as a shared owner.
    #[inline]
    pub(crate) fn metadata_arc(&self) -> &Arc<TableMetadata> {
        &self.metadata
    }

    /// Returns the sparse secondary-index slot count.
    #[inline]
    pub(crate) fn index_slot_count(&self) -> usize {
        self.secondary_indexes.len()
    }

    /// Returns the sparse secondary-index runtime slots.
    #[inline]
    pub(crate) fn secondary_indexes(&self) -> &[Option<Arc<SecondaryIndex<EvictableBufferPool>>>] {
        &self.secondary_indexes
    }

    /// Consumes the layout and returns its secondary-index runtime slots.
    #[inline]
    pub(crate) fn into_secondary_indexes(
        self,
    ) -> Box<[Option<Arc<SecondaryIndex<EvictableBufferPool>>>]> {
        self.secondary_indexes
    }

    /// Returns one active secondary-index runtime by stable index number.
    #[inline]
    pub(crate) fn secondary_index(
        &self,
        index_no: usize,
    ) -> RuntimeResult<&SecondaryIndex<EvictableBufferPool>> {
        self.secondary_indexes
            .get(index_no)
            .and_then(Option::as_deref)
            .ok_or_else(|| {
                Report::new(InternalError::SecondaryIndexOutOfBounds)
                    .attach(format!(
                        "index_no={index_no}, index_slot_count={}",
                        self.index_slot_count()
                    ))
                    .change_context(RuntimeError::IndexAccess)
                    .attach("operation=resolve_secondary_index_runtime")
            })
    }

    /// Iterates active secondary-index runtimes by stable index number.
    #[inline]
    pub(crate) fn active_secondary_indexes(
        &self,
    ) -> impl Iterator<Item = (usize, &SecondaryIndex<EvictableBufferPool>)> + '_ {
        self.secondary_indexes
            .iter()
            .enumerate()
            .filter_map(|(index_no, index)| index.as_deref().map(|index| (index_no, index)))
    }
}

/// Retired user-table secondary-index runtime awaiting async MemIndex destroy.
pub(crate) struct RetiredSecondaryIndex {
    /// Stable secondary-index slot retired from the active runtime layout.
    pub(crate) index_no: usize,
    /// Layout generation that retired this runtime index.
    pub(crate) retired_generation: u64,
    /// Secondary-index runtime waiting for asynchronous MemIndex destruction.
    pub(crate) index: Arc<SecondaryIndex<EvictableBufferPool>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::{BufferPool, PoolGuards, PoolRole};
    use crate::catalog::{
        ActiveIndexSpec, ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexSpec,
    };
    use crate::table::tests::*;
    use crate::value::ValKind;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use tempfile::TempDir;

    fn table2_columns() -> Vec<ColumnSpec> {
        vec![
            ColumnSpec::new("id", ValKind::I32, ColumnAttributes::empty()),
            ColumnSpec::new("name", ValKind::VarByte, ColumnAttributes::empty()),
        ]
    }

    fn metadata_without_indexes() -> Arc<TableMetadata> {
        Arc::new(
            TableMetadata::try_new(
                vec![ColumnSpec::new(
                    "id",
                    ValKind::I32,
                    ColumnAttributes::empty(),
                )],
                vec![],
            )
            .expect("valid table metadata"),
        )
    }

    #[test]
    fn runtime_layout_accepts_matching_empty_index_shape() {
        let metadata = metadata_without_indexes();
        let layout = TableRuntimeLayout::new(
            7,
            Arc::clone(&metadata),
            Vec::<Option<Arc<SecondaryIndex<EvictableBufferPool>>>>::new().into_boxed_slice(),
        );

        assert_eq!(layout.generation(), 7);
        assert_eq!(layout.metadata().idx.index_slot_count(), 0);
        assert_eq!(layout.index_slot_count(), 0);
    }

    #[test]
    fn runtime_layout_rejects_structural_mismatches() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "runtime_layout_validation").await;
            let table_id = create_table2_for_test(&engine).await;
            let layout = table_for_internal_assertion(&engine, table_id).layout_snapshot();
            let runtime = Arc::clone(layout.secondary_indexes()[0].as_ref().unwrap());

            assert!(
                catch_unwind(AssertUnwindSafe(|| {
                    TableRuntimeLayout::new(
                        layout.generation() + 1,
                        Arc::clone(layout.metadata_arc()),
                        vec![None].into_boxed_slice(),
                    )
                }))
                .is_err()
            );

            let inactive_metadata = Arc::new(
                TableMetadata::try_new_with_next_index_no(table2_columns(), vec![], 1).unwrap(),
            );
            assert!(
                catch_unwind(AssertUnwindSafe(|| {
                    TableRuntimeLayout::new(
                        layout.generation() + 1,
                        inactive_metadata,
                        vec![Some(Arc::clone(&runtime))].into_boxed_slice(),
                    )
                }))
                .is_err()
            );

            let shifted_metadata = Arc::new(
                TableMetadata::try_new_with_next_index_no(
                    table2_columns(),
                    vec![ActiveIndexSpec::new(
                        1,
                        IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::UK),
                    )],
                    2,
                )
                .unwrap(),
            );
            assert!(
                catch_unwind(AssertUnwindSafe(|| {
                    TableRuntimeLayout::new(
                        layout.generation() + 1,
                        shifted_metadata,
                        vec![None, Some(Arc::clone(&runtime))].into_boxed_slice(),
                    )
                }))
                .is_err()
            );

            let non_unique_metadata = Arc::new(
                TableMetadata::try_new(
                    table2_columns(),
                    vec![IndexSpec::new(
                        vec![IndexKey::new(0)],
                        IndexAttributes::empty(),
                    )],
                )
                .unwrap(),
            );
            assert!(
                catch_unwind(AssertUnwindSafe(|| {
                    TableRuntimeLayout::new(
                        layout.generation() + 1,
                        non_unique_metadata,
                        vec![Some(runtime)].into_boxed_slice(),
                    )
                }))
                .is_err()
            );
        });
    }

    #[test]
    fn test_runtime_layout_install_retires_removed_index_after_old_snapshot_drops() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
            let table_id = create_table2_for_test(&engine).await;
            let old_layout = table_for_internal_assertion(&engine, table_id).layout_snapshot();
            assert_eq!(old_layout.metadata().idx.active_index_count(), 1);

            let metadata_without_indexes = Arc::new(
                TableMetadata::try_new_with_next_index_no(
                    vec![
                        ColumnSpec::new("id", ValKind::I32, ColumnAttributes::empty()),
                        ColumnSpec::new("name", ValKind::VarByte, ColumnAttributes::empty()),
                    ],
                    vec![],
                    old_layout.metadata().idx.next_index_no(),
                )
                .unwrap(),
            );
            let mut inactive_slots: Vec<Option<Arc<SecondaryIndex<EvictableBufferPool>>>> =
                Vec::with_capacity(old_layout.index_slot_count());
            inactive_slots.resize_with(old_layout.index_slot_count(), || None);
            let new_layout = TableRuntimeLayout::new(
                old_layout.generation() + 1,
                metadata_without_indexes,
                inactive_slots.into_boxed_slice(),
            );

            let installed = table_for_internal_assertion(&engine, table_id)
                .install_runtime_layout(old_layout.generation(), new_layout);
            assert_eq!(old_layout.metadata().idx.active_index_count(), 1);
            assert_eq!(installed.metadata().idx.active_index_count(), 0);
            assert_eq!(
                installed.metadata().idx.next_index_no(),
                old_layout.metadata().idx.next_index_no()
            );
            assert_eq!(
                installed.metadata().idx.index_slot_count(),
                old_layout.metadata().idx.index_slot_count()
            );
            assert_eq!(installed.index_slot_count(), old_layout.index_slot_count());
            assert!(installed.secondary_indexes()[0].is_none());
            assert_eq!(
                table_for_internal_assertion(&engine, table_id)
                    .metadata()
                    .idx
                    .active_index_count(),
                0
            );
            assert!(
                table_for_internal_assertion(&engine, table_id).has_retired_secondary_indexes()
            );

            let guards = PoolGuards::builder()
                .push(PoolRole::Index, engine.inner().index_pool.pool_guard())
                .build();
            assert_eq!(
                table_for_internal_assertion(&engine, table_id)
                    .cleanup_retired_secondary_indexes(&guards)
                    .await
                    .unwrap(),
                0
            );
            drop(old_layout);
            assert_eq!(
                table_for_internal_assertion(&engine, table_id)
                    .cleanup_retired_secondary_indexes(&guards)
                    .await
                    .unwrap(),
                1
            );
            assert!(
                !table_for_internal_assertion(&engine, table_id).has_retired_secondary_indexes()
            );
        })
    }
}
