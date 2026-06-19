use crate::buffer::EvictableBufferPool;
use crate::catalog::TableMetadata;
use crate::error::{Error, InternalError, Result};
use crate::index::SecondaryIndex;
use error_stack::Report;
use std::sync::Arc;

#[inline]
fn invalid_runtime_layout(message: impl Into<String>) -> Error {
    Report::new(InternalError::Generic)
        .attach(format!("invalid table runtime layout: {}", message.into()))
        .into()
}

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
    ) -> Result<Self> {
        let layout = Self {
            generation,
            metadata,
            secondary_indexes,
        };
        layout.validate()?;
        Ok(layout)
    }

    /// Validate layout shape against metadata and index runtime identity.
    #[inline]
    pub(crate) fn validate(&self) -> Result<()> {
        if self.secondary_indexes.len() != self.metadata.idx.index_slot_count() {
            return Err(invalid_runtime_layout(format!(
                "slot count mismatch: runtime_slots={}, metadata_slots={}",
                self.secondary_indexes.len(),
                self.metadata.idx.index_slot_count()
            )));
        }

        for (index_no, _) in self.metadata.idx.active_indexes() {
            if self
                .secondary_indexes
                .get(index_no)
                .and_then(Option::as_ref)
                .is_none()
            {
                return Err(invalid_runtime_layout(format!(
                    "active metadata index missing runtime slot: index_no={index_no}"
                )));
            }
        }

        for (index_no, index) in self.secondary_indexes.iter().enumerate() {
            let Some(index) = index else {
                continue;
            };
            if self.metadata.idx.index_spec(index_no).is_none() {
                return Err(invalid_runtime_layout(format!(
                    "runtime slot has no active metadata spec: index_no={index_no}"
                )));
            }
            if index.index_no() != index_no {
                return Err(invalid_runtime_layout(format!(
                    "runtime index_no mismatch: slot={index_no}, runtime={}",
                    index.index_no()
                )));
            }
        }

        Ok(())
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
    ) -> Result<&SecondaryIndex<EvictableBufferPool>> {
        self.secondary_indexes
            .get(index_no)
            .and_then(Option::as_deref)
            .ok_or_else(|| {
                Report::new(InternalError::SecondaryIndexOutOfBounds)
                    .attach(format!(
                        "index_no={index_no}, index_slot_count={}",
                        self.index_slot_count()
                    ))
                    .into()
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
    pub(crate) index_no: usize,
    pub(crate) retired_generation: u64,
    pub(crate) index: Arc<SecondaryIndex<EvictableBufferPool>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::{BufferPool, PoolGuards, PoolRole};
    use crate::catalog::{ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexSpec};
    use crate::table::tests::*;
    use crate::value::ValKind;
    use tempfile::TempDir;

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

    fn metadata_with_primary_index() -> Arc<TableMetadata> {
        Arc::new(
            TableMetadata::try_new(
                vec![ColumnSpec::new(
                    "id",
                    ValKind::I32,
                    ColumnAttributes::empty(),
                )],
                vec![IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::PK)],
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
        )
        .expect("empty index layout should be valid");

        assert_eq!(layout.generation(), 7);
        assert_eq!(layout.metadata().idx.index_slot_count(), 0);
        assert_eq!(layout.index_slot_count(), 0);
    }

    #[test]
    fn runtime_layout_rejects_slot_count_mismatch() {
        let metadata = metadata_without_indexes();
        let err = match TableRuntimeLayout::new(0, metadata, vec![None].into_boxed_slice()) {
            Ok(_) => panic!("extra runtime slot should be rejected"),
            Err(err) => err,
        };

        assert!(err.is_kind(crate::error::ErrorKind::Internal), "{err:?}");
    }

    #[test]
    fn runtime_layout_rejects_active_index_without_runtime_slot() {
        let metadata = metadata_with_primary_index();
        let err = match TableRuntimeLayout::new(0, metadata, vec![None].into_boxed_slice()) {
            Ok(_) => panic!("active metadata index requires runtime slot"),
            Err(err) => err,
        };

        assert!(err.is_kind(crate::error::ErrorKind::Internal), "{err:?}");
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
            )
            .unwrap();

            let installed = table_for_internal_assertion(&engine, table_id)
                .install_runtime_layout(old_layout.generation(), new_layout)
                .unwrap();
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

    #[test]
    fn test_runtime_layout_install_rejects_shrinking_index_slots() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
            let table_id = create_table2_for_test(&engine).await;
            let old_layout = table_for_internal_assertion(&engine, table_id).layout_snapshot();
            assert_eq!(old_layout.index_slot_count(), 1);

            let shrinking_metadata = Arc::new(
                TableMetadata::try_new(
                    vec![
                        ColumnSpec::new("id", ValKind::I32, ColumnAttributes::empty()),
                        ColumnSpec::new("name", ValKind::VarByte, ColumnAttributes::empty()),
                    ],
                    vec![],
                )
                .expect("valid table metadata"),
            );
            let shrinking_layout = TableRuntimeLayout::new(
                old_layout.generation() + 1,
                shrinking_metadata,
                Vec::<Option<Arc<SecondaryIndex<EvictableBufferPool>>>>::new().into_boxed_slice(),
            )
            .unwrap();

            let result = table_for_internal_assertion(&engine, table_id)
                .install_runtime_layout(old_layout.generation(), shrinking_layout);
            assert!(result.is_err());
            let err = result.err().unwrap();
            assert!(
                format!("{err:?}").contains("new layout must not shrink sparse index slots"),
                "{err:?}"
            );
            assert_eq!(
                table_for_internal_assertion(&engine, table_id)
                    .layout_snapshot()
                    .generation(),
                old_layout.generation()
            );
        })
    }
}
