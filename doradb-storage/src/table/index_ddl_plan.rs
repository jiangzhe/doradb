use super::{CurrentDefinitionAllocatorView, IndexPlacement, Table, TableRuntimeLayout};
use crate::catalog::{
    IndexID, IndexRef, SecondaryIndexRoot, SecondaryIndexSlot, StorageIndexSpec,
    TableIndexMetadata, TableMetadata,
};
use crate::error::{
    DataIntegrityError, DataIntegrityResult, OperationError, OperationOrRuntimeResult, RuntimeError,
};
use crate::file::meta_block::validate_secondary_index_state;
use crate::file::table_file::ActiveRoot;
use crate::id::TableID;
use error_stack::{Report, ResultExt};
use std::mem::take;
use std::sync::Arc;

/// Owned, Table-finalized CREATE INDEX execution plan.
pub(crate) struct CreateIndexPlan {
    table_id: TableID,
    table: Arc<Table>,
    old_layout: Arc<TableRuntimeLayout>,
    active_root: ActiveRoot,
    index: IndexRef,
    new_metadata: Arc<TableMetadata>,
    new_index_spec: TableIndexMetadata,
    secondary_index_slots: Vec<SecondaryIndexSlot>,
    placement: IndexPlacement,
    skipped_retired_runtime: bool,
}

impl CreateIndexPlan {
    /// Returns the target Table identity.
    #[inline]
    pub(crate) const fn table_id(&self) -> TableID {
        self.table_id
    }

    /// Returns the target Table owner.
    #[inline]
    pub(crate) fn table(&self) -> &Arc<Table> {
        &self.table
    }

    /// Returns the runtime layout captured during finalization.
    #[inline]
    pub(crate) fn old_layout(&self) -> &Arc<TableRuntimeLayout> {
        &self.old_layout
    }

    /// Returns the active root captured during finalization.
    #[inline]
    pub(crate) const fn active_root(&self) -> &ActiveRoot {
        &self.active_root
    }

    /// Returns the finalized index generation.
    #[inline]
    pub(crate) const fn index(&self) -> IndexRef {
        self.index
    }

    /// Returns the finalized Table metadata.
    #[inline]
    pub(crate) fn new_metadata(&self) -> &Arc<TableMetadata> {
        &self.new_metadata
    }

    /// Returns the finalized index metadata.
    #[inline]
    pub(crate) const fn new_index_spec(&self) -> &TableIndexMetadata {
        &self.new_index_spec
    }

    /// Takes the finalized root-slot vector for Table-file publication.
    #[inline]
    pub(crate) fn take_secondary_index_slots(&mut self) -> Vec<SecondaryIndexSlot> {
        take(&mut self.secondary_index_slots)
    }

    /// Returns the authority-selected physical placement.
    #[inline]
    pub(crate) const fn placement(&self) -> IndexPlacement {
        self.placement
    }

    /// Returns whether allocation skipped a checkpoint-covered pinned runtime.
    #[inline]
    pub(crate) const fn skipped_retired_runtime(&self) -> bool {
        self.skipped_retired_runtime
    }
}

/// Owned, Table-finalized DROP INDEX execution plan.
pub(crate) struct DropIndexPlan {
    table_id: TableID,
    table: Arc<Table>,
    old_layout: Arc<TableRuntimeLayout>,
    index: IndexRef,
    new_metadata: Arc<TableMetadata>,
    secondary_index_slots: Vec<SecondaryIndexSlot>,
}

impl DropIndexPlan {
    /// Returns the target Table identity.
    #[inline]
    pub(crate) const fn table_id(&self) -> TableID {
        self.table_id
    }

    /// Returns the target Table owner.
    #[inline]
    pub(crate) fn table(&self) -> &Arc<Table> {
        &self.table
    }

    /// Returns the runtime layout captured during finalization.
    #[inline]
    pub(crate) fn old_layout(&self) -> &Arc<TableRuntimeLayout> {
        &self.old_layout
    }

    /// Returns the finalized retired index generation.
    #[inline]
    pub(crate) const fn index(&self) -> IndexRef {
        self.index
    }

    /// Returns the finalized Table metadata.
    #[inline]
    pub(crate) fn new_metadata(&self) -> &Arc<TableMetadata> {
        &self.new_metadata
    }

    /// Takes the finalized root-slot vector for Table-file publication.
    #[inline]
    pub(crate) fn take_secondary_index_slots(&mut self) -> Vec<SecondaryIndexSlot> {
        take(&mut self.secondary_index_slots)
    }
}

/// Authoritative current Table definition captured under index-DDL exclusion.
struct CurrentIndexDdlDefinition {
    table_id: TableID,
    old_layout: Arc<TableRuntimeLayout>,
    active_root: ActiveRoot,
    allocator: CurrentDefinitionAllocatorView,
}

impl Table {
    /// Finalizes one CREATE INDEX from the current Table-owned definition.
    pub(crate) fn finalize_create_index(
        self: &Arc<Self>,
        index_spec: StorageIndexSpec,
    ) -> OperationOrRuntimeResult<CreateIndexPlan> {
        let definition = self
            .current_index_ddl_definition()
            .change_context(RuntimeError::CatalogAccess)
            .attach("operation=create_index, phase=validate_current_definition")?;
        let CurrentIndexDdlDefinition {
            table_id,
            old_layout,
            active_root,
            allocator,
        } = definition;
        let (placement, skipped_retired_runtime) =
            self.select_index_create_placement(allocator.metadata().idx.index_slot_count_u32())?;
        let (index, new_metadata_value) = allocator.metadata().try_with_finalized_created_index(
            index_spec,
            allocator.effective_next_index_id(),
            placement,
        )?;
        let new_metadata = Arc::new(new_metadata_value);
        let new_index_spec = new_metadata
            .idx
            .require_index_spec(index.slot())
            .expect("newly created index metadata must contain its allocated slot")
            .clone();
        let mut secondary_index_slots = active_root.secondary_index_slots.clone();
        secondary_index_slots.resize(
            new_metadata.idx.index_slot_count(),
            SecondaryIndexSlot::Vacant,
        );
        secondary_index_slots[index.slot().as_usize()] = SecondaryIndexSlot::Active {
            index_id: index.id(),
            root: SecondaryIndexRoot::Empty,
        };
        Ok(CreateIndexPlan {
            table_id,
            table: Arc::clone(self),
            old_layout,
            active_root,
            index,
            new_metadata,
            new_index_spec,
            secondary_index_slots,
            placement,
            skipped_retired_runtime,
        })
    }

    /// Finalizes one DROP INDEX from the current Table-owned definition.
    pub(crate) fn finalize_drop_index(
        self: &Arc<Self>,
        index_id: IndexID,
    ) -> OperationOrRuntimeResult<DropIndexPlan> {
        let definition = self
            .current_index_ddl_definition()
            .change_context(RuntimeError::CatalogAccess)
            .attach("operation=drop_index, phase=validate_current_definition")?;
        let CurrentIndexDdlDefinition {
            table_id,
            old_layout,
            active_root,
            allocator: _,
        } = definition;
        let old_metadata = old_layout.metadata();
        let index = old_layout.resolve_index_id(index_id).ok_or_else(|| {
            Report::new(OperationError::IndexNotFound).attach(format!(
                "drop index target not found: table_id={table_id}, index_id={index_id}, reason=inactive_runtime_layout"
            ))
        })?;
        old_layout.secondary_index(index).map_err(|report| {
            report
                .change_context(RuntimeError::CatalogAccess)
                .attach("operation=drop_index, phase=validate_runtime")
        })?;
        let new_metadata = Arc::new(old_metadata.without_index(index)?);
        let mut secondary_index_slots = active_root.secondary_index_slots.clone();
        secondary_index_slots[index.slot().as_usize()] = SecondaryIndexSlot::Retired(index.id());
        Ok(DropIndexPlan {
            table_id,
            table: Arc::clone(self),
            old_layout,
            index,
            new_metadata,
            secondary_index_slots,
        })
    }

    /// Captures and validates state shared by typed CREATE and DROP finalization.
    fn current_index_ddl_definition(
        self: &Arc<Self>,
    ) -> DataIntegrityResult<CurrentIndexDdlDefinition> {
        let table_id = self.table_id();
        let old_layout = self.layout_snapshot();
        let active_root = self.file().active_root_unchecked().clone();
        validate_index_ddl_root_shape(table_id, &active_root, old_layout.metadata())?;
        let allocator = self.current_index_allocator_view(&old_layout, &active_root)?;
        Ok(CurrentIndexDdlDefinition {
            table_id,
            old_layout,
            active_root,
            allocator,
        })
    }
}

/// Validates that one active root represents the captured runtime metadata.
#[inline]
fn validate_index_ddl_root_shape(
    table_id: TableID,
    active_root: &ActiveRoot,
    metadata: &TableMetadata,
) -> DataIntegrityResult<()> {
    if active_root.metadata.as_ref() != metadata {
        return Err(
            Report::new(DataIntegrityError::InvalidRootInvariant).attach(format!(
                "index DDL root metadata mismatch: table_id={table_id}"
            )),
        );
    }
    let expected_slots = metadata.idx.index_slot_count();
    let actual_slots = active_root.secondary_index_slots.len();
    if actual_slots != expected_slots {
        return Err(Report::new(DataIntegrityError::InvalidRootInvariant).attach(format!(
            "index DDL secondary-root slot mismatch: table_id={table_id}, actual_slots={actual_slots}, expected_slots={expected_slots}"
        )));
    }
    validate_secondary_index_state(metadata, &active_root.secondary_index_slots)
        .change_context(DataIntegrityError::InvalidRootInvariant)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{
        ActiveIndexSpec, IndexSlot, StorageColumnFlags, StorageColumnSpec, StorageIndexFlags,
        StorageIndexKey,
    };
    use crate::id::TrxID;
    use crate::value::ValKind;
    use std::num::NonZeroU64;

    #[test]
    fn validate_index_ddl_root_shape_rejects_inconsistent_inactive_slot() {
        let metadata = TableMetadata::try_new_with_index_slot_count(
            vec![
                StorageColumnSpec::new(ValKind::I32, StorageColumnFlags::empty()),
                StorageColumnSpec::new(ValKind::I32, StorageColumnFlags::empty()),
            ],
            vec![ActiveIndexSpec::new(
                IndexRef::new(IndexID::new(0), IndexSlot::new(0)),
                StorageIndexSpec::new(vec![StorageIndexKey::new(0)], StorageIndexFlags::PK),
            )],
            IndexSlot::new(2),
        )
        .unwrap();
        let mut active_root = ActiveRoot::new(TrxID::new(20), 128, Arc::new(metadata.clone()));
        active_root.secondary_index_slots[1] = SecondaryIndexSlot::Active {
            index_id: IndexID::new(0),
            root: SecondaryIndexRoot::Present(NonZeroU64::new(99).unwrap()),
        };

        let err =
            validate_index_ddl_root_shape(TableID::new(42), &active_root, &metadata).unwrap_err();

        assert_eq!(
            err.downcast_ref::<DataIntegrityError>().copied(),
            Some(DataIntegrityError::InvalidRootInvariant)
        );
    }
}
