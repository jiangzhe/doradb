use super::{
    CurrentDefinitionAllocatorView, IndexPlacement, Table, TableDefinitionKind, TableRuntimeLayout,
};
use crate::catalog::{
    CatalogDefinitionEffects, CurrentTableDefinition, IndexID, IndexRef, SecondaryIndexRoot,
    SecondaryIndexSlot, StorageIndexSpec, StorageTableDefinition, TableDescriptorObject,
    TableIndexMetadata, TableMetadata, validate_table_descriptor_against_metadata,
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
    definition_effects: CatalogDefinitionEffects,
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

    /// Returns catalog definition effects committed with numeric metadata.
    #[inline]
    pub(crate) const fn definition_effects(&self) -> &CatalogDefinitionEffects {
        &self.definition_effects
    }
}

/// Table-finalized CREATE INDEX state awaiting catalog definition effects.
struct CreateIndexPartialPlan {
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

impl CreateIndexPartialPlan {
    /// Completes this partial plan with explicit catalog definition effects.
    #[inline]
    fn with_effects(self, definition_effects: CatalogDefinitionEffects) -> CreateIndexPlan {
        let Self {
            table_id,
            table,
            old_layout,
            active_root,
            index,
            new_metadata,
            new_index_spec,
            secondary_index_slots,
            placement,
            skipped_retired_runtime,
        } = self;
        CreateIndexPlan {
            table_id,
            table,
            old_layout,
            active_root,
            index,
            new_metadata,
            new_index_spec,
            secondary_index_slots,
            placement,
            skipped_retired_runtime,
            definition_effects,
        }
    }

    /// Completes this partial plan without catalog definition effects.
    #[inline]
    fn no_effects(self) -> CreateIndexPlan {
        self.with_effects(CatalogDefinitionEffects::none())
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
    definition_effects: CatalogDefinitionEffects,
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

    /// Returns catalog definition effects committed with numeric metadata.
    #[inline]
    pub(crate) const fn definition_effects(&self) -> &CatalogDefinitionEffects {
        &self.definition_effects
    }
}

/// Table-finalized DROP INDEX state awaiting catalog definition effects.
struct DropIndexPartialPlan {
    table_id: TableID,
    table: Arc<Table>,
    old_layout: Arc<TableRuntimeLayout>,
    index: IndexRef,
    new_metadata: Arc<TableMetadata>,
    secondary_index_slots: Vec<SecondaryIndexSlot>,
}

impl DropIndexPartialPlan {
    /// Completes this partial plan with explicit catalog definition effects.
    #[inline]
    fn with_effects(self, definition_effects: CatalogDefinitionEffects) -> DropIndexPlan {
        let Self {
            table_id,
            table,
            old_layout,
            index,
            new_metadata,
            secondary_index_slots,
        } = self;
        DropIndexPlan {
            table_id,
            table,
            old_layout,
            index,
            new_metadata,
            secondary_index_slots,
            definition_effects,
        }
    }

    /// Completes this partial plan without catalog definition effects.
    #[inline]
    fn no_effects(self) -> DropIndexPlan {
        self.with_effects(CatalogDefinitionEffects::none())
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
    /// Requires one DDL API family to match the table's immutable definition owner.
    #[inline]
    fn require_definition_kind(
        &self,
        expected: TableDefinitionKind,
        operation: &'static str,
    ) -> OperationOrRuntimeResult<()> {
        if self.definition_kind == expected {
            return Ok(());
        }
        Err(Report::new(OperationError::InvalidMetadata)
            .attach(format!(
                "{} {operation} is not allowed for {} table_id={}",
                expected.label(),
                self.definition_kind.label(),
                self.table_id()
            ))
            .into())
    }

    /// Finalizes one CREATE INDEX from the current Table-owned definition.
    pub(crate) fn finalize_create_index(
        self: &Arc<Self>,
        index_spec: StorageIndexSpec,
    ) -> OperationOrRuntimeResult<CreateIndexPlan> {
        self.require_definition_kind(TableDefinitionKind::Unmanaged, "CREATE INDEX")?;
        let definition = self
            .current_index_ddl_definition()
            .change_context(RuntimeError::CatalogAccess)
            .attach("operation=create_index, phase=validate_current_definition")?;
        let partial = self.prepare_create_index_from_definition(definition, index_spec)?;
        Ok(partial.no_effects())
    }

    /// Captures the private current managed definition under metadata-S.
    pub(crate) fn current_managed_definition(
        self: &Arc<Self>,
        descriptor: TableDescriptorObject,
    ) -> OperationOrRuntimeResult<CurrentTableDefinition> {
        self.require_definition_kind(TableDefinitionKind::Managed, "DDL")?;
        let definition = self
            .current_index_ddl_definition()
            .change_context(RuntimeError::CatalogAccess)
            .attach("operation=managed_ddl, phase=validate_current_definition")?;
        validate_table_descriptor_against_metadata(
            &descriptor,
            definition.table_id,
            definition.allocator.metadata(),
        )
        .change_context(RuntimeError::CatalogAccess)
        .attach("operation=managed_ddl, phase=validate_descriptor_stamp")?;
        Ok(CurrentTableDefinition::new(
            StorageTableDefinition::from_metadata(definition.allocator.metadata()),
            descriptor,
            definition.allocator.metadata().storage_epoch,
            definition.allocator.effective_next_index_id(),
        ))
    }

    /// Revalidates and finalizes a managed CREATE INDEX callback result.
    pub(crate) fn finalize_managed_create_index(
        self: &Arc<Self>,
        expected: &CurrentTableDefinition,
        current_descriptor: TableDescriptorObject,
        index_spec: StorageIndexSpec,
        payload: Box<[u8]>,
    ) -> OperationOrRuntimeResult<CreateIndexPlan> {
        self.require_definition_kind(TableDefinitionKind::Managed, "CREATE INDEX")?;
        let definition = self
            .current_index_ddl_definition()
            .change_context(RuntimeError::CatalogAccess)
            .attach("operation=create_managed_index, phase=validate_current_definition")?;
        validate_table_descriptor_against_metadata(
            &current_descriptor,
            definition.table_id,
            definition.allocator.metadata(),
        )
        .change_context(RuntimeError::CatalogAccess)
        .attach("operation=create_managed_index, phase=validate_descriptor_stamp")?;
        if managed_definition_changed(expected, &definition, &current_descriptor, true) {
            return Err(schema_changed(definition.table_id, "create_managed_index").into());
        }
        let revision = current_descriptor
            .descriptor_revision
            .checked_add(1)
            .ok_or_else(|| {
                Report::new(OperationError::InvalidMetadata)
                    .attach("managed descriptor revision exhausted")
            })?;
        let partial = self.prepare_create_index_from_definition(definition, index_spec)?;
        let descriptor = TableDescriptorObject {
            table_id: partial.table_id,
            descriptor_revision: revision,
            compiled_storage_epoch: partial.new_metadata.storage_epoch,
            storage_schema_fingerprint: partial.new_metadata.storage_schema_fingerprint(),
            payload,
        };
        Ok(partial.with_effects(CatalogDefinitionEffects::replace(descriptor)))
    }

    fn prepare_create_index_from_definition(
        self: &Arc<Self>,
        definition: CurrentIndexDdlDefinition,
        index_spec: StorageIndexSpec,
    ) -> OperationOrRuntimeResult<CreateIndexPartialPlan> {
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
        Ok(CreateIndexPartialPlan {
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
        self.require_definition_kind(TableDefinitionKind::Unmanaged, "DROP INDEX")?;
        let definition = self
            .current_index_ddl_definition()
            .change_context(RuntimeError::CatalogAccess)
            .attach("operation=drop_index, phase=validate_current_definition")?;
        let partial = self.prepare_drop_index_from_definition(definition, index_id)?;
        Ok(partial.no_effects())
    }

    /// Revalidates and finalizes a managed DROP INDEX callback result.
    pub(crate) fn finalize_managed_drop_index(
        self: &Arc<Self>,
        expected: &CurrentTableDefinition,
        current_descriptor: TableDescriptorObject,
        index_id: IndexID,
        payload: Box<[u8]>,
    ) -> OperationOrRuntimeResult<DropIndexPlan> {
        self.require_definition_kind(TableDefinitionKind::Managed, "DROP INDEX")?;
        let definition = self
            .current_index_ddl_definition()
            .change_context(RuntimeError::CatalogAccess)
            .attach("operation=drop_managed_index, phase=validate_current_definition")?;
        validate_table_descriptor_against_metadata(
            &current_descriptor,
            definition.table_id,
            definition.allocator.metadata(),
        )
        .change_context(RuntimeError::CatalogAccess)
        .attach("operation=drop_managed_index, phase=validate_descriptor_stamp")?;
        if managed_definition_changed(expected, &definition, &current_descriptor, false) {
            return Err(schema_changed(definition.table_id, "drop_managed_index").into());
        }
        let revision = current_descriptor
            .descriptor_revision
            .checked_add(1)
            .ok_or_else(|| {
                Report::new(OperationError::InvalidMetadata)
                    .attach("managed descriptor revision exhausted")
            })?;
        let partial = self.prepare_drop_index_from_definition(definition, index_id)?;
        let descriptor = TableDescriptorObject {
            table_id: partial.table_id,
            descriptor_revision: revision,
            compiled_storage_epoch: partial.new_metadata.storage_epoch,
            storage_schema_fingerprint: partial.new_metadata.storage_schema_fingerprint(),
            payload,
        };
        Ok(partial.with_effects(CatalogDefinitionEffects::replace(descriptor)))
    }

    fn prepare_drop_index_from_definition(
        self: &Arc<Self>,
        definition: CurrentIndexDdlDefinition,
        index_id: IndexID,
    ) -> OperationOrRuntimeResult<DropIndexPartialPlan> {
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
        Ok(DropIndexPartialPlan {
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

#[inline]
fn managed_definition_changed(
    expected: &CurrentTableDefinition,
    current: &CurrentIndexDdlDefinition,
    current_descriptor: &TableDescriptorObject,
    compare_allocator: bool,
) -> bool {
    expected.storage_epoch() != current.allocator.metadata().storage_epoch
        || expected.descriptor().descriptor_revision != current_descriptor.descriptor_revision
        || expected.descriptor().compiled_storage_epoch != current_descriptor.compiled_storage_epoch
        || expected.descriptor().storage_schema_fingerprint
            != current_descriptor.storage_schema_fingerprint
        || (compare_allocator
            && expected.effective_next_index_id() != current.allocator.effective_next_index_id())
}

#[inline]
fn schema_changed(table_id: TableID, operation: &'static str) -> Report<OperationError> {
    Report::new(OperationError::SchemaChanged).attach(format!(
        "managed DDL definition changed after interpretation: operation={operation}, table_id={table_id}"
    ))
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
