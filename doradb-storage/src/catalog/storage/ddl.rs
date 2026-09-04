use super::{CatalogStorage, ColumnObject, IndexObject, TableObject};
use crate::catalog::{
    CatalogDefinitionEffects, IndexRef, TableBindingEffect, TableDescriptorEffect, TableMetadata,
};
use crate::error::{
    DataIntegrityError, MultiDomainResultExt, QuadResult, RuntimeError, RuntimeOrFatalError,
    RuntimeOrFatalResult,
};
use crate::id::TableID;
use crate::log::redo::DDLRedo;
use crate::trx::PrivateTransaction;
use error_stack::Report;

impl CatalogStorage {
    /// Stages all canonical numeric catalog rows for a newly allocated table.
    pub(crate) async fn stage_create_table(
        &self,
        trx: &mut PrivateTransaction,
        table_id: TableID,
        metadata: &TableMetadata,
        definition_effects: &CatalogDefinitionEffects,
    ) -> QuadResult<()> {
        validate_catalog_engine_health(trx, "stage_create_table")?;

        // Binding uniqueness is the only expected CREATE catalog-DML failure.
        // Resolve it before staging invariant-only numeric and descriptor rows.
        self.stage_create_binding_effect(trx, definition_effects)
            .await?;

        let table = table_object(table_id, metadata);
        let columns = metadata
            .col
            .columns()
            .iter()
            .map(|column| ColumnObject {
                table_id,
                column_id: column.id,
                storage_ordinal: column.ordinal,
                value_kind: column.value_kind,
                value_flags: column.flags,
            })
            .collect::<Vec<_>>();
        let indexes = metadata
            .idx
            .active_indexes()
            .map(|(_, index)| IndexObject {
                table_id,
                index: index.index,
                index_flags: index.flags,
                keys: index.keys.clone(),
            })
            .collect::<Vec<_>>();

        self.tables().insert(trx, &table).await?;
        self.columns().insert_batch(trx, &columns).await?;
        if !indexes.is_empty() {
            self.indexes().insert_batch(trx, &indexes).await?;
        }
        self.stage_create_descriptor_effect(trx, definition_effects)
            .await?;
        trx.install_ddl_redo(DDLRedo::CreateTable(table_id));
        Ok(())
    }

    /// Stages the canonical catalog cascade for a validated table drop.
    pub(crate) async fn stage_drop_table(
        &self,
        trx: &mut PrivateTransaction,
        table_id: TableID,
        metadata: &TableMetadata,
        definition_effects: &CatalogDefinitionEffects,
    ) -> RuntimeOrFatalResult<()> {
        validate_catalog_engine_health(trx, "stage_drop_table")?;

        let indexes_deleted = self.indexes().delete_by_table_id(trx, table_id).await?;
        if indexes_deleted != metadata.idx.active_index_count() {
            return invalid_drop_catalog_state(format!(
                "drop-table catalog index delete count mismatch: table_id={table_id}, actual={indexes_deleted}, expected={}",
                metadata.idx.active_index_count()
            ));
        }
        let columns_deleted = self.columns().delete_by_table_id(trx, table_id).await?;
        if columns_deleted != metadata.col.col_count() {
            return invalid_drop_catalog_state(format!(
                "drop-table catalog column delete count mismatch: table_id={table_id}, actual={columns_deleted}, expected={}",
                metadata.col.col_count()
            ));
        }
        self.table_replay_silent_watermarks()
            .delete_by_table_id(trx, table_id)
            .await?;
        self.stage_definition_effects(trx, definition_effects)
            .await?;
        let table_deleted = self.tables().delete_by_id(trx, table_id).await?;
        if !table_deleted {
            return invalid_drop_catalog_state(format!(
                "drop-table catalog table row is missing: table_id={table_id}"
            ));
        }
        self.validate_drop_table_absence(trx, table_id).await?;
        trx.install_ddl_redo(DDLRedo::DropTable(table_id));
        Ok(())
    }

    /// Stages canonical catalog metadata for one exact newly allocated index.
    pub(crate) async fn stage_create_index(
        &self,
        trx: &mut PrivateTransaction,
        table_id: TableID,
        index: IndexRef,
        new_metadata: &TableMetadata,
        definition_effects: &CatalogDefinitionEffects,
    ) -> RuntimeOrFatalResult<()> {
        validate_catalog_engine_health(trx, "stage_create_index")?;

        let index_spec = new_metadata
            .idx
            .index_spec(index.slot())
            .filter(|spec| spec.index == index)
            .unwrap_or_else(|| {
                panic!(
                    "create-index prepared metadata lacks exact generation: table_id={table_id}, index={index}"
                )
            });
        let table_replaced = self
            .tables()
            .replace(trx, &table_object(table_id, new_metadata))
            .await?;
        assert!(
            table_replaced,
            "create-index catalog table row is missing: table_id={table_id}"
        );
        self.indexes()
            .insert(
                trx,
                &IndexObject {
                    table_id,
                    index,
                    index_flags: index_spec.flags,
                    keys: index_spec.keys.clone(),
                },
            )
            .await?;
        self.stage_definition_effects(trx, definition_effects)
            .await?;
        trx.install_ddl_redo(DDLRedo::CreateIndex {
            table_id,
            index_id: index.id(),
            index_slot: index.slot(),
        });
        Ok(())
    }

    /// Stages removal of one exact active index and the advanced table epoch.
    pub(crate) async fn stage_drop_index(
        &self,
        trx: &mut PrivateTransaction,
        table_id: TableID,
        index: IndexRef,
        new_metadata: &TableMetadata,
        definition_effects: &CatalogDefinitionEffects,
    ) -> RuntimeOrFatalResult<()> {
        validate_catalog_engine_health(trx, "stage_drop_index")?;

        let index_deleted = self
            .indexes()
            .delete_by_id(trx, table_id, index.id())
            .await?;
        assert!(
            index_deleted,
            "drop-index catalog row is missing: table_id={table_id}, index={index}"
        );
        let table_replaced = self
            .tables()
            .replace(trx, &table_object(table_id, new_metadata))
            .await?;
        assert!(
            table_replaced,
            "drop-index catalog table row is missing: table_id={table_id}"
        );
        self.stage_definition_effects(trx, definition_effects)
            .await?;
        trx.install_ddl_redo(DDLRedo::DropIndex {
            table_id,
            index_id: index.id(),
            index_slot: index.slot(),
        });
        Ok(())
    }

    async fn stage_definition_effects(
        &self,
        trx: &mut PrivateTransaction,
        effects: &CatalogDefinitionEffects,
    ) -> RuntimeOrFatalResult<()> {
        match effects.descriptor() {
            TableDescriptorEffect::Insert(descriptor) => {
                self.table_descriptors().insert(trx, descriptor).await?;
            }
            TableDescriptorEffect::Replace(descriptor) => {
                let replaced = self.table_descriptors().replace(trx, descriptor).await?;
                if !replaced {
                    return invalid_drop_catalog_state(format!(
                        "managed descriptor replacement target is missing: table_id={}",
                        descriptor.table_id
                    ));
                }
            }
            TableDescriptorEffect::None | TableDescriptorEffect::DeleteIfPresent(_) => {}
        }
        match effects.bindings() {
            TableBindingEffect::None => {}
            TableBindingEffect::Insert(bindings) => {
                self.table_bindings().insert_batch(trx, bindings).await?;
            }
            TableBindingEffect::DeleteByTableID(table_id) => {
                self.table_bindings()
                    .delete_by_table_id(trx, *table_id)
                    .await?;
            }
        }
        match effects.descriptor() {
            TableDescriptorEffect::DeleteIfPresent(table_id) => {
                self.table_descriptors()
                    .delete_by_table_id(trx, *table_id)
                    .await?;
            }
            TableDescriptorEffect::None
            | TableDescriptorEffect::Insert(_)
            | TableDescriptorEffect::Replace(_) => {}
        }
        Ok(())
    }

    /// Stages the optimistic binding insertion before invariant-only CREATE DML.
    async fn stage_create_binding_effect(
        &self,
        trx: &mut PrivateTransaction,
        effects: &CatalogDefinitionEffects,
    ) -> QuadResult<()> {
        match effects.bindings() {
            TableBindingEffect::Insert(bindings) => {
                self.table_bindings()
                    .try_insert_unique_batch(trx, bindings)
                    .await?;
            }
            TableBindingEffect::None => {}
            TableBindingEffect::DeleteByTableID(_) => {
                panic!("CREATE TABLE received a binding-delete effect")
            }
        }
        Ok(())
    }

    /// Stages the managed descriptor after all canonical numeric CREATE rows.
    async fn stage_create_descriptor_effect(
        &self,
        trx: &mut PrivateTransaction,
        effects: &CatalogDefinitionEffects,
    ) -> QuadResult<()> {
        match effects.descriptor() {
            TableDescriptorEffect::Insert(descriptor) => {
                self.table_descriptors().insert(trx, descriptor).await?;
            }
            TableDescriptorEffect::None => {}
            TableDescriptorEffect::Replace(_) | TableDescriptorEffect::DeleteIfPresent(_) => {
                panic!("CREATE TABLE received a non-insert descriptor effect")
            }
        }
        Ok(())
    }
}

#[inline]
fn table_object(table_id: TableID, metadata: &TableMetadata) -> TableObject {
    TableObject {
        table_id,
        storage_epoch: metadata.storage_epoch,
        next_column_id: metadata.col.next_column_id(),
        next_index_id: metadata.idx.next_index_id(),
        index_slot_count: metadata.idx.index_slot_count_u32(),
    }
}

#[inline]
fn validate_catalog_engine_health(
    trx: &PrivateTransaction,
    operation: &'static str,
) -> RuntimeOrFatalResult<()> {
    trx.ensure_engine_healthy()
        .map_err(RuntimeOrFatalError::from)
        .attach_with(|| format!("operation={operation}, phase=check_engine_health"))
}

#[inline]
fn invalid_drop_catalog_state(message: String) -> RuntimeOrFatalResult<()> {
    Err(RuntimeOrFatalError::from(
        Report::new(DataIntegrityError::InvalidRootInvariant)
            .attach(message)
            .change_context(RuntimeError::CatalogAccess),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::storage::tests::begin_catalog_test_trx;
    use crate::catalog::tests::open_catalog_test_engine;
    use crate::catalog::{
        BindingNamespaceID, StorageColumnFlags, StorageColumnSpec, TableBindingObject,
        TableDescriptorObject,
    };
    use crate::error::{OperationError, QuadError};
    use crate::session::tests::SessionTestExt;
    use crate::value::ValKind;
    use tempfile::TempDir;

    #[test]
    fn test_create_binding_collision_precedes_numeric_and_descriptor_dml() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = open_catalog_test_engine(temp_dir.path().to_path_buf(), None).await;
            let session = engine.new_session().unwrap();
            let storage = &engine.inner().core.catalog().storage;
            let namespace_id = BindingNamespaceID::new(42);
            let binding_key: Box<[u8]> = Box::from(&b"occupied"[..]);
            let candidate_table_id = TableID::new(90_001);
            let metadata = TableMetadata::try_new(
                vec![StorageColumnSpec::new(
                    ValKind::I32,
                    StorageColumnFlags::empty(),
                )],
                vec![],
            )
            .unwrap();
            let descriptor = TableDescriptorObject {
                table_id: candidate_table_id,
                descriptor_revision: 0,
                compiled_storage_epoch: metadata.storage_epoch,
                storage_schema_fingerprint: metadata.storage_schema_fingerprint(),
                payload: Box::from(&b"candidate"[..]),
            };
            let effects = CatalogDefinitionEffects::insert(
                descriptor,
                vec![TableBindingObject {
                    namespace_id,
                    binding_key: binding_key.clone(),
                    table_id: candidate_table_id,
                }]
                .into_boxed_slice(),
            );
            let mut trx = begin_catalog_test_trx(&session);
            storage
                .table_bindings()
                .insert_batch(
                    trx.trx(),
                    &[TableBindingObject {
                        namespace_id,
                        binding_key,
                        table_id: TableID::new(90_000),
                    }],
                )
                .await
                .unwrap();

            let err = storage
                .stage_create_table(trx.trx(), candidate_table_id, &metadata, &effects)
                .await
                .unwrap_err();
            let QuadError::Operation(report) = err else {
                panic!("binding collision changed error domain")
            };
            assert_eq!(*report.current_context(), OperationError::DuplicateKey);
            assert!(
                storage
                    .tables()
                    .find_uncommitted_by_id(&session.pool_guards(), candidate_table_id)
                    .await
                    .unwrap()
                    .is_none()
            );
            assert!(
                storage
                    .table_descriptors()
                    .find_uncommitted_by_table_id(&session.pool_guards(), candidate_table_id)
                    .await
                    .unwrap()
                    .is_none()
            );

            trx.rollback().await;
        });
    }
}
