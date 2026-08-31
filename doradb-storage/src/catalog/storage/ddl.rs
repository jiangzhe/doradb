use super::{CatalogStorage, ColumnObject, IndexObject, TableObject};
use crate::catalog::{IndexRef, TableMetadata};
use crate::error::{MultiDomainResultExt, RuntimeOrFatalError, RuntimeOrFatalResult};
use crate::id::TableID;
use crate::log::redo::DDLRedo;
use crate::trx::PrivateTransaction;

impl CatalogStorage {
    /// Stages all canonical numeric catalog rows for a newly allocated table.
    pub(crate) async fn stage_create_table(
        &self,
        trx: &mut PrivateTransaction,
        table_id: TableID,
        metadata: &TableMetadata,
    ) -> RuntimeOrFatalResult<()> {
        validate_catalog_engine_health(trx, "stage_create_table")?;

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
        trx.install_ddl_redo(DDLRedo::CreateTable(table_id));
        Ok(())
    }

    /// Stages the canonical catalog cascade for a validated table drop.
    pub(crate) async fn stage_drop_table(
        &self,
        trx: &mut PrivateTransaction,
        table_id: TableID,
        metadata: &TableMetadata,
    ) -> RuntimeOrFatalResult<()> {
        validate_catalog_engine_health(trx, "stage_drop_table")?;

        let indexes_deleted = self.indexes().delete_by_table_id(trx, table_id).await?;
        assert_eq!(
            indexes_deleted,
            metadata.idx.active_index_count(),
            "drop-table catalog index delete count mismatch: table_id={table_id}"
        );
        let columns_deleted = self.columns().delete_by_table_id(trx, table_id).await?;
        assert_eq!(
            columns_deleted,
            metadata.col.col_count(),
            "drop-table catalog column delete count mismatch: table_id={table_id}"
        );
        let table_deleted = self.tables().delete_by_id(trx, table_id).await?;
        assert!(
            table_deleted,
            "drop-table catalog table row is missing: table_id={table_id}"
        );
        self.table_replay_silent_watermarks()
            .delete_by_table_id(trx, table_id)
            .await?;
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
        trx.install_ddl_redo(DDLRedo::DropIndex {
            table_id,
            index_id: index.id(),
            index_slot: index.slot(),
        });
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
