use super::{CatalogStorage, ColumnObject, IndexColumnObject, IndexObject, TableObject};
use crate::catalog::{IndexSlot, TableMetadata};
use crate::error::{MultiDomainResultExt, RuntimeOrFatalError, RuntimeOrFatalResult};
use crate::id::TableID;
use crate::log::redo::DDLRedo;
use crate::trx::PrivateTransaction;

impl CatalogStorage {
    /// Stage all persisted catalog rows for a newly allocated table.
    pub(crate) async fn stage_create_table(
        &self,
        trx: &mut PrivateTransaction,
        table_id: TableID,
        metadata: &TableMetadata,
    ) -> RuntimeOrFatalResult<()> {
        validate_catalog_engine_health(trx, "stage_create_table")?;

        let table = TableObject {
            table_id,
            next_index_slot: metadata.idx.next_index_slot(),
        };
        let columns = metadata
            .col
            .col_names()
            .iter()
            .zip(metadata.col.col_types())
            .zip(metadata.col.col_attrs())
            .enumerate()
            .map(
                |(column_no, ((column_name, column_type), column_attributes))| ColumnObject {
                    table_id,
                    column_no: column_no as u16,
                    column_name: column_name.clone(),
                    column_type: column_type.kind,
                    column_attributes: *column_attributes,
                },
            )
            .collect::<Vec<_>>();
        let indexes = metadata
            .idx
            .active_indexes()
            .map(|(index_slot, index_spec)| IndexObject {
                table_id,
                index_slot,
                index_attributes: index_spec.attributes,
            })
            .collect::<Vec<_>>();
        let index_columns = metadata
            .idx
            .active_indexes()
            .flat_map(|(index_slot, index_spec)| {
                index_spec
                    .cols
                    .iter()
                    .enumerate()
                    .map(move |(index_column_no, index_key)| IndexColumnObject {
                        table_id,
                        index_slot,
                        index_column_no: index_column_no as u16,
                        column_no: index_key.col_no,
                        index_order: index_key.order,
                    })
            })
            .collect::<Vec<_>>();

        self.tables().insert(trx, &table).await?;
        self.columns().insert_batch(trx, &columns).await?;
        if !indexes.is_empty() {
            self.indexes().insert_batch(trx, &indexes).await?;
        }
        if !index_columns.is_empty() {
            self.index_columns()
                .insert_batch(trx, &index_columns)
                .await?;
        }
        trx.install_ddl_redo(DDLRedo::CreateTable(table_id));
        Ok(())
    }

    /// Stage the ordered catalog cascade for a validated table drop.
    pub(crate) async fn stage_drop_table(
        &self,
        trx: &mut PrivateTransaction,
        table_id: TableID,
        metadata: &TableMetadata,
    ) -> RuntimeOrFatalResult<()> {
        validate_catalog_engine_health(trx, "stage_drop_table")?;

        let index_columns_deleted = self
            .index_columns()
            .delete_by_table_id(trx, table_id)
            .await?;
        let expected_index_columns = metadata
            .idx
            .active_indexes()
            .map(|(_, spec)| spec.cols.len())
            .sum::<usize>();
        assert_eq!(
            index_columns_deleted, expected_index_columns,
            "drop-table catalog invariant violated: index-column delete count mismatch, table_id={table_id}"
        );

        let indexes_deleted = self.indexes().delete_by_table_id(trx, table_id).await?;
        assert_eq!(
            indexes_deleted,
            metadata.idx.active_index_count(),
            "drop-table catalog invariant violated: index delete count mismatch, table_id={table_id}"
        );

        let columns_deleted = self.columns().delete_by_table_id(trx, table_id).await?;
        assert_eq!(
            columns_deleted,
            metadata.col.col_count(),
            "drop-table catalog invariant violated: column delete count mismatch, table_id={table_id}"
        );

        let table_deleted = self.tables().delete_by_id(trx, table_id).await?;
        assert!(
            table_deleted,
            "drop-table catalog invariant violated: validated table row is missing, table_id={table_id}"
        );

        self.table_replay_silent_watermarks()
            .delete_by_table_id(trx, table_id)
            .await?;

        trx.install_ddl_redo(DDLRedo::DropTable(table_id));
        Ok(())
    }

    /// Stage persisted metadata for one newly allocated secondary index.
    pub(crate) async fn stage_create_index(
        &self,
        trx: &mut PrivateTransaction,
        table_id: TableID,
        index_slot: IndexSlot,
        new_metadata: &TableMetadata,
    ) -> RuntimeOrFatalResult<()> {
        validate_catalog_engine_health(trx, "stage_create_index")?;

        let expected_next_index_slot = index_slot.checked_next().unwrap_or_else(|| {
            panic!(
                "create-index prepared metadata overflow: table_id={table_id}, index_slot={index_slot}"
            )
        });
        assert_eq!(
            new_metadata.idx.next_index_slot(),
            expected_next_index_slot,
            "create-index prepared metadata mismatch: table_id={table_id}, index_slot={index_slot}"
        );
        let index_spec = new_metadata
            .idx
            .index_spec(index_slot)
            .unwrap_or_else(|| {
                panic!(
                    "create-index prepared metadata has inactive index: table_id={table_id}, index_slot={index_slot}"
                )
            });

        let table_deleted = self
            .tables()
            .replace(
                trx,
                &TableObject {
                    table_id,
                    next_index_slot: new_metadata.idx.next_index_slot(),
                },
            )
            .await?;
        assert!(
            table_deleted,
            "create-index catalog invariant violated: validated table row is missing, table_id={table_id}"
        );
        self.indexes()
            .insert(
                trx,
                &IndexObject {
                    table_id,
                    index_slot,
                    index_attributes: index_spec.attributes,
                },
            )
            .await?;
        if !index_spec.cols.is_empty() {
            let index_columns = index_spec
                .cols
                .iter()
                .enumerate()
                .map(|(index_column_no, index_key)| IndexColumnObject {
                    table_id,
                    index_slot,
                    index_column_no: index_column_no as u16,
                    column_no: index_key.col_no,
                    index_order: index_key.order,
                })
                .collect::<Vec<_>>();
            self.index_columns()
                .insert_batch(trx, &index_columns)
                .await?;
        }

        trx.install_ddl_redo(DDLRedo::CreateIndex {
            table_id,
            index_slot,
        });
        Ok(())
    }

    /// Stage the ordered persisted-row deletion for one active secondary index.
    pub(crate) async fn stage_drop_index(
        &self,
        trx: &mut PrivateTransaction,
        table_id: TableID,
        index_slot: IndexSlot,
        old_metadata: &TableMetadata,
    ) -> RuntimeOrFatalResult<()> {
        validate_catalog_engine_health(trx, "stage_drop_index")?;

        assert!(
            old_metadata.idx.next_index_slot() > index_slot,
            "drop-index prepared metadata mismatch: table_id={table_id}, index_slot={index_slot}, next_index_slot={}",
            old_metadata.idx.next_index_slot()
        );
        let index_spec = old_metadata
            .idx
            .index_spec(index_slot)
            .unwrap_or_else(|| {
                panic!(
                    "drop-index prepared metadata has inactive index: table_id={table_id}, index_slot={index_slot}"
                )
            });

        let deleted_columns = self
            .index_columns()
            .delete_by_index(trx, table_id, index_slot)
            .await?;
        assert_eq!(
            deleted_columns,
            index_spec.cols.len(),
            "drop-index catalog invariant violated: index-column delete count mismatch, table_id={table_id}, index_slot={index_slot}"
        );

        let index_deleted = self
            .indexes()
            .delete_by_id(trx, table_id, index_slot)
            .await?;
        assert!(
            index_deleted,
            "drop-index catalog invariant violated: validated index row is missing, table_id={table_id}, index_slot={index_slot}"
        );

        trx.install_ddl_redo(DDLRedo::DropIndex {
            table_id,
            index_slot,
        });
        Ok(())
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
