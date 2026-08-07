use super::{CatalogStorage, ColumnObject, IndexColumnObject, IndexObject, TableObject};
use crate::catalog::{IndexNo, TableMetadata};
use crate::error::{RuntimeError, RuntimeResult};
use crate::id::TableID;
use crate::log::redo::DDLRedo;
use crate::trx::PrivateTransaction;
use error_stack::ResultExt;

impl CatalogStorage {
    /// Stage all persisted catalog rows for a newly allocated table.
    pub(crate) async fn stage_create_table(
        &self,
        trx: &mut PrivateTransaction,
        table_id: TableID,
        metadata: &TableMetadata,
    ) -> RuntimeResult<()> {
        validate_catalog_engine_health(trx, "stage_create_table")?;

        let table = TableObject {
            table_id,
            next_index_no: metadata.idx.next_index_no(),
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
            .map(|(index_no, index_spec)| IndexObject {
                table_id,
                index_no: index_no as IndexNo,
                index_attributes: index_spec.attributes,
            })
            .collect::<Vec<_>>();
        let index_columns = metadata
            .idx
            .active_indexes()
            .flat_map(|(index_no, index_spec)| {
                index_spec
                    .cols
                    .iter()
                    .enumerate()
                    .map(move |(index_column_no, index_key)| IndexColumnObject {
                        table_id,
                        index_no: index_no as IndexNo,
                        index_column_no: index_column_no as u16,
                        column_no: index_key.col_no,
                        index_order: index_key.order,
                    })
            })
            .collect::<Vec<_>>();

        trx.stage_statement(async |stmt| self.tables().insert(stmt, &table).await)
            .await?;
        trx.stage_statement(async |stmt| {
            for column in &columns {
                self.columns().insert(stmt, column).await?;
            }
            Ok(())
        })
        .await?;
        if !indexes.is_empty() {
            trx.stage_statement(async |stmt| {
                for index in &indexes {
                    self.indexes().insert(stmt, index).await?;
                }
                Ok(())
            })
            .await?;
        }
        if !index_columns.is_empty() {
            trx.stage_statement(async |stmt| {
                for index_column in &index_columns {
                    self.index_columns().insert(stmt, index_column).await?;
                }
                Ok(())
            })
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
    ) -> RuntimeResult<()> {
        validate_catalog_engine_health(trx, "stage_drop_table")?;

        let index_columns_deleted = trx
            .stage_statement(async |stmt| {
                self.index_columns()
                    .delete_by_table_id(stmt, table_id)
                    .await
            })
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

        let indexes_deleted = trx
            .stage_statement(async |stmt| self.indexes().delete_by_table_id(stmt, table_id).await)
            .await?;
        assert_eq!(
            indexes_deleted,
            metadata.idx.active_index_count(),
            "drop-table catalog invariant violated: index delete count mismatch, table_id={table_id}"
        );

        let columns_deleted = trx
            .stage_statement(async |stmt| self.columns().delete_by_table_id(stmt, table_id).await)
            .await?;
        assert_eq!(
            columns_deleted,
            metadata.col.col_count(),
            "drop-table catalog invariant violated: column delete count mismatch, table_id={table_id}"
        );

        let table_deleted = trx
            .stage_statement(async |stmt| self.tables().delete_by_id(stmt, table_id).await)
            .await?;
        assert!(
            table_deleted,
            "drop-table catalog invariant violated: validated table row is missing, table_id={table_id}"
        );

        trx.stage_statement(async |stmt| {
            self.table_replay_silent_watermarks()
                .delete_by_table_id(stmt, table_id)
                .await
        })
        .await?;

        trx.install_ddl_redo(DDLRedo::DropTable(table_id));
        Ok(())
    }

    /// Stage persisted metadata for one newly allocated secondary index.
    pub(crate) async fn stage_create_index(
        &self,
        trx: &mut PrivateTransaction,
        table_id: TableID,
        index_no: IndexNo,
        new_metadata: &TableMetadata,
    ) -> RuntimeResult<()> {
        validate_catalog_engine_health(trx, "stage_create_index")?;

        let expected_next_index_no = index_no.checked_add(1).unwrap_or_else(|| {
            panic!(
                "create-index prepared metadata overflow: table_id={table_id}, index_no={index_no}"
            )
        });
        assert_eq!(
            new_metadata.idx.next_index_no(),
            expected_next_index_no,
            "create-index prepared metadata mismatch: table_id={table_id}, index_no={index_no}"
        );
        let index_spec = new_metadata
            .idx
            .index_spec(usize::from(index_no))
            .unwrap_or_else(|| {
                panic!(
                    "create-index prepared metadata has inactive index: table_id={table_id}, index_no={index_no}"
                )
            });

        trx.stage_statement(async |stmt| {
            let table_deleted = self.tables().delete_by_id(stmt, table_id).await?;
            assert!(
                table_deleted,
                "create-index catalog invariant violated: validated table row is missing, table_id={table_id}"
            );
            self.tables()
                .insert(
                    stmt,
                    &TableObject {
                        table_id,
                        next_index_no: new_metadata.idx.next_index_no(),
                    },
                )
                .await
        })
        .await?;
        trx.stage_statement(async |stmt| {
            self.indexes()
                .insert(
                    stmt,
                    &IndexObject {
                        table_id,
                        index_no,
                        index_attributes: index_spec.attributes,
                    },
                )
                .await
        })
        .await?;
        if !index_spec.cols.is_empty() {
            trx.stage_statement(async |stmt| {
                for (index_column_no, index_key) in index_spec.cols.iter().enumerate() {
                    self.index_columns()
                        .insert(
                            stmt,
                            &IndexColumnObject {
                                table_id,
                                index_no,
                                index_column_no: index_column_no as u16,
                                column_no: index_key.col_no,
                                index_order: index_key.order,
                            },
                        )
                        .await?;
                }
                Ok(())
            })
            .await?;
        }

        trx.install_ddl_redo(DDLRedo::CreateIndex { table_id, index_no });
        Ok(())
    }

    /// Stage the ordered persisted-row deletion for one active secondary index.
    pub(crate) async fn stage_drop_index(
        &self,
        trx: &mut PrivateTransaction,
        table_id: TableID,
        index_no: IndexNo,
        old_metadata: &TableMetadata,
    ) -> RuntimeResult<()> {
        validate_catalog_engine_health(trx, "stage_drop_index")?;

        assert!(
            old_metadata.idx.next_index_no() > index_no,
            "drop-index prepared metadata mismatch: table_id={table_id}, index_no={index_no}, next_index_no={}",
            old_metadata.idx.next_index_no()
        );
        let index_spec = old_metadata
            .idx
            .index_spec(usize::from(index_no))
            .unwrap_or_else(|| {
                panic!(
                    "drop-index prepared metadata has inactive index: table_id={table_id}, index_no={index_no}"
                )
            });

        let deleted_columns = trx
            .stage_statement(async |stmt| {
                self.index_columns()
                    .delete_by_index(stmt, table_id, index_no)
                    .await
            })
            .await?;
        assert_eq!(
            deleted_columns,
            index_spec.cols.len(),
            "drop-index catalog invariant violated: index-column delete count mismatch, table_id={table_id}, index_no={index_no}"
        );

        let index_deleted = trx
            .stage_statement(async |stmt| {
                self.indexes().delete_by_id(stmt, table_id, index_no).await
            })
            .await?;
        assert!(
            index_deleted,
            "drop-index catalog invariant violated: validated index row is missing, table_id={table_id}, index_no={index_no}"
        );

        trx.install_ddl_redo(DDLRedo::DropIndex { table_id, index_no });
        Ok(())
    }
}

#[inline]
fn validate_catalog_engine_health(
    trx: &PrivateTransaction,
    operation: &'static str,
) -> RuntimeResult<()> {
    trx.ensure_engine_healthy()
        .change_context(RuntimeError::CatalogAccess)
        .attach_with(|| format!("operation={operation}, phase=check_engine_health"))
}
