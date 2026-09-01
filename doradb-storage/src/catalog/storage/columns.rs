use crate::buffer::PoolGuards;
use crate::catalog::storage::CatalogDefinition;
use crate::catalog::storage::object::ColumnObject;
use crate::catalog::table::{TableColumnLayout, TableMetadata};
use crate::catalog::{CatalogIndexNo, CatalogTable};
use crate::catalog::{
    ColumnID, ColumnOrdinal, StorageColumnFlags, StorageColumnSpec, StorageIndexFlags,
    StorageIndexKey, StorageIndexSpec, catalog_table_id_from_slot,
};
use crate::error::{
    DataIntegrityError, DataIntegrityResult, MultiDomainResultExt, RuntimeError,
    RuntimeOrFatalResult, RuntimeResult,
};
use crate::id::TableID;
#[cfg(test)]
use crate::row::ops::DeleteMvcc;
use crate::row::{Row, RowRead};
use crate::table::IndexLookupCriteria;
use crate::trx::PrivateTransaction;
use crate::value::Val;
use crate::value::ValKind;
use error_stack::Report;
use error_stack::ResultExt;
use std::sync::OnceLock;

pub(super) const TABLE_ID_COLUMNS: TableID = catalog_table_id_from_slot(1);
const COL_NO_COLUMNS_TABLE_ID: usize = 0;
const COL_NO_COLUMNS_COLUMN_ID: usize = 1;
const COL_NO_COLUMNS_STORAGE_ORDINAL: usize = 2;
const COL_NO_COLUMNS_VALUE_KIND: usize = 3;
const COL_NO_COLUMNS_VALUE_FLAGS: usize = 4;
const PK_NO_COLUMNS: CatalogIndexNo = CatalogIndexNo::new(0);

/// Runtime accessor for `catalog.columns`.
pub(crate) struct Columns<'a> {
    pub(super) table: &'a CatalogTable,
}

impl Columns<'_> {
    /// Insert an ordered column batch through one private statement.
    pub(crate) async fn insert_batch(
        &self,
        trx: &mut PrivateTransaction,
        objects: &[ColumnObject],
    ) -> RuntimeOrFatalResult<()> {
        let rows = objects.iter().map(cols_from_column_object).collect();
        trx.catalog_insert_batch_mvcc(self.table, rows)
            .await
            .attach("operation=catalog_columns_insert_batch")
    }

    /// List all columns of one table from uncommitted-visible catalog rows.
    pub(crate) async fn list_uncommitted_by_table_id(
        &self,
        guards: &PoolGuards,
        table_id: TableID,
    ) -> RuntimeResult<Vec<ColumnObject>> {
        let mut res = vec![];
        let mut decode_error = None;
        self.table
            .table_scan_uncommitted(guards, |col_layout, row| {
                if row.is_deleted() {
                    return true;
                }
                match row_to_column_object(col_layout, row) {
                    Ok(obj) if obj.table_id == table_id => res.push(obj),
                    Ok(_) => {}
                    Err(err) => {
                        decode_error = Some(err);
                        return false;
                    }
                }
                true
            })
            .await
            .change_context(RuntimeError::CatalogAccess)
            .attach_with(|| format!("operation=list_catalog_columns, table_id={table_id}"))?;
        if let Some(err) = decode_error {
            return Err(err
                .change_context(RuntimeError::CatalogAccess)
                .attach("operation=list_catalog_columns, phase=decode_row"));
        }
        Ok(res)
    }

    /// Delete a column by `(table_id, column_id)`.
    #[cfg(test)]
    pub(crate) async fn delete_by_id(
        &self,
        trx: &mut PrivateTransaction,
        table_id: TableID,
        column_id: ColumnID,
    ) -> RuntimeOrFatalResult<bool> {
        let key_vals = vec![Val::from(table_id), Val::from(column_id.get())];
        let res = trx
            .catalog_delete_primary_key_mvcc(self.table, PK_NO_COLUMNS, key_vals)
            .await
            .attach_with(|| {
                format!(
                    "operation=catalog_columns_delete, table_id={table_id}, column_id={column_id}"
                )
            })?;
        Ok(matches!(res, DeleteMvcc::Deleted))
    }

    /// Delete all columns for one table and return the number of deleted rows.
    pub(crate) async fn delete_by_table_id(
        &self,
        trx: &mut PrivateTransaction,
        table_id: TableID,
    ) -> RuntimeOrFatalResult<usize> {
        let columns = self.list_current_locked_by_table_id(trx, table_id).await?;
        let keys = columns
            .into_iter()
            .map(|column| vec![Val::from(table_id), Val::from(column.column_id.get())])
            .collect();
        trx.catalog_delete_primary_key_batch_mvcc(self.table, PK_NO_COLUMNS, keys)
            .await
            .attach_with(|| {
                format!("operation=catalog_columns_delete_by_table, table_id={table_id}")
            })
    }

    /// Lists one table's columns through its bounded primary-key range in the
    /// owning DDL transaction's locked current view.
    async fn list_current_locked_by_table_id(
        &self,
        trx: &PrivateTransaction,
        table_id: TableID,
    ) -> RuntimeResult<Vec<ColumnObject>> {
        let lower = [Val::from(table_id), Val::from(0u32)];
        let upper = [Val::from(table_id), Val::from(u32::MAX)];
        let mut columns = Vec::new();
        let mut decode_error = None;
        self.table
            .index_lookup_current_locked(
                trx,
                PK_NO_COLUMNS,
                IndexLookupCriteria::UniqueInclusive {
                    lower: &lower,
                    upper: &upper,
                },
                |col_layout, row| match row_to_column_object(col_layout, row) {
                    Ok(column) => {
                        columns.push(column);
                        true
                    }
                    Err(err) => {
                        decode_error = Some(err);
                        false
                    }
                },
            )
            .await
            .attach_with(|| {
                format!("operation=list_locked_catalog_columns, table_id={table_id}")
            })?;
        if let Some(err) = decode_error {
            return Err(err
                .change_context(RuntimeError::CatalogAccess)
                .attach("operation=list_locked_catalog_columns, phase=decode_row"));
        }
        Ok(columns)
    }
}

#[inline]
fn cols_from_column_object(obj: &ColumnObject) -> Vec<Val> {
    vec![
        Val::from(obj.table_id),
        Val::from(obj.column_id.get()),
        Val::from(obj.storage_ordinal.get()),
        Val::from(obj.value_kind as u32),
        Val::from(obj.value_flags.bits()),
    ]
}

/// Return static table definition of `catalog.columns`.
pub(super) fn catalog_definition_of_columns() -> &'static CatalogDefinition {
    static DEF: OnceLock<CatalogDefinition> = OnceLock::new();
    DEF.get_or_init(|| {
        CatalogDefinition {
            table_id: TABLE_ID_COLUMNS,
            metadata: TableMetadata::try_new(
                vec![
                    // table_id U64: owning user table.
                    StorageColumnSpec::new(ValKind::U64, StorageColumnFlags::empty()),
                    // column_id U32: stable table-local column identity.
                    StorageColumnSpec::new(ValKind::U32, StorageColumnFlags::empty()),
                    // storage_ordinal U16: physical position in stored rows.
                    StorageColumnSpec::new(ValKind::U16, StorageColumnFlags::empty()),
                    // value_kind U32: encoded logical value kind.
                    StorageColumnSpec::new(ValKind::U32, StorageColumnFlags::empty()),
                    // value_flags U32: storage column flags such as NULLABLE.
                    StorageColumnSpec::new(ValKind::U32, StorageColumnFlags::empty()),
                ],
                vec![
                    // Primary key: (table_id, column_id).
                    // Unique physical-position mapping: (table_id, storage_ordinal).
                    StorageIndexSpec::new(
                        vec![StorageIndexKey::new(0), StorageIndexKey::new(1)],
                        StorageIndexFlags::PK,
                    ),
                    StorageIndexSpec::new(
                        vec![StorageIndexKey::new(0), StorageIndexKey::new(2)],
                        StorageIndexFlags::UK,
                    ),
                ],
            )
            .expect("valid table metadata"),
        }
    })
}

#[inline]
fn row_to_column_object(
    col_layout: &TableColumnLayout,
    row: Row<'_>,
) -> DataIntegrityResult<ColumnObject> {
    let vals = (0..5)
        .map(|idx| row.val(col_layout, idx))
        .collect::<Vec<_>>();
    column_object_from_vals(&vals)
}

pub(super) fn column_object_from_vals(vals: &[Val]) -> DataIntegrityResult<ColumnObject> {
    if vals.len() != 5 {
        return Err(
            Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                "catalog.columns value count {}, expected 5",
                vals.len()
            )),
        );
    }
    let table_id = vals[COL_NO_COLUMNS_TABLE_ID]
        .as_u64()
        .map(TableID::from)
        .ok_or_else(|| Report::new(DataIntegrityError::InvalidPayload))?;
    let column_id = vals[COL_NO_COLUMNS_COLUMN_ID]
        .as_u32()
        .map(ColumnID::new)
        .ok_or_else(|| Report::new(DataIntegrityError::InvalidPayload))?;
    let storage_ordinal = vals[COL_NO_COLUMNS_STORAGE_ORDINAL]
        .as_u16()
        .map(ColumnOrdinal::new)
        .ok_or_else(|| Report::new(DataIntegrityError::InvalidPayload))?;
    let value_kind_raw = vals[COL_NO_COLUMNS_VALUE_KIND]
        .as_u32()
        .ok_or_else(|| Report::new(DataIntegrityError::InvalidPayload))?;
    let value_kind = ValKind::try_from(value_kind_raw).map_err(|_| {
        Report::new(DataIntegrityError::InvalidPayload).attach(format!(
            "unknown catalog column value kind {value_kind_raw}"
        ))
    })?;
    let value_flags_raw = vals[COL_NO_COLUMNS_VALUE_FLAGS]
        .as_u32()
        .ok_or_else(|| Report::new(DataIntegrityError::InvalidPayload))?;
    let value_flags = StorageColumnFlags::from_bits(value_flags_raw).ok_or_else(|| {
        Report::new(DataIntegrityError::InvalidPayload)
            .attach(format!("unknown catalog column flags {value_flags_raw:#x}"))
    })?;
    Ok(ColumnObject {
        table_id,
        column_id,
        storage_ordinal,
        value_kind,
        value_flags,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::storage::tests::begin_catalog_test_trx;
    use crate::catalog::tests::open_catalog_test_engine;
    use crate::log::redo::DDLRedo;
    use crate::session::tests::SessionTestExt;
    use tempfile::TempDir;

    #[test]
    fn test_columns_delete_by_id() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = open_catalog_test_engine(main_dir, None).await;
            let session = engine.new_session().unwrap();

            let col_42_0 = ColumnObject {
                table_id: TableID::new(42),
                column_id: ColumnID::new(0),
                storage_ordinal: ColumnOrdinal::new(0),
                value_kind: ValKind::U32,
                value_flags: StorageColumnFlags::empty(),
            };
            let col_42_1 = ColumnObject {
                table_id: TableID::new(42),
                column_id: ColumnID::new(1),
                storage_ordinal: ColumnOrdinal::new(1),
                value_kind: ValKind::U64,
                value_flags: StorageColumnFlags::empty(),
            };
            let col_43_0 = ColumnObject {
                table_id: TableID::new(43),
                column_id: ColumnID::new(0),
                storage_ordinal: ColumnOrdinal::new(0),
                value_kind: ValKind::U16,
                value_flags: StorageColumnFlags::empty(),
            };

            let mut trx = begin_catalog_test_trx(&session);
            engine
                .inner()
                .core
                .catalog()
                .storage
                .columns()
                .insert_batch(trx.trx(), &[col_42_0, col_42_1, col_43_0])
                .await
                .unwrap();
            trx.commit(DDLRedo::CreateTable(TableID::new(42))).await;

            let mut trx = begin_catalog_test_trx(&session);
            assert!(
                engine
                    .inner()
                    .core
                    .catalog()
                    .storage
                    .columns()
                    .delete_by_id(trx.trx(), TableID::new(42), ColumnID::new(1))
                    .await
                    .unwrap()
            );
            assert!(
                !engine
                    .inner()
                    .core
                    .catalog()
                    .storage
                    .columns()
                    .delete_by_id(trx.trx(), TableID::new(42), ColumnID::new(9))
                    .await
                    .unwrap()
            );
            trx.commit(DDLRedo::DropTable(TableID::new(42))).await;

            let cols_42 = engine
                .inner()
                .core
                .catalog()
                .storage
                .columns()
                .list_uncommitted_by_table_id(&session.pool_guards(), TableID::new(42))
                .await
                .unwrap();
            assert_eq!(cols_42.len(), 1);
            assert_eq!(cols_42[0].column_id, ColumnID::new(0));

            let cols_43 = engine
                .inner()
                .core
                .catalog()
                .storage
                .columns()
                .list_uncommitted_by_table_id(&session.pool_guards(), TableID::new(43))
                .await
                .unwrap();
            assert_eq!(cols_43.len(), 1);
            assert_eq!(cols_43[0].column_id, ColumnID::new(0));

            let mut trx = begin_catalog_test_trx(&session);
            assert!(
                !engine
                    .inner()
                    .core
                    .catalog()
                    .storage
                    .columns()
                    .delete_by_id(trx.trx(), TableID::new(42), ColumnID::new(1))
                    .await
                    .unwrap()
            );
            assert!(
                engine
                    .inner()
                    .core
                    .catalog()
                    .storage
                    .columns()
                    .delete_by_id(trx.trx(), TableID::new(42), ColumnID::new(0))
                    .await
                    .unwrap()
            );
            assert!(
                engine
                    .inner()
                    .core
                    .catalog()
                    .storage
                    .columns()
                    .delete_by_id(trx.trx(), TableID::new(43), ColumnID::new(0))
                    .await
                    .unwrap()
            );
            trx.commit(DDLRedo::DropTable(TableID::new(42))).await;

            assert!(
                engine
                    .inner()
                    .core
                    .catalog()
                    .storage
                    .columns()
                    .list_uncommitted_by_table_id(&session.pool_guards(), TableID::new(42))
                    .await
                    .unwrap()
                    .is_empty()
            );
            assert!(
                engine
                    .inner()
                    .core
                    .catalog()
                    .storage
                    .columns()
                    .list_uncommitted_by_table_id(&session.pool_guards(), TableID::new(43))
                    .await
                    .unwrap()
                    .is_empty()
            );

            drop(session);
            drop(engine);
        });
    }

    #[test]
    fn test_columns_delete_by_table_id_counts_and_is_idempotent() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = open_catalog_test_engine(main_dir, None).await;
            let session = engine.new_session().unwrap();

            let columns = [
                ColumnObject {
                    table_id: TableID::new(42),
                    column_id: ColumnID::new(0),
                    storage_ordinal: ColumnOrdinal::new(0),
                    value_kind: ValKind::U32,
                    value_flags: StorageColumnFlags::empty(),
                },
                ColumnObject {
                    table_id: TableID::new(42),
                    column_id: ColumnID::new(1),
                    storage_ordinal: ColumnOrdinal::new(1),
                    value_kind: ValKind::U64,
                    value_flags: StorageColumnFlags::empty(),
                },
                ColumnObject {
                    table_id: TableID::new(43),
                    column_id: ColumnID::new(0),
                    storage_ordinal: ColumnOrdinal::new(0),
                    value_kind: ValKind::U16,
                    value_flags: StorageColumnFlags::empty(),
                },
            ];

            let mut trx = begin_catalog_test_trx(&session);
            engine
                .inner()
                .core
                .catalog()
                .storage
                .columns()
                .insert_batch(trx.trx(), &columns)
                .await
                .unwrap();
            trx.commit(DDLRedo::CreateTable(TableID::new(42))).await;

            let mut trx = begin_catalog_test_trx(&session);
            assert_eq!(
                engine
                    .inner()
                    .core
                    .catalog()
                    .storage
                    .columns()
                    .delete_by_table_id(trx.trx(), TableID::new(42))
                    .await
                    .unwrap(),
                2
            );
            assert_eq!(
                engine
                    .inner()
                    .core
                    .catalog()
                    .storage
                    .columns()
                    .delete_by_table_id(trx.trx(), TableID::new(42))
                    .await
                    .unwrap(),
                0
            );
            trx.commit(DDLRedo::DropTable(TableID::new(42))).await;

            assert!(
                engine
                    .inner()
                    .core
                    .catalog()
                    .storage
                    .columns()
                    .list_uncommitted_by_table_id(&session.pool_guards(), TableID::new(42))
                    .await
                    .unwrap()
                    .is_empty()
            );
            let remaining = engine
                .inner()
                .core
                .catalog()
                .storage
                .columns()
                .list_uncommitted_by_table_id(&session.pool_guards(), TableID::new(43))
                .await
                .unwrap();
            assert_eq!(remaining.len(), 1);
            assert_eq!(remaining[0].column_id, ColumnID::new(0));

            drop(session);
            drop(engine);
        });
    }
}
