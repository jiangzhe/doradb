use crate::buffer::PoolGuards;
use crate::catalog::CatalogTable;
use crate::catalog::storage::CatalogDefinition;
use crate::catalog::storage::object::ColumnObject;
use crate::catalog::table::{TableColumnLayout, TableMetadata};
use crate::catalog::{
    ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexSpec, catalog_table_id_from_slot,
};
use crate::error::{MultiDomainResultExt, RuntimeError, RuntimeOrFatalResult, RuntimeResult};
use crate::id::TableID;
#[cfg(test)]
use crate::row::ops::DeleteMvcc;
use crate::row::{Row, RowRead};
use crate::trx::PrivateTransaction;
use crate::value::Val;
use crate::value::ValKind;
use error_stack::ResultExt;
use semistr::SemiStr;
use std::sync::OnceLock;

pub(super) const TABLE_ID_COLUMNS: TableID = catalog_table_id_from_slot(1);
const COL_NO_COLUMNS_TABLE_ID: usize = 0;
const COL_NAME_COLUMNS_TABLE_ID: &str = "table_id";
const COL_NO_COLUMNS_COLUMN_NO: usize = 1;
const COL_NAME_COLUMNS_COLUMN_NO: &str = "column_no";
const COL_NO_COLUMNS_COLUMN_NAME: usize = 2;
const COL_NAME_COLUMNS_COLUMN_NAME: &str = "column_name";
const COL_NO_COLUMNS_COLUMN_TYPE: usize = 3;
const COL_NAME_COLUMNS_COLUMN_TYPE: &str = "column_type";
const COL_NO_COLUMNS_COLUMN_ATTRIBUTES: usize = 4;
const COL_NAME_COLUMNS_COLUMN_ATTRIBUTES: &str = "column_attributes";
const PK_NO_COLUMNS: usize = 0;

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
        self.table
            .table_scan_uncommitted(guards, |col_layout, row| {
                if row.is_deleted() {
                    return true;
                }
                // filter by table id before deserializing the whole object.
                let table_id_in_row = row
                    .val(col_layout, COL_NO_COLUMNS_TABLE_ID)
                    .as_u64()
                    .unwrap();
                if table_id_in_row == table_id.as_u64() {
                    let obj = row_to_column_object(col_layout, row);
                    res.push(obj);
                }
                true
            })
            .await
            .change_context(RuntimeError::CatalogAccess)
            .attach_with(|| format!("operation=list_catalog_columns, table_id={table_id}"))?;
        Ok(res)
    }

    /// Delete a column by (table_id, column_no).
    #[cfg(test)]
    pub(crate) async fn delete_by_id(
        &self,
        trx: &mut PrivateTransaction,
        table_id: TableID,
        column_no: u16,
    ) -> RuntimeOrFatalResult<bool> {
        let key_vals = vec![Val::from(table_id), Val::from(column_no)];
        let res = trx
            .catalog_delete_primary_key_mvcc(self.table, PK_NO_COLUMNS, key_vals)
            .await
            .attach_with(|| {
                format!(
                    "operation=catalog_columns_delete, table_id={table_id}, column_no={column_no}"
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
        let columns = self
            .list_uncommitted_by_table_id(trx.pool_guards(), table_id)
            .await?;
        let keys = columns
            .into_iter()
            .map(|column| vec![Val::from(table_id), Val::from(column.column_no)])
            .collect();
        trx.catalog_delete_primary_key_batch_mvcc(self.table, PK_NO_COLUMNS, keys)
            .await
            .attach_with(|| {
                format!("operation=catalog_columns_delete_by_table, table_id={table_id}")
            })
    }
}

#[inline]
fn cols_from_column_object(obj: &ColumnObject) -> Vec<Val> {
    vec![
        Val::from(obj.table_id),
        Val::from(obj.column_no),
        Val::from(obj.column_name.as_str()),
        Val::from(obj.column_type as u32),
        Val::from(obj.column_attributes.bits()),
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
                    // table_id unsigned bigint not null
                    ColumnSpec {
                        column_name: SemiStr::new(COL_NAME_COLUMNS_TABLE_ID),
                        column_type: ValKind::U64,
                        column_attributes: ColumnAttributes::INDEX,
                    },
                    // column_no unsigned smallint not null
                    ColumnSpec {
                        column_name: SemiStr::new(COL_NAME_COLUMNS_COLUMN_NO),
                        column_type: ValKind::U16,
                        column_attributes: ColumnAttributes::INDEX,
                    },
                    // column_name string not null
                    ColumnSpec {
                        column_name: SemiStr::new(COL_NAME_COLUMNS_COLUMN_NAME),
                        column_type: ValKind::VarByte,
                        column_attributes: ColumnAttributes::empty(),
                    },
                    // column_type unsgined int not null
                    ColumnSpec {
                        column_name: SemiStr::new(COL_NAME_COLUMNS_COLUMN_TYPE),
                        column_type: ValKind::U32,
                        column_attributes: ColumnAttributes::empty(),
                    },
                    // column_attributes unsgined int not null
                    ColumnSpec {
                        column_name: SemiStr::new(COL_NAME_COLUMNS_COLUMN_ATTRIBUTES),
                        column_type: ValKind::U32,
                        column_attributes: ColumnAttributes::empty(),
                    },
                ],
                vec![
                    // primary key pk_columns (table_id, column_no)
                    IndexSpec::new(
                        vec![IndexKey::new(0), IndexKey::new(1)],
                        IndexAttributes::PK,
                    ),
                ],
            )
            .expect("valid table metadata"),
        }
    })
}

#[inline]
fn row_to_column_object(col_layout: &TableColumnLayout, row: Row<'_>) -> ColumnObject {
    let table_id = TableID::from(
        row.val(col_layout, COL_NO_COLUMNS_TABLE_ID)
            .as_u64()
            .unwrap(),
    );
    let column_no = row
        .val(col_layout, COL_NO_COLUMNS_COLUMN_NO)
        .as_u16()
        .unwrap();
    let column_name = row.str(COL_NO_COLUMNS_COLUMN_NAME).unwrap();
    let column_type = row
        .val(col_layout, COL_NO_COLUMNS_COLUMN_TYPE)
        .as_u32()
        .unwrap();
    let column_attributes = row
        .val(col_layout, COL_NO_COLUMNS_COLUMN_ATTRIBUTES)
        .as_u32()
        .unwrap();
    ColumnObject {
        table_id,
        column_no,
        column_name: SemiStr::new(column_name),
        // Invariant: production catalog writers derive this field directly
        // from `ValKind`, and checkpoint/redo preserve that u32 code without
        // narrowing it. An unknown code therefore indicates an internal
        // catalog writer or recovery invariant violation.
        column_type: ValKind::decode(column_type)
            .expect("validated catalog column row must contain a known value kind"),
        column_attributes: ColumnAttributes::from_bits_truncate(column_attributes),
    }
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
                column_no: 0,
                column_name: SemiStr::new("c0"),
                column_type: ValKind::U32,
                column_attributes: ColumnAttributes::empty(),
            };
            let col_42_1 = ColumnObject {
                table_id: TableID::new(42),
                column_no: 1,
                column_name: SemiStr::new("c1"),
                column_type: ValKind::U64,
                column_attributes: ColumnAttributes::empty(),
            };
            let col_43_0 = ColumnObject {
                table_id: TableID::new(43),
                column_no: 0,
                column_name: SemiStr::new("c0"),
                column_type: ValKind::U16,
                column_attributes: ColumnAttributes::empty(),
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
                    .delete_by_id(trx.trx(), TableID::new(42), 1)
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
                    .delete_by_id(trx.trx(), TableID::new(42), 9)
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
            assert_eq!(cols_42[0].column_no, 0);

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
            assert_eq!(cols_43[0].column_no, 0);

            let mut trx = begin_catalog_test_trx(&session);
            assert!(
                !engine
                    .inner()
                    .core
                    .catalog()
                    .storage
                    .columns()
                    .delete_by_id(trx.trx(), TableID::new(42), 1)
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
                    .delete_by_id(trx.trx(), TableID::new(42), 0)
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
                    .delete_by_id(trx.trx(), TableID::new(43), 0)
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
                    column_no: 0,
                    column_name: SemiStr::new("c0"),
                    column_type: ValKind::U32,
                    column_attributes: ColumnAttributes::empty(),
                },
                ColumnObject {
                    table_id: TableID::new(42),
                    column_no: 1,
                    column_name: SemiStr::new("c1"),
                    column_type: ValKind::U64,
                    column_attributes: ColumnAttributes::empty(),
                },
                ColumnObject {
                    table_id: TableID::new(43),
                    column_no: 0,
                    column_name: SemiStr::new("other"),
                    column_type: ValKind::U16,
                    column_attributes: ColumnAttributes::empty(),
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
            assert_eq!(remaining[0].column_no, 0);

            drop(session);
            drop(engine);
        });
    }
}
