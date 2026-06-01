use crate::buffer::PoolGuards;
use crate::catalog::CatalogTable;
use crate::catalog::storage::CatalogDefinition;
use crate::catalog::storage::object::ColumnObject;
use crate::catalog::table::{TableColumnLayout, TableMetadata};
use crate::catalog::{ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexSpec};
use crate::error::Result;
use crate::id::TableID;
use crate::row::ops::{DeleteMvcc, SelectKey};
use crate::row::{Row, RowRead};
use crate::trx::stmt::Statement;
use crate::value::Val;
use crate::value::ValKind;
use semistr::SemiStr;
use std::sync::OnceLock;

pub(super) const TABLE_ID_COLUMNS: TableID = TableID::new(1);
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
        column_type: ValKind::try_from(column_type as u8).unwrap(),
        column_attributes: ColumnAttributes::from_bits_truncate(column_attributes),
    }
}

/// Runtime accessor for `catalog.columns`.
pub(crate) struct Columns<'a> {
    pub(super) table: &'a CatalogTable,
}

impl Columns<'_> {
    /// Insert a column.
    pub(crate) async fn insert(&self, stmt: &mut Statement<'_>, obj: &ColumnObject) -> bool {
        let cols = vec![
            Val::from(obj.table_id),
            Val::from(obj.column_no),
            Val::from(obj.column_name.as_str()),
            Val::from(obj.column_type as u32),
            Val::from(obj.column_attributes.bits()),
        ];
        stmt.catalog_insert_mvcc(self.table, cols).await.is_ok()
    }

    /// List all columns of one table from uncommitted-visible catalog rows.
    pub(crate) async fn list_uncommitted_by_table_id(
        &self,
        guards: &PoolGuards,
        table_id: TableID,
    ) -> Result<Vec<ColumnObject>> {
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
            .await?;
        Ok(res)
    }

    /// Delete a column by (table_id, column_no).
    pub(crate) async fn delete_by_id(
        &self,
        stmt: &mut Statement<'_>,
        table_id: TableID,
        column_no: u16,
    ) -> bool {
        let key = SelectKey::new(
            PK_NO_COLUMNS,
            vec![Val::from(table_id), Val::from(column_no)],
        );
        stmt.catalog_delete_unique_mvcc(self.table, &key, true)
            .await
            .is_ok_and(|res| matches!(res, DeleteMvcc::Deleted))
    }

    /// Delete all columns for one table and return the number of deleted rows.
    pub(crate) async fn delete_by_table_id(
        &self,
        stmt: &mut Statement<'_>,
        table_id: TableID,
    ) -> Result<usize> {
        let Some(guards) = stmt.ctx().pool_guards().cloned() else {
            return Ok(0);
        };
        let columns = self.list_uncommitted_by_table_id(&guards, table_id).await?;
        let mut deleted = 0;
        for column in columns {
            if self.delete_by_id(stmt, table_id, column.column_no).await {
                deleted += 1;
            }
        }
        Ok(deleted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::tests::open_catalog_test_engine;
    use crate::session::tests::SessionTestExt;
    use crate::trx::ActiveTrx;
    use crate::trx::redo::DDLRedo;
    use tempfile::TempDir;

    fn mark_catalog_ddl(trx: &mut ActiveTrx, ddl: DDLRedo) {
        let old = trx.effects_mut().redo_mut().ddl.replace(Box::new(ddl));
        debug_assert!(old.is_none());
    }

    #[test]
    fn test_columns_delete_by_id() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = open_catalog_test_engine(main_dir, None).await;
            let mut session = engine.new_session().unwrap();

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

            let mut trx = session.begin_trx().unwrap();
            trx.exec(async |stmt| {
                assert!(
                    engine
                        .catalog()
                        .storage
                        .columns()
                        .insert(stmt, &col_42_0)
                        .await
                );
                assert!(
                    engine
                        .catalog()
                        .storage
                        .columns()
                        .insert(stmt, &col_42_1)
                        .await
                );
                assert!(
                    engine
                        .catalog()
                        .storage
                        .columns()
                        .insert(stmt, &col_43_0)
                        .await
                );
                Ok(())
            })
            .await
            .unwrap();
            mark_catalog_ddl(&mut trx, DDLRedo::CreateTable(TableID::new(42)));
            trx.commit().await.unwrap();

            let mut trx = session.begin_trx().unwrap();
            trx.exec(async |stmt| {
                assert!(
                    engine
                        .catalog()
                        .storage
                        .columns()
                        .delete_by_id(stmt, TableID::new(42), 1)
                        .await
                );
                assert!(
                    !engine
                        .catalog()
                        .storage
                        .columns()
                        .delete_by_id(stmt, TableID::new(42), 9)
                        .await
                );
                Ok(())
            })
            .await
            .unwrap();
            mark_catalog_ddl(&mut trx, DDLRedo::DropTable(TableID::new(42)));
            trx.commit().await.unwrap();

            let cols_42 = engine
                .catalog()
                .storage
                .columns()
                .list_uncommitted_by_table_id(&session.pool_guards(), TableID::new(42))
                .await
                .unwrap();
            assert_eq!(cols_42.len(), 1);
            assert_eq!(cols_42[0].column_no, 0);

            let cols_43 = engine
                .catalog()
                .storage
                .columns()
                .list_uncommitted_by_table_id(&session.pool_guards(), TableID::new(43))
                .await
                .unwrap();
            assert_eq!(cols_43.len(), 1);
            assert_eq!(cols_43[0].column_no, 0);

            let mut trx = session.begin_trx().unwrap();
            trx.exec(async |stmt| {
                assert!(
                    !engine
                        .catalog()
                        .storage
                        .columns()
                        .delete_by_id(stmt, TableID::new(42), 1)
                        .await
                );
                assert!(
                    engine
                        .catalog()
                        .storage
                        .columns()
                        .delete_by_id(stmt, TableID::new(42), 0)
                        .await
                );
                assert!(
                    engine
                        .catalog()
                        .storage
                        .columns()
                        .delete_by_id(stmt, TableID::new(43), 0)
                        .await
                );
                Ok(())
            })
            .await
            .unwrap();
            mark_catalog_ddl(&mut trx, DDLRedo::DropTable(TableID::new(42)));
            trx.commit().await.unwrap();

            assert!(
                engine
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
            let mut session = engine.new_session().unwrap();

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

            let mut trx = session.begin_trx().unwrap();
            trx.exec(async |stmt| {
                for column in &columns {
                    assert!(
                        engine
                            .catalog()
                            .storage
                            .columns()
                            .insert(stmt, column)
                            .await
                    );
                }
                Ok(())
            })
            .await
            .unwrap();
            mark_catalog_ddl(&mut trx, DDLRedo::CreateTable(TableID::new(42)));
            trx.commit().await.unwrap();

            let mut trx = session.begin_trx().unwrap();
            trx.exec(async |stmt| {
                assert_eq!(
                    engine
                        .catalog()
                        .storage
                        .columns()
                        .delete_by_table_id(stmt, TableID::new(42))
                        .await
                        .unwrap(),
                    2
                );
                assert_eq!(
                    engine
                        .catalog()
                        .storage
                        .columns()
                        .delete_by_table_id(stmt, TableID::new(42))
                        .await
                        .unwrap(),
                    0
                );
                Ok(())
            })
            .await
            .unwrap();
            mark_catalog_ddl(&mut trx, DDLRedo::DropTable(TableID::new(42)));
            trx.commit().await.unwrap();

            assert!(
                engine
                    .catalog()
                    .storage
                    .columns()
                    .list_uncommitted_by_table_id(&session.pool_guards(), TableID::new(42))
                    .await
                    .unwrap()
                    .is_empty()
            );
            let remaining = engine
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
