use crate::buffer::PoolGuards;
use crate::catalog::CatalogTable;
use crate::catalog::storage::CatalogDefinition;
use crate::catalog::storage::object::ColumnObject;
use crate::catalog::table::TableMetadata;
use crate::catalog::{ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexSpec, TableID};
use crate::row::ops::{DeleteMvcc, InsertMvcc, SelectKey};
use crate::row::{Row, RowRead};
use crate::stmt::Statement;
use crate::table::TableAccess;
use crate::value::Val;
use crate::value::ValKind;
use semistr::SemiStr;
use std::sync::OnceLock;

pub const TABLE_ID_COLUMNS: TableID = 1;
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
const PK_NAME_COLUMNS: &str = "pk_columns";

/// Return static table definition of `catalog.columns`.
pub fn catalog_definition_of_columns() -> &'static CatalogDefinition {
    static DEF: OnceLock<CatalogDefinition> = OnceLock::new();
    DEF.get_or_init(|| {
        CatalogDefinition {
            table_id: TABLE_ID_COLUMNS,
            metadata: TableMetadata::new(
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
                        PK_NAME_COLUMNS,
                        vec![IndexKey::new(0), IndexKey::new(1)],
                        IndexAttributes::PK,
                    ),
                ],
            ),
        }
    })
}

#[inline]
fn row_to_column_object(metadata: &TableMetadata, row: Row<'_>) -> ColumnObject {
    let table_id = row.val(metadata, COL_NO_COLUMNS_TABLE_ID).as_u64().unwrap();
    let column_no = row
        .val(metadata, COL_NO_COLUMNS_COLUMN_NO)
        .as_u16()
        .unwrap();
    let column_name = row.str(COL_NO_COLUMNS_COLUMN_NAME).unwrap();
    let column_type = row
        .val(metadata, COL_NO_COLUMNS_COLUMN_TYPE)
        .as_u32()
        .unwrap();
    let column_attributes = row
        .val(metadata, COL_NO_COLUMNS_COLUMN_ATTRIBUTES)
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
pub struct Columns<'a> {
    pub(super) table: &'a CatalogTable,
}

impl Columns<'_> {
    /// Insert a column.
    pub async fn insert(&self, stmt: &mut Statement, obj: &ColumnObject) -> bool {
        let cols = vec![
            Val::from(obj.table_id),
            Val::from(obj.column_no),
            Val::from(obj.column_name.as_str()),
            Val::from(obj.column_type as u32),
            Val::from(obj.column_attributes.bits()),
        ];
        matches!(
            self.table.accessor().insert_mvcc(stmt, cols).await,
            Ok(InsertMvcc::Inserted(_))
        )
    }

    /// List all columns of one table from uncommitted-visible catalog rows.
    pub async fn list_uncommitted_by_table_id(
        &self,
        guards: &PoolGuards,
        table_id: TableID,
    ) -> Vec<ColumnObject> {
        let mut res = vec![];
        self.table
            .accessor()
            .table_scan_uncommitted(guards, |metadata, row| {
                if row.is_deleted() {
                    return true;
                }
                // filter by table id before deserializing the whole object.
                let table_id_in_row = row.val(metadata, COL_NO_COLUMNS_TABLE_ID).as_u64().unwrap();
                if table_id_in_row == table_id {
                    let obj = row_to_column_object(metadata, row);
                    res.push(obj);
                }
                true
            })
            .await;
        res
    }

    /// Delete a column by (table_id, column_no).
    pub async fn delete_by_id(
        &self,
        stmt: &mut Statement,
        table_id: TableID,
        column_no: u16,
    ) -> bool {
        let key = SelectKey::new(
            PK_NO_COLUMNS,
            vec![Val::from(table_id), Val::from(column_no)],
        );
        self.table
            .accessor()
            .delete_unique_mvcc(stmt, &key, true)
            .await
            .is_ok_and(|res| matches!(res, DeleteMvcc::Deleted))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conf::{EngineConfig, TrxSysConfig};
    use tempfile::TempDir;

    #[test]
    fn test_columns_delete_by_id() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = EngineConfig::default()
                .storage_root(main_dir)
                .trx(TrxSysConfig::default().skip_recovery(true))
                .build()
                .await
                .unwrap();
            let mut session = engine.try_new_session().unwrap();

            let col_42_0 = ColumnObject {
                table_id: 42,
                column_no: 0,
                column_name: SemiStr::new("c0"),
                column_type: ValKind::U32,
                column_attributes: ColumnAttributes::empty(),
            };
            let col_42_1 = ColumnObject {
                table_id: 42,
                column_no: 1,
                column_name: SemiStr::new("c1"),
                column_type: ValKind::U64,
                column_attributes: ColumnAttributes::empty(),
            };
            let col_43_0 = ColumnObject {
                table_id: 43,
                column_no: 0,
                column_name: SemiStr::new("c0"),
                column_type: ValKind::U16,
                column_attributes: ColumnAttributes::empty(),
            };

            let mut stmt = session.try_begin_trx().unwrap().unwrap().start_stmt();
            assert!(
                engine
                    .catalog()
                    .storage
                    .columns()
                    .insert(&mut stmt, &col_42_0)
                    .await
            );
            assert!(
                engine
                    .catalog()
                    .storage
                    .columns()
                    .insert(&mut stmt, &col_42_1)
                    .await
            );
            assert!(
                engine
                    .catalog()
                    .storage
                    .columns()
                    .insert(&mut stmt, &col_43_0)
                    .await
            );
            stmt.succeed().commit().await.unwrap();

            let mut stmt = session.try_begin_trx().unwrap().unwrap().start_stmt();
            assert!(
                engine
                    .catalog()
                    .storage
                    .columns()
                    .delete_by_id(&mut stmt, 42, 1)
                    .await
            );
            assert!(
                !engine
                    .catalog()
                    .storage
                    .columns()
                    .delete_by_id(&mut stmt, 42, 9)
                    .await
            );
            stmt.succeed().commit().await.unwrap();

            let cols_42 = engine
                .catalog()
                .storage
                .columns()
                .list_uncommitted_by_table_id(session.pool_guards(), 42)
                .await;
            assert_eq!(cols_42.len(), 1);
            assert_eq!(cols_42[0].column_no, 0);

            let cols_43 = engine
                .catalog()
                .storage
                .columns()
                .list_uncommitted_by_table_id(session.pool_guards(), 43)
                .await;
            assert_eq!(cols_43.len(), 1);
            assert_eq!(cols_43[0].column_no, 0);

            let mut stmt = session.try_begin_trx().unwrap().unwrap().start_stmt();
            assert!(
                !engine
                    .catalog()
                    .storage
                    .columns()
                    .delete_by_id(&mut stmt, 42, 1)
                    .await
            );
            assert!(
                engine
                    .catalog()
                    .storage
                    .columns()
                    .delete_by_id(&mut stmt, 42, 0)
                    .await
            );
            assert!(
                engine
                    .catalog()
                    .storage
                    .columns()
                    .delete_by_id(&mut stmt, 43, 0)
                    .await
            );
            stmt.succeed().commit().await.unwrap();

            assert!(
                engine
                    .catalog()
                    .storage
                    .columns()
                    .list_uncommitted_by_table_id(session.pool_guards(), 42)
                    .await
                    .is_empty()
            );
            assert!(
                engine
                    .catalog()
                    .storage
                    .columns()
                    .list_uncommitted_by_table_id(session.pool_guards(), 43)
                    .await
                    .is_empty()
            );

            drop(session);
            drop(engine);
        });
    }
}
