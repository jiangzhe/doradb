use crate::buffer::PoolGuards;
use crate::catalog::CatalogTable;
use crate::catalog::storage::CatalogDefinition;
use crate::catalog::storage::object::TableObject;
use crate::catalog::table::{TableColumnLayout, TableMetadata};
use crate::catalog::{
    ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexSpec, catalog_table_id_from_slot,
};
use crate::error::{RuntimeError, RuntimeResult};
use crate::id::TableID;
use crate::row::ops::DeleteMvcc;
use crate::row::{Row, RowRead};
use crate::trx::stmt::Statement;
use crate::value::Val;
use crate::value::ValKind;
use error_stack::ResultExt;
use semistr::SemiStr;
use std::sync::OnceLock;

/// Catalog table id for `catalog.tables`.
pub(crate) const TABLE_ID_TABLES: TableID = catalog_table_id_from_slot(0);
const COL_NO_TABLES_TABLE_ID: usize = 0;
const COL_NAME_TABLES_TABLE_ID: &str = "table_id";
const COL_NO_TABLES_NEXT_INDEX_NO: usize = 1;
const COL_NAME_TABLES_NEXT_INDEX_NO: &str = "next_index_no";
const PK_NO_TABLES: usize = 0;

/// Runtime accessor for `catalog.tables`.
pub(crate) struct Tables<'a> {
    pub(super) table: &'a CatalogTable,
}

impl Tables<'_> {
    /// List all table rows from uncommitted-visible catalog state.
    pub(crate) async fn list_uncommitted(
        &self,
        guards: &PoolGuards,
    ) -> RuntimeResult<Vec<TableObject>> {
        let mut res = vec![];
        self.table
            .table_scan_uncommitted(guards, |col_layout, row| {
                if row.is_deleted() {
                    return true;
                }
                res.push(row_to_table_object(col_layout, row));
                true
            })
            .await
            .change_context(RuntimeError::CatalogAccess)
            .attach("operation=list_catalog_tables")?;
        Ok(res)
    }

    /// Find a table by id.
    #[inline]
    pub(crate) async fn find_uncommitted_by_id(
        &self,
        guards: &PoolGuards,
        table_id: TableID,
    ) -> RuntimeResult<Option<TableObject>> {
        let key_vals = [Val::from(table_id)];
        self.table
            .index_lookup_unique_uncommitted(guards, PK_NO_TABLES, &key_vals, row_to_table_object)
            .await
            .change_context(RuntimeError::CatalogAccess)
            .attach_with(|| format!("operation=find_catalog_table, table_id={table_id}"))
    }

    /// Insert a table row whose primary key is owned by the current DDL.
    ///
    /// Create-table uses an atomically allocated id. Create-index first deletes
    /// and then reinserts the same row in one metadata-gated transaction. The
    /// primary key is therefore unique by construction, and the statement
    /// boundary asserts if storage reports an Operation failure.
    pub(crate) async fn insert(
        &self,
        stmt: &mut Statement<'_>,
        obj: &TableObject,
    ) -> RuntimeResult<()> {
        let cols = vec![Val::from(obj.table_id), Val::from(obj.next_index_no)];
        stmt.catalog_insert_mvcc(self.table, cols)
            .await
            .map(|_| ())
            .attach_with(|| format!("operation=catalog_tables_insert, table_id={}", obj.table_id))
    }

    /// Delete a table by id.
    pub(crate) async fn delete_by_id(
        &self,
        stmt: &mut Statement<'_>,
        id: TableID,
    ) -> RuntimeResult<bool> {
        let key_vals = [Val::from(id)];
        let res = stmt
            .catalog_delete_primary_key_mvcc(self.table, PK_NO_TABLES, &key_vals, true)
            .await
            .attach_with(|| format!("operation=catalog_tables_delete, table_id={id}"))?;
        Ok(matches!(res, DeleteMvcc::Deleted))
    }
}

/// Return static table definition of `catalog.tables`.
pub(crate) fn catalog_definition_of_tables() -> &'static CatalogDefinition {
    static DEF: OnceLock<CatalogDefinition> = OnceLock::new();
    DEF.get_or_init(|| {
        CatalogDefinition {
            table_id: TABLE_ID_TABLES,
            metadata: TableMetadata::try_new(
                vec![
                    // table_id unsigned bigint primary key not null
                    ColumnSpec {
                        column_name: SemiStr::new(COL_NAME_TABLES_TABLE_ID),
                        column_type: ValKind::U64,
                        column_attributes: ColumnAttributes::INDEX,
                    },
                    // next_index_no unsigned smallint not null
                    ColumnSpec {
                        column_name: SemiStr::new(COL_NAME_TABLES_NEXT_INDEX_NO),
                        column_type: ValKind::U16,
                        column_attributes: ColumnAttributes::empty(),
                    },
                ],
                vec![
                    // primary key (table_id)
                    IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::PK),
                ],
            )
            .expect("valid table metadata"),
        }
    })
}

#[inline]
fn row_to_table_object(col_layout: &TableColumnLayout, row: Row<'_>) -> TableObject {
    let table_id = TableID::from(
        row.val(col_layout, COL_NO_TABLES_TABLE_ID)
            .as_u64()
            .unwrap(),
    );
    let next_index_no = row
        .val(col_layout, COL_NO_TABLES_NEXT_INDEX_NO)
        .as_u16()
        .unwrap();
    TableObject {
        table_id,
        next_index_no,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::{BufferPool, PoolGuards, PoolRole};
    use crate::catalog::storage::tests::mark_catalog_ddl;
    use crate::catalog::tests::{open_catalog_test_engine, table1};
    use crate::error::DiscloseResultExt;
    use crate::log::redo::DDLRedo;
    use crate::session::tests::SessionTestExt;
    use tempfile::TempDir;

    #[test]
    fn test_tables_delete_by_id() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = open_catalog_test_engine(main_dir, None).await;
            let mut session = engine.new_session().unwrap();

            let table100 = TableObject {
                table_id: TableID::new(100),
                next_index_no: 0,
            };
            let table101 = TableObject {
                table_id: TableID::new(101),
                next_index_no: 0,
            };
            let mut trx = session.begin_trx().unwrap();
            trx.exec(async |stmt| {
                engine
                    .catalog()
                    .storage
                    .tables()
                    .insert(stmt, &table100)
                    .await
                    .disclose()?;
                engine
                    .catalog()
                    .storage
                    .tables()
                    .insert(stmt, &table101)
                    .await
                    .disclose()?;
                mark_catalog_ddl(stmt, DDLRedo::CreateTable(table100.table_id));
                Ok(())
            })
            .await
            .unwrap();
            trx.commit().await.unwrap();

            let mut trx = session.begin_trx().unwrap();
            trx.exec(async |stmt| {
                assert!(
                    engine
                        .catalog()
                        .storage
                        .tables()
                        .delete_by_id(stmt, table100.table_id)
                        .await
                        .disclose()?
                );
                assert!(
                    !engine
                        .catalog()
                        .storage
                        .tables()
                        .delete_by_id(stmt, TableID::new(999))
                        .await
                        .disclose()?
                );
                mark_catalog_ddl(stmt, DDLRedo::DropTable(table100.table_id));
                Ok(())
            })
            .await
            .unwrap();
            trx.commit().await.unwrap();

            assert!(
                engine
                    .catalog()
                    .storage
                    .tables()
                    .find_uncommitted_by_id(&session.pool_guards(), table100.table_id)
                    .await
                    .unwrap()
                    .is_none()
            );
            assert!(
                engine
                    .catalog()
                    .storage
                    .tables()
                    .find_uncommitted_by_id(&session.pool_guards(), table101.table_id)
                    .await
                    .unwrap()
                    .is_some()
            );

            drop(session);
            drop(engine);
        });
    }

    #[test]
    fn test_catalog_lookup_uses_meta_guard_only() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = open_catalog_test_engine(main_dir, None).await;

            let table_id = table1(&engine).await;
            {
                let guards = PoolGuards::builder()
                    .push(PoolRole::Meta, engine.inner().meta_pool.pool_guard())
                    .build();
                assert!(
                    engine
                        .catalog()
                        .storage
                        .tables()
                        .find_uncommitted_by_id(&guards, table_id)
                        .await
                        .unwrap()
                        .is_some()
                );
            }

            drop(engine);
        });
    }
}
