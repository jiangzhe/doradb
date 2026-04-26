use crate::buffer::PoolGuards;
use crate::catalog::CatalogTable;
use crate::catalog::storage::CatalogDefinition;
use crate::catalog::storage::object::TableObject;
use crate::catalog::table::TableMetadata;
use crate::catalog::{ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexSpec, TableID};
use crate::row::ops::{DeleteMvcc, SelectKey};
use crate::row::{Row, RowRead};
use crate::stmt::Statement;
use crate::table::TableAccess;
use crate::value::Val;
use crate::value::ValKind;
use semistr::SemiStr;
use std::sync::OnceLock;

pub const TABLE_ID_TABLES: TableID = 0;
const COL_NO_TABLES_TABLE_ID: usize = 0;
const COL_NAME_TABLES_TABLE_ID: &str = "table_id";
const PK_NO_TABLES: usize = 0;
const PK_NAME_TABLES: &str = "pk_tables";

/// Return static table definition of `catalog.tables`.
pub fn catalog_definition_of_tables() -> &'static CatalogDefinition {
    static DEF: OnceLock<CatalogDefinition> = OnceLock::new();
    DEF.get_or_init(|| {
        CatalogDefinition {
            table_id: TABLE_ID_TABLES,
            metadata: TableMetadata::new(
                vec![
                    // table_id unsigned bigint primary key not null
                    ColumnSpec {
                        column_name: SemiStr::new(COL_NAME_TABLES_TABLE_ID),
                        column_type: ValKind::U64,
                        column_attributes: ColumnAttributes::INDEX,
                    },
                ],
                vec![
                    // primary key pk_tables (table_id)
                    IndexSpec::new(PK_NAME_TABLES, vec![IndexKey::new(0)], IndexAttributes::PK),
                ],
            ),
        }
    })
}

#[inline]
fn row_to_table_object(metadata: &TableMetadata, row: Row<'_>) -> TableObject {
    let table_id = row.val(metadata, COL_NO_TABLES_TABLE_ID).as_u64().unwrap();
    TableObject { table_id }
}

/// Runtime accessor for `catalog.tables`.
pub struct Tables<'a> {
    pub(super) table: &'a CatalogTable,
}

impl Tables<'_> {
    /// List all table rows from uncommitted-visible catalog state.
    pub async fn list_uncommitted(&self, guards: &PoolGuards) -> Vec<TableObject> {
        let mut res = vec![];
        self.table
            .accessor()
            .table_scan_uncommitted(guards, |metadata, row| {
                if row.is_deleted() {
                    return true;
                }
                res.push(row_to_table_object(metadata, row));
                true
            })
            .await;
        res
    }

    /// Find a table by id.
    #[inline]
    pub async fn find_uncommitted_by_id(
        &self,
        guards: &PoolGuards,
        table_id: TableID,
    ) -> crate::error::Result<Option<TableObject>> {
        let key = SelectKey::new(PK_NO_TABLES, vec![Val::from(table_id)]);
        self.table
            .accessor()
            .index_lookup_unique_uncommitted(guards, &key, row_to_table_object)
            .await
    }

    /// Insert a table.
    pub async fn insert(&self, stmt: &mut Statement, obj: &TableObject) -> bool {
        let cols = vec![Val::from(obj.table_id)];
        let (ctx, effects) = stmt.ctx_and_effects_mut();
        self.table
            .accessor()
            .insert_mvcc(ctx, effects, cols)
            .await
            .is_ok()
    }

    /// Delete a table by id.
    pub async fn delete_by_id(&self, stmt: &mut Statement, id: TableID) -> bool {
        let key = SelectKey::new(PK_NO_TABLES, vec![Val::from(id)]);
        let (ctx, effects) = stmt.ctx_and_effects_mut();
        self.table
            .accessor()
            .delete_unique_mvcc(ctx, effects, &key, true)
            .await
            .is_ok_and(|res| matches!(res, DeleteMvcc::Deleted))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::{BufferPool, PoolGuards, PoolRole};
    use crate::catalog::tests::table1;
    use crate::conf::{EngineConfig, TrxSysConfig};
    use tempfile::TempDir;

    #[test]
    fn test_tables_delete_by_id() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = EngineConfig::default()
                .storage_root(main_dir)
                .trx(TrxSysConfig::default())
                .build()
                .await
                .unwrap();
            let mut session = engine.try_new_session().unwrap();

            let table100 = TableObject { table_id: 100 };
            let table101 = TableObject { table_id: 101 };
            let mut stmt = session.try_begin_trx().unwrap().unwrap().start_stmt();
            assert!(
                engine
                    .catalog()
                    .storage
                    .tables()
                    .insert(&mut stmt, &table100)
                    .await
            );
            assert!(
                engine
                    .catalog()
                    .storage
                    .tables()
                    .insert(&mut stmt, &table101)
                    .await
            );
            stmt.succeed().commit().await.unwrap();

            let mut stmt = session.try_begin_trx().unwrap().unwrap().start_stmt();
            assert!(
                engine
                    .catalog()
                    .storage
                    .tables()
                    .delete_by_id(&mut stmt, table100.table_id)
                    .await
            );
            assert!(
                !engine
                    .catalog()
                    .storage
                    .tables()
                    .delete_by_id(&mut stmt, 999)
                    .await
            );
            stmt.succeed().commit().await.unwrap();

            assert!(
                engine
                    .catalog()
                    .storage
                    .tables()
                    .find_uncommitted_by_id(session.pool_guards(), table100.table_id)
                    .await
                    .unwrap()
                    .is_none()
            );
            assert!(
                engine
                    .catalog()
                    .storage
                    .tables()
                    .find_uncommitted_by_id(session.pool_guards(), table101.table_id)
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
            let engine = EngineConfig::default()
                .storage_root(main_dir)
                .trx(TrxSysConfig::default())
                .build()
                .await
                .unwrap();

            let table_id = table1(&engine).await;
            {
                let guards = PoolGuards::builder()
                    .push(PoolRole::Meta, engine.meta_pool.pool_guard())
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
