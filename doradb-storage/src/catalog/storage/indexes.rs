use crate::buffer::PoolGuards;
use crate::catalog::CatalogTable;
use crate::catalog::storage::CatalogDefinition;
use crate::catalog::storage::object::{IndexColumnObject, IndexObject};
use crate::catalog::table::TableMetadata;
use crate::catalog::{
    ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexOrder, IndexSpec, TableID,
};
use crate::row::ops::{DeleteMvcc, SelectKey};
use crate::row::{Row, RowRead};
use crate::table::TableAccess;
use crate::trx::stmt::Statement;
use crate::value::Val;
use crate::value::ValKind;
use semistr::SemiStr;
use std::sync::OnceLock;

/* Indexes table */

pub const TABLE_ID_INDEXES: TableID = 2;
const COL_NO_INDEXES_TABLE_ID: usize = 0;
const COL_NAME_INDEXES_TABLE_ID: &str = "table_id";
const COL_NO_INDEXES_INDEX_NO: usize = 1;
const COL_NAME_INDEXES_INDEX_NO: &str = "index_no";
const COL_NO_INDEXES_INDEX_ATTRIBUTES: usize = 2;
const COL_NAME_INDEXES_INDEX_ATTRIBUTES: &str = "index_attributes";
const PK_NO_INDEXES: usize = 0;

/// Return static table definition of `catalog.indexes`.
pub fn catalog_definition_of_indexes() -> &'static CatalogDefinition {
    static DEF: OnceLock<CatalogDefinition> = OnceLock::new();
    DEF.get_or_init(|| {
        CatalogDefinition {
            table_id: TABLE_ID_INDEXES,
            metadata: TableMetadata::new(
                vec![
                    // table_id unsgined bigint not null
                    ColumnSpec {
                        column_name: SemiStr::new(COL_NAME_INDEXES_TABLE_ID),
                        column_type: ValKind::U64,
                        column_attributes: ColumnAttributes::INDEX,
                    },
                    // index_no unsigned smallint not null
                    ColumnSpec {
                        column_name: SemiStr::new(COL_NAME_INDEXES_INDEX_NO),
                        column_type: ValKind::U16,
                        column_attributes: ColumnAttributes::INDEX,
                    },
                    // index_attributes unsigned int not null
                    ColumnSpec {
                        column_name: SemiStr::new(COL_NAME_INDEXES_INDEX_ATTRIBUTES),
                        column_type: ValKind::U32,
                        column_attributes: ColumnAttributes::empty(),
                    },
                ],
                vec![
                    // primary key (table_id, index_no)
                    IndexSpec::new(
                        vec![IndexKey::new(0), IndexKey::new(1)],
                        IndexAttributes::PK,
                    ),
                ],
            ),
        }
    })
}

#[inline]
fn row_to_index_object(metadata: &TableMetadata, row: Row<'_>) -> IndexObject {
    let table_id = row.val(metadata, COL_NO_INDEXES_TABLE_ID).as_u64().unwrap();
    let index_no = row.val(metadata, COL_NO_INDEXES_INDEX_NO).as_u16().unwrap();
    let index_attributes = row
        .val(metadata, COL_NO_INDEXES_INDEX_ATTRIBUTES)
        .as_u32()
        .unwrap();
    IndexObject {
        table_id,
        index_no,
        index_attributes: IndexAttributes::from_bits_truncate(index_attributes),
    }
}

/// Runtime accessor for `catalog.indexes`.
pub struct Indexes<'a> {
    pub(super) table: &'a CatalogTable,
}

impl Indexes<'_> {
    /// Insert an index.
    pub async fn insert(&self, stmt: &mut Statement<'_>, obj: &IndexObject) -> bool {
        let cols = vec![
            Val::from(obj.table_id),
            Val::from(obj.index_no),
            Val::from(obj.index_attributes.bits()),
        ];
        stmt.catalog_insert_mvcc(self.table, cols).await.is_ok()
    }

    /// Delete an index by (table_id, index_no).
    pub async fn delete_by_id(
        &self,
        stmt: &mut Statement<'_>,
        table_id: TableID,
        index_no: u16,
    ) -> bool {
        let key = SelectKey::new(
            PK_NO_INDEXES,
            vec![Val::from(table_id), Val::from(index_no)],
        );
        stmt.catalog_delete_unique_mvcc(self.table, &key, true)
            .await
            .is_ok_and(|res| matches!(res, DeleteMvcc::Deleted))
    }

    /// Delete all indexes for one table and return the number of deleted rows.
    pub async fn delete_by_table_id(&self, stmt: &mut Statement<'_>, table_id: TableID) -> usize {
        let Some(guards) = stmt.ctx().pool_guards().cloned() else {
            return 0;
        };
        let indexes = self.list_uncommitted_by_table_id(&guards, table_id).await;
        let mut deleted = 0;
        for index in indexes {
            if self.delete_by_id(stmt, table_id, index.index_no).await {
                deleted += 1;
            }
        }
        deleted
    }

    /// List all indexes by given table id.
    pub async fn list_uncommitted_by_table_id(
        &self,
        guards: &PoolGuards,
        table_id: TableID,
    ) -> Vec<IndexObject> {
        let mut res = vec![];
        self.table
            .accessor()
            .table_scan_uncommitted(guards, |metadata, row| {
                if row.is_deleted() {
                    return true;
                }
                // filter by table id before deserializing the whole object.
                let table_id_in_row = row.val(metadata, COL_NO_INDEXES_TABLE_ID).as_u64().unwrap();
                if table_id_in_row == table_id {
                    let obj = row_to_index_object(metadata, row);
                    res.push(obj);
                }
                true
            })
            .await;
        res
    }
}

/* Index columns table */

pub const TABLE_ID_INDEX_COLUMNS: TableID = 3;
const COL_NO_INDEX_COLUMNS_TABLE_ID: usize = 0;
const COL_NAME_INDEX_COLUMNS_TABLE_ID: &str = "table_id";
const COL_NO_INDEX_COLUMNS_INDEX_NO: usize = 1;
const COL_NAME_INDEX_COLUMNS_INDEX_NO: &str = "index_no";
const COL_NO_INDEX_COLUMNS_INDEX_COLUMN_NO: usize = 2;
const COL_NAME_INDEX_COLUMNS_INDEX_COLUMN_NO: &str = "index_column_no";
const COL_NO_INDEX_COLUMNS_COLUMN_NO: usize = 3;
const COL_NAME_INDEX_COLUMNS_COLUMN_NO: &str = "column_no";

const COL_NO_INDEX_COLUMNS_INDEX_ORDER: usize = 4;
const COL_NAME_INDEX_COLUMNS_INDEX_ORDER: &str = "index_order";
const PK_NO_INDEX_COLUMNS: usize = 0;

/// Return static table definition of `catalog.index_columns`.
pub fn catalog_definition_of_index_columns() -> &'static CatalogDefinition {
    static DEF: OnceLock<CatalogDefinition> = OnceLock::new();
    DEF.get_or_init(|| {
        CatalogDefinition {
            table_id: TABLE_ID_INDEX_COLUMNS,
            metadata: TableMetadata::new(
                vec![
                    // table_id unsigned bigint not null
                    ColumnSpec {
                        column_name: SemiStr::new(COL_NAME_INDEX_COLUMNS_TABLE_ID),
                        column_type: ValKind::U64,
                        column_attributes: ColumnAttributes::INDEX,
                    },
                    // index_no unsigned smallint not null
                    ColumnSpec {
                        column_name: SemiStr::new(COL_NAME_INDEX_COLUMNS_INDEX_NO),
                        column_type: ValKind::U16,
                        column_attributes: ColumnAttributes::INDEX,
                    },
                    // index_column_no unsigned smallint not null
                    ColumnSpec {
                        column_name: SemiStr::new(COL_NAME_INDEX_COLUMNS_INDEX_COLUMN_NO),
                        column_type: ValKind::U16,
                        column_attributes: ColumnAttributes::INDEX,
                    },
                    // column_no unsigned smallint not null
                    ColumnSpec {
                        column_name: SemiStr::new(COL_NAME_INDEX_COLUMNS_COLUMN_NO),
                        column_type: ValKind::U16,
                        column_attributes: ColumnAttributes::empty(),
                    },
                    // descending boolean not null
                    ColumnSpec {
                        column_name: SemiStr::new(COL_NAME_INDEX_COLUMNS_INDEX_ORDER),
                        column_type: ValKind::U8,
                        column_attributes: ColumnAttributes::empty(),
                    },
                ],
                vec![
                    // primary key pk_index_columns
                    // (table_id, index_no, index_column_no)
                    IndexSpec::new(
                        vec![IndexKey::new(0), IndexKey::new(1), IndexKey::new(2)],
                        IndexAttributes::PK,
                    ),
                ],
            ),
        }
    })
}

#[inline]
fn row_to_index_column_object(metadata: &TableMetadata, row: Row<'_>) -> IndexColumnObject {
    let table_id = row
        .val(metadata, COL_NO_INDEX_COLUMNS_TABLE_ID)
        .as_u64()
        .unwrap();
    let index_no = row
        .val(metadata, COL_NO_INDEX_COLUMNS_INDEX_NO)
        .as_u16()
        .unwrap();
    let index_column_no = row
        .val(metadata, COL_NO_INDEX_COLUMNS_INDEX_COLUMN_NO)
        .as_u16()
        .unwrap();
    let column_no = row
        .val(metadata, COL_NO_INDEX_COLUMNS_COLUMN_NO)
        .as_u16()
        .unwrap();
    let index_order = row
        .val(metadata, COL_NO_INDEX_COLUMNS_INDEX_ORDER)
        .as_u8()
        .unwrap();
    IndexColumnObject {
        table_id,
        index_no,
        index_column_no,
        column_no,
        index_order: IndexOrder::from(index_order),
    }
}

/// Runtime accessor for `catalog.index_columns`.
pub struct IndexColumns<'a> {
    pub(super) table: &'a CatalogTable,
}

impl IndexColumns<'_> {
    /// Insert one index-column mapping row.
    pub async fn insert(&self, stmt: &mut Statement<'_>, obj: &IndexColumnObject) -> bool {
        let cols = vec![
            Val::from(obj.table_id),
            Val::from(obj.index_no),
            Val::from(obj.index_column_no),
            Val::from(obj.column_no),
            Val::from(obj.index_order as u8),
        ];
        stmt.catalog_insert_mvcc(self.table, cols).await.is_ok()
    }

    async fn delete_by_id(
        &self,
        stmt: &mut Statement<'_>,
        table_id: TableID,
        index_no: u16,
        index_column_no: u16,
    ) -> bool {
        let key = SelectKey::new(
            PK_NO_INDEX_COLUMNS,
            vec![
                Val::from(table_id),
                Val::from(index_no),
                Val::from(index_column_no),
            ],
        );
        stmt.catalog_delete_unique_mvcc(self.table, &key, true)
            .await
            .is_ok_and(|res| matches!(res, DeleteMvcc::Deleted))
    }

    /// Delete all index-column rows by `(table_id, index_no)`.
    pub async fn delete_by_index(
        &self,
        stmt: &mut Statement<'_>,
        table_id: TableID,
        index_no: u16,
    ) -> usize {
        let Some(guards) = stmt.ctx().pool_guards().cloned() else {
            return 0;
        };
        let index_columns = self.list_uncommitted_by_table_id(&guards, table_id).await;
        let mut deleted = 0;
        for index_column in index_columns
            .into_iter()
            .filter(|index_column| index_column.index_no == index_no)
        {
            if self
                .delete_by_id(stmt, table_id, index_no, index_column.index_column_no)
                .await
            {
                deleted += 1;
            }
        }
        deleted
    }

    /// Delete all index-column rows for one table and return the number of deleted rows.
    pub async fn delete_by_table_id(&self, stmt: &mut Statement<'_>, table_id: TableID) -> usize {
        let Some(guards) = stmt.ctx().pool_guards().cloned() else {
            return 0;
        };
        let index_columns = self.list_uncommitted_by_table_id(&guards, table_id).await;
        let mut deleted = 0;
        for index_column in index_columns {
            if self
                .delete_by_id(
                    stmt,
                    table_id,
                    index_column.index_no,
                    index_column.index_column_no,
                )
                .await
            {
                deleted += 1;
            }
        }
        deleted
    }

    /// List all index-column rows of one table from uncommitted-visible rows.
    pub async fn list_uncommitted_by_table_id(
        &self,
        guards: &PoolGuards,
        table_id: TableID,
    ) -> Vec<IndexColumnObject> {
        let mut res = vec![];
        self.table
            .accessor()
            .table_scan_uncommitted(guards, |metadata, row| {
                if row.is_deleted() {
                    return true;
                }
                let table_id_in_row = row
                    .val(metadata, COL_NO_INDEX_COLUMNS_TABLE_ID)
                    .as_u64()
                    .unwrap();
                if table_id_in_row == table_id {
                    let obj = row_to_index_column_object(metadata, row);
                    res.push(obj);
                }
                true
            })
            .await;
        res
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conf::{EngineConfig, TrxSysConfig};
    use crate::trx::ActiveTrx;
    use crate::trx::redo::DDLRedo;
    use tempfile::TempDir;

    fn mark_catalog_ddl(trx: &mut ActiveTrx, ddl: DDLRedo) {
        let old = trx.effects_mut().redo_mut().ddl.replace(Box::new(ddl));
        debug_assert!(old.is_none());
    }

    #[test]
    fn test_indexes_delete_by_id() {
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

            let idx_42_0 = IndexObject {
                table_id: 42,
                index_no: 0,
                index_attributes: IndexAttributes::PK,
            };
            let idx_42_1 = IndexObject {
                table_id: 42,
                index_no: 1,
                index_attributes: IndexAttributes::empty(),
            };
            let idx_43_0 = IndexObject {
                table_id: 43,
                index_no: 0,
                index_attributes: IndexAttributes::PK,
            };

            let mut trx = session.try_begin_trx().unwrap().unwrap();
            trx.exec(async |stmt| {
                assert!(
                    engine
                        .catalog()
                        .storage
                        .indexes()
                        .insert(stmt, &idx_42_0)
                        .await
                );
                assert!(
                    engine
                        .catalog()
                        .storage
                        .indexes()
                        .insert(stmt, &idx_42_1)
                        .await
                );
                assert!(
                    engine
                        .catalog()
                        .storage
                        .indexes()
                        .insert(stmt, &idx_43_0)
                        .await
                );
                Ok(())
            })
            .await
            .unwrap();
            mark_catalog_ddl(&mut trx, DDLRedo::CreateTable(42));
            trx.commit().await.unwrap();

            let mut trx = session.try_begin_trx().unwrap().unwrap();
            trx.exec(async |stmt| {
                assert!(
                    engine
                        .catalog()
                        .storage
                        .indexes()
                        .delete_by_id(stmt, 42, 1)
                        .await
                );
                assert!(
                    !engine
                        .catalog()
                        .storage
                        .indexes()
                        .delete_by_id(stmt, 42, 9)
                        .await
                );
                Ok(())
            })
            .await
            .unwrap();
            mark_catalog_ddl(&mut trx, DDLRedo::DropTable(42));
            trx.commit().await.unwrap();

            let idx_42 = engine
                .catalog()
                .storage
                .indexes()
                .list_uncommitted_by_table_id(session.pool_guards(), 42)
                .await;
            assert_eq!(idx_42.len(), 1);
            assert_eq!(idx_42[0].index_no, 0);

            let idx_43 = engine
                .catalog()
                .storage
                .indexes()
                .list_uncommitted_by_table_id(session.pool_guards(), 43)
                .await;
            assert_eq!(idx_43.len(), 1);
            assert_eq!(idx_43[0].index_no, 0);

            let mut trx = session.try_begin_trx().unwrap().unwrap();
            trx.exec(async |stmt| {
                assert!(
                    !engine
                        .catalog()
                        .storage
                        .indexes()
                        .delete_by_id(stmt, 42, 1)
                        .await
                );
                assert!(
                    engine
                        .catalog()
                        .storage
                        .indexes()
                        .delete_by_id(stmt, 42, 0)
                        .await
                );
                assert!(
                    engine
                        .catalog()
                        .storage
                        .indexes()
                        .delete_by_id(stmt, 43, 0)
                        .await
                );
                Ok(())
            })
            .await
            .unwrap();
            mark_catalog_ddl(&mut trx, DDLRedo::DropTable(42));
            trx.commit().await.unwrap();

            assert!(
                engine
                    .catalog()
                    .storage
                    .indexes()
                    .list_uncommitted_by_table_id(session.pool_guards(), 42)
                    .await
                    .is_empty()
            );
            assert!(
                engine
                    .catalog()
                    .storage
                    .indexes()
                    .list_uncommitted_by_table_id(session.pool_guards(), 43)
                    .await
                    .is_empty()
            );

            drop(session);
            drop(engine);
        });
    }

    #[test]
    fn test_indexes_delete_by_table_id_counts_and_is_idempotent() {
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

            let indexes = [
                IndexObject {
                    table_id: 42,
                    index_no: 0,
                    index_attributes: IndexAttributes::PK,
                },
                IndexObject {
                    table_id: 42,
                    index_no: 1,
                    index_attributes: IndexAttributes::empty(),
                },
                IndexObject {
                    table_id: 43,
                    index_no: 0,
                    index_attributes: IndexAttributes::PK,
                },
            ];

            let mut trx = session.try_begin_trx().unwrap().unwrap();
            trx.exec(async |stmt| {
                for index in &indexes {
                    assert!(engine.catalog().storage.indexes().insert(stmt, index).await);
                }
                Ok(())
            })
            .await
            .unwrap();
            mark_catalog_ddl(&mut trx, DDLRedo::CreateTable(42));
            trx.commit().await.unwrap();

            let mut trx = session.try_begin_trx().unwrap().unwrap();
            trx.exec(async |stmt| {
                assert_eq!(
                    engine
                        .catalog()
                        .storage
                        .indexes()
                        .delete_by_table_id(stmt, 42)
                        .await,
                    2
                );
                assert_eq!(
                    engine
                        .catalog()
                        .storage
                        .indexes()
                        .delete_by_table_id(stmt, 42)
                        .await,
                    0
                );
                Ok(())
            })
            .await
            .unwrap();
            mark_catalog_ddl(&mut trx, DDLRedo::DropTable(42));
            trx.commit().await.unwrap();

            assert!(
                engine
                    .catalog()
                    .storage
                    .indexes()
                    .list_uncommitted_by_table_id(session.pool_guards(), 42)
                    .await
                    .is_empty()
            );
            let remaining = engine
                .catalog()
                .storage
                .indexes()
                .list_uncommitted_by_table_id(session.pool_guards(), 43)
                .await;
            assert_eq!(remaining.len(), 1);
            assert_eq!(remaining[0].index_no, 0);

            drop(session);
            drop(engine);
        });
    }

    #[test]
    fn test_index_columns_delete_by_index_and_table_id() {
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

            let index_columns = [
                IndexColumnObject {
                    table_id: 42,
                    index_no: 0,
                    index_column_no: 0,
                    column_no: 0,
                    index_order: IndexOrder::Asc,
                },
                IndexColumnObject {
                    table_id: 42,
                    index_no: 1,
                    index_column_no: 0,
                    column_no: 1,
                    index_order: IndexOrder::Asc,
                },
                IndexColumnObject {
                    table_id: 42,
                    index_no: 1,
                    index_column_no: 1,
                    column_no: 2,
                    index_order: IndexOrder::Desc,
                },
                IndexColumnObject {
                    table_id: 43,
                    index_no: 1,
                    index_column_no: 0,
                    column_no: 0,
                    index_order: IndexOrder::Asc,
                },
            ];

            let mut trx = session.try_begin_trx().unwrap().unwrap();
            trx.exec(async |stmt| {
                for index_column in &index_columns {
                    assert!(
                        engine
                            .catalog()
                            .storage
                            .index_columns()
                            .insert(stmt, index_column)
                            .await
                    );
                }
                Ok(())
            })
            .await
            .unwrap();
            mark_catalog_ddl(&mut trx, DDLRedo::CreateTable(42));
            trx.commit().await.unwrap();

            let mut trx = session.try_begin_trx().unwrap().unwrap();
            trx.exec(async |stmt| {
                assert_eq!(
                    engine
                        .catalog()
                        .storage
                        .index_columns()
                        .delete_by_index(stmt, 42, 1)
                        .await,
                    2
                );
                assert_eq!(
                    engine
                        .catalog()
                        .storage
                        .index_columns()
                        .delete_by_index(stmt, 42, 1)
                        .await,
                    0
                );
                Ok(())
            })
            .await
            .unwrap();
            mark_catalog_ddl(&mut trx, DDLRedo::DropTable(42));
            trx.commit().await.unwrap();

            let remaining_42 = engine
                .catalog()
                .storage
                .index_columns()
                .list_uncommitted_by_table_id(session.pool_guards(), 42)
                .await;
            assert_eq!(remaining_42.len(), 1);
            assert_eq!(remaining_42[0].index_no, 0);

            let mut trx = session.try_begin_trx().unwrap().unwrap();
            trx.exec(async |stmt| {
                assert_eq!(
                    engine
                        .catalog()
                        .storage
                        .index_columns()
                        .delete_by_table_id(stmt, 42)
                        .await,
                    1
                );
                assert_eq!(
                    engine
                        .catalog()
                        .storage
                        .index_columns()
                        .delete_by_table_id(stmt, 42)
                        .await,
                    0
                );
                Ok(())
            })
            .await
            .unwrap();
            mark_catalog_ddl(&mut trx, DDLRedo::DropTable(42));
            trx.commit().await.unwrap();

            assert!(
                engine
                    .catalog()
                    .storage
                    .index_columns()
                    .list_uncommitted_by_table_id(session.pool_guards(), 42)
                    .await
                    .is_empty()
            );
            let remaining_43 = engine
                .catalog()
                .storage
                .index_columns()
                .list_uncommitted_by_table_id(session.pool_guards(), 43)
                .await;
            assert_eq!(remaining_43.len(), 1);
            assert_eq!(remaining_43[0].table_id, 43);
            assert_eq!(remaining_43[0].index_no, 1);

            drop(session);
            drop(engine);
        });
    }
}
