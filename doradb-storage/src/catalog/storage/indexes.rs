use crate::buffer::PoolGuards;
use crate::catalog::storage::CatalogDefinition;
use crate::catalog::storage::object::{IndexColumnObject, IndexObject};
use crate::catalog::table::{TableColumnLayout, TableMetadata};
use crate::catalog::{CatalogIndexNo, CatalogTable};
use crate::catalog::{
    ColumnAttributes, ColumnSpec, IndexAttributes, IndexKeySpec, IndexOrder, IndexSlot, IndexSpec,
    catalog_table_id_from_slot,
};
use crate::error::{MultiDomainResultExt, RuntimeError, RuntimeOrFatalResult, RuntimeResult};
use crate::id::TableID;
use crate::row::ops::DeleteMvcc;
use crate::row::{Row, RowRead};
use crate::trx::PrivateTransaction;
use crate::value::Val;
use crate::value::ValKind;
use error_stack::ResultExt;
use semistr::SemiStr;
use std::sync::OnceLock;

/* Indexes table */

pub(super) const TABLE_ID_INDEXES: TableID = catalog_table_id_from_slot(2);
const COL_NO_INDEXES_TABLE_ID: usize = 0;
const COL_NAME_INDEXES_TABLE_ID: &str = "table_id";
const COL_NO_INDEXES_INDEX_NO: usize = 1;
const COL_NAME_INDEXES_INDEX_NO: &str = "index_no";
const COL_NO_INDEXES_INDEX_ATTRIBUTES: usize = 2;
const COL_NAME_INDEXES_INDEX_ATTRIBUTES: &str = "index_attributes";
const PK_NO_INDEXES: CatalogIndexNo = CatalogIndexNo::new(0);

/* Index columns table */

pub(super) const TABLE_ID_INDEX_COLUMNS: TableID = catalog_table_id_from_slot(3);
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
const PK_NO_INDEX_COLUMNS: CatalogIndexNo = CatalogIndexNo::new(0);

/// Runtime accessor for `catalog.indexes`.
pub(crate) struct Indexes<'a> {
    pub(super) table: &'a CatalogTable,
}

impl Indexes<'_> {
    /// Insert an index with a table-local allocated index slot.
    ///
    /// `(table_id, index_slot)` is unique by construction. Operation failures are
    /// asserted at the statement boundary.
    pub(crate) async fn insert(
        &self,
        trx: &mut PrivateTransaction,
        obj: &IndexObject,
    ) -> RuntimeOrFatalResult<()> {
        trx.catalog_insert_mvcc(self.table, cols_from_index_object(obj))
            .await
            .map(|_| ())
            .attach_with(|| {
                format!(
                    "operation=catalog_indexes_insert, table_id={}, index_slot={}",
                    obj.table_id, obj.index_slot
                )
            })
    }

    /// Insert an ordered index batch through one private statement.
    pub(crate) async fn insert_batch(
        &self,
        trx: &mut PrivateTransaction,
        objects: &[IndexObject],
    ) -> RuntimeOrFatalResult<()> {
        let rows = objects.iter().map(cols_from_index_object).collect();
        trx.catalog_insert_batch_mvcc(self.table, rows)
            .await
            .attach("operation=catalog_indexes_insert_batch")
    }

    /// Delete an index by `(table_id, index_slot)`.
    pub(crate) async fn delete_by_id(
        &self,
        trx: &mut PrivateTransaction,
        table_id: TableID,
        index_slot: IndexSlot,
    ) -> RuntimeOrFatalResult<bool> {
        let key_vals = vec![Val::from(table_id), Val::from(index_slot.get())];
        let res = trx
            .catalog_delete_primary_key_mvcc(self.table, PK_NO_INDEXES, key_vals)
            .await
            .attach_with(|| {
                format!(
                    "operation=catalog_indexes_delete, table_id={table_id}, index_slot={index_slot}"
                )
            })?;
        Ok(matches!(res, DeleteMvcc::Deleted))
    }

    /// Delete all indexes for one table and return the number of deleted rows.
    pub(crate) async fn delete_by_table_id(
        &self,
        trx: &mut PrivateTransaction,
        table_id: TableID,
    ) -> RuntimeOrFatalResult<usize> {
        let indexes = self
            .list_uncommitted_by_table_id(trx.pool_guards(), table_id)
            .await?;
        let keys = indexes
            .into_iter()
            .map(|index| vec![Val::from(table_id), Val::from(index.index_slot.get())])
            .collect();
        trx.catalog_delete_primary_key_batch_mvcc(self.table, PK_NO_INDEXES, keys)
            .await
            .attach_with(|| {
                format!("operation=catalog_indexes_delete_by_table, table_id={table_id}")
            })
    }

    /// List all indexes by given table id.
    pub(crate) async fn list_uncommitted_by_table_id(
        &self,
        guards: &PoolGuards,
        table_id: TableID,
    ) -> RuntimeResult<Vec<IndexObject>> {
        let mut res = vec![];
        self.table
            .table_scan_uncommitted(guards, |col_layout, row| {
                if row.is_deleted() {
                    return true;
                }
                // filter by table id before deserializing the whole object.
                let table_id_in_row = row
                    .val(col_layout, COL_NO_INDEXES_TABLE_ID)
                    .as_u64()
                    .unwrap();
                if table_id_in_row == table_id.as_u64() {
                    let obj = row_to_index_object(col_layout, row);
                    res.push(obj);
                }
                true
            })
            .await
            .change_context(RuntimeError::CatalogAccess)
            .attach_with(|| format!("operation=list_catalog_indexes, table_id={table_id}"))?;
        Ok(res)
    }
}

/// Runtime accessor for `catalog.index_columns`.
pub(crate) struct IndexColumns<'a> {
    pub(super) table: &'a CatalogTable,
}

impl IndexColumns<'_> {
    /// Insert one index-column mapping row derived from index-key order.
    ///
    /// The enumerated `index_column_no` makes the composite primary key unique
    /// by construction. Operation failures are invariant violations.
    #[cfg(test)]
    pub(crate) async fn insert(
        &self,
        trx: &mut PrivateTransaction,
        obj: &IndexColumnObject,
    ) -> RuntimeOrFatalResult<()> {
        trx.catalog_insert_mvcc(self.table, cols_from_index_column_object(obj))
            .await
            .map(|_| ())
            .attach_with(|| {
                format!(
                    "operation=catalog_index_columns_insert, table_id={}, index_slot={}, index_column_no={}",
                    obj.table_id, obj.index_slot, obj.index_column_no
                )
            })
    }

    /// Insert an ordered index-column batch through one private statement.
    pub(crate) async fn insert_batch(
        &self,
        trx: &mut PrivateTransaction,
        objects: &[IndexColumnObject],
    ) -> RuntimeOrFatalResult<()> {
        let rows = objects.iter().map(cols_from_index_column_object).collect();
        trx.catalog_insert_batch_mvcc(self.table, rows)
            .await
            .attach("operation=catalog_index_columns_insert_batch")
    }

    /// Delete all index-column rows by `(table_id, index_slot)`.
    pub(crate) async fn delete_by_index(
        &self,
        trx: &mut PrivateTransaction,
        table_id: TableID,
        index_slot: IndexSlot,
    ) -> RuntimeOrFatalResult<usize> {
        let index_columns = self
            .list_uncommitted_by_table_id(trx.pool_guards(), table_id)
            .await?;
        let keys = index_columns
            .into_iter()
            .filter(|index_column| index_column.index_slot == index_slot)
            .map(|index_column| {
                vec![
                    Val::from(table_id),
                    Val::from(index_slot.get()),
                    Val::from(index_column.index_column_no),
                ]
            })
            .collect();
        trx.catalog_delete_primary_key_batch_mvcc(self.table, PK_NO_INDEX_COLUMNS, keys)
            .await
            .attach_with(|| {
                format!(
                    "operation=catalog_index_columns_delete_by_index, table_id={table_id}, index_slot={index_slot}"
                )
            })
    }

    /// Delete all index-column rows for one table and return the number of deleted rows.
    pub(crate) async fn delete_by_table_id(
        &self,
        trx: &mut PrivateTransaction,
        table_id: TableID,
    ) -> RuntimeOrFatalResult<usize> {
        let index_columns = self
            .list_uncommitted_by_table_id(trx.pool_guards(), table_id)
            .await?;
        let keys = index_columns
            .into_iter()
            .map(|index_column| {
                vec![
                    Val::from(table_id),
                    Val::from(index_column.index_slot.get()),
                    Val::from(index_column.index_column_no),
                ]
            })
            .collect();
        trx.catalog_delete_primary_key_batch_mvcc(self.table, PK_NO_INDEX_COLUMNS, keys)
            .await
            .attach_with(|| {
                format!("operation=catalog_index_columns_delete_by_table, table_id={table_id}")
            })
    }

    /// List all index-column rows of one table from uncommitted-visible rows.
    pub(crate) async fn list_uncommitted_by_table_id(
        &self,
        guards: &PoolGuards,
        table_id: TableID,
    ) -> RuntimeResult<Vec<IndexColumnObject>> {
        let mut res = vec![];
        self.table
            .table_scan_uncommitted(guards, |col_layout, row| {
                if row.is_deleted() {
                    return true;
                }
                let table_id_in_row = row
                    .val(col_layout, COL_NO_INDEX_COLUMNS_TABLE_ID)
                    .as_u64()
                    .unwrap();
                if table_id_in_row == table_id.as_u64() {
                    let obj = row_to_index_column_object(col_layout, row);
                    res.push(obj);
                }
                true
            })
            .await
            .change_context(RuntimeError::CatalogAccess)
            .attach_with(|| format!("operation=list_catalog_index_columns, table_id={table_id}"))?;
        Ok(res)
    }
}

#[inline]
fn cols_from_index_object(obj: &IndexObject) -> Vec<Val> {
    vec![
        Val::from(obj.table_id),
        Val::from(obj.index_slot.get()),
        Val::from(obj.index_attributes.bits()),
    ]
}

#[inline]
fn cols_from_index_column_object(obj: &IndexColumnObject) -> Vec<Val> {
    vec![
        Val::from(obj.table_id),
        Val::from(obj.index_slot.get()),
        Val::from(obj.index_column_no),
        Val::from(obj.column_no),
        Val::from(obj.index_order as u8),
    ]
}

/// Return static table definition of `catalog.indexes`.
pub(super) fn catalog_definition_of_indexes() -> &'static CatalogDefinition {
    static DEF: OnceLock<CatalogDefinition> = OnceLock::new();
    DEF.get_or_init(|| {
        CatalogDefinition {
            table_id: TABLE_ID_INDEXES,
            metadata: TableMetadata::try_new(
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
                        vec![IndexKeySpec::new(0), IndexKeySpec::new(1)],
                        IndexAttributes::PK,
                    ),
                ],
            )
            .expect("valid table metadata"),
        }
    })
}

/// Return static table definition of `catalog.index_columns`.
pub(super) fn catalog_definition_of_index_columns() -> &'static CatalogDefinition {
    static DEF: OnceLock<CatalogDefinition> = OnceLock::new();
    DEF.get_or_init(|| {
        CatalogDefinition {
            table_id: TABLE_ID_INDEX_COLUMNS,
            metadata: TableMetadata::try_new(
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
                        vec![
                            IndexKeySpec::new(0),
                            IndexKeySpec::new(1),
                            IndexKeySpec::new(2),
                        ],
                        IndexAttributes::PK,
                    ),
                ],
            )
            .expect("valid table metadata"),
        }
    })
}

#[inline]
fn row_to_index_object(col_layout: &TableColumnLayout, row: Row<'_>) -> IndexObject {
    let table_id = TableID::from(
        row.val(col_layout, COL_NO_INDEXES_TABLE_ID)
            .as_u64()
            .unwrap(),
    );
    let index_slot = row
        .val(col_layout, COL_NO_INDEXES_INDEX_NO)
        .as_u16()
        .unwrap();
    let index_attributes = row
        .val(col_layout, COL_NO_INDEXES_INDEX_ATTRIBUTES)
        .as_u32()
        .unwrap();
    IndexObject {
        table_id,
        index_slot: IndexSlot::from(index_slot),
        index_attributes: IndexAttributes::from_bits_truncate(index_attributes),
    }
}

#[inline]
fn row_to_index_column_object(col_layout: &TableColumnLayout, row: Row<'_>) -> IndexColumnObject {
    let table_id = TableID::from(
        row.val(col_layout, COL_NO_INDEX_COLUMNS_TABLE_ID)
            .as_u64()
            .unwrap(),
    );
    let index_slot = row
        .val(col_layout, COL_NO_INDEX_COLUMNS_INDEX_NO)
        .as_u16()
        .unwrap();
    let index_column_no = row
        .val(col_layout, COL_NO_INDEX_COLUMNS_INDEX_COLUMN_NO)
        .as_u16()
        .unwrap();
    let column_no = row
        .val(col_layout, COL_NO_INDEX_COLUMNS_COLUMN_NO)
        .as_u16()
        .unwrap();
    let index_order = row
        .val(col_layout, COL_NO_INDEX_COLUMNS_INDEX_ORDER)
        .as_u8()
        .unwrap();
    IndexColumnObject {
        table_id,
        index_slot: IndexSlot::from(index_slot),
        index_column_no,
        column_no,
        index_order: IndexOrder::from(index_order),
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
    fn test_indexes_delete_by_id() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = open_catalog_test_engine(main_dir, None).await;
            let session = engine.new_session().unwrap();

            let idx_42_0 = IndexObject {
                table_id: TableID::new(42),
                index_slot: IndexSlot::new(0),
                index_attributes: IndexAttributes::PK,
            };
            let idx_42_1 = IndexObject {
                table_id: TableID::new(42),
                index_slot: IndexSlot::new(1),
                index_attributes: IndexAttributes::empty(),
            };
            let idx_43_0 = IndexObject {
                table_id: TableID::new(43),
                index_slot: IndexSlot::new(0),
                index_attributes: IndexAttributes::PK,
            };

            let mut trx = begin_catalog_test_trx(&session);
            engine
                .inner()
                .core
                .catalog()
                .storage
                .indexes()
                .insert_batch(trx.trx(), &[idx_42_0, idx_42_1, idx_43_0])
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
                    .indexes()
                    .delete_by_id(trx.trx(), TableID::new(42), IndexSlot::new(1),)
                    .await
                    .unwrap()
            );
            assert!(
                !engine
                    .inner()
                    .core
                    .catalog()
                    .storage
                    .indexes()
                    .delete_by_id(trx.trx(), TableID::new(42), IndexSlot::new(9),)
                    .await
                    .unwrap()
            );
            trx.commit(DDLRedo::DropTable(TableID::new(42))).await;

            let idx_42 = engine
                .inner()
                .core
                .catalog()
                .storage
                .indexes()
                .list_uncommitted_by_table_id(&session.pool_guards(), TableID::new(42))
                .await
                .unwrap();
            assert_eq!(idx_42.len(), 1);
            assert_eq!(idx_42[0].index_slot, IndexSlot::new(0));

            let idx_43 = engine
                .inner()
                .core
                .catalog()
                .storage
                .indexes()
                .list_uncommitted_by_table_id(&session.pool_guards(), TableID::new(43))
                .await
                .unwrap();
            assert_eq!(idx_43.len(), 1);
            assert_eq!(idx_43[0].index_slot, IndexSlot::new(0));

            let mut trx = begin_catalog_test_trx(&session);
            assert!(
                !engine
                    .inner()
                    .core
                    .catalog()
                    .storage
                    .indexes()
                    .delete_by_id(trx.trx(), TableID::new(42), IndexSlot::new(1),)
                    .await
                    .unwrap()
            );
            assert!(
                engine
                    .inner()
                    .core
                    .catalog()
                    .storage
                    .indexes()
                    .delete_by_id(trx.trx(), TableID::new(42), IndexSlot::new(0),)
                    .await
                    .unwrap()
            );
            assert!(
                engine
                    .inner()
                    .core
                    .catalog()
                    .storage
                    .indexes()
                    .delete_by_id(trx.trx(), TableID::new(43), IndexSlot::new(0),)
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
                    .indexes()
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
                    .indexes()
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
    fn test_indexes_delete_by_table_id_counts_and_is_idempotent() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = open_catalog_test_engine(main_dir, None).await;
            let session = engine.new_session().unwrap();

            let indexes = [
                IndexObject {
                    table_id: TableID::new(42),
                    index_slot: IndexSlot::new(0),
                    index_attributes: IndexAttributes::PK,
                },
                IndexObject {
                    table_id: TableID::new(42),
                    index_slot: IndexSlot::new(1),
                    index_attributes: IndexAttributes::empty(),
                },
                IndexObject {
                    table_id: TableID::new(43),
                    index_slot: IndexSlot::new(0),
                    index_attributes: IndexAttributes::PK,
                },
            ];

            let mut trx = begin_catalog_test_trx(&session);
            engine
                .inner()
                .core
                .catalog()
                .storage
                .indexes()
                .insert_batch(trx.trx(), &indexes)
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
                    .indexes()
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
                    .indexes()
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
                    .indexes()
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
                .indexes()
                .list_uncommitted_by_table_id(&session.pool_guards(), TableID::new(43))
                .await
                .unwrap();
            assert_eq!(remaining.len(), 1);
            assert_eq!(remaining[0].index_slot, IndexSlot::new(0));

            drop(session);
            drop(engine);
        });
    }

    #[test]
    fn test_index_columns_delete_by_index_and_table_id() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = open_catalog_test_engine(main_dir, None).await;
            let session = engine.new_session().unwrap();

            let index_columns = [
                IndexColumnObject {
                    table_id: TableID::new(42),
                    index_slot: IndexSlot::new(0),
                    index_column_no: 0,
                    column_no: 0,
                    index_order: IndexOrder::Asc,
                },
                IndexColumnObject {
                    table_id: TableID::new(42),
                    index_slot: IndexSlot::new(1),
                    index_column_no: 0,
                    column_no: 1,
                    index_order: IndexOrder::Asc,
                },
                IndexColumnObject {
                    table_id: TableID::new(42),
                    index_slot: IndexSlot::new(1),
                    index_column_no: 1,
                    column_no: 2,
                    index_order: IndexOrder::Desc,
                },
                IndexColumnObject {
                    table_id: TableID::new(43),
                    index_slot: IndexSlot::new(1),
                    index_column_no: 0,
                    column_no: 0,
                    index_order: IndexOrder::Asc,
                },
            ];

            let mut trx = begin_catalog_test_trx(&session);
            engine
                .inner()
                .core
                .catalog()
                .storage
                .index_columns()
                .insert_batch(trx.trx(), &index_columns)
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
                    .index_columns()
                    .delete_by_index(trx.trx(), TableID::new(42), IndexSlot::new(1),)
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
                    .index_columns()
                    .delete_by_index(trx.trx(), TableID::new(42), IndexSlot::new(1),)
                    .await
                    .unwrap(),
                0
            );
            trx.commit(DDLRedo::DropTable(TableID::new(42))).await;

            let remaining_42 = engine
                .inner()
                .core
                .catalog()
                .storage
                .index_columns()
                .list_uncommitted_by_table_id(&session.pool_guards(), TableID::new(42))
                .await
                .unwrap();
            assert_eq!(remaining_42.len(), 1);
            assert_eq!(remaining_42[0].index_slot, IndexSlot::new(0));

            let mut trx = begin_catalog_test_trx(&session);
            assert_eq!(
                engine
                    .inner()
                    .core
                    .catalog()
                    .storage
                    .index_columns()
                    .delete_by_table_id(trx.trx(), TableID::new(42))
                    .await
                    .unwrap(),
                1
            );
            assert_eq!(
                engine
                    .inner()
                    .core
                    .catalog()
                    .storage
                    .index_columns()
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
                    .index_columns()
                    .list_uncommitted_by_table_id(&session.pool_guards(), TableID::new(42))
                    .await
                    .unwrap()
                    .is_empty()
            );
            let remaining_43 = engine
                .inner()
                .core
                .catalog()
                .storage
                .index_columns()
                .list_uncommitted_by_table_id(&session.pool_guards(), TableID::new(43))
                .await
                .unwrap();
            assert_eq!(remaining_43.len(), 1);
            assert_eq!(remaining_43[0].table_id, TableID::new(43));
            assert_eq!(remaining_43[0].index_slot, IndexSlot::new(1));

            drop(session);
            drop(engine);
        });
    }
}
