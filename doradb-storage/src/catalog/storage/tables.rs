use crate::buffer::PoolGuards;
use crate::catalog::storage::CatalogDefinition;
use crate::catalog::storage::object::TableObject;
use crate::catalog::table::{TableColumnLayout, TableMetadata};
use crate::catalog::{CatalogIndexNo, CatalogTable};
use crate::catalog::{
    ID_DOMAIN_END, StorageColumnFlags, StorageColumnSpec, StorageIndexFlags, StorageIndexKey,
    StorageIndexSpec, catalog_table_id_from_slot,
};
use crate::error::{
    DataIntegrityError, DataIntegrityResult, MultiDomainResultExt, RuntimeError,
    RuntimeOrFatalResult, RuntimeResult,
};
use crate::id::TableID;
use crate::row::ops::DeleteMvcc;
use crate::row::{Row, RowRead};
use crate::trx::PrivateTransaction;
use crate::value::Val;
use crate::value::ValKind;
use error_stack::Report;
use error_stack::ResultExt;
use std::sync::OnceLock;

/// Catalog table id for `catalog.tables`.
pub(crate) const TABLE_ID_TABLES: TableID = catalog_table_id_from_slot(0);
const COL_NO_TABLES_TABLE_ID: usize = 0;
const COL_NO_TABLES_STORAGE_EPOCH: usize = 1;
const COL_NO_TABLES_NEXT_COLUMN_ID: usize = 2;
const COL_NO_TABLES_NEXT_INDEX_ID: usize = 3;
const COL_NO_TABLES_INDEX_SLOT_COUNT: usize = 4;
const PK_NO_TABLES: CatalogIndexNo = CatalogIndexNo::new(0);

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
        let mut decode_error = None;
        self.table
            .table_scan_uncommitted(guards, |col_layout, row| {
                if row.is_deleted() {
                    return true;
                }
                match row_to_table_object(col_layout, row) {
                    Ok(object) => {
                        res.push(object);
                        true
                    }
                    Err(err) => {
                        decode_error = Some(err);
                        false
                    }
                }
            })
            .await
            .change_context(RuntimeError::CatalogAccess)
            .attach("operation=list_catalog_tables")?;
        if let Some(err) = decode_error {
            return Err(err
                .change_context(RuntimeError::CatalogAccess)
                .attach("operation=list_catalog_tables, phase=decode_row"));
        }
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
        let vals = self
            .table
            .index_lookup_unique_uncommitted(guards, PK_NO_TABLES, &key_vals, |col_layout, row| {
                (0..5)
                    .map(|idx| row.val(col_layout, idx))
                    .collect::<Vec<_>>()
            })
            .await
            .change_context(RuntimeError::CatalogAccess)
            .attach_with(|| format!("operation=find_catalog_table, table_id={table_id}"))?;
        vals.map(|vals| table_object_from_vals(&vals))
            .transpose()
            .change_context(RuntimeError::CatalogAccess)
            .attach_with(|| {
                format!("operation=find_catalog_table, phase=decode_row, table_id={table_id}")
            })
    }

    /// Insert a table row whose primary key is owned by the current DDL.
    ///
    /// Create-table uses an atomically allocated id. Create-index first deletes
    /// and then reinserts the same row in one metadata-gated transaction. The
    /// primary key is therefore unique by construction, and the statement
    /// boundary asserts if storage reports an Operation failure.
    pub(crate) async fn insert(
        &self,
        trx: &mut PrivateTransaction,
        obj: &TableObject,
    ) -> RuntimeOrFatalResult<()> {
        let cols = vec![
            Val::from(obj.table_id),
            Val::from(obj.storage_epoch),
            Val::from(obj.next_column_id),
            Val::from(obj.next_index_id),
            Val::from(obj.index_slot_count),
        ];
        trx.catalog_insert_mvcc(self.table, cols)
            .await
            .map(|_| ())
            .attach_with(|| format!("operation=catalog_tables_insert, table_id={}", obj.table_id))
    }

    /// Delete a table by id.
    pub(crate) async fn delete_by_id(
        &self,
        trx: &mut PrivateTransaction,
        id: TableID,
    ) -> RuntimeOrFatalResult<bool> {
        let res = trx
            .catalog_delete_primary_key_mvcc(self.table, PK_NO_TABLES, vec![Val::from(id)])
            .await
            .attach_with(|| format!("operation=catalog_tables_delete, table_id={id}"))?;
        Ok(matches!(res, DeleteMvcc::Deleted))
    }

    /// Replace the table metadata row through one delete-then-insert statement.
    pub(crate) async fn replace(
        &self,
        trx: &mut PrivateTransaction,
        obj: &TableObject,
    ) -> RuntimeOrFatalResult<bool> {
        let key_vals = vec![Val::from(obj.table_id)];
        let cols = vec![
            Val::from(obj.table_id),
            Val::from(obj.storage_epoch),
            Val::from(obj.next_column_id),
            Val::from(obj.next_index_id),
            Val::from(obj.index_slot_count),
        ];
        let res = trx
            .catalog_replace_primary_key_mvcc(self.table, PK_NO_TABLES, key_vals, cols)
            .await
            .attach_with(|| {
                format!(
                    "operation=catalog_tables_replace, table_id={}",
                    obj.table_id
                )
            })?;
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
                    // table_id U64: stable user-table identity.
                    StorageColumnSpec::new(ValKind::U64, StorageColumnFlags::empty()),
                    // storage_epoch U64: monotonic active storage-schema epoch.
                    StorageColumnSpec::new(ValKind::U64, StorageColumnFlags::empty()),
                    // next_column_id U64: exclusive stable column-ID allocator bound.
                    StorageColumnSpec::new(ValKind::U64, StorageColumnFlags::empty()),
                    // next_index_id U64: exclusive stable index-ID allocator bound.
                    StorageColumnSpec::new(ValKind::U64, StorageColumnFlags::empty()),
                    // index_slot_count U32: exclusive physical index-slot count.
                    StorageColumnSpec::new(ValKind::U32, StorageColumnFlags::empty()),
                ],
                vec![
                    // Primary key: table_id.
                    StorageIndexSpec::new(vec![StorageIndexKey::new(0)], StorageIndexFlags::PK),
                ],
            )
            .expect("valid table metadata"),
        }
    })
}

#[inline]
fn row_to_table_object(
    col_layout: &TableColumnLayout,
    row: Row<'_>,
) -> DataIntegrityResult<TableObject> {
    let vals = (0..5)
        .map(|idx| row.val(col_layout, idx))
        .collect::<Vec<_>>();
    table_object_from_vals(&vals)
}

pub(super) fn table_object_from_vals(vals: &[Val]) -> DataIntegrityResult<TableObject> {
    if vals.len() != 5 {
        return Err(
            Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                "catalog.tables value count {}, expected 5",
                vals.len()
            )),
        );
    }
    let table_id = vals[COL_NO_TABLES_TABLE_ID]
        .as_u64()
        .map(TableID::from)
        .ok_or_else(|| Report::new(DataIntegrityError::InvalidPayload))?;
    let storage_epoch = vals[COL_NO_TABLES_STORAGE_EPOCH]
        .as_u64()
        .ok_or_else(|| Report::new(DataIntegrityError::InvalidPayload))?;
    let next_column_id = vals[COL_NO_TABLES_NEXT_COLUMN_ID]
        .as_u64()
        .ok_or_else(|| Report::new(DataIntegrityError::InvalidPayload))?;
    let next_index_id = vals[COL_NO_TABLES_NEXT_INDEX_ID]
        .as_u64()
        .ok_or_else(|| Report::new(DataIntegrityError::InvalidPayload))?;
    let index_slot_count = vals[COL_NO_TABLES_INDEX_SLOT_COUNT]
        .as_u32()
        .ok_or_else(|| Report::new(DataIntegrityError::InvalidPayload))?;
    if next_column_id > ID_DOMAIN_END || next_index_id > ID_DOMAIN_END {
        return Err(Report::new(DataIntegrityError::InvalidPayload).attach(format!(
            "catalog.tables allocator exceeds stable ID domain: next_column_id={next_column_id}, next_index_id={next_index_id}"
        )));
    }
    if index_slot_count > u32::from(u16::MAX) + 1 {
        return Err(
            Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                "catalog.tables index_slot_count exceeds physical domain: {index_slot_count}"
            )),
        );
    }
    Ok(TableObject {
        table_id,
        storage_epoch,
        next_column_id,
        next_index_id,
        index_slot_count,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::{BufferPool, PoolGuards, PoolRole};
    use crate::catalog::storage::tests::begin_catalog_test_trx;
    use crate::catalog::tests::{open_catalog_test_engine, table1};
    use crate::log::redo::DDLRedo;
    use crate::session::tests::SessionTestExt;
    use tempfile::TempDir;

    #[test]
    fn test_tables_delete_by_id() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = open_catalog_test_engine(main_dir, None).await;
            let session = engine.new_session().unwrap();

            let table100 = TableObject {
                table_id: TableID::new(100),
                storage_epoch: 0,
                next_column_id: 0,
                next_index_id: 0,
                index_slot_count: 0,
            };
            let table101 = TableObject {
                table_id: TableID::new(101),
                storage_epoch: 0,
                next_column_id: 0,
                next_index_id: 0,
                index_slot_count: 0,
            };
            let mut trx = begin_catalog_test_trx(&session);
            engine
                .inner()
                .core
                .catalog()
                .storage
                .tables()
                .insert(trx.trx(), &table100)
                .await
                .unwrap();
            engine
                .inner()
                .core
                .catalog()
                .storage
                .tables()
                .insert(trx.trx(), &table101)
                .await
                .unwrap();
            trx.commit(DDLRedo::CreateTable(table100.table_id)).await;

            let mut trx = begin_catalog_test_trx(&session);
            assert!(
                engine
                    .inner()
                    .core
                    .catalog()
                    .storage
                    .tables()
                    .delete_by_id(trx.trx(), table100.table_id)
                    .await
                    .unwrap()
            );
            assert!(
                !engine
                    .inner()
                    .core
                    .catalog()
                    .storage
                    .tables()
                    .delete_by_id(trx.trx(), TableID::new(999))
                    .await
                    .unwrap()
            );
            trx.commit(DDLRedo::DropTable(table100.table_id)).await;

            assert!(
                engine
                    .inner()
                    .core
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
                    .inner()
                    .core
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
                    .push(
                        PoolRole::Meta,
                        engine.inner().pools.meta.create_base_guard(),
                    )
                    .build();
                assert!(
                    engine
                        .inner()
                        .core
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
