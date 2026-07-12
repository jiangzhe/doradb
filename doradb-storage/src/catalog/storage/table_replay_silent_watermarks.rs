use crate::buffer::PoolGuards;
use crate::catalog::CatalogTable;
use crate::catalog::storage::CatalogDefinition;
use crate::catalog::storage::object::SilentWatermarkObject;
use crate::catalog::table::{TableColumnLayout, TableMetadata};
use crate::catalog::{
    ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexSpec, catalog_table_id_from_slot,
};
use crate::error::{DataIntegrityError, Error, Result};
use crate::id::{TableID, TrxID};
use crate::row::ops::DeleteMvcc;
use crate::row::{Row, RowRead};
use crate::table::NoTrxUpsertChange;
use crate::trx::stmt::Statement;
use crate::value::Val;
use crate::value::ValKind;
use error_stack::Report;
use semistr::SemiStr;
use std::sync::OnceLock;

/// Catalog table id for `catalog.table_replay_silent_watermarks`.
pub(crate) const TABLE_ID_TABLE_REPLAY_SILENT_WATERMARKS: TableID = catalog_table_id_from_slot(4);
const COL_NO_TABLE_REPLAY_SILENT_WATERMARKS_TABLE_ID: usize = 0;
const COL_NAME_TABLE_REPLAY_SILENT_WATERMARKS_TABLE_ID: &str = "table_id";
const COL_NO_TABLE_REPLAY_SILENT_WATERMARKS_HEAP_REDO_START_TS: usize = 1;
const COL_NAME_TABLE_REPLAY_SILENT_WATERMARKS_HEAP_REDO_START_TS: &str = "heap_redo_start_ts";
const COL_NO_TABLE_REPLAY_SILENT_WATERMARKS_DELETION_CUTOFF_TS: usize = 2;
const COL_NAME_TABLE_REPLAY_SILENT_WATERMARKS_DELETION_CUTOFF_TS: &str = "deletion_cutoff_ts";
const PK_NO_TABLE_REPLAY_SILENT_WATERMARKS: usize = 0;

/// Runtime accessor for `catalog.table_replay_silent_watermarks`.
pub(crate) struct TableReplaySilentWatermarks<'a> {
    pub(super) table: &'a CatalogTable,
}

impl TableReplaySilentWatermarks<'_> {
    /// Return the catalog table id used for logical redo grouping.
    #[inline]
    pub(crate) fn table_id(&self) -> TableID {
        self.table.table_id()
    }

    /// Find a silent replay watermark by user table id.
    #[inline]
    pub(crate) async fn find_uncommitted_by_table_id(
        &self,
        guards: &PoolGuards,
        table_id: TableID,
    ) -> Result<Option<SilentWatermarkObject>> {
        let key_vals = [Val::from(table_id)];
        self.table
            .index_lookup_unique_uncommitted(
                guards,
                PK_NO_TABLE_REPLAY_SILENT_WATERMARKS,
                &key_vals,
                row_to_table_replay_silent_watermark_object,
            )
            .await
    }

    /// Upsert one caller-supplied monotonic live watermark without transaction state.
    ///
    /// The supplied callback is forwarded to the catalog table's no-transaction
    /// primary-key upsert and therefore runs at most once, only after a
    /// successful insert or update.
    pub(crate) async fn upsert_no_trx<F>(
        &self,
        guards: &PoolGuards,
        obj: &SilentWatermarkObject,
        on_change: F,
    ) -> Result<()>
    where
        F: FnOnce(NoTrxUpsertChange),
    {
        debug_assert!({
            let current = self
                .find_uncommitted_by_table_id(guards, obj.table_id)
                .await?;
            current.is_none_or(|current| {
                current.heap_redo_start_ts <= obj.heap_redo_start_ts
                    && current.deletion_cutoff_ts <= obj.deletion_cutoff_ts
            })
        });
        self.table
            .upsert_primary_key_no_trx(
                guards,
                cols_from_table_replay_silent_watermark(obj),
                false,
                on_change,
            )
            .await
    }

    /// Delete one watermark row by user table id.
    pub(crate) async fn delete_by_table_id(
        &self,
        stmt: &mut Statement<'_>,
        table_id: TableID,
    ) -> Result<bool> {
        let key_vals = [Val::from(table_id)];
        let res = stmt
            .catalog_delete_primary_key_mvcc(
                self.table,
                PK_NO_TABLE_REPLAY_SILENT_WATERMARKS,
                &key_vals,
                true,
            )
            .await?;
        Ok(matches!(res, DeleteMvcc::Deleted))
    }
}

#[inline]
fn cols_from_table_replay_silent_watermark(obj: &SilentWatermarkObject) -> Vec<Val> {
    vec![
        Val::from(obj.table_id),
        Val::from(obj.heap_redo_start_ts.as_u64()),
        Val::from(obj.deletion_cutoff_ts.as_u64()),
    ]
}

/// Return static table definition of `catalog.table_replay_silent_watermarks`.
pub(super) fn catalog_definition_of_table_replay_silent_watermarks() -> &'static CatalogDefinition {
    static DEF: OnceLock<CatalogDefinition> = OnceLock::new();
    DEF.get_or_init(|| CatalogDefinition {
        table_id: TABLE_ID_TABLE_REPLAY_SILENT_WATERMARKS,
        metadata: TableMetadata::try_new(
            vec![
                ColumnSpec {
                    column_name: SemiStr::new(COL_NAME_TABLE_REPLAY_SILENT_WATERMARKS_TABLE_ID),
                    column_type: ValKind::U64,
                    column_attributes: ColumnAttributes::INDEX,
                },
                ColumnSpec {
                    column_name: SemiStr::new(
                        COL_NAME_TABLE_REPLAY_SILENT_WATERMARKS_HEAP_REDO_START_TS,
                    ),
                    column_type: ValKind::U64,
                    column_attributes: ColumnAttributes::empty(),
                },
                ColumnSpec {
                    column_name: SemiStr::new(
                        COL_NAME_TABLE_REPLAY_SILENT_WATERMARKS_DELETION_CUTOFF_TS,
                    ),
                    column_type: ValKind::U64,
                    column_attributes: ColumnAttributes::empty(),
                },
            ],
            vec![IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::PK)],
        )
        .expect("valid table metadata"),
    })
}

#[inline]
pub(super) fn table_replay_silent_watermark_object_from_vals(
    vals: &[Val],
) -> Result<SilentWatermarkObject> {
    let table_id = val_u64(
        vals,
        COL_NO_TABLE_REPLAY_SILENT_WATERMARKS_TABLE_ID,
        "table_id",
    )?;
    let heap_redo_start_ts = val_u64(
        vals,
        COL_NO_TABLE_REPLAY_SILENT_WATERMARKS_HEAP_REDO_START_TS,
        "heap_redo_start_ts",
    )?;
    let deletion_cutoff_ts = val_u64(
        vals,
        COL_NO_TABLE_REPLAY_SILENT_WATERMARKS_DELETION_CUTOFF_TS,
        "deletion_cutoff_ts",
    )?;
    Ok(SilentWatermarkObject {
        table_id: TableID::new(table_id),
        heap_redo_start_ts: TrxID::new(heap_redo_start_ts),
        deletion_cutoff_ts: TrxID::new(deletion_cutoff_ts),
    })
}

#[inline]
fn row_to_table_replay_silent_watermark_object(
    col_layout: &TableColumnLayout,
    row: Row<'_>,
) -> SilentWatermarkObject {
    let table_id = TableID::from(
        row.val(col_layout, COL_NO_TABLE_REPLAY_SILENT_WATERMARKS_TABLE_ID)
            .as_u64()
            .unwrap(),
    );
    let heap_redo_start_ts = row
        .val(
            col_layout,
            COL_NO_TABLE_REPLAY_SILENT_WATERMARKS_HEAP_REDO_START_TS,
        )
        .as_u64()
        .unwrap();
    let deletion_cutoff_ts = row
        .val(
            col_layout,
            COL_NO_TABLE_REPLAY_SILENT_WATERMARKS_DELETION_CUTOFF_TS,
        )
        .as_u64()
        .unwrap();
    SilentWatermarkObject {
        table_id,
        heap_redo_start_ts: TrxID::new(heap_redo_start_ts),
        deletion_cutoff_ts: TrxID::new(deletion_cutoff_ts),
    }
}

#[inline]
fn val_u64(vals: &[Val], idx: usize, name: &'static str) -> Result<u64> {
    vals.get(idx).and_then(Val::as_u64).ok_or_else(|| {
        Error::from(
            Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                "invalid table replay silent watermark {name} column at index {idx}"
            )),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::tests::open_catalog_test_engine;
    use crate::log::redo::{DDLRedo, RowRedoKind};
    use crate::row::ops::{SelectKey, UpdateCol};
    use crate::session::tests::SessionTestExt;
    use tempfile::TempDir;

    #[test]
    fn test_sys_trx_silent_watermark_emits_monotonic_logical_redo() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = open_catalog_test_engine(temp_dir.path().to_path_buf(), None).await;
            let session = engine.new_session().unwrap();
            let guards = session.pool_guards();
            let table_id = TableID::new(102);

            let mut insert = engine.inner().trx_sys.begin_sys_trx();
            insert
                .upsert_silent_watermark(
                    engine.catalog(),
                    &guards,
                    SilentWatermarkObject {
                        table_id,
                        heap_redo_start_ts: TrxID::new(7),
                        deletion_cutoff_ts: TrxID::new(11),
                    },
                )
                .await
                .unwrap();
            assert!(matches!(
                insert.redo().ddl.as_deref(),
                Some(DDLRedo::TableReplaySilentWatermark { table_id: id }) if *id == table_id
            ));
            let insert_redo = insert
                .redo()
                .dml
                .get(&TABLE_ID_TABLE_REPLAY_SILENT_WATERMARKS)
                .unwrap()
                .rows
                .values()
                .next()
                .unwrap();
            assert!(matches!(&insert_redo.kind, RowRedoKind::Insert(..)));
            engine.inner().trx_sys.commit_sys(insert).unwrap();

            let mut update = engine.inner().trx_sys.begin_sys_trx();
            update
                .upsert_silent_watermark(
                    engine.catalog(),
                    &guards,
                    SilentWatermarkObject {
                        table_id,
                        heap_redo_start_ts: TrxID::new(7),
                        deletion_cutoff_ts: TrxID::new(13),
                    },
                )
                .await
                .unwrap();
            let update_redo = update
                .redo()
                .dml
                .get(&TABLE_ID_TABLE_REPLAY_SILENT_WATERMARKS)
                .unwrap()
                .rows
                .values()
                .next()
                .unwrap();
            assert!(matches!(
                &update_redo.kind,
                RowRedoKind::UpdateByPrimaryKey(key, cols)
                    if key == &SelectKey::new(0, vec![Val::from(table_id)])
                        && cols == &vec![UpdateCol {
                            idx: COL_NO_TABLE_REPLAY_SILENT_WATERMARKS_DELETION_CUTOFF_TS,
                            val: Val::from(13u64),
                        }]
            ));
            engine.inner().trx_sys.commit_sys(update).unwrap();

            let mut no_op = engine.inner().trx_sys.begin_sys_trx();
            no_op
                .upsert_silent_watermark(
                    engine.catalog(),
                    &guards,
                    SilentWatermarkObject {
                        table_id,
                        heap_redo_start_ts: TrxID::new(7),
                        deletion_cutoff_ts: TrxID::new(13),
                    },
                )
                .await
                .unwrap();
            assert!(no_op.redo().dml.is_empty());
            assert!(matches!(
                no_op.redo().ddl.as_deref(),
                Some(DDLRedo::TableReplaySilentWatermark { table_id: id }) if *id == table_id
            ));
        });
    }
}
