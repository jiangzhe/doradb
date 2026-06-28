use crate::buffer::PoolGuards;
use crate::catalog::CatalogTable;
use crate::catalog::storage::CatalogDefinition;
use crate::catalog::storage::object::SlientWatermarkObject;
use crate::catalog::table::{TableColumnLayout, TableMetadata};
use crate::catalog::{ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexSpec};
use crate::error::{DataIntegrityError, Error, InternalError, Result};
use crate::id::{TableID, TrxID};
use crate::row::ops::{DeleteMvcc, SelectKey};
use crate::row::{Row, RowRead};
use crate::trx::stmt::Statement;
use crate::value::Val;
use crate::value::ValKind;
use error_stack::Report;
use semistr::SemiStr;
use std::sync::OnceLock;

/// Catalog table id for `catalog.table_replay_silent_watermarks`.
pub(crate) const TABLE_ID_TABLE_REPLAY_SILENT_WATERMARKS: TableID = TableID::new(4);
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
    /// List all watermark rows from uncommitted-visible catalog state.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "task scope requires the catalog accessor even though production callers use point lookup and checkpoint-root decoding"
        )
    )]
    pub(crate) async fn list_uncommitted(
        &self,
        guards: &PoolGuards,
    ) -> Result<Vec<SlientWatermarkObject>> {
        let mut res = vec![];
        self.table
            .table_scan_uncommitted(guards, |col_layout, row| {
                if row.is_deleted() {
                    return true;
                }
                res.push(row_to_table_replay_silent_watermark_object(col_layout, row));
                true
            })
            .await?;
        Ok(res)
    }

    /// Find a silent replay watermark by user table id.
    #[inline]
    pub(crate) async fn find_uncommitted_by_table_id(
        &self,
        guards: &PoolGuards,
        table_id: TableID,
    ) -> Result<Option<SlientWatermarkObject>> {
        let key = SelectKey::new(
            PK_NO_TABLE_REPLAY_SILENT_WATERMARKS,
            vec![Val::from(table_id)],
        );
        self.table
            .index_lookup_unique_uncommitted(
                guards,
                &key,
                row_to_table_replay_silent_watermark_object,
            )
            .await
    }

    /// Upsert one watermark row by fieldwise maximum.
    pub(crate) async fn upsert(
        &self,
        stmt: &mut Statement<'_>,
        obj: &SlientWatermarkObject,
    ) -> Result<bool> {
        let existing = self
            .find_uncommitted_by_table_id(stmt.runtime().pool_guards(), obj.table_id)
            .await?;
        let merged = existing.map_or(*obj, |existing| SlientWatermarkObject {
            table_id: obj.table_id,
            heap_redo_start_ts: existing.heap_redo_start_ts.max(obj.heap_redo_start_ts),
            deletion_cutoff_ts: existing.deletion_cutoff_ts.max(obj.deletion_cutoff_ts),
        });

        if existing == Some(merged) {
            return Ok(false);
        }
        if existing.is_some() {
            let deleted = self.delete_by_table_id(stmt, obj.table_id).await?;
            debug_assert!(deleted);
        }
        self.insert(stmt, &merged).await?;
        Ok(true)
    }

    /// Delete one watermark row by user table id.
    pub(crate) async fn delete_by_table_id(
        &self,
        stmt: &mut Statement<'_>,
        table_id: TableID,
    ) -> Result<bool> {
        let key = SelectKey::new(
            PK_NO_TABLE_REPLAY_SILENT_WATERMARKS,
            vec![Val::from(table_id)],
        );
        let res = stmt
            .catalog_delete_unique_mvcc(self.table, &key, true)
            .await?;
        Ok(matches!(res, DeleteMvcc::Deleted))
    }

    async fn insert(&self, stmt: &mut Statement<'_>, obj: &SlientWatermarkObject) -> Result<()> {
        let cols = vec![
            Val::from(obj.table_id),
            Val::from(obj.heap_redo_start_ts.as_u64()),
            Val::from(obj.deletion_cutoff_ts.as_u64()),
        ];
        if stmt.catalog_insert_mvcc(self.table, cols).await.is_ok() {
            return Ok(());
        }
        Err(Report::new(InternalError::Generic)
            .attach(format!(
                "insert table replay silent watermark failed: table_id={}",
                obj.table_id
            ))
            .into())
    }
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
) -> Result<SlientWatermarkObject> {
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
    Ok(SlientWatermarkObject {
        table_id: TableID::new(table_id),
        heap_redo_start_ts: TrxID::new(heap_redo_start_ts),
        deletion_cutoff_ts: TrxID::new(deletion_cutoff_ts),
    })
}

#[inline]
fn row_to_table_replay_silent_watermark_object(
    col_layout: &TableColumnLayout,
    row: Row<'_>,
) -> SlientWatermarkObject {
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
    SlientWatermarkObject {
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
    use crate::log::redo::DDLRedo;
    use crate::session::tests::SessionTestExt;
    use crate::trx::Transaction;
    use tempfile::TempDir;

    fn mark_watermark_ddl(trx: &mut Transaction, table_id: TableID) {
        let old = trx
            .set_ddl_redo(DDLRedo::TableReplaySilentWatermark { table_id })
            .unwrap();
        debug_assert!(old.is_none());
    }

    #[test]
    fn test_table_replay_silent_watermark_upsert_merges_fieldwise_max() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = open_catalog_test_engine(temp_dir.path().to_path_buf(), None).await;
            let mut session = engine.new_session().unwrap();
            let table_id = TableID::new(100);

            let mut trx = session.begin_trx().unwrap();
            let inserted = trx
                .exec(async |stmt| {
                    engine
                        .catalog()
                        .storage
                        .table_replay_silent_watermarks()
                        .upsert(
                            stmt,
                            &SlientWatermarkObject {
                                table_id,
                                heap_redo_start_ts: TrxID::new(7),
                                deletion_cutoff_ts: TrxID::new(9),
                            },
                        )
                        .await
                })
                .await
                .unwrap();
            assert!(inserted);
            mark_watermark_ddl(&mut trx, table_id);
            trx.commit().await.unwrap();

            let mut trx = session.begin_trx().unwrap();
            let replaced = trx
                .exec(async |stmt| {
                    engine
                        .catalog()
                        .storage
                        .table_replay_silent_watermarks()
                        .upsert(
                            stmt,
                            &SlientWatermarkObject {
                                table_id,
                                heap_redo_start_ts: TrxID::new(6),
                                deletion_cutoff_ts: TrxID::new(11),
                            },
                        )
                        .await
                })
                .await
                .unwrap();
            assert!(replaced);
            mark_watermark_ddl(&mut trx, table_id);
            trx.commit().await.unwrap();

            let mut trx = session.begin_trx().unwrap();
            let noop = trx
                .exec(async |stmt| {
                    engine
                        .catalog()
                        .storage
                        .table_replay_silent_watermarks()
                        .upsert(
                            stmt,
                            &SlientWatermarkObject {
                                table_id,
                                heap_redo_start_ts: TrxID::new(7),
                                deletion_cutoff_ts: TrxID::new(10),
                            },
                        )
                        .await
                })
                .await
                .unwrap();
            assert!(!noop);
            mark_watermark_ddl(&mut trx, table_id);
            trx.commit().await.unwrap();

            let guards = session.pool_guards();
            let rows = engine
                .catalog()
                .storage
                .table_replay_silent_watermarks()
                .list_uncommitted(&guards)
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(
                rows[0],
                SlientWatermarkObject {
                    table_id,
                    heap_redo_start_ts: TrxID::new(7),
                    deletion_cutoff_ts: TrxID::new(11),
                }
            );
        });
    }
}
