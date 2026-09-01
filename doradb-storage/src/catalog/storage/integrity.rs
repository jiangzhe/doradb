use super::CatalogStorage;
use super::auxiliary::{
    PK_NO_TABLE_DESCRIPTORS, TABLE_ID_NO_TABLE_BINDINGS, TABLE_ID_TABLE_BINDINGS,
    TABLE_ID_TABLE_DESCRIPTORS,
};
use super::columns::TABLE_ID_COLUMNS;
use super::indexes::TABLE_ID_INDEXES;
use super::table_replay_silent_watermarks::TABLE_ID_TABLE_REPLAY_SILENT_WATERMARKS;
use super::tables::TABLE_ID_TABLES;
use crate::buffer::{PoolGuard, PoolGuards};
use crate::catalog::CatalogIndexNo;
use crate::error::{DataIntegrityError, DataIntegrityResult, RuntimeError, RuntimeResult};
use crate::file::multi_table_file::{CATALOG_TABLE_ROOT_DESC_COUNT, CatalogTableRootDesc};
use crate::id::TableID;
use crate::map::FastHashSet;
use crate::row::RowRead;
use crate::table::IndexLookupCriteria;
use crate::trx::PrivateTransaction;
use crate::value::Val;
use error_stack::{Report, ResultExt};

const PRIMARY_INDEX: CatalogIndexNo = CatalogIndexNo::new(0);

#[derive(Clone, Copy)]
struct CatalogSatelliteSpec {
    table_id: TableID,
    name: &'static str,
    parent_column: usize,
}

const CATALOG_SATELLITES: [CatalogSatelliteSpec; 5] = [
    CatalogSatelliteSpec {
        table_id: TABLE_ID_COLUMNS,
        name: "catalog.columns",
        parent_column: 0,
    },
    CatalogSatelliteSpec {
        table_id: TABLE_ID_INDEXES,
        name: "catalog.indexes",
        parent_column: 0,
    },
    CatalogSatelliteSpec {
        table_id: TABLE_ID_TABLE_DESCRIPTORS,
        name: "catalog.table_descriptors",
        parent_column: 0,
    },
    CatalogSatelliteSpec {
        table_id: TABLE_ID_TABLE_REPLAY_SILENT_WATERMARKS,
        name: "catalog.table_replay_silent_watermarks",
        parent_column: 0,
    },
    CatalogSatelliteSpec {
        table_id: TABLE_ID_TABLE_BINDINGS,
        name: "catalog.table_bindings",
        parent_column: 2,
    },
];

enum CatalogParentView<'a> {
    Live {
        guards: &'a PoolGuards,
    },
    Projected {
        roots: &'a [CatalogTableRootDesc; CATALOG_TABLE_ROOT_DESC_COUNT],
        disk_guard: &'a PoolGuard,
    },
}

impl CatalogStorage {
    /// Validates the complete post-replay in-memory catalog parent relation.
    pub(crate) async fn validate_live_catalog_parent_integrity(
        &self,
        guards: &PoolGuards,
    ) -> RuntimeResult<()> {
        self.validate_catalog_parent_integrity(CatalogParentView::Live { guards })
            .await
    }

    /// Validates the complete not-yet-published catalog root set.
    pub(super) async fn validate_projected_catalog_parent_integrity(
        &self,
        roots: &[CatalogTableRootDesc; CATALOG_TABLE_ROOT_DESC_COUNT],
        disk_guard: &PoolGuard,
    ) -> RuntimeResult<()> {
        self.validate_catalog_parent_integrity(CatalogParentView::Projected { roots, disk_guard })
            .await
    }

    async fn validate_catalog_parent_integrity(
        &self,
        view: CatalogParentView<'_>,
    ) -> RuntimeResult<()> {
        let mut parents = FastHashSet::default();
        match &view {
            CatalogParentView::Live { guards } => {
                self.visit_live_catalog_parent_column(
                    guards,
                    TABLE_ID_TABLES,
                    0,
                    "live",
                    |table_id| {
                        parents.insert(table_id);
                        Ok(())
                    },
                )
                .await?;
            }
            CatalogParentView::Projected { roots, disk_guard } => {
                let root = roots[0];
                self.visit_projected_catalog_column(root, TABLE_ID_TABLES, 0, disk_guard, |val| {
                    let table_id = decode_table_id(val, "catalog.tables", "projected")?;
                    parents.insert(table_id);
                    Ok(())
                })
                .await?;
            }
        }

        for spec in CATALOG_SATELLITES {
            match &view {
                CatalogParentView::Live { guards } => {
                    self.visit_live_catalog_parent_column(
                        guards,
                        spec.table_id,
                        spec.parent_column,
                        "live",
                        |table_id| require_parent(&parents, spec, table_id, "live"),
                    )
                    .await?;
                }
                CatalogParentView::Projected { roots, disk_guard } => {
                    let slot = crate::catalog::catalog_table_slot(spec.table_id)
                        .expect("satellite inventory uses catalog table ids");
                    self.visit_projected_catalog_column(
                        roots[slot],
                        spec.table_id,
                        spec.parent_column,
                        disk_guard,
                        |val| {
                            let table_id = decode_table_id(val, spec.name, "projected")?;
                            require_parent(&parents, spec, table_id, "projected")
                        },
                    )
                    .await?;
                }
            }
        }
        Ok(())
    }

    async fn visit_live_catalog_parent_column<F>(
        &self,
        guards: &PoolGuards,
        table_id: TableID,
        parent_column: usize,
        view: &'static str,
        mut visitor: F,
    ) -> RuntimeResult<()>
    where
        F: FnMut(TableID) -> DataIntegrityResult<()>,
    {
        let table = self.get_catalog_table(table_id).ok_or_else(|| {
            Report::new(DataIntegrityError::InvalidRootInvariant)
                .attach(format!(
                    "catalog parent inventory table is unavailable: view={view}, table_id={table_id}"
                ))
                .change_context(RuntimeError::CatalogAccess)
        })?;
        let mut visit_error = None;
        table
            .table_scan_uncommitted(guards, |layout, row| {
                if row.is_deleted() {
                    return true;
                }
                let result =
                    decode_table_id(row.val(layout, parent_column), catalog_name(table_id), view)
                        .and_then(&mut visitor);
                if let Err(err) = result {
                    visit_error = Some(err);
                    return false;
                }
                true
            })
            .await
            .change_context(RuntimeError::CatalogAccess)
            .attach_with(|| {
                format!(
                    "operation=validate_catalog_parent_integrity, view={view}, table_id={table_id}"
                )
            })?;
        if let Some(err) = visit_error {
            return Err(err
                .change_context(RuntimeError::CatalogAccess)
                .attach("operation=validate_catalog_parent_integrity"));
        }
        Ok(())
    }

    /// Requires an existing central row for a known satellite in the same
    /// locked current transaction view.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Phase 7 binding resolution consumes this checked parent helper"
        )
    )]
    pub(crate) async fn require_catalog_parent(
        &self,
        trx: &PrivateTransaction,
        satellite_table_id: TableID,
        table_id: TableID,
    ) -> RuntimeResult<()> {
        let Some(spec) = CATALOG_SATELLITES
            .iter()
            .copied()
            .find(|spec| spec.table_id == satellite_table_id)
        else {
            return Err(Report::new(DataIntegrityError::InvalidRootInvariant)
                .attach(format!(
                    "unknown catalog satellite: view=locked_current, satellite_table_id={satellite_table_id}, table_id={table_id}"
                ))
                .change_context(RuntimeError::CatalogAccess));
        };
        let key = [Val::from(table_id)];
        let mut found = false;
        self.tables[0]
            .index_lookup_current_locked(
                trx,
                PRIMARY_INDEX,
                IndexLookupCriteria::UniqueExact(&key),
                |_, _| {
                    found = true;
                    false
                },
            )
            .await?;
        if found {
            return Ok(());
        }
        Err(orphan_error(spec, table_id, "locked_current")
            .change_context(RuntimeError::CatalogAccess))
    }

    /// Proves that DROP's staged locked-current view contains no row owned by
    /// the target table in any catalog relation.
    pub(super) async fn validate_drop_table_absence(
        &self,
        trx: &PrivateTransaction,
        table_id: TableID,
    ) -> RuntimeResult<()> {
        let exact = [Val::from(table_id)];
        self.require_lookup_empty(
            trx,
            TABLE_ID_TABLES,
            PRIMARY_INDEX,
            IndexLookupCriteria::UniqueExact(&exact),
            "catalog.tables",
            table_id,
        )
        .await?;

        let lower = [Val::from(table_id), Val::from(0u32)];
        let upper = [Val::from(table_id), Val::from(u32::MAX)];
        for (satellite_table_id, name) in [
            (TABLE_ID_COLUMNS, "catalog.columns"),
            (TABLE_ID_INDEXES, "catalog.indexes"),
        ] {
            self.require_lookup_empty(
                trx,
                satellite_table_id,
                PRIMARY_INDEX,
                IndexLookupCriteria::UniqueInclusive {
                    lower: &lower,
                    upper: &upper,
                },
                name,
                table_id,
            )
            .await?;
        }
        self.require_lookup_empty(
            trx,
            TABLE_ID_TABLE_DESCRIPTORS,
            PK_NO_TABLE_DESCRIPTORS,
            IndexLookupCriteria::UniqueExact(&exact),
            "catalog.table_descriptors",
            table_id,
        )
        .await?;
        self.require_lookup_empty(
            trx,
            TABLE_ID_TABLE_REPLAY_SILENT_WATERMARKS,
            PRIMARY_INDEX,
            IndexLookupCriteria::UniqueExact(&exact),
            "catalog.table_replay_silent_watermarks",
            table_id,
        )
        .await?;
        self.require_lookup_empty(
            trx,
            TABLE_ID_TABLE_BINDINGS,
            TABLE_ID_NO_TABLE_BINDINGS,
            IndexLookupCriteria::NonUniqueExact(&exact),
            "catalog.table_bindings",
            table_id,
        )
        .await
    }

    async fn require_lookup_empty(
        &self,
        trx: &PrivateTransaction,
        catalog_table_id: TableID,
        index_slot: CatalogIndexNo,
        criteria: IndexLookupCriteria<'_>,
        name: &'static str,
        table_id: TableID,
    ) -> RuntimeResult<()> {
        let slot = crate::catalog::catalog_table_slot(catalog_table_id).ok_or_else(|| {
            Report::new(DataIntegrityError::InvalidRootInvariant)
                .attach(format!(
                    "DROP absence inventory is invalid: table_id={catalog_table_id}"
                ))
                .change_context(RuntimeError::CatalogAccess)
        })?;
        let mut found = false;
        self.tables[slot]
            .index_lookup_current_locked(trx, index_slot, criteria, |_, _| {
                found = true;
                false
            })
            .await?;
        if found {
            return Err(Report::new(DataIntegrityError::InvalidRootInvariant)
                .attach(format!(
                    "DROP staged catalog row survived: view=locked_current, satellite={name}, satellite_table_id={catalog_table_id}, table_id={table_id}"
                ))
                .change_context(RuntimeError::CatalogAccess));
        }
        Ok(())
    }
}

fn require_parent(
    parents: &FastHashSet<TableID>,
    spec: CatalogSatelliteSpec,
    table_id: TableID,
    view: &'static str,
) -> DataIntegrityResult<()> {
    if parents.contains(&table_id) {
        return Ok(());
    }
    Err(orphan_error(spec, table_id, view))
}

fn orphan_error(
    spec: CatalogSatelliteSpec,
    table_id: TableID,
    view: &'static str,
) -> Report<DataIntegrityError> {
    Report::new(DataIntegrityError::InvalidRootInvariant).attach(format!(
        "orphan catalog satellite row: view={view}, satellite={}, satellite_table_id={}, table_id={table_id}",
        spec.name, spec.table_id
    ))
}

fn decode_table_id(
    val: Val,
    table_name: &'static str,
    view: &'static str,
) -> DataIntegrityResult<TableID> {
    val.as_u64().map(TableID::new).ok_or_else(|| {
        Report::new(DataIntegrityError::InvalidRootInvariant).attach(format!(
            "catalog parent table_id has wrong type: view={view}, table={table_name}"
        ))
    })
}

fn catalog_name(table_id: TableID) -> &'static str {
    if table_id == TABLE_ID_TABLES {
        return "catalog.tables";
    }
    CATALOG_SATELLITES
        .iter()
        .find(|spec| spec.table_id == table_id)
        .map_or("unknown", |spec| spec.name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::storage::tests::begin_catalog_test_trx;
    use crate::catalog::tests::open_catalog_test_engine;
    use crate::error::DataIntegrityError;
    use crate::session::tests::{SessionTestExt, begin_test_mandatory_private_trx};
    use tempfile::TempDir;

    fn satellite_row(table_id: TableID, parent: TableID) -> Vec<Val> {
        if table_id == TABLE_ID_COLUMNS {
            return vec![
                Val::from(parent),
                Val::from(0u32),
                Val::from(0u16),
                Val::from(crate::value::ValKind::U64 as u32),
                Val::from(0u32),
            ];
        }
        if table_id == TABLE_ID_INDEXES {
            return vec![
                Val::from(parent),
                Val::from(0u32),
                Val::from(0u16),
                Val::from(0u32),
                Val::from(vec![1, 1, 0, 0, 0, 0, 0, 0]),
            ];
        }
        if table_id == TABLE_ID_TABLE_DESCRIPTORS {
            return vec![
                Val::from(parent),
                Val::from(0u64),
                Val::from(0u64),
                Val::from(vec![0; 32]),
                Val::from(Vec::<u8>::new()),
            ];
        }
        if table_id == TABLE_ID_TABLE_REPLAY_SILENT_WATERMARKS {
            return vec![Val::from(parent), Val::from(1u64), Val::from(1u64)];
        }
        assert_eq!(table_id, TABLE_ID_TABLE_BINDINGS);
        vec![
            Val::from(0u64),
            Val::from(vec![1u8]),
            Val::from(parent),
            Val::from(0u8),
        ]
    }

    fn central_row(table_id: TableID) -> Vec<Val> {
        vec![
            Val::from(table_id),
            Val::from(0u64),
            Val::from(0u64),
            Val::from(0u64),
            Val::from(0u32),
        ]
    }

    #[test]
    fn test_live_parent_validation_rejects_each_satellite_orphan() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = open_catalog_test_engine(temp_dir.path().to_path_buf(), None).await;
            let session = engine.new_session().unwrap();
            let storage = &engine.inner().core.catalog().storage;
            let orphan = TableID::new(42);

            for spec in CATALOG_SATELLITES {
                let mut transaction = begin_catalog_test_trx(&session);
                let slot = crate::catalog::catalog_table_slot(spec.table_id).unwrap();
                transaction
                    .trx()
                    .catalog_insert_mvcc(
                        &storage.tables[slot],
                        satellite_row(spec.table_id, orphan),
                    )
                    .await
                    .unwrap();
                let err = storage
                    .validate_live_catalog_parent_integrity(&session.pool_guards())
                    .await
                    .unwrap_err();
                let report = format!("{err:?}");
                assert_eq!(
                    err.downcast_ref::<DataIntegrityError>().copied(),
                    Some(DataIntegrityError::InvalidRootInvariant)
                );
                assert!(report.contains(spec.name), "{report}");
                assert!(report.contains("table_id=42"), "{report}");
                transaction.rollback().await;
            }
        });
    }

    #[test]
    fn test_live_parent_validation_accepts_complete_view_and_checked_parent() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = open_catalog_test_engine(temp_dir.path().to_path_buf(), None).await;
            let session = engine.new_session().unwrap();
            let storage = &engine.inner().core.catalog().storage;
            let parent = TableID::new(43);
            let mut transaction = begin_catalog_test_trx(&session);
            transaction
                .trx()
                .catalog_insert_mvcc(&storage.tables[0], central_row(parent))
                .await
                .unwrap();
            for spec in CATALOG_SATELLITES {
                let slot = crate::catalog::catalog_table_slot(spec.table_id).unwrap();
                transaction
                    .trx()
                    .catalog_insert_mvcc(
                        &storage.tables[slot],
                        satellite_row(spec.table_id, parent),
                    )
                    .await
                    .unwrap();
            }
            storage
                .validate_live_catalog_parent_integrity(&session.pool_guards())
                .await
                .unwrap();
            storage
                .require_catalog_parent(transaction.trx(), TABLE_ID_TABLE_BINDINGS, parent)
                .await
                .unwrap();
            let err = storage
                .require_catalog_parent(
                    transaction.trx(),
                    TABLE_ID_TABLE_BINDINGS,
                    TableID::new(99),
                )
                .await
                .unwrap_err();
            assert_eq!(
                err.downcast_ref::<DataIntegrityError>().copied(),
                Some(DataIntegrityError::InvalidRootInvariant)
            );
            transaction.rollback().await;
        });
    }

    #[test]
    fn test_locked_current_lookup_rejects_missing_lock_authority() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = open_catalog_test_engine(temp_dir.path().to_path_buf(), None).await;
            let session = engine.new_session().unwrap();
            let storage = &engine.inner().core.catalog().storage;
            let (mut operation, trx) = begin_test_mandatory_private_trx(&session);
            let err = storage
                .require_catalog_parent(&trx, TABLE_ID_COLUMNS, TableID::new(1))
                .await
                .unwrap_err();
            assert_eq!(
                err.downcast_ref::<DataIntegrityError>().copied(),
                Some(DataIntegrityError::InvalidRootInvariant)
            );
            assert!(format!("{err:?}").contains("lacks metadata-S or data-IX authority"));
            trx.rollback_catalog_ddl().await.unwrap();
            operation.assert_finish_ready();
            operation.finish();
        });
    }

    #[test]
    fn test_drop_absence_detects_binding_by_reverse_table_id_index() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = open_catalog_test_engine(temp_dir.path().to_path_buf(), None).await;
            let session = engine.new_session().unwrap();
            let storage = &engine.inner().core.catalog().storage;
            let table_id = TableID::new(44);
            let mut transaction = begin_catalog_test_trx(&session);
            let binding_slot = crate::catalog::catalog_table_slot(TABLE_ID_TABLE_BINDINGS).unwrap();
            transaction
                .trx()
                .catalog_insert_mvcc(
                    &storage.tables[binding_slot],
                    satellite_row(TABLE_ID_TABLE_BINDINGS, table_id),
                )
                .await
                .unwrap();

            let err = storage
                .validate_drop_table_absence(transaction.trx(), table_id)
                .await
                .unwrap_err();
            let report = format!("{err:?}");
            assert_eq!(
                err.downcast_ref::<DataIntegrityError>().copied(),
                Some(DataIntegrityError::InvalidRootInvariant)
            );
            assert!(report.contains("catalog.table_bindings"), "{report}");
            assert!(report.contains("table_id=44"), "{report}");
            transaction.rollback().await;
        });
    }
}
