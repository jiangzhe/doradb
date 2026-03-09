// mod index;
pub mod runtime;
pub mod spec;
pub mod storage;
pub mod table;

pub use runtime::*;
pub use spec::*;
pub use storage::*;
pub use table::*;

use crate::buffer::guard::PageSharedGuard;
use crate::buffer::page::{PageID, VersionedPageID};
use crate::buffer::{BufferPool, EvictableBufferPool, FixedBufferPool, GlobalReadonlyBufferPool};
use crate::error::{Error, Result};
use crate::file::table_fs::TableFileSystem;
use crate::index::BlockIndex;
use crate::index::SecondaryIndex;
use crate::latch::LatchFallbackMode;
use crate::lifetime::StaticLifetime;
use crate::row::ops::SelectKey;
use crate::row::{RowID, RowPage};
use crate::table::{ColumnDeletionBuffer, Table, TableAccess};
use crate::trx::sys::{CatalogCheckpointBatch, TransactionSystem};
use dashmap::DashMap;
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

pub const ROW_ID_COL_NAME: &str = "__row_id";

pub type ObjID = u64;
pub type TableID = ObjID;
pub type ColumnID = ObjID;
pub type IndexID = ObjID;
pub const USER_OBJ_ID_START: ObjID = 0x0001_0000_0000_0000;

/// Return whether an object id belongs to user-managed catalog space.
#[inline]
pub const fn is_user_obj_id(obj_id: ObjID) -> bool {
    obj_id >= USER_OBJ_ID_START
}

/// Return whether an object id belongs to built-in catalog table space.
#[inline]
pub const fn is_catalog_obj_id(obj_id: ObjID) -> bool {
    !is_user_obj_id(obj_id)
}

/// Catalog contains metadata of user tables.
pub struct Catalog {
    next_user_obj_id: AtomicU64,
    user_tables: DashMap<TableID, Table>,
    pub storage: CatalogStorage,
}

impl Catalog {
    /// Create a catalog runtime from persisted catalog storage.
    #[inline]
    pub fn new(storage: CatalogStorage) -> Self {
        let next_user_obj_id = storage.next_user_obj_id();
        Catalog {
            next_user_obj_id: AtomicU64::new(next_user_obj_id),
            user_tables: DashMap::new(),
            storage,
        }
    }

    /// Allocate and return the next user object id.
    #[inline]
    pub fn next_user_obj_id(&self) -> ObjID {
        self.next_user_obj_id.fetch_add(1, Ordering::SeqCst)
    }

    #[inline]
    fn try_update_next_user_obj_id(&self, next_user_obj_id: ObjID) {
        self.next_user_obj_id
            .fetch_max(next_user_obj_id, Ordering::SeqCst);
    }

    /// Return the current next user object id without allocating one.
    #[inline]
    pub fn curr_next_user_obj_id(&self) -> ObjID {
        self.next_user_obj_id.load(Ordering::Acquire)
    }

    /// Trigger one ad-hoc catalog checkpoint publish.
    ///
    /// # Panics
    ///
    /// Panics if another checkpoint is already in progress on the same
    /// `CatalogStorage`/`MultiTableFile`. Concurrent checkpoint publishes are
    /// not supported by design; the underlying [`CowFile`] enforces a single
    /// mutable writer via an atomic claim and will panic on violation.
    #[inline]
    pub async fn checkpoint_now(&self, trx_sys: &TransactionSystem) -> Result<()> {
        let batch = self.scan_checkpoint_batch(trx_sys)?;
        self.apply_checkpoint_batch(batch).await
    }

    /// Scan persisted redo logs and collect one safe catalog checkpoint batch.
    ///
    /// This call satisfies `scan_catalog_checkpoint_batch`'s precondition by
    /// using the global persisted watermark across all log partitions.
    ///
    /// Scanned batches are intended for single-flight publish flow and must not
    /// be raced with other catalog checkpoint publishes on the same `Catalog`.
    pub fn scan_checkpoint_batch(
        &self,
        trx_sys: &TransactionSystem,
    ) -> Result<CatalogCheckpointBatch> {
        let snapshot = self.storage.checkpoint_snapshot()?;
        let catalog_replay_start_ts = snapshot.catalog_replay_start_ts;
        let durable_upper_cts = trx_sys.persisted_watermark_cts();
        trx_sys.scan_catalog_checkpoint_batch(
            catalog_replay_start_ts,
            durable_upper_cts,
            |table_id| self.loaded_table_heap_redo_start_ts(table_id),
        )
    }

    /// Apply one scanned catalog checkpoint batch into `catalog.mtb`.
    ///
    /// # Panics
    ///
    /// Panics if another mutable writer is already active on `catalog.mtb`.
    /// Only one checkpoint publish may be in flight at a time per `Catalog`
    /// instance; callers are responsible for ensuring mutual exclusion at a
    /// higher level (e.g., a single background checkpoint task).
    #[inline]
    pub async fn apply_checkpoint_batch(&self, batch: CatalogCheckpointBatch) -> Result<()> {
        self.storage
            .apply_checkpoint_batch(batch, self.curr_next_user_obj_id())
            .await
    }

    /// Returns whether a table is user table.
    #[inline]
    pub fn is_user_table(&self, table_id: TableID) -> bool {
        is_user_obj_id(table_id)
    }

    /// Enable page committer for tables, excluding catalog tables.
    /// No page creation should be persisted in redo log for catalog tables.
    #[inline]
    pub async fn enable_page_committer_for_tables(&self, trx_sys: &'static TransactionSystem) {
        for entry in &self.user_tables {
            entry.value().blk_idx.enable_page_committer(trx_sys);
        }
    }

    /// Reload one user table runtime from catalog metadata and table file.
    pub async fn reload_create_table(
        &self,
        mem_pool: &'static EvictableBufferPool,
        index_pool: &'static FixedBufferPool,
        table_fs: &'static TableFileSystem,
        global_disk_pool: &'static GlobalReadonlyBufferPool,
        table_id: TableID,
    ) -> Result<()> {
        if self.user_tables.contains_key(&table_id) {
            return Err(Error::TableAlreadyExists);
        }
        let res = self.storage.tables().find_uncommitted_by_id(table_id).await;
        match res {
            Some(table) => {
                // Phase 2 allocator semantics: only table ids consume global user object ids.
                self.try_update_next_user_obj_id(
                    table.table_id.saturating_add(1).max(USER_OBJ_ID_START),
                );

                // todo: use secondary index to improve performance
                let mut columns = self
                    .storage
                    .columns()
                    .list_uncommitted_by_table_id(table_id)
                    .await;
                debug_assert!(!columns.is_empty());
                columns.sort_by_key(|c| c.column_no);

                let column_specs = columns
                    .into_iter()
                    .map(|c| ColumnSpec::new(&c.column_name, c.column_type, c.column_attributes))
                    .collect::<Vec<_>>();

                let mut indexes = self
                    .storage
                    .indexes()
                    .list_uncommitted_by_table_id(table_id)
                    .await;
                indexes.sort_by_key(|index| index.index_no);

                let mut index_columns = self
                    .storage
                    .index_columns()
                    .list_uncommitted_by_table_id(table_id)
                    .await;
                index_columns.sort_by_key(|ic| (ic.index_no, ic.index_column_no));
                let mut index_columns_by_index_no: BTreeMap<u16, Vec<IndexColumnObject>> =
                    BTreeMap::new();
                for index_column in index_columns {
                    index_columns_by_index_no
                        .entry(index_column.index_no)
                        .or_default()
                        .push(index_column);
                }

                let mut index_specs = vec![];
                for index in indexes {
                    let mut index_cols = vec![];
                    for index_column in index_columns_by_index_no
                        .remove(&index.index_no)
                        .unwrap_or_default()
                    {
                        let ik = IndexKey {
                            col_no: index_column.column_no,
                            order: index_column.index_order,
                        };
                        index_cols.push(ik);
                    }
                    index_specs.push(IndexSpec::new(
                        &index.index_name,
                        index_cols,
                        index.index_attributes,
                    ));
                }
                let table_file = table_fs.open_table_file(table.table_id).await?;
                let active_root = table_file.active_root();
                let metadata_in_catalog = TableMetadata::new(column_specs, index_specs);
                let metadata_in_file = &*active_root.metadata;
                if &metadata_in_catalog != metadata_in_file {
                    return Err(Error::InvalidState);
                }
                let row_id_bound = active_root.pivot_row_id;

                let blk_idx = BlockIndex::new(
                    self.storage.meta_pool,
                    table.table_id,
                    row_id_bound,
                    active_root.column_block_index_root,
                    Arc::clone(&table_file),
                    global_disk_pool,
                )
                .await;
                let table =
                    Table::new(mem_pool, index_pool, global_disk_pool, blk_idx, table_file).await;
                let old = self.user_tables.insert(table_id, table);
                if old.is_some() {
                    return Err(Error::TableAlreadyExists);
                }
                Ok(())
            }
            None => Err(Error::TableNotFound),
        }
    }

    /// Get a user-table runtime handle by table id.
    #[inline]
    pub async fn get_table(&self, table_id: TableID) -> Option<Table> {
        if is_catalog_obj_id(table_id) {
            return None;
        }
        self.user_tables
            .get(&table_id)
            .map(|table| table.value().clone())
    }

    /// Get a catalog-table runtime handle by table id.
    #[inline]
    pub fn get_catalog_table(&self, table_id: TableID) -> Option<CatalogTable> {
        self.storage.get_catalog_table(table_id)
    }

    /// Insert a user table runtime into the in-memory cache.
    #[inline]
    pub fn insert_user_table(&self, table: Table) {
        let table_id = table.table_id();
        let old = self.user_tables.insert(table_id, table);
        debug_assert!(old.is_none());
    }

    /// Remove a user table runtime from the in-memory cache.
    #[inline]
    pub fn remove_user_table(&self, table_id: TableID) -> Option<Table> {
        self.user_tables.remove(&table_id).map(|(_, table)| table)
    }

    /// Return the metadata buffer pool used by catalog/index metadata pages.
    #[inline]
    pub fn meta_pool(&self) -> &'static FixedBufferPool {
        self.storage.meta_pool
    }

    #[inline]
    fn loaded_table_heap_redo_start_ts(&self, table_id: TableID) -> Option<u64> {
        self.user_tables
            .get(&table_id)
            .map(|table| table.file.active_root().heap_redo_start_ts)
    }
}

unsafe impl Send for Catalog {}
unsafe impl Sync for Catalog {}
unsafe impl StaticLifetime for Catalog {}

/// Unified runtime handle for either user table or catalog table.
pub enum TableHandle {
    User(Table),
    Catalog(CatalogTable),
}

impl TableHandle {
    #[inline]
    pub fn blk_idx(&self) -> &BlockIndex {
        match self {
            TableHandle::User(table) => &table.blk_idx,
            TableHandle::Catalog(table) => &table.blk_idx,
        }
    }

    #[inline]
    pub fn sec_idx(&self) -> &[SecondaryIndex] {
        match self {
            TableHandle::User(table) => &table.sec_idx,
            TableHandle::Catalog(table) => &table.sec_idx,
        }
    }

    #[inline]
    pub fn pivot_row_id(&self) -> RowID {
        self.blk_idx().pivot_row_id()
    }

    #[inline]
    pub fn deletion_buffer(&self) -> Option<&ColumnDeletionBuffer> {
        match self {
            TableHandle::User(table) => Some(table.deletion_buffer()),
            TableHandle::Catalog(_) => None,
        }
    }

    #[inline]
    pub async fn try_get_row_page_versioned_shared(
        &self,
        page_id: VersionedPageID,
    ) -> Option<PageSharedGuard<RowPage>> {
        match self {
            TableHandle::User(table) => {
                let page_guard = table
                    .mem_pool
                    .try_get_page_versioned::<RowPage>(page_id, LatchFallbackMode::Shared)
                    .await?;
                page_guard.lock_shared_async().await
            }
            TableHandle::Catalog(table) => {
                let page_guard = table
                    .mem_pool
                    .try_get_page_versioned::<RowPage>(page_id, LatchFallbackMode::Shared)
                    .await?;
                page_guard.lock_shared_async().await
            }
        }
    }

    #[inline]
    pub async fn get_row_page_shared(&self, page_id: PageID) -> Option<PageSharedGuard<RowPage>> {
        match self {
            TableHandle::User(table) => {
                table
                    .mem_pool
                    .get_page::<RowPage>(page_id, LatchFallbackMode::Shared)
                    .await
                    .lock_shared_async()
                    .await
            }
            TableHandle::Catalog(table) => {
                table
                    .mem_pool
                    .get_page::<RowPage>(page_id, LatchFallbackMode::Shared)
                    .await
                    .lock_shared_async()
                    .await
            }
        }
    }

    #[inline]
    pub async fn delete_index(&self, key: &SelectKey, row_id: RowID, unique: bool) -> bool {
        match self {
            TableHandle::User(table) => table.accessor().delete_index(key, row_id, unique).await,
            TableHandle::Catalog(table) => table.accessor().delete_index(key, row_id, unique).await,
        }
    }
}

/// Per-operation table handle cache used by rollback/recovery paths.
pub struct TableCache<'a> {
    catalog: &'a Catalog,
    tables: HashMap<TableID, TableHandle>,
    missing: HashSet<TableID>,
}

impl<'a> TableCache<'a> {
    /// Create an empty table cache bound to one catalog instance.
    #[inline]
    pub fn new(catalog: &'a Catalog) -> Self {
        TableCache {
            catalog,
            tables: HashMap::new(),
            missing: HashSet::new(),
        }
    }

    /// Returns cached table handle for given id.
    ///
    /// If table is not cached, this method loads it from catalog and caches
    /// positive/negative lookup result.
    #[inline]
    pub async fn get_table_ref(&mut self, table_id: TableID) -> Option<&TableHandle> {
        match self.tables.entry(table_id) {
            Entry::Vacant(vac) => {
                if self.missing.contains(&table_id) {
                    return None;
                }
                let loaded = if is_catalog_obj_id(table_id) {
                    self.catalog
                        .get_catalog_table(table_id)
                        .map(TableHandle::Catalog)
                } else {
                    self.catalog
                        .get_table(table_id)
                        .await
                        .map(TableHandle::User)
                };
                match loaded {
                    Some(table) => {
                        let res = vac.insert(table);
                        Some(&*res)
                    }
                    None => {
                        let _ = self.missing.insert(table_id);
                        None
                    }
                }
            }
            Entry::Occupied(occ) => {
                let res = occ.into_mut();
                Some(&*res)
            }
        }
    }

    /// Returns cached table handle and requires table to exist.
    ///
    /// This method is intended for rollback paths where table id in undo log
    /// must always map to an existing table.
    #[inline]
    pub async fn must_get_table(&mut self, table_id: TableID) -> &TableHandle {
        match self.get_table_ref(table_id).await {
            Some(table) => table,
            None => panic!("table {table_id} not found in catalog"),
        }
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::catalog::{ColumnAttributes, IndexAttributes, IndexKey, IndexSpec, TableSpec};
    use crate::engine::{Engine, EngineConfig};
    use crate::trx::MIN_SNAPSHOT_TS;
    use crate::trx::sys::CatalogCheckpointScanStopReason;
    use crate::trx::sys_conf::TrxSysConfig;
    use crate::value::{Val, ValKind};
    use semistr::SemiStr;
    use tempfile::TempDir;

    /// Table1 has single i32 column, with unique index of this column.
    #[inline]
    pub(crate) async fn table1(engine: &Engine) -> TableID {
        let mut session = engine.new_session();
        let table_id = session
            .create_table(
                TableSpec {
                    columns: vec![ColumnSpec {
                        column_name: SemiStr::new("id"),
                        column_type: ValKind::I32,
                        column_attributes: ColumnAttributes::empty(),
                    }],
                },
                vec![IndexSpec::new(
                    "idx_table1_id",
                    vec![IndexKey::new(0)],
                    IndexAttributes::PK,
                )],
            )
            .await
            .unwrap();

        drop(session);
        table_id
    }

    /// Table2 has i32(unique key) and string column.
    #[inline]
    pub(crate) async fn table2(engine: &Engine) -> TableID {
        let mut session = engine.new_session();
        let table_id = session
            .create_table(
                TableSpec {
                    columns: vec![
                        ColumnSpec {
                            column_name: SemiStr::new("id"),
                            column_type: ValKind::I32,
                            column_attributes: ColumnAttributes::empty(),
                        },
                        ColumnSpec {
                            column_name: SemiStr::new("name"),
                            column_type: ValKind::VarByte,
                            column_attributes: ColumnAttributes::empty(),
                        },
                    ],
                },
                vec![IndexSpec::new(
                    "idx_table2_id",
                    vec![IndexKey::new(0)],
                    IndexAttributes::PK,
                )],
            )
            .await
            .unwrap();

        drop(session);
        table_id
    }

    /// Table3 has single string key column.
    #[inline]
    pub(crate) async fn table3(engine: &Engine) -> TableID {
        let mut session = engine.new_session();

        let table_id = session
            .create_table(
                TableSpec {
                    columns: vec![ColumnSpec {
                        column_name: SemiStr::new("name"),
                        column_type: ValKind::VarByte,
                        column_attributes: ColumnAttributes::empty(),
                    }],
                },
                vec![IndexSpec::new(
                    "idx_table3_name",
                    vec![IndexKey::new(0)],
                    IndexAttributes::PK,
                )],
            )
            .await
            .unwrap();

        drop(session);
        table_id
    }

    /// Table4 has two i32 columns.
    /// First is unique index.
    /// Second is non-unique index.
    #[inline]
    pub(crate) async fn table4(engine: &Engine) -> TableID {
        let mut session = engine.new_session();

        let table_id = session
            .create_table(
                TableSpec {
                    columns: vec![
                        ColumnSpec {
                            column_name: SemiStr::new("id"),
                            column_type: ValKind::I32,
                            column_attributes: ColumnAttributes::empty(),
                        },
                        ColumnSpec {
                            column_name: SemiStr::new("val"),
                            column_type: ValKind::I32,
                            column_attributes: ColumnAttributes::empty(),
                        },
                    ],
                },
                vec![
                    IndexSpec::new(
                        "idx_table4_id",
                        vec![IndexKey::new(0)],
                        // unique index.
                        IndexAttributes::PK,
                    ),
                    IndexSpec::new(
                        "idx_table4_val",
                        vec![IndexKey::new(1)],
                        // non-unique index.
                        IndexAttributes::empty(),
                    ),
                ],
            )
            .await
            .unwrap();

        drop(session);
        table_id
    }

    #[test]
    fn test_catalog_user_obj_id_boundary_predicates() {
        assert!(is_catalog_obj_id(USER_OBJ_ID_START - 1));
        assert!(!is_catalog_obj_id(USER_OBJ_ID_START));
        assert!(!is_user_obj_id(USER_OBJ_ID_START - 1));
        assert!(is_user_obj_id(USER_OBJ_ID_START));
    }

    #[test]
    fn test_bootstrap_creates_catalog_mtb_without_catalog_tbl_files() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_string_lossy().to_string();
            let engine = EngineConfig::default()
                .main_dir(main_dir.clone())
                .trx(TrxSysConfig::default().skip_recovery(true))
                .build()
                .await
                .unwrap();
            drop(engine);

            let data_dir = temp_dir.path();
            assert!(data_dir.join("catalog.mtb").exists());
            for table_id in 0..4u64 {
                assert!(!data_dir.join(format!("{table_id}.tbl")).exists());
            }
        });
    }

    #[test]
    fn test_next_user_obj_id_monotonic_across_restart() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_string_lossy().to_string();

            let engine = EngineConfig::default()
                .main_dir(main_dir.clone())
                .trx(
                    TrxSysConfig::default()
                        .log_file_prefix("catalog-allocator")
                        .skip_recovery(false),
                )
                .build()
                .await
                .unwrap();
            assert_eq!(engine.catalog().curr_next_user_obj_id(), USER_OBJ_ID_START);
            let mut session = engine.new_session();
            let table_spec = TableSpec {
                columns: vec![
                    ColumnSpec {
                        column_name: SemiStr::new("id"),
                        column_type: ValKind::I32,
                        column_attributes: ColumnAttributes::empty(),
                    },
                    ColumnSpec {
                        column_name: SemiStr::new("k1"),
                        column_type: ValKind::I32,
                        column_attributes: ColumnAttributes::empty(),
                    },
                    ColumnSpec {
                        column_name: SemiStr::new("k2"),
                        column_type: ValKind::I32,
                        column_attributes: ColumnAttributes::empty(),
                    },
                ],
            };
            let index_specs = vec![
                IndexSpec::new(
                    "idx_allocator_pk",
                    vec![IndexKey::new(0)],
                    IndexAttributes::PK,
                ),
                IndexSpec::new(
                    "idx_allocator_k12",
                    vec![IndexKey::new(1), IndexKey::new(2)],
                    IndexAttributes::empty(),
                ),
            ];
            let table_id1 = session.create_table(table_spec, index_specs).await.unwrap();
            assert_eq!(engine.catalog().curr_next_user_obj_id(), table_id1 + 1);
            drop(session);
            drop(engine);

            let engine = EngineConfig::default()
                .main_dir(main_dir)
                .trx(
                    TrxSysConfig::default()
                        .log_file_prefix("catalog-allocator")
                        .skip_recovery(false),
                )
                .build()
                .await
                .unwrap();
            assert_eq!(engine.catalog().curr_next_user_obj_id(), table_id1 + 1);
            let table_id2 = table1(&engine).await;
            assert!(table_id1 >= USER_OBJ_ID_START);
            assert_eq!(table_id2, table_id1 + 1);
            drop(engine);
        });
    }

    #[test]
    fn test_catalog_checkpoint_now_publish_and_noop() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_string_lossy().to_string();

            let engine = EngineConfig::default()
                .main_dir(main_dir)
                .trx(
                    TrxSysConfig::default()
                        .log_file_prefix("catalog-checkpoint-now")
                        .skip_recovery(false),
                )
                .build()
                .await
                .unwrap();

            let snap0 = engine.catalog().storage.checkpoint_snapshot().unwrap();
            assert_eq!(snap0.catalog_replay_start_ts, MIN_SNAPSHOT_TS);
            assert!(
                snap0
                    .meta
                    .table_roots
                    .iter()
                    .all(|root| root.root_page_id.is_none() && root.pivot_row_id == 0)
            );

            let _ = table1(&engine).await;
            engine
                .catalog()
                .checkpoint_now(engine.trx_sys)
                .await
                .unwrap();
            let snap1 = engine.catalog().storage.checkpoint_snapshot().unwrap();
            assert!(snap1.catalog_replay_start_ts > MIN_SNAPSHOT_TS);
            assert_eq!(
                snap1.meta.next_user_obj_id,
                engine.catalog().curr_next_user_obj_id()
            );
            assert!(
                snap1
                    .meta
                    .table_roots
                    .iter()
                    .any(|root| root.root_page_id.is_some())
            );
            assert!(
                snap1
                    .meta
                    .table_roots
                    .iter()
                    .any(|root| root.pivot_row_id > 0)
            );

            engine
                .catalog()
                .checkpoint_now(engine.trx_sys)
                .await
                .unwrap();
            let snap2 = engine.catalog().storage.checkpoint_snapshot().unwrap();
            assert_eq!(snap2.catalog_replay_start_ts, snap1.catalog_replay_start_ts);
            assert_eq!(snap2.meta.table_roots, snap1.meta.table_roots);
        });
    }

    #[test]
    fn test_catalog_checkpoint_now_heartbeat_without_catalog_ops() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_string_lossy().to_string();

            let engine = EngineConfig::default()
                .main_dir(main_dir)
                .trx(
                    TrxSysConfig::default()
                        .log_file_prefix("catalog-checkpoint-heartbeat")
                        .skip_recovery(false),
                )
                .build()
                .await
                .unwrap();

            let table_id = table1(&engine).await;
            engine
                .catalog()
                .checkpoint_now(engine.trx_sys)
                .await
                .unwrap();
            let snap1 = engine.catalog().storage.checkpoint_snapshot().unwrap();
            assert!(snap1.catalog_replay_start_ts > MIN_SNAPSHOT_TS);
            let roots_before = snap1.meta.table_roots;

            let table = engine.catalog().get_table(table_id).await.unwrap();
            let mut session = engine.new_session();
            let mut stmt = session.begin_trx().unwrap().start_stmt();
            let res = stmt.insert_row(&table, vec![Val::I32(7)]).await;
            assert!(res.is_ok());
            stmt.succeed().commit().await.unwrap();

            engine
                .catalog()
                .checkpoint_now(engine.trx_sys)
                .await
                .unwrap();
            let snap2 = engine.catalog().storage.checkpoint_snapshot().unwrap();
            assert!(snap2.catalog_replay_start_ts > snap1.catalog_replay_start_ts);
            assert_eq!(snap2.meta.table_roots, roots_before);
            assert_eq!(snap2.meta.next_user_obj_id, snap1.meta.next_user_obj_id);
        });
    }

    #[test]
    fn test_catalog_checkpoint_scan_apply_full_range() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_string_lossy().to_string();

            let engine = EngineConfig::default()
                .main_dir(main_dir)
                .trx(
                    TrxSysConfig::default()
                        .log_file_prefix("catalog-checkpoint-batch-full-range")
                        .skip_recovery(false),
                )
                .build()
                .await
                .unwrap();

            let _ = table1(&engine).await;
            let _ = table2(&engine).await;

            let batch1 = engine
                .catalog()
                .scan_checkpoint_batch(engine.trx_sys)
                .unwrap();
            assert_eq!(batch1.catalog_ddl_txn_count, 2);
            assert_eq!(
                batch1.stop_reason,
                CatalogCheckpointScanStopReason::ReachedDurableUpper
            );
            let safe_cts_1 = batch1.safe_cts;
            engine
                .catalog()
                .apply_checkpoint_batch(batch1)
                .await
                .unwrap();
            let snap1 = engine.catalog().storage.checkpoint_snapshot().unwrap();
            assert_eq!(snap1.catalog_replay_start_ts, safe_cts_1 + 1);

            let batch2 = engine
                .catalog()
                .scan_checkpoint_batch(engine.trx_sys)
                .unwrap();
            assert_eq!(batch2.catalog_ddl_txn_count, 0);
            assert_eq!(batch2.safe_cts, safe_cts_1);
            engine
                .catalog()
                .apply_checkpoint_batch(batch2)
                .await
                .unwrap();
            let snap2 = engine.catalog().storage.checkpoint_snapshot().unwrap();
            assert_eq!(snap2.catalog_replay_start_ts, snap1.catalog_replay_start_ts);
        });
    }
}
