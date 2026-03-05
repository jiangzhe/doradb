// mod index;
pub mod spec;
pub mod storage;
pub mod table;

pub use spec::*;
pub use storage::*;
pub use table::*;

use crate::buffer::{FixedBufferPool, GlobalReadonlyBufferPool};
use crate::error::{Error, Result};
use crate::file::table_fs::TableFileSystem;
use crate::index::BlockIndex;
use crate::latch::RwLock;
use crate::lifetime::StaticLifetime;
use crate::table::Table;
use crate::trx::sys::TransactionSystem;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

pub const ROW_ID_COL_NAME: &str = "__row_id";

pub type ObjID = u64;
pub type TableID = ObjID;
pub type ColumnID = ObjID;
pub type IndexID = ObjID;
pub const USER_OBJ_ID_START: ObjID = 0x0001_0000_0000_0000;

#[inline]
pub const fn is_user_obj_id(obj_id: ObjID) -> bool {
    obj_id >= USER_OBJ_ID_START
}

#[inline]
pub const fn is_catalog_obj_id(obj_id: ObjID) -> bool {
    !is_user_obj_id(obj_id)
}

/// Catalog contains metadata of user tables.
pub struct Catalog {
    next_user_obj_id: AtomicU64,
    pub cache: CatalogCache,
    pub storage: CatalogStorage,
}

impl Catalog {
    #[inline]
    pub fn new(storage: CatalogStorage) -> Self {
        let mut cache = CatalogCache::new();
        let next_user_obj_id = storage.next_user_obj_id();
        for (table_id, table) in storage.all() {
            cache.tables.get_mut().insert(table_id, table);
        }
        Catalog {
            next_user_obj_id: AtomicU64::new(next_user_obj_id),
            cache,
            storage,
        }
    }

    #[inline]
    pub fn next_user_obj_id(&self) -> ObjID {
        self.next_user_obj_id.fetch_add(1, Ordering::SeqCst)
    }

    #[inline]
    fn try_update_next_user_obj_id(&self, next_user_obj_id: ObjID) {
        self.next_user_obj_id
            .fetch_max(next_user_obj_id, Ordering::SeqCst);
    }

    #[inline]
    pub fn curr_next_user_obj_id(&self) -> ObjID {
        self.next_user_obj_id.load(Ordering::Acquire)
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
        let tables_g = self.cache.tables.read().await;
        for table in tables_g.values() {
            if self.is_user_table(table.table_id()) {
                table.blk_idx.enable_page_committer(trx_sys);
            }
        }
    }

    pub async fn reload_create_table(
        &self,
        index_pool: &'static FixedBufferPool,
        table_fs: &'static TableFileSystem,
        global_disk_pool: &'static GlobalReadonlyBufferPool,
        table_id: TableID,
    ) -> Result<()> {
        // todo
        let res = self.storage.tables().find_uncommitted_by_id(table_id).await;
        match res {
            Some(table) => {
                // A table creation involves table, column and index.
                // Find the maximum existing object id and then move allocator to next id.
                let mut max_obj_id = table.table_id;

                // todo: use secondary index to improve performance
                let columns = self
                    .storage
                    .columns()
                    .list_uncommitted_by_table_id(table_id)
                    .await;
                debug_assert!(!columns.is_empty());
                if let Some(column_id) = columns.iter().map(|c| c.column_id).max() {
                    max_obj_id = max_obj_id.max(column_id);
                }

                let column_specs = columns
                    .into_iter()
                    .map(|c| ColumnSpec::new(&c.column_name, c.column_type, c.column_attributes))
                    .collect::<Vec<_>>();

                let indexes = self
                    .storage
                    .indexes()
                    .list_uncommitted_by_table_id(table_id)
                    .await;
                if let Some(index_id) = indexes.iter().map(|i| i.index_id).max() {
                    max_obj_id = max_obj_id.max(index_id);
                }
                self.try_update_next_user_obj_id(
                    max_obj_id.saturating_add(1).max(USER_OBJ_ID_START),
                );

                let mut index_specs = vec![];
                for index in indexes {
                    let mut index_columns = self
                        .storage
                        .index_columns()
                        .list_uncommitted_by_index_id(index.index_id)
                        .await;
                    index_columns.sort_by_key(|ic| ic.index_column_no);
                    let mut index_cols = vec![];
                    for index_column in index_columns {
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
                // todo: refine recovery of corrupted file.
                let metadata_in_catalog = TableMetadata::new(column_specs, index_specs);
                let metadata_in_file = &*active_root.metadata;
                debug_assert_eq!(&metadata_in_catalog, metadata_in_file);
                let row_id_bound = active_root.pivot_row_id;

                let blk_idx = BlockIndex::new(
                    self.storage.meta_pool,
                    table.table_id,
                    row_id_bound,
                    table_file.active_root().column_block_index_root,
                    Arc::clone(&table_file),
                    global_disk_pool,
                )
                .await;
                let table = Table::new(
                    self.storage.mem_pool,
                    index_pool,
                    global_disk_pool,
                    blk_idx,
                    table_file,
                )
                .await;
                // Update table into cache
                let mut table_cache_g = self.cache.tables.write().await;
                let res = table_cache_g.insert(table_id, table);
                assert!(res.is_none());
                Ok(())
            }
            None => Err(Error::TableNotFound),
        }
    }

    #[inline]
    pub async fn get_table(&self, table_id: TableID) -> Option<Table> {
        let g = self.cache.tables.read().await;
        g.get(&table_id).cloned()
    }

    #[inline]
    pub fn meta_pool(&self) -> &'static FixedBufferPool {
        self.storage.meta_pool
    }
}

unsafe impl Send for Catalog {}
unsafe impl Sync for Catalog {}
unsafe impl StaticLifetime for Catalog {}

pub struct CatalogCache {
    pub tables: RwLock<HashMap<TableID, Table>>,
}

impl CatalogCache {
    #[allow(clippy::new_without_default)]
    #[inline]
    pub fn new() -> Self {
        CatalogCache {
            tables: RwLock::new(HashMap::new()),
        }
    }
}

pub struct TableCache<'a> {
    catalog: &'a Catalog,
    tables: HashMap<TableID, Table>,
    missing: HashSet<TableID>,
}

impl<'a> TableCache<'a> {
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
    pub async fn get_table_ref(&mut self, table_id: TableID) -> Option<&Table> {
        match self.tables.entry(table_id) {
            Entry::Vacant(vac) => {
                if self.missing.contains(&table_id) {
                    return None;
                }
                match self.catalog.get_table(table_id).await {
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
    pub async fn must_get_table(&mut self, table_id: TableID) -> &Table {
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
    use crate::trx::sys_conf::TrxSysConfig;
    use crate::value::ValKind;
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
            let table_id1 = table1(&engine).await;
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
            let table_id2 = table1(&engine).await;
            assert!(table_id1 >= USER_OBJ_ID_START);
            assert!(table_id2 > table_id1);
            drop(engine);
        });
    }
}
