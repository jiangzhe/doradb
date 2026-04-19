mod checkpoint;
// mod index;
pub mod runtime;
pub mod spec;
pub mod storage;
pub mod table;

pub use checkpoint::*;
pub use runtime::*;
pub use spec::*;
pub use storage::*;
pub use table::*;

use crate::DiskPool;
use crate::buffer::guard::PageSharedGuard;
use crate::buffer::page::{PageID, VersionedPageID};
use crate::buffer::{
    BufferPool, EvictableBufferPool, FixedBufferPool, PoolGuards, PoolRole, ReadonlyBufferPool,
};
use crate::component::{Component, ComponentRegistry, MetaPool, ShelfScope};
use crate::error::{Error, Result};
use crate::file::fs::FileSystem;
use crate::index::{BlockIndex, RowLocation};
use crate::quiescent::{QuiescentBox, QuiescentGuard};
use crate::row::ops::SelectKey;
use crate::row::{RowID, RowPage};
use crate::table::{ColumnDeletionBuffer, IndexRollback, Table, TableAccess};
use crate::trx::TrxID;
use crate::trx::undo::IndexUndo;
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
    user_tables: DashMap<TableID, Arc<Table>>,
    pub storage: CatalogStorage,
}

impl Component for Catalog {
    type Config = ();
    type Owned = Self;
    type Access = QuiescentGuard<Self>;

    const NAME: &'static str = "catalog";

    #[inline]
    async fn build(
        _config: Self::Config,
        registry: &mut ComponentRegistry,
        _shelf: ShelfScope<'_, Self>,
    ) -> Result<()> {
        let meta_pool = registry.dependency::<MetaPool>()?;
        let table_fs = registry.dependency::<FileSystem>()?;
        let disk_pool = registry.dependency::<DiskPool>()?;
        let storage = CatalogStorage::new(
            meta_pool.clone_inner(),
            table_fs.clone(),
            disk_pool.clone_inner(),
        )
        .await?;
        registry.register::<Self>(Catalog::new(storage).await?)
    }

    #[inline]
    fn access(owner: &QuiescentBox<Self::Owned>) -> Self::Access {
        owner.guard()
    }

    #[inline]
    fn shutdown(_component: &Self::Owned) {}
}

impl Catalog {
    /// Create a catalog runtime from persisted catalog storage.
    #[inline]
    pub async fn new(storage: CatalogStorage) -> Result<Self> {
        let pool_guards = PoolGuards::builder()
            .push(PoolRole::Meta, storage.meta_pool.pool_guard())
            .push(PoolRole::Disk, storage.disk_pool.pool_guard())
            .build();
        let snapshot = storage.checkpoint_snapshot()?;
        storage
            .bootstrap_from_checkpoint(&snapshot, &pool_guards)
            .await?;
        let next_user_obj_id = storage.next_user_obj_id();
        Ok(Catalog {
            next_user_obj_id: AtomicU64::new(next_user_obj_id),
            user_tables: DashMap::new(),
            storage,
        })
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

    /// Apply one scanned catalog checkpoint batch into `catalog.mtb`.
    ///
    /// # Panics
    ///
    /// Panics if another mutable writer is already active on the shared
    /// `catalog.mtb` `MultiTableFile`. Only one checkpoint publish may be in
    /// flight at a time per shared `CatalogStorage`/`MultiTableFile`; callers
    /// are responsible for ensuring mutual exclusion at a higher level (e.g.,
    /// a single background checkpoint task).
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

    /// Reload one user table runtime from catalog metadata and table file.
    pub(crate) async fn reload_create_table(
        &self,
        mem_pool: QuiescentGuard<EvictableBufferPool>,
        index_pool: QuiescentGuard<EvictableBufferPool>,
        table_fs: &FileSystem,
        disk_pool: QuiescentGuard<ReadonlyBufferPool>,
        guards: &PoolGuards,
        table_id: TableID,
    ) -> Result<()> {
        if self.user_tables.contains_key(&table_id) {
            return Err(Error::TableAlreadyExists);
        }
        let res = self
            .storage
            .tables()
            .find_uncommitted_by_id(guards, table_id)
            .await?;
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
                    .list_uncommitted_by_table_id(guards, table_id)
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
                    .list_uncommitted_by_table_id(guards, table_id)
                    .await;
                indexes.sort_by_key(|index| index.index_no);

                let mut index_columns = self
                    .storage
                    .index_columns()
                    .list_uncommitted_by_table_id(guards, table_id)
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
                let table_file = table_fs
                    .open_table_file(table.table_id, disk_pool.clone())
                    .await?;
                let active_root = table_file.active_root();
                let metadata_in_catalog = TableMetadata::new(column_specs, index_specs);
                let metadata_in_file = &*active_root.metadata;
                if &metadata_in_catalog != metadata_in_file {
                    return Err(Error::InvalidState);
                }
                let row_id_bound = active_root.pivot_row_id;
                let meta_pool_guard = guards.meta_guard();
                let index_pool_guard = guards.index_guard();

                let blk_idx = BlockIndex::new(
                    self.storage.meta_pool.clone(),
                    meta_pool_guard,
                    row_id_bound,
                    active_root.column_block_index_root,
                )
                .await?;
                let table = Arc::new(
                    Table::new(
                        mem_pool.clone(),
                        index_pool.clone(),
                        index_pool_guard,
                        table.table_id,
                        blk_idx,
                        table_file,
                        disk_pool.clone(),
                    )
                    .await?,
                );
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
    pub async fn get_table(&self, table_id: TableID) -> Option<Arc<Table>> {
        if is_catalog_obj_id(table_id) {
            return None;
        }
        self.user_tables
            .get(&table_id)
            .map(|table| Arc::clone(table.value()))
    }

    /// Get a catalog-table runtime handle by table id.
    #[inline]
    pub fn get_catalog_table(&self, table_id: TableID) -> Option<Arc<CatalogTable>> {
        self.storage.get_catalog_table(table_id)
    }

    /// Insert a user table runtime into the in-memory cache.
    #[inline]
    pub fn insert_user_table(&self, table: Arc<Table>) {
        let table_id = table.table_id();
        let old = self.user_tables.insert(table_id, table);
        debug_assert!(old.is_none());
    }

    /// Remove a user table runtime from the in-memory cache.
    #[inline]
    pub fn remove_user_table(&self, table_id: TableID) -> Option<Arc<Table>> {
        self.user_tables.remove(&table_id).map(|(_, table)| table)
    }

    /// Return the metadata buffer pool used by catalog/index metadata pages.
    #[inline]
    pub fn meta_pool(&self) -> &FixedBufferPool {
        &self.storage.meta_pool
    }

    #[inline]
    fn loaded_table_replay_start_ts(&self, table_id: TableID) -> Option<u64> {
        self.user_tables.get(&table_id).map(|table| {
            let root = table.file().active_root();
            root.heap_redo_start_ts.min(root.deletion_cutoff_ts)
        })
    }
}

/// Unified runtime handle for either user table or catalog table.
pub enum TableHandle {
    User(Arc<Table>),
    Catalog(Arc<CatalogTable>),
}

impl TableHandle {
    /// Returns the row page index for this table handle.
    #[inline]
    pub fn blk_idx(&self) -> &BlockIndex {
        match self {
            TableHandle::User(table) => &table.blk_idx,
            TableHandle::Catalog(table) => &table.blk_idx,
        }
    }

    /// Returns the secondary indexes exposed by this table handle.
    #[inline]
    pub fn pivot_row_id(&self) -> RowID {
        self.blk_idx().pivot_row_id()
    }

    /// Returns the deletion buffer if this handle refers to a user table.
    #[inline]
    pub fn deletion_buffer(&self) -> Option<&ColumnDeletionBuffer> {
        match self {
            TableHandle::User(table) => Some(table.deletion_buffer()),
            TableHandle::Catalog(_) => None,
        }
    }

    /// Resolve a row id to its current storage location.
    #[inline]
    pub async fn find_row(&self, guards: &PoolGuards, row_id: RowID) -> RowLocation {
        match self {
            TableHandle::User(table) => table.find_row(guards, row_id).await,
            TableHandle::Catalog(table) => table.find_row(guards, row_id).await,
        }
    }

    /// Load a versioned row page in shared mode if the requested generation still matches.
    #[inline]
    pub async fn get_row_page_versioned_shared(
        &self,
        guards: &PoolGuards,
        page_id: VersionedPageID,
    ) -> Result<Option<PageSharedGuard<RowPage>>> {
        match self {
            TableHandle::User(table) => table.get_row_page_versioned_shared(guards, page_id).await,
            TableHandle::Catalog(table) => {
                table.get_row_page_versioned_shared(guards, page_id).await
            }
        }
    }

    /// Load a row page in shared mode through the appropriate pool slot.
    #[inline]
    pub async fn get_row_page_shared(
        &self,
        guards: &PoolGuards,
        page_id: PageID,
    ) -> Result<Option<PageSharedGuard<RowPage>>> {
        match self {
            TableHandle::User(table) => table.get_row_page_shared(guards, page_id).await,
            TableHandle::Catalog(table) => table.get_row_page_shared(guards, page_id).await,
        }
    }

    /// Delete one secondary-index entry if it is no longer needed.
    #[inline]
    pub async fn delete_index(
        &self,
        guards: &PoolGuards,
        key: &SelectKey,
        row_id: RowID,
        unique: bool,
        min_active_sts: TrxID,
    ) -> Result<bool> {
        match self {
            TableHandle::User(table) => {
                table
                    .accessor()
                    .delete_index(guards, key, row_id, unique, min_active_sts)
                    .await
            }
            TableHandle::Catalog(table) => {
                table
                    .accessor()
                    .delete_index(guards, key, row_id, unique, min_active_sts)
                    .await
            }
        }
    }

    /// Roll back one secondary-index undo entry through the concrete table runtime.
    #[inline]
    pub(crate) async fn rollback_index_entry(
        &self,
        entry: IndexUndo,
        guards: &PoolGuards,
        ts: TrxID,
    ) -> Result<()> {
        match self {
            TableHandle::User(table) => table.rollback_index_entry(entry, guards, ts).await,
            TableHandle::Catalog(table) => table.rollback_index_entry(entry, guards, ts).await,
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
    use crate::conf::{EngineConfig, TrxSysConfig};
    use crate::engine::Engine;
    use crate::error::{BlockCorruptionCause, BlockKind, Error, FileKind};
    use crate::file::BlockID;
    use crate::file::block_integrity::{BLOCK_INTEGRITY_HEADER_SIZE, write_block_checksum};
    use crate::file::cow_file::COW_FILE_PAGE_SIZE;
    use crate::index::{COLUMN_BLOCK_HEADER_SIZE, COLUMN_BLOCK_LEAF_HEADER_SIZE, ColumnBlockIndex};
    use crate::table::TablePersistence;
    use crate::trx::MIN_SNAPSHOT_TS;
    use crate::trx::sys::CatalogCheckpointScanStopReason;
    use crate::value::{Val, ValKind};
    use semistr::SemiStr;
    use std::fs::OpenOptions;
    use std::io::{Read, Seek, SeekFrom, Write};
    use tempfile::TempDir;

    /// Table1 has single i32 column, with unique index of this column.
    #[inline]
    pub(crate) async fn table1(engine: &Engine) -> TableID {
        let mut session = engine.try_new_session().unwrap();
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
        let mut session = engine.try_new_session().unwrap();
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
        let mut session = engine.try_new_session().unwrap();

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
        let mut session = engine.try_new_session().unwrap();

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

    fn corrupt_page_checksum(path: impl AsRef<std::path::Path>, page_id: u64) {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        let offset = page_id * COW_FILE_PAGE_SIZE as u64 + (COW_FILE_PAGE_SIZE as u64 - 1);
        file.seek(SeekFrom::Start(offset)).unwrap();
        let mut byte = [0u8; 1];
        file.read_exact(&mut byte).unwrap();
        byte[0] ^= 0xFF;
        file.seek(SeekFrom::Start(offset)).unwrap();
        file.write_all(&byte).unwrap();
        file.flush().unwrap();
    }

    fn rewrite_page_with_checksum(
        path: impl AsRef<std::path::Path>,
        page_id: u64,
        rewrite: impl FnOnce(&mut [u8]),
    ) {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        let offset = page_id * COW_FILE_PAGE_SIZE as u64;
        let mut page = vec![0u8; COW_FILE_PAGE_SIZE];
        file.seek(SeekFrom::Start(offset)).unwrap();
        file.read_exact(&mut page).unwrap();
        rewrite(&mut page);
        write_block_checksum(&mut page);
        file.seek(SeekFrom::Start(offset)).unwrap();
        file.write_all(&page).unwrap();
        file.flush().unwrap();
    }

    fn corrupt_leaf_delete_codec(
        path: impl AsRef<std::path::Path>,
        page_id: u64,
        prefix_idx: usize,
    ) {
        rewrite_page_with_checksum(path, page_id, |page| {
            let byte_offset = leaf_entry_payload_offset(page, prefix_idx) + 35;
            page[byte_offset] = 0xFF;
        });
    }

    fn leaf_entry_payload_offset(page: &[u8], prefix_idx: usize) -> usize {
        const SEARCH_TYPE_PLAIN: u8 = 1;
        const SEARCH_TYPE_DELTA_U32: u8 = 2;
        const SEARCH_TYPE_DELTA_U16: u8 = 3;

        let payload_start = BLOCK_INTEGRITY_HEADER_SIZE;
        let search_type = page[payload_start + COLUMN_BLOCK_HEADER_SIZE];
        let (prefix_size, entry_offset_offset) = match search_type {
            SEARCH_TYPE_PLAIN => (10usize, 8usize),
            SEARCH_TYPE_DELTA_U32 => (6usize, 4usize),
            SEARCH_TYPE_DELTA_U16 => (4usize, 2usize),
            _ => panic!("invalid leaf search type {search_type}"),
        };
        let prefix_offset =
            payload_start + COLUMN_BLOCK_LEAF_HEADER_SIZE + prefix_idx * prefix_size;
        let entry_offset = u16::from_le_bytes(
            page[prefix_offset + entry_offset_offset..prefix_offset + entry_offset_offset + 2]
                .try_into()
                .unwrap(),
        ) as usize;
        payload_start + COLUMN_BLOCK_LEAF_HEADER_SIZE + entry_offset
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
            let main_dir = temp_dir.path().to_path_buf();
            let engine = EngineConfig::default()
                .storage_root(main_dir.clone())
                .trx(TrxSysConfig::default())
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
            let main_dir = temp_dir.path().to_path_buf();

            let engine = EngineConfig::default()
                .storage_root(main_dir.clone())
                .trx(TrxSysConfig::default().log_file_stem("catalog-allocator"))
                .build()
                .await
                .unwrap();
            assert_eq!(engine.catalog().curr_next_user_obj_id(), USER_OBJ_ID_START);
            let mut session = engine.try_new_session().unwrap();
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
                .storage_root(main_dir)
                .trx(TrxSysConfig::default().log_file_stem("catalog-allocator"))
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
            let main_dir = temp_dir.path().to_path_buf();

            let engine = EngineConfig::default()
                .storage_root(main_dir)
                .trx(TrxSysConfig::default().log_file_stem("catalog-checkpoint-now"))
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
                    .all(|root| root.root_block_id.is_none() && root.pivot_row_id == 0)
            );

            let _ = table1(&engine).await;
            engine
                .catalog()
                .checkpoint_now(&engine.trx_sys)
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
                    .any(|root| root.root_block_id.is_some())
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
                .checkpoint_now(&engine.trx_sys)
                .await
                .unwrap();
            let snap2 = engine.catalog().storage.checkpoint_snapshot().unwrap();
            assert_eq!(snap2.catalog_replay_start_ts, snap1.catalog_replay_start_ts);
            assert_eq!(snap2.meta.table_roots, snap1.meta.table_roots);
        });
    }

    #[test]
    fn test_catalog_bootstrap_fails_on_corrupted_checkpoint_lwc_block() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();

            let engine = EngineConfig::default()
                .storage_root(main_dir.clone())
                .trx(TrxSysConfig::default().log_file_stem("catalog-checkpoint-corrupt-bootstrap"))
                .build()
                .await
                .unwrap();

            let _ = table1(&engine).await;
            engine
                .catalog()
                .checkpoint_now(&engine.trx_sys)
                .await
                .unwrap();

            let snap = engine.catalog().storage.checkpoint_snapshot().unwrap();
            let root = snap
                .meta
                .table_roots
                .iter()
                .copied()
                .find(|root| root.root_block_id.is_some())
                .expect("catalog checkpoint should publish at least one root");
            let root_block_id = BlockID::from(root.root_block_id.unwrap().get());
            let block_id = {
                let disk_pool_guard = engine.catalog().storage.disk_pool.pool_guard();
                let index = ColumnBlockIndex::new(
                    root_block_id,
                    root.pivot_row_id,
                    engine.catalog().storage.mtb.file_kind(),
                    engine.catalog().storage.mtb.sparse_file(),
                    &engine.catalog().storage.disk_pool,
                    &disk_pool_guard,
                );
                let entry = index
                    .collect_leaf_entries()
                    .await
                    .unwrap()
                    .into_iter()
                    .next()
                    .expect("catalog checkpoint should publish at least one LWC block");
                entry.block_id()
            };
            drop(engine);

            corrupt_page_checksum(main_dir.join("catalog.mtb"), u64::from(block_id));

            let err = match EngineConfig::default()
                .storage_root(main_dir)
                .trx(TrxSysConfig::default().log_file_stem("catalog-checkpoint-corrupt-bootstrap"))
                .build()
                .await
            {
                Ok(_) => panic!("expected catalog bootstrap corruption failure"),
                Err(err) => err,
            };
            assert!(matches!(
                err,
                Error::BlockCorrupted {
                    file_kind: FileKind::CatalogMultiTableFile,
                    block_kind: BlockKind::LwcBlock,
                    block_id: page_id,
                    cause: BlockCorruptionCause::ChecksumMismatch,
                } if page_id == block_id
            ));
        });
    }

    #[test]
    fn test_catalog_bootstrap_fails_on_invalid_v2_delete_metadata() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();

            let engine = EngineConfig::default()
                .storage_root(main_dir.clone())
                .trx(
                    TrxSysConfig::default()
                        .log_file_stem("catalog-checkpoint-invalid-delete-metadata"),
                )
                .build()
                .await
                .unwrap();

            let _ = table1(&engine).await;
            engine
                .catalog()
                .checkpoint_now(&engine.trx_sys)
                .await
                .unwrap();

            let snap = engine.catalog().storage.checkpoint_snapshot().unwrap();
            let root = snap
                .meta
                .table_roots
                .iter()
                .copied()
                .find(|root| root.root_block_id.is_some())
                .expect("catalog checkpoint should publish at least one root");
            let root_block_id = BlockID::from(root.root_block_id.unwrap().get());
            let entry = {
                let disk_pool_guard = engine.catalog().storage.disk_pool.pool_guard();
                let index = ColumnBlockIndex::new(
                    root_block_id,
                    root.pivot_row_id,
                    engine.catalog().storage.mtb.file_kind(),
                    engine.catalog().storage.mtb.sparse_file(),
                    &engine.catalog().storage.disk_pool,
                    &disk_pool_guard,
                );
                index
                    .collect_leaf_entries()
                    .await
                    .unwrap()
                    .into_iter()
                    .next()
                    .expect("catalog checkpoint should publish at least one leaf entry")
            };
            drop(engine);

            corrupt_leaf_delete_codec(
                main_dir.join("catalog.mtb"),
                u64::from(entry.leaf_page_id),
                0,
            );

            let err = match EngineConfig::default()
                .storage_root(main_dir)
                .trx(
                    TrxSysConfig::default()
                        .log_file_stem("catalog-checkpoint-invalid-delete-metadata"),
                )
                .build()
                .await
            {
                Ok(_) => panic!("expected catalog bootstrap invalid-metadata failure"),
                Err(err) => err,
            };
            assert!(matches!(
                err,
                Error::BlockCorrupted {
                    file_kind: FileKind::CatalogMultiTableFile,
                    block_kind: BlockKind::ColumnBlockIndex,
                    block_id: page_id,
                    cause: BlockCorruptionCause::InvalidPayload,
                } if page_id == entry.leaf_page_id
            ));
        });
    }

    #[test]
    fn test_catalog_checkpoint_now_heartbeat_without_catalog_ops() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();

            let engine = EngineConfig::default()
                .storage_root(main_dir)
                .trx(TrxSysConfig::default().log_file_stem("catalog-checkpoint-heartbeat"))
                .build()
                .await
                .unwrap();

            let table_id = table1(&engine).await;
            engine
                .catalog()
                .checkpoint_now(&engine.trx_sys)
                .await
                .unwrap();
            let snap1 = engine.catalog().storage.checkpoint_snapshot().unwrap();
            assert!(snap1.catalog_replay_start_ts > MIN_SNAPSHOT_TS);
            let roots_before = snap1.meta.table_roots;

            let table = engine.catalog().get_table(table_id).await.unwrap();
            let mut session = engine.try_new_session().unwrap();
            let mut stmt = session.try_begin_trx().unwrap().unwrap().start_stmt();
            let res = stmt.insert_row(&table, vec![Val::I32(7)]).await;
            assert!(res.is_ok());
            stmt.succeed().commit().await.unwrap();

            engine
                .catalog()
                .checkpoint_now(&engine.trx_sys)
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
            let main_dir = temp_dir.path().to_path_buf();

            let engine = EngineConfig::default()
                .storage_root(main_dir)
                .trx(TrxSysConfig::default().log_file_stem("catalog-checkpoint-batch-full-range"))
                .build()
                .await
                .unwrap();

            let _ = table1(&engine).await;
            let _ = table2(&engine).await;

            let batch1 = engine
                .catalog()
                .scan_checkpoint_batch(&engine.trx_sys)
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
                .scan_checkpoint_batch(&engine.trx_sys)
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

    #[test]
    fn test_catalog_checkpoint_now_heartbeat_with_mixed_user_table_checkpoint_states() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();

            let engine = EngineConfig::default()
                .storage_root(main_dir)
                .trx(TrxSysConfig::default().log_file_stem("catalog-checkpoint-mixed-user-states"))
                .build()
                .await
                .unwrap();

            let checkpointed_table_id = table1(&engine).await;
            let replay_only_table_id = table2(&engine).await;

            engine
                .catalog()
                .checkpoint_now(&engine.trx_sys)
                .await
                .unwrap();
            let snap1 = engine.catalog().storage.checkpoint_snapshot().unwrap();
            assert!(snap1.catalog_replay_start_ts > MIN_SNAPSHOT_TS);
            let roots_before = snap1.meta.table_roots;

            let checkpointed_table = engine
                .catalog()
                .get_table(checkpointed_table_id)
                .await
                .unwrap();
            let replay_only_table = engine
                .catalog()
                .get_table(replay_only_table_id)
                .await
                .unwrap();

            let mut session = engine.try_new_session().unwrap();

            let mut stmt = session.try_begin_trx().unwrap().unwrap().start_stmt();
            let res = stmt
                .insert_row(&checkpointed_table, vec![Val::I32(7)])
                .await;
            assert!(res.is_ok());
            stmt.succeed().commit().await.unwrap();

            let mut stmt = session.try_begin_trx().unwrap().unwrap().start_stmt();
            let res = stmt
                .insert_row(
                    &replay_only_table,
                    vec![Val::I32(9), Val::from("replay-backed")],
                )
                .await;
            assert!(res.is_ok());
            stmt.succeed().commit().await.unwrap();

            checkpointed_table.freeze(&session, usize::MAX).await;
            let mut checkpoint_session = engine.try_new_session().unwrap();
            let checkpoint_outcome = checkpointed_table
                .checkpoint(&mut checkpoint_session)
                .await
                .unwrap();
            assert!(matches!(
                checkpoint_outcome,
                crate::table::CheckpointOutcome::Published { .. }
            ));

            assert!(checkpointed_table.file().active_root().pivot_row_id > 0);
            assert_eq!(replay_only_table.file().active_root().pivot_row_id, 0);
            assert!(
                checkpointed_table.file().active_root().heap_redo_start_ts
                    > snap1.catalog_replay_start_ts
            );

            engine
                .catalog()
                .checkpoint_now(&engine.trx_sys)
                .await
                .unwrap();
            let snap2 = engine.catalog().storage.checkpoint_snapshot().unwrap();
            assert!(snap2.catalog_replay_start_ts > snap1.catalog_replay_start_ts);
            assert_eq!(snap2.meta.table_roots, roots_before);
            assert_eq!(snap2.meta.next_user_obj_id, snap1.meta.next_user_obj_id);
        });
    }
}
