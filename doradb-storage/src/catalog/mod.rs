mod checkpoint;
mod index;
pub(crate) mod spec;
pub(crate) mod storage;
pub(crate) mod table;

pub(crate) use checkpoint::*;
pub(crate) use index::*;
pub(crate) use spec::ActiveIndexSpec;
pub use spec::{
    ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexNo, IndexOrder, IndexSpec,
    TableSpec,
};
pub(crate) use storage::*;
pub(crate) use table::*;

use crate::DiskPool;
use crate::buffer::{
    BufferPool, EvictableBufferPool, FixedBufferPool, PoolGuard, PoolGuards, PoolRole,
    ReadonlyBufferPool,
};
use crate::component::{Component, ComponentRegistry, MetaPool, ShelfScope};
use crate::error::{DataIntegrityError, Error, OperationError, Result};
use crate::file::fs::FileSystem;
use crate::id::{RowID, TableID, TrxID};
use crate::index::BlockIndex;
use crate::quiescent::{QuiescentBox, QuiescentGuard};
use crate::row::ops::SelectKey;
use crate::table::{MemTable, Table, TableRuntimeLayout};
use crate::trx::MIN_SNAPSHOT_TS;
use crate::trx::undo::IndexUndo;
use dashmap::DashMap;
use error_stack::Report;
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

pub(crate) const USER_OBJ_ID_START: TableID = TableID::new(0x0001_0000_0000_0000);

/// Return whether an object id belongs to user-managed catalog space.
#[inline]
pub(crate) const fn is_user_obj_id(obj_id: TableID) -> bool {
    obj_id.as_u64() >= USER_OBJ_ID_START.as_u64()
}

/// Return whether an object id belongs to built-in catalog table space.
#[inline]
pub(crate) const fn is_catalog_obj_id(obj_id: TableID) -> bool {
    !is_user_obj_id(obj_id)
}

/// Dedicated runtime wrapper for catalog logical tables.
pub(crate) struct CatalogTable {
    pub(crate) mem: MemTable<FixedBufferPool, FixedBufferPool>,
}

impl CatalogTable {
    /// Build a catalog table runtime from catalog-specific construction inputs.
    #[inline]
    pub(crate) async fn new(
        mem_pool: QuiescentGuard<FixedBufferPool>,
        meta_pool_guard: &PoolGuard,
        table_id: TableID,
        blk_idx: BlockIndex,
        metadata: Arc<TableMetadata>,
    ) -> Result<Self> {
        let mem = MemTable::new(
            mem_pool.clone(),
            mem_pool.row_pool_role(),
            mem_pool,
            PoolRole::Meta,
            meta_pool_guard,
            table_id,
            metadata,
            blk_idx,
            MIN_SNAPSHOT_TS,
        )
        .await?;
        Ok(CatalogTable { mem })
    }
}

impl Deref for CatalogTable {
    type Target = MemTable<FixedBufferPool, FixedBufferPool>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.mem
    }
}

/// Catalog contains metadata of user tables.
pub(crate) struct Catalog {
    next_table_id: AtomicU64,
    user_tables: DashMap<TableID, Arc<Table>>,
    pub(crate) storage: CatalogStorage,
    checkpoint_gate: CatalogCheckpointGate,
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
    pub(crate) async fn new(storage: CatalogStorage) -> Result<Self> {
        let pool_guards = PoolGuards::builder()
            .push(PoolRole::Meta, storage.meta_pool.pool_guard())
            .push(PoolRole::Disk, storage.disk_pool.pool_guard())
            .build();
        let snapshot = storage.checkpoint_snapshot()?;
        storage
            .bootstrap_from_checkpoint(&snapshot, &pool_guards)
            .await?;
        let next_table_id = storage.next_table_id();
        Ok(Catalog {
            next_table_id: AtomicU64::new(next_table_id.as_u64()),
            user_tables: DashMap::new(),
            storage,
            checkpoint_gate: CatalogCheckpointGate::new(),
        })
    }

    /// Allocate and return the next table id.
    #[inline]
    pub(crate) fn next_table_id(&self) -> TableID {
        TableID::new(self.next_table_id.fetch_add(1, Ordering::SeqCst))
    }

    #[inline]
    fn try_update_next_table_id(&self, next_table_id: TableID) {
        self.next_table_id
            .fetch_max(next_table_id.as_u64(), Ordering::SeqCst);
    }

    /// Return the current next table id without allocating one.
    #[inline]
    pub(crate) fn curr_next_table_id(&self) -> TableID {
        TableID::new(self.next_table_id.load(Ordering::Acquire))
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
    pub(crate) async fn apply_checkpoint_batch(&self, batch: CatalogCheckpointBatch) -> Result<()> {
        self.storage
            .apply_checkpoint_batch(batch, self.curr_next_table_id())
            .await
    }

    /// Returns whether a table is user table.
    #[inline]
    pub(crate) fn is_user_table(&self, table_id: TableID) -> bool {
        is_user_obj_id(table_id)
    }

    /// Reload one user table runtime from catalog metadata and table file.
    ///
    /// Returns `true` when catalog metadata exactly matches the table-file root
    /// metadata. Returns `false` when the metadata differs only by a recoverable
    /// index-DDL gap and recovery must replay catalog index-DDL rows before final
    /// metadata validation.
    pub(crate) async fn reload_create_table(
        &self,
        mem_pool: QuiescentGuard<EvictableBufferPool>,
        index_pool: QuiescentGuard<EvictableBufferPool>,
        table_fs: &FileSystem,
        disk_pool: QuiescentGuard<ReadonlyBufferPool>,
        table_id: TableID,
    ) -> Result<bool> {
        if self.user_tables.contains_key(&table_id) {
            return Err(Report::new(OperationError::TableAlreadyExists)
                .attach(format!("reload user table: table_id={table_id}"))
                .into());
        }
        let guards = PoolGuards::builder()
            .push(PoolRole::Meta, self.storage.meta_pool.pool_guard())
            .push(PoolRole::Index, index_pool.pool_guard())
            .push(PoolRole::Disk, disk_pool.pool_guard())
            .build();
        let (table, metadata_in_catalog) = self
            .user_table_metadata_from_catalog(&guards, table_id)
            .await?;

        // Phase 2 allocator semantics: only table ids consume the global allocator.
        self.try_update_next_table_id(table.table_id.saturating_add(1).max(USER_OBJ_ID_START));

        let table_file = table_fs
            .open_table_file(table.table_id, disk_pool.clone())
            .await?;
        // `catalog_load_boundary`: loading a user table binds one root for
        // metadata validation and block-index initialization.
        let active_root = table_file.active_root_unchecked();
        let metadata_in_file = &*active_root.metadata;
        let metadata_matched = if &metadata_in_catalog == metadata_in_file {
            true
        } else if index_ddl_metadata_reconcilable(
            table.table_id,
            &metadata_in_catalog,
            metadata_in_file,
        )? {
            false
        } else {
            return Err(Report::new(DataIntegrityError::InvalidRootInvariant)
                .attach(format!(
                    "reload user table metadata mismatch outside index-DDL reconciliation: table_id={}",
                    table.table_id
                ))
                .into());
        };

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
            return Err(Report::new(OperationError::TableAlreadyExists)
                .attach(format!("insert reloaded user table: table_id={table_id}"))
                .into());
        }
        Ok(metadata_matched)
    }

    /// Build user-table metadata from current in-memory catalog rows.
    pub(crate) async fn user_table_metadata_from_catalog(
        &self,
        guards: &PoolGuards,
        table_id: TableID,
    ) -> Result<(TableObject, TableMetadata)> {
        let table = self
            .storage
            .tables()
            .find_uncommitted_by_id(guards, table_id)
            .await?
            .ok_or_else(|| {
                Error::from(
                    Report::new(OperationError::TableNotFound)
                        .attach(format!("reload user table metadata: table_id={table_id}")),
                )
            })?;

        // todo: use secondary index to improve performance
        let mut columns = self
            .storage
            .columns()
            .list_uncommitted_by_table_id(guards, table_id)
            .await?;
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
            .await?;
        indexes.sort_by_key(|index| index.index_no);

        let mut index_columns = self
            .storage
            .index_columns()
            .list_uncommitted_by_table_id(guards, table_id)
            .await?;
        index_columns.sort_by_key(|ic| (ic.index_no, ic.index_column_no));
        let mut index_columns_by_index_no: BTreeMap<u16, Vec<IndexColumnObject>> = BTreeMap::new();
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
            index_specs.push(ActiveIndexSpec::new(
                index.index_no,
                IndexSpec::new(index_cols, index.index_attributes),
            ));
        }
        if !index_columns_by_index_no.is_empty() {
            let index_numbers = index_columns_by_index_no
                .keys()
                .copied()
                .collect::<Vec<_>>();
            let count: usize = index_columns_by_index_no
                .values()
                .map(|index_columns| index_columns.len())
                .sum();
            return Err(Report::new(DataIntegrityError::InvalidRootInvariant)
                .attach(format!(
                    "orphaned catalog index-column rows without matching index rows: table_id={table_id}, index_numbers={index_numbers:?}, count={count}"
                ))
                .into());
        }
        let metadata = TableMetadata::try_new_with_next_index_no(
            column_specs,
            index_specs,
            table.next_index_no,
        )?;
        Ok((table, metadata))
    }

    /// Get a user-table runtime handle by table id.
    #[inline]
    pub(crate) async fn get_table(&self, table_id: TableID) -> Option<Arc<Table>> {
        self.get_table_now(table_id)
    }

    /// Get a user-table runtime handle synchronously by table id.
    #[inline]
    pub(crate) fn get_table_now(&self, table_id: TableID) -> Option<Arc<Table>> {
        if is_catalog_obj_id(table_id) {
            return None;
        }
        self.user_tables
            .get(&table_id)
            .map(|table| Arc::clone(table.value()))
    }

    /// Acquires the catalog metadata-change gate for future index DDL.
    #[inline]
    pub(crate) async fn begin_metadata_change(&self) -> CatalogMetadataChangeLease<'_> {
        self.checkpoint_gate.begin_metadata_change().await
    }

    /// Validates that a user-table runtime exists and still admits foreground work.
    #[inline]
    pub(crate) async fn validate_user_table_live(
        &self,
        table_id: TableID,
        operation: &'static str,
    ) -> Result<Arc<Table>> {
        let Some(table) = self.get_table(table_id).await else {
            return Err(Report::new(OperationError::TableNotFound)
                .attach(format!("operation={operation}, table_id={table_id}"))
                .into());
        };
        table.check_foreground_live(operation)?;
        Ok(table)
    }

    /// Get a catalog-table runtime handle by table id.
    #[inline]
    pub(crate) fn get_catalog_table(&self, table_id: TableID) -> Option<Arc<CatalogTable>> {
        self.storage.get_catalog_table(table_id)
    }

    /// Insert a user table runtime into the in-memory cache.
    #[inline]
    pub(crate) fn insert_user_table(&self, table: Arc<Table>) -> Result<()> {
        let table_id = table.table_id();
        match self.user_tables.entry(table_id) {
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(table);
                Ok(())
            }
            dashmap::mapref::entry::Entry::Occupied(_) => {
                Err(Report::new(OperationError::TableAlreadyExists)
                    .attach(format!("insert user table runtime: table_id={table_id}"))
                    .into())
            }
        }
    }

    /// Remove a user table runtime from the in-memory cache.
    #[inline]
    pub(crate) fn remove_user_table(&self, table_id: TableID) -> Option<Arc<Table>> {
        self.user_tables.remove(&table_id).map(|(_, table)| table)
    }
}

#[inline]
fn index_ddl_metadata_reconcilable(
    table_id: TableID,
    catalog: &TableMetadata,
    file: &TableMetadata,
) -> Result<bool> {
    // The active table root may be ahead of checkpointed catalog rows: recovery
    // can replay later index-DDL catalog rows to make catalog metadata catch up.
    // The opposite direction is unrecoverable here because replay cannot make a
    // table root that has already been opened acquire missing allocation state.
    if file.idx.next_index_no() < catalog.idx.next_index_no() {
        return Err(Report::new(DataIntegrityError::InvalidRootInvariant)
            .attach(format!(
                "index-DDL reconciliation found catalog allocation ahead of table root: table_id={table_id}, catalog_next_index_no={}, file_next_index_no={}",
                catalog.idx.next_index_no(),
                file.idx.next_index_no()
            ))
            .into());
    }
    if catalog.col != file.col {
        return Ok(false);
    }

    let max_slots = catalog
        .idx
        .index_slot_count()
        .max(file.idx.index_slot_count());
    for index_no in 0..max_slots {
        let catalog_spec = catalog.idx.index_spec(index_no);
        let file_spec = file.idx.index_spec(index_no);
        if let (Some(catalog_spec), Some(file_spec)) = (catalog_spec, file_spec)
            && catalog_spec != file_spec
        {
            return Ok(false);
        }
    }
    Ok(true)
}

/// User-table cache entry used by rollback and purge paths.
///
/// Row-only paths can use the table directly. Index rollback and purge use this
/// entry to lazily pin one user-table layout snapshot for repeated same-table
/// index operations. A purge cycle may cache a layout before a later DROP INDEX
/// publishes a new inactive slot; that is safe only because RFC 0018 keeps
/// `index_no` stable and non-reused. If the cached/current layout sees the slot
/// inactive, index purge is a no-op; if it still sees the old slot active, it
/// can only touch the old runtime/root identity for that same stable slot.
pub(crate) struct UserTableCacheEntry {
    table: Arc<Table>,
    user_layout: Option<Arc<TableRuntimeLayout>>,
}

impl UserTableCacheEntry {
    #[inline]
    fn new(table: Arc<Table>) -> Self {
        UserTableCacheEntry {
            table,
            user_layout: None,
        }
    }

    /// Returns the bound user-table runtime.
    #[inline]
    pub(crate) fn table(&self) -> &Table {
        self.table.as_ref()
    }

    /// Roll back one secondary-index undo entry through the bound user table.
    #[inline]
    pub(crate) async fn rollback_index_entry(
        &mut self,
        entry: IndexUndo,
        guards: &PoolGuards,
        ts: TrxID,
    ) -> Result<()> {
        let table = &self.table;
        let layout = self
            .user_layout
            .get_or_insert_with(|| table.layout_snapshot());
        table
            .rollback_index_entry_with_layout(layout, entry, guards, ts)
            .await
    }

    /// Delete one user secondary-index entry if it is no longer needed.
    #[inline]
    pub(crate) async fn delete_index(
        &mut self,
        guards: &PoolGuards,
        key: &SelectKey,
        row_id: RowID,
        unique: bool,
        min_active_sts: TrxID,
    ) -> Result<bool> {
        let table = &self.table;
        let layout = self
            .user_layout
            .get_or_insert_with(|| table.layout_snapshot());
        table
            .accessor_with_layout(layout.as_ref())
            .delete_index(guards, key, row_id, unique, min_active_sts)
            .await
    }
}

/// Per-operation table cache used by rollback/recovery paths.
pub(crate) struct TableCache<'a> {
    catalog: &'a Catalog,
    user_tables: HashMap<TableID, UserTableCacheEntry>,
    catalog_tables: HashMap<TableID, Arc<CatalogTable>>,
    missing: HashSet<TableID>,
}

impl<'a> TableCache<'a> {
    /// Create an empty table cache bound to one catalog instance.
    #[inline]
    pub(crate) fn new(catalog: &'a Catalog) -> Self {
        TableCache {
            catalog,
            user_tables: HashMap::new(),
            catalog_tables: HashMap::new(),
            missing: HashSet::new(),
        }
    }

    /// Returns cached user table for given id.
    ///
    /// If table is not cached, this method loads it from catalog and caches
    /// positive/negative lookup result.
    #[inline]
    pub(crate) async fn get_user_table(&mut self, table_id: TableID) -> Option<&Table> {
        self.get_user_entry_mut(table_id)
            .await
            .map(|binding| binding.table())
    }

    /// Returns cached catalog table for given id.
    ///
    /// If table is not cached, this method loads it from catalog and caches
    /// positive/negative lookup result.
    #[inline]
    pub(crate) fn get_catalog_table(&mut self, table_id: TableID) -> Option<&CatalogTable> {
        if !is_catalog_obj_id(table_id) {
            return None;
        }
        match self.catalog_tables.entry(table_id) {
            Entry::Vacant(vac) => {
                if self.missing.contains(&table_id) {
                    return None;
                }
                match self.catalog.get_catalog_table(table_id) {
                    Some(table) => Some(Arc::as_ref(vac.insert(table))),
                    None => {
                        let _ = self.missing.insert(table_id);
                        None
                    }
                }
            }
            Entry::Occupied(occ) => Some(Arc::as_ref(occ.into_mut())),
        }
    }

    /// Returns cached user-table entry for given id.
    ///
    /// Index maintenance paths use this mutable entry to lazily pin one
    /// user-table layout snapshot for repeated same-table index operations.
    #[inline]
    pub(crate) async fn get_user_entry_mut(
        &mut self,
        table_id: TableID,
    ) -> Option<&mut UserTableCacheEntry> {
        if is_catalog_obj_id(table_id) {
            return None;
        }
        match self.user_tables.entry(table_id) {
            Entry::Vacant(vac) => {
                if self.missing.contains(&table_id) {
                    return None;
                }
                match self.catalog.get_table(table_id).await {
                    Some(table) => {
                        let res = vac.insert(UserTableCacheEntry::new(table));
                        Some(res)
                    }
                    None => {
                        let _ = self.missing.insert(table_id);
                        None
                    }
                }
            }
            Entry::Occupied(occ) => {
                let res = occ.into_mut();
                Some(res)
            }
        }
    }

    /// Returns cached user table and requires table to exist.
    ///
    /// This method is intended for rollback paths where table id in undo log
    /// must always map to an existing table.
    #[inline]
    pub(crate) async fn must_get_user_table(&mut self, table_id: TableID) -> &Table {
        match self.get_user_table(table_id).await {
            Some(table) => table,
            None => panic!("table {table_id} not found in catalog"),
        }
    }

    /// Returns cached catalog table and requires table to exist.
    ///
    /// This method is intended for rollback paths where table id in undo log
    /// must always map to an existing table.
    #[inline]
    pub(crate) fn must_get_catalog_table(&mut self, table_id: TableID) -> &CatalogTable {
        match self.get_catalog_table(table_id) {
            Some(table) => table,
            None => panic!("table {table_id} not found in catalog"),
        }
    }

    /// Returns cached user-table entry and requires table to exist.
    ///
    /// This method is intended for rollback paths where table id in undo log
    /// must always map to an existing table.
    #[inline]
    pub(crate) async fn must_get_user_entry_mut(
        &mut self,
        table_id: TableID,
    ) -> &mut UserTableCacheEntry {
        match self.get_user_entry_mut(table_id).await {
            Some(entry) => entry,
            None => panic!("table {table_id} not found in catalog"),
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::catalog::CatalogCheckpointScanStopReason;
    use crate::catalog::{ColumnAttributes, IndexAttributes, IndexKey, IndexSpec, TableSpec};
    use crate::conf::{EngineConfig, TrxSysConfig};
    use crate::engine::Engine;
    use crate::error::{CompletionErrorKind, DataIntegrityError, Error};
    use crate::file::block_integrity::{BLOCK_INTEGRITY_HEADER_SIZE, write_block_checksum};
    use crate::file::cow_file::COW_FILE_PAGE_SIZE;
    use crate::id::BlockID;
    use crate::index::{COLUMN_BLOCK_HEADER_SIZE, COLUMN_BLOCK_LEAF_HEADER_SIZE, ColumnBlockIndex};
    use crate::table::TablePersistence;
    use crate::trx::redo::DDLRedo;
    use crate::trx::{ActiveTrx, MIN_SNAPSHOT_TS};
    use crate::value::{Val, ValKind};
    use semistr::SemiStr;
    use std::fs::OpenOptions;
    use std::io::{Read, Seek, SeekFrom, Write};
    use tempfile::TempDir;

    #[inline]
    pub(crate) fn catalog_test_engine_config(
        main_dir: impl Into<std::path::PathBuf>,
        log_file_stem: Option<&str>,
    ) -> EngineConfig {
        let mut trx = TrxSysConfig::default();
        if let Some(log_file_stem) = log_file_stem {
            trx = trx.log_file_stem(log_file_stem);
        }
        EngineConfig::default().storage_root(main_dir).trx(trx)
    }

    #[inline]
    pub(crate) async fn open_catalog_test_engine(
        main_dir: impl Into<std::path::PathBuf>,
        log_file_stem: Option<&str>,
    ) -> Engine {
        catalog_test_engine_config(main_dir, log_file_stem)
            .build()
            .await
            .unwrap()
    }

    #[inline]
    pub(crate) async fn expect_catalog_test_engine_error(
        main_dir: impl Into<std::path::PathBuf>,
        log_file_stem: Option<&str>,
        expected_message: &str,
    ) -> Error {
        match catalog_test_engine_config(main_dir, log_file_stem)
            .build()
            .await
        {
            Ok(_) => panic!("{expected_message}"),
            Err(err) => err,
        }
    }

    /// Table1 has single i32 column, with unique index of this column.
    #[inline]
    pub(crate) async fn table1(engine: &Engine) -> TableID {
        let mut session = engine.new_session().unwrap();
        let table_id = session
            .create_table(
                TableSpec {
                    columns: vec![ColumnSpec {
                        column_name: SemiStr::new("id"),
                        column_type: ValKind::I32,
                        column_attributes: ColumnAttributes::empty(),
                    }],
                },
                vec![IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::PK)],
            )
            .await
            .unwrap();

        drop(session);
        table_id
    }

    /// Table2 has i32(unique key) and string column.
    #[inline]
    pub(crate) async fn table2(engine: &Engine) -> TableID {
        let mut session = engine.new_session().unwrap();
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
                vec![IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::PK)],
            )
            .await
            .unwrap();

        drop(session);
        table_id
    }

    /// Table3 has single string key column.
    #[inline]
    pub(crate) async fn table3(engine: &Engine) -> TableID {
        let mut session = engine.new_session().unwrap();

        let table_id = session
            .create_table(
                TableSpec {
                    columns: vec![ColumnSpec {
                        column_name: SemiStr::new("name"),
                        column_type: ValKind::VarByte,
                        column_attributes: ColumnAttributes::empty(),
                    }],
                },
                vec![IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::PK)],
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
        let mut session = engine.new_session().unwrap();

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
                        vec![IndexKey::new(0)],
                        // unique index.
                        IndexAttributes::PK,
                    ),
                    IndexSpec::new(
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

    fn assert_catalog_data_integrity(err: Error) {
        let report = format!("{err:?}");
        if matches!(
            err.completion_error(),
            Some(CompletionErrorKind::DataIntegrity(_))
        ) {
            assert!(report.contains("propagate from other threads"), "{report}");
            assert!(report.contains("wait for"), "{report}");
        } else {
            assert!(err.data_integrity_error().is_some(), "{report}");
        }
    }

    fn mark_catalog_ddl(trx: &mut ActiveTrx, ddl: DDLRedo) {
        let old = trx.effects_mut().redo_mut().ddl.replace(Box::new(ddl));
        debug_assert!(old.is_none());
    }

    #[test]
    fn test_catalog_user_obj_id_boundary_predicates() {
        let before_user = TableID::new(USER_OBJ_ID_START.as_u64() - 1);
        assert!(is_catalog_obj_id(before_user));
        assert!(!is_catalog_obj_id(USER_OBJ_ID_START));
        assert!(!is_user_obj_id(before_user));
        assert!(is_user_obj_id(USER_OBJ_ID_START));
    }

    #[test]
    fn test_index_ddl_metadata_reconcilable_rejects_column_attribute_mismatch() {
        let catalog_metadata = TableMetadata::try_new(
            vec![ColumnSpec::new(
                "id",
                ValKind::I32,
                ColumnAttributes::empty(),
            )],
            vec![IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::PK)],
        )
        .expect("valid table metadata");
        let file_metadata = TableMetadata::try_new(
            vec![ColumnSpec::new("id", ValKind::I32, ColumnAttributes::INDEX)],
            vec![IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::PK)],
        )
        .expect("valid table metadata");
        assert_eq!(catalog_metadata.col.col_names, file_metadata.col.col_names);
        assert_eq!(catalog_metadata.col.col_types, file_metadata.col.col_types);
        assert_ne!(catalog_metadata.col.col_attrs, file_metadata.col.col_attrs);
        assert!(
            !index_ddl_metadata_reconcilable(TableID::new(42), &catalog_metadata, &file_metadata)
                .unwrap()
        );
    }

    #[test]
    fn test_index_ddl_metadata_reconcilable_allows_file_ahead_of_catalog() {
        let columns = || {
            vec![
                ColumnSpec::new("id", ValKind::I32, ColumnAttributes::empty()),
                ColumnSpec::new("value", ValKind::I32, ColumnAttributes::empty()),
            ]
        };
        let primary_index = || IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::PK);
        let secondary_index = || IndexSpec::new(vec![IndexKey::new(1)], IndexAttributes::empty());

        let catalog_metadata =
            TableMetadata::try_new(columns(), vec![primary_index()]).expect("valid table metadata");
        let file_metadata =
            TableMetadata::try_new(columns(), vec![primary_index(), secondary_index()])
                .expect("valid table metadata");

        assert!(file_metadata.idx.next_index_no() > catalog_metadata.idx.next_index_no());
        assert!(
            index_ddl_metadata_reconcilable(TableID::new(42), &catalog_metadata, &file_metadata)
                .unwrap()
        );
    }

    #[test]
    fn test_index_ddl_metadata_reconcilable_errors_when_catalog_ahead_of_file() {
        let columns = || {
            vec![
                ColumnSpec::new("id", ValKind::I32, ColumnAttributes::empty()),
                ColumnSpec::new("value", ValKind::I32, ColumnAttributes::empty()),
            ]
        };
        let primary_index = || IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::PK);
        let secondary_index = || IndexSpec::new(vec![IndexKey::new(1)], IndexAttributes::empty());

        let catalog_metadata =
            TableMetadata::try_new(columns(), vec![primary_index(), secondary_index()])
                .expect("valid table metadata");
        let file_metadata =
            TableMetadata::try_new(columns(), vec![primary_index()]).expect("valid table metadata");

        assert!(catalog_metadata.idx.next_index_no() > file_metadata.idx.next_index_no());
        let err =
            index_ddl_metadata_reconcilable(TableID::new(42), &catalog_metadata, &file_metadata)
                .unwrap_err();
        assert_eq!(
            err.data_integrity_error(),
            Some(DataIntegrityError::InvalidRootInvariant)
        );
        let report = format!("{err:?}");
        assert!(report.contains("table_id=42"), "{report}");
        assert!(report.contains("catalog_next_index_no=2"), "{report}");
        assert!(report.contains("file_next_index_no=1"), "{report}");
    }

    #[test]
    fn test_user_table_metadata_rejects_orphan_index_columns() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = open_catalog_test_engine(
                temp_dir.path().to_path_buf(),
                Some("catalog-orphan-index-column"),
            )
            .await;
            let mut session = engine.new_session().unwrap();
            let table_id = session
                .create_table(
                    TableSpec {
                        columns: vec![ColumnSpec::new(
                            "id",
                            ValKind::I32,
                            ColumnAttributes::empty(),
                        )],
                    },
                    vec![IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::PK)],
                )
                .await
                .unwrap();
            let orphan_index_no = 7;

            let mut trx = session.begin_trx().unwrap();
            trx.exec(async |stmt| {
                assert!(
                    engine
                        .catalog()
                        .storage
                        .index_columns()
                        .insert(
                            stmt,
                            &IndexColumnObject {
                                table_id,
                                index_no: orphan_index_no,
                                index_column_no: 0,
                                column_no: 0,
                                index_order: IndexOrder::Asc,
                            },
                        )
                        .await
                );
                Ok(())
            })
            .await
            .unwrap();
            mark_catalog_ddl(
                &mut trx,
                DDLRedo::CreateIndex {
                    table_id,
                    index_no: orphan_index_no,
                },
            );
            trx.commit().await.unwrap();

            let guards = PoolGuards::builder()
                .push(PoolRole::Meta, engine.meta_pool.pool_guard())
                .build();
            let err = engine
                .catalog()
                .user_table_metadata_from_catalog(&guards, table_id)
                .await
                .unwrap_err();
            assert_eq!(
                err.data_integrity_error(),
                Some(DataIntegrityError::InvalidRootInvariant)
            );
            let report = format!("{err:?}");
            assert!(report.contains(&format!("table_id={table_id}")), "{report}");
            assert!(
                report.contains(&format!("index_numbers=[{orphan_index_no}]")),
                "{report}"
            );
            assert!(report.contains("count=1"), "{report}");
        });
    }

    #[test]
    fn test_bootstrap_creates_catalog_mtb_without_catalog_tbl_files() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = open_catalog_test_engine(main_dir.clone(), None).await;
            drop(engine);

            let data_dir = temp_dir.path();
            assert!(data_dir.join("catalog.mtb").exists());
            for table_id in 0..4u64 {
                assert!(!data_dir.join(format!("{table_id}.tbl")).exists());
            }
        });
    }

    #[test]
    fn test_next_table_id_monotonic_across_restart() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();

            let engine =
                open_catalog_test_engine(main_dir.clone(), Some("catalog-allocator")).await;
            assert_eq!(engine.catalog().curr_next_table_id(), USER_OBJ_ID_START);
            let mut session = engine.new_session().unwrap();
            let table_spec = TableSpec::new(vec![
                ColumnSpec::new("id", ValKind::I32, ColumnAttributes::empty()),
                ColumnSpec::new("k1", ValKind::I32, ColumnAttributes::empty()),
                ColumnSpec::new("k2", ValKind::I32, ColumnAttributes::empty()),
            ]);
            let index_specs = vec![
                IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::PK),
                IndexSpec::new(
                    vec![IndexKey::new(1), IndexKey::new(2)],
                    IndexAttributes::empty(),
                ),
            ];
            let table_id1 = session.create_table(table_spec, index_specs).await.unwrap();
            assert_eq!(engine.catalog().curr_next_table_id(), table_id1 + 1);
            drop(session);
            drop(engine);

            let engine = open_catalog_test_engine(main_dir, Some("catalog-allocator")).await;
            assert_eq!(engine.catalog().curr_next_table_id(), table_id1 + 1);
            let table_id2 = table1(&engine).await;
            assert!(table_id1 >= USER_OBJ_ID_START);
            assert_eq!(table_id2, table_id1 + 1);
            drop(engine);
        });
    }

    #[test]
    fn test_next_index_no_persists_across_restart_and_catalog_checkpoint() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let log_stem = "stable-index-metadata";

            let engine = open_catalog_test_engine(main_dir.clone(), Some(log_stem)).await;
            let mut session = engine.new_session().unwrap();
            let table_id = session
                .create_table(
                    TableSpec {
                        columns: vec![
                            ColumnSpec::new("id", ValKind::I32, ColumnAttributes::empty()),
                            ColumnSpec::new("name", ValKind::VarByte, ColumnAttributes::empty()),
                        ],
                    },
                    vec![
                        IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::PK),
                        IndexSpec::new(vec![IndexKey::new(1)], IndexAttributes::empty()),
                    ],
                )
                .await
                .unwrap();
            let table = engine.catalog().get_table(table_id).await.unwrap();
            assert_eq!(table.metadata().idx.next_index_no(), 2);
            assert_eq!(
                table
                    .metadata()
                    .idx
                    .active_indexes()
                    .map(|(index_no, _)| index_no)
                    .collect::<Vec<_>>(),
                vec![0, 1]
            );
            assert_eq!(
                table
                    .file()
                    .active_root_unchecked()
                    .secondary_index_roots
                    .len(),
                2
            );
            drop(table);
            drop(session);
            drop(engine);

            let engine = open_catalog_test_engine(main_dir.clone(), Some(log_stem)).await;
            let table = engine.catalog().get_table(table_id).await.unwrap();
            assert_eq!(table.metadata().idx.next_index_no(), 2);
            assert_eq!(
                table
                    .file()
                    .active_root_unchecked()
                    .secondary_index_roots
                    .len(),
                2
            );
            let indexes = engine
                .catalog()
                .storage
                .indexes()
                .list_uncommitted_by_table_id(
                    &PoolGuards::builder()
                        .push(PoolRole::Meta, engine.meta_pool.pool_guard())
                        .build(),
                    table_id,
                )
                .await
                .unwrap();
            assert_eq!(
                indexes
                    .iter()
                    .map(|index| index.index_no)
                    .collect::<Vec<_>>(),
                vec![0, 1]
            );
            drop(table);
            engine
                .catalog()
                .checkpoint_now(&engine.trx_sys)
                .await
                .unwrap();
            drop(engine);

            let engine = open_catalog_test_engine(main_dir, Some(log_stem)).await;
            let table = engine.catalog().get_table(table_id).await.unwrap();
            assert_eq!(table.metadata().idx.next_index_no(), 2);
            assert_eq!(table.metadata().idx.active_index_count(), 2);
            assert_eq!(
                table
                    .file()
                    .active_root_unchecked()
                    .secondary_index_roots
                    .len(),
                2
            );
            drop(table);
            drop(engine);
        });
    }

    #[test]
    fn test_catalog_checkpoint_now_publish_and_noop() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();

            let engine = open_catalog_test_engine(main_dir, Some("catalog-checkpoint-now")).await;

            let snap0 = engine.catalog().storage.checkpoint_snapshot().unwrap();
            assert_eq!(snap0.catalog_replay_start_ts, MIN_SNAPSHOT_TS);
            assert!(
                snap0
                    .meta
                    .table_roots
                    .iter()
                    .all(|root| root.root_block_id.is_none() && root.pivot_row_id == RowID::new(0))
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
                snap1.meta.next_table_id,
                engine.catalog().curr_next_table_id()
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
                    .any(|root| root.pivot_row_id > RowID::new(0))
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

            let engine = open_catalog_test_engine(
                main_dir.clone(),
                Some("catalog-checkpoint-corrupt-bootstrap"),
            )
            .await;

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

            let err = expect_catalog_test_engine_error(
                main_dir,
                Some("catalog-checkpoint-corrupt-bootstrap"),
                "expected catalog bootstrap corruption failure",
            )
            .await;
            assert_catalog_data_integrity(err);
        });
    }

    #[test]
    fn test_catalog_bootstrap_fails_on_invalid_v2_delete_metadata() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();

            let engine = open_catalog_test_engine(
                main_dir.clone(),
                Some("catalog-checkpoint-invalid-delete-metadata"),
            )
            .await;

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
                u64::from(entry.leaf_block_id),
                0,
            );

            let err = expect_catalog_test_engine_error(
                main_dir,
                Some("catalog-checkpoint-invalid-delete-metadata"),
                "expected catalog bootstrap invalid-metadata failure",
            )
            .await;
            assert_catalog_data_integrity(err);
        });
    }

    #[test]
    fn test_catalog_checkpoint_now_heartbeat_without_catalog_ops() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();

            let engine =
                open_catalog_test_engine(main_dir, Some("catalog-checkpoint-heartbeat")).await;

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
            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();
            trx.exec(async |stmt| {
                stmt.table_insert_mvcc(&table, vec![Val::I32(7)]).await?;
                Ok(())
            })
            .await
            .unwrap();
            trx.commit().await.unwrap();

            engine
                .catalog()
                .checkpoint_now(&engine.trx_sys)
                .await
                .unwrap();
            let snap2 = engine.catalog().storage.checkpoint_snapshot().unwrap();
            assert!(snap2.catalog_replay_start_ts > snap1.catalog_replay_start_ts);
            assert_eq!(snap2.meta.table_roots, roots_before);
            assert_eq!(snap2.meta.next_table_id, snap1.meta.next_table_id);
        });
    }

    #[test]
    fn test_catalog_checkpoint_scan_apply_full_range() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();

            let engine =
                open_catalog_test_engine(main_dir, Some("catalog-checkpoint-batch-full-range"))
                    .await;

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

            let engine =
                open_catalog_test_engine(main_dir, Some("catalog-checkpoint-mixed-user-states"))
                    .await;

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

            let mut session = engine.new_session().unwrap();

            let mut trx = session.begin_trx().unwrap();
            trx.exec(async |stmt| {
                stmt.table_insert_mvcc(&checkpointed_table, vec![Val::I32(7)])
                    .await?;
                Ok(())
            })
            .await
            .unwrap();
            trx.commit().await.unwrap();

            let mut trx = session.begin_trx().unwrap();
            trx.exec(async |stmt| {
                stmt.table_insert_mvcc(
                    &replay_only_table,
                    vec![Val::I32(9), Val::from("replay-backed")],
                )
                .await?;
                Ok(())
            })
            .await
            .unwrap();
            trx.commit().await.unwrap();

            checkpointed_table
                .freeze(&session, usize::MAX)
                .await
                .unwrap();
            let mut checkpoint_session = engine.new_session().unwrap();
            let checkpoint_outcome = checkpointed_table
                .checkpoint(&mut checkpoint_session)
                .await
                .unwrap();
            assert!(matches!(
                checkpoint_outcome,
                crate::table::CheckpointOutcome::Published { .. }
            ));

            assert!(
                checkpointed_table
                    .file()
                    .active_root_unchecked()
                    .pivot_row_id
                    > RowID::new(0)
            );
            assert_eq!(
                replay_only_table
                    .file()
                    .active_root_unchecked()
                    .pivot_row_id,
                RowID::new(0)
            );
            assert!(
                checkpointed_table
                    .file()
                    .active_root_unchecked()
                    .heap_redo_start_ts
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
            assert_eq!(snap2.meta.next_table_id, snap1.meta.next_table_id);
        });
    }
}
