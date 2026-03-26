use crate::buffer::{GlobalReadonlyBufferPool, ReadonlyBufferPool};
use crate::catalog::table::TableMetadata;
use crate::catalog::{TableID, USER_OBJ_ID_START};
use crate::component::{Component, ComponentRegistry, ShelfScope, Supplier};
use crate::conf::TableFileSystemConfig;
use crate::conf::path::{path_to_utf8, validate_catalog_file_name};
use crate::error::{Error, PersistedFileKind, Result};
use crate::file::cow_file::COW_FILE_PAGE_SIZE;
use crate::file::multi_table_file::{CATALOG_MTB_PERSISTED_FILE_ID, MultiTableFile};
use crate::file::table_file::{ActiveRoot, TABLE_FILE_INITIAL_SIZE};
use crate::file::table_file::{MutableTableFile, TableFile};
use crate::file::{TableFsRequest, TableFsStateMachine};
use crate::io::{AIOClient, IOBackendStats, IOBackendStatsHandle, IOWorkerBuilder, StorageBackend};
use crate::quiescent::{QuiescentBox, QuiescentGuard};
use parking_lot::Mutex;
use std::path::{Component as PathComponent, Path, PathBuf};
use std::sync::Arc;
use std::thread::JoinHandle;

/// TableFileSystem provides functionalities including
/// creating, opening, closing and removing table files.
pub struct TableFileSystem {
    io_client: AIOClient<TableFsRequest>,
    io_backend_stats: IOBackendStatsHandle,
    data_dir: PathBuf,
    // Catalog multi-table file name.
    catalog_file_name: String,
}

pub(crate) struct TableFileSystemWorkersOwned {
    fs: QuiescentGuard<TableFileSystem>,
    handle: Mutex<Option<JoinHandle<()>>>,
}

pub(crate) struct TableFileSystemWorkers;

impl TableFileSystem {
    /// Create a new table-file subsystem.
    ///
    /// This initializes one async IO context and its event-loop thread for
    /// table and catalog file reads and writes.
    #[inline]
    fn new(
        io_depth: usize,
        data_dir: PathBuf,
        catalog_file_name: String,
    ) -> Result<(Self, IOWorkerBuilder<TableFsRequest>)> {
        let ctx = StorageBackend::new(io_depth)?;
        let io_backend_stats = ctx.stats_handle();
        let (worker, io_client) = ctx.io_worker();
        Ok((
            TableFileSystem {
                io_client,
                io_backend_stats,
                data_dir,
                catalog_file_name,
            },
            worker,
        ))
    }

    /// Create a new table file.
    /// If trunc is set to true, old file will be overwritten.
    /// Otherwise, an error will be returned if file already exists.
    #[inline]
    pub fn create_table_file(
        &self,
        table_id: TableID,
        metadata: Arc<TableMetadata>,
        trunc: bool,
    ) -> Result<MutableTableFile> {
        let file_path = self.table_file_path(table_id);
        let table_file = TableFile::create(
            &file_path,
            TABLE_FILE_INITIAL_SIZE,
            table_id,
            self.io_client.clone(),
            trunc,
        )?;
        let initial_pages = TABLE_FILE_INITIAL_SIZE / COW_FILE_PAGE_SIZE;
        let active_root = ActiveRoot::new(0, initial_pages, metadata);
        Ok(MutableTableFile::new(Arc::new(table_file), active_root))
    }

    /// Open an existing table file.
    #[inline]
    pub async fn open_table_file(
        &self,
        table_id: TableID,
        global_disk_pool: QuiescentGuard<GlobalReadonlyBufferPool>,
    ) -> Result<(Arc<TableFile>, ReadonlyBufferPool)> {
        let file_path = self.table_file_path(table_id);
        let table_file = Arc::new(TableFile::open(
            &file_path,
            table_id,
            self.io_client.clone(),
        )?);
        let disk_pool = ReadonlyBufferPool::new(
            table_id,
            PersistedFileKind::TableFile,
            Arc::clone(&table_file),
            global_disk_pool,
        );
        let active_root = table_file.load_active_root_from_pool(&disk_pool).await?;
        let old_root = table_file.swap_active_root(active_root);
        debug_assert!(old_root.is_none());
        Ok((table_file, disk_pool))
    }

    /// Build file path for a logical table id.
    ///
    /// User table ids use fixed-width hex naming (`<016x>.tbl`), while
    /// reserved/catalog ids keep compact decimal names.
    #[inline]
    pub fn table_file_path(&self, table_id: TableID) -> String {
        let file_name = if table_id >= USER_OBJ_ID_START {
            format!("{table_id:016x}.tbl")
        } else {
            format!("{table_id}.tbl")
        };
        path_to_string(&self.data_dir.join(file_name), "table file path")
    }

    /// Build absolute path for the unified catalog file (`*.mtb`).
    #[inline]
    pub fn catalog_mtb_file_path(&self) -> String {
        path_to_string(
            &self.data_dir.join(&self.catalog_file_name),
            "catalog multi-table file path",
        )
    }

    /// Returns one snapshot of backend-owned submit/wait activity.
    #[inline]
    pub fn io_backend_stats(&self) -> IOBackendStats {
        self.io_backend_stats.snapshot()
    }

    /// Open existing catalog multi-table file or create a new one.
    #[inline]
    pub async fn open_or_create_multi_table_file(
        &self,
        global_disk_pool: QuiescentGuard<GlobalReadonlyBufferPool>,
    ) -> Result<(Arc<MultiTableFile>, ReadonlyBufferPool)> {
        let mtb =
            MultiTableFile::open_or_create(self.catalog_mtb_file_path(), self.io_client.clone())
                .await?;
        let disk_pool = ReadonlyBufferPool::new(
            CATALOG_MTB_PERSISTED_FILE_ID,
            PersistedFileKind::CatalogMultiTableFile,
            Arc::clone(&mtb),
            global_disk_pool,
        );
        if mtb
            .active_root_ptr()
            .load(std::sync::atomic::Ordering::Acquire)
            .is_null()
        {
            let active_root = mtb.load_active_root_from_pool(&disk_pool).await?;
            let old_root = mtb.swap_active_root(active_root);
            debug_assert!(old_root.is_none());
        }
        Ok((mtb, disk_pool))
    }

    fn start_worker(worker: IOWorkerBuilder<TableFsRequest>) -> JoinHandle<()> {
        let state_machine = TableFsStateMachine::new();
        worker.bind(state_machine).start_thread()
    }
}

impl Supplier<TableFileSystemWorkers> for TableFileSystem {
    type Provision = IOWorkerBuilder<TableFsRequest>;
}

impl Component for TableFileSystem {
    type Config = TableFileSystemConfig;
    type Owned = Self;
    type Access = crate::quiescent::QuiescentGuard<Self>;

    const NAME: &'static str = "table_fs";

    #[inline]
    async fn build(
        config: Self::Config,
        registry: &mut ComponentRegistry,
        mut shelf: ShelfScope<'_, Self>,
    ) -> Result<()> {
        let (fs, setup) = config.build_parts()?;
        registry.register::<Self>(fs)?;
        shelf.put::<TableFileSystemWorkers>(setup)?;
        TableFileSystemWorkers::build((), registry, shelf.scope::<TableFileSystemWorkers>()).await
    }

    #[inline]
    fn access(owner: &QuiescentBox<Self::Owned>) -> Self::Access {
        owner.guard()
    }

    #[inline]
    fn shutdown(_component: &Self::Owned) {}
}

impl Component for TableFileSystemWorkers {
    type Config = ();
    type Owned = TableFileSystemWorkersOwned;
    type Access = ();

    const NAME: &'static str = "table_fs_workers";

    #[inline]
    async fn build(
        _config: Self::Config,
        registry: &mut ComponentRegistry,
        mut shelf: ShelfScope<'_, Self>,
    ) -> Result<()> {
        let fs = registry.dependency::<TableFileSystem>()?;
        let handle = TableFileSystem::start_worker(
            shelf.take::<TableFileSystem>().ok_or(Error::InvalidState)?,
        );
        registry.register::<Self>(TableFileSystemWorkersOwned {
            fs,
            handle: Mutex::new(Some(handle)),
        })
    }

    #[inline]
    fn access(_owner: &QuiescentBox<Self::Owned>) -> Self::Access {}

    #[inline]
    fn shutdown(component: &Self::Owned) {
        component.fs.io_client.shutdown();
        if let Some(handle) = component.handle.lock().take() {
            handle.join().unwrap();
        }
    }
}

impl TableFileSystemConfig {
    /// Validate config and construct a [`TableFileSystem`].
    #[inline]
    fn build_parts(self) -> Result<(TableFileSystem, IOWorkerBuilder<TableFsRequest>)> {
        if !validate_catalog_file_name(&self.catalog_file_name) {
            return Err(Error::InvalidStoragePath(format!(
                "catalog file name must be a plain `.mtb` file name: {}",
                self.catalog_file_name
            )));
        }
        let data_dir = validate_data_dir(&self.data_dir)?;
        TableFileSystem::new(self.io_depth, data_dir, self.catalog_file_name)
    }

    #[inline]
    pub fn build(self) -> Result<TableFileSystem> {
        Ok(self.build_parts()?.0)
    }
}

#[inline]
fn validate_data_dir(data_dir: &Path) -> Result<PathBuf> {
    if data_dir.as_os_str().is_empty() {
        return Err(Error::InvalidStoragePath(
            "data_dir must not be empty".into(),
        ));
    }
    path_to_utf8(data_dir, "data_dir")?;
    if data_dir
        .components()
        .any(|component| matches!(component, PathComponent::ParentDir))
    {
        return Err(Error::InvalidStoragePath(format!(
            "data_dir must not contain parent traversal: {}",
            data_dir.display()
        )));
    }
    Ok(data_dir.to_path_buf())
}

#[inline]
fn path_to_string(path: &Path, field: &str) -> String {
    path_to_utf8(path, field)
        .expect("table file system paths are validated during construction")
        .to_owned()
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::catalog::{
        ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexSpec, USER_OBJ_ID_START,
    };
    use crate::error::Error;
    use crate::thread::join_worker;
    use crate::value::ValKind;
    use std::path::Path;
    use tempfile::TempDir;

    pub(crate) struct StartedTableFileSystem {
        owner: QuiescentBox<TableFileSystem>,
        handle: Mutex<Option<JoinHandle<()>>>,
    }

    impl StartedTableFileSystem {
        #[inline]
        pub(crate) fn guard(&self) -> QuiescentGuard<TableFileSystem> {
            self.owner.guard()
        }

        #[inline]
        pub(crate) fn shutdown(&self) {
            self.owner.io_client.shutdown();
            join_worker(&self.handle);
        }
    }

    impl std::ops::Deref for StartedTableFileSystem {
        type Target = TableFileSystem;

        #[inline]
        fn deref(&self) -> &Self::Target {
            &self.owner
        }
    }

    impl Drop for StartedTableFileSystem {
        #[inline]
        fn drop(&mut self) {
            self.shutdown();
        }
    }

    fn start_test_fs(config: TableFileSystemConfig) -> StartedTableFileSystem {
        let (fs, event_loop) = config.build_parts().unwrap();
        let owner = QuiescentBox::new(fs);
        let handle = TableFileSystem::start_worker(event_loop);
        StartedTableFileSystem {
            owner,
            handle: Mutex::new(Some(handle)),
        }
    }

    #[inline]
    pub(crate) fn build_test_fs() -> (TempDir, StartedTableFileSystem) {
        let temp_dir = TempDir::new().unwrap();
        let fs = build_test_fs_in(temp_dir.path());
        (temp_dir, fs)
    }

    #[inline]
    pub(crate) fn build_test_fs_in(data_dir: &Path) -> StartedTableFileSystem {
        start_test_fs(TableFileSystemConfig::default().data_dir(data_dir))
    }

    #[test]
    fn test_table_file_system_shutdown_is_idempotent() {
        let (_temp_dir, fs) = build_test_fs();

        fs.shutdown();
        fs.shutdown();
    }

    #[test]
    fn test_user_table_file_uses_hex_name() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();

            let metadata = Arc::new(TableMetadata::new(
                vec![ColumnSpec::new(
                    "c0",
                    ValKind::U32,
                    ColumnAttributes::empty(),
                )],
                vec![IndexSpec::new(
                    "idx_pk",
                    vec![IndexKey::new(0)],
                    IndexAttributes::PK,
                )],
            ));
            let mutable = fs
                .create_table_file(USER_OBJ_ID_START, Arc::clone(&metadata), false)
                .unwrap();
            let (table_file, old_root) = mutable.commit(1, false).await.unwrap();
            drop(old_root);

            let path = fs.table_file_path(USER_OBJ_ID_START);
            assert!(
                path.ends_with("0001000000000000.tbl"),
                "unexpected user table file path: {path}"
            );
            assert!(Path::new(&path).exists());

            drop(table_file);
            drop(fs);
        });
    }

    #[test]
    fn test_catalog_file_name_default_and_custom_path() {
        let (temp_dir, fs) = build_test_fs();
        assert!(fs.catalog_mtb_file_path().ends_with("catalog.mtb"));
        drop(fs);

        let fs = TableFileSystemConfig::default()
            .data_dir(temp_dir.path())
            .catalog_file_name("cat_meta.mtb")
            .build()
            .unwrap();
        assert!(fs.catalog_mtb_file_path().ends_with("cat_meta.mtb"));
        drop(fs);
    }

    #[test]
    fn test_catalog_file_name_validation() {
        let temp_dir = TempDir::new().unwrap();
        let res = TableFileSystemConfig::default()
            .data_dir(temp_dir.path())
            .catalog_file_name("catalog.bin")
            .build();
        assert!(res.is_err());

        let res = TableFileSystemConfig::default()
            .data_dir(temp_dir.path())
            .catalog_file_name("dir/catalog.mtb")
            .build();
        assert!(res.is_err());

        let res = TableFileSystemConfig::default()
            .data_dir(temp_dir.path())
            .catalog_file_name("../catalog.mtb")
            .build();
        assert!(res.is_err());
    }

    #[test]
    fn test_data_dir_validation() {
        let err = match TableFileSystemConfig::default()
            .data_dir(PathBuf::new())
            .build()
        {
            Ok(_) => panic!("expected invalid storage path"),
            Err(err) => err,
        };
        assert!(matches!(err, Error::InvalidStoragePath(_)));

        let err = match TableFileSystemConfig::default().data_dir("../data").build() {
            Ok(_) => panic!("expected invalid storage path"),
            Err(err) => err,
        };
        assert!(matches!(err, Error::InvalidStoragePath(_)));
    }

    #[cfg(unix)]
    #[test]
    fn test_data_dir_rejects_non_utf8_path() {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;

        let path = PathBuf::from(OsString::from_vec(vec![b'd', b'a', b't', b'a', 0xff]));
        let err = match TableFileSystemConfig::default().data_dir(path).build() {
            Ok(_) => panic!("expected invalid storage path"),
            Err(err) => err,
        };
        assert!(matches!(err, Error::InvalidStoragePath(_)));
    }
}
