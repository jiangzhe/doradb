use crate::catalog::table::TableMetadata;
use crate::catalog::{TableID, USER_OBJ_ID_START};
use crate::error::{Error, Result};
use crate::file::cow_file::COW_FILE_PAGE_SIZE;
use crate::file::multi_table_file::MultiTableFile;
use crate::file::table_file::{ActiveRoot, TABLE_FILE_INITIAL_SIZE};
use crate::file::table_file::{MutableTableFile, TableFile};
use crate::file::{FileIO, FileIOListener, FixedSizeBufferFreeList};
use crate::io::{AIOClient, AIOContext};
use crate::storage_path::{path_to_utf8, validate_catalog_file_name};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::thread::JoinHandle;

/// TableFileSystem provides functionalities including
/// creating, opening, closing and removing table files.
pub struct TableFileSystem {
    io_client: AIOClient<FileIO>,
    handle: Mutex<Option<JoinHandle<()>>>,
    buf_list: FixedSizeBufferFreeList,
    data_dir: PathBuf,
    // Catalog multi-table file name.
    catalog_file_name: String,
}

impl TableFileSystem {
    /// Create a new table-file subsystem.
    ///
    /// This initializes one async IO context, its event loop thread, and a
    /// shared direct-buffer free list for table and catalog files.
    #[inline]
    fn new(io_depth: usize, data_dir: PathBuf, catalog_file_name: String) -> Result<Self> {
        let ctx = AIOContext::new(io_depth)?;
        let buf_list = FixedSizeBufferFreeList::new(COW_FILE_PAGE_SIZE, io_depth, io_depth * 2);
        let (event_loop, io_client) = ctx.event_loop();
        let listener = FileIOListener::new(buf_list.clone());
        let handle = event_loop.start_thread(listener);
        Ok(TableFileSystem {
            io_client,
            handle: Mutex::new(Some(handle)),
            buf_list,
            data_dir,
            catalog_file_name,
        })
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
            self.io_client.clone(),
            self.buf_list.clone(),
            trunc,
        )?;
        let initial_pages = TABLE_FILE_INITIAL_SIZE / COW_FILE_PAGE_SIZE;
        let active_root = ActiveRoot::new(0, initial_pages, metadata);
        Ok(MutableTableFile::new(Arc::new(table_file), active_root))
    }

    /// Open an existing table file.
    #[inline]
    pub async fn open_table_file(&self, table_id: TableID) -> Result<Arc<TableFile>> {
        let file_path = self.table_file_path(table_id);
        let table_file =
            TableFile::open(&file_path, self.io_client.clone(), self.buf_list.clone())?;
        let active_root = table_file.load_active_root().await?;
        let old_root = table_file.swap_active_root(active_root);
        debug_assert!(old_root.is_none());
        Ok(Arc::new(table_file))
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

    /// Open existing catalog multi-table file or create a new one.
    #[inline]
    pub async fn open_or_create_multi_table_file(&self) -> Result<Arc<MultiTableFile>> {
        MultiTableFile::open_or_create(
            self.catalog_mtb_file_path(),
            self.io_client.clone(),
            self.buf_list.clone(),
        )
        .await
    }

    #[inline]
    pub(crate) fn shutdown(&self) {
        self.io_client.shutdown();
        if let Some(handle) = self.handle.lock().take() {
            handle.join().unwrap();
        }
    }
}

impl Drop for TableFileSystem {
    #[inline]
    fn drop(&mut self) {
        self.shutdown();
    }
}

const DEFAULT_TABLE_FILE_IO_DEPTH: usize = 64;
const DEFAULT_TABLE_FILE_DATA_DIR: &str = ".";
const DEFAULT_TABLE_FILE_READONLY_BUFFER_SIZE: usize = 256 * 1024 * 1024;
const DEFAULT_CATALOG_FILE_NAME: &str = "catalog.mtb";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableFileSystemConfig {
    // IO depth of reading/write table files.
    pub io_depth: usize,
    // Data directory used for table and catalog files.
    pub data_dir: PathBuf,
    // Global readonly buffer pool size in bytes.
    pub readonly_buffer_size: usize,
    // Catalog multi-table file name.
    pub catalog_file_name: String,
}

impl TableFileSystemConfig {
    /// Set async IO queue depth used by table-file subsystem.
    #[inline]
    pub fn io_depth(mut self, io_depth: usize) -> Self {
        self.io_depth = io_depth;
        self
    }

    /// Set data directory used for table and catalog files.
    #[inline]
    pub fn data_dir(mut self, data_dir: impl Into<PathBuf>) -> Self {
        self.data_dir = data_dir.into();
        self
    }

    /// Set global readonly-buffer-pool size in bytes.
    #[inline]
    pub fn readonly_buffer_size(mut self, readonly_buffer_size: usize) -> Self {
        self.readonly_buffer_size = readonly_buffer_size;
        self
    }

    /// Set unified catalog file name under `data_dir` (must end with `.mtb`).
    #[inline]
    pub fn catalog_file_name(mut self, catalog_file_name: impl Into<String>) -> Self {
        self.catalog_file_name = catalog_file_name.into();
        self
    }

    /// Validate config and construct a [`TableFileSystem`].
    #[inline]
    pub fn build(self) -> Result<TableFileSystem> {
        if !validate_catalog_file_name(&self.catalog_file_name) {
            return Err(Error::InvalidStoragePath(format!(
                "catalog file name must be a plain `.mtb` file name: {}",
                self.catalog_file_name
            )));
        }
        let data_dir = validate_data_dir(&self.data_dir)?;
        TableFileSystem::new(self.io_depth, data_dir, self.catalog_file_name)
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
        .any(|component| matches!(component, Component::ParentDir))
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

impl Default for TableFileSystemConfig {
    #[inline]
    fn default() -> Self {
        TableFileSystemConfig {
            io_depth: DEFAULT_TABLE_FILE_IO_DEPTH,
            data_dir: PathBuf::from(DEFAULT_TABLE_FILE_DATA_DIR),
            readonly_buffer_size: DEFAULT_TABLE_FILE_READONLY_BUFFER_SIZE,
            catalog_file_name: String::from(DEFAULT_CATALOG_FILE_NAME),
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::catalog::{
        ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexSpec, USER_OBJ_ID_START,
    };
    use crate::error::Error;
    use crate::value::ValKind;
    use std::path::Path;
    use tempfile::TempDir;

    #[inline]
    pub(crate) fn build_test_fs() -> (TempDir, TableFileSystem) {
        let temp_dir = TempDir::new().unwrap();
        let fs = build_test_fs_in(temp_dir.path());
        (temp_dir, fs)
    }

    #[inline]
    pub(crate) fn build_test_fs_in(data_dir: &Path) -> TableFileSystem {
        TableFileSystemConfig::default()
            .data_dir(data_dir)
            .build()
            .unwrap()
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
