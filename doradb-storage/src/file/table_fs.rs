use crate::catalog::table::TableMetadata;
use crate::catalog::{TableID, USER_OBJ_ID_START};
use crate::error::Result;
use crate::file::multi_table_file::MultiTableFile;
use crate::file::table_file::{ActiveRoot, TABLE_FILE_INITIAL_SIZE};
use crate::file::table_file::{MutableTableFile, TABLE_FILE_PAGE_SIZE, TableFile};
use crate::file::{FileIO, FileIOListener, FixedSizeBufferFreeList};
use crate::io::{AIOClient, AIOContext};
use crate::lifetime::StaticLifetime;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;
use std::thread::JoinHandle;

/// TableFileSystem provides functionalities including
/// creating, opening, closing and removing table files.
pub struct TableFileSystem {
    io_client: AIOClient<FileIO>,
    handle: Option<JoinHandle<()>>,
    buf_list: FixedSizeBufferFreeList,
    base_dir: String,
    // Catalog multi-table file name.
    catalog_file_name: String,
}

impl TableFileSystem {
    /// Create a new table file system.
    #[inline]
    pub fn new(io_depth: usize, base_dir: String, catalog_file_name: String) -> Result<Self> {
        let ctx = AIOContext::new(io_depth)?;
        let buf_list = FixedSizeBufferFreeList::new(TABLE_FILE_PAGE_SIZE, io_depth, io_depth * 2);
        let (event_loop, io_client) = ctx.event_loop();
        let listener = FileIOListener::new(buf_list.clone());
        let handle = event_loop.start_thread(listener);
        Ok(TableFileSystem {
            io_client,
            handle: Some(handle),
            buf_list,
            base_dir,
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
        let initial_pages = TABLE_FILE_INITIAL_SIZE / TABLE_FILE_PAGE_SIZE;
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

    #[inline]
    pub fn table_file_path(&self, table_id: TableID) -> String {
        if table_id >= USER_OBJ_ID_START {
            format!("{}/{table_id:016x}.tbl", self.base_dir)
        } else {
            format!("{}/{}.tbl", self.base_dir, table_id)
        }
    }

    #[inline]
    pub fn catalog_mtb_file_path(&self) -> String {
        format!("{}/{}", self.base_dir, self.catalog_file_name)
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
}

impl Drop for TableFileSystem {
    #[inline]
    fn drop(&mut self) {
        self.io_client.shutdown();
        self.handle.take().unwrap().join().unwrap();
    }
}

unsafe impl StaticLifetime for TableFileSystem {}

const DEFAULT_TABLE_FILE_IO_DEPTH: usize = 64;
const DEFAULT_TABLE_FILE_BASE_DIR: &str = ".";
const DEFAULT_TABLE_FILE_READONLY_BUFFER_SIZE: usize = 256 * 1024 * 1024;
const DEFAULT_CATALOG_FILE_NAME: &str = "catalog.mtb";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableFileSystemConfig {
    // IO depth of reading/write table files.
    pub io_depth: usize,
    // Base directory.
    pub base_dir: String,
    // Global readonly buffer pool size in bytes.
    pub readonly_buffer_size: usize,
    // Catalog multi-table file name.
    pub catalog_file_name: String,
}

impl TableFileSystemConfig {
    #[inline]
    pub fn with_main_dir(mut self, main_dir: impl AsRef<Path>) -> Self {
        let base_dir = main_dir.as_ref().join(&self.base_dir);
        self.base_dir = base_dir.to_string_lossy().to_string();
        self
    }

    #[inline]
    pub fn io_depth(mut self, io_depth: usize) -> Self {
        self.io_depth = io_depth;
        self
    }

    #[inline]
    pub fn data_dir(mut self, base_dir: impl Into<String>) -> Self {
        self.base_dir = base_dir.into();
        self
    }

    #[inline]
    pub fn readonly_buffer_size(mut self, readonly_buffer_size: usize) -> Self {
        self.readonly_buffer_size = readonly_buffer_size;
        self
    }

    #[inline]
    pub fn catalog_file_name(mut self, catalog_file_name: impl Into<String>) -> Self {
        self.catalog_file_name = catalog_file_name.into();
        self
    }

    #[inline]
    pub fn build(self) -> Result<TableFileSystem> {
        if !validate_catalog_file_name(&self.catalog_file_name) {
            return Err(crate::error::Error::InvalidArgument);
        }
        TableFileSystem::new(self.io_depth, self.base_dir, self.catalog_file_name)
    }
}

impl Default for TableFileSystemConfig {
    #[inline]
    fn default() -> Self {
        TableFileSystemConfig {
            io_depth: DEFAULT_TABLE_FILE_IO_DEPTH,
            base_dir: String::from(DEFAULT_TABLE_FILE_BASE_DIR),
            readonly_buffer_size: DEFAULT_TABLE_FILE_READONLY_BUFFER_SIZE,
            catalog_file_name: String::from(DEFAULT_CATALOG_FILE_NAME),
        }
    }
}

#[inline]
fn validate_catalog_file_name(file_name: &str) -> bool {
    if file_name.is_empty() || !file_name.ends_with(".mtb") {
        return false;
    }
    // Must be a plain file name under base_dir, not a path.
    Path::new(file_name).file_name().and_then(|n| n.to_str()) == Some(file_name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{
        ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexSpec, USER_OBJ_ID_START,
    };
    use crate::value::ValKind;
    use std::path::Path;
    use tempfile::TempDir;

    #[test]
    fn test_user_table_file_uses_hex_name() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let fs = TableFileSystemConfig::default()
                .with_main_dir(temp_dir.path())
                .build()
                .unwrap();

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
        let temp_dir = TempDir::new().unwrap();
        let fs = TableFileSystemConfig::default()
            .with_main_dir(temp_dir.path())
            .build()
            .unwrap();
        assert!(fs.catalog_mtb_file_path().ends_with("catalog.mtb"));
        drop(fs);

        let fs = TableFileSystemConfig::default()
            .with_main_dir(temp_dir.path())
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
            .with_main_dir(temp_dir.path())
            .catalog_file_name("catalog.bin")
            .build();
        assert!(res.is_err());

        let res = TableFileSystemConfig::default()
            .with_main_dir(temp_dir.path())
            .catalog_file_name("dir/catalog.mtb")
            .build();
        assert!(res.is_err());
    }
}
