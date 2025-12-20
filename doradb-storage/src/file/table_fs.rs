use crate::catalog::TableID;
use crate::catalog::table::TableMetadata;
use crate::error::Result;
use crate::file::table_file::{ActiveRoot, TABLE_FILE_INITIAL_SIZE};
use crate::file::table_file::{MutableTableFile, TABLE_FILE_PAGE_SIZE, TableFile};
use crate::file::{FileIO, FileIOListener, FixedSizeBufferFreeList};
use crate::io::{AIOClient, AIOContext};
use crate::lifetime::StaticLifetime;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::thread::JoinHandle;

/// TableFileSystem provides functionalities including
/// creating, opening, closing and removing table files.
pub struct TableFileSystem {
    io_client: AIOClient<FileIO>,
    handle: Option<JoinHandle<()>>,
    buf_list: FixedSizeBufferFreeList,
    base_dir: String,
}

impl TableFileSystem {
    /// Create a new table file system.
    #[inline]
    pub fn new(io_depth: usize, base_dir: String) -> Result<Self> {
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
        })
    }

    /// Create a new table file.
    /// If trunc is set to true, old file will be overwritten.
    /// Otherwise, an error will be returned if file already exists.
    #[inline]
    pub fn create_table_file(
        &self,
        table_id: TableID,
        metadata: TableMetadata,
        trunc: bool,
    ) -> Result<MutableTableFile> {
        let file_path = format!("{}/{}.tbl", self.base_dir, table_id);
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
        let file_path = format!("{}/{}.tbl", self.base_dir, table_id);
        let table_file =
            TableFile::open(&file_path, self.io_client.clone(), self.buf_list.clone())?;
        let active_root = table_file.load_active_root().await?;
        let old_root = table_file.swap_active_root(active_root);
        debug_assert!(old_root.is_none());
        Ok(Arc::new(table_file))
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableFileSystemConfig {
    // IO depth of reading/write table files.
    pub io_depth: usize,
    // Base directory.
    pub base_dir: String,
}

impl TableFileSystemConfig {
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
    pub fn build(self) -> Result<TableFileSystem> {
        TableFileSystem::new(self.io_depth, self.base_dir)
    }
}

impl Default for TableFileSystemConfig {
    #[inline]
    fn default() -> Self {
        TableFileSystemConfig {
            io_depth: DEFAULT_TABLE_FILE_IO_DEPTH,
            base_dir: String::from(DEFAULT_TABLE_FILE_BASE_DIR),
        }
    }
}
