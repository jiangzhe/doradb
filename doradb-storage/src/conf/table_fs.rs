use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use super::consts::{
    DEFAULT_CATALOG_FILE_NAME, DEFAULT_TABLE_FILE_DATA_DIR, DEFAULT_TABLE_FILE_IO_DEPTH,
    DEFAULT_TABLE_FILE_READONLY_BUFFER_SIZE,
};

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
