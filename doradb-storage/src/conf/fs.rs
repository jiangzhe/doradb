use crate::conf::path::{path_to_utf8, validate_catalog_file_name};
use crate::error::{Error, Result};
use crate::file::fs::{FileSystem, FileSystemWorkersProvision, build_file_system};
use serde::{Deserialize, Serialize};
use std::path::{Component as PathComponent, Path, PathBuf};

use super::consts::{
    DEFAULT_CATALOG_FILE_NAME, DEFAULT_TABLE_FILE_DATA_DIR, DEFAULT_TABLE_FILE_IO_DEPTH,
    DEFAULT_TABLE_FILE_READONLY_BUFFER_SIZE,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileSystemConfig {
    // IO depth of reading/write table files.
    pub io_depth: usize,
    // Data directory used for table and catalog files.
    pub data_dir: PathBuf,
    // Global readonly buffer pool size in bytes.
    pub readonly_buffer_size: usize,
    // Catalog multi-table file name.
    pub catalog_file_name: String,
}

impl FileSystemConfig {
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

    #[inline]
    fn validate_parts(self) -> Result<(PathBuf, String, usize)> {
        if !validate_catalog_file_name(&self.catalog_file_name) {
            return Err(Error::InvalidStoragePath(format!(
                "catalog file name must be a plain `.mtb` file name: {}",
                self.catalog_file_name
            )));
        }
        let data_dir = validate_data_dir(&self.data_dir)?;
        Ok((data_dir, self.catalog_file_name, self.io_depth))
    }

    #[inline]
    pub(crate) fn build_engine_parts(self) -> Result<(FileSystem, FileSystemWorkersProvision)> {
        let (data_dir, catalog_file_name, io_depth) = self.validate_parts()?;
        build_file_system(io_depth, data_dir, catalog_file_name)
    }
}

impl Default for FileSystemConfig {
    #[inline]
    fn default() -> Self {
        FileSystemConfig {
            io_depth: DEFAULT_TABLE_FILE_IO_DEPTH,
            data_dir: PathBuf::from(DEFAULT_TABLE_FILE_DATA_DIR),
            readonly_buffer_size: DEFAULT_TABLE_FILE_READONLY_BUFFER_SIZE,
            catalog_file_name: String::from(DEFAULT_CATALOG_FILE_NAME),
        }
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
