use crate::buffer::minimum_readonly_pool_bytes;
use crate::conf::path::{path_to_utf8, validate_catalog_file_name};
use crate::error::{ConfigError, ConfigResult, IoResult};
use crate::file::cow_file::COW_FILE_PAGE_SIZE;
use crate::file::fs::{FileSystem, StorageIOWorkerBuilder, build_file_system};
use crate::file::multi_table_file::MULTI_TABLE_FILE_INITIAL_SIZE;
use crate::file::table_file::TABLE_FILE_INITIAL_SIZE;
use error_stack::{Report, ResultExt};
use std::path::{Component as PathComponent, Path, PathBuf};

use super::consts::{
    DEFAULT_CATALOG_FILE_NAME, DEFAULT_COW_FILE_MAX_SIZE, DEFAULT_TABLE_FILE_DATA_DIR,
    DEFAULT_TABLE_FILE_IO_DEPTH, DEFAULT_TABLE_FILE_READONLY_BUFFER_SIZE,
};

/// Configuration for table and catalog file-system resources.
#[derive(Debug, Clone)]
pub struct FileSystemConfig {
    /// IO depth for reading and writing table files.
    pub io_depth: usize,
    /// Data directory used for table and catalog files.
    pub data_dir: PathBuf,
    /// Global readonly-buffer-pool size in bytes.
    pub readonly_buffer_size: usize,
    /// Catalog multi-table file name.
    pub catalog_file_name: String,
    /// Maximum logical size in bytes of each durable CoW table or catalog file.
    pub cow_file_max_size: usize,
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

    /// Set the maximum logical size of each durable CoW table or catalog file.
    #[inline]
    pub fn cow_file_max_size(mut self, cow_file_max_size: usize) -> Self {
        self.cow_file_max_size = cow_file_max_size;
        self
    }

    /// Validate and normalize construction inputs for the file system.
    #[inline]
    pub(crate) fn validate(self) -> ConfigResult<ValidatedFileSystemConfig> {
        if self.io_depth == 0 {
            return Err(Report::new(ConfigError::InvalidIoDepth).attach("file.io_depth=0"));
        }
        let min_readonly_buffer_size = minimum_readonly_pool_bytes();
        if self.readonly_buffer_size < min_readonly_buffer_size {
            return Err(
                Report::new(ConfigError::InvalidFixedBufferPoolSize).attach(format!(
                    "file.readonly_buffer_size={}, min_supported={min_readonly_buffer_size}",
                    self.readonly_buffer_size
                )),
            );
        }
        if !validate_catalog_file_name(&self.catalog_file_name) {
            return Err(
                Report::new(ConfigError::InvalidCatalogFileName).attach(format!(
                    "catalog file name must be a plain `.mtb` file name: {}",
                    self.catalog_file_name
                )),
            );
        }
        let cow_file_max_pages = self.cow_file_max_size / COW_FILE_PAGE_SIZE;
        if self.cow_file_max_size < TABLE_FILE_INITIAL_SIZE
            || self.cow_file_max_size < MULTI_TABLE_FILE_INITIAL_SIZE
            || !self.cow_file_max_size.is_multiple_of(COW_FILE_PAGE_SIZE)
            || cow_file_max_pages == 0
        {
            return Err(Report::new(ConfigError::InvalidCowFileSize).attach(format!(
                "file.cow_file_max_size={}, table_initial_size={TABLE_FILE_INITIAL_SIZE}, multi_table_initial_size={MULTI_TABLE_FILE_INITIAL_SIZE}, cow_file_page_size={COW_FILE_PAGE_SIZE}",
                self.cow_file_max_size
            )));
        }
        let data_dir = validate_data_dir(&self.data_dir)
            .attach_with(|| format!("invalid data_dir: {}", self.data_dir.display()))?;
        Ok(ValidatedFileSystemConfig {
            io_depth: self.io_depth,
            data_dir,
            catalog_file_name: self.catalog_file_name,
            cow_file_max_pages,
        })
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
            cow_file_max_size: DEFAULT_COW_FILE_MAX_SIZE,
        }
    }
}

/// Validated construction inputs for the table and catalog file system.
#[derive(Debug)]
pub(crate) struct ValidatedFileSystemConfig {
    io_depth: usize,
    data_dir: PathBuf,
    catalog_file_name: String,
    cow_file_max_pages: usize,
}

impl ValidatedFileSystemConfig {
    /// Build the table file-system and its storage IO worker.
    #[inline]
    pub(crate) fn build_engine_parts(self) -> IoResult<(FileSystem, StorageIOWorkerBuilder)> {
        build_file_system(
            self.io_depth,
            self.data_dir,
            self.catalog_file_name,
            self.cow_file_max_pages,
        )
    }
}

#[inline]
fn validate_data_dir(data_dir: &Path) -> ConfigResult<PathBuf> {
    if data_dir.as_os_str().is_empty() {
        return Err(
            Report::new(ConfigError::PathMustNotBeEmpty).attach("data_dir must not be empty")
        );
    }
    path_to_utf8(data_dir).attach_with(|| format!("invalid data_dir: {}", data_dir.display()))?;
    if data_dir
        .components()
        .any(|component| matches!(component, PathComponent::ParentDir))
    {
        return Err(
            Report::new(ConfigError::PathMustNotContainParentTraversal).attach(format!(
                "data_dir must not contain parent traversal: {}",
                data_dir.display()
            )),
        );
    }
    Ok(data_dir.to_path_buf())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_config_report(err: Report<ConfigError>, expected: ConfigError, snippets: &[&str]) {
        assert_eq!(err.current_context(), &expected);
        let output = format!("{err:?}");
        for snippet in snippets {
            assert!(output.contains(snippet), "missing `{snippet}` in {output}");
        }
    }

    #[test]
    fn test_file_system_config_validation_reports_details() {
        let err = FileSystemConfig::default()
            .catalog_file_name("catalog.bin")
            .validate()
            .unwrap_err();
        assert_config_report(
            err,
            ConfigError::InvalidCatalogFileName,
            &["plain `.mtb` file name", "catalog.bin"],
        );

        let err = validate_data_dir(Path::new("")).unwrap_err();
        assert_config_report(err, ConfigError::PathMustNotBeEmpty, &["must not be empty"]);

        let err = validate_data_dir(Path::new("../data")).unwrap_err();
        assert_config_report(
            err,
            ConfigError::PathMustNotContainParentTraversal,
            &["parent traversal", "../data"],
        );

        assert_eq!(
            FileSystemConfig::default().cow_file_max_size,
            DEFAULT_COW_FILE_MAX_SIZE
        );
        for invalid_size in [
            TABLE_FILE_INITIAL_SIZE - COW_FILE_PAGE_SIZE,
            TABLE_FILE_INITIAL_SIZE + 1,
        ] {
            let err = FileSystemConfig::default()
                .cow_file_max_size(invalid_size)
                .validate()
                .unwrap_err();
            assert_config_report(
                err,
                ConfigError::InvalidCowFileSize,
                &[
                    "file.cow_file_max_size",
                    "table_initial_size",
                    "multi_table_initial_size",
                    "cow_file_page_size",
                ],
            );
        }

        let validated = FileSystemConfig::default()
            .cow_file_max_size(TABLE_FILE_INITIAL_SIZE * 3)
            .validate()
            .unwrap();
        assert_eq!(
            validated.cow_file_max_pages,
            TABLE_FILE_INITIAL_SIZE * 3 / COW_FILE_PAGE_SIZE
        );
    }
}
