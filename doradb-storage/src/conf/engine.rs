use crate::error::{Error, Result};
use byte_unit::Byte;
use serde::{Deserialize, Serialize};
use std::fs::{self, OpenOptions};
use std::io::{ErrorKind, Write};
use std::path::{Component, Path, PathBuf};

use super::consts::{
    DEFAULT_ENGINE_INDEX_BUFFER, DEFAULT_ENGINE_INDEX_MAX_FILE_SIZE,
    DEFAULT_ENGINE_INDEX_SWAP_FILE, DEFAULT_ENGINE_META_BUFFER, STORAGE_LAYOUT_FILE_NAME,
    STORAGE_LAYOUT_VERSION,
};
use super::path::{
    path_to_utf8, validate_catalog_file_name, validate_log_file_stem,
    validate_swap_file_path_candidate,
};
use super::{EvictableBufferPoolConfig, FileSystemConfig, TrxSysConfig};
use crate::conf::consts::MAX_LOG_PARTITIONS;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct DurableStorageLayout {
    version: u32,
    data_dir: String,
    catalog_file_name: String,
    log_dir: String,
    log_file_stem: String,
    log_partitions: usize,
}

struct StoragePathResolveInput<'a> {
    storage_root: &'a Path,
    data_dir: &'a Path,
    catalog_file_name: &'a str,
    log_dir: &'a Path,
    log_file_stem: &'a str,
    log_partitions: usize,
    data_swap_file: &'a Path,
    index_swap_file: &'a Path,
}

#[derive(Debug, Clone)]
pub(crate) struct ResolvedStoragePaths {
    storage_root: PathBuf,
    data_dir: PathBuf,
    catalog_file_path: PathBuf,
    log_dir: PathBuf,
    log_file_stem: String,
    data_swap_file: PathBuf,
    index_swap_file: PathBuf,
    durable_layout: DurableStorageLayout,
}

impl ResolvedStoragePaths {
    fn resolve(input: StoragePathResolveInput<'_>) -> Result<Self> {
        if !validate_catalog_file_name(input.catalog_file_name) {
            return Err(Error::InvalidStoragePath(format!(
                "catalog file name must be a plain `.mtb` file name: {}",
                input.catalog_file_name
            )));
        }
        if !validate_log_file_stem(input.log_file_stem) {
            return Err(Error::InvalidStoragePath(format!(
                "log file stem must be a plain file name without glob characters: {}",
                input.log_file_stem
            )));
        }
        if input.log_partitions == 0 || input.log_partitions > MAX_LOG_PARTITIONS {
            return Err(Error::InvalidStoragePath(format!(
                "log partitions must be in 1..={MAX_LOG_PARTITIONS}: {}",
                input.log_partitions
            )));
        }
        validate_swap_file_path_candidate("data_swap_file", input.data_swap_file)?;
        validate_swap_file_path_candidate("index_swap_file", input.index_swap_file)?;

        let storage_root = resolve_storage_root(input.storage_root)?;
        let data_dir_rel = normalize_relative_dir(input.data_dir, "data_dir")?;
        let log_dir_rel = normalize_relative_dir(input.log_dir, "log_dir")?;
        let data_swap_file_rel =
            normalize_relative_file_path(input.data_swap_file, "data_swap_file")?;
        let index_swap_file_rel =
            normalize_relative_file_path(input.index_swap_file, "index_swap_file")?;

        let data_dir = storage_root.join(&data_dir_rel);
        let catalog_file_path = data_dir.join(input.catalog_file_name);
        let log_dir = storage_root.join(&log_dir_rel);
        let data_swap_file = storage_root.join(&data_swap_file_rel);
        let index_swap_file = storage_root.join(&index_swap_file_rel);
        let durable_layout = DurableStorageLayout {
            version: STORAGE_LAYOUT_VERSION,
            data_dir: relative_dir_display(&data_dir_rel, "data_dir")?,
            catalog_file_name: input.catalog_file_name.to_string(),
            log_dir: relative_dir_display(&log_dir_rel, "log_dir")?,
            log_file_stem: input.log_file_stem.to_string(),
            log_partitions: input.log_partitions,
        };
        let res = ResolvedStoragePaths {
            storage_root,
            data_dir,
            catalog_file_path,
            log_dir,
            log_file_stem: input.log_file_stem.to_string(),
            data_swap_file,
            index_swap_file,
            durable_layout,
        };
        res.validate_non_overlapping_paths()?;
        Ok(res)
    }

    pub(crate) fn data_dir_path(&self) -> &Path {
        &self.data_dir
    }

    pub(crate) fn log_dir_path(&self) -> &Path {
        &self.log_dir
    }

    pub(crate) fn data_swap_file_path(&self) -> &Path {
        &self.data_swap_file
    }

    pub(crate) fn index_swap_file_path(&self) -> &Path {
        &self.index_swap_file
    }

    pub(crate) fn ensure_directories(&self) -> Result<()> {
        fs::create_dir_all(&self.storage_root)?;
        fs::create_dir_all(&self.data_dir)?;
        fs::create_dir_all(&self.log_dir)?;
        if let Some(parent) = self.data_swap_file.parent() {
            fs::create_dir_all(parent)?;
        }
        if let Some(parent) = self.index_swap_file.parent() {
            fs::create_dir_all(parent)?;
        }
        Ok(())
    }

    pub(crate) fn validate_marker_if_present(&self) -> Result<()> {
        let marker_path = self.marker_path();
        if !marker_path.exists() {
            return Ok(());
        }
        self.validate_marker(&marker_path)
    }

    pub(crate) fn persist_marker_if_missing(&self) -> Result<()> {
        let marker_path = self.marker_path();
        let marker = toml::to_string(&self.durable_layout).map_err(|_| Error::InternalError)?;
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&marker_path)
        {
            Ok(mut file) => {
                file.write_all(marker.as_bytes())?;
                Ok(())
            }
            Err(err) if err.kind() == ErrorKind::AlreadyExists => {
                self.validate_marker(&marker_path)
            }
            Err(err) => Err(err.into()),
        }
    }

    fn marker_path(&self) -> PathBuf {
        self.storage_root.join(STORAGE_LAYOUT_FILE_NAME)
    }

    fn validate_marker(&self, marker_path: &Path) -> Result<()> {
        let raw = fs::read_to_string(marker_path)?;
        let actual = toml::from_str::<DurableStorageLayout>(&raw)
            .map_err(|_| Error::StorageLayoutMismatch("invalid storage-layout.toml".into()))?;
        if actual != self.durable_layout {
            return Err(Error::StorageLayoutMismatch(format!(
                "expected {:?}, found {:?}",
                self.durable_layout, actual
            )));
        }
        Ok(())
    }

    fn validate_non_overlapping_paths(&self) -> Result<()> {
        if self.data_swap_file == self.index_swap_file {
            return Err(Error::InvalidStoragePath(format!(
                "swap files must not overlap each other: {}",
                self.data_swap_file.display()
            )));
        }
        self.validate_swap_file_non_overlap("data_swap_file", &self.data_swap_file)?;
        self.validate_swap_file_non_overlap("index_swap_file", &self.index_swap_file)?;
        Ok(())
    }

    fn log_family_base_path(&self) -> PathBuf {
        self.log_dir.join(&self.log_file_stem)
    }

    fn validate_swap_file_non_overlap(&self, field: &str, path: &Path) -> Result<()> {
        if path == self.data_dir {
            return Err(Error::InvalidStoragePath(format!(
                "{field} must not overlap data_dir: {}",
                path.display()
            )));
        }
        if path == self.log_dir {
            return Err(Error::InvalidStoragePath(format!(
                "{field} must not overlap log_dir: {}",
                path.display()
            )));
        }
        if self.data_dir != self.storage_root && path.parent() == Some(self.data_dir.as_path()) {
            return Err(Error::InvalidStoragePath(format!(
                "{field} must not use data_dir as its parent directory: {}",
                path.display()
            )));
        }
        if self.log_dir != self.storage_root && path.parent() == Some(self.log_dir.as_path()) {
            return Err(Error::InvalidStoragePath(format!(
                "{field} must not use log_dir as its parent directory: {}",
                path.display()
            )));
        }
        if aliases_reserved_path(path, &self.catalog_file_path) {
            return Err(Error::InvalidStoragePath(format!(
                "{field} must not overlap catalog file: {}",
                path.display()
            )));
        }
        let marker_path = self.marker_path();
        if aliases_reserved_path(path, &marker_path) {
            return Err(Error::InvalidStoragePath(format!(
                "{field} must not overlap storage marker: {}",
                path.display()
            )));
        }
        if path
            .extension()
            .and_then(|ext| ext.to_str())
            .is_some_and(|ext| ext.eq_ignore_ascii_case("tbl") || ext.eq_ignore_ascii_case("mtb"))
        {
            return Err(Error::InvalidStoragePath(format!(
                "{field} must not use durable storage suffix `.tbl` or `.mtb`: {}",
                path.display()
            )));
        }
        let log_family_base = self.log_family_base_path();
        if aliases_reserved_path(path, &log_family_base) {
            return Err(Error::InvalidStoragePath(format!(
                "{field} must not overlap redo log family: {}",
                path.display()
            )));
        }
        let same_parent = path.parent() == Some(self.log_dir.as_path());
        let swap_file_name = path_to_utf8(
            Path::new(
                path.file_name()
                    .expect("swap file must resolve to a file path"),
            ),
            field,
        )?;
        if same_parent && swap_file_name.starts_with(&format!("{}.", self.log_file_stem)) {
            return Err(Error::InvalidStoragePath(format!(
                "{field} must not overlap redo log family: {}",
                path.display()
            )));
        }
        Ok(())
    }
}

fn aliases_reserved_path(path: &Path, reserved: &Path) -> bool {
    path == reserved || path.starts_with(reserved) || reserved.starts_with(path)
}

/// Storage-engine configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineConfig {
    pub(crate) storage_root: PathBuf,
    pub(crate) trx: TrxSysConfig,
    pub(crate) meta_buffer: Byte,
    pub(crate) index_buffer: Byte,
    pub(crate) index_swap_file: PathBuf,
    pub(crate) index_max_file_size: Byte,
    pub(crate) data_buffer: EvictableBufferPoolConfig,
    pub(crate) file: FileSystemConfig,
}

impl Default for EngineConfig {
    #[inline]
    fn default() -> Self {
        EngineConfig {
            storage_root: PathBuf::from("."),
            trx: TrxSysConfig::default(),
            meta_buffer: Byte::from_u64(DEFAULT_ENGINE_META_BUFFER as u64),
            index_buffer: Byte::from_u64(DEFAULT_ENGINE_INDEX_BUFFER as u64),
            index_swap_file: PathBuf::from(DEFAULT_ENGINE_INDEX_SWAP_FILE),
            index_max_file_size: Byte::from_u64(DEFAULT_ENGINE_INDEX_MAX_FILE_SIZE as u64),
            data_buffer: EvictableBufferPoolConfig::default(),
            file: FileSystemConfig::default(),
        }
    }
}

impl EngineConfig {
    #[inline]
    pub fn storage_root(mut self, storage_root: impl Into<PathBuf>) -> Self {
        self.storage_root = storage_root.into();
        self
    }

    #[inline]
    pub fn trx(mut self, trx: TrxSysConfig) -> Self {
        self.trx = trx;
        self
    }

    #[inline]
    pub fn meta_buffer(mut self, meta_buffer: impl Into<Byte>) -> Self {
        self.meta_buffer = meta_buffer.into();
        self
    }

    #[inline]
    pub fn index_buffer(mut self, index_buffer: impl Into<Byte>) -> Self {
        self.index_buffer = index_buffer.into();
        self
    }

    #[inline]
    pub fn index_swap_file(mut self, index_swap_file: impl Into<PathBuf>) -> Self {
        self.index_swap_file = index_swap_file.into();
        self
    }

    #[inline]
    pub fn index_max_file_size(mut self, index_max_file_size: impl Into<Byte>) -> Self {
        self.index_max_file_size = index_max_file_size.into();
        self
    }

    #[inline]
    pub fn data_buffer(mut self, data_buffer: EvictableBufferPoolConfig) -> Self {
        self.data_buffer = data_buffer;
        self
    }

    #[inline]
    pub fn file(mut self, file: FileSystemConfig) -> Self {
        self.file = file;
        self
    }

    #[inline]
    pub(crate) fn resolve_storage_paths(&self) -> Result<ResolvedStoragePaths> {
        ResolvedStoragePaths::resolve(StoragePathResolveInput {
            storage_root: &self.storage_root,
            data_dir: &self.file.data_dir,
            catalog_file_name: &self.file.catalog_file_name,
            log_dir: self.trx.log_dir_ref(),
            log_file_stem: self.trx.log_file_stem_ref(),
            log_partitions: self.trx.log_partitions,
            data_swap_file: self.data_buffer.data_swap_file_ref(),
            index_swap_file: &self.index_swap_file,
        })
    }
}

fn resolve_storage_root(storage_root: &Path) -> Result<PathBuf> {
    if storage_root.as_os_str().is_empty() {
        return Err(Error::InvalidStoragePath(
            "storage root must not be empty".into(),
        ));
    }
    let abs = if storage_root.is_absolute() {
        storage_root.to_path_buf()
    } else {
        std::env::current_dir()?.join(storage_root)
    };
    fs::create_dir_all(&abs)?;
    fs::canonicalize(abs).map_err(Into::into)
}

fn normalize_relative_dir(path: &Path, field: &str) -> Result<PathBuf> {
    normalize_relative_path(path, field, true)
}

fn normalize_relative_file_path(path: &Path, field: &str) -> Result<PathBuf> {
    let normalized = normalize_relative_path(path, field, false)?;
    if normalized.as_os_str().is_empty() {
        return Err(Error::InvalidStoragePath(format!(
            "{field} must resolve to a file path"
        )));
    }
    Ok(normalized)
}

fn normalize_relative_path(path: &Path, field: &str, allow_empty: bool) -> Result<PathBuf> {
    if path.is_absolute() {
        return Err(Error::InvalidStoragePath(format!(
            "{field} must be relative to storage_root"
        )));
    }
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::Normal(seg) => normalized.push(seg),
            Component::ParentDir => {
                return Err(Error::InvalidStoragePath(format!(
                    "{field} must not escape storage_root"
                )));
            }
            Component::RootDir | Component::Prefix(_) => {
                return Err(Error::InvalidStoragePath(format!(
                    "{field} must be relative to storage_root"
                )));
            }
        }
    }
    if !allow_empty && normalized.as_os_str().is_empty() {
        return Err(Error::InvalidStoragePath(format!(
            "{field} must not be empty"
        )));
    }
    Ok(normalized)
}

fn relative_dir_display(path: &Path, field: &str) -> Result<String> {
    if path.as_os_str().is_empty() {
        Ok(".".to_string())
    } else {
        Ok(path_to_utf8(path, field)?.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_resolve_storage_paths_default_layout() {
        let root = TempDir::new().unwrap();
        let paths = EngineConfig::default()
            .storage_root(root.path())
            .resolve_storage_paths()
            .unwrap();
        assert_eq!(paths.durable_layout.catalog_file_name, "catalog.mtb");
        assert!(paths.data_dir.starts_with(root.path()));
        assert!(paths.log_dir.starts_with(root.path()));
        assert!(paths.data_swap_file.ends_with("data.swp"));
        assert!(paths.index_swap_file.ends_with("index.swp"));
    }

    #[test]
    fn test_validate_swap_file_suffix_and_escape() {
        validate_swap_file_path_candidate("data_swap_file", "data.swp").unwrap();

        let err = validate_swap_file_path_candidate("data_swap_file", "data.bin").unwrap_err();
        assert!(matches!(err, Error::InvalidStoragePath(_)));

        let err = EngineConfig::default()
            .data_buffer(EvictableBufferPoolConfig::default().data_swap_file("../data.swp"))
            .resolve_storage_paths()
            .unwrap_err();
        assert!(matches!(err, Error::InvalidStoragePath(_)));
    }

    #[test]
    fn test_marker_rejects_durable_layout_change() {
        let root = TempDir::new().unwrap();
        let initial = EngineConfig::default()
            .storage_root(root.path())
            .resolve_storage_paths()
            .unwrap();
        initial.ensure_directories().unwrap();
        initial.validate_marker_if_present().unwrap();
        initial.persist_marker_if_missing().unwrap();

        let changed = EngineConfig::default()
            .storage_root(root.path())
            .file(FileSystemConfig::default().data_dir("data"))
            .resolve_storage_paths()
            .unwrap();
        changed.ensure_directories().unwrap();
        let err = changed.validate_marker_if_present().unwrap_err();
        assert!(matches!(err, Error::StorageLayoutMismatch(_)));
    }

    #[test]
    fn test_swap_files_reject_overlap() {
        let err = EngineConfig::default()
            .data_buffer(EvictableBufferPoolConfig::default().data_swap_file("shared.swp"))
            .index_swap_file("shared.swp")
            .resolve_storage_paths()
            .unwrap_err();
        assert!(matches!(err, Error::InvalidStoragePath(_)));
    }

    #[test]
    fn test_swap_files_reject_data_dir_aliases() {
        let err = EngineConfig::default()
            .file(FileSystemConfig::default().data_dir("data.swp"))
            .data_buffer(EvictableBufferPoolConfig::default().data_swap_file("data.swp"))
            .resolve_storage_paths()
            .unwrap_err();
        assert!(matches!(err, Error::InvalidStoragePath(_)));

        let err = EngineConfig::default()
            .file(FileSystemConfig::default().data_dir("data.swp"))
            .data_buffer(EvictableBufferPoolConfig::default().data_swap_file("data.swp/heap.swp"))
            .resolve_storage_paths()
            .unwrap_err();
        assert!(matches!(err, Error::InvalidStoragePath(_)));
    }

    #[test]
    fn test_swap_files_reject_log_dir_aliases() {
        let err = EngineConfig::default()
            .trx(TrxSysConfig::default().log_dir("log.swp"))
            .index_swap_file("log.swp")
            .resolve_storage_paths()
            .unwrap_err();
        assert!(matches!(err, Error::InvalidStoragePath(_)));

        let err = EngineConfig::default()
            .trx(TrxSysConfig::default().log_dir("log.swp"))
            .index_swap_file("log.swp/index.swp")
            .resolve_storage_paths()
            .unwrap_err();
        assert!(matches!(err, Error::InvalidStoragePath(_)));
    }

    #[test]
    fn test_swap_files_reject_reserved_file_descendants() {
        let err = EngineConfig::default()
            .data_buffer(
                EvictableBufferPoolConfig::default().data_swap_file("catalog.mtb/data.swp"),
            )
            .resolve_storage_paths()
            .unwrap_err();
        assert!(matches!(err, Error::InvalidStoragePath(_)));

        let err = EngineConfig::default()
            .index_swap_file(format!("{STORAGE_LAYOUT_FILE_NAME}/index.swp"))
            .resolve_storage_paths()
            .unwrap_err();
        assert!(matches!(err, Error::InvalidStoragePath(_)));

        let err = EngineConfig::default()
            .index_swap_file("redo.log/index.swp")
            .resolve_storage_paths()
            .unwrap_err();
        assert!(matches!(err, Error::InvalidStoragePath(_)));
    }
}
