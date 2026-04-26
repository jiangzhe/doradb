use crate::error::{ConfigError, ConfigResult, Error, Result};
use byte_unit::Byte;
use error_stack::{Report, ResultExt, ensure};
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
    fn resolve(input: StoragePathResolveInput<'_>) -> ConfigResult<Self> {
        (|| {
            ensure!(
                validate_catalog_file_name(input.catalog_file_name),
                ConfigError::InvalidCatalogFileName
            );
            Ok(())
        })()
        .attach_with(|| {
            format!(
                "catalog file name must be a plain `.mtb` file name: {}",
                input.catalog_file_name
            )
        })?;
        (|| {
            ensure!(
                validate_log_file_stem(input.log_file_stem),
                ConfigError::InvalidLogFileStem
            );
            Ok(())
        })()
        .attach_with(|| {
            format!(
                "log file stem must be a plain file name without glob characters: {}",
                input.log_file_stem
            )
        })?;
        (|| {
            ensure!(
                input.log_partitions > 0 && input.log_partitions <= MAX_LOG_PARTITIONS,
                ConfigError::InvalidLogPartitions
            );
            Ok(())
        })()
        .attach_with(|| {
            format!(
                "log partitions must be in 1..={MAX_LOG_PARTITIONS}: {}",
                input.log_partitions
            )
        })?;
        validate_swap_file_path_candidate("data_swap_file", input.data_swap_file).attach_with(
            || format!("invalid data_swap_file: {}", input.data_swap_file.display()),
        )?;
        validate_swap_file_path_candidate("index_swap_file", input.index_swap_file).attach_with(
            || {
                format!(
                    "invalid index_swap_file: {}",
                    input.index_swap_file.display()
                )
            },
        )?;

        let storage_root = resolve_storage_root(input.storage_root)
            .attach_with(|| format!("invalid storage_root: {}", input.storage_root.display()))?;
        let data_dir_rel = normalize_relative_dir(input.data_dir, "data_dir")
            .attach_with(|| format!("invalid data_dir: {}", input.data_dir.display()))?;
        let log_dir_rel = normalize_relative_dir(input.log_dir, "log_dir")
            .attach_with(|| format!("invalid log_dir: {}", input.log_dir.display()))?;
        let data_swap_file_rel =
            normalize_relative_file_path(input.data_swap_file, "data_swap_file").attach_with(
                || format!("invalid data_swap_file: {}", input.data_swap_file.display()),
            )?;
        let index_swap_file_rel =
            normalize_relative_file_path(input.index_swap_file, "index_swap_file").attach_with(
                || {
                    format!(
                        "invalid index_swap_file: {}",
                        input.index_swap_file.display()
                    )
                },
            )?;

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
        let marker = toml::to_string(&self.durable_layout).map_err(|_| Error::internal())?;
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
        let actual = parse_durable_layout(&raw).map_err(Error::from)?;
        validate_durable_layout_match(&self.durable_layout, &actual).map_err(Error::from)?;
        Ok(())
    }

    fn validate_non_overlapping_paths(&self) -> ConfigResult<()> {
        (|| {
            ensure!(
                self.data_swap_file != self.index_swap_file,
                ConfigError::PathsMustNotOverlap
            );
            Ok(())
        })()
        .attach_with(|| {
            format!(
                "swap files must not overlap each other: {}",
                self.data_swap_file.display()
            )
        })?;
        self.validate_swap_file_non_overlap("data_swap_file", &self.data_swap_file)?;
        self.validate_swap_file_non_overlap("index_swap_file", &self.index_swap_file)?;
        Ok(())
    }

    fn log_family_base_path(&self) -> PathBuf {
        self.log_dir.join(&self.log_file_stem)
    }

    fn validate_swap_file_non_overlap(&self, field: &str, path: &Path) -> ConfigResult<()> {
        (|| {
            ensure!(
                path != self.data_dir,
                ConfigError::PathMustNotOverlapReservedLocation
            );
            Ok(())
        })()
        .attach_with(|| format!("{field} must not overlap data_dir: {}", path.display()))?;
        (|| {
            ensure!(
                path != self.log_dir,
                ConfigError::PathMustNotOverlapReservedLocation
            );
            Ok(())
        })()
        .attach_with(|| format!("{field} must not overlap log_dir: {}", path.display()))?;
        (|| {
            ensure!(
                self.data_dir == self.storage_root
                    || path.parent() != Some(self.data_dir.as_path()),
                ConfigError::PathMustNotUseReservedParentDirectory
            );
            Ok(())
        })()
        .attach_with(|| {
            format!(
                "{field} must not use data_dir as its parent directory: {}",
                path.display()
            )
        })?;
        (|| {
            ensure!(
                self.log_dir == self.storage_root || path.parent() != Some(self.log_dir.as_path()),
                ConfigError::PathMustNotUseReservedParentDirectory
            );
            Ok(())
        })()
        .attach_with(|| {
            format!(
                "{field} must not use log_dir as its parent directory: {}",
                path.display()
            )
        })?;
        (|| {
            ensure!(
                !aliases_reserved_path(path, &self.catalog_file_path),
                ConfigError::PathMustNotOverlapReservedLocation
            );
            Ok(())
        })()
        .attach_with(|| format!("{field} must not overlap catalog file: {}", path.display()))?;
        let marker_path = self.marker_path();
        (|| {
            ensure!(
                !aliases_reserved_path(path, &marker_path),
                ConfigError::PathMustNotOverlapReservedLocation
            );
            Ok(())
        })()
        .attach_with(|| {
            format!(
                "{field} must not overlap storage marker: {}",
                path.display()
            )
        })?;
        (|| {
            ensure!(
                !path
                    .extension()
                    .and_then(|ext| ext.to_str())
                    .is_some_and(
                        |ext| ext.eq_ignore_ascii_case("tbl") || ext.eq_ignore_ascii_case("mtb")
                    ),
                ConfigError::PathMustNotUseDurableStorageSuffix
            );
            Ok(())
        })()
        .attach_with(|| {
            format!(
                "{field} must not use durable storage suffix `.tbl` or `.mtb`: {}",
                path.display()
            )
        })?;
        let log_family_base = self.log_family_base_path();
        (|| {
            ensure!(
                !aliases_reserved_path(path, &log_family_base),
                ConfigError::PathMustNotOverlapReservedLocation
            );
            Ok(())
        })()
        .attach_with(|| {
            format!(
                "{field} must not overlap redo log family: {}",
                path.display()
            )
        })?;
        let same_parent = path.parent() == Some(self.log_dir.as_path());
        let swap_file_name = path_to_utf8(
            Path::new(
                path.file_name()
                    .expect("swap file must resolve to a file path"),
            ),
            field,
        )
        .attach_with(|| format!("invalid {field}: {}", path.display()))?;
        (|| {
            ensure!(
                !same_parent || !swap_file_name.starts_with(&format!("{}.", self.log_file_stem)),
                ConfigError::PathMustNotOverlapReservedLocation
            );
            Ok(())
        })()
        .attach_with(|| {
            format!(
                "{field} must not overlap redo log family: {}",
                path.display()
            )
        })?;
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
    pub(crate) fn resolve_storage_paths(&self) -> ConfigResult<ResolvedStoragePaths> {
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

fn parse_durable_layout(raw: &str) -> ConfigResult<DurableStorageLayout> {
    toml::from_str::<DurableStorageLayout>(raw).map_err(|err| {
        Report::new(ConfigError::InvalidStorageLayoutMarker)
            .attach(format!("invalid storage-layout.toml: {err}"))
    })
}

fn validate_durable_layout_match(
    expected: &DurableStorageLayout,
    actual: &DurableStorageLayout,
) -> ConfigResult<()> {
    (|| {
        ensure!(actual == expected, ConfigError::StorageLayoutMismatch);
        Ok(())
    })()
    .attach_with(|| format!("expected {:?}, found {:?}", expected, actual))?;
    Ok(())
}

fn resolve_storage_root(storage_root: &Path) -> ConfigResult<PathBuf> {
    (|| {
        ensure!(
            !storage_root.as_os_str().is_empty(),
            ConfigError::PathMustNotBeEmpty
        );
        Ok(())
    })()
    .attach_with(|| "storage_root must not be empty".to_string())?;
    if storage_root.is_absolute() {
        return Ok(storage_root.to_path_buf());
    }
    std::env::current_dir()
        .map(|cwd| cwd.join(storage_root))
        .map_err(|err| {
            Report::new(ConfigError::PathMustBeRelativeToStorageRoot).attach(format!(
                "failed to resolve storage_root `{}` against current directory: {err}",
                storage_root.display()
            ))
        })
}

fn normalize_relative_dir(path: &Path, field: &str) -> ConfigResult<PathBuf> {
    normalize_relative_path(path, field, true)
}

fn normalize_relative_file_path(path: &Path, field: &str) -> ConfigResult<PathBuf> {
    let normalized = normalize_relative_path(path, field, false)
        .attach_with(|| format!("invalid {field}: {}", path.display()))?;
    (|| {
        ensure!(
            !normalized.as_os_str().is_empty(),
            ConfigError::PathMustResolveToFile
        );
        Ok(())
    })()
    .attach_with(|| format!("{field} must resolve to a file path"))?;
    Ok(normalized)
}

fn normalize_relative_path(path: &Path, field: &str, allow_empty: bool) -> ConfigResult<PathBuf> {
    (|| {
        ensure!(
            !path.is_absolute(),
            ConfigError::PathMustBeRelativeToStorageRoot
        );
        Ok(())
    })()
    .attach_with(|| format!("{field} must be relative to storage_root"))?;
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::Normal(seg) => normalized.push(seg),
            Component::ParentDir => {
                return Err(Report::new(ConfigError::PathMustNotEscapeStorageRoot)
                    .attach(format!("{field} must not escape storage_root")));
            }
            Component::RootDir | Component::Prefix(_) => {
                return Err(Report::new(ConfigError::PathMustBeRelativeToStorageRoot)
                    .attach(format!("{field} must be relative to storage_root")));
            }
        }
    }
    (|| {
        ensure!(
            allow_empty || !normalized.as_os_str().is_empty(),
            ConfigError::PathMustNotBeEmpty
        );
        Ok(())
    })()
    .attach_with(|| format!("{field} must not be empty"))?;
    Ok(normalized)
}

fn relative_dir_display(path: &Path, field: &str) -> ConfigResult<String> {
    if path.as_os_str().is_empty() {
        Ok(".".to_string())
    } else {
        Ok(path_to_utf8(path, field)
            .attach_with(|| format!("invalid {field}: {}", path.display()))?
            .to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorKind;
    use error_stack::Report;
    use tempfile::TempDir;

    fn assert_config_report(err: Report<ConfigError>, expected: ConfigError, snippets: &[&str]) {
        assert_eq!(err.current_context(), &expected);
        let debug = format!("{err:?}");
        for snippet in snippets {
            assert!(debug.contains(snippet), "missing `{snippet}` in {debug}");
        }
    }

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
        assert_config_report(
            err,
            ConfigError::PathMustUseRequiredSuffix,
            &["data_swap_file", ".swp", "data.bin"],
        );

        let err = EngineConfig::default()
            .data_buffer(EvictableBufferPoolConfig::default().data_swap_file("../data.swp"))
            .resolve_storage_paths()
            .unwrap_err();
        assert_config_report(
            err,
            ConfigError::PathMustNotEscapeStorageRoot,
            &["data_swap_file", "storage_root"],
        );
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
        assert!(err.is_kind(ErrorKind::Config));
        assert_eq!(
            err.report().downcast_ref::<ConfigError>().copied(),
            Some(ConfigError::StorageLayoutMismatch)
        );
    }

    #[test]
    fn test_swap_files_reject_overlap() {
        let err = EngineConfig::default()
            .data_buffer(EvictableBufferPoolConfig::default().data_swap_file("shared.swp"))
            .index_swap_file("shared.swp")
            .resolve_storage_paths()
            .unwrap_err();
        assert_config_report(err, ConfigError::PathsMustNotOverlap, &["shared.swp"]);
    }

    #[test]
    fn test_swap_files_reject_data_dir_aliases() {
        let err = EngineConfig::default()
            .file(FileSystemConfig::default().data_dir("data.swp"))
            .data_buffer(EvictableBufferPoolConfig::default().data_swap_file("data.swp"))
            .resolve_storage_paths()
            .unwrap_err();
        assert_config_report(
            err,
            ConfigError::PathMustNotOverlapReservedLocation,
            &["data_swap_file", "data_dir"],
        );

        let err = EngineConfig::default()
            .file(FileSystemConfig::default().data_dir("data.swp"))
            .data_buffer(EvictableBufferPoolConfig::default().data_swap_file("data.swp/heap.swp"))
            .resolve_storage_paths()
            .unwrap_err();
        assert_config_report(
            err,
            ConfigError::PathMustNotUseReservedParentDirectory,
            &["data_swap_file", "data_dir"],
        );
    }

    #[test]
    fn test_swap_files_reject_log_dir_aliases() {
        let err = EngineConfig::default()
            .trx(TrxSysConfig::default().log_dir("log.swp"))
            .index_swap_file("log.swp")
            .resolve_storage_paths()
            .unwrap_err();
        assert_config_report(
            err,
            ConfigError::PathMustNotOverlapReservedLocation,
            &["index_swap_file", "log_dir"],
        );

        let err = EngineConfig::default()
            .trx(TrxSysConfig::default().log_dir("log.swp"))
            .index_swap_file("log.swp/index.swp")
            .resolve_storage_paths()
            .unwrap_err();
        assert_config_report(
            err,
            ConfigError::PathMustNotUseReservedParentDirectory,
            &["index_swap_file", "log_dir"],
        );
    }

    #[test]
    fn test_swap_files_reject_reserved_file_descendants() {
        let err = EngineConfig::default()
            .data_buffer(
                EvictableBufferPoolConfig::default().data_swap_file("catalog.mtb/data.swp"),
            )
            .resolve_storage_paths()
            .unwrap_err();
        assert_config_report(
            err,
            ConfigError::PathMustNotOverlapReservedLocation,
            &["data_swap_file", "catalog file"],
        );

        let err = EngineConfig::default()
            .index_swap_file(format!("{STORAGE_LAYOUT_FILE_NAME}/index.swp"))
            .resolve_storage_paths()
            .unwrap_err();
        assert_config_report(
            err,
            ConfigError::PathMustNotOverlapReservedLocation,
            &["index_swap_file", "storage marker"],
        );

        let err = EngineConfig::default()
            .index_swap_file("redo.log/index.swp")
            .resolve_storage_paths()
            .unwrap_err();
        assert_config_report(
            err,
            ConfigError::PathMustNotOverlapReservedLocation,
            &["index_swap_file", "redo log family"],
        );
    }
}
