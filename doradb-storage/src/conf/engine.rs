use crate::error::{ConfigError, ConfigResult, IoError, IoResult};
use byte_unit::Byte;
use error_stack::{Report, ResultExt};
use serde::{Deserialize, Serialize};
use std::env::current_dir;
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct DurableStorageLayout {
    version: u32,
    data_dir: String,
    catalog_file_name: String,
    log_dir: String,
    log_file_stem: String,
}

struct StoragePathResolveInput<'a> {
    storage_root: &'a Path,
    data_dir: &'a Path,
    catalog_file_name: &'a str,
    log_dir: &'a Path,
    log_file_stem: &'a str,
    data_swap_file: &'a Path,
    index_swap_file: &'a Path,
}

/// Absolute engine paths resolved from storage-root-relative configuration.
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
        if !validate_catalog_file_name(input.catalog_file_name) {
            return Err(
                Report::new(ConfigError::InvalidCatalogFileName).attach(format!(
                    "catalog file name must be a plain `.mtb` file name: {}",
                    input.catalog_file_name
                )),
            );
        }
        if !validate_log_file_stem(input.log_file_stem) {
            return Err(Report::new(ConfigError::InvalidLogFileStem).attach(format!(
                "log file stem must be a plain file name without glob characters: {}",
                input.log_file_stem
            )));
        }
        validate_swap_file_path_candidate(input.data_swap_file).attach_with(|| {
            format!("invalid data_swap_file: {}", input.data_swap_file.display())
        })?;
        validate_swap_file_path_candidate(input.index_swap_file).attach_with(|| {
            format!(
                "invalid index_swap_file: {}",
                input.index_swap_file.display()
            )
        })?;

        let storage_root = resolve_storage_root(input.storage_root)
            .attach_with(|| format!("invalid storage_root: {}", input.storage_root.display()))?;
        let data_dir_rel = normalize_relative_dir(input.data_dir)
            .attach_with(|| format!("invalid data_dir: {}", input.data_dir.display()))?;
        let log_dir_rel = normalize_relative_dir(input.log_dir)
            .attach_with(|| format!("invalid log_dir: {}", input.log_dir.display()))?;
        let data_swap_file_rel =
            normalize_relative_file_path(input.data_swap_file).attach_with(|| {
                format!("invalid data_swap_file: {}", input.data_swap_file.display())
            })?;
        let index_swap_file_rel =
            normalize_relative_file_path(input.index_swap_file).attach_with(|| {
                format!(
                    "invalid index_swap_file: {}",
                    input.index_swap_file.display()
                )
            })?;

        let data_dir = storage_root.join(&data_dir_rel);
        let catalog_file_path = data_dir.join(input.catalog_file_name);
        let log_dir = storage_root.join(&log_dir_rel);
        let data_swap_file = storage_root.join(&data_swap_file_rel);
        let index_swap_file = storage_root.join(&index_swap_file_rel);
        let durable_layout = DurableStorageLayout {
            version: STORAGE_LAYOUT_VERSION,
            data_dir: relative_dir_display(&data_dir_rel).attach("invalid normalized data_dir")?,
            catalog_file_name: input.catalog_file_name.to_string(),
            log_dir: relative_dir_display(&log_dir_rel).attach("invalid normalized log_dir")?,
            log_file_stem: input.log_file_stem.to_string(),
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

    /// Return the resolved table-data directory.
    pub(crate) fn data_dir_path(&self) -> &Path {
        &self.data_dir
    }

    /// Return the resolved redo-log directory.
    pub(crate) fn log_dir_path(&self) -> &Path {
        &self.log_dir
    }

    /// Return the resolved data-buffer swap-file path.
    pub(crate) fn data_swap_file_path(&self) -> &Path {
        &self.data_swap_file
    }

    /// Return the resolved index-buffer swap-file path.
    pub(crate) fn index_swap_file_path(&self) -> &Path {
        &self.index_swap_file
    }

    /// Ensure configured storage directories and swap-file parent directories exist.
    pub(crate) fn ensure_directories(&self) -> IoResult<()> {
        fs::create_dir_all(&self.storage_root).map_err(|err| {
            let kind = err.kind();
            Report::new(err)
                .change_context(IoError::from(kind))
                .attach(format!(
                    "create storage root directory: path={}",
                    self.storage_root.display()
                ))
        })?;
        fs::create_dir_all(&self.data_dir).map_err(|err| {
            let kind = err.kind();
            Report::new(err)
                .change_context(IoError::from(kind))
                .attach(format!(
                    "create table data directory: path={}",
                    self.data_dir.display()
                ))
        })?;
        fs::create_dir_all(&self.log_dir).map_err(|err| {
            let kind = err.kind();
            Report::new(err)
                .change_context(IoError::from(kind))
                .attach(format!(
                    "create redo log directory: path={}",
                    self.log_dir.display()
                ))
        })?;
        if let Some(parent) = self.data_swap_file.parent() {
            fs::create_dir_all(parent).map_err(|err| {
                let kind = err.kind();
                Report::new(err)
                    .change_context(IoError::from(kind))
                    .attach(format!(
                        "create data swap-file parent directory: path={}",
                        parent.display()
                    ))
            })?;
        }
        if let Some(parent) = self.index_swap_file.parent() {
            fs::create_dir_all(parent).map_err(|err| {
                let kind = err.kind();
                Report::new(err)
                    .change_context(IoError::from(kind))
                    .attach(format!(
                        "create index swap-file parent directory: path={}",
                        parent.display()
                    ))
            })?;
        }
        Ok(())
    }

    /// Validate the durable storage-layout marker and report whether it exists.
    ///
    /// This probe is not a cross-process synchronization boundary:
    /// `Path::exists` suppresses metadata errors, and another process can
    /// replace the marker before `validate_marker` reads it. Until
    /// `docs/backlogs/000162-single-process-storage-bootstrap-lock.md` is
    /// implemented, callers must provide exclusive storage-root ownership.
    pub(crate) fn validate_marker_if_present(&self) -> ConfigResult<bool> {
        let marker_path = self.marker_path();
        if !marker_path.exists() {
            return Ok(false);
        }
        self.validate_marker(&marker_path)?;
        Ok(true)
    }

    /// Persist the durable storage-layout marker using exclusive creation.
    pub(crate) fn persist_marker(&self) -> IoResult<()> {
        let marker_path = self.marker_path();
        let marker = self.serialize_durable_layout()?;
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&marker_path)
        {
            Ok(mut file) => {
                file.write_all(marker.as_bytes()).map_err(|err| {
                    let kind = err.kind();
                    Report::new(err)
                        .change_context(IoError::from(kind))
                        .attach(format!(
                            "write durable storage layout marker: path={}",
                            marker_path.display()
                        ))
                })?;
                Ok(())
            }
            Err(err) => {
                let kind = err.kind();
                Err(Report::new(err)
                    .change_context(IoError::from(kind))
                    .attach(format!(
                        "create durable storage layout marker: path={}",
                        marker_path.display()
                    )))
            }
        }
    }

    #[inline]
    fn serialize_durable_layout(&self) -> IoResult<String> {
        toml::to_string(&self.durable_layout).map_err(|err| {
            Report::new(err)
                .change_context(IoError::from(ErrorKind::InvalidData))
                .attach("serialize durable storage layout marker")
        })
    }

    fn marker_path(&self) -> PathBuf {
        self.storage_root.join(STORAGE_LAYOUT_FILE_NAME)
    }

    fn validate_marker(&self, marker_path: &Path) -> ConfigResult<()> {
        let raw = fs::read_to_string(marker_path).map_err(|err| {
            Report::new(IoError::from(err.kind()))
                .attach(format!("{err}"))
                .change_context(ConfigError::StorageLayoutMarkerRead)
                .attach(format!(
                    "read durable storage layout marker: {}",
                    marker_path.display()
                ))
        })?;
        let actual = parse_durable_layout(&raw)?;
        validate_durable_layout_match(&self.durable_layout, &actual)?;
        Ok(())
    }

    fn validate_non_overlapping_paths(&self) -> ConfigResult<()> {
        if self.data_swap_file == self.index_swap_file {
            return Err(
                Report::new(ConfigError::PathsMustNotOverlap).attach(format!(
                    "swap files must not overlap each other: {}",
                    self.data_swap_file.display()
                )),
            );
        }
        self.validate_swap_file_non_overlap("data_swap_file", &self.data_swap_file)?;
        self.validate_swap_file_non_overlap("index_swap_file", &self.index_swap_file)?;
        Ok(())
    }

    fn log_family_base_path(&self) -> PathBuf {
        self.log_dir.join(&self.log_file_stem)
    }

    fn validate_swap_file_non_overlap(&self, field: &str, path: &Path) -> ConfigResult<()> {
        if path == self.data_dir {
            return Err(
                Report::new(ConfigError::PathMustNotOverlapReservedLocation).attach(format!(
                    "{field} must not overlap data_dir: {}",
                    path.display()
                )),
            );
        }
        if path == self.log_dir {
            return Err(
                Report::new(ConfigError::PathMustNotOverlapReservedLocation).attach(format!(
                    "{field} must not overlap log_dir: {}",
                    path.display()
                )),
            );
        }
        if self.data_dir != self.storage_root && path.parent() == Some(self.data_dir.as_path()) {
            return Err(
                Report::new(ConfigError::PathMustNotUseReservedParentDirectory).attach(format!(
                    "{field} must not use data_dir as its parent directory: {}",
                    path.display()
                )),
            );
        }
        if self.log_dir != self.storage_root && path.parent() == Some(self.log_dir.as_path()) {
            return Err(
                Report::new(ConfigError::PathMustNotUseReservedParentDirectory).attach(format!(
                    "{field} must not use log_dir as its parent directory: {}",
                    path.display()
                )),
            );
        }
        if aliases_reserved_path(path, &self.catalog_file_path) {
            return Err(
                Report::new(ConfigError::PathMustNotOverlapReservedLocation).attach(format!(
                    "{field} must not overlap catalog file: {}",
                    path.display()
                )),
            );
        }
        let marker_path = self.marker_path();
        if aliases_reserved_path(path, &marker_path) {
            return Err(
                Report::new(ConfigError::PathMustNotOverlapReservedLocation).attach(format!(
                    "{field} must not overlap storage marker: {}",
                    path.display()
                )),
            );
        }
        if path
            .extension()
            .and_then(|ext| ext.to_str())
            .is_some_and(|ext| ext.eq_ignore_ascii_case("tbl") || ext.eq_ignore_ascii_case("mtb"))
        {
            return Err(
                Report::new(ConfigError::PathMustNotUseDurableStorageSuffix).attach(format!(
                    "{field} must not use durable storage suffix `.tbl` or `.mtb`: {}",
                    path.display()
                )),
            );
        }
        let log_family_base = self.log_family_base_path();
        if aliases_reserved_path(path, &log_family_base) {
            return Err(
                Report::new(ConfigError::PathMustNotOverlapReservedLocation).attach(format!(
                    "{field} must not overlap redo log family: {}",
                    path.display()
                )),
            );
        }
        let same_parent = path.parent() == Some(self.log_dir.as_path());
        let swap_file_name = path_to_utf8(Path::new(
            path.file_name()
                .expect("swap file must resolve to a file path"),
        ))
        .attach_with(|| format!("invalid {field}: {}", path.display()))?;
        if same_parent && swap_file_name.starts_with(&format!("{}.", self.log_file_stem)) {
            return Err(
                Report::new(ConfigError::PathMustNotOverlapReservedLocation).attach(format!(
                    "{field} must not overlap redo log family: {}",
                    path.display()
                )),
            );
        }
        Ok(())
    }
}

/// Storage-engine configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineConfig {
    /// Root directory for all storage-engine files.
    pub(crate) storage_root: PathBuf,
    /// Transaction-system configuration.
    pub(crate) trx: TrxSysConfig,
    /// Metadata buffer-pool size.
    pub(crate) meta_buffer: Byte,
    /// Index buffer-pool memory size.
    pub(crate) index_buffer: Byte,
    /// Index-buffer swap-file path relative to `storage_root`.
    pub(crate) index_swap_file: PathBuf,
    /// Maximum size allowed for the index-buffer swap file.
    pub(crate) index_max_file_size: Byte,
    /// Data buffer-pool configuration.
    pub(crate) data_buffer: EvictableBufferPoolConfig,
    /// Table and catalog file-system configuration.
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
    /// Set the root directory for storage-engine files.
    #[inline]
    pub fn storage_root(mut self, storage_root: impl Into<PathBuf>) -> Self {
        self.storage_root = storage_root.into();
        self
    }

    /// Set the transaction-system configuration.
    #[inline]
    pub fn trx(mut self, trx: TrxSysConfig) -> Self {
        self.trx = trx;
        self
    }

    /// Set the metadata buffer-pool size.
    #[inline]
    pub fn meta_buffer(mut self, meta_buffer: impl Into<Byte>) -> Self {
        self.meta_buffer = meta_buffer.into();
        self
    }

    /// Set the index buffer-pool memory size.
    #[inline]
    pub fn index_buffer(mut self, index_buffer: impl Into<Byte>) -> Self {
        self.index_buffer = index_buffer.into();
        self
    }

    /// Set the index-buffer swap-file path relative to `storage_root`.
    #[inline]
    pub fn index_swap_file(mut self, index_swap_file: impl Into<PathBuf>) -> Self {
        self.index_swap_file = index_swap_file.into();
        self
    }

    /// Set the maximum size allowed for the index-buffer swap file.
    #[inline]
    pub fn index_max_file_size(mut self, index_max_file_size: impl Into<Byte>) -> Self {
        self.index_max_file_size = index_max_file_size.into();
        self
    }

    /// Set the data buffer-pool configuration.
    #[inline]
    pub fn data_buffer(mut self, data_buffer: EvictableBufferPoolConfig) -> Self {
        self.data_buffer = data_buffer;
        self
    }

    /// Set the table and catalog file-system configuration.
    #[inline]
    pub fn file(mut self, file: FileSystemConfig) -> Self {
        self.file = file;
        self
    }

    /// Resolve and validate storage paths for engine startup.
    #[inline]
    pub(crate) fn resolve_storage_paths(&self) -> ConfigResult<ResolvedStoragePaths> {
        ResolvedStoragePaths::resolve(StoragePathResolveInput {
            storage_root: &self.storage_root,
            data_dir: &self.file.data_dir,
            catalog_file_name: &self.file.catalog_file_name,
            log_dir: self.trx.log_dir_ref(),
            log_file_stem: self.trx.log_file_stem_ref(),
            data_swap_file: self.data_buffer.data_swap_file_ref(),
            index_swap_file: &self.index_swap_file,
        })
    }
}

fn aliases_reserved_path(path: &Path, reserved: &Path) -> bool {
    path == reserved || path.starts_with(reserved) || reserved.starts_with(path)
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
    if actual != expected {
        return Err(Report::new(ConfigError::StorageLayoutMismatch)
            .attach(format!("expected {:?}, found {:?}", expected, actual)));
    }
    Ok(())
}

fn resolve_storage_root(storage_root: &Path) -> ConfigResult<PathBuf> {
    if storage_root.as_os_str().is_empty() {
        return Err(
            Report::new(ConfigError::PathMustNotBeEmpty).attach("storage_root must not be empty")
        );
    }
    if storage_root.is_absolute() {
        return Ok(storage_root.to_path_buf());
    }
    current_dir()
        .map(|cwd| cwd.join(storage_root))
        .map_err(|err| {
            Report::new(ConfigError::PathMustBeRelativeToStorageRoot).attach(format!(
                "failed to resolve storage_root `{}` against current directory: {err}",
                storage_root.display()
            ))
        })
}

fn normalize_relative_dir(path: &Path) -> ConfigResult<PathBuf> {
    normalize_relative_path(path, true)
}

fn normalize_relative_file_path(path: &Path) -> ConfigResult<PathBuf> {
    let normalized = normalize_relative_path(path, false)
        .attach_with(|| format!("invalid relative file path: {}", path.display()))?;
    if normalized.as_os_str().is_empty() {
        return Err(Report::new(ConfigError::PathMustResolveToFile)
            .attach(format!("path must resolve to a file: {}", path.display())));
    }
    Ok(normalized)
}

fn normalize_relative_path(path: &Path, allow_empty: bool) -> ConfigResult<PathBuf> {
    if path.is_absolute() {
        return Err(
            Report::new(ConfigError::PathMustBeRelativeToStorageRoot).attach(format!(
                "path must be relative to storage_root: {}",
                path.display()
            )),
        );
    }
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::Normal(seg) => normalized.push(seg),
            Component::ParentDir => {
                return Err(
                    Report::new(ConfigError::PathMustNotEscapeStorageRoot).attach(format!(
                        "path must not escape storage_root: {}",
                        path.display()
                    )),
                );
            }
            Component::RootDir | Component::Prefix(_) => {
                return Err(
                    Report::new(ConfigError::PathMustBeRelativeToStorageRoot).attach(format!(
                        "path must be relative to storage_root: {}",
                        path.display()
                    )),
                );
            }
        }
    }
    if !allow_empty && normalized.as_os_str().is_empty() {
        return Err(Report::new(ConfigError::PathMustNotBeEmpty)
            .attach(format!("path must not be empty: {}", path.display())));
    }
    Ok(normalized)
}

fn relative_dir_display(path: &Path) -> ConfigResult<String> {
    if path.as_os_str().is_empty() {
        Ok(".".to_string())
    } else {
        Ok(path_to_utf8(path)
            .attach_with(|| format!("invalid relative directory: {}", path.display()))?
            .to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use error_stack::Report;
    use std::fs::{create_dir, write};
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
        validate_swap_file_path_candidate("data.swp").unwrap();

        let err = validate_swap_file_path_candidate("").unwrap_err();
        assert_config_report(err, ConfigError::PathMustNotBeEmpty, &["must not be empty"]);

        let err = validate_swap_file_path_candidate("data.bin").unwrap_err();
        assert_config_report(
            err,
            ConfigError::PathMustUseRequiredSuffix,
            &[".swp", "data.bin"],
        );

        let err = EngineConfig::default()
            .data_buffer(EvictableBufferPoolConfig::default().data_swap_file("data.bin"))
            .resolve_storage_paths()
            .unwrap_err();
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
        assert!(!initial.validate_marker_if_present().unwrap());
        initial.persist_marker().unwrap();
        assert!(initial.validate_marker_if_present().unwrap());

        let changed = EngineConfig::default()
            .storage_root(root.path())
            .file(FileSystemConfig::default().data_dir("data"))
            .resolve_storage_paths()
            .unwrap();
        changed.ensure_directories().unwrap();
        let err = changed.validate_marker_if_present().unwrap_err();
        assert_eq!(err.current_context(), &ConfigError::StorageLayoutMismatch);
    }

    #[test]
    fn test_serialize_durable_layout_is_io_typed() {
        let root = TempDir::new().unwrap();
        let paths = EngineConfig::default()
            .storage_root(root.path())
            .resolve_storage_paths()
            .unwrap();

        let marker: IoResult<String> = paths.serialize_durable_layout();
        let actual = toml::from_str::<DurableStorageLayout>(&marker.unwrap()).unwrap();
        assert_eq!(actual, paths.durable_layout);
    }

    #[test]
    fn test_ensure_directories_is_io_typed() {
        let root = TempDir::new().unwrap();
        let storage_root = root.path().join("storage-root-file");
        write(&storage_root, "not a directory").unwrap();
        let paths = EngineConfig::default()
            .storage_root(&storage_root)
            .resolve_storage_paths()
            .unwrap();

        let err: Report<IoError> = paths.ensure_directories().unwrap_err();
        assert_eq!(err.current_context().kind(), ErrorKind::AlreadyExists);
        let report = format!("{err:?}");
        assert!(report.contains("create storage root directory"), "{report}");
        assert!(report.contains(storage_root.to_str().unwrap()), "{report}");
    }

    #[test]
    fn test_persist_marker_reports_existing_marker_as_io() {
        let root = TempDir::new().unwrap();
        let paths = EngineConfig::default()
            .storage_root(root.path())
            .resolve_storage_paths()
            .unwrap();
        paths.ensure_directories().unwrap();
        assert!(!paths.validate_marker_if_present().unwrap());
        write(
            paths.marker_path(),
            paths.serialize_durable_layout().unwrap(),
        )
        .unwrap();

        let err: Report<IoError> = paths.persist_marker().unwrap_err();
        assert_eq!(err.current_context().kind(), ErrorKind::AlreadyExists);
        let report = format!("{err:?}");
        assert!(
            report.contains("create durable storage layout marker"),
            "{report}"
        );
        assert!(
            report.contains(paths.marker_path().to_str().unwrap()),
            "{report}"
        );
    }

    #[test]
    fn test_marker_read_failure_retains_io_beneath_config() {
        let root = TempDir::new().unwrap();
        let paths = EngineConfig::default()
            .storage_root(root.path())
            .resolve_storage_paths()
            .unwrap();
        create_dir(paths.marker_path()).unwrap();

        let err = paths.validate_marker_if_present().unwrap_err();
        assert_eq!(err.current_context(), &ConfigError::StorageLayoutMarkerRead);
        assert_eq!(
            err.downcast_ref::<IoError>().copied().map(IoError::kind),
            Some(std::io::ErrorKind::IsADirectory)
        );
        assert!(
            format!("{err:?}").contains(paths.marker_path().to_str().unwrap()),
            "read failure must identify the marker path"
        );
    }

    #[test]
    fn test_marker_rejects_malformed_toml() {
        let root = TempDir::new().unwrap();
        let paths = EngineConfig::default()
            .storage_root(root.path())
            .resolve_storage_paths()
            .unwrap();
        write(paths.marker_path(), "version = [").unwrap();

        let err = paths.validate_marker_if_present().unwrap_err();
        assert_eq!(
            err.current_context(),
            &ConfigError::InvalidStorageLayoutMarker
        );
    }

    #[test]
    fn test_marker_rejects_v1_layout_with_log_partitions() {
        let root = TempDir::new().unwrap();
        let paths = EngineConfig::default()
            .storage_root(root.path())
            .resolve_storage_paths()
            .unwrap();
        paths.ensure_directories().unwrap();
        // Keep the removed v1 field in this fixture to prove old partitioned
        // storage-layout markers are rejected by the version bump.
        write(
            paths.marker_path(),
            r#"version = 1
data_dir = "."
catalog_file_name = "catalog.mtb"
log_dir = "."
log_file_stem = "redo.log"
log_partitions = 1
"#,
        )
        .unwrap();

        let err = paths.validate_marker_if_present().unwrap_err();
        assert_eq!(err.current_context(), &ConfigError::StorageLayoutMismatch);
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
