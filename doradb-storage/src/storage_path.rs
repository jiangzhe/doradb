use crate::error::{Error, Result};
use crate::trx::sys_conf::MAX_LOG_PARTITIONS;
use serde::{Deserialize, Serialize};
use std::fs::{self, OpenOptions};
use std::io::{ErrorKind, Write};
use std::path::{Component, Path, PathBuf};

pub(crate) const STORAGE_LAYOUT_FILE_NAME: &str = "storage-layout.toml";
const STORAGE_LAYOUT_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct DurableStorageLayout {
    version: u32,
    data_dir: String,
    catalog_file_name: String,
    log_dir: String,
    log_file_stem: String,
    log_partitions: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct ResolvedStoragePaths {
    storage_root: PathBuf,
    data_dir: PathBuf,
    catalog_file_path: PathBuf,
    log_dir: PathBuf,
    log_file_stem: String,
    data_swap_file: PathBuf,
    durable_layout: DurableStorageLayout,
}

impl ResolvedStoragePaths {
    pub(crate) fn resolve(
        storage_root: &Path,
        data_dir: &Path,
        catalog_file_name: &str,
        log_dir: &Path,
        log_file_stem: &str,
        log_partitions: usize,
        data_swap_file: &Path,
    ) -> Result<Self> {
        if !validate_catalog_file_name(catalog_file_name) {
            return Err(Error::InvalidStoragePath(format!(
                "catalog file name must be a plain `.mtb` file name: {catalog_file_name}"
            )));
        }
        if !validate_log_file_stem(log_file_stem) {
            return Err(Error::InvalidStoragePath(format!(
                "log file stem must be a plain file name without glob characters: {log_file_stem}"
            )));
        }
        if log_partitions == 0 || log_partitions > MAX_LOG_PARTITIONS {
            return Err(Error::InvalidStoragePath(format!(
                "log partitions must be in 1..={MAX_LOG_PARTITIONS}: {log_partitions}"
            )));
        }
        validate_swap_file_path_candidate(data_swap_file)?;

        let storage_root = resolve_storage_root(storage_root)?;
        let data_dir_rel = normalize_relative_dir(data_dir, "data_dir")?;
        let log_dir_rel = normalize_relative_dir(log_dir, "log_dir")?;
        let data_swap_file_rel = normalize_relative_file_path(data_swap_file, "data_swap_file")?;

        let data_dir = storage_root.join(&data_dir_rel);
        let catalog_file_path = data_dir.join(catalog_file_name);
        let log_dir = storage_root.join(&log_dir_rel);
        let data_swap_file = storage_root.join(&data_swap_file_rel);
        let durable_layout = DurableStorageLayout {
            version: STORAGE_LAYOUT_VERSION,
            data_dir: relative_dir_display(&data_dir_rel, "data_dir")?,
            catalog_file_name: catalog_file_name.to_string(),
            log_dir: relative_dir_display(&log_dir_rel, "log_dir")?,
            log_file_stem: log_file_stem.to_string(),
            log_partitions,
        };
        let res = ResolvedStoragePaths {
            storage_root,
            data_dir,
            catalog_file_path,
            log_dir,
            log_file_stem: log_file_stem.to_string(),
            data_swap_file,
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

    pub(crate) fn ensure_directories(&self) -> Result<()> {
        fs::create_dir_all(&self.storage_root)?;
        fs::create_dir_all(&self.data_dir)?;
        fs::create_dir_all(&self.log_dir)?;
        if let Some(parent) = self.data_swap_file.parent() {
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
        if self.data_swap_file == self.catalog_file_path {
            return Err(Error::InvalidStoragePath(format!(
                "data swap file must not overlap catalog file: {}",
                self.data_swap_file.display()
            )));
        }
        if self.data_swap_file == self.marker_path() {
            return Err(Error::InvalidStoragePath(format!(
                "data swap file must not overlap storage marker: {}",
                self.data_swap_file.display()
            )));
        }
        if self
            .data_swap_file
            .extension()
            .and_then(|ext| ext.to_str())
            .is_some_and(|ext| ext.eq_ignore_ascii_case("tbl") || ext.eq_ignore_ascii_case("mtb"))
        {
            return Err(Error::InvalidStoragePath(format!(
                "data swap file must not use durable storage suffix `.tbl` or `.mtb`: {}",
                self.data_swap_file.display()
            )));
        }
        let log_family_base = self.log_family_base_path();
        if self.data_swap_file == log_family_base {
            return Err(Error::InvalidStoragePath(format!(
                "data swap file must not overlap redo log family: {}",
                self.data_swap_file.display()
            )));
        }
        let same_parent = self.data_swap_file.parent() == Some(self.log_dir.as_path());
        let swap_file_name = path_to_utf8(
            Path::new(
                self.data_swap_file
                    .file_name()
                    .expect("data swap file must resolve to a file path"),
            ),
            "data_swap_file",
        )?;
        if same_parent && swap_file_name.starts_with(&format!("{}.", self.log_file_stem)) {
            return Err(Error::InvalidStoragePath(format!(
                "data swap file must not overlap redo log family: {}",
                self.data_swap_file.display()
            )));
        }
        Ok(())
    }

    fn log_family_base_path(&self) -> PathBuf {
        self.log_dir.join(&self.log_file_stem)
    }
}

pub(crate) fn validate_catalog_file_name(file_name: &str) -> bool {
    if file_name.is_empty() || !file_name.ends_with(".mtb") {
        return false;
    }
    Path::new(file_name).file_name().and_then(|n| n.to_str()) == Some(file_name)
}

pub(crate) fn validate_log_file_stem(file_stem: &str) -> bool {
    if file_stem.is_empty() {
        return false;
    }
    if Path::new(file_stem).file_name().and_then(|n| n.to_str()) != Some(file_stem) {
        return false;
    }
    !file_stem
        .chars()
        .any(|c| matches!(c, '*' | '?' | '[' | ']' | '{' | '}'))
}

pub(crate) fn validate_swap_file_path_candidate(path: impl AsRef<Path>) -> Result<()> {
    let path = path.as_ref();
    if path.as_os_str().is_empty() {
        return Err(Error::InvalidStoragePath(format!(
            "data swap file must end with `.bin`: {}",
            path.display()
        )));
    }
    if !path_to_utf8(path, "data_swap_file")?.ends_with(".bin") {
        return Err(Error::InvalidStoragePath(format!(
            "data swap file must end with `.bin`: {}",
            path.display()
        )));
    }
    if path.file_name().is_none() {
        return Err(Error::InvalidStoragePath(format!(
            "data swap file must resolve to a file path: {}",
            path.display()
        )));
    }
    Ok(())
}

pub(crate) fn path_to_utf8<'a>(path: &'a Path, field: &str) -> Result<&'a str> {
    path.to_str()
        .ok_or_else(|| Error::InvalidStoragePath(format!("{field} must be valid UTF-8")))
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
        let paths = ResolvedStoragePaths::resolve(
            root.path(),
            Path::new("."),
            "catalog.mtb",
            Path::new("."),
            "redo.log",
            1,
            Path::new("data.bin"),
        )
        .unwrap();
        assert_eq!(paths.durable_layout.catalog_file_name, "catalog.mtb");
        assert!(paths.data_dir.starts_with(root.path()));
        assert!(paths.log_dir.starts_with(root.path()));
        assert!(paths.data_swap_file.ends_with("data.bin"));
    }

    #[test]
    fn test_validate_swap_file_suffix_and_escape() {
        let err = validate_swap_file_path_candidate("data.swap").unwrap_err();
        assert!(matches!(err, Error::InvalidStoragePath(_)));

        let err = ResolvedStoragePaths::resolve(
            Path::new("."),
            Path::new("."),
            "catalog.mtb",
            Path::new("."),
            "redo.log",
            1,
            Path::new("../data.bin"),
        )
        .unwrap_err();
        assert!(matches!(err, Error::InvalidStoragePath(_)));
    }

    #[test]
    fn test_marker_rejects_durable_layout_change() {
        let root = TempDir::new().unwrap();
        let initial = ResolvedStoragePaths::resolve(
            root.path(),
            Path::new("."),
            "catalog.mtb",
            Path::new("."),
            "redo.log",
            1,
            Path::new("data.bin"),
        )
        .unwrap();
        initial.ensure_directories().unwrap();
        initial.validate_marker_if_present().unwrap();
        initial.persist_marker_if_missing().unwrap();

        let changed = ResolvedStoragePaths::resolve(
            root.path(),
            Path::new("data"),
            "catalog.mtb",
            Path::new("."),
            "redo.log",
            1,
            Path::new("data.bin"),
        )
        .unwrap();
        changed.ensure_directories().unwrap();
        let err = changed.validate_marker_if_present().unwrap_err();
        assert!(matches!(err, Error::StorageLayoutMismatch(_)));
    }
}
