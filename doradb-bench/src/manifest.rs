use crate::cli::IndexMode;
use crate::error::{BenchError, Result};
use serde::{Deserialize, Serialize};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

pub(super) const MANIFEST_FILE_NAME: &str = "benchmark-manifest.toml";
pub(super) const RESULT_MARKDOWN_FILE_NAME: &str = "benchmark-result.md";
pub(super) const INTERNAL_STATS_CSV_FILE_NAME: &str = "benchmark-internal-stats.csv";
pub(super) const RESULT_CSV_FILE_NAME: &str = "benchmark-result.csv";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct KeyRange {
    pub(super) start: u64,
    pub(super) len: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct SchemaManifest {
    pub(super) logical_key_column: String,
    pub(super) payload_column: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct RuntimeManifest {
    pub(super) next_key: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct Manifest {
    pub(super) table_id: u64,
    pub(super) index: IndexMode,
    pub(super) schema: SchemaManifest,
    pub(super) runtime: RuntimeManifest,
}

impl Manifest {
    pub(super) fn new(table_id: u64, index: IndexMode) -> Self {
        Self {
            table_id,
            index,
            schema: SchemaManifest {
                logical_key_column: "logical_key".to_owned(),
                payload_column: "payload".to_owned(),
            },
            runtime: RuntimeManifest { next_key: 0 },
        }
    }

    pub(super) fn key_range(&self, rows: u64) -> Result<KeyRange> {
        let start = self.runtime.next_key;
        next_key_after(start, rows)?;
        Ok(KeyRange { start, len: rows })
    }

    pub(super) fn advance_key_range(&mut self, rows: u64) -> Result<()> {
        self.runtime.next_key = next_key_after(self.runtime.next_key, rows)?;
        Ok(())
    }
}

pub(super) fn manifest_path(storage_root: &Path) -> PathBuf {
    storage_root.join(MANIFEST_FILE_NAME)
}

pub(super) fn result_markdown_path(storage_root: &Path) -> PathBuf {
    storage_root.join(RESULT_MARKDOWN_FILE_NAME)
}

pub(super) fn internal_stats_csv_path(storage_root: &Path) -> PathBuf {
    storage_root.join(INTERNAL_STATS_CSV_FILE_NAME)
}

pub(super) fn result_csv_path(storage_root: &Path) -> PathBuf {
    storage_root.join(RESULT_CSV_FILE_NAME)
}

pub(super) fn read_manifest(storage_root: &Path) -> Result<Manifest> {
    let path = manifest_path(storage_root);
    let contents = fs::read_to_string(&path).map_err(|err| {
        BenchError::message(format!(
            "failed to read benchmark manifest {}: {err}",
            path.display()
        ))
    })?;
    Ok(toml::from_str(&contents)?)
}

pub(super) fn write_manifest(storage_root: &Path, manifest: &Manifest) -> Result<()> {
    let path = manifest_path(storage_root);
    let contents = toml::to_string_pretty(manifest)?;
    fs::write(&path, contents).map_err(|err| {
        BenchError::message(format!(
            "failed to write benchmark manifest {}: {err}",
            path.display()
        ))
    })
}

pub(super) fn write_manifest_exclusive(storage_root: &Path, manifest: &Manifest) -> Result<()> {
    let path = manifest_path(storage_root);
    let contents = toml::to_string_pretty(manifest)?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .map_err(|err| {
            BenchError::message(format!(
                "failed to create benchmark manifest {}: {err}",
                path.display()
            ))
        })?;
    file.write_all(contents.as_bytes()).map_err(|err| {
        BenchError::message(format!(
            "failed to write benchmark manifest {}: {err}",
            path.display()
        ))
    })
}

fn next_key_after(start: u64, rows: u64) -> Result<u64> {
    start.checked_add(rows).ok_or_else(|| {
        BenchError::message(format!("key range exhausted for requested --num {rows}"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn allocate_runtime_key_ranges_across_runs() {
        let mut manifest = Manifest::new(7, IndexMode::None);
        let first_run = manifest.key_range(10).unwrap();
        assert_eq!(first_run.start, 0);
        manifest.advance_key_range(10).unwrap();
        assert_eq!(manifest.runtime.next_key, 10);

        let second_run = manifest.key_range(5).unwrap();
        assert_eq!(second_run.start, 10);
        manifest.advance_key_range(5).unwrap();
        assert_eq!(manifest.runtime.next_key, 15);
    }

    #[test]
    fn reject_exhausted_runtime_key_range() {
        let mut manifest = Manifest::new(7, IndexMode::None);
        manifest.runtime.next_key = u64::MAX;
        assert!(manifest.key_range(1).is_err());
    }

    #[test]
    fn roundtrip_manifest_toml() {
        let temp = TempDir::new().unwrap();
        let manifest = Manifest::new(42, IndexMode::Unique);
        write_manifest(temp.path(), &manifest).unwrap();
        let loaded = read_manifest(temp.path()).unwrap();
        assert_eq!(loaded.table_id, 42);
        assert_eq!(loaded.index, IndexMode::Unique);
    }

    #[test]
    fn written_manifest_omits_version_and_prepare_footprint() {
        let temp = TempDir::new().unwrap();
        let manifest = Manifest::new(42, IndexMode::Unique);
        write_manifest(temp.path(), &manifest).unwrap();
        let contents = fs::read_to_string(manifest_path(temp.path())).unwrap();
        assert!(!contents.contains("schema_version"));
        assert!(!contents.contains("[prepare]"));
        assert!(!contents.contains("workload_set"));
    }

    #[test]
    fn exclusive_manifest_write_rejects_existing_manifest_without_overwrite() {
        let temp = TempDir::new().unwrap();
        let first = Manifest::new(42, IndexMode::Unique);
        write_manifest_exclusive(temp.path(), &first).unwrap();

        let second = Manifest::new(7, IndexMode::None);
        assert!(write_manifest_exclusive(temp.path(), &second).is_err());

        let loaded = read_manifest(temp.path()).unwrap();
        assert_eq!(loaded.table_id, 42);
        assert_eq!(loaded.index, IndexMode::Unique);
    }

    #[test]
    fn read_manifest_tolerates_extra_fields() {
        let temp = TempDir::new().unwrap();
        fs::write(
            manifest_path(temp.path()),
            r#"
schema_version = 2
table_id = 42
index = "unique"

[schema]
logical_key_column = "logical_key"
payload_column = "payload"

[prepare]
workload_set = ["fillseq", "fillrandom", "fillbatch"]

[runtime]
next_key = 11
"#,
        )
        .unwrap();
        let loaded = read_manifest(temp.path()).unwrap();
        assert_eq!(loaded.table_id, 42);
        assert_eq!(loaded.index, IndexMode::Unique);
        assert_eq!(loaded.runtime.next_key, 11);
    }
}
