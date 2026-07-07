use crate::cli::{
    DEFAULT_BATCH_SIZE, DEFAULT_VALUE_SIZE, IndexMode, RunDefaults, Workload, validate_batch_size,
    validate_value_size, validate_workers,
};
use crate::error::{BenchError, Result};
use serde::{Deserialize, Deserializer, Serialize};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::result::Result as StdResult;

pub(super) const MANIFEST_FILE_NAME: &str = "benchmark-manifest.toml";
pub(super) const RESULT_MARKDOWN_FILE_NAME: &str = "benchmark-result.md";
pub(super) const INTERNAL_STATS_CSV_FILE_NAME: &str = "benchmark-internal-stats.csv";
pub(super) const RESULT_CSV_FILE_NAME: &str = "benchmark-result.csv";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct KeyRange {
    pub(super) start: u64,
    pub(super) len: u64,
}

impl KeyRange {
    pub(super) fn end(&self) -> Result<u64> {
        self.start
            .checked_add(self.len)
            .ok_or_else(|| BenchError::message("key range end overflow"))
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct DefaultsManifest {
    pub(super) threads: usize,
    pub(super) sessions: usize,
    #[serde(default = "default_value_size")]
    pub(super) value_size: usize,
    #[serde(default = "default_batch_size")]
    pub(super) batch_size: u64,
}

impl DefaultsManifest {
    pub(super) fn new(
        threads: usize,
        sessions: usize,
        value_size: usize,
        batch_size: u64,
    ) -> Result<Self> {
        RunDefaults::new(threads, sessions, value_size, batch_size)?;
        Ok(Self {
            threads,
            sessions,
            value_size,
            batch_size,
        })
    }

    fn validate(&self) -> Result<()> {
        validate_workers(self.threads, self.sessions)?;
        validate_value_size(self.value_size)?;
        validate_batch_size(self.batch_size)
    }

    pub(super) fn run_defaults(&self) -> RunDefaults {
        RunDefaults {
            threads: self.threads,
            sessions: self.sessions,
            value_size: self.value_size,
            batch_size: self.batch_size,
        }
    }
}

impl Default for DefaultsManifest {
    fn default() -> Self {
        Self {
            threads: 1,
            sessions: 1,
            value_size: DEFAULT_VALUE_SIZE,
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct SchemaManifest {
    pub(super) logical_key_column: String,
    pub(super) payload_column: String,
}

#[derive(Clone, Debug, Serialize)]
pub(super) struct RuntimeManifest {
    pub(super) next_key: u64,
    pub(super) rows_inserted: u64,
}

impl<'de> Deserialize<'de> for RuntimeManifest {
    fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawRuntimeManifest {
            next_key: u64,
            rows_inserted: Option<u64>,
        }

        let raw = RawRuntimeManifest::deserialize(deserializer)?;
        Ok(Self {
            next_key: raw.next_key,
            rows_inserted: raw.rows_inserted.unwrap_or(raw.next_key),
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct Manifest {
    pub(super) table_id: u64,
    pub(super) index: IndexMode,
    #[serde(default)]
    pub(super) defaults: DefaultsManifest,
    pub(super) schema: SchemaManifest,
    pub(super) runtime: RuntimeManifest,
}

impl Manifest {
    #[cfg(test)]
    pub(super) fn new(table_id: u64, index: IndexMode) -> Self {
        Self::new_with_defaults(table_id, index, DefaultsManifest::default())
    }

    pub(super) fn new_with_defaults(
        table_id: u64,
        index: IndexMode,
        defaults: DefaultsManifest,
    ) -> Self {
        Self {
            table_id,
            index,
            defaults,
            schema: SchemaManifest {
                logical_key_column: "logical_key".to_owned(),
                payload_column: "payload".to_owned(),
            },
            runtime: RuntimeManifest {
                next_key: 0,
                rows_inserted: 0,
            },
        }
    }

    pub(super) fn key_range(&self, rows: u64) -> Result<KeyRange> {
        let start = self.runtime.next_key;
        next_key_after(start, rows)?;
        Ok(KeyRange { start, len: rows })
    }

    pub(super) fn record_insert_success(&mut self, rows: u64) -> Result<()> {
        let next_key = next_key_after(self.runtime.next_key, rows)?;
        let rows_inserted = self
            .runtime
            .rows_inserted
            .checked_add(rows)
            .ok_or_else(|| BenchError::message("inserted row counter overflow"))?;
        self.runtime.next_key = next_key;
        self.runtime.rows_inserted = rows_inserted;
        Ok(())
    }

    pub(super) fn loaded_key_range(&self) -> Result<KeyRange> {
        if self.runtime.next_key == 0 || self.runtime.rows_inserted == 0 {
            return Err(BenchError::message(
                "read workload requires loaded benchmark data; run insert-seq or insert-rand first",
            ));
        }
        Ok(KeyRange {
            start: 0,
            len: self.runtime.next_key,
        })
    }

    pub(super) fn validate_workload_compatible(&self, workload: Workload) -> Result<()> {
        match workload {
            Workload::InsertSeq | Workload::InsertRand | Workload::TableScan => {}
            Workload::LookupSeq | Workload::LookupRand => {
                if self.index != IndexMode::Unique {
                    return Err(BenchError::message(format!(
                        "{workload} workload requires prepared index mode unique; found {}",
                        self.index
                    )));
                }
            }
            Workload::IndexScan => {
                if self.index != IndexMode::NonUnique {
                    return Err(BenchError::message(format!(
                        "{workload} workload requires prepared index mode non-unique; found {}",
                        self.index
                    )));
                }
            }
        }
        if !matches!(workload, Workload::InsertSeq | Workload::InsertRand) {
            self.loaded_key_range()?;
        }
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
    let manifest: Manifest = toml::from_str(&contents)?;
    manifest.defaults.validate()?;
    Ok(manifest)
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

fn default_value_size() -> usize {
    DEFAULT_VALUE_SIZE
}

fn default_batch_size() -> u64 {
    DEFAULT_BATCH_SIZE
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
        manifest.record_insert_success(10).unwrap();
        assert_eq!(manifest.runtime.next_key, 10);
        assert_eq!(manifest.runtime.rows_inserted, 10);

        let second_run = manifest.key_range(5).unwrap();
        assert_eq!(second_run.start, 10);
        manifest.record_insert_success(5).unwrap();
        assert_eq!(manifest.runtime.next_key, 15);
        assert_eq!(manifest.runtime.rows_inserted, 15);
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
        assert_eq!(loaded.defaults, DefaultsManifest::default());
        assert_eq!(loaded.defaults.value_size, DEFAULT_VALUE_SIZE);
        assert_eq!(loaded.defaults.batch_size, DEFAULT_BATCH_SIZE);
        assert_eq!(loaded.runtime.rows_inserted, 0);
    }

    #[test]
    fn roundtrip_manifest_with_defaults_and_inserted_rows() {
        let temp = TempDir::new().unwrap();
        let mut manifest = Manifest::new_with_defaults(
            42,
            IndexMode::NonUnique,
            DefaultsManifest::new(2, 4, 256, 8).unwrap(),
        );
        manifest.record_insert_success(7).unwrap();

        write_manifest(temp.path(), &manifest).unwrap();
        let loaded = read_manifest(temp.path()).unwrap();
        assert_eq!(loaded.index, IndexMode::NonUnique);
        assert_eq!(loaded.defaults.threads, 2);
        assert_eq!(loaded.defaults.sessions, 4);
        assert_eq!(loaded.defaults.value_size, 256);
        assert_eq!(loaded.defaults.batch_size, 8);
        assert_eq!(loaded.runtime.next_key, 7);
        assert_eq!(loaded.runtime.rows_inserted, 7);
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
        assert_eq!(loaded.runtime.rows_inserted, 11);
        assert_eq!(loaded.defaults, DefaultsManifest::default());
    }

    #[test]
    fn read_manifest_defaults_missing_sizing_fields() {
        let temp = TempDir::new().unwrap();
        fs::write(
            manifest_path(temp.path()),
            r#"
table_id = 42
index = "unique"

[defaults]
threads = 2
sessions = 4

[schema]
logical_key_column = "logical_key"
payload_column = "payload"

[runtime]
next_key = 0
rows_inserted = 0
"#,
        )
        .unwrap();

        let loaded = read_manifest(temp.path()).unwrap();

        assert_eq!(loaded.defaults.threads, 2);
        assert_eq!(loaded.defaults.sessions, 4);
        assert_eq!(loaded.defaults.value_size, DEFAULT_VALUE_SIZE);
        assert_eq!(loaded.defaults.batch_size, DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn read_manifest_rejects_invalid_sizing_defaults() {
        let temp = TempDir::new().unwrap();
        fs::write(
            manifest_path(temp.path()),
            r#"
table_id = 42
index = "unique"

[defaults]
threads = 1
sessions = 1
value_size = 0
batch_size = 1

[schema]
logical_key_column = "logical_key"
payload_column = "payload"

[runtime]
next_key = 0
rows_inserted = 0
"#,
        )
        .unwrap();

        assert!(read_manifest(temp.path()).is_err());
    }

    #[test]
    fn loaded_key_range_requires_inserted_rows() {
        let mut manifest = Manifest::new(7, IndexMode::Unique);
        assert!(manifest.loaded_key_range().is_err());
        manifest.record_insert_success(3).unwrap();
        assert_eq!(
            manifest.loaded_key_range().unwrap(),
            KeyRange { start: 0, len: 3 }
        );
    }

    #[test]
    fn record_insert_success_rejects_counter_overflow() {
        let mut manifest = Manifest::new(7, IndexMode::None);
        manifest.runtime.rows_inserted = u64::MAX;
        assert!(manifest.record_insert_success(1).is_err());
    }

    #[test]
    fn validate_workload_compatibility_checks_index_and_loaded_data() {
        let mut manifest = Manifest::new(7, IndexMode::None);
        assert!(
            manifest
                .validate_workload_compatible(Workload::InsertSeq)
                .is_ok()
        );
        assert!(
            manifest
                .validate_workload_compatible(Workload::LookupSeq)
                .is_err()
        );

        manifest = Manifest::new(7, IndexMode::Unique);
        assert!(
            manifest
                .validate_workload_compatible(Workload::LookupRand)
                .is_err()
        );
        manifest.record_insert_success(2).unwrap();
        assert!(
            manifest
                .validate_workload_compatible(Workload::LookupRand)
                .is_ok()
        );
        assert!(
            manifest
                .validate_workload_compatible(Workload::IndexScan)
                .is_err()
        );

        let mut manifest = Manifest::new(7, IndexMode::NonUnique);
        manifest.record_insert_success(2).unwrap();
        assert!(
            manifest
                .validate_workload_compatible(Workload::IndexScan)
                .is_ok()
        );
        assert!(
            manifest
                .validate_workload_compatible(Workload::LookupSeq)
                .is_err()
        );
    }
}
