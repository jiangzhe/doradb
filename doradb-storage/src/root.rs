//! Storage-root path resolution, durable layout control, and exclusive ownership.

use crate::component::{Component, ComponentRegistry, ShelfScope};
use crate::conf::path::{
    path_to_utf8, validate_catalog_file_name, validate_log_file_stem,
    validate_swap_file_path_candidate,
};
use crate::error::{ConfigError, ConfigResult, IoError, IoResult};
use crate::quiescent::QuiescentBox;
use error_stack::{Report, ResultExt};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::convert::Infallible;
use std::env::current_dir;
use std::fmt;
use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::{Error as StdIoError, ErrorKind, Read, Seek, SeekFrom, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Component as PathComponent, Path, PathBuf};
use std::process::id as process_id;
use std::result::Result as StdResult;
use std::str::from_utf8;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static STORAGE_LAYOUT_TEMP_NONCE: AtomicU64 = AtomicU64::new(0);

/// Marker file stored at `storage_root` that records the durable storage layout.
pub(crate) const STORAGE_LAYOUT_FILE_NAME: &str = "storage-layout.toml";
/// Storage-layout marker version used for durable compatibility checks.
const STORAGE_LAYOUT_VERSION: u32 = 3;
/// Persistent file whose operating-system lock owns one canonical storage root.
const STORAGE_LOCK_FILE_NAME: &str = "storage.lock";
/// Version of the diagnostic record stored in [`STORAGE_LOCK_FILE_NAME`].
const STORAGE_LOCK_RECORD_VERSION: u32 = 1;
/// Maximum number of lock-record bytes read by a contending process.
const STORAGE_LOCK_DIAGNOSTIC_MAX_BYTES: usize = 4096;
/// Prefix reserved for same-directory storage-layout publication files.
const STORAGE_LAYOUT_TEMP_PREFIX: &str = ".storage-layout.toml.tmp.";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct DurableStorageLayout {
    version: u32,
    data_dir: String,
    catalog_file_name: String,
    log_dir: String,
    log_file_stem: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct StorageLockRecord {
    version: u32,
    pid: u32,
    acquired_unix_ms: u64,
}

/// Valid diagnostic fields reported by the process holding a storage root.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StorageRootOwnerDiagnostic {
    /// Process identifier written by the lock holder.
    pub(crate) pid: u32,
    /// Lock-acquisition time in milliseconds since the Unix epoch.
    pub(crate) acquired_unix_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StorageRootOwnerDiagnosticState {
    Valid(StorageRootOwnerDiagnostic),
    Invalid,
    Unavailable,
}

/// Result of one nonblocking storage-root lock attempt.
pub(crate) enum StorageRootLeaseAttempt {
    /// The caller now owns the canonical root.
    Acquired(StorageRootLease),
    /// Another open file description owns the authoritative OS lock.
    Contended {
        diagnostic: Option<StorageRootOwnerDiagnostic>,
        diagnostic_status: &'static str,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MarkerPublicationState {
    NotInstalled,
    InstalledDurabilityUnknown,
}

impl fmt::Display for MarkerPublicationState {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotInstalled => f.write_str("not_installed"),
            Self::InstalledDurabilityUnknown => f.write_str("installed_durability_unknown"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MarkerPublicationStage {
    Serialize,
    CreateTemp,
    WriteTemp,
    SyncTemp,
    InstallFinal,
    RemoveTemp,
    SyncDirectory,
}

impl fmt::Display for MarkerPublicationStage {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Serialize => "serialize",
            Self::CreateTemp => "create_temp",
            Self::WriteTemp => "write_temp",
            Self::SyncTemp => "sync_temp",
            Self::InstallFinal => "install_final",
            Self::RemoveTemp => "remove_temp",
            Self::SyncDirectory => "sync_directory",
        })
    }
}

/// Storage-root portion projected from an [`EngineConfig`](crate::conf::EngineConfig).
pub(crate) struct StoragePathResolveInput<'a> {
    storage_root: &'a Path,
    data_dir: &'a Path,
    catalog_file_name: &'a str,
    log_dir: &'a Path,
    log_file_stem: &'a str,
    data_swap_file: &'a Path,
    index_swap_file: &'a Path,
}

impl<'a> StoragePathResolveInput<'a> {
    /// Construct storage-root path resolution input from projected configuration values.
    #[inline]
    pub(crate) fn new(
        storage_root: &'a Path,
        data_dir: &'a Path,
        catalog_file_name: &'a str,
        log_dir: &'a Path,
        log_file_stem: &'a str,
        data_swap_file: &'a Path,
        index_swap_file: &'a Path,
    ) -> Self {
        Self {
            storage_root,
            data_dir,
            catalog_file_name,
            log_dir,
            log_file_stem,
            data_swap_file,
            index_swap_file,
        }
    }
}

/// Absolute engine paths resolved from storage-root-relative configuration.
#[derive(Debug, Clone)]
pub(crate) struct ResolvedStoragePaths {
    storage_root: PathBuf,
    data_dir_rel: PathBuf,
    data_dir: PathBuf,
    catalog_file_path: PathBuf,
    log_dir_rel: PathBuf,
    log_dir: PathBuf,
    log_file_stem: String,
    data_swap_file_rel: PathBuf,
    data_swap_file: PathBuf,
    index_swap_file_rel: PathBuf,
    index_swap_file: PathBuf,
    durable_layout: DurableStorageLayout,
}

impl ResolvedStoragePaths {
    /// Validate and resolve storage-root configuration into absolute engine paths.
    pub(crate) fn resolve(input: StoragePathResolveInput<'_>) -> ConfigResult<Self> {
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
            data_dir_rel,
            data_dir,
            catalog_file_path,
            log_dir_rel,
            log_dir,
            log_file_stem: input.log_file_stem.to_string(),
            data_swap_file_rel,
            data_swap_file,
            index_swap_file_rel,
            index_swap_file,
            durable_layout,
        };
        res.validate_control_namespace()?;
        res.validate_non_overlapping_paths()?;
        Ok(res)
    }

    /// Create and canonicalize the storage root, then rebase all subordinate paths.
    pub(crate) fn prepare_storage_root(mut self) -> IoResult<Self> {
        fs::create_dir_all(&self.storage_root).map_err(|err| {
            let kind = err.kind();
            Report::new(err)
                .change_context(IoError::from(kind))
                .attach(format!(
                    "operation=create_storage_root, phase=prepare, configured_root={}",
                    self.storage_root.display()
                ))
        })?;
        let configured_root = self.storage_root.clone();
        let canonical_root = fs::canonicalize(&configured_root).map_err(|err| {
            let kind = err.kind();
            Report::new(err)
                .change_context(IoError::from(kind))
                .attach(format!(
                    "operation=canonicalize_storage_root, phase=prepare, configured_root={}",
                    configured_root.display()
                ))
        })?;
        self.storage_root = canonical_root;
        self.data_dir = self.storage_root.join(&self.data_dir_rel);
        self.catalog_file_path = self
            .data_dir
            .join(self.durable_layout.catalog_file_name.as_str());
        self.log_dir = self.storage_root.join(&self.log_dir_rel);
        self.data_swap_file = self.storage_root.join(&self.data_swap_file_rel);
        self.index_swap_file = self.storage_root.join(&self.index_swap_file_rel);
        Ok(self)
    }

    /// Return the canonical storage-root path after preparation.
    pub(crate) fn storage_root_path(&self) -> &Path {
        &self.storage_root
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

    /// Ensure subordinate storage directories and swap-file parent directories exist.
    pub(crate) fn ensure_directories(&self) -> IoResult<()> {
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
    pub(crate) fn validate_marker_if_present(&self) -> ConfigResult<bool> {
        let marker_path = self.marker_path();
        let raw = match fs::read_to_string(&marker_path) {
            Ok(raw) => raw,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(false),
            Err(err) => {
                let kind = err.kind();
                return Err(Report::new(err)
                    .change_context(IoError::from(kind))
                    .change_context(ConfigError::StorageLayoutMarkerRead)
                    .attach(format!(
                        "operation=read_storage_layout_marker, marker_path={}",
                        marker_path.display()
                    )));
            }
        };
        let actual = parse_durable_layout(&raw)?;
        validate_durable_layout_match(&self.durable_layout, &actual)?;
        Ok(true)
    }

    /// Remove recognized crash-left marker temporaries and sync the root directory.
    pub(crate) fn cleanup_stale_marker_temps(&self) -> IoResult<()> {
        let entries = fs::read_dir(&self.storage_root).map_err(|err| {
            let kind = err.kind();
            Report::new(err)
                .change_context(IoError::from(kind))
                .attach(format!(
                    "operation=scan_marker_temporaries, phase=scan, storage_root={}",
                    self.storage_root.display()
                ))
        })?;
        for entry in entries {
            let entry = entry.map_err(|err| {
                let kind = err.kind();
                Report::new(err)
                    .change_context(IoError::from(kind))
                    .attach(format!(
                        "operation=scan_marker_temporaries, phase=read_entry, storage_root={}",
                        self.storage_root.display()
                    ))
            })?;
            let file_name = entry.file_name();
            let Some(file_name) = file_name.to_str() else {
                continue;
            };
            if !is_storage_layout_temp_name(file_name) {
                continue;
            }
            let temp_path = entry.path();
            let file_type = entry.file_type().map_err(|err| {
                let kind = err.kind();
                Report::new(err)
                    .change_context(IoError::from(kind))
                    .attach(format!(
                        "operation=classify_marker_temporary, phase=classify, storage_root={}, temp_path={}",
                        self.storage_root.display(),
                        temp_path.display()
                    ))
            })?;
            if file_type.is_dir() {
                let err = StdIoError::new(
                    ErrorKind::InvalidData,
                    "reserved marker temporary name is occupied by a directory",
                );
                let kind = err.kind();
                return Err(Report::new(err)
                    .change_context(IoError::from(kind))
                    .attach(format!(
                        "operation=classify_marker_temporary, phase=reject_directory, storage_root={}, temp_path={}",
                        self.storage_root.display(),
                        temp_path.display()
                    )));
            }
            if let Err(err) = fs::remove_file(&temp_path) {
                let kind = err.kind();
                let err = Report::new(err)
                    .change_context(IoError::from(kind))
                    .attach(format!(
                        "operation=remove_marker_temporary, phase=unlink, storage_root={}, temp_path={}",
                        self.storage_root.display(),
                        temp_path.display()
                    ));
                return Err(attach_cleanup_failure(
                    err,
                    self.sync_root_directory(),
                    "stale marker temporary failure directory sync",
                ));
            }
        }
        self.sync_root_directory()
    }

    /// Publish the durable storage-layout marker atomically without clobbering a peer.
    pub(crate) fn persist_marker(&self) -> IoResult<()> {
        let marker_path = self.marker_path();
        let marker = self.serialize_durable_layout().map_err(|err| {
            attach_publication_context(
                err,
                MarkerPublicationStage::Serialize,
                MarkerPublicationState::NotInstalled,
                &self.storage_root,
                &marker_path,
                None,
            )
        })?;
        let nonce = STORAGE_LAYOUT_TEMP_NONCE.fetch_add(1, Ordering::Relaxed);
        let temp_path = self.storage_root.join(format!(
            "{STORAGE_LAYOUT_TEMP_PREFIX}{}.{nonce}",
            process_id()
        ));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp_path)
            .map_err(|err| {
                let kind = err.kind();
                attach_publication_context(
                    Report::new(err)
                        .change_context(IoError::from(kind))
                        .attach("create marker publication temporary"),
                    MarkerPublicationStage::CreateTemp,
                    MarkerPublicationState::NotInstalled,
                    &self.storage_root,
                    &marker_path,
                    Some(&temp_path),
                )
            })?;
        #[cfg(test)]
        if let Err(err) =
            tests::run_marker_publication_test_hook(tests::MarkerPublicationTestStage::TempCreated)
        {
            drop(file);
            return Err(self.handle_preinstall_failure(
                attach_publication_context(
                    err,
                    MarkerPublicationStage::CreateTemp,
                    MarkerPublicationState::NotInstalled,
                    &self.storage_root,
                    &marker_path,
                    Some(&temp_path),
                ),
                &temp_path,
            ));
        }
        if let Err(err) = file.write_all(marker.as_bytes()) {
            drop(file);
            let kind = err.kind();
            return Err(self.handle_preinstall_failure(
                attach_publication_context(
                    Report::new(err)
                        .change_context(IoError::from(kind))
                        .attach("write marker publication temporary"),
                    MarkerPublicationStage::WriteTemp,
                    MarkerPublicationState::NotInstalled,
                    &self.storage_root,
                    &marker_path,
                    Some(&temp_path),
                ),
                &temp_path,
            ));
        }
        #[cfg(test)]
        if let Err(err) =
            tests::run_marker_publication_test_hook(tests::MarkerPublicationTestStage::TempWritten)
        {
            drop(file);
            return Err(self.handle_preinstall_failure(
                attach_publication_context(
                    err,
                    MarkerPublicationStage::WriteTemp,
                    MarkerPublicationState::NotInstalled,
                    &self.storage_root,
                    &marker_path,
                    Some(&temp_path),
                ),
                &temp_path,
            ));
        }
        if let Err(err) = file.sync_all() {
            drop(file);
            let kind = err.kind();
            return Err(self.handle_preinstall_failure(
                attach_publication_context(
                    Report::new(err)
                        .change_context(IoError::from(kind))
                        .attach("sync marker publication temporary"),
                    MarkerPublicationStage::SyncTemp,
                    MarkerPublicationState::NotInstalled,
                    &self.storage_root,
                    &marker_path,
                    Some(&temp_path),
                ),
                &temp_path,
            ));
        }
        #[cfg(test)]
        if let Err(err) =
            tests::run_marker_publication_test_hook(tests::MarkerPublicationTestStage::TempSynced)
        {
            drop(file);
            return Err(self.handle_preinstall_failure(
                attach_publication_context(
                    err,
                    MarkerPublicationStage::SyncTemp,
                    MarkerPublicationState::NotInstalled,
                    &self.storage_root,
                    &marker_path,
                    Some(&temp_path),
                ),
                &temp_path,
            ));
        }
        drop(file);
        if let Err(err) = fs::hard_link(&temp_path, &marker_path) {
            let kind = err.kind();
            return Err(self.handle_preinstall_failure(
                attach_publication_context(
                    Report::new(err)
                        .change_context(IoError::from(kind))
                        .attach("install marker final name with hard link"),
                    MarkerPublicationStage::InstallFinal,
                    MarkerPublicationState::NotInstalled,
                    &self.storage_root,
                    &marker_path,
                    Some(&temp_path),
                ),
                &temp_path,
            ));
        }
        #[cfg(test)]
        if let Err(err) = tests::run_marker_publication_test_hook(
            tests::MarkerPublicationTestStage::FinalInstalled,
        ) {
            return Err(self.handle_postinstall_failure(
                attach_publication_context(
                    err,
                    MarkerPublicationStage::InstallFinal,
                    MarkerPublicationState::InstalledDurabilityUnknown,
                    &self.storage_root,
                    &marker_path,
                    Some(&temp_path),
                ),
                &temp_path,
            ));
        }
        if let Err(err) = fs::remove_file(&temp_path) {
            let kind = err.kind();
            return Err(self.handle_postinstall_failure(
                attach_publication_context(
                    Report::new(err)
                        .change_context(IoError::from(kind))
                        .attach("remove installed marker temporary name"),
                    MarkerPublicationStage::RemoveTemp,
                    MarkerPublicationState::InstalledDurabilityUnknown,
                    &self.storage_root,
                    &marker_path,
                    Some(&temp_path),
                ),
                &temp_path,
            ));
        }
        #[cfg(test)]
        if let Err(err) =
            tests::run_marker_publication_test_hook(tests::MarkerPublicationTestStage::TempRemoved)
        {
            return Err(self.handle_postinstall_failure(
                attach_publication_context(
                    err,
                    MarkerPublicationStage::RemoveTemp,
                    MarkerPublicationState::InstalledDurabilityUnknown,
                    &self.storage_root,
                    &marker_path,
                    Some(&temp_path),
                ),
                &temp_path,
            ));
        }
        if let Err(err) = self.sync_root_directory() {
            return Err(self.handle_postinstall_failure(
                attach_publication_context(
                    err,
                    MarkerPublicationStage::SyncDirectory,
                    MarkerPublicationState::InstalledDurabilityUnknown,
                    &self.storage_root,
                    &marker_path,
                    Some(&temp_path),
                ),
                &temp_path,
            ));
        }
        Ok(())
    }

    #[inline]
    fn serialize_durable_layout(&self) -> IoResult<String> {
        toml::to_string(&self.durable_layout).map_err(|err| {
            Report::new(err)
                .change_context(IoError::from(ErrorKind::InvalidData))
                .attach("serialize durable storage layout marker")
        })
    }

    /// Return the durable storage-layout marker path.
    pub(crate) fn marker_path(&self) -> PathBuf {
        self.storage_root.join(STORAGE_LAYOUT_FILE_NAME)
    }

    /// Return the persistent storage-root lease path.
    pub(crate) fn lock_path(&self) -> PathBuf {
        self.storage_root.join(STORAGE_LOCK_FILE_NAME)
    }

    fn sync_root_directory(&self) -> IoResult<()> {
        let root = File::open(&self.storage_root).map_err(|err| {
            let kind = err.kind();
            Report::new(err)
                .change_context(IoError::from(kind))
                .attach(format!(
                    "operation=open_storage_root_for_sync, storage_root={}",
                    self.storage_root.display()
                ))
        })?;
        root.sync_all().map_err(|err| {
            let kind = err.kind();
            Report::new(err)
                .change_context(IoError::from(kind))
                .attach(format!(
                    "operation=sync_storage_root_directory, storage_root={}",
                    self.storage_root.display()
                ))
        })
    }

    fn handle_preinstall_failure(&self, err: Report<IoError>, temp_path: &Path) -> Report<IoError> {
        attach_cleanup_failure(
            err,
            self.remove_temp_and_sync(temp_path),
            "pre-install marker temporary cleanup failed",
        )
    }

    fn handle_postinstall_failure(
        &self,
        err: Report<IoError>,
        temp_path: &Path,
    ) -> Report<IoError> {
        attach_cleanup_failure(
            err,
            self.remove_temp_and_sync(temp_path),
            "post-install marker temporary cleanup failed",
        )
    }

    fn remove_temp_and_sync(&self, temp_path: &Path) -> IoResult<()> {
        match fs::remove_file(temp_path) {
            Ok(()) => {}
            Err(err) if err.kind() == ErrorKind::NotFound => {}
            Err(err) => {
                let kind = err.kind();
                let err = Report::new(err)
                    .change_context(IoError::from(kind))
                    .attach(format!(
                        "operation=remove_marker_temporary, phase=cleanup, storage_root={}, temp_path={}",
                        self.storage_root.display(),
                        temp_path.display()
                    ));
                return Err(attach_cleanup_failure(
                    err,
                    self.sync_root_directory(),
                    "marker temporary cleanup directory sync failed",
                ));
            }
        }
        self.sync_root_directory()
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

    fn validate_control_namespace(&self) -> ConfigResult<()> {
        for (field, path) in [
            ("data_dir", self.data_dir_rel.as_path()),
            ("log_dir", self.log_dir_rel.as_path()),
            ("data_swap_file", self.data_swap_file_rel.as_path()),
            ("index_swap_file", self.index_swap_file_rel.as_path()),
        ] {
            let Some(first) = path.components().next() else {
                continue;
            };
            let PathComponent::Normal(first) = first else {
                continue;
            };
            let Some(first) = first.to_str() else {
                continue;
            };
            let reserved = first == STORAGE_LOCK_FILE_NAME || is_storage_layout_temp_name(first);
            if reserved {
                return Err(Report::new(
                    ConfigError::PathMustNotOverlapReservedLocation,
                )
                .attach(format!(
                    "{field} must not consume the reserved storage control entry: path={}, reserved_entry={first}",
                    path.display()
                )));
            }
        }

        if self.log_dir_rel.as_os_str().is_empty()
            && (self.log_file_stem == STORAGE_LOCK_FILE_NAME
                || is_storage_layout_temp_name(&self.log_file_stem))
        {
            return Err(
                Report::new(ConfigError::PathMustNotOverlapReservedLocation).attach(format!(
                    "log_file_stem must not consume a reserved root control entry: {}",
                    self.log_file_stem
                )),
            );
        }
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
        let lock_path = self.storage_root.join(STORAGE_LOCK_FILE_NAME);
        if aliases_reserved_path(path, &lock_path) {
            return Err(
                Report::new(ConfigError::PathMustNotOverlapReservedLocation).attach(format!(
                    "{field} must not overlap storage lock: {}",
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

/// First-registered component that retains exclusive ownership of a storage root.
pub(crate) struct StorageRootLease {
    file: Mutex<Option<File>>,
}

impl StorageRootLease {
    /// Try to own the prepared canonical storage root and publish owner diagnostics.
    pub(crate) fn try_acquire(paths: &ResolvedStoragePaths) -> IoResult<StorageRootLeaseAttempt> {
        let lock_path = paths.lock_path();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(&lock_path)
            .map_err(|err| {
                let kind = err.kind();
                Report::new(err)
                    .change_context(IoError::from(kind))
                    .attach(format!(
                        "operation=open_storage_root_lock, phase=open, storage_root={}, lock_path={}",
                        paths.storage_root.display(),
                        lock_path.display()
                    ))
            })?;
        let metadata = file.metadata().map_err(|err| {
            let kind = err.kind();
            Report::new(err)
                .change_context(IoError::from(kind))
                .attach(format!(
                    "operation=inspect_storage_root_lock, phase=metadata, storage_root={}, lock_path={}",
                    paths.storage_root.display(),
                    lock_path.display()
                ))
        })?;
        if !metadata.file_type().is_file() {
            let err = StdIoError::new(
                ErrorKind::InvalidData,
                "storage root lock object must be a regular file",
            );
            let kind = err.kind();
            return Err(Report::new(err)
                .change_context(IoError::from(kind))
                .attach(format!(
                    "operation=inspect_storage_root_lock, phase=require_regular_file, storage_root={}, lock_path={}",
                    paths.storage_root.display(),
                    lock_path.display()
                )));
        }

        match file.try_lock() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => {
                let diagnostic = read_contender_diagnostic(&mut file);
                let (diagnostic, diagnostic_status) = match diagnostic {
                    StorageRootOwnerDiagnosticState::Valid(diagnostic) => {
                        (Some(diagnostic), "valid")
                    }
                    StorageRootOwnerDiagnosticState::Invalid => (None, "invalid"),
                    StorageRootOwnerDiagnosticState::Unavailable => (None, "unavailable"),
                };
                return Ok(StorageRootLeaseAttempt::Contended {
                    diagnostic,
                    diagnostic_status,
                });
            }
            Err(TryLockError::Error(err)) => {
                let kind = err.kind();
                return Err(Report::new(err)
                    .change_context(IoError::from(kind))
                    .attach(format!(
                        "operation=acquire_storage_root_lock, phase=try_lock, storage_root={}, lock_path={}",
                        paths.storage_root.display(),
                        lock_path.display()
                    )));
            }
        }

        #[cfg(test)]
        tests::run_storage_root_lease_test_hook(tests::StorageRootLeaseTestStage::LockAcquired)?;
        let acquired_unix_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|err| {
                Report::new(err)
                    .change_context(IoError::from(ErrorKind::InvalidData))
                    .attach(format!(
                        "operation=construct_storage_root_owner_record, phase=clock, storage_root={}, lock_path={}",
                        paths.storage_root.display(),
                        lock_path.display()
                    ))
            })?
            .as_millis() as u64;
        let record = StorageLockRecord {
            version: STORAGE_LOCK_RECORD_VERSION,
            pid: process_id(),
            acquired_unix_ms,
        };
        let raw = toml::to_string(&record).map_err(|err| {
            Report::new(err)
                .change_context(IoError::from(ErrorKind::InvalidData))
                .attach(format!(
                    "operation=serialize_storage_root_owner_record, phase=serialize, storage_root={}, lock_path={}",
                    paths.storage_root.display(),
                    lock_path.display()
                ))
        })?;
        file.seek(SeekFrom::Start(0)).map_err(|err| {
            let kind = err.kind();
            Report::new(err)
                .change_context(IoError::from(kind))
                .attach(format!(
                    "operation=write_storage_root_owner_record, phase=seek, storage_root={}, lock_path={}",
                    paths.storage_root.display(),
                    lock_path.display()
                ))
        })?;
        file.set_len(0).map_err(|err| {
            let kind = err.kind();
            Report::new(err)
                .change_context(IoError::from(kind))
                .attach(format!(
                    "operation=write_storage_root_owner_record, phase=truncate, storage_root={}, lock_path={}",
                    paths.storage_root.display(),
                    lock_path.display()
                ))
        })?;
        #[cfg(test)]
        tests::run_storage_root_lease_test_hook(tests::StorageRootLeaseTestStage::RecordTruncated)?;
        file.write_all(raw.as_bytes()).map_err(|err| {
            let kind = err.kind();
            Report::new(err)
                .change_context(IoError::from(kind))
                .attach(format!(
                    "operation=write_storage_root_owner_record, phase=write, storage_root={}, lock_path={}",
                    paths.storage_root.display(),
                    lock_path.display()
                ))
        })?;
        #[cfg(test)]
        tests::run_storage_root_lease_test_hook(tests::StorageRootLeaseTestStage::RecordWritten)?;
        file.sync_all().map_err(|err| {
            let kind = err.kind();
            Report::new(err)
                .change_context(IoError::from(kind))
                .attach(format!(
                    "operation=write_storage_root_owner_record, phase=sync, storage_root={}, lock_path={}",
                    paths.storage_root.display(),
                    lock_path.display()
                ))
        })?;
        #[cfg(test)]
        tests::run_storage_root_lease_test_hook(tests::StorageRootLeaseTestStage::RecordSynced)?;
        Ok(StorageRootLeaseAttempt::Acquired(Self {
            file: Mutex::new(Some(file)),
        }))
    }
}

impl Component for StorageRootLease {
    type Config = Self;
    type Owned = Self;
    type Access = ();
    type Error = Infallible;

    const NAME: &'static str = "storage_root_lease";

    #[inline]
    async fn build(
        config: Self::Config,
        registry: &mut ComponentRegistry,
        _shelf: ShelfScope<'_, Self>,
    ) -> StdResult<(), Self::Error> {
        registry.register::<Self>(config);
        Ok(())
    }

    #[inline]
    fn access(_owner: &QuiescentBox<Self::Owned>) -> Self::Access {}

    #[inline]
    fn shutdown(component: &Self::Owned) {
        drop(component.file.lock().take());
    }
}

fn attach_cleanup_failure(
    err: Report<IoError>,
    cleanup: IoResult<()>,
    context: &'static str,
) -> Report<IoError> {
    match cleanup {
        Ok(()) => err,
        Err(cleanup_err) => err.attach(cleanup_err.attach(context)),
    }
}

fn attach_publication_context(
    err: Report<IoError>,
    stage: MarkerPublicationStage,
    state: MarkerPublicationState,
    storage_root: &Path,
    marker_path: &Path,
    temp_path: Option<&Path>,
) -> Report<IoError> {
    let paths = if let Some(temp_path) = temp_path {
        format!(
            "operation=publish_storage_layout_marker, stage={stage}, storage_root={}, marker_path={}, temp_path={}",
            storage_root.display(),
            marker_path.display(),
            temp_path.display()
        )
    } else {
        format!(
            "operation=publish_storage_layout_marker, stage={stage}, storage_root={}, marker_path={}",
            storage_root.display(),
            marker_path.display()
        )
    };
    err.attach(state).attach(paths)
}

fn read_contender_diagnostic(file: &mut File) -> StorageRootOwnerDiagnosticState {
    if file.seek(SeekFrom::Start(0)).is_err() {
        return StorageRootOwnerDiagnosticState::Unavailable;
    }
    let Ok(metadata) = file.metadata() else {
        return StorageRootOwnerDiagnosticState::Unavailable;
    };
    if metadata.len() > STORAGE_LOCK_DIAGNOSTIC_MAX_BYTES as u64 {
        return StorageRootOwnerDiagnosticState::Invalid;
    }
    let mut raw = Vec::with_capacity(STORAGE_LOCK_DIAGNOSTIC_MAX_BYTES);
    if file
        .take(STORAGE_LOCK_DIAGNOSTIC_MAX_BYTES as u64)
        .read_to_end(&mut raw)
        .is_err()
    {
        return StorageRootOwnerDiagnosticState::Unavailable;
    }
    let Ok(raw) = from_utf8(&raw) else {
        return StorageRootOwnerDiagnosticState::Invalid;
    };
    let Ok(record) = toml::from_str::<StorageLockRecord>(raw) else {
        return StorageRootOwnerDiagnosticState::Invalid;
    };
    if record.version != STORAGE_LOCK_RECORD_VERSION
        || record.pid == 0
        || record.acquired_unix_ms == 0
    {
        return StorageRootOwnerDiagnosticState::Invalid;
    }
    StorageRootOwnerDiagnosticState::Valid(StorageRootOwnerDiagnostic {
        pid: record.pid,
        acquired_unix_ms: record.acquired_unix_ms,
    })
}

fn is_storage_layout_temp_name(file_name: &str) -> bool {
    let Some(suffix) = file_name.strip_prefix(STORAGE_LAYOUT_TEMP_PREFIX) else {
        return false;
    };
    let Some((pid, nonce)) = suffix.split_once('.') else {
        return false;
    };
    !pid.is_empty()
        && !nonce.is_empty()
        && !nonce.contains('.')
        && pid.bytes().all(|byte| byte.is_ascii_digit())
        && nonce.bytes().all(|byte| byte.is_ascii_digit())
        && pid.parse::<u32>().is_ok()
        && nonce.parse::<u64>().is_ok()
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
            PathComponent::CurDir => {}
            PathComponent::Normal(seg) => normalized.push(seg),
            PathComponent::ParentDir => {
                return Err(
                    Report::new(ConfigError::PathMustNotEscapeStorageRoot).attach(format!(
                        "path must not escape storage_root: {}",
                        path.display()
                    )),
                );
            }
            PathComponent::RootDir | PathComponent::Prefix(_) => {
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
    use crate::conf::{EngineConfig, EvictableBufferPoolConfig, FileSystemConfig, TrxSysConfig};
    use error_stack::Report;
    use std::cell::RefCell;
    use std::env::{current_exe, var_os};
    use std::fs::{create_dir, read, write};
    use std::io::{BufRead, BufReader, stdin, stdout};
    use std::os::unix::fs::{MetadataExt, symlink};
    use std::process::{Child, Command, Stdio, exit};
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;
    use tempfile::TempDir;

    const STORAGE_LEASE_HELPER_ENV: &str = "DORADB_STORAGE_LEASE_HELPER_ROOT";
    const STORAGE_LEASE_HELPER_TEST: &str = "root::tests::storage_root_lease_subprocess_helper";
    const STORAGE_MARKER_HELPER_ROOT_ENV: &str = "DORADB_STORAGE_MARKER_HELPER_ROOT";
    const STORAGE_MARKER_HELPER_STAGE_ENV: &str = "DORADB_STORAGE_MARKER_HELPER_STAGE";
    const STORAGE_MARKER_HELPER_TEST: &str = "root::tests::storage_marker_subprocess_helper";

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum MarkerPublicationTestStage {
        TempCreated,
        TempWritten,
        TempSynced,
        FinalInstalled,
        TempRemoved,
    }

    impl fmt::Display for MarkerPublicationTestStage {
        #[inline]
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(match self {
                Self::TempCreated => "temp_created",
                Self::TempWritten => "temp_written",
                Self::TempSynced => "temp_synced",
                Self::FinalInstalled => "final_installed",
                Self::TempRemoved => "temp_removed",
            })
        }
    }

    type MarkerPublicationTestHook = Box<dyn FnMut(MarkerPublicationTestStage) -> IoResult<()>>;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum StorageRootLeaseTestStage {
        LockAcquired,
        RecordTruncated,
        RecordWritten,
        RecordSynced,
    }

    type StorageRootLeaseTestHook = Box<dyn FnMut(StorageRootLeaseTestStage) -> IoResult<()>>;

    thread_local! {
        static MARKER_PUBLICATION_TEST_HOOK: RefCell<Option<MarkerPublicationTestHook>> =
            RefCell::new(None);
        static STORAGE_ROOT_LEASE_TEST_HOOK: RefCell<Option<StorageRootLeaseTestHook>> =
            RefCell::new(None);
    }

    struct MarkerPublicationTestHookGuard;

    impl Drop for MarkerPublicationTestHookGuard {
        fn drop(&mut self) {
            MARKER_PUBLICATION_TEST_HOOK.with(|test_hook| {
                test_hook.borrow_mut().take();
            });
        }
    }

    struct StorageRootLeaseTestHookGuard;

    impl Drop for StorageRootLeaseTestHookGuard {
        fn drop(&mut self) {
            STORAGE_ROOT_LEASE_TEST_HOOK.with(|test_hook| {
                test_hook.borrow_mut().take();
            });
        }
    }

    fn install_marker_publication_test_hook(
        test_hook: impl FnMut(MarkerPublicationTestStage) -> IoResult<()> + 'static,
    ) -> MarkerPublicationTestHookGuard {
        MARKER_PUBLICATION_TEST_HOOK.with(|slot| {
            let mut slot = slot.borrow_mut();
            assert!(
                slot.is_none(),
                "marker publication test hook already installed"
            );
            slot.replace(Box::new(test_hook));
        });
        MarkerPublicationTestHookGuard
    }

    fn install_storage_root_lease_test_hook(
        test_hook: impl FnMut(StorageRootLeaseTestStage) -> IoResult<()> + 'static,
    ) -> StorageRootLeaseTestHookGuard {
        STORAGE_ROOT_LEASE_TEST_HOOK.with(|slot| {
            let mut slot = slot.borrow_mut();
            assert!(
                slot.is_none(),
                "storage-root lease test hook already installed"
            );
            slot.replace(Box::new(test_hook));
        });
        StorageRootLeaseTestHookGuard
    }

    pub(super) fn run_marker_publication_test_hook(
        stage: MarkerPublicationTestStage,
    ) -> IoResult<()> {
        MARKER_PUBLICATION_TEST_HOOK.with(|test_hook| match test_hook.borrow_mut().as_mut() {
            Some(test_hook) => test_hook(stage),
            None => Ok(()),
        })
    }

    pub(super) fn run_storage_root_lease_test_hook(
        stage: StorageRootLeaseTestStage,
    ) -> IoResult<()> {
        STORAGE_ROOT_LEASE_TEST_HOOK.with(|test_hook| match test_hook.borrow_mut().as_mut() {
            Some(test_hook) => test_hook(stage),
            None => Ok(()),
        })
    }

    fn persist_marker_with_test_hook(
        paths: &ResolvedStoragePaths,
        test_hook: impl FnMut(MarkerPublicationTestStage) -> IoResult<()> + 'static,
    ) -> IoResult<()> {
        let _test_hook = install_marker_publication_test_hook(test_hook);
        paths.persist_marker()
    }

    fn try_acquire_with_test_hook(
        paths: &ResolvedStoragePaths,
        test_hook: impl FnMut(StorageRootLeaseTestStage) -> IoResult<()> + 'static,
    ) -> IoResult<StorageRootLeaseAttempt> {
        let _test_hook = install_storage_root_lease_test_hook(test_hook);
        StorageRootLease::try_acquire(paths)
    }

    fn prepared_paths(root: &Path) -> ResolvedStoragePaths {
        EngineConfig::default()
            .storage_root(root)
            .resolve_storage_paths()
            .unwrap()
            .prepare_storage_root()
            .unwrap()
    }

    fn acquired_lease(paths: &ResolvedStoragePaths) -> StorageRootLease {
        match StorageRootLease::try_acquire(paths).unwrap() {
            StorageRootLeaseAttempt::Acquired(lease) => lease,
            StorageRootLeaseAttempt::Contended { .. } => {
                panic!("fresh test root must be acquirable")
            }
        }
    }

    fn marker_temp_paths(paths: &ResolvedStoragePaths) -> Vec<PathBuf> {
        fs::read_dir(paths.storage_root_path())
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(is_storage_layout_temp_name)
            })
            .collect()
    }

    fn start_storage_lease_helper(root: &Path) -> (Child, u32) {
        let mut child = Command::new(current_exe().unwrap())
            .args(["--exact", STORAGE_LEASE_HELPER_TEST, "--nocapture"])
            .env(STORAGE_LEASE_HELPER_ENV, root)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        let (ready_tx, ready_rx) = mpsc::channel();
        thread::spawn(move || {
            let mut reader = BufReader::new(stdout);
            let mut ready_sent = false;
            loop {
                let mut line = String::new();
                match reader.read_line(&mut line) {
                    Ok(0) => {
                        if !ready_sent {
                            let _ = ready_tx.send(Err(StdIoError::new(
                                ErrorKind::UnexpectedEof,
                                "storage lease helper exited before readiness",
                            )));
                        }
                        return;
                    }
                    Ok(_) if !ready_sent && line.contains("READY ") => {
                        ready_sent = true;
                        let _ = ready_tx.send(Ok(line));
                    }
                    Ok(_) => {}
                    Err(err) => {
                        if !ready_sent {
                            let _ = ready_tx.send(Err(err));
                        }
                        return;
                    }
                }
            }
        });
        let line = ready_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("storage lease helper readiness watchdog expired")
            .expect("storage lease helper readiness read failed");
        let pid = line
            .split_once("READY ")
            .map(|(_, pid)| pid)
            .and_then(|pid| pid.trim().parse::<u32>().ok())
            .unwrap_or_else(|| {
                let status = child.try_wait().unwrap();
                panic!("invalid storage lease helper readiness `{line:?}`, status={status:?}")
            });
        (child, pid)
    }

    fn run_storage_marker_helper(root: &Path, stage: &str) {
        let status = Command::new(current_exe().unwrap())
            .args(["--exact", STORAGE_MARKER_HELPER_TEST, "--nocapture"])
            .env(STORAGE_MARKER_HELPER_ROOT_ENV, root)
            .env(STORAGE_MARKER_HELPER_STAGE_ENV, stage)
            .status()
            .unwrap();
        assert!(status.success(), "marker crash helper failed: {status}");
    }

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
    fn test_prepare_storage_root_canonicalizes_alias_and_preserves_layout() {
        let parent = TempDir::new().unwrap();
        let target = parent.path().join("target");
        create_dir(&target).unwrap();
        let alias = parent.path().join("alias");
        symlink(&target, &alias).unwrap();

        let paths = prepared_paths(&alias);
        assert_eq!(paths.storage_root_path(), target.canonicalize().unwrap());
        assert_eq!(paths.data_dir_path(), target.canonicalize().unwrap());
        assert_eq!(paths.durable_layout.data_dir, ".");
        assert_eq!(paths.durable_layout.log_dir, ".");
    }

    #[test]
    fn test_prepare_storage_root_create_failure_retains_io_source() {
        let root = TempDir::new().unwrap();
        let storage_root = root.path().join("storage-root-file");
        write(&storage_root, "not a directory").unwrap();
        let paths = EngineConfig::default()
            .storage_root(&storage_root)
            .resolve_storage_paths()
            .unwrap();

        let err = paths.prepare_storage_root().unwrap_err();
        assert_eq!(err.current_context().kind(), ErrorKind::AlreadyExists);
        assert!(err.downcast_ref::<StdIoError>().is_some());
        let output = format!("{err:?}");
        assert!(output.contains("create_storage_root"), "{output}");
        assert!(output.contains(storage_root.to_str().unwrap()), "{output}");
    }

    #[test]
    fn test_reserved_storage_control_namespace_is_rejected() {
        for err in [
            EngineConfig::default()
                .file(FileSystemConfig::default().data_dir(STORAGE_LOCK_FILE_NAME))
                .resolve_storage_paths()
                .unwrap_err(),
            EngineConfig::default()
                .trx(TrxSysConfig::default().log_dir(format!("{STORAGE_LOCK_FILE_NAME}/redo")))
                .resolve_storage_paths()
                .unwrap_err(),
            EngineConfig::default()
                .file(
                    FileSystemConfig::default()
                        .data_dir(format!("{STORAGE_LAYOUT_TEMP_PREFIX}12.34")),
                )
                .resolve_storage_paths()
                .unwrap_err(),
            EngineConfig::default()
                .index_swap_file(format!("{STORAGE_LAYOUT_TEMP_PREFIX}12.34/index.swp"))
                .resolve_storage_paths()
                .unwrap_err(),
        ] {
            assert_config_report(
                err,
                ConfigError::PathMustNotOverlapReservedLocation,
                &["reserved storage control entry"],
            );
        }

        EngineConfig::default()
            .file(FileSystemConfig::default().data_dir("."))
            .trx(TrxSysConfig::default().log_dir("."))
            .resolve_storage_paths()
            .unwrap();
    }

    #[test]
    fn test_storage_root_lease_writes_diagnostics_and_contends_without_mutation() {
        let root = TempDir::new().unwrap();
        let paths = prepared_paths(root.path());
        let lease = acquired_lease(&paths);
        let lock_path = root.path().join(STORAGE_LOCK_FILE_NAME);
        let original = read(&lock_path).unwrap();
        let record: StorageLockRecord = toml::from_str(from_utf8(&original).unwrap()).unwrap();
        assert_eq!(record.version, STORAGE_LOCK_RECORD_VERSION);
        assert_eq!(record.pid, process_id());
        assert_ne!(record.acquired_unix_ms, 0);

        match StorageRootLease::try_acquire(&paths).unwrap() {
            StorageRootLeaseAttempt::Acquired(_) => panic!("second lease must contend"),
            StorageRootLeaseAttempt::Contended {
                diagnostic,
                diagnostic_status,
            } => {
                assert_eq!(diagnostic_status, "valid");
                assert_eq!(diagnostic.unwrap().pid, process_id());
            }
        }
        assert_eq!(read(&lock_path).unwrap(), original);

        drop(lease);
        drop(acquired_lease(&paths));
        assert!(
            lock_path.exists(),
            "persistent lock file must not be unlinked"
        );
    }

    #[test]
    fn test_storage_root_lease_record_failure_releases_lock() {
        let root = TempDir::new().unwrap();
        let paths = prepared_paths(root.path());
        let err = match try_acquire_with_test_hook(&paths, |stage| {
            if stage == StorageRootLeaseTestStage::RecordTruncated {
                let err = StdIoError::other("injected owner-record failure");
                let kind = err.kind();
                Err(Report::new(err)
                    .change_context(IoError::from(kind))
                    .attach("injected lease test hook"))
            } else {
                Ok(())
            }
        }) {
            Ok(_) => panic!("injected owner-record failure must abort acquisition"),
            Err(err) => err,
        };
        assert!(format!("{err:?}").contains("injected owner-record failure"));
        drop(acquired_lease(&paths));
    }

    #[test]
    fn test_storage_root_contention_bounds_invalid_diagnostics() {
        let root = TempDir::new().unwrap();
        let paths = prepared_paths(root.path());
        let _lease = acquired_lease(&paths);
        let lock_path = root.path().join(STORAGE_LOCK_FILE_NAME);
        let mut external = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&lock_path)
            .unwrap();
        external
            .write_all(&vec![b'x'; STORAGE_LOCK_DIAGNOSTIC_MAX_BYTES + 128])
            .unwrap();
        drop(external);

        match StorageRootLease::try_acquire(&paths).unwrap() {
            StorageRootLeaseAttempt::Acquired(_) => panic!("second lease must contend"),
            StorageRootLeaseAttempt::Contended {
                diagnostic,
                diagnostic_status,
            } => {
                assert!(diagnostic.is_none());
                assert_eq!(diagnostic_status, "invalid");
            }
        }
    }

    #[test]
    fn storage_root_lease_subprocess_helper() {
        let Some(root) = var_os(STORAGE_LEASE_HELPER_ENV) else {
            return;
        };
        let paths = prepared_paths(Path::new(&root));
        let _lease = acquired_lease(&paths);
        writeln!(stdout(), "READY {}", process_id()).unwrap();
        stdout().flush().unwrap();
        let mut release = [0_u8; 1];
        stdin().read_exact(&mut release).unwrap();
    }

    #[test]
    fn test_storage_root_lease_cross_process_release_and_process_exit() {
        let parent = TempDir::new().unwrap();
        let root = parent.path().join("storage");
        let (mut child, child_pid) = start_storage_lease_helper(&root);
        let alias = parent.path().join("alias");
        symlink(&root, &alias).unwrap();
        let alias_paths = prepared_paths(&alias);
        match StorageRootLease::try_acquire(&alias_paths).unwrap() {
            StorageRootLeaseAttempt::Acquired(_) => {
                panic!("canonical storage-root alias must contend")
            }
            StorageRootLeaseAttempt::Contended { diagnostic, .. } => {
                assert_eq!(diagnostic.unwrap().pid, child_pid);
            }
        }

        child.stdin.take().unwrap().write_all(b"x").unwrap();
        assert!(child.wait().unwrap().success());
        drop(acquired_lease(&alias_paths));

        let (mut child, _) = start_storage_lease_helper(&root);
        child.kill().unwrap();
        let status = child.wait().unwrap();
        assert!(!status.success());
        drop(acquired_lease(&alias_paths));
    }

    #[test]
    fn storage_marker_subprocess_helper() {
        let (Some(root), Some(target_stage)) = (
            var_os(STORAGE_MARKER_HELPER_ROOT_ENV),
            var_os(STORAGE_MARKER_HELPER_STAGE_ENV),
        ) else {
            return;
        };
        let target_stage = target_stage.into_string().unwrap();
        let paths = prepared_paths(Path::new(&root));
        let _lease = acquired_lease(&paths);
        paths.cleanup_stale_marker_temps().unwrap();
        let hook_target_stage = target_stage.clone();
        let _ = persist_marker_with_test_hook(&paths, move |stage| {
            if stage.to_string() == hook_target_stage {
                exit(0);
            }
            Ok(())
        });
        panic!("marker helper did not observe target stage {target_stage}");
    }

    #[test]
    fn test_marker_process_exit_states_are_recoverable() {
        for (stage, final_installed) in [
            ("temp_written", false),
            ("temp_synced", false),
            ("final_installed", true),
            ("temp_removed", true),
        ] {
            let root = TempDir::new().unwrap();
            run_storage_marker_helper(root.path(), stage);
            let paths = prepared_paths(root.path());
            let _lease = acquired_lease(&paths);
            if final_installed {
                assert!(paths.validate_marker_if_present().unwrap(), "stage={stage}");
            } else {
                assert!(
                    !paths.validate_marker_if_present().unwrap(),
                    "stage={stage}"
                );
            }
            if stage == "final_installed" {
                let temps = marker_temp_paths(&paths);
                assert_eq!(temps.len(), 1);
                assert_eq!(
                    fs::metadata(paths.marker_path()).unwrap().ino(),
                    fs::metadata(&temps[0]).unwrap().ino()
                );
            }
            paths.cleanup_stale_marker_temps().unwrap();
            assert!(marker_temp_paths(&paths).is_empty(), "stage={stage}");
            if final_installed {
                assert!(paths.validate_marker_if_present().unwrap(), "stage={stage}");
            }
        }
    }

    #[test]
    fn test_marker_publication_stage_diagnostics_are_stable() {
        let publication_cases = [
            (MarkerPublicationStage::Serialize, "serialize"),
            (MarkerPublicationStage::CreateTemp, "create_temp"),
            (MarkerPublicationStage::WriteTemp, "write_temp"),
            (MarkerPublicationStage::SyncTemp, "sync_temp"),
            (MarkerPublicationStage::InstallFinal, "install_final"),
            (MarkerPublicationStage::RemoveTemp, "remove_temp"),
            (MarkerPublicationStage::SyncDirectory, "sync_directory"),
        ];
        for (stage, expected) in publication_cases {
            assert_eq!(stage.to_string(), expected);
        }

        let test_hook_cases = [
            (MarkerPublicationTestStage::TempCreated, "temp_created"),
            (MarkerPublicationTestStage::TempWritten, "temp_written"),
            (MarkerPublicationTestStage::TempSynced, "temp_synced"),
            (
                MarkerPublicationTestStage::FinalInstalled,
                "final_installed",
            ),
            (MarkerPublicationTestStage::TempRemoved, "temp_removed"),
        ];
        for (stage, expected) in test_hook_cases {
            assert_eq!(stage.to_string(), expected);
        }
    }

    #[test]
    fn test_storage_root_lock_rejects_symlink() {
        let root = TempDir::new().unwrap();
        let paths = prepared_paths(root.path());
        let target = root.path().join("lock-target");
        write(&target, "not authoritative").unwrap();
        symlink(&target, root.path().join(STORAGE_LOCK_FILE_NAME)).unwrap();

        let err = match StorageRootLease::try_acquire(&paths) {
            Ok(_) => panic!("lock-file symlink must be rejected"),
            Err(err) => err,
        };
        let output = format!("{err:?}");
        assert!(output.contains("open_storage_root_lock"), "{output}");
        assert!(output.contains(STORAGE_LOCK_FILE_NAME), "{output}");
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

        let err = EngineConfig::default()
            .index_swap_file("index.bin")
            .resolve_storage_paths()
            .unwrap_err();
        assert_config_report(
            err,
            ConfigError::PathMustUseRequiredSuffix,
            &["index_swap_file", ".swp", "index.bin"],
        );

        let err = EngineConfig::default()
            .trx(TrxSysConfig::default().log_file_stem("invalid*stem"))
            .resolve_storage_paths()
            .unwrap_err();
        assert_config_report(
            err,
            ConfigError::InvalidLogFileStem,
            &["plain file name", "invalid*stem"],
        );

        let err = EngineConfig::default()
            .file(FileSystemConfig::default().catalog_file_name("catalog.bin"))
            .resolve_storage_paths()
            .unwrap_err();
        assert_config_report(
            err,
            ConfigError::InvalidCatalogFileName,
            &["plain `.mtb` file name", "catalog.bin"],
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
        let data_dir = root.path().join("data");
        write(&data_dir, "not a directory").unwrap();
        let paths = EngineConfig::default()
            .storage_root(root.path())
            .file(FileSystemConfig::default().data_dir("data"))
            .resolve_storage_paths()
            .unwrap()
            .prepare_storage_root()
            .unwrap();

        let err: Report<IoError> = paths.ensure_directories().unwrap_err();
        assert_eq!(err.current_context().kind(), ErrorKind::AlreadyExists);
        let report = format!("{err:?}");
        assert!(report.contains("create table data directory"), "{report}");
        assert!(report.contains(data_dir.to_str().unwrap()), "{report}");
    }

    #[test]
    fn test_subordinate_directory_failures_retain_io_context() {
        let cases = [
            (
                EngineConfig::default().trx(TrxSysConfig::default().log_dir("log")),
                "log",
                "create redo log directory",
            ),
            (
                EngineConfig::default().data_buffer(
                    EvictableBufferPoolConfig::default().data_swap_file("data-swap/data.swp"),
                ),
                "data-swap",
                "create data swap-file parent directory",
            ),
            (
                EngineConfig::default().index_swap_file("index-swap/index.swp"),
                "index-swap",
                "create index swap-file parent directory",
            ),
        ];
        for (config, blocker, expected) in cases {
            let root = TempDir::new().unwrap();
            let paths = config
                .storage_root(root.path())
                .resolve_storage_paths()
                .unwrap()
                .prepare_storage_root()
                .unwrap();
            write(root.path().join(blocker), "not a directory").unwrap();
            let err = paths.ensure_directories().unwrap_err();
            let output = format!("{err:?}");
            assert!(output.contains(expected), "{output}");
            assert!(err.downcast_ref::<StdIoError>().is_some());
        }
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
        let original = "arbitrary existing marker bytes".to_string();
        write(paths.marker_path(), &original).unwrap();

        let err: Report<IoError> = paths.persist_marker().unwrap_err();
        assert_eq!(err.current_context().kind(), ErrorKind::AlreadyExists);
        let report = format!("{err:?}");
        assert!(
            report.contains("install marker final name with hard link"),
            "{report}"
        );
        assert!(report.contains("not_installed"), "{report}");
        assert!(
            report.contains(paths.marker_path().to_str().unwrap()),
            "{report}"
        );
        assert_eq!(read(paths.marker_path()).unwrap(), original.as_bytes());
        assert!(marker_temp_paths(&paths).is_empty());
    }

    #[test]
    fn test_marker_publication_cleans_preinstall_failure() {
        for failed_stage in [
            MarkerPublicationTestStage::TempCreated,
            MarkerPublicationTestStage::TempWritten,
            MarkerPublicationTestStage::TempSynced,
        ] {
            let root = TempDir::new().unwrap();
            let paths = prepared_paths(root.path());
            let err = persist_marker_with_test_hook(&paths, move |stage| {
                if stage == failed_stage {
                    let err = StdIoError::other("injected pre-install failure");
                    let kind = err.kind();
                    Err(Report::new(err)
                        .change_context(IoError::from(kind))
                        .attach("injected marker test hook"))
                } else {
                    Ok(())
                }
            })
            .unwrap_err();
            let output = format!("{err:?}");
            assert!(output.contains("not_installed"), "{output}");
            assert!(!paths.marker_path().exists());
            assert!(marker_temp_paths(&paths).is_empty());
        }
    }

    #[test]
    fn test_marker_publication_keeps_complete_final_after_install_failure() {
        for failed_stage in [
            MarkerPublicationTestStage::FinalInstalled,
            MarkerPublicationTestStage::TempRemoved,
        ] {
            let root = TempDir::new().unwrap();
            let paths = prepared_paths(root.path());
            let err = persist_marker_with_test_hook(&paths, move |stage| {
                if stage == failed_stage {
                    let err = StdIoError::other("injected post-install failure");
                    let kind = err.kind();
                    Err(Report::new(err)
                        .change_context(IoError::from(kind))
                        .attach("injected marker test hook"))
                } else {
                    Ok(())
                }
            })
            .unwrap_err();
            let output = format!("{err:?}");
            assert!(output.contains("installed_durability_unknown"), "{output}");
            assert!(paths.validate_marker_if_present().unwrap());
            assert!(marker_temp_paths(&paths).is_empty());
        }
    }

    #[test]
    fn test_stale_marker_temp_cleanup_is_exact_and_rejects_directories() {
        let root = TempDir::new().unwrap();
        let paths = prepared_paths(root.path());
        let stale = root
            .path()
            .join(format!("{STORAGE_LAYOUT_TEMP_PREFIX}12.34"));
        let unrelated = root
            .path()
            .join(format!("{STORAGE_LAYOUT_TEMP_PREFIX}12.invalid"));
        write(&stale, "partial").unwrap();
        write(&unrelated, "keep").unwrap();
        paths.cleanup_stale_marker_temps().unwrap();
        assert!(!stale.exists());
        assert!(unrelated.exists());

        let stale_dir = root
            .path()
            .join(format!("{STORAGE_LAYOUT_TEMP_PREFIX}56.78"));
        create_dir(&stale_dir).unwrap();
        let err = paths.cleanup_stale_marker_temps().unwrap_err();
        let output = format!("{err:?}");
        assert!(output.contains("reject_directory"), "{output}");
        assert!(output.contains(stale_dir.to_str().unwrap()), "{output}");
        assert!(stale_dir.is_dir());
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
            Some(ErrorKind::IsADirectory)
        );
        assert!(err.downcast_ref::<StdIoError>().is_some());
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
