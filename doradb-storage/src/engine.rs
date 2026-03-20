//! Storage engine for DoraDB.
//!
//! This module provides the main entry point of the storage engine,
//! including start, stop, recover, and execute commands. See
//! `docs/engine-component-lifetime.md` for the runtime-versus-owner lifetime
//! model that this module and [`crate::component::ComponentRegistry`] enforce.
use crate::buffer::{EvictableBufferPoolConfig, PoolRole};
use crate::catalog::Catalog;
use crate::component::{
    ComponentRegistry, DiskPoolConfig, IndexPoolConfig, MetaPoolConfig, RegistryBuilder,
};
use crate::error::{Error, Result};
use crate::file::table_fs::{TableFileSystem, TableFileSystemConfig};
use crate::quiescent::QuiescentGuard;
use crate::session::Session;
use crate::storage_path::ResolvedStoragePaths;
use crate::trx::sys::TransactionSystem;
use crate::trx::sys_conf::TrxSysConfig;
use crate::{DiskPool, IndexPool, MemPool, MetaPool};
use byte_unit::Byte;
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EngineLifecycleState {
    Running = 0,
    ShuttingDown = 1,
    Shutdown = 2,
}

struct EngineLifecycle {
    state: AtomicU8,
    admission_gate: RwLock<()>,
    finalize_lock: Mutex<()>,
}

impl EngineLifecycle {
    #[inline]
    fn new() -> Self {
        Self {
            state: AtomicU8::new(EngineLifecycleState::Running as u8),
            admission_gate: RwLock::new(()),
            finalize_lock: Mutex::new(()),
        }
    }

    #[inline]
    fn state(&self) -> EngineLifecycleState {
        match self.state.load(Ordering::Acquire) {
            x if x == EngineLifecycleState::Running as u8 => EngineLifecycleState::Running,
            x if x == EngineLifecycleState::ShuttingDown as u8 => {
                EngineLifecycleState::ShuttingDown
            }
            x if x == EngineLifecycleState::Shutdown as u8 => EngineLifecycleState::Shutdown,
            x => panic!("invalid engine lifecycle state: {x}"),
        }
    }

    #[inline]
    fn set_state(&self, state: EngineLifecycleState) {
        self.state.store(state as u8, Ordering::Release);
    }
}

/// Storage engine owner.
///
/// `Engine` owns teardown-only state such as the top-level
/// [`ComponentRegistry`], while [`EngineRef`] and [`Session`] hold only the
/// shared runtime handle in [`EngineInner`]. Explicit shutdown and final owner
/// drop therefore stay with the owner object instead of the cloneable runtime
/// access path.
pub struct Engine {
    inner: Option<Arc<EngineInner>>,
    components: Option<ComponentRegistry>,
}

impl Deref for Engine {
    type Target = EngineInner;

    #[inline]
    fn deref(&self) -> &EngineInner {
        self.inner().as_ref()
    }
}

impl Engine {
    #[inline]
    fn inner(&self) -> &Arc<EngineInner> {
        self.inner
            .as_ref()
            .expect("engine owner keeps runtime handle until drop")
    }

    #[inline]
    fn components(&self) -> &ComponentRegistry {
        self.components
            .as_ref()
            .expect("engine owner keeps component registry until drop")
    }

    #[inline]
    fn release_owned_parts(&mut self) -> (Arc<EngineInner>, ComponentRegistry) {
        let inner = self
            .inner
            .take()
            .expect("engine runtime handle is present until final drop");
        let components = self
            .components
            .take()
            .expect("engine component registry is present until final drop");
        (inner, components)
    }

    /// Try to create a new session while the engine is still running.
    #[inline]
    pub fn try_new_session(&self) -> Result<Session> {
        self.inner()
            .with_running_admission(|| Session::new(self.new_ref()))
    }

    /// Return the shared catalog handle.
    #[inline]
    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    /// Clone the shared runtime handle.
    #[inline]
    pub fn new_ref(&self) -> EngineRef {
        EngineRef(Arc::clone(self.inner()))
    }

    /// Start idempotent engine shutdown.
    ///
    /// Shutdown rejects new work immediately, waits for user-owned
    /// [`EngineRef`]s and sessions to drain, then dispatches component shutdown
    /// in reverse registration order.
    #[inline]
    pub fn shutdown(&self) -> Result<()> {
        self.finalize_shutdown()
    }

    #[inline]
    fn finalize_shutdown(&self) -> Result<()> {
        let inner = self.inner();
        let _finalize = inner.lifecycle.finalize_lock.lock();
        if inner.lifecycle.state() == EngineLifecycleState::Shutdown {
            return Ok(());
        }

        {
            let _gate = inner.lifecycle.admission_gate.write();
            if inner.lifecycle.state() == EngineLifecycleState::Running {
                inner
                    .lifecycle
                    .set_state(EngineLifecycleState::ShuttingDown);
            }
        }

        // Any live session/transaction/statement keeps an `EngineRef` alive
        // through `SessionState`. Requiring the last strong reference here
        // gives transaction-system shutdown a clean point where user-originated
        // work has already drained before we start disabling runtime state.
        let strong_count = Arc::strong_count(inner);
        if strong_count != 1 {
            return Err(Error::StorageEngineShutdownBusy(strong_count - 1));
        }

        self.components().shutdown_all();
        inner.lifecycle.set_state(EngineLifecycleState::Shutdown);
        Ok(())
    }
}

impl Drop for Engine {
    #[inline]
    fn drop(&mut self) {
        if let Err(err) = self.finalize_shutdown() {
            if matches!(err, Error::StorageEngineShutdownBusy(_)) {
                // Fatal owner-drop violations still need to stop background
                // workers, but the owner registry cannot be dropped while
                // leaked runtime refs still retain component guards.
                let components = self
                    .components
                    .take()
                    .expect("engine component registry is present until drop");
                components.shutdown_all();
                std::mem::forget(components);
            }
            panic!("fatal: engine shutdown failed: {err}");
        }

        // Drop the shared runtime handle before registry-owned component
        // owners. That makes the owner/runtime split explicit instead of
        // relying on incidental struct layout.
        let (inner, components) = self.release_owned_parts();
        drop(inner);
        drop(components);
    }
}

/// Cloneable shared runtime handle for the storage engine.
///
/// `EngineRef` intentionally does not own shutdown orchestration. It only
/// exposes the runtime state needed by sessions and internal subsystems.
#[derive(Clone)]
pub struct EngineRef(Arc<EngineInner>);

impl Deref for EngineRef {
    type Target = EngineInner;
    #[inline]
    fn deref(&self) -> &EngineInner {
        &self.0
    }
}

impl EngineRef {
    /// Try to create a new session while the engine is still running.
    #[inline]
    pub fn try_new_session(&self) -> Result<Session> {
        self.0.with_running_admission(|| Session::new(self.clone()))
    }

    /// Return the shared catalog handle.
    #[inline]
    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }
}

/// Shared runtime state for an [`Engine`].
///
/// The fields here are the cloneable handles that sessions and other runtime
/// objects may retain. Owner-only teardown state lives on [`Engine`] itself.
pub struct EngineInner {
    /// Shared catalog handle.
    pub catalog: QuiescentGuard<Catalog>,
    /// Shared transaction-system handle.
    pub trx_sys: QuiescentGuard<TransactionSystem>,
    /// Metadata pool used for block-index and catalog tables.
    pub meta_pool: MetaPool,
    /// Secondary-index pool.
    pub index_pool: IndexPool,
    /// In-memory row-page pool for table data.
    pub mem_pool: MemPool,
    /// Table-file subsystem that runs persistent page IO.
    pub table_fs: QuiescentGuard<TableFileSystem>,
    /// Global readonly pool for persisted table-file reads.
    pub disk_pool: DiskPool,
    lifecycle: EngineLifecycle,
}

impl EngineInner {
    #[inline]
    pub(crate) fn with_running_admission<T>(&self, f: impl FnOnce() -> T) -> Result<T> {
        let _gate = self.lifecycle.admission_gate.read();
        if self.lifecycle.state() != EngineLifecycleState::Running {
            return Err(Error::StorageEngineShutdown);
        }
        Ok(f())
    }
}

const DEFAULT_META_BUFFER: usize = 32 * 1024 * 1024;
const DEFAULT_INDEX_BUFFER: usize = 1024 * 1024 * 1024;

/// Storage-engine configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineConfig {
    storage_root: PathBuf,
    trx: TrxSysConfig,
    meta_buffer: Byte,
    index_buffer: Byte,
    data_buffer: EvictableBufferPoolConfig,
    file: TableFileSystemConfig,
}

impl Default for EngineConfig {
    #[inline]
    fn default() -> Self {
        EngineConfig {
            storage_root: PathBuf::from("."),
            trx: TrxSysConfig::default(),
            meta_buffer: Byte::from_u64(DEFAULT_META_BUFFER as u64),
            index_buffer: Byte::from_u64(DEFAULT_INDEX_BUFFER as u64),
            data_buffer: EvictableBufferPoolConfig::default(),
            file: TableFileSystemConfig::default(),
        }
    }
}

impl EngineConfig {
    /// Set the storage root directory.
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

    /// Set the metadata-pool size.
    #[inline]
    pub fn meta_buffer(mut self, meta_buffer: impl Into<Byte>) -> Self {
        self.meta_buffer = meta_buffer.into();
        self
    }

    /// Set the secondary-index-pool size.
    #[inline]
    pub fn index_buffer(mut self, index_buffer: impl Into<Byte>) -> Self {
        self.index_buffer = index_buffer.into();
        self
    }

    /// Set the row-data buffer-pool configuration.
    #[inline]
    pub fn data_buffer(mut self, data_buffer: EvictableBufferPoolConfig) -> Self {
        self.data_buffer = data_buffer;
        self
    }

    /// Set the table-file subsystem configuration.
    #[inline]
    pub fn file(mut self, file: TableFileSystemConfig) -> Self {
        self.file = file;
        self
    }

    /// Build the storage engine and all registered components.
    #[inline]
    pub async fn build(self) -> Result<Engine> {
        let resolved = ResolvedStoragePaths::resolve(
            &self.storage_root,
            &self.file.data_dir,
            &self.file.catalog_file_name,
            self.trx.log_dir_ref(),
            self.trx.log_file_stem_ref(),
            self.trx.log_partitions,
            self.data_buffer.data_swap_file_ref(),
        )?;
        resolved.validate_marker_if_present()?;
        resolved.ensure_directories()?;

        let file = self.file.data_dir(resolved.data_dir_path());
        let readonly_buffer_size = file.readonly_buffer_size;
        let trx_cfg = self.trx.log_dir(resolved.log_dir_path());
        let mut builder = RegistryBuilder::new();
        // Components are registered in one fixed dependency order. Reverse
        // registration order then defines both explicit shutdown order and the
        // final owner drop order.
        builder
            .build::<DiskPool>(DiskPoolConfig::new(readonly_buffer_size))
            .await?;
        builder.build::<TableFileSystem>(file).await?;
        builder
            .build::<MetaPool>(MetaPoolConfig::new(self.meta_buffer.as_u64() as usize))
            .await?;
        builder
            .build::<IndexPool>(IndexPoolConfig::new(self.index_buffer.as_u64() as usize))
            .await?;
        builder
            .build::<MemPool>(
                self.data_buffer
                    .role(PoolRole::Mem)
                    .data_swap_file(resolved.data_swap_file_path()),
            )
            .await?;
        // Catalog owns user-table runtimes, and those runtimes retain buffer-pool
        // guards for row/index/readonly access. Register catalog after the pools it
        // can pin so reverse shutdown/drop order releases table guards before pool
        // owners are torn down.
        builder.build::<Catalog>(()).await?;
        builder.build::<TransactionSystem>(trx_cfg).await?;

        resolved.persist_marker_if_missing()?;
        let registry = builder.finish()?;
        let catalog = registry.dependency::<Catalog>()?;
        let trx_sys = registry.dependency::<TransactionSystem>()?;
        let meta_pool = registry.dependency::<MetaPool>()?;
        let index_pool = registry.dependency::<IndexPool>()?;
        let mem_pool = registry.dependency::<MemPool>()?;
        let table_fs = registry.dependency::<TableFileSystem>()?;
        let disk_pool = registry.dependency::<DiskPool>()?;
        let engine_inner = EngineInner {
            catalog,
            trx_sys,
            meta_pool,
            index_pool,
            mem_pool,
            table_fs,
            disk_pool,
            lifecycle: EngineLifecycle::new(),
        };
        Ok(Engine {
            inner: Some(Arc::new(engine_inner)),
            components: Some(registry),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::{FixedBufferPool, GlobalReadonlyBufferPool};
    use crate::catalog::storage::CatalogStorage;
    use crate::catalog::tests::table1;
    use crate::error::Error;
    use crate::file::build_test_fs_in;
    use crate::storage_path::STORAGE_LAYOUT_FILE_NAME;
    use std::fs;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use tempfile::TempDir;

    const TEST_POOL_BYTES: usize = 64 * 1024 * 1024;

    fn test_engine_config_for(root: &std::path::Path) -> EngineConfig {
        EngineConfig::default()
            .storage_root(root)
            .meta_buffer(TEST_POOL_BYTES)
            .index_buffer(TEST_POOL_BYTES)
            .data_buffer(
                EvictableBufferPoolConfig::default()
                    .role(PoolRole::Mem)
                    .max_mem_size(TEST_POOL_BYTES)
                    .max_file_size(128usize * 1024 * 1024),
            )
            .file(TableFileSystemConfig::default().readonly_buffer_size(TEST_POOL_BYTES))
            .trx(TrxSysConfig::default().skip_recovery(false))
    }

    #[test]
    fn test_engine_config() {
        let config = EngineConfig::default();
        let config_str = toml::to_string(&config).unwrap();
        assert!(config_str.contains("storage_root"));
        assert!(config_str.contains("data_swap_file"));
        assert!(config_str.contains("log_dir"));
        assert!(config_str.contains("log_file_stem"));
    }

    #[test]
    fn test_storage_layout_marker_allows_data_swap_change() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = test_engine_config_for(root.path()).build().await.unwrap();
            drop(engine);

            let engine = test_engine_config_for(root.path())
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .role(PoolRole::Mem)
                        .max_mem_size(64usize * 1024 * 1024)
                        .max_file_size(128usize * 1024 * 1024)
                        .data_swap_file("alt-data.bin"),
                )
                .build()
                .await
                .unwrap();
            drop(engine);
        });
    }

    #[test]
    fn test_storage_layout_marker_rejects_data_dir_change() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = test_engine_config_for(root.path()).build().await.unwrap();
            drop(engine);

            let err = match test_engine_config_for(root.path())
                .file(TableFileSystemConfig::default().data_dir("data"))
                .build()
                .await
            {
                Ok(_) => panic!("expected storage layout mismatch"),
                Err(err) => err,
            };
            assert!(matches!(err, Error::StorageLayoutMismatch(_)));
        });
    }

    #[test]
    fn test_storage_layout_mismatch_does_not_create_new_directories() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = test_engine_config_for(root.path()).build().await.unwrap();
            drop(engine);

            let new_data_dir = root.path().join("other-data");
            assert!(!new_data_dir.exists());

            let err = match test_engine_config_for(root.path())
                .file(TableFileSystemConfig::default().data_dir("other-data"))
                .build()
                .await
            {
                Ok(_) => panic!("expected storage layout mismatch"),
                Err(err) => err,
            };
            assert!(matches!(err, Error::StorageLayoutMismatch(_)));
            assert!(!new_data_dir.exists());
        });
    }

    #[test]
    fn test_storage_layout_marker_allows_storage_root_relocation() {
        smol::block_on(async {
            let parent = TempDir::new().unwrap();
            let root_a = parent.path().join("root-a");
            let root_b = parent.path().join("root-b");

            let engine = test_engine_config_for(&root_a).build().await.unwrap();
            drop(engine);

            fs::rename(&root_a, &root_b).unwrap();

            let engine = test_engine_config_for(&root_b).build().await.unwrap();
            drop(engine);
        });
    }

    #[test]
    fn test_failed_startup_does_not_persist_storage_layout_marker() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let marker_path = root.path().join(STORAGE_LAYOUT_FILE_NAME);

            let err = match EngineConfig::default()
                .storage_root(root.path())
                .meta_buffer(TEST_POOL_BYTES)
                .index_buffer(TEST_POOL_BYTES)
                .file(TableFileSystemConfig::default().readonly_buffer_size(TEST_POOL_BYTES))
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .role(PoolRole::Mem)
                        .max_mem_size(1024usize * 1024)
                        .max_file_size(2usize * 1024 * 1024),
                )
                .trx(TrxSysConfig::default().skip_recovery(false))
                .build()
                .await
            {
                Ok(_) => panic!("expected startup failure"),
                Err(err) => err,
            };
            assert!(matches!(err, Error::BufferPoolSizeTooSmall));
            assert!(!marker_path.exists());

            let engine = test_engine_config_for(root.path())
                .file(
                    TableFileSystemConfig::default()
                        .data_dir("data")
                        .readonly_buffer_size(TEST_POOL_BYTES),
                )
                .build()
                .await
                .unwrap();
            drop(engine);
            assert!(marker_path.exists());
        });
    }

    #[test]
    fn test_engine_shutdown_is_idempotent_and_rejects_new_work() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = test_engine_config_for(root.path()).build().await.unwrap();

            engine.shutdown().unwrap();
            engine.shutdown().unwrap();

            let err = match engine.try_new_session() {
                Ok(_) => panic!("expected shutdown error"),
                Err(err) => err,
            };
            assert!(matches!(err, Error::StorageEngineShutdown));

            let engine_ref = engine.new_ref();
            let err = match engine_ref.try_new_session() {
                Ok(_) => panic!("expected shutdown error"),
                Err(err) => err,
            };
            assert!(matches!(err, Error::StorageEngineShutdown));
            drop(engine_ref);
        });
    }

    #[test]
    fn test_engine_shutdown_busy_until_refs_drop() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = test_engine_config_for(root.path()).build().await.unwrap();
            let mut session = engine.try_new_session().unwrap();

            let err = match engine.shutdown() {
                Ok(_) => panic!("expected busy shutdown error"),
                Err(err) => err,
            };
            assert!(matches!(err, Error::StorageEngineShutdownBusy(1)));

            let err = match engine.try_new_session() {
                Ok(_) => panic!("expected shutdown error"),
                Err(err) => err,
            };
            assert!(matches!(err, Error::StorageEngineShutdown));

            let err = match session.try_begin_trx() {
                Ok(_) => panic!("expected shutdown error"),
                Err(err) => err,
            };
            assert!(matches!(err, Error::StorageEngineShutdown));

            drop(session);
            engine.shutdown().unwrap();
        });
    }

    #[test]
    fn test_engine_shutdown_keeps_external_table_refs_valid() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = test_engine_config_for(root.path()).build().await.unwrap();
            let table_id = table1(&engine).await;
            let table = engine.catalog().get_table(table_id).await.unwrap();

            engine.shutdown().unwrap();

            assert_eq!(table.table_id(), table_id);
            assert!(engine.catalog().get_table(table_id).await.is_some());
        });
    }

    #[test]
    fn test_drop_engine_panics_when_extra_refs_exist() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = test_engine_config_for(root.path()).build().await.unwrap();
            let leaked_ref = engine.new_ref();

            let res = catch_unwind(AssertUnwindSafe(|| drop(engine)));
            assert!(res.is_err());

            drop(leaked_ref);
        });
    }

    #[test]
    fn test_unstarted_transaction_system_shutdown_is_safe() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let data_dir = temp_dir.path().join("data");
            let log_dir = temp_dir.path().join("log");
            let swap_file = temp_dir.path().join("data.bin");
            fs::create_dir_all(&data_dir).unwrap();
            fs::create_dir_all(&log_dir).unwrap();
            let table_fs = build_test_fs_in(&data_dir);
            let meta_pool = crate::quiescent::QuiescentBox::new(
                FixedBufferPool::with_capacity(PoolRole::Meta, TEST_POOL_BYTES).unwrap(),
            );
            let index_pool = crate::quiescent::QuiescentBox::new(
                FixedBufferPool::with_capacity(PoolRole::Index, TEST_POOL_BYTES).unwrap(),
            );
            let mem_pool = crate::quiescent::QuiescentBox::new(
                EvictableBufferPoolConfig::default()
                    .role(PoolRole::Mem)
                    .data_swap_file(&swap_file)
                    .max_mem_size(TEST_POOL_BYTES)
                    .max_file_size(128usize * 1024 * 1024)
                    .build()
                    .unwrap()
                    .0,
            );
            let disk_pool = crate::quiescent::QuiescentBox::new(
                GlobalReadonlyBufferPool::with_capacity(PoolRole::Disk, TEST_POOL_BYTES).unwrap(),
            );
            let catalog = crate::quiescent::QuiescentBox::new(
                Catalog::new(
                    CatalogStorage::new(
                        meta_pool.guard(),
                        index_pool.guard(),
                        &table_fs,
                        disk_pool.guard(),
                    )
                    .await
                    .unwrap(),
                )
                .await
                .unwrap(),
            );

            let pending = TrxSysConfig::default()
                .log_dir(&log_dir)
                .log_file_stem("pending-startup-cleanup")
                .skip_recovery(true)
                .prepare(
                    meta_pool.guard(),
                    index_pool.guard(),
                    mem_pool.guard(),
                    table_fs.guard(),
                    disk_pool.guard(),
                    catalog.guard(),
                )
                .await
                .unwrap();

            let (trx_sys, startup) = pending.into_parts();
            drop(startup);
            drop(trx_sys);
        });
    }
}
