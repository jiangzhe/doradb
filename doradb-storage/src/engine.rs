//! Storage engine for DoraDB.
//!
//! This module provides the main entry point of the storage engine,
//! including start, stop, recover, and execute commands. See
//! `docs/engine-component-lifetime.md` for the runtime-versus-owner lifetime
//! model that this module and [`crate::component::ComponentRegistry`] enforce.
use crate::buffer::PoolRole;
use crate::buffer::SharedPoolEvictorWorkers;
use crate::catalog::Catalog;
use crate::component::{
    ComponentRegistry, DiskPoolConfig, IndexPoolConfig, MetaPoolConfig, RegistryBuilder,
};
use crate::conf::EngineConfig;
use crate::error::{Error, Result};
use crate::file::fs::{FileSystem, FileSystemWorkers};
use crate::quiescent::QuiescentGuard;
use crate::session::Session;
use crate::trx::sys::TransactionSystem;
use crate::{DiskPool, IndexPool, MemPool, MetaPool};
use parking_lot::{Mutex, RwLock};
use std::ops::Deref;
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
        let inner = self.inner();
        inner.with_running_admission(|| Session::new(EngineRef(Arc::clone(inner))))
    }

    /// Return the shared catalog handle.
    #[inline]
    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    /// Try to clone the shared runtime handle while the engine is still
    /// running.
    #[inline]
    pub fn new_ref(&self) -> Result<EngineRef> {
        let inner = self.inner();
        inner.with_running_admission(|| EngineRef(Arc::clone(inner)))
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
        // through `SessionState`. Owner-side `EngineRef` creation and session
        // admission also hold the read side of `admission_gate`, so once the
        // Running -> ShuttingDown transition completes under the write lock no
        // new owner-created runtime handles can appear before this snapshot.
        // Requiring the last strong reference here gives transaction-system
        // shutdown a clean point where user-originated work has already
        // drained before we start disabling runtime state.
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
    pub table_fs: QuiescentGuard<FileSystem>,
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
        self.trx_sys.ensure_runtime_healthy()?;
        Ok(f())
    }
}

impl EngineConfig {
    /// Build the storage engine and all registered components.
    #[inline]
    pub async fn build(self) -> Result<Engine> {
        let resolved = self.resolve_storage_paths()?;
        resolved.validate_marker_if_present()?;
        // Startup prefers a small, durable-safety-focused preflight over trying
        // to exhaust every possible path conflict up front. It is acceptable for
        // later setup steps to fail, but those failures must not clobber durable
        // files or persist `storage-layout.toml` before the engine is fully built.
        resolved.ensure_directories()?;

        let file = self.file.data_dir(resolved.data_dir_path());
        let readonly_buffer_size = file.readonly_buffer_size;
        let trx_cfg = self.trx.log_dir(resolved.log_dir_path());
        let mut builder = RegistryBuilder::new();
        // Components are registered in one fixed dependency order. Reverse
        // registration order then defines both explicit shutdown order and the
        // final owner drop order.
        builder.build::<FileSystem>(file).await?;
        builder
            .build::<DiskPool>(DiskPoolConfig::new(readonly_buffer_size))
            .await?;
        builder
            .build::<MetaPool>(MetaPoolConfig::new(self.meta_buffer.as_u64() as usize))
            .await?;
        builder
            .build::<IndexPool>(IndexPoolConfig::new(
                self.index_buffer.as_u64() as usize,
                resolved.index_swap_file_path(),
                self.index_max_file_size.as_u64() as usize,
            ))
            .await?;
        builder
            .build::<MemPool>(
                self.data_buffer
                    .role(PoolRole::Mem)
                    .data_swap_file(resolved.data_swap_file_path()),
            )
            .await?;
        builder.build::<FileSystemWorkers>(()).await?;
        builder.build::<SharedPoolEvictorWorkers>(()).await?;
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
        let table_fs = registry.dependency::<FileSystem>()?;
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
    use crate::buffer::test_io_backend_stats_handle_identity as pool_stats_handle_identity;
    use crate::catalog::tests::table1;
    use crate::conf::consts::STORAGE_LAYOUT_FILE_NAME;
    use crate::conf::{EngineConfig, EvictableBufferPoolConfig, FileSystemConfig, TrxSysConfig};
    use crate::error::Error;
    use crate::file::fs::tests::io_backend_stats_handle_identity as fs_stats_handle_identity;
    use std::fs;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::sync::{Arc as StdArc, Barrier};
    use tempfile::TempDir;

    const TEST_POOL_BYTES: usize = 64 * 1024 * 1024;

    fn test_engine_config_for(root: &std::path::Path) -> EngineConfig {
        EngineConfig::default()
            .storage_root(root)
            .meta_buffer(TEST_POOL_BYTES)
            .index_buffer(TEST_POOL_BYTES)
            .index_max_file_size(128usize * 1024 * 1024)
            .data_buffer(
                EvictableBufferPoolConfig::default()
                    .role(PoolRole::Mem)
                    .max_mem_size(TEST_POOL_BYTES)
                    .max_file_size(128usize * 1024 * 1024),
            )
            .file(FileSystemConfig::default().readonly_buffer_size(TEST_POOL_BYTES))
            .trx(TrxSysConfig::default().skip_recovery(false))
    }

    #[test]
    fn test_engine_config() {
        let config = EngineConfig::default();
        let config_str = toml::to_string(&config).unwrap();
        assert!(config_str.contains("storage_root"));
        assert!(config_str.contains("index_swap_file"));
        assert!(config_str.contains("data_swap_file"));
        assert!(config_str.contains("log_dir"));
        assert!(config_str.contains("log_file_stem"));
        assert!(!config_str.contains("max_io_depth"));
    }

    #[test]
    fn test_engine_component_order_uses_shared_storage_and_evictor_workers() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = test_engine_config_for(root.path()).build().await.unwrap();

            assert_eq!(
                engine.components().component_names(),
                vec![
                    "fs",
                    "disk_pool",
                    "meta_pool",
                    "index_pool",
                    "mem_pool",
                    "fs_workers",
                    "shared_pool_evictor_workers",
                    "catalog",
                    "trx_sys",
                    "trx_sys_workers",
                ]
            );
        });
    }

    #[test]
    fn test_engine_shared_storage_runtime_reuses_one_backend_stats_handle() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = test_engine_config_for(root.path())
                .file(
                    FileSystemConfig::default()
                        .io_depth(7)
                        .readonly_buffer_size(TEST_POOL_BYTES),
                )
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .role(PoolRole::Mem)
                        .max_mem_size(TEST_POOL_BYTES)
                        .max_file_size(128usize * 1024 * 1024),
                )
                .build()
                .await
                .unwrap();

            let table_stats = fs_stats_handle_identity(&engine.table_fs);
            let mem_stats = pool_stats_handle_identity(&engine.mem_pool);
            let index_stats = pool_stats_handle_identity(&engine.index_pool);

            assert_eq!(table_stats, mem_stats);
            assert_eq!(table_stats, index_stats);
        });
    }

    #[test]
    fn test_engine_shared_storage_io_depth_comes_from_file_system_config() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = test_engine_config_for(root.path())
                .file(
                    FileSystemConfig::default()
                        .io_depth(7)
                        .readonly_buffer_size(TEST_POOL_BYTES),
                )
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .role(PoolRole::Mem)
                        .max_mem_size(TEST_POOL_BYTES)
                        .max_file_size(128usize * 1024 * 1024),
                )
                .build()
                .await
                .unwrap();

            assert_eq!(engine.table_fs.configured_io_depth(), 7);
            assert_eq!(
                engine.mem_pool.io_backend_stats(),
                engine.table_fs.io_backend_stats()
            );
            assert_eq!(
                engine.index_pool.io_backend_stats(),
                engine.table_fs.io_backend_stats()
            );
        });
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
                        .data_swap_file("alt-data.swp"),
                )
                .build()
                .await
                .unwrap();
            drop(engine);
        });
    }

    #[test]
    fn test_storage_layout_marker_allows_index_swap_change() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = test_engine_config_for(root.path()).build().await.unwrap();
            drop(engine);

            let engine = test_engine_config_for(root.path())
                .index_swap_file("alt-index.swp")
                .build()
                .await
                .unwrap();
            drop(engine);
        });
    }

    #[test]
    fn test_engine_startup_creates_default_swap_files() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = test_engine_config_for(root.path()).build().await.unwrap();
            drop(engine);

            assert!(root.path().join("data.swp").exists());
            assert!(root.path().join("index.swp").exists());
        });
    }

    #[test]
    fn test_storage_layout_marker_rejects_data_dir_change() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = test_engine_config_for(root.path()).build().await.unwrap();
            drop(engine);

            let err = match test_engine_config_for(root.path())
                .file(FileSystemConfig::default().data_dir("data"))
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
                .file(FileSystemConfig::default().data_dir("other-data"))
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
                .file(FileSystemConfig::default().readonly_buffer_size(TEST_POOL_BYTES))
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
                    FileSystemConfig::default()
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
    fn test_invalid_swap_path_startup_does_not_persist_storage_layout_marker() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let marker_path = root.path().join(STORAGE_LAYOUT_FILE_NAME);

            let err = match test_engine_config_for(root.path())
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .role(PoolRole::Mem)
                        .max_mem_size(TEST_POOL_BYTES)
                        .max_file_size(128usize * 1024 * 1024)
                        .data_swap_file("catalog.mtb/data.swp"),
                )
                .build()
                .await
            {
                Ok(_) => panic!("expected startup failure"),
                Err(err) => err,
            };
            assert!(matches!(err, Error::InvalidStoragePath(_)));
            assert!(!marker_path.exists());
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

            let err = match engine.new_ref() {
                Ok(_) => panic!("expected shutdown error"),
                Err(err) => err,
            };
            assert!(matches!(err, Error::StorageEngineShutdown));
        });
    }

    #[test]
    fn test_engine_ref_rejected_once_shutdown_begins() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = test_engine_config_for(root.path()).build().await.unwrap();
            let engine_ref = engine.new_ref().unwrap();

            let err = match engine.shutdown() {
                Ok(_) => panic!("expected busy shutdown error"),
                Err(err) => err,
            };
            assert!(matches!(err, Error::StorageEngineShutdownBusy(1)));

            let err = match engine_ref.try_new_session() {
                Ok(_) => panic!("expected shutdown error"),
                Err(err) => err,
            };
            assert!(matches!(err, Error::StorageEngineShutdown));

            let err = match engine.new_ref() {
                Ok(_) => panic!("expected shutdown error"),
                Err(err) => err,
            };
            assert!(matches!(err, Error::StorageEngineShutdown));

            drop(engine_ref);
            engine.shutdown().unwrap();
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
            assert!(!session.in_trx());

            drop(session);
            engine.shutdown().unwrap();
        });
    }

    #[test]
    fn test_same_session_rejects_overlapping_transactions() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = test_engine_config_for(root.path()).build().await.unwrap();
            let mut session = engine.try_new_session().unwrap();

            let trx = session.try_begin_trx().unwrap().unwrap();
            assert!(session.in_trx());
            assert!(session.try_begin_trx().unwrap().is_none());

            trx.rollback().await.unwrap();
            assert!(!session.in_trx());
        });
    }

    #[test]
    fn test_same_session_reuse_after_commit() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = test_engine_config_for(root.path()).build().await.unwrap();
            let mut session = engine.try_new_session().unwrap();

            let mut trx = session.try_begin_trx().unwrap().unwrap();
            trx.add_pseudo_redo_log_entry();
            let cts = trx.commit().await.unwrap();
            assert!(cts > 0);
            assert!(!session.in_trx());

            let trx = session.try_begin_trx().unwrap().unwrap();
            trx.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_same_session_reuse_after_rollback() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = test_engine_config_for(root.path()).build().await.unwrap();
            let mut session = engine.try_new_session().unwrap();

            let trx = session.try_begin_trx().unwrap().unwrap();
            trx.rollback().await.unwrap();
            assert!(!session.in_trx());

            let trx = session.try_begin_trx().unwrap().unwrap();
            trx.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_same_session_reuse_after_readonly_commit() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = test_engine_config_for(root.path()).build().await.unwrap();
            let mut session = engine.try_new_session().unwrap();

            let trx = session.try_begin_trx().unwrap().unwrap();
            let cts = trx.commit().await.unwrap();
            assert_eq!(cts, 0);
            assert!(!session.in_trx());

            let trx = session.try_begin_trx().unwrap().unwrap();
            trx.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_distinct_sessions_can_hold_overlapping_transactions() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = test_engine_config_for(root.path()).build().await.unwrap();
            let mut session1 = engine.try_new_session().unwrap();
            let mut session2 = engine.try_new_session().unwrap();

            let trx1 = session1.try_begin_trx().unwrap().unwrap();
            let trx2 = session2.try_begin_trx().unwrap().unwrap();

            assert!(session1.in_trx());
            assert!(session2.in_trx());

            trx1.rollback().await.unwrap();
            trx2.rollback().await.unwrap();
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
            let leaked_ref = engine.new_ref().unwrap();

            let res = catch_unwind(AssertUnwindSafe(|| drop(engine)));
            assert!(res.is_err());

            drop(leaked_ref);
        });
    }

    #[test]
    fn test_shutdown_race_with_owner_ref_creation() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = test_engine_config_for(root.path()).build().await.unwrap();
            let barrier = StdArc::new(Barrier::new(3));
            let engine = &engine;

            std::thread::scope(|scope| {
                let shutdown_barrier = StdArc::clone(&barrier);
                let shutdown_handle = scope.spawn(move || {
                    shutdown_barrier.wait();
                    engine.shutdown()
                });

                let ref_barrier = StdArc::clone(&barrier);
                let ref_handle = scope.spawn(move || {
                    ref_barrier.wait();
                    engine.new_ref()
                });

                barrier.wait();

                let shutdown_res = shutdown_handle.join().unwrap();
                let new_ref_res = ref_handle.join().unwrap();

                match (shutdown_res, new_ref_res) {
                    (Ok(()), Err(Error::StorageEngineShutdown)) => {}
                    (Err(Error::StorageEngineShutdownBusy(1)), Ok(engine_ref)) => {
                        drop(engine_ref);
                        engine.shutdown().unwrap();
                    }
                    (Ok(()), Ok(engine_ref)) => {
                        drop(engine_ref);
                        panic!(
                            "shutdown succeeded but owner-side EngineRef creation also succeeded"
                        );
                    }
                    (Err(err), Ok(engine_ref)) => {
                        drop(engine_ref);
                        panic!("unexpected shutdown result during race: {err:?}");
                    }
                    (Ok(()), Err(err)) => {
                        panic!("unexpected new_ref error after successful shutdown: {err:?}");
                    }
                    (Err(shutdown_err), Err(new_ref_err)) => {
                        panic!(
                            "unexpected shutdown/new_ref race outcome: shutdown={shutdown_err:?}, new_ref={new_ref_err:?}"
                        );
                    }
                }
            });
        });
    }

    #[test]
    fn test_unstarted_transaction_system_shutdown_is_safe() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let log_dir = temp_dir.path().join("log");
            fs::create_dir_all(&log_dir).unwrap();
            let engine = test_engine_config_for(temp_dir.path())
                .file(
                    FileSystemConfig::default()
                        .data_dir("data")
                        .readonly_buffer_size(TEST_POOL_BYTES),
                )
                .trx(TrxSysConfig::default().skip_recovery(true))
                .build()
                .await
                .unwrap();

            let (trx_sys, startup) = TrxSysConfig::default()
                .log_dir(&log_dir)
                .log_file_stem("pending-startup-cleanup")
                .skip_recovery(true)
                .prepare(
                    engine.meta_pool.clone_inner(),
                    engine.index_pool.clone_inner(),
                    engine.mem_pool.clone_inner(),
                    engine.table_fs.clone(),
                    engine.disk_pool.clone_inner(),
                    engine.catalog.clone(),
                )
                .await
                .unwrap();
            drop(startup);
            drop(trx_sys);
        });
    }
}
