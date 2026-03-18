//! Storage engine for DoraDB.
//!
//! This module provides the main entry point of the storage engine,
//! including start, stop, recover, and execute commands.
use crate::buffer::{
    EvictableBufferPool, EvictableBufferPoolConfig, FixedBufferPool, GlobalReadonlyBufferPool,
    PoolRole,
};
use crate::catalog::Catalog;
use crate::error::{Error, Result};
use crate::file::table_fs::{TableFileSystem, TableFileSystemConfig};
use crate::quiescent::{QuiDAG, QuiescentGuard};
use crate::session::Session;
use crate::storage_path::ResolvedStoragePaths;
use crate::trx::sys::TransactionSystem;
use crate::trx::sys_conf::TrxSysConfig;
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

/// Storage engine of DoraDB.
pub struct Engine(Arc<EngineInner>);

impl Deref for Engine {
    type Target = EngineInner;
    #[inline]
    fn deref(&self) -> &EngineInner {
        &self.0
    }
}

impl Engine {
    #[inline]
    pub fn try_new_session(&self) -> Result<Session> {
        self.0
            .with_running_admission(|| Session::new(self.new_ref()))
    }

    #[inline]
    pub fn catalog(&self) -> &Catalog {
        &self.trx_sys.catalog
    }

    #[inline]
    pub fn new_ref(&self) -> EngineRef {
        EngineRef(Arc::clone(&self.0))
    }

    #[inline]
    pub fn shutdown(&self) -> Result<()> {
        self.finalize_shutdown()
    }

    #[inline]
    fn finalize_shutdown(&self) -> Result<()> {
        let _finalize = self.0.lifecycle.finalize_lock.lock();
        if self.0.lifecycle.state() == EngineLifecycleState::Shutdown {
            return Ok(());
        }

        {
            let _gate = self.0.lifecycle.admission_gate.write();
            if self.0.lifecycle.state() == EngineLifecycleState::Running {
                self.0
                    .lifecycle
                    .set_state(EngineLifecycleState::ShuttingDown);
            }
        }

        // Any live session/transaction/statement keeps an `EngineRef` alive
        // through `SessionState`. Requiring the last strong reference here
        // gives transaction-system shutdown a clean point where user-originated
        // work has already drained before we start disabling runtime state.
        let strong_count = Arc::strong_count(&self.0);
        if strong_count != 1 {
            return Err(Error::StorageEngineShutdownBusy(strong_count - 1));
        }

        self.0.shutdown_components();
        self.0.lifecycle.set_state(EngineLifecycleState::Shutdown);
        Ok(())
    }
}

impl Drop for Engine {
    #[inline]
    fn drop(&mut self) {
        // Engine is supposed to be last one to drop.
        if Arc::strong_count(&self.0) != 1 {
            panic!("fatal: engine ref is leaked");
        }
        if let Err(err) = self.finalize_shutdown() {
            panic!("fatal: engine shutdown failed: {err}");
        }
    }
}

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
    #[inline]
    pub fn try_new_session(&self) -> Result<Session> {
        self.0.with_running_admission(|| Session::new(self.clone()))
    }

    #[inline]
    pub fn catalog(&self) -> &Catalog {
        &self.trx_sys.catalog
    }
}

struct EngineOwners {
    _dag: QuiDAG,
}

impl EngineOwners {
    #[inline]
    fn new(dag: QuiDAG) -> Self {
        Self { _dag: dag }
    }
}

// SAFETY: `EngineOwners` is private engine teardown state. The embedded DAG is
// not exposed through any public API and only drops once the final `Arc` to the
// enclosing engine has gone away, after the leaked-engine-ref check in
// `Engine::drop` has ensured no shared engine handles remain.
unsafe impl Send for EngineOwners {}
// SAFETY: see `Send` above. Concurrent readers only observe the engine's public
// component references; the private DAG is not accessed concurrently.
unsafe impl Sync for EngineOwners {}

struct EngineBuildCleanup {
    trx_sys: Option<QuiescentGuard<TransactionSystem>>,
    mem_pool: Option<QuiescentGuard<EvictableBufferPool>>,
    table_fs: Option<QuiescentGuard<TableFileSystem>>,
    disk_pool: Option<QuiescentGuard<GlobalReadonlyBufferPool>>,
}

impl EngineBuildCleanup {
    #[inline]
    fn new() -> Self {
        Self {
            trx_sys: None,
            mem_pool: None,
            table_fs: None,
            disk_pool: None,
        }
    }

    #[inline]
    fn disarm(&mut self) {
        self.trx_sys = None;
        self.mem_pool = None;
        self.table_fs = None;
        self.disk_pool = None;
    }
}

impl Drop for EngineBuildCleanup {
    #[inline]
    fn drop(&mut self) {
        // Failed startup can return after guarded worker threads have already
        // been spawned but before a `TransactionSystem` exists, or after a
        // started `TransactionSystem` but before the `Engine` takes ownership.
        // Shut workers down explicitly before the local `QuiDAG` drops owners,
        // otherwise `QuiescentBox::drop` will wait forever on those keepalive
        // guards.
        if let Some(trx_sys) = self.trx_sys.take() {
            trx_sys.shutdown();
        }
        if let Some(mem_pool) = self.mem_pool.take() {
            mem_pool.shutdown();
        }
        if let Some(table_fs) = self.table_fs.take() {
            table_fs.shutdown();
        }
        if let Some(disk_pool) = self.disk_pool.take() {
            disk_pool.shutdown();
        }
    }
}

pub struct EngineInner {
    pub trx_sys: QuiescentGuard<TransactionSystem>,
    // meta pool is used for block index and catalog tables.
    pub meta_pool: QuiescentGuard<FixedBufferPool>,
    // index pool is used for secondary index.
    // This pool will be optimized to support CoW B+tree index.
    pub index_pool: QuiescentGuard<FixedBufferPool>,
    // data pool is used for data tables.
    pub mem_pool: QuiescentGuard<EvictableBufferPool>,
    // Table file system to handle async IO of files on disk.
    pub table_fs: QuiescentGuard<TableFileSystem>,
    // Global readonly buffer pool for table-file page reads.
    pub disk_pool: QuiescentGuard<GlobalReadonlyBufferPool>,
    lifecycle: EngineLifecycle,
    _owners: EngineOwners,
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

    #[inline]
    fn shutdown_components(&self) {
        // `trx_sys` owns the worker threads and must quiesce before the
        // lower-level pools and table filesystem begin tearing down.
        self.trx_sys.shutdown();
        self.mem_pool.shutdown();
        self.table_fs.shutdown();
        self.disk_pool.shutdown();
    }
}

unsafe impl Send for Engine {}
unsafe impl Sync for Engine {}

const DEFAULT_META_BUFFER: usize = 32 * 1024 * 1024;
const DEFAULT_INDEX_BUFFER: usize = 1024 * 1024 * 1024;

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
    pub fn data_buffer(mut self, data_buffer: EvictableBufferPoolConfig) -> Self {
        self.data_buffer = data_buffer;
        self
    }

    #[inline]
    pub fn file(mut self, file: TableFileSystemConfig) -> Self {
        self.file = file;
        self
    }

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
        let mut dag = QuiDAG::new();
        let mut cleanup = EngineBuildCleanup::new();

        let disk_pool_h = dag.insert(
            "disk_pool",
            GlobalReadonlyBufferPool::with_capacity(PoolRole::Disk, readonly_buffer_size)?,
        )?;
        cleanup.disk_pool = Some(disk_pool_h.guard());
        GlobalReadonlyBufferPool::start_evictor_thread_guarded(disk_pool_h.guard());

        let table_fs_h = dag.insert("table_fs", file.build()?)?;
        cleanup.table_fs = Some(table_fs_h.guard());

        let meta_pool_h = dag.insert(
            "meta_pool",
            FixedBufferPool::with_capacity(PoolRole::Meta, self.meta_buffer.as_u64() as usize)?,
        )?;

        // todo: implement index pool
        let index_pool_h = dag.insert(
            "index_pool",
            FixedBufferPool::with_capacity(PoolRole::Index, self.index_buffer.as_u64() as usize)?,
        )?;

        let mem_pool_h = dag.insert(
            "mem_pool",
            self.data_buffer
                .role(PoolRole::Mem)
                .data_swap_file(resolved.data_swap_file_path())
                .build()?,
        )?;
        cleanup.mem_pool = Some(mem_pool_h.guard());
        EvictableBufferPool::start_background_workers_guarded(mem_pool_h.guard());

        // Startup may fail during catalog bootstrap or log recovery, before a
        // stable `Engine` exists and therefore before normal engine shutdown is
        // available. Keep the started component guards in `cleanup` armed until
        // the very end so any `?` below tears workers down before `dag` drop.
        let trx_cfg = self.trx.log_dir(resolved.log_dir_path());
        let pending_trx_sys = trx_cfg
            .prepare(
                meta_pool_h.guard(),
                index_pool_h.guard(),
                mem_pool_h.guard(),
                table_fs_h.guard(),
                disk_pool_h.guard(),
            )
            .await?;
        let (trx_sys, trx_startup) = pending_trx_sys.into_parts();
        let trx_sys_h = dag.insert_with_deps(
            "trx_sys",
            trx_sys,
            [
                meta_pool_h.id(),
                index_pool_h.id(),
                mem_pool_h.id(),
                table_fs_h.id(),
                disk_pool_h.id(),
            ],
        )?;
        let started_trx_sys = trx_startup.start(trx_sys_h.guard()).await;
        cleanup.trx_sys = Some(started_trx_sys.clone());
        dag.drop_before(table_fs_h.id(), disk_pool_h.id())?;
        dag.seal()?;

        resolved.persist_marker_if_missing()?;
        let engine_inner = EngineInner {
            trx_sys: started_trx_sys,
            meta_pool: meta_pool_h.guard(),
            index_pool: index_pool_h.guard(),
            mem_pool: mem_pool_h.guard(),
            table_fs: table_fs_h.guard(),
            disk_pool: disk_pool_h.guard(),
            lifecycle: EngineLifecycle::new(),
            _owners: EngineOwners::new(dag),
        };
        drop((
            trx_sys_h,
            mem_pool_h,
            index_pool_h,
            meta_pool_h,
            table_fs_h,
            disk_pool_h,
        ));
        cleanup.disarm();
        Ok(Engine(Arc::new(engine_inner)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::tests::table1;
    use crate::error::Error;
    use crate::storage_path::STORAGE_LAYOUT_FILE_NAME;
    use std::fs;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::sync::{Arc, Mutex};
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

            // This test deliberately violates the engine drop contract to
            // assert the panic. After that assertion, shut the running
            // components down explicitly so the surviving `EngineRef` can drop
            // without leaving worker guards alive forever.
            leaked_ref.shutdown_components();
            drop(leaked_ref);
        });
    }

    struct DropOrderStatic {
        name: &'static str,
        drops: Arc<Mutex<Vec<&'static str>>>,
    }

    impl Drop for DropOrderStatic {
        fn drop(&mut self) {
            self.drops.lock().unwrap().push(self.name);
        }
    }

    #[test]
    fn test_engine_owner_dag_enforces_runtime_and_order_only_edges() {
        let drops = Arc::new(Mutex::new(Vec::new()));
        let mut dag = QuiDAG::new();

        let disk_pool_h = dag
            .insert(
                "disk_pool",
                DropOrderStatic {
                    name: "disk_pool",
                    drops: Arc::clone(&drops),
                },
            )
            .unwrap();

        let table_fs_h = dag
            .insert(
                "table_fs",
                DropOrderStatic {
                    name: "table_fs",
                    drops: Arc::clone(&drops),
                },
            )
            .unwrap();

        let meta_pool_h = dag
            .insert(
                "meta_pool",
                DropOrderStatic {
                    name: "meta_pool",
                    drops: Arc::clone(&drops),
                },
            )
            .unwrap();

        let index_pool_h = dag
            .insert(
                "index_pool",
                DropOrderStatic {
                    name: "index_pool",
                    drops: Arc::clone(&drops),
                },
            )
            .unwrap();

        let mem_pool_h = dag
            .insert(
                "mem_pool",
                DropOrderStatic {
                    name: "mem_pool",
                    drops: Arc::clone(&drops),
                },
            )
            .unwrap();

        let trx_sys_h = dag
            .insert_with_deps(
                "trx_sys",
                DropOrderStatic {
                    name: "trx_sys",
                    drops: Arc::clone(&drops),
                },
                [
                    meta_pool_h.id(),
                    index_pool_h.id(),
                    mem_pool_h.id(),
                    table_fs_h.id(),
                    disk_pool_h.id(),
                ],
            )
            .unwrap();
        dag.drop_before(table_fs_h.id(), disk_pool_h.id()).unwrap();
        dag.seal().unwrap();
        drop((
            trx_sys_h,
            mem_pool_h,
            index_pool_h,
            meta_pool_h,
            table_fs_h,
            disk_pool_h,
        ));
        drop(dag);

        let drops = drops.lock().unwrap();
        let pos = |name| drops.iter().position(|d| *d == name).unwrap();
        assert!(pos("trx_sys") < pos("meta_pool"));
        assert!(pos("trx_sys") < pos("index_pool"));
        assert!(pos("trx_sys") < pos("mem_pool"));
        assert!(pos("trx_sys") < pos("table_fs"));
        assert!(pos("trx_sys") < pos("disk_pool"));
        assert!(pos("table_fs") < pos("disk_pool"));
    }

    #[test]
    fn test_engine_owner_dag_unsealed_cleanup_uses_reverse_insertion_order() {
        let drops = Arc::new(Mutex::new(Vec::new()));
        let mut dag = QuiDAG::new();

        let disk_pool_h = dag
            .insert(
                "disk_pool",
                DropOrderStatic {
                    name: "disk_pool",
                    drops: Arc::clone(&drops),
                },
            )
            .unwrap();

        let table_fs_h = dag
            .insert(
                "table_fs",
                DropOrderStatic {
                    name: "table_fs",
                    drops: Arc::clone(&drops),
                },
            )
            .unwrap();

        let meta_pool_h = dag
            .insert(
                "meta_pool",
                DropOrderStatic {
                    name: "meta_pool",
                    drops: Arc::clone(&drops),
                },
            )
            .unwrap();

        let index_pool_h = dag
            .insert(
                "index_pool",
                DropOrderStatic {
                    name: "index_pool",
                    drops: Arc::clone(&drops),
                },
            )
            .unwrap();

        let mem_pool_h = dag
            .insert(
                "mem_pool",
                DropOrderStatic {
                    name: "mem_pool",
                    drops: Arc::clone(&drops),
                },
            )
            .unwrap();

        let trx_sys_h = dag
            .insert(
                "trx_sys",
                DropOrderStatic {
                    name: "trx_sys",
                    drops: Arc::clone(&drops),
                },
            )
            .unwrap();

        drop((
            trx_sys_h,
            mem_pool_h,
            index_pool_h,
            meta_pool_h,
            table_fs_h,
            disk_pool_h,
        ));
        drop(dag);

        assert_eq!(
            drops.lock().unwrap().as_slice(),
            &[
                "trx_sys",
                "mem_pool",
                "index_pool",
                "meta_pool",
                "table_fs",
                "disk_pool"
            ]
        );
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
            let table_fs = crate::quiescent::QuiescentBox::new(
                TableFileSystemConfig::default()
                    .data_dir(&data_dir)
                    .build()
                    .unwrap(),
            );
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
                    .unwrap(),
            );
            let disk_pool = crate::quiescent::QuiescentBox::new(
                GlobalReadonlyBufferPool::with_capacity(PoolRole::Disk, TEST_POOL_BYTES).unwrap(),
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
                )
                .await
                .unwrap();

            let (trx_sys, _startup) = pending.into_parts();
            trx_sys.shutdown();
            trx_sys.shutdown();
        });
    }
}
