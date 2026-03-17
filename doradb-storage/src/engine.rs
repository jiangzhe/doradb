//! Storage engine for DoraDB.
//!
//! This module provides the main entry point of the storage engine,
//! including start, stop, recover, and execute commands.
use crate::buffer::{
    EvictableBufferPool, EvictableBufferPoolConfig, FixedBufferPool, GlobalReadonlyBufferPool,
    PoolRole,
};
use crate::catalog::Catalog;
use crate::error::Result;
use crate::file::table_fs::{TableFileSystem, TableFileSystemConfig};
use crate::lifetime::StaticLifetime;
use crate::quiescent::{QuiDAG, QuiDep, QuiHandle};
use crate::session::Session;
use crate::storage_path::ResolvedStoragePaths;
use crate::trx::sys::TransactionSystem;
use crate::trx::sys_conf::TrxSysConfig;
use byte_unit::Byte;
use serde::{Deserialize, Serialize};
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::Arc;

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
    pub fn new_session(&self) -> Session {
        Session::new(self.new_ref())
    }

    #[inline]
    pub fn catalog(&self) -> &Catalog {
        &self.trx_sys.catalog
    }

    #[inline]
    pub fn new_ref(&self) -> EngineRef {
        EngineRef(Arc::clone(&self.0))
    }
}

impl Drop for Engine {
    #[inline]
    fn drop(&mut self) {
        // Engine is supposed to be last one to drop.
        if Arc::strong_count(&self.0) != 1 {
            panic!("fatal: engine ref is leaked");
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
    pub fn new_session(&self) -> Session {
        Session::new(self.clone())
    }

    #[inline]
    pub fn catalog(&self) -> &Catalog {
        &self.trx_sys.catalog
    }
}

struct StaticOwner<T: StaticLifetime + 'static> {
    inner: &'static T,
}

impl<T: StaticLifetime + 'static> StaticOwner<T> {
    #[inline]
    fn new(inner: &'static T) -> Self {
        Self { inner }
    }

    #[inline]
    pub(crate) fn as_static(&self) -> &'static T {
        self.inner
    }
}

impl<T: StaticLifetime + 'static> Drop for StaticOwner<T> {
    #[inline]
    fn drop(&mut self) {
        // SAFETY: `StaticOwner<T>` owns exactly one leaked `&'static T` created
        // during engine assembly and is dropped exactly once via the engine DAG.
        unsafe {
            StaticLifetime::drop_static(self.inner);
        }
    }
}

pub(crate) struct StaticHandle<T: StaticLifetime + 'static> {
    inner: &'static T,
    owner: Option<QuiDep<StaticOwner<T>>>,
}

impl<T: StaticLifetime + 'static> StaticHandle<T> {
    #[inline]
    pub(crate) fn as_static(&self) -> &'static T {
        self.inner
    }
}

impl<T: StaticLifetime + 'static> Clone for StaticHandle<T> {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            inner: self.inner,
            owner: self.owner.clone(),
        }
    }
}

impl<T: StaticLifetime + 'static> From<&'static T> for StaticHandle<T> {
    #[inline]
    fn from(inner: &'static T) -> Self {
        Self { inner, owner: None }
    }
}

impl<T: StaticLifetime + 'static> From<QuiDep<StaticOwner<T>>> for StaticHandle<T> {
    #[inline]
    fn from(owner: QuiDep<StaticOwner<T>>) -> Self {
        let inner = owner.as_static();
        Self {
            inner,
            owner: Some(owner),
        }
    }
}

impl<T: StaticLifetime + 'static> Deref for StaticHandle<T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.inner
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

pub struct EngineInner {
    pub trx_sys: &'static TransactionSystem,
    // meta pool is used for block index and catalog tables.
    pub meta_pool: &'static FixedBufferPool,
    // index pool is used for secondary index.
    // This pool will be optimized to support CoW B+tree index.
    pub index_pool: &'static FixedBufferPool,
    // data pool is used for data tables.
    pub mem_pool: &'static EvictableBufferPool,
    // Table file system to handle async IO of files on disk.
    pub table_fs: &'static TableFileSystem,
    // Global readonly buffer pool for table-file page reads.
    pub disk_pool: &'static GlobalReadonlyBufferPool,
    _owners: EngineOwners,
}

unsafe impl Send for Engine {}
unsafe impl Sync for Engine {}

#[inline]
fn insert_static_owner<T>(
    dag: &mut QuiDAG,
    name: impl Into<String>,
    inner: &'static T,
) -> Result<QuiHandle<StaticOwner<T>>>
where
    T: StaticLifetime + 'static,
{
    Ok(dag.insert(name, StaticOwner::new(inner))?)
}

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

        let disk_pool =
            GlobalReadonlyBufferPool::with_capacity_static(PoolRole::Disk, readonly_buffer_size)?;
        let disk_pool_h = insert_static_owner(&mut dag, "disk_pool", disk_pool)?;

        let table_fs = StaticLifetime::new_static(file.build()?);
        let table_fs_h = insert_static_owner(&mut dag, "table_fs", table_fs)?;

        let meta_pool = FixedBufferPool::with_capacity_static(
            PoolRole::Meta,
            self.meta_buffer.as_u64() as usize,
        )?;
        let meta_pool_h = insert_static_owner(&mut dag, "meta_pool", meta_pool)?;

        // todo: implement index pool
        let index_pool = FixedBufferPool::with_capacity_static(
            PoolRole::Index,
            self.index_buffer.as_u64() as usize,
        )?;
        let index_pool_h = insert_static_owner(&mut dag, "index_pool", index_pool)?;

        let mem_pool = StaticLifetime::new_static(
            self.data_buffer
                .role(PoolRole::Mem)
                .data_swap_file(resolved.data_swap_file_path())
                .build()?,
        );
        let mem_pool_h = insert_static_owner(&mut dag, "mem_pool", mem_pool)?;

        let trx_cfg = self.trx.log_dir(resolved.log_dir_path());
        let mem_pool_h_for_trx = mem_pool_h.clone();
        let disk_pool_h_for_trx = disk_pool_h.clone();
        let trx_sys_h = dag
            .node("trx_sys")?
            .depends_on(&meta_pool_h)?
            .depends_on(&index_pool_h)?
            .depends_on(&mem_pool_h)?
            .depends_on(&table_fs_h)?
            .depends_on(&disk_pool_h)?
            .build_async(move |deps| async move {
                let pending_trx_sys = trx_cfg
                    .prepare_static(
                        meta_pool,
                        index_pool,
                        StaticHandle::from(deps.dep(&mem_pool_h_for_trx)),
                        table_fs,
                        StaticHandle::from(deps.dep(&disk_pool_h_for_trx)),
                    )
                    .await?;
                let trx_sys = pending_trx_sys.start().await;
                Ok::<StaticOwner<TransactionSystem>, crate::error::Error>(StaticOwner::new(trx_sys))
            })
            .await?;
        dag.drop_before(table_fs_h.id(), disk_pool_h.id())?;
        dag.seal()?;

        resolved.persist_marker_if_missing()?;
        let started_trx_sys = {
            let trx_sys_guard = trx_sys_h.guard();
            trx_sys_guard.as_static()
        };
        drop((
            trx_sys_h,
            mem_pool_h,
            index_pool_h,
            meta_pool_h,
            table_fs_h,
            disk_pool_h,
        ));
        Ok(Engine(Arc::new(EngineInner {
            trx_sys: started_trx_sys,
            meta_pool,
            index_pool,
            mem_pool,
            table_fs,
            disk_pool,
            _owners: EngineOwners::new(dag),
        })))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;
    use crate::lifetime::StaticLifetimeScope;
    use crate::storage_path::STORAGE_LAYOUT_FILE_NAME;
    use std::fs;
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

    struct DropOrderStatic {
        name: &'static str,
        drops: Arc<Mutex<Vec<&'static str>>>,
    }

    impl Drop for DropOrderStatic {
        fn drop(&mut self) {
            self.drops.lock().unwrap().push(self.name);
        }
    }

    unsafe impl StaticLifetime for DropOrderStatic {}

    #[test]
    fn test_engine_owner_dag_enforces_runtime_and_order_only_edges() {
        let drops = Arc::new(Mutex::new(Vec::new()));
        let mut dag = QuiDAG::new();

        let disk_pool = StaticLifetime::new_static(DropOrderStatic {
            name: "disk_pool",
            drops: Arc::clone(&drops),
        });
        let disk_pool_h = insert_static_owner(&mut dag, "disk_pool", disk_pool).unwrap();

        let table_fs = StaticLifetime::new_static(DropOrderStatic {
            name: "table_fs",
            drops: Arc::clone(&drops),
        });
        let table_fs_h = insert_static_owner(&mut dag, "table_fs", table_fs).unwrap();

        let meta_pool = StaticLifetime::new_static(DropOrderStatic {
            name: "meta_pool",
            drops: Arc::clone(&drops),
        });
        let meta_pool_h = insert_static_owner(&mut dag, "meta_pool", meta_pool).unwrap();

        let index_pool = StaticLifetime::new_static(DropOrderStatic {
            name: "index_pool",
            drops: Arc::clone(&drops),
        });
        let index_pool_h = insert_static_owner(&mut dag, "index_pool", index_pool).unwrap();

        let mem_pool = StaticLifetime::new_static(DropOrderStatic {
            name: "mem_pool",
            drops: Arc::clone(&drops),
        });
        let mem_pool_h = insert_static_owner(&mut dag, "mem_pool", mem_pool).unwrap();

        let trx_sys = StaticLifetime::new_static(DropOrderStatic {
            name: "trx_sys",
            drops: Arc::clone(&drops),
        });
        let trx_sys_h = dag
            .insert_with_deps(
                "trx_sys",
                StaticOwner::new(trx_sys),
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

        let disk_pool = StaticLifetime::new_static(DropOrderStatic {
            name: "disk_pool",
            drops: Arc::clone(&drops),
        });
        let disk_pool_h = insert_static_owner(&mut dag, "disk_pool", disk_pool).unwrap();

        let table_fs = StaticLifetime::new_static(DropOrderStatic {
            name: "table_fs",
            drops: Arc::clone(&drops),
        });
        let table_fs_h = insert_static_owner(&mut dag, "table_fs", table_fs).unwrap();

        let meta_pool = StaticLifetime::new_static(DropOrderStatic {
            name: "meta_pool",
            drops: Arc::clone(&drops),
        });
        let meta_pool_h = insert_static_owner(&mut dag, "meta_pool", meta_pool).unwrap();

        let index_pool = StaticLifetime::new_static(DropOrderStatic {
            name: "index_pool",
            drops: Arc::clone(&drops),
        });
        let index_pool_h = insert_static_owner(&mut dag, "index_pool", index_pool).unwrap();

        let mem_pool = StaticLifetime::new_static(DropOrderStatic {
            name: "mem_pool",
            drops: Arc::clone(&drops),
        });
        let mem_pool_h = insert_static_owner(&mut dag, "mem_pool", mem_pool).unwrap();

        let trx_sys = StaticLifetime::new_static(DropOrderStatic {
            name: "trx_sys",
            drops: Arc::clone(&drops),
        });
        let trx_sys_h = insert_static_owner(&mut dag, "trx_sys", trx_sys).unwrap();

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
    fn test_unstarted_transaction_system_drop_is_safe() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let data_dir = temp_dir.path().join("data");
            let log_dir = temp_dir.path().join("log");
            let swap_file = temp_dir.path().join("data.bin");
            fs::create_dir_all(&data_dir).unwrap();
            fs::create_dir_all(&log_dir).unwrap();
            let scope = StaticLifetimeScope::new();
            let table_fs = scope.adopt(StaticLifetime::new_static(
                TableFileSystemConfig::default()
                    .data_dir(&data_dir)
                    .build()
                    .unwrap(),
            ));
            let meta_pool = scope.adopt(
                FixedBufferPool::with_capacity_static(PoolRole::Meta, TEST_POOL_BYTES).unwrap(),
            );
            let index_pool = scope.adopt(
                FixedBufferPool::with_capacity_static(PoolRole::Index, TEST_POOL_BYTES).unwrap(),
            );
            let mem_pool = scope.adopt(
                EvictableBufferPoolConfig::default()
                    .role(PoolRole::Mem)
                    .data_swap_file(&swap_file)
                    .max_mem_size(TEST_POOL_BYTES)
                    .max_file_size(128usize * 1024 * 1024)
                    .build_static()
                    .unwrap(),
            );
            let disk_pool = scope.adopt(
                GlobalReadonlyBufferPool::with_capacity_static(PoolRole::Disk, TEST_POOL_BYTES)
                    .unwrap(),
            );

            let pending = TrxSysConfig::default()
                .log_dir(&log_dir)
                .log_file_stem("pending-startup-cleanup")
                .skip_recovery(true)
                .prepare_static(
                    meta_pool.as_static(),
                    index_pool.as_static(),
                    mem_pool.as_static(),
                    table_fs.as_static(),
                    disk_pool.as_static(),
                )
                .await
                .unwrap();

            // SAFETY: this test exercises the failed-startup cleanup path where
            // the leaked transaction system is dropped before `start()` runs.
            unsafe {
                StaticLifetime::drop_static(pending.trx_sys.as_static());
            }
            drop(pending);
        });
    }
}
