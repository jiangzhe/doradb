//! Storage engine for DoraDB.
//!
//! This module provides the main entry point of the storage engine,
//! including start, stop, recover, and execute commands.
use crate::buffer::{
    EvictableBufferPool, EvictableBufferPoolConfig, FixedBufferPool, GlobalReadonlyBufferPool,
};
use crate::catalog::Catalog;
use crate::error::Result;
use crate::file::table_fs::{TableFileSystem, TableFileSystemConfig};
use crate::lifetime::StaticLifetime;
use crate::session::Session;
use crate::storage_path::ResolvedStoragePaths;
use crate::trx::sys::TransactionSystem;
use crate::trx::sys_conf::TrxSysConfig;
use byte_unit::Byte;
use serde::{Deserialize, Serialize};
use std::ops::Deref;
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
        unsafe {
            StaticLifetime::drop_static(self.trx_sys);
            StaticLifetime::drop_static(self.mem_pool);
            StaticLifetime::drop_static(self.meta_pool);
            StaticLifetime::drop_static(self.index_pool);
            StaticLifetime::drop_static(self.table_fs);
            StaticLifetime::drop_static(self.disk_pool);
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
}

unsafe impl Send for Engine {}
unsafe impl Sync for Engine {}

const DEFAULT_META_BUFFER: usize = 32 * 1024 * 1024;
const DEFAULT_INDEX_BUFFER: usize = 1024 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineConfig {
    storage_root: String,
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
            storage_root: String::from("."),
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
    pub fn storage_root(mut self, storage_root: impl Into<String>) -> Self {
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
        resolved.ensure_directories()?;
        resolved.validate_marker_if_present()?;

        let file = self.file.data_dir(resolved.data_dir_string());
        let readonly_buffer_size = file.readonly_buffer_size;
        let table_fs = file.build()?;
        let table_fs = StaticLifetime::new_static(table_fs);
        // todo: avoid resource leak when errors occur.
        let meta_pool = FixedBufferPool::with_capacity_static(self.meta_buffer.as_u64() as usize)?;
        // todo: implement index pool
        let index_pool =
            FixedBufferPool::with_capacity_static(self.index_buffer.as_u64() as usize)?;
        let mem_pool = self
            .data_buffer
            .data_swap_file(resolved.data_swap_file_string())
            .build()?;
        let mem_pool = StaticLifetime::new_static(mem_pool);
        let disk_pool = GlobalReadonlyBufferPool::with_capacity_static(readonly_buffer_size)?;
        let pending_trx_sys = self
            .trx
            .log_dir(resolved.log_dir_string())
            .prepare_static(meta_pool, index_pool, mem_pool, table_fs, disk_pool)
            .await?;
        resolved.persist_marker_if_missing()?;
        let trx_sys = pending_trx_sys.start().await;
        Ok(Engine(Arc::new(EngineInner {
            trx_sys,
            meta_pool,
            index_pool,
            mem_pool,
            table_fs,
            disk_pool,
        })))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;
    use crate::storage_path::STORAGE_LAYOUT_FILE_NAME;
    use std::fs;
    use tempfile::TempDir;

    const TEST_POOL_BYTES: usize = 64 * 1024 * 1024;

    fn test_engine_config_for(root: &std::path::Path) -> EngineConfig {
        EngineConfig::default()
            .storage_root(root.to_string_lossy().to_string())
            .meta_buffer(TEST_POOL_BYTES)
            .index_buffer(TEST_POOL_BYTES)
            .data_buffer(
                EvictableBufferPoolConfig::default()
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
                .storage_root(root.path().to_string_lossy().to_string())
                .meta_buffer(TEST_POOL_BYTES)
                .index_buffer(TEST_POOL_BYTES)
                .file(TableFileSystemConfig::default().readonly_buffer_size(TEST_POOL_BYTES))
                .data_buffer(
                    EvictableBufferPoolConfig::default()
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
}
