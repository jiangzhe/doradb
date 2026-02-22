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
            StaticLifetime::drop_static(self.data_pool);
            StaticLifetime::drop_static(self.meta_pool);
            StaticLifetime::drop_static(self.index_pool);
            StaticLifetime::drop_static(self.table_fs);
            StaticLifetime::drop_static(self.readonly_pool);
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
    pub data_pool: &'static EvictableBufferPool,
    // Table file system to handle async IO of files on disk.
    pub table_fs: &'static TableFileSystem,
    // Global readonly buffer pool for table-file page reads.
    pub readonly_pool: &'static GlobalReadonlyBufferPool,
}

unsafe impl Send for Engine {}
unsafe impl Sync for Engine {}

const DEFAULT_META_BUFFER: usize = 32 * 1024 * 1024;
const DEFAULT_INDEX_BUFFER: usize = 1024 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineConfig {
    main_dir: String,
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
            main_dir: String::from("."),
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
    pub fn main_dir(mut self, main_dir: impl Into<String>) -> Self {
        self.main_dir = main_dir.into();
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
        std::fs::create_dir_all(&self.main_dir)?;
        let file = self.file.with_main_dir(&self.main_dir);
        let readonly_buffer_size = file.readonly_buffer_size;
        std::fs::create_dir_all(&file.base_dir)?;
        let table_fs = file.build()?;
        let table_fs = StaticLifetime::new_static(table_fs);
        // todo: avoid resource leak when errors occur.
        let meta_pool = FixedBufferPool::with_capacity_static(self.meta_buffer.as_u64() as usize)?;
        // todo: implement index pool
        let index_pool =
            FixedBufferPool::with_capacity_static(self.index_buffer.as_u64() as usize)?;
        let data_pool = self.data_buffer.with_main_dir(&self.main_dir).build()?;
        let data_pool = StaticLifetime::new_static(data_pool);
        let readonly_pool = GlobalReadonlyBufferPool::with_capacity_static(readonly_buffer_size)?;
        let trx_sys = self
            .trx
            .with_main_dir(&self.main_dir)
            .build_static(meta_pool, index_pool, data_pool, table_fs, readonly_pool)
            .await?;
        Ok(Engine(Arc::new(EngineInner {
            trx_sys,
            meta_pool,
            index_pool,
            data_pool,
            table_fs,
            readonly_pool,
        })))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_engine_config() {
        let config = EngineConfig::default();
        println!("{:?}", config);
        let config_str = toml::to_string(&config).unwrap();
        println!("{}", config_str);
    }
}
