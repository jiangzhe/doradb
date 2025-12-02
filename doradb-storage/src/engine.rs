//! Storage engine for DoraDB.
//!
//! This module provides the main entry point of the storage engine,
//! including start, stop, recover, and execute commands.
use crate::buffer::{EvictableBufferPool, EvictableBufferPoolConfig, FixedBufferPool};
use crate::catalog::Catalog;
use crate::error::Result;
use crate::lifetime::StaticLifetime;
use crate::session::Session;
use crate::trx::sys::TransactionSystem;
use crate::trx::sys_conf::TrxSysConfig;
use byte_unit::Byte;
use serde::{Deserialize, Serialize};
use std::ops::Deref;

/// Storage engine of DoraDB.
pub struct Engine(EngineInner);

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
        Session::new(self.weak())
    }

    #[inline]
    pub fn catalog(&self) -> &Catalog {
        &self.trx_sys.catalog
    }

    #[inline]
    pub fn weak(&self) -> Self {
        Engine(EngineInner {
            trx_sys: self.0.trx_sys,
            meta_pool: self.0.meta_pool,
            index_pool: self.0.index_pool,
            data_pool: self.0.data_pool,
            stop_all: false,
        })
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
    // only one instance should have this flag to be true to
    // stop the engine.
    stop_all: bool,
}

impl Drop for EngineInner {
    #[inline]
    fn drop(&mut self) {
        if self.stop_all {
            unsafe {
                StaticLifetime::drop_static(self.trx_sys);
                StaticLifetime::drop_static(self.data_pool);
            }
        }
    }
}

unsafe impl Send for Engine {}
unsafe impl Sync for Engine {}

pub struct EngineInitializer {
    meta_pool: &'static FixedBufferPool,
    index_pool: &'static FixedBufferPool,
    data_pool: &'static EvictableBufferPool,
    trx_sys_config: TrxSysConfig,
}

impl EngineInitializer {
    #[inline]
    pub async fn init(self) -> Result<Engine> {
        let trx_sys = self
            .trx_sys_config
            .build()
            .init(self.meta_pool, self.index_pool, self.data_pool)
            .await?;
        let engine = Engine(EngineInner {
            meta_pool: self.meta_pool,
            index_pool: self.index_pool,
            data_pool: self.data_pool,
            trx_sys,
            stop_all: true,
        });
        Ok(engine)
    }
}

const DEFAULT_META_BUFFER: usize = 32 * 1024 * 1024;
const DEFAULT_INDEX_BUFFER: usize = 1024 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineConfig {
    trx: TrxSysConfig,
    meta_buffer: Byte,
    index_buffer: Byte,
    data_buffer: EvictableBufferPoolConfig,
}

impl Default for EngineConfig {
    #[inline]
    fn default() -> Self {
        EngineConfig {
            trx: TrxSysConfig::default(),
            meta_buffer: Byte::from_u64(DEFAULT_META_BUFFER as u64),
            index_buffer: Byte::from_u64(DEFAULT_INDEX_BUFFER as u64),
            data_buffer: EvictableBufferPoolConfig::default(),
        }
    }
}

impl EngineConfig {
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
    pub fn build(self) -> Result<EngineInitializer> {
        let meta_pool = FixedBufferPool::with_capacity_static(self.meta_buffer.as_u64() as usize)?;
        // todo: implement index pool
        let index_pool =
            FixedBufferPool::with_capacity_static(self.index_buffer.as_u64() as usize)?;
        let data_pool = self.data_buffer.build()?;
        let data_pool = StaticLifetime::new_static(data_pool);
        Ok(EngineInitializer {
            meta_pool,
            index_pool,
            data_pool,
            trx_sys_config: self.trx,
        })
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
