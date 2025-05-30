//! Storage engine for DoraDB.
//!
//! This module provides the main entry point of the storage engine,
//! including start, stop, recover, and execute commands.
use crate::buffer::{BufferPool, EvictableBufferPool, EvictableBufferPoolConfig, FixedBufferPool};
use crate::catalog::Catalog;
use crate::error::Result;
use crate::lifetime::StaticLifetime;
use crate::session::Session;
use crate::trx::sys::TransactionSystem;
use crate::trx::sys_conf::TrxSysConfig;
use serde::{Deserialize, Serialize};
use std::ops::Deref;
use std::panic::{RefUnwindSafe, UnwindSafe};

pub type DefaultEngine = Engine<EvictableBufferPool>;

/// Storage engine of DoraDB.
pub struct Engine<P: BufferPool>(EngineInner<P>);

impl<P: BufferPool> Deref for Engine<P> {
    type Target = EngineInner<P>;
    #[inline]
    fn deref(&self) -> &EngineInner<P> {
        &self.0
    }
}

impl<P: BufferPool> Engine<P> {
    #[inline]
    pub fn new_session(&self) -> Session<P> {
        Session::new(self.weak())
    }

    #[inline]
    pub fn catalog(&self) -> &Catalog<P> {
        &self.trx_sys.catalog
    }

    #[inline]
    pub fn weak(&self) -> Self {
        Engine(EngineInner {
            trx_sys: self.0.trx_sys,
            buf_pool: self.0.buf_pool,
            stop_all: false,
        })
    }
}

pub struct EngineInner<P: BufferPool> {
    pub trx_sys: &'static TransactionSystem<P>,
    pub buf_pool: &'static P,
    // only one instance should have this flag to be true to
    // stop the engine.
    stop_all: bool,
}

impl<P: BufferPool> Drop for EngineInner<P> {
    #[inline]
    fn drop(&mut self) {
        if self.stop_all {
            unsafe {
                StaticLifetime::drop_static(self.trx_sys);
                StaticLifetime::drop_static(self.buf_pool);
            }
        }
    }
}

unsafe impl<P: BufferPool> Send for Engine<P> {}
unsafe impl<P: BufferPool> Sync for Engine<P> {}
impl<P: BufferPool> UnwindSafe for Engine<P> {}
impl<P: BufferPool> RefUnwindSafe for Engine<P> {}

impl Engine<FixedBufferPool> {
    /// Create a new engine initializer with fixed buffer pool.
    #[inline]
    pub fn new_fixed_initializer(
        mem_size: usize,
        trx_sys_config: TrxSysConfig,
    ) -> Result<EngineInitializer<FixedBufferPool>> {
        let buf_pool = FixedBufferPool::with_capacity(mem_size)?;
        let buf_pool = StaticLifetime::new_static(buf_pool);
        Ok(EngineInitializer {
            buf_pool,
            trx_sys_config,
        })
    }
}

pub struct EngineInitializer<P: BufferPool> {
    buf_pool: &'static P,
    trx_sys_config: TrxSysConfig,
}

impl<P: BufferPool> EngineInitializer<P> {
    #[inline]
    pub async fn init(self) -> Result<Engine<P>> {
        let trx_sys = self.trx_sys_config.build().init(self.buf_pool).await?;
        let engine = Engine(EngineInner {
            buf_pool: self.buf_pool,
            trx_sys,
            stop_all: true,
        });
        Ok(engine)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct EngineConfig {
    trx: TrxSysConfig,
    buffer: EvictableBufferPoolConfig,
}

impl EngineConfig {
    #[inline]
    pub fn trx(mut self, trx: TrxSysConfig) -> Self {
        self.trx = trx;
        self
    }

    #[inline]
    pub fn buffer(mut self, buffer: EvictableBufferPoolConfig) -> Self {
        self.buffer = buffer;
        self
    }

    #[inline]
    pub fn build(self) -> Result<EngineInitializer<EvictableBufferPool>> {
        let (buf_pool, pool_start_ctx) = self.buffer.build()?;
        let buf_pool = StaticLifetime::new_static(buf_pool);
        buf_pool.start(pool_start_ctx);
        Ok(EngineInitializer {
            buf_pool,
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
