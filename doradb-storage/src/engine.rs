//! Storage engine for DoraDB.
//!
//! This module provides the main entry point of the storage engine,
//! including start, stop, recover, and execute commands.

use crate::buffer::{BufferPool, EvictableBufferPool, EvictableBufferPoolConfig, FixedBufferPool};
use crate::catalog::{Catalog, CatalogCache, CatalogStorage};
use crate::error::Result;
use crate::lifetime::StaticLifetime;
use crate::session::{Session, SessionWorker};
use crate::trx::sys::{TransactionSystem, TrxSysConfig};
use serde::{Deserialize, Serialize};
use std::mem::{ManuallyDrop, MaybeUninit};
use std::panic::{RefUnwindSafe, UnwindSafe};

/// Storage engine of DoraDB.
pub struct Engine<P: BufferPool> {
    pub trx_sys: TransactionSystem<P>,
    pub catalog: Catalog<P>,
    pub buf_pool: P,
}

impl<P: BufferPool> Engine<P> {
    #[inline]
    pub fn new_session(&'static self) -> Session<P> {
        Session::new(self)
    }

    #[inline]
    pub fn new_session_worker(&'static self) -> SessionWorker<P> {
        SessionWorker::new(self)
    }
}

unsafe impl<P: BufferPool> StaticLifetime for Engine<P> {
    #[inline]
    unsafe fn drop_static(this: &'static Self) {
        let mut engine = Box::from_raw(this as *const _ as *mut ManuallyDrop<Engine<P>>);
        // control the drop order manually.
        std::ptr::drop_in_place(&mut engine.trx_sys);
        std::ptr::drop_in_place(&mut engine.catalog);
        std::ptr::drop_in_place(&mut engine.buf_pool);
    }
}
unsafe impl<P: BufferPool> Send for Engine<P> {}
unsafe impl<P: BufferPool> Sync for Engine<P> {}
impl<P: BufferPool> UnwindSafe for Engine<P> {}
impl<P: BufferPool> RefUnwindSafe for Engine<P> {}

impl Engine<FixedBufferPool> {
    /// Create a new engine with fixed buffer pool.
    #[inline]
    pub async fn new_fixed(
        mem_size: usize,
        trx_sys_config: TrxSysConfig,
    ) -> Result<&'static Engine<FixedBufferPool>> {
        let buf_pool = FixedBufferPool::with_capacity(mem_size)?;
        let (trx_sys, trx_sys_start_ctx) = trx_sys_config.build();
        unsafe {
            let engine = Box::new(MaybeUninit::<Engine<FixedBufferPool>>::uninit());
            let engine = Box::leak(engine).assume_init_mut();

            std::ptr::write(&mut engine.buf_pool, buf_pool);
            std::ptr::write(&mut engine.trx_sys, trx_sys);

            let catalog_cache = CatalogCache::new();
            let catalog_storage = CatalogStorage::new(&engine.buf_pool).await;
            let catalog = Catalog::new(catalog_cache, catalog_storage);
            std::ptr::write(&mut engine.catalog, catalog);

            engine
                .trx_sys
                .start(&engine.buf_pool, &engine.catalog, trx_sys_start_ctx);

            Ok(engine)
        }
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
    pub async fn build_static(self) -> Result<&'static Engine<EvictableBufferPool>> {
        let (buf_pool, pool_start_ctx) = self.buffer.build()?;
        let (trx_sys, trx_sys_start_ctx) = self.trx.build();
        unsafe {
            let engine = Box::new(MaybeUninit::<Engine<EvictableBufferPool>>::uninit());
            let engine = Box::leak(engine).assume_init_mut();

            std::ptr::write(&mut engine.buf_pool, buf_pool);
            engine.buf_pool.start(pool_start_ctx);

            std::ptr::write(&mut engine.trx_sys, trx_sys);

            let catalog_cache = CatalogCache::new();
            let catalog_storage = CatalogStorage::new(&engine.buf_pool).await;
            let catalog = Catalog::new(catalog_cache, catalog_storage);
            std::ptr::write(&mut engine.catalog, catalog);

            engine
                .trx_sys
                .start(&engine.buf_pool, &engine.catalog, trx_sys_start_ctx);

            Ok(engine)
        }
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
