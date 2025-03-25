//! Storage engine for DoraDB.
//!
//! This module provides the main entry point of the storage engine,
//! including start, stop, recover, and execute commands.

use crate::buffer::EvictableBufferPoolConfig;
use crate::error::Result;
use crate::trx::sys::TrxSysConfig;
use serde::{Deserialize, Serialize};

pub struct Engine {}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct EngineConfig {
    pub trx: TrxSysConfig,
    pub buffer: EvictableBufferPoolConfig,
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
