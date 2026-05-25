//! Compression algorithms.
//!
//! This module includes compression algorithms used in storage.
//! Currently lightweight columnar compression is supposed to be
//! enough, e.g. FOR+bitpacking, dict, FSST.

pub(crate) mod bitpacking;

pub(crate) use bitpacking::*;
