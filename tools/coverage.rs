#!/usr/bin/env -S cargo +nightly-2026-05-22 -q -Zscript
---
[package]
edition = "2024"

[dependencies]
proc-macro2 = { version = "=1.0.107", features = ["span-locations"] }
quote = "=1.0.47"
syn = { version = "=2.0.119", features = ["full", "visit"] }
serde = { version = "=1.0.228", features = ["derive"] }
serde_json = "=1.0.151"
blake3 = "=1.8.7"
toml = "=1.1.4"

[dev-dependencies]
tempfile = "=3.27.0"
---

#[path = "coverage/mod.rs"]
mod coverage;

use std::env;
use std::process::exit;

fn main() {
    if let Err(error) = coverage::main(env::args().skip(1)) {
        eprintln!("coverage: {error}");
        exit(1);
    }
}
