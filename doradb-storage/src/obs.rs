//! Observability logging facade.
//!
//! This module is for application-facing runtime observability logs. Keep redo
//! and commit-log implementation references under `crate::log`.

pub(crate) use log::{Level, debug, error, info, log_enabled, warn};
