//! Observability logging facade.
//!
//! This module is for application-facing runtime observability logs. Keep redo
//! and commit-log implementation references under `crate::log`.

pub(crate) use log::{debug, error, info, warn};
