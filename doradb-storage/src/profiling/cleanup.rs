use crate::catalog::IndexID;

/// Aggregate result for a full-scan user-table secondary MemIndex cleanup pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MemIndexCleanupStats {
    /// One row per secondary index scanned by this pass.
    pub indexes: Vec<SecondaryMemIndexCleanupIndexStats>,
}

/// Cleanup result for one secondary MemIndex.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecondaryMemIndexCleanupIndexStats {
    /// Stable table-local secondary-index identity.
    pub index_id: IndexID,
    /// Whether the scanned index is unique.
    pub unique: bool,
    /// Number of MemIndex entries processed as cleanup candidates.
    pub scanned: usize,
    /// Number of MemIndex entries physically removed.
    pub removed: usize,
    /// Number of MemIndex entries intentionally retained.
    pub retained: usize,
    /// Number of live MemIndex entries skipped before key materialization.
    pub skipped_live: usize,
    /// Number of hot delete overlays skipped before key materialization.
    pub skipped_hot_deleted: usize,
}

impl SecondaryMemIndexCleanupIndexStats {
    /// Start measurements for one selected secondary index.
    #[inline]
    pub(crate) fn new(index_id: IndexID, unique: bool) -> Self {
        Self {
            index_id,
            unique,
            scanned: 0,
            removed: 0,
            retained: 0,
            skipped_live: 0,
            skipped_hot_deleted: 0,
        }
    }

    /// Count one candidate and its actual cleanup decision.
    #[inline]
    pub(crate) fn record(&mut self, remove: bool) {
        self.scanned += 1;
        if remove {
            self.removed += 1;
        } else {
            self.retained += 1;
        }
    }

    /// Count live entries skipped before key materialization.
    #[inline]
    pub(crate) fn record_skipped_live(&mut self, count: usize) {
        self.skipped_live += count;
    }

    /// Count hot delete overlays skipped before key materialization.
    #[inline]
    pub(crate) fn record_skipped_hot_deleted(&mut self, count: usize) {
        self.skipped_hot_deleted += count;
    }
}
