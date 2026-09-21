use crate::id::{TableID, TrxID};
use crate::map::FastHashMap;

/// Durable replay bounds for one recovered user table.
#[derive(Clone, Copy, Debug)]
pub(crate) struct TableReplayBounds {
    /// Active table-root publication timestamp observed during recovery bootstrap.
    pub(crate) root_ts: TrxID,
    /// Lower bound for replaying heap row-page redo for this table.
    pub(crate) heap_redo_start_ts: TrxID,
    /// Lower bound for replaying persisted cold-delete metadata for this table.
    pub(crate) deletion_cutoff_ts: TrxID,
}

impl TableReplayBounds {
    /// Earliest table redo timestamp that may still affect recovered state.
    #[inline]
    pub(crate) fn replay_start_ts(self) -> TrxID {
        self.heap_redo_start_ts.min(self.deletion_cutoff_ts)
    }

    /// Highest timestamp carried by the table root replay bounds.
    #[inline]
    pub(crate) fn max_recovered_cts_seed(self) -> TrxID {
        self.root_ts
            .max(self.heap_redo_start_ts)
            .max(self.deletion_cutoff_ts)
    }
}

/// Recovery replay cursors and timestamp-generator watermark.
pub(crate) struct RecoveryTimeline {
    /// Catalog checkpoint boundary.
    pub(crate) catalog_replay_start_ts: TrxID,
    /// Earliest redo timestamp that may affect any recovered state.
    pub(crate) replay_floor: TrxID,
    /// Highest timestamp observed during recovery planning/replay.
    pub(crate) max_recovered_cts: TrxID,
    /// Per loaded user-table replay bounds.
    pub(crate) table_bounds: FastHashMap<TableID, TableReplayBounds>,
}

impl RecoveryTimeline {
    /// Create a timeline initialized to the minimum snapshot timestamp.
    #[inline]
    pub(crate) fn new(initial_ts: TrxID) -> Self {
        Self {
            catalog_replay_start_ts: initial_ts,
            replay_floor: initial_ts,
            max_recovered_cts: initial_ts,
            table_bounds: FastHashMap::default(),
        }
    }

    /// Apply catalog checkpoint replay cursor state.
    #[inline]
    pub(crate) fn seed_catalog_checkpoint(&mut self, catalog_replay_start_ts: TrxID) {
        self.catalog_replay_start_ts = catalog_replay_start_ts;
        self.replay_floor = catalog_replay_start_ts;
        self.max_recovered_cts = self.max_recovered_cts.max(catalog_replay_start_ts);
    }

    /// Apply one loaded table's replay bounds to floor and timestamp watermark.
    #[inline]
    pub(crate) fn seed_table_bounds(&mut self, bounds: TableReplayBounds) {
        self.replay_floor = self.replay_floor.min(bounds.replay_start_ts());
        self.seed_recovered_cts(bounds.max_recovered_cts_seed());
    }

    /// Apply a timestamp seen in skipped segments or decoded redo headers.
    #[inline]
    pub(crate) fn seed_recovered_cts(&mut self, cts: TrxID) {
        self.max_recovered_cts = self.max_recovered_cts.max(cts);
    }
}

#[cfg(test)]
mod tests {
    use super::{RecoveryTimeline, TableReplayBounds};
    use crate::id::TrxID;

    /// Purpose: Choose the replay start when heap and deletion boundaries differ.
    /// Expected: Replay begins at the earlier boundary regardless of which source supplies it.
    #[test]
    fn table_replay_bounds_start_at_min_heap_or_deletion_cutoff() {
        let heap_first = TableReplayBounds {
            root_ts: TrxID::new(10),
            heap_redo_start_ts: TrxID::new(3),
            deletion_cutoff_ts: TrxID::new(7),
        };
        let deletion_first = TableReplayBounds {
            root_ts: TrxID::new(10),
            heap_redo_start_ts: TrxID::new(8),
            deletion_cutoff_ts: TrxID::new(4),
        };

        assert_eq!(heap_first.replay_start_ts(), TrxID::new(3));
        assert_eq!(deletion_first.replay_start_ts(), TrxID::new(4));
    }

    /// Purpose: Combine catalog, table, and recovered transaction timestamps into the recovery timeline.
    /// Expected: Each timestamp source can advance the watermark without regression, while replay floors retain earlier bounds.
    #[test]
    fn recovery_timeline_seeds_watermark_from_all_sources() {
        let mut timeline = RecoveryTimeline::new(TrxID::new(1));
        assert_eq!(timeline.catalog_replay_start_ts, TrxID::new(1));
        assert_eq!(timeline.replay_floor, TrxID::new(1));
        assert_eq!(timeline.max_recovered_cts, TrxID::new(1));
        timeline.seed_catalog_checkpoint(TrxID::new(10));
        assert_eq!(timeline.catalog_replay_start_ts, TrxID::new(10));
        assert_eq!(timeline.replay_floor, TrxID::new(10));
        assert_eq!(timeline.max_recovered_cts, TrxID::new(10));
        for (source, root, heap, deletion, floor, watermark) in [
            ("root", 12, 4, 8, 4, 12),
            ("heap", 11, 14, 8, 4, 14),
            ("deletion", 11, 8, 16, 4, 16),
            ("older table", 11, 8, 3, 3, 16),
        ] {
            timeline.seed_table_bounds(TableReplayBounds {
                root_ts: TrxID::new(root),
                heap_redo_start_ts: TrxID::new(heap),
                deletion_cutoff_ts: TrxID::new(deletion),
            });
            assert_eq!(timeline.replay_floor, TrxID::new(floor), "{source}");
            assert_eq!(
                timeline.max_recovered_cts,
                TrxID::new(watermark),
                "{source}"
            );
        }
        timeline.seed_recovered_cts(TrxID::new(20));
        assert_eq!(timeline.max_recovered_cts, TrxID::new(20));
        timeline.seed_recovered_cts(TrxID::new(15));

        assert_eq!(timeline.catalog_replay_start_ts, TrxID::new(10));
        assert_eq!(timeline.replay_floor, TrxID::new(3));
        assert_eq!(timeline.max_recovered_cts, TrxID::new(20));
    }
}
