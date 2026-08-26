use super::{RowPageDescriptor, TableScanWorklist};
use crate::conf::TableScanConfig;
use crate::id::{BlockID, RowID};
use crate::index::ColumnLeafEntry;
use std::num::NonZeroUsize;

/// One immutable storage-level unit in captured table-scan order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TableScanUnit {
    /// One persisted LWC block-index entry.
    Cold(ColumnLeafEntry),
    /// One original hot row page and its captured RowID reservation.
    Hot(RowPageDescriptor),
}

impl TableScanUnit {
    #[inline]
    const fn kind(&self) -> TableScanUnitKind {
        match self {
            Self::Cold(_) => TableScanUnitKind::Cold,
            Self::Hot(_) => TableScanUnitKind::Hot,
        }
    }
}

#[derive(Clone, Copy)]
enum TableScanUnitKind {
    Cold,
    Hot,
}

/// Resource-free output from one deterministic physical plan compilation.
#[derive(Debug)]
pub(crate) struct CompiledTableScanPlan {
    /// Captured persisted column block-index root.
    pub(crate) column_root: BlockID,
    /// Captured cold/hot RowID boundary.
    pub(crate) pivot_row_id: RowID,
    /// Ordered cold units followed by ordered hot units.
    pub(crate) units: Vec<TableScanUnit>,
    /// Checked cumulative normalized unit weights, beginning with zero.
    pub(crate) weight_prefix: Vec<u64>,
    /// Compact initial partition boundaries in unit-index space.
    pub(crate) partition_offsets: Vec<usize>,
}

/// Compile a captured physical worklist into deterministic units.
pub(crate) fn compile_table_scan_plan(
    worklist: TableScanWorklist,
    config: TableScanConfig,
) -> CompiledTableScanPlan {
    let TableScanWorklist {
        column_root,
        pivot_row_id,
        cold_entries,
        hot_pages,
    } = worklist;

    let mut units = Vec::with_capacity(cold_entries.len() + hot_pages.len());
    units.extend(cold_entries.into_iter().map(TableScanUnit::Cold));
    units.extend(hot_pages.into_iter().map(TableScanUnit::Hot));
    let (weight_prefix, budget) =
        normalized_weight_prefix(units.iter().map(TableScanUnit::kind), config);
    let partition_offsets = greedy_partition_offsets(&weight_prefix, budget);
    CompiledTableScanPlan {
        column_root,
        pivot_row_id,
        units,
        weight_prefix,
        partition_offsets,
    }
}

/// Derive deterministic best-effort offsets from an existing checked prefix.
pub(crate) fn repartition_table_scan_offsets(
    weight_prefix: &[u64],
    target_partitions: NonZeroUsize,
) -> Vec<usize> {
    assert!(
        !weight_prefix.is_empty(),
        "table scan invariant violated: repartition requires a nonempty weight prefix"
    );
    // Rust targets use at most 64-bit `usize`, so this conversion is lossless.
    let target_partitions = target_partitions.get() as u64;
    let total_weight = weight_prefix[weight_prefix.len() - 1];
    let budget = total_weight.div_ceil(target_partitions);
    greedy_partition_offsets(weight_prefix, budget)
}

fn normalized_weight_prefix(
    unit_kinds: impl ExactSizeIterator<Item = TableScanUnitKind>,
    config: TableScanConfig,
) -> (Vec<u64>, u64) {
    // Engine startup validation constrains both values to 1..=8192, so their
    // conversion and product are infallible on every supported Rust target.
    let cold_weight = config.row_pages_per_partition as u64;
    let hot_weight = config.lwc_blocks_per_partition as u64;
    let budget = cold_weight * hot_weight;
    let unit_count = unit_kinds.len();
    let max_weight = cold_weight.max(hot_weight);
    assert!(
        unit_count as u64 <= u64::MAX / max_weight,
        "table scan invariant violated: normalized weight prefix exceeds u64, unit_count={unit_count}, max_weight={max_weight}"
    );

    let mut weight_prefix = Vec::with_capacity(unit_count + 1);
    weight_prefix.push(0u64);
    let mut total_weight = 0u64;
    for kind in unit_kinds {
        let weight = match kind {
            TableScanUnitKind::Cold => cold_weight,
            TableScanUnitKind::Hot => hot_weight,
        };
        total_weight += weight;
        weight_prefix.push(total_weight);
    }
    (weight_prefix, budget)
}

/// Greedily pack consecutive physical units under one normalized weight budget.
///
/// A cut precedes the first unit that would exceed the budget. An indivisible
/// unit that exceeds the budget remains a nonempty singleton partition.
fn greedy_partition_offsets(weight_prefix: &[u64], budget: u64) -> Vec<usize> {
    let unit_count = weight_prefix.len() - 1;
    if unit_count == 0 {
        return vec![0, 0];
    }
    let mut offsets = vec![0];
    let mut partition_start = 0usize;
    for unit_idx in 0..unit_count {
        let partition_weight = weight_prefix[unit_idx + 1] - weight_prefix[partition_start];
        if partition_weight > budget && unit_idx > partition_start {
            offsets.push(unit_idx);
            partition_start = unit_idx;
        }
    }
    offsets.push(unit_count);
    offsets
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::PageID;
    use std::iter::repeat_n;

    fn hot_worklist(ranges: &[(u64, u64)]) -> TableScanWorklist {
        TableScanWorklist {
            column_root: BlockID::new(7),
            pivot_row_id: RowID::new(100),
            cold_entries: Vec::new(),
            hot_pages: ranges
                .iter()
                .enumerate()
                .map(|(idx, &(start, end))| RowPageDescriptor {
                    page_id: PageID::new(idx as u64 + 1),
                    start_row_id: RowID::new(start),
                    end_row_id: RowID::new(end),
                })
                .collect(),
        }
    }

    fn compile_unit_kinds(
        cold_count: usize,
        hot_count: usize,
        config: TableScanConfig,
    ) -> (Vec<u64>, Vec<usize>) {
        let kinds = repeat_n(TableScanUnitKind::Cold, cold_count)
            .chain(repeat_n(TableScanUnitKind::Hot, hot_count))
            .collect::<Vec<_>>();
        let (weight_prefix, budget) = normalized_weight_prefix(kinds.into_iter(), config);
        let offsets = greedy_partition_offsets(&weight_prefix, budget);
        (weight_prefix, offsets)
    }

    #[test]
    fn empty_and_hot_initial_offsets_follow_shared_budget() {
        let empty = compile_table_scan_plan(hot_worklist(&[]), TableScanConfig::default());
        assert_eq!(empty.partition_offsets, [0, 0]);
        assert_eq!(empty.weight_prefix, [0]);

        for (count, expected) in [(1, vec![0, 1]), (32, vec![0, 32]), (64, vec![0, 32, 64])] {
            let ranges = (0..count)
                .map(|idx| (100 + idx as u64 * 2, 101 + idx as u64 * 2))
                .collect::<Vec<_>>();
            let compiled =
                compile_table_scan_plan(hot_worklist(&ranges), TableScanConfig::default());
            assert_eq!(compiled.partition_offsets, expected, "count={count}");
            assert_eq!(compiled.weight_prefix.last(), Some(&(count as u64 * 16)));
        }
    }

    #[test]
    fn cold_and_mixed_initial_offsets_use_one_normalized_budget() {
        for (count, expected) in [
            (1, vec![0, 1]),
            (16, vec![0, 16]),
            (17, vec![0, 16, 17]),
            (32, vec![0, 16, 32]),
        ] {
            let (weight_prefix, offsets) = compile_unit_kinds(count, 0, TableScanConfig::default());
            assert_eq!(offsets, expected, "count={count}");
            assert_eq!(weight_prefix.last(), Some(&(count as u64 * 32)));
        }

        let (_, partial_offsets) = compile_unit_kinds(8, 16, TableScanConfig::default());
        assert_eq!(partial_offsets, [0, 24]);
        let (_, exact_offsets) = compile_unit_kinds(16, 32, TableScanConfig::default());
        assert_eq!(exact_offsets, [0, 16, 48]);
    }

    #[test]
    fn repartition_reuses_greedy_budget_packing() {
        assert_eq!(
            repartition_table_scan_offsets(&[0, 6, 12, 18], NonZeroUsize::new(2).unwrap()),
            [0, 1, 2, 3]
        );
        assert_eq!(
            repartition_table_scan_offsets(&[0, 1, 2, 3, 4], NonZeroUsize::new(3).unwrap()),
            [0, 2, 4]
        );
        assert_eq!(
            repartition_table_scan_offsets(&[0, 10, 11, 12], NonZeroUsize::new(3).unwrap()),
            [0, 1, 3]
        );
        assert_eq!(
            repartition_table_scan_offsets(&[0, 6, 12, 18], NonZeroUsize::new(1).unwrap()),
            [0, 3]
        );
        assert_eq!(
            repartition_table_scan_offsets(&[0, 10, 20], NonZeroUsize::new(8).unwrap()),
            [0, 1, 2]
        );
        assert_eq!(
            repartition_table_scan_offsets(&[0], NonZeroUsize::new(8).unwrap()),
            [0, 0]
        );
    }

    #[test]
    fn custom_config_cross_normalizes_hot_weight() {
        let config = TableScanConfig::default()
            .lwc_blocks_per_partition(3)
            .row_pages_per_partition(5);
        let compiled = compile_table_scan_plan(
            hot_worklist(&[(100, 101), (102, 103), (104, 105), (106, 107)]),
            config,
        );
        assert_eq!(compiled.weight_prefix, [0, 3, 6, 9, 12]);
        assert_eq!(compiled.partition_offsets, [0, 4]);
    }
}
