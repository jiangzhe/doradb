//! Partition-local validation against CREATE's already sorted, distinct cold keys.
use super::IndexBuildEntry;
use super::merge::{HotBatch, PreparedHotMerge, execution_error, observe_stop};
use crate::error::RuntimeOrFatalResult;
use crate::id::RowID;
use crate::index::BTreeKey;
#[cfg(feature = "profiling")]
use crate::profiling::{ColdHotMeasurements, clock::Instant};
use std::cmp::Ordering;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

/// Immutable ownership transferred after cold sorting and cold/cold validation.
#[derive(Clone)]
pub(crate) struct ColdUniqueKeys(Arc<Vec<IndexBuildEntry>>);

impl ColdUniqueKeys {
    /// Move the existing allocation without copying entries or encoded keys.
    pub(crate) fn new(entries: Vec<IndexBuildEntry>) -> Self {
        Self(Arc::new(entries))
    }

    /// Retained allocation bytes, excluding allocator overhead and shared-owner headers.
    #[cfg(feature = "profiling")]
    pub(crate) fn retained_bytes(&self) -> u64 {
        use crate::memcmp::MEM_CMP_KEY_INLINE;
        (self.0.capacity() * size_of::<IndexBuildEntry>()
            + self
                .0
                .iter()
                .map(|entry| {
                    let len = entry.key.as_bytes().len();
                    if len > MEM_CMP_KEY_INLINE { len } else { 0 }
                })
                .sum::<usize>()) as u64
    }
}

/// Caller-selected cross-tier contract, separate from hot distinctness.
#[derive(Clone)]
pub(crate) enum ColdValidation {
    /// Recovery and non-unique CREATE need no cross-tier checking.
    NotRequired,
    /// Unique CREATE requires checking, including when the cold vector is empty.
    Required(ColdUniqueKeys),
}

impl ColdValidation {
    /// Verify every expected partition before selecting its earliest conflict.
    pub(super) fn complete<'a>(
        &self,
        plan: &Arc<PreparedHotMerge>,
        summaries: impl Iterator<Item = Option<&'a ColdHotSummary>>,
    ) -> RuntimeOrFatalResult<Result<ColdHotCompletion, ColdHotDuplicate>> {
        let mut entries = 0;
        let mut count = 0;
        let mut conflict: Option<ColdHotDuplicate> = None;
        #[cfg(feature = "profiling")]
        let mut measurements = ColdHotMeasurements::default();
        for (partition, summary) in summaries.enumerate() {
            if partition >= plan.partitions() {
                return Err(execution_error("unexpected cold/hot partition completion"));
            }
            let range = plan.range(partition);
            match (self, summary) {
                (Self::NotRequired, None) => (),
                (Self::Required(keys), Some(summary))
                    if Arc::ptr_eq(plan, &summary.plan)
                        && Arc::ptr_eq(&keys.0, &summary.keys.0)
                        && summary.partition == partition
                        && summary.consumed == range.len() =>
                {
                    if let Some(candidate) = summary.conflict
                        && conflict.is_none_or(|old| candidate.hot_rank < old.hot_rank)
                    {
                        conflict = Some(candidate);
                    }
                    #[cfg(feature = "profiling")]
                    {
                        measurements.comparisons += summary.measurements.comparisons;
                        measurements.worker_nanos += summary.measurements.worker_nanos;
                        measurements.max_sync_nanos = measurements
                            .max_sync_nanos
                            .max(summary.measurements.max_sync_nanos);
                    }
                }
                _ => {
                    return Err(execution_error(
                        "cold/hot completion identity or coverage mismatch",
                    ));
                }
            }
            entries += range.len();
            count += 1;
        }
        if count != plan.partitions() {
            return Err(execution_error("missing cold/hot partition completions"));
        }
        Ok(match conflict {
            Some(conflict) => Err(conflict),
            None => Ok(match self {
                Self::NotRequired => ColdHotCompletion::NotRequired { entries },
                Self::Required(_) => ColdHotCompletion::Checked {
                    entries,
                    #[cfg(feature = "profiling")]
                    measurements,
                },
            }),
        })
    }
}

/// Earliest hot rank matching a live cold key in a completed partition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ColdHotDuplicate {
    /// Global hot rank identifying the conflict.
    pub(crate) hot_rank: usize,
    /// Participating live hot row.
    pub(crate) hot_row: RowID,
    /// Participating live cold row.
    pub(crate) cold_row: RowID,
}

/// Only the cursor can mint this plan-bound, exhaustive coverage summary.
pub(super) struct ColdHotSummary {
    plan: Arc<PreparedHotMerge>,
    keys: ColdUniqueKeys,
    partition: usize,
    consumed: usize,
    conflict: Option<ColdHotDuplicate>,
    #[cfg(feature = "profiling")]
    measurements: ColdHotMeasurements,
}

/// Completed cross-tier authority, retained separately from hot completion.
pub(super) enum ColdHotCompletion {
    /// The caller's trusted or non-unique contract needs no cross-tier checking.
    NotRequired { entries: usize },
    /// Every hot partition completed checking against the selected cold owner.
    Checked {
        entries: usize,
        #[cfg(feature = "profiling")]
        measurements: ColdHotMeasurements,
    },
}

impl ColdHotCompletion {
    /// Certified hot coverage for this caller's selected validation mode.
    #[inline]
    pub(super) fn entries(&self) -> usize {
        match self {
            Self::NotRequired { entries } | Self::Checked { entries, .. } => *entries,
        }
    }

    /// Cross-tier worker measurements; trusted construction performs no comparisons.
    #[cfg(feature = "profiling")]
    #[inline]
    pub(super) fn measurements(&self) -> ColdHotMeasurements {
        match self {
            Self::NotRequired { .. } => ColdHotMeasurements::default(),
            Self::Checked { measurements, .. } => *measurements,
        }
    }
}

/// Monotonic cold slice cursor retained across all batches in one hot partition.
pub(super) struct ColdHotCursor {
    keys: ColdUniqueKeys,
    position: usize,
    end: usize,
    summary: ColdHotSummary,
}

impl ColdHotCursor {
    /// Include both endpoint keys; equal keys may belong to adjacent cold slices.
    pub(super) fn new(keys: ColdUniqueKeys, plan: Arc<PreparedHotMerge>, partition: usize) -> Self {
        #[cfg(feature = "profiling")]
        let started = Instant::now();
        let (first, last) = plan.endpoints(partition);
        #[cfg(feature = "profiling")]
        let mut comparisons = 0;
        let position = keys.0.partition_point(|entry| {
            #[cfg(feature = "profiling")]
            {
                comparisons += 1;
            }
            entry.key < first.key
        });
        let end = position
            + keys.0[position..].partition_point(|entry| {
                #[cfg(feature = "profiling")]
                {
                    comparisons += 1;
                }
                entry.key <= last.key
            });
        #[cfg(feature = "profiling")]
        let nanos = started.elapsed().as_nanos() as u64;
        Self {
            keys: keys.clone(),
            position,
            end,
            summary: ColdHotSummary {
                plan,
                keys,
                partition,
                consumed: 0,
                conflict: None,
                #[cfg(feature = "profiling")]
                measurements: ColdHotMeasurements {
                    comparisons,
                    worker_nanos: nanos,
                    max_sync_nanos: nanos,
                },
            },
        }
    }

    /// Validate one bounded batch synchronously before packing; the caller yields
    /// between batches and keeps consuming after construction is inhibited.
    pub(super) fn validate(
        &mut self,
        batch: &HotBatch<'_>,
        stop: &AtomicBool,
    ) -> RuntimeOrFatalResult<()> {
        let range = self.summary.plan.range(self.summary.partition);
        let ranks = batch.ranks();
        if ranks.start != range.start + self.summary.consumed || ranks.end > range.end {
            return Err(execution_error("cold/hot batch coverage mismatch"));
        }
        observe_stop(stop)?;
        #[cfg(feature = "profiling")]
        let started = Instant::now();
        if self.summary.conflict.is_none() {
            for offset in 0..ranks.len() {
                let (_, hot) = batch.entry(offset).unwrap_or_else(|| {
                    unreachable!("cold validation uses bounded batch coordinates")
                });
                self.seek(&hot.key);
                if self.position < self.end
                    && self.compare(self.position, &hot.key) == Ordering::Equal
                {
                    self.summary.conflict = Some(ColdHotDuplicate {
                        hot_rank: ranks.start + offset,
                        hot_row: hot.row_id,
                        cold_row: self.keys.0[self.position].row_id,
                    });
                    batch.inhibit_construction();
                    break;
                }
            }
        }
        #[cfg(feature = "profiling")]
        {
            let nanos = started.elapsed().as_nanos() as u64;
            self.summary.measurements.worker_nanos += nanos;
            self.summary.measurements.max_sync_nanos =
                self.summary.measurements.max_sync_nanos.max(nanos);
        }
        observe_stop(stop)?;
        self.summary.consumed += ranks.len();
        Ok(())
    }

    #[inline]
    fn compare(&mut self, position: usize, key: &BTreeKey) -> Ordering {
        #[cfg(feature = "profiling")]
        {
            self.summary.measurements.comparisons += 1;
        }
        self.keys.0[position].key.cmp(key)
    }

    // At most twice usize::BITS comparisons, even for a very large cold gap.
    // All arithmetic is bounded by the inclusive partition's selected slice.
    fn seek(&mut self, key: &BTreeKey) {
        if self.position == self.end || self.compare(self.position, key) != Ordering::Less {
            return;
        }
        let base = self.position;
        let remaining = self.end - base;
        let mut step = 1usize;
        let mut low = base + 1;
        while step < remaining && self.compare(base + step, key) == Ordering::Less {
            low = base + step + 1;
            step = step.saturating_mul(2).min(remaining);
        }
        let mut high = base + step.min(remaining);
        while low < high {
            let mid = low + (high - low) / 2;
            if self.compare(mid, key) == Ordering::Less {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        self.position = low;
    }

    /// Transfer the summary through the stream's existing completion path.
    #[inline]
    pub(super) fn finish(self) -> ColdHotSummary {
        self.summary
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::component::{ComponentRegistry, RegistryBuilder};
    use crate::conf::ThreadPoolConfig;
    use crate::index::build::merge::{
        CompletedPartition, HotMergeConsumption, HotPartitionConsumer, PartitionMergeStream,
        test_prepare_packed, test_runs,
    };
    use crate::index::build::{DuplicateCheck, MemoryBudget};
    use crate::poison::EnginePoisoner;
    use crate::quiescent::QuiescentGuard;
    use crate::runtime::thread_pool::{ThreadPool, ThreadPoolWorkers};
    use crate::runtime::yield_now;

    struct PoolScope(ComponentRegistry);

    impl Drop for PoolScope {
        fn drop(&mut self) {
            assert!(!self.0.shutdown_all().is_degraded());
        }
    }

    struct Check(ColdUniqueKeys);

    impl HotPartitionConsumer for Check {
        type Output = ColdHotSummary;

        async fn consume(
            &self,
            mut stream: PartitionMergeStream,
        ) -> RuntimeOrFatalResult<CompletedPartition<Self::Output>> {
            let (plan, partition) = stream.identity();
            let mut cursor = ColdHotCursor::new(self.0.clone(), plan, partition);
            while let Some(batch) = stream.next_batch()? {
                let consumed = cursor.summary.consumed;
                let position = cursor.position;
                let conflict = cursor.summary.conflict;
                #[cfg(feature = "profiling")]
                let comparisons = cursor.summary.measurements.comparisons;
                // Stop checks remain authoritative even after a conflict.
                assert!(cursor.validate(&batch, &AtomicBool::new(true)).is_err());
                assert_eq!(cursor.summary.consumed, consumed);
                assert_eq!(cursor.position, position);
                assert_eq!(cursor.summary.conflict, conflict);
                cursor.validate(&batch, &AtomicBool::new(false))?;
                assert_eq!(cursor.summary.consumed, consumed + batch.ranks().len());
                if conflict.is_some() {
                    assert_eq!(cursor.summary.conflict, conflict);
                    assert_eq!(cursor.position, position);
                    #[cfg(feature = "profiling")]
                    assert_eq!(cursor.summary.measurements.comparisons, comparisons);
                }
                if cursor.summary.conflict.is_some() {
                    assert!(
                        batch.construction_inhibited(),
                        "discovering batch must see live inhibition"
                    );
                }
                // Replaying the same batch must not count its coverage twice.
                assert!(cursor.validate(&batch, &AtomicBool::new(false)).is_err());
                assert_eq!(cursor.summary.consumed, consumed + batch.ranks().len());
                yield_now().await;
            }
            stream.finish(cursor.finish())
        }
    }

    async fn pool() -> (PoolScope, QuiescentGuard<ThreadPool>) {
        let mut builder = RegistryBuilder::new();
        builder.build::<EnginePoisoner>(()).await.unwrap();
        builder
            .build::<ThreadPool>(ThreadPoolConfig::default().worker_threads(2))
            .await
            .unwrap();
        builder.build::<ThreadPoolWorkers>(()).await.unwrap();
        let registry = builder.finish();
        let pool = registry.dependency::<ThreadPool>();
        (PoolScope(registry), pool)
    }

    fn keys(values: impl IntoIterator<Item = u32>) -> ColdUniqueKeys {
        ColdUniqueKeys::new(
            values
                .into_iter()
                .map(|value| IndexBuildEntry {
                    key: BTreeKey::from(value),
                    row_id: RowID::new(u64::from(value)),
                })
                .collect(),
        )
    }

    /// Purpose: Compare cold validation against membership across gaps, equal-key cuts and early conflicts followed by full batches and later hot duplicates.
    /// Expected: Partitions retain their first match without further searches, reject stopped/replayed batches, and finish exact coverage with deterministic hot and cold conflicts.
    #[test]
    fn cold_hot_partitions_match_membership_oracle() {
        smol::block_on(async {
            let (_registry, pool) = pool().await;
            for (groups, cold, partitions, batch) in [
                (vec![], vec![1, 2], 0, 3),
                (vec![vec![0, 1, 3, 4, 8]], vec![], 1, 2),
                (vec![vec![0, 1, 3, 4, 8]], vec![4], 1, 2),
                (vec![vec![0, 2, 4], vec![1, 3, 5]], vec![0, 5], 4, 2),
                (
                    vec![vec![1, 2, 2, 2, 9], vec![2, 2, 2, 7]],
                    vec![2, 7],
                    5,
                    2,
                ),
                (
                    vec![vec![0, 8, 40_001], vec![1, 6, 40_000]],
                    (10..40_001).collect(),
                    4,
                    1,
                ),
                (
                    vec![(0..65_553).collect(), vec![65_552]],
                    vec![0, 32_768, 65_552],
                    1,
                    32_768,
                ),
            ] {
                let runs = test_runs(
                    groups
                        .into_iter()
                        .map(|group| group.into_iter().map(BTreeKey::from).collect())
                        .collect(),
                    DuplicateCheck::Collect,
                    MemoryBudget::new(16 * 1024 * 1024),
                );
                let mut oracle: Vec<_> = runs
                    .runs()
                    .iter()
                    .enumerate()
                    .flat_map(|(run, entries)| {
                        (0..entries.entries().len()).map(move |position| (run, position))
                    })
                    .collect();
                oracle.sort_by(|&left, &right| runs.compare(left, right).unwrap());
                let cold = keys(cold);
                let plan =
                    test_prepare_packed(runs.clone(), pool.clone(), 2, partitions, batch).await;
                let mut merge =
                    HotMergeConsumption::new(plan.clone(), pool.clone(), Check(cold.clone()));
                let outcome = merge.execute().await.unwrap();
                let hot_conflict = oracle.windows(2).enumerate().find_map(|(rank, pair)| {
                    let left = runs.entry(pair[0].0, pair[0].1).unwrap();
                    let right = runs.entry(pair[1].0, pair[1].1).unwrap();
                    (left.key == right.key).then_some((rank + 1, [left.row_id, right.row_id]))
                });
                assert_eq!(
                    outcome.validation.err().map(|c| (c.right_rank, c.rows)),
                    hot_conflict
                );
                let expected: Vec<_> = oracle
                    .iter()
                    .enumerate()
                    .filter_map(|(rank, &(run, position))| {
                        let hot = runs.entry(run, position).unwrap();
                        cold.0
                            .iter()
                            .find(|entry| entry.key == hot.key)
                            .map(|entry| ColdHotDuplicate {
                                hot_rank: rank,
                                hot_row: hot.row_id,
                                cold_row: entry.row_id,
                            })
                    })
                    .collect();
                for (partition, summary) in outcome.outputs.iter().enumerate() {
                    let range = plan.range(partition);
                    assert_eq!(summary.consumed, range.len());
                    assert_eq!(
                        summary.conflict,
                        expected
                            .iter()
                            .find(|hit| range.contains(&hit.hot_rank))
                            .copied()
                    );
                }
                let completion = ColdValidation::Required(cold)
                    .complete(&plan, outcome.outputs.iter().map(Some))
                    .unwrap();
                match completion {
                    Ok(completion) => {
                        assert_eq!(expected, []);
                        assert_eq!(completion.entries(), oracle.len());
                    }
                    Err(conflict) => assert_eq!(Some(&conflict), expected.first()),
                }
            }
            drop(pool);
        });
    }

    /// Purpose: Reject unchecked, missing, foreign and incomplete cross-tier summaries before installation authority is minted.
    /// Expected: Only exhaustive summaries bound to the selected plan and partition satisfy required checking.
    #[test]
    fn cold_completion_rejects_invalid_evidence() {
        smol::block_on(async {
            let (_registry, pool) = pool().await;
            let runs = test_runs(
                vec![vec![BTreeKey::from(1u32)]],
                DuplicateCheck::Collect,
                MemoryBudget::new(1024 * 1024),
            );
            let plan = test_prepare_packed(runs.clone(), pool.clone(), 2, 1, 1).await;
            let foreign = test_prepare_packed(runs, pool.clone(), 2, 1, 1).await;
            let keys = keys([]);
            let validation = ColdValidation::Required(keys.clone());
            assert!(validation.complete(&plan, std::iter::empty()).is_err());
            assert!(validation.complete(&plan, [None].into_iter()).is_err());
            let mut summary = ColdHotCursor::new(keys, plan.clone(), 0).finish();
            assert!(
                validation
                    .complete(&plan, [Some(&summary)].into_iter())
                    .is_err()
            );
            summary.consumed = 1;
            assert!(
                ColdValidation::Required(ColdUniqueKeys::new(Vec::new()))
                    .complete(&plan, [Some(&summary)].into_iter())
                    .is_err()
            );
            assert!(
                validation
                    .complete(&foreign, [Some(&summary)].into_iter())
                    .is_err()
            );
            assert!(
                ColdValidation::NotRequired
                    .complete(&plan, [Some(&summary)].into_iter())
                    .is_err()
            );
            assert!(
                validation
                    .complete(&plan, [Some(&summary)].into_iter())
                    .unwrap()
                    .is_ok()
            );
            drop(pool);
        });
    }

    /// Purpose: Preserve the original cold vector allocation and count outlined keys without charging inline storage twice.
    /// Expected: Shared owners point at the moved allocation and retained bytes include vector capacity plus only outlined payloads.
    #[test]
    fn cold_owner_moves_storage_without_copying() {
        let mut entries = Vec::with_capacity(8);
        entries.push(IndexBuildEntry {
            key: BTreeKey::from(1u32),
            row_id: RowID::new(1),
        });
        entries.push(IndexBuildEntry {
            key: BTreeKey::from(vec![42u8; 512].as_slice()),
            row_id: RowID::new(2),
        });
        let pointer = entries.as_ptr();
        #[cfg(feature = "profiling")]
        let expected = entries.capacity() * size_of::<IndexBuildEntry>() + 512;
        let keys = ColdUniqueKeys::new(entries);
        assert_eq!(keys.0.as_ptr(), pointer);
        assert!(Arc::ptr_eq(&keys.0, &keys.clone().0));
        #[cfg(feature = "profiling")]
        assert_eq!(keys.retained_bytes(), expected as u64);
    }
}
