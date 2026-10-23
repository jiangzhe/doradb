//! Checkpoint strategy selection and application against a captured secondary root.
//! The bounded planner lives here; physical execution belongs to `checkpoint_builder`.
use super::checkpoint_builder::BuildMode;
use super::co_rank;
use super::merge::{HotEntryRef, execution_error};
use super::{BudgetedVec, BuildOutcome, MemoryBudget, RunKind, SortedRuns};
use crate::catalog::SecondaryIndexRoot;
use crate::error::{RuntimeError, RuntimeOrFatalResult};
use crate::file::table_file::MutableTableFile;
use crate::id::{BlockID, RowID, TrxID};
use crate::index::btree::{BTreeSlot, BTreeValue};
use crate::index::disk_tree::{DiskTree, DiskTreeSpec};
use crate::index::disk_tree_stats::DiskTreeStats;
#[cfg(feature = "profiling")]
use crate::profiling::clock::Instant;
use crate::quiescent::QuiescentGuard;
use crate::runtime::thread_pool::ThreadPool;
use error_stack::ResultExt;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

#[cfg(test)]
pub(crate) use tests::{
    apply_non_unique, apply_unique, mutation_runs, partial_rewrite, partial_with_rewrite_work,
    reconstruct, reconstruct_admitted, strategy,
};

#[cfg(test)]
pub(crate) use super::checkpoint_builder::checkpoint_budgets;

pub(crate) use super::checkpoint_builder::{CheckpointAdmission, CheckpointBuildPolicy};

// Coefficients are work units per persisted block. Read includes decoding;
// packing includes comparison/fence work; write includes validation and ingress.
// Task 000333 calibrates volume and retention against matched total checkpoint
// costs, including wide-key growth and broad deletion.
const READ: f64 = 1.0;
const PACK: f64 = 2.0;
const WRITE: f64 = 4.0;
const RETAIN_MARGIN: f64 = 0.99;
const PUT_RECONSTRUCT_VOLUME: u64 = 16;
const INITIAL_PATHS: usize = 16;
const MAX_PATHS: usize = 64;
const MAX_IDENTITIES: usize = 4096;

/// Inputs for one secondary-index checkpoint against a captured root.
/// Application consumes this request and returns a candidate root for the
/// enclosing table checkpoint to publish after its remaining work succeeds.
pub(crate) struct IndexCheckpoint<'a, 'tree, F: DiskTreeSpec> {
    /// Captured tree used by planning and physical reads.
    pub(super) tree: &'a DiskTree<'tree, F>,
    /// Descriptor captured with the borrowed tree from the pinned index generation.
    pub(super) root: SecondaryIndexRoot,
    /// Private file fork receiving replacement blocks.
    pub(super) file: &'a mut MutableTableFile,
    /// Selected sorted mutation runs.
    pub(super) runs: Arc<SortedRuns>,
    /// Checkpoint timestamp stored in replacement nodes.
    pub(super) ts: TrxID,
    /// Shared worker pool for merge and packing jobs.
    pub(super) pool: QuiescentGuard<ThreadPool>,
    /// Retained checkpoint memory and progress admission.
    pub(super) admission: Arc<CheckpointAdmission>,
}

impl<'a, 'tree, F: DiskTreeSpec> IndexCheckpoint<'a, 'tree, F> {
    /// Retain checkpoint inputs without allocating or starting work.
    #[inline]
    pub(crate) fn new(
        tree: &'a DiskTree<'tree, F>,
        root: SecondaryIndexRoot,
        file: &'a mut MutableTableFile,
        runs: Arc<SortedRuns>,
        ts: TrxID,
        pool: QuiescentGuard<ThreadPool>,
        admission: Arc<CheckpointAdmission>,
    ) -> Self {
        Self {
            tree,
            root,
            file,
            runs,
            ts,
            pool,
            admission,
        }
    }

    /// Reject a descriptor from a different root before planning or output starts.
    pub(super) fn validate_root(&self) -> RuntimeOrFatalResult<()> {
        if self.root.block_id() != self.tree.snapshot().1 {
            return Err(execution_error(
                "checkpoint descriptor differs from captured tree",
            ));
        }
        Ok(())
    }

    /// Select and apply the physical checkpoint strategy against the captured root.
    pub(crate) async fn apply(self) -> RuntimeOrFatalResult<SecondaryIndexRoot> {
        self.validate_root()?;
        #[cfg(feature = "profiling")]
        let started = Instant::now();
        let strategy = {
            let scratch = self.admission.assembler.clone();
            let mut planner = CheckpointPlanner::new(self.tree, self.root, &self.runs, &scratch)?;
            let strategy = planner.choose().await?;
            #[cfg(feature = "profiling")]
            planner.record(strategy, started, &self.admission);
            strategy
        };
        let mode = match strategy {
            Strategy::NoWork => return Ok(self.root),
            Strategy::Reconstruct => BuildMode::Reconstruct,
            Strategy::PartialRewrite => BuildMode::PartialRewrite,
        };
        self.build(mode).await
    }
}

/// Physical work selected before any replacement DiskTree image is emitted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Strategy {
    NoWork,
    Reconstruct,
    PartialRewrite,
}

#[derive(Clone, Copy)]
struct Probe {
    #[cfg(test)]
    row: Option<RowID>,
    multiplicity: usize,
    changed: bool,
    inserted: bool,
    removed: bool,
}

/// Inputs, bounded sampling state and costs for one strategy selection.
/// Estimated costs affect work selection only; they grant no content authority.
struct CheckpointPlanner<'a, 'tree, F: DiskTreeSpec> {
    tree: &'a DiskTree<'tree, F>,
    runs: &'a SortedRuns,
    mutation_count: usize,
    stats: DiskTreeStats,
    put_count: u64,
    put_bytes: u64,
    delete_count: u64,
    base_mass: u64,
    put_mass: u64,
    node_reads: usize,
    delta_paths: usize,
    identity_probes: usize,
    seen: BudgetedVec<(HotEntryRef, Probe)>,
    delta_leaves: BudgetedVec<BlockID>,
    changed_keys: usize,
    inserted_sum: f64,
    removed_sum: f64,
    inserted_bytes: f64,
    removed_bytes: f64,
    effective_sum: f64,
    uncertain: bool,
    reconstruct_cost: f64,
    partial_cost: f64,
}

impl<'a, 'tree, F: DiskTreeSpec> CheckpointPlanner<'a, 'tree, F> {
    /// Initialize selection state without allocating sampling caches.
    fn new(
        tree: &'a DiskTree<'tree, F>,
        root: SecondaryIndexRoot,
        runs: &'a SortedRuns,
        scratch: &'a MemoryBudget,
    ) -> RuntimeOrFatalResult<Self> {
        let mut put_count = 0u64;
        let mut put_bytes = 0u64;
        let mut delete_count = 0u64;
        // Runs own disjoint entries under one input budget, so their total
        // entry count and full key bytes fit usize as well as u64.
        for run in runs.runs() {
            let count = run.entries().len() as u64;
            match run.kind {
                RunKind::Put => {
                    put_count += count;
                    put_bytes += run.mutation_key_bytes;
                }
                RunKind::Delete => delete_count += count,
                RunKind::Create => {
                    return Err(execution_error("checkpoint planner received CREATE input"));
                }
            }
        }
        let mutation_count = (put_count + delete_count) as usize;
        let stats = root.stats();
        Ok(Self {
            tree,
            runs,
            mutation_count,
            stats,
            put_count,
            put_bytes,
            delete_count,
            base_mass: Self::mass(stats.entries, stats.encoded_key_bytes)?,
            put_mass: Self::mass(put_count, put_bytes)?,
            node_reads: 0,
            delta_paths: 0,
            identity_probes: 0,
            seen: BudgetedVec::new(scratch),
            delta_leaves: BudgetedVec::new(scratch),
            changed_keys: 0,
            inserted_sum: 0.0,
            removed_sum: 0.0,
            inserted_bytes: 0.0,
            removed_bytes: 0.0,
            effective_sum: 0.0,
            uncertain: false,
            reconstruct_cost: 0.0,
            partial_cost: 0.0,
        })
    }

    fn mass(entries: u64, key_bytes: u64) -> RuntimeOrFatalResult<u64> {
        entries
            .checked_mul((size_of::<BTreeSlot>() + F::LeafValue::ENCODED_LEN) as u64)
            .and_then(|bytes| bytes.checked_add(key_bytes))
            .ok_or_else(|| execution_error("checkpoint uncompressed entry mass overflow"))
    }

    /// Required totals settle PUT-only choices before any cache admission or node read.
    async fn choose(&mut self) -> RuntimeOrFatalResult<Strategy> {
        if self.mutation_count == 0 {
            return Ok(Strategy::NoWork);
        }
        let Some(root) = self.tree.snapshot().1 else {
            return Ok(Strategy::Reconstruct);
        };
        if self.stats.height == 0 {
            return Ok(Strategy::Reconstruct);
        }
        if self.delete_count == 0 {
            return Ok(
                if self.put_count / PUT_RECONSTRUCT_VOLUME >= self.stats.entries
                    || self.put_mass / PUT_RECONSTRUCT_VOLUME >= self.base_mass
                {
                    Strategy::Reconstruct
                } else {
                    Strategy::PartialRewrite
                },
            );
        }
        if self.mutation_count < 128 {
            return Ok(Strategy::PartialRewrite);
        }
        self.seen
            .ensure_capacity(MAX_PATHS, "checkpoint probe keys")
            .change_context(RuntimeError::CheckpointExecution)?;
        self.delta_leaves
            .ensure_capacity(MAX_PATHS, "checkpoint probe leaves")
            .change_context(RuntimeError::CheckpointExecution)?;
        for target in [INITIAL_PATHS, MAX_PATHS] {
            for sample in self.delta_paths..target {
                self.sample_delta(root, sample).await?;
                if self.uncertain {
                    return Ok(Strategy::PartialRewrite);
                }
            }
            self.update_costs();
            if let Some(strategy) = self.decision() {
                return Ok(strategy);
            }
        }
        Ok(Strategy::PartialRewrite)
    }

    /// Probe one merged rank, retaining duplicate sampling weight when its key is cached.
    async fn sample_delta(&mut self, root: BlockID, sample: usize) -> RuntimeOrFatalResult<()> {
        let runs = self.runs;
        let count = self.mutation_count;
        let rank = if count <= MAX_PATHS {
            sample.min(count - 1)
        } else {
            (((sample as u8).reverse_bits() as usize >> 2) * count + count / 2) / MAX_PATHS
        };
        let cut = if runs.single_run().is_some() {
            co_rank::direct(runs, rank)?
        } else {
            let BuildOutcome::Complete(cut) =
                co_rank::co_rank(runs, rank, &AtomicBool::new(false))?
            else {
                unreachable!("bounded selector has no stop request")
            };
            cut
        };
        if let Some(reference) = cut.right {
            let key = reference.resolve(runs).key.as_bytes();
            let cached = self
                .seen
                .iter()
                .find(|(old, _)| old.resolve(runs).key.as_bytes() == key);
            let result = if let Some((_, result)) = cached {
                Some(*result)
            } else {
                let (leaf, owner) = self.lookup(root, key).await?;
                let result = self.probe(key, owner);
                if let Some(result) = result {
                    if result.changed {
                        self.changed_keys += 1;
                        if !self.delta_leaves.contains(&leaf) {
                            self.delta_leaves.push_reserved(leaf);
                        }
                    }
                    self.seen.push_reserved((reference, result));
                }
                result
            };
            match result {
                Some(result) => {
                    // Repeated quantiles still carry sampling weight. Cache the
                    // bounded identity inspection, not its statistical contribution.
                    let weight = count as f64 / result.multiplicity.max(1) as f64;
                    if result.inserted {
                        self.inserted_sum += weight;
                        self.inserted_bytes += weight * key.len() as f64;
                    }
                    if result.removed {
                        self.removed_sum += weight;
                        self.removed_bytes += weight * key.len() as f64;
                    }
                    if result.changed {
                        self.effective_sum += weight;
                    }
                }
                None => self.uncertain = true,
            }
        }
        self.delta_paths += 1;
        Ok(())
    }

    /// Estimate mutation effects and locality without replacing any known root total.
    fn update_costs(&mut self) {
        let samples = self.delta_paths as f64;
        let n = self.stats.entries as f64;
        let l = self.stats.leaf_nodes as f64;
        let b = self.stats.branch_nodes as f64;
        let removed = (self.removed_sum / samples).min(n);
        let inserted = (self.inserted_sum / samples).min(self.put_count as f64);
        let width = (size_of::<BTreeSlot>() + F::LeafValue::ENCODED_LEN) as f64;
        let removed_mass = (self.removed_bytes / samples).min(self.stats.encoded_key_bytes as f64)
            + removed * width;
        let inserted_mass =
            (self.inserted_bytes / samples).min(self.put_bytes as f64) + inserted * width;
        let mass_per_leaf = self.base_mass as f64 / l;
        let output_entries = n - removed + inserted;
        let output = if output_entries == 0.0 {
            0.0
        } else {
            ((self.base_mass as f64 - removed_mass + inserted_mass) / mass_per_leaf).max(1.0)
        };
        let growth = inserted_mass / mass_per_leaf;
        let u = self.delta_leaves.len() as f64;
        // Cap the extrapolation population before applying observed diversity.
        // Capping only the final result treats moderate contiguous batches as
        // touching every leaf, even after refinement observes a small leaf set.
        let touched = if self.changed_keys == 0 {
            0.0
        } else {
            u.max((self.effective_sum / samples).min(l) * u / self.changed_keys as f64)
                .min(l)
        };
        let output_branches = if output <= 1.0 {
            0.0
        } else {
            (b * (output - 1.0) / (l - 1.0).max(1.0)).max(1.0)
        };
        let ancestors = (f64::from(self.stats.height) * touched).min(b);
        let touched_output = (output - (l - touched)).max(0.0);
        self.reconstruct_cost = (l + b) * READ + (output + output_branches) * (PACK + WRITE);
        self.partial_cost = (touched + ancestors) * READ
            + (touched_output + ancestors + growth.sqrt()) * (PACK + WRITE);
    }

    /// Deterministic diversity extrapolation is a heuristic, not a confidence bound.
    fn decision(&self) -> Option<Strategy> {
        if self.uncertain || self.changed_keys == 0 {
            return Some(Strategy::PartialRewrite);
        }
        let ratio = self.partial_cost / self.reconstruct_cost.max(1.0);
        if ratio > 1.35 || ratio < 0.75 || self.delta_paths == MAX_PATHS {
            return Some(if ratio > RETAIN_MARGIN {
                Strategy::Reconstruct
            } else {
                Strategy::PartialRewrite
            });
        }
        None
    }

    /// Reduce one sampled key while charging the shared identity-inspection allowance.
    fn probe(&mut self, key: &[u8], owner: Option<RowID>) -> Option<Probe> {
        let runs = self.runs;
        let examined = &mut self.identity_probes;
        if *examined == MAX_IDENTITIES {
            return None;
        }
        *examined += 1;
        let mut multiplicity = 0;
        let mut latest = None;
        let mut deleted = false;
        for run in runs.runs() {
            let entries = run.matching_entries(key);
            multiplicity += entries.len();
            let Some(last) = entries.last() else {
                continue;
            };
            match run.kind {
                RunKind::Put => {
                    if latest.is_none_or(|(group, _)| run.group_id > group) {
                        latest = Some((run.group_id, last.row_id));
                    }
                }
                RunKind::Delete if !F::UNIQUE => deleted = true,
                RunKind::Delete => {
                    // Historical deletes preceding unique-index creation can repeat
                    // a key with different owners. Inspect all under the shared cap.
                    for entry in entries {
                        if *examined == MAX_IDENTITIES {
                            return None;
                        }
                        *examined += 1;
                        deleted |= owner == Some(entry.row_id);
                    }
                }
                RunKind::Create => return None,
            }
        }
        let row = if !F::UNIQUE && deleted {
            None
        } else {
            latest
                .map(|(_, row)| row)
                .or_else(|| owner.filter(|_| !deleted))
        };
        Some(Probe {
            #[cfg(test)]
            row,
            multiplicity,
            changed: row != owner,
            inserted: owner.is_none() && row.is_some(),
            removed: owner.is_some() && row.is_none(),
        })
    }

    /// Resolve one mutation key to its captured owner and destination leaf.
    async fn lookup(
        &mut self,
        mut block: BlockID,
        key: &[u8],
    ) -> RuntimeOrFatalResult<(BlockID, Option<RowID>)> {
        let mut expected = None;
        loop {
            self.node_reads += 1;
            let guard = self.tree.read_node(block).await?;
            let node = guard.view();
            if expected.is_some_and(|height| node.height() != height) {
                return Err(execution_error(
                    "checkpoint delta probe child height mismatch",
                ));
            }
            if node.is_leaf() {
                let row = match node.search_key(key)? {
                    Some(slot) => Some(
                        F::entry_row(&node.leaf_entry(slot)?)
                            .change_context(RuntimeError::CheckpointExecution)?,
                    ),
                    None => None,
                };
                return Ok((block, row));
            }
            let child = node.lower_bound_child_entry_idx(key)?;
            expected = Some(node.height() - 1);
            block = node.child_block(child)?;
        }
    }

    /// Record successful selection without exposing planner state to physical execution.
    #[cfg(feature = "profiling")]
    fn record(&self, strategy: Strategy, started: Instant, admission: &CheckpointAdmission) {
        admission.measure(|m| {
            m.no_work += u64::from(strategy == Strategy::NoWork);
            m.reconstruct += u64::from(strategy == Strategy::Reconstruct);
            m.partial += u64::from(strategy == Strategy::PartialRewrite);
            for run in self.runs.runs() {
                match run.kind {
                    RunKind::Put => m.put_entries += run.entries().len() as u64,
                    RunKind::Delete => m.delete_entries += run.entries().len() as u64,
                    RunKind::Create => (),
                }
            }
            m.probe_nanos += started.elapsed().as_nanos() as u64;
            m.base_nodes_read += self.node_reads as u64;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conf::CheckpointIndexBuildConfig;
    use crate::error::RuntimeError;
    use crate::error::{ResourceError, RuntimeOrFatalError};
    use crate::file::table_file::MutableTableFile;
    use crate::id::{RowID, TrxID};
    use crate::index::BTreeKey;
    use crate::index::build::checkpoint::CheckpointBuildPolicy;
    use crate::index::build::checkpoint_builder::{RewriteWork, rewrite_work};
    use crate::index::build::co_rank;
    use crate::index::build::cross_tier::test_with_cold;
    use crate::index::build::loser_tree::LoserTree;
    use crate::index::build::merge::test_pool;
    use crate::index::build::{
        BudgetedVec, BuildEntries, BuildOutcome, DuplicateCheck, IndexBuildEntry, MemoryBudget,
        SortedRun, fail_build_budget,
    };
    use crate::index::disk_tree::{NonUniqueDiskTree, UniqueDiskTree, UniqueDiskTreeSpec};
    use crate::index::disk_tree_stats::{root_descriptor, verify_root};
    use crate::memcmp::MEM_CMP_KEY_INLINE;
    use crate::quiescent::QuiescentGuard;
    use crate::runtime::thread_pool::ThreadPool;
    use error_stack::ResultExt;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;

    struct MutationStream<'a> {
        runs: &'a SortedRuns,
        tree: LoserTree,
        next: Option<HotEntryRef>,
    }

    impl<'a> MutationStream<'a> {
        fn new(runs: &'a SortedRuns) -> RuntimeOrFatalResult<Self> {
            if runs.runs().iter().any(|run| run.kind == RunKind::Create) {
                return Err(execution_error("checkpoint merge received CREATE input"));
            }
            let start = co_rank::endpoint(runs, 0)?;
            let count = runs.runs().iter().map(|run| run.entries().len()).sum();
            let end = co_rank::endpoint(runs, count)?;
            let mut tree = LoserTree::new(runs, &start.positions, &end.positions, &runs.budget)
                .change_context(RuntimeError::CheckpointExecution)?;
            let next = tree.pop(runs);
            Ok(Self { runs, tree, next })
        }

        fn pop(&mut self) -> Option<HotEntryRef> {
            let next = self.next.take()?;
            self.next = self.tree.pop(self.runs);
            Some(next)
        }
    }

    /// Inspect production strategy and sample count for structural-oracle fixtures.
    pub(crate) async fn strategy<F: DiskTreeSpec>(
        tree: &DiskTree<'_, F>,
        runs: &SortedRuns,
    ) -> (&'static str, usize) {
        let scratch =
            MemoryBudget::new(CheckpointIndexBuildConfig::default().assembler_scratch_bytes);
        let mut planner =
            CheckpointPlanner::new(tree, root_descriptor(tree).await.unwrap(), runs, &scratch)
                .unwrap();
        let strategy = planner.choose().await.unwrap();
        assert!(planner.delta_paths <= 64);
        assert!(planner.identity_probes <= 4096);
        let strategy = match strategy {
            Strategy::NoWork => "no-work",
            Strategy::Reconstruct => "reconstruct",
            Strategy::PartialRewrite => "partial",
        };
        (strategy, planner.delta_paths)
    }

    /// Exercise the production selector from the physical ownership fixture.
    pub(crate) async fn apply_unique(
        tree: &UniqueDiskTree<'_>,
        file: &mut MutableTableFile,
        runs: &SortedRuns,
        ts: TrxID,
    ) -> RuntimeOrFatalResult<Option<BlockID>> {
        tests::apply_fixture(tree, file, runs, ts).await
    }

    /// Exercise the production selector from the exact-membership fixture.
    pub(crate) async fn apply_non_unique(
        tree: &NonUniqueDiskTree<'_>,
        file: &mut MutableTableFile,
        runs: &SortedRuns,
        ts: TrxID,
    ) -> RuntimeOrFatalResult<Option<BlockID>> {
        tests::apply_fixture(tree, file, runs, ts).await
    }

    /// Force partial work and inspect test-only boundary work and scratch usage.
    pub(crate) async fn partial_with_rewrite_work<F: DiskTreeSpec>(
        tree: &DiskTree<'_, F>,
        file: &mut MutableTableFile,
        runs: Arc<SortedRuns>,
        pool: QuiescentGuard<ThreadPool>,
        policy: CheckpointBuildPolicy,
        ts: TrxID,
    ) -> (Option<BlockID>, RewriteWork) {
        let admission = CheckpointAdmission::new(policy).unwrap();
        let root = IndexCheckpoint::new(
            tree,
            root_descriptor(tree).await.unwrap(),
            file,
            runs,
            ts,
            pool,
            admission.clone(),
        )
        .build(BuildMode::PartialRewrite)
        .await
        .unwrap();
        (
            verify_root(tree, root).await.unwrap(),
            rewrite_work(&admission),
        )
    }

    /// Run the production reconstruction path with fixture-owned admission.
    pub(crate) async fn reconstruct<F: DiskTreeSpec>(
        tree: &DiskTree<'_, F>,
        file: &mut MutableTableFile,
        runs: Arc<SortedRuns>,
        pool: QuiescentGuard<ThreadPool>,
        policy: CheckpointBuildPolicy,
        ts: TrxID,
    ) -> RuntimeOrFatalResult<Option<BlockID>> {
        let admission = CheckpointAdmission::new(policy)?;
        reconstruct_admitted(tree, file, runs, pool, admission, ts).await
    }

    /// Reconstruct with caller-retained budget owners for fault and cleanup assertions.
    pub(crate) async fn reconstruct_admitted<F: DiskTreeSpec>(
        tree: &DiskTree<'_, F>,
        file: &mut MutableTableFile,
        runs: Arc<SortedRuns>,
        pool: QuiescentGuard<ThreadPool>,
        admission: Arc<CheckpointAdmission>,
        ts: TrxID,
    ) -> RuntimeOrFatalResult<Option<BlockID>> {
        let root = IndexCheckpoint::new(
            tree,
            root_descriptor(tree).await?,
            file,
            runs,
            ts,
            pool,
            admission,
        )
        .build(BuildMode::Reconstruct)
        .await?;
        verify_root(tree, root).await
    }

    /// Force the production partial path for independent physical-oracle fixtures.
    pub(crate) async fn partial_rewrite<F: DiskTreeSpec>(
        tree: &DiskTree<'_, F>,
        file: &mut MutableTableFile,
        runs: Arc<SortedRuns>,
        pool: QuiescentGuard<ThreadPool>,
        policy: CheckpointBuildPolicy,
        ts: TrxID,
    ) -> RuntimeOrFatalResult<Option<BlockID>> {
        let admission = CheckpointAdmission::new(policy)?;
        let root = IndexCheckpoint::new(
            tree,
            root_descriptor(tree).await?,
            file,
            runs,
            ts,
            pool,
            admission,
        )
        .build(BuildMode::PartialRewrite)
        .await?;
        verify_root(tree, root).await
    }

    /// Construct real checkpoint runs in reverse arrival order for storage fixtures.
    pub(crate) fn mutation_runs(
        groups: Vec<(RunKind, Vec<(BTreeKey, RowID)>)>,
        unique: bool,
    ) -> SortedRuns {
        let budget = MemoryBudget::new(64 * 1024 * 1024);
        let mut runs = BudgetedVec::new(&budget);
        for (group_id, (kind, entries)) in groups.into_iter().enumerate() {
            let mut mutation_key_bytes = 0u64;
            let mut rows = BuildEntries::new(&budget);
            for (key, row_id) in entries {
                mutation_key_bytes += key.as_bytes().len() as u64;
                rows.push_with(
                    if key.as_bytes().len() > MEM_CMP_KEY_INLINE {
                        key.as_bytes().len()
                    } else {
                        0
                    },
                    "fixture entries",
                    "fixture outlined keys",
                    || IndexBuildEntry { key, row_id },
                )
                .unwrap();
            }
            let BuildOutcome::Complete(run) = SortedRun::finish_mutations(
                rows,
                mutation_key_bytes,
                group_id,
                kind,
                unique,
                &AtomicBool::new(false),
            )
            .unwrap() else {
                panic!("fixture unexpectedly stopped")
            };
            if !run.entries().is_empty() {
                runs.push(Arc::new(run), "fixture owners").unwrap();
            }
        }
        runs.reverse();
        SortedRuns::from_admitted(runs, budget, DuplicateCheck::Skip)
    }

    /// Retain real run owners while exercising the production selector.
    pub(super) async fn apply_fixture<F: DiskTreeSpec>(
        tree: &DiskTree<'_, F>,
        file: &mut MutableTableFile,
        runs: &SortedRuns,
        ts: TrxID,
    ) -> RuntimeOrFatalResult<Option<BlockID>> {
        let (_scope, pool) = test_pool(4).await;
        let policy = CheckpointBuildPolicy::new(CheckpointIndexBuildConfig::default(), 4).unwrap();
        let admission = CheckpointAdmission::new(policy)?;
        let mut owners = BudgetedVec::new(&runs.budget);
        for run in runs.runs() {
            owners.push(run.clone(), "fixture retained runs").unwrap();
        }
        let retained = Arc::new(SortedRuns::from_admitted(
            owners,
            runs.budget.clone(),
            DuplicateCheck::Skip,
        ));
        let root = IndexCheckpoint::new(
            tree,
            root_descriptor(tree).await?,
            file,
            retained,
            ts,
            pool,
            admission,
        )
        .apply()
        .await?;
        verify_root(tree, root).await
    }

    /// Purpose: Cover missing, singleton and repeated keys at sorted-run boundaries.
    /// Expected: Borrowed matches preserve every owner in source order and agree with a linear input oracle, including empty runs and outlined keys.
    #[test]
    fn matching_entries_preserve_complete_key_groups() {
        let budget = MemoryBudget::new(1024);
        let BuildOutcome::Complete(empty) = SortedRun::finish_mutations(
            BuildEntries::new(&budget),
            0,
            0,
            RunKind::Put,
            true,
            &AtomicBool::new(false),
        )
        .unwrap() else {
            panic!("fixture unexpectedly stopped")
        };
        assert!(empty.matching_entries(b"key").is_empty());
        for width in [1, 128] {
            let key = |value: u8| {
                let mut bytes = vec![value];
                bytes.resize(width, 7);
                BTreeKey::from(bytes.as_slice())
            };
            for values in [
                vec![2],
                vec![2, 4, 6],
                vec![2, 2],
                vec![2, 2, 4, 6, 6, 6, 8, 10, 10],
                vec![2; 16],
            ] {
                let entries = values
                    .iter()
                    .enumerate()
                    .map(|(position, &value)| (key(value), RowID::new(position as u64 + 1)))
                    .collect();
                let runs = mutation_runs(vec![(RunKind::Put, entries)], true);
                let retained = runs.budget.used();
                for query in 0..12 {
                    let expected: Vec<_> = values
                        .iter()
                        .enumerate()
                        .filter(|(_, value)| **value == query)
                        .map(|(position, _)| RowID::new(position as u64 + 1))
                        .collect();
                    let matches = runs.runs()[0].matching_entries(key(query).as_bytes());
                    let actual: Vec<_> = matches.iter().map(|entry| entry.row_id).collect();
                    assert_eq!(
                        actual, expected,
                        "width={width}, keys={values:?}, query={query}"
                    );
                    assert!(matches.iter().all(|entry| entry.key == key(query)));
                }
                assert_eq!(runs.budget.used(), retained);
            }
        }
    }

    /// Purpose: Probe mixed runs with repeated puts, historical delete owners and absent keys.
    /// Expected: Probes preserve source-order ownership, conditional deletion and exact cross-run multiplicity while charging every inspected delete owner.
    #[test]
    fn probes_preserve_owners_and_sampling_weights() {
        use RunKind::{Delete, Put};
        smol::block_on(test_with_cold(Vec::new(), async |cold| {
            let tree = cold.tree().unwrap();
            let entry = |key: &[u8], row| (BTreeKey::from(key), RowID::new(row));
            let runs = mutation_runs(
                vec![
                    (Put, vec![entry(b"a", 30), entry(b"a", 10)]),
                    (
                        Delete,
                        vec![
                            entry(b"a", 20),
                            entry(b"b", 40),
                            entry(b"b", 50),
                            entry(b"b", 60),
                        ],
                    ),
                    (Put, vec![entry(b"a", 40), entry(b"a", 5), entry(b"c", 70)]),
                    (Delete, vec![entry(b"a", 5), entry(b"c", 70)]),
                ],
                true,
            );
            let scratch =
                MemoryBudget::new(CheckpointIndexBuildConfig::default().assembler_scratch_bytes);
            let mut planner = CheckpointPlanner::new(
                &tree,
                root_descriptor(&tree).await.unwrap(),
                &runs,
                &scratch,
            )
            .unwrap();
            for (key, owner, expected, multiplicity, inspected) in [
                (b"a", Some(30), Some(5), 6, 3),
                (b"a", None, Some(5), 6, 3),
                (b"b", Some(40), None, 3, 4),
                (b"b", Some(50), None, 3, 4),
                (b"b", Some(60), None, 3, 4),
                (b"b", Some(99), Some(99), 3, 4),
                (b"b", None, None, 3, 4),
                (b"c", None, Some(70), 2, 2),
                (b"d", Some(99), Some(99), 0, 1),
                (b"d", None, None, 0, 1),
            ] {
                let before = planner.identity_probes;
                let result = planner.probe(key, owner.map(RowID::new)).unwrap();
                assert_eq!(
                    (
                        result.row,
                        result.multiplicity,
                        result.changed,
                        result.inserted
                    ),
                    (
                        expected.map(RowID::new),
                        multiplicity,
                        expected != owner,
                        owner.is_none() && expected.is_some()
                    ),
                    "key={key:?}, owner={owner:?}"
                );
                assert_eq!(planner.identity_probes - before, inspected);
            }
        }));
    }

    /// Purpose: Exhaust the shared probe allowance while checking a repeated unique DELETE key.
    /// Expected: Only a fully inspected owner group yields a result, even if its first owner matches; incomplete probes stop exactly at the cap.
    #[test]
    fn unique_delete_probes_require_budget_for_every_owner() {
        smol::block_on(test_with_cold(Vec::new(), async |cold| {
            let tree = cold.tree().unwrap();
            let runs = mutation_runs(
                vec![(
                    RunKind::Delete,
                    [10, 20, 30]
                        .map(|row| (BTreeKey::from(&b"key"[..]), RowID::new(row)))
                        .to_vec(),
                )],
                true,
            );
            let first_owner = runs.runs()[0].entries()[0].row_id;
            let scratch =
                MemoryBudget::new(CheckpointIndexBuildConfig::default().assembler_scratch_bytes);
            for remaining in [4, 3, 1, 0] {
                let mut planner = CheckpointPlanner::new(
                    &tree,
                    root_descriptor(&tree).await.unwrap(),
                    &runs,
                    &scratch,
                )
                .unwrap();
                planner.identity_probes = MAX_IDENTITIES - remaining;
                let result = planner.probe(b"key", Some(first_owner));
                assert_eq!(result.is_some(), remaining == 4, "remaining={remaining}");
                if let Some(result) = result {
                    assert_eq!(result.row, None);
                    assert_eq!(result.multiplicity, 3);
                    assert!(result.changed);
                    assert!(!result.inserted);
                }
                assert_eq!(planner.identity_probes, MAX_IDENTITIES);
                assert!(planner.probe(b"absent", None).is_none());
                assert_eq!(planner.identity_probes, MAX_IDENTITIES);
            }
        }));
    }

    /// Purpose: Preserve original unique-put order despite duplicate keys and reversed run arrival.
    /// Expected: Shared streaming merge orders equal keys by source group and stable within-group order, without retaining scratch afterward.
    #[test]
    fn mutation_merge_preserves_stable_source_order() {
        let key = || BTreeKey::from(&b"same"[..]);
        let runs = mutation_runs(
            vec![
                (
                    RunKind::Put,
                    vec![(key(), RowID::new(30)), (key(), RowID::new(10))],
                ),
                (RunKind::Delete, vec![(key(), RowID::new(20))]),
                (
                    RunKind::Put,
                    vec![(key(), RowID::new(40)), (key(), RowID::new(5))],
                ),
            ],
            true,
        );
        let budget = runs.budget.clone();
        let retained = budget.used();
        {
            let mut stream = MutationStream::new(&runs).unwrap();
            let mut rows = Vec::new();
            while let Some(entry) = stream.pop() {
                rows.push(entry.resolve(&runs).row_id);
            }
            assert_eq!(rows, [30, 10, 20, 40, 5].map(RowID::new));
        }
        assert_eq!(budget.used(), retained);
        drop(runs);
        assert_eq!(budget.used(), 0);
    }

    /// Purpose: Fail stable-sort admission before allocating temporary storage.
    /// Expected: The typed memory cause survives and both entries and reservations are released.
    #[test]
    fn stable_sort_admission_releases_input_on_failure() {
        let budget = MemoryBudget::new(4096);
        let mut rows = BuildEntries::new(&budget);
        rows.push_with(0, "fixture entry", "fixture key", || IndexBuildEntry {
            key: BTreeKey::from(&b"key"[..]),
            row_id: RowID::new(1),
        })
        .unwrap();
        fail_build_budget(&budget, "checkpoint stable sort");
        let result =
            SortedRun::finish_mutations(rows, 3, 0, RunKind::Put, true, &AtomicBool::new(false));
        let Err(error) = result else {
            panic!("sort admission must fail")
        };
        let RuntimeOrFatalError::Runtime(error) = error else {
            panic!("expected runtime memory failure")
        };
        assert!(error.contains::<ResourceError>());
        assert_eq!(budget.used(), 0);
    }

    /// Purpose: Protect the exact PUT-volume thresholds without reading or allocating probe state.
    /// Expected: Repeated keys and owner replacements use raw volume, leaf shape stays explicit, and every shortcut uses zero reads and scratch bytes.
    #[test]
    fn put_only_planning_uses_required_totals_without_probes() {
        smol::block_on(test_with_cold(Vec::new(), async |cold| {
            let empty = cold.tree().unwrap();
            let (runtime, _, guard) = empty.snapshot();
            // Deliberately unreadable block: these branches must never perform I/O.
            let block = BlockID::new(999);
            let tree = runtime.open(Some(block), &guard);
            let stats = DiskTreeStats {
                entries: 1000,
                encoded_key_bytes: 4000,
                leaf_nodes: 10,
                branch_nodes: 1,
                height: 1,
            };
            for (count, width, expected) in [
                (0, 4, Strategy::NoWork),
                (1, 4, Strategy::PartialRewrite),
                (999, 4, Strategy::PartialRewrite),
                (1000, 4, Strategy::PartialRewrite),
                (15999, 4, Strategy::PartialRewrite),
                (16000, 4, Strategy::Reconstruct),
                (16001, 4, Strategy::Reconstruct),
                (10, 31983, Strategy::PartialRewrite),
                (10, 31984, Strategy::Reconstruct),
                (10, 31985, Strategy::Reconstruct),
            ] {
                let runs = mutation_runs(
                    vec![(
                        RunKind::Put,
                        (0..count)
                            .map(|row| (BTreeKey::from(vec![7; width].as_slice()), RowID::new(row)))
                            .collect(),
                    )],
                    true,
                );
                let root = SecondaryIndexRoot::from_parts(Some(block), stats).unwrap();
                let scratch = MemoryBudget::new(0);
                let mut planner = CheckpointPlanner::new(&tree, root, &runs, &scratch).unwrap();
                assert_eq!(
                    planner.choose().await.unwrap(),
                    expected,
                    "count={count}, width={width}"
                );
                assert_eq!(planner.node_reads, 0);
                assert_eq!(planner.delta_paths, 0);
                assert!(planner.seen.is_empty() && planner.delta_leaves.is_empty());
                assert_eq!(scratch.used(), 0);
            }
            let runs = mutation_runs(
                vec![(
                    RunKind::Delete,
                    vec![(BTreeKey::from(&b"absent"[..]), RowID::new(1))],
                )],
                true,
            );
            let leaf = SecondaryIndexRoot::from_parts(
                Some(block),
                DiskTreeStats {
                    leaf_nodes: 1,
                    branch_nodes: 0,
                    height: 0,
                    ..stats
                },
            )
            .unwrap();
            let scratch = MemoryBudget::new(0);
            let mut planner = CheckpointPlanner::new(&tree, leaf, &runs, &scratch).unwrap();
            assert_eq!(planner.choose().await.unwrap(), Strategy::Reconstruct);
            assert_eq!(planner.node_reads, 0);
            assert!(CheckpointPlanner::<UniqueDiskTreeSpec>::mass(u64::MAX, 0).is_err());
        }));
    }

    /// Purpose: Preserve repeated quantile contributions while caching complete-key deletion probes.
    /// Expected: One repeated effective delete estimates exactly one removal and one changed key; no-effect and identity-cap exhaustion conservatively retain partial work.
    #[test]
    fn deletion_planning_weights_cached_groups_and_bounds_inspection() {
        let entries = (0u32..6000)
            .map(|key| IndexBuildEntry {
                key: BTreeKey::from(key.to_be_bytes().as_slice()),
                row_id: RowID::new(u64::from(key) + 1),
            })
            .collect();
        smol::block_on(test_with_cold(entries, async |cold| {
            let tree = cold.tree().unwrap();
            let root = root_descriptor(&tree).await.unwrap();
            assert!(root.stats().height > 0);
            for (count, owner, changed) in [(200, 1, true), (200, 99999, false), (4096, 1, false)] {
                let runs = mutation_runs(
                    vec![(
                        RunKind::Delete,
                        (0..count)
                            .map(|_| {
                                (
                                    BTreeKey::from(0u32.to_be_bytes().as_slice()),
                                    RowID::new(owner),
                                )
                            })
                            .collect(),
                    )],
                    true,
                );
                let scratch = MemoryBudget::new(128 * 1024);
                let mut planner = CheckpointPlanner::new(&tree, root, &runs, &scratch).unwrap();
                assert_eq!(planner.choose().await.unwrap(), Strategy::PartialRewrite);
                assert!(planner.delta_paths <= 64);
                assert!(planner.identity_probes <= MAX_IDENTITIES);
                if count == 4096 {
                    assert!(planner.uncertain);
                    assert_eq!(planner.identity_probes, MAX_IDENTITIES);
                    assert_eq!(planner.seen.len(), 0);
                } else {
                    assert_eq!(planner.seen.len(), 1);
                    assert_eq!(planner.node_reads, usize::from(root.stats().height) + 1);
                    assert_eq!(planner.identity_probes, count + 1);
                    assert_eq!(planner.changed_keys, usize::from(changed));
                    assert_eq!(
                        planner.removed_sum / planner.delta_paths as f64,
                        f64::from(changed)
                    );
                    assert_eq!(
                        planner.removed_bytes / planner.delta_paths as f64,
                        if changed { 4.0 } else { 0.0 }
                    );
                }
            }
        }));
    }
}
