//! Checkpoint strategy selection and application against a captured secondary root.
//! The bounded planner lives here; physical execution belongs to `checkpoint_builder`.
use super::checkpoint_builder::BuildMode;
use super::co_rank;
use super::merge::{HotEntryRef, execution_error};
use super::{BudgetedVec, BuildOutcome, MemoryBudget, RunKind, SortedRuns};
use crate::error::{RuntimeError, RuntimeOrFatalResult};
use crate::file::table_file::MutableTableFile;
use crate::id::{BlockID, RowID, TrxID};
use crate::index::disk_tree::{DiskTree, DiskTreeNodeGuard, DiskTreeSpec};
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
// Calibration retains these block-work weights and a 10% retention margin;
// task 000332 records the matched production cases and forced-strategy oracles.
const READ: f64 = 1.0;
const PACK: f64 = 2.0;
const WRITE: f64 = 4.0;
const RETAIN_MARGIN: f64 = 1.10;
const INITIAL_PATHS: usize = 16;
const MAX_PATHS: usize = 64;
const MAX_IDENTITIES: usize = 4096;

/// Inputs for one secondary-index checkpoint against a captured root.
/// Application consumes this request and returns a candidate root for the
/// enclosing table checkpoint to publish after its remaining work succeeds.
pub(crate) struct IndexCheckpoint<'a, 'tree, F: DiskTreeSpec> {
    /// Captured tree used by planning and physical reads.
    pub(super) tree: &'a DiskTree<'tree, F>,
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
        file: &'a mut MutableTableFile,
        runs: Arc<SortedRuns>,
        ts: TrxID,
        pool: QuiescentGuard<ThreadPool>,
        admission: Arc<CheckpointAdmission>,
    ) -> Self {
        Self {
            tree,
            file,
            runs,
            ts,
            pool,
            admission,
        }
    }

    /// Select and apply the physical checkpoint strategy against the captured root.
    pub(crate) async fn apply(self) -> RuntimeOrFatalResult<Option<BlockID>> {
        #[cfg(feature = "profiling")]
        let started = Instant::now();
        let strategy = {
            let scratch = self.admission.assembler.clone();
            let mut planner = CheckpointPlanner::new(self.tree, &self.runs, &scratch);
            let strategy = planner.choose().await?;
            #[cfg(feature = "profiling")]
            planner.record(strategy, started, &self.admission);
            strategy
        };
        let mode = match strategy {
            Strategy::NoWork => return Ok(self.tree.snapshot().1),
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
    row: Option<RowID>,
    multiplicity: usize,
    changed: bool,
    inserted: bool,
}

/// Inputs, bounded sampling state and costs for one strategy selection.
/// Estimated costs affect work selection only; they grant no content authority.
struct CheckpointPlanner<'a, 'tree, F: DiskTreeSpec> {
    tree: &'a DiskTree<'tree, F>,
    runs: &'a SortedRuns,
    scratch: &'a MemoryBudget,
    mutation_count: usize,
    exact_entries: Option<usize>,
    base_height: usize,
    node_reads: usize,
    base_paths: usize,
    delta_paths: usize,
    identity_probes: usize,
    seen: BudgetedVec<(HotEntryRef, Probe)>,
    delta_leaves: BudgetedVec<BlockID>,
    leaf_sum: f64,
    entry_sum: f64,
    surviving_sum: f64,
    touched_sum: f64,
    growth_sum: f64,
    effective_sum: f64,
    uncertain: bool,
    reconstruct_cost: f64,
    partial_cost: f64,
}

impl<'a, 'tree, F: DiskTreeSpec> CheckpointPlanner<'a, 'tree, F> {
    /// Initialize selection state without allocating sampling caches.
    fn new(tree: &'a DiskTree<'tree, F>, runs: &'a SortedRuns, scratch: &'a MemoryBudget) -> Self {
        Self {
            tree,
            runs,
            scratch,
            mutation_count: runs.runs().iter().map(|run| run.entries().len()).sum(),
            exact_entries: None,
            base_height: 0,
            node_reads: 0,
            base_paths: 0,
            delta_paths: 0,
            identity_probes: 0,
            seen: BudgetedVec::new(scratch),
            delta_leaves: BudgetedVec::new(scratch),
            leaf_sum: 0.0,
            entry_sum: 0.0,
            surviving_sum: 0.0,
            touched_sum: 0.0,
            growth_sum: 0.0,
            effective_sum: 0.0,
            uncertain: false,
            reconstruct_cost: 0.0,
            partial_cost: 0.0,
        }
    }

    /// Select at 16 samples, refining inconclusive estimates to 64 before falling back.
    async fn choose(&mut self) -> RuntimeOrFatalResult<Strategy> {
        if self.mutation_count == 0 {
            return Ok(Strategy::NoWork);
        }
        let Some(root) = self.tree.snapshot().1 else {
            return Ok(Strategy::Reconstruct);
        };
        let guard = self.tree.read_node(root).await?;
        self.base_height = guard.view().height();
        self.node_reads = 1;
        let leaf_root = guard.view().is_leaf();
        if !leaf_root && self.mutation_count < 128 {
            return Ok(Strategy::PartialRewrite);
        }
        self.exact_entries = leaf_root.then(|| guard.view().count());
        drop(guard);
        // Admit bounded caches only after shortcuts and release of the root guard.
        self.seen
            .ensure_capacity(MAX_PATHS, "checkpoint probe keys")
            .change_context(RuntimeError::CheckpointExecution)?;
        self.delta_leaves
            .ensure_capacity(MAX_PATHS, "checkpoint probe leaves")
            .change_context(RuntimeError::CheckpointExecution)?;
        for target in [INITIAL_PATHS, MAX_PATHS] {
            self.sample_until(root, target).await?;
            self.update_costs();
            if let Some(strategy) = self.decision() {
                return Ok(strategy);
            }
            if self.identity_probes == MAX_IDENTITIES {
                break;
            }
        }
        // Unresolved estimates deliberately preserve sparse locality.
        Ok(Strategy::PartialRewrite)
    }

    /// Retain sample order and inspect a root leaf only once across both stages.
    async fn sample_until(&mut self, root: BlockID, target: usize) -> RuntimeOrFatalResult<()> {
        // Bit reversal spreads the initial 16 positions across the traversal interval.
        // Refinement fills the gaps up to 64 positions; paths may reach the same leaf.
        for sample in self.delta_paths..target {
            if self.exact_entries.is_none() || sample == 0 {
                self.sample_base(root, sample).await?;
            }
            self.sample_delta(root, sample).await?;
        }
        Ok(())
    }

    /// Accumulate weighted base size, surviving entries and mutation overlap.
    async fn sample_base(&mut self, root: BlockID, sample: usize) -> RuntimeOrFatalResult<()> {
        let stratum = (sample as u8).reverse_bits() as usize >> 2;
        let (guard, weight) = self.sampled_leaf(root, stratum).await?;
        let node = guard.view();
        let slots = if self.exact_entries.is_some() {
            node.count()
        } else {
            node.count().min(16)
        };
        let mut surviving = 0;
        let mut effective = false;
        for n in 0..slots {
            let slot = n * node.count() / slots;
            let entry = node.leaf_entry(slot)?;
            let row = F::entry_row(&entry).change_context(RuntimeError::CheckpointExecution)?;
            let _key_admission = self
                .scratch
                .reserve(entry.key.len(), "checkpoint probe key")
                .change_context(RuntimeError::CheckpointExecution)?;
            let key = entry.key.to_vec();
            match self.probe(&key, Some(row)) {
                Some(result) => {
                    surviving += usize::from(result.row.is_some());
                    effective |= result.changed;
                }
                None => {
                    self.uncertain = true;
                    surviving += 1;
                }
            }
        }
        // A range can contain new puts even when none of its old owners move.
        let _fences = self
            .scratch
            .reserve(
                node.fence_key(false).len() + node.fence_key(true).len(),
                "checkpoint probe fences",
            )
            .change_context(RuntimeError::CheckpointExecution)?;
        let lower = node.fence_key(false).to_vec();
        let upper = (!node.has_no_upper_fence()).then(|| node.fence_key(true).to_vec());
        let touched = effective
            || self.runs.runs().iter().any(|run| {
                let at = run
                    .entries()
                    .partition_point(|entry| entry.key.as_bytes() < lower.as_slice());
                run.entries().get(at).is_some_and(|entry| {
                    upper
                        .as_ref()
                        .is_none_or(|upper| entry.key.as_bytes() < upper.as_slice())
                })
            });
        self.leaf_sum += weight;
        self.entry_sum += weight * node.count() as f64;
        self.surviving_sum += weight
            * if slots == 0 {
                0.0
            } else {
                node.count() as f64 * surviving as f64 / slots as f64
            };
        self.touched_sum += if touched { weight } else { 0.0 };
        self.base_paths += 1;
        Ok(())
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
                if !self.delta_leaves.contains(&leaf) {
                    self.delta_leaves.push_reserved(leaf);
                }
                let result = self.probe(key, owner);
                if let Some(result) = result {
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
                        self.growth_sum += weight;
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

    /// Convert accumulated observations into costs, retaining cardinalities only for profiling.
    fn update_costs(&mut self) {
        let paths = self.delta_paths;
        let base_leaves = self.leaf_sum / self.base_paths as f64;
        let base_entries = self
            .exact_entries
            .map_or(self.entry_sum / self.base_paths as f64, |count| {
                count as f64
            });
        let surviving_entries = self.surviving_sum / self.base_paths as f64;
        let growth_entries = self.growth_sum / paths as f64;
        let touched = self.touched_sum / self.base_paths as f64;
        let touched_leaves = touched.max(self.delta_leaves.len() as f64).min(base_leaves);
        let average_entries = (base_entries / base_leaves.max(1.0)).max(1.0);
        let output = (surviving_entries + growth_entries) / average_entries;
        let growth_blocks = growth_entries / average_entries;
        let touched_output = (output - (base_leaves - touched_leaves)).max(0.0);
        let parents = (self.base_height as f64 * touched_leaves.sqrt()).min(base_leaves);
        let probing = self.node_reads as f64 * READ / 16.0;
        self.reconstruct_cost = probing + base_leaves * READ + output * (PACK + WRITE);
        self.partial_cost = probing
            + (touched_leaves + parents) * READ
            + (touched_output + parents + growth_blocks.sqrt()) * (PACK + WRITE);
    }

    /// Return an exact shortcut or sufficiently separated cost choice at a stage boundary.
    fn decision(&self) -> Option<Strategy> {
        let paths = self.delta_paths;
        if self.exact_entries.is_some()
            && self.mutation_count <= paths
            && self.effective_sum == 0.0
            && !self.uncertain
        {
            return Some(Strategy::NoWork);
        }
        // Leaf roots have exact base geometry; growth creates independent key
        // ranges even when the old tree supplied only one physical leaf.
        if self.exact_entries.is_some() && self.effective_sum > 0.0 && !self.uncertain {
            return Some(Strategy::Reconstruct);
        }
        let ratio = self.partial_cost / self.reconstruct_cost.max(1.0);
        if !self.uncertain && (ratio > 1.35 || ratio < 0.75 || paths == MAX_PATHS) {
            if ratio > RETAIN_MARGIN {
                return Some(Strategy::Reconstruct);
            }
            return Some(Strategy::PartialRewrite);
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
            row,
            multiplicity,
            changed: row != owner,
            inserted: owner.is_none() && row.is_some(),
        })
    }

    /// Descend one stratified position and retain its reciprocal path-probability weight.
    async fn sampled_leaf(
        &mut self,
        mut block: BlockID,
        numerator: usize,
    ) -> RuntimeOrFatalResult<(DiskTreeNodeGuard<F>, f64)> {
        let mut fraction = (numerator as f64 + 0.5) / MAX_PATHS as f64;
        let mut weight = 1.0;
        let mut expected = None;
        loop {
            self.node_reads += 1;
            let guard = self.tree.read_node(block).await?;
            let node = guard.view();
            if expected.is_some_and(|height| node.height() != height) {
                return Err(execution_error("checkpoint probe child height mismatch"));
            }
            if node.is_leaf() {
                return Ok((guard, weight));
            }
            let fanout = node.count() + 1;
            let scaled = fraction * fanout as f64;
            let child = (scaled as usize).min(fanout - 1);
            fraction = scaled - child as f64;
            weight *= fanout as f64;
            expected = Some(node.height() - 1);
            block = node.child_block(child)?;
        }
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
    use crate::index::disk_tree::{NonUniqueDiskTree, UniqueDiskTree};
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
        let mut planner = CheckpointPlanner::new(tree, runs, &scratch);
        let strategy = planner.choose().await.unwrap();
        assert!(planner.base_paths <= 64 && planner.delta_paths <= 64);
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
        let root = IndexCheckpoint::new(tree, file, runs, ts, pool, admission.clone())
            .build(BuildMode::PartialRewrite)
            .await
            .unwrap();
        (root, rewrite_work(&admission))
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
        IndexCheckpoint::new(tree, file, runs, ts, pool, admission)
            .build(BuildMode::Reconstruct)
            .await
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
        IndexCheckpoint::new(tree, file, runs, ts, pool, admission)
            .build(BuildMode::PartialRewrite)
            .await
    }

    /// Construct real checkpoint runs in reverse arrival order for storage fixtures.
    pub(crate) fn mutation_runs(
        groups: Vec<(RunKind, Vec<(BTreeKey, RowID)>)>,
        unique: bool,
    ) -> SortedRuns {
        let budget = MemoryBudget::new(64 * 1024 * 1024);
        let mut runs = BudgetedVec::new(&budget);
        for (group_id, (kind, entries)) in groups.into_iter().enumerate() {
            let mut rows = BuildEntries::new(&budget);
            for (key, row_id) in entries {
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
            let BuildOutcome::Complete(run) =
                SortedRun::finish_mutations(rows, group_id, kind, unique, &AtomicBool::new(false))
                    .unwrap()
            else {
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
        IndexCheckpoint::new(tree, file, retained, ts, pool, admission)
            .apply()
            .await
    }

    /// Purpose: Cover missing, singleton and repeated keys at sorted-run boundaries.
    /// Expected: Borrowed matches preserve every owner in source order and agree with a linear input oracle, including empty runs and outlined keys.
    #[test]
    fn matching_entries_preserve_complete_key_groups() {
        let budget = MemoryBudget::new(1024);
        let BuildOutcome::Complete(empty) = SortedRun::finish_mutations(
            BuildEntries::new(&budget),
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
            let mut planner = CheckpointPlanner::new(&tree, &runs, &scratch);
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
                let mut planner = CheckpointPlanner::new(&tree, &runs, &scratch);
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
            SortedRun::finish_mutations(rows, 0, RunKind::Put, true, &AtomicBool::new(false));
        let Err(error) = result else {
            panic!("sort admission must fail")
        };
        let RuntimeOrFatalError::Runtime(error) = error else {
            panic!("expected runtime memory failure")
        };
        assert!(error.contains::<ResourceError>());
        assert_eq!(budget.used(), 0);
    }
}
