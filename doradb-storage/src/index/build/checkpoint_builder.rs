//! Physical checkpoint construction: range reconciliation, subtree reuse and root assembly.
//! Strategy selection is supplied by the caller; this module owns admitted execution
//! and settles every accepted worker and write before returning a candidate root.
use super::checkpoint::IndexCheckpoint;
use super::disk_builder::{ColdBuildPolicy, minimum_progress_bytes};
use super::merge::{
    CompletedPartition, HotEntryRef, MergeAcknowledgement, MergeConsumption, MergePreparation,
    PartitionConsumer, PartitionMergeStream, PreparedMerge, execution_error,
};
use super::packing::{max_node_slots, pack_admitted_node, plan_candidates, plan_parent};
use super::{
    BudgetedEntries, BudgetedVec, BuildOutcome, MemoryBudget, MemoryReservation,
    PAYLOAD_CREDIT_BYTES, PayloadSize, RunKind, SortedRuns,
};
use crate::buffer::PoolGuard;
use crate::completion::Completion;
use crate::error::{MultiDomainResultExt, RuntimeError, RuntimeOrFatalError, RuntimeOrFatalResult};
use crate::file::cow_file::MutableCowFile;
use crate::file::table_file::MutableTableFile;
use crate::id::{BlockID, RowID, TrxID};
use crate::index::BTreeKey;
use crate::index::btree::algo::{KnownFenceNodeParams, PackedNodeEntry, PackedNodePlanParams};
use crate::index::btree::{BTreeEntryView, BTreeKeyView};
use crate::index::btree::{BTreeNode, BTreeU64};
use crate::index::disk_tree::DiskTreeNodeView;
use crate::index::disk_tree::{
    DISK_TREE_BLOCK_SIZE, DiskTree, DiskTreeNodeGuard, DiskTreeRuntime, DiskTreeSpec, LogicalEntry,
};
use crate::index::util::Maskable;
use crate::io::DirectBuf;
use crate::layout;
use crate::memcmp::MEM_CMP_KEY_INLINE;
use crate::runtime::yield_now;
use error_stack::ResultExt;
use futures::{FutureExt, future::BoxFuture};
use parking_lot::Mutex;
use std::marker::PhantomData;
use std::sync::Arc;

#[cfg(feature = "profiling")]
use crate::profiling::{CheckpointIndexMeasurements, clock::Instant};

#[cfg(test)]
pub(crate) use tests::{RewriteWork, rewrite_work};

/// Provisional state retained by each active or unassembled checkpoint job.
const JOB_BYTES: usize = 4 * 1024 * 1024;

/// Ordered assembly, final-fence repair and bounded planning storage.
pub(super) const ASSEMBLER_BYTES: usize = 12 * 1024 * 1024;

// Optional partial-rewrite absorption cannot revisit an ever-growing prefix.
const OPTIONAL_LEAF_CANDIDATES: usize = 8;

/// Physical execution mode selected before any replacement image is emitted.
#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum BuildMode {
    /// Repack all captured base entries together with the selected mutations.
    Reconstruct,
    /// Preserve unaffected subtrees while repacking changed ranges.
    PartialRewrite,
}

/// Invocation-wide progress reserved before checkpoint sources or keys grow.
/// The fixed frontier admission outlives every sequential index consumer.
pub(crate) struct CheckpointAdmission {
    policy: ColdBuildPolicy,
    budget: MemoryBudget,
    _frontier: MemoryReservation,
    output: Mutex<MemoryReservation>,
    #[cfg(feature = "profiling")]
    measurements: Mutex<CheckpointIndexMeasurements>,
    #[cfg(test)]
    rewrite_work: Mutex<RewriteWork>,
}

impl CheckpointAdmission {
    /// Protect one job, ordered assembly and the minimum durable output pipeline.
    pub(crate) fn new(
        budget: &MemoryBudget,
        policy: ColdBuildPolicy,
    ) -> RuntimeOrFatalResult<Arc<Self>> {
        let frontier = budget
            .reserve(
                ASSEMBLER_BYTES + JOB_BYTES,
                "checkpoint provisional progress",
            )
            .change_context(RuntimeError::CheckpointExecution)?;
        let bytes =
            minimum_progress_bytes(true, policy.max_ready_buffers, policy.max_in_flight_writes)
                .zip(minimum_progress_bytes(
                    false,
                    policy.max_ready_buffers,
                    policy.max_in_flight_writes,
                ))
                .map(|(a, b)| a.max(b))
                .ok_or_else(|| execution_error("checkpoint progress capacity overflow"))?;
        let output = budget
            .reserve(bytes, "checkpoint minimum progress")
            .change_context(RuntimeError::CheckpointExecution)?;
        Ok(Arc::new(Self {
            policy,
            budget: budget.clone(),
            _frontier: frontier,
            output: Mutex::new(output),
            #[cfg(feature = "profiling")]
            measurements: Mutex::new(CheckpointIndexMeasurements::default()),
            #[cfg(test)]
            rewrite_work: Mutex::new(RewriteWork::default()),
        }))
    }

    /// Accumulate private measurements for the public checkpoint's success boundary.
    #[cfg(feature = "profiling")]
    pub(crate) fn measure(&self, update: impl FnOnce(&mut CheckpointIndexMeasurements)) {
        update(&mut self.measurements.lock());
    }

    /// Snapshot after successful publication and complete child settlement.
    #[cfg(feature = "profiling")]
    pub(crate) fn measurements(&self) -> CheckpointIndexMeasurements {
        let mut sample = *self.measurements.lock();
        sample.max_scratch_bytes = self.budget.peak() as u64;
        sample
    }

    fn output(
        &self,
        bytes: usize,
        purpose: &'static str,
    ) -> RuntimeOrFatalResult<MemoryReservation> {
        Ok(self
            .output
            .lock()
            .take(bytes, purpose)
            .change_context(RuntimeError::CheckpointExecution)?)
    }
}

enum BaseEvent {
    EntryReady,
    LeafFinished { leaf: Descriptor, complete: bool },
    SubtreeReused(Descriptor),
    // Either the old range is exhausted or the next mutation belongs to a gap
    // after this leaf's last entry. The mutation stream decides whether to stop.
    NoEntry,
}

struct ReadRange<'a> {
    lower: Option<&'a [u8]>,
    upper: Option<&'a [u8]>,
    mutation: Option<&'a [u8]>,
    reuse: bool,
    budget: &'a MemoryBudget,
}

struct Frame {
    block: BlockID,
    height: usize,
    next: usize,
    end: usize,
}

/// Height-bounded old-root reader. Each reader pins only its current leaf; branch
/// continuation coordinates survive the read without pinning the whole path.
struct BaseReader<F: DiskTreeSpec> {
    runtime: DiskTreeRuntime<F>,
    root: Option<BlockID>,
    guard: PoolGuard,
    path: BudgetedVec<Frame>,
    leaf: Option<DiskTreeNodeGuard<F>>,
    slot: usize,
    pending: Option<BlockID>,
    leaf_complete: bool,
    exhausted: bool,
    #[cfg(feature = "profiling")]
    reads: u64,
}

impl<F: DiskTreeSpec> BaseReader<F> {
    fn new(tree: &DiskTree<'_, F>, budget: &MemoryBudget) -> Self {
        let (runtime, root, guard) = tree.snapshot();
        Self {
            runtime,
            root,
            guard,
            path: BudgetedVec::new(budget),
            leaf: None,
            slot: 0,
            pending: root,
            leaf_complete: false,
            exhausted: false,
            #[cfg(feature = "profiling")]
            reads: 0,
        }
    }

    async fn descend(
        &mut self,
        mut block: BlockID,
        mut height: Option<usize>,
        key: &[u8],
        range: &ReadRange<'_>,
    ) -> RuntimeOrFatalResult<Option<Descriptor>> {
        loop {
            #[cfg(feature = "profiling")]
            {
                self.reads += 1;
            }
            let guard = self
                .runtime
                .open(self.root, &self.guard)
                .read_node(block)
                .await?;
            let node = guard.view();
            if height.is_some_and(|height| height != node.height()) {
                return Err(execution_error("checkpoint base child height mismatch"));
            }
            if range
                .upper
                .is_some_and(|upper| !node.fence_key(false).cmp_bytes(upper).is_lt())
            {
                self.exhausted = true;
                self.path.clear();
                return Ok(None);
            }
            let complete = range
                .lower
                .is_none_or(|lower| !node.fence_key(false).cmp_bytes(lower).is_lt())
                && range.upper.is_none_or(|upper| {
                    !node.has_no_upper_fence() && !node.fence_key(true).cmp_bytes(upper).is_gt()
                });
            let untouched = range.mutation.is_none_or(|mutation| {
                !node.has_no_upper_fence() && !node.fence_key(true).cmp_bytes(mutation).is_gt()
            });
            if range.reuse && complete && untouched {
                return Ok(Some(Descriptor {
                    promotion: None,
                    reused: true,
                    block,
                    lower: OwnedKey::view(node.fence_key(false), range.budget)?,
                    upper: if node.has_no_upper_fence() {
                        None
                    } else {
                        Some(OwnedKey::view(node.fence_key(true), range.budget)?)
                    },
                    height: node.height() as u16,
                }));
            }
            if node.is_leaf() {
                self.leaf_complete = complete;
                self.slot = node.lower_bound_slot_idx(key)?;
                self.leaf = Some(guard);
                return Ok(None);
            }
            let child = node.lower_bound_child_entry_idx(key)?;
            let next = node.child_block(child)?;
            self.path
                .push(
                    Frame {
                        block,
                        height: node.height(),
                        next: child + 1,
                        end: node.count() + 1,
                    },
                    "checkpoint base path",
                )
                .change_context(RuntimeError::CheckpointExecution)?;
            if let Some(reused) = self.untouched_child(node, child, range)? {
                return Ok(Some(reused));
            }
            height = Some(node.height() - 1);
            block = next;
        }
    }

    async fn next_event(&mut self, range: &ReadRange<'_>) -> RuntimeOrFatalResult<BaseEvent> {
        if self.exhausted {
            return Ok(BaseEvent::NoEntry);
        }
        if let Some(root) = self.pending.take()
            && let Some(reused) = self
                .descend(root, None, range.lower.unwrap_or(&[]), range)
                .await?
        {
            return Ok(BaseEvent::SubtreeReused(reused));
        }
        loop {
            if self.exhausted {
                return Ok(BaseEvent::NoEntry);
            }
            if let Some(leaf) = &self.leaf {
                let node = leaf.view();
                let before_upper = self.slot < node.count()
                    && match range.upper {
                        Some(key) => node.key(self.slot)?.cmp_bytes(key).is_lt(),
                        None => true,
                    };
                if before_upper {
                    return Ok(BaseEvent::EntryReady);
                }
                // New identities between the last old entry and this leaf's
                // upper fence still belong to this same original attachment.
                if range.mutation.is_some_and(|key| {
                    range.upper.is_none_or(|upper| key < upper)
                        && (node.has_no_upper_fence()
                            || node.fence_key(true).cmp_bytes(key).is_gt())
                }) {
                    return Ok(BaseEvent::NoEntry);
                }
                if range.reuse {
                    let finished = BaseEvent::LeafFinished {
                        leaf: Descriptor {
                            promotion: None,
                            reused: true,
                            block: leaf.block_id(),
                            lower: OwnedKey::view(node.fence_key(false), range.budget)?,
                            upper: if node.has_no_upper_fence() {
                                None
                            } else {
                                Some(OwnedKey::view(node.fence_key(true), range.budget)?)
                            },
                            height: 0,
                        },
                        complete: self.leaf_complete,
                    };
                    if range.upper.is_some_and(|upper| {
                        node.has_no_upper_fence() || node.fence_key(true).cmp_bytes(upper).is_ge()
                    }) {
                        self.exhausted = true;
                        self.path.clear();
                    }
                    self.leaf = None;
                    return Ok(finished);
                }
            }
            self.leaf = None;
            let Some(mut frame) = self.path.pop() else {
                return Ok(BaseEvent::NoEntry);
            };
            if frame.next == frame.end {
                continue;
            }
            #[cfg(feature = "profiling")]
            {
                self.reads += 1;
            }
            let guard = self
                .runtime
                .open(self.root, &self.guard)
                .read_node(frame.block)
                .await?;
            if guard.view().height() != frame.height {
                return Err(execution_error("checkpoint base path changed height"));
            }
            let block = guard.view().child_block(frame.next)?;
            let reused = self.untouched_child(guard.view(), frame.next, range)?;
            frame.next += 1;
            let height = frame.height - 1;
            self.path.push_reserved(frame);
            drop(guard);
            if let Some(reused) = reused {
                return Ok(BaseEvent::SubtreeReused(reused));
            }
            if let Some(reused) = self.descend(block, Some(height), &[], range).await? {
                return Ok(BaseEvent::SubtreeReused(reused));
            }
        }
    }

    fn untouched_child(
        &self,
        node: DiskTreeNodeView<'_, F>,
        child: usize,
        range: &ReadRange<'_>,
    ) -> RuntimeOrFatalResult<Option<Descriptor>> {
        if !range.reuse {
            return Ok(None);
        }
        let lower = if child == 0 {
            node.fence_key(false)
        } else {
            node.key(child - 1)?
        };
        let upper = if child < node.count() {
            Some(node.key(child)?)
        } else if node.has_no_upper_fence() {
            None
        } else {
            Some(node.fence_key(true))
        };
        let complete = range.lower.is_none_or(|key| !lower.cmp_bytes(key).is_lt())
            && range
                .upper
                .is_none_or(|key| upper.is_some_and(|upper| !upper.cmp_bytes(key).is_gt()));
        let untouched = range
            .mutation
            .is_none_or(|key| upper.is_some_and(|upper| !upper.cmp_bytes(key).is_gt()));
        if !complete || !untouched {
            return Ok(None);
        }
        Ok(Some(Descriptor {
            promotion: None,
            reused: true,
            block: node.child_block(child)?,
            lower: OwnedKey::view(lower, range.budget)?,
            upper: upper
                .map(|key| OwnedKey::view(key, range.budget))
                .transpose()?,
            height: node.child_height()?,
        }))
    }

    #[inline]
    fn needs_traversal(&self) -> bool {
        self.leaf
            .as_ref()
            .is_none_or(|leaf| self.slot >= leaf.view().count())
    }

    #[inline]
    fn entry(&self) -> RuntimeOrFatalResult<BTreeEntryView<'_, F::LeafValue>> {
        let leaf = self
            .leaf
            .as_ref()
            .ok_or_else(|| execution_error("checkpoint base reader has no current leaf"))?;
        Ok(leaf.view().leaf_entry(self.slot)?)
    }
}

struct OwnedKey {
    key: BTreeKey,
    _allocation: MemoryReservation,
}

impl OwnedKey {
    #[inline]
    fn admit(len: usize, budget: &MemoryBudget) -> RuntimeOrFatalResult<MemoryReservation> {
        budget
            .reserve(
                size_of::<Self>()
                    + 2 * size_of::<usize>()
                    + if len > MEM_CMP_KEY_INLINE { len } else { 0 },
                "checkpoint retained key",
            )
            .change_context(RuntimeError::CheckpointExecution)
            .map_err(Into::into)
    }

    fn copy(key: &[u8], budget: &MemoryBudget) -> RuntimeOrFatalResult<Arc<Self>> {
        let allocation = Self::admit(key.len(), budget)?;
        Ok(Arc::new(Self {
            key: BTreeKey::from(key),
            _allocation: allocation,
        }))
    }

    fn view(key: BTreeKeyView<'_>, budget: &MemoryBudget) -> RuntimeOrFatalResult<Arc<Self>> {
        let allocation = Self::admit(key.len(), budget)?;
        Ok(Arc::new(Self {
            key: key.to_btree_key(),
            _allocation: allocation,
        }))
    }

    #[inline]
    fn reuse_or_copy(
        key: &[u8],
        previous: Option<&Arc<Self>>,
        budget: &MemoryBudget,
    ) -> RuntimeOrFatalResult<Arc<Self>> {
        if let Some(previous) = previous.filter(|previous| previous.bytes() == key) {
            return Ok(previous.clone());
        }
        Self::copy(key, budget)
    }

    #[inline]
    fn bytes(&self) -> &[u8] {
        self.key.as_bytes()
    }
}

enum EntryKey {
    Delta(HotEntryRef),
    Base(BTreeKey),
}

struct Entry {
    key: EntryKey,
    row: RowID,
}

impl PayloadSize for Entry {
    #[inline]
    fn payload_bytes(&self) -> usize {
        match &self.key {
            EntryKey::Base(key) if key.as_bytes().len() > MEM_CMP_KEY_INLINE => {
                key.as_bytes().len()
            }
            _ => 0,
        }
    }
}

impl Entry {
    #[inline]
    fn key<'a>(&'a self, runs: &'a SortedRuns) -> &'a [u8] {
        match &self.key {
            EntryKey::Delta(reference) => reference.resolve(runs).key.as_bytes(),
            EntryKey::Base(key) => key.as_bytes(),
        }
    }

    #[inline]
    fn value<F: DiskTreeSpec>(&self) -> F::LeafValue {
        F::leaf_value(&LogicalEntry {
            key: Vec::new(),
            row_id: F::UNIQUE.then_some(self.row),
        })
    }
}

struct Image {
    promotion: Option<(BlockID, u16, bool)>,
    buf: DirectBuf,
    allocation: MemoryReservation,
    lower: Arc<OwnedKey>,
    upper: Option<Arc<OwnedKey>>,
    height: u16,
    count: usize,
}

impl Image {
    fn from_children(
        children: &mut BudgetedVec<Descriptor>,
        scratch: &MemoryBudget,
        admission: &CheckpointAdmission,
        ts: TrxID,
    ) -> RuntimeOrFatalResult<Self> {
        let first = &children[0];
        let mut slots = BudgetedVec::new(scratch);
        let last_upper = children.last().and_then(|child| child.upper.as_ref());
        let plan = plan_parent(
            &mut slots,
            children.len(),
            last_upper.map(|key| key.bytes()),
            "checkpoint parent slots",
            |index| {
                let child = &children[index];
                (child.lower.bytes(), child.block)
            },
        )?;
        let count = plan.count;
        let upper = children
            .get(count)
            .map(|child| child.lower.clone())
            .or_else(|| last_upper.cloned());
        let height = first
            .height
            .checked_add(1)
            .ok_or_else(|| execution_error("checkpoint tree height overflow"))?;
        let allocation = admission.output(DISK_TREE_BLOCK_SIZE, "checkpoint output image")?;
        let image = Image {
            promotion: (count == 1).then(|| {
                first
                    .promotion
                    .unwrap_or((first.block, first.height, first.reused))
            }),
            buf: pack_admitted_node(
                plan.node_params(height, ts, upper.as_ref().map(|key| key.bytes())),
                &slots[..count - 1],
            )?,
            allocation,
            lower: first.lower.clone(),
            upper,
            height,
            count: count - 1,
        };
        drop(slots);
        consume_prefix(children, count);
        Ok(image)
    }
}

enum Output {
    Image(Image),
    // Either a captured subtree or a stable image already submitted to Writer.
    // Descriptor::reused distinguishes which one may still need fence repair.
    Subtree(Descriptor),
}

struct RangeSummary {
    #[cfg(feature = "profiling")]
    measurements: CheckpointIndexMeasurements,
    packets: usize,
    changed: bool,
}

struct Packet {
    plan: Arc<PreparedMerge>,
    partition: usize,
    ordinal: usize,
    output: Output,
    stable: bool,
    ack: flume::Sender<()>,
}

struct JobOutput<'a> {
    sender: &'a flume::Sender<Packet>,
    plan: Arc<PreparedMerge>,
    partition: usize,
    next: usize,
    pending: Option<(usize, Output)>,
}

impl JobOutput<'_> {
    #[inline]
    fn upper(&self) -> Option<&Arc<OwnedKey>> {
        match &self.pending.as_ref()?.1 {
            Output::Image(image) => image.upper.as_ref(),
            Output::Subtree(child) => child.upper.as_ref(),
        }
    }

    async fn stage(&mut self, output: Output) -> RuntimeOrFatalResult<()> {
        let lower = match &output {
            Output::Image(image) => image.lower.bytes(),
            Output::Subtree(child) => child.lower.bytes(),
        };
        // Keep the first image provisional for the preceding range. An interior
        // image becomes stable once its successor confirms the upper fence.
        let stable = self.pending.as_ref().is_some_and(|(ordinal, pending)| {
            let Output::Image(image) = pending else {
                return false;
            };
            *ordinal != 0
                && image
                    .upper
                    .as_ref()
                    .is_some_and(|upper| upper.bytes() == lower)
        });
        let old = self.pending.replace((self.next, output));
        self.next += 1;
        if let Some((ordinal, output)) = old {
            self.send(ordinal, output, stable).await?;
        }
        Ok(())
    }

    async fn send(
        &mut self,
        ordinal: usize,
        output: Output,
        stable: bool,
    ) -> RuntimeOrFatalResult<()> {
        let (ack, accepted) = flume::bounded(1);
        self.sender
            .send_async(Packet {
                plan: self.plan.clone(),
                partition: self.partition,
                ordinal,
                output,
                stable,
                ack,
            })
            .await
            .map_err(|_| execution_error("checkpoint assembler stopped accepting output"))?;
        // The assembler is the sole progress producer. Queue delivery transfers
        // ownership; acknowledgement follows consumption or transfer into A.
        // Receiver teardown wakes rejected producers; the merge owner drains all
        // accepted jobs before its read authority and invocation budget disappear.
        accepted
            .recv_async()
            .await
            .map_err(|_| execution_error("checkpoint assembler rejected output"))?;
        Ok(())
    }

    async fn finish(&mut self) -> RuntimeOrFatalResult<()> {
        if let Some((ordinal, output)) = self.pending.take() {
            self.send(ordinal, output, false).await?;
        }
        Ok(())
    }
}

/// A complete-key reducer backed by the shared partition stream and run owner.
struct MutationReducer {
    stream: PartitionMergeStream,
    plan: Arc<PreparedMerge>,
    next: Option<HotEntryRef>,
}

impl MutationReducer {
    /// Prime bounded lookahead without copying a merged operation vector.
    fn new(mut stream: PartitionMergeStream) -> RuntimeOrFatalResult<BuildOutcome<Self>> {
        let (plan, _) = stream.identity();
        let BuildOutcome::Complete(next) = stream.next_entry()? else {
            return Ok(BuildOutcome::Stopped);
        };
        Ok(BuildOutcome::Complete(Self { stream, plan, next }))
    }

    /// The next complete mutation identity, borrowed from resident source storage.
    #[inline]
    fn peek(&self) -> Option<HotEntryRef> {
        self.next
    }

    /// Reconcile one complete key against its captured base owner. Unique puts
    /// suppress all deletes; otherwise only a matching owner authorizes removal.
    /// Non-unique synthetic overlap preserves delete-wins compatibility.
    #[inline]
    fn reduce(
        &mut self,
        owner: Option<RowID>,
        unique: bool,
    ) -> RuntimeOrFatalResult<BuildOutcome<Option<RowID>>> {
        let first = self
            .next
            .ok_or_else(|| execution_error("checkpoint reducer exhausted"))?;
        let runs = self.plan.runs();
        let key = first.resolve(runs).key.as_bytes();
        let mut put = None;
        let mut removed = false;
        while let Some(reference) = self.next {
            let entry = reference.resolve(runs);
            if entry.key.as_bytes() != key {
                break;
            }
            match reference.kind(runs) {
                RunKind::Put => put = Some(entry.row_id),
                RunKind::Delete => removed |= !unique || owner == Some(entry.row_id),
                RunKind::Create => {
                    return Err(execution_error("checkpoint reducer received CREATE input"));
                }
            }
            let BuildOutcome::Complete(next) = self.stream.next_entry()? else {
                return Ok(BuildOutcome::Stopped);
            };
            self.next = next;
        }
        Ok(BuildOutcome::Complete(if !unique && removed {
            None
        } else {
            put.or_else(|| owner.filter(|_| !removed))
        }))
    }

    /// Certify exact operation consumption only after the final group was reduced.
    fn finish<T: Send + 'static>(
        self,
        output: T,
    ) -> RuntimeOrFatalResult<BuildOutcome<CompletedPartition<T>>> {
        if self.next.is_some() {
            return Err(execution_error(
                "checkpoint reducer omitted a mutation group",
            ));
        }
        self.stream.finish(output)
    }
}

struct LeafConsumer<F: DiskTreeSpec> {
    runtime: DiskTreeRuntime<F>,
    root: Option<BlockID>,
    guard: PoolGuard,
    output: flume::Sender<Packet>,
    jobs: BudgetedVec<MemoryBudget>,
    budget: MemoryBudget,
    admission: Arc<CheckpointAdmission>,
    ts: TrxID,
    partial: bool,
}

impl<F: DiskTreeSpec> PartitionConsumer for LeafConsumer<F> {
    type Output = RangeSummary;

    async fn consume(
        &self,
        stream: PartitionMergeStream,
    ) -> RuntimeOrFatalResult<BuildOutcome<CompletedPartition<RangeSummary>>> {
        let (plan, partition) = stream.identity();
        let BuildOutcome::Complete(mutations) = MutationReducer::new(stream)? else {
            return Ok(BuildOutcome::Stopped);
        };
        RangeWorker::new(self, &plan, partition, mutations)?
            .run()
            .await
    }
}

// One partition's merge and leaf-packing state. Bounds borrow the caller's plan,
// so advancing the worker never borrows keys from its own output state.
struct RangeWorker<'a, F: DiskTreeSpec> {
    consumer: &'a LeafConsumer<F>,
    runs: &'a SortedRuns,
    lower: Option<&'a [u8]>,
    upper: Option<&'a [u8]>,
    base: BaseReader<F>,
    mutations: MutationReducer,
    window: BudgetedEntries<Entry>,
    output: JobOutput<'a>,
    scratch: MemoryBudget,
    capacity: usize,
    changed: bool,
    leaf_changed: bool,
    leaf_emitted: bool,
    #[cfg(feature = "profiling")]
    measurements: CheckpointIndexMeasurements,
}

impl<'a, F: DiskTreeSpec> RangeWorker<'a, F> {
    fn new(
        consumer: &'a LeafConsumer<F>,
        plan: &'a Arc<PreparedMerge>,
        partition: usize,
        mutations: MutationReducer,
    ) -> RuntimeOrFatalResult<Self> {
        let runs = plan.runs();
        let (lower, upper) = plan.key_range(partition);
        // The invocation owns JOB_BYTES through ordered acknowledgement; this
        // inner budget enforces the per-job allowance against actual growth.
        let scratch = consumer.jobs[partition].clone();
        let tree = consumer.runtime.open(consumer.root, &consumer.guard);
        let base = BaseReader::new(&tree, &scratch);
        let capacity = 2 * max_node_slots::<F::LeafValue>() + 1;
        let mut window = BudgetedEntries::with_payload_credit(&scratch, PAYLOAD_CREDIT_BYTES);
        window
            .ensure_capacity(capacity, "checkpoint leaf window")
            .change_context(RuntimeError::CheckpointExecution)?;
        Ok(Self {
            consumer,
            runs,
            lower: lower.map(|r| r.resolve(runs).key.as_bytes()),
            upper: upper.map(|r| r.resolve(runs).key.as_bytes()),
            base,
            mutations,
            window,
            output: JobOutput {
                sender: &consumer.output,
                plan: plan.clone(),
                partition,
                next: 0,
                pending: None,
            },
            scratch,
            capacity,
            changed: false,
            leaf_changed: false,
            leaf_emitted: false,
            #[cfg(feature = "profiling")]
            measurements: CheckpointIndexMeasurements::default(),
        })
    }

    async fn run(mut self) -> RuntimeOrFatalResult<BuildOutcome<CompletedPartition<RangeSummary>>> {
        loop {
            let mutation = self.mutations.peek();
            if self.base.needs_traversal() {
                self.window.trim_payload();
            }
            let present = match self
                .base
                .next_event(&ReadRange {
                    lower: self.lower,
                    upper: self.upper,
                    mutation: mutation.map(|r| r.resolve(self.runs).key.as_bytes()),
                    reuse: self.consumer.partial,
                    budget: &self.consumer.budget,
                })
                .await?
            {
                BaseEvent::EntryReady => true,
                BaseEvent::NoEntry if mutation.is_none() => break,
                BaseEvent::NoEntry => false,
                BaseEvent::LeafFinished { leaf, complete } => {
                    self.finish_leaf(leaf, complete).await?;
                    continue;
                }
                BaseEvent::SubtreeReused(child) => {
                    self.reuse_subtree(child).await?;
                    continue;
                }
            };
            if let BuildOutcome::Stopped = self.advance(present, mutation)? {
                return Ok(BuildOutcome::Stopped);
            }
            // Flush before another wide key can exhaust the bounded allowance.
            if self.window.len() == self.capacity
                || (self.window.len() > 1 && self.scratch.used() > JOB_BYTES / 2)
            {
                self.leaf_emitted = true;
                self.emit(self.upper).await?;
                yield_now().await;
            }
        }
        self.finish().await
    }

    #[inline]
    fn advance(
        &mut self,
        present: bool,
        mutation: Option<HotEntryRef>,
    ) -> RuntimeOrFatalResult<BuildOutcome<()>> {
        let take_base = if present {
            match mutation {
                Some(r) => self
                    .base
                    .entry()?
                    .key
                    .cmp_bytes(r.resolve(self.runs).key.as_bytes())
                    .is_lt(),
                None => true,
            }
        } else {
            false
        };
        if take_base {
            let entry = self.base.entry()?;
            let row = F::entry_row(&entry).change_context(RuntimeError::CheckpointExecution)?;
            self.window
                .push_with(
                    if entry.key.len() > MEM_CMP_KEY_INLINE {
                        entry.key.len()
                    } else {
                        0
                    },
                    "checkpoint leaf window",
                    "checkpoint decoded base key",
                    || Entry {
                        key: EntryKey::Base(entry.key.to_btree_key()),
                        row,
                    },
                )
                .change_context(RuntimeError::CheckpointExecution)?;
            self.base.slot += 1;
        } else {
            let reference = mutation
                .ok_or_else(|| execution_error("checkpoint merge lost its next mutation"))?;
            let equal = present
                && self
                    .base
                    .entry()?
                    .key
                    .cmp_bytes(reference.resolve(self.runs).key.as_bytes())
                    .is_eq();
            let owner = if equal {
                let row = F::entry_row(&self.base.entry()?)
                    .change_context(RuntimeError::CheckpointExecution)?;
                self.base.slot += 1;
                Some(row)
            } else {
                None
            };
            let BuildOutcome::Complete(row) = self.mutations.reduce(owner, F::UNIQUE)? else {
                return Ok(BuildOutcome::Stopped);
            };
            self.changed |= row != owner;
            self.leaf_changed |= row != owner;
            if let Some(row) = row {
                self.window
                    .push_with(
                        0,
                        "checkpoint leaf window",
                        "checkpoint decoded base key",
                        || Entry {
                            key: EntryKey::Delta(reference),
                            row,
                        },
                    )
                    .change_context(RuntimeError::CheckpointExecution)?;
            }
        }
        Ok(BuildOutcome::Complete(()))
    }

    async fn finish_leaf(&mut self, leaf: Descriptor, complete: bool) -> RuntimeOrFatalResult<()> {
        if complete && !self.leaf_changed && !self.leaf_emitted {
            self.window.clear();
            #[cfg(feature = "profiling")]
            {
                self.measurements.reused_subtrees += 1;
            }
            self.output.stage(Output::Subtree(leaf)).await?;
        } else {
            let fence = match (leaf.upper.as_ref().map(|key| key.bytes()), self.upper) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (a, b) => a.or(b),
            };
            self.flush_window(fence).await?;
        }
        self.leaf_changed = false;
        self.leaf_emitted = false;
        Ok(())
    }

    async fn reuse_subtree(&mut self, child: Descriptor) -> RuntimeOrFatalResult<()> {
        #[cfg(feature = "profiling")]
        {
            self.measurements.reused_subtrees += 1;
        }
        self.flush_window(Some(child.lower.bytes())).await?;
        self.output.stage(Output::Subtree(child)).await
    }

    async fn flush_window(&mut self, upper: Option<&[u8]>) -> RuntimeOrFatalResult<()> {
        while !self.window.is_empty() {
            self.emit(upper).await?;
        }
        Ok(())
    }

    async fn finish(
        mut self,
    ) -> RuntimeOrFatalResult<BuildOutcome<CompletedPartition<RangeSummary>>> {
        while !self.window.is_empty() {
            self.emit(self.upper).await?;
            yield_now().await;
        }
        #[cfg(feature = "profiling")]
        {
            self.measurements.base_nodes_read = self.base.reads;
        }
        self.output.finish().await?;
        self.mutations.finish(RangeSummary {
            #[cfg(feature = "profiling")]
            measurements: self.measurements,
            packets: self.output.next,
            changed: self.changed,
        })
    }

    async fn emit(&mut self, upper: Option<&[u8]>) -> RuntimeOrFatalResult<()> {
        self.window.trim_payload();
        let window = &self.window;
        let runs = self.runs;
        let lower = window[0].key(runs);
        let mut entries = BudgetedVec::new(&self.scratch);
        let count = plan_candidates(
            &mut entries,
            window.len(),
            PackedNodePlanParams {
                lower_fence: lower,
                upper_fence: upper,
                min_slots: 1,
            },
            "checkpoint packed leaf slots",
            |index| PackedNodeEntry {
                key: window[index].key(runs),
                value: window[index].value::<F>(),
            },
        )?;
        let upper = window.get(count).map(|entry| entry.key(runs)).or(upper);
        let allocation = self
            .consumer
            .admission
            .output(DISK_TREE_BLOCK_SIZE, "checkpoint output image")?;
        let buf = pack_admitted_node(
            KnownFenceNodeParams {
                height: 0,
                ts: self.consumer.ts,
                lower_fence: lower,
                lower_fence_value: BTreeU64::INVALID_VALUE,
                upper_fence: upper,
                hints_enabled: true,
            },
            &entries[..count],
        )?;
        let image = Image {
            promotion: None,
            buf,
            allocation,
            lower: OwnedKey::reuse_or_copy(lower, self.output.upper(), &self.consumer.budget)?,
            upper: upper
                .map(|upper| OwnedKey::copy(upper, &self.consumer.budget))
                .transpose()?,
            height: 0,
            count,
        };
        self.output.stage(Output::Image(image)).await?;
        drop(entries);
        self.window.consume_prefix(count);
        Ok(())
    }
}

struct Descriptor {
    promotion: Option<(BlockID, u16, bool)>,
    reused: bool,
    block: BlockID,
    lower: Arc<OwnedKey>,
    upper: Option<Arc<OwnedKey>>,
    height: u16,
}

struct QueuedOutput {
    output: Output,
    _provisional: MemoryReservation,
}

struct OrderedOutput {
    outputs: BudgetedVec<Option<QueuedOutput>>,
    budget: MemoryBudget,
    next: usize,
    received: usize,
}

impl OrderedOutput {
    fn push(&mut self, output: Output) -> RuntimeOrFatalResult<()> {
        let (lower, upper, image) = match &output {
            Output::Image(image) => (&image.lower, &image.upper, DISK_TREE_BLOCK_SIZE),
            Output::Subtree(child) => (&child.lower, &child.upper, 0),
        };
        let bytes = image + lower.bytes().len() + upper.as_ref().map_or(0, |key| key.bytes().len());
        let provisional = self
            .budget
            .reserve(bytes, "checkpoint ordered provisional payload")
            .change_context(RuntimeError::CheckpointExecution)?;
        self.outputs
            .push(
                Some(QueuedOutput {
                    output,
                    _provisional: provisional,
                }),
                "checkpoint ordered output",
            )
            .change_context(RuntimeError::CheckpointExecution)?;
        self.received += 1;
        Ok(())
    }

    fn pop(&mut self) -> Option<QueuedOutput> {
        let output = self.outputs.get_mut(self.next)?.take();
        self.next += 1;
        output
    }

    fn release(&mut self) {
        self.outputs = BudgetedVec::new(&self.budget);
    }
}

struct Write {
    block: BlockID,
    completion: Arc<Completion<()>>,
    _observer: MemoryReservation,
}

/// Retains every accepted write through successful observation or cleanup.
struct Writer<'a> {
    file: &'a mut MutableTableFile,
    admission: Arc<CheckpointAdmission>,
    limit: usize,
    writes: BudgetedVec<Write>,
    allocations: BudgetedVec<BlockID>,
}

impl Writer<'_> {
    async fn write(&mut self, image: Image) -> RuntimeOrFatalResult<Descriptor> {
        if self.writes.len() == self.limit {
            self.observe().await?;
        }
        self.writes
            .reserve_one("checkpoint write ledger")
            .change_context(RuntimeError::CheckpointExecution)?;
        self.allocations
            .reserve_one("checkpoint allocation ledger")
            .change_context(RuntimeError::CheckpointExecution)?;
        let observer = self.admission.output(1024, "checkpoint write observer")?;
        let block = self.file.allocate_block()?;
        self.allocations.push_reserved(block);
        let ingress = self
            .file
            .prepare_disk_tree_write(block, image.buf, image.allocation)?;
        let completion = ingress
            .await
            .map_err(|error| error.into_runtime_or_fatal(RuntimeError::CheckpointExecution))
            .attach("operation=checkpoint_reconcile, phase=write_acceptance")?;
        self.writes.push_reserved(Write {
            block,
            completion,
            _observer: observer,
        });
        Ok(Descriptor {
            promotion: image.promotion,
            reused: false,
            block,
            lower: image.lower,
            upper: image.upper,
            height: image.height,
        })
    }

    async fn observe(&mut self) -> RuntimeOrFatalResult<()> {
        let pending = &self.writes[0];
        let result = pending.completion.wait_result().await;
        #[cfg(feature = "profiling")]
        if result.is_ok() {
            self.admission.measure(|m| m.written_nodes += 1);
        }
        let block = pending.block;
        self.writes.swap_remove(0);
        result
            .map_err(|error| error.into_runtime_or_fatal(RuntimeError::CheckpointExecution))
            .attach_with(|| {
                format!("operation=checkpoint_reconcile, phase=write_completion, block={block}")
            })
    }

    async fn settle(&mut self, rollback: bool) -> RuntimeOrFatalResult<()> {
        let mut failure: Option<RuntimeOrFatalError> = None;
        while !self.writes.is_empty() {
            if let Err(error) = self.observe().await {
                failure = Some(match failure {
                    Some(old) => old.merge_cleanup(error),
                    None => error,
                });
            }
        }
        if rollback && !matches!(failure, Some(RuntimeOrFatalError::Fatal(_))) {
            for &block in &*self.allocations {
                self.file.rollback_allocated_block(block);
            }
        }
        failure.map_or(Ok(()), Err)
    }
}

struct Assembler<F: DiskTreeSpec> {
    runtime: DiskTreeRuntime<F>,
    root: Option<BlockID>,
    guard: PoolGuard,
    pending: Option<Image>,
    pending_candidates: usize,
    partial: bool,
    levels: BudgetedVec<BudgetedVec<Descriptor>>,
    scratch: MemoryBudget,
    budget: MemoryBudget,
    admission: Arc<CheckpointAdmission>,
    ts: TrxID,
    _shape: PhantomData<F>,
}

impl<F: DiskTreeSpec> Assembler<F> {
    fn new(
        tree: &DiskTree<'_, F>,
        admission: Arc<CheckpointAdmission>,
        ts: TrxID,
        partial: bool,
    ) -> Self {
        let (runtime, root, guard) = tree.snapshot();
        let budget = &admission.budget;
        let scratch = MemoryBudget::new(ASSEMBLER_BYTES);
        Self {
            runtime,
            root,
            guard,
            pending: None,
            pending_candidates: 0,
            partial,
            levels: BudgetedVec::new(&scratch),
            scratch,
            budget: budget.clone(),
            admission,
            ts,
            _shape: PhantomData,
        }
    }

    async fn append(&mut self, writer: &mut Writer<'_>, image: Image) -> RuntimeOrFatalResult<()> {
        #[cfg(test)]
        {
            self.admission.rewrite_work.lock().candidate_visits += 1;
        }
        let Some(pending) = self.pending.take() else {
            self.bridge(writer, Some(image.lower.clone())).await?;
            self.pending = Some(image);
            self.pending_candidates = 1;
            return Ok(());
        };
        if pending.lower.bytes() >= image.lower.bytes() {
            return Err(execution_error(
                "checkpoint leaves arrived outside physical key order",
            ));
        }
        let candidates = self.pending_candidates + 1;
        // Full reconstruction has only range-boundary images here: stable
        // interiors bypass assembly. Combining its few-survivor boundaries is
        // required to produce one final open leaf when they fit. Partial rewrite
        // can expose arbitrarily many old leaf boundaries, so optional absorption
        // must stop after W candidates, even when the combined payload fits.
        let adjacent = pending
            .upper
            .as_ref()
            .is_some_and(|upper| upper.bytes() == image.lower.bytes());
        if (self.partial && candidates > OPTIONAL_LEAF_CANDIDATES)
            || (pending.count + image.count > max_node_slots::<F::LeafValue>() && adjacent)
        {
            self.flush(writer, pending, image.lower.clone()).await?;
            self.pending = Some(image);
            self.pending_candidates = 1;
            return Ok(());
        }
        let mut entries = BudgetedEntries::with_payload_credit(&self.scratch, PAYLOAD_CREDIT_BYTES);
        self.decode(&pending, &mut entries)?;
        self.decode(&image, &mut entries)?;
        self.repack(writer, entries, image.upper).await?;
        self.pending_candidates = candidates;
        Ok(())
    }

    async fn flush(
        &mut self,
        writer: &mut Writer<'_>,
        image: Image,
        upper: Arc<OwnedKey>,
    ) -> RuntimeOrFatalResult<()> {
        let image = if image
            .upper
            .as_ref()
            .is_some_and(|key| key.bytes() == upper.bytes())
        {
            image
        } else {
            // Required fence repair remains legal after optional absorption ends.
            let mut entries =
                BudgetedEntries::with_payload_credit(&self.scratch, PAYLOAD_CREDIT_BYTES);
            self.decode(&image, &mut entries)?;
            self.repack(writer, entries, Some(upper)).await?;
            self.pending
                .take()
                .ok_or_else(|| execution_error("checkpoint flush lost provisional tail"))?
        };
        let descriptor = writer.write(image).await?;
        self.push(writer, descriptor).await
    }

    async fn reuse(
        &mut self,
        writer: &mut Writer<'_>,
        child: Descriptor,
    ) -> RuntimeOrFatalResult<()> {
        if let Some(image) = self.pending.take() {
            self.flush(writer, image, child.lower.clone()).await?;
        }
        self.pending_candidates = 0;
        self.bridge(writer, Some(child.lower.clone())).await?;
        for level in 0..usize::from(child.height) {
            while self
                .levels
                .get(level)
                .is_some_and(|children| !children.is_empty())
            {
                let image = self.parent(level)?;
                let descriptor = writer.write(image).await?;
                self.push(writer, descriptor).await?;
            }
        }
        self.push(writer, child).await
    }

    async fn bridge(
        &mut self,
        writer: &mut Writer<'_>,
        upper: Option<Arc<OwnedKey>>,
    ) -> RuntimeOrFatalResult<()> {
        let Some(level) = self.levels.iter().position(|children| !children.is_empty()) else {
            return Ok(());
        };
        let last = self.levels[level]
            .last()
            .unwrap_or_else(|| unreachable!("nonempty frontier level"));
        if last.upper.as_ref().map(|key| key.bytes()) == upper.as_ref().map(|key| key.bytes()) {
            return Ok(());
        }
        if !last.reused {
            return Err(execution_error(
                "checkpoint attempted to reopen a finalized interior",
            ));
        }
        let old = self.levels[level]
            .pop()
            .unwrap_or_else(|| unreachable!("retained right edge"));
        let mut repaired = self.repair(writer, old, upper).await?;
        repaired.reverse();
        while let Some(child) = repaired.pop() {
            self.push(writer, child).await?;
        }
        Ok(())
    }

    fn repair<'a>(
        &'a mut self,
        writer: &'a mut Writer<'_>,
        old: Descriptor,
        upper: Option<Arc<OwnedKey>>,
    ) -> BoxFuture<'a, RuntimeOrFatalResult<BudgetedVec<Descriptor>>> {
        Box::pin(async move {
            let _frame = self
                .scratch
                .reserve(2048, "checkpoint fence repair frame")
                .change_context(RuntimeError::CheckpointExecution)?;
            #[cfg(feature = "profiling")]
            self.admission.measure(|m| m.base_nodes_read += 1);
            let guard = self
                .runtime
                .open(self.root, &self.guard)
                .read_node(old.block)
                .await?;
            let node = guard.view();
            if node.height() != usize::from(old.height)
                || node.fence_key(false).cmp_bytes(old.lower.bytes()).is_ne()
            {
                return Err(execution_error(
                    "checkpoint retained edge changed identity or height",
                ));
            }
            if node.is_leaf() {
                self.repair_leaf(writer, guard, upper).await
            } else {
                self.repair_branch(writer, guard, old, upper).await
            }
        })
    }

    async fn repair_leaf(
        &mut self,
        writer: &mut Writer<'_>,
        guard: DiskTreeNodeGuard<F>,
        upper: Option<Arc<OwnedKey>>,
    ) -> RuntimeOrFatalResult<BudgetedVec<Descriptor>> {
        let node = guard.view();
        let mut result = BudgetedVec::new(&self.scratch);
        let mut entries = BudgetedEntries::with_payload_credit(&self.scratch, PAYLOAD_CREDIT_BYTES);
        entries
            .ensure_capacity(node.count(), "checkpoint old edge entries")
            .change_context(RuntimeError::CheckpointExecution)?;
        for slot in 0..node.count() {
            let entry = node.leaf_entry(slot)?;
            let row = F::entry_row(&entry).change_context(RuntimeError::CheckpointExecution)?;
            entries
                .push_with(
                    if entry.key.len() > MEM_CMP_KEY_INLINE {
                        entry.key.len()
                    } else {
                        0
                    },
                    "checkpoint old edge entries",
                    "checkpoint old edge key",
                    || Entry {
                        key: EntryKey::Base(entry.key.to_btree_key()),
                        row,
                    },
                )
                .change_context(RuntimeError::CheckpointExecution)?;
        }
        drop(guard);
        while !entries.is_empty() {
            let image = self.pack_leaf_prefix(
                &mut entries,
                result
                    .last()
                    .and_then(|child: &Descriptor| child.upper.as_ref()),
                upper.as_ref(),
                "checkpoint repaired leaf slots",
            )?;
            result
                .reserve_one("checkpoint repaired forest")
                .change_context(RuntimeError::CheckpointExecution)?;
            result.push_reserved(writer.write(image).await?);
        }
        Ok(result)
    }

    async fn repair_branch(
        &mut self,
        writer: &mut Writer<'_>,
        guard: DiskTreeNodeGuard<F>,
        old: Descriptor,
        upper: Option<Arc<OwnedKey>>,
    ) -> RuntimeOrFatalResult<BudgetedVec<Descriptor>> {
        let node = guard.view();
        let mut result = BudgetedVec::new(&self.scratch);
        let mut children = BudgetedVec::new(&self.scratch);
        children
            .ensure_capacity(node.count() + 1, "checkpoint old edge children")
            .change_context(RuntimeError::CheckpointExecution)?;
        let mut lower = old.lower;
        for slot in 0..=node.count() {
            let child_upper = if slot == node.count() {
                old.upper.clone()
            } else {
                Some(OwnedKey::view(node.key(slot)?, &self.budget)?)
            };
            children.push_reserved(Descriptor {
                promotion: None,
                reused: true,
                block: node.child_block(slot)?,
                lower: lower.clone(),
                upper: child_upper.clone(),
                height: old.height - 1,
            });
            if let Some(next) = child_upper {
                lower = next;
            }
        }
        drop(guard);
        let edge = children
            .pop()
            .ok_or_else(|| execution_error("checkpoint branch has no right edge"))?;
        let mut repaired = self.repair(writer, edge, upper).await?;
        repaired.reverse();
        while let Some(child) = repaired.pop() {
            children
                .push(child, "checkpoint repaired children")
                .change_context(RuntimeError::CheckpointExecution)?;
        }
        while !children.is_empty() {
            let image =
                Image::from_children(&mut children, &self.scratch, &self.admission, self.ts)?;
            result
                .reserve_one("checkpoint repaired forest")
                .change_context(RuntimeError::CheckpointExecution)?;
            result.push_reserved(writer.write(image).await?);
        }
        Ok(result)
    }

    fn decode(
        &self,
        image: &Image,
        entries: &mut BudgetedEntries<Entry>,
    ) -> RuntimeOrFatalResult<()> {
        entries
            .ensure_capacity(entries.len() + image.count, "checkpoint boundary entries")
            .change_context(RuntimeError::CheckpointExecution)?;
        let node: &BTreeNode = layout::ref_from_bytes(image.buf.data());
        for slot in 0..image.count {
            let entry = node
                .entry_view_checked::<F::LeafValue>(slot)
                .ok_or_else(|| {
                    execution_error("checkpoint provisional image has invalid leaf entry")
                })?;
            F::validate_leaf_entry(&entry).change_context(RuntimeError::CheckpointExecution)?;
            let row = F::entry_row(&entry).change_context(RuntimeError::CheckpointExecution)?;
            entries
                .push_with(
                    if entry.key.len() > MEM_CMP_KEY_INLINE {
                        entry.key.len()
                    } else {
                        0
                    },
                    "checkpoint boundary entries",
                    "checkpoint boundary key",
                    || Entry {
                        key: EntryKey::Base(entry.key.to_btree_key()),
                        row,
                    },
                )
                .change_context(RuntimeError::CheckpointExecution)?;
            #[cfg(test)]
            {
                self.admission.rewrite_work.lock().revisited_entries += 1;
            }
        }
        Ok(())
    }

    // Pack and consume one prefix. Callers decide whether its image can be
    // written immediately or must remain provisional for the next fence.
    fn pack_leaf_prefix(
        &self,
        entries: &mut BudgetedEntries<Entry>,
        lower: Option<&Arc<OwnedKey>>,
        upper: Option<&Arc<OwnedKey>>,
        purpose: &'static str,
    ) -> RuntimeOrFatalResult<Image> {
        entries.trim_payload();
        let bytes = |index: usize| match &entries[index].key {
            EntryKey::Base(key) => key.as_bytes(),
            EntryKey::Delta(_) => unreachable!("assembler decodes provisional images"),
        };
        let mut slots = BudgetedVec::new(&self.scratch);
        let count = plan_candidates(
            &mut slots,
            entries.len(),
            PackedNodePlanParams {
                lower_fence: bytes(0),
                upper_fence: upper.map(|key| key.bytes()),
                min_slots: 1,
            },
            purpose,
            |index| PackedNodeEntry {
                key: bytes(index),
                value: entries[index].value::<F>(),
            },
        )?;
        let next_upper = if count < entries.len() {
            Some(OwnedKey::copy(bytes(count), &self.budget)?)
        } else {
            upper.cloned()
        };
        let allocation = self
            .admission
            .output(DISK_TREE_BLOCK_SIZE, "checkpoint output image")?;
        let image = Image {
            promotion: None,
            buf: pack_admitted_node(
                KnownFenceNodeParams {
                    height: 0,
                    ts: self.ts,
                    lower_fence: bytes(0),
                    lower_fence_value: BTreeU64::INVALID_VALUE,
                    upper_fence: next_upper.as_ref().map(|key| key.bytes()),
                    hints_enabled: true,
                },
                &slots[..count],
            )?,
            allocation,
            lower: OwnedKey::reuse_or_copy(bytes(0), lower, &self.budget)?,
            upper: next_upper,
            height: 0,
            count,
        };
        drop(slots);
        entries.consume_prefix(count);
        Ok(image)
    }

    async fn repack(
        &mut self,
        writer: &mut Writer<'_>,
        mut entries: BudgetedEntries<Entry>,
        upper: Option<Arc<OwnedKey>>,
    ) -> RuntimeOrFatalResult<()> {
        // Boundary entries are owned base-shaped keys, so no run resolution is
        // required. Keep the complete finite/open upper fence through planning.
        let mut lower = None;
        while !entries.is_empty() {
            let image = self.pack_leaf_prefix(
                &mut entries,
                lower.as_ref(),
                upper.as_ref(),
                "checkpoint boundary packing",
            )?;
            lower = image.upper.clone();
            if entries.is_empty() {
                self.pending = Some(image);
            } else {
                let child = writer.write(image).await?;
                self.push(writer, child).await?;
            }
        }
        Ok(())
    }

    async fn push(
        &mut self,
        writer: &mut Writer<'_>,
        mut child: Descriptor,
    ) -> RuntimeOrFatalResult<()> {
        loop {
            let level = usize::from(child.height);
            while self.levels.len() <= level {
                self.levels
                    .push(
                        BudgetedVec::new(&self.scratch),
                        "checkpoint parent frontier",
                    )
                    .change_context(RuntimeError::CheckpointExecution)?;
            }
            let children = &mut self.levels[level];
            if let Some(previous) = children.last()
                && (previous.height != child.height
                    || previous
                        .upper
                        .as_ref()
                        .is_none_or(|upper| upper.bytes() != child.lower.bytes()))
            {
                return Err(execution_error(
                    "checkpoint parent children have discontinuous fences or heights",
                ));
            }
            children
                .push(child, "checkpoint parent children")
                .change_context(RuntimeError::CheckpointExecution)?;
            if children.len() <= 2 * max_node_slots::<BTreeU64>() + 1 {
                return Ok(());
            }
            let image = self.parent(level)?;
            child = writer.write(image).await?;
        }
    }

    fn parent(&mut self, level: usize) -> RuntimeOrFatalResult<Image> {
        Image::from_children(
            &mut self.levels[level],
            &self.scratch,
            &self.admission,
            self.ts,
        )
    }

    async fn finish(&mut self, writer: &mut Writer<'_>) -> RuntimeOrFatalResult<Option<BlockID>> {
        if let Some(image) = self.pending.take() {
            if image.upper.is_some() {
                let mut entries =
                    BudgetedEntries::with_payload_credit(&self.scratch, PAYLOAD_CREDIT_BYTES);
                self.decode(&image, &mut entries)?;
                self.repack(writer, entries, None).await?;
                let image = self.pending.take().ok_or_else(|| {
                    execution_error("checkpoint final fence repair lost its tail")
                })?;
                let child = writer.write(image).await?;
                self.push(writer, child).await?;
            } else {
                let child = writer.write(image).await?;
                self.push(writer, child).await?;
            }
        }
        self.bridge(writer, None).await?;
        let mut level = 0;
        loop {
            if level == self.levels.len() {
                return Ok(None);
            }
            if self.levels[level].is_empty() {
                level += 1;
                continue;
            }
            if self.levels[level].len() == 1
                && self.levels[level + 1..]
                    .iter()
                    .all(|children| children.is_empty())
            {
                let root = &self.levels[level][0];
                if root.upper.is_some() {
                    return Err(execution_error(
                        "checkpoint candidate root retains a finite upper fence",
                    ));
                }
                let (mut block, mut height, reused) =
                    root.promotion
                        .unwrap_or((root.block, root.height, root.reused));
                while reused && height != 0 {
                    let guard = self
                        .runtime
                        .open(self.root, &self.guard)
                        .read_node(block)
                        .await?;
                    #[cfg(feature = "profiling")]
                    self.admission.measure(|m| m.base_nodes_read += 1);
                    let node = guard.view();
                    if node.height() != usize::from(height) || !node.has_no_upper_fence() {
                        return Err(execution_error(
                            "checkpoint root promotion has invalid final shape",
                        ));
                    }
                    if node.count() != 0 {
                        break;
                    }
                    block = node.child_block(0)?;
                    height -= 1;
                }
                return Ok(Some(block));
            }
            while !self.levels[level].is_empty() {
                let image = self.parent(level)?;
                let child = writer.write(image).await?;
                self.push(writer, child).await?;
            }
            level += 1;
        }
    }
}

impl<F: DiskTreeSpec> IndexCheckpoint<'_, '_, F> {
    /// Build a replacement with admitted range workers and an ordered assembler.
    /// Root authority follows exact merge coverage and successful terminal
    /// observation of every accepted write.
    pub(super) async fn build(self, mode: BuildMode) -> RuntimeOrFatalResult<Option<BlockID>> {
        CheckpointBuilder::new(self, mode).await?.run().await
    }
}

struct CheckpointBuilder<'a, F: DiskTreeSpec> {
    assembler: Assembler<F>,
    writer: Writer<'a>,
    consumption: MergeConsumption<LeafConsumer<F>>,
    input: flume::Receiver<Packet>,
    queues: BudgetedVec<OrderedOutput>,
    plan: Arc<PreparedMerge>,
    root: Option<BlockID>,
    partition: usize,
    changed: bool,
    #[cfg(feature = "profiling")]
    started: Instant,
    // Keep the preparation and admission alive until all workers, handoffs and
    // writes have settled and their owned buffers have been released.
    _preparation: MergePreparation,
    _queue_admission: MemoryReservation,
    _allowance: MemoryReservation,
    _admission: Arc<CheckpointAdmission>,
}

impl<'a, F: DiskTreeSpec> CheckpointBuilder<'a, F> {
    async fn new(
        checkpoint: IndexCheckpoint<'a, '_, F>,
        mode: BuildMode,
    ) -> RuntimeOrFatalResult<Self> {
        let IndexCheckpoint {
            tree,
            file,
            runs,
            ts,
            pool,
            admission,
        } = checkpoint;
        #[cfg(feature = "profiling")]
        let started = Instant::now();
        let partial = mode == BuildMode::PartialRewrite;
        let policy = admission.policy;
        let budget = runs.budget.clone();
        let mut allowance = MemoryReservation::new(&budget);
        let entries: usize = runs.runs().iter().map(|run| run.entries().len()).sum();
        let mut workers = 1;
        while workers < policy.max_workers && workers < entries.div_ceil(65_536) {
            if allowance
                .grow(JOB_BYTES, "checkpoint additional job")
                .is_err()
            {
                break;
            }
            workers += 1;
        }
        let mut preparation = MergePreparation::new(runs, pool.clone(), workers)?;
        let plan = preparation.execute().await?;
        #[cfg(feature = "profiling")]
        admission.measure(|m| {
            m.max_workers = m.max_workers.max(workers.min(plan.partitions()) as u64);
        });
        let (output, input) = flume::bounded(policy.max_ready_buffers);
        let queue_admission = budget
            .reserve(
                policy.max_ready_buffers.next_power_of_two() * 2 * size_of::<Packet>(),
                "checkpoint handoff queue",
            )
            .change_context(RuntimeError::CheckpointExecution)?;
        let mut jobs = BudgetedVec::new(&budget);
        let mut queues = BudgetedVec::new(&budget);
        for _ in 0..plan.partitions() {
            let job = MemoryBudget::new(JOB_BYTES);
            queues
                .push(
                    OrderedOutput {
                        outputs: BudgetedVec::new(&job),
                        budget: job.clone(),
                        next: 0,
                        received: 0,
                    },
                    "checkpoint ordered queues",
                )
                .change_context(RuntimeError::CheckpointExecution)?;
            jobs.push(job, "checkpoint job budgets")
                .change_context(RuntimeError::CheckpointExecution)?;
        }
        let (runtime, root, guard) = tree.snapshot();
        let consumption = MergeConsumption::new_admitted(
            plan.clone(),
            pool,
            LeafConsumer {
                runtime,
                root,
                guard,
                output,
                jobs,
                budget: budget.clone(),
                admission: admission.clone(),
                ts,
                partial,
            },
        )?;
        let writer = Writer {
            file,
            admission: admission.clone(),
            limit: policy.max_in_flight_writes,
            writes: BudgetedVec::new(&budget),
            allocations: BudgetedVec::new(&budget),
        };
        let assembler = Assembler::<F>::new(tree, admission.clone(), ts, partial);
        Ok(Self {
            assembler,
            writer,
            consumption,
            input,
            queues,
            plan,
            root,
            partition: 0,
            changed: false,
            #[cfg(feature = "profiling")]
            started,
            _preparation: preparation,
            _queue_admission: queue_admission,
            _allowance: allowance,
            _admission: admission,
        })
    }

    async fn run(mut self) -> RuntimeOrFatalResult<Option<BlockID>> {
        let result = self.consume().await;
        if let Err(error) = result {
            return Err(self.settle_failure(error).await);
        }
        #[cfg(feature = "profiling")]
        self._admission
            .measure(|m| m.reconcile_nanos += self.started.elapsed().as_nanos() as u64);
        #[cfg(test)]
        {
            let mut work = self._admission.rewrite_work.lock();
            work.max_frontier_bytes = work
                .max_frontier_bytes
                .max(self.assembler.scratch.peak() as u64);
        }
        result
    }

    async fn consume(&mut self) -> RuntimeOrFatalResult<Option<BlockID>> {
        while self.partition < self.plan.partitions() {
            if let Some(output) = self.queues[self.partition].pop() {
                self.assemble(output).await?;
                continue;
            }
            enum Progress<T> {
                Complete(T),
                Packet(Result<Packet, flume::RecvError>),
            }
            let progress = {
                let completion = self.consumption.next_output().fuse();
                let packet = self.input.recv_async().fuse();
                futures::pin_mut!(completion, packet);
                futures::select_biased! {
                    result = completion => Progress::Complete(result),
                    packet = packet => Progress::Packet(packet),
                }
            };
            match progress {
                Progress::Packet(Ok(packet)) => self.accept(packet).await?,
                Progress::Packet(Err(_)) => {
                    return Err(execution_error(
                        "checkpoint output closed before completion",
                    ));
                }
                Progress::Complete(result) => {
                    let (summary, ack) = result?.ok_or_else(|| {
                        execution_error("checkpoint merge omitted a range completion")
                    })?;
                    self.complete_partition(summary, ack)?;
                }
            }
        }
        self.finish().await
    }

    async fn assemble(&mut self, output: QueuedOutput) -> RuntimeOrFatalResult<()> {
        match output.output {
            Output::Image(image) => self.assembler.append(&mut self.writer, image).await?,
            Output::Subtree(child) => self.assembler.reuse(&mut self.writer, child).await?,
        }
        Ok(())
    }

    async fn accept(&mut self, packet: Packet) -> RuntimeOrFatalResult<()> {
        if !Arc::ptr_eq(&packet.plan, &self.plan)
            || packet.partition < self.partition
            || packet.partition >= self.queues.len()
            || packet.ordinal != self.queues[packet.partition].received
        {
            return Err(execution_error(
                "checkpoint output identity or sequence mismatch",
            ));
        }
        let output = match packet.output {
            Output::Image(image) if packet.stable => {
                Output::Subtree(self.writer.write(image).await?)
            }
            output => output,
        };
        self.queues[packet.partition].push(output)?;
        packet
            .ack
            .send(())
            .map_err(|_| execution_error("checkpoint output producer detached"))?;
        Ok(())
    }

    fn complete_partition(
        &mut self,
        summary: RangeSummary,
        ack: MergeAcknowledgement,
    ) -> RuntimeOrFatalResult<()> {
        if summary.packets != self.queues[self.partition].received
            || self.queues[self.partition].next != self.queues[self.partition].received
        {
            return Err(execution_error(
                "checkpoint packet count differs from exhausted range",
            ));
        }
        self.changed |= summary.changed;
        #[cfg(feature = "profiling")]
        self._admission.measure(|m| m.record(&summary.measurements));
        // Packet acknowledgements only transfer payload ownership. This ordered
        // acknowledgement releases the job allowance and permits another job.
        self.queues[self.partition].release();
        self.consumption.acknowledge(ack)?;
        self.partition += 1;
        Ok(())
    }

    async fn finish(&mut self) -> RuntimeOrFatalResult<Option<BlockID>> {
        if self.consumption.next_output().await?.is_some() {
            return Err(execution_error("checkpoint merge produced an extra range"));
        }
        let completion = self.consumption.execute().await?;
        if completion.validation.is_err() {
            return Err(execution_error(
                "checkpoint mutation input acquired CREATE duplicate evidence",
            ));
        }
        if !self.changed {
            self.writer.settle(true).await?;
            return Ok(self.root);
        }
        let root = self.assembler.finish(&mut self.writer).await?;
        self.writer.settle(false).await?;
        Ok(root)
    }

    async fn settle_failure(&mut self, mut error: RuntimeOrFatalError) -> RuntimeOrFatalError {
        let cleanup = self.consumption.settle_rejecting(&self.input, drop).await;
        if let Err(cleanup) = cleanup {
            error = error.merge_cleanup(cleanup);
        }
        if let Err(cleanup) = self
            .writer
            .settle(!matches!(error, RuntimeOrFatalError::Fatal(_)))
            .await
        {
            error = error.merge_cleanup(cleanup);
        }
        error
    }
}

fn consume_prefix<T>(values: &mut BudgetedVec<T>, count: usize) {
    values.rotate_left(count);
    for _ in 0..count {
        values.pop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::build::checkpoint::mutation_runs;
    use crate::index::build::{fail_build_budget, payload_accounting};

    /// Private observations for the bounded partial-rewrite regression test.
    #[derive(Clone, Copy, Debug, Default)]
    pub(crate) struct RewriteWork {
        /// Provisional leaf images considered by ordered assembly.
        pub(crate) candidate_visits: u64,
        /// Leaf entries decoded during boundary combination or repair.
        pub(crate) revisited_entries: u64,
        /// Largest actual assembler scratch occupancy.
        pub(crate) max_frontier_bytes: u64,
    }

    /// Inspect boundary work after all workers and output have settled.
    pub(crate) fn rewrite_work(admission: &CheckpointAdmission) -> RewriteWork {
        *admission.rewrite_work.lock()
    }

    /// Purpose: Account mixed checkpoint entries without charging borrowed or inline key payloads.
    /// Expected: Only owned outlined keys consume payload credit, and consuming them preserves the source run and remaining entries.
    #[test]
    fn mixed_entries_charge_only_owned_payloads() {
        let key = vec![7; 128];
        let runs = mutation_runs(
            vec![(
                RunKind::Put,
                vec![(BTreeKey::from(key.as_slice()), RowID::new(1))],
            )],
            true,
        );
        let source_bytes = runs.budget.used();
        let budget = MemoryBudget::new(256 * 1024);
        let mut entries = BudgetedEntries::with_payload_credit(&budget, PAYLOAD_CREDIT_BYTES);
        entries.ensure_capacity(3, "test entries").unwrap();
        let metadata = budget.used();
        entries
            .push_with(key.len(), "test entries", "test payload", || Entry {
                key: EntryKey::Base(BTreeKey::from(key.as_slice())),
                row: RowID::new(2),
            })
            .unwrap();
        fail_build_budget(&budget, "test payload");
        entries
            .push_with(0, "test entries", "test payload", || Entry {
                key: EntryKey::Delta(HotEntryRef::new(&runs, 0, 0)),
                row: RowID::new(1),
            })
            .unwrap();
        entries
            .push_with(0, "test entries", "test payload", || Entry {
                key: EntryKey::Base(BTreeKey::from(&b"inline"[..])),
                row: RowID::new(3),
            })
            .unwrap();
        assert_eq!(
            payload_accounting(&entries),
            (key.len(), PAYLOAD_CREDIT_BYTES)
        );
        assert_eq!(budget.used(), metadata + PAYLOAD_CREDIT_BYTES);
        entries.consume_prefix(1);
        assert_eq!(budget.used(), metadata);
        assert_eq!(entries[0].key(&runs), key);
        assert_eq!(entries[1].key(&runs), b"inline");
        assert_eq!(runs.budget.used(), source_bytes);
        drop(entries);
        assert_eq!(budget.used(), 0);
    }

    /// Purpose: Retain shared fence admission through the final owner and distinguish unequal boundaries.
    /// Expected: Copy and view charge identical allocations, matching fences share without readmission, and gaps require independently admitted keys.
    #[test]
    fn retained_fences_share_only_equal_boundaries() {
        for len in [0, MEM_CMP_KEY_INLINE, MEM_CMP_KEY_INLINE + 1, 256] {
            let budget = MemoryBudget::new(4096);
            let bytes = vec![7; len];
            let original = OwnedKey::copy(&bytes, &budget).unwrap();
            let charge = size_of::<OwnedKey>()
                + 2 * size_of::<usize>()
                + if len > MEM_CMP_KEY_INLINE { len } else { 0 };
            assert_eq!(budget.used(), charge);
            let mut upper = bytes.clone();
            if let Some(last) = upper.last_mut() {
                *last = 8;
            } else {
                upper.push(8);
            }
            let buf = pack_admitted_node::<BTreeU64>(
                KnownFenceNodeParams {
                    height: 0,
                    ts: TrxID::new(1),
                    lower_fence: &bytes,
                    lower_fence_value: BTreeU64::INVALID_VALUE,
                    upper_fence: Some(&upper),
                    hints_enabled: true,
                },
                &[PackedNodeEntry {
                    key: &bytes,
                    value: BTreeU64::from(1),
                }],
            )
            .unwrap();
            let node: &BTreeNode = layout::ref_from_bytes(buf.data());
            let viewed = OwnedKey::view(node.fence_view_checked(false).unwrap(), &budget).unwrap();
            assert_eq!(viewed.bytes(), bytes);
            assert_eq!(budget.used(), 2 * charge);
            drop(viewed);
            let different = OwnedKey::reuse_or_copy(b"gap", Some(&original), &budget).unwrap();
            assert!(!Arc::ptr_eq(&original, &different));
            assert_eq!(different.bytes(), b"gap");
            drop(different);
            fail_build_budget(&budget, "checkpoint retained key");
            let shared = OwnedKey::reuse_or_copy(&bytes, Some(&original), &budget).unwrap();
            assert!(Arc::ptr_eq(&original, &shared));
            assert_eq!(budget.used(), charge);
            assert!(OwnedKey::reuse_or_copy(b"gap", Some(&shared), &budget).is_err());
            assert!(OwnedKey::reuse_or_copy(&bytes, None, &budget).is_err());
            drop(original);
            assert_eq!(budget.used(), charge);
            assert_eq!(shared.bytes(), bytes);
            drop(shared);
            assert_eq!(budget.used(), 0);
        }
    }
}
